// INTERLOCKyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/opcode"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/planner/property"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	util2 "github.com/whtcorpsinc/milevadb/soliton"
	utilberolinaAllegroSQL "github.com/whtcorpsinc/milevadb/soliton/berolinaAllegroSQL"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	driver "github.com/whtcorpsinc/milevadb/types/berolinaAllegroSQL_driver"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"go.uber.org/zap"
)

type visitInfo struct {
	privilege allegrosql.PrivilegeType
	EDB       string
	block     string
	column    string
	err       error
}

type indexNestedLoopJoinBlocks struct {
	inljBlocks  []hintBlockInfo
	inlhjBlocks []hintBlockInfo
	inlmjBlocks []hintBlockInfo
}

type blockHintInfo struct {
	indexNestedLoopJoinBlocks
	sortMergeJoinBlocks         []hintBlockInfo
	broadcastJoinBlocks         []hintBlockInfo
	broadcastJoinPreferredLocal []hintBlockInfo
	hashJoinBlocks              []hintBlockInfo
	indexHintList               []indexHintInfo
	tiflashBlocks               []hintBlockInfo
	einsteindbBlocks            []hintBlockInfo
	aggHints                    aggHintInfo
	indexMergeHintList          []indexHintInfo
	timeRangeHint               ast.HintTimeRange
	limitHints                  limitHintInfo
}

type limitHintInfo struct {
	preferLimitToINTERLOCK bool
}

type hintBlockInfo struct {
	dbName       perceptron.CIStr
	tblName      perceptron.CIStr
	partitions   []perceptron.CIStr
	selectOffset int
	matched      bool
}

type indexHintInfo struct {
	dbName     perceptron.CIStr
	tblName    perceptron.CIStr
	partitions []perceptron.CIStr
	indexHint  *ast.IndexHint
	// Matched indicates whether this index hint
	// has been successfully applied to a DataSource.
	// If an indexHintInfo is not matched after building
	// a Select statement, we will generate a warning for it.
	matched bool
}

func (hint *indexHintInfo) hintTypeString() string {
	switch hint.indexHint.HintType {
	case ast.HintUse:
		return "use_index"
	case ast.HintIgnore:
		return "ignore_index"
	case ast.HintForce:
		return "force_index"
	}
	return ""
}

// indexString formats the indexHint as dbName.blockName[, indexNames].
func (hint *indexHintInfo) indexString() string {
	var indexListString string
	indexList := make([]string, len(hint.indexHint.IndexNames))
	for i := range hint.indexHint.IndexNames {
		indexList[i] = hint.indexHint.IndexNames[i].L
	}
	if len(indexList) > 0 {
		indexListString = fmt.Sprintf(", %s", strings.Join(indexList, ", "))
	}
	return fmt.Sprintf("%s.%s%s", hint.dbName, hint.tblName, indexListString)
}

type aggHintInfo struct {
	preferAggType        uint
	preferAggToINTERLOCK bool
}

// QueryTimeRange represents a time range specified by TIME_RANGE hint
type QueryTimeRange struct {
	From time.Time
	To   time.Time
}

// Condition returns a WHERE clause base on it's value
func (tr *QueryTimeRange) Condition() string {
	return fmt.Sprintf("where time>='%s' and time<='%s'", tr.From.Format(MetricBlockTimeFormat), tr.To.Format(MetricBlockTimeFormat))
}

func blockNames2HintBlockInfo(ctx stochastikctx.Context, hintName string, hintBlocks []ast.HintBlock, p *hint.BlockHintProcessor, nodeType hint.NodeType, currentOffset int) []hintBlockInfo {
	if len(hintBlocks) == 0 {
		return nil
	}
	hintBlockInfos := make([]hintBlockInfo, 0, len(hintBlocks))
	defaultDBName := perceptron.NewCIStr(ctx.GetStochastikVars().CurrentDB)
	isInapplicable := false
	for _, hintBlock := range hintBlocks {
		blockInfo := hintBlockInfo{
			dbName:       hintBlock.DBName,
			tblName:      hintBlock.BlockName,
			partitions:   hintBlock.PartitionList,
			selectOffset: p.GetHintOffset(hintBlock.QBName, nodeType, currentOffset),
		}
		if blockInfo.dbName.L == "" {
			blockInfo.dbName = defaultDBName
		}
		switch hintName {
		case MilevaDBMergeJoin, HintSMJ, MilevaDBIndexNestedLoopJoin, HintINLJ, HintINLHJ, HintINLMJ, MilevaDBHashJoin, HintHJ:
			if len(blockInfo.partitions) > 0 {
				isInapplicable = true
			}
		}
		hintBlockInfos = append(hintBlockInfos, blockInfo)
	}
	if isInapplicable {
		ctx.GetStochastikVars().StmtCtx.AppendWarning(
			errors.New(fmt.Sprintf("Optimizer Hint %s is inapplicable on specified partitions",
				restore2JoinHint(hintName, hintBlockInfos))))
		return nil
	}
	return hintBlockInfos
}

// ifPreferAsLocalInBCJoin checks if there is a data source specified as local read by hint
func (info *blockHintInfo) ifPreferAsLocalInBCJoin(p LogicalPlan, blockOffset int) bool {
	alias := extractBlockAlias(p, blockOffset)
	if alias != nil {
		blockNames := make([]*hintBlockInfo, 1)
		blockNames[0] = alias
		return info.matchBlockName(blockNames, info.broadcastJoinPreferredLocal)
	}
	for _, c := range p.Children() {
		if info.ifPreferAsLocalInBCJoin(c, blockOffset) {
			return true
		}
	}
	return false
}

func (info *blockHintInfo) ifPreferMergeJoin(blockNames ...*hintBlockInfo) bool {
	return info.matchBlockName(blockNames, info.sortMergeJoinBlocks)
}

func (info *blockHintInfo) ifPreferBroadcastJoin(blockNames ...*hintBlockInfo) bool {
	return info.matchBlockName(blockNames, info.broadcastJoinBlocks)
}

func (info *blockHintInfo) ifPreferHashJoin(blockNames ...*hintBlockInfo) bool {
	return info.matchBlockName(blockNames, info.hashJoinBlocks)
}

func (info *blockHintInfo) ifPreferINLJ(blockNames ...*hintBlockInfo) bool {
	return info.matchBlockName(blockNames, info.indexNestedLoopJoinBlocks.inljBlocks)
}

func (info *blockHintInfo) ifPreferINLHJ(blockNames ...*hintBlockInfo) bool {
	return info.matchBlockName(blockNames, info.indexNestedLoopJoinBlocks.inlhjBlocks)
}

func (info *blockHintInfo) ifPreferINLMJ(blockNames ...*hintBlockInfo) bool {
	return info.matchBlockName(blockNames, info.indexNestedLoopJoinBlocks.inlmjBlocks)
}

func (info *blockHintInfo) ifPreferTiFlash(blockName *hintBlockInfo) *hintBlockInfo {
	if blockName == nil {
		return nil
	}
	for i, tbl := range info.tiflashBlocks {
		if blockName.dbName.L == tbl.dbName.L && blockName.tblName.L == tbl.tblName.L && tbl.selectOffset == blockName.selectOffset {
			info.tiflashBlocks[i].matched = true
			return &tbl
		}
	}
	return nil
}

func (info *blockHintInfo) ifPreferEinsteinDB(blockName *hintBlockInfo) *hintBlockInfo {
	if blockName == nil {
		return nil
	}
	for i, tbl := range info.einsteindbBlocks {
		if blockName.dbName.L == tbl.dbName.L && blockName.tblName.L == tbl.tblName.L && tbl.selectOffset == blockName.selectOffset {
			info.einsteindbBlocks[i].matched = true
			return &tbl
		}
	}
	return nil
}

// matchBlockName checks whether the hint hit the need.
// Only need either side matches one on the list.
// Even though you can put 2 blocks on the list,
// it doesn't mean optimizer will reorder to make them
// join directly.
// Which it joins on with depend on sequence of traverse
// and without reorder, user might adjust themselves.
// This is similar to MyALLEGROSQL hints.
func (info *blockHintInfo) matchBlockName(blocks []*hintBlockInfo, hintBlocks []hintBlockInfo) bool {
	hintMatched := false
	for _, block := range blocks {
		for i, curEntry := range hintBlocks {
			if block == nil {
				continue
			}
			if curEntry.dbName.L == block.dbName.L && curEntry.tblName.L == block.tblName.L && block.selectOffset == curEntry.selectOffset {
				hintBlocks[i].matched = true
				hintMatched = true
				break
			}
		}
	}
	return hintMatched
}

func restore2BlockHint(hintBlocks ...hintBlockInfo) string {
	buffer := bytes.NewBufferString("")
	for i, block := range hintBlocks {
		buffer.WriteString(block.tblName.L)
		if len(block.partitions) > 0 {
			buffer.WriteString(" PARTITION(")
			for j, partition := range block.partitions {
				if j > 0 {
					buffer.WriteString(", ")
				}
				buffer.WriteString(partition.L)
			}
			buffer.WriteString(")")
		}
		if i < len(hintBlocks)-1 {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

func restore2JoinHint(hintType string, hintBlocks []hintBlockInfo) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(hintType))
	buffer.WriteString("(")
	buffer.WriteString(restore2BlockHint(hintBlocks...))
	buffer.WriteString(") */")
	return buffer.String()
}

func restore2IndexHint(hintType string, hintIndex indexHintInfo) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(hintType))
	buffer.WriteString("(")
	buffer.WriteString(restore2BlockHint(hintBlockInfo{
		dbName:     hintIndex.dbName,
		tblName:    hintIndex.tblName,
		partitions: hintIndex.partitions,
	}))
	if hintIndex.indexHint != nil && len(hintIndex.indexHint.IndexNames) > 0 {
		for i, indexName := range hintIndex.indexHint.IndexNames {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(" " + indexName.L)
		}
	}
	buffer.WriteString(") */")
	return buffer.String()
}

func restore2StorageHint(tiflashBlocks, einsteindbBlocks []hintBlockInfo) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(HintReadFromStorage))
	buffer.WriteString("(")
	if len(tiflashBlocks) > 0 {
		buffer.WriteString("tiflash[")
		buffer.WriteString(restore2BlockHint(tiflashBlocks...))
		buffer.WriteString("]")
		if len(einsteindbBlocks) > 0 {
			buffer.WriteString(", ")
		}
	}
	if len(einsteindbBlocks) > 0 {
		buffer.WriteString("einsteindb[")
		buffer.WriteString(restore2BlockHint(einsteindbBlocks...))
		buffer.WriteString("]")
	}
	buffer.WriteString(") */")
	return buffer.String()
}

func extractUnmatchedBlocks(hintBlocks []hintBlockInfo) []string {
	var blockNames []string
	for _, block := range hintBlocks {
		if !block.matched {
			blockNames = append(blockNames, block.tblName.O)
		}
	}
	return blockNames
}

// clauseCode indicates in which clause the column is currently.
type clauseCode int

const (
	unknowClause clauseCode = iota
	fieldList
	havingClause
	onClause
	orderByClause
	whereClause
	groupByClause
	showStatement
	globalOrderByClause
)

var clauseMsg = map[clauseCode]string{
	unknowClause:        "",
	fieldList:           "field list",
	havingClause:        "having clause",
	onClause:            "on clause",
	orderByClause:       "order clause",
	whereClause:         "where clause",
	groupByClause:       "group statement",
	showStatement:       "show statement",
	globalOrderByClause: "global ORDER clause",
}

type capFlagType = uint64

const (
	_ capFlagType = iota
	// canExpandAST indicates whether the origin AST can be expanded during plan
	// building. ONLY used for `CreateViewStmt` now.
	canExpandAST
	// collectUnderlyingViewName indicates whether to collect the underlying
	// view names of a CreateViewStmt during plan building.
	collectUnderlyingViewName
)

// PlanBuilder builds Plan from an ast.Node.
// It just builds the ast node straightforwardly.
type PlanBuilder struct {
	ctx          stochastikctx.Context
	is           schemareplicant.SchemaReplicant
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.DeferredCausetNameExpr]int
	// visitInfo is used for privilege check.
	visitInfo     []visitInfo
	blockHintInfo []blockHintInfo
	// optFlag indicates the flags of the optimizer rules.
	optFlag uint64
	// capFlag indicates the capability flags.
	capFlag capFlagType

	curClause clauseCode

	// rewriterPool stores the expressionRewriter we have created to reuse it if it has been released.
	// rewriterCounter counts how many rewriter is being used.
	rewriterPool    []*expressionRewriter
	rewriterCounter int

	windowSpecs     map[string]*ast.WindowSpec
	inUFIDelateStmt bool
	inDeleteStmt    bool
	// inStraightJoin represents whether the current "SELECT" statement has
	// "STRAIGHT_JOIN" option.
	inStraightJoin bool

	// handleHelper records the handle column position for blocks. Delete/UFIDelate/SelectLock/UnionScan may need this information.
	// It collects the information by the following procedure:
	//   Since we build the plan tree from bottom to top, we maintain a stack to record the current handle information.
	//   If it's a dataSource/blockDual node, we create a new map.
	//   If it's a aggregation, we pop the map and push a nil map since no handle information left.
	//   If it's a union, we pop all children's and push a nil map.
	//   If it's a join, we pop its children's out then merge them and push the new map to stack.
	//   If we meet a subquery, it's clearly that it's a independent problem so we just pop one map out when we finish building the subquery.
	handleHelper *handleDefCausHelper

	hintProcessor *hint.BlockHintProcessor
	// selectOffset is the offsets of current processing select stmts.
	selectOffset []int

	// SelectLock need this information to locate the dagger on partitions.
	partitionedBlock []block.PartitionedBlock
	// CreateView needs this information to check whether exists nested view.
	underlyingViewNames set.StringSet

	// evalDefaultExpr needs this information to find the corresponding column.
	// It stores the OutputNames before buildProjection.
	allNames [][]*types.FieldName
}

type handleDefCausHelper struct {
	id2HandleMapStack []map[int64][]HandleDefCauss
	stackTail         int
}

func (hch *handleDefCausHelper) appendDefCausToLastMap(tblID int64, handleDefCauss HandleDefCauss) {
	tailMap := hch.id2HandleMapStack[hch.stackTail-1]
	tailMap[tblID] = append(tailMap[tblID], handleDefCauss)
}

func (hch *handleDefCausHelper) popMap() map[int64][]HandleDefCauss {
	ret := hch.id2HandleMapStack[hch.stackTail-1]
	hch.stackTail--
	hch.id2HandleMapStack = hch.id2HandleMapStack[:hch.stackTail]
	return ret
}

func (hch *handleDefCausHelper) pushMap(m map[int64][]HandleDefCauss) {
	hch.id2HandleMapStack = append(hch.id2HandleMapStack, m)
	hch.stackTail++
}

func (hch *handleDefCausHelper) mergeAndPush(m1, m2 map[int64][]HandleDefCauss) {
	newMap := make(map[int64][]HandleDefCauss, mathutil.Max(len(m1), len(m2)))
	for k, v := range m1 {
		newMap[k] = make([]HandleDefCauss, len(v))
		INTERLOCKy(newMap[k], v)
	}
	for k, v := range m2 {
		if _, ok := newMap[k]; ok {
			newMap[k] = append(newMap[k], v...)
		} else {
			newMap[k] = make([]HandleDefCauss, len(v))
			INTERLOCKy(newMap[k], v)
		}
	}
	hch.pushMap(newMap)
}

func (hch *handleDefCausHelper) tailMap() map[int64][]HandleDefCauss {
	return hch.id2HandleMapStack[hch.stackTail-1]
}

// GetVisitInfo gets the visitInfo of the PlanBuilder.
func (b *PlanBuilder) GetVisitInfo() []visitInfo {
	return b.visitInfo
}

// GetDBBlockInfo gets the accessed dbs and blocks info.
func (b *PlanBuilder) GetDBBlockInfo() []stmtctx.BlockEntry {
	var blocks []stmtctx.BlockEntry
	existsFunc := func(tbls []stmtctx.BlockEntry, tbl *stmtctx.BlockEntry) bool {
		for _, t := range tbls {
			if t == *tbl {
				return true
			}
		}
		return false
	}
	for _, v := range b.visitInfo {
		tbl := &stmtctx.BlockEntry{EDB: v.EDB, Block: v.block}
		if !existsFunc(blocks, tbl) {
			blocks = append(blocks, *tbl)
		}
	}
	return blocks
}

// GetOptFlag gets the optFlag of the PlanBuilder.
func (b *PlanBuilder) GetOptFlag() uint64 {
	return b.optFlag
}

func (b *PlanBuilder) getSelectOffset() int {
	if len(b.selectOffset) > 0 {
		return b.selectOffset[len(b.selectOffset)-1]
	}
	return -1
}

func (b *PlanBuilder) pushSelectOffset(offset int) {
	b.selectOffset = append(b.selectOffset, offset)
}

func (b *PlanBuilder) popSelectOffset() {
	b.selectOffset = b.selectOffset[:len(b.selectOffset)-1]
}

// NewPlanBuilder creates a new PlanBuilder.
func NewPlanBuilder(sctx stochastikctx.Context, is schemareplicant.SchemaReplicant, processor *hint.BlockHintProcessor) *PlanBuilder {
	if processor == nil {
		sctx.GetStochastikVars().PlannerSelectBlockAsName = nil
	} else {
		sctx.GetStochastikVars().PlannerSelectBlockAsName = make([]ast.HintBlock, processor.MaxSelectStmtOffset()+1)
	}
	return &PlanBuilder{
		ctx:           sctx,
		is:            is,
		colMapper:     make(map[*ast.DeferredCausetNameExpr]int),
		handleHelper:  &handleDefCausHelper{id2HandleMapStack: make([]map[int64][]HandleDefCauss, 0)},
		hintProcessor: processor,
	}
}

// Build builds the ast node to a Plan.
func (b *PlanBuilder) Build(ctx context.Context, node ast.Node) (Plan, error) {
	b.optFlag |= flagPrunDeferredCausets
	switch x := node.(type) {
	case *ast.AdminStmt:
		return b.buildAdmin(ctx, x)
	case *ast.DeallocateStmt:
		return &Deallocate{Name: x.Name}, nil
	case *ast.DeleteStmt:
		return b.buildDelete(ctx, x)
	case *ast.ExecuteStmt:
		return b.buildExecute(ctx, x)
	case *ast.ExplainStmt:
		return b.buildExplain(ctx, x)
	case *ast.ExplainForStmt:
		return b.buildExplainFor(x)
	case *ast.TraceStmt:
		return b.buildTrace(x)
	case *ast.InsertStmt:
		return b.buildInsert(ctx, x)
	case *ast.LoadDataStmt:
		return b.buildLoadData(ctx, x)
	case *ast.LoadStatsStmt:
		return b.buildLoadStats(x), nil
	case *ast.IndexAdviseStmt:
		return b.buildIndexAdvise(x), nil
	case *ast.PrepareStmt:
		return b.buildPrepare(x), nil
	case *ast.SelectStmt:
		if x.SelectIntoOpt != nil {
			return b.buildSelectInto(ctx, x)
		}
		return b.buildSelect(ctx, x)
	case *ast.SetOprStmt:
		return b.buildSetOpr(ctx, x)
	case *ast.UFIDelateStmt:
		return b.buildUFIDelate(ctx, x)
	case *ast.ShowStmt:
		return b.buildShow(ctx, x)
	case *ast.DoStmt:
		return b.buildDo(ctx, x)
	case *ast.SetStmt:
		return b.buildSet(ctx, x)
	case *ast.SetConfigStmt:
		return b.buildSetConfig(ctx, x)
	case *ast.AnalyzeBlockStmt:
		return b.buildAnalyze(x)
	case *ast.BinlogStmt, *ast.FlushStmt, *ast.UseStmt, *ast.BRIEStmt,
		*ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt, *ast.CreateUserStmt, *ast.SetPwdStmt, *ast.AlterInstanceStmt,
		*ast.GrantStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.RevokeStmt, *ast.KillStmt, *ast.DropStatsStmt,
		*ast.GrantRoleStmt, *ast.RevokeRoleStmt, *ast.SetRoleStmt, *ast.SetDefaultRoleStmt, *ast.ShutdownStmt,
		*ast.CreateStatisticsStmt, *ast.DropStatisticsStmt:
		return b.buildSimple(node.(ast.StmtNode))
	case ast.DBSNode:
		return b.buildDBS(ctx, x)
	case *ast.CreateBindingStmt:
		return b.buildCreateBindPlan(x)
	case *ast.DropBindingStmt:
		return b.buildDropBindPlan(x)
	case *ast.ChangeStmt:
		return b.buildChange(x)
	case *ast.SplitRegionStmt:
		return b.buildSplitRegion(x)
	}
	return nil, ErrUnsupportedType.GenWithStack("Unsupported type %T", node)
}

func (b *PlanBuilder) buildSetConfig(ctx context.Context, v *ast.SetConfigStmt) (Plan, error) {
	privErr := ErrSpecificAccessDenied.GenWithStackByArgs("CONFIG")
	b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.ConfigPriv, "", "", "", privErr)
	mockBlockPlan := LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
	expr, _, err := b.rewrite(ctx, v.Value, mockBlockPlan, nil, true)
	return &SetConfig{Name: v.Name, Type: v.Type, Instance: v.Instance, Value: expr}, err
}

func (b *PlanBuilder) buildChange(v *ast.ChangeStmt) (Plan, error) {
	exe := &Change{
		ChangeStmt: v,
	}
	return exe, nil
}

func (b *PlanBuilder) buildExecute(ctx context.Context, v *ast.ExecuteStmt) (Plan, error) {
	vars := make([]expression.Expression, 0, len(v.UsingVars))
	for _, expr := range v.UsingVars {
		newExpr, _, err := b.rewrite(ctx, expr, nil, nil, true)
		if err != nil {
			return nil, err
		}
		vars = append(vars, newExpr)
	}
	exe := &Execute{Name: v.Name, UsingVars: vars, ExecID: v.ExecID}
	if v.BinaryArgs != nil {
		exe.PrepareParams = v.BinaryArgs.([]types.Causet)
	}
	return exe, nil
}

func (b *PlanBuilder) buildDo(ctx context.Context, v *ast.DoStmt) (Plan, error) {
	var p LogicalPlan
	dual := LogicalBlockDual{RowCount: 1}.Init(b.ctx, b.getSelectOffset())
	dual.SetSchema(expression.NewSchema())
	p = dual
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(v.Exprs))}.Init(b.ctx, b.getSelectOffset())
	proj.names = make([]*types.FieldName, len(v.Exprs))
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(v.Exprs))...)
	for _, astExpr := range v.Exprs {
		expr, np, err := b.rewrite(ctx, astExpr, p, nil, true)
		if err != nil {
			return nil, err
		}
		p = np
		proj.Exprs = append(proj.Exprs, expr)
		schemaReplicant.Append(&expression.DeferredCauset{
			UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			RetType:  expr.GetType(),
		})
	}
	proj.SetChildren(p)
	proj.self = proj
	proj.SetSchema(schemaReplicant)
	proj.CalculateNoDelay = true
	return proj, nil
}

func (b *PlanBuilder) buildSet(ctx context.Context, v *ast.SetStmt) (Plan, error) {
	p := &Set{}
	for _, vars := range v.Variables {
		if vars.IsGlobal {
			err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", err)
		}
		assign := &expression.VarAssignment{
			Name:     vars.Name,
			IsGlobal: vars.IsGlobal,
			IsSystem: vars.IsSystem,
		}
		if _, ok := vars.Value.(*ast.DefaultExpr); !ok {
			if cn, ok2 := vars.Value.(*ast.DeferredCausetNameExpr); ok2 && cn.Name.Block.L == "" {
				// Convert column name expression to string value expression.
				char, col := b.ctx.GetStochastikVars().GetCharsetInfo()
				vars.Value = ast.NewValueExpr(cn.Name.Name.O, char, col)
			}
			mockBlockPlan := LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
			var err error
			assign.Expr, _, err = b.rewrite(ctx, vars.Value, mockBlockPlan, nil, true)
			if err != nil {
				return nil, err
			}
		} else {
			assign.IsDefault = true
		}
		if vars.ExtendValue != nil {
			assign.ExtendValue = &expression.Constant{
				Value:   vars.ExtendValue.(*driver.ValueExpr).Causet,
				RetType: &vars.ExtendValue.(*driver.ValueExpr).Type,
			}
		}
		p.VarAssigns = append(p.VarAssigns, assign)
	}
	return p, nil
}

func (b *PlanBuilder) buildDropBindPlan(v *ast.DropBindingStmt) (Plan, error) {
	p := &ALLEGROSQLBindPlan{
		ALLEGROSQLBindOp:    OpALLEGROSQLBindDrop,
		NormdOrigALLEGROSQL: berolinaAllegroSQL.Normalize(v.OriginSel.Text()),
		IsGlobal:            v.GlobalSINTERLOCKe,
		EDB:                 utilberolinaAllegroSQL.GetDefaultDB(v.OriginSel, b.ctx.GetStochastikVars().CurrentDB),
	}
	if v.HintedSel != nil {
		p.BindALLEGROSQL = v.HintedSel.Text()
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", nil)
	return p, nil
}

func (b *PlanBuilder) buildCreateBindPlan(v *ast.CreateBindingStmt) (Plan, error) {
	charSet, collation := b.ctx.GetStochastikVars().GetCharsetInfo()
	p := &ALLEGROSQLBindPlan{
		ALLEGROSQLBindOp:    OpALLEGROSQLBindCreate,
		NormdOrigALLEGROSQL: berolinaAllegroSQL.Normalize(v.OriginSel.Text()),
		BindALLEGROSQL:      v.HintedSel.Text(),
		IsGlobal:            v.GlobalSINTERLOCKe,
		BindStmt:            v.HintedSel,
		EDB:                 utilberolinaAllegroSQL.GetDefaultDB(v.OriginSel, b.ctx.GetStochastikVars().CurrentDB),
		Charset:             charSet,
		DefCauslation:       collation,
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", nil)
	return p, nil
}

// detectSelectAgg detects an aggregate function or GROUP BY clause.
func (b *PlanBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.Fields.Fields {
		if ast.HasAggFlag(f.Expr) {
			return true
		}
	}
	if sel.Having != nil {
		if ast.HasAggFlag(sel.Having.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasAggFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

func (b *PlanBuilder) detectSelectWindow(sel *ast.SelectStmt) bool {
	for _, f := range sel.Fields.Fields {
		if ast.HasWindowFlag(f.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasWindowFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

func getPathByIndexName(paths []*soliton.AccessPath, idxName perceptron.CIStr, tblInfo *perceptron.BlockInfo) *soliton.AccessPath {
	var blockPath *soliton.AccessPath
	for _, path := range paths {
		if path.IsBlockPath() {
			blockPath = path
			continue
		}
		if path.Index.Name.L == idxName.L {
			return path
		}
	}
	if isPrimaryIndex(idxName) && (tblInfo.PKIsHandle || tblInfo.IsCommonHandle) {
		return blockPath
	}
	return nil
}

func isPrimaryIndex(indexName perceptron.CIStr) bool {
	return indexName.L == "primary"
}

func genTiFlashPath(tblInfo *perceptron.BlockInfo, isGlobalRead bool) *soliton.AccessPath {
	tiFlashPath := &soliton.AccessPath{StoreType: ekv.TiFlash, IsTiFlashGlobalRead: isGlobalRead}
	fillContentForBlockPath(tiFlashPath, tblInfo)
	return tiFlashPath
}

func fillContentForBlockPath(blockPath *soliton.AccessPath, tblInfo *perceptron.BlockInfo) {
	if tblInfo.IsCommonHandle {
		blockPath.IsCommonHandlePath = true
		for _, index := range tblInfo.Indices {
			if index.Primary {
				blockPath.Index = index
				break
			}
		}
	} else {
		blockPath.IsIntHandlePath = true
	}
}

func getPossibleAccessPaths(ctx stochastikctx.Context, blockHints *blockHintInfo, indexHints []*ast.IndexHint, tbl block.Block, dbName, tblName perceptron.CIStr) ([]*soliton.AccessPath, error) {
	tblInfo := tbl.Meta()
	publicPaths := make([]*soliton.AccessPath, 0, len(tblInfo.Indices)+2)
	tp := ekv.EinsteinDB
	if tbl.Type().IsClusterBlock() {
		tp = ekv.MilevaDB
	}
	blockPath := &soliton.AccessPath{StoreType: tp}
	fillContentForBlockPath(blockPath, tblInfo)
	publicPaths = append(publicPaths, blockPath)
	if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
		publicPaths = append(publicPaths, genTiFlashPath(tblInfo, false))
		publicPaths = append(publicPaths, genTiFlashPath(tblInfo, true))
	}
	optimizerUseInvisibleIndexes := ctx.GetStochastikVars().OptimizerUseInvisibleIndexes
	for _, index := range tblInfo.Indices {
		if index.State == perceptron.StatePublic {
			// Filter out invisible index, because they are not visible for optimizer
			if !optimizerUseInvisibleIndexes && index.Invisible {
				continue
			}
			if tblInfo.IsCommonHandle && index.Primary {
				continue
			}
			publicPaths = append(publicPaths, &soliton.AccessPath{Index: index})
		}
	}

	hasScanHint, hasUseOrForce := false, false
	available := make([]*soliton.AccessPath, 0, len(publicPaths))
	ignored := make([]*soliton.AccessPath, 0, len(publicPaths))

	// Extract comment-style index hint like /*+ INDEX(t, idx1, idx2) */.
	indexHintsLen := len(indexHints)
	if blockHints != nil {
		for i, hint := range blockHints.indexHintList {
			if hint.dbName.L == dbName.L && hint.tblName.L == tblName.L {
				indexHints = append(indexHints, hint.indexHint)
				blockHints.indexHintList[i].matched = true
			}
		}
	}

	_, isolationReadEnginesHasEinsteinDB := ctx.GetStochastikVars().GetIsolationReadEngines()[ekv.EinsteinDB]
	for i, hint := range indexHints {
		if hint.HintSINTERLOCKe != ast.HintForScan {
			continue
		}

		hasScanHint = true

		if !isolationReadEnginesHasEinsteinDB {
			if hint.IndexNames != nil {
				engineVals, _ := ctx.GetStochastikVars().GetSystemVar(variable.MilevaDBIsolationReadEngines)
				err := errors.New(fmt.Sprintf("MilevaDB doesn't support index in the isolation read engines(value: '%v')", engineVals))
				if i < indexHintsLen {
					return nil, err
				}
				ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
			}
			continue
		}
		// It is syntactically valid to omit index_list for USE INDEX, which means “use no indexes”.
		// Omitting index_list for FORCE INDEX or IGNORE INDEX is a syntax error.
		// See https://dev.allegrosql.com/doc/refman/8.0/en/index-hints.html.
		if hint.IndexNames == nil && hint.HintType != ast.HintIgnore {
			if path := getBlockPath(publicPaths); path != nil {
				hasUseOrForce = true
				path.Forced = true
				available = append(available, path)
			}
		}
		for _, idxName := range hint.IndexNames {
			path := getPathByIndexName(publicPaths, idxName, tblInfo)
			if path == nil {
				err := ErrKeyDoesNotExist.GenWithStackByArgs(idxName, tblInfo.Name)
				// if hint is from comment-style allegrosql hints, we should throw a warning instead of error.
				if i < indexHintsLen {
					return nil, err
				}
				ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
				continue
			}
			if hint.HintType == ast.HintIgnore {
				// DefCauslect all the ignored index hints.
				ignored = append(ignored, path)
				continue
			}
			// Currently we don't distinguish between "FORCE" and "USE" because
			// our cost estimation is not reliable.
			hasUseOrForce = true
			path.Forced = true
			available = append(available, path)
		}
	}

	if !hasScanHint || !hasUseOrForce {
		available = publicPaths
	}

	available = removeIgnoredPaths(available, ignored, tblInfo)

	// If we have got "FORCE" or "USE" index hint but got no available index,
	// we have to use block scan.
	if len(available) == 0 {
		available = append(available, blockPath)
	}
	return available, nil
}

func filterPathByIsolationRead(ctx stochastikctx.Context, paths []*soliton.AccessPath, dbName perceptron.CIStr) ([]*soliton.AccessPath, error) {
	// TODO: filter paths with isolation read locations.
	if dbName.L == allegrosql.SystemDB {
		return paths, nil
	}
	isolationReadEngines := ctx.GetStochastikVars().GetIsolationReadEngines()
	availableEngine := map[ekv.StoreType]struct{}{}
	var availableEngineStr string
	for i := len(paths) - 1; i >= 0; i-- {
		if _, ok := availableEngine[paths[i].StoreType]; !ok {
			availableEngine[paths[i].StoreType] = struct{}{}
			if availableEngineStr != "" {
				availableEngineStr += ", "
			}
			availableEngineStr += paths[i].StoreType.Name()
		}
		if _, ok := isolationReadEngines[paths[i].StoreType]; !ok && paths[i].StoreType != ekv.MilevaDB {
			paths = append(paths[:i], paths[i+1:]...)
		}
	}
	var err error
	if len(paths) == 0 {
		engineVals, _ := ctx.GetStochastikVars().GetSystemVar(variable.MilevaDBIsolationReadEngines)
		err = ErrInternal.GenWithStackByArgs(fmt.Sprintf("Can not find access path matching '%v'(value: '%v'). Available values are '%v'.",
			variable.MilevaDBIsolationReadEngines, engineVals, availableEngineStr))
	}
	return paths, err
}

func removeIgnoredPaths(paths, ignoredPaths []*soliton.AccessPath, tblInfo *perceptron.BlockInfo) []*soliton.AccessPath {
	if len(ignoredPaths) == 0 {
		return paths
	}
	remainedPaths := make([]*soliton.AccessPath, 0, len(paths))
	for _, path := range paths {
		if path.IsBlockPath() || getPathByIndexName(ignoredPaths, path.Index.Name, tblInfo) == nil {
			remainedPaths = append(remainedPaths, path)
		}
	}
	return remainedPaths
}

func (b *PlanBuilder) buildSelectLock(src LogicalPlan, dagger *ast.SelectLockInfo) *LogicalLock {
	selectLock := LogicalLock{
		Lock:             dagger,
		tblID2Handle:     b.handleHelper.tailMap(),
		partitionedBlock: b.partitionedBlock,
	}.Init(b.ctx)
	selectLock.SetChildren(src)
	return selectLock
}

func (b *PlanBuilder) buildPrepare(x *ast.PrepareStmt) Plan {
	p := &Prepare{
		Name: x.Name,
	}
	if x.ALLEGROSQLVar != nil {
		if v, ok := b.ctx.GetStochastikVars().Users[strings.ToLower(x.ALLEGROSQLVar.Name)]; ok {
			p.ALLEGROSQLText = v.GetString()
		} else {
			p.ALLEGROSQLText = "NULL"
		}
	} else {
		p.ALLEGROSQLText = x.ALLEGROSQLText
	}
	return p
}

func (b *PlanBuilder) buildAdmin(ctx context.Context, as *ast.AdminStmt) (Plan, error) {
	var ret Plan
	var err error
	switch as.Tp {
	case ast.AdminCheckBlock, ast.AdminChecHoTTex:
		ret, err = b.buildAdminCheckBlock(ctx, as)
		if err != nil {
			return ret, err
		}
	case ast.AdminRecoverIndex:
		p := &RecoverIndex{Block: as.Blocks[0], IndexName: as.Index}
		p.setSchemaAndNames(buildRecoverIndexFields())
		ret = p
	case ast.AdminCleanupIndex:
		p := &CleanupIndex{Block: as.Blocks[0], IndexName: as.Index}
		p.setSchemaAndNames(buildCleanupIndexFields())
		ret = p
	case ast.AdminChecksumBlock:
		p := &ChecksumBlock{Blocks: as.Blocks}
		p.setSchemaAndNames(buildChecksumBlockSchema())
		ret = p
	case ast.AdminShowNextRowID:
		p := &ShowNextRowID{BlockName: as.Blocks[0]}
		p.setSchemaAndNames(buildShowNextRowID())
		ret = p
	case ast.AdminShowDBS:
		p := &ShowDBS{}
		p.setSchemaAndNames(buildShowDBSFields())
		ret = p
	case ast.AdminShowDBSJobs:
		p := LogicalShowDBSJobs{JobNumber: as.JobNumber}.Init(b.ctx)
		p.setSchemaAndNames(buildShowDBSJobsFields())
		for _, col := range p.schemaReplicant.DeferredCausets {
			col.UniqueID = b.ctx.GetStochastikVars().AllocPlanDeferredCausetID()
		}
		ret = p
		if as.Where != nil {
			ret, err = b.buildSelection(ctx, p, as.Where, nil)
			if err != nil {
				return nil, err
			}
		}
	case ast.AdminCancelDBSJobs:
		p := &CancelDBSJobs{JobIDs: as.JobIDs}
		p.setSchemaAndNames(buildCancelDBSJobsFields())
		ret = p
	case ast.AdminChecHoTTexRange:
		schemaReplicant, names, err := b.buildChecHoTTexSchema(as.Blocks[0], as.Index)
		if err != nil {
			return nil, err
		}

		p := &ChecHoTTexRange{Block: as.Blocks[0], IndexName: as.Index, HandleRanges: as.HandleRanges}
		p.setSchemaAndNames(schemaReplicant, names)
		ret = p
	case ast.AdminShowDBSJobQueries:
		p := &ShowDBSJobQueries{JobIDs: as.JobIDs}
		p.setSchemaAndNames(buildShowDBSJobQueriesFields())
		ret = p
	case ast.AdminShowSlow:
		p := &ShowSlow{ShowSlow: as.ShowSlow}
		p.setSchemaAndNames(buildShowSlowSchema())
		ret = p
	case ast.AdminReloadExprPushdownBlacklist:
		return &ReloadExprPushdownBlacklist{}, nil
	case ast.AdminReloadOptMemruleBlacklist:
		return &ReloadOptMemruleBlacklist{}, nil
	case ast.AdminPluginEnable:
		return &AdminPlugins{CausetAction: Enable, Plugins: as.Plugins}, nil
	case ast.AdminPluginDisable:
		return &AdminPlugins{CausetAction: Disable, Plugins: as.Plugins}, nil
	case ast.AdminFlushBindings:
		return &ALLEGROSQLBindPlan{ALLEGROSQLBindOp: OpFlushBindings}, nil
	case ast.AdminCaptureBindings:
		return &ALLEGROSQLBindPlan{ALLEGROSQLBindOp: OpCaptureBindings}, nil
	case ast.AdminEvolveBindings:
		return &ALLEGROSQLBindPlan{ALLEGROSQLBindOp: OpEvolveBindings}, nil
	case ast.AdminReloadBindings:
		return &ALLEGROSQLBindPlan{ALLEGROSQLBindOp: OpReloadBindings}, nil
	case ast.AdminShowTelemetry:
		p := &AdminShowTelemetry{}
		p.setSchemaAndNames(buildShowTelemetrySchema())
		ret = p
	case ast.AdminResetTelemetryID:
		return &AdminResetTelemetryID{}, nil
	case ast.AdminReloadStatistics:
		return &Simple{Statement: as}, nil
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
	}

	// Admin command can only be executed by administrator.
	b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", nil)
	return ret, nil
}

// getGenExprs gets generated expressions map.
func (b *PlanBuilder) getGenExprs(ctx context.Context, dbName perceptron.CIStr, tbl block.Block, idx *perceptron.IndexInfo, exprDefCauss *expression.Schema, names types.NameSlice) (
	map[perceptron.BlockDeferredCausetID]expression.Expression, error) {
	tblInfo := tbl.Meta()
	genExprsMap := make(map[perceptron.BlockDeferredCausetID]expression.Expression)
	exprs := make([]expression.Expression, 0, len(tbl.DefCauss()))
	genExprIdxs := make([]perceptron.BlockDeferredCausetID, len(tbl.DefCauss()))
	mockBlockPlan := LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
	mockBlockPlan.SetSchema(exprDefCauss)
	mockBlockPlan.names = names
	for i, colExpr := range mockBlockPlan.Schema().DeferredCausets {
		col := tbl.DefCauss()[i]
		var expr expression.Expression
		expr = colExpr
		if col.IsGenerated() && !col.GeneratedStored {
			var err error
			expr, _, err = b.rewrite(ctx, col.GeneratedExpr, mockBlockPlan, nil, true)
			if err != nil {
				return nil, errors.Trace(err)
			}
			found := false
			for _, column := range idx.DeferredCausets {
				if strings.EqualFold(col.Name.L, column.Name.L) {
					found = true
					break
				}
			}
			if found {
				genDeferredCausetID := perceptron.BlockDeferredCausetID{BlockID: tblInfo.ID, DeferredCausetID: col.DeferredCausetInfo.ID}
				genExprsMap[genDeferredCausetID] = expr
				genExprIdxs[i] = genDeferredCausetID
			}
		}
		exprs = append(exprs, expr)
	}
	// Re-iterate expressions to handle those virtual generated columns that refers to the other generated columns.
	for i, expr := range exprs {
		exprs[i] = expression.DeferredCausetSubstitute(expr, mockBlockPlan.Schema(), exprs)
		if _, ok := genExprsMap[genExprIdxs[i]]; ok {
			genExprsMap[genExprIdxs[i]] = exprs[i]
		}
	}
	return genExprsMap, nil
}

// FindDeferredCausetInfoByID finds DeferredCausetInfo in defcaus by ID.
func FindDeferredCausetInfoByID(colInfos []*perceptron.DeferredCausetInfo, id int64) *perceptron.DeferredCausetInfo {
	for _, info := range colInfos {
		if info.ID == id {
			return info
		}
	}
	return nil
}

func (b *PlanBuilder) buildPhysicalIndexLookUpReader(ctx context.Context, dbName perceptron.CIStr, tbl block.Block, idx *perceptron.IndexInfo) (Plan, error) {
	tblInfo := tbl.Meta()
	physicalID, isPartition := getPhysicalID(tbl)
	fullExprDefCauss, _, err := expression.BlockInfo2SchemaAndNames(b.ctx, dbName, tblInfo)
	if err != nil {
		return nil, err
	}
	extraInfo, extraDefCaus, hasExtraDefCaus := tryGetPkExtraDeferredCauset(b.ctx.GetStochastikVars(), tblInfo)
	pkHandleInfo, pkHandleDefCaus, hasPkIsHandle := tryGetPkHandleDefCaus(tblInfo, fullExprDefCauss)
	commonInfos, commonDefCauss, hasCommonDefCauss := tryGetCommonHandleDefCauss(tbl, fullExprDefCauss)
	idxDefCausInfos := getIndexDeferredCausetInfos(tblInfo, idx)
	idxDefCausSchema := getIndexDefCaussSchema(tblInfo, idx, fullExprDefCauss)
	idxDefCauss, idxDefCausLens := expression.IndexInfo2PrefixDefCauss(idxDefCausInfos, idxDefCausSchema.DeferredCausets, idx)

	is := PhysicalIndexScan{
		Block:            tblInfo,
		BlockAsName:      &tblInfo.Name,
		DBName:           dbName,
		DeferredCausets:  idxDefCausInfos,
		Index:            idx,
		IdxDefCauss:      idxDefCauss,
		IdxDefCausLens:   idxDefCausLens,
		dataSourceSchema: idxDefCausSchema.Clone(),
		Ranges:           ranger.FullRange(),
		physicalBlockID:  physicalID,
		isPartition:      isPartition,
	}.Init(b.ctx, b.getSelectOffset())
	// There is no alternative plan choices, so just use pseudo stats to avoid panic.
	is.stats = &property.StatsInfo{HistDefCausl: &(statistics.PseudoBlock(tblInfo)).HistDefCausl}
	if hasCommonDefCauss {
		for _, c := range commonInfos {
			is.DeferredCausets = append(is.DeferredCausets, c.DeferredCausetInfo)
		}
	}
	is.initSchema(append(is.IdxDefCauss, commonDefCauss...), true)

	// It's double read case.
	ts := PhysicalBlockScan{
		DeferredCausets: idxDefCausInfos,
		Block:           tblInfo,
		BlockAsName:     &tblInfo.Name,
		physicalBlockID: physicalID,
		isPartition:     isPartition,
	}.Init(b.ctx, b.getSelectOffset())
	ts.SetSchema(idxDefCausSchema)
	ts.DeferredCausets = ExpandVirtualDeferredCauset(ts.DeferredCausets, ts.schemaReplicant, ts.Block.DeferredCausets)
	switch {
	case hasExtraDefCaus:
		ts.DeferredCausets = append(ts.DeferredCausets, extraInfo)
		ts.schemaReplicant.Append(extraDefCaus)
		ts.HandleIdx = []int{len(ts.DeferredCausets) - 1}
	case hasPkIsHandle:
		ts.DeferredCausets = append(ts.DeferredCausets, pkHandleInfo)
		ts.schemaReplicant.Append(pkHandleDefCaus)
		ts.HandleIdx = []int{len(ts.DeferredCausets) - 1}
	case hasCommonDefCauss:
		ts.HandleIdx = make([]int, 0, len(commonDefCauss))
		for pkOffset, cInfo := range commonInfos {
			found := false
			for i, c := range ts.DeferredCausets {
				if c.ID == cInfo.ID {
					found = true
					ts.HandleIdx = append(ts.HandleIdx, i)
					break
				}
			}
			if !found {
				ts.DeferredCausets = append(ts.DeferredCausets, cInfo.DeferredCausetInfo)
				ts.schemaReplicant.Append(commonDefCauss[pkOffset])
				ts.HandleIdx = append(ts.HandleIdx, len(ts.DeferredCausets)-1)
			}

		}
	}

	INTERLOCK := &INTERLOCKTask{
		indexPlan:            is,
		blockPlan:            ts,
		tblDefCausHists:      is.stats.HistDefCausl,
		extraHandleDefCaus:   extraDefCaus,
		commonHandleDefCauss: commonDefCauss,
	}
	rootT := finishINTERLOCKTask(b.ctx, INTERLOCK).(*rootTask)
	if err := rootT.p.ResolveIndices(); err != nil {
		return nil, err
	}
	return rootT.p, nil
}

func getIndexDeferredCausetInfos(tblInfo *perceptron.BlockInfo, idx *perceptron.IndexInfo) []*perceptron.DeferredCausetInfo {
	ret := make([]*perceptron.DeferredCausetInfo, len(idx.DeferredCausets))
	for i, idxDefCaus := range idx.DeferredCausets {
		ret[i] = tblInfo.DeferredCausets[idxDefCaus.Offset]
	}
	return ret
}

func getIndexDefCaussSchema(tblInfo *perceptron.BlockInfo, idx *perceptron.IndexInfo, allDefCausSchema *expression.Schema) *expression.Schema {
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(idx.DeferredCausets))...)
	for _, idxDefCaus := range idx.DeferredCausets {
		for i, colInfo := range tblInfo.DeferredCausets {
			if colInfo.Name.L == idxDefCaus.Name.L {
				schemaReplicant.Append(allDefCausSchema.DeferredCausets[i])
				break
			}
		}
	}
	return schemaReplicant
}

func getPhysicalID(t block.Block) (physicalID int64, isPartition bool) {
	tblInfo := t.Meta()
	if tblInfo.GetPartitionInfo() != nil {
		pid := t.(block.PhysicalBlock).GetPhysicalID()
		return pid, true
	}
	return tblInfo.ID, false
}

func tryGetPkExtraDeferredCauset(sv *variable.StochastikVars, tblInfo *perceptron.BlockInfo) (*perceptron.DeferredCausetInfo, *expression.DeferredCauset, bool) {
	if tblInfo.IsCommonHandle || tblInfo.PKIsHandle {
		return nil, nil, false
	}
	info := perceptron.NewExtraHandleDefCausInfo()
	expDefCaus := &expression.DeferredCauset{
		RetType:  types.NewFieldType(allegrosql.TypeLonglong),
		UniqueID: sv.AllocPlanDeferredCausetID(),
		ID:       perceptron.ExtraHandleID,
	}
	return info, expDefCaus, true
}

func tryGetCommonHandleDefCauss(t block.Block, allDefCausSchema *expression.Schema) ([]*block.DeferredCauset, []*expression.DeferredCauset, bool) {
	tblInfo := t.Meta()
	if !tblInfo.IsCommonHandle {
		return nil, nil, false
	}
	pk := blocks.FindPrimaryIndex(tblInfo)
	commonHandleDefCauss, _ := expression.IndexInfo2DefCauss(tblInfo.DeferredCausets, allDefCausSchema.DeferredCausets, pk)
	commonHandelDefCausInfos := blocks.TryGetCommonPkDeferredCausets(t)
	return commonHandelDefCausInfos, commonHandleDefCauss, true
}

func tryGetPkHandleDefCaus(tblInfo *perceptron.BlockInfo, allDefCausSchema *expression.Schema) (*perceptron.DeferredCausetInfo, *expression.DeferredCauset, bool) {
	if !tblInfo.PKIsHandle {
		return nil, nil, false
	}
	for i, c := range tblInfo.DeferredCausets {
		if allegrosql.HasPriKeyFlag(c.Flag) {
			return c, allDefCausSchema.DeferredCausets[i], true
		}
	}
	return nil, nil, false
}

func (b *PlanBuilder) buildPhysicalIndexLookUpReaders(ctx context.Context, dbName perceptron.CIStr, tbl block.Block, indices []block.Index) ([]Plan, []*perceptron.IndexInfo, error) {
	tblInfo := tbl.Meta()
	// get index information
	indexInfos := make([]*perceptron.IndexInfo, 0, len(tblInfo.Indices))
	indexLookUpReaders := make([]Plan, 0, len(tblInfo.Indices))
	for _, idx := range indices {
		idxInfo := idx.Meta()
		if idxInfo.State != perceptron.StatePublic {
			logutil.Logger(context.Background()).Info("build physical index lookup reader, the index isn't public",
				zap.String("index", idxInfo.Name.O), zap.Stringer("state", idxInfo.State), zap.String("block", tblInfo.Name.O))
			continue
		}
		indexInfos = append(indexInfos, idxInfo)
		// For partition blocks.
		if pi := tbl.Meta().GetPartitionInfo(); pi != nil {
			for _, def := range pi.Definitions {
				t := tbl.(block.PartitionedBlock).GetPartition(def.ID)
				reader, err := b.buildPhysicalIndexLookUpReader(ctx, dbName, t, idxInfo)
				if err != nil {
					return nil, nil, err
				}
				indexLookUpReaders = append(indexLookUpReaders, reader)
			}
			continue
		}
		// For non-partition blocks.
		reader, err := b.buildPhysicalIndexLookUpReader(ctx, dbName, tbl, idxInfo)
		if err != nil {
			return nil, nil, err
		}
		indexLookUpReaders = append(indexLookUpReaders, reader)
	}
	if len(indexLookUpReaders) == 0 {
		return nil, nil, nil
	}
	return indexLookUpReaders, indexInfos, nil
}

func (b *PlanBuilder) buildAdminCheckBlock(ctx context.Context, as *ast.AdminStmt) (*CheckBlock, error) {
	tblName := as.Blocks[0]
	blockInfo := as.Blocks[0].BlockInfo
	tbl, ok := b.is.BlockByID(blockInfo.ID)
	if !ok {
		return nil, schemareplicant.ErrBlockNotExists.GenWithStackByArgs(tblName.DBInfo.Name.O, blockInfo.Name.O)
	}
	p := &CheckBlock{
		DBName: tblName.Schema.O,
		Block:  tbl,
	}
	var readerPlans []Plan
	var indexInfos []*perceptron.IndexInfo
	var err error
	if as.Tp == ast.AdminChecHoTTex {
		// get index information
		var idx block.Index
		idxName := strings.ToLower(as.Index)
		for _, index := range tbl.Indices() {
			if index.Meta().Name.L == idxName {
				idx = index
				break
			}
		}
		if idx == nil {
			return nil, errors.Errorf("index %s do not exist", as.Index)
		}
		if idx.Meta().State != perceptron.StatePublic {
			return nil, errors.Errorf("index %s state %s isn't public", as.Index, idx.Meta().State)
		}
		p.ChecHoTTex = true
		readerPlans, indexInfos, err = b.buildPhysicalIndexLookUpReaders(ctx, tblName.Schema, tbl, []block.Index{idx})
	} else {
		readerPlans, indexInfos, err = b.buildPhysicalIndexLookUpReaders(ctx, tblName.Schema, tbl, tbl.Indices())
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	readers := make([]*PhysicalIndexLookUpReader, 0, len(readerPlans))
	for _, plan := range readerPlans {
		readers = append(readers, plan.(*PhysicalIndexLookUpReader))
	}
	p.IndexInfos = indexInfos
	p.IndexLookUpReaders = readers
	return p, nil
}

func (b *PlanBuilder) buildChecHoTTexSchema(tn *ast.BlockName, indexName string) (*expression.Schema, types.NameSlice, error) {
	schemaReplicant := expression.NewSchema()
	var names types.NameSlice
	indexName = strings.ToLower(indexName)
	indicesInfo := tn.BlockInfo.Indices
	defcaus := tn.BlockInfo.DefCauss()
	for _, idxInfo := range indicesInfo {
		if idxInfo.Name.L != indexName {
			continue
		}
		for _, idxDefCaus := range idxInfo.DeferredCausets {
			col := defcaus[idxDefCaus.Offset]
			names = append(names, &types.FieldName{
				DefCausName: idxDefCaus.Name,
				TblName:     tn.Name,
				DBName:      tn.Schema,
			})
			schemaReplicant.Append(&expression.DeferredCauset{
				RetType:  &col.FieldType,
				UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
				ID:       col.ID})
		}
		names = append(names, &types.FieldName{
			DefCausName: perceptron.NewCIStr("extra_handle"),
			TblName:     tn.Name,
			DBName:      tn.Schema,
		})
		schemaReplicant.Append(&expression.DeferredCauset{
			RetType:  types.NewFieldType(allegrosql.TypeLonglong),
			UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			ID:       -1,
		})
	}
	if schemaReplicant.Len() == 0 {
		return nil, nil, errors.Errorf("index %s not found", indexName)
	}
	return schemaReplicant, names, nil
}

// getDefCaussInfo returns the info of index columns, normal columns and primary key.
func getDefCaussInfo(tn *ast.BlockName) (indicesInfo []*perceptron.IndexInfo, defcausInfo []*perceptron.DeferredCausetInfo) {
	tbl := tn.BlockInfo
	for _, col := range tbl.DeferredCausets {
		// The virtual column will not causetstore any data in EinsteinDB, so it should be ignored when collect statistics
		if col.IsGenerated() && !col.GeneratedStored {
			continue
		}
		if allegrosql.HasPriKeyFlag(col.Flag) && (tbl.PKIsHandle || tbl.IsCommonHandle) {
			continue
		}
		defcausInfo = append(defcausInfo, col)
	}
	for _, idx := range tn.BlockInfo.Indices {
		if idx.State == perceptron.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	return
}

// BuildHandleDefCaussForAnalyze is exported for test.
func BuildHandleDefCaussForAnalyze(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo) HandleDefCauss {
	var handleDefCauss HandleDefCauss
	switch {
	case tblInfo.PKIsHandle:
		pkDefCaus := tblInfo.GetPkDefCausInfo()
		handleDefCauss = &IntHandleDefCauss{col: &expression.DeferredCauset{
			ID:      pkDefCaus.ID,
			RetType: &pkDefCaus.FieldType,
			Index:   pkDefCaus.Offset,
		}}
	case tblInfo.IsCommonHandle:
		pkIdx := blocks.FindPrimaryIndex(tblInfo)
		pkDefCausLen := len(pkIdx.DeferredCausets)
		columns := make([]*expression.DeferredCauset, pkDefCausLen)
		for i := 0; i < pkDefCausLen; i++ {
			colInfo := tblInfo.DeferredCausets[pkIdx.DeferredCausets[i].Offset]
			columns[i] = &expression.DeferredCauset{
				ID:      colInfo.ID,
				RetType: &colInfo.FieldType,
				Index:   colInfo.Offset,
			}
		}
		handleDefCauss = &CommonHandleDefCauss{
			tblInfo: tblInfo,
			idxInfo: pkIdx,
			columns: columns,
			sc:      ctx.GetStochastikVars().StmtCtx,
		}
	}
	return handleDefCauss
}

func getPhysicalIDsAndPartitionNames(tblInfo *perceptron.BlockInfo, partitionNames []perceptron.CIStr) ([]int64, []string, error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		if len(partitionNames) != 0 {
			return nil, nil, errors.Trace(dbs.ErrPartitionMgmtOnNonpartitioned)
		}
		return []int64{tblInfo.ID}, []string{""}, nil
	}
	if len(partitionNames) == 0 {
		ids := make([]int64, 0, len(pi.Definitions))
		names := make([]string, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
			names = append(names, def.Name.O)
		}
		return ids, names, nil
	}
	ids := make([]int64, 0, len(partitionNames))
	names := make([]string, 0, len(partitionNames))
	for _, name := range partitionNames {
		found := false
		for _, def := range pi.Definitions {
			if def.Name.L == name.L {
				found = true
				ids = append(ids, def.ID)
				names = append(names, def.Name.O)
				break
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("can not found the specified partition name %s in the block definition", name.O)
		}
	}
	return ids, names, nil
}

func (b *PlanBuilder) buildAnalyzeBlock(as *ast.AnalyzeBlockStmt, opts map[ast.AnalyzeOptionType]uint64) (Plan, error) {
	p := &Analyze{Opts: opts}
	for _, tbl := range as.BlockNames {
		if tbl.BlockInfo.IsView() {
			return nil, errors.Errorf("analyze view %s is not supported now.", tbl.Name.O)
		}
		if tbl.BlockInfo.IsSequence() {
			return nil, errors.Errorf("analyze sequence %s is not supported now.", tbl.Name.O)
		}
		idxInfo, colInfo := getDefCaussInfo(tbl)
		physicalIDs, names, err := getPhysicalIDsAndPartitionNames(tbl.BlockInfo, as.PartitionNames)
		if err != nil {
			return nil, err
		}
		for _, idx := range idxInfo {
			for i, id := range physicalIDs {
				info := analyzeInfo{DBName: tbl.Schema.O, BlockName: tbl.Name.O, PartitionName: names[i], BlockID: AnalyzeBlockID{PersistID: id, DefCauslectIDs: []int64{id}}, Incremental: as.Incremental}
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{
					IndexInfo:   idx,
					analyzeInfo: info,
					TblInfo:     tbl.BlockInfo,
				})
			}
		}
		handleDefCauss := BuildHandleDefCaussForAnalyze(b.ctx, tbl.BlockInfo)
		if len(colInfo) > 0 || handleDefCauss != nil {
			for i, id := range physicalIDs {
				info := analyzeInfo{DBName: tbl.Schema.O, BlockName: tbl.Name.O, PartitionName: names[i], BlockID: AnalyzeBlockID{PersistID: id, DefCauslectIDs: []int64{id}}, Incremental: as.Incremental}
				p.DefCausTasks = append(p.DefCausTasks, AnalyzeDeferredCausetsTask{
					HandleDefCauss: handleDefCauss,
					DefCaussInfo:   colInfo,
					analyzeInfo:    info,
					TblInfo:        tbl.BlockInfo,
				})
			}
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeIndex(as *ast.AnalyzeBlockStmt, opts map[ast.AnalyzeOptionType]uint64) (Plan, error) {
	p := &Analyze{Opts: opts}
	tblInfo := as.BlockNames[0].BlockInfo
	physicalIDs, names, err := getPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	for _, idxName := range as.IndexNames {
		if isPrimaryIndex(idxName) {
			handleDefCauss := BuildHandleDefCaussForAnalyze(b.ctx, tblInfo)
			if handleDefCauss != nil {
				for i, id := range physicalIDs {
					info := analyzeInfo{DBName: as.BlockNames[0].Schema.O, BlockName: as.BlockNames[0].Name.O, PartitionName: names[i], BlockID: AnalyzeBlockID{PersistID: id, DefCauslectIDs: []int64{id}}, Incremental: as.Incremental}
					p.DefCausTasks = append(p.DefCausTasks, AnalyzeDeferredCausetsTask{HandleDefCauss: handleDefCauss, analyzeInfo: info, TblInfo: tblInfo})
				}
				continue
			}
		}
		idx := tblInfo.FindIndexByName(idxName.L)
		if idx == nil || idx.State != perceptron.StatePublic {
			return nil, ErrAnalyzeMissIndex.GenWithStackByArgs(idxName.O, tblInfo.Name.O)
		}
		for i, id := range physicalIDs {
			info := analyzeInfo{DBName: as.BlockNames[0].Schema.O, BlockName: as.BlockNames[0].Name.O, PartitionName: names[i], BlockID: AnalyzeBlockID{PersistID: id, DefCauslectIDs: []int64{id}}, Incremental: as.Incremental}
			p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, analyzeInfo: info, TblInfo: tblInfo})
		}
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeAllIndex(as *ast.AnalyzeBlockStmt, opts map[ast.AnalyzeOptionType]uint64) (Plan, error) {
	p := &Analyze{Opts: opts}
	tblInfo := as.BlockNames[0].BlockInfo
	physicalIDs, names, err := getPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == perceptron.StatePublic {
			for i, id := range physicalIDs {
				info := analyzeInfo{DBName: as.BlockNames[0].Schema.O, BlockName: as.BlockNames[0].Name.O, PartitionName: names[i], BlockID: AnalyzeBlockID{PersistID: id, DefCauslectIDs: []int64{id}}, Incremental: as.Incremental}
				p.IdxTasks = append(p.IdxTasks, AnalyzeIndexTask{IndexInfo: idx, analyzeInfo: info, TblInfo: tblInfo})
			}
		}
	}
	handleDefCauss := BuildHandleDefCaussForAnalyze(b.ctx, tblInfo)
	if handleDefCauss != nil {
		for i, id := range physicalIDs {
			info := analyzeInfo{DBName: as.BlockNames[0].Schema.O, BlockName: as.BlockNames[0].Name.O, PartitionName: names[i], BlockID: AnalyzeBlockID{PersistID: id, DefCauslectIDs: []int64{id}}, Incremental: as.Incremental}
			p.DefCausTasks = append(p.DefCausTasks, AnalyzeDeferredCausetsTask{HandleDefCauss: handleDefCauss, analyzeInfo: info, TblInfo: tblInfo})
		}
	}
	return p, nil
}

var cmSketchSizeLimit = ekv.TxnEntrySizeLimit / binary.MaxVarintLen32

var analyzeOptionLimit = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    1024,
	ast.AnalyzeOptNumTopN:       1024,
	ast.AnalyzeOptCMSketchWidth: cmSketchSizeLimit,
	ast.AnalyzeOptCMSketchDepth: cmSketchSizeLimit,
	ast.AnalyzeOptNumSamples:    100000,
}

var analyzeOptionDefault = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    256,
	ast.AnalyzeOptNumTopN:       20,
	ast.AnalyzeOptCMSketchWidth: 2048,
	ast.AnalyzeOptCMSketchDepth: 5,
	ast.AnalyzeOptNumSamples:    10000,
}

func handleAnalyzeOptions(opts []ast.AnalyzeOpt) (map[ast.AnalyzeOptionType]uint64, error) {
	optMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	for key, val := range analyzeOptionDefault {
		optMap[key] = val
	}
	for _, opt := range opts {
		if opt.Type == ast.AnalyzeOptNumTopN {
			if opt.Value > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("value of analyze option %s should not larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
		} else {
			if opt.Value == 0 || opt.Value > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("value of analyze option %s should be positive and not larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
		}
		optMap[opt.Type] = opt.Value
	}
	if optMap[ast.AnalyzeOptCMSketchWidth]*optMap[ast.AnalyzeOptCMSketchDepth] > cmSketchSizeLimit {
		return nil, errors.Errorf("cm sketch size(depth * width) should not larger than %d", cmSketchSizeLimit)
	}
	return optMap, nil
}

func (b *PlanBuilder) buildAnalyze(as *ast.AnalyzeBlockStmt) (Plan, error) {
	// If enable fast analyze, the storage must be einsteindb.CausetStorage.
	if _, isEinsteinDBStorage := b.ctx.GetStore().(einsteindb.CausetStorage); !isEinsteinDBStorage && b.ctx.GetStochastikVars().EnableFastAnalyze {
		return nil, errors.Errorf("Only support fast analyze in einsteindb storage.")
	}
	for _, tbl := range as.BlockNames {
		user := b.ctx.GetStochastikVars().User
		var insertErr, selectErr error
		if user != nil {
			insertErr = ErrBlockaccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, tbl.Name.O)
			selectErr = ErrBlockaccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, tbl.Name.O)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.InsertPriv, tbl.Schema.O, tbl.Name.O, "", insertErr)
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SelectPriv, tbl.Schema.O, tbl.Name.O, "", selectErr)
	}
	opts, err := handleAnalyzeOptions(as.AnalyzeOpts)
	if err != nil {
		return nil, err
	}
	if as.IndexFlag {
		if len(as.IndexNames) == 0 {
			return b.buildAnalyzeAllIndex(as, opts)
		}
		return b.buildAnalyzeIndex(as, opts)
	}
	return b.buildAnalyzeBlock(as, opts)
}

func buildShowNextRowID() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(4)
	schemaReplicant.Append(buildDeferredCausetWithName("", "DB_NAME", allegrosql.TypeVarchar, allegrosql.MaxDatabaseNameLength))
	schemaReplicant.Append(buildDeferredCausetWithName("", "TABLE_NAME", allegrosql.TypeVarchar, allegrosql.MaxBlockNameLength))
	schemaReplicant.Append(buildDeferredCausetWithName("", "COLUMN_NAME", allegrosql.TypeVarchar, allegrosql.MaxDeferredCausetNameLength))
	schemaReplicant.Append(buildDeferredCausetWithName("", "NEXT_GLOBAL_ROW_ID", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "ID_TYPE", allegrosql.TypeVarchar, 15))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildShowDBSFields() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(6)
	schemaReplicant.Append(buildDeferredCausetWithName("", "SCHEMA_VER", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "OWNER_ID", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "OWNER_ADDRESS", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName("", "RUNNING_JOBS", allegrosql.TypeVarchar, 256))
	schemaReplicant.Append(buildDeferredCausetWithName("", "SELF_ID", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "QUERY", allegrosql.TypeVarchar, 256))

	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildRecoverIndexFields() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(2)
	schemaReplicant.Append(buildDeferredCausetWithName("", "ADDED_COUNT", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "SCAN_COUNT", allegrosql.TypeLonglong, 4))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildCleanupIndexFields() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(1)
	schemaReplicant.Append(buildDeferredCausetWithName("", "REMOVED_COUNT", allegrosql.TypeLonglong, 4))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildShowDBSJobsFields() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(11)
	schemaReplicant.Append(buildDeferredCausetWithName("", "JOB_ID", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "DB_NAME", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "TABLE_NAME", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "JOB_TYPE", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "SCHEMA_STATE", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "SCHEMA_ID", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "TABLE_ID", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "ROW_COUNT", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "START_TIME", allegrosql.TypeDatetime, 19))
	schemaReplicant.Append(buildDeferredCausetWithName("", "END_TIME", allegrosql.TypeDatetime, 19))
	schemaReplicant.Append(buildDeferredCausetWithName("", "STATE", allegrosql.TypeVarchar, 64))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildBlockRegionsSchema() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(11)
	schemaReplicant.Append(buildDeferredCausetWithName("", "REGION_ID", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "START_KEY", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "END_KEY", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "LEADER_ID", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "LEADER_STORE_ID", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "PEERS", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "SCATTERING", allegrosql.TypeTiny, 1))
	schemaReplicant.Append(buildDeferredCausetWithName("", "WRITTEN_BYTES", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "READ_BYTES", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "APPROXIMATE_SIZE(MB)", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "APPROXIMATE_KEYS", allegrosql.TypeLonglong, 4))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildSplitRegionsSchema() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(2)
	schemaReplicant.Append(buildDeferredCausetWithName("", "TOTAL_SPLIT_REGION", allegrosql.TypeLonglong, 4))
	schemaReplicant.Append(buildDeferredCausetWithName("", "SCATTER_FINISH_RATIO", allegrosql.TypeDouble, 8))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildShowDBSJobQueriesFields() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(1)
	schemaReplicant.Append(buildDeferredCausetWithName("", "QUERY", allegrosql.TypeVarchar, 256))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildShowSlowSchema() (*expression.Schema, types.NameSlice) {
	longlongSize, _ := allegrosql.GetDefaultFieldLengthAndDecimal(allegrosql.TypeLonglong)
	tinySize, _ := allegrosql.GetDefaultFieldLengthAndDecimal(allegrosql.TypeTiny)
	timestampSize, _ := allegrosql.GetDefaultFieldLengthAndDecimal(allegrosql.TypeTimestamp)
	durationSize, _ := allegrosql.GetDefaultFieldLengthAndDecimal(allegrosql.TypeDuration)

	schemaReplicant := newDeferredCausetsWithNames(11)
	schemaReplicant.Append(buildDeferredCausetWithName("", "ALLEGROALLEGROSQL", allegrosql.TypeVarchar, 4096))
	schemaReplicant.Append(buildDeferredCausetWithName("", "START", allegrosql.TypeTimestamp, timestampSize))
	schemaReplicant.Append(buildDeferredCausetWithName("", "DURATION", allegrosql.TypeDuration, durationSize))
	schemaReplicant.Append(buildDeferredCausetWithName("", "DETAILS", allegrosql.TypeVarchar, 256))
	schemaReplicant.Append(buildDeferredCausetWithName("", "SUCC", allegrosql.TypeTiny, tinySize))
	schemaReplicant.Append(buildDeferredCausetWithName("", "CONN_ID", allegrosql.TypeLonglong, longlongSize))
	schemaReplicant.Append(buildDeferredCausetWithName("", "TRANSACTION_TS", allegrosql.TypeLonglong, longlongSize))
	schemaReplicant.Append(buildDeferredCausetWithName("", "USER", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName("", "EDB", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "TABLE_IDS", allegrosql.TypeVarchar, 256))
	schemaReplicant.Append(buildDeferredCausetWithName("", "INDEX_IDS", allegrosql.TypeVarchar, 256))
	schemaReplicant.Append(buildDeferredCausetWithName("", "INTERNAL", allegrosql.TypeTiny, tinySize))
	schemaReplicant.Append(buildDeferredCausetWithName("", "DIGEST", allegrosql.TypeVarchar, 64))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildCancelDBSJobsFields() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(2)
	schemaReplicant.Append(buildDeferredCausetWithName("", "JOB_ID", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "RESULT", allegrosql.TypeVarchar, 128))

	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildBRIESchema() (*expression.Schema, types.NameSlice) {
	longlongSize, _ := allegrosql.GetDefaultFieldLengthAndDecimal(allegrosql.TypeLonglong)
	datetimeSize, _ := allegrosql.GetDefaultFieldLengthAndDecimal(allegrosql.TypeDatetime)

	schemaReplicant := newDeferredCausetsWithNames(5)
	schemaReplicant.Append(buildDeferredCausetWithName("", "Destination", allegrosql.TypeVarchar, 255))
	schemaReplicant.Append(buildDeferredCausetWithName("", "Size", allegrosql.TypeLonglong, longlongSize))
	schemaReplicant.Append(buildDeferredCausetWithName("", "BackupTS", allegrosql.TypeLonglong, longlongSize))
	schemaReplicant.Append(buildDeferredCausetWithName("", "Queue Time", allegrosql.TypeDatetime, datetimeSize))
	schemaReplicant.Append(buildDeferredCausetWithName("", "Execution Time", allegrosql.TypeDatetime, datetimeSize))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildShowTelemetrySchema() (*expression.Schema, types.NameSlice) {
	schemaReplicant := newDeferredCausetsWithNames(1)
	schemaReplicant.Append(buildDeferredCausetWithName("", "TRACKING_ID", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName("", "LAST_STATUS", allegrosql.TypeString, allegrosql.MaxBlobWidth))
	schemaReplicant.Append(buildDeferredCausetWithName("", "DATA_PREVIEW", allegrosql.TypeString, allegrosql.MaxBlobWidth))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildDeferredCausetWithName(blockName, name string, tp byte, size int) (*expression.DeferredCauset, *types.FieldName) {
	cs, cl := types.DefaultCharsetForType(tp)
	flag := allegrosql.UnsignedFlag
	if tp == allegrosql.TypeVarchar || tp == allegrosql.TypeBlob {
		cs = charset.CharsetUTF8MB4
		cl = charset.DefCauslationUTF8MB4
		flag = 0
	}

	fieldType := &types.FieldType{
		Charset:     cs,
		DefCauslate: cl,
		Tp:          tp,
		Flen:        size,
		Flag:        flag,
	}
	return &expression.DeferredCauset{
		RetType: fieldType,
	}, &types.FieldName{DBName: util2.InformationSchemaName, TblName: perceptron.NewCIStr(blockName), DefCausName: perceptron.NewCIStr(name)}
}

type columnsWithNames struct {
	defcaus []*expression.DeferredCauset
	names   types.NameSlice
}

func newDeferredCausetsWithNames(cap int) *columnsWithNames {
	return &columnsWithNames{
		defcaus: make([]*expression.DeferredCauset, 0, 2),
		names:   make(types.NameSlice, 0, 2),
	}
}

func (cwn *columnsWithNames) Append(col *expression.DeferredCauset, name *types.FieldName) {
	cwn.defcaus = append(cwn.defcaus, col)
	cwn.names = append(cwn.names, name)
}

func (cwn *columnsWithNames) col2Schema() *expression.Schema {
	return expression.NewSchema(cwn.defcaus...)
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.LogicAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

func (b *PlanBuilder) buildShow(ctx context.Context, show *ast.ShowStmt) (Plan, error) {
	p := LogicalShow{
		ShowContents: ShowContents{
			Tp:                show.Tp,
			DBName:            show.DBName,
			Block:             show.Block,
			DeferredCauset:    show.DeferredCauset,
			IndexName:         show.IndexName,
			Flag:              show.Flag,
			User:              show.User,
			Roles:             show.Roles,
			Full:              show.Full,
			IfNotExists:       show.IfNotExists,
			GlobalSINTERLOCKe: show.GlobalSINTERLOCKe,
			Extended:          show.Extended,
		},
	}.Init(b.ctx)
	isView := false
	isSequence := false
	switch show.Tp {
	case ast.ShowBlocks, ast.ShowBlockStatus:
		if p.DBName == "" {
			return nil, ErrNoDB
		}
	case ast.ShowCreateBlock, ast.ShowCreateSequence:
		user := b.ctx.GetStochastikVars().User
		var err error
		if user != nil {
			err = ErrBlockaccessDenied.GenWithStackByArgs("SHOW", user.AuthUsername, user.AuthHostname, show.Block.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.AllPrivMask, show.Block.Schema.L, show.Block.Name.L, "", err)
		if block, err := b.is.BlockByName(show.Block.Schema, show.Block.Name); err == nil {
			isView = block.Meta().IsView()
			isSequence = block.Meta().IsSequence()
		}
	case ast.ShowCreateView:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SHOW VIEW")
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.ShowViewPriv, show.Block.Schema.L, show.Block.Name.L, "", err)
	case ast.ShowBackups, ast.ShowRestores:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", err)
	case ast.ShowBlockNextRowId:
		p := &ShowNextRowID{BlockName: show.Block}
		p.setSchemaAndNames(buildShowNextRowID())
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SelectPriv, show.Block.Schema.L, show.Block.Name.L, "", ErrPrivilegeCheckFail)
		return p, nil
	case ast.ShowStatsBuckets, ast.ShowStatsHistograms, ast.ShowStatsMeta, ast.ShowStatsHealthy:
		user := b.ctx.GetStochastikVars().User
		var err error
		if user != nil {
			err = ErrDBaccessDenied.GenWithStackByArgs(user.AuthUsername, user.AuthHostname, allegrosql.SystemDB)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SelectPriv, allegrosql.SystemDB, "", "", err)
	}
	schemaReplicant, names := buildShowSchema(show, isView, isSequence)
	p.SetSchema(schemaReplicant)
	p.names = names
	for _, col := range p.schemaReplicant.DeferredCausets {
		col.UniqueID = b.ctx.GetStochastikVars().AllocPlanDeferredCausetID()
	}
	var err error
	var np LogicalPlan
	np = p
	if show.Pattern != nil {
		show.Pattern.Expr = &ast.DeferredCausetNameExpr{
			Name: &ast.DeferredCausetName{Name: p.OutputNames()[0].DefCausName},
		}
		np, err = b.buildSelection(ctx, np, show.Pattern, nil)
		if err != nil {
			return nil, err
		}
	}
	if show.Where != nil {
		np, err = b.buildSelection(ctx, np, show.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if np != p {
		b.optFlag |= flagEliminateProjection
		fieldsLen := len(p.schemaReplicant.DeferredCausets)
		proj := LogicalProjection{Exprs: make([]expression.Expression, 0, fieldsLen)}.Init(b.ctx, 0)
		schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, fieldsLen)...)
		for _, col := range p.schemaReplicant.DeferredCausets {
			proj.Exprs = append(proj.Exprs, col)
			newDefCaus := col.Clone().(*expression.DeferredCauset)
			newDefCaus.UniqueID = b.ctx.GetStochastikVars().AllocPlanDeferredCausetID()
			schemaReplicant.Append(newDefCaus)
		}
		proj.SetSchema(schemaReplicant)
		proj.SetChildren(np)
		proj.SetOutputNames(np.OutputNames())
		np = proj
	}
	if show.Tp == ast.ShowVariables || show.Tp == ast.ShowStatus {
		b.curClause = orderByClause
		orderByDefCaus := np.Schema().DeferredCausets[0].Clone().(*expression.DeferredCauset)
		sort := LogicalSort{
			ByItems: []*soliton.ByItems{{Expr: orderByDefCaus}},
		}.Init(b.ctx, b.getSelectOffset())
		sort.SetChildren(np)
		np = sort
	}
	return np, nil
}

func (b *PlanBuilder) buildSimple(node ast.StmtNode) (Plan, error) {
	p := &Simple{Statement: node}

	switch raw := node.(type) {
	case *ast.FlushStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("RELOAD")
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.ReloadPriv, "", "", "", err)
	case *ast.AlterInstanceStmt:
		err := ErrSpecificAccessDenied.GenWithStack("ALTER INSTANCE")
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", err)
	case *ast.AlterUserStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.CreateUserPriv, "", "", "", err)
	case *ast.GrantStmt:
		if b.ctx.GetStochastikVars().CurrentDB == "" && raw.Level.DBName == "" {
			if raw.Level.Level == ast.GrantLevelBlock {
				return nil, ErrNoDB
			}
		}
		b.visitInfo = collectVisitInfoFromGrantStmt(b.ctx, b.visitInfo, raw)
	case *ast.BRIEStmt:
		p.setSchemaAndNames(buildBRIESchema())
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", err)
	case *ast.GrantRoleStmt, *ast.RevokeRoleStmt:
		err := ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", err)
	case *ast.RevokeStmt:
		b.visitInfo = collectVisitInfoFromRevokeStmt(b.ctx, b.visitInfo, raw)
	case *ast.KillStmt:
		// If you have the SUPER privilege, you can kill all threads and statements.
		// Otherwise, you can kill only your own threads and statements.
		sm := b.ctx.GetStochastikManager()
		if sm != nil {
			if pi, ok := sm.GetProcessInfo(raw.ConnectionID); ok {
				loginUser := b.ctx.GetStochastikVars().User
				if pi.User != loginUser.Username {
					b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", nil)
				}
			}
		}
	case *ast.UseStmt:
		if raw.DBName == "" {
			return nil, ErrNoDB
		}
	case *ast.ShutdownStmt:
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.ShutdownPriv, "", "", "", nil)
	case *ast.CreateStatisticsStmt:
		var selectErr, insertErr error
		user := b.ctx.GetStochastikVars().User
		if user != nil {
			selectErr = ErrBlockaccessDenied.GenWithStackByArgs("CREATE STATISTICS", user.AuthUsername,
				user.AuthHostname, raw.Block.Name.L)
			insertErr = ErrBlockaccessDenied.GenWithStackByArgs("CREATE STATISTICS", user.AuthUsername,
				user.AuthHostname, "stats_extended")
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SelectPriv, raw.Block.Schema.L,
			raw.Block.Name.L, "", selectErr)
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.InsertPriv, allegrosql.SystemDB,
			"stats_extended", "", insertErr)
	case *ast.DropStatisticsStmt:
		var err error
		user := b.ctx.GetStochastikVars().User
		if user != nil {
			err = ErrBlockaccessDenied.GenWithStackByArgs("DROP STATISTICS", user.AuthUsername,
				user.AuthHostname, "stats_extended")
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.UFIDelatePriv, allegrosql.SystemDB,
			"stats_extended", "", err)
	}
	return p, nil
}

func collectVisitInfoFromRevokeStmt(sctx stochastikctx.Context, vi []visitInfo, stmt *ast.RevokeStmt) []visitInfo {
	// To use REVOKE, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	blockName := stmt.Level.BlockName
	if dbName == "" {
		dbName = sctx.GetStochastikVars().CurrentDB
	}
	vi = appendVisitInfo(vi, allegrosql.GrantPriv, dbName, blockName, "", nil)

	var allPrivs []allegrosql.PrivilegeType
	for _, item := range stmt.Privs {
		if item.Priv == allegrosql.AllPriv {
			switch stmt.Level.Level {
			case ast.GrantLevelGlobal:
				allPrivs = allegrosql.AllGlobalPrivs
			case ast.GrantLevelDB:
				allPrivs = allegrosql.AllDBPrivs
			case ast.GrantLevelBlock:
				allPrivs = allegrosql.AllBlockPrivs
			}
			break
		}
		vi = appendVisitInfo(vi, item.Priv, dbName, blockName, "", nil)
	}

	for _, priv := range allPrivs {
		vi = appendVisitInfo(vi, priv, dbName, blockName, "", nil)
	}

	return vi
}

func collectVisitInfoFromGrantStmt(sctx stochastikctx.Context, vi []visitInfo, stmt *ast.GrantStmt) []visitInfo {
	// To use GRANT, you must have the GRANT OPTION privilege,
	// and you must have the privileges that you are granting.
	dbName := stmt.Level.DBName
	blockName := stmt.Level.BlockName
	if dbName == "" {
		dbName = sctx.GetStochastikVars().CurrentDB
	}
	vi = appendVisitInfo(vi, allegrosql.GrantPriv, dbName, blockName, "", nil)

	var allPrivs []allegrosql.PrivilegeType
	for _, item := range stmt.Privs {
		if item.Priv == allegrosql.AllPriv {
			switch stmt.Level.Level {
			case ast.GrantLevelGlobal:
				allPrivs = allegrosql.AllGlobalPrivs
			case ast.GrantLevelDB:
				allPrivs = allegrosql.AllDBPrivs
			case ast.GrantLevelBlock:
				allPrivs = allegrosql.AllBlockPrivs
			}
			break
		}
		vi = appendVisitInfo(vi, item.Priv, dbName, blockName, "", nil)
	}

	for _, priv := range allPrivs {
		vi = appendVisitInfo(vi, priv, dbName, blockName, "", nil)
	}

	return vi
}

func (b *PlanBuilder) getDefaultValue(col *block.DeferredCauset) (*expression.Constant, error) {
	var (
		value types.Causet
		err   error
	)
	if col.DefaultIsExpr && col.DefaultExpr != nil {
		value, err = block.EvalDefCausDefaultExpr(b.ctx, col.ToInfo(), col.DefaultExpr)
	} else {
		value, err = block.GetDefCausDefaultValue(b.ctx, col.ToInfo())
	}
	if err != nil {
		return nil, err
	}
	return &expression.Constant{Value: value, RetType: &col.FieldType}, nil
}

func (b *PlanBuilder) findDefaultValue(defcaus []*block.DeferredCauset, name *ast.DeferredCausetName) (*expression.Constant, error) {
	for _, col := range defcaus {
		if col.Name.L == name.Name.L {
			return b.getDefaultValue(col)
		}
	}
	return nil, ErrUnknownDeferredCauset.GenWithStackByArgs(name.Name.O, "field_list")
}

// resolveGeneratedDeferredCausets resolves generated columns with their generation
// expressions respectively. onDups indicates which columns are in on-duplicate list.
func (b *PlanBuilder) resolveGeneratedDeferredCausets(ctx context.Context, columns []*block.DeferredCauset, onDups map[string]struct{}, mockPlan LogicalPlan) (igc InsertGeneratedDeferredCausets, err error) {
	for _, column := range columns {
		if !column.IsGenerated() {
			continue
		}
		columnName := &ast.DeferredCausetName{Name: column.Name}
		columnName.SetText(column.Name.O)

		idx, err := expression.FindFieldName(mockPlan.OutputNames(), columnName)
		if err != nil {
			return igc, err
		}
		colExpr := mockPlan.Schema().DeferredCausets[idx]

		expr, _, err := b.rewrite(ctx, column.GeneratedExpr, mockPlan, nil, true)
		if err != nil {
			return igc, err
		}

		igc.DeferredCausets = append(igc.DeferredCausets, columnName)
		igc.Exprs = append(igc.Exprs, expr)
		if onDups == nil {
			continue
		}
		for dep := range column.Dependences {
			if _, ok := onDups[dep]; ok {
				assign := &expression.Assignment{DefCaus: colExpr, DefCausName: column.Name, Expr: expr}
				igc.OnDuplicates = append(igc.OnDuplicates, assign)
				break
			}
		}
	}
	return igc, nil
}

func (b *PlanBuilder) buildInsert(ctx context.Context, insert *ast.InsertStmt) (Plan, error) {
	ts, ok := insert.Block.BlockRefs.Left.(*ast.BlockSource)
	if !ok {
		return nil, schemareplicant.ErrBlockNotExists.GenWithStackByArgs()
	}
	tn, ok := ts.Source.(*ast.BlockName)
	if !ok {
		return nil, schemareplicant.ErrBlockNotExists.GenWithStackByArgs()
	}
	blockInfo := tn.BlockInfo
	if blockInfo.IsView() {
		err := errors.Errorf("insert into view %s is not supported now.", blockInfo.Name.O)
		if insert.IsReplace {
			err = errors.Errorf("replace into view %s is not supported now.", blockInfo.Name.O)
		}
		return nil, err
	}
	if blockInfo.IsSequence() {
		err := errors.Errorf("insert into sequence %s is not supported now.", blockInfo.Name.O)
		if insert.IsReplace {
			err = errors.Errorf("replace into sequence %s is not supported now.", blockInfo.Name.O)
		}
		return nil, err
	}
	// Build Schema with DBName otherwise DeferredCausetRef with DBName cannot match any DeferredCauset in Schema.
	schemaReplicant, names, err := expression.BlockInfo2SchemaAndNames(b.ctx, tn.Schema, blockInfo)
	if err != nil {
		return nil, err
	}
	blockInPlan, ok := b.is.BlockByID(blockInfo.ID)
	if !ok {
		return nil, errors.Errorf("Can't get block %s.", blockInfo.Name.O)
	}

	insertPlan := Insert{
		Block:             blockInPlan,
		DeferredCausets:   insert.DeferredCausets,
		blockSchema:       schemaReplicant,
		blockDefCausNames: names,
		IsReplace:         insert.IsReplace,
	}.Init(b.ctx)

	if blockInfo.GetPartitionInfo() != nil && len(insert.PartitionNames) != 0 {
		givenPartitionSets := make(map[int64]struct{}, len(insert.PartitionNames))
		// check partition by name.
		for _, name := range insert.PartitionNames {
			id, err := blocks.FindPartitionByName(blockInfo, name.L)
			if err != nil {
				return nil, err
			}
			givenPartitionSets[id] = struct{}{}
		}
		pt := blockInPlan.(block.PartitionedBlock)
		insertPlan.Block = blocks.NewPartitionBlockithGivenSets(pt, givenPartitionSets)
	} else if len(insert.PartitionNames) != 0 {
		return nil, ErrPartitionClauseOnNonpartitioned
	}

	var authErr error
	if b.ctx.GetStochastikVars().User != nil {
		authErr = ErrBlockaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetStochastikVars().User.AuthUsername,
			b.ctx.GetStochastikVars().User.AuthHostname, blockInfo.Name.L)
	}

	b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.InsertPriv, tn.DBInfo.Name.L,
		blockInfo.Name.L, "", authErr)

	mockBlockPlan := LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
	mockBlockPlan.SetSchema(insertPlan.blockSchema)
	mockBlockPlan.names = insertPlan.blockDefCausNames

	checkRefDeferredCauset := func(n ast.Node) ast.Node {
		if insertPlan.NeedFillDefaultValue {
			return n
		}
		switch n.(type) {
		case *ast.DeferredCausetName, *ast.DeferredCausetNameExpr:
			insertPlan.NeedFillDefaultValue = true
		}
		return n
	}

	if len(insert.Setlist) > 0 {
		// Branch for `INSERT ... SET ...`.
		err := b.buildSetValuesOfInsert(ctx, insert, insertPlan, mockBlockPlan, checkRefDeferredCauset)
		if err != nil {
			return nil, err
		}
	} else if len(insert.Lists) > 0 {
		// Branch for `INSERT ... VALUES ...`.
		err := b.buildValuesListOfInsert(ctx, insert, insertPlan, mockBlockPlan, checkRefDeferredCauset)
		if err != nil {
			return nil, err
		}
	} else {
		// Branch for `INSERT ... SELECT ...`.
		err := b.buildSelectPlanOfInsert(ctx, insert, insertPlan)
		if err != nil {
			return nil, err
		}
	}

	mockBlockPlan.SetSchema(insertPlan.Schema4OnDuplicate)
	mockBlockPlan.names = insertPlan.names4OnDuplicate

	onDupDefCausSet, err := insertPlan.resolveOnDuplicate(insert.OnDuplicate, blockInfo, func(node ast.ExprNode) (expression.Expression, error) {
		return b.rewriteInsertOnDuplicateUFIDelate(ctx, node, mockBlockPlan, insertPlan)
	})
	if err != nil {
		return nil, err
	}

	// Calculate generated columns.
	mockBlockPlan.schemaReplicant = insertPlan.blockSchema
	mockBlockPlan.names = insertPlan.blockDefCausNames
	insertPlan.GenDefCauss, err = b.resolveGeneratedDeferredCausets(ctx, insertPlan.Block.DefCauss(), onDupDefCausSet, mockBlockPlan)
	if err != nil {
		return nil, err
	}

	err = insertPlan.ResolveIndices()
	return insertPlan, err
}

func (p *Insert) resolveOnDuplicate(onDup []*ast.Assignment, tblInfo *perceptron.BlockInfo, yield func(ast.ExprNode) (expression.Expression, error)) (map[string]struct{}, error) {
	onDupDefCausSet := make(map[string]struct{}, len(onDup))
	colMap := make(map[string]*block.DeferredCauset, len(p.Block.DefCauss()))
	for _, col := range p.Block.DefCauss() {
		colMap[col.Name.L] = col
	}
	for _, assign := range onDup {
		// Check whether the column to be uFIDelated exists in the source block.
		idx, err := expression.FindFieldName(p.blockDefCausNames, assign.DeferredCauset)
		if err != nil {
			return nil, err
		} else if idx < 0 {
			return nil, ErrUnknownDeferredCauset.GenWithStackByArgs(assign.DeferredCauset.OrigDefCausName(), "field list")
		}

		column := colMap[assign.DeferredCauset.Name.L]
		if column.Hidden {
			return nil, ErrUnknownDeferredCauset.GenWithStackByArgs(column.Name, clauseMsg[fieldList])
		}
		// Check whether the column to be uFIDelated is the generated column.
		defaultExpr := extractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.DeferredCauset
		}
		// Note: For INSERT, REPLACE, and UFIDelATE, if a generated column is inserted into, replaced, or uFIDelated explicitly, the only permitted value is DEFAULT.
		// see https://dev.allegrosql.com/doc/refman/8.0/en/create-block-generated-columns.html
		if column.IsGenerated() {
			if defaultExpr != nil {
				continue
			}
			return nil, ErrBadGeneratedDeferredCauset.GenWithStackByArgs(assign.DeferredCauset.Name.O, tblInfo.Name.O)
		}

		onDupDefCausSet[column.Name.L] = struct{}{}

		expr, err := yield(assign.Expr)
		if err != nil {
			return nil, err
		}

		p.OnDuplicate = append(p.OnDuplicate, &expression.Assignment{
			DefCaus:     p.blockSchema.DeferredCausets[idx],
			DefCausName: p.blockDefCausNames[idx].DefCausName,
			Expr:        expr,
		})
	}
	return onDupDefCausSet, nil
}

func (b *PlanBuilder) getAffectDefCauss(insertStmt *ast.InsertStmt, insertPlan *Insert) (affectedValuesDefCauss []*block.DeferredCauset, err error) {
	if len(insertStmt.DeferredCausets) > 0 {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name (col_name [, col_name] ...) {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name (col_name [, col_name] ...) SELECT ...`.
		colName := make([]string, 0, len(insertStmt.DeferredCausets))
		for _, col := range insertStmt.DeferredCausets {
			colName = append(colName, col.Name.O)
		}
		var missingDefCausName string
		affectedValuesDefCauss, missingDefCausName = block.FindDefCauss(insertPlan.Block.VisibleDefCauss(), colName, insertPlan.Block.Meta().PKIsHandle)
		if missingDefCausName != "" {
			return nil, ErrUnknownDeferredCauset.GenWithStackByArgs(missingDefCausName, clauseMsg[fieldList])
		}
	} else if len(insertStmt.Setlist) == 0 {
		// This branch is for the following scenarios:
		// 1. `INSERT INTO tbl_name {VALUES | VALUE} (value_list) [, (value_list)] ...`,
		// 2. `INSERT INTO tbl_name SELECT ...`.
		affectedValuesDefCauss = insertPlan.Block.VisibleDefCauss()
	}
	return affectedValuesDefCauss, nil
}

func (b *PlanBuilder) buildSetValuesOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert, mockBlockPlan *LogicalBlockDual, checkRefDeferredCauset func(n ast.Node) ast.Node) error {
	blockInfo := insertPlan.Block.Meta()
	colNames := make([]string, 0, len(insert.Setlist))
	exprDefCauss := make([]*expression.DeferredCauset, 0, len(insert.Setlist))
	for _, assign := range insert.Setlist {
		idx, err := expression.FindFieldName(insertPlan.blockDefCausNames, assign.DeferredCauset)
		if err != nil {
			return err
		}
		if idx < 0 {
			return errors.Errorf("Can't find column %s", assign.DeferredCauset)
		}
		colNames = append(colNames, assign.DeferredCauset.Name.L)
		exprDefCauss = append(exprDefCauss, insertPlan.blockSchema.DeferredCausets[idx])
	}

	// Check whether the column to be uFIDelated is the generated column.
	tDefCauss, missingDefCausName := block.FindDefCauss(insertPlan.Block.VisibleDefCauss(), colNames, blockInfo.PKIsHandle)
	if missingDefCausName != "" {
		return ErrUnknownDeferredCauset.GenWithStackByArgs(missingDefCausName, clauseMsg[fieldList])
	}
	generatedDeferredCausets := make(map[string]struct{}, len(tDefCauss))
	for _, tDefCaus := range tDefCauss {
		if tDefCaus.IsGenerated() {
			generatedDeferredCausets[tDefCaus.Name.L] = struct{}{}
		}
	}

	insertPlan.AllAssignmentsAreConstant = true
	for i, assign := range insert.Setlist {
		defaultExpr := extractDefaultExpr(assign.Expr)
		if defaultExpr != nil {
			defaultExpr.Name = assign.DeferredCauset
		}
		// Note: For INSERT, REPLACE, and UFIDelATE, if a generated column is inserted into, replaced, or uFIDelated explicitly, the only permitted value is DEFAULT.
		// see https://dev.allegrosql.com/doc/refman/8.0/en/create-block-generated-columns.html
		if _, ok := generatedDeferredCausets[assign.DeferredCauset.Name.L]; ok {
			if defaultExpr != nil {
				continue
			}
			return ErrBadGeneratedDeferredCauset.GenWithStackByArgs(assign.DeferredCauset.Name.O, blockInfo.Name.O)
		}
		b.curClause = fieldList
		// subquery in insert values should not reference upper sINTERLOCKe
		usingPlan := mockBlockPlan
		if _, ok := assign.Expr.(*ast.SubqueryExpr); ok {
			usingPlan = LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
		}
		expr, _, err := b.rewriteWithPreprocess(ctx, assign.Expr, usingPlan, nil, nil, true, checkRefDeferredCauset)
		if err != nil {
			return err
		}
		if insertPlan.AllAssignmentsAreConstant {
			_, isConstant := expr.(*expression.Constant)
			insertPlan.AllAssignmentsAreConstant = isConstant
		}

		insertPlan.SetList = append(insertPlan.SetList, &expression.Assignment{
			DefCaus:     exprDefCauss[i],
			DefCausName: perceptron.NewCIStr(colNames[i]),
			Expr:        expr,
		})
	}
	insertPlan.Schema4OnDuplicate = insertPlan.blockSchema
	insertPlan.names4OnDuplicate = insertPlan.blockDefCausNames
	return nil
}

func (b *PlanBuilder) buildValuesListOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert, mockBlockPlan *LogicalBlockDual, checkRefDeferredCauset func(n ast.Node) ast.Node) error {
	affectedValuesDefCauss, err := b.getAffectDefCauss(insert, insertPlan)
	if err != nil {
		return err
	}

	// If value_list and col_list are empty and we have a generated column, we can still write data to this block.
	// For example, insert into t values(); can be executed successfully if t has a generated column.
	if len(insert.DeferredCausets) > 0 || len(insert.Lists[0]) > 0 {
		// If value_list or col_list is not empty, the length of value_list should be the same with that of col_list.
		if len(insert.Lists[0]) != len(affectedValuesDefCauss) {
			return ErrWrongValueCountOnRow.GenWithStackByArgs(1)
		}
	}

	insertPlan.AllAssignmentsAreConstant = true
	totalBlockDefCauss := insertPlan.Block.DefCauss()
	for i, valuesItem := range insert.Lists {
		// The length of all the value_list should be the same.
		// "insert into t values (), ()" is valid.
		// "insert into t values (), (1)" is not valid.
		// "insert into t values (1), ()" is not valid.
		// "insert into t values (1,2), (1)" is not valid.
		if i > 0 && len(insert.Lists[i-1]) != len(insert.Lists[i]) {
			return ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
		}
		exprList := make([]expression.Expression, 0, len(valuesItem))
		for j, valueItem := range valuesItem {
			var expr expression.Expression
			var err error
			var generatedDeferredCausetWithDefaultExpr bool
			col := affectedValuesDefCauss[j]
			switch x := valueItem.(type) {
			case *ast.DefaultExpr:
				if col.IsGenerated() {
					if x.Name != nil {
						return ErrBadGeneratedDeferredCauset.GenWithStackByArgs(col.Name.O, insertPlan.Block.Meta().Name.O)
					}
					generatedDeferredCausetWithDefaultExpr = true
					break
				}
				if x.Name != nil {
					expr, err = b.findDefaultValue(totalBlockDefCauss, x.Name)
				} else {
					expr, err = b.getDefaultValue(affectedValuesDefCauss[j])
				}
			case *driver.ValueExpr:
				expr = &expression.Constant{
					Value:   x.Causet,
					RetType: &x.Type,
				}
			default:
				b.curClause = fieldList
				// subquery in insert values should not reference upper sINTERLOCKe
				usingPlan := mockBlockPlan
				if _, ok := valueItem.(*ast.SubqueryExpr); ok {
					usingPlan = LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
				}
				expr, _, err = b.rewriteWithPreprocess(ctx, valueItem, usingPlan, nil, nil, true, checkRefDeferredCauset)
			}
			if err != nil {
				return err
			}
			if insertPlan.AllAssignmentsAreConstant {
				_, isConstant := expr.(*expression.Constant)
				insertPlan.AllAssignmentsAreConstant = isConstant
			}
			// Note: For INSERT, REPLACE, and UFIDelATE, if a generated column is inserted into, replaced, or uFIDelated explicitly, the only permitted value is DEFAULT.
			// see https://dev.allegrosql.com/doc/refman/8.0/en/create-block-generated-columns.html
			if col.IsGenerated() {
				if generatedDeferredCausetWithDefaultExpr {
					continue
				}
				return ErrBadGeneratedDeferredCauset.GenWithStackByArgs(col.Name.O, insertPlan.Block.Meta().Name.O)
			}
			exprList = append(exprList, expr)
		}
		insertPlan.Lists = append(insertPlan.Lists, exprList)
	}
	insertPlan.Schema4OnDuplicate = insertPlan.blockSchema
	insertPlan.names4OnDuplicate = insertPlan.blockDefCausNames
	return nil
}

func (b *PlanBuilder) buildSelectPlanOfInsert(ctx context.Context, insert *ast.InsertStmt, insertPlan *Insert) error {
	affectedValuesDefCauss, err := b.getAffectDefCauss(insert, insertPlan)
	if err != nil {
		return err
	}
	selectPlan, err := b.Build(ctx, insert.Select)
	if err != nil {
		return err
	}

	// Check to guarantee that the length of the event returned by select is equal to that of affectedValuesDefCauss.
	if selectPlan.Schema().Len() != len(affectedValuesDefCauss) {
		return ErrWrongValueCountOnRow.GenWithStackByArgs(1)
	}

	// Check to guarantee that there's no generated column.
	// This check should be done after the above one to make its behavior compatible with MyALLEGROSQL.
	// For example, block t has two columns, namely a and b, and b is a generated column.
	// "insert into t (b) select * from t" will raise an error that the column count is not matched.
	// "insert into t select * from t" will raise an error that there's a generated column in the column list.
	// If we do this check before the above one, "insert into t (b) select * from t" will raise an error
	// that there's a generated column in the column list.
	for _, col := range affectedValuesDefCauss {
		if col.IsGenerated() {
			return ErrBadGeneratedDeferredCauset.GenWithStackByArgs(col.Name.O, insertPlan.Block.Meta().Name.O)
		}
	}

	names := selectPlan.OutputNames()
	insertPlan.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag, selectPlan.(LogicalPlan))
	if err != nil {
		return err
	}

	// schema4NewRow is the schemaReplicant for the newly created data record based on
	// the result of the select statement.
	schema4NewRow := expression.NewSchema(make([]*expression.DeferredCauset, len(insertPlan.Block.DefCauss()))...)
	names4NewRow := make(types.NameSlice, len(insertPlan.Block.DefCauss()))
	// TODO: don't clone it.
	for i, selDefCaus := range insertPlan.SelectPlan.Schema().DeferredCausets {
		ordinal := affectedValuesDefCauss[i].Offset
		schema4NewRow.DeferredCausets[ordinal] = &expression.DeferredCauset{}
		*schema4NewRow.DeferredCausets[ordinal] = *selDefCaus

		schema4NewRow.DeferredCausets[ordinal].RetType = &types.FieldType{}
		*schema4NewRow.DeferredCausets[ordinal].RetType = affectedValuesDefCauss[i].FieldType

		names4NewRow[ordinal] = names[i]
	}
	for i := range schema4NewRow.DeferredCausets {
		if schema4NewRow.DeferredCausets[i] == nil {
			schema4NewRow.DeferredCausets[i] = &expression.DeferredCauset{UniqueID: insertPlan.ctx.GetStochastikVars().AllocPlanDeferredCausetID()}
			names4NewRow[i] = types.EmptyName
		}
	}
	insertPlan.Schema4OnDuplicate = expression.MergeSchema(insertPlan.blockSchema, schema4NewRow)
	insertPlan.names4OnDuplicate = append(insertPlan.blockDefCausNames.Shallow(), names4NewRow...)
	return nil
}

func (b *PlanBuilder) buildLoadData(ctx context.Context, ld *ast.LoadDataStmt) (Plan, error) {
	p := &LoadData{
		IsLocal:                    ld.IsLocal,
		OnDuplicate:                ld.OnDuplicate,
		Path:                       ld.Path,
		Block:                      ld.Block,
		DeferredCausets:            ld.DeferredCausets,
		FieldsInfo:                 ld.FieldsInfo,
		LinesInfo:                  ld.LinesInfo,
		IgnoreLines:                ld.IgnoreLines,
		DeferredCausetAssignments:  ld.DeferredCausetAssignments,
		DeferredCausetsAndUserVars: ld.DeferredCausetsAndUserVars,
	}
	user := b.ctx.GetStochastikVars().User
	var insertErr error
	if user != nil {
		insertErr = ErrBlockaccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, p.Block.Name.O)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.InsertPriv, p.Block.Schema.O, p.Block.Name.O, "", insertErr)
	blockInfo := p.Block.BlockInfo
	blockInPlan, ok := b.is.BlockByID(blockInfo.ID)
	if !ok {
		EDB := b.ctx.GetStochastikVars().CurrentDB
		return nil, schemareplicant.ErrBlockNotExists.GenWithStackByArgs(EDB, blockInfo.Name.O)
	}
	schemaReplicant, names, err := expression.BlockInfo2SchemaAndNames(b.ctx, perceptron.NewCIStr(""), blockInfo)
	if err != nil {
		return nil, err
	}
	mockBlockPlan := LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
	mockBlockPlan.SetSchema(schemaReplicant)
	mockBlockPlan.names = names

	p.GenDefCauss, err = b.resolveGeneratedDeferredCausets(ctx, blockInPlan.DefCauss(), nil, mockBlockPlan)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (b *PlanBuilder) buildLoadStats(ld *ast.LoadStatsStmt) Plan {
	p := &LoadStats{Path: ld.Path}
	return p
}

func (b *PlanBuilder) buildIndexAdvise(node *ast.IndexAdviseStmt) Plan {
	p := &IndexAdvise{
		IsLocal:     node.IsLocal,
		Path:        node.Path,
		MaxMinutes:  node.MaxMinutes,
		MaxIndexNum: node.MaxIndexNum,
		LinesInfo:   node.LinesInfo,
	}
	return p
}

func (b *PlanBuilder) buildSplitRegion(node *ast.SplitRegionStmt) (Plan, error) {
	if node.SplitSyntaxOpt != nil && node.SplitSyntaxOpt.HasPartition && node.Block.BlockInfo.Partition == nil {
		return nil, ErrPartitionClauseOnNonpartitioned
	}
	if len(node.IndexName.L) != 0 {
		return b.buildSplitIndexRegion(node)
	}
	return b.buildSplitBlockRegion(node)
}

func (b *PlanBuilder) buildSplitIndexRegion(node *ast.SplitRegionStmt) (Plan, error) {
	tblInfo := node.Block.BlockInfo
	indexInfo := tblInfo.FindIndexByName(node.IndexName.L)
	if indexInfo == nil {
		return nil, ErrKeyDoesNotExist.GenWithStackByArgs(node.IndexName, tblInfo.Name)
	}
	mockBlockPlan := LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
	schemaReplicant, names, err := expression.BlockInfo2SchemaAndNames(b.ctx, node.Block.Schema, tblInfo)
	if err != nil {
		return nil, err
	}
	mockBlockPlan.SetSchema(schemaReplicant)
	mockBlockPlan.names = names

	p := &SplitRegion{
		BlockInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
		IndexInfo:      indexInfo,
	}
	p.names = names
	p.setSchemaAndNames(buildSplitRegionsSchema())
	// Split index regions by user specified value lists.
	if len(node.SplitOpt.ValueLists) > 0 {
		indexValues := make([][]types.Causet, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			if len(valuesItem) > len(indexInfo.DeferredCausets) {
				return nil, ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
			}
			values, err := b.convertValue2DeferredCausetType(valuesItem, mockBlockPlan, indexInfo, tblInfo)
			if err != nil {
				return nil, err
			}
			indexValues = append(indexValues, values)
		}
		p.ValueLists = indexValues
		return p, nil
	}

	// Split index regions by lower, upper value.
	checkLowerUpperValue := func(valuesItem []ast.ExprNode, name string) ([]types.Causet, error) {
		if len(valuesItem) == 0 {
			return nil, errors.Errorf("Split index `%v` region %s value count should more than 0", indexInfo.Name, name)
		}
		if len(valuesItem) > len(indexInfo.DeferredCausets) {
			return nil, errors.Errorf("Split index `%v` region column count doesn't match value count at %v", indexInfo.Name, name)
		}
		return b.convertValue2DeferredCausetType(valuesItem, mockBlockPlan, indexInfo, tblInfo)
	}
	lowerValues, err := checkLowerUpperValue(node.SplitOpt.Lower, "lower")
	if err != nil {
		return nil, err
	}
	upperValues, err := checkLowerUpperValue(node.SplitOpt.Upper, "upper")
	if err != nil {
		return nil, err
	}
	p.Lower = lowerValues
	p.Upper = upperValues

	maxSplitRegionNum := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	if node.SplitOpt.Num > maxSplitRegionNum {
		return nil, errors.Errorf("Split index region num exceeded the limit %v", maxSplitRegionNum)
	} else if node.SplitOpt.Num < 1 {
		return nil, errors.Errorf("Split index region num should more than 0")
	}
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

func (b *PlanBuilder) convertValue2DeferredCausetType(valuesItem []ast.ExprNode, mockBlockPlan LogicalPlan, indexInfo *perceptron.IndexInfo, tblInfo *perceptron.BlockInfo) ([]types.Causet, error) {
	values := make([]types.Causet, 0, len(valuesItem))
	for j, valueItem := range valuesItem {
		colOffset := indexInfo.DeferredCausets[j].Offset
		value, err := b.convertValue(valueItem, mockBlockPlan, tblInfo.DeferredCausets[colOffset])
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (b *PlanBuilder) convertValue(valueItem ast.ExprNode, mockBlockPlan LogicalPlan, col *perceptron.DeferredCausetInfo) (d types.Causet, err error) {
	var expr expression.Expression
	switch x := valueItem.(type) {
	case *driver.ValueExpr:
		expr = &expression.Constant{
			Value:   x.Causet,
			RetType: &x.Type,
		}
	default:
		expr, _, err = b.rewrite(context.TODO(), valueItem, mockBlockPlan, nil, true)
		if err != nil {
			return d, err
		}
	}
	constant, ok := expr.(*expression.Constant)
	if !ok {
		return d, errors.New("Expect constant values")
	}
	value, err := constant.Eval(chunk.Row{})
	if err != nil {
		return d, err
	}
	d, err = value.ConvertTo(b.ctx.GetStochastikVars().StmtCtx, &col.FieldType)
	if err != nil {
		if !types.ErrTruncated.Equal(err) && !types.ErrTruncatedWrongVal.Equal(err) {
			return d, err
		}
		valStr, err1 := value.ToString()
		if err1 != nil {
			return d, err
		}
		return d, types.ErrTruncated.GenWithStack("Incorrect value: '%-.128s' for column '%.192s'", valStr, col.Name.O)
	}
	return d, nil
}

func (b *PlanBuilder) buildSplitBlockRegion(node *ast.SplitRegionStmt) (Plan, error) {
	tblInfo := node.Block.BlockInfo
	handleDefCausInfos := buildHandleDeferredCausetInfos(tblInfo)
	mockBlockPlan := LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
	schemaReplicant, names, err := expression.BlockInfo2SchemaAndNames(b.ctx, node.Block.Schema, tblInfo)
	if err != nil {
		return nil, err
	}
	mockBlockPlan.SetSchema(schemaReplicant)
	mockBlockPlan.names = names

	p := &SplitRegion{
		BlockInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
	}
	p.setSchemaAndNames(buildSplitRegionsSchema())
	if len(node.SplitOpt.ValueLists) > 0 {
		values := make([][]types.Causet, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			data, err := convertValueListToData(valuesItem, handleDefCausInfos, i, b, mockBlockPlan)
			if err != nil {
				return nil, err
			}
			values = append(values, data)
		}
		p.ValueLists = values
		return p, nil
	}

	p.Lower, err = convertValueListToData(node.SplitOpt.Lower, handleDefCausInfos, lowerBound, b, mockBlockPlan)
	if err != nil {
		return nil, err
	}
	p.Upper, err = convertValueListToData(node.SplitOpt.Upper, handleDefCausInfos, upperBound, b, mockBlockPlan)
	if err != nil {
		return nil, err
	}

	maxSplitRegionNum := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	if node.SplitOpt.Num > maxSplitRegionNum {
		return nil, errors.Errorf("Split block region num exceeded the limit %v", maxSplitRegionNum)
	} else if node.SplitOpt.Num < 1 {
		return nil, errors.Errorf("Split block region num should more than 0")
	}
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

func buildHandleDeferredCausetInfos(tblInfo *perceptron.BlockInfo) []*perceptron.DeferredCausetInfo {
	switch {
	case tblInfo.PKIsHandle:
		if col := tblInfo.GetPkDefCausInfo(); col != nil {
			return []*perceptron.DeferredCausetInfo{col}
		}
	case tblInfo.IsCommonHandle:
		pkIdx := blocks.FindPrimaryIndex(tblInfo)
		pkDefCauss := make([]*perceptron.DeferredCausetInfo, 0, len(pkIdx.DeferredCausets))
		defcaus := tblInfo.DeferredCausets
		for _, idxDefCaus := range pkIdx.DeferredCausets {
			pkDefCauss = append(pkDefCauss, defcaus[idxDefCaus.Offset])
		}
		return pkDefCauss
	default:
		return []*perceptron.DeferredCausetInfo{perceptron.NewExtraHandleDefCausInfo()}
	}
	return nil
}

const (
	lowerBound int = -1
	upperBound int = -2
)

func convertValueListToData(valueList []ast.ExprNode, handleDefCausInfos []*perceptron.DeferredCausetInfo, rowIdx int,
	b *PlanBuilder, mockBlockPlan *LogicalBlockDual) ([]types.Causet, error) {
	if len(valueList) != len(handleDefCausInfos) {
		var err error
		switch rowIdx {
		case lowerBound:
			err = errors.Errorf("Split block region lower value count should be %d", len(handleDefCausInfos))
		case upperBound:
			err = errors.Errorf("Split block region upper value count should be %d", len(handleDefCausInfos))
		default:
			err = ErrWrongValueCountOnRow.GenWithStackByArgs(rowIdx)
		}
		return nil, err
	}
	data := make([]types.Causet, 0, len(handleDefCausInfos))
	for i, v := range valueList {
		convertedCauset, err := b.convertValue(v, mockBlockPlan, handleDefCausInfos[i])
		if err != nil {
			return nil, err
		}
		data = append(data, convertedCauset)
	}
	return data, nil
}

func (b *PlanBuilder) buildDBS(ctx context.Context, node ast.DBSNode) (Plan, error) {
	var authErr error
	switch v := node.(type) {
	case *ast.AlterDatabaseStmt:
		if v.AlterDefaultDatabase {
			v.Name = b.ctx.GetStochastikVars().CurrentDB
		}
		if v.Name == "" {
			return nil, ErrNoDB
		}
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.AlterPriv, v.Name, "", "", authErr)
	case *ast.AlterBlockStmt:
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.Block.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.AlterPriv, v.Block.Schema.L,
			v.Block.Name.L, "", authErr)
		for _, spec := range v.Specs {
			if spec.Tp == ast.AlterBlockRenameBlock || spec.Tp == ast.AlterBlockExchangePartition {
				if b.ctx.GetStochastikVars().User != nil {
					authErr = ErrBlockaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetStochastikVars().User.AuthUsername,
						b.ctx.GetStochastikVars().User.AuthHostname, v.Block.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.DropPriv, v.Block.Schema.L,
					v.Block.Name.L, "", authErr)

				if b.ctx.GetStochastikVars().User != nil {
					authErr = ErrBlockaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetStochastikVars().User.AuthUsername,
						b.ctx.GetStochastikVars().User.AuthHostname, spec.NewBlock.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.CreatePriv, spec.NewBlock.Schema.L,
					spec.NewBlock.Name.L, "", authErr)

				if b.ctx.GetStochastikVars().User != nil {
					authErr = ErrBlockaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetStochastikVars().User.AuthUsername,
						b.ctx.GetStochastikVars().User.AuthHostname, spec.NewBlock.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.InsertPriv, spec.NewBlock.Schema.L,
					spec.NewBlock.Name.L, "", authErr)
			} else if spec.Tp == ast.AlterBlockDropPartition {
				if b.ctx.GetStochastikVars().User != nil {
					authErr = ErrBlockaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetStochastikVars().User.AuthUsername,
						b.ctx.GetStochastikVars().User.AuthHostname, v.Block.Name.L)
				}
				b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.DropPriv, v.Block.Schema.L,
					v.Block.Name.L, "", authErr)
			}
		}
	case *ast.CreateDatabaseStmt:
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.CreatePriv, v.Name,
			"", "", authErr)
	case *ast.CreateIndexStmt:
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("INDEX", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.Block.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.IndexPriv, v.Block.Schema.L,
			v.Block.Name.L, "", authErr)
	case *ast.CreateBlockStmt:
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.Block.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.CreatePriv, v.Block.Schema.L,
			v.Block.Name.L, "", authErr)
		if v.ReferBlock != nil {
			if b.ctx.GetStochastikVars().User != nil {
				authErr = ErrBlockaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetStochastikVars().User.AuthUsername,
					b.ctx.GetStochastikVars().User.AuthHostname, v.ReferBlock.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SelectPriv, v.ReferBlock.Schema.L,
				v.ReferBlock.Name.L, "", authErr)
		}
	case *ast.CreateViewStmt:
		b.capFlag |= canExpandAST
		b.capFlag |= collectUnderlyingViewName
		defer func() {
			b.capFlag &= ^canExpandAST
			b.capFlag &= ^collectUnderlyingViewName
		}()
		b.underlyingViewNames = set.NewStringSet()
		plan, err := b.Build(ctx, v.Select)
		if err != nil {
			return nil, err
		}
		if b.underlyingViewNames.Exist(v.ViewName.Schema.L + "." + v.ViewName.Name.L) {
			return nil, ErrNoSuchBlock.GenWithStackByArgs(v.ViewName.Schema.O, v.ViewName.Name.O)
		}
		schemaReplicant := plan.Schema()
		names := plan.OutputNames()
		if v.DefCauss == nil {
			adjustOverlongViewDefCausname(plan.(LogicalPlan))
			v.DefCauss = make([]perceptron.CIStr, len(schemaReplicant.DeferredCausets))
			for i, name := range names {
				v.DefCauss[i] = name.DefCausName
			}
		}
		if len(v.DefCauss) != schemaReplicant.Len() {
			return nil, dbs.ErrViewWrongList
		}
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("CREATE VIEW", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.ViewName.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.CreateViewPriv, v.ViewName.Schema.L,
			v.ViewName.Name.L, "", authErr)
		if v.Definer.CurrentUser && b.ctx.GetStochastikVars().User != nil {
			v.Definer = b.ctx.GetStochastikVars().User
		}
		if b.ctx.GetStochastikVars().User != nil && v.Definer.String() != b.ctx.GetStochastikVars().User.String() {
			err = ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "",
				"", "", err)
		}
	case *ast.CreateSequenceStmt:
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.Name.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.CreatePriv, v.Name.Schema.L,
			v.Name.Name.L, "", authErr)
	case *ast.DroFIDelatabaseStmt:
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrDBaccessDenied.GenWithStackByArgs(b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.Name)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.DropPriv, v.Name,
			"", "", authErr)
	case *ast.DropIndexStmt:
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("INDEx", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.Block.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.IndexPriv, v.Block.Schema.L,
			v.Block.Name.L, "", authErr)
	case *ast.DropBlockStmt:
		for _, blockVal := range v.Blocks {
			if b.ctx.GetStochastikVars().User != nil {
				authErr = ErrBlockaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetStochastikVars().User.AuthUsername,
					b.ctx.GetStochastikVars().User.AuthHostname, blockVal.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.DropPriv, blockVal.Schema.L,
				blockVal.Name.L, "", authErr)
		}
	case *ast.DropSequenceStmt:
		for _, sequence := range v.Sequences {
			if b.ctx.GetStochastikVars().User != nil {
				authErr = ErrBlockaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetStochastikVars().User.AuthUsername,
					b.ctx.GetStochastikVars().User.AuthHostname, sequence.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.DropPriv, sequence.Schema.L,
				sequence.Name.L, "", authErr)
		}
	case *ast.TruncateBlockStmt:
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.Block.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.DropPriv, v.Block.Schema.L,
			v.Block.Name.L, "", authErr)
	case *ast.RenameBlockStmt:
		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.OldBlock.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.AlterPriv, v.OldBlock.Schema.L,
			v.OldBlock.Name.L, "", authErr)

		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("DROP", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.OldBlock.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.DropPriv, v.OldBlock.Schema.L,
			v.OldBlock.Name.L, "", authErr)

		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("CREATE", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.NewBlock.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.CreatePriv, v.NewBlock.Schema.L,
			v.NewBlock.Name.L, "", authErr)

		if b.ctx.GetStochastikVars().User != nil {
			authErr = ErrBlockaccessDenied.GenWithStackByArgs("INSERT", b.ctx.GetStochastikVars().User.AuthUsername,
				b.ctx.GetStochastikVars().User.AuthHostname, v.NewBlock.Name.L)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.InsertPriv, v.NewBlock.Schema.L,
			v.NewBlock.Name.L, "", authErr)
	case *ast.RecoverBlockStmt, *ast.FlashBackBlockStmt:
		// Recover block command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", nil)
	case *ast.LockBlocksStmt, *ast.UnlockBlocksStmt:
		// TODO: add Lock Block privilege check.
	case *ast.CleanupBlockLockStmt:
		// This command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", nil)
	case *ast.RepairBlockStmt:
		// Repair block command can only be executed by administrator.
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", nil)
	}
	p := &DBS{Statement: node}
	return p, nil
}

const (
	// TraceFormatRow indicates event tracing format.
	TraceFormatRow = "event"
	// TraceFormatJSON indicates json tracing format.
	TraceFormatJSON = "json"
	// TraceFormatLog indicates log tracing format.
	TraceFormatLog = "log"
)

// buildTrace builds a trace plan. Inside this method, it first optimize the
// underlying query and then constructs a schemaReplicant, which will be used to constructs
// rows result.
func (b *PlanBuilder) buildTrace(trace *ast.TraceStmt) (Plan, error) {
	p := &Trace{StmtNode: trace.Stmt, Format: trace.Format}
	switch trace.Format {
	case TraceFormatRow:
		schemaReplicant := newDeferredCausetsWithNames(3)
		schemaReplicant.Append(buildDeferredCausetWithName("", "operation", allegrosql.TypeString, allegrosql.MaxBlobWidth))
		schemaReplicant.Append(buildDeferredCausetWithName("", "startTS", allegrosql.TypeString, allegrosql.MaxBlobWidth))
		schemaReplicant.Append(buildDeferredCausetWithName("", "duration", allegrosql.TypeString, allegrosql.MaxBlobWidth))
		p.SetSchema(schemaReplicant.col2Schema())
		p.names = schemaReplicant.names
	case TraceFormatJSON:
		schemaReplicant := newDeferredCausetsWithNames(1)
		schemaReplicant.Append(buildDeferredCausetWithName("", "operation", allegrosql.TypeString, allegrosql.MaxBlobWidth))
		p.SetSchema(schemaReplicant.col2Schema())
		p.names = schemaReplicant.names
	case TraceFormatLog:
		schemaReplicant := newDeferredCausetsWithNames(4)
		schemaReplicant.Append(buildDeferredCausetWithName("", "time", allegrosql.TypeTimestamp, allegrosql.MaxBlobWidth))
		schemaReplicant.Append(buildDeferredCausetWithName("", "event", allegrosql.TypeString, allegrosql.MaxBlobWidth))
		schemaReplicant.Append(buildDeferredCausetWithName("", "tags", allegrosql.TypeString, allegrosql.MaxBlobWidth))
		schemaReplicant.Append(buildDeferredCausetWithName("", "spanName", allegrosql.TypeString, allegrosql.MaxBlobWidth))
		p.SetSchema(schemaReplicant.col2Schema())
		p.names = schemaReplicant.names
	default:
		return nil, errors.New("trace format should be one of 'event', 'log' or 'json'")
	}
	return p, nil
}

func (b *PlanBuilder) buildExplainPlan(targetPlan Plan, format string, rows [][]string, analyze bool, execStmt ast.StmtNode) (Plan, error) {
	p := &Explain{
		TargetPlan: targetPlan,
		Format:     format,
		Analyze:    analyze,
		ExecStmt:   execStmt,
		Rows:       rows,
	}
	p.ctx = b.ctx
	return p, p.prepareSchema()
}

// buildExplainFor gets *last* (maybe running or finished) query plan from connection #connection id.
// See https://dev.allegrosql.com/doc/refman/8.0/en/explain-for-connection.html.
func (b *PlanBuilder) buildExplainFor(explainFor *ast.ExplainForStmt) (Plan, error) {
	processInfo, ok := b.ctx.GetStochastikManager().GetProcessInfo(explainFor.ConnectionID)
	if !ok {
		return nil, ErrNoSuchThread.GenWithStackByArgs(explainFor.ConnectionID)
	}
	if b.ctx.GetStochastikVars() != nil && b.ctx.GetStochastikVars().User != nil {
		if b.ctx.GetStochastikVars().User.Username != processInfo.User {
			err := ErrAccessDenied.GenWithStackByArgs(b.ctx.GetStochastikVars().User.Username, b.ctx.GetStochastikVars().User.Hostname)
			// Different from MyALLEGROSQL's behavior and document.
			b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SuperPriv, "", "", "", err)
		}
	}

	targetPlan, ok := processInfo.Plan.(Plan)
	if !ok || targetPlan == nil {
		return &Explain{Format: explainFor.Format}, nil
	}
	var rows [][]string
	if explainFor.Format == ast.ExplainFormatROW {
		rows = processInfo.PlanExplainRows
	}
	return b.buildExplainPlan(targetPlan, explainFor.Format, rows, false, nil)
}

func (b *PlanBuilder) buildExplain(ctx context.Context, explain *ast.ExplainStmt) (Plan, error) {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(ctx, show)
	}
	targetPlan, _, err := OptimizeAstNode(ctx, b.ctx, explain.Stmt, b.is)
	if err != nil {
		return nil, err
	}

	return b.buildExplainPlan(targetPlan, explain.Format, nil, explain.Analyze, explain.Stmt)
}

func (b *PlanBuilder) buildSelectInto(ctx context.Context, sel *ast.SelectStmt) (Plan, error) {
	selectIntoInfo := sel.SelectIntoOpt
	sel.SelectIntoOpt = nil
	targetPlan, _, err := OptimizeAstNode(ctx, b.ctx, sel, b.is)
	if err != nil {
		return nil, err
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.FilePriv, "", "", "", ErrSpecificAccessDenied.GenWithStackByArgs("FILE"))
	return &SelectInto{
		TargetPlan: targetPlan,
		IntoOpt:    selectIntoInfo,
	}, nil
}

func buildShowProcedureSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "ROUTINES"
	schemaReplicant := newDeferredCausetsWithNames(11)
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "EDB", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Name", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Type", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Definer", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Modified", allegrosql.TypeDatetime, 19))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Created", allegrosql.TypeDatetime, 19))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Security_type", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Comment", allegrosql.TypeBlob, 196605))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "character_set_client", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "collation_connection", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Database DefCauslation", allegrosql.TypeVarchar, 32))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildShowTriggerSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "TRIGGERS"
	schemaReplicant := newDeferredCausetsWithNames(11)
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Trigger", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Event", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Block", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Statement", allegrosql.TypeBlob, 196605))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Timing", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Created", allegrosql.TypeDatetime, 19))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "sql_mode", allegrosql.TypeBlob, 8192))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Definer", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "character_set_client", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "collation_connection", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Database DefCauslation", allegrosql.TypeVarchar, 32))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildShowEventsSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "EVENTS"
	schemaReplicant := newDeferredCausetsWithNames(15)
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "EDB", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Name", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Time zone", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Definer", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Type", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Execute At", allegrosql.TypeDatetime, 19))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Interval Value", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Interval Field", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Starts", allegrosql.TypeDatetime, 19))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Ends", allegrosql.TypeDatetime, 19))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Status", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Originator", allegrosql.TypeInt24, 4))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "character_set_client", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "collation_connection", allegrosql.TypeVarchar, 32))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Database DefCauslation", allegrosql.TypeVarchar, 32))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

func buildShowWarningsSchema() (*expression.Schema, types.NameSlice) {
	tblName := "WARNINGS"
	schemaReplicant := newDeferredCausetsWithNames(3)
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Level", allegrosql.TypeVarchar, 64))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Code", allegrosql.TypeLong, 19))
	schemaReplicant.Append(buildDeferredCausetWithName(tblName, "Message", allegrosql.TypeVarchar, 64))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

// buildShowSchema builds column info for ShowStmt including column name and type.
func buildShowSchema(s *ast.ShowStmt, isView bool, isSequence bool) (schemaReplicant *expression.Schema, outputNames []*types.FieldName) {
	var names []string
	var ftypes []byte
	switch s.Tp {
	case ast.ShowProcedureStatus:
		return buildShowProcedureSchema()
	case ast.ShowTriggers:
		return buildShowTriggerSchema()
	case ast.ShowEvents:
		return buildShowEventsSchema()
	case ast.ShowWarnings, ast.ShowErrors:
		return buildShowWarningsSchema()
	case ast.ShowRegions:
		return buildBlockRegionsSchema()
	case ast.ShowEngines:
		names = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	case ast.ShowConfig:
		names = []string{"Type", "Instance", "Name", "Value"}
	case ast.ShowDatabases:
		names = []string{"Database"}
	case ast.ShowOpenBlocks:
		names = []string{"Database", "Block", "In_use", "Name_locked"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLong, allegrosql.TypeLong}
	case ast.ShowBlocks:
		names = []string{fmt.Sprintf("Blocks_in_%s", s.DBName)}
		if s.Full {
			names = append(names, "Block_type")
		}
	case ast.ShowBlockStatus:
		names = []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length",
			"Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment",
			"Create_time", "UFIDelate_time", "Check_time", "DefCauslation", "Checksum",
			"Create_options", "Comment"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLonglong, allegrosql.TypeVarchar, allegrosql.TypeLonglong, allegrosql.TypeLonglong,
			allegrosql.TypeLonglong, allegrosql.TypeLonglong, allegrosql.TypeLonglong, allegrosql.TypeLonglong, allegrosql.TypeLonglong,
			allegrosql.TypeDatetime, allegrosql.TypeDatetime, allegrosql.TypeDatetime, allegrosql.TypeVarchar, allegrosql.TypeVarchar,
			allegrosql.TypeVarchar, allegrosql.TypeVarchar}
	case ast.ShowDeferredCausets:
		names = block.DefCausDescFieldNames(s.Full)
	case ast.ShowCharset:
		names = []string{"Charset", "Description", "Default collation", "Maxlen"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLonglong}
	case ast.ShowVariables, ast.ShowStatus:
		names = []string{"Variable_name", "Value"}
	case ast.ShowDefCauslation:
		names = []string{"DefCauslation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLonglong,
			allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLonglong}
	case ast.ShowCreateBlock, ast.ShowCreateSequence:
		if isSequence {
			names = []string{"Sequence", "Create Sequence"}
		} else if isView {
			names = []string{"View", "Create View", "character_set_client", "collation_connection"}
		} else {
			names = []string{"Block", "Create Block"}
		}
	case ast.ShowCreateUser:
		if s.User != nil {
			names = []string{fmt.Sprintf("CREATE USER for %s", s.User)}
		}
	case ast.ShowCreateView:
		names = []string{"View", "Create View", "character_set_client", "collation_connection"}
	case ast.ShowCreateDatabase:
		names = []string{"Database", "Create Database"}
	case ast.ShowDrainerStatus:
		names = []string{"NodeID", "Address", "State", "Max_Commit_Ts", "UFIDelate_Time"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLonglong, allegrosql.TypeVarchar}
	case ast.ShowGrants:
		if s.User != nil {
			names = []string{fmt.Sprintf("Grants for %s", s.User)}
		} else {
			// Don't know the name yet, so just say "user"
			names = []string{"Grants for User"}
		}
	case ast.ShowIndex:
		names = []string{"Block", "Non_unique", "Key_name", "Seq_in_index",
			"DeferredCauset_name", "DefCauslation", "Cardinality", "Sub_part", "Packed",
			"Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeLonglong, allegrosql.TypeVarchar, allegrosql.TypeLonglong,
			allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLonglong, allegrosql.TypeLonglong,
			allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar,
			allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar}
	case ast.ShowPlugins:
		names = []string{"Name", "Status", "Type", "Library", "License", "Version"}
		ftypes = []byte{
			allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		}
	case ast.ShowProcessList:
		names = []string{"Id", "User", "Host", "EDB", "Command", "Time", "State", "Info"}
		ftypes = []byte{allegrosql.TypeLonglong, allegrosql.TypeVarchar, allegrosql.TypeVarchar,
			allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLong, allegrosql.TypeVarchar, allegrosql.TypeString}
	case ast.ShowPumpStatus:
		names = []string{"NodeID", "Address", "State", "Max_Commit_Ts", "UFIDelate_Time"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLonglong, allegrosql.TypeVarchar}
	case ast.ShowStatsMeta:
		names = []string{"Db_name", "Block_name", "Partition_name", "UFIDelate_time", "Modify_count", "Row_count"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeDatetime, allegrosql.TypeLonglong, allegrosql.TypeLonglong}
	case ast.ShowStatsHistograms:
		names = []string{"Db_name", "Block_name", "Partition_name", "DeferredCauset_name", "Is_index", "UFIDelate_time", "Distinct_count", "Null_count", "Avg_col_size", "Correlation"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeTiny, allegrosql.TypeDatetime,
			allegrosql.TypeLonglong, allegrosql.TypeLonglong, allegrosql.TypeDouble, allegrosql.TypeDouble}
	case ast.ShowStatsBuckets:
		names = []string{"Db_name", "Block_name", "Partition_name", "DeferredCauset_name", "Is_index", "Bucket_id", "Count",
			"Repeats", "Lower_Bound", "Upper_Bound"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeTiny, allegrosql.TypeLonglong,
			allegrosql.TypeLonglong, allegrosql.TypeLonglong, allegrosql.TypeVarchar, allegrosql.TypeVarchar}
	case ast.ShowStatsHealthy:
		names = []string{"Db_name", "Block_name", "Partition_name", "Healthy"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLonglong}
	case ast.ShowProfiles: // ShowProfiles is deprecated.
		names = []string{"Query_ID", "Duration", "Query"}
		ftypes = []byte{allegrosql.TypeLong, allegrosql.TypeDouble, allegrosql.TypeVarchar}
	case ast.ShowMasterStatus:
		names = []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeLonglong, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar}
	case ast.ShowPrivileges:
		names = []string{"Privilege", "Context", "Comment"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar}
	case ast.ShowBindings:
		names = []string{"Original_sql", "Bind_sql", "Default_db", "Status", "Create_time", "UFIDelate_time", "Charset", "DefCauslation", "Source"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeDatetime, allegrosql.TypeDatetime, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar}
	case ast.ShowAnalyzeStatus:
		names = []string{"Block_schema", "Block_name", "Partition_name", "Job_info", "Processed_rows", "Start_time", "State"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLonglong, allegrosql.TypeDatetime, allegrosql.TypeVarchar}
	case ast.ShowBuiltins:
		names = []string{"Supported_builtin_functions"}
		ftypes = []byte{allegrosql.TypeVarchar}
	case ast.ShowBackups, ast.ShowRestores:
		names = []string{"Destination", "State", "Progress", "Queue_time", "Execution_time", "Finish_time", "Connection"}
		ftypes = []byte{allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeDouble, allegrosql.TypeDatetime, allegrosql.TypeDatetime, allegrosql.TypeDatetime, allegrosql.TypeLonglong}
	}

	schemaReplicant = expression.NewSchema(make([]*expression.DeferredCauset, 0, len(names))...)
	outputNames = make([]*types.FieldName, 0, len(names))
	for i := range names {
		col := &expression.DeferredCauset{}
		outputNames = append(outputNames, &types.FieldName{DefCausName: perceptron.NewCIStr(names[i])})
		// User varchar as the default return column type.
		tp := allegrosql.TypeVarchar
		if len(ftypes) != 0 && ftypes[i] != allegrosql.TypeUnspecified {
			tp = ftypes[i]
		}
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = allegrosql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.DefCauslate = types.DefaultCharsetForType(tp)
		col.RetType = fieldType
		schemaReplicant.Append(col)
	}
	return
}

func buildChecksumBlockSchema() (*expression.Schema, []*types.FieldName) {
	schemaReplicant := newDeferredCausetsWithNames(5)
	schemaReplicant.Append(buildDeferredCausetWithName("", "Db_name", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName("", "Block_name", allegrosql.TypeVarchar, 128))
	schemaReplicant.Append(buildDeferredCausetWithName("", "Checksum_crc64_xor", allegrosql.TypeLonglong, 22))
	schemaReplicant.Append(buildDeferredCausetWithName("", "Total_kvs", allegrosql.TypeLonglong, 22))
	schemaReplicant.Append(buildDeferredCausetWithName("", "Total_bytes", allegrosql.TypeLonglong, 22))
	return schemaReplicant.col2Schema(), schemaReplicant.names
}

// adjustOverlongViewDefCausname adjusts the overlong outputNames of a view to
// `new_exp_$off` where `$off` is the offset of the output column, $off starts from 1.
// There is still some MyALLEGROSQL compatible problems.
func adjustOverlongViewDefCausname(plan LogicalPlan) {
	outputNames := plan.OutputNames()
	for i := range outputNames {
		if outputName := outputNames[i].DefCausName.L; len(outputName) > allegrosql.MaxDeferredCausetNameLength {
			outputNames[i].DefCausName = perceptron.NewCIStr(fmt.Sprintf("name_exp_%d", i+1))
		}
	}
}
