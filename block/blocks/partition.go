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

package blocks

import (
	"bytes"
	"context"
	stderr "errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

// Both partition and partitionedBlock implement the block.Block interface.
var _ block.Block = &partition{}
var _ block.Block = &partitionedBlock{}

// partitionedBlock implements the block.PartitionedBlock interface.
var _ block.PartitionedBlock = &partitionedBlock{}

// partition is a feature from MyALLEGROSQL:
// See https://dev.allegrosql.com/doc/refman/8.0/en/partitioning.html
// A partition block may contain many partitions, each partition has a unique partition
// id. The underlying representation of a partition and a normal block (a block with no
// partitions) is basically the same.
// partition also implements the block.Block interface.
type partition struct {
	BlockCommon
}

// GetPhysicalID implements block.Block GetPhysicalID interface.
func (p *partition) GetPhysicalID() int64 {
	return p.physicalBlockID
}

// partitionedBlock implements the block.PartitionedBlock interface.
// partitionedBlock is a block, it contains many Partitions.
type partitionedBlock struct {
	BlockCommon
	partitionExpr   *PartitionExpr
	partitions      map[int64]*partition
	evalBufferTypes []*types.FieldType
	evalBufferPool  sync.Pool
}

func newPartitionedBlock(tbl *BlockCommon, tblInfo *perceptron.BlockInfo) (block.Block, error) {
	ret := &partitionedBlock{BlockCommon: *tbl}
	partitionExpr, err := newPartitionExpr(tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.partitionExpr = partitionExpr
	initEvalBufferType(ret)
	ret.evalBufferPool = sync.Pool{
		New: func() interface{} {
			return initEvalBuffer(ret)
		},
	}
	if err := initBlockIndices(&ret.BlockCommon); err != nil {
		return nil, errors.Trace(err)
	}
	pi := tblInfo.GetPartitionInfo()
	partitions := make(map[int64]*partition, len(pi.Definitions))
	for _, p := range pi.Definitions {
		var t partition
		err := initBlockCommonWithIndices(&t.BlockCommon, tblInfo, p.ID, tbl.DeferredCausets, tbl.allocs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		partitions[p.ID] = &t
	}
	ret.partitions = partitions
	return ret, nil
}

func newPartitionExpr(tblInfo *perceptron.BlockInfo) (*PartitionExpr, error) {
	ctx := mock.NewContext()
	dbName := perceptron.NewCIStr(ctx.GetStochastikVars().CurrentDB)
	defCausumns, names, err := expression.DeferredCausetInfos2DeferredCausetsAndNames(ctx, dbName, tblInfo.Name, tblInfo.DefCauss(), tblInfo)
	if err != nil {
		return nil, err
	}
	pi := tblInfo.GetPartitionInfo()
	switch pi.Type {
	case perceptron.PartitionTypeRange:
		return generateRangePartitionExpr(ctx, pi, defCausumns, names)
	case perceptron.PartitionTypeHash:
		return generateHashPartitionExpr(ctx, pi, defCausumns, names)
	}
	panic("cannot reach here")
}

// PartitionExpr is the partition definition expressions.
type PartitionExpr struct {
	// UpperBounds: (x < y1); (x < y2); (x < y3), used by locatePartition.
	UpperBounds []expression.Expression
	// OrigExpr is the partition expression ast used in point get.
	OrigExpr ast.ExprNode
	// Expr is the hash partition expression.
	Expr expression.Expression
	// Used in the range pruning process.
	*ForRangePruning
	// Used in the range defCausumn pruning process.
	*ForRangeDeferredCausetsPruning
}

func initEvalBufferType(t *partitionedBlock) {
	hasExtraHandle := false
	numDefCauss := len(t.DefCauss())
	if !t.Meta().PKIsHandle {
		hasExtraHandle = true
		numDefCauss++
	}
	t.evalBufferTypes = make([]*types.FieldType, numDefCauss)
	for i, defCaus := range t.DefCauss() {
		t.evalBufferTypes[i] = &defCaus.FieldType
	}

	if hasExtraHandle {
		t.evalBufferTypes[len(t.evalBufferTypes)-1] = types.NewFieldType(allegrosql.TypeLonglong)
	}
}

func initEvalBuffer(t *partitionedBlock) *chunk.MutRow {
	evalBuffer := chunk.MutRowFromTypes(t.evalBufferTypes)
	return &evalBuffer
}

// ForRangeDeferredCausetsPruning is used for range partition pruning.
type ForRangeDeferredCausetsPruning struct {
	LessThan []expression.Expression
	MaxValue bool
}

func dataForRangeDeferredCausetsPruning(ctx stochastikctx.Context, pi *perceptron.PartitionInfo, schemaReplicant *expression.Schema, names []*types.FieldName, p *berolinaAllegroSQL.berolinaAllegroSQL) (*ForRangeDeferredCausetsPruning, error) {
	var res ForRangeDeferredCausetsPruning
	res.LessThan = make([]expression.Expression, len(pi.Definitions))
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Use a bool flag instead of math.MaxInt64 to avoid the corner cases.
			res.MaxValue = true
		} else {
			tmp, err := parseSimpleExprWithNames(p, ctx, pi.Definitions[i].LessThan[0], schemaReplicant, names)
			if err != nil {
				return nil, err
			}
			res.LessThan[i] = tmp
		}
	}
	return &res, nil
}

// parseSimpleExprWithNames parses simple expression string to Expression.
// The expression string must only reference the defCausumn in the given NameSlice.
func parseSimpleExprWithNames(p *berolinaAllegroSQL.berolinaAllegroSQL, ctx stochastikctx.Context, exprStr string, schemaReplicant *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	exprNode, err := parseExpr(p, exprStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return expression.RewriteSimpleExprWithNames(ctx, exprNode, schemaReplicant, names)
}

// ForRangePruning is used for range partition pruning.
type ForRangePruning struct {
	LessThan []int64
	MaxValue bool
	Unsigned bool
}

// dataForRangePruning extracts the less than parts from 'partition p0 less than xx ... partitoin p1 less than ...'
func dataForRangePruning(sctx stochastikctx.Context, pi *perceptron.PartitionInfo) (*ForRangePruning, error) {
	var maxValue bool
	var unsigned bool
	lessThan := make([]int64, len(pi.Definitions))
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Use a bool flag instead of math.MaxInt64 to avoid the corner cases.
			maxValue = true
		} else {
			var err error
			lessThan[i], err = strconv.ParseInt(pi.Definitions[i].LessThan[0], 10, 64)
			var numErr *strconv.NumError
			if stderr.As(err, &numErr) && numErr.Err == strconv.ErrRange {
				var tmp uint64
				tmp, err = strconv.ParseUint(pi.Definitions[i].LessThan[0], 10, 64)
				lessThan[i] = int64(tmp)
				unsigned = true
			}
			if err != nil {
				val, ok := fixOldVersionPartitionInfo(sctx, pi.Definitions[i].LessThan[0])
				if !ok {
					logutil.BgLogger().Error("wrong partition definition", zap.String("less than", pi.Definitions[i].LessThan[0]))
					return nil, errors.WithStack(err)
				}
				lessThan[i] = val
			}
		}
	}
	return &ForRangePruning{
		LessThan: lessThan,
		MaxValue: maxValue,
		Unsigned: unsigned,
	}, nil
}

func fixOldVersionPartitionInfo(sctx stochastikctx.Context, str string) (int64, bool) {
	// less than value should be calculate to integer before persistent.
	// Old version MilevaDB may not do it and causetstore the raw expression.
	tmp, err := parseSimpleExprWithNames(berolinaAllegroSQL.New(), sctx, str, nil, nil)
	if err != nil {
		return 0, false
	}
	ret, isNull, err := tmp.EvalInt(sctx, chunk.Row{})
	if err != nil || isNull {
		return 0, false
	}
	return ret, true
}

// rangePartitionString returns the partition string for a range typed partition.
func rangePartitionString(pi *perceptron.PartitionInfo) string {
	// partition by range expr
	if len(pi.DeferredCausets) == 0 {
		return pi.Expr
	}

	// partition by range defCausumns (c1)
	if len(pi.DeferredCausets) == 1 {
		return pi.DeferredCausets[0].L
	}

	// partition by range defCausumns (c1, c2, ...)
	panic("create block assert len(defCausumns) = 1")
}

func generateRangePartitionExpr(ctx stochastikctx.Context, pi *perceptron.PartitionInfo,
	defCausumns []*expression.DeferredCauset, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	locateExprs := make([]expression.Expression, 0, len(pi.Definitions))
	var buf bytes.Buffer
	p := berolinaAllegroSQL.New()
	schemaReplicant := expression.NewSchema(defCausumns...)
	partStr := rangePartitionString(pi)
	for i := 0; i < len(pi.Definitions); i++ {
		if strings.EqualFold(pi.Definitions[i].LessThan[0], "MAXVALUE") {
			// Expr less than maxvalue is always true.
			fmt.Fprintf(&buf, "true")
		} else {
			fmt.Fprintf(&buf, "((%s) < (%s))", partStr, pi.Definitions[i].LessThan[0])
		}

		expr, err := parseSimpleExprWithNames(p, ctx, buf.String(), schemaReplicant, names)
		if err != nil {
			// If it got an error here, dbs may hang forever, so this error log is important.
			logutil.BgLogger().Error("wrong block partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		locateExprs = append(locateExprs, expr)
		buf.Reset()
	}
	ret := &PartitionExpr{
		UpperBounds: locateExprs,
	}

	switch len(pi.DeferredCausets) {
	case 0:
		exprs, err := parseSimpleExprWithNames(p, ctx, pi.Expr, schemaReplicant, names)
		if err != nil {
			return nil, err
		}
		tmp, err := dataForRangePruning(ctx, pi)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret.Expr = exprs
		ret.ForRangePruning = tmp
	case 1:
		tmp, err := dataForRangeDeferredCausetsPruning(ctx, pi, schemaReplicant, names, p)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret.ForRangeDeferredCausetsPruning = tmp
	default:
		panic("range defCausumn partition currently support only one defCausumn")
	}
	return ret, nil
}

func generateHashPartitionExpr(ctx stochastikctx.Context, pi *perceptron.PartitionInfo,
	defCausumns []*expression.DeferredCauset, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	schemaReplicant := expression.NewSchema(defCausumns...)
	origExpr, err := parseExpr(berolinaAllegroSQL.New(), pi.Expr)
	if err != nil {
		return nil, err
	}
	exprs, err := rewritePartitionExpr(ctx, origExpr, schemaReplicant, names)
	if err != nil {
		// If it got an error here, dbs may hang forever, so this error log is important.
		logutil.BgLogger().Error("wrong block partition expression", zap.String("expression", pi.Expr), zap.Error(err))
		return nil, errors.Trace(err)
	}
	exprs.HashCode(ctx.GetStochastikVars().StmtCtx)
	return &PartitionExpr{
		Expr:     exprs,
		OrigExpr: origExpr,
	}, nil
}

// PartitionExpr returns the partition expression.
func (t *partitionedBlock) PartitionExpr() (*PartitionExpr, error) {
	return t.partitionExpr, nil
}

// PartitionRecordKey is exported for test.
func PartitionRecordKey(pid int64, handle int64) ekv.Key {
	recordPrefix := blockcodec.GenBlockRecordPrefix(pid)
	return blockcodec.EncodeRecordKey(recordPrefix, ekv.IntHandle(handle))
}

// locatePartition returns the partition ID of the input record.
func (t *partitionedBlock) locatePartition(ctx stochastikctx.Context, pi *perceptron.PartitionInfo, r []types.Causet) (int64, error) {
	var err error
	var idx int
	switch t.meta.Partition.Type {
	case perceptron.PartitionTypeRange:
		if len(pi.DeferredCausets) == 0 {
			idx, err = t.locateRangePartition(ctx, pi, r)
		} else {
			idx, err = t.locateRangeDeferredCausetPartition(ctx, pi, r)
		}
	case perceptron.PartitionTypeHash:
		idx, err = t.locateHashPartition(ctx, pi, r)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return pi.Definitions[idx].ID, nil
}

func (t *partitionedBlock) locateRangeDeferredCausetPartition(ctx stochastikctx.Context, pi *perceptron.PartitionInfo, r []types.Causet) (int, error) {
	var err error
	var isNull bool
	partitionExprs := t.partitionExpr.UpperBounds
	evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
	defer t.evalBufferPool.Put(evalBuffer)
	idx := sort.Search(len(partitionExprs), func(i int) bool {
		evalBuffer.SetCausets(r...)
		ret, isNull, err := partitionExprs[i].EvalInt(ctx, evalBuffer.ToRow())
		if err != nil {
			return true // Break the search.
		}
		if isNull {
			// If the defCausumn value used to determine the partition is NULL, the event is inserted into the lowest partition.
			// See https://dev.allegrosql.com/doc/allegrosql-partitioning-excerpt/5.7/en/partitioning-handling-nulls.html
			return true // Break the search.
		}
		return ret > 0
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		idx = 0
	}
	if idx < 0 || idx >= len(partitionExprs) {
		// The data does not belong to any of the partition returns `block has no partition for value %s`.
		var valueMsg string
		if pi.Expr != "" {
			e, err := expression.ParseSimpleExprWithBlockInfo(ctx, pi.Expr, t.meta)
			if err == nil {
				val, _, err := e.EvalInt(ctx, chunk.MutRowFromCausets(r).ToRow())
				if err == nil {
					valueMsg = fmt.Sprintf("%d", val)
				}
			}
		} else {
			// When the block is partitioned by range defCausumns.
			valueMsg = "from defCausumn_list"
		}
		return 0, block.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
	}
	return idx, nil
}

func (t *partitionedBlock) locateRangePartition(ctx stochastikctx.Context, pi *perceptron.PartitionInfo, r []types.Causet) (int, error) {
	var (
		ret    int64
		val    int64
		isNull bool
		err    error
	)
	if defCaus, ok := t.partitionExpr.Expr.(*expression.DeferredCauset); ok {
		if r[defCaus.Index].IsNull() {
			isNull = true
		}
		ret = r[defCaus.Index].GetInt64()
	} else {
		evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
		defer t.evalBufferPool.Put(evalBuffer)
		evalBuffer.SetCausets(r...)
		val, isNull, err = t.partitionExpr.Expr.EvalInt(ctx, evalBuffer.ToRow())
		if err != nil {
			return 0, err
		}
		ret = val
	}
	unsigned := allegrosql.HasUnsignedFlag(t.partitionExpr.Expr.GetType().Flag)
	ranges := t.partitionExpr.ForRangePruning
	length := len(ranges.LessThan)
	pos := sort.Search(length, func(i int) bool {
		if isNull {
			return true
		}
		return ranges.compare(i, ret, unsigned) > 0
	})
	if isNull {
		pos = 0
	}
	if pos < 0 || pos >= length {
		// The data does not belong to any of the partition returns `block has no partition for value %s`.
		var valueMsg string
		if pi.Expr != "" {
			e, err := expression.ParseSimpleExprWithBlockInfo(ctx, pi.Expr, t.meta)
			if err == nil {
				val, _, err := e.EvalInt(ctx, chunk.MutRowFromCausets(r).ToRow())
				if err == nil {
					valueMsg = fmt.Sprintf("%d", val)
				}
			}
		} else {
			// When the block is partitioned by range defCausumns.
			valueMsg = "from defCausumn_list"
		}
		return 0, block.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
	}
	return pos, nil
}

// TODO: supports linear hashing
func (t *partitionedBlock) locateHashPartition(ctx stochastikctx.Context, pi *perceptron.PartitionInfo, r []types.Causet) (int, error) {
	if defCaus, ok := t.partitionExpr.Expr.(*expression.DeferredCauset); ok {
		ret := r[defCaus.Index].GetInt64()
		ret = ret % int64(t.meta.Partition.Num)
		if ret < 0 {
			ret = -ret
		}
		return int(ret), nil
	}
	evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
	defer t.evalBufferPool.Put(evalBuffer)
	evalBuffer.SetCausets(r...)
	ret, isNull, err := t.partitionExpr.Expr.EvalInt(ctx, evalBuffer.ToRow())
	if err != nil {
		return 0, err
	}
	if isNull {
		return 0, nil
	}
	ret = ret % int64(t.meta.Partition.Num)
	if ret < 0 {
		ret = -ret
	}
	return int(ret), nil
}

// GetPartition returns a Block, which is actually a partition.
func (t *partitionedBlock) GetPartition(pid int64) block.PhysicalBlock {
	// Attention, can't simply use `return t.partitions[pid]` here.
	// Because A nil of type *partition is a HoTT of `block.PhysicalBlock`
	p, ok := t.partitions[pid]
	if !ok {
		return nil
	}
	return p
}

// GetPartitionByRow returns a Block, which is actually a Partition.
func (t *partitionedBlock) GetPartitionByRow(ctx stochastikctx.Context, r []types.Causet) (block.PhysicalBlock, error) {
	pid, err := t.locatePartition(ctx, t.Meta().GetPartitionInfo(), r)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.partitions[pid], nil
}

// AddRecord implements the AddRecord method for the block.Block interface.
func (t *partitionedBlock) AddRecord(ctx stochastikctx.Context, r []types.Causet, opts ...block.AddRecordOption) (recordID ekv.Handle, err error) {
	return partitionedBlockAddRecord(ctx, t, r, nil, opts)
}

func partitionedBlockAddRecord(ctx stochastikctx.Context, t *partitionedBlock, r []types.Causet, partitionSelection map[int64]struct{}, opts []block.AddRecordOption) (recordID ekv.Handle, err error) {
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if partitionSelection != nil {
		if _, ok := partitionSelection[pid]; !ok {
			return nil, errors.WithStack(block.ErrRowDoesNotMatchGivenPartitionSet)
		}
	}
	tbl := t.GetPartition(pid)
	return tbl.AddRecord(ctx, r, opts...)
}

// partitionBlockWithGivenSets is used for this HoTT of grammar: partition (p0,p1)
// Basically it is the same as partitionedBlock except that partitionBlockWithGivenSets
// checks the given partition set for AddRecord/UFIDelateRecord operations.
type partitionBlockWithGivenSets struct {
	*partitionedBlock
	partitions map[int64]struct{}
}

// NewPartitionBlockithGivenSets creates a new partition block from a partition block.
func NewPartitionBlockithGivenSets(tbl block.PartitionedBlock, partitions map[int64]struct{}) block.PartitionedBlock {
	if raw, ok := tbl.(*partitionedBlock); ok {
		return &partitionBlockWithGivenSets{
			partitionedBlock: raw,
			partitions:       partitions,
		}
	}
	return tbl
}

// AddRecord implements the AddRecord method for the block.Block interface.
func (t *partitionBlockWithGivenSets) AddRecord(ctx stochastikctx.Context, r []types.Causet, opts ...block.AddRecordOption) (recordID ekv.Handle, err error) {
	return partitionedBlockAddRecord(ctx, t.partitionedBlock, r, t.partitions, opts)
}

// RemoveRecord implements block.Block RemoveRecord interface.
func (t *partitionedBlock) RemoveRecord(ctx stochastikctx.Context, h ekv.Handle, r []types.Causet) error {
	partitionInfo := t.meta.GetPartitionInfo()
	pid, err := t.locatePartition(ctx, partitionInfo, r)
	if err != nil {
		return errors.Trace(err)
	}

	tbl := t.GetPartition(pid)
	return tbl.RemoveRecord(ctx, h, r)
}

// UFIDelateRecord implements block.Block UFIDelateRecord interface.
// `touched` means which defCausumns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WriblockDefCauss()`.
func (t *partitionedBlock) UFIDelateRecord(ctx context.Context, sctx stochastikctx.Context, h ekv.Handle, currData, newData []types.Causet, touched []bool) error {
	return partitionedBlockUFIDelateRecord(ctx, sctx, t, h, currData, newData, touched, nil)
}

func (t *partitionBlockWithGivenSets) UFIDelateRecord(ctx context.Context, sctx stochastikctx.Context, h ekv.Handle, currData, newData []types.Causet, touched []bool) error {
	return partitionedBlockUFIDelateRecord(ctx, sctx, t.partitionedBlock, h, currData, newData, touched, t.partitions)
}

func partitionedBlockUFIDelateRecord(gctx context.Context, ctx stochastikctx.Context, t *partitionedBlock, h ekv.Handle, currData, newData []types.Causet, touched []bool, partitionSelection map[int64]struct{}) error {
	partitionInfo := t.meta.GetPartitionInfo()
	from, err := t.locatePartition(ctx, partitionInfo, currData)
	if err != nil {
		return errors.Trace(err)
	}
	to, err := t.locatePartition(ctx, partitionInfo, newData)
	if err != nil {
		return errors.Trace(err)
	}
	if partitionSelection != nil {
		if _, ok := partitionSelection[to]; !ok {
			return errors.WithStack(block.ErrRowDoesNotMatchGivenPartitionSet)
		}
	}

	// The old and new data locate in different partitions.
	// Remove record from old partition and add record to new partition.
	if from != to {
		_, err = t.GetPartition(to).AddRecord(ctx, newData)
		if err != nil {
			return errors.Trace(err)
		}
		// UFIDelateRecord should be side effect free, but there're two steps here.
		// What would happen if step1 succeed but step2 meets error? It's hard
		// to rollback.
		// So this special order is chosen: add record first, errors such as
		// 'Key Already Exists' will generally happen during step1, errors are
		// unlikely to happen in step2.
		err = t.GetPartition(from).RemoveRecord(ctx, h, currData)
		if err != nil {
			logutil.BgLogger().Error("uFIDelate partition record fails", zap.String("message", "new record inserted while old record is not removed"), zap.Error(err))
			return errors.Trace(err)
		}
		return nil
	}

	tbl := t.GetPartition(to)
	return tbl.UFIDelateRecord(gctx, ctx, h, currData, newData, touched)
}

// FindPartitionByName finds partition in block meta by name.
func FindPartitionByName(meta *perceptron.BlockInfo, parName string) (int64, error) {
	// Hash partition block use p0, p1, p2, p3 as partition names automatically.
	parName = strings.ToLower(parName)
	for _, def := range meta.Partition.Definitions {
		if strings.EqualFold(def.Name.L, parName) {
			return def.ID, nil
		}
	}
	return -1, errors.Trace(block.ErrUnknownPartition.GenWithStackByArgs(parName, meta.Name.O))
}

func parseExpr(p *berolinaAllegroSQL.berolinaAllegroSQL, exprStr string) (ast.ExprNode, error) {
	exprStr = "select " + exprStr
	stmts, _, err := p.Parse(exprStr, "", "")
	if err != nil {
		return nil, soliton.SyntaxWarn(err)
	}
	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	return fields[0].Expr, nil
}

func rewritePartitionExpr(ctx stochastikctx.Context, field ast.ExprNode, schemaReplicant *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	expr, err := expression.RewriteSimpleExprWithNames(ctx, field, schemaReplicant, names)
	return expr, err
}

func compareUnsigned(v1, v2 int64) int {
	switch {
	case uint64(v1) > uint64(v2):
		return 1
	case uint64(v1) == uint64(v2):
		return 0
	}
	return -1
}

func (lt *ForRangePruning) compare(ith int, v int64, unsigned bool) int {
	if ith == len(lt.LessThan)-1 {
		if lt.MaxValue {
			return 1
		}
	}
	if unsigned {
		return compareUnsigned(lt.LessThan[ith], v)
	}
	switch {
	case lt.LessThan[ith] > v:
		return 1
	case lt.LessThan[ith] == v:
		return 0
	}
	return -1
}
