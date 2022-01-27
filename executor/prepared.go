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

package executor

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/planner"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	driver "github.com/whtcorpsinc/milevadb/types/berolinaAllegroSQL_driver"
	"go.uber.org/zap"
)

var (
	_ Executor = &DeallocateExec{}
	_ Executor = &ExecuteExec{}
	_ Executor = &PrepareExec{}
)

type paramMarkerSorter struct {
	markers []ast.ParamMarkerExpr
}

func (p *paramMarkerSorter) Len() int {
	return len(p.markers)
}

func (p *paramMarkerSorter) Less(i, j int) bool {
	return p.markers[i].(*driver.ParamMarkerExpr).Offset < p.markers[j].(*driver.ParamMarkerExpr).Offset
}

func (p *paramMarkerSorter) Swap(i, j int) {
	p.markers[i], p.markers[j] = p.markers[j], p.markers[i]
}

type paramMarkerExtractor struct {
	markers []ast.ParamMarkerExpr
}

func (e *paramMarkerExtractor) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (e *paramMarkerExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if x, ok := in.(*driver.ParamMarkerExpr); ok {
		e.markers = append(e.markers, x)
	}
	return in, true
}

// PrepareExec represents a PREPARE executor.
type PrepareExec struct {
	baseExecutor

	is      schemareplicant.SchemaReplicant
	name    string
	sqlText string

	ID         uint32
	ParamCount int
	Fields     []*ast.ResultField
}

// NewPrepareExec creates a new PrepareExec.
func NewPrepareExec(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant, sqlTxt string) *PrepareExec {
	base := newBaseExecutor(ctx, nil, 0)
	base.initCap = chunk.ZeroCapacity
	return &PrepareExec{
		baseExecutor: base,
		is:           is,
		sqlText:      sqlTxt,
	}
}

// Next implements the Executor Next interface.
func (e *PrepareExec) Next(ctx context.Context, req *chunk.Chunk) error {
	vars := e.ctx.GetStochastikVars()
	if e.ID != 0 {
		// Must be the case when we retry a prepare.
		// Make sure it is idempotent.
		_, ok := vars.PreparedStmts[e.ID]
		if ok {
			return nil
		}
	}
	charset, defCauslation := vars.GetCharsetInfo()
	var (
		stmts []ast.StmtNode
		err   error
	)
	if sqlberolinaAllegroSQL, ok := e.ctx.(sqlexec.ALLEGROSQLberolinaAllegroSQL); ok {
		stmts, err = sqlberolinaAllegroSQL.ParseALLEGROSQL(e.sqlText, charset, defCauslation)
	} else {
		p := berolinaAllegroSQL.New()
		p.EnableWindowFunc(vars.EnableWindowFunction)
		var warns []error
		stmts, warns, err = p.Parse(e.sqlText, charset, defCauslation)
		for _, warn := range warns {
			e.ctx.GetStochastikVars().StmtCtx.AppendWarning(soliton.SyntaxWarn(warn))
		}
	}
	if err != nil {
		return soliton.SyntaxError(err)
	}
	if len(stmts) != 1 {
		return ErrPrepareMulti
	}
	stmt := stmts[0]

	err = ResetContextOfStmt(e.ctx, stmt)
	if err != nil {
		return err
	}

	var extractor paramMarkerExtractor
	stmt.Accept(&extractor)

	// DBS Statements can not accept parameters
	if _, ok := stmt.(ast.DBSNode); ok && len(extractor.markers) > 0 {
		return ErrPrepareDBS
	}

	// Prepare parameters should NOT over 2 bytes(MaxUint16)
	// https://dev.allegrosql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK.
	if len(extractor.markers) > math.MaxUint16 {
		return ErrPsManyParam
	}

	err = plannercore.Preprocess(e.ctx, stmt, e.is, plannercore.InPrepare)
	if err != nil {
		return err
	}

	// The parameter markers are appended in visiting order, which may not
	// be the same as the position order in the query string. We need to
	// sort it by position.
	sorter := &paramMarkerSorter{markers: extractor.markers}
	sort.Sort(sorter)
	e.ParamCount = len(sorter.markers)
	for i := 0; i < e.ParamCount; i++ {
		sorter.markers[i].SetOrder(i)
	}
	prepared := &ast.Prepared{
		Stmt:          stmt,
		StmtType:      GetStmtLabel(stmt),
		Params:        sorter.markers,
		SchemaVersion: e.is.SchemaMetaVersion(),
	}

	if !plannercore.PreparedPlanCacheEnabled() {
		prepared.UseCache = false
	} else {
		if !e.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
			prepared.UseCache = plannercore.Cacheable(stmt, e.is)
		} else {
			prepared.UseCache = plannercore.Cacheable(stmt, nil)
		}
	}

	// We try to build the real statement of preparedStmt.
	for i := range prepared.Params {
		param := prepared.Params[i].(*driver.ParamMarkerExpr)
		param.Causet.SetNull()
		param.InExecute = false
	}
	var p plannercore.Plan
	e.ctx.GetStochastikVars().PlanID = 0
	e.ctx.GetStochastikVars().PlanDeferredCausetID = 0
	destBuilder := plannercore.NewPlanBuilder(e.ctx, e.is, &hint.BlockHintProcessor{})
	p, err = destBuilder.Build(ctx, stmt)
	if err != nil {
		return err
	}
	if _, ok := stmt.(*ast.SelectStmt); ok {
		e.Fields = defCausNames2ResultFields(p.Schema(), p.OutputNames(), vars.CurrentDB)
	}
	if e.ID == 0 {
		e.ID = vars.GetNextPreparedStmtID()
	}
	if e.name != "" {
		vars.PreparedStmtNameToID[e.name] = e.ID
	}

	normalized, digest := berolinaAllegroSQL.NormalizeDigest(prepared.Stmt.Text())
	preparedObj := &plannercore.CachedPrepareStmt{
		PreparedAst:   prepared,
		VisitInfos:    destBuilder.GetVisitInfo(),
		NormalizedALLEGROSQL: normalized,
		ALLEGROSQLDigest:     digest,
	}
	return vars.AddPreparedStmt(e.ID, preparedObj)
}

// ExecuteExec represents an EXECUTE executor.
// It cannot be executed by itself, all it needs to do is to build
// another Executor from a prepared statement.
type ExecuteExec struct {
	baseExecutor

	is            schemareplicant.SchemaReplicant
	name          string
	usingVars     []expression.Expression
	stmtExec      Executor
	stmt          ast.StmtNode
	plan          plannercore.Plan
	id            uint32
	lowerPriority bool
	outputNames   []*types.FieldName
}

// Next implements the Executor Next interface.
func (e *ExecuteExec) Next(ctx context.Context, req *chunk.Chunk) error {
	return nil
}

// Build builds a prepared statement into an executor.
// After Build, e.StmtExec will be used to do the real execution.
func (e *ExecuteExec) Build(b *executorBuilder) error {
	ok, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(e.ctx, e.plan)
	if err != nil {
		return err
	}
	if ok {
		err = e.ctx.InitTxnWithStartTS(math.MaxUint64)
	}
	if err != nil {
		return err
	}
	stmtExec := b.build(e.plan)
	if b.err != nil {
		log.Warn("rebuild plan in EXECUTE statement failed", zap.String("labelName of PREPARE statement", e.name))
		return errors.Trace(b.err)
	}
	e.stmtExec = stmtExec
	if e.ctx.GetStochastikVars().StmtCtx.Priority == allegrosql.NoPriority {
		e.lowerPriority = needLowerPriority(e.plan)
	}
	return nil
}

// DeallocateExec represent a DEALLOCATE executor.
type DeallocateExec struct {
	baseExecutor

	Name string
}

// Next implements the Executor Next interface.
func (e *DeallocateExec) Next(ctx context.Context, req *chunk.Chunk) error {
	vars := e.ctx.GetStochastikVars()
	id, ok := vars.PreparedStmtNameToID[e.Name]
	if !ok {
		return errors.Trace(plannercore.ErrStmtNotFound)
	}
	preparedPointer := vars.PreparedStmts[id]
	preparedObj, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
	if !ok {
		return errors.Errorf("invalid CachedPrepareStmt type")
	}
	prepared := preparedObj.PreparedAst
	delete(vars.PreparedStmtNameToID, e.Name)
	if plannercore.PreparedPlanCacheEnabled() {
		e.ctx.PreparedPlanCache().Delete(plannercore.NewPSTMTPlanCacheKey(
			vars, id, prepared.SchemaVersion,
		))
	}
	vars.RemovePreparedStmt(id)
	return nil
}

// CompileExecutePreparedStmt compiles a stochastik Execute command to a stmt.Statement.
func CompileExecutePreparedStmt(ctx context.Context, sctx stochastikctx.Context,
	ID uint32, args []types.Causet) (sqlexec.Statement, error) {
	startTime := time.Now()
	defer func() {
		sctx.GetStochastikVars().DurationCompile = time.Since(startTime)
	}()
	execStmt := &ast.ExecuteStmt{ExecID: ID}
	if err := ResetContextOfStmt(sctx, execStmt); err != nil {
		return nil, err
	}
	execStmt.BinaryArgs = args
	is := schemareplicant.GetSchemaReplicant(sctx)
	execPlan, names, err := planner.Optimize(ctx, sctx, execStmt, is)
	if err != nil {
		return nil, err
	}

	stmt := &ExecStmt{
		GoCtx:       ctx,
		SchemaReplicant:  is,
		Plan:        execPlan,
		StmtNode:    execStmt,
		Ctx:         sctx,
		OutputNames: names,
	}
	if preparedPointer, ok := sctx.GetStochastikVars().PreparedStmts[ID]; ok {
		preparedObj, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
		if !ok {
			return nil, errors.Errorf("invalid CachedPrepareStmt type")
		}
		stmtCtx := sctx.GetStochastikVars().StmtCtx
		stmt.Text = preparedObj.PreparedAst.Stmt.Text()
		stmtCtx.OriginalALLEGROSQL = stmt.Text
		stmtCtx.InitALLEGROSQLDigest(preparedObj.NormalizedALLEGROSQL, preparedObj.ALLEGROSQLDigest)
	}
	return stmt, nil
}
