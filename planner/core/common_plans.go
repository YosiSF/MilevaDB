MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
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
	"fmt"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/privilege"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/hint"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/kvcache"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/ranger"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/texttree"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	driver "github.com/whtcorpsinc/MilevaDB-Prod/types/berolinaAllegroSQL_driver"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

var planCacheCounter = metrics.PlanCacheCounter.WithLabelValues("prepare")

// ShowDBS is for showing DBS information.
type ShowDBS struct {
	baseSchemaProducer
}

// ShowSlow is for showing slow queries.
type ShowSlow struct {
	baseSchemaProducer

	*ast.ShowSlow
}

// ShowDBSJobQueries is for showing DBS job queries allegrosql.
type ShowDBSJobQueries struct {
	baseSchemaProducer

	JobIDs []int64
}

// ShowNextRowID is for showing the next global event ID.
type ShowNextRowID struct {
	baseSchemaProducer
	BlockName *ast.BlockName
}

// CheckBlock is used for checking block data, built from the 'admin check block' statement.
type CheckBlock struct {
	baseSchemaProducer

	DBName             string
	Block              block.Block
	IndexInfos         []*perceptron.IndexInfo
	IndexLookUpReaders []*PhysicalIndexLookUpReader
	ChecHoTTex         bool
}

// RecoverIndex is used for backfilling corrupted index data.
type RecoverIndex struct {
	baseSchemaProducer

	Block     *ast.BlockName
	IndexName string
}

// CleanupIndex is used to delete dangling index data.
type CleanupIndex struct {
	baseSchemaProducer

	Block     *ast.BlockName
	IndexName string
}

// ChecHoTTexRange is used for checking index data, output the index values that handle within begin and end.
type ChecHoTTexRange struct {
	baseSchemaProducer

	Block     *ast.BlockName
	IndexName string

	HandleRanges []ast.HandleRange
}

// ChecksumBlock is used for calculating block checksum, built from the `admin checksum block` statement.
type ChecksumBlock struct {
	baseSchemaProducer

	Blocks []*ast.BlockName
}

// CancelDBSJobs represents a cancel DBS jobs plan.
type CancelDBSJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// ReloadExprPushdownBlacklist reloads the data from expr_pushdown_blacklist block.
type ReloadExprPushdownBlacklist struct {
	baseSchemaProducer
}

// ReloadOptMemruleBlacklist reloads the data from opt_rule_blacklist block.
type ReloadOptMemruleBlacklist struct {
	baseSchemaProducer
}

// AdminPluginsCausetAction indicate action will be taken on plugins.
type AdminPluginsCausetAction int

const (
	// Enable indicates enable plugins.
	Enable AdminPluginsCausetAction = iota + 1
	// Disable indicates disable plugins.
	Disable
)

// AdminPlugins administrates milevadb plugins.
type AdminPlugins struct {
	baseSchemaProducer
	CausetAction AdminPluginsCausetAction
	Plugins      []string
}

// AdminShowTelemetry displays telemetry status including tracking ID, status and so on.
type AdminShowTelemetry struct {
	baseSchemaProducer
}

// AdminResetTelemetryID regenerates a new telemetry tracking ID.
type AdminResetTelemetryID struct {
	baseSchemaProducer
}

// Change represents a change plan.
type Change struct {
	baseSchemaProducer
	*ast.ChangeStmt
}

// Prepare represents prepare plan.
type Prepare struct {
	baseSchemaProducer

	Name           string
	ALLEGROSQLText string
}

// Execute represents prepare plan.
type Execute struct {
	baseSchemaProducer

	Name          string
	UsingVars     []expression.Expression
	PrepareParams []types.Causet
	ExecID        uint32
	Stmt          ast.StmtNode
	StmtType      string
	Plan          Plan
}

// OptimizePreparedPlan optimizes the prepared statement.
func (e *Execute) OptimizePreparedPlan(ctx context.Context, sctx stochastikctx.Context, is schemareplicant.SchemaReplicant) error {
	vars := sctx.GetStochaseinstein_dbars()
	if e.Name != "" {
		e.ExecID = vars.PreparedStmtNameToID[e.Name]
	}
	preparedPointer, ok := vars.PreparedStmts[e.ExecID]
	if !ok {
		return errors.Trace(ErrStmtNotFound)
	}
	preparedObj, ok := preparedPointer.(*CachedPrepareStmt)
	if !ok {
		return errors.Errorf("invalid CachedPrepareStmt type")
	}
	prepared := preparedObj.PreparedAst
	vars.StmtCtx.StmtType = prepared.StmtType

	paramLen := len(e.PrepareParams)
	if paramLen > 0 {
		// for binary protocol execute, argument is placed in vars.PrepareParams
		if len(prepared.Params) != paramLen {
			return errors.Trace(ErrWrongParamCount)
		}
		vars.PreparedParams = e.PrepareParams
		for i, val := range vars.PreparedParams {
			param := prepared.Params[i].(*driver.ParamMarkerExpr)
			param.Causet = val
			param.InExecute = true
		}
	} else {
		// for `execute stmt using @a, @b, @c`, using value in e.UsingVars
		if len(prepared.Params) != len(e.UsingVars) {
			return errors.Trace(ErrWrongParamCount)
		}

		for i, usingVar := range e.UsingVars {
			val, err := usingVar.Eval(chunk.Row{})
			if err != nil {
				return err
			}
			param := prepared.Params[i].(*driver.ParamMarkerExpr)
			param.Causet = val
			param.InExecute = true
			vars.PreparedParams = append(vars.PreparedParams, val)
		}
	}

	if prepared.SchemaVersion != is.SchemaMetaVersion() {
		// In order to avoid some correctness issues, we have to clear the
		// cached plan once the schemaReplicant version is changed.
		// Cached plan in prepared struct does NOT have a "cache key" with
		// schemaReplicant version like prepared plan cache key
		prepared.CachedPlan = nil
		preparedObj.Executor = nil
		// If the schemaReplicant version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schemaReplicant changed.
		err := Preprocess(sctx, prepared.Stmt, is, InPrepare)
		if err != nil {
			return ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
		}
		prepared.SchemaVersion = is.SchemaMetaVersion()
	}
	err := e.getPhysicalPlan(ctx, sctx, is, preparedObj)
	if err != nil {
		return err
	}
	e.Stmt = prepared.Stmt
	return nil
}

func (e *Execute) checkPreparedPriv(ctx context.Context, sctx stochastikctx.Context,
	preparedObj *CachedPrepareStmt, is schemareplicant.SchemaReplicant) error {
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := CheckPrivilege(sctx.GetStochaseinstein_dbars().ActiveRoles, pm, preparedObj.VisitInfos); err != nil {
			return err
		}
	}
	err := CheckBlockLock(sctx, is, preparedObj.VisitInfos)
	return err
}

func (e *Execute) setFoundInPlanCache(sctx stochastikctx.Context, opt bool) error {
	vars := sctx.GetStochaseinstein_dbars()
	err := vars.SetSystemVar(variable.MilevaDBFoundInPlanCache, variable.BoolToIntStr(opt))
	return err
}

func (e *Execute) getPhysicalPlan(ctx context.Context, sctx stochastikctx.Context, is schemareplicant.SchemaReplicant, preparedStmt *CachedPrepareStmt) error {
	stmtCtx := sctx.GetStochaseinstein_dbars().StmtCtx
	prepared := preparedStmt.PreparedAst
	stmtCtx.UseCache = prepared.UseCache
	var cacheKey kvcache.Key
	if prepared.UseCache {
		cacheKey = NewPSTMTPlanCacheKey(sctx.GetStochaseinstein_dbars(), e.ExecID, prepared.SchemaVersion)
	}
	if prepared.CachedPlan != nil {
		// Rewriting the expression in the select.where condition  will convert its
		// type from "paramMarker" to "CouplingConstantWithRadix".When Point Select queries are executed,
		// the expression in the where condition will not be evaluated,
		// so you don't need to consider whether prepared.useCache is enabled.
		plan := prepared.CachedPlan.(Plan)
		names := prepared.CachedNames.(types.NameSlice)
		err := e.rebuildRange(plan)
		if err != nil {
			logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
			goto REBUILD
		}
		if metrics.ResetblockPlanCacheCounterFortTest {
			metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
		} else {
			planCacheCounter.Inc()
		}
		err = e.setFoundInPlanCache(sctx, true)
		if err != nil {
			return err
		}
		e.names = names
		e.Plan = plan
		stmtCtx.PointExec = true
		return nil
	}
	if prepared.UseCache {
		if cacheValue, exists := sctx.PreparedPlanCache().Get(cacheKey); exists {
			if err := e.checkPreparedPriv(ctx, sctx, preparedStmt, is); err != nil {
				return err
			}
			cachedVal := cacheValue.(*PSTMTPlanCacheValue)
			planValid := true
			for tblInfo, unionScan := range cachedVal.TblInfo2UnionScan {
				if !unionScan && blockHasDirtyContent(sctx, tblInfo) {
					planValid = false
					// TODO we can inject UnionScan into cached plan to avoid invalidating it, though
					// rebuilding the filters in UnionScan is pretty trivial.
					sctx.PreparedPlanCache().Delete(cacheKey)
					break
				}
			}
			if planValid {
				err := e.rebuildRange(cachedVal.Plan)
				if err != nil {
					logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
					goto REBUILD
				}
				err = e.setFoundInPlanCache(sctx, true)
				if err != nil {
					return err
				}
				if metrics.ResetblockPlanCacheCounterFortTest {
					metrics.PlanCacheCounter.WithLabelValues("prepare").Inc()
				} else {
					planCacheCounter.Inc()
				}
				e.names = cachedVal.OutPutNames
				e.Plan = cachedVal.Plan
				stmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
				return nil
			}
		}
	}

REBUILD:
	stmt := TryAddExtraLimit(sctx, prepared.Stmt)
	p, names, err := OptimizeAstNode(ctx, sctx, stmt, is)
	if err != nil {
		return err
	}
	err = e.tryCachePointPlan(ctx, sctx, preparedStmt, is, p)
	if err != nil {
		return err
	}
	e.names = names
	e.Plan = p
	_, isBlockDual := p.(*PhysicalBlockDual)
	if !isBlockDual && prepared.UseCache {
		cached := NewPSTMTPlanCacheValue(p, names, stmtCtx.TblInfo2UnionScan)
		preparedStmt.NormalizedPlan, preparedStmt.PlanDigest = NormalizePlan(p)
		stmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
		sctx.PreparedPlanCache().Put(cacheKey, cached)
	}
	err = e.setFoundInPlanCache(sctx, false)
	return err
}

// tryCachePointPlan will try to cache point execution plan, there may be some
// short paths for these executions, currently "point select" and "point uFIDelate"
func (e *Execute) tryCachePointPlan(ctx context.Context, sctx stochastikctx.Context,
	preparedStmt *CachedPrepareStmt, is schemareplicant.SchemaReplicant, p Plan) error {
	var (
		prepared = preparedStmt.PreparedAst
		ok       bool
		err      error
		names    types.NameSlice
	)
	switch p.(type) {
	case *PointGetPlan:
		ok, err = IsPointGetWithPKOrUniqueKeyByAutoCommit(sctx, p)
		names = p.OutputNames()
		if err != nil {
			return err
		}
	case *UFIDelate:
		ok, err = IsPointUFIDelateByAutoCommit(sctx, p)
		if err != nil {
			return err
		}
		if ok {
			// make constant expression causetstore paramMarker
			sctx.GetStochaseinstein_dbars().StmtCtx.PointExec = true
			p, names, err = OptimizeAstNode(ctx, sctx, prepared.Stmt, is)
		}
	}
	if ok {
		// just cache point plan now
		prepared.CachedPlan = p
		prepared.CachedNames = names
		preparedStmt.NormalizedPlan, preparedStmt.PlanDigest = NormalizePlan(p)
		sctx.GetStochaseinstein_dbars().StmtCtx.SetPlanDigest(preparedStmt.NormalizedPlan, preparedStmt.PlanDigest)
	}
	return err
}

func (e *Execute) rebuildRange(p Plan) error {
	sctx := p.SCtx()
	sc := p.SCtx().GetStochaseinstein_dbars().StmtCtx
	var err error
	switch x := p.(type) {
	case *PhysicalBlockReader:
		ts := x.BlockPlans[0].(*PhysicalBlockScan)
		var pkDefCaus *expression.DeferredCauset
		if ts.Block.IsCommonHandle {
			pk := blocks.FindPrimaryIndex(ts.Block)
			pkDefCauss := make([]*expression.DeferredCauset, 0, len(pk.DeferredCausets))
			pkDefCaussLen := make([]int, 0, len(pk.DeferredCausets))
			for _, colInfo := range pk.DeferredCausets {
				pkDefCauss = append(pkDefCauss, expression.DefCausInfo2DefCaus(ts.schemaReplicant.DeferredCausets, ts.Block.DeferredCausets[colInfo.Offset]))
				pkDefCaussLen = append(pkDefCaussLen, colInfo.Length)
			}
			res, err := ranger.DetachCondAndBuildRangeForIndex(p.SCtx(), ts.AccessCondition, pkDefCauss, pkDefCaussLen)
			if err != nil {
				return err
			}
			ts.Ranges = res.Ranges
		} else {
			if ts.Block.PKIsHandle {
				if pkDefCausInfo := ts.Block.GetPkDefCausInfo(); pkDefCausInfo != nil {
					pkDefCaus = expression.DefCausInfo2DefCaus(ts.schemaReplicant.DeferredCausets, pkDefCausInfo)
				}
			}
			if pkDefCaus != nil {
				ts.Ranges, err = ranger.BuildBlockRange(ts.AccessCondition, sc, pkDefCaus.RetType)
				if err != nil {
					return err
				}
			} else {
				ts.Ranges = ranger.FullIntRange(false)
			}
		}
	case *PhysicalIndexReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return err
		}
	case *PhysicalIndexLookUpReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return err
		}
	case *PointGetPlan:
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxDefCauss, x.IdxDefCausLens)
				if err != nil {
					return err
				}
				for i := range x.IndexValues {
					x.IndexValues[i] = ranges.Ranges[0].LowVal[i]
				}
			} else {
				var pkDefCaus *expression.DeferredCauset
				if x.TblInfo.PKIsHandle {
					if pkDefCausInfo := x.TblInfo.GetPkDefCausInfo(); pkDefCausInfo != nil {
						pkDefCaus = expression.DefCausInfo2DefCaus(x.schemaReplicant.DeferredCausets, pkDefCausInfo)
					}
				}
				if pkDefCaus != nil {
					ranges, err := ranger.BuildBlockRange(x.AccessConditions, x.ctx.GetStochaseinstein_dbars().StmtCtx, pkDefCaus.RetType)
					if err != nil {
						return err
					}
					x.Handle = ekv.IntHandle(ranges[0].LowVal[0].GetInt64())
				}
			}
		}
		// The code should never run here as long as we're not using point get for partition block.
		// And if we change the logic one day, here work as defensive programming to cache the error.
		if x.PartitionInfo != nil {
			return errors.New("point get for partition block can not use plan cache")
		}
		if x.HandleParam != nil {
			var iv int64
			iv, err = x.HandleParam.Causet.ToInt64(sc)
			if err != nil {
				return err
			}
			x.Handle = ekv.IntHandle(iv)
			return nil
		}
		for i, param := range x.IndexValueParams {
			if param != nil {
				x.IndexValues[i] = param.Causet
			}
		}
		return nil
	case *BatchPointGetPlan:
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxDefCauss, x.IdxDefCausLens)
				if err != nil {
					return err
				}
				for i := range x.IndexValues {
					for j := range ranges.Ranges[i].LowVal {
						x.IndexValues[i][j] = ranges.Ranges[i].LowVal[j]
					}
				}
			} else {
				var pkDefCaus *expression.DeferredCauset
				if x.TblInfo.PKIsHandle {
					if pkDefCausInfo := x.TblInfo.GetPkDefCausInfo(); pkDefCausInfo != nil {
						pkDefCaus = expression.DefCausInfo2DefCaus(x.schemaReplicant.DeferredCausets, pkDefCausInfo)
					}
				}
				if pkDefCaus != nil {
					ranges, err := ranger.BuildBlockRange(x.AccessConditions, x.ctx.GetStochaseinstein_dbars().StmtCtx, pkDefCaus.RetType)
					if err != nil {
						return err
					}
					for i := range ranges {
						x.Handles[i] = ekv.IntHandle(ranges[i].LowVal[0].GetInt64())
					}
				}
			}
		}
		for i, param := range x.HandleParams {
			if param != nil {
				var iv int64
				iv, err = param.Causet.ToInt64(sc)
				if err != nil {
					return err
				}
				x.Handles[i] = ekv.IntHandle(iv)
			}
		}
		for i, params := range x.IndexValueParams {
			if len(params) < 1 {
				continue
			}
			for j, param := range params {
				if param != nil {
					x.IndexValues[i][j] = param.Causet
				}
			}
		}
	case PhysicalPlan:
		for _, child := range x.Children() {
			err = e.rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *Insert:
		if x.SelectPlan != nil {
			return e.rebuildRange(x.SelectPlan)
		}
	case *UFIDelate:
		if x.SelectPlan != nil {
			return e.rebuildRange(x.SelectPlan)
		}
	case *Delete:
		if x.SelectPlan != nil {
			return e.rebuildRange(x.SelectPlan)
		}
	}
	return nil
}

func (e *Execute) buildRangeForIndexScan(sctx stochastikctx.Context, is *PhysicalIndexScan) ([]*ranger.Range, error) {
	if len(is.IdxDefCauss) == 0 {
		return ranger.FullRange(), nil
	}
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, is.IdxDefCauss, is.IdxDefCausLens)
	if err != nil {
		return nil, err
	}
	return res.Ranges, nil
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	baseSchemaProducer

	Name string
}

// Set represents a plan for set stmt.
type Set struct {
	baseSchemaProducer

	VarAssigns []*expression.VarAssignment
}

// SetConfig represents a plan for set config stmt.
type SetConfig struct {
	baseSchemaProducer

	Type     string
	Instance string
	Name     string
	Value    expression.Expression
}

// ALLEGROSQLBindOpType repreents the ALLEGROALLEGROSQL bind type
type ALLEGROSQLBindOpType int

const (
	// OpALLEGROSQLBindCreate represents the operation to create a ALLEGROALLEGROSQL bind.
	OpALLEGROSQLBindCreate ALLEGROSQLBindOpType = iota
	// OpALLEGROSQLBindDrop represents the operation to drop a ALLEGROALLEGROSQL bind.
	OpALLEGROSQLBindDrop
	// OpFlushBindings is used to flush plan bindings.
	OpFlushBindings
	// OpCaptureBindings is used to capture plan bindings.
	OpCaptureBindings
	// OpEvolveBindings is used to evolve plan binding.
	OpEvolveBindings
	// OpReloadBindings is used to reload plan binding.
	OpReloadBindings
)

// ALLEGROSQLBindPlan represents a plan for ALLEGROALLEGROSQL bind.
type ALLEGROSQLBindPlan struct {
	baseSchemaProducer

	ALLEGROSQLBindOp    ALLEGROSQLBindOpType
	NormdOrigALLEGROSQL string
	BindALLEGROSQL      string
	IsGlobal            bool
	BindStmt            ast.StmtNode
	EDB                 string
	Charset             string
	DefCauslation       string
}

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	baseSchemaProducer

	Statement ast.StmtNode
}

// InsertGeneratedDeferredCausets is for completing generated columns in Insert.
// We resolve generation expressions in plan, and eval those in executor.
type InsertGeneratedDeferredCausets struct {
	DeferredCausets []*ast.DeferredCausetName
	Exprs           []expression.Expression
	OnDuplicates    []*expression.Assignment
}

// Insert represents an insert plan.
type Insert struct {
	baseSchemaProducer

	Block             block.Block
	blockSchema       *expression.Schema
	blockDefCausNames types.NameSlice
	DeferredCausets   []*ast.DeferredCausetName
	Lists             [][]expression.Expression
	SetList           []*expression.Assignment

	OnDuplicate        []*expression.Assignment
	Schema4OnDuplicate *expression.Schema
	names4OnDuplicate  types.NameSlice

	GenDefCauss InsertGeneratedDeferredCausets

	SelectPlan PhysicalPlan

	IsReplace bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	AllAssignmentsAreCouplingConstantWithRadix bool
}

// UFIDelate represents UFIDelate plan.
type UFIDelate struct {
	baseSchemaProducer

	OrderedList []*expression.Assignment

	AllAssignmentsAreCouplingConstantWithRadix bool

	SelectPlan PhysicalPlan

	TblDefCausPosInfos TblDefCausPosInfoSlice

	// Used when partition sets are given.
	// e.g. uFIDelate t partition(p0) set a = 1;
	PartitionedBlock []block.PartitionedBlock
}

// Delete represents a delete plan.
type Delete struct {
	baseSchemaProducer

	IsMultiBlock bool

	SelectPlan PhysicalPlan

	TblDefCausPosInfos TblDefCausPosInfoSlice
}

// AnalyzeBlockID is hybrid block id used to analyze block.
type AnalyzeBlockID struct {
	PersistID      int64
	DefCauslectIDs []int64
}

// StoreAsDefCauslectID indicates whether collect block id is same as persist block id.
// for new partition implementation is TRUE but FALSE for old partition implementation
func (h *AnalyzeBlockID) StoreAsDefCauslectID() bool {
	return h.PersistID == h.DefCauslectIDs[0]
}

func (h *AnalyzeBlockID) String() string {
	return fmt.Sprintf("%d => %v", h.DefCauslectIDs, h.PersistID)
}

// Equals indicates whether two block id is equal.
func (h *AnalyzeBlockID) Equals(t *AnalyzeBlockID) bool {
	if h == t {
		return true
	}
	if h == nil || t == nil {
		return false
	}
	if h.PersistID != t.PersistID {
		return false
	}
	if len(h.DefCauslectIDs) != len(t.DefCauslectIDs) {
		return false
	}
	if len(h.DefCauslectIDs) == 1 {
		return h.DefCauslectIDs[0] == t.DefCauslectIDs[0]
	}
	for _, hp := range h.DefCauslectIDs {
		var matchOne bool
		for _, tp := range t.DefCauslectIDs {
			if tp == hp {
				matchOne = true
				break
			}
		}
		if !matchOne {
			return false
		}
	}
	return true
}

// analyzeInfo is used to causetstore the database name, block name and partition name of analyze task.
type analyzeInfo struct {
	DBName        string
	BlockName     string
	PartitionName string
	BlockID       AnalyzeBlockID
	Incremental   bool
}

// AnalyzeDeferredCausetsTask is used for analyze columns.
type AnalyzeDeferredCausetsTask struct {
	HandleDefCauss HandleDefCauss
	DefCaussInfo   []*perceptron.DeferredCausetInfo
	TblInfo        *perceptron.BlockInfo
	analyzeInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	IndexInfo *perceptron.IndexInfo
	TblInfo   *perceptron.BlockInfo
	analyzeInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	baseSchemaProducer

	DefCausTasks []AnalyzeDeferredCausetsTask
	IdxTasks     []AnalyzeIndexTask
	Opts         map[ast.AnalyzeOptionType]uint64
}

// LoadData represents a loaddata plan.
type LoadData struct {
	baseSchemaProducer

	IsLocal         bool
	OnDuplicate     ast.OnDuplicateKeyHandlingType
	Path            string
	Block           *ast.BlockName
	DeferredCausets []*ast.DeferredCausetName
	FieldsInfo      *ast.FieldsClause
	LinesInfo       *ast.LinesClause
	IgnoreLines     uint64

	DeferredCausetAssignments  []*ast.Assignment
	DeferredCausetsAndUserVars []*ast.DeferredCausetNameOrUserVar

	GenDefCauss InsertGeneratedDeferredCausets
}

// LoadStats represents a load stats plan.
type LoadStats struct {
	baseSchemaProducer

	Path string
}

// IndexAdvise represents a index advise plan.
type IndexAdvise struct {
	baseSchemaProducer

	IsLocal     bool
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *ast.MaxIndexNumClause
	LinesInfo   *ast.LinesClause
}

// SplitRegion represents a split regions plan.
type SplitRegion struct {
	baseSchemaProducer

	BlockInfo      *perceptron.BlockInfo
	PartitionNames []perceptron.CIStr
	IndexInfo      *perceptron.IndexInfo
	Lower          []types.Causet
	Upper          []types.Causet
	Num            int
	ValueLists     [][]types.Causet
}

// SplitRegionStatus represents a split regions status plan.
type SplitRegionStatus struct {
	baseSchemaProducer

	Block     block.Block
	IndexInfo *perceptron.IndexInfo
}

// DBS represents a DBS statement plan.
type DBS struct {
	baseSchemaProducer

	Statement ast.DBSNode
}

// SelectInto represents a select-into plan.
type SelectInto struct {
	baseSchemaProducer

	TargetPlan Plan
	IntoOpt    *ast.SelectIntoOption
}

// Explain represents a explain plan.
type Explain struct {
	baseSchemaProducer

	TargetPlan Plan
	Format     string
	Analyze    bool
	ExecStmt   ast.StmtNode

	Rows           [][]string
	explainedPlans map[int]bool
}

// GetExplainRowsForPlan get explain rows for plan.
func GetExplainRowsForPlan(plan Plan) (rows [][]string) {
	explain := &Explain{
		TargetPlan: plan,
		Format:     ast.ExplainFormatROW,
		Analyze:    false,
	}
	if err := explain.RenderResult(); err != nil {
		return rows
	}
	return explain.Rows
}

// prepareSchema prepares explain's result schemaReplicant.
func (e *Explain) prepareSchema() error {
	var fieldNames []string
	format := strings.ToLower(e.Format)

	switch {
	case format == ast.ExplainFormatROW && !e.Analyze:
		fieldNames = []string{"id", "estRows", "task", "access object", "operator info"}
	case format == ast.ExplainFormatROW && e.Analyze:
		fieldNames = []string{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == ast.ExplainFormatDOT:
		fieldNames = []string{"dot contents"}
	case format == ast.ExplainFormatHint:
		fieldNames = []string{"hint"}
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}

	cwn := &columnsWithNames{
		defcaus: make([]*expression.DeferredCauset, 0, len(fieldNames)),
		names:   make([]*types.FieldName, 0, len(fieldNames)),
	}

	for _, fieldName := range fieldNames {
		cwn.Append(buildDeferredCausetWithName("", fieldName, allegrosql.TypeString, allegrosql.MaxBlobWidth))
	}
	e.SetSchema(cwn.col2Schema())
	e.names = cwn.names
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	if e.TargetPlan == nil {
		return nil
	}
	switch strings.ToLower(e.Format) {
	case ast.ExplainFormatROW:
		if e.Rows == nil || e.Analyze {
			e.explainedPlans = map[int]bool{}
			err := e.explainPlanInRowFormat(e.TargetPlan, "root", "", "", true)
			if err != nil {
				return err
			}
		}
	case ast.ExplainFormatDOT:
		if physicalPlan, ok := e.TargetPlan.(PhysicalPlan); ok {
			e.prepareDotInfo(physicalPlan)
		}
	case ast.ExplainFormatHint:
		hints := GenHintsFromPhysicalPlan(e.TargetPlan)
		hints = append(hints, hint.ExtractBlockHintsFromStmtNode(e.ExecStmt, nil)...)
		e.Rows = append(e.Rows, []string{hint.RestoreOptimizerHints(hints)})
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

// explainPlanInRowFormat generates explain information for root-tasks.
func (e *Explain) explainPlanInRowFormat(p Plan, taskType, driverSide, indent string, isLastChild bool) (err error) {
	e.prepareOperatorInfo(p, taskType, driverSide, indent, isLastChild)
	e.explainedPlans[p.ID()] = true

	// For every child we create a new sub-tree rooted by it.
	childIndent := texttree.Indent4Child(indent, isLastChild)

	if physPlan, ok := p.(PhysicalPlan); ok {
		// indicate driven side and driving side of 'join' and 'apply'
		// See issue https://github.com/whtcorpsinc/MilevaDB-Prod/issues/14602.
		driverSideInfo := make([]string, len(physPlan.Children()))
		buildSide := -1

		switch plan := physPlan.(type) {
		case *PhysicalApply:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalHashJoin:
			if plan.UseOuterToBuild {
				buildSide = plan.InnerChildIdx ^ 1
			} else {
				buildSide = plan.InnerChildIdx
			}
		case *PhysicalMergeJoin:
			if plan.JoinType == RightOuterJoin {
				buildSide = 0
			} else {
				buildSide = 1
			}
		case *PhysicalIndexJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalIndexMergeJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalIndexHashJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalBroadCastJoin:
			buildSide = plan.InnerChildIdx
		}

		if buildSide != -1 {
			driverSideInfo[0], driverSideInfo[1] = "(Build)", "(Probe)"
		} else {
			buildSide = 0
		}

		// Always put the Build above the Probe.
		for i := range physPlan.Children() {
			pchild := &physPlan.Children()[i^buildSide]
			if e.explainedPlans[(*pchild).ID()] {
				continue
			}
			err = e.explainPlanInRowFormat(*pchild, taskType, driverSideInfo[i], childIndent, i == len(physPlan.Children())-1)
			if err != nil {
				return
			}
		}
	}

	switch x := p.(type) {
	case *PhysicalBlockReader:
		var storeType string
		switch x.StoreType {
		case ekv.EinsteinDB, ekv.TiFlash, ekv.MilevaDB:
			// expected do nothing
		default:
			return errors.Errorf("the causetstore type %v is unknown", x.StoreType)
		}
		storeType = x.StoreType.Name()
		err = e.explainPlanInRowFormat(x.blockPlan, "INTERLOCK["+storeType+"]", "", childIndent, true)
	case *PhysicalIndexReader:
		err = e.explainPlanInRowFormat(x.indexPlan, "INTERLOCK[einsteindb]", "", childIndent, true)
	case *PhysicalIndexLookUpReader:
		err = e.explainPlanInRowFormat(x.indexPlan, "INTERLOCK[einsteindb]", "(Build)", childIndent, false)
		err = e.explainPlanInRowFormat(x.blockPlan, "INTERLOCK[einsteindb]", "(Probe)", childIndent, true)
	case *PhysicalIndexMergeReader:
		for _, pchild := range x.partialPlans {
			err = e.explainPlanInRowFormat(pchild, "INTERLOCK[einsteindb]", "(Build)", childIndent, false)
		}
		err = e.explainPlanInRowFormat(x.blockPlan, "INTERLOCK[einsteindb]", "(Probe)", childIndent, true)
	case *Insert:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", "", childIndent, true)
		}
	case *UFIDelate:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", "", childIndent, true)
		}
	case *Delete:
		if x.SelectPlan != nil {
			err = e.explainPlanInRowFormat(x.SelectPlan, "root", "", childIndent, true)
		}
	case *Execute:
		if x.Plan != nil {
			err = e.explainPlanInRowFormat(x.Plan, "root", "", indent, true)
		}
	}
	return
}

func getRuntimeInfo(ctx stochastikctx.Context, p Plan) (actRows, analyzeInfo, memoryInfo, diskInfo string) {
	runtimeStatsDefCausl := ctx.GetStochaseinstein_dbars().StmtCtx.RuntimeStatsDefCausl
	if runtimeStatsDefCausl == nil {
		return
	}
	explainID := p.ID()

	// There maybe some mock information for INTERLOCK task to let runtimeStatsDefCausl.Exists(p.ExplainID()) is true.
	// So check INTERLOCKTaskEkxecDetail first and print the real INTERLOCK task information if it's not empty.
	if runtimeStatsDefCausl.ExistsINTERLOCKStats(explainID) {
		INTERLOCKstats := runtimeStatsDefCausl.GetINTERLOCKStats(explainID)
		analyzeInfo = INTERLOCKstats.String()
		actRows = fmt.Sprint(INTERLOCKstats.GetActRows())
	} else if runtimeStatsDefCausl.ExistsRootStats(explainID) {
		rootstats := runtimeStatsDefCausl.GetRootStats(explainID)
		analyzeInfo = rootstats.String()
		actRows = fmt.Sprint(rootstats.GetActRows())
	} else {
		analyzeInfo = "time:0ns, loops:0"
		actRows = "0"
	}

	memoryInfo = "N/A"
	memTracker := ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker.SearchTrackerWithoutLock(p.ID())
	if memTracker != nil {
		memoryInfo = memTracker.BytesToString(memTracker.MaxConsumed())
	}

	diskInfo = "N/A"
	diskTracker := ctx.GetStochaseinstein_dbars().StmtCtx.DiskTracker.SearchTrackerWithoutLock(p.ID())
	if diskTracker != nil {
		diskInfo = diskTracker.BytesToString(diskTracker.MaxConsumed())
	}
	return
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, estimated rows, task type, access object and other operator info.
func (e *Explain) prepareOperatorInfo(p Plan, taskType, driverSide, indent string, isLastChild bool) {
	if p.ExplainID().String() == "_0" {
		return
	}

	id := texttree.PrettyIdentifier(p.ExplainID().String()+driverSide, indent, isLastChild)

	estRows := "N/A"
	if si := p.statsInfo(); si != nil {
		estRows = strconv.FormatFloat(si.RowCount, 'f', 2, 64)
	}

	var accessObject, operatorInfo string
	if plan, ok := p.(dataAccesser); ok {
		accessObject = plan.AccessObject()
		operatorInfo = plan.OperatorInfo(false)
	} else {
		if pa, ok := p.(partitionAccesser); ok && e.ctx != nil {
			accessObject = pa.accessObject(e.ctx)
		}
		operatorInfo = p.ExplainInfo()
	}

	var event []string
	if e.Analyze {
		actRows, analyzeInfo, memoryInfo, diskInfo := getRuntimeInfo(e.ctx, p)
		event = []string{id, estRows, actRows, taskType, accessObject, analyzeInfo, operatorInfo, memoryInfo, diskInfo}
	} else {
		event = []string{id, estRows, taskType, accessObject, operatorInfo}
	}
	e.Rows = append(e.Rows, event)
}

func (e *Explain) prepareDotInfo(p PhysicalPlan) {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "\ndigraph %s {\n", p.ExplainID())
	e.prepareTaskDot(p, "root", buffer)
	buffer.WriteString("}\n")

	e.Rows = append(e.Rows, []string{buffer.String()})
}

func (e *Explain) prepareTaskDot(p PhysicalPlan, taskTp string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "subgraph cluster%v{\n", p.ID())
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	fmt.Fprintf(buffer, "label = \"%s\"\n", taskTp)

	if len(p.Children()) == 0 {
		if taskTp == "INTERLOCK" {
			fmt.Fprintf(buffer, "\"%s\"\n}\n", p.ExplainID())
			return
		}
		fmt.Fprintf(buffer, "\"%s\"\n", p.ExplainID())
	}

	var CausetTasks []PhysicalPlan
	var pipelines []string

	for planQueue := []PhysicalPlan{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curPlan := planQueue[0]
		switch INTERLOCKPlan := curPlan.(type) {
		case *PhysicalBlockReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", INTERLOCKPlan.ExplainID(), INTERLOCKPlan.blockPlan.ExplainID()))
			CausetTasks = append(CausetTasks, INTERLOCKPlan.blockPlan)
		case *PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", INTERLOCKPlan.ExplainID(), INTERLOCKPlan.indexPlan.ExplainID()))
			CausetTasks = append(CausetTasks, INTERLOCKPlan.indexPlan)
		case *PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", INTERLOCKPlan.ExplainID(), INTERLOCKPlan.blockPlan.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", INTERLOCKPlan.ExplainID(), INTERLOCKPlan.indexPlan.ExplainID()))
			CausetTasks = append(CausetTasks, INTERLOCKPlan.blockPlan)
			CausetTasks = append(CausetTasks, INTERLOCKPlan.indexPlan)
		case *PhysicalIndexMergeReader:
			for i := 0; i < len(INTERLOCKPlan.partialPlans); i++ {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", INTERLOCKPlan.ExplainID(), INTERLOCKPlan.partialPlans[i].ExplainID()))
				CausetTasks = append(CausetTasks, INTERLOCKPlan.partialPlans[i])
			}
			if INTERLOCKPlan.blockPlan != nil {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", INTERLOCKPlan.ExplainID(), INTERLOCKPlan.blockPlan.ExplainID()))
				CausetTasks = append(CausetTasks, INTERLOCKPlan.blockPlan)
			}
		}
		for _, child := range curPlan.Children() {
			fmt.Fprintf(buffer, "\"%s\" -> \"%s\"\n", curPlan.ExplainID(), child.ExplainID())
			planQueue = append(planQueue, child)
		}
	}
	buffer.WriteString("}\n")

	for _, INTERLOCK := range CausetTasks {
		e.prepareTaskDot(INTERLOCK.(PhysicalPlan), "INTERLOCK", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. stochastik is not InTxn
//  3. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx stochastikctx.Context, p Plan) (bool, error) {
	if !IsAutoCommitTxn(ctx) {
		return false, nil
	}

	// check plan
	if proj, ok := p.(*PhysicalProjection); ok {
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *PhysicalIndexReader:
		indexScan := v.IndexPlans[0].(*PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(ctx.GetStochaseinstein_dbars().StmtCtx), nil
	case *PhysicalBlockReader:
		blockScan := v.BlockPlans[0].(*PhysicalBlockScan)
		isPointRange := len(blockScan.Ranges) == 1 && blockScan.Ranges[0].IsPoint(ctx.GetStochaseinstein_dbars().StmtCtx)
		if !isPointRange {
			return false, nil
		}
		pkLength := 1
		if blockScan.Block.IsCommonHandle {
			pkIdx := blocks.FindPrimaryIndex(blockScan.Block)
			pkLength = len(pkIdx.DeferredCausets)
		}
		return len(blockScan.Ranges[0].LowVal) == pkLength, nil
	case *PointGetPlan:
		// If the PointGetPlan needs to read data using unique index (double read), we
		// can't use max uint64, because using math.MaxUint64 can't guarantee repeablock-read
		// and the data and index would be inconsistent!
		isPointGet := v.IndexInfo == nil || (v.IndexInfo.Primary && v.TblInfo.IsCommonHandle)
		return isPointGet, nil
	default:
		return false, nil
	}
}

// IsAutoCommitTxn checks if stochastik is in autocommit mode and not InTxn
// used for fast plan like point get
func IsAutoCommitTxn(ctx stochastikctx.Context) bool {
	return ctx.GetStochaseinstein_dbars().IsAutocommit() && !ctx.GetStochaseinstein_dbars().InTxn()
}

// IsPointUFIDelateByAutoCommit checks if plan p is point uFIDelate and is in autocommit context
func IsPointUFIDelateByAutoCommit(ctx stochastikctx.Context, p Plan) (bool, error) {
	if !IsAutoCommitTxn(ctx) {
		return false, nil
	}

	// check plan
	uFIDelPlan, ok := p.(*UFIDelate)
	if !ok {
		return false, nil
	}
	if _, isFastSel := uFIDelPlan.SelectPlan.(*PointGetPlan); isFastSel {
		return true, nil
	}
	return false, nil
}

func buildSchemaAndNameFromIndex(defcaus []*expression.DeferredCauset, dbName perceptron.CIStr, tblInfo *perceptron.BlockInfo, idxInfo *perceptron.IndexInfo) (*expression.Schema, types.NameSlice) {
	schemaReplicant := expression.NewSchema(defcaus...)
	idxDefCauss := idxInfo.DeferredCausets
	names := make([]*types.FieldName, 0, len(idxDefCauss))
	tblName := tblInfo.Name
	for _, col := range idxDefCauss {
		names = append(names, &types.FieldName{
			OrigTblName:     tblName,
			OrigDefCausName: col.Name,
			DBName:          dbName,
			TblName:         tblName,
			DefCausName:     col.Name,
		})
	}
	return schemaReplicant, names
}

func buildSchemaAndNameFromPKDefCaus(pkDefCaus *expression.DeferredCauset, dbName perceptron.CIStr, tblInfo *perceptron.BlockInfo) (*expression.Schema, types.NameSlice) {
	schemaReplicant := expression.NewSchema([]*expression.DeferredCauset{pkDefCaus}...)
	names := make([]*types.FieldName, 0, 1)
	tblName := tblInfo.Name
	col := tblInfo.GetPkDefCausInfo()
	names = append(names, &types.FieldName{
		OrigTblName:     tblName,
		OrigDefCausName: col.Name,
		DBName:          dbName,
		TblName:         tblName,
		DefCausName:     col.Name,
	})
	return schemaReplicant, names
}

func locateHashPartition(ctx stochastikctx.Context, expr expression.Expression, pi *perceptron.PartitionInfo, r []types.Causet) (int, error) {
	ret, isNull, err := expr.EvalInt(ctx, chunk.MutRowFromCausets(r).ToRow())
	if err != nil {
		return 0, err
	}
	if isNull {
		return 0, nil
	}
	if ret < 0 {
		ret = 0 - ret
	}
	return int(ret % int64(pi.Num)), nil
}
