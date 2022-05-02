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

package planner

import (
	"context"
	"math"
	"runtime/trace"
	"strings"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/bindinfo"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/cascades"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/privilege"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/hint"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

// GetPreparedStmt extract the prepared statement from the execute statement.
func GetPreparedStmt(stmt *ast.ExecuteStmt, vars *variable.Stochaseinstein_dbars) (ast.StmtNode, error) {
	var ok bool
	execID := stmt.ExecID
	if stmt.Name != "" {
		if execID, ok = vars.PreparedStmtNameToID[stmt.Name]; !ok {
			return nil, plannercore.ErrStmtNotFound
		}
	}
	if preparedPointer, ok := vars.PreparedStmts[execID]; ok {
		preparedObj, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
		if !ok {
			return nil, errors.Errorf("invalid CachedPrepareStmt type")
		}
		return preparedObj.PreparedAst.Stmt, nil
	}
	return nil, plannercore.ErrStmtNotFound
}

// IsReadOnly check whether the ast.Node is a read only statement.
func IsReadOnly(node ast.Node, vars *variable.Stochaseinstein_dbars) bool {
	if execStmt, isExecStmt := node.(*ast.ExecuteStmt); isExecStmt {
		s, err := GetPreparedStmt(execStmt, vars)
		if err != nil {
			logutil.BgLogger().Warn("GetPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(s)
	}
	return ast.IsReadOnly(node)
}

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, sctx stochastikctx.Context, node ast.Node, is schemareplicant.SchemaReplicant) (plannercore.Plan, types.NameSlice, error) {
	sessVars := sctx.GetStochaseinstein_dbars()

	// Because for write stmt, TiFlash has a different results when dagger the data in point get plan. We ban the TiFlash
	// engine in not read only stmt.
	if _, isolationReadContainTiFlash := sessVars.IsolationReadEngines[ekv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(node, sessVars) {
		delete(sessVars.IsolationReadEngines, ekv.TiFlash)
		defer func() {
			sessVars.IsolationReadEngines[ekv.TiFlash] = struct{}{}
		}()
	}

	if _, isolationReadContainEinsteinDB := sessVars.IsolationReadEngines[ekv.EinsteinDB]; isolationReadContainEinsteinDB {
		var fp plannercore.Plan
		if fpv, ok := sctx.Value(plannercore.PointPlanKey).(plannercore.PointPlanVal); ok {
			// point plan is already tried in a multi-statement query.
			fp = fpv.Plan
		} else {
			fp = plannercore.TryFastPlan(sctx, node)
		}
		if fp != nil {
			if !useMaxTS(sctx, fp) {
				sctx.PrepareTSFuture(ctx)
			}
			return fp, fp.OutputNames(), nil
		}
	}

	sctx.PrepareTSFuture(ctx)

	blockHints := hint.ExtractBlockHintsFromStmtNode(node, sctx)
	stmtHints, warns := handleStmtHints(blockHints)
	sessVars.StmtCtx.StmtHints = stmtHints
	for _, warn := range warns {
		sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(warn)
	}
	warns = warns[:0]
	bestPlan, names, _, err := optimize(ctx, sctx, node, is)
	if err != nil {
		return nil, nil, err
	}
	if !(sessVars.UsePlanBaselines || sessVars.EvolvePlanBaselines) {
		return bestPlan, names, nil
	}
	stmtNode, ok := node.(ast.StmtNode)
	if !ok {
		return bestPlan, names, nil
	}
	bindRecord, sINTERLOCKe := getBindRecord(sctx, stmtNode)
	if bindRecord == nil {
		return bestPlan, names, nil
	}
	if sctx.GetStochaseinstein_dbars().SelectLimit != math.MaxUint64 {
		sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(errors.New("sql_select_limit is set, so plan binding is not activated"))
		return bestPlan, names, nil
	}
	bestPlanHint := plannercore.GenHintsFromPhysicalPlan(bestPlan)
	if len(bindRecord.Bindings) > 0 {
		orgBinding := bindRecord.Bindings[0] // the first is the original binding
		for _, tbHint := range blockHints {  // consider block hints which contained by the original binding
			if orgBinding.Hint.ContainBlockHint(tbHint.HintName.String()) {
				bestPlanHint = append(bestPlanHint, tbHint)
			}
		}
	}
	bestPlanHintStr := hint.RestoreOptimizerHints(bestPlanHint)

	defer func() {
		sessVars.StmtCtx.StmtHints = stmtHints
		for _, warn := range warns {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(warn)
		}
	}()
	binding := bindRecord.FindBinding(bestPlanHintStr)
	// If the best bestPlan is in baselines, just use it.
	if binding != nil && binding.Status == bindinfo.Using {
		if sctx.GetStochaseinstein_dbars().UsePlanBaselines {
			stmtHints, warns = handleStmtHints(binding.Hint.GetFirstBlockHints())
		}
		return bestPlan, names, nil
	}
	bestCostAmongHints := math.MaxFloat64
	var bestPlanAmongHints plannercore.Plan
	originHints := hint.DefCauslectHint(stmtNode)
	// Try to find the best binding.
	for _, binding := range bindRecord.Bindings {
		if binding.Status != bindinfo.Using {
			continue
		}
		metrics.BindUsageCounter.WithLabelValues(sINTERLOCKe).Inc()
		hint.BindHint(stmtNode, binding.Hint)
		curStmtHints, curWarns := handleStmtHints(binding.Hint.GetFirstBlockHints())
		sctx.GetStochaseinstein_dbars().StmtCtx.StmtHints = curStmtHints
		plan, _, cost, err := optimize(ctx, sctx, node, is)
		if err != nil {
			binding.Status = bindinfo.Invalid
			handleInvalidBindRecord(ctx, sctx, sINTERLOCKe, bindinfo.BindRecord{
				OriginalALLEGROSQL: bindRecord.OriginalALLEGROSQL,
				EDB:                bindRecord.EDB,
				Bindings:           []bindinfo.Binding{binding},
			})
			continue
		}
		if cost < bestCostAmongHints {
			if sctx.GetStochaseinstein_dbars().UsePlanBaselines {
				stmtHints, warns = curStmtHints, curWarns
			}
			bestCostAmongHints = cost
			bestPlanAmongHints = plan
		}
	}
	// 1. If there is already a evolution task, we do not need to handle it again.
	// 2. If the origin binding contain `read_from_storage` hint, we should ignore the evolve task.
	// 3. If the best plan contain TiFlash hint, we should ignore the evolve task.
	if sctx.GetStochaseinstein_dbars().EvolvePlanBaselines && binding == nil &&
		!originHints.ContainBlockHint(plannercore.HintReadFromStorage) &&
		!bindRecord.Bindings[0].Hint.ContainBlockHint(plannercore.HintReadFromStorage) {
		handleEvolveTasks(ctx, sctx, bindRecord, stmtNode, bestPlanHintStr)
	}
	// Restore the hint to avoid changing the stmt node.
	hint.BindHint(stmtNode, originHints)
	if sctx.GetStochaseinstein_dbars().UsePlanBaselines && bestPlanAmongHints != nil {
		return bestPlanAmongHints, names, nil
	}
	return bestPlan, names, nil
}

func optimize(ctx context.Context, sctx stochastikctx.Context, node ast.Node, is schemareplicant.SchemaReplicant) (plannercore.Plan, types.NameSlice, float64, error) {
	// build logical plan
	sctx.GetStochaseinstein_dbars().PlanID = 0
	sctx.GetStochaseinstein_dbars().PlanDeferredCausetID = 0
	hintProcessor := &hint.BlockHintProcessor{Ctx: sctx}
	node.Accept(hintProcessor)
	builder := plannercore.NewPlanBuilder(sctx, is, hintProcessor)

	// reset fields about rewrite
	sctx.GetStochaseinstein_dbars().RewritePhaseInfo.Reset()
	beginRewrite := time.Now()
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, 0, err
	}
	sctx.GetStochaseinstein_dbars().RewritePhaseInfo.DurationRewrite = time.Since(beginRewrite)

	sctx.GetStochaseinstein_dbars().StmtCtx.Blocks = builder.GetDBBlockInfo()
	activeRoles := sctx.GetStochaseinstein_dbars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the block information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := plannercore.CheckPrivilege(activeRoles, pm, builder.GetVisitInfo()); err != nil {
			return nil, nil, 0, err
		}
	}

	if err := plannercore.CheckBlockLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, nil, 0, err
	}

	// Handle the execute statement.
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err := execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return p, p.OutputNames(), 0, err
	}

	names := p.OutputNames()

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(plannercore.LogicalPlan)
	if !isLogicalPlan {
		return p, names, 0, nil
	}

	// Handle the logical plan statement, use cascades planner if enabled.
	if sctx.GetStochaseinstein_dbars().GetEnableCascadesPlanner() {
		finalPlan, cost, err := cascades.DefaultOptimizer.FindBestPlan(sctx, logic)
		return finalPlan, names, cost, err
	}

	beginOpt := time.Now()
	finalPlan, cost, err := plannercore.DoOptimize(ctx, sctx, builder.GetOptFlag(), logic)
	sctx.GetStochaseinstein_dbars().DurationOptimization = time.Since(beginOpt)
	return finalPlan, names, cost, err
}

func extractSelectAndNormalizeDigest(stmtNode ast.StmtNode) (*ast.SelectStmt, string, string) {
	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		switch x.Stmt.(type) {
		case *ast.SelectStmt:
			plannercore.EraseLastSemicolon(x)
			normalizeExplainALLEGROSQL := berolinaAllegroSQL.Normalize(x.Text())
			idx := strings.Index(normalizeExplainALLEGROSQL, "select")
			normalizeALLEGROSQL := normalizeExplainALLEGROSQL[idx:]
			hash := berolinaAllegroSQL.DigestNormalized(normalizeALLEGROSQL)
			return x.Stmt.(*ast.SelectStmt), normalizeALLEGROSQL, hash
		}
	case *ast.SelectStmt:
		plannercore.EraseLastSemicolon(x)
		normalizedALLEGROSQL, hash := berolinaAllegroSQL.NormalizeDigest(x.Text())
		return x, normalizedALLEGROSQL, hash
	}
	return nil, "", ""
}

func getBindRecord(ctx stochastikctx.Context, stmt ast.StmtNode) (*bindinfo.BindRecord, string) {
	// When the petri is initializing, the bind will be nil.
	if ctx.Value(bindinfo.StochastikBindInfoKeyType) == nil {
		return nil, ""
	}
	selectStmt, normalizedALLEGROSQL, hash := extractSelectAndNormalizeDigest(stmt)
	if selectStmt == nil {
		return nil, ""
	}
	stochastikHandle := ctx.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
	bindRecord := stochastikHandle.GetBindRecord(normalizedALLEGROSQL, ctx.GetStochaseinstein_dbars().CurrentDB)
	if bindRecord == nil {
		bindRecord = stochastikHandle.GetBindRecord(normalizedALLEGROSQL, "")
	}
	if bindRecord != nil {
		if bindRecord.HasUsingBinding() {
			return bindRecord, metrics.SINTERLOCKeStochastik
		}
		return nil, ""
	}
	globalHandle := petri.GetPetri(ctx).BindHandle()
	if globalHandle == nil {
		return nil, ""
	}
	bindRecord = globalHandle.GetBindRecord(hash, normalizedALLEGROSQL, ctx.GetStochaseinstein_dbars().CurrentDB)
	if bindRecord == nil {
		bindRecord = globalHandle.GetBindRecord(hash, normalizedALLEGROSQL, "")
	}
	return bindRecord, metrics.SINTERLOCKeGlobal
}

func handleInvalidBindRecord(ctx context.Context, sctx stochastikctx.Context, level string, bindRecord bindinfo.BindRecord) {
	stochastikHandle := sctx.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
	err := stochastikHandle.DropBindRecord(bindRecord.OriginalALLEGROSQL, bindRecord.EDB, &bindRecord.Bindings[0])
	if err != nil {
		logutil.Logger(ctx).Info("drop stochastik bindings failed")
	}
	if level == metrics.SINTERLOCKeStochastik {
		return
	}

	globalHandle := petri.GetPetri(sctx).BindHandle()
	globalHandle.AddDropInvalidBindTask(&bindRecord)
}

func handleEvolveTasks(ctx context.Context, sctx stochastikctx.Context, br *bindinfo.BindRecord, stmtNode ast.StmtNode, planHint string) {
	bindALLEGROSQL := bindinfo.GenerateBindALLEGROSQL(ctx, stmtNode, planHint)
	if bindALLEGROSQL == "" {
		return
	}
	charset, collation := sctx.GetStochaseinstein_dbars().GetCharsetInfo()
	binding := bindinfo.Binding{
		BindALLEGROSQL: bindALLEGROSQL,
		Status:         bindinfo.PendingVerify,
		Charset:        charset,
		DefCauslation:  collation,
		Source:         bindinfo.Evolve,
	}
	globalHandle := petri.GetPetri(sctx).BindHandle()
	globalHandle.AddEvolvePlanTask(br.OriginalALLEGROSQL, br.EDB, binding)
}

// useMaxTS returns true when meets following conditions:
//  1. ctx is auto commit tagged.
//  2. plan is point get by pk.
func useMaxTS(ctx stochastikctx.Context, p plannercore.Plan) bool {
	if !plannercore.IsAutoCommitTxn(ctx) {
		return false
	}

	v, ok := p.(*plannercore.PointGetPlan)
	return ok && (v.IndexInfo == nil || (v.IndexInfo.Primary && v.TblInfo.IsCommonHandle))
}

// OptimizeExecStmt to optimize prepare statement protocol "execute" statement
// this is a short path ONLY does things filling prepare related params
// for point select like plan which does not need extra things
func OptimizeExecStmt(ctx context.Context, sctx stochastikctx.Context,
	execAst *ast.ExecuteStmt, is schemareplicant.SchemaReplicant) (plannercore.Plan, error) {
	defer trace.StartRegion(ctx, "Optimize").End()
	var err error
	builder := plannercore.NewPlanBuilder(sctx, is, nil)
	p, err := builder.Build(ctx, execAst)
	if err != nil {
		return nil, err
	}
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err = execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return execPlan.Plan, err
	}
	err = errors.Errorf("invalid result plan type, should be Execute")
	return nil, err
}

func handleStmtHints(hints []*ast.BlockOptimizerHint) (stmtHints stmtctx.StmtHints, warns []error) {
	if len(hints) == 0 {
		return
	}
	var memoryQuotaHint, useToJAHint, useCascadesHint, maxExecutionTime, forceNthPlan *ast.BlockOptimizerHint
	var memoryQuotaHintCnt, useToJAHintCnt, useCascadesHintCnt, noIndexMergeHintCnt, readReplicaHintCnt, maxExecutionTimeCnt, forceNthPlanCnt int
	for _, hint := range hints {
		switch hint.HintName.L {
		case "memory_quota":
			memoryQuotaHint = hint
			memoryQuotaHintCnt++
		case "use_toja":
			useToJAHint = hint
			useToJAHintCnt++
		case "use_cascades":
			useCascadesHint = hint
			useCascadesHintCnt++
		case "no_index_merge":
			noIndexMergeHintCnt++
		case "read_consistent_replica":
			readReplicaHintCnt++
		case "max_execution_time":
			maxExecutionTimeCnt++
			maxExecutionTime = hint
		case "nth_plan":
			forceNthPlanCnt++
			forceNthPlan = hint
		}
	}
	// Handle MEMORY_QUOTA
	if memoryQuotaHintCnt != 0 {
		if memoryQuotaHintCnt > 1 {
			warn := errors.Errorf("MEMORY_QUOTA() s defined more than once, only the last definition takes effect: MEMORY_QUOTA(%v)", memoryQuotaHint.HintData.(int64))
			warns = append(warns, warn)
		}
		// Executor use MemoryQuota <= 0 to indicate no memory limit, here use < 0 to handle hint syntax error.
		if memoryQuota := memoryQuotaHint.HintData.(int64); memoryQuota < 0 {
			warn := errors.New("The use of MEMORY_QUOTA hint is invalid, valid usage: MEMORY_QUOTA(10 MB) or MEMORY_QUOTA(10 GB)")
			warns = append(warns, warn)
		} else {
			stmtHints.HasMemQuotaHint = true
			stmtHints.MemQuotaQuery = memoryQuota
			if memoryQuota == 0 {
				warn := errors.New("Setting the MEMORY_QUOTA to 0 means no memory limit")
				warns = append(warns, warn)
			}
		}
	}
	// Handle USE_TOJA
	if useToJAHintCnt != 0 {
		if useToJAHintCnt > 1 {
			warn := errors.Errorf("USE_TOJA() is defined more than once, only the last definition takes effect: USE_TOJA(%v)", useToJAHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasAllowInSubqToJoinAnPosetDaggHint = true
		stmtHints.AllowInSubqToJoinAnPosetDagg = useToJAHint.HintData.(bool)
	}
	// Handle USE_CASCADES
	if useCascadesHintCnt != 0 {
		if useCascadesHintCnt > 1 {
			warn := errors.Errorf("USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES(%v)", useCascadesHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasEnableCascadesPlannerHint = true
		stmtHints.EnableCascadesPlanner = useCascadesHint.HintData.(bool)
	}
	// Handle NO_INDEX_MERGE
	if noIndexMergeHintCnt != 0 {
		if noIndexMergeHintCnt > 1 {
			warn := errors.New("NO_INDEX_MERGE() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.NoIndexMergeHint = true
	}
	// Handle READ_CONSISTENT_REPLICA
	if readReplicaHintCnt != 0 {
		if readReplicaHintCnt > 1 {
			warn := errors.New("READ_CONSISTENT_REPLICA() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.HasReplicaReadHint = true
		stmtHints.ReplicaRead = byte(ekv.ReplicaReadFollower)
	}
	// Handle MAX_EXECUTION_TIME
	if maxExecutionTimeCnt != 0 {
		if maxExecutionTimeCnt > 1 {
			warn := errors.Errorf("MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(%v)", maxExecutionTime.HintData.(uint64))
			warns = append(warns, warn)
		}
		stmtHints.HasMaxExecutionTime = true
		stmtHints.MaxExecutionTime = maxExecutionTime.HintData.(uint64)
	}
	// Handle NTH_PLAN
	if forceNthPlanCnt != 0 {
		if forceNthPlanCnt > 1 {
			warn := errors.Errorf("NTH_PLAN() is defined more than once, only the last definition takes effect: NTH_PLAN(%v)", forceNthPlan.HintData.(int64))
			warns = append(warns, warn)
		}
		stmtHints.ForceNthPlan = forceNthPlan.HintData.(int64)
		if stmtHints.ForceNthPlan < 1 {
			stmtHints.ForceNthPlan = -1
			warn := errors.Errorf("the hintdata for NTH_PLAN() is too small, hint ignored.")
			warns = append(warns, warn)
		}
	} else {
		stmtHints.ForceNthPlan = -1
	}
	return
}

func init() {
	plannercore.OptimizeAstNode = Optimize
}
