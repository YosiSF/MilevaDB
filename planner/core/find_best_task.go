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
	"math"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/planner/property"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/collate"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
	"golang.org/x/tools/container/intsets"
)

const (
	// SelectionFactor is the default factor of the selectivity.
	// For example, If we have no idea how to estimate the selectivity
	// of a Selection or a JoinCondition, we can use this default value.
	SelectionFactor = 0.8
	distinctFactor  = 0.8
)

var aggFuncFactor = map[string]float64{
	ast.AggFuncCount:       1.0,
	ast.AggFuncSum:         1.0,
	ast.AggFuncAvg:         2.0,
	ast.AggFuncFirstRow:    0.1,
	ast.AggFuncMax:         1.0,
	ast.AggFuncMin:         1.0,
	ast.AggFuncGroupConcat: 1.0,
	ast.AggFuncBitOr:       0.9,
	ast.AggFuncBitXor:      0.9,
	ast.AggFuncBitAnd:      0.9,
	ast.AggFuncVarPop:      3.0,
	ast.AggFuncVarSamp:     3.0,
	ast.AggFuncStddevPop:   3.0,
	ast.AggFuncStddevSamp:  3.0,
	"default":              1.5,
}

// PlanCounterTp is used in hint nth_plan() to indicate which plan to use.
type PlanCounterTp int64

// PlanCounterDisabled is the default value of PlanCounterTp, indicating that optimizer needn't force a plan.
var PlanCounterDisabled PlanCounterTp = -1

// Dec minus PlanCounterTp value by x.
func (c *PlanCounterTp) Dec(x int64) {
	if *c <= 0 {
		return
	}
	*c = PlanCounterTp(int64(*c) - x)
	if *c < 0 {
		*c = 0
	}
}

// Empty indicates whether the PlanCounterTp is clear now.
func (c *PlanCounterTp) Empty() bool {
	return *c == 0
}

// IsForce indicates whether to force a plan.
func (c *PlanCounterTp) IsForce() bool {
	return *c != -1
}

// wholeTaskTypes records all possible HoTTs of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = [...]property.TaskType{property.INTERLOCKSingleReadTaskType, property.CoFIDeloubleReadTaskType, property.RootTaskType}

var invalidTask = &rootTask{cst: math.MaxFloat64}

// GetPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns.
func GetPropByOrderByItems(items []*soliton.ByItems) (*property.PhysicalProperty, bool) {
	propItems := make([]property.Item, 0, len(items))
	for _, item := range items {
		col, ok := item.Expr.(*expression.DeferredCauset)
		if !ok {
			return nil, false
		}
		propItems = append(propItems, property.Item{DefCaus: col, Desc: item.Desc})
	}
	return &property.PhysicalProperty{Items: propItems}, true
}

// GetPropByOrderByItemsContainScalarFunc will check if this sort property can be pushed or not. In order to simplify the
// problem, we only consider the case that all expression are columns or some special scalar functions.
func GetPropByOrderByItemsContainScalarFunc(items []*soliton.ByItems) (*property.PhysicalProperty, bool, bool) {
	propItems := make([]property.Item, 0, len(items))
	onlyDeferredCauset := true
	for _, item := range items {
		switch expr := item.Expr.(type) {
		case *expression.DeferredCauset:
			propItems = append(propItems, property.Item{DefCaus: expr, Desc: item.Desc})
		case *expression.ScalarFunction:
			col, desc := expr.GetSingleDeferredCauset(item.Desc)
			if col == nil {
				return nil, false, false
			}
			propItems = append(propItems, property.Item{DefCaus: col, Desc: desc})
			onlyDeferredCauset = false
		default:
			return nil, false, false
		}
	}
	return &property.PhysicalProperty{Items: propItems}, true, onlyDeferredCauset
}

func (p *LogicalBlockDual) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp) (task, int64, error) {
	// If the required property is not empty and the event count > 1,
	// we cannot ensure this required property.
	// But if the event count is 0 or 1, we don't need to care about the property.
	if (!prop.IsEmpty() && p.RowCount > 1) || planCounter.Empty() {
		return invalidTask, 0, nil
	}
	dual := PhysicalBlockDual{
		RowCount: p.RowCount,
	}.Init(p.ctx, p.stats, p.blockOffset)
	dual.SetSchema(p.schemaReplicant)
	planCounter.Dec(1)
	return &rootTask{p: dual}, 1, nil
}

func (p *LogicalShow) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp) (task, int64, error) {
	if !prop.IsEmpty() || planCounter.Empty() {
		return invalidTask, 0, nil
	}
	pShow := PhysicalShow{ShowContents: p.ShowContents}.Init(p.ctx)
	pShow.SetSchema(p.schemaReplicant)
	planCounter.Dec(1)
	return &rootTask{p: pShow}, 1, nil
}

func (p *LogicalShowDBSJobs) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp) (task, int64, error) {
	if !prop.IsEmpty() || planCounter.Empty() {
		return invalidTask, 0, nil
	}
	pShow := PhysicalShowDBSJobs{JobNumber: p.JobNumber}.Init(p.ctx)
	pShow.SetSchema(p.schemaReplicant)
	planCounter.Dec(1)
	return &rootTask{p: pShow}, 1, nil
}

// rebuildChildTasks rebuilds the childTasks to make the clock_th combination.
func (p *baseLogicalPlan) rebuildChildTasks(childTasks *[]task, pp PhysicalPlan, childCnts []int64, planCounter int64, TS uint64) error {
	// The taskMap of children nodes should be rolled back first.
	for _, child := range p.children {
		child.rollBackTaskMap(TS)
	}

	multAll := int64(1)
	var curClock PlanCounterTp
	for _, x := range childCnts {
		multAll *= x
	}
	*childTasks = (*childTasks)[:0]
	for j, child := range p.children {
		multAll /= childCnts[j]
		curClock = PlanCounterTp((planCounter-1)/multAll + 1)
		childTask, _, err := child.findBestTask(pp.GetChildReqProps(j), &curClock)
		planCounter = (planCounter-1)%multAll + 1
		if err != nil {
			return err
		}
		if curClock != 0 {
			return errors.Errorf("PlanCounterTp planCounter is not handled")
		}
		if childTask != nil && childTask.invalid() {
			return errors.Errorf("The current plan is invalid, please skip this plan.")
		}
		*childTasks = append(*childTasks, childTask)
	}
	return nil
}

func (p *baseLogicalPlan) enumeratePhysicalPlans4Task(physicalPlans []PhysicalPlan, prop *property.PhysicalProperty, planCounter *PlanCounterTp) (task, int64, error) {
	var bestTask task = invalidTask
	var curCntPlan, cntPlan int64
	childTasks := make([]task, 0, len(p.children))
	childCnts := make([]int64, len(p.children))
	cntPlan = 0
	for _, pp := range physicalPlans {
		// Find best child tasks firstly.
		childTasks = childTasks[:0]
		// The curCntPlan records the number of possible plans for pp
		curCntPlan = 1
		TimeStampNow := p.GetlogicalTS4TaskMap()
		savedPlanID := p.ctx.GetStochastikVars().PlanID
		for j, child := range p.children {
			childTask, cnt, err := child.findBestTask(pp.GetChildReqProps(j), &PlanCounterDisabled)
			childCnts[j] = cnt
			if err != nil {
				return nil, 0, err
			}
			curCntPlan = curCntPlan * cnt
			if childTask != nil && childTask.invalid() {
				break
			}
			childTasks = append(childTasks, childTask)
		}

		// This check makes sure that there is no invalid child task.
		if len(childTasks) != len(p.children) {
			continue
		}

		// If the target plan can be found in this physicalPlan(pp), rebuild childTasks to build the corresponding combination.
		if planCounter.IsForce() && int64(*planCounter) <= curCntPlan {
			p.ctx.GetStochastikVars().PlanID = savedPlanID
			curCntPlan = int64(*planCounter)
			err := p.rebuildChildTasks(&childTasks, pp, childCnts, int64(*planCounter), TimeStampNow)
			if err != nil {
				return nil, 0, err
			}
		}

		// Combine best child tasks with parent physical plan.
		curTask := pp.attach2Task(childTasks...)

		if prop.IsFlashOnlyProp() {
			if _, ok := curTask.(*INTERLOCKTask); !ok {
				continue
			}
		}

		// Enforce curTask property
		if prop.Enforced {
			curTask = enforceProperty(prop, curTask, p.basePlan.ctx)
		}

		// Optimize by shuffle executor to running in parallel manner.
		if prop.IsEmpty() {
			// Currently, we do not regard shuffled plan as a new plan.
			curTask = optimizeByShuffle(pp, curTask, p.basePlan.ctx)
		}

		cntPlan += curCntPlan
		planCounter.Dec(curCntPlan)

		if planCounter.Empty() {
			bestTask = curTask
			break
		}

		// Get the most efficient one.
		if curTask.cost() < bestTask.cost() || (bestTask.invalid() && !curTask.invalid()) {
			bestTask = curTask
		}
	}
	return bestTask, cntPlan, nil
}

// findBestTask implements LogicalPlan interface.
func (p *baseLogicalPlan) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp) (bestTask task, cntPlan int64, err error) {
	// If p is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		return nil, 1, nil
	}
	// Look up the task with this prop in the task map.
	// It's used to reduce double counting.
	bestTask = p.getTask(prop)
	if bestTask != nil {
		planCounter.Dec(1)
		return bestTask, 1, nil
	}

	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.INTERLOCKTiFlashLocalReadTaskType && prop.TaskTp != property.INTERLOCKTiFlashGlobalReadTaskType {
		// Currently all plan cannot totally push down.
		p.storeTask(prop, invalidTask)
		return invalidTask, 0, nil
	}

	bestTask = invalidTask
	cntPlan = 0
	// prop should be read only because its cached hashcode might be not consistent
	// when it is changed. So we clone a new one for the temporary changes.
	newProp := prop.Clone()
	newProp.Enforced = prop.Enforced
	var plansFitsProp, plansNeedEnforce []PhysicalPlan
	var hintWorksWithProp bool
	// Maybe the plan can satisfy the required property,
	// so we try to get the task without the enforced sort first.
	plansFitsProp, hintWorksWithProp = p.self.exhaustPhysicalPlans(newProp)
	if !hintWorksWithProp && !newProp.IsEmpty() {
		// If there is a hint in the plan and the hint cannot satisfy the property,
		// we enforce this property and try to generate the PhysicalPlan again to
		// make sure the hint can work.
		newProp.Enforced = true
	}

	if newProp.Enforced {
		// Then, we use the empty property to get physicalPlans and
		// try to get the task with an enforced sort.
		newProp.Items = []property.Item{}
		newProp.ExpectedCnt = math.MaxFloat64
		var hintCanWork bool
		plansNeedEnforce, hintCanWork = p.self.exhaustPhysicalPlans(newProp)
		if hintCanWork && !hintWorksWithProp {
			// If the hint can work with the empty property, but cannot work with
			// the required property, we give up `plansFitProp` to make sure the hint
			// can work.
			plansFitsProp = nil
		}
		if !hintCanWork && !hintWorksWithProp && !prop.Enforced {
			// If the original property is not enforced and hint cannot
			// work anyway, we give up `plansNeedEnforce` for efficiency,
			plansNeedEnforce = nil
		}
		newProp.Items = prop.Items
		newProp.ExpectedCnt = prop.ExpectedCnt
	}

	newProp.Enforced = false
	var cnt int64
	var curTask task
	if bestTask, cnt, err = p.enumeratePhysicalPlans4Task(plansFitsProp, newProp, planCounter); err != nil {
		return nil, 0, err
	}
	cntPlan += cnt
	if planCounter.Empty() {
		goto END
	}

	newProp.Enforced = true
	curTask, cnt, err = p.enumeratePhysicalPlans4Task(plansNeedEnforce, newProp, planCounter)
	if err != nil {
		return nil, 0, err
	}
	cntPlan += cnt
	if planCounter.Empty() {
		bestTask = curTask
		goto END
	}
	if curTask.cost() < bestTask.cost() || (bestTask.invalid() && !curTask.invalid()) {
		bestTask = curTask
	}

END:
	p.storeTask(prop, bestTask)
	return bestTask, cntPlan, nil
}

func (p *LogicalMemBlock) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp) (t task, cntPlan int64, err error) {
	if !prop.IsEmpty() || planCounter.Empty() {
		return invalidTask, 0, nil
	}
	memBlock := PhysicalMemBlock{
		DBName:          p.DBName,
		Block:           p.BlockInfo,
		DeferredCausets: p.BlockInfo.DeferredCausets,
		Extractor:       p.Extractor,
		QueryTimeRange:  p.QueryTimeRange,
	}.Init(p.ctx, p.stats, p.blockOffset)
	memBlock.SetSchema(p.schemaReplicant)
	planCounter.Dec(1)
	return &rootTask{p: memBlock}, 1, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return block dual.
func (ds *DataSource) tryToGetDualTask() (task, error) {
	for _, cond := range ds.pushedDownConds {
		if con, ok := cond.(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
			result, _, err := expression.EvalBool(ds.ctx, []expression.Expression{cond}, chunk.Row{})
			if err != nil {
				return nil, err
			}
			if !result {
				dual := PhysicalBlockDual{}.Init(ds.ctx, ds.stats, ds.blockOffset)
				dual.SetSchema(ds.schemaReplicant)
				return &rootTask{
					p: dual,
				}, nil
			}
		}
	}
	return nil, nil
}

// candidatePath is used to maintain required info for skyline pruning.
type candidatePath struct {
	path         *soliton.AccessPath
	columnSet    *intsets.Sparse // columnSet is the set of columns that occurred in the access conditions.
	isSingleScan bool
	isMatchProp  bool
}

// compareDeferredCausetSet will compares the two set. The last return value is used to indicate
// if they are comparable, it is false when both two sets have columns that do not occur in the other.
// When the second return value is true, the value of first:
// (1) -1 means that `l` is a strict subset of `r`;
// (2) 0 means that `l` equals to `r`;
// (3) 1 means that `l` is a strict superset of `r`.
func compareDeferredCausetSet(l, r *intsets.Sparse) (int, bool) {
	lLen, rLen := l.Len(), r.Len()
	if lLen < rLen {
		// -1 is meaningful only when l.SubsetOf(r) is true.
		return -1, l.SubsetOf(r)
	}
	if lLen == rLen {
		// 0 is meaningful only when l.SubsetOf(r) is true.
		return 0, l.SubsetOf(r)
	}
	// 1 is meaningful only when r.SubsetOf(l) is true.
	return 1, r.SubsetOf(l)
}

func compareBool(l, r bool) int {
	if l == r {
		return 0
	}
	if !l {
		return -1
	}
	return 1
}

// compareCandidates is the core of skyline pruning. It compares the two candidate paths on three dimensions:
// (1): the set of columns that occurred in the access condition,
// (2): whether or not it matches the physical property
// (3): does it require a double scan.
// If `x` is not worse than `y` at all factors,
// and there exists one factor that `x` is better than `y`, then `x` is better than `y`.
func compareCandidates(lhs, rhs *candidatePath) int {
	setsResult, comparable := compareDeferredCausetSet(lhs.columnSet, rhs.columnSet)
	if !comparable {
		return 0
	}
	scanResult := compareBool(lhs.isSingleScan, rhs.isSingleScan)
	matchResult := compareBool(lhs.isMatchProp, rhs.isMatchProp)
	sum := setsResult + scanResult + matchResult
	if setsResult >= 0 && scanResult >= 0 && matchResult >= 0 && sum > 0 {
		return 1
	}
	if setsResult <= 0 && scanResult <= 0 && matchResult <= 0 && sum < 0 {
		return -1
	}
	return 0
}

func (ds *DataSource) getBlockCandidate(path *soliton.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	if path.IsIntHandlePath {
		pkDefCaus := ds.getPKIsHandleDefCaus()
		if len(prop.Items) == 1 && pkDefCaus != nil {
			candidate.isMatchProp = prop.Items[0].DefCaus.Equal(nil, pkDefCaus)
			if path.StoreType == ekv.TiFlash {
				candidate.isMatchProp = candidate.isMatchProp && !prop.Items[0].Desc
			}
		}
	} else {
		all, _ := prop.AllSameOrder()
		// When the prop is empty or `all` is false, `isMatchProp` is better to be `false` because
		// it needs not to keep order for index scan.
		if !prop.IsEmpty() && all {
			for i, col := range path.IdxDefCauss {
				if col.Equal(nil, prop.Items[0].DefCaus) {
					candidate.isMatchProp = matchIndicesProp(path.IdxDefCauss[i:], path.IdxDefCausLens[i:], prop.Items)
					break
				} else if i >= path.EqCondCount {
					break
				}
			}
		}
	}
	candidate.columnSet = expression.ExtractDeferredCausetSet(path.AccessConds)
	candidate.isSingleScan = true
	return candidate
}

func (ds *DataSource) getIndexCandidate(path *soliton.AccessPath, prop *property.PhysicalProperty, isSingleScan bool) *candidatePath {
	candidate := &candidatePath{path: path}
	all, _ := prop.AllSameOrder()
	// When the prop is empty or `all` is false, `isMatchProp` is better to be `false` because
	// it needs not to keep order for index scan.
	if !prop.IsEmpty() && all {
		for i, col := range path.IdxDefCauss {
			if col.Equal(nil, prop.Items[0].DefCaus) {
				candidate.isMatchProp = matchIndicesProp(path.IdxDefCauss[i:], path.IdxDefCausLens[i:], prop.Items)
				break
			} else if i >= path.EqCondCount {
				break
			}
		}
	}
	candidate.columnSet = expression.ExtractDeferredCausetSet(path.AccessConds)
	candidate.isSingleScan = isSingleScan
	return candidate
}

func (ds *DataSource) getIndexMergeCandidate(path *soliton.AccessPath) *candidatePath {
	candidate := &candidatePath{path: path}
	return candidate
}

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func (ds *DataSource) skylinePruning(prop *property.PhysicalProperty) []*candidatePath {
	candidates := make([]*candidatePath, 0, 4)
	for _, path := range ds.possibleAccessPaths {
		if path.PartialIndexPaths != nil {
			candidates = append(candidates, ds.getIndexMergeCandidate(path))
			continue
		}
		// if we already know the range of the scan is empty, just return a BlockDual
		if len(path.Ranges) == 0 && !ds.ctx.GetStochastikVars().StmtCtx.UseCache {
			return []*candidatePath{{path: path}}
		}
		if path.StoreType != ekv.TiFlash && (prop.TaskTp == property.INTERLOCKTiFlashLocalReadTaskType || prop.TaskTp == property.INTERLOCKTiFlashGlobalReadTaskType) {
			continue
		}
		var currentCandidate *candidatePath
		if path.IsBlockPath() {
			if path.StoreType == ekv.TiFlash {
				if path.IsTiFlashGlobalRead && prop.TaskTp == property.INTERLOCKTiFlashGlobalReadTaskType {
					currentCandidate = ds.getBlockCandidate(path, prop)
				}
				if !path.IsTiFlashGlobalRead && prop.TaskTp != property.INTERLOCKTiFlashGlobalReadTaskType {
					currentCandidate = ds.getBlockCandidate(path, prop)
				}
			} else {
				if !path.IsTiFlashGlobalRead && !prop.IsFlashOnlyProp() {
					currentCandidate = ds.getBlockCandidate(path, prop)
				}
			}
			if currentCandidate == nil {
				continue
			}
		} else {
			coveredByIdx := ds.isCoveringIndex(ds.schemaReplicant.DeferredCausets, path.FullIdxDefCauss, path.FullIdxDefCausLens, ds.blockInfo)
			if len(path.AccessConds) > 0 || !prop.IsEmpty() || path.Forced || coveredByIdx {
				// We will use index to generate physical plan if any of the following conditions is satisfied:
				// 1. This path's access cond is not nil.
				// 2. We have a non-empty prop to match.
				// 3. This index is forced to choose.
				// 4. The needed columns are all covered by index columns(and handleDefCaus).
				currentCandidate = ds.getIndexCandidate(path, prop, coveredByIdx)
			} else {
				continue
			}
		}
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			if candidates[i].path.StoreType == ekv.TiFlash {
				continue
			}
			result := compareCandidates(candidates[i], currentCandidate)
			if result == 1 {
				pruned = true
				// We can break here because the current candidate cannot prune others anymore.
				break
			} else if result == -1 {
				candidates = append(candidates[:i], candidates[i+1:]...)
			}
		}
		if !pruned {
			candidates = append(candidates, currentCandidate)
		}
	}
	return candidates
}

// findBestTask implements the PhysicalPlan interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (ds *DataSource) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp) (t task, cntPlan int64, err error) {
	// If ds is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		planCounter.Dec(1)
		return nil, 1, nil
	}

	t = ds.getTask(prop)
	if t != nil {
		cntPlan = 1
		planCounter.Dec(1)
		return
	}
	var cnt int64
	// If prop.enforced is true, the prop.defcaus need to be set nil for ds.findBestTask.
	// Before function return, reset it for enforcing task prop and storing map<prop,task>.
	oldPropDefCauss := prop.Items
	if prop.Enforced {
		// First, get the bestTask without enforced prop
		prop.Enforced = false
		t, cnt, err = ds.findBestTask(prop, planCounter)
		if err != nil {
			return nil, 0, err
		}
		prop.Enforced = true
		if t != invalidTask {
			ds.storeTask(prop, t)
			cntPlan = cnt
			return
		}
		// Next, get the bestTask with enforced prop
		prop.Items = []property.Item{}
	}
	defer func() {
		if err != nil {
			return
		}
		if prop.Enforced {
			prop.Items = oldPropDefCauss
			t = enforceProperty(prop, t, ds.basePlan.ctx)
		}
		ds.storeTask(prop, t)
	}()

	t, err = ds.tryToGetDualTask()
	if err != nil || t != nil {
		planCounter.Dec(1)
		return t, 1, err
	}

	t = invalidTask
	candidates := ds.skylinePruning(prop)

	cntPlan = 0
	for _, candidate := range candidates {
		path := candidate.path
		if path.PartialIndexPaths != nil {
			idxMergeTask, err := ds.convertToIndexMergeScan(prop, candidate)
			if err != nil {
				return nil, 0, err
			}
			if !idxMergeTask.invalid() {
				cntPlan += 1
				planCounter.Dec(1)
			}
			if idxMergeTask.cost() < t.cost() || planCounter.Empty() {
				t = idxMergeTask
			}
			if planCounter.Empty() {
				return t, cntPlan, nil
			}
			continue
		}
		// if we already know the range of the scan is empty, just return a BlockDual
		if len(path.Ranges) == 0 && !ds.ctx.GetStochastikVars().StmtCtx.UseCache {
			dual := PhysicalBlockDual{}.Init(ds.ctx, ds.stats, ds.blockOffset)
			dual.SetSchema(ds.schemaReplicant)
			cntPlan += 1
			planCounter.Dec(1)
			return &rootTask{
				p: dual,
			}, cntPlan, nil
		}
		canConvertPointGet := (!ds.isPartition && len(path.Ranges) > 0) || (ds.isPartition && len(path.Ranges) == 1)
		canConvertPointGet = canConvertPointGet && candidate.path.StoreType != ekv.TiFlash
		if !candidate.path.IsIntHandlePath {
			canConvertPointGet = canConvertPointGet &&
				candidate.path.Index.Unique && !candidate.path.Index.HasPrefixIndex()
			idxDefCaussLen := len(candidate.path.Index.DeferredCausets)
			for _, ran := range candidate.path.Ranges {
				if len(ran.LowVal) != idxDefCaussLen {
					canConvertPointGet = false
					break
				}
			}
		}
		if ds.block.Meta().GetPartitionInfo() != nil && ds.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
			canConvertPointGet = false
		}
		if canConvertPointGet {
			allRangeIsPoint := true
			for _, ran := range path.Ranges {
				if !ran.IsPoint(ds.ctx.GetStochastikVars().StmtCtx) {
					allRangeIsPoint = false
					break
				}
			}
			if allRangeIsPoint {
				var pointGetTask task
				if len(path.Ranges) == 1 {
					pointGetTask = ds.convertToPointGet(prop, candidate)
				} else {
					pointGetTask = ds.convertToBatchPointGet(prop, candidate)
				}
				if !pointGetTask.invalid() {
					cntPlan += 1
					planCounter.Dec(1)
				}
				if pointGetTask.cost() < t.cost() || planCounter.Empty() {
					t = pointGetTask
					if planCounter.Empty() {
						return
					}
					continue
				}
			}
		}
		if path.IsBlockPath() {
			if ds.preferStoreType&preferTiFlash != 0 && path.StoreType == ekv.EinsteinDB {
				continue
			}
			if ds.preferStoreType&preferEinsteinDB != 0 && path.StoreType == ekv.TiFlash {
				continue
			}
			tblTask, err := ds.convertToBlockScan(prop, candidate)
			if err != nil {
				return nil, 0, err
			}
			if !tblTask.invalid() {
				cntPlan += 1
				planCounter.Dec(1)
			}
			if tblTask.cost() < t.cost() || planCounter.Empty() {
				t = tblTask
			}
			if planCounter.Empty() {
				return t, cntPlan, nil
			}
			continue
		}
		// TiFlash storage do not support index scan.
		if ds.preferStoreType&preferTiFlash != 0 {
			continue
		}
		idxTask, err := ds.convertToIndexScan(prop, candidate)
		if err != nil {
			return nil, 0, err
		}
		if !idxTask.invalid() {
			cntPlan += 1
			planCounter.Dec(1)
		}
		if idxTask.cost() < t.cost() || planCounter.Empty() {
			t = idxTask
		}
		if planCounter.Empty() {
			return t, cntPlan, nil
		}
	}

	return
}

func (ds *DataSource) convertToIndexMergeScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	if prop.TaskTp != property.RootTaskType || !prop.IsEmpty() {
		return invalidTask, nil
	}
	path := candidate.path
	var totalCost, totalRowCount float64
	scans := make([]PhysicalPlan, 0, len(path.PartialIndexPaths))
	INTERLOCK := &INTERLOCKTask{
		indexPlanFinished: true,
		tblDefCausHists:   ds.TblDefCausHists,
	}
	INTERLOCK.partitionInfo = PartitionInfo{
		PruningConds:        ds.allConds,
		PartitionNames:      ds.partitionNames,
		DeferredCausets:     ds.TblDefCauss,
		DeferredCausetNames: ds.names,
	}
	for _, partPath := range path.PartialIndexPaths {
		var scan PhysicalPlan
		var partialCost, rowCount float64
		if partPath.IsBlockPath() {
			scan, partialCost, rowCount = ds.convertToPartialBlockScan(prop, partPath)
		} else {
			scan, partialCost, rowCount = ds.convertToPartialIndexScan(prop, partPath)
		}
		scans = append(scans, scan)
		totalCost += partialCost
		totalRowCount += rowCount
	}

	ts, partialCost, err := ds.buildIndexMergeBlockScan(prop, path.BlockFilters, totalRowCount)
	if err != nil {
		return nil, err
	}
	totalCost += partialCost
	INTERLOCK.blockPlan = ts
	INTERLOCK.idxMergePartPlans = scans
	INTERLOCK.cst = totalCost
	task = finishINTERLOCKTask(ds.ctx, INTERLOCK)
	return task, nil
}

func (ds *DataSource) convertToPartialIndexScan(prop *property.PhysicalProperty, path *soliton.AccessPath) (
	indexPlan PhysicalPlan,
	partialCost float64,
	rowCount float64) {
	idx := path.Index
	is, partialCost, rowCount := ds.getOriginalPhysicalIndexScan(prop, path, false, false)
	rowSize := is.indexScanRowSize(idx, ds, false)
	// TODO: Consider using isCoveringIndex() to avoid another BlockRead
	indexConds := path.IndexFilters
	sessVars := ds.ctx.GetStochastikVars()
	if indexConds != nil {
		var selectivity float64
		partialCost += rowCount * sessVars.INTERLOCKCPUFactor
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		rowCount = is.stats.RowCount * selectivity
		stats := &property.StatsInfo{RowCount: rowCount}
		stats.StatsVersion = ds.statisticBlock.Version
		if ds.statisticBlock.Pseudo {
			stats.StatsVersion = statistics.PseudoVersion
		}
		indexPlan := PhysicalSelection{Conditions: indexConds}.Init(is.ctx, stats, ds.blockOffset)
		indexPlan.SetChildren(is)
		partialCost += rowCount * rowSize * sessVars.NetworkFactor
		return indexPlan, partialCost, rowCount
	}
	partialCost += rowCount * rowSize * sessVars.NetworkFactor
	indexPlan = is
	return indexPlan, partialCost, rowCount
}

func (ds *DataSource) convertToPartialBlockScan(prop *property.PhysicalProperty, path *soliton.AccessPath) (
	blockPlan PhysicalPlan,
	partialCost float64,
	rowCount float64) {
	ts, partialCost, rowCount := ds.getOriginalPhysicalBlockScan(prop, path, false)
	rowSize := ds.TblDefCausHists.GetAvgRowSize(ds.ctx, ds.TblDefCauss, false, false)
	sessVars := ds.ctx.GetStochastikVars()
	if len(ts.filterCondition) > 0 {
		selectivity, _, err := ds.blockStats.HistDefCausl.Selectivity(ds.ctx, ts.filterCondition, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		blockPlan = PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.ctx, ts.stats.ScaleByExpectCnt(selectivity*rowCount), ds.blockOffset)
		blockPlan.SetChildren(ts)
		partialCost += rowCount * sessVars.INTERLOCKCPUFactor
		partialCost += selectivity * rowCount * rowSize * sessVars.NetworkFactor
		return blockPlan, partialCost, rowCount
	}
	partialCost += rowCount * rowSize * sessVars.NetworkFactor
	blockPlan = ts
	return blockPlan, partialCost, rowCount
}

func (ds *DataSource) buildIndexMergeBlockScan(prop *property.PhysicalProperty, blockFilters []expression.Expression, totalRowCount float64) (PhysicalPlan, float64, error) {
	var partialCost float64
	sessVars := ds.ctx.GetStochastikVars()
	ts := PhysicalBlockScan{
		Block:           ds.blockInfo,
		DeferredCausets: ds.DeferredCausets,
		BlockAsName:     ds.BlockAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalBlockID: ds.physicalBlockID,
		HandleDefCauss:  ds.handleDefCauss,
	}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.schemaReplicant.Clone())
	if ts.HandleDefCauss == nil {
		handleDefCaus := ds.getPKIsHandleDefCaus()
		if handleDefCaus == nil {
			handleDefCaus, _ = ts.appendExtraHandleDefCaus(ds)
		}
		ts.HandleDefCauss = NewIntHandleDefCauss(handleDefCaus)
	}
	var err error
	ts.HandleDefCauss, err = ts.HandleDefCauss.ResolveIndices(ts.schemaReplicant)
	if err != nil {
		return nil, 0, err
	}
	if ts.Block.PKIsHandle {
		if pkDefCausInfo := ts.Block.GetPkDefCausInfo(); pkDefCausInfo != nil {
			if ds.statisticBlock.DeferredCausets[pkDefCausInfo.ID] != nil {
				ts.Hist = &ds.statisticBlock.DeferredCausets[pkDefCausInfo.ID].Histogram
			}
		}
	}
	rowSize := ds.TblDefCausHists.GetBlockAvgRowSize(ds.ctx, ds.TblDefCauss, ts.StoreType, true)
	partialCost += totalRowCount * rowSize * sessVars.ScanFactor
	ts.stats = ds.blockStats.ScaleByExpectCnt(totalRowCount)
	if ds.statisticBlock.Pseudo {
		ts.stats.StatsVersion = statistics.PseudoVersion
	}
	if len(blockFilters) > 0 {
		partialCost += totalRowCount * sessVars.INTERLOCKCPUFactor
		selectivity, _, err := ds.blockStats.HistDefCausl.Selectivity(ds.ctx, blockFilters, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		sel := PhysicalSelection{Conditions: blockFilters}.Init(ts.ctx, ts.stats.ScaleByExpectCnt(selectivity*totalRowCount), ts.blockOffset)
		sel.SetChildren(ts)
		return sel, partialCost, nil
	}
	return ts, partialCost, nil
}

func indexCoveringDefCaus(col *expression.DeferredCauset, indexDefCauss []*expression.DeferredCauset, idxDefCausLens []int) bool {
	for i, indexDefCaus := range indexDefCauss {
		isFullLen := idxDefCausLens[i] == types.UnspecifiedLength || idxDefCausLens[i] == col.RetType.Flen
		if indexDefCaus != nil && col.Equal(nil, indexDefCaus) && isFullLen {
			return true
		}
	}
	return false
}

func (ds *DataSource) isCoveringIndex(columns, indexDeferredCausets []*expression.DeferredCauset, idxDefCausLens []int, tblInfo *perceptron.BlockInfo) bool {
	for _, col := range columns {
		if tblInfo.PKIsHandle && allegrosql.HasPriKeyFlag(col.RetType.Flag) {
			continue
		}
		if col.ID == perceptron.ExtraHandleID {
			continue
		}
		coveredByPlainIndex := indexCoveringDefCaus(col, indexDeferredCausets, idxDefCausLens)
		coveredByClusteredIndex := indexCoveringDefCaus(col, ds.commonHandleDefCauss, ds.commonHandleLens)
		if !coveredByPlainIndex && !coveredByClusteredIndex {
			return false
		}

		isClusteredNewDefCauslationIdx := collate.NewDefCauslationEnabled() &&
			col.GetType().EvalType() == types.ETString &&
			!allegrosql.HasBinaryFlag(col.GetType().Flag)
		if !coveredByPlainIndex && coveredByClusteredIndex && isClusteredNewDefCauslationIdx {
			return false
		}
	}
	return true
}

// If there is a block reader which needs to keep order, we should append a pk to block scan.
func (ts *PhysicalBlockScan) appendExtraHandleDefCaus(ds *DataSource) (*expression.DeferredCauset, bool) {
	handleDefCauss := ds.handleDefCauss
	if handleDefCauss != nil {
		return handleDefCauss.GetDefCaus(0), false
	}
	handleDefCaus := ds.newExtraHandleSchemaDefCaus()
	ts.schemaReplicant.Append(handleDefCaus)
	ts.DeferredCausets = append(ts.DeferredCausets, perceptron.NewExtraHandleDefCausInfo())
	return handleDefCaus, true
}

// convertToIndexScan converts the DataSource to index scan with idx.
func (ds *DataSource) convertToIndexScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	if !candidate.isSingleScan {
		// If it's parent requires single read task, return max cost.
		if prop.TaskTp == property.INTERLOCKSingleReadTaskType {
			return invalidTask, nil
		}
	} else if prop.TaskTp == property.CoFIDeloubleReadTaskType {
		// If it's parent requires double read task, return max cost.
		return invalidTask, nil
	}
	if !prop.IsEmpty() && !candidate.isMatchProp {
		return invalidTask, nil
	}
	path := candidate.path
	is, cost, _ := ds.getOriginalPhysicalIndexScan(prop, path, candidate.isMatchProp, candidate.isSingleScan)
	INTERLOCK := &INTERLOCKTask{
		indexPlan:       is,
		tblDefCausHists: ds.TblDefCausHists,
		tblDefCauss:     ds.TblDefCauss,
	}
	INTERLOCK.partitionInfo = PartitionInfo{
		PruningConds:        ds.allConds,
		PartitionNames:      ds.partitionNames,
		DeferredCausets:     ds.TblDefCauss,
		DeferredCausetNames: ds.names,
	}
	if !candidate.isSingleScan {
		// On this way, it's double read case.
		ts := PhysicalBlockScan{
			DeferredCausets: ds.DeferredCausets,
			Block:           is.Block,
			BlockAsName:     ds.BlockAsName,
			isPartition:     ds.isPartition,
			physicalBlockID: ds.physicalBlockID,
		}.Init(ds.ctx, is.blockOffset)
		ts.SetSchema(ds.schemaReplicant.Clone())
		INTERLOCK.blockPlan = ts
	}
	INTERLOCK.cst = cost
	task = INTERLOCK
	if INTERLOCK.blockPlan != nil && ds.blockInfo.IsCommonHandle {
		INTERLOCK.commonHandleDefCauss = ds.commonHandleDefCauss
	}
	if candidate.isMatchProp {
		if INTERLOCK.blockPlan != nil && !ds.blockInfo.IsCommonHandle {
			col, isNew := INTERLOCK.blockPlan.(*PhysicalBlockScan).appendExtraHandleDefCaus(ds)
			INTERLOCK.extraHandleDefCaus = col
			INTERLOCK.doubleReadNeedProj = isNew
		}
		INTERLOCK.keepOrder = true
		// IndexScan on partition block can't keep order.
		if ds.blockInfo.GetPartitionInfo() != nil {
			return invalidTask, nil
		}
	}
	// prop.IsEmpty() would always return true when coming to here,
	// so we can just use prop.ExpectedCnt as parameter of addPushedDownSelection.
	finalStats := ds.stats.ScaleByExpectCnt(prop.ExpectedCnt)
	is.addPushedDownSelection(INTERLOCK, ds, path, finalStats)
	if prop.TaskTp == property.RootTaskType {
		task = finishINTERLOCKTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (is *PhysicalIndexScan) indexScanRowSize(idx *perceptron.IndexInfo, ds *DataSource, isForScan bool) float64 {
	scanDefCauss := make([]*expression.DeferredCauset, 0, len(idx.DeferredCausets)+1)
	// If `initSchema` has already appended the handle column in schemaReplicant, just use schemaReplicant columns, otherwise, add extra handle column.
	if len(idx.DeferredCausets) == len(is.schemaReplicant.DeferredCausets) {
		scanDefCauss = append(scanDefCauss, is.schemaReplicant.DeferredCausets...)
		handleDefCaus := ds.getPKIsHandleDefCaus()
		if handleDefCaus != nil {
			scanDefCauss = append(scanDefCauss, handleDefCaus)
		}
	} else {
		scanDefCauss = is.schemaReplicant.DeferredCausets
	}
	if isForScan {
		return ds.TblDefCausHists.GetIndexAvgRowSize(is.ctx, scanDefCauss, is.Index.Unique)
	}
	return ds.TblDefCausHists.GetAvgRowSize(is.ctx, scanDefCauss, true, false)
}

// initSchema is used to set the schemaReplicant of PhysicalIndexScan. Before calling this,
// make sure the following field of PhysicalIndexScan are initialized:
//   PhysicalIndexScan.Block         *perceptron.BlockInfo
//   PhysicalIndexScan.Index         *perceptron.IndexInfo
//   PhysicalIndexScan.Index.DeferredCausets []*IndexDeferredCauset
//   PhysicalIndexScan.IdxDefCauss       []*expression.DeferredCauset
//   PhysicalIndexScan.DeferredCausets       []*perceptron.DeferredCausetInfo
func (is *PhysicalIndexScan) initSchema(idxExprDefCauss []*expression.DeferredCauset, isDoubleRead bool) {
	indexDefCauss := make([]*expression.DeferredCauset, len(is.IdxDefCauss), len(is.Index.DeferredCausets)+1)
	INTERLOCKy(indexDefCauss, is.IdxDefCauss)

	for i := len(is.IdxDefCauss); i < len(is.Index.DeferredCausets); i++ {
		if idxExprDefCauss[i] != nil {
			indexDefCauss = append(indexDefCauss, idxExprDefCauss[i])
		} else {
			// TODO: try to reuse the col generated when building the DataSource.
			indexDefCauss = append(indexDefCauss, &expression.DeferredCauset{
				ID:       is.Block.DeferredCausets[is.Index.DeferredCausets[i].Offset].ID,
				RetType:  &is.Block.DeferredCausets[is.Index.DeferredCausets[i].Offset].FieldType,
				UniqueID: is.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			})
		}
	}
	is.NeedCommonHandle = is.Block.IsCommonHandle

	if is.NeedCommonHandle {
		for i := len(is.Index.DeferredCausets); i < len(idxExprDefCauss); i++ {
			indexDefCauss = append(indexDefCauss, idxExprDefCauss[i])
		}
		is.SetSchema(expression.NewSchema(indexDefCauss...))
		return
	}
	setHandle := len(indexDefCauss) > len(is.Index.DeferredCausets)
	if !setHandle {
		for i, col := range is.DeferredCausets {
			if (allegrosql.HasPriKeyFlag(col.Flag) && is.Block.PKIsHandle) || col.ID == perceptron.ExtraHandleID {
				indexDefCauss = append(indexDefCauss, is.dataSourceSchema.DeferredCausets[i])
				setHandle = true
				break
			}
		}
	}
	// If it's double read case, the first index must return handle. So we should add extra handle column
	// if there isn't a handle column.
	if isDoubleRead && !setHandle {
		if !is.Block.IsCommonHandle {
			indexDefCauss = append(indexDefCauss, &expression.DeferredCauset{
				RetType:  types.NewFieldType(allegrosql.TypeLonglong),
				ID:       perceptron.ExtraHandleID,
				UniqueID: is.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			})
		}
	}

	is.SetSchema(expression.NewSchema(indexDefCauss...))
}

func (is *PhysicalIndexScan) addPushedDownSelection(INTERLOCKTask *INTERLOCKTask, p *DataSource, path *soliton.AccessPath, finalStats *property.StatsInfo) {
	// Add filter condition to block plan now.
	indexConds, blockConds := path.IndexFilters, path.BlockFilters

	blockConds, INTERLOCKTask.rootTaskConds = SplitSelCondsWithVirtualDeferredCauset(blockConds)

	var newRootConds []expression.Expression
	indexConds, newRootConds = expression.PushDownExprs(is.ctx.GetStochastikVars().StmtCtx, indexConds, is.ctx.GetClient(), ekv.EinsteinDB)
	INTERLOCKTask.rootTaskConds = append(INTERLOCKTask.rootTaskConds, newRootConds...)

	blockConds, newRootConds = expression.PushDownExprs(is.ctx.GetStochastikVars().StmtCtx, blockConds, is.ctx.GetClient(), ekv.EinsteinDB)
	INTERLOCKTask.rootTaskConds = append(INTERLOCKTask.rootTaskConds, newRootConds...)

	sessVars := is.ctx.GetStochastikVars()
	if indexConds != nil {
		INTERLOCKTask.cst += INTERLOCKTask.count() * sessVars.INTERLOCKCPUFactor
		var selectivity float64
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		count := is.stats.RowCount * selectivity
		stats := p.blockStats.ScaleByExpectCnt(count)
		indexSel := PhysicalSelection{Conditions: indexConds}.Init(is.ctx, stats, is.blockOffset)
		indexSel.SetChildren(is)
		INTERLOCKTask.indexPlan = indexSel
	}
	if len(blockConds) > 0 {
		INTERLOCKTask.finishIndexPlan()
		INTERLOCKTask.cst += INTERLOCKTask.count() * sessVars.INTERLOCKCPUFactor
		blockSel := PhysicalSelection{Conditions: blockConds}.Init(is.ctx, finalStats, is.blockOffset)
		blockSel.SetChildren(INTERLOCKTask.blockPlan)
		INTERLOCKTask.blockPlan = blockSel
	}
}

// SplitSelCondsWithVirtualDeferredCauset filter the select conditions which contain virtual column
func SplitSelCondsWithVirtualDeferredCauset(conds []expression.Expression) ([]expression.Expression, []expression.Expression) {
	var filterConds []expression.Expression
	for i := len(conds) - 1; i >= 0; i-- {
		if expression.ContainVirtualDeferredCauset(conds[i : i+1]) {
			filterConds = append(filterConds, conds[i])
			conds = append(conds[:i], conds[i+1:]...)
		}
	}
	return conds, filterConds
}

func matchIndicesProp(idxDefCauss []*expression.DeferredCauset, colLens []int, propItems []property.Item) bool {
	if len(idxDefCauss) < len(propItems) {
		return false
	}
	for i, item := range propItems {
		if colLens[i] != types.UnspecifiedLength || !item.DefCaus.Equal(nil, idxDefCauss[i]) {
			return false
		}
	}
	return true
}

func (ds *DataSource) splitIndexFilterConditions(conditions []expression.Expression, indexDeferredCausets []*expression.DeferredCauset, idxDefCausLens []int,
	block *perceptron.BlockInfo) (indexConds, blockConds []expression.Expression) {
	var indexConditions, blockConditions []expression.Expression
	for _, cond := range conditions {
		if ds.isCoveringIndex(expression.ExtractDeferredCausets(cond), indexDeferredCausets, idxDefCausLens, block) {
			indexConditions = append(indexConditions, cond)
		} else {
			blockConditions = append(blockConditions, cond)
		}
	}
	return indexConditions, blockConditions
}

// getMostCorrDefCausFromExprs checks if column in the condition is correlated enough with handle. If the condition
// contains multiple columns, return nil and get the max correlation, which would be used in the heuristic estimation.
func getMostCorrDefCausFromExprs(exprs []expression.Expression, histDefCausl *statistics.Block, threshold float64) (*expression.DeferredCauset, float64) {
	var defcaus []*expression.DeferredCauset
	defcaus = expression.ExtractDeferredCausetsFromExpressions(defcaus, exprs, nil)
	if len(defcaus) == 0 {
		return nil, 0
	}
	colSet := set.NewInt64Set()
	var corr float64
	var corrDefCaus *expression.DeferredCauset
	for _, col := range defcaus {
		if colSet.Exist(col.UniqueID) {
			continue
		}
		colSet.Insert(col.UniqueID)
		hist, ok := histDefCausl.DeferredCausets[col.ID]
		if !ok {
			continue
		}
		curCorr := math.Abs(hist.Correlation)
		if corrDefCaus == nil || corr < curCorr {
			corrDefCaus = col
			corr = curCorr
		}
	}
	if len(colSet) == 1 && corr >= threshold {
		return corrDefCaus, corr
	}
	return nil, corr
}

// getDeferredCausetRangeCounts estimates event count for each range respectively.
func getDeferredCausetRangeCounts(sc *stmtctx.StatementContext, colID int64, ranges []*ranger.Range, histDefCausl *statistics.Block, idxID int64) ([]float64, bool) {
	var err error
	var count float64
	rangeCounts := make([]float64, len(ranges))
	for i, ran := range ranges {
		if idxID >= 0 {
			idxHist := histDefCausl.Indices[idxID]
			if idxHist == nil || idxHist.IsInvalid(false) {
				return nil, false
			}
			count, err = histDefCausl.GetRowCountByIndexRanges(sc, idxID, []*ranger.Range{ran})
		} else {
			colHist, ok := histDefCausl.DeferredCausets[colID]
			if !ok || colHist.IsInvalid(sc, false) {
				return nil, false
			}
			count, err = histDefCausl.GetRowCountByDeferredCausetRanges(sc, colID, []*ranger.Range{ran})
		}
		if err != nil {
			return nil, false
		}
		rangeCounts[i] = count
	}
	return rangeCounts, true
}

// convertRangeFromExpectedCnt builds new ranges used to estimate event count we need to scan in block scan before finding specified
// number of tuples which fall into input ranges.
func convertRangeFromExpectedCnt(ranges []*ranger.Range, rangeCounts []float64, expectedCnt float64, desc bool) ([]*ranger.Range, float64, bool) {
	var i int
	var count float64
	var convertedRanges []*ranger.Range
	if desc {
		for i = len(ranges) - 1; i >= 0; i-- {
			if count+rangeCounts[i] >= expectedCnt {
				break
			}
			count += rangeCounts[i]
		}
		if i < 0 {
			return nil, 0, true
		}
		convertedRanges = []*ranger.Range{{LowVal: ranges[i].HighVal, HighVal: []types.Causet{types.MaxValueCauset()}, LowExclude: !ranges[i].HighExclude}}
	} else {
		for i = 0; i < len(ranges); i++ {
			if count+rangeCounts[i] >= expectedCnt {
				break
			}
			count += rangeCounts[i]
		}
		if i == len(ranges) {
			return nil, 0, true
		}
		convertedRanges = []*ranger.Range{{LowVal: []types.Causet{{}}, HighVal: ranges[i].LowVal, HighExclude: !ranges[i].LowExclude}}
	}
	return convertedRanges, count, false
}

// crossEstimateRowCount estimates event count of block scan using histogram of another column which is in BlockFilters
// and has high order correlation with handle column. For example, if the query is like:
// `select * from tbl where a = 1 order by pk limit 1`
// if order of column `a` is strictly correlated with column `pk`, the event count of block scan should be:
// `1 + row_count(a < 1 or a is null)`
func (ds *DataSource) crossEstimateRowCount(path *soliton.AccessPath, expectedCnt float64, desc bool) (float64, bool, float64) {
	if ds.statisticBlock.Pseudo || len(path.BlockFilters) == 0 {
		return 0, false, 0
	}
	col, corr := getMostCorrDefCausFromExprs(path.BlockFilters, ds.statisticBlock, ds.ctx.GetStochastikVars().CorrelationThreshold)
	// If block scan is not full range scan, we cannot use histogram of other columns for estimation, because
	// the histogram reflects value distribution in the whole block level.
	if col == nil || len(path.AccessConds) > 0 {
		return 0, false, corr
	}
	colInfoID := col.ID
	colID := col.UniqueID
	colHist := ds.statisticBlock.DeferredCausets[colInfoID]
	if colHist.Correlation < 0 {
		desc = !desc
	}
	accessConds, remained := ranger.DetachCondsForDeferredCauset(ds.ctx, path.BlockFilters, col)
	if len(accessConds) == 0 {
		return 0, false, corr
	}
	sc := ds.ctx.GetStochastikVars().StmtCtx
	ranges, err := ranger.BuildDeferredCausetRange(accessConds, sc, col.RetType, types.UnspecifiedLength)
	if len(ranges) == 0 || err != nil {
		return 0, err == nil, corr
	}
	idxID, idxExists := ds.stats.HistDefCausl.DefCausID2IdxID[colID]
	if !idxExists {
		idxID = -1
	}
	rangeCounts, ok := getDeferredCausetRangeCounts(sc, colInfoID, ranges, ds.statisticBlock, idxID)
	if !ok {
		return 0, false, corr
	}
	convertedRanges, count, isFull := convertRangeFromExpectedCnt(ranges, rangeCounts, expectedCnt, desc)
	if isFull {
		return path.CountAfterAccess, true, 0
	}
	var rangeCount float64
	if idxExists {
		rangeCount, err = ds.statisticBlock.GetRowCountByIndexRanges(sc, idxID, convertedRanges)
	} else {
		rangeCount, err = ds.statisticBlock.GetRowCountByDeferredCausetRanges(sc, colInfoID, convertedRanges)
	}
	if err != nil {
		return 0, false, corr
	}
	scanCount := rangeCount + expectedCnt - count
	if len(remained) > 0 {
		scanCount = scanCount / SelectionFactor
	}
	scanCount = math.Min(scanCount, path.CountAfterAccess)
	return scanCount, true, 0
}

// GetPhysicalScan returns PhysicalBlockScan for the LogicalBlockScan.
func (s *LogicalBlockScan) GetPhysicalScan(schemaReplicant *expression.Schema, stats *property.StatsInfo) *PhysicalBlockScan {
	ds := s.Source
	ts := PhysicalBlockScan{
		Block:           ds.blockInfo,
		DeferredCausets: ds.DeferredCausets,
		BlockAsName:     ds.BlockAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalBlockID: ds.physicalBlockID,
		Ranges:          s.Ranges,
		AccessCondition: s.AccessConds,
	}.Init(s.ctx, s.blockOffset)
	ts.stats = stats
	ts.SetSchema(schemaReplicant.Clone())
	if ts.Block.PKIsHandle {
		if pkDefCausInfo := ts.Block.GetPkDefCausInfo(); pkDefCausInfo != nil {
			if ds.statisticBlock.DeferredCausets[pkDefCausInfo.ID] != nil {
				ts.Hist = &ds.statisticBlock.DeferredCausets[pkDefCausInfo.ID].Histogram
			}
		}
	}
	return ts
}

// GetPhysicalIndexScan returns PhysicalIndexScan for the logical IndexScan.
func (s *LogicalIndexScan) GetPhysicalIndexScan(schemaReplicant *expression.Schema, stats *property.StatsInfo) *PhysicalIndexScan {
	ds := s.Source
	is := PhysicalIndexScan{
		Block:            ds.blockInfo,
		BlockAsName:      ds.BlockAsName,
		DBName:           ds.DBName,
		DeferredCausets:  s.DeferredCausets,
		Index:            s.Index,
		IdxDefCauss:      s.IdxDefCauss,
		IdxDefCausLens:   s.IdxDefCausLens,
		AccessCondition:  s.AccessConds,
		Ranges:           s.Ranges,
		dataSourceSchema: ds.schemaReplicant,
		isPartition:      ds.isPartition,
		physicalBlockID:  ds.physicalBlockID,
	}.Init(ds.ctx, ds.blockOffset)
	is.stats = stats
	is.initSchema(s.FullIdxDefCauss, s.IsDoubleRead)
	return is
}

// convertToBlockScan converts the DataSource to block scan.
func (ds *DataSource) convertToBlockScan(prop *property.PhysicalProperty, candidate *candidatePath) (task task, err error) {
	// It will be handled in convertToIndexScan.
	if prop.TaskTp == property.CoFIDeloubleReadTaskType {
		return invalidTask, nil
	}
	if !prop.IsEmpty() && !candidate.isMatchProp {
		return invalidTask, nil
	}
	ts, cost, _ := ds.getOriginalPhysicalBlockScan(prop, candidate.path, candidate.isMatchProp)
	INTERLOCKTask := &INTERLOCKTask{
		blockPlan:         ts,
		indexPlanFinished: true,
		tblDefCausHists:   ds.TblDefCausHists,
		cst:               cost,
	}
	INTERLOCKTask.partitionInfo = PartitionInfo{
		PruningConds:        ds.allConds,
		PartitionNames:      ds.partitionNames,
		DeferredCausets:     ds.TblDefCauss,
		DeferredCausetNames: ds.names,
	}
	task = INTERLOCKTask
	if candidate.isMatchProp {
		INTERLOCKTask.keepOrder = true
		// BlockScan on partition block can't keep order.
		if ds.blockInfo.GetPartitionInfo() != nil {
			return invalidTask, nil
		}
	}
	ts.addPushedDownSelection(INTERLOCKTask, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt))
	if prop.IsFlashOnlyProp() && len(INTERLOCKTask.rootTaskConds) != 0 {
		return invalidTask, nil
	}
	if prop.TaskTp == property.RootTaskType {
		task = finishINTERLOCKTask(ds.ctx, task)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (ds *DataSource) convertToPointGet(prop *property.PhysicalProperty, candidate *candidatePath) task {
	if !prop.IsEmpty() && !candidate.isMatchProp {
		return invalidTask
	}
	if prop.TaskTp == property.CoFIDeloubleReadTaskType && candidate.isSingleScan ||
		prop.TaskTp == property.INTERLOCKSingleReadTaskType && !candidate.isSingleScan {
		return invalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(1))
	pointGetPlan := PointGetPlan{
		ctx:              ds.ctx,
		AccessConditions: candidate.path.AccessConds,
		schemaReplicant:  ds.schemaReplicant.Clone(),
		dbName:           ds.DBName.L,
		TblInfo:          ds.BlockInfo(),
		outputNames:      ds.OutputNames(),
		LockWaitTime:     ds.ctx.GetStochastikVars().LockWaitTimeout,
		DeferredCausets:  ds.DeferredCausets,
	}.Init(ds.ctx, ds.blockStats.ScaleByExpectCnt(accessCnt), ds.blockOffset)
	var partitionInfo *perceptron.PartitionDefinition
	if ds.isPartition {
		if pi := ds.blockInfo.GetPartitionInfo(); pi != nil {
			for _, def := range pi.Definitions {
				if def.ID == ds.physicalBlockID {
					partitionInfo = &def
					break
				}
			}
		}
		if partitionInfo == nil {
			return invalidTask
		}
	}
	rTsk := &rootTask{p: pointGetPlan}
	var cost float64
	if candidate.path.IsIntHandlePath {
		pointGetPlan.Handle = ekv.IntHandle(candidate.path.Ranges[0].LowVal[0].GetInt64())
		pointGetPlan.UnsignedHandle = allegrosql.HasUnsignedFlag(ds.handleDefCauss.GetDefCaus(0).RetType.Flag)
		pointGetPlan.PartitionInfo = partitionInfo
		cost = pointGetPlan.GetCost(ds.TblDefCauss)
		// Add filter condition to block plan now.
		if len(candidate.path.BlockFilters) > 0 {
			sessVars := ds.ctx.GetStochastikVars()
			cost += pointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: candidate.path.BlockFilters,
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(pointGetPlan)
			rTsk.p = sel
		}
	} else {
		pointGetPlan.IndexInfo = candidate.path.Index
		pointGetPlan.IdxDefCauss = candidate.path.IdxDefCauss
		pointGetPlan.IdxDefCausLens = candidate.path.IdxDefCausLens
		pointGetPlan.IndexValues = candidate.path.Ranges[0].LowVal
		pointGetPlan.PartitionInfo = partitionInfo
		if candidate.isSingleScan {
			cost = pointGetPlan.GetCost(candidate.path.IdxDefCauss)
		} else {
			cost = pointGetPlan.GetCost(ds.TblDefCauss)
		}
		// Add index condition to block plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.BlockFilters) > 0 {
			sessVars := ds.ctx.GetStochastikVars()
			cost += pointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.BlockFilters...),
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(pointGetPlan)
			rTsk.p = sel
		}
	}

	rTsk.cst = cost
	return rTsk
}

func (ds *DataSource) convertToBatchPointGet(prop *property.PhysicalProperty, candidate *candidatePath) task {
	if !prop.IsEmpty() && !candidate.isMatchProp {
		return invalidTask
	}
	if prop.TaskTp == property.CoFIDeloubleReadTaskType && candidate.isSingleScan ||
		prop.TaskTp == property.INTERLOCKSingleReadTaskType && !candidate.isSingleScan {
		return invalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(len(candidate.path.Ranges)))
	batchPointGetPlan := BatchPointGetPlan{
		ctx:              ds.ctx,
		AccessConditions: candidate.path.AccessConds,
		TblInfo:          ds.BlockInfo(),
		KeepOrder:        !prop.IsEmpty(),
		DeferredCausets:  ds.DeferredCausets,
	}.Init(ds.ctx, ds.blockStats.ScaleByExpectCnt(accessCnt), ds.schemaReplicant.Clone(), ds.names, ds.blockOffset)
	if batchPointGetPlan.KeepOrder {
		batchPointGetPlan.Desc = prop.Items[0].Desc
	}
	rTsk := &rootTask{p: batchPointGetPlan}
	var cost float64
	if candidate.path.IsIntHandlePath {
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.Handles = append(batchPointGetPlan.Handles, ekv.IntHandle(ran.LowVal[0].GetInt64()))
		}
		cost = batchPointGetPlan.GetCost(ds.TblDefCauss)
		// Add filter condition to block plan now.
		if len(candidate.path.BlockFilters) > 0 {
			sessVars := ds.ctx.GetStochastikVars()
			cost += batchPointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: candidate.path.BlockFilters,
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(batchPointGetPlan)
			rTsk.p = sel
		}
	} else {
		batchPointGetPlan.IndexInfo = candidate.path.Index
		batchPointGetPlan.IdxDefCauss = candidate.path.IdxDefCauss
		batchPointGetPlan.IdxDefCausLens = candidate.path.IdxDefCausLens
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.IndexValues = append(batchPointGetPlan.IndexValues, ran.LowVal)
		}
		if !prop.IsEmpty() {
			batchPointGetPlan.KeepOrder = true
			batchPointGetPlan.Desc = prop.Items[0].Desc
		}
		if candidate.isSingleScan {
			cost = batchPointGetPlan.GetCost(candidate.path.IdxDefCauss)
		} else {
			cost = batchPointGetPlan.GetCost(ds.TblDefCauss)
		}
		// Add index condition to block plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.BlockFilters) > 0 {
			sessVars := ds.ctx.GetStochastikVars()
			cost += batchPointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.BlockFilters...),
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(batchPointGetPlan)
			rTsk.p = sel
		}
	}

	rTsk.cst = cost
	return rTsk
}

func (ts *PhysicalBlockScan) addPushedDownSelection(INTERLOCKTask *INTERLOCKTask, stats *property.StatsInfo) {
	ts.filterCondition, INTERLOCKTask.rootTaskConds = SplitSelCondsWithVirtualDeferredCauset(ts.filterCondition)
	var newRootConds []expression.Expression
	ts.filterCondition, newRootConds = expression.PushDownExprs(ts.ctx.GetStochastikVars().StmtCtx, ts.filterCondition, ts.ctx.GetClient(), ts.StoreType)
	INTERLOCKTask.rootTaskConds = append(INTERLOCKTask.rootTaskConds, newRootConds...)

	// Add filter condition to block plan now.
	sessVars := ts.ctx.GetStochastikVars()
	if len(ts.filterCondition) > 0 {
		INTERLOCKTask.cst += INTERLOCKTask.count() * sessVars.INTERLOCKCPUFactor
		sel := PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.ctx, stats, ts.blockOffset)
		sel.SetChildren(ts)
		INTERLOCKTask.blockPlan = sel
	}
}

func (ds *DataSource) getOriginalPhysicalBlockScan(prop *property.PhysicalProperty, path *soliton.AccessPath, isMatchProp bool) (*PhysicalBlockScan, float64, float64) {
	ts := PhysicalBlockScan{
		Block:           ds.blockInfo,
		DeferredCausets: ds.DeferredCausets,
		BlockAsName:     ds.BlockAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalBlockID: ds.physicalBlockID,
		Ranges:          path.Ranges,
		AccessCondition: path.AccessConds,
		filterCondition: path.BlockFilters,
		StoreType:       path.StoreType,
		IsGlobalRead:    path.IsTiFlashGlobalRead,
	}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.schemaReplicant.Clone())
	if ts.Block.PKIsHandle {
		if pkDefCausInfo := ts.Block.GetPkDefCausInfo(); pkDefCausInfo != nil {
			if ds.statisticBlock.DeferredCausets[pkDefCausInfo.ID] != nil {
				ts.Hist = &ds.statisticBlock.DeferredCausets[pkDefCausInfo.ID].Histogram
			}
		}
	}
	rowCount := path.CountAfterAccess
	if prop.ExpectedCnt < ds.stats.RowCount {
		count, ok, corr := ds.crossEstimateRowCount(path, prop.ExpectedCnt, isMatchProp && prop.Items[0].Desc)
		if ok {
			// TODO: actually, before using this count as the estimated event count of block scan, we need additionally
			// check if count < row_count(first_region | last_region), and use the larger one since we build one INTERLOCKTask
			// for one region now, so even if it is `limit 1`, we have to scan at least one region in block scan.
			// Currently, we can use `einsteindbrpc.CmdDebugGetRegionProperties` interface as `getSampRegionsRowCount()` does
			// to get the event count in a region, but that result contains MVCC old version rows, so it is not that accurate.
			// Considering that when this scenario happens, the execution time is close between IndexScan and BlockScan,
			// we do not add this check temporarily.
			rowCount = count
		} else if corr < 1 {
			correlationFactor := math.Pow(1-corr, float64(ds.ctx.GetStochastikVars().CorrelationExpFactor))
			selectivity := ds.stats.RowCount / rowCount
			rowCount = math.Min(prop.ExpectedCnt/selectivity/correlationFactor, rowCount)
		}
	}
	// We need NDV of columns since it may be used in cost estimation of join. Precisely speaking,
	// we should track NDV of each histogram bucket, and sum up the NDV of buckets we actually need
	// to scan, but this would only help improve accuracy of NDV for one column, for other columns,
	// we still need to assume values are uniformly distributed. For simplicity, we use uniform-assumption
	// for all columns now, as we do in `deriveStatsByFilter`.
	ts.stats = ds.blockStats.ScaleByExpectCnt(rowCount)
	var rowSize float64
	if ts.StoreType == ekv.EinsteinDB {
		rowSize = ds.TblDefCausHists.GetBlockAvgRowSize(ds.ctx, ds.TblDefCauss, ts.StoreType, true)
	} else {
		// If `ds.handleDefCaus` is nil, then the schemaReplicant of blockScan doesn't have handle column.
		// This logic can be ensured in column pruning.
		rowSize = ds.TblDefCausHists.GetBlockAvgRowSize(ds.ctx, ts.Schema().DeferredCausets, ts.StoreType, ds.handleDefCauss != nil)
	}
	sessVars := ds.ctx.GetStochastikVars()
	cost := rowCount * rowSize * sessVars.ScanFactor
	if ts.IsGlobalRead {
		cost += rowCount * sessVars.NetworkFactor * rowSize
	}
	if isMatchProp {
		if prop.Items[0].Desc {
			ts.Desc = true
			cost = rowCount * rowSize * sessVars.DescScanFactor
		}
		ts.KeepOrder = true
	}
	switch ts.StoreType {
	case ekv.EinsteinDB:
		cost += float64(len(ts.Ranges)) * sessVars.SeekFactor
	case ekv.TiFlash:
		cost += float64(len(ts.Ranges)) * float64(len(ts.DeferredCausets)) * sessVars.SeekFactor
	}
	return ts, cost, rowCount
}

func (ds *DataSource) getOriginalPhysicalIndexScan(prop *property.PhysicalProperty, path *soliton.AccessPath, isMatchProp bool, isSingleScan bool) (*PhysicalIndexScan, float64, float64) {
	idx := path.Index
	is := PhysicalIndexScan{
		Block:            ds.blockInfo,
		BlockAsName:      ds.BlockAsName,
		DBName:           ds.DBName,
		DeferredCausets:  ds.DeferredCausets,
		Index:            idx,
		IdxDefCauss:      path.IdxDefCauss,
		IdxDefCausLens:   path.IdxDefCausLens,
		AccessCondition:  path.AccessConds,
		Ranges:           path.Ranges,
		dataSourceSchema: ds.schemaReplicant,
		isPartition:      ds.isPartition,
		physicalBlockID:  ds.physicalBlockID,
	}.Init(ds.ctx, ds.blockOffset)
	statsTbl := ds.statisticBlock
	if statsTbl.Indices[idx.ID] != nil {
		is.Hist = &statsTbl.Indices[idx.ID].Histogram
	}
	rowCount := path.CountAfterAccess
	is.initSchema(append(path.FullIdxDefCauss, ds.commonHandleDefCauss...), !isSingleScan)
	// Only use expectedCnt when it's smaller than the count we calculated.
	// e.g. IndexScan(count1)->After Filter(count2). The `ds.stats.RowCount` is count2. count1 is the one we need to calculate
	// If expectedCnt and count2 are both zero and we go into the below `if` block, the count1 will be set to zero though it's shouldn't be.
	if (isMatchProp || prop.IsEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		selectivity := ds.stats.RowCount / path.CountAfterAccess
		rowCount = math.Min(prop.ExpectedCnt/selectivity, rowCount)
	}
	is.stats = ds.blockStats.ScaleByExpectCnt(rowCount)
	rowSize := is.indexScanRowSize(idx, ds, true)
	sessVars := ds.ctx.GetStochastikVars()
	cost := rowCount * rowSize * sessVars.ScanFactor
	if isMatchProp {
		if prop.Items[0].Desc {
			is.Desc = true
			cost = rowCount * rowSize * sessVars.DescScanFactor
		}
		is.KeepOrder = true
	}
	cost += float64(len(is.Ranges)) * sessVars.SeekFactor
	return is, cost, rowCount
}
