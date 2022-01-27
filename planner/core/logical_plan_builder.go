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
	"context"
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/format"
	"github.com/whtcorpsinc/berolinaAllegroSQL/opcode"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/planner/property"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	util2 "github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	utilhint "github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	driver "github.com/whtcorpsinc/milevadb/types/berolinaAllegroSQL_driver"
)

const (
	// MilevaDBMergeJoin is hint enforce merge join.
	MilevaDBMergeJoin = "milevadb_smj"
	// HintSMJ is hint enforce merge join.
	HintSMJ = "merge_join"

	// MilevaDBBroadCastJoin indicates applying broadcast join by force.
	MilevaDBBroadCastJoin = "milevadb_bcj"

	// HintBCJ indicates applying broadcast join by force.
	HintBCJ = "broadcast_join"
	// HintBCJPreferLocal specifies the preferred local read block
	HintBCJPreferLocal = "broadcast_join_local"

	// MilevaDBIndexNestedLoopJoin is hint enforce index nested loop join.
	MilevaDBIndexNestedLoopJoin = "milevadb_inlj"
	// HintINLJ is hint enforce index nested loop join.
	HintINLJ = "inl_join"
	// HintINLHJ is hint enforce index nested loop hash join.
	HintINLHJ = "inl_hash_join"
	// HintINLMJ is hint enforce index nested loop merge join.
	HintINLMJ = "inl_merge_join"
	// MilevaDBHashJoin is hint enforce hash join.
	MilevaDBHashJoin = "milevadb_hj"
	// HintHJ is hint enforce hash join.
	HintHJ = "hash_join"
	// HintHashAgg is hint enforce hash aggregation.
	HintHashAgg = "hash_agg"
	// HintStreamAgg is hint enforce stream aggregation.
	HintStreamAgg = "stream_agg"
	// HintUseIndex is hint enforce using some indexes.
	HintUseIndex = "use_index"
	// HintIgnoreIndex is hint enforce ignoring some indexes.
	HintIgnoreIndex = "ignore_index"
	// HintAggToINTERLOCK is hint enforce pushing aggregation to interlock.
	HintAggToINTERLOCK = "agg_to_INTERLOCK"
	// HintReadFromStorage is hint enforce some blocks read from specific type of storage.
	HintReadFromStorage = "read_from_storage"
	// HintTiFlash is a label represents the tiflash storage type.
	HintTiFlash = "tiflash"
	// HintEinsteinDB is a label represents the einsteindb storage type.
	HintEinsteinDB = "einsteindb"
	// HintIndexMerge is a hint to enforce using some indexes at the same time.
	HintIndexMerge = "use_index_merge"
	// HintTimeRange is a hint to specify the time range for metrics summary blocks
	HintTimeRange = "time_range"
	// HintIgnorePlanCache is a hint to enforce ignoring plan cache
	HintIgnorePlanCache = "ignore_plan_cache"
	// HintLimitToINTERLOCK is a hint enforce pushing limit or topn to interlock.
	HintLimitToINTERLOCK = "limit_to_INTERLOCK"
)

const (
	// ErrExprInSelect  is in select fields for the error of ErrFieldNotInGroupBy
	ErrExprInSelect = "SELECT list"
	// ErrExprInOrderBy  is in order by items for the error of ErrFieldNotInGroupBy
	ErrExprInOrderBy = "ORDER BY"
)

func (la *LogicalAggregation) collectGroupByDeferredCausets() {
	la.groupByDefCauss = la.groupByDefCauss[:0]
	for _, item := range la.GroupByItems {
		if col, ok := item.(*expression.DeferredCauset); ok {
			la.groupByDefCauss = append(la.groupByDefCauss, col)
		}
	}
}

// aggOrderByResolver is currently resolving expressions of order by clause
// in aggregate function GROUP_CONCAT.
type aggOrderByResolver struct {
	ctx       stochastikctx.Context
	err       error
	args      []ast.ExprNode
	exprDepth int // exprDepth is the depth of current expression in expression tree.
}

func (a *aggOrderByResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	a.exprDepth++
	switch n := inNode.(type) {
	case *driver.ParamMarkerExpr:
		if a.exprDepth == 1 {
			_, isNull, isExpectedType := getUintFromNode(a.ctx, n)
			// For constant uint expression in top level, it should be treated as position expression.
			if !isNull && isExpectedType {
				return expression.ConstructPositionExpr(n), true
			}
		}
	}
	return inNode, false
}

func (a *aggOrderByResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.PositionExpr:
		pos, isNull, err := expression.PosFromPositionExpr(a.ctx, v)
		if err != nil {
			a.err = err
		}
		if err != nil || isNull {
			return inNode, false
		}
		if pos < 1 || pos > len(a.args) {
			errPos := strconv.Itoa(pos)
			if v.P != nil {
				errPos = "?"
			}
			a.err = ErrUnknownDeferredCauset.FastGenByArgs(errPos, "order clause")
			return inNode, false
		}
		ret := a.args[pos-1]
		return ret, true
	}
	return inNode, true
}

func (b *PlanBuilder) builPosetDaggregation(ctx context.Context, p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression) (LogicalPlan, map[int]int, error) {
	b.optFlag |= flagBuildKeyInfo
	b.optFlag |= flagPushDownAgg
	// We may apply aggregation eliminate optimization.
	// So we add the flagMaxMinEliminate to try to convert max/min to topn and flagPushDownTopN to handle the newly added topn operator.
	b.optFlag |= flagMaxMinEliminate
	b.optFlag |= flagPushDownTopN
	// when we eliminate the max and min we may add `is not null` filter.
	b.optFlag |= flagPredicatePushDown
	b.optFlag |= flagEliminateAgg
	b.optFlag |= flagEliminateProjection

	plan4Agg := LogicalAggregation{AggFuncs: make([]*aggregation.AggFuncDesc, 0, len(aggFuncList))}.Init(b.ctx, b.getSelectOffset())
	if hint := b.BlockHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	schema4Agg := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(aggFuncList)+p.Schema().Len())...)
	names := make(types.NameSlice, 0, len(aggFuncList)+p.Schema().Len())
	// aggIdxMap maps the old index to new index after applying common aggregation functions elimination.
	aggIndexMap := make(map[int]int)

	for i, aggFunc := range aggFuncList {
		newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
		for _, arg := range aggFunc.Args {
			newArg, np, err := b.rewrite(ctx, arg, p, nil, true)
			if err != nil {
				return nil, nil, err
			}
			p = np
			newArgList = append(newArgList, newArg)
		}
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, aggFunc.F, newArgList, aggFunc.Distinct)
		if err != nil {
			return nil, nil, err
		}
		if aggFunc.Order != nil {
			trueArgs := aggFunc.Args[:len(aggFunc.Args)-1] // the last argument is SEPARATOR, remote it.
			resolver := &aggOrderByResolver{
				ctx:  b.ctx,
				args: trueArgs,
			}
			for _, byItem := range aggFunc.Order.Items {
				resolver.exprDepth = 0
				resolver.err = nil
				retExpr, _ := byItem.Expr.Accept(resolver)
				if resolver.err != nil {
					return nil, nil, errors.Trace(resolver.err)
				}
				newByItem, np, err := b.rewrite(ctx, retExpr.(ast.ExprNode), p, nil, true)
				if err != nil {
					return nil, nil, err
				}
				p = np
				newFunc.OrderByItems = append(newFunc.OrderByItems, &soliton.ByItems{Expr: newByItem, Desc: byItem.Desc})
			}
		}
		combined := false
		for j, oldFunc := range plan4Agg.AggFuncs {
			if oldFunc.Equal(b.ctx, newFunc) {
				aggIndexMap[i] = j
				combined = true
				break
			}
		}
		if !combined {
			position := len(plan4Agg.AggFuncs)
			aggIndexMap[i] = position
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			schema4Agg.Append(&expression.DeferredCauset{
				UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
				RetType:  newFunc.RetTp,
			})
			names = append(names, types.EmptyName)
		}
	}
	for i, col := range p.Schema().DeferredCausets {
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		newDefCaus, _ := col.Clone().(*expression.DeferredCauset)
		newDefCaus.RetType = newFunc.RetTp
		schema4Agg.Append(newDefCaus)
		names = append(names, p.OutputNames()[i])
	}
	plan4Agg.names = names
	plan4Agg.SetChildren(p)
	plan4Agg.GroupByItems = gbyItems
	plan4Agg.SetSchema(schema4Agg)
	plan4Agg.collectGroupByDeferredCausets()
	return plan4Agg, aggIndexMap, nil
}

func (b *PlanBuilder) buildResultSetNode(ctx context.Context, node ast.ResultSetNode) (p LogicalPlan, err error) {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildJoin(ctx, x)
	case *ast.BlockSource:
		var isBlockName bool
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			p, err = b.buildSelect(ctx, v)
		case *ast.SetOprStmt:
			p, err = b.buildSetOpr(ctx, v)
		case *ast.BlockName:
			p, err = b.buildDataSource(ctx, v, &x.AsName)
			isBlockName = true
		default:
			err = ErrUnsupportedType.GenWithStackByArgs(v)
		}
		if err != nil {
			return nil, err
		}

		for _, name := range p.OutputNames() {
			if name.Hidden {
				continue
			}
			if x.AsName.L != "" {
				name.TblName = x.AsName
			}
		}
		// `BlockName` is not a select block, so we do not need to handle it.
		if !isBlockName && b.ctx.GetStochastikVars().PlannerSelectBlockAsName != nil {
			b.ctx.GetStochastikVars().PlannerSelectBlockAsName[p.SelectBlockOffset()] = ast.HintBlock{DBName: p.OutputNames()[0].DBName, BlockName: p.OutputNames()[0].TblName}
		}
		// Duplicate column name in one block is not allowed.
		// "select * from (select 1, 1) as a;" is duplicate
		dupNames := make(map[string]struct{}, len(p.Schema().DeferredCausets))
		for _, name := range p.OutputNames() {
			colName := name.DefCausName.O
			if _, ok := dupNames[colName]; ok {
				return nil, ErrDupFieldName.GenWithStackByArgs(colName)
			}
			dupNames[colName] = struct{}{}
		}
		return p, nil
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	case *ast.SetOprStmt:
		return b.buildSetOpr(ctx, x)
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.ResultSetNode(%T) for buildResultSetNode()", x)
	}
}

// pushDownConstExpr checks if the condition is from filter condition, if true, push it down to both
// children of join, whatever the join type is; if false, push it down to inner child of outer join,
// and both children of non-outer-join.
func (p *LogicalJoin) pushDownConstExpr(expr expression.Expression, leftCond []expression.Expression,
	rightCond []expression.Expression, filterCond bool) ([]expression.Expression, []expression.Expression) {
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			rightCond = append(rightCond, expr)
		}
	case RightOuterJoin:
		if filterCond {
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			leftCond = append(leftCond, expr)
		}
	case SemiJoin, InnerJoin:
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	case AntiSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
		}
		rightCond = append(rightCond, expr)
	}
	return leftCond, rightCond
}

func (p *LogicalJoin) extractOnCondition(conditions []expression.Expression, deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	return p.ExtractOnCondition(conditions, p.children[0].Schema(), p.children[1].Schema(), deriveLeft, deriveRight)
}

// ExtractOnCondition divide conditions in CNF of join node into 4 groups.
// These conditions can be where conditions, join conditions, or collection of both.
// If deriveLeft/deriveRight is set, we would try to derive more conditions for left/right plan.
func (p *LogicalJoin) ExtractOnCondition(
	conditions []expression.Expression,
	leftSchema *expression.Schema,
	rightSchema *expression.Schema,
	deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	for _, expr := range conditions {
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && len(binop.GetArgs()) == 2 {
			ctx := binop.GetCtx()
			arg0, lOK := binop.GetArgs()[0].(*expression.DeferredCauset)
			arg1, rOK := binop.GetArgs()[1].(*expression.DeferredCauset)
			if lOK && rOK {
				leftDefCaus := leftSchema.RetrieveDeferredCauset(arg0)
				rightDefCaus := rightSchema.RetrieveDeferredCauset(arg1)
				if leftDefCaus == nil || rightDefCaus == nil {
					leftDefCaus = leftSchema.RetrieveDeferredCauset(arg1)
					rightDefCaus = rightSchema.RetrieveDeferredCauset(arg0)
					arg0, arg1 = arg1, arg0
				}
				if leftDefCaus != nil && rightDefCaus != nil {
					if deriveLeft {
						if isNullRejected(ctx, leftSchema, expr) && !allegrosql.HasNotNullFlag(leftDefCaus.RetType.Flag) {
							notNullExpr := expression.BuildNotNullExpr(ctx, leftDefCaus)
							leftCond = append(leftCond, notNullExpr)
						}
					}
					if deriveRight {
						if isNullRejected(ctx, rightSchema, expr) && !allegrosql.HasNotNullFlag(rightDefCaus.RetType.Flag) {
							notNullExpr := expression.BuildNotNullExpr(ctx, rightDefCaus)
							rightCond = append(rightCond, notNullExpr)
						}
					}
					// For queries like `select a in (select a from s where s.b = t.b) from t`,
					// if subquery is empty caused by `s.b = t.b`, the result should always be
					// false even if t.a is null or s.a is null. To make this join "empty aware",
					// we should differentiate `t.a = s.a` from other column equal conditions, so
					// we put it into OtherConditions instead of EqualConditions of join.
					if binop.FuncName.L == ast.EQ && !arg0.InOperand && !arg1.InOperand {
						cond := expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), arg0, arg1)
						eqCond = append(eqCond, cond.(*expression.ScalarFunction))
						continue
					}
				}
			}
		}
		columns := expression.ExtractDeferredCausets(expr)
		// `columns` may be empty, if the condition is like `correlated_column op constant`, or `constant`,
		// push this HoTT of constant condition down according to join type.
		if len(columns) == 0 {
			leftCond, rightCond = p.pushDownConstExpr(expr, leftCond, rightCond, deriveLeft || deriveRight)
			continue
		}
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if !leftSchema.Contains(col) {
				allFromLeft = false
			}
			if !rightSchema.Contains(col) {
				allFromRight = false
			}
		}
		if allFromRight {
			rightCond = append(rightCond, expr)
		} else if allFromLeft {
			leftCond = append(leftCond, expr)
		} else {
			// Relax expr to two supersets: leftRelaxedCond and rightRelaxedCond, the expression now is
			// `expr AND leftRelaxedCond AND rightRelaxedCond`. Motivation is to push filters down to
			// children as much as possible.
			if deriveLeft {
				leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, leftSchema)
				if leftRelaxedCond != nil {
					leftCond = append(leftCond, leftRelaxedCond)
				}
			}
			if deriveRight {
				rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, rightSchema)
				if rightRelaxedCond != nil {
					rightCond = append(rightCond, rightRelaxedCond)
				}
			}
			otherCond = append(otherCond, expr)
		}
	}
	return
}

// extractBlockAlias returns block alias of the LogicalPlan's columns.
// It will return nil when there are multiple block alias, because the alias is only used to check if
// the logicalPlan match some optimizer hints, and hints are not expected to take effect in this case.
func extractBlockAlias(p Plan, parentOffset int) *hintBlockInfo {
	if len(p.OutputNames()) > 0 && p.OutputNames()[0].TblName.L != "" {
		firstName := p.OutputNames()[0]
		for _, name := range p.OutputNames() {
			if name.TblName.L != firstName.TblName.L || name.DBName.L != firstName.DBName.L {
				return nil
			}
		}
		blockOffset := p.SelectBlockOffset()
		blockAsNames := p.SCtx().GetStochastikVars().PlannerSelectBlockAsName
		// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
		if blockOffset != parentOffset && blockAsNames != nil && blockAsNames[blockOffset].BlockName.L != "" {
			blockOffset = parentOffset
		}
		return &hintBlockInfo{dbName: firstName.DBName, tblName: firstName.TblName, selectOffset: blockOffset}
	}
	return nil
}

func (p *LogicalJoin) getPreferredBCJLocalIndex() (hasPrefer bool, prefer int) {
	if p.hintInfo == nil {
		return
	}
	if p.hintInfo.ifPreferAsLocalInBCJoin(p.children[0], p.blockOffset) {
		return true, 0
	}
	if p.hintInfo.ifPreferAsLocalInBCJoin(p.children[1], p.blockOffset) {
		return true, 1
	}
	return false, 0
}

func (p *LogicalJoin) setPreferredJoinType(hintInfo *blockHintInfo) {
	if hintInfo == nil {
		return
	}

	lhsAlias := extractBlockAlias(p.children[0], p.blockOffset)
	rhsAlias := extractBlockAlias(p.children[1], p.blockOffset)
	if hintInfo.ifPreferMergeJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferMergeJoin
	}
	if hintInfo.ifPreferBroadcastJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferBCJoin
	}
	if hintInfo.ifPreferHashJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferHashJoin
	}
	if hintInfo.ifPreferINLJ(lhsAlias) {
		p.preferJoinType |= preferLeftAsINLJInner
	}
	if hintInfo.ifPreferINLJ(rhsAlias) {
		p.preferJoinType |= preferRightAsINLJInner
	}
	if hintInfo.ifPreferINLHJ(lhsAlias) {
		p.preferJoinType |= preferLeftAsINLHJInner
	}
	if hintInfo.ifPreferINLHJ(rhsAlias) {
		p.preferJoinType |= preferRightAsINLHJInner
	}
	if hintInfo.ifPreferINLMJ(lhsAlias) {
		p.preferJoinType |= preferLeftAsINLMJInner
	}
	if hintInfo.ifPreferINLMJ(rhsAlias) {
		p.preferJoinType |= preferRightAsINLMJInner
	}
	if containDifferentJoinTypes(p.preferJoinType) {
		errMsg := "Join hints are conflict, you can only specify one type of join"
		warning := ErrInternal.GenWithStack(errMsg)
		p.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
		p.preferJoinType = 0
	}
	// set hintInfo for further usage if this hint info can be used.
	if p.preferJoinType != 0 {
		p.hintInfo = hintInfo
	}
}

func (ds *DataSource) setPreferredStoreType(hintInfo *blockHintInfo) {
	if hintInfo == nil {
		return
	}

	var alias *hintBlockInfo
	if len(ds.BlockAsName.L) != 0 {
		alias = &hintBlockInfo{dbName: ds.DBName, tblName: *ds.BlockAsName, selectOffset: ds.SelectBlockOffset()}
	} else {
		alias = &hintBlockInfo{dbName: ds.DBName, tblName: ds.blockInfo.Name, selectOffset: ds.SelectBlockOffset()}
	}
	if hintTbl := hintInfo.ifPreferEinsteinDB(alias); hintTbl != nil {
		for _, path := range ds.possibleAccessPaths {
			if path.StoreType == ekv.EinsteinDB {
				ds.preferStoreType |= preferEinsteinDB
				ds.preferPartitions[preferEinsteinDB] = hintTbl.partitions
				break
			}
		}
		if ds.preferStoreType&preferEinsteinDB == 0 {
			errMsg := fmt.Sprintf("No available path for block %s.%s with the causetstore type %s of the hint /*+ read_from_storage */, "+
				"please check the status of the block replica and variable value of milevadb_isolation_read_engines(%v)",
				ds.DBName.O, ds.block.Meta().Name.O, ekv.EinsteinDB.Name(), ds.ctx.GetStochastikVars().GetIsolationReadEngines())
			warning := ErrInternal.GenWithStack(errMsg)
			ds.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
		}
	}
	if hintTbl := hintInfo.ifPreferTiFlash(alias); hintTbl != nil {
		// 1. `ds.blockInfo.Partition == nil`, which means the hint takes effect in the whole block.
		// 2. `ds.preferStoreType != 0`, which means there's a hint hit the both EinsteinDB value and TiFlash value for block.
		// If it's satisfied the above two conditions, then we can make sure there are some hints conflicted.
		if ds.preferStoreType != 0 && ds.blockInfo.Partition == nil {
			errMsg := fmt.Sprintf("CausetStorage hints are conflict, you can only specify one storage type of block %s.%s",
				alias.dbName.L, alias.tblName.L)
			warning := ErrInternal.GenWithStack(errMsg)
			ds.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
			ds.preferStoreType = 0
			return
		}
		for _, path := range ds.possibleAccessPaths {
			if path.StoreType == ekv.TiFlash {
				ds.preferStoreType |= preferTiFlash
				ds.preferPartitions[preferTiFlash] = hintTbl.partitions
				break
			}
		}
		if ds.preferStoreType&preferTiFlash == 0 {
			errMsg := fmt.Sprintf("No available path for block %s.%s with the causetstore type %s of the hint /*+ read_from_storage */, "+
				"please check the status of the block replica and variable value of milevadb_isolation_read_engines(%v)",
				ds.DBName.O, ds.block.Meta().Name.O, ekv.TiFlash.Name(), ds.ctx.GetStochastikVars().GetIsolationReadEngines())
			warning := ErrInternal.GenWithStack(errMsg)
			ds.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
		}
	}
}

func resetNotNullFlag(schemaReplicant *expression.Schema, start, end int) {
	for i := start; i < end; i++ {
		col := *schemaReplicant.DeferredCausets[i]
		newFieldType := *col.RetType
		newFieldType.Flag &= ^allegrosql.NotNullFlag
		col.RetType = &newFieldType
		schemaReplicant.DeferredCausets[i] = &col
	}
}

func (b *PlanBuilder) buildJoin(ctx context.Context, joinNode *ast.Join) (LogicalPlan, error) {
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UFIDelATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	if joinNode.Right == nil {
		return b.buildResultSetNode(ctx, joinNode.Left)
	}

	b.optFlag = b.optFlag | flagPredicatePushDown

	leftPlan, err := b.buildResultSetNode(ctx, joinNode.Left)
	if err != nil {
		return nil, err
	}

	rightPlan, err := b.buildResultSetNode(ctx, joinNode.Right)
	if err != nil {
		return nil, err
	}

	handleMap1 := b.handleHelper.popMap()
	handleMap2 := b.handleHelper.popMap()
	b.handleHelper.mergeAndPush(handleMap1, handleMap2)

	joinPlan := LogicalJoin{StraightJoin: joinNode.StraightJoin || b.inStraightJoin}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))
	joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len()+rightPlan.Schema().Len())
	INTERLOCKy(joinPlan.names, leftPlan.OutputNames())
	INTERLOCKy(joinPlan.names[leftPlan.Schema().Len():], rightPlan.OutputNames())

	// Set join type.
	switch joinNode.Tp {
	case ast.LeftJoin:
		// left outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = LeftOuterJoin
		resetNotNullFlag(joinPlan.schemaReplicant, leftPlan.Schema().Len(), joinPlan.schemaReplicant.Len())
	case ast.RightJoin:
		// right outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = RightOuterJoin
		resetNotNullFlag(joinPlan.schemaReplicant, 0, leftPlan.Schema().Len())
	default:
		b.optFlag = b.optFlag | flagJoinReOrder
		joinPlan.JoinType = InnerJoin
	}

	// Merge sub join's redundantSchema into this join plan. When handle query like
	// select t2.a from (t1 join t2 using (a)) join t3 using (a);
	// we can simply search in the top level join plan to find redundant column.
	var (
		lRedundantSchema, rRedundantSchema *expression.Schema
		lRedundantNames, rRedundantNames   types.NameSlice
	)
	if left, ok := leftPlan.(*LogicalJoin); ok && left.redundantSchema != nil {
		lRedundantSchema = left.redundantSchema
		lRedundantNames = left.redundantNames
	}
	if right, ok := rightPlan.(*LogicalJoin); ok && right.redundantSchema != nil {
		rRedundantSchema = right.redundantSchema
		rRedundantNames = right.redundantNames
	}
	joinPlan.redundantSchema = expression.MergeSchema(lRedundantSchema, rRedundantSchema)
	joinPlan.redundantNames = make([]*types.FieldName, len(lRedundantNames)+len(rRedundantNames))
	INTERLOCKy(joinPlan.redundantNames, lRedundantNames)
	INTERLOCKy(joinPlan.redundantNames[len(lRedundantNames):], rRedundantNames)

	// Set preferred join algorithm if some join hints is specified by user.
	joinPlan.setPreferredJoinType(b.BlockHints())

	// "NATURAL JOIN" doesn't have "ON" or "USING" conditions.
	//
	// The "NATURAL [LEFT] JOIN" of two blocks is defined to be semantically
	// equivalent to an "INNER JOIN" or a "LEFT JOIN" with a "USING" clause
	// that names all columns that exist in both blocks.
	//
	// See https://dev.allegrosql.com/doc/refman/5.7/en/join.html for more detail.
	if joinNode.NaturalJoin {
		err = b.buildNaturalJoin(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.Using != nil {
		err = b.buildUsingClause(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.On != nil {
		b.curClause = onClause
		onExpr, newPlan, err := b.rewrite(ctx, joinNode.On.Expr, joinPlan, nil, false)
		if err != nil {
			return nil, err
		}
		if newPlan != joinPlan {
			return nil, errors.New("ON condition doesn't support subqueries yet")
		}
		onCondition := expression.SplitCNFItems(onExpr)
		joinPlan.AttachOnConds(onCondition)
	} else if joinPlan.JoinType == InnerJoin {
		// If a inner join without "ON" or "USING" clause, it's a cartesian
		// product over the join blocks.
		joinPlan.cartesianJoin = true
	}

	return joinPlan, nil
}

// buildUsingClause eliminate the redundant columns and ordering columns based
// on the "USING" clause.
//
// According to the standard ALLEGROALLEGROSQL, columns are ordered in the following way:
// 1. coalesced common columns of "leftPlan" and "rightPlan", in the order they
//    appears in "leftPlan".
// 2. the rest columns in "leftPlan", in the order they appears in "leftPlan".
// 3. the rest columns in "rightPlan", in the order they appears in "rightPlan".
func (b *PlanBuilder) buildUsingClause(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, join *ast.Join) error {
	filter := make(map[string]bool, len(join.Using))
	for _, col := range join.Using {
		filter[col.Name.L] = true
	}
	return b.coalesceCommonDeferredCausets(p, leftPlan, rightPlan, join.Tp, filter)
}

// buildNaturalJoin builds natural join output schemaReplicant. It finds out all the common columns
// then using the same mechanism as buildUsingClause to eliminate redundant columns and build join conditions.
// According to standard ALLEGROALLEGROSQL, producing this display order:
// 	All the common columns
// 	Every column in the first (left) block that is not a common column
// 	Every column in the second (right) block that is not a common column
func (b *PlanBuilder) buildNaturalJoin(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, join *ast.Join) error {
	return b.coalesceCommonDeferredCausets(p, leftPlan, rightPlan, join.Tp, nil)
}

// coalesceCommonDeferredCausets is used by buildUsingClause and buildNaturalJoin. The filter is used by buildUsingClause.
func (b *PlanBuilder) coalesceCommonDeferredCausets(p *LogicalJoin, leftPlan, rightPlan LogicalPlan, joinTp ast.JoinType, filter map[string]bool) error {
	lsc := leftPlan.Schema().Clone()
	rsc := rightPlan.Schema().Clone()
	if joinTp == ast.LeftJoin {
		resetNotNullFlag(rsc, 0, rsc.Len())
	} else if joinTp == ast.RightJoin {
		resetNotNullFlag(lsc, 0, lsc.Len())
	}
	lDeferredCausets, rDeferredCausets := lsc.DeferredCausets, rsc.DeferredCausets
	lNames, rNames := leftPlan.OutputNames().Shallow(), rightPlan.OutputNames().Shallow()
	if joinTp == ast.RightJoin {
		lNames, rNames = rNames, lNames
		lDeferredCausets, rDeferredCausets = rsc.DeferredCausets, lsc.DeferredCausets
	}

	// Find out all the common columns and put them ahead.
	commonLen := 0
	for i, lName := range lNames {
		for j := commonLen; j < len(rNames); j++ {
			if lName.DefCausName.L != rNames[j].DefCausName.L {
				continue
			}

			if len(filter) > 0 {
				if !filter[lName.DefCausName.L] {
					break
				}
				// Mark this column exist.
				filter[lName.DefCausName.L] = false
			}

			col := lDeferredCausets[i]
			INTERLOCKy(lDeferredCausets[commonLen+1:i+1], lDeferredCausets[commonLen:i])
			lDeferredCausets[commonLen] = col

			name := lNames[i]
			INTERLOCKy(lNames[commonLen+1:i+1], lNames[commonLen:i])
			lNames[commonLen] = name

			col = rDeferredCausets[j]
			INTERLOCKy(rDeferredCausets[commonLen+1:j+1], rDeferredCausets[commonLen:j])
			rDeferredCausets[commonLen] = col

			name = rNames[j]
			INTERLOCKy(rNames[commonLen+1:j+1], rNames[commonLen:j])
			rNames[commonLen] = name

			commonLen++
			break
		}
	}

	if len(filter) > 0 && len(filter) != commonLen {
		for col, notExist := range filter {
			if notExist {
				return ErrUnknownDeferredCauset.GenWithStackByArgs(col, "from clause")
			}
		}
	}

	schemaDefCauss := make([]*expression.DeferredCauset, len(lDeferredCausets)+len(rDeferredCausets)-commonLen)
	INTERLOCKy(schemaDefCauss[:len(lDeferredCausets)], lDeferredCausets)
	INTERLOCKy(schemaDefCauss[len(lDeferredCausets):], rDeferredCausets[commonLen:])
	names := make(types.NameSlice, len(schemaDefCauss))
	INTERLOCKy(names, lNames)
	INTERLOCKy(names[len(lNames):], rNames[commonLen:])

	conds := make([]expression.Expression, 0, commonLen)
	for i := 0; i < commonLen; i++ {
		lc, rc := lsc.DeferredCausets[i], rsc.DeferredCausets[i]
		cond, err := expression.NewFunction(b.ctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), lc, rc)
		if err != nil {
			return err
		}
		conds = append(conds, cond)
	}

	p.SetSchema(expression.NewSchema(schemaDefCauss...))
	p.names = names
	p.redundantSchema = expression.MergeSchema(p.redundantSchema, expression.NewSchema(rDeferredCausets[:commonLen]...))
	p.redundantNames = append(p.redundantNames.Shallow(), rNames[:commonLen]...)
	p.OtherConditions = append(conds, p.OtherConditions...)

	return nil
}

func (b *PlanBuilder) buildSelection(ctx context.Context, p LogicalPlan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, error) {
	b.optFlag |= flagPredicatePushDown
	if b.curClause != havingClause {
		b.curClause = whereClause
	}

	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := LogicalSelection{}.Init(b.ctx, b.getSelectOffset())
	for _, cond := range conditions {
		expr, np, err := b.rewrite(ctx, cond, p, AggMapper, false)
		if err != nil {
			return nil, err
		}
		p = np
		if expr == nil {
			continue
		}
		cnfItems := expression.SplitCNFItems(expr)
		for _, item := range cnfItems {
			if con, ok := item.(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
				ret, _, err := expression.EvalBool(b.ctx, expression.CNFExprs{con}, chunk.Row{})
				if err != nil || ret {
					continue
				}
				// If there is condition which is always false, return dual plan directly.
				dual := LogicalBlockDual{}.Init(b.ctx, b.getSelectOffset())
				dual.names = p.OutputNames()
				dual.SetSchema(p.Schema())
				return dual, nil
			}
			expressions = append(expressions, item)
		}
	}
	if len(expressions) == 0 {
		return p, nil
	}
	selection.Conditions = expressions
	selection.SetChildren(p)
	return selection, nil
}

// buildProjectionFieldNameFromDeferredCausets builds the field name, block name and database name when field expression is a column reference.
func (b *PlanBuilder) buildProjectionFieldNameFromDeferredCausets(origField *ast.SelectField, colNameField *ast.DeferredCausetNameExpr, name *types.FieldName) (colName, origDefCausName, tblName, origTblName, dbName perceptron.CIStr) {
	origTblName, origDefCausName, dbName = name.OrigTblName, name.OrigDefCausName, name.DBName
	if origField.AsName.L == "" {
		colName = colNameField.Name.Name
	} else {
		colName = origField.AsName
	}
	if tblName.L == "" {
		tblName = name.TblName
	} else {
		tblName = colNameField.Name.Block
	}
	return
}

// buildProjectionFieldNameFromExpressions builds the field name when field expression is a normal expression.
func (b *PlanBuilder) buildProjectionFieldNameFromExpressions(ctx context.Context, field *ast.SelectField) (perceptron.CIStr, error) {
	if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
		// When the query is select t.a from t group by a; The DeferredCauset Name should be a but not t.a;
		return agg.Args[0].(*ast.DeferredCausetNameExpr).Name.Name, nil
	}

	innerExpr := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	funcCall, isFuncCall := innerExpr.(*ast.FuncCallExpr)
	// When used to produce a result set column, NAME_CONST() causes the column to have the given name.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
	if isFuncCall && funcCall.FnName.L == ast.NameConst {
		if v, err := evalAstExpr(b.ctx, funcCall.Args[0]); err == nil {
			if s, err := v.ToString(); err == nil {
				return perceptron.NewCIStr(s), nil
			}
		}
		return perceptron.NewCIStr(""), ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
	}
	valueExpr, isValueExpr := innerExpr.(*driver.ValueExpr)

	// Non-literal: Output as inputed, except that comments need to be removed.
	if !isValueExpr {
		return perceptron.NewCIStr(berolinaAllegroSQL.SpecFieldPattern.ReplaceAllStringFunc(field.Text(), berolinaAllegroSQL.TrimComment)), nil
	}

	// Literal: Need special processing
	switch valueExpr.HoTT() {
	case types.HoTTString:
		projName := valueExpr.GetString()
		projOffset := valueExpr.GetProjectionOffset()
		if projOffset >= 0 {
			projName = projName[:projOffset]
		}
		// See #3686, #3994:
		// For string literals, string content is used as column name. Non-graph initial characters are trimmed.
		fieldName := strings.TrimLeftFunc(projName, func(r rune) bool {
			return !unicode.IsOneOf(allegrosql.RangeGraph, r)
		})
		return perceptron.NewCIStr(fieldName), nil
	case types.HoTTNull:
		// See #4053, #3685
		return perceptron.NewCIStr("NULL"), nil
	case types.HoTTBinaryLiteral:
		// Don't rewrite BIT literal or HEX literals
		return perceptron.NewCIStr(field.Text()), nil
	case types.HoTTInt64:
		// See #9683
		// TRUE or FALSE can be a int64
		if allegrosql.HasIsBooleanFlag(valueExpr.Type.Flag) {
			if i := valueExpr.GetValue().(int64); i == 0 {
				return perceptron.NewCIStr("FALSE"), nil
			}
			return perceptron.NewCIStr("TRUE"), nil
		}
		fallthrough

	default:
		fieldName := field.Text()
		fieldName = strings.TrimLeft(fieldName, "\t\n +(")
		fieldName = strings.TrimRight(fieldName, "\t\n )")
		return perceptron.NewCIStr(fieldName), nil
	}
}

// buildProjectionField builds the field object according to SelectField in projection.
func (b *PlanBuilder) buildProjectionField(ctx context.Context, p LogicalPlan, field *ast.SelectField, expr expression.Expression) (*expression.DeferredCauset, *types.FieldName, error) {
	var origTblName, tblName, origDefCausName, colName, dbName perceptron.CIStr
	innerNode := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	col, isDefCaus := expr.(*expression.DeferredCauset)
	// Correlated column won't affect the final output names. So we can put it in any of the three logic block.
	// Don't put it into the first block just for simplifying the codes.
	if colNameField, ok := innerNode.(*ast.DeferredCausetNameExpr); ok && isDefCaus {
		// Field is a column reference.
		idx := p.Schema().DeferredCausetIndex(col)
		var name *types.FieldName
		// The column maybe the one from join's redundant part.
		// TODO: Fully support USING/NATURAL JOIN, refactor here.
		if idx == -1 {
			if join, ok := p.(*LogicalJoin); ok {
				idx = join.redundantSchema.DeferredCausetIndex(col)
				name = join.redundantNames[idx]
			}
		} else {
			name = p.OutputNames()[idx]
		}
		colName, origDefCausName, tblName, origTblName, dbName = b.buildProjectionFieldNameFromDeferredCausets(field, colNameField, name)
	} else if field.AsName.L != "" {
		// Field has alias.
		colName = field.AsName
	} else {
		// Other: field is an expression.
		var err error
		if colName, err = b.buildProjectionFieldNameFromExpressions(ctx, field); err != nil {
			return nil, nil, err
		}
	}
	name := &types.FieldName{
		TblName:         tblName,
		OrigTblName:     origTblName,
		DefCausName:     colName,
		OrigDefCausName: origDefCausName,
		DBName:          dbName,
	}
	if isDefCaus {
		return col, name, nil
	}
	newDefCaus := &expression.DeferredCauset{
		UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
		RetType:  expr.GetType(),
	}
	return newDefCaus, name, nil
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *PlanBuilder) buildProjection(ctx context.Context, p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int, considerWindow bool, expandGenerateDeferredCauset bool) (LogicalPlan, int, error) {
	b.optFlag |= flagEliminateProjection
	b.curClause = fieldList
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(fields))}.Init(b.ctx, b.getSelectOffset())
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(fields))...)
	oldLen := 0
	newNames := make([]*types.FieldName, 0, len(fields))
	for i, field := range fields {
		if !field.Auxiliary {
			oldLen++
		}

		isWindowFuncField := ast.HasWindowFlag(field.Expr)
		// Although window functions occurs in the select fields, but it has to be processed after having clause.
		// So when we build the projection for select fields, we need to skip the window function.
		// When `considerWindow` is false, we will only build fields for non-window functions, so we add fake placeholders.
		// for window functions. These fake placeholders will be erased in column pruning.
		// When `considerWindow` is true, all the non-window fields have been built, so we just use the schemaReplicant columns.
		if considerWindow && !isWindowFuncField {
			col := p.Schema().DeferredCausets[i]
			proj.Exprs = append(proj.Exprs, col)
			schemaReplicant.Append(col)
			newNames = append(newNames, p.OutputNames()[i])
			continue
		} else if !considerWindow && isWindowFuncField {
			expr := expression.NewZero()
			proj.Exprs = append(proj.Exprs, expr)
			col, name, err := b.buildProjectionField(ctx, p, field, expr)
			if err != nil {
				return nil, 0, err
			}
			schemaReplicant.Append(col)
			newNames = append(newNames, name)
			continue
		}
		newExpr, np, err := b.rewriteWithPreprocess(ctx, field.Expr, p, mapper, windowMapper, true, nil)
		if err != nil {
			return nil, 0, err
		}

		// For window functions in the order by clause, we will append an field for it.
		// We need rewrite the window mapper here so order by clause could find the added field.
		if considerWindow && isWindowFuncField && field.Auxiliary {
			if windowExpr, ok := field.Expr.(*ast.WindowFuncExpr); ok {
				windowMapper[windowExpr] = i
			}
		}

		p = np
		proj.Exprs = append(proj.Exprs, newExpr)

		col, name, err := b.buildProjectionField(ctx, p, field, newExpr)
		if err != nil {
			return nil, 0, err
		}
		schemaReplicant.Append(col)
		newNames = append(newNames, name)
	}
	proj.SetSchema(schemaReplicant)
	proj.names = newNames
	if expandGenerateDeferredCauset {
		// Sometimes we need to add some fields to the projection so that we can use generate column substitute
		// optimization. For example: select a+1 from t order by a+1, with a virtual generate column c as (a+1) and
		// an index on c. We need to add c into the projection so that we can replace a+1 with c.
		exprToDeferredCauset := make(ExprDeferredCausetMap)
		collectGenerateDeferredCauset(p, exprToDeferredCauset)
		for expr, col := range exprToDeferredCauset {
			idx := p.Schema().DeferredCausetIndex(col)
			if idx == -1 {
				continue
			}
			if proj.schemaReplicant.Contains(col) {
				continue
			}
			proj.schemaReplicant.DeferredCausets = append(proj.schemaReplicant.DeferredCausets, col)
			proj.Exprs = append(proj.Exprs, expr)
			proj.names = append(proj.names, p.OutputNames()[idx])
		}
	}
	proj.SetChildren(p)
	return proj, oldLen, nil
}

func (b *PlanBuilder) buildDistinct(child LogicalPlan, length int) (*LogicalAggregation, error) {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagPushDownAgg
	plan4Agg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, child.Schema().Len()),
		GroupByItems: expression.DeferredCauset2Exprs(child.Schema().Clone().DeferredCausets[:length]),
	}.Init(b.ctx, child.SelectBlockOffset())
	if hint := b.BlockHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.collectGroupByDeferredCausets()
	for _, col := range child.Schema().DeferredCausets {
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, aggDesc)
	}
	plan4Agg.SetChildren(child)
	plan4Agg.SetSchema(child.Schema().Clone())
	plan4Agg.names = child.OutputNames()
	// Distinct will be rewritten as first_row, we reset the type here since the return type
	// of first_row is not always the same as the column arg of first_row.
	for i, col := range plan4Agg.schemaReplicant.DeferredCausets {
		col.RetType = plan4Agg.AggFuncs[i].RetTp
	}
	return plan4Agg, nil
}

// unionJoinFieldType finds the type which can carry the given types in Union.
func unionJoinFieldType(a, b *types.FieldType) *types.FieldType {
	resultTp := types.NewFieldType(types.MergeFieldType(a.Tp, b.Tp))
	// This logic will be intelligible when it is associated with the buildProjection4Union logic.
	if resultTp.Tp == allegrosql.TypeNewDecimal {
		// The decimal result type will be unsigned only when all the decimals to be united are unsigned.
		resultTp.Flag &= b.Flag & allegrosql.UnsignedFlag
	} else {
		// Non-decimal results will be unsigned when the first ALLEGROALLEGROSQL statement result in the union is unsigned.
		resultTp.Flag |= a.Flag & allegrosql.UnsignedFlag
	}
	resultTp.Decimal = mathutil.Max(a.Decimal, b.Decimal)
	// `Flen - Decimal` is the fraction before '.'
	resultTp.Flen = mathutil.Max(a.Flen-a.Decimal, b.Flen-b.Decimal) + resultTp.Decimal
	if resultTp.EvalType() != types.ETInt && (a.EvalType() == types.ETInt || b.EvalType() == types.ETInt) && resultTp.Flen < allegrosql.MaxIntWidth {
		resultTp.Flen = allegrosql.MaxIntWidth
	}
	resultTp.Charset = a.Charset
	resultTp.DefCauslate = a.DefCauslate
	expression.SetBinFlagOrBinStr(b, resultTp)
	return resultTp
}

func (b *PlanBuilder) buildProjection4Union(ctx context.Context, u *LogicalUnionAll) {
	unionDefCauss := make([]*expression.DeferredCauset, 0, u.children[0].Schema().Len())
	names := make([]*types.FieldName, 0, u.children[0].Schema().Len())

	// Infer union result types by its children's schemaReplicant.
	for i, col := range u.children[0].Schema().DeferredCausets {
		resultTp := col.RetType
		for j := 1; j < len(u.children); j++ {
			childTp := u.children[j].Schema().DeferredCausets[i].RetType
			resultTp = unionJoinFieldType(resultTp, childTp)
		}
		names = append(names, &types.FieldName{DefCausName: u.children[0].OutputNames()[i].DefCausName})
		unionDefCauss = append(unionDefCauss, &expression.DeferredCauset{
			RetType:  resultTp,
			UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
		})
	}
	u.schemaReplicant = expression.NewSchema(unionDefCauss...)
	u.names = names
	// Process each child and add a projection above original child.
	// So the schemaReplicant of `UnionAll` can be the same with its children's.
	for childID, child := range u.children {
		exprs := make([]expression.Expression, len(child.Schema().DeferredCausets))
		for i, srcDefCaus := range child.Schema().DeferredCausets {
			dstType := unionDefCauss[i].RetType
			srcType := srcDefCaus.RetType
			if !srcType.Equal(dstType) {
				exprs[i] = expression.BuildCastFunction4Union(b.ctx, srcDefCaus, dstType)
			} else {
				exprs[i] = srcDefCaus
			}
		}
		b.optFlag |= flagEliminateProjection
		proj := LogicalProjection{Exprs: exprs, AvoidDeferredCausetEvaluator: true}.Init(b.ctx, b.getSelectOffset())
		proj.SetSchema(u.schemaReplicant.Clone())
		proj.SetChildren(child)
		u.children[childID] = proj
	}
}

func (b *PlanBuilder) buildSetOpr(ctx context.Context, setOpr *ast.SetOprStmt) (LogicalPlan, error) {
	// Because INTERSECT has higher precedence than UNION and EXCEPT. We build it first.
	selectPlans := make([]LogicalPlan, 0, len(setOpr.SelectList.Selects))
	afterSetOprs := make([]*ast.SetOprType, 0, len(setOpr.SelectList.Selects))
	selects := setOpr.SelectList.Selects
	for i := 0; i < len(selects); i++ {
		intersects := []*ast.SelectStmt{selects[i]}
		for i+1 < len(selects) && *selects[i+1].AfterSetOperator == ast.Intersect {
			intersects = append(intersects, selects[i+1])
			i++
		}
		selectPlan, afterSetOpr, err := b.buildIntersect(ctx, intersects)
		if err != nil {
			return nil, err
		}
		selectPlans = append(selectPlans, selectPlan)
		afterSetOprs = append(afterSetOprs, afterSetOpr)
	}
	setOprPlan, err := b.buildExcept(ctx, selectPlans, afterSetOprs)
	if err != nil {
		return nil, err
	}

	oldLen := setOprPlan.Schema().Len()

	for i := 0; i < len(setOpr.SelectList.Selects); i++ {
		b.handleHelper.popMap()
	}
	b.handleHelper.pushMap(nil)

	if setOpr.OrderBy != nil {
		setOprPlan, err = b.buildSort(ctx, setOprPlan, setOpr.OrderBy.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	if setOpr.Limit != nil {
		setOprPlan, err = b.buildLimit(setOprPlan, setOpr.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Fix issue #8189 (https://github.com/whtcorpsinc/milevadb/issues/8189).
	// If there are extra expressions generated from `ORDER BY` clause, generate a `Projection` to remove them.
	if oldLen != setOprPlan.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.DeferredCauset2Exprs(setOprPlan.Schema().DeferredCausets[:oldLen])}.Init(b.ctx, b.getSelectOffset())
		proj.SetChildren(setOprPlan)
		schemaReplicant := expression.NewSchema(setOprPlan.Schema().Clone().DeferredCausets[:oldLen]...)
		for _, col := range schemaReplicant.DeferredCausets {
			col.UniqueID = b.ctx.GetStochastikVars().AllocPlanDeferredCausetID()
		}
		proj.names = setOprPlan.OutputNames()[:oldLen]
		proj.SetSchema(schemaReplicant)
		return proj, nil
	}
	return setOprPlan, nil
}

func (b *PlanBuilder) buildSemiJoinForSetOperator(
	leftOriginPlan LogicalPlan,
	rightPlan LogicalPlan,
	joinType JoinType) (leftPlan LogicalPlan, err error) {
	leftPlan, err = b.buildDistinct(leftOriginPlan, leftOriginPlan.Schema().Len())
	if err != nil {
		return nil, err
	}
	joinPlan := LogicalJoin{JoinType: joinType}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(leftPlan.Schema())
	joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len())
	INTERLOCKy(joinPlan.names, leftPlan.OutputNames())
	for j := 0; j < len(rightPlan.Schema().DeferredCausets); j++ {
		leftDefCaus, rightDefCaus := leftPlan.Schema().DeferredCausets[j], rightPlan.Schema().DeferredCausets[j]
		eqCond, err := expression.NewFunction(b.ctx, ast.NullEQ, types.NewFieldType(allegrosql.TypeTiny), leftDefCaus, rightDefCaus)
		if err != nil {
			return nil, err
		}
		if leftDefCaus.RetType.Tp != rightDefCaus.RetType.Tp {
			joinPlan.OtherConditions = append(joinPlan.OtherConditions, eqCond)
		} else {
			joinPlan.EqualConditions = append(joinPlan.EqualConditions, eqCond.(*expression.ScalarFunction))
		}
	}
	return joinPlan, nil
}

// buildIntersect build the set operator for 'intersect'. It is called before buildExcept and buildUnion because of its
// higher precedence.
func (b *PlanBuilder) buildIntersect(ctx context.Context, selects []*ast.SelectStmt) (LogicalPlan, *ast.SetOprType, error) {
	leftPlan, err := b.buildSelect(ctx, selects[0])
	if err != nil {
		return nil, nil, err
	}
	if len(selects) == 1 {
		return leftPlan, selects[0].AfterSetOperator, nil
	}

	columnNums := leftPlan.Schema().Len()
	for i := 1; i < len(selects); i++ {
		rightPlan, err := b.buildSelect(ctx, selects[i])
		if err != nil {
			return nil, nil, err
		}
		if rightPlan.Schema().Len() != columnNums {
			return nil, nil, ErrWrongNumberOfDeferredCausetsInSelect.GenWithStackByArgs()
		}
		leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, SemiJoin)
		if err != nil {
			return nil, nil, err
		}
	}
	return leftPlan, selects[0].AfterSetOperator, nil
}

// buildExcept build the set operators for 'except', and in this function, it calls buildUnion at the same time. Because
// Union and except has the same precedence.
func (b *PlanBuilder) buildExcept(ctx context.Context, selects []LogicalPlan, afterSetOpts []*ast.SetOprType) (LogicalPlan, error) {
	unionPlans := []LogicalPlan{selects[0]}
	tmpAfterSetOpts := []*ast.SetOprType{nil}
	columnNums := selects[0].Schema().Len()
	for i := 1; i < len(selects); i++ {
		rightPlan := selects[i]
		if rightPlan.Schema().Len() != columnNums {
			return nil, ErrWrongNumberOfDeferredCausetsInSelect.GenWithStackByArgs()
		}
		if *afterSetOpts[i] == ast.Except {
			leftPlan, err := b.buildUnion(ctx, unionPlans, tmpAfterSetOpts)
			if err != nil {
				return nil, err
			}
			leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, AntiSemiJoin)
			if err != nil {
				return nil, err
			}
			unionPlans = []LogicalPlan{leftPlan}
			tmpAfterSetOpts = []*ast.SetOprType{nil}
		} else {
			unionPlans = append(unionPlans, rightPlan)
			tmpAfterSetOpts = append(tmpAfterSetOpts, afterSetOpts[i])
		}
	}
	return b.buildUnion(ctx, unionPlans, tmpAfterSetOpts)
}

func (b *PlanBuilder) buildUnion(ctx context.Context, selects []LogicalPlan, afterSetOpts []*ast.SetOprType) (LogicalPlan, error) {
	if len(selects) == 1 {
		return selects[0], nil
	}
	distinctSelectPlans, allSelectPlans, err := b.divideUnionSelectPlans(ctx, selects, afterSetOpts)
	if err != nil {
		return nil, err
	}

	unionDistinctPlan := b.buildUnionAll(ctx, distinctSelectPlans)
	if unionDistinctPlan != nil {
		unionDistinctPlan, err = b.buildDistinct(unionDistinctPlan, unionDistinctPlan.Schema().Len())
		if err != nil {
			return nil, err
		}
		if len(allSelectPlans) > 0 {
			// Can't change the statements order in order to get the correct column info.
			allSelectPlans = append([]LogicalPlan{unionDistinctPlan}, allSelectPlans...)
		}
	}

	unionAllPlan := b.buildUnionAll(ctx, allSelectPlans)
	unionPlan := unionDistinctPlan
	if unionAllPlan != nil {
		unionPlan = unionAllPlan
	}

	return unionPlan, nil
}

// divideUnionSelectPlans resolves union's select stmts to logical plans.
// and divide result plans into "union-distinct" and "union-all" parts.
// divide rule ref:
//		https://dev.allegrosql.com/doc/refman/5.7/en/union.html
// "Mixed UNION types are treated such that a DISTINCT union overrides any ALL union to its left."
func (b *PlanBuilder) divideUnionSelectPlans(ctx context.Context, selects []LogicalPlan, setOprTypes []*ast.SetOprType) (distinctSelects []LogicalPlan, allSelects []LogicalPlan, err error) {
	firstUnionAllIdx := 0
	columnNums := selects[0].Schema().Len()
	for i := len(selects) - 1; i > 0; i-- {
		if firstUnionAllIdx == 0 && *setOprTypes[i] != ast.UnionAll {
			firstUnionAllIdx = i + 1
		}
		if selects[i].Schema().Len() != columnNums {
			return nil, nil, ErrWrongNumberOfDeferredCausetsInSelect.GenWithStackByArgs()
		}
	}
	return selects[:firstUnionAllIdx], selects[firstUnionAllIdx:], nil
}

func (b *PlanBuilder) buildUnionAll(ctx context.Context, subPlan []LogicalPlan) LogicalPlan {
	if len(subPlan) == 0 {
		return nil
	}
	u := LogicalUnionAll{}.Init(b.ctx, b.getSelectOffset())
	u.children = subPlan
	b.buildProjection4Union(ctx, u)
	return u
}

// itemTransformer transforms ParamMarkerExpr to PositionExpr in the context of ByItem
type itemTransformer struct {
}

func (t *itemTransformer) Enter(inNode ast.Node) (ast.Node, bool) {
	switch n := inNode.(type) {
	case *driver.ParamMarkerExpr:
		newNode := expression.ConstructPositionExpr(n)
		return newNode, true
	}
	return inNode, false
}

func (t *itemTransformer) Leave(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

func (b *PlanBuilder) buildSort(ctx context.Context, p LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int) (*LogicalSort, error) {
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		b.curClause = globalOrderByClause
	} else {
		b.curClause = orderByClause
	}
	sort := LogicalSort{}.Init(b.ctx, b.getSelectOffset())
	exprs := make([]*soliton.ByItems, 0, len(byItems))
	transformer := &itemTransformer{}
	for _, item := range byItems {
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewriteWithPreprocess(ctx, item.Expr, p, aggMapper, windowMapper, true, nil)
		if err != nil {
			return nil, err
		}

		p = np
		exprs = append(exprs, &soliton.ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	sort.SetChildren(p)
	return sort, nil
}

// getUintFromNode gets uint64 value from ast.Node.
// For ordinary statement, node should be uint64 constant value.
// For prepared statement, node is string. We should convert it to uint64.
func getUintFromNode(ctx stochastikctx.Context, n ast.Node) (uVal uint64, isNull bool, isExpectedType bool) {
	var val interface{}
	switch v := n.(type) {
	case *driver.ValueExpr:
		val = v.GetValue()
	case *driver.ParamMarkerExpr:
		if !v.InExecute {
			return 0, false, true
		}
		param, err := expression.ParamMarkerExpression(ctx, v)
		if err != nil {
			return 0, false, false
		}
		str, isNull, err := expression.GetStringFromConstant(ctx, param)
		if err != nil {
			return 0, false, false
		}
		if isNull {
			return 0, true, true
		}
		val = str
	default:
		return 0, false, false
	}
	switch v := val.(type) {
	case uint64:
		return v, false, true
	case int64:
		if v >= 0 {
			return uint64(v), false, true
		}
	case string:
		sc := ctx.GetStochastikVars().StmtCtx
		uVal, err := types.StrToUint(sc, v, false)
		if err != nil {
			return 0, false, false
		}
		return uVal, false, true
	}
	return 0, false, false
}

func extractLimitCountOffset(ctx stochastikctx.Context, limit *ast.Limit) (count uint64,
	offset uint64, err error) {
	var isExpectedType bool
	if limit.Count != nil {
		count, _, isExpectedType = getUintFromNode(ctx, limit.Count)
		if !isExpectedType {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	if limit.Offset != nil {
		offset, _, isExpectedType = getUintFromNode(ctx, limit.Offset)
		if !isExpectedType {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	return count, offset, nil
}

func (b *PlanBuilder) buildLimit(src LogicalPlan, limit *ast.Limit) (LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPushDownTopN
	var (
		offset, count uint64
		err           error
	)
	if count, offset, err = extractLimitCountOffset(b.ctx, limit); err != nil {
		return nil, err
	}

	if count > math.MaxUint64-offset {
		count = math.MaxUint64 - offset
	}
	if offset+count == 0 {
		blockDual := LogicalBlockDual{RowCount: 0}.Init(b.ctx, b.getSelectOffset())
		blockDual.schemaReplicant = src.Schema()
		blockDual.names = src.OutputNames()
		return blockDual, nil
	}
	li := LogicalLimit{
		Offset: offset,
		Count:  count,
	}.Init(b.ctx, b.getSelectOffset())
	if hint := b.BlockHints(); hint != nil {
		li.limitHints = hint.limitHints
	}
	li.SetChildren(src)
	return li, nil
}

// colMatch means that if a match b, e.g. t.a can match test.t.a but test.t.a can't match t.a.
// Because column a want column from database test exactly.
func colMatch(a *ast.DeferredCausetName, b *ast.DeferredCausetName) bool {
	if a.Schema.L == "" || a.Schema.L == b.Schema.L {
		if a.Block.L == "" || a.Block.L == b.Block.L {
			return a.Name.L == b.Name.L
		}
	}
	return false
}

func matchField(f *ast.SelectField, col *ast.DeferredCausetNameExpr, ignoreAsName bool) bool {
	// if col specify a block name, resolve from block source directly.
	if col.Name.Block.L == "" {
		if f.AsName.L == "" || ignoreAsName {
			if curDefCaus, isDefCaus := f.Expr.(*ast.DeferredCausetNameExpr); isDefCaus {
				return curDefCaus.Name.Name.L == col.Name.Name.L
			} else if _, isFunc := f.Expr.(*ast.FuncCallExpr); isFunc {
				// Fix issue 7331
				// If there are some function calls in SelectField, we check if
				// DeferredCausetNameExpr in GroupByClause matches one of these function calls.
				// Example: select concat(k1,k2) from t group by `concat(k1,k2)`,
				// `concat(k1,k2)` matches with function call concat(k1, k2).
				return strings.ToLower(f.Text()) == col.Name.Name.L
			}
			// a expression without as name can't be matched.
			return false
		}
		return f.AsName.L == col.Name.Name.L
	}
	return false
}

func resolveFromSelectFields(v *ast.DeferredCausetNameExpr, fields []*ast.SelectField, ignoreAsName bool) (index int, err error) {
	var matchedExpr ast.ExprNode
	index = -1
	for i, field := range fields {
		if field.Auxiliary {
			continue
		}
		if matchField(field, v, ignoreAsName) {
			curDefCaus, isDefCaus := field.Expr.(*ast.DeferredCausetNameExpr)
			if !isDefCaus {
				return i, nil
			}
			if matchedExpr == nil {
				matchedExpr = curDefCaus
				index = i
			} else if !colMatch(matchedExpr.(*ast.DeferredCausetNameExpr).Name, curDefCaus.Name) &&
				!colMatch(curDefCaus.Name, matchedExpr.(*ast.DeferredCausetNameExpr).Name) {
				return -1, ErrAmbiguous.GenWithStackByArgs(curDefCaus.Name.Name.L, clauseMsg[fieldList])
			}
		}
	}
	return
}

// havingWindowAndOrderbyExprResolver visits Expr tree.
// It converts DefCausunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type havingWindowAndOrderbyExprResolver struct {
	inAggFunc    bool
	inWindowFunc bool
	inWindowSpec bool
	inExpr       bool
	err          error
	p            LogicalPlan
	selectFields []*ast.SelectField
	aggMapper    map[*ast.AggregateFuncExpr]int
	colMapper    map[*ast.DeferredCausetNameExpr]int
	gbyItems     []*ast.ByItem
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	curClause    clauseCode
}

// Enter implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = true
	case *ast.WindowFuncExpr:
		a.inWindowFunc = true
	case *ast.WindowSpec:
		a.inWindowSpec = true
	case *driver.ParamMarkerExpr, *ast.DeferredCausetNameExpr, *ast.DeferredCausetName:
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		return n, true
	default:
		a.inExpr = true
	}
	return n, false
}

func (a *havingWindowAndOrderbyExprResolver) resolveFromPlan(v *ast.DeferredCausetNameExpr, p LogicalPlan) (int, error) {
	idx, err := expression.FindFieldName(p.OutputNames(), v.Name)
	if err != nil {
		return -1, err
	}
	if idx < 0 {
		return -1, nil
	}
	col := p.Schema().DeferredCausets[idx]
	if col.IsHidden {
		return -1, ErrUnknownDeferredCauset.GenWithStackByArgs(v.Name, clauseMsg[a.curClause])
	}
	name := p.OutputNames()[idx]
	newDefCausName := &ast.DeferredCausetName{
		Schema: name.DBName,
		Block:  name.TblName,
		Name:   name.DefCausName,
	}
	for i, field := range a.selectFields {
		if c, ok := field.Expr.(*ast.DeferredCausetNameExpr); ok && colMatch(c.Name, newDefCausName) {
			return i, nil
		}
	}
	sf := &ast.SelectField{
		Expr:      &ast.DeferredCausetNameExpr{Name: newDefCausName},
		Auxiliary: true,
	}
	sf.Expr.SetType(col.GetType())
	a.selectFields = append(a.selectFields, sf)
	return len(a.selectFields) - 1, nil
}

// Leave implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = false
		a.aggMapper[v] = len(a.selectFields)
		a.selectFields = append(a.selectFields, &ast.SelectField{
			Auxiliary: true,
			Expr:      v,
			AsName:    perceptron.NewCIStr(fmt.Sprintf("sel_agg_%d", len(a.selectFields))),
		})
	case *ast.WindowFuncExpr:
		a.inWindowFunc = false
		if a.curClause == havingClause {
			a.err = ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(strings.ToLower(v.F))
			return node, false
		}
		if a.curClause == orderByClause {
			a.selectFields = append(a.selectFields, &ast.SelectField{
				Auxiliary: true,
				Expr:      v,
				AsName:    perceptron.NewCIStr(fmt.Sprintf("sel_window_%d", len(a.selectFields))),
			})
		}
	case *ast.WindowSpec:
		a.inWindowSpec = false
	case *ast.DeferredCausetNameExpr:
		resolveFieldsFirst := true
		if a.inAggFunc || a.inWindowFunc || a.inWindowSpec || (a.curClause == orderByClause && a.inExpr) || a.curClause == fieldList {
			resolveFieldsFirst = false
		}
		if !a.inAggFunc && a.curClause != orderByClause {
			for _, item := range a.gbyItems {
				if col, ok := item.Expr.(*ast.DeferredCausetNameExpr); ok &&
					(colMatch(v.Name, col.Name) || colMatch(col.Name, v.Name)) {
					resolveFieldsFirst = false
					break
				}
			}
		}
		var index int
		if resolveFieldsFirst {
			index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			if a.err != nil {
				return node, false
			}
			if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
				a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
				return node, false
			}
			if index == -1 {
				if a.curClause == orderByClause {
					index, a.err = a.resolveFromPlan(v, a.p)
				} else if a.curClause == havingClause && v.Name.Block.L != "" {
					// For ALLEGROSQLs like:
					//   select a from t b having b.a;
					index, a.err = a.resolveFromPlan(v, a.p)
					if a.err != nil {
						return node, false
					}
					if index != -1 {
						// For ALLEGROSQLs like:
						//   select a+1 from t having t.a;
						newV := v
						newV.Name = &ast.DeferredCausetName{Name: v.Name.Name}
						index, a.err = resolveFromSelectFields(newV, a.selectFields, true)
					}
				} else {
					index, a.err = resolveFromSelectFields(v, a.selectFields, true)
				}
			}
		} else {
			// We should ignore the err when resolving from schemaReplicant. Because we could resolve successfully
			// when considering select fields.
			var err error
			index, err = a.resolveFromPlan(v, a.p)
			_ = err
			if index == -1 && a.curClause != fieldList {
				index, a.err = resolveFromSelectFields(v, a.selectFields, false)
				if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
					a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
					return node, false
				}
			}
		}
		if a.err != nil {
			return node, false
		}
		if index == -1 {
			// If we can't find it any where, it may be a correlated columns.
			for _, names := range a.outerNames {
				idx, err1 := expression.FindFieldName(names, v.Name)
				if err1 != nil {
					a.err = err1
					return node, false
				}
				if idx >= 0 {
					return n, true
				}
			}
			a.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.Name.OrigDefCausName(), clauseMsg[a.curClause])
			return node, false
		}
		if a.inAggFunc {
			return a.selectFields[index].Expr, true
		}
		a.colMapper[v] = index
	}
	return n, true
}

// resolveHavingAndOrderBy will process aggregate functions and resolve the columns that don't exist in select fields.
// If we found some columns that are not in select fields, we will append it to select fields and uFIDelate the colMapper.
// When we rewrite the order by / having expression, we will find column in map at first.
func (b *PlanBuilder) resolveHavingAndOrderBy(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	if sel.GroupBy != nil {
		extractor.gbyItems = sel.GroupBy.Items
	}
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		extractor.curClause = havingClause
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			return nil, nil, errors.Trace(extractor.err)
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	havingAggMapper := extractor.aggMapper
	extractor.aggMapper = make(map[*ast.AggregateFuncExpr]int)
	extractor.inExpr = false
	// Extract agg funcs from order by clause.
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			if ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, nil, errors.Trace(extractor.err)
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return havingAggMapper, extractor.aggMapper, nil
}

func (b *PlanBuilder) extractAggFuncs(fields []*ast.SelectField) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	extractor := &AggregateFuncExtractor{}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int, len(aggList))

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, totalAggMapper
}

// resolveWindowFunction will process window functions and resolve the columns that don't exist in select fields.
func (b *PlanBuilder) resolveWindowFunction(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	extractor.curClause = fieldList
	for _, field := range sel.Fields.Fields {
		if !ast.HasWindowFlag(field.Expr) {
			continue
		}
		n, ok := field.Expr.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
		field.Expr = n.(ast.ExprNode)
	}
	for _, spec := range sel.WindowSpecs {
		_, ok := spec.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
	}
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			if !ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, extractor.err
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return extractor.aggMapper, nil
}

// gbyResolver resolves group by items from select fields.
type gbyResolver struct {
	ctx             stochastikctx.Context
	fields          []*ast.SelectField
	schemaReplicant *expression.Schema
	names           []*types.FieldName
	err             error
	inExpr          bool
	isParam         bool

	exprDepth int // exprDepth is the depth of current expression in expression tree.
}

func (g *gbyResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	g.exprDepth++
	switch n := inNode.(type) {
	case *ast.SubqueryExpr, *ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr:
		return inNode, true
	case *driver.ParamMarkerExpr:
		g.isParam = true
		if g.exprDepth == 1 {
			_, isNull, isExpectedType := getUintFromNode(g.ctx, n)
			// For constant uint expression in top level, it should be treated as position expression.
			if !isNull && isExpectedType {
				return expression.ConstructPositionExpr(n), true
			}
		}
		return n, true
	case *driver.ValueExpr, *ast.DeferredCausetNameExpr, *ast.ParenthesesExpr, *ast.DeferredCausetName:
	default:
		g.inExpr = true
	}
	return inNode, false
}

func (g *gbyResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	extractor := &AggregateFuncExtractor{}
	switch v := inNode.(type) {
	case *ast.DeferredCausetNameExpr:
		idx, err := expression.FindFieldName(g.names, v.Name)
		if idx < 0 || !g.inExpr {
			var index int
			index, g.err = resolveFromSelectFields(v, g.fields, false)
			if g.err != nil {
				return inNode, false
			}
			if idx >= 0 {
				return inNode, true
			}
			if index != -1 {
				ret := g.fields[index].Expr
				ret.Accept(extractor)
				if len(extractor.AggFuncs) != 0 {
					err = ErrIllegalReference.GenWithStackByArgs(v.Name.OrigDefCausName(), "reference to group function")
				} else if ast.HasWindowFlag(ret) {
					err = ErrIllegalReference.GenWithStackByArgs(v.Name.OrigDefCausName(), "reference to window function")
				} else {
					return ret, true
				}
			}
			g.err = err
			return inNode, false
		}
	case *ast.PositionExpr:
		pos, isNull, err := expression.PosFromPositionExpr(g.ctx, v)
		if err != nil {
			g.err = ErrUnknown.GenWithStackByArgs()
		}
		if err != nil || isNull {
			return inNode, false
		}
		if pos < 1 || pos > len(g.fields) {
			g.err = errors.Errorf("Unknown column '%d' in 'group statement'", pos)
			return inNode, false
		}
		ret := g.fields[pos-1].Expr
		ret.Accept(extractor)
		if len(extractor.AggFuncs) != 0 {
			g.err = ErrWrongGroupField.GenWithStackByArgs(g.fields[pos-1].Text())
			return inNode, false
		}
		if _, ok := ret.(*ast.WindowFuncExpr); ok {
			g.err = ErrWrongGroupField.GenWithStackByArgs(g.fields[pos-1].Text())
			return inNode, false
		}
		return ret, true
	case *ast.ValuesExpr:
		if v.DeferredCauset == nil {
			g.err = ErrUnknownDeferredCauset.GenWithStackByArgs("", "VALUES() function")
		}
	}
	return inNode, true
}

func tblInfoFromDefCaus(from ast.ResultSetNode, name *types.FieldName) *perceptron.BlockInfo {
	var blockList []*ast.BlockName
	blockList = extractBlockList(from, blockList, true)
	for _, field := range blockList {
		if field.Name.L == name.TblName.L {
			return field.BlockInfo
		}
		if field.Name.L != name.TblName.L {
			continue
		}
		if field.Schema.L == name.DBName.L {
			return field.BlockInfo
		}
	}
	return nil
}

func buildFuncDependDefCaus(p LogicalPlan, cond ast.ExprNode) (*types.FieldName, *types.FieldName) {
	binOpExpr, ok := cond.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, nil
	}
	if binOpExpr.Op != opcode.EQ {
		return nil, nil
	}
	lDefCausExpr, ok := binOpExpr.L.(*ast.DeferredCausetNameExpr)
	if !ok {
		return nil, nil
	}
	rDefCausExpr, ok := binOpExpr.R.(*ast.DeferredCausetNameExpr)
	if !ok {
		return nil, nil
	}
	lIdx, err := expression.FindFieldName(p.OutputNames(), lDefCausExpr.Name)
	if err != nil {
		return nil, nil
	}
	rIdx, err := expression.FindFieldName(p.OutputNames(), rDefCausExpr.Name)
	if err != nil {
		return nil, nil
	}
	return p.OutputNames()[lIdx], p.OutputNames()[rIdx]
}

func buildWhereFuncDepend(p LogicalPlan, where ast.ExprNode) map[*types.FieldName]*types.FieldName {
	whereConditions := splitWhere(where)
	colDependMap := make(map[*types.FieldName]*types.FieldName, 2*len(whereConditions))
	for _, cond := range whereConditions {
		lDefCaus, rDefCaus := buildFuncDependDefCaus(p, cond)
		if lDefCaus == nil || rDefCaus == nil {
			continue
		}
		colDependMap[lDefCaus] = rDefCaus
		colDependMap[rDefCaus] = lDefCaus
	}
	return colDependMap
}

func buildJoinFuncDepend(p LogicalPlan, from ast.ResultSetNode) map[*types.FieldName]*types.FieldName {
	switch x := from.(type) {
	case *ast.Join:
		if x.On == nil {
			return nil
		}
		onConditions := splitWhere(x.On.Expr)
		colDependMap := make(map[*types.FieldName]*types.FieldName, len(onConditions))
		for _, cond := range onConditions {
			lDefCaus, rDefCaus := buildFuncDependDefCaus(p, cond)
			if lDefCaus == nil || rDefCaus == nil {
				continue
			}
			lTbl := tblInfoFromDefCaus(x.Left, lDefCaus)
			if lTbl == nil {
				lDefCaus, rDefCaus = rDefCaus, lDefCaus
			}
			switch x.Tp {
			case ast.CrossJoin:
				colDependMap[lDefCaus] = rDefCaus
				colDependMap[rDefCaus] = lDefCaus
			case ast.LeftJoin:
				colDependMap[rDefCaus] = lDefCaus
			case ast.RightJoin:
				colDependMap[lDefCaus] = rDefCaus
			}
		}
		return colDependMap
	default:
		return nil
	}
}

func checkDefCausFuncDepend(
	p LogicalPlan,
	name *types.FieldName,
	tblInfo *perceptron.BlockInfo,
	gbyDefCausNames map[*types.FieldName]struct{},
	whereDependNames, joinDependNames map[*types.FieldName]*types.FieldName,
) bool {
	for _, index := range tblInfo.Indices {
		if !index.Unique {
			continue
		}
		funcDepend := true
		for _, indexDefCaus := range index.DeferredCausets {
			iDefCausInfo := tblInfo.DeferredCausets[indexDefCaus.Offset]
			if !allegrosql.HasNotNullFlag(iDefCausInfo.Flag) {
				funcDepend = false
				break
			}
			cn := &ast.DeferredCausetName{
				Schema: name.DBName,
				Block:  name.TblName,
				Name:   iDefCausInfo.Name,
			}
			iIdx, err := expression.FindFieldName(p.OutputNames(), cn)
			if err != nil || iIdx < 0 {
				funcDepend = false
				break
			}
			iName := p.OutputNames()[iIdx]
			if _, ok := gbyDefCausNames[iName]; ok {
				continue
			}
			if wDefCaus, ok := whereDependNames[iName]; ok {
				if _, ok = gbyDefCausNames[wDefCaus]; ok {
					continue
				}
			}
			if jDefCaus, ok := joinDependNames[iName]; ok {
				if _, ok = gbyDefCausNames[jDefCaus]; ok {
					continue
				}
			}
			funcDepend = false
			break
		}
		if funcDepend {
			return true
		}
	}
	primaryFuncDepend := true
	hasPrimaryField := false
	for _, colInfo := range tblInfo.DeferredCausets {
		if !allegrosql.HasPriKeyFlag(colInfo.Flag) {
			continue
		}
		hasPrimaryField = true
		pkName := &ast.DeferredCausetName{
			Schema: name.DBName,
			Block:  name.TblName,
			Name:   colInfo.Name,
		}
		pIdx, err := expression.FindFieldName(p.OutputNames(), pkName)
		if err != nil {
			primaryFuncDepend = false
			break
		}
		pDefCaus := p.OutputNames()[pIdx]
		if _, ok := gbyDefCausNames[pDefCaus]; ok {
			continue
		}
		if wDefCaus, ok := whereDependNames[pDefCaus]; ok {
			if _, ok = gbyDefCausNames[wDefCaus]; ok {
				continue
			}
		}
		if jDefCaus, ok := joinDependNames[pDefCaus]; ok {
			if _, ok = gbyDefCausNames[jDefCaus]; ok {
				continue
			}
		}
		primaryFuncDepend = false
		break
	}
	return primaryFuncDepend && hasPrimaryField
}

// ErrExprLoc is for generate the ErrFieldNotInGroupBy error info
type ErrExprLoc struct {
	Offset int
	Loc    string
}

func checkExprInGroupBy(
	p LogicalPlan,
	expr ast.ExprNode,
	offset int,
	loc string,
	gbyDefCausNames map[*types.FieldName]struct{},
	gbyExprs []ast.ExprNode,
	notInGbyDefCausNames map[*types.FieldName]ErrExprLoc,
) {
	if _, ok := expr.(*ast.AggregateFuncExpr); ok {
		return
	}
	if _, ok := expr.(*ast.DeferredCausetNameExpr); !ok {
		for _, gbyExpr := range gbyExprs {
			if reflect.DeepEqual(gbyExpr, expr) {
				return
			}
		}
	}
	// Function `any_value` can be used in aggregation, even `ONLY_FULL_GROUP_BY` is set.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value for details
	if f, ok := expr.(*ast.FuncCallExpr); ok {
		if f.FnName.L == ast.AnyValue {
			return
		}
	}
	colMap := make(map[*types.FieldName]struct{}, len(p.Schema().DeferredCausets))
	allDefCausFromExprNode(p, expr, colMap)
	for col := range colMap {
		if _, ok := gbyDefCausNames[col]; !ok {
			notInGbyDefCausNames[col] = ErrExprLoc{Offset: offset, Loc: loc}
		}
	}
}

func (b *PlanBuilder) checkOnlyFullGroupBy(p LogicalPlan, sel *ast.SelectStmt) (err error) {
	if sel.GroupBy != nil {
		err = b.checkOnlyFullGroupByWithGroupClause(p, sel)
	} else {
		err = b.checkOnlyFullGroupByWithOutGroupClause(p, sel.Fields.Fields)
	}
	return err
}

func (b *PlanBuilder) checkOnlyFullGroupByWithGroupClause(p LogicalPlan, sel *ast.SelectStmt) error {
	gbyDefCausNames := make(map[*types.FieldName]struct{}, len(sel.Fields.Fields))
	gbyExprs := make([]ast.ExprNode, 0, len(sel.Fields.Fields))
	for _, byItem := range sel.GroupBy.Items {
		expr := getInnerFromParenthesesAndUnaryPlus(byItem.Expr)
		if colExpr, ok := expr.(*ast.DeferredCausetNameExpr); ok {
			idx, err := expression.FindFieldName(p.OutputNames(), colExpr.Name)
			if err != nil || idx < 0 {
				continue
			}
			gbyDefCausNames[p.OutputNames()[idx]] = struct{}{}
		} else {
			gbyExprs = append(gbyExprs, expr)
		}
	}

	notInGbyDefCausNames := make(map[*types.FieldName]ErrExprLoc, len(sel.Fields.Fields))
	for offset, field := range sel.Fields.Fields {
		if field.Auxiliary {
			continue
		}
		checkExprInGroupBy(p, getInnerFromParenthesesAndUnaryPlus(field.Expr), offset, ErrExprInSelect, gbyDefCausNames, gbyExprs, notInGbyDefCausNames)
	}

	if sel.OrderBy != nil {
		for offset, item := range sel.OrderBy.Items {
			if colName, ok := item.Expr.(*ast.DeferredCausetNameExpr); ok {
				index, err := resolveFromSelectFields(colName, sel.Fields.Fields, false)
				if err != nil {
					return err
				}
				// If the ByItem is in fields list, it has been checked already in above.
				if index >= 0 {
					continue
				}
			}
			checkExprInGroupBy(p, item.Expr, offset, ErrExprInOrderBy, gbyDefCausNames, gbyExprs, notInGbyDefCausNames)
		}
	}
	if len(notInGbyDefCausNames) == 0 {
		return nil
	}

	whereDepends := buildWhereFuncDepend(p, sel.Where)
	joinDepends := buildJoinFuncDepend(p, sel.From.BlockRefs)
	tblMap := make(map[*perceptron.BlockInfo]struct{}, len(notInGbyDefCausNames))
	for name, errExprLoc := range notInGbyDefCausNames {
		tblInfo := tblInfoFromDefCaus(sel.From.BlockRefs, name)
		if tblInfo == nil {
			continue
		}
		if _, ok := tblMap[tblInfo]; ok {
			continue
		}
		if checkDefCausFuncDepend(p, name, tblInfo, gbyDefCausNames, whereDepends, joinDepends) {
			tblMap[tblInfo] = struct{}{}
			continue
		}
		switch errExprLoc.Loc {
		case ErrExprInSelect:
			return ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, name.DBName.O+"."+name.TblName.O+"."+name.OrigDefCausName.O)
		case ErrExprInOrderBy:
			return ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, sel.OrderBy.Items[errExprLoc.Offset].Expr.Text())
		}
		return nil
	}
	return nil
}

func (b *PlanBuilder) checkOnlyFullGroupByWithOutGroupClause(p LogicalPlan, fields []*ast.SelectField) error {
	resolver := colResolverForOnlyFullGroupBy{}
	for idx, field := range fields {
		resolver.exprIdx = idx
		field.Accept(&resolver)
		err := resolver.Check()
		if err != nil {
			return err
		}
	}
	return nil
}

// colResolverForOnlyFullGroupBy visits Expr tree to find out if an Expr tree is an aggregation function.
// If so, find out the first column name that not in an aggregation function.
type colResolverForOnlyFullGroupBy struct {
	firstNonAggDefCaus    *ast.DeferredCausetName
	exprIdx               int
	firstNonAggDefCausIdx int
	hasAggFuncOrAnyValue  bool
}

func (c *colResolverForOnlyFullGroupBy) Enter(node ast.Node) (ast.Node, bool) {
	switch t := node.(type) {
	case *ast.AggregateFuncExpr:
		c.hasAggFuncOrAnyValue = true
		return node, true
	case *ast.FuncCallExpr:
		// enable function `any_value` in aggregation even `ONLY_FULL_GROUP_BY` is set
		if t.FnName.L == ast.AnyValue {
			c.hasAggFuncOrAnyValue = true
			return node, true
		}
	case *ast.DeferredCausetNameExpr:
		if c.firstNonAggDefCaus == nil {
			c.firstNonAggDefCaus, c.firstNonAggDefCausIdx = t.Name, c.exprIdx
		}
		return node, true
	case *ast.SubqueryExpr:
		return node, true
	}
	return node, false
}

func (c *colResolverForOnlyFullGroupBy) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

func (c *colResolverForOnlyFullGroupBy) Check() error {
	if c.hasAggFuncOrAnyValue && c.firstNonAggDefCaus != nil {
		return ErrMixOfGroupFuncAndFields.GenWithStackByArgs(c.firstNonAggDefCausIdx+1, c.firstNonAggDefCaus.Name.O)
	}
	return nil
}

type colNameResolver struct {
	p     LogicalPlan
	names map[*types.FieldName]struct{}
}

func (c *colNameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch inNode.(type) {
	case *ast.DeferredCausetNameExpr, *ast.SubqueryExpr, *ast.AggregateFuncExpr:
		return inNode, true
	}
	return inNode, false
}

func (c *colNameResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.DeferredCausetNameExpr:
		idx, err := expression.FindFieldName(c.p.OutputNames(), v.Name)
		if err == nil && idx >= 0 {
			c.names[c.p.OutputNames()[idx]] = struct{}{}
		}
	}
	return inNode, true
}

func allDefCausFromExprNode(p LogicalPlan, n ast.Node, names map[*types.FieldName]struct{}) {
	extractor := &colNameResolver{
		p:     p,
		names: names,
	}
	n.Accept(extractor)
}

func (b *PlanBuilder) resolveGbyExprs(ctx context.Context, p LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) (LogicalPlan, []expression.Expression, error) {
	b.curClause = groupByClause
	exprs := make([]expression.Expression, 0, len(gby.Items))
	resolver := &gbyResolver{
		ctx:             b.ctx,
		fields:          fields,
		schemaReplicant: p.Schema(),
		names:           p.OutputNames(),
	}
	for _, item := range gby.Items {
		resolver.inExpr = false
		resolver.exprDepth = 0
		resolver.isParam = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			return nil, nil, errors.Trace(resolver.err)
		}
		if !resolver.isParam {
			item.Expr = retExpr.(ast.ExprNode)
		}

		itemExpr := retExpr.(ast.ExprNode)
		expr, np, err := b.rewrite(ctx, itemExpr, p, nil, true)
		if err != nil {
			return nil, nil, err
		}

		exprs = append(exprs, expr)
		p = np
	}
	return p, exprs, nil
}

func (b *PlanBuilder) unfoldWildStar(p LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField, err error) {
	for i, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
			continue
		}
		if field.WildCard.Block.L == "" && i > 0 {
			return nil, ErrInvalidWildCard
		}
		dbName := field.WildCard.Schema
		tblName := field.WildCard.Block
		findTblNameInSchema := false
		for i, name := range p.OutputNames() {
			col := p.Schema().DeferredCausets[i]
			if col.IsHidden {
				continue
			}
			if (dbName.L == "" || dbName.L == name.DBName.L) &&
				(tblName.L == "" || tblName.L == name.TblName.L) &&
				col.ID != perceptron.ExtraHandleID {
				findTblNameInSchema = true
				colName := &ast.DeferredCausetNameExpr{
					Name: &ast.DeferredCausetName{
						Schema: name.DBName,
						Block:  name.TblName,
						Name:   name.DefCausName,
					}}
				colName.SetType(col.GetType())
				field := &ast.SelectField{Expr: colName}
				field.SetText(name.DefCausName.O)
				resultList = append(resultList, field)
			}
		}
		if !findTblNameInSchema {
			return nil, ErrBadBlock.GenWithStackByArgs(tblName)
		}
	}
	return resultList, nil
}

func (b *PlanBuilder) pushHintWithoutBlockWarning(hint *ast.BlockOptimizerHint) {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(0, &sb)
	if err := hint.Restore(ctx); err != nil {
		return
	}
	errMsg := fmt.Sprintf("Hint %s is inapplicable. Please specify the block names in the arguments.", sb.String())
	b.ctx.GetStochastikVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
}

func (b *PlanBuilder) pushBlockHints(hints []*ast.BlockOptimizerHint, nodeType utilhint.NodeType, currentLevel int) {
	hints = b.hintProcessor.GetCurrentStmtHints(hints, nodeType, currentLevel)
	var (
		sortMergeBlocks, INLJBlocks, INLHJBlocks, INLMJBlocks, hashJoinBlocks, BCBlocks, BCJPreferLocalBlocks []hintBlockInfo
		indexHintList, indexMergeHintList                                                                     []indexHintInfo
		tiflashBlocks, einsteindbBlocks                                                                       []hintBlockInfo
		aggHints                                                                                              aggHintInfo
		timeRangeHint                                                                                         ast.HintTimeRange
		limitHints                                                                                            limitHintInfo
	)
	for _, hint := range hints {
		// Set warning for the hint that requires the block name.
		switch hint.HintName.L {
		case MilevaDBMergeJoin, HintSMJ, MilevaDBIndexNestedLoopJoin, HintINLJ, HintINLHJ, HintINLMJ,
			MilevaDBHashJoin, HintHJ, HintUseIndex, HintIgnoreIndex, HintIndexMerge:
			if len(hint.Blocks) == 0 {
				b.pushHintWithoutBlockWarning(hint)
				continue
			}
		}

		switch hint.HintName.L {
		case MilevaDBMergeJoin, HintSMJ:
			sortMergeBlocks = append(sortMergeBlocks, blockNames2HintBlockInfo(b.ctx, hint.HintName.L, hint.Blocks, b.hintProcessor, nodeType, currentLevel)...)
		case MilevaDBBroadCastJoin, HintBCJ:
			BCBlocks = append(BCBlocks, blockNames2HintBlockInfo(b.ctx, hint.HintName.L, hint.Blocks, b.hintProcessor, nodeType, currentLevel)...)
		case HintBCJPreferLocal:
			BCJPreferLocalBlocks = append(BCJPreferLocalBlocks, blockNames2HintBlockInfo(b.ctx, hint.HintName.L, hint.Blocks, b.hintProcessor, nodeType, currentLevel)...)
		case MilevaDBIndexNestedLoopJoin, HintINLJ:
			INLJBlocks = append(INLJBlocks, blockNames2HintBlockInfo(b.ctx, hint.HintName.L, hint.Blocks, b.hintProcessor, nodeType, currentLevel)...)
		case HintINLHJ:
			INLHJBlocks = append(INLHJBlocks, blockNames2HintBlockInfo(b.ctx, hint.HintName.L, hint.Blocks, b.hintProcessor, nodeType, currentLevel)...)
		case HintINLMJ:
			INLMJBlocks = append(INLMJBlocks, blockNames2HintBlockInfo(b.ctx, hint.HintName.L, hint.Blocks, b.hintProcessor, nodeType, currentLevel)...)
		case MilevaDBHashJoin, HintHJ:
			hashJoinBlocks = append(hashJoinBlocks, blockNames2HintBlockInfo(b.ctx, hint.HintName.L, hint.Blocks, b.hintProcessor, nodeType, currentLevel)...)
		case HintHashAgg:
			aggHints.preferAggType |= preferHashAgg
		case HintStreamAgg:
			aggHints.preferAggType |= preferStreamAgg
		case HintAggToINTERLOCK:
			aggHints.preferAggToINTERLOCK = true
		case HintUseIndex:
			dbName := hint.Blocks[0].DBName
			if dbName.L == "" {
				dbName = perceptron.NewCIStr(b.ctx.GetStochastikVars().CurrentDB)
			}
			indexHintList = append(indexHintList, indexHintInfo{
				dbName:     dbName,
				tblName:    hint.Blocks[0].BlockName,
				partitions: hint.Blocks[0].PartitionList,
				indexHint: &ast.IndexHint{
					IndexNames:      hint.Indexes,
					HintType:        ast.HintUse,
					HintSINTERLOCKe: ast.HintForScan,
				},
			})
		case HintIgnoreIndex:
			dbName := hint.Blocks[0].DBName
			if dbName.L == "" {
				dbName = perceptron.NewCIStr(b.ctx.GetStochastikVars().CurrentDB)
			}
			indexHintList = append(indexHintList, indexHintInfo{
				dbName:     dbName,
				tblName:    hint.Blocks[0].BlockName,
				partitions: hint.Blocks[0].PartitionList,
				indexHint: &ast.IndexHint{
					IndexNames:      hint.Indexes,
					HintType:        ast.HintIgnore,
					HintSINTERLOCKe: ast.HintForScan,
				},
			})
		case HintReadFromStorage:
			switch hint.HintData.(perceptron.CIStr).L {
			case HintTiFlash:
				tiflashBlocks = append(tiflashBlocks, blockNames2HintBlockInfo(b.ctx, hint.HintName.L, hint.Blocks, b.hintProcessor, nodeType, currentLevel)...)
			case HintEinsteinDB:
				einsteindbBlocks = append(einsteindbBlocks, blockNames2HintBlockInfo(b.ctx, hint.HintName.L, hint.Blocks, b.hintProcessor, nodeType, currentLevel)...)
			}
		case HintIndexMerge:
			dbName := hint.Blocks[0].DBName
			if dbName.L == "" {
				dbName = perceptron.NewCIStr(b.ctx.GetStochastikVars().CurrentDB)
			}
			indexMergeHintList = append(indexMergeHintList, indexHintInfo{
				dbName:     dbName,
				tblName:    hint.Blocks[0].BlockName,
				partitions: hint.Blocks[0].PartitionList,
				indexHint: &ast.IndexHint{
					IndexNames:      hint.Indexes,
					HintType:        ast.HintUse,
					HintSINTERLOCKe: ast.HintForScan,
				},
			})
		case HintTimeRange:
			timeRangeHint = hint.HintData.(ast.HintTimeRange)
		case HintLimitToINTERLOCK:
			limitHints.preferLimitToINTERLOCK = true
		default:
			// ignore hints that not implemented
		}
	}
	b.blockHintInfo = append(b.blockHintInfo, blockHintInfo{
		sortMergeJoinBlocks:         sortMergeBlocks,
		broadcastJoinBlocks:         BCBlocks,
		broadcastJoinPreferredLocal: BCJPreferLocalBlocks,
		indexNestedLoopJoinBlocks:   indexNestedLoopJoinBlocks{INLJBlocks, INLHJBlocks, INLMJBlocks},
		hashJoinBlocks:              hashJoinBlocks,
		indexHintList:               indexHintList,
		tiflashBlocks:               tiflashBlocks,
		einsteindbBlocks:            einsteindbBlocks,
		aggHints:                    aggHints,
		indexMergeHintList:          indexMergeHintList,
		timeRangeHint:               timeRangeHint,
		limitHints:                  limitHints,
	})
}

func (b *PlanBuilder) popBlockHints() {
	hintInfo := b.blockHintInfo[len(b.blockHintInfo)-1]
	b.appendUnmatchedIndexHintWarning(hintInfo.indexHintList, false)
	b.appendUnmatchedIndexHintWarning(hintInfo.indexMergeHintList, true)
	b.appendUnmatchedJoinHintWarning(HintINLJ, MilevaDBIndexNestedLoopJoin, hintInfo.indexNestedLoopJoinBlocks.inljBlocks)
	b.appendUnmatchedJoinHintWarning(HintINLHJ, "", hintInfo.indexNestedLoopJoinBlocks.inlhjBlocks)
	b.appendUnmatchedJoinHintWarning(HintINLMJ, "", hintInfo.indexNestedLoopJoinBlocks.inlmjBlocks)
	b.appendUnmatchedJoinHintWarning(HintSMJ, MilevaDBMergeJoin, hintInfo.sortMergeJoinBlocks)
	b.appendUnmatchedJoinHintWarning(HintBCJ, MilevaDBBroadCastJoin, hintInfo.broadcastJoinBlocks)
	b.appendUnmatchedJoinHintWarning(HintBCJPreferLocal, "", hintInfo.broadcastJoinPreferredLocal)
	b.appendUnmatchedJoinHintWarning(HintHJ, MilevaDBHashJoin, hintInfo.hashJoinBlocks)
	b.appendUnmatchedStorageHintWarning(hintInfo.tiflashBlocks, hintInfo.einsteindbBlocks)
	b.blockHintInfo = b.blockHintInfo[:len(b.blockHintInfo)-1]
}

func (b *PlanBuilder) appendUnmatchedIndexHintWarning(indexHints []indexHintInfo, usedForIndexMerge bool) {
	for _, hint := range indexHints {
		if !hint.matched {
			var hintTypeString string
			if usedForIndexMerge {
				hintTypeString = "use_index_merge"
			} else {
				hintTypeString = hint.hintTypeString()
			}
			errMsg := fmt.Sprintf("%s(%s) is inapplicable, check whether the block(%s.%s) exists",
				hintTypeString,
				hint.indexString(),
				hint.dbName,
				hint.tblName,
			)
			b.ctx.GetStochastikVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
		}
	}
}

func (b *PlanBuilder) appendUnmatchedJoinHintWarning(joinType string, joinTypeAlias string, hintBlocks []hintBlockInfo) {
	unMatchedBlocks := extractUnmatchedBlocks(hintBlocks)
	if len(unMatchedBlocks) == 0 {
		return
	}
	if len(joinTypeAlias) != 0 {
		joinTypeAlias = fmt.Sprintf(" or %s", restore2JoinHint(joinTypeAlias, hintBlocks))
	}

	errMsg := fmt.Sprintf("There are no matching block names for (%s) in optimizer hint %s%s. Maybe you can use the block alias name",
		strings.Join(unMatchedBlocks, ", "), restore2JoinHint(joinType, hintBlocks), joinTypeAlias)
	b.ctx.GetStochastikVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
}

func (b *PlanBuilder) appendUnmatchedStorageHintWarning(tiflashBlocks, einsteindbBlocks []hintBlockInfo) {
	unMatchedTiFlashBlocks := extractUnmatchedBlocks(tiflashBlocks)
	unMatchedEinsteinDBBlocks := extractUnmatchedBlocks(einsteindbBlocks)
	if len(unMatchedTiFlashBlocks)+len(unMatchedEinsteinDBBlocks) == 0 {
		return
	}
	errMsg := fmt.Sprintf("There are no matching block names for (%s) in optimizer hint %s. Maybe you can use the block alias name",
		strings.Join(append(unMatchedTiFlashBlocks, unMatchedEinsteinDBBlocks...), ", "),
		restore2StorageHint(tiflashBlocks, einsteindbBlocks))
	b.ctx.GetStochastikVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
}

// BlockHints returns the *blockHintInfo of PlanBuilder.
func (b *PlanBuilder) BlockHints() *blockHintInfo {
	if len(b.blockHintInfo) == 0 {
		return nil
	}
	return &(b.blockHintInfo[len(b.blockHintInfo)-1])
}

func (b *PlanBuilder) buildSelect(ctx context.Context, sel *ast.SelectStmt) (p LogicalPlan, err error) {
	b.pushSelectOffset(sel.QueryBlockOffset)
	b.pushBlockHints(sel.BlockHints, utilhint.TypeSelect, sel.QueryBlockOffset)
	defer func() {
		b.popSelectOffset()
		// block hints are only visible in the current SELECT statement.
		b.popBlockHints()
	}()
	enableNoopFuncs := b.ctx.GetStochastikVars().EnableNoopFuncs
	if sel.SelectStmtOpts != nil {
		if sel.SelectStmtOpts.CalcFoundRows && !enableNoopFuncs {
			err = expression.ErrFunctionsNoopImpl.GenWithStackByArgs("ALLEGROSQL_CALC_FOUND_ROWS")
			return nil, err
		}
		origin := b.inStraightJoin
		b.inStraightJoin = sel.SelectStmtOpts.StraightJoin
		defer func() { b.inStraightJoin = origin }()
	}

	var (
		aggFuncs                      []*ast.AggregateFuncExpr
		havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
		windowAggMap                  map[*ast.AggregateFuncExpr]int
		gbyDefCauss                   []expression.Expression
	)

	if sel.From != nil {
		p, err = b.buildResultSetNode(ctx, sel.From.BlockRefs)
		if err != nil {
			return nil, err
		}
	} else {
		p = b.buildBlockDual()
	}

	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return nil, err
	}
	if b.capFlag&canExpandAST != 0 {
		originalFields = sel.Fields.Fields
	}

	if sel.GroupBy != nil {
		p, gbyDefCauss, err = b.resolveGbyExprs(ctx, p, sel.GroupBy, sel.Fields.Fields)
		if err != nil {
			return nil, err
		}
	}

	if b.ctx.GetStochastikVars().ALLEGROSQLMode.HasOnlyFullGroupBy() && sel.From != nil {
		err = b.checkOnlyFullGroupBy(p, sel)
		if err != nil {
			return nil, err
		}
	}

	hasWindowFuncField := b.detectSelectWindow(sel)
	if hasWindowFuncField {
		windowAggMap, err = b.resolveWindowFunction(sel, p)
		if err != nil {
			return nil, err
		}
	}
	// We must resolve having and order by clause before build projection,
	// because when the query is "select a+1 as b from t having sum(b) < 0", we must replace sum(b) to sum(a+1),
	// which only can be done before building projection and extracting Agg functions.
	havingMap, orderMap, err = b.resolveHavingAndOrderBy(sel, p)
	if err != nil {
		return nil, err
	}

	// b.allNames will be used in evalDefaultExpr(). Default function is special because it needs to find the
	// corresponding column name, but does not need the value in the column.
	// For example, select a from t order by default(b), the column b will not be in select fields. Also because
	// buildSort is after buildProjection, so we need get OutputNames before BuildProjection and causetstore in allNames.
	// Otherwise, we will get select fields instead of all OutputNames, so that we can't find the column b in the
	// above example.
	b.allNames = append(b.allNames, p.OutputNames())
	defer func() { b.allNames = b.allNames[:len(b.allNames)-1] }()

	if sel.Where != nil {
		p, err = b.buildSelection(ctx, p, sel.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if sel.LockInfo != nil && sel.LockInfo.LockType != ast.SelectLockNone {
		if sel.LockInfo.LockType == ast.SelectLockInShareMode && !enableNoopFuncs {
			err = expression.ErrFunctionsNoopImpl.GenWithStackByArgs("LOCK IN SHARE MODE")
			return nil, err
		}
		p = b.buildSelectLock(p, sel.LockInfo)
	}
	b.handleHelper.popMap()
	b.handleHelper.pushMap(nil)

	hasAgg := b.detectSelectAgg(sel)
	if hasAgg {
		aggFuncs, totalMap = b.extractAggFuncs(sel.Fields.Fields)
		var aggIndexMap map[int]int
		p, aggIndexMap, err = b.builPosetDaggregation(ctx, p, aggFuncs, gbyDefCauss)
		if err != nil {
			return nil, err
		}
		for k, v := range totalMap {
			totalMap[k] = aggIndexMap[v]
		}
	}

	var oldLen int
	// According to https://dev.allegrosql.com/doc/refman/8.0/en/window-functions-usage.html,
	// we can only process window functions after having clause, so `considerWindow` is false now.
	p, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, totalMap, nil, false, sel.OrderBy != nil)
	if err != nil {
		return nil, err
	}

	if sel.Having != nil {
		b.curClause = havingClause
		p, err = b.buildSelection(ctx, p, sel.Having.Expr, havingMap)
		if err != nil {
			return nil, err
		}
	}

	b.windowSpecs, err = buildWindowSpecs(sel.WindowSpecs)
	if err != nil {
		return nil, err
	}

	var windowMapper map[*ast.WindowFuncExpr]int
	if hasWindowFuncField {
		windowFuncs := extractWindowFuncs(sel.Fields.Fields)
		// we need to check the func args first before we check the window spec
		err := b.checkWindowFuncArgs(ctx, p, windowFuncs, windowAggMap)
		if err != nil {
			return nil, err
		}
		groupedFuncs, err := b.groupWindowFuncs(windowFuncs)
		if err != nil {
			return nil, err
		}
		p, windowMapper, err = b.buildWindowFunctions(ctx, p, groupedFuncs, windowAggMap)
		if err != nil {
			return nil, err
		}
		// Now we build the window function fields.
		p, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, windowAggMap, windowMapper, true, false)
		if err != nil {
			return nil, err
		}
	}

	if sel.Distinct {
		p, err = b.buildDistinct(p, oldLen)
		if err != nil {
			return nil, err
		}
	}

	if sel.OrderBy != nil {
		p, err = b.buildSort(ctx, p, sel.OrderBy.Items, orderMap, windowMapper)
		if err != nil {
			return nil, err
		}
	}

	if sel.Limit != nil {
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			return nil, err
		}
	}

	sel.Fields.Fields = originalFields
	if oldLen != p.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.DeferredCauset2Exprs(p.Schema().DeferredCausets[:oldLen])}.Init(b.ctx, b.getSelectOffset())
		proj.SetChildren(p)
		schemaReplicant := expression.NewSchema(p.Schema().Clone().DeferredCausets[:oldLen]...)
		for _, col := range schemaReplicant.DeferredCausets {
			col.UniqueID = b.ctx.GetStochastikVars().AllocPlanDeferredCausetID()
		}
		proj.names = p.OutputNames()[:oldLen]
		proj.SetSchema(schemaReplicant)
		return proj, nil
	}

	return p, nil
}

func (b *PlanBuilder) buildBlockDual() *LogicalBlockDual {
	b.handleHelper.pushMap(nil)
	return LogicalBlockDual{RowCount: 1}.Init(b.ctx, b.getSelectOffset())
}

func (ds *DataSource) newExtraHandleSchemaDefCaus() *expression.DeferredCauset {
	return &expression.DeferredCauset{
		RetType:  types.NewFieldType(allegrosql.TypeLonglong),
		UniqueID: ds.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
		ID:       perceptron.ExtraHandleID,
		OrigName: fmt.Sprintf("%v.%v.%v", ds.DBName, ds.blockInfo.Name, perceptron.ExtraHandleName),
	}
}

// getStatsBlock gets statistics information for a block specified by "blockID".
// A pseudo statistics block is returned in any of the following scenario:
// 1. milevadb-server started and statistics handle has not been initialized.
// 2. block event count from statistics is zero.
// 3. statistics is outdated.
func getStatsBlock(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, pid int64) *statistics.Block {
	statsHandle := petri.GetPetri(ctx).StatsHandle()

	// 1. milevadb-server started and statistics handle has not been initialized.
	if statsHandle == nil {
		return statistics.PseudoBlock(tblInfo)
	}

	var statsTbl *statistics.Block
	if pid != tblInfo.ID {
		statsTbl = statsHandle.GetPartitionStats(tblInfo, pid)
	} else {
		statsTbl = statsHandle.GetBlockStats(tblInfo)
	}

	// 2. block event count from statistics is zero.
	if statsTbl.Count == 0 {
		return statistics.PseudoBlock(tblInfo)
	}

	// 3. statistics is outdated.
	if statsTbl.IsOutdated() {
		tbl := *statsTbl
		tbl.Pseudo = true
		statsTbl = &tbl
		metrics.PseudoEstimation.Inc()
	}
	return statsTbl
}

func (b *PlanBuilder) buildDataSource(ctx context.Context, tn *ast.BlockName, asName *perceptron.CIStr) (LogicalPlan, error) {
	dbName := tn.Schema
	stochastikVars := b.ctx.GetStochastikVars()
	if dbName.L == "" {
		dbName = perceptron.NewCIStr(stochastikVars.CurrentDB)
	}

	tbl, err := b.is.BlockByName(dbName, tn.Name)
	if err != nil {
		return nil, err
	}

	blockInfo := tbl.Meta()
	var authErr error
	if stochastikVars.User != nil {
		authErr = ErrBlockaccessDenied.FastGenByArgs("SELECT", stochastikVars.User.AuthUsername, stochastikVars.User.AuthHostname, blockInfo.Name.L)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SelectPriv, dbName.L, blockInfo.Name.L, "", authErr)

	if tbl.Type().IsVirtualBlock() {
		return b.buildMemBlock(ctx, dbName, blockInfo)
	}

	if blockInfo.IsView() {
		if b.capFlag&collectUnderlyingViewName != 0 {
			b.underlyingViewNames.Insert(dbName.L + "." + tn.Name.L)
		}
		return b.BuildDataSourceFromView(ctx, dbName, blockInfo)
	}

	if blockInfo.GetPartitionInfo() != nil {
		// Use the new partition implementation, clean up the code here when it's full implemented.
		if !b.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
			b.optFlag = b.optFlag | flagPartitionProcessor
		}

		pt := tbl.(block.PartitionedBlock)
		// check partition by name.
		if len(tn.PartitionNames) > 0 {
			pids := make(map[int64]struct{}, len(tn.PartitionNames))
			for _, name := range tn.PartitionNames {
				pid, err := blocks.FindPartitionByName(blockInfo, name.L)
				if err != nil {
					return nil, err
				}
				pids[pid] = struct{}{}
			}
			pt = blocks.NewPartitionBlockithGivenSets(pt, pids)
		}
		b.partitionedBlock = append(b.partitionedBlock, pt)
	} else if len(tn.PartitionNames) != 0 {
		return nil, ErrPartitionClauseOnNonpartitioned
	}

	tblName := *asName
	if tblName.L == "" {
		tblName = tn.Name
	}
	possiblePaths, err := getPossibleAccessPaths(b.ctx, b.BlockHints(), tn.IndexHints, tbl, dbName, tblName)
	if err != nil {
		return nil, err
	}
	possiblePaths, err = filterPathByIsolationRead(b.ctx, possiblePaths, dbName)
	if err != nil {
		return nil, err
	}

	// Try to substitute generate column only if there is an index on generate column.
	for _, index := range blockInfo.Indices {
		if index.State != perceptron.StatePublic {
			continue
		}
		for _, indexDefCaus := range index.DeferredCausets {
			colInfo := tbl.DefCauss()[indexDefCaus.Offset]
			if colInfo.IsGenerated() && !colInfo.GeneratedStored {
				b.optFlag |= flagGcSubstitute
				break
			}
		}
	}

	var columns []*block.DeferredCauset
	if b.inUFIDelateStmt {
		// create block t(a int, b int).
		// Imagine that, There are 2 MilevaDB instances in the cluster, name A, B. We add a column `c` to block t in the MilevaDB cluster.
		// One of the MilevaDB, A, the column type in its schemareplicant is changed to public. And in the other MilevaDB, the column type is
		// still StateWriteReorganization.
		// MilevaDB A: insert into t values(1, 2, 3);
		// MilevaDB B: uFIDelate t set a = 2 where b = 2;
		// If we use tbl.DefCauss() here, the uFIDelate statement, will ignore the col `c`, and the data `3` will lost.
		columns = tbl.WriblockDefCauss()
	} else if b.inDeleteStmt {
		// All hidden columns are needed because we need to delete the expression index that consists of hidden columns.
		columns = tbl.FullHiddenDefCaussAndVisibleDefCauss()
	} else {
		columns = tbl.DefCauss()
	}
	var statisticBlock *statistics.Block
	if _, ok := tbl.(block.PartitionedBlock); !ok {
		statisticBlock = getStatsBlock(b.ctx, tbl.Meta(), tbl.Meta().ID)
	}

	// extract the IndexMergeHint
	var indexMergeHints []indexHintInfo
	if hints := b.BlockHints(); hints != nil {
		for i, hint := range hints.indexMergeHintList {
			if hint.tblName.L == tblName.L && hint.dbName.L == dbName.L {
				hints.indexMergeHintList[i].matched = true
				// check whether the index names in IndexMergeHint are valid.
				invalidIdxNames := make([]string, 0, len(hint.indexHint.IndexNames))
				for _, idxName := range hint.indexHint.IndexNames {
					hasIdxName := false
					for _, path := range possiblePaths {
						if path.IsBlockPath() {
							if idxName.L == "primary" {
								hasIdxName = true
								break
							}
							continue
						}
						if idxName.L == path.Index.Name.L {
							hasIdxName = true
							break
						}
					}
					if !hasIdxName {
						invalidIdxNames = append(invalidIdxNames, idxName.String())
					}
				}
				if len(invalidIdxNames) == 0 {
					indexMergeHints = append(indexMergeHints, hint)
				} else {
					// Append warning if there are invalid index names.
					errMsg := fmt.Sprintf("use_index_merge(%s) is inapplicable, check whether the indexes (%s) "+
						"exist, or the indexes are conflicted with use_index/ignore_index hints.",
						hint.indexString(), strings.Join(invalidIdxNames, ", "))
					b.ctx.GetStochastikVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
				}
			}
		}
	}
	ds := DataSource{
		DBName:              dbName,
		BlockAsName:         asName,
		block:               tbl,
		blockInfo:           blockInfo,
		statisticBlock:      statisticBlock,
		astIndexHints:       tn.IndexHints,
		IndexHints:          b.BlockHints().indexHintList,
		indexMergeHints:     indexMergeHints,
		possibleAccessPaths: possiblePaths,
		DeferredCausets:     make([]*perceptron.DeferredCausetInfo, 0, len(columns)),
		partitionNames:      tn.PartitionNames,
		TblDefCauss:         make([]*expression.DeferredCauset, 0, len(columns)),
		preferPartitions:    make(map[int][]perceptron.CIStr),
	}.Init(b.ctx, b.getSelectOffset())
	var handleDefCauss HandleDefCauss
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(columns))...)
	names := make([]*types.FieldName, 0, len(columns))
	for i, col := range columns {
		ds.DeferredCausets = append(ds.DeferredCausets, col.ToInfo())
		names = append(names, &types.FieldName{
			DBName:          dbName,
			TblName:         blockInfo.Name,
			DefCausName:     col.Name,
			OrigTblName:     blockInfo.Name,
			OrigDefCausName: col.Name,
			Hidden:          col.Hidden,
		})
		newDefCaus := &expression.DeferredCauset{
			UniqueID: stochastikVars.AllocPlanDeferredCausetID(),
			ID:       col.ID,
			RetType:  &col.FieldType,
			OrigName: names[i].String(),
			IsHidden: col.Hidden,
		}
		if col.IsPKHandleDeferredCauset(blockInfo) {
			handleDefCauss = &IntHandleDefCauss{col: newDefCaus}
		}
		schemaReplicant.Append(newDefCaus)
		ds.TblDefCauss = append(ds.TblDefCauss, newDefCaus)
	}
	// We append an extra handle column to the schemaReplicant when the handle
	// column is not the primary key of "ds".
	if handleDefCauss == nil {
		if blockInfo.IsCommonHandle {
			primaryIdx := blocks.FindPrimaryIndex(blockInfo)
			handleDefCauss = NewCommonHandleDefCauss(b.ctx.GetStochastikVars().StmtCtx, blockInfo, primaryIdx, ds.TblDefCauss)
		} else {
			extraDefCaus := ds.newExtraHandleSchemaDefCaus()
			handleDefCauss = &IntHandleDefCauss{col: extraDefCaus}
			ds.DeferredCausets = append(ds.DeferredCausets, perceptron.NewExtraHandleDefCausInfo())
			schemaReplicant.Append(extraDefCaus)
			names = append(names, &types.FieldName{
				DBName:          dbName,
				TblName:         blockInfo.Name,
				DefCausName:     perceptron.ExtraHandleName,
				OrigDefCausName: perceptron.ExtraHandleName,
			})
			ds.TblDefCauss = append(ds.TblDefCauss, extraDefCaus)
		}
	}
	ds.handleDefCauss = handleDefCauss
	handleMap := make(map[int64][]HandleDefCauss)
	handleMap[blockInfo.ID] = []HandleDefCauss{handleDefCauss}
	b.handleHelper.pushMap(handleMap)
	ds.SetSchema(schemaReplicant)
	ds.names = names
	ds.setPreferredStoreType(b.BlockHints())

	// Init commonHandleDefCauss and commonHandleLens for data source.
	if blockInfo.IsCommonHandle {
		ds.commonHandleDefCauss, ds.commonHandleLens = expression.IndexInfo2DefCauss(ds.DeferredCausets, ds.schemaReplicant.DeferredCausets, blocks.FindPrimaryIndex(blockInfo))
	}
	// Init FullIdxDefCauss, FullIdxDefCausLens for accessPaths.
	for _, path := range ds.possibleAccessPaths {
		if !path.IsIntHandlePath {
			path.FullIdxDefCauss, path.FullIdxDefCausLens = expression.IndexInfo2DefCauss(ds.DeferredCausets, ds.schemaReplicant.DeferredCausets, path.Index)
		}
	}

	var result LogicalPlan = ds
	dirty := blockHasDirtyContent(b.ctx, blockInfo)
	if dirty {
		us := LogicalUnionScan{handleDefCauss: handleDefCauss}.Init(b.ctx, b.getSelectOffset())
		us.SetChildren(ds)
		result = us
	}
	if stochastikVars.StmtCtx.TblInfo2UnionScan == nil {
		stochastikVars.StmtCtx.TblInfo2UnionScan = make(map[*perceptron.BlockInfo]bool)
	}
	stochastikVars.StmtCtx.TblInfo2UnionScan[blockInfo] = dirty

	for i, colExpr := range ds.Schema().DeferredCausets {
		var expr expression.Expression
		if i < len(columns) {
			if columns[i].IsGenerated() && !columns[i].GeneratedStored {
				var err error
				expr, _, err = b.rewrite(ctx, columns[i].GeneratedExpr, ds, nil, true)
				if err != nil {
					return nil, err
				}
				colExpr.VirtualExpr = expr.Clone()
			}
		}
	}

	return result, nil
}

func (b *PlanBuilder) timeRangeForSummaryBlock() QueryTimeRange {
	const defaultSummaryDuration = 30 * time.Minute
	hints := b.BlockHints()
	// User doesn't use TIME_RANGE hint
	if hints == nil || (hints.timeRangeHint.From == "" && hints.timeRangeHint.To == "") {
		to := time.Now()
		from := to.Add(-defaultSummaryDuration)
		return QueryTimeRange{From: from, To: to}
	}

	// Parse time specified by user via TIM_RANGE hint
	parse := func(s string) (time.Time, bool) {
		t, err := time.ParseInLocation(MetricBlockTimeFormat, s, time.Local)
		if err != nil {
			b.ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
		}
		return t, err == nil
	}
	from, fromValid := parse(hints.timeRangeHint.From)
	to, toValid := parse(hints.timeRangeHint.To)
	switch {
	case !fromValid && !toValid:
		to = time.Now()
		from = to.Add(-defaultSummaryDuration)
	case fromValid && !toValid:
		to = from.Add(defaultSummaryDuration)
	case !fromValid && toValid:
		from = to.Add(-defaultSummaryDuration)
	}

	return QueryTimeRange{From: from, To: to}
}

func (b *PlanBuilder) buildMemBlock(_ context.Context, dbName perceptron.CIStr, blockInfo *perceptron.BlockInfo) (LogicalPlan, error) {
	// We can use the `blockInfo.DeferredCausets` directly because the memory block has
	// a sblock schemaReplicant and there is no online DBS on the memory block.
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(blockInfo.DeferredCausets))...)
	names := make([]*types.FieldName, 0, len(blockInfo.DeferredCausets))
	var handleDefCauss HandleDefCauss
	for _, col := range blockInfo.DeferredCausets {
		names = append(names, &types.FieldName{
			DBName:          dbName,
			TblName:         blockInfo.Name,
			DefCausName:     col.Name,
			OrigTblName:     blockInfo.Name,
			OrigDefCausName: col.Name,
		})
		// NOTE: Rewrite the expression if memory block supports generated columns in the future
		newDefCaus := &expression.DeferredCauset{
			UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			ID:       col.ID,
			RetType:  &col.FieldType,
		}
		if blockInfo.PKIsHandle && allegrosql.HasPriKeyFlag(col.Flag) {
			handleDefCauss = &IntHandleDefCauss{col: newDefCaus}
		}
		schemaReplicant.Append(newDefCaus)
	}

	if handleDefCauss != nil {
		handleMap := make(map[int64][]HandleDefCauss)
		handleMap[blockInfo.ID] = []HandleDefCauss{handleDefCauss}
		b.handleHelper.pushMap(handleMap)
	} else {
		b.handleHelper.pushMap(nil)
	}

	// NOTE: Add a `LogicalUnionScan` if we support uFIDelate memory block in the future
	p := LogicalMemBlock{
		DBName:    dbName,
		BlockInfo: blockInfo,
	}.Init(b.ctx, b.getSelectOffset())
	p.SetSchema(schemaReplicant)
	p.names = names

	// Some memory blocks can receive some predicates
	switch dbName.L {
	case util2.MetricSchemaName.L:
		p.Extractor = newMetricBlockExtractor()
	case util2.InformationSchemaName.L:
		switch strings.ToUpper(blockInfo.Name.O) {
		case schemareplicant.BlockClusterConfig, schemareplicant.BlockClusterLoad, schemareplicant.BlockClusterHardware, schemareplicant.BlockClusterSystemInfo:
			p.Extractor = &ClusterBlockExtractor{}
		case schemareplicant.BlockClusterLog:
			p.Extractor = &ClusterLogBlockExtractor{}
		case schemareplicant.BlockInspectionResult:
			p.Extractor = &InspectionResultBlockExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryBlock()
		case schemareplicant.BlockInspectionSummary:
			p.Extractor = &InspectionSummaryBlockExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryBlock()
		case schemareplicant.BlockInspectionMemrules:
			p.Extractor = &InspectionMemruleBlockExtractor{}
		case schemareplicant.BlockMetricSummary, schemareplicant.BlockMetricSummaryByLabel:
			p.Extractor = &MetricSummaryBlockExtractor{}
			p.QueryTimeRange = b.timeRangeForSummaryBlock()
		case schemareplicant.BlockSlowQuery:
			p.Extractor = &SlowQueryExtractor{}
		case schemareplicant.BlockStorageStats:
			p.Extractor = &BlockStorageStatsExtractor{}
		case schemareplicant.BlockTiFlashBlocks, schemareplicant.BlockTiFlashSegments:
			p.Extractor = &TiFlashSystemBlockExtractor{}
		}
	}
	return p, nil
}

// BuildDataSourceFromView is used to build LogicalPlan from view
func (b *PlanBuilder) BuildDataSourceFromView(ctx context.Context, dbName perceptron.CIStr, blockInfo *perceptron.BlockInfo) (LogicalPlan, error) {
	charset, collation := b.ctx.GetStochastikVars().GetCharsetInfo()
	viewberolinaAllegroSQL := berolinaAllegroSQL.New()
	viewberolinaAllegroSQL.EnableWindowFunc(b.ctx.GetStochastikVars().EnableWindowFunction)
	selectNode, err := viewberolinaAllegroSQL.ParseOneStmt(blockInfo.View.SelectStmt, charset, collation)
	if err != nil {
		return nil, err
	}
	originalVisitInfo := b.visitInfo
	b.visitInfo = make([]visitInfo, 0)
	selectLogicalPlan, err := b.Build(ctx, selectNode)
	if err != nil {
		err = ErrViewInvalid.GenWithStackByArgs(dbName.O, blockInfo.Name.O)
		return nil, err
	}

	if blockInfo.View.Security == perceptron.SecurityDefiner {
		if pm := privilege.GetPrivilegeManager(b.ctx); pm != nil {
			for _, v := range b.visitInfo {
				if !pm.RequestVerificationWithUser(v.EDB, v.block, v.column, v.privilege, blockInfo.View.Definer) {
					return nil, ErrViewInvalid.GenWithStackByArgs(dbName.O, blockInfo.Name.O)
				}
			}
		}
		b.visitInfo = b.visitInfo[:0]
	}
	b.visitInfo = append(originalVisitInfo, b.visitInfo...)

	if b.ctx.GetStochastikVars().StmtCtx.InExplainStmt {
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.ShowViewPriv, dbName.L, blockInfo.Name.L, "", ErrViewNoExplain)
	}

	if len(blockInfo.DeferredCausets) != selectLogicalPlan.Schema().Len() {
		return nil, ErrViewInvalid.GenWithStackByArgs(dbName.O, blockInfo.Name.O)
	}

	return b.buildProjUponView(ctx, dbName, blockInfo, selectLogicalPlan)
}

func (b *PlanBuilder) buildProjUponView(ctx context.Context, dbName perceptron.CIStr, blockInfo *perceptron.BlockInfo, selectLogicalPlan Plan) (LogicalPlan, error) {
	columnInfo := blockInfo.DefCauss()
	defcaus := selectLogicalPlan.Schema().Clone().DeferredCausets
	outputNamesOfUnderlyingSelect := selectLogicalPlan.OutputNames().Shallow()
	// In the old version of VIEW implementation, blockInfo.View.DefCauss is used to
	// causetstore the origin columns' names of the underlying SelectStmt used when
	// creating the view.
	if blockInfo.View.DefCauss != nil {
		defcaus = defcaus[:0]
		outputNamesOfUnderlyingSelect = outputNamesOfUnderlyingSelect[:0]
		for _, info := range columnInfo {
			idx := expression.FindFieldNameIdxByDefCausName(selectLogicalPlan.OutputNames(), info.Name.L)
			if idx == -1 {
				return nil, ErrViewInvalid.GenWithStackByArgs(dbName.O, blockInfo.Name.O)
			}
			defcaus = append(defcaus, selectLogicalPlan.Schema().DeferredCausets[idx])
			outputNamesOfUnderlyingSelect = append(outputNamesOfUnderlyingSelect, selectLogicalPlan.OutputNames()[idx])
		}
	}

	projSchema := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(blockInfo.DeferredCausets))...)
	projExprs := make([]expression.Expression, 0, len(blockInfo.DeferredCausets))
	projNames := make(types.NameSlice, 0, len(blockInfo.DeferredCausets))
	for i, name := range outputNamesOfUnderlyingSelect {
		origDefCausName := name.DefCausName
		if blockInfo.View.DefCauss != nil {
			origDefCausName = blockInfo.View.DefCauss[i]
		}
		projNames = append(projNames, &types.FieldName{
			// TblName is the of view instead of the name of the underlying block.
			TblName:         blockInfo.Name,
			OrigTblName:     name.OrigTblName,
			DefCausName:     columnInfo[i].Name,
			OrigDefCausName: origDefCausName,
			DBName:          dbName,
		})
		projSchema.Append(&expression.DeferredCauset{
			UniqueID: defcaus[i].UniqueID,
			RetType:  defcaus[i].GetType(),
		})
		projExprs = append(projExprs, defcaus[i])
	}
	projUponView := LogicalProjection{Exprs: projExprs}.Init(b.ctx, b.getSelectOffset())
	projUponView.names = projNames
	projUponView.SetChildren(selectLogicalPlan.(LogicalPlan))
	projUponView.SetSchema(projSchema)
	return projUponView, nil
}

// buildApplyWithJoinType builds apply plan with outerPlan and innerPlan, which apply join with particular join type for
// every event from outerPlan and the whole innerPlan.
func (b *PlanBuilder) buildApplyWithJoinType(outerPlan, innerPlan LogicalPlan, tp JoinType) LogicalPlan {
	b.optFlag = b.optFlag | flagPredicatePushDown | flagBuildKeyInfo | flagDecorrelate
	ap := LogicalApply{LogicalJoin: LogicalJoin{JoinType: tp}}.Init(b.ctx, b.getSelectOffset())
	ap.SetChildren(outerPlan, innerPlan)
	ap.names = make([]*types.FieldName, outerPlan.Schema().Len()+innerPlan.Schema().Len())
	INTERLOCKy(ap.names, outerPlan.OutputNames())
	ap.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
	// Note that, tp can only be LeftOuterJoin or InnerJoin, so we don't consider other outer joins.
	if tp == LeftOuterJoin {
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		resetNotNullFlag(ap.schemaReplicant, outerPlan.Schema().Len(), ap.schemaReplicant.Len())
	}
	for i := outerPlan.Schema().Len(); i < ap.Schema().Len(); i++ {
		ap.names[i] = types.EmptyName
	}
	return ap
}

// buildSemiApply builds apply plan with outerPlan and innerPlan, which apply semi-join for every event from outerPlan and the whole innerPlan.
func (b *PlanBuilder) buildSemiApply(outerPlan, innerPlan LogicalPlan, condition []expression.Expression, asScalar, not bool) (LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPredicatePushDown | flagBuildKeyInfo | flagDecorrelate

	join, err := b.buildSemiJoin(outerPlan, innerPlan, condition, asScalar, not)
	if err != nil {
		return nil, err
	}

	ap := &LogicalApply{LogicalJoin: *join}
	ap.tp = plancodec.TypeApply
	ap.self = ap
	return ap, nil
}

func (b *PlanBuilder) buildMaxOneRow(p LogicalPlan) LogicalPlan {
	maxOneRow := LogicalMaxOneRow{}.Init(b.ctx, b.getSelectOffset())
	maxOneRow.SetChildren(p)
	return maxOneRow
}

func (b *PlanBuilder) buildSemiJoin(outerPlan, innerPlan LogicalPlan, onCondition []expression.Expression, asScalar bool, not bool) (*LogicalJoin, error) {
	joinPlan := LogicalJoin{}.Init(b.ctx, b.getSelectOffset())
	for i, expr := range onCondition {
		onCondition[i] = expr.Decorrelate(outerPlan.Schema())
	}
	joinPlan.SetChildren(outerPlan, innerPlan)
	joinPlan.AttachOnConds(onCondition)
	joinPlan.names = make([]*types.FieldName, outerPlan.Schema().Len(), outerPlan.Schema().Len()+innerPlan.Schema().Len()+1)
	INTERLOCKy(joinPlan.names, outerPlan.OutputNames())
	if asScalar {
		newSchema := outerPlan.Schema().Clone()
		newSchema.Append(&expression.DeferredCauset{
			RetType:  types.NewFieldType(allegrosql.TypeTiny),
			UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
		})
		joinPlan.names = append(joinPlan.names, types.EmptyName)
		joinPlan.SetSchema(newSchema)
		if not {
			joinPlan.JoinType = AntiLeftOuterSemiJoin
		} else {
			joinPlan.JoinType = LeftOuterSemiJoin
		}
	} else {
		joinPlan.SetSchema(outerPlan.Schema().Clone())
		if not {
			joinPlan.JoinType = AntiSemiJoin
		} else {
			joinPlan.JoinType = SemiJoin
		}
	}
	// Apply forces to choose hash join currently, so don't worry the hints will take effect if the semi join is in one apply.
	if b.BlockHints() != nil {
		outerAlias := extractBlockAlias(outerPlan, joinPlan.blockOffset)
		innerAlias := extractBlockAlias(innerPlan, joinPlan.blockOffset)
		if b.BlockHints().ifPreferMergeJoin(outerAlias, innerAlias) {
			joinPlan.preferJoinType |= preferMergeJoin
		}
		if b.BlockHints().ifPreferHashJoin(outerAlias, innerAlias) {
			joinPlan.preferJoinType |= preferHashJoin
		}
		if b.BlockHints().ifPreferINLJ(innerAlias) {
			joinPlan.preferJoinType = preferRightAsINLJInner
		}
		if b.BlockHints().ifPreferINLHJ(innerAlias) {
			joinPlan.preferJoinType = preferRightAsINLHJInner
		}
		if b.BlockHints().ifPreferINLMJ(innerAlias) {
			joinPlan.preferJoinType = preferRightAsINLMJInner
		}
		// If there're multiple join hints, they're conflict.
		if bits.OnesCount(joinPlan.preferJoinType) > 1 {
			return nil, errors.New("Join hints are conflict, you can only specify one type of join")
		}
	}
	return joinPlan, nil
}

func getBlockOffset(names []*types.FieldName, handleName *types.FieldName) (int, error) {
	for i, name := range names {
		if name.DBName.L == handleName.DBName.L && name.TblName.L == handleName.TblName.L {
			return i, nil
		}
	}
	return -1, errors.Errorf("Couldn't get column information when do uFIDelate/delete")
}

// TblDefCausPosInfo represents an mapper from column index to handle index.
type TblDefCausPosInfo struct {
	TblID int64
	// Start and End represent the ordinal range [Start, End) of the consecutive columns.
	Start, End int
	// HandleOrdinal represents the ordinal of the handle column.
	HandleDefCauss HandleDefCauss
	IsCommonHandle bool // TODO: fix redesign uFIDelate join block and remove me!
}

// TblDefCausPosInfoSlice attaches the methods of sort.Interface to []TblDefCausPosInfos sorting in increasing order.
type TblDefCausPosInfoSlice []TblDefCausPosInfo

// Len implements sort.Interface#Len.
func (c TblDefCausPosInfoSlice) Len() int {
	return len(c)
}

// Swap implements sort.Interface#Swap.
func (c TblDefCausPosInfoSlice) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// Less implements sort.Interface#Less.
func (c TblDefCausPosInfoSlice) Less(i, j int) bool {
	return c[i].Start < c[j].Start
}

// FindHandle finds the ordinal of the corresponding handle column.
func (c TblDefCausPosInfoSlice) FindHandle(colOrdinal int) (int, bool) {
	if len(c) == 0 {
		return 0, false
	}
	// find the smallest index of the range that its start great than colOrdinal.
	// @see https://godoc.org/sort#Search
	rangeBehindOrdinal := sort.Search(len(c), func(i int) bool { return c[i].Start > colOrdinal })
	if rangeBehindOrdinal == 0 {
		return 0, false
	}
	if c[rangeBehindOrdinal-1].IsCommonHandle {
		// TODO: fix redesign uFIDelate join block to fix me.
		return 0, false
	}
	return c[rangeBehindOrdinal-1].HandleDefCauss.GetDefCaus(0).Index, true
}

// buildDeferredCausets2Handle builds columns to handle mapping.
func buildDeferredCausets2Handle(
	names []*types.FieldName,
	tblID2Handle map[int64][]HandleDefCauss,
	tblID2Block map[int64]block.Block,
	onlyWriblockDefCaus bool,
) (TblDefCausPosInfoSlice, error) {
	var defcaus2Handles TblDefCausPosInfoSlice
	for tblID, handleDefCauss := range tblID2Handle {
		tbl := tblID2Block[tblID]
		var tblLen int
		if onlyWriblockDefCaus {
			tblLen = len(tbl.WriblockDefCauss())
		} else {
			tblLen = len(tbl.DefCauss())
		}
		for _, handleDefCaus := range handleDefCauss {
			offset, err := getBlockOffset(names, names[handleDefCaus.GetDefCaus(0).Index])
			if err != nil {
				return nil, err
			}
			end := offset + tblLen
			defcaus2Handles = append(defcaus2Handles, TblDefCausPosInfo{tblID, offset, end, handleDefCaus, tbl.Meta().IsCommonHandle})
			// TODO: fix me for cluster index
		}
	}
	sort.Sort(defcaus2Handles)
	return defcaus2Handles, nil
}

func (b *PlanBuilder) buildUFIDelate(ctx context.Context, uFIDelate *ast.UFIDelateStmt) (Plan, error) {
	b.pushSelectOffset(0)
	b.pushBlockHints(uFIDelate.BlockHints, utilhint.TypeUFIDelate, 0)
	defer func() {
		b.popSelectOffset()
		// block hints are only visible in the current UFIDelATE statement.
		b.popBlockHints()
	}()

	// uFIDelate subquery block should be forbidden
	var asNameList []string
	asNameList = extractBlockSourceAsNames(uFIDelate.BlockRefs.BlockRefs, asNameList, true)
	for _, asName := range asNameList {
		for _, assign := range uFIDelate.List {
			if assign.DeferredCauset.Block.L == asName {
				return nil, ErrNonUFIDelablockBlock.GenWithStackByArgs(asName, "UFIDelATE")
			}
		}
	}

	b.inUFIDelateStmt = true

	p, err := b.buildResultSetNode(ctx, uFIDelate.BlockRefs.BlockRefs)
	if err != nil {
		return nil, err
	}

	var blockList []*ast.BlockName
	blockList = extractBlockList(uFIDelate.BlockRefs.BlockRefs, blockList, false)
	for _, t := range blockList {
		dbName := t.Schema.L
		if dbName == "" {
			dbName = b.ctx.GetStochastikVars().CurrentDB
		}
		if t.BlockInfo.IsView() {
			return nil, errors.Errorf("uFIDelate view %s is not supported now.", t.Name.O)
		}
		if t.BlockInfo.IsSequence() {
			return nil, errors.Errorf("uFIDelate sequence %s is not supported now.", t.Name.O)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.SelectPriv, dbName, t.Name.L, "", nil)
	}

	oldSchemaLen := p.Schema().Len()
	if uFIDelate.Where != nil {
		p, err = b.buildSelection(ctx, p, uFIDelate.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if b.ctx.GetStochastikVars().TxnCtx.IsPessimistic {
		if uFIDelate.BlockRefs.BlockRefs.Right == nil {
			// buildSelectLock is an optimization that can reduce RPC call.
			// We only need do this optimization for single block uFIDelate which is the most common case.
			// When BlockRefs.Right is nil, it is single block uFIDelate.
			p = b.buildSelectLock(p, &ast.SelectLockInfo{
				LockType: ast.SelectLockForUFIDelate,
			})
		}
	}

	if uFIDelate.Order != nil {
		p, err = b.buildSort(ctx, p, uFIDelate.Order.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	if uFIDelate.Limit != nil {
		p, err = b.buildLimit(p, uFIDelate.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Add project to freeze the order of output columns.
	proj := LogicalProjection{Exprs: expression.DeferredCauset2Exprs(p.Schema().DeferredCausets[:oldSchemaLen])}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(expression.NewSchema(make([]*expression.DeferredCauset, oldSchemaLen)...))
	proj.names = make(types.NameSlice, len(p.OutputNames()))
	INTERLOCKy(proj.names, p.OutputNames())
	INTERLOCKy(proj.schemaReplicant.DeferredCausets, p.Schema().DeferredCausets[:oldSchemaLen])
	proj.SetChildren(p)
	p = proj

	var uFIDelateBlockList []*ast.BlockName
	uFIDelateBlockList = extractBlockList(uFIDelate.BlockRefs.BlockRefs, uFIDelateBlockList, true)
	orderedList, np, allAssignmentsAreConstant, err := b.buildUFIDelateLists(ctx, uFIDelateBlockList, uFIDelate.List, p)
	if err != nil {
		return nil, err
	}
	p = np

	uFIDelt := UFIDelate{
		OrderedList:               orderedList,
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
	}.Init(b.ctx)
	uFIDelt.names = p.OutputNames()
	// We cannot apply projection elimination when building the subplan, because
	// columns in orderedList cannot be resolved.
	uFIDelt.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag&^flagEliminateProjection, p)
	if err != nil {
		return nil, err
	}
	err = uFIDelt.ResolveIndices()
	if err != nil {
		return nil, err
	}
	tblID2Handle, err := resolveIndicesForTblID2Handle(b.handleHelper.tailMap(), uFIDelt.SelectPlan.Schema())
	if err != nil {
		return nil, err
	}
	tblID2block := make(map[int64]block.Block, len(tblID2Handle))
	for id := range tblID2Handle {
		tblID2block[id], _ = b.is.BlockByID(id)
	}
	uFIDelt.TblDefCausPosInfos, err = buildDeferredCausets2Handle(uFIDelt.OutputNames(), tblID2Handle, tblID2block, true)
	if err == nil {
		err = checkUFIDelateList(b.ctx, tblID2block, uFIDelt)
	}
	uFIDelt.PartitionedBlock = b.partitionedBlock
	return uFIDelt, err
}

// GetUFIDelateDeferredCausets gets the columns of uFIDelated lists.
func GetUFIDelateDeferredCausets(ctx stochastikctx.Context, orderedList []*expression.Assignment, schemaLen int) ([]bool, error) {
	assignFlag := make([]bool, schemaLen)
	for _, v := range orderedList {
		if !ctx.GetStochastikVars().AllowWriteRowID && v.DefCaus.ID == perceptron.ExtraHandleID {
			return nil, errors.Errorf("insert, uFIDelate and replace statements for _milevadb_rowid are not supported.")
		}
		idx := v.DefCaus.Index
		assignFlag[idx] = true
	}
	return assignFlag, nil
}

func checkUFIDelateList(ctx stochastikctx.Context, tblID2block map[int64]block.Block, uFIDelt *UFIDelate) error {
	assignFlags, err := GetUFIDelateDeferredCausets(ctx, uFIDelt.OrderedList, uFIDelt.SelectPlan.Schema().Len())
	if err != nil {
		return err
	}
	for _, content := range uFIDelt.TblDefCausPosInfos {
		tbl := tblID2block[content.TblID]
		flags := assignFlags[content.Start:content.End]
		for i, col := range tbl.WriblockDefCauss() {
			if flags[i] && col.State != perceptron.StatePublic {
				return ErrUnknownDeferredCauset.GenWithStackByArgs(col.Name, clauseMsg[fieldList])
			}
		}
	}
	return nil
}

func (b *PlanBuilder) buildUFIDelateLists(
	ctx context.Context,
	blockList []*ast.BlockName,
	list []*ast.Assignment,
	p LogicalPlan,
) (newList []*expression.Assignment,
	po LogicalPlan,
	allAssignmentsAreConstant bool,
	e error,
) {
	b.curClause = fieldList
	// modifyDeferredCausets indicates which columns are in set list,
	// and if it is set to `DEFAULT`
	modifyDeferredCausets := make(map[string]bool, p.Schema().Len())
	var columnsIdx map[*ast.DeferredCausetName]int
	cacheDeferredCausetsIdx := false
	if len(p.OutputNames()) > 16 {
		cacheDeferredCausetsIdx = true
		columnsIdx = make(map[*ast.DeferredCausetName]int, len(list))
	}
	for _, assign := range list {
		idx, err := expression.FindFieldName(p.OutputNames(), assign.DeferredCauset)
		if err != nil {
			return nil, nil, false, err
		}
		if idx < 0 {
			return nil, nil, false, ErrUnknownDeferredCauset.GenWithStackByArgs(assign.DeferredCauset.Name, "field list")
		}
		if cacheDeferredCausetsIdx {
			columnsIdx[assign.DeferredCauset] = idx
		}
		name := p.OutputNames()[idx]
		columnFullName := fmt.Sprintf("%s.%s.%s", name.DBName.L, name.TblName.L, name.DefCausName.L)
		// We save a flag for the column in map `modifyDeferredCausets`
		// This flag indicated if assign keyword `DEFAULT` to the column
		if extractDefaultExpr(assign.Expr) != nil {
			modifyDeferredCausets[columnFullName] = true
		} else {
			modifyDeferredCausets[columnFullName] = false
		}
	}

	// If columns in set list contains generated columns, raise error.
	// And, fill virtualAssignments here; that's for generated columns.
	virtualAssignments := make([]*ast.Assignment, 0)

	for _, tn := range blockList {
		blockInfo := tn.BlockInfo
		blockVal, found := b.is.BlockByID(blockInfo.ID)
		if !found {
			return nil, nil, false, schemareplicant.ErrBlockNotExists.GenWithStackByArgs(tn.DBInfo.Name.O, blockInfo.Name.O)
		}
		for i, colInfo := range blockInfo.DeferredCausets {
			if !colInfo.IsGenerated() {
				continue
			}
			columnFullName := fmt.Sprintf("%s.%s.%s", tn.Schema.L, tn.Name.L, colInfo.Name.L)
			isDefault, ok := modifyDeferredCausets[columnFullName]
			if ok && colInfo.Hidden {
				return nil, nil, false, ErrUnknownDeferredCauset.GenWithStackByArgs(colInfo.Name, clauseMsg[fieldList])
			}
			// Note: For INSERT, REPLACE, and UFIDelATE, if a generated column is inserted into, replaced, or uFIDelated explicitly, the only permitted value is DEFAULT.
			// see https://dev.allegrosql.com/doc/refman/8.0/en/create-block-generated-columns.html
			if ok && !isDefault {
				return nil, nil, false, ErrBadGeneratedDeferredCauset.GenWithStackByArgs(colInfo.Name.O, blockInfo.Name.O)
			}
			virtualAssignments = append(virtualAssignments, &ast.Assignment{
				DeferredCauset: &ast.DeferredCausetName{Schema: tn.Schema, Block: tn.Name, Name: colInfo.Name},
				Expr:           blockVal.DefCauss()[i].GeneratedExpr,
			})
		}
	}

	allAssignmentsAreConstant = true
	newList = make([]*expression.Assignment, 0, p.Schema().Len())
	tblDbMap := make(map[string]string, len(blockList))
	for _, tbl := range blockList {
		tblDbMap[tbl.Name.L] = tbl.DBInfo.Name.L
	}

	allAssignments := append(list, virtualAssignments...)
	for i, assign := range allAssignments {
		var idx int
		var err error
		if cacheDeferredCausetsIdx {
			if i, ok := columnsIdx[assign.DeferredCauset]; ok {
				idx = i
			} else {
				idx, err = expression.FindFieldName(p.OutputNames(), assign.DeferredCauset)
			}
		} else {
			idx, err = expression.FindFieldName(p.OutputNames(), assign.DeferredCauset)
		}
		if err != nil {
			return nil, nil, false, err
		}
		col := p.Schema().DeferredCausets[idx]
		name := p.OutputNames()[idx]
		var newExpr expression.Expression
		var np LogicalPlan
		if i < len(list) {
			// If assign `DEFAULT` to column, fill the `defaultExpr.Name` before rewrite expression
			if expr := extractDefaultExpr(assign.Expr); expr != nil {
				expr.Name = assign.DeferredCauset
			}
			newExpr, np, err = b.rewrite(ctx, assign.Expr, p, nil, false)
		} else {
			// rewrite with generation expression
			rewritePreprocess := func(expr ast.Node) ast.Node {
				switch x := expr.(type) {
				case *ast.DeferredCausetName:
					return &ast.DeferredCausetName{
						Schema: assign.DeferredCauset.Schema,
						Block:  assign.DeferredCauset.Block,
						Name:   x.Name,
					}
				default:
					return expr
				}
			}
			newExpr, np, err = b.rewriteWithPreprocess(ctx, assign.Expr, p, nil, nil, false, rewritePreprocess)
		}
		if err != nil {
			return nil, nil, false, err
		}
		if _, isConst := newExpr.(*expression.Constant); !isConst {
			allAssignmentsAreConstant = false
		}
		p = np
		newList = append(newList, &expression.Assignment{DefCaus: col, DefCausName: name.DefCausName, Expr: newExpr})
		dbName := name.DBName.L
		// To solve issue#10028, we need to get database name by the block alias name.
		if dbNameTmp, ok := tblDbMap[name.TblName.L]; ok {
			dbName = dbNameTmp
		}
		if dbName == "" {
			dbName = b.ctx.GetStochastikVars().CurrentDB
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.UFIDelatePriv, dbName, name.OrigTblName.L, "", nil)
	}
	return newList, p, allAssignmentsAreConstant, nil
}

// extractDefaultExpr extract a `DefaultExpr` from `ExprNode`,
// If it is a `DEFAULT` function like `DEFAULT(a)`, return nil.
// Only if it is `DEFAULT` keyword, it will return the `DefaultExpr`.
func extractDefaultExpr(node ast.ExprNode) *ast.DefaultExpr {
	if expr, ok := node.(*ast.DefaultExpr); ok && expr.Name == nil {
		return expr
	}
	return nil
}

func (b *PlanBuilder) buildDelete(ctx context.Context, delete *ast.DeleteStmt) (Plan, error) {
	b.pushSelectOffset(0)
	b.pushBlockHints(delete.BlockHints, utilhint.TypeDelete, 0)
	defer func() {
		b.popSelectOffset()
		// block hints are only visible in the current DELETE statement.
		b.popBlockHints()
	}()

	b.inDeleteStmt = true

	p, err := b.buildResultSetNode(ctx, delete.BlockRefs.BlockRefs)
	if err != nil {
		return nil, err
	}
	oldSchema := p.Schema()
	oldLen := oldSchema.Len()

	if delete.Where != nil {
		p, err = b.buildSelection(ctx, p, delete.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if b.ctx.GetStochastikVars().TxnCtx.IsPessimistic {
		if !delete.IsMultiBlock {
			p = b.buildSelectLock(p, &ast.SelectLockInfo{
				LockType: ast.SelectLockForUFIDelate,
			})
		}
	}

	if delete.Order != nil {
		p, err = b.buildSort(ctx, p, delete.Order.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	if delete.Limit != nil {
		p, err = b.buildLimit(p, delete.Limit)
		if err != nil {
			return nil, err
		}
	}

	proj := LogicalProjection{Exprs: expression.DeferredCauset2Exprs(p.Schema().DeferredCausets[:oldLen])}.Init(b.ctx, b.getSelectOffset())
	proj.SetChildren(p)
	proj.SetSchema(oldSchema.Clone())
	proj.names = p.OutputNames()[:oldLen]
	p = proj

	handleDefCaussMap := b.handleHelper.tailMap()
	for _, defcaus := range handleDefCaussMap {
		for _, col := range defcaus {
			for i := 0; i < col.NumDefCauss(); i++ {
				exprDefCaus := col.GetDefCaus(i)
				if proj.Schema().Contains(exprDefCaus) {
					continue
				}
				proj.Exprs = append(proj.Exprs, exprDefCaus)
				proj.Schema().DeferredCausets = append(proj.Schema().DeferredCausets, exprDefCaus)
				proj.names = append(proj.names, types.EmptyName)
			}
		}
	}

	del := Delete{
		IsMultiBlock: delete.IsMultiBlock,
	}.Init(b.ctx)

	del.names = p.OutputNames()
	del.SelectPlan, _, err = DoOptimize(ctx, b.ctx, b.optFlag, p)
	if err != nil {
		return nil, err
	}

	tblID2Handle, err := resolveIndicesForTblID2Handle(handleDefCaussMap, del.SelectPlan.Schema())
	if err != nil {
		return nil, err
	}

	var blockList []*ast.BlockName
	blockList = extractBlockList(delete.BlockRefs.BlockRefs, blockList, true)

	// DefCauslect visitInfo.
	if delete.Blocks != nil {
		// Delete a, b from a, b, c, d... add a and b.
		for _, tn := range delete.Blocks.Blocks {
			foundMatch := false
			for _, v := range blockList {
				dbName := v.Schema.L
				if dbName == "" {
					dbName = b.ctx.GetStochastikVars().CurrentDB
				}
				if (tn.Schema.L == "" || tn.Schema.L == dbName) && tn.Name.L == v.Name.L {
					tn.Schema.L = dbName
					tn.DBInfo = v.DBInfo
					tn.BlockInfo = v.BlockInfo
					foundMatch = true
					break
				}
			}
			if !foundMatch {
				var asNameList []string
				asNameList = extractBlockSourceAsNames(delete.BlockRefs.BlockRefs, asNameList, false)
				for _, asName := range asNameList {
					tblName := tn.Name.L
					if tn.Schema.L != "" {
						tblName = tn.Schema.L + "." + tblName
					}
					if asName == tblName {
						// check allegrosql like: `delete a from (select * from t) as a, t`
						return nil, ErrNonUFIDelablockBlock.GenWithStackByArgs(tn.Name.O, "DELETE")
					}
				}
				// check allegrosql like: `delete b from (select * from t) as a, t`
				return nil, ErrUnknownBlock.GenWithStackByArgs(tn.Name.O, "MULTI DELETE")
			}
			if tn.BlockInfo.IsView() {
				return nil, errors.Errorf("delete view %s is not supported now.", tn.Name.O)
			}
			if tn.BlockInfo.IsSequence() {
				return nil, errors.Errorf("delete sequence %s is not supported now.", tn.Name.O)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.DeletePriv, tn.Schema.L, tn.BlockInfo.Name.L, "", nil)
		}
	} else {
		// Delete from a, b, c, d.
		for _, v := range blockList {
			if v.BlockInfo.IsView() {
				return nil, errors.Errorf("delete view %s is not supported now.", v.Name.O)
			}
			if v.BlockInfo.IsSequence() {
				return nil, errors.Errorf("delete sequence %s is not supported now.", v.Name.O)
			}
			dbName := v.Schema.L
			if dbName == "" {
				dbName = b.ctx.GetStochastikVars().CurrentDB
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, allegrosql.DeletePriv, dbName, v.Name.L, "", nil)
		}
	}
	if del.IsMultiBlock {
		// tblID2BlockName is the block map value is an array which contains block aliases.
		// Block ID may not be unique for deleting multiple blocks, for statements like
		// `delete from t as t1, t as t2`, the same block has two alias, we have to identify a block
		// by its alias instead of ID.
		tblID2BlockName := make(map[int64][]*ast.BlockName, len(delete.Blocks.Blocks))
		for _, tn := range delete.Blocks.Blocks {
			tblID2BlockName[tn.BlockInfo.ID] = append(tblID2BlockName[tn.BlockInfo.ID], tn)
		}
		tblID2Handle = del.cleanTblID2HandleMap(tblID2BlockName, tblID2Handle, del.names)
	}
	tblID2block := make(map[int64]block.Block, len(tblID2Handle))
	for id := range tblID2Handle {
		tblID2block[id], _ = b.is.BlockByID(id)
	}
	del.TblDefCausPosInfos, err = buildDeferredCausets2Handle(del.names, tblID2Handle, tblID2block, false)
	return del, err
}

func resolveIndicesForTblID2Handle(tblID2Handle map[int64][]HandleDefCauss, schemaReplicant *expression.Schema) (map[int64][]HandleDefCauss, error) {
	newMap := make(map[int64][]HandleDefCauss, len(tblID2Handle))
	for i, defcaus := range tblID2Handle {
		for _, col := range defcaus {
			resolvedDefCaus, err := col.ResolveIndices(schemaReplicant)
			if err != nil {
				return nil, err
			}
			newMap[i] = append(newMap[i], resolvedDefCaus)
		}
	}
	return newMap, nil
}

func (p *Delete) cleanTblID2HandleMap(
	blocksToDelete map[int64][]*ast.BlockName,
	tblID2Handle map[int64][]HandleDefCauss,
	outputNames []*types.FieldName,
) map[int64][]HandleDefCauss {
	for id, defcaus := range tblID2Handle {
		names, ok := blocksToDelete[id]
		if !ok {
			delete(tblID2Handle, id)
			continue
		}
		for i := len(defcaus) - 1; i >= 0; i-- {
			hDefCauss := defcaus[i]
			var hasMatch bool
			for j := 0; j < hDefCauss.NumDefCauss(); j++ {
				if p.matchingDeletingBlock(names, outputNames[hDefCauss.GetDefCaus(j).Index]) {
					hasMatch = true
					break
				}
			}
			if !hasMatch {
				defcaus = append(defcaus[:i], defcaus[i+1:]...)
			}
		}
		if len(defcaus) == 0 {
			delete(tblID2Handle, id)
			continue
		}
		tblID2Handle[id] = defcaus
	}
	return tblID2Handle
}

// matchingDeletingBlock checks whether this column is from the block which is in the deleting list.
func (p *Delete) matchingDeletingBlock(names []*ast.BlockName, name *types.FieldName) bool {
	for _, n := range names {
		if (name.DBName.L == "" || name.DBName.L == n.Schema.L) && name.TblName.L == n.Name.L {
			return true
		}
	}
	return false
}

func getWindowName(name string) string {
	if name == "" {
		return "<unnamed window>"
	}
	return name
}

// buildProjectionForWindow builds the projection for expressions in the window specification that is not an column,
// so after the projection, window functions only needs to deal with columns.
func (b *PlanBuilder) buildProjectionForWindow(ctx context.Context, p LogicalPlan, spec *ast.WindowSpec, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) (LogicalPlan, []property.Item, []property.Item, []expression.Expression, error) {
	b.optFlag |= flagEliminateProjection

	var partitionItems, orderItems []*ast.ByItem
	if spec.PartitionBy != nil {
		partitionItems = spec.PartitionBy.Items
	}
	if spec.OrderBy != nil {
		orderItems = spec.OrderBy.Items
	}

	projLen := len(p.Schema().DeferredCausets) + len(partitionItems) + len(orderItems) + len(args)
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, projLen)}.Init(b.ctx, b.getSelectOffset())
	proj.SetSchema(expression.NewSchema(make([]*expression.DeferredCauset, 0, projLen)...))
	proj.names = make([]*types.FieldName, p.Schema().Len(), projLen)
	for _, col := range p.Schema().DeferredCausets {
		proj.Exprs = append(proj.Exprs, col)
		proj.schemaReplicant.Append(col)
	}
	INTERLOCKy(proj.names, p.OutputNames())

	propertyItems := make([]property.Item, 0, len(partitionItems)+len(orderItems))
	var err error
	p, propertyItems, err = b.buildByItemsForWindow(ctx, p, proj, partitionItems, propertyItems, aggMap)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	lenPartition := len(propertyItems)
	p, propertyItems, err = b.buildByItemsForWindow(ctx, p, proj, orderItems, propertyItems, aggMap)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	newArgList := make([]expression.Expression, 0, len(args))
	for _, arg := range args {
		newArg, np, err := b.rewrite(ctx, arg, p, aggMap, true)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		p = np
		switch newArg.(type) {
		case *expression.DeferredCauset, *expression.Constant:
			newArgList = append(newArgList, newArg)
			continue
		}
		proj.Exprs = append(proj.Exprs, newArg)
		proj.names = append(proj.names, types.EmptyName)
		col := &expression.DeferredCauset{
			UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			RetType:  newArg.GetType(),
		}
		proj.schemaReplicant.Append(col)
		newArgList = append(newArgList, col)
	}

	proj.SetChildren(p)
	return proj, propertyItems[:lenPartition], propertyItems[lenPartition:], newArgList, nil
}

func (b *PlanBuilder) buildArgs4WindowFunc(ctx context.Context, p LogicalPlan, args []ast.ExprNode, aggMap map[*ast.AggregateFuncExpr]int) ([]expression.Expression, error) {
	b.optFlag |= flagEliminateProjection

	newArgList := make([]expression.Expression, 0, len(args))
	// use below index for created a new col definition
	// it's okay here because we only want to return the args used in window function
	newDefCausIndex := 0
	for _, arg := range args {
		newArg, np, err := b.rewrite(ctx, arg, p, aggMap, true)
		if err != nil {
			return nil, err
		}
		p = np
		switch newArg.(type) {
		case *expression.DeferredCauset, *expression.Constant:
			newArgList = append(newArgList, newArg)
			continue
		}
		col := &expression.DeferredCauset{
			UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			RetType:  newArg.GetType(),
		}
		newDefCausIndex += 1
		newArgList = append(newArgList, col)
	}
	return newArgList, nil
}

func (b *PlanBuilder) buildByItemsForWindow(
	ctx context.Context,
	p LogicalPlan,
	proj *LogicalProjection,
	items []*ast.ByItem,
	retItems []property.Item,
	aggMap map[*ast.AggregateFuncExpr]int,
) (LogicalPlan, []property.Item, error) {
	transformer := &itemTransformer{}
	for _, item := range items {
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewrite(ctx, item.Expr, p, aggMap, true)
		if err != nil {
			return nil, nil, err
		}
		p = np
		if it.GetType().Tp == allegrosql.TypeNull {
			continue
		}
		if col, ok := it.(*expression.DeferredCauset); ok {
			retItems = append(retItems, property.Item{DefCaus: col, Desc: item.Desc})
			continue
		}
		proj.Exprs = append(proj.Exprs, it)
		proj.names = append(proj.names, types.EmptyName)
		col := &expression.DeferredCauset{
			UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			RetType:  it.GetType(),
		}
		proj.schemaReplicant.Append(col)
		retItems = append(retItems, property.Item{DefCaus: col, Desc: item.Desc})
	}
	return p, retItems, nil
}

// buildWindowFunctionFrameBound builds the bounds of window function frames.
// For type `Rows`, the bound expr must be an unsigned integer.
// For type `Range`, the bound expr must be temporal or numeric types.
func (b *PlanBuilder) buildWindowFunctionFrameBound(ctx context.Context, spec *ast.WindowSpec, orderByItems []property.Item, boundClause *ast.FrameBound) (*FrameBound, error) {
	frameType := spec.Frame.Type
	bound := &FrameBound{Type: boundClause.Type, UnBounded: boundClause.UnBounded}
	if bound.UnBounded {
		return bound, nil
	}

	if frameType == ast.Rows {
		if bound.Type == ast.CurrentRow {
			return bound, nil
		}
		numRows, _, _ := getUintFromNode(b.ctx, boundClause.Expr)
		bound.Num = numRows
		return bound, nil
	}

	bound.CalcFuncs = make([]expression.Expression, len(orderByItems))
	bound.CmpFuncs = make([]expression.CompareFunc, len(orderByItems))
	if bound.Type == ast.CurrentRow {
		for i, item := range orderByItems {
			col := item.DefCaus
			bound.CalcFuncs[i] = col
			bound.CmpFuncs[i] = expression.GetCmpFunction(b.ctx, col, col)
		}
		return bound, nil
	}

	col := orderByItems[0].DefCaus
	// TODO: We also need to raise error for non-deterministic expressions, like rand().
	val, err := evalAstExpr(b.ctx, boundClause.Expr)
	if err != nil {
		return nil, ErrWindowRangeBoundNotConstant.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	expr := expression.Constant{Value: val, RetType: boundClause.Expr.GetType()}

	checker := &paramMarkerInPrepareChecker{}
	boundClause.Expr.Accept(checker)

	// If it has paramMarker and is in prepare stmt. We don't need to eval it since its value is not decided yet.
	if !checker.inPrepareStmt {
		// Do not raise warnings for truncate.
		oriIgnoreTruncate := b.ctx.GetStochastikVars().StmtCtx.IgnoreTruncate
		b.ctx.GetStochastikVars().StmtCtx.IgnoreTruncate = true
		uVal, isNull, err := expr.EvalInt(b.ctx, chunk.Row{})
		b.ctx.GetStochastikVars().StmtCtx.IgnoreTruncate = oriIgnoreTruncate
		if uVal < 0 || isNull || err != nil {
			return nil, ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
	}

	desc := orderByItems[0].Desc
	if boundClause.Unit != ast.TimeUnitInvalid {
		// TODO: Perhaps we don't need to transcode this back to generic string
		unitVal := boundClause.Unit.String()
		unit := expression.Constant{
			Value:   types.NewStringCauset(unitVal),
			RetType: types.NewFieldType(allegrosql.TypeVarchar),
		}

		// When the order is asc:
		//   `+` for following, and `-` for the preceding
		// When the order is desc, `+` becomes `-` and vice-versa.
		funcName := ast.DateAdd
		if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
			funcName = ast.DateSub
		}
		bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx, funcName, col.RetType, col, &expr, &unit)
		if err != nil {
			return nil, err
		}
		bound.CmpFuncs[0] = expression.GetCmpFunction(b.ctx, orderByItems[0].DefCaus, bound.CalcFuncs[0])
		return bound, nil
	}
	// When the order is asc:
	//   `+` for following, and `-` for the preceding
	// When the order is desc, `+` becomes `-` and vice-versa.
	funcName := ast.Plus
	if (!desc && bound.Type == ast.Preceding) || (desc && bound.Type == ast.Following) {
		funcName = ast.Minus
	}
	bound.CalcFuncs[0], err = expression.NewFunctionBase(b.ctx, funcName, col.RetType, col, &expr)
	if err != nil {
		return nil, err
	}
	bound.CmpFuncs[0] = expression.GetCmpFunction(b.ctx, orderByItems[0].DefCaus, bound.CalcFuncs[0])
	return bound, nil
}

// paramMarkerInPrepareChecker checks whether the given ast tree has paramMarker and is in prepare statement.
type paramMarkerInPrepareChecker struct {
	inPrepareStmt bool
}

// Enter implements Visitor Interface.
func (pc *paramMarkerInPrepareChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch v := in.(type) {
	case *driver.ParamMarkerExpr:
		pc.inPrepareStmt = !v.InExecute
		return v, true
	}
	return in, false
}

// Leave implements Visitor Interface.
func (pc *paramMarkerInPrepareChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// buildWindowFunctionFrame builds the window function frames.
// See https://dev.allegrosql.com/doc/refman/8.0/en/window-functions-frames.html
func (b *PlanBuilder) buildWindowFunctionFrame(ctx context.Context, spec *ast.WindowSpec, orderByItems []property.Item) (*WindowFrame, error) {
	frameClause := spec.Frame
	if frameClause == nil {
		return nil, nil
	}
	frame := &WindowFrame{Type: frameClause.Type}
	var err error
	frame.Start, err = b.buildWindowFunctionFrameBound(ctx, spec, orderByItems, &frameClause.Extent.Start)
	if err != nil {
		return nil, err
	}
	frame.End, err = b.buildWindowFunctionFrameBound(ctx, spec, orderByItems, &frameClause.Extent.End)
	return frame, err
}

func (b *PlanBuilder) checkWindowFuncArgs(ctx context.Context, p LogicalPlan, windowFuncExprs []*ast.WindowFuncExpr, windowAggMap map[*ast.AggregateFuncExpr]int) error {
	for _, windowFuncExpr := range windowFuncExprs {
		if strings.ToLower(windowFuncExpr.F) == ast.AggFuncGroupConcat {
			return ErrNotSupportedYet.GenWithStackByArgs("group_concat as window function")
		}
		args, err := b.buildArgs4WindowFunc(ctx, p, windowFuncExpr.Args, windowAggMap)
		if err != nil {
			return err
		}
		desc, err := aggregation.NewWindowFuncDesc(b.ctx, windowFuncExpr.F, args)
		if err != nil {
			return err
		}
		if desc == nil {
			return ErrWrongArguments.GenWithStackByArgs(strings.ToLower(windowFuncExpr.F))
		}
	}
	return nil
}

func getAllByItems(itemsBuf []*ast.ByItem, spec *ast.WindowSpec) []*ast.ByItem {
	itemsBuf = itemsBuf[:0]
	if spec.PartitionBy != nil {
		itemsBuf = append(itemsBuf, spec.PartitionBy.Items...)
	}
	if spec.OrderBy != nil {
		itemsBuf = append(itemsBuf, spec.OrderBy.Items...)
	}
	return itemsBuf
}

func restoreByItemText(item *ast.ByItem) string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(0, &sb)
	err := item.Expr.Restore(ctx)
	if err != nil {
		return ""
	}
	return sb.String()
}

func compareItems(lItems []*ast.ByItem, rItems []*ast.ByItem) bool {
	minLen := mathutil.Min(len(lItems), len(rItems))
	for i := 0; i < minLen; i++ {
		res := strings.Compare(restoreByItemText(lItems[i]), restoreByItemText(rItems[i]))
		if res != 0 {
			return res < 0
		}
		res = compareBool(lItems[i].Desc, rItems[i].Desc)
		if res != 0 {
			return res < 0
		}
	}
	return len(lItems) < len(rItems)
}

type windowFuncs struct {
	spec  *ast.WindowSpec
	funcs []*ast.WindowFuncExpr
}

// sortWindowSpecs sorts the window specifications by reversed alphabetical order, then we could add less `Sort` operator
// in physical plan because the window functions with the same partition by and order by clause will be at near places.
func sortWindowSpecs(groupedFuncs map[*ast.WindowSpec][]*ast.WindowFuncExpr) []windowFuncs {
	windows := make([]windowFuncs, 0, len(groupedFuncs))
	for spec, funcs := range groupedFuncs {
		windows = append(windows, windowFuncs{spec, funcs})
	}
	lItemsBuf := make([]*ast.ByItem, 0, 4)
	rItemsBuf := make([]*ast.ByItem, 0, 4)
	sort.SliceSblock(windows, func(i, j int) bool {
		lItemsBuf = getAllByItems(lItemsBuf, windows[i].spec)
		rItemsBuf = getAllByItems(rItemsBuf, windows[j].spec)
		return !compareItems(lItemsBuf, rItemsBuf)
	})
	return windows
}

func (b *PlanBuilder) buildWindowFunctions(ctx context.Context, p LogicalPlan, groupedFuncs map[*ast.WindowSpec][]*ast.WindowFuncExpr, aggMap map[*ast.AggregateFuncExpr]int) (LogicalPlan, map[*ast.WindowFuncExpr]int, error) {
	args := make([]ast.ExprNode, 0, 4)
	windowMap := make(map[*ast.WindowFuncExpr]int)
	for _, window := range sortWindowSpecs(groupedFuncs) {
		args = args[:0]
		spec, funcs := window.spec, window.funcs
		for _, windowFunc := range funcs {
			args = append(args, windowFunc.Args...)
		}
		np, partitionBy, orderBy, args, err := b.buildProjectionForWindow(ctx, p, spec, args, aggMap)
		if err != nil {
			return nil, nil, err
		}
		err = b.checkOriginWindowSpecs(funcs, orderBy)
		if err != nil {
			return nil, nil, err
		}
		frame, err := b.buildWindowFunctionFrame(ctx, spec, orderBy)
		if err != nil {
			return nil, nil, err
		}

		window := LogicalWindow{
			PartitionBy: partitionBy,
			OrderBy:     orderBy,
			Frame:       frame,
		}.Init(b.ctx, b.getSelectOffset())
		window.names = make([]*types.FieldName, np.Schema().Len())
		INTERLOCKy(window.names, np.OutputNames())
		schemaReplicant := np.Schema().Clone()
		descs := make([]*aggregation.WindowFuncDesc, 0, len(funcs))
		preArgs := 0
		for _, windowFunc := range funcs {
			desc, err := aggregation.NewWindowFuncDesc(b.ctx, windowFunc.F, args[preArgs:preArgs+len(windowFunc.Args)])
			if err != nil {
				return nil, nil, err
			}
			if desc == nil {
				return nil, nil, ErrWrongArguments.GenWithStackByArgs(strings.ToLower(windowFunc.F))
			}
			preArgs += len(windowFunc.Args)
			desc.WrapCastForAggArgs(b.ctx)
			descs = append(descs, desc)
			windowMap[windowFunc] = schemaReplicant.Len()
			schemaReplicant.Append(&expression.DeferredCauset{
				UniqueID: b.ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
				RetType:  desc.RetTp,
			})
			window.names = append(window.names, types.EmptyName)
		}
		window.WindowFuncDescs = descs
		window.SetChildren(np)
		window.SetSchema(schemaReplicant)
		p = window
	}
	return p, windowMap, nil
}

// checkOriginWindowSpecs checks the validation for origin window specifications for a group of functions.
// Because of the grouped specification is different from it, we should especially check them before build window frame.
func (b *PlanBuilder) checkOriginWindowSpecs(funcs []*ast.WindowFuncExpr, orderByItems []property.Item) error {
	for _, f := range funcs {
		if f.IgnoreNull {
			return ErrNotSupportedYet.GenWithStackByArgs("IGNORE NULLS")
		}
		if f.Distinct {
			return ErrNotSupportedYet.GenWithStackByArgs("<window function>(DISTINCT ..)")
		}
		if f.FromLast {
			return ErrNotSupportedYet.GenWithStackByArgs("FROM LAST")
		}
		spec := &f.Spec
		if f.Spec.Name.L != "" {
			spec = b.windowSpecs[f.Spec.Name.L]
		}
		if spec.Frame == nil {
			continue
		}
		if spec.Frame.Type == ast.Groups {
			return ErrNotSupportedYet.GenWithStackByArgs("GROUPS")
		}
		start, end := spec.Frame.Extent.Start, spec.Frame.Extent.End
		if start.Type == ast.Following && start.UnBounded {
			return ErrWindowFrameStartIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		if end.Type == ast.Preceding && end.UnBounded {
			return ErrWindowFrameEndIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		if start.Type == ast.Following && (end.Type == ast.Preceding || end.Type == ast.CurrentRow) {
			return ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		if (start.Type == ast.Following || start.Type == ast.CurrentRow) && end.Type == ast.Preceding {
			return ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}

		err := b.checkOriginWindowFrameBound(&start, spec, orderByItems)
		if err != nil {
			return err
		}
		err = b.checkOriginWindowFrameBound(&end, spec, orderByItems)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *PlanBuilder) checkOriginWindowFrameBound(bound *ast.FrameBound, spec *ast.WindowSpec, orderByItems []property.Item) error {
	if bound.Type == ast.CurrentRow || bound.UnBounded {
		return nil
	}

	frameType := spec.Frame.Type
	if frameType == ast.Rows {
		if bound.Unit != ast.TimeUnitInvalid {
			return ErrWindowRowsIntervalUse.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		_, isNull, isExpectedType := getUintFromNode(b.ctx, bound.Expr)
		if isNull || !isExpectedType {
			return ErrWindowFrameIllegal.GenWithStackByArgs(getWindowName(spec.Name.O))
		}
		return nil
	}

	if len(orderByItems) != 1 {
		return ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	orderItemType := orderByItems[0].DefCaus.RetType.Tp
	isNumeric, isTemporal := types.IsTypeNumeric(orderItemType), types.IsTypeTemporal(orderItemType)
	if !isNumeric && !isTemporal {
		return ErrWindowRangeFrameOrderType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if bound.Unit != ast.TimeUnitInvalid && !isTemporal {
		return ErrWindowRangeFrameNumericType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	if bound.Unit == ast.TimeUnitInvalid && !isNumeric {
		return ErrWindowRangeFrameTemporalType.GenWithStackByArgs(getWindowName(spec.Name.O))
	}
	return nil
}

func extractWindowFuncs(fields []*ast.SelectField) []*ast.WindowFuncExpr {
	extractor := &WindowFuncExtractor{}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	return extractor.windowFuncs
}

func (b *PlanBuilder) handleDefaultFrame(spec *ast.WindowSpec, windowFuncName string) (*ast.WindowSpec, bool) {
	needFrame := aggregation.NeedFrame(windowFuncName)
	// According to MyALLEGROSQL, In the absence of a frame clause, the default frame depends on whether an ORDER BY clause is present:
	//   (1) With order by, the default frame is equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
	//   (2) Without order by, the default frame is includes all partition rows, equivalent to "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
	//       or "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", which is the same as an empty frame.
	// https://dev.allegrosql.com/doc/refman/8.0/en/window-functions-frames.html
	if needFrame && spec.Frame == nil && spec.OrderBy != nil {
		newSpec := *spec
		newSpec.Frame = &ast.FrameClause{
			Type: ast.Ranges,
			Extent: ast.FrameExtent{
				Start: ast.FrameBound{Type: ast.Preceding, UnBounded: true},
				End:   ast.FrameBound{Type: ast.CurrentRow},
			},
		}
		return &newSpec, true
	}
	// "RANGE/ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" is equivalent to empty frame.
	if needFrame && spec.Frame != nil &&
		spec.Frame.Extent.Start.UnBounded && spec.Frame.Extent.End.UnBounded {
		newSpec := *spec
		newSpec.Frame = nil
		return &newSpec, true
	}
	// For functions that operate on the entire partition, the frame clause will be ignored.
	if !needFrame && spec.Frame != nil {
		specName := spec.Name.O
		b.ctx.GetStochastikVars().StmtCtx.AppendNote(ErrWindowFunctionIgnoresFrame.GenWithStackByArgs(windowFuncName, getWindowName(specName)))
		newSpec := *spec
		newSpec.Frame = nil
		return &newSpec, true
	}
	return spec, false
}

// groupWindowFuncs groups the window functions according to the window specification name.
// TODO: We can group the window function by the definition of window specification.
func (b *PlanBuilder) groupWindowFuncs(windowFuncs []*ast.WindowFuncExpr) (map[*ast.WindowSpec][]*ast.WindowFuncExpr, error) {
	// uFIDelatedSpecMap is used to handle the specifications that have frame clause changed.
	uFIDelatedSpecMap := make(map[string]*ast.WindowSpec)
	groupedWindow := make(map[*ast.WindowSpec][]*ast.WindowFuncExpr)
	for _, windowFunc := range windowFuncs {
		if windowFunc.Spec.Name.L == "" {
			spec := &windowFunc.Spec
			if spec.Ref.L != "" {
				ref, ok := b.windowSpecs[spec.Ref.L]
				if !ok {
					return nil, ErrWindowNoSuchWindow.GenWithStackByArgs(getWindowName(spec.Ref.O))
				}
				err := mergeWindowSpec(spec, ref)
				if err != nil {
					return nil, err
				}
			}
			spec, _ = b.handleDefaultFrame(spec, windowFunc.F)
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
			continue
		}

		name := windowFunc.Spec.Name.L
		spec, ok := b.windowSpecs[name]
		if !ok {
			return nil, ErrWindowNoSuchWindow.GenWithStackByArgs(windowFunc.Spec.Name.O)
		}
		newSpec, uFIDelated := b.handleDefaultFrame(spec, windowFunc.F)
		if !uFIDelated {
			groupedWindow[spec] = append(groupedWindow[spec], windowFunc)
		} else {
			if _, ok := uFIDelatedSpecMap[name]; !ok {
				uFIDelatedSpecMap[name] = newSpec
			}
			uFIDelatedSpec := uFIDelatedSpecMap[name]
			groupedWindow[uFIDelatedSpec] = append(groupedWindow[uFIDelatedSpec], windowFunc)
		}
	}
	return groupedWindow, nil
}

// resolveWindowSpec resolve window specifications for allegrosql like `select ... from t window w1 as (w2), w2 as (partition by a)`.
// We need to resolve the referenced window to get the definition of current window spec.
func resolveWindowSpec(spec *ast.WindowSpec, specs map[string]*ast.WindowSpec, inStack map[string]bool) error {
	if inStack[spec.Name.L] {
		return errors.Trace(ErrWindowCircularityInWindowGraph)
	}
	if spec.Ref.L == "" {
		return nil
	}
	ref, ok := specs[spec.Ref.L]
	if !ok {
		return ErrWindowNoSuchWindow.GenWithStackByArgs(spec.Ref.O)
	}
	inStack[spec.Name.L] = true
	err := resolveWindowSpec(ref, specs, inStack)
	if err != nil {
		return err
	}
	inStack[spec.Name.L] = false
	return mergeWindowSpec(spec, ref)
}

func mergeWindowSpec(spec, ref *ast.WindowSpec) error {
	if ref.Frame != nil {
		return ErrWindowNoInherentFrame.GenWithStackByArgs(ref.Name.O)
	}
	if spec.PartitionBy != nil {
		return errors.Trace(ErrWindowNoChildPartitioning)
	}
	if ref.OrderBy != nil {
		if spec.OrderBy != nil {
			return ErrWindowNoRedefineOrderBy.GenWithStackByArgs(getWindowName(spec.Name.O), ref.Name.O)
		}
		spec.OrderBy = ref.OrderBy
	}
	spec.PartitionBy = ref.PartitionBy
	spec.Ref = perceptron.NewCIStr("")
	return nil
}

func buildWindowSpecs(specs []ast.WindowSpec) (map[string]*ast.WindowSpec, error) {
	specsMap := make(map[string]*ast.WindowSpec, len(specs))
	for _, spec := range specs {
		if _, ok := specsMap[spec.Name.L]; ok {
			return nil, ErrWindowDuplicateName.GenWithStackByArgs(spec.Name.O)
		}
		newSpec := spec
		specsMap[spec.Name.L] = &newSpec
	}
	inStack := make(map[string]bool, len(specs))
	for _, spec := range specsMap {
		err := resolveWindowSpec(spec, specsMap, inStack)
		if err != nil {
			return nil, err
		}
	}
	return specsMap, nil
}

// extractBlockList extracts all the BlockNames from node.
// If asName is true, extract AsName prior to OrigName.
// Privilege check should use OrigName, while expression may use AsName.
func extractBlockList(node ast.ResultSetNode, input []*ast.BlockName, asName bool) []*ast.BlockName {
	switch x := node.(type) {
	case *ast.Join:
		input = extractBlockList(x.Left, input, asName)
		input = extractBlockList(x.Right, input, asName)
	case *ast.BlockSource:
		if s, ok := x.Source.(*ast.BlockName); ok {
			if x.AsName.L != "" && asName {
				newBlockName := *s
				newBlockName.Name = x.AsName
				newBlockName.Schema = perceptron.NewCIStr("")
				input = append(input, &newBlockName)
			} else {
				input = append(input, s)
			}
		}
	}
	return input
}

// extractBlockSourceAsNames extracts BlockSource.AsNames from node.
// if onlySelectStmt is set to be true, only extracts AsNames when BlockSource.Source.(type) == *ast.SelectStmt
func extractBlockSourceAsNames(node ast.ResultSetNode, input []string, onlySelectStmt bool) []string {
	switch x := node.(type) {
	case *ast.Join:
		input = extractBlockSourceAsNames(x.Left, input, onlySelectStmt)
		input = extractBlockSourceAsNames(x.Right, input, onlySelectStmt)
	case *ast.BlockSource:
		if _, ok := x.Source.(*ast.SelectStmt); !ok && onlySelectStmt {
			break
		}
		if s, ok := x.Source.(*ast.BlockName); ok {
			if x.AsName.L == "" {
				input = append(input, s.Name.L)
				break
			}
		}
		input = append(input, x.AsName.L)
	}
	return input
}

func appendVisitInfo(vi []visitInfo, priv allegrosql.PrivilegeType, EDB, tbl, col string, err error) []visitInfo {
	return append(vi, visitInfo{
		privilege: priv,
		EDB:       EDB,
		block:     tbl,
		column:    col,
		err:       err,
	})
}

func getInnerFromParenthesesAndUnaryPlus(expr ast.ExprNode) ast.ExprNode {
	if pexpr, ok := expr.(*ast.ParenthesesExpr); ok {
		return getInnerFromParenthesesAndUnaryPlus(pexpr.Expr)
	}
	if uexpr, ok := expr.(*ast.UnaryOperationExpr); ok && uexpr.Op == opcode.Plus {
		return getInnerFromParenthesesAndUnaryPlus(uexpr.V)
	}
	return expr
}

// containDifferentJoinTypes checks whether `preferJoinType` contains different
// join types.
func containDifferentJoinTypes(preferJoinType uint) bool {
	inlMask := preferRightAsINLJInner ^ preferLeftAsINLJInner
	inlhjMask := preferRightAsINLHJInner ^ preferLeftAsINLHJInner
	inlmjMask := preferRightAsINLMJInner ^ preferLeftAsINLMJInner

	mask := inlMask ^ inlhjMask ^ inlmjMask
	onesCount := bits.OnesCount(preferJoinType & ^mask)
	if onesCount > 1 || onesCount == 1 && preferJoinType&mask > 0 {
		return true
	}

	cnt := 0
	if preferJoinType&inlMask > 0 {
		cnt++
	}
	if preferJoinType&inlhjMask > 0 {
		cnt++
	}
	if preferJoinType&inlmjMask > 0 {
		cnt++
	}
	return cnt > 1
}
