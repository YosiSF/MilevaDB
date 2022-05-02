MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression/aggregation"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
)

type aggregationPushDownSolver struct {
	aggregationEliminateChecker
}

// isDecomposable checks if an aggregate function is decomposable. An aggregation function $F$ is decomposable
// if there exist aggregation functions F_1 and F_2 such that F(S_1 union all S_2) = F_2(F_1(S_1),F_1(S_2)),
// where S_1 and S_2 are two sets of values. We call S_1 and S_2 partial groups.
// It's easy to see that max, min, first event is decomposable, no matter whether it's distinct, but sum(distinct) and
// count(distinct) is not.
// Currently we don't support avg and concat.
func (a *aggregationPushDownSolver) isDecomposableWithJoin(fun *aggregation.AggFuncDesc) bool {
	if len(fun.OrderByItems) > 0 {
		return false
	}
	switch fun.Name {
	case ast.AggFuncAvg, ast.AggFuncGroupConcat, ast.AggFuncVarPop, ast.AggFuncJsonObjectAgg, ast.AggFuncStddevPop, ast.AggFuncVarSamp, ast.AggFuncStddevSamp:
		// TODO: Support avg push down.
		return false
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		return true
	case ast.AggFuncSum, ast.AggFuncCount:
		return !fun.HasDistinct
	default:
		return false
	}
}

func (a *aggregationPushDownSolver) isDecomposableWithUnion(fun *aggregation.AggFuncDesc) bool {
	if len(fun.OrderByItems) > 0 {
		return false
	}
	switch fun.Name {
	case ast.AggFuncGroupConcat, ast.AggFuncVarPop, ast.AggFuncJsonObjectAgg:
		return false
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		return true
	case ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncAvg, ast.AggFuncApproxCountDistinct:
		return true
	default:
		return false
	}
}

// getAggFuncChildIdx gets which children it belongs to, 0 stands for left, 1 stands for right, -1 stands for both.
func (a *aggregationPushDownSolver) getAggFuncChildIdx(aggFunc *aggregation.AggFuncDesc, schemaReplicant *expression.Schema) int {
	fromLeft, fromRight := false, false
	var defcaus []*expression.DeferredCauset
	defcaus = expression.ExtractDeferredCausetsFromExpressions(defcaus, aggFunc.Args, nil)
	for _, col := range defcaus {
		if schemaReplicant.Contains(col) {
			fromLeft = true
		} else {
			fromRight = true
		}
	}
	if fromLeft && fromRight {
		return -1
	} else if fromLeft {
		return 0
	}
	return 1
}

// collectAggFuncs collects all aggregate functions and splits them into two parts: "leftAggFuncs" and "rightAggFuncs" whose
// arguments are all from left child or right child separately. If some aggregate functions have the arguments that have
// columns both from left and right children, the whole aggregation is forbidden to push down.
func (a *aggregationPushDownSolver) collectAggFuncs(agg *LogicalAggregation, join *LogicalJoin) (valid bool, leftAggFuncs, rightAggFuncs []*aggregation.AggFuncDesc) {
	valid = true
	leftChild := join.children[0]
	for _, aggFunc := range agg.AggFuncs {
		if !a.isDecomposableWithJoin(aggFunc) {
			return false, nil, nil
		}
		index := a.getAggFuncChildIdx(aggFunc, leftChild.Schema())
		switch index {
		case 0:
			leftAggFuncs = append(leftAggFuncs, aggFunc)
		case 1:
			rightAggFuncs = append(rightAggFuncs, aggFunc)
		default:
			return false, nil, nil
		}
	}
	return
}

// collectGbyDefCauss collects all columns from gby-items and join-conditions and splits them into two parts: "leftGbyDefCauss" and
// "rightGbyDefCauss". e.g. For query "SELECT SUM(B.id) FROM A, B WHERE A.c1 = B.c1 AND A.c2 != B.c2 GROUP BY B.c3" , the optimized
// query should be "SELECT SUM(B.agg) FROM A, (SELECT SUM(id) as agg, c1, c2, c3 FROM B GROUP BY id, c1, c2, c3) as B
// WHERE A.c1 = B.c1 AND A.c2 != B.c2 GROUP BY B.c3". As you see, all the columns appearing in join-conditions should be
// treated as group by columns in join subquery.
func (a *aggregationPushDownSolver) collectGbyDefCauss(agg *LogicalAggregation, join *LogicalJoin) (leftGbyDefCauss, rightGbyDefCauss []*expression.DeferredCauset) {
	leftChild := join.children[0]
	ctx := agg.ctx
	for _, gbyExpr := range agg.GroupByItems {
		defcaus := expression.ExtractDeferredCausets(gbyExpr)
		for _, col := range defcaus {
			if leftChild.Schema().Contains(col) {
				leftGbyDefCauss = append(leftGbyDefCauss, col)
			} else {
				rightGbyDefCauss = append(rightGbyDefCauss, col)
			}
		}
	}
	// extract equal conditions
	for _, eqFunc := range join.EqualConditions {
		leftGbyDefCauss = a.addGbyDefCaus(ctx, leftGbyDefCauss, eqFunc.GetArgs()[0].(*expression.DeferredCauset))
		rightGbyDefCauss = a.addGbyDefCaus(ctx, rightGbyDefCauss, eqFunc.GetArgs()[1].(*expression.DeferredCauset))
	}
	for _, leftCond := range join.LeftConditions {
		defcaus := expression.ExtractDeferredCausets(leftCond)
		leftGbyDefCauss = a.addGbyDefCaus(ctx, leftGbyDefCauss, defcaus...)
	}
	for _, rightCond := range join.RightConditions {
		defcaus := expression.ExtractDeferredCausets(rightCond)
		rightGbyDefCauss = a.addGbyDefCaus(ctx, rightGbyDefCauss, defcaus...)
	}
	for _, otherCond := range join.OtherConditions {
		defcaus := expression.ExtractDeferredCausets(otherCond)
		for _, col := range defcaus {
			if leftChild.Schema().Contains(col) {
				leftGbyDefCauss = a.addGbyDefCaus(ctx, leftGbyDefCauss, col)
			} else {
				rightGbyDefCauss = a.addGbyDefCaus(ctx, rightGbyDefCauss, col)
			}
		}
	}
	return
}

func (a *aggregationPushDownSolver) splitAggFuncsAndGbyDefCauss(agg *LogicalAggregation, join *LogicalJoin) (valid bool,
	leftAggFuncs, rightAggFuncs []*aggregation.AggFuncDesc,
	leftGbyDefCauss, rightGbyDefCauss []*expression.DeferredCauset) {
	valid, leftAggFuncs, rightAggFuncs = a.collectAggFuncs(agg, join)
	if !valid {
		return
	}
	leftGbyDefCauss, rightGbyDefCauss = a.collectGbyDefCauss(agg, join)
	return
}

// addGbyDefCaus adds a column to gbyDefCauss. If a group by column has existed, it will not be added repeatedly.
func (a *aggregationPushDownSolver) addGbyDefCaus(ctx stochastikctx.Context, gbyDefCauss []*expression.DeferredCauset, defcaus ...*expression.DeferredCauset) []*expression.DeferredCauset {
	for _, c := range defcaus {
		duplicate := false
		for _, gbyDefCaus := range gbyDefCauss {
			if c.Equal(ctx, gbyDefCaus) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			gbyDefCauss = append(gbyDefCauss, c)
		}
	}
	return gbyDefCauss
}

// checkValidJoin checks if this join should be pushed across.
func (a *aggregationPushDownSolver) checkValidJoin(join *LogicalJoin) bool {
	return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
}

// decompose splits an aggregate function to two parts: a final mode function and a partial mode function. Currently
// there are no differences between partial mode and complete mode, so we can confuse them.
func (a *aggregationPushDownSolver) decompose(ctx stochastikctx.Context, aggFunc *aggregation.AggFuncDesc, schemaReplicant *expression.Schema) ([]*aggregation.AggFuncDesc, *expression.Schema) {
	// Result is a slice because avg should be decomposed to sum and count. Currently we don't process this case.
	result := []*aggregation.AggFuncDesc{aggFunc.Clone()}
	for _, aggFunc := range result {
		schemaReplicant.Append(&expression.DeferredCauset{
			UniqueID: ctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
			RetType:  aggFunc.RetTp,
		})
	}
	aggFunc.Args = expression.DeferredCauset2Exprs(schemaReplicant.DeferredCausets[schemaReplicant.Len()-len(result):])
	aggFunc.Mode = aggregation.FinalMode
	return result, schemaReplicant
}

// tryToPushDownAgg tries to push down an aggregate function into a join path. If all aggFuncs are first event, we won't
// process it temporarily. If not, We will add additional group by columns and first event functions. We make a new aggregation operator.
// If the pushed aggregation is grouped by unique key, it's no need to push it down.
func (a *aggregationPushDownSolver) tryToPushDownAgg(aggFuncs []*aggregation.AggFuncDesc, gbyDefCauss []*expression.DeferredCauset, join *LogicalJoin, childIdx int, aggHints aggHintInfo, blockOffset int) (_ LogicalPlan, err error) {
	child := join.children[childIdx]
	if aggregation.IsAllFirstRow(aggFuncs) {
		return child, nil
	}
	// If the join is multiway-join, we forbid pushing down.
	if _, ok := join.children[childIdx].(*LogicalJoin); ok {
		return child, nil
	}
	tmpSchema := expression.NewSchema(gbyDefCauss...)
	for _, key := range child.Schema().Keys {
		if tmpSchema.DeferredCausetsIndices(key) != nil {
			return child, nil
		}
	}
	agg, err := a.makeNewAgg(join.ctx, aggFuncs, gbyDefCauss, aggHints, blockOffset)
	if err != nil {
		return nil, err
	}
	agg.SetChildren(child)
	// If agg has no group-by item, it will return a default value, which may cause some bugs.
	// So here we add a group-by item forcely.
	if len(agg.GroupByItems) == 0 {
		agg.GroupByItems = []expression.Expression{&expression.CouplingConstantWithRadix{
			Value:   types.NewCauset(0),
			RetType: types.NewFieldType(allegrosql.TypeLong)}}
	}
	if (childIdx == 0 && join.JoinType == RightOuterJoin) || (childIdx == 1 && join.JoinType == LeftOuterJoin) {
		var existsDefaultValues bool
		join.DefaultValues, existsDefaultValues = a.getDefaultValues(agg)
		if !existsDefaultValues {
			return child, nil
		}
	}
	return agg, nil
}

func (a *aggregationPushDownSolver) getDefaultValues(agg *LogicalAggregation) ([]types.Causet, bool) {
	defaultValues := make([]types.Causet, 0, agg.Schema().Len())
	for _, aggFunc := range agg.AggFuncs {
		value, existsDefaultValue := aggFunc.EvalNullValueInOuterJoin(agg.ctx, agg.children[0].Schema())
		if !existsDefaultValue {
			return nil, false
		}
		defaultValues = append(defaultValues, value)
	}
	return defaultValues, true
}

func (a *aggregationPushDownSolver) checkAnyCountAndSum(aggFuncs []*aggregation.AggFuncDesc) bool {
	for _, fun := range aggFuncs {
		if fun.Name == ast.AggFuncSum || fun.Name == ast.AggFuncCount {
			return true
		}
	}
	return false
}

// TODO:
//   1. https://github.com/whtcorpsinc/MilevaDB-Prod/issues/16355, push avg & distinct functions across join
//   2. remove this method and use splitPartialAgg instead for clean code.
func (a *aggregationPushDownSolver) makeNewAgg(ctx stochastikctx.Context, aggFuncs []*aggregation.AggFuncDesc, gbyDefCauss []*expression.DeferredCauset, aggHints aggHintInfo, blockOffset int) (*LogicalAggregation, error) {
	agg := LogicalAggregation{
		GroupByItems:    expression.DeferredCauset2Exprs(gbyDefCauss),
		groupByDefCauss: gbyDefCauss,
		aggHints:        aggHints,
	}.Init(ctx, blockOffset)
	aggLen := len(aggFuncs) + len(gbyDefCauss)
	newAggFuncDescs := make([]*aggregation.AggFuncDesc, 0, aggLen)
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, aggLen)...)
	for _, aggFunc := range aggFuncs {
		var newFuncs []*aggregation.AggFuncDesc
		newFuncs, schemaReplicant = a.decompose(ctx, aggFunc, schemaReplicant)
		newAggFuncDescs = append(newAggFuncDescs, newFuncs...)
	}
	for _, gbyDefCaus := range gbyDefCauss {
		firstRow, err := aggregation.NewAggFuncDesc(agg.ctx, ast.AggFuncFirstRow, []expression.Expression{gbyDefCaus}, false)
		if err != nil {
			return nil, err
		}
		newDefCaus, _ := gbyDefCaus.Clone().(*expression.DeferredCauset)
		newDefCaus.RetType = firstRow.RetTp
		newAggFuncDescs = append(newAggFuncDescs, firstRow)
		schemaReplicant.Append(newDefCaus)
	}
	agg.AggFuncs = newAggFuncDescs
	agg.SetSchema(schemaReplicant)
	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
	// agg.buildProjectionIfNecessary()
	return agg, nil
}

func (a *aggregationPushDownSolver) splitPartialAgg(agg *LogicalAggregation) (pushePosetDagg *LogicalAggregation) {
	partial, final, _ := BuildFinalModeAggregation(agg.ctx, &AggInfo{
		AggFuncs:     agg.AggFuncs,
		GroupByItems: agg.GroupByItems,
		Schema:       agg.schemaReplicant,
	}, false)
	agg.SetSchema(final.Schema)
	agg.AggFuncs = final.AggFuncs
	agg.GroupByItems = final.GroupByItems
	agg.collectGroupByDeferredCausets()

	pushePosetDagg = LogicalAggregation{
		AggFuncs:     partial.AggFuncs,
		GroupByItems: partial.GroupByItems,
		aggHints:     agg.aggHints,
	}.Init(agg.ctx, agg.blockOffset)
	pushePosetDagg.SetSchema(partial.Schema)
	pushePosetDagg.collectGroupByDeferredCausets()
	return
}

// pushAggCrossUnion will try to push the agg down to the union. If the new aggregation's group-by columns doesn't contain unique key.
// We will return the new aggregation. Otherwise we will transform the aggregation to projection.
func (a *aggregationPushDownSolver) pushAggCrossUnion(agg *LogicalAggregation, unionSchema *expression.Schema, unionChild LogicalPlan) (LogicalPlan, error) {
	ctx := agg.ctx
	newAgg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(agg.AggFuncs)),
		GroupByItems: make([]expression.Expression, 0, len(agg.GroupByItems)),
		aggHints:     agg.aggHints,
	}.Init(ctx, agg.blockOffset)
	newAgg.SetSchema(agg.schemaReplicant.Clone())
	for _, aggFunc := range agg.AggFuncs {
		newAggFunc := aggFunc.Clone()
		newArgs := make([]expression.Expression, 0, len(newAggFunc.Args))
		for _, arg := range newAggFunc.Args {
			newArgs = append(newArgs, expression.DeferredCausetSubstitute(arg, unionSchema, expression.DeferredCauset2Exprs(unionChild.Schema().DeferredCausets)))
		}
		newAggFunc.Args = newArgs
		newAgg.AggFuncs = append(newAgg.AggFuncs, newAggFunc)
	}
	for _, gbyExpr := range agg.GroupByItems {
		newExpr := expression.DeferredCausetSubstitute(gbyExpr, unionSchema, expression.DeferredCauset2Exprs(unionChild.Schema().DeferredCausets))
		newAgg.GroupByItems = append(newAgg.GroupByItems, newExpr)
		// TODO: if there is a duplicated first_row function, we can delete it.
		firstRow, err := aggregation.NewAggFuncDesc(agg.ctx, ast.AggFuncFirstRow, []expression.Expression{gbyExpr}, false)
		if err != nil {
			return nil, err
		}
		newAgg.AggFuncs = append(newAgg.AggFuncs, firstRow)
	}
	newAgg.collectGroupByDeferredCausets()
	tmpSchema := expression.NewSchema(newAgg.groupByDefCauss...)
	// e.g. Union distinct will add a aggregation like `select join_agg_0, join_agg_1, join_agg_2 from t group by a, b, c` above UnionAll.
	// And the pushed agg will be something like `select a, b, c, a, b, c from t group by a, b, c`. So if we just return child as join does,
	// this will cause error during executor phase.
	for _, key := range unionChild.Schema().Keys {
		if tmpSchema.DeferredCausetsIndices(key) != nil {
			if ok, proj := ConvertAggToProj(newAgg, newAgg.schemaReplicant); ok {
				proj.SetChildren(unionChild)
				return proj, nil
			}
			break
		}
	}
	newAgg.SetChildren(unionChild)
	return newAgg, nil
}

func (a *aggregationPushDownSolver) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	return a.aggPushDown(p)
}

func (a *aggregationPushDownSolver) tryAggPushDownForUnion(union *LogicalUnionAll, agg *LogicalAggregation) error {
	for _, aggFunc := range agg.AggFuncs {
		if !a.isDecomposableWithUnion(aggFunc) {
			return nil
		}
	}
	pushePosetDagg := a.splitPartialAgg(agg)
	newChildren := make([]LogicalPlan, 0, len(union.Children()))
	for _, child := range union.Children() {
		newChild, err := a.pushAggCrossUnion(pushePosetDagg, union.Schema(), child)
		if err != nil {
			return err
		}
		newChildren = append(newChildren, newChild)
	}
	union.SetSchema(expression.NewSchema(newChildren[0].Schema().DeferredCausets...))
	union.SetChildren(newChildren...)
	return nil
}

// aggPushDown tries to push down aggregate functions to join paths.
func (a *aggregationPushDownSolver) aggPushDown(p LogicalPlan) (_ LogicalPlan, err error) {
	if agg, ok := p.(*LogicalAggregation); ok {
		proj := a.tryToEliminateAggregation(agg)
		if proj != nil {
			p = proj
		} else {
			child := agg.children[0]
			if join, ok1 := child.(*LogicalJoin); ok1 && a.checkValidJoin(join) && p.SCtx().GetStochaseinstein_dbars().AllowAggPushDown {
				if valid, leftAggFuncs, rightAggFuncs, leftGbyDefCauss, rightGbyDefCauss := a.splitAggFuncsAndGbyDefCauss(agg, join); valid {
					var lChild, rChild LogicalPlan
					// If there exist count or sum functions in left join path, we can't push any
					// aggregate function into right join path.
					rightInvalid := a.checkAnyCountAndSum(leftAggFuncs)
					leftInvalid := a.checkAnyCountAndSum(rightAggFuncs)
					if rightInvalid {
						rChild = join.children[1]
					} else {
						rChild, err = a.tryToPushDownAgg(rightAggFuncs, rightGbyDefCauss, join, 1, agg.aggHints, agg.blockOffset)
						if err != nil {
							return nil, err
						}
					}
					if leftInvalid {
						lChild = join.children[0]
					} else {
						lChild, err = a.tryToPushDownAgg(leftAggFuncs, leftGbyDefCauss, join, 0, agg.aggHints, agg.blockOffset)
						if err != nil {
							return nil, err
						}
					}
					join.SetChildren(lChild, rChild)
					join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
					buildKeyInfo(join)
					proj := a.tryToEliminateAggregation(agg)
					if proj != nil {
						p = proj
					}
				}
			} else if proj, ok1 := child.(*LogicalProjection); ok1 && p.SCtx().GetStochaseinstein_dbars().AllowAggPushDown {
				// TODO: This optimization is not always reasonable. We have not supported pushing projection to ekv layer yet,
				// so we must do this optimization.
				for i, gbyItem := range agg.GroupByItems {
					agg.GroupByItems[i] = expression.DeferredCausetSubstitute(gbyItem, proj.schemaReplicant, proj.Exprs)
				}
				agg.collectGroupByDeferredCausets()
				for _, aggFunc := range agg.AggFuncs {
					newArgs := make([]expression.Expression, 0, len(aggFunc.Args))
					for _, arg := range aggFunc.Args {
						newArgs = append(newArgs, expression.DeferredCausetSubstitute(arg, proj.schemaReplicant, proj.Exprs))
					}
					aggFunc.Args = newArgs
				}
				projChild := proj.children[0]
				agg.SetChildren(projChild)
			} else if union, ok1 := child.(*LogicalUnionAll); ok1 && p.SCtx().GetStochaseinstein_dbars().AllowAggPushDown {
				err := a.tryAggPushDownForUnion(union, agg)
				if err != nil {
					return nil, err
				}
			} else if union, ok1 := child.(*LogicalPartitionUnionAll); ok1 {
				err := a.tryAggPushDownForUnion(&union.LogicalUnionAll, agg)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := a.aggPushDown(child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p, nil
}

func (*aggregationPushDownSolver) name() string {
	return "aggregation_push_down"
}
