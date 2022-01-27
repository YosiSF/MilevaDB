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
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// injectExtraProjection is used to extract the expressions of specific
// operators into a physical Projection operator and inject the Projection below
// the operators. Thus we can accelerate the expression evaluation by eager
// evaluation.
func injectExtraProjection(plan PhysicalPlan) PhysicalPlan {
	return NewProjInjector().inject(plan)
}

type projInjector struct {
}

// NewProjInjector builds a projInjector.
func NewProjInjector() *projInjector {
	return &projInjector{}
}

func (pe *projInjector) inject(plan PhysicalPlan) PhysicalPlan {
	for i, child := range plan.Children() {
		plan.Children()[i] = pe.inject(child)
	}

	switch p := plan.(type) {
	case *PhysicalHashAgg:
		plan = InjectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalStreamAgg:
		plan = InjectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalSort:
		plan = InjectProjBelowSort(p, p.ByItems)
	case *PhysicalTopN:
		plan = InjectProjBelowSort(p, p.ByItems)
	case *NominalSort:
		plan = TurnNominalSortIntoProj(p, p.OnlyDeferredCauset, p.ByItems)
	}
	return plan
}

// wrapCastForAggFunc wraps the args of an aggregate function with a cast function.
// If the mode is FinalMode or Partial2Mode, we do not need to wrap cast upon the args,
// since the types of the args are already the expected.
func wrapCastForAggFuncs(sctx stochastikctx.Context, aggFuncs []*aggregation.AggFuncDesc) {
	for i := range aggFuncs {
		if aggFuncs[i].Mode != aggregation.FinalMode && aggFuncs[i].Mode != aggregation.Partial2Mode {
			aggFuncs[i].WrapCastForAggArgs(sctx)
		}
	}
}

// InjectProjBelowAgg injects a ProjOperator below AggOperator. So that All
// scalar functions in aggregation may speed up by vectorized evaluation in
// the `proj`. If all the args of `aggFuncs`, and all the item of `groupByItems`
// are columns or constants, we do not need to build the `proj`.
func InjectProjBelowAgg(aggPlan PhysicalPlan, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) PhysicalPlan {
	hasScalarFunc := false

	wrapCastForAggFuncs(aggPlan.SCtx(), aggFuncs)
	for i := 0; !hasScalarFunc && i < len(aggFuncs); i++ {
		for _, arg := range aggFuncs[i].Args {
			_, isScalarFunc := arg.(*expression.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
		for _, byItem := range aggFuncs[i].OrderByItems {
			_, isScalarFunc := byItem.Expr.(*expression.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
	}
	for i := 0; !hasScalarFunc && i < len(groupByItems); i++ {
		_, isScalarFunc := groupByItems[i].(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return aggPlan
	}

	projSchemaDefCauss := make([]*expression.DeferredCauset, 0, len(aggFuncs)+len(groupByItems))
	projExprs := make([]expression.Expression, 0, cap(projSchemaDefCauss))
	cursor := 0

	for _, f := range aggFuncs {
		for i, arg := range f.Args {
			if _, isCnst := arg.(*expression.Constant); isCnst {
				continue
			}
			projExprs = append(projExprs, arg)
			newArg := &expression.DeferredCauset{
				UniqueID: aggPlan.SCtx().GetStochastikVars().AllocPlanDeferredCausetID(),
				RetType:  arg.GetType(),
				Index:    cursor,
			}
			projSchemaDefCauss = append(projSchemaDefCauss, newArg)
			f.Args[i] = newArg
			cursor++
		}
		for _, byItem := range f.OrderByItems {
			if _, isCnst := byItem.Expr.(*expression.Constant); isCnst {
				continue
			}
			projExprs = append(projExprs, byItem.Expr)
			newArg := &expression.DeferredCauset{
				UniqueID: aggPlan.SCtx().GetStochastikVars().AllocPlanDeferredCausetID(),
				RetType:  byItem.Expr.GetType(),
				Index:    cursor,
			}
			projSchemaDefCauss = append(projSchemaDefCauss, newArg)
			byItem.Expr = newArg
			cursor++
		}
	}

	for i, item := range groupByItems {
		if _, isCnst := item.(*expression.Constant); isCnst {
			continue
		}
		projExprs = append(projExprs, item)
		newArg := &expression.DeferredCauset{
			UniqueID: aggPlan.SCtx().GetStochastikVars().AllocPlanDeferredCausetID(),
			RetType:  item.GetType(),
			Index:    cursor,
		}
		projSchemaDefCauss = append(projSchemaDefCauss, newArg)
		groupByItems[i] = newArg
		cursor++
	}

	child := aggPlan.Children()[0]
	prop := aggPlan.GetChildReqProps(0).Clone()
	proj := PhysicalProjection{
		Exprs:                        projExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(aggPlan.SCtx(), child.statsInfo().ScaleByExpectCnt(prop.ExpectedCnt), aggPlan.SelectBlockOffset(), prop)
	proj.SetSchema(expression.NewSchema(projSchemaDefCauss...))
	proj.SetChildren(child)

	aggPlan.SetChildren(proj)
	return aggPlan
}

// InjectProjBelowSort extracts the ScalarFunctions of `orderByItems` into a
// PhysicalProjection and injects it below PhysicalTopN/PhysicalSort. The schemaReplicant
// of PhysicalSort and PhysicalTopN are the same as the schemaReplicant of their
// children. When a projection is injected as the child of PhysicalSort and
// PhysicalTopN, some extra columns will be added into the schemaReplicant of the
// Projection, thus we need to add another Projection upon them to prune the
// redundant columns.
func InjectProjBelowSort(p PhysicalPlan, orderByItems []*soliton.ByItems) PhysicalPlan {
	hasScalarFunc, numOrderByItems := false, len(orderByItems)
	for i := 0; !hasScalarFunc && i < numOrderByItems; i++ {
		_, isScalarFunc := orderByItems[i].Expr.(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return p
	}

	topProjExprs := make([]expression.Expression, 0, p.Schema().Len())
	for i := range p.Schema().DeferredCausets {
		col := p.Schema().DeferredCausets[i].Clone().(*expression.DeferredCauset)
		col.Index = i
		topProjExprs = append(topProjExprs, col)
	}
	topProj := PhysicalProjection{
		Exprs:                        topProjExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(p.SCtx(), p.statsInfo(), p.SelectBlockOffset(), nil)
	topProj.SetSchema(p.Schema().Clone())
	topProj.SetChildren(p)

	childPlan := p.Children()[0]
	bottomProjSchemaDefCauss := make([]*expression.DeferredCauset, 0, len(childPlan.Schema().DeferredCausets)+numOrderByItems)
	bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().DeferredCausets)+numOrderByItems)
	for _, col := range childPlan.Schema().DeferredCausets {
		newDefCaus := col.Clone().(*expression.DeferredCauset)
		newDefCaus.Index = childPlan.Schema().DeferredCausetIndex(newDefCaus)
		bottomProjSchemaDefCauss = append(bottomProjSchemaDefCauss, newDefCaus)
		bottomProjExprs = append(bottomProjExprs, newDefCaus)
	}

	for _, item := range orderByItems {
		itemExpr := item.Expr
		if _, isScalarFunc := itemExpr.(*expression.ScalarFunction); !isScalarFunc {
			continue
		}
		bottomProjExprs = append(bottomProjExprs, itemExpr)
		newArg := &expression.DeferredCauset{
			UniqueID: p.SCtx().GetStochastikVars().AllocPlanDeferredCausetID(),
			RetType:  itemExpr.GetType(),
			Index:    len(bottomProjSchemaDefCauss),
		}
		bottomProjSchemaDefCauss = append(bottomProjSchemaDefCauss, newArg)
		item.Expr = newArg
	}

	childProp := p.GetChildReqProps(0).Clone()
	bottomProj := PhysicalProjection{
		Exprs:                        bottomProjExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(p.SCtx(), childPlan.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.SelectBlockOffset(), childProp)
	bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaDefCauss...))
	bottomProj.SetChildren(childPlan)
	p.SetChildren(bottomProj)

	if origChildProj, isChildProj := childPlan.(*PhysicalProjection); isChildProj {
		refine4NeighbourProj(bottomProj, origChildProj)
	}

	return topProj
}

// TurnNominalSortIntoProj will turn nominal sort into two projections. This is to check if the scalar functions will
// overflow.
func TurnNominalSortIntoProj(p PhysicalPlan, onlyDeferredCauset bool, orderByItems []*soliton.ByItems) PhysicalPlan {
	if onlyDeferredCauset {
		return p.Children()[0]
	}

	numOrderByItems := len(orderByItems)
	childPlan := p.Children()[0]

	bottomProjSchemaDefCauss := make([]*expression.DeferredCauset, 0, len(childPlan.Schema().DeferredCausets)+numOrderByItems)
	bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().DeferredCausets)+numOrderByItems)
	for _, col := range childPlan.Schema().DeferredCausets {
		newDefCaus := col.Clone().(*expression.DeferredCauset)
		newDefCaus.Index = childPlan.Schema().DeferredCausetIndex(newDefCaus)
		bottomProjSchemaDefCauss = append(bottomProjSchemaDefCauss, newDefCaus)
		bottomProjExprs = append(bottomProjExprs, newDefCaus)
	}

	for _, item := range orderByItems {
		itemExpr := item.Expr
		if _, isScalarFunc := itemExpr.(*expression.ScalarFunction); !isScalarFunc {
			continue
		}
		bottomProjExprs = append(bottomProjExprs, itemExpr)
		newArg := &expression.DeferredCauset{
			UniqueID: p.SCtx().GetStochastikVars().AllocPlanDeferredCausetID(),
			RetType:  itemExpr.GetType(),
			Index:    len(bottomProjSchemaDefCauss),
		}
		bottomProjSchemaDefCauss = append(bottomProjSchemaDefCauss, newArg)
	}

	childProp := p.GetChildReqProps(0).Clone()
	bottomProj := PhysicalProjection{
		Exprs:                        bottomProjExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(p.SCtx(), childPlan.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.SelectBlockOffset(), childProp)
	bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaDefCauss...))
	bottomProj.SetChildren(childPlan)

	topProjExprs := make([]expression.Expression, 0, childPlan.Schema().Len())
	for i := range childPlan.Schema().DeferredCausets {
		col := childPlan.Schema().DeferredCausets[i].Clone().(*expression.DeferredCauset)
		col.Index = i
		topProjExprs = append(topProjExprs, col)
	}
	topProj := PhysicalProjection{
		Exprs:                        topProjExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(p.SCtx(), childPlan.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.SelectBlockOffset(), childProp)
	topProj.SetSchema(childPlan.Schema().Clone())
	topProj.SetChildren(bottomProj)

	if origChildProj, isChildProj := childPlan.(*PhysicalProjection); isChildProj {
		refine4NeighbourProj(bottomProj, origChildProj)
	}

	return topProj
}
