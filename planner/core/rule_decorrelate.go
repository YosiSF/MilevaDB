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
	"math"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	"github.com/whtcorpsinc/milevadb/types"
)

// canPullUpAgg checks if an apply can pull an aggregation up.
func (la *LogicalApply) canPullUpAgg() bool {
	if la.JoinType != InnerJoin && la.JoinType != LeftOuterJoin {
		return false
	}
	if len(la.EqualConditions)+len(la.LeftConditions)+len(la.RightConditions)+len(la.OtherConditions) > 0 {
		return false
	}
	return len(la.children[0].Schema().Keys) > 0
}

// canPullUp checks if an aggregation can be pulled up. An aggregate function like count(*) cannot be pulled up.
func (la *LogicalAggregation) canPullUp() bool {
	if len(la.GroupByItems) > 0 {
		return false
	}
	for _, f := range la.AggFuncs {
		for _, arg := range f.Args {
			expr := expression.EvaluateExprWithNull(la.ctx, la.children[0].Schema(), arg)
			if con, ok := expr.(*expression.Constant); !ok || !con.Value.IsNull() {
				return false
			}
		}
	}
	return true
}

// deCorDefCausFromEqExpr checks whether it's an equal condition of form `col = correlated col`. If so we will change the decorrelated
// column to normal column to make a new equal condition.
func (la *LogicalApply) deCorDefCausFromEqExpr(expr expression.Expression) expression.Expression {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return nil
	}
	if col, lOk := sf.GetArgs()[0].(*expression.DeferredCauset); lOk {
		if corDefCaus, rOk := sf.GetArgs()[1].(*expression.CorrelatedDeferredCauset); rOk {
			ret := corDefCaus.Decorrelate(la.Schema())
			if _, ok := ret.(*expression.CorrelatedDeferredCauset); ok {
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			return expression.NewFunctionInternal(la.ctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), ret, col)
		}
	}
	if corDefCaus, lOk := sf.GetArgs()[0].(*expression.CorrelatedDeferredCauset); lOk {
		if col, rOk := sf.GetArgs()[1].(*expression.DeferredCauset); rOk {
			ret := corDefCaus.Decorrelate(la.Schema())
			if _, ok := ret.(*expression.CorrelatedDeferredCauset); ok {
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			return expression.NewFunctionInternal(la.ctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), ret, col)
		}
	}
	return nil
}

// ExtractCorrelatedDefCauss4LogicalPlan recursively extracts all of the correlated columns
// from a plan tree by calling LogicalPlan.ExtractCorrelatedDefCauss.
func ExtractCorrelatedDefCauss4LogicalPlan(p LogicalPlan) []*expression.CorrelatedDeferredCauset {
	corDefCauss := p.ExtractCorrelatedDefCauss()
	for _, child := range p.Children() {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4LogicalPlan(child)...)
	}
	return corDefCauss
}

// ExtractCorrelatedDefCauss4PhysicalPlan recursively extracts all of the correlated columns
// from a plan tree by calling PhysicalPlan.ExtractCorrelatedDefCauss.
func ExtractCorrelatedDefCauss4PhysicalPlan(p PhysicalPlan) []*expression.CorrelatedDeferredCauset {
	corDefCauss := p.ExtractCorrelatedDefCauss()
	for _, child := range p.Children() {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalPlan(child)...)
	}
	return corDefCauss
}

// decorrelateSolver tries to convert apply plan to join plan.
type decorrelateSolver struct{}

func (s *decorrelateSolver) aggDefaultValueMap(agg *LogicalAggregation) map[int]*expression.Constant {
	defaultValueMap := make(map[int]*expression.Constant, len(agg.AggFuncs))
	for i, f := range agg.AggFuncs {
		switch f.Name {
		case ast.AggFuncBitOr, ast.AggFuncBitXor, ast.AggFuncCount:
			defaultValueMap[i] = expression.NewZero()
		case ast.AggFuncBitAnd:
			defaultValueMap[i] = &expression.Constant{Value: types.NewUintCauset(math.MaxUint64), RetType: types.NewFieldType(allegrosql.TypeLonglong)}
		}
	}
	return defaultValueMap
}

// optimize implements logicalOptMemrule interface.
func (s *decorrelateSolver) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	if apply, ok := p.(*LogicalApply); ok {
		outerPlan := apply.children[0]
		innerPlan := apply.children[1]
		apply.CorDefCauss = extractCorDeferredCausetsBySchema4LogicalPlan(apply.children[1], apply.children[0].Schema())
		if len(apply.CorDefCauss) == 0 {
			// If the inner plan is non-correlated, the apply will be simplified to join.
			join := &apply.LogicalJoin
			join.self = join
			p = join
		} else if sel, ok := innerPlan.(*LogicalSelection); ok {
			// If the inner plan is a selection, we add this condition to join predicates.
			// Notice that no matter what HoTT of join is, it's always right.
			newConds := make([]expression.Expression, 0, len(sel.Conditions))
			for _, cond := range sel.Conditions {
				newConds = append(newConds, cond.Decorrelate(outerPlan.Schema()))
			}
			apply.AttachOnConds(newConds)
			innerPlan = sel.children[0]
			apply.SetChildren(outerPlan, innerPlan)
			return s.optimize(ctx, p)
		} else if m, ok := innerPlan.(*LogicalMaxOneRow); ok {
			if m.children[0].MaxOneRow() {
				innerPlan = m.children[0]
				apply.SetChildren(outerPlan, innerPlan)
				return s.optimize(ctx, p)
			}
		} else if proj, ok := innerPlan.(*LogicalProjection); ok {
			for i, expr := range proj.Exprs {
				proj.Exprs[i] = expr.Decorrelate(outerPlan.Schema())
			}
			apply.columnSubstitute(proj.Schema(), proj.Exprs)
			innerPlan = proj.children[0]
			apply.SetChildren(outerPlan, innerPlan)
			if apply.JoinType != SemiJoin && apply.JoinType != LeftOuterSemiJoin && apply.JoinType != AntiSemiJoin && apply.JoinType != AntiLeftOuterSemiJoin {
				proj.SetSchema(apply.Schema())
				proj.Exprs = append(expression.DeferredCauset2Exprs(outerPlan.Schema().Clone().DeferredCausets), proj.Exprs...)
				apply.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
				np, err := s.optimize(ctx, p)
				if err != nil {
					return nil, err
				}
				proj.SetChildren(np)
				return proj, nil
			}
			return s.optimize(ctx, p)
		} else if agg, ok := innerPlan.(*LogicalAggregation); ok {
			if apply.canPullUpAgg() && agg.canPullUp() {
				innerPlan = agg.children[0]
				apply.JoinType = LeftOuterJoin
				apply.SetChildren(outerPlan, innerPlan)
				agg.SetSchema(apply.Schema())
				agg.GroupByItems = expression.DeferredCauset2Exprs(outerPlan.Schema().Keys[0])
				newAggFuncs := make([]*aggregation.AggFuncDesc, 0, apply.Schema().Len())

				outerDefCaussInSchema := make([]*expression.DeferredCauset, 0, outerPlan.Schema().Len())
				for i, col := range outerPlan.Schema().DeferredCausets {
					first, err := aggregation.NewAggFuncDesc(agg.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
					if err != nil {
						return nil, err
					}
					newAggFuncs = append(newAggFuncs, first)

					outerDefCaus, _ := outerPlan.Schema().DeferredCausets[i].Clone().(*expression.DeferredCauset)
					outerDefCaus.RetType = first.RetTp
					outerDefCaussInSchema = append(outerDefCaussInSchema, outerDefCaus)
				}
				apply.SetSchema(expression.MergeSchema(expression.NewSchema(outerDefCaussInSchema...), innerPlan.Schema()))
				resetNotNullFlag(apply.schemaReplicant, outerPlan.Schema().Len(), apply.schemaReplicant.Len())

				for i, aggFunc := range agg.AggFuncs {
					if idx := apply.schemaReplicant.DeferredCausetIndex(aggFunc.Args[0].(*expression.DeferredCauset)); idx != -1 {
						desc, err := aggregation.NewAggFuncDesc(agg.ctx, agg.AggFuncs[i].Name, []expression.Expression{apply.schemaReplicant.DeferredCausets[idx]}, false)
						if err != nil {
							return nil, err
						}
						newAggFuncs = append(newAggFuncs, desc)
					}
				}
				agg.AggFuncs = newAggFuncs
				np, err := s.optimize(ctx, p)
				if err != nil {
					return nil, err
				}
				agg.SetChildren(np)
				// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
				// agg.buildProjectionIfNecessary()
				agg.collectGroupByDeferredCausets()
				return agg, nil
			}
			// We can pull up the equal conditions below the aggregation as the join key of the apply, if only
			// the equal conditions contain the correlated column of this apply.
			if sel, ok := agg.children[0].(*LogicalSelection); ok && apply.JoinType == LeftOuterJoin {
				var (
					eqCondWithCorDefCaus []*expression.ScalarFunction
					remainedExpr         []expression.Expression
				)
				// Extract the equal condition.
				for _, cond := range sel.Conditions {
					if expr := apply.deCorDefCausFromEqExpr(cond); expr != nil {
						eqCondWithCorDefCaus = append(eqCondWithCorDefCaus, expr.(*expression.ScalarFunction))
					} else {
						remainedExpr = append(remainedExpr, cond)
					}
				}
				if len(eqCondWithCorDefCaus) > 0 {
					originalExpr := sel.Conditions
					sel.Conditions = remainedExpr
					apply.CorDefCauss = extractCorDeferredCausetsBySchema4LogicalPlan(apply.children[1], apply.children[0].Schema())
					// There's no other correlated column.
					groupByDefCauss := expression.NewSchema(agg.groupByDefCauss...)
					if len(apply.CorDefCauss) == 0 {
						join := &apply.LogicalJoin
						join.EqualConditions = append(join.EqualConditions, eqCondWithCorDefCaus...)
						for _, eqCond := range eqCondWithCorDefCaus {
							clonedDefCaus := eqCond.GetArgs()[1].(*expression.DeferredCauset)
							// If the join key is not in the aggregation's schemaReplicant, add first event function.
							if agg.schemaReplicant.DeferredCausetIndex(eqCond.GetArgs()[1].(*expression.DeferredCauset)) == -1 {
								newFunc, err := aggregation.NewAggFuncDesc(apply.ctx, ast.AggFuncFirstRow, []expression.Expression{clonedDefCaus}, false)
								if err != nil {
									return nil, err
								}
								agg.AggFuncs = append(agg.AggFuncs, newFunc)
								agg.schemaReplicant.Append(clonedDefCaus)
								agg.schemaReplicant.DeferredCausets[agg.schemaReplicant.Len()-1].RetType = newFunc.RetTp
							}
							// If group by defcaus don't contain the join key, add it into this.
							if !groupByDefCauss.Contains(clonedDefCaus) {
								agg.GroupByItems = append(agg.GroupByItems, clonedDefCaus)
								groupByDefCauss.Append(clonedDefCaus)
							}
						}
						agg.collectGroupByDeferredCausets()
						// The selection may be useless, check and remove it.
						if len(sel.Conditions) == 0 {
							agg.SetChildren(sel.children[0])
						}
						defaultValueMap := s.aggDefaultValueMap(agg)
						// We should use it directly, rather than building a projection.
						if len(defaultValueMap) > 0 {
							proj := LogicalProjection{}.Init(agg.ctx, agg.blockOffset)
							proj.SetSchema(apply.schemaReplicant)
							proj.Exprs = expression.DeferredCauset2Exprs(apply.schemaReplicant.DeferredCausets)
							for i, val := range defaultValueMap {
								pos := proj.schemaReplicant.DeferredCausetIndex(agg.schemaReplicant.DeferredCausets[i])
								ifNullFunc := expression.NewFunctionInternal(agg.ctx, ast.Ifnull, types.NewFieldType(allegrosql.TypeLonglong), agg.schemaReplicant.DeferredCausets[i], val)
								proj.Exprs[pos] = ifNullFunc
							}
							proj.SetChildren(apply)
							p = proj
						}
						return s.optimize(ctx, p)
					}
					sel.Conditions = originalExpr
					apply.CorDefCauss = extractCorDeferredCausetsBySchema4LogicalPlan(apply.children[1], apply.children[0].Schema())
				}
			}
		} else if sort, ok := innerPlan.(*LogicalSort); ok {
			// Since we only pull up Selection, Projection, Aggregation, MaxOneRow,
			// the top level Sort has no effect on the subquery's result.
			innerPlan = sort.children[0]
			apply.SetChildren(outerPlan, innerPlan)
			return s.optimize(ctx, p)
		}
	}
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		np, err := s.optimize(ctx, child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, np)
	}
	p.SetChildren(newChildren...)
	return p, nil
}

func (*decorrelateSolver) name() string {
	return "decorrelate"
}
