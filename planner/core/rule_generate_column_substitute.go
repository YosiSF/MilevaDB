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
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
)

type gcSubstituter struct {
}

// ExprDeferredCausetMap is used to causetstore all expressions of indexed generated columns in a block,
// and map them to the generated columns,
// thus we can substitute the expression in a query to an indexed generated column.
type ExprDeferredCausetMap map[expression.Expression]*expression.DeferredCauset

// optimize try to replace the expression to indexed virtual generate column in where, group by, order by, and field clause
// so that we can use the index on expression.
// For example: select a+1 from t order by a+1, with a virtual generate column c as (a+1) and
// an index on c. We need to replace a+1 with c so that we can use the index on c.
// See also https://dev.allegrosql.com/doc/refman/8.0/en/generated-column-index-optimizations.html
func (gc *gcSubstituter) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	exprToDeferredCauset := make(ExprDeferredCausetMap)
	collectGenerateDeferredCauset(lp, exprToDeferredCauset)
	if len(exprToDeferredCauset) == 0 {
		return lp, nil
	}
	return gc.substitute(ctx, lp, exprToDeferredCauset), nil
}

// collectGenerateDeferredCauset collect the generate column and save them to a map from their expressions to themselves.
// For the sake of simplicity, we don't collect the stored generate column because we can't get their expressions directly.
// TODO: support stored generate column.
func collectGenerateDeferredCauset(lp LogicalPlan, exprToDeferredCauset ExprDeferredCausetMap) {
	for _, child := range lp.Children() {
		collectGenerateDeferredCauset(child, exprToDeferredCauset)
	}
	ds, ok := lp.(*DataSource)
	if !ok {
		return
	}
	tblInfo := ds.blockInfo
	for _, idx := range tblInfo.Indices {
		for _, idxPart := range idx.DeferredCausets {
			colInfo := tblInfo.DeferredCausets[idxPart.Offset]
			if colInfo.IsGenerated() && !colInfo.GeneratedStored {
				s := ds.schemaReplicant.DeferredCausets
				col := expression.DefCausInfo2DefCaus(s, colInfo)
				if col != nil && col.GetType().Equal(col.VirtualExpr.GetType()) {
					exprToDeferredCauset[col.VirtualExpr] = col
				}
			}
		}
	}
}

func tryToSubstituteExpr(expr *expression.Expression, sctx stochastikctx.Context, candidateExpr expression.Expression, tp types.EvalType, schemaReplicant *expression.Schema, col *expression.DeferredCauset) {
	if (*expr).Equal(sctx, candidateExpr) && candidateExpr.GetType().EvalType() == tp &&
		schemaReplicant.DeferredCausetIndex(col) != -1 {
		*expr = col
	}
}

func (gc *gcSubstituter) substitute(ctx context.Context, lp LogicalPlan, exprToDeferredCauset ExprDeferredCausetMap) LogicalPlan {
	sctx := lp.SCtx().GetStochaseinstein_dbars().StmtCtx
	var expr *expression.Expression
	var tp types.EvalType
	switch x := lp.(type) {
	case *LogicalSelection:
		for _, cond := range x.Conditions {
			sf, ok := cond.(*expression.ScalarFunction)
			if !ok {
				continue
			}
			switch sf.FuncName.L {
			case ast.EQ, ast.LT, ast.LE, ast.GT, ast.GE:
				if sf.GetArgs()[0].ConstItem(sctx) {
					expr = &sf.GetArgs()[1]
					tp = sf.GetArgs()[0].GetType().EvalType()
				} else if sf.GetArgs()[1].ConstItem(sctx) {
					expr = &sf.GetArgs()[0]
					tp = sf.GetArgs()[1].GetType().EvalType()
				} else {
					continue
				}
				for candidateExpr, column := range exprToDeferredCauset {
					tryToSubstituteExpr(expr, lp.SCtx(), candidateExpr, tp, x.Schema(), column)
				}
			case ast.In:
				expr = &sf.GetArgs()[0]
				tp = sf.GetArgs()[1].GetType().EvalType()
				canSubstitute := true
				// Can only substitute if all the operands on the right-hand
				// side are constants of the same type.
				for i := 1; i < len(sf.GetArgs()); i++ {
					if !sf.GetArgs()[i].ConstItem(sctx) || sf.GetArgs()[i].GetType().EvalType() != tp {
						canSubstitute = false
						break
					}
				}
				if canSubstitute {
					for candidateExpr, column := range exprToDeferredCauset {
						tryToSubstituteExpr(expr, lp.SCtx(), candidateExpr, tp, x.Schema(), column)
					}
				}
			}
		}
	case *LogicalProjection:
		for i := range x.Exprs {
			tp = x.Exprs[i].GetType().EvalType()
			for candidateExpr, column := range exprToDeferredCauset {
				tryToSubstituteExpr(&x.Exprs[i], lp.SCtx(), candidateExpr, tp, x.children[0].Schema(), column)
			}
		}
	case *LogicalSort:
		for i := range x.ByItems {
			tp = x.ByItems[i].Expr.GetType().EvalType()
			for candidateExpr, column := range exprToDeferredCauset {
				tryToSubstituteExpr(&x.ByItems[i].Expr, lp.SCtx(), candidateExpr, tp, x.Schema(), column)
			}
		}
		// TODO: Uncomment these code after we support virtual generate column push down.
		//case *LogicalAggregation:
		//	for _, aggFunc := range x.AggFuncs {
		//		for i := 0; i < len(aggFunc.Args); i++ {
		//			tp = aggFunc.Args[i].GetType().EvalType()
		//			for candidateExpr, column := range exprToDeferredCauset {
		//				if aggFunc.Args[i].Equal(lp.SCtx(), candidateExpr) && candidateExpr.GetType().EvalType() == tp &&
		//					x.Schema().DeferredCausetIndex(column) != -1 {
		//					aggFunc.Args[i] = column
		//				}
		//			}
		//		}
		//	}
		//	for i := 0; i < len(x.GroupByItems); i++ {
		//		tp = x.GroupByItems[i].GetType().EvalType()
		//		for candidateExpr, column := range exprToDeferredCauset {
		//			if x.GroupByItems[i].Equal(lp.SCtx(), candidateExpr) && candidateExpr.GetType().EvalType() == tp &&
		//				x.Schema().DeferredCausetIndex(column) != -1 {
		//				x.GroupByItems[i] = column
		//				x.groupByDefCauss = append(x.groupByDefCauss, column)
		//			}
		//		}
		//	}
	}
	for _, child := range lp.Children() {
		gc.substitute(ctx, child, exprToDeferredCauset)
	}
	return lp
}

func (*gcSubstituter) name() string {
	return "generate_column_substitute"
}
