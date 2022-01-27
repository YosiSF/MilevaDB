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
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

type aggregationEliminator struct {
	aggregationEliminateChecker
}

type aggregationEliminateChecker struct {
}

// tryToEliminateAggregation will eliminate aggregation grouped by unique key.
// e.g. select min(b) from t group by a. If a is a unique key, then this allegrosql is equal to `select b from t group by a`.
// For count(expr), sum(expr), avg(expr), count(distinct expr, [expr...]) we may need to rewrite the expr. Details are shown below.
// If we can eliminate agg successful, we return a projection. Else we return a nil pointer.
func (a *aggregationEliminateChecker) tryToEliminateAggregation(agg *LogicalAggregation) *LogicalProjection {
	for _, af := range agg.AggFuncs {
		// TODO(issue #9968): Actually, we can rewrite GROUP_CONCAT when all the
		// arguments it accepts are promised to be NOT-NULL.
		// When it accepts only 1 argument, we can extract this argument into a
		// projection.
		// When it accepts multiple arguments, we can wrap the arguments with a
		// function CONCAT_WS and extract this function into a projection.
		// BUT, GROUP_CONCAT should truncate the final result according to the
		// system variable `group_concat_max_len`. To ensure the correctness of
		// the result, we close the elimination of GROUP_CONCAT here.
		if af.Name == ast.AggFuncGroupConcat {
			return nil
		}
	}
	schemaByGroupby := expression.NewSchema(agg.groupByDefCauss...)
	coveredByUniqueKey := false
	for _, key := range agg.children[0].Schema().Keys {
		if schemaByGroupby.DeferredCausetsIndices(key) != nil {
			coveredByUniqueKey = true
			break
		}
	}
	if coveredByUniqueKey {
		// GroupByDefCauss has unique key, so this aggregation can be removed.
		if ok, proj := ConvertAggToProj(agg, agg.schemaReplicant); ok {
			proj.SetChildren(agg.children[0])
			return proj
		}
	}
	return nil
}

// ConvertAggToProj convert aggregation to projection.
func ConvertAggToProj(agg *LogicalAggregation, schemaReplicant *expression.Schema) (bool, *LogicalProjection) {
	proj := LogicalProjection{
		Exprs: make([]expression.Expression, 0, len(agg.AggFuncs)),
	}.Init(agg.ctx, agg.blockOffset)
	for _, fun := range agg.AggFuncs {
		ok, expr := rewriteExpr(agg.ctx, fun)
		if !ok {
			return false, nil
		}
		proj.Exprs = append(proj.Exprs, expr)
	}
	proj.SetSchema(schemaReplicant.Clone())
	return true, proj
}

// rewriteExpr will rewrite the aggregate function to expression doesn't contain aggregate function.
func rewriteExpr(ctx stochastikctx.Context, aggFunc *aggregation.AggFuncDesc) (bool, expression.Expression) {
	switch aggFunc.Name {
	case ast.AggFuncCount:
		if aggFunc.Mode == aggregation.FinalMode {
			return true, wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
		}
		return true, rewriteCount(ctx, aggFunc.Args, aggFunc.RetTp)
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncFirstRow, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncGroupConcat:
		return true, wrapCastFunction(ctx, aggFunc.Args[0], aggFunc.RetTp)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		return true, rewriteBitFunc(ctx, aggFunc.Name, aggFunc.Args[0], aggFunc.RetTp)
	default:
		return false, nil
	}
}

func rewriteCount(ctx stochastikctx.Context, exprs []expression.Expression, targetTp *types.FieldType) expression.Expression {
	// If is count(expr), we will change it to if(isnull(expr), 0, 1).
	// If is count(distinct x, y, z) we will change it to if(isnull(x) or isnull(y) or isnull(z), 0, 1).
	// If is count(expr not null), we will change it to constant 1.
	isNullExprs := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		if allegrosql.HasNotNullFlag(expr.GetType().Flag) {
			isNullExprs = append(isNullExprs, expression.NewZero())
		} else {
			isNullExpr := expression.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(allegrosql.TypeTiny), expr)
			isNullExprs = append(isNullExprs, isNullExpr)
		}
	}

	innerExpr := expression.ComposeDNFCondition(ctx, isNullExprs...)
	newExpr := expression.NewFunctionInternal(ctx, ast.If, targetTp, innerExpr, expression.NewZero(), expression.NewOne())
	return newExpr
}

func rewriteBitFunc(ctx stochastikctx.Context, funcType string, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	// For not integer type. We need to cast(cast(arg as signed) as unsigned) to make the bit function work.
	innerCast := expression.WrapWithCastAsInt(ctx, arg)
	outerCast := wrapCastFunction(ctx, innerCast, targetTp)
	var finalExpr expression.Expression
	if funcType != ast.AggFuncBitAnd {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, targetTp, outerCast, expression.NewZero())
	} else {
		finalExpr = expression.NewFunctionInternal(ctx, ast.Ifnull, outerCast.GetType(), outerCast, &expression.Constant{Value: types.NewUintCauset(math.MaxUint64), RetType: targetTp})
	}
	return finalExpr
}

// wrapCastFunction will wrap a cast if the targetTp is not equal to the arg's.
func wrapCastFunction(ctx stochastikctx.Context, arg expression.Expression, targetTp *types.FieldType) expression.Expression {
	if arg.GetType() == targetTp {
		return arg
	}
	return expression.BuildCastFunction(ctx, arg, targetTp)
}

func (a *aggregationEliminator) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := a.optimize(ctx, child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return p, nil
	}
	if proj := a.tryToEliminateAggregation(agg); proj != nil {
		return proj, nil
	}
	return p, nil
}

func (*aggregationEliminator) name() string {
	return "aggregation_eliminate"
}
