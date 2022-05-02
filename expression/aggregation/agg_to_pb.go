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

package aggregation

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// AggFuncToPBExpr converts aggregate function to pb.
func AggFuncToPBExpr(sc *stmtctx.StatementContext, client ekv.Client, aggFunc *AggFuncDesc) *fidelpb.Expr {
	// if aggFunc.HasDistinct {
	// do nothing and ignore aggFunc.HasDistinct
	// }
	if len(aggFunc.OrderByItems) > 0 {
		return nil
	}
	pc := expression.NewPBConverter(client, sc)
	var tp fidelpb.ExprType
	switch aggFunc.Name {
	case ast.AggFuncCount:
		tp = fidelpb.ExprType_Count
	case ast.AggFuncApproxCountDistinct:
		tp = fidelpb.ExprType_ApproxCountDistinct
	case ast.AggFuncFirstEvent:
		tp = fidelpb.ExprType_First
	case ast.AggFuncGroupConcat:
		tp = fidelpb.ExprType_GroupConcat
	case ast.AggFuncMax:
		tp = fidelpb.ExprType_Max
	case ast.AggFuncMin:
		tp = fidelpb.ExprType_Min
	case ast.AggFuncSum:
		tp = fidelpb.ExprType_Sum
	case ast.AggFuncAvg:
		tp = fidelpb.ExprType_Avg
	case ast.AggFuncBitOr:
		tp = fidelpb.ExprType_Agg_BitOr
	case ast.AggFuncBitXor:
		tp = fidelpb.ExprType_Agg_BitXor
	case ast.AggFuncBitAnd:
		tp = fidelpb.ExprType_Agg_BitAnd
	case ast.AggFuncVarPop:
		tp = fidelpb.ExprType_VarPop
	case ast.AggFuncJsonObjectAgg:
		tp = fidelpb.ExprType_JsonObjectAgg
	case ast.AggFuncStddevPop:
		tp = fidelpb.ExprType_StddevPop
	case ast.AggFuncVarSamp:
		tp = fidelpb.ExprType_VarSamp
	case ast.AggFuncStddevSamp:
		tp = fidelpb.ExprType_StddevSamp
	}
	if !client.IsRequestTypeSupported(ekv.ReqTypeSelect, int64(tp)) {
		return nil
	}

	children := make([]*fidelpb.Expr, 0, len(aggFunc.Args))
	for _, arg := range aggFunc.Args {
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	return &fidelpb.Expr{Tp: tp, Children: children, FieldType: expression.ToPBFieldType(aggFunc.RetTp)}
}

// PBExprToAggFuncDesc converts pb to aggregate function.
func PBExprToAggFuncDesc(ctx stochastikctx.Context, aggFunc *fidelpb.Expr, fieldTps []*types.FieldType) (*AggFuncDesc, error) {
	var name string
	switch aggFunc.Tp {
	case fidelpb.ExprType_Count:
		name = ast.AggFuncCount
	case fidelpb.ExprType_ApproxCountDistinct:
		name = ast.AggFuncApproxCountDistinct
	case fidelpb.ExprType_First:
		name = ast.AggFuncFirstEvent
	case fidelpb.ExprType_GroupConcat:
		name = ast.AggFuncGroupConcat
	case fidelpb.ExprType_Max:
		name = ast.AggFuncMax
	case fidelpb.ExprType_Min:
		name = ast.AggFuncMin
	case fidelpb.ExprType_Sum:
		name = ast.AggFuncSum
	case fidelpb.ExprType_Avg:
		name = ast.AggFuncAvg
	case fidelpb.ExprType_Agg_BitOr:
		name = ast.AggFuncBitOr
	case fidelpb.ExprType_Agg_BitXor:
		name = ast.AggFuncBitXor
	case fidelpb.ExprType_Agg_BitAnd:
		name = ast.AggFuncBitAnd
	default:
		return nil, errors.Errorf("unknown aggregation function type: %v", aggFunc.Tp)
	}

	args, err := expression.PBToExprs(aggFunc.Children, fieldTps, ctx.GetStochaseinstein_dbars().StmtCtx)
	if err != nil {
		return nil, err
	}
	base := baseFuncDesc{
		Name:  name,
		Args:  args,
		RetTp: expression.FieldTypeFromPB(aggFunc.FieldType),
	}
	base.WrapCastForAggArgs(ctx)
	return &AggFuncDesc{
		baseFuncDesc: base,
		Mode:         Partial1Mode,
		HasDistinct:  false,
	}, nil
}
