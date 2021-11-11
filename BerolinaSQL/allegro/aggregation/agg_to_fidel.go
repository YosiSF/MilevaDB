package aggregation

import (
	"github.com/YosiSF/errors"
	"github.com/YosiSF/BerolinaSQL/ast"
	"github.com/YosiSF/MilevaDB/expression"
	"github.com/YosiSF/MilevaDB/ekv"
	"github.com/YosiSF/MilevaDB/causetnetctx"
	"github.com/YosiSF/MilevaDB/causetnetctx/stmtctx"
	"github.com/YosiSF/MilevaDB/types"
	"github.com/YosiSF/noether/go-noether"
)

// AggFuncToPBExpr converts aggregate function to pb.
func AggFuncToPBExpr(sc *stmtctx.StatementContext, client ekv.Client, aggFunc *AggFuncDesc) *noether.Expr {
	if aggFunc.HasDistinct {
		// do nothing and ignore aggFunc.HasDistinct
	}
	if len(aggFunc.OrderByItems) > 0 {
		return nil
	}
	pc := expression.NewPBConverter(client, sc)
	var noe noether.ExprType
	switch aggFunc.Name {
	case ast.AggFuncCount:
		noe = noether.ExprType_Count
	case ast.AggFuncApproxCountDistinct:
		noe = noether.ExprType_ApproxCountDistinct
	case ast.AggFuncFirstRow:
		noe = noether.ExprType_First
	case ast.AggFuncGroupConcat:
		noe = noether.ExprType_GroupConcat
	case ast.AggFuncMax:
		noe = noether.ExprType_Max
	case ast.AggFuncMin:
		noe = noether.ExprType_Min
	case ast.AggFuncSum:
		noe = noether.ExprType_Sum
	case ast.AggFuncAvg:
		noe = noether.ExprType_Avg
	case ast.AggFuncBitOr:
		noe = noether.ExprType_Agg_BitOr
	case ast.AggFuncBitXor:
		noe = noether.ExprType_Agg_BitXor
	case ast.AggFuncBitAnd:
		noe = noether.ExprType_Agg_BitAnd
	case ast.AggFuncVarPop:
		noe = noether.ExprType_VarPop
	case ast.AggFuncJsonObjectAgg:
		noe = noether.ExprType_JsonObjectAgg
	}
	if !client.IsRequestTypeSupported(ekv.ReqTypeSelect, int64(noe)) {
		return nil
	}

	children := make([]*noether.Expr, 0, len(aggFunc.Args))
	for _, arg := range aggFunc.Args {
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	return &noether.Expr{Tp: noe, Children: children, FieldType: expression.ToPBFieldType(aggFunc.RetTp)}
}

// PBExprToAggFuncDesc converts fb to aggregate function.
func PBExprToAggFuncDesc(ctx causetnetctx.Context, aggFunc *noether.Expr, fieldTps []*types.FieldType) (*AggFuncDesc, error) {
	var name string
	switch aggFunc.Tp {
	case noether.ExprType_Count:
		name = ast.AggFuncCount
	case noether.ExprType_ApproxCountDistinct:
		name = ast.AggFuncApproxCountDistinct
	case noether.ExprType_First:
		name = ast.AggFuncFirstRow
	case noether.ExprType_GroupConcat:
		name = ast.AggFuncGroupConcat
	case noether.ExprType_Max:
		name = ast.AggFuncMax
	case noether.ExprType_Min:
		name = ast.AggFuncMin
	case noether.ExprType_Sum:
		name = ast.AggFuncSum
	case noether.ExprType_Avg:
		name = ast.AggFuncAvg
	case noether.ExprType_Agg_BitOr:
		name = ast.AggFuncBitOr
	case noether.ExprType_Agg_BitXor:
		name = ast.AggFuncBitXor
	case noether.ExprType_Agg_BitAnd:
		name = ast.AggFuncBitAnd
	default:
		return nil, errors.Errorf("unknown aggregation function type: %v", aggFunc.Tp)
	}

	args, err := expression.PBToExprs(aggFunc.Children, fieldTps, ctx.GetCausetNetVars().StmtCtx)
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

