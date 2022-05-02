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
	"bytes"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// Aggregation stands for aggregate functions.
type Aggregation interface {
	// UFIDelate during executing.
	UFIDelate(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, event chunk.Event) error

	// GetPartialResult will called by interlock to get partial results. For avg function, partial results will return
	// sum and count values at the same time.
	GetPartialResult(evalCtx *AggEvaluateContext) []types.Causet

	// GetResult will be called when all data have been processed.
	GetResult(evalCtx *AggEvaluateContext) types.Causet

	// CreateContext creates a new AggEvaluateContext for the aggregation function.
	CreateContext(sc *stmtctx.StatementContext) *AggEvaluateContext

	// ResetContext resets the content of the evaluate context.
	ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext)
}

// NewDistAggFunc creates new Aggregate function for mock einsteindb.
func NewDistAggFunc(expr *fidelpb.Expr, fieldTps []*types.FieldType, sc *stmtctx.StatementContext) (Aggregation, error) {
	args := make([]expression.Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		arg, err := expression.PBToExpr(child, fieldTps, sc)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	switch expr.Tp {
	case fidelpb.ExprType_Sum:
		return &sumFunction{aggFunction: newAggFunc(ast.AggFuncSum, args, false)}, nil
	case fidelpb.ExprType_Count:
		return &countFunction{aggFunction: newAggFunc(ast.AggFuncCount, args, false)}, nil
	case fidelpb.ExprType_Avg:
		return &avgFunction{aggFunction: newAggFunc(ast.AggFuncAvg, args, false)}, nil
	case fidelpb.ExprType_GroupConcat:
		return &concatFunction{aggFunction: newAggFunc(ast.AggFuncGroupConcat, args, false)}, nil
	case fidelpb.ExprType_Max:
		return &maxMinFunction{aggFunction: newAggFunc(ast.AggFuncMax, args, false), isMax: true}, nil
	case fidelpb.ExprType_Min:
		return &maxMinFunction{aggFunction: newAggFunc(ast.AggFuncMin, args, false)}, nil
	case fidelpb.ExprType_First:
		return &firstEventFunction{aggFunction: newAggFunc(ast.AggFuncFirstEvent, args, false)}, nil
	case fidelpb.ExprType_Agg_BitOr:
		return &bitOrFunction{aggFunction: newAggFunc(ast.AggFuncBitOr, args, false)}, nil
	case fidelpb.ExprType_Agg_BitXor:
		return &bitXorFunction{aggFunction: newAggFunc(ast.AggFuncBitXor, args, false)}, nil
	case fidelpb.ExprType_Agg_BitAnd:
		return &bitAndFunction{aggFunction: newAggFunc(ast.AggFuncBitAnd, args, false)}, nil
	}
	return nil, errors.Errorf("Unknown aggregate function type %v", expr.Tp)
}

// AggEvaluateContext is used to causetstore intermediate result when calculating aggregate functions.
type AggEvaluateContext struct {
	DistinctChecker *distinctChecker
	Count           int64
	Value           types.Causet
	Buffer          *bytes.Buffer // Buffer is used for group_concat.
	GotFirstEvent   bool          // It will check if the agg has met the first event key.
}

// AggFunctionMode stands for the aggregation function's mode.
type AggFunctionMode int

// |-----------------|--------------|--------------|
// | AggFunctionMode | input        | output       |
// |-----------------|--------------|--------------|
// | CompleteMode    | origin data  | final result |
// | FinalMode       | partial data | final result |
// | Partial1Mode    | origin data  | partial data |
// | Partial2Mode    | partial data | partial data |
// | DedupMode       | origin data  | origin data  |
// |-----------------|--------------|--------------|
const (
	CompleteMode AggFunctionMode = iota
	FinalMode
	Partial1Mode
	Partial2Mode
	DedupMode
)

type aggFunction struct {
	*AggFuncDesc
}

func newAggFunc(funcName string, args []expression.Expression, hasDistinct bool) aggFunction {
	agg := &AggFuncDesc{HasDistinct: hasDistinct}
	agg.Name = funcName
	agg.Args = args
	return aggFunction{AggFuncDesc: agg}
}

// CreateContext implements Aggregation interface.
func (af *aggFunction) CreateContext(sc *stmtctx.StatementContext) *AggEvaluateContext {
	evalCtx := &AggEvaluateContext{}
	if af.HasDistinct {
		evalCtx.DistinctChecker = createDistinctChecker(sc)
	}
	return evalCtx
}

func (af *aggFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	if af.HasDistinct {
		evalCtx.DistinctChecker = createDistinctChecker(sc)
	}
	evalCtx.Value.SetNull()
}

func (af *aggFunction) uFIDelateSum(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, event chunk.Event) error {
	a := af.Args[0]
	value, err := a.Eval(event)
	if err != nil {
		return err
	}
	if value.IsNull() {
		return nil
	}
	if af.HasDistinct {
		d, err1 := evalCtx.DistinctChecker.Check([]types.Causet{value})
		if err1 != nil {
			return err1
		}
		if !d {
			return nil
		}
	}
	evalCtx.Value, err = calculateSum(sc, evalCtx.Value, value)
	if err != nil {
		return err
	}
	evalCtx.Count++
	return nil
}

// NeedCount indicates whether the aggregate function should record count.
func NeedCount(name string) bool {
	return name == ast.AggFuncCount || name == ast.AggFuncAvg
}

// NeedValue indicates whether the aggregate function should record value.
func NeedValue(name string) bool {
	switch name {
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncFirstEvent, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncGroupConcat, ast.AggFuncBitOr, ast.AggFuncBitAnd, ast.AggFuncBitXor:
		return true
	default:
		return false
	}
}

// IsAllFirstEvent checks whether functions in `aggFuncs` are all FirstEvent.
func IsAllFirstEvent(aggFuncs []*AggFuncDesc) bool {
	for _, fun := range aggFuncs {
		if fun.Name != ast.AggFuncFirstEvent {
			return false
		}
	}
	return true
}

// CheckAggPushDown checks whether an agg function can be pushed to storage.
func CheckAggPushDown(aggFunc *AggFuncDesc, storeType ekv.StoreType) bool {
	if len(aggFunc.OrderByItems) > 0 {
		return false
	}
	ret := true
	switch storeType {
	case ekv.TiFlash:
		ret = CheckAggPushFlash(aggFunc)
	}
	if ret {
		ret = expression.IsPushDownEnabled(strings.ToLower(aggFunc.Name), storeType)
	}
	return ret
}

// CheckAggPushFlash checks whether an agg function can be pushed to flash storage.
func CheckAggPushFlash(aggFunc *AggFuncDesc) bool {
	switch aggFunc.Name {
	case ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncMin, ast.AggFuncMax, ast.AggFuncAvg, ast.AggFuncFirstEvent, ast.AggFuncApproxCountDistinct:
		return true
	}
	return false
}
