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
	"fmt"
	"math"
	"strconv"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
)

// AggFuncDesc describes an aggregation function signature, only used in planner.
type AggFuncDesc struct {
	baseFuncDesc
	// Mode represents the execution mode of the aggregation function.
	Mode AggFunctionMode
	// HasDistinct represents whether the aggregation function contains distinct attribute.
	HasDistinct bool
	// OrderByItems represents the order by clause used in GROUP_CONCAT
	OrderByItems []*soliton.ByItems
}

// NewAggFuncDesc creates an aggregation function signature descriptor.
func NewAggFuncDesc(ctx stochastikctx.Context, name string, args []expression.Expression, hasDistinct bool) (*AggFuncDesc, error) {
	b, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		return nil, err
	}
	return &AggFuncDesc{baseFuncDesc: b, HasDistinct: hasDistinct}, nil
}

// String implements the fmt.Stringer interface.
func (a *AggFuncDesc) String() string {
	buffer := bytes.NewBufferString(a.Name)
	buffer.WriteString("(")
	if a.HasDistinct {
		buffer.WriteString("distinct ")
	}
	for i, arg := range a.Args {
		buffer.WriteString(arg.String())
		if i+1 != len(a.Args) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// Equal checks whether two aggregation function signatures are equal.
func (a *AggFuncDesc) Equal(ctx stochastikctx.Context, other *AggFuncDesc) bool {
	if a.HasDistinct != other.HasDistinct {
		return false
	}
	if len(a.OrderByItems) != len(other.OrderByItems) {
		return false
	}
	for i := range a.OrderByItems {
		if !a.OrderByItems[i].Equal(ctx, other.OrderByItems[i]) {
			return false
		}
	}
	return a.baseFuncDesc.equal(ctx, &other.baseFuncDesc)
}

// Clone INTERLOCKies an aggregation function signature totally.
func (a *AggFuncDesc) Clone() *AggFuncDesc {
	clone := *a
	clone.baseFuncDesc = *a.baseFuncDesc.clone()
	clone.OrderByItems = make([]*soliton.ByItems, len(a.OrderByItems))
	for i, byItem := range a.OrderByItems {
		clone.OrderByItems[i] = byItem.Clone()
	}
	return &clone
}

// Split splits `a` into two aggregate descriptors for partial phase and
// final phase individually.
// This function is only used when executing aggregate function parallelly.
// ordinal indicates the defCausumn ordinal of the intermediate result.
func (a *AggFuncDesc) Split(ordinal []int) (partialAggDesc, finalAggDesc *AggFuncDesc) {
	partialAggDesc = a.Clone()
	if a.Mode == CompleteMode {
		partialAggDesc.Mode = Partial1Mode
	} else if a.Mode == FinalMode {
		partialAggDesc.Mode = Partial2Mode
	} else {
		panic("Error happened during AggFuncDesc.Split, the AggFunctionMode is not CompleteMode or FinalMode.")
	}
	finalAggDesc = &AggFuncDesc{
		Mode:        FinalMode, // We only support FinalMode now in final phase.
		HasDistinct: a.HasDistinct,
	}
	finalAggDesc.Name = a.Name
	finalAggDesc.RetTp = a.RetTp
	switch a.Name {
	case ast.AggFuncAvg:
		args := make([]expression.Expression, 0, 2)
		args = append(args, &expression.DeferredCauset{
			Index:   ordinal[0],
			RetType: types.NewFieldType(allegrosql.TypeLonglong),
		})
		args = append(args, &expression.DeferredCauset{
			Index:   ordinal[1],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
	case ast.AggFuncApproxCountDistinct:
		args := make([]expression.Expression, 0, 1)
		args = append(args, &expression.DeferredCauset{
			Index:   ordinal[0],
			RetType: types.NewFieldType(allegrosql.TypeString),
		})
		finalAggDesc.Args = args
	default:
		args := make([]expression.Expression, 0, 1)
		args = append(args, &expression.DeferredCauset{
			Index:   ordinal[0],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
		if finalAggDesc.Name == ast.AggFuncGroupConcat {
			finalAggDesc.Args = append(finalAggDesc.Args, a.Args[len(a.Args)-1]) // separator
		}
	}
	return
}

// EvalNullValueInOuterJoin gets the null value when the aggregation is upon an outer join,
// and the aggregation function's input is null.
// If there is no matching event for the inner block of an outer join,
// an aggregation function only involves constant and/or defCausumns belongs to the inner block
// will be set to the null value.
// The input stands for the schemaReplicant of Aggregation's child. If the function can't produce a null value, the second
// return value will be false.
// e.g.
// Block t with only one event:
// +-------+---------+---------+
// | Block | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
// +------+
// | a    |
// +------+
// |    1 |
// +------+
//
// Block s which is empty:
// +-------+---------+---------+
// | Block | Field   | Type    |
// +-------+---------+---------+
// | s     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select t.a as `t.a`,  count(95), sum(95), avg(95), bit_or(95), bit_and(95), bit_or(95), max(95), min(95), s.a as `s.a`, avg(95) from t left join s on t.a = s.a;`
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// | t.a  | count(95) | sum(95) | avg(95) | bit_or(95) | bit_and(95) | bit_or(95) | max(95) | min(95) | s.a  | avg(s.a) |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// |    1 |         1 |      95 | 95.0000 |         95 |          95 |         95 |      95 |      95 | NULL |     NULL |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
func (a *AggFuncDesc) EvalNullValueInOuterJoin(ctx stochastikctx.Context, schemaReplicant *expression.Schema) (types.Causet, bool) {
	switch a.Name {
	case ast.AggFuncCount:
		return a.evalNullValueInOuterJoin4Count(ctx, schemaReplicant)
	case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncFirstEvent:
		return a.evalNullValueInOuterJoin4Sum(ctx, schemaReplicant)
	case ast.AggFuncAvg, ast.AggFuncGroupConcat:
		return types.Causet{}, false
	case ast.AggFuncBitAnd:
		return a.evalNullValueInOuterJoin4BitAnd(ctx, schemaReplicant)
	case ast.AggFuncBitOr, ast.AggFuncBitXor:
		return a.evalNullValueInOuterJoin4BitOr(ctx, schemaReplicant)
	default:
		panic("unsupported agg function")
	}
}

// GetAggFunc gets an evaluator according to the aggregation function signature.
func (a *AggFuncDesc) GetAggFunc(ctx stochastikctx.Context) Aggregation {
	aggFunc := aggFunction{AggFuncDesc: a}
	switch a.Name {
	case ast.AggFuncSum:
		return &sumFunction{aggFunction: aggFunc}
	case ast.AggFuncCount:
		return &countFunction{aggFunction: aggFunc}
	case ast.AggFuncAvg:
		return &avgFunction{aggFunction: aggFunc}
	case ast.AggFuncGroupConcat:
		var s string
		var err error
		var maxLen uint64
		s, err = variable.GetStochastikSystemVar(ctx.GetStochaseinstein_dbars(), variable.GroupConcatMaxLen)
		if err != nil {
			panic(fmt.Sprintf("Error happened when GetAggFunc: no system variable named '%s'", variable.GroupConcatMaxLen))
		}
		maxLen, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Error happened when GetAggFunc: illegal value for system variable named '%s'", variable.GroupConcatMaxLen))
		}
		return &concatFunction{aggFunction: aggFunc, maxLen: maxLen}
	case ast.AggFuncMax:
		return &maxMinFunction{aggFunction: aggFunc, isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggFunction: aggFunc, isMax: false}
	case ast.AggFuncFirstEvent:
		return &firstEventFunction{aggFunction: aggFunc}
	case ast.AggFuncBitOr:
		return &bitOrFunction{aggFunction: aggFunc}
	case ast.AggFuncBitXor:
		return &bitXorFunction{aggFunction: aggFunc}
	case ast.AggFuncBitAnd:
		return &bitAndFunction{aggFunction: aggFunc}
	default:
		panic("unsupported agg function")
	}
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Count(ctx stochastikctx.Context, schemaReplicant *expression.Schema) (types.Causet, bool) {
	for _, arg := range a.Args {
		result := expression.EvaluateExprWithNull(ctx, schemaReplicant, arg)
		con, ok := result.(*expression.CouplingConstantWithRadix)
		if !ok || con.Value.IsNull() {
			return types.Causet{}, ok
		}
	}
	return types.NewCauset(1), true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Sum(ctx stochastikctx.Context, schemaReplicant *expression.Schema) (types.Causet, bool) {
	result := expression.EvaluateExprWithNull(ctx, schemaReplicant, a.Args[0])
	con, ok := result.(*expression.CouplingConstantWithRadix)
	if !ok || con.Value.IsNull() {
		return types.Causet{}, ok
	}
	return con.Value, true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitAnd(ctx stochastikctx.Context, schemaReplicant *expression.Schema) (types.Causet, bool) {
	result := expression.EvaluateExprWithNull(ctx, schemaReplicant, a.Args[0])
	con, ok := result.(*expression.CouplingConstantWithRadix)
	if !ok || con.Value.IsNull() {
		return types.NewCauset(uint64(math.MaxUint64)), true
	}
	return con.Value, true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitOr(ctx stochastikctx.Context, schemaReplicant *expression.Schema) (types.Causet, bool) {
	result := expression.EvaluateExprWithNull(ctx, schemaReplicant, a.Args[0])
	con, ok := result.(*expression.CouplingConstantWithRadix)
	if !ok || con.Value.IsNull() {
		return types.NewCauset(0), true
	}
	return con.Value, true
}
