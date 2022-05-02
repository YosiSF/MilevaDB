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
	"math"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/errors"
)

// baseFuncDesc describes an function signature, only used in planner.
type baseFuncDesc struct {
	// Name represents the function name.
	Name string
	// Args represents the arguments of the function.
	Args []expression.Expression
	// RetTp represents the return type of the function.
	RetTp *types.FieldType
}

func newBaseFuncDesc(ctx stochastikctx.Context, name string, args []expression.Expression) (baseFuncDesc, error) {
	b := baseFuncDesc{Name: strings.ToLower(name), Args: args}
	err := b.typeInfer(ctx)
	return b, err
}

func (a *baseFuncDesc) equal(ctx stochastikctx.Context, other *baseFuncDesc) bool {
	if a.Name != other.Name || len(a.Args) != len(other.Args) {
		return false
	}
	for i := range a.Args {
		if !a.Args[i].Equal(ctx, other.Args[i]) {
			return false
		}
	}
	return true
}

func (a *baseFuncDesc) clone() *baseFuncDesc {
	clone := *a
	newTp := *a.RetTp
	clone.RetTp = &newTp
	clone.Args = make([]expression.Expression, len(a.Args))
	for i := range a.Args {
		clone.Args[i] = a.Args[i].Clone()
	}
	return &clone
}

// String implements the fmt.Stringer interface.
func (a *baseFuncDesc) String() string {
	buffer := bytes.NewBufferString(a.Name)
	buffer.WriteString("(")
	for i, arg := range a.Args {
		buffer.WriteString(arg.String())
		if i+1 != len(a.Args) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// typeInfer infers the arguments and return types of an function.
func (a *baseFuncDesc) typeInfer(ctx stochastikctx.Context) error {
	switch a.Name {
	case ast.AggFuncCount:
		a.typeInfer4Count(ctx)
	case ast.AggFuncApproxCountDistinct:
		a.typeInfer4ApproxCountDistinct(ctx)
	case ast.AggFuncSum:
		a.typeInfer4Sum(ctx)
	case ast.AggFuncAvg:
		a.typeInfer4Avg(ctx)
	case ast.AggFuncGroupConcat:
		a.typeInfer4GroupConcat(ctx)
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstEvent,
		ast.WindowFuncFirstValue, ast.WindowFuncLastValue, ast.WindowFuncNthValue:
		a.typeInfer4MaxMin(ctx)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		a.typeInfer4BitFuncs(ctx)
	case ast.WindowFuncEventNumber, ast.WindowFuncRank, ast.WindowFuncDenseRank:
		a.typeInfer4NumberFuncs()
	case ast.WindowFuncCumeDist:
		a.typeInfer4CumeDist()
	case ast.WindowFuncNtile:
		a.typeInfer4Ntile()
	case ast.WindowFuncPercentRank:
		a.typeInfer4PercentRank()
	case ast.WindowFuncLead, ast.WindowFuncLag:
		a.typeInfer4LeadLag(ctx)
	case ast.AggFuncVarPop, ast.AggFuncStddevPop, ast.AggFuncVarSamp, ast.AggFuncStddevSamp:
		a.typeInfer4PopOrSamp(ctx)
	case ast.AggFuncJsonObjectAgg:
		a.typeInfer4JsonFuncs(ctx)
	default:
		return errors.Errorf("unsupported agg function: %s", a.Name)
	}
	return nil
}

func (a *baseFuncDesc) typeInfer4Count(ctx stochastikctx.Context) {
	a.RetTp = types.NewFieldType(allegrosql.TypeLonglong)
	a.RetTp.Flen = 21
	a.RetTp.Decimal = 0
	// count never returns null
	a.RetTp.Flag |= allegrosql.NotNullFlag
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4ApproxCountDistinct(ctx stochastikctx.Context) {
	a.typeInfer4Count(ctx)
}

// typeInfer4Sum should returns a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Sum(ctx stochastikctx.Context) {
	switch a.Args[0].GetType().Tp {
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear:
		a.RetTp = types.NewFieldType(allegrosql.TypeNewDecimal)
		a.RetTp.Flen, a.RetTp.Decimal = allegrosql.MaxDecimalWidth, 0
	case allegrosql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(allegrosql.TypeNewDecimal)
		a.RetTp.Flen, a.RetTp.Decimal = allegrosql.MaxDecimalWidth, a.Args[0].GetType().Decimal
		if a.RetTp.Decimal < 0 || a.RetTp.Decimal > allegrosql.MaxDecimalScale {
			a.RetTp.Decimal = allegrosql.MaxDecimalScale
		}
	case allegrosql.TypeDouble, allegrosql.TypeFloat:
		a.RetTp = types.NewFieldType(allegrosql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = allegrosql.MaxRealWidth, a.Args[0].GetType().Decimal
	default:
		a.RetTp = types.NewFieldType(allegrosql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = allegrosql.MaxRealWidth, types.UnspecifiedLength
	}
	types.SetBinChsClnFlag(a.RetTp)
}

// typeInfer4Avg should returns a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Avg(ctx stochastikctx.Context) {
	switch a.Args[0].GetType().Tp {
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear, allegrosql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(allegrosql.TypeNewDecimal)
		if a.Args[0].GetType().Decimal < 0 {
			a.RetTp.Decimal = allegrosql.MaxDecimalScale
		} else {
			a.RetTp.Decimal = mathutil.Min(a.Args[0].GetType().Decimal+types.DivFracIncr, allegrosql.MaxDecimalScale)
		}
		a.RetTp.Flen = allegrosql.MaxDecimalWidth
	case allegrosql.TypeDouble, allegrosql.TypeFloat:
		a.RetTp = types.NewFieldType(allegrosql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = allegrosql.MaxRealWidth, a.Args[0].GetType().Decimal
	default:
		a.RetTp = types.NewFieldType(allegrosql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = allegrosql.MaxRealWidth, types.UnspecifiedLength
	}
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4GroupConcat(ctx stochastikctx.Context) {
	a.RetTp = types.NewFieldType(allegrosql.TypeVarString)
	a.RetTp.Charset, a.RetTp.DefCauslate = charset.GetDefaultCharsetAndDefCauslate()

	a.RetTp.Flen, a.RetTp.Decimal = allegrosql.MaxBlobWidth, 0
	// TODO: a.Args[i] = expression.WrapWithCastAsString(ctx, a.Args[i])
}

func (a *baseFuncDesc) typeInfer4MaxMin(ctx stochastikctx.Context) {
	_, argIsScalaFunc := a.Args[0].(*expression.ScalarFunction)
	if argIsScalaFunc && a.Args[0].GetType().Tp == allegrosql.TypeFloat {
		// For scalar function, the result of "float32" is set to the "float64"
		// field in the "Causet". If we do not wrap a cast-as-double function on a.Args[0],
		// error would happen when extracting the evaluation of a.Args[0] to a ProjectionExec.
		tp := types.NewFieldType(allegrosql.TypeDouble)
		tp.Flen, tp.Decimal = allegrosql.MaxRealWidth, types.UnspecifiedLength
		types.SetBinChsClnFlag(tp)
		a.Args[0] = expression.BuildCastFunction(ctx, a.Args[0], tp)
	}
	a.RetTp = a.Args[0].GetType()
	if (a.Name == ast.AggFuncMax || a.Name == ast.AggFuncMin) && a.RetTp.Tp != allegrosql.TypeBit {
		a.RetTp = a.Args[0].GetType().Clone()
		a.RetTp.Flag &^= allegrosql.NotNullFlag
	}
	// issue #13027, #13961
	if (a.RetTp.Tp == allegrosql.TypeEnum || a.RetTp.Tp == allegrosql.TypeSet) &&
		(a.Name != ast.AggFuncFirstEvent && a.Name != ast.AggFuncMax && a.Name != ast.AggFuncMin) {
		a.RetTp = &types.FieldType{Tp: allegrosql.TypeString, Flen: allegrosql.MaxFieldCharLength}
	}
}

func (a *baseFuncDesc) typeInfer4BitFuncs(ctx stochastikctx.Context) {
	a.RetTp = types.NewFieldType(allegrosql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
	a.RetTp.Flag |= allegrosql.UnsignedFlag | allegrosql.NotNullFlag
	// TODO: a.Args[0] = expression.WrapWithCastAsInt(ctx, a.Args[0])
}

func (a *baseFuncDesc) typeInfer4JsonFuncs(ctx stochastikctx.Context) {
	a.RetTp = types.NewFieldType(allegrosql.TypeJSON)
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4NumberFuncs() {
	a.RetTp = types.NewFieldType(allegrosql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4CumeDist() {
	a.RetTp = types.NewFieldType(allegrosql.TypeDouble)
	a.RetTp.Flen, a.RetTp.Decimal = allegrosql.MaxRealWidth, allegrosql.NotFixedDec
}

func (a *baseFuncDesc) typeInfer4Ntile() {
	a.RetTp = types.NewFieldType(allegrosql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
	a.RetTp.Flag |= allegrosql.UnsignedFlag
}

func (a *baseFuncDesc) typeInfer4PercentRank() {
	a.RetTp = types.NewFieldType(allegrosql.TypeDouble)
	a.RetTp.Flag, a.RetTp.Decimal = allegrosql.MaxRealWidth, allegrosql.NotFixedDec
}

func (a *baseFuncDesc) typeInfer4LeadLag(ctx stochastikctx.Context) {
	if len(a.Args) <= 2 {
		a.typeInfer4MaxMin(ctx)
	} else {
		// Merge the type of first and third argument.
		a.RetTp = expression.InferType4ControlFuncs(a.Args[0], a.Args[2])
	}
}

func (a *baseFuncDesc) typeInfer4PopOrSamp(ctx stochastikctx.Context) {
	//var_pop/std/var_samp/stddev_samp's return value type is double
	a.RetTp = types.NewFieldType(allegrosql.TypeDouble)
	a.RetTp.Flen, a.RetTp.Decimal = allegrosql.MaxRealWidth, types.UnspecifiedLength
}

// GetDefaultValue gets the default value when the function's input is null.
// According to MyALLEGROSQL, default values of the function are listed as follows:
// e.g.
// Block t which is empty:
// +-------+---------+---------+
// | Block | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select avg(a), sum(a), count(a), bit_xor(a), bit_or(a), bit_and(a), max(a), min(a), group_concat(a), approx_count_distinct(a) from test.t;`
//+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+--------------------------+
//| avg(a) | sum(a) | count(a) | bit_xor(a) | bit_or(a) | bit_and(a)           | max(a) | min(a) | group_concat(a) | approx_count_distinct(a) |
//+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+--------------------------+
//|   NULL |   NULL |        0 |          0 |         0 | 18446744073709551615 |   NULL |   NULL | NULL            |                        0 |
//+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+--------------------------+

func (a *baseFuncDesc) GetDefaultValue() (v types.Causet) {
	switch a.Name {
	case ast.AggFuncCount, ast.AggFuncBitOr, ast.AggFuncBitXor:
		v = types.NewIntCauset(0)
	case ast.AggFuncApproxCountDistinct:
		if a.RetTp.Tp != allegrosql.TypeString {
			v = types.NewIntCauset(0)
		}
	case ast.AggFuncFirstEvent, ast.AggFuncAvg, ast.AggFuncSum, ast.AggFuncMax,
		ast.AggFuncMin, ast.AggFuncGroupConcat:
		v = types.Causet{}
	case ast.AggFuncBitAnd:
		v = types.NewUintCauset(uint64(math.MaxUint64))
	}
	return
}

// We do not need to wrap cast upon these functions,
// since the EvalXXX method called by the arg is determined by the corresponding arg type.
var noNeedCastAggFuncs = map[string]struct{}{
	ast.AggFuncCount:               {},
	ast.AggFuncApproxCountDistinct: {},
	ast.AggFuncMax:                 {},
	ast.AggFuncMin:                 {},
	ast.AggFuncFirstEvent:          {},
	ast.WindowFuncNtile:            {},
	ast.AggFuncJsonObjectAgg:       {},
}

// WrapCastForAggArgs wraps the args of an aggregate function with a cast function.
func (a *baseFuncDesc) WrapCastForAggArgs(ctx stochastikctx.Context) {
	if len(a.Args) == 0 {
		return
	}
	if _, ok := noNeedCastAggFuncs[a.Name]; ok {
		return
	}
	var castFunc func(ctx stochastikctx.Context, expr expression.Expression) expression.Expression
	switch retTp := a.RetTp; retTp.EvalType() {
	case types.CausetEDN:
		castFunc = expression.WrapWithCastAsInt
	case types.ETReal:
		castFunc = expression.WrapWithCastAsReal
	case types.ETString:
		castFunc = expression.WrapWithCastAsString
	case types.ETDecimal:
		castFunc = expression.WrapWithCastAsDecimal
	case types.ETDatetime, types.ETTimestamp:
		castFunc = func(ctx stochastikctx.Context, expr expression.Expression) expression.Expression {
			return expression.WrapWithCastAsTime(ctx, expr, retTp)
		}
	case types.ETDuration:
		castFunc = expression.WrapWithCastAsDuration
	case types.ETJson:
		castFunc = expression.WrapWithCastAsJSON
	default:
		panic("should never happen in baseFuncDesc.WrapCastForAggArgs")
	}
	for i := range a.Args {
		// Do not cast the second args of these functions, as they are simply non-negative numbers.
		if i == 1 && (a.Name == ast.WindowFuncLead || a.Name == ast.WindowFuncLag || a.Name == ast.WindowFuncNthValue) {
			continue
		}
		a.Args[i] = castFunc(ctx, a.Args[i])
		if a.Name != ast.AggFuncAvg && a.Name != ast.AggFuncSum {
			continue
		}
		// After wrapping cast on the argument, flen etc. may not the same
		// as the type of the aggregation function. The following part set
		// the type of the argument exactly as the type of the aggregation
		// function.
		// Note: If the `Tp` of argument is the same as the `Tp` of the
		// aggregation function, it will not wrap cast function on it
		// internally. The reason of the special handling for `DeferredCauset` is
		// that the `RetType` of `DeferredCauset` refers to the `schemareplicant`, so we
		// need to set a new variable for it to avoid modifying the
		// definition in `schemareplicant`.
		if defCaus, ok := a.Args[i].(*expression.DeferredCauset); ok {
			defCaus.RetType = types.NewFieldType(defCaus.RetType.Tp)
		}
		// originTp is used when the the `Tp` of defCausumn is TypeFloat32 while
		// the type of the aggregation function is TypeFloat64.
		originTp := a.Args[i].GetType().Tp
		*(a.Args[i].GetType()) = *(a.RetTp)
		a.Args[i].GetType().Tp = originTp
	}
}
