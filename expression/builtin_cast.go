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

// We implement 6 CastAsXXFunctionClass for `cast` built-in functions.
// XX means the return type of the `cast` built-in functions.
// XX contains the following 6 types:
// Int, Decimal, Real, String, Time, Duration.

// We implement 6 CastYYAsXXSig built-in function signatures for every CastAsXXFunctionClass.
// builtinCastXXAsYYSig takes a argument of type XX and returns a value of type YY.

package expression

import (
	"math"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

var (
	_ functionClass = &castAsIntFunctionClass{}
	_ functionClass = &castAsRealFunctionClass{}
	_ functionClass = &castAsStringFunctionClass{}
	_ functionClass = &castAsDecimalFunctionClass{}
	_ functionClass = &castAsTimeFunctionClass{}
	_ functionClass = &castAsDurationFunctionClass{}
	_ functionClass = &castAsJSONFunctionClass{}
)

var (
	_ builtinFunc = &builtinCastIntAsIntSig{}
	_ builtinFunc = &builtinCastIntAsRealSig{}
	_ builtinFunc = &builtinCastIntAsStringSig{}
	_ builtinFunc = &builtinCastIntAsDecimalSig{}
	_ builtinFunc = &builtinCastIntAsTimeSig{}
	_ builtinFunc = &builtinCastIntAsDurationSig{}
	_ builtinFunc = &builtinCastIntAsJSONSig{}

	_ builtinFunc = &builtinCastRealAsIntSig{}
	_ builtinFunc = &builtinCastRealAsRealSig{}
	_ builtinFunc = &builtinCastRealAsStringSig{}
	_ builtinFunc = &builtinCastRealAsDecimalSig{}
	_ builtinFunc = &builtinCastRealAsTimeSig{}
	_ builtinFunc = &builtinCastRealAsDurationSig{}
	_ builtinFunc = &builtinCastRealAsJSONSig{}

	_ builtinFunc = &builtinCastDecimalAsIntSig{}
	_ builtinFunc = &builtinCastDecimalAsRealSig{}
	_ builtinFunc = &builtinCastDecimalAsStringSig{}
	_ builtinFunc = &builtinCastDecimalAsDecimalSig{}
	_ builtinFunc = &builtinCastDecimalAsTimeSig{}
	_ builtinFunc = &builtinCastDecimalAsDurationSig{}
	_ builtinFunc = &builtinCastDecimalAsJSONSig{}

	_ builtinFunc = &builtinCastStringAsIntSig{}
	_ builtinFunc = &builtinCastStringAsRealSig{}
	_ builtinFunc = &builtinCastStringAsStringSig{}
	_ builtinFunc = &builtinCastStringAsDecimalSig{}
	_ builtinFunc = &builtinCastStringAsTimeSig{}
	_ builtinFunc = &builtinCastStringAsDurationSig{}
	_ builtinFunc = &builtinCastStringAsJSONSig{}

	_ builtinFunc = &builtinCastTimeAsIntSig{}
	_ builtinFunc = &builtinCastTimeAsRealSig{}
	_ builtinFunc = &builtinCastTimeAsStringSig{}
	_ builtinFunc = &builtinCastTimeAsDecimalSig{}
	_ builtinFunc = &builtinCastTimeAsTimeSig{}
	_ builtinFunc = &builtinCastTimeAsDurationSig{}
	_ builtinFunc = &builtinCastTimeAsJSONSig{}

	_ builtinFunc = &builtinCastDurationAsIntSig{}
	_ builtinFunc = &builtinCastDurationAsRealSig{}
	_ builtinFunc = &builtinCastDurationAsStringSig{}
	_ builtinFunc = &builtinCastDurationAsDecimalSig{}
	_ builtinFunc = &builtinCastDurationAsTimeSig{}
	_ builtinFunc = &builtinCastDurationAsDurationSig{}
	_ builtinFunc = &builtinCastDurationAsJSONSig{}

	_ builtinFunc = &builtinCastJSONAsIntSig{}
	_ builtinFunc = &builtinCastJSONAsRealSig{}
	_ builtinFunc = &builtinCastJSONAsStringSig{}
	_ builtinFunc = &builtinCastJSONAsDecimalSig{}
	_ builtinFunc = &builtinCastJSONAsTimeSig{}
	_ builtinFunc = &builtinCastJSONAsDurationSig{}
	_ builtinFunc = &builtinCastJSONAsJSONSig{}
)

type castAsIntFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsIntFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	b, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp.EvalType())
	if err != nil {
		return nil, err
	}
	bf := newBaseBuiltinCastFunc(b, ctx.Value(inUnionCastContext) != nil)
	bf.tp = c.tp
	if args[0].GetType().Hybrid() || IsBinaryLiteral(args[0]) {
		sig = &builtinCastIntAsIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastIntAsInt)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastIntAsInt)
	case types.ETReal:
		sig = &builtinCastRealAsIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastRealAsInt)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDecimalAsInt)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastTimeAsInt)
	case types.ETDuration:
		sig = &builtinCastDurationAsIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDurationAsInt)
	case types.ETJson:
		sig = &builtinCastJSONAsIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastJsonAsInt)
	case types.ETString:
		sig = &builtinCastStringAsIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastStringAsInt)
	default:
		panic("unsupported types.EvalType in castAsIntFunctionClass")
	}
	return sig, nil
}

type castAsRealFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsRealFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	b, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp.EvalType())
	if err != nil {
		return nil, err
	}
	bf := newBaseBuiltinCastFunc(b, ctx.Value(inUnionCastContext) != nil)
	bf.tp = c.tp
	if IsBinaryLiteral(args[0]) {
		sig = &builtinCastRealAsRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastRealAsReal)
		return sig, nil
	}
	var argTp types.EvalType
	if args[0].GetType().Hybrid() {
		argTp = types.ETInt
	} else {
		argTp = args[0].GetType().EvalType()
	}
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastIntAsReal)
	case types.ETReal:
		sig = &builtinCastRealAsRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastRealAsReal)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDecimalAsReal)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastTimeAsReal)
	case types.ETDuration:
		sig = &builtinCastDurationAsRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDurationAsReal)
	case types.ETJson:
		sig = &builtinCastJSONAsRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastJsonAsReal)
	case types.ETString:
		sig = &builtinCastStringAsRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastStringAsReal)
	default:
		panic("unsupported types.EvalType in castAsRealFunctionClass")
	}
	return sig, nil
}

type castAsDecimalFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsDecimalFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	b, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp.EvalType())
	if err != nil {
		return nil, err
	}
	bf := newBaseBuiltinCastFunc(b, ctx.Value(inUnionCastContext) != nil)
	bf.tp = c.tp
	if IsBinaryLiteral(args[0]) {
		sig = &builtinCastDecimalAsDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDecimalAsDecimal)
		return sig, nil
	}
	var argTp types.EvalType
	if args[0].GetType().Hybrid() {
		argTp = types.ETInt
	} else {
		argTp = args[0].GetType().EvalType()
	}
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastIntAsDecimal)
	case types.ETReal:
		sig = &builtinCastRealAsDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastRealAsDecimal)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDecimalAsDecimal)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastTimeAsDecimal)
	case types.ETDuration:
		sig = &builtinCastDurationAsDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDurationAsDecimal)
	case types.ETJson:
		sig = &builtinCastJSONAsDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastJsonAsDecimal)
	case types.ETString:
		sig = &builtinCastStringAsDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastStringAsDecimal)
	default:
		panic("unsupported types.EvalType in castAsDecimalFunctionClass")
	}
	return sig, nil
}

type castAsStringFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsStringFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp.EvalType())
	if err != nil {
		return nil, err
	}
	bf.tp = c.tp
	if args[0].GetType().Hybrid() || IsBinaryLiteral(args[0]) {
		sig = &builtinCastStringAsStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastStringAsString)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastIntAsString)
	case types.ETReal:
		sig = &builtinCastRealAsStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastRealAsString)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDecimalAsString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastTimeAsString)
	case types.ETDuration:
		sig = &builtinCastDurationAsStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDurationAsString)
	case types.ETJson:
		sig = &builtinCastJSONAsStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastJsonAsString)
	case types.ETString:
		sig = &builtinCastStringAsStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastStringAsString)
	default:
		panic("unsupported types.EvalType in castAsStringFunctionClass")
	}
	return sig, nil
}

type castAsTimeFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsTimeFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp.EvalType())
	if err != nil {
		return nil, err
	}
	bf.tp = c.tp
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastIntAsTime)
	case types.ETReal:
		sig = &builtinCastRealAsTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastRealAsTime)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDecimalAsTime)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastTimeAsTime)
	case types.ETDuration:
		sig = &builtinCastDurationAsTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDurationAsTime)
	case types.ETJson:
		sig = &builtinCastJSONAsTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastJsonAsTime)
	case types.ETString:
		sig = &builtinCastStringAsTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastStringAsTime)
	default:
		panic("unsupported types.EvalType in castAsTimeFunctionClass")
	}
	return sig, nil
}

type castAsDurationFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsDurationFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp.EvalType())
	if err != nil {
		return nil, err
	}
	bf.tp = c.tp
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastIntAsDuration)
	case types.ETReal:
		sig = &builtinCastRealAsDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastRealAsDuration)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDecimalAsDuration)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastTimeAsDuration)
	case types.ETDuration:
		sig = &builtinCastDurationAsDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDurationAsDuration)
	case types.ETJson:
		sig = &builtinCastJSONAsDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastJsonAsDuration)
	case types.ETString:
		sig = &builtinCastStringAsDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastStringAsDuration)
	default:
		panic("unsupported types.EvalType in castAsDurationFunctionClass")
	}
	return sig, nil
}

type castAsJSONFunctionClass struct {
	baseFunctionClass

	tp *types.FieldType
}

func (c *castAsJSONFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp.EvalType())
	if err != nil {
		return nil, err
	}
	bf.tp = c.tp
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsJSONSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastIntAsJson)
	case types.ETReal:
		sig = &builtinCastRealAsJSONSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastRealAsJson)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsJSONSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDecimalAsJson)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsJSONSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastTimeAsJson)
	case types.ETDuration:
		sig = &builtinCastDurationAsJSONSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastDurationAsJson)
	case types.ETJson:
		sig = &builtinCastJSONAsJSONSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CastJsonAsJson)
	case types.ETString:
		sig = &builtinCastStringAsJSONSig{bf}
		sig.getRetTp().Flag |= allegrosql.ParseToJSONFlag
		sig.setPbCode(fidelpb.ScalarFuncSig_CastStringAsJson)
	default:
		panic("unsupported types.EvalType in castAsJSONFunctionClass")
	}
	return sig, nil
}

type builtinCastIntAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsIntSig) evalInt(event chunk.Event) (res int64, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return
	}
	if b.inUnion && allegrosql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
		res = 0
	}
	return
}

type builtinCastIntAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsRealSig) evalReal(event chunk.Event) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	if unsignedArgs0 := allegrosql.HasUnsignedFlag(b.args[0].GetType().Flag); !allegrosql.HasUnsignedFlag(b.tp.Flag) && !unsignedArgs0 {
		res = float64(val)
	} else if b.inUnion && !unsignedArgs0 && val < 0 {
		// Round up to 0 if the value is negative but the expression eval type is unsigned in `UNION` statement
		// NOTE: the following expressions are equal (so choose the more efficient one):
		// `b.inUnion && allegrosql.HasUnsignedFlag(b.tp.Flag) && !unsignedArgs0 && val < 0`
		// `b.inUnion && !unsignedArgs0 && val < 0`
		res = 0
	} else {
		// recall that, int to float is different from uint to float
		res = float64(uint64(val))
	}
	return res, false, err
}

type builtinCastIntAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsDecimalSig) evalDecimal(event chunk.Event) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}

	if unsignedArgs0 := allegrosql.HasUnsignedFlag(b.args[0].GetType().Flag); !allegrosql.HasUnsignedFlag(b.tp.Flag) && !unsignedArgs0 {
		res = types.NewDecFromInt(val)
		// Round up to 0 if the value is negative but the expression eval type is unsigned in `UNION` statement
		// NOTE: the following expressions are equal (so choose the more efficient one):
		// `b.inUnion && allegrosql.HasUnsignedFlag(b.tp.Flag) && !unsignedArgs0 && val < 0`
		// `b.inUnion && !unsignedArgs0 && val < 0`
	} else if b.inUnion && !unsignedArgs0 && val < 0 {
		res = &types.MyDecimal{}
	} else {
		res = types.NewDecFromUint(uint64(val))
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, b.ctx.GetStochastikVars().StmtCtx)
	return res, isNull, err
}

type builtinCastIntAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsStringSig) evalString(event chunk.Event) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	if !allegrosql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		res = strconv.FormatInt(val, 10)
	} else {
		res = strconv.FormatUint(uint64(val), 10)
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, b.ctx.GetStochastikVars().StmtCtx, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastIntAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsTimeSig) evalTime(event chunk.Event) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseTimeFromNum(b.ctx.GetStochastikVars().StmtCtx, val, b.tp.Tp, int8(b.tp.Decimal))
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	if b.tp.Tp == allegrosql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, nil
}

type builtinCastIntAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsDurationSig) evalDuration(event chunk.Event) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	dur, err := types.NumberToDuration(val, int8(b.tp.Decimal))
	if err != nil {
		if types.ErrOverflow.Equal(err) {
			err = b.ctx.GetStochastikVars().StmtCtx.HandleOverflow(err, err)
		}
		return res, true, err
	}
	return dur, false, err
}

type builtinCastIntAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastIntAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastIntAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsJSONSig) evalJSON(event chunk.Event) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	if allegrosql.HasIsBooleanFlag(b.args[0].GetType().Flag) {
		res = json.CreateBinary(val != 0)
	} else if allegrosql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		res = json.CreateBinary(uint64(val))
	} else {
		res = json.CreateBinary(val)
	}
	return res, false, nil
}

type builtinCastRealAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsJSONSig) evalJSON(event chunk.Event) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(b.ctx, event)
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	return json.CreateBinary(val), isNull, err
}

type builtinCastDecimalAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsJSONSig) evalJSON(event chunk.Event) (json.BinaryJSON, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, event)
	if isNull || err != nil {
		return json.BinaryJSON{}, true, err
	}
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	f64, err := val.ToFloat64()
	if err != nil {
		return json.BinaryJSON{}, true, err
	}
	return json.CreateBinary(f64), isNull, err
}

type builtinCastStringAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsJSONSig) evalJSON(event chunk.Event) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	if allegrosql.HasParseToJSONFlag(b.tp.Flag) {
		res, err = json.ParseBinaryFromString(val)
	} else {
		res = json.CreateBinary(val)
	}
	return res, false, err
}

type builtinCastDurationAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsJSONSig) evalJSON(event chunk.Event) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	val.Fsp = types.MaxFsp
	return json.CreateBinary(val.String()), false, nil
}

type builtinCastTimeAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsJSONSig) evalJSON(event chunk.Event) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Type() == allegrosql.TypeDatetime || val.Type() == allegrosql.TypeTimestamp {
		val.SetFsp(types.MaxFsp)
	}
	return json.CreateBinary(val.String()), false, nil
}

type builtinCastRealAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsRealSig) evalReal(event chunk.Event) (res float64, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalReal(b.ctx, event)
	if b.inUnion && allegrosql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
		res = 0
	}
	return
}

type builtinCastRealAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsIntSig) evalInt(event chunk.Event) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	if !allegrosql.HasUnsignedFlag(b.tp.Flag) {
		res, err = types.ConvertFloatToInt(val, types.IntergerSignedLowerBound(allegrosql.TypeLonglong), types.IntergerSignedUpperBound(allegrosql.TypeLonglong), allegrosql.TypeLonglong)
	} else if b.inUnion && val < 0 {
		res = 0
	} else {
		var uintVal uint64
		sc := b.ctx.GetStochastikVars().StmtCtx
		uintVal, err = types.ConvertFloatToUint(sc, val, types.IntergerUnsignedUpperBound(allegrosql.TypeLonglong), allegrosql.TypeLonglong)
		res = int64(uintVal)
	}
	if types.ErrOverflow.Equal(err) {
		err = b.ctx.GetStochastikVars().StmtCtx.HandleOverflow(err, err)
	}
	return res, isNull, err
}

type builtinCastRealAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsDecimalSig) evalDecimal(event chunk.Event) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	res = new(types.MyDecimal)
	if !b.inUnion || val >= 0 {
		err = res.FromFloat64(val)
		if types.ErrOverflow.Equal(err) {
			warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", b.args[0])
			err = b.ctx.GetStochastikVars().StmtCtx.HandleOverflow(err, warnErr)
		} else if types.ErrTruncated.Equal(err) {
			// This behavior is consistent with MyALLEGROSQL.
			err = nil
		}
		if err != nil {
			return res, false, err
		}
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, b.ctx.GetStochastikVars().StmtCtx)
	return res, false, err
}

type builtinCastRealAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsStringSig) evalString(event chunk.Event) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}

	bits := 64
	if b.args[0].GetType().Tp == allegrosql.TypeFloat {
		// b.args[0].EvalReal() casts the value from float32 to float64, for example:
		// float32(208.867) is cast to float64(208.86700439)
		// If we strconv.FormatFloat the value with 64bits, the result is incorrect!
		bits = 32
	}
	res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatFloat(val, 'f', -1, bits), b.tp, b.ctx.GetStochastikVars().StmtCtx, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastRealAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsTimeSig) evalTime(event chunk.Event) (types.Time, bool, error) {
	val, isNull, err := b.args[0].EvalReal(b.ctx, event)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}
	// MyALLEGROSQL compatibility: 0 should not be converted to null, see #11203
	fv := strconv.FormatFloat(val, 'f', -1, 64)
	if fv == "0" {
		return types.ZeroTime, false, nil
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err := types.ParseTime(sc, fv, b.tp.Tp, int8(b.tp.Decimal))
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	if b.tp.Tp == allegrosql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, nil
}

type builtinCastRealAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastRealAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastRealAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsDurationSig) evalDuration(event chunk.Event) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalReal(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseDuration(b.ctx.GetStochastikVars().StmtCtx, strconv.FormatFloat(val, 'f', -1, 64), int8(b.tp.Decimal))
	if err != nil {
		if types.ErrTruncatedWrongVal.Equal(err) {
			err = b.ctx.GetStochastikVars().StmtCtx.HandleTruncate(err)
			// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
			if res == types.ZeroDuration {
				return res, true, err
			}
		}
	}
	return res, false, err
}

type builtinCastDecimalAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsDecimalSig) evalDecimal(event chunk.Event) (res *types.MyDecimal, isNull bool, err error) {
	evalDecimal, isNull, err := b.args[0].EvalDecimal(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	res = &types.MyDecimal{}
	if !(b.inUnion && allegrosql.HasUnsignedFlag(b.tp.Flag) && evalDecimal.IsNegative()) {
		*res = *evalDecimal
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, err
}

type builtinCastDecimalAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsIntSig) evalInt(event chunk.Event) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}

	// Round is needed for both unsigned and signed.
	var to types.MyDecimal
	err = val.Round(&to, 0, types.ModeHalfEven)
	if err != nil {
		return 0, true, err
	}

	if !allegrosql.HasUnsignedFlag(b.tp.Flag) {
		res, err = to.ToInt()
	} else if b.inUnion && to.IsNegative() {
		res = 0
	} else {
		var uintRes uint64
		uintRes, err = to.ToUint()
		res = int64(uintRes)
	}

	if types.ErrOverflow.Equal(err) {
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", val)
		err = b.ctx.GetStochastikVars().StmtCtx.HandleOverflow(err, warnErr)
	}

	return res, false, err
}

type builtinCastDecimalAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsStringSig) evalString(event chunk.Event) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(string(val.ToString()), b.tp, sc, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastDecimalAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsRealSig) evalReal(event chunk.Event) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	if b.inUnion && allegrosql.HasUnsignedFlag(b.tp.Flag) && val.IsNegative() {
		res = 0
	} else {
		res, err = val.ToFloat64()
	}
	return res, false, err
}

type builtinCastDecimalAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsTimeSig) evalTime(event chunk.Event) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ParseTimeFromFloatString(sc, string(val.ToString()), b.tp.Tp, int8(b.tp.Decimal))
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	if b.tp.Tp == allegrosql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, err
}

type builtinCastDecimalAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDecimalAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastDecimalAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsDurationSig) evalDuration(event chunk.Event) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDecimal(b.ctx, event)
	if isNull || err != nil {
		return res, true, err
	}
	res, err = types.ParseDuration(b.ctx.GetStochastikVars().StmtCtx, string(val.ToString()), int8(b.tp.Decimal))
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = b.ctx.GetStochastikVars().StmtCtx.HandleTruncate(err)
		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
		if res == types.ZeroDuration {
			return res, true, err
		}
	}
	return res, false, err
}

type builtinCastStringAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsStringSig) evalString(event chunk.Event) (res string, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(res, b.tp, sc, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastStringAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

// handleOverflow handles the overflow caused by cast string as int,
// see https://dev.allegrosql.com/doc/refman/5.7/en/out-of-range-and-overflow.html.
// When an out-of-range value is assigned to an integer defCausumn, MyALLEGROSQL stores the value representing the corresponding endpoint of the defCausumn data type range. If it is in select statement, it will return the
// endpoint value with a warning.
func (b *builtinCastStringAsIntSig) handleOverflow(origRes int64, origStr string, origErr error, isNegative bool) (res int64, err error) {
	res, err = origRes, origErr
	if err == nil {
		return
	}

	sc := b.ctx.GetStochastikVars().StmtCtx
	if types.ErrOverflow.Equal(origErr) {
		if isNegative {
			res = math.MinInt64
		} else {
			uval := uint64(math.MaxUint64)
			res = int64(uval)
		}
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("INTEGER", origStr)
		err = sc.HandleOverflow(origErr, warnErr)
	}
	return
}

func (b *builtinCastStringAsIntSig) evalInt(event chunk.Event) (res int64, isNull bool, err error) {
	if b.args[0].GetType().Hybrid() || IsBinaryLiteral(b.args[0]) {
		return b.args[0].EvalInt(b.ctx, event)
	}
	val, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}

	val = strings.TrimSpace(val)
	isNegative := false
	if len(val) > 1 && val[0] == '-' { // negative number
		isNegative = true
	}

	var ures uint64
	sc := b.ctx.GetStochastikVars().StmtCtx
	if !isNegative {
		ures, err = types.StrToUint(sc, val, true)
		res = int64(ures)

		if err == nil && !allegrosql.HasUnsignedFlag(b.tp.Flag) && ures > uint64(math.MaxInt64) {
			sc.AppendWarning(types.ErrCastAsSignedOverflow)
		}
	} else if b.inUnion && allegrosql.HasUnsignedFlag(b.tp.Flag) {
		res = 0
	} else {
		res, err = types.StrToInt(sc, val, true)
		if err == nil && allegrosql.HasUnsignedFlag(b.tp.Flag) {
			// If overflow, don't append this warnings
			sc.AppendWarning(types.ErrCastNegIntAsUnsigned)
		}
	}

	res, err = b.handleOverflow(res, val, err, isNegative)
	return res, false, err
}

type builtinCastStringAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsRealSig) evalReal(event chunk.Event) (res float64, isNull bool, err error) {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].EvalReal(b.ctx, event)
	}
	val, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sctx := b.ctx.GetStochastikVars().StmtCtx
	if val == "" && (sctx.InInsertStmt || sctx.InUFIDelateStmt) {
		return 0, false, nil
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.StrToFloat(sc, val, true)
	if err != nil {
		return 0, false, err
	}
	if b.inUnion && allegrosql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
		res = 0
	}
	res, err = types.ProduceFloatWithSpecifiedTp(res, b.tp, sc)
	return res, false, err
}

type builtinCastStringAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsDecimalSig) evalDecimal(event chunk.Event) (res *types.MyDecimal, isNull bool, err error) {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].EvalDecimal(b.ctx, event)
	}
	val, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	val = strings.TrimSpace(val)
	isNegative := len(val) > 1 && val[0] == '-'
	res = new(types.MyDecimal)
	sc := b.ctx.GetStochastikVars().StmtCtx
	if !(b.inUnion && allegrosql.HasUnsignedFlag(b.tp.Flag) && isNegative) {
		err = sc.HandleTruncate(res.FromString([]byte(val)))
		if err != nil {
			return res, false, err
		}
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, err
}

type builtinCastStringAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsTimeSig) evalTime(event chunk.Event) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ParseTime(sc, val, b.tp.Tp, int8(b.tp.Decimal))
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	if b.tp.Tp == allegrosql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return res, false, nil
}

type builtinCastStringAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastStringAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastStringAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsDurationSig) evalDuration(event chunk.Event) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseDuration(b.ctx.GetStochastikVars().StmtCtx, val, int8(b.tp.Decimal))
	if types.ErrTruncatedWrongVal.Equal(err) {
		sc := b.ctx.GetStochastikVars().StmtCtx
		err = sc.HandleTruncate(err)
		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
		if res == types.ZeroDuration {
			return res, true, err
		}
	}
	return res, false, err
}

type builtinCastTimeAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsTimeSig) evalTime(event chunk.Event) (res types.Time, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalTime(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}

	sc := b.ctx.GetStochastikVars().StmtCtx
	if res, err = res.Convert(sc, b.tp.Tp); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	res, err = res.RoundFrac(sc, int8(b.tp.Decimal))
	if b.tp.Tp == allegrosql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		res.SetType(b.tp.Tp)
	}
	return res, false, err
}

type builtinCastTimeAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsIntSig) evalInt(event chunk.Event) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	t, err := val.RoundFrac(sc, types.DefaultFsp)
	if err != nil {
		return res, false, err
	}
	res, err = t.ToNumber().ToInt()
	return res, false, err
}

type builtinCastTimeAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsRealSig) evalReal(event chunk.Event) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastTimeAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsDecimalSig) evalDecimal(event chunk.Event) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.tp, sc)
	return res, false, err
}

type builtinCastTimeAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsStringSig) evalString(event chunk.Event) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, sc, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

type builtinCastTimeAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastTimeAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsDurationSig) evalDuration(event chunk.Event) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ConvertToDuration()
	if err != nil {
		return res, false, err
	}
	res, err = res.RoundFrac(int8(b.tp.Decimal))
	return res, false, err
}

type builtinCastDurationAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsDurationSig) evalDuration(event chunk.Event) (res types.Duration, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalDuration(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = res.RoundFrac(int8(b.tp.Decimal))
	return res, false, err
}

type builtinCastDurationAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsIntSig) evalInt(event chunk.Event) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	dur, err := val.RoundFrac(types.DefaultFsp)
	if err != nil {
		return res, false, err
	}
	res, err = dur.ToNumber().ToInt()
	return res, false, err
}

type builtinCastDurationAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsRealSig) evalReal(event chunk.Event) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Fsp, err = types.CheckFsp(int(val.Fsp)); err != nil {
		return res, false, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastDurationAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsDecimalSig) evalDecimal(event chunk.Event) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Fsp, err = types.CheckFsp(int(val.Fsp)); err != nil {
		return res, false, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.tp, sc)
	return res, false, err
}

type builtinCastDurationAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsStringSig) evalString(event chunk.Event) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, sc, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, b.ctx)
}

func padZeroForBinaryType(s string, tp *types.FieldType, ctx stochastikctx.Context) (string, bool, error) {
	flen := tp.Flen
	if tp.Tp == allegrosql.TypeString && types.IsBinaryStr(tp) && len(s) < flen {
		sc := ctx.GetStochastikVars().StmtCtx
		valStr, _ := ctx.GetStochastikVars().GetSystemVar(variable.MaxAllowedPacket)
		maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
		if err != nil {
			return "", false, errors.Trace(err)
		}
		if uint64(flen) > maxAllowedPacket {
			sc.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("cast_as_binary", maxAllowedPacket))
			return "", true, nil
		}
		padding := make([]byte, flen-len(s))
		s = string(append([]byte(s), padding...))
	}
	return s, false, nil
}

type builtinCastDurationAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastDurationAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsTimeSig) evalTime(event chunk.Event) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = val.ConvertToTime(sc, b.tp.Tp)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	res, err = res.RoundFrac(sc, int8(b.tp.Decimal))
	return res, false, err
}

type builtinCastJSONAsJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsJSONSig) evalJSON(event chunk.Event) (res json.BinaryJSON, isNull bool, err error) {
	return b.args[0].EvalJSON(b.ctx, event)
}

type builtinCastJSONAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsIntSig) evalInt(event chunk.Event) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ConvertJSONToInt(sc, val, allegrosql.HasUnsignedFlag(b.tp.Flag))
	return
}

type builtinCastJSONAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsRealSig) evalReal(event chunk.Event) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ConvertJSONToFloat(sc, val)
	return
}

type builtinCastJSONAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsDecimalSig) evalDecimal(event chunk.Event) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ConvertJSONToDecimal(sc, val)
	if err != nil {
		return res, false, err
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.tp, sc)
	return res, false, err
}

type builtinCastJSONAsStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsStringSig) evalString(event chunk.Event) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	return val.String(), false, nil
}

type builtinCastJSONAsTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsTimeSig) evalTime(event chunk.Event) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	s, err := val.Unquote()
	if err != nil {
		return res, false, err
	}
	sc := b.ctx.GetStochastikVars().StmtCtx
	res, err = types.ParseTime(sc, s, b.tp.Tp, int8(b.tp.Decimal))
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	if b.tp.Tp == allegrosql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
	}
	return
}

type builtinCastJSONAsDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCastJSONAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsDurationSig) evalDuration(event chunk.Event) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}
	s, err := val.Unquote()
	if err != nil {
		return res, false, err
	}
	res, err = types.ParseDuration(b.ctx.GetStochastikVars().StmtCtx, s, int8(b.tp.Decimal))
	if types.ErrTruncatedWrongVal.Equal(err) {
		sc := b.ctx.GetStochastikVars().StmtCtx
		err = sc.HandleTruncate(err)
	}
	return
}

// inCastContext is stochastik key type that indicates whether executing
// in special cast context that negative unsigned num will be zero.
type inCastContext int

func (i inCastContext) String() string {
	return "__cast_ctx"
}

// inUnionCastContext is stochastik key value that indicates whether executing in
// union cast context.
// @see BuildCastFunction4Union
const inUnionCastContext inCastContext = 0

// hasSpecialCast checks if this expr has its own special cast function.
// for example(#9713): when doing arithmetic using results of function DayName,
// "Monday" should be regarded as 0, "Tuesday" should be regarded as 1 and so on.
func hasSpecialCast(ctx stochastikctx.Context, expr Expression, tp *types.FieldType) bool {
	switch f := expr.(type) {
	case *ScalarFunction:
		switch f.FuncName.L {
		case ast.DayName:
			switch tp.EvalType() {
			case types.ETInt, types.ETReal:
				return true
			}
		}
	}
	return false
}

// BuildCastFunction4Union build a implicitly CAST ScalarFunction from the Union
// Expression.
func BuildCastFunction4Union(ctx stochastikctx.Context, expr Expression, tp *types.FieldType) (res Expression) {
	ctx.SetValue(inUnionCastContext, struct{}{})
	defer func() {
		ctx.SetValue(inUnionCastContext, nil)
	}()
	return BuildCastFunction(ctx, expr, tp)
}

// BuildCastFunction builds a CAST ScalarFunction from the Expression.
func BuildCastFunction(ctx stochastikctx.Context, expr Expression, tp *types.FieldType) (res Expression) {
	if hasSpecialCast(ctx, expr, tp) {
		return expr
	}

	var fc functionClass
	switch tp.EvalType() {
	case types.ETInt:
		fc = &castAsIntFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDecimal:
		fc = &castAsDecimalFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETReal:
		fc = &castAsRealFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDatetime, types.ETTimestamp:
		fc = &castAsTimeFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDuration:
		fc = &castAsDurationFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETJson:
		fc = &castAsJSONFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETString:
		fc = &castAsStringFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	}
	f, err := fc.getFunction(ctx, []Expression{expr})
	terror.Log(err)
	res = &ScalarFunction{
		FuncName: perceptron.NewCIStr(ast.Cast),
		RetType:  tp,
		Function: f,
	}
	// We do not fold CAST if the eval type of this scalar function is ETJson
	// since we may reset the flag of the field type of CastAsJson later which
	// would affect the evaluation of it.
	if tp.EvalType() != types.ETJson {
		res = FoldConstant(res)
	}
	return res
}

// WrapWithCastAsInt wraps `expr` with `cast` if the return type of expr is not
// type int, otherwise, returns `expr` directly.
func WrapWithCastAsInt(ctx stochastikctx.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETInt {
		return expr
	}
	tp := types.NewFieldType(allegrosql.TypeLonglong)
	tp.Flen, tp.Decimal = expr.GetType().Flen, 0
	types.SetBinChsClnFlag(tp)
	tp.Flag |= expr.GetType().Flag & allegrosql.UnsignedFlag
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsReal wraps `expr` with `cast` if the return type of expr is not
// type real, otherwise, returns `expr` directly.
func WrapWithCastAsReal(ctx stochastikctx.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETReal {
		return expr
	}
	tp := types.NewFieldType(allegrosql.TypeDouble)
	tp.Flen, tp.Decimal = allegrosql.MaxRealWidth, types.UnspecifiedLength
	types.SetBinChsClnFlag(tp)
	tp.Flag |= expr.GetType().Flag & allegrosql.UnsignedFlag
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDecimal wraps `expr` with `cast` if the return type of expr is
// not type decimal, otherwise, returns `expr` directly.
func WrapWithCastAsDecimal(ctx stochastikctx.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETDecimal {
		return expr
	}
	tp := types.NewFieldType(allegrosql.TypeNewDecimal)
	tp.Flen, tp.Decimal = expr.GetType().Flen, expr.GetType().Decimal
	if expr.GetType().EvalType() == types.ETInt {
		tp.Flen = allegrosql.MaxIntWidth
	}
	types.SetBinChsClnFlag(tp)
	tp.Flag |= expr.GetType().Flag & allegrosql.UnsignedFlag
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsString wraps `expr` with `cast` if the return type of expr is
// not type string, otherwise, returns `expr` directly.
func WrapWithCastAsString(ctx stochastikctx.Context, expr Expression) Expression {
	exprTp := expr.GetType()
	if exprTp.EvalType() == types.ETString {
		return expr
	}
	argLen := exprTp.Flen
	// If expr is decimal, we should take the decimal point and negative sign
	// into consideration, so we set `expr.GetType().Flen + 2` as the `argLen`.
	// Since the length of float and double is not accurate, we do not handle
	// them.
	if exprTp.Tp == allegrosql.TypeNewDecimal && argLen != int(types.UnspecifiedFsp) {
		argLen += 2
	}
	if exprTp.EvalType() == types.ETInt {
		argLen = allegrosql.MaxIntWidth
	}
	tp := types.NewFieldType(allegrosql.TypeVarString)
	tp.Charset, tp.DefCauslate = expr.CharsetAndDefCauslation(ctx)
	tp.Flen, tp.Decimal = argLen, types.UnspecifiedLength
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsTime wraps `expr` with `cast` if the return type of expr is not
// same as type of the specified `tp` , otherwise, returns `expr` directly.
func WrapWithCastAsTime(ctx stochastikctx.Context, expr Expression, tp *types.FieldType) Expression {
	exprTp := expr.GetType().Tp
	if tp.Tp == exprTp {
		return expr
	} else if (exprTp == allegrosql.TypeDate || exprTp == allegrosql.TypeTimestamp) && tp.Tp == allegrosql.TypeDatetime {
		return expr
	}
	switch x := expr.GetType(); x.Tp {
	case allegrosql.TypeDatetime, allegrosql.TypeTimestamp, allegrosql.TypeDate, allegrosql.TypeDuration:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = int(types.MaxFsp)
	}
	switch tp.Tp {
	case allegrosql.TypeDate:
		tp.Flen = allegrosql.MaxDateWidth
	case allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		tp.Flen = allegrosql.MaxDatetimeWidthNoFsp
		if tp.Decimal > 0 {
			tp.Flen = tp.Flen + 1 + tp.Decimal
		}
	}
	types.SetBinChsClnFlag(tp)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDuration wraps `expr` with `cast` if the return type of expr is
// not type duration, otherwise, returns `expr` directly.
func WrapWithCastAsDuration(ctx stochastikctx.Context, expr Expression) Expression {
	if expr.GetType().Tp == allegrosql.TypeDuration {
		return expr
	}
	tp := types.NewFieldType(allegrosql.TypeDuration)
	switch x := expr.GetType(); x.Tp {
	case allegrosql.TypeDatetime, allegrosql.TypeTimestamp, allegrosql.TypeDate:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = int(types.MaxFsp)
	}
	tp.Flen = allegrosql.MaxDurationWidthNoFsp
	if tp.Decimal > 0 {
		tp.Flen = tp.Flen + 1 + tp.Decimal
	}
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsJSON wraps `expr` with `cast` if the return type of expr is not
// type json, otherwise, returns `expr` directly.
func WrapWithCastAsJSON(ctx stochastikctx.Context, expr Expression) Expression {
	if expr.GetType().Tp == allegrosql.TypeJSON && !allegrosql.HasParseToJSONFlag(expr.GetType().Flag) {
		return expr
	}
	tp := &types.FieldType{
		Tp:          allegrosql.TypeJSON,
		Flen:        12582912, // FIXME: Here the Flen is not trusted.
		Decimal:     0,
		Charset:     allegrosql.DefaultCharset,
		DefCauslate: allegrosql.DefaultDefCauslationName,
		Flag:        allegrosql.BinaryFlag,
	}
	return BuildCastFunction(ctx, expr, tp)
}
