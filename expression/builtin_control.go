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

package expression

import (
	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
)

var (
	_ functionClass = &caseWhenFunctionClass{}
	_ functionClass = &ifFunctionClass{}
	_ functionClass = &ifNullFunctionClass{}
)

var (
	_ builtinFunc = &builtinCaseWhenIntSig{}
	_ builtinFunc = &builtinCaseWhenRealSig{}
	_ builtinFunc = &builtinCaseWhenDecimalSig{}
	_ builtinFunc = &builtinCaseWhenStringSig{}
	_ builtinFunc = &builtinCaseWhenTimeSig{}
	_ builtinFunc = &builtinCaseWhenDurationSig{}
	_ builtinFunc = &builtinCaseWhenJSONSig{}
	_ builtinFunc = &builtinIfNullIntSig{}
	_ builtinFunc = &builtinIfNullRealSig{}
	_ builtinFunc = &builtinIfNullDecimalSig{}
	_ builtinFunc = &builtinIfNullStringSig{}
	_ builtinFunc = &builtinIfNullTimeSig{}
	_ builtinFunc = &builtinIfNullDurationSig{}
	_ builtinFunc = &builtinIfNullJSONSig{}
	_ builtinFunc = &builtinIfIntSig{}
	_ builtinFunc = &builtinIfRealSig{}
	_ builtinFunc = &builtinIfDecimalSig{}
	_ builtinFunc = &builtinIfStringSig{}
	_ builtinFunc = &builtinIfTimeSig{}
	_ builtinFunc = &builtinIfDurationSig{}
	_ builtinFunc = &builtinIfJSONSig{}
)

// InferType4ControlFuncs infer result type for builtin IF, IFNULL, NULLIF, LEAD and LAG.
func InferType4ControlFuncs(lexp, rexp Expression) *types.FieldType {
	lhs, rhs := lexp.GetType(), rexp.GetType()
	resultFieldType := &types.FieldType{}
	if lhs.Tp == allegrosql.TypeNull {
		*resultFieldType = *rhs
		// If both arguments are NULL, make resulting type BINARY(0).
		if rhs.Tp == allegrosql.TypeNull {
			resultFieldType.Tp = allegrosql.TypeString
			resultFieldType.Flen, resultFieldType.Decimal = 0, 0
			types.SetBinChsClnFlag(resultFieldType)
		}
	} else if rhs.Tp == allegrosql.TypeNull {
		*resultFieldType = *lhs
	} else {
		resultFieldType = types.AggFieldType([]*types.FieldType{lhs, rhs})
		evalType := types.AggregateEvalType([]*types.FieldType{lhs, rhs}, &resultFieldType.Flag)
		if evalType == types.CausetEDN {
			resultFieldType.Decimal = 0
		} else {
			if lhs.Decimal == types.UnspecifiedLength || rhs.Decimal == types.UnspecifiedLength {
				resultFieldType.Decimal = types.UnspecifiedLength
			} else {
				resultFieldType.Decimal = mathutil.Max(lhs.Decimal, rhs.Decimal)
			}
		}
		if types.IsNonBinaryStr(lhs) && !types.IsBinaryStr(rhs) {
			resultFieldType.DefCauslate, resultFieldType.Charset, _, _ = inferDefCauslation(lexp, rexp)
			resultFieldType.Flag = 0
			if allegrosql.HasBinaryFlag(lhs.Flag) || !types.IsNonBinaryStr(rhs) {
				resultFieldType.Flag |= allegrosql.BinaryFlag
			}
		} else if types.IsNonBinaryStr(rhs) && !types.IsBinaryStr(lhs) {
			resultFieldType.DefCauslate, resultFieldType.Charset, _, _ = inferDefCauslation(lexp, rexp)
			resultFieldType.Flag = 0
			if allegrosql.HasBinaryFlag(rhs.Flag) || !types.IsNonBinaryStr(lhs) {
				resultFieldType.Flag |= allegrosql.BinaryFlag
			}
		} else if types.IsBinaryStr(lhs) || types.IsBinaryStr(rhs) || !evalType.IsStringHoTT() {
			types.SetBinChsClnFlag(resultFieldType)
		} else {
			resultFieldType.Charset, resultFieldType.DefCauslate, resultFieldType.Flag = allegrosql.DefaultCharset, allegrosql.DefaultDefCauslationName, 0
		}
		if evalType == types.ETDecimal || evalType == types.CausetEDN {
			lhsUnsignedFlag, rhsUnsignedFlag := allegrosql.HasUnsignedFlag(lhs.Flag), allegrosql.HasUnsignedFlag(rhs.Flag)
			lhsFlagLen, rhsFlagLen := 0, 0
			if !lhsUnsignedFlag {
				lhsFlagLen = 1
			}
			if !rhsUnsignedFlag {
				rhsFlagLen = 1
			}
			lhsFlen := lhs.Flen - lhsFlagLen
			rhsFlen := rhs.Flen - rhsFlagLen
			if lhs.Decimal != types.UnspecifiedLength {
				lhsFlen -= lhs.Decimal
			}
			if lhs.Decimal != types.UnspecifiedLength {
				rhsFlen -= rhs.Decimal
			}
			resultFieldType.Flen = mathutil.Max(lhsFlen, rhsFlen) + resultFieldType.Decimal + 1
		} else {
			resultFieldType.Flen = mathutil.Max(lhs.Flen, rhs.Flen)
		}
	}
	// Fix decimal for int and string.
	resultEvalType := resultFieldType.EvalType()
	if resultEvalType == types.CausetEDN {
		resultFieldType.Decimal = 0
	} else if resultEvalType == types.ETString {
		if lhs.Tp != allegrosql.TypeNull || rhs.Tp != allegrosql.TypeNull {
			resultFieldType.Decimal = types.UnspecifiedLength
		}
	}
	return resultFieldType
}

type caseWhenFunctionClass struct {
	baseFunctionClass
}

func (c *caseWhenFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	l := len(args)
	// Fill in each 'THEN' clause parameter type.
	fieldTps := make([]*types.FieldType, 0, (l+1)/2)
	decimal, flen, isBinaryStr, isBinaryFlag := args[1].GetType().Decimal, 0, false, false
	for i := 1; i < l; i += 2 {
		fieldTps = append(fieldTps, args[i].GetType())
		decimal = mathutil.Max(decimal, args[i].GetType().Decimal)
		if args[i].GetType().Flen == -1 {
			flen = -1
		} else if flen != -1 {
			flen = mathutil.Max(flen, args[i].GetType().Flen)
		}
		isBinaryStr = isBinaryStr || types.IsBinaryStr(args[i].GetType())
		isBinaryFlag = isBinaryFlag || !types.IsNonBinaryStr(args[i].GetType())
	}
	if l%2 == 1 {
		fieldTps = append(fieldTps, args[l-1].GetType())
		decimal = mathutil.Max(decimal, args[l-1].GetType().Decimal)
		if args[l-1].GetType().Flen == -1 {
			flen = -1
		} else if flen != -1 {
			flen = mathutil.Max(flen, args[l-1].GetType().Flen)
		}
		isBinaryStr = isBinaryStr || types.IsBinaryStr(args[l-1].GetType())
		isBinaryFlag = isBinaryFlag || !types.IsNonBinaryStr(args[l-1].GetType())
	}

	fieldTp := types.AggFieldType(fieldTps)
	tp := fieldTp.EvalType()

	if tp == types.CausetEDN {
		decimal = 0
	}
	fieldTp.Decimal, fieldTp.Flen = decimal, flen
	if fieldTp.EvalType().IsStringHoTT() && !isBinaryStr {
		fieldTp.Charset, fieldTp.DefCauslate = charset.CharsetUTF8MB4, charset.DefCauslationUTF8MB4
	}
	if isBinaryFlag {
		fieldTp.Flag |= allegrosql.BinaryFlag
	}
	// Set retType to BINARY(0) if all arguments are of type NULL.
	if fieldTp.Tp == allegrosql.TypeNull {
		fieldTp.Flen, fieldTp.Decimal = 0, types.UnspecifiedLength
		types.SetBinChsClnFlag(fieldTp)
	}
	argTps := make([]types.EvalType, 0, l)
	for i := 0; i < l-1; i += 2 {
		if args[i], err = wrapWithIsTrue(ctx, true, args[i], false); err != nil {
			return nil, err
		}
		argTps = append(argTps, types.CausetEDN, tp)
	}
	if l%2 == 1 {
		argTps = append(argTps, tp)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, tp, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp = fieldTp

	switch tp {
	case types.CausetEDN:
		bf.tp.Decimal = 0
		sig = &builtinCaseWhenIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CaseWhenInt)
	case types.ETReal:
		sig = &builtinCaseWhenRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CaseWhenReal)
	case types.ETDecimal:
		sig = &builtinCaseWhenDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CaseWhenDecimal)
	case types.ETString:
		bf.tp.Decimal = types.UnspecifiedLength
		sig = &builtinCaseWhenStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CaseWhenString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCaseWhenTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CaseWhenTime)
	case types.ETDuration:
		sig = &builtinCaseWhenDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CaseWhenDuration)
	case types.ETJson:
		sig = &builtinCaseWhenJSONSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_CaseWhenJson)
	}
	return sig, nil
}

type builtinCaseWhenIntSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenIntSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCaseWhenIntSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenIntSig) evalInt(event chunk.Event) (ret int64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, event)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalInt(b.ctx, event)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalInt(b.ctx, event)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenRealSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenRealSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCaseWhenRealSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenRealSig) evalReal(event chunk.Event) (ret float64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, event)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalReal(b.ctx, event)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalReal(b.ctx, event)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenDecimalSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCaseWhenDecimalSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenDecimalSig) evalDecimal(event chunk.Event) (ret *types.MyDecimal, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, event)
		if err != nil {
			return nil, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDecimal(b.ctx, event)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDecimal(b.ctx, event)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenStringSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenStringSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCaseWhenStringSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenStringSig) evalString(event chunk.Event) (ret string, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, event)
		if err != nil {
			return "", isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalString(b.ctx, event)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalString(b.ctx, event)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenTimeSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinCaseWhenTimeSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenTimeSig) evalTime(event chunk.Event) (ret types.Time, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, event)
		if err != nil {
			return ret, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalTime(b.ctx, event)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalTime(b.ctx, event)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenDurationSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinCaseWhenDurationSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenDurationSig) evalDuration(event chunk.Event) (ret types.Duration, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, event)
		if err != nil {
			return ret, true, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDuration(b.ctx, event)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDuration(b.ctx, event)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinCaseWhenJSONSig) Clone() builtinFunc {
	newSig := &builtinCaseWhenJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinCaseWhenJSONSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case
func (b *builtinCaseWhenJSONSig) evalJSON(event chunk.Event) (ret json.BinaryJSON, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.ctx, event)
		if err != nil {
			return
		}
		if isNull || condition == 0 {
			continue
		}
		return args[i+1].EvalJSON(b.ctx, event)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		return args[l-1].EvalJSON(b.ctx, event)
	}
	return ret, true, nil
}

type ifFunctionClass struct {
	baseFunctionClass
}

// getFunction see https://dev.allegrosql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (c *ifFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	retTp := InferType4ControlFuncs(args[1], args[2])
	evalTps := retTp.EvalType()
	args[0], err = wrapWithIsTrue(ctx, true, args[0], false)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, evalTps, types.CausetEDN, evalTps, evalTps)
	if err != nil {
		return nil, err
	}
	retTp.Flag |= bf.tp.Flag
	bf.tp = retTp
	switch evalTps {
	case types.CausetEDN:
		sig = &builtinIfIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfInt)
	case types.ETReal:
		sig = &builtinIfRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfReal)
	case types.ETDecimal:
		sig = &builtinIfDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfDecimal)
	case types.ETString:
		sig = &builtinIfStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinIfTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfTime)
	case types.ETDuration:
		sig = &builtinIfDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfDuration)
	case types.ETJson:
		sig = &builtinIfJSONSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfJson)
	}
	return sig, nil
}

type builtinIfIntSig struct {
	baseBuiltinFunc
}

func (b *builtinIfIntSig) Clone() builtinFunc {
	newSig := &builtinIfIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfIntSig) evalInt(event chunk.Event) (ret int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, event)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalInt(b.ctx, event)
	}
	return b.args[2].EvalInt(b.ctx, event)
}

type builtinIfRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfRealSig) Clone() builtinFunc {
	newSig := &builtinIfRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfRealSig) evalReal(event chunk.Event) (ret float64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, event)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalReal(b.ctx, event)
	}
	return b.args[2].EvalReal(b.ctx, event)
}

type builtinIfDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDecimalSig) Clone() builtinFunc {
	newSig := &builtinIfDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDecimalSig) evalDecimal(event chunk.Event) (ret *types.MyDecimal, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, event)
	if err != nil {
		return nil, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalDecimal(b.ctx, event)
	}
	return b.args[2].EvalDecimal(b.ctx, event)
}

type builtinIfStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfStringSig) Clone() builtinFunc {
	newSig := &builtinIfStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfStringSig) evalString(event chunk.Event) (ret string, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, event)
	if err != nil {
		return "", true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalString(b.ctx, event)
	}
	return b.args[2].EvalString(b.ctx, event)
}

type builtinIfTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinIfTimeSig) Clone() builtinFunc {
	newSig := &builtinIfTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfTimeSig) evalTime(event chunk.Event) (ret types.Time, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, event)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalTime(b.ctx, event)
	}
	return b.args[2].EvalTime(b.ctx, event)
}

type builtinIfDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinIfDurationSig) Clone() builtinFunc {
	newSig := &builtinIfDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfDurationSig) evalDuration(event chunk.Event) (ret types.Duration, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, event)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalDuration(b.ctx, event)
	}
	return b.args[2].EvalDuration(b.ctx, event)
}

type builtinIfJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinIfJSONSig) Clone() builtinFunc {
	newSig := &builtinIfJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfJSONSig) evalJSON(event chunk.Event) (ret json.BinaryJSON, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, event)
	if err != nil {
		return ret, true, err
	}
	if !isNull0 && arg0 != 0 {
		return b.args[1].EvalJSON(b.ctx, event)
	}
	return b.args[2].EvalJSON(b.ctx, event)
}

type ifNullFunctionClass struct {
	baseFunctionClass
}

func (c *ifNullFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhs, rhs := args[0].GetType(), args[1].GetType()
	retTp := InferType4ControlFuncs(args[0], args[1])
	retTp.Flag |= (lhs.Flag & allegrosql.NotNullFlag) | (rhs.Flag & allegrosql.NotNullFlag)
	if lhs.Tp == allegrosql.TypeNull && rhs.Tp == allegrosql.TypeNull {
		retTp.Tp = allegrosql.TypeNull
		retTp.Flen, retTp.Decimal = 0, -1
		types.SetBinChsClnFlag(retTp)
	}
	evalTps := retTp.EvalType()
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, evalTps, evalTps, evalTps)
	if err != nil {
		return nil, err
	}
	bf.tp = retTp
	switch evalTps {
	case types.CausetEDN:
		sig = &builtinIfNullIntSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfNullInt)
	case types.ETReal:
		sig = &builtinIfNullRealSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfNullReal)
	case types.ETDecimal:
		sig = &builtinIfNullDecimalSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfNullDecimal)
	case types.ETString:
		sig = &builtinIfNullStringSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfNullString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinIfNullTimeSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfNullTime)
	case types.ETDuration:
		sig = &builtinIfNullDurationSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfNullDuration)
	case types.ETJson:
		sig = &builtinIfNullJSONSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_IfNullJson)
	}
	return sig, nil
}

type builtinIfNullIntSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullIntSig) Clone() builtinFunc {
	newSig := &builtinIfNullIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullIntSig) evalInt(event chunk.Event) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalInt(b.ctx, event)
	return arg1, isNull || err != nil, err
}

type builtinIfNullRealSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullRealSig) Clone() builtinFunc {
	newSig := &builtinIfNullRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullRealSig) evalReal(event chunk.Event) (float64, bool, error) {
	arg0, isNull, err := b.args[0].EvalReal(b.ctx, event)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalReal(b.ctx, event)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullDecimalSig) Clone() builtinFunc {
	newSig := &builtinIfNullDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDecimalSig) evalDecimal(event chunk.Event) (*types.MyDecimal, bool, error) {
	arg0, isNull, err := b.args[0].EvalDecimal(b.ctx, event)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalDecimal(b.ctx, event)
	return arg1, isNull || err != nil, err
}

type builtinIfNullStringSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullStringSig) Clone() builtinFunc {
	newSig := &builtinIfNullStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullStringSig) evalString(event chunk.Event) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalString(b.ctx, event)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalString(b.ctx, event)
	return arg1, isNull || err != nil, err
}

type builtinIfNullTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullTimeSig) Clone() builtinFunc {
	newSig := &builtinIfNullTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullTimeSig) evalTime(event chunk.Event) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, event)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalTime(b.ctx, event)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullDurationSig) Clone() builtinFunc {
	newSig := &builtinIfNullDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDurationSig) evalDuration(event chunk.Event) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, event)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, event)
	return arg1, isNull || err != nil, err
}

type builtinIfNullJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinIfNullJSONSig) Clone() builtinFunc {
	newSig := &builtinIfNullJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullJSONSig) evalJSON(event chunk.Event) (json.BinaryJSON, bool, error) {
	arg0, isNull, err := b.args[0].EvalJSON(b.ctx, event)
	if !isNull {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.args[1].EvalJSON(b.ctx, event)
	return arg1, isNull || err != nil, err
}
