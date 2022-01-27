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

package expression

import (
	"strings"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

var (
	_ functionClass = &inFunctionClass{}
	_ functionClass = &rowFunctionClass{}
	_ functionClass = &setVarFunctionClass{}
	_ functionClass = &getVarFunctionClass{}
	_ functionClass = &lockFunctionClass{}
	_ functionClass = &releaseLockFunctionClass{}
	_ functionClass = &valuesFunctionClass{}
	_ functionClass = &bitCountFunctionClass{}
	_ functionClass = &getParamFunctionClass{}
)

var (
	_ builtinFunc = &builtinSleepSig{}
	_ builtinFunc = &builtinInIntSig{}
	_ builtinFunc = &builtinInStringSig{}
	_ builtinFunc = &builtinInDecimalSig{}
	_ builtinFunc = &builtinInRealSig{}
	_ builtinFunc = &builtinInTimeSig{}
	_ builtinFunc = &builtinInDurationSig{}
	_ builtinFunc = &builtinInJSONSig{}
	_ builtinFunc = &builtinEventSig{}
	_ builtinFunc = &builtinSetVarSig{}
	_ builtinFunc = &builtinGetVarSig{}
	_ builtinFunc = &builtinLockSig{}
	_ builtinFunc = &builtinReleaseLockSig{}
	_ builtinFunc = &builtinValuesIntSig{}
	_ builtinFunc = &builtinValuesRealSig{}
	_ builtinFunc = &builtinValuesDecimalSig{}
	_ builtinFunc = &builtinValuesStringSig{}
	_ builtinFunc = &builtinValuesTimeSig{}
	_ builtinFunc = &builtinValuesDurationSig{}
	_ builtinFunc = &builtinValuesJSONSig{}
	_ builtinFunc = &builtinBitCountSig{}
	_ builtinFunc = &builtinGetParamStringSig{}
)

type inFunctionClass struct {
	baseFunctionClass
}

func (c *inFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = args[0].GetType().EvalType()
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	switch args[0].GetType().EvalType() {
	case types.ETInt:
		inInt := builtinInIntSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inInt.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inInt, err
		}
		sig = &inInt
		sig.setPbCode(fidelpb.ScalarFuncSig_InInt)
	case types.ETString:
		inStr := builtinInStringSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inStr.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inStr, err
		}
		sig = &inStr
		sig.setPbCode(fidelpb.ScalarFuncSig_InString)
	case types.ETReal:
		inReal := builtinInRealSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inReal.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inReal, err
		}
		sig = &inReal
		sig.setPbCode(fidelpb.ScalarFuncSig_InReal)
	case types.ETDecimal:
		inDecimal := builtinInDecimalSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inDecimal.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inDecimal, err
		}
		sig = &inDecimal
		sig.setPbCode(fidelpb.ScalarFuncSig_InDecimal)
	case types.ETDatetime, types.ETTimestamp:
		inTime := builtinInTimeSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inTime.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inTime, err
		}
		sig = &inTime
		sig.setPbCode(fidelpb.ScalarFuncSig_InTime)
	case types.ETDuration:
		inDuration := builtinInDurationSig{baseInSig: baseInSig{baseBuiltinFunc: bf}}
		err := inDuration.buildHashMapForConstArgs(ctx)
		if err != nil {
			return &inDuration, err
		}
		sig = &inDuration
		sig.setPbCode(fidelpb.ScalarFuncSig_InDuration)
	case types.ETJson:
		sig = &builtinInJSONSig{baseBuiltinFunc: bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_InJson)
	}
	return sig, nil
}

type baseInSig struct {
	baseBuiltinFunc
	nonConstArgs []Expression
	hasNull      bool
}

// builtinInIntSig see https://dev.allegrosql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInIntSig struct {
	baseInSig
	// the bool value in the map is used to identify whether the constant stored in key is signed or unsigned
	hashSet map[int64]bool
}

func (b *builtinInIntSig) buildHashMapForConstArgs(ctx stochastikctx.Context) error {
	b.nonConstArgs = []Expression{b.args[0]}
	b.hashSet = make(map[int64]bool, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetStochastikVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalInt(ctx, chunk.Event{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet[val] = allegrosql.HasUnsignedFlag(b.args[i].GetType().Flag)
		} else {
			b.nonConstArgs = append(b.nonConstArgs, b.args[i])
		}
	}
	return nil
}

func (b *builtinInIntSig) Clone() builtinFunc {
	newSig := &builtinInIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgs = make([]Expression, 0, len(b.nonConstArgs))
	for _, arg := range b.nonConstArgs {
		newSig.nonConstArgs = append(newSig.nonConstArgs, arg.Clone())
	}
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInIntSig) evalInt(event chunk.Event) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, event)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	isUnsigned0 := allegrosql.HasUnsignedFlag(b.args[0].GetType().Flag)

	args := b.args
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		if isUnsigned, ok := b.hashSet[arg0]; ok {
			if (isUnsigned0 && isUnsigned) || (!isUnsigned0 && !isUnsigned) {
				return 1, false, nil
			}
			if arg0 >= 0 {
				return 1, false, nil
			}
		}
	}

	hasNull := b.hasNull
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalInt(b.ctx, event)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		isUnsigned := allegrosql.HasUnsignedFlag(arg.GetType().Flag)
		if isUnsigned0 && isUnsigned {
			if evaledArg == arg0 {
				return 1, false, nil
			}
		} else if !isUnsigned0 && !isUnsigned {
			if evaledArg == arg0 {
				return 1, false, nil
			}
		} else if !isUnsigned0 && isUnsigned {
			if arg0 >= 0 && evaledArg == arg0 {
				return 1, false, nil
			}
		} else {
			if evaledArg >= 0 && evaledArg == arg0 {
				return 1, false, nil
			}
		}
	}
	return 0, hasNull, nil
}

// builtinInStringSig see https://dev.allegrosql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInStringSig struct {
	baseInSig
	hashSet set.StringSet
}

func (b *builtinInStringSig) buildHashMapForConstArgs(ctx stochastikctx.Context) error {
	b.nonConstArgs = []Expression{b.args[0]}
	b.hashSet = set.NewStringSet()
	defCauslator := defCauslate.GetDefCauslator(b.defCauslation)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetStochastikVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalString(ctx, chunk.Event{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet.Insert(string(defCauslator.Key(val))) // should do memory INTERLOCKy here
		} else {
			b.nonConstArgs = append(b.nonConstArgs, b.args[i])
		}
	}

	return nil
}

func (b *builtinInStringSig) Clone() builtinFunc {
	newSig := &builtinInStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgs = make([]Expression, 0, len(b.nonConstArgs))
	for _, arg := range b.nonConstArgs {
		newSig.nonConstArgs = append(newSig.nonConstArgs, arg.Clone())
	}
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInStringSig) evalInt(event chunk.Event) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalString(b.ctx, event)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}

	args := b.args
	defCauslator := defCauslate.GetDefCauslator(b.defCauslation)
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		if b.hashSet.Exist(string(defCauslator.Key(arg0))) {
			return 1, false, nil
		}
	}

	hasNull := b.hasNull
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalString(b.ctx, event)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if types.CompareString(arg0, evaledArg, b.defCauslation) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInRealSig see https://dev.allegrosql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInRealSig struct {
	baseInSig
	hashSet set.Float64Set
}

func (b *builtinInRealSig) buildHashMapForConstArgs(ctx stochastikctx.Context) error {
	b.nonConstArgs = []Expression{b.args[0]}
	b.hashSet = set.NewFloat64Set()
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetStochastikVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalReal(ctx, chunk.Event{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet.Insert(val)
		} else {
			b.nonConstArgs = append(b.nonConstArgs, b.args[i])
		}
	}

	return nil
}

func (b *builtinInRealSig) Clone() builtinFunc {
	newSig := &builtinInRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgs = make([]Expression, 0, len(b.nonConstArgs))
	for _, arg := range b.nonConstArgs {
		newSig.nonConstArgs = append(newSig.nonConstArgs, arg.Clone())
	}
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInRealSig) evalInt(event chunk.Event) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalReal(b.ctx, event)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	args := b.args
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		if b.hashSet.Exist(arg0) {
			return 1, false, nil
		}
	}
	hasNull := b.hasNull
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalReal(b.ctx, event)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0 == evaledArg {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInDecimalSig see https://dev.allegrosql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDecimalSig struct {
	baseInSig
	hashSet set.StringSet
}

func (b *builtinInDecimalSig) buildHashMapForConstArgs(ctx stochastikctx.Context) error {
	b.nonConstArgs = []Expression{b.args[0]}
	b.hashSet = set.NewStringSet()
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetStochastikVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalDecimal(ctx, chunk.Event{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			key, err := val.ToHashKey()
			if err != nil {
				return err
			}
			b.hashSet.Insert(string(key))
		} else {
			b.nonConstArgs = append(b.nonConstArgs, b.args[i])
		}
	}

	return nil
}

func (b *builtinInDecimalSig) Clone() builtinFunc {
	newSig := &builtinInDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgs = make([]Expression, 0, len(b.nonConstArgs))
	for _, arg := range b.nonConstArgs {
		newSig.nonConstArgs = append(newSig.nonConstArgs, arg.Clone())
	}
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInDecimalSig) evalInt(event chunk.Event) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalDecimal(b.ctx, event)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}

	args := b.args
	key, err := arg0.ToHashKey()
	if err != nil {
		return 0, true, err
	}
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		if b.hashSet.Exist(string(key)) {
			return 1, false, nil
		}
	}

	hasNull := b.hasNull
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalDecimal(b.ctx, event)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInTimeSig see https://dev.allegrosql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInTimeSig struct {
	baseInSig
	hashSet map[types.Time]struct{}
}

func (b *builtinInTimeSig) buildHashMapForConstArgs(ctx stochastikctx.Context) error {
	b.nonConstArgs = []Expression{b.args[0]}
	b.hashSet = make(map[types.Time]struct{}, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetStochastikVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalTime(ctx, chunk.Event{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet[val] = struct{}{}
		} else {
			b.nonConstArgs = append(b.nonConstArgs, b.args[i])
		}
	}

	return nil
}

func (b *builtinInTimeSig) Clone() builtinFunc {
	newSig := &builtinInTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgs = make([]Expression, 0, len(b.nonConstArgs))
	for _, arg := range b.nonConstArgs {
		newSig.nonConstArgs = append(newSig.nonConstArgs, arg.Clone())
	}
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInTimeSig) evalInt(event chunk.Event) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalTime(b.ctx, event)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	args := b.args
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		if _, ok := b.hashSet[arg0]; ok {
			return 1, false, nil
		}
	}
	hasNull := b.hasNull
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalTime(b.ctx, event)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInDurationSig see https://dev.allegrosql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDurationSig struct {
	baseInSig
	hashSet map[time.Duration]struct{}
}

func (b *builtinInDurationSig) buildHashMapForConstArgs(ctx stochastikctx.Context) error {
	b.nonConstArgs = []Expression{b.args[0]}
	b.hashSet = make(map[time.Duration]struct{}, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		if b.args[i].ConstItem(b.ctx.GetStochastikVars().StmtCtx) {
			val, isNull, err := b.args[i].EvalDuration(ctx, chunk.Event{})
			if err != nil {
				return err
			}
			if isNull {
				b.hasNull = true
				continue
			}
			b.hashSet[val.Duration] = struct{}{}
		} else {
			b.nonConstArgs = append(b.nonConstArgs, b.args[i])
		}
	}

	return nil
}

func (b *builtinInDurationSig) Clone() builtinFunc {
	newSig := &builtinInDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.nonConstArgs = make([]Expression, 0, len(b.nonConstArgs))
	for _, arg := range b.nonConstArgs {
		newSig.nonConstArgs = append(newSig.nonConstArgs, arg.Clone())
	}
	newSig.hashSet = b.hashSet
	newSig.hasNull = b.hasNull
	return newSig
}

func (b *builtinInDurationSig) evalInt(event chunk.Event) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalDuration(b.ctx, event)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	args := b.args
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		if _, ok := b.hashSet[arg0.Duration]; ok {
			return 1, false, nil
		}
	}
	hasNull := b.hasNull
	for _, arg := range args[1:] {
		evaledArg, isNull, err := arg.EvalDuration(b.ctx, event)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInJSONSig see https://dev.allegrosql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinInJSONSig) Clone() builtinFunc {
	newSig := &builtinInJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinInJSONSig) evalInt(event chunk.Event) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalJSON(b.ctx, event)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.args[1:] {
		evaledArg, isNull, err := arg.EvalJSON(b.ctx, event)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		result := json.CompareBinary(evaledArg, arg0)
		if result == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

type rowFunctionClass struct {
	baseFunctionClass
}

func (c *rowFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range argTps {
		argTps[i] = args[i].GetType().EvalType()
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	sig = &builtinEventSig{bf}
	return sig, nil
}

type builtinEventSig struct {
	baseBuiltinFunc
}

func (b *builtinEventSig) Clone() builtinFunc {
	newSig := &builtinEventSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString rowFunc should always be flattened in expression rewrite phrase.
func (b *builtinEventSig) evalString(event chunk.Event) (string, bool, error) {
	panic("builtinEventSig.evalString() should never be called.")
}

type setVarFunctionClass struct {
	baseFunctionClass
}

func (c *setVarFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = args[1].GetType().Flen
	// TODO: we should consider the type of the argument, but not take it as string for all situations.
	sig = &builtinSetVarSig{bf}
	return sig, err
}

type builtinSetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinSetVarSig) Clone() builtinFunc {
	newSig := &builtinSetVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetVarSig) evalString(event chunk.Event) (res string, isNull bool, err error) {
	var varName string
	stochastikVars := b.ctx.GetStochastikVars()
	varName, isNull, err = b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}

	causet, err := b.args[1].Eval(event)
	isNull = causet.IsNull()
	if isNull || err != nil {
		return "", isNull, err
	}

	res, err = causet.ToString()
	if err != nil {
		return "", isNull, err
	}

	varName = strings.ToLower(varName)
	stochastikVars.UsersLock.Lock()
	stochastikVars.SetUserVar(varName, stringutil.INTERLOCKy(res), causet.DefCauslation())
	stochastikVars.UsersLock.Unlock()
	return res, false, nil
}

type getVarFunctionClass struct {
	baseFunctionClass
}

func (c *getVarFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	// TODO: we should consider the type of the argument, but not take it as string for all situations.
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = allegrosql.MaxFieldVarCharLength
	if err := c.resolveDefCauslation(ctx, args, &bf); err != nil {
		return nil, err
	}
	sig = &builtinGetVarSig{bf}
	return sig, nil
}

func (c *getVarFunctionClass) resolveDefCauslation(ctx stochastikctx.Context, args []Expression, bf *baseBuiltinFunc) (err error) {
	if constant, ok := args[0].(*Constant); ok {
		varName, err := constant.Value.ToString()
		if err != nil {
			return err
		}
		varName = strings.ToLower(varName)
		ctx.GetStochastikVars().UsersLock.RLock()
		defer ctx.GetStochastikVars().UsersLock.RUnlock()
		if v, ok := ctx.GetStochastikVars().Users[varName]; ok {
			bf.tp.DefCauslate = v.DefCauslation()
			if len(bf.tp.Charset) <= 0 {
				charset, _ := ctx.GetStochastikVars().GetCharsetInfo()
				bf.tp.Charset = charset
			}
			return nil
		}
	}

	return nil
}

type builtinGetVarSig struct {
	baseBuiltinFunc
}

func (b *builtinGetVarSig) Clone() builtinFunc {
	newSig := &builtinGetVarSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetVarSig) evalString(event chunk.Event) (string, bool, error) {
	stochastikVars := b.ctx.GetStochastikVars()
	varName, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	varName = strings.ToLower(varName)
	stochastikVars.UsersLock.RLock()
	defer stochastikVars.UsersLock.RUnlock()
	if v, ok := stochastikVars.Users[varName]; ok {
		return v.GetString(), false, nil
	}
	return "", true, nil
}

type valuesFunctionClass struct {
	baseFunctionClass

	offset int
	tp     *types.FieldType
}

func (c *valuesFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFunc(ctx, c.funcName, args, c.tp.EvalType())
	if err != nil {
		return nil, err
	}
	bf.tp = c.tp
	switch c.tp.EvalType() {
	case types.ETInt:
		sig = &builtinValuesIntSig{bf, c.offset}
	case types.ETReal:
		sig = &builtinValuesRealSig{bf, c.offset}
	case types.ETDecimal:
		sig = &builtinValuesDecimalSig{bf, c.offset}
	case types.ETString:
		sig = &builtinValuesStringSig{bf, c.offset}
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinValuesTimeSig{bf, c.offset}
	case types.ETDuration:
		sig = &builtinValuesDurationSig{bf, c.offset}
	case types.ETJson:
		sig = &builtinValuesJSONSig{bf, c.offset}
	}
	return sig, nil
}

type builtinValuesIntSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesIntSig) Clone() builtinFunc {
	newSig := &builtinValuesIntSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinValuesIntSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesIntSig) evalInt(_ chunk.Event) (int64, bool, error) {
	if !b.ctx.GetStochastikVars().StmtCtx.InInsertStmt {
		return 0, true, nil
	}
	event := b.ctx.GetStochastikVars().CurrInsertValues
	if event.IsEmpty() {
		return 0, true, errors.New("Stochastik current insert values is nil")
	}
	if b.offset < event.Len() {
		if event.IsNull(b.offset) {
			return 0, true, nil
		}
		// For BinaryLiteral, see issue #15310
		val := event.GetRaw(b.offset)
		if len(val) > 8 {
			return 0, true, errors.New("Stochastik current insert values is too long")
		}
		if len(val) < 8 {
			var binary types.BinaryLiteral = val
			v, err := binary.ToInt(b.ctx.GetStochastikVars().StmtCtx)
			if err != nil {
				return 0, true, errors.Trace(err)
			}
			return int64(v), false, nil
		}
		return event.GetInt64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Stochastik current insert values len %d and defCausumn's offset %v don't match", event.Len(), b.offset)
}

type builtinValuesRealSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesRealSig) Clone() builtinFunc {
	newSig := &builtinValuesRealSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinValuesRealSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesRealSig) evalReal(_ chunk.Event) (float64, bool, error) {
	if !b.ctx.GetStochastikVars().StmtCtx.InInsertStmt {
		return 0, true, nil
	}
	event := b.ctx.GetStochastikVars().CurrInsertValues
	if event.IsEmpty() {
		return 0, true, errors.New("Stochastik current insert values is nil")
	}
	if b.offset < event.Len() {
		if event.IsNull(b.offset) {
			return 0, true, nil
		}
		if b.getRetTp().Tp == allegrosql.TypeFloat {
			return float64(event.GetFloat32(b.offset)), false, nil
		}
		return event.GetFloat64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Stochastik current insert values len %d and defCausumn's offset %v don't match", event.Len(), b.offset)
}

type builtinValuesDecimalSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesDecimalSig) Clone() builtinFunc {
	newSig := &builtinValuesDecimalSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinValuesDecimalSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDecimalSig) evalDecimal(_ chunk.Event) (*types.MyDecimal, bool, error) {
	if !b.ctx.GetStochastikVars().StmtCtx.InInsertStmt {
		return nil, true, nil
	}
	event := b.ctx.GetStochastikVars().CurrInsertValues
	if event.IsEmpty() {
		return nil, true, errors.New("Stochastik current insert values is nil")
	}
	if b.offset < event.Len() {
		if event.IsNull(b.offset) {
			return nil, true, nil
		}
		return event.GetMyDecimal(b.offset), false, nil
	}
	return nil, true, errors.Errorf("Stochastik current insert values len %d and defCausumn's offset %v don't match", event.Len(), b.offset)
}

type builtinValuesStringSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesStringSig) Clone() builtinFunc {
	newSig := &builtinValuesStringSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinValuesStringSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesStringSig) evalString(_ chunk.Event) (string, bool, error) {
	if !b.ctx.GetStochastikVars().StmtCtx.InInsertStmt {
		return "", true, nil
	}
	event := b.ctx.GetStochastikVars().CurrInsertValues
	if event.IsEmpty() {
		return "", true, errors.New("Stochastik current insert values is nil")
	}
	if b.offset >= event.Len() {
		return "", true, errors.Errorf("Stochastik current insert values len %d and defCausumn's offset %v don't match", event.Len(), b.offset)
	}

	if event.IsNull(b.offset) {
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	if retType := b.getRetTp(); retType.Hybrid() {
		val := event.GetCauset(b.offset, retType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	return event.GetString(b.offset), false, nil
}

type builtinValuesTimeSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesTimeSig) Clone() builtinFunc {
	newSig := &builtinValuesTimeSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinValuesTimeSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesTimeSig) evalTime(_ chunk.Event) (types.Time, bool, error) {
	if !b.ctx.GetStochastikVars().StmtCtx.InInsertStmt {
		return types.ZeroTime, true, nil
	}
	event := b.ctx.GetStochastikVars().CurrInsertValues
	if event.IsEmpty() {
		return types.ZeroTime, true, errors.New("Stochastik current insert values is nil")
	}
	if b.offset < event.Len() {
		if event.IsNull(b.offset) {
			return types.ZeroTime, true, nil
		}
		return event.GetTime(b.offset), false, nil
	}
	return types.ZeroTime, true, errors.Errorf("Stochastik current insert values len %d and defCausumn's offset %v don't match", event.Len(), b.offset)
}

type builtinValuesDurationSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesDurationSig) Clone() builtinFunc {
	newSig := &builtinValuesDurationSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinValuesDurationSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDurationSig) evalDuration(_ chunk.Event) (types.Duration, bool, error) {
	if !b.ctx.GetStochastikVars().StmtCtx.InInsertStmt {
		return types.Duration{}, true, nil
	}
	event := b.ctx.GetStochastikVars().CurrInsertValues
	if event.IsEmpty() {
		return types.Duration{}, true, errors.New("Stochastik current insert values is nil")
	}
	if b.offset < event.Len() {
		if event.IsNull(b.offset) {
			return types.Duration{}, true, nil
		}
		duration := event.GetDuration(b.offset, b.getRetTp().Decimal)
		return duration, false, nil
	}
	return types.Duration{}, true, errors.Errorf("Stochastik current insert values len %d and defCausumn's offset %v don't match", event.Len(), b.offset)
}

type builtinValuesJSONSig struct {
	baseBuiltinFunc

	offset int
}

func (b *builtinValuesJSONSig) Clone() builtinFunc {
	newSig := &builtinValuesJSONSig{offset: b.offset}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinValuesJSONSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesJSONSig) evalJSON(_ chunk.Event) (json.BinaryJSON, bool, error) {
	if !b.ctx.GetStochastikVars().StmtCtx.InInsertStmt {
		return json.BinaryJSON{}, true, nil
	}
	event := b.ctx.GetStochastikVars().CurrInsertValues
	if event.IsEmpty() {
		return json.BinaryJSON{}, true, errors.New("Stochastik current insert values is nil")
	}
	if b.offset < event.Len() {
		if event.IsNull(b.offset) {
			return json.BinaryJSON{}, true, nil
		}
		return event.GetJSON(b.offset), false, nil
	}
	return json.BinaryJSON{}, true, errors.Errorf("Stochastik current insert values len %d and defCausumn's offset %v don't match", event.Len(), b.offset)
}

type bitCountFunctionClass struct {
	baseFunctionClass
}

func (c *bitCountFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 2
	sig := &builtinBitCountSig{bf}
	return sig, nil
}

type builtinBitCountSig struct {
	baseBuiltinFunc
}

func (b *builtinBitCountSig) Clone() builtinFunc {
	newSig := &builtinBitCountSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals BIT_COUNT(N).
// See https://dev.allegrosql.com/doc/refman/5.7/en/bit-functions.html#function_bit-count
func (b *builtinBitCountSig) evalInt(event chunk.Event) (int64, bool, error) {
	n, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if err != nil || isNull {
		if err != nil && types.ErrOverflow.Equal(err) {
			return 64, false, nil
		}
		return 0, true, err
	}
	return bitCount(n), false, nil
}

// getParamFunctionClass for plan cache of prepared statements
type getParamFunctionClass struct {
	baseFunctionClass
}

// getFunction gets function
// TODO: more typed functions will be added when typed parameters are supported.
func (c *getParamFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = allegrosql.MaxFieldVarCharLength
	sig := &builtinGetParamStringSig{bf}
	return sig, nil
}

type builtinGetParamStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGetParamStringSig) Clone() builtinFunc {
	newSig := &builtinGetParamStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGetParamStringSig) evalString(event chunk.Event) (string, bool, error) {
	stochastikVars := b.ctx.GetStochastikVars()
	idx, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	v := stochastikVars.PreparedParams[idx]

	str, err := v.ToString()
	if err != nil {
		return "", true, nil
	}
	return str, false, nil
}
