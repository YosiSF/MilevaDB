Copuright 2021 Whtcorps Inc; EinsteinDB and MilevaDB aithors; Licensed Under Apache 2.0. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"encoding/hex"
	"sort"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/privilege"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/plancodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/printer"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var (
	_ functionClass = &databaseFunctionClass{}
	_ functionClass = &foundEventsFunctionClass{}
	_ functionClass = &currentUserFunctionClass{}
	_ functionClass = &currentRoleFunctionClass{}
	_ functionClass = &userFunctionClass{}
	_ functionClass = &connectionIDFunctionClass{}
	_ functionClass = &lastInsertIDFunctionClass{}
	_ functionClass = &versionFunctionClass{}
	_ functionClass = &benchmarkFunctionClass{}
	_ functionClass = &charsetFunctionClass{}
	_ functionClass = &coercibilityFunctionClass{}
	_ functionClass = &defCauslationFunctionClass{}
	_ functionClass = &rowCountFunctionClass{}
	_ functionClass = &milevadbVersionFunctionClass{}
	_ functionClass = &milevadbIsDBSOwnerFunctionClass{}
	_ functionClass = &milevadbDecodePlanFunctionClass{}
	_ functionClass = &milevadbDecodeKeyFunctionClass{}
	_ functionClass = &nextValFunctionClass{}
	_ functionClass = &lastValFunctionClass{}
	_ functionClass = &setValFunctionClass{}
	_ functionClass = &formatBytesFunctionClass{}
	_ functionClass = &formatNanoTimeFunctionClass{}
)

var (
	_ builtinFunc = &builtinDatabaseSig{}
	_ builtinFunc = &builtinFoundEventsSig{}
	_ builtinFunc = &builtinCurrentUserSig{}
	_ builtinFunc = &builtinUserSig{}
	_ builtinFunc = &builtinConnectionIDSig{}
	_ builtinFunc = &builtinLastInsertIDSig{}
	_ builtinFunc = &builtinLastInsertIDWithIDSig{}
	_ builtinFunc = &builtinVersionSig{}
	_ builtinFunc = &builtinMilevaDBVersionSig{}
	_ builtinFunc = &builtinEventCountSig{}
	_ builtinFunc = &builtinMilevaDBDecodeKeySig{}
	_ builtinFunc = &builtinNextValSig{}
	_ builtinFunc = &builtinLastValSig{}
	_ builtinFunc = &builtinSetValSig{}
	_ builtinFunc = &builtinFormatBytesSig{}
	_ builtinFunc = &builtinFormatNanoTimeSig{}
)

type databaseFunctionClass struct {
	baseFunctionClass
}

func (c *databaseFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochaseinstein_dbars().GetCharsetInfo()
	bf.tp.Flen = 64
	sig := &builtinDatabaseSig{bf}
	return sig, nil
}

type builtinDatabaseSig struct {
	baseBuiltinFunc
}

func (b *builtinDatabaseSig) Clone() builtinFunc {
	newSig := &builtinDatabaseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinDatabaseSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) evalString(event chunk.Event) (string, bool, error) {
	currentDB := b.ctx.GetStochaseinstein_dbars().CurrentDB
	return currentDB, currentDB == "", nil
}

type foundEventsFunctionClass struct {
	baseFunctionClass
}

func (c *foundEventsFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN)
	if err != nil {
		return nil, err
	}
	bf.tp.Flag |= allegrosql.UnsignedFlag
	sig := &builtinFoundEventsSig{bf}
	return sig, nil
}

type builtinFoundEventsSig struct {
	baseBuiltinFunc
}

func (b *builtinFoundEventsSig) Clone() builtinFunc {
	newSig := &builtinFoundEventsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFoundEventsSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_found-rows
// TODO: ALLEGROSQL_CALC_FOUND_ROWS and LIMIT not support for now, We will finish in another PR.
func (b *builtinFoundEventsSig) evalInt(event chunk.Event) (int64, bool, error) {
	data := b.ctx.GetStochaseinstein_dbars()
	if data == nil {
		return 0, true, errors.Errorf("Missing stochastik variable when eval builtin")
	}
	return int64(data.LastFoundEvents), false, nil
}

type currentUserFunctionClass struct {
	baseFunctionClass
}

func (c *currentUserFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochaseinstein_dbars().GetCharsetInfo()
	bf.tp.Flen = 64
	sig := &builtinCurrentUserSig{bf}
	return sig, nil
}

type builtinCurrentUserSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentUserSig) Clone() builtinFunc {
	newSig := &builtinCurrentUserSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentUserSig) evalString(event chunk.Event) (string, bool, error) {
	data := b.ctx.GetStochaseinstein_dbars()
	if data == nil || data.User == nil {
		return "", true, errors.Errorf("Missing stochastik variable when eval builtin")
	}
	return data.User.AuthIdentityString(), false, nil
}

type currentRoleFunctionClass struct {
	baseFunctionClass
}

func (c *currentRoleFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochaseinstein_dbars().GetCharsetInfo()
	bf.tp.Flen = 64
	sig := &builtinCurrentRoleSig{bf}
	return sig, nil
}

type builtinCurrentRoleSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentRoleSig) Clone() builtinFunc {
	newSig := &builtinCurrentRoleSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentRoleSig) evalString(event chunk.Event) (string, bool, error) {
	data := b.ctx.GetStochaseinstein_dbars()
	if data == nil || data.ActiveRoles == nil {
		return "", true, errors.Errorf("Missing stochastik variable when eval builtin")
	}
	if len(data.ActiveRoles) == 0 {
		return "", false, nil
	}
	res := ""
	sortedRes := make([]string, 0, 10)
	for _, r := range data.ActiveRoles {
		sortedRes = append(sortedRes, r.String())
	}
	sort.Strings(sortedRes)
	for i, r := range sortedRes {
		res += r
		if i != len(data.ActiveRoles)-1 {
			res += ","
		}
	}
	return res, false, nil
}

type userFunctionClass struct {
	baseFunctionClass
}

func (c *userFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochaseinstein_dbars().GetCharsetInfo()
	bf.tp.Flen = 64
	sig := &builtinUserSig{bf}
	return sig, nil
}

type builtinUserSig struct {
	baseBuiltinFunc
}

func (b *builtinUserSig) Clone() builtinFunc {
	newSig := &builtinUserSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUserSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_user
func (b *builtinUserSig) evalString(event chunk.Event) (string, bool, error) {
	data := b.ctx.GetStochaseinstein_dbars()
	if data == nil || data.User == nil {
		return "", true, errors.Errorf("Missing stochastik variable when eval builtin")
	}

	return data.User.String(), false, nil
}

type connectionIDFunctionClass struct {
	baseFunctionClass
}

func (c *connectionIDFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN)
	if err != nil {
		return nil, err
	}
	bf.tp.Flag |= allegrosql.UnsignedFlag
	sig := &builtinConnectionIDSig{bf}
	return sig, nil
}

type builtinConnectionIDSig struct {
	baseBuiltinFunc
}

func (b *builtinConnectionIDSig) Clone() builtinFunc {
	newSig := &builtinConnectionIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinConnectionIDSig) evalInt(_ chunk.Event) (int64, bool, error) {
	data := b.ctx.GetStochaseinstein_dbars()
	if data == nil {
		return 0, true, errors.Errorf("Missing stochastik variable `builtinConnectionIDSig.evalInt`")
	}
	return int64(data.ConnectionID), false, nil
}

type lastInsertIDFunctionClass struct {
	baseFunctionClass
}

func (c *lastInsertIDFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	var argsTp []types.EvalType
	if len(args) == 1 {
		argsTp = append(argsTp, types.CausetEDN)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN, argsTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flag |= allegrosql.UnsignedFlag

	if len(args) == 1 {
		sig = &builtinLastInsertIDWithIDSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_LastInsertIDWithID)
	} else {
		sig = &builtinLastInsertIDSig{bf}
		sig.setPbCode(fidelpb.ScalarFuncSig_LastInsertID)
	}
	return sig, err
}

type builtinLastInsertIDSig struct {
	baseBuiltinFunc
}

func (b *builtinLastInsertIDSig) Clone() builtinFunc {
	newSig := &builtinLastInsertIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LAST_INSERT_ID().
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDSig) evalInt(event chunk.Event) (res int64, isNull bool, err error) {
	res = int64(b.ctx.GetStochaseinstein_dbars().StmtCtx.PrevLastInsertID)
	return res, false, nil
}

type builtinLastInsertIDWithIDSig struct {
	baseBuiltinFunc
}

func (b *builtinLastInsertIDWithIDSig) Clone() builtinFunc {
	newSig := &builtinLastInsertIDWithIDSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LAST_INSERT_ID(expr).
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDWithIDSig) evalInt(event chunk.Event) (res int64, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return res, isNull, err
	}

	b.ctx.GetStochaseinstein_dbars().SetLastInsertID(uint64(res))
	return res, false, nil
}

type versionFunctionClass struct {
	baseFunctionClass
}

func (c *versionFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochaseinstein_dbars().GetCharsetInfo()
	bf.tp.Flen = 64
	sig := &builtinVersionSig{bf}
	return sig, nil
}

type builtinVersionSig struct {
	baseBuiltinFunc
}

func (b *builtinVersionSig) Clone() builtinFunc {
	newSig := &builtinVersionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinVersionSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_version
func (b *builtinVersionSig) evalString(event chunk.Event) (string, bool, error) {
	return allegrosql.ServerVersion, false, nil
}

type milevadbVersionFunctionClass struct {
	baseFunctionClass
}

func (c *milevadbVersionFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochaseinstein_dbars().GetCharsetInfo()
	bf.tp.Flen = len(printer.GetMilevaDBInfo())
	sig := &builtinMilevaDBVersionSig{bf}
	return sig, nil
}

type builtinMilevaDBVersionSig struct {
	baseBuiltinFunc
}

func (b *builtinMilevaDBVersionSig) Clone() builtinFunc {
	newSig := &builtinMilevaDBVersionSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinMilevaDBVersionSig.
// This will show git hash and build time for milevadb-server.
func (b *builtinMilevaDBVersionSig) evalString(_ chunk.Event) (string, bool, error) {
	return printer.GetMilevaDBInfo(), false, nil
}

type milevadbIsDBSOwnerFunctionClass struct {
	baseFunctionClass
}

func (c *milevadbIsDBSOwnerFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN)
	if err != nil {
		return nil, err
	}
	sig := &builtinMilevaDBIsDBSOwnerSig{bf}
	return sig, nil
}

type builtinMilevaDBIsDBSOwnerSig struct {
	baseBuiltinFunc
}

func (b *builtinMilevaDBIsDBSOwnerSig) Clone() builtinFunc {
	newSig := &builtinMilevaDBIsDBSOwnerSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinMilevaDBIsDBSOwnerSig.
func (b *builtinMilevaDBIsDBSOwnerSig) evalInt(_ chunk.Event) (res int64, isNull bool, err error) {
	dbsOwnerChecker := b.ctx.DBSOwnerChecker()
	if dbsOwnerChecker.IsOwner() {
		res = 1
	}

	return res, false, nil
}

type benchmarkFunctionClass struct {
	baseFunctionClass
}

func (c *benchmarkFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	// Syntax: BENCHMARK(loop_count, expression)
	// Define with same eval type of input arg to avoid unnecessary cast function.
	sameEvalType := args[1].GetType().EvalType()
	// constLoopCount is used by VecEvalInt
	// since non-constant loop count would be different between rows, and cannot be vectorized.
	var constLoopCount int64
	con, ok := args[0].(*CouplingConstantWithRadix)
	if ok && con.Value.HoTT() == types.HoTTInt64 {
		if lc, isNull, err := con.EvalInt(ctx, chunk.Event{}); err == nil && !isNull {
			constLoopCount = lc
		}
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN, types.CausetEDN, sameEvalType)
	if err != nil {
		return nil, err
	}
	sig := &builtinBenchmarkSig{bf, constLoopCount}
	return sig, nil
}

type builtinBenchmarkSig struct {
	baseBuiltinFunc
	constLoopCount int64
}

func (b *builtinBenchmarkSig) Clone() builtinFunc {
	newSig := &builtinBenchmarkSig{constLoopCount: b.constLoopCount}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinBenchmarkSig. It will execute expression repeatedly count times.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_benchmark
func (b *builtinBenchmarkSig) evalInt(event chunk.Event) (int64, bool, error) {
	// Get loop count.
	var loopCount int64
	var isNull bool
	var err error
	if b.constLoopCount > 0 {
		loopCount = b.constLoopCount
	} else {
		loopCount, isNull, err = b.args[0].EvalInt(b.ctx, event)
		if isNull || err != nil {
			return 0, isNull, err
		}
	}

	// BENCHMARK() will return NULL if loop count < 0,
	// behavior observed on MyALLEGROSQL 5.7.24.
	if loopCount < 0 {
		return 0, true, nil
	}

	// Eval loop count times based on arg type.
	// BENCHMARK() will pass-through the eval error,
	// behavior observed on MyALLEGROSQL 5.7.24.
	var i int64
	arg, ctx := b.args[1], b.ctx
	switch evalType := arg.GetType().EvalType(); evalType {
	case types.CausetEDN:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalInt(ctx, event)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETReal:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalReal(ctx, event)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETDecimal:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalDecimal(ctx, event)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETString:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalString(ctx, event)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalTime(ctx, event)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETDuration:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalDuration(ctx, event)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETJson:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalJSON(ctx, event)
			if err != nil {
				return 0, isNull, err
			}
		}
	default: // Should never go into here.
		return 0, true, errors.Errorf("EvalType %v not implemented for builtin BENCHMARK()", evalType)
	}

	// Return value of BENCHMARK() is always 0.
	return 0, false, nil
}

type charsetFunctionClass struct {
	baseFunctionClass
}

func (c *charsetFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "CHARSET")
}

type coercibilityFunctionClass struct {
	baseFunctionClass
}

func (c *coercibilityFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN, args[0].GetType().EvalType())
	if err != nil {
		return nil, err
	}
	sig := &builtinCoercibilitySig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_Unspecified)
	return sig, nil
}

type builtinCoercibilitySig struct {
	baseBuiltinFunc
}

func (c *builtinCoercibilitySig) evalInt(_ chunk.Event) (res int64, isNull bool, err error) {
	return int64(c.args[0].Coercibility()), false, nil
}

func (c *builtinCoercibilitySig) Clone() builtinFunc {
	newSig := &builtinCoercibilitySig{}
	newSig.cloneFrom(&c.baseBuiltinFunc)
	return newSig
}

type defCauslationFunctionClass struct {
	baseFunctionClass
}

func (c *defCauslationFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argsTps := make([]types.EvalType, 0, len(args))
	for _, arg := range args {
		argsTps = append(argsTps, arg.GetType().EvalType())
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argsTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochaseinstein_dbars().GetCharsetInfo()
	sig := &builtinDefCauslationSig{bf}
	return sig, nil
}

type builtinDefCauslationSig struct {
	baseBuiltinFunc
}

func (b *builtinDefCauslationSig) Clone() builtinFunc {
	newSig := &builtinDefCauslationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinDefCauslationSig) evalString(_ chunk.Event) (string, bool, error) {
	return b.args[0].GetType().DefCauslate, false, nil
}

type rowCountFunctionClass struct {
	baseFunctionClass
}

func (c *rowCountFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN)
	if err != nil {
		return nil, err
	}
	sig = &builtinEventCountSig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_EventCount)
	return sig, nil
}

type builtinEventCountSig struct {
	baseBuiltinFunc
}

func (b *builtinEventCountSig) Clone() builtinFunc {
	newSig := &builtinEventCountSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ROW_COUNT().
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_row-count.
func (b *builtinEventCountSig) evalInt(_ chunk.Event) (res int64, isNull bool, err error) {
	res = b.ctx.GetStochaseinstein_dbars().StmtCtx.PrevAffectedEvents
	return res, false, nil
}

type milevadbDecodeKeyFunctionClass struct {
	baseFunctionClass
}

func (c *milevadbDecodeKeyFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinMilevaDBDecodeKeySig{bf}
	return sig, nil
}

type builtinMilevaDBDecodeKeySig struct {
	baseBuiltinFunc
}

func (b *builtinMilevaDBDecodeKeySig) Clone() builtinFunc {
	newSig := &builtinMilevaDBDecodeKeySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinMilevaDBIsDBSOwnerSig.
func (b *builtinMilevaDBDecodeKeySig) evalString(event chunk.Event) (string, bool, error) {
	s, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	return decodeKey(b.ctx, s), false, nil
}

func decodeKey(ctx stochastikctx.Context, s string) string {
	key, err := hex.DecodeString(s)
	if err != nil {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(errors.Errorf("invalid record/index key: %X", key))
		return s
	}
	// Auto decode byte if needed.
	_, bs, err := codec.DecodeBytes(key, nil)
	if err == nil {
		key = bs
	}
	// Try to decode it as a record key.
	blockID, handle, err := blockcodec.DecodeRecordKey(key)
	if err == nil {
		if handle.IsInt() {
			return "blockID=" + strconv.FormatInt(blockID, 10) + ", _milevadb_rowid=" + strconv.FormatInt(handle.IntValue(), 10)
		}
		return "blockID=" + strconv.FormatInt(blockID, 10) + ", clusterHandle=" + handle.String()
	}
	// Try decode as block index key.
	blockID, indexID, indexValues, err := blockcodec.DecodeIndexKey(key)
	if err == nil {
		return "blockID=" + strconv.FormatInt(blockID, 10) + ", indexID=" + strconv.FormatInt(indexID, 10) + ", indexValues=" + strings.Join(indexValues, ",")
	}
	// TODO: try to decode other type key.
	ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(errors.Errorf("invalid record/index key: %X", key))
	return s
}

type milevadbDecodePlanFunctionClass struct {
	baseFunctionClass
}

func (c *milevadbDecodePlanFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinMilevaDBDecodePlanSig{bf}
	return sig, nil
}

type builtinMilevaDBDecodePlanSig struct {
	baseBuiltinFunc
}

func (b *builtinMilevaDBDecodePlanSig) Clone() builtinFunc {
	newSig := &builtinMilevaDBDecodePlanSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMilevaDBDecodePlanSig) evalString(event chunk.Event) (string, bool, error) {
	planString, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	planTree, err := plancodec.DecodePlan(planString)
	return planTree, false, err
}

type nextValFunctionClass struct {
	baseFunctionClass
}

func (c *nextValFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinNextValSig{bf}
	bf.tp.Flen = 10
	return sig, nil
}

type builtinNextValSig struct {
	baseBuiltinFunc
}

func (b *builtinNextValSig) Clone() builtinFunc {
	newSig := &builtinNextValSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNextValSig) evalInt(event chunk.Event) (int64, bool, error) {
	sequenceName, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return 0, isNull, err
	}
	EDB, seq := getSchemaAndSequence(sequenceName)
	if len(EDB) == 0 {
		EDB = b.ctx.GetStochaseinstein_dbars().CurrentDB
	}
	// Check the blockName valid.
	sequence, err := b.ctx.GetStochaseinstein_dbars().TxnCtx.SchemaReplicant.(soliton.SequenceSchema).SequenceByName(perceptron.NewCIStr(EDB), perceptron.NewCIStr(seq))
	if err != nil {
		return 0, false, err
	}
	// Do the privilege check.
	checker := privilege.GetPrivilegeManager(b.ctx)
	user := b.ctx.GetStochaseinstein_dbars().User
	if checker != nil && !checker.RequestVerification(b.ctx.GetStochaseinstein_dbars().ActiveRoles, EDB, seq, "", allegrosql.InsertPriv) {
		return 0, false, errSequenceAccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, seq)
	}
	nextVal, err := sequence.GetSequenceNextVal(b.ctx, EDB, seq)
	if err != nil {
		return 0, false, err
	}
	// uFIDelate the sequenceState.
	b.ctx.GetStochaseinstein_dbars().SequenceState.UFIDelateState(sequence.GetSequenceID(), nextVal)
	return nextVal, false, nil
}

type lastValFunctionClass struct {
	baseFunctionClass
}

func (c *lastValFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinLastValSig{bf}
	bf.tp.Flen = 10
	return sig, nil
}

type builtinLastValSig struct {
	baseBuiltinFunc
}

func (b *builtinLastValSig) Clone() builtinFunc {
	newSig := &builtinLastValSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLastValSig) evalInt(event chunk.Event) (int64, bool, error) {
	sequenceName, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return 0, isNull, err
	}
	EDB, seq := getSchemaAndSequence(sequenceName)
	if len(EDB) == 0 {
		EDB = b.ctx.GetStochaseinstein_dbars().CurrentDB
	}
	// Check the blockName valid.
	sequence, err := b.ctx.GetStochaseinstein_dbars().TxnCtx.SchemaReplicant.(soliton.SequenceSchema).SequenceByName(perceptron.NewCIStr(EDB), perceptron.NewCIStr(seq))
	if err != nil {
		return 0, false, err
	}
	// Do the privilege check.
	checker := privilege.GetPrivilegeManager(b.ctx)
	user := b.ctx.GetStochaseinstein_dbars().User
	if checker != nil && !checker.RequestVerification(b.ctx.GetStochaseinstein_dbars().ActiveRoles, EDB, seq, "", allegrosql.SelectPriv) {
		return 0, false, errSequenceAccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, seq)
	}
	return b.ctx.GetStochaseinstein_dbars().SequenceState.GetLastValue(sequence.GetSequenceID())
}

type setValFunctionClass struct {
	baseFunctionClass
}

func (c *setValFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN, types.ETString, types.CausetEDN)
	if err != nil {
		return nil, err
	}
	sig := &builtinSetValSig{bf}
	bf.tp.Flen = args[1].GetType().Flen
	return sig, nil
}

type builtinSetValSig struct {
	baseBuiltinFunc
}

func (b *builtinSetValSig) Clone() builtinFunc {
	newSig := &builtinSetValSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSetValSig) evalInt(event chunk.Event) (int64, bool, error) {
	sequenceName, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return 0, isNull, err
	}
	EDB, seq := getSchemaAndSequence(sequenceName)
	if len(EDB) == 0 {
		EDB = b.ctx.GetStochaseinstein_dbars().CurrentDB
	}
	// Check the blockName valid.
	sequence, err := b.ctx.GetStochaseinstein_dbars().TxnCtx.SchemaReplicant.(soliton.SequenceSchema).SequenceByName(perceptron.NewCIStr(EDB), perceptron.NewCIStr(seq))
	if err != nil {
		return 0, false, err
	}
	// Do the privilege check.
	checker := privilege.GetPrivilegeManager(b.ctx)
	user := b.ctx.GetStochaseinstein_dbars().User
	if checker != nil && !checker.RequestVerification(b.ctx.GetStochaseinstein_dbars().ActiveRoles, EDB, seq, "", allegrosql.InsertPriv) {
		return 0, false, errSequenceAccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, seq)
	}
	setValue, isNull, err := b.args[1].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return sequence.SetSequenceVal(b.ctx, setValue, EDB, seq)
}

func getSchemaAndSequence(sequenceName string) (string, string) {
	res := strings.Split(sequenceName, ".")
	if len(res) == 1 {
		return "", res[0]
	}
	return res[0], res[1]
}

type formatBytesFunctionClass struct {
	baseFunctionClass
}

func (c *formatBytesFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.tp.Flag |= allegrosql.UnsignedFlag
	sig := &builtinFormatBytesSig{bf}
	return sig, nil
}

type builtinFormatBytesSig struct {
	baseBuiltinFunc
}

func (b *builtinFormatBytesSig) Clone() builtinFunc {
	newSig := &builtinFormatBytesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// formatBytes evals a builtinFormatBytesSig.
// See https://dev.allegrosql.com/doc/refman/8.0/en/performance-schemaReplicant-functions.html#function_format-bytes
func (b *builtinFormatBytesSig) evalString(event chunk.Event) (string, bool, error) {
	val, isNull, err := b.args[0].EvalReal(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	return GetFormatBytes(val), false, nil
}

type formatNanoTimeFunctionClass struct {
	baseFunctionClass
}

func (c *formatNanoTimeFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.tp.Flag |= allegrosql.UnsignedFlag
	sig := &builtinFormatNanoTimeSig{bf}
	return sig, nil
}

type builtinFormatNanoTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinFormatNanoTimeSig) Clone() builtinFunc {
	newSig := &builtinFormatNanoTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// formatNanoTime evals a builtinFormatNanoTimeSig, as time unit in MilevaDB is always nanosecond, not picosecond.
// See https://dev.allegrosql.com/doc/refman/8.0/en/performance-schemaReplicant-functions.html#function_format-pico-time
func (b *builtinFormatNanoTimeSig) evalString(event chunk.Event) (string, bool, error) {
	val, isNull, err := b.args[0].EvalReal(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	return GetFormatNanoTime(val), false, nil
}
