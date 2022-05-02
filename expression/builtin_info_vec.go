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
	"sort"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/printer"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

func (b *builtinDatabaseSig) vectorized() bool {
	return true
}

// evalString evals a builtinDatabaseSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) vecEvalString(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()

	currentDB := b.ctx.GetStochaseinstein_dbars().CurrentDB
	result.ReserveString(n)
	if currentDB == "" {
		for i := 0; i < n; i++ {
			result.AppendNull()
		}
	} else {
		for i := 0; i < n; i++ {
			result.AppendString(currentDB)
		}
	}
	return nil
}

func (b *builtinConnectionIDSig) vectorized() bool {
	return true
}

func (b *builtinConnectionIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	data := b.ctx.GetStochaseinstein_dbars()
	if data == nil {
		return errors.Errorf("Missing stochastik variable in `builtinConnectionIDSig.vecEvalInt`")
	}
	connectionID := int64(data.ConnectionID)
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = connectionID
	}
	return nil
}

func (b *builtinMilevaDBVersionSig) vectorized() bool {
	return true
}

func (b *builtinMilevaDBVersionSig) vecEvalString(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	result.ReserveString(n)
	info := printer.GetMilevaDBInfo()
	for i := 0; i < n; i++ {
		result.AppendString(info)
	}
	return nil
}

func (b *builtinEventCountSig) vectorized() bool {
	return true
}

// evalInt evals ROW_COUNT().
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_row-count
func (b *builtinEventCountSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	res := b.ctx.GetStochaseinstein_dbars().StmtCtx.PrevAffectedEvents
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinCurrentUserSig) vectorized() bool {
	return true
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentUserSig) vecEvalString(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()

	data := b.ctx.GetStochaseinstein_dbars()
	result.ReserveString(n)
	if data == nil || data.User == nil {
		return errors.Errorf("Missing stochastik variable when eval builtin")
	}
	for i := 0; i < n; i++ {
		result.AppendString(data.User.AuthIdentityString())
	}
	return nil
}

func (b *builtinCurrentRoleSig) vectorized() bool {
	return true
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentRoleSig) vecEvalString(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()

	data := b.ctx.GetStochaseinstein_dbars()
	if data == nil || data.ActiveRoles == nil {
		return errors.Errorf("Missing stochastik variable when eval builtin")
	}

	result.ReserveString(n)
	if len(data.ActiveRoles) == 0 {
		for i := 0; i < n; i++ {
			result.AppendString("")
		}
		return nil
	}

	sortedRes := make([]string, 0, 10)
	for _, r := range data.ActiveRoles {
		sortedRes = append(sortedRes, r.String())
	}
	sort.Strings(sortedRes)
	res := strings.Join(sortedRes, ",")
	for i := 0; i < n; i++ {
		result.AppendString(res)
	}
	return nil
}

func (b *builtinUserSig) vectorized() bool {
	return true
}

// evalString evals a builtinUserSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_user
func (b *builtinUserSig) vecEvalString(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	data := b.ctx.GetStochaseinstein_dbars()
	if data == nil || data.User == nil {
		return errors.Errorf("Missing stochastik variable when eval builtin")
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendString(data.User.String())
	}
	return nil
}

func (b *builtinMilevaDBIsDBSOwnerSig) vectorized() bool {
	return true
}

func (b *builtinMilevaDBIsDBSOwnerSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	dbsOwnerChecker := b.ctx.DBSOwnerChecker()
	var res int64
	if dbsOwnerChecker.IsOwner() {
		res = 1
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinFoundEventsSig) vectorized() bool {
	return true
}

func (b *builtinFoundEventsSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	data := b.ctx.GetStochaseinstein_dbars()
	if data == nil {
		return errors.Errorf("Missing stochastik variable when eval builtin")
	}
	lastFoundEvents := int64(data.LastFoundEvents)
	n := input.NumEvents()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := range i64s {
		i64s[i] = lastFoundEvents
	}
	return nil
}

func (b *builtinBenchmarkSig) vectorized() bool {
	return b.constLoopCount > 0
}

func (b *builtinBenchmarkSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	loopCount := b.constLoopCount
	arg, ctx := b.args[1], b.ctx
	evalType := arg.GetType().EvalType()
	buf, err := b.bufSlabPredictor.get(evalType, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf)

	var k int64
	switch evalType {
	case types.CausetEDN:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalInt(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETReal:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalReal(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETDecimal:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalDecimal(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETString:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalString(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalTime(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETDuration:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalDuration(ctx, input, buf); err != nil {
				return err
			}
		}
	case types.ETJson:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalJSON(ctx, input, buf); err != nil {
				return err
			}
		}
	default: // Should never go into here.
		return errors.Errorf("EvalType %v not implemented for builtin BENCHMARK()", evalType)
	}

	// Return value of BENCHMARK() is always 0.
	// even if args[1].IsNull(i)
	result.ResizeInt64(n, false)

	return nil
}

func (b *builtinLastInsertIDSig) vectorized() bool {
	return true
}

func (b *builtinLastInsertIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	res := int64(b.ctx.GetStochaseinstein_dbars().StmtCtx.PrevLastInsertID)
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinLastInsertIDWithIDSig) vectorized() bool {
	return true
}

func (b *builtinLastInsertIDWithIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := len(i64s) - 1; i >= 0; i-- {
		if !result.IsNull(i) {
			b.ctx.GetStochaseinstein_dbars().SetLastInsertID(uint64(i64s[i]))
			break
		}
	}
	return nil
}

func (b *builtinVersionSig) vectorized() bool {
	return true
}

func (b *builtinVersionSig) vecEvalString(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendString(allegrosql.ServerVersion)
	}
	return nil
}

func (b *builtinMilevaDBDecodeKeySig) vectorized() bool {
	return true
}

func (b *builtinMilevaDBDecodeKeySig) vecEvalString(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	buf, err := b.bufSlabPredictor.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(decodeKey(b.ctx, buf.GetString(i)))
	}
	return nil
}
