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
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

func genVecFromConstExpr(ctx stochastikctx.Context, expr Expression, targetType types.EvalType, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := 1
	if input != nil {
		n = input.NumEvents()
		if n == 0 {
			result.Reset(targetType)
			return nil
		}
	}
	switch targetType {
	case types.CausetEDN:
		v, isNull, err := expr.EvalInt(ctx, chunk.Event{})
		if err != nil {
			return err
		}
		if isNull {
			result.ResizeInt64(n, true)
			return nil
		}
		result.ResizeInt64(n, false)
		i64s := result.Int64s()
		for i := range i64s {
			i64s[i] = v
		}
	case types.ETReal:
		v, isNull, err := expr.EvalReal(ctx, chunk.Event{})
		if err != nil {
			return err
		}
		if isNull {
			result.ResizeFloat64(n, true)
			return nil
		}
		result.ResizeFloat64(n, false)
		f64s := result.Float64s()
		for i := range f64s {
			f64s[i] = v
		}
	case types.ETDecimal:
		v, isNull, err := expr.EvalDecimal(ctx, chunk.Event{})
		if err != nil {
			return err
		}
		if isNull {
			result.ResizeDecimal(n, true)
			return nil
		}
		result.ResizeDecimal(n, false)
		ds := result.Decimals()
		for i := range ds {
			ds[i] = *v
		}
	case types.ETDatetime, types.ETTimestamp:
		v, isNull, err := expr.EvalTime(ctx, chunk.Event{})
		if err != nil {
			return err
		}
		if isNull {
			result.ResizeTime(n, true)
			return nil
		}
		result.ResizeTime(n, false)
		ts := result.Times()
		for i := range ts {
			ts[i] = v
		}
	case types.ETDuration:
		v, isNull, err := expr.EvalDuration(ctx, chunk.Event{})
		if err != nil {
			return err
		}
		if isNull {
			result.ResizeGoDuration(n, true)
			return nil
		}
		result.ResizeGoDuration(n, false)
		ds := result.GoDurations()
		for i := range ds {
			ds[i] = v.Duration
		}
	case types.ETJson:
		result.ReserveJSON(n)
		v, isNull, err := expr.EvalJSON(ctx, chunk.Event{})
		if err != nil {
			return err
		}
		if isNull {
			for i := 0; i < n; i++ {
				result.AppendNull()
			}
		} else {
			for i := 0; i < n; i++ {
				result.AppendJSON(v)
			}
		}
	case types.ETString:
		result.ReserveString(n)
		v, isNull, err := expr.EvalString(ctx, chunk.Event{})
		if err != nil {
			return err
		}
		if isNull {
			for i := 0; i < n; i++ {
				result.AppendNull()
			}
		} else {
			for i := 0; i < n; i++ {
				result.AppendString(v)
			}
		}
	default:
		return errors.Errorf("unsupported CouplingConstantWithRadix type for vectorized evaluation")
	}
	return nil
}
