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
	"strconv"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

// Vectorizable checks whether a list of expressions can employ vectorized execution.
func Vectorizable(exprs []Expression) bool {
	for _, expr := range exprs {
		if HasGetSetVarFunc(expr) {
			return false
		}
	}
	return checkSequenceFunction(exprs)
}

// checkSequenceFunction indicates whether the exprs can be evaluated as a vector.
// When two or more of this three(nextval, lastval, setval) exists in exprs list and one of them is nextval, it should be eval event by event.
func checkSequenceFunction(exprs []Expression) bool {
	var (
		nextval int
		lastval int
		setval  int
	)
	for _, expr := range exprs {
		scalaFunc, ok := expr.(*ScalarFunction)
		if !ok {
			continue
		}
		switch scalaFunc.FuncName.L {
		case ast.NextVal:
			nextval++
		case ast.LastVal:
			lastval++
		case ast.SetVal:
			setval++
		}
	}
	// case1: nextval && other sequence function.
	// case2: more than one nextval.
	if (nextval > 0 && (lastval > 0 || setval > 0)) || (nextval > 1) {
		return false
	}
	return true
}

// HasGetSetVarFunc checks whether an expression contains SetVar/GetVar function.
func HasGetSetVarFunc(expr Expression) bool {
	scalaFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar {
		return true
	}
	if scalaFunc.FuncName.L == ast.GetVar {
		return true
	}
	for _, arg := range scalaFunc.GetArgs() {
		if HasGetSetVarFunc(arg) {
			return true
		}
	}
	return false
}

// HasAssignSetVarFunc checks whether an expression contains SetVar function and assign a value
func HasAssignSetVarFunc(expr Expression) bool {
	scalaFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar {
		for _, arg := range scalaFunc.GetArgs() {
			if _, ok := arg.(*ScalarFunction); ok {
				return true
			}
		}
	}
	for _, arg := range scalaFunc.GetArgs() {
		if HasAssignSetVarFunc(arg) {
			return true
		}
	}
	return false
}

// VectorizedExecute evaluates a list of expressions defCausumn by defCausumn and append their results to "output" Chunk.
func VectorizedExecute(ctx stochastikctx.Context, exprs []Expression, iterator *chunk.Iterator4Chunk, output *chunk.Chunk) error {
	for defCausID, expr := range exprs {
		err := evalOneDeferredCauset(ctx, expr, iterator, output, defCausID)
		if err != nil {
			return err
		}
	}
	return nil
}

func evalOneVec(ctx stochastikctx.Context, expr Expression, input *chunk.Chunk, output *chunk.Chunk, defCausIdx int) error {
	ft := expr.GetType()
	result := output.DeferredCauset(defCausIdx)
	switch ft.EvalType() {
	case types.CausetEDN:
		if err := expr.VecEvalInt(ctx, input, result); err != nil {
			return err
		}
		if ft.Tp == allegrosql.TypeBit {
			i64s := result.Int64s()
			buf := chunk.NewDeferredCauset(ft, input.NumEvents())
			buf.ReserveBytes(input.NumEvents())
			uintBuf := make([]byte, 8)
			for i := range i64s {
				if result.IsNull(i) {
					buf.AppendNull()
				} else {
					buf.AppendBytes(strconv.AppendUint(uintBuf[:0], uint64(i64s[i]), 10))
				}
			}
			// TODO: recycle all old DeferredCausets returned here.
			output.SetDefCaus(defCausIdx, buf)
		} // else if allegrosql.HasUnsignedFlag(ft.Flag) {
		// the underlying memory formats of int64 and uint64 are the same in Golang,
		// so we can do a no-op here.
		// }
	case types.ETReal:
		if err := expr.VecEvalReal(ctx, input, result); err != nil {
			return err
		}
		if ft.Tp == allegrosql.TypeFloat {
			f64s := result.Float64s()
			n := input.NumEvents()
			buf := chunk.NewDeferredCauset(ft, n)
			buf.ResizeFloat32(n, false)
			f32s := buf.Float32s()
			for i := range f64s {
				if result.IsNull(i) {
					buf.SetNull(i, true)
				} else {
					f32s[i] = float32(f64s[i])
				}
			}
			output.SetDefCaus(defCausIdx, buf)
		}
	case types.ETDecimal:
		return expr.VecEvalDecimal(ctx, input, result)
	case types.ETDatetime, types.ETTimestamp:
		return expr.VecEvalTime(ctx, input, result)
	case types.ETDuration:
		return expr.VecEvalDuration(ctx, input, result)
	case types.ETJson:
		return expr.VecEvalJSON(ctx, input, result)
	case types.ETString:
		if err := expr.VecEvalString(ctx, input, result); err != nil {
			return err
		}
		if ft.Tp == allegrosql.TypeEnum {
			n := input.NumEvents()
			buf := chunk.NewDeferredCauset(ft, n)
			buf.ReserveEnum(n)
			for i := 0; i < n; i++ {
				if result.IsNull(i) {
					buf.AppendNull()
				} else {
					buf.AppendEnum(types.Enum{Value: 0, Name: result.GetString(i)})
				}
			}
			output.SetDefCaus(defCausIdx, buf)
		} else if ft.Tp == allegrosql.TypeSet {
			n := input.NumEvents()
			buf := chunk.NewDeferredCauset(ft, n)
			buf.ReserveSet(n)
			for i := 0; i < n; i++ {
				if result.IsNull(i) {
					buf.AppendNull()
				} else {
					buf.AppendSet(types.Set{Value: 0, Name: result.GetString(i)})
				}
			}
			output.SetDefCaus(defCausIdx, buf)
		}
	}
	return nil
}

func evalOneDeferredCauset(ctx stochastikctx.Context, expr Expression, iterator *chunk.Iterator4Chunk, output *chunk.Chunk, defCausID int) (err error) {
	switch fieldType, evalType := expr.GetType(), expr.GetType().EvalType(); evalType {
	case types.CausetEDN:
		for event := iterator.Begin(); err == nil && event != iterator.End(); event = iterator.Next() {
			err = executeToInt(ctx, expr, fieldType, event, output, defCausID)
		}
	case types.ETReal:
		for event := iterator.Begin(); err == nil && event != iterator.End(); event = iterator.Next() {
			err = executeToReal(ctx, expr, fieldType, event, output, defCausID)
		}
	case types.ETDecimal:
		for event := iterator.Begin(); err == nil && event != iterator.End(); event = iterator.Next() {
			err = executeToDecimal(ctx, expr, fieldType, event, output, defCausID)
		}
	case types.ETDatetime, types.ETTimestamp:
		for event := iterator.Begin(); err == nil && event != iterator.End(); event = iterator.Next() {
			err = executeToDatetime(ctx, expr, fieldType, event, output, defCausID)
		}
	case types.ETDuration:
		for event := iterator.Begin(); err == nil && event != iterator.End(); event = iterator.Next() {
			err = executeToDuration(ctx, expr, fieldType, event, output, defCausID)
		}
	case types.ETJson:
		for event := iterator.Begin(); err == nil && event != iterator.End(); event = iterator.Next() {
			err = executeToJSON(ctx, expr, fieldType, event, output, defCausID)
		}
	case types.ETString:
		for event := iterator.Begin(); err == nil && event != iterator.End(); event = iterator.Next() {
			err = executeToString(ctx, expr, fieldType, event, output, defCausID)
		}
	}
	return err
}

func evalOneCell(ctx stochastikctx.Context, expr Expression, event chunk.Event, output *chunk.Chunk, defCausID int) (err error) {
	switch fieldType, evalType := expr.GetType(), expr.GetType().EvalType(); evalType {
	case types.CausetEDN:
		err = executeToInt(ctx, expr, fieldType, event, output, defCausID)
	case types.ETReal:
		err = executeToReal(ctx, expr, fieldType, event, output, defCausID)
	case types.ETDecimal:
		err = executeToDecimal(ctx, expr, fieldType, event, output, defCausID)
	case types.ETDatetime, types.ETTimestamp:
		err = executeToDatetime(ctx, expr, fieldType, event, output, defCausID)
	case types.ETDuration:
		err = executeToDuration(ctx, expr, fieldType, event, output, defCausID)
	case types.ETJson:
		err = executeToJSON(ctx, expr, fieldType, event, output, defCausID)
	case types.ETString:
		err = executeToString(ctx, expr, fieldType, event, output, defCausID)
	}
	return err
}

func executeToInt(ctx stochastikctx.Context, expr Expression, fieldType *types.FieldType, event chunk.Event, output *chunk.Chunk, defCausID int) error {
	res, isNull, err := expr.EvalInt(ctx, event)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(defCausID)
		return nil
	}
	if fieldType.Tp == allegrosql.TypeBit {
		output.AppendBytes(defCausID, strconv.AppendUint(make([]byte, 0, 8), uint64(res), 10))
		return nil
	}
	if allegrosql.HasUnsignedFlag(fieldType.Flag) {
		output.AppendUint64(defCausID, uint64(res))
		return nil
	}
	output.AppendInt64(defCausID, res)
	return nil
}

func executeToReal(ctx stochastikctx.Context, expr Expression, fieldType *types.FieldType, event chunk.Event, output *chunk.Chunk, defCausID int) error {
	res, isNull, err := expr.EvalReal(ctx, event)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(defCausID)
		return nil
	}
	if fieldType.Tp == allegrosql.TypeFloat {
		output.AppendFloat32(defCausID, float32(res))
		return nil
	}
	output.AppendFloat64(defCausID, res)
	return nil
}

func executeToDecimal(ctx stochastikctx.Context, expr Expression, fieldType *types.FieldType, event chunk.Event, output *chunk.Chunk, defCausID int) error {
	res, isNull, err := expr.EvalDecimal(ctx, event)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(defCausID)
		return nil
	}
	output.AppendMyDecimal(defCausID, res)
	return nil
}

func executeToDatetime(ctx stochastikctx.Context, expr Expression, fieldType *types.FieldType, event chunk.Event, output *chunk.Chunk, defCausID int) error {
	res, isNull, err := expr.EvalTime(ctx, event)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(defCausID)
	} else {
		output.AppendTime(defCausID, res)
	}
	return nil
}

func executeToDuration(ctx stochastikctx.Context, expr Expression, fieldType *types.FieldType, event chunk.Event, output *chunk.Chunk, defCausID int) error {
	res, isNull, err := expr.EvalDuration(ctx, event)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(defCausID)
	} else {
		output.AppendDuration(defCausID, res)
	}
	return nil
}

func executeToJSON(ctx stochastikctx.Context, expr Expression, fieldType *types.FieldType, event chunk.Event, output *chunk.Chunk, defCausID int) error {
	res, isNull, err := expr.EvalJSON(ctx, event)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(defCausID)
	} else {
		output.AppendJSON(defCausID, res)
	}
	return nil
}

func executeToString(ctx stochastikctx.Context, expr Expression, fieldType *types.FieldType, event chunk.Event, output *chunk.Chunk, defCausID int) error {
	res, isNull, err := expr.EvalString(ctx, event)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(defCausID)
	} else if fieldType.Tp == allegrosql.TypeEnum {
		val := types.Enum{Value: uint64(0), Name: res}
		output.AppendEnum(defCausID, val)
	} else if fieldType.Tp == allegrosql.TypeSet {
		val := types.Set{Value: uint64(0), Name: res}
		output.AppendSet(defCausID, val)
	} else {
		output.AppendString(defCausID, res)
	}
	return nil
}

// VectorizedFilter applies a list of filters to a Chunk and
// returns a bool slice, which indicates whether a event is passed the filters.
// Filters is executed vectorized.
func VectorizedFilter(ctx stochastikctx.Context, filters []Expression, iterator *chunk.Iterator4Chunk, selected []bool) (_ []bool, err error) {
	selected, _, err = VectorizedFilterConsiderNull(ctx, filters, iterator, selected, nil)
	return selected, err
}

// VectorizedFilterConsiderNull applies a list of filters to a Chunk and
// returns two bool slices, `selected` indicates whether a event passed the
// filters, `isNull` indicates whether the result of the filter is null.
// Filters is executed vectorized.
func VectorizedFilterConsiderNull(ctx stochastikctx.Context, filters []Expression, iterator *chunk.Iterator4Chunk, selected []bool, isNull []bool) ([]bool, []bool, error) {
	// canVectorized used to check whether all of the filters can be vectorized evaluated
	canVectorized := true
	for _, filter := range filters {
		if !filter.Vectorized() {
			canVectorized = false
			break
		}
	}

	input := iterator.GetChunk()
	sel := input.Sel()
	var err error
	if canVectorized && ctx.GetStochaseinstein_dbars().EnableVectorizedExpression {
		selected, isNull, err = vectorizedFilter(ctx, filters, iterator, selected, isNull)
	} else {
		selected, isNull, err = rowBasedFilter(ctx, filters, iterator, selected, isNull)
	}
	if err != nil || sel == nil {
		return selected, isNull, err
	}

	// When the input.Sel() != nil, we need to handle the selected slice and input.Sel()
	// Get the index which is not appeared in input.Sel() and set the selected[index] = false
	selectedLength := len(selected)
	unselected := allocZeroSlice(selectedLength)
	defer deallocateZeroSlice(unselected)
	// unselected[i] == 1 means that the i-th event is not selected
	for i := 0; i < selectedLength; i++ {
		unselected[i] = 1
	}
	for _, ind := range sel {
		unselected[ind] = 0
	}
	for i := 0; i < selectedLength; i++ {
		if selected[i] && unselected[i] == 1 {
			selected[i] = false
		}
	}
	return selected, isNull, err
}

// rowBasedFilter filters by event.
func rowBasedFilter(ctx stochastikctx.Context, filters []Expression, iterator *chunk.Iterator4Chunk, selected []bool, isNull []bool) ([]bool, []bool, error) {
	// If input.Sel() != nil, we will call input.SetSel(nil) to clear the sel slice in input chunk.
	// After the function finished, then we reset the sel in input chunk.
	// Then the caller will handle the input.sel and selected slices.
	input := iterator.GetChunk()
	if input.Sel() != nil {
		defer input.SetSel(input.Sel())
		input.SetSel(nil)
		iterator = chunk.NewIterator4Chunk(input)
	}

	selected = selected[:0]
	for i, numEvents := 0, iterator.Len(); i < numEvents; i++ {
		selected = append(selected, true)
	}
	if isNull != nil {
		isNull = isNull[:0]
		for i, numEvents := 0, iterator.Len(); i < numEvents; i++ {
			isNull = append(isNull, false)
		}
	}
	var (
		filterResult       int64
		bVal, isNullResult bool
		err                error
	)
	for _, filter := range filters {
		isIntType := true
		if filter.GetType().EvalType() != types.CausetEDN {
			isIntType = false
		}
		for event := iterator.Begin(); event != iterator.End(); event = iterator.Next() {
			if !selected[event.Idx()] {
				continue
			}
			if isIntType {
				filterResult, isNullResult, err = filter.EvalInt(ctx, event)
				if err != nil {
					return nil, nil, err
				}
				selected[event.Idx()] = selected[event.Idx()] && !isNullResult && (filterResult != 0)
			} else {
				// TODO: should rewrite the filter to `cast(expr as SIGNED) != 0` and always use `EvalInt`.
				bVal, isNullResult, err = EvalBool(ctx, []Expression{filter}, event)
				if err != nil {
					return nil, nil, err
				}
				selected[event.Idx()] = selected[event.Idx()] && bVal
			}
			if isNull != nil {
				isNull[event.Idx()] = isNull[event.Idx()] || isNullResult
			}
		}
	}
	return selected, isNull, nil
}

// vectorizedFilter filters by vector.
func vectorizedFilter(ctx stochastikctx.Context, filters []Expression, iterator *chunk.Iterator4Chunk, selected []bool, isNull []bool) ([]bool, []bool, error) {
	selected, isNull, err := VecEvalBool(ctx, filters, iterator.GetChunk(), selected, isNull)
	if err != nil {
		return nil, nil, err
	}

	return selected, isNull, nil
}
