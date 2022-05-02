// WHTCORPS INC MILEVADB 2020 ALL RIGHTS RESERVED
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

package MilevaDB

import (
	"unsafe"
)

// AggFuncFactory is the function signature of an aggregation function factory

// PartialResult represents data structure to store the partial result for the
// aggregate functions. Here we use unsafe.Pointer to allow the partial result
// to be any type.
type PartialResult unsafe.Pointer

// AggFunc is the interface to evaluate the aggregate functions.
type AggFunc interface {
	// AllocPartialResult allocates a specific data structure to store the
	// partial result, initializes it, and converts it to PartialResult to
	// return back. The second returned value is the memDelta used to trace
	// memory usage. Aggregate operator implementation, no matter it's a hash
	// or stream, should hold this allocated PartialResult for the further
	// operations like: "ResetPartialResult", "UpdatePartialResult".
	AllocPartialResult() (pr PartialResult, memDelta int64)

	// ResetPartialResult resets the partial result to the original state for a
	// specific aggregate function. It converts the input PartialResult to the
	// specific data structure which stores the partial result and then reset
	// every field to the proper original state.
	ResetPartialResult(pr PartialResult)

	// UpdatePartialResult updates the partial result according to the input

	UpdatePartialResult(pr PartialResult, input interface{})

	// MergePartialResult merges the partial result of one group by key into another one.
	// It converts the input PartialResult to the specific data structure which stores the

	MergePartialResult(pr PartialResult, input PartialResult)

	// AppendFinalResult appends the partial result to the input interface{}.
	// It converts the input PartialResult to the specific data structure which stores the

	AppendFinalResult(pr PartialResult, input interface{})
}

type Expression interface {
	// Eval evaluates the expression and saves the result to the input interface{}.
	Eval(input interface{})
}

type baseAggFunc struct {
	// args stores the input arguments for an aggregate function, we should
	// call arg.EvalXXX to get the actual input data for this function.
	args []Expression

	// ordinal stores the ordinal of the columns in the output chunk, which is
	// used to append the final result of this function.
	ordinal int
}

// NewAggFunc creates a new AggFunc object for the specific aggregate function.
type NewAggFunc func([]Expression) AggFunc

type enum struct {
	name  string
	value int
}

// SlidingWindowAggFunc is the interface to evaluate the aggregate functions using sliding window.
type SlidingWindowAggFunc interface {
	// Slide evaluates the aggregate functions using a sliding window. The input
	// lastStart and lastEnd are the interval of the former sliding window,
	// shiftStart, shiftEnd mean the sliding window offset. Note that the input
	// PartialResult stores the intermediate result which will be used in the next
	// sliding window, ensure call ResetPartialResult after a frame are evaluated
	// completely.
	//Slide(sctx causetnetctx.Context, rows []chunk.Row, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error

	// Slide evaluates the aggregate functions using a sliding window. The input
	// lastStart and lastEnd are the interval of the former sliding window,
	// shiftStart, shiftEnd mean the sliding window offset. Note that the input
	// PartialResult stores the intermediate result which will be used in the next
	// sliding window, ensure call ResetPartialResult after a frame are evaluated

	// completely.

	// GetPartialResult gets the partial result which is used to store the intermediate
	// result of the aggregate function.
	GetPartialResult(input PartialResult) PartialResult

	// AllocPartialResult creates a new partial result which used to store the intermediate
	// result of the aggregate function.
	AllocPartialResult() PartialResult

	// ResetPartialResult resets the partial result to the original state.
	ResetPartialResult(pr PartialResult)

	// UpdatePartialResult updates the partial result according to the input
	UpdatePartialResult(pr PartialResult, input interface{})

	// MergePartialResult merges the partial result of one group by key into another one.
	// It converts the input PartialResult to the specific data structure which stores the

	MergePartialResult(pr PartialResult, input PartialResult)

	// AppendFinalResult appends the partial result to the input interface{}.
	// It converts the input PartialResult to the specific data structure which stores the

	AppendFinalResult(pr PartialResult, input interface{}) error //error is for test
}

type WindowAggFuncer interface {
	// AllocPartialResult creates a new partial result which used to store the intermediate
	// result of the aggregate function.
	AllocPartialResult() PartialResult

	// ResetPartialResult resets the partial result to the original state.
	ResetPartialResult(pr PartialResult)

	// UpdatePartialResult updates the partial result according to the input
	UpdatePartialResult(pr PartialResult, input interface{})

	// MergePartialResult merges the partial result of one group by key into another one.
	// It converts the input PartialResult to the specific data structure which stores the

	MergePartialResult(pr PartialResult, input PartialResult)

	// AppendFinalResult appends the partial result to the input interface{}.
	// It converts the input PartialResult to the specific data structure which stores the

	AppendFinalResult(pr PartialResult, input interface{})
}

// NewWindowAggFunc creates a new WindowAggFunc object for the specific aggregate function.
type NewWindowAggFunc func([]Expression) WindowAggFuncer //WindowAggFuncer is the interface to evaluate the aggregate functions using sliding window.

// WindowFrameBoundType is the type of a bound of a window frame.
type WindowFrameBoundType int

// WindowFrameBoundTypes are the types of a bound of a window frame.
const (
	Inclusive = WindowFrameBoundType(iota)
	Exclusive
)

// WindowFrameBound is the type of a bound of a window frame.
type WindowFrameBound struct {
	Type WindowFrameBoundType
	Num  int64
}

type Bound struct {
	Lower *WindowFrameBound
	Upper *WindowFrameBound
}

// WindowFrame is the type of a window frame.
type WindowFrame struct {
	Start Bound
	End   Bound
}
