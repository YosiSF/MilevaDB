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

package aggfuncs

import (
	"unsafe"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

const (
	// DefPartialResult4VarPopFloat64Size is the size of partialResult4VarPopFloat64
	DefPartialResult4VarPopFloat64Size = int64(unsafe.Sizeof(partialResult4VarPopFloat64{}))
	// DefPartialResult4VarPoFIDelistinctFloat64Size is the size of partialResult4VarPoFIDelistinctFloat64
	DefPartialResult4VarPoFIDelistinctFloat64Size = int64(unsafe.Sizeof(partialResult4VarPoFIDelistinctFloat64{}))
)

type baseVarPopAggFunc struct {
	baseAggFunc
}

type varPop4Float64 struct {
	baseVarPopAggFunc
}

type partialResult4VarPopFloat64 struct {
	count    int64
	sum      float64
	variance float64
}

func (e *varPop4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4VarPopFloat64{}), DefPartialResult4VarPopFloat64Size
}

func (e *varPop4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4VarPopFloat64)(pr)
	p.count = 0
	p.sum = 0
	p.variance = 0
}

func (e *varPop4Float64) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarPopFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	variance := p.variance / float64(p.count)
	chk.AppendFloat64(e.ordinal, variance)
	return nil
}

func calculateIntermediate(count int64, sum float64, input float64, variance float64) float64 {
	t := float64(count)*input - sum
	variance += (t * t) / (float64(count * (count - 1)))
	return variance
}

func (e *varPop4Float64) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4VarPopFloat64)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, event)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if isNull {
			continue
		}
		p.count++
		p.sum += input
		if p.count > 1 {
			p.variance = calculateIntermediate(p.count, p.sum, input, p.variance)
		}
	}
	return 0, nil
}

func calculateMerge(srcCount, dstCount int64, srcSum, dstSum, srcVariance, dstVariance float64) float64 {
	srcCountFloat64 := float64(srcCount)
	dstCountFloat64 := float64(dstCount)

	t := (srcCountFloat64/dstCountFloat64)*dstSum - srcSum
	dstVariance += srcVariance + ((dstCountFloat64/srcCountFloat64)/(dstCountFloat64+srcCountFloat64))*t*t
	return dstVariance
}

func (e *varPop4Float64) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4VarPopFloat64)(src), (*partialResult4VarPopFloat64)(dst)
	if p1.count == 0 {
		return 0, nil
	}
	if p2.count == 0 {
		p2.count = p1.count
		p2.sum = p1.sum
		p2.variance = p1.variance
		return 0, nil
	}
	if p2.count != 0 && p1.count != 0 {
		p2.variance = calculateMerge(p1.count, p2.count, p1.sum, p2.sum, p1.variance, p2.variance)
		p2.count += p1.count
		p2.sum += p1.sum
	}
	return 0, nil
}

type varPop4DistinctFloat64 struct {
	baseVarPopAggFunc
}

type partialResult4VarPoFIDelistinctFloat64 struct {
	count    int64
	sum      float64
	variance float64
	valSet   set.Float64Set
}

func (e *varPop4DistinctFloat64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4VarPoFIDelistinctFloat64)
	p.count = 0
	p.sum = 0
	p.variance = 0
	p.valSet = set.NewFloat64Set()
	return PartialResult(p), DefPartialResult4VarPoFIDelistinctFloat64Size
}

func (e *varPop4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4VarPoFIDelistinctFloat64)(pr)
	p.count = 0
	p.sum = 0
	p.variance = 0
	p.valSet = set.NewFloat64Set()
}

func (e *varPop4DistinctFloat64) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4VarPoFIDelistinctFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	variance := p.variance / float64(p.count)
	chk.AppendFloat64(e.ordinal, variance)
	return nil
}

func (e *varPop4DistinctFloat64) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4VarPoFIDelistinctFloat64)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, event)
		if err != nil {
			return memDelta, errors.Trace(err)
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}
		p.valSet.Insert(input)
		p.count++
		p.sum += input

		memDelta += DefFloat64Size
		if p.count > 1 {
			p.variance = calculateIntermediate(p.count, p.sum, input, p.variance)
		}
	}
	return memDelta, nil
}
