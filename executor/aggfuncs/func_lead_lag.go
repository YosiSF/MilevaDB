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

	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

const (
	// DefPartialResult4LeadLagSize is the size of partialResult4LeadLag
	DefPartialResult4LeadLagSize = int64(unsafe.Sizeof(partialResult4LeadLag{}))
)

type baseLeadLag struct {
	baseAggFunc
	valueEvaluator // TODO: move it to partial result when parallel execution is supported.

	defaultExpr expression.Expression
	offset      uint64
}

type partialResult4LeadLag struct {
	rows   []chunk.Event
	curIdx uint64
}

func (v *baseLeadLag) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4LeadLag{}), DefPartialResult4LeadLagSize
}

func (v *baseLeadLag) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4LeadLag)(pr)
	p.rows = p.rows[:0]
	p.curIdx = 0
}

func (v *baseLeadLag) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4LeadLag)(pr)
	p.rows = append(p.rows, rowsInGroup...)
	memDelta += int64(len(rowsInGroup)) * DefEventSize
	return memDelta, nil
}

type lead struct {
	baseLeadLag
}

func (v *lead) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4LeadLag)(pr)
	var err error
	if p.curIdx+v.offset < uint64(len(p.rows)) {
		_, err = v.evaluateEvent(sctx, v.args[0], p.rows[p.curIdx+v.offset])
	} else {
		_, err = v.evaluateEvent(sctx, v.defaultExpr, p.rows[p.curIdx])
	}
	if err != nil {
		return err
	}
	v.appendResult(chk, v.ordinal)
	p.curIdx++
	return nil
}

type lag struct {
	baseLeadLag
}

func (v *lag) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4LeadLag)(pr)
	var err error
	if p.curIdx >= v.offset {
		_, err = v.evaluateEvent(sctx, v.args[0], p.rows[p.curIdx-v.offset])
	} else {
		_, err = v.evaluateEvent(sctx, v.defaultExpr, p.rows[p.curIdx])
	}
	if err != nil {
		return err
	}
	v.appendResult(chk, v.ordinal)
	p.curIdx++
	return nil
}
