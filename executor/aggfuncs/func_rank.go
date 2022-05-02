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

package aggfuncs

import (
	"unsafe"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
)

const (
	//DefPartialResult4RankSize is the size of partialResult4Rank
	DefPartialResult4RankSize = int64(unsafe.Sizeof(partialResult4Rank{}))
)

type rank struct {
	baseAggFunc
	isDense bool
	rowComparer
}

type partialResult4Rank struct {
	curIdx   int64
	lastRank int64
	rows     []chunk.Event
}

func (r *rank) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4Rank{}), DefPartialResult4RankSize
}

func (r *rank) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Rank)(pr)
	p.curIdx = 0
	p.lastRank = 0
	p.rows = p.rows[:0]
}

func (r *rank) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Rank)(pr)
	p.rows = append(p.rows, rowsInGroup...)
	memDelta += int64(len(rowsInGroup)) * DefEventSize
	return memDelta, nil
}

func (r *rank) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Rank)(pr)
	p.curIdx++
	if p.curIdx == 1 {
		p.lastRank = 1
		chk.AppendInt64(r.ordinal, p.lastRank)
		return nil
	}
	if r.compareEvents(p.rows[p.curIdx-2], p.rows[p.curIdx-1]) == 0 {
		chk.AppendInt64(r.ordinal, p.lastRank)
		return nil
	}
	if r.isDense {
		p.lastRank++
	} else {
		p.lastRank = p.curIdx
	}
	chk.AppendInt64(r.ordinal, p.lastRank)
	return nil
}

type rowComparer struct {
	cmpFuncs   []chunk.CompareFunc
	defCausIdx []int
}

func buildEventComparer(defcaus []*expression.DeferredCauset) rowComparer {
	rc := rowComparer{}
	rc.defCausIdx = make([]int, 0, len(defcaus))
	rc.cmpFuncs = make([]chunk.CompareFunc, 0, len(defcaus))
	for _, defCaus := range defcaus {
		cmpFunc := chunk.GetCompareFunc(defCaus.RetType)
		if cmpFunc == nil {
			continue
		}
		rc.cmpFuncs = append(rc.cmpFuncs, chunk.GetCompareFunc(defCaus.RetType))
		rc.defCausIdx = append(rc.defCausIdx, defCaus.Index)
	}
	return rc
}

func (rc *rowComparer) compareEvents(prev, curr chunk.Event) int {
	for i, idx := range rc.defCausIdx {
		res := rc.cmpFuncs[i](prev, idx, curr, idx)
		if res != 0 {
			return res
		}
	}
	return 0
}
