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

	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

const (
	// DefPartialResult4CumeDistSize is the size of partialResult4CumeDist
	DefPartialResult4CumeDistSize = int64(unsafe.Sizeof(partialResult4CumeDist{}))
)

type cumeDist struct {
	baseAggFunc
	rowComparer
}

type partialResult4CumeDist struct {
	curIdx   int
	lastRank int
	rows     []chunk.Event
}

func (r *cumeDist) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4CumeDist{}), DefPartialResult4CumeDistSize
}

func (r *cumeDist) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4CumeDist)(pr)
	p.curIdx = 0
	p.lastRank = 0
	p.rows = p.rows[:0]
}

func (r *cumeDist) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4CumeDist)(pr)
	p.rows = append(p.rows, rowsInGroup...)
	memDelta += int64(len(rowsInGroup)) * DefEventSize
	return memDelta, nil
}

func (r *cumeDist) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4CumeDist)(pr)
	numEvents := len(p.rows)
	for p.lastRank < numEvents && r.compareEvents(p.rows[p.curIdx], p.rows[p.lastRank]) == 0 {
		p.lastRank++
	}
	p.curIdx++
	chk.AppendFloat64(r.ordinal, float64(p.lastRank)/float64(numEvents))
	return nil
}
