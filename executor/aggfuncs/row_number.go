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
	// DefPartialResult4EventNumberSize is the size of partialResult4EventNumberSize
	DefPartialResult4EventNumberSize = int64(unsafe.Sizeof(partialResult4EventNumber{}))
)

type rowNumber struct {
	baseAggFunc
}

type partialResult4EventNumber struct {
	curIdx int64
}

func (rn *rowNumber) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4EventNumber{}), DefPartialResult4EventNumberSize
}

func (rn *rowNumber) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4EventNumber)(pr)
	p.curIdx = 0
}

func (rn *rowNumber) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	return 0, nil
}

func (rn *rowNumber) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4EventNumber)(pr)
	p.curIdx++
	chk.AppendInt64(rn.ordinal, p.curIdx)
	return nil
}
