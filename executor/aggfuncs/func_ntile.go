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

	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
)

const (
	// DefPartialResult4Ntile is the size of partialResult4Ntile
	DefPartialResult4Ntile = int64(unsafe.Sizeof(partialResult4Ntile{}))
)

// ntile divides the partition into n ranked groups and returns the group number a event belongs to.
// e.g. We have 11 rows and n = 3. They will be divided into 3 groups.
//      First 4 rows belongs to group 1. Following 4 rows belongs to group 2. The last 3 rows belongs to group 3.
type ntile struct {
	n uint64
	baseAggFunc
}

type partialResult4Ntile struct {
	curIdx      uint64
	curGroupIdx uint64
	remainder   uint64
	quotient    uint64
	numEvents   uint64
}

func (n *ntile) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4Ntile{curGroupIdx: 1}), DefPartialResult4Ntile
}

func (n *ntile) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Ntile)(pr)
	p.curIdx = 0
	p.curGroupIdx = 1
	p.numEvents = 0
}

func (n *ntile) UFIDelatePartialResult(_ stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Ntile)(pr)
	p.numEvents += uint64(len(rowsInGroup))
	// UFIDelate the quotient and remainder.
	if n.n != 0 {
		p.quotient = p.numEvents / n.n
		p.remainder = p.numEvents % n.n
	}
	return 0, nil
}

func (n *ntile) AppendFinalResult2Chunk(_ stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Ntile)(pr)

	// If the divisor is 0, the arg of NTILE would be NULL. So we just return NULL.
	if n.n == 0 {
		chk.AppendNull(n.ordinal)
		return nil
	}

	chk.AppendUint64(n.ordinal, p.curGroupIdx)

	p.curIdx++
	curMaxIdx := p.quotient
	if p.curGroupIdx <= p.remainder {
		curMaxIdx++
	}
	if p.curIdx == curMaxIdx {
		p.curIdx = 0
		p.curGroupIdx++
	}
	return nil
}
