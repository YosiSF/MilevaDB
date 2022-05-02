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

package executor

import (
	"fmt"
	"hash"
	"hash/fnv"

	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
)

func (s *pkgTestSuite) testHashBlocks(c *C) {
	var ht baseHashBlock
	test := func() {
		ht.Put(1, chunk.EventPtr{ChkIdx: 1, EventIdx: 1})
		c.Check(ht.Get(1), DeepEquals, []chunk.EventPtr{{ChkIdx: 1, EventIdx: 1}})

		rawData := map[uint64][]chunk.EventPtr{}
		for i := uint64(0); i < 10; i++ {
			for j := uint64(0); j < initialEntrySliceLen*i; j++ {
				rawData[i] = append(rawData[i], chunk.EventPtr{ChkIdx: uint32(i), EventIdx: uint32(j)})
			}
		}
		// put all rawData into ht vertically
		for j := uint64(0); j < initialEntrySliceLen*9; j++ {
			for i := 9; i >= 0; i-- {
				i := uint64(i)
				if !(j < initialEntrySliceLen*i) {
					break
				}
				ht.Put(i, rawData[i][j])
			}
		}
		// check
		totalCount := 0
		for i := uint64(0); i < 10; i++ {
			totalCount += len(rawData[i])
			c.Check(ht.Get(i), DeepEquals, rawData[i])
		}
		c.Check(ht.Len(), Equals, uint64(totalCount))
	}
	// test unsafeHashBlock
	ht = newUnsafeHashBlock(0)
	test()
	// test ConcurrentMapHashBlock
	ht = newConcurrentMapHashBlock()
	test()
}

func initBuildChunk(numEvents int) (*chunk.Chunk, []*types.FieldType) {
	numDefCauss := 6
	defCausTypes := make([]*types.FieldType, 0, numDefCauss)
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeNewDecimal})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeJSON})

	oldChk := chunk.NewChunkWithCapacity(defCausTypes, numEvents)
	for i := 0; i < numEvents; i++ {
		str := fmt.Sprintf("%d.12345", i)
		oldChk.AppendNull(0)
		oldChk.AppendInt64(1, int64(i))
		oldChk.AppendString(2, str)
		oldChk.AppendString(3, str)
		oldChk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		oldChk.AppendJSON(5, json.CreateBinary(str))
	}
	return oldChk, defCausTypes
}

func initProbeChunk(numEvents int) (*chunk.Chunk, []*types.FieldType) {
	numDefCauss := 3
	defCausTypes := make([]*types.FieldType, 0, numDefCauss)
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})

	oldChk := chunk.NewChunkWithCapacity(defCausTypes, numEvents)
	for i := 0; i < numEvents; i++ {
		str := fmt.Sprintf("%d.12345", i)
		oldChk.AppendNull(0)
		oldChk.AppendInt64(1, int64(i))
		oldChk.AppendString(2, str)
	}
	return oldChk, defCausTypes
}

type hashDefCauslision struct {
	count int
}

func (h *hashDefCauslision) Sum64() uint64 {
	h.count++
	return 0
}
func (h hashDefCauslision) Write(p []byte) (n int, err error) { return len(p), nil }
func (h hashDefCauslision) Reset()                            {}
func (h hashDefCauslision) Sum(b []byte) []byte               { panic("not implemented") }
func (h hashDefCauslision) Size() int                         { panic("not implemented") }
func (h hashDefCauslision) BlockSize() int                    { panic("not implemented") }

func (s *pkgTestSerialSuite) TestHashEventContainer(c *C) {
	hashFunc := func() hash.Hash64 {
		return fnv.New64()
	}
	rowContainer := s.testHashEventContainer(c, hashFunc, false)
	c.Assert(rowContainer.stat.probeDefCauslision, Equals, 0)
	// On windows time.Now() is imprecise, the elapse time may equal 0
	c.Assert(rowContainer.stat.buildBlockElapse >= 0, IsTrue)

	rowContainer = s.testHashEventContainer(c, hashFunc, true)
	c.Assert(rowContainer.stat.probeDefCauslision, Equals, 0)
	c.Assert(rowContainer.stat.buildBlockElapse >= 0, IsTrue)

	h := &hashDefCauslision{count: 0}
	hashFuncDefCauslision := func() hash.Hash64 {
		return h
	}
	rowContainer = s.testHashEventContainer(c, hashFuncDefCauslision, false)
	c.Assert(h.count > 0, IsTrue)
	c.Assert(rowContainer.stat.probeDefCauslision > 0, IsTrue)
	c.Assert(rowContainer.stat.buildBlockElapse >= 0, IsTrue)
}

func (s *pkgTestSerialSuite) testHashEventContainer(c *C, hashFunc func() hash.Hash64, spill bool) *hashEventContainer {
	sctx := mock.NewContext()
	var err error
	numEvents := 10

	chk0, defCausTypes := initBuildChunk(numEvents)
	chk1, _ := initBuildChunk(numEvents)

	hCtx := &hashContext{
		allTypes:      defCausTypes,
		keyDefCausIdx: []int{1, 2},
	}
	hCtx.hasNull = make([]bool, numEvents)
	for i := 0; i < numEvents; i++ {
		hCtx.hashVals = append(hCtx.hashVals, hashFunc())
	}
	rowContainer := newHashEventContainer(sctx, 0, hCtx)
	tracker := rowContainer.GetMemTracker()
	tracker.SetLabel(memory.LabelForBuildSideResult)
	if spill {
		tracker.SetBytesLimit(1)
		rowContainer.rowContainer.CausetActionSpillForTest().CausetAction(tracker)
	}
	err = rowContainer.PutChunk(chk0, nil)
	c.Assert(err, IsNil)
	err = rowContainer.PutChunk(chk1, nil)
	c.Assert(err, IsNil)
	rowContainer.CausetActionSpill().(*chunk.SpillDiskCausetAction).WaitForTest()
	c.Assert(rowContainer.alreadySpilledSafeForTest(), Equals, spill)
	c.Assert(rowContainer.GetMemTracker().BytesConsumed() == 0, Equals, spill)
	c.Assert(rowContainer.GetMemTracker().BytesConsumed() > 0, Equals, !spill)
	if rowContainer.alreadySpilledSafeForTest() {
		c.Assert(rowContainer.GetDiskTracker(), NotNil)
		c.Assert(rowContainer.GetDiskTracker().BytesConsumed() > 0, Equals, true)
	}

	probeChk, probeDefCausType := initProbeChunk(2)
	probeEvent := probeChk.GetEvent(1)
	probeCtx := &hashContext{
		allTypes:      probeDefCausType,
		keyDefCausIdx: []int{1, 2},
	}
	probeCtx.hasNull = make([]bool, 1)
	probeCtx.hashVals = append(hCtx.hashVals, hashFunc())
	matched, _, err := rowContainer.GetMatchedEventsAndPtrs(hCtx.hashVals[1].Sum64(), probeEvent, probeCtx)
	c.Assert(err, IsNil)
	c.Assert(len(matched), Equals, 2)
	c.Assert(matched[0].GetCausetEvent(defCausTypes), DeepEquals, chk0.GetEvent(1).GetCausetEvent(defCausTypes))
	c.Assert(matched[1].GetCausetEvent(defCausTypes), DeepEquals, chk1.GetEvent(1).GetCausetEvent(defCausTypes))
	return rowContainer
}
