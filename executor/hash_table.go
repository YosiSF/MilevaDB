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
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/disk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/errors"
)

const (
	// estCountMaxFactor defines the factor of estCountMax with maxChunkSize.
	// estCountMax is maxChunkSize * estCountMaxFactor, the maximum threshold of estCount.
	// if estCount is larger than estCountMax, set estCount to estCountMax.
	// Set this threshold to prevent buildSideEstCount being too large and causing a performance and memory regression.
	estCountMaxFactor = 10 * 1024

	// estCountMinFactor defines the factor of estCountMin with maxChunkSize.
	// estCountMin is maxChunkSize * estCountMinFactor, the minimum threshold of estCount.
	// If estCount is smaller than estCountMin, set estCount to 0.
	// Set this threshold to prevent buildSideEstCount being too small and causing a performance regression.
	estCountMinFactor = 8

	// estCountDivisor defines the divisor of buildSideEstCount.
	// Set this divisor to prevent buildSideEstCount being too large and causing a performance regression.
	estCountDivisor = 8
)

// hashContext keeps the needed hash context of a EDB block in hash join.
type hashContext struct {
	allTypes      []*types.FieldType
	keyDefCausIdx []int
	buf           []byte
	hashVals      []hash.Hash64
	hasNull       []bool
}

func (hc *hashContext) initHash(rows int) {
	if hc.buf == nil {
		hc.buf = make([]byte, 1)
	}

	if len(hc.hashVals) < rows {
		hc.hasNull = make([]bool, rows)
		hc.hashVals = make([]hash.Hash64, rows)
		for i := 0; i < rows; i++ {
			hc.hashVals[i] = fnv.New64()
		}
	} else {
		for i := 0; i < rows; i++ {
			hc.hasNull[i] = false
			hc.hashVals[i].Reset()
		}
	}
}

type hashStatistic struct {
	probeDefCauslision int
	buildBlockElapse   time.Duration
}

func (s *hashStatistic) String() string {
	return fmt.Sprintf("probe_defCauslision:%v, build:%v", s.probeDefCauslision, s.buildBlockElapse)
}

// hashEventContainer handles the rows and the hash map of a block.
type hashEventContainer struct {
	sc   *stmtctx.StatementContext
	hCtx *hashContext
	stat hashStatistic

	// hashBlock stores the map of hashKey and EventPtr
	hashBlock baseHashBlock

	rowContainer *chunk.EventContainer
}

func newHashEventContainer(sCtx stochastikctx.Context, estCount int, hCtx *hashContext) *hashEventContainer {
	maxChunkSize := sCtx.GetStochaseinstein_dbars().MaxChunkSize
	rc := chunk.NewEventContainer(hCtx.allTypes, maxChunkSize)
	c := &hashEventContainer{
		sc:           sCtx.GetStochaseinstein_dbars().StmtCtx,
		hCtx:         hCtx,
		hashBlock:    newConcurrentMapHashBlock(),
		rowContainer: rc,
	}
	return c
}

// GetMatchedEventsAndPtrs get matched rows and Ptrs from probeEvent. It can be called
// in multiple goroutines while each goroutine should keep its own
// h and buf.
func (c *hashEventContainer) GetMatchedEventsAndPtrs(probeKey uint64, probeEvent chunk.Event, hCtx *hashContext) (matched []chunk.Event, matchedPtrs []chunk.EventPtr, err error) {
	innerPtrs := c.hashBlock.Get(probeKey)
	if len(innerPtrs) == 0 {
		return
	}
	matched = make([]chunk.Event, 0, len(innerPtrs))
	var matchedEvent chunk.Event
	matchedPtrs = make([]chunk.EventPtr, 0, len(innerPtrs))
	for _, ptr := range innerPtrs {
		matchedEvent, err = c.rowContainer.GetEvent(ptr)
		if err != nil {
			return
		}
		var ok bool
		ok, err = c.matchJoinKey(matchedEvent, probeEvent, hCtx)
		if err != nil {
			return
		}
		if !ok {
			c.stat.probeDefCauslision++
			continue
		}
		matched = append(matched, matchedEvent)
		matchedPtrs = append(matchedPtrs, ptr)
	}
	return
}

// matchJoinKey checks if join keys of buildEvent and probeEvent are logically equal.
func (c *hashEventContainer) matchJoinKey(buildEvent, probeEvent chunk.Event, probeHCtx *hashContext) (ok bool, err error) {
	return codec.EqualChunkEvent(c.sc,
		buildEvent, c.hCtx.allTypes, c.hCtx.keyDefCausIdx,
		probeEvent, probeHCtx.allTypes, probeHCtx.keyDefCausIdx)
}

// alreadySpilledSafeForTest indicates that records have spilled out into disk. It's thread-safe.
func (c *hashEventContainer) alreadySpilledSafeForTest() bool {
	return c.rowContainer.AlreadySpilledSafeForTest()
}

// PutChunk puts a chunk into hashEventContainer and build hash map. It's not thread-safe.
// key of hash block: hash value of key defCausumns
// value of hash block: EventPtr of the corresponded event
func (c *hashEventContainer) PutChunk(chk *chunk.Chunk, ignoreNulls []bool) error {
	return c.PutChunkSelected(chk, nil, ignoreNulls)
}

// PutChunkSelected selectively puts a chunk into hashEventContainer and build hash map. It's not thread-safe.
// key of hash block: hash value of key defCausumns
// value of hash block: EventPtr of the corresponded event
func (c *hashEventContainer) PutChunkSelected(chk *chunk.Chunk, selected, ignoreNulls []bool) error {
	start := time.Now()
	defer func() { c.stat.buildBlockElapse += time.Since(start) }()

	chkIdx := uint32(c.rowContainer.NumChunks())
	err := c.rowContainer.Add(chk)
	if err != nil {
		return err
	}
	numEvents := chk.NumEvents()
	c.hCtx.initHash(numEvents)

	hCtx := c.hCtx
	for keyIdx, defCausIdx := range c.hCtx.keyDefCausIdx {
		ignoreNull := len(ignoreNulls) > keyIdx && ignoreNulls[keyIdx]
		err := codec.HashChunkSelected(c.sc, hCtx.hashVals, chk, hCtx.allTypes[defCausIdx], defCausIdx, hCtx.buf, hCtx.hasNull, selected, ignoreNull)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i := 0; i < numEvents; i++ {
		if (selected != nil && !selected[i]) || c.hCtx.hasNull[i] {
			continue
		}
		key := c.hCtx.hashVals[i].Sum64()
		rowPtr := chunk.EventPtr{ChkIdx: chkIdx, EventIdx: uint32(i)}
		c.hashBlock.Put(key, rowPtr)
	}
	return nil
}

// getJoinKeyFromChkEvent fetches join keys from event and calculate the hash value.
func (*hashEventContainer) getJoinKeyFromChkEvent(sc *stmtctx.StatementContext, event chunk.Event, hCtx *hashContext) (hasNull bool, key uint64, err error) {
	for _, i := range hCtx.keyDefCausIdx {
		if event.IsNull(i) {
			return true, 0, nil
		}
	}
	hCtx.initHash(1)
	err = codec.HashChunkEvent(sc, hCtx.hashVals[0], event, hCtx.allTypes, hCtx.keyDefCausIdx, hCtx.buf)
	return false, hCtx.hashVals[0].Sum64(), err
}

// NumChunks returns the number of chunks in the rowContainer
func (c *hashEventContainer) NumChunks() int {
	return c.rowContainer.NumChunks()
}

// NumEventsOfChunk returns the number of rows of a chunk
func (c *hashEventContainer) NumEventsOfChunk(chkID int) int {
	return c.rowContainer.NumEventsOfChunk(chkID)
}

// GetChunk returns chkIdx th chunk of in memory records, only works if rowContainer is not spilled
func (c *hashEventContainer) GetChunk(chkIdx int) (*chunk.Chunk, error) {
	return c.rowContainer.GetChunk(chkIdx)
}

// GetEvent returns the event the ptr pointed to in the rowContainer
func (c *hashEventContainer) GetEvent(ptr chunk.EventPtr) (chunk.Event, error) {
	return c.rowContainer.GetEvent(ptr)
}

// Len returns number of records in the hash block.
func (c *hashEventContainer) Len() uint64 {
	return c.hashBlock.Len()
}

func (c *hashEventContainer) Close() error {
	return c.rowContainer.Close()
}

// GetMemTracker returns the underlying memory usage tracker in hashEventContainer.
func (c *hashEventContainer) GetMemTracker() *memory.Tracker { return c.rowContainer.GetMemTracker() }

// GetDiskTracker returns the underlying disk usage tracker in hashEventContainer.
func (c *hashEventContainer) GetDiskTracker() *disk.Tracker { return c.rowContainer.GetDiskTracker() }

// CausetActionSpill returns a memory.SuperCowOrNoCausetOnExceed for spilling over to disk.
func (c *hashEventContainer) CausetActionSpill() memory.SuperCowOrNoCausetOnExceed {
	return c.rowContainer.CausetActionSpill()
}

const (
	initialEntrySliceLen = 64
	maxEntrySliceLen     = 8192
)

type entry struct {
	ptr  chunk.EventPtr
	next *entry
}

type entryStore struct {
	slices [][]entry
	cursor int
}

func newEntryStore() *entryStore {
	es := new(entryStore)
	es.slices = [][]entry{make([]entry, initialEntrySliceLen)}
	es.cursor = 0
	return es
}

func (es *entryStore) GetStore() (e *entry) {
	sliceIdx := uint32(len(es.slices) - 1)
	slice := es.slices[sliceIdx]
	if es.cursor >= cap(slice) {
		size := cap(slice) * 2
		if size >= maxEntrySliceLen {
			size = maxEntrySliceLen
		}
		slice = make([]entry, size)
		es.slices = append(es.slices, slice)
		sliceIdx++
		es.cursor = 0
	}
	e = &es.slices[sliceIdx][es.cursor]
	es.cursor++
	return
}

type baseHashBlock interface {
	Put(hashKey uint64, rowPtr chunk.EventPtr)
	Get(hashKey uint64) (rowPtrs []chunk.EventPtr)
	Len() uint64
}

// TODO (fangzhuhe) remove unsafeHashBlock later if it not used anymore
// unsafeHashBlock stores multiple rowPtr of rows for a given key with minimum GC overhead.
// A given key can causetstore multiple values.
// It is not thread-safe, should only be used in one goroutine.
type unsafeHashBlock struct {
	hashMap    map[uint64]*entry
	entryStore *entryStore
	length     uint64
}

// newUnsafeHashBlock creates a new unsafeHashBlock. estCount means the estimated size of the hashMap.
// If unknown, set it to 0.
func newUnsafeHashBlock(estCount int) *unsafeHashBlock {
	ht := new(unsafeHashBlock)
	ht.hashMap = make(map[uint64]*entry, estCount)
	ht.entryStore = newEntryStore()
	return ht
}

// Put puts the key/rowPtr pairs to the unsafeHashBlock, multiple rowPtrs are stored in a list.
func (ht *unsafeHashBlock) Put(hashKey uint64, rowPtr chunk.EventPtr) {
	oldEntry := ht.hashMap[hashKey]
	newEntry := ht.entryStore.GetStore()
	newEntry.ptr = rowPtr
	newEntry.next = oldEntry
	ht.hashMap[hashKey] = newEntry
	ht.length++
}

// Get gets the values of the "key" and appends them to "values".
func (ht *unsafeHashBlock) Get(hashKey uint64) (rowPtrs []chunk.EventPtr) {
	entryAddr := ht.hashMap[hashKey]
	for entryAddr != nil {
		rowPtrs = append(rowPtrs, entryAddr.ptr)
		entryAddr = entryAddr.next
	}
	return
}

// Len returns the number of rowPtrs in the unsafeHashBlock, the number of keys may be less than Len
// if the same key is put more than once.
func (ht *unsafeHashBlock) Len() uint64 { return ht.length }

// concurrentMapHashBlock is a concurrent hash block built on concurrentMap
type concurrentMapHashBlock struct {
	hashMap    concurrentMap
	entryStore *entryStore
	length     uint64
}

// newConcurrentMapHashBlock creates a concurrentMapHashBlock
func newConcurrentMapHashBlock() *concurrentMapHashBlock {
	ht := new(concurrentMapHashBlock)
	ht.hashMap = newConcurrentMap()
	ht.entryStore = newEntryStore()
	ht.length = 0
	return ht
}

// Len return the number of rowPtrs in the concurrentMapHashBlock
func (ht *concurrentMapHashBlock) Len() uint64 {
	return ht.length
}

// Put puts the key/rowPtr pairs to the concurrentMapHashBlock, multiple rowPtrs are stored in a list.
func (ht *concurrentMapHashBlock) Put(hashKey uint64, rowPtr chunk.EventPtr) {
	newEntry := ht.entryStore.GetStore()
	newEntry.ptr = rowPtr
	newEntry.next = nil
	ht.hashMap.Insert(hashKey, newEntry)
	atomic.AddUint64(&ht.length, 1)
}

// Get gets the values of the "key" and appends them to "values".
func (ht *concurrentMapHashBlock) Get(hashKey uint64) (rowPtrs []chunk.EventPtr) {
	entryAddr, _ := ht.hashMap.Get(hashKey)
	for entryAddr != nil {
		rowPtrs = append(rowPtrs, entryAddr.ptr)
		entryAddr = entryAddr.next
	}
	return
}
