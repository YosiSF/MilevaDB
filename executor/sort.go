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

package executor

import (
	"container/heap"
	"context"
	"errors"
	"sort"

	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/expression"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/disk"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/types"
)

// SortExec represents sorting executor.
type SortExec struct {
	baseExecutor

	ByItems []*soliton.ByItems
	Idx     int
	fetched bool
	schemaReplicant  *expression.Schema

	keyExprs []expression.Expression
	keyTypes []*types.FieldType
	// keyDeferredCausets is the defCausumn index of the by items.
	keyDeferredCausets []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// rowChunks is the chunks to causetstore event values.
	rowChunks *chunk.SortedEventContainer

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	// partitionList is the chunks to causetstore event values for partitions. Every partition is a sorted list.
	partitionList []*chunk.SortedEventContainer

	// multiWayMerge uses multi-way merge for spill disk.
	// The multi-way merge algorithm can refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm
	multiWayMerge *multiWayMerge
	// spillCausetAction save the CausetAction for spill disk.
	spillCausetAction *chunk.SortAndSpillDiskCausetAction
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	for _, container := range e.partitionList {
		err := container.Close()
		if err != nil {
			return err
		}
	}
	e.partitionList = e.partitionList[:0]

	if e.rowChunks != nil {
		e.memTracker.Consume(-e.rowChunks.GetMemTracker().BytesConsumed())
		e.rowChunks = nil
	}
	e.memTracker = nil
	e.diskTracker = nil
	e.multiWayMerge = nil
	e.spillCausetAction = nil
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = false
	e.Idx = 0

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.id, -1)
		e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)
		e.diskTracker = memory.NewTracker(e.id, -1)
		e.diskTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.DiskTracker)
	}
	e.partitionList = e.partitionList[:0]
	return e.children[0].Open(ctx)
}

// Next implements the Executor Next interface.
// Sort constructs the result following these step:
// 1. Read as mush as rows into memory.
// 2. If memory quota is triggered, sort these rows in memory and put them into disk as partition 1, then reset
//    the memory quota trigger and return to step 1
// 3. If memory quota is not triggered and child is consumed, sort these rows in memory as partition N.
// 4. Merge sort if the count of partitions is larger than 1. If there is only one partition in step 4, it works
//    just like in-memory sort before.
func (e *SortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.initCompareFuncs()
		e.buildKeyDeferredCausets()
		err := e.fetchEventChunks(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}

	if len(e.partitionList) == 0 {
		return nil
	}
	if len(e.partitionList) > 1 {
		if err := e.externalSorting(req); err != nil {
			return err
		}
	} else {
		for !req.IsFull() && e.Idx < e.partitionList[0].NumEvent() {
			event, err := e.partitionList[0].GetSortedEvent(e.Idx)
			if err != nil {
				return err
			}
			req.AppendEvent(event)
			e.Idx++
		}
	}
	return nil
}

type partitionPointer struct {
	event         chunk.Event
	partitionID int
	consumed    int
}

type multiWayMerge struct {
	lessEventFunction func(rowI chunk.Event, rowJ chunk.Event) bool
	elements        []partitionPointer
}

func (h *multiWayMerge) Less(i, j int) bool {
	rowI := h.elements[i].event
	rowJ := h.elements[j].event
	return h.lessEventFunction(rowI, rowJ)
}

func (h *multiWayMerge) Len() int {
	return len(h.elements)
}

func (h *multiWayMerge) Push(x interface{}) {
	// Should never be called.
}

func (h *multiWayMerge) Pop() interface{} {
	h.elements = h.elements[:len(h.elements)-1]
	return nil
}

func (h *multiWayMerge) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}

func (e *SortExec) externalSorting(req *chunk.Chunk) (err error) {
	if e.multiWayMerge == nil {
		e.multiWayMerge = &multiWayMerge{e.lessEvent, make([]partitionPointer, 0, len(e.partitionList))}
		for i := 0; i < len(e.partitionList); i++ {
			event, err := e.partitionList[i].GetSortedEvent(0)
			if err != nil {
				return err
			}
			e.multiWayMerge.elements = append(e.multiWayMerge.elements, partitionPointer{event: event, partitionID: i, consumed: 0})
		}
		heap.Init(e.multiWayMerge)
	}

	for !req.IsFull() && e.multiWayMerge.Len() > 0 {
		partitionPtr := e.multiWayMerge.elements[0]
		req.AppendEvent(partitionPtr.event)
		partitionPtr.consumed++
		if partitionPtr.consumed >= e.partitionList[partitionPtr.partitionID].NumEvent() {
			heap.Remove(e.multiWayMerge, 0)
			continue
		}
		partitionPtr.event, err = e.partitionList[partitionPtr.partitionID].
			GetSortedEvent(partitionPtr.consumed)
		if err != nil {
			return err
		}
		e.multiWayMerge.elements[0] = partitionPtr
		heap.Fix(e.multiWayMerge, 0)
	}
	return nil
}

func (e *SortExec) fetchEventChunks(ctx context.Context) error {
	fields := retTypes(e)
	byItemsDesc := make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}
	e.rowChunks = chunk.NewSortedEventContainer(fields, e.maxChunkSize, byItemsDesc, e.keyDeferredCausets, e.keyCmpFuncs)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(memory.LabelForEventChunks)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		e.spillCausetAction = e.rowChunks.CausetActionSpill()
		failpoint.Inject("testSortedEventContainerSpill", func(val failpoint.Value) {
			if val.(bool) {
				e.spillCausetAction = e.rowChunks.CausetActionSpillForTest()
				defer e.spillCausetAction.WaitForTest()
			}
		})
		e.ctx.GetStochastikVars().StmtCtx.MemTracker.FallbackOldAndSetNewCausetAction(e.spillCausetAction)
		e.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
		e.rowChunks.GetDiskTracker().SetLabel(memory.LabelForEventChunks)
	}
	for {
		chk := newFirstChunk(e.children[0])
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return err
		}
		rowCount := chk.NumEvents()
		if rowCount == 0 {
			break
		}
		if err := e.rowChunks.Add(chk); err != nil {
			if errors.Is(err, chunk.ErrCannotAddBecauseSorted) {
				e.partitionList = append(e.partitionList, e.rowChunks)
				e.rowChunks = chunk.NewSortedEventContainer(fields, e.maxChunkSize, byItemsDesc, e.keyDeferredCausets, e.keyCmpFuncs)
				e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
				e.rowChunks.GetMemTracker().SetLabel(memory.LabelForEventChunks)
				e.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
				e.rowChunks.GetDiskTracker().SetLabel(memory.LabelForEventChunks)
				e.spillCausetAction = e.rowChunks.CausetActionSpill()
				failpoint.Inject("testSortedEventContainerSpill", func(val failpoint.Value) {
					if val.(bool) {
						e.spillCausetAction = e.rowChunks.CausetActionSpillForTest()
						defer e.spillCausetAction.WaitForTest()
					}
				})
				e.ctx.GetStochastikVars().StmtCtx.MemTracker.FallbackOldAndSetNewCausetAction(e.spillCausetAction)
				err = e.rowChunks.Add(chk)
			}
			if err != nil {
				return err
			}
		}
	}
	if e.rowChunks.NumEvent() > 0 {
		e.rowChunks.Sort()
		e.partitionList = append(e.partitionList, e.rowChunks)
	}
	return nil
}

func (e *SortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *SortExec) buildKeyDeferredCausets() {
	e.keyDeferredCausets = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		defCaus := by.Expr.(*expression.DeferredCauset)
		e.keyDeferredCausets = append(e.keyDeferredCausets, defCaus.Index)
	}
}

func (e *SortExec) lessEvent(rowI, rowJ chunk.Event) bool {
	for i, defCausIdx := range e.keyDeferredCausets {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, defCausIdx, rowJ, defCausIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the block, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	SortExec
	limit      *plannercore.PhysicalLimit
	totalLimit uint64

	// rowChunks is the chunks to causetstore event values.
	rowChunks *chunk.List
	// rowPointer causetstore the chunk index and event index for each event.
	rowPtrs []chunk.EventPtr

	chkHeap *topNChunkHeap
}

// topNChunkHeap implements heap.Interface.
type topNChunkHeap struct {
	*TopNExec
}

// Less implement heap.Interface, but since we mantains a max heap,
// this function returns true if event i is greater than event j.
func (h *topNChunkHeap) Less(i, j int) bool {
	rowI := h.rowChunks.GetEvent(h.rowPtrs[i])
	rowJ := h.rowChunks.GetEvent(h.rowPtrs[j])
	return h.greaterEvent(rowI, rowJ)
}

func (h *topNChunkHeap) greaterEvent(rowI, rowJ chunk.Event) bool {
	for i, defCausIdx := range h.keyDeferredCausets {
		cmpFunc := h.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, defCausIdx, rowJ, defCausIdx)
		if h.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp > 0 {
			return true
		} else if cmp < 0 {
			return false
		}
	}
	return false
}

func (h *topNChunkHeap) Len() int {
	return len(h.rowPtrs)
}

func (h *topNChunkHeap) Push(x interface{}) {
	// Should never be called.
}

func (h *topNChunkHeap) Pop() interface{} {
	h.rowPtrs = h.rowPtrs[:len(h.rowPtrs)-1]
	// We don't need the popped value, return nil to avoid memory allocation.
	return nil
}

func (h *topNChunkHeap) Swap(i, j int) {
	h.rowPtrs[i], h.rowPtrs[j] = h.rowPtrs[j], h.rowPtrs[i]
}

// keyDeferredCausetsLess is the less function for key defCausumns.
func (e *TopNExec) keyDeferredCausetsLess(i, j int) bool {
	rowI := e.rowChunks.GetEvent(e.rowPtrs[i])
	rowJ := e.rowChunks.GetEvent(e.rowPtrs[j])
	return e.lessEvent(rowI, rowJ)
}

func (e *TopNExec) initPointers() {
	e.rowPtrs = make([]chunk.EventPtr, 0, e.rowChunks.Len())
	e.memTracker.Consume(int64(8 * e.rowChunks.Len()))
	for chkIdx := 0; chkIdx < e.rowChunks.NumChunks(); chkIdx++ {
		rowChk := e.rowChunks.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < rowChk.NumEvents(); rowIdx++ {
			e.rowPtrs = append(e.rowPtrs, chunk.EventPtr{ChkIdx: uint32(chkIdx), EventIdx: uint32(rowIdx)})
		}
	}
}

// Open implements the Executor Open interface.
func (e *TopNExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	e.fetched = false
	e.Idx = 0

	return e.children[0].Open(ctx)
}

// Next implements the Executor Next interface.
func (e *TopNExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.totalLimit = e.limit.Offset + e.limit.Count
		e.Idx = int(e.limit.Offset)
		err := e.loadChunksUntilTotalLimit(ctx)
		if err != nil {
			return err
		}
		err = e.executeTopN(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}
	if e.Idx >= len(e.rowPtrs) {
		return nil
	}
	for !req.IsFull() && e.Idx < len(e.rowPtrs) {
		event := e.rowChunks.GetEvent(e.rowPtrs[e.Idx])
		req.AppendEvent(event)
		e.Idx++
	}
	return nil
}

func (e *TopNExec) loadChunksUntilTotalLimit(ctx context.Context) error {
	e.chkHeap = &topNChunkHeap{e}
	e.rowChunks = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(memory.LabelForEventChunks)
	for uint64(e.rowChunks.Len()) < e.totalLimit {
		srcChk := newFirstChunk(e.children[0])
		// adjust required rows by total limit
		srcChk.SetRequiredEvents(int(e.totalLimit-uint64(e.rowChunks.Len())), e.maxChunkSize)
		err := Next(ctx, e.children[0], srcChk)
		if err != nil {
			return err
		}
		if srcChk.NumEvents() == 0 {
			break
		}
		e.rowChunks.Add(srcChk)
	}
	e.initPointers()
	e.initCompareFuncs()
	e.buildKeyDeferredCausets()
	return nil
}

const topNCompactionFactor = 4

func (e *TopNExec) executeTopN(ctx context.Context) error {
	heap.Init(e.chkHeap)
	for uint64(len(e.rowPtrs)) > e.totalLimit {
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(e.chkHeap)
	}
	childEventChk := newFirstChunk(e.children[0])
	for {
		err := Next(ctx, e.children[0], childEventChk)
		if err != nil {
			return err
		}
		if childEventChk.NumEvents() == 0 {
			break
		}
		err = e.processChildChk(childEventChk)
		if err != nil {
			return err
		}
		if e.rowChunks.Len() > len(e.rowPtrs)*topNCompactionFactor {
			err = e.doCompaction()
			if err != nil {
				return err
			}
		}
	}
	sort.Slice(e.rowPtrs, e.keyDeferredCausetsLess)
	return nil
}

func (e *TopNExec) processChildChk(childEventChk *chunk.Chunk) error {
	for i := 0; i < childEventChk.NumEvents(); i++ {
		heapMaxPtr := e.rowPtrs[0]
		var heapMax, next chunk.Event
		heapMax = e.rowChunks.GetEvent(heapMaxPtr)
		next = childEventChk.GetEvent(i)
		if e.chkHeap.greaterEvent(heapMax, next) {
			// Evict heap max, keep the next event.
			e.rowPtrs[0] = e.rowChunks.AppendEvent(childEventChk.GetEvent(i))
			heap.Fix(e.chkHeap, 0)
		}
	}
	return nil
}

// doCompaction rebuild the chunks and event pointers to release memory.
// If we don't do compaction, in a extreme case like the child data is already ascending sorted
// but we want descending top N, then we will keep all data in memory.
// But if data is distributed randomly, this function will be called log(n) times.
func (e *TopNExec) doCompaction() error {
	newEventChunks := chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
	newEventPtrs := make([]chunk.EventPtr, 0, e.rowChunks.Len())
	for _, rowPtr := range e.rowPtrs {
		newEventPtr := newEventChunks.AppendEvent(e.rowChunks.GetEvent(rowPtr))
		newEventPtrs = append(newEventPtrs, newEventPtr)
	}
	newEventChunks.GetMemTracker().SetLabel(memory.LabelForEventChunks)
	e.memTracker.ReplaceChild(e.rowChunks.GetMemTracker(), newEventChunks.GetMemTracker())
	e.rowChunks = newEventChunks

	e.memTracker.Consume(int64(-8 * len(e.rowPtrs)))
	e.memTracker.Consume(int64(8 * len(newEventPtrs)))
	e.rowPtrs = newEventPtrs
	return nil
}
