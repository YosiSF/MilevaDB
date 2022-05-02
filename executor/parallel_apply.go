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
	"context"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

type result struct {
	chk *chunk.Chunk
	err error
}

type outerEvent struct {
	event    *chunk.Event
	selected bool // if this event is selected by the outer side
}

// ParallelNestedLoopApplyExec is the executor for apply.
type ParallelNestedLoopApplyExec struct {
	baseExecutor

	// outer-side fields
	cursor          int
	outerExec       Executor
	outerFilter     expression.CNFExprs
	outerList       *chunk.List
	outerEventMutex sync.Mutex
	outer           bool

	// inner-side fields
	// use slices since the inner side is paralleled
	corDefCauss   [][]*expression.CorrelatedDeferredCauset
	innerFilter   []expression.CNFExprs
	innerExecs    []Executor
	innerList     []*chunk.List
	innerChunk    []*chunk.Chunk
	innerSelected [][]bool
	innerIter     []chunk.Iterator
	outerEvent    []*chunk.Event
	hasMatch      []bool
	hasNull       []bool
	joiners       []joiner

	// fields about concurrency control
	concurrency  int
	started      uint32
	freeChkCh    chan *chunk.Chunk
	resultChkCh  chan result
	outerEventCh chan outerEvent
	exit         chan struct{}
	workerWg     sync.WaitGroup
	notifyWg     sync.WaitGroup

	// fields about cache
	cache              *applyCache
	useCache           bool
	cacheHitCounter    int64
	cacheAccessCounter int64
	cacheLock          sync.RWMutex

	memTracker *memory.Tracker // track memory usage.
}

// Open implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Open(ctx context.Context) error {
	err := e.outerExec.Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker)

	e.outerList = chunk.NewList(retTypes(e.outerExec), e.initCap, e.maxChunkSize)
	e.outerList.GetMemTracker().SetLabel(memory.LabelForOuterList)
	e.outerList.GetMemTracker().AttachTo(e.memTracker)

	e.innerList = make([]*chunk.List, e.concurrency)
	e.innerChunk = make([]*chunk.Chunk, e.concurrency)
	e.innerSelected = make([][]bool, e.concurrency)
	e.innerIter = make([]chunk.Iterator, e.concurrency)
	e.outerEvent = make([]*chunk.Event, e.concurrency)
	e.hasMatch = make([]bool, e.concurrency)
	e.hasNull = make([]bool, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		e.innerChunk[i] = newFirstChunk(e.innerExecs[i])
		e.innerList[i] = chunk.NewList(retTypes(e.innerExecs[i]), e.initCap, e.maxChunkSize)
		e.innerList[i].GetMemTracker().SetLabel(memory.LabelForInnerList)
		e.innerList[i].GetMemTracker().AttachTo(e.memTracker)
	}

	e.freeChkCh = make(chan *chunk.Chunk, e.concurrency)
	e.resultChkCh = make(chan result, e.concurrency+1) // innerWorkers + outerWorker
	e.outerEventCh = make(chan outerEvent)
	e.exit = make(chan struct{})
	for i := 0; i < e.concurrency; i++ {
		e.freeChkCh <- newFirstChunk(e)
	}

	if e.useCache {
		if e.cache, err = newApplyCache(e.ctx); err != nil {
			return err
		}
		e.cache.GetMemTracker().AttachTo(e.memTracker)
	}
	return nil
}

// Next implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if atomic.CompareAndSwapUint32(&e.started, 0, 1) {
		e.workerWg.Add(1)
		go e.outerWorker(ctx)
		for i := 0; i < e.concurrency; i++ {
			e.workerWg.Add(1)
			workID := i
			go e.innerWorker(ctx, workID)
		}
		e.notifyWg.Add(1)
		go e.notifyWorker(ctx)
	}
	result := <-e.resultChkCh
	if result.err != nil {
		return result.err
	}
	if result.chk == nil { // no more data
		req.Reset()
		return nil
	}
	req.SwapDeferredCausets(result.chk)
	e.freeChkCh <- result.chk
	return nil
}

// Close implements the Executor interface.
func (e *ParallelNestedLoopApplyExec) Close() error {
	e.memTracker = nil
	err := e.outerExec.Close()
	if atomic.LoadUint32(&e.started) == 1 {
		close(e.exit)
		e.notifyWg.Wait()
		e.started = 0
	}

	if e.runtimeStats != nil {
		runtimeStats := newJoinRuntimeStats()
		e.ctx.GetStochaseinstein_dbars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, runtimeStats)
		if e.useCache {
			var hitRatio float64
			if e.cacheAccessCounter > 0 {
				hitRatio = float64(e.cacheHitCounter) / float64(e.cacheAccessCounter)
			}
			runtimeStats.setCacheInfo(true, hitRatio)
		} else {
			runtimeStats.setCacheInfo(false, 0)
		}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", e.concurrency))
	}
	return err
}

// notifyWorker waits for all inner/outer-workers finishing and then put an empty
// chunk into the resultCh to notify the upper executor there is no more data.
func (e *ParallelNestedLoopApplyExec) notifyWorker(ctx context.Context) {
	defer e.handleWorkerPanic(ctx, &e.notifyWg)
	e.workerWg.Wait()
	e.putResult(nil, nil)
}

func (e *ParallelNestedLoopApplyExec) outerWorker(ctx context.Context) {
	defer trace.StartRegion(ctx, "ParallelApplyOuterWorker").End()
	defer e.handleWorkerPanic(ctx, &e.workerWg)
	var selected []bool
	var err error
	for {
		chk := newFirstChunk(e.outerExec)
		if err := Next(ctx, e.outerExec, chk); err != nil {
			e.putResult(nil, err)
			return
		}
		if chk.NumEvents() == 0 {
			close(e.outerEventCh)
			return
		}
		e.outerList.Add(chk)
		outerIter := chunk.NewIterator4Chunk(chk)
		selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, selected)
		if err != nil {
			e.putResult(nil, err)
			return
		}
		for i := 0; i < chk.NumEvents(); i++ {
			event := chk.GetEvent(i)
			select {
			case e.outerEventCh <- outerEvent{&event, selected[i]}:
			case <-e.exit:
				return
			}
		}
	}
}

func (e *ParallelNestedLoopApplyExec) innerWorker(ctx context.Context, id int) {
	defer trace.StartRegion(ctx, "ParallelApplyInnerWorker").End()
	defer e.handleWorkerPanic(ctx, &e.workerWg)
	for {
		var chk *chunk.Chunk
		select {
		case chk = <-e.freeChkCh:
		case <-e.exit:
			return
		}
		err := e.fillInnerChunk(ctx, id, chk)
		if err == nil && chk.NumEvents() == 0 { // no more data, this goroutine can exit
			return
		}
		if e.putResult(chk, err) {
			return
		}
	}
}

func (e *ParallelNestedLoopApplyExec) putResult(chk *chunk.Chunk, err error) (exit bool) {
	select {
	case e.resultChkCh <- result{chk, err}:
		return false
	case <-e.exit:
		return true
	}
}

func (e *ParallelNestedLoopApplyExec) handleWorkerPanic(ctx context.Context, wg *sync.WaitGroup) {
	if r := recover(); r != nil {
		err := errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("parallel nested loop join worker panicked", zap.Error(err), zap.Stack("stack"))
		e.resultChkCh <- result{nil, err}
	}
	if wg != nil {
		wg.Done()
	}
}

// fetchAllInners reads all data from the inner block and stores them in a List.
func (e *ParallelNestedLoopApplyExec) fetchAllInners(ctx context.Context, id int) (err error) {
	var key []byte
	for _, defCaus := range e.corDefCauss[id] {
		*defCaus.Data = e.outerEvent[id].GetCauset(defCaus.Index, defCaus.RetType)
		if e.useCache {
			if key, err = codec.EncodeKey(e.ctx.GetStochaseinstein_dbars().StmtCtx, key, *defCaus.Data); err != nil {
				return err
			}
		}
	}
	if e.useCache { // look up the cache
		atomic.AddInt64(&e.cacheAccessCounter, 1)
		e.cacheLock.RLock()
		value, err := e.cache.Get(key)
		e.cacheLock.RUnlock()
		if err != nil {
			return err
		}
		if value != nil {
			e.innerList[id] = value
			atomic.AddInt64(&e.cacheHitCounter, 1)
			return nil
		}
	}

	err = e.innerExecs[id].Open(ctx)
	defer terror.Call(e.innerExecs[id].Close)
	if err != nil {
		return err
	}

	if e.useCache {
		// create a new one in this case since it may be in the cache
		e.innerList[id] = chunk.NewList(retTypes(e.innerExecs[id]), e.initCap, e.maxChunkSize)
	} else {
		e.innerList[id].Reset()
	}

	innerIter := chunk.NewIterator4Chunk(e.innerChunk[id])
	for {
		err := Next(ctx, e.innerExecs[id], e.innerChunk[id])
		if err != nil {
			return err
		}
		if e.innerChunk[id].NumEvents() == 0 {
			break
		}

		e.innerSelected[id], err = expression.VectorizedFilter(e.ctx, e.innerFilter[id], innerIter, e.innerSelected[id])
		if err != nil {
			return err
		}
		for event := innerIter.Begin(); event != innerIter.End(); event = innerIter.Next() {
			if e.innerSelected[id][event.Idx()] {
				e.innerList[id].AppendEvent(event)
			}
		}
	}

	if e.useCache { // uFIDelate the cache
		e.cacheLock.Lock()
		defer e.cacheLock.Unlock()
		if _, err := e.cache.Set(key, e.innerList[id]); err != nil {
			return err
		}
	}
	return nil
}

func (e *ParallelNestedLoopApplyExec) fetchNextOuterEvent(id int, req *chunk.Chunk) (event *chunk.Event, exit bool) {
	for {
		select {
		case outerEvent, ok := <-e.outerEventCh:
			if !ok { // no more data
				return nil, false
			}
			if !outerEvent.selected {
				if e.outer {
					e.joiners[id].onMissMatch(false, *outerEvent.event, req)
					if req.IsFull() {
						return nil, false
					}
				}
				continue // try the next outer event
			}
			return outerEvent.event, false
		case <-e.exit:
			return nil, true
		}
	}
}

func (e *ParallelNestedLoopApplyExec) fillInnerChunk(ctx context.Context, id int, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter[id] == nil || e.innerIter[id].Current() == e.innerIter[id].End() {
			if e.outerEvent[id] != nil && !e.hasMatch[id] {
				e.joiners[id].onMissMatch(e.hasNull[id], *e.outerEvent[id], req)
			}
			var exit bool
			e.outerEvent[id], exit = e.fetchNextOuterEvent(id, req)
			if exit || req.IsFull() || e.outerEvent[id] == nil {
				return nil
			}

			e.hasMatch[id] = false
			e.hasNull[id] = false

			err = e.fetchAllInners(ctx, id)
			if err != nil {
				return err
			}
			e.innerIter[id] = chunk.NewIterator4List(e.innerList[id])
			e.innerIter[id].Begin()
		}

		matched, isNull, err := e.joiners[id].tryToMatchInners(*e.outerEvent[id], e.innerIter[id], req)
		e.hasMatch[id] = e.hasMatch[id] || matched
		e.hasNull[id] = e.hasNull[id] || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}
