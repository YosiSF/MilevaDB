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
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/trace"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/distsql"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

var (
	_ Executor = &BlockReaderExecutor{}
	_ Executor = &IndexReaderExecutor{}
	_ Executor = &IndexLookUpExecutor{}
)

// LookupBlockTaskChannelSize represents the channel size of the index double read taskChan.
var LookupBlockTaskChannelSize int32 = 50

// lookupBlockTask is created from a partial result of an index request which
// contains the handles in those index keys.
type lookupBlockTask struct {
	handles []ekv.Handle
	rowIdx  []int // rowIdx represents the handle index for every event. Only used when keep order.
	rows    []chunk.Event
	idxEvents *chunk.Chunk
	cursor  int

	doneCh chan error

	// indexOrder map is used to save the original index order for the handles.
	// Without this map, the original index order might be lost.
	// The handles fetched from index is originally ordered by index, but we need handles to be ordered by itself
	// to do block request.
	indexOrder *ekv.HandleMap
	// duplicatedIndexOrder map likes indexOrder. But it's used when checHoTTexValue isn't nil and
	// the same handle of index has multiple values.
	duplicatedIndexOrder *ekv.HandleMap

	// memUsage records the memory usage of this task calculated by block worker.
	// memTracker is used to release memUsage after task is done and unused.
	//
	// The sequence of function calls are:
	//   1. calculate task.memUsage.
	//   2. task.memTracker = blockWorker.memTracker
	//   3. task.memTracker.Consume(task.memUsage)
	//   4. task.memTracker.Consume(-task.memUsage)
	//
	// Step 1~3 are completed in "blockWorker.executeTask".
	// Step 4   is  completed in "IndexLookUpExecutor.Next".
	memUsage   int64
	memTracker *memory.Tracker
}

func (task *lookupBlockTask) Len() int {
	return len(task.rows)
}

func (task *lookupBlockTask) Less(i, j int) bool {
	return task.rowIdx[i] < task.rowIdx[j]
}

func (task *lookupBlockTask) Swap(i, j int) {
	task.rowIdx[i], task.rowIdx[j] = task.rowIdx[j], task.rowIdx[i]
	task.rows[i], task.rows[j] = task.rows[j], task.rows[i]
}

// Closeable is a interface for closeable structures.
type Closeable interface {
	// Close closes the object.
	Close() error
}

// closeAll closes all objects even if an object returns an error.
// If multiple objects returns error, the first error will be returned.
func closeAll(objs ...Closeable) error {
	var err error
	for _, obj := range objs {
		if obj != nil {
			err1 := obj.Close()
			if err == nil && err1 != nil {
				err = err1
			}
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// handleIsExtra checks whether this defCausumn is a extra handle defCausumn generated during plan building phase.
func handleIsExtra(defCaus *expression.DeferredCauset) bool {
	if defCaus != nil && defCaus.ID == perceptron.ExtraHandleID {
		return true
	}
	return false
}

func splitRanges(ranges []*ranger.Range, keepOrder bool, desc bool) ([]*ranger.Range, []*ranger.Range) {
	if len(ranges) == 0 || ranges[0].LowVal[0].HoTT() == types.HoTTInt64 {
		return ranges, nil
	}
	idx := sort.Search(len(ranges), func(i int) bool { return ranges[i].HighVal[0].GetUint64() > math.MaxInt64 })
	if idx == len(ranges) {
		return ranges, nil
	}
	if ranges[idx].LowVal[0].GetUint64() > math.MaxInt64 {
		signedRanges := ranges[0:idx]
		unsignedRanges := ranges[idx:]
		if !keepOrder {
			return append(unsignedRanges, signedRanges...), nil
		}
		if desc {
			return unsignedRanges, signedRanges
		}
		return signedRanges, unsignedRanges
	}
	signedRanges := make([]*ranger.Range, 0, idx+1)
	unsignedRanges := make([]*ranger.Range, 0, len(ranges)-idx)
	signedRanges = append(signedRanges, ranges[0:idx]...)
	if !(ranges[idx].LowVal[0].GetUint64() == math.MaxInt64 && ranges[idx].LowExclude) {
		signedRanges = append(signedRanges, &ranger.Range{
			LowVal:     ranges[idx].LowVal,
			LowExclude: ranges[idx].LowExclude,
			HighVal:    []types.Causet{types.NewUintCauset(math.MaxInt64)},
		})
	}
	if !(ranges[idx].HighVal[0].GetUint64() == math.MaxInt64+1 && ranges[idx].HighExclude) {
		unsignedRanges = append(unsignedRanges, &ranger.Range{
			LowVal:      []types.Causet{types.NewUintCauset(math.MaxInt64 + 1)},
			HighVal:     ranges[idx].HighVal,
			HighExclude: ranges[idx].HighExclude,
		})
	}
	if idx < len(ranges) {
		unsignedRanges = append(unsignedRanges, ranges[idx+1:]...)
	}
	if !keepOrder {
		return append(unsignedRanges, signedRanges...), nil
	}
	if desc {
		return unsignedRanges, signedRanges
	}
	return signedRanges, unsignedRanges
}

// rebuildIndexRanges will be called if there's correlated defCausumn in access conditions. We will rebuild the range
// by substitute correlated defCausumn with the constant.
func rebuildIndexRanges(ctx stochastikctx.Context, is *plannercore.PhysicalIndexScan, idxDefCauss []*expression.DeferredCauset, defCausLens []int) (ranges []*ranger.Range, err error) {
	access := make([]expression.Expression, 0, len(is.AccessCondition))
	for _, cond := range is.AccessCondition {
		newCond, err1 := expression.SubstituteCorDefCaus2Constant(cond)
		if err1 != nil {
			return nil, err1
		}
		access = append(access, newCond)
	}
	ranges, _, err = ranger.DetachSimpleCondAndBuildRangeForIndex(ctx, access, idxDefCauss, defCausLens)
	return ranges, err
}

// IndexReaderExecutor sends posetPosetDag request and reads index data from ekv layer.
type IndexReaderExecutor struct {
	baseExecutor

	// For a partitioned block, the IndexReaderExecutor works on a partition, so
	// the type of this block field is actually `block.PhysicalBlock`.
	block           block.Block
	index           *perceptron.IndexInfo
	physicalBlockID int64
	ranges          []*ranger.Range
	// kvRanges are only used for union scan.
	kvRanges []ekv.KeyRange
	posetPosetDagPB    *fidelpb.PosetDagRequest
	startTS  uint64

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result distsql.SelectResult
	// defCausumns are only required by union scan.
	defCausumns []*perceptron.DeferredCausetInfo
	// outputDeferredCausets are only required by union scan.
	outputDeferredCausets []*expression.DeferredCauset

	feedback  *statistics.QueryFeedback
	streaming bool

	keepOrder bool
	desc      bool

	corDefCausInFilter bool
	corDefCausInAccess bool
	idxDefCauss        []*expression.DeferredCauset
	defCausLens        []int
	plans          []plannercore.PhysicalPlan

	memTracker *memory.Tracker

	selectResultHook // for testing
}

// Close clears all resources hold by current object.
func (e *IndexReaderExecutor) Close() error {
	err := e.result.Close()
	e.result = nil
	e.ctx.StoreQueryFeedback(e.feedback)
	return err
}

// Next implements the Executor Next interface.
func (e *IndexReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	err := e.result.Next(ctx, req)
	if err != nil {
		e.feedback.Invalidate()
	}
	return err
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open(ctx context.Context) error {
	var err error
	if e.corDefCausInAccess {
		e.ranges, err = rebuildIndexRanges(e.ctx, e.plans[0].(*plannercore.PhysicalIndexScan), e.idxDefCauss, e.defCausLens)
		if err != nil {
			return err
		}
	}
	kvRanges, err := distsql.IndexRangesToKVRanges(e.ctx.GetStochastikVars().StmtCtx, e.physicalBlockID, e.index.ID, e.ranges, e.feedback)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	return e.open(ctx, kvRanges)
}

func (e *IndexReaderExecutor) open(ctx context.Context, kvRanges []ekv.KeyRange) error {
	var err error
	if e.corDefCausInFilter {
		e.posetPosetDagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)
		if err != nil {
			return err
		}
	}

	if e.runtimeStats != nil {
		defCauslExec := true
		e.posetPosetDagPB.DefCauslectExecutionSummaries = &defCauslExec
	}
	e.kvRanges = kvRanges

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetPosetDagRequest(e.posetPosetDagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromStochastikVars(e.ctx.GetStochastikVars()).
		SetMemTracker(e.memTracker).
		Build()
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	e.result, err = e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	e.result.Fetch(ctx)
	return nil
}

// IndexLookUpExecutor implements double read for index scan.
type IndexLookUpExecutor struct {
	baseExecutor

	block   block.Block
	index   *perceptron.IndexInfo
	ranges  []*ranger.Range
	posetPosetDagPB   *fidelpb.PosetDagRequest
	startTS uint64
	// handleIdx is the index of handle, which is only used for case of keeping order.
	handleIdx       []int
	handleDefCauss      []*expression.DeferredCauset
	primaryKeyIndex *perceptron.IndexInfo
	blockRequest    *fidelpb.PosetDagRequest
	// defCausumns are only required by union scan.
	defCausumns []*perceptron.DeferredCausetInfo
	*dataReaderBuilder
	// All fields above are immublock.

	idxWorkerWg sync.WaitGroup
	tblWorkerWg sync.WaitGroup
	finished    chan struct{}

	resultCh   chan *lookupBlockTask
	resultCurr *lookupBlockTask
	feedback   *statistics.QueryFeedback

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// checHoTTexValue is used to check the consistency of the index data.
	*checHoTTexValue

	kvRanges      []ekv.KeyRange
	workerStarted bool

	keepOrder bool
	desc      bool

	indexStreaming bool
	blockStreaming bool

	corDefCausInIdxSide bool
	corDefCausInTblSide bool
	corDefCausInAccess  bool
	idxPlans        []plannercore.PhysicalPlan
	tblPlans        []plannercore.PhysicalPlan
	idxDefCauss         []*expression.DeferredCauset
	defCausLens         []int
	// PushedLimit is used to skip the preceding and tailing handles when Limit is sunk into IndexLookUpReader.
	PushedLimit *plannercore.PushedDownLimit
}

type getHandleType int8

const (
	getHandleFromIndex getHandleType = iota
	getHandleFromBlock
)

type checHoTTexValue struct {
	idxDefCausTps  []*types.FieldType
	idxTblDefCauss []*block.DeferredCauset
}

// Open implements the Executor Open interface.
func (e *IndexLookUpExecutor) Open(ctx context.Context) error {
	var err error
	if e.corDefCausInAccess {
		e.ranges, err = rebuildIndexRanges(e.ctx, e.idxPlans[0].(*plannercore.PhysicalIndexScan), e.idxDefCauss, e.defCausLens)
		if err != nil {
			return err
		}
	}
	sc := e.ctx.GetStochastikVars().StmtCtx
	physicalID := getPhysicalBlockID(e.block)
	if e.index.ID == -1 {
		e.kvRanges, err = distsql.CommonHandleRangesToKVRanges(sc, physicalID, e.ranges)
	} else {
		e.kvRanges, err = distsql.IndexRangesToKVRanges(sc, physicalID, e.index.ID, e.ranges, e.feedback)
	}
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	err = e.open(ctx)
	if err != nil {
		e.feedback.Invalidate()
	}
	return err
}

func (e *IndexLookUpExecutor) open(ctx context.Context) error {
	// We have to initialize "memTracker" and other execution resources in here
	// instead of in function "Open", because this "IndexLookUpExecutor" may be
	// constructed by a "IndexLookUpJoin" and "Open" will not be called in that
	// situation.
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	e.finished = make(chan struct{})
	e.resultCh = make(chan *lookupBlockTask, atomic.LoadInt32(&LookupBlockTaskChannelSize))

	var err error
	if e.corDefCausInIdxSide {
		e.posetPosetDagPB.Executors, _, err = constructDistExec(e.ctx, e.idxPlans)
		if err != nil {
			return err
		}
	}

	if e.corDefCausInTblSide {
		e.blockRequest.Executors, _, err = constructDistExec(e.ctx, e.tblPlans)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *IndexLookUpExecutor) startWorkers(ctx context.Context, initBatchSize int) error {
	// indexWorker will write to workCh and blockWorker will read from workCh,
	// so fetching index and getting block data can run concurrently.
	workCh := make(chan *lookupBlockTask, 1)
	if err := e.startIndexWorker(ctx, e.kvRanges, workCh, initBatchSize); err != nil {
		return err
	}
	e.startBlockWorker(ctx, workCh)
	e.workerStarted = true
	return nil
}

func (e *IndexLookUpExecutor) isCommonHandle() bool {
	return !(len(e.handleDefCauss) == 1 && e.handleDefCauss[0].ID == perceptron.ExtraHandleID) && e.block.Meta() != nil && e.block.Meta().IsCommonHandle
}

func (e *IndexLookUpExecutor) getRetTpsByHandle() []*types.FieldType {
	var tps []*types.FieldType
	if e.isCommonHandle() {
		for _, handleDefCaus := range e.handleDefCauss {
			tps = append(tps, handleDefCaus.RetType)
		}
	} else {
		tps = []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	}
	if e.checHoTTexValue != nil {
		tps = e.idxDefCausTps
	}
	return tps
}

// startIndexWorker launch a background goroutine to fetch handles, send the results to workCh.
func (e *IndexLookUpExecutor) startIndexWorker(ctx context.Context, kvRanges []ekv.KeyRange, workCh chan<- *lookupBlockTask, initBatchSize int) error {
	if e.runtimeStats != nil {
		defCauslExec := true
		e.posetPosetDagPB.DefCauslectExecutionSummaries = &defCauslExec
	}

	tracker := memory.NewTracker(memory.LabelForIndexWorker, -1)
	tracker.AttachTo(e.memTracker)
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetPosetDagRequest(e.posetPosetDagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.indexStreaming).
		SetFromStochastikVars(e.ctx.GetStochastikVars()).
		SetMemTracker(tracker).
		Build()
	if err != nil {
		return err
	}
	tps := e.getRetTpsByHandle()
	// Since the first read only need handle information. So its returned defCaus is only 1.
	result, err := distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, tps, e.feedback, getPhysicalPlanIDs(e.idxPlans), e.id)
	if err != nil {
		return err
	}
	result.Fetch(ctx)
	worker := &indexWorker{
		idxLookup:       e,
		workCh:          workCh,
		finished:        e.finished,
		resultCh:        e.resultCh,
		keepOrder:       e.keepOrder,
		batchSize:       initBatchSize,
		checHoTTexValue: e.checHoTTexValue,
		maxBatchSize:    e.ctx.GetStochastikVars().IndexLookupSize,
		maxChunkSize:    e.maxChunkSize,
		PushedLimit:     e.PushedLimit,
	}
	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}
	e.idxWorkerWg.Add(1)
	go func() {
		defer trace.StartRegion(ctx, "IndexLookUpIndexWorker").End()
		ctx1, cancel := context.WithCancel(ctx)
		_, err := worker.fetchHandles(ctx1, result)
		if err != nil {
			e.feedback.Invalidate()
		}
		cancel()
		if err := result.Close(); err != nil {
			logutil.Logger(ctx).Error("close Select result failed", zap.Error(err))
		}
		e.ctx.StoreQueryFeedback(e.feedback)
		close(workCh)
		close(e.resultCh)
		e.idxWorkerWg.Done()
	}()
	return nil
}

// startBlockWorker launchs some background goroutines which pick tasks from workCh and execute the task.
func (e *IndexLookUpExecutor) startBlockWorker(ctx context.Context, workCh <-chan *lookupBlockTask) {
	lookupConcurrencyLimit := e.ctx.GetStochastikVars().IndexLookupConcurrency()
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		workerID := i
		worker := &blockWorker{
			idxLookup:       e,
			workCh:          workCh,
			finished:        e.finished,
			buildTblReader:  e.buildBlockReader,
			keepOrder:       e.keepOrder,
			handleIdx:       e.handleIdx,
			checHoTTexValue: e.checHoTTexValue,
			memTracker:      memory.NewTracker(workerID, -1),
		}
		worker.memTracker.AttachTo(e.memTracker)
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			defer trace.StartRegion(ctx1, "IndexLookUpBlockWorker").End()
			worker.pickAndExecTask(ctx1)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

func (e *IndexLookUpExecutor) buildBlockReader(ctx context.Context, handles []ekv.Handle) (Executor, error) {
	blockReaderExec := &BlockReaderExecutor{
		baseExecutor:   newBaseExecutor(e.ctx, e.schemaReplicant, 0),
		block:          e.block,
		posetPosetDagPB:          e.blockRequest,
		startTS:        e.startTS,
		defCausumns:        e.defCausumns,
		streaming:      e.blockStreaming,
		feedback:       statistics.NewQueryFeedback(0, nil, 0, false),
		corDefCausInFilter: e.corDefCausInTblSide,
		plans:          e.tblPlans,
	}
	blockReaderExec.buildVirtualDeferredCausetInfo()
	blockReader, err := e.dataReaderBuilder.buildBlockReaderFromHandles(ctx, blockReaderExec, handles)
	if err != nil {
		logutil.Logger(ctx).Error("build block reader from handles failed", zap.Error(err))
		return nil, err
	}
	return blockReader, nil
}

// Close implements Exec Close interface.
func (e *IndexLookUpExecutor) Close() error {
	if !e.workerStarted || e.finished == nil {
		return nil
	}

	close(e.finished)
	// Drain the resultCh and discard the result, in case that Next() doesn't fully
	// consume the data, background worker still writing to resultCh and block forever.
	for range e.resultCh {
	}
	e.idxWorkerWg.Wait()
	e.tblWorkerWg.Wait()
	e.finished = nil
	e.workerStarted = false
	e.memTracker = nil
	e.resultCurr = nil
	return nil
}

// Next implements Exec Next interface.
func (e *IndexLookUpExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.workerStarted {
		if err := e.startWorkers(ctx, req.RequiredEvents()); err != nil {
			return err
		}
	}
	req.Reset()
	for {
		resultTask, err := e.getResultTask()
		if err != nil {
			return err
		}
		if resultTask == nil {
			return nil
		}
		for resultTask.cursor < len(resultTask.rows) {
			req.AppendEvent(resultTask.rows[resultTask.cursor])
			resultTask.cursor++
			if req.IsFull() {
				return nil
			}
		}
	}
}

func (e *IndexLookUpExecutor) getResultTask() (*lookupBlockTask, error) {
	if e.resultCurr != nil && e.resultCurr.cursor < len(e.resultCurr.rows) {
		return e.resultCurr, nil
	}
	task, ok := <-e.resultCh
	if !ok {
		return nil, nil
	}
	if err := <-task.doneCh; err != nil {
		return nil, err
	}

	// Release the memory usage of last task before we handle a new task.
	if e.resultCurr != nil {
		e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

// indexWorker is used by IndexLookUpExecutor to maintain index lookup background goroutines.
type indexWorker struct {
	idxLookup *IndexLookUpExecutor
	workCh    chan<- *lookupBlockTask
	finished  <-chan struct{}
	resultCh  chan<- *lookupBlockTask
	keepOrder bool

	// batchSize is for lightweight startup. It will be increased exponentially until reaches the max batch size value.
	batchSize    int
	maxBatchSize int
	maxChunkSize int

	// checHoTTexValue is used to check the consistency of the index data.
	*checHoTTexValue
	// PushedLimit is used to skip the preceding and tailing handles when Limit is sunk into IndexLookUpReader.
	PushedLimit *plannercore.PushedDownLimit
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are sent to workCh to be further processed by blockWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (w *indexWorker) fetchHandles(ctx context.Context, result distsql.SelectResult) (count uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("indexWorker in IndexLookupExecutor panicked", zap.String("stack", string(buf)))
			err4Panic := errors.Errorf("%v", r)
			doneCh := make(chan error, 1)
			doneCh <- err4Panic
			w.resultCh <- &lookupBlockTask{
				doneCh: doneCh,
			}
			if err != nil {
				err = errors.Trace(err4Panic)
			}
		}
	}()
	retTps := w.idxLookup.getRetTpsByHandle()
	chk := chunk.NewChunkWithCapacity(retTps, w.idxLookup.maxChunkSize)
	for {
		handles, retChunk, scannedKeys, err := w.extractTaskHandles(ctx, chk, result, count)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- err
			w.resultCh <- &lookupBlockTask{
				doneCh: doneCh,
			}
			return count, err
		}
		count += scannedKeys
		if len(handles) == 0 {
			return count, nil
		}
		task := w.buildBlockTask(handles, retChunk)
		select {
		case <-ctx.Done():
			return count, nil
		case <-w.finished:
			return count, nil
		case w.workCh <- task:
			w.resultCh <- task
		}
	}
}

func (w *indexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult, count uint64) (
	handles []ekv.Handle, retChk *chunk.Chunk, scannedKeys uint64, err error) {
	var handleOffset []int
	for i := range w.idxLookup.handleDefCauss {
		handleOffset = append(handleOffset, chk.NumDefCauss()-len(w.idxLookup.handleDefCauss)+i)
	}
	if len(handleOffset) == 0 {
		handleOffset = []int{chk.NumDefCauss() - 1}
	}
	handles = make([]ekv.Handle, 0, w.batchSize)
	// PushedLimit would always be nil for ChecHoTTex or CheckBlock, we add this check just for insurance.
	checkLimit := (w.PushedLimit != nil) && (w.checHoTTexValue == nil)
	for len(handles) < w.batchSize {
		requiredEvents := w.batchSize - len(handles)
		if checkLimit {
			if w.PushedLimit.Offset+w.PushedLimit.Count <= scannedKeys+count {
				return handles, nil, scannedKeys, nil
			}
			leftCnt := w.PushedLimit.Offset + w.PushedLimit.Count - scannedKeys - count
			if uint64(requiredEvents) > leftCnt {
				requiredEvents = int(leftCnt)
			}
		}
		chk.SetRequiredEvents(requiredEvents, w.maxChunkSize)
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return handles, nil, scannedKeys, err
		}
		if chk.NumEvents() == 0 {
			return handles, retChk, scannedKeys, nil
		}
		for i := 0; i < chk.NumEvents(); i++ {
			scannedKeys++
			if checkLimit {
				if (count + scannedKeys) <= w.PushedLimit.Offset {
					// Skip the preceding Offset handles.
					continue
				}
				if (count + scannedKeys) > (w.PushedLimit.Offset + w.PushedLimit.Count) {
					// Skip the handles after Offset+Count.
					return handles, nil, scannedKeys, nil
				}
			}
			h, err := w.idxLookup.getHandle(chk.GetEvent(i), handleOffset, w.idxLookup.isCommonHandle(), getHandleFromIndex)
			if err != nil {
				return handles, retChk, scannedKeys, err
			}
			handles = append(handles, h)
		}
		if w.checHoTTexValue != nil {
			if retChk == nil {
				retChk = chunk.NewChunkWithCapacity(w.idxDefCausTps, w.batchSize)
			}
			retChk.Append(chk, 0, chk.NumEvents())
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, scannedKeys, nil
}

func (w *indexWorker) buildBlockTask(handles []ekv.Handle, retChk *chunk.Chunk) *lookupBlockTask {
	var indexOrder *ekv.HandleMap
	var duplicatedIndexOrder *ekv.HandleMap
	if w.keepOrder {
		// Save the index order.
		indexOrder = ekv.NewHandleMap()
		for i, h := range handles {
			indexOrder.Set(h, i)
		}
	}

	if w.checHoTTexValue != nil {
		// Save the index order.
		indexOrder = ekv.NewHandleMap()
		duplicatedIndexOrder = ekv.NewHandleMap()
		for i, h := range handles {
			if _, ok := indexOrder.Get(h); ok {
				duplicatedIndexOrder.Set(h, i)
			} else {
				indexOrder.Set(h, i)
			}
		}
	}

	task := &lookupBlockTask{
		handles:              handles,
		indexOrder:           indexOrder,
		duplicatedIndexOrder: duplicatedIndexOrder,
		idxEvents:              retChk,
	}

	task.doneCh = make(chan error, 1)
	return task
}

// blockWorker is used by IndexLookUpExecutor to maintain block lookup background goroutines.
type blockWorker struct {
	idxLookup      *IndexLookUpExecutor
	workCh         <-chan *lookupBlockTask
	finished       <-chan struct{}
	buildTblReader func(ctx context.Context, handles []ekv.Handle) (Executor, error)
	keepOrder      bool
	handleIdx      []int

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// checHoTTexValue is used to check the consistency of the index data.
	*checHoTTexValue
}

// pickAndExecTask picks tasks from workCh, and execute them.
func (w *blockWorker) pickAndExecTask(ctx context.Context) {
	var task *lookupBlockTask
	var ok bool
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("blockWorker in IndexLookUpExecutor panicked", zap.String("stack", string(buf)))
			task.doneCh <- errors.Errorf("%v", r)
		}
	}()
	for {
		// Don't check ctx.Done() on purpose. If background worker get the signal and all
		// exit immediately, stochastik's goroutine doesn't know this and still calling Next(),
		// it may block reading task.doneCh forever.
		select {
		case task, ok = <-w.workCh:
			if !ok {
				return
			}
		case <-w.finished:
			return
		}
		err := w.executeTask(ctx, task)
		task.doneCh <- err
	}
}

func (e *IndexLookUpExecutor) getHandle(event chunk.Event, handleIdx []int,
	isCommonHandle bool, tp getHandleType) (handle ekv.Handle, err error) {
	if isCommonHandle {
		var handleEncoded []byte
		var datums []types.Causet
		for i, idx := range handleIdx {
			// If the new defCauslation is enabled and the handle contains non-binary string,
			// the handle in the index is encoded as "sortKey". So we cannot restore its
			// original value(the primary key) here.
			// We use a trick to avoid encoding the "sortKey" again by changing the charset
			// defCauslation to `binary`.
			// TODO: Add the restore value to the secondary index to remove this trick.
			rtp := e.handleDefCauss[i].RetType
			if defCauslate.NewDefCauslationEnabled() && rtp.EvalType() == types.ETString &&
				!allegrosql.HasBinaryFlag(rtp.Flag) && tp == getHandleFromIndex {
				rtp = rtp.Clone()
				rtp.DefCauslate = charset.DefCauslationBin
				datums = append(datums, event.GetCauset(idx, rtp))
				continue
			}
			datums = append(datums, event.GetCauset(idx, e.handleDefCauss[i].RetType))
		}
		if tp == getHandleFromBlock {
			blockcodec.TruncateIndexValues(e.block.Meta(), e.primaryKeyIndex, datums)
		}
		handleEncoded, err = codec.EncodeKey(e.ctx.GetStochastikVars().StmtCtx, nil, datums...)
		if err != nil {
			return nil, err
		}
		handle, err = ekv.NewCommonHandle(handleEncoded)
		if err != nil {
			return nil, err
		}
	} else {
		if len(handleIdx) == 0 {
			handle = ekv.IntHandle(event.GetInt64(0))
		} else {
			handle = ekv.IntHandle(event.GetInt64(handleIdx[0]))
		}
	}
	return
}

func (w *blockWorker) compareData(ctx context.Context, task *lookupBlockTask, blockReader Executor) error {
	chk := newFirstChunk(blockReader)
	tblInfo := w.idxLookup.block.Meta()
	vals := make([]types.Causet, 0, len(w.idxTblDefCauss))
	for {
		err := Next(ctx, blockReader, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumEvents() == 0 {
			task.indexOrder.Range(func(h ekv.Handle, val interface{}) bool {
				idxEvent := task.idxEvents.GetEvent(val.(int))
				err = errors.Errorf("handle %#v, index:%#v != record:%#v", h, idxEvent.GetCauset(0, w.idxDefCausTps[0]), nil)
				return false
			})
			if err != nil {
				return err
			}
			break
		}

		iter := chunk.NewIterator4Chunk(chk)
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			handle, err := w.idxLookup.getHandle(event, w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromBlock)
			if err != nil {
				return err
			}
			v, ok := task.indexOrder.Get(handle)
			if !ok {
				v, _ = task.duplicatedIndexOrder.Get(handle)
			}
			offset, _ := v.(int)
			task.indexOrder.Delete(handle)
			idxEvent := task.idxEvents.GetEvent(offset)
			vals = vals[:0]
			for i, defCaus := range w.idxTblDefCauss {
				vals = append(vals, event.GetCauset(i, &defCaus.FieldType))
			}
			blockcodec.TruncateIndexValues(tblInfo, w.idxLookup.index, vals)
			for i, val := range vals {
				defCaus := w.idxTblDefCauss[i]
				tp := &defCaus.FieldType
				ret := chunk.Compare(idxEvent, i, &val)
				if ret != 0 {
					return errors.Errorf("defCaus %s, handle %#v, index:%#v != record:%#v", defCaus.Name, handle, idxEvent.GetCauset(i, tp), val)
				}
			}
		}
	}

	return nil
}

// executeTask executes the block look up tasks. We will construct a block reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (w *blockWorker) executeTask(ctx context.Context, task *lookupBlockTask) error {
	blockReader, err := w.buildTblReader(ctx, task.handles)
	if err != nil {
		logutil.Logger(ctx).Error("build block reader failed", zap.Error(err))
		return err
	}
	defer terror.Call(blockReader.Close)

	if w.checHoTTexValue != nil {
		return w.compareData(ctx, task, blockReader)
	}

	task.memTracker = w.memTracker
	memUsage := int64(cap(task.handles) * 8)
	task.memUsage = memUsage
	task.memTracker.Consume(memUsage)
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Event, 0, handleCnt)
	for {
		chk := newFirstChunk(blockReader)
		err = Next(ctx, blockReader, chk)
		if err != nil {
			logutil.Logger(ctx).Error("block reader fetch next chunk failed", zap.Error(err))
			return err
		}
		if chk.NumEvents() == 0 {
			break
		}
		memUsage = chk.MemoryUsage()
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		iter := chunk.NewIterator4Chunk(chk)
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			task.rows = append(task.rows, event)
		}
	}

	defer trace.StartRegion(ctx, "IndexLookUpBlockCompute").End()
	memUsage = int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Event{}))
	task.memUsage += memUsage
	task.memTracker.Consume(memUsage)
	if w.keepOrder {
		task.rowIdx = make([]int, 0, len(task.rows))
		for i := range task.rows {
			handle, err := w.idxLookup.getHandle(task.rows[i], w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromBlock)
			if err != nil {
				return err
			}
			rowIdx, _ := task.indexOrder.Get(handle)
			task.rowIdx = append(task.rowIdx, rowIdx.(int))
		}
		memUsage = int64(cap(task.rowIdx) * 4)
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		sort.Sort(task)
	}

	if handleCnt != len(task.rows) && !soliton.HasCancelled(ctx) {
		if len(w.idxLookup.tblPlans) == 1 {
			obtainedHandlesMap := ekv.NewHandleMap()
			for _, event := range task.rows {
				handle, err := w.idxLookup.getHandle(event, w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromBlock)
				if err != nil {
					return err
				}
				obtainedHandlesMap.Set(handle, true)
			}

			logutil.Logger(ctx).Error("inconsistent index handles", zap.String("index", w.idxLookup.index.Name.O),
				zap.Int("index_cnt", handleCnt), zap.Int("block_cnt", len(task.rows)),
				zap.String("missing_handles", fmt.Sprint(GetLackHandles(task.handles, obtainedHandlesMap))),
				zap.String("total_handles", fmt.Sprint(task.handles)))

			// block scan in double read can never has conditions according to convertToIndexScan.
			// if this block scan has no condition, the number of rows it returns must equal to the length of handles.
			return errors.Errorf("inconsistent index %s handle count %d isn't equal to value count %d",
				w.idxLookup.index.Name.O, handleCnt, len(task.rows))
		}
	}

	return nil
}

// GetLackHandles gets the handles in expectedHandles but not in obtainedHandlesMap.
func GetLackHandles(expectedHandles []ekv.Handle, obtainedHandlesMap *ekv.HandleMap) []ekv.Handle {
	diffCnt := len(expectedHandles) - obtainedHandlesMap.Len()
	diffHandles := make([]ekv.Handle, 0, diffCnt)
	var cnt int
	for _, handle := range expectedHandles {
		isExist := false
		if _, ok := obtainedHandlesMap.Get(handle); ok {
			obtainedHandlesMap.Delete(handle)
			isExist = true
		}
		if !isExist {
			diffHandles = append(diffHandles, handle)
			cnt++
			if cnt == diffCnt {
				break
			}
		}
	}

	return diffHandles
}

func getPhysicalPlanIDs(plans []plannercore.PhysicalPlan) []int {
	planIDs := make([]int, 0, len(plans))
	for _, p := range plans {
		planIDs = append(planIDs, p.ID())
	}
	return planIDs
}
