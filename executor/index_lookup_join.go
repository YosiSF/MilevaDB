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
	"bytes"
	"context"
	"runtime"
	"runtime/trace"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mvmap"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/ranger"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

var _ Executor = &IndexLookUpJoin{}

// IndexLookUpJoin employs one outer worker and N innerWorkers to execute concurrently.
// It preserves the order of the outer block and support batch lookup.
//
// The execution flow is very similar to IndexLookUpReader:
// 1. outerWorker read N outer rows, build a task and send it to result channel and inner worker channel.
// 2. The innerWorker receives the task, builds key ranges from outer rows and fetch inner rows, builds inner event hash map.
// 3. main thread receives the task, waits for inner worker finish handling the task.
// 4. main thread join each outer event by look up the inner rows hash map in the task.
type IndexLookUpJoin struct {
	baseExecutor

	resultCh   <-chan *lookUpJoinTask
	cancelFunc context.CancelFunc
	workerWg   *sync.WaitGroup

	outerCtx outerCtx
	innerCtx innerCtx

	task       *lookUpJoinTask
	joinResult *chunk.Chunk
	innerIter  chunk.Iterator

	joiner      joiner
	isOuterJoin bool

	requiredEvents int64

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
	innerPtrBytes [][]byte

	// lastDefCausHelper causetstore the information for last defCaus if there's complicated filter like defCaus > x_defCaus and defCaus < x_defCaus + 100.
	lastDefCausHelper *plannercore.DefCausWithCmpFuncManager

	memTracker *memory.Tracker // track memory usage.

	stats *indexLookUpJoinRuntimeStats
}

type outerCtx struct {
	rowTypes    []*types.FieldType
	keyDefCauss []int
	filter      expression.CNFExprs
}

type innerCtx struct {
	readerBuilder    *dataReaderBuilder
	rowTypes         []*types.FieldType
	keyDefCauss      []int
	defCausLens      []int
	hasPrefixDefCaus bool
}

type lookUpJoinTask struct {
	outerResult *chunk.List
	outerMatch  [][]bool

	innerResult       *chunk.List
	encodedLookUpKeys []*chunk.Chunk
	lookupMap         *mvmap.MVMap
	matchedInners     []chunk.Event

	doneCh   chan error
	cursor   chunk.EventPtr
	hasMatch bool
	hasNull  bool

	memTracker *memory.Tracker // track memory usage.
}

type outerWorker struct {
	outerCtx

	lookup *IndexLookUpJoin

	ctx      stochastikctx.Context
	executor Executor

	maxBatchSize int
	batchSize    int

	resultCh chan<- *lookUpJoinTask
	innerCh  chan<- *lookUpJoinTask

	parentMemTracker *memory.Tracker
}

type innerWorker struct {
	innerCtx

	taskCh      <-chan *lookUpJoinTask
	outerCtx    outerCtx
	ctx         stochastikctx.Context
	executorChk *chunk.Chunk

	indexRanges               []*ranger.Range
	nextDefCausCompareFilters *plannercore.DefCausWithCmpFuncManager
	keyOff2IdxOff             []int
	stats                     *innerWorkerRuntimeStats
}

// Open implements the Executor interface.
func (e *IndexLookUpJoin) Open(ctx context.Context) error {
	// Be careful, very dirty replog in this line!!!
	// IndexLookUpJoin need to rebuild executor (the dataReaderBuilder) during
	// executing. However `executor.Next()` is lazy evaluation when the RecordSet
	// result is drained.
	// Lazy evaluation means the saved stochastik context may change during executor's
	// building and its running.
	// A specific sequence for example:
	//
	// e := buildExecutor()   // txn at build time
	// recordSet := runStmt(e)
	// stochastik.CommitTxn()    // txn closed
	// recordSet.Next()
	// e.dataReaderBuilder.Build() // txn is used again, which is already closed
	//
	// The trick here is `getSnapshotTS` will cache snapshot ts in the dataReaderBuilder,
	// so even txn is destroyed later, the dataReaderBuilder could still use the
	// cached snapshot ts to construct PosetDag.
	_, err := e.innerCtx.readerBuilder.getSnapshotTS()
	if err != nil {
		return err
	}

	err = e.children[0].Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker)
	e.innerPtrBytes = make([][]byte, 0, 8)
	if e.runtimeStats != nil {
		e.stats = &indexLookUpJoinRuntimeStats{}
		e.ctx.GetStochaseinstein_dbars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, e.stats)
	}
	e.startWorkers(ctx)
	return nil
}

func (e *IndexLookUpJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.GetStochaseinstein_dbars().IndexLookupJoinConcurrency()
	if e.stats != nil {
		e.stats.concurrency = concurrency
	}
	resultCh := make(chan *lookUpJoinTask, concurrency)
	e.resultCh = resultCh
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpJoinTask, concurrency)
	e.workerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh).run(workerCtx, e.workerWg)
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.newInnerWorker(innerCh).run(workerCtx, e.workerWg)
	}
}

func (e *IndexLookUpJoin) newOuterWorker(resultCh, innerCh chan *lookUpJoinTask) *outerWorker {
	ow := &outerWorker{
		outerCtx:         e.outerCtx,
		ctx:              e.ctx,
		executor:         e.children[0],
		resultCh:         resultCh,
		innerCh:          innerCh,
		batchSize:        32,
		maxBatchSize:     e.ctx.GetStochaseinstein_dbars().IndexJoinBatchSize,
		parentMemTracker: e.memTracker,
		lookup:           e,
	}
	return ow
}

func (e *IndexLookUpJoin) newInnerWorker(taskCh chan *lookUpJoinTask) *innerWorker {
	// Since multiple inner workers run concurrently, we should INTERLOCKy join's indexRanges for every worker to avoid data race.
	INTERLOCKiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		INTERLOCKiedRanges = append(INTERLOCKiedRanges, ran.Clone())
	}

	var innerStats *innerWorkerRuntimeStats
	if e.stats != nil {
		innerStats = &e.stats.innerWorker
	}
	iw := &innerWorker{
		innerCtx:      e.innerCtx,
		outerCtx:      e.outerCtx,
		taskCh:        taskCh,
		ctx:           e.ctx,
		executorChk:   chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
		indexRanges:   INTERLOCKiedRanges,
		keyOff2IdxOff: e.keyOff2IdxOff,
		stats:         innerStats,
	}
	if e.lastDefCausHelper != nil {
		// nextCwf.TmpCouplingConstantWithRadix needs to be reset for every individual
		// inner worker to avoid data race when the inner workers is running
		// concurrently.
		nextCwf := *e.lastDefCausHelper
		nextCwf.TmpCouplingConstantWithRadix = make([]*expression.CouplingConstantWithRadix, len(e.lastDefCausHelper.TmpCouplingConstantWithRadix))
		for i := range e.lastDefCausHelper.TmpCouplingConstantWithRadix {
			nextCwf.TmpCouplingConstantWithRadix[i] = &expression.CouplingConstantWithRadix{RetType: nextCwf.TargetDefCaus.RetType}
		}
		iw.nextDefCausCompareFilters = &nextCwf
	}
	return iw
}

// Next implements the Executor interface.
func (e *IndexLookUpJoin) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredEvents, int64(req.RequiredEvents()))
	}
	req.Reset()
	e.joinResult.Reset()
	for {
		task, err := e.getFinishedTask(ctx)
		if err != nil {
			return err
		}
		if task == nil {
			return nil
		}
		startTime := time.Now()
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			e.lookUpMatchedInners(task, task.cursor)
			e.innerIter = chunk.NewIterator4Slice(task.matchedInners)
			e.innerIter.Begin()
		}

		outerEvent := task.outerResult.GetEvent(task.cursor)
		if e.innerIter.Current() != e.innerIter.End() {
			matched, isNull, err := e.joiner.tryToMatchInners(outerEvent, e.innerIter, req)
			if err != nil {
				return err
			}
			task.hasMatch = task.hasMatch || matched
			task.hasNull = task.hasNull || isNull
		}
		if e.innerIter.Current() == e.innerIter.End() {
			if !task.hasMatch {
				e.joiner.onMissMatch(task.hasNull, outerEvent, req)
			}
			task.cursor.EventIdx++
			if int(task.cursor.EventIdx) == task.outerResult.GetChunk(int(task.cursor.ChkIdx)).NumEvents() {
				task.cursor.ChkIdx++
				task.cursor.EventIdx = 0
			}
			task.hasMatch = false
			task.hasNull = false
		}
		if e.stats != nil {
			atomic.AddInt64(&e.stats.probe, int64(time.Since(startTime)))
		}
		if req.IsFull() {
			return nil
		}
	}
}

func (e *IndexLookUpJoin) getFinishedTask(ctx context.Context) (*lookUpJoinTask, error) {
	task := e.task
	if task != nil && int(task.cursor.ChkIdx) < task.outerResult.NumChunks() {
		return task, nil
	}

	select {
	case task = <-e.resultCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if task == nil {
		return nil, nil
	}

	select {
	case err := <-task.doneCh:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	e.task = task
	return task, nil
}

func (e *IndexLookUpJoin) lookUpMatchedInners(task *lookUpJoinTask, rowPtr chunk.EventPtr) {
	outerKey := task.encodedLookUpKeys[rowPtr.ChkIdx].GetEvent(int(rowPtr.EventIdx)).GetBytes(0)
	e.innerPtrBytes = task.lookupMap.Get(outerKey, e.innerPtrBytes[:0])
	task.matchedInners = task.matchedInners[:0]

	for _, b := range e.innerPtrBytes {
		ptr := *(*chunk.EventPtr)(unsafe.Pointer(&b[0]))
		matchedInner := task.innerResult.GetEvent(ptr)
		task.matchedInners = append(task.matchedInners, matchedInner)
	}
}

func (ow *outerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer trace.StartRegion(ctx, "IndexLookupJoinOuterWorker").End()
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("outerWorker panicked", zap.String("stack", string(buf)))
			task := &lookUpJoinTask{doneCh: make(chan error, 1)}
			task.doneCh <- errors.Errorf("%v", r)
			ow.pushToChan(ctx, task, ow.resultCh)
		}
		close(ow.resultCh)
		close(ow.innerCh)
		wg.Done()
	}()
	for {
		task, err := ow.buildTask(ctx)
		if err != nil {
			task.doneCh <- err
			ow.pushToChan(ctx, task, ow.resultCh)
			return
		}
		if task == nil {
			return
		}

		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			return
		}

		if finished := ow.pushToChan(ctx, task, ow.resultCh); finished {
			return
		}
	}
}

func (ow *outerWorker) pushToChan(ctx context.Context, task *lookUpJoinTask, dst chan<- *lookUpJoinTask) bool {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

// buildTask builds a lookUpJoinTask and read outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task.
func (ow *outerWorker) buildTask(ctx context.Context) (*lookUpJoinTask, error) {
	task := &lookUpJoinTask{
		doneCh:      make(chan error, 1),
		outerResult: newList(ow.executor),
		lookupMap:   mvmap.NewMVMap(),
	}
	task.memTracker = memory.NewTracker(-1, -1)
	task.outerResult.GetMemTracker().AttachTo(task.memTracker)
	task.memTracker.AttachTo(ow.parentMemTracker)

	ow.increaseBatchSize()
	requiredEvents := ow.batchSize
	if ow.lookup.isOuterJoin {
		// If it is outerJoin, push the requiredEvents down.
		// Note: buildTask is triggered when `Open` is called, but
		// ow.lookup.requiredEvents is set when `Next` is called. Thus we check
		// whether it's 0 here.
		if parentRequired := int(atomic.LoadInt64(&ow.lookup.requiredEvents)); parentRequired != 0 {
			requiredEvents = parentRequired
		}
	}
	maxChunkSize := ow.ctx.GetStochaseinstein_dbars().MaxChunkSize
	for requiredEvents > task.outerResult.Len() {
		chk := chunk.NewChunkWithCapacity(ow.outerCtx.rowTypes, maxChunkSize)
		chk = chk.SetRequiredEvents(requiredEvents, maxChunkSize)
		err := Next(ctx, ow.executor, chk)
		if err != nil {
			return task, err
		}
		if chk.NumEvents() == 0 {
			break
		}

		task.outerResult.Add(chk)
	}
	if task.outerResult.Len() == 0 {
		return nil, nil
	}
	numChks := task.outerResult.NumChunks()
	if ow.filter != nil {
		task.outerMatch = make([][]bool, task.outerResult.NumChunks())
		var err error
		for i := 0; i < numChks; i++ {
			chk := task.outerResult.GetChunk(i)
			outerMatch := make([]bool, 0, chk.NumEvents())
			task.memTracker.Consume(int64(cap(outerMatch)))
			task.outerMatch[i], err = expression.VectorizedFilter(ow.ctx, ow.filter, chunk.NewIterator4Chunk(chk), outerMatch)
			if err != nil {
				return task, err
			}
		}
	}
	task.encodedLookUpKeys = make([]*chunk.Chunk, task.outerResult.NumChunks())
	for i := range task.encodedLookUpKeys {
		task.encodedLookUpKeys[i] = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeBlob)}, task.outerResult.GetChunk(i).NumEvents())
	}
	return task, nil
}

func (ow *outerWorker) increaseBatchSize() {
	if ow.batchSize < ow.maxBatchSize {
		ow.batchSize *= 2
	}
	if ow.batchSize > ow.maxBatchSize {
		ow.batchSize = ow.maxBatchSize
	}
}

func (iw *innerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer trace.StartRegion(ctx, "IndexLookupJoinInnerWorker").End()
	var task *lookUpJoinTask
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("innerWorker panicked", zap.String("stack", string(buf)))
			// "task != nil" is guaranteed when panic happened.
			task.doneCh <- errors.Errorf("%v", r)
		}
		wg.Done()
	}()

	for ok := true; ok; {
		select {
		case task, ok = <-iw.taskCh:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}

		err := iw.handleTask(ctx, task)
		task.doneCh <- err
	}
}

type indexJoinLookUpContent struct {
	keys  []types.Causet
	event chunk.Event
}

func (iw *innerWorker) handleTask(ctx context.Context, task *lookUpJoinTask) error {
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.totalTime, int64(time.Since(start)))
		}()
	}
	lookUpContents, err := iw.constructLookupContent(task)
	if err != nil {
		return err
	}
	err = iw.fetchInnerResults(ctx, task, lookUpContents)
	if err != nil {
		return err
	}
	err = iw.buildLookUpMap(task)
	if err != nil {
		return err
	}
	return nil
}

func (iw *innerWorker) constructLookupContent(task *lookUpJoinTask) ([]*indexJoinLookUpContent, error) {
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.task, 1)
			atomic.AddInt64(&iw.stats.construct, int64(time.Since(start)))
		}()
	}
	lookUpContents := make([]*indexJoinLookUpContent, 0, task.outerResult.Len())
	keyBuf := make([]byte, 0, 64)
	for chkIdx := 0; chkIdx < task.outerResult.NumChunks(); chkIdx++ {
		chk := task.outerResult.GetChunk(chkIdx)
		numEvents := chk.NumEvents()
		for rowIdx := 0; rowIdx < numEvents; rowIdx++ {
			dLookUpKey, err := iw.constructCausetLookupKey(task, chkIdx, rowIdx)
			if err != nil {
				return nil, err
			}
			if dLookUpKey == nil {
				// Append null to make looUpKeys the same length as outer Result.
				task.encodedLookUpKeys[chkIdx].AppendNull(0)
				continue
			}
			keyBuf = keyBuf[:0]
			keyBuf, err = codec.EncodeKey(iw.ctx.GetStochaseinstein_dbars().StmtCtx, keyBuf, dLookUpKey...)
			if err != nil {
				return nil, err
			}
			// CausetStore the encoded lookup key in chunk, so we can use it to lookup the matched inners directly.
			task.encodedLookUpKeys[chkIdx].AppendBytes(0, keyBuf)
			if iw.hasPrefixDefCaus {
				for i := range iw.outerCtx.keyDefCauss {
					// If it's a prefix defCausumn. Try to fix it.
					if iw.defCausLens[i] != types.UnspecifiedLength {
						ranger.CutCausetByPrefixLen(&dLookUpKey[i], iw.defCausLens[i], iw.rowTypes[iw.keyDefCauss[i]])
					}
				}
				// dLookUpKey is sorted and deduplicated at sortAndDedupLookUpContents.
				// So we don't need to do it here.
			}
			lookUpContents = append(lookUpContents, &indexJoinLookUpContent{keys: dLookUpKey, event: chk.GetEvent(rowIdx)})
		}
	}

	for i := range task.encodedLookUpKeys {
		task.memTracker.Consume(task.encodedLookUpKeys[i].MemoryUsage())
	}
	lookUpContents = iw.sortAndDedupLookUpContents(lookUpContents)
	return lookUpContents, nil
}

func (iw *innerWorker) constructCausetLookupKey(task *lookUpJoinTask, chkIdx, rowIdx int) ([]types.Causet, error) {
	if task.outerMatch != nil && !task.outerMatch[chkIdx][rowIdx] {
		return nil, nil
	}
	outerEvent := task.outerResult.GetChunk(chkIdx).GetEvent(rowIdx)
	sc := iw.ctx.GetStochaseinstein_dbars().StmtCtx
	keyLen := len(iw.keyDefCauss)
	dLookupKey := make([]types.Causet, 0, keyLen)
	for i, keyDefCaus := range iw.outerCtx.keyDefCauss {
		outerValue := outerEvent.GetCauset(keyDefCaus, iw.outerCtx.rowTypes[keyDefCaus])
		// Join-on-condition can be promised to be equal-condition in
		// IndexNestedLoopJoin, thus the filter will always be false if
		// outerValue is null, and we don't need to lookup it.
		if outerValue.IsNull() {
			return nil, nil
		}
		innerDefCausType := iw.rowTypes[iw.keyDefCauss[i]]
		innerValue, err := outerValue.ConvertTo(sc, innerDefCausType)
		if err != nil {
			// If the converted outerValue overflows, we don't need to lookup it.
			if terror.ErrorEqual(err, types.ErrOverflow) {
				return nil, nil
			}
			if terror.ErrorEqual(err, types.ErrTruncated) && (innerDefCausType.Tp == allegrosql.TypeSet || innerDefCausType.Tp == allegrosql.TypeEnum) {
				return nil, nil
			}
			return nil, err
		}
		cmp, err := outerValue.CompareCauset(sc, &innerValue)
		if err != nil {
			return nil, err
		}
		if cmp != 0 {
			// If the converted outerValue is not equal to the origin outerValue, we don't need to lookup it.
			return nil, nil
		}
		dLookupKey = append(dLookupKey, innerValue)
	}
	return dLookupKey, nil
}

func (iw *innerWorker) sortAndDedupLookUpContents(lookUpContents []*indexJoinLookUpContent) []*indexJoinLookUpContent {
	if len(lookUpContents) < 2 {
		return lookUpContents
	}
	sc := iw.ctx.GetStochaseinstein_dbars().StmtCtx
	sort.Slice(lookUpContents, func(i, j int) bool {
		cmp := compareEvent(sc, lookUpContents[i].keys, lookUpContents[j].keys)
		if cmp != 0 || iw.nextDefCausCompareFilters == nil {
			return cmp < 0
		}
		return iw.nextDefCausCompareFilters.CompareEvent(lookUpContents[i].event, lookUpContents[j].event) < 0
	})
	deDupedLookupKeys := lookUpContents[:1]
	for i := 1; i < len(lookUpContents); i++ {
		cmp := compareEvent(sc, lookUpContents[i].keys, lookUpContents[i-1].keys)
		if cmp != 0 || (iw.nextDefCausCompareFilters != nil && iw.nextDefCausCompareFilters.CompareEvent(lookUpContents[i].event, lookUpContents[i-1].event) != 0) {
			deDupedLookupKeys = append(deDupedLookupKeys, lookUpContents[i])
		}
	}
	return deDupedLookupKeys
}

func compareEvent(sc *stmtctx.StatementContext, left, right []types.Causet) int {
	for idx := 0; idx < len(left); idx++ {
		cmp, err := left[idx].CompareCauset(sc, &right[idx])
		// We only compare rows with the same type, no error to return.
		terror.Log(err)
		if cmp > 0 {
			return 1
		} else if cmp < 0 {
			return -1
		}
	}
	return 0
}

func (iw *innerWorker) fetchInnerResults(ctx context.Context, task *lookUpJoinTask, lookUpContent []*indexJoinLookUpContent) error {
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.fetch, int64(time.Since(start)))
		}()
	}
	innerExec, err := iw.readerBuilder.buildExecutorForIndexJoin(ctx, lookUpContent, iw.indexRanges, iw.keyOff2IdxOff, iw.nextDefCausCompareFilters)
	if err != nil {
		return err
	}
	defer terror.Call(innerExec.Close)
	innerResult := chunk.NewList(retTypes(innerExec), iw.ctx.GetStochaseinstein_dbars().MaxChunkSize, iw.ctx.GetStochaseinstein_dbars().MaxChunkSize)
	innerResult.GetMemTracker().SetLabel(memory.LabelForBuildSideResult)
	innerResult.GetMemTracker().AttachTo(task.memTracker)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		err := Next(ctx, innerExec, iw.executorChk)
		if err != nil {
			return err
		}
		if iw.executorChk.NumEvents() == 0 {
			break
		}
		innerResult.Add(iw.executorChk)
		iw.executorChk = newFirstChunk(innerExec)
	}
	task.innerResult = innerResult
	return nil
}

func (iw *innerWorker) buildLookUpMap(task *lookUpJoinTask) error {
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.build, int64(time.Since(start)))
		}()
	}
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 8)
	for i := 0; i < task.innerResult.NumChunks(); i++ {
		chk := task.innerResult.GetChunk(i)
		for j := 0; j < chk.NumEvents(); j++ {
			innerEvent := chk.GetEvent(j)
			if iw.hasNullInJoinKey(innerEvent) {
				continue
			}

			keyBuf = keyBuf[:0]
			for _, keyDefCaus := range iw.keyDefCauss {
				d := innerEvent.GetCauset(keyDefCaus, iw.rowTypes[keyDefCaus])
				var err error
				keyBuf, err = codec.EncodeKey(iw.ctx.GetStochaseinstein_dbars().StmtCtx, keyBuf, d)
				if err != nil {
					return err
				}
			}
			rowPtr := chunk.EventPtr{ChkIdx: uint32(i), EventIdx: uint32(j)}
			*(*chunk.EventPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
			task.lookupMap.Put(keyBuf, valBuf)
		}
	}
	return nil
}

func (iw *innerWorker) hasNullInJoinKey(event chunk.Event) bool {
	for _, ordinal := range iw.keyDefCauss {
		if event.IsNull(ordinal) {
			return true
		}
	}
	return false
}

// Close implements the Executor interface.
func (e *IndexLookUpJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.workerWg.Wait()
	e.memTracker = nil
	e.task = nil
	return e.baseExecutor.Close()
}

type indexLookUpJoinRuntimeStats struct {
	concurrency int
	probe       int64
	innerWorker innerWorkerRuntimeStats
}

type innerWorkerRuntimeStats struct {
	totalTime int64
	task      int64
	construct int64
	fetch     int64
	build     int64
	join      int64
}

func (e *indexLookUpJoinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	if e.innerWorker.totalTime > 0 {
		buf.WriteString("inner:{total:")
		buf.WriteString(time.Duration(e.innerWorker.totalTime).String())
		buf.WriteString(", concurrency:")
		if e.concurrency > 0 {
			buf.WriteString(strconv.Itoa(e.concurrency))
		} else {
			buf.WriteString("OFF")
		}
		buf.WriteString(", task:")
		buf.WriteString(strconv.FormatInt(e.innerWorker.task, 10))
		buf.WriteString(", construct:")
		buf.WriteString(time.Duration(e.innerWorker.construct).String())
		buf.WriteString(", fetch:")
		buf.WriteString(time.Duration(e.innerWorker.fetch).String())
		buf.WriteString(", build:")
		buf.WriteString(time.Duration(e.innerWorker.build).String())
		if e.innerWorker.join > 0 {
			buf.WriteString(", join:")
			buf.WriteString(time.Duration(e.innerWorker.join).String())
		}
		buf.WriteString("}")
	}
	if e.probe > 0 {
		buf.WriteString(", probe:")
		buf.WriteString(time.Duration(e.probe).String())
	}
	return buf.String()
}

func (e *indexLookUpJoinRuntimeStats) Clone() execdetails.RuntimeStats {
	return &indexLookUpJoinRuntimeStats{
		concurrency: e.concurrency,
		probe:       e.probe,
		innerWorker: e.innerWorker,
	}
}

func (e *indexLookUpJoinRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*indexLookUpJoinRuntimeStats)
	if !ok {
		return
	}
	e.probe += tmp.probe
	e.innerWorker.totalTime += tmp.innerWorker.totalTime
	e.innerWorker.task += tmp.innerWorker.task
	e.innerWorker.construct += tmp.innerWorker.construct
	e.innerWorker.fetch += tmp.innerWorker.fetch
	e.innerWorker.build += tmp.innerWorker.build
	e.innerWorker.join += tmp.innerWorker.join
}

// Tp implements the RuntimeStats interface.
func (e *indexLookUpJoinRuntimeStats) Tp() int {
	return execdetails.TpIndexLookUpJoinRuntimeStats
}
