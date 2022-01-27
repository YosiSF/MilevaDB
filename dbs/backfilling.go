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

package dbs

import (
	"context"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	dbsutil "github.com/whtcorpsinc/milevadb/dbs/soliton"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	decoder "github.com/whtcorpsinc/milevadb/soliton/rowDecoder"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"go.uber.org/zap"
)

type backfillWorkerType byte

const (
	typeAddIndexWorker                backfillWorkerType = 0
	typeUFIDelateDeferredCausetWorker backfillWorkerType = 1
)

func (bWT backfillWorkerType) String() string {
	switch bWT {
	case typeAddIndexWorker:
		return "add index"
	case typeUFIDelateDeferredCausetWorker:
		return "uFIDelate column"
	default:
		return "unknown"
	}
}

type backfiller interface {
	BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error)
	AddMetricInfo(float64)
}

type backfillResult struct {
	addedCount int
	scanCount  int
	nextHandle ekv.Handle
	err        error
}

// backfillTaskContext is the context of the batch adding indices or uFIDelating column values.
// After finishing the batch adding indices or uFIDelating column values, result in backfillTaskContext will be merged into backfillResult.
type backfillTaskContext struct {
	nextHandle ekv.Handle
	done       bool
	addedCount int
	scanCount  int
}

type backfillWorker struct {
	id        int
	dbsWorker *worker
	batchCnt  int
	sessCtx   stochastikctx.Context
	taskCh    chan *reorgBackfillTask
	resultCh  chan *backfillResult
	block     block.Block
	closed    bool
	priority  int
}

func newBackfillWorker(sessCtx stochastikctx.Context, worker *worker, id int, t block.PhysicalTable) *backfillWorker {
	return &backfillWorker{
		id:        id,
		block:     t,
		dbsWorker: worker,
		batchCnt:  int(variable.GetDBSReorgBatchSize()),
		sessCtx:   sessCtx,
		taskCh:    make(chan *reorgBackfillTask, 1),
		resultCh:  make(chan *backfillResult, 1),
		priority:  ekv.PriorityLow,
	}
}

func (w *backfillWorker) Close() {
	if !w.closed {
		w.closed = true
		close(w.taskCh)
	}
}

func closeBackfillWorkers(workers []*backfillWorker) {
	for _, worker := range workers {
		worker.Close()
	}
}

type reorgBackfillTask struct {
	physicalTableID int64
	startHandle     ekv.Handle
	endHandle       ekv.Handle
	// endIncluded indicates whether the range include the endHandle.
	// When the last handle is math.MaxInt64, set endIncluded to true to
	// tell worker backfilling index of endHandle.
	endIncluded bool
}

func (r *reorgBackfillTask) String() string {
	rightParenthesis := ")"
	if r.endIncluded {
		rightParenthesis = "]"
	}
	return "physicalTableID" + strconv.FormatInt(r.physicalTableID, 10) + "_" + "[" + r.startHandle.String() + "," + r.endHandle.String() + rightParenthesis
}

func logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	if threshold == 0 {
		threshold = atomic.LoadUint32(&variable.DBSSlowOprThreshold)
	}

	if elapsed >= time.Duration(threshold)*time.Millisecond {
		logutil.BgLogger().Info("[dbs] slow operations", zap.Duration("takeTimes", elapsed), zap.String("msg", slowMsg))
	}
}

// mergeBackfillCtxToResult merge partial result in taskCtx into result.
func mergeBackfillCtxToResult(taskCtx *backfillTaskContext, result *backfillResult) {
	result.nextHandle = taskCtx.nextHandle
	result.addedCount += taskCtx.addedCount
	result.scanCount += taskCtx.scanCount
}

// handleBackfillTask backfills range [task.startHandle, task.endHandle) handle's index to block.
func (w *backfillWorker) handleBackfillTask(d *dbsCtx, task *reorgBackfillTask, bf backfiller) *backfillResult {
	handleRange := *task
	result := &backfillResult{addedCount: 0, nextHandle: handleRange.startHandle, err: nil}
	lastLogCount := 0
	lastLogTime := time.Now()
	startTime := lastLogTime

	for {
		// Give job chance to be canceled, if we not check it here,
		// if there is panic in bf.BackfillDataInTxn we will never cancel the job.
		// Because reorgRecordTask may run a long time,
		// we should check whether this dbs job is still runnable.
		err := w.dbsWorker.isReorgRunnable(d)
		if err != nil {
			result.err = err
			return result
		}

		taskCtx, err := bf.BackfillDataInTxn(handleRange)
		if err != nil {
			result.err = err
			return result
		}

		bf.AddMetricInfo(float64(taskCtx.addedCount))
		mergeBackfillCtxToResult(&taskCtx, result)
		w.dbsWorker.reorgCtx.increaseRowCount(int64(taskCtx.addedCount))

		if num := result.scanCount - lastLogCount; num >= 30000 {
			lastLogCount = result.scanCount
			logutil.BgLogger().Info("[dbs] backfill worker back fill index", zap.Int("workerID", w.id), zap.Int("addedCount", result.addedCount),
				zap.Int("scanCount", result.scanCount), zap.String("nextHandle", toString(taskCtx.nextHandle)), zap.Float64("speed(rows/s)", float64(num)/time.Since(lastLogTime).Seconds()))
			lastLogTime = time.Now()
		}

		handleRange.startHandle = taskCtx.nextHandle
		if taskCtx.done {
			break
		}
	}
	logutil.BgLogger().Info("[dbs] backfill worker finish task", zap.Int("workerID", w.id),
		zap.String("task", task.String()), zap.Int("addedCount", result.addedCount),
		zap.Int("scanCount", result.scanCount), zap.String("nextHandle", toString(result.nextHandle)),
		zap.String("takeTime", time.Since(startTime).String()))
	return result
}

func (w *backfillWorker) run(d *dbsCtx, bf backfiller) {
	logutil.BgLogger().Info("[dbs] backfill worker start", zap.Int("workerID", w.id))
	defer func() {
		w.resultCh <- &backfillResult{err: errReorgPanic}
	}()
	defer soliton.Recover(metrics.LabelDBS, "backfillWorker.run", nil, false)
	for {
		task, more := <-w.taskCh
		if !more {
			break
		}

		logutil.BgLogger().Debug("[dbs] backfill worker got task", zap.Int("workerID", w.id), zap.String("task", task.String()))
		failpoint.Inject("mockBackfillRunErr", func() {
			if w.id == 0 {
				result := &backfillResult{addedCount: 0, nextHandle: nil, err: errors.Errorf("mock backfill error")}
				w.resultCh <- result
				failpoint.Continue()
			}
		})

		// Dynamic change batch size.
		w.batchCnt = int(variable.GetDBSReorgBatchSize())
		result := w.handleBackfillTask(d, task, bf)
		w.resultCh <- result
	}
	logutil.BgLogger().Info("[dbs] backfill worker exit", zap.Int("workerID", w.id))
}

// splitTableRanges uses FIDel region's key ranges to split the backfilling block key range space,
// to speed up backfilling data in block with disperse handle.
// The `t` should be a non-partitioned block or a partition.
func splitTableRanges(t block.PhysicalTable, causetstore ekv.CausetStorage, startHandle, endHandle ekv.Handle) ([]ekv.KeyRange, error) {
	startRecordKey := t.RecordKey(startHandle)
	endRecordKey := t.RecordKey(endHandle)

	logutil.BgLogger().Info("[dbs] split block range from FIDel", zap.Int64("physicalTableID", t.GetPhysicalID()),
		zap.String("startHandle", toString(startHandle)), zap.String("endHandle", toString(endHandle)))
	kvRange := ekv.KeyRange{StartKey: startRecordKey, EndKey: endRecordKey}
	s, ok := causetstore.(einsteindb.CausetStorage)
	if !ok {
		// Only support split ranges in einsteindb.CausetStorage now.
		return []ekv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := einsteindb.NewBackofferWithVars(context.Background(), maxSleep, nil)
	ranges, err := einsteindb.SplitRegionRanges(bo, s.GetRegionCache(), []ekv.KeyRange{kvRange})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ranges) == 0 {
		return nil, errors.Trace(errInvalidSplitRegionRanges)
	}
	return ranges, nil
}

func (w *worker) waitTaskResults(workers []*backfillWorker, taskCnt int, totalAddedCount *int64, startHandle ekv.Handle) (ekv.Handle, int64, error) {
	var (
		addedCount int64
		nextHandle = startHandle
		firstErr   error
	)
	for i := 0; i < taskCnt; i++ {
		worker := workers[i]
		result := <-worker.resultCh
		if firstErr == nil && result.err != nil {
			firstErr = result.err
			// We should wait all working workers exits, any way.
			continue
		}

		if result.err != nil {
			logutil.BgLogger().Warn("[dbs] backfill worker failed", zap.Int("workerID", worker.id),
				zap.Error(result.err))
		}

		if firstErr == nil {
			*totalAddedCount += int64(result.addedCount)
			addedCount += int64(result.addedCount)
			nextHandle = result.nextHandle
		}
	}

	return nextHandle, addedCount, errors.Trace(firstErr)
}

// handleReorgTasks sends tasks to workers, and waits for all the running workers to return results,
// there are taskCnt running workers.
func (w *worker) handleReorgTasks(reorgInfo *reorgInfo, totalAddedCount *int64, workers []*backfillWorker, batchTasks []*reorgBackfillTask) error {
	for i, task := range batchTasks {
		workers[i].taskCh <- task
	}

	startHandle := batchTasks[0].startHandle
	taskCnt := len(batchTasks)
	startTime := time.Now()
	nextHandle, taskAddedCount, err := w.waitTaskResults(workers, taskCnt, totalAddedCount, startHandle)
	elapsedTime := time.Since(startTime)
	if err == nil {
		err = w.isReorgRunnable(reorgInfo.d)
	}

	if err != nil {
		// UFIDelate the reorg handle that has been processed.
		err1 := ekv.RunInNewTxn(reorgInfo.d.causetstore, true, func(txn ekv.Transaction) error {
			return errors.Trace(reorgInfo.UFIDelateReorgMeta(txn, nextHandle, reorgInfo.EndHandle, reorgInfo.PhysicalTableID))
		})
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime.Seconds())
		logutil.BgLogger().Warn("[dbs] backfill worker handle batch tasks failed",
			zap.Int64("totalAddedCount", *totalAddedCount), zap.String("startHandle", toString(startHandle)),
			zap.String("nextHandle", toString(nextHandle)), zap.Int64("batchAddedCount", taskAddedCount),
			zap.String("taskFailedError", err.Error()), zap.String("takeTime", elapsedTime.String()),
			zap.NamedError("uFIDelateHandleError", err1))
		return errors.Trace(err)
	}

	// nextHandle will be uFIDelated periodically in runReorgJob, so no need to uFIDelate it here.
	w.reorgCtx.setNextHandle(nextHandle)
	metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime.Seconds())
	logutil.BgLogger().Info("[dbs] backfill worker handle batch tasks successful", zap.Int64("totalAddedCount", *totalAddedCount), zap.String("startHandle", toString(startHandle)),
		zap.String("nextHandle", toString(nextHandle)), zap.Int64("batchAddedCount", taskAddedCount), zap.String("takeTime", elapsedTime.String()))
	return nil
}

func decodeHandleRange(keyRange ekv.KeyRange) (ekv.Handle, ekv.Handle, error) {
	startHandle, err := blockcodec.DecodeRowKey(keyRange.StartKey)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	endHandle, err := blockcodec.DecodeRowKey(keyRange.EndKey)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return startHandle, endHandle, nil
}

// sendRangeTaskToWorkers sends tasks to workers, and returns remaining kvRanges that is not handled.
func (w *worker) sendRangeTaskToWorkers(workers []*backfillWorker, reorgInfo *reorgInfo,
	totalAddedCount *int64, kvRanges []ekv.KeyRange, globalEndHandle ekv.Handle) ([]ekv.KeyRange, error) {
	batchTasks := make([]*reorgBackfillTask, 0, len(workers))
	physicalTableID := reorgInfo.PhysicalTableID

	// Build reorg tasks.
	for _, keyRange := range kvRanges {
		startHandle, endHandle, err := decodeHandleRange(keyRange)
		if err != nil {
			return nil, errors.Trace(err)
		}

		endIncluded := false
		if endHandle.Equal(globalEndHandle) {
			endIncluded = true
		}
		task := &reorgBackfillTask{physicalTableID, startHandle, endHandle, endIncluded}
		batchTasks = append(batchTasks, task)

		if len(batchTasks) >= len(workers) {
			break
		}
	}

	if len(batchTasks) == 0 {
		return nil, nil
	}

	// Wait tasks finish.
	err := w.handleReorgTasks(reorgInfo, totalAddedCount, workers, batchTasks)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(batchTasks) < len(kvRanges) {
		// There are kvRanges not handled.
		remains := kvRanges[len(batchTasks):]
		return remains, nil
	}

	return nil, nil
}

var (
	// TestCheckWorkerNumCh use for test adjust backfill worker.
	TestCheckWorkerNumCh = make(chan struct{})
	// TestCheckWorkerNumber use for test adjust backfill worker.
	TestCheckWorkerNumber = int32(16)
)

func loadDBSReorgVars(w *worker) error {
	// Get stochastikctx from context resource pool.
	var ctx stochastikctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)
	return dbsutil.LoadDBSReorgVars(ctx)
}

func makeuFIDelecodeDefCausMap(sessCtx stochastikctx.Context, t block.Block) (map[int64]decoder.DeferredCauset, error) {
	dbName := perceptron.NewCIStr(sessCtx.GetStochastikVars().CurrentDB)
	wriblockDefCausInfos := make([]*perceptron.DeferredCausetInfo, 0, len(t.WriblockDefCauss()))
	for _, col := range t.WriblockDefCauss() {
		wriblockDefCausInfos = append(wriblockDefCausInfos, col.DeferredCausetInfo)
	}
	exprDefCauss, _, err := expression.DeferredCausetInfos2DeferredCausetsAndNames(sessCtx, dbName, t.Meta().Name, wriblockDefCausInfos, t.Meta())
	if err != nil {
		return nil, err
	}
	mockSchema := expression.NewSchema(exprDefCauss...)

	decodeDefCausMap := decoder.BuildFullDecodeDefCausMap(t.WriblockDefCauss(), mockSchema)

	return decodeDefCausMap, nil
}

// writePhysicalTableRecord handles the "add index" or "modify/change column" reorganization state for a non-partitioned block or a partition.
// For a partitioned block, it should be handled partition by partition.
//
// How to "add index" or "uFIDelate column value" in reorganization state?
// Concurrently process the @@milevadb_dbs_reorg_worker_cnt tasks. Each task deals with a handle range of the index/event record.
// The handle range is split from FIDel regions now. Each worker deal with a region block key range one time.
// Each handle range by estimation, concurrent processing needs to perform after the handle range has been acquired.
// The operation flow is as follows:
//	1. Open numbers of defaultWorkers goroutines.
//	2. Split block key range from FIDel regions.
//	3. Send tasks to running workers by workers's task channel. Each task deals with a region key ranges.
//	4. Wait all these running tasks finished, then continue to step 3, until all tasks is done.
// The above operations are completed in a transaction.
// Finally, uFIDelate the concurrent processing of the total number of rows, and causetstore the completed handle value.
func (w *worker) writePhysicalTableRecord(t block.PhysicalTable, bfWorkerType backfillWorkerType, indexInfo *perceptron.IndexInfo, oldDefCausInfo, colInfo *perceptron.DeferredCausetInfo, reorgInfo *reorgInfo) error {
	job := reorgInfo.Job
	totalAddedCount := job.GetRowCount()

	startHandle, endHandle := reorgInfo.StartHandle, reorgInfo.EndHandle
	sessCtx := newContext(reorgInfo.d.causetstore)
	decodeDefCausMap, err := makeuFIDelecodeDefCausMap(sessCtx, t)
	if err != nil {
		return errors.Trace(err)
	}

	if err := w.isReorgRunnable(reorgInfo.d); err != nil {
		return errors.Trace(err)
	}
	if startHandle == nil && endHandle == nil {
		return nil
	}

	failpoint.Inject("MockCaseWhenParseFailure", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("job.ErrCount:" + strconv.Itoa(int(job.ErrorCount)) + ", mock unknown type: ast.whenClause."))
		}
	})

	// variable.dbsReorgWorkerCounter can be modified by system variable "milevadb_dbs_reorg_worker_cnt".
	workerCnt := variable.GetDBSReorgWorkerCounter()
	backfillWorkers := make([]*backfillWorker, 0, workerCnt)
	defer func() {
		closeBackfillWorkers(backfillWorkers)
	}()

	for {
		kvRanges, err := splitTableRanges(t, reorgInfo.d.causetstore, startHandle, endHandle)
		if err != nil {
			return errors.Trace(err)
		}

		// For dynamic adjust backfill worker number.
		if err := loadDBSReorgVars(w); err != nil {
			logutil.BgLogger().Error("[dbs] load DBS reorganization variable failed", zap.Error(err))
		}
		workerCnt = variable.GetDBSReorgWorkerCounter()
		// If only have 1 range, we can only start 1 worker.
		if len(kvRanges) < int(workerCnt) {
			workerCnt = int32(len(kvRanges))
		}
		// Enlarge the worker size.
		for i := len(backfillWorkers); i < int(workerCnt); i++ {
			sessCtx := newContext(reorgInfo.d.causetstore)
			sessCtx.GetStochastikVars().StmtCtx.IsDBSJobInQueue = true

			if bfWorkerType == typeAddIndexWorker {
				idxWorker := newAddIndexWorker(sessCtx, w, i, t, indexInfo, decodeDefCausMap)
				idxWorker.priority = job.Priority
				backfillWorkers = append(backfillWorkers, idxWorker.backfillWorker)
				go idxWorker.backfillWorker.run(reorgInfo.d, idxWorker)
			} else {
				uFIDelateWorker := newUFIDelateDeferredCausetWorker(sessCtx, w, i, t, oldDefCausInfo, colInfo, decodeDefCausMap)
				uFIDelateWorker.priority = job.Priority
				backfillWorkers = append(backfillWorkers, uFIDelateWorker.backfillWorker)
				go uFIDelateWorker.backfillWorker.run(reorgInfo.d, uFIDelateWorker)
			}
		}
		// Shrink the worker size.
		if len(backfillWorkers) > int(workerCnt) {
			workers := backfillWorkers[workerCnt:]
			backfillWorkers = backfillWorkers[:workerCnt]
			closeBackfillWorkers(workers)
		}

		failpoint.Inject("checkBackfillWorkerNum", func(val failpoint.Value) {
			if val.(bool) {
				num := int(atomic.LoadInt32(&TestCheckWorkerNumber))
				if num != 0 {
					if num > len(kvRanges) {
						if len(backfillWorkers) != len(kvRanges) {
							failpoint.Return(errors.Errorf("check backfill worker num error, len ekv ranges is: %v, check backfill worker num is: %v, actual record num is: %v", len(kvRanges), num, len(backfillWorkers)))
						}
					} else if num != len(backfillWorkers) {
						failpoint.Return(errors.Errorf("check backfill worker num error, len ekv ranges is: %v, check backfill worker num is: %v, actual record num is: %v", len(kvRanges), num, len(backfillWorkers)))
					}
					TestCheckWorkerNumCh <- struct{}{}
				}
			}
		})

		logutil.BgLogger().Info("[dbs] start backfill workers to reorg record", zap.Int("workerCnt", len(backfillWorkers)),
			zap.Int("regionCnt", len(kvRanges)), zap.String("startHandle", toString(startHandle)), zap.String("endHandle", toString(endHandle)))
		remains, err := w.sendRangeTaskToWorkers(backfillWorkers, reorgInfo, &totalAddedCount, kvRanges, endHandle)
		if err != nil {
			return errors.Trace(err)
		}

		if len(remains) == 0 {
			break
		}
		startHandle, _, err = decodeHandleRange(remains[0])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h ekv.Handle, rowKey ekv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotRows(causetstore ekv.CausetStorage, priority int, t block.Block, version uint64, startHandle ekv.Handle, endHandle ekv.Handle, endIncluded bool, fn recordIterFunc) error {
	var firstKey ekv.Key
	if startHandle == nil {
		firstKey = t.RecordPrefix()
	} else {
		firstKey = t.RecordKey(startHandle)
	}

	var upperBound ekv.Key
	if endHandle == nil {
		upperBound = t.RecordPrefix().PrefixNext()
	} else {
		if endIncluded {
			if endHandle.IsInt() && endHandle.IntValue() == math.MaxInt64 {
				upperBound = t.RecordKey(endHandle).PrefixNext()
			} else {
				upperBound = t.RecordKey(endHandle.Next())
			}
		} else {
			upperBound = t.RecordKey(endHandle)
		}
	}

	ver := ekv.Version{Ver: version}
	snap, err := causetstore.GetSnapshot(ver)
	snap.SetOption(ekv.Priority, priority)
	if err != nil {
		return errors.Trace(err)
	}

	it, err := snap.Iter(firstKey, upperBound)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	for it.Valid() {
		if !it.Key().HasPrefix(t.RecordPrefix()) {
			break
		}

		var handle ekv.Handle
		handle, err = blockcodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}
		rk := t.RecordKey(handle)

		more, err := fn(handle, rk, it.Value())
		if !more || err != nil {
			return errors.Trace(err)
		}

		err = ekv.NextUntil(it, soliton.RowKeyPrefixFilter(rk))
		if err != nil {
			if ekv.ErrNotExist.Equal(err) {
				break
			}
			return errors.Trace(err)
		}
	}

	return nil
}
