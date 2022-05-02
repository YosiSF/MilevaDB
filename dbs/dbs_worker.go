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

package dbs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	pumpcli "github.com/whtcorpsinc/MilevaDB-Prod-tools/milevadb-binlog/pump_client"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	milevadbutil "github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/admin"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"go.uber.org/zap"
)

var (
	// RunWorker indicates if this MilevaDB server starts DBS worker and can run DBS job.
	RunWorker = true
	// dbsWorkerID is used for generating the next DBS worker ID.
	dbsWorkerID = int32(0)
	// WaitTimeWhenErrorOccurred is waiting interval when processing DBS jobs encounter errors.
	WaitTimeWhenErrorOccurred = int64(1 * time.Second)
)

// GetWaitTimeWhenErrorOccurred return waiting interval when processing DBS jobs encounter errors.
func GetWaitTimeWhenErrorOccurred() time.Duration {
	return time.Duration(atomic.LoadInt64(&WaitTimeWhenErrorOccurred))
}

// SetWaitTimeWhenErrorOccurred uFIDelate waiting interval when processing DBS jobs encounter errors.
func SetWaitTimeWhenErrorOccurred(dur time.Duration) {
	atomic.StoreInt64(&WaitTimeWhenErrorOccurred, int64(dur))
}

type workerType byte

const (
	// generalWorker is the worker who handles all DBS statements except “add index”.
	generalWorker workerType = 0
	// addIdxWorker is the worker who handles the operation of adding indexes.
	addIdxWorker workerType = 1
	// waitDependencyJobInterval is the interval when the dependency job doesn't be done.
	waitDependencyJobInterval = 200 * time.Millisecond
	// noneDependencyJob means a job has no dependency-job.
	noneDependencyJob = 0
)

// worker is used for handling DBS jobs.
// Now we have two HoTTs of workers.
type worker struct {
	id       int32
	tp       workerType
	dbsJobCh chan struct{}
	ctx      context.Context
	wg       sync.WaitGroup

	sessPool        *stochastikPool // sessPool is used to new stochastik to execute ALLEGROALLEGROSQL in dbs package.
	reorgCtx        *reorgCtx       // reorgCtx is used for reorganization.
	delRangeManager delRangeManager
	logCtx          context.Context
}

func newWorker(ctx context.Context, tp workerType, sessPool *stochastikPool, delRangeMgr delRangeManager) *worker {
	worker := &worker{
		id:              atomic.AddInt32(&dbsWorkerID, 1),
		tp:              tp,
		dbsJobCh:        make(chan struct{}, 1),
		ctx:             ctx,
		reorgCtx:        &reorgCtx{notifyCancelReorgJob: 0},
		sessPool:        sessPool,
		delRangeManager: delRangeMgr,
	}

	worker.logCtx = logutil.WithKeyValue(context.Background(), "worker", worker.String())
	return worker
}

func (w *worker) typeStr() string {
	var str string
	switch w.tp {
	case generalWorker:
		str = "general"
	case addIdxWorker:
		str = perceptron.AddIndexStr
	default:
		str = "unknown"
	}
	return str
}

func (w *worker) String() string {
	return fmt.Sprintf("worker %d, tp %s", w.id, w.typeStr())
}

func (w *worker) close() {
	startTime := time.Now()
	w.wg.Wait()
	logutil.Logger(w.logCtx).Info("[dbs] DBS worker closed", zap.Duration("take time", time.Since(startTime)))
}

// start is used for async online schemaReplicant changing, it will try to become the owner firstly,
// then wait or pull the job queue to handle a schemaReplicant change job.
func (w *worker) start(d *dbsCtx) {
	logutil.Logger(w.logCtx).Info("[dbs] start DBS worker")
	defer w.wg.Done()
	defer milevadbutil.Recover(
		metrics.LabelDBSWorker,
		fmt.Sprintf("DBS ID %s, %s start", d.uuid, w),
		nil, true,
	)

	// We use 4 * lease time to check owner's timeout, so here, we will uFIDelate owner's status
	// every 2 * lease time. If lease is 0, we will use default 1s.
	// But we use etcd to speed up, normally it takes less than 1s now, so we use 1s as the max value.
	checkTime := chooseLeaseTime(2*d.lease, 1*time.Second)

	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			logutil.Logger(w.logCtx).Debug("[dbs] wait to check DBS status again", zap.Duration("interval", checkTime))
		case <-w.dbsJobCh:
		case <-w.ctx.Done():
			return
		}

		err := w.handleDBSJobQueue(d)
		if err != nil {
			logutil.Logger(w.logCtx).Error("[dbs] handle DBS job failed", zap.Error(err))
		}
	}
}

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// buildJobDependence sets the curjob's dependency-ID.
// The dependency-job's ID must less than the current job's ID, and we need the largest one in the list.
func buildJobDependence(t *meta.Meta, curJob *perceptron.Job) error {
	// Jobs in the same queue are ordered. If we want to find a job's dependency-job, we need to look for
	// it from the other queue. So if the job is "CausetActionAddIndex" job, we need find its dependency-job from DefaultJobList.
	var jobs []*perceptron.Job
	var err error
	switch curJob.Type {
	case perceptron.CausetActionAddIndex, perceptron.CausetActionAddPrimaryKey:
		jobs, err = t.GetAllDBSJobsInQueue(meta.DefaultJobListKey)
	default:
		jobs, err = t.GetAllDBSJobsInQueue(meta.AddIndexJobListKey)
	}
	if err != nil {
		return errors.Trace(err)
	}

	for _, job := range jobs {
		if curJob.ID < job.ID {
			continue
		}
		isDependent, err := curJob.IsDependentOn(job)
		if err != nil {
			return errors.Trace(err)
		}
		if isDependent {
			logutil.BgLogger().Info("[dbs] current DBS job depends on other job", zap.String("currentJob", curJob.String()), zap.String("dependentJob", job.String()))
			curJob.DependencyID = job.ID
			break
		}
	}
	return nil
}

func (d *dbs) limitDBSJobs() {
	defer d.wg.Done()
	defer milevadbutil.Recover(metrics.LabelDBS, "limitDBSJobs", nil, true)

	tasks := make([]*limitJobTask, 0, batchAddingJobs)
	for {
		select {
		case task := <-d.limitJobCh:
			tasks = tasks[:0]
			jobLen := len(d.limitJobCh)
			tasks = append(tasks, task)
			for i := 0; i < jobLen; i++ {
				tasks = append(tasks, <-d.limitJobCh)
			}
			d.addBatchDBSJobs(tasks)
		case <-d.ctx.Done():
			return
		}
	}
}

// addBatchDBSJobs gets global job IDs and puts the DBS jobs in the DBS queue.
func (d *dbs) addBatchDBSJobs(tasks []*limitJobTask) {
	startTime := time.Now()
	err := ekv.RunInNewTxn(d.causetstore, true, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		ids, err := t.GenGlobalIDs(len(tasks))
		if err != nil {
			return errors.Trace(err)
		}
		for i, task := range tasks {
			job := task.job
			job.Version = currentVersion
			job.StartTS = txn.StartTS()
			job.ID = ids[i]
			if err = buildJobDependence(t, job); err != nil {
				return errors.Trace(err)
			}

			if job.Type == perceptron.CausetActionAddIndex || job.Type == perceptron.CausetActionAddPrimaryKey {
				jobKey := meta.AddIndexJobListKey
				err = t.EnQueueDBSJob(job, jobKey)
			} else {
				err = t.EnQueueDBSJob(job)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	var jobs string
	for _, task := range tasks {
		task.err <- err
		jobs += task.job.String() + "; "
		metrics.DBSWorkerHistogram.WithLabelValues(metrics.WorkerAddDBSJob, task.job.Type.String(),
			metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}
	logutil.BgLogger().Info("[dbs] add DBS jobs", zap.Int("batch count", len(tasks)), zap.String("jobs", jobs))
}

// getHistoryDBSJob gets a DBS job with job's ID from history queue.
func (d *dbs) getHistoryDBSJob(id int64) (*perceptron.Job, error) {
	var job *perceptron.Job

	err := ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDBSJob(id)
		return errors.Trace(err1)
	})

	return job, errors.Trace(err)
}

// getFirstDBSJob gets the first DBS job form DBS queue.
func (w *worker) getFirstDBSJob(t *meta.Meta) (*perceptron.Job, error) {
	job, err := t.GetDBSJobByIdx(0)
	return job, errors.Trace(err)
}

// handleUFIDelateJobError handles the too large DBS job.
func (w *worker) handleUFIDelateJobError(t *meta.Meta, job *perceptron.Job, err error) error {
	if err == nil {
		return nil
	}
	if ekv.ErrEntryTooLarge.Equal(err) {
		logutil.Logger(w.logCtx).Warn("[dbs] uFIDelate DBS job failed", zap.String("job", job.String()), zap.Error(err))
		// Reduce this txn entry size.
		job.BinlogInfo.Clean()
		job.Error = toTError(err)
		job.ErrorCount++
		job.SchemaState = perceptron.StateNone
		job.State = perceptron.JobStateCancelled
		err = w.finishDBSJob(t, job)
	}
	return errors.Trace(err)
}

// uFIDelateDBSJob uFIDelates the DBS job information.
// Every time we enter another state except final state, we must call this function.
func (w *worker) uFIDelateDBSJob(t *meta.Meta, job *perceptron.Job, meetErr bool) error {
	failpoint.Inject("mockErrEntrySizeTooLarge", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ekv.ErrEntryTooLarge)
		}
	})
	uFIDelateRawArgs := true
	// If there is an error when running job and the RawArgs hasn't been decoded by DecodeArgs,
	// so we shouldn't replace RawArgs with the marshaling Args.
	if meetErr && (job.RawArgs != nil && job.Args == nil) {
		logutil.Logger(w.logCtx).Info("[dbs] meet something wrong before uFIDelate DBS job, shouldn't uFIDelate raw args",
			zap.String("job", job.String()))
		uFIDelateRawArgs = false
	}
	return errors.Trace(t.UFIDelateDBSJob(0, job, uFIDelateRawArgs))
}

func (w *worker) deleteRange(job *perceptron.Job) error {
	var err error
	if job.Version <= currentVersion {
		err = w.delRangeManager.addDelRangeJob(job)
	} else {
		err = errInvalidDBSJobVersion.GenWithStackByArgs(job.Version, currentVersion)
	}
	return errors.Trace(err)
}

// finishDBSJob deletes the finished DBS job in the dbs queue and puts it to history queue.
// If the DBS job need to handle in background, it will prepare a background job.
func (w *worker) finishDBSJob(t *meta.Meta, job *perceptron.Job) (err error) {
	startTime := time.Now()
	defer func() {
		metrics.DBSWorkerHistogram.WithLabelValues(metrics.WorkerFinishDBSJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	if !job.IsCancelled() {
		switch job.Type {
		case perceptron.CausetActionAddIndex, perceptron.CausetActionAddPrimaryKey:
			if job.State != perceptron.JobStateRollbackDone {
				break
			}

			// After rolling back an AddIndex operation, we need to use delete-range to delete the half-done index data.
			err = w.deleteRange(job)
		case perceptron.CausetActionDropSchema, perceptron.CausetActionDropBlock, perceptron.CausetActionTruncateBlock, perceptron.CausetActionDropIndex, perceptron.CausetActionDropPrimaryKey,
			perceptron.CausetActionDropBlockPartition, perceptron.CausetActionTruncateBlockPartition, perceptron.CausetActionDropDeferredCauset, perceptron.CausetActionDropDeferredCausets, perceptron.CausetActionModifyDeferredCauset:
			err = w.deleteRange(job)
		}
	}
	switch job.Type {
	case perceptron.CausetActionRecoverBlock:
		err = finishRecoverBlock(w, t, job)
	}
	if err != nil {
		return errors.Trace(err)
	}

	_, err = t.DeQueueDBSJob()
	if err != nil {
		return errors.Trace(err)
	}

	job.BinlogInfo.FinishedTS = t.StartTS
	logutil.Logger(w.logCtx).Info("[dbs] finish DBS job", zap.String("job", job.String()))
	uFIDelateRawArgs := true
	if job.Type == perceptron.CausetActionAddPrimaryKey && !job.IsCancelled() {
		// CausetActionAddPrimaryKey needs to check the warnings information in job.Args.
		// Notice: warnings is used to support non-strict mode.
		uFIDelateRawArgs = false
	}
	err = t.AddHistoryDBSJob(job, uFIDelateRawArgs)
	return errors.Trace(err)
}

func finishRecoverBlock(w *worker, t *meta.Meta, job *perceptron.Job) error {
	tbInfo := &perceptron.BlockInfo{}
	var autoIncID, autoRandID, dropJobID, recoverBlockCheckFlag int64
	var snapshotTS uint64
	err := job.DecodeArgs(tbInfo, &autoIncID, &dropJobID, &snapshotTS, &recoverBlockCheckFlag, &autoRandID)
	if err != nil {
		return errors.Trace(err)
	}
	if recoverBlockCheckFlag == recoverBlockCheckFlagEnableGC {
		err = enableGC(w)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func isDependencyJobDone(t *meta.Meta, job *perceptron.Job) (bool, error) {
	if job.DependencyID == noneDependencyJob {
		return true, nil
	}

	historyJob, err := t.GetHistoryDBSJob(job.DependencyID)
	if err != nil {
		return false, errors.Trace(err)
	}
	if historyJob == nil {
		return false, nil
	}
	logutil.BgLogger().Info("[dbs] current DBS job dependent job is finished", zap.String("currentJob", job.String()), zap.Int64("dependentJobID", job.DependencyID))
	job.DependencyID = noneDependencyJob
	return true, nil
}

func newMetaWithQueueTp(txn ekv.Transaction, tp string) *meta.Meta {
	if tp == perceptron.AddIndexStr || tp == perceptron.AddPrimaryKeyStr {
		return meta.NewMeta(txn, meta.AddIndexJobListKey)
	}
	return meta.NewMeta(txn)
}

// handleDBSJobQueue handles DBS jobs in DBS Job queue.
func (w *worker) handleDBSJobQueue(d *dbsCtx) error {
	once := true
	waitDependencyJobCnt := 0
	for {
		if isChanClosed(w.ctx.Done()) {
			return nil
		}

		var (
			job       *perceptron.Job
			schemaVer int64
			runJobErr error
		)
		waitTime := 2 * d.lease
		err := ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
			// We are not owner, return and retry checking later.
			if !d.isOwner() {
				return nil
			}

			var err error
			t := newMetaWithQueueTp(txn, w.typeStr())
			// We become the owner. Get the first job and run it.
			job, err = w.getFirstDBSJob(t)
			if job == nil || err != nil {
				return errors.Trace(err)
			}
			if isDone, err1 := isDependencyJobDone(t, job); err1 != nil || !isDone {
				return errors.Trace(err1)
			}

			if once {
				w.waitSchemaSynced(d, job, waitTime)
				once = false
				return nil
			}

			if job.IsDone() || job.IsRollbackDone() {
				if !job.IsRollbackDone() {
					job.State = perceptron.JobStateSynced
				}
				err = w.finishDBSJob(t, job)
				return errors.Trace(err)
			}

			d.mu.RLock()
			d.mu.hook.OnJobRunBefore(job)
			d.mu.RUnlock()

			// If running job meets error, we will save this error in job Error
			// and retry later if the job is not cancelled.
			schemaVer, runJobErr = w.runDBSJob(d, t, job)
			if job.IsCancelled() {
				txn.Reset()
				err = w.finishDBSJob(t, job)
				return errors.Trace(err)
			}
			if runJobErr != nil && !job.IsRollingback() && !job.IsRollbackDone() {
				// If the running job meets an error
				// and the job state is rolling back, it means that we have already handled this error.
				// Some DBS jobs (such as adding indexes) may need to uFIDelate the block info and the schemaReplicant version,
				// then shouldn't discard the KV modification.
				// And the job state is rollback done, it means the job was already finished, also shouldn't discard too.
				// Otherwise, we should discard the KV modification when running job.
				txn.Reset()
				// If error happens after uFIDelateSchemaVersion(), then the schemaVer is uFIDelated.
				// Result in the retry duration is up to 2 * lease.
				schemaVer = 0
			}
			err = w.uFIDelateDBSJob(t, job, runJobErr != nil)
			if err = w.handleUFIDelateJobError(t, job, err); err != nil {
				return errors.Trace(err)
			}
			writeBinlog(d.binlogCli, txn, job)
			return nil
		})

		if runJobErr != nil {
			// wait a while to retry again. If we don't wait here, DBS will retry this job immediately,
			// which may act like a deadlock.
			logutil.Logger(w.logCtx).Info("[dbs] run DBS job failed, sleeps a while then retries it.",
				zap.Duration("waitTime", GetWaitTimeWhenErrorOccurred()), zap.Error(runJobErr))
			time.Sleep(GetWaitTimeWhenErrorOccurred())
		}

		if err != nil {
			return errors.Trace(err)
		} else if job == nil {
			// No job now, return and retry getting later.
			return nil
		}
		w.waitDependencyJobFinished(job, &waitDependencyJobCnt)

		// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
		// If the job is done or still running or rolling back, we will wait 2 * lease time to guarantee other servers to uFIDelate
		// the newest schemaReplicant.
		ctx, cancel := context.WithTimeout(w.ctx, waitTime)
		w.waitSchemaChanged(ctx, d, waitTime, schemaVer, job)
		cancel()

		d.mu.RLock()
		d.mu.hook.OnJobUFIDelated(job)
		d.mu.RUnlock()

		if job.IsSynced() || job.IsCancelled() {
			asyncNotify(d.dbsJobDoneCh)
		}
	}
}

func skipWriteBinlog(job *perceptron.Job) bool {
	switch job.Type {
	// CausetActionUFIDelateTiFlashReplicaStatus is a MilevaDB internal DBS,
	// it's used to uFIDelate block's TiFlash replica available status.
	case perceptron.CausetActionUFIDelateTiFlashReplicaStatus:
		return true
	// It is done without modifying block info, bin log is not needed
	case perceptron.CausetActionAlterBlockAlterPartition:
		return true
	}

	return false
}

func writeBinlog(binlogCli *pumpcli.PumpsClient, txn ekv.Transaction, job *perceptron.Job) {
	if job.IsDone() || job.IsRollbackDone() ||
		// When this defCausumn is in the "delete only" and "delete reorg" states, the binlog of "drop defCausumn" has not been written yet,
		// but the defCausumn has been removed from the binlog of the write operation.
		// So we add this binlog to enable downstream components to handle DML correctly in this schemaReplicant state.
		((job.Type == perceptron.CausetActionDropDeferredCauset || job.Type == perceptron.CausetActionDropDeferredCausets) && job.SchemaState == perceptron.StateDeleteOnly) {
		if skipWriteBinlog(job) {
			return
		}
		binloginfo.SetDBSBinlog(binlogCli, txn, job.ID, int32(job.SchemaState), job.Query)
	}
}

// waitDependencyJobFinished waits for the dependency-job to be finished.
// If the dependency job isn't finished yet, we'd better wait a moment.
func (w *worker) waitDependencyJobFinished(job *perceptron.Job, cnt *int) {
	if job.DependencyID != noneDependencyJob {
		intervalCnt := int(3 * time.Second / waitDependencyJobInterval)
		if *cnt%intervalCnt == 0 {
			logutil.Logger(w.logCtx).Info("[dbs] DBS job need to wait dependent job, sleeps a while, then retries it.",
				zap.Int64("jobID", job.ID),
				zap.Int64("dependentJobID", job.DependencyID),
				zap.Duration("waitTime", waitDependencyJobInterval))
		}
		time.Sleep(waitDependencyJobInterval)
		*cnt++
	} else {
		*cnt = 0
	}
}

func chooseLeaseTime(t, max time.Duration) time.Duration {
	if t == 0 || t > max {
		return max
	}
	return t
}

// runDBSJob runs a DBS job. It returns the current schemaReplicant version in this transaction and the error.
func (w *worker) runDBSJob(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	defer milevadbutil.Recover(metrics.LabelDBSWorker, fmt.Sprintf("%s runDBSJob", w),
		func() {
			// If run DBS job panic, just cancel the DBS jobs.
			job.State = perceptron.JobStateCancelling
		}, false)

	// Mock for run dbs job panic.
	failpoint.Inject("mockPanicInRunDBSJob", func(val failpoint.Value) {})

	logutil.Logger(w.logCtx).Info("[dbs] run DBS job", zap.String("job", job.String()))
	timeStart := time.Now()
	defer func() {
		metrics.DBSWorkerHistogram.WithLabelValues(metrics.WorkerRunDBSJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()
	if job.IsFinished() {
		return
	}
	// The cause of this job state is that the job is cancelled by client.
	if job.IsCancelling() {
		return convertJob2RollbackJob(w, d, t, job)
	}

	if !job.IsRollingback() && !job.IsCancelling() {
		job.State = perceptron.JobStateRunning
	}

	switch job.Type {
	case perceptron.CausetActionCreateSchema:
		ver, err = onCreateSchema(d, t, job)
	case perceptron.CausetActionModifySchemaCharsetAndDefCauslate:
		ver, err = onModifySchemaCharsetAndDefCauslate(t, job)
	case perceptron.CausetActionDropSchema:
		ver, err = onDropSchema(t, job)
	case perceptron.CausetActionCreateBlock:
		ver, err = onCreateBlock(d, t, job)
	case perceptron.CausetActionRepairBlock:
		ver, err = onRepairBlock(d, t, job)
	case perceptron.CausetActionCreateView:
		ver, err = onCreateView(d, t, job)
	case perceptron.CausetActionDropBlock, perceptron.CausetActionDropView, perceptron.CausetActionDropSequence:
		ver, err = onDropBlockOrView(t, job)
	case perceptron.CausetActionDropBlockPartition:
		ver, err = onDropBlockPartition(t, job)
	case perceptron.CausetActionTruncateBlockPartition:
		ver, err = onTruncateBlockPartition(d, t, job)
	case perceptron.CausetActionExchangeBlockPartition:
		ver, err = w.onExchangeBlockPartition(d, t, job)
	case perceptron.CausetActionAddDeferredCauset:
		ver, err = onAddDeferredCauset(d, t, job)
	case perceptron.CausetActionAddDeferredCausets:
		ver, err = onAddDeferredCausets(d, t, job)
	case perceptron.CausetActionDropDeferredCauset:
		ver, err = onDropDeferredCauset(t, job)
	case perceptron.CausetActionDropDeferredCausets:
		ver, err = onDropDeferredCausets(t, job)
	case perceptron.CausetActionModifyDeferredCauset:
		ver, err = w.onModifyDeferredCauset(d, t, job)
	case perceptron.CausetActionSetDefaultValue:
		ver, err = onSetDefaultValue(t, job)
	case perceptron.CausetActionAddIndex:
		ver, err = w.onCreateIndex(d, t, job, false)
	case perceptron.CausetActionAddPrimaryKey:
		ver, err = w.onCreateIndex(d, t, job, true)
	case perceptron.CausetActionDropIndex, perceptron.CausetActionDropPrimaryKey:
		ver, err = onDropIndex(t, job)
	case perceptron.CausetActionRenameIndex:
		ver, err = onRenameIndex(t, job)
	case perceptron.CausetActionAddForeignKey:
		ver, err = onCreateForeignKey(t, job)
	case perceptron.CausetActionDropForeignKey:
		ver, err = onDropForeignKey(t, job)
	case perceptron.CausetActionTruncateBlock:
		ver, err = onTruncateBlock(d, t, job)
	case perceptron.CausetActionRebaseAutoID:
		ver, err = onRebaseRowIDType(d.causetstore, t, job)
	case perceptron.CausetActionRebaseAutoRandomBase:
		ver, err = onRebaseAutoRandomType(d.causetstore, t, job)
	case perceptron.CausetActionRenameBlock:
		ver, err = onRenameBlock(d, t, job)
	case perceptron.CausetActionShardRowID:
		ver, err = w.onShardRowID(d, t, job)
	case perceptron.CausetActionModifyBlockComment:
		ver, err = onModifyBlockComment(t, job)
	case perceptron.CausetActionModifyBlockAutoIdCache:
		ver, err = onModifyBlockAutoIDCache(t, job)
	case perceptron.CausetActionAddBlockPartition:
		ver, err = onAddBlockPartition(d, t, job)
	case perceptron.CausetActionModifyBlockCharsetAndDefCauslate:
		ver, err = onModifyBlockCharsetAndDefCauslate(t, job)
	case perceptron.CausetActionRecoverBlock:
		ver, err = w.onRecoverBlock(d, t, job)
	case perceptron.CausetActionLockBlock:
		ver, err = onLockBlocks(t, job)
	case perceptron.CausetActionUnlockBlock:
		ver, err = onUnlockBlocks(t, job)
	case perceptron.CausetActionSetTiFlashReplica:
		ver, err = w.onSetBlockFlashReplica(t, job)
	case perceptron.CausetActionUFIDelateTiFlashReplicaStatus:
		ver, err = onUFIDelateFlashReplicaStatus(t, job)
	case perceptron.CausetActionCreateSequence:
		ver, err = onCreateSequence(d, t, job)
	case perceptron.CausetActionAlterIndexVisibility:
		ver, err = onAlterIndexVisibility(t, job)
	case perceptron.CausetActionAlterBlockAlterPartition:
		ver, err = onAlterBlockPartition(t, job)
	default:
		// Invalid job, cancel it.
		job.State = perceptron.JobStateCancelled
		err = errInvalidDBSJob.GenWithStack("invalid dbs job type: %v", job.Type)
	}

	// Save errors in job, so that others can know errors happened.
	if err != nil {
		job.Error = toTError(err)
		job.ErrorCount++

		// If job is cancelled, we shouldn't return an error and shouldn't load DBS variables.
		if job.State == perceptron.JobStateCancelled {
			logutil.Logger(w.logCtx).Info("[dbs] DBS job is cancelled normally", zap.Error(err))
			return ver, nil
		}
		logutil.Logger(w.logCtx).Error("[dbs] run DBS job error", zap.Error(err))

		// Load global dbs variables.
		if err1 := loadDBSVars(w); err1 != nil {
			logutil.Logger(w.logCtx).Error("[dbs] load DBS global variable failed", zap.Error(err1))
		}
		// Check error limit to avoid falling into an infinite loop.
		if job.ErrorCount > variable.GetDBSErrorCountLimit() && job.State == perceptron.JobStateRunning && admin.IsJobRollbackable(job) {
			logutil.Logger(w.logCtx).Warn("[dbs] DBS job error count exceed the limit, cancelling it now", zap.Int64("jobID", job.ID), zap.Int64("errorCountLimit", variable.GetDBSErrorCountLimit()))
			job.State = perceptron.JobStateCancelling
		}
	}
	return
}

func loadDBSVars(w *worker) error {
	// Get stochastikctx from context resource pool.
	var ctx stochastikctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)
	return soliton.LoadDBSVars(ctx)
}

func toTError(err error) *terror.Error {
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	if ok {
		return tErr
	}

	// TODO: Add the error code.
	return terror.ClassDBS.Synthesize(terror.CodeUnknown, err.Error())
}

// waitSchemaChanged waits for the completion of uFIDelating all servers' schemaReplicant. In order to make sure that happens,
// we wait 2 * lease time.
func (w *worker) waitSchemaChanged(ctx context.Context, d *dbsCtx, waitTime time.Duration, latestSchemaVersion int64, job *perceptron.Job) {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return
	}
	if waitTime == 0 {
		return
	}

	timeStart := time.Now()
	var err error
	defer func() {
		metrics.DBSWorkerHistogram.WithLabelValues(metrics.WorkerWaitSchemaChanged, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()

	if latestSchemaVersion == 0 {
		logutil.Logger(w.logCtx).Info("[dbs] schemaReplicant version doesn't change")
		return
	}

	err = d.schemaSyncer.OwnerUFIDelateGlobalVersion(ctx, latestSchemaVersion)
	if err != nil {
		logutil.Logger(w.logCtx).Info("[dbs] uFIDelate latest schemaReplicant version failed", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			// If err is context.DeadlineExceeded, it means waitTime(2 * lease) is elapsed. So all the schemas are synced by ticker.
			// There is no need to use etcd to sync. The function returns directly.
			return
		}
	}

	// OwnerCheckAllVersions returns only when context is timeout(2 * lease) or all MilevaDB schemas are synced.
	err = d.schemaSyncer.OwnerCheckAllVersions(ctx, latestSchemaVersion)
	if err != nil {
		logutil.Logger(w.logCtx).Info("[dbs] wait latest schemaReplicant version to deadline", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			return
		}
		d.schemaSyncer.NotifyCleanExpiredPaths()
		// Wait until timeout.
		select {
		case <-ctx.Done():
			return
		}
	}
	logutil.Logger(w.logCtx).Info("[dbs] wait latest schemaReplicant version changed",
		zap.Int64("ver", latestSchemaVersion),
		zap.Duration("take time", time.Since(timeStart)),
		zap.String("job", job.String()))
}

// waitSchemaSynced handles the following situation:
// If the job enters a new state, and the worker crashs when it's in the process of waiting for 2 * lease time,
// Then the worker restarts quickly, we may run the job immediately again,
// but in this case we don't wait enough 2 * lease time to let other servers uFIDelate the schemaReplicant.
// So here we get the latest schemaReplicant version to make sure all servers' schemaReplicant version uFIDelate to the latest schemaReplicant version
// in a cluster, or to wait for 2 * lease time.
func (w *worker) waitSchemaSynced(d *dbsCtx, job *perceptron.Job, waitTime time.Duration) {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return
	}
	ctx, cancelFunc := context.WithTimeout(w.ctx, waitTime)
	defer cancelFunc()

	latestSchemaVersion, err := d.schemaSyncer.MustGetGlobalVersion(ctx)
	if err != nil {
		logutil.Logger(w.logCtx).Warn("[dbs] get global version failed", zap.Error(err))
		return
	}
	w.waitSchemaChanged(ctx, d, waitTime, latestSchemaVersion, job)
}

// uFIDelateSchemaVersion increments the schemaReplicant version by 1 and sets SchemaDiff.
func uFIDelateSchemaVersion(t *meta.Meta, job *perceptron.Job) (int64, error) {
	schemaVersion, err := t.GenSchemaVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	diff := &perceptron.SchemaDiff{
		Version:  schemaVersion,
		Type:     job.Type,
		SchemaID: job.SchemaID,
	}
	switch job.Type {
	case perceptron.CausetActionTruncateBlock:
		// Truncate block has two block ID, should be handled differently.
		err = job.DecodeArgs(&diff.BlockID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.OldBlockID = job.BlockID
	case perceptron.CausetActionCreateView:
		tbInfo := &perceptron.BlockInfo{}
		var orReplace bool
		var oldTbInfoID int64
		if err := job.DecodeArgs(tbInfo, &orReplace, &oldTbInfoID); err != nil {
			return 0, errors.Trace(err)
		}
		// When the statement is "create or replace view " and we need to drop the old view,
		// it has two block IDs and should be handled differently.
		if oldTbInfoID > 0 && orReplace {
			diff.OldBlockID = oldTbInfoID
		}
		diff.BlockID = tbInfo.ID
	case perceptron.CausetActionRenameBlock:
		err = job.DecodeArgs(&diff.OldSchemaID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.BlockID = job.BlockID
	case perceptron.CausetActionExchangeBlockPartition:
		var (
			ptSchemaID int64
			ptBlockID  int64
		)
		err = job.DecodeArgs(&diff.BlockID, &ptSchemaID, &ptBlockID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.OldBlockID = job.BlockID
		affects := make([]*perceptron.AffectedOption, 1)
		affects[0] = &perceptron.AffectedOption{
			SchemaID:   ptSchemaID,
			BlockID:    ptBlockID,
			OldBlockID: ptBlockID,
		}
		diff.AffectedOpts = affects
	default:
		diff.BlockID = job.BlockID
	}
	err = t.SetSchemaDiff(diff)
	return schemaVersion, errors.Trace(err)
}

func isChanClosed(quitCh <-chan struct{}) bool {
	select {
	case <-quitCh:
		return true
	default:
		return false
	}
}
