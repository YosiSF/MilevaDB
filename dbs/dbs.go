// INTERLOCKyright 2020 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	pumpcli "github.com/whtcorpsinc/milevadb-tools/milevadb-binlog/pump_client"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs/soliton"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/owner"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	goutil "github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"go.uber.org/zap"
)

const (
	// currentVersion is for all new DBS jobs.
	currentVersion = 1
	// DBSOwnerKey is the dbs owner path that is saved to etcd, and it's exported for testing.
	DBSOwnerKey = "/milevadb/dbs/fg/owner"
	dbsPrompt   = "dbs"

	shardRowIDBitsMax = 15

	batchAddingJobs = 10

	// PartitionCountLimit is limit of the number of partitions in a block.
	// Reference linking https://dev.allegrosql.com/doc/refman/5.7/en/partitioning-limitations.html.
	PartitionCountLimit = 8192
)

// OnExist specifies what to do when a new object has a name defCauslision.
type OnExist uint8

const (
	// OnExistError throws an error on name defCauslision.
	OnExistError OnExist = iota
	// OnExistIgnore skips creating the new object.
	OnExistIgnore
	// OnExistReplace replaces the old object by the new object. This is only
	// supported by VIEWs at the moment. For other object types, this is
	// equivalent to OnExistError.
	OnExistReplace
)

var (
	// BlockDeferredCausetCountLimit is limit of the number of defCausumns in a block.
	// It's exported for testing.
	BlockDeferredCausetCountLimit = uint32(512)
	// EnableSplitBlockRegion is a flag to decide whether to split a new region for
	// a newly created block. It takes effect only if the CausetStorage supports split
	// region.
	EnableSplitBlockRegion = uint32(0)
)

// DBS is responsible for uFIDelating schemaReplicant in data causetstore and maintaining in-memory SchemaReplicant cache.
type DBS interface {
	CreateSchema(ctx stochastikctx.Context, name perceptron.CIStr, charsetInfo *ast.CharsetOpt) error
	AlterSchema(ctx stochastikctx.Context, stmt *ast.AlterDatabaseStmt) error
	DropSchema(ctx stochastikctx.Context, schemaReplicant perceptron.CIStr) error
	CreateBlock(ctx stochastikctx.Context, stmt *ast.CreateBlockStmt) error
	CreateView(ctx stochastikctx.Context, stmt *ast.CreateViewStmt) error
	DropBlock(ctx stochastikctx.Context, blockIdent ast.Ident) (err error)
	RecoverBlock(ctx stochastikctx.Context, recoverInfo *RecoverInfo) (err error)
	DropView(ctx stochastikctx.Context, blockIdent ast.Ident) (err error)
	CreateIndex(ctx stochastikctx.Context, blockIdent ast.Ident, keyType ast.IndexKeyType, indexName perceptron.CIStr,
		defCausumnNames []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error
	DropIndex(ctx stochastikctx.Context, blockIdent ast.Ident, indexName perceptron.CIStr, ifExists bool) error
	AlterBlock(ctx stochastikctx.Context, blockIdent ast.Ident, spec []*ast.AlterBlockSpec) error
	TruncateBlock(ctx stochastikctx.Context, blockIdent ast.Ident) error
	RenameBlock(ctx stochastikctx.Context, oldBlockIdent, newBlockIdent ast.Ident, isAlterBlock bool) error
	LockBlocks(ctx stochastikctx.Context, stmt *ast.LockBlocksStmt) error
	UnlockBlocks(ctx stochastikctx.Context, lockedBlocks []perceptron.BlockLockTpInfo) error
	CleanupBlockLock(ctx stochastikctx.Context, blocks []*ast.BlockName) error
	UFIDelateBlockReplicaInfo(ctx stochastikctx.Context, physicalID int64, available bool) error
	RepairBlock(ctx stochastikctx.Context, block *ast.BlockName, createStmt *ast.CreateBlockStmt) error
	CreateSequence(ctx stochastikctx.Context, stmt *ast.CreateSequenceStmt) error
	DropSequence(ctx stochastikctx.Context, blockIdent ast.Ident, ifExists bool) (err error)

	// CreateSchemaWithInfo creates a database (schemaReplicant) given its database info.
	//
	// If `tryRetainID` is true, this method will try to keep the database ID specified in
	// the `info` rather than generating new ones. This is just a hint though, if the ID defCauslides
	// with an existing database a new ID will always be used.
	//
	// WARNING: the DBS owns the `info` after calling this function, and will modify its fields
	// in-place. If you want to keep using `info`, please call Clone() first.
	CreateSchemaWithInfo(
		ctx stochastikctx.Context,
		info *perceptron.DBInfo,
		onExist OnExist,
		tryRetainID bool) error

	// CreateBlockWithInfo creates a block, view or sequence given its block info.
	//
	// If `tryRetainID` is true, this method will try to keep the block ID specified in the `info`
	// rather than generating new ones. This is just a hint though, if the ID defCauslides with an
	// existing block a new ID will always be used.
	//
	// WARNING: the DBS owns the `info` after calling this function, and will modify its fields
	// in-place. If you want to keep using `info`, please call Clone() first.
	CreateBlockWithInfo(
		ctx stochastikctx.Context,
		schemaReplicant perceptron.CIStr,
		info *perceptron.BlockInfo,
		onExist OnExist,
		tryRetainID bool) error

	// Start campaigns the owner and starts workers.
	// ctxPool is used for the worker's delRangeManager and creates stochastik.
	Start(ctxPool *pools.ResourcePool) error
	// GetLease returns current schemaReplicant lease time.
	GetLease() time.Duration
	// Stats returns the DBS statistics.
	Stats(vars *variable.StochastikVars) (map[string]interface{}, error)
	// GetSINTERLOCKe gets the status variables sINTERLOCKe.
	GetSINTERLOCKe(status string) variable.SINTERLOCKeFlag
	// Stop stops DBS worker.
	Stop() error
	// RegisterEventCh registers event channel for dbs.
	RegisterEventCh(chan<- *soliton.Event)
	// SchemaSyncer gets the schemaReplicant syncer.
	SchemaSyncer() soliton.SchemaSyncer
	// OwnerManager gets the owner manager.
	OwnerManager() owner.Manager
	// GetID gets the dbs ID.
	GetID() string
	// GetBlockMaxRowID gets the max event ID of a normal block or a partition.
	GetBlockMaxHandle(startTS uint64, tbl block.PhysicalBlock) (ekv.Handle, bool, error)
	// SetBinlogClient sets the binlog client for DBS worker. It's exported for testing.
	SetBinlogClient(*pumpcli.PumpsClient)
	// GetHook gets the hook. It's exported for testing.
	GetHook() Callback
}

type limitJobTask struct {
	job *perceptron.Job
	err chan error
}

// dbs is used to handle the statements that define the structure or schemaReplicant of the database.
type dbs struct {
	m          sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup // It's only used to deal with data race in restart_test.
	limitJobCh chan *limitJobTask

	*dbsCtx
	workers     map[workerType]*worker
	sessPool    *stochastikPool
	delRangeMgr delRangeManager
}

// dbsCtx is the context when we use worker to handle DBS jobs.
type dbsCtx struct {
	uuid         string
	causetstore  ekv.CausetStorage
	ownerManager owner.Manager
	schemaSyncer soliton.SchemaSyncer
	dbsJobDoneCh chan struct{}
	dbsEventCh   chan<- *soliton.Event
	lease        time.Duration        // lease is schemaReplicant lease.
	binlogCli    *pumpcli.PumpsClient // binlogCli is used for Binlog.
	infoHandle   *schemareplicant.Handle
	blockLockCkr soliton.DeadBlockLockChecker

	// hook may be modified.
	mu struct {
		sync.RWMutex
		hook        Callback
		interceptor Interceptor
	}
}

func (dc *dbsCtx) isOwner() bool {
	isOwner := dc.ownerManager.IsOwner()
	logutil.BgLogger().Debug("[dbs] check whether is the DBS owner", zap.Bool("isOwner", isOwner), zap.String("selfID", dc.uuid))
	if isOwner {
		metrics.DBSCounter.WithLabelValues(metrics.DBSOwner + "_" + allegrosql.MilevaDBReleaseVersion).Inc()
	}
	return isOwner
}

// RegisterEventCh registers passed channel for dbs Event.
func (d *dbs) RegisterEventCh(ch chan<- *soliton.Event) {
	d.dbsEventCh = ch
}

// asyncNotifyEvent will notify the dbs event to outside world, say statistic handle. When the channel is full, we may
// give up notify and log it.
func asyncNotifyEvent(d *dbsCtx, e *soliton.Event) {
	if d.dbsEventCh != nil {
		if d.lease == 0 {
			// If lease is 0, it's always used in test.
			select {
			case d.dbsEventCh <- e:
			default:
			}
			return
		}
		for i := 0; i < 10; i++ {
			select {
			case d.dbsEventCh <- e:
				return
			default:
				logutil.BgLogger().Warn("[dbs] fail to notify DBS event", zap.String("event", e.String()))
				time.Sleep(time.Microsecond * 10)
			}
		}
	}
}

// NewDBS creates a new DBS.
func NewDBS(ctx context.Context, options ...Option) DBS {
	return newDBS(ctx, options...)
}

func newDBS(ctx context.Context, options ...Option) *dbs {
	opt := &Options{
		Hook: &BaseCallback{},
	}
	for _, o := range options {
		o(opt)
	}

	id := uuid.New().String()
	var manager owner.Manager
	var syncer soliton.SchemaSyncer
	var deadLockCkr soliton.DeadBlockLockChecker
	if etcdCli := opt.EtcdCli; etcdCli == nil {
		// The etcdCli is nil if the causetstore is localstore which is only used for testing.
		// So we use mockOwnerManager and MockSchemaSyncer.
		manager = owner.NewMockManager(ctx, id)
		syncer = NewMockSchemaSyncer()
	} else {
		manager = owner.NewOwnerManager(ctx, etcdCli, dbsPrompt, id, DBSOwnerKey)
		syncer = soliton.NewSchemaSyncer(ctx, etcdCli, id, manager)
		deadLockCkr = soliton.NewDeadBlockLockChecker(etcdCli)
	}

	dbsCtx := &dbsCtx{
		uuid:         id,
		causetstore:  opt.CausetStore,
		lease:        opt.Lease,
		dbsJobDoneCh: make(chan struct{}, 1),
		ownerManager: manager,
		schemaSyncer: syncer,
		binlogCli:    binloginfo.GetPumpsClient(),
		infoHandle:   opt.InfoHandle,
		blockLockCkr: deadLockCkr,
	}
	dbsCtx.mu.hook = opt.Hook
	dbsCtx.mu.interceptor = &BaseInterceptor{}
	d := &dbs{
		ctx:        ctx,
		dbsCtx:     dbsCtx,
		limitJobCh: make(chan *limitJobTask, batchAddingJobs),
	}

	return d
}

// Stop implements DBS.Stop interface.
func (d *dbs) Stop() error {
	d.m.Lock()
	defer d.m.Unlock()

	d.close()
	logutil.BgLogger().Info("[dbs] stop DBS", zap.String("ID", d.uuid))
	return nil
}

func (d *dbs) newDeleteRangeManager(mock bool) delRangeManager {
	var delRangeMgr delRangeManager
	if !mock {
		delRangeMgr = newDelRangeManager(d.causetstore, d.sessPool)
		logutil.BgLogger().Info("[dbs] start delRangeManager OK", zap.Bool("is a emulator", !d.causetstore.SupportDeleteRange()))
	} else {
		delRangeMgr = newMockDelRangeManager()
	}

	delRangeMgr.start()
	return delRangeMgr
}

// Start implements DBS.Start interface.
func (d *dbs) Start(ctxPool *pools.ResourcePool) error {
	logutil.BgLogger().Info("[dbs] start DBS", zap.String("ID", d.uuid), zap.Bool("runWorker", RunWorker))
	d.ctx, d.cancel = context.WithCancel(d.ctx)

	d.wg.Add(1)
	go d.limitDBSJobs()

	// If RunWorker is true, we need campaign owner and do DBS job.
	// Otherwise, we needn't do that.
	if RunWorker {
		err := d.ownerManager.CampaignOwner()
		if err != nil {
			return errors.Trace(err)
		}

		d.workers = make(map[workerType]*worker, 2)
		d.sessPool = newStochastikPool(ctxPool)
		d.delRangeMgr = d.newDeleteRangeManager(ctxPool == nil)
		d.workers[generalWorker] = newWorker(d.ctx, generalWorker, d.sessPool, d.delRangeMgr)
		d.workers[addIdxWorker] = newWorker(d.ctx, addIdxWorker, d.sessPool, d.delRangeMgr)
		for _, worker := range d.workers {
			worker.wg.Add(1)
			w := worker
			go w.start(d.dbsCtx)

			metrics.DBSCounter.WithLabelValues(fmt.Sprintf("%s_%s", metrics.CreateDBS, worker.String())).Inc()

			// When the start function is called, we will send a fake job to let worker
			// checks owner firstly and try to find whether a job exists and run.
			asyncNotify(worker.dbsJobCh)
		}

		go d.schemaSyncer.StartCleanWork()
		if config.BlockLockEnabled() {
			d.wg.Add(1)
			go d.startCleanDeadBlockLock()
		}
		metrics.DBSCounter.WithLabelValues(metrics.StartCleanWork).Inc()
	}

	variable.RegisterStatistics(d)

	metrics.DBSCounter.WithLabelValues(metrics.CreateDBSInstance).Inc()
	return nil
}

func (d *dbs) close() {
	if isChanClosed(d.ctx.Done()) {
		return
	}

	startTime := time.Now()
	d.cancel()
	d.wg.Wait()
	d.ownerManager.Cancel()
	d.schemaSyncer.Close()

	for _, worker := range d.workers {
		worker.close()
	}
	// d.delRangeMgr using stochastik from d.sessPool.
	// Put it before d.sessPool.close to reduce the time spent by d.sessPool.close.
	if d.delRangeMgr != nil {
		d.delRangeMgr.clear()
	}
	if d.sessPool != nil {
		d.sessPool.close()
	}

	logutil.BgLogger().Info("[dbs] DBS closed", zap.String("ID", d.uuid), zap.Duration("take time", time.Since(startTime)))
}

// GetLease implements DBS.GetLease interface.
func (d *dbs) GetLease() time.Duration {
	d.m.RLock()
	lease := d.lease
	d.m.RUnlock()
	return lease
}

// GetSchemaReplicantWithInterceptor gets the schemareplicant binding to d. It's exported for testing.
// Please don't use this function, it is used by TestParallelDBSBeforeRunDBSJob to intercept the calling of d.infoHandle.Get(), use d.infoHandle.Get() instead.
// Otherwise, the TestParallelDBSBeforeRunDBSJob will hang up forever.
func (d *dbs) GetSchemaReplicantWithInterceptor(ctx stochastikctx.Context) schemareplicant.SchemaReplicant {
	is := d.infoHandle.Get()

	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mu.interceptor.OnGetSchemaReplicant(ctx, is)
}

func (d *dbs) genGlobalIDs(count int) ([]int64, error) {
	var ret []int64
	err := ekv.RunInNewTxn(d.causetstore, true, func(txn ekv.Transaction) error {
		failpoint.Inject("mockGenGlobalIDFail", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("gofail genGlobalIDs error"))
			}
		})

		m := meta.NewMeta(txn)
		var err error
		ret, err = m.GenGlobalIDs(count)
		return err
	})

	return ret, err
}

// SchemaSyncer implements DBS.SchemaSyncer interface.
func (d *dbs) SchemaSyncer() soliton.SchemaSyncer {
	return d.schemaSyncer
}

// OwnerManager implements DBS.OwnerManager interface.
func (d *dbs) OwnerManager() owner.Manager {
	return d.ownerManager
}

// GetID implements DBS.GetID interface.
func (d *dbs) GetID() string {
	return d.uuid
}

func checkJobMaxInterval(job *perceptron.Job) time.Duration {
	// The job of adding index takes more time to process.
	// So it uses the longer time.
	if job.Type == perceptron.CausetActionAddIndex || job.Type == perceptron.CausetActionAddPrimaryKey {
		return 3 * time.Second
	}
	if job.Type == perceptron.CausetActionCreateBlock || job.Type == perceptron.CausetActionCreateSchema {
		return 500 * time.Millisecond
	}
	return 1 * time.Second
}

func (d *dbs) asyncNotifyWorker(jobTp perceptron.CausetActionType) {
	// If the workers don't run, we needn't to notify workers.
	if !RunWorker {
		return
	}

	if jobTp == perceptron.CausetActionAddIndex || jobTp == perceptron.CausetActionAddPrimaryKey {
		asyncNotify(d.workers[addIdxWorker].dbsJobCh)
	} else {
		asyncNotify(d.workers[generalWorker].dbsJobCh)
	}
}

func (d *dbs) doDBSJob(ctx stochastikctx.Context, job *perceptron.Job) error {
	if isChanClosed(d.ctx.Done()) {
		return d.ctx.Err()
	}

	// Get a global job ID and put the DBS job in the queue.
	job.Query, _ = ctx.Value(stochastikctx.QueryString).(string)
	task := &limitJobTask{job, make(chan error)}
	d.limitJobCh <- task
	err := <-task.err

	ctx.GetStochastikVars().StmtCtx.IsDBSJobInQueue = true

	// Notice worker that we push a new job and wait the job done.
	d.asyncNotifyWorker(job.Type)
	logutil.BgLogger().Info("[dbs] start DBS job", zap.String("job", job.String()), zap.String("query", job.Query))

	var historyJob *perceptron.Job
	jobID := job.ID
	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 0.5s now, so we use 0.5s or 1s or 3s as the max value.
	ticker := time.NewTicker(chooseLeaseTime(10*d.lease, checkJobMaxInterval(job)))
	startTime := time.Now()
	metrics.JobsGauge.WithLabelValues(job.Type.String()).Inc()
	defer func() {
		ticker.Stop()
		metrics.JobsGauge.WithLabelValues(job.Type.String()).Dec()
		metrics.HandleJobHistogram.WithLabelValues(job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	for {
		failpoint.Inject("storeCloseInLoop", func(_ failpoint.Value) {
			d.cancel()
		})

		select {
		case <-d.dbsJobDoneCh:
		case <-ticker.C:
		case <-d.ctx.Done():
			logutil.BgLogger().Error("[dbs] doDBSJob will quit because context done", zap.Error(d.ctx.Err()))
			err := d.ctx.Err()
			return err
		}

		historyJob, err = d.getHistoryDBSJob(jobID)
		if err != nil {
			logutil.BgLogger().Error("[dbs] get history DBS job failed, check again", zap.Error(err))
			continue
		} else if historyJob == nil {
			logutil.BgLogger().Debug("[dbs] DBS job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

		// If a job is a history job, the state must be JobStateSynced or JobStateRollbackDone or JobStateCancelled.
		if historyJob.IsSynced() {
			logutil.BgLogger().Info("[dbs] DBS job is finished", zap.Int64("jobID", jobID))
			return nil
		}

		if historyJob.Error != nil {
			return errors.Trace(historyJob.Error)
		}
		// Only for JobStateCancelled job which is adding defCausumns or drop defCausumns.
		if historyJob.IsCancelled() && (historyJob.Type == perceptron.CausetActionAddDeferredCausets || historyJob.Type == perceptron.CausetActionDropDeferredCausets) {
			logutil.BgLogger().Info("[dbs] DBS job is cancelled", zap.Int64("jobID", jobID))
			return nil
		}
		panic("When the state is JobStateRollbackDone or JobStateCancelled, historyJob.Error should never be nil")
	}
}

func (d *dbs) callHookOnChanged(err error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	err = d.mu.hook.OnChanged(err)
	return errors.Trace(err)
}

// SetBinlogClient implements DBS.SetBinlogClient interface.
func (d *dbs) SetBinlogClient(binlogCli *pumpcli.PumpsClient) {
	d.binlogCli = binlogCli
}

// GetHook implements DBS.GetHook interface.
func (d *dbs) GetHook() Callback {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.mu.hook
}

func (d *dbs) startCleanDeadBlockLock() {
	defer func() {
		goutil.Recover(metrics.LabelDBS, "startCleanDeadBlockLock", nil, false)
		d.wg.Done()
	}()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !d.ownerManager.IsOwner() {
				continue
			}
			deadLockBlocks, err := d.blockLockCkr.GetDeadLockedBlocks(d.ctx, d.infoHandle.Get().AllSchemas())
			if err != nil {
				logutil.BgLogger().Info("[dbs] get dead block dagger failed.", zap.Error(err))
				continue
			}
			for se, blocks := range deadLockBlocks {
				err := d.CleanDeadBlockLock(blocks, se)
				if err != nil {
					logutil.BgLogger().Info("[dbs] clean dead block dagger failed.", zap.Error(err))
				}
			}
		case <-d.ctx.Done():
			return
		}
	}
}

// RecoverInfo contains information needed by DBS.RecoverBlock.
type RecoverInfo struct {
	SchemaID      int64
	BlockInfo     *perceptron.BlockInfo
	DropJobID     int64
	SnapshotTS    uint64
	CurAutoIncID  int64
	CurAutoRandID int64
}
