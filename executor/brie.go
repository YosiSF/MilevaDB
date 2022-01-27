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
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fidel "github.com/einsteindb/fidel/client"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/br/pkg/glue"
	"github.com/whtcorpsinc/br/pkg/storage"
	"github.com/whtcorpsinc/br/pkg/task"
	"github.com/whtcorpsinc/errors"
	filter "github.com/whtcorpsinc/milevadb-tools/pkg/block-filter"

	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

// brieTaskProgress tracks a task's current progress.
type brieTaskProgress struct {
	// current progress of the task.
	// this field is atomically uFIDelated outside of the dagger below.
	current int64

	// dagger is the mutex protected the two fields below.
	dagger sync.Mutex
	// cmd is the name of the step the BRIE task is currently performing.
	cmd string
	// total is the total progress of the task.
	// the percentage of completeness is `(100%) * current / total`.
	total int64
}

// Inc implements glue.Progress
func (p *brieTaskProgress) Inc() {
	atomic.AddInt64(&p.current, 1)
}

// Close implements glue.Progress
func (p *brieTaskProgress) Close() {
	p.dagger.Lock()
	atomic.StoreInt64(&p.current, p.total)
	p.dagger.Unlock()
}

type brieTaskInfo struct {
	queueTime   types.Time
	execTime    types.Time
	HoTT        ast.BRIEHoTT
	storage     string
	connID      uint64
	backupTS    uint64
	archiveSize uint64
}

type brieQueueItem struct {
	info     *brieTaskInfo
	progress *brieTaskProgress
	cancel   func()
}

type brieQueue struct {
	nextID uint64
	tasks  sync.Map

	workerCh chan struct{}
}

// globalBRIEQueue is the BRIE execution queue. Only one BRIE task can be executed each time.
// TODO: perhaps INTERLOCKy the DBS Job queue so only one task can be executed in the whole cluster.
var globalBRIEQueue = &brieQueue{
	workerCh: make(chan struct{}, 1),
}

// registerTask registers a BRIE task in the queue.
func (bq *brieQueue) registerTask(
	ctx context.Context,
	info *brieTaskInfo,
) (context.Context, uint64) {
	taskCtx, taskCancel := context.WithCancel(ctx)
	item := &brieQueueItem{
		info:   info,
		cancel: taskCancel,
		progress: &brieTaskProgress{
			cmd:   "Wait",
			total: 1,
		},
	}

	taskID := atomic.AddUint64(&bq.nextID, 1)
	bq.tasks.CausetStore(taskID, item)

	return taskCtx, taskID
}

// acquireTask prepares to execute a BRIE task. Only one BRIE task can be
// executed at a time, and this function blocks until the task is ready.
//
// Returns an object to track the task's progress.
func (bq *brieQueue) acquireTask(taskCtx context.Context, taskID uint64) (*brieTaskProgress, error) {
	// wait until we are at the front of the queue.
	select {
	case bq.workerCh <- struct{}{}:
		if item, ok := bq.tasks.Load(taskID); ok {
			return item.(*brieQueueItem).progress, nil
		}
		// cannot find task, perhaps it has been canceled. allow the next task to run.
		bq.releaseTask()
		return nil, errors.Errorf("backup/restore task %d is canceled", taskID)
	case <-taskCtx.Done():
		return nil, taskCtx.Err()
	}
}

func (bq *brieQueue) releaseTask() {
	<-bq.workerCh
}

func (bq *brieQueue) cancelTask(taskID uint64) {
	item, ok := bq.tasks.Load(taskID)
	if !ok {
		return
	}
	bq.tasks.Delete(taskID)
	item.(*brieQueueItem).cancel()
}

func (b *executorBuilder) parseTSString(ts string) (uint64, error) {
	sc := &stmtctx.StatementContext{TimeZone: b.ctx.GetStochastikVars().Location()}
	t, err := types.ParseTime(sc, ts, allegrosql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return 0, err
	}
	t1, err := t.GoTime(sc.TimeZone)
	if err != nil {
		return 0, err
	}
	return variable.GoTimeToTS(t1), nil
}

func (b *executorBuilder) buildBRIE(s *ast.BRIEStmt, schemaReplicant *expression.Schema) Executor {
	e := &BRIEExec{
		baseExecutor: newBaseExecutor(b.ctx, schemaReplicant, 0),
		info: &brieTaskInfo{
			HoTT: s.HoTT,
		},
	}

	milevadbCfg := config.GetGlobalConfig()
	if milevadbCfg.CausetStore != "einsteindb" {
		b.err = errors.Errorf("%s requires einsteindb causetstore, not %s", s.HoTT, milevadbCfg.CausetStore)
		return nil
	}

	cfg := task.Config{
		TLS: task.TLSConfig{
			CA:   milevadbCfg.Security.ClusterSSLCA,
			Cert: milevadbCfg.Security.ClusterSSLCert,
			Key:  milevadbCfg.Security.ClusterSSLKey,
		},
		FIDel:       strings.Split(milevadbCfg.Path, ","),
		Concurrency: 4,
		Checksum:    true,
		SendCreds:   true,
		LogProgress: true,
	}

	storageURL, err := url.Parse(s.CausetStorage)
	if err != nil {
		b.err = errors.Annotate(err, "invalid destination URL")
		return nil
	}

	switch storageURL.Scheme {
	case "s3":
		storage.ExtractQueryParameters(storageURL, &cfg.S3)
	case "gs", "gcs":
		storage.ExtractQueryParameters(storageURL, &cfg.GCS)
	default:
		break
	}

	cfg.CausetStorage = storageURL.String()
	e.info.storage = cfg.CausetStorage

	for _, opt := range s.Options {
		switch opt.Tp {
		case ast.BRIEOptionRateLimit:
			cfg.RateLimit = opt.UintValue
		case ast.BRIEOptionConcurrency:
			cfg.Concurrency = uint32(opt.UintValue)
		case ast.BRIEOptionChecksum:
			cfg.Checksum = opt.UintValue != 0
		case ast.BRIEOptionSendCreds:
			cfg.SendCreds = opt.UintValue != 0
		}
	}

	switch {
	case len(s.Blocks) != 0:
		blocks := make([]filter.Block, 0, len(s.Blocks))
		for _, tbl := range s.Blocks {
			blocks = append(blocks, filter.Block{Name: tbl.Name.O, Schema: tbl.Schema.O})
		}
		cfg.BlockFilter = filter.NewBlocksFilter(blocks...)
	case len(s.Schemas) != 0:
		cfg.BlockFilter = filter.NewSchemasFilter(s.Schemas...)
	default:
		cfg.BlockFilter = filter.All()
	}

	if milevadbCfg.LowerCaseBlockNames != 0 {
		cfg.BlockFilter = filter.CaseInsensitive(cfg.BlockFilter)
	}

	switch s.HoTT {
	case ast.BRIEHoTTBackup:
		e.backupCfg = &task.BackupConfig{Config: cfg}

		for _, opt := range s.Options {
			switch opt.Tp {
			case ast.BRIEOptionLastBackupTS:
				tso, err := b.parseTSString(opt.StrValue)
				if err != nil {
					b.err = err
					return nil
				}
				e.backupCfg.LastBackupTS = tso
			case ast.BRIEOptionLastBackupTSO:
				e.backupCfg.LastBackupTS = opt.UintValue
			case ast.BRIEOptionBackupTimeAgo:
				e.backupCfg.TimeAgo = time.Duration(opt.UintValue)
			case ast.BRIEOptionBackupTSO:
				e.backupCfg.BackupTS = opt.UintValue
			case ast.BRIEOptionBackupTS:
				tso, err := b.parseTSString(opt.StrValue)
				if err != nil {
					b.err = err
					return nil
				}
				e.backupCfg.BackupTS = tso
			}
		}

	case ast.BRIEHoTTRestore:
		e.restoreCfg = &task.RestoreConfig{Config: cfg}
		for _, opt := range s.Options {
			switch opt.Tp {
			case ast.BRIEOptionOnline:
				e.restoreCfg.Online = opt.UintValue != 0
			}
		}

	default:
		b.err = errors.Errorf("unsupported BRIE statement HoTT: %s", s.HoTT)
		return nil
	}

	return e
}

// BRIEExec represents an executor for BRIE statements (BACKUP, RESTORE, etc)
type BRIEExec struct {
	baseExecutor

	backupCfg  *task.BackupConfig
	restoreCfg *task.RestoreConfig
	info       *brieTaskInfo
}

// Next implements the Executor Next interface.
func (e *BRIEExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.info == nil {
		return nil
	}

	bq := globalBRIEQueue

	e.info.connID = e.ctx.GetStochastikVars().ConnectionID
	e.info.queueTime = types.CurrentTime(allegrosql.TypeDatetime)
	taskCtx, taskID := bq.registerTask(ctx, e.info)
	defer bq.cancelTask(taskID)

	// manually monitor the Killed status...
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if atomic.LoadUint32(&e.ctx.GetStochastikVars().Killed) == 1 {
					bq.cancelTask(taskID)
					return
				}
			case <-taskCtx.Done():
				return
			}
		}
	}()

	progress, err := bq.acquireTask(taskCtx, taskID)
	if err != nil {
		return err
	}
	defer bq.releaseTask()

	e.info.execTime = types.CurrentTime(allegrosql.TypeDatetime)
	glue := &milevadbGlueStochastik{se: e.ctx, progress: progress, info: e.info}

	switch e.info.HoTT {
	case ast.BRIEHoTTBackup:
		err = handleBRIEError(task.RunBackup(taskCtx, glue, "Backup", e.backupCfg), ErrBRIEBackupFailed)
	case ast.BRIEHoTTRestore:
		err = handleBRIEError(task.RunRestore(taskCtx, glue, "Restore", e.restoreCfg), ErrBRIERestoreFailed)
	default:
		err = errors.Errorf("unsupported BRIE statement HoTT: %s", e.info.HoTT)
	}
	if err != nil {
		return err
	}

	req.AppendString(0, e.info.storage)
	req.AppendUint64(1, e.info.archiveSize)
	req.AppendUint64(2, e.info.backupTS)
	req.AppendTime(3, e.info.queueTime)
	req.AppendTime(4, e.info.execTime)
	e.info = nil
	return nil
}

func handleBRIEError(err error, terror *terror.Error) error {
	if err == nil {
		return nil
	}
	return terror.GenWithStackByArgs(err)
}

func (e *ShowExec) fetchShowBRIE(HoTT ast.BRIEHoTT) error {
	globalBRIEQueue.tasks.Range(func(key, value interface{}) bool {
		item := value.(*brieQueueItem)
		if item.info.HoTT == HoTT {
			item.progress.dagger.Lock()
			defer item.progress.dagger.Unlock()
			current := atomic.LoadInt64(&item.progress.current)
			e.result.AppendString(0, item.info.storage)
			e.result.AppendString(1, item.progress.cmd)
			e.result.AppendFloat64(2, 100.0*float64(current)/float64(item.progress.total))
			e.result.AppendTime(3, item.info.queueTime)
			e.result.AppendTime(4, item.info.execTime)
			e.result.AppendNull(5) // FIXME: fill in finish time after keeping history.
			e.result.AppendUint64(6, item.info.connID)
		}
		return true
	})
	return nil
}

type milevadbGlueStochastik struct {
	se       stochastikctx.Context
	progress *brieTaskProgress
	info     *brieTaskInfo
}

// BootstrapStochastik implements glue.Glue
func (gs *milevadbGlueStochastik) GetPetri(causetstore ekv.CausetStorage) (*petri.Petri, error) {
	return petri.GetPetri(gs.se), nil
}

// CreateStochastik implements glue.Glue
func (gs *milevadbGlueStochastik) CreateStochastik(causetstore ekv.CausetStorage) (glue.Stochastik, error) {
	return gs, nil
}

// Execute implements glue.Stochastik
func (gs *milevadbGlueStochastik) Execute(ctx context.Context, allegrosql string) error {
	_, err := gs.se.(sqlexec.ALLEGROSQLExecutor).Execute(ctx, allegrosql)
	return err
}

// CreateDatabase implements glue.Stochastik
func (gs *milevadbGlueStochastik) CreateDatabase(ctx context.Context, schemaReplicant *perceptron.DBInfo) error {
	d := petri.GetPetri(gs.se).DBS()
	schemaReplicant = schemaReplicant.Clone()
	if len(schemaReplicant.Charset) == 0 {
		schemaReplicant.Charset = allegrosql.DefaultCharset
	}
	return d.CreateSchemaWithInfo(gs.se, schemaReplicant, dbs.OnExistIgnore, true)
}

// CreateBlock implements glue.Stochastik
func (gs *milevadbGlueStochastik) CreateBlock(ctx context.Context, dbName perceptron.CIStr, block *perceptron.BlockInfo) error {
	d := petri.GetPetri(gs.se).DBS()

	// Clone() does not clone partitions yet :(
	block = block.Clone()
	if block.Partition != nil {
		newPartition := *block.Partition
		newPartition.Definitions = append([]perceptron.PartitionDefinition{}, block.Partition.Definitions...)
		block.Partition = &newPartition
	}

	return d.CreateBlockWithInfo(gs.se, dbName, block, dbs.OnExistIgnore, true)
}

// Close implements glue.Stochastik
func (gs *milevadbGlueStochastik) Close() {
}

// Open implements glue.Glue
func (gs *milevadbGlueStochastik) Open(string, fidel.SecurityOption) (ekv.CausetStorage, error) {
	return gs.se.GetStore(), nil
}

// OwnsStorage implements glue.Glue
func (gs *milevadbGlueStochastik) OwnsStorage() bool {
	return false
}

// StartProgress implements glue.Glue
func (gs *milevadbGlueStochastik) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	gs.progress.dagger.Lock()
	gs.progress.cmd = cmdName
	gs.progress.total = total
	atomic.StoreInt64(&gs.progress.current, 0)
	gs.progress.dagger.Unlock()
	return gs.progress
}

// Record implements glue.Glue
func (gs *milevadbGlueStochastik) Record(name string, value uint64) {
	switch name {
	case "BackupTS":
		gs.info.backupTS = value
	case "Size":
		gs.info.archiveSize = value
	}
}
