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

package petri

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ngaut/pools"
	"github.com/ngaut/sync2"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/bindinfo"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/owner"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/privilege/privileges"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/schemareplicant/perfschema"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/expensivequery"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/petriutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/telemetry"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Petri represents a storage space. Different petris can use the same database name.
// Multiple petris can be used in parallel without synchronization.
type Petri struct {
	causetstore          ekv.CausetStorage
	infoHandle           *schemareplicant.Handle
	privHandle           *privileges.Handle
	bindHandle           *bindinfo.BindHandle
	statsHandle          unsafe.Pointer
	statsLease           time.Duration
	dbs                  dbs.DBS
	info                 *infosync.InfoSyncer
	m                    sync.Mutex
	SchemaValidator      SchemaValidator
	sysStochastikPool    *stochastikPool
	exit                 chan struct{}
	etcdClient           *clientv3.Client
	gvc                  GlobalVariableCache
	slowQuery            *topNSlowQueries
	expensiveQueryHandle *expensivequery.Handle
	wg                   sync.WaitGroup
	statsUFIDelating     sync2.AtomicInt32
	cancel               context.CancelFunc
	indexUsageSyncLease  time.Duration
}

// loadSchemaReplicant loads schemareplicant at startTS into handle, usedSchemaVersion is the currently used
// schemareplicant version, if it is the same as the schemaReplicant version at startTS, we don't need to reload again.
// It returns the latest schemaReplicant version, the changed block IDs, whether it's a full load and an error.
func (do *Petri) loadSchemaReplicant(handle *schemareplicant.Handle, usedSchemaVersion int64,
	startTS uint64) (neededSchemaVersion int64, change *einsteindb.RelatedSchemaChange, fullLoad bool, err error) {
	snapshot, err := do.causetstore.GetSnapshot(ekv.NewVersion(startTS))
	if err != nil {
		return 0, nil, fullLoad, err
	}
	m := meta.NewSnapshotMeta(snapshot)
	neededSchemaVersion, err = m.GetSchemaVersion()
	if err != nil {
		return 0, nil, fullLoad, err
	}
	if usedSchemaVersion != 0 && usedSchemaVersion == neededSchemaVersion {
		return neededSchemaVersion, nil, fullLoad, nil
	}

	// UFIDelate self schemaReplicant version to etcd.
	defer func() {
		// There are two possibilities for not uFIDelating the self schemaReplicant version to etcd.
		// 1. Failed to loading schemaReplicant information.
		// 2. When users use history read feature, the neededSchemaVersion isn't the latest schemaReplicant version.
		if err != nil || neededSchemaVersion < do.SchemaReplicant().SchemaMetaVersion() {
			logutil.BgLogger().Info("do not uFIDelate self schemaReplicant version to etcd",
				zap.Int64("usedSchemaVersion", usedSchemaVersion),
				zap.Int64("neededSchemaVersion", neededSchemaVersion), zap.Error(err))
			return
		}

		err = do.dbs.SchemaSyncer().UFIDelateSelfVersion(context.Background(), neededSchemaVersion)
		if err != nil {
			logutil.BgLogger().Info("uFIDelate self version failed",
				zap.Int64("usedSchemaVersion", usedSchemaVersion),
				zap.Int64("neededSchemaVersion", neededSchemaVersion), zap.Error(err))
		}
	}()

	startTime := time.Now()
	ok, relatedChanges, err := do.tryLoadSchemaDiffs(m, usedSchemaVersion, neededSchemaVersion)
	if err != nil {
		// We can fall back to full load, don't need to return the error.
		logutil.BgLogger().Error("failed to load schemaReplicant diff", zap.Error(err))
	}
	if ok {
		logutil.BgLogger().Info("diff load SchemaReplicant success",
			zap.Int64("usedSchemaVersion", usedSchemaVersion),
			zap.Int64("neededSchemaVersion", neededSchemaVersion),
			zap.Duration("start time", time.Since(startTime)),
			zap.Int64s("phyTblIDs", relatedChanges.PhyTblIDS),
			zap.Uint64s("actionTypes", relatedChanges.CausetActionTypes))
		return neededSchemaVersion, relatedChanges, fullLoad, nil
	}

	fullLoad = true
	schemas, err := do.fetchAllSchemasWithBlocks(m)
	if err != nil {
		return 0, nil, fullLoad, err
	}

	newISBuilder, err := schemareplicant.NewBuilder(handle).InitWithDBInfos(schemas, neededSchemaVersion)
	if err != nil {
		return 0, nil, fullLoad, err
	}
	logutil.BgLogger().Info("full load SchemaReplicant success",
		zap.Int64("usedSchemaVersion", usedSchemaVersion),
		zap.Int64("neededSchemaVersion", neededSchemaVersion),
		zap.Duration("start time", time.Since(startTime)))
	newISBuilder.Build()
	return neededSchemaVersion, nil, fullLoad, nil
}

func (do *Petri) fetchAllSchemasWithBlocks(m *meta.Meta) ([]*perceptron.DBInfo, error) {
	allSchemas, err := m.ListDatabases()
	if err != nil {
		return nil, err
	}
	splittedSchemas := do.splitForConcurrentFetch(allSchemas)
	doneCh := make(chan error, len(splittedSchemas))
	for _, schemas := range splittedSchemas {
		go do.fetchSchemasWithBlocks(schemas, m, doneCh)
	}
	for range splittedSchemas {
		err = <-doneCh
		if err != nil {
			return nil, err
		}
	}
	return allSchemas, nil
}

// fetchSchemaConcurrency controls the goroutines to load schemas, but more goroutines
// increase the memory usage when calling json.Unmarshal(), which would cause OOM,
// so we decrease the concurrency.
const fetchSchemaConcurrency = 1

func (do *Petri) splitForConcurrentFetch(schemas []*perceptron.DBInfo) [][]*perceptron.DBInfo {
	groupSize := (len(schemas) + fetchSchemaConcurrency - 1) / fetchSchemaConcurrency
	splitted := make([][]*perceptron.DBInfo, 0, fetchSchemaConcurrency)
	schemaCnt := len(schemas)
	for i := 0; i < schemaCnt; i += groupSize {
		end := i + groupSize
		if end > schemaCnt {
			end = schemaCnt
		}
		splitted = append(splitted, schemas[i:end])
	}
	return splitted
}

func (do *Petri) fetchSchemasWithBlocks(schemas []*perceptron.DBInfo, m *meta.Meta, done chan error) {
	for _, di := range schemas {
		if di.State != perceptron.StatePublic {
			// schemaReplicant is not public, can't be used outside.
			continue
		}
		blocks, err := m.ListBlocks(di.ID)
		if err != nil {
			done <- err
			return
		}
		// If TreatOldVersionUTF8AsUTF8MB4 was enable, need to convert the old version schemaReplicant UTF8 charset to UTF8MB4.
		if config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
			for _, tbInfo := range blocks {
				schemareplicant.ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo)
			}
		}
		di.Blocks = make([]*perceptron.BlockInfo, 0, len(blocks))
		for _, tbl := range blocks {
			if tbl.State != perceptron.StatePublic {
				// schemaReplicant is not public, can't be used outside.
				continue
			}
			schemareplicant.ConvertCharsetDefCauslateToLowerCaseIfNeed(tbl)
			// Check whether the block is in repair mode.
			if petriutil.RepairInfo.InRepairMode() && petriutil.RepairInfo.CheckAndFetchRepairedBlock(di, tbl) {
				continue
			}
			di.Blocks = append(di.Blocks, tbl)
		}
	}
	done <- nil
}

const (
	initialVersion         = 0
	maxNumberOfDiffsToLoad = 100
)

func isTooOldSchema(usedVersion, newVersion int64) bool {
	if usedVersion == initialVersion || newVersion-usedVersion > maxNumberOfDiffsToLoad {
		return true
	}
	return false
}

// tryLoadSchemaDiffs tries to only load latest schemaReplicant changes.
// Return true if the schemaReplicant is loaded successfully.
// Return false if the schemaReplicant can not be loaded by schemaReplicant diff, then we need to do full load.
// The second returned value is the delta uFIDelated block and partition IDs.
func (do *Petri) tryLoadSchemaDiffs(m *meta.Meta, usedVersion, newVersion int64) (bool, *einsteindb.RelatedSchemaChange, error) {
	// If there isn't any used version, or used version is too old, we do full load.
	// And when users use history read feature, we will set usedVersion to initialVersion, then full load is needed.
	if isTooOldSchema(usedVersion, newVersion) {
		return false, nil, nil
	}
	var diffs []*perceptron.SchemaDiff
	for usedVersion < newVersion {
		usedVersion++
		diff, err := m.GetSchemaDiff(usedVersion)
		if err != nil {
			return false, nil, err
		}
		if diff == nil {
			// If diff is missing for any version between used and new version, we fall back to full reload.
			return false, nil, nil
		}
		diffs = append(diffs, diff)
	}
	builder := schemareplicant.NewBuilder(do.infoHandle).InitWithOldSchemaReplicant()
	phyTblIDs := make([]int64, 0, len(diffs))
	actions := make([]uint64, 0, len(diffs))
	for _, diff := range diffs {
		IDs, err := builder.ApplyDiff(m, diff)
		if err != nil {
			return false, nil, err
		}
		if canSkipSchemaCheckerDBS(diff.Type) {
			continue
		}
		phyTblIDs = append(phyTblIDs, IDs...)
		for i := 0; i < len(IDs); i++ {
			actions = append(actions, uint64(1<<diff.Type))
		}
	}
	builder.Build()
	relatedChange := einsteindb.RelatedSchemaChange{}
	relatedChange.PhyTblIDS = phyTblIDs
	relatedChange.CausetActionTypes = actions
	return true, &relatedChange, nil
}

func canSkipSchemaCheckerDBS(tp perceptron.CausetActionType) bool {
	switch tp {
	case perceptron.CausetActionUFIDelateTiFlashReplicaStatus, perceptron.CausetActionSetTiFlashReplica:
		return true
	}
	return false
}

// SchemaReplicant gets information schemaReplicant from petri.
func (do *Petri) SchemaReplicant() schemareplicant.SchemaReplicant {
	return do.infoHandle.Get()
}

// GetSnapshotSchemaReplicant gets a snapshot information schemaReplicant.
func (do *Petri) GetSnapshotSchemaReplicant(snapshotTS uint64) (schemareplicant.SchemaReplicant, error) {
	snapHandle := do.infoHandle.EmptyClone()
	// For the snapHandle, it's an empty Handle, so its usedSchemaVersion is initialVersion.
	_, _, _, err := do.loadSchemaReplicant(snapHandle, initialVersion, snapshotTS)
	if err != nil {
		return nil, err
	}
	return snapHandle.Get(), nil
}

// GetSnapshotMeta gets a new snapshot meta at startTS.
func (do *Petri) GetSnapshotMeta(startTS uint64) (*meta.Meta, error) {
	snapshot, err := do.causetstore.GetSnapshot(ekv.NewVersion(startTS))
	if err != nil {
		return nil, err
	}
	return meta.NewSnapshotMeta(snapshot), nil
}

// DBS gets DBS from petri.
func (do *Petri) DBS() dbs.DBS {
	return do.dbs
}

// InfoSyncer gets infoSyncer from petri.
func (do *Petri) InfoSyncer() *infosync.InfoSyncer {
	return do.info
}

// CausetStore gets KV causetstore from petri.
func (do *Petri) CausetStore() ekv.CausetStorage {
	return do.causetstore
}

// GetSINTERLOCKe gets the status variables sINTERLOCKe.
func (do *Petri) GetSINTERLOCKe(status string) variable.SINTERLOCKeFlag {
	// Now petri status variables sINTERLOCKe are all default sINTERLOCKe.
	return variable.DefaultStatusVarSINTERLOCKeFlag
}

// Reload reloads SchemaReplicant.
// It's public in order to do the test.
func (do *Petri) Reload() error {
	failpoint.Inject("ErrorMockReloadFailed", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock reload failed"))
		}
	})

	// Lock here for only once at the same time.
	do.m.Lock()
	defer do.m.Unlock()

	startTime := time.Now()

	var err error
	var neededSchemaVersion int64

	ver, err := do.causetstore.CurrentVersion()
	if err != nil {
		return err
	}

	schemaVersion := int64(0)
	oldSchemaReplicant := do.infoHandle.Get()
	if oldSchemaReplicant != nil {
		schemaVersion = oldSchemaReplicant.SchemaMetaVersion()
	}

	var (
		fullLoad       bool
		relatedChanges *einsteindb.RelatedSchemaChange
	)
	neededSchemaVersion, relatedChanges, fullLoad, err = do.loadSchemaReplicant(do.infoHandle, schemaVersion, ver.Ver)
	metrics.LoadSchemaDuration.Observe(time.Since(startTime).Seconds())
	if err != nil {
		metrics.LoadSchemaCounter.WithLabelValues("failed").Inc()
		return err
	}
	metrics.LoadSchemaCounter.WithLabelValues("succ").Inc()

	if fullLoad {
		logutil.BgLogger().Info("full load and reset schemaReplicant validator")
		do.SchemaValidator.Reset()
	}
	do.SchemaValidator.UFIDelate(ver.Ver, schemaVersion, neededSchemaVersion, relatedChanges)

	lease := do.DBS().GetLease()
	sub := time.Since(startTime)
	// Reload interval is lease / 2, if load schemaReplicant time elapses more than this interval,
	// some query maybe responded by ErrSchemaReplicantExpired error.
	if sub > (lease/2) && lease > 0 {
		logutil.BgLogger().Warn("loading schemaReplicant takes a long time", zap.Duration("take time", sub))
	}

	return nil
}

// LogSlowQuery keeps topN recent slow queries in petri.
func (do *Petri) LogSlowQuery(query *SlowQueryInfo) {
	do.slowQuery.mu.RLock()
	defer do.slowQuery.mu.RUnlock()
	if do.slowQuery.mu.closed {
		return
	}

	select {
	case do.slowQuery.ch <- query:
	default:
	}
}

// ShowSlowQuery returns the slow queries.
func (do *Petri) ShowSlowQuery(showSlow *ast.ShowSlow) []*SlowQueryInfo {
	msg := &showSlowMessage{
		request: showSlow,
	}
	msg.Add(1)
	do.slowQuery.msgCh <- msg
	msg.Wait()
	return msg.result
}

func (do *Petri) topNSlowQueryLoop() {
	defer soliton.Recover(metrics.LabelPetri, "topNSlowQueryLoop", nil, false)
	ticker := time.NewTicker(time.Minute * 10)
	defer func() {
		ticker.Stop()
		do.wg.Done()
		logutil.BgLogger().Info("topNSlowQueryLoop exited.")
	}()
	for {
		select {
		case now := <-ticker.C:
			do.slowQuery.RemoveExpired(now)
		case info, ok := <-do.slowQuery.ch:
			if !ok {
				return
			}
			do.slowQuery.Append(info)
		case msg := <-do.slowQuery.msgCh:
			req := msg.request
			switch req.Tp {
			case ast.ShowSlowTop:
				msg.result = do.slowQuery.QueryTop(int(req.Count), req.HoTT)
			case ast.ShowSlowRecent:
				msg.result = do.slowQuery.QueryRecent(int(req.Count))
			default:
				msg.result = do.slowQuery.QueryAll()
			}
			msg.Done()
		}
	}
}

func (do *Petri) infoSyncerKeeper() {
	defer func() {
		do.wg.Done()
		logutil.BgLogger().Info("infoSyncerKeeper exited.")
		soliton.Recover(metrics.LabelPetri, "infoSyncerKeeper", nil, false)
	}()
	ticker := time.NewTicker(infosync.ReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			do.info.ReportMinStartTS(do.CausetStore())
		case <-do.info.Done():
			logutil.BgLogger().Info("server info syncer need to restart")
			if err := do.info.Restart(context.Background()); err != nil {
				logutil.BgLogger().Error("server info syncer restart failed", zap.Error(err))
			} else {
				logutil.BgLogger().Info("server info syncer restarted")
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Petri) topologySyncerKeeper() {
	defer soliton.Recover(metrics.LabelPetri, "topologySyncerKeeper", nil, false)
	ticker := time.NewTicker(infosync.TopologyTimeToRefresh)
	defer func() {
		ticker.Stop()
		do.wg.Done()
		logutil.BgLogger().Info("topologySyncerKeeper exited.")
	}()

	for {
		select {
		case <-ticker.C:
			err := do.info.StoreTopologyInfo(context.Background())
			if err != nil {
				logutil.BgLogger().Error("refresh topology in loop failed", zap.Error(err))
			}
		case <-do.info.TopologyDone():
			logutil.BgLogger().Info("server topology syncer need to restart")
			if err := do.info.RestartTopology(context.Background()); err != nil {
				logutil.BgLogger().Error("server topology syncer restart failed", zap.Error(err))
			} else {
				logutil.BgLogger().Info("server topology syncer restarted")
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Petri) loadSchemaInLoop(ctx context.Context, lease time.Duration) {
	defer soliton.Recover(metrics.LabelPetri, "loadSchemaInLoop", nil, true)
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(lease / 2)
	defer func() {
		ticker.Stop()
		do.wg.Done()
		logutil.BgLogger().Info("loadSchemaInLoop exited.")
	}()
	syncer := do.dbs.SchemaSyncer()

	for {
		select {
		case <-ticker.C:
			err := do.Reload()
			if err != nil {
				logutil.BgLogger().Error("reload schemaReplicant in loop failed", zap.Error(err))
			}
		case _, ok := <-syncer.GlobalVersionCh():
			err := do.Reload()
			if err != nil {
				logutil.BgLogger().Error("reload schemaReplicant in loop failed", zap.Error(err))
			}
			if !ok {
				logutil.BgLogger().Warn("reload schemaReplicant in loop, schemaReplicant syncer need rewatch")
				// Make sure the rewatch doesn't affect load schemaReplicant, so we watch the global schemaReplicant version asynchronously.
				syncer.WatchGlobalSchemaVer(context.Background())
			}
		case <-syncer.Done():
			// The schemaReplicant syncer stops, we need stop the schemaReplicant validator to synchronize the schemaReplicant version.
			logutil.BgLogger().Info("reload schemaReplicant in loop, schemaReplicant syncer need restart")
			// The etcd is responsible for schemaReplicant synchronization, we should ensure there is at most two different schemaReplicant version
			// in the MilevaDB cluster, to make the data/schemaReplicant be consistent. If we lost connection/stochastik to etcd, the cluster
			// will treats this MilevaDB as a down instance, and etcd will remove the key of `/milevadb/dbs/all_schema_versions/milevadb-id`.
			// Say the schemaReplicant version now is 1, the owner is changing the schemaReplicant version to 2, it will not wait for this down MilevaDB syncing the schemaReplicant,
			// then continue to change the MilevaDB schemaReplicant to version 3. Unfortunately, this down MilevaDB schemaReplicant version will still be version 1.
			// And version 1 is not consistent to version 3. So we need to stop the schemaReplicant validator to prohibit the DML executing.
			do.SchemaValidator.Stop()
			err := do.mustRestartSyncer(ctx)
			if err != nil {
				logutil.BgLogger().Error("reload schemaReplicant in loop, schemaReplicant syncer restart failed", zap.Error(err))
				break
			}
			// The schemaReplicant maybe changed, must reload schemaReplicant then the schemaReplicant validator can restart.
			exitLoop := do.mustReload()
			// petri is cosed.
			if exitLoop {
				logutil.BgLogger().Error("petri is closed, exit loadSchemaInLoop")
				return
			}
			do.SchemaValidator.Restart()
			logutil.BgLogger().Info("schemaReplicant syncer restarted")
		case <-do.exit:
			return
		}
	}
}

// mustRestartSyncer tries to restart the SchemaSyncer.
// It returns until it's successful or the petri is stoped.
func (do *Petri) mustRestartSyncer(ctx context.Context) error {
	syncer := do.dbs.SchemaSyncer()

	for {
		err := syncer.Restart(ctx)
		if err == nil {
			return nil
		}
		// If the petri has stopped, we return an error immediately.
		if do.isClose() {
			return err
		}
		logutil.BgLogger().Error("restart the schemaReplicant syncer failed", zap.Error(err))
		time.Sleep(time.Second)
	}
}

// mustReload tries to Reload the schemaReplicant, it returns until it's successful or the petri is closed.
// it returns false when it is successful, returns true when the petri is closed.
func (do *Petri) mustReload() (exitLoop bool) {
	for {
		err := do.Reload()
		if err == nil {
			logutil.BgLogger().Info("mustReload succeed")
			return false
		}

		// If the petri is closed, we returns immediately.
		logutil.BgLogger().Info("reload the schemaReplicant failed", zap.Error(err))
		if do.isClose() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (do *Petri) isClose() bool {
	select {
	case <-do.exit:
		logutil.BgLogger().Info("petri is closed")
		return true
	default:
	}
	return false
}

// Close closes the Petri and release its resource.
func (do *Petri) Close() {
	if do == nil {
		return
	}
	startTime := time.Now()
	if do.dbs != nil {
		terror.Log(do.dbs.Stop())
	}
	if do.info != nil {
		do.info.RemoveServerInfo()
		do.info.RemoveMinStartTS()
	}
	close(do.exit)
	if do.etcdClient != nil {
		terror.Log(errors.Trace(do.etcdClient.Close()))
	}

	do.sysStochastikPool.Close()
	do.slowQuery.Close()
	do.cancel()
	do.wg.Wait()
	logutil.BgLogger().Info("petri closed", zap.Duration("take time", time.Since(startTime)))
}

type dbsCallback struct {
	dbs.BaseCallback
	do *Petri
}

func (c *dbsCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("performing DBS change, must reload")

	err = c.do.Reload()
	if err != nil {
		logutil.BgLogger().Error("performing DBS change failed", zap.Error(err))
	}

	return nil
}

const resourceIdleTimeout = 3 * time.Minute // resources in the ResourcePool will be recycled after idleTimeout

// NewPetri creates a new petri. Should not create multiple petris for the same causetstore.
func NewPetri(causetstore ekv.CausetStorage, dbsLease time.Duration, statsLease time.Duration, idxUsageSyncLease time.Duration, factory pools.Factory) *Petri {
	capacity := 200 // capacity of the sysStochastikPool size
	do := &Petri{
		causetstore:         causetstore,
		exit:                make(chan struct{}),
		sysStochastikPool:   newStochastikPool(capacity, factory),
		statsLease:          statsLease,
		infoHandle:          schemareplicant.NewHandle(causetstore),
		slowQuery:           newTopNSlowQueries(30, time.Hour*24*7, 500),
		indexUsageSyncLease: idxUsageSyncLease,
	}

	do.SchemaValidator = NewSchemaValidator(dbsLease, do)
	return do
}

// Init initializes a petri.
func (do *Petri) Init(dbsLease time.Duration, sysFactory func(*Petri) (pools.Resource, error)) error {
	perfschema.Init()
	if ebd, ok := do.causetstore.(einsteindb.EtcdBackend); ok {
		var addrs []string
		var err error
		if addrs, err = ebd.EtcdAddrs(); err != nil {
			return err
		}
		if addrs != nil {
			cfg := config.GetGlobalConfig()
			// silence etcd warn log, when petri closed, it won't randomly print warn log
			// see details at the issue https://github.com/whtcorpsinc/milevadb/issues/15479
			etcdLogCfg := zap.NewProductionConfig()
			etcdLogCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
			cli, err := clientv3.New(clientv3.Config{
				LogConfig:        &etcdLogCfg,
				Endpoints:        addrs,
				AutoSyncInterval: 30 * time.Second,
				DialTimeout:      5 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithBackoffMaxDelay(time.Second * 3),
					grpc.WithKeepaliveParams(keepalive.ClientParameters{
						Time:    time.Duration(cfg.EinsteinDBClient.GrpcKeepAliveTime) * time.Second,
						Timeout: time.Duration(cfg.EinsteinDBClient.GrpcKeepAliveTimeout) * time.Second,
					}),
				},
				TLS: ebd.TLSConfig(),
			})
			if err != nil {
				return errors.Trace(err)
			}
			do.etcdClient = cli
		}
	}

	// TODO: Here we create new stochastik with sysFac in DBS,
	// which will use `do` as Petri instead of call `domap.Get`.
	// That's because `domap.Get` requires a dagger, but before
	// we initialize Petri finish, we can't require that again.
	// After we remove the lazy logic of creating Petri, we
	// can simplify code here.
	sysFac := func() (pools.Resource, error) {
		return sysFactory(do)
	}
	sysCtxPool := pools.NewResourcePool(sysFac, 2, 2, resourceIdleTimeout)
	ctx, cancelFunc := context.WithCancel(context.Background())
	do.cancel = cancelFunc
	callback := &dbsCallback{do: do}
	d := do.dbs
	do.dbs = dbs.NewDBS(
		ctx,
		dbs.WithEtcdClient(do.etcdClient),
		dbs.WithStore(do.causetstore),
		dbs.WithInfoHandle(do.infoHandle),
		dbs.WithHook(callback),
		dbs.WithLease(dbsLease),
	)
	err := do.dbs.Start(sysCtxPool)
	if err != nil {
		return err
	}
	failpoint.Inject("MockReplaceDBS", func(val failpoint.Value) {
		if val.(bool) {
			if err := do.dbs.Stop(); err != nil {
				logutil.BgLogger().Error("stop DBS failed", zap.Error(err))
			}
			do.dbs = d
		}
	})

	skipRegisterToDashboard := config.GetGlobalConfig().SkipRegisterToDashboard
	err = do.dbs.SchemaSyncer().Init(ctx)
	if err != nil {
		return err
	}
	do.info, err = infosync.GlobalInfoSyncerInit(ctx, do.dbs.GetID(), do.etcdClient, skipRegisterToDashboard)
	if err != nil {
		return err
	}
	err = do.Reload()
	if err != nil {
		return err
	}

	// Only when the causetstore is local that the lease value is 0.
	// If the causetstore is local, it doesn't need loadSchemaInLoop.
	if dbsLease > 0 {
		do.wg.Add(1)
		// Local causetstore needs to get the change information for every DBS state in each stochastik.
		go do.loadSchemaInLoop(ctx, dbsLease)
	}
	do.wg.Add(1)
	go do.topNSlowQueryLoop()

	do.wg.Add(1)
	go do.infoSyncerKeeper()

	if !skipRegisterToDashboard {
		do.wg.Add(1)
		go do.topologySyncerKeeper()
	}

	return nil
}

type stochastikPool struct {
	resources chan pools.Resource
	factory   pools.Factory
	mu        struct {
		sync.RWMutex
		closed bool
	}
}

func newStochastikPool(cap int, factory pools.Factory) *stochastikPool {
	return &stochastikPool{
		resources: make(chan pools.Resource, cap),
		factory:   factory,
	}
}

func (p *stochastikPool) Get() (resource pools.Resource, err error) {
	var ok bool
	select {
	case resource, ok = <-p.resources:
		if !ok {
			err = errors.New("stochastik pool closed")
		}
	default:
		resource, err = p.factory()
	}
	return
}

func (p *stochastikPool) Put(resource pools.Resource) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.mu.closed {
		resource.Close()
		return
	}

	select {
	case p.resources <- resource:
	default:
		resource.Close()
	}
}
func (p *stochastikPool) Close() {
	p.mu.Lock()
	if p.mu.closed {
		p.mu.Unlock()
		return
	}
	p.mu.closed = true
	close(p.resources)
	p.mu.Unlock()

	for r := range p.resources {
		r.Close()
	}
}

// SysStochastikPool returns the system stochastik pool.
func (do *Petri) SysStochastikPool() *stochastikPool {
	return do.sysStochastikPool
}

// GetEtcdClient returns the etcd client.
func (do *Petri) GetEtcdClient() *clientv3.Client {
	return do.etcdClient
}

// LoadPrivilegeLoop create a goroutine loads privilege blocks in a loop, it
// should be called only once in BootstrapStochastik.
func (do *Petri) LoadPrivilegeLoop(ctx stochastikctx.Context) error {
	ctx.GetStochastikVars().InRestrictedALLEGROSQL = true
	do.privHandle = privileges.NewHandle()
	err := do.privHandle.UFIDelate(ctx)
	if err != nil {
		return err
	}

	var watchCh clientv3.WatchChan
	duration := 5 * time.Minute
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), privilegeKey)
		duration = 10 * time.Minute
	}

	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("loadPrivilegeInLoop exited.")
			soliton.Recover(metrics.LabelPetri, "loadPrivilegeInLoop", nil, false)
		}()
		var count int
		for {
			ok := true
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
			case <-time.After(duration):
			}
			if !ok {
				logutil.BgLogger().Error("load privilege loop watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), privilegeKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}

			count = 0
			err := do.privHandle.UFIDelate(ctx)
			metrics.LoadPrivilegeCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Error("load privilege failed", zap.Error(err))
			}
		}
	}()
	return nil
}

// PrivilegeHandle returns the MyALLEGROSQLPrivilege.
func (do *Petri) PrivilegeHandle() *privileges.Handle {
	return do.privHandle
}

// BindHandle returns petri's bindHandle.
func (do *Petri) BindHandle() *bindinfo.BindHandle {
	return do.bindHandle
}

// LoadBindInfoLoop create a goroutine loads BindInfo in a loop, it should
// be called only once in BootstrapStochastik.
func (do *Petri) LoadBindInfoLoop(ctxForHandle stochastikctx.Context, ctxForEvolve stochastikctx.Context) error {
	ctxForHandle.GetStochastikVars().InRestrictedALLEGROSQL = true
	ctxForEvolve.GetStochastikVars().InRestrictedALLEGROSQL = true
	do.bindHandle = bindinfo.NewBindHandle(ctxForHandle)
	err := do.bindHandle.UFIDelate(true)
	if err != nil || bindinfo.Lease == 0 {
		return err
	}

	do.globalBindHandleWorkerLoop()
	do.handleEvolvePlanTasksLoop(ctxForEvolve)
	return nil
}

func (do *Petri) globalBindHandleWorkerLoop() {
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("globalBindHandleWorkerLoop exited.")
			soliton.Recover(metrics.LabelPetri, "globalBindHandleWorkerLoop", nil, false)
		}()
		bindWorkerTicker := time.NewTicker(bindinfo.Lease)
		defer bindWorkerTicker.Stop()
		for {
			select {
			case <-do.exit:
				return
			case <-bindWorkerTicker.C:
				err := do.bindHandle.UFIDelate(false)
				if err != nil {
					logutil.BgLogger().Error("uFIDelate bindinfo failed", zap.Error(err))
				}
				do.bindHandle.DropInvalidBindRecord()
				if variable.MilevaDBOptOn(variable.CapturePlanBaseline.GetVal()) {
					do.bindHandle.CaptureBaselines()
				}
				do.bindHandle.SaveEvolveTasksToStore()
			}
		}
	}()
}

func (do *Petri) handleEvolvePlanTasksLoop(ctx stochastikctx.Context) {
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("handleEvolvePlanTasksLoop exited.")
			soliton.Recover(metrics.LabelPetri, "handleEvolvePlanTasksLoop", nil, false)
		}()
		owner := do.newOwnerManager(bindinfo.Prompt, bindinfo.OwnerKey)
		for {
			select {
			case <-do.exit:
				owner.Cancel()
				return
			case <-time.After(bindinfo.Lease):
			}
			if owner.IsOwner() {
				err := do.bindHandle.HandleEvolvePlanTask(ctx, false)
				if err != nil {
					logutil.BgLogger().Info("evolve plan failed", zap.Error(err))
				}
			}
		}
	}()
}

// TelemetryLoop create a goroutine that reports usage data in a loop, it should be called only once
// in BootstrapStochastik.
func (do *Petri) TelemetryLoop(ctx stochastikctx.Context) {
	ctx.GetStochastikVars().InRestrictedALLEGROSQL = true
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("handleTelemetryLoop exited.")
			soliton.Recover(metrics.LabelPetri, "handleTelemetryLoop", nil, false)
		}()
		owner := do.newOwnerManager(telemetry.Prompt, telemetry.OwnerKey)
		for {
			select {
			case <-do.exit:
				owner.Cancel()
				return
			case <-time.After(telemetry.ReportInterval):
				if !owner.IsOwner() {
					continue
				}
				err := telemetry.ReportUsageData(ctx, do.GetEtcdClient())
				if err != nil {
					// Only status uFIDelate errors will be printed out
					logutil.BgLogger().Warn("handleTelemetryLoop status uFIDelate failed", zap.Error(err))
				}
			}
		}
	}()
}

// StatsHandle returns the statistic handle.
func (do *Petri) StatsHandle() *handle.Handle {
	return (*handle.Handle)(atomic.LoadPointer(&do.statsHandle))
}

// CreateStatsHandle is used only for test.
func (do *Petri) CreateStatsHandle(ctx stochastikctx.Context) {
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(handle.NewHandle(ctx, do.statsLease)))
}

// StatsUFIDelating checks if the stats worker is uFIDelating.
func (do *Petri) StatsUFIDelating() bool {
	return do.statsUFIDelating.Get() > 0
}

// SetStatsUFIDelating sets the value of stats uFIDelating.
func (do *Petri) SetStatsUFIDelating(val bool) {
	if val {
		do.statsUFIDelating.Set(1)
	} else {
		do.statsUFIDelating.Set(0)
	}
}

// RunAutoAnalyze indicates if this MilevaDB server starts auto analyze worker and can run auto analyze job.
var RunAutoAnalyze = true

// UFIDelateBlockStatsLoop creates a goroutine loads stats info and uFIDelates stats info in a loop.
// It will also start a goroutine to analyze blocks automatically.
// It should be called only once in BootstrapStochastik.
func (do *Petri) UFIDelateBlockStatsLoop(ctx stochastikctx.Context) error {
	ctx.GetStochastikVars().InRestrictedALLEGROSQL = true
	statsHandle := handle.NewHandle(ctx, do.statsLease)
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(statsHandle))
	do.dbs.RegisterEventCh(statsHandle.DBSEventCh())
	// Negative stats lease indicates that it is in test, it does not need uFIDelate.
	if do.statsLease >= 0 {
		do.wg.Add(1)
		go do.loadStatsWorker()
	}
	if do.statsLease <= 0 {
		return nil
	}
	owner := do.newOwnerManager(handle.StatsPrompt, handle.StatsOwnerKey)
	do.wg.Add(1)
	do.SetStatsUFIDelating(true)
	go do.uFIDelateStatsWorker(ctx, owner)
	if RunAutoAnalyze {
		do.wg.Add(1)
		go do.autoAnalyzeWorker(owner)
	}
	return nil
}

func (do *Petri) newOwnerManager(prompt, ownerKey string) owner.Manager {
	id := do.dbs.OwnerManager().ID()
	var statsOwner owner.Manager
	if do.etcdClient == nil {
		statsOwner = owner.NewMockManager(context.Background(), id)
	} else {
		statsOwner = owner.NewOwnerManager(context.Background(), do.etcdClient, prompt, id, ownerKey)
	}
	// TODO: Need to do something when err is not nil.
	err := statsOwner.CampaignOwner()
	if err != nil {
		logutil.BgLogger().Warn("campaign owner failed", zap.Error(err))
	}
	return statsOwner
}

func (do *Petri) loadStatsWorker() {
	defer soliton.Recover(metrics.LabelPetri, "loadStatsWorker", nil, false)
	lease := do.statsLease
	if lease == 0 {
		lease = 3 * time.Second
	}
	loadTicker := time.NewTicker(lease)
	defer func() {
		loadTicker.Stop()
		do.wg.Done()
		logutil.BgLogger().Info("loadStatsWorker exited.")
	}()
	statsHandle := do.StatsHandle()
	t := time.Now()
	err := statsHandle.InitStats(do.SchemaReplicant())
	if err != nil {
		logutil.BgLogger().Debug("init stats info failed", zap.Error(err))
	} else {
		logutil.BgLogger().Info("init stats info time", zap.Duration("take time", time.Since(t)))
	}
	for {
		select {
		case <-loadTicker.C:
			err = statsHandle.UFIDelate(do.SchemaReplicant())
			if err != nil {
				logutil.BgLogger().Debug("uFIDelate stats info failed", zap.Error(err))
			}
			err = statsHandle.LoadNeededHistograms()
			if err != nil {
				logutil.BgLogger().Debug("load histograms failed", zap.Error(err))
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Petri) uFIDelateStatsWorker(ctx stochastikctx.Context, owner owner.Manager) {
	defer soliton.Recover(metrics.LabelPetri, "uFIDelateStatsWorker", nil, false)
	lease := do.statsLease
	deltaUFIDelateTicker := time.NewTicker(20 * lease)
	gcStatsTicker := time.NewTicker(100 * lease)
	dumpFeedbackTicker := time.NewTicker(200 * lease)
	loadFeedbackTicker := time.NewTicker(5 * lease)
	statsHandle := do.StatsHandle()
	defer func() {
		loadFeedbackTicker.Stop()
		dumpFeedbackTicker.Stop()
		gcStatsTicker.Stop()
		deltaUFIDelateTicker.Stop()
		do.SetStatsUFIDelating(false)
		do.wg.Done()
		logutil.BgLogger().Info("uFIDelateStatsWorker exited.")
	}()
	for {
		select {
		case <-do.exit:
			statsHandle.FlushStats()
			owner.Cancel()
			return
			// This channel is sent only by dbs owner.
		case t := <-statsHandle.DBSEventCh():
			err := statsHandle.HandleDBSEvent(t)
			if err != nil {
				logutil.BgLogger().Debug("handle dbs event failed", zap.Error(err))
			}
		case <-deltaUFIDelateTicker.C:
			err := statsHandle.DumpStatsDeltaToKV(handle.DumFIDelelta)
			if err != nil {
				logutil.BgLogger().Debug("dump stats delta failed", zap.Error(err))
			}
			statsHandle.UFIDelateErrorRate(do.SchemaReplicant())
		case <-loadFeedbackTicker.C:
			statsHandle.UFIDelateStatsByLocalFeedback(do.SchemaReplicant())
			if !owner.IsOwner() {
				continue
			}
			err := statsHandle.HandleUFIDelateStats(do.SchemaReplicant())
			if err != nil {
				logutil.BgLogger().Debug("uFIDelate stats using feedback failed", zap.Error(err))
			}
		case <-dumpFeedbackTicker.C:
			err := statsHandle.DumpStatsFeedbackToKV()
			if err != nil {
				logutil.BgLogger().Debug("dump stats feedback failed", zap.Error(err))
			}
		case <-gcStatsTicker.C:
			if !owner.IsOwner() {
				continue
			}
			err := statsHandle.GCStats(do.SchemaReplicant(), do.DBS().GetLease())
			if err != nil {
				logutil.BgLogger().Debug("GC stats failed", zap.Error(err))
			}
		}
	}
}

func (do *Petri) autoAnalyzeWorker(owner owner.Manager) {
	defer soliton.Recover(metrics.LabelPetri, "autoAnalyzeWorker", nil, false)
	statsHandle := do.StatsHandle()
	analyzeTicker := time.NewTicker(do.statsLease)
	defer func() {
		analyzeTicker.Stop()
		do.wg.Done()
		logutil.BgLogger().Info("autoAnalyzeWorker exited.")
	}()
	for {
		select {
		case <-analyzeTicker.C:
			if owner.IsOwner() {
				statsHandle.HandleAutoAnalyze(do.SchemaReplicant())
			}
		case <-do.exit:
			return
		}
	}
}

// ExpensiveQueryHandle returns the expensive query handle.
func (do *Petri) ExpensiveQueryHandle() *expensivequery.Handle {
	return do.expensiveQueryHandle
}

// InitExpensiveQueryHandle init the expensive query handler.
func (do *Petri) InitExpensiveQueryHandle() {
	do.expensiveQueryHandle = expensivequery.NewExpensiveQueryHandle(do.exit)
}

const privilegeKey = "/milevadb/privilege"

// NotifyUFIDelatePrivilege uFIDelates privilege key in etcd, MilevaDB client that watches
// the key will get notification.
func (do *Petri) NotifyUFIDelatePrivilege(ctx stochastikctx.Context) {
	if do.etcdClient != nil {
		event := do.etcdClient.KV
		_, err := event.Put(context.Background(), privilegeKey, "")
		if err != nil {
			logutil.BgLogger().Warn("notify uFIDelate privilege failed", zap.Error(err))
		}
	}
	// uFIDelate locally
	_, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(`FLUSH PRIVILEGES`)
	if err != nil {
		logutil.BgLogger().Error("unable to uFIDelate privileges", zap.Error(err))
	}
}

var (
	// ErrSchemaReplicantExpired returns the error that information schemaReplicant is out of date.
	ErrSchemaReplicantExpired = terror.ClassPetri.New(errno.ErrSchemaReplicantExpired, errno.MyALLEGROSQLErrName[errno.ErrSchemaReplicantExpired])
	// ErrSchemaReplicantChanged returns the error that information schemaReplicant is changed.
	ErrSchemaReplicantChanged = terror.ClassPetri.New(errno.ErrSchemaReplicantChanged,
		errno.MyALLEGROSQLErrName[errno.ErrSchemaReplicantChanged]+". "+ekv.TxnRetryableMark)
)
