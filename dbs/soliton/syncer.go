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

package soliton

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/owner"
	milevadbutil "github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.uber.org/zap"
)

const (
	// DBSAllSchemaVersions is the path on etcd that is used to causetstore all servers current schemaReplicant versions.
	// It's exported for testing.
	DBSAllSchemaVersions = "/milevadb/dbs/all_schema_versions"
	// DBSGlobalSchemaVersion is the path on etcd that is used to causetstore the latest schemaReplicant versions.
	// It's exported for testing.
	DBSGlobalSchemaVersion = "/milevadb/dbs/global_schema_version"
	// InitialVersion is the initial schemaReplicant version for every server.
	// It's exported for testing.
	InitialVersion          = "0"
	putKeyNoRetry           = 1
	keyOFIDelefaultRetryCnt = 3
	putKeyRetryUnlimited    = math.MaxInt64
	keyOFIDelefaultTimeout  = 2 * time.Second
	keyOpRetryInterval      = 30 * time.Millisecond
	checkVersInterval       = 20 * time.Millisecond

	dbsPrompt = "dbs-syncer"
)

var (
	// CheckVersFirstWaitTime is a waitting time before the owner checks all the servers of the schemaReplicant version,
	// and it's an exported variable for testing.
	CheckVersFirstWaitTime = 50 * time.Millisecond
	// SyncerStochastikTTL is the etcd stochastik's TTL in seconds.
	// and it's an exported variable for testing.
	SyncerStochastikTTL = 90
)

// SchemaSyncer is used to synchronize schemaReplicant version between the DBS worker leader and followers through etcd.
type SchemaSyncer interface {
	// Init sets the global schemaReplicant version path to etcd if it isn't exist,
	// then watch this path, and initializes the self schemaReplicant version to etcd.
	Init(ctx context.Context) error
	// UFIDelateSelfVersion uFIDelates the current version to the self path on etcd.
	UFIDelateSelfVersion(ctx context.Context, version int64) error
	// OwnerUFIDelateGlobalVersion uFIDelates the latest version to the global path on etcd until uFIDelating is successful or the ctx is done.
	OwnerUFIDelateGlobalVersion(ctx context.Context, version int64) error
	// GlobalVersionCh gets the chan for watching global version.
	GlobalVersionCh() clientv3.WatchChan
	// WatchGlobalSchemaVer watches the global schemaReplicant version.
	WatchGlobalSchemaVer(ctx context.Context)
	// MustGetGlobalVersion gets the global version. The only reason it fails is that ctx is done.
	MustGetGlobalVersion(ctx context.Context) (int64, error)
	// Done returns a channel that closes when the syncer is no longer being refreshed.
	Done() <-chan struct{}
	// Restart restarts the syncer when it's on longer being refreshed.
	Restart(ctx context.Context) error
	// OwnerCheckAllVersions checks whether all followers' schemaReplicant version are equal to
	// the latest schemaReplicant version. If the result is false, wait for a while and check again soliton the processing time reach 2 * lease.
	// It returns until all servers' versions are equal to the latest version or the ctx is done.
	OwnerCheckAllVersions(ctx context.Context, latestVer int64) error
	// NotifyCleanExpiredPaths informs to clean up expired paths.
	// The returned value is used for testing.
	NotifyCleanExpiredPaths() bool
	// StartCleanWork starts to clean up tasks.
	StartCleanWork()
	// Close ends SchemaSyncer.
	Close()
}

type ownerChecker interface {
	IsOwner() bool
}

type schemaVersionSyncer struct {
	selfSchemaVerPath string
	etcdCli           *clientv3.Client
	stochastik        unsafe.Pointer
	mu                struct {
		sync.RWMutex
		globalVerCh clientv3.WatchChan
	}

	// for clean worker
	ownerChecker              ownerChecker
	notifyCleanExpiredPathsCh chan struct{}
	ctx                       context.Context
	cancel                    context.CancelFunc
}

// NewSchemaSyncer creates a new SchemaSyncer.
func NewSchemaSyncer(ctx context.Context, etcdCli *clientv3.Client, id string, oc ownerChecker) SchemaSyncer {
	childCtx, cancelFunc := context.WithCancel(ctx)
	return &schemaVersionSyncer{
		etcdCli:                   etcdCli,
		selfSchemaVerPath:         fmt.Sprintf("%s/%s", DBSAllSchemaVersions, id),
		ownerChecker:              oc,
		notifyCleanExpiredPathsCh: make(chan struct{}, 1),
		ctx:                       childCtx,
		cancel:                    cancelFunc,
	}
}

// PutKVToEtcd puts key value to etcd.
// etcdCli is client of etcd.
// retryCnt is retry time when an error occurs.
// opts is configures of etcd Operations.
func PutKVToEtcd(ctx context.Context, etcdCli *clientv3.Client, retryCnt int, key, val string,
	opts ...clientv3.OpOption) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		if isContextDone(ctx) {
			return errors.Trace(ctx.Err())
		}

		childCtx, cancel := context.WithTimeout(ctx, keyOFIDelefaultTimeout)
		_, err = etcdCli.Put(childCtx, key, val, opts...)
		cancel()
		if err == nil {
			return nil
		}
		logutil.BgLogger().Warn("[dbs] etcd-cli put ekv failed", zap.String("key", key), zap.String("value", val), zap.Error(err), zap.Int("retryCnt", i))
		time.Sleep(keyOpRetryInterval)
	}
	return errors.Trace(err)
}

// Init implements SchemaSyncer.Init interface.
func (s *schemaVersionSyncer) Init(ctx context.Context) error {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerInit, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	_, err = s.etcdCli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(DBSGlobalSchemaVersion), "=", 0)).
		Then(clientv3.OpPut(DBSGlobalSchemaVersion, InitialVersion)).
		Commit()
	if err != nil {
		return errors.Trace(err)
	}
	logPrefix := fmt.Sprintf("[%s] %s", dbsPrompt, s.selfSchemaVerPath)
	stochastik, err := owner.NewStochastik(ctx, logPrefix, s.etcdCli, owner.NewStochastikDefaultRetryCnt, SyncerStochastikTTL)
	if err != nil {
		return errors.Trace(err)
	}
	s.storeStochastik(stochastik)

	s.mu.Lock()
	s.mu.globalVerCh = s.etcdCli.Watch(ctx, DBSGlobalSchemaVersion)
	s.mu.Unlock()

	err = PutKVToEtcd(ctx, s.etcdCli, keyOFIDelefaultRetryCnt, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.loadStochastik().Lease()))
	return errors.Trace(err)
}

func (s *schemaVersionSyncer) loadStochastik() *concurrency.Stochastik {
	return (*concurrency.Stochastik)(atomic.LoadPointer(&s.stochastik))
}

func (s *schemaVersionSyncer) storeStochastik(stochastik *concurrency.Stochastik) {
	atomic.StorePointer(&s.stochastik, (unsafe.Pointer)(stochastik))
}

// Done implements SchemaSyncer.Done interface.
func (s *schemaVersionSyncer) Done() <-chan struct{} {
	failpoint.Inject("ErrorMockStochastikDone", func(val failpoint.Value) {
		if val.(bool) {
			err := s.loadStochastik().Close()
			logutil.BgLogger().Error("close stochastik failed", zap.Error(err))
		}
	})

	return s.loadStochastik().Done()
}

// Restart implements SchemaSyncer.Restart interface.
func (s *schemaVersionSyncer) Restart(ctx context.Context) error {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerRestart, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	logPrefix := fmt.Sprintf("[%s] %s", dbsPrompt, s.selfSchemaVerPath)
	// NewStochastik's context will affect the exit of the stochastik.
	stochastik, err := owner.NewStochastik(ctx, logPrefix, s.etcdCli, owner.NewStochastikRetryUnlimited, SyncerStochastikTTL)
	if err != nil {
		return errors.Trace(err)
	}
	s.storeStochastik(stochastik)

	childCtx, cancel := context.WithTimeout(ctx, keyOFIDelefaultTimeout)
	defer cancel()
	err = PutKVToEtcd(childCtx, s.etcdCli, putKeyRetryUnlimited, s.selfSchemaVerPath, InitialVersion,
		clientv3.WithLease(s.loadStochastik().Lease()))

	return errors.Trace(err)
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *schemaVersionSyncer) GlobalVersionCh() clientv3.WatchChan {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.globalVerCh
}

// WatchGlobalSchemaVer implements SchemaSyncer.WatchGlobalSchemaVer interface.
func (s *schemaVersionSyncer) WatchGlobalSchemaVer(ctx context.Context) {
	startTime := time.Now()
	// Make sure the globalVerCh doesn't receive the information of 'close' before we finish the rewatch.
	s.mu.Lock()
	s.mu.globalVerCh = nil
	s.mu.Unlock()

	go func() {
		defer func() {
			metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerRewatch, metrics.RetLabel(nil)).Observe(time.Since(startTime).Seconds())
		}()
		ch := s.etcdCli.Watch(ctx, DBSGlobalSchemaVersion)

		s.mu.Lock()
		s.mu.globalVerCh = ch
		s.mu.Unlock()
		logutil.BgLogger().Info("[dbs] syncer watch global schemaReplicant finished")
	}()
}

// UFIDelateSelfVersion implements SchemaSyncer.UFIDelateSelfVersion interface.
func (s *schemaVersionSyncer) UFIDelateSelfVersion(ctx context.Context, version int64) error {
	startTime := time.Now()
	ver := strconv.FormatInt(version, 10)
	err := PutKVToEtcd(ctx, s.etcdCli, putKeyNoRetry, s.selfSchemaVerPath, ver,
		clientv3.WithLease(s.loadStochastik().Lease()))

	metrics.UFIDelateSelfVersionHistogram.WithLabelValues(metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// OwnerUFIDelateGlobalVersion implements SchemaSyncer.OwnerUFIDelateGlobalVersion interface.
func (s *schemaVersionSyncer) OwnerUFIDelateGlobalVersion(ctx context.Context, version int64) error {
	startTime := time.Now()
	ver := strconv.FormatInt(version, 10)
	// TODO: If the version is larger than the original global version, we need set the version.
	// Otherwise, we'd better set the original global version.
	err := PutKVToEtcd(ctx, s.etcdCli, putKeyRetryUnlimited, DBSGlobalSchemaVersion, ver)
	metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerUFIDelateGlobalVersion, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// removeSelfVersionPath remove the self path from etcd.
func (s *schemaVersionSyncer) removeSelfVersionPath() error {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DeploySyncerHistogram.WithLabelValues(metrics.SyncerClear, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	err = DeleteKeyFromEtcd(s.selfSchemaVerPath, s.etcdCli, keyOFIDelefaultRetryCnt, keyOFIDelefaultTimeout)
	return errors.Trace(err)
}

// DeleteKeyFromEtcd deletes key value from etcd.
func DeleteKeyFromEtcd(key string, etcdCli *clientv3.Client, retryCnt int, timeout time.Duration) error {
	var err error
	ctx := context.Background()
	for i := 0; i < retryCnt; i++ {
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err = etcdCli.Delete(childCtx, key)
		cancel()
		if err == nil {
			return nil
		}
		logutil.BgLogger().Warn("[dbs] etcd-cli delete key failed", zap.String("key", key), zap.Error(err), zap.Int("retryCnt", i))
	}
	return errors.Trace(err)
}

// MustGetGlobalVersion implements SchemaSyncer.MustGetGlobalVersion interface.
func (s *schemaVersionSyncer) MustGetGlobalVersion(ctx context.Context) (int64, error) {
	startTime := time.Now()
	var (
		err  error
		ver  int
		resp *clientv3.GetResponse
	)
	failedCnt := 0
	intervalCnt := int(time.Second / keyOpRetryInterval)

	defer func() {
		metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerGetGlobalVersion, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	for {
		if err != nil {
			if failedCnt%intervalCnt == 0 {
				logutil.BgLogger().Info("[dbs] syncer get global version failed", zap.Error(err))
			}
			time.Sleep(keyOpRetryInterval)
			failedCnt++
		}

		if isContextDone(ctx) {
			err = errors.Trace(ctx.Err())
			return 0, err
		}

		resp, err = s.etcdCli.Get(ctx, DBSGlobalSchemaVersion)
		if err != nil {
			continue
		}
		if len(resp.Kvs) > 0 {
			ver, err = strconv.Atoi(string(resp.Kvs[0].Value))
			if err == nil {
				return int64(ver), nil
			}
		}
	}
}

func isContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *schemaVersionSyncer) OwnerCheckAllVersions(ctx context.Context, latestVer int64) error {
	startTime := time.Now()
	time.Sleep(CheckVersFirstWaitTime)
	notMatchVerCnt := 0
	intervalCnt := int(time.Second / checkVersInterval)
	uFIDelatedMap := make(map[string]struct{})

	var err error
	defer func() {
		metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerCheckAllVersions, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	for {
		if isContextDone(ctx) {
			// ctx is canceled or timeout.
			err = errors.Trace(ctx.Err())
			return err
		}

		resp, err := s.etcdCli.Get(ctx, DBSAllSchemaVersions, clientv3.WithPrefix())
		if err != nil {
			logutil.BgLogger().Info("[dbs] syncer check all versions failed, continue checking.", zap.Error(err))
			continue
		}

		succ := true
		for _, ekv := range resp.Kvs {
			if _, ok := uFIDelatedMap[string(ekv.Key)]; ok {
				continue
			}

			ver, err := strconv.Atoi(string(ekv.Value))
			if err != nil {
				logutil.BgLogger().Info("[dbs] syncer check all versions, convert value to int failed, continue checking.", zap.String("dbs", string(ekv.Key)), zap.String("value", string(ekv.Value)), zap.Error(err))
				succ = false
				break
			}
			if int64(ver) < latestVer {
				if notMatchVerCnt%intervalCnt == 0 {
					logutil.BgLogger().Info("[dbs] syncer check all versions, someone is not synced, continue checking",
						zap.String("dbs", string(ekv.Key)), zap.Int("currentVer", ver), zap.Int64("latestVer", latestVer))
				}
				succ = false
				notMatchVerCnt++
				break
			}
			uFIDelatedMap[string(ekv.Key)] = struct{}{}
		}
		if succ {
			return nil
		}
		time.Sleep(checkVersInterval)
	}
}

const (
	oFIDelefaultRetryCnt = 10
	failedGetTTLLimit    = 20
	oFIDelefaultTimeout  = 3 * time.Second
	opRetryInterval      = 500 * time.Millisecond
)

// NeededCleanTTL is exported for testing.
var NeededCleanTTL = int64(-60)

func (s *schemaVersionSyncer) StartCleanWork() {
	defer milevadbutil.Recover(metrics.LabelDBSSyncer, "StartCleanWorker", nil, false)

	for {
		select {
		case <-s.notifyCleanExpiredPathsCh:
			if !s.ownerChecker.IsOwner() {
				continue
			}

			for i := 0; i < oFIDelefaultRetryCnt; i++ {
				childCtx, cancelFunc := context.WithTimeout(s.ctx, oFIDelefaultTimeout)
				resp, err := s.etcdCli.Leases(childCtx)
				cancelFunc()
				if err != nil {
					logutil.BgLogger().Info("[dbs] syncer clean expired paths, failed to get leases.", zap.Error(err))
					continue
				}

				if isFinished := s.doCleanExpirePaths(resp.Leases); isFinished {
					break
				}
				time.Sleep(opRetryInterval)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *schemaVersionSyncer) Close() {
	s.cancel()

	err := s.removeSelfVersionPath()
	if err != nil {
		logutil.BgLogger().Error("[dbs] remove self version path failed", zap.Error(err))
	}
}

func (s *schemaVersionSyncer) NotifyCleanExpiredPaths() bool {
	var isNotified bool
	var err error
	startTime := time.Now()
	select {
	case s.notifyCleanExpiredPathsCh <- struct{}{}:
		isNotified = true
	default:
		err = errors.New("channel is full, failed to notify clean expired paths")
	}
	metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerNotifyCleanExpirePaths, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return isNotified
}

func (s *schemaVersionSyncer) doCleanExpirePaths(leases []clientv3.LeaseStatus) bool {
	failedGetIDs := 0
	failedRevokeIDs := 0
	startTime := time.Now()

	defer func() {
		metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerCleanExpirePaths, metrics.RetLabel(nil)).Observe(time.Since(startTime).Seconds())
	}()
	// TODO: Now LeaseStatus only has lease ID.
	for _, lease := range leases {
		// The DBS owner key uses '%x', so here print it too.
		leaseID := fmt.Sprintf("%x, %d", lease.ID, lease.ID)
		childCtx, cancelFunc := context.WithTimeout(s.ctx, oFIDelefaultTimeout)
		ttlResp, err := s.etcdCli.TimeToLive(childCtx, lease.ID)
		cancelFunc()
		if err != nil {
			logutil.BgLogger().Info("[dbs] syncer clean expired paths, failed to get one TTL.", zap.String("leaseID", leaseID), zap.Error(err))
			failedGetIDs++
			continue
		}

		if failedGetIDs > failedGetTTLLimit {
			return false
		}
		if ttlResp.TTL >= NeededCleanTTL {
			continue
		}

		st := time.Now()
		childCtx, cancelFunc = context.WithTimeout(s.ctx, oFIDelefaultTimeout)
		_, err = s.etcdCli.Revoke(childCtx, lease.ID)
		cancelFunc()
		if err != nil && terror.ErrorEqual(err, rpctypes.ErrLeaseNotFound) {
			logutil.BgLogger().Warn("[dbs] syncer clean expired paths, failed to revoke lease.", zap.String("leaseID", leaseID),
				zap.Int64("TTL", ttlResp.TTL), zap.Error(err))
			failedRevokeIDs++
		}
		logutil.BgLogger().Warn("[dbs] syncer clean expired paths,", zap.String("leaseID", leaseID), zap.Int64("TTL", ttlResp.TTL))
		metrics.OwnerHandleSyncerHistogram.WithLabelValues(metrics.OwnerCleanOneExpirePath, metrics.RetLabel(err)).Observe(time.Since(st).Seconds())
	}

	if failedGetIDs == 0 && failedRevokeIDs == 0 {
		return true
	}
	return false
}
