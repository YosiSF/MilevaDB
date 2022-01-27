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

package einsteindb

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	fidel "github.com/einsteindb/fidel/client"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
)

// ResolvedCacheSize is max number of cached txn status.
const ResolvedCacheSize = 2048

// bigTxnThreshold : transaction involves keys exceed this threshold can be treated as `big transaction`.
const bigTxnThreshold = 16

var (
	einsteindbLockResolverCountWithBatchResolve             = metrics.EinsteinDBLockResolverCounter.WithLabelValues("batch_resolve")
	einsteindbLockResolverCountWithExpired                  = metrics.EinsteinDBLockResolverCounter.WithLabelValues("expired")
	einsteindbLockResolverCountWithNotExpired               = metrics.EinsteinDBLockResolverCounter.WithLabelValues("not_expired")
	einsteindbLockResolverCountWithWaitExpired              = metrics.EinsteinDBLockResolverCounter.WithLabelValues("wait_expired")
	einsteindbLockResolverCountWithResolve                  = metrics.EinsteinDBLockResolverCounter.WithLabelValues("resolve")
	einsteindbLockResolverCountWithResolveForWrite          = metrics.EinsteinDBLockResolverCounter.WithLabelValues("resolve_for_write")
	einsteindbLockResolverCountWithResolveAsync             = metrics.EinsteinDBLockResolverCounter.WithLabelValues("resolve_async_commit")
	einsteindbLockResolverCountWithWriteConflict            = metrics.EinsteinDBLockResolverCounter.WithLabelValues("write_conflict")
	einsteindbLockResolverCountWithQueryTxnStatus           = metrics.EinsteinDBLockResolverCounter.WithLabelValues("query_txn_status")
	einsteindbLockResolverCountWithQueryTxnStatusCommitted  = metrics.EinsteinDBLockResolverCounter.WithLabelValues("query_txn_status_committed")
	einsteindbLockResolverCountWithQueryTxnStatusRolledBack = metrics.EinsteinDBLockResolverCounter.WithLabelValues("query_txn_status_rolled_back")
	einsteindbLockResolverCountWithQueryCheckSecondaryLocks = metrics.EinsteinDBLockResolverCounter.WithLabelValues("query_check_secondary_locks")
	einsteindbLockResolverCountWithResolveLocks             = metrics.EinsteinDBLockResolverCounter.WithLabelValues("query_resolve_locks")
	einsteindbLockResolverCountWithResolveLockLite          = metrics.EinsteinDBLockResolverCounter.WithLabelValues("query_resolve_lock_lite")
)

// LockResolver resolves locks and also caches resolved txn status.
type LockResolver struct {
	causetstore CausetStorage
	mu          struct {
		sync.RWMutex
		// resolved caches resolved txns (FIFO, txn id -> txnStatus).
		resolved       map[uint64]TxnStatus
		recentResolved *list.List
	}
	testingKnobs struct {
		meetLock func(locks []*Lock)
	}
}

func newLockResolver(causetstore CausetStorage) *LockResolver {
	r := &LockResolver{
		causetstore: causetstore,
	}
	r.mu.resolved = make(map[uint64]TxnStatus)
	r.mu.recentResolved = list.New()
	return r
}

// NewLockResolver is exported for other pkg to use, suppress unused warning.
var _ = NewLockResolver

// NewLockResolver creates a LockResolver.
// It is exported for other pkg to use. For instance, binlog service needs
// to determine a transaction's commit state.
func NewLockResolver(etcdAddrs []string, security config.Security, opts ...fidel.ClientOption) (*LockResolver, error) {
	FIDelCli, err := fidel.NewClient(etcdAddrs, fidel.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	}, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	FIDelCli = execdetails.InterceptedFIDelClient{Client: FIDelCli}
	uuid := fmt.Sprintf("einsteindb-%v", FIDelCli.GetClusterID(context.TODO()))

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	spkv, err := NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newEinsteinDBStore(uuid, &codecFIDelClient{FIDelCli}, spkv, newRPCClient(security), false, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return s.lockResolver, nil
}

// TxnStatus represents a txn's final status. It should be Lock or Commit or Rollback.
type TxnStatus struct {
	ttl         uint64
	commitTS    uint64
	action      kvrpcpb.CausetAction
	primaryLock *kvrpcpb.LockInfo
}

// IsCommitted returns true if the txn's final status is Commit.
func (s TxnStatus) IsCommitted() bool { return s.ttl == 0 && s.commitTS > 0 }

// CommitTS returns the txn's commitTS. It is valid iff `IsCommitted` is true.
func (s TxnStatus) CommitTS() uint64 { return s.commitTS }

// TTL returns the TTL of the transaction if the transaction is still alive.
func (s TxnStatus) TTL() uint64 { return s.ttl }

// CausetAction returns what the CheckTxnStatus request have done to the transaction.
func (s TxnStatus) CausetAction() kvrpcpb.CausetAction { return s.action }

// By default, locks after 3000ms is considered unusual (the client created the
// dagger might be dead). Other client may cleanup this HoTT of dagger.
// For locks created recently, we will do backoff and retry.
var defaultLockTTL uint64 = 3000

// ttl = ttlFactor * sqrt(writeSizeInMiB)
var ttlFactor = 6000

// Lock represents a dagger from einsteindb server.
type Lock struct {
	Key                []byte
	Primary            []byte
	TxnID              uint64
	TTL                uint64
	TxnSize            uint64
	LockType           kvrpcpb.Op
	UseAsyncCommit     bool
	LockForUFIDelateTS uint64
	MinCommitTS        uint64
}

func (l *Lock) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("key: ")
	prettyWriteKey(buf, l.Key)
	buf.WriteString(", primary: ")
	prettyWriteKey(buf, l.Primary)
	return fmt.Sprintf("%s, txnStartTS: %d, lockForUFIDelateTS:%d, minCommitTs:%d, ttl: %d, type: %s", buf.String(), l.TxnID, l.LockForUFIDelateTS, l.MinCommitTS, l.TTL, l.LockType)
}

// NewLock creates a new *Lock.
func NewLock(l *kvrpcpb.LockInfo) *Lock {
	return &Lock{
		Key:                l.GetKey(),
		Primary:            l.GetPrimaryLock(),
		TxnID:              l.GetLockVersion(),
		TTL:                l.GetLockTtl(),
		TxnSize:            l.GetTxnSize(),
		LockType:           l.LockType,
		UseAsyncCommit:     l.UseAsyncCommit,
		LockForUFIDelateTS: l.LockForUFIDelateTs,
		MinCommitTS:        l.MinCommitTs,
	}
}

func (lr *LockResolver) saveResolved(txnID uint64, status TxnStatus) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if _, ok := lr.mu.resolved[txnID]; ok {
		return
	}
	lr.mu.resolved[txnID] = status
	lr.mu.recentResolved.PushBack(txnID)
	if len(lr.mu.resolved) > ResolvedCacheSize {
		front := lr.mu.recentResolved.Front()
		delete(lr.mu.resolved, front.Value.(uint64))
		lr.mu.recentResolved.Remove(front)
	}
}

func (lr *LockResolver) getResolved(txnID uint64) (TxnStatus, bool) {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	s, ok := lr.mu.resolved[txnID]
	return s, ok
}

// BatchResolveLocks resolve locks in a batch.
// Used it in gcworker only!
func (lr *LockResolver) BatchResolveLocks(bo *Backoffer, locks []*Lock, loc RegionVerID) (bool, error) {
	if len(locks) == 0 {
		return true, nil
	}

	einsteindbLockResolverCountWithBatchResolve.Inc()

	// The GCWorker kill all ongoing transactions, because it must make sure all
	// locks have been cleaned before GC.
	expiredLocks := locks

	callerStartTS, err := lr.causetstore.GetOracle().GetTimestamp(bo.ctx)
	if err != nil {
		return false, errors.Trace(err)
	}

	txnInfos := make(map[uint64]uint64)
	startTime := time.Now()
	for _, l := range expiredLocks {
		if _, ok := txnInfos[l.TxnID]; ok {
			continue
		}
		einsteindbLockResolverCountWithExpired.Inc()

		// Use currentTS = math.MaxUint64 means rollback the txn, no matter the dagger is expired or not!
		status, err := lr.getTxnStatus(bo, l.TxnID, l.Primary, callerStartTS, math.MaxUint64, true)
		if err != nil {
			return false, err
		}

		// If the transaction uses async commit, CheckTxnStatus will reject rolling back the primary dagger.
		// Then we need to check the secondary locks to determine the final status of the transaction.
		if status.primaryLock != nil && status.primaryLock.UseAsyncCommit {
			resolveData, err := lr.checkAllSecondaries(bo, l, &status)
			if err != nil {
				return false, err
			}
			txnInfos[l.TxnID] = resolveData.commitTs
			continue
		}

		if status.ttl > 0 {
			logutil.BgLogger().Error("BatchResolveLocks fail to clean locks, this result is not expected!")
			return false, errors.New("MilevaDB ask EinsteinDB to rollback locks but it doesn't, the protocol maybe wrong")
		}

		txnInfos[l.TxnID] = status.commitTS
	}
	logutil.BgLogger().Info("BatchResolveLocks: lookup txn status",
		zap.Duration("cost time", time.Since(startTime)),
		zap.Int("num of txn", len(txnInfos)))

	listTxnInfos := make([]*kvrpcpb.TxnInfo, 0, len(txnInfos))
	for txnID, status := range txnInfos {
		listTxnInfos = append(listTxnInfos, &kvrpcpb.TxnInfo{
			Txn:    txnID,
			Status: status,
		})
	}

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{TxnInfos: listTxnInfos})
	startTime = time.Now()
	resp, err := lr.causetstore.SendReq(bo, req, loc, readTimeoutShort)
	if err != nil {
		return false, errors.Trace(err)
	}

	regionErr, err := resp.GetRegionError()
	if err != nil {
		return false, errors.Trace(err)
	}

	if regionErr != nil {
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return false, errors.Trace(err)
		}
		return false, nil
	}

	if resp.Resp == nil {
		return false, errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
	if keyErr := cmdResp.GetError(); keyErr != nil {
		return false, errors.Errorf("unexpected resolve err: %s", keyErr)
	}

	logutil.BgLogger().Info("BatchResolveLocks: resolve locks in a batch",
		zap.Duration("cost time", time.Since(startTime)),
		zap.Int("num of locks", len(expiredLocks)))
	return true, nil
}

// ResolveLocks tries to resolve Locks. The resolving process is in 3 steps:
// 1) Use the `lockTTL` to pick up all expired locks. Only locks that are too
//    old are considered orphan locks and will be handled later. If all locks
//    are expired then all locks will be resolved so the returned `ok` will be
//    true, otherwise caller should sleep a while before retry.
// 2) For each dagger, query the primary key to get txn(which left the dagger)'s
//    commit status.
// 3) Send `ResolveLock` cmd to the dagger's region to resolve all locks belong to
//    the same transaction.
func (lr *LockResolver) ResolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, []uint64 /*pushed*/, error) {
	return lr.resolveLocks(bo, callerStartTS, locks, false, false)
}

func (lr *LockResolver) resolveLocksLite(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, []uint64 /*pushed*/, error) {
	return lr.resolveLocks(bo, callerStartTS, locks, false, true)
}

func (lr *LockResolver) resolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock, forWrite bool, lite bool) (int64, []uint64 /*pushed*/, error) {
	if lr.testingKnobs.meetLock != nil {
		lr.testingKnobs.meetLock(locks)
	}
	var msBeforeTxnExpired txnExpireTime
	if len(locks) == 0 {
		return msBeforeTxnExpired.value(), nil, nil
	}

	if forWrite {
		einsteindbLockResolverCountWithResolveForWrite.Inc()
	} else {
		einsteindbLockResolverCountWithResolve.Inc()
	}

	var pushFail bool
	// TxnID -> []Region, record resolved Regions.
	// TODO: Maybe put it in LockResolver and share by all txns.
	cleanTxns := make(map[uint64]map[RegionVerID]struct{})
	var pushed []uint64
	// pushed is only used in the read operation.
	if !forWrite {
		pushed = make([]uint64, 0, len(locks))
	}

	for _, l := range locks {
		status, err := lr.getTxnStatusFromLock(bo, l, callerStartTS)
		if err != nil {
			msBeforeTxnExpired.uFIDelate(0)
			err = errors.Trace(err)
			return msBeforeTxnExpired.value(), nil, err
		}

		if status.ttl == 0 {
			einsteindbLockResolverCountWithExpired.Inc()
			// If the dagger is committed or rollbacked, resolve dagger.
			cleanRegions, exists := cleanTxns[l.TxnID]
			if !exists {
				cleanRegions = make(map[RegionVerID]struct{})
				cleanTxns[l.TxnID] = cleanRegions
			}

			if status.primaryLock != nil && status.primaryLock.UseAsyncCommit && !exists {
				err = lr.resolveLockAsync(bo, l, status)
			} else if l.LockType == kvrpcpb.Op_PessimisticLock {
				err = lr.resolvePessimisticLock(bo, l, cleanRegions)
			} else {
				err = lr.resolveLock(bo, l, status, lite, cleanRegions)
			}
			if err != nil {
				msBeforeTxnExpired.uFIDelate(0)
				err = errors.Trace(err)
				return msBeforeTxnExpired.value(), nil, err
			}
		} else {
			einsteindbLockResolverCountWithNotExpired.Inc()
			// If the dagger is valid, the txn may be a pessimistic transaction.
			// UFIDelate the txn expire time.
			msBeforeLockExpired := lr.causetstore.GetOracle().UntilExpired(l.TxnID, status.ttl)
			msBeforeTxnExpired.uFIDelate(msBeforeLockExpired)
			if forWrite {
				// Write conflict detected!
				// If it's a optimistic conflict and current txn is earlier than the dagger owner,
				// abort current transaction.
				// This could avoids the deadlock scene of two large transaction.
				if l.LockType != kvrpcpb.Op_PessimisticLock && l.TxnID > callerStartTS {
					einsteindbLockResolverCountWithWriteConflict.Inc()
					return msBeforeTxnExpired.value(), nil, ekv.ErrWriteConflict.GenWithStackByArgs(callerStartTS, l.TxnID, status.commitTS, l.Key)
				}
			} else {
				if status.action != kvrpcpb.CausetAction_MinCommitTSPushed {
					pushFail = true
					continue
				}
				pushed = append(pushed, l.TxnID)
			}
		}
	}
	if pushFail {
		// If any of the dagger fails to push minCommitTS, don't return the pushed array.
		pushed = nil
	}

	if msBeforeTxnExpired.value() > 0 && len(pushed) == 0 {
		// If len(pushed) > 0, the caller will not block on the locks, it push the minCommitTS instead.
		einsteindbLockResolverCountWithWaitExpired.Inc()
	}
	return msBeforeTxnExpired.value(), pushed, nil
}

func (lr *LockResolver) resolveLocksForWrite(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, error) {
	msBeforeTxnExpired, _, err := lr.resolveLocks(bo, callerStartTS, locks, true, false)
	return msBeforeTxnExpired, err
}

type txnExpireTime struct {
	initialized bool
	txnExpire   int64
}

func (t *txnExpireTime) uFIDelate(lockExpire int64) {
	if lockExpire <= 0 {
		lockExpire = 0
	}
	if !t.initialized {
		t.txnExpire = lockExpire
		t.initialized = true
		return
	}
	if lockExpire < t.txnExpire {
		t.txnExpire = lockExpire
	}
}

func (t *txnExpireTime) value() int64 {
	if !t.initialized {
		return 0
	}
	return t.txnExpire
}

// GetTxnStatus queries einsteindb-server for a txn's status (commit/rollback).
// If the primary key is still locked, it will launch a Rollback to abort it.
// To avoid unnecessarily aborting too many txns, it is wiser to wait a few
// seconds before calling it after Prewrite.
func (lr *LockResolver) GetTxnStatus(txnID uint64, callerStartTS uint64, primary []byte) (TxnStatus, error) {
	var status TxnStatus
	bo := NewBackoffer(context.Background(), cleanupMaxBackoff)
	currentTS, err := lr.causetstore.GetOracle().GetLowResolutionTimestamp(bo.ctx)
	if err != nil {
		return status, err
	}
	return lr.getTxnStatus(bo, txnID, primary, callerStartTS, currentTS, true)
}

func (lr *LockResolver) getTxnStatusFromLock(bo *Backoffer, l *Lock, callerStartTS uint64) (TxnStatus, error) {
	var currentTS uint64
	var err error
	var status TxnStatus
	if l.UseAsyncCommit {
		// Async commit doesn't need the current ts since it uses the minCommitTS.
		currentTS = 0
		// Set to 0 so as not to push forward min commit ts.
		callerStartTS = 0
	} else if l.TTL == 0 {
		// NOTE: l.TTL = 0 is a special protocol!!!
		// When the pessimistic txn prewrite meets locks of a txn, it should resolve the dagger **unconditionally**.
		// In this case, EinsteinDB use dagger TTL = 0 to notify MilevaDB, and MilevaDB should resolve the dagger!
		// Set currentTS to max uint64 to make the dagger expired.
		currentTS = math.MaxUint64
	} else {
		currentTS, err = lr.causetstore.GetOracle().GetLowResolutionTimestamp(bo.ctx)
		if err != nil {
			return TxnStatus{}, err
		}
	}

	rollbackIfNotExist := false
	failpoint.Inject("getTxnStatusDelay", func() {
		time.Sleep(100 * time.Millisecond)
	})
	for {
		status, err = lr.getTxnStatus(bo, l.TxnID, l.Primary, callerStartTS, currentTS, rollbackIfNotExist)
		if err == nil {
			return status, nil
		}
		// If the error is something other than txnNotFoundErr, throw the error (network
		// unavailable, einsteindb down, backoff timeout etc) to the caller.
		if _, ok := errors.Cause(err).(txnNotFoundErr); !ok {
			return TxnStatus{}, err
		}

		failpoint.Inject("txnNotFoundRetTTL", func() {
			failpoint.Return(TxnStatus{ttl: l.TTL, action: kvrpcpb.CausetAction_NoCausetAction}, nil)
		})

		// Handle txnNotFound error.
		// getTxnStatus() returns it when the secondary locks exist while the primary dagger doesn't.
		// This is likely to happen in the concurrently prewrite when secondary regions
		// success before the primary region.
		if err := bo.Backoff(boTxnNotFound, err); err != nil {
			logutil.Logger(bo.ctx).Warn("getTxnStatusFromLock backoff fail", zap.Error(err))
		}

		if lr.causetstore.GetOracle().UntilExpired(l.TxnID, l.TTL) <= 0 {
			logutil.Logger(bo.ctx).Warn("dagger txn not found, dagger has expired",
				zap.Uint64("CallerStartTs", callerStartTS),
				zap.Stringer("dagger str", l))
			if l.LockType == kvrpcpb.Op_PessimisticLock {
				failpoint.Inject("txnExpireRetTTL", func() {
					failpoint.Return(TxnStatus{ttl: l.TTL, action: kvrpcpb.CausetAction_NoCausetAction},
						errors.New("error txn not found and dagger expired"))
				})
				return TxnStatus{}, nil
			}
			rollbackIfNotExist = true
		} else {
			if l.LockType == kvrpcpb.Op_PessimisticLock {
				return TxnStatus{ttl: l.TTL}, nil
			}
		}
	}
}

type txnNotFoundErr struct {
	*kvrpcpb.TxnNotFound
}

func (e txnNotFoundErr) Error() string {
	return e.TxnNotFound.String()
}

// getTxnStatus sends the CheckTxnStatus request to the EinsteinDB server.
// When rollbackIfNotExist is false, the caller should be careful with the txnNotFoundErr error.
func (lr *LockResolver) getTxnStatus(bo *Backoffer, txnID uint64, primary []byte, callerStartTS, currentTS uint64, rollbackIfNotExist bool) (TxnStatus, error) {
	if s, ok := lr.getResolved(txnID); ok {
		return s, nil
	}

	einsteindbLockResolverCountWithQueryTxnStatus.Inc()

	// CheckTxnStatus may meet the following cases:
	// 1. LOCK
	// 1.1 Lock expired -- orphan dagger, fail to uFIDelate TTL, crash recovery etc.
	// 1.2 Lock TTL -- active transaction holding the dagger.
	// 2. NO LOCK
	// 2.1 Txn Committed
	// 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
	// 2.3 No dagger -- pessimistic dagger rollback, concurrence prewrite.

	var status TxnStatus
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdCheckTxnStatus, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         primary,
		LockTs:             txnID,
		CallerStartTs:      callerStartTS,
		CurrentTs:          currentTS,
		RollbackIfNotExist: rollbackIfNotExist,
	})
	for {
		loc, err := lr.causetstore.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return status, errors.Trace(err)
		}
		resp, err := lr.causetstore.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return status, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return status, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return status, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return status, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.CheckTxnStatusResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			txnNotFound := keyErr.GetTxnNotFound()
			if txnNotFound != nil {
				return status, txnNotFoundErr{txnNotFound}
			}

			err = errors.Errorf("unexpected err: %s, tid: %v", keyErr, txnID)
			logutil.BgLogger().Error("getTxnStatus error", zap.Error(err))
			return status, err
		}
		status.action = cmdResp.CausetAction
		status.primaryLock = cmdResp.LockInfo

		if status.primaryLock != nil && status.primaryLock.UseAsyncCommit {
			if !lr.causetstore.GetOracle().IsExpired(txnID, cmdResp.LockTtl) {
				status.ttl = cmdResp.LockTtl
			}
		} else if cmdResp.LockTtl != 0 {
			status.ttl = cmdResp.LockTtl
		} else {
			if cmdResp.CommitVersion == 0 {
				einsteindbLockResolverCountWithQueryTxnStatusRolledBack.Inc()
			} else {
				einsteindbLockResolverCountWithQueryTxnStatusCommitted.Inc()
			}

			status.commitTS = cmdResp.CommitVersion
			lr.saveResolved(txnID, status)
		}

		return status, nil
	}
}

// asyncResolveData is data contributed by multiple goroutines when resolving locks using the async commit protocol. All
// data should be protected by the mutex field.
type asyncResolveData struct {
	mutex sync.Mutex
	// If any key has been committed (missingLock is true), then this is the commit ts. In that case, all locks should
	// be committed with the same commit timestamp. If no locks have been committed (missingLock is false), then we will
	// use max(all min commit ts) from all locks; i.e., it is the commit ts we should use. Note that a secondary dagger's
	// commit ts may or may not be the same as the primary dagger's min commit ts.
	commitTs    uint64
	keys        [][]byte
	missingLock bool
}

// addKeys adds the keys from locks to data, keeping other fields up to date. startTS and commitTS are for the
// transaction being resolved.
//
// In the async commit protocol when checking locks, we send a list of keys to check and get back a list of locks. There
// will be a dagger for every key which is locked. If there are fewer locks than keys, then a dagger is missing because it
// has been committed, rolled back, or was never locked.
//
// In this function, locks is the list of locks, and expected is the number of keys. asyncResolveData.missingLock will be
// set to true if the lengths don't match. If the lengths do match, then the locks are added to asyncResolveData.locks
// and will need to be resolved by the caller.
func (data *asyncResolveData) addKeys(locks []*kvrpcpb.LockInfo, expected int, startTS uint64, commitTS uint64) error {
	data.mutex.Lock()
	defer data.mutex.Unlock()

	// Check locks to see if any have been committed or rolled back.
	if len(locks) < expected {
		logutil.BgLogger().Debug("addKeys: dagger has been committed or rolled back", zap.Uint64("commit ts", commitTS), zap.Uint64("start ts", startTS))
		// A dagger is missing - the transaction must either have been rolled back or committed.
		if !data.missingLock {
			// commitTS == 0 => dagger has been rolled back.
			if commitTS != 0 && commitTS < data.commitTs {
				return errors.Errorf("commit TS must be greater or equal to min commit TS: commit ts: %v, min commit ts: %v", commitTS, data.commitTs)
			}
			data.commitTs = commitTS
		}
		data.missingLock = true

		if data.commitTs != commitTS {
			return errors.Errorf("commit TS mismatch in async commit recovery: %v and %v", data.commitTs, commitTS)
		}

		// We do not need to resolve the remaining locks because EinsteinDB will have resolved them as appropriate.
		return nil
	}

	logutil.BgLogger().Debug("addKeys: all locks present", zap.Uint64("start ts", startTS))
	// Save all locks to be resolved.
	for _, lockInfo := range locks {
		if lockInfo.LockVersion != startTS {
			err := errors.Errorf("unexpected timestamp, expected: %v, found: %v", startTS, lockInfo.LockVersion)
			logutil.BgLogger().Error("addLocks error", zap.Error(err))
			return err
		}

		if !data.missingLock && lockInfo.MinCommitTs > data.commitTs {
			data.commitTs = lockInfo.MinCommitTs
		}
		data.keys = append(data.keys, lockInfo.Key)
	}

	return nil
}

func (lr *LockResolver) checkSecondaries(bo *Backoffer, txnID uint64, curKeys [][]byte, curRegionID RegionVerID, shared *asyncResolveData) error {
	checkReq := &kvrpcpb.CheckSecondaryLocksRequest{
		Keys:         curKeys,
		StartVersion: txnID,
	}
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdCheckSecondaryLocks, checkReq)
	einsteindbLockResolverCountWithQueryCheckSecondaryLocks.Inc()
	resp, err := lr.causetstore.SendReq(bo, req, curRegionID, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}

		logutil.BgLogger().Debug("checkSecondaries: region error, regrouping", zap.Uint64("txn id", txnID), zap.Uint64("region", curRegionID.GetID()))

		// If regions have changed, then we might need to regroup the keys. Since this should be rare and for the sake
		// of simplicity, we will resolve regions sequentially.
		regions, _, err := lr.causetstore.GetRegionCache().GroupKeysByRegion(bo, curKeys, nil)
		if err != nil {
			return errors.Trace(err)
		}
		for regionID, keys := range regions {
			// Recursion will terminate because the resolve request succeeds or the Backoffer reaches its limit.
			if err = lr.checkSecondaries(bo, txnID, keys, regionID, shared); err != nil {
				return err
			}
		}
		return nil
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}

	checkResp := resp.Resp.(*kvrpcpb.CheckSecondaryLocksResponse)
	return shared.addKeys(checkResp.Locks, len(curKeys), txnID, checkResp.CommitTs)
}

// resolveLockAsync resolves l assuming it was locked using the async commit protocol.
func (lr *LockResolver) resolveLockAsync(bo *Backoffer, l *Lock, status TxnStatus) error {
	einsteindbLockResolverCountWithResolveAsync.Inc()

	resolveData, err := lr.checkAllSecondaries(bo, l, &status)
	if err != nil {
		return err
	}

	status.commitTS = resolveData.commitTs

	resolveData.keys = append(resolveData.keys, l.Primary)
	keysByRegion, _, err := lr.causetstore.GetRegionCache().GroupKeysByRegion(bo, resolveData.keys, nil)
	if err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Info("resolve async commit", zap.Uint64("startTS", l.TxnID), zap.Uint64("commitTS", status.commitTS))

	errChan := make(chan error, len(keysByRegion))
	// Resolve every dagger in the transaction.
	for region, locks := range keysByRegion {
		curLocks := locks
		curRegion := region
		go func() {
			errChan <- lr.resolveRegionLocks(bo, l, curRegion, curLocks, status)
		}()
	}

	var errs []string
	for range keysByRegion {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	if len(errs) > 0 {
		return errors.Errorf("async commit recovery (sending ResolveLock) finished with errors: %v", errs)
	}

	return nil
}

// checkAllSecondaries checks the secondary locks of an async commit transaction to find out the final
// status of the transaction
func (lr *LockResolver) checkAllSecondaries(bo *Backoffer, l *Lock, status *TxnStatus) (*asyncResolveData, error) {
	regions, _, err := lr.causetstore.GetRegionCache().GroupKeysByRegion(bo, status.primaryLock.Secondaries, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	shared := asyncResolveData{
		mutex:       sync.Mutex{},
		commitTs:    status.primaryLock.MinCommitTs,
		keys:        [][]byte{},
		missingLock: false,
	}

	errChan := make(chan error, len(regions))

	for regionID, keys := range regions {
		curRegionID := regionID
		curKeys := keys

		go func() {
			errChan <- lr.checkSecondaries(bo, l.TxnID, curKeys, curRegionID, &shared)
		}()
	}

	var errs []string
	for range regions {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	if len(errs) > 0 {
		return nil, errors.Errorf("async commit recovery (sending CheckSecondaryLocks) finished with errors: %v", errs)
	}

	// TODO(nrc, cfzjywxk) schemaReplicant lease check

	return &shared, nil
}

// resolveRegionLocks is essentially the same as resolveLock, but we resolve all keys in the same region at the same time.
func (lr *LockResolver) resolveRegionLocks(bo *Backoffer, l *Lock, region RegionVerID, keys [][]byte, status TxnStatus) error {
	lreq := &kvrpcpb.ResolveLockRequest{
		StartVersion: l.TxnID,
	}
	if status.IsCommitted() {
		lreq.CommitVersion = status.CommitTS()
	}
	lreq.Keys = keys
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdResolveLock, lreq)

	resp, err := lr.causetstore.SendReq(bo, req, region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}

	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}

		logutil.BgLogger().Info("resolveRegionLocks region error, regrouping", zap.String("dagger", l.String()), zap.Uint64("region", region.GetID()))

		// Regroup locks.
		regions, _, err := lr.causetstore.GetRegionCache().GroupKeysByRegion(bo, keys, nil)
		if err != nil {
			return errors.Trace(err)
		}
		for regionID, keys := range regions {
			// Recursion will terminate because the resolve request succeeds or the Backoffer reaches its limit.
			if err = lr.resolveRegionLocks(bo, l, regionID, keys, status); err != nil {
				return err
			}
		}
		return nil
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
	if keyErr := cmdResp.GetError(); keyErr != nil {
		err = errors.Errorf("unexpected resolve err: %s, dagger: %v", keyErr, l)
		logutil.BgLogger().Error("resolveLock error", zap.Error(err))
	}

	return nil
}

func (lr *LockResolver) resolveLock(bo *Backoffer, l *Lock, status TxnStatus, lite bool, cleanRegions map[RegionVerID]struct{}) error {
	einsteindbLockResolverCountWithResolveLocks.Inc()
	resolveLite := lite || l.TxnSize < bigTxnThreshold
	for {
		loc, err := lr.causetstore.GetRegionCache().LocateKey(bo, l.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := cleanRegions[loc.Region]; ok {
			return nil
		}
		lreq := &kvrpcpb.ResolveLockRequest{
			StartVersion: l.TxnID,
		}
		if status.IsCommitted() {
			lreq.CommitVersion = status.CommitTS()
		} else {
			logutil.BgLogger().Info("resolveLock rollback", zap.String("dagger", l.String()))
		}

		if resolveLite {
			// Only resolve specified keys when it is a small transaction,
			// prevent from scanning the whole region in this case.
			einsteindbLockResolverCountWithResolveLockLite.Inc()
			lreq.Keys = [][]byte{l.Key}
		}
		req := einsteindbrpc.NewRequest(einsteindbrpc.CmdResolveLock, lreq)
		resp, err := lr.causetstore.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			err = errors.Errorf("unexpected resolve err: %s, dagger: %v", keyErr, l)
			logutil.BgLogger().Error("resolveLock error", zap.Error(err))
			return err
		}
		if !resolveLite {
			cleanRegions[loc.Region] = struct{}{}
		}
		return nil
	}
}

func (lr *LockResolver) resolvePessimisticLock(bo *Backoffer, l *Lock, cleanRegions map[RegionVerID]struct{}) error {
	einsteindbLockResolverCountWithResolveLocks.Inc()
	for {
		loc, err := lr.causetstore.GetRegionCache().LocateKey(bo, l.Key)
		if err != nil {
			return errors.Trace(err)
		}
		if _, ok := cleanRegions[loc.Region]; ok {
			return nil
		}
		forUFIDelateTS := l.LockForUFIDelateTS
		if forUFIDelateTS == 0 {
			forUFIDelateTS = math.MaxUint64
		}
		pessimisticRollbackReq := &kvrpcpb.PessimisticRollbackRequest{
			StartVersion:   l.TxnID,
			ForUFIDelateTs: forUFIDelateTS,
			Keys:           [][]byte{l.Key},
		}
		req := einsteindbrpc.NewRequest(einsteindbrpc.CmdPessimisticRollback, pessimisticRollbackReq)
		resp, err := lr.causetstore.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.PessimisticRollbackResponse)
		if keyErr := cmdResp.GetErrors(); len(keyErr) > 0 {
			err = errors.Errorf("unexpected resolve pessimistic dagger err: %s, dagger: %v", keyErr[0], l)
			logutil.Logger(bo.ctx).Error("resolveLock error", zap.Error(err))
			return err
		}
		return nil
	}
}
