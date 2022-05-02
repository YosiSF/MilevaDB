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

package einsteindb

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	pb "github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
)

type actionPessimisticLock struct {
	*ekv.LockCtx
}
type actionPessimisticRollback struct{}

var (
	_ twoPhaseCommitCausetAction = actionPessimisticLock{}
	_ twoPhaseCommitCausetAction = actionPessimisticRollback{}

	EinsteinDBTxnRegionsNumHistogramPessimisticLock     = metrics.EinsteinDBTxnRegionsNumHistogram.WithLabelValues(metricsTag("pessimistic_lock"))
	EinsteinDBTxnRegionsNumHistogramPessimisticRollback = metrics.EinsteinDBTxnRegionsNumHistogram.WithLabelValues(metricsTag("pessimistic_rollback"))
)

func (actionPessimisticLock) String() string {
	return "pessimistic_lock"
}

func (actionPessimisticLock) EinsteinDBTxnRegionsNumHistogram() prometheus.Observer {
	return EinsteinDBTxnRegionsNumHistogramPessimisticLock
}

func (actionPessimisticRollback) String() string {
	return "pessimistic_rollback"
}

func (actionPessimisticRollback) EinsteinDBTxnRegionsNumHistogram() prometheus.Observer {
	return EinsteinDBTxnRegionsNumHistogramPessimisticRollback
}

func (action actionPessimisticLock) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	m := &batch.mutations
	mutations := make([]*pb.Mutation, m.len())
	for i := range m.keys {
		mut := &pb.Mutation{
			Op:  pb.Op_PessimisticLock,
			Key: m.keys[i],
		}
		if c.txn.us.HasPresumeKeyNotExists(m.keys[i]) {
			mut.Assertion = pb.Assertion_NotExist
		}
		mutations[i] = mut
	}
	elapsed := uint64(time.Since(c.txn.startTime) / time.Millisecond)
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdPessimisticLock, &pb.PessimisticLockRequest{
		Mutations:      mutations,
		PrimaryLock:    c.primary(),
		StartVersion:   c.startTS,
		ForUFIDelateTs: c.forUFIDelateTS,
		LockTtl:        elapsed + atomic.LoadUint64(&ManagedLockTTL),
		IsFirstLock:    c.isFirstLock,
		WaitTimeout:    action.LockWaitTime,
		ReturnValues:   action.ReturnValues,
		MinCommitTs:    c.forUFIDelateTS + 1,
	}, pb.Context{Priority: c.priority, SyncLog: c.syncLog})
	lockWaitStartTime := action.WaitStartTime
	for {
		// if lockWaitTime set, refine the request `WaitTimeout` field based on timeout limit
		if action.LockWaitTime > 0 {
			timeLeft := action.LockWaitTime - (time.Since(lockWaitStartTime)).Milliseconds()
			if timeLeft <= 0 {
				req.PessimisticLock().WaitTimeout = ekv.LockNoWait
			} else {
				req.PessimisticLock().WaitTimeout = timeLeft
			}
		}
		failpoint.Inject("PessimisticLockErrWriteConflict", func() error {
			time.Sleep(300 * time.Millisecond)
			return ekv.ErrWriteConflict
		})
		startTime := time.Now()
		resp, err := c.causetstore.SendReq(bo, req, batch.region, readTimeoutShort)
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCTime, int64(time.Since(startTime)))
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCCount, 1)
		}
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
			err = c.pessimisticLockMutations(bo, action.LockCtx, batch.mutations)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		lockResp := resp.Resp.(*pb.PessimisticLockResponse)
		keyErrs := lockResp.GetErrors()
		if len(keyErrs) == 0 {
			if action.ReturnValues {
				action.ValuesLock.Lock()
				for i, mutation := range mutations {
					action.Values[string(mutation.Key)] = ekv.ReturnedValue{Value: lockResp.Values[i]}
				}
				action.ValuesLock.Unlock()
			}
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				key := alreadyExist.GetKey()
				return c.extractKeyExistsErr(key)
			}
			if deadlock := keyErr.Deadlock; deadlock != nil {
				return &ErrDeadlock{Deadlock: deadlock}
			}

			// Extract dagger from key error
			dagger, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			locks = append(locks, dagger)
		}
		// Because we already waited on einsteindb, no need to Backoff here.
		// einsteindb default will wait 3s(also the maximum wait value) when dagger error occurs
		startTime = time.Now()
		msBeforeTxnExpired, _, err := c.causetstore.lockResolver.ResolveLocks(bo, 0, locks)
		if err != nil {
			return errors.Trace(err)
		}
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.ResolveLockTime, int64(time.Since(startTime)))
		}

		// If msBeforeTxnExpired is not zero, it means there are still locks blocking us acquiring
		// the pessimistic dagger. We should return acquire fail with nowait set or timeout error if necessary.
		if msBeforeTxnExpired > 0 {
			if action.LockWaitTime == ekv.LockNoWait {
				return ErrLockAcquireFailAndNoWaitSet
			} else if action.LockWaitTime == ekv.LockAlwaysWait {
				// do nothing but keep wait
			} else {
				// the lockWaitTime is set, we should return wait timeout if we are still blocked by a dagger
				if time.Since(lockWaitStartTime).Milliseconds() >= action.LockWaitTime {
					return errors.Trace(ErrLockWaitTimeout)
				}
			}
			if action.LockCtx.PessimisticLockWaited != nil {
				atomic.StoreInt32(action.LockCtx.PessimisticLockWaited, 1)
			}
		}

		// Handle the killed flag when waiting for the pessimistic dagger.
		// When a txn runs into LockKeys() and backoff here, it has no chance to call
		// executor.Next() and check the killed flag.
		if action.Killed != nil {
			// Do not reset the killed flag here!
			// actionPessimisticLock runs on each region parallelly, we have to consider that
			// the error may be dropped.
			if atomic.LoadUint32(action.Killed) == 1 {
				return errors.Trace(ErrQueryInterrupted)
			}
		}
	}
}

func (actionPessimisticRollback) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdPessimisticRollback, &pb.PessimisticRollbackRequest{
		StartVersion:   c.startTS,
		ForUFIDelateTs: c.forUFIDelateTS,
		Keys:           batch.mutations.keys,
	})
	resp, err := c.causetstore.SendReq(bo, req, batch.region, readTimeoutShort)
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
		err = c.pessimisticRollbackMutations(bo, batch.mutations)
		return errors.Trace(err)
	}
	return nil
}

func (c *twoPhaseCommitter) pessimisticLockMutations(bo *Backoffer, lockCtx *ekv.LockCtx, mutations CommitterMutations) error {
	return c.doCausetActionOnMutations(bo, actionPessimisticLock{lockCtx}, mutations)
}

func (c *twoPhaseCommitter) pessimisticRollbackMutations(bo *Backoffer, mutations CommitterMutations) error {
	return c.doCausetActionOnMutations(bo, actionPessimisticRollback{}, mutations)
}
