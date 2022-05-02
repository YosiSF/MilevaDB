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
	"math"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	pb "github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"go.uber.org/zap"
)

type actionPrewrite struct{}

var _ twoPhaseCommitCausetAction = actionPrewrite{}
var EinsteinDBTxnRegionsNumHistogramPrewrite = metrics.EinsteinDBTxnRegionsNumHistogram.WithLabelValues(metricsTag("prewrite"))

func (actionPrewrite) String() string {
	return "prewrite"
}

func (actionPrewrite) EinsteinDBTxnRegionsNumHistogram() prometheus.Observer {
	return EinsteinDBTxnRegionsNumHistogramPrewrite
}

func (c *twoPhaseCommitter) buildPrewriteRequest(batch batchMutations, txnSize uint64) *einsteindbrpc.Request {
	m := &batch.mutations
	mutations := make([]*pb.Mutation, m.len())
	for i := range m.keys {
		mutations[i] = &pb.Mutation{
			Op:    m.ops[i],
			Key:   m.keys[i],
			Value: m.values[i],
		}
	}
	var minCommitTS uint64
	if c.forUFIDelateTS > 0 {
		minCommitTS = c.forUFIDelateTS + 1
	} else {
		minCommitTS = c.startTS + 1
	}

	failpoint.Inject("mockZeroCommitTS", func(val failpoint.Value) {
		// Should be val.(uint64) but failpoint doesn't support that.
		if tmp, ok := val.(int); ok && uint64(tmp) == c.startTS {
			minCommitTS = 0
		}
	})

	req := &pb.PrewriteRequest{
		Mutations:         mutations,
		PrimaryLock:       c.primary(),
		StartVersion:      c.startTS,
		LockTtl:           c.lockTTL,
		IsPessimisticLock: m.isPessimisticLock,
		ForUFIDelateTs:    c.forUFIDelateTS,
		TxnSize:           txnSize,
		MinCommitTs:       minCommitTS,
	}

	if c.isAsyncCommit() {
		if batch.isPrimary {
			req.Secondaries = c.asyncSecondaries()
		}
		req.UseAsyncCommit = true
		// The async commit can not be used for large transactions, and the commit ts can't be pushed.
		req.MinCommitTs = 0
	}

	return einsteindbrpc.NewRequest(einsteindbrpc.CmdPrewrite, req, pb.Context{Priority: c.priority, SyncLog: c.syncLog})
}

func (action actionPrewrite) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	txnSize := uint64(c.regionTxnSize[batch.region.id])
	// When we retry because of a region miss, we don't know the transaction size. We set the transaction size here
	// to MaxUint64 to avoid unexpected "resolve dagger lite".
	if len(bo.errors) > 0 {
		txnSize = math.MaxUint64
	}

	req := c.buildPrewriteRequest(batch, txnSize)
	for {
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
			err = c.prewriteMutations(bo, batch.mutations)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		prewriteResp := resp.Resp.(*pb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			if batch.isPrimary {
				// After writing the primary key, if the size of the transaction is large than 32M,
				// start the ttlManager. The ttlManager will be closed in einsteindbTxn.Commit().
				if int64(c.txnSize) > config.GetGlobalConfig().EinsteinDBClient.TTLRefreshedTxnSize {
					c.run(c, nil)
				}
			}
			if c.isAsyncCommit() {
				// 0 if the min_commit_ts is not ready or any other reason that async
				// commit cannot proceed. The client can then fallback to normal way to
				// continue committing the transaction if prewrite are all finished.
				if prewriteResp.MinCommitTs == 0 {
					if c.testingKnobs.noFallBack {
						return nil
					}
					logutil.Logger(bo.ctx).Warn("async commit cannot proceed since the returned minCommitTS is zero, "+
						"fallback to normal path", zap.Uint64("startTS", c.startTS))
					c.setAsyncCommit(false)
				} else {
					c.mu.Lock()
					if prewriteResp.MinCommitTs > c.minCommitTS {
						c.minCommitTS = prewriteResp.MinCommitTs
					}
					c.mu.Unlock()
				}
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

			// Extract dagger from key error
			dagger, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			logutil.BgLogger().Info("prewrite encounters dagger",
				zap.Uint64("conn", c.connID),
				zap.Stringer("dagger", dagger))
			locks = append(locks, dagger)
		}
		start := time.Now()
		msBeforeExpired, err := c.causetstore.lockResolver.resolveLocksForWrite(bo, c.startTS, locks)
		if err != nil {
			return errors.Trace(err)
		}
		atomic.AddInt64(&c.getDetail().ResolveLockTime, int64(time.Since(start)))
		if msBeforeExpired > 0 {
			err = bo.BackoffWithMaxSleep(BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (c *twoPhaseCommitter) prewriteMutations(bo *Backoffer, mutations CommitterMutations) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.prewriteMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	return c.doCausetActionOnMutations(bo, actionPrewrite{}, mutations)
}
