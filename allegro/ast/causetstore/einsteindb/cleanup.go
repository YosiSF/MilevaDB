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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	pb "github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

type actionCleanup struct{}

var _ twoPhaseCommitCausetAction = actionCleanup{}
var EinsteinDBTxnRegionsNumHistogramCleanup = metrics.EinsteinDBTxnRegionsNumHistogram.WithLabelValues(metricsTag("cleanup"))

func (actionCleanup) String() string {
	return "cleanup"
}

func (actionCleanup) EinsteinDBTxnRegionsNumHistogram() prometheus.Observer {
	return EinsteinDBTxnRegionsNumHistogramCleanup
}

func (actionCleanup) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdBatchRollback, &pb.BatchRollbackRequest{
		Keys:         batch.mutations.keys,
		StartVersion: c.startTS,
	}, pb.Context{Priority: c.priority, SyncLog: c.syncLog})
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
		err = c.cleanupMutations(bo, batch.mutations)
		return errors.Trace(err)
	}
	if keyErr := resp.Resp.(*pb.BatchRollbackResponse).GetError(); keyErr != nil {
		err = errors.Errorf("conn %d 2PC cleanup failed: %s", c.connID, keyErr)
		logutil.BgLogger().Debug("2PC failed cleanup key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}
	return nil
}

func (c *twoPhaseCommitter) cleanupMutations(bo *Backoffer, mutations CommitterMutations) error {
	return c.doCausetActionOnMutations(bo, actionCleanup{}, mutations)
}
