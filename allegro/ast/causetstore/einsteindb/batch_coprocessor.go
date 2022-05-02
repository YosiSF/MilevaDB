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
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/metapb"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

// batchINTERLOCKTask comprises of multiple INTERLOCKTask that will send to same causetstore.
type batchINTERLOCKTask struct {
	storeAddr string
	cmdType   einsteindbrpc.CmdType

	CausetTasks []INTERLOCKTaskAndRPCContext
}

type batchINTERLOCKResponse struct {
	pbResp *interlock.BatchResponse
	detail *INTERLOCKRuntimeStats

	// batch Causet Response is yet to return startKey. So batchINTERLOCK cannot retry partially.
	startKey ekv.Key
	err      error
	respSize int64
	respTime time.Duration
}

// GetData implements the ekv.ResultSubset GetData interface.
func (rs *batchINTERLOCKResponse) GetData() []byte {
	return rs.pbResp.Data
}

// GetStartKey implements the ekv.ResultSubset GetStartKey interface.
func (rs *batchINTERLOCKResponse) GetStartKey() ekv.Key {
	return rs.startKey
}

// GetExecDetails is unavailable currently, because TiFlash has not collected exec details for batch INTERLOCK.
// TODO: Will fix in near future.
func (rs *batchINTERLOCKResponse) GetINTERLOCKRuntimeStats() *INTERLOCKRuntimeStats {
	return rs.detail
}

// MemSize returns how many bytes of memory this response use
func (rs *batchINTERLOCKResponse) MemSize() int64 {
	if rs.respSize != 0 {
		return rs.respSize
	}

	// ignore rs.err
	rs.respSize += int64(cap(rs.startKey))
	if rs.detail != nil {
		rs.respSize += int64(sizeofExecDetails)
	}
	if rs.pbResp != nil {
		// Using a approximate size since it's hard to get a accurate value.
		rs.respSize += int64(rs.pbResp.Size())
	}
	return rs.respSize
}

func (rs *batchINTERLOCKResponse) RespTime() time.Duration {
	return rs.respTime
}

type INTERLOCKTaskAndRPCContext struct {
	task *INTERLOCKTask
	ctx  *RPCContext
}

func buildBatchCausetTasks(bo *Backoffer, cache *RegionCache, ranges *INTERLOCKRanges, req *ekv.Request) ([]*batchINTERLOCKTask, error) {
	start := time.Now()
	const cmdType = einsteindbrpc.CmdBatchINTERLOCK
	rangesLen := ranges.len()
	for {
		var tasks []*INTERLOCKTask
		appendTask := func(regionWithRangeInfo *KeyLocation, ranges *INTERLOCKRanges) {
			tasks = append(tasks, &INTERLOCKTask{
				region:    regionWithRangeInfo.Region,
				ranges:    ranges,
				cmdType:   cmdType,
				storeType: req.StoreType,
			})
		}

		err := splitRanges(bo, cache, ranges, appendTask)
		if err != nil {
			return nil, errors.Trace(err)
		}

		var batchTasks []*batchINTERLOCKTask

		storeTaskMap := make(map[string]*batchINTERLOCKTask)
		needRetry := false
		for _, task := range tasks {
			rpcCtx, err := cache.GetTiFlashRPCContext(bo, task.region)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We should retry and generate new tasks.
			if rpcCtx == nil {
				needRetry = true
				err = bo.Backoff(BoRegionMiss, errors.New("Cannot find region or TiFlash peer"))
				logutil.BgLogger().Info("retry for TiFlash peer or region missing", zap.Uint64("region id", task.region.GetID()))
				if err != nil {
					return nil, errors.Trace(err)
				}
				break
			}
			if batchINTERLOCK, ok := storeTaskMap[rpcCtx.Addr]; ok {
				batchINTERLOCK.CausetTasks = append(batchINTERLOCK.CausetTasks, INTERLOCKTaskAndRPCContext{task: task, ctx: rpcCtx})
			} else {
				batchTask := &batchINTERLOCKTask{
					storeAddr:   rpcCtx.Addr,
					cmdType:     cmdType,
					CausetTasks: []INTERLOCKTaskAndRPCContext{{task, rpcCtx}},
				}
				storeTaskMap[rpcCtx.Addr] = batchTask
			}
		}
		if needRetry {
			continue
		}
		for _, task := range storeTaskMap {
			batchTasks = append(batchTasks, task)
		}

		if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
			logutil.BgLogger().Warn("buildBatchCausetTasks takes too much time",
				zap.Duration("elapsed", elapsed),
				zap.Int("range len", rangesLen),
				zap.Int("task len", len(batchTasks)))
		}
		einsteindbTxnRegionsNumHistogramWithBatchinterlocking_directorate.Observe(float64(len(batchTasks)))
		return batchTasks, nil
	}
}

func (c *INTERLOCKClient) sendBatch(ctx context.Context, req *ekv.Request, vars *ekv.Variables) ekv.Response {
	if req.KeepOrder || req.Desc {
		return INTERLOCKErrorResponse{errors.New("batch interlock cannot prove keep order or desc property")}
	}
	ctx = context.WithValue(ctx, txnStartKey, req.StartTs)
	bo := NewBackofferWithVars(ctx, INTERLOCKBuildTaskMaxBackoff, vars)
	tasks, err := buildBatchCausetTasks(bo, c.causetstore.regionCache, &INTERLOCKRanges{mid: req.KeyRanges}, req)
	if err != nil {
		return INTERLOCKErrorResponse{err}
	}
	it := &batchINTERLOCKIterator{
		causetstore: c.causetstore,
		req:         req,
		finishCh:    make(chan struct{}),
		vars:        vars,
		memTracker:  req.MemTracker,
		clientHelper: clientHelper{
			LockResolver:      c.causetstore.lockResolver,
			RegionCache:       c.causetstore.regionCache,
			Client:            c.causetstore.client,
			minCommitTSPushed: &minCommitTSPushed{data: make(map[uint64]struct{}, 5)},
		},
		rpcCancel: NewRPCanceller(),
	}
	ctx = context.WithValue(ctx, RPCCancellerCtxKey{}, it.rpcCancel)
	it.tasks = tasks
	it.respChan = make(chan *batchINTERLOCKResponse, 2048)
	go it.run(ctx)
	return it
}

type batchINTERLOCKIterator struct {
	clientHelper

	causetstore *einsteindbStore
	req         *ekv.Request
	finishCh    chan struct{}

	tasks []*batchINTERLOCKTask

	// Batch results are stored in respChan.
	respChan chan *batchINTERLOCKResponse

	vars *ekv.Variables

	memTracker *memory.Tracker

	replicaReadSeed uint32

	rpcCancel *RPCCanceller

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32
}

func (b *batchINTERLOCKIterator) run(ctx context.Context) {
	// We run workers for every batch INTERLOCK.
	for _, task := range b.tasks {
		b.wg.Add(1)
		bo := NewBackofferWithVars(ctx, INTERLOCKNextMaxBackoff, b.vars)
		go b.handleTask(ctx, bo, task)
	}
	b.wg.Wait()
	close(b.respChan)
}

// Next returns next interlock result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (b *batchINTERLOCKIterator) Next(ctx context.Context) (ekv.ResultSubset, error) {
	var (
		resp   *batchINTERLOCKResponse
		ok     bool
		closed bool
	)

	// Get next fetched resp from chan
	resp, ok, closed = b.recvFromRespCh(ctx)
	if !ok || closed {
		return nil, nil
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := b.causetstore.CheckVisibility(b.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (b *batchINTERLOCKIterator) recvFromRespCh(ctx context.Context) (resp *batchINTERLOCKResponse, ok bool, exit bool) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-b.respChan:
			return
		case <-ticker.C:
			if atomic.LoadUint32(b.vars.Killed) == 1 {
				resp = &batchINTERLOCKResponse{err: ErrQueryInterrupted}
				ok = true
				return
			}
		case <-b.finishCh:
			exit = true
			return
		case <-ctx.Done():
			// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
			if atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
				close(b.finishCh)
			}
			exit = true
			return
		}
	}
}

// Close releases the resource.
func (b *batchINTERLOCKIterator) Close() error {
	if atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		close(b.finishCh)
	}
	b.rpcCancel.CancelAll()
	b.wg.Wait()
	return nil
}

func (b *batchINTERLOCKIterator) handleTask(ctx context.Context, bo *Backoffer, task *batchINTERLOCKTask) {
	logutil.BgLogger().Debug("handle batch task")
	tasks := []*batchINTERLOCKTask{task}
	for idx := 0; idx < len(tasks); idx++ {
		ret, err := b.handleTaskOnce(ctx, bo, tasks[idx])
		if err != nil {
			resp := &batchINTERLOCKResponse{err: errors.Trace(err), detail: new(INTERLOCKRuntimeStats)}
			b.sendToRespCh(resp)
			break
		}
		tasks = append(tasks, ret...)
	}
	b.wg.Done()
}

// Merge all ranges and request again.
func (b *batchINTERLOCKIterator) retryBatchINTERLOCKTask(ctx context.Context, bo *Backoffer, batchTask *batchINTERLOCKTask) ([]*batchINTERLOCKTask, error) {
	ranges := &INTERLOCKRanges{}
	for _, taskCtx := range batchTask.CausetTasks {
		taskCtx.task.ranges.do(func(ran *ekv.KeyRange) {
			ranges.mid = append(ranges.mid, *ran)
		})
	}
	return buildBatchCausetTasks(bo, b.RegionCache, ranges, b.req)
}

func (b *batchINTERLOCKIterator) handleTaskOnce(ctx context.Context, bo *Backoffer, task *batchINTERLOCKTask) ([]*batchINTERLOCKTask, error) {
	logutil.BgLogger().Debug("handle batch task once")
	sender := NewRegionBatchRequestSender(b.causetstore.regionCache, b.causetstore.client)
	var regionInfos []*interlock.RegionInfo
	for _, task := range task.CausetTasks {
		regionInfos = append(regionInfos, &interlock.RegionInfo{
			RegionId: task.task.region.id,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: task.task.region.confVer,
				Version: task.task.region.ver,
			},
			Ranges: task.task.ranges.toPBRanges(),
		})
	}

	INTERLOCKReq := interlock.BatchRequest{
		Tp:        b.req.Tp,
		StartTs:   b.req.StartTs,
		Data:      b.req.Data,
		SchemaVer: b.req.SchemaVar,
		Regions:   regionInfos,
	}

	req := einsteindbrpc.NewRequest(task.cmdType, &INTERLOCKReq, kvrpcpb.Context{
		IsolationLevel: pbIsolationLevel(b.req.IsolationLevel),
		Priority:       kvPriorityToCommandPri(b.req.Priority),
		NotFillCache:   b.req.NotFillCache,
		HandleTime:     true,
		ScanDetail:     true,
		TaskId:         b.req.TaskID,
	})
	req.StoreTp = ekv.TiFlash

	logutil.BgLogger().Debug("send batch request to ", zap.String("req info", req.String()), zap.Int("INTERLOCK task len", len(task.CausetTasks)))
	resp, retry, cancel, err := sender.sendStreamReqToAddr(bo, task.CausetTasks, req, ReadTimeoutUltraLong)
	// If there are causetstore errors, we should retry for all regions.
	if retry {
		return b.retryBatchINTERLOCKTask(ctx, bo, task)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer cancel()
	return nil, b.handleStreamedBatchINTERLOCKResponse(ctx, bo, resp.Resp.(*einsteindbrpc.BatchINTERLOCKStreamResponse), task)
}

func (b *batchINTERLOCKIterator) handleStreamedBatchINTERLOCKResponse(ctx context.Context, bo *Backoffer, response *einsteindbrpc.BatchINTERLOCKStreamResponse, task *batchINTERLOCKTask) (err error) {
	defer response.Close()
	resp := response.BatchResponse
	if resp == nil {
		// streaming request returns io.EOF, so the first Response is nil.
		return
	}
	for {
		err = b.handleBatchINTERLOCKResponse(bo, resp, task)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err = response.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}

			if err1 := bo.Backoff(boEinsteinDBRPC, errors.Errorf("recv stream response error: %v, task causetstore addr: %s", err, task.storeAddr)); err1 != nil {
				return errors.Trace(err)
			}

			// No interlock.Response for network error, rebuild task based on the last success one.
			if errors.Cause(err) == context.Canceled {
				logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
			} else {
				logutil.BgLogger().Info("stream unknown error", zap.Error(err))
			}
			return errors.Trace(err)
		}
	}
}

func (b *batchINTERLOCKIterator) handleBatchINTERLOCKResponse(bo *Backoffer, response *interlock.BatchResponse, task *batchINTERLOCKTask) (err error) {
	if otherErr := response.GetOtherError(); otherErr != "" {
		err = errors.Errorf("other error: %s", otherErr)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", b.req.StartTs),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		return errors.Trace(err)
	}

	resp := batchINTERLOCKResponse{
		pbResp: response,
		detail: new(INTERLOCKRuntimeStats),
	}

	resp.detail.BackoffTime = time.Duration(bo.totalSleep) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(bo.backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(bo.backoffTimes))
	for backoff := range bo.backoffTimes {
		backoffName := backoff.String()
		resp.detail.BackoffTimes[backoffName] = bo.backoffTimes[backoff]
		resp.detail.BackoffSleep[backoffName] = time.Duration(bo.backoffSleepMS[backoff]) * time.Millisecond
	}
	resp.detail.CalleeAddress = task.storeAddr

	b.sendToRespCh(&resp)

	return
}

func (b *batchINTERLOCKIterator) sendToRespCh(resp *batchINTERLOCKResponse) (exit bool) {
	select {
	case b.respChan <- resp:
	case <-b.finishCh:
		exit = true
	}
	return
}
