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
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/gogo/protobuf/proto"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"go.uber.org/zap"
)

var einsteindbTxnRegionsNumHistogramWithinterlocking_directorate = metrics.EinsteinDBTxnRegionsNumHistogram.WithLabelValues("interlock")
var einsteindbTxnRegionsNumHistogramWithBatchinterlocking_directorate = metrics.EinsteinDBTxnRegionsNumHistogram.WithLabelValues("batch_interlocking_directorate")

// INTERLOCKClient is interlock client.
type INTERLOCKClient struct {
	ekv.RequestTypeSupportedChecker
	causetstore     *einsteindbStore
	replicaReadSeed uint32
}

// Send builds the request and gets the interlock iterator response.
func (c *INTERLOCKClient) Send(ctx context.Context, req *ekv.Request, vars *ekv.Variables) ekv.Response {
	if req.StoreType == ekv.TiFlash && req.BatchINTERLOCK {
		logutil.BgLogger().Debug("send batch requests")
		return c.sendBatch(ctx, req, vars)
	}
	ctx = context.WithValue(ctx, txnStartKey, req.StartTs)
	bo := NewBackofferWithVars(ctx, INTERLOCKBuildTaskMaxBackoff, vars)
	tasks, err := buildCausetTasks(bo, c.causetstore.regionCache, &INTERLOCKRanges{mid: req.KeyRanges}, req)
	if err != nil {
		return INTERLOCKErrorResponse{err}
	}
	it := &INTERLOCKIterator{
		causetstore:     c.causetstore,
		req:             req,
		concurrency:     req.Concurrency,
		finishCh:        make(chan struct{}),
		vars:            vars,
		memTracker:      req.MemTracker,
		replicaReadSeed: c.replicaReadSeed,
		rpcCancel:       NewRPCanceller(),
	}
	it.minCommitTSPushed.data = make(map[uint64]struct{}, 5)
	it.tasks = tasks
	if it.concurrency > len(tasks) {
		it.concurrency = len(tasks)
	}
	if it.concurrency < 1 {
		// Make sure that there is at least one worker.
		it.concurrency = 1
	}

	if it.req.KeepOrder {
		it.sendRate = newRateLimit(2 * it.concurrency)
	} else {
		it.respChan = make(chan *INTERLOCKResponse, it.concurrency)
		it.sendRate = newRateLimit(it.concurrency)
	}

	if !it.req.Streaming {
		ctx = context.WithValue(ctx, RPCCancellerCtxKey{}, it.rpcCancel)
	}
	it.open(ctx)
	return it
}

// INTERLOCKTask contains a related Region and KeyRange for a ekv.Request.
type INTERLOCKTask struct {
	region RegionVerID
	ranges *INTERLOCKRanges

	respChan  chan *INTERLOCKResponse
	storeAddr string
	cmdType   einsteindbrpc.CmdType
	storeType ekv.StoreType
}

func (r *INTERLOCKTask) String() string {
	return fmt.Sprintf("region(%d %d %d) ranges(%d) causetstore(%s)",
		r.region.id, r.region.confVer, r.region.ver, r.ranges.len(), r.storeAddr)
}

// INTERLOCKRanges is like []ekv.KeyRange, but may has extra elements at head/tail.
// It's for avoiding alloc big slice during build INTERLOCKTask.
type INTERLOCKRanges struct {
	first *ekv.KeyRange
	mid   []ekv.KeyRange
	last  *ekv.KeyRange
}

func (r *INTERLOCKRanges) String() string {
	var s string
	r.do(func(ran *ekv.KeyRange) {
		s += fmt.Sprintf("[%q, %q]", ran.StartKey, ran.EndKey)
	})
	return s
}

func (r *INTERLOCKRanges) len() int {
	var l int
	if r.first != nil {
		l++
	}
	l += len(r.mid)
	if r.last != nil {
		l++
	}
	return l
}

func (r *INTERLOCKRanges) at(i int) ekv.KeyRange {
	if r.first != nil {
		if i == 0 {
			return *r.first
		}
		i--
	}
	if i < len(r.mid) {
		return r.mid[i]
	}
	return *r.last
}

func (r *INTERLOCKRanges) slice(from, to int) *INTERLOCKRanges {
	var ran INTERLOCKRanges
	if r.first != nil {
		if from == 0 && to > 0 {
			ran.first = r.first
		}
		if from > 0 {
			from--
		}
		if to > 0 {
			to--
		}
	}
	if to <= len(r.mid) {
		ran.mid = r.mid[from:to]
	} else {
		if from <= len(r.mid) {
			ran.mid = r.mid[from:]
		}
		if from < to {
			ran.last = r.last
		}
	}
	return &ran
}

func (r *INTERLOCKRanges) do(f func(ran *ekv.KeyRange)) {
	if r.first != nil {
		f(r.first)
	}
	for _, ran := range r.mid {
		f(&ran)
	}
	if r.last != nil {
		f(r.last)
	}
}

func (r *INTERLOCKRanges) toPBRanges() []*interlock.KeyRange {
	ranges := make([]*interlock.KeyRange, 0, r.len())
	r.do(func(ran *ekv.KeyRange) {
		ranges = append(ranges, &interlock.KeyRange{
			Start: ran.StartKey,
			End:   ran.EndKey,
		})
	})
	return ranges
}

// split ranges into (left, right) by key.
func (r *INTERLOCKRanges) split(key []byte) (*INTERLOCKRanges, *INTERLOCKRanges) {
	n := sort.Search(r.len(), func(i int) bool {
		cur := r.at(i)
		return len(cur.EndKey) == 0 || bytes.Compare(cur.EndKey, key) > 0
	})
	// If a range p contains the key, it will split to 2 parts.
	if n < r.len() {
		p := r.at(n)
		if bytes.Compare(key, p.StartKey) > 0 {
			left := r.slice(0, n)
			left.last = &ekv.KeyRange{StartKey: p.StartKey, EndKey: key}
			right := r.slice(n+1, r.len())
			right.first = &ekv.KeyRange{StartKey: key, EndKey: p.EndKey}
			return left, right
		}
	}
	return r.slice(0, n), r.slice(n, r.len())
}

// rangesPerTask limits the length of the ranges slice sent in one INTERLOCKTask.
const rangesPerTask = 25000

func buildCausetTasks(bo *Backoffer, cache *RegionCache, ranges *INTERLOCKRanges, req *ekv.Request) ([]*INTERLOCKTask, error) {
	start := time.Now()
	cmdType := einsteindbrpc.CmdINTERLOCK
	if req.Streaming {
		cmdType = einsteindbrpc.CmdINTERLOCKStream
	}

	if req.StoreType == ekv.MilevaDB {
		return buildMilevaDBMemCausetTasks(ranges, req)
	}

	rangesLen := ranges.len()
	var tasks []*INTERLOCKTask
	appendTask := func(regionWithRangeInfo *KeyLocation, ranges *INTERLOCKRanges) {
		// EinsteinDB will return gRPC error if the message is too large. So we need to limit the length of the ranges slice
		// to make sure the message can be sent successfully.
		rLen := ranges.len()
		for i := 0; i < rLen; {
			nextI := mathutil.Min(i+rangesPerTask, rLen)
			tasks = append(tasks, &INTERLOCKTask{
				region: regionWithRangeInfo.Region,
				ranges: ranges.slice(i, nextI),
				// Channel buffer is 2 for handling region split.
				// In a common case, two region split tasks will not be blocked.
				respChan:  make(chan *INTERLOCKResponse, 2),
				cmdType:   cmdType,
				storeType: req.StoreType,
			})
			i = nextI
		}
	}

	err := splitRanges(bo, cache, ranges, appendTask)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if req.Desc {
		reverseTasks(tasks)
	}
	if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
		logutil.BgLogger().Warn("buildCausetTasks takes too much time",
			zap.Duration("elapsed", elapsed),
			zap.Int("range len", rangesLen),
			zap.Int("task len", len(tasks)))
	}
	einsteindbTxnRegionsNumHistogramWithinterlocking_directorate.Observe(float64(len(tasks)))
	return tasks, nil
}

func buildMilevaDBMemCausetTasks(ranges *INTERLOCKRanges, req *ekv.Request) ([]*INTERLOCKTask, error) {
	servers, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return nil, err
	}
	cmdType := einsteindbrpc.CmdINTERLOCK
	if req.Streaming {
		cmdType = einsteindbrpc.CmdINTERLOCKStream
	}
	tasks := make([]*INTERLOCKTask, 0, len(servers))
	for _, ser := range servers {
		addr := ser.IP + ":" + strconv.FormatUint(uint64(ser.StatusPort), 10)
		tasks = append(tasks, &INTERLOCKTask{
			ranges:    ranges,
			respChan:  make(chan *INTERLOCKResponse, 2),
			cmdType:   cmdType,
			storeType: req.StoreType,
			storeAddr: addr,
		})
	}
	return tasks, nil
}

func splitRanges(bo *Backoffer, cache *RegionCache, ranges *INTERLOCKRanges, fn func(regionWithRangeInfo *KeyLocation, ranges *INTERLOCKRanges)) error {
	for ranges.len() > 0 {
		loc, err := cache.LocateKey(bo, ranges.at(0).StartKey)
		if err != nil {
			return errors.Trace(err)
		}

		// Iterate to the first range that is not complete in the region.
		var i int
		for ; i < ranges.len(); i++ {
			r := ranges.at(i)
			if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
				break
			}
		}
		// All rest ranges belong to the same region.
		if i == ranges.len() {
			fn(loc, ranges)
			break
		}

		r := ranges.at(i)
		if loc.Contains(r.StartKey) {
			// Part of r is not in the region. We need to split it.
			taskRanges := ranges.slice(0, i)
			taskRanges.last = &ekv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   loc.EndKey,
			}
			fn(loc, taskRanges)

			ranges = ranges.slice(i+1, ranges.len())
			ranges.first = &ekv.KeyRange{
				StartKey: loc.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// rs[i] is not in the region.
			taskRanges := ranges.slice(0, i)
			fn(loc, taskRanges)
			ranges = ranges.slice(i, ranges.len())
		}
	}

	return nil
}

// SplitRegionRanges get the split ranges from fidel region.
func SplitRegionRanges(bo *Backoffer, cache *RegionCache, keyRanges []ekv.KeyRange) ([]ekv.KeyRange, error) {
	ranges := INTERLOCKRanges{mid: keyRanges}

	var ret []ekv.KeyRange
	appendRange := func(regionWithRangeInfo *KeyLocation, ranges *INTERLOCKRanges) {
		for i := 0; i < ranges.len(); i++ {
			ret = append(ret, ranges.at(i))
		}
	}

	err := splitRanges(bo, cache, &ranges, appendRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func reverseTasks(tasks []*INTERLOCKTask) {
	for i := 0; i < len(tasks)/2; i++ {
		j := len(tasks) - i - 1
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}

type INTERLOCKIterator struct {
	causetstore *einsteindbStore
	req         *ekv.Request
	concurrency int
	finishCh    chan struct{}

	// If keepOrder, results are stored in INTERLOCKTask.respChan, read them out one by one.
	tasks []*INTERLOCKTask
	curr  int

	// sendRate controls the sending rate of INTERLOCKIteratorTaskSender
	sendRate *rateLimit

	// Otherwise, results are stored in respChan.
	respChan chan *INTERLOCKResponse

	vars *ekv.Variables

	memTracker *memory.Tracker

	replicaReadSeed uint32

	rpcCancel *RPCCanceller

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32

	minCommitTSPushed
}

// INTERLOCKIteratorWorker receives tasks from INTERLOCKIteratorTaskSender, handles tasks and sends the INTERLOCKResponse to respChan.
type INTERLOCKIteratorWorker struct {
	taskCh      <-chan *INTERLOCKTask
	wg          *sync.WaitGroup
	causetstore *einsteindbStore
	req         *ekv.Request
	respChan    chan<- *INTERLOCKResponse
	finishCh    <-chan struct{}
	vars        *ekv.Variables
	clientHelper

	memTracker *memory.Tracker

	replicaReadSeed uint32

	sendRate *rateLimit
}

// INTERLOCKIteratorTaskSender sends tasks to taskCh then wait for the workers to exit.
type INTERLOCKIteratorTaskSender struct {
	taskCh   chan<- *INTERLOCKTask
	wg       *sync.WaitGroup
	tasks    []*INTERLOCKTask
	finishCh <-chan struct{}
	respChan chan<- *INTERLOCKResponse
	sendRate *rateLimit
}

type INTERLOCKResponse struct {
	pbResp   *interlock.Response
	detail   *INTERLOCKRuntimeStats
	startKey ekv.Key
	err      error
	respSize int64
	respTime time.Duration
}

const (
	sizeofExecDetails   = int(unsafe.Sizeof(execdetails.ExecDetails{}))
	sizeofCommitDetails = int(unsafe.Sizeof(execdetails.CommitDetails{}))
)

// GetData implements the ekv.ResultSubset GetData interface.
func (rs *INTERLOCKResponse) GetData() []byte {
	return rs.pbResp.Data
}

// GetStartKey implements the ekv.ResultSubset GetStartKey interface.
func (rs *INTERLOCKResponse) GetStartKey() ekv.Key {
	return rs.startKey
}

func (rs *INTERLOCKResponse) GetINTERLOCKRuntimeStats() *INTERLOCKRuntimeStats {
	return rs.detail
}

// MemSize returns how many bytes of memory this response use
func (rs *INTERLOCKResponse) MemSize() int64 {
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

func (rs *INTERLOCKResponse) RespTime() time.Duration {
	return rs.respTime
}

const minLogINTERLOCKTaskTime = 300 * time.Millisecond

// run is a worker function that get a INTERLOCKTask from channel, handle it and
// send the result back.
func (worker *INTERLOCKIteratorWorker) run(ctx context.Context) {
	defer worker.wg.Done()
	for task := range worker.taskCh {
		respCh := worker.respChan
		if respCh == nil {
			respCh = task.respChan
		}

		worker.handleTask(ctx, task, respCh)
		close(task.respChan)
		if worker.respChan != nil {
			worker.sendRate.putToken()
		}
		if worker.vars != nil && worker.vars.Killed != nil && atomic.LoadUint32(worker.vars.Killed) == 1 {
			return
		}
		select {
		case <-worker.finishCh:
			return
		default:
		}
	}
}

// open starts workers and sender goroutines.
func (it *INTERLOCKIterator) open(ctx context.Context) {
	taskCh := make(chan *INTERLOCKTask, 1)
	it.wg.Add(it.concurrency)
	// Start it.concurrency number of workers to handle INTERLOCK requests.
	for i := 0; i < it.concurrency; i++ {
		worker := &INTERLOCKIteratorWorker{
			taskCh:      taskCh,
			wg:          &it.wg,
			causetstore: it.causetstore,
			req:         it.req,
			respChan:    it.respChan,
			finishCh:    it.finishCh,
			vars:        it.vars,
			clientHelper: clientHelper{
				LockResolver:      it.causetstore.lockResolver,
				RegionCache:       it.causetstore.regionCache,
				minCommitTSPushed: &it.minCommitTSPushed,
				Client:            it.causetstore.client,
			},

			memTracker: it.memTracker,

			replicaReadSeed: it.replicaReadSeed,
			sendRate:        it.sendRate,
		}
		go worker.run(ctx)
	}
	taskSender := &INTERLOCKIteratorTaskSender{
		taskCh:   taskCh,
		wg:       &it.wg,
		tasks:    it.tasks,
		finishCh: it.finishCh,
		sendRate: it.sendRate,
	}
	taskSender.respChan = it.respChan
	go taskSender.run()
}

func (sender *INTERLOCKIteratorTaskSender) run() {
	// Send tasks to feed the worker goroutines.
	for _, t := range sender.tasks {
		// we control the sending rate to prevent all tasks
		// being done (aka. all of the responses are buffered) by INTERLOCKIteratorWorker.
		// We keep the number of inflight tasks within the number of 2 * concurrency when Keep Order is true.
		// If KeepOrder is false, the number equals the concurrency.
		// It sends one more task if a task has been finished in INTERLOCKIterator.Next.
		exit := sender.sendRate.getToken(sender.finishCh)
		if exit {
			break
		}
		exit = sender.sendToTaskCh(t)
		if exit {
			break
		}
	}
	close(sender.taskCh)

	// Wait for worker goroutines to exit.
	sender.wg.Wait()
	if sender.respChan != nil {
		close(sender.respChan)
	}
}

func (it *INTERLOCKIterator) recvFromRespCh(ctx context.Context, respCh <-chan *INTERLOCKResponse) (resp *INTERLOCKResponse, ok bool, exit bool) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-respCh:
			if it.memTracker != nil && resp != nil {
				it.memTracker.Consume(-resp.MemSize())
			}
			return
		case <-it.finishCh:
			exit = true
			return
		case <-ticker.C:
			if atomic.LoadUint32(it.vars.Killed) == 1 {
				resp = &INTERLOCKResponse{err: ErrQueryInterrupted}
				ok = true
				return
			}
		case <-ctx.Done():
			// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
			if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
				close(it.finishCh)
			}
			exit = true
			return
		}
	}
}

func (sender *INTERLOCKIteratorTaskSender) sendToTaskCh(t *INTERLOCKTask) (exit bool) {
	select {
	case sender.taskCh <- t:
	case <-sender.finishCh:
		exit = true
	}
	return
}

func (worker *INTERLOCKIteratorWorker) sendToRespCh(resp *INTERLOCKResponse, respCh chan<- *INTERLOCKResponse, checkOOM bool) (exit bool) {
	if worker.memTracker != nil && checkOOM {
		worker.memTracker.Consume(resp.MemSize())
	}
	select {
	case respCh <- resp:
	case <-worker.finishCh:
		exit = true
	}
	return
}

// Next returns next interlock result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (it *INTERLOCKIterator) Next(ctx context.Context) (ekv.ResultSubset, error) {
	var (
		resp   *INTERLOCKResponse
		ok     bool
		closed bool
	)
	// If data order matters, response should be returned in the same order as INTERLOCKTask slice.
	// Otherwise all responses are returned from a single channel.
	if it.respChan != nil {
		// Get next fetched resp from chan
		resp, ok, closed = it.recvFromRespCh(ctx, it.respChan)
		if !ok || closed {
			return nil, nil
		}
	} else {
		for {
			if it.curr >= len(it.tasks) {
				// Resp will be nil if iterator is finishCh.
				return nil, nil
			}
			task := it.tasks[it.curr]
			resp, ok, closed = it.recvFromRespCh(ctx, task.respChan)
			if closed {
				// Close() is already called, so Next() is invalid.
				return nil, nil
			}
			if ok {
				break
			}
			// Switch to next task.
			it.tasks[it.curr] = nil
			it.curr++
			it.sendRate.putToken()
		}
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := it.causetstore.CheckVisibility(it.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

// Associate each region with an independent backoffer. In this way, when multiple regions are
// unavailable, MilevaDB can execute very quickly without blocking
func chooseBackoffer(ctx context.Context, backoffermap map[uint64]*Backoffer, task *INTERLOCKTask, worker *INTERLOCKIteratorWorker) *Backoffer {
	bo, ok := backoffermap[task.region.id]
	if ok {
		return bo
	}
	newbo := NewBackofferWithVars(ctx, INTERLOCKNextMaxBackoff, worker.vars)
	backoffermap[task.region.id] = newbo
	return newbo
}

// handleTask handles single INTERLOCKTask, sends the result to channel, retry automatically on error.
func (worker *INTERLOCKIteratorWorker) handleTask(ctx context.Context, task *INTERLOCKTask, respCh chan<- *INTERLOCKResponse) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Error("INTERLOCKIteratorWork meet panic",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
			resp := &INTERLOCKResponse{err: errors.Errorf("%v", r)}
			// if panic has happened, set checkOOM to false to avoid another panic.
			worker.sendToRespCh(resp, respCh, false)
		}
	}()
	remainTasks := []*INTERLOCKTask{task}
	backoffermap := make(map[uint64]*Backoffer)
	for len(remainTasks) > 0 {
		curTask := remainTasks[0]
		bo := chooseBackoffer(ctx, backoffermap, curTask, worker)
		tasks, err := worker.handleTaskOnce(bo, curTask, respCh)
		if err != nil {
			resp := &INTERLOCKResponse{err: errors.Trace(err)}
			worker.sendToRespCh(resp, respCh, true)
			return
		}
		// test whether the ctx is cancelled
		if bo.vars != nil && bo.vars.Killed != nil && atomic.LoadUint32(bo.vars.Killed) == 1 {
			return
		}

		if len(tasks) > 0 {
			remainTasks = append(tasks, remainTasks[1:]...)
		} else {
			remainTasks = remainTasks[1:]
		}
	}
}

// handleTaskOnce handles single INTERLOCKTask, successful results are send to channel.
// If error happened, returns error. If region split or meet dagger, returns the remain tasks.
func (worker *INTERLOCKIteratorWorker) handleTaskOnce(bo *Backoffer, task *INTERLOCKTask, ch chan<- *INTERLOCKResponse) ([]*INTERLOCKTask, error) {
	failpoint.Inject("handleTaskOnceError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mock handleTaskOnce error"))
		}
	})

	INTERLOCKReq := interlock.Request{
		Tp:        worker.req.Tp,
		StartTs:   worker.req.StartTs,
		Data:      worker.req.Data,
		Ranges:    task.ranges.toPBRanges(),
		SchemaVer: worker.req.SchemaVar,
	}

	var cacheKey []byte = nil
	var cacheValue *INTERLOCKrCacheValue = nil

	// If there are many ranges, it is very likely to be a TableLookupRequest. They are not worth to cache since
	// computing is not the main cost. Ignore such requests directly to avoid slowly building the cache key.
	if task.cmdType == einsteindbrpc.CmdINTERLOCK && worker.causetstore.INTERLOCKrCache != nil && worker.req.Cacheable && len(INTERLOCKReq.Ranges) < 10 {
		cKey, err := INTERLOCKrCacheBuildKey(&INTERLOCKReq)
		if err == nil {
			cacheKey = cKey
			cValue := worker.causetstore.INTERLOCKrCache.Get(cKey)
			INTERLOCKReq.IsCacheEnabled = true
			if cValue != nil && cValue.RegionID == task.region.id && cValue.TimeStamp <= worker.req.StartTs {
				// Append cache version to the request to skip interlocking_directorate computation if possible
				// when request result is cached
				INTERLOCKReq.CacheIfMatchVersion = cValue.RegionDataVersion
				cacheValue = cValue
			} else {
				INTERLOCKReq.CacheIfMatchVersion = 0
			}
		} else {
			logutil.BgLogger().Warn("Failed to build INTERLOCKr cache key", zap.Error(err))
		}
	}

	req := einsteindbrpc.NewReplicaReadRequest(task.cmdType, &INTERLOCKReq, worker.req.ReplicaRead, &worker.replicaReadSeed, kvrpcpb.Context{
		IsolationLevel: pbIsolationLevel(worker.req.IsolationLevel),
		Priority:       kvPriorityToCommandPri(worker.req.Priority),
		NotFillCache:   worker.req.NotFillCache,
		HandleTime:     true,
		ScanDetail:     true,
		TaskId:         worker.req.TaskID,
	})
	req.StoreTp = task.storeType
	startTime := time.Now()
	if worker.Stats == nil {
		worker.Stats = make(map[einsteindbrpc.CmdType]*RPCRuntimeStats)
	}
	resp, rpcCtx, storeAddr, err := worker.SendReqCtx(bo, req, task.region, ReadTimeoutMedium, task.storeType, task.storeAddr)
	if err != nil {
		if task.storeType == ekv.MilevaDB {
			err = worker.handleMilevaDBSendReqErr(err, task, ch)
			return nil, err
		}
		return nil, errors.Trace(err)
	}

	// Set task.storeAddr field so its task.String() method have the causetstore address information.
	task.storeAddr = storeAddr
	costTime := time.Since(startTime)
	if costTime > minLogINTERLOCKTaskTime {
		worker.logTimeINTERLOCKTask(costTime, task, bo, resp)
	}
	metrics.EinsteinDBinterlocking_directorateHistogram.Observe(costTime.Seconds())

	if task.cmdType == einsteindbrpc.CmdINTERLOCKStream {
		return worker.handleINTERLOCKStreamResult(bo, rpcCtx, resp.Resp.(*einsteindbrpc.INTERLOCKStreamResponse), task, ch, costTime)
	}

	// Handles the response for non-streaming INTERLOCKTask.
	return worker.handleINTERLOCKResponse(bo, rpcCtx, &INTERLOCKResponse{pbResp: resp.Resp.(*interlock.Response)}, cacheKey, cacheValue, task, ch, nil, costTime)
}

type minCommitTSPushed struct {
	data map[uint64]struct{}
	sync.RWMutex
}

func (m *minCommitTSPushed) UFIDelate(from []uint64) {
	m.Lock()
	for _, v := range from {
		m.data[v] = struct{}{}
	}
	m.Unlock()
}

func (m *minCommitTSPushed) Get() []uint64 {
	m.RLock()
	defer m.RUnlock()
	if len(m.data) == 0 {
		return nil
	}

	ret := make([]uint64, 0, len(m.data))
	for k := range m.data {
		ret = append(ret, k)
	}
	return ret
}

// clientHelper wraps LockResolver and RegionRequestSender.
// It's introduced to support the new dagger resolving pattern in the large transaction.
// In the large transaction protocol, sending requests and resolving locks are
// context-dependent. For example, when a send request meets a secondary dagger, we'll
// call ResolveLock, and if the dagger belongs to a large transaction, we may retry
// the request. If there is no context information about the resolved locks, we'll
// meet the secondary dagger again and run into a deadloop.
type clientHelper struct {
	*LockResolver
	*RegionCache
	*minCommitTSPushed
	Client
	resolveLite bool
	RegionRequestRuntimeStats
}

// ResolveLocks wraps the ResolveLocks function and causetstore the resolved result.
func (ch *clientHelper) ResolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, error) {
	var err error
	var resolvedLocks []uint64
	var msBeforeTxnExpired int64
	if ch.Stats != nil {
		defer func(start time.Time) {
			recordRegionRequestRuntimeStats(ch.Stats, einsteindbrpc.CmdResolveLock, time.Since(start))
		}(time.Now())
	}
	if ch.resolveLite {
		msBeforeTxnExpired, resolvedLocks, err = ch.LockResolver.resolveLocksLite(bo, callerStartTS, locks)
	} else {
		msBeforeTxnExpired, resolvedLocks, err = ch.LockResolver.ResolveLocks(bo, callerStartTS, locks)
	}
	if err != nil {
		return msBeforeTxnExpired, err
	}
	if len(resolvedLocks) > 0 {
		ch.minCommitTSPushed.UFIDelate(resolvedLocks)
		return 0, nil
	}
	return msBeforeTxnExpired, nil
}

// SendReqCtx wraps the SendReqCtx function and use the resolved dagger result in the kvrpcpb.Context.
func (ch *clientHelper) SendReqCtx(bo *Backoffer, req *einsteindbrpc.Request, regionID RegionVerID, timeout time.Duration, sType ekv.StoreType, directStoreAddr string) (*einsteindbrpc.Response, *RPCContext, string, error) {
	sender := NewRegionRequestSender(ch.RegionCache, ch.Client)
	if len(directStoreAddr) > 0 {
		sender.storeAddr = directStoreAddr
	}
	sender.Stats = ch.Stats
	req.Context.ResolvedLocks = ch.minCommitTSPushed.Get()
	resp, ctx, err := sender.SendReqCtx(bo, req, regionID, timeout, sType)
	return resp, ctx, sender.storeAddr, err
}

const (
	minLogBackoffTime   = 100
	minLogKVProcessTime = 100
	minLogKVWaitTime    = 200
)

func (worker *INTERLOCKIteratorWorker) logTimeINTERLOCKTask(costTime time.Duration, task *INTERLOCKTask, bo *Backoffer, resp *einsteindbrpc.Response) {
	logStr := fmt.Sprintf("[TIME_INTERLOCK_PROCESS] resp_time:%s txnStartTS:%d region_id:%d store_addr:%s", costTime, worker.req.StartTs, task.region.id, task.storeAddr)
	if bo.totalSleep > minLogBackoffTime {
		backoffTypes := strings.Replace(fmt.Sprintf("%v", bo.types), " ", ",", -1)
		logStr += fmt.Sprintf(" backoff_ms:%d backoff_types:%s", bo.totalSleep, backoffTypes)
	}
	var detail *kvrpcpb.ExecDetails
	if resp.Resp != nil {
		switch r := resp.Resp.(type) {
		case *interlock.Response:
			detail = r.ExecDetails
		case *einsteindbrpc.INTERLOCKStreamResponse:
			// streaming request returns io.EOF, so the first INTERLOCKStreamResponse.Response maybe nil.
			if r.Response != nil {
				detail = r.Response.ExecDetails
			}
		default:
			panic("unreachable")
		}
	}

	if detail != nil && detail.HandleTime != nil {
		processMs := detail.HandleTime.ProcessMs
		waitMs := detail.HandleTime.WaitMs
		if processMs > minLogKVProcessTime {
			logStr += fmt.Sprintf(" kv_process_ms:%d", processMs)
			if detail.ScanDetail != nil {
				logStr = appendScanDetail(logStr, "write", detail.ScanDetail.Write)
				logStr = appendScanDetail(logStr, "data", detail.ScanDetail.Data)
				logStr = appendScanDetail(logStr, "dagger", detail.ScanDetail.Lock)
			}
		}
		if waitMs > minLogKVWaitTime {
			logStr += fmt.Sprintf(" kv_wait_ms:%d", waitMs)
			if processMs <= minLogKVProcessTime {
				logStr = strings.Replace(logStr, "TIME_INTERLOCK_PROCESS", "TIME_INTERLOCK_WAIT", 1)
			}
		}
	}
	logutil.Logger(bo.ctx).Info(logStr)
}

func appendScanDetail(logStr string, columnFamily string, scanInfo *kvrpcpb.ScanInfo) string {
	if scanInfo != nil {
		logStr += fmt.Sprintf(" scan_total_%s:%d", columnFamily, scanInfo.Total)
		logStr += fmt.Sprintf(" scan_processed_%s:%d", columnFamily, scanInfo.Processed)
	}
	return logStr
}

func (worker *INTERLOCKIteratorWorker) handleINTERLOCKStreamResult(bo *Backoffer, rpcCtx *RPCContext, stream *einsteindbrpc.INTERLOCKStreamResponse, task *INTERLOCKTask, ch chan<- *INTERLOCKResponse, costTime time.Duration) ([]*INTERLOCKTask, error) {
	defer stream.Close()
	var resp *interlock.Response
	var lastRange *interlock.KeyRange
	resp = stream.Response
	if resp == nil {
		// streaming request returns io.EOF, so the first Response is nil.
		return nil, nil
	}
	for {
		remainedTasks, err := worker.handleINTERLOCKResponse(bo, rpcCtx, &INTERLOCKResponse{pbResp: resp}, nil, nil, task, ch, lastRange, costTime)
		if err != nil || len(remainedTasks) != 0 {
			return remainedTasks, errors.Trace(err)
		}
		resp, err = stream.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil, nil
			}

			if err1 := bo.Backoff(boEinsteinDBRPC, errors.Errorf("recv stream response error: %v, task: %s", err, task)); err1 != nil {
				return nil, errors.Trace(err)
			}

			// No interlock.Response for network error, rebuild task based on the last success one.
			if errors.Cause(err) == context.Canceled {
				logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
			} else {
				logutil.BgLogger().Info("stream unknown error", zap.Error(err))
			}
			return worker.buildCausetTasksFromRemain(bo, lastRange, task)
		}
		if resp.Range != nil {
			lastRange = resp.Range
		}
	}
}

// handleINTERLOCKResponse checks interlock Response for region splidaggerd dagger,
// returns more tasks when that happens, or handles the response if no error.
// if we're handling streaming interlock response, lastRange is the range of last
// successful response, otherwise it's nil.
func (worker *INTERLOCKIteratorWorker) handleINTERLOCKResponse(bo *Backoffer, rpcCtx *RPCContext, resp *INTERLOCKResponse, cacheKey []byte, cacheValue *INTERLOCKrCacheValue, task *INTERLOCKTask, ch chan<- *INTERLOCKResponse, lastRange *interlock.KeyRange, costTime time.Duration) ([]*INTERLOCKTask, error) {
	if regionErr := resp.pbResp.GetRegionError(); regionErr != nil {
		if rpcCtx != nil && task.storeType == ekv.MilevaDB {
			resp.err = errors.Errorf("error: %v", regionErr)
			worker.sendToRespCh(resp, ch, true)
			return nil, nil
		}
		errStr := fmt.Sprintf("region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, error:%s",
			task.region.id, task.region.ver, task.storeType.Name(), task.storeAddr, regionErr.String())
		if err := bo.Backoff(BoRegionMiss, errors.New(errStr)); err != nil {
			return nil, errors.Trace(err)
		}
		// We may meet RegionError at the first packet, but not during visiting the stream.
		return buildCausetTasks(bo, worker.causetstore.regionCache, task.ranges, worker.req)
	}
	if lockErr := resp.pbResp.GetLocked(); lockErr != nil {
		logutil.BgLogger().Debug("interlock encounters",
			zap.Stringer("dagger", lockErr))
		msBeforeExpired, err1 := worker.ResolveLocks(bo, worker.req.StartTs, []*Lock{NewLock(lockErr)})
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if msBeforeExpired > 0 {
			if err := bo.BackoffWithMaxSleep(boTxnLockFast, int(msBeforeExpired), errors.New(lockErr.String())); err != nil {
				return nil, errors.Trace(err)
			}
		}
		return worker.buildCausetTasksFromRemain(bo, lastRange, task)
	}
	if otherErr := resp.pbResp.GetOtherError(); otherErr != "" {
		err := errors.Errorf("other error: %s", otherErr)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", worker.req.StartTs),
			zap.Uint64("regionID", task.region.id),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	// When the request is using streaming API, the `Range` is not nil.
	if resp.pbResp.Range != nil {
		resp.startKey = resp.pbResp.Range.Start
	} else if task.ranges != nil && task.ranges.len() > 0 {
		resp.startKey = task.ranges.at(0).StartKey
	}
	if resp.detail == nil {
		resp.detail = new(INTERLOCKRuntimeStats)
	}
	resp.detail.Stats = worker.Stats
	worker.Stats = nil
	resp.detail.BackoffTime = time.Duration(bo.totalSleep) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(bo.backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(bo.backoffTimes))
	for backoff := range bo.backoffTimes {
		backoffName := backoff.String()
		resp.detail.BackoffTimes[backoffName] = bo.backoffTimes[backoff]
		resp.detail.BackoffSleep[backoffName] = time.Duration(bo.backoffSleepMS[backoff]) * time.Millisecond
	}
	if rpcCtx != nil {
		resp.detail.CalleeAddress = rpcCtx.Addr
	}
	resp.respTime = costTime
	if pbDetails := resp.pbResp.ExecDetails; pbDetails != nil {
		if handleTime := pbDetails.HandleTime; handleTime != nil {
			resp.detail.WaitTime = time.Duration(handleTime.WaitMs) * time.Millisecond
			resp.detail.ProcessTime = time.Duration(handleTime.ProcessMs) * time.Millisecond
		}
		if scanDetail := pbDetails.ScanDetail; scanDetail != nil {
			if scanDetail.Write != nil {
				resp.detail.TotalKeys += scanDetail.Write.Total
				resp.detail.ProcessedKeys += scanDetail.Write.Processed
			}
		}
	}
	if resp.pbResp.IsCacheHit {
		if cacheValue == nil {
			return nil, errors.New("Internal error: received illegal EinsteinDB response")
		}
		// Cache hit and is valid: use cached data as response data and we don't uFIDelate the cache.
		data := make([]byte, len(cacheValue.Data))
		INTERLOCKy(data, cacheValue.Data)
		resp.pbResp.Data = data
		resp.detail.INTERLOCKrCacheHit = true
	} else {
		// Cache not hit or cache hit but not valid: uFIDelate the cache if the response can be cached.
		if cacheKey != nil && resp.pbResp.CanBeCached && resp.pbResp.CacheLastVersion > 0 {
			if worker.causetstore.INTERLOCKrCache.CheckAdmission(resp.pbResp.Data.Size(), resp.detail.ProcessTime) {
				data := make([]byte, len(resp.pbResp.Data))
				INTERLOCKy(data, resp.pbResp.Data)

				newCacheValue := INTERLOCKrCacheValue{
					Data:              data,
					TimeStamp:         worker.req.StartTs,
					RegionID:          task.region.id,
					RegionDataVersion: resp.pbResp.CacheLastVersion,
				}
				worker.causetstore.INTERLOCKrCache.Set(cacheKey, &newCacheValue)
			}
		}
	}
	worker.sendToRespCh(resp, ch, true)
	return nil, nil
}

// INTERLOCKRuntimeStats contains execution detail information.
type INTERLOCKRuntimeStats struct {
	execdetails.ExecDetails
	RegionRequestRuntimeStats

	INTERLOCKrCacheHit bool
}

func (worker *INTERLOCKIteratorWorker) handleMilevaDBSendReqErr(err error, task *INTERLOCKTask, ch chan<- *INTERLOCKResponse) error {
	errCode := errno.ErrUnknown
	errMsg := err.Error()
	if terror.ErrorEqual(err, ErrEinsteinDBServerTimeout) {
		errCode = errno.ErrEinsteinDBServerTimeout
		errMsg = "MilevaDB server timeout, address is " + task.storeAddr
	}
	selResp := fidelpb.SelectResponse{
		Warnings: []*fidelpb.Error{
			{
				Code: int32(errCode),
				Msg:  errMsg,
			},
		},
	}
	data, err := proto.Marshal(&selResp)
	if err != nil {
		return errors.Trace(err)
	}
	resp := &INTERLOCKResponse{
		pbResp: &interlock.Response{
			Data: data,
		},
		detail: &INTERLOCKRuntimeStats{},
	}
	worker.sendToRespCh(resp, ch, true)
	return nil
}

func (worker *INTERLOCKIteratorWorker) buildCausetTasksFromRemain(bo *Backoffer, lastRange *interlock.KeyRange, task *INTERLOCKTask) ([]*INTERLOCKTask, error) {
	remainedRanges := task.ranges
	if worker.req.Streaming && lastRange != nil {
		remainedRanges = worker.calculateRemain(task.ranges, lastRange, worker.req.Desc)
	}
	return buildCausetTasks(bo, worker.causetstore.regionCache, remainedRanges, worker.req)
}

// calculateRemain splits the input ranges into two, and take one of them according to desc flag.
// It's used in streaming API, to calculate which range is consumed and what needs to be retry.
// For example:
// ranges: [r1 --> r2) [r3 --> r4)
// split:      [s1   -->   s2)
// In normal scan order, all data before s1 is consumed, so the remain ranges should be [s1 --> r2) [r3 --> r4)
// In reverse scan order, all data after s2 is consumed, so the remain ranges should be [r1 --> r2) [r3 --> s2)
func (worker *INTERLOCKIteratorWorker) calculateRemain(ranges *INTERLOCKRanges, split *interlock.KeyRange, desc bool) *INTERLOCKRanges {
	if desc {
		left, _ := ranges.split(split.End)
		return left
	}
	_, right := ranges.split(split.Start)
	return right
}

func (it *INTERLOCKIterator) Close() error {
	if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
		close(it.finishCh)
	}
	it.rpcCancel.CancelAll()
	it.wg.Wait()
	return nil
}

type rateLimit struct {
	token chan struct{}
}

func newRateLimit(n int) *rateLimit {
	return &rateLimit{
		token: make(chan struct{}, n),
	}
}

func (r *rateLimit) getToken(done <-chan struct{}) (exit bool) {
	select {
	case <-done:
		return true
	case r.token <- struct{}{}:
		return false
	}
}

func (r *rateLimit) putToken() {
	select {
	case <-r.token:
	default:
		panic("put a redundant token")
	}
}

// INTERLOCKErrorResponse returns error when calling Next()
type INTERLOCKErrorResponse struct{ error }

func (it INTERLOCKErrorResponse) Next(ctx context.Context) (ekv.ResultSubset, error) {
	return nil, it.error
}

func (it INTERLOCKErrorResponse) Close() error {
	return nil
}
