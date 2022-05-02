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

package dbs

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/distsql"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/ranger"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/sqlexec"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"go.uber.org/zap"
)

// reorgCtx is for reorganization.
type reorgCtx struct {
	// doneCh is used to notify.
	// If the reorganization job is done, we will use this channel to notify outer.
	// TODO: Now we use goroutine to simulate reorganization jobs, later we may
	// use a persistent job list.
	doneCh chan error
	// rowCount is used to simulate a job's event count.
	rowCount int64
	// notifyCancelReorgJob is used to notify the backfilling goroutine if the DBS job is cancelled.
	// 0: job is not canceled.
	// 1: job is canceled.
	notifyCancelReorgJob int32
	// doneHandle is used to simulate the handle that has been processed.
	doneHandle atomic.Value // nullableHandle
}

// nullableHandle can causetstore <nil> handle.
// Storing a nil object to atomic.Value can lead to panic. This is a workaround.
type nullableHandle struct {
	handle ekv.Handle
}

// toString is used in log to avoid nil dereference panic.
func toString(handle ekv.Handle) string {
	if handle == nil {
		return "<nil>"
	}
	return handle.String()
}

// newContext gets a context. It is only used for adding defCausumn in reorganization state.
func newContext(causetstore ekv.CausetStorage) stochastikctx.Context {
	c := mock.NewContext()
	c.CausetStore = causetstore
	c.GetStochaseinstein_dbars().SetStatusFlag(allegrosql.ServerStatusAutocommit, false)
	c.GetStochaseinstein_dbars().StmtCtx.TimeZone = time.UTC
	return c
}

const defaultWaitReorgTimeout = 10 * time.Second

// ReorgWaitTimeout is the timeout that wait dbs in write reorganization stage.
var ReorgWaitTimeout = 5 * time.Second

func (rc *reorgCtx) notifyReorgCancel() {
	atomic.StoreInt32(&rc.notifyCancelReorgJob, 1)
}

func (rc *reorgCtx) cleanNotifyReorgCancel() {
	atomic.StoreInt32(&rc.notifyCancelReorgJob, 0)
}

func (rc *reorgCtx) isReorgCanceled() bool {
	return atomic.LoadInt32(&rc.notifyCancelReorgJob) == 1
}

func (rc *reorgCtx) setRowCount(count int64) {
	atomic.StoreInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) setNextHandle(doneHandle ekv.Handle) {
	rc.doneHandle.CausetStore(nullableHandle{handle: doneHandle})
}

func (rc *reorgCtx) increaseRowCount(count int64) {
	atomic.AddInt64(&rc.rowCount, count)
}

func (rc *reorgCtx) getRowCountAndHandle() (int64, ekv.Handle) {
	event := atomic.LoadInt64(&rc.rowCount)
	h, _ := (rc.doneHandle.Load()).(nullableHandle)
	return event, h.handle
}

func (rc *reorgCtx) clean() {
	rc.setRowCount(0)
	rc.setNextHandle(nil)
	rc.doneCh = nil
}

func (w *worker) runReorgJob(t *meta.Meta, reorgInfo *reorgInfo, tblInfo *perceptron.BlockInfo, lease time.Duration, f func() error) error {
	job := reorgInfo.Job
	if w.reorgCtx.doneCh == nil {
		// start a reorganization job
		w.wg.Add(1)
		w.reorgCtx.doneCh = make(chan error, 1)
		// initial reorgCtx
		w.reorgCtx.setRowCount(job.GetRowCount())
		w.reorgCtx.setNextHandle(reorgInfo.StartHandle)
		go func() {
			defer w.wg.Done()
			w.reorgCtx.doneCh <- f()
		}()
	}

	waitTimeout := defaultWaitReorgTimeout
	// if lease is 0, we are using a local storage,
	// and we can wait the reorganization to be done here.
	// if lease > 0, we don't need to wait here because
	// we should uFIDelate some job's progress context and try checking again,
	// so we use a very little timeout here.
	if lease > 0 {
		waitTimeout = ReorgWaitTimeout
	}

	// wait reorganization job done or timeout
	select {
	case err := <-w.reorgCtx.doneCh:
		rowCount, _ := w.reorgCtx.getRowCountAndHandle()
		logutil.BgLogger().Info("[dbs] run reorg job done", zap.Int64("handled rows", rowCount))
		// UFIDelate a job's RowCount.
		job.SetRowCount(rowCount)
		if err == nil {
			metrics.AddIndexProgress.Set(100)
		}
		w.reorgCtx.clean()
		return errors.Trace(err)
	case <-w.ctx.Done():
		logutil.BgLogger().Info("[dbs] run reorg job quit")
		w.reorgCtx.setNextHandle(nil)
		w.reorgCtx.setRowCount(0)
		// We return errWaitReorgTimeout here too, so that outer loop will break.
		return errWaitReorgTimeout
	case <-time.After(waitTimeout):
		rowCount, doneHandle := w.reorgCtx.getRowCountAndHandle()
		// UFIDelate a job's RowCount.
		job.SetRowCount(rowCount)
		uFIDelateAddIndexProgress(w, tblInfo, rowCount)
		// UFIDelate a reorgInfo's handle.
		err := t.UFIDelateDBSReorgStartHandle(job, doneHandle)
		logutil.BgLogger().Info("[dbs] run reorg job wait timeout", zap.Duration("waitTime", waitTimeout),
			zap.Int64("totalAddedRowCount", rowCount), zap.String("doneHandle", toString(doneHandle)), zap.Error(err))
		// If timeout, we will return, check the owner and retry to wait job done again.
		return errWaitReorgTimeout
	}
}

func uFIDelateAddIndexProgress(w *worker, tblInfo *perceptron.BlockInfo, addedRowCount int64) {
	if tblInfo == nil || addedRowCount == 0 {
		return
	}
	totalCount := getBlockTotalCount(w, tblInfo)
	progress := float64(0)
	if totalCount > 0 {
		progress = float64(addedRowCount) / float64(totalCount)
	} else {
		progress = 1
	}
	if progress > 1 {
		progress = 1
	}
	metrics.AddIndexProgress.Set(progress * 100)
}

func getBlockTotalCount(w *worker, tblInfo *perceptron.BlockInfo) int64 {
	var ctx stochastikctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return statistics.PseudoRowCount
	}
	defer w.sessPool.put(ctx)

	executor, ok := ctx.(sqlexec.RestrictedALLEGROSQLExecutor)
	// `mock.Context` is used in tests, which doesn't implement RestrictedALLEGROSQLExecutor
	if !ok {
		return statistics.PseudoRowCount
	}
	allegrosql := fmt.Sprintf("select block_rows from information_schema.blocks where milevadb_block_id=%v;", tblInfo.ID)
	rows, _, err := executor.ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return statistics.PseudoRowCount
	}
	if len(rows) != 1 {
		return statistics.PseudoRowCount
	}
	return rows[0].GetInt64(0)
}

func (w *worker) isReorgRunnable(d *dbsCtx) error {
	if isChanClosed(w.ctx.Done()) {
		// Worker is closed. So it can't do the reorganizational job.
		return errInvalidWorker.GenWithStack("worker is closed")
	}

	if w.reorgCtx.isReorgCanceled() {
		// Job is cancelled. So it can't be done.
		return errCancelledDBSJob
	}

	if !d.isOwner() {
		// If it's not the owner, we will try later, so here just returns an error.
		logutil.BgLogger().Info("[dbs] DBS worker is not the DBS owner", zap.String("ID", d.uuid))
		return errors.Trace(errNotOwner)
	}
	return nil
}

type reorgInfo struct {
	*perceptron.Job

	// StartHandle is the first handle of the adding indices block.
	StartHandle ekv.Handle
	// EndHandle is the last handle of the adding indices block.
	EndHandle ekv.Handle
	d         *dbsCtx
	first     bool
	// PhysicalBlockID is used for partitioned block.
	// DBS reorganize for a partitioned block will handle partitions one by one,
	// PhysicalBlockID is used to trace the current partition we are handling.
	// If the block is not partitioned, PhysicalBlockID would be BlockID.
	PhysicalBlockID int64
}

func (r *reorgInfo) String() string {
	return "StartHandle:" + toString(r.StartHandle) + "," +
		"EndHandle:" + toString(r.EndHandle) + "," +
		"first:" + strconv.FormatBool(r.first) + "," +
		"PhysicalBlockID:" + strconv.FormatInt(r.PhysicalBlockID, 10)
}

func constructDescBlockScanPB(physicalBlockID int64, tblInfo *perceptron.BlockInfo, handleDefCauss []*perceptron.DeferredCausetInfo) *fidelpb.Executor {
	tblScan := blocks.BuildBlockScanFromInfos(tblInfo, handleDefCauss)
	tblScan.BlockId = physicalBlockID
	tblScan.Desc = true
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeBlockScan, TblScan: tblScan}
}

func constructLimitPB(count uint64) *fidelpb.Executor {
	limitExec := &fidelpb.Limit{
		Limit: count,
	}
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeLimit, Limit: limitExec}
}

func buildDescBlockScanPosetDag(ctx stochastikctx.Context, tbl block.PhysicalBlock, handleDefCauss []*perceptron.DeferredCausetInfo, limit uint64) (*fidelpb.PosetDagRequest, error) {
	posetPosetDagReq := &fidelpb.PosetDagRequest{}
	_, timeZoneOffset := time.Now().In(time.UTC).Zone()
	posetPosetDagReq.TimeZoneOffset = int64(timeZoneOffset)
	for i := range handleDefCauss {
		posetPosetDagReq.OutputOffsets = append(posetPosetDagReq.OutputOffsets, uint32(i))
	}
	posetPosetDagReq.Flags |= perceptron.FlagInSelectStmt

	tblScanExec := constructDescBlockScanPB(tbl.GetPhysicalID(), tbl.Meta(), handleDefCauss)
	posetPosetDagReq.Executors = append(posetPosetDagReq.Executors, tblScanExec)
	posetPosetDagReq.Executors = append(posetPosetDagReq.Executors, constructLimitPB(limit))
	distsql.SetEncodeType(ctx, posetPosetDagReq)
	return posetPosetDagReq, nil
}

func getDeferredCausetsTypes(defCausumns []*perceptron.DeferredCausetInfo) []*types.FieldType {
	defCausTypes := make([]*types.FieldType, 0, len(defCausumns))
	for _, defCaus := range defCausumns {
		defCausTypes = append(defCausTypes, &defCaus.FieldType)
	}
	return defCausTypes
}

// buildDescBlockScan builds a desc block scan upon tblInfo.
func (dc *dbsCtx) buildDescBlockScan(ctx context.Context, startTS uint64, tbl block.PhysicalBlock,
	handleDefCauss []*perceptron.DeferredCausetInfo, limit uint64) (distsql.SelectResult, error) {
	sctx := newContext(dc.causetstore)
	posetPosetDagPB, err := buildDescBlockScanPosetDag(sctx, tbl, handleDefCauss, limit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var b distsql.RequestBuilder
	var builder *distsql.RequestBuilder
	if !tbl.Meta().IsCommonHandle {
		ranges := ranger.FullIntRange(false)
		builder = b.SetBlockRanges(tbl.GetPhysicalID(), ranges, nil)
	} else {
		ranges := ranger.FullNotNullRange()
		builder = b.SetCommonHandleRanges(sctx.GetStochaseinstein_dbars().StmtCtx, tbl.GetPhysicalID(), ranges)
	}
	builder.SetPosetDagRequest(posetPosetDagPB).
		SetStartTS(startTS).
		SetKeepOrder(true).
		SetConcurrency(1).SetDesc(true)

	builder.Request.NotFillCache = true
	builder.Request.Priority = ekv.PriorityLow

	kvReq, err := builder.Build()
	if err != nil {
		return nil, errors.Trace(err)
	}

	result, err := distsql.Select(ctx, sctx, kvReq, getDeferredCausetsTypes(handleDefCauss), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

// GetBlockMaxHandle gets the max handle of a PhysicalBlock.
func (dc *dbsCtx) GetBlockMaxHandle(startTS uint64, tbl block.PhysicalBlock) (maxHandle ekv.Handle, emptyBlock bool, err error) {
	var handleDefCauss []*perceptron.DeferredCausetInfo
	var pkIdx *perceptron.IndexInfo
	tblInfo := tbl.Meta()
	switch {
	case tblInfo.PKIsHandle:
		for _, defCaus := range tbl.Meta().DeferredCausets {
			if allegrosql.HasPriKeyFlag(defCaus.Flag) {
				handleDefCauss = []*perceptron.DeferredCausetInfo{defCaus}
				break
			}
		}
	case tblInfo.IsCommonHandle:
		pkIdx = blocks.FindPrimaryIndex(tblInfo)
		defcaus := tblInfo.DefCauss()
		for _, idxDefCaus := range pkIdx.DeferredCausets {
			handleDefCauss = append(handleDefCauss, defcaus[idxDefCaus.Offset])
		}
	default:
		handleDefCauss = []*perceptron.DeferredCausetInfo{perceptron.NewExtraHandleDefCausInfo()}
	}

	ctx := context.Background()
	// build a desc scan of tblInfo, which limit is 1, we can use it to retrieve the last handle of the block.
	result, err := dc.buildDescBlockScan(ctx, startTS, tbl, handleDefCauss, 1)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	defer terror.Call(result.Close)

	chk := chunk.New(getDeferredCausetsTypes(handleDefCauss), 1, 1)
	err = result.Next(ctx, chk)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if chk.NumRows() == 0 {
		// empty block
		return nil, true, nil
	}
	sessCtx := newContext(dc.causetstore)
	event := chk.GetRow(0)
	if tblInfo.IsCommonHandle {
		maxHandle, err = buildCommonHandleFromChunkRow(sessCtx.GetStochaseinstein_dbars().StmtCtx, tblInfo, pkIdx, handleDefCauss, event)
		return maxHandle, false, err
	}
	return ekv.IntHandle(event.GetInt64(0)), false, nil
}

func buildCommonHandleFromChunkRow(sctx *stmtctx.StatementContext, tblInfo *perceptron.BlockInfo, idxInfo *perceptron.IndexInfo,
	defcaus []*perceptron.DeferredCausetInfo, event chunk.Row) (ekv.Handle, error) {
	fieldTypes := make([]*types.FieldType, 0, len(defcaus))
	for _, defCaus := range defcaus {
		fieldTypes = append(fieldTypes, &defCaus.FieldType)
	}
	datumRow := event.GetCausetRow(fieldTypes)
	blockcodec.TruncateIndexValues(tblInfo, idxInfo, datumRow)

	var handleBytes []byte
	handleBytes, err := codec.EncodeKey(sctx, nil, datumRow...)
	if err != nil {
		return nil, err
	}
	return ekv.NewCommonHandle(handleBytes)
}

// getBlockRange gets the start and end handle of a block (or partition).
func getBlockRange(d *dbsCtx, tbl block.PhysicalBlock, snapshotVer uint64, priority int) (startHandle, endHandle ekv.Handle, err error) {
	// Get the start handle of this partition.
	err = iterateSnapshotRows(d.causetstore, priority, tbl, snapshotVer, nil, nil, true,
		func(h ekv.Handle, rowKey ekv.Key, rawRecord []byte) (bool, error) {
			startHandle = h
			return false, nil
		})
	if err != nil {
		return startHandle, endHandle, errors.Trace(err)
	}
	var emptyBlock bool
	endHandle, emptyBlock, err = d.GetBlockMaxHandle(snapshotVer, tbl)
	if err != nil {
		return startHandle, endHandle, errors.Trace(err)
	}
	if emptyBlock || endHandle.Compare(startHandle) < 0 {
		logutil.BgLogger().Info("[dbs] get block range, endHandle < startHandle", zap.String("block", fmt.Sprintf("%v", tbl.Meta())),
			zap.Int64("block/partition ID", tbl.GetPhysicalID()), zap.String("endHandle", toString(endHandle)), zap.String("startHandle", toString(startHandle)))
		endHandle = startHandle
	}
	return
}

func getValidCurrentVersion(causetstore ekv.CausetStorage) (ver ekv.Version, err error) {
	ver, err = causetstore.CurrentVersion()
	if err != nil {
		return ver, errors.Trace(err)
	} else if ver.Ver <= 0 {
		return ver, errInvalidStoreVer.GenWithStack("invalid storage current version %d", ver.Ver)
	}
	return ver, nil
}

func getReorgInfo(d *dbsCtx, t *meta.Meta, job *perceptron.Job, tbl block.Block) (*reorgInfo, error) {
	var (
		start ekv.Handle
		end   ekv.Handle
		pid   int64
		info  reorgInfo
	)

	if job.SnapshotVer == 0 {
		info.first = true
		// get the current version for reorganization if we don't have
		ver, err := getValidCurrentVersion(d.causetstore)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tblInfo := tbl.Meta()
		pid = tblInfo.ID
		var tb block.PhysicalBlock
		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			pid = pi.Definitions[0].ID
			tb = tbl.(block.PartitionedBlock).GetPartition(pid)
		} else {
			tb = tbl.(block.PhysicalBlock)
		}
		start, end, err = getBlockRange(d, tb, ver.Ver, job.Priority)
		if err != nil {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Info("[dbs] job get block range",
			zap.Int64("jobID", job.ID), zap.Int64("physicalBlockID", pid),
			zap.String("startHandle", toString(start)), zap.String("endHandle", toString(end)))

		failpoint.Inject("errorUFIDelateReorgHandle", func() (*reorgInfo, error) {
			return &info, errors.New("occur an error when uFIDelate reorg handle")
		})
		err = t.UFIDelateDBSReorgHandle(job, start, end, pid)
		if err != nil {
			return &info, errors.Trace(err)
		}
		// UFIDelate info should after data persistent.
		job.SnapshotVer = ver.Ver
	} else {
		var err error
		start, end, pid, err = t.GetDBSReorgHandle(job, tbl.Meta().IsCommonHandle)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	info.Job = job
	info.d = d
	info.StartHandle = start
	info.EndHandle = end
	info.PhysicalBlockID = pid

	return &info, nil
}

func (r *reorgInfo) UFIDelateReorgMeta(txn ekv.Transaction, startHandle, endHandle ekv.Handle, physicalBlockID int64) error {
	if startHandle == nil && endHandle == nil {
		return nil
	}
	t := meta.NewMeta(txn)
	return errors.Trace(t.UFIDelateDBSReorgHandle(r.Job, startHandle, endHandle, physicalBlockID))
}
