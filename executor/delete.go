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

package executor

import (
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
)

// DeleteExec represents a delete executor.
// See https://dev.allegrosql.com/doc/refman/5.7/en/delete.html
type DeleteExec struct {
	baseExecutor

	IsMultiBlock bool
	tblID2Block  map[int64]block.Block

	// tblDefCausPosInfos stores relationship between defCausumn ordinal to its block handle.
	// the defCausumns ordinals is present in ordinal range format, @see plannercore.TblDefCausPosInfos
	tblDefCausPosInfos plannercore.TblDefCausPosInfoSlice
	memTracker         *memory.Tracker
}

// Next implements the Executor Next interface.
func (e *DeleteExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.IsMultiBlock {
		return e.deleteMultiBlocksByChunk(ctx)
	}
	return e.deleteSingleBlockByChunk(ctx)
}

func (e *DeleteExec) deleteOneEvent(tbl block.Block, handleDefCauss plannercore.HandleDefCauss, isExtraHandle bool, event []types.Causet) error {
	end := len(event)
	if isExtraHandle {
		end--
	}
	handle, err := handleDefCauss.BuildHandleByCausets(event)
	if err != nil {
		return err
	}
	err = e.removeEvent(e.ctx, tbl, handle, event[:end])
	if err != nil {
		return err
	}
	return nil
}

func (e *DeleteExec) deleteSingleBlockByChunk(ctx context.Context) error {
	var (
		tbl            block.Block
		isExtrahandle  bool
		handleDefCauss plannercore.HandleDefCauss
		rowCount       int
	)
	for _, info := range e.tblDefCausPosInfos {
		tbl = e.tblID2Block[info.TblID]
		handleDefCauss = info.HandleDefCauss
		if !tbl.Meta().IsCommonHandle {
			isExtrahandle = handleDefCauss.IsInt() && handleDefCauss.GetDefCaus(0).ID == perceptron.ExtraHandleID
		}
	}

	batchDMLSize := e.ctx.GetStochaseinstein_dbars().DMLBatchSize
	// If milevadb_batch_delete is ON and not in a transaction, we could use BatchDelete mode.
	batchDelete := e.ctx.GetStochaseinstein_dbars().BatchDelete && !e.ctx.GetStochaseinstein_dbars().InTxn() &&
		config.GetGlobalConfig().EnableBatchDML && batchDMLSize > 0
	fields := retTypes(e.children[0])
	chk := newFirstChunk(e.children[0])
	memUsageOfChk := int64(0)
	for {
		e.memTracker.Consume(-memUsageOfChk)
		iter := chunk.NewIterator4Chunk(chk)
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return err
		}
		if chk.NumEvents() == 0 {
			break
		}
		memUsageOfChk = chk.MemoryUsage()
		e.memTracker.Consume(memUsageOfChk)
		for chunkEvent := iter.Begin(); chunkEvent != iter.End(); chunkEvent = iter.Next() {
			if batchDelete && rowCount >= batchDMLSize {
				e.ctx.StmtCommit()
				if err = e.ctx.NewTxn(ctx); err != nil {
					// We should return a special error for batch insert.
					return ErrBatchInsertFail.GenWithStack("BatchDelete failed with error: %v", err)
				}
				rowCount = 0
			}

			datumEvent := chunkEvent.GetCausetEvent(fields)
			err = e.deleteOneEvent(tbl, handleDefCauss, isExtrahandle, datumEvent)
			if err != nil {
				return err
			}
			rowCount++
		}
		chk = chunk.Renew(chk, e.maxChunkSize)
	}

	return nil
}

func (e *DeleteExec) composeTblEventMap(tblEventMap blockEventMapType, defCausPosInfos []plannercore.TblDefCausPosInfo, joinedEvent []types.Causet) error {
	// iterate all the joined blocks, and got the INTERLOCKresonding rows in joinedEvent.
	for _, info := range defCausPosInfos {
		if tblEventMap[info.TblID] == nil {
			tblEventMap[info.TblID] = ekv.NewHandleMap()
		}
		handle, err := info.HandleDefCauss.BuildHandleByCausets(joinedEvent)
		if err != nil {
			return err
		}
		// tblEventMap[info.TblID][handle] hold the event quantum binding to this block and this handle.
		tblEventMap[info.TblID].Set(handle, joinedEvent[info.Start:info.End])
	}
	return nil
}

func (e *DeleteExec) deleteMultiBlocksByChunk(ctx context.Context) error {
	defCausPosInfos := e.tblDefCausPosInfos
	tblEventMap := make(blockEventMapType)
	fields := retTypes(e.children[0])
	chk := newFirstChunk(e.children[0])
	memUsageOfChk := int64(0)
	for {
		e.memTracker.Consume(-memUsageOfChk)
		iter := chunk.NewIterator4Chunk(chk)
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return err
		}
		if chk.NumEvents() == 0 {
			break
		}
		memUsageOfChk = chk.MemoryUsage()
		e.memTracker.Consume(memUsageOfChk)

		for joinedChunkEvent := iter.Begin(); joinedChunkEvent != iter.End(); joinedChunkEvent = iter.Next() {
			joinedCausetEvent := joinedChunkEvent.GetCausetEvent(fields)
			err := e.composeTblEventMap(tblEventMap, defCausPosInfos, joinedCausetEvent)
			if err != nil {
				return err
			}
		}
		chk = chunk.Renew(chk, e.maxChunkSize)
	}

	return e.removeEventsInTblEventMap(tblEventMap)
}

func (e *DeleteExec) removeEventsInTblEventMap(tblEventMap blockEventMapType) error {
	for id, rowMap := range tblEventMap {
		var err error
		rowMap.Range(func(h ekv.Handle, val interface{}) bool {
			err = e.removeEvent(e.ctx, e.tblID2Block[id], h, val.([]types.Causet))
			if err != nil {
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *DeleteExec) removeEvent(ctx stochastikctx.Context, t block.Block, h ekv.Handle, data []types.Causet) error {
	txnState, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	memUsageOfTxnState := txnState.Size()
	err = t.RemoveRecord(ctx, h, data)
	if err != nil {
		return err
	}
	e.memTracker.Consume(int64(txnState.Size() - memUsageOfTxnState))
	ctx.GetStochaseinstein_dbars().StmtCtx.AddAffectedEvents(1)
	return nil
}

// Close implements the Executor Close interface.
func (e *DeleteExec) Close() error {
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *DeleteExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker)

	return e.children[0].Open(ctx)
}

// blockEventMapType is a map for unique (Block, Event) pair. key is the blockID.
// the key in map[int64]Event is the joined block handle, which represent a unique reference event.
// the value in map[int64]Event is the deleting event.
type blockEventMapType map[int64]*ekv.HandleMap
