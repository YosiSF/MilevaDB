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
	"fmt"
	"runtime/trace"

	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

// ReplaceExec represents a replace executor.
type ReplaceExec struct {
	*InsertValues
	Priority int
}

// Close implements the Executor Close interface.
func (e *ReplaceExec) Close() error {
	e.setMessage()
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *ReplaceExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker)

	if e.SelectExec != nil {
		return e.SelectExec.Open(ctx)
	}
	e.initEvalBuffer()
	return nil
}

// removeEvent removes the duplicate event and cleanup its keys in the key-value map,
// but if the to-be-removed event equals to the to-be-added event, no remove or add things to do.
func (e *ReplaceExec) removeEvent(ctx context.Context, txn ekv.Transaction, handle ekv.Handle, r toBeCheckedEvent) (bool, error) {
	newEvent := r.event
	oldEvent, err := getOldEvent(ctx, e.ctx, txn, r.t, handle, e.GenExprs)
	if err != nil {
		logutil.BgLogger().Error("get old event failed when replace",
			zap.String("handle", handle.String()),
			zap.String("toBeInsertedEvent", types.CausetsToStrNoErr(r.event)))
		if ekv.IsErrNotFound(err) {
			err = errors.NotFoundf("can not be duplicated event, due to old event not found. handle %s", handle)
		}
		return false, err
	}

	rowUnchanged, err := types.EqualCausets(e.ctx.GetStochaseinstein_dbars().StmtCtx, oldEvent, newEvent)
	if err != nil {
		return false, err
	}
	if rowUnchanged {
		e.ctx.GetStochaseinstein_dbars().StmtCtx.AddAffectedEvents(1)
		return true, nil
	}

	err = r.t.RemoveRecord(e.ctx, handle, oldEvent)
	if err != nil {
		return false, err
	}
	e.ctx.GetStochaseinstein_dbars().StmtCtx.AddAffectedEvents(1)
	return false, nil
}

// replaceEvent removes all duplicate rows for one event, then inserts it.
func (e *ReplaceExec) replaceEvent(ctx context.Context, r toBeCheckedEvent) error {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}

	if r.handleKey != nil {
		handle, err := blockcodec.DecodeEventKey(r.handleKey.newKey)
		if err != nil {
			return err
		}

		if _, err := txn.Get(ctx, r.handleKey.newKey); err == nil {
			rowUnchanged, err := e.removeEvent(ctx, txn, handle, r)
			if err != nil {
				return err
			}
			if rowUnchanged {
				return nil
			}
		} else {
			if !ekv.IsErrNotFound(err) {
				return err
			}
		}
	}

	// Keep on removing duplicated rows.
	for {
		rowUnchanged, foundDupKey, err := e.removeIndexEvent(ctx, txn, r)
		if err != nil {
			return err
		}
		if rowUnchanged {
			return nil
		}
		if foundDupKey {
			continue
		}
		break
	}

	// No duplicated rows now, insert the event.
	err = e.addRecord(ctx, r.event)
	if err != nil {
		return err
	}
	return nil
}

// removeIndexEvent removes the event which has a duplicated key.
// the return values:
//     1. bool: true when the event is unchanged. This means no need to remove, and then add the event.
//     2. bool: true when found the duplicated key. This only means that duplicated key was found,
//              and the event was removed.
//     3. error: the error.
func (e *ReplaceExec) removeIndexEvent(ctx context.Context, txn ekv.Transaction, r toBeCheckedEvent) (bool, bool, error) {
	for _, uk := range r.uniqueKeys {
		val, err := txn.Get(ctx, uk.newKey)
		if err != nil {
			if ekv.IsErrNotFound(err) {
				continue
			}
			return false, false, err
		}
		handle, err := blockcodec.DecodeHandleInUniqueIndexValue(val, uk.commonHandle)
		if err != nil {
			return false, true, err
		}
		rowUnchanged, err := e.removeEvent(ctx, txn, handle, r)
		if err != nil {
			return false, true, err
		}
		return rowUnchanged, true, nil
	}
	return false, false, nil
}

func (e *ReplaceExec) exec(ctx context.Context, newEvents [][]types.Causet) error {
	/*
	 * MyALLEGROSQL uses the following algorithm for REPLACE (and LOAD DATA ... REPLACE):
	 *  1. Try to insert the new event into the block
	 *  2. While the insertion fails because a duplicate-key error occurs for a primary key or unique index:
	 *  3. Delete from the block the conflicting event that has the duplicate key value
	 *  4. Try again to insert the new event into the block
	 * See http://dev.allegrosql.com/doc/refman/5.7/en/replace.html
	 *
	 * For REPLACE statements, the affected-rows value is 2 if the new event replaced an old event,
	 * because in this case, one event was inserted after the duplicate was deleted.
	 * See http://dev.allegrosql.com/doc/refman/5.7/en/allegrosql-affected-rows.html
	 */

	defer trace.StartRegion(ctx, "ReplaceExec").End()
	// Get keys need to be checked.
	toBeCheckedEvents, err := getKeysNeedCheck(ctx, e.ctx, e.Block, newEvents)
	if err != nil {
		return err
	}

	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	txnSize := txn.Size()

	if e.defCauslectRuntimeStatsEnabled() {
		if snapshot := txn.GetSnapshot(); snapshot != nil {
			snapshot.SetOption(ekv.DefCauslectRuntimeStats, e.stats.SnapshotRuntimeStats)
			defer snapshot.DelOption(ekv.DefCauslectRuntimeStats)
		}
	}

	// Use BatchGet to fill cache.
	// It's an optimization and could be removed without affecting correctness.
	if err = prefetchDataCache(ctx, txn, toBeCheckedEvents); err != nil {
		return err
	}

	e.ctx.GetStochaseinstein_dbars().StmtCtx.AddRecordEvents(uint64(len(newEvents)))
	for _, r := range toBeCheckedEvents {
		err = e.replaceEvent(ctx, r)
		if err != nil {
			return err
		}
	}
	e.memTracker.Consume(int64(txn.Size() - txnSize))
	return nil
}

// Next implements the Executor Next interface.
func (e *ReplaceExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if len(e.children) > 0 && e.children[0] != nil {
		return insertEventsFromSelect(ctx, e)
	}
	return insertEvents(ctx, e)
}

// setMessage sets info message(ERR_INSERT_INFO) generated by REPLACE statement
func (e *ReplaceExec) setMessage() {
	stmtCtx := e.ctx.GetStochaseinstein_dbars().StmtCtx
	numRecords := stmtCtx.RecordEvents()
	if e.SelectExec != nil || numRecords > 1 {
		numWarnings := stmtCtx.WarningCount()
		numDuplicates := stmtCtx.AffectedEvents() - numRecords
		msg := fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInsertInfo], numRecords, numDuplicates, numWarnings)
		stmtCtx.SetMessage(msg)
	}
}
