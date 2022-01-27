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

package executor

import (
	"context"
	"fmt"
	"runtime/trace"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/types"
)

// UFIDelateExec represents a new uFIDelate executor.
type UFIDelateExec struct {
	baseExecutor

	OrderedList []*expression.Assignment

	// uFIDelatedEventKeys is a map for unique (Block, handle) pair.
	// The value is true if the event is changed, or false otherwise
	uFIDelatedEventKeys map[int64]*ekv.HandleMap
	tblID2block         map[int64]block.Block

	matched uint64 // a counter of matched rows during uFIDelate
	// tblDefCausPosInfos stores relationship between defCausumn ordinal to its block handle.
	// the defCausumns ordinals is present in ordinal range format, @see plannercore.TblDefCausPosInfos
	tblDefCausPosInfos        plannercore.TblDefCausPosInfoSlice
	evalBuffer                chunk.MutEvent
	allAssignmentsAreConstant bool
	drained                   bool
	memTracker                *memory.Tracker

	stats *runtimeStatsWithSnapshot
}

func (e *UFIDelateExec) exec(ctx context.Context, schemaReplicant *expression.Schema, event, newData []types.Causet) error {
	defer trace.StartRegion(ctx, "UFIDelateExec").End()
	assignFlag, err := plannercore.GetUFIDelateDeferredCausets(e.ctx, e.OrderedList, schemaReplicant.Len())
	if err != nil {
		return err
	}
	if e.uFIDelatedEventKeys == nil {
		e.uFIDelatedEventKeys = make(map[int64]*ekv.HandleMap)
	}
	for _, content := range e.tblDefCausPosInfos {
		tbl := e.tblID2block[content.TblID]
		if e.uFIDelatedEventKeys[content.TblID] == nil {
			e.uFIDelatedEventKeys[content.TblID] = ekv.NewHandleMap()
		}
		var handle ekv.Handle
		handle, err = content.HandleDefCauss.BuildHandleByCausets(event)
		if err != nil {
			return err
		}

		oldData := event[content.Start:content.End]
		newBlockData := newData[content.Start:content.End]
		uFIDelablock := false
		flags := assignFlag[content.Start:content.End]
		for _, flag := range flags {
			if flag {
				uFIDelablock = true
				break
			}
		}
		if !uFIDelablock {
			// If there's nothing to uFIDelate, we can just skip current event
			continue
		}
		var changed bool
		v, ok := e.uFIDelatedEventKeys[content.TblID].Get(handle)
		if !ok {
			// Event is matched for the first time, increment `matched` counter
			e.matched++
		} else {
			changed = v.(bool)
		}
		if changed {
			// Each matched event is uFIDelated once, even if it matches the conditions multiple times.
			continue
		}

		// UFIDelate event
		changed, err1 := uFIDelateRecord(ctx, e.ctx, handle, oldData, newBlockData, flags, tbl, false, e.memTracker)
		if err1 == nil {
			e.uFIDelatedEventKeys[content.TblID].Set(handle, changed)
			continue
		}

		sc := e.ctx.GetStochastikVars().StmtCtx
		if ekv.ErrKeyExists.Equal(err1) && sc.DupKeyAsWarning {
			sc.AppendWarning(err1)
			continue
		}
		return err1
	}
	return nil
}

// canNotUFIDelate checks the handle of a record to decide whether that record
// can not be uFIDelated. The handle is NULL only when it is the inner side of an
// outer join: the outer event can not match any inner rows, and in this scenario
// the inner handle field is filled with a NULL value.
//
// This fixes: https://github.com/whtcorpsinc/milevadb/issues/7176.
func (e *UFIDelateExec) canNotUFIDelate(handle types.Causet) bool {
	return handle.IsNull()
}

// Next implements the Executor Next interface.
func (e *UFIDelateExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.drained {
		numEvents, err := e.uFIDelateEvents(ctx)
		if err != nil {
			return err
		}
		e.drained = true
		e.ctx.GetStochastikVars().StmtCtx.AddRecordEvents(uint64(numEvents))
	}
	return nil
}

func (e *UFIDelateExec) uFIDelateEvents(ctx context.Context) (int, error) {
	fields := retTypes(e.children[0])
	defcausInfo := make([]*block.DeferredCauset, len(fields))
	for _, content := range e.tblDefCausPosInfos {
		tbl := e.tblID2block[content.TblID]
		for i, c := range tbl.WriblockDefCauss() {
			defcausInfo[content.Start+i] = c
		}
	}
	globalEventIdx := 0
	chk := newFirstChunk(e.children[0])
	if !e.allAssignmentsAreConstant {
		e.evalBuffer = chunk.MutEventFromTypes(fields)
	}
	composeFunc := e.fastComposeNewEvent
	if !e.allAssignmentsAreConstant {
		composeFunc = e.composeNewEvent
	}
	memUsageOfChk := int64(0)
	totalNumEvents := 0
	for {
		e.memTracker.Consume(-memUsageOfChk)
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return 0, err
		}

		if chk.NumEvents() == 0 {
			break
		}
		memUsageOfChk = chk.MemoryUsage()
		e.memTracker.Consume(memUsageOfChk)
		if e.defCauslectRuntimeStatsEnabled() {
			txn, err := e.ctx.Txn(false)
			if err == nil && txn.GetSnapshot() != nil {
				txn.GetSnapshot().SetOption(ekv.DefCauslectRuntimeStats, e.stats.SnapshotRuntimeStats)
			}
		}
		for rowIdx := 0; rowIdx < chk.NumEvents(); rowIdx++ {
			chunkEvent := chk.GetEvent(rowIdx)
			datumEvent := chunkEvent.GetCausetEvent(fields)
			newEvent, err1 := composeFunc(globalEventIdx, datumEvent, defcausInfo)
			if err1 != nil {
				return 0, err1
			}
			if err := e.exec(ctx, e.children[0].Schema(), datumEvent, newEvent); err != nil {
				return 0, err
			}
		}
		totalNumEvents += chk.NumEvents()
		chk = chunk.Renew(chk, e.maxChunkSize)
	}
	return totalNumEvents, nil
}

func (e *UFIDelateExec) handleErr(defCausName perceptron.CIStr, rowIdx int, err error) error {
	if err == nil {
		return nil
	}

	if types.ErrDataTooLong.Equal(err) {
		return resetErrDataTooLong(defCausName.O, rowIdx+1, err)
	}

	if types.ErrOverflow.Equal(err) {
		return types.ErrWarnDataOutOfRange.GenWithStackByArgs(defCausName.O, rowIdx+1)
	}

	return err
}

func (e *UFIDelateExec) fastComposeNewEvent(rowIdx int, oldEvent []types.Causet, defcaus []*block.DeferredCauset) ([]types.Causet, error) {
	newEventData := types.CloneEvent(oldEvent)
	for _, assign := range e.OrderedList {
		handleIdx, handleFound := e.tblDefCausPosInfos.FindHandle(assign.DefCaus.Index)
		if handleFound && e.canNotUFIDelate(oldEvent[handleIdx]) {
			continue
		}

		con := assign.Expr.(*expression.Constant)
		val, err := con.Eval(emptyEvent)
		if err = e.handleErr(assign.DefCausName, rowIdx, err); err != nil {
			return nil, err
		}

		// info of `_milevadb_rowid` defCausumn is nil.
		// No need to cast `_milevadb_rowid` defCausumn value.
		if defcaus[assign.DefCaus.Index] != nil {
			val, err = block.CastValue(e.ctx, val, defcaus[assign.DefCaus.Index].DeferredCausetInfo, false, false)
			if err = e.handleErr(assign.DefCausName, rowIdx, err); err != nil {
				return nil, err
			}
		}

		val.INTERLOCKy(&newEventData[assign.DefCaus.Index])
	}
	return newEventData, nil
}

func (e *UFIDelateExec) composeNewEvent(rowIdx int, oldEvent []types.Causet, defcaus []*block.DeferredCauset) ([]types.Causet, error) {
	newEventData := types.CloneEvent(oldEvent)
	e.evalBuffer.SetCausets(newEventData...)
	for _, assign := range e.OrderedList {
		handleIdx, handleFound := e.tblDefCausPosInfos.FindHandle(assign.DefCaus.Index)
		if handleFound && e.canNotUFIDelate(oldEvent[handleIdx]) {
			continue
		}
		val, err := assign.Expr.Eval(e.evalBuffer.ToEvent())
		if err = e.handleErr(assign.DefCausName, rowIdx, err); err != nil {
			return nil, err
		}

		// info of `_milevadb_rowid` defCausumn is nil.
		// No need to cast `_milevadb_rowid` defCausumn value.
		if defcaus[assign.DefCaus.Index] != nil {
			val, err = block.CastValue(e.ctx, val, defcaus[assign.DefCaus.Index].DeferredCausetInfo, false, false)
			if err = e.handleErr(assign.DefCausName, rowIdx, err); err != nil {
				return nil, err
			}
		}

		val.INTERLOCKy(&newEventData[assign.DefCaus.Index])
		e.evalBuffer.SetCauset(assign.DefCaus.Index, val)
	}
	return newEventData, nil
}

// Close implements the Executor Close interface.
func (e *UFIDelateExec) Close() error {
	e.setMessage()
	if e.runtimeStats != nil && e.stats != nil {
		txn, err := e.ctx.Txn(false)
		if err == nil && txn.GetSnapshot() != nil {
			txn.GetSnapshot().DelOption(ekv.DefCauslectRuntimeStats)
		}
	}
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *UFIDelateExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	return e.children[0].Open(ctx)
}

// setMessage sets info message(ERR_UFIDelATE_INFO) generated by UFIDelATE statement
func (e *UFIDelateExec) setMessage() {
	stmtCtx := e.ctx.GetStochastikVars().StmtCtx
	numMatched := e.matched
	numChanged := stmtCtx.UFIDelatedEvents()
	numWarnings := stmtCtx.WarningCount()
	msg := fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUFIDelateInfo], numMatched, numChanged, numWarnings)
	stmtCtx.SetMessage(msg)
}

func (e *UFIDelateExec) defCauslectRuntimeStatsEnabled() bool {
	if e.runtimeStats != nil {
		if e.stats == nil {
			snapshotStats := &einsteindb.SnapshotRuntimeStats{}
			e.stats = &runtimeStatsWithSnapshot{
				SnapshotRuntimeStats: snapshotStats,
			}
			e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, e.stats)
		}
		return true
	}
	return false
}
