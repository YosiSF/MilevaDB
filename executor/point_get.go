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
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/rowcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
)

func (b *executorBuilder) buildPointGet(p *plannercore.PointGetPlan) Executor {
	if b.ctx.GetStochaseinstein_dbars().IsPessimisticReadConsistency() {
		if err := b.refreshForUFIDelateTSForRC(); err != nil {
			b.err = err
			return nil
		}
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	e := &PointGetExecutor{
		baseExecutor: newBaseExecutor(b.ctx, p.Schema(), p.ID()),
	}
	e.base().initCap = 1
	e.base().maxChunkSize = 1
	if p.Lock {
		b.hasLock = true
	}
	e.Init(p, startTS)
	return e
}

// PointGetExecutor executes point select query.
type PointGetExecutor struct {
	baseExecutor

	tblInfo      *perceptron.BlockInfo
	handle       ekv.Handle
	idxInfo      *perceptron.IndexInfo
	partInfo     *perceptron.PartitionDefinition
	idxKey       ekv.Key
	handleVal    []byte
	idxVals      []types.Causet
	startTS      uint64
	txn          ekv.Transaction
	snapshot     ekv.Snapshot
	done         bool
	dagger       bool
	lockWaitTime int64
	rowDecoder   *rowcodec.ChunkDecoder

	defCausumns []*perceptron.DeferredCausetInfo
	// virtualDeferredCausetIndex records all the indices of virtual defCausumns and sort them in definition
	// to make sure we can compute the virtual defCausumn in right order.
	virtualDeferredCausetIndex []int

	// virtualDeferredCausetRetFieldTypes records the RetFieldTypes of virtual defCausumns.
	virtualDeferredCausetRetFieldTypes []*types.FieldType

	stats *runtimeStatsWithSnapshot
}

// Init set fields needed for PointGetExecutor reuse, this does NOT change baseExecutor field
func (e *PointGetExecutor) Init(p *plannercore.PointGetPlan, startTs uint64) {
	decoder := NewEventDecoder(e.ctx, p.Schema(), p.TblInfo)
	e.tblInfo = p.TblInfo
	e.handle = p.Handle
	e.idxInfo = p.IndexInfo
	e.idxVals = p.IndexValues
	e.startTS = startTs
	e.done = false
	e.dagger = p.Lock
	e.lockWaitTime = p.LockWaitTime
	e.rowDecoder = decoder
	e.partInfo = p.PartitionInfo
	e.defCausumns = p.DeferredCausets
	e.buildVirtualDeferredCausetInfo()
}

// buildVirtualDeferredCausetInfo saves virtual defCausumn indices and sort them in definition order
func (e *PointGetExecutor) buildVirtualDeferredCausetInfo() {
	e.virtualDeferredCausetIndex = buildVirtualDeferredCausetIndex(e.Schema(), e.defCausumns)
	if len(e.virtualDeferredCausetIndex) > 0 {
		e.virtualDeferredCausetRetFieldTypes = make([]*types.FieldType, len(e.virtualDeferredCausetIndex))
		for i, idx := range e.virtualDeferredCausetIndex {
			e.virtualDeferredCausetRetFieldTypes[i] = e.schemaReplicant.DeferredCausets[idx].RetType
		}
	}
}

// Open implements the Executor interface.
func (e *PointGetExecutor) Open(context.Context) error {
	txnCtx := e.ctx.GetStochaseinstein_dbars().TxnCtx
	snapshotTS := e.startTS
	if e.dagger {
		snapshotTS = txnCtx.GetForUFIDelateTS()
	}
	var err error
	e.txn, err = e.ctx.Txn(false)
	if err != nil {
		return err
	}
	if e.txn.Valid() && txnCtx.StartTS == txnCtx.GetForUFIDelateTS() {
		e.snapshot = e.txn.GetSnapshot()
	} else {
		e.snapshot, err = e.ctx.GetStore().GetSnapshot(ekv.Version{Ver: snapshotTS})
		if err != nil {
			return err
		}
	}
	if e.runtimeStats != nil {
		snapshotStats := &einsteindb.SnapshotRuntimeStats{}
		e.stats = &runtimeStatsWithSnapshot{
			SnapshotRuntimeStats: snapshotStats,
		}
		e.snapshot.SetOption(ekv.DefCauslectRuntimeStats, snapshotStats)
		e.ctx.GetStochaseinstein_dbars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, e.stats)
	}
	if e.ctx.GetStochaseinstein_dbars().GetReplicaRead().IsFollowerRead() {
		e.snapshot.SetOption(ekv.ReplicaRead, ekv.ReplicaReadFollower)
	}
	e.snapshot.SetOption(ekv.TaskID, e.ctx.GetStochaseinstein_dbars().StmtCtx.TaskID)
	return nil
}

// Close implements the Executor interface.
func (e *PointGetExecutor) Close() error {
	if e.runtimeStats != nil && e.snapshot != nil {
		e.snapshot.DelOption(ekv.DefCauslectRuntimeStats)
	}
	e.done = false
	return nil
}

// Next implements the Executor interface.
func (e *PointGetExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true

	var tblID int64
	var err error
	if e.partInfo != nil {
		tblID = e.partInfo.ID
	} else {
		tblID = e.tblInfo.ID
	}
	if e.idxInfo != nil {
		if isCommonHandleRead(e.tblInfo, e.idxInfo) {
			handleBytes, err := EncodeUniqueIndexValuesForKey(e.ctx, e.tblInfo, e.idxInfo, e.idxVals)
			if err != nil {
				return err
			}
			e.handle, err = ekv.NewCommonHandle(handleBytes)
			if err != nil {
				return err
			}
		} else {
			e.idxKey, err = EncodeUniqueIndexKey(e.ctx, e.tblInfo, e.idxInfo, e.idxVals, tblID)
			if err != nil && !ekv.ErrNotExist.Equal(err) {
				return err
			}

			e.handleVal, err = e.get(ctx, e.idxKey)
			if err != nil {
				if !ekv.ErrNotExist.Equal(err) {
					return err
				}
			}
			if len(e.handleVal) == 0 {
				// handle is not found, try dagger the index key if isolation level is not read consistency
				if e.ctx.GetStochaseinstein_dbars().IsPessimisticReadConsistency() {
					return nil
				}
				return e.lockKeyIfNeeded(ctx, e.idxKey)
			}
			var iv ekv.Handle
			iv, err = blockcodec.DecodeHandleInUniqueIndexValue(e.handleVal, e.tblInfo.IsCommonHandle)
			if err != nil {
				return err
			}
			e.handle = iv

			// The injection is used to simulate following scenario:
			// 1. Stochastik A create a point get query but pause before second time `GET` ekv from backend
			// 2. Stochastik B create an UFIDelATE query to uFIDelate the record that will be obtained in step 1
			// 3. Then point get retrieve data from backend after step 2 finished
			// 4. Check the result
			failpoint.InjectContext(ctx, "pointGetRepeablockReadTest-step1", func() {
				if ch, ok := ctx.Value("pointGetRepeablockReadTest").(chan struct{}); ok {
					// Make `UFIDelATE` continue
					close(ch)
				}
				// Wait `UFIDelATE` finished
				failpoint.InjectContext(ctx, "pointGetRepeablockReadTest-step2", nil)
			})
		}
	}

	key := blockcodec.EncodeEventKeyWithHandle(tblID, e.handle)
	val, err := e.getAndLock(ctx, key)
	if err != nil {
		return err
	}
	if len(val) == 0 {
		if e.idxInfo != nil && !isCommonHandleRead(e.tblInfo, e.idxInfo) {
			return ekv.ErrNotExist.GenWithStack("inconsistent extra index %s, handle %d not found in block",
				e.idxInfo.Name.O, e.handle)
		}
		return nil
	}
	err = DecodeEventValToChunk(e.base().ctx, e.schemaReplicant, e.tblInfo, e.handle, val, req, e.rowDecoder)
	if err != nil {
		return err
	}

	err = FillVirtualDeferredCausetValue(e.virtualDeferredCausetRetFieldTypes, e.virtualDeferredCausetIndex,
		e.schemaReplicant, e.defCausumns, e.ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (e *PointGetExecutor) getAndLock(ctx context.Context, key ekv.Key) (val []byte, err error) {
	if e.ctx.GetStochaseinstein_dbars().IsPessimisticReadConsistency() {
		// Only Lock the exist keys in RC isolation.
		val, err = e.get(ctx, key)
		if err != nil {
			if !ekv.ErrNotExist.Equal(err) {
				return nil, err
			}
			return nil, nil
		}
		err = e.lockKeyIfNeeded(ctx, key)
		if err != nil {
			return nil, err
		}
		return val, nil
	}
	// Lock the key before get in RR isolation, then get will get the value from the cache.
	err = e.lockKeyIfNeeded(ctx, key)
	if err != nil {
		return nil, err
	}
	val, err = e.get(ctx, key)
	if err != nil {
		if !ekv.ErrNotExist.Equal(err) {
			return nil, err
		}
		return nil, nil
	}
	return val, nil
}

func (e *PointGetExecutor) lockKeyIfNeeded(ctx context.Context, key []byte) error {
	if e.dagger {
		seVars := e.ctx.GetStochaseinstein_dbars()
		lockCtx := newLockCtx(seVars, e.lockWaitTime)
		lockCtx.ReturnValues = true
		lockCtx.Values = map[string]ekv.ReturnedValue{}
		err := doLockKeys(ctx, e.ctx, lockCtx, key)
		if err != nil {
			return err
		}
		lockCtx.ValuesLock.Lock()
		defer lockCtx.ValuesLock.Unlock()
		for key, val := range lockCtx.Values {
			if !val.AlreadyLocked {
				seVars.TxnCtx.SetPessimisticLockCache(ekv.Key(key), val.Value)
			}
		}
		if len(e.handleVal) > 0 {
			seVars.TxnCtx.SetPessimisticLockCache(e.idxKey, e.handleVal)
		}
	}
	return nil
}

// get will first try to get from txn buffer, then check the pessimistic dagger cache,
// then the causetstore. Kv.ErrNotExist will be returned if key is not found
func (e *PointGetExecutor) get(ctx context.Context, key ekv.Key) ([]byte, error) {
	if len(key) == 0 {
		return nil, ekv.ErrNotExist
	}
	if e.txn.Valid() && !e.txn.IsReadOnly() {
		// We cannot use txn.Get directly here because the snapshot in txn and the snapshot of e.snapshot may be
		// different for pessimistic transaction.
		val, err := e.txn.GetMemBuffer().Get(ctx, key)
		if err == nil {
			return val, err
		}
		if !ekv.IsErrNotFound(err) {
			return nil, err
		}
		// key does not exist in mem buffer, check the dagger cache
		var ok bool
		val, ok = e.ctx.GetStochaseinstein_dbars().TxnCtx.GetKeyInPessimisticLockCache(key)
		if ok {
			return val, nil
		}
		// fallthrough to snapshot get.
	}
	return e.snapshot.Get(ctx, key)
}

// EncodeUniqueIndexKey encodes a unique index key.
func EncodeUniqueIndexKey(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, idxInfo *perceptron.IndexInfo, idxVals []types.Causet, tID int64) (_ []byte, err error) {
	encodedIdxVals, err := EncodeUniqueIndexValuesForKey(ctx, tblInfo, idxInfo, idxVals)
	if err != nil {
		return nil, err
	}
	return blockcodec.EncodeIndexSeekKey(tID, idxInfo.ID, encodedIdxVals), nil
}

// EncodeUniqueIndexValuesForKey encodes unique index values for a key.
func EncodeUniqueIndexValuesForKey(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, idxInfo *perceptron.IndexInfo, idxVals []types.Causet) (_ []byte, err error) {
	sc := ctx.GetStochaseinstein_dbars().StmtCtx
	for i := range idxVals {
		defCausInfo := tblInfo.DeferredCausets[idxInfo.DeferredCausets[i].Offset]
		// block.CastValue will append 0x0 if the string value's length is smaller than the BINARY defCausumn's length.
		// So we don't use CastValue for string value for now.
		// TODO: merge two if branch.
		if defCausInfo.Tp == allegrosql.TypeString || defCausInfo.Tp == allegrosql.TypeVarString || defCausInfo.Tp == allegrosql.TypeVarchar {
			var str string
			str, err = idxVals[i].ToString()
			idxVals[i].SetString(str, defCausInfo.FieldType.DefCauslate)
		} else {
			idxVals[i], err = block.CastValue(ctx, idxVals[i], defCausInfo, true, false)
			if types.ErrOverflow.Equal(err) {
				return nil, ekv.ErrNotExist
			}
		}
		if err != nil {
			return nil, err
		}
	}

	encodedIdxVals, err := codec.EncodeKey(sc, nil, idxVals...)
	if err != nil {
		return nil, err
	}
	return encodedIdxVals, nil
}

// DecodeEventValToChunk decodes event value into chunk checking event format used.
func DecodeEventValToChunk(sctx stochastikctx.Context, schemaReplicant *expression.Schema, tblInfo *perceptron.BlockInfo,
	handle ekv.Handle, rowVal []byte, chk *chunk.Chunk, rd *rowcodec.ChunkDecoder) error {
	if rowcodec.IsNewFormat(rowVal) {
		return rd.DecodeToChunk(rowVal, handle, chk)
	}
	return decodeOldEventValToChunk(sctx, schemaReplicant, tblInfo, handle, rowVal, chk)
}

func decodeOldEventValToChunk(sctx stochastikctx.Context, schemaReplicant *expression.Schema, tblInfo *perceptron.BlockInfo, handle ekv.Handle,
	rowVal []byte, chk *chunk.Chunk) error {
	pkDefCauss := blocks.TryGetCommonPkDeferredCausetIds(tblInfo)
	defCausID2CutPos := make(map[int64]int, schemaReplicant.Len())
	for _, defCaus := range schemaReplicant.DeferredCausets {
		if _, ok := defCausID2CutPos[defCaus.ID]; !ok {
			defCausID2CutPos[defCaus.ID] = len(defCausID2CutPos)
		}
	}
	cutVals, err := blockcodec.CutEventNew(rowVal, defCausID2CutPos)
	if err != nil {
		return err
	}
	if cutVals == nil {
		cutVals = make([][]byte, len(defCausID2CutPos))
	}
	decoder := codec.NewDecoder(chk, sctx.GetStochaseinstein_dbars().Location())
	for i, defCaus := range schemaReplicant.DeferredCausets {
		// fill the virtual defCausumn value after event calculation
		if defCaus.VirtualExpr != nil {
			chk.AppendNull(i)
			continue
		}
		ok, err := tryDecodeFromHandle(tblInfo, i, defCaus, handle, chk, decoder, pkDefCauss)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		cutPos := defCausID2CutPos[defCaus.ID]
		if len(cutVals[cutPos]) == 0 {
			defCausInfo := getDefCausInfoByID(tblInfo, defCaus.ID)
			d, err1 := block.GetDefCausOriginDefaultValue(sctx, defCausInfo)
			if err1 != nil {
				return err1
			}
			chk.AppendCauset(i, &d)
			continue
		}
		_, err = decoder.DecodeOne(cutVals[cutPos], i, defCaus.RetType)
		if err != nil {
			return err
		}
	}
	return nil
}

func tryDecodeFromHandle(tblInfo *perceptron.BlockInfo, i int, defCaus *expression.DeferredCauset, handle ekv.Handle, chk *chunk.Chunk, decoder *codec.Decoder, pkDefCauss []int64) (bool, error) {
	if tblInfo.PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.RetType.Flag) {
		chk.AppendInt64(i, handle.IntValue())
		return true, nil
	}
	if defCaus.ID == perceptron.ExtraHandleID {
		chk.AppendInt64(i, handle.IntValue())
		return true, nil
	}
	// Try to decode common handle.
	if allegrosql.HasPriKeyFlag(defCaus.RetType.Flag) {
		for i, hid := range pkDefCauss {
			if defCaus.ID == hid {
				_, err := decoder.DecodeOne(handle.EncodedDefCaus(i), i, defCaus.RetType)
				if err != nil {
					return false, errors.Trace(err)
				}
				return true, nil
			}
		}
	}
	return false, nil
}

func getDefCausInfoByID(tbl *perceptron.BlockInfo, defCausID int64) *perceptron.DeferredCausetInfo {
	for _, defCaus := range tbl.DeferredCausets {
		if defCaus.ID == defCausID {
			return defCaus
		}
	}
	return nil
}

type runtimeStatsWithSnapshot struct {
	*einsteindb.SnapshotRuntimeStats
}

func (e *runtimeStatsWithSnapshot) String() string {
	if e.SnapshotRuntimeStats != nil {
		return e.SnapshotRuntimeStats.String()
	}
	return ""
}

// Clone implements the RuntimeStats interface.
func (e *runtimeStatsWithSnapshot) Clone() execdetails.RuntimeStats {
	newRs := &runtimeStatsWithSnapshot{}
	if e.SnapshotRuntimeStats != nil {
		snapshotStats := e.SnapshotRuntimeStats.Clone()
		newRs.SnapshotRuntimeStats = snapshotStats.(*einsteindb.SnapshotRuntimeStats)
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (e *runtimeStatsWithSnapshot) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*runtimeStatsWithSnapshot)
	if !ok {
		return
	}
	if tmp.SnapshotRuntimeStats != nil {
		if e.SnapshotRuntimeStats == nil {
			snapshotStats := tmp.SnapshotRuntimeStats.Clone()
			e.SnapshotRuntimeStats = snapshotStats.(*einsteindb.SnapshotRuntimeStats)
			return
		}
		e.SnapshotRuntimeStats.Merge(tmp.SnapshotRuntimeStats)
	}
}

// Tp implements the RuntimeStats interface.
func (e *runtimeStatsWithSnapshot) Tp() int {
	return execdetails.TpRuntimeStatsWithSnapshot
}
