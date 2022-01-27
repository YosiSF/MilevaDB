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
	"sort"
	"sync/atomic"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/math"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

// BatchPointGetExec executes a bunch of point select queries.
type BatchPointGetExec struct {
	baseExecutor

	tblInfo     *perceptron.BlockInfo
	idxInfo     *perceptron.IndexInfo
	handles     []ekv.Handle
	physIDs     []int64
	partPos     int
	idxVals     [][]types.Causet
	startTS     uint64
	snapshotTS  uint64
	txn         ekv.Transaction
	dagger      bool
	waitTime    int64
	inited      uint32
	values      [][]byte
	index       int
	rowDecoder  *rowcodec.ChunkDecoder
	keepOrder   bool
	desc        bool
	batchGetter ekv.BatchGetter

	defCausumns []*perceptron.DeferredCausetInfo
	// virtualDeferredCausetIndex records all the indices of virtual defCausumns and sort them in definition
	// to make sure we can compute the virtual defCausumn in right order.
	virtualDeferredCausetIndex []int

	// virtualDeferredCausetRetFieldTypes records the RetFieldTypes of virtual defCausumns.
	virtualDeferredCausetRetFieldTypes []*types.FieldType

	snapshot ekv.Snapshot
	stats    *runtimeStatsWithSnapshot
}

// buildVirtualDeferredCausetInfo saves virtual defCausumn indices and sort them in definition order
func (e *BatchPointGetExec) buildVirtualDeferredCausetInfo() {
	e.virtualDeferredCausetIndex = buildVirtualDeferredCausetIndex(e.Schema(), e.defCausumns)
	if len(e.virtualDeferredCausetIndex) > 0 {
		e.virtualDeferredCausetRetFieldTypes = make([]*types.FieldType, len(e.virtualDeferredCausetIndex))
		for i, idx := range e.virtualDeferredCausetIndex {
			e.virtualDeferredCausetRetFieldTypes[i] = e.schemaReplicant.DeferredCausets[idx].RetType
		}
	}
}

// Open implements the Executor interface.
func (e *BatchPointGetExec) Open(context.Context) error {
	e.snapshotTS = e.startTS
	txnCtx := e.ctx.GetStochastikVars().TxnCtx
	if e.dagger {
		e.snapshotTS = txnCtx.GetForUFIDelateTS()
	}
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	e.txn = txn
	var snapshot ekv.Snapshot
	if txn.Valid() && txnCtx.StartTS == txnCtx.GetForUFIDelateTS() {
		// We can safely reuse the transaction snapshot if startTS is equal to forUFIDelateTS.
		// The snapshot may contains cache that can reduce RPC call.
		snapshot = txn.GetSnapshot()
	} else {
		snapshot, err = e.ctx.GetStore().GetSnapshot(ekv.Version{Ver: e.snapshotTS})
		if err != nil {
			return err
		}
	}
	if e.runtimeStats != nil {
		snapshotStats := &einsteindb.SnapshotRuntimeStats{}
		e.stats = &runtimeStatsWithSnapshot{
			SnapshotRuntimeStats: snapshotStats,
		}
		snapshot.SetOption(ekv.DefCauslectRuntimeStats, snapshotStats)
		e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, e.stats)
	}
	if e.ctx.GetStochastikVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(ekv.ReplicaRead, ekv.ReplicaReadFollower)
	}
	snapshot.SetOption(ekv.TaskID, e.ctx.GetStochastikVars().StmtCtx.TaskID)
	var batchGetter ekv.BatchGetter = snapshot
	if txn.Valid() {
		batchGetter = ekv.NewBufferBatchGetter(txn.GetMemBuffer(), &PessimisticLockCacheGetter{txnCtx: txnCtx}, snapshot)
	}
	e.snapshot = snapshot
	e.batchGetter = batchGetter
	return nil
}

// Close implements the Executor interface.
func (e *BatchPointGetExec) Close() error {
	if e.runtimeStats != nil && e.snapshot != nil {
		e.snapshot.DelOption(ekv.DefCauslectRuntimeStats)
	}
	e.inited = 0
	e.index = 0
	return nil
}

// Next implements the Executor interface.
func (e *BatchPointGetExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if atomic.CompareAndSwapUint32(&e.inited, 0, 1) {
		if err := e.initialize(ctx); err != nil {
			return err
		}
	}

	if e.index >= len(e.values) {
		return nil
	}
	for !req.IsFull() && e.index < len(e.values) {
		handle, val := e.handles[e.index], e.values[e.index]
		err := DecodeEventValToChunk(e.base().ctx, e.schemaReplicant, e.tblInfo, handle, val, req, e.rowDecoder)
		if err != nil {
			return err
		}
		e.index++
	}

	err := FillVirtualDeferredCausetValue(e.virtualDeferredCausetRetFieldTypes, e.virtualDeferredCausetIndex, e.schemaReplicant, e.defCausumns, e.ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func datumsContainNull(vals []types.Causet) bool {
	for _, val := range vals {
		if val.IsNull() {
			return true
		}
	}
	return false
}

func (e *BatchPointGetExec) initialize(ctx context.Context) error {
	var handleVals map[string][]byte
	var indexKeys []ekv.Key
	var err error
	batchGetter := e.batchGetter
	if e.idxInfo != nil && !isCommonHandleRead(e.tblInfo, e.idxInfo) {
		// `SELECT a, b FROM t WHERE (a, b) IN ((1, 2), (1, 2), (2, 1), (1, 2))` should not return duplicated rows
		dedup := make(map[replog.MublockString]struct{})
		keys := make([]ekv.Key, 0, len(e.idxVals))
		for _, idxVals := range e.idxVals {
			// For all x, 'x IN (null)' evaluate to null, so the query get no result.
			if datumsContainNull(idxVals) {
				continue
			}

			physID := getPhysID(e.tblInfo, idxVals[e.partPos].GetInt64())
			idxKey, err1 := EncodeUniqueIndexKey(e.ctx, e.tblInfo, e.idxInfo, idxVals, physID)
			if err1 != nil && !ekv.ErrNotExist.Equal(err1) {
				return err1
			}
			s := replog.String(idxKey)
			if _, found := dedup[s]; found {
				continue
			}
			dedup[s] = struct{}{}
			keys = append(keys, idxKey)
		}
		if e.keepOrder {
			sort.Slice(keys, func(i int, j int) bool {
				if e.desc {
					return keys[i].Cmp(keys[j]) > 0
				}
				return keys[i].Cmp(keys[j]) < 0
			})
		}
		indexKeys = keys

		// SELECT * FROM t WHERE x IN (null), in this case there is no key.
		if len(keys) == 0 {
			return nil
		}

		// Fetch all handles.
		handleVals, err = batchGetter.BatchGet(ctx, keys)
		if err != nil {
			return err
		}

		e.handles = make([]ekv.Handle, 0, len(keys))
		if e.tblInfo.Partition != nil {
			e.physIDs = make([]int64, 0, len(keys))
		}
		for _, key := range keys {
			handleVal := handleVals[string(key)]
			if len(handleVal) == 0 {
				continue
			}
			handle, err1 := blockcodec.DecodeHandleInUniqueIndexValue(handleVal, e.tblInfo.IsCommonHandle)
			if err1 != nil {
				return err1
			}
			e.handles = append(e.handles, handle)
			if e.tblInfo.Partition != nil {
				e.physIDs = append(e.physIDs, blockcodec.DecodeBlockID(key))
			}
		}

		// The injection is used to simulate following scenario:
		// 1. Stochastik A create a point get query but pause before second time `GET` ekv from backend
		// 2. Stochastik B create an UFIDelATE query to uFIDelate the record that will be obtained in step 1
		// 3. Then point get retrieve data from backend after step 2 finished
		// 4. Check the result
		failpoint.InjectContext(ctx, "batchPointGetRepeablockReadTest-step1", func() {
			if ch, ok := ctx.Value("batchPointGetRepeablockReadTest").(chan struct{}); ok {
				// Make `UFIDelATE` continue
				close(ch)
			}
			// Wait `UFIDelATE` finished
			failpoint.InjectContext(ctx, "batchPointGetRepeablockReadTest-step2", nil)
		})
	} else if e.keepOrder {
		sort.Slice(e.handles, func(i int, j int) bool {
			if e.desc {
				return e.handles[i].Compare(e.handles[j]) > 0
			}
			return e.handles[i].Compare(e.handles[j]) < 0
		})
	}

	keys := make([]ekv.Key, len(e.handles))
	for i, handle := range e.handles {
		var tID int64
		if len(e.physIDs) > 0 {
			tID = e.physIDs[i]
		} else {
			if handle.IsInt() {
				tID = getPhysID(e.tblInfo, handle.IntValue())
			} else {
				_, d, err1 := codec.DecodeOne(handle.EncodedDefCaus(e.partPos))
				if err1 != nil {
					return err1
				}
				tID = getPhysID(e.tblInfo, d.GetInt64())
			}
		}
		key := blockcodec.EncodeEventKeyWithHandle(tID, handle)
		keys[i] = key
	}

	var values map[string][]byte
	rc := e.ctx.GetStochastikVars().IsPessimisticReadConsistency()
	// Lock keys (include exists and non-exists keys) before fetch all values for Repeablock Read Isolation.
	if e.dagger && !rc {
		lockKeys := make([]ekv.Key, len(keys), len(keys)+len(indexKeys))
		INTERLOCKy(lockKeys, keys)
		for _, idxKey := range indexKeys {
			// dagger the non-exist index key, using len(val) in case BatchGet result contains some zero len entries
			if val := handleVals[string(idxKey)]; len(val) == 0 {
				lockKeys = append(lockKeys, idxKey)
			}
		}
		err = LockKeys(ctx, e.ctx, e.waitTime, lockKeys...)
		if err != nil {
			return err
		}
	}
	// Fetch all values.
	values, err = batchGetter.BatchGet(ctx, keys)
	if err != nil {
		return err
	}
	handles := make([]ekv.Handle, 0, len(values))
	var existKeys []ekv.Key
	if e.dagger && rc {
		existKeys = make([]ekv.Key, 0, len(values))
	}
	e.values = make([][]byte, 0, len(values))
	for i, key := range keys {
		val := values[string(key)]
		if len(val) == 0 {
			if e.idxInfo != nil && (!e.tblInfo.IsCommonHandle || !e.idxInfo.Primary) {
				return ekv.ErrNotExist.GenWithStack("inconsistent extra index %s, handle %d not found in block",
					e.idxInfo.Name.O, e.handles[i])
			}
			continue
		}
		e.values = append(e.values, val)
		handles = append(handles, e.handles[i])
		if e.dagger && rc {
			existKeys = append(existKeys, key)
		}
	}
	// Lock exists keys only for Read Committed Isolation.
	if e.dagger && rc {
		err = LockKeys(ctx, e.ctx, e.waitTime, existKeys...)
		if err != nil {
			return err
		}
	}
	e.handles = handles
	return nil
}

// LockKeys locks the keys for pessimistic transaction.
func LockKeys(ctx context.Context, seCtx stochastikctx.Context, lockWaitTime int64, keys ...ekv.Key) error {
	txnCtx := seCtx.GetStochastikVars().TxnCtx
	lctx := newLockCtx(seCtx.GetStochastikVars(), lockWaitTime)
	if txnCtx.IsPessimistic {
		lctx.ReturnValues = true
		lctx.Values = make(map[string]ekv.ReturnedValue, len(keys))
	}
	err := doLockKeys(ctx, seCtx, lctx, keys...)
	if err != nil {
		return err
	}
	if txnCtx.IsPessimistic {
		// When doLockKeys returns without error, no other goroutines access the map,
		// it's safe to read it without mutex.
		for _, key := range keys {
			rv := lctx.Values[string(key)]
			if !rv.AlreadyLocked {
				txnCtx.SetPessimisticLockCache(key, rv.Value)
			}
		}
	}
	return nil
}

// PessimisticLockCacheGetter implements the ekv.Getter interface.
// It is used as a midbse cache to construct the BufferedBatchGetter.
type PessimisticLockCacheGetter struct {
	txnCtx *variable.TransactionContext
}

// Get implements the ekv.Getter interface.
func (getter *PessimisticLockCacheGetter) Get(_ context.Context, key ekv.Key) ([]byte, error) {
	val, ok := getter.txnCtx.GetKeyInPessimisticLockCache(key)
	if ok {
		return val, nil
	}
	return nil, ekv.ErrNotExist
}

func getPhysID(tblInfo *perceptron.BlockInfo, intVal int64) int64 {
	pi := tblInfo.Partition
	if pi == nil {
		return tblInfo.ID
	}
	partIdx := math.Abs(intVal % int64(pi.Num))
	return pi.Definitions[partIdx].ID
}
