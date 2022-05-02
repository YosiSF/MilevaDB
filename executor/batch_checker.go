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
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/stringutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
)

type keyValueWithDupInfo struct {
	newKey       ekv.Key
	dupErr       error
	commonHandle bool
}

type toBeCheckedEvent struct {
	event      []types.Causet
	handleKey  *keyValueWithDupInfo
	uniqueKeys []*keyValueWithDupInfo
	// t is the block or partition this event belongs to.
	t block.Block
}

// encodeNewEvent encodes a new event to value.
func encodeNewEvent(ctx stochastikctx.Context, t block.Block, event []types.Causet) ([]byte, error) {
	defCausIDs := make([]int64, 0, len(event))
	skimmedEvent := make([]types.Causet, 0, len(event))
	for _, defCaus := range t.DefCauss() {
		if !blocks.CanSkip(t.Meta(), defCaus, &event[defCaus.Offset]) {
			defCausIDs = append(defCausIDs, defCaus.ID)
			skimmedEvent = append(skimmedEvent, event[defCaus.Offset])
		}
	}
	sctx, rd := ctx.GetStochaseinstein_dbars().StmtCtx, &ctx.GetStochaseinstein_dbars().EventEncoder
	newEventValue, err := blockcodec.EncodeEvent(sctx, skimmedEvent, defCausIDs, nil, nil, rd)
	if err != nil {
		return nil, err
	}
	return newEventValue, nil
}

// getKeysNeedCheck gets keys converted from to-be-insert rows to record keys and unique index keys,
// which need to be checked whether they are duplicate keys.
func getKeysNeedCheck(ctx context.Context, sctx stochastikctx.Context, t block.Block, rows [][]types.Causet) ([]toBeCheckedEvent, error) {
	nUnique := 0
	for _, v := range t.WriblockIndices() {
		if v.Meta().Unique {
			nUnique++
		}
	}
	toBeCheckEvents := make([]toBeCheckedEvent, 0, len(rows))

	var handleDefCauss []*block.DeferredCauset
	// Get handle defCausumn if PK is handle.
	if t.Meta().PKIsHandle {
		for _, defCaus := range t.DefCauss() {
			if defCaus.IsPKHandleDeferredCauset(t.Meta()) {
				handleDefCauss = append(handleDefCauss, defCaus)
				break
			}
		}
	} else {
		handleDefCauss = blocks.TryGetCommonPkDeferredCausets(t)
	}

	var err error
	for _, event := range rows {
		toBeCheckEvents, err = getKeysNeedCheckOneEvent(sctx, t, event, nUnique, handleDefCauss, toBeCheckEvents)
		if err != nil {
			return nil, err
		}
	}
	return toBeCheckEvents, nil
}

func getKeysNeedCheckOneEvent(ctx stochastikctx.Context, t block.Block, event []types.Causet, nUnique int, handleDefCauss []*block.DeferredCauset, result []toBeCheckedEvent) ([]toBeCheckedEvent, error) {
	var err error
	if p, ok := t.(block.PartitionedBlock); ok {
		t, err = p.GetPartitionByEvent(ctx, event)
		if err != nil {
			return nil, err
		}
	}

	uniqueKeys := make([]*keyValueWithDupInfo, 0, nUnique)
	// Append record keys and errors.
	var handle ekv.Handle
	if t.Meta().IsCommonHandle {
		var err error
		handleOrdinals := make([]int, 0, len(handleDefCauss))
		for _, defCaus := range handleDefCauss {
			handleOrdinals = append(handleOrdinals, defCaus.Offset)
		}
		handle, err = ekv.BuildHandleFromCausetEvent(ctx.GetStochaseinstein_dbars().StmtCtx, event, handleOrdinals)
		if err != nil {
			return nil, err
		}
	} else if len(handleDefCauss) > 0 {
		handle = ekv.IntHandle(event[handleDefCauss[0].Offset].GetInt64())
	}
	var handleKey *keyValueWithDupInfo
	if handle != nil {
		fn := func() string {
			return ekv.GetDuplicateErrorHandleString(handle)
		}
		handleKey = &keyValueWithDupInfo{
			newKey: t.RecordKey(handle),
			dupErr: ekv.ErrKeyExists.FastGenByArgs(stringutil.MemoizeStr(fn), "PRIMARY"),
		}
	}

	// addChangingDefCausTimes is used to fetch values while processing "modify/change defCausumn" operation.
	addChangingDefCausTimes := 0
	// append unique keys and errors
	for _, v := range t.WriblockIndices() {
		if !v.Meta().Unique {
			continue
		}
		if t.Meta().IsCommonHandle && v.Meta().Primary {
			continue
		}
		if len(event) < len(t.WriblockDefCauss()) && addChangingDefCausTimes == 0 {
			if defCaus := blocks.FindChangingDefCaus(t.WriblockDefCauss(), v.Meta()); defCaus != nil {
				event = append(event, event[defCaus.DependencyDeferredCausetOffset])
				addChangingDefCausTimes++
			}
		}
		defCausVals, err1 := v.FetchValues(event, nil)
		if err1 != nil {
			return nil, err1
		}
		// Pass handle = 0 to GenIndexKey,
		// due to we only care about distinct key.
		key, distinct, err1 := v.GenIndexKey(ctx.GetStochaseinstein_dbars().StmtCtx,
			defCausVals, ekv.IntHandle(0), nil)
		if err1 != nil {
			return nil, err1
		}
		// Skip the non-distinct keys.
		if !distinct {
			continue
		}
		defCausValStr, err1 := types.CausetsToString(defCausVals, false)
		if err1 != nil {
			return nil, err1
		}
		uniqueKeys = append(uniqueKeys, &keyValueWithDupInfo{
			newKey:       key,
			dupErr:       ekv.ErrKeyExists.FastGenByArgs(defCausValStr, v.Meta().Name),
			commonHandle: t.Meta().IsCommonHandle,
		})
	}
	if addChangingDefCausTimes == 1 {
		event = event[:len(event)-1]
	}
	result = append(result, toBeCheckedEvent{
		event:      event,
		handleKey:  handleKey,
		uniqueKeys: uniqueKeys,
		t:          t,
	})
	return result, nil
}

// getOldEvent gets the block record event from storage for batch check.
// t could be a normal block or a partition, but it must not be a PartitionedBlock.
func getOldEvent(ctx context.Context, sctx stochastikctx.Context, txn ekv.Transaction, t block.Block, handle ekv.Handle,
	genExprs []expression.Expression) ([]types.Causet, error) {
	oldValue, err := txn.Get(ctx, t.RecordKey(handle))
	if err != nil {
		return nil, err
	}

	defcaus := t.WriblockDefCauss()
	oldEvent, oldEventMap, err := blocks.DecodeRawEventData(sctx, t.Meta(), handle, defcaus, oldValue)
	if err != nil {
		return nil, err
	}
	// Fill write-only and write-reorg defCausumns with originDefaultValue if not found in oldValue.
	gIdx := 0
	for _, defCaus := range defcaus {
		if defCaus.State != perceptron.StatePublic && oldEvent[defCaus.Offset].IsNull() {
			_, found := oldEventMap[defCaus.ID]
			if !found {
				oldEvent[defCaus.Offset], err = block.GetDefCausOriginDefaultValue(sctx, defCaus.ToInfo())
				if err != nil {
					return nil, err
				}
			}
		}
		if defCaus.IsGenerated() {
			// only the virtual defCausumn needs fill back.
			if !defCaus.GeneratedStored {
				val, err := genExprs[gIdx].Eval(chunk.MutEventFromCausets(oldEvent).ToEvent())
				if err != nil {
					return nil, err
				}
				oldEvent[defCaus.Offset], err = block.CastValue(sctx, val, defCaus.ToInfo(), false, false)
				if err != nil {
					return nil, err
				}
			}
			gIdx++
		}
	}
	return oldEvent, nil
}
