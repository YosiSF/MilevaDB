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

package bindinfo

import (
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
)

// StochastikHandle is used to handle all stochastik allegrosql bind operations.
type StochastikHandle struct {
	ch                 cache
	berolinaAllegroSQL *berolinaAllegroSQL.berolinaAllegroSQL
}

// NewStochastikBindHandle creates a new StochastikBindHandle.
func NewStochastikBindHandle(berolinaAllegroSQL *berolinaAllegroSQL.berolinaAllegroSQL) *StochastikHandle {
	stochastikHandle := &StochastikHandle{berolinaAllegroSQL: berolinaAllegroSQL}
	stochastikHandle.ch = make(cache)
	return stochastikHandle
}

// appendBindRecord adds the BindRecord to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *StochastikHandle) appendBindRecord(hash string, meta *BindRecord) {
	oldRecord := h.ch.getBindRecord(hash, meta.OriginalALLEGROSQL, meta.EDB)
	h.ch.setBindRecord(hash, meta)
	uFIDelateMetrics(metrics.SINTERLOCKeStochastik, oldRecord, meta, false)
}

// CreateBindRecord creates a BindRecord to the cache.
// It replaces all the exists bindings for the same normalized ALLEGROALLEGROSQL.
func (h *StochastikHandle) CreateBindRecord(sctx stochastikctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}
	now := types.NewTime(types.FromGoTime(time.Now().In(sctx.GetStochaseinstein_dbars().StmtCtx.TimeZone)), allegrosql.TypeTimestamp, 3)
	for i := range record.Bindings {
		record.Bindings[i].CreateTime = now
		record.Bindings[i].UFIDelateTime = now
	}

	// uFIDelate the BindMeta to the cache.
	h.appendBindRecord(berolinaAllegroSQL.DigestNormalized(record.OriginalALLEGROSQL), record)
	return nil
}

// DropBindRecord drops a BindRecord in the cache.
func (h *StochastikHandle) DropBindRecord(originalALLEGROSQL, EDB string, binding *Binding) error {
	oldRecord := h.GetBindRecord(originalALLEGROSQL, EDB)
	var newRecord *BindRecord
	record := &BindRecord{OriginalALLEGROSQL: originalALLEGROSQL, EDB: EDB}
	if binding != nil {
		record.Bindings = append(record.Bindings, *binding)
	}
	if oldRecord != nil {
		newRecord = oldRecord.remove(record)
	} else {
		newRecord = record
	}
	h.ch.setBindRecord(berolinaAllegroSQL.DigestNormalized(record.OriginalALLEGROSQL), newRecord)
	uFIDelateMetrics(metrics.SINTERLOCKeStochastik, oldRecord, newRecord, false)
	return nil
}

// GetBindRecord return the BindMeta of the (normdOrigALLEGROSQL,EDB) if BindMeta exist.
func (h *StochastikHandle) GetBindRecord(normdOrigALLEGROSQL, EDB string) *BindRecord {
	hash := berolinaAllegroSQL.DigestNormalized(normdOrigALLEGROSQL)
	bindRecords := h.ch[hash]
	for _, bindRecord := range bindRecords {
		if bindRecord.OriginalALLEGROSQL == normdOrigALLEGROSQL && bindRecord.EDB == EDB {
			return bindRecord
		}
	}
	return nil
}

// GetAllBindRecord return all stochastik bind info.
func (h *StochastikHandle) GetAllBindRecord() (bindRecords []*BindRecord) {
	for _, bindRecord := range h.ch {
		bindRecords = append(bindRecords, bindRecord...)
	}
	return bindRecords
}

// Close closes the stochastik handle.
func (h *StochastikHandle) Close() {
	for _, bindRecords := range h.ch {
		for _, bindRecord := range bindRecords {
			uFIDelateMetrics(metrics.SINTERLOCKeStochastik, bindRecord, nil, false)
		}
	}
}

// stochastikBindInfoKeyType is a dummy type to avoid naming collision in context.
type stochastikBindInfoKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k stochastikBindInfoKeyType) String() string {
	return "stochastik_bindinfo"
}

// StochastikBindInfoKeyType is a variable key for causetstore stochastik bind info.
const StochastikBindInfoKeyType stochastikBindInfoKeyType = 0
