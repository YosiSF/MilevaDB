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

package bindinfo

import (
	"time"
	"unsafe"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

const (
	// Using is the bind info's in use status.
	Using = "using"
	// deleted is the bind info's deleted status.
	deleted = "deleted"
	// Invalid is the bind info's invalid status.
	Invalid = "invalid"
	// PendingVerify means the bind info needs to be verified.
	PendingVerify = "pending verify"
	// Rejected means that the bind has been rejected after verify process.
	// We can retry it after certain time has passed.
	Rejected = "rejected"
	// Manual indicates the binding is created by ALLEGROALLEGROSQL like "create binding for ...".
	Manual = "manual"
	// Capture indicates the binding is captured by MilevaDB automatically.
	Capture = "capture"
	// Evolve indicates the binding is evolved by MilevaDB from old bindings.
	Evolve = "evolve"
)

// Binding stores the basic bind hint info.
type Binding struct {
	BindALLEGROSQL string
	// Status represents the status of the binding. It can only be one of the following values:
	// 1. deleted: BindRecord is deleted, can not be used anymore.
	// 2. using: Binding is in the normal active mode.
	Status        string
	CreateTime    types.Time
	UFIDelateTime types.Time
	Source        string
	Charset       string
	DefCauslation string
	// Hint is the parsed hints, it is used to bind hints to stmt node.
	Hint *hint.HintsSet
	// ID is the string form of Hint. It would be non-empty only when the status is `Using` or `PendingVerify`.
	ID string
}

func (b *Binding) isSame(rb *Binding) bool {
	if b.ID != "" && rb.ID != "" {
		return b.ID == rb.ID
	}
	// Sometimes we cannot construct `ID` because of the changed schemaReplicant, so we need to compare by bind allegrosql.
	return b.BindALLEGROSQL == rb.BindALLEGROSQL
}

// SinceUFIDelateTime returns the duration since last uFIDelate time. Export for test.
func (b *Binding) SinceUFIDelateTime() (time.Duration, error) {
	uFIDelateTime, err := b.UFIDelateTime.GoTime(time.Local)
	if err != nil {
		return 0, err
	}
	return time.Since(uFIDelateTime), nil
}

// cache is a k-v map, key is original allegrosql, value is a slice of BindRecord.
type cache map[string][]*BindRecord

// BindRecord represents a allegrosql bind record retrieved from the storage.
type BindRecord struct {
	OriginalALLEGROSQL string
	EDB                string

	Bindings []Binding
}

// HasUsingBinding checks if there are any using bindings in bind record.
func (br *BindRecord) HasUsingBinding() bool {
	for _, binding := range br.Bindings {
		if binding.Status == Using {
			return true
		}
	}
	return false
}

// FindBinding find bindings in BindRecord.
func (br *BindRecord) FindBinding(hint string) *Binding {
	for _, binding := range br.Bindings {
		if binding.ID == hint {
			return &binding
		}
	}
	return nil
}

// prepareHints builds ID and Hint for BindRecord. If sctx is not nil, we check if
// the BindALLEGROSQL is still valid.
func (br *BindRecord) prepareHints(sctx stochastikctx.Context) error {
	p := berolinaAllegroSQL.New()
	for i, bind := range br.Bindings {
		if (bind.Hint != nil && bind.ID != "") || bind.Status == deleted {
			continue
		}
		if sctx != nil {
			_, err := getHintsForALLEGROSQL(sctx, bind.BindALLEGROSQL)
			if err != nil {
				return err
			}
		}
		hintsSet, warns, err := hint.ParseHintsSet(p, bind.BindALLEGROSQL, bind.Charset, bind.DefCauslation, br.EDB)
		if err != nil {
			return err
		}
		hintsStr, err := hintsSet.Restore()
		if err != nil {
			return err
		}
		// For `create global binding for select * from t using select * from t`, we allow it though hintsStr is empty.
		// For `create global binding for select * from t using select /*+ non_exist_hint() */ * from t`,
		// the hint is totally invalid, we escalate warning to error.
		if hintsStr == "" && len(warns) > 0 {
			return warns[0]
		}
		br.Bindings[i].Hint = hintsSet
		br.Bindings[i].ID = hintsStr
	}
	return nil
}

// `merge` merges two BindRecord. It will replace old bindings with new bindings if there are new uFIDelates.
func merge(lBindRecord, rBindRecord *BindRecord) *BindRecord {
	if lBindRecord == nil {
		return rBindRecord
	}
	if rBindRecord == nil {
		return lBindRecord
	}
	result := lBindRecord.shallowINTERLOCKy()
	for _, rbind := range rBindRecord.Bindings {
		found := false
		for j, lbind := range lBindRecord.Bindings {
			if lbind.isSame(&rbind) {
				found = true
				if rbind.UFIDelateTime.Compare(lbind.UFIDelateTime) >= 0 {
					result.Bindings[j] = rbind
				}
				break
			}
		}
		if !found {
			result.Bindings = append(result.Bindings, rbind)
		}
	}
	return result
}

func (br *BindRecord) remove(deleted *BindRecord) *BindRecord {
	// Delete all bindings.
	if len(deleted.Bindings) == 0 {
		return &BindRecord{OriginalALLEGROSQL: br.OriginalALLEGROSQL, EDB: br.EDB}
	}
	result := br.shallowINTERLOCKy()
	for _, deletedBind := range deleted.Bindings {
		for i, bind := range result.Bindings {
			if bind.isSame(&deletedBind) {
				result.Bindings = append(result.Bindings[:i], result.Bindings[i+1:]...)
				break
			}
		}
	}
	return result
}

func (br *BindRecord) removeDeletedBindings() *BindRecord {
	result := BindRecord{OriginalALLEGROSQL: br.OriginalALLEGROSQL, EDB: br.EDB, Bindings: make([]Binding, 0, len(br.Bindings))}
	for _, binding := range br.Bindings {
		if binding.Status != deleted {
			result.Bindings = append(result.Bindings, binding)
		}
	}
	return &result
}

// shallowINTERLOCKy shallow INTERLOCKies the BindRecord.
func (br *BindRecord) shallowINTERLOCKy() *BindRecord {
	result := BindRecord{
		OriginalALLEGROSQL: br.OriginalALLEGROSQL,
		EDB:                br.EDB,
		Bindings:           make([]Binding, len(br.Bindings)),
	}
	INTERLOCKy(result.Bindings, br.Bindings)
	return &result
}

func (br *BindRecord) isSame(other *BindRecord) bool {
	return br.OriginalALLEGROSQL == other.OriginalALLEGROSQL && br.EDB == other.EDB
}

var statusIndex = map[string]int{
	Using:   0,
	deleted: 1,
	Invalid: 2,
}

func (br *BindRecord) metrics() ([]float64, []int) {
	sizes := make([]float64, len(statusIndex))
	count := make([]int, len(statusIndex))
	if br == nil {
		return sizes, count
	}
	commonLength := float64(len(br.OriginalALLEGROSQL) + len(br.EDB))
	// We treat it as deleted if there are no bindings. It could only occur in stochastik handles.
	if len(br.Bindings) == 0 {
		sizes[statusIndex[deleted]] = commonLength
		count[statusIndex[deleted]] = 1
		return sizes, count
	}
	// Make the common length counted in the first binding.
	sizes[statusIndex[br.Bindings[0].Status]] = commonLength
	for _, binding := range br.Bindings {
		sizes[statusIndex[binding.Status]] += binding.size()
		count[statusIndex[binding.Status]]++
	}
	return sizes, count
}

// size calculates the memory size of a bind info.
func (b *Binding) size() float64 {
	res := len(b.BindALLEGROSQL) + len(b.Status) + 2*int(unsafe.Sizeof(b.CreateTime)) + len(b.Charset) + len(b.DefCauslation)
	return float64(res)
}

func uFIDelateMetrics(sINTERLOCKe string, before *BindRecord, after *BindRecord, sizeOnly bool) {
	beforeSize, beforeCount := before.metrics()
	afterSize, afterCount := after.metrics()
	for status, index := range statusIndex {
		metrics.BindMemoryUsage.WithLabelValues(sINTERLOCKe, status).Add(afterSize[index] - beforeSize[index])
		if !sizeOnly {
			metrics.BindTotalGauge.WithLabelValues(sINTERLOCKe, status).Add(float64(afterCount[index] - beforeCount[index]))
		}
	}
}
