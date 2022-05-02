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

package petri

import (
	"sort"
	"sync"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"go.uber.org/zap"
)

type checkResult int

const (
	// ResultSucc means schemaValidator's check is passing.
	ResultSucc checkResult = iota
	// ResultFail means schemaValidator's check is fail.
	ResultFail
	// ResultUnknown means schemaValidator doesn't know the check would be success or fail.
	ResultUnknown
)

// SchemaValidator is the interface for checking the validity of schemaReplicant version.
type SchemaValidator interface {
	// UFIDelate the schemaReplicant validator, add a new item, delete the expired deltaSchemaInfos.
	// The latest schemaVer is valid within leaseGrantTime plus lease duration.
	// Add the changed block IDs to the new schemaReplicant information,
	// which is produced when the oldSchemaVer is uFIDelated to the newSchemaVer.
	UFIDelate(leaseGrantTime uint64, oldSchemaVer, newSchemaVer int64, change *einsteindb.RelatedSchemaChange)
	// Check is it valid for a transaction to use schemaVer and related blocks, at timestamp txnTS.
	Check(txnTS uint64, schemaVer int64, relatedPhysicalBlockIDs []int64) (*einsteindb.RelatedSchemaChange, checkResult)
	// Stop stops checking the valid of transaction.
	Stop()
	// Restart restarts the schemaReplicant validator after it is stopped.
	Restart()
	// Reset resets SchemaValidator to initial state.
	Reset()
	// IsStarted indicates whether SchemaValidator is started.
	IsStarted() bool
}

type deltaSchemaInfo struct {
	schemaVersion        int64
	relatedIDs           []int64
	relatedCausetActions []uint64
}

type schemaValidator struct {
	isStarted             bool
	mux                   sync.RWMutex
	lease                 time.Duration
	latestSchemaVer       int64
	latestSchemaReplicant schemareplicant.SchemaReplicant
	do                    *Petri
	latestSchemaExpire    time.Time
	// deltaSchemaInfos is a queue that maintain the history of changes.
	deltaSchemaInfos []deltaSchemaInfo
}

// NewSchemaValidator returns a SchemaValidator structure.
func NewSchemaValidator(lease time.Duration, do *Petri) SchemaValidator {
	return &schemaValidator{
		isStarted:        true,
		lease:            lease,
		deltaSchemaInfos: make([]deltaSchemaInfo, 0, variable.DefMilevaDBMaxDeltaSchemaCount),
		do:               do,
	}
}

func (s *schemaValidator) IsStarted() bool {
	s.mux.RLock()
	isStarted := s.isStarted
	s.mux.RUnlock()
	return isStarted
}

func (s *schemaValidator) LatestSchemaVersion() int64 {
	s.mux.RLock()
	latestSchemaVer := s.latestSchemaVer
	s.mux.RUnlock()
	return latestSchemaVer
}

func (s *schemaValidator) Stop() {
	logutil.BgLogger().Info("the schemaReplicant validator stops")
	metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorStop).Inc()
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = false
	s.latestSchemaVer = 0
	s.deltaSchemaInfos = s.deltaSchemaInfos[:0]
}

func (s *schemaValidator) Restart() {
	metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorRestart).Inc()
	logutil.BgLogger().Info("the schemaReplicant validator restarts")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = true
}

func (s *schemaValidator) Reset() {
	metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorReset).Inc()
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = true
	s.latestSchemaVer = 0
	s.deltaSchemaInfos = s.deltaSchemaInfos[:0]
}

func (s *schemaValidator) UFIDelate(leaseGrantTS uint64, oldVer, currVer int64, change *einsteindb.RelatedSchemaChange) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if !s.isStarted {
		logutil.BgLogger().Info("the schemaReplicant validator stopped before uFIDelating")
		return
	}

	// Renew the lease.
	s.latestSchemaVer = currVer
	if s.do != nil {
		s.latestSchemaReplicant = s.do.SchemaReplicant()
	}
	leaseGrantTime := oracle.GetTimeFromTS(leaseGrantTS)
	leaseExpire := leaseGrantTime.Add(s.lease - time.Millisecond)
	s.latestSchemaExpire = leaseExpire

	// UFIDelate the schemaReplicant deltaItem information.
	if currVer != oldVer {
		s.enqueue(currVer, change)
		var tblIDs []int64
		var actionTypes []uint64
		if change != nil {
			tblIDs = change.PhyTblIDS
			actionTypes = change.CausetActionTypes
		}
		logutil.BgLogger().Debug("uFIDelate schemaReplicant validator", zap.Int64("oldVer", oldVer),
			zap.Int64("currVer", currVer), zap.Int64s("changedBlockIDs", tblIDs), zap.Uint64s("changedCausetActionTypes", actionTypes))
	}
}

// isRelatedBlocksChanged returns the result whether relatedBlockIDs is changed
// from usedVer to the latest schemaReplicant version.
// NOTE, this function should be called under dagger!
func (s *schemaValidator) isRelatedBlocksChanged(currVer int64, blockIDs []int64) (einsteindb.RelatedSchemaChange, bool) {
	res := einsteindb.RelatedSchemaChange{}
	if len(s.deltaSchemaInfos) == 0 {
		metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorCacheEmpty).Inc()
		logutil.BgLogger().Info("schemaReplicant change history is empty", zap.Int64("currVer", currVer))
		return res, true
	}
	newerDeltas := s.findNewerDeltas(currVer)
	if len(newerDeltas) == len(s.deltaSchemaInfos) {
		metrics.LoadSchemaCounter.WithLabelValues(metrics.SchemaValidatorCacheMiss).Inc()
		logutil.BgLogger().Info("the schemaReplicant version is much older than the latest version", zap.Int64("currVer", currVer),
			zap.Int64("latestSchemaVer", s.latestSchemaVer), zap.Reflect("deltas", newerDeltas))
		return res, true
	}

	changedTblMap := make(map[int64]uint64)
	for _, item := range newerDeltas {
		for i, tblID := range item.relatedIDs {
			for _, relatedTblID := range blockIDs {
				if tblID == relatedTblID {
					changedTblMap[tblID] |= item.relatedCausetActions[i]
				}
			}
		}
	}
	if len(changedTblMap) > 0 {
		tblIds := make([]int64, 0, len(changedTblMap))
		actionTypes := make([]uint64, 0, len(changedTblMap))
		for id := range changedTblMap {
			tblIds = append(tblIds, id)
		}
		sort.Slice(tblIds, func(i, j int) bool { return tblIds[i] < tblIds[j] })
		for _, tblID := range tblIds {
			actionTypes = append(actionTypes, changedTblMap[tblID])
		}
		res.PhyTblIDS = tblIds
		res.CausetActionTypes = actionTypes
		res.Amendable = true
		return res, true
	}
	return res, false
}

func (s *schemaValidator) findNewerDeltas(currVer int64) []deltaSchemaInfo {
	q := s.deltaSchemaInfos
	pos := len(q)
	for i := len(q) - 1; i >= 0 && q[i].schemaVersion > currVer; i-- {
		pos = i
	}
	return q[pos:]
}

// Check checks schemaReplicant validity, returns true if use schemaVer and related blocks at txnTS is legal.
func (s *schemaValidator) Check(txnTS uint64, schemaVer int64, relatedPhysicalBlockIDs []int64) (*einsteindb.RelatedSchemaChange, checkResult) {
	s.mux.RLock()
	defer s.mux.RUnlock()
	if !s.isStarted {
		logutil.BgLogger().Info("the schemaReplicant validator stopped before checking")
		return nil, ResultUnknown
	}
	if s.lease == 0 {
		return nil, ResultSucc
	}

	// Schema changed, result decided by whether related blocks change.
	if schemaVer < s.latestSchemaVer {
		// The DBS relatedPhysicalBlockIDs is empty.
		if len(relatedPhysicalBlockIDs) == 0 {
			logutil.BgLogger().Info("the related physical block ID is empty", zap.Int64("schemaVer", schemaVer),
				zap.Int64("latestSchemaVer", s.latestSchemaVer))
			return nil, ResultFail
		}

		relatedChanges, changed := s.isRelatedBlocksChanged(schemaVer, relatedPhysicalBlockIDs)
		if changed {
			if relatedChanges.Amendable {
				relatedChanges.LatestSchemaReplicant = s.latestSchemaReplicant
				return &relatedChanges, ResultFail
			}
			return nil, ResultFail
		}
		return nil, ResultSucc
	}

	// Schema unchanged, maybe success or the schemaReplicant validator is unavailable.
	t := oracle.GetTimeFromTS(txnTS)
	if t.After(s.latestSchemaExpire) {
		return nil, ResultUnknown
	}
	return nil, ResultSucc
}

func (s *schemaValidator) enqueue(schemaVersion int64, change *einsteindb.RelatedSchemaChange) {
	maxCnt := int(variable.GetMaxDeltaSchemaCount())
	if maxCnt <= 0 {
		logutil.BgLogger().Info("the schemaReplicant validator enqueue", zap.Int("delta max count", maxCnt))
		return
	}

	delta := deltaSchemaInfo{schemaVersion, []int64{}, []uint64{}}
	if change != nil {
		delta.relatedIDs = change.PhyTblIDS
		delta.relatedCausetActions = change.CausetActionTypes
	}
	if len(s.deltaSchemaInfos) == 0 {
		s.deltaSchemaInfos = append(s.deltaSchemaInfos, delta)
		return
	}

	lastOffset := len(s.deltaSchemaInfos) - 1
	// The first item we needn't to merge, because we hope to cover more versions.
	if lastOffset != 0 && containIn(s.deltaSchemaInfos[lastOffset], delta) {
		s.deltaSchemaInfos[lastOffset] = delta
	} else {
		s.deltaSchemaInfos = append(s.deltaSchemaInfos, delta)
	}

	if len(s.deltaSchemaInfos) > maxCnt {
		logutil.BgLogger().Info("the schemaReplicant validator enqueue, queue is too long",
			zap.Int("delta max count", maxCnt), zap.Int64("remove schemaReplicant version", s.deltaSchemaInfos[0].schemaVersion))
		s.deltaSchemaInfos = s.deltaSchemaInfos[1:]
	}
}

// containIn is checks if lasteDelta is included in curDelta considering block id and action type.
func containIn(lastDelta, curDelta deltaSchemaInfo) bool {
	if len(lastDelta.relatedIDs) > len(curDelta.relatedIDs) {
		return false
	}

	var isEqual bool
	for i, lastTblID := range lastDelta.relatedIDs {
		isEqual = false
		for j, curTblID := range curDelta.relatedIDs {
			if lastTblID == curTblID && lastDelta.relatedCausetActions[i] == curDelta.relatedCausetActions[j] {
				isEqual = true
				break
			}
		}
		if !isEqual {
			return false
		}
	}

	return true
}
