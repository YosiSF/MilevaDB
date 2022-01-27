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

package core

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/kvcache"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	atomic2 "go.uber.org/atomic"
)

var (
	// preparedPlanCacheEnabledValue stores the global config "prepared-plan-cache-enabled".
	// If the value of "prepared-plan-cache-enabled" is true, preparedPlanCacheEnabledValue's value is 1.
	// Otherwise, preparedPlanCacheEnabledValue's value is 0.
	preparedPlanCacheEnabledValue int32
	// PreparedPlanCacheCapacity stores the global config "prepared-plan-cache-capacity".
	PreparedPlanCacheCapacity uint = 100
	// PreparedPlanCacheMemoryGuardRatio stores the global config "prepared-plan-cache-memory-guard-ratio".
	PreparedPlanCacheMemoryGuardRatio = 0.1
	// PreparedPlanCacheMaxMemory stores the max memory size defined in the global config "performance-server-memory-quota".
	PreparedPlanCacheMaxMemory = *atomic2.NewUint64(math.MaxUint64)
)

const (
	preparedPlanCacheEnabled = 1
	preparedPlanCacheUnable  = 0
)

// SetPreparedPlanCache sets isEnabled to true, then prepared plan cache is enabled.
func SetPreparedPlanCache(isEnabled bool) {
	if isEnabled {
		atomic.StoreInt32(&preparedPlanCacheEnabledValue, preparedPlanCacheEnabled)
	} else {
		atomic.StoreInt32(&preparedPlanCacheEnabledValue, preparedPlanCacheUnable)
	}
}

// PreparedPlanCacheEnabled returns whether the prepared plan cache is enabled.
func PreparedPlanCacheEnabled() bool {
	isEnabled := atomic.LoadInt32(&preparedPlanCacheEnabledValue)
	return isEnabled == preparedPlanCacheEnabled
}

type pstmtPlanCacheKey struct {
	database             string
	connID               uint64
	pstmtID              uint32
	snapshot             uint64
	schemaVersion        int64
	sqlMode              allegrosql.ALLEGROSQLMode
	timezoneOffset       int
	isolationReadEngines map[ekv.StoreType]struct{}
	selectLimit          uint64

	hash []byte
}

// Hash implements Key interface.
func (key *pstmtPlanCacheKey) Hash() []byte {
	if len(key.hash) == 0 {
		var (
			dbBytes    = replog.Slice(key.database)
			bufferSize = len(dbBytes) + 8*6 + 3*8
		)
		if key.hash == nil {
			key.hash = make([]byte, 0, bufferSize)
		}
		key.hash = append(key.hash, dbBytes...)
		key.hash = codec.EncodeInt(key.hash, int64(key.connID))
		key.hash = codec.EncodeInt(key.hash, int64(key.pstmtID))
		key.hash = codec.EncodeInt(key.hash, int64(key.snapshot))
		key.hash = codec.EncodeInt(key.hash, key.schemaVersion)
		key.hash = codec.EncodeInt(key.hash, int64(key.sqlMode))
		key.hash = codec.EncodeInt(key.hash, int64(key.timezoneOffset))
		if _, ok := key.isolationReadEngines[ekv.MilevaDB]; ok {
			key.hash = append(key.hash, ekv.MilevaDB.Name()...)
		}
		if _, ok := key.isolationReadEngines[ekv.EinsteinDB]; ok {
			key.hash = append(key.hash, ekv.EinsteinDB.Name()...)
		}
		if _, ok := key.isolationReadEngines[ekv.TiFlash]; ok {
			key.hash = append(key.hash, ekv.TiFlash.Name()...)
		}
		key.hash = codec.EncodeInt(key.hash, int64(key.selectLimit))
	}
	return key.hash
}

// SetPstmtIDSchemaVersion implements PstmtCacheKeyMutator interface to change pstmtID and schemaVersion of cacheKey.
// so we can reuse Key instead of new every time.
func SetPstmtIDSchemaVersion(key kvcache.Key, pstmtID uint32, schemaVersion int64, isolationReadEngines map[ekv.StoreType]struct{}) {
	psStmtKey, isPsStmtKey := key.(*pstmtPlanCacheKey)
	if !isPsStmtKey {
		return
	}
	psStmtKey.pstmtID = pstmtID
	psStmtKey.schemaVersion = schemaVersion
	psStmtKey.isolationReadEngines = make(map[ekv.StoreType]struct{})
	for k, v := range isolationReadEngines {
		psStmtKey.isolationReadEngines[k] = v
	}
	psStmtKey.hash = psStmtKey.hash[:0]
}

// NewPSTMTPlanCacheKey creates a new pstmtPlanCacheKey object.
func NewPSTMTPlanCacheKey(stochastikVars *variable.StochastikVars, pstmtID uint32, schemaVersion int64) kvcache.Key {
	timezoneOffset := 0
	if stochastikVars.TimeZone != nil {
		_, timezoneOffset = time.Now().In(stochastikVars.TimeZone).Zone()
	}
	key := &pstmtPlanCacheKey{
		database:             stochastikVars.CurrentDB,
		connID:               stochastikVars.ConnectionID,
		pstmtID:              pstmtID,
		snapshot:             stochastikVars.SnapshotTS,
		schemaVersion:        schemaVersion,
		sqlMode:              stochastikVars.ALLEGROSQLMode,
		timezoneOffset:       timezoneOffset,
		isolationReadEngines: make(map[ekv.StoreType]struct{}),
		selectLimit:          stochastikVars.SelectLimit,
	}
	for k, v := range stochastikVars.IsolationReadEngines {
		key.isolationReadEngines[k] = v
	}
	return key
}

// PSTMTPlanCacheValue stores the cached Statement and StmtNode.
type PSTMTPlanCacheValue struct {
	Plan              Plan
	OutPutNames       []*types.FieldName
	TblInfo2UnionScan map[*perceptron.BlockInfo]bool
}

// NewPSTMTPlanCacheValue creates a ALLEGROSQLCacheValue.
func NewPSTMTPlanCacheValue(plan Plan, names []*types.FieldName, srcMap map[*perceptron.BlockInfo]bool) *PSTMTPlanCacheValue {
	dstMap := make(map[*perceptron.BlockInfo]bool)
	for k, v := range srcMap {
		dstMap[k] = v
	}
	return &PSTMTPlanCacheValue{
		Plan:              plan,
		OutPutNames:       names,
		TblInfo2UnionScan: dstMap,
	}
}

// CachedPrepareStmt causetstore prepared ast from PrepareExec and other related fields
type CachedPrepareStmt struct {
	PreparedAst          *ast.Prepared
	VisitInfos           []visitInfo
	DeferredCausetInfos  interface{}
	Executor             interface{}
	NormalizedALLEGROSQL string
	NormalizedPlan       string
	ALLEGROSQLDigest     string
	PlanDigest           string
}
