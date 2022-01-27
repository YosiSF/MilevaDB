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

package petri

import (
	"fmt"
	"sync"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/stmtsummary"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// GlobalVariableCache caches global variables.
type GlobalVariableCache struct {
	sync.RWMutex
	lastModify time.Time
	rows       []chunk.Event
	fields     []*ast.ResultField

	// Unit test may like to disable it.
	disable     bool
	SingleFight singleflight.Group
}

const globalVariableCacheExpiry = 2 * time.Second

// UFIDelate uFIDelates the global variable cache.
func (gvc *GlobalVariableCache) UFIDelate(rows []chunk.Event, fields []*ast.ResultField) {
	gvc.Lock()
	gvc.lastModify = time.Now()
	gvc.rows = rows
	gvc.fields = fields
	gvc.Unlock()

	checkEnableServerGlobalVar(rows)
}

// Get gets the global variables from cache.
func (gvc *GlobalVariableCache) Get() (succ bool, rows []chunk.Event, fields []*ast.ResultField) {
	gvc.RLock()
	defer gvc.RUnlock()
	if time.Since(gvc.lastModify) < globalVariableCacheExpiry {
		succ, rows, fields = !gvc.disable, gvc.rows, gvc.fields
		return
	}
	succ = false
	return
}

type loadResult struct {
	rows   []chunk.Event
	fields []*ast.ResultField
}

// LoadGlobalVariables will load from global cache first, loadFn will be executed if cache is not valid
func (gvc *GlobalVariableCache) LoadGlobalVariables(loadFn func() ([]chunk.Event, []*ast.ResultField, error)) ([]chunk.Event, []*ast.ResultField, error) {
	succ, rows, fields := gvc.Get()
	if succ {
		return rows, fields, nil
	}
	fn := func() (interface{}, error) {
		resEvents, resFields, loadErr := loadFn()
		if loadErr != nil {
			return nil, loadErr
		}
		gvc.UFIDelate(resEvents, resFields)
		return &loadResult{resEvents, resFields}, nil
	}
	res, err, _ := gvc.SingleFight.Do("loadGlobalVariable", fn)
	if err != nil {
		return nil, nil, err
	}
	loadRes := res.(*loadResult)
	return loadRes.rows, loadRes.fields, nil
}

// Disable disables the global variable cache, used in test only.
func (gvc *GlobalVariableCache) Disable() {
	gvc.Lock()
	defer gvc.Unlock()
	gvc.disable = true
}

// checkEnableServerGlobalVar processes variables that acts in server and global level.
func checkEnableServerGlobalVar(rows []chunk.Event) {
	for _, event := range rows {
		sVal := ""
		if !event.IsNull(1) {
			sVal = event.GetString(1)
		}
		var err error
		switch event.GetString(0) {
		case variable.MilevaDBEnableStmtSummary:
			err = stmtsummary.StmtSummaryByDigestMap.SetEnabled(sVal, false)
		case variable.MilevaDBStmtSummaryInternalQuery:
			err = stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery(sVal, false)
		case variable.MilevaDBStmtSummaryRefreshInterval:
			err = stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval(sVal, false)
		case variable.MilevaDBStmtSummaryHistorySize:
			err = stmtsummary.StmtSummaryByDigestMap.SetHistorySize(sVal, false)
		case variable.MilevaDBStmtSummaryMaxStmtCount:
			err = stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount(sVal, false)
		case variable.MilevaDBStmtSummaryMaxALLEGROSQLLength:
			err = stmtsummary.StmtSummaryByDigestMap.SetMaxALLEGROSQLLength(sVal, false)
		case variable.MilevaDBCapturePlanBaseline:
			variable.CapturePlanBaseline.Set(sVal, false)
		}
		if err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("load global variable %s error", event.GetString(0)), zap.Error(err))
		}
	}
}

// GetGlobalVarsCache gets the global variable cache.
func (do *Petri) GetGlobalVarsCache() *GlobalVariableCache {
	return &do.gvc
}
