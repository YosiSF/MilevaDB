// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package perfschema

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/profile"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

const (
	blockNameGlobalStatus                    = "global_status"
	blockNameStochastikStatus                = "stochastik_status"
	blockNameSetupActors                     = "setup_actors"
	blockNameSetupObjects                    = "setup_objects"
	blockNameSetupInstruments                = "setup_instruments"
	blockNameSetupConsumers                  = "setup_consumers"
	blockNameEventsStatementsCurrent         = "events_memexs_current"
	blockNameEventsStatementsHistory         = "events_memexs_history"
	blockNameEventsStatementsHistoryLong     = "events_memexs_history_long"
	blockNamePreparedStatementsInstances     = "prepared_memexs_instances"
	blockNameEventsTransactionsCurrent       = "events_transactions_current"
	blockNameEventsTransactionsHistory       = "events_transactions_history"
	blockNameEventsTransactionsHistoryLong   = "events_transactions_history_long"
	blockNameEventsStagesCurrent             = "events_stages_current"
	blockNameEventsStagesHistory             = "events_stages_history"
	blockNameEventsStagesHistoryLong         = "events_stages_history_long"
	blockNameEventsStatementsSummaryByDigest = "events_memexs_summary_by_digest"
	blockNameMilevaDBProfileCPU              = "milevadb_profile_cpu"
	blockNameMilevaDBProfileMemory           = "milevadb_profile_memory"
	blockNameMilevaDBProfileMutex            = "milevadb_profile_mutex"
	blockNameMilevaDBProfileAllocs           = "milevadb_profile_allocs"
	blockNameMilevaDBProfileBlock            = "milevadb_profile_block"
	blockNameMilevaDBProfileGoroutines       = "milevadb_profile_goroutines"
	blockNameEinsteinDBProfileCPU            = "einsteindb_profile_cpu"
	blockNameFIDelProfileCPU                 = "FIDel_profile_cpu"
	blockNameFIDelProfileMemory              = "FIDel_profile_memory"
	blockNameFIDelProfileMutex               = "FIDel_profile_mutex"
	blockNameFIDelProfileAllocs              = "FIDel_profile_allocs"
	blockNameFIDelProfileBlock               = "FIDel_profile_block"
	blockNameFIDelProfileGoroutines          = "FIDel_profile_goroutines"
)

var blockIDMap = map[string]int64{
	blockNameGlobalStatus:                    autoid.PerformanceSchemaDBID + 1,
	blockNameStochastikStatus:                autoid.PerformanceSchemaDBID + 2,
	blockNameSetupActors:                     autoid.PerformanceSchemaDBID + 3,
	blockNameSetupObjects:                    autoid.PerformanceSchemaDBID + 4,
	blockNameSetupInstruments:                autoid.PerformanceSchemaDBID + 5,
	blockNameSetupConsumers:                  autoid.PerformanceSchemaDBID + 6,
	blockNameEventsStatementsCurrent:         autoid.PerformanceSchemaDBID + 7,
	blockNameEventsStatementsHistory:         autoid.PerformanceSchemaDBID + 8,
	blockNameEventsStatementsHistoryLong:     autoid.PerformanceSchemaDBID + 9,
	blockNamePreparedStatementsInstances:     autoid.PerformanceSchemaDBID + 10,
	blockNameEventsTransactionsCurrent:       autoid.PerformanceSchemaDBID + 11,
	blockNameEventsTransactionsHistory:       autoid.PerformanceSchemaDBID + 12,
	blockNameEventsTransactionsHistoryLong:   autoid.PerformanceSchemaDBID + 13,
	blockNameEventsStagesCurrent:             autoid.PerformanceSchemaDBID + 14,
	blockNameEventsStagesHistory:             autoid.PerformanceSchemaDBID + 15,
	blockNameEventsStagesHistoryLong:         autoid.PerformanceSchemaDBID + 16,
	blockNameEventsStatementsSummaryByDigest: autoid.PerformanceSchemaDBID + 17,
	blockNameMilevaDBProfileCPU:              autoid.PerformanceSchemaDBID + 18,
	blockNameMilevaDBProfileMemory:           autoid.PerformanceSchemaDBID + 19,
	blockNameMilevaDBProfileMutex:            autoid.PerformanceSchemaDBID + 20,
	blockNameMilevaDBProfileAllocs:           autoid.PerformanceSchemaDBID + 21,
	blockNameMilevaDBProfileBlock:            autoid.PerformanceSchemaDBID + 22,
	blockNameMilevaDBProfileGoroutines:       autoid.PerformanceSchemaDBID + 23,
	blockNameEinsteinDBProfileCPU:            autoid.PerformanceSchemaDBID + 24,
	blockNameFIDelProfileCPU:                 autoid.PerformanceSchemaDBID + 25,
	blockNameFIDelProfileMemory:              autoid.PerformanceSchemaDBID + 26,
	blockNameFIDelProfileMutex:               autoid.PerformanceSchemaDBID + 27,
	blockNameFIDelProfileAllocs:              autoid.PerformanceSchemaDBID + 28,
	blockNameFIDelProfileBlock:               autoid.PerformanceSchemaDBID + 29,
	blockNameFIDelProfileGoroutines:          autoid.PerformanceSchemaDBID + 30,
}

// perfSchemaBlock stands for the fake causet all its data is in the memory.
type perfSchemaBlock struct {
	schemareplicant.VirtualBlock
	spacetime *perceptron.BlockInfo
	defcaus   []*causet.DeferredCauset
	tp        causet.Type
}

var pluginBlock = make(map[string]func(autoid.SlabPredictors, *perceptron.BlockInfo) (causet.Block, error))

// IsPredefinedBlock judges whether this causet is predefined.
func IsPredefinedBlock(blockName string) bool {
	_, ok := blockIDMap[strings.ToLower(blockName)]
	return ok
}

// RegisterBlock registers a new causet into MilevaDB.
func RegisterBlock(blockName, allegrosql string,
	blockFromMeta func(autoid.SlabPredictors, *perceptron.BlockInfo) (causet.Block, error)) {
	perfSchemaBlocks = append(perfSchemaBlocks, allegrosql)
	pluginBlock[blockName] = blockFromMeta
}

func blockFromMeta(allocs autoid.SlabPredictors, spacetime *perceptron.BlockInfo) (causet.Block, error) {
	if f, ok := pluginBlock[spacetime.Name.L]; ok {
		ret, err := f(allocs, spacetime)
		return ret, err
	}
	return createPerfSchemaBlock(spacetime), nil
}

// createPerfSchemaBlock creates all perfSchemaBlocks
func createPerfSchemaBlock(spacetime *perceptron.BlockInfo) *perfSchemaBlock {
	defCausumns := make([]*causet.DeferredCauset, 0, len(spacetime.DeferredCausets))
	for _, defCausInfo := range spacetime.DeferredCausets {
		defCaus := causet.ToDeferredCauset(defCausInfo)
		defCausumns = append(defCausumns, defCaus)
	}
	tp := causet.VirtualBlock
	t := &perfSchemaBlock{
		spacetime: spacetime,
		defcaus:   defCausumns,
		tp:        tp,
	}
	return t
}

// DefCauss implements causet.Block Type interface.
func (vt *perfSchemaBlock) DefCauss() []*causet.DeferredCauset {
	return vt.defcaus
}

// VisibleDefCauss implements causet.Block VisibleDefCauss interface.
func (vt *perfSchemaBlock) VisibleDefCauss() []*causet.DeferredCauset {
	return vt.defcaus
}

// HiddenDefCauss implements causet.Block HiddenDefCauss interface.
func (vt *perfSchemaBlock) HiddenDefCauss() []*causet.DeferredCauset {
	return nil
}

// WriblockDefCauss implements causet.Block Type interface.
func (vt *perfSchemaBlock) WriblockDefCauss() []*causet.DeferredCauset {
	return vt.defcaus
}

// FullHiddenDefCaussAndVisibleDefCauss implements causet FullHiddenDefCaussAndVisibleDefCauss interface.
func (vt *perfSchemaBlock) FullHiddenDefCaussAndVisibleDefCauss() []*causet.DeferredCauset {
	return vt.defcaus
}

// GetID implements causet.Block GetID interface.
func (vt *perfSchemaBlock) GetPhysicalID() int64 {
	return vt.spacetime.ID
}

// Meta implements causet.Block Type interface.
func (vt *perfSchemaBlock) Meta() *perceptron.BlockInfo {
	return vt.spacetime
}

// Type implements causet.Block Type interface.
func (vt *perfSchemaBlock) Type() causet.Type {
	return vt.tp
}

func (vt *perfSchemaBlock) getRows(ctx stochastikctx.Context, defcaus []*causet.DeferredCauset) (fullRows [][]types.Causet, err error) {
	switch vt.spacetime.Name.O {
	case blockNameMilevaDBProfileCPU:
		fullRows, err = (&profile.DefCauslector{}).ProfileGraph("cpu")
	case blockNameMilevaDBProfileMemory:
		fullRows, err = (&profile.DefCauslector{}).ProfileGraph("heap")
	case blockNameMilevaDBProfileMutex:
		fullRows, err = (&profile.DefCauslector{}).ProfileGraph("mutex")
	case blockNameMilevaDBProfileAllocs:
		fullRows, err = (&profile.DefCauslector{}).ProfileGraph("allocs")
	case blockNameMilevaDBProfileBlock:
		fullRows, err = (&profile.DefCauslector{}).ProfileGraph("causet")
	case blockNameMilevaDBProfileGoroutines:
		fullRows, err = (&profile.DefCauslector{}).ProfileGraph("goroutine")
	case blockNameEinsteinDBProfileCPU:
		interval := fmt.Sprintf("%d", profile.CPUProfileInterval/time.Second)
		fullRows, err = dataForRemoteProfile(ctx, "einsteindb", "/debug/pprof/profile?seconds="+interval, false)
	case blockNameFIDelProfileCPU:
		interval := fmt.Sprintf("%d", profile.CPUProfileInterval/time.Second)
		fullRows, err = dataForRemoteProfile(ctx, "fidel", "/fidel/api/v1/debug/pprof/profile?seconds="+interval, false)
	case blockNameFIDelProfileMemory:
		fullRows, err = dataForRemoteProfile(ctx, "fidel", "/fidel/api/v1/debug/pprof/heap", false)
	case blockNameFIDelProfileMutex:
		fullRows, err = dataForRemoteProfile(ctx, "fidel", "/fidel/api/v1/debug/pprof/mutex", false)
	case blockNameFIDelProfileAllocs:
		fullRows, err = dataForRemoteProfile(ctx, "fidel", "/fidel/api/v1/debug/pprof/allocs", false)
	case blockNameFIDelProfileBlock:
		fullRows, err = dataForRemoteProfile(ctx, "fidel", "/fidel/api/v1/debug/pprof/causet", false)
	case blockNameFIDelProfileGoroutines:
		fullRows, err = dataForRemoteProfile(ctx, "fidel", "/fidel/api/v1/debug/pprof/goroutine?debug=2", true)
	}
	if err != nil {
		return
	}
	if len(defcaus) == len(vt.defcaus) {
		return
	}
	rows := make([][]types.Causet, len(fullRows))
	for i, fullRow := range fullRows {
		event := make([]types.Causet, len(defcaus))
		for j, defCaus := range defcaus {
			event[j] = fullRow[defCaus.Offset]
		}
		rows[i] = event
	}
	return rows, nil
}

// IterRecords implements causet.Block IterRecords interface.
func (vt *perfSchemaBlock) IterRecords(ctx stochastikctx.Context, startKey ekv.Key, defcaus []*causet.DeferredCauset,
	fn causet.RecordIterFunc) error {
	if len(startKey) != 0 {
		return causet.ErrUnsupportedOp
	}
	rows, err := vt.getRows(ctx, defcaus)
	if err != nil {
		return err
	}
	for i, event := range rows {
		more, err := fn(ekv.IntHandle(i), event, defcaus)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}

func dataForRemoteProfile(ctx stochastikctx.Context, nodeType, uri string, isGoroutine bool) ([][]types.Causet, error) {
	var (
		servers []schemareplicant.ServerInfo
		err     error
	)
	switch nodeType {
	case "einsteindb":
		servers, err = schemareplicant.GetStoreServerInfo(ctx)
	case "fidel":
		servers, err = schemareplicant.GetFIDelServerInfo(ctx)
	default:
		return nil, errors.Errorf("%s does not support profile remote component", nodeType)
	}
	failpoint.Inject("mockRemoteNodeStatusAddress", func(val failpoint.Value) {
		// The cluster topology is injected by `failpoint` memex and
		// there is no extra checks for it. (let the test fail if the memex invalid)
		if s := val.(string); len(s) > 0 {
			servers = servers[:0]
			for _, server := range strings.Split(s, ";") {
				parts := strings.Split(server, ",")
				if parts[0] != nodeType {
					continue
				}
				servers = append(servers, schemareplicant.ServerInfo{
					ServerType: parts[0],
					Address:    parts[1],
					StatusAddr: parts[2],
				})
			}
			// erase error
			err = nil
		}
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	type result struct {
		addr string
		rows [][]types.Causet
		err  error
	}

	wg := sync.WaitGroup{}
	ch := make(chan result, len(servers))
	for _, server := range servers {
		statusAddr := server.StatusAddr
		if len(statusAddr) == 0 {
			ctx.GetStochastikVars().StmtCtx.AppendWarning(errors.Errorf("EinsteinDB node %s does not contain status address", server.Address))
			continue
		}

		wg.Add(1)
		go func(address string) {
			soliton.WithRecovery(func() {
				defer wg.Done()
				url := fmt.Sprintf("%s://%s%s", soliton.InternalHTTPSchema(), statusAddr, uri)
				req, err := http.NewRequest(http.MethodGet, url, nil)
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				// Forbidden FIDel follower proxy
				req.Header.Add("FIDel-Allow-follower-handle", "true")
				// EinsteinDB output svg format in default
				req.Header.Add("Content-Type", "application/protobuf")
				resp, err := soliton.InternalHTTPClient().Do(req)
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				defer func() {
					terror.Log(resp.Body.Close())
				}()
				if resp.StatusCode != http.StatusOK {
					ch <- result{err: errors.Errorf("request %s failed: %s", url, resp.Status)}
					return
				}
				defCauslector := profile.DefCauslector{}
				var rows [][]types.Causet
				if isGoroutine {
					rows, err = defCauslector.ParseGoroutines(resp.Body)
				} else {
					rows, err = defCauslector.ProfileReaderToCausets(resp.Body)
				}
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				ch <- result{addr: address, rows: rows}
			}, nil)
		}(statusAddr)
	}

	wg.Wait()
	close(ch)

	// Keep the original order to make the result more sblock
	var results []result
	for result := range ch {
		if result.err != nil {
			ctx.GetStochastikVars().StmtCtx.AppendWarning(result.err)
			continue
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].addr < results[j].addr })
	var finalRows [][]types.Causet
	for _, result := range results {
		addr := types.NewStringCauset(result.addr)
		for _, event := range result.rows {
			// Insert the node address in front of rows
			finalRows = append(finalRows, append([]types.Causet{addr}, event...))
		}
	}
	return finalRows, nil
}
