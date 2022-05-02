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
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/FIDelapi"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/set"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/diagnosticspb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/sysutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const clusterLogBatchSize = 256

type dummyCloser struct{}

func (dummyCloser) close() error { return nil }

type memBlockRetriever interface {
	retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error)
	close() error
}

// MemBlockReaderExec executes memBlock information retrieving from the MemBlock components
type MemBlockReaderExec struct {
	baseExecutor
	block     *perceptron.BlockInfo
	retriever memBlockRetriever
	// cacheRetrieved is used to indicate whether has the parent executor retrieved
	// from inspection cache in inspection mode.
	cacheRetrieved bool
}

func (e *MemBlockReaderExec) isInspectionCacheableBlock(tblName string) bool {
	switch tblName {
	case strings.ToLower(schemareplicant.BlockClusterConfig),
		strings.ToLower(schemareplicant.BlockClusterInfo),
		strings.ToLower(schemareplicant.BlockClusterSystemInfo),
		strings.ToLower(schemareplicant.BlockClusterLoad),
		strings.ToLower(schemareplicant.BlockClusterHardware):
		return true
	default:
		return false
	}
}

// Next implements the Executor Next interface.
func (e *MemBlockReaderExec) Next(ctx context.Context, req *chunk.Chunk) error {
	var (
		rows [][]types.Causet
		err  error
	)

	// The `InspectionBlockCache` will be assigned in the begin of retrieving` and be
	// cleaned at the end of retrieving, so nil represents currently in non-inspection mode.
	if cache, tbl := e.ctx.GetStochaseinstein_dbars().InspectionBlockCache, e.block.Name.L; cache != nil &&
		e.isInspectionCacheableBlock(tbl) {
		// TODO: cached rows will be returned fully, we should refactor this part.
		if !e.cacheRetrieved {
			// Obtain data from cache first.
			cached, found := cache[tbl]
			if !found {
				rows, err := e.retriever.retrieve(ctx, e.ctx)
				cached = variable.BlockSnapshot{Events: rows, Err: err}
				cache[tbl] = cached
			}
			e.cacheRetrieved = true
			rows, err = cached.Events, cached.Err
		}
	} else {
		rows, err = e.retriever.retrieve(ctx, e.ctx)
	}
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		req.Reset()
		return nil
	}

	req.GrowAndReset(len(rows))
	mublockEvent := chunk.MutEventFromTypes(retTypes(e))
	for _, event := range rows {
		mublockEvent.SetCausets(event...)
		req.AppendEvent(mublockEvent.ToEvent())
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *MemBlockReaderExec) Close() error {
	return e.retriever.close()
}

type clusterConfigRetriever struct {
	dummyCloser
	retrieved bool
	extractor *plannercore.ClusterBlockExtractor
}

// retrieve implements the memBlockRetriever interface
func (e *clusterConfigRetriever) retrieve(_ context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	e.retrieved = true
	return fetchClusterConfig(sctx, e.extractor.NodeTypes, e.extractor.Instances)
}

func fetchClusterConfig(sctx stochastikctx.Context, nodeTypes, nodeAddrs set.StringSet) ([][]types.Causet, error) {
	type result struct {
		idx  int
		rows [][]types.Causet
		err  error
	}
	serversInfo, err := schemareplicant.GetClusterServerInfo(sctx)
	failpoint.Inject("mockClusterConfigServerInfo", func(val failpoint.Value) {
		if s := val.(string); len(s) > 0 {
			// erase the error
			serversInfo, err = parseFailpointServerInfo(s), nil
		}
	})
	if err != nil {
		return nil, err
	}
	serversInfo = filterClusterServerInfo(serversInfo, nodeTypes, nodeAddrs)

	var finalEvents [][]types.Causet
	wg := sync.WaitGroup{}
	ch := make(chan result, len(serversInfo))
	for i, srv := range serversInfo {
		typ := srv.ServerType
		address := srv.Address
		statusAddr := srv.StatusAddr
		if len(statusAddr) == 0 {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(errors.Errorf("%s node %s does not contain status address", typ, address))
			continue
		}
		wg.Add(1)
		go func(index int) {
			soliton.WithRecovery(func() {
				defer wg.Done()
				var url string
				switch typ {
				case "fidel":
					url = fmt.Sprintf("%s://%s%s", soliton.InternalHTTPSchema(), statusAddr, FIDelapi.Config)
				case "einsteindb", "milevadb":
					url = fmt.Sprintf("%s://%s/config", soliton.InternalHTTPSchema(), statusAddr)
				default:
					ch <- result{err: errors.Errorf("unknown node type: %s(%s)", typ, address)}
					return
				}

				req, err := http.NewRequest(http.MethodGet, url, nil)
				if err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				req.Header.Add("FIDel-Allow-follower-handle", "true")
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
				var nested map[string]interface{}
				if err = json.NewDecoder(resp.Body).Decode(&nested); err != nil {
					ch <- result{err: errors.Trace(err)}
					return
				}
				data := config.FlattenConfigItems(nested)
				type item struct {
					key string
					val string
				}
				var items []item
				for key, val := range data {
					var str string
					switch val.(type) {
					case string: // remove quotes
						str = val.(string)
					default:
						tmp, err := json.Marshal(val)
						if err != nil {
							ch <- result{err: errors.Trace(err)}
							return
						}
						str = string(tmp)
					}
					items = append(items, item{key: key, val: str})
				}
				sort.Slice(items, func(i, j int) bool { return items[i].key < items[j].key })
				var rows [][]types.Causet
				for _, item := range items {
					rows = append(rows, types.MakeCausets(
						typ,
						address,
						item.key,
						item.val,
					))
				}
				ch <- result{idx: index, rows: rows}
			}, nil)
		}(i)
	}

	wg.Wait()
	close(ch)

	// Keep the original order to make the result more sblock
	var results []result
	for result := range ch {
		if result.err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(result.err)
			continue
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].idx < results[j].idx })
	for _, result := range results {
		finalEvents = append(finalEvents, result.rows...)
	}
	return finalEvents, nil
}

type clusterServerInfoRetriever struct {
	dummyCloser
	extractor      *plannercore.ClusterBlockExtractor
	serverInfoType diagnosticspb.ServerInfoType
	retrieved      bool
}

// retrieve implements the memBlockRetriever interface
func (e *clusterServerInfoRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	e.retrieved = true

	serversInfo, err := schemareplicant.GetClusterServerInfo(sctx)
	if err != nil {
		return nil, err
	}
	serversInfo = filterClusterServerInfo(serversInfo, e.extractor.NodeTypes, e.extractor.Instances)

	type result struct {
		idx  int
		rows [][]types.Causet
		err  error
	}
	wg := sync.WaitGroup{}
	ch := make(chan result, len(serversInfo))
	infoTp := e.serverInfoType
	finalEvents := make([][]types.Causet, 0, len(serversInfo)*10)
	for i, srv := range serversInfo {
		address := srv.Address
		remote := address
		if srv.ServerType == "milevadb" {
			remote = srv.StatusAddr
		}
		wg.Add(1)
		go func(index int, remote, address, serverTP string) {
			soliton.WithRecovery(func() {
				defer wg.Done()
				items, err := getServerInfoByGRPC(ctx, remote, infoTp)
				if err != nil {
					ch <- result{idx: index, err: err}
					return
				}
				partEvents := serverInfoItemToEvents(items, serverTP, address)
				ch <- result{idx: index, rows: partEvents}
			}, nil)
		}(i, remote, address, srv.ServerType)
	}
	wg.Wait()
	close(ch)
	// Keep the original order to make the result more sblock
	var results []result
	for result := range ch {
		if result.err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(result.err)
			continue
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].idx < results[j].idx })
	for _, result := range results {
		finalEvents = append(finalEvents, result.rows...)
	}
	return finalEvents, nil
}

func serverInfoItemToEvents(items []*diagnosticspb.ServerInfoItem, tp, addr string) [][]types.Causet {
	rows := make([][]types.Causet, 0, len(items))
	for _, v := range items {
		for _, item := range v.Pairs {
			event := types.MakeCausets(
				tp,
				addr,
				v.Tp,
				v.Name,
				item.Key,
				item.Value,
			)
			rows = append(rows, event)
		}
	}
	return rows
}

func getServerInfoByGRPC(ctx context.Context, address string, tp diagnosticspb.ServerInfoType) ([]*diagnosticspb.ServerInfoItem, error) {
	opt := grpc.WithInsecure()
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	conn, err := grpc.Dial(address, opt)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Error("close grpc connection error", zap.Error(err))
		}
	}()

	cli := diagnosticspb.NewDiagnosticsClient(conn)
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	r, err := cli.ServerInfo(ctx, &diagnosticspb.ServerInfoRequest{Tp: tp})
	if err != nil {
		return nil, err
	}
	return r.Items, nil
}

func parseFailpointServerInfo(s string) []schemareplicant.ServerInfo {
	var serversInfo []schemareplicant.ServerInfo
	servers := strings.Split(s, ";")
	for _, server := range servers {
		parts := strings.Split(server, ",")
		serversInfo = append(serversInfo, schemareplicant.ServerInfo{
			ServerType: parts[0],
			Address:    parts[1],
			StatusAddr: parts[2],
		})
	}
	return serversInfo
}

func filterClusterServerInfo(serversInfo []schemareplicant.ServerInfo, nodeTypes, addresses set.StringSet) []schemareplicant.ServerInfo {
	if len(nodeTypes) == 0 && len(addresses) == 0 {
		return serversInfo
	}

	filterServers := make([]schemareplicant.ServerInfo, 0, len(serversInfo))
	for _, srv := range serversInfo {
		// Skip some node type which has been filtered in WHERE clause
		// e.g: SELECT * FROM cluster_config WHERE type='einsteindb'
		if len(nodeTypes) > 0 && !nodeTypes.Exist(srv.ServerType) {
			continue
		}
		// Skip some node address which has been filtered in WHERE clause
		// e.g: SELECT * FROM cluster_config WHERE address='192.16.8.12:2379'
		if len(addresses) > 0 && !addresses.Exist(srv.Address) {
			continue
		}
		filterServers = append(filterServers, srv)
	}
	return filterServers
}

type clusterLogRetriever struct {
	isDrained  bool
	retrieving bool
	heap       *logResponseHeap
	extractor  *plannercore.ClusterLogBlockExtractor
	cancel     context.CancelFunc
}

type logStreamResult struct {
	// Read the next stream result while current messages is drained
	next chan logStreamResult

	addr     string
	typ      string
	messages []*diagnosticspb.LogMessage
	err      error
}

type logResponseHeap []logStreamResult

func (h logResponseHeap) Len() int {
	return len(h)
}

func (h logResponseHeap) Less(i, j int) bool {
	if lhs, rhs := h[i].messages[0].Time, h[j].messages[0].Time; lhs != rhs {
		return lhs < rhs
	}
	return h[i].typ < h[j].typ
}

func (h logResponseHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *logResponseHeap) Push(x interface{}) {
	*h = append(*h, x.(logStreamResult))
}

func (h *logResponseHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (e *clusterLogRetriever) initialize(ctx context.Context, sctx stochastikctx.Context) ([]chan logStreamResult, error) {
	serversInfo, err := schemareplicant.GetClusterServerInfo(sctx)
	failpoint.Inject("mockClusterLogServerInfo", func(val failpoint.Value) {
		// erase the error
		err = nil
		if s := val.(string); len(s) > 0 {
			serversInfo = parseFailpointServerInfo(s)
		}
	})
	if err != nil {
		return nil, err
	}

	instances := e.extractor.Instances
	nodeTypes := e.extractor.NodeTypes
	serversInfo = filterClusterServerInfo(serversInfo, nodeTypes, instances)

	var levels []diagnosticspb.LogLevel
	for l := range e.extractor.LogLevels {
		levels = append(levels, sysutil.ParseLogLevel(l))
	}

	// To avoid search log interface overload, the user should specify the time range, and at least one pattern
	// in normally ALLEGROALLEGROSQL.
	if e.extractor.StartTime == 0 {
		return nil, errors.New("denied to scan logs, please specified the start time, such as `time > '2020-01-01 00:00:00'`")
	}
	if e.extractor.EndTime == 0 {
		return nil, errors.New("denied to scan logs, please specified the end time, such as `time < '2020-01-01 00:00:00'`")
	}
	patterns := e.extractor.Patterns
	if len(patterns) == 0 && len(levels) == 0 && len(instances) == 0 && len(nodeTypes) == 0 {
		return nil, errors.New("denied to scan full logs (use `SELECT * FROM cluster_log WHERE message LIKE '%'` explicitly if intentionally)")
	}

	req := &diagnosticspb.SearchLogRequest{
		StartTime: e.extractor.StartTime,
		EndTime:   e.extractor.EndTime,
		Levels:    levels,
		Patterns:  patterns,
	}

	return e.startRetrieving(ctx, sctx, serversInfo, req)
}

func (e *clusterLogRetriever) startRetrieving(
	ctx context.Context,
	sctx stochastikctx.Context,
	serversInfo []schemareplicant.ServerInfo,
	req *diagnosticspb.SearchLogRequest) ([]chan logStreamResult, error) {
	// gRPC options
	opt := grpc.WithInsecure()
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	// The retrieve progress may be abort
	ctx, e.cancel = context.WithCancel(ctx)

	var results []chan logStreamResult
	for _, srv := range serversInfo {
		typ := srv.ServerType
		address := srv.Address
		statusAddr := srv.StatusAddr
		if len(statusAddr) == 0 {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(errors.Errorf("%s node %s does not contain status address", typ, address))
			continue
		}
		ch := make(chan logStreamResult)
		results = append(results, ch)

		go func(ch chan logStreamResult, serverType, address, statusAddr string) {
			soliton.WithRecovery(func() {
				defer close(ch)

				// The MilevaDB provides diagnostics service via status address
				remote := address
				if serverType == "milevadb" {
					remote = statusAddr
				}
				conn, err := grpc.Dial(remote, opt)
				if err != nil {
					ch <- logStreamResult{addr: address, typ: serverType, err: err}
					return
				}
				defer terror.Call(conn.Close)

				cli := diagnosticspb.NewDiagnosticsClient(conn)
				stream, err := cli.SearchLog(ctx, req)
				if err != nil {
					ch <- logStreamResult{addr: address, typ: serverType, err: err}
					return
				}

				for {
					res, err := stream.Recv()
					if err != nil && err == io.EOF {
						return
					}
					if err != nil {
						select {
						case ch <- logStreamResult{addr: address, typ: serverType, err: err}:
						case <-ctx.Done():
						}
						return
					}

					result := logStreamResult{next: ch, addr: address, typ: serverType, messages: res.Messages}
					select {
					case ch <- result:
					case <-ctx.Done():
						return
					}
				}
			}, nil)
		}(ch, typ, address, statusAddr)
	}

	return results, nil
}

func (e *clusterLogRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.extractor.SkipRequest || e.isDrained {
		return nil, nil
	}

	if !e.retrieving {
		e.retrieving = true
		results, err := e.initialize(ctx, sctx)
		if err != nil {
			e.isDrained = true
			return nil, err
		}

		// initialize the heap
		e.heap = &logResponseHeap{}
		for _, ch := range results {
			result := <-ch
			if result.err != nil || len(result.messages) == 0 {
				if result.err != nil {
					sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(result.err)
				}
				continue
			}
			*e.heap = append(*e.heap, result)
		}
		heap.Init(e.heap)
	}

	// Merge the results
	var finalEvents [][]types.Causet
	for e.heap.Len() > 0 && len(finalEvents) < clusterLogBatchSize {
		minTimeItem := heap.Pop(e.heap).(logStreamResult)
		headMessage := minTimeItem.messages[0]
		loggingTime := time.Unix(headMessage.Time/1000, (headMessage.Time%1000)*int64(time.Millisecond))
		finalEvents = append(finalEvents, types.MakeCausets(
			loggingTime.Format("2006/01/02 15:04:05.000"),
			minTimeItem.typ,
			minTimeItem.addr,
			strings.ToUpper(headMessage.Level.String()),
			headMessage.Message,
		))
		minTimeItem.messages = minTimeItem.messages[1:]
		// Current streaming result is drained, read the next to supply.
		if len(minTimeItem.messages) == 0 {
			result := <-minTimeItem.next
			if result.err != nil {
				sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(result.err)
				continue
			}
			if len(result.messages) > 0 {
				heap.Push(e.heap, result)
			}
		} else {
			heap.Push(e.heap, minTimeItem)
		}
	}

	// All streams are drained
	e.isDrained = e.heap.Len() == 0

	return finalEvents, nil
}

func (e *clusterLogRetriever) close() error {
	if e.cancel != nil {
		e.cancel()
	}
	return nil
}
