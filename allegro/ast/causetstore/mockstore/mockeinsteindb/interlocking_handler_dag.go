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

package mockeinsteindb

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression/aggregation"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/collate"
	mockpkg "github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/rowcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/timeutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
	"github.com/whtcorpsinc/ekvproto/pkg/errorpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var dummySlice = make([]byte, 0)

type posetPosetDagContext struct {
	posetPosetDagReq *fidelpb.PosetDagRequest
	keyRanges        []*interlock.KeyRange
	startTS          uint64
	evalCtx          *evalContext
}

func (h *rpcHandler) handleCoFIDelAGRequest(req *interlock.Request) *interlock.Response {
	resp := &interlock.Response{}
	posetPosetDagCtx, e, posetPosetDagReq, err := h.buildPosetDagExecutor(req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}

	var rows [][][]byte
	ctx := context.TODO()
	for {
		var event [][]byte
		event, err = e.Next(ctx)
		if err != nil {
			break
		}
		if event == nil {
			break
		}
		rows = append(rows, event)
	}

	var execDetails []*execDetail
	if posetPosetDagReq.DefCauslectExecutionSummaries != nil && *posetPosetDagReq.DefCauslectExecutionSummaries {
		execDetails = e.ExecDetails()
	}

	selResp := h.initSelectResponse(err, posetPosetDagCtx.evalCtx.sc.GetWarnings(), e.Counts())
	if err == nil {
		err = h.fillUFIDelata4SelectResponse(selResp, posetPosetDagReq, posetPosetDagCtx, rows)
	}
	return buildResp(selResp, execDetails, err)
}

func (h *rpcHandler) buildPosetDagExecutor(req *interlock.Request) (*posetPosetDagContext, executor, *fidelpb.PosetDagRequest, error) {
	if len(req.Ranges) == 0 {
		return nil, nil, nil, errors.New("request range is null")
	}
	if req.GetTp() != ekv.ReqTypePosetDag {
		return nil, nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}

	posetPosetDagReq := new(fidelpb.PosetDagRequest)
	err := proto.Unmarshal(req.Data, posetPosetDagReq)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	sc := flagsToStatementContext(posetPosetDagReq.Flags)
	sc.TimeZone, err = constructTimeZone(posetPosetDagReq.TimeZoneName, int(posetPosetDagReq.TimeZoneOffset))
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	ctx := &posetPosetDagContext{
		posetPosetDagReq: posetPosetDagReq,
		keyRanges:        req.Ranges,
		startTS:          req.StartTs,
		evalCtx:          &evalContext{sc: sc},
	}
	var e executor
	if len(posetPosetDagReq.Executors) == 0 {
		e, err = h.buildPosetDagForTiFlash(ctx, posetPosetDagReq.RootExecutor)
	} else {
		e, err = h.buildPosetDag(ctx, posetPosetDagReq.Executors)
	}
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	return ctx, e, posetPosetDagReq, err
}

// constructTimeZone constructs timezone by name first. When the timezone name
// is set, the daylight saving problem must be considered. Otherwise the
// timezone offset in seconds east of UTC is used to constructed the timezone.
func constructTimeZone(name string, offset int) (*time.Location, error) {
	return timeutil.ConstructTimeZone(name, offset)
}

func (h *rpcHandler) handleINTERLOCKStream(ctx context.Context, req *interlock.Request) (einsteindbpb.EinsteinDB_interlocking_directorateStreamClient, error) {
	posetPosetDagCtx, e, posetPosetDagReq, err := h.buildPosetDagExecutor(req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &mockINTERLOCKStreamClient{
		exec:             e,
		req:              posetPosetDagReq,
		ctx:              ctx,
		posetPosetDagCtx: posetPosetDagCtx,
	}, nil
}

func (h *rpcHandler) buildExec(ctx *posetPosetDagContext, curr *fidelpb.Executor) (executor, *fidelpb.Executor, error) {
	var currExec executor
	var err error
	var childExec *fidelpb.Executor
	switch curr.GetTp() {
	case fidelpb.ExecType_TypeTableScan:
		currExec, err = h.buildTableScan(ctx, curr)
	case fidelpb.ExecType_TypeIndexScan:
		currExec, err = h.buildIndexScan(ctx, curr)
	case fidelpb.ExecType_TypeSelection:
		currExec, err = h.buildSelection(ctx, curr)
		childExec = curr.Selection.Child
	case fidelpb.ExecType_TypeAggregation:
		currExec, err = h.buildHashAgg(ctx, curr)
		childExec = curr.Aggregation.Child
	case fidelpb.ExecType_TypeStreamAgg:
		currExec, err = h.buildStreamAgg(ctx, curr)
		childExec = curr.Aggregation.Child
	case fidelpb.ExecType_TypeTopN:
		currExec, err = h.buildTopN(ctx, curr)
		childExec = curr.TopN.Child
	case fidelpb.ExecType_TypeLimit:
		currExec = &limitExec{limit: curr.Limit.GetLimit(), execDetail: new(execDetail)}
		childExec = curr.Limit.Child
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", curr.GetTp())
	}

	return currExec, childExec, errors.Trace(err)
}

func (h *rpcHandler) buildPosetDagForTiFlash(ctx *posetPosetDagContext, farther *fidelpb.Executor) (executor, error) {
	curr, child, err := h.buildExec(ctx, farther)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if child != nil {
		childExec, err := h.buildPosetDagForTiFlash(ctx, child)
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetSrcExec(childExec)
	}
	return curr, nil
}

func (h *rpcHandler) buildPosetDag(ctx *posetPosetDagContext, executors []*fidelpb.Executor) (executor, error) {
	var src executor
	for i := 0; i < len(executors); i++ {
		curr, _, err := h.buildExec(ctx, executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetSrcExec(src)
		src = curr
	}
	return src, nil
}

func (h *rpcHandler) buildTableScan(ctx *posetPosetDagContext, executor *fidelpb.Executor) (*blockScanExec, error) {
	columns := executor.TblScan.DeferredCausets
	ctx.evalCtx.setDeferredCausetInfo(columns)
	ranges, err := h.extractKVRanges(ctx.keyRanges, executor.TblScan.Desc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startTS := ctx.startTS
	if startTS == 0 {
		startTS = ctx.posetPosetDagReq.GetStartTsFallback()
	}
	colInfos := make([]rowcodec.DefCausInfo, len(columns))
	for i := range colInfos {
		col := columns[i]
		colInfos[i] = rowcodec.DefCausInfo{
			ID:         col.DeferredCausetId,
			Ft:         ctx.evalCtx.fieldTps[i],
			IsPKHandle: col.GetPkHandle(),
		}
	}
	defVal := func(i int) ([]byte, error) {
		col := columns[i]
		if col.DefaultVal == nil {
			return nil, nil
		}
		// col.DefaultVal always be  varint `[flag]+[value]`.
		if len(col.DefaultVal) < 1 {
			panic("invalid default value")
		}
		return col.DefaultVal, nil
	}
	rd := rowcodec.NewByteDecoder(colInfos, []int64{-1}, defVal, nil)
	e := &blockScanExec{
		TableScan:      executor.TblScan,
		kvRanges:       ranges,
		colIDs:         ctx.evalCtx.colIDs,
		startTS:        startTS,
		isolationLevel: h.isolationLevel,
		resolvedLocks:  h.resolvedLocks,
		mvsr-oocStore:      h.mvsr-oocStore,
		execDetail:     new(execDetail),
		rd:             rd,
	}

	if ctx.posetPosetDagReq.DefCauslectRangeCounts != nil && *ctx.posetPosetDagReq.DefCauslectRangeCounts {
		e.counts = make([]int64, len(ranges))
	}
	return e, nil
}

func (h *rpcHandler) buildIndexScan(ctx *posetPosetDagContext, executor *fidelpb.Executor) (*indexScanExec, error) {
	var err error
	columns := executor.IdxScan.DeferredCausets
	ctx.evalCtx.setDeferredCausetInfo(columns)
	length := len(columns)
	hdStatus := blockcodec.HandleNotNeeded
	// The PKHandle column info has been collected in ctx.
	if columns[length-1].GetPkHandle() {
		if allegrosql.HasUnsignedFlag(uint(columns[length-1].GetFlag())) {
			hdStatus = blockcodec.HandleIsUnsigned
		} else {
			hdStatus = blockcodec.HandleDefault
		}
		columns = columns[:length-1]
	} else if columns[length-1].DeferredCausetId == perceptron.ExtraHandleID {
		columns = columns[:length-1]
	}
	ranges, err := h.extractKVRanges(ctx.keyRanges, executor.IdxScan.Desc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startTS := ctx.startTS
	if startTS == 0 {
		startTS = ctx.posetPosetDagReq.GetStartTsFallback()
	}
	colInfos := make([]rowcodec.DefCausInfo, 0, len(columns))
	for i := range columns {
		col := columns[i]
		colInfos = append(colInfos, rowcodec.DefCausInfo{
			ID:         col.DeferredCausetId,
			Ft:         ctx.evalCtx.fieldTps[i],
			IsPKHandle: col.GetPkHandle(),
		})
	}
	e := &indexScanExec{
		IndexScan:      executor.IdxScan,
		kvRanges:       ranges,
		defcausLen:     len(columns),
		startTS:        startTS,
		isolationLevel: h.isolationLevel,
		resolvedLocks:  h.resolvedLocks,
		mvsr-oocStore:      h.mvsr-oocStore,
		hdStatus:       hdStatus,
		execDetail:     new(execDetail),
		colInfos:       colInfos,
	}
	if ctx.posetPosetDagReq.DefCauslectRangeCounts != nil && *ctx.posetPosetDagReq.DefCauslectRangeCounts {
		e.counts = make([]int64, len(ranges))
	}
	return e, nil
}

func (h *rpcHandler) buildSelection(ctx *posetPosetDagContext, executor *fidelpb.Executor) (*selectionExec, error) {
	var err error
	var relatedDefCausOffsets []int
	pbConds := executor.Selection.Conditions
	for _, cond := range pbConds {
		relatedDefCausOffsets, err = extractOffsetsInExpr(cond, ctx.evalCtx.columnInfos, relatedDefCausOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &selectionExec{
		evalCtx:               ctx.evalCtx,
		relatedDefCausOffsets: relatedDefCausOffsets,
		conditions:            conds,
		event:                 make([]types.Causet, len(ctx.evalCtx.columnInfos)),
		execDetail:            new(execDetail),
	}, nil
}

func (h *rpcHandler) getAggInfo(ctx *posetPosetDagContext, executor *fidelpb.Executor) ([]aggregation.Aggregation, []expression.Expression, []int, error) {
	length := len(executor.Aggregation.AggFunc)
	aggs := make([]aggregation.Aggregation, 0, length)
	var err error
	var relatedDefCausOffsets []int
	for _, expr := range executor.Aggregation.AggFunc {
		var aggExpr aggregation.Aggregation
		aggExpr, err = aggregation.NewDistAggFunc(expr, ctx.evalCtx.fieldTps, ctx.evalCtx.sc)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		aggs = append(aggs, aggExpr)
		relatedDefCausOffsets, err = extractOffsetsInExpr(expr, ctx.evalCtx.columnInfos, relatedDefCausOffsets)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
	}
	for _, item := range executor.Aggregation.GroupBy {
		relatedDefCausOffsets, err = extractOffsetsInExpr(item, ctx.evalCtx.columnInfos, relatedDefCausOffsets)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
	}
	groupBys, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, executor.Aggregation.GetGroupBy())
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	return aggs, groupBys, relatedDefCausOffsets, nil
}

func (h *rpcHandler) buildHashAgg(ctx *posetPosetDagContext, executor *fidelpb.Executor) (*hashAggExec, error) {
	aggs, groupBys, relatedDefCausOffsets, err := h.getAggInfo(ctx, executor)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &hashAggExec{
		evalCtx:               ctx.evalCtx,
		aggExprs:              aggs,
		groupByExprs:          groupBys,
		groups:                make(map[string]struct{}),
		groupKeys:             make([][]byte, 0),
		relatedDefCausOffsets: relatedDefCausOffsets,
		event:                 make([]types.Causet, len(ctx.evalCtx.columnInfos)),
		execDetail:            new(execDetail),
	}, nil
}

func (h *rpcHandler) buildStreamAgg(ctx *posetPosetDagContext, executor *fidelpb.Executor) (*streamAggExec, error) {
	aggs, groupBys, relatedDefCausOffsets, err := h.getAggInfo(ctx, executor)
	if err != nil {
		return nil, errors.Trace(err)
	}
	aggCtxs := make([]*aggregation.AggEvaluateContext, 0, len(aggs))
	for _, agg := range aggs {
		aggCtxs = append(aggCtxs, agg.CreateContext(ctx.evalCtx.sc))
	}

	return &streamAggExec{
		evalCtx:               ctx.evalCtx,
		aggExprs:              aggs,
		aggCtxs:               aggCtxs,
		groupByExprs:          groupBys,
		currGroupByValues:     make([][]byte, 0),
		relatedDefCausOffsets: relatedDefCausOffsets,
		event:                 make([]types.Causet, len(ctx.evalCtx.columnInfos)),
		execDetail:            new(execDetail),
	}, nil
}

func (h *rpcHandler) buildTopN(ctx *posetPosetDagContext, executor *fidelpb.Executor) (*topNExec, error) {
	topN := executor.TopN
	var err error
	var relatedDefCausOffsets []int
	pbConds := make([]*fidelpb.Expr, len(topN.OrderBy))
	for i, item := range topN.OrderBy {
		relatedDefCausOffsets, err = extractOffsetsInExpr(item.Expr, ctx.evalCtx.columnInfos, relatedDefCausOffsets)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pbConds[i] = item.Expr
	}
	heap := &topNHeap{
		totalCount: int(topN.Limit),
		topNSorter: topNSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.evalCtx.sc,
		},
	}

	conds, err := convertToExprs(ctx.evalCtx.sc, ctx.evalCtx.fieldTps, pbConds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &topNExec{
		heap:                  heap,
		evalCtx:               ctx.evalCtx,
		relatedDefCausOffsets: relatedDefCausOffsets,
		orderByExprs:          conds,
		event:                 make([]types.Causet, len(ctx.evalCtx.columnInfos)),
		execDetail:            new(execDetail),
	}, nil
}

type evalContext struct {
	colIDs      map[int64]int
	columnInfos []*fidelpb.DeferredCausetInfo
	fieldTps    []*types.FieldType
	sc          *stmtctx.StatementContext
}

func (e *evalContext) setDeferredCausetInfo(defcaus []*fidelpb.DeferredCausetInfo) {
	e.columnInfos = make([]*fidelpb.DeferredCausetInfo, len(defcaus))
	INTERLOCKy(e.columnInfos, defcaus)

	e.colIDs = make(map[int64]int, len(e.columnInfos))
	e.fieldTps = make([]*types.FieldType, 0, len(e.columnInfos))
	for i, col := range e.columnInfos {
		ft := fieldTypeFromPBDeferredCauset(col)
		e.fieldTps = append(e.fieldTps, ft)
		e.colIDs[col.GetDeferredCausetId()] = i
	}
}

// decodeRelatedDeferredCausetVals decodes data to Causet slice according to the event information.
func (e *evalContext) decodeRelatedDeferredCausetVals(relatedDefCausOffsets []int, value [][]byte, event []types.Causet) error {
	var err error
	for _, offset := range relatedDefCausOffsets {
		event[offset], err = blockcodec.DecodeDeferredCausetValue(value[offset], e.fieldTps[offset], e.sc.TimeZone)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// flagsToStatementContext creates a StatementContext from a `fidelpb.SelectRequest.Flags`.
func flagsToStatementContext(flags uint64) *stmtctx.StatementContext {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = (flags & perceptron.FlagIgnoreTruncate) > 0
	sc.TruncateAsWarning = (flags & perceptron.FlagTruncateAsWarning) > 0
	sc.InInsertStmt = (flags & perceptron.FlagInInsertStmt) > 0
	sc.InSelectStmt = (flags & perceptron.FlagInSelectStmt) > 0
	sc.InDeleteStmt = (flags & perceptron.FlagInUFIDelateOrDeleteStmt) > 0
	sc.OverflowAsWarning = (flags & perceptron.FlagOverflowAsWarning) > 0
	sc.IgnoreZeroInDate = (flags & perceptron.FlagIgnoreZeroInDate) > 0
	sc.DividedByZeroAsWarning = (flags & perceptron.FlagDividedByZeroAsWarning) > 0
	// TODO set FlagInSetOprStmt,
	return sc
}

// MockGRPCClientStream is exported for testing purpose.
func MockGRPCClientStream() grpc.ClientStream {
	return mockClientStream{}
}

// mockClientStream implements grpc ClientStream interface, its methods are never called.
type mockClientStream struct{}

// Header implements grpc.ClientStream interface
func (mockClientStream) Header() (metadata.MD, error) { return nil, nil }

// Trailer implements grpc.ClientStream interface
func (mockClientStream) Trailer() metadata.MD { return nil }

// CloseSend implements grpc.ClientStream interface
func (mockClientStream) CloseSend() error { return nil }

// Context implements grpc.ClientStream interface
func (mockClientStream) Context() context.Context { return nil }

// SendMsg implements grpc.ClientStream interface
func (mockClientStream) SendMsg(m interface{}) error { return nil }

// RecvMsg implements grpc.ClientStream interface
func (mockClientStream) RecvMsg(m interface{}) error { return nil }

type mockINTERLOCKStreamClient struct {
	mockClientStream

	req              *fidelpb.PosetDagRequest
	exec             executor
	ctx              context.Context
	posetPosetDagCtx *posetPosetDagContext
	finished         bool
}

type mockBathINTERLOCKErrClient struct {
	mockClientStream

	*errorpb.Error
}

func (mock *mockBathINTERLOCKErrClient) Recv() (*interlock.BatchResponse, error) {
	return &interlock.BatchResponse{
		OtherError: mock.Error.Message,
	}, nil
}

type mockBatchCoFIDelataClient struct {
	mockClientStream

	chunks []fidelpb.Chunk
	idx    int
}

func (mock *mockBatchCoFIDelataClient) Recv() (*interlock.BatchResponse, error) {
	if mock.idx < len(mock.chunks) {
		res := fidelpb.SelectResponse{
			Chunks: []fidelpb.Chunk{mock.chunks[mock.idx]},
		}
		raw, err := res.Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		mock.idx++
		return &interlock.BatchResponse{
			Data: raw,
		}, nil
	}
	return nil, io.EOF
}

type mockINTERLOCKStreamErrClient struct {
	mockClientStream

	*errorpb.Error
}

func (mock *mockINTERLOCKStreamErrClient) Recv() (*interlock.Response, error) {
	return &interlock.Response{
		RegionError: mock.Error,
	}, nil
}

func (mock *mockINTERLOCKStreamClient) Recv() (*interlock.Response, error) {
	select {
	case <-mock.ctx.Done():
		return nil, mock.ctx.Err()
	default:
	}

	if mock.finished {
		return nil, io.EOF
	}

	if hook := mock.ctx.Value(mockpkg.HookKeyForTest("mockEinsteinDBStreamRecvHook")); hook != nil {
		hook.(func(context.Context))(mock.ctx)
	}

	var resp interlock.Response
	chunk, finish, ran, counts, warnings, err := mock.readBlockFromExecutor()
	resp.Range = ran
	if err != nil {
		if locked, ok := errors.Cause(err).(*ErrLocked); ok {
			resp.Locked = &kvrpcpb.LockInfo{
				Key:         locked.Key,
				PrimaryLock: locked.Primary,
				LockVersion: locked.StartTS,
				LockTtl:     locked.TTL,
			}
		} else {
			resp.OtherError = err.Error()
		}
		return &resp, nil
	}
	if finish {
		// Just mark it, need to handle the last chunk.
		mock.finished = true
	}

	data, err := chunk.Marshal()
	if err != nil {
		resp.OtherError = err.Error()
		return &resp, nil
	}
	var Warnings []*fidelpb.Error
	if len(warnings) > 0 {
		Warnings = make([]*fidelpb.Error, 0, len(warnings))
		for i := range warnings {
			Warnings = append(Warnings, toPBError(warnings[i].Err))
		}
	}
	streamResponse := fidelpb.StreamResponse{
		Error:        toPBError(err),
		Data:         data,
		Warnings:     Warnings,
		OutputCounts: counts,
	}
	resp.Data, err = proto.Marshal(&streamResponse)
	if err != nil {
		resp.OtherError = err.Error()
	}
	return &resp, nil
}

func (mock *mockINTERLOCKStreamClient) readBlockFromExecutor() (fidelpb.Chunk, bool, *interlock.KeyRange, []int64, []stmtctx.ALLEGROSQLWarn, error) {
	var chunk fidelpb.Chunk
	var ran interlock.KeyRange
	var finish bool
	var desc bool
	mock.exec.ResetCounts()
	ran.Start, desc = mock.exec.Cursor()
	for count := 0; count < rowsPerChunk; count++ {
		event, err := mock.exec.Next(mock.ctx)
		if err != nil {
			ran.End, _ = mock.exec.Cursor()
			return chunk, false, &ran, nil, nil, errors.Trace(err)
		}
		if event == nil {
			finish = true
			break
		}
		for _, offset := range mock.req.OutputOffsets {
			chunk.RowsData = append(chunk.RowsData, event[offset]...)
		}
	}

	ran.End, _ = mock.exec.Cursor()
	if desc {
		ran.Start, ran.End = ran.End, ran.Start
	}
	warnings := mock.posetPosetDagCtx.evalCtx.sc.GetWarnings()
	mock.posetPosetDagCtx.evalCtx.sc.SetWarnings(nil)
	return chunk, finish, &ran, mock.exec.Counts(), warnings, nil
}

func (h *rpcHandler) initSelectResponse(err error, warnings []stmtctx.ALLEGROSQLWarn, counts []int64) *fidelpb.SelectResponse {
	selResp := &fidelpb.SelectResponse{
		Error:        toPBError(err),
		OutputCounts: counts,
	}
	for i := range warnings {
		selResp.Warnings = append(selResp.Warnings, toPBError(warnings[i].Err))
	}
	return selResp
}

func (h *rpcHandler) fillUFIDelata4SelectResponse(selResp *fidelpb.SelectResponse, posetPosetDagReq *fidelpb.PosetDagRequest, posetPosetDagCtx *posetPosetDagContext, rows [][][]byte) error {
	switch posetPosetDagReq.EncodeType {
	case fidelpb.EncodeType_TypeDefault:
		h.encodeDefault(selResp, rows, posetPosetDagReq.OutputOffsets)
	case fidelpb.EncodeType_TypeChunk:
		colTypes := h.constructRespSchema(posetPosetDagCtx)
		loc := posetPosetDagCtx.evalCtx.sc.TimeZone
		err := h.encodeChunk(selResp, rows, colTypes, posetPosetDagReq.OutputOffsets, loc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *rpcHandler) constructRespSchema(posetPosetDagCtx *posetPosetDagContext) []*types.FieldType {
	var root *fidelpb.Executor
	if len(posetPosetDagCtx.posetPosetDagReq.Executors) == 0 {
		root = posetPosetDagCtx.posetPosetDagReq.RootExecutor
	} else {
		root = posetPosetDagCtx.posetPosetDagReq.Executors[len(posetPosetDagCtx.posetPosetDagReq.Executors)-1]
	}
	agg := root.Aggregation
	if agg == nil {
		return posetPosetDagCtx.evalCtx.fieldTps
	}

	schemaReplicant := make([]*types.FieldType, 0, len(agg.AggFunc)+len(agg.GroupBy))
	for i := range agg.AggFunc {
		if agg.AggFunc[i].Tp == fidelpb.ExprType_Avg {
			// Avg function requests two columns : Count , Sum
			// This line addend the Count(TypeLonglong) to the schemaReplicant.
			schemaReplicant = append(schemaReplicant, types.NewFieldType(allegrosql.TypeLonglong))
		}
		schemaReplicant = append(schemaReplicant, expression.PbTypeToFieldType(agg.AggFunc[i].FieldType))
	}
	for i := range agg.GroupBy {
		schemaReplicant = append(schemaReplicant, expression.PbTypeToFieldType(agg.GroupBy[i].FieldType))
	}
	return schemaReplicant
}

func (h *rpcHandler) encodeDefault(selResp *fidelpb.SelectResponse, rows [][][]byte, colOrdinal []uint32) {
	var chunks []fidelpb.Chunk
	for i := range rows {
		requestedRow := dummySlice
		for _, ordinal := range colOrdinal {
			requestedRow = append(requestedRow, rows[i][ordinal]...)
		}
		chunks = appendRow(chunks, requestedRow, i)
	}
	selResp.Chunks = chunks
	selResp.EncodeType = fidelpb.EncodeType_TypeDefault
}

func (h *rpcHandler) encodeChunk(selResp *fidelpb.SelectResponse, rows [][][]byte, colTypes []*types.FieldType, colOrdinal []uint32, loc *time.Location) error {
	var chunks []fidelpb.Chunk
	respDefCausTypes := make([]*types.FieldType, 0, len(colOrdinal))
	for _, ordinal := range colOrdinal {
		respDefCausTypes = append(respDefCausTypes, colTypes[ordinal])
	}
	chk := chunk.NewChunkWithCapacity(respDefCausTypes, rowsPerChunk)
	encoder := chunk.NewCodec(respDefCausTypes)
	decoder := codec.NewDecoder(chk, loc)
	for i := range rows {
		for j, ordinal := range colOrdinal {
			_, err := decoder.DecodeOne(rows[i][ordinal], j, colTypes[ordinal])
			if err != nil {
				return err
			}
		}
		if i%rowsPerChunk == rowsPerChunk-1 {
			chunks = append(chunks, fidelpb.Chunk{})
			cur := &chunks[len(chunks)-1]
			cur.RowsData = append(cur.RowsData, encoder.Encode(chk)...)
			chk.Reset()
		}
	}
	if chk.NumRows() > 0 {
		chunks = append(chunks, fidelpb.Chunk{})
		cur := &chunks[len(chunks)-1]
		cur.RowsData = append(cur.RowsData, encoder.Encode(chk)...)
		chk.Reset()
	}
	selResp.Chunks = chunks
	selResp.EncodeType = fidelpb.EncodeType_TypeChunk
	return nil
}

func buildResp(selResp *fidelpb.SelectResponse, execDetails []*execDetail, err error) *interlock.Response {
	resp := &interlock.Response{}

	if len(execDetails) > 0 {
		execSummary := make([]*fidelpb.ExecutorExecutionSummary, 0, len(execDetails))
		for _, d := range execDetails {
			costNs := uint64(d.timeProcessed / time.Nanosecond)
			rows := uint64(d.numProducedRows)
			numIter := uint64(d.numIterations)
			execSummary = append(execSummary, &fidelpb.ExecutorExecutionSummary{
				TimeProcessedNs: &costNs,
				NumProducedRows: &rows,
				NumIterations:   &numIter,
			})
		}
		selResp.ExecutionSummaries = execSummary
	}

	// Select errors have been contained in `SelectResponse.Error`
	if locked, ok := errors.Cause(err).(*ErrLocked); ok {
		resp.Locked = &kvrpcpb.LockInfo{
			Key:         locked.Key,
			PrimaryLock: locked.Primary,
			LockVersion: locked.StartTS,
			LockTtl:     locked.TTL,
		}
	}
	data, err := proto.Marshal(selResp)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	resp.Data = data
	return resp
}

func toPBError(err error) *fidelpb.Error {
	if err == nil {
		return nil
	}
	perr := new(fidelpb.Error)
	switch x := err.(type) {
	case *terror.Error:
		sqlErr := terror.ToALLEGROSQLError(x)
		perr.Code = int32(sqlErr.Code)
		perr.Msg = sqlErr.Message
	default:
		e := errors.Cause(err)
		switch y := e.(type) {
		case *terror.Error:
			tmp := terror.ToALLEGROSQLError(y)
			perr.Code = int32(tmp.Code)
			perr.Msg = tmp.Message
		default:
			perr.Code = int32(1)
			perr.Msg = err.Error()
		}
	}
	return perr
}

// extractKVRanges extracts ekv.KeyRanges slice from a SelectRequest.
func (h *rpcHandler) extractKVRanges(keyRanges []*interlock.KeyRange, descScan bool) (kvRanges []ekv.KeyRange, err error) {
	for _, kran := range keyRanges {
		if bytes.Compare(kran.GetStart(), kran.GetEnd()) >= 0 {
			err = errors.Errorf("invalid range, start should be smaller than end: %v %v", kran.GetStart(), kran.GetEnd())
			return
		}

		upperKey := kran.GetEnd()
		if bytes.Compare(upperKey, h.rawStartKey) <= 0 {
			continue
		}
		lowerKey := kran.GetStart()
		if len(h.rawEndKey) != 0 && bytes.Compare(lowerKey, h.rawEndKey) >= 0 {
			break
		}
		var kvr ekv.KeyRange
		kvr.StartKey = maxStartKey(lowerKey, h.rawStartKey)
		kvr.EndKey = minEndKey(upperKey, h.rawEndKey)
		kvRanges = append(kvRanges, kvr)
	}
	if descScan {
		reverseKVRanges(kvRanges)
	}
	return
}

func reverseKVRanges(kvRanges []ekv.KeyRange) {
	for i := 0; i < len(kvRanges)/2; i++ {
		j := len(kvRanges) - i - 1
		kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
	}
}

const rowsPerChunk = 64

func appendRow(chunks []fidelpb.Chunk, data []byte, rowCnt int) []fidelpb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, fidelpb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

func maxStartKey(rangeStartKey ekv.Key, regionStartKey []byte) []byte {
	if bytes.Compare(rangeStartKey, regionStartKey) > 0 {
		return rangeStartKey
	}
	return regionStartKey
}

func minEndKey(rangeEndKey ekv.Key, regionEndKey []byte) []byte {
	if len(regionEndKey) == 0 || bytes.Compare(rangeEndKey, regionEndKey) < 0 {
		return rangeEndKey
	}
	return regionEndKey
}

func isDuplicated(offsets []int, offset int) bool {
	for _, idx := range offsets {
		if idx == offset {
			return true
		}
	}
	return false
}

func extractOffsetsInExpr(expr *fidelpb.Expr, columns []*fidelpb.DeferredCausetInfo, collector []int) ([]int, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.GetTp() == fidelpb.ExprType_DeferredCausetRef {
		_, idx, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !isDuplicated(collector, int(idx)) {
			collector = append(collector, int(idx))
		}
		return collector, nil
	}
	var err error
	for _, child := range expr.Children {
		collector, err = extractOffsetsInExpr(child, columns, collector)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return collector, nil
}

// fieldTypeFromPBDeferredCauset creates a types.FieldType from fidelpb.DeferredCausetInfo.
func fieldTypeFromPBDeferredCauset(col *fidelpb.DeferredCausetInfo) *types.FieldType {
	return &types.FieldType{
		Tp:          byte(col.GetTp()),
		Flag:        uint(col.Flag),
		Flen:        int(col.GetDeferredCausetLen()),
		Decimal:     int(col.GetDecimal()),
		Elems:       col.Elems,
		DefCauslate: allegrosql.DefCauslations[uint8(collate.RestoreDefCauslationIDIfNeeded(col.GetDefCauslation()))],
	}
}
