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

package INTERLOCKhandler

import (
	"bytes"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ngaut/entangledstore/einsteindb/dbreader"
	"github.com/ngaut/entangledstore/lockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/solomonkey"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression/aggregation"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/collate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/rowcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/solomonkeyproto/pkg/interlock"
	"github.com/whtcorpsinc/solomonkeyproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// HandleINTERLOCKRequest handles interlock request.
func HandleINTERLOCKRequest(dbReader *dbreader.DBReader, lockStore *lockstore.MemStore, req *interlock.Request) *interlock.Response {
	switch req.Tp {
	case solomonkey.ReqTypePosetDag:
		return handleCoFIDelAGRequest(dbReader, lockStore, req)
	case solomonkey.ReqTypeAnalyze:
		return handleINTERLOCKAnalyzeRequest(dbReader, req)
	case solomonkey.ReqTypeChecksum:
		return handleINTERLOCKChecksumRequest(dbReader, req)
	}
	return &interlock.Response{OtherError: fmt.Sprintf("unsupported request type %d", req.GetTp())}
}

type posetPosetDagContext struct {
	*evalContext
	dbReader         *dbreader.DBReader
	lockStore        *lockstore.MemStore
	resolvedLocks    []uint64
	posetPosetDagReq *fidelpb.PosetDagRequest
	keyRanges        []*interlock.KeyRange
	startTS          uint64
}

// handleCoFIDelAGRequest handles interlock PosetDag request.
func handleCoFIDelAGRequest(dbReader *dbreader.DBReader, lockStore *lockstore.MemStore, req *interlock.Request) *interlock.Response {
	startTime := time.Now()
	resp := &interlock.Response{}
	posetPosetDagCtx, posetPosetDagReq, err := buildPosetDag(dbReader, lockStore, req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	closureExec, err := buildClosureExecutor(posetPosetDagCtx, posetPosetDagReq)
	if err != nil {
		return buildResp(nil, nil, posetPosetDagReq, err, posetPosetDagCtx.sc.GetWarnings(), time.Since(startTime))
	}
	chunks, err := closureExec.execute()
	return buildResp(chunks, closureExec.counts, posetPosetDagReq, err, posetPosetDagCtx.sc.GetWarnings(), time.Since(startTime))
}

func buildPosetDag(reader *dbreader.DBReader, lockStore *lockstore.MemStore, req *interlock.Request) (*posetPosetDagContext, *fidelpb.PosetDagRequest, error) {
	if len(req.Ranges) == 0 {
		return nil, nil, errors.New("request range is null")
	}
	if req.GetTp() != solomonkey.ReqTypePosetDag {
		return nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}

	posetPosetDagReq := new(fidelpb.PosetDagRequest)
	err := proto.Unmarshal(req.Data, posetPosetDagReq)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	sc := flagsToStatementContext(posetPosetDagReq.Flags)
	sc.TimeZone = time.FixedZone("UTC", int(posetPosetDagReq.TimeZoneOffset))
	ctx := &posetPosetDagContext{
		evalContext:      &evalContext{sc: sc},
		dbReader:         reader,
		lockStore:        lockStore,
		posetPosetDagReq: posetPosetDagReq,
		keyRanges:        req.Ranges,
		startTS:          req.StartTs,
		resolvedLocks:    req.Context.ResolvedLocks,
	}
	scanExec := posetPosetDagReq.Executors[0]
	if scanExec.Tp == fidelpb.ExecType_TypeTableScan {
		ctx.setDeferredCausetInfo(scanExec.TblScan.DeferredCausets)
		ctx.primaryDefCauss = scanExec.TblScan.PrimaryDeferredCausetIds
	} else {
		ctx.setDeferredCausetInfo(scanExec.IdxScan.DeferredCausets)
	}
	return ctx, posetPosetDagReq, err
}

func getAggInfo(ctx *posetPosetDagContext, pbAgg *fidelpb.Aggregation) ([]aggregation.Aggregation, []expression.Expression, error) {
	length := len(pbAgg.AggFunc)
	aggs := make([]aggregation.Aggregation, 0, length)
	var err error
	for _, expr := range pbAgg.AggFunc {
		var aggExpr aggregation.Aggregation
		aggExpr, err = aggregation.NewDistAggFunc(expr, ctx.fieldTps, ctx.sc)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		aggs = append(aggs, aggExpr)
	}
	groupBys, err := convertToExprs(ctx.sc, ctx.fieldTps, pbAgg.GetGroupBy())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return aggs, groupBys, nil
}

func getTopNInfo(ctx *evalContext, topN *fidelpb.TopN) (heap *topNHeap, conds []expression.Expression, err error) {
	pbConds := make([]*fidelpb.Expr, len(topN.OrderBy))
	for i, item := range topN.OrderBy {
		pbConds[i] = item.Expr
	}
	heap = &topNHeap{
		totalCount: int(topN.Limit),
		topNSorter: topNSorter{
			orderByItems: topN.OrderBy,
			sc:           ctx.sc,
		},
	}
	if conds, err = convertToExprs(ctx.sc, ctx.fieldTps, pbConds); err != nil {
		return nil, nil, errors.Trace(err)
	}

	return heap, conds, nil
}

type evalContext struct {
	colIDs          map[int64]int
	columnInfos     []*fidelpb.DeferredCausetInfo
	fieldTps        []*types.FieldType
	primaryDefCauss []int64
	sc              *stmtctx.StatementContext
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

func (e *evalContext) newRowDecoder() (*rowcodec.ChunkDecoder, error) {
	var (
		pkDefCauss []int64
		defcaus    = make([]rowcodec.DefCausInfo, 0, len(e.columnInfos))
	)
	for i := range e.columnInfos {
		info := e.columnInfos[i]
		ft := e.fieldTps[i]
		col := rowcodec.DefCausInfo{
			ID:         info.DeferredCausetId,
			Ft:         ft,
			IsPKHandle: info.PkHandle,
		}
		defcaus = append(defcaus, col)
		if info.PkHandle {
			pkDefCauss = append(pkDefCauss, info.DeferredCausetId)
		}
	}
	if len(pkDefCauss) == 0 {
		if e.primaryDefCauss != nil {
			pkDefCauss = e.primaryDefCauss
		} else {
			pkDefCauss = []int64{0}
		}
	}
	def := func(i int, chk *chunk.Chunk) error {
		info := e.columnInfos[i]
		if info.PkHandle || len(info.DefaultVal) == 0 {
			chk.AppendNull(i)
			return nil
		}
		decoder := codec.NewDecoder(chk, e.sc.TimeZone)
		_, err := decoder.DecodeOne(info.DefaultVal, i, e.fieldTps[i])
		if err != nil {
			return err
		}
		return nil
	}
	return rowcodec.NewChunkDecoder(defcaus, pkDefCauss, def, e.sc.TimeZone), nil
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
	return sc
}

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the dagger then retry.
type ErrLocked struct {
	Key      []byte
	Primary  []byte
	StartTS  uint64
	TTL      uint64
	LockType uint8
}

// BuildLockErr generates ErrKeyLocked objects
func BuildLockErr(key []byte, primaryKey []byte, startTS uint64, TTL uint64, lockType uint8) *ErrLocked {
	errLocked := &ErrLocked{
		Key:      key,
		Primary:  primaryKey,
		StartTS:  startTS,
		TTL:      TTL,
		LockType: lockType,
	}
	return errLocked
}

// Error formats the dagger to a string.
func (e *ErrLocked) Error() string {
	return fmt.Sprintf("key is locked, key: %q, Type: %v, primary: %q, startTS: %v", e.Key, e.LockType, e.Primary, e.StartTS)
}

func buildResp(chunks []fidelpb.Chunk, counts []int64, posetPosetDagReq *fidelpb.PosetDagRequest, err error, warnings []stmtctx.ALLEGROSQLWarn, dur time.Duration) *interlock.Response {
	resp := &interlock.Response{}
	selResp := &fidelpb.SelectResponse{
		Error:        toPBError(err),
		Chunks:       chunks,
		OutputCounts: counts,
	}
	if posetPosetDagReq.DefCauslectExecutionSummaries != nil && *posetPosetDagReq.DefCauslectExecutionSummaries {
		execSummary := make([]*fidelpb.ExecutorExecutionSummary, len(posetPosetDagReq.Executors))
		for i := range execSummary {
			// TODO: Add real executor execution summary information.
			execSummary[i] = &fidelpb.ExecutorExecutionSummary{}
		}
		selResp.ExecutionSummaries = execSummary
	}
	if len(warnings) > 0 {
		selResp.Warnings = make([]*fidelpb.Error, 0, len(warnings))
		for i := range warnings {
			selResp.Warnings = append(selResp.Warnings, toPBError(warnings[i].Err))
		}
	}
	if locked, ok := errors.Cause(err).(*ErrLocked); ok {
		resp.Locked = &kvrpcpb.LockInfo{
			Key:         locked.Key,
			PrimaryLock: locked.Primary,
			LockVersion: locked.StartTS,
			LockTtl:     locked.TTL,
		}
	}
	resp.ExecDetails = &kvrpcpb.ExecDetails{
		HandleTime: &kvrpcpb.HandleTime{ProcessMs: int64(dur / time.Millisecond)},
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
	e := errors.Cause(err)
	switch y := e.(type) {
	case *terror.Error:
		tmp := terror.ToALLEGROSQLError(y)
		perr.Code = int32(tmp.Code)
		perr.Msg = tmp.Message
	case *allegrosql.ALLEGROSQLError:
		perr.Code = int32(y.Code)
		perr.Msg = y.Message
	default:
		perr.Code = int32(1)
		perr.Msg = err.Error()
	}
	return perr
}

// extractKVRanges extracts solomonkey.KeyRanges slice from a SelectRequest.
func extractKVRanges(startKey, endKey []byte, keyRanges []*interlock.KeyRange, descScan bool) (kvRanges []solomonkey.KeyRange, err error) {
	kvRanges = make([]solomonkey.KeyRange, 0, len(keyRanges))
	for _, kran := range keyRanges {
		if bytes.Compare(kran.GetStart(), kran.GetEnd()) >= 0 {
			err = errors.Errorf("invalid range, start should be smaller than end: %v %v", kran.GetStart(), kran.GetEnd())
			return
		}

		upperKey := kran.GetEnd()
		if bytes.Compare(upperKey, startKey) <= 0 {
			continue
		}
		lowerKey := kran.GetStart()
		if len(endKey) != 0 && bytes.Compare(lowerKey, endKey) >= 0 {
			break
		}
		r := solomonkey.KeyRange{
			StartKey: solomonkey.Key(maxStartKey(lowerKey, startKey)),
			EndKey:   solomonkey.Key(minEndKey(upperKey, endKey)),
		}
		kvRanges = append(kvRanges, r)
	}
	if descScan {
		reverseKVRanges(kvRanges)
	}
	return
}

func reverseKVRanges(kvRanges []solomonkey.KeyRange) {
	for i := 0; i < len(kvRanges)/2; i++ {
		j := len(kvRanges) - i - 1
		kvRanges[i], kvRanges[j] = kvRanges[j], kvRanges[i]
	}
}

func maxStartKey(rangeStartKey solomonkey.Key, regionStartKey []byte) []byte {
	if bytes.Compare([]byte(rangeStartKey), regionStartKey) > 0 {
		return []byte(rangeStartKey)
	}
	return regionStartKey
}

func minEndKey(rangeEndKey solomonkey.Key, regionEndKey []byte) []byte {
	if len(regionEndKey) == 0 || bytes.Compare([]byte(rangeEndKey), regionEndKey) < 0 {
		return []byte(rangeEndKey)
	}
	return regionEndKey
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

// handleINTERLOCKChecksumRequest handles interlock check sum request.
func handleINTERLOCKChecksumRequest(dbReader *dbreader.DBReader, req *interlock.Request) *interlock.Response {
	resp := &fidelpb.ChecksumResponse{
		Checksum:   1,
		TotalKvs:   1,
		TotalBytes: 1,
	}
	data, err := resp.Marshal()
	if err != nil {
		return &interlock.Response{OtherError: fmt.Sprintf("marshal checksum response error: %v", err)}
	}
	return &interlock.Response{Data: data}
}
