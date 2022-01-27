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

package INTERLOCKhandler

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/juju/errors"
	"github.com/ngaut/entangledstore/einsteindb/dbreader"
	"github.com/ngaut/entangledstore/einsteindb/mvcc"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	mockpkg "github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

const chunkMaxRows = 1024

const (
	pkDefCausNotExists = iota
	pkDefCausIsSigned
	pkDefCausIsUnsigned
	pkDefCausIsCommon
)

func mapPkStatusToHandleStatus(pkStatus int) blockcodec.HandleStatus {
	switch pkStatus {
	case pkDefCausNotExists:
		return blockcodec.HandleNotNeeded
	case pkDefCausIsCommon | pkDefCausIsSigned:
		return blockcodec.HandleDefault
	case pkDefCausIsUnsigned:
		return blockcodec.HandleIsUnsigned
	}
	return blockcodec.HandleDefault
}

// buildClosureExecutor build a closureExecutor for the PosetDagRequest.
// Currently the composition of executors are:
// 	blockScan|indexScan [selection] [topN | limit | agg]
func buildClosureExecutor(posetPosetDagCtx *posetPosetDagContext, posetPosetDagReq *fidelpb.PosetDagRequest) (*closureExecutor, error) {
	ce, err := newClosureExecutor(posetPosetDagCtx, posetPosetDagReq)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executors := posetPosetDagReq.Executors
	scanExec := executors[0]
	if scanExec.Tp == fidelpb.ExecType_TypeTableScan {
		ce.processor = &blockScanProcessor{closureExecutor: ce}
	} else {
		ce.processor = &indexScanProcessor{closureExecutor: ce}
	}
	if len(executors) == 1 {
		return ce, nil
	}
	if secondExec := executors[1]; secondExec.Tp == fidelpb.ExecType_TypeSelection {
		ce.selectionCtx.conditions, err = convertToExprs(ce.sc, ce.fieldTps, secondExec.Selection.Conditions)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ce.processor = &selectionProcessor{closureExecutor: ce}
	}
	lastExecutor := executors[len(executors)-1]
	switch lastExecutor.Tp {
	case fidelpb.ExecType_TypeLimit:
		ce.limit = int(lastExecutor.Limit.Limit)
	case fidelpb.ExecType_TypeTopN:
		err = buildTopNProcessor(ce, lastExecutor.TopN)
	case fidelpb.ExecType_TypeAggregation:
		err = buildHashAggProcessor(ce, posetPosetDagCtx, lastExecutor.Aggregation)
	case fidelpb.ExecType_TypeStreamAgg:
		err = buildStreamAggProcessor(ce, posetPosetDagCtx, executors)
	case fidelpb.ExecType_TypeSelection:
		ce.processor = &selectionProcessor{closureExecutor: ce}
	default:
		panic("unknown executor type " + lastExecutor.Tp.String())
	}
	if err != nil {
		return nil, err
	}
	return ce, nil
}

func convertToExprs(sc *stmtctx.StatementContext, fieldTps []*types.FieldType, pbExprs []*fidelpb.Expr) ([]expression.Expression, error) {
	exprs := make([]expression.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := expression.PBToExpr(expr, fieldTps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

func newClosureExecutor(posetPosetDagCtx *posetPosetDagContext, posetPosetDagReq *fidelpb.PosetDagRequest) (*closureExecutor, error) {
	e := &closureExecutor{
		posetPosetDagContext: posetPosetDagCtx,
		outputOff:            posetPosetDagReq.OutputOffsets,
		startTS:              posetPosetDagCtx.startTS,
		limit:                math.MaxInt64,
	}
	seCtx := mockpkg.NewContext()
	seCtx.GetStochastikVars().StmtCtx = e.sc
	e.seCtx = seCtx
	executors := posetPosetDagReq.Executors
	scanExec := executors[0]
	switch scanExec.Tp {
	case fidelpb.ExecType_TypeTableScan:
		tblScan := executors[0].TblScan
		e.unique = true
		e.scanCtx.desc = tblScan.Desc
	case fidelpb.ExecType_TypeIndexScan:
		idxScan := executors[0].IdxScan
		e.unique = idxScan.GetUnique()
		e.scanCtx.desc = idxScan.Desc
		e.initIdxScanCtx(idxScan)
	default:
		panic(fmt.Sprintf("unknown first executor type %s", executors[0].Tp))
	}
	ranges, err := extractKVRanges(posetPosetDagCtx.dbReader.StartKey, posetPosetDagCtx.dbReader.EndKey, posetPosetDagCtx.keyRanges, e.scanCtx.desc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if posetPosetDagReq.GetDefCauslectRangeCounts() {
		e.counts = make([]int64, len(ranges))
	}
	e.kvRanges = ranges
	e.scanCtx.chk = chunk.NewChunkWithCapacity(e.fieldTps, 32)
	if e.idxScanCtx == nil {
		e.scanCtx.decoder, err = e.evalContext.newRowDecoder()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return e, nil
}

func (e *closureExecutor) initIdxScanCtx(idxScan *fidelpb.IndexScan) {
	e.idxScanCtx = new(idxScanCtx)
	e.idxScanCtx.columnLen = len(e.columnInfos)
	e.idxScanCtx.pkStatus = pkDefCausNotExists

	e.idxScanCtx.primaryDeferredCausetIds = idxScan.PrimaryDeferredCausetIds

	lastDeferredCauset := e.columnInfos[len(e.columnInfos)-1]

	if len(e.idxScanCtx.primaryDeferredCausetIds) == 0 {
		if lastDeferredCauset.GetPkHandle() {
			if allegrosql.HasUnsignedFlag(uint(lastDeferredCauset.GetFlag())) {
				e.idxScanCtx.pkStatus = pkDefCausIsUnsigned
			} else {
				e.idxScanCtx.pkStatus = pkDefCausIsSigned
			}
			e.idxScanCtx.columnLen--
		} else if lastDeferredCauset.DeferredCausetId == perceptron.ExtraHandleID {
			e.idxScanCtx.pkStatus = pkDefCausIsSigned
			e.idxScanCtx.columnLen--
		}
	} else {
		e.idxScanCtx.pkStatus = pkDefCausIsCommon
		e.idxScanCtx.columnLen -= len(e.idxScanCtx.primaryDeferredCausetIds)
	}

	colInfos := make([]rowcodec.DefCausInfo, len(e.columnInfos))
	for i := range colInfos {
		col := e.columnInfos[i]
		colInfos[i] = rowcodec.DefCausInfo{
			ID:         col.DeferredCausetId,
			Ft:         e.fieldTps[i],
			IsPKHandle: col.GetPkHandle(),
		}
	}
	e.idxScanCtx.colInfos = colInfos

	colIDs := make(map[int64]int, len(colInfos))
	for i, col := range colInfos[:e.idxScanCtx.columnLen] {
		colIDs[col.ID] = i
	}
	e.scanCtx.newDefCauslationIds = colIDs

	// We don't need to decode handle here, and colIDs >= 0 always.
	e.scanCtx.newDefCauslationRd = rowcodec.NewByteDecoder(colInfos[:e.idxScanCtx.columnLen], []int64{-1}, nil, nil)
}

func isCountAgg(pbAgg *fidelpb.Aggregation) bool {
	if len(pbAgg.AggFunc) == 1 && len(pbAgg.GroupBy) == 0 {
		aggFunc := pbAgg.AggFunc[0]
		if aggFunc.Tp == fidelpb.ExprType_Count && len(aggFunc.Children) == 1 {
			return true
		}
	}
	return false
}

func tryBuildCountProcessor(e *closureExecutor, executors []*fidelpb.Executor) (bool, error) {
	if len(executors) > 2 {
		return false, nil
	}
	agg := executors[1].Aggregation
	if !isCountAgg(agg) {
		return false, nil
	}
	child := agg.AggFunc[0].Children[0]
	switch child.Tp {
	case fidelpb.ExprType_DeferredCausetRef:
		_, idx, err := codec.DecodeInt(child.Val)
		if err != nil {
			return false, errors.Trace(err)
		}
		e.aggCtx.col = e.columnInfos[idx]
		if e.aggCtx.col.PkHandle {
			e.processor = &countStarProcessor{skipVal: skipVal(true), closureExecutor: e}
		} else {
			e.processor = &countDeferredCausetProcessor{closureExecutor: e}
		}
	case fidelpb.ExprType_Null, fidelpb.ExprType_ScalarFunc:
		return false, nil
	default:
		e.processor = &countStarProcessor{skipVal: skipVal(true), closureExecutor: e}
	}
	return true, nil
}

func buildTopNProcessor(e *closureExecutor, topN *fidelpb.TopN) error {
	heap, conds, err := getTopNInfo(e.evalContext, topN)
	if err != nil {
		return errors.Trace(err)
	}

	ctx := &topNCtx{
		heap:         heap,
		orderByExprs: conds,
		sortRow:      e.newTopNSortRow(),
	}

	e.topNCtx = ctx
	e.processor = &topNProcessor{closureExecutor: e}
	return nil
}

func buildHashAggProcessor(e *closureExecutor, ctx *posetPosetDagContext, agg *fidelpb.Aggregation) error {
	aggs, groupBys, err := getAggInfo(ctx, agg)
	if err != nil {
		return err
	}
	e.processor = &hashAggProcessor{
		closureExecutor: e,
		aggExprs:        aggs,
		groupByExprs:    groupBys,
		groups:          map[string]struct{}{},
		groupKeys:       nil,
		aggCtxsMap:      map[string][]*aggregation.AggEvaluateContext{},
	}
	return nil
}

func buildStreamAggProcessor(e *closureExecutor, ctx *posetPosetDagContext, executors []*fidelpb.Executor) error {
	ok, err := tryBuildCountProcessor(e, executors)
	if err != nil || ok {
		return err
	}
	return buildHashAggProcessor(e, ctx, executors[len(executors)-1].Aggregation)
}

// closureExecutor is an execution engine that flatten the PosetDagRequest.Executors to a single closure `processor` that
// process key/value pairs. We can define many closures for different HoTTs of requests, try to use the specially
// optimized one for some frequently used query.
type closureExecutor struct {
	*posetPosetDagContext
	outputOff    []uint32
	seCtx        stochastikctx.Context
	kvRanges     []ekv.KeyRange
	startTS      uint64
	ignoreLock   bool
	lockChecked  bool
	scanCtx      scanCtx
	idxScanCtx   *idxScanCtx
	selectionCtx selectionCtx
	aggCtx       aggCtx
	topNCtx      *topNCtx

	rowCount int
	unique   bool
	limit    int

	oldChunks []fidelpb.Chunk
	oldRowBuf []byte
	processor closureProcessor

	counts []int64
}

type closureProcessor interface {
	dbreader.ScanProcessor
	Finish() error
}

type scanCtx struct {
	count                    int
	limit                    int
	chk                      *chunk.Chunk
	desc                     bool
	decoder                  *rowcodec.ChunkDecoder
	primaryDeferredCausetIds []int64

	newDefCauslationRd  *rowcodec.BytesDecoder
	newDefCauslationIds map[int64]int
}

type idxScanCtx struct {
	pkStatus                 int
	columnLen                int
	colInfos                 []rowcodec.DefCausInfo
	primaryDeferredCausetIds []int64
}

type aggCtx struct {
	col *fidelpb.DeferredCausetInfo
}

type selectionCtx struct {
	conditions []expression.Expression
}

type topNCtx struct {
	heap         *topNHeap
	orderByExprs []expression.Expression
	sortRow      *sortRow
}

func (e *closureExecutor) execute() ([]fidelpb.Chunk, error) {
	err := e.checkRangeLock()
	if err != nil {
		return nil, errors.Trace(err)
	}
	dbReader := e.dbReader
	for i, ran := range e.kvRanges {
		if e.isPointGetRange(ran) {
			val, err := dbReader.Get(ran.StartKey, e.startTS)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(val) == 0 {
				continue
			}
			if e.counts != nil {
				e.counts[i]++
			}
			err = e.processor.Process(ran.StartKey, val)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			oldCnt := e.rowCount
			if e.scanCtx.desc {
				err = dbReader.ReverseScan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e.processor)
			} else {
				err = dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e.processor)
			}
			delta := int64(e.rowCount - oldCnt)
			if e.counts != nil {
				e.counts[i] += delta
			}
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if e.rowCount == e.limit {
			break
		}
	}
	err = e.processor.Finish()
	return e.oldChunks, err
}

func (e *closureExecutor) isPointGetRange(ran ekv.KeyRange) bool {
	if len(e.primaryDefCauss) > 0 {
		return false
	}
	return e.unique && ran.IsPoint()
}

func (e *closureExecutor) checkRangeLock() error {
	if !e.ignoreLock && !e.lockChecked {
		for _, ran := range e.kvRanges {
			err := e.checkRangeLockForRange(ran)
			if err != nil {
				return err
			}
		}
		e.lockChecked = true
	}
	return nil
}

func (e *closureExecutor) checkRangeLockForRange(ran ekv.KeyRange) error {
	it := e.lockStore.NewIterator()
	for it.Seek(ran.StartKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), ran.EndKey) {
			break
		}
		dagger := mvcc.DecodeLock(it.Value())
		err := checkLock(dagger, it.Key(), e.startTS, e.resolvedLocks)
		if err != nil {
			return err
		}
	}
	return nil
}

type countStarProcessor struct {
	skipVal
	*closureExecutor
}

// countStarProcess is used for `count(*)`.
func (e *countStarProcessor) Process(key, value []byte) error {
	e.rowCount++
	return nil
}

func (e *countStarProcessor) Finish() error {
	return e.countFinish()
}

// countFinish is used for `count(*)`.
func (e *closureExecutor) countFinish() error {
	d := types.NewIntCauset(int64(e.rowCount))
	rowData, err := codec.EncodeValue(e.sc, nil, d)
	if err != nil {
		return errors.Trace(err)
	}
	e.oldChunks = appendRow(e.oldChunks, rowData, 0)
	return nil
}

type countDeferredCausetProcessor struct {
	skipVal
	*closureExecutor
}

func (e *countDeferredCausetProcessor) Process(key, value []byte) error {
	if e.idxScanCtx != nil {
		values, _, err := blockcodec.CutIndexKeyNew(key, e.idxScanCtx.columnLen)
		if err != nil {
			return errors.Trace(err)
		}
		if values[0][0] != codec.NilFlag {
			e.rowCount++
		}
	} else {
		// Since the handle value doesn't affect the count result, we don't need to decode the handle.
		isNull, err := e.scanCtx.decoder.DeferredCausetIsNull(value, e.aggCtx.col.DeferredCausetId, e.aggCtx.col.DefaultVal)
		if err != nil {
			return errors.Trace(err)
		}
		if !isNull {
			e.rowCount++
		}
	}
	return nil
}

func (e *countDeferredCausetProcessor) Finish() error {
	return e.countFinish()
}

type skipVal bool

func (s skipVal) SkipValue() bool {
	return bool(s)
}

type blockScanProcessor struct {
	skipVal
	*closureExecutor
}

func (e *blockScanProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return dbreader.ScanBreak
	}
	e.rowCount++
	err := e.blockScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *blockScanProcessor) Finish() error {
	return e.scanFinish()
}

func (e *closureExecutor) processCore(key, value []byte) error {
	if e.idxScanCtx != nil {
		return e.indexScanProcessCore(key, value)
	}
	return e.blockScanProcessCore(key, value)
}

func (e *closureExecutor) hasSelection() bool {
	return len(e.selectionCtx.conditions) > 0
}

func (e *closureExecutor) processSelection() (gotRow bool, err error) {
	chk := e.scanCtx.chk
	event := chk.GetRow(chk.NumRows() - 1)
	gotRow = true
	for _, expr := range e.selectionCtx.conditions {
		wc := e.sc.WarningCount()
		d, err := expr.Eval(event)
		if err != nil {
			return false, errors.Trace(err)
		}

		if d.IsNull() {
			gotRow = false
		} else {
			isBool, err := d.ToBool(e.sc)
			isBool, err = expression.HandleOverflowOnSelection(e.sc, isBool, err)
			if err != nil {
				return false, errors.Trace(err)
			}
			gotRow = isBool != 0
		}
		if !gotRow {
			if e.sc.WarningCount() > wc {
				// Deep-INTERLOCKy error object here, because the data it referenced is going to be truncated.
				warns := e.sc.TruncateWarnings(int(wc))
				for i, warn := range warns {
					warns[i].Err = e.INTERLOCKyError(warn.Err)
				}
				e.sc.AppendWarnings(warns)
			}
			chk.TruncateTo(chk.NumRows() - 1)
			break
		}
	}
	return
}

func (e *closureExecutor) INTERLOCKyError(err error) error {
	if err == nil {
		return nil
	}
	var ret error
	x := errors.Cause(err)
	switch y := x.(type) {
	case *terror.Error:
		ret = terror.ToALLEGROSQLError(y)
	default:
		ret = errors.New(err.Error())
	}
	return ret
}

func (e *closureExecutor) blockScanProcessCore(key, value []byte) error {
	handle, err := blockcodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.scanCtx.decoder.DecodeToChunk(value, handle, e.scanCtx.chk)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *closureExecutor) scanFinish() error {
	return e.chunkToOldChunk(e.scanCtx.chk)
}

type indexScanProcessor struct {
	skipVal
	*closureExecutor
}

func (e *indexScanProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return dbreader.ScanBreak
	}
	e.rowCount++
	err := e.indexScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *indexScanProcessor) Finish() error {
	return e.scanFinish()
}

func (e *closureExecutor) indexScanProcessCore(key, value []byte) error {
	handleStatus := mapPkStatusToHandleStatus(e.idxScanCtx.pkStatus)
	restoredDefCauss := make([]rowcodec.DefCausInfo, 0, len(e.idxScanCtx.colInfos))
	for _, c := range e.idxScanCtx.colInfos {
		if c.ID != -1 {
			restoredDefCauss = append(restoredDefCauss, c)
		}
	}
	values, err := blockcodec.DecodeIndexKV(key, value, e.idxScanCtx.columnLen, handleStatus, restoredDefCauss)
	if err != nil {
		return err
	}
	chk := e.scanCtx.chk
	decoder := codec.NewDecoder(chk, e.sc.TimeZone)
	for i, colVal := range values {
		if i < len(e.fieldTps) {
			_, err = decoder.DecodeOne(colVal, i, e.fieldTps[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (e *closureExecutor) chunkToOldChunk(chk *chunk.Chunk) error {
	var oldRow []types.Causet
	for i := 0; i < chk.NumRows(); i++ {
		oldRow = oldRow[:0]
		for _, outputOff := range e.outputOff {
			d := chk.GetRow(i).GetCauset(int(outputOff), e.fieldTps[outputOff])
			oldRow = append(oldRow, d)
		}
		var err error
		e.oldRowBuf, err = codec.EncodeValue(e.sc, e.oldRowBuf[:0], oldRow...)
		if err != nil {
			return errors.Trace(err)
		}
		e.oldChunks = appendRow(e.oldChunks, e.oldRowBuf, i)
	}
	chk.Reset()
	return nil
}

type selectionProcessor struct {
	skipVal
	*closureExecutor
}

func (e *selectionProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return dbreader.ScanBreak
	}
	err := e.processCore(key, value)
	if err != nil {
		return errors.Trace(err)
	}
	gotRow, err := e.processSelection()
	if err != nil {
		return err
	}
	if gotRow {
		e.rowCount++
		if e.scanCtx.chk.NumRows() == chunkMaxRows {
			err = e.chunkToOldChunk(e.scanCtx.chk)
		}
	}
	return err
}

func (e *selectionProcessor) Finish() error {
	return e.scanFinish()
}

type topNProcessor struct {
	skipVal
	*closureExecutor
}

func (e *topNProcessor) Process(key, value []byte) (err error) {
	if err = e.processCore(key, value); err != nil {
		return err
	}
	if e.hasSelection() {
		gotRow, err1 := e.processSelection()
		if err1 != nil || !gotRow {
			return err1
		}
	}

	ctx := e.topNCtx
	event := e.scanCtx.chk.GetRow(0)
	for i, expr := range ctx.orderByExprs {
		d, err := expr.Eval(event)
		if err != nil {
			return errors.Trace(err)
		}
		d.INTERLOCKy(&ctx.sortRow.key[i])
	}
	e.scanCtx.chk.Reset()

	if ctx.heap.tryToAddRow(ctx.sortRow) {
		ctx.sortRow.data[0] = safeINTERLOCKy(key)
		ctx.sortRow.data[1] = safeINTERLOCKy(value)
		ctx.sortRow = e.newTopNSortRow()
	}
	return errors.Trace(ctx.heap.err)
}

func (e *closureExecutor) newTopNSortRow() *sortRow {
	return &sortRow{
		key:  make([]types.Causet, len(e.evalContext.columnInfos)),
		data: make([][]byte, 2),
	}
}

func (e *topNProcessor) Finish() error {
	ctx := e.topNCtx
	sort.Sort(&ctx.heap.topNSorter)
	chk := e.scanCtx.chk
	for _, event := range ctx.heap.rows {
		err := e.processCore(event.data[0], event.data[1])
		if err != nil {
			return err
		}
		if chk.NumRows() == chunkMaxRows {
			if err = e.chunkToOldChunk(chk); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return e.chunkToOldChunk(chk)
}

type hashAggProcessor struct {
	skipVal
	*closureExecutor

	aggExprs     []aggregation.Aggregation
	groupByExprs []expression.Expression
	groups       map[string]struct{}
	groupKeys    [][]byte
	aggCtxsMap   map[string][]*aggregation.AggEvaluateContext
}

func (e *hashAggProcessor) Process(key, value []byte) (err error) {
	err = e.processCore(key, value)
	if err != nil {
		return err
	}
	if e.hasSelection() {
		gotRow, err1 := e.processSelection()
		if err1 != nil || !gotRow {
			return err1
		}
	}
	event := e.scanCtx.chk.GetRow(e.scanCtx.chk.NumRows() - 1)
	gk, err := e.getGroupKey(event)
	if _, ok := e.groups[string(gk)]; !ok {
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
	}
	// UFIDelate aggregate expressions.
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		err = agg.UFIDelate(aggCtxs[i], e.sc, event)
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.scanCtx.chk.Reset()
	return nil
}

func (e *hashAggProcessor) getGroupKey(event chunk.Row) ([]byte, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return nil, nil
	}
	key := make([]byte, 0, 32)
	for _, item := range e.groupByExprs {
		v, err := item.Eval(event)
		if err != nil {
			return nil, errors.Trace(err)
		}
		b, err := codec.EncodeValue(e.sc, nil, v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		key = append(key, b...)
	}
	return key, nil
}

func (e *hashAggProcessor) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	aggCtxs, ok := e.aggCtxsMap[string(groupKey)]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			aggCtxs = append(aggCtxs, agg.CreateContext(e.sc))
		}
		e.aggCtxsMap[string(groupKey)] = aggCtxs
	}
	return aggCtxs
}

func (e *hashAggProcessor) Finish() error {
	for i, gk := range e.groupKeys {
		aggCtxs := e.getContexts(gk)
		e.oldRowBuf = e.oldRowBuf[:0]
		for i, agg := range e.aggExprs {
			partialResults := agg.GetPartialResult(aggCtxs[i])
			var err error
			e.oldRowBuf, err = codec.EncodeValue(e.sc, e.oldRowBuf, partialResults...)
			if err != nil {
				return err
			}
		}
		e.oldRowBuf = append(e.oldRowBuf, gk...)
		e.oldChunks = appendRow(e.oldChunks, e.oldRowBuf, i)
	}
	return nil
}

func safeINTERLOCKy(b []byte) []byte {
	return append([]byte{}, b...)
}

func checkLock(dagger mvcc.MvccLock, key []byte, startTS uint64, resolved []uint64) error {
	if isResolved(startTS, resolved) {
		return nil
	}
	lockVisible := dagger.StartTS < startTS
	isWriteLock := dagger.Op == uint8(kvrpcpb.Op_Put) || dagger.Op == uint8(kvrpcpb.Op_Del)
	isPrimaryGet := startTS == math.MaxUint64 && bytes.Equal(dagger.Primary, key)
	if lockVisible && isWriteLock && !isPrimaryGet {
		return BuildLockErr(key, dagger.Primary, dagger.StartTS, uint64(dagger.TTL), dagger.Op)
	}
	return nil
}

func isResolved(startTS uint64, resolved []uint64) bool {
	for _, v := range resolved {
		if startTS == v {
			return true
		}
	}
	return false
}

func exceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
