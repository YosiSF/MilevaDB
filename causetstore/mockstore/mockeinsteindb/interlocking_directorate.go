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

package mockeinsteindb

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var (
	_ executor = &blockScanExec{}
	_ executor = &indexScanExec{}
	_ executor = &selectionExec{}
	_ executor = &limitExec{}
	_ executor = &topNExec{}
)

type execDetail struct {
	timeProcessed   time.Duration
	numProducedRows int
	numIterations   int
}

func (e *execDetail) uFIDelate(begin time.Time, event [][]byte) {
	e.timeProcessed += time.Since(begin)
	e.numIterations++
	if event != nil {
		e.numProducedRows++
	}
}

type executor interface {
	SetSrcExec(executor)
	GetSrcExec() executor
	ResetCounts()
	Counts() []int64
	Next(ctx context.Context) ([][]byte, error)
	// Cursor returns the key gonna to be scanned by the Next() function.
	Cursor() (key []byte, desc bool)
	// ExecDetails returns its and its children's execution details.
	// The order is same as PosetDagRequest.Executors, which children are in front of parents.
	ExecDetails() []*execDetail
}

type blockScanExec struct {
	*fidelpb.TableScan
	colIDs         map[int64]int
	kvRanges       []ekv.KeyRange
	startTS        uint64
	isolationLevel kvrpcpb.IsolationLevel
	resolvedLocks  []uint64
	mvccStore      MVCCStore
	cursor         int
	seekKey        []byte
	start          int
	counts         []int64
	execDetail     *execDetail
	rd             *rowcodec.BytesDecoder

	src executor
}

func (e *blockScanExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *blockScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *blockScanExec) GetSrcExec() executor {
	return e.src
}

func (e *blockScanExec) ResetCounts() {
	if e.counts != nil {
		e.start = e.cursor
		e.counts[e.start] = 0
	}
}

func (e *blockScanExec) Counts() []int64 {
	if e.counts == nil {
		return nil
	}
	if e.seekKey == nil {
		return e.counts[e.start:e.cursor]
	}
	return e.counts[e.start : e.cursor+1]
}

func (e *blockScanExec) Cursor() ([]byte, bool) {
	if len(e.seekKey) > 0 {
		return e.seekKey, e.Desc
	}

	if e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			return ran.StartKey, e.Desc
		}

		if e.Desc {
			return ran.EndKey, e.Desc
		}
		return ran.StartKey, e.Desc
	}

	if e.Desc {
		return e.kvRanges[len(e.kvRanges)-1].StartKey, e.Desc
	}
	return e.kvRanges[len(e.kvRanges)-1].EndKey, e.Desc
}

func (e *blockScanExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.uFIDelate(begin, value)
	}(time.Now())
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() {
			value, err = e.getRowFromPoint(ran)
			if err != nil {
				return nil, errors.Trace(err)
			}
			e.cursor++
			if value == nil {
				continue
			}
			if e.counts != nil {
				e.counts[e.cursor-1]++
			}
			return value, nil
		}
		value, err = e.getRowFromRange(ran)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if value == nil {
			e.seekKey = nil
			e.cursor++
			continue
		}
		if e.counts != nil {
			e.counts[e.cursor]++
		}
		return value, nil
	}

	return nil, nil
}

func (e *blockScanExec) getRowFromPoint(ran ekv.KeyRange) ([][]byte, error) {
	val, err := e.mvccStore.Get(ran.StartKey, e.startTS, e.isolationLevel, e.resolvedLocks)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, nil
	}
	handle, err := blockcodec.DecodeRowKey(ran.StartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	event, err := getRowData(e.DeferredCausets, e.colIDs, handle.IntValue(), val, e.rd)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return event, nil
}

func (e *blockScanExec) getRowFromRange(ran ekv.KeyRange) ([][]byte, error) {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	var pairs []Pair
	var pair Pair
	if e.Desc {
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS, e.isolationLevel, e.resolvedLocks)
	} else {
		pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS, e.isolationLevel, e.resolvedLocks)
	}
	if len(pairs) > 0 {
		pair = pairs[0]
	}
	if pair.Err != nil {
		// TODO: Handle dagger error.
		return nil, errors.Trace(pair.Err)
	}
	if pair.Key == nil {
		return nil, nil
	}
	if e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			return nil, nil
		}
		e.seekKey = blockcodec.TruncateToRowKeyLen(pair.Key)
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			return nil, nil
		}
		e.seekKey = ekv.Key(pair.Key).PrefixNext()
	}

	handle, err := blockcodec.DecodeRowKey(pair.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	event, err := getRowData(e.DeferredCausets, e.colIDs, handle.IntValue(), pair.Value, e.rd)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return event, nil
}

type indexScanExec struct {
	*fidelpb.IndexScan
	defcausLen     int
	kvRanges       []ekv.KeyRange
	startTS        uint64
	isolationLevel kvrpcpb.IsolationLevel
	resolvedLocks  []uint64
	mvccStore      MVCCStore
	cursor         int
	seekKey        []byte
	hdStatus       blockcodec.HandleStatus
	start          int
	counts         []int64
	execDetail     *execDetail
	colInfos       []rowcodec.DefCausInfo

	src executor
}

func (e *indexScanExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *indexScanExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *indexScanExec) GetSrcExec() executor {
	return e.src
}

func (e *indexScanExec) ResetCounts() {
	if e.counts != nil {
		e.start = e.cursor
		e.counts[e.start] = 0
	}
}

func (e *indexScanExec) Counts() []int64 {
	if e.counts == nil {
		return nil
	}
	if e.seekKey == nil {
		return e.counts[e.start:e.cursor]
	}
	return e.counts[e.start : e.cursor+1]
}

func (e *indexScanExec) isUnique() bool {
	return e.Unique != nil && *e.Unique
}

func (e *indexScanExec) Cursor() ([]byte, bool) {
	if len(e.seekKey) > 0 {
		return e.seekKey, e.Desc
	}
	if e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() && e.isUnique() {
			return ran.StartKey, e.Desc
		}
		if e.Desc {
			return ran.EndKey, e.Desc
		}
		return ran.StartKey, e.Desc
	}
	if e.Desc {
		return e.kvRanges[len(e.kvRanges)-1].StartKey, e.Desc
	}
	return e.kvRanges[len(e.kvRanges)-1].EndKey, e.Desc
}

func (e *indexScanExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.uFIDelate(begin, value)
	}(time.Now())
	for e.cursor < len(e.kvRanges) {
		ran := e.kvRanges[e.cursor]
		if ran.IsPoint() && e.isUnique() {
			value, err = e.getRowFromPoint(ran)
			if err != nil {
				return nil, errors.Trace(err)
			}
			e.cursor++
			if value == nil {
				continue
			}
			if e.counts != nil {
				e.counts[e.cursor-1]++
			}
		} else {
			value, err = e.getRowFromRange(ran)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if value == nil {
				e.cursor++
				e.seekKey = nil
				continue
			}
			if e.counts != nil {
				e.counts[e.cursor]++
			}
		}
		return value, nil
	}

	return nil, nil
}

// getRowFromPoint is only used for unique key.
func (e *indexScanExec) getRowFromPoint(ran ekv.KeyRange) ([][]byte, error) {
	val, err := e.mvccStore.Get(ran.StartKey, e.startTS, e.isolationLevel, e.resolvedLocks)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, nil
	}
	return blockcodec.DecodeIndexKV(ran.StartKey, val, e.defcausLen, e.hdStatus, e.colInfos)
}

func (e *indexScanExec) getRowFromRange(ran ekv.KeyRange) ([][]byte, error) {
	if e.seekKey == nil {
		if e.Desc {
			e.seekKey = ran.EndKey
		} else {
			e.seekKey = ran.StartKey
		}
	}
	var pairs []Pair
	var pair Pair
	if e.Desc {
		pairs = e.mvccStore.ReverseScan(ran.StartKey, e.seekKey, 1, e.startTS, e.isolationLevel, e.resolvedLocks)
	} else {
		pairs = e.mvccStore.Scan(e.seekKey, ran.EndKey, 1, e.startTS, e.isolationLevel, e.resolvedLocks)
	}
	if len(pairs) > 0 {
		pair = pairs[0]
	}
	if pair.Err != nil {
		// TODO: Handle dagger error.
		return nil, errors.Trace(pair.Err)
	}
	if pair.Key == nil {
		return nil, nil
	}
	if e.Desc {
		if bytes.Compare(pair.Key, ran.StartKey) < 0 {
			return nil, nil
		}
		e.seekKey = pair.Key
	} else {
		if bytes.Compare(pair.Key, ran.EndKey) >= 0 {
			return nil, nil
		}
		e.seekKey = ekv.Key(pair.Key).PrefixNext()
	}
	return blockcodec.DecodeIndexKV(pair.Key, pair.Value, e.defcausLen, e.hdStatus, e.colInfos)
}

type selectionExec struct {
	conditions            []expression.Expression
	relatedDefCausOffsets []int
	event                 []types.Causet
	evalCtx               *evalContext
	src                   executor
	execDetail            *execDetail
}

func (e *selectionExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *selectionExec) SetSrcExec(exec executor) {
	e.src = exec
}

func (e *selectionExec) GetSrcExec() executor {
	return e.src
}

func (e *selectionExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *selectionExec) Counts() []int64 {
	return e.src.Counts()
}

// evalBool evaluates expression to a boolean value.
func evalBool(exprs []expression.Expression, event []types.Causet, ctx *stmtctx.StatementContext) (bool, error) {
	for _, expr := range exprs {
		data, err := expr.Eval(chunk.MutRowFromCausets(event).ToRow())
		if err != nil {
			return false, errors.Trace(err)
		}
		if data.IsNull() {
			return false, nil
		}

		isBool, err := data.ToBool(ctx)
		isBool, err = expression.HandleOverflowOnSelection(ctx, isBool, err)
		if err != nil {
			return false, errors.Trace(err)
		}
		if isBool == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (e *selectionExec) Cursor() ([]byte, bool) {
	return e.src.Cursor()
}

func (e *selectionExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.uFIDelate(begin, value)
	}(time.Now())
	for {
		value, err = e.src.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if value == nil {
			return nil, nil
		}

		err = e.evalCtx.decodeRelatedDeferredCausetVals(e.relatedDefCausOffsets, value, e.event)
		if err != nil {
			return nil, errors.Trace(err)
		}
		match, err := evalBool(e.conditions, e.event, e.evalCtx.sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return value, nil
		}
	}
}

type topNExec struct {
	heap                  *topNHeap
	evalCtx               *evalContext
	relatedDefCausOffsets []int
	orderByExprs          []expression.Expression
	event                 []types.Causet
	cursor                int
	executed              bool
	execDetail            *execDetail

	src executor
}

func (e *topNExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *topNExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *topNExec) GetSrcExec() executor {
	return e.src
}

func (e *topNExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *topNExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *topNExec) innerNext(ctx context.Context) (bool, error) {
	value, err := e.src.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}
	err = e.evalTopN(value)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *topNExec) Cursor() ([]byte, bool) {
	panic("don't not use interlock streaming API for topN!")
}

func (e *topNExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.uFIDelate(begin, value)
	}(time.Now())
	if !e.executed {
		for {
			hasMore, err := e.innerNext(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				sort.Sort(&e.heap.topNSorter)
				break
			}
		}
		e.executed = true
	}
	if e.cursor >= len(e.heap.rows) {
		return nil, nil
	}
	event := e.heap.rows[e.cursor]
	e.cursor++

	return event.data, nil
}

// evalTopN evaluates the top n elements from the data. The input receives a record including its handle and data.
// And this function will check if this record can replace one of the old records.
func (e *topNExec) evalTopN(value [][]byte) error {
	newRow := &sortRow{
		key: make([]types.Causet, len(e.orderByExprs)),
	}
	err := e.evalCtx.decodeRelatedDeferredCausetVals(e.relatedDefCausOffsets, value, e.event)
	if err != nil {
		return errors.Trace(err)
	}
	for i, expr := range e.orderByExprs {
		newRow.key[i], err = expr.Eval(chunk.MutRowFromCausets(e.event).ToRow())
		if err != nil {
			return errors.Trace(err)
		}
	}

	if e.heap.tryToAddRow(newRow) {
		newRow.data = append(newRow.data, value...)
	}
	return errors.Trace(e.heap.err)
}

type limitExec struct {
	limit  uint64
	cursor uint64

	src executor

	execDetail *execDetail
}

func (e *limitExec) ExecDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.ExecDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *limitExec) SetSrcExec(src executor) {
	e.src = src
}

func (e *limitExec) GetSrcExec() executor {
	return e.src
}

func (e *limitExec) ResetCounts() {
	e.src.ResetCounts()
}

func (e *limitExec) Counts() []int64 {
	return e.src.Counts()
}

func (e *limitExec) Cursor() ([]byte, bool) {
	return e.src.Cursor()
}

func (e *limitExec) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.uFIDelate(begin, value)
	}(time.Now())
	if e.cursor >= e.limit {
		return nil, nil
	}

	value, err = e.src.Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		return nil, nil
	}
	e.cursor++
	return value, nil
}

func hasDefCausVal(data [][]byte, colIDs map[int64]int, id int64) bool {
	offset, ok := colIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}

// getRowData decodes raw byte slice to event data.
func getRowData(columns []*fidelpb.DeferredCausetInfo, colIDs map[int64]int, handle int64, value []byte, rd *rowcodec.BytesDecoder) ([][]byte, error) {
	if rowcodec.IsNewFormat(value) {
		return rd.DecodeToBytes(colIDs, ekv.IntHandle(handle), value, nil)
	}
	values, err := blockcodec.CutRowNew(value, colIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if values == nil {
		values = make([][]byte, len(colIDs))
	}
	// Fill the handle and null columns.
	for _, col := range columns {
		id := col.GetDeferredCausetId()
		offset := colIDs[id]
		if col.GetPkHandle() || id == perceptron.ExtraHandleID {
			var handleCauset types.Causet
			if allegrosql.HasUnsignedFlag(uint(col.GetFlag())) {
				// PK column is Unsigned.
				handleCauset = types.NewUintCauset(uint64(handle))
			} else {
				handleCauset = types.NewIntCauset(handle)
			}
			handleData, err1 := codec.EncodeValue(nil, nil, handleCauset)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			values[offset] = handleData
			continue
		}
		if hasDefCausVal(values, colIDs, id) {
			continue
		}
		if len(col.DefaultVal) > 0 {
			values[offset] = col.DefaultVal
			continue
		}
		if allegrosql.HasNotNullFlag(uint(col.GetFlag())) {
			return nil, errors.Errorf("Miss column %d", id)
		}

		values[offset] = []byte{codec.NilFlag}
	}

	return values, nil
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
