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
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/collate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/rowcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

func (h *rpcHandler) handleINTERLOCKAnalyzeRequest(req *interlock.Request) *interlock.Response {
	resp := &interlock.Response{}
	if len(req.Ranges) == 0 {
		return resp
	}
	if req.GetTp() != ekv.ReqTypeAnalyze {
		return resp
	}
	if err := h.checkRequestContext(req.GetContext()); err != nil {
		resp.RegionError = err
		return resp
	}
	analyzeReq := new(fidelpb.AnalyzeReq)
	err := proto.Unmarshal(req.Data, analyzeReq)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	if analyzeReq.Tp == fidelpb.AnalyzeType_TypeIndex {
		resp, err = h.handleAnalyzeIndexReq(req, analyzeReq)
	} else {
		resp, err = h.handleAnalyzeDeferredCausetsReq(req, analyzeReq)
	}
	if err != nil {
		resp.OtherError = err.Error()
	}
	return resp
}

func (h *rpcHandler) handleAnalyzeIndexReq(req *interlock.Request, analyzeReq *fidelpb.AnalyzeReq) (*interlock.Response, error) {
	ranges, err := h.extractKVRanges(req.Ranges, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	startTS := req.StartTs
	if startTS == 0 {
		startTS = analyzeReq.GetStartTsFallback()
	}
	e := &indexScanExec{
		defcausLen:     int(analyzeReq.IdxReq.NumDeferredCausets),
		kvRanges:       ranges,
		startTS:        startTS,
		isolationLevel: h.isolationLevel,
		mvsr-oocStore:      h.mvsr-oocStore,
		IndexScan:      &fidelpb.IndexScan{Desc: false},
		execDetail:     new(execDetail),
		hdStatus:       blockcodec.HandleNotNeeded,
	}
	statsBuilder := statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(allegrosql.TypeBlob))
	var cms *statistics.CMSketch
	if analyzeReq.IdxReq.CmsketchDepth != nil && analyzeReq.IdxReq.CmsketchWidth != nil {
		cms = statistics.NewCMSketch(*analyzeReq.IdxReq.CmsketchDepth, *analyzeReq.IdxReq.CmsketchWidth)
	}
	ctx := context.TODO()
	var values [][]byte
	for {
		values, err = e.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if values == nil {
			break
		}
		var value []byte
		for _, val := range values {
			value = append(value, val...)
			if cms != nil {
				cms.InsertBytes(value)
			}
		}
		err = statsBuilder.Iterate(types.NewBytesCauset(value))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	hg := statistics.HistogramToProto(statsBuilder.Hist())
	var cm *fidelpb.CMSketch
	if cms != nil {
		cm = statistics.CMSketchToProto(cms)
	}
	data, err := proto.Marshal(&fidelpb.AnalyzeIndexResp{Hist: hg, Cms: cm})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &interlock.Response{Data: data}, nil
}

type analyzeDeferredCausetsExec struct {
	tblExec *blockScanExec
	fields  []*ast.ResultField
}

func (h *rpcHandler) handleAnalyzeDeferredCausetsReq(req *interlock.Request, analyzeReq *fidelpb.AnalyzeReq) (_ *interlock.Response, err error) {
	sc := flagsToStatementContext(analyzeReq.Flags)
	sc.TimeZone, err = constructTimeZone("", int(analyzeReq.TimeZoneOffset))
	if err != nil {
		return nil, errors.Trace(err)
	}

	evalCtx := &evalContext{sc: sc}
	columns := analyzeReq.DefCausReq.DeferredCausetsInfo
	evalCtx.setDeferredCausetInfo(columns)
	ranges, err := h.extractKVRanges(req.Ranges, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	startTS := req.StartTs
	if startTS == 0 {
		startTS = analyzeReq.GetStartTsFallback()
	}
	colInfos := make([]rowcodec.DefCausInfo, len(columns))
	for i := range columns {
		col := columns[i]
		colInfos[i] = rowcodec.DefCausInfo{
			ID:         col.DeferredCausetId,
			Ft:         evalCtx.fieldTps[i],
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
	e := &analyzeDeferredCausetsExec{
		tblExec: &blockScanExec{
			TableScan:      &fidelpb.TableScan{DeferredCausets: columns},
			kvRanges:       ranges,
			colIDs:         evalCtx.colIDs,
			startTS:        startTS,
			isolationLevel: h.isolationLevel,
			mvsr-oocStore:      h.mvsr-oocStore,
			execDetail:     new(execDetail),
			rd:             rd,
		},
	}
	e.fields = make([]*ast.ResultField, len(columns))
	for i := range e.fields {
		rf := new(ast.ResultField)
		rf.DeferredCauset = new(perceptron.DeferredCausetInfo)
		rf.DeferredCauset.FieldType = types.FieldType{Tp: allegrosql.TypeBlob, Flen: allegrosql.MaxBlobWidth, Charset: allegrosql.DefaultCharset, DefCauslate: allegrosql.DefaultDefCauslationName}
		e.fields[i] = rf
	}

	pkID := int64(-1)
	numDefCauss := len(columns)
	if columns[0].GetPkHandle() {
		pkID = columns[0].DeferredCausetId
		columns = columns[1:]
		numDefCauss--
	}
	collators := make([]collate.DefCauslator, numDefCauss)
	fts := make([]*types.FieldType, numDefCauss)
	for i, col := range columns {
		ft := fieldTypeFromPBDeferredCauset(col)
		fts[i] = ft
		if ft.EvalType() == types.ETString {
			collators[i] = collate.GetDefCauslator(ft.DefCauslate)
		}
	}
	colReq := analyzeReq.DefCausReq
	builder := statistics.SampleBuilder{
		Sc:                sc,
		RecordSet:         e,
		DefCausLen:        numDefCauss,
		MaxBucketSize:     colReq.BucketSize,
		MaxFMSketchSize:   colReq.SketchSize,
		MaxSampleSize:     colReq.SampleSize,
		DefCauslators:     collators,
		DefCaussFieldType: fts,
	}
	if pkID != -1 {
		builder.PkBuilder = statistics.NewSortedBuilder(sc, builder.MaxBucketSize, pkID, types.NewFieldType(allegrosql.TypeBlob))
	}
	if colReq.CmsketchWidth != nil && colReq.CmsketchDepth != nil {
		builder.CMSketchWidth = *colReq.CmsketchWidth
		builder.CMSketchDepth = *colReq.CmsketchDepth
	}
	collectors, pkBuilder, err := builder.DefCauslectDeferredCausetStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	colResp := &fidelpb.AnalyzeDeferredCausetsResp{}
	if pkID != -1 {
		colResp.PkHist = statistics.HistogramToProto(pkBuilder.Hist())
	}
	for _, c := range collectors {
		colResp.DefCauslectors = append(colResp.DefCauslectors, statistics.SampleDefCauslectorToProto(c))
	}
	data, err := proto.Marshal(colResp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &interlock.Response{Data: data}, nil
}

// Fields implements the sqlexec.RecordSet Fields interface.
func (e *analyzeDeferredCausetsExec) Fields() []*ast.ResultField {
	return e.fields
}

func (e *analyzeDeferredCausetsExec) getNext(ctx context.Context) ([]types.Causet, error) {
	values, err := e.tblExec.Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if values == nil {
		return nil, nil
	}
	datumRow := make([]types.Causet, 0, len(values))
	for _, val := range values {
		d := types.NewBytesCauset(val)
		if len(val) == 1 && val[0] == codec.NilFlag {
			d.SetNull()
		}
		datumRow = append(datumRow, d)
	}
	return datumRow, nil
}

func (e *analyzeDeferredCausetsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	event, err := e.getNext(ctx)
	if event == nil || err != nil {
		return errors.Trace(err)
	}
	for i := 0; i < len(event); i++ {
		req.AppendCauset(i, &event[i])
	}
	return nil
}

func (e *analyzeDeferredCausetsExec) NewChunk() *chunk.Chunk {
	fields := make([]*types.FieldType, 0, len(e.fields))
	for _, field := range e.fields {
		fields = append(fields, &field.DeferredCauset.FieldType)
	}
	return chunk.NewChunkWithCapacity(fields, 1)
}

// Close implements the sqlexec.RecordSet Close interface.
func (e *analyzeDeferredCausetsExec) Close() error {
	return nil
}
