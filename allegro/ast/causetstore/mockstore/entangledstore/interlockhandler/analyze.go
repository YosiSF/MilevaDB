// INTERLOCKyright 2020-present WHTCORPS INC, Inc.
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
	"math"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/entangledstore/einsteindb/dbreader"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/collate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/rowcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/badger/y"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"golang.org/x/net/context"
)

// handleINTERLOCKAnalyzeRequest handles interlock analyze request.
func handleINTERLOCKAnalyzeRequest(dbReader *dbreader.DBReader, req *interlock.Request) *interlock.Response {
	resp := &interlock.Response{}
	if len(req.Ranges) == 0 {
		return resp
	}
	if req.GetTp() != ekv.ReqTypeAnalyze {
		return resp
	}
	analyzeReq := new(fidelpb.AnalyzeReq)
	err := proto.Unmarshal(req.Data, analyzeReq)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	ranges, err := extractKVRanges(dbReader.StartKey, dbReader.EndKey, req.Ranges, false)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	y.Assert(len(ranges) >= 1)
	if analyzeReq.Tp == fidelpb.AnalyzeType_TypeIndex {
		resp, err = handleAnalyzeIndexReq(dbReader, ranges, analyzeReq, req.StartTs)
	} else if analyzeReq.Tp == fidelpb.AnalyzeType_TypeCommonHandle {
		resp, err = handleAnalyzeCommonHandleReq(dbReader, ranges, analyzeReq, req.StartTs)
	} else {
		resp, err = handleAnalyzeDeferredCausetsReq(dbReader, ranges, analyzeReq, req.StartTs)
	}
	if err != nil {
		resp = &interlock.Response{
			OtherError: err.Error(),
		}
	}
	return resp
}

func handleAnalyzeIndexReq(dbReader *dbreader.DBReader, rans []ekv.KeyRange, analyzeReq *fidelpb.AnalyzeReq, startTS uint64) (*interlock.Response, error) {
	processor := &analyzeIndexProcessor{
		colLen:       int(analyzeReq.IdxReq.NumDeferredCausets),
		statsBuilder: statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(allegrosql.TypeBlob)),
	}
	if analyzeReq.IdxReq.CmsketchDepth != nil && analyzeReq.IdxReq.CmsketchWidth != nil {
		processor.cms = statistics.NewCMSketch(*analyzeReq.IdxReq.CmsketchDepth, *analyzeReq.IdxReq.CmsketchWidth)
	}
	for _, ran := range rans {
		err := dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, startTS, processor)
		if err != nil {
			return nil, err
		}
	}
	hg := statistics.HistogramToProto(processor.statsBuilder.Hist())
	var cm *fidelpb.CMSketch
	if processor.cms != nil {
		cm = statistics.CMSketchToProto(processor.cms)
	}
	data, err := proto.Marshal(&fidelpb.AnalyzeIndexResp{Hist: hg, Cms: cm})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &interlock.Response{Data: data}, nil
}

func handleAnalyzeCommonHandleReq(dbReader *dbreader.DBReader, rans []ekv.KeyRange, analyzeReq *fidelpb.AnalyzeReq, startTS uint64) (*interlock.Response, error) {
	processor := &analyzeCommonHandleProcessor{
		colLen:       int(analyzeReq.IdxReq.NumDeferredCausets),
		statsBuilder: statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(allegrosql.TypeBlob)),
	}
	if analyzeReq.IdxReq.CmsketchDepth != nil && analyzeReq.IdxReq.CmsketchWidth != nil {
		processor.cms = statistics.NewCMSketch(*analyzeReq.IdxReq.CmsketchDepth, *analyzeReq.IdxReq.CmsketchWidth)
	}
	for _, ran := range rans {
		err := dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, startTS, processor)
		if err != nil {
			return nil, err
		}
	}
	hg := statistics.HistogramToProto(processor.statsBuilder.Hist())
	var cm *fidelpb.CMSketch
	if processor.cms != nil {
		cm = statistics.CMSketchToProto(processor.cms)
	}
	data, err := proto.Marshal(&fidelpb.AnalyzeIndexResp{Hist: hg, Cms: cm})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &interlock.Response{Data: data}, nil
}

type analyzeIndexProcessor struct {
	skipVal

	colLen       int
	statsBuilder *statistics.SortedBuilder
	cms          *statistics.CMSketch
	rowBuf       []byte
}

func (p *analyzeIndexProcessor) Process(key, value []byte) error {
	values, _, err := blockcodec.CutIndexKeyNew(key, p.colLen)
	if err != nil {
		return err
	}
	p.rowBuf = p.rowBuf[:0]
	for _, val := range values {
		p.rowBuf = append(p.rowBuf, val...)
		if p.cms != nil {
			p.cms.InsertBytes(p.rowBuf)
		}
	}
	rowData := safeINTERLOCKy(p.rowBuf)
	err = p.statsBuilder.Iterate(types.NewBytesCauset(rowData))
	if err != nil {
		return err
	}
	return nil
}

type analyzeCommonHandleProcessor struct {
	skipVal

	colLen       int
	statsBuilder *statistics.SortedBuilder
	cms          *statistics.CMSketch
	rowBuf       []byte
}

func (p *analyzeCommonHandleProcessor) Process(key, value []byte) error {
	values, _, err := blockcodec.CutCommonHandle(key, p.colLen)
	if err != nil {
		return err
	}
	p.rowBuf = p.rowBuf[:0]
	for _, val := range values {
		p.rowBuf = append(p.rowBuf, val...)
		if p.cms != nil {
			p.cms.InsertBytes(p.rowBuf)
		}
	}
	rowData := safeINTERLOCKy(p.rowBuf)
	err = p.statsBuilder.Iterate(types.NewBytesCauset(rowData))
	if err != nil {
		return err
	}
	return nil
}

type analyzeDeferredCausetsExec struct {
	skipVal
	reader  *dbreader.DBReader
	ranges  []ekv.KeyRange
	curRan  int
	seekKey []byte
	endKey  []byte
	startTS uint64

	chk     *chunk.Chunk
	decoder *rowcodec.ChunkDecoder
	req     *chunk.Chunk
	evalCtx *evalContext
	fields  []*ast.ResultField
}

func handleAnalyzeDeferredCausetsReq(dbReader *dbreader.DBReader, rans []ekv.KeyRange, analyzeReq *fidelpb.AnalyzeReq, startTS uint64) (*interlock.Response, error) {
	sc := flagsToStatementContext(analyzeReq.Flags)
	sc.TimeZone = time.FixedZone("UTC", int(analyzeReq.TimeZoneOffset))
	evalCtx := &evalContext{sc: sc}
	columns := analyzeReq.DefCausReq.DeferredCausetsInfo
	evalCtx.setDeferredCausetInfo(columns)
	if len(analyzeReq.DefCausReq.PrimaryDeferredCausetIds) > 0 {
		evalCtx.primaryDefCauss = analyzeReq.DefCausReq.PrimaryDeferredCausetIds
	}
	decoder, err := evalCtx.newRowDecoder()
	if err != nil {
		return nil, err
	}
	e := &analyzeDeferredCausetsExec{
		reader:  dbReader,
		seekKey: rans[0].StartKey,
		endKey:  rans[0].EndKey,
		ranges:  rans,
		curRan:  0,
		startTS: startTS,
		chk:     chunk.NewChunkWithCapacity(evalCtx.fieldTps, 1),
		decoder: decoder,
		evalCtx: evalCtx,
	}
	e.fields = make([]*ast.ResultField, len(columns))
	for i := range e.fields {
		rf := new(ast.ResultField)
		rf.DeferredCauset = new(perceptron.DeferredCausetInfo)
		rf.DeferredCauset.FieldType = types.FieldType{Tp: allegrosql.TypeBlob, Flen: allegrosql.MaxBlobWidth, Charset: charset.CharsetUTF8, DefCauslate: charset.DefCauslationUTF8}
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

func (e *analyzeDeferredCausetsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	e.req = req
	err := e.reader.Scan(e.seekKey, e.endKey, math.MaxInt64, e.startTS, e)
	if err != nil {
		return err
	}
	if req.NumRows() < req.Capacity() {
		if e.curRan == len(e.ranges)-1 {
			e.seekKey = e.endKey
		} else {
			e.curRan++
			e.seekKey = e.ranges[e.curRan].StartKey
			e.endKey = e.ranges[e.curRan].EndKey
		}
	}
	return nil
}

func (e *analyzeDeferredCausetsExec) Process(key, value []byte) error {
	handle, err := blockcodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.decoder.DecodeToChunk(value, handle, e.chk)
	if err != nil {
		return errors.Trace(err)
	}
	event := e.chk.GetRow(0)
	for i, tp := range e.evalCtx.fieldTps {
		d := event.GetCauset(i, tp)
		if d.IsNull() {
			e.req.AppendNull(i)
			continue
		}

		value, err := blockcodec.EncodeValue(e.evalCtx.sc, nil, d)
		if err != nil {
			return err
		}
		e.req.AppendBytes(i, value)
	}
	e.chk.Reset()
	if e.req.NumRows() == e.req.Capacity() {
		e.seekKey = ekv.Key(key).PrefixNext()
		return dbreader.ScanBreak
	}
	return nil
}

func (e *analyzeDeferredCausetsExec) NewChunk() *chunk.Chunk {
	fields := make([]*types.FieldType, 0, len(e.fields))
	for _, field := range e.fields {
		fields = append(fields, &field.DeferredCauset.FieldType)
	}
	return chunk.NewChunkWithCapacity(fields, 1024)
}

// Close implements the sqlexec.RecordSet Close interface.
func (e *analyzeDeferredCausetsExec) Close() error {
	return nil
}
