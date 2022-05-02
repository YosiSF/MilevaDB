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

package aggfuncs_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/dgryski/go-farm"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/executor/aggfuncs"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression/aggregation"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/replog"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/set"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
)

var _ = Suite(&testSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	TestingT(t)
}

type testSuite struct {
	*berolinaAllegroSQL.berolinaAllegroSQL
	ctx         stochastikctx.Context
	cluster     cluster.Cluster
	causetstore ekv.CausetStorage
	petri       *petri.Petri
}

func (s *testSuite) SetUpSuite(c *C) {
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	s.ctx = mock.NewContext()
	s.ctx.GetStochaseinstein_dbars().StmtCtx.TimeZone = time.Local
	causetstore, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	d, err := stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	d.SetStatsUFIDelating(true)
	s.petri = d
}

func (s *testSuite) TearDownSuite(c *C) {
}

func (s *testSuite) SetUpTest(c *C) {
	s.ctx.GetStochaseinstein_dbars().PlanDeferredCausetID = 0
}

func (s *testSuite) TearDownTest(c *C) {
	s.ctx.GetStochaseinstein_dbars().StmtCtx.SetWarnings(nil)
}

type aggTest struct {
	dataType  *types.FieldType
	numEvents int
	dataGen   func(i int) types.Causet
	funcName  string
	results   []types.Causet
	orderBy   bool
}

func (p *aggTest) genSrcChk() *chunk.Chunk {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, p.numEvents)
	for i := 0; i < p.numEvents; i++ {
		dt := p.dataGen(i)
		srcChk.AppendCauset(0, &dt)
	}
	srcChk.AppendCauset(0, &types.Causet{})
	return srcChk
}

// messUpChunk messes up the chunk for testing memory reference.
func (p *aggTest) messUpChunk(c *chunk.Chunk) {
	for i := 0; i < p.numEvents; i++ {
		raw := c.DeferredCauset(0).GetRaw(i)
		for i := range raw {
			raw[i] = 255
		}
	}
}

type multiArgsAggTest struct {
	dataTypes []*types.FieldType
	retType   *types.FieldType
	numEvents int
	dataGens  []func(i int) types.Causet
	funcName  string
	results   []types.Causet
	orderBy   bool
}

func (p *multiArgsAggTest) genSrcChk() *chunk.Chunk {
	srcChk := chunk.NewChunkWithCapacity(p.dataTypes, p.numEvents)
	for i := 0; i < p.numEvents; i++ {
		for j := 0; j < len(p.dataGens); j++ {
			fdt := p.dataGens[j](i)
			srcChk.AppendCauset(j, &fdt)
		}
	}
	srcChk.AppendCauset(0, &types.Causet{})
	return srcChk
}

// messUpChunk messes up the chunk for testing memory reference.
func (p *multiArgsAggTest) messUpChunk(c *chunk.Chunk) {
	for i := 0; i < p.numEvents; i++ {
		for j := 0; j < len(p.dataGens); j++ {
			raw := c.DeferredCauset(j).GetRaw(i)
			for i := range raw {
				raw[i] = 255
			}
		}
	}
}

type uFIDelateMemDeltaGens func(*chunk.Chunk, *types.FieldType) (memDeltas []int64, err error)

func defaultUFIDelateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumEvents(); i++ {
		memDeltas = append(memDeltas, int64(0))
	}
	return memDeltas, nil
}

func approxCountDistinctUFIDelateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)

	buf := make([]byte, 8)
	p := aggfuncs.NewPartialResult4ApproxCountDistinct()
	for i := 0; i < srcChk.NumEvents(); i++ {
		event := srcChk.GetEvent(i)
		if event.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		oldMemUsage := p.MemUsage()
		switch dataType.Tp {
		case allegrosql.TypeLonglong:
			val := event.GetInt64(0)
			*(*int64)(unsafe.Pointer(&buf[0])) = val
		case allegrosql.TypeString:
			val := event.GetString(0)
			buf = codec.EncodeCompactBytes(buf, replog.Slice(val))
		default:
			return memDeltas, errors.Errorf("unsupported type - %v", dataType.Tp)
		}

		x := farm.Hash64(buf)
		p.InsertHash64(x)
		newMemUsage := p.MemUsage()
		memDelta := newMemUsage - oldMemUsage
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func distinctUFIDelateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	valSet := set.NewStringSet()
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumEvents(); i++ {
		event := srcChk.GetEvent(i)
		if event.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		val := ""
		memDelta := int64(0)
		switch dataType.Tp {
		case allegrosql.TypeLonglong:
			val = strconv.FormatInt(event.GetInt64(0), 10)
			memDelta = aggfuncs.DefInt64Size
		case allegrosql.TypeFloat:
			val = strconv.FormatFloat(float64(event.GetFloat32(0)), 'f', 6, 64)
			memDelta = aggfuncs.DefFloat64Size
		case allegrosql.TypeDouble:
			val = strconv.FormatFloat(event.GetFloat64(0), 'f', 6, 64)
			memDelta = aggfuncs.DefFloat64Size
		case allegrosql.TypeNewDecimal:
			decimal := event.GetMyDecimal(0)
			hash, err := decimal.ToHashKey()
			if err != nil {
				memDeltas = append(memDeltas, int64(0))
				continue
			}
			val = string(replog.String(hash))
			memDelta = int64(len(val))
		case allegrosql.TypeString:
			val = event.GetString(0)
			memDelta = int64(len(val))
		case allegrosql.TypeDate:
			val = event.GetTime(0).String()
			memDelta = aggfuncs.DefTimeSize
		case allegrosql.TypeDuration:
			val = strconv.FormatInt(event.GetInt64(0), 10)
			memDelta = aggfuncs.DefInt64Size
		case allegrosql.TypeJSON:
			jsonVal := event.GetJSON(0)
			bytes := make([]byte, 0)
			bytes = append(bytes, jsonVal.TypeCode)
			bytes = append(bytes, jsonVal.Value...)
			val = string(bytes)
			memDelta = int64(len(val))
		default:
			return memDeltas, errors.Errorf("unsupported type - %v", dataType.Tp)
		}
		if valSet.Exist(val) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		valSet.Insert(val)
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func rowMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumEvents(); i++ {
		memDelta := aggfuncs.DefEventSize
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

type aggMemTest struct {
	aggTest               aggTest
	allocMemDelta         int64
	uFIDelateMemDeltaGens uFIDelateMemDeltaGens
	isDistinct            bool
}

func builPosetDaggMemTester(funcName string, tp byte, numEvents int, allocMemDelta int64, uFIDelateMemDeltaGens uFIDelateMemDeltaGens, isDistinct bool) aggMemTest {
	aggTest := builPosetDaggTester(funcName, tp, numEvents)
	pt := aggMemTest{
		aggTest:               aggTest,
		allocMemDelta:         allocMemDelta,
		uFIDelateMemDeltaGens: uFIDelateMemDeltaGens,
		isDistinct:            isDistinct,
	}
	return pt
}

func (s *testSuite) testMergePartialResult(c *C, p aggTest) {
	srcChk := p.genSrcChk()
	iter := chunk.NewIterator4Chunk(srcChk)

	args := []expression.Expression{&expression.DeferredCauset{RetType: p.dataType, Index: 0}}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.CouplingConstantWithRadix{Value: types.NewStringCauset(" "), RetType: types.NewFieldType(allegrosql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	partialDesc, finalDesc := desc.Split([]int{0, 1})

	// build partial func for partial phase.
	partialFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialResult, _ := partialFunc.AllocPartialResult()

	// build final func for final phase.
	finalFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, 1)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		resultChk = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 1)
	}

	// uFIDelate partial result.
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		partialFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, partialResult)
	}
	p.messUpChunk(srcChk)
	partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	dt := resultChk.GetEvent(0).GetCauset(0, p.dataType)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		dt = resultChk.GetEvent(0).GetCauset(0, types.NewFieldType(allegrosql.TypeString))
	}
	result, err := dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[0]))

	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)
	partialFunc.ResetPartialResult(partialResult)

	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	iter.Begin()
	iter.Next()
	for event := iter.Next(); event != iter.End(); event = iter.Next() {
		partialFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, partialResult)
	}
	p.messUpChunk(srcChk)
	resultChk.Reset()
	partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	dt = resultChk.GetEvent(0).GetCauset(0, p.dataType)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		dt = resultChk.GetEvent(0).GetCauset(0, types.NewFieldType(allegrosql.TypeString))
	}
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))
	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)

	if p.funcName == ast.AggFuncApproxCountDistinct {
		resultChk = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}, 1)
	}
	resultChk.Reset()
	err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)

	dt = resultChk.GetEvent(0).GetCauset(0, p.dataType)
	if p.funcName == ast.AggFuncApproxCountDistinct {
		dt = resultChk.GetEvent(0).GetCauset(0, types.NewFieldType(allegrosql.TypeLonglong))
	}
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[2])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[2]))
}

func builPosetDaggTester(funcName string, tp byte, numEvents int, results ...interface{}) aggTest {
	return builPosetDaggTesterWithFieldType(funcName, types.NewFieldType(tp), numEvents, results...)
}

func builPosetDaggTesterWithFieldType(funcName string, ft *types.FieldType, numEvents int, results ...interface{}) aggTest {
	pt := aggTest{
		dataType:  ft,
		numEvents: numEvents,
		funcName:  funcName,
		dataGen:   getDataGenFunc(ft),
	}
	for _, result := range results {
		pt.results = append(pt.results, types.NewCauset(result))
	}
	return pt
}

func (s *testSuite) testMultiArgsMergePartialResult(c *C, p multiArgsAggTest) {
	srcChk := p.genSrcChk()
	iter := chunk.NewIterator4Chunk(srcChk)

	args := make([]expression.Expression, len(p.dataTypes))
	for k := 0; k < len(p.dataTypes); k++ {
		args[k] = &expression.DeferredCauset{RetType: p.dataTypes[k], Index: k}
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	partialDesc, finalDesc := desc.Split([]int{0, 1})

	// build partial func for partial phase.
	partialFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialResult, _ := partialFunc.AllocPartialResult()

	// build final func for final phase.
	finalFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.retType}, 1)

	// uFIDelate partial result.
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		partialFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, partialResult)
	}
	p.messUpChunk(srcChk)
	partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	dt := resultChk.GetEvent(0).GetCauset(0, p.retType)
	result, err := dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)

	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)
	partialFunc.ResetPartialResult(partialResult)

	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	iter.Begin()
	iter.Next()
	for event := iter.Next(); event != iter.End(); event = iter.Next() {
		partialFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, partialResult)
	}
	p.messUpChunk(srcChk)
	resultChk.Reset()
	partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	dt = resultChk.GetEvent(0).GetCauset(0, p.retType)
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)
	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)

	dt = resultChk.GetEvent(0).GetCauset(0, p.retType)
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[2])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)
}

// for multiple args in aggfuncs such as json_objectagg(c1, c2)
func buildMultiArgsAggTester(funcName string, tps []byte, rt byte, numEvents int, results ...interface{}) multiArgsAggTest {
	fts := make([]*types.FieldType, len(tps))
	for i := 0; i < len(tps); i++ {
		fts[i] = types.NewFieldType(tps[i])
	}
	return buildMultiArgsAggTesterWithFieldType(funcName, fts, types.NewFieldType(rt), numEvents, results...)
}

func buildMultiArgsAggTesterWithFieldType(funcName string, fts []*types.FieldType, rt *types.FieldType, numEvents int, results ...interface{}) multiArgsAggTest {
	dataGens := make([]func(i int) types.Causet, len(fts))
	for i := 0; i < len(fts); i++ {
		dataGens[i] = getDataGenFunc(fts[i])
	}
	mt := multiArgsAggTest{
		dataTypes: fts,
		retType:   rt,
		numEvents: numEvents,
		funcName:  funcName,
		dataGens:  dataGens,
	}
	for _, result := range results {
		mt.results = append(mt.results, types.NewCauset(result))
	}
	return mt
}

func getDataGenFunc(ft *types.FieldType) func(i int) types.Causet {
	switch ft.Tp {
	case allegrosql.TypeLonglong:
		return func(i int) types.Causet { return types.NewIntCauset(int64(i)) }
	case allegrosql.TypeFloat:
		return func(i int) types.Causet { return types.NewFloat32Causet(float32(i)) }
	case allegrosql.TypeNewDecimal:
		return func(i int) types.Causet { return types.NewDecimalCauset(types.NewDecFromInt(int64(i))) }
	case allegrosql.TypeDouble:
		return func(i int) types.Causet { return types.NewFloat64Causet(float64(i)) }
	case allegrosql.TypeString:
		return func(i int) types.Causet { return types.NewStringCauset(fmt.Sprintf("%d", i)) }
	case allegrosql.TypeDate:
		return func(i int) types.Causet { return types.NewTimeCauset(types.TimeFromDays(int64(i + 365))) }
	case allegrosql.TypeDuration:
		return func(i int) types.Causet { return types.NewDurationCauset(types.Duration{Duration: time.Duration(i)}) }
	case allegrosql.TypeJSON:
		return func(i int) types.Causet { return types.NewCauset(json.CreateBinary(int64(i))) }
	case allegrosql.TypeEnum:
		elems := []string{"a", "b", "c", "d", "e"}
		return func(i int) types.Causet {
			e, _ := types.ParseEnumValue(elems, uint64(i+1))
			return types.NewDefCauslateMysqlEnumCauset(e, ft.DefCauslate)
		}
	case allegrosql.TypeSet:
		elems := []string{"a", "b", "c", "d", "e"}
		return func(i int) types.Causet {
			e, _ := types.ParseSetValue(elems, uint64(i+1))
			return types.NewMysqlSetCauset(e, ft.DefCauslate)
		}
	}
	return nil
}

func (s *testSuite) testAggFunc(c *C, p aggTest) {
	srcChk := p.genSrcChk()

	args := []expression.Expression{&expression.DeferredCauset{RetType: p.dataType, Index: 0}}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.CouplingConstantWithRadix{Value: types.NewStringCauset(" "), RetType: types.NewFieldType(allegrosql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		finalFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, finalPr)
	}
	p.messUpChunk(srcChk)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt := resultChk.GetEvent(0).GetCauset(0, desc.RetTp)
	result, err := dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetEvent(0).GetCauset(0, desc.RetTp)
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[0]))

	// test the agg func with distinct
	desc, err = aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, true)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ = finalFunc.AllocPartialResult()

	resultChk.Reset()
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		finalFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, finalPr)
	}
	p.messUpChunk(srcChk)
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		finalFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, finalPr)
	}
	p.messUpChunk(srcChk)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetEvent(0).GetCauset(0, desc.RetTp)
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetEvent(0).GetCauset(0, desc.RetTp)
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[0]))
}

func (s *testSuite) testAggMemFunc(c *C, p aggMemTest) {
	srcChk := p.aggTest.genSrcChk()

	args := []expression.Expression{&expression.DeferredCauset{RetType: p.aggTest.dataType, Index: 0}}
	if p.aggTest.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.CouplingConstantWithRadix{Value: types.NewStringCauset(" "), RetType: types.NewFieldType(allegrosql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.aggTest.funcName, args, p.isDistinct)
	c.Assert(err, IsNil)
	if p.aggTest.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	finalPr, memDelta := finalFunc.AllocPartialResult()
	c.Assert(memDelta, Equals, p.allocMemDelta)

	uFIDelateMemDeltas, err := p.uFIDelateMemDeltaGens(srcChk, p.aggTest.dataType)
	c.Assert(err, IsNil)
	iter := chunk.NewIterator4Chunk(srcChk)
	i := 0
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		memDelta, err := finalFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, finalPr)
		c.Assert(err, IsNil)
		c.Assert(memDelta, Equals, uFIDelateMemDeltas[i])
		i++
	}
}

func (s *testSuite) testMultiArgsAggFunc(c *C, p multiArgsAggTest) {
	srcChk := p.genSrcChk()

	args := make([]expression.Expression, len(p.dataTypes))
	for k := 0; k < len(p.dataTypes); k++ {
		args[k] = &expression.DeferredCauset{RetType: p.dataTypes[k], Index: k}
	}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.CouplingConstantWithRadix{Value: types.NewStringCauset(" "), RetType: types.NewFieldType(allegrosql.TypeString)})
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		finalFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, finalPr)
	}
	p.messUpChunk(srcChk)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt := resultChk.GetEvent(0).GetCauset(0, desc.RetTp)
	result, err := dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetEvent(0).GetCauset(0, desc.RetTp)
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[0]))

	// test the agg func with distinct
	desc, err = aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, true)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ = finalFunc.AllocPartialResult()

	resultChk.Reset()
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		finalFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, finalPr)
	}
	p.messUpChunk(srcChk)
	srcChk = p.genSrcChk()
	iter = chunk.NewIterator4Chunk(srcChk)
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		finalFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, finalPr)
	}
	p.messUpChunk(srcChk)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetEvent(0).GetCauset(0, desc.RetTp)
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	dt = resultChk.GetEvent(0).GetCauset(0, desc.RetTp)
	result, err = dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)
}

func (s *testSuite) benchmarkAggFunc(b *testing.B, p aggTest) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, p.numEvents)
	for i := 0; i < p.numEvents; i++ {
		dt := p.dataGen(i)
		srcChk.AppendCauset(0, &dt)
	}
	srcChk.AppendCauset(0, &types.Causet{})

	args := []expression.Expression{&expression.DeferredCauset{RetType: p.dataType, Index: 0}}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.CouplingConstantWithRadix{Value: types.NewStringCauset(" "), RetType: types.NewFieldType(allegrosql.TypeString)})
	}
	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
	iter := chunk.NewIterator4Chunk(srcChk)
	input := make([]chunk.Event, 0, iter.Len())
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		input = append(input, event)
	}
	b.Run(fmt.Sprintf("%v/%v", p.funcName, p.dataType), func(b *testing.B) {
		s.baseBenchmarkAggFunc(b, finalFunc, input, resultChk)
	})

	desc, err = aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, true)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(s.ctx, desc, 0)
	resultChk.Reset()
	b.Run(fmt.Sprintf("%v(distinct)/%v", p.funcName, p.dataType), func(b *testing.B) {
		s.baseBenchmarkAggFunc(b, finalFunc, input, resultChk)
	})
}

func (s *testSuite) benchmarkMultiArgsAggFunc(b *testing.B, p multiArgsAggTest) {
	srcChk := chunk.NewChunkWithCapacity(p.dataTypes, p.numEvents)
	for i := 0; i < p.numEvents; i++ {
		for j := 0; j < len(p.dataGens); j++ {
			fdt := p.dataGens[j](i)
			srcChk.AppendCauset(j, &fdt)
		}
	}
	srcChk.AppendCauset(0, &types.Causet{})

	args := make([]expression.Expression, len(p.dataTypes))
	for k := 0; k < len(p.dataTypes); k++ {
		args[k] = &expression.DeferredCauset{RetType: p.dataTypes[k], Index: k}
	}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.CouplingConstantWithRadix{Value: types.NewStringCauset(" "), RetType: types.NewFieldType(allegrosql.TypeString)})
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)
	iter := chunk.NewIterator4Chunk(srcChk)
	input := make([]chunk.Event, 0, iter.Len())
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		input = append(input, event)
	}
	b.Run(fmt.Sprintf("%v/%v", p.funcName, p.dataTypes), func(b *testing.B) {
		s.baseBenchmarkAggFunc(b, finalFunc, input, resultChk)
	})

	desc, err = aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, true)
	if err != nil {
		b.Fatal(err)
	}
	if p.orderBy {
		desc.OrderByItems = []*soliton.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(s.ctx, desc, 0)
	resultChk.Reset()
	b.Run(fmt.Sprintf("%v(distinct)/%v", p.funcName, p.dataTypes), func(b *testing.B) {
		s.baseBenchmarkAggFunc(b, finalFunc, input, resultChk)
	})
}

func (s *testSuite) baseBenchmarkAggFunc(b *testing.B,
	finalFunc aggfuncs.AggFunc, input []chunk.Event, output *chunk.Chunk) {
	finalPr, _ := finalFunc.AllocPartialResult()
	output.Reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		finalFunc.UFIDelatePartialResult(s.ctx, input, finalPr)
		b.StopTimer()
		output.Reset()
		b.StartTimer()
	}
}
