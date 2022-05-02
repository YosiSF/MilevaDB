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
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"

	"github.com/whtcorpsinc/MilevaDB-Prod/executor/aggfuncs"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression/aggregation"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
)

type windowTest struct {
	dataType        *types.FieldType
	numEvents       int
	funcName        string
	args            []expression.Expression
	orderByDefCauss []*expression.DeferredCauset
	results         []types.Causet
}

func (p *windowTest) genSrcChk() *chunk.Chunk {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, p.numEvents)
	dataGen := getDataGenFunc(p.dataType)
	for i := 0; i < p.numEvents; i++ {
		dt := dataGen(i)
		srcChk.AppendCauset(0, &dt)
	}
	return srcChk
}

type windowMemTest struct {
	windowTest            windowTest
	allocMemDelta         int64
	uFIDelateMemDeltaGens uFIDelateMemDeltaGens
}

func (s *testSuite) testWindowFunc(c *C, p windowTest) {
	srcChk := p.genSrcChk()

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, p.args, false)
	c.Assert(err, IsNil)
	finalFunc := aggfuncs.BuildWindowFunctions(s.ctx, desc, 0, p.orderByDefCauss)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		_, err = finalFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, finalPr)
		c.Assert(err, IsNil)
	}

	c.Assert(p.numEvents, Equals, len(p.results))
	for i := 0; i < p.numEvents; i++ {
		err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
		c.Assert(err, IsNil)
		dt := resultChk.GetEvent(0).GetCauset(0, desc.RetTp)
		result, err := dt.CompareCauset(s.ctx.GetStochaseinstein_dbars().StmtCtx, &p.results[i])
		c.Assert(err, IsNil)
		c.Assert(result, Equals, 0)
		resultChk.Reset()
	}
	finalFunc.ResetPartialResult(finalPr)
}

func (s *testSuite) testWindowAggMemFunc(c *C, p windowMemTest) {
	srcChk := p.windowTest.genSrcChk()

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.windowTest.funcName, p.windowTest.args, false)
	c.Assert(err, IsNil)
	finalFunc := aggfuncs.BuildWindowFunctions(s.ctx, desc, 0, p.windowTest.orderByDefCauss)
	finalPr, memDelta := finalFunc.AllocPartialResult()
	c.Assert(memDelta, Equals, p.allocMemDelta)

	uFIDelateMemDeltas, err := p.uFIDelateMemDeltaGens(srcChk, p.windowTest.dataType)
	c.Assert(err, IsNil)

	i := 0
	iter := chunk.NewIterator4Chunk(srcChk)
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		memDelta, err = finalFunc.UFIDelatePartialResult(s.ctx, []chunk.Event{event}, finalPr)
		c.Assert(err, IsNil)
		c.Assert(memDelta, Equals, uFIDelateMemDeltas[i])
		i++
	}
}

func buildWindowTesterWithArgs(funcName string, tp byte, args []expression.Expression, orderByDefCauss int, numEvents int, results ...interface{}) windowTest {
	pt := windowTest{
		dataType:  types.NewFieldType(tp),
		numEvents: numEvents,
		funcName:  funcName,
	}
	if funcName != ast.WindowFuncNtile {
		pt.args = append(pt.args, &expression.DeferredCauset{RetType: pt.dataType, Index: 0})
	}
	pt.args = append(pt.args, args...)
	if orderByDefCauss > 0 {
		pt.orderByDefCauss = append(pt.orderByDefCauss, &expression.DeferredCauset{RetType: pt.dataType, Index: 0})
	}

	for _, result := range results {
		pt.results = append(pt.results, types.NewCauset(result))
	}
	return pt
}

func buildWindowTester(funcName string, tp byte, constantArg uint64, orderByDefCauss int, numEvents int, results ...interface{}) windowTest {
	pt := windowTest{
		dataType:  types.NewFieldType(tp),
		numEvents: numEvents,
		funcName:  funcName,
	}
	if funcName != ast.WindowFuncNtile {
		pt.args = append(pt.args, &expression.DeferredCauset{RetType: pt.dataType, Index: 0})
	}
	if constantArg > 0 {
		pt.args = append(pt.args, &expression.CouplingConstantWithRadix{Value: types.NewUintCauset(constantArg)})
	}
	if orderByDefCauss > 0 {
		pt.orderByDefCauss = append(pt.orderByDefCauss, &expression.DeferredCauset{RetType: pt.dataType, Index: 0})
	}

	for _, result := range results {
		pt.results = append(pt.results, types.NewCauset(result))
	}
	return pt
}

func buildWindowMemTester(funcName string, tp byte, constantArg uint64, numEvents int, orderByDefCauss int, allocMemDelta int64, uFIDelateMemDeltaGens uFIDelateMemDeltaGens) windowMemTest {
	windowTest := buildWindowTester(funcName, tp, constantArg, orderByDefCauss, numEvents)
	pt := windowMemTest{
		windowTest:            windowTest,
		allocMemDelta:         allocMemDelta,
		uFIDelateMemDeltaGens: uFIDelateMemDeltaGens,
	}
	return pt
}

func buildWindowMemTesterWithArgs(funcName string, tp byte, args []expression.Expression, orderByDefCauss int, numEvents int, allocMemDelta int64, uFIDelateMemDeltaGens uFIDelateMemDeltaGens) windowMemTest {
	windowTest := buildWindowTesterWithArgs(funcName, tp, args, orderByDefCauss, numEvents)
	pt := windowMemTest{
		windowTest:            windowTest,
		allocMemDelta:         allocMemDelta,
		uFIDelateMemDeltaGens: uFIDelateMemDeltaGens,
	}
	return pt
}

func (s *testSuite) TestWindowFunctions(c *C) {
	tests := []windowTest{
		buildWindowTester(ast.WindowFuncCumeDist, allegrosql.TypeLonglong, 0, 1, 1, 1),
		buildWindowTester(ast.WindowFuncCumeDist, allegrosql.TypeLonglong, 0, 0, 2, 1, 1),
		buildWindowTester(ast.WindowFuncCumeDist, allegrosql.TypeLonglong, 0, 1, 4, 0.25, 0.5, 0.75, 1),

		buildWindowTester(ast.WindowFuncDenseRank, allegrosql.TypeLonglong, 0, 0, 2, 1, 1),
		buildWindowTester(ast.WindowFuncDenseRank, allegrosql.TypeLonglong, 0, 1, 4, 1, 2, 3, 4),

		buildWindowTester(ast.WindowFuncFirstValue, allegrosql.TypeLonglong, 0, 1, 2, 0, 0),
		buildWindowTester(ast.WindowFuncFirstValue, allegrosql.TypeFloat, 0, 1, 2, 0, 0),
		buildWindowTester(ast.WindowFuncFirstValue, allegrosql.TypeDouble, 0, 1, 2, 0, 0),
		buildWindowTester(ast.WindowFuncFirstValue, allegrosql.TypeNewDecimal, 0, 1, 2, types.NewDecFromInt(0), types.NewDecFromInt(0)),
		buildWindowTester(ast.WindowFuncFirstValue, allegrosql.TypeString, 0, 1, 2, "0", "0"),
		buildWindowTester(ast.WindowFuncFirstValue, allegrosql.TypeDate, 0, 1, 2, types.TimeFromDays(365), types.TimeFromDays(365)),
		buildWindowTester(ast.WindowFuncFirstValue, allegrosql.TypeDuration, 0, 1, 2, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(0)}),
		buildWindowTester(ast.WindowFuncFirstValue, allegrosql.TypeJSON, 0, 1, 2, json.CreateBinary(int64(0)), json.CreateBinary(int64(0))),

		buildWindowTester(ast.WindowFuncLastValue, allegrosql.TypeLonglong, 1, 0, 2, 1, 1),

		buildWindowTester(ast.WindowFuncNthValue, allegrosql.TypeLonglong, 2, 0, 3, 1, 1, 1),
		buildWindowTester(ast.WindowFuncNthValue, allegrosql.TypeLonglong, 5, 0, 3, nil, nil, nil),

		buildWindowTester(ast.WindowFuncNtile, allegrosql.TypeLonglong, 3, 0, 4, 1, 1, 2, 3),
		buildWindowTester(ast.WindowFuncNtile, allegrosql.TypeLonglong, 5, 0, 3, 1, 2, 3),

		buildWindowTester(ast.WindowFuncPercentRank, allegrosql.TypeLonglong, 0, 1, 1, 0),
		buildWindowTester(ast.WindowFuncPercentRank, allegrosql.TypeLonglong, 0, 0, 3, 0, 0, 0),
		buildWindowTester(ast.WindowFuncPercentRank, allegrosql.TypeLonglong, 0, 1, 4, 0, 0.3333333333333333, 0.6666666666666666, 1),

		buildWindowTester(ast.WindowFuncRank, allegrosql.TypeLonglong, 0, 1, 1, 1),
		buildWindowTester(ast.WindowFuncRank, allegrosql.TypeLonglong, 0, 0, 3, 1, 1, 1),
		buildWindowTester(ast.WindowFuncRank, allegrosql.TypeLonglong, 0, 1, 4, 1, 2, 3, 4),

		buildWindowTester(ast.WindowFuncEventNumber, allegrosql.TypeLonglong, 0, 0, 4, 1, 2, 3, 4),
	}
	for _, test := range tests {
		s.testWindowFunc(c, test)
	}
}
