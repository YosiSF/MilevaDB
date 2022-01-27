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

package aggfuncs_test

import (
	"encoding/binary"
	"testing"

	"github.com/dgryski/go-farm"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/executor/aggfuncs"
)

func genApproxDistinctMergePartialResult(begin, end uint64) string {
	o := aggfuncs.NewPartialResult4ApproxCountDistinct()
	encodedBytes := make([]byte, 8)
	for i := begin; i < end; i++ {
		binary.LittleEndian.PutUint64(encodedBytes, i)
		x := farm.Hash64(encodedBytes)
		o.InsertHash64(x)
	}
	return string(o.Serialize())
}

func (s *testSuite) TestMergePartialResult4Count(c *C) {
	tester := builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeLonglong, 5, 5, 3, 8)
	s.testMergePartialResult(c, tester)

	tester = builPosetDaggTester(ast.AggFuncApproxCountDistinct, allegrosql.TypeLonglong, 5, genApproxDistinctMergePartialResult(0, 5), genApproxDistinctMergePartialResult(2, 5), 5)
	s.testMergePartialResult(c, tester)
}

func (s *testSuite) TestCount(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeLonglong, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeFloat, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeDouble, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeNewDecimal, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeString, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeDate, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeDuration, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeJSON, 5, 0, 5),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
	tests2 := []multiArgsAggTest{
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeLonglong, allegrosql.TypeLonglong}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeFloat, allegrosql.TypeFloat}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeDouble, allegrosql.TypeDouble}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeNewDecimal, allegrosql.TypeNewDecimal}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeString, allegrosql.TypeString}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeDate, allegrosql.TypeDate}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeDuration, allegrosql.TypeDuration}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeJSON, allegrosql.TypeJSON}, allegrosql.TypeLonglong, 5, 0, 5),
	}
	for _, test := range tests2 {
		s.testMultiArgsAggFunc(c, test)
	}

	tests3 := []aggTest{
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeLonglong, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeFloat, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeDouble, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeNewDecimal, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeString, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeDate, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeDuration, 5, 0, 5),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeJSON, 5, 0, 5),
	}
	for _, test := range tests3 {
		s.testAggFunc(c, test)
	}

	tests4 := []multiArgsAggTest{
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeLonglong, allegrosql.TypeLonglong}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeFloat, allegrosql.TypeFloat}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeDouble, allegrosql.TypeDouble}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeNewDecimal, allegrosql.TypeNewDecimal}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeString, allegrosql.TypeString}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeDate, allegrosql.TypeDate}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeDuration, allegrosql.TypeDuration}, allegrosql.TypeLonglong, 5, 0, 5),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeJSON, allegrosql.TypeJSON}, allegrosql.TypeLonglong, 5, 0, 5),
	}

	for _, test := range tests4 {
		s.testMultiArgsAggFunc(c, test)
	}
}

func (s *testSuite) TestMemCount(c *C) {
	tests := []aggMemTest{
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeFloat, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeString, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeDate, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeDuration, 5,
			aggfuncs.DefPartialResult4CountSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4CountDistinctIntSize, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeFloat, 5,
			aggfuncs.DefPartialResult4CountDistinctRealSize, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4CountDistinctRealSize, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4CountDistinctDecimalSize, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeString, 5,
			aggfuncs.DefPartialResult4CountDistinctStringSize, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeDate, 5,
			aggfuncs.DefPartialResult4CountWithDistinctSize, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeDuration, 5,
			aggfuncs.DefPartialResult4CountDistinctDurationSize, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncCount, allegrosql.TypeJSON, 5,
			aggfuncs.DefPartialResult4CountWithDistinctSize, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncApproxCountDistinct, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4ApproxCountDistinctSize, approxCountDistinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncApproxCountDistinct, allegrosql.TypeString, 5,
			aggfuncs.DefPartialResult4ApproxCountDistinctSize, approxCountDistinctUFIDelateMemDeltaGens, true),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}

func BenchmarkCount(b *testing.B) {
	s := testSuite{}
	s.SetUpSuite(nil)

	rowNum := 50000
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeFloat, rowNum, 0, rowNum),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeDouble, rowNum, 0, rowNum),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeNewDecimal, rowNum, 0, rowNum),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeString, rowNum, 0, rowNum),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeDate, rowNum, 0, rowNum),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeDuration, rowNum, 0, rowNum),
		builPosetDaggTester(ast.AggFuncCount, allegrosql.TypeJSON, rowNum, 0, rowNum),
	}
	for _, test := range tests {
		s.benchmarkAggFunc(b, test)
	}

	tests2 := []multiArgsAggTest{
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeLonglong, allegrosql.TypeLonglong}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeFloat, allegrosql.TypeFloat}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeDouble, allegrosql.TypeDouble}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeNewDecimal, allegrosql.TypeNewDecimal}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeString, allegrosql.TypeString}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeDate, allegrosql.TypeDate}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeDuration, allegrosql.TypeDuration}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncCount, []byte{allegrosql.TypeJSON, allegrosql.TypeJSON}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
	}
	for _, test := range tests2 {
		s.benchmarkMultiArgsAggFunc(b, test)
	}

	tests3 := []multiArgsAggTest{
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeLonglong, allegrosql.TypeLonglong}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeFloat, allegrosql.TypeFloat}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeDouble, allegrosql.TypeDouble}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeNewDecimal, allegrosql.TypeNewDecimal}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeString, allegrosql.TypeString}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeDate, allegrosql.TypeDate}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeDuration, allegrosql.TypeDuration}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
		buildMultiArgsAggTester(ast.AggFuncApproxCountDistinct, []byte{allegrosql.TypeJSON, allegrosql.TypeJSON}, allegrosql.TypeLonglong, rowNum, 0, rowNum),
	}
	for _, test := range tests3 {
		s.benchmarkMultiArgsAggFunc(b, test)
	}
}
