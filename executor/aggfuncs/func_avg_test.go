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
	"testing"

	"github.com/whtcorpsinc/MilevaDB-Prod/executor/aggfuncs"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
)

func (s *testSuite) TestMergePartialResult4Avg(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncAvg, allegrosql.TypeNewDecimal, 5, 2.0, 3.0, 2.375),
		builPosetDaggTester(ast.AggFuncAvg, allegrosql.TypeDouble, 5, 2.0, 3.0, 2.375),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestAvg(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncAvg, allegrosql.TypeNewDecimal, 5, nil, 2.0),
		builPosetDaggTester(ast.AggFuncAvg, allegrosql.TypeDouble, 5, nil, 2.0),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}

func (s *testSuite) TestMemAvg(c *C) {
	tests := []aggMemTest{
		builPosetDaggMemTester(ast.AggFuncAvg, allegrosql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4AvgDecimalSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncAvg, allegrosql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4AvgDistinctDecimalSize, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncAvg, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4AvgFloat64Size, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncAvg, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4AvgDistinctFloat64Size, distinctUFIDelateMemDeltaGens, true),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}

func BenchmarkAvg(b *testing.B) {
	s := testSuite{}
	s.SetUpSuite(nil)

	rowNum := 50000
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncAvg, allegrosql.TypeNewDecimal, rowNum, nil, 2.0),
		builPosetDaggTester(ast.AggFuncAvg, allegrosql.TypeDouble, rowNum, nil, 2.0),
	}
	for _, test := range tests {
		s.benchmarkAggFunc(b, test)
	}
}
