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
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/executor/aggfuncs"
	"github.com/whtcorpsinc/milevadb/types"
)

func (s *testSuite) TestMergePartialResult4Sum(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncSum, allegrosql.TypeNewDecimal, 5, types.NewDecFromInt(10), types.NewDecFromInt(9), types.NewDecFromInt(19)),
		builPosetDaggTester(ast.AggFuncSum, allegrosql.TypeDouble, 5, 10.0, 9.0, 19.0),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestSum(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncSum, allegrosql.TypeNewDecimal, 5, nil, types.NewDecFromInt(10)),
		builPosetDaggTester(ast.AggFuncSum, allegrosql.TypeDouble, 5, nil, 10.0),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}

func (s *testSuite) TestMemSum(c *C) {
	tests := []aggMemTest{
		builPosetDaggMemTester(ast.AggFuncSum, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4SumFloat64Size, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncSum, allegrosql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4SumDecimalSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncSum, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4SumDistinctFloat64Size, distinctUFIDelateMemDeltaGens, true),
		builPosetDaggMemTester(ast.AggFuncSum, allegrosql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4SumDistinctDecimalSize, distinctUFIDelateMemDeltaGens, true),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}
