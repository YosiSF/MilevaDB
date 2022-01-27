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
)

func (s *testSuite) TestMergePartialResult4BitFuncs(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncBitAnd, allegrosql.TypeLonglong, 5, 0, 0, 0),
		builPosetDaggTester(ast.AggFuncBitOr, allegrosql.TypeLonglong, 5, 7, 7, 7),
		builPosetDaggTester(ast.AggFuncBitXor, allegrosql.TypeLonglong, 5, 4, 5, 1),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestMemBitFunc(c *C) {
	tests := []aggMemTest{
		builPosetDaggMemTester(ast.AggFuncBitAnd, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4BitFuncSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncBitOr, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4BitFuncSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncBitXor, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4BitFuncSize, defaultUFIDelateMemDeltaGens, false),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}
