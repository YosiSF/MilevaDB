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

func (s *testSuite) TestMemCumeDist(c *C) {
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncCumeDist, allegrosql.TypeLonglong, 0, 1, 1,
			aggfuncs.DefPartialResult4CumeDistSize, rowMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncCumeDist, allegrosql.TypeLonglong, 0, 2, 0,
			aggfuncs.DefPartialResult4CumeDistSize, rowMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncCumeDist, allegrosql.TypeLonglong, 0, 4, 1,
			aggfuncs.DefPartialResult4CumeDistSize, rowMemDeltaGens),
	}
	for _, test := range tests {
		s.testWindowAggMemFunc(c, test)
	}
}
