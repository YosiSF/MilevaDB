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
	"github.com/whtcorpsinc/MilevaDB-Prod/executor/aggfuncs"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
)

func (s *testSuite) TestMemRank(c *C) {
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncRank, allegrosql.TypeLonglong, 0, 1, 1,
			aggfuncs.DefPartialResult4RankSize, rowMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncRank, allegrosql.TypeLonglong, 0, 3, 0,
			aggfuncs.DefPartialResult4RankSize, rowMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncRank, allegrosql.TypeLonglong, 0, 4, 1,
			aggfuncs.DefPartialResult4RankSize, rowMemDeltaGens),
	}
	for _, test := range tests {
		s.testWindowAggMemFunc(c, test)
	}
}
