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

func (s *testSuite) TestMemEventNumber(c *C) {
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncEventNumber, allegrosql.TypeLonglong, 0, 0, 4,
			aggfuncs.DefPartialResult4EventNumberSize, defaultUFIDelateMemDeltaGens),
	}
	for _, test := range tests {
		s.testWindowAggMemFunc(c, test)
	}
}
