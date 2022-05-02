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

package expression

import (
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var vecBuiltinLikeCases = map[string][]vecExprBenchCase{
	ast.Like: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.CausetEDN},
			geners: []dataGenerator{nil, nil, newRangeInt64Gener(int('\\'), int('\\')+1)},
		},
	},
	ast.Regexp: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinLikeFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinLikeCases)
}

func BenchmarkVectorizedBuiltinLikeFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinLikeCases)
}
