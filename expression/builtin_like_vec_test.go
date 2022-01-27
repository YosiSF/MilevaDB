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

package expression

import (
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/types"
)

var vecBuiltinLikeCases = map[string][]vecExprBenchCase{
	ast.Like: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETInt},
			geners: []dataGenerator{nil, nil, newRangeInt64Gener(int('\\'), int('\\')+1)},
		},
	},
	ast.Regexp: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinLikeFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinLikeCases)
}

func BenchmarkVectorizedBuiltinLikeFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinLikeCases)
}
