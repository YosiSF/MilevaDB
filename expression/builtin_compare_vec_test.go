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

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/types"
)

var vecBuiltinCompareCases = map[string][]vecExprBenchCase{
	ast.NE: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong},
			},
		},
	},
	ast.IsNull: {},
	ast.LE: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong},
			},
		},
	},
	ast.LT: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong},
			},
		},
	},
	ast.Coalesce: {},
	ast.NullEQ: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
	},
	ast.GT: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong},
			},
		},
	},
	ast.EQ: {},
	ast.GE: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong},
			},
		},
	},
	ast.Date: {},
	ast.Greatest: {
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETInt}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal, types.ETReal}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime, types.ETDatetime}},
	},
	ast.Least: {
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETInt}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal, types.ETReal}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime, types.ETDatetime}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}},
	},
	ast.Interval: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, newRangeRealGener(0, 10, 0)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal, types.ETReal}, geners: []dataGenerator{nil, newRangeRealGener(0, 10, 0), newRangeRealGener(10, 20, 0)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal, types.ETReal, types.ETReal}, geners: []dataGenerator{nil, newRangeRealGener(0, 10, 0), newRangeRealGener(10, 20, 0), newRangeRealGener(20, 30, 0)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETInt}, geners: []dataGenerator{nil, newRangeInt64Gener(0, 10), newRangeInt64Gener(10, 20)}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETInt, types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETInt, types.ETInt}, geners: []dataGenerator{nil, newRangeInt64Gener(0, 10), newRangeInt64Gener(10, 20), newRangeInt64Gener(20, 30)}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinCompareEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinCompareCases)
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinCompareFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinCompareCases)
}

func BenchmarkVectorizedBuiltinCompareEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinCompareCases)
}

func BenchmarkVectorizedBuiltinCompareFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinCompareCases)
}
