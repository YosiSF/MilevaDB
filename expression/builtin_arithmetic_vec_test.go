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
	"math"
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var vecBuiltinArithmeticCases = map[string][]vecExprBenchCase{
	ast.LE: {},
	ast.Minus: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN}, geners: []dataGenerator{newRangeInt64Gener(-100000, 100000), newRangeInt64Gener(-100000, 100000)}},
	},
	ast.Div: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, newRangeRealGener(0, 0, 0)}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, newRangeDecimalGener(0, 0, 0.2)}},
	},
	ast.IntDiv: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, newRangeDecimalGener(0, 0, 0.2)}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeNewDecimal, Flag: allegrosql.UnsignedFlag}, nil},
			geners:             []dataGenerator{newRangeDecimalGener(0, 10000, 0.2), newRangeDecimalGener(0, 10000, 0.2)},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal},
			childrenFieldTypes: []*types.FieldType{nil, {Tp: allegrosql.TypeNewDecimal, Flag: allegrosql.UnsignedFlag}},
			geners:             []dataGenerator{newRangeDecimalGener(0, 10000, 0.2), newRangeDecimalGener(0, 10000, 0.2)},
		},
		// when the final result is at (-1, 0], it should be return 0 instead of the error
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal},
			childrenFieldTypes: []*types.FieldType{nil, {Tp: allegrosql.TypeNewDecimal, Flag: allegrosql.UnsignedFlag}},
			geners:             []dataGenerator{newRangeDecimalGener(-100, -1, 0.2), newRangeDecimalGener(1000, 2000, 0.2)},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			geners: []dataGenerator{
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{
				{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
	},
	ast.Mod: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, newRangeRealGener(0, 0, 0)}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{newRangeRealGener(0, 0, 0), nil}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, newRangeRealGener(0, 0, 1)}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, newRangeDecimalGener(0, 0, 0)}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{newRangeDecimalGener(0, 0, 0), nil}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}, geners: []dataGenerator{nil, newRangeDecimalGener(0, 0, 1)}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN}, geners: []dataGenerator{nil, newRangeInt64Gener(0, 1)}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN}, geners: []dataGenerator{newRangeInt64Gener(0, 1), nil}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			geners: []dataGenerator{
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
			},
		},
	},
	ast.Or: {},
	ast.Mul: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN}, geners: []dataGenerator{newRangeInt64Gener(-10000, 10000), newRangeInt64Gener(-10000, 10000)}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN}, childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeInt24, Flag: allegrosql.UnsignedFlag}, {Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, 10000),
				newRangeInt64Gener(0, 10000),
			},
		},
	},
	ast.Round: {},
	ast.And:   {},
	ast.Plus: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			geners: []dataGenerator{
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
				newRangeInt64Gener(math.MinInt64/2, math.MaxInt64/2),
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag},
				{Tp: allegrosql.TypeLonglong}},
			geners: []dataGenerator{
				newRangeInt64Gener(0, math.MaxInt64),
				newRangeInt64Gener(0, math.MaxInt64),
			},
		},
	},
	ast.NE: {},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinArithmeticFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinArithmeticCases)
}

func BenchmarkVectorizedBuiltinArithmeticFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinArithmeticCases)
}
