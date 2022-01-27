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

// Code generated by go generate in expression/generator; DO NOT EDIT.

package expression

import (
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/milevadb/types"
)

var vecGeneratedBuiltinCompareCases = map[string][]vecExprBenchCase{
	ast.LT: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
	},
	ast.LE: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
	},
	ast.GT: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
	},
	ast.GE: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
	},
	ast.EQ: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
	},
	ast.NE: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
	},
	ast.NullEQ: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETJson, types.ETJson}},
	},
	ast.Coalesce: {

		{
			retEvalType:   types.ETInt,
			childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETInt},
			geners: []dataGenerator{
				gener{*newDefaultGener(0.2, types.ETInt)},
				gener{*newDefaultGener(0.2, types.ETInt)},
				gener{*newDefaultGener(0.2, types.ETInt)},
			},
		},

		{
			retEvalType:   types.ETReal,
			childrenTypes: []types.EvalType{types.ETReal, types.ETReal, types.ETReal},
			geners: []dataGenerator{
				gener{*newDefaultGener(0.2, types.ETReal)},
				gener{*newDefaultGener(0.2, types.ETReal)},
				gener{*newDefaultGener(0.2, types.ETReal)},
			},
		},

		{
			retEvalType:   types.ETDecimal,
			childrenTypes: []types.EvalType{types.ETDecimal, types.ETDecimal, types.ETDecimal},
			geners: []dataGenerator{
				gener{*newDefaultGener(0.2, types.ETDecimal)},
				gener{*newDefaultGener(0.2, types.ETDecimal)},
				gener{*newDefaultGener(0.2, types.ETDecimal)},
			},
		},

		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString},
			geners: []dataGenerator{
				gener{*newDefaultGener(0.2, types.ETString)},
				gener{*newDefaultGener(0.2, types.ETString)},
				gener{*newDefaultGener(0.2, types.ETString)},
			},
		},

		{
			retEvalType:   types.ETDatetime,
			childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime, types.ETDatetime},
			geners: []dataGenerator{
				gener{*newDefaultGener(0.2, types.ETDatetime)},
				gener{*newDefaultGener(0.2, types.ETDatetime)},
				gener{*newDefaultGener(0.2, types.ETDatetime)},
			},
		},

		{
			retEvalType:   types.ETDuration,
			childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration, types.ETDuration},
			geners: []dataGenerator{
				gener{*newDefaultGener(0.2, types.ETDuration)},
				gener{*newDefaultGener(0.2, types.ETDuration)},
				gener{*newDefaultGener(0.2, types.ETDuration)},
			},
		},

		{
			retEvalType:   types.ETJson,
			childrenTypes: []types.EvalType{types.ETJson, types.ETJson, types.ETJson},
			geners: []dataGenerator{
				gener{*newDefaultGener(0.2, types.ETJson)},
				gener{*newDefaultGener(0.2, types.ETJson)},
				gener{*newDefaultGener(0.2, types.ETJson)},
			},
		},
	},
}

func (s *testEvaluatorSuite) TestVectorizedGeneratedBuiltinCompareEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecGeneratedBuiltinCompareCases)
}

func (s *testEvaluatorSuite) TestVectorizedGeneratedBuiltinCompareFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecGeneratedBuiltinCompareCases)
}

func BenchmarkVectorizedGeneratedBuiltinCompareEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecGeneratedBuiltinCompareCases)
}

func BenchmarkVectorizedGeneratedBuiltinCompareFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecGeneratedBuiltinCompareCases)
}
