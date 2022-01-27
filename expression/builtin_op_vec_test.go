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
	"math"
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/types"
)

var vecBuiltinOpCases = map[string][]vecExprBenchCase{
	ast.IsTruthWithoutNull: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.IsFalsity: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.LogicOr: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOFIDelataGeners()},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETDuration}},
	},
	ast.LogicXor: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOFIDelataGeners()},
	},
	ast.Xor: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOFIDelataGeners()},
	},
	ast.LogicAnd: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOFIDelataGeners()},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETDuration}},
	},
	ast.Or: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOFIDelataGeners()},
	},
	ast.BitNeg: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.UnaryNot: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.And: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: makeBinaryLogicOFIDelataGeners()},
	},
	ast.RightShift: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
	},
	ast.LeftShift: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}},
	},
	ast.UnaryMinus: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETDecimal, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
		{
			retEvalType:        types.ETInt,
			childrenTypes:      []types.EvalType{types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag}},
			geners:             []dataGenerator{newRangeInt64Gener(0, math.MaxInt64)},
		},
	},
	ast.IsNull: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDecimal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDuration}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETDatetime}},
	},
}

// givenValsGener returns the items sequentially from the slice given at
// the construction time. If this slice is exhausted, it falls back to
// the fallback generator.
type givenValsGener struct {
	given    []interface{}
	idx      int
	fallback dataGenerator
}

func (g *givenValsGener) gen() interface{} {
	if g.idx >= len(g.given) {
		return g.fallback.gen()
	}
	v := g.given[g.idx]
	g.idx++
	return v
}

func makeGivenValsOrDefaultGener(vals []interface{}, eType types.EvalType) *givenValsGener {
	g := &givenValsGener{}
	g.given = vals
	g.fallback = newDefaultGener(0.2, eType)
	return g
}

func makeBinaryLogicOFIDelataGeners() []dataGenerator {
	// TODO: rename this to makeBinaryOFIDelataGenerator, since the BIT ops are also using it?
	pairs := [][]interface{}{
		{nil, nil},
		{0, nil},
		{nil, 0},
		{1, nil},
		{nil, 1},
		{0, 0},
		{0, 1},
		{1, 0},
		{1, 1},
		{-1, 1},
	}

	maybeToInt64 := func(v interface{}) interface{} {
		if v == nil {
			return nil
		}
		return int64(v.(int))
	}

	n := len(pairs)
	arg0s := make([]interface{}, n)
	arg1s := make([]interface{}, n)
	for i, p := range pairs {
		arg0s[i] = maybeToInt64(p[0])
		arg1s[i] = maybeToInt64(p[1])
	}
	return []dataGenerator{
		makeGivenValsOrDefaultGener(arg0s, types.ETInt),
		makeGivenValsOrDefaultGener(arg1s, types.ETInt)}
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinOpFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinOpCases)
}

func BenchmarkVectorizedBuiltinOpFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinOpCases)
}

func (s *testEvaluatorSuite) TestBuiltinUnaryMinusIntSig(c *C) {
	ctx := mock.NewContext()
	ft := eType2FieldType(types.ETInt)
	defCaus0 := &DeferredCauset{RetType: ft, Index: 0}
	f, err := funcs[ast.UnaryMinus].getFunction(ctx, []Expression{defCaus0})
	c.Assert(err, IsNil)
	input := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, 1024)
	result := chunk.NewDeferredCauset(ft, 1024)

	c.Assert(allegrosql.HasUnsignedFlag(defCaus0.GetType().Flag), IsFalse)
	input.AppendInt64(0, 233333)
	c.Assert(f.vecEvalInt(input, result), IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(-233333))
	input.Reset()
	input.AppendInt64(0, math.MinInt64)
	c.Assert(f.vecEvalInt(input, result), NotNil)
	input.DeferredCauset(0).SetNull(0, true)
	c.Assert(f.vecEvalInt(input, result), IsNil)
	c.Assert(result.IsNull(0), IsTrue)

	defCaus0.GetType().Flag |= allegrosql.UnsignedFlag
	c.Assert(allegrosql.HasUnsignedFlag(defCaus0.GetType().Flag), IsTrue)
	input.Reset()
	input.AppendUint64(0, 233333)
	c.Assert(f.vecEvalInt(input, result), IsNil)
	c.Assert(result.GetInt64(0), Equals, int64(-233333))
	input.Reset()
	input.AppendUint64(0, -(math.MinInt64)+1)
	c.Assert(f.vecEvalInt(input, result), NotNil)
	input.DeferredCauset(0).SetNull(0, true)
	c.Assert(f.vecEvalInt(input, result), IsNil)
	c.Assert(result.IsNull(0), IsTrue)
}
