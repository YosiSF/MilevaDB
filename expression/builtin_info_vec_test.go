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
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

type milevadbKeyGener struct {
	inner *defaultGener
}

func (g *milevadbKeyGener) gen() interface{} {
	blockID := g.inner.gen().(int64)
	var result []byte
	if rand.Intn(2) == 1 {
		// Generate a record key
		handle := g.inner.gen().(int64)
		result = blockcodec.EncodeEventKeyWithHandle(blockID, ekv.IntHandle(handle))
	} else {
		// Generate an index key
		idx := g.inner.gen().(int64)
		result = blockcodec.EncodeBlockIndexPrefix(blockID, idx)
	}
	return hex.EncodeToString(result)
}

var vecBuiltinInfoCases = map[string][]vecExprBenchCase{
	ast.Version: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.MilevaDBVersion: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.CurrentUser: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.FoundEvents: {
		{retEvalType: types.CausetEDN},
	},
	ast.Database: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.User: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.MilevaDBDecodeKey: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString},
			geners: []dataGenerator{&milevadbKeyGener{
				inner: newDefaultGener(0, types.CausetEDN),
			}},
		},
	},
	ast.EventCount: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{}},
	},
	ast.CurrentRole: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.MilevaDBIsDBSOwner: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{}},
	},
	ast.ConnectionID: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{}},
	},
	ast.LastInsertId: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN}},
	},
	ast.Benchmark: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN},
			constants: []*CouplingConstantWithRadix{{Value: types.NewIntCauset(10), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.ETReal},
			constants: []*CouplingConstantWithRadix{{Value: types.NewIntCauset(11), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.ETDecimal},
			constants: []*CouplingConstantWithRadix{{Value: types.NewIntCauset(12), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.ETString},
			constants: []*CouplingConstantWithRadix{{Value: types.NewIntCauset(13), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.ETDatetime},
			constants: []*CouplingConstantWithRadix{{Value: types.NewIntCauset(14), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.ETTimestamp},
			constants: []*CouplingConstantWithRadix{{Value: types.NewIntCauset(15), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.ETDuration},
			constants: []*CouplingConstantWithRadix{{Value: types.NewIntCauset(16), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.CausetEDN, types.ETJson},
			constants: []*CouplingConstantWithRadix{{Value: types.NewIntCauset(17), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinInfoFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinInfoCases)
}

func BenchmarkVectorizedBuiltinInfoFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinInfoCases)
}
