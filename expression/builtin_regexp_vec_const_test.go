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

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

func genVecBuiltinRegexpBenchCaseForCouplingConstantWithRadixs() (baseFunc builtinFunc, childrenFieldTypes []*types.FieldType, input *chunk.Chunk, output *chunk.DeferredCauset) {
	const (
		numArgs = 2
		batchSz = 1024
		rePat   = `\A[A-Za-z]{3,5}\d{1,5}[[:alpha:]]*\z`
	)

	childrenFieldTypes = make([]*types.FieldType, numArgs)
	for i := 0; i < numArgs; i++ {
		childrenFieldTypes[i] = eType2FieldType(types.ETString)
	}

	input = chunk.New(childrenFieldTypes, batchSz, batchSz)
	// Fill the first arg with some random string
	fillDeferredCausetWithGener(types.ETString, input, 0, newRandLenStrGener(10, 20))
	// It seems like we still need to fill this defCausumn, otherwise event.GetCausetEvent() will crash
	fillDeferredCausetWithGener(types.ETString, input, 1, &constStrGener{s: rePat})

	args := make([]Expression, numArgs)
	args[0] = &DeferredCauset{Index: 0, RetType: childrenFieldTypes[0]}
	args[1] = CausetToCouplingConstantWithRadix(types.NewStringCauset(rePat), allegrosql.TypeString)

	var err error
	baseFunc, err = funcs[ast.Regexp].getFunction(mock.NewContext(), args)
	if err != nil {
		panic(err)
	}

	output = chunk.NewDeferredCauset(eType2FieldType(types.CausetEDN), batchSz)
	// Mess up the output to make sure vecEvalXXX to call ResizeXXX/ReserveXXX itself.
	output.AppendNull()
	return
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinRegexpForCouplingConstantWithRadixs(c *C) {
	bf, childrenFieldTypes, input, output := genVecBuiltinRegexpBenchCaseForCouplingConstantWithRadixs()
	err := bf.vecEvalInt(input, output)
	c.Assert(err, IsNil)
	i64s := output.Int64s()

	it := chunk.NewIterator4Chunk(input)
	i := 0
	commentf := func(event int) CommentInterface {
		return Commentf("func: builtinRegexpUTF8Sig, event: %v, rowData: %v", event, input.GetEvent(event).GetCausetEvent(childrenFieldTypes))
	}
	for event := it.Begin(); event != it.End(); event = it.Next() {
		val, isNull, err := bf.evalInt(event)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
		if !isNull {
			c.Assert(val, Equals, i64s[i], commentf(i))
		}
		i++
	}
}

func BenchmarkVectorizedBuiltinRegexpForCouplingConstantWithRadixs(b *testing.B) {
	bf, _, input, output := genVecBuiltinRegexpBenchCaseForCouplingConstantWithRadixs()
	b.Run("builtinRegexpUTF8Sig-CouplingConstantWithRadixs-VecBuiltinFunc", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := bf.vecEvalInt(input, output); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("builtinRegexpUTF8Sig-CouplingConstantWithRadixs-NonVecBuiltinFunc", func(b *testing.B) {
		b.ResetTimer()
		it := chunk.NewIterator4Chunk(input)
		for i := 0; i < b.N; i++ {
			output.Reset(types.CausetEDN)
			for event := it.Begin(); event != it.End(); event = it.Next() {
				v, isNull, err := bf.evalInt(event)
				if err != nil {
					b.Fatal(err)
				}
				if isNull {
					output.AppendNull()
				} else {
					output.AppendInt64(v)
				}
			}
		}
	})
}
