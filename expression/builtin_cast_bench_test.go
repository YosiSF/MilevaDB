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
	"math/rand"
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

func genCastIntAsInt() (*builtinCastIntAsIntSig, *chunk.Chunk, *chunk.DeferredCauset) {
	defCaus := &DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0}
	baseFunc, err := newBaseBuiltinFunc(mock.NewContext(), "", []Expression{defCaus}, 0)
	if err != nil {
		panic(err)
	}
	baseCast := newBaseBuiltinCastFunc(baseFunc, false)
	cast := &builtinCastIntAsIntSig{baseCast}
	input := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendInt64(0, rand.Int63n(10000)-5000)
	}
	result := chunk.NewDeferredCauset(types.NewFieldType(allegrosql.TypeLonglong), 1024)
	return cast, input, result
}

func BenchmarkCastIntAsIntEvent(b *testing.B) {
	cast, input, _ := genCastIntAsInt()
	it := chunk.NewIterator4Chunk(input)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for event := it.Begin(); event != it.End(); event = it.Next() {
			if _, _, err := cast.evalInt(event); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkCastIntAsIntVec(b *testing.B) {
	cast, input, result := genCastIntAsInt()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cast.vecEvalInt(input, result); err != nil {
			b.Fatal(err)
		}
	}
}
