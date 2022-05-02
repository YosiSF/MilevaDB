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
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

func (s *testEvaluatorSuite) TestDeferredCauset(c *C) {
	defCaus := &DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), UniqueID: 1}

	c.Assert(defCaus.Equal(nil, defCaus), IsTrue)
	c.Assert(defCaus.Equal(nil, &DeferredCauset{}), IsFalse)
	c.Assert(defCaus.IsCorrelated(), IsFalse)
	c.Assert(defCaus.Equal(nil, defCaus.Decorrelate(nil)), IsTrue)

	marshal, err := defCaus.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(marshal, DeepEquals, []byte{0x22, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x23, 0x31, 0x22})

	intCauset := types.NewIntCauset(1)
	corDefCaus := &CorrelatedDeferredCauset{DeferredCauset: *defCaus, Data: &intCauset}
	invalidCorDefCaus := &CorrelatedDeferredCauset{DeferredCauset: DeferredCauset{}}
	schemaReplicant := NewSchema(&DeferredCauset{UniqueID: 1})
	c.Assert(corDefCaus.Equal(nil, corDefCaus), IsTrue)
	c.Assert(corDefCaus.Equal(nil, invalidCorDefCaus), IsFalse)
	c.Assert(corDefCaus.IsCorrelated(), IsTrue)
	c.Assert(corDefCaus.ConstItem(nil), IsFalse)
	c.Assert(corDefCaus.Decorrelate(schemaReplicant).Equal(nil, defCaus), IsTrue)
	c.Assert(invalidCorDefCaus.Decorrelate(schemaReplicant).Equal(nil, invalidCorDefCaus), IsTrue)

	intCorDefCaus := &CorrelatedDeferredCauset{DeferredCauset: DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong)},
		Data: &intCauset}
	intVal, isNull, err := intCorDefCaus.EvalInt(s.ctx, chunk.Event{})
	c.Assert(intVal, Equals, int64(1))
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	realCauset := types.NewFloat64Causet(1.2)
	realCorDefCaus := &CorrelatedDeferredCauset{DeferredCauset: DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble)},
		Data: &realCauset}
	realVal, isNull, err := realCorDefCaus.EvalReal(s.ctx, chunk.Event{})
	c.Assert(realVal, Equals, float64(1.2))
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	decimalCauset := types.NewDecimalCauset(types.NewDecFromStringForTest("1.2"))
	decimalCorDefCaus := &CorrelatedDeferredCauset{DeferredCauset: DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal)},
		Data: &decimalCauset}
	decVal, isNull, err := decimalCorDefCaus.EvalDecimal(s.ctx, chunk.Event{})
	c.Assert(decVal.Compare(types.NewDecFromStringForTest("1.2")), Equals, 0)
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	stringCauset := types.NewStringCauset("abc")
	stringCorDefCaus := &CorrelatedDeferredCauset{DeferredCauset: DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeVarchar)},
		Data: &stringCauset}
	strVal, isNull, err := stringCorDefCaus.EvalString(s.ctx, chunk.Event{})
	c.Assert(strVal, Equals, "abc")
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	durationCorDefCaus := &CorrelatedDeferredCauset{DeferredCauset: DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration)},
		Data: &durationCauset}
	durationVal, isNull, err := durationCorDefCaus.EvalDuration(s.ctx, chunk.Event{})
	c.Assert(durationVal.Compare(duration), Equals, 0)
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	timeCauset := types.NewTimeCauset(tm)
	timeCorDefCaus := &CorrelatedDeferredCauset{DeferredCauset: DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime)},
		Data: &timeCauset}
	timeVal, isNull, err := timeCorDefCaus.EvalTime(s.ctx, chunk.Event{})
	c.Assert(timeVal.Compare(tm), Equals, 0)
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestDeferredCausetHashCode(c *C) {
	defCaus1 := &DeferredCauset{
		UniqueID: 12,
	}
	c.Assert(defCaus1.HashCode(nil), DeepEquals, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc})

	defCaus2 := &DeferredCauset{
		UniqueID: 2,
	}
	c.Assert(defCaus2.HashCode(nil), DeepEquals, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2})
}

func (s *testEvaluatorSuite) TestDeferredCauset2Expr(c *C) {
	defcaus := make([]*DeferredCauset, 0, 5)
	for i := 0; i < 5; i++ {
		defcaus = append(defcaus, &DeferredCauset{UniqueID: int64(i)})
	}

	exprs := DeferredCauset2Exprs(defcaus)
	for i := range exprs {
		c.Assert(exprs[i].Equal(nil, defcaus[i]), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestDefCausInfo2DefCaus(c *C) {
	defCaus0, defCaus1 := &DeferredCauset{ID: 0}, &DeferredCauset{ID: 1}
	defcaus := []*DeferredCauset{defCaus0, defCaus1}
	defCausInfo := &perceptron.DeferredCausetInfo{ID: 0}
	res := DefCausInfo2DefCaus(defcaus, defCausInfo)
	c.Assert(res.Equal(nil, defCaus1), IsTrue)

	defCausInfo.ID = 3
	res = DefCausInfo2DefCaus(defcaus, defCausInfo)
	c.Assert(res, IsNil)
}

func (s *testEvaluatorSuite) TestIndexInfo2DefCauss(c *C) {
	defCaus0 := &DeferredCauset{UniqueID: 0, ID: 0, RetType: types.NewFieldType(allegrosql.TypeLonglong)}
	defCaus1 := &DeferredCauset{UniqueID: 1, ID: 1, RetType: types.NewFieldType(allegrosql.TypeLonglong)}
	defCausInfo0 := &perceptron.DeferredCausetInfo{ID: 0, Name: perceptron.NewCIStr("0")}
	defCausInfo1 := &perceptron.DeferredCausetInfo{ID: 1, Name: perceptron.NewCIStr("1")}
	indexDefCaus0, indexDefCaus1 := &perceptron.IndexDeferredCauset{Name: perceptron.NewCIStr("0")}, &perceptron.IndexDeferredCauset{Name: perceptron.NewCIStr("1")}
	indexInfo := &perceptron.IndexInfo{DeferredCausets: []*perceptron.IndexDeferredCauset{indexDefCaus0, indexDefCaus1}}

	defcaus := []*DeferredCauset{defCaus0}
	defCausInfos := []*perceptron.DeferredCausetInfo{defCausInfo0}
	resDefCauss, lengths := IndexInfo2PrefixDefCauss(defCausInfos, defcaus, indexInfo)
	c.Assert(len(resDefCauss), Equals, 1)
	c.Assert(len(lengths), Equals, 1)
	c.Assert(resDefCauss[0].Equal(nil, defCaus0), IsTrue)

	defcaus = []*DeferredCauset{defCaus1}
	defCausInfos = []*perceptron.DeferredCausetInfo{defCausInfo1}
	resDefCauss, lengths = IndexInfo2PrefixDefCauss(defCausInfos, defcaus, indexInfo)
	c.Assert(len(resDefCauss), Equals, 0)
	c.Assert(len(lengths), Equals, 0)

	defcaus = []*DeferredCauset{defCaus0, defCaus1}
	defCausInfos = []*perceptron.DeferredCausetInfo{defCausInfo0, defCausInfo1}
	resDefCauss, lengths = IndexInfo2PrefixDefCauss(defCausInfos, defcaus, indexInfo)
	c.Assert(len(resDefCauss), Equals, 2)
	c.Assert(len(lengths), Equals, 2)
	c.Assert(resDefCauss[0].Equal(nil, defCaus0), IsTrue)
	c.Assert(resDefCauss[1].Equal(nil, defCaus1), IsTrue)
}

func (s *testEvaluatorSuite) TestDefCausHybird(c *C) {
	ctx := mock.NewContext()

	// bit
	ft := types.NewFieldType(allegrosql.TypeBit)
	defCaus := &DeferredCauset{RetType: ft, Index: 0}
	input := chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		num, err := types.ParseBitStr(fmt.Sprintf("0b%b", i))
		c.Assert(err, IsNil)
		input.AppendBytes(0, num)
	}
	result, err := newBuffer(types.CausetEDN, 1024)
	c.Assert(err, IsNil)
	c.Assert(defCaus.VecEvalInt(ctx, input, result), IsNil)

	it := chunk.NewIterator4Chunk(input)
	for event, i := it.Begin(), 0; event != it.End(); event, i = it.Next(), i+1 {
		v, _, err := defCaus.EvalInt(ctx, event)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetInt64(i))
	}

	// use a container which has the different field type with bit
	result, err = newBuffer(types.ETString, 1024)
	c.Assert(err, IsNil)
	c.Assert(defCaus.VecEvalInt(ctx, input, result), IsNil)
	for event, i := it.Begin(), 0; event != it.End(); event, i = it.Next(), i+1 {
		v, _, err := defCaus.EvalInt(ctx, event)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetInt64(i))
	}

	// enum
	ft = types.NewFieldType(allegrosql.TypeEnum)
	defCaus.RetType = ft
	input = chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendEnum(0, types.Enum{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}
	result, err = newBuffer(types.ETString, 1024)
	c.Assert(err, IsNil)
	c.Assert(defCaus.VecEvalString(ctx, input, result), IsNil)

	it = chunk.NewIterator4Chunk(input)
	for event, i := it.Begin(), 0; event != it.End(); event, i = it.Next(), i+1 {
		v, _, err := defCaus.EvalString(ctx, event)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetString(i))
	}

	// set
	ft = types.NewFieldType(allegrosql.TypeSet)
	defCaus.RetType = ft
	input = chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendSet(0, types.Set{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}
	result, err = newBuffer(types.ETString, 1024)
	c.Assert(err, IsNil)
	c.Assert(defCaus.VecEvalString(ctx, input, result), IsNil)

	it = chunk.NewIterator4Chunk(input)
	for event, i := it.Begin(), 0; event != it.End(); event, i = it.Next(), i+1 {
		v, _, err := defCaus.EvalString(ctx, event)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetString(i))
	}
}
