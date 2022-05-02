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
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

func (s *testEvaluatorSuite) TestNewValuesFunc(c *C) {
	res := NewValuesFunc(s.ctx, 0, types.NewFieldType(allegrosql.TypeLonglong))
	c.Assert(res.FuncName.O, Equals, "values")
	c.Assert(res.RetType.Tp, Equals, allegrosql.TypeLonglong)
	_, ok := res.Function.(*builtinValuesIntSig)
	c.Assert(ok, IsTrue)
}

func (s *testEvaluatorSuite) TestEvaluateExprWithNull(c *C) {
	tblInfo := newTestBlockBuilder("").add("defCaus0", allegrosql.TypeLonglong).add("defCaus1", allegrosql.TypeLonglong).build()
	schemaReplicant := blockInfoToSchemaForTest(tblInfo)
	defCaus0 := schemaReplicant.DeferredCausets[0]
	defCaus1 := schemaReplicant.DeferredCausets[1]
	schemaReplicant.DeferredCausets = schemaReplicant.DeferredCausets[:1]
	innerIfNull, err := newFunctionForTest(s.ctx, ast.Ifnull, defCaus1, NewOne())
	c.Assert(err, IsNil)
	outerIfNull, err := newFunctionForTest(s.ctx, ast.Ifnull, defCaus0, innerIfNull)
	c.Assert(err, IsNil)

	res := EvaluateExprWithNull(s.ctx, schemaReplicant, outerIfNull)
	c.Assert(res.String(), Equals, "ifnull(DeferredCauset#1, 1)")

	schemaReplicant.DeferredCausets = append(schemaReplicant.DeferredCausets, defCaus1)
	// ifnull(null, ifnull(null, 1))
	res = EvaluateExprWithNull(s.ctx, schemaReplicant, outerIfNull)
	c.Assert(res.Equal(s.ctx, NewOne()), IsTrue)
}

func (s *testEvaluatorSuite) TestCouplingConstantWithRadix(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	c.Assert(NewZero().IsCorrelated(), IsFalse)
	c.Assert(NewZero().ConstItem(sc), IsTrue)
	c.Assert(NewZero().Decorrelate(nil).Equal(s.ctx, NewZero()), IsTrue)
	c.Assert(NewZero().HashCode(sc), DeepEquals, []byte{0x0, 0x8, 0x0})
	c.Assert(NewZero().Equal(s.ctx, NewOne()), IsFalse)
	res, err := NewZero().MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []byte{0x22, 0x30, 0x22})
}

func (s *testEvaluatorSuite) TestIsBinaryLiteral(c *C) {
	defCaus := &DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeEnum)}
	c.Assert(IsBinaryLiteral(defCaus), IsFalse)
	defCaus.RetType.Tp = allegrosql.TypeSet
	c.Assert(IsBinaryLiteral(defCaus), IsFalse)
	defCaus.RetType.Tp = allegrosql.TypeBit
	c.Assert(IsBinaryLiteral(defCaus), IsFalse)
	defCaus.RetType.Tp = allegrosql.TypeDuration
	c.Assert(IsBinaryLiteral(defCaus), IsFalse)

	con := &CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeVarString), Value: types.NewBinaryLiteralCauset([]byte{byte(0), byte(1)})}
	c.Assert(IsBinaryLiteral(con), IsTrue)
	con.Value = types.NewIntCauset(1)
	c.Assert(IsBinaryLiteral(con), IsFalse)
}

func (s *testEvaluatorSuite) TestConstItem(c *C) {
	sf := newFunction(ast.Rand)
	c.Assert(sf.ConstItem(s.ctx.GetStochaseinstein_dbars().StmtCtx), Equals, false)
	sf = newFunction(ast.UUID)
	c.Assert(sf.ConstItem(s.ctx.GetStochaseinstein_dbars().StmtCtx), Equals, false)
	sf = newFunction(ast.GetParam, NewOne())
	c.Assert(sf.ConstItem(s.ctx.GetStochaseinstein_dbars().StmtCtx), Equals, false)
	sf = newFunction(ast.Abs, NewOne())
	c.Assert(sf.ConstItem(s.ctx.GetStochaseinstein_dbars().StmtCtx), Equals, true)
}

func (s *testEvaluatorSuite) TestVectorizable(c *C) {
	exprs := make([]Expression, 0, 4)
	sf := newFunction(ast.Rand)
	defCausumn := &DeferredCauset{
		UniqueID: 0,
		RetType:  types.NewFieldType(allegrosql.TypeLonglong),
	}
	exprs = append(exprs, sf)
	exprs = append(exprs, NewOne())
	exprs = append(exprs, NewNull())
	exprs = append(exprs, defCausumn)
	c.Assert(Vectorizable(exprs), Equals, true)

	defCausumn0 := &DeferredCauset{
		UniqueID: 1,
		RetType:  types.NewFieldType(allegrosql.TypeString),
	}
	defCausumn1 := &DeferredCauset{
		UniqueID: 2,
		RetType:  types.NewFieldType(allegrosql.TypeString),
	}
	defCausumn2 := &DeferredCauset{
		UniqueID: 3,
		RetType:  types.NewFieldType(allegrosql.TypeLonglong),
	}
	exprs = exprs[:0]
	sf = newFunction(ast.SetVar, defCausumn0, defCausumn1)
	exprs = append(exprs, sf)
	c.Assert(Vectorizable(exprs), Equals, false)

	exprs = exprs[:0]
	sf = newFunction(ast.GetVar, defCausumn0)
	exprs = append(exprs, sf)
	c.Assert(Vectorizable(exprs), Equals, false)

	exprs = exprs[:0]
	sf = newFunction(ast.NextVal, defCausumn0)
	exprs = append(exprs, sf)
	sf = newFunction(ast.LastVal, defCausumn0)
	exprs = append(exprs, sf)
	sf = newFunction(ast.SetVal, defCausumn1, defCausumn2)
	exprs = append(exprs, sf)
	c.Assert(Vectorizable(exprs), Equals, false)
}

type testBlockBuilder struct {
	blockName       string
	defCausumnNames []string
	tps             []byte
}

func newTestBlockBuilder(blockName string) *testBlockBuilder {
	return &testBlockBuilder{blockName: blockName}
}

func (builder *testBlockBuilder) add(name string, tp byte) *testBlockBuilder {
	builder.defCausumnNames = append(builder.defCausumnNames, name)
	builder.tps = append(builder.tps, tp)
	return builder
}

func (builder *testBlockBuilder) build() *perceptron.BlockInfo {
	ti := &perceptron.BlockInfo{
		ID:    1,
		Name:  perceptron.NewCIStr(builder.blockName),
		State: perceptron.StatePublic,
	}
	for i, defCausName := range builder.defCausumnNames {
		tp := builder.tps[i]
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = allegrosql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.DefCauslate = types.DefaultCharsetForType(tp)
		ti.DeferredCausets = append(ti.DeferredCausets, &perceptron.DeferredCausetInfo{
			ID:        int64(i + 1),
			Name:      perceptron.NewCIStr(defCausName),
			Offset:    i,
			FieldType: *fieldType,
			State:     perceptron.StatePublic,
		})
	}
	return ti
}

func blockInfoToSchemaForTest(blockInfo *perceptron.BlockInfo) *Schema {
	defCausumns := blockInfo.DeferredCausets
	schemaReplicant := NewSchema(make([]*DeferredCauset, 0, len(defCausumns))...)
	for i, defCaus := range defCausumns {
		schemaReplicant.Append(&DeferredCauset{
			UniqueID: int64(i),
			ID:       defCaus.ID,
			RetType:  &defCaus.FieldType,
		})
	}
	return schemaReplicant
}

func (s *testEvaluatorSuite) TestEvalExpr(c *C) {
	ctx := mock.NewContext()
	eTypes := []types.EvalType{types.CausetEDN, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	tNames := []string{"int", "real", "decimal", "string", "timestamp", "datetime", "duration"}
	for i := 0; i < len(tNames); i++ {
		ft := eType2FieldType(eTypes[i])
		defCausExpr := &DeferredCauset{Index: 0, RetType: ft}
		input := chunk.New([]*types.FieldType{ft}, 1024, 1024)
		fillDeferredCausetWithGener(eTypes[i], input, 0, nil)
		defCausBuf := chunk.NewDeferredCauset(ft, 1024)
		defCausBuf2 := chunk.NewDeferredCauset(ft, 1024)
		var err error
		c.Assert(defCausExpr.Vectorized(), IsTrue)
		ctx.GetStochaseinstein_dbars().EnableVectorizedExpression = false
		err = EvalExpr(ctx, defCausExpr, input, defCausBuf)
		if err != nil {
			c.Fatal(err)
		}
		ctx.GetStochaseinstein_dbars().EnableVectorizedExpression = true
		err = EvalExpr(ctx, defCausExpr, input, defCausBuf2)
		if err != nil {
			c.Fatal(err)
		}
		for j := 0; j < 1024; j++ {
			isNull := defCausBuf.IsNull(j)
			isNull2 := defCausBuf2.IsNull(j)
			c.Assert(isNull, Equals, isNull2)
			if isNull {
				continue
			}
			c.Assert(string(defCausBuf.GetRaw(j)), Equals, string(defCausBuf2.GetRaw(j)))
		}
	}
}
