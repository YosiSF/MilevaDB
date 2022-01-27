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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

var _ = Suite(&testExpressionSuite{})

type testExpressionSuite struct{}

func newDeferredCauset(id int) *DeferredCauset {
	return newDeferredCausetWithType(id, types.NewFieldType(allegrosql.TypeLonglong))
}

func newDeferredCausetWithType(id int, t *types.FieldType) *DeferredCauset {
	return &DeferredCauset{
		UniqueID: int64(id),
		RetType:  t,
	}
}

func newLonglong(value int64) *Constant {
	return &Constant{
		Value:   types.NewIntCauset(value),
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
}

func newDate(year, month, day int) *Constant {
	return newTimeConst(year, month, day, 0, 0, 0, allegrosql.TypeDate)
}

func newTimestamp(yy, mm, dd, hh, min, ss int) *Constant {
	return newTimeConst(yy, mm, dd, hh, min, ss, allegrosql.TypeTimestamp)
}

func newTimeConst(yy, mm, dd, hh, min, ss int, tp uint8) *Constant {
	var tmp types.Causet
	tmp.SetMysqlTime(types.NewTime(types.FromDate(yy, mm, dd, 0, 0, 0, 0), tp, types.DefaultFsp))
	return &Constant{
		Value:   tmp,
		RetType: types.NewFieldType(tp),
	}
}

func newFunction(funcName string, args ...Expression) Expression {
	typeLong := types.NewFieldType(allegrosql.TypeLonglong)
	return NewFunctionInternal(mock.NewContext(), funcName, typeLong, args...)
}

func (*testExpressionSuite) TestConstantPropagation(c *C) {
	tests := []struct {
		solver     []PropagateConstantSolver
		conditions []Expression
		result     string
	}{
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.EQ, newDeferredCauset(1), newDeferredCauset(2)),
				newFunction(ast.EQ, newDeferredCauset(2), newDeferredCauset(3)),
				newFunction(ast.EQ, newDeferredCauset(3), newLonglong(1)),
				newFunction(ast.LogicOr, newLonglong(1), newDeferredCauset(0)),
			},
			result: "1, eq(DeferredCauset#0, 1), eq(DeferredCauset#1, 1), eq(DeferredCauset#2, 1), eq(DeferredCauset#3, 1)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.EQ, newDeferredCauset(1), newLonglong(1)),
				newFunction(ast.NE, newDeferredCauset(2), newLonglong(2)),
			},
			result: "eq(DeferredCauset#0, 1), eq(DeferredCauset#1, 1), ne(DeferredCauset#2, 2)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.EQ, newDeferredCauset(1), newLonglong(1)),
				newFunction(ast.EQ, newDeferredCauset(2), newDeferredCauset(3)),
				newFunction(ast.GE, newDeferredCauset(2), newLonglong(2)),
				newFunction(ast.NE, newDeferredCauset(2), newLonglong(4)),
				newFunction(ast.NE, newDeferredCauset(3), newLonglong(5)),
			},
			result: "eq(DeferredCauset#0, 1), eq(DeferredCauset#1, 1), eq(DeferredCauset#2, DeferredCauset#3), ge(DeferredCauset#2, 2), ge(DeferredCauset#3, 2), ne(DeferredCauset#2, 4), ne(DeferredCauset#2, 5), ne(DeferredCauset#3, 4), ne(DeferredCauset#3, 5)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(2)),
				newFunction(ast.GE, newDeferredCauset(1), newLonglong(0)),
			},
			result: "eq(DeferredCauset#0, DeferredCauset#1), eq(DeferredCauset#0, DeferredCauset#2), ge(DeferredCauset#0, 0), ge(DeferredCauset#1, 0), ge(DeferredCauset#2, 0)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.GT, newDeferredCauset(0), newLonglong(2)),
				newFunction(ast.GT, newDeferredCauset(1), newLonglong(3)),
				newFunction(ast.LT, newDeferredCauset(0), newLonglong(1)),
				newFunction(ast.GT, newLonglong(2), newDeferredCauset(1)),
			},
			result: "eq(DeferredCauset#0, DeferredCauset#1), gt(2, DeferredCauset#0), gt(2, DeferredCauset#1), gt(DeferredCauset#0, 2), gt(DeferredCauset#0, 3), gt(DeferredCauset#1, 2), gt(DeferredCauset#1, 3), lt(DeferredCauset#0, 1), lt(DeferredCauset#1, 1)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newLonglong(1), newDeferredCauset(0)),
				newLonglong(0),
			},
			result: "0",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.In, newDeferredCauset(0), newLonglong(1), newLonglong(2)),
				newFunction(ast.In, newDeferredCauset(1), newLonglong(3), newLonglong(4)),
			},
			result: "eq(DeferredCauset#0, DeferredCauset#1), in(DeferredCauset#0, 1, 2), in(DeferredCauset#0, 3, 4), in(DeferredCauset#1, 1, 2), in(DeferredCauset#1, 3, 4)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.EQ, newDeferredCauset(0), newFunction(ast.BitLength, newDeferredCauset(2))),
			},
			result: "eq(DeferredCauset#0, DeferredCauset#1), eq(DeferredCauset#0, bit_length(cast(DeferredCauset#2, var_string(20)))), eq(DeferredCauset#1, bit_length(cast(DeferredCauset#2, var_string(20))))",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.LE, newFunction(ast.Mul, newDeferredCauset(0), newDeferredCauset(0)), newLonglong(50)),
			},
			result: "eq(DeferredCauset#0, DeferredCauset#1), le(mul(DeferredCauset#0, DeferredCauset#0), 50), le(mul(DeferredCauset#1, DeferredCauset#1), 50)",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.LE, newDeferredCauset(0), newFunction(ast.Plus, newDeferredCauset(1), newLonglong(1))),
			},
			result: "eq(DeferredCauset#0, DeferredCauset#1), le(DeferredCauset#0, plus(DeferredCauset#0, 1)), le(DeferredCauset#0, plus(DeferredCauset#1, 1)), le(DeferredCauset#1, plus(DeferredCauset#1, 1))",
		},
		{
			solver: []PropagateConstantSolver{newPropConstSolver()},
			conditions: []Expression{
				newFunction(ast.EQ, newDeferredCauset(0), newDeferredCauset(1)),
				newFunction(ast.LE, newDeferredCauset(0), newFunction(ast.Rand)),
			},
			result: "eq(DeferredCauset#0, DeferredCauset#1), le(cast(DeferredCauset#0, double BINARY), rand())",
		},
	}
	for _, tt := range tests {
		for _, solver := range tt.solver {
			ctx := mock.NewContext()
			conds := make([]Expression, 0, len(tt.conditions))
			for _, cd := range tt.conditions {
				conds = append(conds, FoldConstant(cd))
			}
			newConds := solver.PropagateConstant(ctx, conds)
			var result []string
			for _, v := range newConds {
				result = append(result, v.String())
			}
			sort.Strings(result)
			c.Assert(strings.Join(result, ", "), Equals, tt.result, Commentf("different for expr %s", tt.conditions))
		}
	}
}

func (*testExpressionSuite) TestConstantFolding(c *C) {
	tests := []struct {
		condition Expression
		result    string
	}{
		{
			condition: newFunction(ast.LT, newDeferredCauset(0), newFunction(ast.Plus, newLonglong(1), newLonglong(2))),
			result:    "lt(DeferredCauset#0, 3)",
		},
		{
			condition: newFunction(ast.LT, newDeferredCauset(0), newFunction(ast.Greatest, newLonglong(1), newLonglong(2))),
			result:    "lt(DeferredCauset#0, 2)",
		},
		{
			condition: newFunction(ast.EQ, newDeferredCauset(0), newFunction(ast.Rand)),
			result:    "eq(cast(DeferredCauset#0, double BINARY), rand())",
		},
		{
			condition: newFunction(ast.IsNull, newLonglong(1)),
			result:    "0",
		},
		{
			condition: newFunction(ast.EQ, newDeferredCauset(0), newFunction(ast.UnaryNot, newFunction(ast.Plus, newLonglong(1), newLonglong(1)))),
			result:    "eq(DeferredCauset#0, 0)",
		},
		{
			condition: newFunction(ast.LT, newDeferredCauset(0), newFunction(ast.Plus, newDeferredCauset(1), newFunction(ast.Plus, newLonglong(2), newLonglong(1)))),
			result:    "lt(DeferredCauset#0, plus(DeferredCauset#1, 3))",
		},
	}
	for _, tt := range tests {
		newConds := FoldConstant(tt.condition)
		c.Assert(newConds.String(), Equals, tt.result, Commentf("different for expr %s", tt.condition))
	}
}

func (*testExpressionSuite) TestDeferredExprNullConstantFold(c *C) {
	nullConst := &Constant{
		Value:        types.NewCauset(nil),
		RetType:      types.NewFieldType(allegrosql.TypeTiny),
		DeferredExpr: NewNull(),
	}
	tests := []struct {
		condition Expression
		deferred  string
	}{
		{
			condition: newFunction(ast.LT, newDeferredCauset(0), nullConst),
			deferred:  "lt(DeferredCauset#0, <nil>)",
		},
	}
	for _, tt := range tests {
		comment := Commentf("different for expr %s", tt.condition)
		sf, ok := tt.condition.(*ScalarFunction)
		c.Assert(ok, IsTrue, comment)
		sf.GetCtx().GetStochastikVars().StmtCtx.InNullRejectCheck = true
		newConds := FoldConstant(tt.condition)
		newConst, ok := newConds.(*Constant)
		c.Assert(ok, IsTrue, comment)
		c.Assert(newConst.DeferredExpr.String(), Equals, tt.deferred, comment)
	}
}

func (*testExpressionSuite) TestDeferredParamNotNull(c *C) {
	ctx := mock.NewContext()
	testTime := time.Now()
	ctx.GetStochastikVars().PreparedParams = []types.Causet{
		types.NewIntCauset(1),
		types.NewDecimalCauset(types.NewDecFromStringForTest("20170118123950.123")),
		types.NewTimeCauset(types.NewTime(types.FromGoTime(testTime), allegrosql.TypeTimestamp, 6)),
		types.NewDurationCauset(types.ZeroDuration),
		types.NewStringCauset("{}"),
		types.NewBinaryLiteralCauset([]byte{1}),
		types.NewBytesCauset([]byte{'b'}),
		types.NewFloat32Causet(1.1),
		types.NewFloat64Causet(2.1),
		types.NewUintCauset(100),
		types.NewMysqlBitCauset([]byte{1}),
		types.NewMysqlEnumCauset(types.Enum{Name: "n", Value: 2}),
	}
	cstInt := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 0}, RetType: newIntFieldType()}
	cstDec := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 1}, RetType: newDecimalFieldType()}
	cstTime := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 2}, RetType: newDateFieldType()}
	cstDuration := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 3}, RetType: newDurFieldType()}
	cstJSON := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 4}, RetType: newJSONFieldType()}
	cstBytes := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 6}, RetType: newBlobFieldType()}
	cstBinary := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 5}, RetType: newBinaryLiteralFieldType()}
	cstFloat32 := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 7}, RetType: newFloatFieldType()}
	cstFloat64 := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 8}, RetType: newFloatFieldType()}
	cstUint := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 9}, RetType: newIntFieldType()}
	cstBit := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 10}, RetType: newBinaryLiteralFieldType()}
	cstEnum := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 11}, RetType: newEnumFieldType()}

	c.Assert(allegrosql.TypeVarString, Equals, cstJSON.GetType().Tp)
	c.Assert(allegrosql.TypeNewDecimal, Equals, cstDec.GetType().Tp)
	c.Assert(allegrosql.TypeLonglong, Equals, cstInt.GetType().Tp)
	c.Assert(allegrosql.TypeLonglong, Equals, cstUint.GetType().Tp)
	c.Assert(allegrosql.TypeTimestamp, Equals, cstTime.GetType().Tp)
	c.Assert(allegrosql.TypeDuration, Equals, cstDuration.GetType().Tp)
	c.Assert(allegrosql.TypeBlob, Equals, cstBytes.GetType().Tp)
	c.Assert(allegrosql.TypeBit, Equals, cstBinary.GetType().Tp)
	c.Assert(allegrosql.TypeBit, Equals, cstBit.GetType().Tp)
	c.Assert(allegrosql.TypeFloat, Equals, cstFloat32.GetType().Tp)
	c.Assert(allegrosql.TypeDouble, Equals, cstFloat64.GetType().Tp)
	c.Assert(allegrosql.TypeEnum, Equals, cstEnum.GetType().Tp)

	d, _, err := cstInt.EvalInt(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(d, Equals, int64(1))
	r, _, err := cstFloat64.EvalReal(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(r, Equals, float64(2.1))
	de, _, err := cstDec.EvalDecimal(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(de.String(), Equals, "20170118123950.123")
	s, _, err := cstBytes.EvalString(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(s, Equals, "b")
	t, _, err := cstTime.EvalTime(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(t.Compare(ctx.GetStochastikVars().PreparedParams[2].GetMysqlTime()), Equals, 0)
	dur, _, err := cstDuration.EvalDuration(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(dur.Duration, Equals, types.ZeroDuration.Duration)
	json, _, err := cstJSON.EvalJSON(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(json, NotNil)
}

func (*testExpressionSuite) TestDeferredExprNotNull(c *C) {
	m := &MockExpr{}
	ctx := mock.NewContext()
	cst := &Constant{DeferredExpr: m, RetType: newIntFieldType()}
	m.i, m.err = nil, fmt.Errorf("ERROR")
	_, _, err := cst.EvalInt(ctx, chunk.Event{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalReal(ctx, chunk.Event{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalDecimal(ctx, chunk.Event{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalString(ctx, chunk.Event{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalTime(ctx, chunk.Event{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalDuration(ctx, chunk.Event{})
	c.Assert(err, NotNil)
	_, _, err = cst.EvalJSON(ctx, chunk.Event{})
	c.Assert(err, NotNil)

	m.i, m.err = nil, nil
	_, isNull, err := cst.EvalInt(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalReal(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalDecimal(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalString(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalTime(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalDuration(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	_, isNull, err = cst.EvalJSON(ctx, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)

	m.i = int64(2333)
	xInt, _, _ := cst.EvalInt(ctx, chunk.Event{})
	c.Assert(xInt, Equals, int64(2333))

	m.i = float64(123.45)
	xFlo, _, _ := cst.EvalReal(ctx, chunk.Event{})
	c.Assert(xFlo, Equals, float64(123.45))

	m.i = "abc"
	xStr, _, _ := cst.EvalString(ctx, chunk.Event{})
	c.Assert(xStr, Equals, "abc")

	m.i = &types.MyDecimal{}
	xDec, _, _ := cst.EvalDecimal(ctx, chunk.Event{})
	c.Assert(xDec.Compare(m.i.(*types.MyDecimal)), Equals, 0)

	m.i = types.ZeroTime
	xTim, _, _ := cst.EvalTime(ctx, chunk.Event{})
	c.Assert(xTim.Compare(m.i.(types.Time)), Equals, 0)

	m.i = types.Duration{}
	xDur, _, _ := cst.EvalDuration(ctx, chunk.Event{})
	c.Assert(xDur.Compare(m.i.(types.Duration)), Equals, 0)

	m.i = json.BinaryJSON{}
	xJsn, _, _ := cst.EvalJSON(ctx, chunk.Event{})
	c.Assert(m.i.(json.BinaryJSON).String(), Equals, xJsn.String())

	cln := cst.Clone().(*Constant)
	c.Assert(cln.DeferredExpr, Equals, cst.DeferredExpr)
}

func (*testExpressionSuite) TestVectorizedConstant(c *C) {
	// fixed-length type with/without Sel
	for _, cst := range []*Constant{
		{RetType: newIntFieldType(), Value: types.NewIntCauset(2333)},
		{RetType: newIntFieldType(), DeferredExpr: &Constant{RetType: newIntFieldType(), Value: types.NewIntCauset(2333)}}} {
		chk := chunk.New([]*types.FieldType{newIntFieldType()}, 1024, 1024)
		for i := 0; i < 1024; i++ {
			chk.AppendInt64(0, int64(i))
		}
		defCaus := chunk.NewDeferredCauset(newIntFieldType(), 1024)
		ctx := mock.NewContext()
		c.Assert(cst.VecEvalInt(ctx, chk, defCaus), IsNil)
		i64s := defCaus.Int64s()
		c.Assert(len(i64s), Equals, 1024)
		for _, v := range i64s {
			c.Assert(v, Equals, int64(2333))
		}

		// fixed-length type with Sel
		sel := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
		chk.SetSel(sel)
		c.Assert(cst.VecEvalInt(ctx, chk, defCaus), IsNil)
		i64s = defCaus.Int64s()
		for i := range sel {
			c.Assert(i64s[i], Equals, int64(2333))
		}
	}

	// var-length type with/without Sel
	for _, cst := range []*Constant{
		{RetType: newStringFieldType(), Value: types.NewStringCauset("hello")},
		{RetType: newStringFieldType(), DeferredExpr: &Constant{RetType: newStringFieldType(), Value: types.NewStringCauset("hello")}}} {
		chk := chunk.New([]*types.FieldType{newIntFieldType()}, 1024, 1024)
		for i := 0; i < 1024; i++ {
			chk.AppendInt64(0, int64(i))
		}
		cst = &Constant{DeferredExpr: nil, RetType: newStringFieldType(), Value: types.NewStringCauset("hello")}
		chk.SetSel(nil)
		defCaus := chunk.NewDeferredCauset(newStringFieldType(), 1024)
		ctx := mock.NewContext()
		c.Assert(cst.VecEvalString(ctx, chk, defCaus), IsNil)
		for i := 0; i < 1024; i++ {
			c.Assert(defCaus.GetString(i), Equals, "hello")
		}

		// var-length type with Sel
		sel := []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29}
		chk.SetSel(sel)
		c.Assert(cst.VecEvalString(ctx, chk, defCaus), IsNil)
		for i := range sel {
			c.Assert(defCaus.GetString(i), Equals, "hello")
		}
	}
}

func (*testExpressionSuite) TestGetTypeThreadSafe(c *C) {
	ctx := mock.NewContext()
	ctx.GetStochastikVars().PreparedParams = []types.Causet{
		types.NewIntCauset(1),
	}
	con := &Constant{ParamMarker: &ParamMarker{ctx: ctx, order: 0}, RetType: newStringFieldType()}
	ft1 := con.GetType()
	ft2 := con.GetType()
	c.Assert(ft1, Not(Equals), ft2)
}
