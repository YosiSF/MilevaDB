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
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

var _ = Suite(&testEvalSuite{})
var _ = SerialSuites(&testEvalSerialSuite{})

type testEvalSuite struct {
	defCausID int64
}

type testEvalSerialSuite struct {
}

func (s *testEvalSerialSuite) TestPBToExprWithNewDefCauslation(c *C) {
	sc := new(stmtctx.StatementContext)
	fieldTps := make([]*types.FieldType, 1)

	cases := []struct {
		name    string
		expName string
		id      int32
		pbID    int32
	}{
		{"utf8_general_ci", "utf8_general_ci", 33, 33},
		{"UTF8MB4_BIN", "utf8mb4_bin", 46, 46},
		{"utf8mb4_bin", "utf8mb4_bin", 46, 46},
		{"utf8mb4_general_ci", "utf8mb4_general_ci", 45, 45},
		{"", "utf8mb4_bin", 46, 46},
		{"some_error_defCauslation", "utf8mb4_bin", 46, 46},
		{"utf8_unicode_ci", "utf8_unicode_ci", 192, 192},
		{"utf8mb4_unicode_ci", "utf8mb4_unicode_ci", 224, 224},
	}

	for _, cs := range cases {
		ft := types.NewFieldType(allegrosql.TypeString)
		ft.DefCauslate = cs.name
		expr := new(fidelpb.Expr)
		expr.Tp = fidelpb.ExprType_String
		expr.FieldType = toPBFieldType(ft)
		c.Assert(expr.FieldType.DefCauslate, Equals, cs.pbID)

		e, err := PBToExpr(expr, fieldTps, sc)
		c.Assert(err, IsNil)
		cons, ok := e.(*Constant)
		c.Assert(ok, IsTrue)
		c.Assert(cons.Value.DefCauslation(), Equals, cs.expName)
	}
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	for _, cs := range cases {
		ft := types.NewFieldType(allegrosql.TypeString)
		ft.DefCauslate = cs.name
		expr := new(fidelpb.Expr)
		expr.Tp = fidelpb.ExprType_String
		expr.FieldType = toPBFieldType(ft)
		c.Assert(expr.FieldType.DefCauslate, Equals, -cs.pbID)

		e, err := PBToExpr(expr, fieldTps, sc)
		c.Assert(err, IsNil)
		cons, ok := e.(*Constant)
		c.Assert(ok, IsTrue)
		c.Assert(cons.Value.DefCauslation(), Equals, cs.expName)
	}
}

func (s *testEvalSuite) SetUpSuite(c *C) {
	s.defCausID = 0
}

func (s *testEvalSuite) allocDefCausID() int64 {
	s.defCausID++
	return s.defCausID
}

func (s *testEvalSuite) TearDownTest(c *C) {
	s.defCausID = 0
}

func (s *testEvalSuite) TestPBToExpr(c *C) {
	sc := new(stmtctx.StatementContext)
	fieldTps := make([]*types.FieldType, 1)
	ds := []types.Causet{types.NewIntCauset(1), types.NewUintCauset(1), types.NewFloat64Causet(1),
		types.NewDecimalCauset(newMyDecimal(c, "1")), types.NewDurationCauset(newDuration(time.Second))}

	for _, d := range ds {
		expr := datumExpr(c, d)
		expr.Val = expr.Val[:len(expr.Val)/2]
		_, err := PBToExpr(expr, fieldTps, sc)
		c.Assert(err, NotNil)
	}

	expr := &fidelpb.Expr{
		Tp: fidelpb.ExprType_ScalarFunc,
		Children: []*fidelpb.Expr{
			{
				Tp: fidelpb.ExprType_ValueList,
			},
		},
	}
	_, err := PBToExpr(expr, fieldTps, sc)
	c.Assert(err, IsNil)

	val := make([]byte, 0, 32)
	val = codec.EncodeInt(val, 1)
	expr = &fidelpb.Expr{
		Tp: fidelpb.ExprType_ScalarFunc,
		Children: []*fidelpb.Expr{
			{
				Tp:  fidelpb.ExprType_ValueList,
				Val: val[:len(val)/2],
			},
		},
	}
	_, err = PBToExpr(expr, fieldTps, sc)
	c.Assert(err, NotNil)

	expr = &fidelpb.Expr{
		Tp: fidelpb.ExprType_ScalarFunc,
		Children: []*fidelpb.Expr{
			{
				Tp:  fidelpb.ExprType_ValueList,
				Val: val,
			},
		},
		Sig:       fidelpb.ScalarFuncSig_AbsInt,
		FieldType: ToPBFieldType(newIntFieldType()),
	}
	_, err = PBToExpr(expr, fieldTps, sc)
	c.Assert(err, NotNil)
}

// TestEval test expr.Eval().
func (s *testEvalSuite) TestEval(c *C) {
	event := chunk.MutEventFromCausets([]types.Causet{types.NewCauset(100)}).ToEvent()
	fieldTps := make([]*types.FieldType, 1)
	fieldTps[0] = types.NewFieldType(allegrosql.TypeLonglong)
	tests := []struct {
		expr   *fidelpb.Expr
		result types.Causet
	}{
		// Causets.
		{
			datumExpr(c, types.NewFloat32Causet(1.1)),
			types.NewFloat32Causet(1.1),
		},
		{
			datumExpr(c, types.NewFloat64Causet(1.1)),
			types.NewFloat64Causet(1.1),
		},
		{
			datumExpr(c, types.NewIntCauset(1)),
			types.NewIntCauset(1),
		},
		{
			datumExpr(c, types.NewUintCauset(1)),
			types.NewUintCauset(1),
		},
		{
			datumExpr(c, types.NewBytesCauset([]byte("abc"))),
			types.NewBytesCauset([]byte("abc")),
		},
		{
			datumExpr(c, types.NewStringCauset("abc")),
			types.NewStringCauset("abc"),
		},
		{
			datumExpr(c, types.Causet{}),
			types.Causet{},
		},
		{
			datumExpr(c, types.NewDurationCauset(types.Duration{Duration: time.Hour})),
			types.NewDurationCauset(types.Duration{Duration: time.Hour}),
		},
		{
			datumExpr(c, types.NewDecimalCauset(types.NewDecFromFloatForTest(1.1))),
			types.NewDecimalCauset(types.NewDecFromFloatForTest(1.1)),
		},
		// DeferredCausets.
		{
			defCausumnExpr(0),
			types.NewIntCauset(100),
		},
		// Scalar Functions.
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_JsonDepthSig,
				toPBFieldType(newIntFieldType()),
				jsonCausetExpr(c, `true`),
			),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_JsonDepthSig,
				toPBFieldType(newIntFieldType()),
				jsonCausetExpr(c, `[10, {"a": 20}]`),
			),
			types.NewIntCauset(3),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_JsonStorageSizeSig,
				toPBFieldType(newIntFieldType()),
				jsonCausetExpr(c, `[{"a":{"a":1},"b":2}]`),
			),
			types.NewIntCauset(25),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_JsonSearchSig,
				toPBFieldType(newJSONFieldType()),
				jsonCausetExpr(c, `["abc", [{"k": "10"}, "def"], {"x":"abc"}, {"y":"bcd"}]`),
				datumExpr(c, types.NewBytesCauset([]byte(`all`))),
				datumExpr(c, types.NewBytesCauset([]byte(`10`))),
				datumExpr(c, types.NewBytesCauset([]byte(`\`))),
				datumExpr(c, types.NewBytesCauset([]byte(`$**.k`))),
			),
			newJSONCauset(c, `"$[1][0].k"`),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastIntAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(2333))),
			types.NewIntCauset(2333),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastRealAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Causet(2333))),
			types.NewIntCauset(2333),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastStringAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringCauset("2333"))),
			types.NewIntCauset(2333),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastDecimalAsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2333")))),
			types.NewIntCauset(2333),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastIntAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewIntCauset(2333))),
			types.NewFloat64Causet(2333),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastRealAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Causet(2333))),
			types.NewFloat64Causet(2333),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastStringAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewStringCauset("2333"))),
			types.NewFloat64Causet(2333),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastDecimalAsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2333")))),
			types.NewFloat64Causet(2333),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastStringAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewStringCauset("2333"))),
			types.NewStringCauset("2333"),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastIntAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewIntCauset(2333))),
			types.NewStringCauset("2333"),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastRealAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewFloat64Causet(2333))),
			types.NewStringCauset("2333"),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastDecimalAsString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2333")))),
			types.NewStringCauset("2333"),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastDecimalAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2333")))),
			types.NewDecimalCauset(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastIntAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewIntCauset(2333))),
			types.NewDecimalCauset(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastRealAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewFloat64Causet(2333))),
			types.NewDecimalCauset(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastStringAsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewStringCauset("2333"))),
			types.NewDecimalCauset(newMyDecimal(c, "2333")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_GEInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(2)), datumExpr(c, types.NewIntCauset(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LEInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(2))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NEInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(2))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NullEQInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewCauset(nil))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_GEReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Causet(2)), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LEReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Causet(1)), datumExpr(c, types.NewFloat64Causet(2))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LTReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Causet(1)), datumExpr(c, types.NewFloat64Causet(2))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_EQReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Causet(1)), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NEReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Causet(1)), datumExpr(c, types.NewFloat64Causet(2))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NullEQReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewCauset(nil))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_GEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2"))), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2")))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LTDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2")))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_EQDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NEDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2")))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NullEQDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewCauset(nil))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_GEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationCauset(newDuration(time.Second*2))), datumExpr(c, types.NewDurationCauset(newDuration(time.Second)))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_GTDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationCauset(newDuration(time.Second*2))), datumExpr(c, types.NewDurationCauset(newDuration(time.Second)))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_EQDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationCauset(newDuration(time.Second))), datumExpr(c, types.NewDurationCauset(newDuration(time.Second)))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationCauset(newDuration(time.Second))), datumExpr(c, types.NewDurationCauset(newDuration(time.Second*2)))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NEDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationCauset(newDuration(time.Second))), datumExpr(c, types.NewDurationCauset(newDuration(time.Second*2)))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NullEQDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewCauset(nil))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_GEString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringCauset("1")), datumExpr(c, types.NewStringCauset("1"))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LEString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringCauset("1")), datumExpr(c, types.NewStringCauset("1"))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NEString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringCauset("2")), datumExpr(c, types.NewStringCauset("1"))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NullEQString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewCauset(nil))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_GTJson,
				toPBFieldType(newIntFieldType()), jsonCausetExpr(c, "[2]"), jsonCausetExpr(c, "[1]")),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_GEJson,
				toPBFieldType(newIntFieldType()), jsonCausetExpr(c, "[2]"), jsonCausetExpr(c, "[1]")),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LTJson,
				toPBFieldType(newIntFieldType()), jsonCausetExpr(c, "[1]"), jsonCausetExpr(c, "[2]")),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LEJson,
				toPBFieldType(newIntFieldType()), jsonCausetExpr(c, "[1]"), jsonCausetExpr(c, "[2]")),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_EQJson,
				toPBFieldType(newIntFieldType()), jsonCausetExpr(c, "[1]"), jsonCausetExpr(c, "[1]")),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NEJson,
				toPBFieldType(newIntFieldType()), jsonCausetExpr(c, "[1]"), jsonCausetExpr(c, "[2]")),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_NullEQJson,
				toPBFieldType(newIntFieldType()), jsonCausetExpr(c, "[1]"), jsonCausetExpr(c, "[1]")),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_DecimalIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(nil))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_DurationIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(nil))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_RealIsNull,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(nil))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LeftShift,
				ToPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(1)), datumExpr(c, types.NewIntCauset(1))),
			types.NewIntCauset(2),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_AbsInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(-1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_AbsUInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewUintCauset(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_AbsReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Causet(-1.23))),
			types.NewFloat64Causet(1.23),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_AbsDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "-1.23")))),
			types.NewDecimalCauset(newMyDecimal(c, "1.23")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LogicalAnd,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LogicalOr,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(0))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_LogicalXor,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(0))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_BitAndSig,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_BitOrSig,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(0))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_BitXorSig,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(0))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_BitNegSig,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(0))),
			types.NewIntCauset(-1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_InReal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Causet(1)), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_InDecimal,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_InString,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewStringCauset("1")), datumExpr(c, types.NewStringCauset("1"))),
			types.NewIntCauset(1),
		},
		//{
		//	scalarFunctionExpr(fidelpb.ScalarFuncSig_InTime,
		//		toPBFieldType(newIntFieldType()), datumExpr(c, types.NewTimeCauset(types.ZeroDate)), datumExpr(c, types.NewTimeCauset(types.ZeroDate))),
		//	types.NewIntCauset(1),
		//},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_InDuration,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDurationCauset(newDuration(time.Second))), datumExpr(c, types.NewDurationCauset(newDuration(time.Second)))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfNullInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewIntCauset(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(2))),
			types.NewIntCauset(2),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfNullReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewFloat64Causet(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Causet(1)), datumExpr(c, types.NewFloat64Causet(2))),
			types.NewFloat64Causet(2),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfNullDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewDecimalCauset(newMyDecimal(c, "1")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2")))),
			types.NewDecimalCauset(newMyDecimal(c, "2")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfNullString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewStringCauset("1"))),
			types.NewStringCauset("1"),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewStringCauset("2"))),
			types.NewStringCauset("2"),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfNullDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewCauset(nil)), datumExpr(c, types.NewDurationCauset(newDuration(time.Second)))),
			types.NewDurationCauset(newDuration(time.Second)),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_IfDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewDurationCauset(newDuration(time.Second*2)))),
			types.NewDurationCauset(newDuration(time.Second * 2)),
		},

		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastIntAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewIntCauset(1))),
			types.NewDurationCauset(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastRealAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewDurationCauset(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastDecimalAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewDurationCauset(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastDurationAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewDurationCauset(newDuration(time.Second*1)))),
			types.NewDurationCauset(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastStringAsDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewStringCauset("1"))),
			types.NewDurationCauset(newDuration(time.Second * 1)),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastTimeAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewTimeCauset(newDateTime(c, "2000-01-01")))),
			types.NewTimeCauset(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastIntAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewIntCauset(20000101))),
			types.NewTimeCauset(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastRealAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewFloat64Causet(20000101))),
			types.NewTimeCauset(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastDecimalAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "20000101")))),
			types.NewTimeCauset(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CastStringAsTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewStringCauset("20000101"))),
			types.NewTimeCauset(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_PlusInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(2))),
			types.NewIntCauset(3),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_PlusDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2")))),
			types.NewDecimalCauset(newMyDecimal(c, "3")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_PlusReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Causet(1)), datumExpr(c, types.NewFloat64Causet(2))),
			types.NewFloat64Causet(3),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_MinusInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(2))),
			types.NewIntCauset(-1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_MinusDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2")))),
			types.NewDecimalCauset(newMyDecimal(c, "-1")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_MinusReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Causet(1)), datumExpr(c, types.NewFloat64Causet(2))),
			types.NewFloat64Causet(-1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_MultiplyInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1)), datumExpr(c, types.NewIntCauset(2))),
			types.NewIntCauset(2),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_MultiplyDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1"))), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "2")))),
			types.NewDecimalCauset(newMyDecimal(c, "2")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_MultiplyReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Causet(1)), datumExpr(c, types.NewFloat64Causet(2))),
			types.NewFloat64Causet(2),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CeilIntToInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CeilIntToDec,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewIntCauset(1))),
			types.NewDecimalCauset(newMyDecimal(c, "1")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CeilDecToInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CeilReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewFloat64Causet(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_FloorIntToInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_FloorIntToDec,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewIntCauset(1))),
			types.NewDecimalCauset(newMyDecimal(c, "1")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_FloorDecToInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_FloorReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewFloat64Causet(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CoalesceInt,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewIntCauset(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CoalesceReal,
				toPBFieldType(newRealFieldType()), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewFloat64Causet(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CoalesceDecimal,
				toPBFieldType(newDecimalFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewDecimalCauset(newMyDecimal(c, "1")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CoalesceString,
				toPBFieldType(newStringFieldType()), datumExpr(c, types.NewStringCauset("1"))),
			types.NewStringCauset("1"),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CoalesceDuration,
				toPBFieldType(newDurFieldType()), datumExpr(c, types.NewDurationCauset(newDuration(time.Second)))),
			types.NewDurationCauset(newDuration(time.Second)),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CoalesceTime,
				toPBFieldType(newDateFieldType()), datumExpr(c, types.NewTimeCauset(newDateTime(c, "2000-01-01")))),
			types.NewTimeCauset(newDateTime(c, "2000-01-01")),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CaseWhenInt,
				toPBFieldType(newIntFieldType())),
			types.NewCauset(nil),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CaseWhenReal,
				toPBFieldType(newRealFieldType())),
			types.NewCauset(nil),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CaseWhenDecimal,
				toPBFieldType(newDecimalFieldType())),
			types.NewCauset(nil),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CaseWhenDuration,
				toPBFieldType(newDurFieldType())),
			types.NewCauset(nil),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CaseWhenTime,
				toPBFieldType(newDateFieldType())),
			types.NewCauset(nil),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_CaseWhenJson,
				toPBFieldType(newJSONFieldType())),
			types.NewCauset(nil),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_RealIsFalse,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewIntCauset(0),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_DecimalIsFalse,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewIntCauset(0),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_RealIsTrue,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewFloat64Causet(1))),
			types.NewIntCauset(1),
		},
		{
			scalarFunctionExpr(fidelpb.ScalarFuncSig_DecimalIsTrue,
				toPBFieldType(newIntFieldType()), datumExpr(c, types.NewDecimalCauset(newMyDecimal(c, "1")))),
			types.NewIntCauset(1),
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		expr, err := PBToExpr(tt.expr, fieldTps, sc)
		c.Assert(err, IsNil)
		result, err := expr.Eval(event)
		c.Assert(err, IsNil)
		c.Assert(result.HoTT(), Equals, tt.result.HoTT())
		cmp, err := result.CompareCauset(sc, &tt.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func datumExpr(c *C, d types.Causet) *fidelpb.Expr {
	expr := new(fidelpb.Expr)
	switch d.HoTT() {
	case types.HoTTInt64:
		expr.Tp = fidelpb.ExprType_Int64
		expr.Val = codec.EncodeInt(nil, d.GetInt64())
	case types.HoTTUint64:
		expr.Tp = fidelpb.ExprType_Uint64
		expr.Val = codec.EncodeUint(nil, d.GetUint64())
	case types.HoTTString:
		expr.Tp = fidelpb.ExprType_String
		expr.FieldType = toPBFieldType(types.NewFieldType(allegrosql.TypeString))
		expr.Val = d.GetBytes()
	case types.HoTTBytes:
		expr.Tp = fidelpb.ExprType_Bytes
		expr.Val = d.GetBytes()
	case types.HoTTFloat32:
		expr.Tp = fidelpb.ExprType_Float32
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.HoTTFloat64:
		expr.Tp = fidelpb.ExprType_Float64
		expr.Val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.HoTTMysqlDuration:
		expr.Tp = fidelpb.ExprType_MysqlDuration
		expr.Val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.HoTTMysqlDecimal:
		expr.Tp = fidelpb.ExprType_MysqlDecimal
		var err error
		expr.Val, err = codec.EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		c.Assert(err, IsNil)
	case types.HoTTMysqlJSON:
		expr.Tp = fidelpb.ExprType_MysqlJson
		var err error
		expr.Val = make([]byte, 0, 1024)
		expr.Val, err = codec.EncodeValue(nil, expr.Val, d)
		c.Assert(err, IsNil)
	case types.HoTTMysqlTime:
		expr.Tp = fidelpb.ExprType_MysqlTime
		var err error
		expr.Val, err = codec.EncodeMyALLEGROSQLTime(nil, d.GetMysqlTime(), allegrosql.TypeUnspecified, nil)
		c.Assert(err, IsNil)
		expr.FieldType = ToPBFieldType(newDateFieldType())
	default:
		expr.Tp = fidelpb.ExprType_Null
	}
	return expr
}

func newJSONCauset(c *C, s string) (d types.Causet) {
	j, err := json.ParseBinaryFromString(s)
	c.Assert(err, IsNil)
	d.SetMysqlJSON(j)
	return d
}

func jsonCausetExpr(c *C, s string) *fidelpb.Expr {
	return datumExpr(c, newJSONCauset(c, s))
}

func defCausumnExpr(defCausumnID int64) *fidelpb.Expr {
	expr := new(fidelpb.Expr)
	expr.Tp = fidelpb.ExprType_DeferredCausetRef
	expr.Val = codec.EncodeInt(nil, defCausumnID)
	return expr
}

// toPBFieldType converts *types.FieldType to *fidelpb.FieldType.
func toPBFieldType(ft *types.FieldType) *fidelpb.FieldType {
	return &fidelpb.FieldType{
		Tp:          int32(ft.Tp),
		Flag:        uint32(ft.Flag),
		Flen:        int32(ft.Flen),
		Decimal:     int32(ft.Decimal),
		Charset:     ft.Charset,
		DefCauslate: defCauslationToProto(ft.DefCauslate),
	}
}

func newMyDecimal(c *C, s string) *types.MyDecimal {
	d := new(types.MyDecimal)
	c.Assert(d.FromString([]byte(s)), IsNil)
	return d
}

func newDuration(dur time.Duration) types.Duration {
	return types.Duration{
		Duration: dur,
		Fsp:      types.DefaultFsp,
	}
}

func newDateTime(c *C, s string) types.Time {
	t, err := types.ParseDate(nil, s)
	c.Assert(err, IsNil)
	return t
}

func newDateFieldType() *types.FieldType {
	return &types.FieldType{
		Tp: allegrosql.TypeDate,
	}
}

func newIntFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      allegrosql.TypeLonglong,
		Flen:    allegrosql.MaxIntWidth,
		Decimal: 0,
		Flag:    allegrosql.BinaryFlag,
	}
}

func newDurFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:      allegrosql.TypeDuration,
		Decimal: int(types.DefaultFsp),
	}
}

func newStringFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   allegrosql.TypeVarString,
		Flen: types.UnspecifiedLength,
	}
}

func newRealFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   allegrosql.TypeFloat,
		Flen: types.UnspecifiedLength,
	}
}

func newDecimalFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:   allegrosql.TypeNewDecimal,
		Flen: types.UnspecifiedLength,
	}
}

func newJSONFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:          allegrosql.TypeJSON,
		Flen:        types.UnspecifiedLength,
		Decimal:     0,
		Charset:     charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
	}
}

func newFloatFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:          allegrosql.TypeFloat,
		Flen:        types.UnspecifiedLength,
		Decimal:     0,
		Charset:     charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
	}
}

func newBinaryLiteralFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:          allegrosql.TypeBit,
		Flen:        types.UnspecifiedLength,
		Decimal:     0,
		Charset:     charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
	}
}

func newBlobFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:          allegrosql.TypeBlob,
		Flen:        types.UnspecifiedLength,
		Decimal:     0,
		Charset:     charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
	}
}

func newEnumFieldType() *types.FieldType {
	return &types.FieldType{
		Tp:          allegrosql.TypeEnum,
		Flen:        types.UnspecifiedLength,
		Decimal:     0,
		Charset:     charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
	}
}

func scalarFunctionExpr(sigCode fidelpb.ScalarFuncSig, retType *fidelpb.FieldType, args ...*fidelpb.Expr) *fidelpb.Expr {
	return &fidelpb.Expr{
		Tp:        fidelpb.ExprType_ScalarFunc,
		Sig:       sigCode,
		Children:  args,
		FieldType: retType,
	}
}
