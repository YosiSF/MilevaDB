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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

func init() {
	fpname := "github.com/whtcorpsinc/MilevaDB-Prod/expression/PanicIfPbCodeUnspecified"
	err := failpoint.Enable(fpname, "return(true)")
	if err != nil {
		panic(errors.Errorf("enable global failpoint `%s` failed: %v", fpname, err))
	}
}

type dataGen4Expr2PbTest struct {
}

func (dg *dataGen4Expr2PbTest) genDeferredCauset(tp byte, id int64) *DeferredCauset {
	return &DeferredCauset{
		RetType: types.NewFieldType(tp),
		ID:      id,
		Index:   int(id),
	}
}

func (s *testEvaluatorSuite) TestCouplingConstantWithRadix2Pb(c *C) {
	c.Skip("constant pb has changed")
	var constExprs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	// can be transformed
	constValue := new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset(nil)
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTNull)
	constExprs = append(constExprs, constValue)

	constValue = new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset(int64(100))
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTInt64)
	constExprs = append(constExprs, constValue)

	constValue = new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset(uint64(100))
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTUint64)
	constExprs = append(constExprs, constValue)

	constValue = new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset("100")
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTString)
	constExprs = append(constExprs, constValue)

	constValue = new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset([]byte{'1', '2', '4', 'c'})
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTBytes)
	constExprs = append(constExprs, constValue)

	constValue = new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset(types.NewDecFromInt(110))
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTMysqlDecimal)
	constExprs = append(constExprs, constValue)

	constValue = new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset(types.Duration{})
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTMysqlDuration)
	constExprs = append(constExprs, constValue)

	// can not be transformed
	constValue = new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset(float32(100))
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTFloat32)
	constExprs = append(constExprs, constValue)

	constValue = new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset(float64(100))
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTFloat64)
	constExprs = append(constExprs, constValue)

	constValue = new(CouplingConstantWithRadix)
	constValue.Value = types.NewCauset(types.Enum{Name: "A", Value: 19})
	c.Assert(constValue.Value.HoTT(), Equals, types.HoTTMysqlEnum)
	constExprs = append(constExprs, constValue)

	pushed, remained := PushDownExprs(sc, constExprs, client, ekv.UnSpecified)
	c.Assert(len(pushed), Equals, len(constExprs)-3)
	c.Assert(len(remained), Equals, 3)

	pbExprs, err := ExpressionsToPBList(sc, constExprs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":0,\"sig\":0}",
		"{\"tp\":1,\"val\":\"gAAAAAAAAGQ=\",\"sig\":0}",
		"{\"tp\":2,\"val\":\"AAAAAAAAAGQ=\",\"sig\":0}",
		"{\"tp\":5,\"val\":\"MTAw\",\"sig\":0}",
		"{\"tp\":6,\"val\":\"MTI0Yw==\",\"sig\":0}",
		"{\"tp\":102,\"val\":\"AwCAbg==\",\"sig\":0}",
		"{\"tp\":103,\"val\":\"gAAAAAAAAAA=\",\"sig\":0}",
	}
	for i, pbExpr := range pbExprs {
		if i+3 < len(pbExprs) {
			js, err := json.Marshal(pbExpr)
			c.Assert(err, IsNil)
			c.Assert(string(js), Equals, jsons[i])
		} else {
			c.Assert(pbExpr, IsNil)
		}
	}
}

func (s *testEvaluatorSuite) TestDeferredCauset2Pb(c *C) {
	var defCausExprs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeBit, 1))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeSet, 2))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeEnum, 3))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeGeometry, 4))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeUnspecified, 5))

	pushed, remained := PushDownExprs(sc, defCausExprs, client, ekv.UnSpecified)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, len(defCausExprs))

	for _, defCaus := range defCausExprs { // cannot be pushed down
		_, err := ExpressionsToPBList(sc, []Expression{defCaus}, client)
		c.Assert(err, NotNil)
	}

	defCausExprs = defCausExprs[:0]
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeTiny, 1))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeShort, 2))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeLong, 3))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeFloat, 4))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeDouble, 5))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeNull, 6))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeTimestamp, 7))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeLonglong, 8))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeInt24, 9))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeDate, 10))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeDuration, 11))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeDatetime, 12))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeYear, 13))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeVarchar, 15))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeJSON, 16))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeNewDecimal, 17))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeTinyBlob, 18))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeMediumBlob, 19))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeLongBlob, 20))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeBlob, 21))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeVarString, 22))
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeString, 23))
	pushed, remained = PushDownExprs(sc, defCausExprs, client, ekv.UnSpecified)
	c.Assert(len(pushed), Equals, len(defCausExprs))
	c.Assert(len(remained), Equals, 0)

	pbExprs, err := ExpressionsToPBList(sc, defCausExprs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":2,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":4,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAY=\",\"sig\":0,\"field_type\":{\"tp\":6,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAc=\",\"sig\":0,\"field_type\":{\"tp\":7,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAg=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAk=\",\"sig\":0,\"field_type\":{\"tp\":9,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAo=\",\"sig\":0,\"field_type\":{\"tp\":10,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAs=\",\"sig\":0,\"field_type\":{\"tp\":11,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAw=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA0=\",\"sig\":0,\"field_type\":{\"tp\":13,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAA8=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABA=\",\"sig\":0,\"field_type\":{\"tp\":245,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABE=\",\"sig\":0,\"field_type\":{\"tp\":246,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABI=\",\"sig\":0,\"field_type\":{\"tp\":249,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABM=\",\"sig\":0,\"field_type\":{\"tp\":250,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABQ=\",\"sig\":0,\"field_type\":{\"tp\":251,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABU=\",\"sig\":0,\"field_type\":{\"tp\":252,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABY=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAABc=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":46,\"charset\":\"\"}}",
	}
	for i, pbExpr := range pbExprs {
		c.Assert(pbExprs, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i], Commentf("%v\n", i))
	}

	for _, expr := range defCausExprs {
		expr.(*DeferredCauset).ID = 0
		expr.(*DeferredCauset).Index = 0
	}

	pushed, remained = PushDownExprs(sc, defCausExprs, client, ekv.UnSpecified)
	c.Assert(len(pushed), Equals, len(defCausExprs))
	c.Assert(len(remained), Equals, 0)
}

func (s *testEvaluatorSuite) TestCompareFunc2Pb(c *C) {
	var compareExprs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.LT, ast.LE, ast.GT, ast.GE, ast.EQ, ast.NE, ast.NullEQ}
	for _, funcName := range funcNames {
		fc, err := NewFunction(mock.NewContext(), funcName, types.NewFieldType(allegrosql.TypeUnspecified), dg.genDeferredCauset(allegrosql.TypeLonglong, 1), dg.genDeferredCauset(allegrosql.TypeLonglong, 2))
		c.Assert(err, IsNil)
		compareExprs = append(compareExprs, fc)
	}

	pushed, remained := PushDownExprs(sc, compareExprs, client, ekv.UnSpecified)
	c.Assert(len(pushed), Equals, len(compareExprs))
	c.Assert(len(remained), Equals, 0)

	pbExprs, err := ExpressionsToPBList(sc, compareExprs, client)
	c.Assert(err, IsNil)
	c.Assert(len(pbExprs), Equals, len(compareExprs))
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":100,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":110,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":120,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":130,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":140,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":150,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":8,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":160,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
	}
	for i, pbExpr := range pbExprs {
		c.Assert(pbExprs, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSuite) TestLikeFunc2Pb(c *C) {
	var likeFuncs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)

	retTp := types.NewFieldType(allegrosql.TypeString)
	retTp.Charset = charset.CharsetUTF8
	retTp.DefCauslate = charset.DefCauslationUTF8
	args := []Expression{
		&CouplingConstantWithRadix{RetType: retTp, Value: types.NewCauset("string")},
		&CouplingConstantWithRadix{RetType: retTp, Value: types.NewCauset("pattern")},
		&CouplingConstantWithRadix{RetType: retTp, Value: types.NewCauset(`%abc%`)},
		&CouplingConstantWithRadix{RetType: retTp, Value: types.NewCauset("\\")},
	}
	ctx := mock.NewContext()
	retTp = types.NewFieldType(allegrosql.TypeUnspecified)
	fc, err := NewFunction(ctx, ast.Like, retTp, args[0], args[1], args[3])
	c.Assert(err, IsNil)
	likeFuncs = append(likeFuncs, fc)

	fc, err = NewFunction(ctx, ast.Like, retTp, args[0], args[2], args[3])
	c.Assert(err, IsNil)
	likeFuncs = append(likeFuncs, fc)

	pbExprs, err := ExpressionsToPBList(sc, likeFuncs, client)
	c.Assert(err, IsNil)
	results := []string{
		`{"tp":10000,"children":[{"tp":5,"val":"c3RyaW5n","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"defCauslate":83,"charset":"utf8"}},{"tp":5,"val":"cGF0dGVybg==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"defCauslate":83,"charset":"utf8"}},{"tp":10000,"val":"CAA=","children":[{"tp":5,"val":"XA==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"defCauslate":83,"charset":"utf8"}}],"sig":30,"field_type":{"tp":8,"flag":128,"flen":-1,"decimal":0,"defCauslate":63,"charset":"binary"}}],"sig":4310,"field_type":{"tp":8,"flag":128,"flen":1,"decimal":0,"defCauslate":63,"charset":"binary"}}`,
		`{"tp":10000,"children":[{"tp":5,"val":"c3RyaW5n","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"defCauslate":83,"charset":"utf8"}},{"tp":5,"val":"JWFiYyU=","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"defCauslate":83,"charset":"utf8"}},{"tp":10000,"val":"CAA=","children":[{"tp":5,"val":"XA==","sig":0,"field_type":{"tp":254,"flag":1,"flen":-1,"decimal":-1,"defCauslate":83,"charset":"utf8"}}],"sig":30,"field_type":{"tp":8,"flag":128,"flen":-1,"decimal":0,"defCauslate":63,"charset":"binary"}}],"sig":4310,"field_type":{"tp":8,"flag":128,"flen":1,"decimal":0,"defCauslate":63,"charset":"binary"}}`,
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, results[i])
	}
}

func (s *testEvaluatorSuite) TestArithmeticalFunc2Pb(c *C) {
	var arithmeticalFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.Plus, ast.Minus, ast.Mul, ast.Div}
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(allegrosql.TypeUnspecified),
			dg.genDeferredCauset(allegrosql.TypeDouble, 1),
			dg.genDeferredCauset(allegrosql.TypeDouble, 2))
		c.Assert(err, IsNil)
		arithmeticalFuncs = append(arithmeticalFuncs, fc)
	}

	jsons := make(map[string]string)
	jsons[ast.Plus] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":200,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"binary\"}}"
	jsons[ast.Minus] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":204,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"binary\"}}"
	jsons[ast.Mul] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":208,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"binary\"}}"
	jsons[ast.Div] = "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":211,\"field_type\":{\"tp\":5,\"flag\":128,\"flen\":23,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"binary\"}}"

	pbExprs, err := ExpressionsToPBList(sc, arithmeticalFuncs, client)
	c.Assert(err, IsNil)
	for i, pbExpr := range pbExprs {
		c.Assert(pbExpr, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[funcNames[i]], Commentf("%v\n", funcNames[i]))
	}

	funcNames = []string{ast.Mod, ast.IntDiv} // cannot be pushed down
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(allegrosql.TypeUnspecified),
			dg.genDeferredCauset(allegrosql.TypeDouble, 1),
			dg.genDeferredCauset(allegrosql.TypeDouble, 2))
		c.Assert(err, IsNil)
		_, err = ExpressionsToPBList(sc, []Expression{fc}, client)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestDateFunc2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	fc, err := NewFunction(
		mock.NewContext(),
		ast.DateFormat,
		types.NewFieldType(allegrosql.TypeUnspecified),
		dg.genDeferredCauset(allegrosql.TypeDatetime, 1),
		dg.genDeferredCauset(allegrosql.TypeString, 2))
	c.Assert(err, IsNil)
	funcs := []Expression{fc}
	pbExprs, err := ExpressionsToPBList(sc, funcs, client)
	c.Assert(err, IsNil)
	c.Assert(pbExprs[0], NotNil)
	js, err := json.Marshal(pbExprs[0])
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":12,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":46,\"charset\":\"\"}}],\"sig\":6001,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":0,\"decimal\":-1,\"defCauslate\":46,\"charset\":\"utf8mb4\"}}")
}

func (s *testEvaluatorSuite) TestLogicalFunc2Pb(c *C) {
	var logicalFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.LogicAnd, ast.LogicOr, ast.LogicXor, ast.UnaryNot}
	for i, funcName := range funcNames {
		args := []Expression{dg.genDeferredCauset(allegrosql.TypeTiny, 1)}
		if i+1 < len(funcNames) {
			args = append(args, dg.genDeferredCauset(allegrosql.TypeTiny, 2))
		}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(allegrosql.TypeUnspecified),
			args...,
		)
		c.Assert(err, IsNil)
		logicalFuncs = append(logicalFuncs, fc)
	}

	pbExprs, err := ExpressionsToPBList(sc, logicalFuncs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3101,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3102,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3103,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":1,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3104,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSuite) TestBitwiseFunc2Pb(c *C) {
	var bitwiseFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.And, ast.Or, ast.Xor, ast.LeftShift, ast.RightShift, ast.BitNeg}
	for i, funcName := range funcNames {
		args := []Expression{dg.genDeferredCauset(allegrosql.TypeLong, 1)}
		if i+1 < len(funcNames) {
			args = append(args, dg.genDeferredCauset(allegrosql.TypeLong, 2))
		}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(allegrosql.TypeUnspecified),
			args...,
		)
		c.Assert(err, IsNil)
		bitwiseFuncs = append(bitwiseFuncs, fc)
	}

	pbExprs, err := ExpressionsToPBList(sc, bitwiseFuncs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3118,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3119,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3120,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3129,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3130,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3121,\"field_type\":{\"tp\":8,\"flag\":160,\"flen\":20,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSerialSuites) TestPanicIfPbCodeUnspecified(c *C) {
	dg := new(dataGen4Expr2PbTest)
	args := []Expression{dg.genDeferredCauset(allegrosql.TypeLong, 1), dg.genDeferredCauset(allegrosql.TypeLong, 2)}
	fc, err := NewFunction(
		mock.NewContext(),
		ast.And,
		types.NewFieldType(allegrosql.TypeUnspecified),
		args...,
	)
	c.Assert(err, IsNil)
	fn := fc.(*ScalarFunction)
	fn.Function.setPbCode(fidelpb.ScalarFuncSig_Unspecified)
	c.Assert(fn.Function.PbCode(), Equals, fidelpb.ScalarFuncSig_Unspecified)

	pc := PbConverter{client: new(mock.Client), sc: new(stmtctx.StatementContext)}
	c.Assert(func() { pc.ExprToPB(fn) }, PanicMatches, "unspecified PbCode: .*")
}

func (s *testEvaluatorSerialSuites) TestPushDownSwitcher(c *C) {
	var funcs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	cases := []struct {
		name   string
		sig    fidelpb.ScalarFuncSig
		enable bool
	}{
		// Note that so far ScalarFuncSigs here are not be pushed down when the failpoint PushDownTestSwitcher
		// is disable, which is the prerequisite to pass this test.
		// Need to be replaced with other non pushed down ScalarFuncSigs if they are pushed down one day.
		{ast.Sin, fidelpb.ScalarFuncSig_Sin, true},
		{ast.Cos, fidelpb.ScalarFuncSig_Cos, false},
		{ast.Tan, fidelpb.ScalarFuncSig_Tan, true},
	}
	var enabled []string
	for _, funcName := range cases {
		args := []Expression{dg.genDeferredCauset(allegrosql.TypeLong, 1)}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName.name,
			types.NewFieldType(allegrosql.TypeUnspecified),
			args...,
		)
		c.Assert(err, IsNil)
		funcs = append(funcs, fc)
		if funcName.enable {
			enabled = append(enabled, funcName.name)
		}
	}

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/expression/PushDownTestSwitcher", `return("all")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/expression/PushDownTestSwitcher"), IsNil)
	}()

	pbExprs, err := ExpressionsToPBList(sc, funcs, client)
	c.Assert(err, IsNil)
	c.Assert(len(pbExprs), Equals, len(cases))
	for i, pbExpr := range pbExprs {
		c.Assert(pbExpr.Sig, Equals, cases[i].sig, Commentf("function: %s, sig: %v", cases[i].name, cases[i].sig))
	}

	// All disabled
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/expression/PushDownTestSwitcher", `return("")`), IsNil)
	pc := PbConverter{client: client, sc: sc}
	for i := range funcs {
		pbExpr := pc.ExprToPB(funcs[i])
		c.Assert(pbExpr, IsNil, Commentf("function: %s, sig: %v", cases[i].name, cases[i].sig))
	}

	// Partial enabled
	fpexpr := fmt.Sprintf(`return("%s")`, strings.Join(enabled, ","))
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/expression/PushDownTestSwitcher", fpexpr), IsNil)
	for i := range funcs {
		pbExpr := pc.ExprToPB(funcs[i])
		if !cases[i].enable {
			c.Assert(pbExpr, IsNil, Commentf("function: %s, sig: %v", cases[i].name, cases[i].sig))
			continue
		}
		c.Assert(pbExpr.Sig, Equals, cases[i].sig, Commentf("function: %s, sig: %v", cases[i].name, cases[i].sig))
	}
}

func (s *testEvaluatorSuite) TestControlFunc2Pb(c *C) {
	var controlFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{
		ast.Case,
		ast.If,
		ast.Ifnull,
	}
	for i, funcName := range funcNames {
		args := []Expression{dg.genDeferredCauset(allegrosql.TypeLong, 1)}
		args = append(args, dg.genDeferredCauset(allegrosql.TypeLong, 2))
		if i < 2 {
			args = append(args, dg.genDeferredCauset(allegrosql.TypeLong, 3))
		}
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(allegrosql.TypeUnspecified),
			args...,
		)
		c.Assert(err, IsNil)
		controlFuncs = append(controlFuncs, fc)
	}

	pbExprs, err := ExpressionsToPBList(sc, controlFuncs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":4208,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":4107,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":4101,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":-1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
		"null",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

func (s *testEvaluatorSuite) TestOtherFunc2Pb(c *C) {
	var otherFuncs = make([]Expression, 0)
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.Coalesce, ast.IsNull}
	for _, funcName := range funcNames {
		fc, err := NewFunction(
			mock.NewContext(),
			funcName,
			types.NewFieldType(allegrosql.TypeUnspecified),
			dg.genDeferredCauset(allegrosql.TypeLong, 1),
		)
		c.Assert(err, IsNil)
		otherFuncs = append(otherFuncs, fc)
	}

	pbExprs, err := ExpressionsToPBList(sc, otherFuncs, client)
	c.Assert(err, IsNil)
	jsons := map[string]string{
		ast.Coalesce: "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":4201,\"field_type\":{\"tp\":3,\"flag\":128,\"flen\":0,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}",
		ast.IsNull:   "{\"tp\":10000,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":3,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}}],\"sig\":3116,\"field_type\":{\"tp\":8,\"flag\":128,\"flen\":1,\"decimal\":0,\"defCauslate\":63,\"charset\":\"binary\"}}",
	}
	for i, pbExpr := range pbExprs {
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[funcNames[i]])
	}
}

func (s *testEvaluatorSuite) TestExprPushDownToFlash(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	exprs := make([]Expression, 0)

	jsonDeferredCauset := dg.genDeferredCauset(allegrosql.TypeJSON, 1)
	intDeferredCauset := dg.genDeferredCauset(allegrosql.TypeLonglong, 2)
	function, err := NewFunction(mock.NewContext(), ast.JSONLength, types.NewFieldType(allegrosql.TypeLonglong), jsonDeferredCauset)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.If, types.NewFieldType(allegrosql.TypeLonglong), intDeferredCauset, intDeferredCauset, intDeferredCauset)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.BitNeg, types.NewFieldType(allegrosql.TypeLonglong), intDeferredCauset)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = NewFunction(mock.NewContext(), ast.Xor, types.NewFieldType(allegrosql.TypeLonglong), intDeferredCauset, intDeferredCauset)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	canPush := CanExprsPushDown(sc, exprs, client, ekv.TiFlash)
	c.Assert(canPush, Equals, true)

	function, err = NewFunction(mock.NewContext(), ast.JSONDepth, types.NewFieldType(allegrosql.TypeLonglong), jsonDeferredCauset)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)
	pushed, remained := PushDownExprs(sc, exprs, client, ekv.TiFlash)
	c.Assert(len(pushed), Equals, len(exprs)-1)
	c.Assert(len(remained), Equals, 1)
}

func (s *testEvaluatorSuite) TestExprOnlyPushDownToFlash(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	function, err := NewFunction(mock.NewContext(), ast.TimestamFIDeliff, types.NewFieldType(allegrosql.TypeLonglong),
		dg.genDeferredCauset(allegrosql.TypeString, 1), dg.genDeferredCauset(allegrosql.TypeDatetime, 2), dg.genDeferredCauset(allegrosql.TypeDatetime, 3))
	c.Assert(err, IsNil)
	var exprs = make([]Expression, 0)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, ekv.UnSpecified)
	c.Assert(len(pushed), Equals, 1)
	c.Assert(len(remained), Equals, 0)

	canPush := CanExprsPushDown(sc, exprs, client, ekv.TiFlash)
	c.Assert(canPush, Equals, true)
	canPush = CanExprsPushDown(sc, exprs, client, ekv.EinsteinDB)
	c.Assert(canPush, Equals, false)

	pushed, remained = PushDownExprs(sc, exprs, client, ekv.TiFlash)
	c.Assert(len(pushed), Equals, 1)
	c.Assert(len(remained), Equals, 0)

	pushed, remained = PushDownExprs(sc, exprs, client, ekv.EinsteinDB)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, 1)
}

func (s *testEvaluatorSuite) TestExprOnlyPushDownToEinsteinDB(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	function, err := NewFunction(mock.NewContext(), "dayofyear", types.NewFieldType(allegrosql.TypeLonglong), dg.genDeferredCauset(allegrosql.TypeDatetime, 1))
	c.Assert(err, IsNil)
	var exprs = make([]Expression, 0)
	exprs = append(exprs, function)

	pushed, remained := PushDownExprs(sc, exprs, client, ekv.UnSpecified)
	c.Assert(len(pushed), Equals, 1)
	c.Assert(len(remained), Equals, 0)

	canPush := CanExprsPushDown(sc, exprs, client, ekv.TiFlash)
	c.Assert(canPush, Equals, false)
	canPush = CanExprsPushDown(sc, exprs, client, ekv.EinsteinDB)
	c.Assert(canPush, Equals, true)

	pushed, remained = PushDownExprs(sc, exprs, client, ekv.TiFlash)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, 1)
	pushed, remained = PushDownExprs(sc, exprs, client, ekv.EinsteinDB)
	c.Assert(len(pushed), Equals, 1)
	c.Assert(len(remained), Equals, 0)
}

func (s *testEvaluatorSuite) TestGroupByItem2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	item := dg.genDeferredCauset(allegrosql.TypeDouble, 0)
	pbByItem := GroupByItemToPB(sc, client, item)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},\"desc\":false}")

	item = dg.genDeferredCauset(allegrosql.TypeDouble, 1)
	pbByItem = GroupByItemToPB(sc, client, item)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},\"desc\":false}")
}

func (s *testEvaluatorSuite) TestSortByItem2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	item := dg.genDeferredCauset(allegrosql.TypeDouble, 0)
	pbByItem := SortByItemToPB(sc, client, item, false)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},\"desc\":false}")

	item = dg.genDeferredCauset(allegrosql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, false)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},\"desc\":false}")

	item = dg.genDeferredCauset(allegrosql.TypeDouble, 1)
	pbByItem = SortByItemToPB(sc, client, item, true)
	js, err = json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":63,\"charset\":\"\"}},\"desc\":true}")
}

func (s *testEvaluatorSerialSuites) TestMetadata(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/expression/PushDownTestSwitcher", `return("all")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/expression/PushDownTestSwitcher"), IsNil)
	}()

	pc := PbConverter{client: client, sc: sc}

	metadata := new(fidelpb.InUnionMetadata)
	var err error
	// InUnion flag is false in `BuildCastFunction` when `ScalarFuncSig_CastStringAsInt`
	cast := BuildCastFunction(mock.NewContext(), dg.genDeferredCauset(allegrosql.TypeString, 1), types.NewFieldType(allegrosql.TypeLonglong))
	c.Assert(cast.(*ScalarFunction).Function.metadata(), DeepEquals, &fidelpb.InUnionMetadata{InUnion: false})
	expr := pc.ExprToPB(cast)
	c.Assert(expr.Sig, Equals, fidelpb.ScalarFuncSig_CastStringAsInt)
	c.Assert(len(expr.Val), Greater, 0)
	err = proto.Unmarshal(expr.Val, metadata)
	c.Assert(err, IsNil)
	c.Assert(metadata.InUnion, Equals, false)

	// InUnion flag is nil in `BuildCastFunction4Union` when `ScalarFuncSig_CastIntAsString`
	castInUnion := BuildCastFunction4Union(mock.NewContext(), dg.genDeferredCauset(allegrosql.TypeLonglong, 1), types.NewFieldType(allegrosql.TypeString))
	c.Assert(castInUnion.(*ScalarFunction).Function.metadata(), IsNil)
	expr = pc.ExprToPB(castInUnion)
	c.Assert(expr.Sig, Equals, fidelpb.ScalarFuncSig_CastIntAsString)
	c.Assert(len(expr.Val), Equals, 0)

	// InUnion flag is true in `BuildCastFunction4Union` when `ScalarFuncSig_CastStringAsInt`
	castInUnion = BuildCastFunction4Union(mock.NewContext(), dg.genDeferredCauset(allegrosql.TypeString, 1), types.NewFieldType(allegrosql.TypeLonglong))
	c.Assert(castInUnion.(*ScalarFunction).Function.metadata(), DeepEquals, &fidelpb.InUnionMetadata{InUnion: true})
	expr = pc.ExprToPB(castInUnion)
	c.Assert(expr.Sig, Equals, fidelpb.ScalarFuncSig_CastStringAsInt)
	c.Assert(len(expr.Val), Greater, 0)
	err = proto.Unmarshal(expr.Val, metadata)
	c.Assert(err, IsNil)
	c.Assert(metadata.InUnion, Equals, true)
}

func defCausumnDefCauslation(c *DeferredCauset, defCausl string) *DeferredCauset {
	c.RetType.DefCauslate = defCausl
	return c
}

func (s *testEvaluatorSerialSuites) TestNewDefCauslationsEnabled(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	var defCausExprs []Expression
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	defCausExprs = defCausExprs[:0]
	defCausExprs = append(defCausExprs, dg.genDeferredCauset(allegrosql.TypeVarchar, 1))
	defCausExprs = append(defCausExprs, defCausumnDefCauslation(dg.genDeferredCauset(allegrosql.TypeVarchar, 2), "some_invalid_defCauslation"))
	defCausExprs = append(defCausExprs, defCausumnDefCauslation(dg.genDeferredCauset(allegrosql.TypeVarString, 3), "utf8mb4_general_ci"))
	defCausExprs = append(defCausExprs, defCausumnDefCauslation(dg.genDeferredCauset(allegrosql.TypeString, 4), "utf8mb4_0900_ai_ci"))
	defCausExprs = append(defCausExprs, defCausumnDefCauslation(dg.genDeferredCauset(allegrosql.TypeVarchar, 5), "utf8_bin"))
	defCausExprs = append(defCausExprs, defCausumnDefCauslation(dg.genDeferredCauset(allegrosql.TypeVarchar, 6), "utf8_unicode_ci"))
	pushed, _ := PushDownExprs(sc, defCausExprs, client, ekv.UnSpecified)
	c.Assert(len(pushed), Equals, len(defCausExprs))
	pbExprs, err := ExpressionsToPBList(sc, defCausExprs, client)
	c.Assert(err, IsNil)
	jsons := []string{
		"{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":-46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAI=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":-46,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAM=\",\"sig\":0,\"field_type\":{\"tp\":253,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":-45,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAQ=\",\"sig\":0,\"field_type\":{\"tp\":254,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":-255,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAU=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":-83,\"charset\":\"\"}}",
		"{\"tp\":201,\"val\":\"gAAAAAAAAAY=\",\"sig\":0,\"field_type\":{\"tp\":15,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":-192,\"charset\":\"\"}}",
	}
	for i, pbExpr := range pbExprs {
		c.Assert(pbExprs, NotNil)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i], Commentf("%v\n", i))
	}

	item := defCausumnDefCauslation(dg.genDeferredCauset(allegrosql.TypeDouble, 0), "utf8mb4_0900_ai_ci")
	pbByItem := GroupByItemToPB(sc, client, item)
	js, err := json.Marshal(pbByItem)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "{\"expr\":{\"tp\":201,\"val\":\"gAAAAAAAAAA=\",\"sig\":0,\"field_type\":{\"tp\":5,\"flag\":0,\"flen\":-1,\"decimal\":-1,\"defCauslate\":-255,\"charset\":\"\"}},\"desc\":false}")
}

func (s *testEvalSerialSuite) TestPushDefCauslationDown(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	dg := new(dataGen4Expr2PbTest)
	fc, err := NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(allegrosql.TypeUnspecified), dg.genDeferredCauset(allegrosql.TypeVarchar, 0), dg.genDeferredCauset(allegrosql.TypeVarchar, 1))
	c.Assert(err, IsNil)
	client := new(mock.Client)
	sc := new(stmtctx.StatementContext)

	tps := []*types.FieldType{types.NewFieldType(allegrosql.TypeVarchar), types.NewFieldType(allegrosql.TypeVarchar)}
	for _, defCausl := range []string{charset.DefCauslationBin, charset.DefCauslationLatin1, charset.DefCauslationUTF8, charset.DefCauslationUTF8MB4} {
		fc.SetCharsetAndDefCauslation("binary", defCausl) // only defCauslation matters
		pbExpr, err := ExpressionsToPBList(sc, []Expression{fc}, client)
		c.Assert(err, IsNil)
		expr, err := PBToExpr(pbExpr[0], tps, sc)
		c.Assert(err, IsNil)
		_, eDefCausl := expr.CharsetAndDefCauslation(nil)
		c.Assert(eDefCausl, Equals, defCausl)
	}
}
