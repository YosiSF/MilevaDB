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
	"strings"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	driver "github.com/whtcorpsinc/milevadb/types/berolinaAllegroSQL_driver"
)

func (s *testExpressionSuite) TestGetTimeValue(c *C) {
	ctx := mock.NewContext()
	v, err := GetTimeValue(ctx, "2012-12-12 00:00:00", allegrosql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.HoTT(), Equals, types.HoTTMysqlTime)
	timeValue := v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")
	stochastikVars := ctx.GetStochastikVars()
	variable.SetStochastikSystemVar(stochastikVars, "timestamp", types.NewStringCauset(""))
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", allegrosql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.HoTT(), Equals, types.HoTTMysqlTime)
	timeValue = v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	variable.SetStochastikSystemVar(stochastikVars, "timestamp", types.NewStringCauset("0"))
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", allegrosql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.HoTT(), Equals, types.HoTTMysqlTime)
	timeValue = v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	variable.SetStochastikSystemVar(stochastikVars, "timestamp", types.Causet{})
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", allegrosql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)

	c.Assert(v.HoTT(), Equals, types.HoTTMysqlTime)
	timeValue = v.GetMysqlTime()
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	variable.SetStochastikSystemVar(stochastikVars, "timestamp", types.NewStringCauset("1234"))

	tbl := []struct {
		Expr interface{}
		Ret  interface{}
	}{
		{"2012-12-12 00:00:00", "2012-12-12 00:00:00"},
		{ast.CurrentTimestamp, time.Unix(1234, 0).Format(types.TimeFormat)},
		{types.ZeroDatetimeStr, "0000-00-00 00:00:00"},
		{ast.NewValueExpr("2012-12-12 00:00:00", charset.CharsetUTF8MB4, charset.DefCauslationUTF8MB4), "2012-12-12 00:00:00"},
		{ast.NewValueExpr(int64(0), "", ""), "0000-00-00 00:00:00"},
		{ast.NewValueExpr(nil, "", ""), nil},
		{&ast.FuncCallExpr{FnName: perceptron.NewCIStr(ast.CurrentTimestamp)}, strings.ToUpper(ast.CurrentTimestamp)},
		//{&ast.UnaryOperationExpr{Op: opcode.Minus, V: ast.NewValueExpr(int64(0))}, "0000-00-00 00:00:00"},
	}

	for i, t := range tbl {
		comment := Commentf("expr: %d", i)
		v, err := GetTimeValue(ctx, t.Expr, allegrosql.TypeTimestamp, types.MinFsp)
		c.Assert(err, IsNil)

		switch v.HoTT() {
		case types.HoTTMysqlTime:
			c.Assert(v.GetMysqlTime().String(), DeepEquals, t.Ret, comment)
		default:
			c.Assert(v.GetValue(), DeepEquals, t.Ret, comment)
		}
	}

	errTbl := []struct {
		Expr interface{}
	}{
		{"2012-13-12 00:00:00"},
		{ast.NewValueExpr("2012-13-12 00:00:00", charset.CharsetUTF8MB4, charset.DefCauslationUTF8MB4)},
		{ast.NewValueExpr(int64(1), "", "")},
		{&ast.FuncCallExpr{FnName: perceptron.NewCIStr("xxx")}},
		//{&ast.UnaryOperationExpr{Op: opcode.Minus, V: ast.NewValueExpr(int64(1))}},
	}

	for _, t := range errTbl {
		_, err := GetTimeValue(ctx, t.Expr, allegrosql.TypeTimestamp, types.MinFsp)
		c.Assert(err, NotNil)
	}
}

func (s *testExpressionSuite) TestIsCurrentTimestampExpr(c *C) {
	buildTimestampFuncCallExpr := func(i int64) *ast.FuncCallExpr {
		var args []ast.ExprNode
		if i != 0 {
			args = []ast.ExprNode{&driver.ValueExpr{Causet: types.NewIntCauset(i)}}
		}
		return &ast.FuncCallExpr{FnName: perceptron.NewCIStr("CURRENT_TIMESTAMP"), Args: args}
	}

	v := IsValidCurrentTimestampExpr(ast.NewValueExpr("abc", charset.CharsetUTF8MB4, charset.DefCauslationUTF8MB4), nil)
	c.Assert(v, IsFalse)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(0), nil)
	c.Assert(v, IsTrue)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(3), &types.FieldType{Decimal: 3})
	c.Assert(v, IsTrue)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(1), &types.FieldType{Decimal: 3})
	c.Assert(v, IsFalse)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(0), &types.FieldType{Decimal: 3})
	c.Assert(v, IsFalse)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(2), &types.FieldType{Decimal: 0})
	c.Assert(v, IsFalse)
	v = IsValidCurrentTimestampExpr(buildTimestampFuncCallExpr(2), nil)
	c.Assert(v, IsFalse)
}

func (s *testExpressionSuite) TestCurrentTimestampTimeZone(c *C) {
	ctx := mock.NewContext()
	stochastikVars := ctx.GetStochastikVars()

	variable.SetStochastikSystemVar(stochastikVars, "timestamp", types.NewStringCauset("1234"))
	variable.SetStochastikSystemVar(stochastikVars, "time_zone", types.NewStringCauset("+00:00"))
	v, err := GetTimeValue(ctx, ast.CurrentTimestamp, allegrosql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)
	c.Assert(v.GetMysqlTime(), DeepEquals, types.NewTime(
		types.FromDate(1970, 1, 1, 0, 20, 34, 0),
		allegrosql.TypeTimestamp, types.DefaultFsp))

	// CurrentTimestamp from "timestamp" stochastik variable is based on UTC, so change timezone
	// would get different value.
	variable.SetStochastikSystemVar(stochastikVars, "time_zone", types.NewStringCauset("+08:00"))
	v, err = GetTimeValue(ctx, ast.CurrentTimestamp, allegrosql.TypeTimestamp, types.MinFsp)
	c.Assert(err, IsNil)
	c.Assert(v.GetMysqlTime(), DeepEquals, types.NewTime(
		types.FromDate(1970, 1, 1, 8, 20, 34, 0),
		allegrosql.TypeTimestamp, types.DefaultFsp))
}
