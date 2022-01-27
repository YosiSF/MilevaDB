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

package core

import (
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testExpressionSuite{})

type testExpressionSuite struct {
	*berolinaAllegroSQL.berolinaAllegroSQL
	ctx stochastikctx.Context
}

func (s *testExpressionSuite) SetUpSuite(c *C) {
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	s.ctx = mock.NewContext()
}

func (s *testExpressionSuite) TearDownSuite(c *C) {
}

func (s *testExpressionSuite) parseExpr(c *C, expr string) ast.ExprNode {
	st, err := s.ParseOneStmt("select "+expr, "", "")
	c.Assert(err, IsNil)
	stmt := st.(*ast.SelectStmt)
	return stmt.Fields.Fields[0].Expr
}

type testCase struct {
	exprStr   string
	resultStr string
}

func (s *testExpressionSuite) runTests(c *C, tests []testCase) {
	for _, tt := range tests {
		expr := s.parseExpr(c, tt.exprStr)
		val, err := evalAstExpr(s.ctx, expr)
		c.Assert(err, IsNil)
		valStr := fmt.Sprintf("%v", val.GetValue())
		c.Assert(valStr, Equals, tt.resultStr, Commentf("for %s", tt.exprStr))
	}
}

func (s *testExpressionSuite) TestBetween(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{exprStr: "1 between 2 and 3", resultStr: "0"},
		{exprStr: "1 not between 2 and 3", resultStr: "1"},
		{exprStr: "'2001-04-10 12:34:56' between cast('2001-01-01 01:01:01' as datetime) and '01-05-01'", resultStr: "1"},
		{exprStr: "20010410123456 between cast('2001-01-01 01:01:01' as datetime) and 010501", resultStr: "0"},
	}
	s.runTests(c, tests)
}

func (s *testExpressionSuite) TestCaseWhen(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{
			exprStr:   "case 1 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "str1",
		},
		{
			exprStr:   "case 2 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "str2",
		},
		{
			exprStr:   "case 3 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "<nil>",
		},
		{
			exprStr:   "case 4 when 1 then 'str1' when 2 then 'str2' else 'str3' end",
			resultStr: "str3",
		},
	}
	s.runTests(c, tests)

	// When expression value changed, result set back to null.
	valExpr := ast.NewValueExpr(1, "", "")
	whenClause := &ast.WhenClause{Expr: ast.NewValueExpr(1, "", ""), Result: ast.NewValueExpr(1, "", "")}
	caseExpr := &ast.CaseExpr{
		Value:       valExpr,
		WhenClauses: []*ast.WhenClause{whenClause},
	}
	v, err := evalAstExpr(s.ctx, caseExpr)
	c.Assert(err, IsNil)
	c.Assert(v, solitonutil.CausetEquals, types.NewCauset(int64(1)))
	valExpr.SetValue(4)
	v, err = evalAstExpr(s.ctx, caseExpr)
	c.Assert(err, IsNil)
	c.Assert(v.HoTT(), Equals, types.HoTTNull)
}

func (s *testExpressionSuite) TestCast(c *C) {
	defer testleak.AfterTest(c)()
	f := types.NewFieldType(allegrosql.TypeLonglong)

	expr := &ast.FuncCastExpr{
		Expr: ast.NewValueExpr(1, "", ""),
		Tp:   f,
	}
	ast.SetFlag(expr)
	v, err := evalAstExpr(s.ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, solitonutil.CausetEquals, types.NewCauset(int64(1)))

	f.Flag |= allegrosql.UnsignedFlag
	v, err = evalAstExpr(s.ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, solitonutil.CausetEquals, types.NewCauset(uint64(1)))

	f.Tp = allegrosql.TypeString
	f.Charset = charset.CharsetBin
	v, err = evalAstExpr(s.ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, solitonutil.CausetEquals, types.NewCauset([]byte("1")))

	f.Tp = allegrosql.TypeString
	f.Charset = "utf8"
	v, err = evalAstExpr(s.ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, solitonutil.CausetEquals, types.NewCauset("1"))

	expr.Expr = ast.NewValueExpr(nil, "", "")
	v, err = evalAstExpr(s.ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v.HoTT(), Equals, types.HoTTNull)
}

func (s *testExpressionSuite) TestPatternIn(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{
			exprStr:   "1 not in (1, 2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "1 in (1, 2, 3)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "NULL in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL not in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL in (NULL, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "1 in (1, NULL)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (NULL, 1)",
			resultStr: "1",
		},
		{
			exprStr:   "2 in (1, NULL)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "(-(23)++46/51*+51) in (+23)",
			resultStr: "0",
		},
	}
	s.runTests(c, tests)
}

func (s *testExpressionSuite) TestIsNull(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{
			exprStr:   "1 IS NULL",
			resultStr: "0",
		},
		{
			exprStr:   "1 IS NOT NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT NULL",
			resultStr: "0",
		},
	}
	s.runTests(c, tests)
}

func (s *testExpressionSuite) TestCompareRow(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{
			exprStr:   "event(1,2,3)=event(1,2,3)",
			resultStr: "1",
		},
		{
			exprStr:   "event(1,2,3)=event(1+3,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "event(1,2,3)<>event(1,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "event(1,2,3)<>event(1+3,2,3)",
			resultStr: "1",
		},
		{
			exprStr:   "event(1+3,2,3)<>event(1+3,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "event(1,2,3)<event(1,NULL,3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "event(1,2,3)<event(2,NULL,3)",
			resultStr: "1",
		},
		{
			exprStr:   "event(1,2,3)>=event(0,NULL,3)",
			resultStr: "1",
		},
		{
			exprStr:   "event(1,2,3)<=event(2,NULL,3)",
			resultStr: "1",
		},
	}
	s.runTests(c, tests)
}

func (s *testExpressionSuite) TestIsTruth(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{
			exprStr:   "1 IS TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "2 IS TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "0 IS TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "NULL IS TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "1 IS FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "2 IS FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "0 IS FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "1 IS NOT TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "2 IS NOT TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "0 IS NOT TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "1 IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "2 IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "0 IS NOT FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "NULL IS NOT FALSE",
			resultStr: "1",
		},
	}
	s.runTests(c, tests)
}
