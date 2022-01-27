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

package aggregation

import (
	"encoding/json"
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testEvaluatorSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type dataGen4Expr2PbTest struct {
}

func (dg *dataGen4Expr2PbTest) genDeferredCauset(tp byte, id int64) *expression.DeferredCauset {
	return &expression.DeferredCauset{
		RetType: types.NewFieldType(tp),
		ID:      id,
		Index:   int(id),
	}
}

type testEvaluatorSuite struct {
	*berolinaAllegroSQL.berolinaAllegroSQL
	ctx stochastikctx.Context
}

func (s *testEvaluatorSuite) SetUpSuite(c *C) {
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	s.ctx = mock.NewContext()
}

func (s *testEvaluatorSuite) TearDownSuite(c *C) {
}

func (s *testEvaluatorSuite) TestAggFunc2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncAvg, ast.AggFuncGroupConcat, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstEvent}
	funcTypes := []*types.FieldType{
		types.NewFieldType(allegrosql.TypeDouble),
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeDouble),
		types.NewFieldType(allegrosql.TypeVarchar),
		types.NewFieldType(allegrosql.TypeDouble),
		types.NewFieldType(allegrosql.TypeDouble),
		types.NewFieldType(allegrosql.TypeDouble),
	}

	jsons := []string{
		`{"tp":3002,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}`,
		`{"tp":3001,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}],"sig":0,"field_type":{"tp":8,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}`,
		`{"tp":3003,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}`,
		"null",
		`{"tp":3005,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}`,
		`{"tp":3004,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}`,
		`{"tp":3006,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"defCauslate":63,"charset":""}}`,
	}
	for i, funcName := range funcNames {
		for _, hasDistinct := range []bool{true, false} {
			args := []expression.Expression{dg.genDeferredCauset(allegrosql.TypeDouble, 1)}
			aggFunc, err := NewAggFuncDesc(s.ctx, funcName, args, hasDistinct)
			c.Assert(err, IsNil)
			aggFunc.RetTp = funcTypes[i]
			pbExpr := AggFuncToPBExpr(sc, client, aggFunc)
			js, err := json.Marshal(pbExpr)
			c.Assert(err, IsNil)
			c.Assert(string(js), Equals, jsons[i])
		}
	}
}
