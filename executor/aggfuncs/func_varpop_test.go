package aggfuncs_test

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/executor/aggfuncs"
	"github.com/whtcorpsinc/milevadb/types"
)

func (s *testSuite) TestMergePartialResult4Varpop(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncVarPop, allegrosql.TypeDouble, 5, types.NewFloat64Causet(float64(2)), types.NewFloat64Causet(float64(2)/float64(3)), types.NewFloat64Causet(float64(59)/float64(8)-float64(19*19)/float64(8*8))),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestVarpop(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncVarPop, allegrosql.TypeDouble, 5, nil, types.NewFloat64Causet(float64(2))),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}

func (s *testSuite) TestMemVarpop(c *C) {
	tests := []aggMemTest{
		builPosetDaggMemTester(ast.AggFuncVarPop, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4VarPopFloat64Size, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncVarPop, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4VarPoFIDelistinctFloat64Size, distinctUFIDelateMemDeltaGens, true),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}
