package aggfuncs_test

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
)

func (s *testSuite) TestMergePartialResult4Stddevpop(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncStddevPop, allegrosql.TypeDouble, 5, 1.4142135623730951, 0.816496580927726, 1.3169567191065923),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestStddevpop(c *C) {
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncStddevPop, allegrosql.TypeDouble, 5, nil, 1.4142135623730951),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}
