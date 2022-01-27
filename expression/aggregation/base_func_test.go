package aggregation

import (
	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
)

var _ = check.Suite(&testBaseFuncSuite{})

type testBaseFuncSuite struct {
	ctx stochastikctx.Context
}

func (s *testBaseFuncSuite) SetUpSuite(c *check.C) {
	s.ctx = mock.NewContext()
}

func (s *testBaseFuncSuite) TestClone(c *check.C) {
	defCaus := &expression.DeferredCauset{
		UniqueID: 0,
		RetType:  types.NewFieldType(allegrosql.TypeLonglong),
	}
	desc, err := newBaseFuncDesc(s.ctx, ast.AggFuncFirstEvent, []expression.Expression{defCaus})
	c.Assert(err, check.IsNil)
	cloned := desc.clone()
	c.Assert(desc.equal(s.ctx, cloned), check.IsTrue)

	defCaus1 := &expression.DeferredCauset{
		UniqueID: 1,
		RetType:  types.NewFieldType(allegrosql.TypeVarchar),
	}
	cloned.Args[0] = defCaus1

	c.Assert(desc.Args[0], check.Equals, defCaus)
	c.Assert(desc.equal(s.ctx, cloned), check.IsFalse)
}

func (s *testBaseFuncSuite) TestMaxMin(c *check.C) {
	defCaus := &expression.DeferredCauset{
		UniqueID: 0,
		RetType:  types.NewFieldType(allegrosql.TypeLonglong),
	}
	defCaus.RetType.Flag |= allegrosql.NotNullFlag
	desc, err := newBaseFuncDesc(s.ctx, ast.AggFuncMax, []expression.Expression{defCaus})
	c.Assert(err, check.IsNil)
	c.Assert(allegrosql.HasNotNullFlag(desc.RetTp.Flag), check.IsFalse)
}
