package aggregation

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/check"
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
