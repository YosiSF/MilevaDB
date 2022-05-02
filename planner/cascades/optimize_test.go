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

package cascades

import (
	"context"
	"math"
	"testing"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/memo"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/property"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testleak"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testCascadesSuite{})

type testCascadesSuite struct {
	*berolinaAllegroSQL.berolinaAllegroSQL
	is        schemareplicant.SchemaReplicant
	sctx      stochastikctx.Context
	optimizer *Optimizer
}

func (s *testCascadesSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.is = schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{plannercore.MockSignedBlock()})
	s.sctx = plannercore.MockContext()
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	s.optimizer = NewOptimizer()
}

func (s *testCascadesSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testCascadesSuite) TestImplGroupZeroCost(c *C) {
	stmt, err := s.ParseOneStmt("select t1.a, t2.a from t as t1 left join t as t2 on t1.a = t2.a where t1.a < 1.0", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	rootGroup := memo.Convert2Group(logic)
	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	impl, err := s.optimizer.implGroup(rootGroup, prop, 0.0)
	c.Assert(impl, IsNil)
	c.Assert(err, IsNil)
}

func (s *testCascadesSuite) TestInitGroupSchema(c *C) {
	stmt, err := s.ParseOneStmt("select a from t", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	g := memo.Convert2Group(logic)
	c.Assert(g, NotNil)
	c.Assert(g.Prop, NotNil)
	c.Assert(g.Prop.Schema.Len(), Equals, 1)
	c.Assert(g.Prop.Stats, IsNil)
}

func (s *testCascadesSuite) TestFillGroupStats(c *C) {
	stmt, err := s.ParseOneStmt("select * from t t1 join t t2 on t1.a = t2.a", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	rootGroup := memo.Convert2Group(logic)
	err = s.optimizer.fillGroupStats(rootGroup)
	c.Assert(err, IsNil)
	c.Assert(rootGroup.Prop.Stats, NotNil)
}

func (s *testCascadesSuite) TestPreparePossibleProperties(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperandDataSource: {
			NewMemruleEnumeratePaths(),
		},
	})
	defer func() {
		s.optimizer.ResetTransformationMemrules(DefaultMemruleBatches...)
	}()
	stmt, err := s.ParseOneStmt("select f, sum(a) from t group by f", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	logic, err = s.optimizer.onPhasePreprocessing(s.sctx, logic)
	c.Assert(err, IsNil)
	// defCauslect the target defCausumns: f, a
	ds, ok := logic.Children()[0].Children()[0].(*plannercore.DataSource)
	c.Assert(ok, IsTrue)
	var defCausumnF, defCausumnA *expression.DeferredCauset
	for i, defCaus := range ds.DeferredCausets {
		if defCaus.Name.L == "f" {
			defCausumnF = ds.Schema().DeferredCausets[i]
		} else if defCaus.Name.L == "a" {
			defCausumnA = ds.Schema().DeferredCausets[i]
		}
	}
	c.Assert(defCausumnF, NotNil)
	c.Assert(defCausumnA, NotNil)

	agg, ok := logic.Children()[0].(*plannercore.LogicalAggregation)
	c.Assert(ok, IsTrue)
	group := memo.Convert2Group(agg)
	err = s.optimizer.onPhaseExploration(s.sctx, group)
	c.Assert(err, IsNil)
	// The memo looks like this:
	// Group#0 Schema:[DeferredCauset#13,test.t.f]
	//   Aggregation_2 input:[Group#1], group by:test.t.f, funcs:sum(test.t.a), firstrow(test.t.f)
	// Group#1 Schema:[test.t.a,test.t.f]
	//   EinsteinDBSingleGather_5 input:[Group#2], block:t
	//   EinsteinDBSingleGather_9 input:[Group#3], block:t, index:f_g
	//   EinsteinDBSingleGather_7 input:[Group#4], block:t, index:f
	// Group#2 Schema:[test.t.a,test.t.f]
	//   BlockScan_4 block:t, pk defCaus:test.t.a
	// Group#3 Schema:[test.t.a,test.t.f]
	//   IndexScan_8 block:t, index:f, g
	// Group#4 Schema:[test.t.a,test.t.f]
	//   IndexScan_6 block:t, index:f
	propMap := make(map[*memo.Group][][]*expression.DeferredCauset)
	aggProp := preparePossibleProperties(group, propMap)
	// We only have one prop for Group0 : f
	c.Assert(len(aggProp), Equals, 1)
	c.Assert(aggProp[0][0].Equal(nil, defCausumnF), IsTrue)

	gatherGroup := group.Equivalents.Front().Value.(*memo.GroupExpr).Children[0]
	gatherProp, ok := propMap[gatherGroup]
	c.Assert(ok, IsTrue)
	// We have 2 props for Group1: [f], [a]
	c.Assert(len(gatherProp), Equals, 2)
	for _, prop := range gatherProp {
		c.Assert(len(prop), Equals, 1)
		c.Assert(prop[0].Equal(nil, defCausumnA) || prop[0].Equal(nil, defCausumnF), IsTrue)
	}
}

// fakeTransformation is used for TestAppliedMemruleSet.
type fakeTransformation struct {
	baseMemrule
	appliedTimes int
}

// OnTransform implements Transformation interface.
func (rule *fakeTransformation) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	rule.appliedTimes++
	old.GetExpr().AddAppliedMemrule(rule)
	return []*memo.GroupExpr{old.GetExpr()}, true, false, nil
}

func (s *testCascadesSuite) TestAppliedMemruleSet(c *C) {
	rule := fakeTransformation{}
	rule.pattern = memo.NewPattern(memo.OperandProjection, memo.EngineAll)
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperandProjection: {
			&rule,
		},
	})
	defer func() {
		s.optimizer.ResetTransformationMemrules(DefaultMemruleBatches...)
	}()
	stmt, err := s.ParseOneStmt("select 1", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	group := memo.Convert2Group(logic)
	err = s.optimizer.onPhaseExploration(s.sctx, group)
	c.Assert(err, IsNil)
	c.Assert(rule.appliedTimes, Equals, 1)
}
