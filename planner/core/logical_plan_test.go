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
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/format"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/planner/property"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
)

var _ = Suite(&testPlanSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testPlanSuite struct {
	*berolinaAllegroSQL.berolinaAllegroSQL

	is  schemareplicant.SchemaReplicant
	ctx stochastikctx.Context

	testData solitonutil.TestData

	optimizeVars map[string]string
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.is = schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{MockSignedBlock(), MockUnsignedBlock(), MockView()})
	s.ctx = MockContext()
	s.ctx.GetStochastikVars().EnableWindowFunction = true
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	s.berolinaAllegroSQL.EnableWindowFunc(true)

	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "plan_suite_unexported")
	c.Assert(err, IsNil)
}

func (s *testPlanSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testPlanSuite) TestPredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for ith, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagDecorrelate|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[ith] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[ith], Commentf("for %s %d", ca, ith))
	}
}

func (s *testPlanSuite) TestJoinPredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			Left  string
			Right string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagDecorrelate|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		proj, ok := p.(*LogicalProjection)
		c.Assert(ok, IsTrue, comment)
		join, ok := proj.children[0].(*LogicalJoin)
		c.Assert(ok, IsTrue, comment)
		leftPlan, ok := join.children[0].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		rightPlan, ok := join.children[1].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		leftCond := fmt.Sprintf("%s", leftPlan.pushedDownConds)
		rightCond := fmt.Sprintf("%s", rightPlan.pushedDownConds)
		s.testData.OnRecord(func() {
			output[i].Left, output[i].Right = leftCond, rightCond
		})
		c.Assert(leftCond, Equals, output[i].Left, comment)
		c.Assert(rightCond, Equals, output[i].Right, comment)
	}
}

func (s *testPlanSuite) TestOuterWherePredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			Sel   string
			Left  string
			Right string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagDecorrelate|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		proj, ok := p.(*LogicalProjection)
		c.Assert(ok, IsTrue, comment)
		selection, ok := proj.children[0].(*LogicalSelection)
		c.Assert(ok, IsTrue, comment)
		selCond := fmt.Sprintf("%s", selection.Conditions)
		s.testData.OnRecord(func() {
			output[i].Sel = selCond
		})
		c.Assert(selCond, Equals, output[i].Sel, comment)
		join, ok := selection.children[0].(*LogicalJoin)
		c.Assert(ok, IsTrue, comment)
		leftPlan, ok := join.children[0].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		rightPlan, ok := join.children[1].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		leftCond := fmt.Sprintf("%s", leftPlan.pushedDownConds)
		rightCond := fmt.Sprintf("%s", rightPlan.pushedDownConds)
		s.testData.OnRecord(func() {
			output[i].Left, output[i].Right = leftCond, rightCond
		})
		c.Assert(leftCond, Equals, output[i].Left, comment)
		c.Assert(rightCond, Equals, output[i].Right, comment)
	}
}

func (s *testPlanSuite) TestSimplifyOuterJoin(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			Best     string
			JoinType string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		planString := ToString(p)
		s.testData.OnRecord(func() {
			output[i].Best = planString
		})
		c.Assert(planString, Equals, output[i].Best, comment)
		join, ok := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		if !ok {
			join, ok = p.(LogicalPlan).Children()[0].Children()[0].(*LogicalJoin)
			c.Assert(ok, IsTrue, comment)
		}
		s.testData.OnRecord(func() {
			output[i].JoinType = join.JoinType.String()
		})
		c.Assert(join.JoinType.String(), Equals, output[i].JoinType, comment)
	}
}

func (s *testPlanSuite) TestAntiSemiJoinConstFalse(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		allegrosql string
		best       string
		joinType   string
	}{
		{
			allegrosql: "select a from t t1 where not exists (select a from t t2 where t1.a = t2.a and t2.b = 1 and t2.b = 2)",
			best:       "Join{DataScan(t1)->DataScan(t2)}(test.t.a,test.t.a)->Projection",
			joinType:   "anti semi join",
		},
	}

	ctx := context.Background()
	for _, ca := range tests {
		comment := Commentf("for %s", ca.allegrosql)
		stmt, err := s.ParseOneStmt(ca.allegrosql, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagDecorrelate|flagPredicatePushDown|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		c.Assert(ToString(p), Equals, ca.best, comment)
		join, _ := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		c.Assert(join.JoinType.String(), Equals, ca.joinType, comment)
	}
}

func (s *testPlanSuite) TestDeriveNotNullConds(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			Plan  string
			Left  string
			Right string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain|flagDecorrelate, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].Plan = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[i].Plan, comment)
		join := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		left := join.Children()[0].(*DataSource)
		right := join.Children()[1].(*DataSource)
		leftConds := fmt.Sprintf("%s", left.pushedDownConds)
		rightConds := fmt.Sprintf("%s", right.pushedDownConds)
		s.testData.OnRecord(func() {
			output[i].Left, output[i].Right = leftConds, rightConds
		})
		c.Assert(leftConds, Equals, output[i].Left, comment)
		c.Assert(rightConds, Equals, output[i].Right, comment)
	}
}

func buildLogicPlan4GroupBy(s *testPlanSuite, c *C, allegrosql string) (Plan, error) {
	sqlMode := s.ctx.GetStochastikVars().ALLEGROSQLMode
	mockedBlockInfo := MockSignedBlock()
	// mock the block info here for later use
	// enable only full group by
	s.ctx.GetStochastikVars().ALLEGROSQLMode = sqlMode | allegrosql.ModeOnlyFullGroupBy
	defer func() { s.ctx.GetStochastikVars().ALLEGROSQLMode = sqlMode }() // restore it
	comment := Commentf("for %s", allegrosql)
	stmt, err := s.ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil, comment)

	stmt.(*ast.SelectStmt).From.BlockRefs.Left.(*ast.BlockSource).Source.(*ast.BlockName).BlockInfo = mockedBlockInfo

	p, _, err := BuildLogicalPlan(context.Background(), s.ctx, stmt, s.is)
	return p, err
}

func (s *testPlanSuite) TestGroupByWhenNotExistDefCauss(c *C) {
	sqlTests := []struct {
		allegrosql       string
		expectedErrMatch string
	}{
		{
			allegrosql:       "select a from t group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has an as column alias
			allegrosql:       "select a as tempField from t group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has as block alias
			allegrosql:       "select tempBlock.a from t as tempBlock group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.tempBlock\\.a'.*",
		},
		{
			// has a func call
			allegrosql:       "select length(a) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two defcaus
			allegrosql:       "select length(b + a) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two defcaus
			allegrosql:       "select length(a + b) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two defcaus
			allegrosql:       "select length(a + b) as tempField from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
	}
	for _, test := range sqlTests {
		allegrosql := test.allegrosql
		p, err := buildLogicPlan4GroupBy(s, c, allegrosql)
		c.Assert(err, NotNil)
		c.Assert(p, IsNil)
		c.Assert(err, ErrorMatches, test.expectedErrMatch)
	}
}

func (s *testPlanSuite) TestDupRandJoinCondsPushDown(c *C) {
	allegrosql := "select * from t as t1 join t t2 on t1.a > rand() and t1.a > rand()"
	comment := Commentf("for %s", allegrosql)
	stmt, err := s.ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil, comment)
	p, _, err := BuildLogicalPlan(context.Background(), s.ctx, stmt, s.is)
	c.Assert(err, IsNil, comment)
	p, err = logicalOptimize(context.TODO(), flagPredicatePushDown, p.(LogicalPlan))
	c.Assert(err, IsNil, comment)
	proj, ok := p.(*LogicalProjection)
	c.Assert(ok, IsTrue, comment)
	join, ok := proj.children[0].(*LogicalJoin)
	c.Assert(ok, IsTrue, comment)
	leftPlan, ok := join.children[0].(*LogicalSelection)
	c.Assert(ok, IsTrue, comment)
	leftCond := fmt.Sprintf("%s", leftPlan.Conditions)
	// Condition with mublock function cannot be de-duplicated when push down join conds.
	c.Assert(leftCond, Equals, "[gt(cast(test.t.a, double BINARY), rand()) gt(cast(test.t.a, double BINARY), rand())]", comment)
}

func (s *testPlanSuite) TestBlockPartition(c *C) {
	defer testleak.AfterTest(c)()
	definitions := []perceptron.PartitionDefinition{
		{
			ID:       41,
			Name:     perceptron.NewCIStr("p1"),
			LessThan: []string{"16"},
		},
		{
			ID:       42,
			Name:     perceptron.NewCIStr("p2"),
			LessThan: []string{"32"},
		},
		{
			ID:       43,
			Name:     perceptron.NewCIStr("p3"),
			LessThan: []string{"64"},
		},
		{
			ID:       44,
			Name:     perceptron.NewCIStr("p4"),
			LessThan: []string{"128"},
		},
		{
			ID:       45,
			Name:     perceptron.NewCIStr("p5"),
			LessThan: []string{"maxvalue"},
		},
	}
	is := MockPartitionSchemaReplicant(definitions)
	// is1 equals to is without maxvalue partition.
	definitions1 := make([]perceptron.PartitionDefinition, len(definitions)-1)
	INTERLOCKy(definitions1, definitions)
	is1 := MockPartitionSchemaReplicant(definitions1)
	isChoices := []schemareplicant.SchemaReplicant{is, is1}

	var (
		input []struct {
			ALLEGROALLEGROSQL string
			IsIdx             int
		}
		output []string
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca.ALLEGROALLEGROSQL)
		stmt, err := s.ParseOneStmt(ca.ALLEGROALLEGROSQL, "", "")
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {

		})
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, isChoices[ca.IsIdx])
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagDecorrelate|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain|flagPredicatePushDown|flagPartitionProcessor, p.(LogicalPlan))
		c.Assert(err, IsNil)
		planString := ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(ToString(p), Equals, output[i], Commentf("for %s", ca))
	}
}

func (s *testPlanSuite) TestSubquery(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for ith, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)

		Preprocess(s.ctx, stmt, s.is)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagDecorrelate|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain, lp)
			c.Assert(err, IsNil)
		}
		s.testData.OnRecord(func() {
			output[ith] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[ith], Commentf("for %s %d", ca, ith))
	}
}

func (s *testPlanSuite) TestPlanBuilder(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, ca := range input {
		comment := Commentf("for %s", ca)
		stmt, err := s.ParseOneStmt(ca, "", "")
		c.Assert(err, IsNil, comment)

		s.ctx.GetStochastikVars().SetHashJoinConcurrency(1)
		Preprocess(s.ctx, stmt, s.is)
		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(context.TODO(), flagPrunDeferredCausets|flagPrunDeferredCausetsAgain, lp)
			c.Assert(err, IsNil)
		}
		s.testData.OnRecord(func() {
			output[i] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[i], Commentf("for %s", ca))
	}
}

func (s *testPlanSuite) TestJoinReOrder(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagJoinReOrder, p.(LogicalPlan))
		c.Assert(err, IsNil)
		planString := ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestEagerAggregation(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []string
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	s.ctx.GetStochastikVars().AllowAggPushDown = true
	for ith, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagPredicatePushDown|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain|flagPushDownAgg, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[ith] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[ith], Commentf("for %s %d", tt, ith))
	}
	s.ctx.GetStochastikVars().AllowAggPushDown = false
}

func (s *testPlanSuite) TestDeferredCausetPruning(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []map[int][]string
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		lp, err := logicalOptimize(ctx, flagPredicatePushDown|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i] = make(map[int][]string)
		})
		s.checkDataSourceDefCauss(lp, c, output[i], comment)
	}
}

func (s *testPlanSuite) TestProjectionEliminator(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		allegrosql string
		best       string
	}{
		{
			allegrosql: "select 1+num from (select 1+a as num from t) t1;",
			best:       "DataScan(t)->Projection",
		},
	}

	ctx := context.Background()
	for ith, tt := range tests {
		comment := Commentf("for %s", tt.allegrosql)
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain|flagEliminateProjection, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, Commentf("for %s %d", tt.allegrosql, ith))
	}
}

func (s *testPlanSuite) TestAllocID(c *C) {
	ctx := MockContext()
	pA := DataSource{}.Init(ctx, 0)
	pB := DataSource{}.Init(ctx, 0)
	c.Assert(pA.id+1, Equals, pB.id)
}

func (s *testPlanSuite) checkDataSourceDefCauss(p LogicalPlan, c *C, ans map[int][]string, comment CommentInterface) {
	switch p.(type) {
	case *DataSource:
		s.testData.OnRecord(func() {
			ans[p.ID()] = make([]string, p.Schema().Len())
		})
		colList, ok := ans[p.ID()]
		c.Assert(ok, IsTrue, Commentf("For %v DataSource ID %d Not found", comment, p.ID()))
		c.Assert(len(p.Schema().DeferredCausets), Equals, len(colList), comment)
		for i, col := range p.Schema().DeferredCausets {
			s.testData.OnRecord(func() {
				colList[i] = col.String()
			})
			c.Assert(col.String(), Equals, colList[i], comment)
		}
	case *LogicalUnionAll:
		s.testData.OnRecord(func() {
			ans[p.ID()] = make([]string, p.Schema().Len())
		})
		colList, ok := ans[p.ID()]
		c.Assert(ok, IsTrue, Commentf("For %v UnionAll ID %d Not found", comment, p.ID()))
		c.Assert(len(p.Schema().DeferredCausets), Equals, len(colList), comment)
		for i, col := range p.Schema().DeferredCausets {
			s.testData.OnRecord(func() {
				colList[i] = col.String()
			})
			c.Assert(col.String(), Equals, colList[i], comment)
		}
	}
	for _, child := range p.Children() {
		s.checkDataSourceDefCauss(child, c, ans, comment)
	}
}

func (s *testPlanSuite) TestValidate(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		allegrosql string
		err        *terror.Error
	}{
		{
			allegrosql: "select date_format((1,2), '%H');",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select cast((1,2) as date)",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select (1,2) between (3,4) and (5,6)",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select (1,2) rlike '1'",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select (1,2) like '1'",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select case(1,2) when(1,2) then true end",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select (1,2) in ((3,4),(5,6))",
			err:        nil,
		},
		{
			allegrosql: "select event(1,(2,3)) in (select a,b from t)",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select event(1,2) in (select a,b from t)",
			err:        nil,
		},
		{
			allegrosql: "select (1,2) in ((3,4),5)",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select (1,2) is true",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select (1,2) is null",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select (+(1,2))=(1,2)",
			err:        nil,
		},
		{
			allegrosql: "select (-(1,2))=(1,2)",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select (1,2)||(1,2)",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select (1,2) < (3,4)",
			err:        nil,
		},
		{
			allegrosql: "select (1,2) < 3",
			err:        expression.ErrOperandDeferredCausets,
		},
		{
			allegrosql: "select 1, * from t",
			err:        ErrInvalidWildCard,
		},
		{
			allegrosql: "select *, 1 from t",
			err:        nil,
		},
		{
			allegrosql: "select 1, t.* from t",
			err:        nil,
		},
		{
			allegrosql: "select 1 from t t1, t t2 where t1.a > all((select a) union (select a))",
			err:        ErrAmbiguous,
		},
		{
			allegrosql: "insert into t set a = 1, b = a + 1",
			err:        nil,
		},
		{
			allegrosql: "insert into t set a = 1, b = values(a) + 1",
			err:        nil,
		},
		{
			allegrosql: "select a, b, c from t order by 0",
			err:        ErrUnknownDeferredCauset,
		},
		{
			allegrosql: "select a, b, c from t order by 4",
			err:        ErrUnknownDeferredCauset,
		},
		{
			allegrosql: "select a as c1, b as c1 from t order by c1",
			err:        ErrAmbiguous,
		},
		{
			allegrosql: "(select a as b, b from t) union (select a, b from t) order by b",
			err:        ErrAmbiguous,
		},
		{
			allegrosql: "(select a as b, b from t) union (select a, b from t) order by a",
			err:        ErrUnknownDeferredCauset,
		},
		{
			allegrosql: "select * from t t1 use index(e)",
			err:        ErrKeyDoesNotExist,
		},
		{
			allegrosql: "select a from t having c2",
			err:        ErrUnknownDeferredCauset,
		},
		{
			allegrosql: "select a from t group by c2 + 1 having c2",
			err:        ErrUnknownDeferredCauset,
		},
		{
			allegrosql: "select a as b, b from t having b",
			err:        ErrAmbiguous,
		},
		{
			allegrosql: "select a + 1 from t having a",
			err:        ErrUnknownDeferredCauset,
		},
		{
			allegrosql: "select a from t having sum(avg(a))",
			err:        ErrInvalidGroupFuncUse,
		},
		{
			allegrosql: "select concat(c_str, d_str) from t group by `concat(c_str, d_str)`",
			err:        nil,
		},
		{
			allegrosql: "select concat(c_str, d_str) from t group by `concat(c_str,d_str)`",
			err:        ErrUnknownDeferredCauset,
		},
		{
			allegrosql: "select a from t b having b.a",
			err:        nil,
		},
		{
			allegrosql: "select b.a from t b having b.a",
			err:        nil,
		},
		{
			allegrosql: "select b.a from t b having a",
			err:        nil,
		},
		{
			allegrosql: "select a+1 from t having t.a",
			err:        ErrUnknownDeferredCauset,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		allegrosql := tt.allegrosql
		comment := Commentf("for %s", allegrosql)
		stmt, err := s.ParseOneStmt(allegrosql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		_, _, err = BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		if tt.err == nil {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(tt.err.Equal(err), IsTrue, comment)
		}
	}
}

func (s *testPlanSuite) checkUniqueKeys(p LogicalPlan, c *C, ans map[int][][]string, allegrosql string) {
	s.testData.OnRecord(func() {
		ans[p.ID()] = make([][]string, len(p.Schema().Keys))
	})
	keyList, ok := ans[p.ID()]
	c.Assert(ok, IsTrue, Commentf("for %s, %v not found", allegrosql, p.ID()))
	c.Assert(len(p.Schema().Keys), Equals, len(keyList), Commentf("for %s, %v, the number of key doesn't match, the schemaReplicant is %s", allegrosql, p.ID(), p.Schema()))
	for i := range keyList {
		s.testData.OnRecord(func() {
			keyList[i] = make([]string, len(p.Schema().Keys[i]))
		})
		c.Assert(len(p.Schema().Keys[i]), Equals, len(keyList[i]), Commentf("for %s, %v %v, the number of column doesn't match", allegrosql, p.ID(), keyList[i]))
		for j := range keyList[i] {
			s.testData.OnRecord(func() {
				keyList[i][j] = p.Schema().Keys[i][j].String()
			})
			c.Assert(p.Schema().Keys[i][j].String(), Equals, keyList[i][j], Commentf("for %s, %v %v, column dosen't match", allegrosql, p.ID(), keyList[i]))
		}
	}
	s.testData.OnRecord(func() {
		ans[p.ID()] = keyList
	})
	for _, child := range p.Children() {
		s.checkUniqueKeys(child, c, ans, allegrosql)
	}
}

func (s *testPlanSuite) TestUniqueKeyInfo(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []map[int][][]string
	s.testData.GetTestCases(c, &input, &output)
	s.testData.OnRecord(func() {
		output = make([]map[int][][]string, len(input))
	})

	ctx := context.Background()
	for ith, tt := range input {
		comment := Commentf("for %s %d", tt, ith)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		lp, err := logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunDeferredCausets|flagBuildKeyInfo, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[ith] = make(map[int][][]string)
		})
		s.checkUniqueKeys(lp, c, output[ith], tt)
	}
}

func (s *testPlanSuite) TestAggPrune(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)

		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunDeferredCausets|flagPrunDeferredCausetsAgain|flagBuildKeyInfo|flagEliminateAgg|flagEliminateProjection, p.(LogicalPlan))
		c.Assert(err, IsNil)
		planString := ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], comment)
	}
}

func (s *testPlanSuite) TestVisitInfo(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		allegrosql string
		ans        []visitInfo
	}{
		{
			allegrosql: "insert into t (a) values (1)",
			ans: []visitInfo{
				{allegrosql.InsertPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "delete from t where a = 1",
			ans: []visitInfo{
				{allegrosql.DeletePriv, "test", "t", "", nil},
				{allegrosql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "delete from a1 using t as a1 inner join t as a2 where a1.a = a2.a",
			ans: []visitInfo{
				{allegrosql.DeletePriv, "test", "t", "", nil},
				{allegrosql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "uFIDelate t set a = 7 where a = 1",
			ans: []visitInfo{
				{allegrosql.UFIDelatePriv, "test", "t", "", nil},
				{allegrosql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "uFIDelate t, (select * from t) a1 set t.a = a1.a;",
			ans: []visitInfo{
				{allegrosql.UFIDelatePriv, "test", "t", "", nil},
				{allegrosql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "uFIDelate t a1 set a1.a = a1.a + 1",
			ans: []visitInfo{
				{allegrosql.UFIDelatePriv, "test", "t", "", nil},
				{allegrosql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "select a, sum(e) from t group by a",
			ans: []visitInfo{
				{allegrosql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "truncate block t",
			ans: []visitInfo{
				{allegrosql.DropPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "drop block t",
			ans: []visitInfo{
				{allegrosql.DropPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "create block t (a int)",
			ans: []visitInfo{
				{allegrosql.CreatePriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "create block t1 like t",
			ans: []visitInfo{
				{allegrosql.CreatePriv, "test", "t1", "", nil},
				{allegrosql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "create database test",
			ans: []visitInfo{
				{allegrosql.CreatePriv, "test", "", "", nil},
			},
		},
		{
			allegrosql: "drop database test",
			ans: []visitInfo{
				{allegrosql.DropPriv, "test", "", "", nil},
			},
		},
		{
			allegrosql: "create index t_1 on t (a)",
			ans: []visitInfo{
				{allegrosql.IndexPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "drop index e on t",
			ans: []visitInfo{
				{allegrosql.IndexPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: `grant all privileges on test.* to 'test'@'%'`,
			ans: []visitInfo{
				{allegrosql.SelectPriv, "test", "", "", nil},
				{allegrosql.InsertPriv, "test", "", "", nil},
				{allegrosql.UFIDelatePriv, "test", "", "", nil},
				{allegrosql.DeletePriv, "test", "", "", nil},
				{allegrosql.CreatePriv, "test", "", "", nil},
				{allegrosql.DropPriv, "test", "", "", nil},
				{allegrosql.GrantPriv, "test", "", "", nil},
				{allegrosql.AlterPriv, "test", "", "", nil},
				{allegrosql.ExecutePriv, "test", "", "", nil},
				{allegrosql.IndexPriv, "test", "", "", nil},
				{allegrosql.CreateViewPriv, "test", "", "", nil},
				{allegrosql.ShowViewPriv, "test", "", "", nil},
			},
		},
		{
			allegrosql: `grant select on test.ttt to 'test'@'%'`,
			ans: []visitInfo{
				{allegrosql.SelectPriv, "test", "ttt", "", nil},
				{allegrosql.GrantPriv, "test", "ttt", "", nil},
			},
		},
		{
			allegrosql: `grant select on ttt to 'test'@'%'`,
			ans: []visitInfo{
				{allegrosql.SelectPriv, "test", "ttt", "", nil},
				{allegrosql.GrantPriv, "test", "ttt", "", nil},
			},
		},
		{
			allegrosql: `revoke all privileges on test.* from 'test'@'%'`,
			ans: []visitInfo{
				{allegrosql.SelectPriv, "test", "", "", nil},
				{allegrosql.InsertPriv, "test", "", "", nil},
				{allegrosql.UFIDelatePriv, "test", "", "", nil},
				{allegrosql.DeletePriv, "test", "", "", nil},
				{allegrosql.CreatePriv, "test", "", "", nil},
				{allegrosql.DropPriv, "test", "", "", nil},
				{allegrosql.GrantPriv, "test", "", "", nil},
				{allegrosql.AlterPriv, "test", "", "", nil},
				{allegrosql.ExecutePriv, "test", "", "", nil},
				{allegrosql.IndexPriv, "test", "", "", nil},
				{allegrosql.CreateViewPriv, "test", "", "", nil},
				{allegrosql.ShowViewPriv, "test", "", "", nil},
			},
		},
		{
			allegrosql: `set password for 'root'@'%' = 'xxxxx'`,
			ans:        []visitInfo{},
		},
		{
			allegrosql: `show create block test.ttt`,
			ans: []visitInfo{
				{allegrosql.AllPrivMask, "test", "ttt", "", nil},
			},
		},
		{
			allegrosql: "alter block t add column a int(4)",
			ans: []visitInfo{
				{allegrosql.AlterPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "rename block t_old to t_new",
			ans: []visitInfo{
				{allegrosql.AlterPriv, "test", "t_old", "", nil},
				{allegrosql.DropPriv, "test", "t_old", "", nil},
				{allegrosql.CreatePriv, "test", "t_new", "", nil},
				{allegrosql.InsertPriv, "test", "t_new", "", nil},
			},
		},
		{
			allegrosql: "alter block t_old rename to t_new",
			ans: []visitInfo{
				{allegrosql.AlterPriv, "test", "t_old", "", nil},
				{allegrosql.DropPriv, "test", "t_old", "", nil},
				{allegrosql.CreatePriv, "test", "t_new", "", nil},
				{allegrosql.InsertPriv, "test", "t_new", "", nil},
			},
		},
		{
			allegrosql: "alter block t drop partition p0;",
			ans: []visitInfo{
				{allegrosql.AlterPriv, "test", "t", "", nil},
				{allegrosql.DropPriv, "test", "t", "", nil},
			},
		},
		{
			allegrosql: "flush privileges",
			ans: []visitInfo{
				{allegrosql.ReloadPriv, "", "", "", ErrSpecificAccessDenied},
			},
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.allegrosql)
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
		builder.ctx.GetStochastikVars().SetHashJoinConcurrency(1)
		_, err = builder.Build(context.TODO(), stmt)
		c.Assert(err, IsNil, comment)

		checkVisitInfo(c, builder.visitInfo, tt.ans, comment)
	}
}

type visitInfoArray []visitInfo

func (v visitInfoArray) Len() int {
	return len(v)
}

func (v visitInfoArray) Less(i, j int) bool {
	if v[i].privilege < v[j].privilege {
		return true
	}
	if v[i].EDB < v[j].EDB {
		return true
	}
	if v[i].block < v[j].block {
		return true
	}
	if v[i].column < v[j].column {
		return true
	}

	return false
}

func (v visitInfoArray) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

func unique(v []visitInfo) []visitInfo {
	repeat := 0
	for i := 1; i < len(v); i++ {
		if v[i] == v[i-1] {
			repeat++
		} else {
			v[i-repeat] = v[i]
		}
	}
	return v[:len(v)-repeat]
}

func checkVisitInfo(c *C, v1, v2 []visitInfo, comment CommentInterface) {
	sort.Sort(visitInfoArray(v1))
	sort.Sort(visitInfoArray(v2))
	v1 = unique(v1)
	v2 = unique(v2)

	c.Assert(len(v1), Equals, len(v2), comment)
	for i := 0; i < len(v1); i++ {
		// loose compare errors for code match
		c.Assert(terror.ErrorEqual(v1[i].err, v2[i].err), IsTrue, Commentf("err1 %v, err2 %v for %s", v1[i].err, v2[i].err, comment))
		// compare remainder
		v1[i].err = v2[i].err
		c.Assert(v1[i], Equals, v2[i], comment)
	}
}

func (s *testPlanSuite) TestUnion(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	var input []string
	var output []struct {
		Best string
		Err  bool
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.TODO()
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
		plan, err := builder.Build(ctx, stmt)
		s.testData.OnRecord(func() {
			output[i].Err = err != nil
		})
		if output[i].Err {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		p := plan.(LogicalPlan)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		s.testData.OnRecord(func() {
			output[i].Best = ToString(p)
		})
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestTopNPushDown(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.TODO()
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[i], comment)
	}
}

func (s *testPlanSuite) TestNameResolver(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		allegrosql string
		err        string
	}{
		{"select a from t", ""},
		{"select c3 from t", "[planner:1054]Unknown column 'c3' in 'field list'"},
		{"select c1 from t4", "[schemaReplicant:1146]Block 'test.t4' doesn't exist"},
		{"select * from t", ""},
		{"select t.* from t", ""},
		{"select t2.* from t", "[planner:1051]Unknown block 't2'"},
		{"select b as a, c as a from t group by a", "[planner:1052]DeferredCauset 'c' in field list is ambiguous"},
		{"select 1 as a, b as a, c as a from t group by a", ""},
		{"select a, b as a from t group by a+1", ""},
		{"select c, a as c from t order by c+1", ""},
		{"select * from t as t1, t as t2 join t as t3 on t2.a = t3.a", ""},
		{"select * from t as t1, t as t2 join t as t3 on t1.c1 = t2.a", "[planner:1054]Unknown column 't1.c1' in 'on clause'"},
		{"select a from t group by a having a = 3", ""},
		{"select a from t group by a having c2 = 3", "[planner:1054]Unknown column 'c2' in 'having clause'"},
		{"select a from t where exists (select b)", ""},
		{"select cnt from (select count(a) as cnt from t group by b) as t2 group by cnt", ""},
		{"select a from t where t11.a < t.a", "[planner:1054]Unknown column 't11.a' in 'where clause'"},
		{"select a from t having t11.c1 < t.a", "[planner:1054]Unknown column 't11.c1' in 'having clause'"},
		{"select a from t where t.a < t.a order by t11.c1", "[planner:1054]Unknown column 't11.c1' in 'order clause'"},
		{"select a from t group by t11.c1", "[planner:1054]Unknown column 't11.c1' in 'group statement'"},
		{"delete a from (select * from t ) as a, t", "[planner:1288]The target block a of the DELETE is not uFIDelablock"},
		{"delete b from (select * from t ) as a, t", "[planner:1109]Unknown block 'b' in MULTI DELETE"},
		{"select '' as fakeDefCaus from t group by values(fakeDefCaus)", "[planner:1054]Unknown column '' in 'VALUES() function'"},
		{"uFIDelate t, (select * from ht) as b set b.a = t.a", "[planner:1288]The target block b of the UFIDelATE is not uFIDelablock"},
		{"select row_number() over () from t group by 1", "[planner:1056]Can't group on 'row_number() over ()'"},
	}

	ctx := context.Background()
	for _, t := range tests {
		comment := Commentf("for %s", t.allegrosql)
		stmt, err := s.ParseOneStmt(t.allegrosql, "", "")
		c.Assert(err, IsNil, comment)
		s.ctx.GetStochastikVars().SetHashJoinConcurrency(1)

		_, _, err = BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		if t.err == "" {
			c.Check(err, IsNil)
		} else {
			c.Assert(err.Error(), Equals, t.err)
		}
	}
}

func (s *testPlanSuite) TestOuterJoinEliminator(c *C) {
	defer testleak.AfterTest(c)()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.TODO()
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		planString := ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], comment)
	}
}

func (s *testPlanSuite) TestSelectView(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		allegrosql string
		best       string
	}{
		{
			allegrosql: "select * from v",
			best:       "DataScan(t)->Projection",
		},
		{
			allegrosql: "select v.b, v.c, v.d from v",
			best:       "DataScan(t)->Projection",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v allegrosql:%s", i, tt.allegrosql)
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestWindowFunction(c *C) {
	s.optimizeVars = map[string]string{
		variable.MilevaDBWindowConcurrency: "1",
	}
	defer func() {
		s.optimizeVars = nil
		testleak.AfterTest(c)()
	}()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	s.doTestWindowFunction(c, input, output)
}

func (s *testPlanSuite) TestWindowParallelFunction(c *C) {
	s.optimizeVars = map[string]string{
		variable.MilevaDBWindowConcurrency: "4",
	}
	defer func() {
		s.optimizeVars = nil
		testleak.AfterTest(c)()
	}()
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	s.doTestWindowFunction(c, input, output)
}

func (s *testPlanSuite) doTestWindowFunction(c *C, input, output []string) {
	ctx := context.TODO()
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		p, stmt, err := s.optimize(ctx, tt)
		if err != nil {
			s.testData.OnRecord(func() {
				output[i] = err.Error()
			})
			c.Assert(err.Error(), Equals, output[i], comment)
			continue
		}
		s.testData.OnRecord(func() {
			output[i] = ToString(p)
		})
		c.Assert(ToString(p), Equals, output[i], comment)

		var sb strings.Builder
		// After restore, the result should be the same.
		err = stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		c.Assert(err, IsNil)
		p, _, err = s.optimize(ctx, sb.String())
		if err != nil {
			c.Assert(err.Error(), Equals, output[i], comment)
			continue
		}
		c.Assert(ToString(p), Equals, output[i], comment)
	}
}

func (s *testPlanSuite) optimize(ctx context.Context, allegrosql string) (PhysicalPlan, ast.Node, error) {
	stmt, err := s.ParseOneStmt(allegrosql, "", "")
	if err != nil {
		return nil, nil, err
	}
	err = Preprocess(s.ctx, stmt, s.is)
	if err != nil {
		return nil, nil, err
	}

	sctx := MockContext()
	for k, v := range s.optimizeVars {
		if err = sctx.GetStochastikVars().SetSystemVar(k, v); err != nil {
			return nil, nil, err
		}
	}
	builder := NewPlanBuilder(sctx, s.is, &hint.BlockHintProcessor{})
	p, err := builder.Build(ctx, stmt)
	if err != nil {
		return nil, nil, err
	}
	p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
	if err != nil {
		return nil, nil, err
	}
	p, _, err = physicalOptimize(p.(LogicalPlan), &PlanCounterDisabled)
	return p.(PhysicalPlan), stmt, err
}

func byItemsToProperty(byItems []*soliton.ByItems) *property.PhysicalProperty {
	pp := &property.PhysicalProperty{}
	for _, item := range byItems {
		pp.Items = append(pp.Items, property.Item{DefCaus: item.Expr.(*expression.DeferredCauset), Desc: item.Desc})
	}
	return pp
}

func pathsName(paths []*candidatePath) string {
	var names []string
	for _, path := range paths {
		if path.path.IsBlockPath() {
			names = append(names, "PRIMARY_KEY")
		} else {
			names = append(names, path.path.Index.Name.O)
		}
	}
	return strings.Join(names, ",")
}

func (s *testPlanSuite) TestSkylinePruning(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		allegrosql string
		result     string
	}{
		{
			allegrosql: "select * from t",
			result:     "PRIMARY_KEY",
		},
		{
			allegrosql: "select * from t order by f",
			result:     "PRIMARY_KEY,f,f_g",
		},
		{
			allegrosql: "select * from t where a > 1",
			result:     "PRIMARY_KEY",
		},
		{
			allegrosql: "select * from t where a > 1 order by f",
			result:     "PRIMARY_KEY,f,f_g",
		},
		{
			allegrosql: "select * from t where f > 1",
			result:     "PRIMARY_KEY,f,f_g",
		},
		{
			allegrosql: "select f from t where f > 1",
			result:     "f,f_g",
		},
		{
			allegrosql: "select f from t where f > 1 order by a",
			result:     "PRIMARY_KEY,f,f_g",
		},
		{
			allegrosql: "select * from t where f > 1 and g > 1",
			result:     "PRIMARY_KEY,f,g,f_g",
		},
		{
			allegrosql: "select count(1) from t",
			result:     "PRIMARY_KEY,c_d_e,f,g,f_g,c_d_e_str,e_d_c_str_prefix",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v allegrosql:%s", i, tt.allegrosql)
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		if err != nil {
			c.Assert(err.Error(), Equals, tt.result, comment)
			continue
		}
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		lp := p.(LogicalPlan)
		_, err = lp.recursiveDeriveStats(nil)
		c.Assert(err, IsNil, comment)
		var ds *DataSource
		var byItems []*soliton.ByItems
		for ds == nil {
			switch v := lp.(type) {
			case *DataSource:
				ds = v
			case *LogicalSort:
				byItems = v.ByItems
				lp = lp.Children()[0]
			case *LogicalProjection:
				newItems := make([]*soliton.ByItems, 0, len(byItems))
				for _, col := range byItems {
					idx := v.schemaReplicant.DeferredCausetIndex(col.Expr.(*expression.DeferredCauset))
					switch expr := v.Exprs[idx].(type) {
					case *expression.DeferredCauset:
						newItems = append(newItems, &soliton.ByItems{Expr: expr, Desc: col.Desc})
					}
				}
				byItems = newItems
				lp = lp.Children()[0]
			default:
				lp = lp.Children()[0]
			}
		}
		paths := ds.skylinePruning(byItemsToProperty(byItems))
		c.Assert(pathsName(paths), Equals, tt.result, comment)
	}
}

func (s *testPlanSuite) TestFastPlanContextBlocks(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		allegrosql string
		fastPlan   bool
	}{
		{
			"select * from t where a=1",
			true,
		},
		{

			"uFIDelate t set f=0 where a=43215",
			true,
		},
		{
			"delete from t where a =43215",
			true,
		},
		{
			"select * from t where a>1",
			false,
		},
	}
	for _, tt := range tests {
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil)
		Preprocess(s.ctx, stmt, s.is)
		s.ctx.GetStochastikVars().StmtCtx.Blocks = nil
		p := TryFastPlan(s.ctx, stmt)
		if tt.fastPlan {
			c.Assert(p, NotNil)
			c.Assert(len(s.ctx.GetStochastikVars().StmtCtx.Blocks), Equals, 1)
			c.Assert(s.ctx.GetStochastikVars().StmtCtx.Blocks[0].Block, Equals, "t")
			c.Assert(s.ctx.GetStochastikVars().StmtCtx.Blocks[0].EDB, Equals, "test")
		} else {
			c.Assert(p, IsNil)
			c.Assert(len(s.ctx.GetStochastikVars().StmtCtx.Blocks), Equals, 0)
		}
	}
}

func (s *testPlanSuite) TestUFIDelateEQCond(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		allegrosql string
		best       string
	}{
		{
			allegrosql: "select t1.a from t t1, t t2 where t1.a = t2.a+1",
			best:       "Join{DataScan(t1)->DataScan(t2)->Projection}(test.t.a,DeferredCauset#25)->Projection",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v allegrosql:%s", i, tt.allegrosql)
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestConflictedJoinTypeHints(c *C) {
	defer testleak.AfterTest(c)()
	allegrosql := "select /*+ INL_JOIN(t1) HASH_JOIN(t1) */ * from t t1, t t2 where t1.e = t2.e"
	ctx := context.TODO()
	stmt, err := s.ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil)
	Preprocess(s.ctx, stmt, s.is)
	builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
	p, err := builder.Build(ctx, stmt)
	c.Assert(err, IsNil)
	p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
	c.Assert(err, IsNil)
	proj, ok := p.(*LogicalProjection)
	c.Assert(ok, IsTrue)
	join, ok := proj.Children()[0].(*LogicalJoin)
	c.Assert(ok, IsTrue)
	c.Assert(join.hintInfo, IsNil)
	c.Assert(join.preferJoinType, Equals, uint(0))
}

func (s *testPlanSuite) TestSimplyOuterJoinWithOnlyOuterExpr(c *C) {
	defer testleak.AfterTest(c)()
	allegrosql := "select * from t t1 right join t t0 ON TRUE where CONCAT_WS(t0.e=t0.e, 0, NULL) IS NULL"
	ctx := context.TODO()
	stmt, err := s.ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil)
	Preprocess(s.ctx, stmt, s.is)
	builder := NewPlanBuilder(MockContext(), s.is, &hint.BlockHintProcessor{})
	p, err := builder.Build(ctx, stmt)
	c.Assert(err, IsNil)
	p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
	c.Assert(err, IsNil)
	proj, ok := p.(*LogicalProjection)
	c.Assert(ok, IsTrue)
	join, ok := proj.Children()[0].(*LogicalJoin)
	c.Assert(ok, IsTrue)
	// previous wrong JoinType is InnerJoin
	c.Assert(join.JoinType, Equals, RightOuterJoin)
}
