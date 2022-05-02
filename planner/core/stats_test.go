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

package core_test

import (
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/property"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/hint"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	. "github.com/whtcorpsinc/check"
)

var _ = Suite(&testStatsSuite{})

type testStatsSuite struct {
	*berolinaAllegroSQL.berolinaAllegroSQL
	testData solitonutil.TestData
}

func (s *testStatsSuite) SetUpSuite(c *C) {
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	s.berolinaAllegroSQL.EnableWindowFunc(true)

	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "stats_suite")
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testStatsSuite) TestGroupNDVs(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t1 values(1,1),(1,2),(2,1),(2,2),(1,1)")
	tk.MustExec("create block t2(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t2 values(1,1),(1,2),(1,3),(2,1),(2,2),(2,3),(3,1),(3,2),(3,3),(1,1)")
	tk.MustExec("analyze block t1")
	tk.MustExec("analyze block t2")

	ctx := context.Background()
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		AggInput          string
		JoinInput         string
	}
	is := dom.SchemaReplicant()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		core.Preprocess(tk.Se, stmt, is)
		builder := core.NewPlanBuilder(tk.Se, is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil, comment)
		p, err = core.LogicalOptimize(ctx, builder.GetOptFlag(), p.(core.LogicalPlan))
		c.Assert(err, IsNil, comment)
		lp := p.(core.LogicalPlan)
		_, err = core.RecursiveDeriveStats4Test(lp)
		c.Assert(err, IsNil, comment)
		var agg *core.LogicalAggregation
		var join *core.LogicalJoin
		stack := make([]core.LogicalPlan, 0, 2)
		traversed := false
		for !traversed {
			switch v := lp.(type) {
			case *core.LogicalAggregation:
				agg = v
				lp = lp.Children()[0]
			case *core.LogicalJoin:
				join = v
				lp = v.Children()[0]
				stack = append(stack, v.Children()[1])
			case *core.LogicalApply:
				lp = lp.Children()[0]
				stack = append(stack, v.Children()[1])
			case *core.LogicalUnionAll:
				lp = lp.Children()[0]
				for i := 1; i < len(v.Children()); i++ {
					stack = append(stack, v.Children()[i])
				}
			case *core.DataSource:
				if len(stack) == 0 {
					traversed = true
				} else {
					lp = stack[0]
					stack = stack[1:]
				}
			default:
				lp = lp.Children()[0]
			}
		}
		aggInput := ""
		joinInput := ""
		if agg != nil {
			s := core.GetStats4Test(agg.Children()[0])
			aggInput = property.ToString(s.GroupNDVs)
		}
		if join != nil {
			l := core.GetStats4Test(join.Children()[0])
			r := core.GetStats4Test(join.Children()[1])
			joinInput = property.ToString(l.GroupNDVs) + ";" + property.ToString(r.GroupNDVs)
		}
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].AggInput = aggInput
			output[i].JoinInput = joinInput
		})
		c.Assert(aggInput, Equals, output[i].AggInput, comment)
		c.Assert(joinInput, Equals, output[i].JoinInput, comment)
	}
}
