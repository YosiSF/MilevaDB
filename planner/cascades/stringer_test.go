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

	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/memo"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
)

var _ = Suite(&testStringerSuite{})

type testStringerSuite struct {
	*berolinaAllegroSQL.berolinaAllegroSQL
	is        schemareplicant.SchemaReplicant
	sctx      stochastikctx.Context
	testData  solitonutil.TestData
	optimizer *Optimizer
}

func (s *testStringerSuite) SetUpSuite(c *C) {
	s.is = schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{plannercore.MockSignedBlock()})
	s.sctx = plannercore.MockContext()
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	s.optimizer = NewOptimizer()
	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "stringer_suite")
	c.Assert(err, IsNil)
}

func (s *testStringerSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testStringerSuite) TestGroupStringer(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperandSelection: {
			NewMemrulePushSelDownEinsteinDBSingleGather(),
			NewMemrulePushSelDownBlockScan(),
		},
		memo.OperandDataSource: {
			NewMemruleEnumeratePaths(),
		},
	})
	defer func() {
		s.optimizer.ResetTransformationMemrules(DefaultMemruleBatches...)
	}()
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		stmt, err := s.ParseOneStmt(allegrosql, "", "")
		c.Assert(err, IsNil)
		p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
		c.Assert(err, IsNil)
		logic, ok := p.(plannercore.LogicalPlan)
		c.Assert(ok, IsTrue)
		logic, err = s.optimizer.onPhasePreprocessing(s.sctx, logic)
		c.Assert(err, IsNil)
		group := memo.Convert2Group(logic)
		err = s.optimizer.onPhaseExploration(s.sctx, group)
		c.Assert(err, IsNil)
		group.BuildKeyInfo()
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Result = ToString(group)
		})
		c.Assert(ToString(group), DeepEquals, output[i].Result)
	}
}
