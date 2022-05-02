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

var _ = Suite(&testTransformationMemruleSuite{})

type testTransformationMemruleSuite struct {
	*berolinaAllegroSQL.berolinaAllegroSQL
	is        schemareplicant.SchemaReplicant
	sctx      stochastikctx.Context
	testData  solitonutil.TestData
	optimizer *Optimizer
}

func (s *testTransformationMemruleSuite) SetUpSuite(c *C) {
	s.is = schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{plannercore.MockSignedBlock()})
	s.sctx = plannercore.MockContext()
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	s.optimizer = NewOptimizer()
	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "transformation_rules_suite")
	c.Assert(err, IsNil)
	s.berolinaAllegroSQL.EnableWindowFunc(true)
}

func (s *testTransformationMemruleSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func testGroupToString(input []string, output []struct {
	ALLEGROALLEGROSQL string
	Result            []string
}, s *testTransformationMemruleSuite, c *C) {
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
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Result = ToString(group)
		})
		c.Assert(ToString(group), DeepEquals, output[i].Result)
	}
}

func (s *testTransformationMemruleSuite) TestAggPushDownGather(c *C) {
	s.optimizer.ResetTransformationMemrules(TransformationMemruleBatch{
		memo.OperanPosetDaggregation: {
			NewMemrulePushAggDownGather(),
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
		// BuildKeyInfo here to test the KeyInfo for partialAgg.
		group.BuildKeyInfo()
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Result = ToString(group)
		})
		c.Assert(ToString(group), DeepEquals, output[i].Result)
	}
}

func (s *testTransformationMemruleSuite) TestPredicatePushDown(c *C) {
	s.optimizer.ResetTransformationMemrules(
		TransformationMemruleBatch{ // MilevaDB layer
			memo.OperandSelection: {
				NewMemrulePushSelDownSort(),
				NewMemrulePushSelDownProjection(),
				NewMemrulePushSelDownAggregation(),
				NewMemrulePushSelDownJoin(),
				NewMemrulePushSelDownUnionAll(),
				NewMemrulePushSelDownWindow(),
				NewMemruleMergeAdjacentSelection(),
			},
		},
		TransformationMemruleBatch{ // EinsteinDB layer
			memo.OperandSelection: {
				NewMemrulePushSelDownBlockScan(),
				NewMemrulePushSelDownEinsteinDBSingleGather(),
				NewMemrulePushSelDownIndexScan(),
			},
			memo.OperandDataSource: {
				NewMemruleEnumeratePaths(),
			},
		},
	)
	defer func() {
		s.optimizer.ResetTransformationMemrules(DefaultMemruleBatches...)
	}()
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestTopNMemrules(c *C) {
	s.optimizer.ResetTransformationMemrules(
		TransformationMemruleBatch{ // MilevaDB layer
			memo.OperandLimit: {
				NewMemruleTransformLimitToTopN(),
				NewMemrulePushLimitDownProjection(),
				NewMemrulePushLimitDownUnionAll(),
				NewMemrulePushLimitDownOuterJoin(),
				NewMemruleMergeAdjacentLimit(),
			},
			memo.OperandTopN: {
				NewMemrulePushTopNDownProjection(),
				NewMemrulePushTopNDownOuterJoin(),
				NewMemrulePushTopNDownUnionAll(),
			},
		},
		TransformationMemruleBatch{ // EinsteinDB layer
			memo.OperandLimit: {
				NewMemrulePushLimitDownEinsteinDBSingleGather(),
			},
			memo.OperandTopN: {
				NewMemrulePushTopNDownEinsteinDBSingleGather(),
			},
			memo.OperandDataSource: {
				NewMemruleEnumeratePaths(),
			},
		},
	)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestProjectionElimination(c *C) {
	s.optimizer.ResetTransformationMemrules(TransformationMemruleBatch{
		memo.OperandProjection: {
			NewMemruleEliminateProjection(),
			NewMemruleMergeAdjacentProjection(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestEliminateMaxMin(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperanPosetDaggregation: {
			NewMemruleEliminateSingleMaxMin(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestMergeAggregationProjection(c *C) {
	s.optimizer.ResetTransformationMemrules(TransformationMemruleBatch{
		memo.OperanPosetDaggregation: {
			NewMemruleMergeAggregationProjection(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestMergeAdjacentTopN(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperandLimit: {
			NewMemruleTransformLimitToTopN(),
		},
		memo.OperandTopN: {
			NewMemrulePushTopNDownProjection(),
			NewMemruleMergeAdjacentTopN(),
		},
		memo.OperandProjection: {
			NewMemruleMergeAdjacentProjection(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestMergeAdjacentLimit(c *C) {
	s.optimizer.ResetTransformationMemrules(TransformationMemruleBatch{
		memo.OperandLimit: {
			NewMemrulePushLimitDownProjection(),
			NewMemruleMergeAdjacentLimit(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestTransformLimitToBlockDual(c *C) {
	s.optimizer.ResetTransformationMemrules(TransformationMemruleBatch{
		memo.OperandLimit: {
			NewMemruleTransformLimitToBlockDual(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestPostTransformationMemrules(c *C) {
	s.optimizer.ResetTransformationMemrules(TransformationMemruleBatch{
		memo.OperandLimit: {
			NewMemruleTransformLimitToTopN(),
		},
	}, PostTransformationBatch)
	defer func() {
		s.optimizer.ResetTransformationMemrules(DefaultMemruleBatches...)
	}()
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestPushLimitDownEinsteinDBSingleGather(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperandLimit: {
			NewMemrulePushLimitDownEinsteinDBSingleGather(),
		},
		memo.OperandProjection: {
			NewMemruleEliminateProjection(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestEliminateOuterJoin(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperanPosetDaggregation: {
			NewMemruleEliminateOuterJoinBelowAggregation(),
		},
		memo.OperandProjection: {
			NewMemruleEliminateOuterJoinBelowProjection(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestTransformAggregateCaseToSelection(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperanPosetDaggregation: {
			NewMemruleTransformAggregateCaseToSelection(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestTransformAggToProj(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperanPosetDaggregation: {
			NewMemruleTransformAggToProj(),
		},
		memo.OperandProjection: {
			NewMemruleMergeAdjacentProjection(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestDecorrelate(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperandApply: {
			NewMemrulePullSelectionUpApply(),
			NewMemruleTransformApplyToJoin(),
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
	testGroupToString(input, output, s, c)
}

func (s *testTransformationMemruleSuite) TestInjectProj(c *C) {
	s.optimizer.ResetTransformationMemrules(map[memo.Operand][]Transformation{
		memo.OperandLimit: {
			NewMemruleTransformLimitToTopN(),
		},
	}, map[memo.Operand][]Transformation{
		memo.OperanPosetDaggregation: {
			NewMemruleInjectProjectionBelowAgg(),
		},
		memo.OperandTopN: {
			NewMemruleInjectProjectionBelowTopN(),
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
	testGroupToString(input, output, s, c)
}
