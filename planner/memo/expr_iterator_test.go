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

package memo

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	. "github.com/whtcorpsinc/check"
)

func (s *testMemoSuite) TestNewExprIterFromGroupElem(c *C) {
	g0 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx, 0)), s.schemaReplicant)
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0)))

	g1 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalSelection{}.Init(s.sctx, 0)), s.schemaReplicant)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0)))
	g1.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(s.sctx, 0)))
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(s.sctx, 0)))

	expr := NewGroupExpr(plannercore.LogicalJoin{}.Init(s.sctx, 0))
	expr.Children = append(expr.Children, g0)
	expr.Children = append(expr.Children, g1)
	g2 := NewGroupWithSchema(expr, s.schemaReplicant)

	pattern := BuildPattern(OperandJoin, EngineAll, BuildPattern(OperandProjection, EngineAll), BuildPattern(OperandSelection, EngineAll))
	iter := NewExprIterFromGroupElem(g2.Equivalents.Front(), pattern)

	c.Assert(iter, NotNil)
	c.Assert(iter.Group, IsNil)
	c.Assert(iter.Element, Equals, g2.Equivalents.Front())
	c.Assert(iter.matched, Equals, true)
	c.Assert(iter.Operand, Equals, OperandJoin)
	c.Assert(len(iter.Children), Equals, 2)

	c.Assert(iter.Children[0].Group, Equals, g0)
	c.Assert(iter.Children[0].Element, Equals, g0.GetFirstElem(OperandProjection))
	c.Assert(iter.Children[0].matched, Equals, true)
	c.Assert(iter.Children[0].Operand, Equals, OperandProjection)
	c.Assert(len(iter.Children[0].Children), Equals, 0)

	c.Assert(iter.Children[1].Group, Equals, g1)
	c.Assert(iter.Children[1].Element, Equals, g1.GetFirstElem(OperandSelection))
	c.Assert(iter.Children[1].matched, Equals, true)
	c.Assert(iter.Children[1].Operand, Equals, OperandSelection)
	c.Assert(len(iter.Children[0].Children), Equals, 0)
}

func (s *testMemoSuite) TestExprIterNext(c *C) {
	g0 := NewGroupWithSchema(NewGroupExpr(
		plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewZero()}}.Init(s.sctx, 0)), s.schemaReplicant)
	g0.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 1}.Init(s.sctx, 0)))
	g0.Insert(NewGroupExpr(
		plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}}.Init(s.sctx, 0)))
	g0.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 2}.Init(s.sctx, 0)))
	g0.Insert(NewGroupExpr(
		plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewNull()}}.Init(s.sctx, 0)))

	g1 := NewGroupWithSchema(NewGroupExpr(
		plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewNull()}}.Init(s.sctx, 0)), s.schemaReplicant)
	g1.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 3}.Init(s.sctx, 0)))
	g1.Insert(NewGroupExpr(
		plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(s.sctx, 0)))
	g1.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 4}.Init(s.sctx, 0)))
	g1.Insert(NewGroupExpr(
		plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewZero()}}.Init(s.sctx, 0)))

	expr := NewGroupExpr(plannercore.LogicalJoin{}.Init(s.sctx, 0))
	expr.Children = append(expr.Children, g0)
	expr.Children = append(expr.Children, g1)
	g2 := NewGroupWithSchema(expr, s.schemaReplicant)

	pattern := BuildPattern(OperandJoin, EngineAll, BuildPattern(OperandProjection, EngineAll), BuildPattern(OperandSelection, EngineAll))
	iter := NewExprIterFromGroupElem(g2.Equivalents.Front(), pattern)
	c.Assert(iter, NotNil)

	count := 0
	for ; iter.Matched(); iter.Next() {
		count++
		c.Assert(iter.Group, IsNil)
		c.Assert(iter.matched, Equals, true)
		c.Assert(iter.Operand, Equals, OperandJoin)
		c.Assert(len(iter.Children), Equals, 2)

		c.Assert(iter.Children[0].Group, Equals, g0)
		c.Assert(iter.Children[0].matched, Equals, true)
		c.Assert(iter.Children[0].Operand, Equals, OperandProjection)
		c.Assert(len(iter.Children[0].Children), Equals, 0)

		c.Assert(iter.Children[1].Group, Equals, g1)
		c.Assert(iter.Children[1].matched, Equals, true)
		c.Assert(iter.Children[1].Operand, Equals, OperandSelection)
		c.Assert(len(iter.Children[1].Children), Equals, 0)
	}

	c.Assert(count, Equals, 9)
}

func (s *testMemoSuite) TestExprIterReset(c *C) {
	g0 := NewGroupWithSchema(NewGroupExpr(
		plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewZero()}}.Init(s.sctx, 0)), s.schemaReplicant)
	g0.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 1}.Init(s.sctx, 0)))
	g0.Insert(NewGroupExpr(
		plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}}.Init(s.sctx, 0)))
	g0.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 2}.Init(s.sctx, 0)))
	g0.Insert(NewGroupExpr(
		plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewNull()}}.Init(s.sctx, 0)))

	sel1 := NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewNull()}}.Init(s.sctx, 0))
	sel2 := NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(s.sctx, 0))
	sel3 := NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewZero()}}.Init(s.sctx, 0))
	g1 := NewGroupWithSchema(sel1, s.schemaReplicant)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 3}.Init(s.sctx, 0)))
	g1.Insert(sel2)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 4}.Init(s.sctx, 0)))
	g1.Insert(sel3)

	g2 := NewGroupWithSchema(NewGroupExpr(
		plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewNull()}}.Init(s.sctx, 0)), s.schemaReplicant)
	g2.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 3}.Init(s.sctx, 0)))
	g2.Insert(NewGroupExpr(
		plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(s.sctx, 0)))
	g2.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 4}.Init(s.sctx, 0)))
	g2.Insert(NewGroupExpr(
		plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewZero()}}.Init(s.sctx, 0)))

	// link join with Group 0 and 1
	expr := NewGroupExpr(plannercore.LogicalJoin{}.Init(s.sctx, 0))
	expr.Children = append(expr.Children, g0)
	expr.Children = append(expr.Children, g1)
	g3 := NewGroupWithSchema(expr, s.schemaReplicant)

	// link sel 1~3 with Group 2
	sel1.Children = append(sel1.Children, g2)
	sel2.Children = append(sel2.Children, g2)
	sel3.Children = append(sel3.Children, g2)

	// create a pattern: join(proj, sel(limit))
	lhsPattern := BuildPattern(OperandProjection, EngineAll)
	rhsPattern := BuildPattern(OperandSelection, EngineAll, BuildPattern(OperandLimit, EngineAll))
	pattern := BuildPattern(OperandJoin, EngineAll, lhsPattern, rhsPattern)

	// create expression iterator for the pattern on join
	iter := NewExprIterFromGroupElem(g3.Equivalents.Front(), pattern)
	c.Assert(iter, NotNil)

	count := 0
	for ; iter.Matched(); iter.Next() {
		count++
		c.Assert(iter.Group, IsNil)
		c.Assert(iter.matched, Equals, true)
		c.Assert(iter.Operand, Equals, OperandJoin)
		c.Assert(len(iter.Children), Equals, 2)

		c.Assert(iter.Children[0].Group, Equals, g0)
		c.Assert(iter.Children[0].matched, Equals, true)
		c.Assert(iter.Children[0].Operand, Equals, OperandProjection)
		c.Assert(len(iter.Children[0].Children), Equals, 0)

		c.Assert(iter.Children[1].Group, Equals, g1)
		c.Assert(iter.Children[1].matched, Equals, true)
		c.Assert(iter.Children[1].Operand, Equals, OperandSelection)
		c.Assert(len(iter.Children[1].Children), Equals, 1)

		c.Assert(iter.Children[1].Children[0].Group, Equals, g2)
		c.Assert(iter.Children[1].Children[0].matched, Equals, true)
		c.Assert(iter.Children[1].Children[0].Operand, Equals, OperandLimit)
		c.Assert(len(iter.Children[1].Children[0].Children), Equals, 0)
	}

	c.Assert(count, Equals, 18)
}

func countMatchedIter(group *Group, pattern *Pattern) int {
	count := 0
	for elem := group.Equivalents.Front(); elem != nil; elem = elem.Next() {
		iter := NewExprIterFromGroupElem(elem, pattern)
		if iter == nil {
			continue
		}
		for ; iter.Matched(); iter.Next() {
			count++
		}
	}
	return count
}

func (s *testMemoSuite) TestExprIterWithEngineType(c *C) {
	g1 := NewGroupWithSchema(NewGroupExpr(
		plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(s.sctx, 0)), s.schemaReplicant).SetEngineType(EngineTiFlash)
	g1.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 1}.Init(s.sctx, 0)))
	g1.Insert(NewGroupExpr(
		plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}}.Init(s.sctx, 0)))
	g1.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 2}.Init(s.sctx, 0)))

	g2 := NewGroupWithSchema(NewGroupExpr(
		plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(s.sctx, 0)), s.schemaReplicant).SetEngineType(EngineEinsteinDB)
	g2.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 2}.Init(s.sctx, 0)))
	g2.Insert(NewGroupExpr(
		plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}}.Init(s.sctx, 0)))
	g2.Insert(NewGroupExpr(
		plannercore.LogicalLimit{Count: 3}.Init(s.sctx, 0)))

	flashGather := NewGroupExpr(
		plannercore.EinsteinDBSingleGather{}.Init(s.sctx, 0))
	flashGather.Children = append(flashGather.Children, g1)
	g3 := NewGroupWithSchema(flashGather, s.schemaReplicant).SetEngineType(EngineMilevaDB)

	einsteindbGather := NewGroupExpr(
		plannercore.EinsteinDBSingleGather{}.Init(s.sctx, 0))
	einsteindbGather.Children = append(einsteindbGather.Children, g2)
	g3.Insert(einsteindbGather)

	join := NewGroupExpr(
		plannercore.LogicalJoin{}.Init(s.sctx, 0))
	join.Children = append(join.Children, g3, g3)
	g4 := NewGroupWithSchema(join, s.schemaReplicant).SetEngineType(EngineMilevaDB)

	// The Groups look like this:
	// Group 4
	//     Join input:[Group3, Group3]
	// Group 3
	//     EinsteinDBSingleGather input:[Group2] EngineEinsteinDB
	//     EinsteinDBSingleGather input:[Group1] EngineTiFlash
	// Group 2
	//     Selection
	//     Projection
	//     Limit
	//     Limit
	// Group 1
	//     Selection
	//     Projection
	//     Limit
	//     Limit

	p0 := BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandLimit, EngineEinsteinDBOnly))
	c.Assert(countMatchedIter(g3, p0), Equals, 2)
	p1 := BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandLimit, EngineTiFlashOnly))
	c.Assert(countMatchedIter(g3, p1), Equals, 2)
	p2 := BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandLimit, EngineEinsteinDBOrTiFlash))
	c.Assert(countMatchedIter(g3, p2), Equals, 4)
	p3 := BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandSelection, EngineTiFlashOnly))
	c.Assert(countMatchedIter(g3, p3), Equals, 1)
	p4 := BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandProjection, EngineEinsteinDBOnly))
	c.Assert(countMatchedIter(g3, p4), Equals, 1)

	p5 := BuildPattern(
		OperandJoin,
		EngineMilevaDBOnly,
		BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandLimit, EngineEinsteinDBOnly)),
		BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandLimit, EngineEinsteinDBOnly)),
	)
	c.Assert(countMatchedIter(g4, p5), Equals, 4)
	p6 := BuildPattern(
		OperandJoin,
		EngineMilevaDBOnly,
		BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandLimit, EngineTiFlashOnly)),
		BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandLimit, EngineEinsteinDBOnly)),
	)
	c.Assert(countMatchedIter(g4, p6), Equals, 4)
	p7 := BuildPattern(
		OperandJoin,
		EngineMilevaDBOnly,
		BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandLimit, EngineEinsteinDBOrTiFlash)),
		BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly, BuildPattern(OperandLimit, EngineEinsteinDBOrTiFlash)),
	)
	c.Assert(countMatchedIter(g4, p7), Equals, 16)

	// This is not a test case for EngineType. This case is to test
	// the Pattern without a leaf AnyOperand. It is more efficient to
	// test it here.
	p8 := BuildPattern(
		OperandJoin,
		EngineMilevaDBOnly,
		BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly),
		BuildPattern(OperandEinsteinDBSingleGather, EngineMilevaDBOnly),
	)
	c.Assert(countMatchedIter(g4, p8), Equals, 4)
}
