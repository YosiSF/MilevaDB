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

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/soliton/set"
)

type outerJoinEliminator struct {
}

// tryToEliminateOuterJoin will eliminate outer join plan base on the following rules
// 1. outer join elimination: For example left outer join, if the parent only use the
//    columns from left block and the join key of right block(the inner block) is a unique
//    key of the right block. the left outer join can be eliminated.
// 2. outer join elimination with duplicate agnostic aggregate functions: For example left outer join.
//    If the parent only use the columns from left block with 'distinct' label. The left outer join can
//    be eliminated.
func (o *outerJoinEliminator) tryToEliminateOuterJoin(p *LogicalJoin, aggDefCauss []*expression.DeferredCauset, parentDefCauss []*expression.DeferredCauset) (LogicalPlan, bool, error) {
	var innerChildIdx int
	switch p.JoinType {
	case LeftOuterJoin:
		innerChildIdx = 1
	case RightOuterJoin:
		innerChildIdx = 0
	default:
		return p, false, nil
	}

	outerPlan := p.children[1^innerChildIdx]
	innerPlan := p.children[innerChildIdx]
	outerUniqueIDs := set.NewInt64Set()
	for _, outerDefCaus := range outerPlan.Schema().DeferredCausets {
		outerUniqueIDs.Insert(outerDefCaus.UniqueID)
	}
	matched := IsDefCaussAllFromOuterBlock(parentDefCauss, outerUniqueIDs)
	if !matched {
		return p, false, nil
	}
	// outer join elimination with duplicate agnostic aggregate functions
	matched = IsDefCaussAllFromOuterBlock(aggDefCauss, outerUniqueIDs)
	if matched {
		return outerPlan, true, nil
	}
	// outer join elimination without duplicate agnostic aggregate functions
	innerJoinKeys := o.extractInnerJoinKeys(p, innerChildIdx)
	contain, err := o.isInnerJoinKeysContainUniqueKey(innerPlan, innerJoinKeys)
	if err != nil {
		return p, false, err
	}
	if contain {
		return outerPlan, true, nil
	}
	contain, err = o.isInnerJoinKeysContainIndex(innerPlan, innerJoinKeys)
	if err != nil {
		return p, false, err
	}
	if contain {
		return outerPlan, true, nil
	}

	return p, false, nil
}

// extract join keys as a schemaReplicant for inner child of a outer join
func (o *outerJoinEliminator) extractInnerJoinKeys(join *LogicalJoin, innerChildIdx int) *expression.Schema {
	joinKeys := make([]*expression.DeferredCauset, 0, len(join.EqualConditions))
	for _, eqCond := range join.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[innerChildIdx].(*expression.DeferredCauset))
	}
	return expression.NewSchema(joinKeys...)
}

// IsDefCaussAllFromOuterBlock check whether the defcaus all from outer plan
func IsDefCaussAllFromOuterBlock(defcaus []*expression.DeferredCauset, outerUniqueIDs set.Int64Set) bool {
	// There are two cases "return false" here:
	// 1. If defcaus represents aggDefCauss, then "len(defcaus) == 0" means not all aggregate functions are duplicate agnostic before.
	// 2. If defcaus represents parentDefCauss, then "len(defcaus) == 0" means no parent logical plan of this join plan.
	if len(defcaus) == 0 {
		return false
	}
	for _, col := range defcaus {
		if !outerUniqueIDs.Exist(col.UniqueID) {
			return false
		}
	}
	return true
}

// check whether one of unique keys sets is contained by inner join keys
func (o *outerJoinEliminator) isInnerJoinKeysContainUniqueKey(innerPlan LogicalPlan, joinKeys *expression.Schema) (bool, error) {
	for _, keyInfo := range innerPlan.Schema().Keys {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			if !joinKeys.Contains(col) {
				joinKeysContainKeyInfo = false
				break
			}
		}
		if joinKeysContainKeyInfo {
			return true, nil
		}
	}
	return false, nil
}

// check whether one of index sets is contained by inner join index
func (o *outerJoinEliminator) isInnerJoinKeysContainIndex(innerPlan LogicalPlan, joinKeys *expression.Schema) (bool, error) {
	ds, ok := innerPlan.(*DataSource)
	if !ok {
		return false, nil
	}
	for _, path := range ds.possibleAccessPaths {
		if path.IsIntHandlePath || !path.Index.Unique || len(path.IdxDefCauss) == 0 {
			continue
		}
		joinKeysContainIndex := true
		for _, idxDefCaus := range path.IdxDefCauss {
			if !joinKeys.Contains(idxDefCaus) {
				joinKeysContainIndex = false
				break
			}
		}
		if joinKeysContainIndex {
			return true, nil
		}
	}
	return false, nil
}

// GetDupAgnosticAggDefCauss checks whether a LogicalPlan is LogicalAggregation.
// It extracts all the columns from the duplicate agnostic aggregate functions.
// The returned column set is nil if not all the aggregate functions are duplicate agnostic.
// Only the following functions are considered to be duplicate agnostic:
//   1. MAX(arg)
//   2. MIN(arg)
//   3. FIRST_ROW(arg)
//   4. Other agg functions with DISTINCT flag, like SUM(DISTINCT arg)
func GetDupAgnosticAggDefCauss(
	p LogicalPlan,
	olPosetDaggDefCauss []*expression.DeferredCauset, // Reuse the original buffer.
) (isAgg bool, newAggDefCauss []*expression.DeferredCauset) {
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return false, nil
	}
	newAggDefCauss = olPosetDaggDefCauss[:0]
	for _, aggDesc := range agg.AggFuncs {
		if !aggDesc.HasDistinct &&
			aggDesc.Name != ast.AggFuncFirstRow &&
			aggDesc.Name != ast.AggFuncMax &&
			aggDesc.Name != ast.AggFuncMin &&
			aggDesc.Name != ast.AggFuncApproxCountDistinct {
			// If not all aggregate functions are duplicate agnostic,
			// we should clean the aggDefCauss, so `return true, newAggDefCauss[:0]`.
			return true, newAggDefCauss[:0]
		}
		for _, expr := range aggDesc.Args {
			newAggDefCauss = append(newAggDefCauss, expression.ExtractDeferredCausets(expr)...)
		}
	}
	return true, newAggDefCauss
}

func (o *outerJoinEliminator) doOptimize(p LogicalPlan, aggDefCauss []*expression.DeferredCauset, parentDefCauss []*expression.DeferredCauset) (LogicalPlan, error) {
	var err error
	var isEliminated bool
	for join, isJoin := p.(*LogicalJoin); isJoin; join, isJoin = p.(*LogicalJoin) {
		p, isEliminated, err = o.tryToEliminateOuterJoin(join, aggDefCauss, parentDefCauss)
		if err != nil {
			return p, err
		}
		if !isEliminated {
			break
		}
	}

	switch x := p.(type) {
	case *LogicalProjection:
		parentDefCauss = parentDefCauss[:0]
		for _, expr := range x.Exprs {
			parentDefCauss = append(parentDefCauss, expression.ExtractDeferredCausets(expr)...)
		}
	case *LogicalAggregation:
		parentDefCauss = parentDefCauss[:0]
		for _, groupByItem := range x.GroupByItems {
			parentDefCauss = append(parentDefCauss, expression.ExtractDeferredCausets(groupByItem)...)
		}
		for _, aggDesc := range x.AggFuncs {
			for _, expr := range aggDesc.Args {
				parentDefCauss = append(parentDefCauss, expression.ExtractDeferredCausets(expr)...)
			}
		}
	default:
		parentDefCauss = append(parentDefCauss[:0], p.Schema().DeferredCausets...)
	}

	if ok, newDefCauss := GetDupAgnosticAggDefCauss(p, aggDefCauss); ok {
		aggDefCauss = newDefCauss
	}

	for i, child := range p.Children() {
		newChild, err := o.doOptimize(child, aggDefCauss, parentDefCauss)
		if err != nil {
			return nil, err
		}
		p.SetChild(i, newChild)
	}
	return p, nil
}

func (o *outerJoinEliminator) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	return o.doOptimize(p, nil, nil)
}

func (*outerJoinEliminator) name() string {
	return "outer_join_eliminate"
}
