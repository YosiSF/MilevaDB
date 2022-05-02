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

package core

import (
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
)

type buildKeySolver struct{}

func (s *buildKeySolver) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	buildKeyInfo(lp)
	return lp, nil
}

// buildKeyInfo recursively calls LogicalPlan's BuildKeyInfo method.
func buildKeyInfo(lp LogicalPlan) {
	for _, child := range lp.Children() {
		buildKeyInfo(child)
	}
	childSchema := make([]*expression.Schema, len(lp.Children()))
	for i, child := range lp.Children() {
		childSchema[i] = child.Schema()
	}
	lp.BuildKeyInfo(lp.Schema(), childSchema)
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (la *LogicalAggregation) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	if la.IsPartialModeAgg() {
		return
	}
	la.logicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	for _, key := range childSchema[0].Keys {
		indices := selfSchema.DeferredCausetsIndices(key)
		if indices == nil {
			continue
		}
		newKey := make([]*expression.DeferredCauset, 0, len(key))
		for _, i := range indices {
			newKey = append(newKey, selfSchema.DeferredCausets[i])
		}
		selfSchema.Keys = append(selfSchema.Keys, newKey)
	}
	if len(la.groupByDefCauss) == len(la.GroupByItems) && len(la.GroupByItems) > 0 {
		indices := selfSchema.DeferredCausetsIndices(la.groupByDefCauss)
		if indices != nil {
			newKey := make([]*expression.DeferredCauset, 0, len(indices))
			for _, i := range indices {
				newKey = append(newKey, selfSchema.DeferredCausets[i])
			}
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
	if len(la.GroupByItems) == 0 {
		la.maxOneRow = true
	}
}

// If a condition is the form of (uniqueKey = constant) or (uniqueKey = Correlated column), it returns at most one event.
// This function will check it.
func (p *LogicalSelection) checkMaxOneRowCond(unique expression.Expression, constOrCorDefCaus expression.Expression, childSchema *expression.Schema) bool {
	col, ok := unique.(*expression.DeferredCauset)
	if !ok {
		return false
	}
	if !childSchema.IsUniqueKey(col) {
		return false
	}
	_, okCon := constOrCorDefCaus.(*expression.CouplingConstantWithRadix)
	if okCon {
		return true
	}
	_, okCorDefCaus := constOrCorDefCaus.(*expression.CorrelatedDeferredCauset)
	return okCorDefCaus
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalSelection) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	for _, cond := range p.Conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.EQ {
			if p.checkMaxOneRowCond(sf.GetArgs()[0], sf.GetArgs()[1], childSchema[0]) || p.checkMaxOneRowCond(sf.GetArgs()[1], sf.GetArgs()[0], childSchema[0]) {
				p.maxOneRow = true
				break
			}
		}
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalLimit) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	if p.Count == 1 {
		p.maxOneRow = true
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalTopN) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	if p.Count == 1 {
		p.maxOneRow = true
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalBlockDual) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.baseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	if p.RowCount == 1 {
		p.maxOneRow = true
	}
}

// A bijection exists between columns of a projection's schemaReplicant and this projection's Exprs.
// Sometimes we need a schemaReplicant made by expr of Exprs to convert a column in child's schemaReplicant to a column in this projection's Schema.
func (p *LogicalProjection) buildSchemaByExprs(selfSchema *expression.Schema) *expression.Schema {
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, selfSchema.Len())...)
	for _, expr := range p.Exprs {
		if col, isDefCaus := expr.(*expression.DeferredCauset); isDefCaus {
			schemaReplicant.Append(col)
		} else {
			// If the expression is not a column, we add a column to occupy the position.
			schemaReplicant.Append(&expression.DeferredCauset{
				UniqueID: p.ctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
				RetType:  expr.GetType(),
			})
		}
	}
	return schemaReplicant
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalProjection) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.logicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	schemaReplicant := p.buildSchemaByExprs(selfSchema)
	for _, key := range childSchema[0].Keys {
		indices := schemaReplicant.DeferredCausetsIndices(key)
		if indices == nil {
			continue
		}
		newKey := make([]*expression.DeferredCauset, 0, len(key))
		for _, i := range indices {
			newKey = append(newKey, selfSchema.DeferredCausets[i])
		}
		selfSchema.Keys = append(selfSchema.Keys, newKey)
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (p *LogicalJoin) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.logicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	switch p.JoinType {
	case SemiJoin, LeftOuterSemiJoin, AntiSemiJoin, AntiLeftOuterSemiJoin:
		selfSchema.Keys = childSchema[0].Clone().Keys
	case InnerJoin, LeftOuterJoin, RightOuterJoin:
		// If there is no equal conditions, then cartesian product can't be prevented and unique key information will destroy.
		if len(p.EqualConditions) == 0 {
			return
		}
		lOk := false
		rOk := false
		// Such as 'select * from t1 join t2 where t1.a = t2.a and t1.b = t2.b'.
		// If one sides (a, b) is a unique key, then the unique key information is remained.
		// But we don't consider this situation currently.
		// Only key made by one column is considered now.
		for _, expr := range p.EqualConditions {
			ln := expr.GetArgs()[0].(*expression.DeferredCauset)
			rn := expr.GetArgs()[1].(*expression.DeferredCauset)
			for _, key := range childSchema[0].Keys {
				if len(key) == 1 && key[0].Equal(p.ctx, ln) {
					lOk = true
					break
				}
			}
			for _, key := range childSchema[1].Keys {
				if len(key) == 1 && key[0].Equal(p.ctx, rn) {
					rOk = true
					break
				}
			}
		}
		// For inner join, if one side of one equal condition is unique key,
		// another side's unique key information will all be reserved.
		// If it's an outer join, NULL value will fill some position, which will destroy the unique key information.
		if lOk && p.JoinType != LeftOuterJoin {
			selfSchema.Keys = append(selfSchema.Keys, childSchema[1].Keys...)
		}
		if rOk && p.JoinType != RightOuterJoin {
			selfSchema.Keys = append(selfSchema.Keys, childSchema[0].Keys...)
		}
	}
}

// checHoTTexCanBeKey checks whether an Index can be a Key in schemaReplicant.
func checHoTTexCanBeKey(idx *perceptron.IndexInfo, columns []*perceptron.DeferredCausetInfo, schemaReplicant *expression.Schema) expression.KeyInfo {
	if !idx.Unique {
		return nil
	}
	newKey := make([]*expression.DeferredCauset, 0, len(idx.DeferredCausets))
	ok := true
	for _, idxDefCaus := range idx.DeferredCausets {
		// The columns of this index should all occur in column schemaReplicant.
		// Since null value could be duplicate in unique key. So we check NotNull flag of every column.
		find := false
		for i, col := range columns {
			if idxDefCaus.Name.L == col.Name.L {
				if !allegrosql.HasNotNullFlag(col.Flag) {
					break
				}
				newKey = append(newKey, schemaReplicant.DeferredCausets[i])
				find = true
				break
			}
		}
		if !find {
			ok = false
			break
		}
	}
	if ok {
		return newKey
	}
	return nil
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (ds *DataSource) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	for _, path := range ds.possibleAccessPaths {
		if path.IsIntHandlePath {
			continue
		}
		if newKey := checHoTTexCanBeKey(path.Index, ds.DeferredCausets, selfSchema); newKey != nil {
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
	if ds.blockInfo.PKIsHandle {
		for i, col := range ds.DeferredCausets {
			if allegrosql.HasPriKeyFlag(col.Flag) {
				selfSchema.Keys = append(selfSchema.Keys, []*expression.DeferredCauset{selfSchema.DeferredCausets[i]})
				break
			}
		}
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (ts *LogicalBlockScan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	ts.Source.BuildKeyInfo(selfSchema, childSchema)
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (is *LogicalIndexScan) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = nil
	for _, path := range is.Source.possibleAccessPaths {
		if path.IsBlockPath() {
			continue
		}
		if newKey := checHoTTexCanBeKey(path.Index, is.DeferredCausets, selfSchema); newKey != nil {
			selfSchema.Keys = append(selfSchema.Keys, newKey)
		}
	}
	handle := is.getPKIsHandleDefCaus(selfSchema)
	if handle != nil {
		selfSchema.Keys = append(selfSchema.Keys, []*expression.DeferredCauset{handle})
	}
}

// BuildKeyInfo implements LogicalPlan BuildKeyInfo interface.
func (tg *EinsteinDBSingleGather) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	selfSchema.Keys = childSchema[0].Keys
}

func (*buildKeySolver) name() string {
	return "build_keys"
}
