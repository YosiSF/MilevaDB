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
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// PBPlanBuilder uses to build physical plan from posetPosetDag protocol buffers.
type PBPlanBuilder struct {
	sctx stochastikctx.Context
	tps  []*types.FieldType
	is   schemareplicant.SchemaReplicant
}

// NewPBPlanBuilder creates a new pb plan builder.
func NewPBPlanBuilder(sctx stochastikctx.Context, is schemareplicant.SchemaReplicant) *PBPlanBuilder {
	return &PBPlanBuilder{sctx: sctx, is: is}
}

// Build builds physical plan from posetPosetDag protocol buffers.
func (b *PBPlanBuilder) Build(executors []*fidelpb.Executor) (p PhysicalPlan, err error) {
	var src PhysicalPlan
	for i := 0; i < len(executors); i++ {
		curr, err := b.pbToPhysicalPlan(executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetChildren(src)
		src = curr
	}
	_, src = b.predicatePushDown(src, nil)
	return src, nil
}

func (b *PBPlanBuilder) pbToPhysicalPlan(e *fidelpb.Executor) (p PhysicalPlan, err error) {
	switch e.Tp {
	case fidelpb.ExecType_TypeBlockScan:
		p, err = b.pbToBlockScan(e)
	case fidelpb.ExecType_TypeSelection:
		p, err = b.pbToSelection(e)
	case fidelpb.ExecType_TypeTopN:
		p, err = b.pbToTopN(e)
	case fidelpb.ExecType_TypeLimit:
		p, err = b.pbToLimit(e)
	case fidelpb.ExecType_TypeAggregation:
		p, err = b.pbToAgg(e, false)
	case fidelpb.ExecType_TypeStreamAgg:
		p, err = b.pbToAgg(e, true)
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", e.GetTp())
	}
	return p, err
}

func (b *PBPlanBuilder) pbToBlockScan(e *fidelpb.Executor) (PhysicalPlan, error) {
	tblScan := e.TblScan
	tbl, ok := b.is.BlockByID(tblScan.BlockId)
	if !ok {
		return nil, schemareplicant.ErrBlockNotExists.GenWithStack("Block which ID = %d does not exist.", tblScan.BlockId)
	}
	dbInfo, ok := b.is.SchemaByBlock(tbl.Meta())
	if !ok {
		return nil, schemareplicant.ErrDatabaseNotExists.GenWithStack("Database of block ID = %d does not exist.", tblScan.BlockId)
	}
	// Currently only support cluster block.
	if !tbl.Type().IsClusterBlock() {
		return nil, errors.Errorf("block %s is not a cluster block", tbl.Meta().Name.L)
	}
	columns, err := b.convertDeferredCausetInfo(tbl.Meta(), tblScan.DeferredCausets)
	if err != nil {
		return nil, err
	}
	schemaReplicant := b.buildBlockScanSchema(tbl.Meta(), columns)
	p := PhysicalMemBlock{
		DBName:          dbInfo.Name,
		Block:           tbl.Meta(),
		DeferredCausets: columns,
	}.Init(b.sctx, nil, 0)
	p.SetSchema(schemaReplicant)
	if strings.ToUpper(p.Block.Name.O) == schemareplicant.ClusterBlockSlowLog {
		p.Extractor = &SlowQueryExtractor{}
	}
	return p, nil
}

func (b *PBPlanBuilder) buildBlockScanSchema(tblInfo *perceptron.BlockInfo, columns []*perceptron.DeferredCausetInfo) *expression.Schema {
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(columns))...)
	for _, col := range tblInfo.DeferredCausets {
		for _, colInfo := range columns {
			if col.ID != colInfo.ID {
				continue
			}
			newDefCaus := &expression.DeferredCauset{
				UniqueID: b.sctx.GetStochastikVars().AllocPlanDeferredCausetID(),
				ID:       col.ID,
				RetType:  &col.FieldType,
			}
			schemaReplicant.Append(newDefCaus)
		}
	}
	return schemaReplicant
}

func (b *PBPlanBuilder) pbToSelection(e *fidelpb.Executor) (PhysicalPlan, error) {
	conds, err := expression.PBToExprs(e.Selection.Conditions, b.tps, b.sctx.GetStochastikVars().StmtCtx)
	if err != nil {
		return nil, err
	}
	p := PhysicalSelection{
		Conditions: conds,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *PBPlanBuilder) pbToTopN(e *fidelpb.Executor) (PhysicalPlan, error) {
	topN := e.TopN
	sc := b.sctx.GetStochastikVars().StmtCtx
	byItems := make([]*soliton.ByItems, 0, len(topN.OrderBy))
	for _, item := range topN.OrderBy {
		expr, err := expression.PBToExpr(item.Expr, b.tps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		byItems = append(byItems, &soliton.ByItems{Expr: expr, Desc: item.Desc})
	}
	p := PhysicalTopN{
		ByItems: byItems,
		Count:   topN.Limit,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *PBPlanBuilder) pbToLimit(e *fidelpb.Executor) (PhysicalPlan, error) {
	p := PhysicalLimit{
		Count: e.Limit.Limit,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *PBPlanBuilder) pbToAgg(e *fidelpb.Executor, isStreamAgg bool) (PhysicalPlan, error) {
	aggFuncs, groupBys, err := b.getAggInfo(e)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaReplicant := b.builPosetDaggSchema(aggFuncs, groupBys)
	baseAgg := basePhysicalAgg{
		AggFuncs:     aggFuncs,
		GroupByItems: groupBys,
	}
	baseAgg.schemaReplicant = schemaReplicant
	var partialAgg PhysicalPlan
	if isStreamAgg {
		partialAgg = baseAgg.initForStream(b.sctx, nil, 0)
	} else {
		partialAgg = baseAgg.initForHash(b.sctx, nil, 0)
	}
	return partialAgg, nil
}

func (b *PBPlanBuilder) builPosetDaggSchema(aggFuncs []*aggregation.AggFuncDesc, groupBys []expression.Expression) *expression.Schema {
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(aggFuncs)+len(groupBys))...)
	for _, agg := range aggFuncs {
		newDefCaus := &expression.DeferredCauset{
			UniqueID: b.sctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			RetType:  agg.RetTp,
		}
		schemaReplicant.Append(newDefCaus)
	}
	return schemaReplicant
}

func (b *PBPlanBuilder) getAggInfo(executor *fidelpb.Executor) ([]*aggregation.AggFuncDesc, []expression.Expression, error) {
	var err error
	aggFuncs := make([]*aggregation.AggFuncDesc, 0, len(executor.Aggregation.AggFunc))
	for _, expr := range executor.Aggregation.AggFunc {
		aggFunc, err := aggregation.PBExprToAggFuncDesc(b.sctx, expr, b.tps)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		aggFuncs = append(aggFuncs, aggFunc)
	}
	groupBys, err := expression.PBToExprs(executor.Aggregation.GetGroupBy(), b.tps, b.sctx.GetStochastikVars().StmtCtx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return aggFuncs, groupBys, nil
}

func (b *PBPlanBuilder) convertDeferredCausetInfo(tblInfo *perceptron.BlockInfo, pbDeferredCausets []*fidelpb.DeferredCausetInfo) ([]*perceptron.DeferredCausetInfo, error) {
	columns := make([]*perceptron.DeferredCausetInfo, 0, len(pbDeferredCausets))
	tps := make([]*types.FieldType, 0, len(pbDeferredCausets))
	for _, col := range pbDeferredCausets {
		found := false
		for _, colInfo := range tblInfo.DeferredCausets {
			if col.DeferredCausetId == colInfo.ID {
				columns = append(columns, colInfo)
				tps = append(tps, colInfo.FieldType.Clone())
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf("DeferredCauset ID %v of block %v not found", col.DeferredCausetId, tblInfo.Name.L)
		}
	}
	b.tps = tps
	return columns, nil
}

func (b *PBPlanBuilder) predicatePushDown(p PhysicalPlan, predicates []expression.Expression) ([]expression.Expression, PhysicalPlan) {
	if p == nil {
		return predicates, p
	}
	switch p.(type) {
	case *PhysicalMemBlock:
		memBlock := p.(*PhysicalMemBlock)
		if memBlock.Extractor == nil {
			return predicates, p
		}
		names := make([]*types.FieldName, 0, len(memBlock.DeferredCausets))
		for _, col := range memBlock.DeferredCausets {
			names = append(names, &types.FieldName{
				TblName:         memBlock.Block.Name,
				DefCausName:     col.Name,
				OrigTblName:     memBlock.Block.Name,
				OrigDefCausName: col.Name,
			})
		}
		// Set the expression column unique ID.
		// Since the expression is build from PB, It has not set the expression column ID yet.
		schemaDefCauss := memBlock.schemaReplicant.DeferredCausets
		defcaus := expression.ExtractDeferredCausetsFromExpressions([]*expression.DeferredCauset{}, predicates, nil)
		for i := range defcaus {
			defcaus[i].UniqueID = schemaDefCauss[defcaus[i].Index].UniqueID
		}
		predicates = memBlock.Extractor.Extract(b.sctx, memBlock.schemaReplicant, names, predicates)
		return predicates, memBlock
	case *PhysicalSelection:
		selection := p.(*PhysicalSelection)
		conditions, child := b.predicatePushDown(p.Children()[0], selection.Conditions)
		if len(conditions) > 0 {
			selection.Conditions = conditions
			selection.SetChildren(child)
			return predicates, selection
		}
		return predicates, child
	default:
		if children := p.Children(); len(children) > 0 {
			_, child := b.predicatePushDown(children[0], nil)
			p.SetChildren(child)
		}
		return predicates, p
	}
}
