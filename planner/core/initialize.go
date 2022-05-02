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
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/property"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/plancodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

// Init initializes LogicalAggregation.
func (la LogicalAggregation) Init(ctx stochastikctx.Context, offset int) *LogicalAggregation {
	la.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeAgg, &la, offset)
	return &la
}

// Init initializes LogicalJoin.
func (p LogicalJoin) Init(ctx stochastikctx.Context, offset int) *LogicalJoin {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeJoin, &p, offset)
	return &p
}

// Init initializes DataSource.
func (ds DataSource) Init(ctx stochastikctx.Context, offset int) *DataSource {
	ds.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeDataSource, &ds, offset)
	return &ds
}

// Init initializes EinsteinDBSingleGather.
func (sg EinsteinDBSingleGather) Init(ctx stochastikctx.Context, offset int) *EinsteinDBSingleGather {
	sg.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeEinsteinDBSingleGather, &sg, offset)
	return &sg
}

// Init initializes LogicalBlockScan.
func (ts LogicalBlockScan) Init(ctx stochastikctx.Context, offset int) *LogicalBlockScan {
	ts.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeBlockScan, &ts, offset)
	return &ts
}

// Init initializes LogicalIndexScan.
func (is LogicalIndexScan) Init(ctx stochastikctx.Context, offset int) *LogicalIndexScan {
	is.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeIdxScan, &is, offset)
	return &is
}

// Init initializes LogicalApply.
func (la LogicalApply) Init(ctx stochastikctx.Context, offset int) *LogicalApply {
	la.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeApply, &la, offset)
	return &la
}

// Init initializes LogicalSelection.
func (p LogicalSelection) Init(ctx stochastikctx.Context, offset int) *LogicalSelection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeSel, &p, offset)
	return &p
}

// Init initializes PhysicalSelection.
func (p PhysicalSelection) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalSelection {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeSel, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalUnionScan.
func (p LogicalUnionScan) Init(ctx stochastikctx.Context, offset int) *LogicalUnionScan {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeUnionScan, &p, offset)
	return &p
}

// Init initializes LogicalProjection.
func (p LogicalProjection) Init(ctx stochastikctx.Context, offset int) *LogicalProjection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeProj, &p, offset)
	return &p
}

// Init initializes PhysicalProjection.
func (p PhysicalProjection) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalProjection {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeProj, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalUnionAll.
func (p LogicalUnionAll) Init(ctx stochastikctx.Context, offset int) *LogicalUnionAll {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeUnion, &p, offset)
	return &p
}

// Init initializes LogicalPartitionUnionAll.
func (p LogicalPartitionUnionAll) Init(ctx stochastikctx.Context, offset int) *LogicalPartitionUnionAll {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypePartitionUnion, &p, offset)
	return &p
}

// Init initializes PhysicalUnionAll.
func (p PhysicalUnionAll) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalUnionAll {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeUnion, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalSort.
func (ls LogicalSort) Init(ctx stochastikctx.Context, offset int) *LogicalSort {
	ls.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeSort, &ls, offset)
	return &ls
}

// Init initializes PhysicalSort.
func (p PhysicalSort) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalSort {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeSort, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes NominalSort.
func (p NominalSort) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *NominalSort {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeSort, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalTopN.
func (lt LogicalTopN) Init(ctx stochastikctx.Context, offset int) *LogicalTopN {
	lt.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeTopN, &lt, offset)
	return &lt
}

// Init initializes PhysicalTopN.
func (p PhysicalTopN) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalTopN {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeTopN, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalLimit.
func (p LogicalLimit) Init(ctx stochastikctx.Context, offset int) *LogicalLimit {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeLimit, &p, offset)
	return &p
}

// Init initializes PhysicalLimit.
func (p PhysicalLimit) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalLimit {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeLimit, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalBlockDual.
func (p LogicalBlockDual) Init(ctx stochastikctx.Context, offset int) *LogicalBlockDual {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeDual, &p, offset)
	return &p
}

// Init initializes PhysicalBlockDual.
func (p PhysicalBlockDual) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int) *PhysicalBlockDual {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeDual, &p, offset)
	p.stats = stats
	return &p
}

// Init initializes LogicalMaxOneRow.
func (p LogicalMaxOneRow) Init(ctx stochastikctx.Context, offset int) *LogicalMaxOneRow {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeMaxOneRow, &p, offset)
	return &p
}

// Init initializes PhysicalMaxOneRow.
func (p PhysicalMaxOneRow) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalMaxOneRow {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeMaxOneRow, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalWindow.
func (p LogicalWindow) Init(ctx stochastikctx.Context, offset int) *LogicalWindow {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeWindow, &p, offset)
	return &p
}

// Init initializes PhysicalWindow.
func (p PhysicalWindow) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalWindow {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeWindow, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalShuffle.
func (p PhysicalShuffle) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalShuffle {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeShuffle, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalShuffleDataSourceStub.
func (p PhysicalShuffleDataSourceStub) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalShuffleDataSourceStub {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeShuffleDataSourceStub, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes UFIDelate.
func (p UFIDelate) Init(ctx stochastikctx.Context) *UFIDelate {
	p.basePlan = newBasePlan(ctx, plancodec.TypeUFIDelate, 0)
	return &p
}

// Init initializes Delete.
func (p Delete) Init(ctx stochastikctx.Context) *Delete {
	p.basePlan = newBasePlan(ctx, plancodec.TypeDelete, 0)
	return &p
}

// Init initializes Insert.
func (p Insert) Init(ctx stochastikctx.Context) *Insert {
	p.basePlan = newBasePlan(ctx, plancodec.TypeInsert, 0)
	return &p
}

// Init initializes LogicalShow.
func (p LogicalShow) Init(ctx stochastikctx.Context) *LogicalShow {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeShow, &p, 0)
	return &p
}

// Init initializes LogicalShowDBSJobs.
func (p LogicalShowDBSJobs) Init(ctx stochastikctx.Context) *LogicalShowDBSJobs {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeShowDBSJobs, &p, 0)
	return &p
}

// Init initializes PhysicalShow.
func (p PhysicalShow) Init(ctx stochastikctx.Context) *PhysicalShow {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeShow, &p, 0)
	// Just use pseudo stats to avoid panic.
	p.stats = &property.StatsInfo{RowCount: 1}
	return &p
}

// Init initializes PhysicalShowDBSJobs.
func (p PhysicalShowDBSJobs) Init(ctx stochastikctx.Context) *PhysicalShowDBSJobs {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeShowDBSJobs, &p, 0)
	// Just use pseudo stats to avoid panic.
	p.stats = &property.StatsInfo{RowCount: 1}
	return &p
}

// Init initializes LogicalLock.
func (p LogicalLock) Init(ctx stochastikctx.Context) *LogicalLock {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeLock, &p, 0)
	return &p
}

// Init initializes PhysicalLock.
func (p PhysicalLock) Init(ctx stochastikctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalLock {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeLock, &p, 0)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalBlockScan.
func (p PhysicalBlockScan) Init(ctx stochastikctx.Context, offset int) *PhysicalBlockScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeBlockScan, &p, offset)
	return &p
}

// Init initializes PhysicalIndexScan.
func (p PhysicalIndexScan) Init(ctx stochastikctx.Context, offset int) *PhysicalIndexScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeIdxScan, &p, offset)
	return &p
}

// Init initializes LogicalMemBlock.
func (p LogicalMemBlock) Init(ctx stochastikctx.Context, offset int) *LogicalMemBlock {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeMemBlockScan, &p, offset)
	return &p
}

// Init initializes PhysicalMemBlock.
func (p PhysicalMemBlock) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int) *PhysicalMemBlock {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeMemBlockScan, &p, offset)
	p.stats = stats
	return &p
}

// Init initializes PhysicalHashJoin.
func (p PhysicalHashJoin) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalHashJoin {
	tp := plancodec.TypeHashJoin
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, tp, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes BatchPointGetPlan.
func (p PhysicalBroadCastJoin) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalBroadCastJoin {
	tp := plancodec.TypeBroadcastJoin
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, tp, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p

}

// Init initializes PhysicalMergeJoin.
func (p PhysicalMergeJoin) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int) *PhysicalMergeJoin {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeMergeJoin, &p, offset)
	p.stats = stats
	return &p
}

// Init initializes basePhysicalAgg.
func (base basePhysicalAgg) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int) *basePhysicalAgg {
	base.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeHashAgg, &base, offset)
	base.stats = stats
	return &base
}

func (base basePhysicalAgg) initForHash(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalHashAgg {
	p := &PhysicalHashAgg{base}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeHashAgg, p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

func (base basePhysicalAgg) initForStream(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalStreamAgg {
	p := &PhysicalStreamAgg{base}
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeStreamAgg, p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

// Init initializes PhysicalApply.
func (p PhysicalApply) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalApply {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeApply, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalUnionScan.
func (p PhysicalUnionScan) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalUnionScan {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeUnionScan, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalIndexLookUpReader.
func (p PhysicalIndexLookUpReader) Init(ctx stochastikctx.Context, offset int) *PhysicalIndexLookUpReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeIndexLookUp, &p, offset)
	p.BlockPlans = flattenPushDownPlan(p.blockPlan)
	p.IndexPlans = flattenPushDownPlan(p.indexPlan)
	p.schemaReplicant = p.blockPlan.Schema()
	return &p
}

// Init initializes PhysicalIndexMergeReader.
func (p PhysicalIndexMergeReader) Init(ctx stochastikctx.Context, offset int) *PhysicalIndexMergeReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeIndexMerge, &p, offset)
	if p.blockPlan != nil {
		p.stats = p.blockPlan.statsInfo()
	} else {
		var totalRowCount float64
		for _, partPlan := range p.partialPlans {
			totalRowCount += partPlan.StatsCount()
		}
		p.stats = p.partialPlans[0].statsInfo().ScaleByExpectCnt(totalRowCount)
		p.stats.StatsVersion = p.partialPlans[0].statsInfo().StatsVersion
	}
	p.PartialPlans = make([][]PhysicalPlan, 0, len(p.partialPlans))
	for _, partialPlan := range p.partialPlans {
		tempPlans := flattenPushDownPlan(partialPlan)
		p.PartialPlans = append(p.PartialPlans, tempPlans)
	}
	if p.blockPlan != nil {
		p.BlockPlans = flattenPushDownPlan(p.blockPlan)
		p.schemaReplicant = p.blockPlan.Schema()
	} else {
		switch p.PartialPlans[0][0].(type) {
		case *PhysicalBlockScan:
			p.schemaReplicant = p.PartialPlans[0][0].Schema()
		default:
			is := p.PartialPlans[0][0].(*PhysicalIndexScan)
			p.schemaReplicant = is.dataSourceSchema
		}
	}
	return &p
}

// Init initializes PhysicalBlockReader.
func (p PhysicalBlockReader) Init(ctx stochastikctx.Context, offset int) *PhysicalBlockReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeBlockReader, &p, offset)
	if p.blockPlan != nil {
		p.BlockPlans = flattenPushDownPlan(p.blockPlan)
		p.schemaReplicant = p.blockPlan.Schema()
	}
	return &p
}

// Init initializes PhysicalIndexReader.
func (p PhysicalIndexReader) Init(ctx stochastikctx.Context, offset int) *PhysicalIndexReader {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeIndexReader, &p, offset)
	p.SetSchema(nil)
	return &p
}

// Init initializes PhysicalIndexJoin.
func (p PhysicalIndexJoin) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalIndexJoin {
	p.basePhysicalPlan = newBasePhysicalPlan(ctx, plancodec.TypeIndexJoin, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalIndexMergeJoin.
func (p PhysicalIndexMergeJoin) Init(ctx stochastikctx.Context) *PhysicalIndexMergeJoin {
	ctx.GetStochaseinstein_dbars().PlanID++
	p.tp = plancodec.TypeIndexMergeJoin
	p.id = ctx.GetStochaseinstein_dbars().PlanID
	p.ctx = ctx
	return &p
}

// Init initializes PhysicalIndexHashJoin.
func (p PhysicalIndexHashJoin) Init(ctx stochastikctx.Context) *PhysicalIndexHashJoin {
	ctx.GetStochaseinstein_dbars().PlanID++
	p.tp = plancodec.TypeIndexHashJoin
	p.id = ctx.GetStochaseinstein_dbars().PlanID
	p.ctx = ctx
	return &p
}

// Init initializes BatchPointGetPlan.
func (p BatchPointGetPlan) Init(ctx stochastikctx.Context, stats *property.StatsInfo, schemaReplicant *expression.Schema, names []*types.FieldName, offset int) *BatchPointGetPlan {
	p.basePlan = newBasePlan(ctx, plancodec.TypeBatchPointGet, offset)
	p.schemaReplicant = schemaReplicant
	p.names = names
	p.stats = stats
	p.DeferredCausets = ExpandVirtualDeferredCauset(p.DeferredCausets, p.schemaReplicant, p.TblInfo.DeferredCausets)
	return &p
}

// Init initializes PointGetPlan.
func (p PointGetPlan) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PointGetPlan {
	p.basePlan = newBasePlan(ctx, plancodec.TypePointGet, offset)
	p.stats = stats
	p.DeferredCausets = ExpandVirtualDeferredCauset(p.DeferredCausets, p.schemaReplicant, p.TblInfo.DeferredCausets)
	return &p
}

func flattenTreePlan(plan PhysicalPlan, plans []PhysicalPlan) []PhysicalPlan {
	plans = append(plans, plan)
	for _, child := range plan.Children() {
		plans = flattenTreePlan(child, plans)
	}
	return plans
}

// flattenPushDownPlan converts a plan tree to a list, whose head is the leaf node like block scan.
func flattenPushDownPlan(p PhysicalPlan) []PhysicalPlan {
	plans := make([]PhysicalPlan, 0, 5)
	plans = flattenTreePlan(p, plans)
	for i := 0; i < len(plans)/2; i++ {
		j := len(plans) - i - 1
		plans[i], plans[j] = plans[j], plans[i]
	}
	return plans
}
