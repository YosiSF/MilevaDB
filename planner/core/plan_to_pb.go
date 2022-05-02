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
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/distsql"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression/aggregation"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/ranger"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// ToPB implements PhysicalPlan ToPB interface.
func (p *basePhysicalPlan) ToPB(_ stochastikctx.Context, _ ekv.StoreType) (*fidelpb.Executor, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.basePlan.ExplainID())
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashAgg) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.Executor, error) {
	sc := ctx.GetStochaseinstein_dbars().StmtCtx
	client := ctx.GetClient()
	groupByExprs, err := expression.ExpressionsToPBList(sc, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &fidelpb.Aggregation{
		GroupBy: groupByExprs,
	}
	for _, aggFunc := range p.AggFuncs {
		aggExec.AggFunc = append(aggExec.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
	}
	executorID := ""
	if storeType == ekv.TiFlash {
		var err error
		aggExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeAggregation, Aggregation: aggExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalStreamAgg) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.Executor, error) {
	sc := ctx.GetStochaseinstein_dbars().StmtCtx
	client := ctx.GetClient()
	groupByExprs, err := expression.ExpressionsToPBList(sc, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &fidelpb.Aggregation{
		GroupBy: groupByExprs,
	}
	for _, aggFunc := range p.AggFuncs {
		aggExec.AggFunc = append(aggExec.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
	}
	executorID := ""
	if storeType == ekv.TiFlash {
		var err error
		aggExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeStreamAgg, Aggregation: aggExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalSelection) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.Executor, error) {
	sc := ctx.GetStochaseinstein_dbars().StmtCtx
	client := ctx.GetClient()
	conditions, err := expression.ExpressionsToPBList(sc, p.Conditions, client)
	if err != nil {
		return nil, err
	}
	selExec := &fidelpb.Selection{
		Conditions: conditions,
	}
	executorID := ""
	if storeType == ekv.TiFlash {
		var err error
		selExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeSelection, Selection: selExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalTopN) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.Executor, error) {
	sc := ctx.GetStochaseinstein_dbars().StmtCtx
	client := ctx.GetClient()
	topNExec := &fidelpb.TopN{
		Limit: p.Count,
	}
	for _, item := range p.ByItems {
		topNExec.OrderBy = append(topNExec.OrderBy, expression.SortByItemToPB(sc, client, item.Expr, item.Desc))
	}
	executorID := ""
	if storeType == ekv.TiFlash {
		var err error
		topNExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeTopN, TopN: topNExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalLimit) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.Executor, error) {
	limitExec := &fidelpb.Limit{
		Limit: p.Count,
	}
	executorID := ""
	if storeType == ekv.TiFlash {
		var err error
		limitExec.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeLimit, Limit: limitExec, ExecutorId: &executorID}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalBlockScan) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.Executor, error) {
	tsExec := blocks.BuildBlockScanFromInfos(p.Block, p.DeferredCausets)
	tsExec.Desc = p.Desc
	if p.isPartition {
		tsExec.BlockId = p.physicalBlockID
	}
	executorID := ""
	if storeType == ekv.TiFlash && p.IsGlobalRead {
		tsExec.NextReadEngine = fidelpb.EngineType_TiFlash
		ranges := distsql.BlockRangesToKVRanges(tsExec.BlockId, p.Ranges, nil)
		for _, keyRange := range ranges {
			tsExec.Ranges = append(tsExec.Ranges, fidelpb.KeyRange{Low: keyRange.StartKey, High: keyRange.EndKey})
		}
	}
	if storeType == ekv.TiFlash {
		executorID = p.ExplainID().String()
	}
	err := SetPBDeferredCausetsDefaultValue(ctx, tsExec.DeferredCausets, p.DeferredCausets)
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeBlockScan, TblScan: tsExec, ExecutorId: &executorID}, err
}

// checkCoverIndex checks whether we can pass unique info to EinsteinDB. We should push it if and only if the length of
// range and index are equal.
func checkCoverIndex(idx *perceptron.IndexInfo, ranges []*ranger.Range) bool {
	// If the index is (c1, c2) but the query range only contains c1, it is not a unique get.
	if !idx.Unique {
		return false
	}
	for _, rg := range ranges {
		if len(rg.LowVal) != len(idx.DeferredCausets) {
			return false
		}
	}
	return true
}

func findDeferredCausetInfoByID(infos []*perceptron.DeferredCausetInfo, id int64) *perceptron.DeferredCausetInfo {
	for _, info := range infos {
		if info.ID == id {
			return info
		}
	}
	return nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalIndexScan) ToPB(ctx stochastikctx.Context, _ ekv.StoreType) (*fidelpb.Executor, error) {
	columns := make([]*perceptron.DeferredCausetInfo, 0, p.schemaReplicant.Len())
	blockDeferredCausets := p.Block.DefCauss()
	for _, col := range p.schemaReplicant.DeferredCausets {
		if col.ID == perceptron.ExtraHandleID {
			columns = append(columns, perceptron.NewExtraHandleDefCausInfo())
		} else {
			columns = append(columns, findDeferredCausetInfoByID(blockDeferredCausets, col.ID))
		}
	}
	var pkDefCausIds []int64
	if p.NeedCommonHandle {
		pkDefCausIds = blocks.TryGetCommonPkDeferredCausetIds(p.Block)
	}
	idxExec := &fidelpb.IndexScan{
		BlockId:                  p.Block.ID,
		IndexId:                  p.Index.ID,
		DeferredCausets:          soliton.DeferredCausetsToProto(columns, p.Block.PKIsHandle),
		Desc:                     p.Desc,
		PrimaryDeferredCausetIds: pkDefCausIds,
	}
	if p.isPartition {
		idxExec.BlockId = p.physicalBlockID
	}
	unique := checkCoverIndex(p.Index, p.Ranges)
	idxExec.Unique = &unique
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeIndexScan, IdxScan: idxExec}, nil
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalBroadCastJoin) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.Executor, error) {
	sc := ctx.GetStochaseinstein_dbars().StmtCtx
	client := ctx.GetClient()
	leftJoinKeys := make([]expression.Expression, 0, len(p.LeftJoinKeys))
	rightJoinKeys := make([]expression.Expression, 0, len(p.RightJoinKeys))
	for _, leftKey := range p.LeftJoinKeys {
		leftJoinKeys = append(leftJoinKeys, leftKey)
	}
	for _, rightKey := range p.RightJoinKeys {
		rightJoinKeys = append(rightJoinKeys, rightKey)
	}
	lChildren, err := p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rChildren, err := p.children[1].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}

	left, err := expression.ExpressionsToPBList(sc, leftJoinKeys, client)
	if err != nil {
		return nil, err
	}
	right, err := expression.ExpressionsToPBList(sc, rightJoinKeys, client)
	if err != nil {
		return nil, err
	}
	pbJoinType := fidelpb.JoinType_TypeInnerJoin
	switch p.JoinType {
	case LeftOuterJoin:
		pbJoinType = fidelpb.JoinType_TypeLeftOuterJoin
	case RightOuterJoin:
		pbJoinType = fidelpb.JoinType_TypeRightOuterJoin
	}
	join := &fidelpb.Join{
		JoinType:      pbJoinType,
		JoinExecType:  fidelpb.JoinExecType_TypeHashJoin,
		InnerIdx:      int64(p.InnerChildIdx),
		LeftJoinKeys:  left,
		RightJoinKeys: right,
		Children:      []*fidelpb.Executor{lChildren, rChildren},
	}

	executorID := p.ExplainID().String()
	return &fidelpb.Executor{Tp: fidelpb.ExecType_TypeJoin, Join: join, ExecutorId: &executorID}, nil
}

// SetPBDeferredCausetsDefaultValue sets the default values of fidelpb.DeferredCausetInfos.
func SetPBDeferredCausetsDefaultValue(ctx stochastikctx.Context, pbDeferredCausets []*fidelpb.DeferredCausetInfo, columns []*perceptron.DeferredCausetInfo) error {
	for i, c := range columns {
		// For virtual columns, we set their default values to NULL so that EinsteinDB will return NULL properly,
		// They real values will be compute later.
		if c.IsGenerated() && !c.GeneratedStored {
			pbDeferredCausets[i].DefaultVal = []byte{codec.NilFlag}
		}
		if c.OriginDefaultValue == nil {
			continue
		}

		sessVars := ctx.GetStochaseinstein_dbars()
		originStrict := sessVars.StrictALLEGROSQLMode
		sessVars.StrictALLEGROSQLMode = false
		d, err := block.GetDefCausOriginDefaultValue(ctx, c)
		sessVars.StrictALLEGROSQLMode = originStrict
		if err != nil {
			return err
		}

		pbDeferredCausets[i].DefaultVal, err = blockcodec.EncodeValue(sessVars.StmtCtx, nil, d)
		if err != nil {
			return err
		}
	}
	return nil
}

// SupportStreaming returns true if a pushed down operation supports using interlock streaming API.
// Note that this function handle pushed down physical plan only! It's called in constructPosetDagReq.
// Some plans are difficult (if possible) to implement streaming, and some are pointless to do so.
// TODO: Support more HoTTs of physical plan.
func SupportStreaming(p PhysicalPlan) bool {
	switch p.(type) {
	case *PhysicalIndexScan, *PhysicalSelection, *PhysicalBlockScan:
		return true
	}
	return false
}
