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

package executor

import (
	"context"
	"sort"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/distsql"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/ranger"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/stringutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// make sure `BlockReaderExecutor` implements `Executor`.
var _ Executor = &BlockReaderExecutor{}

// selectResultHook is used to replog distsql.SelectWithRuntimeStats safely for testing.
type selectResultHook struct {
	selectResultFunc func(ctx context.Context, sctx stochastikctx.Context, kvReq *ekv.Request,
		fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, INTERLOCKPlanIDs []int) (distsql.SelectResult, error)
}

func (sr selectResultHook) SelectResult(ctx context.Context, sctx stochastikctx.Context, kvReq *ekv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, INTERLOCKPlanIDs []int, rootPlanID int) (distsql.SelectResult, error) {
	if sr.selectResultFunc == nil {
		return distsql.SelectWithRuntimeStats(ctx, sctx, kvReq, fieldTypes, fb, INTERLOCKPlanIDs, rootPlanID)
	}
	return sr.selectResultFunc(ctx, sctx, kvReq, fieldTypes, fb, INTERLOCKPlanIDs)
}

type kvRangeBuilder interface {
	buildKeyRange(pid int64) ([]ekv.KeyRange, error)
}

// BlockReaderExecutor sends PosetDag request and reads block data from ekv layer.
type BlockReaderExecutor struct {
	baseExecutor

	block block.Block

	// The source of key ranges varies from case to case.
	// It may be calculated from PyhsicalPlan by executorBuilder, or calculated from argument by dataBuilder;
	// It may be calculated from ranger.Ranger, or calculated from handles.
	// The block ID may also change because of the partition block, and causes the key range to change.
	// So instead of keeping a `range` struct field, it's better to define a interface.
	kvRangeBuilder
	// TODO: remove this field, use the kvRangeBuilder interface.
	ranges []*ranger.Range

	// kvRanges are only use for union scan.
	kvRanges        []ekv.KeyRange
	posetPosetDagPB *fidelpb.PosetDagRequest
	startTS         uint64
	// defCausumns are only required by union scan and virtual defCausumn.
	defCausumns []*perceptron.DeferredCausetInfo

	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler *blockResultHandler
	feedback      *statistics.QueryFeedback
	plans         []plannercore.PhysicalPlan
	blockPlan     plannercore.PhysicalPlan

	memTracker       *memory.Tracker
	selectResultHook // for testing

	keepOrder bool
	desc      bool
	streaming bool
	storeType ekv.StoreType
	// corDefCausInFilter tells whether there's correlated defCausumn in filter.
	corDefCausInFilter bool
	// corDefCausInAccess tells whether there's correlated defCausumn in access conditions.
	corDefCausInAccess bool
	// virtualDeferredCausetIndex records all the indices of virtual defCausumns and sort them in definition
	// to make sure we can compute the virtual defCausumn in right order.
	virtualDeferredCausetIndex []int
	// virtualDeferredCausetRetFieldTypes records the RetFieldTypes of virtual defCausumns.
	virtualDeferredCausetRetFieldTypes []*types.FieldType
	// batchINTERLOCK indicates whether use super batch interlock request, only works for TiFlash engine.
	batchINTERLOCK bool
}

// Open initialzes necessary variables for using this executor.
func (e *BlockReaderExecutor) Open(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("BlockReaderExecutor.Open", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker)

	var err error
	if e.corDefCausInFilter {
		if e.storeType == ekv.TiFlash {
			execs, _, err := constructDistExecForTiFlash(e.ctx, e.blockPlan)
			if err != nil {
				return err
			}
			e.posetPosetDagPB.RootExecutor = execs[0]
		} else {
			e.posetPosetDagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)
			if err != nil {
				return err
			}
		}
	}
	if e.runtimeStats != nil {
		defCauslExec := true
		e.posetPosetDagPB.DefCauslectExecutionSummaries = &defCauslExec
	}
	if e.corDefCausInAccess {
		ts := e.plans[0].(*plannercore.PhysicalBlockScan)
		access := ts.AccessCondition
		pkTP := ts.Block.GetPkDefCausInfo().FieldType
		e.ranges, err = ranger.BuildBlockRange(access, e.ctx.GetStochaseinstein_dbars().StmtCtx, &pkTP)
		if err != nil {
			return err
		}
	}

	e.resultHandler = &blockResultHandler{}
	if e.feedback != nil && e.feedback.Hist != nil {
		// EncodeInt don't need *statement.Context.
		var ok bool
		e.ranges, ok = e.feedback.Hist.SplitRange(nil, e.ranges, false)
		if !ok {
			e.feedback.Invalidate()
		}
	}
	firstPartRanges, secondPartRanges := splitRanges(e.ranges, e.keepOrder, e.desc)
	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	e.resultHandler.open(firstResult, secondResult)
	return nil
}

// Next fills data into the chunk passed by its caller.
// The task was actually done by blockReaderHandler.
func (e *BlockReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	logutil.Eventf(ctx, "block scan block: %s, range: %v", stringutil.MemoizeStr(func() string {
		var blockName string
		if meta := e.block.Meta(); meta != nil {
			blockName = meta.Name.L
		}
		return blockName
	}), e.ranges)
	if err := e.resultHandler.nextChunk(ctx, req); err != nil {
		e.feedback.Invalidate()
		return err
	}

	err := FillVirtualDeferredCausetValue(e.virtualDeferredCausetRetFieldTypes, e.virtualDeferredCausetIndex, e.schemaReplicant, e.defCausumns, e.ctx, req)
	if err != nil {
		return err
	}

	return nil
}

// Close implements the Executor Close interface.
func (e *BlockReaderExecutor) Close() error {
	var err error
	if e.resultHandler != nil {
		err = e.resultHandler.Close()
	}
	e.kvRanges = e.kvRanges[:0]
	e.ctx.StoreQueryFeedback(e.feedback)
	return err
}

// buildResp first builds request and sends it to einsteindb using distsql.Select. It uses SelectResut returned by the callee
// to fetch all results.
func (e *BlockReaderExecutor) buildResp(ctx context.Context, ranges []*ranger.Range) (distsql.SelectResult, error) {
	var builder distsql.RequestBuilder
	var reqBuilder *distsql.RequestBuilder
	if e.kvRangeBuilder != nil {
		kvRange, err := e.kvRangeBuilder.buildKeyRange(getPhysicalBlockID(e.block))
		if err != nil {
			return nil, err
		}
		reqBuilder = builder.SetKeyRanges(kvRange)
	} else if e.block.Meta() != nil && e.block.Meta().IsCommonHandle {
		reqBuilder = builder.SetCommonHandleRanges(e.ctx.GetStochaseinstein_dbars().StmtCtx, getPhysicalBlockID(e.block), ranges)
	} else {
		reqBuilder = builder.SetBlockRanges(getPhysicalBlockID(e.block), ranges, e.feedback)
	}
	kvReq, err := reqBuilder.
		SetPosetDagRequest(e.posetPosetDagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromStochaseinstein_dbars(e.ctx.GetStochaseinstein_dbars()).
		SetMemTracker(e.memTracker).
		SetStoreType(e.storeType).
		SetAllowBatchINTERLOCK(e.batchINTERLOCK).
		Build()
	if err != nil {
		return nil, err
	}
	e.kvRanges = append(e.kvRanges, kvReq.KeyRanges...)

	result, err := e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	return result, nil
}

func buildVirtualDeferredCausetIndex(schemaReplicant *expression.Schema, defCausumns []*perceptron.DeferredCausetInfo) []int {
	virtualDeferredCausetIndex := make([]int, 0, len(defCausumns))
	for i, defCaus := range schemaReplicant.DeferredCausets {
		if defCaus.VirtualExpr != nil {
			virtualDeferredCausetIndex = append(virtualDeferredCausetIndex, i)
		}
	}
	sort.Slice(virtualDeferredCausetIndex, func(i, j int) bool {
		return plannercore.FindDeferredCausetInfoByID(defCausumns, schemaReplicant.DeferredCausets[virtualDeferredCausetIndex[i]].ID).Offset <
			plannercore.FindDeferredCausetInfoByID(defCausumns, schemaReplicant.DeferredCausets[virtualDeferredCausetIndex[j]].ID).Offset
	})
	return virtualDeferredCausetIndex
}

// buildVirtualDeferredCausetInfo saves virtual defCausumn indices and sort them in definition order
func (e *BlockReaderExecutor) buildVirtualDeferredCausetInfo() {
	e.virtualDeferredCausetIndex = buildVirtualDeferredCausetIndex(e.Schema(), e.defCausumns)
	if len(e.virtualDeferredCausetIndex) > 0 {
		e.virtualDeferredCausetRetFieldTypes = make([]*types.FieldType, len(e.virtualDeferredCausetIndex))
		for i, idx := range e.virtualDeferredCausetIndex {
			e.virtualDeferredCausetRetFieldTypes[i] = e.schemaReplicant.DeferredCausets[idx].RetType
		}
	}
}

type blockResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true and want ascending order,
	// `optionalResult` will handles the request whose range is in signed int range, and
	// `result` will handle the request whose range is exceed signed int range.
	// If we want descending order, `optionalResult` will handles the request whose range is exceed signed, and
	// the `result` will handle the request whose range is in signed.
	// Otherwise, we just set `optionalFinished` true and the `result` handles the whole ranges.
	optionalResult distsql.SelectResult
	result         distsql.SelectResult

	optionalFinished bool
}

func (tr *blockResultHandler) open(optionalResult, result distsql.SelectResult) {
	if optionalResult == nil {
		tr.optionalFinished = true
		tr.result = result
		return
	}
	tr.optionalResult = optionalResult
	tr.result = result
	tr.optionalFinished = false
}

func (tr *blockResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if !tr.optionalFinished {
		err := tr.optionalResult.Next(ctx, chk)
		if err != nil {
			return err
		}
		if chk.NumEvents() > 0 {
			return nil
		}
		tr.optionalFinished = true
	}
	return tr.result.Next(ctx, chk)
}

func (tr *blockResultHandler) nextRaw(ctx context.Context) (data []byte, err error) {
	if !tr.optionalFinished {
		data, err = tr.optionalResult.NextRaw(ctx)
		if err != nil {
			return nil, err
		}
		if data != nil {
			return data, nil
		}
		tr.optionalFinished = true
	}
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (tr *blockResultHandler) Close() error {
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return err
}
