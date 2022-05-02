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
	"fmt"
	"math/rand"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/distsql"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

type requiredEventsSelectResult struct {
	retTypes          []*types.FieldType
	totalEvents       int
	count             int
	expectedEventsRet []int
	numNextCalled     int
}

func (r *requiredEventsSelectResult) Fetch(context.Context)                   {}
func (r *requiredEventsSelectResult) NextRaw(context.Context) ([]byte, error) { return nil, nil }
func (r *requiredEventsSelectResult) Close() error                            { return nil }

func (r *requiredEventsSelectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	defer func() {
		if r.numNextCalled >= len(r.expectedEventsRet) {
			return
		}
		rowsRet := chk.NumEvents()
		expected := r.expectedEventsRet[r.numNextCalled]
		if rowsRet != expected {
			panic(fmt.Sprintf("unexpected number of rows returned, obtain: %v, expected: %v", rowsRet, expected))
		}
		r.numNextCalled++
	}()
	chk.Reset()
	if r.count > r.totalEvents {
		return nil
	}
	required := mathutil.Min(chk.RequiredEvents(), r.totalEvents-r.count)
	for i := 0; i < required; i++ {
		chk.AppendEvent(r.genOneEvent())
	}
	r.count += required
	return nil
}

func (r *requiredEventsSelectResult) genOneEvent() chunk.Event {
	event := chunk.MutEventFromTypes(r.retTypes)
	for i := range r.retTypes {
		event.SetValue(i, r.genValue(r.retTypes[i]))
	}
	return event.ToEvent()
}

func (r *requiredEventsSelectResult) genValue(valType *types.FieldType) interface{} {
	switch valType.Tp {
	case allegrosql.TypeLong, allegrosql.TypeLonglong:
		return int64(rand.Int())
	case allegrosql.TypeDouble:
		return rand.Float64()
	default:
		panic("not implement")
	}
}

func mockDistsqlSelectCtxSet(totalEvents int, expectedEventsRet []int) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, "totalEvents", totalEvents)
	ctx = context.WithValue(ctx, "expectedEventsRet", expectedEventsRet)
	return ctx
}

func mockDistsqlSelectCtxGet(ctx context.Context) (totalEvents int, expectedEventsRet []int) {
	totalEvents = ctx.Value("totalEvents").(int)
	expectedEventsRet = ctx.Value("expectedEventsRet").([]int)
	return
}

func mockSelectResult(ctx context.Context, sctx stochastikctx.Context, kvReq *ekv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, INTERLOCKPlanIDs []int) (distsql.SelectResult, error) {
	totalEvents, expectedEventsRet := mockDistsqlSelectCtxGet(ctx)
	return &requiredEventsSelectResult{
		retTypes:          fieldTypes,
		totalEvents:       totalEvents,
		expectedEventsRet: expectedEventsRet,
	}, nil
}

func buildBlockReader(sctx stochastikctx.Context) Executor {
	e := &BlockReaderExecutor{
		baseExecutor:     buildMockBaseExec(sctx),
		block:            &blocks.BlockCommon{},
		posetPosetDagPB:  buildMockPosetDagRequest(sctx),
		selectResultHook: selectResultHook{mockSelectResult},
	}
	return e
}

func buildMockPosetDagRequest(sctx stochastikctx.Context) *fidelpb.PosetDagRequest {
	builder := newExecutorBuilder(sctx, nil)
	req, _, err := builder.constructPosetDagReq([]core.PhysicalPlan{&core.PhysicalBlockScan{
		DeferredCausets: []*perceptron.DeferredCausetInfo{},
		Block:           &perceptron.BlockInfo{ID: 12345, PKIsHandle: false},
		Desc:            false,
	}}, ekv.EinsteinDB)
	if err != nil {
		panic(err)
	}
	return req
}

func buildMockBaseExec(sctx stochastikctx.Context) baseExecutor {
	retTypes := []*types.FieldType{types.NewFieldType(allegrosql.TypeDouble), types.NewFieldType(allegrosql.TypeLonglong)}
	defcaus := make([]*expression.DeferredCauset, len(retTypes))
	for i := range retTypes {
		defcaus[i] = &expression.DeferredCauset{Index: i, RetType: retTypes[i]}
	}
	schemaReplicant := expression.NewSchema(defcaus...)
	baseExec := newBaseExecutor(sctx, schemaReplicant, 0)
	return baseExec
}

func (s *testExecSuite) TestBlockReaderRequiredEvents(c *C) {
	maxChunkSize := defaultCtx().GetStochaseinstein_dbars().MaxChunkSize
	testCases := []struct {
		totalEvents      int
		requiredEvents   []int
		expectedEvents   []int
		expectedEventsDS []int
	}{
		{
			totalEvents:      10,
			requiredEvents:   []int{1, 5, 3, 10},
			expectedEvents:   []int{1, 5, 3, 1},
			expectedEventsDS: []int{1, 5, 3, 1},
		},
		{
			totalEvents:      maxChunkSize + 1,
			requiredEvents:   []int{1, 5, 3, 10, maxChunkSize},
			expectedEvents:   []int{1, 5, 3, 10, (maxChunkSize + 1) - 1 - 5 - 3 - 10},
			expectedEventsDS: []int{1, 5, 3, 10, (maxChunkSize + 1) - 1 - 5 - 3 - 10},
		},
		{
			totalEvents:      3*maxChunkSize + 1,
			requiredEvents:   []int{3, 10, maxChunkSize},
			expectedEvents:   []int{3, 10, maxChunkSize},
			expectedEventsDS: []int{3, 10, maxChunkSize},
		},
	}
	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := mockDistsqlSelectCtxSet(testCase.totalEvents, testCase.expectedEventsDS)
		exec := buildBlockReader(sctx)
		c.Assert(exec.Open(ctx), IsNil)
		chk := newFirstChunk(exec)
		for i := range testCase.requiredEvents {
			chk.SetRequiredEvents(testCase.requiredEvents[i], maxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumEvents(), Equals, testCase.expectedEvents[i])
		}
		c.Assert(exec.Close(), IsNil)
	}
}

func buildIndexReader(sctx stochastikctx.Context) Executor {
	e := &IndexReaderExecutor{
		baseExecutor:     buildMockBaseExec(sctx),
		posetPosetDagPB:  buildMockPosetDagRequest(sctx),
		index:            &perceptron.IndexInfo{},
		selectResultHook: selectResultHook{mockSelectResult},
	}
	return e
}

func (s *testExecSuite) TestIndexReaderRequiredEvents(c *C) {
	maxChunkSize := defaultCtx().GetStochaseinstein_dbars().MaxChunkSize
	testCases := []struct {
		totalEvents      int
		requiredEvents   []int
		expectedEvents   []int
		expectedEventsDS []int
	}{
		{
			totalEvents:      10,
			requiredEvents:   []int{1, 5, 3, 10},
			expectedEvents:   []int{1, 5, 3, 1},
			expectedEventsDS: []int{1, 5, 3, 1},
		},
		{
			totalEvents:      maxChunkSize + 1,
			requiredEvents:   []int{1, 5, 3, 10, maxChunkSize},
			expectedEvents:   []int{1, 5, 3, 10, (maxChunkSize + 1) - 1 - 5 - 3 - 10},
			expectedEventsDS: []int{1, 5, 3, 10, (maxChunkSize + 1) - 1 - 5 - 3 - 10},
		},
		{
			totalEvents:      3*maxChunkSize + 1,
			requiredEvents:   []int{3, 10, maxChunkSize},
			expectedEvents:   []int{3, 10, maxChunkSize},
			expectedEventsDS: []int{3, 10, maxChunkSize},
		},
	}
	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := mockDistsqlSelectCtxSet(testCase.totalEvents, testCase.expectedEventsDS)
		exec := buildIndexReader(sctx)
		c.Assert(exec.Open(ctx), IsNil)
		chk := newFirstChunk(exec)
		for i := range testCase.requiredEvents {
			chk.SetRequiredEvents(testCase.requiredEvents[i], maxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumEvents(), Equals, testCase.expectedEvents[i])
		}
		c.Assert(exec.Close(), IsNil)
	}
}
