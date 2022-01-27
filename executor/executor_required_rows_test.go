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

package executor

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/disk"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

type requiredEventsDataSource struct {
	baseExecutor
	totalEvents int
	count       int
	ctx         stochastikctx.Context

	expectedEventsRet []int
	numNextCalled     int

	generator func(valType *types.FieldType) interface{}
}

func newRequiredEventsDataSourceWithGenerator(ctx stochastikctx.Context, totalEvents int, expectedEventsRet []int,
	gen func(valType *types.FieldType) interface{}) *requiredEventsDataSource {
	ds := newRequiredEventsDataSource(ctx, totalEvents, expectedEventsRet)
	ds.generator = gen
	return ds
}

func newRequiredEventsDataSource(ctx stochastikctx.Context, totalEvents int, expectedEventsRet []int) *requiredEventsDataSource {
	// the schemaReplicant of output is fixed now, which is [Double, Long]
	retTypes := []*types.FieldType{types.NewFieldType(allegrosql.TypeDouble), types.NewFieldType(allegrosql.TypeLonglong)}
	defcaus := make([]*expression.DeferredCauset, len(retTypes))
	for i := range retTypes {
		defcaus[i] = &expression.DeferredCauset{Index: i, RetType: retTypes[i]}
	}
	schemaReplicant := expression.NewSchema(defcaus...)
	baseExec := newBaseExecutor(ctx, schemaReplicant, 0)
	return &requiredEventsDataSource{baseExec, totalEvents, 0, ctx, expectedEventsRet, 0, defaultGenerator}
}

func (r *requiredEventsDataSource) Next(ctx context.Context, req *chunk.Chunk) error {
	defer func() {
		if r.expectedEventsRet == nil {
			r.numNextCalled++
			return
		}
		rowsRet := req.NumEvents()
		expected := r.expectedEventsRet[r.numNextCalled]
		if rowsRet != expected {
			panic(fmt.Sprintf("unexpected number of rows returned, obtain: %v, expected: %v", rowsRet, expected))
		}
		r.numNextCalled++
	}()

	req.Reset()
	if r.count > r.totalEvents {
		return nil
	}
	required := mathutil.Min(req.RequiredEvents(), r.totalEvents-r.count)
	for i := 0; i < required; i++ {
		req.AppendEvent(r.genOneEvent())
	}
	r.count += required
	return nil
}

func (r *requiredEventsDataSource) genOneEvent() chunk.Event {
	event := chunk.MutEventFromTypes(retTypes(r))
	for i, tp := range retTypes(r) {
		event.SetValue(i, r.generator(tp))
	}
	return event.ToEvent()
}

func defaultGenerator(valType *types.FieldType) interface{} {
	switch valType.Tp {
	case allegrosql.TypeLong, allegrosql.TypeLonglong:
		return int64(rand.Int())
	case allegrosql.TypeDouble:
		return rand.Float64()
	default:
		panic("not implement")
	}
}

func (r *requiredEventsDataSource) checkNumNextCalled() error {
	if r.numNextCalled != len(r.expectedEventsRet) {
		return fmt.Errorf("unexpected number of call on Next, obtain: %v, expected: %v",
			r.numNextCalled, len(r.expectedEventsRet))
	}
	return nil
}

func (s *testExecSuite) TestLimitRequiredEvents(c *C) {
	maxChunkSize := defaultCtx().GetStochastikVars().MaxChunkSize
	testCases := []struct {
		totalEvents      int
		limitOffset      int
		limitCount       int
		requiredEvents   []int
		expectedEvents   []int
		expectedEventsDS []int
	}{
		{
			totalEvents:      20,
			limitOffset:      0,
			limitCount:       10,
			requiredEvents:   []int{3, 5, 1, 500, 500},
			expectedEvents:   []int{3, 5, 1, 1, 0},
			expectedEventsDS: []int{3, 5, 1, 1},
		},
		{
			totalEvents:      20,
			limitOffset:      0,
			limitCount:       25,
			requiredEvents:   []int{9, 500},
			expectedEvents:   []int{9, 11},
			expectedEventsDS: []int{9, 11},
		},
		{
			totalEvents:      100,
			limitOffset:      50,
			limitCount:       30,
			requiredEvents:   []int{10, 5, 10, 20},
			expectedEvents:   []int{10, 5, 10, 5},
			expectedEventsDS: []int{60, 5, 10, 5},
		},
		{
			totalEvents:      100,
			limitOffset:      101,
			limitCount:       10,
			requiredEvents:   []int{10},
			expectedEvents:   []int{0},
			expectedEventsDS: []int{100, 0},
		},
		{
			totalEvents:      maxChunkSize + 20,
			limitOffset:      maxChunkSize + 1,
			limitCount:       10,
			requiredEvents:   []int{3, 3, 3, 100},
			expectedEvents:   []int{3, 3, 3, 1},
			expectedEventsDS: []int{maxChunkSize, 4, 3, 3, 1},
		},
	}

	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := context.Background()
		ds := newRequiredEventsDataSource(sctx, testCase.totalEvents, testCase.expectedEventsDS)
		exec := buildLimitExec(sctx, ds, testCase.limitOffset, testCase.limitCount)
		c.Assert(exec.Open(ctx), IsNil)
		chk := newFirstChunk(exec)
		for i := range testCase.requiredEvents {
			chk.SetRequiredEvents(testCase.requiredEvents[i], sctx.GetStochastikVars().MaxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumEvents(), Equals, testCase.expectedEvents[i])
		}
		c.Assert(exec.Close(), IsNil)
		c.Assert(ds.checkNumNextCalled(), IsNil)
	}
}

func buildLimitExec(ctx stochastikctx.Context, src Executor, offset, count int) Executor {
	n := mathutil.Min(count, ctx.GetStochastikVars().MaxChunkSize)
	base := newBaseExecutor(ctx, src.Schema(), 0, src)
	base.initCap = n
	limitExec := &LimitExec{
		baseExecutor: base,
		begin:        uint64(offset),
		end:          uint64(offset + count),
	}
	return limitExec
}

func defaultCtx() stochastikctx.Context {
	ctx := mock.NewContext()
	ctx.GetStochastikVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetStochastikVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetStochastikVars().StmtCtx.MemTracker = memory.NewTracker(-1, ctx.GetStochastikVars().MemQuotaQuery)
	ctx.GetStochastikVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	ctx.GetStochastikVars().SnapshotTS = uint64(1)
	return ctx
}

func (s *testExecSuite) TestSortRequiredEvents(c *C) {
	maxChunkSize := defaultCtx().GetStochastikVars().MaxChunkSize
	testCases := []struct {
		totalEvents      int
		groupBy          []int
		requiredEvents   []int
		expectedEvents   []int
		expectedEventsDS []int
	}{
		{
			totalEvents:      10,
			groupBy:          []int{0},
			requiredEvents:   []int{1, 5, 3, 10},
			expectedEvents:   []int{1, 5, 3, 1},
			expectedEventsDS: []int{10, 0},
		},
		{
			totalEvents:      10,
			groupBy:          []int{0, 1},
			requiredEvents:   []int{1, 5, 3, 10},
			expectedEvents:   []int{1, 5, 3, 1},
			expectedEventsDS: []int{10, 0},
		},
		{
			totalEvents:      maxChunkSize + 1,
			groupBy:          []int{0},
			requiredEvents:   []int{1, 5, 3, 10, maxChunkSize},
			expectedEvents:   []int{1, 5, 3, 10, (maxChunkSize + 1) - 1 - 5 - 3 - 10},
			expectedEventsDS: []int{maxChunkSize, 1, 0},
		},
		{
			totalEvents:      3*maxChunkSize + 1,
			groupBy:          []int{0},
			requiredEvents:   []int{1, 5, 3, 10, maxChunkSize},
			expectedEvents:   []int{1, 5, 3, 10, maxChunkSize},
			expectedEventsDS: []int{maxChunkSize, maxChunkSize, maxChunkSize, 1, 0},
		},
	}

	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := context.Background()
		ds := newRequiredEventsDataSource(sctx, testCase.totalEvents, testCase.expectedEventsDS)
		byItems := make([]*soliton.ByItems, 0, len(testCase.groupBy))
		for _, groupBy := range testCase.groupBy {
			defCaus := ds.Schema().DeferredCausets[groupBy]
			byItems = append(byItems, &soliton.ByItems{Expr: defCaus})
		}
		exec := buildSortExec(sctx, byItems, ds)
		c.Assert(exec.Open(ctx), IsNil)
		chk := newFirstChunk(exec)
		for i := range testCase.requiredEvents {
			chk.SetRequiredEvents(testCase.requiredEvents[i], maxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumEvents(), Equals, testCase.expectedEvents[i])
		}
		c.Assert(exec.Close(), IsNil)
		c.Assert(ds.checkNumNextCalled(), IsNil)
	}
}

func buildSortExec(sctx stochastikctx.Context, byItems []*soliton.ByItems, src Executor) Executor {
	sortExec := SortExec{
		baseExecutor:    newBaseExecutor(sctx, src.Schema(), 0, src),
		ByItems:         byItems,
		schemaReplicant: src.Schema(),
	}
	return &sortExec
}

func (s *testExecSuite) TestTopNRequiredEvents(c *C) {
	maxChunkSize := defaultCtx().GetStochastikVars().MaxChunkSize
	testCases := []struct {
		totalEvents      int
		topNOffset       int
		topNCount        int
		groupBy          []int
		requiredEvents   []int
		expectedEvents   []int
		expectedEventsDS []int
	}{
		{
			totalEvents:      10,
			topNOffset:       0,
			topNCount:        10,
			groupBy:          []int{0},
			requiredEvents:   []int{1, 1, 1, 1, 10},
			expectedEvents:   []int{1, 1, 1, 1, 6},
			expectedEventsDS: []int{10, 0},
		},
		{
			totalEvents:      100,
			topNOffset:       15,
			topNCount:        11,
			groupBy:          []int{0},
			requiredEvents:   []int{1, 1, 1, 1, 10},
			expectedEvents:   []int{1, 1, 1, 1, 7},
			expectedEventsDS: []int{26, 100 - 26, 0},
		},
		{
			totalEvents:      100,
			topNOffset:       95,
			topNCount:        10,
			groupBy:          []int{0},
			requiredEvents:   []int{1, 2, 3, 10},
			expectedEvents:   []int{1, 2, 2, 0},
			expectedEventsDS: []int{100, 0, 0},
		},
		{
			totalEvents:      maxChunkSize + 20,
			topNOffset:       1,
			topNCount:        5,
			groupBy:          []int{0, 1},
			requiredEvents:   []int{1, 3, 7, 10},
			expectedEvents:   []int{1, 3, 1, 0},
			expectedEventsDS: []int{6, maxChunkSize, 14, 0},
		},
		{
			totalEvents:      maxChunkSize + maxChunkSize + 20,
			topNOffset:       maxChunkSize + 10,
			topNCount:        8,
			groupBy:          []int{0, 1},
			requiredEvents:   []int{1, 2, 3, 5, 7},
			expectedEvents:   []int{1, 2, 3, 2, 0},
			expectedEventsDS: []int{maxChunkSize, 18, maxChunkSize, 2, 0},
		},
		{
			totalEvents:      maxChunkSize*5 + 10,
			topNOffset:       maxChunkSize*5 + 20,
			topNCount:        10,
			groupBy:          []int{0, 1},
			requiredEvents:   []int{1, 2, 3},
			expectedEvents:   []int{0, 0, 0},
			expectedEventsDS: []int{maxChunkSize, maxChunkSize, maxChunkSize, maxChunkSize, maxChunkSize, 10, 0, 0},
		},
		{
			totalEvents:      maxChunkSize + maxChunkSize + 10,
			topNOffset:       10,
			topNCount:        math.MaxInt64,
			groupBy:          []int{0, 1},
			requiredEvents:   []int{1, 2, 3, maxChunkSize, maxChunkSize},
			expectedEvents:   []int{1, 2, 3, maxChunkSize, maxChunkSize - 1 - 2 - 3},
			expectedEventsDS: []int{maxChunkSize, maxChunkSize, 10, 0, 0},
		},
	}

	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := context.Background()
		ds := newRequiredEventsDataSource(sctx, testCase.totalEvents, testCase.expectedEventsDS)
		byItems := make([]*soliton.ByItems, 0, len(testCase.groupBy))
		for _, groupBy := range testCase.groupBy {
			defCaus := ds.Schema().DeferredCausets[groupBy]
			byItems = append(byItems, &soliton.ByItems{Expr: defCaus})
		}
		exec := buildTopNExec(sctx, testCase.topNOffset, testCase.topNCount, byItems, ds)
		c.Assert(exec.Open(ctx), IsNil)
		chk := newFirstChunk(exec)
		for i := range testCase.requiredEvents {
			chk.SetRequiredEvents(testCase.requiredEvents[i], maxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumEvents(), Equals, testCase.expectedEvents[i])
		}
		c.Assert(exec.Close(), IsNil)
		c.Assert(ds.checkNumNextCalled(), IsNil)
	}
}

func buildTopNExec(ctx stochastikctx.Context, offset, count int, byItems []*soliton.ByItems, src Executor) Executor {
	sortExec := SortExec{
		baseExecutor:    newBaseExecutor(ctx, src.Schema(), 0, src),
		ByItems:         byItems,
		schemaReplicant: src.Schema(),
	}
	return &TopNExec{
		SortExec: sortExec,
		limit:    &plannercore.PhysicalLimit{Count: uint64(count), Offset: uint64(offset)},
	}
}

func (s *testExecSuite) TestSelectionRequiredEvents(c *C) {
	gen01 := func() func(valType *types.FieldType) interface{} {
		closureCount := 0
		return func(valType *types.FieldType) interface{} {
			switch valType.Tp {
			case allegrosql.TypeLong, allegrosql.TypeLonglong:
				ret := int64(closureCount % 2)
				closureCount++
				return ret
			case allegrosql.TypeDouble:
				return rand.Float64()
			default:
				panic("not implement")
			}
		}
	}

	maxChunkSize := defaultCtx().GetStochastikVars().MaxChunkSize
	testCases := []struct {
		totalEvents       int
		filtersOfDefCaus1 int
		requiredEvents    []int
		expectedEvents    []int
		expectedEventsDS  []int
		gen               func(valType *types.FieldType) interface{}
	}{
		{
			totalEvents:      20,
			requiredEvents:   []int{1, 2, 3, 4, 5, 20},
			expectedEvents:   []int{1, 2, 3, 4, 5, 5},
			expectedEventsDS: []int{20, 0},
		},
		{
			totalEvents:       20,
			filtersOfDefCaus1: 0,
			requiredEvents:    []int{1, 3, 5, 7, 9},
			expectedEvents:    []int{1, 3, 5, 1, 0},
			expectedEventsDS:  []int{20, 0, 0},
			gen:               gen01(),
		},
		{
			totalEvents:       maxChunkSize + 20,
			filtersOfDefCaus1: 1,
			requiredEvents:    []int{1, 3, 5, maxChunkSize},
			expectedEvents:    []int{1, 3, 5, maxChunkSize/2 - 1 - 3 - 5 + 10},
			expectedEventsDS:  []int{maxChunkSize, 20, 0},
			gen:               gen01(),
		},
	}

	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := context.Background()
		var filters []expression.Expression
		var ds *requiredEventsDataSource
		if testCase.gen == nil {
			// ignore filters
			ds = newRequiredEventsDataSource(sctx, testCase.totalEvents, testCase.expectedEventsDS)
		} else {
			ds = newRequiredEventsDataSourceWithGenerator(sctx, testCase.totalEvents, testCase.expectedEventsDS, testCase.gen)
			f, err := expression.NewFunction(
				sctx, ast.EQ, types.NewFieldType(byte(types.ETInt)), ds.Schema().DeferredCausets[1], &expression.Constant{
					Value:   types.NewCauset(testCase.filtersOfDefCaus1),
					RetType: types.NewFieldType(allegrosql.TypeTiny),
				})
			c.Assert(err, IsNil)
			filters = append(filters, f)
		}
		exec := buildSelectionExec(sctx, filters, ds)
		c.Assert(exec.Open(ctx), IsNil)
		chk := newFirstChunk(exec)
		for i := range testCase.requiredEvents {
			chk.SetRequiredEvents(testCase.requiredEvents[i], maxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumEvents(), Equals, testCase.expectedEvents[i])
		}
		c.Assert(exec.Close(), IsNil)
		c.Assert(ds.checkNumNextCalled(), IsNil)
	}
}

func buildSelectionExec(ctx stochastikctx.Context, filters []expression.Expression, src Executor) Executor {
	return &SelectionExec{
		baseExecutor: newBaseExecutor(ctx, src.Schema(), 0, src),
		filters:      filters,
	}
}

func (s *testExecSuite) TestProjectionUnparallelRequiredEvents(c *C) {
	maxChunkSize := defaultCtx().GetStochastikVars().MaxChunkSize
	testCases := []struct {
		totalEvents      int
		requiredEvents   []int
		expectedEvents   []int
		expectedEventsDS []int
	}{
		{
			totalEvents:      20,
			requiredEvents:   []int{1, 3, 5, 7, 9},
			expectedEvents:   []int{1, 3, 5, 7, 4},
			expectedEventsDS: []int{1, 3, 5, 7, 4},
		},
		{
			totalEvents:      maxChunkSize + 10,
			requiredEvents:   []int{1, 3, 5, 7, 9, maxChunkSize},
			expectedEvents:   []int{1, 3, 5, 7, 9, maxChunkSize - 1 - 3 - 5 - 7 - 9 + 10},
			expectedEventsDS: []int{1, 3, 5, 7, 9, maxChunkSize - 1 - 3 - 5 - 7 - 9 + 10},
		},
		{
			totalEvents:      maxChunkSize*2 + 10,
			requiredEvents:   []int{1, 7, 9, maxChunkSize, maxChunkSize + 10},
			expectedEvents:   []int{1, 7, 9, maxChunkSize, maxChunkSize + 10 - 1 - 7 - 9},
			expectedEventsDS: []int{1, 7, 9, maxChunkSize, maxChunkSize + 10 - 1 - 7 - 9},
		},
	}

	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := context.Background()
		ds := newRequiredEventsDataSource(sctx, testCase.totalEvents, testCase.expectedEventsDS)
		exprs := make([]expression.Expression, 0, len(ds.Schema().DeferredCausets))
		if len(exprs) == 0 {
			for _, defCaus := range ds.Schema().DeferredCausets {
				exprs = append(exprs, defCaus)
			}
		}
		exec := buildProjectionExec(sctx, exprs, ds, 0)
		c.Assert(exec.Open(ctx), IsNil)
		chk := newFirstChunk(exec)
		for i := range testCase.requiredEvents {
			chk.SetRequiredEvents(testCase.requiredEvents[i], maxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumEvents(), Equals, testCase.expectedEvents[i])
		}
		c.Assert(exec.Close(), IsNil)
		c.Assert(ds.checkNumNextCalled(), IsNil)
	}
}

func (s *testExecSuite) TestProjectionParallelRequiredEvents(c *C) {
	c.Skip("not sblock because of goroutine schedule")
	maxChunkSize := defaultCtx().GetStochastikVars().MaxChunkSize
	testCases := []struct {
		totalEvents      int
		numWorkers       int
		requiredEvents   []int
		expectedEvents   []int
		expectedEventsDS []int
	}{
		{
			totalEvents:      20,
			numWorkers:       1,
			requiredEvents:   []int{1, 2, 3, 4, 5, 6, 1, 1},
			expectedEvents:   []int{1, 1, 2, 3, 4, 5, 4, 0},
			expectedEventsDS: []int{1, 1, 2, 3, 4, 5, 4, 0},
		},
		{
			totalEvents:      maxChunkSize * 2,
			numWorkers:       1,
			requiredEvents:   []int{7, maxChunkSize, maxChunkSize, maxChunkSize},
			expectedEvents:   []int{7, 7, maxChunkSize, maxChunkSize - 14},
			expectedEventsDS: []int{7, 7, maxChunkSize, maxChunkSize - 14, 0},
		},
		{
			totalEvents:      20,
			numWorkers:       2,
			requiredEvents:   []int{1, 2, 3, 4, 5, 6, 1, 1, 1},
			expectedEvents:   []int{1, 1, 1, 2, 3, 4, 5, 3, 0},
			expectedEventsDS: []int{1, 1, 1, 2, 3, 4, 5, 3, 0},
		},
	}

	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := context.Background()
		ds := newRequiredEventsDataSource(sctx, testCase.totalEvents, testCase.expectedEventsDS)
		exprs := make([]expression.Expression, 0, len(ds.Schema().DeferredCausets))
		if len(exprs) == 0 {
			for _, defCaus := range ds.Schema().DeferredCausets {
				exprs = append(exprs, defCaus)
			}
		}
		exec := buildProjectionExec(sctx, exprs, ds, testCase.numWorkers)
		c.Assert(exec.Open(ctx), IsNil)
		chk := newFirstChunk(exec)
		for i := range testCase.requiredEvents {
			chk.SetRequiredEvents(testCase.requiredEvents[i], maxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumEvents(), Equals, testCase.expectedEvents[i])

			// wait projectionInputFetcher blocked on fetching data
			// from child in the background.
			time.Sleep(time.Millisecond * 25)
		}
		c.Assert(exec.Close(), IsNil)
		c.Assert(ds.checkNumNextCalled(), IsNil)
	}
}

func buildProjectionExec(ctx stochastikctx.Context, exprs []expression.Expression, src Executor, numWorkers int) Executor {
	return &ProjectionExec{
		baseExecutor:  newBaseExecutor(ctx, src.Schema(), 0, src),
		numWorkers:    int64(numWorkers),
		evaluatorSuit: expression.NewEvaluatorSuite(exprs, false),
	}
}

func divGenerator(factor int) func(valType *types.FieldType) interface{} {
	closureCountInt := 0
	closureCountDouble := 0
	return func(valType *types.FieldType) interface{} {
		switch valType.Tp {
		case allegrosql.TypeLong, allegrosql.TypeLonglong:
			ret := int64(closureCountInt / factor)
			closureCountInt++
			return ret
		case allegrosql.TypeDouble:
			ret := float64(closureCountInt / factor)
			closureCountDouble++
			return ret
		default:
			panic("not implement")
		}
	}
}

func (s *testExecSuite) TestStreamAggRequiredEvents(c *C) {
	maxChunkSize := defaultCtx().GetStochastikVars().MaxChunkSize
	testCases := []struct {
		totalEvents      int
		aggFunc          string
		requiredEvents   []int
		expectedEvents   []int
		expectedEventsDS []int
		gen              func(valType *types.FieldType) interface{}
	}{
		{
			totalEvents:      1000000,
			aggFunc:          ast.AggFuncSum,
			requiredEvents:   []int{1, 2, 3, 4, 5, 6, 7},
			expectedEvents:   []int{1, 2, 3, 4, 5, 6, 7},
			expectedEventsDS: []int{maxChunkSize},
			gen:              divGenerator(1),
		},
		{
			totalEvents:      maxChunkSize * 3,
			aggFunc:          ast.AggFuncAvg,
			requiredEvents:   []int{1, 3},
			expectedEvents:   []int{1, 2},
			expectedEventsDS: []int{maxChunkSize, maxChunkSize, maxChunkSize, 0},
			gen:              divGenerator(maxChunkSize),
		},
		{
			totalEvents:      maxChunkSize*2 - 1,
			aggFunc:          ast.AggFuncMax,
			requiredEvents:   []int{maxChunkSize/2 + 1},
			expectedEvents:   []int{maxChunkSize/2 + 1},
			expectedEventsDS: []int{maxChunkSize, maxChunkSize - 1},
			gen:              divGenerator(2),
		},
	}

	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := context.Background()
		ds := newRequiredEventsDataSourceWithGenerator(sctx, testCase.totalEvents, testCase.expectedEventsDS, testCase.gen)
		childDefCauss := ds.Schema().DeferredCausets
		schemaReplicant := expression.NewSchema(childDefCauss...)
		groupBy := []expression.Expression{childDefCauss[1]}
		aggFunc, err := aggregation.NewAggFuncDesc(sctx, testCase.aggFunc, []expression.Expression{childDefCauss[0]}, true)
		c.Assert(err, IsNil)
		aggFuncs := []*aggregation.AggFuncDesc{aggFunc}
		exec := buildStreamAggExecutor(sctx, ds, schemaReplicant, aggFuncs, groupBy)
		c.Assert(exec.Open(ctx), IsNil)
		chk := newFirstChunk(exec)
		for i := range testCase.requiredEvents {
			chk.SetRequiredEvents(testCase.requiredEvents[i], maxChunkSize)
			c.Assert(exec.Next(ctx, chk), IsNil)
			c.Assert(chk.NumEvents(), Equals, testCase.expectedEvents[i])
		}
		c.Assert(exec.Close(), IsNil)
		c.Assert(ds.checkNumNextCalled(), IsNil)
	}
}

func (s *testExecSuite) TestMergeJoinRequiredEvents(c *C) {
	justReturn1 := func(valType *types.FieldType) interface{} {
		switch valType.Tp {
		case allegrosql.TypeLong, allegrosql.TypeLonglong:
			return int64(1)
		case allegrosql.TypeDouble:
			return float64(1)
		default:
			panic("not support")
		}
	}
	joinTypes := []plannercore.JoinType{plannercore.RightOuterJoin, plannercore.LeftOuterJoin,
		plannercore.LeftOuterSemiJoin, plannercore.AntiLeftOuterSemiJoin}
	for _, joinType := range joinTypes {
		ctx := defaultCtx()
		required := make([]int, 100)
		for i := range required {
			required[i] = rand.Int()%ctx.GetStochastikVars().MaxChunkSize + 1
		}
		innerSrc := newRequiredEventsDataSourceWithGenerator(ctx, 1, nil, justReturn1)             // just return one event: (1, 1)
		outerSrc := newRequiredEventsDataSourceWithGenerator(ctx, 10000000, required, justReturn1) // always return (1, 1)
		exec := buildMergeJoinExec(ctx, joinType, innerSrc, outerSrc)
		c.Assert(exec.Open(context.Background()), IsNil)

		chk := newFirstChunk(exec)
		for i := range required {
			chk.SetRequiredEvents(required[i], ctx.GetStochastikVars().MaxChunkSize)
			c.Assert(exec.Next(context.Background(), chk), IsNil)
		}
		c.Assert(exec.Close(), IsNil)
		c.Assert(outerSrc.checkNumNextCalled(), IsNil)
	}
}

func genTestChunk4VecGroupChecker(chkEvents []int, sameNum int) (expr []expression.Expression, inputs []*chunk.Chunk) {
	chkNum := len(chkEvents)
	numEvents := 0
	inputs = make([]*chunk.Chunk, chkNum)
	fts := make([]*types.FieldType, 1)
	fts[0] = types.NewFieldType(allegrosql.TypeLonglong)
	for i := 0; i < chkNum; i++ {
		inputs[i] = chunk.New(fts, chkEvents[i], chkEvents[i])
		numEvents += chkEvents[i]
	}
	var numGroups int
	if numEvents%sameNum == 0 {
		numGroups = numEvents / sameNum
	} else {
		numGroups = numEvents/sameNum + 1
	}

	rand.Seed(time.Now().Unix())
	nullPos := rand.Intn(numGroups)
	cnt := 0
	val := rand.Int63()
	for i := 0; i < chkNum; i++ {
		defCaus := inputs[i].DeferredCauset(0)
		defCaus.ResizeInt64(chkEvents[i], false)
		i64s := defCaus.Int64s()
		for j := 0; j < chkEvents[i]; j++ {
			if cnt == sameNum {
				val = rand.Int63()
				cnt = 0
				nullPos--
			}
			if nullPos == 0 {
				defCaus.SetNull(j, true)
			} else {
				i64s[j] = val
			}
			cnt++
		}
	}

	expr = make([]expression.Expression, 1)
	expr[0] = &expression.DeferredCauset{
		RetType: &types.FieldType{Tp: allegrosql.TypeLonglong, Flen: allegrosql.MaxIntWidth},
		Index:   0,
	}
	return
}

func (s *testExecSuite) TestVecGroupChecker(c *C) {
	testCases := []struct {
		chunkEvents    []int
		expectedGroups int
		expectedFlag   []bool
		sameNum        int
	}{
		{
			chunkEvents:    []int{1024, 1},
			expectedGroups: 1025,
			expectedFlag:   []bool{false, false},
			sameNum:        1,
		},
		{
			chunkEvents:    []int{1024, 1},
			expectedGroups: 1,
			expectedFlag:   []bool{false, true},
			sameNum:        1025,
		},
		{
			chunkEvents:    []int{1, 1},
			expectedGroups: 1,
			expectedFlag:   []bool{false, true},
			sameNum:        2,
		},
		{
			chunkEvents:    []int{1, 1},
			expectedGroups: 2,
			expectedFlag:   []bool{false, false},
			sameNum:        1,
		},
		{
			chunkEvents:    []int{2, 2},
			expectedGroups: 2,
			expectedFlag:   []bool{false, false},
			sameNum:        2,
		},
		{
			chunkEvents:    []int{2, 2},
			expectedGroups: 1,
			expectedFlag:   []bool{false, true},
			sameNum:        4,
		},
	}

	ctx := mock.NewContext()
	for _, testCase := range testCases {
		expr, inputChks := genTestChunk4VecGroupChecker(testCase.chunkEvents, testCase.sameNum)
		groupChecker := newVecGroupChecker(ctx, expr)
		groupNum := 0
		for i, inputChk := range inputChks {
			flag, err := groupChecker.splitIntoGroups(inputChk)
			c.Assert(err, IsNil)
			c.Assert(flag, Equals, testCase.expectedFlag[i])
			if flag {
				groupNum += groupChecker.groupCount - 1
			} else {
				groupNum += groupChecker.groupCount
			}
		}
		c.Assert(groupNum, Equals, testCase.expectedGroups)
	}
}

func buildMergeJoinExec(ctx stochastikctx.Context, joinType plannercore.JoinType, innerSrc, outerSrc Executor) Executor {
	if joinType == plannercore.RightOuterJoin {
		innerSrc, outerSrc = outerSrc, innerSrc
	}

	innerDefCauss := innerSrc.Schema().DeferredCausets
	outerDefCauss := outerSrc.Schema().DeferredCausets
	j := plannercore.BuildMergeJoinPlan(ctx, joinType, outerDefCauss, innerDefCauss)

	j.SetChildren(&mockPlan{exec: outerSrc}, &mockPlan{exec: innerSrc})
	defcaus := append(append([]*expression.DeferredCauset{}, outerDefCauss...), innerDefCauss...)
	schemaReplicant := expression.NewSchema(defcaus...)
	j.SetSchema(schemaReplicant)

	j.CompareFuncs = make([]expression.CompareFunc, 0, len(j.LeftJoinKeys))
	for i := range j.LeftJoinKeys {
		j.CompareFuncs = append(j.CompareFuncs, expression.GetCmpFunction(nil, j.LeftJoinKeys[i], j.RightJoinKeys[i]))
	}

	b := newExecutorBuilder(ctx, nil)
	return b.build(j)
}

type mockPlan struct {
	MockPhysicalPlan
	exec Executor
}

func (mp *mockPlan) GetExecutor() Executor {
	return mp.exec
}

func (mp *mockPlan) Schema() *expression.Schema {
	return mp.exec.Schema()
}

func (s *testExecSuite) TestVecGroupCheckerDATARACE(c *C) {
	ctx := mock.NewContext()

	mTypes := []byte{allegrosql.TypeVarString, allegrosql.TypeNewDecimal, allegrosql.TypeJSON}
	for _, mType := range mTypes {
		exprs := make([]expression.Expression, 1)
		exprs[0] = &expression.DeferredCauset{
			RetType: &types.FieldType{Tp: mType},
			Index:   0,
		}
		vgc := newVecGroupChecker(ctx, exprs)

		fts := []*types.FieldType{types.NewFieldType(mType)}
		chk := chunk.New(fts, 1, 1)
		vgc.allocateBuffer = func(evalType types.EvalType, capacity int) (*chunk.DeferredCauset, error) {
			return chk.DeferredCauset(0), nil
		}
		vgc.releaseBuffer = func(defCausumn *chunk.DeferredCauset) {}

		switch mType {
		case allegrosql.TypeVarString:
			chk.DeferredCauset(0).ReserveString(1)
			chk.DeferredCauset(0).AppendString("abc")
		case allegrosql.TypeNewDecimal:
			chk.DeferredCauset(0).ResizeDecimal(1, false)
			chk.DeferredCauset(0).Decimals()[0] = *types.NewDecFromInt(123)
		case allegrosql.TypeJSON:
			chk.DeferredCauset(0).ReserveJSON(1)
			j := new(json.BinaryJSON)
			c.Assert(j.UnmarshalJSON([]byte(fmt.Sprintf(`{"%v":%v}`, 123, 123))), IsNil)
			chk.DeferredCauset(0).AppendJSON(*j)
		}

		_, err := vgc.splitIntoGroups(chk)
		c.Assert(err, IsNil)

		switch mType {
		case allegrosql.TypeVarString:
			c.Assert(vgc.firstEventCausets[0].GetString(), Equals, "abc")
			c.Assert(vgc.lastEventCausets[0].GetString(), Equals, "abc")
			chk.DeferredCauset(0).ReserveString(1)
			chk.DeferredCauset(0).AppendString("edf")
			c.Assert(vgc.firstEventCausets[0].GetString(), Equals, "abc")
			c.Assert(vgc.lastEventCausets[0].GetString(), Equals, "abc")
		case allegrosql.TypeNewDecimal:
			c.Assert(vgc.firstEventCausets[0].GetMysqlDecimal().String(), Equals, "123")
			c.Assert(vgc.lastEventCausets[0].GetMysqlDecimal().String(), Equals, "123")
			chk.DeferredCauset(0).ResizeDecimal(1, false)
			chk.DeferredCauset(0).Decimals()[0] = *types.NewDecFromInt(456)
			c.Assert(vgc.firstEventCausets[0].GetMysqlDecimal().String(), Equals, "123")
			c.Assert(vgc.lastEventCausets[0].GetMysqlDecimal().String(), Equals, "123")
		case allegrosql.TypeJSON:
			c.Assert(vgc.firstEventCausets[0].GetMysqlJSON().String(), Equals, `{"123": 123}`)
			c.Assert(vgc.lastEventCausets[0].GetMysqlJSON().String(), Equals, `{"123": 123}`)
			chk.DeferredCauset(0).ReserveJSON(1)
			j := new(json.BinaryJSON)
			c.Assert(j.UnmarshalJSON([]byte(fmt.Sprintf(`{"%v":%v}`, 456, 456))), IsNil)
			chk.DeferredCauset(0).AppendJSON(*j)
			c.Assert(vgc.firstEventCausets[0].GetMysqlJSON().String(), Equals, `{"123": 123}`)
			c.Assert(vgc.lastEventCausets[0].GetMysqlJSON().String(), Equals, `{"123": 123}`)
		}
	}
}
