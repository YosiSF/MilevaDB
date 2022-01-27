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
	"encoding/base64"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/planner/property"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/disk"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap/zapcore"
)

var (
	_          Executor          = &mockDataSource{}
	_          core.PhysicalPlan = &mockDataPhysicalPlan{}
	wideString                   = strings.Repeat("x", 5*1024)
)

type mockDataSourceParameters struct {
	schemaReplicant *expression.Schema
	genDataFunc     func(event int, typ *types.FieldType) interface{}
	ndvs            []int  // number of distinct values on defCausumns[i] and zero represents no limit
	orders          []bool // defCausumns[i] should be ordered if orders[i] is true
	rows            int    // number of rows the DataSource should output
	ctx             stochastikctx.Context
}

type mockDataSource struct {
	baseExecutor
	p        mockDataSourceParameters
	genData  []*chunk.Chunk
	chunks   []*chunk.Chunk
	chunkPtr int
}

type mockDataPhysicalPlan struct {
	MockPhysicalPlan
	schemaReplicant *expression.Schema
	exec            Executor
}

func (mp *mockDataPhysicalPlan) GetExecutor() Executor {
	return mp.exec
}

func (mp *mockDataPhysicalPlan) Schema() *expression.Schema {
	return mp.schemaReplicant
}

func (mp *mockDataPhysicalPlan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return "mockData_0"
	})
}

func (mp *mockDataPhysicalPlan) Stats() *property.StatsInfo {
	return nil
}

func (mp *mockDataPhysicalPlan) SelectBlockOffset() int {
	return 0
}

func (mds *mockDataSource) genDefCausCausets(defCaus int) (results []interface{}) {
	typ := mds.retFieldTypes[defCaus]
	order := false
	if defCaus < len(mds.p.orders) {
		order = mds.p.orders[defCaus]
	}
	rows := mds.p.rows
	NDV := 0
	if defCaus < len(mds.p.ndvs) {
		NDV = mds.p.ndvs[defCaus]
	}
	results = make([]interface{}, 0, rows)
	if NDV == 0 {
		if mds.p.genDataFunc == nil {
			for i := 0; i < rows; i++ {
				results = append(results, mds.randCauset(typ))
			}
		} else {
			for i := 0; i < rows; i++ {
				results = append(results, mds.p.genDataFunc(i, typ))
			}
		}
	} else {
		datumSet := make(map[string]bool, NDV)
		datums := make([]interface{}, 0, NDV)
		for len(datums) < NDV {
			d := mds.randCauset(typ)
			str := fmt.Sprintf("%v", d)
			if datumSet[str] {
				continue
			}
			datumSet[str] = true
			datums = append(datums, d)
		}

		for i := 0; i < rows; i++ {
			results = append(results, datums[rand.Intn(NDV)])
		}
	}

	if order {
		sort.Slice(results, func(i, j int) bool {
			switch typ.Tp {
			case allegrosql.TypeLong, allegrosql.TypeLonglong:
				return results[i].(int64) < results[j].(int64)
			case allegrosql.TypeDouble:
				return results[i].(float64) < results[j].(float64)
			case allegrosql.TypeVarString:
				return results[i].(string) < results[j].(string)
			default:
				panic("not implement")
			}
		})
	}

	return
}

func (mds *mockDataSource) randCauset(typ *types.FieldType) interface{} {
	switch typ.Tp {
	case allegrosql.TypeLong, allegrosql.TypeLonglong:
		return int64(rand.Int())
	case allegrosql.TypeDouble, allegrosql.TypeFloat:
		return rand.Float64()
	case allegrosql.TypeNewDecimal:
		var d types.MyDecimal
		return d.FromInt(int64(rand.Int()))
	case allegrosql.TypeVarString:
		buff := make([]byte, 10)
		rand.Read(buff)
		return base64.RawURLEncoding.EncodeToString(buff)
	default:
		panic("not implement")
	}
}

func (mds *mockDataSource) prepareChunks() {
	mds.chunks = make([]*chunk.Chunk, len(mds.genData))
	for i := range mds.chunks {
		mds.chunks[i] = mds.genData[i].INTERLOCKyConstruct()
	}
	mds.chunkPtr = 0
}

func (mds *mockDataSource) Next(ctx context.Context, req *chunk.Chunk) error {
	if mds.chunkPtr >= len(mds.chunks) {
		req.Reset()
		return nil
	}
	dataChk := mds.chunks[mds.chunkPtr]
	dataChk.SwapDeferredCausets(req)
	mds.chunkPtr++
	return nil
}

func buildMockDataSource(opt mockDataSourceParameters) *mockDataSource {
	baseExec := newBaseExecutor(opt.ctx, opt.schemaReplicant, 0)
	m := &mockDataSource{baseExec, opt, nil, nil, 0}
	rTypes := retTypes(m)
	defCausData := make([][]interface{}, len(rTypes))
	for i := 0; i < len(rTypes); i++ {
		defCausData[i] = m.genDefCausCausets(i)
	}

	m.genData = make([]*chunk.Chunk, (m.p.rows+m.maxChunkSize-1)/m.maxChunkSize)
	for i := range m.genData {
		m.genData[i] = chunk.NewChunkWithCapacity(retTypes(m), m.maxChunkSize)
	}

	for i := 0; i < m.p.rows; i++ {
		idx := i / m.maxChunkSize
		retTypes := retTypes(m)
		for defCausIdx := 0; defCausIdx < len(rTypes); defCausIdx++ {
			switch retTypes[defCausIdx].Tp {
			case allegrosql.TypeLong, allegrosql.TypeLonglong:
				m.genData[idx].AppendInt64(defCausIdx, defCausData[defCausIdx][i].(int64))
			case allegrosql.TypeDouble, allegrosql.TypeFloat:
				m.genData[idx].AppendFloat64(defCausIdx, defCausData[defCausIdx][i].(float64))
			case allegrosql.TypeNewDecimal:
				m.genData[idx].AppendMyDecimal(defCausIdx, defCausData[defCausIdx][i].(*types.MyDecimal))
			case allegrosql.TypeVarString:
				m.genData[idx].AppendString(defCausIdx, defCausData[defCausIdx][i].(string))
			default:
				panic("not implement")
			}
		}
	}
	return m
}

func buildMockDataSourceWithIndex(opt mockDataSourceParameters, index []int) *mockDataSource {
	opt.orders = make([]bool, len(opt.schemaReplicant.DeferredCausets))
	for _, idx := range index {
		opt.orders[idx] = true
	}
	return buildMockDataSource(opt)
}

// aggTestCase has a fixed schemaReplicant (aggDefCaus Double, groupBy LongLong).
type aggTestCase struct {
	execType    string // "hash" or "stream"
	aggFunc     string // sum, avg, count ....
	groupByNDV  int    // the number of distinct group-by keys
	hasDistinct bool
	rows        int
	concurrency int
	ctx         stochastikctx.Context
}

func (a aggTestCase) defCausumns() []*expression.DeferredCauset {
	return []*expression.DeferredCauset{
		{Index: 0, RetType: types.NewFieldType(allegrosql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(allegrosql.TypeLonglong)},
	}
}

func (a aggTestCase) String() string {
	return fmt.Sprintf("(execType:%v, aggFunc:%v, ndv:%v, hasDistinct:%v, rows:%v, concurrency:%v)",
		a.execType, a.aggFunc, a.groupByNDV, a.hasDistinct, a.rows, a.concurrency)
}

func defaultAggTestCase(exec string) *aggTestCase {
	ctx := mock.NewContext()
	ctx.GetStochastikVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetStochastikVars().MaxChunkSize = variable.DefMaxChunkSize
	return &aggTestCase{exec, ast.AggFuncSum, 1000, false, 10000000, 4, ctx}
}

func buildHashAggExecutor(ctx stochastikctx.Context, src Executor, schemaReplicant *expression.Schema,
	aggFuncs []*aggregation.AggFuncDesc, groupItems []expression.Expression) Executor {
	plan := new(core.PhysicalHashAgg)
	plan.AggFuncs = aggFuncs
	plan.GroupByItems = groupItems
	plan.SetSchema(schemaReplicant)
	plan.Init(ctx, nil, 0)
	plan.SetChildren(nil)
	b := newExecutorBuilder(ctx, nil)
	exec := b.build(plan)
	hashAgg := exec.(*HashAggExec)
	hashAgg.children[0] = src
	return exec
}

func buildStreamAggExecutor(ctx stochastikctx.Context, src Executor, schemaReplicant *expression.Schema,
	aggFuncs []*aggregation.AggFuncDesc, groupItems []expression.Expression) Executor {
	plan := new(core.PhysicalStreamAgg)
	plan.AggFuncs = aggFuncs
	plan.GroupByItems = groupItems
	plan.SetSchema(schemaReplicant)
	plan.Init(ctx, nil, 0)
	plan.SetChildren(nil)
	b := newExecutorBuilder(ctx, nil)
	exec := b.build(plan)
	streamAgg := exec.(*StreamAggExec)
	streamAgg.children[0] = src
	return exec
}

func builPosetDaggExecutor(b *testing.B, testCase *aggTestCase, child Executor) Executor {
	ctx := testCase.ctx
	if err := ctx.GetStochastikVars().SetSystemVar(variable.MilevaDBHashAggFinalConcurrency, fmt.Sprintf("%v", testCase.concurrency)); err != nil {
		b.Fatal(err)
	}
	if err := ctx.GetStochastikVars().SetSystemVar(variable.MilevaDBHashAggPartialConcurrency, fmt.Sprintf("%v", testCase.concurrency)); err != nil {
		b.Fatal(err)
	}

	childDefCauss := testCase.defCausumns()
	schemaReplicant := expression.NewSchema(childDefCauss...)
	groupBy := []expression.Expression{childDefCauss[1]}
	aggFunc, err := aggregation.NewAggFuncDesc(testCase.ctx, testCase.aggFunc, []expression.Expression{childDefCauss[0]}, testCase.hasDistinct)
	if err != nil {
		b.Fatal(err)
	}
	aggFuncs := []*aggregation.AggFuncDesc{aggFunc}

	var aggExec Executor
	switch testCase.execType {
	case "hash":
		aggExec = buildHashAggExecutor(testCase.ctx, child, schemaReplicant, aggFuncs, groupBy)
	case "stream":
		aggExec = buildStreamAggExecutor(testCase.ctx, child, schemaReplicant, aggFuncs, groupBy)
	default:
		b.Fatal("not implement")
	}
	return aggExec
}

func benchmarkAggExecWithCase(b *testing.B, casTest *aggTestCase) {
	defcaus := casTest.defCausumns()
	orders := []bool{false, casTest.execType == "stream"}
	dataSource := buildMockDataSource(mockDataSourceParameters{
		schemaReplicant: expression.NewSchema(defcaus...),
		ndvs:            []int{0, casTest.groupByNDV},
		orders:          orders,
		rows:            casTest.rows,
		ctx:             casTest.ctx,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // prepare a new agg-executor
		aggExec := builPosetDaggExecutor(b, casTest, dataSource)
		tmpCtx := context.Background()
		chk := newFirstChunk(aggExec)
		dataSource.prepareChunks()

		b.StartTimer()
		if err := aggExec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := aggExec.Next(tmpCtx, chk); err != nil {
				b.Fatal(b)
			}
			if chk.NumEvents() == 0 {
				break
			}
		}

		if err := aggExec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkAggEvents(b *testing.B) {
	rows := []int{100000, 1000000, 10000000}
	concurrencies := []int{1, 4, 8, 15, 20, 30, 40}
	for _, event := range rows {
		for _, con := range concurrencies {
			for _, exec := range []string{"hash", "stream"} {
				if exec == "stream" && con > 1 {
					continue
				}
				cas := defaultAggTestCase(exec)
				cas.rows = event
				cas.concurrency = con
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkAggExecWithCase(b, cas)
				})
			}
		}
	}
}

func BenchmarkAggGroupByNDV(b *testing.B) {
	NDVs := []int{10, 100, 1000, 10000, 100000, 1000000, 10000000}
	for _, NDV := range NDVs {
		for _, exec := range []string{"hash", "stream"} {
			cas := defaultAggTestCase(exec)
			cas.groupByNDV = NDV
			b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
				benchmarkAggExecWithCase(b, cas)
			})
		}
	}
}

func BenchmarkAggConcurrency(b *testing.B) {
	concs := []int{1, 4, 8, 15, 20, 30, 40}
	for _, con := range concs {
		for _, exec := range []string{"hash", "stream"} {
			if exec == "stream" && con > 1 {
				continue
			}
			cas := defaultAggTestCase(exec)
			cas.concurrency = con
			b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
				benchmarkAggExecWithCase(b, cas)
			})
		}
	}
}

func BenchmarkAggDistinct(b *testing.B) {
	rows := []int{100000, 1000000, 10000000}
	distincts := []bool{false, true}
	for _, event := range rows {
		for _, exec := range []string{"hash", "stream"} {
			for _, distinct := range distincts {
				cas := defaultAggTestCase(exec)
				cas.rows = event
				cas.hasDistinct = distinct
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkAggExecWithCase(b, cas)
				})
			}
		}
	}
}

func buildWindowExecutor(ctx stochastikctx.Context, windowFunc string, funcs int, frame *core.WindowFrame, srcExec Executor, schemaReplicant *expression.Schema, partitionBy []*expression.DeferredCauset, concurrency int, dataSourceSorted bool) Executor {
	src := &mockDataPhysicalPlan{
		schemaReplicant: srcExec.Schema(),
		exec:            srcExec,
	}

	win := new(core.PhysicalWindow)
	win.WindowFuncDescs = make([]*aggregation.WindowFuncDesc, 0)
	winSchema := schemaReplicant.Clone()
	for i := 0; i < funcs; i++ {
		var args []expression.Expression
		switch windowFunc {
		case ast.WindowFuncNtile:
			args = append(args, &expression.Constant{Value: types.NewUintCauset(2)})
		case ast.WindowFuncNthValue:
			args = append(args, partitionBy[0], &expression.Constant{Value: types.NewUintCauset(2)})
		case ast.AggFuncSum:
			args = append(args, src.Schema().DeferredCausets[0])
		case ast.AggFuncAvg:
			args = append(args, src.Schema().DeferredCausets[0])
		case ast.AggFuncBitXor:
			args = append(args, src.Schema().DeferredCausets[0])
		case ast.AggFuncMax, ast.AggFuncMin:
			args = append(args, src.Schema().DeferredCausets[0])
		default:
			args = append(args, partitionBy[0])
		}
		desc, _ := aggregation.NewWindowFuncDesc(ctx, windowFunc, args)

		win.WindowFuncDescs = append(win.WindowFuncDescs, desc)
		winSchema.Append(&expression.DeferredCauset{
			UniqueID: 10 + (int64)(i),
			RetType:  types.NewFieldType(allegrosql.TypeLonglong),
		})
	}
	for _, defCaus := range partitionBy {
		win.PartitionBy = append(win.PartitionBy, property.Item{DefCaus: defCaus})
	}
	win.Frame = frame
	win.OrderBy = nil

	win.SetSchema(winSchema)
	win.Init(ctx, nil, 0)

	var tail core.PhysicalPlan = win
	if !dataSourceSorted {
		byItems := make([]*soliton.ByItems, 0, len(partitionBy))
		for _, defCaus := range partitionBy {
			byItems = append(byItems, &soliton.ByItems{Expr: defCaus, Desc: false})
		}
		sort := &core.PhysicalSort{ByItems: byItems}
		sort.SetChildren(src)
		win.SetChildren(sort)
		tail = sort
	} else {
		win.SetChildren(src)
	}

	var plan core.PhysicalPlan
	if concurrency > 1 {
		byItems := make([]expression.Expression, 0, len(win.PartitionBy))
		for _, item := range win.PartitionBy {
			byItems = append(byItems, item.DefCaus)
		}

		plan = core.PhysicalShuffle{
			Concurrency:  concurrency,
			Tail:         tail,
			DataSource:   src,
			SplitterType: core.PartitionHashSplitterType,
			HashByItems:  byItems,
		}.Init(ctx, nil, 0)
		plan.SetChildren(win)
	} else {
		plan = win
	}

	b := newExecutorBuilder(ctx, nil)
	exec := b.build(plan)
	return exec
}

// windowTestCase has a fixed schemaReplicant (defCaus Double, partitionBy LongLong, rawData VarString(16), defCaus LongLong).
type windowTestCase struct {
	windowFunc       string
	numFunc          int // The number of windowFuncs. Default: 1.
	frame            *core.WindowFrame
	ndv              int // the number of distinct group-by keys
	rows             int
	concurrency      int
	dataSourceSorted bool
	ctx              stochastikctx.Context
	rawDataSmall     string
	defCausumns      []*expression.DeferredCauset // the defCausumns of mock schemaReplicant
}

func (a windowTestCase) String() string {
	return fmt.Sprintf("(func:%v, aggDefCausType:%s, numFunc:%v, ndv:%v, rows:%v, sorted:%v, concurrency:%v)",
		a.windowFunc, a.defCausumns[0].RetType, a.numFunc, a.ndv, a.rows, a.dataSourceSorted, a.concurrency)
}

func defaultWindowTestCase() *windowTestCase {
	ctx := mock.NewContext()
	ctx.GetStochastikVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetStochastikVars().MaxChunkSize = variable.DefMaxChunkSize
	return &windowTestCase{ast.WindowFuncEventNumber, 1, nil, 1000, 10000000, 1, true, ctx, strings.Repeat("x", 16),
		[]*expression.DeferredCauset{
			{Index: 0, RetType: types.NewFieldType(allegrosql.TypeDouble)},
			{Index: 1, RetType: types.NewFieldType(allegrosql.TypeLonglong)},
			{Index: 2, RetType: types.NewFieldType(allegrosql.TypeVarString)},
			{Index: 3, RetType: types.NewFieldType(allegrosql.TypeLonglong)},
		}}
}

func benchmarkWindowExecWithCase(b *testing.B, casTest *windowTestCase) {
	ctx := casTest.ctx
	if err := ctx.GetStochastikVars().SetSystemVar(variable.MilevaDBWindowConcurrency, fmt.Sprintf("%v", casTest.concurrency)); err != nil {
		b.Fatal(err)
	}

	defcaus := casTest.defCausumns
	dataSource := buildMockDataSource(mockDataSourceParameters{
		schemaReplicant: expression.NewSchema(defcaus...),
		ndvs:            []int{0, casTest.ndv, 0, 0},
		orders:          []bool{false, casTest.dataSourceSorted, false, false},
		rows:            casTest.rows,
		ctx:             casTest.ctx,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // prepare a new window-executor
		childDefCauss := casTest.defCausumns
		schemaReplicant := expression.NewSchema(childDefCauss...)
		windowExec := buildWindowExecutor(casTest.ctx, casTest.windowFunc, casTest.numFunc, casTest.frame, dataSource, schemaReplicant, childDefCauss[1:2], casTest.concurrency, casTest.dataSourceSorted)
		tmpCtx := context.Background()
		chk := newFirstChunk(windowExec)
		dataSource.prepareChunks()

		b.StartTimer()
		if err := windowExec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := windowExec.Next(tmpCtx, chk); err != nil {
				b.Fatal(b)
			}
			if chk.NumEvents() == 0 {
				break
			}
		}

		if err := windowExec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkWindowEvents(b *testing.B) {
	b.ReportAllocs()
	rows := []int{1000, 100000}
	ndvs := []int{10, 1000}
	concs := []int{1, 2, 4}
	for _, event := range rows {
		for _, ndv := range ndvs {
			for _, con := range concs {
				cas := defaultWindowTestCase()
				cas.rows = event
				cas.ndv = ndv
				cas.concurrency = con
				cas.dataSourceSorted = false
				cas.windowFunc = ast.WindowFuncEventNumber // cheapest
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkWindowExecWithCase(b, cas)
				})
			}
		}
	}
}

func BenchmarkWindowFunctions(b *testing.B) {
	b.ReportAllocs()
	windowFuncs := []string{
		ast.WindowFuncEventNumber,
		ast.WindowFuncRank,
		ast.WindowFuncDenseRank,
		ast.WindowFuncCumeDist,
		ast.WindowFuncPercentRank,
		ast.WindowFuncNtile,
		ast.WindowFuncLead,
		ast.WindowFuncLag,
		ast.WindowFuncFirstValue,
		ast.WindowFuncLastValue,
		ast.WindowFuncNthValue,
	}
	concs := []int{1, 4}
	for _, windowFunc := range windowFuncs {
		for _, con := range concs {
			cas := defaultWindowTestCase()
			cas.rows = 100000
			cas.ndv = 1000
			cas.concurrency = con
			cas.dataSourceSorted = false
			cas.windowFunc = windowFunc
			b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
				benchmarkWindowExecWithCase(b, cas)
			})
		}
	}
}

func BenchmarkWindowFunctionsWithFrame(b *testing.B) {
	b.ReportAllocs()
	windowFuncs := []string{
		ast.WindowFuncEventNumber,
		ast.AggFuncBitXor,
	}
	numFuncs := []int{1, 5}
	frames := []*core.WindowFrame{
		{Type: ast.Events, Start: &core.FrameBound{UnBounded: true}, End: &core.FrameBound{Type: ast.CurrentEvent}},
	}
	sortTypes := []bool{false, true}
	concs := []int{1, 2, 3, 4, 5, 6}
	for i, windowFunc := range windowFuncs {
		for _, sorted := range sortTypes {
			for _, numFunc := range numFuncs {
				for _, con := range concs {
					cas := defaultWindowTestCase()
					cas.rows = 100000
					cas.ndv = 1000
					cas.concurrency = con
					cas.dataSourceSorted = sorted
					cas.windowFunc = windowFunc
					cas.numFunc = numFunc
					if i < len(frames) {
						cas.frame = frames[i]
					}
					b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
						benchmarkWindowExecWithCase(b, cas)
					})
				}
			}
		}
	}
}

func BenchmarkWindowFunctionsAggWindowProcessorAboutFrame(b *testing.B) {
	b.ReportAllocs()
	windowFunc := ast.AggFuncMax
	frame := &core.WindowFrame{Type: ast.Events, Start: &core.FrameBound{UnBounded: true}, End: &core.FrameBound{UnBounded: true}}
	cas := defaultWindowTestCase()
	cas.rows = 10000
	cas.ndv = 10
	cas.concurrency = 1
	cas.dataSourceSorted = false
	cas.windowFunc = windowFunc
	cas.numFunc = 1
	cas.frame = frame
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkWindowExecWithCase(b, cas)
	})
}

func baseBenchmarkWindowFunctionsWithSlidingWindow(b *testing.B, frameType ast.FrameType) {
	b.ReportAllocs()
	windowFuncs := []struct {
		aggFunc         string
		aggDefCausTypes byte
	}{
		{ast.AggFuncSum, allegrosql.TypeFloat},
		{ast.AggFuncSum, allegrosql.TypeNewDecimal},
		{ast.AggFuncCount, allegrosql.TypeLong},
		{ast.AggFuncAvg, allegrosql.TypeFloat},
		{ast.AggFuncAvg, allegrosql.TypeNewDecimal},
		{ast.AggFuncBitXor, allegrosql.TypeLong},
		{ast.AggFuncMax, allegrosql.TypeLong},
		{ast.AggFuncMax, allegrosql.TypeFloat},
		{ast.AggFuncMin, allegrosql.TypeLong},
		{ast.AggFuncMin, allegrosql.TypeFloat},
	}
	event := 100000
	ndv := 100
	frame := &core.WindowFrame{
		Type:  frameType,
		Start: &core.FrameBound{Type: ast.Preceding, Num: 10},
		End:   &core.FrameBound{Type: ast.Following, Num: 10},
	}
	for _, windowFunc := range windowFuncs {
		cas := defaultWindowTestCase()
		cas.ctx.GetStochastikVars().WindowingUseHighPrecision = false
		cas.rows = event
		cas.ndv = ndv
		cas.windowFunc = windowFunc.aggFunc
		cas.frame = frame
		cas.defCausumns[0].RetType.Tp = windowFunc.aggDefCausTypes
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkWindowExecWithCase(b, cas)
		})
	}
}

func BenchmarkWindowFunctionsWithSlidingWindow(b *testing.B) {
	baseBenchmarkWindowFunctionsWithSlidingWindow(b, ast.Events)
	baseBenchmarkWindowFunctionsWithSlidingWindow(b, ast.Ranges)
}

type hashJoinTestCase struct {
	rows               int
	defcaus            []*types.FieldType
	concurrency        int
	ctx                stochastikctx.Context
	keyIdx             []int
	joinType           core.JoinType
	disk               bool
	useOuterToBuild    bool
	rawData            string
	childrenUsedSchema [][]bool
}

func (tc hashJoinTestCase) defCausumns() []*expression.DeferredCauset {
	ret := make([]*expression.DeferredCauset, 0)
	for i, t := range tc.defcaus {
		defCausumn := &expression.DeferredCauset{Index: i, RetType: t, UniqueID: tc.ctx.GetStochastikVars().AllocPlanDeferredCausetID()}
		ret = append(ret, defCausumn)
	}
	return ret
}

func (tc hashJoinTestCase) String() string {
	return fmt.Sprintf("(rows:%v, defcaus:%v, concurency:%v, joinKeyIdx: %v, disk:%v)",
		tc.rows, tc.defcaus, tc.concurrency, tc.keyIdx, tc.disk)
}

func defaultHashJoinTestCase(defcaus []*types.FieldType, joinType core.JoinType, useOuterToBuild bool) *hashJoinTestCase {
	ctx := mock.NewContext()
	ctx.GetStochastikVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetStochastikVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetStochastikVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	ctx.GetStochastikVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	ctx.GetStochastikVars().SetIndexLookupJoinConcurrency(4)
	tc := &hashJoinTestCase{rows: 100000, concurrency: 4, ctx: ctx, keyIdx: []int{0, 1}, rawData: wideString}
	tc.defcaus = defcaus
	tc.useOuterToBuild = useOuterToBuild
	tc.joinType = joinType
	return tc
}

func prepare4HashJoin(testCase *hashJoinTestCase, innerExec, outerExec Executor) *HashJoinExec {
	if testCase.useOuterToBuild {
		innerExec, outerExec = outerExec, innerExec
	}
	defcaus0 := innerExec.Schema().DeferredCausets
	defcaus1 := outerExec.Schema().DeferredCausets

	joinSchema := expression.NewSchema()
	if testCase.childrenUsedSchema != nil {
		for i, used := range testCase.childrenUsedSchema[0] {
			if used {
				joinSchema.Append(defcaus0[i])
			}
		}
		for i, used := range testCase.childrenUsedSchema[1] {
			if used {
				joinSchema.Append(defcaus1[i])
			}
		}
	} else {
		joinSchema.Append(defcaus0...)
		joinSchema.Append(defcaus1...)
	}

	joinKeys := make([]*expression.DeferredCauset, 0, len(testCase.keyIdx))
	for _, keyIdx := range testCase.keyIdx {
		joinKeys = append(joinKeys, defcaus0[keyIdx])
	}
	probeKeys := make([]*expression.DeferredCauset, 0, len(testCase.keyIdx))
	for _, keyIdx := range testCase.keyIdx {
		probeKeys = append(probeKeys, defcaus1[keyIdx])
	}
	e := &HashJoinExec{
		baseExecutor:      newBaseExecutor(testCase.ctx, joinSchema, 5, innerExec, outerExec),
		concurrency:       uint(testCase.concurrency),
		joinType:          testCase.joinType, // 0 for InnerJoin, 1 for LeftOutersJoin, 2 for RightOuterJoin
		isOuterJoin:       false,
		buildKeys:         joinKeys,
		probeKeys:         probeKeys,
		buildSideExec:     innerExec,
		probeSideExec:     outerExec,
		buildSideEstCount: float64(testCase.rows),
		useOuterToBuild:   testCase.useOuterToBuild,
	}

	childrenUsedSchema := markChildrenUsedDefCauss(e.Schema(), e.children[0].Schema(), e.children[1].Schema())
	defaultValues := make([]types.Causet, e.buildSideExec.Schema().Len())
	lhsTypes, rhsTypes := retTypes(innerExec), retTypes(outerExec)
	e.joiners = make([]joiner, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joiners[i] = newJoiner(testCase.ctx, e.joinType, true, defaultValues,
			nil, lhsTypes, rhsTypes, childrenUsedSchema)
	}
	memLimit := int64(-1)
	if testCase.disk {
		memLimit = 1
	}
	t := memory.NewTracker(-1, memLimit)
	t.SetSuperCowOrNoCausetOnExceed(nil)
	t2 := disk.NewTracker(-1, -1)
	e.ctx.GetStochastikVars().StmtCtx.MemTracker = t
	e.ctx.GetStochastikVars().StmtCtx.DiskTracker = t2
	return e
}

func benchmarkHashJoinExecWithCase(b *testing.B, casTest *hashJoinTestCase) {
	opt1 := mockDataSourceParameters{
		rows: casTest.rows,
		ctx:  casTest.ctx,
		genDataFunc: func(event int, typ *types.FieldType) interface{} {
			switch typ.Tp {
			case allegrosql.TypeLong, allegrosql.TypeLonglong:
				return int64(event)
			case allegrosql.TypeVarString:
				return casTest.rawData
			case allegrosql.TypeDouble:
				return float64(event)
			default:
				panic("not implement")
			}
		},
	}
	opt2 := opt1
	opt1.schemaReplicant = expression.NewSchema(casTest.defCausumns()...)
	opt2.schemaReplicant = expression.NewSchema(casTest.defCausumns()...)
	dataSource1 := buildMockDataSource(opt1)
	dataSource2 := buildMockDataSource(opt2)
	// Test spill result.
	benchmarkHashJoinExec(b, casTest, dataSource1, dataSource2, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkHashJoinExec(b, casTest, dataSource1, dataSource2, false)
	}
}

func benchmarkHashJoinExec(b *testing.B, casTest *hashJoinTestCase, opt1, opt2 *mockDataSource, testResult bool) {
	b.StopTimer()
	exec := prepare4HashJoin(casTest, opt1, opt2)
	tmpCtx := context.Background()
	chk := newFirstChunk(exec)
	opt1.prepareChunks()
	opt2.prepareChunks()

	totalEvent := 0
	b.StartTimer()
	if err := exec.Open(tmpCtx); err != nil {
		b.Fatal(err)
	}
	for {
		if err := exec.Next(tmpCtx, chk); err != nil {
			b.Fatal(err)
		}
		if chk.NumEvents() == 0 {
			break
		}
		totalEvent += chk.NumEvents()
	}

	if testResult {
		time.Sleep(200 * time.Millisecond)
		if spilled := exec.rowContainer.alreadySpilledSafeForTest(); spilled != casTest.disk {
			b.Fatal("wrong usage with disk:", spilled, casTest.disk)
		}
	}

	if err := exec.Close(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if totalEvent == 0 {
		b.Fatal("totalEvent == 0")
	}
}

func BenchmarkHashJoinInlineProjection(b *testing.B) {
	defcaus := []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeVarString),
	}

	b.ReportAllocs()

	{
		cas := defaultHashJoinTestCase(defcaus, 0, false)
		cas.keyIdx = []int{0}
		cas.childrenUsedSchema = [][]bool{
			{false, true},
			{false, false},
		}
		b.Run("InlineProjection:ON", func(b *testing.B) {
			benchmarkHashJoinExecWithCase(b, cas)
		})
	}

	{
		cas := defaultHashJoinTestCase(defcaus, 0, false)
		cas.keyIdx = []int{0}
		b.Run("InlineProjection:OFF", func(b *testing.B) {
			benchmarkHashJoinExecWithCase(b, cas)
		})
	}
}

func BenchmarkHashJoinExec(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	defcaus := []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeVarString),
	}

	b.ReportAllocs()
	cas := defaultHashJoinTestCase(defcaus, 0, false)
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	cas.disk = true
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	// Replace the wide string defCausumn with double defCausumn
	defcaus = []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeDouble),
	}

	cas = defaultHashJoinTestCase(defcaus, 0, false)
	cas.keyIdx = []int{0}
	cas.rows = 5
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas = defaultHashJoinTestCase(defcaus, 0, false)
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})
}

func BenchmarkOuterHashJoinExec(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	defcaus := []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeVarString),
	}

	b.ReportAllocs()
	cas := defaultHashJoinTestCase(defcaus, 2, true)
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	cas.disk = true
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	// Replace the wide string defCausumn with double defCausumn
	defcaus = []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeDouble),
	}

	cas = defaultHashJoinTestCase(defcaus, 2, true)
	cas.keyIdx = []int{0}
	cas.rows = 5
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas = defaultHashJoinTestCase(defcaus, 2, true)
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})
}

func benchmarkBuildHashBlockForList(b *testing.B, casTest *hashJoinTestCase) {
	opt := mockDataSourceParameters{
		schemaReplicant: expression.NewSchema(casTest.defCausumns()...),
		rows:            casTest.rows,
		ctx:             casTest.ctx,
		genDataFunc: func(event int, typ *types.FieldType) interface{} {
			switch typ.Tp {
			case allegrosql.TypeLong, allegrosql.TypeLonglong:
				return int64(event)
			case allegrosql.TypeVarString:
				return casTest.rawData
			default:
				panic("not implement")
			}
		},
	}
	dataSource1 := buildMockDataSource(opt)
	dataSource2 := buildMockDataSource(opt)

	dataSource1.prepareChunks()
	benchmarkBuildHashBlock(b, casTest, dataSource1, dataSource2, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkBuildHashBlock(b, casTest, dataSource1, dataSource2, false)
	}
}

func benchmarkBuildHashBlock(b *testing.B, casTest *hashJoinTestCase, dataSource1, dataSource2 *mockDataSource, testResult bool) {
	b.StopTimer()
	exec := prepare4HashJoin(casTest, dataSource1, dataSource2)
	tmpCtx := context.Background()
	if err := exec.Open(tmpCtx); err != nil {
		b.Fatal(err)
	}
	exec.prepared = true

	innerResultCh := make(chan *chunk.Chunk, len(dataSource1.chunks))
	for _, chk := range dataSource1.chunks {
		innerResultCh <- chk
	}
	close(innerResultCh)

	b.StartTimer()
	if err := exec.buildHashBlockForList(innerResultCh); err != nil {
		b.Fatal(err)
	}

	if testResult {
		time.Sleep(200 * time.Millisecond)
		if exec.rowContainer.alreadySpilledSafeForTest() != casTest.disk {
			b.Fatal("wrong usage with disk")
		}
	}

	if err := exec.Close(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func BenchmarkBuildHashBlockForList(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	defcaus := []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeVarString),
	}

	b.ReportAllocs()
	cas := defaultHashJoinTestCase(defcaus, 0, false)
	rows := []int{10, 100000}
	keyIdxs := [][]int{{0, 1}, {0}}
	disks := []bool{false, true}
	for _, event := range rows {
		for _, keyIdx := range keyIdxs {
			for _, disk := range disks {
				cas.rows = event
				cas.keyIdx = keyIdx
				cas.disk = disk
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkBuildHashBlockForList(b, cas)
				})
			}
		}
	}
}

type indexJoinTestCase struct {
	outerEvents     int
	innerEvents     int
	concurrency     int
	ctx             stochastikctx.Context
	outerJoinKeyIdx []int
	innerJoinKeyIdx []int
	innerIdx        []int
	needOuterSort   bool
	rawData         string
}

func (tc indexJoinTestCase) defCausumns() []*expression.DeferredCauset {
	return []*expression.DeferredCauset{
		{Index: 0, RetType: types.NewFieldType(allegrosql.TypeLonglong)},
		{Index: 1, RetType: types.NewFieldType(allegrosql.TypeDouble)},
		{Index: 2, RetType: types.NewFieldType(allegrosql.TypeVarString)},
	}
}

func defaultIndexJoinTestCase() *indexJoinTestCase {
	ctx := mock.NewContext()
	ctx.GetStochastikVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetStochastikVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetStochastikVars().SnapshotTS = 1
	ctx.GetStochastikVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	ctx.GetStochastikVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	tc := &indexJoinTestCase{
		outerEvents:     100000,
		innerEvents:     variable.DefMaxChunkSize * 100,
		concurrency:     4,
		ctx:             ctx,
		outerJoinKeyIdx: []int{0, 1},
		innerJoinKeyIdx: []int{0, 1},
		innerIdx:        []int{0, 1},
		rawData:         wideString,
	}
	return tc
}

func (tc indexJoinTestCase) String() string {
	return fmt.Sprintf("(outerEvents:%v, innerEvents:%v, concurency:%v, outerJoinKeyIdx: %v, innerJoinKeyIdx: %v, NeedOuterSort:%v)",
		tc.outerEvents, tc.innerEvents, tc.concurrency, tc.outerJoinKeyIdx, tc.innerJoinKeyIdx, tc.needOuterSort)
}
func (tc indexJoinTestCase) getMockDataSourceOptByEvents(rows int) mockDataSourceParameters {
	return mockDataSourceParameters{
		schemaReplicant: expression.NewSchema(tc.defCausumns()...),
		rows:            rows,
		ctx:             tc.ctx,
		genDataFunc: func(event int, typ *types.FieldType) interface{} {
			switch typ.Tp {
			case allegrosql.TypeLong, allegrosql.TypeLonglong:
				return int64(event)
			case allegrosql.TypeDouble:
				return float64(event)
			case allegrosql.TypeVarString:
				return tc.rawData
			default:
				panic("not implement")
			}
		},
	}
}

func prepare4IndexInnerHashJoin(tc *indexJoinTestCase, outerDS *mockDataSource, innerDS *mockDataSource) Executor {
	outerDefCauss, innerDefCauss := tc.defCausumns(), tc.defCausumns()
	joinSchema := expression.NewSchema(outerDefCauss...)
	joinSchema.Append(innerDefCauss...)
	leftTypes, rightTypes := retTypes(outerDS), retTypes(innerDS)
	defaultValues := make([]types.Causet, len(innerDefCauss))
	defCausLens := make([]int, len(innerDefCauss))
	for i := range defCausLens {
		defCausLens[i] = types.UnspecifiedLength
	}
	keyOff2IdxOff := make([]int, len(tc.outerJoinKeyIdx))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = i
	}
	e := &IndexLookUpJoin{
		baseExecutor: newBaseExecutor(tc.ctx, joinSchema, 1, outerDS),
		outerCtx: outerCtx{
			rowTypes:    leftTypes,
			keyDefCauss: tc.outerJoinKeyIdx,
		},
		innerCtx: innerCtx{
			readerBuilder: &dataReaderBuilder{Plan: &mockPhysicalIndexReader{e: innerDS}, executorBuilder: newExecutorBuilder(tc.ctx, nil)},
			rowTypes:      rightTypes,
			defCausLens:   defCausLens,
			keyDefCauss:   tc.innerJoinKeyIdx,
		},
		workerWg:          new(sync.WaitGroup),
		joiner:            newJoiner(tc.ctx, 0, false, defaultValues, nil, leftTypes, rightTypes, nil),
		isOuterJoin:       false,
		keyOff2IdxOff:     keyOff2IdxOff,
		lastDefCausHelper: nil,
	}
	e.joinResult = newFirstChunk(e)
	return e
}

func prepare4IndexOuterHashJoin(tc *indexJoinTestCase, outerDS *mockDataSource, innerDS *mockDataSource) Executor {
	e := prepare4IndexInnerHashJoin(tc, outerDS, innerDS).(*IndexLookUpJoin)
	idxHash := &IndexNestedLoopHashJoin{IndexLookUpJoin: *e}
	concurrency := tc.concurrency
	idxHash.joiners = make([]joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		idxHash.joiners[i] = e.joiner.Clone()
	}
	return idxHash
}

func prepare4IndexMergeJoin(tc *indexJoinTestCase, outerDS *mockDataSource, innerDS *mockDataSource) Executor {
	outerDefCauss, innerDefCauss := tc.defCausumns(), tc.defCausumns()
	joinSchema := expression.NewSchema(outerDefCauss...)
	joinSchema.Append(innerDefCauss...)
	outerJoinKeys := make([]*expression.DeferredCauset, 0, len(tc.outerJoinKeyIdx))
	innerJoinKeys := make([]*expression.DeferredCauset, 0, len(tc.innerJoinKeyIdx))
	for _, keyIdx := range tc.outerJoinKeyIdx {
		outerJoinKeys = append(outerJoinKeys, outerDefCauss[keyIdx])
	}
	for _, keyIdx := range tc.innerJoinKeyIdx {
		innerJoinKeys = append(innerJoinKeys, innerDefCauss[keyIdx])
	}
	leftTypes, rightTypes := retTypes(outerDS), retTypes(innerDS)
	defaultValues := make([]types.Causet, len(innerDefCauss))
	defCausLens := make([]int, len(innerDefCauss))
	for i := range defCausLens {
		defCausLens[i] = types.UnspecifiedLength
	}
	keyOff2IdxOff := make([]int, len(outerJoinKeys))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = i
	}

	compareFuncs := make([]expression.CompareFunc, 0, len(outerJoinKeys))
	outerCompareFuncs := make([]expression.CompareFunc, 0, len(outerJoinKeys))
	for i := range outerJoinKeys {
		compareFuncs = append(compareFuncs, expression.GetCmpFunction(nil, outerJoinKeys[i], innerJoinKeys[i]))
		outerCompareFuncs = append(outerCompareFuncs, expression.GetCmpFunction(nil, outerJoinKeys[i], outerJoinKeys[i]))
	}
	e := &IndexLookUpMergeJoin{
		baseExecutor: newBaseExecutor(tc.ctx, joinSchema, 2, outerDS),
		outerMergeCtx: outerMergeCtx{
			rowTypes:      leftTypes,
			keyDefCauss:   tc.outerJoinKeyIdx,
			joinKeys:      outerJoinKeys,
			needOuterSort: tc.needOuterSort,
			compareFuncs:  outerCompareFuncs,
		},
		innerMergeCtx: innerMergeCtx{
			readerBuilder: &dataReaderBuilder{Plan: &mockPhysicalIndexReader{e: innerDS}, executorBuilder: newExecutorBuilder(tc.ctx, nil)},
			rowTypes:      rightTypes,
			joinKeys:      innerJoinKeys,
			defCausLens:   defCausLens,
			keyDefCauss:   tc.innerJoinKeyIdx,
			compareFuncs:  compareFuncs,
		},
		workerWg:          new(sync.WaitGroup),
		isOuterJoin:       false,
		keyOff2IdxOff:     keyOff2IdxOff,
		lastDefCausHelper: nil,
	}
	concurrency := e.ctx.GetStochastikVars().IndexLookupJoinConcurrency()
	joiners := make([]joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		joiners[i] = newJoiner(tc.ctx, 0, false, defaultValues, nil, leftTypes, rightTypes, nil)
	}
	e.joiners = joiners
	return e
}

type indexJoinType int8

const (
	indexInnerHashJoin indexJoinType = iota
	indexOuterHashJoin
	indexMergeJoin
)

func benchmarHoTTexJoinExecWithCase(
	b *testing.B,
	tc *indexJoinTestCase,
	outerDS *mockDataSource,
	innerDS *mockDataSource,
	execType indexJoinType,
) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var exec Executor
		switch execType {
		case indexInnerHashJoin:
			exec = prepare4IndexInnerHashJoin(tc, outerDS, innerDS)
		case indexOuterHashJoin:
			exec = prepare4IndexOuterHashJoin(tc, outerDS, innerDS)
		case indexMergeJoin:
			exec = prepare4IndexMergeJoin(tc, outerDS, innerDS)
		}

		tmpCtx := context.Background()
		chk := newFirstChunk(exec)
		outerDS.prepareChunks()
		innerDS.prepareChunks()

		b.StartTimer()
		if err := exec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := exec.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumEvents() == 0 {
				break
			}
		}

		if err := exec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarHoTTexJoinExec(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	b.ReportAllocs()
	tc := defaultIndexJoinTestCase()
	outerOpt := tc.getMockDataSourceOptByEvents(tc.outerEvents)
	innerOpt := tc.getMockDataSourceOptByEvents(tc.innerEvents)
	outerDS := buildMockDataSourceWithIndex(outerOpt, tc.innerIdx)
	innerDS := buildMockDataSourceWithIndex(innerOpt, tc.innerIdx)

	tc.needOuterSort = true
	b.Run(fmt.Sprintf("index merge join need outer sort %v", tc), func(b *testing.B) {
		benchmarHoTTexJoinExecWithCase(b, tc, outerDS, innerDS, indexMergeJoin)
	})

	tc.needOuterSort = false
	b.Run(fmt.Sprintf("index merge join %v", tc), func(b *testing.B) {
		benchmarHoTTexJoinExecWithCase(b, tc, outerDS, innerDS, indexMergeJoin)
	})

	b.Run(fmt.Sprintf("index inner hash join %v", tc), func(b *testing.B) {
		benchmarHoTTexJoinExecWithCase(b, tc, outerDS, innerDS, indexInnerHashJoin)
	})

	b.Run(fmt.Sprintf("index outer hash join %v", tc), func(b *testing.B) {
		benchmarHoTTexJoinExecWithCase(b, tc, outerDS, innerDS, indexOuterHashJoin)
	})
}

type mergeJoinTestCase struct {
	indexJoinTestCase
	childrenUsedSchema [][]bool
}

func prepare4MergeJoin(tc *mergeJoinTestCase, leftExec, rightExec *mockDataSource) *MergeJoinExec {
	outerDefCauss, innerDefCauss := tc.defCausumns(), tc.defCausumns()

	joinSchema := expression.NewSchema()
	if tc.childrenUsedSchema != nil {
		for i, used := range tc.childrenUsedSchema[0] {
			if used {
				joinSchema.Append(outerDefCauss[i])
			}
		}
		for i, used := range tc.childrenUsedSchema[1] {
			if used {
				joinSchema.Append(innerDefCauss[i])
			}
		}
	} else {
		joinSchema.Append(outerDefCauss...)
		joinSchema.Append(innerDefCauss...)
	}

	outerJoinKeys := make([]*expression.DeferredCauset, 0, len(tc.outerJoinKeyIdx))
	innerJoinKeys := make([]*expression.DeferredCauset, 0, len(tc.innerJoinKeyIdx))
	for _, keyIdx := range tc.outerJoinKeyIdx {
		outerJoinKeys = append(outerJoinKeys, outerDefCauss[keyIdx])
	}
	for _, keyIdx := range tc.innerJoinKeyIdx {
		innerJoinKeys = append(innerJoinKeys, innerDefCauss[keyIdx])
	}
	compareFuncs := make([]expression.CompareFunc, 0, len(outerJoinKeys))
	outerCompareFuncs := make([]expression.CompareFunc, 0, len(outerJoinKeys))
	for i := range outerJoinKeys {
		compareFuncs = append(compareFuncs, expression.GetCmpFunction(nil, outerJoinKeys[i], innerJoinKeys[i]))
		outerCompareFuncs = append(outerCompareFuncs, expression.GetCmpFunction(nil, outerJoinKeys[i], outerJoinKeys[i]))
	}

	defaultValues := make([]types.Causet, len(innerDefCauss))

	// only benchmark inner join
	e := &MergeJoinExec{
		stmtCtx:      tc.ctx.GetStochastikVars().StmtCtx,
		baseExecutor: newBaseExecutor(tc.ctx, joinSchema, 3, leftExec, rightExec),
		compareFuncs: compareFuncs,
		isOuterJoin:  false,
	}

	e.joiner = newJoiner(
		tc.ctx,
		0,
		false,
		defaultValues,
		nil,
		retTypes(leftExec),
		retTypes(rightExec),
		tc.childrenUsedSchema,
	)

	e.innerBlock = &mergeJoinBlock{
		isInner:    true,
		childIndex: 1,
		joinKeys:   innerJoinKeys,
	}

	e.outerBlock = &mergeJoinBlock{
		childIndex: 0,
		filters:    nil,
		joinKeys:   outerJoinKeys,
	}

	return e
}

func defaultMergeJoinTestCase() *mergeJoinTestCase {
	return &mergeJoinTestCase{*defaultIndexJoinTestCase(), nil}
}

func newMergeJoinBenchmark(numOuterEvents, numInnerDup, numInnerRedundant int) (tc *mergeJoinTestCase, innerDS, outerDS *mockDataSource) {
	ctx := mock.NewContext()
	ctx.GetStochastikVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetStochastikVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetStochastikVars().SnapshotTS = 1
	ctx.GetStochastikVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	ctx.GetStochastikVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)

	numInnerEvents := numOuterEvents*numInnerDup + numInnerRedundant
	itc := &indexJoinTestCase{
		outerEvents:     numOuterEvents,
		innerEvents:     numInnerEvents,
		concurrency:     4,
		ctx:             ctx,
		outerJoinKeyIdx: []int{0, 1},
		innerJoinKeyIdx: []int{0, 1},
		innerIdx:        []int{0, 1},
		rawData:         wideString,
	}
	tc = &mergeJoinTestCase{*itc, nil}
	outerOpt := mockDataSourceParameters{
		schemaReplicant: expression.NewSchema(tc.defCausumns()...),
		rows:            numOuterEvents,
		ctx:             tc.ctx,
		genDataFunc: func(event int, typ *types.FieldType) interface{} {
			switch typ.Tp {
			case allegrosql.TypeLong, allegrosql.TypeLonglong:
				return int64(event)
			case allegrosql.TypeDouble:
				return float64(event)
			case allegrosql.TypeVarString:
				return tc.rawData
			default:
				panic("not implement")
			}
		},
	}

	innerOpt := mockDataSourceParameters{
		schemaReplicant: expression.NewSchema(tc.defCausumns()...),
		rows:            numInnerEvents,
		ctx:             tc.ctx,
		genDataFunc: func(event int, typ *types.FieldType) interface{} {
			event = event / numInnerDup
			switch typ.Tp {
			case allegrosql.TypeLong, allegrosql.TypeLonglong:
				return int64(event)
			case allegrosql.TypeDouble:
				return float64(event)
			case allegrosql.TypeVarString:
				return tc.rawData
			default:
				panic("not implement")
			}
		},
	}

	innerDS = buildMockDataSource(innerOpt)
	outerDS = buildMockDataSource(outerOpt)

	return
}

type mergeJoinType int8

const (
	innerMergeJoin mergeJoinType = iota
)

func benchmarkMergeJoinExecWithCase(b *testing.B, tc *mergeJoinTestCase, innerDS, outerDS *mockDataSource, joinType mergeJoinType) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var exec Executor
		switch joinType {
		case innerMergeJoin:
			exec = prepare4MergeJoin(tc, innerDS, outerDS)
		}

		tmpCtx := context.Background()
		chk := newFirstChunk(exec)
		outerDS.prepareChunks()
		innerDS.prepareChunks()

		b.StartTimer()
		if err := exec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := exec.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumEvents() == 0 {
				break
			}
		}

		if err := exec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkMergeJoinExec(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)
	b.ReportAllocs()

	totalEvents := 300000

	innerDupAndRedundant := [][]int{
		{1, 0},
		{100, 0},
		{10000, 0},
		{1, 30000},
	}

	childrenUsedSchemas := [][][]bool{
		nil,
		{
			{true, false, false},
			{false, true, false},
		},
	}

	for _, params := range innerDupAndRedundant {
		numInnerDup, numInnerRedundant := params[0], params[1]
		for _, childrenUsedSchema := range childrenUsedSchemas {
			tc, innerDS, outerDS := newMergeJoinBenchmark(totalEvents/numInnerDup, numInnerDup, numInnerRedundant)
			inlineProj := false
			if childrenUsedSchema != nil {
				inlineProj = true
				tc.childrenUsedSchema = childrenUsedSchema
			}

			b.Run(fmt.Sprintf("merge join %v InlineProj:%v", tc, inlineProj), func(b *testing.B) {
				benchmarkMergeJoinExecWithCase(b, tc, outerDS, innerDS, innerMergeJoin)
			})
		}
	}
}

type sortCase struct {
	rows       int
	orderByIdx []int
	ndvs       []int
	ctx        stochastikctx.Context
}

func (tc sortCase) defCausumns() []*expression.DeferredCauset {
	return []*expression.DeferredCauset{
		{Index: 0, RetType: types.NewFieldType(allegrosql.TypeLonglong)},
		{Index: 1, RetType: types.NewFieldType(allegrosql.TypeLonglong)},
	}
}

func (tc sortCase) String() string {
	return fmt.Sprintf("(rows:%v, orderBy:%v, ndvs: %v)", tc.rows, tc.orderByIdx, tc.ndvs)
}

func defaultSortTestCase() *sortCase {
	ctx := mock.NewContext()
	ctx.GetStochastikVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetStochastikVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetStochastikVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	tc := &sortCase{rows: 300000, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	return tc
}

func benchmarkSortExec(b *testing.B, cas *sortCase) {
	opt := mockDataSourceParameters{
		schemaReplicant: expression.NewSchema(cas.defCausumns()...),
		rows:            cas.rows,
		ctx:             cas.ctx,
		ndvs:            cas.ndvs,
	}
	dataSource := buildMockDataSource(opt)
	exec := &SortExec{
		baseExecutor:    newBaseExecutor(cas.ctx, dataSource.schemaReplicant, 4, dataSource),
		ByItems:         make([]*soliton.ByItems, 0, len(cas.orderByIdx)),
		schemaReplicant: dataSource.schemaReplicant,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &soliton.ByItems{Expr: cas.defCausumns()[idx]})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpCtx := context.Background()
		chk := newFirstChunk(exec)
		dataSource.prepareChunks()

		b.StartTimer()
		if err := exec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := exec.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumEvents() == 0 {
				break
			}
		}

		if err := exec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkSortExec(b *testing.B) {
	b.ReportAllocs()
	cas := defaultSortTestCase()
	// all random data
	cas.ndvs = []int{0, 0}
	cas.orderByIdx = []int{0, 1}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkSortExec(b, cas)
	})

	ndvs := []int{1, 10000}
	for _, ndv := range ndvs {
		cas.ndvs = []int{ndv, 0}
		cas.orderByIdx = []int{0, 1}
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkSortExec(b, cas)
		})

		cas.ndvs = []int{ndv, 0}
		cas.orderByIdx = []int{0}
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkSortExec(b, cas)
		})

		cas.ndvs = []int{ndv, 0}
		cas.orderByIdx = []int{1}
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkSortExec(b, cas)
		})
	}
}
