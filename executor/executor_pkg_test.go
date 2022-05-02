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
	"crypto/tls"

	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	plannerutil "github.com/whtcorpsinc/MilevaDB-Prod/planner/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/ranger"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
)

var _ = Suite(&testExecSuite{})
var _ = SerialSuites(&testExecSerialSuite{})

// Note: it's a tricky way to export the `inspectionSummaryMemrules` and `inspectionMemrules` for unit test but invisible for normal code
var (
	InspectionSummaryMemrules = inspectionSummaryMemrules
	InspectionMemrules        = inspectionMemrules
)

type testExecSuite struct {
}

type testExecSerialSuite struct {
}

// mockStochastikManager is a mocked stochastik manager which is used for test.
type mockStochastikManager struct {
	PS []*soliton.ProcessInfo
}

// ShowProcessList implements the StochastikManager.ShowProcessList interface.
func (msm *mockStochastikManager) ShowProcessList() map[uint64]*soliton.ProcessInfo {
	ret := make(map[uint64]*soliton.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockStochastikManager) GetProcessInfo(id uint64) (*soliton.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &soliton.ProcessInfo{}, false
}

// Kill implements the StochastikManager.Kill interface.
func (msm *mockStochastikManager) Kill(cid uint64, query bool) {

}

func (msm *mockStochastikManager) UFIDelateTLSConfig(cfg *tls.Config) {
}

func (s *testExecSuite) TestShowProcessList(c *C) {
	// Compose schemaReplicant.
	names := []string{"Id", "User", "Host", "EDB", "Command", "Time", "State", "Info"}
	ftypes := []byte{allegrosql.TypeLonglong, allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		allegrosql.TypeVarchar, allegrosql.TypeVarchar, allegrosql.TypeLong, allegrosql.TypeVarchar, allegrosql.TypeString}
	schemaReplicant := buildSchema(names, ftypes)

	// Compose a mocked stochastik manager.
	ps := make([]*soliton.ProcessInfo, 0, 1)
	pi := &soliton.ProcessInfo{
		ID:      0,
		User:    "test",
		Host:    "127.0.0.1",
		EDB:     "test",
		Command: 't',
		State:   1,
		Info:    "",
	}
	ps = append(ps, pi)
	sm := &mockStochastikManager{
		PS: ps,
	}
	sctx := mock.NewContext()
	sctx.SetStochastikManager(sm)
	sctx.GetStochaseinstein_dbars().User = &auth.UserIdentity{Username: "test"}

	// Compose executor.
	e := &ShowExec{
		baseExecutor: newBaseExecutor(sctx, schemaReplicant, 0),
		Tp:           ast.ShowProcessList,
	}

	ctx := context.Background()
	err := e.Open(ctx)
	c.Assert(err, IsNil)

	chk := newFirstChunk(e)
	it := chunk.NewIterator4Chunk(chk)
	// Run test and check results.
	for _, p := range ps {
		err = e.Next(context.Background(), chk)
		c.Assert(err, IsNil)
		for event := it.Begin(); event != it.End(); event = it.Next() {
			c.Assert(event.GetUint64(0), Equals, p.ID)
		}
	}
	err = e.Next(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumEvents(), Equals, 0)
	err = e.Close()
	c.Assert(err, IsNil)
}

func buildSchema(names []string, ftypes []byte) *expression.Schema {
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(names))...)
	for i := range names {
		defCaus := &expression.DeferredCauset{
			UniqueID: int64(i),
		}
		// User varchar as the default return defCausumn type.
		tp := allegrosql.TypeVarchar
		if len(ftypes) != 0 && ftypes[0] != allegrosql.TypeUnspecified {
			tp = ftypes[0]
		}
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = allegrosql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.DefCauslate = types.DefaultCharsetForType(tp)
		defCaus.RetType = fieldType
		schemaReplicant.Append(defCaus)
	}
	return schemaReplicant
}

func (s *testExecSuite) TestBuildKvRangesForIndexJoinWithoutCwc(c *C) {
	indexRanges := make([]*ranger.Range, 0, 6)
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 2))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 3, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 2, 1, 1))

	joinKeyEvents := make([]*indexJoinLookUpContent, 0, 5)
	joinKeyEvents = append(joinKeyEvents, &indexJoinLookUpContent{keys: generateCausetSlice(1, 1)})
	joinKeyEvents = append(joinKeyEvents, &indexJoinLookUpContent{keys: generateCausetSlice(1, 2)})
	joinKeyEvents = append(joinKeyEvents, &indexJoinLookUpContent{keys: generateCausetSlice(2, 1)})
	joinKeyEvents = append(joinKeyEvents, &indexJoinLookUpContent{keys: generateCausetSlice(2, 2)})
	joinKeyEvents = append(joinKeyEvents, &indexJoinLookUpContent{keys: generateCausetSlice(2, 3)})

	keyOff2IdxOff := []int{1, 3}
	ctx := mock.NewContext()
	kvRanges, err := buildKvRangesForIndexJoin(ctx, 0, 0, joinKeyEvents, indexRanges, keyOff2IdxOff, nil)
	c.Assert(err, IsNil)
	// Check the kvRanges is in order.
	for i, kvRange := range kvRanges {
		c.Assert(kvRange.StartKey.Cmp(kvRange.EndKey) < 0, IsTrue)
		if i > 0 {
			c.Assert(kvRange.StartKey.Cmp(kvRanges[i-1].EndKey) >= 0, IsTrue)
		}
	}
}

func generateIndexRange(vals ...int64) *ranger.Range {
	lowCausets := generateCausetSlice(vals...)
	highCausets := make([]types.Causet, len(vals))
	INTERLOCKy(highCausets, lowCausets)
	return &ranger.Range{LowVal: lowCausets, HighVal: highCausets}
}

func generateCausetSlice(vals ...int64) []types.Causet {
	datums := make([]types.Causet, len(vals))
	for i, val := range vals {
		datums[i].SetInt64(val)
	}
	return datums
}

func (s *testExecSuite) TestGetFieldsFromLine(c *C) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			`"1","a string","100.20"`,
			[]string{"1", "a string", "100.20"},
		},
		{
			`"2","a string containing a , comma","102.20"`,
			[]string{"2", "a string containing a , comma", "102.20"},
		},
		{
			`"3","a string containing a \" quote","102.20"`,
			[]string{"3", "a string containing a \" quote", "102.20"},
		},
		{
			`"4","a string containing a \", quote and comma","102.20"`,
			[]string{"4", "a string containing a \", quote and comma", "102.20"},
		},
		// Test some escape char.
		{
			`"\0\b\n\r\t\Z\\\  \c\'\""`,
			[]string{string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'})},
		},
		// Test mixed.
		{
			`"123",456,"\t7890",abcd`,
			[]string{"123", "456", "\t7890", "abcd"},
		},
	}

	ldInfo := LoadDataInfo{
		FieldsInfo: &ast.FieldsClause{
			Enclosed:   '"',
			Terminated: ",",
		},
	}

	for _, test := range tests {
		got, err := ldInfo.getFieldsFromLine([]byte(test.input))
		c.Assert(err, IsNil, Commentf("failed: %s", test.input))
		assertEqualStrings(c, got, test.expected)
	}

	_, err := ldInfo.getFieldsFromLine([]byte(`1,a string,100.20`))
	c.Assert(err, IsNil)
}

func assertEqualStrings(c *C, got []field, expect []string) {
	c.Assert(len(got), Equals, len(expect))
	for i := 0; i < len(got); i++ {
		c.Assert(string(got[i].str), Equals, expect[i])
	}
}

func (s *testExecSerialSuite) TestSortSpillDisk(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
		conf.MemQuotaQuery = 1
	})
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testSortedEventContainerSpill", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testSortedEventContainerSpill"), IsNil)
	}()
	ctx := mock.NewContext()
	ctx.GetStochaseinstein_dbars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetStochaseinstein_dbars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	cas := &sortCase{rows: 2048, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	opt := mockDataSourceParameters{
		schemaReplicant: expression.NewSchema(cas.defCausumns()...),
		rows:            cas.rows,
		ctx:             cas.ctx,
		ndvs:            cas.ndvs,
	}
	dataSource := buildMockDataSource(opt)
	exec := &SortExec{
		baseExecutor:    newBaseExecutor(cas.ctx, dataSource.schemaReplicant, 0, dataSource),
		ByItems:         make([]*plannerutil.ByItems, 0, len(cas.orderByIdx)),
		schemaReplicant: dataSource.schemaReplicant,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &plannerutil.ByItems{Expr: cas.defCausumns()[idx]})
	}
	tmpCtx := context.Background()
	chk := newFirstChunk(exec)
	dataSource.prepareChunks()
	err := exec.Open(tmpCtx)
	c.Assert(err, IsNil)
	for {
		err = exec.Next(tmpCtx, chk)
		c.Assert(err, IsNil)
		if chk.NumEvents() == 0 {
			break
		}
	}
	// Test only 1 partition and all data in memory.
	c.Assert(len(exec.partitionList), Equals, 1)
	c.Assert(exec.partitionList[0].AlreadySpilledSafeForTest(), Equals, false)
	c.Assert(exec.partitionList[0].NumEvent(), Equals, 2048)
	err = exec.Close()
	c.Assert(err, IsNil)

	ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker = memory.NewTracker(-1, 1)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	c.Assert(err, IsNil)
	for {
		err = exec.Next(tmpCtx, chk)
		c.Assert(err, IsNil)
		if chk.NumEvents() == 0 {
			break
		}
	}
	// Test 2 partitions and all data in disk.
	// Now spilling is in parallel.
	// Maybe the second add() will called before spilling, depends on
	// Golang goroutine scheduling. So the result has two possibilities.
	if len(exec.partitionList) == 2 {
		c.Assert(len(exec.partitionList), Equals, 2)
		c.Assert(exec.partitionList[0].AlreadySpilledSafeForTest(), Equals, true)
		c.Assert(exec.partitionList[1].AlreadySpilledSafeForTest(), Equals, true)
		c.Assert(exec.partitionList[0].NumEvent(), Equals, 1024)
		c.Assert(exec.partitionList[1].NumEvent(), Equals, 1024)
	} else {
		c.Assert(len(exec.partitionList), Equals, 1)
		c.Assert(exec.partitionList[0].AlreadySpilledSafeForTest(), Equals, true)
		c.Assert(exec.partitionList[0].NumEvent(), Equals, 2048)
	}

	err = exec.Close()
	c.Assert(err, IsNil)

	ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker = memory.NewTracker(-1, 24000)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	c.Assert(err, IsNil)
	for {
		err = exec.Next(tmpCtx, chk)
		c.Assert(err, IsNil)
		if chk.NumEvents() == 0 {
			break
		}
	}
	// Test only 1 partition but spill disk.
	c.Assert(len(exec.partitionList), Equals, 1)
	c.Assert(exec.partitionList[0].AlreadySpilledSafeForTest(), Equals, true)
	c.Assert(exec.partitionList[0].NumEvent(), Equals, 2048)
	err = exec.Close()
	c.Assert(err, IsNil)

	// Test partition nums.
	ctx = mock.NewContext()
	ctx.GetStochaseinstein_dbars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetStochaseinstein_dbars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker = memory.NewTracker(-1, 16864*50)
	ctx.GetStochaseinstein_dbars().StmtCtx.MemTracker.Consume(16864 * 45)
	cas = &sortCase{rows: 20480, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	opt = mockDataSourceParameters{
		schemaReplicant: expression.NewSchema(cas.defCausumns()...),
		rows:            cas.rows,
		ctx:             cas.ctx,
		ndvs:            cas.ndvs,
	}
	dataSource = buildMockDataSource(opt)
	exec = &SortExec{
		baseExecutor:    newBaseExecutor(cas.ctx, dataSource.schemaReplicant, 0, dataSource),
		ByItems:         make([]*plannerutil.ByItems, 0, len(cas.orderByIdx)),
		schemaReplicant: dataSource.schemaReplicant,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &plannerutil.ByItems{Expr: cas.defCausumns()[idx]})
	}
	tmpCtx = context.Background()
	chk = newFirstChunk(exec)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	c.Assert(err, IsNil)
	for {
		err = exec.Next(tmpCtx, chk)
		c.Assert(err, IsNil)
		if chk.NumEvents() == 0 {
			break
		}
	}
	// Don't spill too many partitions.
	c.Assert(len(exec.partitionList) <= 4, IsTrue)
	err = exec.Close()
	c.Assert(err, IsNil)
}
