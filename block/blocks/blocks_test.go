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

package blocks_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testleak"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	binlog "github.com/whtcorpsinc/fidelpb/go-binlog"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	se          stochastik.Stochastik
}

func (ts *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	causetstore, err := mockstore.NewMockStore()
	c.Check(err, IsNil)
	ts.causetstore = causetstore
	ts.dom, err = stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	ts.se, err = stochastik.CreateStochastik4Test(ts.causetstore)
	c.Assert(err, IsNil)
	ctx := ts.se
	ctx.GetStochaseinstein_dbars().BinlogClient = binloginfo.MockPumpsClient(mockPumpClient{})
	ctx.GetStochaseinstein_dbars().InRestrictedALLEGROSQL = false
}

func (ts *testSuite) TearDownSuite(c *C) {
	ts.dom.Close()
	c.Assert(ts.causetstore.Close(), IsNil)
	testleak.AfterTest(c)()
}

type mockPumpClient struct{}

func (m mockPumpClient) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq, opts ...grpc.CallOption) (*binlog.WriteBinlogResp, error) {
	return &binlog.WriteBinlogResp{}, nil
}

func (m mockPumpClient) PullBinlogs(ctx context.Context, in *binlog.PullBinlogReq, opts ...grpc.CallOption) (binlog.Pump_PullBinlogsClient, error) {
	return nil, nil
}

func (ts *testSuite) TestBasic(c *C) {
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	tb, err := ts.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tb.Meta().ID, Greater, int64(0))
	c.Assert(tb.Meta().Name.L, Equals, "t")
	c.Assert(tb.Meta(), NotNil)
	c.Assert(tb.Indices(), NotNil)
	c.Assert(string(tb.FirstKey()), Not(Equals), "")
	c.Assert(string(tb.IndexPrefix()), Not(Equals), "")
	c.Assert(string(tb.RecordPrefix()), Not(Equals), "")
	c.Assert(blocks.FindIndexByDefCausName(tb, "b"), NotNil)

	autoID, err := block.AllocAutoIncrementValue(context.Background(), tb, ts.se)
	c.Assert(err, IsNil)
	c.Assert(autoID, Greater, int64(0))

	handle, err := blocks.AllocHandle(nil, tb)
	c.Assert(err, IsNil)
	c.Assert(handle.IntValue(), Greater, int64(0))

	ctx := ts.se
	rid, err := tb.AddRecord(ctx, types.MakeCausets(1, "abc"))
	c.Assert(err, IsNil)
	c.Assert(rid.IntValue(), Greater, int64(0))
	event, err := tb.Row(ctx, rid)
	c.Assert(err, IsNil)
	c.Assert(len(event), Equals, 2)
	c.Assert(event[0].GetInt64(), Equals, int64(1))

	_, err = tb.AddRecord(ctx, types.MakeCausets(1, "aba"))
	c.Assert(err, NotNil)
	_, err = tb.AddRecord(ctx, types.MakeCausets(2, "abc"))
	c.Assert(err, NotNil)

	c.Assert(tb.UFIDelateRecord(context.Background(), ctx, rid, types.MakeCausets(1, "abc"), types.MakeCausets(1, "cba"), []bool{false, true}), IsNil)

	tb.IterRecords(ctx, tb.FirstKey(), tb.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		return true, nil
	})

	indexCnt := func() int {
		cnt, err1 := countEntriesWithPrefix(ctx, tb.IndexPrefix())
		c.Assert(err1, IsNil)
		return cnt
	}

	// RowWithDefCauss test
	vals, err := tb.RowWithDefCauss(ctx, ekv.IntHandle(1), tb.DefCauss())
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, 2)
	c.Assert(vals[0].GetInt64(), Equals, int64(1))
	defcaus := []*block.DeferredCauset{tb.DefCauss()[1]}
	vals, err = tb.RowWithDefCauss(ctx, ekv.IntHandle(1), defcaus)
	c.Assert(err, IsNil)
	c.Assert(vals, HasLen, 1)
	c.Assert(vals[0].GetBytes(), DeepEquals, []byte("cba"))

	// Make sure there is index data in the storage.
	c.Assert(indexCnt(), Greater, 0)
	c.Assert(tb.RemoveRecord(ctx, rid, types.MakeCausets(1, "cba")), IsNil)
	// Make sure index data is also removed after tb.RemoveRecord().
	c.Assert(indexCnt(), Equals, 0)
	_, err = tb.AddRecord(ctx, types.MakeCausets(1, "abc"))
	c.Assert(err, IsNil)
	c.Assert(indexCnt(), Greater, 0)
	handle, found, err := tb.Seek(ctx, ekv.IntHandle(0))
	c.Assert(handle.IntValue(), Equals, int64(1))
	c.Assert(found, Equals, true)
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "drop block test.t")
	c.Assert(err, IsNil)

	block.MockBlockFromMeta(tb.Meta())
	alc := tb.SlabPredictors(nil).Get(autoid.RowIDAllocType)
	c.Assert(alc, NotNil)

	err = tb.RebaseAutoID(nil, 0, false, autoid.RowIDAllocType)
	c.Assert(err, IsNil)
}

func countEntriesWithPrefix(ctx stochastikctx.Context, prefix []byte) (int, error) {
	cnt := 0
	txn, err := ctx.Txn(true)
	if err != nil {
		return 0, errors.Trace(err)
	}
	err = soliton.ScanMetaWithPrefix(txn, prefix, func(k ekv.Key, v []byte) bool {
		cnt++
		return true
	})
	return cnt, err
}

func (ts *testSuite) TestTypes(c *C) {
	ctx := context.Background()
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.t (c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 text, c6 blob, c7 varchar(64), c8 time, c9 timestamp null default CURRENT_TIMESTAMP, c10 decimal(10,1))")
	c.Assert(err, IsNil)
	_, err = ts.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "insert test.t values (1, 2, 3, 4, '5', '6', '7', '10:10:10', null, 1.4)")
	c.Assert(err, IsNil)
	rs, err := ts.se.Execute(ctx, "select * from test.t where c1 = 1")
	c.Assert(err, IsNil)
	req := rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	c.Assert(rs[0].Close(), IsNil)
	_, err = ts.se.Execute(ctx, "drop block test.t")
	c.Assert(err, IsNil)

	_, err = ts.se.Execute(ctx, "CREATE TABLE test.t (c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned, c5 double, c6 bit(8))")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "insert test.t values (1, 2, 3, 4, 5, 6)")
	c.Assert(err, IsNil)
	rs, err = ts.se.Execute(ctx, "select * from test.t where c1 = 1")
	c.Assert(err, IsNil)
	req = rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	event := req.GetRow(0)
	c.Assert(types.BinaryLiteral(event.GetBytes(5)), DeepEquals, types.NewBinaryLiteralFromUint(6, -1))
	c.Assert(rs[0].Close(), IsNil)
	_, err = ts.se.Execute(ctx, "drop block test.t")
	c.Assert(err, IsNil)

	_, err = ts.se.Execute(ctx, "CREATE TABLE test.t (c1 enum('a', 'b', 'c'))")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "insert test.t values ('a'), (2), ('c')")
	c.Assert(err, IsNil)
	rs, err = ts.se.Execute(ctx, "select c1 + 1 from test.t where c1 = 1")
	c.Assert(err, IsNil)
	req = rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	c.Assert(req.GetRow(0).GetFloat64(0), DeepEquals, float64(2))
	c.Assert(rs[0].Close(), IsNil)
	_, err = ts.se.Execute(ctx, "drop block test.t")
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestUniqueIndexMultipleNullEntries(c *C) {
	ctx := context.Background()
	_, err := ts.se.Execute(ctx, "drop block if exists test.t")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(ctx, "CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(err, IsNil)
	tb, err := ts.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tb.Meta().ID, Greater, int64(0))
	c.Assert(tb.Meta().Name.L, Equals, "t")
	c.Assert(tb.Meta(), NotNil)
	c.Assert(tb.Indices(), NotNil)
	c.Assert(string(tb.FirstKey()), Not(Equals), "")
	c.Assert(string(tb.IndexPrefix()), Not(Equals), "")
	c.Assert(string(tb.RecordPrefix()), Not(Equals), "")
	c.Assert(blocks.FindIndexByDefCausName(tb, "b"), NotNil)

	handle, err := blocks.AllocHandle(nil, tb)
	c.Assert(err, IsNil)
	c.Assert(handle.IntValue(), Greater, int64(0))

	autoid, err := block.AllocAutoIncrementValue(context.Background(), tb, ts.se)
	c.Assert(err, IsNil)
	c.Assert(autoid, Greater, int64(0))

	sctx := ts.se
	c.Assert(sctx.NewTxn(ctx), IsNil)
	_, err = tb.AddRecord(sctx, types.MakeCausets(1, nil))
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(sctx, types.MakeCausets(2, nil))
	c.Assert(err, IsNil)
	txn, err := sctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Rollback(), IsNil)
	_, err = ts.se.Execute(context.Background(), "drop block test.t")
	c.Assert(err, IsNil)
}

func (ts *testSuite) TestRowKeyCodec(c *C) {
	blockVal := []struct {
		blockID int64
		h       int64
		ID      int64
	}{
		{1, 1234567890, 0},
		{2, 1, 0},
		{3, -1, 0},
		{4, -1, 1},
	}

	for _, t := range blockVal {
		b := blockcodec.EncodeRowKeyWithHandle(t.blockID, ekv.IntHandle(t.h))
		blockID, handle, err := blockcodec.DecodeRecordKey(b)
		c.Assert(err, IsNil)
		c.Assert(blockID, Equals, t.blockID)
		c.Assert(handle.IntValue(), Equals, t.h)

		handle, err = blockcodec.DecodeRowKey(b)
		c.Assert(err, IsNil)
		c.Assert(handle.IntValue(), Equals, t.h)
	}

	// test error
	tbl := []string{
		"",
		"x",
		"t1",
		"t12345678",
		"t12345678_i",
		"t12345678_r1",
		"t12345678_r1234567",
	}

	for _, t := range tbl {
		_, err := blockcodec.DecodeRowKey(ekv.Key(t))
		c.Assert(err, NotNil)
	}
}

func (ts *testSuite) TestUnsignedPK(c *C) {
	ts.se.Execute(context.Background(), "DROP TABLE IF EXISTS test.tPK")
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.tPK (a bigint unsigned primary key, b varchar(255))")
	c.Assert(err, IsNil)
	tb, err := ts.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("tPK"))
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	rid, err := tb.AddRecord(ts.se, types.MakeCausets(1, "abc"))
	c.Assert(err, IsNil)
	event, err := tb.Row(ts.se, rid)
	c.Assert(err, IsNil)
	c.Assert(len(event), Equals, 2)
	c.Assert(event[0].HoTT(), Equals, types.HoTTUint64)
	ts.se.StmtCommit()
	txn, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
}

func (ts *testSuite) TestIterRecords(c *C) {
	ts.se.Execute(context.Background(), "DROP TABLE IF EXISTS test.tIter")
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.tIter (a int primary key, b int)")
	c.Assert(err, IsNil)
	_, err = ts.se.Execute(context.Background(), "INSERT test.tIter VALUES (-1, 2), (2, NULL)")
	c.Assert(err, IsNil)
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	tb, err := ts.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("tIter"))
	c.Assert(err, IsNil)
	totalCount := 0
	err = tb.IterRecords(ts.se, tb.FirstKey(), tb.DefCauss(), func(_ ekv.Handle, rec []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		totalCount++
		c.Assert(rec[0].IsNull(), IsFalse)
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(totalCount, Equals, 2)
	txn, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
}

func (ts *testSuite) TestBlockFromMeta(c *C) {
	tk := testkit.NewTestKitWithInit(c, ts.causetstore)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE meta (a int primary key auto_increment, b varchar(255) unique)")
	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	tb, err := ts.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("meta"))
	c.Assert(err, IsNil)
	tbInfo := tb.Meta()

	// For test coverage
	tbInfo.DeferredCausets[0].GeneratedExprString = "a"
	blocks.BlockFromMeta(nil, tbInfo)

	tbInfo.DeferredCausets[0].GeneratedExprString = "test"
	blocks.BlockFromMeta(nil, tbInfo)
	tbInfo.DeferredCausets[0].State = perceptron.StateNone
	tb, err = blocks.BlockFromMeta(nil, tbInfo)
	c.Assert(tb, IsNil)
	c.Assert(err, NotNil)
	tbInfo.State = perceptron.StateNone
	tb, err = blocks.BlockFromMeta(nil, tbInfo)
	c.Assert(tb, IsNil)
	c.Assert(err, NotNil)

	tk.MustExec(`create block t_mock (id int) partition by range (id) (partition p0 values less than maxvalue)`)
	tb, err = ts.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t_mock"))
	c.Assert(err, IsNil)
	t := block.MockBlockFromMeta(tb.Meta())
	_, ok := t.(block.PartitionedBlock)
	c.Assert(ok, IsTrue)
	tk.MustExec("drop block t_mock")
	c.Assert(t.Type(), Equals, block.NormalBlock)

	tk.MustExec("create block t_meta (a int) shard_row_id_bits = 15")
	tb, err = petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t_meta"))
	c.Assert(err, IsNil)
	_, err = blocks.AllocHandle(tk.Se, tb)
	c.Assert(err, IsNil)

	maxID := 1<<(64-15-1) - 1
	err = tb.RebaseAutoID(tk.Se, int64(maxID), false, autoid.RowIDAllocType)
	c.Assert(err, IsNil)

	_, err = blocks.AllocHandle(tk.Se, tb)
	c.Assert(err, NotNil)
}

func (ts *testSuite) TestShardRowIDBitsStep(c *C) {
	tk := testkit.NewTestKit(c, ts.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists shard_t;")
	tk.MustExec("create block shard_t (a int) shard_row_id_bits = 15;")
	tk.MustExec("set @@milevadb_shard_allocate_step=3;")
	tk.MustExec("insert into shard_t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11);")
	rows := tk.MustQuery("select _milevadb_rowid from shard_t;").Rows()
	shards := make(map[int]struct{})
	for _, event := range rows {
		id, err := strconv.ParseUint(event[0].(string), 10, 64)
		c.Assert(err, IsNil)
		shards[int(id>>48)] = struct{}{}
	}
	c.Assert(len(shards), Equals, 4)
}

func (ts *testSuite) TestHiddenDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, ts.causetstore)
	tk.MustExec("DROP DATABASE IF EXISTS test_hidden;")
	tk.MustExec("CREATE DATABASE test_hidden;")
	tk.MustExec("USE test_hidden;")
	tk.MustExec("CREATE TABLE t (a int primary key, b int as (a+1), c int, d int as (c+1) stored, e int, f tinyint as (a+1));")
	tk.MustExec("insert into t values (1, default, 3, default, 5, default);")
	tb, err := ts.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test_hidden"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	defCausInfo := tb.Meta().DeferredCausets
	// Set defCausumn b, d, f to hidden
	defCausInfo[1].Hidden = true
	defCausInfo[3].Hidden = true
	defCausInfo[5].Hidden = true
	tc := tb.(*blocks.BlockCommon)
	// Reset related caches
	tc.VisibleDeferredCausets = nil
	tc.WriblockDeferredCausets = nil
	tc.HiddenDeferredCausets = nil
	tc.FullHiddenDefCaussAndVisibleDeferredCausets = nil

	// Basic test
	defcaus := tb.VisibleDefCauss()
	c.Assert(block.FindDefCaus(defcaus, "a"), NotNil)
	c.Assert(block.FindDefCaus(defcaus, "b"), IsNil)
	c.Assert(block.FindDefCaus(defcaus, "c"), NotNil)
	c.Assert(block.FindDefCaus(defcaus, "d"), IsNil)
	c.Assert(block.FindDefCaus(defcaus, "e"), NotNil)
	hiddenDefCauss := tb.HiddenDefCauss()
	c.Assert(block.FindDefCaus(hiddenDefCauss, "a"), IsNil)
	c.Assert(block.FindDefCaus(hiddenDefCauss, "b"), NotNil)
	c.Assert(block.FindDefCaus(hiddenDefCauss, "c"), IsNil)
	c.Assert(block.FindDefCaus(hiddenDefCauss, "d"), NotNil)
	c.Assert(block.FindDefCaus(hiddenDefCauss, "e"), IsNil)
	defCausInfo[1].State = perceptron.StateDeleteOnly
	defCausInfo[2].State = perceptron.StateDeleteOnly
	fullHiddenDefCaussAndVisibleDeferredCausets := tb.FullHiddenDefCaussAndVisibleDefCauss()
	c.Assert(block.FindDefCaus(fullHiddenDefCaussAndVisibleDeferredCausets, "a"), NotNil)
	c.Assert(block.FindDefCaus(fullHiddenDefCaussAndVisibleDeferredCausets, "b"), NotNil)
	c.Assert(block.FindDefCaus(fullHiddenDefCaussAndVisibleDeferredCausets, "c"), IsNil)
	c.Assert(block.FindDefCaus(fullHiddenDefCaussAndVisibleDeferredCausets, "d"), NotNil)
	c.Assert(block.FindDefCaus(fullHiddenDefCaussAndVisibleDeferredCausets, "e"), NotNil)
	// Reset schemaReplicant states.
	defCausInfo[1].State = perceptron.StatePublic
	defCausInfo[2].State = perceptron.StatePublic

	// Test show create block
	tk.MustQuery("show create block t;").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `e` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test show (extended) defCausumns
	tk.MustQuery("show defCausumns from t").Check(solitonutil.RowsWithSep("|",
		"a|int(11)|NO|PRI|<nil>|",
		"c|int(11)|YES||<nil>|",
		"e|int(11)|YES||<nil>|"))
	tk.MustQuery("show extended defCausumns from t").Check(solitonutil.RowsWithSep("|",
		"a|int(11)|NO|PRI|<nil>|",
		"b|int(11)|YES||<nil>|VIRTUAL GENERATED",
		"c|int(11)|YES||<nil>|",
		"d|int(11)|YES||<nil>|STORED GENERATED",
		"e|int(11)|YES||<nil>|",
		"f|tinyint(4)|YES||<nil>|VIRTUAL GENERATED"))

	// `SELECT` statement
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))
	tk.MustQuery("select a, c, e from t;").Check(testkit.Rows("1 3 5"))

	// Can't use hidden defCausumns in `SELECT` statement
	tk.MustGetErrMsg("select b from t;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("select b+1 from t;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("select b, c from t;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("select a, d from t;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("select d, b from t;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("select * from t where b > 1;", "[planner:1054]Unknown defCausumn 'b' in 'where clause'")
	tk.MustGetErrMsg("select * from t order by b;", "[planner:1054]Unknown defCausumn 'b' in 'order clause'")
	tk.MustGetErrMsg("select * from t group by b;", "[planner:1054]Unknown defCausumn 'b' in 'group statement'")

	// Can't use hidden defCausumns in `INSERT` statement
	// 1. insert into ... values ...
	tk.MustGetErrMsg("insert into t values (1, 2, 3, 4, 5, 6);", "[planner:1136]DeferredCauset count doesn't match value count at event 1")
	tk.MustGetErrMsg("insert into t(b) values (2)", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t(b, c) values (2, 3);", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t(a, d) values (1, 4);", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t(d, b) values (4, 2);", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t(a) values (b);", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t(a) values (d+1);", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	// 2. insert into ... set ...
	tk.MustGetErrMsg("insert into t set b = 2;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set b = 2, c = 3;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1, d = 4;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set d = 4, b = 2;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = b;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = d + 1;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	// 3. insert into ... on duplicated key uFIDelate ...
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key uFIDelate b = 2;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key uFIDelate b = 2, c = 3;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key uFIDelate c = 3, d = 4;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key uFIDelate d = 4, b = 2;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key uFIDelate c = b;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key uFIDelate c = d + 1;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	// 4. replace into ... set ...
	tk.MustGetErrMsg("replace into t set a = 1, b = 2;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, b = 2, c = 3;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, d = 4;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, d = 4, b = 2;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, c = b;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, c = d + 1;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	// 5. insert into ... select ...
	tk.MustExec("create block t1(a int, b int, c int, d int);")
	tk.MustGetErrMsg("insert into t1 select b from t;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select b+1 from t;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select b, c from t;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select a, d from t;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select d, b from t;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select a from t where b > 1;", "[planner:1054]Unknown defCausumn 'b' in 'where clause'")
	tk.MustGetErrMsg("insert into t1 select a from t order by b;", "[planner:1054]Unknown defCausumn 'b' in 'order clause'")
	tk.MustGetErrMsg("insert into t1 select a from t group by b;", "[planner:1054]Unknown defCausumn 'b' in 'group statement'")
	tk.MustExec("drop block t1")

	// `UFIDelATE` statement
	tk.MustGetErrMsg("uFIDelate t set b = 2;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("uFIDelate t set b = 2, c = 3;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("uFIDelate t set a = 1, d = 4;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")

	// FIXME: This allegrosql return unknown defCausumn 'd' in MyALLEGROSQL
	tk.MustGetErrMsg("uFIDelate t set d = 4, b = 2;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")

	tk.MustGetErrMsg("uFIDelate t set a = b;", "[planner:1054]Unknown defCausumn 'b' in 'field list'")
	tk.MustGetErrMsg("uFIDelate t set a = d + 1;", "[planner:1054]Unknown defCausumn 'd' in 'field list'")
	tk.MustGetErrMsg("uFIDelate t set a=1 where b=1;", "[planner:1054]Unknown defCausumn 'b' in 'where clause'")
	tk.MustGetErrMsg("uFIDelate t set a=1 where c=3 order by b;", "[planner:1054]Unknown defCausumn 'b' in 'order clause'")

	// `DELETE` statement
	tk.MustExec("delete from t;")
	tk.MustQuery("select count(*) from t;").Check(testkit.Rows("0"))
	tk.MustExec("insert into t values (1, 3, 5);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))
	tk.MustGetErrMsg("delete from t where b = 1;", "[planner:1054]Unknown defCausumn 'b' in 'where clause'")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))
	tk.MustGetErrMsg("delete from t order by d = 1;", "[planner:1054]Unknown defCausumn 'd' in 'order clause'")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))

	// `DROP COLUMN` statement
	tk.MustGetErrMsg("ALTER TABLE t DROP COLUMN b;", "[dbs:1091]defCausumn b doesn't exist")
	tk.MustQuery("show create block t;").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `e` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show extended defCausumns from t").Check(solitonutil.RowsWithSep("|",
		"a|int(11)|NO|PRI|<nil>|",
		"b|int(11)|YES||<nil>|VIRTUAL GENERATED",
		"c|int(11)|YES||<nil>|",
		"d|int(11)|YES||<nil>|STORED GENERATED",
		"e|int(11)|YES||<nil>|",
		"f|tinyint(4)|YES||<nil>|VIRTUAL GENERATED"))
}

func (ts *testSuite) TestAddRecordWithCtx(c *C) {
	ts.se.Execute(context.Background(), "DROP TABLE IF EXISTS test.tRecord")
	_, err := ts.se.Execute(context.Background(), "CREATE TABLE test.tRecord (a bigint unsigned primary key, b varchar(255))")
	c.Assert(err, IsNil)
	tb, err := ts.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("tRecord"))
	c.Assert(err, IsNil)
	defer ts.se.Execute(context.Background(), "DROP TABLE test.tRecord")

	c.Assert(ts.se.NewTxn(context.Background()), IsNil)
	_, err = ts.se.Txn(true)
	c.Assert(err, IsNil)
	recordCtx := blocks.NewCommonAddRecordCtx(len(tb.DefCauss()))
	blocks.SetAddRecordCtx(ts.se, recordCtx)
	defer blocks.ClearAddRecordCtx(ts.se)

	records := [][]types.Causet{types.MakeCausets(uint64(1), "abc"), types.MakeCausets(uint64(2), "abcd")}
	for _, r := range records {
		rid, err := tb.AddRecord(ts.se, r)
		c.Assert(err, IsNil)
		event, err := tb.Row(ts.se, rid)
		c.Assert(err, IsNil)
		c.Assert(len(event), Equals, len(r))
		c.Assert(event[0].HoTT(), Equals, types.HoTTUint64)
	}

	i := 0
	err = tb.IterRecords(ts.se, tb.FirstKey(), tb.DefCauss(), func(_ ekv.Handle, rec []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, len(records))

	ts.se.StmtCommit()
	txn, err := ts.se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
}
