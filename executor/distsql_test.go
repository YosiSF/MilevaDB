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

package executor_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime/pprof"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/types"
)

func checkGoroutineExists(keyword string) bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	profile.WriteTo(buf, 1)
	str := buf.String()
	return strings.Contains(str, keyword)
}

func (s *testSuite3) TestINTERLOCKClientSend(c *C) {
	c.Skip("not sblock")
	if _, ok := s.causetstore.GetClient().(*einsteindb.INTERLOCKClient); !ok {
		// Make sure the causetstore is einsteindb causetstore.
		return
	}
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block INTERLOCKclient (id int primary key)")

	// Insert 1000 rows.
	var values []string
	for i := 0; i < 1000; i++ {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert INTERLOCKclient values " + strings.Join(values, ","))

	// Get block ID for split.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("INTERLOCKclient"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the block.
	s.cluster.SplitBlock(tblID, 100)

	ctx := context.Background()
	// Send interlock request when the block split.
	rs, err := tk.Exec("select sum(id) from INTERLOCKclient")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.GetEvent(0).GetMyDecimal(0).String(), Equals, "499500")
	rs.Close()

	// Split one region.
	key := blockcodec.EncodeEventKeyWithHandle(tblID, ekv.IntHandle(500))
	region, _ := s.cluster.GetRegionByKey(key)
	peerID := s.cluster.AllocID()
	s.cluster.Split(region.GetId(), s.cluster.AllocID(), key, []uint64{peerID}, peerID)

	// Check again.
	rs, err = tk.Exec("select sum(id) from INTERLOCKclient")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.GetEvent(0).GetMyDecimal(0).String(), Equals, "499500")
	rs.Close()

	// Check there is no goroutine leak.
	rs, err = tk.Exec("select * from INTERLOCKclient order by id")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	rs.Close()
	keyword := "(*INTERLOCKIterator).work"
	c.Check(checkGoroutineExists(keyword), IsFalse)
}

func (s *testSuite3) TestGetLackHandles(c *C) {
	expectedHandles := []ekv.Handle{ekv.IntHandle(1), ekv.IntHandle(2), ekv.IntHandle(3), ekv.IntHandle(4),
		ekv.IntHandle(5), ekv.IntHandle(6), ekv.IntHandle(7), ekv.IntHandle(8), ekv.IntHandle(9), ekv.IntHandle(10)}
	handlesMap := ekv.NewHandleMap()
	for _, h := range expectedHandles {
		handlesMap.Set(h, true)
	}

	// expected handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	// obtained handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	diffHandles := executor.GetLackHandles(expectedHandles, handlesMap)
	c.Assert(diffHandles, HasLen, 0)
	c.Assert(handlesMap.Len(), Equals, 0)

	// expected handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	// obtained handles 2, 3, 4, 6, 7, 8, 9
	retHandles := []ekv.Handle{ekv.IntHandle(2), ekv.IntHandle(3), ekv.IntHandle(4), ekv.IntHandle(6),
		ekv.IntHandle(7), ekv.IntHandle(8), ekv.IntHandle(9)}
	handlesMap = ekv.NewHandleMap()
	handlesMap.Set(ekv.IntHandle(1), true)
	handlesMap.Set(ekv.IntHandle(5), true)
	handlesMap.Set(ekv.IntHandle(10), true)
	diffHandles = executor.GetLackHandles(expectedHandles, handlesMap)
	c.Assert(retHandles, DeepEquals, diffHandles)
}

func (s *testSuite3) TestBigIntPK(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a bigint unsigned primary key, b int, c int, index idx(a, b))")
	tk.MustExec("insert into t values(1, 1, 1), (9223372036854775807, 2, 2)")
	tk.MustQuery("select * from t use index(idx) order by a").Check(testkit.Events("1 1 1", "9223372036854775807 2 2"))
}

func (s *testSuite3) TestCorDefCausToRanges(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only-full-group-by
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2 ,2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8), (9, 9, 9)")
	tk.MustExec("analyze block t")
	// Test single read on block.
	tk.MustQuery("select t.c in (select count(*) from t s ignore index(idx), t t1 where s.a = t.a and s.a = t1.a) from t").Check(testkit.Events("1", "0", "0", "0", "0", "0", "0", "0", "0"))
	// Test single read on index.
	tk.MustQuery("select t.c in (select count(*) from t s use index(idx), t t1 where s.b = t.a and s.a = t1.a) from t").Check(testkit.Events("1", "0", "0", "0", "0", "0", "0", "0", "0"))
	// Test IndexLookUpReader.
	tk.MustQuery("select t.c in (select count(*) from t s use index(idx), t t1 where s.b = t.a and s.c = t1.a) from t").Check(testkit.Events("1", "0", "0", "0", "0", "0", "0", "0", "0"))
}

func (s *testSuiteP1) TestUniqueKeyNullValueSelect(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	// test null in unique-key
	tk.MustExec("create block t (id int default null, c varchar(20), unique id (id));")
	tk.MustExec("insert t (c) values ('a'), ('b'), ('c');")
	res := tk.MustQuery("select * from t where id is null;")
	res.Check(testkit.Events("<nil> a", "<nil> b", "<nil> c"))

	// test null in mul unique-key
	tk.MustExec("drop block t")
	tk.MustExec("create block t (id int default null, b int default 1, c varchar(20), unique id_c(id, b));")
	tk.MustExec("insert t (c) values ('a'), ('b'), ('c');")
	res = tk.MustQuery("select * from t where id is null and b = 1;")
	res.Check(testkit.Events("<nil> 1 a", "<nil> 1 b", "<nil> 1 c"))

	tk.MustExec("drop block t")
	// test null in non-unique-key
	tk.MustExec("create block t (id int default null, c varchar(20), key id (id));")
	tk.MustExec("insert t (c) values ('a'), ('b'), ('c');")
	res = tk.MustQuery("select * from t where id is null;")
	res.Check(testkit.Events("<nil> a", "<nil> b", "<nil> c"))
}

// TestIssue10178 contains tests for https://github.com/whtcorpsinc/milevadb/issues/10178 .
func (s *testSuite3) TestIssue10178(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a bigint unsigned primary key)")
	tk.MustExec("insert into t values(9223372036854775807), (18446744073709551615)")
	tk.MustQuery("select max(a) from t").Check(testkit.Events("18446744073709551615"))
	tk.MustQuery("select * from t where a > 9223372036854775807").Check(testkit.Events("18446744073709551615"))
	tk.MustQuery("select * from t where a < 9223372036854775808").Check(testkit.Events("9223372036854775807"))
}

func (s *testSuite3) TestInconsistentIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, index idx_a(a))")
	is := s.petri.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	idx := tbl.Meta().FindIndexByName("idx_a")
	idxOp := blocks.NewIndex(tbl.Meta().ID, tbl.Meta(), idx)
	ctx := mock.NewContext()
	ctx.CausetStore = s.causetstore

	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i+10, i))
		c.Assert(tk.QueryToErr("select * from t where a>=0"), IsNil)
	}

	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("uFIDelate t set a=%d where a=%d", i, i+10))
		c.Assert(tk.QueryToErr("select * from t where a>=0"), IsNil)
	}

	for i := 0; i < 10; i++ {
		txn, err := s.causetstore.Begin()
		c.Assert(err, IsNil)
		_, err = idxOp.Create(ctx, txn.GetUnionStore(), types.MakeCausets(i+10), ekv.IntHandle(100+i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)

		err = tk.QueryToErr("select * from t use index(idx_a) where a >= 0")
		c.Assert(err.Error(), Equals, fmt.Sprintf("inconsistent index idx_a handle count %d isn't equal to value count 10", i+11))

		// if has other conditions, the inconsistent index check doesn't work.
		err = tk.QueryToErr("select * from t where a>=0 and b<10")
		c.Assert(err, IsNil)
	}

	// fix inconsistent problem to pass CI
	for i := 0; i < 10; i++ {
		txn, err := s.causetstore.Begin()
		c.Assert(err, IsNil)
		err = idxOp.Delete(ctx.GetStochastikVars().StmtCtx, txn, types.MakeCausets(i+10), ekv.IntHandle(100+i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
	}
}

func (s *testSuite3) TestPushLimitDownIndexLookUpReader(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists tbl")
	tk.MustExec("create block tbl(a int, b int, c int, key idx_b_c(b,c))")
	tk.MustExec("insert into tbl values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 1 limit 2,1").Check(testkit.Events("4 4 4"))
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 4 limit 2,1").Check(testkit.Events())
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 3 limit 2,1").Check(testkit.Events())
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 2 limit 2,1").Check(testkit.Events("5 5 5"))
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 1 limit 1").Check(testkit.Events("2 2 2"))
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 1 order by b desc limit 2,1").Check(testkit.Events("3 3 3"))
	tk.MustQuery("select * from tbl use index(idx_b_c) where b > 1 and c > 1 limit 2,1").Check(testkit.Events("4 4 4"))
}
