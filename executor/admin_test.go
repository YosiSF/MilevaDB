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

package executor_test

import (
	"context"
	"fmt"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	allegrosql "github.com/whtcorpsinc/MilevaDB-Prod/errno"
	"github.com/whtcorpsinc/MilevaDB-Prod/executor"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
)

func (s *testSuite1) TestAdminChecHoTTexRange(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists check_index_test;`)
	tk.MustExec(`create block check_index_test (a int, b varchar(10), index a_b (a, b), index b (b))`)
	tk.MustExec(`insert check_index_test values (3, "ab"),(2, "cd"),(1, "ef"),(-1, "hi")`)
	result := tk.MustQuery("admin check index check_index_test a_b (2, 4);")
	result.Check(testkit.Events("1 ef 3", "2 cd 2"))

	result = tk.MustQuery("admin check index check_index_test a_b (3, 5);")
	result.Check(testkit.Events("-1 hi 4", "1 ef 3"))

	tk.MustExec("use allegrosql")
	result = tk.MustQuery("admin check index test.check_index_test a_b (2, 3), (4, 5);")
	result.Check(testkit.Events("-1 hi 4", "2 cd 2"))
}

func (s *testSuite5) TestAdminChecHoTTex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	check := func() {
		tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (5, 5), (10, 10), (11, 11), (NULL, NULL)")
		tk.MustExec("admin check index admin_test c1")
		tk.MustExec("admin check index admin_test c2")
	}
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2))")
	check()

	// Test for hash partition block.
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2)) partition by hash(c2) partitions 5;")
	check()

	// Test for range partition block.
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec(`create block admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2)) PARTITION BY RANGE ( c2 ) (
		PARTITION p0 VALUES LESS THAN (5),
		PARTITION p1 VALUES LESS THAN (10),
		PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	check()
}

func (s *testSuite5) TestAdminRecoverIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (NULL, NULL)")

	r := tk.MustQuery("admin recover index admin_test c1")
	r.Check(testkit.Events("0 3"))

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Events("0 3"))

	tk.MustExec("admin check index admin_test c1")
	tk.MustExec("admin check index admin_test c2")

	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, primary key(c1), unique key(c2))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (3, 3), (10, 10), (20, 20)")
	// pk is handle, no additional unique index, no way to recover
	_, err := tk.Exec("admin recover index admin_test c1")
	// err:index is not found
	c.Assert(err, NotNil)

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Events("0 5"))
	tk.MustExec("admin check index admin_test c2")

	// Make some corrupted index.
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	is := s.petri.SchemaReplicant()
	dbName := perceptron.NewCIStr("test")
	tblName := perceptron.NewCIStr("admin_test")
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("c2")
	indexOpr := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	sc := s.ctx.GetStochaseinstein_dbars().StmtCtx
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(1), ekv.IntHandle(1))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	c.Assert(executor.ErrAdminCheckBlock.Equal(err), IsTrue)
	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Events("4"))

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Events("1 5"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Events("5"))
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check block admin_test")

	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(10), ekv.IntHandle(10))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)
	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Events("1 5"))
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check block admin_test")

	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(1), ekv.IntHandle(1))
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(2), ekv.IntHandle(2))
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(3), ekv.IntHandle(3))
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(10), ekv.IntHandle(10))
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(20), ekv.IntHandle(20))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Events("0"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX()")
	r.Check(testkit.Events("5"))

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Events("5 5"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Events("5"))

	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check block admin_test")
}

func (s *testSuite5) TestClusteredIndexAdminRecoverIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_cluster_index_admin_recover;")
	tk.MustExec("create database test_cluster_index_admin_recover;")
	tk.MustExec("use test_cluster_index_admin_recover;")
	tk.MustExec("set milevadb_enable_clustered_index=1;")
	dbName := perceptron.NewCIStr("test_cluster_index_admin_recover")
	tblName := perceptron.NewCIStr("t")

	// Test no corruption case.
	tk.MustExec("create block t (a varchar(255), b int, c char(10), primary key(a, c), index idx(b));")
	tk.MustExec("insert into t values ('1', 2, '3'), ('1', 2, '4'), ('1', 2, '5');")
	tk.MustQuery("admin recover index t `primary`;").Check(testkit.Events("0 0"))
	tk.MustQuery("admin recover index t `idx`;").Check(testkit.Events("0 3"))
	tk.MustExec("admin check block t;")

	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	is := s.petri.SchemaReplicant()
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("idx")
	indexOpr := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	sc := s.ctx.GetStochaseinstein_dbars().StmtCtx

	// Some index entries are missed.
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	cHandle := solitonutil.MustNewCommonHandle(c, "1", "3")
	err = indexOpr.Delete(sc, txn, types.MakeCausets(2), cHandle)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	tk.MustGetErrCode("admin check block t", allegrosql.ErrAdminCheckBlock)
	tk.MustGetErrCode("admin check index t idx", allegrosql.ErrAdminCheckBlock)

	tk.MustQuery("SELECT COUNT(*) FROM t USE INDEX(idx)").Check(testkit.Events("2"))
	tk.MustQuery("admin recover index t idx").Check(testkit.Events("1 3"))
	tk.MustQuery("SELECT COUNT(*) FROM t USE INDEX(idx)").Check(testkit.Events("3"))
	tk.MustExec("admin check block t;")
}

func (s *testSuite5) TestAdminRecoverPartitionBlockIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	getBlock := func() block.Block {
		s.ctx = mock.NewContext()
		s.ctx.CausetStore = s.causetstore
		is := s.petri.SchemaReplicant()
		dbName := perceptron.NewCIStr("test")
		tblName := perceptron.NewCIStr("admin_test")
		tbl, err := is.BlockByName(dbName, tblName)
		c.Assert(err, IsNil)
		return tbl
	}

	checkFunc := func(tbl block.Block, pid int64, idxValue int) {
		idxInfo := tbl.Meta().FindIndexByName("c2")
		indexOpr := blocks.NewIndex(pid, tbl.Meta(), idxInfo)
		sc := s.ctx.GetStochaseinstein_dbars().StmtCtx
		txn, err := s.causetstore.Begin()
		c.Assert(err, IsNil)
		err = indexOpr.Delete(sc, txn, types.MakeCausets(idxValue), ekv.IntHandle(idxValue))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		err = tk.ExecToErr("admin check block admin_test")
		c.Assert(err, NotNil)
		c.Assert(executor.ErrAdminCheckBlock.Equal(err), IsTrue)

		r := tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
		r.Check(testkit.Events("2"))

		r = tk.MustQuery("admin recover index admin_test c2")
		r.Check(testkit.Events("1 3"))

		r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
		r.Check(testkit.Events("3"))
		tk.MustExec("admin check block admin_test")
	}

	// Test for hash partition block.
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, primary key (c1), index (c2)) partition by hash(c1) partitions 3;")
	tk.MustExec("insert admin_test (c1, c2) values (0, 0), (1, 1), (2, 2)")
	r := tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Events("0 3"))
	tbl := getBlock()
	pi := tbl.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	for i, p := range pi.Definitions {
		checkFunc(tbl, p.ID, i)
	}

	// Test for range partition block.
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec(`create block admin_test (c1 int, c2 int, c3 int default 1, primary key (c1), index (c2)) PARTITION BY RANGE ( c1 ) (
		PARTITION p0 VALUES LESS THAN (5),
		PARTITION p1 VALUES LESS THAN (10),
		PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	tk.MustExec("insert admin_test (c1, c2) values (0, 0), (6, 6), (12, 12)")
	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Events("0 3"))
	tbl = getBlock()
	pi = tbl.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	for i, p := range pi.Definitions {
		checkFunc(tbl, p.ID, i*6)
	}
}

func (s *testSuite5) TestAdminRecoverIndex1(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	dbName := perceptron.NewCIStr("test")
	tblName := perceptron.NewCIStr("admin_test")
	sc := s.ctx.GetStochaseinstein_dbars().StmtCtx
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("set @@milevadb_enable_clustered_index=0;")
	tk.MustExec("create block admin_test (c1 varchar(255), c2 int, c3 int default 1, primary key(c1), unique key(c2))")
	tk.MustExec("insert admin_test (c1, c2) values ('1', 1), ('2', 2), ('3', 3), ('10', 10), ('20', 20)")

	r := tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Events("5"))

	is := s.petri.SchemaReplicant()
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("primary")
	c.Assert(idxInfo, NotNil)
	indexOpr := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets("1"), ekv.IntHandle(1))
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets("2"), ekv.IntHandle(2))
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets("3"), ekv.IntHandle(3))
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets("10"), ekv.IntHandle(4))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Events("1"))

	r = tk.MustQuery("admin recover index admin_test `primary`")
	r.Check(testkit.Events("4 5"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Events("5"))

	tk.MustExec("admin check block admin_test")
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check index admin_test `primary`")
}

func (s *testSuite5) TestAdminCleanupIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, primary key (c1), unique key(c2), key (c3))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 2), (3, 4), (-5, NULL)")
	tk.MustExec("insert admin_test (c1, c3) values (7, 100), (9, 100), (11, NULL)")

	// pk is handle, no need to cleanup
	_, err := tk.Exec("admin cleanup index admin_test `primary`")
	c.Assert(err, NotNil)
	r := tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Events("0"))
	r = tk.MustQuery("admin cleanup index admin_test c3")
	r.Check(testkit.Events("0"))

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	is := s.petri.SchemaReplicant()
	dbName := perceptron.NewCIStr("test")
	tblName := perceptron.NewCIStr("admin_test")
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo2 := tblInfo.FindIndexByName("c2")
	indexOpr2 := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo2)
	idxInfo3 := tblInfo.FindIndexByName("c3")
	indexOpr3 := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo3)

	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(1), ekv.IntHandle(-100))
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(6), ekv.IntHandle(100))
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(8), ekv.IntHandle(100))
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(nil), ekv.IntHandle(101))
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(nil), ekv.IntHandle(102))
	c.Assert(err, IsNil)
	_, err = indexOpr3.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(6), ekv.IntHandle(200))
	c.Assert(err, IsNil)
	_, err = indexOpr3.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(6), ekv.IntHandle(-200))
	c.Assert(err, IsNil)
	_, err = indexOpr3.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(8), ekv.IntHandle(-200))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Events("11"))
	r = tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Events("5"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Events("6"))
	tk.MustExec("admin check index admin_test c2")

	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c3")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
	r.Check(testkit.Events("9"))
	r = tk.MustQuery("admin cleanup index admin_test c3")
	r.Check(testkit.Events("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
	r.Check(testkit.Events("6"))
	tk.MustExec("admin check index admin_test c3")

	tk.MustExec("admin check block admin_test")
}

func (s *testSuite5) TestAdminCleanupIndexForPartitionBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	getBlock := func() block.Block {
		s.ctx = mock.NewContext()
		s.ctx.CausetStore = s.causetstore
		is := s.petri.SchemaReplicant()
		dbName := perceptron.NewCIStr("test")
		tblName := perceptron.NewCIStr("admin_test")
		tbl, err := is.BlockByName(dbName, tblName)
		c.Assert(err, IsNil)
		return tbl
	}

	checkFunc := func(tbl block.Block, pid int64, idxValue, handle int) {
		idxInfo2 := tbl.Meta().FindIndexByName("c2")
		indexOpr2 := blocks.NewIndex(pid, tbl.Meta(), idxInfo2)
		idxInfo3 := tbl.Meta().FindIndexByName("c3")
		indexOpr3 := blocks.NewIndex(pid, tbl.Meta(), idxInfo3)

		txn, err := s.causetstore.Begin()
		c.Assert(err, IsNil)
		_, err = indexOpr2.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(idxValue), ekv.IntHandle(handle))
		c.Assert(err, IsNil)
		_, err = indexOpr3.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(idxValue), ekv.IntHandle(handle))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)

		err = tk.ExecToErr("admin check block admin_test")
		c.Assert(err, NotNil)

		r := tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
		r.Check(testkit.Events("4"))
		r = tk.MustQuery("admin cleanup index admin_test c2")
		r.Check(testkit.Events("1"))
		r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
		r.Check(testkit.Events("3"))

		r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
		r.Check(testkit.Events("4"))
		r = tk.MustQuery("admin cleanup index admin_test c3")
		r.Check(testkit.Events("1"))
		r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
		r.Check(testkit.Events("3"))
		tk.MustExec("admin check block admin_test")
	}

	// Test for hash partition block.
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, primary key (c2), unique index c2(c2), index c3(c3)) partition by hash(c2) partitions 3;")
	tk.MustExec("insert admin_test (c2, c3) values (0, 0), (1, 1), (2, 2)")
	r := tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Events("0"))
	tbl := getBlock()
	pi := tbl.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	for i, p := range pi.Definitions {
		checkFunc(tbl, p.ID, i+6, i+6)
	}

	// Test for range partition block.
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec(`create block admin_test (c1 int, c2 int, c3 int default 1, primary key (c2), unique index c2 (c2), index c3(c3)) PARTITION BY RANGE ( c2 ) (
		PARTITION p0 VALUES LESS THAN (5),
		PARTITION p1 VALUES LESS THAN (10),
		PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	tk.MustExec("insert admin_test (c1, c2) values (0, 0), (6, 6), (12, 12)")
	r = tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Events("0"))
	tbl = getBlock()
	pi = tbl.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	for i, p := range pi.Definitions {
		checkFunc(tbl, p.ID, i*6+1, i*6+1)
	}
}

func (s *testSuite5) TestAdminCleanupIndexPKNotHandle(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("set @@milevadb_enable_clustered_index=0;")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int, primary key (c1, c2))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 2), (3, 4), (-5, 5)")

	r := tk.MustQuery("admin cleanup index admin_test `primary`")
	r.Check(testkit.Events("0"))

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	is := s.petri.SchemaReplicant()
	dbName := perceptron.NewCIStr("test")
	tblName := perceptron.NewCIStr("admin_test")
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("primary")
	indexOpr := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(7, 10), ekv.IntHandle(-100))
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(4, 6), ekv.IntHandle(100))
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(-7, 4), ekv.IntHandle(101))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test `primary`")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Events("6"))
	r = tk.MustQuery("admin cleanup index admin_test `primary`")
	r.Check(testkit.Events("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Events("3"))
	tk.MustExec("admin check index admin_test `primary`")
	tk.MustExec("admin check block admin_test")
}

func (s *testSuite5) TestAdminCleanupIndexMore(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, unique key (c1, c2), key (c2))")
	tk.MustExec("insert admin_test values (1, 2), (3, 4), (5, 6)")

	tk.MustExec("admin cleanup index admin_test c1")
	tk.MustExec("admin cleanup index admin_test c2")

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	is := s.petri.SchemaReplicant()
	dbName := perceptron.NewCIStr("test")
	tblName := perceptron.NewCIStr("admin_test")
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo1 := tblInfo.FindIndexByName("c1")
	indexOpr1 := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo1)
	idxInfo2 := tblInfo.FindIndexByName("c2")
	indexOpr2 := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo2)

	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	for i := 0; i < 2000; i++ {
		c1 := int64(2*i + 7)
		c2 := int64(2*i + 8)
		_, err = indexOpr1.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(c1, c2), ekv.IntHandle(c1))
		c.Assert(err, IsNil, Commentf(errors.ErrorStack(err)))
		_, err = indexOpr2.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(c2), ekv.IntHandle(c1))
		c.Assert(err, IsNil)
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c1")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)
	r := tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX()")
	r.Check(testkit.Events("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c1)")
	r.Check(testkit.Events("2003"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Events("2003"))
	r = tk.MustQuery("admin cleanup index admin_test c1")
	r.Check(testkit.Events("2000"))
	r = tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Events("2000"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c1)")
	r.Check(testkit.Events("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Events("3"))
	tk.MustExec("admin check index admin_test c1")
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check block admin_test")
}

func (s *testSuite5) TestClusteredAdminCleanupIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("set milevadb_enable_clustered_index=1")
	tk.MustExec("create block admin_test (c1 varchar(255), c2 int, c3 char(10) default 'c3', primary key (c1, c3), unique key(c2), key (c3))")
	tk.MustExec("insert admin_test (c1, c2) values ('c1_1', 2), ('c1_2', 4), ('c1_3', NULL)")
	tk.MustExec("insert admin_test (c1, c3) values ('c1_4', 'c3_4'), ('c1_5', 'c3_5'), ('c1_6', default)")

	// Normally, there is no dangling index.
	tk.MustQuery("admin cleanup index admin_test `primary`").Check(testkit.Events("0"))
	tk.MustQuery("admin cleanup index admin_test `c2`").Check(testkit.Events("0"))
	tk.MustQuery("admin cleanup index admin_test `c3`").Check(testkit.Events("0"))

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	tbl, err := s.petri.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("admin_test"))
	c.Assert(err, IsNil)
	// cleanup clustered primary key takes no effect.

	tblInfo := tbl.Meta()
	idxInfo2 := tblInfo.FindIndexByName("c2")
	indexOpr2 := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo2)
	idxInfo3 := tblInfo.FindIndexByName("c3")
	indexOpr3 := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo3)

	c2DanglingIdx := []struct {
		handle ekv.Handle
		idxVal []types.Causet
	}{
		{solitonutil.MustNewCommonHandle(c, "c1_10", "c3_10"), types.MakeCausets(10)},
		{solitonutil.MustNewCommonHandle(c, "c1_10", "c3_11"), types.MakeCausets(11)},
		{solitonutil.MustNewCommonHandle(c, "c1_12", "c3_12"), types.MakeCausets(12)},
	}
	c3DanglingIdx := []struct {
		handle ekv.Handle
		idxVal []types.Causet
	}{
		{solitonutil.MustNewCommonHandle(c, "c1_13", "c3_13"), types.MakeCausets("c3_13")},
		{solitonutil.MustNewCommonHandle(c, "c1_14", "c3_14"), types.MakeCausets("c3_14")},
		{solitonutil.MustNewCommonHandle(c, "c1_15", "c3_15"), types.MakeCausets("c3_15")},
	}
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	for _, di := range c2DanglingIdx {
		_, err := indexOpr2.Create(s.ctx, txn.GetUnionStore(), di.idxVal, di.handle)
		c.Assert(err, IsNil)
	}
	for _, di := range c3DanglingIdx {
		_, err := indexOpr3.Create(s.ctx, txn.GetUnionStore(), di.idxVal, di.handle)
		c.Assert(err, IsNil)
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)
	tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)").Check(testkit.Events("9"))
	tk.MustQuery("admin cleanup index admin_test c2").Check(testkit.Events("3"))
	tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)").Check(testkit.Events("6"))
	tk.MustExec("admin check index admin_test c2")

	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c3")
	c.Assert(err, NotNil)
	tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)").Check(testkit.Events("9"))
	tk.MustQuery("admin cleanup index admin_test c3").Check(testkit.Events("3"))
	tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)").Check(testkit.Events("6"))
	tk.MustExec("admin check index admin_test c3")
	tk.MustExec("admin check block admin_test")
}

func (s *testSuite3) TestAdminCheckPartitionBlockFailed(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test_p")
	tk.MustExec("create block admin_test_p (c1 int key,c2 int,c3 int,index idx(c2)) partition by hash(c1) partitions 4")
	tk.MustExec("insert admin_test_p (c1, c2, c3) values (0,0,0), (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
	tk.MustExec("admin check block admin_test_p")

	// Make some corrupted index. Build the index information.
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	is := s.petri.SchemaReplicant()
	dbName := perceptron.NewCIStr("test")
	tblName := perceptron.NewCIStr("admin_test_p")
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	sc := s.ctx.GetStochaseinstein_dbars().StmtCtx
	tk.Se.GetStochaseinstein_dbars().IndexLookupSize = 3
	tk.Se.GetStochaseinstein_dbars().MaxChunkSize = 3

	// Reduce one event of index on partitions.
	// Block count > index count.
	for i := 0; i <= 5; i++ {
		partitionIdx := i % len(tblInfo.GetPartitionInfo().Definitions)
		indexOpr := blocks.NewIndex(tblInfo.GetPartitionInfo().Definitions[partitionIdx].ID, tblInfo, idxInfo)
		txn, err := s.causetstore.Begin()
		c.Assert(err, IsNil)
		err = indexOpr.Delete(sc, txn, types.MakeCausets(i), ekv.IntHandle(i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		err = tk.ExecToErr("admin check block admin_test_p")
		c.Assert(err.Error(), Equals, fmt.Sprintf("[executor:8003]admin_test_p err:[admin:8223]index:<nil> != record:&admin.RecordData{Handle:%d, Values:[]types.Causet{types.Causet{k:0x1, decimal:0x0, length:0x0, i:%d, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)}}}", i, i))
		c.Assert(executor.ErrAdminCheckBlock.Equal(err), IsTrue)
		// TODO: fix admin recover for partition block.
		//r := tk.MustQuery("admin recover index admin_test_p idx")
		//r.Check(testkit.Events("0 0"))
		//tk.MustExec("admin check block admin_test_p")
		// Manual recover index.
		txn, err = s.causetstore.Begin()
		c.Assert(err, IsNil)
		_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(i), ekv.IntHandle(i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		tk.MustExec("admin check block admin_test_p")
	}

	// Add one event of index on partitions.
	// Block count < index count.
	for i := 0; i <= 5; i++ {
		partitionIdx := i % len(tblInfo.GetPartitionInfo().Definitions)
		indexOpr := blocks.NewIndex(tblInfo.GetPartitionInfo().Definitions[partitionIdx].ID, tblInfo, idxInfo)
		txn, err := s.causetstore.Begin()
		c.Assert(err, IsNil)
		_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(i+8), ekv.IntHandle(i+8))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		err = tk.ExecToErr("admin check block admin_test_p")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, fmt.Sprintf("handle %d, index:types.Causet{k:0x1, decimal:0x0, length:0x0, i:%d, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)} != record:<nil>", i+8, i+8))
		// TODO: fix admin recover for partition block.
		txn, err = s.causetstore.Begin()
		c.Assert(err, IsNil)
		err = indexOpr.Delete(sc, txn, types.MakeCausets(i+8), ekv.IntHandle(i+8))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		tk.MustExec("admin check block admin_test_p")
	}

	// Block count = index count, but the index value was wrong.
	for i := 0; i <= 5; i++ {
		partitionIdx := i % len(tblInfo.GetPartitionInfo().Definitions)
		indexOpr := blocks.NewIndex(tblInfo.GetPartitionInfo().Definitions[partitionIdx].ID, tblInfo, idxInfo)
		txn, err := s.causetstore.Begin()
		c.Assert(err, IsNil)
		_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(i+8), ekv.IntHandle(i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		err = tk.ExecToErr("admin check block admin_test_p")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, fmt.Sprintf("defCaus c2, handle %d, index:types.Causet{k:0x1, decimal:0x0, length:0x0, i:%d, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)} != record:types.Causet{k:0x1, decimal:0x0, length:0x0, i:%d, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)}", i, i+8, i))
		// TODO: fix admin recover for partition block.
		txn, err = s.causetstore.Begin()
		c.Assert(err, IsNil)
		err = indexOpr.Delete(sc, txn, types.MakeCausets(i+8), ekv.IntHandle(i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		tk.MustExec("admin check block admin_test_p")
	}
}

func (s *testSuite5) TestAdminCheckBlockFailed(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 varchar(255) default '1', primary key(c1), key(c3), unique key(c2), key(c2, c3))")
	tk.MustExec("insert admin_test (c1, c2, c3) values (-10, -20, 'y'), (-1, -10, 'z'), (1, 11, 'a'), (2, 12, 'b'), (5, 15, 'c'), (10, 20, 'd'), (20, 30, 'e')")

	// Make some corrupted index. Build the index information.
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	is := s.petri.SchemaReplicant()
	dbName := perceptron.NewCIStr("test")
	tblName := perceptron.NewCIStr("admin_test")
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[1]
	indexOpr := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	sc := s.ctx.GetStochaseinstein_dbars().StmtCtx
	tk.Se.GetStochaseinstein_dbars().IndexLookupSize = 3
	tk.Se.GetStochaseinstein_dbars().MaxChunkSize = 3

	// Reduce one event of index.
	// Block count > index count.
	// Index c2 is missing 11.
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(-10), ekv.IntHandle(-1))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals,
		"[executor:8003]admin_test err:[admin:8223]index:<nil> != record:&admin.RecordData{Handle:-1, Values:[]types.Causet{types.Causet{k:0x1, decimal:0x0, length:0x0, i:-10, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)}}}")
	c.Assert(executor.ErrAdminCheckBlock.Equal(err), IsTrue)
	r := tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Events("1 7"))
	tk.MustExec("admin check block admin_test")

	// Add one event of index.
	// Block count < index count.
	// Index c2 has one more values than block data: 0, and the handle 0 hasn't correlative record.
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(0), ekv.IntHandle(0))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "handle 0, index:types.Causet{k:0x1, decimal:0x0, length:0x0, i:0, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)} != record:<nil>")

	// Add one event of index.
	// Block count < index count.
	// Index c2 has two more values than block data: 10, 13, and these handles have correlative record.
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(0), ekv.IntHandle(0))
	c.Assert(err, IsNil)
	// Make sure the index value "19" is smaller "21". Then we scan to "19" before "21".
	_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(19), ekv.IntHandle(10))
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(13), ekv.IntHandle(2))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "defCaus c2, handle 2, index:types.Causet{k:0x1, decimal:0x0, length:0x0, i:13, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)} != record:types.Causet{k:0x1, decimal:0x0, length:0x0, i:12, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)}")

	// Block count = index count.
	// Two indices have the same handle.
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(13), ekv.IntHandle(2))
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(12), ekv.IntHandle(2))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "defCaus c2, handle 10, index:types.Causet{k:0x1, decimal:0x0, length:0x0, i:19, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)} != record:types.Causet{k:0x1, decimal:0x0, length:0x0, i:20, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)}")

	// Block count = index count.
	// Index c2 has one line of data is 19, the corresponding block data is 20.
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(12), ekv.IntHandle(2))
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(20), ekv.IntHandle(10))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check block admin_test")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "defCaus c2, handle 10, index:types.Causet{k:0x1, decimal:0x0, length:0x0, i:19, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)} != record:types.Causet{k:0x1, decimal:0x0, length:0x0, i:20, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)}")

	// Recover records.
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeCausets(19), ekv.IntHandle(10))
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(20), ekv.IntHandle(10))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	tk.MustExec("admin check block admin_test")
}

func (s *testSuite8) TestAdminCheckBlock(c *C) {
	// test NULL value.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_null (
		a int(11) NOT NULL,
		c int(11) NOT NULL,
		PRIMARY KEY (a, c),
		KEY idx_a (a)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin`)

	tk.MustExec(`insert into test_null(a, c) values(2, 2);`)
	tk.MustExec(`ALTER TABLE test_null ADD COLUMN b int NULL DEFAULT '1795454803' AFTER a;`)
	tk.MustExec(`ALTER TABLE test_null add index b(b);`)
	tk.MustExec("ADMIN CHECK TABLE test_null")

	// Fix unflatten issue in CheckExec.
	tk.MustExec(`drop block if exists test`)
	tk.MustExec(`create block test (
		a time,
		PRIMARY KEY (a)
		);`)

	tk.MustExec(`insert into test set a='12:10:36';`)
	tk.MustExec(`admin check block test`)

	// Test decimal
	tk.MustExec(`drop block if exists test`)
	tk.MustExec("CREATE TABLE test (  a decimal, PRIMARY KEY (a));")
	tk.MustExec("insert into test set a=10;")
	tk.MustExec("admin check block test;")

	// Test timestamp type check block.
	tk.MustExec(`drop block if exists test`)
	tk.MustExec(`create block test ( a  TIMESTAMP, primary key(a) );`)
	tk.MustExec(`insert into test set a='2020-08-10 04:18:49';`)
	tk.MustExec(`admin check block test;`)

	// Test partitioned block.
	tk.MustExec(`drop block if exists test`)
	tk.MustExec(`create block test (
		      a int not null,
		      c int not null,
		      primary key (a, c),
		      key idx_a (a)) partition by range (c) (
		      partition p1 values less than (1),
		      partition p2 values less than (4),
		      partition p3 values less than (7),
		      partition p4 values less than (11))`)
	for i := 1; i <= 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into test values (%d, %d);", i, i))
	}
	tk.MustExec(`admin check block test;`)

	// Test index in virtual generated defCausumn.
	tk.MustExec(`drop block if exists test`)
	tk.MustExec(`create block test ( b json , c int as (JSON_EXTRACT(b,'$.d')), index idxc(c));`)
	tk.MustExec(`INSERT INTO test set b='{"d": 100}';`)
	tk.MustExec(`admin check block test;`)
	// Test prefix index.
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`CREATE TABLE t (
	  			ID CHAR(32) NOT NULL,
	  			name CHAR(32) NOT NULL,
	  			value CHAR(255),
	  			INDEX indexIDname (ID(8),name(8)));`)
	tk.MustExec(`INSERT INTO t VALUES ('keyword','urlprefix','text/ /text');`)
	tk.MustExec(`admin check block t;`)

	tk.MustExec("use allegrosql")
	tk.MustExec(`admin check block test.t;`)
	err := tk.ExecToErr("admin check block t")
	c.Assert(err, NotNil)

	// test add index on time type defCausumn which have default value
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t1`)
	tk.MustExec(`CREATE TABLE t1 (c2 YEAR, PRIMARY KEY (c2))`)
	tk.MustExec(`INSERT INTO t1 SET c2 = '1912'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c3 TIMESTAMP NULL DEFAULT '1976-08-29 16:28:11'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c4 DATE      NULL DEFAULT '1976-08-29'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c5 TIME      NULL DEFAULT '16:28:11'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c6 YEAR      NULL DEFAULT '1976'`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx1 (c2, c3,c4,c5,c6)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx2 (c2)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx3 (c3)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx4 (c4)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx5 (c5)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx6 (c6)`)
	tk.MustExec(`admin check block t1`)

	// Test add index on decimal defCausumn.
	tk.MustExec(`drop block if exists td1;`)
	tk.MustExec(`CREATE TABLE td1 (c2 INT NULL DEFAULT '70');`)
	tk.MustExec(`INSERT INTO td1 SET c2 = '5';`)
	tk.MustExec(`ALTER TABLE td1 ADD COLUMN c4 DECIMAL(12,8) NULL DEFAULT '213.41598062';`)
	tk.MustExec(`ALTER TABLE td1 ADD INDEX id2 (c4) ;`)
	tk.MustExec(`ADMIN CHECK TABLE td1;`)

	// Test add not null defCausumn, then add index.
	tk.MustExec(`drop block if exists t1`)
	tk.MustExec(`create block t1 (a int);`)
	tk.MustExec(`insert into t1 set a=2;`)
	tk.MustExec(`alter block t1 add defCausumn b timestamp not null;`)
	tk.MustExec(`alter block t1 add index(b);`)
	tk.MustExec(`admin check block t1;`)

	// Test for index with change decimal precision.
	tk.MustExec(`drop block if exists t1`)
	tk.MustExec(`create block t1 (a decimal(2,1), index(a))`)
	tk.MustExec(`insert into t1 set a='1.9'`)
	err = tk.ExecToErr(`alter block t1 modify defCausumn a decimal(3,2);`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: can't change decimal defCausumn precision")
	tk.MustExec(`delete from t1;`)
	tk.MustExec(`admin check block t1;`)
}

func (s *testSuite1) TestAdminCheckPrimaryIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a bigint unsigned primary key, b int, c int, index idx(a, b));")
	tk.MustExec("insert into t values(1, 1, 1), (9223372036854775807, 2, 2);")
	tk.MustExec("admin check index t idx;")
}

func (s *testSuite5) TestAdminCheckWithSnapshot(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_t_s")
	tk.MustExec("create block admin_t_s (a int, b int, key(a));")
	tk.MustExec("insert into admin_t_s values (0,0),(1,1);")
	tk.MustExec("admin check block admin_t_s;")
	tk.MustExec("admin check index admin_t_s a;")

	snapshotTime := time.Now()

	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	is := s.petri.SchemaReplicant()
	dbName := perceptron.NewCIStr("test")
	tblName := perceptron.NewCIStr("admin_t_s")
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("a")
	idxOpr := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	_, err = idxOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(2), ekv.IntHandle(100))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check block admin_t_s")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_t_s a")
	c.Assert(err, NotNil)

	// For mockeinsteindb, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "einsteindb_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	uFIDelateSafePoint := fmt.Sprintf(`INSERT INTO allegrosql.milevadb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UFIDelATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(uFIDelateSafePoint)
	// For admin check block when use snapshot.
	tk.MustExec("set @@milevadb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	tk.MustExec("admin check block admin_t_s;")
	tk.MustExec("admin check index admin_t_s a;")

	tk.MustExec("set @@milevadb_snapshot = ''")
	err = tk.ExecToErr("admin check block admin_t_s")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_t_s a")
	c.Assert(err, NotNil)

	r := tk.MustQuery("admin cleanup index admin_t_s a")
	r.Check(testkit.Events("1"))
	tk.MustExec("admin check block admin_t_s;")
	tk.MustExec("admin check index admin_t_s a;")
	tk.MustExec("drop block if exists admin_t_s")
}
