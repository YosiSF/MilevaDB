package executor_test

import (
	"strings"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testSuite9) TestIndexLookupMergeJoinHang(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/executor/IndexMergeJoinMockOOM", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/executor/IndexMergeJoinMockOOM"), IsNil)
	}()

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1 (a int,b int,index idx(a))")
	tk.MustExec("create block t2 (a int,b int,index idx(a))")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(2000,2000)")
	tk.MustExec("insert into t2 values (1,1),(2,2),(3,3),(2000,2000)")
	// Do not hang in index merge join when OOM occurs.
	err := tk.QueryToErr("select /*+ INL_MERGE_JOIN(t1, t2) */ * from t1, t2 where t1.a = t2.a")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "OOM test index merge join doesn't hang here.")
}

func (s *testSuite9) TestIssue18068(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/executor/testIssue18068", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/executor/testIssue18068"), IsNil)
	}()

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t, s")
	tk.MustExec("create block t (a int, index idx(a))")
	tk.MustExec("create block s (a int, index idx(a))")
	tk.MustExec("insert into t values(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("insert into s values(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("set @@milevadb_index_join_batch_size=1")
	tk.MustExec("set @@milevadb_max_chunk_size=32")
	tk.MustExec("set @@milevadb_init_chunk_size=1")
	tk.MustExec("set @@milevadb_index_lookup_join_concurrency=2")

	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
	// Do not hang in index merge join when the second and third execute.
	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
}

func (s *testSuite9) TestIssue18631(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int, b int, c int, d int, primary key(a,b,c))")
	tk.MustExec("create block t2(a int, b int, c int, d int, primary key(a,b,c))")
	tk.MustExec("insert into t1 values(1,1,1,1),(2,2,2,2),(3,3,3,3)")
	tk.MustExec("insert into t2 values(1,1,1,1),(2,2,2,2)")
	firstOperator := tk.MustQuery("explain select /*+ inl_merge_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.a and t1.c = t2.c and t1.b = t2.b order by t1.a desc").Events()[0][0].(string)
	c.Assert(strings.Index(firstOperator, plancodec.TypeIndexMergeJoin), Equals, 0)
	tk.MustQuery("select /*+ inl_merge_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.a and t1.c = t2.c and t1.b = t2.b order by t1.a desc").Check(testkit.Events(
		"3 3 3 3 <nil> <nil> <nil> <nil>",
		"2 2 2 2 2 2 2 2",
		"1 1 1 1 1 1 1 1"))
}

func (s *testSuite9) TestIssue19408(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1  (c_int int, primary key(c_int))")
	tk.MustExec("create block t2  (c_int int, unique key (c_int)) partition by hash (c_int) partitions 4")
	tk.MustExec("insert into t1 values (1), (2), (3), (4), (5)")
	tk.MustExec("insert into t2 select * from t1")
	tk.MustExec("begin")
	tk.MustExec("delete from t1 where c_int = 1")
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1,t2) */ * from t1, t2 where t1.c_int = t2.c_int").Sort().Check(testkit.Events(
		"2 2",
		"3 3",
		"4 4",
		"5 5"))
	tk.MustQuery("select /*+ INL_JOIN(t1,t2) */ * from t1, t2 where t1.c_int = t2.c_int").Sort().Check(testkit.Events(
		"2 2",
		"3 3",
		"4 4",
		"5 5"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1,t2) */ * from t1, t2 where t1.c_int = t2.c_int").Sort().Check(testkit.Events(
		"2 2",
		"3 3",
		"4 4",
		"5 5"))
	tk.MustExec("commit")
}

func (s *testSuiteWithData) TestIndexJoinOnSinglePartitionBlock(c *C) {
	// For issue 19145
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	for _, val := range []string{string(variable.StaticOnly), string(variable.DynamicOnly)} {
		tk.MustExec("set @@milevadb_partition_prune_mode= '" + val + "'")
		tk.MustExec("drop block if exists t1, t2")
		tk.MustExec("create block t1  (c_int int, c_str varchar(40), primary key (c_int) ) partition by range (c_int) ( partition p0 values less than (10), partition p1 values less than maxvalue )")
		tk.MustExec("create block t2  (c_int int, c_str varchar(40), primary key (c_int) ) partition by range (c_int) ( partition p0 values less than (10), partition p1 values less than maxvalue )")
		tk.MustExec("insert into t1 values (1, 'Alice')")
		tk.MustExec("insert into t2 values (1, 'Bob')")
		allegrosql := "select /*+ INL_MERGE_JOIN(t1,t2) */ * from t1 join t2 partition(p0) on t1.c_int = t2.c_int and t1.c_str < t2.c_str"
		tk.MustQuery(allegrosql).Check(testkit.Events("1 Alice 1 Bob"))
		rows := s.testData.ConvertEventsToStrings(tk.MustQuery("explain " + allegrosql).Events())
		// Partition block can't be inner side of index merge join, because it can't keep order.
		c.Assert(strings.Index(rows[0], "IndexMergeJoin"), Equals, -1)
		c.Assert(len(tk.MustQuery("show warnings").Events()) > 0, Equals, true)

		allegrosql = "select /*+ INL_HASH_JOIN(t1,t2) */ * from t1 join t2 partition(p0) on t1.c_int = t2.c_int and t1.c_str < t2.c_str"
		tk.MustQuery(allegrosql).Check(testkit.Events("1 Alice 1 Bob"))
		rows = s.testData.ConvertEventsToStrings(tk.MustQuery("explain " + allegrosql).Events())
		c.Assert(strings.Index(rows[0], "IndexHashJoin"), Equals, 0)

		allegrosql = "select /*+ INL_JOIN(t1,t2) */ * from t1 join t2 partition(p0) on t1.c_int = t2.c_int and t1.c_str < t2.c_str"
		tk.MustQuery(allegrosql).Check(testkit.Events("1 Alice 1 Bob"))
		rows = s.testData.ConvertEventsToStrings(tk.MustQuery("explain " + allegrosql).Events())
		c.Assert(strings.Index(rows[0], "IndexJoin"), Equals, 0)
	}
}
