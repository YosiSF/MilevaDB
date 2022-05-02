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

package core_test

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/collate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
)

var _ = Suite(&testIntegrationSuite{})
var _ = SerialSuites(&testIntegrationSerialSuite{})

type testIntegrationSuite struct {
	testData    solitonutil.TestData
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "integration_suite")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testIntegrationSuite) SetUpTest(c *C) {
	var err error
	s.causetstore, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownTest(c *C) {
	s.dom.Close()
	err := s.causetstore.Close()
	c.Assert(err, IsNil)
}

type testIntegrationSerialSuite struct {
	testData    solitonutil.TestData
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

func (s *testIntegrationSerialSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "integration_serial_suite")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSerialSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testIntegrationSerialSuite) SetUpTest(c *C) {
	var err error
	s.causetstore, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testIntegrationSerialSuite) TearDownTest(c *C) {
	s.dom.Close()
	err := s.causetstore.Close()
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TestShowSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a varchar(10), b int, c int)")
	tk.MustQuery("show columns from t where true").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
		"b int(11) YES  <nil> ",
		"c int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field = 'b'").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b')").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b') and true").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b') and false").Check(testkit.Rows())
	tk.MustExec("insert into t values('c', 0, 0)")
	tk.MustQuery("show columns from t where field < all (select a from t)").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
		"b int(11) YES  <nil> ",
	))
	tk.MustExec("insert into t values('b', 0, 0)")
	tk.MustQuery("show columns from t where field < all (select a from t)").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
	))
}

func (s *testIntegrationSuite) TestPFIDelWithSetVar(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(c1 int, c2 varchar(255))")
	tk.MustExec("insert into t values(1,'a'),(2,'d'),(3,'c')")

	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=1 and t01.c2='d'").Check(testkit.Rows())
	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=2 and t01.c2='d'").Check(testkit.Rows("2 d 2"))
}

func (s *testIntegrationSuite) TestBitDefCausErrorMessage(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists bit_col_t")
	tk.MustExec("create block bit_col_t (a bit(64))")
	tk.MustExec("drop block bit_col_t")
	tk.MustExec("create block bit_col_t (a bit(1))")
	tk.MustExec("drop block bit_col_t")
	tk.MustGetErrCode("create block bit_col_t (a bit(0))", allegrosql.ErrInvalidFieldSize)
	tk.MustGetErrCode("create block bit_col_t (a bit(65))", allegrosql.ErrTooBigDisplaywidth)
}

func (s *testIntegrationSuite) TestPushLimitDownIndexLookUpReader(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("set @@stochastik.milevadb_executor_concurrency = 4;")
	tk.MustExec("set @@stochastik.milevadb_hash_join_concurrency = 5;")
	tk.MustExec("set @@stochastik.milevadb_distsql_scan_concurrency = 15;")
	tk.MustExec("use test")
	tk.MustExec("drop block if exists tbl")
	tk.MustExec("create block tbl(a int, b int, c int, key idx_b_c(b,c))")
	tk.MustExec("insert into tbl values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
	tk.MustExec("analyze block tbl")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestIsFromUnixtimeNullRejective(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a bigint, b bigint);`)
	s.runTestsWithTestData("TestIsFromUnixtimeNullRejective", tk, c)
}

func (s *testIntegrationSuite) runTestsWithTestData(caseName string, tk *testkit.TestKit, c *C) {
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCasesByName(caseName, c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestJoinNotNullFlag(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(x int not null)")
	tk.MustExec("create block t2(x int)")
	tk.MustExec("insert into t2 values (1)")

	tk.MustQuery("select IFNULL((select t1.x from t1 where t1.x = t2.x), 'xxx') as col1 from t2").Check(testkit.Rows("xxx"))
	tk.MustQuery("select ifnull(t1.x, 'xxx') from t2 left join t1 using(x)").Check(testkit.Rows("xxx"))
	tk.MustQuery("select ifnull(t1.x, 'xxx') from t2 natural left join t1").Check(testkit.Rows("xxx"))
}

func (s *testIntegrationSuite) TestAntiJoinConstProp(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int not null, b int not null)")
	tk.MustExec("insert into t1 values (1,1)")
	tk.MustExec("create block t2(a int not null, b int not null)")
	tk.MustExec("insert into t2 values (2,2)")

	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.a = t1.a and t2.a > 1)").Check(testkit.Rows(
		"1 1",
	))
	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.b = t1.b and t2.a > 1)").Check(testkit.Rows(
		"1 1",
	))
	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.b = t1.b and t2.b > 1)").Check(testkit.Rows(
		"1 1",
	))
	tk.MustQuery("select q.a in (select count(*) from t1 s where not exists (select 1 from t1 p where q.a > 1 and p.a = s.a)) from t1 q").Check(testkit.Rows(
		"1",
	))
	tk.MustQuery("select q.a in (select not exists (select 1 from t1 p where q.a > 1 and p.a = s.a) from t1 s) from t1 q").Check(testkit.Rows(
		"1",
	))

	tk.MustExec("drop block t1, t2")
	tk.MustExec("create block t1(a int not null, b int)")
	tk.MustExec("insert into t1 values (1,null)")
	tk.MustExec("create block t2(a int not null, b int)")
	tk.MustExec("insert into t2 values (2,2)")

	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.b > t1.b)").Check(testkit.Rows(
		"1 <nil>",
	))
	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t1.a = 2)").Check(testkit.Rows(
		"1 <nil>",
	))
}

func (s *testIntegrationSuite) TestSimplifyOuterJoinWithCast(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int not null, b datetime default null)")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSerialSuite) TestNoneAccessPathsFoundByIsolationRead(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key)")

	_, err := tk.Exec("select * from t")
	c.Assert(err, IsNil)

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines = 'tiflash'")

	// Don't filter allegrosql.SystemDB by isolation read.
	tk.MustQuery("explain select * from allegrosql.stats_meta").Check(testkit.Rows(
		"BlockReader_5 10000.00 root  data:BlockFullScan_4",
		"└─BlockFullScan_4 10000.00 INTERLOCK[einsteindb] block:stats_meta keep order:false, stats:pseudo"))

	_, err = tk.Exec("select * from t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can not find access path matching 'milevadb_isolation_read_engines'(value: 'tiflash'). Available values are 'einsteindb'.")

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines = 'tiflash, einsteindb'")
	tk.MustExec("select * from t")
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tiflash"}
	})
	// Change instance config doesn't affect isolation read.
	tk.MustExec("select * from t")
}

func (s *testIntegrationSerialSuite) TestSelPushDownTiFlash(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b varchar(20))")

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines = 'tiflash'")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSerialSuite) TestBroadcastJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists d1_t")
	tk.MustExec("create block d1_t(d1_k int, value int)")
	tk.MustExec("insert into d1_t values(1,2),(2,3)")
	tk.MustExec("analyze block d1_t")
	tk.MustExec("drop block if exists d2_t")
	tk.MustExec("create block d2_t(d2_k decimal(10,2), value int)")
	tk.MustExec("insert into d2_t values(10.11,2),(10.12,3)")
	tk.MustExec("analyze block d2_t")
	tk.MustExec("drop block if exists d3_t")
	tk.MustExec("create block d3_t(d3_k date, value int)")
	tk.MustExec("insert into d3_t values(date'2010-01-01',2),(date'2010-01-02',3)")
	tk.MustExec("analyze block d3_t")
	tk.MustExec("drop block if exists fact_t")
	tk.MustExec("create block fact_t(d1_k int, d2_k decimal(10,2), d3_k date, col1 int, col2 int, col3 int)")
	tk.MustExec("insert into fact_t values(1,10.11,date'2010-01-01',1,2,3),(1,10.11,date'2010-01-02',1,2,3),(1,10.12,date'2010-01-01',1,2,3),(1,10.12,date'2010-01-02',1,2,3)")
	tk.MustExec("insert into fact_t values(2,10.11,date'2010-01-01',1,2,3),(2,10.11,date'2010-01-02',1,2,3),(2,10.12,date'2010-01-01',1,2,3),(2,10.12,date'2010-01-02',1,2,3)")
	tk.MustExec("analyze block fact_t")

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		if tblInfo.Name.L == "fact_t" || tblInfo.Name.L == "d1_t" || tblInfo.Name.L == "d2_t" || tblInfo.Name.L == "d3_t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@stochastik.milevadb_allow_batch_INTERLOCK = 1")
	tk.MustExec("set @@stochastik.milevadb_opt_broadcast_join = 1")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}

	// out block of out join should not be global
	_, err := tk.Exec("explain select /*+ broadcast_join(fact_t, d1_t), broadcast_join_local(d1_t) */ count(*) from fact_t left join d1_t on fact_t.d1_k = d1_t.d1_k")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can't find a proper physical plan for this query")
	// join with non-equal condition not supported
	_, err = tk.Exec("explain select /*+ broadcast_join(fact_t, d1_t) */ count(*) from fact_t join d1_t on fact_t.d1_k = d1_t.d1_k and fact_t.col1 > d1_t.value")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can't find a proper physical plan for this query")
	// cartsian join not supported
	_, err = tk.Exec("explain select /*+ broadcast_join(fact_t, d1_t) */ count(*) from fact_t join d1_t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[planner:1815]Internal : Can't find a proper physical plan for this query")
}

func (s *testIntegrationSerialSuite) TestAggPushDownEngine(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b varchar(20))")

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines = 'tiflash'")

	tk.MustQuery("desc select approx_count_distinct(a) from t").Check(testkit.Rows(
		"StreamAgg_16 1.00 root  funcs:approx_count_distinct(DeferredCauset#5)->DeferredCauset#3",
		"└─BlockReader_17 1.00 root  data:StreamAgg_8",
		"  └─StreamAgg_8 1.00 INTERLOCK[tiflash]  funcs:approx_count_distinct(test.t.a)->DeferredCauset#5",
		"    └─BlockFullScan_15 10000.00 INTERLOCK[tiflash] block:t keep order:false, stats:pseudo"))

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines = 'einsteindb'")

	tk.MustQuery("desc select approx_count_distinct(a) from t").Check(testkit.Rows(
		"HashAgg_5 1.00 root  funcs:approx_count_distinct(test.t.a)->DeferredCauset#3",
		"└─BlockReader_11 10000.00 root  data:BlockFullScan_10",
		"  └─BlockFullScan_10 10000.00 INTERLOCK[einsteindb] block:t keep order:false, stats:pseudo"))
}

func (s *testIntegrationSerialSuite) TestIssue15110(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists crm_rd_150m")
	tk.MustExec(`CREATE TABLE crm_rd_150m (
	product varchar(256) DEFAULT NULL,
		uks varchar(16) DEFAULT NULL,
		brand varchar(256) DEFAULT NULL,
		cin varchar(16) DEFAULT NULL,
		created_date timestamp NULL DEFAULT NULL,
		quantity int(11) DEFAULT NULL,
		amount decimal(11,0) DEFAULT NULL,
		pl_date timestamp NULL DEFAULT NULL,
		customer_first_date timestamp NULL DEFAULT NULL,
		recent_date timestamp NULL DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		if tblInfo.Name.L == "crm_rd_150m" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines = 'tiflash'")
	tk.MustExec("explain SELECT count(*) FROM crm_rd_150m dataset_48 WHERE (CASE WHEN (month(dataset_48.customer_first_date)) <= 30 THEN '新客' ELSE NULL END) IS NOT NULL;")
}

func (s *testIntegrationSerialSuite) TestReadFromStorageHint(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t, tt, ttt")
	tk.MustExec("create block t(a int, b int, index ia(a))")
	tk.MustExec("create block tt(a int, b int, primary key(a))")
	tk.MustExec("create block ttt(a int, primary key (a desc))")

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Warn              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertALLEGROSQLWarnToStrings(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertALLEGROSQLWarnToStrings(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
}

func (s *testIntegrationSerialSuite) TestReadFromStorageHintAndIsolationRead(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t, tt, ttt")
	tk.MustExec("create block t(a int, b int, index ia(a))")
	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines=\"einsteindb\"")

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Warn              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		tk.Se.GetStochaseinstein_dbars().StmtCtx.SetWarnings(nil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertALLEGROSQLWarnToStrings(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertALLEGROSQLWarnToStrings(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
}

func (s *testIntegrationSerialSuite) TestIsolationReadTiFlashNotChoosePointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, primary key (a))")

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines=\"tiflash\"")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSerialSuite) TestIsolationReadTiFlashUseIndexHint(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, index idx(a));")

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines=\"tiflash\"")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Warn              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertALLEGROSQLWarnToStrings(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertALLEGROSQLWarnToStrings(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
}

func (s *testIntegrationSerialSuite) TestIsolationReadDoNotFilterSystemDB(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("set @@milevadb_isolation_read_engines = \"tiflash\"")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestPartitionBlockStats(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int)partition by range columns(a)(partition p0 values less than (10), partition p1 values less than(20), partition p2 values less than(30));")
	tk.MustExec("insert into t values(21, 1), (22, 2), (23, 3), (24, 4), (15, 5)")
	tk.MustExec("analyze block t")
	tk.MustExec(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestPartitionPruningForInExpr(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int(11), b int) partition by range (a) (partition p0 values less than (4), partition p1 values less than(10), partition p2 values less than maxvalue);")
	tk.MustExec("insert into t values (1, 1),(10, 10),(11, 11)")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestPartitionPruningForEQ(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a datetime, b int) partition by range(weekday(a)) (partition p0 values less than(10), partition p1 values less than (100))")

	is := schemareplicant.GetSchemaReplicant(tk.Se)
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	pt := tbl.(block.PartitionedBlock)
	query, err := expression.ParseSimpleExprWithBlockInfo(tk.Se, "a = '2020-01-01 00:00:00'", tbl.Meta())
	c.Assert(err, IsNil)
	dbName := perceptron.NewCIStr(tk.Se.GetStochaseinstein_dbars().CurrentDB)
	columns, names, err := expression.DeferredCausetInfos2DeferredCausetsAndNames(tk.Se, dbName, tbl.Meta().Name, tbl.Meta().DefCauss(), tbl.Meta())
	c.Assert(err, IsNil)
	// Even the partition is not monotonous, EQ condition should be prune!
	// select * from t where a = '2020-01-01 00:00:00'
	res, err := core.PartitionPruning(tk.Se, pt, []expression.Expression{query}, nil, columns, names)
	c.Assert(err, IsNil)
	c.Assert(res, HasLen, 1)
	c.Assert(res[0], Equals, 0)
}

func (s *testIntegrationSuite) TestErrNoDB(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create user test")
	_, err := tk.Exec("grant select on test1111 to test@'%'")
	c.Assert(errors.Cause(err), Equals, core.ErrNoDB)
	tk.MustExec("use test")
	tk.MustExec("create block test1111 (id int)")
	tk.MustExec("grant select on test1111 to test@'%'")
}

func (s *testIntegrationSuite) TestMaxMinEliminate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key)")
	tk.MustExec("set @@milevadb_enable_clustered_index=1;")
	tk.MustExec("create block cluster_index_t(a int, b int, c int, primary key (a, b));")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestINLJHintSmallBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int not null, b int, key(a))")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("create block t2(a int not null, b int, key(a))")
	tk.MustExec("insert into t2 values(1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("analyze block t1, t2")
	tk.MustExec("explain select /*+ MilevaDB_INLJ(t1) */ * from t1 join t2 on t1.a = t2.a")
}

func (s *testIntegrationSuite) TestIndexJoinUniqueCompositeIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("set @@milevadb_enable_clustered_index=0")
	tk.MustExec("create block t1(a int not null, c int not null)")
	tk.MustExec("create block t2(a int not null, b int not null, c int not null, primary key(a,b))")
	tk.MustExec("insert into t1 values(1,1)")
	tk.MustExec("insert into t2 values(1,1,1),(1,2,1)")
	tk.MustExec("analyze block t1,t2")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestIndexMerge(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, c int, unique index(a), unique index(b), primary key(c))")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestInvisibleIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")

	// Optimizer cannot see invisible indexes.
	tk.MustExec("create block t(a int, b int, unique index i_a (a) invisible, unique index i_b(b))")
	tk.MustExec("insert into t values (1,2)")

	// Optimizer cannot use invisible indexes.
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1"))
	c.Check(tk.MustUseIndex("select a from t order by a", "i_a"), IsFalse)
	tk.MustQuery("select a from t where a > 0").Check(testkit.Rows("1"))
	c.Check(tk.MustUseIndex("select a from t where a > 1", "i_a"), IsFalse)

	// If use invisible indexes in index hint and allegrosql hint, throw an error.
	errStr := "[planner:1176]Key 'i_a' doesn't exist in block 't'"
	tk.MustGetErrMsg("select * from t use index(i_a)", errStr)
	tk.MustGetErrMsg("select * from t force index(i_a)", errStr)
	tk.MustGetErrMsg("select * from t ignore index(i_a)", errStr)
	tk.MustQuery("select /*+ USE_INDEX(t, i_a) */ * from t")
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, errStr)
	tk.MustQuery("select /*+ IGNORE_INDEX(t, i_a), USE_INDEX(t, i_b) */ a from t order by a")
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, errStr)

	tk.MustExec("admin check block t")
	tk.MustExec("admin check index t i_a")
}

// for issue #14822
func (s *testIntegrationSuite) TestIndexJoinBlockRange(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int, b int, primary key (a), key idx_t1_b (b))")
	tk.MustExec("create block t2(a int, b int, primary key (a), key idx_t1_b (b))")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestTopNByConstFunc(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustQuery("select max(t.col) from (select 'a' as col union all select '' as col) as t").Check(testkit.Rows(
		"a",
	))
}

func (s *testIntegrationSuite) TestSubqueryWithTopN(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int)")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestIndexHintWarning(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int, b int, c int, key a(a), key b(b))")
	tk.MustExec("create block t2(a int, b int, c int, key a(a), key b(b))")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Warnings          []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			tk.MustQuery(tt)
			warns := tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
			output[i].Warnings = make([]string, len(warns))
			for j := range warns {
				output[i].Warnings[j] = warns[j].Err.Error()
			}
		})
		tk.MustQuery(tt)
		warns := tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		c.Assert(len(warns), Equals, len(output[i].Warnings))
		for j := range warns {
			c.Assert(warns[j].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warns[j].Err.Error(), Equals, output[i].Warnings[j])
		}
	}
}

func (s *testIntegrationSuite) TestIssue15546(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t, pt, vt")
	tk.MustExec("create block t(a int, b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("create block pt(a int primary key, b int) partition by range(a) (" +
		"PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20), PARTITION `p2` VALUES LESS THAN (30))")
	tk.MustExec("insert into pt values(1, 1), (11, 11), (21, 21)")
	tk.MustExec("create definer='root'@'localhost' view vt(a, b) as select a, b from t")
	tk.MustQuery("select * from pt, vt where pt.a = vt.a").Check(testkit.Rows("1 1 1 1"))
}

func (s *testIntegrationSuite) TestApproxCountDistinctInPartitionBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int(11), b int) partition by range (a) (partition p0 values less than (3), partition p1 values less than maxvalue);")
	tk.MustExec("insert into t values(1, 1), (2, 1), (3, 1), (4, 2), (4, 2)")
	tk.MustExec("set stochastik milevadb_opt_agg_push_down=1")
	tk.MustExec(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)
	tk.MustQuery("explain select approx_count_distinct(a), b from t group by b order by b desc").Check(testkit.Rows("Sort_11 16000.00 root  test.t.b:desc",
		"└─HashAgg_16 16000.00 root  group by:test.t.b, funcs:approx_count_distinct(DeferredCauset#5)->DeferredCauset#4, funcs:firstrow(DeferredCauset#6)->test.t.b",
		"  └─PartitionUnion_17 16000.00 root  ",
		"    ├─HashAgg_18 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->DeferredCauset#5, funcs:firstrow(test.t.b)->DeferredCauset#6, funcs:firstrow(test.t.b)->test.t.b",
		"    │ └─BlockReader_22 10000.00 root  data:BlockFullScan_21",
		"    │   └─BlockFullScan_21 10000.00 INTERLOCK[einsteindb] block:t, partition:p0 keep order:false, stats:pseudo",
		"    └─HashAgg_25 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->DeferredCauset#5, funcs:firstrow(test.t.b)->DeferredCauset#6, funcs:firstrow(test.t.b)->test.t.b",
		"      └─BlockReader_29 10000.00 root  data:BlockFullScan_28",
		"        └─BlockFullScan_28 10000.00 INTERLOCK[einsteindb] block:t, partition:p1 keep order:false, stats:pseudo"))
	tk.MustQuery("select approx_count_distinct(a), b from t group by b order by b desc").Check(testkit.Rows("1 2", "3 1"))
}

func (s *testIntegrationSuite) TestIssue17813(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists hash_partition_overflow")
	tk.MustExec("create block hash_partition_overflow (c0 bigint unsigned) partition by hash(c0) partitions 3")
	tk.MustExec("insert into hash_partition_overflow values (9223372036854775808)")
	tk.MustQuery("select * from hash_partition_overflow where c0 = 9223372036854775808").Check(testkit.Rows("9223372036854775808"))
	tk.MustQuery("select * from hash_partition_overflow where c0 in (1, 9223372036854775808)").Check(testkit.Rows("9223372036854775808"))
}

func (s *testIntegrationSuite) TestHintWithRequiredProperty(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("set @@stochastik.milevadb_executor_concurrency = 4;")
	tk.MustExec("set @@stochastik.milevadb_hash_join_concurrency = 5;")
	tk.MustExec("set @@stochastik.milevadb_distsql_scan_concurrency = 15;")
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, c int, key b(b))")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Warnings          []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			warnings := tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
			output[i].Warnings = make([]string, len(warnings))
			for j, warning := range warnings {
				output[i].Warnings[j] = warning.Err.Error()
			}
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		warnings := tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, len(output[i].Warnings))
		for j, warning := range warnings {
			c.Assert(output[i].Warnings[j], Equals, warning.Err.Error())
		}
	}
}

func (s *testIntegrationSuite) TestIssue15813(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t0, t1")
	tk.MustExec("create block t0(c0 int primary key)")
	tk.MustExec("create block t1(c0 int primary key)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustExec("CREATE INDEX i0 ON t1(c0)")
	tk.MustQuery("select /*+ MERGE_JOIN(t0, t1) */ * from t0, t1 where t0.c0 = t1.c0").Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestFullGroupByOrderBy(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int)")
	tk.MustQuery("select count(a) as b from t group by a order by b").Check(testkit.Rows())
	err := tk.ExecToErr("select count(a) as cnt from t group by a order by b")
	c.Assert(terror.ErrorEqual(err, core.ErrFieldNotInGroupBy), IsTrue)
}

func (s *testIntegrationSuite) TestHintWithoutBlockWarning(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int, b int, c int, key a(a))")
	tk.MustExec("create block t2(a int, b int, c int, key a(a))")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Warnings          []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			tk.MustQuery(tt)
			warns := tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
			output[i].Warnings = make([]string, len(warns))
			for j := range warns {
				output[i].Warnings[j] = warns[j].Err.Error()
			}
		})
		tk.MustQuery(tt)
		warns := tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		c.Assert(len(warns), Equals, len(output[i].Warnings))
		for j := range warns {
			c.Assert(warns[j].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warns[j].Err.Error(), Equals, output[i].Warnings[j])
		}
	}
}

func (s *testIntegrationSuite) TestIssue15858(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key)")
	tk.MustExec("select * from t t1, (select a from t order by a+1) t2 where t1.a = t2.a")
}

func (s *testIntegrationSuite) TestIssue15846(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(t0 INT UNIQUE);")
	tk.MustExec("CREATE TABLE t1(c0 FLOAT);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(t0) VALUES (NULL), (NULL);")
	tk.MustQuery("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;").Check(testkit.Rows("0", "0"))

	tk.MustExec("drop block if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(t0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 FLOAT);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(t0) VALUES (NULL), (NULL);")
	tk.MustQuery("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;").Check(testkit.Rows("0", "0"))

	tk.MustExec("drop block if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(t0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 FLOAT);")
	tk.MustExec("create unique index idx on t0(t0);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(t0) VALUES (NULL), (NULL);")
	tk.MustQuery("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;").Check(testkit.Rows("0", "0"))
}

func (s *testIntegrationSuite) TestFloorUnixTimestampPruning(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists floor_unix_timestamp")
	tk.MustExec(`create block floor_unix_timestamp (ts timestamp(3))
partition by range (floor(unix_timestamp(ts))) (
partition p0 values less than (unix_timestamp('2020-04-05 00:00:00')),
partition p1 values less than (unix_timestamp('2020-04-12 00:00:00')),
partition p2 values less than (unix_timestamp('2020-04-15 00:00:00')))`)
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-04 00:00:00')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-04 23:59:59.999')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-05 00:00:00')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-05 00:00:00.001')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-12 01:02:03.456')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-14 00:00:42')")
	tk.MustQuery("select count(*) from floor_unix_timestamp where '2020-04-05 00:00:00.001' = ts").Check(testkit.Rows("1"))
	tk.MustQuery("select * from floor_unix_timestamp where ts > '2020-04-05 00:00:00' order by ts").Check(testkit.Rows("2020-04-05 00:00:00.001", "2020-04-12 01:02:03.456", "2020-04-14 00:00:42.000"))
	tk.MustQuery("select count(*) from floor_unix_timestamp where ts <= '2020-04-05 23:00:00'").Check(testkit.Rows("4"))
	tk.MustQuery("select * from floor_unix_timestamp partition(p1, p2) where ts > '2020-04-14 00:00:00'").Check(testkit.Rows("2020-04-14 00:00:42.000"))
}

func (s *testIntegrationSuite) TestIssue16290And16292(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a int, b int, primary key(a));")
	tk.MustExec("insert into t values(1, 1);")

	for i := 0; i <= 1; i++ {
		tk.MustExec(fmt.Sprintf("set stochastik milevadb_opt_agg_push_down = %v", i))

		tk.MustQuery("select avg(a) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1.0000"))
		tk.MustQuery("select avg(b) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1.0000"))
		tk.MustQuery("select count(distinct a) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1"))
		tk.MustQuery("select count(distinct b) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1"))
	}
}

func (s *testIntegrationSuite) TestBlockDualWithRequiredProperty(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (a int, b int) partition by range(a) " +
		"(partition p0 values less than(10), partition p1 values less than MAXVALUE)")
	tk.MustExec("create block t2 (a int, b int)")
	tk.MustExec("select /*+ MERGE_JOIN(t1, t2) */ * from t1 partition (p0), t2  where t1.a > 100 and t1.a = t2.a")
}

func (s *testIntegrationSerialSuite) TestIssue16837(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int,b int,c int,d int,e int,unique key idx_ab(a,b),unique key(c),unique key(d))")
	tk.MustQuery("explain select /*+ use_index_merge(t,c,idx_ab) */ * from t where a = 1 or (e = 1 and c = 1)").Check(testkit.Rows(
		"BlockReader_7 10.00 root  data:Selection_6",
		"└─Selection_6 10.00 INTERLOCK[einsteindb]  or(eq(test.t.a, 1), and(eq(test.t.e, 1), eq(test.t.c, 1)))",
		"  └─BlockFullScan_5 10000.00 INTERLOCK[einsteindb] block:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 IndexMerge is inapplicable or disabled"))
	tk.MustExec("insert into t values (2, 1, 1, 1, 2)")
	tk.MustQuery("select /*+ use_index_merge(t,c,idx_ab) */ * from t where a = 1 or (e = 1 and c = 1)").Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestStreamAggProp(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(1),(1),(2)")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Res               []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testIntegrationSuite) TestOptimizeHintOnPartitionBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec(`create block t (
					a int, b int, c varchar(20),
					primary key(a), key(b), key(c)
				) partition by range columns(a) (
					partition p0 values less than(6),
					partition p1 values less than(11),
					partition p2 values less than(16));`)
	tk.MustExec(`insert into t values (1,1,"1"), (2,2,"2"), (8,8,"8"), (11,11,"11"), (15,15,"15")`)

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Warn              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Warn = s.testData.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warn...))
	}
}

func (s *testIntegrationSerialSuite) TestNotReadOnlyALLEGROSQLOnTiFlash(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b varchar(20))")
	tk.MustExec(`set @@milevadb_isolation_read_engines = "tiflash"`)
	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	err := tk.ExecToErr("select * from t for uFIDelate")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, `[planner:1815]Internal : Can not find access path matching 'milevadb_isolation_read_engines'(value: 'tiflash'). Available values are 'tiflash, einsteindb'.`)

	err = tk.ExecToErr("insert into t select * from t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, `[planner:1815]Internal : Can not find access path matching 'milevadb_isolation_read_engines'(value: 'tiflash'). Available values are 'tiflash, einsteindb'.`)

	tk.MustExec("prepare stmt_insert from 'insert into t select * from t where t.a = ?'")
	tk.MustExec("set @a=1")
	err = tk.ExecToErr("execute stmt_insert using @a")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, `[planner:1815]Internal : Can not find access path matching 'milevadb_isolation_read_engines'(value: 'tiflash'). Available values are 'tiflash, einsteindb'.`)
}

func (s *testIntegrationSuite) TestSelectLimit(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(1),(1),(2)")

	// normal test
	tk.MustExec("set @@stochastik.sql_select_limit=1")
	result := tk.MustQuery("select * from t order by a")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t order by a limit 2")
	result.Check(testkit.Rows("1", "1"))
	tk.MustExec("set @@stochastik.sql_select_limit=default")
	result = tk.MustQuery("select * from t order by a")
	result.Check(testkit.Rows("1", "1", "2"))

	// test for subquery
	tk.MustExec("set @@stochastik.sql_select_limit=1")
	result = tk.MustQuery("select * from (select * from t) s order by a")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from (select * from t limit 2) s order by a") // limit write in subquery, has no effect.
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (select * from t limit 1) s") // limit write in subquery, has no effect.
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t where t.a in (select * from t) limit 3") // select_limit will not effect subquery
	result.Check(testkit.Rows("1", "1", "2"))
	result = tk.MustQuery("select * from (select * from t) s limit 3") // select_limit will not effect subquery
	result.Check(testkit.Rows("1", "1", "2"))

	// test for union
	result = tk.MustQuery("select * from t union all select * from t limit 2") // limit outside subquery
	result.Check(testkit.Rows("1", "1"))
	result = tk.MustQuery("select * from t union all (select * from t limit 2)") // limit inside subquery
	result.Check(testkit.Rows("1"))

	// test for prepare & execute
	tk.MustExec("prepare s1 from 'select * from t where a = ?'")
	tk.MustExec("set @a = 1")
	result = tk.MustQuery("execute s1 using @a")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@stochastik.sql_select_limit=default")
	result = tk.MustQuery("execute s1 using @a")
	result.Check(testkit.Rows("1", "1"))
	tk.MustExec("set @@stochastik.sql_select_limit=1")
	tk.MustExec("prepare s2 from 'select * from t where a = ? limit 3'")
	result = tk.MustQuery("execute s2 using @a") // if prepare stmt has limit, select_limit takes no effect.
	result.Check(testkit.Rows("1", "1"))

	// test for create view
	tk.MustExec("set @@stochastik.sql_select_limit=1")
	tk.MustExec("create definer='root'@'localhost' view s as select * from t") // select limit should not effect create view
	result = tk.MustQuery("select * from s")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@stochastik.sql_select_limit=default")
	result = tk.MustQuery("select * from s")
	result.Check(testkit.Rows("1", "1", "2"))

	// test for DML
	tk.MustExec("set @@stochastik.sql_select_limit=1")
	tk.MustExec("create block b (a int)")
	tk.MustExec("insert into b select * from t") // all values are inserted
	result = tk.MustQuery("select * from b limit 3")
	result.Check(testkit.Rows("1", "1", "2"))
	tk.MustExec("uFIDelate b set a = 2 where a = 1") // all values are uFIDelated
	result = tk.MustQuery("select * from b limit 3")
	result.Check(testkit.Rows("2", "2", "2"))
	result = tk.MustQuery("select * from b")
	result.Check(testkit.Rows("2"))
	tk.MustExec("delete from b where a = 2") // all values are deleted
	result = tk.MustQuery("select * from b")
	result.Check(testkit.Rows())
}

func (s *testIntegrationSuite) TestHintberolinaAllegroSQLWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a int, b int, key(a), key(b));")
	tk.MustExec("select /*+ use_index_merge() */ * from t where a = 1 or b = 1;")
	rows := tk.MustQuery("show warnings;").Rows()
	c.Assert(len(rows), Equals, 1)
}

func (s *testIntegrationSuite) TestIssue16935(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (1), (1), (1), (1), (1), (1);")
	tk.MustExec("CREATE definer='root'@'localhost' VIEW v0(c0) AS SELECT NULL FROM t0;")

	tk.MustQuery("SELECT * FROM t0 LEFT JOIN v0 ON TRUE WHERE v0.c0 IS NULL;")
}

func (s *testIntegrationSuite) TestAccessPathOnClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("set @@milevadb_enable_clustered_index = 1")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (a int, b varchar(20), c decimal(40,10), d int, primary key(a,b), key(c))")
	tk.MustExec(`insert into t1 values (1,"111",1.1,11), (2,"222",2.2,12), (3,"333",3.3,13)`)
	tk.MustExec("analyze block t1")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Res               []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testIntegrationSuite) TestClusterIndexUniqueDoubleRead(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database cluster_idx_unique_double_read;")
	tk.MustExec("use cluster_idx_unique_double_read;")
	defer tk.MustExec("drop database cluster_idx_unique_double_read;")
	tk.MustExec("set @@milevadb_enable_clustered_index = 1")
	tk.MustExec("drop block if exists t")

	tk.MustExec("create block t (a varchar(64), b varchar(64), uk int, v int, primary key(a, b), unique key uuk(uk));")
	tk.MustExec("insert t values ('a', 'a1', 1, 11), ('b', 'b1', 2, 22), ('c', 'c1', 3, 33);")
	tk.MustQuery("select * from t use index (uuk);").Check(testkit.Rows("a a1 1 11", "b b1 2 22", "c c1 3 33"))
}

func (s *testIntegrationSuite) TestIndexJoinOnClusteredIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("set @@milevadb_enable_clustered_index = 1")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t (a int, b varchar(20), c decimal(40,10), d int, primary key(a,b), key(c))")
	tk.MustExec(`insert into t values (1,"111",1.1,11), (2,"222",2.2,12), (3,"333",3.3,13)`)
	tk.MustExec("analyze block t")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Res               []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}
func (s *testIntegrationSerialSuite) TestIssue18984(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t, t2")
	tk.MustExec("set milevadb_enable_clustered_index=1")
	tk.MustExec("create block t(a int, b int, c int, primary key(a, b))")
	tk.MustExec("create block t2(a int, b int, c int, d int, primary key(a,b), index idx(c))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3)")
	tk.MustExec("insert into t2 values(1,2,3,4), (2,4,3,5), (1,3,1,1)")
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ * from t right outer join t2 on t.a=t2.c").Check(testkit.Rows(
		"1 1 1 1 3 1 1",
		"3 3 3 1 2 3 4",
		"3 3 3 2 4 3 5"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t2) */ * from t left outer join t2 on t.a=t2.c").Check(testkit.Rows(
		"1 1 1 1 3 1 1",
		"2 2 2 <nil> <nil> <nil> <nil>",
		"3 3 3 1 2 3 4",
		"3 3 3 2 4 3 5"))
}

func (s *testIntegrationSerialSuite) TestExplainAnalyzePointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b varchar(20))")
	tk.MustExec("insert into t values (1,1)")

	res := tk.MustQuery("explain analyze select * from t where a=1;")
	checkExplain := func(rpc string) {
		resBuff := bytes.NewBufferString("")
		for _, event := range res.Rows() {
			fmt.Fprintf(resBuff, "%s\n", event)
		}
		explain := resBuff.String()
		c.Assert(strings.Contains(explain, rpc+":{num_rpc:"), IsTrue, Commentf("%s", explain))
		c.Assert(strings.Contains(explain, "total_time:"), IsTrue, Commentf("%s", explain))
	}
	checkExplain("Get")
	res = tk.MustQuery("explain analyze select * from t where a in (1,2,3);")
	checkExplain("BatchGet")
}

func (s *testIntegrationSuite) TestPartitionExplain(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`create block pt (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)

	tk.MustExec("set @@milevadb_enable_index_merge = 1;")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func (s *testIntegrationSuite) TestPartialBatchPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c_int int, c_str varchar(40), primary key(c_int, c_str))")
	tk.MustExec("insert into t values (3, 'bose')")
	tk.MustQuery("select * from t where c_int in (3)").Check(testkit.Rows(
		"3 bose",
	))
	tk.MustQuery("select * from t where c_int in (3) or c_str in ('yalow') and c_int in (1, 2)").Check(testkit.Rows(
		"3 bose",
	))
}

func (s *testIntegrationSuite) TestIssue19926(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists ta;")
	tk.MustExec("drop block if exists tb;")
	tk.MustExec("drop block if exists tc;")
	tk.MustExec("drop view if exists v;")
	tk.MustExec("CREATE TABLE `ta`  (\n  `id` varchar(36) NOT NULL ,\n  `status` varchar(1) NOT NULL \n);")
	tk.MustExec("CREATE TABLE `tb`  (\n  `id` varchar(36) NOT NULL ,\n  `status` varchar(1) NOT NULL \n);")
	tk.MustExec("CREATE TABLE `tc`  (\n  `id` varchar(36) NOT NULL ,\n  `status` varchar(1) NOT NULL \n);")
	tk.MustExec("insert into ta values('1','1');")
	tk.MustExec("insert into tb values('1','1');")
	tk.MustExec("insert into tc values('1','1');")
	tk.MustExec("create definer='root'@'localhost' view v as\nselect \nconcat(`ta`.`status`,`tb`.`status`) AS `status`, \n`ta`.`id` AS `id`  from (`ta` join `tb`) \nwhere (`ta`.`id` = `tb`.`id`);")
	tk.MustQuery("SELECT tc.status,v.id FROM tc, v WHERE tc.id = v.id AND v.status = '11';").Check(testkit.Rows("1 1"))
}

func (s *testIntegrationSuite) TestDeleteUsingJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int primary key, b int)")
	tk.MustExec("create block t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("insert into t2 values(2,2)")
	tk.MustExec("delete t1.* from t1 join t2 using (a)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 2"))
}

func (s *testIntegrationSerialSuite) Test19942(c *C) {
	collate.SetNewDefCauslationEnabledForTest(true)
	defer collate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("SET @@milevadb_enable_clustered_index=1;")
	tk.MustExec("CREATE TABLE test.`t` (" +
		"  `a` int(11) NOT NULL," +
		"  `b` varchar(10) COLLATE utf8_general_ci NOT NULL," +
		"  `c` varchar(50) COLLATE utf8_general_ci NOT NULL," +
		"  `d` char(10) NOT NULL," +
		"  PRIMARY KEY (`c`)," +
		"  UNIQUE KEY `a_uniq` (`a`)," +
		"  UNIQUE KEY `b_uniq` (`b`)," +
		"  UNIQUE KEY `d_uniq` (`d`)," +
		"  KEY `a_idx` (`a`)," +
		"  KEY `b_idx` (`b`)," +
		"  KEY `d_idx` (`d`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (1, '1', '0', '1');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (2, ' 2', ' 0', ' 2');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (3, '  3 ', '  3 ', '  3 ');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (4, 'a', 'a   ', 'a');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (5, ' A  ', ' A   ', ' A  ');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (6, ' E', 'é        ', ' E');")

	mkr := func() [][]interface{} {
		return solitonutil.RowsWithSep("|",
			"3|  3 |  3 |  3",
			"2| 2  0| 2",
			"5| A  | A   | A",
			"1|1|0|1",
			"4|a|a   |a",
			"6| E|é        | E")
	}
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`a_uniq`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`b_uniq`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`d_uniq`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`a_idx`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`b_idx`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`d_idx`);").Check(mkr())
	tk.MustExec("admin check block t")
}
