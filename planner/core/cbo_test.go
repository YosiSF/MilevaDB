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

package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/planner"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

var _ = Suite(&testAnalyzeSuite{})

type testAnalyzeSuite struct {
	testData solitonutil.TestData
}

func (s *testAnalyzeSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "analyze_suite")
	c.Assert(err, IsNil)
}

func (s *testAnalyzeSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testAnalyzeSuite) loadBlockStats(fileName string, dom *petri.Petri) error {
	statsPath := filepath.Join("testdata", fileName)
	bytes, err := ioutil.ReadFile(statsPath)
	if err != nil {
		return err
	}
	statsTbl := &handle.JSONBlock{}
	err = json.Unmarshal(bytes, statsTbl)
	if err != nil {
		return err
	}
	statsHandle := dom.StatsHandle()
	err = statsHandle.LoadStatsFromJSON(dom.SchemaReplicant(), statsTbl)
	if err != nil {
		return err
	}
	return nil
}

func (s *testAnalyzeSuite) TestExplainAnalyze(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tk.MustExec("create block t1(a int, b int, c int, key idx(a, b))")
	tk.MustExec("create block t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5)")
	tk.MustExec("insert into t2 values (2, 22), (3, 33), (5, 55), (233, 2), (333, 3), (3434, 5)")
	tk.MustExec("analyze block t1, t2")
	rs := tk.MustQuery("explain analyze select t1.a, t1.b, sum(t1.c) from t1 join t2 on t1.a = t2.b where t1.a > 1")
	c.Assert(len(rs.Rows()), Equals, 10)
	for _, event := range rs.Rows() {
		c.Assert(len(event), Equals, 9)
		execInfo := event[5].(string)
		c.Assert(strings.Contains(execInfo, "time"), Equals, true)
		c.Assert(strings.Contains(execInfo, "loops"), Equals, true)
		if strings.Contains(event[0].(string), "Reader") || strings.Contains(event[0].(string), "IndexLookUp") {
			c.Assert(strings.Contains(execInfo, "INTERLOCKr_cache_hit_ratio"), Equals, true)
		}
	}
}

// TestCBOWithoutAnalyze tests the plan with stats that only have count info.
func (s *testAnalyzeSuite) TestCBOWithoutAnalyze(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("create block t1 (a int)")
	testKit.MustExec("create block t2 (a int)")
	h := dom.StatsHandle()
	c.Assert(h.HandleDBSEvent(<-h.DBSEventCh()), IsNil)
	c.Assert(h.HandleDBSEvent(<-h.DBSEventCh()), IsNil)
	testKit.MustExec("insert into t1 values (1), (2), (3), (4), (5), (6)")
	testKit.MustExec("insert into t2 values (1), (2), (3), (4), (5), (6)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(dom.SchemaReplicant()), IsNil)
	testKit.MustQuery("explain select * from t1, t2 where t1.a = t2.a").Check(testkit.Rows(
		"HashJoin_8 7.49 root  inner join, equal:[eq(test.t1.a, test.t2.a)]",
		"├─BlockReader_15(Build) 5.99 root  data:Selection_14",
		"│ └─Selection_14 5.99 INTERLOCK[einsteindb]  not(isnull(test.t2.a))",
		"│   └─BlockFullScan_13 6.00 INTERLOCK[einsteindb] block:t2 keep order:false, stats:pseudo",
		"└─BlockReader_12(Probe) 5.99 root  data:Selection_11",
		"  └─Selection_11 5.99 INTERLOCK[einsteindb]  not(isnull(test.t1.a))",
		"    └─BlockFullScan_10 6.00 INTERLOCK[einsteindb] block:t1 keep order:false, stats:pseudo",
	))
	testKit.MustQuery("explain format = 'hint' select * from t1, t2 where t1.a = t2.a").Check(testkit.Rows(
		"use_index(@`sel_1` `test`.`t1` ), use_index(@`sel_1` `test`.`t2` ), hash_join(@`sel_1` `test`.`t1`)"))
}

func (s *testAnalyzeSuite) TestStraightJoin(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	testKit.MustExec("use test")
	h := dom.StatsHandle()
	for _, tblName := range []string{"t1", "t2", "t3", "t4"} {
		testKit.MustExec(fmt.Sprintf("create block %s (a int)", tblName))
		c.Assert(h.HandleDBSEvent(<-h.DBSEventCh()), IsNil)
	}
	var input []string
	var output [][]string
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func (s *testAnalyzeSuite) TestBlockDual(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustExec(`use test`)
	h := dom.StatsHandle()
	testKit.MustExec(`create block t(a int)`)
	testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
	c.Assert(h.HandleDBSEvent(<-h.DBSEventCh()), IsNil)

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(dom.SchemaReplicant()), IsNil)

	testKit.MustQuery(`explain select * from t where 1 = 0`).Check(testkit.Rows(
		`BlockDual_6 0.00 root  rows:0`,
	))

	testKit.MustQuery(`explain select * from t where 1 = 1 limit 0`).Check(testkit.Rows(
		`BlockDual_5 0.00 root  rows:0`,
	))
}

func (s *testAnalyzeSuite) TestEstimation(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
		statistics.RatioOfPseudoEstimate.CausetStore(0.7)
	}()
	statistics.RatioOfPseudoEstimate.CausetStore(10.0)
	testKit.MustExec("use test")
	testKit.MustExec("create block t (a int)")
	testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	h := dom.StatsHandle()
	h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze block t")
	for i := 1; i <= 8; i++ {
		testKit.MustExec("delete from t where a = ?", i)
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(dom.SchemaReplicant()), IsNil)
	testKit.MustQuery("explain select count(*) from t group by a").Check(testkit.Rows(
		"HashAgg_9 2.00 root  group by:test.t.a, funcs:count(DeferredCauset#4)->DeferredCauset#3",
		"└─BlockReader_10 2.00 root  data:HashAgg_5",
		"  └─HashAgg_5 2.00 INTERLOCK[einsteindb]  group by:test.t.a, funcs:count(1)->DeferredCauset#4",
		"    └─BlockFullScan_8 8.00 INTERLOCK[einsteindb] block:t keep order:false",
	))
}

func constructInsertALLEGROSQL(i, n int) string {
	allegrosql := "insert into t (a,b,c,e)values "
	for j := 0; j < n; j++ {
		allegrosql += fmt.Sprintf("(%d, %d, '%d', %d)", i*n+j, i, i+j, i*n+j)
		if j != n-1 {
			allegrosql += ", "
		}
	}
	return allegrosql
}

func (s *testAnalyzeSuite) TestIndexRead(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	testKit.MustExec("set @@stochastik.milevadb_executor_concurrency = 4;")
	testKit.MustExec("set @@stochastik.milevadb_hash_join_concurrency = 5;")
	testKit.MustExec("set @@stochastik.milevadb_distsql_scan_concurrency = 15;")

	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t, t1")
	testKit.MustExec("create block t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("create index d on t (d)")
	testKit.MustExec("create index e on t (e)")
	testKit.MustExec("create index b_c on t (b,c)")
	testKit.MustExec("create index ts on t (ts)")
	testKit.MustExec("create block t1 (a int, b int, index idx(a), index idxx(b))")

	// This stats is generated by following format:
	// fill (a, b, c, e) as (i*100+j, i, i+j, i*100+j), i and j is dependent and range of this two are [0, 99].
	err = s.loadBlockStats("analyzesSuiteTestIndexReadT.json", dom)
	c.Assert(err, IsNil)
	for i := 1; i < 16; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t1 values(%v, %v)", i, i))
	}
	testKit.MustExec("analyze block t1")
	ctx := testKit.Se.(stochastikctx.Context)
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	for i, tt := range input {
		stmts, err := stochastik.Parse(ctx, tt)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := petri.GetPetri(ctx).SchemaReplicant()
		err = core.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		planString := core.ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], Commentf("for %s", tt))
	}
}

func (s *testAnalyzeSuite) TestEmptyBlock(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t, t1")
	testKit.MustExec("create block t (c1 int)")
	testKit.MustExec("create block t1 (c1 int)")
	testKit.MustExec("analyze block t, t1")
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		ctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(ctx, tt)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := petri.GetPetri(ctx).SchemaReplicant()
		err = core.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		planString := core.ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], Commentf("for %s", tt))
	}
}

func (s *testAnalyzeSuite) TestAnalyze(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t, t1, t2, t3")
	testKit.MustExec("create block t (a int, b int)")
	testKit.MustExec("create index a on t (a)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("insert into t (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze block t")

	testKit.MustExec("create block t1 (a int, b int)")
	testKit.MustExec("create index a on t1 (a)")
	testKit.MustExec("create index b on t1 (b)")
	testKit.MustExec("insert into t1 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")

	testKit.MustExec("create block t2 (a int, b int)")
	testKit.MustExec("create index a on t2 (a)")
	testKit.MustExec("create index b on t2 (b)")
	testKit.MustExec("insert into t2 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze block t2 index a")

	testKit.MustExec("create block t3 (a int, b int)")
	testKit.MustExec("create index a on t3 (a)")

	testKit.MustExec("create block t4 (a int, b int) partition by range (a) (partition p1 values less than (2), partition p2 values less than (3))")
	testKit.MustExec("create index a on t4 (a)")
	testKit.MustExec("create index b on t4 (b)")
	testKit.MustExec("insert into t4 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze block t4")

	testKit.MustExec("create view v as select * from t")
	_, err = testKit.Exec("analyze block v")
	c.Assert(err.Error(), Equals, "analyze view v is not supported now.")
	testKit.MustExec("drop view v")

	testKit.MustExec("create sequence seq")
	_, err = testKit.Exec("analyze block seq")
	c.Assert(err.Error(), Equals, "analyze sequence seq is not supported now.")
	testKit.MustExec("drop sequence seq")

	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	for i, tt := range input {
		ctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(ctx, tt)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		err = executor.ResetContextOfStmt(ctx, stmt)
		c.Assert(err, IsNil)
		is := petri.GetPetri(ctx).SchemaReplicant()
		err = core.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		planString := core.ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], Commentf("for %s", tt))
	}
}

func (s *testAnalyzeSuite) TestOutdatedAnalyze(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("create block t (a int, b int, index idx(a))")
	for i := 0; i < 10; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d,%d)", i, i))
	}
	h := dom.StatsHandle()
	h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze block t")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(dom.SchemaReplicant()), IsNil)
	statistics.RatioOfPseudoEstimate.CausetStore(10.0)
	testKit.MustQuery("explain select * from t where a <= 5 and b <= 5").Check(testkit.Rows(
		"BlockReader_7 29.77 root  data:Selection_6",
		"└─Selection_6 29.77 INTERLOCK[einsteindb]  le(test.t.a, 5), le(test.t.b, 5)",
		"  └─BlockFullScan_5 80.00 INTERLOCK[einsteindb] block:t keep order:false",
	))
	statistics.RatioOfPseudoEstimate.CausetStore(0.7)
	testKit.MustQuery("explain select * from t where a <= 5 and b <= 5").Check(testkit.Rows(
		"BlockReader_7 8.84 root  data:Selection_6",
		"└─Selection_6 8.84 INTERLOCK[einsteindb]  le(test.t.a, 5), le(test.t.b, 5)",
		"  └─BlockFullScan_5 80.00 INTERLOCK[einsteindb] block:t keep order:false, stats:pseudo",
	))
}

func (s *testAnalyzeSuite) TestPreparedNullParam(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	defer config.RestoreFunc()()
	flags := []bool{false, true}
	for _, flag := range flags {
		config.UFIDelateGlobal(func(conf *config.Config) {
			conf.PreparedPlanCache.Enabled = flag
			conf.PreparedPlanCache.Capacity = 100
		})
		testKit := testkit.NewTestKit(c, causetstore)
		testKit.MustExec("use test")
		testKit.MustExec("drop block if exists t")
		testKit.MustExec("create block t (id int, KEY id (id))")
		testKit.MustExec("insert into t values (1), (2), (3)")

		allegrosql := "select * from t where id = ?"
		best := "Dual"

		ctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(ctx, allegrosql)
		c.Assert(err, IsNil)
		stmt := stmts[0]

		is := petri.GetPetri(ctx).SchemaReplicant()
		err = core.Preprocess(ctx, stmt, is, core.InPrepare)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)

		c.Assert(core.ToString(p), Equals, best, Commentf("for %s", allegrosql))
	}
}

func (s *testAnalyzeSuite) TestNullCount(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t (a int, b int, index idx(a))")
	testKit.MustExec("insert into t values (null, null), (null, null)")
	testKit.MustExec("analyze block t")
	var input []string
	var output [][]string
	s.testData.GetTestCases(c, &input, &output)
	for i := 0; i < 2; i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
	h := dom.StatsHandle()
	h.Clear()
	c.Assert(h.UFIDelate(dom.SchemaReplicant()), IsNil)
	for i := 2; i < 4; i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func (s *testAnalyzeSuite) TestCorrelatedEstimation(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tk.MustExec("create block t(a int, b int, c int, index idx(c,b,a))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9),(10,10,10)")
	tk.MustExec("analyze block t")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		rs := tk.MustQuery(tt)
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(rs.Rows())
		})
		rs.Check(testkit.Rows(output[i]...))
	}
}

func (s *testAnalyzeSuite) TestInconsistentEstimation(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("create block t(a int, b int, c int, index ab(a,b), index ac(a,c))")
	tk.MustExec("insert into t values (1,1,1), (1000,1000,1000)")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values (5,5,5), (10,10,10)")
	}
	tk.MustExec("analyze block t with 2 buckets")
	// Force using the histogram to estimate.
	tk.MustExec("uFIDelate allegrosql.stats_histograms set stats_ver = 0")
	dom.StatsHandle().Clear()
	dom.StatsHandle().UFIDelate(dom.SchemaReplicant())
	// Using the histogram (a, b) to estimate `a = 5` will get 1.22, while using the CM Sketch to estimate
	// the `a = 5 and c = 5` will get 10, it is not consistent.
	tk.MustQuery("explain select * from t use index(ab) where a = 5 and c = 5").
		Check(testkit.Rows(
			"IndexLookUp_8 10.00 root  ",
			"├─IndexRangeScan_5(Build) 12.50 INTERLOCK[einsteindb] block:t, index:ab(a, b) range:[5,5], keep order:false",
			"└─Selection_7(Probe) 10.00 INTERLOCK[einsteindb]  eq(test.t.c, 5)",
			"  └─BlockRowIDScan_6 12.50 INTERLOCK[einsteindb] block:t keep order:false",
		))
}

func newStoreWithBootstrap() (ekv.CausetStorage, *petri.Petri, error) {
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()

	dom, err := stochastik.BootstrapStochastik(causetstore)
	if err != nil {
		return nil, nil, err
	}

	dom.SetStatsUFIDelating(true)
	return causetstore, dom, errors.Trace(err)
}

func BenchmarkOptimize(b *testing.B) {
	c := &C{}
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("create index d on t (d)")
	testKit.MustExec("create index e on t (e)")
	testKit.MustExec("create index b_c on t (b,c)")
	testKit.MustExec("create index ts on t (ts)")
	for i := 0; i < 100; i++ {
		testKit.MustExec(constructInsertALLEGROSQL(i, 100))
	}
	testKit.MustExec("analyze block t")
	tests := []struct {
		allegrosql string
		best       string
	}{
		{
			allegrosql: "select count(*) from t group by e",
			best:       "IndexReader(Index(t.e)[[NULL,+inf]])->StreamAgg",
		},
		{
			allegrosql: "select count(*) from t where e <= 10 group by e",
			best:       "IndexReader(Index(t.e)[[-inf,10]])->StreamAgg",
		},
		{
			allegrosql: "select count(*) from t where e <= 50",
			best:       "IndexReader(Index(t.e)[[-inf,50]]->HashAgg)->HashAgg",
		},
		{
			allegrosql: "select count(*) from t where c > '1' group by b",
			best:       "IndexReader(Index(t.b_c)[[NULL,+inf]]->Sel([gt(test.t.c, 1)]))->StreamAgg",
		},
		{
			allegrosql: "select count(*) from t where e = 1 group by b",
			best:       "IndexLookUp(Index(t.e)[[1,1]], Block(t)->HashAgg)->HashAgg",
		},
		{
			allegrosql: "select count(*) from t where e > 1 group by b",
			best:       "BlockReader(Block(t)->Sel([gt(test.t.e, 1)])->HashAgg)->HashAgg",
		},
		{
			allegrosql: "select count(e) from t where t.b <= 20",
			best:       "IndexLookUp(Index(t.b)[[-inf,20]], Block(t)->HashAgg)->HashAgg",
		},
		{
			allegrosql: "select count(e) from t where t.b <= 30",
			best:       "IndexLookUp(Index(t.b)[[-inf,30]], Block(t)->HashAgg)->HashAgg",
		},
		{
			allegrosql: "select count(e) from t where t.b <= 40",
			best:       "IndexLookUp(Index(t.b)[[-inf,40]], Block(t)->HashAgg)->HashAgg",
		},
		{
			allegrosql: "select count(e) from t where t.b <= 50",
			best:       "BlockReader(Block(t)->Sel([le(test.t.b, 50)])->HashAgg)->HashAgg",
		},
		{
			allegrosql: "select * from t where t.b <= 40",
			best:       "IndexLookUp(Index(t.b)[[-inf,40]], Block(t))",
		},
		{
			allegrosql: "select * from t where t.b <= 50",
			best:       "BlockReader(Block(t)->Sel([le(test.t.b, 50)]))",
		},
		// test panic
		{
			allegrosql: "select * from t where 1 and t.b <= 50",
			best:       "BlockReader(Block(t)->Sel([le(test.t.b, 50)]))",
		},
		{
			allegrosql: "select * from t where t.b <= 100 order by t.a limit 1",
			best:       "BlockReader(Block(t)->Sel([le(test.t.b, 100)])->Limit)->Limit",
		},
		{
			allegrosql: "select * from t where t.b <= 1 order by t.a limit 10",
			best:       "IndexLookUp(Index(t.b)[[-inf,1]]->TopN([test.t.a],0,10), Block(t))->TopN([test.t.a],0,10)",
		},
		{
			allegrosql: "select * from t use index(b) where b = 1 order by a",
			best:       "IndexLookUp(Index(t.b)[[1,1]], Block(t))->Sort",
		},
		// test datetime
		{
			allegrosql: "select * from t where d < cast('1991-09-05' as datetime)",
			best:       "IndexLookUp(Index(t.d)[[-inf,1991-09-05 00:00:00)], Block(t))",
		},
		// test timestamp
		{
			allegrosql: "select * from t where ts < '1991-09-05'",
			best:       "IndexLookUp(Index(t.ts)[[-inf,1991-09-05 00:00:00)], Block(t))",
		},
	}
	for _, tt := range tests {
		ctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(ctx, tt.allegrosql)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := petri.GetPetri(ctx).SchemaReplicant()
		err = core.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)

		b.Run(tt.allegrosql, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
				c.Assert(err, IsNil)
			}
			b.ReportAllocs()
		})
	}
}

func (s *testAnalyzeSuite) TestIssue9562(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk.MustExec("use test")
	var input [][]string
	var output []struct {
		ALLEGROALLEGROSQL []string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				output[i].ALLEGROALLEGROSQL = ts
				if j == len(ts)-1 {
					output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			}
		}
	}
}

func (s *testAnalyzeSuite) TestIssue9805(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec(`
		create block t1 (
			id bigint primary key,
			a bigint not null,
			b varchar(100) not null,
			c varchar(10) not null,
			d bigint as (a % 30) not null,
			key (d, b, c)
		)
	`)
	tk.MustExec(`
		create block t2 (
			id varchar(50) primary key,
			a varchar(100) unique,
			b datetime,
			c varchar(45),
			d int not null unique auto_increment
		)
	`)
	// Test when both blocks are empty, EXPLAIN ANALYZE for IndexLookUp would not panic.
	tk.MustExec("explain analyze select /*+ MilevaDB_INLJ(t2) */ t1.id, t2.a from t1 join t2 on t1.a = t2.d where t1.b = 't2' and t1.d = 4")
}

func (s *testAnalyzeSuite) TestLimitCrossEstimation(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk.MustExec("set @@stochastik.milevadb_executor_concurrency = 4;")
	tk.MustExec("set @@stochastik.milevadb_hash_join_concurrency = 5;")
	tk.MustExec("set @@stochastik.milevadb_distsql_scan_concurrency = 15;")
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int not null, c int not null default 0, index idx_bc(b, c))")
	var input [][]string
	var output []struct {
		ALLEGROALLEGROSQL []string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				output[i].ALLEGROALLEGROSQL = ts
				if j == len(ts)-1 {
					output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			}
		}
	}
}

func (s *testAnalyzeSuite) TestUFIDelateProjEliminate(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int)")
	tk.MustExec("explain uFIDelate t t1, (select distinct b from t) t2 set t1.b = t2.b")
}

func (s *testAnalyzeSuite) TestTiFlashCostPerceptron(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk.MustExec("use test")
	tk.MustExec("create block t (a int, b int, c int, primary key(a))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3)")

	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.CIStr{O: "test", L: "test"}, perceptron.CIStr{O: "t", L: "t"})
	c.Assert(err, IsNil)
	// Set the reploged TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &perceptron.TiFlashReplicaInfo{Count: 1, Available: true}

	var input, output [][]string
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				if j == len(ts)-1 {
					output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
			}
		}
	}
}

func (s *testAnalyzeSuite) TestIndexEqualUnknown(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t, t1")
	testKit.MustExec("set @@milevadb_enable_clustered_index=0")
	testKit.MustExec("CREATE TABLE t(a bigint(20) NOT NULL, b bigint(20) NOT NULL, c bigint(20) NOT NULL, PRIMARY KEY (a,c,b), KEY (b))")
	err = s.loadBlockStats("analyzeSuiteTestIndexEqualUnknownT.json", dom)
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}
