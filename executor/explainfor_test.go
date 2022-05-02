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
	"crypto/tls"
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/kvcache"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	. "github.com/whtcorpsinc/check"
)

// mockStochastikManager is a mocked stochastik manager which is used for test.
type mockStochastikManager1 struct {
	PS []*soliton.ProcessInfo
}

// ShowProcessList implements the StochastikManager.ShowProcessList interface.
func (msm *mockStochastikManager1) ShowProcessList() map[uint64]*soliton.ProcessInfo {
	ret := make(map[uint64]*soliton.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockStochastikManager1) GetProcessInfo(id uint64) (*soliton.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &soliton.ProcessInfo{}, false
}

// Kill implements the StochastikManager.Kill interface.
func (msm *mockStochastikManager1) Kill(cid uint64, query bool) {

}

func (msm *mockStochastikManager1) UFIDelateTLSConfig(cfg *tls.Config) {
}

func (s *testSuite) TestExplainFor(c *C) {
	tkRoot := testkit.NewTestKitWithInit(c, s.causetstore)
	tkUser := testkit.NewTestKitWithInit(c, s.causetstore)
	tkRoot.MustExec("create block t1(c1 int, c2 int)")
	tkRoot.MustExec("create block t2(c1 int, c2 int)")
	tkRoot.MustExec("create user tu@'%'")
	tkRoot.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tkUser.Se.Auth(&auth.UserIdentity{Username: "tu", Hostname: "localhost", CurrentUser: true, AuthUsername: "tu", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkRoot.MustQuery("select * from t1;")
	tkRootProcess := tkRoot.Se.ShowProcess()
	ps := []*soliton.ProcessInfo{tkRootProcess}
	tkRoot.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})
	tkUser.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})
	tkRoot.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Check(testkit.Events(
		"BlockReader_5 10000.00 root  data:BlockFullScan_4",
		"└─BlockFullScan_4 10000.00 INTERLOCK[einsteindb] block:t1 keep order:false, stats:pseudo",
	))
	err := tkUser.ExecToErr(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
	c.Check(core.ErrAccessDenied.Equal(err), IsTrue)
	err = tkUser.ExecToErr("explain for connection 42")
	c.Check(core.ErrNoSuchThread.Equal(err), IsTrue)

	tkRootProcess.Plan = nil
	ps = []*soliton.ProcessInfo{tkRootProcess}
	tkRoot.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})
	tkRoot.MustExec(fmt.Sprintf("explain for connection %d", tkRootProcess.ID))
}

func (s *testSuite) TestIssue11124(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists kankan1")
	tk.MustExec("drop block if exists kankan2")
	tk.MustExec("create block kankan1(id int, name text);")
	tk.MustExec("create block kankan2(id int, h1 text);")
	tk.MustExec("insert into kankan1 values(1, 'a'), (2, 'a');")
	tk.MustExec("insert into kankan2 values(2, 'z');")
	tk.MustQuery("select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1'")
	tkRootProcess := tk.Se.ShowProcess()
	ps := []*soliton.ProcessInfo{tkRootProcess}
	tk.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})
	tk2.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})

	rs := tk.MustQuery("explain select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1'").Events()
	rs2 := tk2.MustQuery(fmt.Sprintf("explain for connection %d", tkRootProcess.ID)).Events()
	for i := range rs {
		c.Assert(rs[i], DeepEquals, rs2[i])
	}
}

func (s *testSuite) TestExplainMemBlockPredicate(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustQuery("desc select * from METRICS_SCHEMA.milevadb_query_duration where time >= '2020-12-23 16:10:13' and time <= '2020-12-23 16:30:13' ").Check(testkit.Events(
		"MemBlockScan_5 10000.00 root block:milevadb_query_duration PromQL:histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance)), start_time:2020-12-23 16:10:13, end_time:2020-12-23 16:30:13, step:1m0s"))
	tk.MustQuery("desc select * from METRICS_SCHEMA.up where time >= '2020-12-23 16:10:13' and time <= '2020-12-23 16:30:13' ").Check(testkit.Events(
		"MemBlockScan_5 10000.00 root block:up PromQL:up{}, start_time:2020-12-23 16:10:13, end_time:2020-12-23 16:30:13, step:1m0s"))
	tk.MustQuery("desc select * from information_schema.cluster_log where time >= '2020-12-23 16:10:13' and time <= '2020-12-23 16:30:13'").Check(testkit.Events(
		"MemBlockScan_5 10000.00 root block:CLUSTER_LOG start_time:2020-12-23 16:10:13, end_time:2020-12-23 16:30:13"))
	tk.MustQuery("desc select * from information_schema.cluster_log where level in ('warn','error') and time >= '2020-12-23 16:10:13' and time <= '2020-12-23 16:30:13'").Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:CLUSTER_LOG start_time:2020-12-23 16:10:13, end_time:2020-12-23 16:30:13, log_levels:["error","warn"]`))
	tk.MustQuery("desc select * from information_schema.cluster_log where type in ('high_cpu_1','high_memory_1') and time >= '2020-12-23 16:10:13' and time <= '2020-12-23 16:30:13'").Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:CLUSTER_LOG start_time:2020-12-23 16:10:13, end_time:2020-12-23 16:30:13, node_types:["high_cpu_1","high_memory_1"]`))
	tk.MustQuery("desc select * from information_schema.slow_query").Check(testkit.Events(
		"MemBlockScan_4 10000.00 root block:SLOW_QUERY only search in the current 'milevadb-slow.log' file"))
	tk.MustQuery("desc select * from information_schema.slow_query where time >= '2020-12-23 16:10:13' and time <= '2020-12-23 16:30:13'").Check(testkit.Events(
		"MemBlockScan_5 10000.00 root block:SLOW_QUERY start_time:2020-12-23 16:10:13.000000, end_time:2020-12-23 16:30:13.000000"))
	tk.MustExec("set @@time_zone = '+00:00';")
	tk.MustQuery("desc select * from information_schema.slow_query where time >= '2020-12-23 16:10:13' and time <= '2020-12-23 16:30:13'").Check(testkit.Events(
		"MemBlockScan_5 10000.00 root block:SLOW_QUERY start_time:2020-12-23 16:10:13.000000, end_time:2020-12-23 16:30:13.000000"))
}

func (s *testSuite) TestExplainClusterBlock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.cluster_config where type in ('einsteindb', 'milevadb')")).Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:CLUSTER_CONFIG node_types:["milevadb","einsteindb"]`))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.cluster_config where instance='192.168.1.7:2379'")).Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:CLUSTER_CONFIG instances:["192.168.1.7:2379"]`))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.cluster_config where type='milevadb' and instance='192.168.1.7:2379'")).Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:CLUSTER_CONFIG node_types:["milevadb"], instances:["192.168.1.7:2379"]`))
}

func (s *testSuite) TestInspectionResultBlock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustQuery("desc select * from information_schema.inspection_result where rule = 'dbs' and rule = 'config'").Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:INSPECTION_RESULT skip_inspection:true`))
	tk.MustQuery("desc select * from information_schema.inspection_result where rule in ('dbs', 'config')").Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:INSPECTION_RESULT rules:["config","dbs"], items:[]`))
	tk.MustQuery("desc select * from information_schema.inspection_result where item in ('dbs.lease', 'raftstore.threadpool')").Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:INSPECTION_RESULT rules:[], items:["dbs.lease","raftstore.threadpool"]`))
	tk.MustQuery("desc select * from information_schema.inspection_result where item in ('dbs.lease', 'raftstore.threadpool') and rule in ('dbs', 'config')").Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:INSPECTION_RESULT rules:["config","dbs"], items:["dbs.lease","raftstore.threadpool"]`))
}

func (s *testSuite) TestInspectionMemruleBlock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.inspection_rules where type='inspection'")).Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:INSPECTION_RULES node_types:["inspection"]`))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.inspection_rules where type='inspection' or type='summary'")).Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:INSPECTION_RULES node_types:["inspection","summary"]`))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.inspection_rules where type='inspection' and type='summary'")).Check(testkit.Events(
		`MemBlockScan_5 10000.00 root block:INSPECTION_RULES skip_request: true`))
}

type testPrepareSerialSuite struct {
	*baseTestSuite
}

func (s *testPrepareSerialSuite) TestExplainForConnPlanCache(c *C) {
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	var err error
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk1.MustExec("use test")
	tk1.MustExec("drop block if exists t")
	tk1.MustExec("create block t(a int)")
	tk1.MustExec("prepare stmt from 'select * from t where a = ?'")
	tk1.MustExec("set @p0='1'")

	executeQuery := "execute stmt using @p0"
	explainQuery := "explain for connection " + strconv.FormatUint(tk1.Se.ShowProcess().ID, 10)
	explainResult := testkit.Events(
		"BlockReader_7 8000.00 root  data:Selection_6",
		"└─Selection_6 8000.00 INTERLOCK[einsteindb]  eq(cast(test.t.a), 1)",
		"  └─BlockFullScan_5 10000.00 INTERLOCK[einsteindb] block:t keep order:false, stats:pseudo",
	)

	// Now the ProcessInfo held by mockStochastikManager1 will not be uFIDelated in real time.
	// So it needs to be reset every time before tk2 query.
	// TODO: replace mockStochastikManager1 with another mockStochastikManager.

	// single test
	tk1.MustExec(executeQuery)
	tk2.Se.SetStochastikManager(&mockStochastikManager1{
		PS: []*soliton.ProcessInfo{tk1.Se.ShowProcess()},
	})
	tk2.MustQuery(explainQuery).Check(explainResult)

	// multiple test, '1000' is both effective and efficient.
	repeats := 1000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for i := 0; i < repeats; i++ {
			tk1.MustExec(executeQuery)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < repeats; i++ {
			tk2.Se.SetStochastikManager(&mockStochastikManager1{
				PS: []*soliton.ProcessInfo{tk1.Se.ShowProcess()},
			})
			tk2.MustQuery(explainQuery).Check(explainResult)
		}
		wg.Done()
	}()

	wg.Wait()
}

func (s *testPrepareSerialSuite) TestExplainDotForExplainPlan(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	rows := tk.MustQuery("select connection_id()").Events()
	c.Assert(len(rows), Equals, 1)
	connID := rows[0][0].(string)
	tk.MustQuery("explain select 1").Check(testkit.Events(
		"Projection_3 1.00 root  1->DeferredCauset#1",
		"└─BlockDual_4 1.00 root  rows:1",
	))

	tkProcess := tk.Se.ShowProcess()
	ps := []*soliton.ProcessInfo{tkProcess}
	tk.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain format=\"dot\" for connection %s", connID)).Check(nil)
}

func (s *testPrepareSerialSuite) TestExplainDotForQuery(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk2 := testkit.NewTestKit(c, s.causetstore)

	rows := tk.MustQuery("select connection_id()").Events()
	c.Assert(len(rows), Equals, 1)
	connID := rows[0][0].(string)
	tk.MustQuery("select 1")
	tkProcess := tk.Se.ShowProcess()
	ps := []*soliton.ProcessInfo{tkProcess}
	tk.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})

	expected := tk2.MustQuery("explain format=\"dot\" select 1").Events()
	got := tk.MustQuery(fmt.Sprintf("explain format=\"dot\" for connection %s", connID)).Events()
	for i := range got {
		c.Assert(got[i], DeepEquals, expected[i])
	}
}

func (s *testSuite) TestExplainBlockStorage(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema'")).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TABLE_STORAGE_STATS schemaReplicant:[\"information_schema\"]")))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TABLE_STORAGE_STATS where TABLE_NAME = 'schemata'")).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TABLE_STORAGE_STATS block:[\"schemata\"]")))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema' and TABLE_NAME = 'schemata'")).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TABLE_STORAGE_STATS schemaReplicant:[\"information_schema\"], block:[\"schemata\"]")))
}

func (s *testSuite) TestInspectionSummaryBlock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustQuery("desc select * from information_schema.inspection_summary where rule='dbs'").Check(testkit.Events(
		`Selection_5 8000.00 root  eq(DeferredCauset#1, "dbs")`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["dbs"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where 'dbs'=rule or rule='config'").Check(testkit.Events(
		`Selection_5 8000.00 root  or(eq("dbs", DeferredCauset#1), eq(DeferredCauset#1, "config"))`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["config","dbs"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where 'dbs'=rule or rule='config' or rule='slow_query'").Check(testkit.Events(
		`Selection_5 8000.00 root  or(eq("dbs", DeferredCauset#1), or(eq(DeferredCauset#1, "config"), eq(DeferredCauset#1, "slow_query")))`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["config","dbs","slow_query"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where (rule='config' or rule='slow_query') and (metrics_name='metric_name3' or metrics_name='metric_name1')").Check(testkit.Events(
		`Selection_5 8000.00 root  or(eq(DeferredCauset#1, "config"), eq(DeferredCauset#1, "slow_query")), or(eq(DeferredCauset#3, "metric_name3"), eq(DeferredCauset#3, "metric_name1"))`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["config","slow_query"], metric_names:["metric_name1","metric_name3"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule in ('dbs', 'slow_query')").Check(testkit.Events(
		`Selection_5 8000.00 root  in(DeferredCauset#1, "dbs", "slow_query")`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["dbs","slow_query"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule in ('dbs', 'slow_query') and metrics_name='metric_name1'").Check(testkit.Events(
		`Selection_5 8000.00 root  eq(DeferredCauset#3, "metric_name1"), in(DeferredCauset#1, "dbs", "slow_query")`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["dbs","slow_query"], metric_names:["metric_name1"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule in ('dbs', 'slow_query') and metrics_name in ('metric_name1', 'metric_name2')").Check(testkit.Events(
		`Selection_5 8000.00 root  in(DeferredCauset#1, "dbs", "slow_query"), in(DeferredCauset#3, "metric_name1", "metric_name2")`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["dbs","slow_query"], metric_names:["metric_name1","metric_name2"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule='dbs' and metrics_name in ('metric_name1', 'metric_name2')").Check(testkit.Events(
		`Selection_5 8000.00 root  eq(DeferredCauset#1, "dbs"), in(DeferredCauset#3, "metric_name1", "metric_name2")`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["dbs"], metric_names:["metric_name1","metric_name2"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule='dbs' and metrics_name='metric_NAME3'").Check(testkit.Events(
		`Selection_5 8000.00 root  eq(DeferredCauset#1, "dbs"), eq(DeferredCauset#3, "metric_NAME3")`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["dbs"], metric_names:["metric_name3"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where rule in ('dbs', 'config') and rule in ('slow_query', 'config')").Check(testkit.Events(
		`Selection_5 8000.00 root  in(DeferredCauset#1, "dbs", "config"), in(DeferredCauset#1, "slow_query", "config")`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["config"]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where metrics_name in ('metric_name1', 'metric_name4') and metrics_name in ('metric_name5', 'metric_name4') and rule in ('dbs', 'config') and rule in ('slow_query', 'config') and quantile in (0.80, 0.90)").Check(testkit.Events(
		`Selection_5 8000.00 root  in(DeferredCauset#1, "dbs", "config"), in(DeferredCauset#1, "slow_query", "config"), in(DeferredCauset#3, "metric_name1", "metric_name4"), in(DeferredCauset#3, "metric_name5", "metric_name4")`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY rules:["config"], metric_names:["metric_name4"], quantiles:[0.800000,0.900000]`,
	))
	tk.MustQuery("desc select * from information_schema.inspection_summary where metrics_name in ('metric_name1', 'metric_name4') and metrics_name in ('metric_name5', 'metric_name4') and metrics_name in ('metric_name5', 'metric_name1') and metrics_name in ('metric_name1', 'metric_name3')").Check(testkit.Events(
		`Selection_5 8000.00 root  in(DeferredCauset#3, "metric_name1", "metric_name3"), in(DeferredCauset#3, "metric_name1", "metric_name4"), in(DeferredCauset#3, "metric_name5", "metric_name1"), in(DeferredCauset#3, "metric_name5", "metric_name4")`,
		`└─MemBlockScan_6 10000.00 root block:INSPECTION_SUMMARY skip_inspection: true`,
	))
}

func (s *testSuite) TestExplainTiFlashSystemBlocks(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tiflashInstance := "192.168.1.7:3930"
	database := "test"
	block := "t"
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_TABLES where TIFLASH_INSTANCE = '%s'", tiflashInstance)).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TIFLASH_TABLES tiflash_instances:[\"%s\"]", tiflashInstance)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_SEGMENTS where TIFLASH_INSTANCE = '%s'", tiflashInstance)).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TIFLASH_SEGMENTS tiflash_instances:[\"%s\"]", tiflashInstance)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_TABLES where MilevaDB_DATABASE = '%s'", database)).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TIFLASH_TABLES milevadb_databases:[\"%s\"]", database)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_SEGMENTS where MilevaDB_DATABASE = '%s'", database)).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TIFLASH_SEGMENTS milevadb_databases:[\"%s\"]", database)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_TABLES where MilevaDB_TABLE = '%s'", block)).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TIFLASH_TABLES milevadb_blocks:[\"%s\"]", block)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_SEGMENTS where MilevaDB_TABLE = '%s'", block)).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TIFLASH_SEGMENTS milevadb_blocks:[\"%s\"]", block)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_TABLES where TIFLASH_INSTANCE = '%s' and MilevaDB_DATABASE = '%s' and MilevaDB_TABLE = '%s'", tiflashInstance, database, block)).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TIFLASH_TABLES tiflash_instances:[\"%s\"], milevadb_databases:[\"%s\"], milevadb_blocks:[\"%s\"]", tiflashInstance, database, block)))
	tk.MustQuery(fmt.Sprintf("desc select * from information_schema.TIFLASH_SEGMENTS where TIFLASH_INSTANCE = '%s' and MilevaDB_DATABASE = '%s' and MilevaDB_TABLE = '%s'", tiflashInstance, database, block)).Check(testkit.Events(
		fmt.Sprintf("MemBlockScan_5 10000.00 root block:TIFLASH_SEGMENTS tiflash_instances:[\"%s\"], milevadb_databases:[\"%s\"], milevadb_blocks:[\"%s\"]", tiflashInstance, database, block)))
}
