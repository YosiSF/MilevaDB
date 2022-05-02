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

package cascades_test

import (
	"fmt"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	. "github.com/whtcorpsinc/check"
)

var _ = Suite(&testIntegrationSuite{})

type testIntegrationSuite struct {
	causetstore ekv.CausetStorage
	testData    solitonutil.TestData
}

func newStoreWithBootstrap() (ekv.CausetStorage, error) {
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		return nil, err
	}
	_, err = stochastik.BootstrapStochastik(causetstore)
	return causetstore, err
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	var err error
	s.causetstore, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "integration_suite")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
	s.causetstore.Close()
}

func (s *testIntegrationSuite) TestSimpleProjDual(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")
	tk.MustQuery("explain select 1").Check(testkit.Rows(
		"Projection_3 1.00 root  1->DeferredCauset#1",
		"└─BlockDual_4 1.00 root  rows:1",
	))
	tk.MustQuery("select 1").Check(testkit.Rows(
		"1",
	))
}

func (s *testIntegrationSuite) TestPKIsHandleRangeScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int)")
	tk.MustExec("insert into t values(1,2),(3,4),(5,6)")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestIndexScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, c int, d int, index idx_b(b), index idx_c_b(c, b))")
	tk.MustExec("insert into t values(1,2,3,100),(4,5,6,200),(7,8,9,300)")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestBasicShow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int)")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")
	tk.MustQuery("desc t").Check(testkit.Rows(
		"a int(11) NO PRI <nil> ",
		"b int(11) YES  <nil> ",
	))
}

func (s *testIntegrationSuite) TestSort(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestAggregation(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")
	tk.MustExec("set stochastik milevadb_executor_concurrency = 4")
	tk.MustExec("set @@stochastik.milevadb_hash_join_concurrency = 5")
	tk.MustExec("set @@stochastik.milevadb_distsql_scan_concurrency = 15;")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestPushdownDistinctEnable(c *C) {
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Plan              []string
			Result            []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@stochastik.%s = 1", variable.MilevaDBOptDistinctAggPushDown),
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testIntegrationSuite) TestPushdownDistinctDisable(c *C) {
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Plan              []string
			Result            []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@stochastik.%s = 0", variable.MilevaDBOptDistinctAggPushDown),
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testIntegrationSuite) doTestPushdownDistinct(c *C, vars, input []string, output []struct {
	ALLEGROALLEGROSQL string
	Plan              []string
	Result            []string
}) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, c int, index(c))")
	tk.MustExec("insert into t values (1, 1, 1), (1, 1, 3), (1, 2, 3), (2, 1, 3), (1, 2, NULL);")
	tk.MustExec("set stochastik sql_mode=''")
	tk.MustExec(fmt.Sprintf("set stochastik %s=1", variable.MilevaDBHashAggPartialConcurrency))
	tk.MustExec(fmt.Sprintf("set stochastik %s=1", variable.MilevaDBHashAggFinalConcurrency))
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")

	for _, v := range vars {
		tk.MustExec(v)
	}

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestSimplePlans(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("set @@stochastik.milevadb_executor_concurrency = 4;")
	tk.MustExec("set @@stochastik.milevadb_hash_join_concurrency = 5;")
	tk.MustExec("set @@stochastik.milevadb_distsql_scan_concurrency = 15;")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("drop block if exists t2")
	tk.MustExec("create block t1(a int primary key, b int)")
	tk.MustExec("create block t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("insert into t2 values (1, 111), (2, 222), (3, 333), (5, 555)")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestApply(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int primary key, b int)")
	tk.MustExec("create block t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values (1, 11), (4, 44), (2, 22), (3, 33)")
	tk.MustExec("insert into t2 values (1, 11), (2, 22), (3, 33)")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestMemBlockScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestTopN(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a int primary key, b int);")
	tk.MustExec("insert into t values (1, 11), (4, 44), (2, 22), (3, 33);")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1;")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestCascadePlannerHashedPartBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists pt1")
	tk.MustExec("create block pt1(a bigint, b bigint) partition by hash(a) partitions 4")
	tk.MustExec(`insert into pt1 values(1,10)`)
	tk.MustExec(`insert into pt1 values(2,20)`)
	tk.MustExec(`insert into pt1 values(3,30)`)
	tk.MustExec(`insert into pt1 values(4,40)`)
	tk.MustExec(`insert into pt1 values(5,50)`)

	tk.MustExec("set @@milevadb_enable_cascades_planner = 1")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testIntegrationSuite) TestInlineProjection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1(a bigint, b bigint, index idx_a(a), index idx_b(b));")
	tk.MustExec("create block t2(a bigint, b bigint, index idx_a(a), index idx_b(b));")
	tk.MustExec("insert into t1 values (1, 1), (2, 2);")
	tk.MustExec("insert into t2 values (1, 1), (3, 3);")
	tk.MustExec("set stochastik milevadb_enable_cascades_planner = 1;")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + allegrosql).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(allegrosql).Rows())
		})
		tk.MustQuery("explain " + allegrosql).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(allegrosql).Check(testkit.Rows(output[i].Result...))
	}
}
