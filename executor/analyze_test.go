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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testFastAnalyze{})

func (s *testSuite1) TestAnalyzePartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	createBlock := `CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	tk.MustExec(createBlock)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("analyze block t")

	is := schemareplicant.GetSchemaReplicant(tk.Se.(stochastikctx.Context))
	block, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi := block.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	do, err := stochastik.GetPetri(s.causetstore)
	c.Assert(err, IsNil)
	handle := do.StatsHandle()
	for _, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(block.Meta(), def.ID)
		c.Assert(statsTbl.Pseudo, IsFalse)
		c.Assert(len(statsTbl.DeferredCausets), Equals, 3)
		c.Assert(len(statsTbl.Indices), Equals, 1)
		for _, defCaus := range statsTbl.DeferredCausets {
			c.Assert(defCaus.Len(), Greater, 0)
		}
		for _, idx := range statsTbl.Indices {
			c.Assert(idx.Len(), Greater, 0)
		}
	}

	tk.MustExec("drop block t")
	tk.MustExec(createBlock)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
	}
	tk.MustExec("alter block t analyze partition p0")
	is = schemareplicant.GetSchemaReplicant(tk.Se.(stochastikctx.Context))
	block, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	pi = block.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)

	for i, def := range pi.Definitions {
		statsTbl := handle.GetPartitionStats(block.Meta(), def.ID)
		if i == 0 {
			c.Assert(statsTbl.Pseudo, IsFalse)
			c.Assert(len(statsTbl.DeferredCausets), Equals, 3)
			c.Assert(len(statsTbl.Indices), Equals, 1)
		} else {
			c.Assert(statsTbl.Pseudo, IsTrue)
		}
	}
}

func (s *testSuite1) TestAnalyzeReplicaReadFollower(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	ctx := tk.Se.(stochastikctx.Context)
	ctx.GetStochastikVars().SetReplicaRead(ekv.ReplicaReadFollower)
	tk.MustExec("analyze block t")
}

func (s *testSuite1) TestClusterIndexAnalyze(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_cluster_index_analyze;")
	tk.MustExec("create database test_cluster_index_analyze;")
	tk.MustExec("use test_cluster_index_analyze;")
	tk.MustExec("set @@milevadb_enable_clustered_index=1;")

	tk.MustExec("create block t (a int, b int, c int, primary key(a, b));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", i, i, i)
	}
	tk.MustExec("analyze block t;")
	tk.MustExec("drop block t;")

	tk.MustExec("create block t (a varchar(255), b int, c float, primary key(c, a));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", strconv.Itoa(i), i, i)
	}
	tk.MustExec("analyze block t;")
	tk.MustExec("drop block t;")

	tk.MustExec("create block t (a char(10), b decimal(5, 3), c int, primary key(a, c, b));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", strconv.Itoa(i), i, i)
	}
	tk.MustExec("analyze block t;")
	tk.MustExec("drop block t;")
}

func (s *testSuite1) TestAnalyzeRestrict(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	ctx := tk.Se.(stochastikctx.Context)
	ctx.GetStochastikVars().InRestrictedALLEGROSQL = true
	tk.MustExec("analyze block t")
}

func (s *testSuite1) TestAnalyzeParameters(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}
	tk.MustExec("insert into t values (19), (19), (19)")

	tk.MustExec("set @@milevadb_enable_fast_analyze = 1")
	tk.MustExec("analyze block t with 30 samples")
	is := schemareplicant.GetSchemaReplicant(tk.Se.(stochastikctx.Context))
	block, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := block.Meta()
	tbl := s.dom.StatsHandle().GetBlockStats(blockInfo)
	defCaus := tbl.DeferredCausets[1]
	c.Assert(defCaus.Len(), Equals, 20)
	c.Assert(len(defCaus.CMSketch.TopN()), Equals, 1)
	width, depth := defCaus.CMSketch.GetWidthAndDepth()
	c.Assert(depth, Equals, int32(5))
	c.Assert(width, Equals, int32(2048))

	tk.MustExec("analyze block t with 4 buckets, 0 topn, 4 cmsketch width, 4 cmsketch depth")
	tbl = s.dom.StatsHandle().GetBlockStats(blockInfo)
	defCaus = tbl.DeferredCausets[1]
	c.Assert(defCaus.Len(), Equals, 4)
	c.Assert(len(defCaus.CMSketch.TopN()), Equals, 0)
	width, depth = defCaus.CMSketch.GetWidthAndDepth()
	c.Assert(depth, Equals, int32(4))
	c.Assert(width, Equals, int32(4))
}

func (s *testSuite1) TestAnalyzeTooLongDeferredCausets(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a json)")
	value := fmt.Sprintf(`{"x":"%s"}`, strings.Repeat("x", allegrosql.MaxFieldVarCharLength))
	tk.MustExec(fmt.Sprintf("insert into t values ('%s')", value))

	tk.MustExec("analyze block t")
	is := schemareplicant.GetSchemaReplicant(tk.Se.(stochastikctx.Context))
	block, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := block.Meta()
	tbl := s.dom.StatsHandle().GetBlockStats(blockInfo)
	c.Assert(tbl.DeferredCausets[1].Len(), Equals, 0)
	c.Assert(tbl.DeferredCausets[1].TotDefCausSize, Equals, int64(65559))
}

func (s *testFastAnalyze) TestAnalyzeFastSample(c *C) {
	var cls cluster.Cluster
	causetstore, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cls = c
		}),
	)
	c.Assert(err, IsNil)
	defer causetstore.Close()
	var dom *petri.Petri
	stochastik.DisableStats4Test()
	stochastik.SetSchemaLease(0)
	dom, err = stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer dom.Close()
	tk := testkit.NewTestKit(c, causetstore)
	executor.RandSeed = 123

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, index index_b(b))")
	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	tid := tblInfo.ID

	// construct 5 regions split by {12, 24, 36, 48}
	splitKeys := generateBlockSplitKeyForInt(tid, []int{12, 24, 36, 48})
	manipulateCluster(cls, splitKeys)

	for i := 0; i < 60; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	handleDefCauss := core.BuildHandleDefCaussForAnalyze(tk.Se, tblInfo)
	var defcausInfo []*perceptron.DeferredCausetInfo
	var indicesInfo []*perceptron.IndexInfo
	for _, defCaus := range tblInfo.DeferredCausets {
		if allegrosql.HasPriKeyFlag(defCaus.Flag) {
			continue
		}
		defcausInfo = append(defcausInfo, defCaus)
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == perceptron.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	opts := make(map[ast.AnalyzeOptionType]uint64)
	opts[ast.AnalyzeOptNumSamples] = 20
	mockExec := &executor.AnalyzeTestFastExec{
		Ctx:             tk.Se.(stochastikctx.Context),
		HandleDefCauss:  handleDefCauss,
		DefCaussInfo:    defcausInfo,
		IdxsInfo:        indicesInfo,
		Concurrency:     1,
		PhysicalBlockID: tbl.(block.PhysicalBlock).GetPhysicalID(),
		TblInfo:         tblInfo,
		Opts:            opts,
	}
	err = mockExec.TestFastSample()
	c.Assert(err, IsNil)
	c.Assert(len(mockExec.DefCauslectors), Equals, 3)
	for i := 0; i < 2; i++ {
		samples := mockExec.DefCauslectors[i].Samples
		c.Assert(len(samples), Equals, 20)
		for j := 1; j < 20; j++ {
			cmp, err := samples[j].Value.CompareCauset(tk.Se.GetStochastikVars().StmtCtx, &samples[j-1].Value)
			c.Assert(err, IsNil)
			c.Assert(cmp, Greater, 0)
		}
	}
}

func checkHistogram(sc *stmtctx.StatementContext, hg *statistics.Histogram) (bool, error) {
	for i := 0; i < len(hg.Buckets); i++ {
		lower, upper := hg.GetLower(i), hg.GetUpper(i)
		cmp, err := upper.CompareCauset(sc, lower)
		if cmp < 0 || err != nil {
			return false, err
		}
		if i == 0 {
			continue
		}
		previousUpper := hg.GetUpper(i - 1)
		cmp, err = lower.CompareCauset(sc, previousUpper)
		if cmp <= 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

func (s *testFastAnalyze) TestFastAnalyze(c *C) {
	var cls cluster.Cluster
	causetstore, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cls = c
		}),
	)
	c.Assert(err, IsNil)
	defer causetstore.Close()
	var dom *petri.Petri
	stochastik.DisableStats4Test()
	stochastik.SetSchemaLease(0)
	dom, err = stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	dom.SetStatsUFIDelating(true)
	defer dom.Close()
	tk := testkit.NewTestKit(c, causetstore)
	executor.RandSeed = 123

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, c char(10), index index_b(b))")
	tk.MustExec("set @@stochastik.milevadb_enable_fast_analyze=1")
	tk.MustExec("set @@stochastik.milevadb_build_stats_concurrency=1")
	// Should not panic.
	tk.MustExec("analyze block t")
	tblInfo, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tblInfo.Meta().ID

	// construct 6 regions split by {10, 20, 30, 40, 50}
	splitKeys := generateBlockSplitKeyForInt(tid, []int{10, 20, 30, 40, 50})
	manipulateCluster(cls, splitKeys)

	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "char")`, i*3, i*3))
	}
	tk.MustExec("analyze block t with 5 buckets, 6 samples")

	is := schemareplicant.GetSchemaReplicant(tk.Se.(stochastikctx.Context))
	block, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := block.Meta()
	tbl := dom.StatsHandle().GetBlockStats(blockInfo)
	// TODO(tangenta): add stats_meta.row_count assertion.
	for _, defCaus := range tbl.DeferredCausets {
		ok, err := checkHistogram(tk.Se.GetStochastikVars().StmtCtx, &defCaus.Histogram)
		c.Assert(err, IsNil)
		c.Assert(ok, IsTrue)
	}
	for _, idx := range tbl.Indices {
		ok, err := checkHistogram(tk.Se.GetStochastikVars().StmtCtx, &idx.Histogram)
		c.Assert(err, IsNil)
		c.Assert(ok, IsTrue)
	}

	// Test CM Sketch built from fast analyze.
	tk.MustExec("create block t1(a int, b int, index idx(a, b))")
	// Should not panic.
	tk.MustExec("analyze block t1")
	tk.MustExec("insert into t1 values (1,1),(1,1),(1,2),(1,2)")
	tk.MustExec("analyze block t1")
	tk.MustQuery("explain select a from t1 where a = 1").Check(testkit.Events(
		"IndexReader_6 4.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 4.00 INTERLOCK[einsteindb] block:t1, index:idx(a, b) range:[1,1], keep order:false"))
	tk.MustQuery("explain select a, b from t1 where a = 1 and b = 1").Check(testkit.Events(
		"IndexReader_6 2.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 2.00 INTERLOCK[einsteindb] block:t1, index:idx(a, b) range:[1 1,1 1], keep order:false"))
	tk.MustQuery("explain select a, b from t1 where a = 1 and b = 2").Check(testkit.Events(
		"IndexReader_6 2.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 2.00 INTERLOCK[einsteindb] block:t1, index:idx(a, b) range:[1 2,1 2], keep order:false"))

	tk.MustExec("create block t2 (a bigint unsigned, primary key(a))")
	tk.MustExec("insert into t2 values (0), (18446744073709551615)")
	tk.MustExec("analyze block t2")
	tk.MustQuery("show stats_buckets where block_name = 't2'").Check(testkit.Events(
		"test t2  a 0 0 1 1 0 0",
		"test t2  a 0 1 2 1 18446744073709551615 18446744073709551615"))

	tk.MustExec(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)
	tk.MustExec(`create block t3 (id int, v int, primary key(id), index k(v)) partition by hash (id) partitions 4`)
	tk.MustExec(`insert into t3 values(1, 1), (2, 2), (5, 1), (9, 3), (13, 3), (17, 5), (3, 0)`)
	tk.MustExec(`analyze block t3`)
	tk.MustQuery(`explain select v from t3 partition(p1) where v = 3`).Check(testkit.Events(
		"IndexReader_7 2.00 root  index:IndexRangeScan_6",
		"└─IndexRangeScan_6 2.00 INTERLOCK[einsteindb] block:t3, partition:p1, index:k(v) range:[3,3], keep order:false",
	))
	tk.MustExec(`set @@milevadb_partition_prune_mode='` + string(variable.DynamicOnly) + `'`)
}

func (s *testSuite1) TestIssue15993(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT PRIMARY KEY);")
	tk.MustExec("set @@milevadb_enable_fast_analyze=1;")
	tk.MustExec("ANALYZE TABLE t0 INDEX PRIMARY;")
}

func (s *testSuite1) TestIssue15751(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT, c1 INT, PRIMARY KEY(c0, c1))")
	tk.MustExec("INSERT INTO t0 VALUES (0, 0)")
	tk.MustExec("set @@milevadb_enable_fast_analyze=1")
	tk.MustExec("ANALYZE TABLE t0")
}

func (s *testSuite1) TestIssue15752(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT)")
	tk.MustExec("INSERT INTO t0 VALUES (0)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustExec("set @@milevadb_enable_fast_analyze=1")
	tk.MustExec("ANALYZE TABLE t0 INDEX i0")
}

func (s *testSuite1) TestAnalyzeIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (id int, v int, primary key(id), index k(v))")
	tk.MustExec("insert into t1(id, v) values(1, 2), (2, 2), (3, 2), (4, 2), (5, 1), (6, 3), (7, 4)")
	tk.MustExec("analyze block t1 index k")
	c.Assert(len(tk.MustQuery("show stats_buckets where block_name = 't1' and defCausumn_name = 'k' and is_index = 1").Events()), Greater, 0)

	func() {
		defer tk.MustExec("set @@stochastik.milevadb_enable_fast_analyze=0")
		tk.MustExec("drop stats t1")
		tk.MustExec("set @@stochastik.milevadb_enable_fast_analyze=1")
		tk.MustExec("analyze block t1 index k")
		c.Assert(len(tk.MustQuery("show stats_buckets where block_name = 't1' and defCausumn_name = 'k' and is_index = 1").Events()), Greater, 1)
	}()
}

func (s *testSuite1) TestAnalyzeIncremental(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.Se.GetStochastikVars().EnableStreaming = false
	s.testAnalyzeIncremental(tk, c)
}

func (s *testSuite1) TestAnalyzeIncrementalStreaming(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.Se.GetStochastikVars().EnableStreaming = true
	s.testAnalyzeIncremental(tk, c)
}

func (s *testSuite1) testAnalyzeIncremental(tk *testkit.TestKit, c *C) {
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, primary key(a), index idx(b))")
	tk.MustExec("analyze incremental block t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Events())
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("analyze incremental block t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Events("test t  a 0 0 1 1 1 1", "test t  idx 1 0 1 1 1 1"))
	tk.MustExec("insert into t values (2,2)")
	tk.MustExec("analyze incremental block t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Events("test t  a 0 0 1 1 1 1", "test t  a 0 1 2 1 2 2", "test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2"))
	tk.MustExec("analyze incremental block t index")
	// Result should not change.
	tk.MustQuery("show stats_buckets").Check(testkit.Events("test t  a 0 0 1 1 1 1", "test t  a 0 1 2 1 2 2", "test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2"))

	// Test analyze incremental with feedback.
	tk.MustExec("insert into t values (3,3)")
	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount
	defer func() {
		statistics.FeedbackProbability.CausetStore(oriProbability)
		handle.MinLogScanCount = oriMinLogCount
	}()
	statistics.FeedbackProbability.CausetStore(1)
	handle.MinLogScanCount = 0
	is := s.dom.SchemaReplicant()
	block, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := block.Meta()
	tk.MustQuery("select * from t use index(idx) where b = 3")
	tk.MustQuery("select * from t where a > 1")
	h := s.dom.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.DumpStatsFeedbackToKV(), IsNil)
	c.Assert(h.HandleUFIDelateStats(is), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	tk.MustQuery("show stats_buckets").Check(testkit.Events("test t  a 0 0 1 1 1 1", "test t  a 0 1 3 0 2 2147483647", "test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2"))
	tblStats := h.GetBlockStats(tblInfo)
	val, err := codec.EncodeKey(tk.Se.GetStochastikVars().StmtCtx, nil, types.NewIntCauset(3))
	c.Assert(err, IsNil)
	c.Assert(tblStats.Indices[tblInfo.Indices[0].ID].CMSketch.QueryBytes(val), Equals, uint64(1))
	c.Assert(statistics.IsAnalyzed(tblStats.Indices[tblInfo.Indices[0].ID].Flag), IsFalse)
	c.Assert(statistics.IsAnalyzed(tblStats.DeferredCausets[tblInfo.DeferredCausets[0].ID].Flag), IsFalse)

	tk.MustExec("analyze incremental block t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Events("test t  a 0 0 1 1 1 1", "test t  a 0 1 2 1 2 2", "test t  a 0 2 3 1 3 3",
		"test t  idx 1 0 1 1 1 1", "test t  idx 1 1 2 1 2 2", "test t  idx 1 2 3 1 3 3"))
	tblStats = h.GetBlockStats(tblInfo)
	c.Assert(tblStats.Indices[tblInfo.Indices[0].ID].CMSketch.QueryBytes(val), Equals, uint64(1))
}

type testFastAnalyze struct {
}

type regionProperityClient struct {
	einsteindb.Client
	mu struct {
		sync.Mutex
		failedOnce bool
		count      int64
	}
}

func (c *regionProperityClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	if req.Type == einsteindbrpc.CmdDebugGetRegionProperties {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.count++
		// Mock failure once.
		if !c.mu.failedOnce {
			c.mu.failedOnce = true
			return &einsteindbrpc.Response{}, nil
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (s *testFastAnalyze) TestFastAnalyzeRetryEventCount(c *C) {
	cli := &regionProperityClient{}
	hijackClient := func(c einsteindb.Client) einsteindb.Client {
		cli.Client = c
		return cli
	}

	var cls cluster.Cluster
	causetstore, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cls = c
		}),
		mockstore.WithClientHijacker(hijackClient),
	)
	c.Assert(err, IsNil)
	defer causetstore.Close()
	dom, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer dom.Close()

	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists retry_row_count")
	tk.MustExec("create block retry_row_count(a int primary key)")
	tblInfo, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("retry_row_count"))
	c.Assert(err, IsNil)
	tid := tblInfo.Meta().ID
	c.Assert(dom.StatsHandle().UFIDelate(dom.SchemaReplicant()), IsNil)
	tk.MustExec("set @@stochastik.milevadb_enable_fast_analyze=1")
	tk.MustExec("set @@stochastik.milevadb_build_stats_concurrency=1")
	for i := 0; i < 30; i++ {
		tk.MustExec(fmt.Sprintf("insert into retry_row_count values (%d)", i))
	}
	cls.SplitBlock(tid, 6)
	// Flush the region cache first.
	tk.MustQuery("select * from retry_row_count")
	tk.MustExec("analyze block retry_row_count")
	event := tk.MustQuery(`show stats_meta where db_name = "test" and block_name = "retry_row_count"`).Events()[0]
	c.Assert(event[5], Equals, "30")
}

func (s *testSuite9) TestFailedAnalyzeRequest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, index index_b(b))")
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/executor/buildStatsFromResult", `return(true)`), IsNil)
	_, err := tk.Exec("analyze block t")
	c.Assert(err.Error(), Equals, "mock buildStatsFromResult error")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/executor/buildStatsFromResult"), IsNil)
}

func (s *testSuite1) TestExtractTopN(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, index index_b(b))")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, 0)", i+10))
	}
	tk.MustExec("analyze block t")
	is := s.dom.SchemaReplicant()
	block, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := block.Meta()
	tblStats := s.dom.StatsHandle().GetBlockStats(tblInfo)
	defCausStats := tblStats.DeferredCausets[tblInfo.DeferredCausets[1].ID]
	c.Assert(len(defCausStats.CMSketch.TopN()), Equals, 1)
	item := defCausStats.CMSketch.TopN()[0]
	c.Assert(item.Count, Equals, uint64(11))
	idxStats := tblStats.Indices[tblInfo.Indices[0].ID]
	c.Assert(len(idxStats.CMSketch.TopN()), Equals, 1)
	item = idxStats.CMSketch.TopN()[0]
	c.Assert(item.Count, Equals, uint64(11))
}

func (s *testSuite1) TestHashInTopN(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b float, c decimal(30, 10), d varchar(20))")
	tk.MustExec(`insert into t values
				(1, 1.1, 11.1, "0110"),
				(2, 2.2, 22.2, "0110"),
				(3, 3.3, 33.3, "0110"),
				(4, 4.4, 44.4, "0440")`)
	for i := 0; i < 3; i++ {
		tk.MustExec("insert into t select * from t")
	}
	// get stats of normal analyze
	tk.MustExec("analyze block t")
	is := s.dom.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	tblStats1 := s.dom.StatsHandle().GetBlockStats(tblInfo).INTERLOCKy()
	// get stats of fast analyze
	tk.MustExec("set @@milevadb_enable_fast_analyze = 1")
	tk.MustExec("analyze block t")
	tblStats2 := s.dom.StatsHandle().GetBlockStats(tblInfo).INTERLOCKy()
	// check the hash for topn
	for _, defCaus := range tblInfo.DeferredCausets {
		topn1 := tblStats1.DeferredCausets[defCaus.ID].CMSketch.TopNMap()
		cm2 := tblStats2.DeferredCausets[defCaus.ID].CMSketch
		for h1, topnMetas := range topn1 {
			for _, topnMeta1 := range topnMetas {
				count2, exists := cm2.QueryTopN(h1, topnMeta1.GetH2(), topnMeta1.Data)
				c.Assert(exists, Equals, true)
				c.Assert(count2, Equals, topnMeta1.Count)
			}
		}
	}
}

func (s *testSuite1) TestNormalAnalyzeOnCommonHandle(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2, t3, t4")
	tk.Se.GetStochastikVars().EnableClusteredIndex = true
	tk.MustExec("CREATE TABLE t1 (a int primary key, b int)")
	tk.MustExec("insert into t1 values(1,1), (2,2), (3,3)")
	tk.MustExec("CREATE TABLE t2 (a varchar(255) primary key, b int)")
	tk.MustExec("insert into t2 values(\"111\",1), (\"222\",2), (\"333\",3)")
	tk.MustExec("CREATE TABLE t3 (a int, b int, c int, primary key (a, b), key(c))")
	tk.MustExec("insert into t3 values(1,1,1), (2,2,2), (3,3,3)")

	tk.MustExec("analyze block t1, t2, t3")

	tk.MustQuery(`show stats_buckets where block_name in ("t1", "t2", "t3")`).Sort().Check(testkit.Events(
		"test t1  a 0 0 1 1 1 1",
		"test t1  a 0 1 2 1 2 2",
		"test t1  a 0 2 3 1 3 3",
		"test t1  b 0 0 1 1 1 1",
		"test t1  b 0 1 2 1 2 2",
		"test t1  b 0 2 3 1 3 3",
		"test t2  PRIMARY 1 0 1 1 111 111",
		"test t2  PRIMARY 1 1 2 1 222 222",
		"test t2  PRIMARY 1 2 3 1 333 333",
		"test t2  a 0 0 1 1 111 111",
		"test t2  a 0 1 2 1 222 222",
		"test t2  a 0 2 3 1 333 333",
		"test t2  b 0 0 1 1 1 1",
		"test t2  b 0 1 2 1 2 2",
		"test t2  b 0 2 3 1 3 3",
		"test t3  PRIMARY 1 0 1 1 (1, 1) (1, 1)",
		"test t3  PRIMARY 1 1 2 1 (2, 2) (2, 2)",
		"test t3  PRIMARY 1 2 3 1 (3, 3) (3, 3)",
		"test t3  a 0 0 1 1 1 1",
		"test t3  a 0 1 2 1 2 2",
		"test t3  a 0 2 3 1 3 3",
		"test t3  b 0 0 1 1 1 1",
		"test t3  b 0 1 2 1 2 2",
		"test t3  b 0 2 3 1 3 3",
		"test t3  c 0 0 1 1 1 1",
		"test t3  c 0 1 2 1 2 2",
		"test t3  c 0 2 3 1 3 3",
		"test t3  c 1 0 1 1 1 1",
		"test t3  c 1 1 2 1 2 2",
		"test t3  c 1 2 3 1 3 3"))
}

func (s *testSuite1) TestDefaultValForAnalyze(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_default_val_for_analyze;")
	tk.MustExec("create database test_default_val_for_analyze;")
	tk.MustExec("use test_default_val_for_analyze")

	tk.MustExec("create block t (a int, key(a));")
	for i := 0; i < 2048; i++ {
		tk.MustExec("insert into t values (0)")
	}
	for i := 1; i < 4; i++ {
		tk.MustExec("insert into t values (?)", i)
	}
	tk.MustExec("analyze block t with 0 topn;")
	tk.MustQuery("explain select * from t where a = 1").Check(testkit.Events("IndexReader_6 512.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 512.00 INTERLOCK[einsteindb] block:t, index:a(a) range:[1,1], keep order:false"))
	tk.MustQuery("explain select * from t where a = 999").Check(testkit.Events("IndexReader_6 0.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 0.00 INTERLOCK[einsteindb] block:t, index:a(a) range:[999,999], keep order:false"))

	tk.MustExec("drop block t;")
	tk.MustExec("create block t (a int, key(a));")
	for i := 0; i < 2048; i++ {
		tk.MustExec("insert into t values (0)")
	}
	for i := 1; i < 2049; i++ {
		tk.MustExec("insert into t values (?)", i)
	}
	tk.MustExec("analyze block t with 0 topn;")
	tk.MustQuery("explain select * from t where a = 1").Check(testkit.Events("IndexReader_6 1.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 1.00 INTERLOCK[einsteindb] block:t, index:a(a) range:[1,1], keep order:false"))
}
