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
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	pb "github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/planner"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/server"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/soliton/gcutil"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	autoid.SetStep(5000)

	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.Log.SlowThreshold = 30000 // 30s
	})
	tmFIDelir := config.GetGlobalConfig().TempStoragePath
	_ = os.RemoveAll(tmFIDelir) // clean the uncleared temp file during the last run.
	_ = os.MkdirAll(tmFIDelir, 0755)
	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

var _ = Suite(&testSuite{&baseTestSuite{}})
var _ = Suite(&testSuiteP1{&baseTestSuite{}})
var _ = Suite(&testSuiteP2{&baseTestSuite{}})
var _ = Suite(&testSuite1{})
var _ = Suite(&testSuite2{&baseTestSuite{}})
var _ = Suite(&testSuite3{&baseTestSuite{}})
var _ = Suite(&testSuite4{&baseTestSuite{}})
var _ = Suite(&testSuite5{&baseTestSuite{}})
var _ = Suite(&testSuiteJoin1{&baseTestSuite{}})
var _ = Suite(&testSuiteJoin2{&baseTestSuite{}})
var _ = Suite(&testSuiteJoin3{&baseTestSuite{}})
var _ = SerialSuites(&testSuiteJoinSerial{&baseTestSuite{}})
var _ = Suite(&testSuiteAgg{baseTestSuite: &baseTestSuite{}})
var _ = Suite(&testSuite6{&baseTestSuite{}})
var _ = Suite(&testSuite7{&baseTestSuite{}})
var _ = Suite(&testSuite8{&baseTestSuite{}})
var _ = Suite(&testClusteredSuite{&baseTestSuite{}})
var _ = SerialSuites(&testShowStatsSuite{&baseTestSuite{}})
var _ = Suite(&testBypassSuite{})
var _ = Suite(&testUFIDelateSuite{})
var _ = Suite(&testPointGetSuite{})
var _ = Suite(&testBatchPointGetSuite{})
var _ = SerialSuites(&testRecoverBlock{})
var _ = SerialSuites(&testMemBlockReaderSuite{&testClusterBlockBase{}})
var _ = SerialSuites(&testFlushSuite{})
var _ = SerialSuites(&testAutoRandomSuite{&baseTestSuite{}})
var _ = SerialSuites(&testClusterBlockSuite{})
var _ = SerialSuites(&testPrepareSerialSuite{&baseTestSuite{}})
var _ = SerialSuites(&testSplitBlock{&baseTestSuite{}})
var _ = Suite(&testSuiteWithData{baseTestSuite: &baseTestSuite{}})
var _ = SerialSuites(&testSerialSuite1{&baseTestSuite{}})
var _ = SerialSuites(&testSlowQuery{&baseTestSuite{}})
var _ = Suite(&partitionBlockSuite{&baseTestSuite{}})
var _ = SerialSuites(&tiflashTestSuite{})
var _ = SerialSuites(&testSerialSuite{&baseTestSuite{}})

type testSuite struct{ *baseTestSuite }
type testSuiteP1 struct{ *baseTestSuite }
type testSuiteP2 struct{ *baseTestSuite }
type testSplitBlock struct{ *baseTestSuite }
type testSuiteWithData struct {
	*baseTestSuite
	testData solitonutil.TestData
}
type testSlowQuery struct{ *baseTestSuite }
type partitionBlockSuite struct{ *baseTestSuite }
type testSerialSuite struct{ *baseTestSuite }

type baseTestSuite struct {
	cluster     cluster.Cluster
	causetstore ekv.CausetStorage
	petri       *petri.Petri
	*berolinaAllegroSQL.berolinaAllegroSQL
	ctx *mock.Context
}

var mockEinsteinDB = flag.Bool("mockEinsteinDB", true, "use mock einsteindb causetstore in executor test")

func (s *baseTestSuite) SetUpSuite(c *C) {
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	flag.Lookup("mockEinsteinDB")
	useMockEinsteinDB := *mockEinsteinDB
	if useMockEinsteinDB {
		causetstore, err := mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c cluster.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				s.cluster = c
			}),
		)
		c.Assert(err, IsNil)
		s.causetstore = causetstore
		stochastik.SetSchemaLease(0)
		stochastik.DisableStats4Test()
	}
	d, err := stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	d.SetStatsUFIDelating(true)
	s.petri = d
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.OOMCausetAction = config.OOMCausetActionLog
	})
}

func (s *testSuiteWithData) SetUpSuite(c *C) {
	s.baseTestSuite.SetUpSuite(c)
	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "executor_suite")
	c.Assert(err, IsNil)
}

func (s *testSuiteWithData) TearDownSuite(c *C) {
	s.baseTestSuite.TearDownSuite(c)
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *baseTestSuite) TearDownSuite(c *C) {
	s.petri.Close()
	s.causetstore.Close()
}

func (s *testSuiteP1) TestPessimisticSelectForUFIDelate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(id int primary key, a int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("begin PESSIMISTIC")
	tk.MustQuery("select a from t where id=1 for uFIDelate").Check(testkit.Events("1"))
	tk.MustExec("uFIDelate t set a=a+1 where id=1")
	tk.MustExec("commit")
	tk.MustQuery("select a from t where id=1").Check(testkit.Events("2"))
}

func (s *testSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Events() {
		blockName := tb[0]
		tk.MustExec(fmt.Sprintf("drop block %v", blockName))
	}
}

func (s *testSuiteP1) TestBind(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists testbind")

	tk.MustExec("create block testbind(i int, s varchar(20))")
	tk.MustExec("create index index_t on testbind(i,s)")
	tk.MustExec("create global binding for select * from testbind using select * from testbind use index for join(index_t)")
	c.Assert(len(tk.MustQuery("show global bindings").Events()), Equals, 1)

	tk.MustExec("create stochastik binding for select * from testbind using select * from testbind use index for join(index_t)")
	c.Assert(len(tk.MustQuery("show stochastik bindings").Events()), Equals, 1)
	tk.MustExec("drop stochastik binding for select * from testbind")
}

func (s *testSuiteP1) TestChange(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("alter block t change a b int")
	tk.MustExec("alter block t change b c bigint")
	c.Assert(tk.ExecToErr("alter block t change c d varchar(100)"), NotNil)
}

func (s *testSuiteP1) TestChangePumpAndDrainer(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// change pump or drainer's state need connect to etcd
	// so will meet error "URL scheme must be http, https, unix, or unixs: /tmp/milevadb"
	err := tk.ExecToErr("change pump to node_state ='paused' for node_id 'pump1'")
	c.Assert(err, ErrorMatches, "URL scheme must be http, https, unix, or unixs.*")
	err = tk.ExecToErr("change drainer to node_state ='paused' for node_id 'drainer1'")
	c.Assert(err, ErrorMatches, "URL scheme must be http, https, unix, or unixs.*")
}

func (s *testSuiteP1) TestLoadStats(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	c.Assert(tk.ExecToErr("load stats"), NotNil)
	c.Assert(tk.ExecToErr("load stats ./xxx.json"), NotNil)
}

func (s *testSuiteP1) TestShow(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database test_show;")
	tk.MustExec("use test_show")

	tk.MustQuery("show engines")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key)")
	c.Assert(len(tk.MustQuery("show index in t").Events()), Equals, 1)
	c.Assert(len(tk.MustQuery("show index from t").Events()), Equals, 1)

	tk.MustQuery("show charset").Check(testkit.Events(
		"utf8 UTF-8 Unicode utf8_bin 3",
		"utf8mb4 UTF-8 Unicode utf8mb4_bin 4",
		"ascii US ASCII ascii_bin 1",
		"latin1 Latin1 latin1_bin 1",
		"binary binary binary 1"))
	c.Assert(len(tk.MustQuery("show master status").Events()), Equals, 1)
	tk.MustQuery("show create database test_show").Check(testkit.Events("test_show CREATE DATABASE `test_show` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	tk.MustQuery("show privileges").Check(testkit.Events("Alter Blocks To alter the block",
		"Alter Blocks To alter the block",
		"Alter routine Functions,Procedures To alter or drop stored functions/procedures",
		"Create Databases,Blocks,Indexes To create new databases and blocks",
		"Create routine Databases To use CREATE FUNCTION/PROCEDURE",
		"Create temporary blocks Databases To use CREATE TEMPORARY TABLE",
		"Create view Blocks To create new views",
		"Create user Server Admin To create new users",
		"Delete Blocks To delete existing rows",
		"Drop Databases,Blocks To drop databases, blocks, and views",
		"Event Server Admin To create, alter, drop and execute events",
		"Execute Functions,Procedures To execute stored routines",
		"File File access on server To read and write files on the server",
		"Grant option Databases,Blocks,Functions,Procedures To give to other users those privileges you possess",
		"Index Blocks To create or drop indexes",
		"Insert Blocks To insert data into blocks",
		"Lock blocks Databases To use LOCK TABLES (together with SELECT privilege)",
		"Process Server Admin To view the plain text of currently executing queries",
		"Proxy Server Admin To make proxy user possible",
		"References Databases,Blocks To have references on blocks",
		"Reload Server Admin To reload or refresh blocks, logs and privileges",
		"Replication client Server Admin To ask where the slave or master servers are",
		"Replication slave Server Admin To read binary log events from the master",
		"Select Blocks To retrieve rows from block",
		"Show databases Server Admin To see all databases with SHOW DATABASES",
		"Show view Blocks To see views with SHOW CREATE VIEW",
		"Shutdown Server Admin To shut down the server",
		"Super Server Admin To use KILL thread, SET GLOBAL, CHANGE MASTER, etc.",
		"Trigger Blocks To use triggers",
		"Create blockspace Server Admin To create/alter/drop blockspaces",
		"UFIDelate Blocks To uFIDelate existing rows",
		"Usage Server Admin No privileges - allow connect only"))
	c.Assert(len(tk.MustQuery("show block status").Events()), Equals, 1)
}

func (s *testSuite3) TestAdmin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("insert admin_test (c1) values (1),(2),(NULL)")

	ctx := context.Background()
	// cancel DBS jobs test
	r, err := tk.Exec("admin cancel dbs jobs 1")
	c.Assert(err, IsNil, Commentf("err %v", err))
	req := r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	event := req.GetEvent(0)
	c.Assert(event.Len(), Equals, 2)
	c.Assert(event.GetString(0), Equals, "1")
	c.Assert(event.GetString(1), Matches, "*DBS Job:1 not found")

	// show dbs test;
	r, err = tk.Exec("admin show dbs")
	c.Assert(err, IsNil)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	event = req.GetEvent(0)
	c.Assert(event.Len(), Equals, 6)
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	dbsInfo, err := admin.GetDBSInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(event.GetInt64(0), Equals, dbsInfo.SchemaVer)
	// TODO: Pass this test.
	// rowOwnerInfos := strings.Split(event.Data[1].GetString(), ",")
	// ownerInfos := strings.Split(dbsInfo.Owner.String(), ",")
	// c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	serverInfo, err := infosync.GetServerInfoByID(ctx, event.GetString(1))
	c.Assert(err, IsNil)
	c.Assert(event.GetString(2), Equals, serverInfo.IP+":"+
		strconv.FormatUint(uint64(serverInfo.Port), 10))
	c.Assert(event.GetString(3), Equals, "")
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumEvents() == 0, IsTrue)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	// show DBS jobs test
	r, err = tk.Exec("admin show dbs jobs")
	c.Assert(err, IsNil)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	event = req.GetEvent(0)
	c.Assert(event.Len(), Equals, 11)
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	historyJobs, err := admin.GetHistoryDBSJobs(txn, admin.DefNumHistoryJobs)
	c.Assert(len(historyJobs), Greater, 1)
	c.Assert(len(event.GetString(1)), Greater, 0)
	c.Assert(err, IsNil)
	c.Assert(event.GetInt64(0), Equals, historyJobs[0].ID)
	c.Assert(err, IsNil)

	r, err = tk.Exec("admin show dbs jobs 20")
	c.Assert(err, IsNil)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	event = req.GetEvent(0)
	c.Assert(event.Len(), Equals, 11)
	c.Assert(event.GetInt64(0), Equals, historyJobs[0].ID)
	c.Assert(err, IsNil)

	// show DBS job queries test
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test2")
	tk.MustExec("create block admin_test2 (c1 int, c2 int, c3 int default 1, index (c1))")
	result := tk.MustQuery(`admin show dbs job queries 1, 1, 1`)
	result.Check(testkit.Events())
	result = tk.MustQuery(`admin show dbs job queries 1, 2, 3, 4`)
	result.Check(testkit.Events())
	historyJobs, err = admin.GetHistoryDBSJobs(txn, admin.DefNumHistoryJobs)
	result = tk.MustQuery(fmt.Sprintf("admin show dbs job queries %d", historyJobs[0].ID))
	result.Check(testkit.Events(historyJobs[0].Query))
	c.Assert(err, IsNil)

	// check block test
	tk.MustExec("create block admin_test1 (c1 int, c2 int default 1, index (c1))")
	tk.MustExec("insert admin_test1 (c1) values (21),(22)")
	r, err = tk.Exec("admin check block admin_test, admin_test1")
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
	// error block name
	err = tk.ExecToErr("admin check block admin_test_error")
	c.Assert(err, NotNil)
	// different index values
	sctx := tk.Se.(stochastikctx.Context)
	dom := petri.GetPetri(sctx)
	is := dom.SchemaReplicant()
	c.Assert(is, NotNil)
	tb, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("admin_test"))
	c.Assert(err, IsNil)
	c.Assert(tb.Indices(), HasLen, 1)
	_, err = tb.Indices()[0].Create(mock.NewContext(), txn.GetUnionStore(), types.MakeCausets(int64(10)), ekv.IntHandle(1))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	errAdmin := tk.ExecToErr("admin check block admin_test")
	c.Assert(errAdmin, NotNil)

	if config.CheckBlockBeforeDrop {
		err = tk.ExecToErr("drop block admin_test")
		c.Assert(err.Error(), Equals, errAdmin.Error())

		// Drop inconsistency index.
		tk.MustExec("alter block admin_test drop index c1")
		tk.MustExec("admin check block admin_test")
	}
	// checksum block test
	tk.MustExec("create block checksum_with_index (id int, count int, PRIMARY KEY(id), KEY(count))")
	tk.MustExec("create block checksum_without_index (id int, count int, PRIMARY KEY(id))")
	r, err = tk.Exec("admin checksum block checksum_with_index, checksum_without_index")
	c.Assert(err, IsNil)
	res := tk.ResultSetToResult(r, Commentf("admin checksum block"))
	// Mockeinsteindb returns 1 for every block/index scan, then we will xor the checksums of a block.
	// For "checksum_with_index", we have two checksums, so the result will be 1^1 = 0.
	// For "checksum_without_index", we only have one checksum, so the result will be 1.
	res.Sort().Check(testkit.Events("test checksum_with_index 0 2 2", "test checksum_without_index 1 1 1"))

	tk.MustExec("drop block if exists t1;")
	tk.MustExec("CREATE TABLE t1 (c2 BOOL, PRIMARY KEY (c2));")
	tk.MustExec("INSERT INTO t1 SET c2 = '0';")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c3 DATETIME NULL DEFAULT '2668-02-03 17:19:31';")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx2 (c3);")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c4 bit(10) default 127;")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx3 (c4);")
	tk.MustExec("admin check block t1;")

	// Test admin show dbs jobs block name after block has been droped.
	tk.MustExec("drop block if exists t1;")
	re := tk.MustQuery("admin show dbs jobs 1")
	rows := re.Events()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][2], Equals, "t1")

	// Test for reverse scan get history dbs jobs when dbs history jobs queue has multiple regions.
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	historyJobs, err = admin.GetHistoryDBSJobs(txn, 20)
	c.Assert(err, IsNil)

	// Split region for history dbs job queues.
	m := meta.NewMeta(txn)
	startKey := meta.DBSJobHistoryKey(m, 0)
	endKey := meta.DBSJobHistoryKey(m, historyJobs[0].ID)
	s.cluster.SplitKeys(startKey, endKey, int(historyJobs[0].ID/5))

	historyJobs2, err := admin.GetHistoryDBSJobs(txn, 20)
	c.Assert(err, IsNil)
	c.Assert(historyJobs, DeepEquals, historyJobs2)
}

func (s *testSuiteP2) TestAdminShowDBSJobs(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists test_admin_show_dbs_jobs")
	tk.MustExec("use test_admin_show_dbs_jobs")
	tk.MustExec("create block t (a int);")

	re := tk.MustQuery("admin show dbs jobs 1")
	event := re.Events()[0]
	c.Assert(event[1], Equals, "test_admin_show_dbs_jobs")
	jobID, err := strconv.Atoi(event[0].(string))
	c.Assert(err, IsNil)

	err = ekv.RunInNewTxn(s.causetstore, true, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		job, err := t.GetHistoryDBSJob(int64(jobID))
		c.Assert(err, IsNil)
		c.Assert(job, NotNil)
		// Test for compatibility. Old MilevaDB version doesn't have SchemaName field, and the BinlogInfo maybe nil.
		// See PR: 11561.
		job.BinlogInfo = nil
		job.SchemaName = ""
		err = t.AddHistoryDBSJob(job, true)
		c.Assert(err, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	re = tk.MustQuery("admin show dbs jobs 1")
	event = re.Events()[0]
	c.Assert(event[1], Equals, "test_admin_show_dbs_jobs")

	re = tk.MustQuery("admin show dbs jobs 1 where job_type='create block'")
	event = re.Events()[0]
	c.Assert(event[1], Equals, "test_admin_show_dbs_jobs")
	c.Assert(event[9], Equals, "<nil>")

	// Test the START_TIME and END_TIME field.
	re = tk.MustQuery("admin show dbs jobs where job_type = 'create block' and start_time > str_to_date('20190101','%Y%m%d%H%i%s')")
	event = re.Events()[0]
	c.Assert(event[2], Equals, "t")
	c.Assert(event[9], Equals, "<nil>")
}

func (s *testSuiteP2) TestAdminChecksumOfPartitionedBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS admin_checksum_partition_test;")
	tk.MustExec("CREATE TABLE admin_checksum_partition_test (a INT) PARTITION BY HASH(a) PARTITIONS 4;")
	tk.MustExec("INSERT INTO admin_checksum_partition_test VALUES (1), (2);")

	r := tk.MustQuery("ADMIN CHECKSUM TABLE admin_checksum_partition_test;")
	r.Check(testkit.Events("test admin_checksum_partition_test 1 5 5"))
}

func (s *baseTestSuite) fillData(tk *testkit.TestKit, block string) {
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("create block %s(id int not null default 1, name varchar(255), PRIMARY KEY(id));", block))

	// insert data
	tk.MustExec(fmt.Sprintf("insert INTO %s VALUES (1, \"hello\");", block))
	tk.CheckExecResult(1, 0)
	tk.MustExec(fmt.Sprintf("insert into %s values (2, \"hello\");", block))
	tk.CheckExecResult(1, 0)
}

type testCase struct {
	data1       []byte
	data2       []byte
	expected    []string
	restData    []byte
	expectedMsg string
}

func checkCases(tests []testCase, ld *executor.LoadDataInfo,
	c *C, tk *testkit.TestKit, ctx stochastikctx.Context, selectALLEGROSQL, deleteALLEGROSQL string) {
	origin := ld.IgnoreLines
	for _, tt := range tests {
		ld.IgnoreLines = origin
		c.Assert(ctx.NewTxn(context.Background()), IsNil)
		ctx.GetStochastikVars().StmtCtx.DupKeyAsWarning = true
		ctx.GetStochastikVars().StmtCtx.BadNullAsWarning = true
		ctx.GetStochastikVars().StmtCtx.InLoadDataStmt = true
		ctx.GetStochastikVars().StmtCtx.InDeleteStmt = false
		data, reachLimit, err1 := ld.InsertData(context.Background(), tt.data1, tt.data2)
		c.Assert(err1, IsNil)
		c.Assert(reachLimit, IsFalse)
		err1 = ld.CheckAndInsertOneBatch(context.Background(), ld.GetEvents(), ld.GetCurBatchCnt())
		c.Assert(err1, IsNil)
		ld.SetMaxEventsInBatch(20000)
		if tt.restData == nil {
			c.Assert(data, HasLen, 0,
				Commentf("data1:%v, data2:%v, data:%v", string(tt.data1), string(tt.data2), string(data)))
		} else {
			c.Assert(data, DeepEquals, tt.restData,
				Commentf("data1:%v, data2:%v, data:%v", string(tt.data1), string(tt.data2), string(data)))
		}
		ld.SetMessage()
		tk.CheckLastMessage(tt.expectedMsg)
		ctx.StmtCommit()
		txn, err := ctx.Txn(true)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		r := tk.MustQuery(selectALLEGROSQL)
		r.Check(solitonutil.EventsWithSep("|", tt.expected...))
		tk.MustExec(deleteALLEGROSQL)
	}
}

func (s *testSuiteP1) TestSelectWithoutFrom(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	r := tk.MustQuery("select 1 + 2*3;")
	r.Check(testkit.Events("7"))

	r = tk.MustQuery(`select _utf8"string";`)
	r.Check(testkit.Events("string"))

	r = tk.MustQuery("select 1 order by 1;")
	r.Check(testkit.Events("1"))
}

// TestSelectBackslashN Issue 3685.
func (s *testSuiteP1) TestSelectBackslashN(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	allegrosql := `select \N;`
	r := tk.MustQuery(allegrosql)
	r.Check(testkit.Events("<nil>"))
	rs, err := tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "NULL")

	allegrosql = `select "\N";`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("N"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `N`)

	tk.MustExec("use test;")
	tk.MustExec("create block test (`\\N` int);")
	tk.MustExec("insert into test values (1);")
	tk.CheckExecResult(1, 0)
	allegrosql = "select * from test;"
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("1"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `\N`)

	allegrosql = `select \N from test;`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("<nil>"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `NULL`)

	allegrosql = `select (\N) from test;`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("<nil>"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `NULL`)

	allegrosql = "select `\\N` from test;"
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("1"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `\N`)

	allegrosql = "select (`\\N`) from test;"
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("1"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `\N`)

	allegrosql = `select '\N' from test;`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("N"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `N`)

	allegrosql = `select ('\N') from test;`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("N"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `N`)
}

// TestSelectNull Issue #4053.
func (s *testSuiteP1) TestSelectNull(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	allegrosql := `select nUll;`
	r := tk.MustQuery(allegrosql)
	r.Check(testkit.Events("<nil>"))
	rs, err := tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `NULL`)

	allegrosql = `select (null);`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("<nil>"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `NULL`)

	allegrosql = `select null+NULL;`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("<nil>"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `null+NULL`)
}

// TestSelectStringLiteral Issue #3686.
func (s *testSuiteP1) TestSelectStringLiteral(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	allegrosql := `select 'abc';`
	r := tk.MustQuery(allegrosql)
	r.Check(testkit.Events("abc"))
	rs, err := tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `abc`)

	allegrosql = `select (('abc'));`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("abc"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `abc`)

	allegrosql = `select 'abc'+'def';`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("0"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, `'abc'+'def'`)

	// Below checks whether leading invalid chars are trimmed.
	allegrosql = "select '\n';"
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("\n"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "")

	allegrosql = "select '\t   defCaus';" // Lowercased letter is a valid char.
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "defCaus")

	allegrosql = "select '\t   DefCaus';" // Uppercased letter is a valid char.
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "DefCaus")

	allegrosql = "select '\n\t   中文 defCaus';" // Chinese char is a valid char.
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "中文 defCaus")

	allegrosql = "select ' \r\n  .defCaus';" // Punctuation is a valid char.
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, ".defCaus")

	allegrosql = "select '   😆defCaus';" // Emoji is a valid char.
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "😆defCaus")

	// Below checks whether trailing invalid chars are preserved.
	allegrosql = `select 'abc   ';`
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "abc   ")

	allegrosql = `select '  abc   123   ';`
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "abc   123   ")

	// Issue #4239.
	allegrosql = `select 'a' ' ' 'string';`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("a string"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "a")

	allegrosql = `select 'a' " " "string";`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("a string"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "a")

	allegrosql = `select 'string' 'string';`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("stringstring"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "string")

	allegrosql = `select "ss" "a";`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("ssa"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "ss")

	allegrosql = `select "ss" "a" "b";`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("ssab"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "ss")

	allegrosql = `select "ss" "a" ' ' "b";`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("ssa b"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "ss")

	allegrosql = `select "ss" "a" ' ' "b" ' ' "d";`
	r = tk.MustQuery(allegrosql)
	r.Check(testkit.Events("ssa b d"))
	rs, err = tk.Exec(allegrosql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.O, Equals, "ss")
}

func (s *testSuiteP1) TestSelectLimit(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	s.fillData(tk, "select_limit")

	tk.MustExec("insert INTO select_limit VALUES (3, \"hello\");")
	tk.CheckExecResult(1, 0)
	tk.MustExec("insert INTO select_limit VALUES (4, \"hello\");")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("select * from select_limit limit 1;")
	r.Check(testkit.Events("1 hello"))

	r = tk.MustQuery("select id from (select * from select_limit limit 1) k where id != 1;")
	r.Check(testkit.Events())

	r = tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 0;")
	r.Check(testkit.Events("1 hello", "2 hello", "3 hello", "4 hello"))

	r = tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 1;")
	r.Check(testkit.Events("2 hello", "3 hello", "4 hello"))

	r = tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 3;")
	r.Check(testkit.Events("4 hello"))

	err := tk.ExecToErr("select * from select_limit limit 18446744073709551616 offset 3;")
	c.Assert(err, NotNil)
}

func (s *testSuiteP1) TestSelectOrderBy(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	s.fillData(tk, "select_order_test")

	// Test star field
	r := tk.MustQuery("select * from select_order_test where id = 1 order by id limit 1 offset 0;")
	r.Check(testkit.Events("1 hello"))

	r = tk.MustQuery("select id from select_order_test order by id desc limit 1 ")
	r.Check(testkit.Events("2"))

	r = tk.MustQuery("select id from select_order_test order by id + 1 desc limit 1 ")
	r.Check(testkit.Events("2"))

	// Test limit
	r = tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 0;")
	r.Check(testkit.Events("1 hello"))

	// Test limit
	r = tk.MustQuery("select id as c1, name from select_order_test order by 2, id limit 1 offset 0;")
	r.Check(testkit.Events("1 hello"))

	// Test limit overflow
	r = tk.MustQuery("select * from select_order_test order by name, id limit 100 offset 0;")
	r.Check(testkit.Events("1 hello", "2 hello"))

	// Test offset overflow
	r = tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 100;")
	r.Check(testkit.Events())

	// Test limit exceeds int range.
	r = tk.MustQuery("select id from select_order_test order by name, id limit 18446744073709551615;")
	r.Check(testkit.Events("1", "2"))

	// Test multiple field
	r = tk.MustQuery("select id, name from select_order_test where id = 1 group by id, name limit 1 offset 0;")
	r.Check(testkit.Events("1 hello"))

	// Test limit + order by
	for i := 3; i <= 10; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"zz\");", i))
	}
	tk.MustExec("insert INTO select_order_test VALUES (10086, \"hi\");")
	for i := 11; i <= 20; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"hh\");", i))
	}
	for i := 21; i <= 30; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"zz\");", i))
	}
	tk.MustExec("insert INTO select_order_test VALUES (1501, \"aa\");")
	r = tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 3;")
	r.Check(testkit.Events("11 hh"))
	tk.MustExec("drop block select_order_test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (1, 3)")
	r = tk.MustQuery("select 1-d as d from t order by d;")
	r.Check(testkit.Events("-2", "-1", "0"))
	r = tk.MustQuery("select 1-d as d from t order by d + 1;")
	r.Check(testkit.Events("0", "-1", "-2"))
	r = tk.MustQuery("select t.d from t order by d;")
	r.Check(testkit.Events("1", "2", "3"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b int, c int)")
	tk.MustExec("insert t values (1, 2, 3)")
	r = tk.MustQuery("select b from (select a,b from t order by a,c) t")
	r.Check(testkit.Events("2"))
	r = tk.MustQuery("select b from (select a,b from t order by a,c limit 1) t")
	r.Check(testkit.Events("2"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, index idx(a))")
	tk.MustExec("insert into t values(1, 1), (2, 2)")
	tk.MustQuery("select * from t where 1 order by b").Check(testkit.Events("1 1", "2 2"))
	tk.MustQuery("select * from t where a between 1 and 2 order by a desc").Check(testkit.Events("2 2", "1 1"))

	// Test double read and topN is pushed down to first read plannercore.
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values(1, 3, 1)")
	tk.MustExec("insert into t values(2, 2, 2)")
	tk.MustExec("insert into t values(3, 1, 3)")
	tk.MustQuery("select * from t use index(idx) order by a desc limit 1").Check(testkit.Events("3 1 3"))

	// Test double read which needs to keep order.
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, key b (b))")
	tk.Se.GetStochastikVars().IndexLookupSize = 3
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%d, %d)", i, 10-i))
	}
	tk.MustQuery("select a from t use index(b) order by b").Check(testkit.Events("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"))
}

func (s *testSuiteP1) TestOrderBy(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c1 int, c2 int, c3 varchar(20))")
	tk.MustExec("insert into t values (1, 2, 'abc'), (2, 1, 'bcd')")

	// Fix issue https://github.com/whtcorpsinc/milevadb/issues/337
	tk.MustQuery("select c1 as a, c1 as b from t order by c1").Check(testkit.Events("1 1", "2 2"))

	tk.MustQuery("select c1 as a, t.c1 as a from t order by a desc").Check(testkit.Events("2 2", "1 1"))
	tk.MustQuery("select c1 as c2 from t order by c2").Check(testkit.Events("1", "2"))
	tk.MustQuery("select sum(c1) from t order by sum(c1)").Check(testkit.Events("3"))
	tk.MustQuery("select c1 as c2 from t order by c2 + 1").Check(testkit.Events("2", "1"))

	// Order by position.
	tk.MustQuery("select * from t order by 1").Check(testkit.Events("1 2 abc", "2 1 bcd"))
	tk.MustQuery("select * from t order by 2").Check(testkit.Events("2 1 bcd", "1 2 abc"))

	// Order by binary.
	tk.MustQuery("select c1, c3 from t order by binary c1 desc").Check(testkit.Events("2 bcd", "1 abc"))
	tk.MustQuery("select c1, c2 from t order by binary c3").Check(testkit.Events("1 2", "2 1"))
}

func (s *testSuiteP1) TestSelectErrorEvent(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	err := tk.ExecToErr("select event(1, 1) from test")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test group by event(1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test order by event(1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test having event(1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select (select 1, 1) from test;")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test group by (select 1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test order by (select 1, 1);")
	c.Assert(err, NotNil)

	err = tk.ExecToErr("select * from test having (select 1, 1);")
	c.Assert(err, NotNil)
}

// TestIssue2612 is related with https://github.com/whtcorpsinc/milevadb/issues/2612
func (s *testSuiteP1) TestIssue2612(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t (
		create_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00',
		finish_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00');`)
	tk.MustExec(`insert into t values ('2020-02-13 15:32:24',  '2020-02-11 17:23:22');`)
	rs, err := tk.Exec(`select timediff(finish_at, create_at) from t;`)
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(req.GetEvent(0).GetDuration(0, 0).String(), Equals, "-46:09:02")
	rs.Close()
}

// TestIssue345 is related with https://github.com/whtcorpsinc/milevadb/issues/345
func (s *testSuiteP1) TestIssue345(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t1, t2`)
	tk.MustExec(`create block t1 (c1 int);`)
	tk.MustExec(`create block t2 (c2 int);`)
	tk.MustExec(`insert into t1 values (1);`)
	tk.MustExec(`insert into t2 values (2);`)
	tk.MustExec(`uFIDelate t1, t2 set t1.c1 = 2, t2.c2 = 1;`)
	tk.MustExec(`uFIDelate t1, t2 set c1 = 2, c2 = 1;`)
	tk.MustExec(`uFIDelate t1 as a, t2 as b set a.c1 = 2, b.c2 = 1;`)

	// Check t1 content
	r := tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Events("2"))
	// Check t2 content
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Events("1"))

	tk.MustExec(`uFIDelate t1 as a, t2 as t1 set a.c1 = 1, t1.c2 = 2;`)
	// Check t1 content
	r = tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Events("1"))
	// Check t2 content
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Events("2"))

	_, err := tk.Exec(`uFIDelate t1 as a, t2 set t1.c1 = 10;`)
	c.Assert(err, NotNil)
}

func (s *testSuiteP1) TestIssue5055(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t1, t2`)
	tk.MustExec(`create block t1 (a int);`)
	tk.MustExec(`create block t2 (a int);`)
	tk.MustExec(`insert into t1 values(1);`)
	tk.MustExec(`insert into t2 values(1);`)
	result := tk.MustQuery("select tbl1.* from (select t1.a, 1 from t1) tbl1 left join t2 tbl2 on tbl1.a = tbl2.a order by tbl1.a desc limit 1;")
	result.Check(testkit.Events("1 1"))
}

func (s *testSuiteWithData) TestSetOperation(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)
	tk.MustExec(`drop block if exists t1, t2, t3`)
	tk.MustExec(`create block t1(a int)`)
	tk.MustExec(`create block t2 like t1`)
	tk.MustExec(`create block t3 like t1`)
	tk.MustExec(`insert into t1 values (1),(1),(2),(3),(null)`)
	tk.MustExec(`insert into t2 values (1),(2),(null),(null)`)
	tk.MustExec(`insert into t3 values (2),(3)`)

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
			output[i].Plan = s.testData.ConvertEventsToStrings(tk.MustQuery("explain " + tt).Events())
			output[i].Res = s.testData.ConvertEventsToStrings(tk.MustQuery(tt).Sort().Events())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Events(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Events(output[i].Res...))
	}
}

func (s *testSuiteWithData) TestSetOperationOnDiffDefCausType(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)
	tk.MustExec(`drop block if exists t1, t2, t3`)
	tk.MustExec(`create block t1(a int, b int)`)
	tk.MustExec(`create block t2(a int, b varchar(20))`)
	tk.MustExec(`create block t3(a int, b decimal(30,10))`)
	tk.MustExec(`insert into t1 values (1,1),(1,1),(2,2),(3,3),(null,null)`)
	tk.MustExec(`insert into t2 values (1,'1'),(2,'2'),(null,null),(null,'3')`)
	tk.MustExec(`insert into t3 values (2,2.1),(3,3)`)

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
			output[i].Plan = s.testData.ConvertEventsToStrings(tk.MustQuery("explain " + tt).Events())
			output[i].Res = s.testData.ConvertEventsToStrings(tk.MustQuery(tt).Sort().Events())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Events(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Events(output[i].Res...))
	}
}

func (s *testSuiteP2) TestUnion(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	testALLEGROSQL := `drop block if exists union_test; create block union_test(id int);`
	tk.MustExec(testALLEGROSQL)

	testALLEGROSQL = `drop block if exists union_test;`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `create block union_test(id int);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `insert union_test values (1),(2)`
	tk.MustExec(testALLEGROSQL)

	testALLEGROSQL = `select * from (select id from union_test union select id from union_test) t order by id;`
	r := tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Events("1", "2"))

	r = tk.MustQuery("select 1 union all select 1")
	r.Check(testkit.Events("1", "1"))

	r = tk.MustQuery("select 1 union all select 1 union select 1")
	r.Check(testkit.Events("1"))

	r = tk.MustQuery("select 1 as a union (select 2) order by a limit 1")
	r.Check(testkit.Events("1"))

	r = tk.MustQuery("select 1 as a union (select 2) order by a limit 1, 1")
	r.Check(testkit.Events("2"))

	r = tk.MustQuery("select id from union_test union all (select 1) order by id desc")
	r.Check(testkit.Events("2", "1", "1"))

	r = tk.MustQuery("select id as a from union_test union (select 1) order by a desc")
	r.Check(testkit.Events("2", "1"))

	r = tk.MustQuery(`select null as a union (select "abc") order by a`)
	r.Check(testkit.Events("<nil>", "abc"))

	r = tk.MustQuery(`select "abc" as a union (select 1) order by a`)
	r.Check(testkit.Events("1", "abc"))

	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (c int, d int)")
	tk.MustExec("insert t1 values (NULL, 1)")
	tk.MustExec("insert t1 values (1, 1)")
	tk.MustExec("insert t1 values (1, 2)")
	tk.MustExec("drop block if exists t2")
	tk.MustExec("create block t2 (c int, d int)")
	tk.MustExec("insert t2 values (1, 3)")
	tk.MustExec("insert t2 values (1, 1)")
	tk.MustExec("drop block if exists t3")
	tk.MustExec("create block t3 (c int, d int)")
	tk.MustExec("insert t3 values (3, 2)")
	tk.MustExec("insert t3 values (4, 3)")
	r = tk.MustQuery(`select sum(c1), c2 from (select c c1, d c2 from t1 union all select d c1, c c2 from t2 union all select c c1, d c2 from t3) x group by c2 order by c2`)
	r.Check(testkit.Events("5 1", "4 2", "4 3"))

	tk.MustExec("drop block if exists t1, t2, t3")
	tk.MustExec("create block t1 (a int primary key)")
	tk.MustExec("create block t2 (a int primary key)")
	tk.MustExec("create block t3 (a int primary key)")
	tk.MustExec("insert t1 values (7), (8)")
	tk.MustExec("insert t2 values (1), (9)")
	tk.MustExec("insert t3 values (2), (3)")
	r = tk.MustQuery("select * from t1 union all select * from t2 union all (select * from t3) order by a limit 2")
	r.Check(testkit.Events("1", "2"))

	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1 (a int)")
	tk.MustExec("create block t2 (a int)")
	tk.MustExec("insert t1 values (2), (1)")
	tk.MustExec("insert t2 values (3), (4)")
	r = tk.MustQuery("select * from t1 union all (select * from t2) order by a limit 1")
	r.Check(testkit.Events("1"))
	r = tk.MustQuery("select (select * from t1 where a != t.a union all (select * from t2 where a != t.a) order by a limit 1) from t1 t")
	r.Check(testkit.Events("1", "2"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (id int unsigned primary key auto_increment, c1 int, c2 int, index c1_c2 (c1, c2))")
	tk.MustExec("insert into t (c1, c2) values (1, 1)")
	tk.MustExec("insert into t (c1, c2) values (1, 2)")
	tk.MustExec("insert into t (c1, c2) values (2, 3)")
	r = tk.MustQuery("select * from (select * from t where t.c1 = 1 union select * from t where t.id = 1) s order by s.id")
	r.Check(testkit.Events("1 1 1", "2 1 2"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (f1 DATE)")
	tk.MustExec("INSERT INTO t VALUES ('1978-11-26')")
	r = tk.MustQuery("SELECT f1+0 FROM t UNION SELECT f1+0 FROM t")
	r.Check(testkit.Events("19781126"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (a int, b int)")
	tk.MustExec("INSERT INTO t VALUES ('1', '1')")
	r = tk.MustQuery("select b from (SELECT * FROM t UNION ALL SELECT a, b FROM t order by a) t")
	r.Check(testkit.Events("1", "1"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (a DECIMAL(4,2))")
	tk.MustExec("INSERT INTO t VALUE(12.34)")
	r = tk.MustQuery("SELECT 1 AS c UNION select a FROM t")
	r.Sort().Check(testkit.Events("1.00", "12.34"))

	// #issue3771
	r = tk.MustQuery("SELECT 'a' UNION SELECT CONCAT('a', -4)")
	r.Sort().Check(testkit.Events("a", "a-4"))

	// test race
	tk.MustQuery("SELECT @x:=0 UNION ALL SELECT @x:=0 UNION ALL SELECT @x")

	// test field tp
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("CREATE TABLE t1 (a date)")
	tk.MustExec("CREATE TABLE t2 (a date)")
	tk.MustExec("SELECT a from t1 UNION select a FROM t2")
	tk.MustQuery("show create block t1").Check(testkit.Events("t1 CREATE TABLE `t1` (\n" + "  `a` date DEFAULT NULL\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Move from stochastik test.
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1 (c double);")
	tk.MustExec("create block t2 (c double);")
	tk.MustExec("insert into t1 value (73);")
	tk.MustExec("insert into t2 value (930);")
	// If set unspecified defCausumn flen to 0, it will cause bug in union.
	// This test is used to prevent the bug reappear.
	tk.MustQuery("select c from t1 union (select c from t2) order by c").Check(testkit.Events("73", "930"))

	// issue 5703
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a date)")
	tk.MustExec("insert into t value ('2020-01-01'), ('2020-01-02')")
	r = tk.MustQuery("(select a from t where a < 0) union (select a from t where a > 0) order by a")
	r.Check(testkit.Events("2020-01-01", "2020-01-02"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t value(0),(0)")
	tk.MustQuery("select 1 from (select a from t union all select a from t) tmp").Check(testkit.Events("1", "1", "1", "1"))
	tk.MustQuery("select 10 as a from dual union select a from t order by a desc limit 1 ").Check(testkit.Events("10"))
	tk.MustQuery("select -10 as a from dual union select a from t order by a limit 1 ").Check(testkit.Events("-10"))
	tk.MustQuery("select count(1) from (select a from t union all select a from t) tmp").Check(testkit.Events("4"))

	err := tk.ExecToErr("select 1 from (select a from t limit 1 union all select a from t limit 1) tmp")
	c.Assert(err, NotNil)
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrWrongUsage))

	err = tk.ExecToErr("select 1 from (select a from t order by a union all select a from t limit 1) tmp")
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrWrongUsage))

	_, err = tk.Exec("(select a from t order by a) union all select a from t limit 1 union all select a from t limit 1")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrWrongUsage), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("(select a from t limit 1) union all select a from t limit 1")
	c.Assert(err, IsNil)
	_, err = tk.Exec("(select a from t order by a) union all select a from t order by a")
	c.Assert(err, IsNil)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t value(1),(2),(3)")

	tk.MustQuery("(select a from t order by a limit 2) union all (select a from t order by a desc limit 2) order by a desc limit 1,2").Check(testkit.Events("2", "2"))
	tk.MustQuery("select a from t union all select a from t order by a desc limit 5").Check(testkit.Events("3", "3", "2", "2", "1"))
	tk.MustQuery("(select a from t order by a desc limit 2) union all select a from t group by a order by a").Check(testkit.Rows("1", "2", "2", "3", "3"))
	tk.MustQuery("(select a from t order by a desc limit 2) union all select 33 as a order by a desc limit 2").Check(testkit.Rows("33", "3"))

	tk.MustQuery("select 1 union select 1 union all select 1").Check(testkit.Rows("1", "1"))
	tk.MustQuery("select 1 union all select 1 union select 1").Check(testkit.Rows("1"))

	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec(`create block t1(a bigint, b bigint);`)
	tk.MustExec(`create block t2(a bigint, b bigint);`)
	tk.MustExec(`insert into t1 values(1, 1);`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t1 select * from t1;`)
	tk.MustExec(`insert into t2 values(1, 1);`)
	tk.MustExec(`set @@milevadb_init_chunk_size=2;`)
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustQuery(`select count(*) from (select t1.a, t1.b from t1 left join t2 on t1.a=t2.a union all select t1.a, t1.a from t1 left join t2 on t1.a=t2.a) tmp;`).Check(testkit.Rows("128"))
	tk.MustQuery(`select tmp.a, count(*) from (select t1.a, t1.b from t1 left join t2 on t1.a=t2.a union all select t1.a, t1.a from t1 left join t2 on t1.a=t2.a) tmp;`).Check(testkit.Rows("1 128"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int)")
	tk.MustExec("insert into t value(1 ,2)")
	tk.MustQuery("select a, b from (select a, 0 as d, b from t union all select a, 0 as d, b from t) test;").Check(testkit.Rows("1 2", "1 2"))

	// #issue 8141
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(a int, b int)")
	tk.MustExec("insert into t1 value(1,2),(1,1),(2,2),(2,2),(3,2),(3,2)")
	tk.MustExec("set @@milevadb_init_chunk_size=2;")
	tk.MustQuery("select count(*) from (select a as c, a as d from t1 union all select a, b from t1) t;").Check(testkit.Rows("12"))

	// #issue 8189 and #issue 8199
	tk.MustExec("drop block if exists t1")
	tk.MustExec("drop block if exists t2")
	tk.MustExec("CREATE TABLE t1 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t1 values(1,'a'),(2,'b'),(3,'c'),(3,'c')")
	tk.MustExec("CREATE TABLE t2 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t2 values(1,'a'),(2,'b'),(3,'c'),(3,'c')")
	tk.MustQuery("select a from t1 union select a from t1 order by (select a+1);").Check(testkit.Rows("1", "2", "3"))

	// #issue 8201
	for i := 0; i < 4; i++ {
		tk.MustQuery("SELECT(SELECT 0 AS a FROM dual UNION SELECT 1 AS a FROM dual ORDER BY a ASC  LIMIT 1) AS dev").Check(testkit.Rows("0"))
	}

	// #issue 8231
	tk.MustExec("drop block if exists t1")
	tk.MustExec("CREATE TABLE t1 (uid int(1))")
	tk.MustExec("INSERT INTO t1 SELECT 150")
	tk.MustQuery("SELECT 'a' UNION SELECT uid FROM t1 order by 1 desc;").Check(testkit.Rows("a", "150"))

	// #issue 8196
	tk.MustExec("drop block if exists t1")
	tk.MustExec("drop block if exists t2")
	tk.MustExec("CREATE TABLE t1 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t1 values(1,'a'),(2,'b'),(3,'c'),(3,'c')")
	tk.MustExec("CREATE TABLE t2 (a int not null, b char (10) not null)")
	tk.MustExec("insert into t2 values(3,'c'),(4,'d'),(5,'f'),(6,'e')")
	tk.MustExec("analyze block t1")
	tk.MustExec("analyze block t2")
	_, err = tk.Exec("(select a,b from t1 limit 2) union all (select a,b from t2 order by a limit 1) order by t1.b")
	c.Assert(err.Error(), Equals, "[planner:1250]Block 't1' from one of the SELECTs cannot be used in global ORDER clause")

	// #issue 9900
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b decimal(6, 3))")
	tk.MustExec("insert into t values(1, 1.000)")
	tk.MustQuery("select count(distinct a), sum(distinct a), avg(distinct a) from (select a from t union all select b from t) tmp;").Check(testkit.Rows("1 1.000 1.0000000"))
}

func (s *testSuite2) TestUnionLimit(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists union_limit")
	tk.MustExec("create block union_limit (id int) partition by hash(id) partitions 30")
	for i := 0; i < 60; i++ {
		tk.MustExec(fmt.Sprintf("insert into union_limit values (%d)", i))
	}
	// Cover the code for worker count limit in the union executor.
	tk.MustQuery("select * from union_limit limit 10")
}

func (s *testSuiteP1) TestNeighbouringProj(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int, b int)")
	tk.MustExec("create block t2(a int, b int)")
	tk.MustExec("insert into t1 value(1, 1), (2, 2)")
	tk.MustExec("insert into t2 value(1, 1), (2, 2)")
	tk.MustQuery("select sum(c) from (select t1.a as a, t1.a as c, length(t1.b) from t1  union select a, b, b from t2) t;").Check(testkit.Rows("5"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a bigint, b bigint, c bigint);")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3);")
	rs := tk.MustQuery("select cast(count(a) as signed), a as another, a from t group by a order by cast(count(a) as signed), a limit 10;")
	rs.Check(testkit.Rows("1 1 1", "1 2 2", "1 3 3"))
}

func (s *testSuiteP1) TestIn(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t (c1 int primary key, c2 int, key c (c2));`)
	for i := 0; i <= 200; i++ {
		tk.MustExec(fmt.Sprintf("insert t values(%d, %d)", i, i))
	}
	queryStr := `select c2 from t where c1 in ('7', '10', '112', '111', '98', '106', '100', '9', '18', '17') order by c2`
	r := tk.MustQuery(queryStr)
	r.Check(testkit.Rows("7", "9", "10", "17", "18", "98", "100", "106", "111", "112"))

	queryStr = `select c2 from t where c1 in ('7a')`
	tk.MustQuery(queryStr).Check(testkit.Rows("7"))
}

func (s *testSuiteP1) TestBlockPKisHandleScan(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int PRIMARY KEY AUTO_INCREMENT)")
	tk.MustExec("insert t values (),()")
	tk.MustExec("insert t values (-100),(0)")

	tests := []struct {
		allegrosql string
		result     [][]interface{}
	}{
		{
			"select * from t",
			testkit.Rows("-100", "1", "2", "3"),
		},
		{
			"select * from t where a = 1",
			testkit.Rows("1"),
		},
		{
			"select * from t where a != 1",
			testkit.Rows("-100", "2", "3"),
		},
		{
			"select * from t where a >= '1.1'",
			testkit.Rows("2", "3"),
		},
		{
			"select * from t where a < '1.1'",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a > '-100.1' and a < 2",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a is null",
			testkit.Rows(),
		}, {
			"select * from t where a is true",
			testkit.Rows("-100", "1", "2", "3"),
		}, {
			"select * from t where a is false",
			testkit.Rows(),
		},
		{
			"select * from t where a in (1, 2)",
			testkit.Rows("1", "2"),
		},
		{
			"select * from t where a between 1 and 2",
			testkit.Rows("1", "2"),
		},
	}

	for _, tt := range tests {
		result := tk.MustQuery(tt.allegrosql)
		result.Check(tt.result)
	}
}

func (s *testSuite8) TestIndexScan(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int unique)")
	tk.MustExec("insert t values (-1), (2), (3), (5), (6), (7), (8), (9)")
	result := tk.MustQuery("select a from t where a < 0 or (a >= 2.1 and a < 5.1) or ( a > 5.9 and a <= 7.9) or a > '8.1'")
	result.Check(testkit.Rows("-1", "3", "5", "6", "7", "9"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int unique)")
	tk.MustExec("insert t values (0)")
	result = tk.MustQuery("select NULL from t ")
	result.Check(testkit.Rows("<nil>"))
	// test for double read
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int unique, b int)")
	tk.MustExec("insert t values (5, 0)")
	tk.MustExec("insert t values (4, 0)")
	tk.MustExec("insert t values (3, 0)")
	tk.MustExec("insert t values (2, 0)")
	tk.MustExec("insert t values (1, 0)")
	tk.MustExec("insert t values (0, 0)")
	result = tk.MustQuery("select * from t order by a limit 3")
	result.Check(testkit.Rows("0 0", "1 0", "2 0"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int unique, b int)")
	tk.MustExec("insert t values (0, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (2, 1)")
	tk.MustExec("insert t values (3, 2)")
	tk.MustExec("insert t values (4, 1)")
	tk.MustExec("insert t values (5, 2)")
	result = tk.MustQuery("select * from t where a < 5 and b = 1 limit 2")
	result.Check(testkit.Rows("0 1", "2 1"))
	tk.MustExec("drop block if exists tab1")
	tk.MustExec("CREATE TABLE tab1(pk INTEGER PRIMARY KEY, defCaus0 INTEGER, defCaus1 FLOAT, defCaus3 INTEGER, defCaus4 FLOAT)")
	tk.MustExec("CREATE INDEX idx_tab1_0 on tab1 (defCaus0)")
	tk.MustExec("CREATE INDEX idx_tab1_1 on tab1 (defCaus1)")
	tk.MustExec("CREATE INDEX idx_tab1_3 on tab1 (defCaus3)")
	tk.MustExec("CREATE INDEX idx_tab1_4 on tab1 (defCaus4)")
	tk.MustExec("INSERT INTO tab1 VALUES(1,37,20.85,30,10.69)")
	result = tk.MustQuery("SELECT pk FROM tab1 WHERE ((defCaus3 <= 6 OR defCaus3 < 29 AND (defCaus0 < 41)) OR defCaus3 > 42) AND defCaus1 >= 96.1 AND defCaus3 = 30 AND defCaus3 > 17 AND (defCaus0 BETWEEN 36 AND 42)")
	result.Check(testkit.Rows())
	tk.MustExec("drop block if exists tab1")
	tk.MustExec("CREATE TABLE tab1(pk INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	tk.MustExec("CREATE INDEX idx_tab1_0 on tab1 (a)")
	tk.MustExec("INSERT INTO tab1 VALUES(1,1,1)")
	tk.MustExec("INSERT INTO tab1 VALUES(2,2,1)")
	tk.MustExec("INSERT INTO tab1 VALUES(3,1,2)")
	tk.MustExec("INSERT INTO tab1 VALUES(4,2,2)")
	result = tk.MustQuery("SELECT * FROM tab1 WHERE pk <= 3 AND a = 1")
	result.Check(testkit.Rows("1 1 1", "3 1 2"))
	result = tk.MustQuery("SELECT * FROM tab1 WHERE pk <= 4 AND a = 1 AND b = 2")
	result.Check(testkit.Rows("3 1 2"))
	tk.MustExec("CREATE INDEX idx_tab1_1 on tab1 (b, a)")
	result = tk.MustQuery("SELECT pk FROM tab1 WHERE b > 1")
	result.Check(testkit.Rows("3", "4"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (a varchar(3), index(a))")
	tk.MustExec("insert t values('aaa'), ('aab')")
	result = tk.MustQuery("select * from t where a >= 'aaaa' and a < 'aabb'")
	result.Check(testkit.Rows("aab"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (a int primary key, b int, c int, index(c))")
	tk.MustExec("insert t values(1, 1, 1), (2, 2, 2), (4, 4, 4), (3, 3, 3), (5, 5, 5)")
	// Test for double read and top n.
	result = tk.MustQuery("select a from t where c >= 2 order by b desc limit 1")
	result.Check(testkit.Rows("5"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a varchar(50) primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values('aa', 1, 1)")
	tk.MustQuery("select * from t use index(idx) where a > 'a'").Check(testkit.Rows("aa 1 1"))

	// fix issue9636
	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE `t` (a int, KEY (a))")
	result = tk.MustQuery(`SELECT * FROM (SELECT * FROM (SELECT a as d FROM t WHERE a IN ('100')) AS x WHERE x.d < "123" ) tmp_count`)
	result.Check(testkit.Rows())
}

func (s *testSuiteP1) TestIndexReverseOrder(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int primary key auto_increment, b int, index idx (b))")
	tk.MustExec("insert t (b) values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)")
	result := tk.MustQuery("select b from t order by b desc")
	result.Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"))
	result = tk.MustQuery("select b from t where b <3 or (b >=6 and b < 8) order by b desc")
	result.Check(testkit.Rows("7", "6", "2", "1", "0"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b int, index idx (b, a))")
	tk.MustExec("insert t values (0, 2), (1, 2), (2, 2), (0, 1), (1, 1), (2, 1), (0, 0), (1, 0), (2, 0)")
	result = tk.MustQuery("select b, a from t order by b, a desc")
	result.Check(testkit.Rows("0 2", "0 1", "0 0", "1 2", "1 1", "1 0", "2 2", "2 1", "2 0"))
}

func (s *testSuiteP1) TestBlockReverseOrder(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int primary key auto_increment, b int)")
	tk.MustExec("insert t (b) values (1), (2), (3), (4), (5), (6), (7), (8), (9)")
	result := tk.MustQuery("select b from t order by a desc")
	result.Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1"))
	result = tk.MustQuery("select a from t where a <3 or (a >=6 and a < 8) order by a desc")
	result.Check(testkit.Rows("7", "6", "2", "1"))
}

func (s *testSuiteP1) TestDefaultNull(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int primary key auto_increment, b int default 1, c int)")
	tk.MustExec("insert t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 <nil>"))
	tk.MustExec("uFIDelate t set b = NULL where a = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustExec("uFIDelate t set c = 1")
	tk.MustQuery("select * from t ").Check(testkit.Rows("1 <nil> 1"))
	tk.MustExec("delete from t where a = 1")
	tk.MustExec("insert t (a) values (1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 <nil>"))
}

func (s *testSuiteP1) TestUnsignedPKDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int unsigned primary key, b int, c int, key idx_ba (b, c, a));")
	tk.MustExec("insert t values (1, 1, 1)")
	result := tk.MustQuery("select * from t;")
	result.Check(testkit.Rows("1 1 1"))
	tk.MustExec("uFIDelate t set c=2 where a=1;")
	result = tk.MustQuery("select * from t where b=1;")
	result.Check(testkit.Rows("1 1 2"))
}

func (s *testSuiteP1) TestJSON(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists test_json")
	tk.MustExec("create block test_json (id int, a json)")
	tk.MustExec(`insert into test_json (id, a) values (1, '{"a":[1,"2",{"aa":"bb"},4],"b":true}')`)
	tk.MustExec(`insert into test_json (id, a) values (2, "null")`)
	tk.MustExec(`insert into test_json (id, a) values (3, null)`)
	tk.MustExec(`insert into test_json (id, a) values (4, 'true')`)
	tk.MustExec(`insert into test_json (id, a) values (5, '3')`)
	tk.MustExec(`insert into test_json (id, a) values (5, '4.0')`)
	tk.MustExec(`insert into test_json (id, a) values (6, '"string"')`)

	result := tk.MustQuery(`select tj.a from test_json tj order by tj.id`)
	result.Check(testkit.Rows(`{"a": [1, "2", {"aa": "bb"}, 4], "b": true}`, "null", "<nil>", "true", "3", "4", `"string"`))

	// Check json_type function
	result = tk.MustQuery(`select json_type(a) from test_json tj order by tj.id`)
	result.Check(testkit.Rows("OBJECT", "NULL", "<nil>", "BOOLEAN", "INTEGER", "DOUBLE", "STRING"))

	// Check json compare with primitives.
	result = tk.MustQuery(`select a from test_json tj where a = 3`)
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery(`select a from test_json tj where a = 4.0`)
	result.Check(testkit.Rows("4"))
	result = tk.MustQuery(`select a from test_json tj where a = true`)
	result.Check(testkit.Rows("true"))
	result = tk.MustQuery(`select a from test_json tj where a = "string"`)
	result.Check(testkit.Rows(`"string"`))

	// Check cast(true/false as JSON).
	result = tk.MustQuery(`select cast(true as JSON)`)
	result.Check(testkit.Rows(`true`))
	result = tk.MustQuery(`select cast(false as JSON)`)
	result.Check(testkit.Rows(`false`))

	// Check two json grammar sugar.
	result = tk.MustQuery(`select a->>'$.a[2].aa' as x, a->'$.b' as y from test_json having x is not null order by id`)
	result.Check(testkit.Rows(`bb true`))
	result = tk.MustQuery(`select a->'$.a[2].aa' as x, a->>'$.b' as y from test_json having x is not null order by id`)
	result.Check(testkit.Rows(`"bb" true`))

	// Check some DBS limits for TEXT/BLOB/JSON defCausumn.
	var err error
	var terr *terror.Error

	_, err = tk.Exec(`create block test_bad_json(a json default '{}')`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrBlobCantHaveDefault))

	_, err = tk.Exec(`create block test_bad_json(a blob default 'hello')`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrBlobCantHaveDefault))

	_, err = tk.Exec(`create block test_bad_json(a text default 'world')`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrBlobCantHaveDefault))

	// check json fields cannot be used as key.
	_, err = tk.Exec(`create block test_bad_json(id int, a json, key (a))`)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrJSONUsedAsKey))

	// check CAST AS JSON.
	result = tk.MustQuery(`select CAST('3' AS JSON), CAST('{}' AS JSON), CAST(null AS JSON)`)
	result.Check(testkit.Rows(`3 {} <nil>`))

	tk.MustQuery("select a, count(1) from test_json group by a order by a").Check(testkit.Rows(
		"<nil> 1",
		"null 1",
		"3 1",
		"4 1",
		`"string" 1`,
		"{\"a\": [1, \"2\", {\"aa\": \"bb\"}, 4], \"b\": true} 1",
		"true 1"))

	// Check cast json to decimal.
	// NOTE: this test case contains a bug, it should be uncommented after the bug is fixed.
	// TODO: Fix bug https://github.com/whtcorpsinc/milevadb/issues/12178
	//tk.MustExec("drop block if exists test_json")
	//tk.MustExec("create block test_json ( a decimal(60,2) as (JSON_EXTRACT(b,'$.c')), b json );")
	//tk.MustExec(`insert into test_json (b) values
	//	('{"c": "1267.1"}'),
	//	('{"c": "1267.01"}'),
	//	('{"c": "1267.1234"}'),
	//	('{"c": "1267.3456"}'),
	//	('{"c": "1234567890123456789012345678901234567890123456789012345"}'),
	//	('{"c": "1234567890123456789012345678901234567890123456789012345.12345"}');`)
	//
	//tk.MustQuery("select a from test_json;").Check(testkit.Rows("1267.10", "1267.01", "1267.12",
	//	"1267.35", "1234567890123456789012345678901234567890123456789012345.00",
	//	"1234567890123456789012345678901234567890123456789012345.12"))
}

func (s *testSuiteP1) TestMultiUFIDelate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_mu (a int primary key, b int, c int)`)
	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)`)

	// Test INSERT ... ON DUPLICATE UFIDelATE set_lists.
	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3) ON DUPLICATE KEY UFIDelATE b = 3, c = b`)
	result := tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 3 3`, `4 5 6`, `7 8 9`))

	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3) ON DUPLICATE KEY UFIDelATE c = 2, b = c+5`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 5 6`, `7 8 9`))

	// Test UFIDelATE ... set_lists.
	tk.MustExec(`UFIDelATE test_mu SET b = 0, c = b WHERE a = 4`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 0 0`, `7 8 9`))

	tk.MustExec(`UFIDelATE test_mu SET c = 8, b = c WHERE a = 4`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 8 8`, `7 8 9`))

	tk.MustExec(`UFIDelATE test_mu SET c = b, b = c WHERE a = 7`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 8 8`, `7 8 8`))
}

func (s *testSuiteP1) TestGeneratedDeferredCausetWrite(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	_, err := tk.Exec(`CREATE TABLE test_gc_write (a int primary key auto_increment, b int, c int as (a+8) virtual)`)
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetRefAutoInc.GenWithStackByArgs("c").Error())
	tk.MustExec(`CREATE TABLE test_gc_write (a int primary key auto_increment, b int, c int as (b+8) virtual)`)
	tk.MustExec(`CREATE TABLE test_gc_write_1 (a int primary key, b int, c int)`)

	tests := []struct {
		stmt string
		err  int
	}{
		// Can't modify generated defCausumn by values.
		{`insert into test_gc_write (a, b, c) values (1, 1, 1)`, allegrosql.ErrBadGeneratedDeferredCauset},
		{`insert into test_gc_write values (1, 1, 1)`, allegrosql.ErrBadGeneratedDeferredCauset},
		// Can't modify generated defCausumn by select clause.
		{`insert into test_gc_write select 1, 1, 1`, allegrosql.ErrBadGeneratedDeferredCauset},
		// Can't modify generated defCausumn by on duplicate clause.
		{`insert into test_gc_write (a, b) values (1, 1) on duplicate key uFIDelate c = 1`, allegrosql.ErrBadGeneratedDeferredCauset},
		// Can't modify generated defCausumn by set.
		{`insert into test_gc_write set a = 1, b = 1, c = 1`, allegrosql.ErrBadGeneratedDeferredCauset},
		// Can't modify generated defCausumn by uFIDelate clause.
		{`uFIDelate test_gc_write set c = 1`, allegrosql.ErrBadGeneratedDeferredCauset},
		// Can't modify generated defCausumn by multi-block uFIDelate clause.
		{`uFIDelate test_gc_write, test_gc_write_1 set test_gc_write.c = 1`, allegrosql.ErrBadGeneratedDeferredCauset},

		// Can insert without generated defCausumns.
		{`insert into test_gc_write (a, b) values (1, 1)`, 0},
		{`insert into test_gc_write set a = 2, b = 2`, 0},
		{`insert into test_gc_write (b) select c from test_gc_write`, 0},
		// Can uFIDelate without generated defCausumns.
		{`uFIDelate test_gc_write set b = 2 where a = 2`, 0},
		{`uFIDelate test_gc_write t1, test_gc_write_1 t2 set t1.b = 3, t2.b = 4`, 0},

		// But now we can't do this, just as same with MyALLEGROSQL 5.7:
		{`insert into test_gc_write values (1, 1)`, allegrosql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write select 1, 1`, allegrosql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write (c) select a, b from test_gc_write`, allegrosql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write (b, c) select a, b from test_gc_write`, allegrosql.ErrBadGeneratedDeferredCauset},
	}
	for _, tt := range tests {
		_, err := tk.Exec(tt.stmt)
		if tt.err != 0 {
			c.Assert(err, NotNil, Commentf("allegrosql is `%v`", tt.stmt))
			terr := errors.Cause(err).(*terror.Error)
			c.Assert(terr.Code(), Equals, errors.ErrCode(tt.err), Commentf("allegrosql is %v", tt.stmt))
		} else {
			c.Assert(err, IsNil)
		}
	}
}

// TestGeneratedDeferredCausetRead tests select generated defCausumns from block.
// They should be calculated from their generation expressions.
func (s *testSuiteP1) TestGeneratedDeferredCausetRead(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_gc_read(a int primary key, b int, c int as (a+b), d int as (a*b) stored, e int as (c*2))`)

	result := tk.MustQuery(`SELECT generation_expression FROM information_schema.defCausumns WHERE block_name = 'test_gc_read' AND defCausumn_name = 'd'`)
	result.Check(testkit.Rows("`a` * `b`"))

	// Insert only defCausumn a and b, leave c and d be calculated from them.
	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (0,null),(1,2),(3,4)`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`))

	tk.MustExec(`INSERT INTO test_gc_read SET a = 5, b = 10`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `5 10 15 50 30`))

	tk.MustExec(`REPLACE INTO test_gc_read (a, b) VALUES (5, 6)`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `5 6 11 30 22`))

	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (5, 8) ON DUPLICATE KEY UFIDelATE b = 9`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `5 9 14 45 28`))

	// Test select only-generated-defCausumn-without-dependences.
	result = tk.MustQuery(`SELECT c, d FROM test_gc_read`)
	result.Check(testkit.Rows(`<nil> <nil>`, `3 2`, `7 12`, `14 45`))

	// Test select only virtual generated defCausumn that refers to other virtual generated defCausumns.
	result = tk.MustQuery(`SELECT e FROM test_gc_read`)
	result.Check(testkit.Rows(`<nil>`, `6`, `14`, `28`))

	// Test order of on duplicate key uFIDelate list.
	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (5, 8) ON DUPLICATE KEY UFIDelATE a = 6, b = a`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `6 6 12 36 24`))

	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (6, 8) ON DUPLICATE KEY UFIDelATE b = 8, a = b`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	// Test where-conditions on virtual/stored generated defCausumns.
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 7`)
	result.Check(testkit.Rows(`3 4 7 12 14`))

	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE d = 64`)
	result.Check(testkit.Rows(`8 8 16 64 32`))

	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE e = 6`)
	result.Check(testkit.Rows(`1 2 3 2 6`))

	// Test uFIDelate where-conditions on virtual/generated defCausumns.
	tk.MustExec(`UFIDelATE test_gc_read SET a = a + 100 WHERE c = 7`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 107`)
	result.Check(testkit.Rows(`103 4 107 412 214`))

	// Test uFIDelate where-conditions on virtual/generated defCausumns.
	tk.MustExec(`UFIDelATE test_gc_read m SET m.a = m.a + 100 WHERE c = 107`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 207`)
	result.Check(testkit.Rows(`203 4 207 812 414`))

	tk.MustExec(`UFIDelATE test_gc_read SET a = a - 200 WHERE d = 812`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE d = 12`)
	result.Check(testkit.Rows(`3 4 7 12 14`))

	tk.MustExec(`INSERT INTO test_gc_read set a = 4, b = d + 1`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`, `3 4 7 12 14`,
		`4 <nil> <nil> <nil> <nil>`, `8 8 16 64 32`))
	tk.MustExec(`DELETE FROM test_gc_read where a = 4`)

	// Test on-conditions on virtual/stored generated defCausumns.
	tk.MustExec(`CREATE TABLE test_gc_help(a int primary key, b int, c int, d int, e int)`)
	tk.MustExec(`INSERT INTO test_gc_help(a, b, c, d, e) SELECT * FROM test_gc_read`)

	result = tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.c = t2.c ORDER BY t1.a`)
	result.Check(testkit.Rows(`1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	result = tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.d = t2.d ORDER BY t1.a`)
	result.Check(testkit.Rows(`1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	result = tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.e = t2.e ORDER BY t1.a`)
	result.Check(testkit.Rows(`1 2 3 2 6`, `3 4 7 12 14`, `8 8 16 64 32`))

	// Test generated defCausumn in subqueries.
	result = tk.MustQuery(`SELECT * FROM test_gc_read t WHERE t.a not in (SELECT t.a FROM test_gc_read t where t.c > 5)`)
	result.Sort().Check(testkit.Rows(`0 <nil> <nil> <nil> <nil>`, `1 2 3 2 6`))

	result = tk.MustQuery(`SELECT * FROM test_gc_read t WHERE t.c in (SELECT t.c FROM test_gc_read t where t.c > 5)`)
	result.Sort().Check(testkit.Rows(`3 4 7 12 14`, `8 8 16 64 32`))

	result = tk.MustQuery(`SELECT tt.b FROM test_gc_read tt WHERE tt.a = (SELECT max(t.a) FROM test_gc_read t WHERE t.c = tt.c) ORDER BY b`)
	result.Check(testkit.Rows(`2`, `4`, `8`))

	// Test aggregation on virtual/stored generated defCausumns.
	result = tk.MustQuery(`SELECT c, sum(a) aa, max(d) dd, sum(e) ee FROM test_gc_read GROUP BY c ORDER BY aa`)
	result.Check(testkit.Rows(`<nil> 0 <nil> <nil>`, `3 1 2 6`, `7 3 12 14`, `16 8 64 32`))

	result = tk.MustQuery(`SELECT a, sum(c), sum(d), sum(e) FROM test_gc_read GROUP BY a ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 3 2 6`, `3 7 12 14`, `8 16 64 32`))

	// Test multi-uFIDelate on generated defCausumns.
	tk.MustExec(`UFIDelATE test_gc_read m, test_gc_read n SET m.a = m.a + 10, n.a = n.a + 10`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`10 <nil> <nil> <nil> <nil>`, `11 2 13 22 26`, `13 4 17 52 34`, `18 8 26 144 52`))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(18)")
	tk.MustExec("uFIDelate test_gc_read set a = a+1 where a in (select a from t)")
	result = tk.MustQuery("select * from test_gc_read order by a")
	result.Check(testkit.Rows(`10 <nil> <nil> <nil> <nil>`, `11 2 13 22 26`, `13 4 17 52 34`, `19 8 27 152 54`))

	// Test different types between generation expression and generated defCausumn.
	tk.MustExec(`CREATE TABLE test_gc_read_cast(a VARCHAR(255), b VARCHAR(255), c INT AS (JSON_EXTRACT(a, b)), d INT AS (JSON_EXTRACT(a, b)) STORED)`)
	tk.MustExec(`INSERT INTO test_gc_read_cast (a, b) VALUES ('{"a": "3"}', '$.a')`)
	result = tk.MustQuery(`SELECT c, d FROM test_gc_read_cast`)
	result.Check(testkit.Rows(`3 3`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_1(a VARCHAR(255), b VARCHAR(255), c ENUM("red", "yellow") AS (JSON_UNQUOTE(JSON_EXTRACT(a, b))))`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_1 (a, b) VALUES ('{"a": "yellow"}', '$.a')`)
	result = tk.MustQuery(`SELECT c FROM test_gc_read_cast_1`)
	result.Check(testkit.Rows(`yellow`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_2( a JSON, b JSON AS (a->>'$.a'))`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_2(a) VALUES ('{"a": "{    \\\"key\\\": \\\"\\u6d4b\\\"    }"}')`)
	result = tk.MustQuery(`SELECT b FROM test_gc_read_cast_2`)
	result.Check(testkit.Rows(`{"key": "测"}`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_3( a JSON, b JSON AS (a->>'$.a'), c INT AS (b * 3.14) )`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_3(a) VALUES ('{"a": "5"}')`)
	result = tk.MustQuery(`SELECT c FROM test_gc_read_cast_3`)
	result.Check(testkit.Rows(`16`))

	_, err := tk.Exec(`INSERT INTO test_gc_read_cast_1 (a, b) VALUES ('{"a": "invalid"}', '$.a')`)
	c.Assert(err, NotNil)

	// Test read generated defCausumns after drop some irrelevant defCausumn
	tk.MustExec(`DROP TABLE IF EXISTS test_gc_read_m`)
	tk.MustExec(`CREATE TABLE test_gc_read_m (a int primary key, b int, c int as (a+1), d int as (c*2))`)
	tk.MustExec(`INSERT INTO test_gc_read_m(a) values (1), (2)`)
	tk.MustExec(`ALTER TABLE test_gc_read_m DROP b`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read_m`)
	result.Check(testkit.Rows(`1 2 4`, `2 3 6`))

	// Test not null generated defCausumns.
	tk.MustExec(`CREATE TABLE test_gc_read_1(a int primary key, b int, c int as (a+b) not null, d int as (a*b) stored)`)
	tk.MustExec(`CREATE TABLE test_gc_read_2(a int primary key, b int, c int as (a+b), d int as (a*b) stored not null)`)
	tests := []struct {
		stmt string
		err  int
	}{
		// Can't insert these records, because generated defCausumns are not null.
		{`insert into test_gc_read_1(a, b) values (1, null)`, allegrosql.ErrBadNull},
		{`insert into test_gc_read_2(a, b) values (1, null)`, allegrosql.ErrBadNull},
	}
	for _, tt := range tests {
		_, err := tk.Exec(tt.stmt)
		if tt.err != 0 {
			c.Assert(err, NotNil)
			terr := errors.Cause(err).(*terror.Error)
			c.Assert(terr.Code(), Equals, errors.ErrCode(tt.err))
		} else {
			c.Assert(err, IsNil)
		}
	}
}

// TestGeneratedDeferredCausetRead tests generated defCausumns using point get and batch point get
func (s *testSuiteP1) TestGeneratedDeferredCausetPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists tu")
	tk.MustExec("CREATE TABLE tu(a int, b int, c int GENERATED ALWAYS AS (a + b) VIRTUAL, d int as (a * b) stored, " +
		"e int GENERATED ALWAYS as (b * 2) VIRTUAL, PRIMARY KEY (a), UNIQUE KEY ukc (c), unique key ukd(d), key ke(e))")
	tk.MustExec("insert into tu(a, b) values(1, 2)")
	tk.MustExec("insert into tu(a, b) values(5, 6)")
	tk.MustQuery("select * from tu for uFIDelate").Check(testkit.Rows("1 2 3 2 4", "5 6 11 30 12"))
	tk.MustQuery("select * from tu where a = 1").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where a in (1, 2)").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where c in (1, 2, 3)").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where c = 3").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select d, e from tu where c = 3").Check(testkit.Rows("2 4"))
	tk.MustQuery("select * from tu where d in (1, 2, 3)").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select * from tu where d = 2").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustQuery("select c, d from tu where d = 2").Check(testkit.Rows("3 2"))
	tk.MustQuery("select d, e from tu where e = 4").Check(testkit.Rows("2 4"))
	tk.MustQuery("select * from tu where e = 4").Check(testkit.Rows("1 2 3 2 4"))
	tk.MustExec("uFIDelate tu set a = a + 1, b = b + 1 where c = 11")
	tk.MustQuery("select * from tu for uFIDelate").Check(testkit.Rows("1 2 3 2 4", "6 7 13 42 14"))
	tk.MustQuery("select * from tu where a = 6").Check(testkit.Rows("6 7 13 42 14"))
	tk.MustQuery("select * from tu where c in (5, 6, 13)").Check(testkit.Rows("6 7 13 42 14"))
	tk.MustQuery("select b, c, e, d from tu where c = 13").Check(testkit.Rows("7 13 14 42"))
	tk.MustQuery("select a, e, d from tu where c in (5, 6, 13)").Check(testkit.Rows("6 14 42"))
	tk.MustExec("drop block if exists tu")
}

func (s *testSuiteP2) TestToPBExpr(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a decimal(10,6), b decimal, index idx_b (b))")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values (1.1, 1.1)")
	tk.MustExec("insert t values (2.4, 2.4)")
	tk.MustExec("insert t values (3.3, 2.7)")
	result := tk.MustQuery("select * from t where a < 2.399999")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where a > 1.5")
	result.Check(testkit.Rows("2.400000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where a <= 1.1")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where b >= 3")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where not (b = 1)")
	result.Check(testkit.Rows("2.400000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where b&1 = a|1")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where b != 2 and b <=> 3")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where b in (3)")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where b not in (1, 2)")
	result.Check(testkit.Rows("3.300000 3"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a varchar(255), b int)")
	tk.MustExec("insert t values ('abc123', 1)")
	tk.MustExec("insert t values ('ab123', 2)")
	result = tk.MustQuery("select * from t where a like 'ab%'")
	result.Check(testkit.Rows("abc123 1", "ab123 2"))
	result = tk.MustQuery("select * from t where a like 'ab_12'")
	result.Check(nil)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int primary key)")
	tk.MustExec("insert t values (1)")
	tk.MustExec("insert t values (2)")
	result = tk.MustQuery("select * from t where not (a = 1)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select * from t where not(not (a = 1))")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t where not(a != 1 and a != 2)")
	result.Check(testkit.Rows("1", "2"))
}

func (s *testSuiteP2) TestCausetXAPI(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a decimal(10,6), b decimal, index idx_b (b))")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values (1.1, 1.1)")
	tk.MustExec("insert t values (2.2, 2.2)")
	tk.MustExec("insert t values (3.3, 2.7)")
	result := tk.MustQuery("select * from t where a > 1.5")
	result.Check(testkit.Rows("2.200000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where b > 1.5")
	result.Check(testkit.Rows("2.200000 2", "3.300000 3"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a time(3), b time, index idx_a (a))")
	tk.MustExec("insert t values ('11:11:11', '11:11:11')")
	tk.MustExec("insert t values ('11:11:12', '11:11:12')")
	tk.MustExec("insert t values ('11:11:13', '11:11:13')")
	result = tk.MustQuery("select * from t where a > '11:11:11.5'")
	result.Check(testkit.Rows("11:11:12.000 11:11:12", "11:11:13.000 11:11:13"))
	result = tk.MustQuery("select * from t where b > '11:11:11.5'")
	result.Check(testkit.Rows("11:11:12.000 11:11:12", "11:11:13.000 11:11:13"))
}

func (s *testSuiteP2) TestALLEGROSQLMode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a tinyint not null)")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	_, err := tk.Exec("insert t values ()")
	c.Check(err, NotNil)

	_, err = tk.Exec("insert t values ('1000')")
	c.Check(err, NotNil)

	tk.MustExec("create block if not exists tdouble (a double(3,2))")
	_, err = tk.Exec("insert tdouble values (10.23)")
	c.Check(err, NotNil)

	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values ()")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1364 Field 'a' doesn't have a default value"))
	_, err = tk.Exec("insert t values (null)")
	c.Check(err, NotNil)
	tk.MustExec("insert ignore t values (null)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 DeferredCauset 'a' cannot be null"))
	tk.MustExec("insert t select null")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 DeferredCauset 'a' cannot be null"))
	tk.MustExec("insert t values (1000)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("0", "0", "0", "127"))

	tk.MustExec("insert tdouble values (10.23)")
	tk.MustQuery("select * from tdouble").Check(testkit.Rows("9.99"))

	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	tk.MustExec("set @@global.sql_mode = ''")

	// Disable global variable cache, so load global stochastik variable take effect immediate.
	s.petri.GetGlobalVarsCache().Disable()
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustExec("use test")
	tk2.MustExec("drop block if exists t2")
	tk2.MustExec("create block t2 (a varchar(3))")
	tk2.MustExec("insert t2 values ('abcd')")
	tk2.MustQuery("select * from t2").Check(testkit.Rows("abc"))

	// stochastik1 is still in strict mode.
	_, err = tk.Exec("insert t2 values ('abcd')")
	c.Check(err, NotNil)
	// Restore original global strict mode.
	tk.MustExec("set @@global.sql_mode = 'STRICT_TRANS_TABLES'")
}

func (s *testSuiteP2) TestBlockDual(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	result := tk.MustQuery("Select 1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select 1 from dual")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select count(*) from dual")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select 1 from dual where 1")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key)")
	tk.MustQuery("select t1.* from t t1, t t2 where t1.a=t2.a and 1=0").Check(testkit.Rows())
}

func (s *testSuiteP2) TestBlockScan(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use information_schema")
	result := tk.MustQuery("select * from schemata")
	// There must be these blocks: information_schema, allegrosql, performance_schema and test.
	c.Assert(len(result.Rows()), GreaterEqual, 4)
	tk.MustExec("use test")
	tk.MustExec("create database mytest")
	rowStr1 := fmt.Sprintf("%s %s %s %s %v", "def", "allegrosql", "utf8mb4", "utf8mb4_bin", nil)
	rowStr2 := fmt.Sprintf("%s %s %s %s %v", "def", "mytest", "utf8mb4", "utf8mb4_bin", nil)
	tk.MustExec("use information_schema")
	result = tk.MustQuery("select * from schemata where schema_name = 'allegrosql'")
	result.Check(testkit.Rows(rowStr1))
	result = tk.MustQuery("select * from schemata where schema_name like 'my%'")
	result.Check(testkit.Rows(rowStr1, rowStr2))
	result = tk.MustQuery("select 1 from blocks limit 1")
	result.Check(testkit.Rows("1"))
}

func (s *testSuiteP2) TestAdapterStatement(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	se.GetStochastikVars().TxnCtx.SchemaReplicant = petri.GetPetri(se).SchemaReplicant()
	compiler := &executor.Compiler{Ctx: se}
	stmtNode, err := s.ParseOneStmt("select 1", "", "")
	c.Check(err, IsNil)
	stmt, err := compiler.Compile(context.TODO(), stmtNode)
	c.Check(err, IsNil)
	c.Check(stmt.OriginText(), Equals, "select 1")

	stmtNode, err = s.ParseOneStmt("create block test.t (a int)", "", "")
	c.Check(err, IsNil)
	stmt, err = compiler.Compile(context.TODO(), stmtNode)
	c.Check(err, IsNil)
	c.Check(stmt.OriginText(), Equals, "create block test.t (a int)")
}

func (s *testSuiteP2) TestIsPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use allegrosql")
	ctx := tk.Se.(stochastikctx.Context)
	tests := map[string]bool{
		"select * from help_topic where name='aaa'":         false,
		"select 1 from help_topic where name='aaa'":         false,
		"select * from help_topic where help_topic_id=1":    true,
		"select * from help_topic where help_category_id=1": false,
	}
	schemaReplicant := schemareplicant.GetSchemaReplicant(ctx)

	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		c.Check(err, IsNil)
		err = plannercore.Preprocess(ctx, stmtNode, schemaReplicant)
		c.Check(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, schemaReplicant)
		c.Check(err, IsNil)
		ret, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, result)
	}
}

func (s *testSuiteP2) TestClusteredIndexIsPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_cluster_index_is_point_get;")
	tk.MustExec("create database test_cluster_index_is_point_get;")
	tk.MustExec("use test_cluster_index_is_point_get;")

	tk.MustExec("set milevadb_enable_clustered_index=1;")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t (a varchar(255), b int, c char(10), primary key (c, a));")
	ctx := tk.Se.(stochastikctx.Context)

	tests := map[string]bool{
		"select 1 from t where a='x'":                   false,
		"select * from t where c='x'":                   false,
		"select * from t where a='x' and c='x'":         true,
		"select * from t where a='x' and c='x' and b=1": false,
	}
	schemaReplicant := schemareplicant.GetSchemaReplicant(ctx)
	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		c.Check(err, IsNil)
		err = plannercore.Preprocess(ctx, stmtNode, schemaReplicant)
		c.Check(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmtNode, schemaReplicant)
		c.Check(err, IsNil)
		ret, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, result)
	}
}

func (s *testSerialSuite) TestPointGetRepeablockRead(c *C) {
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("use test")
	tk1.MustExec(`create block point_get (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into point_get values (1, 1, 1)")
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustExec("use test")

	var (
		step1 = "github.com/whtcorpsinc/milevadb/executor/pointGetRepeablockReadTest-step1"
		step2 = "github.com/whtcorpsinc/milevadb/executor/pointGetRepeablockReadTest-step2"
	)

	c.Assert(failpoint.Enable(step1, "return"), IsNil)
	c.Assert(failpoint.Enable(step2, "pause"), IsNil)

	uFIDelateWaitCh := make(chan struct{})
	go func() {
		ctx := context.WithValue(context.Background(), "pointGetRepeablockReadTest", uFIDelateWaitCh)
		ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
			return fpname == step1 || fpname == step2
		})
		rs, err := tk1.Se.Execute(ctx, "select c from point_get where b = 1")
		c.Assert(err, IsNil)
		result := tk1.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute allegrosql fail"))
		result.Check(testkit.Rows("1"))
	}()

	<-uFIDelateWaitCh // Wait `POINT GET` first time `get`
	c.Assert(failpoint.Disable(step1), IsNil)
	tk2.MustExec("uFIDelate point_get set b = 2, c = 2 where a = 1")
	c.Assert(failpoint.Disable(step2), IsNil)
}

func (s *testSerialSuite) TestBatchPointGetRepeablockRead(c *C) {
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("use test")
	tk1.MustExec(`create block batch_point_get (a int, b int, c int, unique key k_b(a, b, c))`)
	tk1.MustExec("insert into batch_point_get values (1, 1, 1), (2, 3, 4), (3, 4, 5)")
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustExec("use test")

	var (
		step1 = "github.com/whtcorpsinc/milevadb/executor/batchPointGetRepeablockReadTest-step1"
		step2 = "github.com/whtcorpsinc/milevadb/executor/batchPointGetRepeablockReadTest-step2"
	)

	c.Assert(failpoint.Enable(step1, "return"), IsNil)
	c.Assert(failpoint.Enable(step2, "pause"), IsNil)

	uFIDelateWaitCh := make(chan struct{})
	go func() {
		ctx := context.WithValue(context.Background(), "batchPointGetRepeablockReadTest", uFIDelateWaitCh)
		ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
			return fpname == step1 || fpname == step2
		})
		rs, err := tk1.Se.Execute(ctx, "select c from batch_point_get where (a, b, c) in ((1, 1, 1))")
		c.Assert(err, IsNil)
		result := tk1.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute allegrosql fail"))
		result.Check(testkit.Rows("1"))
	}()

	<-uFIDelateWaitCh // Wait `POINT GET` first time `get`
	c.Assert(failpoint.Disable(step1), IsNil)
	tk2.MustExec("uFIDelate batch_point_get set b = 2, c = 2 where a = 1")
	c.Assert(failpoint.Disable(step2), IsNil)
}

func (s *testSerialSuite) TestSplitRegionTimeout(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/MockSplitRegionTimeout", `return(true)`), IsNil)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a varchar(100),b int, index idx1(b,a))")
	tk.MustExec(`split block t index idx1 by (10000,"abcd"),(10000000);`)
	tk.MustExec(`set @@milevadb_wait_split_region_timeout=1`)
	// result 0 0 means split 0 region and 0 region finish scatter regions before timeout.
	tk.MustQuery(`split block t between (0) and (10000) regions 10`).Check(testkit.Rows("0 0"))
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/MockSplitRegionTimeout"), IsNil)

	// Test scatter regions timeout.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/MockScatterRegionTimeout", `return(true)`), IsNil)
	tk.MustQuery(`split block t between (0) and (10000) regions 10`).Check(testkit.Rows("10 1"))
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/MockScatterRegionTimeout"), IsNil)

	// Test pre-split with timeout.
	tk.MustExec("drop block if exists t")
	tk.MustExec("set @@global.milevadb_scatter_region=1;")
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/MockScatterRegionTimeout", `return(true)`), IsNil)
	atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 1)
	start := time.Now()
	tk.MustExec("create block t (a int, b int) partition by hash(a) partitions 5;")
	c.Assert(time.Since(start).Seconds(), Less, 10.0)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/MockScatterRegionTimeout"), IsNil)
}

func (s *testSuiteP2) TestRow(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 3)")
	tk.MustExec("insert t values (2, 1)")
	tk.MustExec("insert t values (2, 3)")
	result := tk.MustQuery("select * from t where (c, d) < (2,2)")
	result.Check(testkit.Rows("1 1", "1 3", "2 1"))
	result = tk.MustQuery("select * from t where (1,2,3) > (3,2,1)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t where event(1,2,3) > (3,2,1)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t where (c, d) = (select * from t where (c,d) = (1,1))")
	result.Check(testkit.Rows("1 1"))
	result = tk.MustQuery("select * from t where (c, d) = (select * from t k where (t.c,t.d) = (c,d))")
	result.Check(testkit.Rows("1 1", "1 3", "2 1", "2 3"))
	result = tk.MustQuery("select (1, 2, 3) < (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 3, 3)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 1, 4)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (2, 3, 4) >= (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) = (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) != (2, 3, 4)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select event(1, 1) in (event(1, 1))")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select event(1, 0) in (event(1, 1))")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select event(1, 1) in (select 1, 1)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select event(1, 1) > event(1, 0)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select event(1, 1) > (select 1, 0)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select 1 > (select 1)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (select 1)")
	result.Check(testkit.Rows("1"))
}

func (s *testSuiteP2) TestDeferredCausetName(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c int, d int)")
	// disable only full group by
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'")
	rs, err := tk.Exec("select 1 + c, count(*) from t")
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 2)
	c.Check(fields[0].DeferredCauset.Name.L, Equals, "1 + c")
	c.Check(fields[0].DeferredCausetAsName.L, Equals, "1 + c")
	c.Check(fields[1].DeferredCauset.Name.L, Equals, "count(*)")
	c.Check(fields[1].DeferredCausetAsName.L, Equals, "count(*)")
	rs.Close()
	rs, err = tk.Exec("select (c) > all (select c from t) from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].DeferredCauset.Name.L, Equals, "(c) > all (select c from t)")
	c.Check(fields[0].DeferredCausetAsName.L, Equals, "(c) > all (select c from t)")
	rs.Close()
	tk.MustExec("begin")
	tk.MustExec("insert t values(1,1)")
	rs, err = tk.Exec("select c d, d c from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 2)
	c.Check(fields[0].DeferredCauset.Name.L, Equals, "c")
	c.Check(fields[0].DeferredCausetAsName.L, Equals, "d")
	c.Check(fields[1].DeferredCauset.Name.L, Equals, "d")
	c.Check(fields[1].DeferredCausetAsName.L, Equals, "c")
	rs.Close()
	// Test case for query a defCausumn of a block.
	// In this case, all attributes have values.
	rs, err = tk.Exec("select c as a from t as t2")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(fields[0].DeferredCauset.Name.L, Equals, "c")
	c.Check(fields[0].DeferredCausetAsName.L, Equals, "a")
	c.Check(fields[0].Block.Name.L, Equals, "t")
	c.Check(fields[0].BlockAsName.L, Equals, "t2")
	c.Check(fields[0].DBName.L, Equals, "test")
	rs.Close()
	// Test case for query a expression which only using constant inputs.
	// In this case, the block, org_block and database attributes will all be empty.
	rs, err = tk.Exec("select hour(1) as a from t as t2")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(fields[0].DeferredCauset.Name.L, Equals, "a")
	c.Check(fields[0].DeferredCausetAsName.L, Equals, "a")
	c.Check(fields[0].Block.Name.L, Equals, "")
	c.Check(fields[0].BlockAsName.L, Equals, "")
	c.Check(fields[0].DBName.L, Equals, "")
	rs.Close()
	// Test case for query a defCausumn wrapped with parentheses and unary plus.
	// In this case, the defCausumn name should be its original name.
	rs, err = tk.Exec("select (c), (+c), +(c), +(+(c)), ++c from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	for i := 0; i < 5; i++ {
		c.Check(fields[i].DeferredCauset.Name.L, Equals, "c")
		c.Check(fields[i].DeferredCausetAsName.L, Equals, "c")
	}
	rs.Close()

	// Test issue https://github.com/whtcorpsinc/milevadb/issues/9639 .
	// Both window function and expression appear in final result field.
	tk.MustExec("set @@milevadb_enable_window_function = 1")
	rs, err = tk.Exec("select 1+1, row_number() over() num from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Assert(fields[0].DeferredCauset.Name.L, Equals, "1+1")
	c.Assert(fields[0].DeferredCausetAsName.L, Equals, "1+1")
	c.Assert(fields[1].DeferredCauset.Name.L, Equals, "num")
	c.Assert(fields[1].DeferredCausetAsName.L, Equals, "num")
	tk.MustExec("set @@milevadb_enable_window_function = 0")
	rs.Close()

	rs, err = tk.Exec("select if(1,c,c) from t;")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Assert(fields[0].DeferredCauset.Name.L, Equals, "if(1,c,c)")
	// It's a compatibility issue. Should be empty instead.
	c.Assert(fields[0].DeferredCausetAsName.L, Equals, "if(1,c,c)")
}

func (s *testSuiteP2) TestSelectVar(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (d int)")
	tk.MustExec("insert into t values(1), (2), (1)")
	// This behavior is different from MyALLEGROSQL.
	result := tk.MustQuery("select @a, @a := d+1 from t")
	result.Check(testkit.Rows("<nil> 2", "2 3", "3 2"))
	// Test for PR #10658.
	tk.MustExec("select ALLEGROSQL_BIG_RESULT d from t group by d")
	tk.MustExec("select ALLEGROSQL_SMALL_RESULT d from t group by d")
	tk.MustExec("select ALLEGROSQL_BUFFER_RESULT d from t group by d")
}

func (s *testSuiteP2) TestHistoryRead(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists history_read")
	tk.MustExec("create block history_read (a int)")
	tk.MustExec("insert history_read values (1)")

	// For mockeinsteindb, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "einsteindb_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	uFIDelateSafePoint := fmt.Sprintf(`INSERT INTO allegrosql.milevadb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UFIDelATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(uFIDelateSafePoint)

	// Set snapshot to a time before save point will fail.
	_, err := tk.Exec("set @@milevadb_snapshot = '2006-01-01 15:04:05.999999'")
	c.Assert(terror.ErrorEqual(err, variable.ErrSnapshotTooOld), IsTrue, Commentf("err %v", err))
	// SnapshotTS Is not uFIDelated if check failed.
	c.Assert(tk.Se.GetStochastikVars().SnapshotTS, Equals, uint64(0))

	curVer1, _ := s.causetstore.CurrentVersion()
	time.Sleep(time.Millisecond)
	snapshotTime := time.Now()
	time.Sleep(time.Millisecond)
	curVer2, _ := s.causetstore.CurrentVersion()
	tk.MustExec("insert history_read values (2)")
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1", "2"))
	tk.MustExec("set @@milevadb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	ctx := tk.Se.(stochastikctx.Context)
	snapshotTS := ctx.GetStochastikVars().SnapshotTS
	c.Assert(snapshotTS, Greater, curVer1.Ver)
	c.Assert(snapshotTS, Less, curVer2.Ver)
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1"))
	_, err = tk.Exec("insert history_read values (2)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("uFIDelate history_read set a = 3 where a = 1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete from history_read where a = 1")
	c.Assert(err, NotNil)
	tk.MustExec("set @@milevadb_snapshot = ''")
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1", "2"))
	tk.MustExec("insert history_read values (3)")
	tk.MustExec("uFIDelate history_read set a = 4 where a = 3")
	tk.MustExec("delete from history_read where a = 1")

	time.Sleep(time.Millisecond)
	snapshotTime = time.Now()
	time.Sleep(time.Millisecond)
	tk.MustExec("alter block history_read add defCausumn b int")
	tk.MustExec("insert history_read values (8, 8), (9, 9)")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2 <nil>", "4 <nil>", "8 8", "9 9"))
	tk.MustExec("set @@milevadb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2", "4"))
	tsoStr := strconv.FormatUint(oracle.EncodeTSO(snapshotTime.UnixNano()/int64(time.Millisecond)), 10)

	tk.MustExec("set @@milevadb_snapshot = '" + tsoStr + "'")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2", "4"))

	tk.MustExec("set @@milevadb_snapshot = ''")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2 <nil>", "4 <nil>", "8 8", "9 9"))
}

func (s *testSuite2) TestLowResolutionTSORead(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("set @@autocommit=1")
	tk.MustExec("use test")
	tk.MustExec("drop block if exists low_resolution_tso")
	tk.MustExec("create block low_resolution_tso(a int)")
	tk.MustExec("insert low_resolution_tso values (1)")

	// enable low resolution tso
	c.Assert(tk.Se.GetStochastikVars().LowResolutionTSO, IsFalse)
	tk.Exec("set @@milevadb_low_resolution_tso = 'on'")
	c.Assert(tk.Se.GetStochastikVars().LowResolutionTSO, IsTrue)

	time.Sleep(3 * time.Second)
	tk.MustQuery("select * from low_resolution_tso").Check(testkit.Rows("1"))
	_, err := tk.Exec("uFIDelate low_resolution_tso set a = 2")
	c.Assert(err, NotNil)
	tk.MustExec("set @@milevadb_low_resolution_tso = 'off'")
	tk.MustExec("uFIDelate low_resolution_tso set a = 2")
	tk.MustQuery("select * from low_resolution_tso").Check(testkit.Rows("2"))
}

func (s *testSuite) TestScanControlSelection(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, c int, index idx_b(b))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	tk.MustQuery("select (select count(1) k from t s where s.b = t1.c) from t t1").Sort().Check(testkit.Rows("0", "1", "3", "3"))
}

func (s *testSuite) TestSimplePosetDag(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	tk.MustQuery("select a from t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery("select * from t where a = 4").Check(testkit.Rows("4 2 3"))
	tk.MustQuery("select a from t limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select a from t order by a desc").Check(testkit.Rows("4", "3", "2", "1"))
	tk.MustQuery("select a from t order by a desc limit 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t order by b desc limit 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t where a < 3").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t where b > 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t where b > 1 and a < 3").Check(testkit.Rows())
	tk.MustQuery("select count(*) from t where b > 1 and a < 3").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*), c from t group by c order by c").Check(testkit.Rows("2 1", "1 2", "1 3"))
	tk.MustQuery("select sum(c) as s from t group by b order by s").Check(testkit.Rows("3", "4"))
	tk.MustQuery("select avg(a) as s from t group by b order by s").Check(testkit.Rows("2.0000", "4.0000"))
	tk.MustQuery("select sum(distinct c) from t group by b").Check(testkit.Rows("3", "3"))

	tk.MustExec("create index i on t(c,b)")
	tk.MustQuery("select a from t where c = 1").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t where c = 1 and a < 2").Check(testkit.Rows("1"))
	tk.MustQuery("select a from t where c = 1 order by a limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t where c = 1 ").Check(testkit.Rows("2"))
	tk.MustExec("create index i1 on t(b)")
	tk.MustQuery("select c from t where b = 2").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where b = 2").Check(testkit.Rows("4 2 3"))
	tk.MustQuery("select count(*) from t where b = 1").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where b = 1 and a > 1 limit 1").Check(testkit.Rows("2 1 1"))

	// Test time push down.
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (id int, c1 datetime);")
	tk.MustExec("insert into t values (1, '2020-06-07 12:12:12')")
	tk.MustQuery("select id from t where c1 = '2020-06-07 12:12:12'").Check(testkit.Rows("1"))

	// Test issue 17816
	tk.MustExec("drop block if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT)")
	tk.MustExec("INSERT INTO t0 VALUES (100000)")
	tk.MustQuery("SELECT * FROM t0 WHERE NOT SPACE(t0.c0)").Check(testkit.Rows("100000"))
}

func (s *testSuite) TestTimestampTimeZone(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (ts timestamp)")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t values ('2020-04-27 22:40:42')")
	// The timestamp will get different value if time_zone stochastik variable changes.
	tests := []struct {
		timezone string
		expect   string
	}{
		{"+10:00", "2020-04-28 08:40:42"},
		{"-6:00", "2020-04-27 16:40:42"},
	}
	for _, tt := range tests {
		tk.MustExec(fmt.Sprintf("set time_zone = '%s'", tt.timezone))
		tk.MustQuery("select * from t").Check(testkit.Rows(tt.expect))
	}

	// For issue https://github.com/whtcorpsinc/milevadb/issues/3467
	tk.MustExec("drop block if exists t1")
	tk.MustExec(`CREATE TABLE t1 (
 	      id bigint(20) NOT NULL AUTO_INCREMENT,
 	      uid int(11) DEFAULT NULL,
 	      datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 	      ip varchar(128) DEFAULT NULL,
 	    PRIMARY KEY (id),
 	      KEY i_datetime (datetime),
 	      KEY i_userid (uid)
 	    );`)
	tk.MustExec(`INSERT INTO t1 VALUES (123381351,1734,"2020-03-31 08:57:10","127.0.0.1");`)
	r := tk.MustQuery("select datetime from t1;") // Cover BlockReaderExec
	r.Check(testkit.Rows("2020-03-31 08:57:10"))
	r = tk.MustQuery("select datetime from t1 where datetime='2020-03-31 08:57:10';")
	r.Check(testkit.Rows("2020-03-31 08:57:10")) // Cover IndexReaderExec
	r = tk.MustQuery("select * from t1 where datetime='2020-03-31 08:57:10';")
	r.Check(testkit.Rows("123381351 1734 2020-03-31 08:57:10 127.0.0.1")) // Cover IndexLookupExec

	// For issue https://github.com/whtcorpsinc/milevadb/issues/3485
	tk.MustExec("set time_zone = 'Asia/Shanghai'")
	tk.MustExec("drop block if exists t1")
	tk.MustExec(`CREATE TABLE t1 (
	    id bigint(20) NOT NULL AUTO_INCREMENT,
	    datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    PRIMARY KEY (id)
	  );`)
	tk.MustExec(`INSERT INTO t1 VALUES (123381351,"2020-03-31 08:57:10");`)
	r = tk.MustQuery(`select * from t1 where datetime="2020-03-31 08:57:10";`)
	r.Check(testkit.Rows("123381351 2020-03-31 08:57:10"))
	tk.MustExec(`alter block t1 add key i_datetime (datetime);`)
	r = tk.MustQuery(`select * from t1 where datetime="2020-03-31 08:57:10";`)
	r.Check(testkit.Rows("123381351 2020-03-31 08:57:10"))
	r = tk.MustQuery(`select * from t1;`)
	r.Check(testkit.Rows("123381351 2020-03-31 08:57:10"))
	r = tk.MustQuery("select datetime from t1 where datetime='2020-03-31 08:57:10';")
	r.Check(testkit.Rows("2020-03-31 08:57:10"))
}

func (s *testSuite) TestTimestamFIDelefaultValueTimeZone(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec(`create block t (a int, b timestamp default "2020-01-17 14:46:14")`)
	tk.MustExec("insert into t set a=1")
	r := tk.MustQuery(`show create block t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '2020-01-17 14:46:14'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t set a=2")
	r = tk.MustQuery(`show create block t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '2020-01-17 06:46:14'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 2020-01-17 06:46:14", "2 2020-01-17 06:46:14"))
	// Test the defCausumn's version is greater than DeferredCausetInfoVersion1.
	sctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(sctx).SchemaReplicant()
	c.Assert(is, NotNil)
	tb, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tb.DefCauss()[1].Version = perceptron.DeferredCausetInfoVersion1 + 1
	tk.MustExec("insert into t set a=3")
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 2020-01-17 06:46:14", "2 2020-01-17 06:46:14", "3 2020-01-17 06:46:14"))
	tk.MustExec("delete from t where a=3")
	// Change time zone back.
	tk.MustExec("set time_zone = '+08:00'")
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 2020-01-17 14:46:14", "2 2020-01-17 14:46:14"))
	tk.MustExec("set time_zone = '-08:00'")
	r = tk.MustQuery(`show create block t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '2020-01-16 22:46:14'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// test zero default value in multiple time zone.
	defer tk.MustExec(fmt.Sprintf("set @@sql_mode='%s'", tk.MustQuery("select @@sql_mode").Rows()[0][0]))
	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("drop block if exists t")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec(`create block t (a int, b timestamp default "0000-00-00 00")`)
	tk.MustExec("insert into t set a=1")
	r = tk.MustQuery(`show create block t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '0000-00-00 00:00:00'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t set a=2")
	r = tk.MustQuery(`show create block t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '0000-00-00 00:00:00'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("set time_zone = '-08:00'")
	tk.MustExec("insert into t set a=3")
	r = tk.MustQuery(`show create block t`)
	r.Check(testkit.Rows("t CREATE TABLE `t` (\n" + "  `a` int(11) DEFAULT NULL,\n" + "  `b` timestamp DEFAULT '0000-00-00 00:00:00'\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	r = tk.MustQuery(`select a,b from t order by a`)
	r.Check(testkit.Rows("1 0000-00-00 00:00:00", "2 0000-00-00 00:00:00", "3 0000-00-00 00:00:00"))

	// test add timestamp defCausumn default current_timestamp.
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`set time_zone = 'Asia/Shanghai'`)
	tk.MustExec(`create block t (a int)`)
	tk.MustExec(`insert into t set a=1`)
	tk.MustExec(`alter block t add defCausumn b timestamp not null default current_timestamp;`)
	timeIn8 := tk.MustQuery("select b from t").Rows()[0][0]
	tk.MustExec(`set time_zone = '+00:00'`)
	timeIn0 := tk.MustQuery("select b from t").Rows()[0][0]
	c.Assert(timeIn8 != timeIn0, IsTrue, Commentf("%v == %v", timeIn8, timeIn0))
	datumTimeIn8, err := expression.GetTimeValue(tk.Se, timeIn8, allegrosql.TypeTimestamp, 0)
	c.Assert(err, IsNil)
	tIn8To0 := datumTimeIn8.GetMysqlTime()
	timeZoneIn8, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	err = tIn8To0.ConvertTimeZone(timeZoneIn8, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(timeIn0 == tIn8To0.String(), IsTrue, Commentf("%v != %v", timeIn0, tIn8To0.String()))

	// test add index.
	tk.MustExec(`alter block t add index(b);`)
	tk.MustExec("admin check block t")
	tk.MustExec(`set time_zone = '+05:00'`)
	tk.MustExec("admin check block t")
}

func (s *testSuite) TestMilevaDBCurrentTS(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select @@milevadb_current_ts").Check(testkit.Rows("0"))
	tk.MustExec("begin")
	rows := tk.MustQuery("select @@milevadb_current_ts").Rows()
	tsStr := rows[0][0].(string)
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(tsStr, Equals, fmt.Sprintf("%d", txn.StartTS()))
	tk.MustExec("begin")
	rows = tk.MustQuery("select @@milevadb_current_ts").Rows()
	newTsStr := rows[0][0].(string)
	txn, err = tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(newTsStr, Equals, fmt.Sprintf("%d", txn.StartTS()))
	c.Assert(newTsStr, Not(Equals), tsStr)
	tk.MustExec("commit")
	tk.MustQuery("select @@milevadb_current_ts").Check(testkit.Rows("0"))

	_, err = tk.Exec("set @@milevadb_current_ts = '1'")
	c.Assert(terror.ErrorEqual(err, variable.ErrReadOnly), IsTrue, Commentf("err %v", err))
}

func (s *testSuite) TestMilevaDBLastTxnInfo(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int primary key)")
	tk.MustQuery("select json_extract(@@milevadb_last_txn_info, '$.start_ts'), json_extract(@@milevadb_last_txn_info, '$.commit_ts')").Check(testkit.Rows("0 0"))

	tk.MustExec("insert into t values (1)")
	rows1 := tk.MustQuery("select json_extract(@@milevadb_last_txn_info, '$.start_ts'), json_extract(@@milevadb_last_txn_info, '$.commit_ts')").Rows()
	c.Assert(rows1[0][0].(string), Greater, "0")
	c.Assert(rows1[0][0].(string), Less, rows1[0][1].(string))

	tk.MustExec("begin")
	tk.MustQuery("select a from t where a = 1").Check(testkit.Rows("1"))
	rows2 := tk.MustQuery("select json_extract(@@milevadb_last_txn_info, '$.start_ts'), json_extract(@@milevadb_last_txn_info, '$.commit_ts'), @@milevadb_current_ts").Rows()
	tk.MustExec("commit")
	rows3 := tk.MustQuery("select json_extract(@@milevadb_last_txn_info, '$.start_ts'), json_extract(@@milevadb_last_txn_info, '$.commit_ts')").Rows()
	c.Assert(rows2[0][0], Equals, rows1[0][0])
	c.Assert(rows2[0][1], Equals, rows1[0][1])
	c.Assert(rows3[0][0], Equals, rows1[0][0])
	c.Assert(rows3[0][1], Equals, rows1[0][1])
	c.Assert(rows2[0][1], Less, rows2[0][2])

	tk.MustExec("begin")
	tk.MustExec("uFIDelate t set a = a + 1 where a = 1")
	rows4 := tk.MustQuery("select json_extract(@@milevadb_last_txn_info, '$.start_ts'), json_extract(@@milevadb_last_txn_info, '$.commit_ts'), @@milevadb_current_ts").Rows()
	tk.MustExec("commit")
	rows5 := tk.MustQuery("select json_extract(@@milevadb_last_txn_info, '$.start_ts'), json_extract(@@milevadb_last_txn_info, '$.commit_ts')").Rows()
	c.Assert(rows4[0][0], Equals, rows1[0][0])
	c.Assert(rows4[0][1], Equals, rows1[0][1])
	c.Assert(rows4[0][2], Equals, rows5[0][0])
	c.Assert(rows4[0][1], Less, rows4[0][2])
	c.Assert(rows4[0][2], Less, rows5[0][1])

	tk.MustExec("begin")
	tk.MustExec("uFIDelate t set a = a + 1 where a = 2")
	tk.MustExec("rollback")
	rows6 := tk.MustQuery("select json_extract(@@milevadb_last_txn_info, '$.start_ts'), json_extract(@@milevadb_last_txn_info, '$.commit_ts')").Rows()
	c.Assert(rows6[0][0], Equals, rows5[0][0])
	c.Assert(rows6[0][1], Equals, rows5[0][1])

	tk.MustExec("begin optimistic")
	tk.MustExec("insert into t values (2)")
	_, err := tk.Exec("commit")
	c.Assert(err, NotNil)
	rows7 := tk.MustQuery("select json_extract(@@milevadb_last_txn_info, '$.start_ts'), json_extract(@@milevadb_last_txn_info, '$.commit_ts'), json_extract(@@milevadb_last_txn_info, '$.error')").Rows()
	c.Assert(rows7[0][0], Greater, rows5[0][0])
	c.Assert(rows7[0][1], Equals, "0")
	c.Assert(strings.Contains(err.Error(), rows7[0][1].(string)), IsTrue)

	_, err = tk.Exec("set @@milevadb_last_txn_info = '{}'")
	c.Assert(terror.ErrorEqual(err, variable.ErrReadOnly), IsTrue, Commentf("err %v", err))
}

func (s *testSuite) TestSelectForUFIDelate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustExec("use test")

	tk.MustExec("drop block if exists t, t1")

	txn, err := tk.Se.Txn(true)
	c.Assert(ekv.ErrInvalidTxn.Equal(err), IsTrue)
	c.Assert(txn.Valid(), IsFalse)
	tk.MustExec("create block t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert t values (11, 2, 3)")
	tk.MustExec("insert t values (12, 2, 3)")
	tk.MustExec("insert t values (13, 2, 3)")

	tk.MustExec("create block t1 (c1 int)")
	tk.MustExec("insert t1 values (11)")

	// conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where c1=11 for uFIDelate")

	tk2.MustExec("begin")
	tk2.MustExec("uFIDelate t set c2=211 where c1=11")
	tk2.MustExec("commit")

	_, err = tk1.Exec("commit")
	c.Assert(err, NotNil)

	// no conflict for subquery.
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where exists(select null from t1 where t1.c1=t.c1) for uFIDelate")

	tk2.MustExec("begin")
	tk2.MustExec("uFIDelate t set c2=211 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// not conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where c1=11 for uFIDelate")

	tk2.MustExec("begin")
	tk2.MustExec("uFIDelate t set c2=22 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// not conflict, auto commit
	tk1.MustExec("set @@autocommit=1;")
	tk1.MustQuery("select * from t where c1=11 for uFIDelate")

	tk2.MustExec("begin")
	tk2.MustExec("uFIDelate t set c2=211 where c1=11")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from (select * from t for uFIDelate) t join t1 for uFIDelate")

	tk2.MustExec("begin")
	tk2.MustExec("uFIDelate t1 set c1 = 13")
	tk2.MustExec("commit")

	_, err = tk1.Exec("commit")
	c.Assert(err, NotNil)

}

func (s *testSuite) TestEmptyEnum(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (e enum('Y', 'N'))")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'")
	_, err := tk.Exec("insert into t values (0)")
	c.Assert(terror.ErrorEqual(err, types.ErrTruncated), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec("insert into t values ('abc')")
	c.Assert(terror.ErrorEqual(err, types.ErrTruncated), IsTrue, Commentf("err %v", err))

	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert into t values (0)")
	tk.MustQuery("select * from t").Check(testkit.Rows(""))
	tk.MustExec("insert into t values ('abc')")
	tk.MustQuery("select * from t").Check(testkit.Rows("", ""))
	tk.MustExec("insert into t values (null)")
	tk.MustQuery("select * from t").Check(testkit.Rows("", "", "<nil>"))
}

// TestIssue4024 This tests https://github.com/whtcorpsinc/milevadb/issues/4024
func (s *testSuite) TestIssue4024(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("use test")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("uFIDelate t, test2.t set test2.t.a=2")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustQuery("select * from test2.t").Check(testkit.Rows("2"))
	tk.MustExec("uFIDelate test.t, test2.t set test.t.a=3")
	tk.MustQuery("select * from t").Check(testkit.Rows("3"))
	tk.MustQuery("select * from test2.t").Check(testkit.Rows("2"))
}

const (
	checkRequestOff = iota
	checkRequestSyncLog
	checkDBSAddIndexPriority
)

type checkRequestClient struct {
	einsteindb.Client
	priority       pb.CommandPri
	lowPriorityCnt uint32
	mu             struct {
		sync.RWMutex
		checkFlags uint32
		syncLog    bool
	}
}

func (c *checkRequestClient) setCheckPriority(priority pb.CommandPri) {
	atomic.StoreInt32((*int32)(&c.priority), int32(priority))
}

func (c *checkRequestClient) getCheckPriority() pb.CommandPri {
	return (pb.CommandPri)(atomic.LoadInt32((*int32)(&c.priority)))
}

func (c *checkRequestClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	c.mu.RLock()
	checkFlags := c.mu.checkFlags
	c.mu.RUnlock()
	if checkFlags == checkRequestSyncLog {
		switch req.Type {
		case einsteindbrpc.CmdPrewrite, einsteindbrpc.CmdCommit:
			c.mu.RLock()
			syncLog := c.mu.syncLog
			c.mu.RUnlock()
			if syncLog != req.SyncLog {
				return nil, errors.New("fail to set sync log")
			}
		}
	} else if checkFlags == checkDBSAddIndexPriority {
		if req.Type == einsteindbrpc.CmdScan {
			if c.getCheckPriority() != req.Priority {
				return nil, errors.New("fail to set priority")
			}
		} else if req.Type == einsteindbrpc.CmdPrewrite {
			if c.getCheckPriority() == pb.CommandPri_Low {
				atomic.AddUint32(&c.lowPriorityCnt, 1)
			}
		}
	}
	return resp, err
}

type testSuiteWithCliBase struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	cli         *checkRequestClient
}

type testSuite1 struct {
	testSuiteWithCliBase
}

func (s *testSuiteWithCliBase) SetUpSuite(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c einsteindb.Client) einsteindb.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.causetstore, err = mockstore.NewMockStore(
		mockstore.WithClientHijacker(hijackClient),
	)
	c.Assert(err, IsNil)
	stochastik.SetStatsLease(0)
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	s.dom.SetStatsUFIDelating(true)
}

func (s *testSuiteWithCliBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *testSuiteWithCliBase) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		tk.MustExec(fmt.Sprintf("drop block %v", blockName))
	}
}

func (s *testSuite2) TestAddIndexPriority(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c einsteindb.Client) einsteindb.Client {
		cli.Client = c
		return cli
	}

	causetstore, err := mockstore.NewMockStore(
		mockstore.WithClientHijacker(hijackClient),
	)
	c.Assert(err, IsNil)
	dom, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t1 (id int, v int)")

	// Insert some data to make sure plan build IndexLookup for t1.
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
	}

	cli.mu.Lock()
	cli.mu.checkFlags = checkDBSAddIndexPriority
	cli.mu.Unlock()

	cli.setCheckPriority(pb.CommandPri_Low)
	tk.MustExec("alter block t1 add index t1_index (id);")

	c.Assert(atomic.LoadUint32(&cli.lowPriorityCnt) > 0, IsTrue)

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()

	tk.MustExec("alter block t1 drop index t1_index;")
	tk.MustExec("SET STOCHASTIK milevadb_dbs_reorg_priority = 'PRIORITY_NORMAL'")

	cli.mu.Lock()
	cli.mu.checkFlags = checkDBSAddIndexPriority
	cli.mu.Unlock()

	cli.setCheckPriority(pb.CommandPri_Normal)
	tk.MustExec("alter block t1 add index t1_index (id);")

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()

	tk.MustExec("alter block t1 drop index t1_index;")
	tk.MustExec("SET STOCHASTIKTIK milevadb_dbs_reorg_priority = 'PRIORITY_HIGH'")

	cli.mu.Lock()
	cli.mu.checkFlags = checkDBSAddIndexPriority
	cli.mu.Unlock()

	cli.setCheckPriority(pb.CommandPri_High)
	tk.MustExec("alter block t1 add index t1_index (id);")

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()
}

func (s *testSuite1) TestAlterBlockComment(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t_1")
	tk.MustExec("create block t_1 (c1 int, c2 int, c3 int default 1, index (c1)) comment = 'test block';")
	tk.MustExec("alter block `t_1` comment 'this is block comment';")
	result := tk.MustQuery("select block_comment from information_schema.blocks where block_name = 't_1';")
	result.Check(testkit.Rows("this is block comment"))
	tk.MustExec("alter block `t_1` comment 'block t comment';")
	result = tk.MustQuery("select block_comment from information_schema.blocks where block_name = 't_1';")
	result.Check(testkit.Rows("block t comment"))
}

func (s *testSuite) TestTimezonePushDown(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t (ts timestamp)")
	defer tk.MustExec("drop block t")
	tk.MustExec(`insert into t values ("2020-09-13 10:02:06")`)

	systemTZ := timeutil.SystemLocation()
	c.Assert(systemTZ.String(), Not(Equals), "System")
	c.Assert(systemTZ.String(), Not(Equals), "Local")
	ctx := context.Background()
	count := 0
	ctx1 := context.WithValue(ctx, "CheckSelectRequestHook", func(req *ekv.Request) {
		count += 1
		posetPosetDagReq := new(fidelpb.PosetDagRequest)
		err := proto.Unmarshal(req.Data, posetPosetDagReq)
		c.Assert(err, IsNil)
		c.Assert(posetPosetDagReq.GetTimeZoneName(), Equals, systemTZ.String())
	})
	tk.Se.Execute(ctx1, `select * from t where ts = "2020-09-13 10:02:06"`)

	tk.MustExec(`set time_zone="System"`)
	tk.Se.Execute(ctx1, `select * from t where ts = "2020-09-13 10:02:06"`)

	c.Assert(count, Equals, 2) // Make sure the hook function is called.
}

func (s *testSuite) TestNotFillCacheFlag(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t (id int primary key)")
	defer tk.MustExec("drop block t")
	tk.MustExec("insert into t values (1)")

	tests := []struct {
		allegrosql string
		expect     bool
	}{
		{"select ALLEGROSQL_NO_CACHE * from t", true},
		{"select ALLEGROSQL_CACHE * from t", false},
		{"select * from t", false},
	}
	count := 0
	ctx := context.Background()
	for _, test := range tests {
		ctx1 := context.WithValue(ctx, "CheckSelectRequestHook", func(req *ekv.Request) {
			count++
			if req.NotFillCache != test.expect {
				c.Errorf("allegrosql=%s, expect=%v, get=%v", test.allegrosql, test.expect, req.NotFillCache)
			}
		})
		rs, err := tk.Se.Execute(ctx1, test.allegrosql)
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs[0], Commentf("allegrosql: %v", test.allegrosql))
	}
	c.Assert(count, Equals, len(tests)) // Make sure the hook function is called.
}

func (s *testSuite1) TestSyncLog(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	cli := s.cli
	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestSyncLog
	cli.mu.syncLog = true
	cli.mu.Unlock()
	tk.MustExec("create block t (id int primary key)")
	cli.mu.Lock()
	cli.mu.syncLog = false
	cli.mu.Unlock()
	tk.MustExec("insert into t values (1)")

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()
}

func (s *testSuite) TestHandleTransfer(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a int, index idx(a))")
	tk.MustExec("insert into t values(1), (2), (4)")
	tk.MustExec("begin")
	tk.MustExec("uFIDelate t set a = 3 where a = 4")
	// test block scan read whose result need handle.
	tk.MustQuery("select * from t ignore index(idx)").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("insert into t values(4)")
	// test single read whose result need handle
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery("select * from t use index(idx) order by a desc").Check(testkit.Rows("4", "3", "2", "1"))
	tk.MustExec("uFIDelate t set a = 5 where a = 3")
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1", "2", "4", "5"))
	tk.MustExec("commit")

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, index idx(a))")
	tk.MustExec("insert into t values(3, 3), (1, 1), (2, 2)")
	// Second test double read.
	tk.MustQuery("select * from t use index(idx) order by a").Check(testkit.Rows("1 1", "2 2", "3 3"))
}

func (s *testSuite) TestBit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c1 bit(2))")
	tk.MustExec("insert into t values (0), (1), (2), (3)")
	_, err := tk.Exec("insert into t values (4)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values ('a')")
	c.Assert(err, NotNil)
	r, err := tk.Exec("select * from t where c1 = 2")
	c.Assert(err, IsNil)
	req := r.NewChunk()
	err = r.Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(types.BinaryLiteral(req.GetRow(0).GetBytes(0)), DeepEquals, types.NewBinaryLiteralFromUint(2, -1))
	r.Close()

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c1 bit(31))")
	tk.MustExec("insert into t values (0x7fffffff)")
	_, err = tk.Exec("insert into t values (0x80000000)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values (0xffffffff)")
	c.Assert(err, NotNil)
	tk.MustExec("insert into t values ('123')")
	tk.MustExec("insert into t values ('1234')")
	_, err = tk.Exec("insert into t values ('12345)")
	c.Assert(err, NotNil)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c1 bit(62))")
	tk.MustExec("insert into t values ('12345678')")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c1 bit(61))")
	_, err = tk.Exec("insert into t values ('12345678')")
	c.Assert(err, NotNil)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c1 bit(32))")
	tk.MustExec("insert into t values (0x7fffffff)")
	tk.MustExec("insert into t values (0xffffffff)")
	_, err = tk.Exec("insert into t values (0x1ffffffff)")
	c.Assert(err, NotNil)
	tk.MustExec("insert into t values ('1234')")
	_, err = tk.Exec("insert into t values ('12345')")
	c.Assert(err, NotNil)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c1 bit(64))")
	tk.MustExec("insert into t values (0xffffffffffffffff)")
	tk.MustExec("insert into t values ('12345678')")
	_, err = tk.Exec("insert into t values ('123456789')")
	c.Assert(err, NotNil)
}

func (s *testSuite) TestEnum(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c enum('a', 'b', 'c'))")
	tk.MustExec("insert into t values ('a'), (2), ('c')")
	tk.MustQuery("select * from t where c = 'a'").Check(testkit.Rows("a"))

	tk.MustQuery("select c + 1 from t where c = 2").Check(testkit.Rows("3"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (null), ('1')")
	tk.MustQuery("select c + 1 from t where c = 1").Check(testkit.Rows("2"))
}

func (s *testSuite) TestSet(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c set('a', 'b', 'c'))")
	tk.MustExec("insert into t values ('a'), (2), ('c'), ('a,b'), ('b,a')")
	tk.MustQuery("select * from t where c = 'a'").Check(testkit.Rows("a"))

	tk.MustQuery("select * from t where c = 'a,b'").Check(testkit.Rows("a,b", "a,b"))

	tk.MustQuery("select c + 1 from t where c = 2").Check(testkit.Rows("3"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (null), ('1')")
	tk.MustQuery("select c + 1 from t where c = 1").Check(testkit.Rows("2"))
}

func (s *testSuite) TestSubqueryInValues(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (id int, name varchar(20))")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (gid int)")

	tk.MustExec("insert into t1 (gid) value (1)")
	tk.MustExec("insert into t (id, name) value ((select gid from t1) ,'asd')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 asd"))
}

func (s *testSuite) TestEnhancedRangeAccess(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int primary key, b int)")
	tk.MustExec("insert into t values(1, 2), (2, 1)")
	tk.MustQuery("select * from t where (a = 1 and b = 2) or (a = 2 and b = 1)").Check(testkit.Rows("1 2", "2 1"))
	tk.MustQuery("select * from t where (a = 1 and b = 1) or (a = 2 and b = 2)").Check(nil)
}

// TestMaxInt64Handle Issue #4810
func (s *testSuite) TestMaxInt64Handle(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(id bigint, PRIMARY KEY (id))")
	tk.MustExec("insert into t values(9223372036854775807)")
	tk.MustExec("select * from t where id = 9223372036854775807")
	tk.MustQuery("select * from t where id = 9223372036854775807;").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("select * from t").Check(testkit.Rows("9223372036854775807"))
	_, err := tk.Exec("insert into t values(9223372036854775807)")
	c.Assert(err, NotNil)
	tk.MustExec("delete from t where id = 9223372036854775807")
	tk.MustQuery("select * from t").Check(nil)
}

func (s *testSuite) TestBlockScanWithPointRanges(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(id int, PRIMARY KEY (id))")
	tk.MustExec("insert into t values(1), (5), (10)")
	tk.MustQuery("select * from t where id in(1, 2, 10)").Check(testkit.Rows("1", "10"))
}

func (s *testSuite) TestUnsignedPk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(id bigint unsigned primary key)")
	var num1, num2 uint64 = math.MaxInt64 + 1, math.MaxInt64 + 2
	tk.MustExec(fmt.Sprintf("insert into t values(%v), (%v), (1), (2)", num1, num2))
	num1Str := strconv.FormatUint(num1, 10)
	num2Str := strconv.FormatUint(num2, 10)
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1", "2", num1Str, num2Str))
	tk.MustQuery("select * from t where id not in (2)").Check(testkit.Rows(num1Str, num2Str, "1"))
	tk.MustExec("drop block t")
	tk.MustExec("create block t(a bigint unsigned primary key, b int, index idx(b))")
	tk.MustExec("insert into t values(9223372036854775808, 1), (1, 1)")
	tk.MustQuery("select * from t use index(idx) where b = 1 and a < 2").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t use index(idx) where b = 1 order by b, a").Check(testkit.Rows("1 1", "9223372036854775808 1"))
}

func (s *testSuite) TestIssue5666(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("set @@profiling=1")
	tk.MustQuery("SELECT QUERY_ID, SUM(DURATION) AS SUM_DURATION FROM INFORMATION_SCHEMA.PROFILING GROUP BY QUERY_ID;").Check(testkit.Rows("0 0"))
}

func (s *testSuite) TestIssue5341(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop block if exists test.t")
	tk.MustExec("create block test.t(a char)")
	tk.MustExec("insert into test.t value('a')")
	tk.MustQuery("select * from test.t where a < 1 order by a limit 0;").Check(testkit.Rows())
}

func (s *testSuite) TestContainDotDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists test.t1")
	tk.MustExec("create block test.t1(t1.a char)")
	tk.MustExec("drop block if exists t2")
	tk.MustExec("create block t2(a char, t2.b int)")

	tk.MustExec("drop block if exists t3")
	_, err := tk.Exec("create block t3(s.a char);")
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrWrongBlockName))
}

func (s *testSuite) TestChecHoTTex(c *C) {
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()

	_, err = se.Execute(context.Background(), "create database test_admin")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_admin")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "create block t (pk int primary key, c int default 1, c1 int default 1, unique key c(c))")
	c.Assert(err, IsNil)
	is := s.petri.SchemaReplicant()
	EDB := perceptron.NewCIStr("test_admin")
	dbInfo, ok := is.SchemaByName(EDB)
	c.Assert(ok, IsTrue)
	tblName := perceptron.NewCIStr("t")
	tbl, err := is.BlockByName(EDB, tblName)
	c.Assert(err, IsNil)
	tbInfo := tbl.Meta()

	alloc := autoid.NewSlabPredictor(s.causetstore, dbInfo.ID, false, autoid.RowIDAllocType)
	tb, err := blocks.BlockFromMeta(autoid.NewSlabPredictors(alloc), tbInfo)
	c.Assert(err, IsNil)

	_, err = se.Execute(context.Background(), "admin check index t c")
	c.Assert(err, IsNil)

	_, err = se.Execute(context.Background(), "admin check index t C")
	c.Assert(err, IsNil)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20)
	// block     data (handle, data): (1, 10), (2, 20)
	recordVal1 := types.MakeCausets(int64(1), int64(10), int64(11))
	recordVal2 := types.MakeCausets(int64(2), int64(20), int64(21))
	c.Assert(s.ctx.NewTxn(context.Background()), IsNil)
	_, err = tb.AddRecord(s.ctx, recordVal1)
	c.Assert(err, IsNil)
	_, err = tb.AddRecord(s.ctx, recordVal2)
	c.Assert(err, IsNil)
	txn, err := s.ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)

	mockCtx := mock.NewContext()
	idx := tb.Indices()[0]
	sc := &stmtctx.StatementContext{TimeZone: time.Local}

	_, err = se.Execute(context.Background(), "admin check index t idx_inexistent")
	c.Assert(strings.Contains(err.Error(), "not exist"), IsTrue)

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30)
	// block     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	_, err = idx.Create(mockCtx, txn.GetUnionStore(), types.MakeCausets(int64(30)), ekv.IntHandle(3))
	c.Assert(err, IsNil)
	key := blockcodec.EncodeRowKey(tb.Meta().ID, ekv.IntHandle(4).Encoded())
	setDefCausValue(c, txn, key, types.NewCauset(int64(40)))
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "admin check index t c")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "handle 3, index:types.Causet{k:0x1, decimal:0x0, length:0x0, i:30, defCauslation:\"\", b:[]uint8(nil), x:interface {}(nil)} != record:<nil>")

	// set data to:
	// index     data (handle, data): (1, 10), (2, 20), (3, 30), (4, 40)
	// block     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	_, err = idx.Create(mockCtx, txn.GetUnionStore(), types.MakeCausets(int64(40)), ekv.IntHandle(4))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "admin check index t c")
	c.Assert(strings.Contains(err.Error(), "block count 3 != index(c) count 4"), IsTrue)

	// set data to:
	// index     data (handle, data): (1, 10), (4, 40)
	// block     data (handle, data): (1, 10), (2, 20), (4, 40)
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = idx.Delete(sc, txn, types.MakeCausets(int64(30)), ekv.IntHandle(3))
	c.Assert(err, IsNil)
	err = idx.Delete(sc, txn, types.MakeCausets(int64(20)), ekv.IntHandle(2))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "admin check index t c")
	c.Assert(strings.Contains(err.Error(), "block count 3 != index(c) count 2"), IsTrue)

	// TODO: pass the case below：
	// set data to:
	// index     data (handle, data): (1, 10), (4, 40), (2, 30)
	// block     data (handle, data): (1, 10), (2, 20), (4, 40)
}

func setDefCausValue(c *C, txn ekv.Transaction, key ekv.Key, v types.Causet) {
	event := []types.Causet{v, {}}
	defCausIDs := []int64{2, 3}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	rd := rowcodec.Encoder{Enable: true}
	value, err := blockcodec.EncodeRow(sc, event, defCausIDs, nil, nil, &rd)
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
}

func (s *testSuite) TestCheckBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	// Test 'admin check block' when the block has a unique index with null values.
	tk.MustExec("use test")
	tk.MustExec("drop block if exists admin_test;")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2));")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (NULL, NULL);")
	tk.MustExec("admin check block admin_test;")
}

func (s *testSuite) TestCheckBlockClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test;")
	tk.MustExec("set @@milevadb_enable_clustered_index = 1;")
	tk.MustExec("drop block if exists admin_test;")
	tk.MustExec("create block admin_test (c1 int, c2 int, c3 int default 1, primary key (c1, c2), index (c1), unique key(c2));")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("admin check block admin_test;")
}

func (s *testSuite) Testinterlocking_directorateStreamingFlag(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("create block t (id int, value int, index idx(id))")
	// Add some data to make statistics work.
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	tests := []struct {
		allegrosql string
		expect     bool
	}{
		{"select * from t", true},                         // BlockReader
		{"select * from t where id = 5", true},            // IndexLookup
		{"select * from t where id > 5", true},            // Filter
		{"select * from t limit 3", false},                // Limit
		{"select avg(id) from t", false},                  // Aggregate
		{"select * from t order by value limit 3", false}, // TopN
	}

	ctx := context.Background()
	for _, test := range tests {
		ctx1 := context.WithValue(ctx, "CheckSelectRequestHook", func(req *ekv.Request) {
			if req.Streaming != test.expect {
				c.Errorf("allegrosql=%s, expect=%v, get=%v", test.allegrosql, test.expect, req.Streaming)
			}
		})
		rs, err := tk.Se.Execute(ctx1, test.allegrosql)
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs[0], Commentf("allegrosql: %v", test.allegrosql))
	}
}

func (s *testSuite) TestIncorrectLimitArg(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec(`use test;`)
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a bigint);`)
	tk.MustExec(`prepare stmt1 from 'select * from t limit ?';`)
	tk.MustExec(`prepare stmt2 from 'select * from t limit ?, ?';`)
	tk.MustExec(`set @a = -1;`)
	tk.MustExec(`set @b =  1;`)

	var err error
	_, err = tk.Se.Execute(context.TODO(), `execute stmt1 using @a;`)
	c.Assert(err.Error(), Equals, `[planner:1210]Incorrect arguments to LIMIT`)

	_, err = tk.Se.Execute(context.TODO(), `execute stmt2 using @b, @a;`)
	c.Assert(err.Error(), Equals, `[planner:1210]Incorrect arguments to LIMIT`)
}

func (s *testSuite) TestLimit(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a bigint, b bigint);`)
	tk.MustExec(`insert into t values(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6);`)
	tk.MustQuery(`select * from t order by a limit 1, 1;`).Check(testkit.Rows(
		"2 2",
	))
	tk.MustQuery(`select * from t order by a limit 1, 2;`).Check(testkit.Rows(
		"2 2",
		"3 3",
	))
	tk.MustQuery(`select * from t order by a limit 1, 3;`).Check(testkit.Rows(
		"2 2",
		"3 3",
		"4 4",
	))
	tk.MustQuery(`select * from t order by a limit 1, 4;`).Check(testkit.Rows(
		"2 2",
		"3 3",
		"4 4",
		"5 5",
	))
	tk.MustExec(`set @@milevadb_init_chunk_size=2;`)
	tk.MustQuery(`select * from t order by a limit 2, 1;`).Check(testkit.Rows(
		"3 3",
	))
	tk.MustQuery(`select * from t order by a limit 2, 2;`).Check(testkit.Rows(
		"3 3",
		"4 4",
	))
	tk.MustQuery(`select * from t order by a limit 2, 3;`).Check(testkit.Rows(
		"3 3",
		"4 4",
		"5 5",
	))
	tk.MustQuery(`select * from t order by a limit 2, 4;`).Check(testkit.Rows(
		"3 3",
		"4 4",
		"5 5",
		"6 6",
	))
}

func (s *testSuite) Testinterlocking_directorateStreamingWarning(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a double)")
	tk.MustExec("insert into t value(1.2)")
	tk.MustExec("set @@stochastik.milevadb_enable_streaming = 1")

	result := tk.MustQuery("select * from t where a/0 > 1")
	result.Check(testkit.Rows())
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|1365|Division by 0"))
}

func (s *testSuite3) TestYearTypeDeleteIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a YEAR, PRIMARY KEY(a));")
	tk.MustExec("insert into t set a = '2151';")
	tk.MustExec("delete from t;")
	tk.MustExec("admin check block t")
}

func (s *testSuite3) TestForSelectSINTERLOCKeInUnion(c *C) {
	// A union B for uFIDelate, the "for uFIDelate" option belongs to union statement, so
	// it should works on both A and B.
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("use test")
	tk1.MustExec("drop block if exists t")
	tk1.MustExec("create block t(a int)")
	tk1.MustExec("insert into t values (1)")

	tk1.MustExec("begin")
	// 'For uFIDelate' would act on the second select.
	tk1.MustQuery("select 1 as a union select a from t for uFIDelate")

	tk2.MustExec("use test")
	tk2.MustExec("uFIDelate t set a = a + 1")

	// As tk1 use select 'for uFIDelate', it should detect conflict and fail.
	_, err := tk1.Exec("commit")
	c.Assert(err, NotNil)

	tk1.MustExec("begin")
	// 'For uFIDelate' would be ignored if 'order by' or 'limit' exists.
	tk1.MustQuery("select 1 as a union select a from t limit 5 for uFIDelate")
	tk1.MustQuery("select 1 as a union select a from t order by a for uFIDelate")

	tk2.MustExec("uFIDelate t set a = a + 1")

	_, err = tk1.Exec("commit")
	c.Assert(err, IsNil)
}

func (s *testSuite3) TestUnsignedDecimalOverflow(c *C) {
	tests := []struct {
		input  interface{}
		hasErr bool
		err    string
	}{{
		-1,
		true,
		"Out of range value for defCausumn",
	}, {
		"-1.1e-1",
		true,
		"Out of range value for defCausumn",
	}, {
		-1.1,
		true,
		"Out of range value for defCausumn",
	}, {
		-0,
		false,
		"",
	},
	}
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a decimal(10,2) unsigned)")
	for _, t := range tests {
		res, err := tk.Exec("insert into t values (?)", t.input)
		if res != nil {
			defer res.Close()
		}
		if t.hasErr {
			c.Assert(err, NotNil)
			c.Assert(strings.Contains(err.Error(), t.err), IsTrue)
		} else {
			c.Assert(err, IsNil)
		}
		if res != nil {
			res.Close()
		}
	}

	tk.MustExec("set sql_mode=''")
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values (?)", -1)
	r := tk.MustQuery("select a from t limit 1")
	r.Check(testkit.Rows("0.00"))
}

func (s *testSuite3) TestIndexJoinBlockDualPanic(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists a")
	tk.MustExec("create block a (f1 int, f2 varchar(32), primary key (f1))")
	tk.MustExec("insert into a (f1,f2) values (1,'a'), (2,'b'), (3,'c')")
	tk.MustQuery("select a.* from a inner join (select 1 as k1,'k2-1' as k2) as k on a.f1=k.k1;").
		Check(testkit.Rows("1 a"))
}

func (s *testSuite3) TestSortLeftJoinWithNullDeferredCausetInRightChildPanic(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int)")
	tk.MustExec("create block t2(a int)")
	tk.MustExec("insert into t1(a) select 1;")
	tk.MustQuery("select b.n from t1 left join (select a as a, null as n from t2) b on b.a = t1.a order by t1.a").
		Check(testkit.Rows("<nil>"))
}

func (s *testSuiteP1) TestUnionAutoSignedCast(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1,t2")
	tk.MustExec("create block t1 (id int, i int, b bigint, d double, dd decimal)")
	tk.MustExec("create block t2 (id int, i int unsigned, b bigint unsigned, d double unsigned, dd decimal unsigned)")
	tk.MustExec("insert into t1 values(1, -1, -1, -1.1, -1)")
	tk.MustExec("insert into t2 values(2, 1, 1, 1.1, 1)")
	tk.MustQuery("select * from t1 union select * from t2 order by id").
		Check(testkit.Rows("1 -1 -1 -1.1 -1", "2 1 1 1.1 1"))
	tk.MustQuery("select id, i, b, d, dd from t2 union select id, i, b, d, dd from t1 order by id").
		Check(testkit.Rows("1 0 0 0 -1", "2 1 1 1.1 1"))
	tk.MustQuery("select id, i from t2 union select id, cast(i as unsigned int) from t1 order by id").
		Check(testkit.Rows("1 18446744073709551615", "2 1"))
	tk.MustQuery("select dd from t2 union all select dd from t2").
		Check(testkit.Rows("1", "1"))

	tk.MustExec("drop block if exists t3,t4")
	tk.MustExec("create block t3 (id int, v int)")
	tk.MustExec("create block t4 (id int, v double unsigned)")
	tk.MustExec("insert into t3 values (1, -1)")
	tk.MustExec("insert into t4 values (2, 1)")
	tk.MustQuery("select id, v from t3 union select id, v from t4 order by id").
		Check(testkit.Rows("1 -1", "2 1"))
	tk.MustQuery("select id, v from t4 union select id, v from t3 order by id").
		Check(testkit.Rows("1 0", "2 1"))

	tk.MustExec("drop block if exists t5,t6,t7")
	tk.MustExec("create block t5 (id int, v bigint unsigned)")
	tk.MustExec("create block t6 (id int, v decimal)")
	tk.MustExec("create block t7 (id int, v bigint)")
	tk.MustExec("insert into t5 values (1, 1)")
	tk.MustExec("insert into t6 values (2, -1)")
	tk.MustExec("insert into t7 values (3, -1)")
	tk.MustQuery("select id, v from t5 union select id, v from t6 order by id").
		Check(testkit.Rows("1 1", "2 -1"))
	tk.MustQuery("select id, v from t5 union select id, v from t7 union select id, v from t6 order by id").
		Check(testkit.Rows("1 1", "2 -1", "3 -1"))
}

func (s *testSuite6) TestUFIDelateJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2, t3, t4, t5, t6, t7")
	tk.MustExec("create block t1(k int, v int)")
	tk.MustExec("create block t2(k int, v int)")
	tk.MustExec("create block t3(id int auto_increment, k int, v int, primary key(id))")
	tk.MustExec("create block t4(k int, v int)")
	tk.MustExec("create block t5(v int, k int, primary key(k))")
	tk.MustExec("insert into t1 values (1, 1)")
	tk.MustExec("insert into t4 values (3, 3)")
	tk.MustExec("create block t6 (id int, v longtext)")
	tk.MustExec("create block t7 (x int, id int, v longtext, primary key(id))")

	// test the normal case that uFIDelate one event for a single block.
	tk.MustExec("uFIDelate t1 set v = 0 where k = 1")
	tk.MustQuery("select k, v from t1 where k = 1").Check(testkit.Rows("1 0"))

	// test the case that the block with auto_increment or none-null defCausumns as the right block of left join.
	tk.MustExec("uFIDelate t1 left join t3 on t1.k = t3.k set t1.v = 1")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select id, k, v from t3").Check(testkit.Rows())

	// test left join and the case that the right block has no matching record but has uFIDelated the right block defCausumns.
	tk.MustExec("uFIDelate t1 left join t2 on t1.k = t2.k set t1.v = t2.v, t2.v = 3")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())

	// test the case that the uFIDelate operation in the left block references data in the right block while data of the right block defCausumns is modified.
	tk.MustExec("uFIDelate t1 left join t2 on t1.k = t2.k set t2.v = 3, t1.v = t2.v")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 <nil>"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())

	// test right join and the case that the left block has no matching record but has uFIDelated the left block defCausumns.
	tk.MustExec("uFIDelate t2 right join t1 on t2.k = t1.k set t2.v = 4, t1.v = 0")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 0"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())

	// test the case of right join and left join at the same time.
	tk.MustExec("uFIDelate t1 left join t2 on t1.k = t2.k right join t4 on t4.k = t2.k set t1.v = 4, t2.v = 4, t4.v = 4")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 0"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows())
	tk.MustQuery("select k, v from t4").Check(testkit.Rows("3 4"))

	// test normal left join and the case that the right block has matching rows.
	tk.MustExec("insert t2 values (1, 10)")
	tk.MustExec("uFIDelate t1 left join t2 on t1.k = t2.k set t2.v = 11")
	tk.MustQuery("select k, v from t2").Check(testkit.Rows("1 11"))

	// test the case of continuously joining the same block and uFIDelating the unmatching records.
	tk.MustExec("uFIDelate t1 t11 left join t2 on t11.k = t2.k left join t1 t12 on t2.v = t12.k set t12.v = 233, t11.v = 111")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("1 111"))
	tk.MustQuery("select k, v from t2").Check(testkit.Rows("1 11"))

	// test the left join case that the left block has records but all records are null.
	tk.MustExec("delete from t1")
	tk.MustExec("delete from t2")
	tk.MustExec("insert into t1 values (null, null)")
	tk.MustExec("uFIDelate t1 left join t2 on t1.k = t2.k set t1.v = 1")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("<nil> 1"))

	// test the case that the right block of left join has an primary key.
	tk.MustExec("insert t5 values(0, 0)")
	tk.MustExec("uFIDelate t1 left join t5 on t1.k = t5.k set t1.v = 2")
	tk.MustQuery("select k, v from t1").Check(testkit.Rows("<nil> 2"))
	tk.MustQuery("select k, v from t5").Check(testkit.Rows("0 0"))

	tk.MustExec("insert into t6 values (1, NULL)")
	tk.MustExec("insert into t7 values (5, 1, 'a')")
	tk.MustExec("uFIDelate t6, t7 set t6.v = t7.v where t6.id = t7.id and t7.x = 5")
	tk.MustQuery("select v from t6").Check(testkit.Rows("a"))
}

func (s *testSuite3) TestMaxOneRow(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)
	tk.MustExec(`drop block if exists t1`)
	tk.MustExec(`drop block if exists t2`)
	tk.MustExec(`create block t1(a double, b double);`)
	tk.MustExec(`create block t2(a double, b double);`)
	tk.MustExec(`insert into t1 values(1, 1), (2, 2), (3, 3);`)
	tk.MustExec(`insert into t2 values(0, 0);`)
	tk.MustExec(`set @@milevadb_init_chunk_size=1;`)
	rs, err := tk.Exec(`select (select t1.a from t1 where t1.a > t2.a) as a from t2;`)
	c.Assert(err, IsNil)

	err = rs.Next(context.TODO(), rs.NewChunk())
	c.Assert(err.Error(), Equals, "subquery returns more than 1 event")

	err = rs.Close()
	c.Assert(err, IsNil)
}

func (s *testSuiteP2) TestCurrentTimestampValueSelection(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t,t1")

	tk.MustExec("create block t (id int, t0 timestamp null default current_timestamp, t1 timestamp(1) null default current_timestamp(1), t2 timestamp(2) null default current_timestamp(2) on uFIDelate current_timestamp(2))")
	tk.MustExec("insert into t (id) values (1)")
	rs := tk.MustQuery("select t0, t1, t2 from t where id = 1")
	t0 := rs.Rows()[0][0].(string)
	t1 := rs.Rows()[0][1].(string)
	t2 := rs.Rows()[0][2].(string)
	c.Assert(len(strings.Split(t0, ".")), Equals, 1)
	c.Assert(len(strings.Split(t1, ".")[1]), Equals, 1)
	c.Assert(len(strings.Split(t2, ".")[1]), Equals, 2)
	tk.MustQuery("select id from t where t0 = ?", t0).Check(testkit.Rows("1"))
	tk.MustQuery("select id from t where t1 = ?", t1).Check(testkit.Rows("1"))
	tk.MustQuery("select id from t where t2 = ?", t2).Check(testkit.Rows("1"))
	time.Sleep(time.Second)
	tk.MustExec("uFIDelate t set t0 = now() where id = 1")
	rs = tk.MustQuery("select t2 from t where id = 1")
	newT2 := rs.Rows()[0][0].(string)
	c.Assert(newT2 != t2, IsTrue)

	tk.MustExec("create block t1 (id int, a timestamp, b timestamp(2), c timestamp(3))")
	tk.MustExec("insert into t1 (id, a, b, c) values (1, current_timestamp(2), current_timestamp, current_timestamp(3))")
	rs = tk.MustQuery("select a, b, c from t1 where id = 1")
	a := rs.Rows()[0][0].(string)
	b := rs.Rows()[0][1].(string)
	d := rs.Rows()[0][2].(string)
	c.Assert(len(strings.Split(a, ".")), Equals, 1)
	c.Assert(strings.Split(b, ".")[1], Equals, "00")
	c.Assert(len(strings.Split(d, ".")[1]), Equals, 3)
}

func (s *testSuite3) TestRowID(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`set @@milevadb_enable_clustered_index=0;`)
	tk.MustExec(`create block t(a varchar(10), b varchar(10), c varchar(1), index idx(a, b, c));`)
	tk.MustExec(`insert into t values('a', 'b', 'c');`)
	tk.MustExec(`insert into t values('a', 'b', 'c');`)
	tk.MustQuery(`select b, _milevadb_rowid from t use index(idx) where a = 'a';`).Check(testkit.Rows(
		`b 1`,
		`b 2`,
	))
	tk.MustExec(`begin;`)
	tk.MustExec(`select * from t for uFIDelate`)
	tk.MustQuery(`select distinct b from t use index(idx) where a = 'a';`).Check(testkit.Rows(`b`))
	tk.MustExec(`commit;`)

	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a varchar(5) primary key)`)
	tk.MustExec(`insert into t values('a')`)
	tk.MustQuery("select *, _milevadb_rowid from t use index(`primary`) where _milevadb_rowid=1").Check(testkit.Rows("a 1"))
}

func (s *testSuite3) TestDoSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a int)`)
	_, err := tk.Exec(`do 1 in (select * from t)`)
	c.Assert(err, IsNil, Commentf("err %v", err))
	tk.MustExec(`insert into t values(1)`)
	r, err := tk.Exec(`do 1 in (select * from t)`)
	c.Assert(err, IsNil, Commentf("err %v", err))
	c.Assert(r, IsNil, Commentf("result of Do not empty"))
}

func (s *testSerialSuite) TestTSOFail(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a int)`)

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/stochastik/mockGetTSFail", "return"), IsNil)
	ctx := failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return fpname == "github.com/whtcorpsinc/milevadb/stochastik/mockGetTSFail"
	})
	_, err := tk.Se.Execute(ctx, `select * from t`)
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/stochastik/mockGetTSFail"), IsNil)
}

func (s *testSuite3) TestSelectHashPartitionBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)
	tk.MustExec(`drop block if exists th`)
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = '1';")
	tk.MustExec(`create block th (a int, b int) partition by hash(a) partitions 3;`)
	defer tk.MustExec(`drop block if exists th`)
	tk.MustExec(`insert into th values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);`)
	tk.MustExec("insert into th values (-1,-1),(-2,-2),(-3,-3),(-4,-4),(-5,-5),(-6,-6),(-7,-7),(-8,-8);")
	tk.MustQuery("select b from th order by a").Check(testkit.Rows("-8", "-7", "-6", "-5", "-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5", "6", "7", "8"))
	tk.MustQuery(" select * from th where a=-2;").Check(testkit.Rows("-2 -2"))
	tk.MustQuery(" select * from th where a=5;").Check(testkit.Rows("5 5"))
}

func (s *testSuiteP1) TestSelectPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)
	tk.MustExec(`drop block if exists th, tr`)
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = '1';")
	tk.MustExec(`create block th (a int, b int) partition by hash(a) partitions 3;`)
	tk.MustExec(`create block tr (a int, b int)
							partition by range (a) (
							partition r0 values less than (4),
							partition r1 values less than (7),
							partition r3 values less than maxvalue)`)
	defer tk.MustExec(`drop block if exists th, tr`)
	tk.MustExec(`insert into th values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);`)
	tk.MustExec("insert into th values (-1,-1),(-2,-2),(-3,-3),(-4,-4),(-5,-5),(-6,-6),(-7,-7),(-8,-8);")
	tk.MustExec(`insert into tr values (-3,-3),(3,3),(4,4),(7,7),(8,8);`)
	// select 1 partition.
	tk.MustQuery("select b from th partition (p0) order by a").Check(testkit.Rows("-6", "-3", "0", "3", "6"))
	tk.MustQuery("select b from tr partition (r0) order by a").Check(testkit.Rows("-3", "3"))
	tk.MustQuery("select b from th partition (p0,P0) order by a").Check(testkit.Rows("-6", "-3", "0", "3", "6"))
	tk.MustQuery("select b from tr partition (r0,R0,r0) order by a").Check(testkit.Rows("-3", "3"))
	// select multi partition.
	tk.MustQuery("select b from th partition (P2,p0) order by a").Check(testkit.Rows("-8", "-6", "-5", "-3", "-2", "0", "2", "3", "5", "6", "8"))
	tk.MustQuery("select b from tr partition (r1,R3) order by a").Check(testkit.Rows("4", "7", "8"))

	// test select unknown partition error
	err := tk.ExecToErr("select b from th partition (p0,p4)")
	c.Assert(err.Error(), Equals, "[block:1735]Unknown partition 'p4' in block 'th'")
	err = tk.ExecToErr("select b from tr partition (r1,r4)")
	c.Assert(err.Error(), Equals, "[block:1735]Unknown partition 'r4' in block 'tr'")

	// test select partition block in transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into th values (10,10),(11,11)")
	tk.MustQuery("select a, b from th where b>10").Check(testkit.Rows("11 11"))
	tk.MustExec("commit")
	tk.MustQuery("select a, b from th where b>10").Check(testkit.Rows("11 11"))
}

func (s *testSuiteP1) TestDeletePartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)
	tk.MustExec(`drop block if exists t1`)
	tk.MustExec(`create block t1 (a int) partition by range (a) (
 partition p0 values less than (10),
 partition p1 values less than (20),
 partition p2 values less than (30),
 partition p3 values less than (40),
 partition p4 values less than MAXVALUE
 )`)
	tk.MustExec("insert into t1 values (1),(11),(21),(31)")
	tk.MustExec("delete from t1 partition (p4)")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("1", "11", "21", "31"))
	tk.MustExec("delete from t1 partition (p0) where a > 10")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("1", "11", "21", "31"))
	tk.MustExec("delete from t1 partition (p0,p1,p2)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("31"))
}

func (s *testSuite) TestSelectView(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block view_t (a int,b int)")
	tk.MustExec("insert into view_t values(1,2)")
	tk.MustExec("create definer='root'@'localhost' view view1 as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view2(c,d) as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view3(c,d) as select a,b from view_t")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustExec("drop block view_t;")
	tk.MustExec("create block view_t(c int,d int)")
	err := tk.ExecToErr("select * from view1")
	c.Assert(err.Error(), Equals, "[planner:1356]View 'test.view1' references invalid block(s) or defCausumn(s) or function(s) or definer/invoker of view lack rights to use them")
	err = tk.ExecToErr("select * from view2")
	c.Assert(err.Error(), Equals, "[planner:1356]View 'test.view2' references invalid block(s) or defCausumn(s) or function(s) or definer/invoker of view lack rights to use them")
	err = tk.ExecToErr("select * from view3")
	c.Assert(err.Error(), Equals, plannercore.ErrViewInvalid.GenWithStackByArgs("test", "view3").Error())
	tk.MustExec("drop block view_t;")
	tk.MustExec("create block view_t(a int,b int,c int)")
	tk.MustExec("insert into view_t values(1,2,3)")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustExec("alter block view_t drop defCausumn a")
	tk.MustExec("alter block view_t add defCausumn a int after b")
	tk.MustExec("uFIDelate view_t set a=1;")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustExec("drop block view_t;")
	tk.MustExec("drop view view1,view2,view3;")

	tk.MustExec("set @@milevadb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@milevadb_enable_window_function = 0")
	}()
	tk.MustExec("create block t(a int, b int)")
	tk.MustExec("insert into t values (1,1),(1,2),(2,1),(2,2)")
	tk.MustExec("create definer='root'@'localhost' view v as select a, first_value(a) over(rows between 1 preceding and 1 following), last_value(a) over(rows between 1 preceding and 1 following) from t")
	result := tk.MustQuery("select * from v")
	result.Check(testkit.Rows("1 1 1", "1 1 2", "2 1 2", "2 2 2"))
	tk.MustExec("drop view v;")
}

type testSuite2 struct {
	*baseTestSuite
}

func (s *testSuite2) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show full blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", blockName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", blockName))
		} else {
			tk.MustExec(fmt.Sprintf("drop block %v", blockName))
		}
	}
}

type testSuite3 struct {
	*baseTestSuite
}

func (s *testSuite3) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show full blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", blockName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", blockName))
		} else {
			tk.MustExec(fmt.Sprintf("drop block %v", blockName))
		}
	}
}

type testSuite4 struct {
	*baseTestSuite
}

func (s *testSuite4) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show full blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", blockName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", blockName))
		} else {
			tk.MustExec(fmt.Sprintf("drop block %v", blockName))
		}
	}
}

type testSuite5 struct {
	*baseTestSuite
}

func (s *testSuite5) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show full blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", blockName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", blockName))
		} else {
			tk.MustExec(fmt.Sprintf("drop block %v", blockName))
		}
	}
}

type testSuite6 struct {
	*baseTestSuite
}

func (s *testSuite6) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show full blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", blockName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", blockName))
		} else {
			tk.MustExec(fmt.Sprintf("drop block %v", blockName))
		}
	}
}

type testSuite7 struct {
	*baseTestSuite
}

func (s *testSuite7) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show full blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", blockName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", blockName))
		} else {
			tk.MustExec(fmt.Sprintf("drop block %v", blockName))
		}
	}
}

type testSuite8 struct {
	*baseTestSuite
}

func (s *testSuite8) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show full blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", blockName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", blockName))
		} else {
			tk.MustExec(fmt.Sprintf("drop block %v", blockName))
		}
	}
}

type testSerialSuite1 struct {
	*baseTestSuite
}

func (s *testSerialSuite1) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show full blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", blockName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", blockName))
		} else {
			tk.MustExec(fmt.Sprintf("drop block %v", blockName))
		}
	}
}

func (s *testSuiteP2) TestStrToDateBuiltin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery(`select str_to_date('20190101','%Y%m%d%!') from dual`).Check(testkit.Rows("2020-01-01"))
	tk.MustQuery(`select str_to_date('20190101','%Y%m%d%f') from dual`).Check(testkit.Rows("2020-01-01 00:00:00.000000"))
	tk.MustQuery(`select str_to_date('20190101','%Y%m%d%H%i%s') from dual`).Check(testkit.Rows("2020-01-01 00:00:00"))
	tk.MustQuery(`select str_to_date('18/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('a18/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('69/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('70/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("1970-10-22"))
	tk.MustQuery(`select str_to_date('8/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("2008-10-22"))
	tk.MustQuery(`select str_to_date('8/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2008-10-22"))
	tk.MustQuery(`select str_to_date('18/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('a18/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('69/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('70/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("1970-10-22"))
	tk.MustQuery(`select str_to_date('018/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("0018-10-22"))
	tk.MustQuery(`select str_to_date('2020/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('018/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18/10/22','%y0/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18/10/22','%Y0/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18a/10/22','%y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18a/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('20188/10/22','%Y/%m/%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('2018510522','%Y5%m5%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('2020^10^22','%Y^%m^%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('2020@10@22','%Y@%m@%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('2020%10%22','%Y%%m%%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('2020(10(22','%Y(%m(%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('2020\10\22','%Y\%m\%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('2020=10=22','%Y=%m=%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('2020+10+22','%Y+%m+%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('2018_10_22','%Y_%m_%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('69510522','%y5%m5%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('69^10^22','%y^%m^%d') from dual`).Check(testkit.Rows("2069-10-22"))
	tk.MustQuery(`select str_to_date('18@10@22','%y@%m@%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('18%10%22','%y%%m%%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18(10(22','%y(%m(%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('18\10\22','%y\%m\%d') from dual`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select str_to_date('18+10+22','%y+%m+%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('18=10=22','%y=%m=%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`select str_to_date('18_10_22','%y_%m_%d') from dual`).Check(testkit.Rows("2020-10-22"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 11:22:33 PM', '%Y-%m-%d %r')`).Check(testkit.Rows("2020-07-04 23:22:33"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 12:22:33 AM', '%Y-%m-%d %r')`).Check(testkit.Rows("2020-07-04 00:22:33"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 12:22:33', '%Y-%m-%d %T')`).Check(testkit.Rows("2020-07-04 12:22:33"))
	tk.MustQuery(`SELECT STR_TO_DATE('2020-07-04 00:22:33', '%Y-%m-%d %T')`).Check(testkit.Rows("2020-07-04 00:22:33"))
}

func (s *testSuiteP2) TestReadPartitionedBlock(c *C) {
	// Test three reader on partitioned block.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists pt")
	tk.MustExec("create block pt (a int, b int, index i_b(b)) partition by range (a) (partition p1 values less than (2), partition p2 values less than (4), partition p3 values less than (6))")
	for i := 0; i < 6; i++ {
		tk.MustExec(fmt.Sprintf("insert into pt values(%d, %d)", i, i))
	}
	// Block reader
	tk.MustQuery("select * from pt order by a").Check(testkit.Rows("0 0", "1 1", "2 2", "3 3", "4 4", "5 5"))
	// Index reader
	tk.MustQuery("select b from pt where b = 3").Check(testkit.Rows("3"))
	// Index lookup
	tk.MustQuery("select a from pt where b = 3").Check(testkit.Rows("3"))
}

func (s *testSplitBlock) TestSplitRegion(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t, t1")
	tk.MustExec("create block t(a varchar(100),b int, index idx1(b,a))")
	tk.MustExec(`split block t index idx1 by (10000,"abcd"),(10000000);`)
	_, err := tk.Exec(`split block t index idx1 by ("abcd");`)
	c.Assert(err, NotNil)
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.WarnDataTruncated))

	// Test for split index region.
	// Check min value is more than max value.
	tk.MustExec(`split block t index idx1 between (0) and (1000000000) regions 10`)
	_, err = tk.Exec(`split block t index idx1 between (2,'a') and (1,'c') regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index `idx1` region lower value (2,a) should less than the upper value (1,c)")

	// Check min value is invalid.
	_, err = tk.Exec(`split block t index idx1 between () and (1) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index `idx1` region lower value count should more than 0")

	// Check max value is invalid.
	_, err = tk.Exec(`split block t index idx1 between (1) and () regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index `idx1` region upper value count should more than 0")

	// Check pre-split region num is too large.
	_, err = tk.Exec(`split block t index idx1 between (0) and (1000000000) regions 10000`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index region num exceeded the limit 1000")

	// Check pre-split region num 0 is invalid.
	_, err = tk.Exec(`split block t index idx1 between (0) and (1000000000) regions 0`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split index region num should more than 0")

	// Test truncate error msg.
	_, err = tk.Exec(`split block t index idx1 between ("aa") and (1000000000) regions 0`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1265]Incorrect value: 'aa' for defCausumn 'b'")

	// Test for split block region.
	tk.MustExec(`split block t between (0) and (1000000000) regions 10`)
	// Check the lower value is more than the upper value.
	_, err = tk.Exec(`split block t between (2) and (1) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split block `t` region lower value 2 should less than the upper value 1")

	// Check the lower value is invalid.
	_, err = tk.Exec(`split block t between () and (1) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split block region lower value count should be 1")

	// Check upper value is invalid.
	_, err = tk.Exec(`split block t between (1) and () regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split block region upper value count should be 1")

	// Check pre-split region num is too large.
	_, err = tk.Exec(`split block t between (0) and (1000000000) regions 10000`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split block region num exceeded the limit 1000")

	// Check pre-split region num 0 is invalid.
	_, err = tk.Exec(`split block t between (0) and (1000000000) regions 0`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split block region num should more than 0")

	// Test truncate error msg.
	_, err = tk.Exec(`split block t between ("aa") and (1000000000) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1265]Incorrect value: 'aa' for defCausumn '_milevadb_rowid'")

	// Test split block region step is too small.
	_, err = tk.Exec(`split block t between (0) and (100) regions 10`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Split block `t` region step value should more than 1000, step 10 is invalid")

	// Test split region by syntax.
	tk.MustExec(`split block t by (0),(1000),(1000000)`)

	// Test split region twice to test for multiple batch split region requests.
	tk.MustExec("create block t1(a int, b int)")
	tk.MustQuery("split block t1 between(0) and (10000) regions 10;").Check(testkit.Rows("9 1"))
	tk.MustQuery("split block t1 between(10) and (10010) regions 5;").Check(testkit.Rows("4 1"))

	// Test split region for partition block.
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int,b int) partition by hash(a) partitions 5;")
	tk.MustQuery("split block t between (0) and (1000000) regions 5;").Check(testkit.Rows("20 1"))
	// Test for `split for region` syntax.
	tk.MustQuery("split region for partition block t between (1000000) and (100000000) regions 10;").Check(testkit.Rows("45 1"))

	// Test split region for partition block with specified partition.
	tk.MustQuery("split block t partition (p1,p2) between (100000000) and (1000000000) regions 5;").Check(testkit.Rows("8 1"))
	// Test for `split for region` syntax.
	tk.MustQuery("split region for partition block t partition (p3,p4) between (100000000) and (1000000000) regions 5;").Check(testkit.Rows("8 1"))
}

func (s *testSplitBlock) TestClusterIndexSplitBlockIntegration(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_cluster_index_index_split_block_integration;")
	tk.MustExec("create database test_cluster_index_index_split_block_integration;")
	tk.MustExec("use test_cluster_index_index_split_block_integration;")
	tk.MustExec("set @@milevadb_enable_clustered_index=1;")

	tk.MustExec("create block t (a varchar(255), b double, c int, primary key (a, b));")

	// Value list length not match.
	lowerMsg := "Split block region lower value count should be 2"
	upperMsg := "Split block region upper value count should be 2"
	tk.MustGetErrMsg("split block t between ('aaa') and ('aaa', 100.0) regions 10;", lowerMsg)
	tk.MustGetErrMsg("split block t between ('aaa', 1.0) and ('aaa', 100.0, 11) regions 10;", upperMsg)

	// Value type not match.
	errMsg := "[types:1265]Incorrect value: 'aaa' for defCausumn 'b'"
	tk.MustGetErrMsg("split block t between ('aaa', 0.0) and (100.0, 'aaa') regions 10;", errMsg)

	// lower bound >= upper bound.
	errMsg = "Split block `t` region lower value (aaa,0) should less than the upper value (aaa,0)"
	tk.MustGetErrMsg("split block t between ('aaa', 0.0) and ('aaa', 0.0) regions 10;", errMsg)
	errMsg = "Split block `t` region lower value (bbb,0) should less than the upper value (aaa,0)"
	tk.MustGetErrMsg("split block t between ('bbb', 0.0) and ('aaa', 0.0) regions 10;", errMsg)

	// Exceed limit 1000.
	errMsg = "Split block region num exceeded the limit 1000"
	tk.MustGetErrMsg("split block t between ('aaa', 0.0) and ('aaa', 0.1) regions 100000;", errMsg)

	// Success.
	tk.MustExec("split block t between ('aaa', 0.0) and ('aaa', 100.0) regions 10;")
	tk.MustExec("split block t by ('aaa', 0.0), ('aaa', 20.0), ('aaa', 100.0);")
	tk.MustExec("split block t by ('aaa', 100.0), ('qqq', 20.0), ('zzz', 100.0), ('zzz', 1000.0);")

	tk.MustExec("drop block t;")
	tk.MustExec("create block t (a int, b int, c int, d int, primary key(a, c, d));")
	tk.MustQuery("split block t between (0, 0, 0) and (0, 0, 1) regions 1000;").Check(testkit.Rows("999 1"))

	tk.MustExec("drop block t;")
	tk.MustExec("create block t (a int, b int, c int, d int, primary key(d, a, c));")
	tk.MustQuery("split block t by (0, 0, 0), (1, 2, 3), (65535, 65535, 65535);").Check(testkit.Rows("3 1"))
}

func (s *testSplitBlock) TestClusterIndexShowBlockRegion(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 1)
	tk.MustExec("set global milevadb_scatter_region = 1")
	tk.MustExec("drop database if exists cluster_index_regions;")
	tk.MustExec("create database cluster_index_regions;")
	tk.MustExec("use cluster_index_regions;")
	tk.MustExec("set @@milevadb_enable_clustered_index=1;")
	tk.MustExec("create block t (a int, b int, c int, primary key(a, b));")
	tk.MustExec("insert t values (1, 1, 1), (2, 2, 2);")
	tk.MustQuery("split block t between (1, 0) and (2, 3) regions 2;").Check(testkit.Rows("1 1"))
	rows := tk.MustQuery("show block t regions").Rows()
	tbl := testGetBlockByName(c, tk.Se, "cluster_index_regions", "t")
	// Check the region start key.
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_r_03800000000000000183800000000000", tbl.Meta().ID))

	tk.MustExec("drop block t;")
	tk.MustExec("create block t (a int, b int);")
	tk.MustQuery("split block t between (0) and (100000) regions 2;").Check(testkit.Rows("1 1"))
	rows = tk.MustQuery("show block t regions").Rows()
	tbl = testGetBlockByName(c, tk.Se, "cluster_index_regions", "t")
	// Check the region start key is int64.
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_r_50000", tbl.Meta().ID))
}

func (s *testSuiteWithData) TestClusterIndexOuterJoinElimination(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`set @@milevadb_enable_clustered_index = 1`)
	tk.MustExec("use test")
	tk.MustExec("create block t (a int, b int, c int, primary key(a,b))")
	rows := tk.MustQuery(`explain select t1.a from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b`).Rows()
	rowStrs := s.testData.ConvertRowsToStrings(rows)
	for _, event := range rowStrs {
		// outer join has been eliminated.
		c.Assert(strings.Index(event, "Join"), Equals, -1)
	}
}

func (s *testSplitBlock) TestShowBlockRegion(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t_regions")
	tk.MustExec("set global milevadb_scatter_region = 1")
	atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 1)
	tk.MustExec("create block t_regions (a int key, b int, c int, index idx(b), index idx2(c))")
	_, err := tk.Exec("split partition block t_regions partition (p1,p2) index idx between (0) and (20000) regions 2;")
	c.Assert(err.Error(), Equals, plannercore.ErrPartitionClauseOnNonpartitioned.Error())

	// Test show block regions.
	tk.MustQuery(`split block t_regions between (-10000) and (10000) regions 4;`).Check(testkit.Rows("4 1"))
	re := tk.MustQuery("show block t_regions regions")
	rows := re.Rows()
	// Block t_regions should have 5 regions now.
	// 4 regions to causetstore record data.
	// 1 region to causetstore index data.
	c.Assert(len(rows), Equals, 5)
	c.Assert(len(rows[0]), Equals, 11)
	tbl := testGetBlockByName(c, tk.Se, "test", "t_regions")
	// Check the region start key.
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_r", tbl.Meta().ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_-5000", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_0", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID))
	c.Assert(rows[4][2], Equals, fmt.Sprintf("t_%d_r", tbl.Meta().ID))

	// Test show block index regions.
	tk.MustQuery(`split block t_regions index idx between (-1000) and (1000) regions 4;`).Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show block t_regions index idx regions")
	rows = re.Rows()
	// The index `idx` of block t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	// Check the region start key.
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d.*", tbl.Meta().ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))

	re = tk.MustQuery("show block t_regions regions")
	rows = re.Rows()
	// The index `idx` of block t_regions should have 9 regions now.
	// 4 regions to causetstore record data.
	// 4 region to causetstore index idx data.
	// 1 region to causetstore index idx2 data.
	c.Assert(len(rows), Equals, 9)
	// Check the region start key.
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_r", tbl.Meta().ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_-5000", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_0", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID))
	c.Assert(rows[4][1], Matches, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[7][2], Equals, fmt.Sprintf("t_%d_i_2_", tbl.Meta().ID))
	c.Assert(rows[8][2], Equals, fmt.Sprintf("t_%d_r", tbl.Meta().ID))

	// Test unsigned primary key and wait scatter finish.
	tk.MustExec("drop block if exists t_regions")
	atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 1)
	tk.MustExec("create block t_regions (a int unsigned key, b int, index idx(b))")

	// Test show block regions.
	tk.MustExec(`set @@stochastik.milevadb_wait_split_region_finish=1;`)
	tk.MustQuery(`split block t_regions by (2500),(5000),(7500);`).Check(testkit.Rows("3 1"))
	re = tk.MustQuery("show block t_regions regions")
	rows = re.Rows()
	// Block t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	tbl = testGetBlockByName(c, tk.Se, "test", "t_regions")
	// Check the region start key.
	c.Assert(rows[0][1], Matches, "t_.*")
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_2500", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_5000", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_7500", tbl.Meta().ID))

	// Test show block index regions.
	tk.MustQuery(`split block t_regions index idx by (250),(500),(750);`).Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show block t_regions index idx regions")
	rows = re.Rows()
	// The index `idx` of block t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	// Check the region start key.
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_1_.*", tbl.Meta().ID))

	// Test show block regions for partition block when disable split region when create block.
	atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 0)
	tk.MustExec("drop block if exists partition_t;")
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = '1';")
	tk.MustExec("create block partition_t (a int, b int,index(a)) partition by hash (a) partitions 3")
	re = tk.MustQuery("show block partition_t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Matches, "t_.*")

	// Test show block regions for partition block when enable split region when create block.
	atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 1)
	tk.MustExec("set @@global.milevadb_scatter_region=1;")
	tk.MustExec("drop block if exists partition_t;")
	tk.MustExec("create block partition_t (a int, b int,index(a)) partition by hash (a) partitions 3")
	re = tk.MustQuery("show block partition_t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 3)
	tbl = testGetBlockByName(c, tk.Se, "test", "partition_t")
	partitionDef := tbl.Meta().GetPartitionInfo().Definitions
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[0].ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[1].ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[2].ID))

	// Test split partition region when add new partition.
	tk.MustExec("drop block if exists partition_t;")
	tk.MustExec(`create block partition_t (a int, b int,index(a)) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20),
		PARTITION p2 VALUES LESS THAN (30));`)
	tk.MustExec(`alter block partition_t add partition ( partition p3 values less than (40), partition p4 values less than (50) );`)
	re = tk.MustQuery("show block partition_t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 5)
	tbl = testGetBlockByName(c, tk.Se, "test", "partition_t")
	partitionDef = tbl.Meta().GetPartitionInfo().Definitions
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[0].ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[1].ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[2].ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[3].ID))
	c.Assert(rows[4][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[4].ID))

	// Test pre-split block region when create block.
	tk.MustExec("drop block if exists t_pre")
	tk.MustExec("create block t_pre (a int, b int) shard_row_id_bits = 2 pre_split_regions=2;")
	re = tk.MustQuery("show block t_pre regions")
	rows = re.Rows()
	// Block t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	tbl = testGetBlockByName(c, tk.Se, "test", "t_pre")
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_2305843009213693952", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_4611686018427387904", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_6917529027641081856", tbl.Meta().ID))

	// Test pre-split block region when create block.
	tk.MustExec("drop block if exists pt_pre")
	tk.MustExec("create block pt_pre (a int, b int) shard_row_id_bits = 2 pre_split_regions=2 partition by hash(a) partitions 3;")
	re = tk.MustQuery("show block pt_pre regions")
	rows = re.Rows()
	// Block t_regions should have 4 regions now.
	c.Assert(len(rows), Equals, 12)
	tbl = testGetBlockByName(c, tk.Se, "test", "pt_pre")
	pi := tbl.Meta().GetPartitionInfo().Definitions
	c.Assert(len(pi), Equals, 3)
	for i, p := range pi {
		c.Assert(rows[1+4*i][1], Equals, fmt.Sprintf("t_%d_r_2305843009213693952", p.ID))
		c.Assert(rows[2+4*i][1], Equals, fmt.Sprintf("t_%d_r_4611686018427387904", p.ID))
		c.Assert(rows[3+4*i][1], Equals, fmt.Sprintf("t_%d_r_6917529027641081856", p.ID))
	}

	defer atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 0)

	// Test split partition block.
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int,b int) partition by hash(a) partitions 5;")
	tk.MustQuery("split block t between (0) and (4000000) regions 4;").Check(testkit.Rows("15 1"))
	re = tk.MustQuery("show block t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 20)
	tbl = testGetBlockByName(c, tk.Se, "test", "t")
	c.Assert(len(tbl.Meta().GetPartitionInfo().Definitions), Equals, 5)
	for i, p := range tbl.Meta().GetPartitionInfo().Definitions {
		c.Assert(rows[i*4+0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*4+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*4+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*4+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	}

	// Test split region for partition block with specified partition.
	tk.MustQuery("split block t partition (p4) between (1000000) and (2000000) regions 5;").Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show block t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 24)
	tbl = testGetBlockByName(c, tk.Se, "test", "t")
	c.Assert(len(tbl.Meta().GetPartitionInfo().Definitions), Equals, 5)
	for i := 0; i < 4; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*4+0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*4+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*4+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*4+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	}
	for i := 4; i < 5; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*4+0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*4+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*4+2][1], Equals, fmt.Sprintf("t_%d_r_1200000", p.ID))
		c.Assert(rows[i*4+3][1], Equals, fmt.Sprintf("t_%d_r_1400000", p.ID))
		c.Assert(rows[i*4+4][1], Equals, fmt.Sprintf("t_%d_r_1600000", p.ID))
		c.Assert(rows[i*4+5][1], Equals, fmt.Sprintf("t_%d_r_1800000", p.ID))
		c.Assert(rows[i*4+6][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*4+7][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	}

	// Test for show block partition regions.
	for i := 0; i < 4; i++ {
		re = tk.MustQuery(fmt.Sprintf("show block t partition (p%v) regions", i))
		rows = re.Rows()
		c.Assert(len(rows), Equals, 4)
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	}
	re = tk.MustQuery("show block t partition (p0, p4) regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 12)
	p := tbl.Meta().GetPartitionInfo().Definitions[0]
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	p = tbl.Meta().GetPartitionInfo().Definitions[4]
	c.Assert(rows[4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[5][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
	c.Assert(rows[6][1], Equals, fmt.Sprintf("t_%d_r_1200000", p.ID))
	c.Assert(rows[7][1], Equals, fmt.Sprintf("t_%d_r_1400000", p.ID))
	c.Assert(rows[8][1], Equals, fmt.Sprintf("t_%d_r_1600000", p.ID))
	c.Assert(rows[9][1], Equals, fmt.Sprintf("t_%d_r_1800000", p.ID))
	c.Assert(rows[10][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
	c.Assert(rows[11][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
	// Test for duplicate partition names.
	re = tk.MustQuery("show block t partition (p0, p0, p0) regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 4)
	p = tbl.Meta().GetPartitionInfo().Definitions[0]
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))

	// Test split partition block index.
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int,b int,index idx(a)) partition by hash(a) partitions 5;")
	tk.MustQuery("split block t between (0) and (4000000) regions 4;").Check(testkit.Rows("20 1"))
	tk.MustQuery("split block t index idx between (0) and (4000000) regions 4;").Check(testkit.Rows("20 1"))
	re = tk.MustQuery("show block t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 40)
	tbl = testGetBlockByName(c, tk.Se, "test", "t")
	c.Assert(len(tbl.Meta().GetPartitionInfo().Definitions), Equals, 5)
	for i := 0; i < 5; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*8+0][1], Equals, fmt.Sprintf("t_%d_r", p.ID))
		c.Assert(rows[i*8+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*8+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*8+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
		c.Assert(rows[i*8+4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*8+5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+7][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	}

	// Test split index region for partition block with specified partition.
	tk.MustQuery("split block t partition (p4) index idx between (0) and (1000000) regions 5;").Check(testkit.Rows("4 1"))
	re = tk.MustQuery("show block t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 44)
	tbl = testGetBlockByName(c, tk.Se, "test", "t")
	c.Assert(len(tbl.Meta().GetPartitionInfo().Definitions), Equals, 5)
	for i := 0; i < 4; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*8+0][1], Equals, fmt.Sprintf("t_%d_r", p.ID))
		c.Assert(rows[i*8+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*8+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*8+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
		c.Assert(rows[i*8+4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*8+5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+7][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	}
	for i := 4; i < 5; i++ {
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[i*8+0][1], Equals, fmt.Sprintf("t_%d_r", p.ID))
		c.Assert(rows[i*8+1][1], Equals, fmt.Sprintf("t_%d_r_1000000", p.ID))
		c.Assert(rows[i*8+2][1], Equals, fmt.Sprintf("t_%d_r_2000000", p.ID))
		c.Assert(rows[i*8+3][1], Equals, fmt.Sprintf("t_%d_r_3000000", p.ID))
		c.Assert(rows[i*8+4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[i*8+5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+7][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+8][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+9][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+10][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[i*8+11][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	}

	// Test show block partition region on unknown-partition.
	err = tk.QueryToErr("show block t partition (p_unknown) index idx regions")
	c.Assert(terror.ErrorEqual(err, block.ErrUnknownPartition), IsTrue)

	// Test show block partition index.
	for i := 0; i < 4; i++ {
		re = tk.MustQuery(fmt.Sprintf("show block t partition (p%v) index idx regions", i))
		rows = re.Rows()
		c.Assert(len(rows), Equals, 4)
		p := tbl.Meta().GetPartitionInfo().Definitions[i]
		c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
		c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
		c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	}
	re = tk.MustQuery("show block t partition (p3,p4) index idx regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 12)
	p = tbl.Meta().GetPartitionInfo().Definitions[3]
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	p = tbl.Meta().GetPartitionInfo().Definitions[4]
	c.Assert(rows[4][1], Equals, fmt.Sprintf("t_%d_", p.ID))
	c.Assert(rows[5][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[6][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[7][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[8][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[9][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[10][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))
	c.Assert(rows[11][1], Matches, fmt.Sprintf("t_%d_i_1_.*", p.ID))

	// Test split for the second index.
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int,b int,index idx(a), index idx2(b))")
	tk.MustQuery("split block t index idx2 between (0) and (4000000) regions 2;").Check(testkit.Rows("3 1"))
	re = tk.MustQuery("show block t regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 4)
	tbl = testGetBlockByName(c, tk.Se, "test", "t")
	c.Assert(rows[0][1], Equals, fmt.Sprintf("t_%d_i_3_", tbl.Meta().ID))
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_", tbl.Meta().ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_i_2_.*", tbl.Meta().ID))
	c.Assert(rows[3][1], Matches, fmt.Sprintf("t_%d_i_2_.*", tbl.Meta().ID))

	// Test show block partition region on non-partition block.
	err = tk.QueryToErr("show block t partition (p3,p4) index idx regions")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrPartitionClauseOnNonpartitioned), IsTrue)
}

func testGetBlockByName(c *C, ctx stochastikctx.Context, EDB, block string) block.Block {
	dom := petri.GetPetri(ctx)
	// Make sure the block schemaReplicant is the new schemaReplicant.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr(EDB), perceptron.NewCIStr(block))
	c.Assert(err, IsNil)
	return tbl
}

func (s *testSuiteP2) TestIssue10435(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(i int, j int, k int)")
	tk.MustExec("insert into t1 VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4)")
	tk.MustExec("INSERT INTO t1 SELECT 10*i,j,5*j FROM t1 UNION SELECT 20*i,j,5*j FROM t1 UNION SELECT 30*i,j,5*j FROM t1")

	tk.MustExec("set @@stochastik.milevadb_enable_window_function=1")
	tk.MustQuery("SELECT SUM(i) OVER W FROM t1 WINDOW w AS (PARTITION BY j ORDER BY i) ORDER BY 1+SUM(i) OVER w").Check(
		testkit.Rows("1", "2", "3", "4", "11", "22", "31", "33", "44", "61", "62", "93", "122", "124", "183", "244"),
	)
}

func (s *testSuiteP2) TestUnsignedFeedback(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	oriProbability := statistics.FeedbackProbability.Load()
	statistics.FeedbackProbability.CausetStore(1.0)
	defer func() { statistics.FeedbackProbability.CausetStore(oriProbability) }()
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a bigint unsigned, b int, primary key(a))")
	tk.MustExec("insert into t values (1,1),(2,2)")
	tk.MustExec("analyze block t")
	tk.MustQuery("select count(distinct b) from t").Check(testkit.Rows("2"))
	result := tk.MustQuery("explain analyze select count(distinct b) from t")
	c.Assert(result.Rows()[2][4], Equals, "block:t")
	c.Assert(result.Rows()[2][6], Equals, "range:[0,+inf], keep order:false")
}

func (s *testSuite) TestOOMPanicCausetAction(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int primary key, b double);")
	tk.MustExec("insert into t values (1,1)")
	sm := &mockStochastikManager1{
		PS: make([]*soliton.ProcessInfo, 0),
	}
	tk.Se.SetStochastikManager(sm)
	s.petri.ExpensiveQueryHandle().SetStochastikManager(sm)
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.OOMCausetAction = config.OOMCausetActionCancel
	})
	tk.MustExec("set @@milevadb_mem_quota_query=1;")
	err := tk.QueryToErr("select sum(b) from t group by a;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")

	// Test insert from select oom panic.
	tk.MustExec("drop block if exists t,t1")
	tk.MustExec("create block t (a bigint);")
	tk.MustExec("create block t1 (a bigint);")
	tk.MustExec("set @@milevadb_mem_quota_query=200;")
	_, err = tk.Exec("insert into t1 values (1),(2),(3),(4),(5);")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
	_, err = tk.Exec("replace into t1 values (1),(2),(3),(4),(5);")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
	tk.MustExec("set @@milevadb_mem_quota_query=10000")
	tk.MustExec("insert into t1 values (1),(2),(3),(4),(5);")
	tk.MustExec("set @@milevadb_mem_quota_query=10;")
	_, err = tk.Exec("insert into t select a from t1 order by a desc;")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
	_, err = tk.Exec("replace into t select a from t1 order by a desc;")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")

	tk.MustExec("set @@milevadb_mem_quota_query=10000")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5);")
	// Set the memory quota to 244 to make this ALLEGROALLEGROSQL panic during the DeleteExec
	// instead of the BlockReaderExec.
	tk.MustExec("set @@milevadb_mem_quota_query=244;")
	_, err = tk.Exec("delete from t")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")

	tk.MustExec("set @@milevadb_mem_quota_query=10000;")
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5);")
	tk.MustExec("set @@milevadb_mem_quota_query=244;")
	_, err = tk.Exec("delete t, t1 from t join t1 on t.a = t1.a")

	tk.MustExec("set @@milevadb_mem_quota_query=100000;")
	tk.MustExec("truncate block t")
	tk.MustExec("insert into t values(1),(2),(3)")
	// set the memory to quota to make the ALLEGROALLEGROSQL panic during UFIDelateExec instead
	// of BlockReader.
	tk.MustExec("set @@milevadb_mem_quota_query=244;")
	_, err = tk.Exec("uFIDelate t set a = 4")
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
}

type testRecoverBlock struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	cluster     cluster.Cluster
	cli         *regionProperityClient
}

func (s *testRecoverBlock) SetUpSuite(c *C) {
	cli := &regionProperityClient{}
	hijackClient := func(c einsteindb.Client) einsteindb.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.causetstore, err = mockstore.NewMockStore(
		mockstore.WithClientHijacker(hijackClient),
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *testRecoverBlock) TearDownSuite(c *C) {
	s.causetstore.Close()
	s.dom.Close()
}

func (s *testRecoverBlock) TestRecoverBlock(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		failpoint.Disable("github.com/whtcorpsinc/milevadb/meta/autoid/mockAutoIDChange")
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop block if exists t_recover")
	tk.MustExec("create block t_recover (a int);")
	defer func(originGC bool) {
		if originGC {
			dbs.EmulatorGCEnable()
		} else {
			dbs.EmulatorGCDisable()
		}
	}(dbs.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete block record as soon as possible after execute drop block dbs.
	dbs.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	timeAfterDrop := time.Now().Add(48 * 60 * 60 * time.Second).Format(gcTimeFormat)
	safePointALLEGROSQL := `INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('einsteindb_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UFIDelATE variable_value = '%[1]s'`
	// clear GC variables first.
	tk.MustExec("delete from allegrosql.milevadb where variable_name in ( 'einsteindb_gc_safe_point','einsteindb_gc_enable' )")

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop block t_recover")

	// if GC safe point is not exists in allegrosql.milevadb
	_, err := tk.Exec("recover block t_recover")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "can not get 'einsteindb_gc_safe_point'")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))

	// if GC enable is not exists in allegrosql.milevadb
	_, err = tk.Exec("recover block t_recover")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:-1]can not get 'einsteindb_gc_enable'")

	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// recover job is before GC safe point
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeAfterDrop))
	_, err = tk.Exec("recover block t_recover")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "Can't find dropped/truncated block 't_recover' in GC safe point"), Equals, true)

	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))
	// if there is a new block with the same name, should return failed.
	tk.MustExec("create block t_recover (a int);")
	_, err = tk.Exec("recover block t_recover")
	c.Assert(err.Error(), Equals, schemareplicant.ErrBlockExists.GenWithStackByArgs("t_recover").Error())

	// drop the new block with the same name, then recover block.
	tk.MustExec("rename block t_recover to t_recover2")

	// do recover block.
	tk.MustExec("recover block t_recover")

	// check recover block meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover block autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// check rebase auto id.
	tk.MustQuery("select a,_milevadb_rowid from t_recover;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003"))

	// recover block by none exits job.
	_, err = tk.Exec(fmt.Sprintf("recover block by job %d", 10000000))
	c.Assert(err, NotNil)

	// Disable GC by manual first, then after recover block, the GC enable status should also be disabled.
	err = gcutil.DisableGC(tk.Se)
	c.Assert(err, IsNil)

	tk.MustExec("delete from t_recover where a > 1")
	tk.MustExec("drop block t_recover")

	tk.MustExec("recover block t_recover")

	// check recover block meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1"))
	// check recover block autoID.
	tk.MustExec("insert into t_recover values (7),(8),(9)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9"))

	// Recover truncate block.
	tk.MustExec("truncate block t_recover")
	tk.MustExec("rename block t_recover to t_recover_new")
	tk.MustExec("recover block t_recover")
	tk.MustExec("insert into t_recover values (10)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9", "10"))

	// Test for recover one block multiple time.
	tk.MustExec("drop block t_recover")
	tk.MustExec("flashback block t_recover to t_recover_tmp")
	_, err = tk.Exec(fmt.Sprintf("recover block t_recover"))
	c.Assert(schemareplicant.ErrBlockExists.Equal(err), IsTrue)

	gcEnable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(gcEnable, Equals, false)
}

func (s *testRecoverBlock) TestFlashbackBlock(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists test_flashback")
	tk.MustExec("use test_flashback")
	tk.MustExec("drop block if exists t_flashback")
	tk.MustExec("create block t_flashback (a int);")
	defer func(originGC bool) {
		if originGC {
			dbs.EmulatorGCEnable()
		} else {
			dbs.EmulatorGCDisable()
		}
	}(dbs.IsEmulatorGCEnable())

	// Disable emulator GC.
	// Otherwise emulator GC will delete block record as soon as possible after execute drop block dbs.
	dbs.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointALLEGROSQL := `INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('einsteindb_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UFIDelATE variable_value = '%[1]s'`
	// Clear GC variables first.
	tk.MustExec("delete from allegrosql.milevadb where variable_name in ( 'einsteindb_gc_safe_point','einsteindb_gc_enable' )")
	// Set GC safe point
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))
	// Set GC enable.
	err := gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	tk.MustExec("insert into t_flashback values (1),(2),(3)")
	tk.MustExec("drop block t_flashback")

	// Test flash block with not_exist_block_name name.
	_, err = tk.Exec("flashback block t_not_exists")
	c.Assert(err.Error(), Equals, "Can't find dropped/truncated block: t_not_exists in DBS history jobs")

	// Test flashback block failed by there is already a new block with the same name.
	// If there is a new block with the same name, should return failed.
	tk.MustExec("create block t_flashback (a int);")
	_, err = tk.Exec("flashback block t_flashback")
	c.Assert(err.Error(), Equals, schemareplicant.ErrBlockExists.GenWithStackByArgs("t_flashback").Error())

	// Drop the new block with the same name, then flashback block.
	tk.MustExec("rename block t_flashback to t_flashback_tmp")

	// Test for flashback block.
	tk.MustExec("flashback block t_flashback")
	// Check flashback block meta and data record.
	tk.MustQuery("select * from t_flashback;").Check(testkit.Rows("1", "2", "3"))
	// Check flashback block autoID.
	tk.MustExec("insert into t_flashback values (4),(5),(6)")
	tk.MustQuery("select * from t_flashback;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// Check rebase auto id.
	tk.MustQuery("select a,_milevadb_rowid from t_flashback;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003"))

	// Test for flashback to new block.
	tk.MustExec("drop block t_flashback")
	tk.MustExec("create block t_flashback (a int);")
	tk.MustExec("flashback block t_flashback to t_flashback2")
	// Check flashback block meta and data record.
	tk.MustQuery("select * from t_flashback2;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	// Check flashback block autoID.
	tk.MustExec("insert into t_flashback2 values (7),(8),(9)")
	tk.MustQuery("select * from t_flashback2;").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	// Check rebase auto id.
	tk.MustQuery("select a,_milevadb_rowid from t_flashback2;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003", "7 10001", "8 10002", "9 10003"))

	// Test for flashback one block multiple time.
	_, err = tk.Exec(fmt.Sprintf("flashback block t_flashback to t_flashback4"))
	c.Assert(schemareplicant.ErrBlockExists.Equal(err), IsTrue)

	// Test for flashback truncated block to new block.
	tk.MustExec("truncate block t_flashback2")
	tk.MustExec("flashback block t_flashback2 to t_flashback3")
	// Check flashback block meta and data record.
	tk.MustQuery("select * from t_flashback3;").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	// Check flashback block autoID.
	tk.MustExec("insert into t_flashback3 values (10),(11)")
	tk.MustQuery("select * from t_flashback3;").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"))
	// Check rebase auto id.
	tk.MustQuery("select a,_milevadb_rowid from t_flashback3;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 5003", "7 10001", "8 10002", "9 10003", "10 15001", "11 15002"))

	// Test for flashback drop partition block.
	tk.MustExec("drop block if exists t_p_flashback")
	tk.MustExec("create block t_p_flashback (a int) partition by hash(a) partitions 4;")
	tk.MustExec("insert into t_p_flashback values (1),(2),(3)")
	tk.MustExec("drop block t_p_flashback")
	tk.MustExec("flashback block t_p_flashback")
	// Check flashback block meta and data record.
	tk.MustQuery("select * from t_p_flashback order by a;").Check(testkit.Rows("1", "2", "3"))
	// Check flashback block autoID.
	tk.MustExec("insert into t_p_flashback values (4),(5)")
	tk.MustQuery("select a,_milevadb_rowid from t_p_flashback order by a;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002"))

	// Test for flashback truncate partition block.
	tk.MustExec("truncate block t_p_flashback")
	tk.MustExec("flashback block t_p_flashback to t_p_flashback1")
	// Check flashback block meta and data record.
	tk.MustQuery("select * from t_p_flashback1 order by a;").Check(testkit.Rows("1", "2", "3", "4", "5"))
	// Check flashback block autoID.
	tk.MustExec("insert into t_p_flashback1 values (6)")
	tk.MustQuery("select a,_milevadb_rowid from t_p_flashback1 order by a;").Check(testkit.Rows("1 1", "2 2", "3 3", "4 5001", "5 5002", "6 10001"))

	tk.MustExec("drop database if exists Test2")
	tk.MustExec("create database Test2")
	tk.MustExec("use Test2")
	tk.MustExec("create block t (a int);")
	tk.MustExec("insert into t values (1),(2)")
	tk.MustExec("drop block t")
	tk.MustExec("flashback block t")
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1", "2"))

	tk.MustExec("drop block t")
	tk.MustExec("drop database if exists Test3")
	tk.MustExec("create database Test3")
	tk.MustExec("use Test3")
	tk.MustExec("create block t (a int);")
	tk.MustExec("drop block t")
	tk.MustExec("drop database Test3")
	tk.MustExec("use Test2")
	tk.MustExec("flashback block t")
	tk.MustExec("insert into t values (3)")
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1", "2", "3"))
}

func (s *testSuiteP2) TestPointGetPreparedPlan(c *C) {
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("drop database if exists ps_text")
	defer tk1.MustExec("drop database if exists ps_text")
	tk1.MustExec("create database ps_text")
	tk1.MustExec("use ps_text")

	tk1.MustExec(`create block t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	pspk1Id, _, _, err := tk1.Se.PrepareStmt("select * from t where a = ?")
	c.Assert(err, IsNil)
	tk1.Se.GetStochastikVars().PreparedStmts[pspk1Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	pspk2Id, _, _, err := tk1.Se.PrepareStmt("select * from t where ? = a ")
	c.Assert(err, IsNil)
	tk1.Se.GetStochastikVars().PreparedStmts[pspk2Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Causet{types.NewCauset(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Causet{types.NewCauset(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Causet{types.NewCauset(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Causet{types.NewCauset(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3"))

	// unique index
	psuk1Id, _, _, err := tk1.Se.PrepareStmt("select * from t where b = ? ")
	c.Assert(err, IsNil)
	tk1.Se.GetStochastikVars().PreparedStmts[psuk1Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	// test schemaReplicant changed, cached plan should be invalidated
	tk1.MustExec("alter block t add defCausumn defCaus4 int default 10 after c")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Causet{types.NewCauset(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	tk1.MustExec("alter block t drop index k_b")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	tk1.MustExec(`insert into t values(4, 3, 3, 11)`)
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10", "4 3 3 11"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	tk1.MustExec("delete from t where a = 4")
	tk1.MustExec("alter block t add index k_b(b)")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, psuk1Id, []types.Causet{types.NewCauset(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	// use pk again
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk2Id, []types.Causet{types.NewCauset(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(3)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("3 3 3 10"))
}

func (s *testSuiteP2) TestPointGetPreparedPlanWithCommitMode(c *C) {
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("drop database if exists ps_text")
	defer tk1.MustExec("drop database if exists ps_text")
	tk1.MustExec("create database ps_text")
	tk1.MustExec("use ps_text")

	tk1.MustExec(`create block t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	pspk1Id, _, _, err := tk1.Se.PrepareStmt("select * from t where a = ?")
	c.Assert(err, IsNil)
	tk1.Se.GetStochastikVars().PreparedStmts[pspk1Id].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false

	ctx := context.Background()
	// first time plan generated
	rs, err := tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(0)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(nil)

	// using the generated plan but with different params
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// uFIDelate rows
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustExec("use ps_text")
	tk2.MustExec("uFIDelate t set c = c + 10 where c = 1")

	// try to point get again
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 1"))

	// try to uFIDelate in stochastik 1
	tk1.MustExec("uFIDelate t set c = c + 10 where c = 1")
	_, err = tk1.Exec("commit")
	c.Assert(ekv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))

	// verify
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("1 1 11"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, pspk1Id, []types.Causet{types.NewCauset(2)})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Rows("2 2 2"))

	tk2.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 11"))
}

func (s *testSuiteP2) TestPointUFIDelatePreparedPlan(c *C) {
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("drop database if exists pu_test")
	defer tk1.MustExec("drop database if exists pu_test")
	tk1.MustExec("create database pu_test")
	tk1.MustExec("use pu_test")

	tk1.MustExec(`create block t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	uFIDelateID1, pc, _, err := tk1.Se.PrepareStmt(`uFIDelate t set c = c + 1 where a = ?`)
	c.Assert(err, IsNil)
	tk1.Se.GetStochastikVars().PreparedStmts[uFIDelateID1].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	c.Assert(pc, Equals, 1)
	uFIDelateID2, pc, _, err := tk1.Se.PrepareStmt(`uFIDelate t set c = c + 2 where ? = a`)
	c.Assert(err, IsNil)
	tk1.Se.GetStochastikVars().PreparedStmts[uFIDelateID2].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	c.Assert(pc, Equals, 1)

	ctx := context.Background()
	// first time plan generated
	rs, err := tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	// using the generated plan but with different params
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))

	// uFIDelateID2
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID2, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 8"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID2, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	// unique index
	uFIDelUkID1, _, _, err := tk1.Se.PrepareStmt(`uFIDelate t set c = c + 10 where b = ?`)
	c.Assert(err, IsNil)
	tk1.Se.GetStochastikVars().PreparedStmts[uFIDelUkID1].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelUkID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 20"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelUkID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 30"))

	// test schemaReplicant changed, cached plan should be invalidated
	tk1.MustExec("alter block t add defCausumn defCaus4 int default 10 after c")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 31 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 32 10"))

	tk1.MustExec("alter block t drop index k_b")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelUkID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 42 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelUkID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 52 10"))

	tk1.MustExec("alter block t add unique index k_b(b)")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelUkID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 62 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelUkID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 72 10"))

	tk1.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 1 10"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2 10"))
}

func (s *testSuiteP2) TestPointUFIDelatePreparedPlanWithCommitMode(c *C) {
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("drop database if exists pu_test2")
	defer tk1.MustExec("drop database if exists pu_test2")
	tk1.MustExec("create database pu_test2")
	tk1.MustExec("use pu_test2")

	tk1.MustExec(`create block t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	ctx := context.Background()
	uFIDelateID1, _, _, err := tk1.Se.PrepareStmt(`uFIDelate t set c = c + 1 where a = ?`)
	tk1.Se.GetStochastikVars().PreparedStmts[uFIDelateID1].(*plannercore.CachedPrepareStmt).PreparedAst.UseCache = false
	c.Assert(err, IsNil)

	// first time plan generated
	rs, err := tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 4"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))

	// next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	// try to exec using point get plan(this plan should not go short path)
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))

	// uFIDelate rows
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustExec("use pu_test2")
	tk2.MustExec(`prepare pu2 from "uFIDelate t set c = c + 2 where ? = a "`)
	tk2.MustExec("set @p3 = 3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 5"))
	tk2.MustExec("execute pu2 using @p3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 7"))
	tk2.MustExec("execute pu2 using @p3")
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))

	// try to uFIDelate in stochastik 1
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 6"))
	_, err = tk1.Exec("commit")
	c.Assert(ekv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))

	// verify
	tk2.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1 1"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2"))
	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))
	tk1.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2 2"))
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 9"))

	// again next start a non autocommit txn
	tk1.MustExec("set autocommit = 0")
	tk1.MustExec("begin")
	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 10"))

	rs, err = tk1.Se.ExecutePreparedStmt(ctx, uFIDelateID1, []types.Causet{types.NewCauset(3)})
	c.Assert(rs, IsNil)
	c.Assert(err, IsNil)
	tk1.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
	tk1.MustExec("commit")

	tk2.MustQuery("select * from t where a = 3").Check(testkit.Rows("3 3 11"))
}

func (s *testSuite1) TestPartitionHashCode(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec(`create block t(c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by hash (c1) partitions 4;`)
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
			for i := 0; i < 5; i++ {
				tk1.MustExec("select * from t")
			}
		}()
	}
	wg.Wait()
}

func (s *testSuite1) TestAlterDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t(a int, primary key(a))")
	tk.MustExec("insert into t(a) values(1)")
	tk.MustExec("alter block t add defCausumn b int default 1")
	tk.MustExec("alter block t alter b set default 2")
	tk.MustQuery("select b from t where a = 1").Check(testkit.Rows("1"))
}

type testClusterBlockSuite struct {
	testSuiteWithCliBase
	rpcserver  *grpc.Server
	listenAddr string
}

func (s *testClusterBlockSuite) SetUpSuite(c *C) {
	s.testSuiteWithCliBase.SetUpSuite(c)
	s.rpcserver, s.listenAddr = s.setUpRPCService(c, "127.0.0.1:0")
}

func (s *testClusterBlockSuite) setUpRPCService(c *C, addr string) (*grpc.Server, string) {
	sm := &mockStochastikManager1{}
	sm.PS = append(sm.PS, &soliton.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: allegrosql.ComQuery,
	})
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	srv := server.NewRPCServer(config.GetGlobalConfig(), s.dom, sm)
	port := lis.Addr().(*net.TCPAddr).Port
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	go func() {
		err = srv.Serve(lis)
		c.Assert(err, IsNil)
	}()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = uint(port)
	})
	return srv, addr
}
func (s *testClusterBlockSuite) TearDownSuite(c *C) {
	if s.rpcserver != nil {
		s.rpcserver.Stop()
		s.rpcserver = nil
	}
	s.testSuiteWithCliBase.TearDownSuite(c)
}

func (s *testClusterBlockSuite) TestSlowQuery(c *C) {
	writeFile := func(file string, data string) {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		c.Assert(err, IsNil)
		_, err = f.Write([]byte(data))
		c.Assert(f.Close(), IsNil)
		c.Assert(err, IsNil)
	}

	logData0 := ""
	logData1 := `
# Time: 2020-02-15T18:00:01.000000+08:00
select 1;
# Time: 2020-02-15T19:00:05.000000+08:00
select 2;`
	logData2 := `
# Time: 2020-02-16T18:00:01.000000+08:00
select 3;
# Time: 2020-02-16T18:00:05.000000+08:00
select 4;`
	logData3 := `
# Time: 2020-02-16T19:00:00.000000+08:00
select 5;
# Time: 2020-02-17T18:00:05.000000+08:00
select 6;`
	logData4 := `
# Time: 2020-05-14T19:03:54.314615176+08:00
select 7;`

	fileName0 := "milevadb-slow-2020-02-14T19-04-05.01.log"
	fileName1 := "milevadb-slow-2020-02-15T19-04-05.01.log"
	fileName2 := "milevadb-slow-2020-02-16T19-04-05.01.log"
	fileName3 := "milevadb-slow-2020-02-17T18-00-05.01.log"
	fileName4 := "milevadb-slow.log"
	writeFile(fileName0, logData0)
	writeFile(fileName1, logData1)
	writeFile(fileName2, logData2)
	writeFile(fileName3, logData3)
	writeFile(fileName4, logData4)
	defer func() {
		os.Remove(fileName0)
		os.Remove(fileName1)
		os.Remove(fileName2)
		os.Remove(fileName3)
		os.Remove(fileName4)
	}()
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	tk.Se.GetStochastikVars().TimeZone = loc
	tk.MustExec("use information_schema")
	cases := []struct {
		prepareALLEGROSQL string
		allegrosql        string
		result            []string
	}{
		{
			allegrosql: "select count(*),min(time),max(time) from %s where time > '2020-01-26 21:51:00' and time < now()",
			result:     []string{"7|2020-02-15 18:00:01.000000|2020-05-14 19:03:54.314615"},
		},
		{
			allegrosql: "select count(*),min(time),max(time) from %s where time > '2020-02-15 19:00:00' and time < '2020-02-16 18:00:02'",
			result:     []string{"2|2020-02-15 19:00:05.000000|2020-02-16 18:00:01.000000"},
		},
		{
			allegrosql: "select count(*),min(time),max(time) from %s where time > '2020-02-16 18:00:02' and time < '2020-02-17 17:00:00'",
			result:     []string{"2|2020-02-16 18:00:05.000000|2020-02-16 19:00:00.000000"},
		},
		{
			allegrosql: "select count(*),min(time),max(time) from %s where time > '2020-02-16 18:00:02' and time < '2020-02-17 20:00:00'",
			result:     []string{"3|2020-02-16 18:00:05.000000|2020-02-17 18:00:05.000000"},
		},
		{
			allegrosql: "select count(*),min(time),max(time) from %s",
			result:     []string{"1|2020-05-14 19:03:54.314615|2020-05-14 19:03:54.314615"},
		},
		{
			allegrosql: "select count(*),min(time) from %s where time > '2020-02-16 20:00:00'",
			result:     []string{"1|2020-02-17 18:00:05.000000"},
		},
		{
			allegrosql: "select count(*) from %s where time > '2020-02-17 20:00:00'",
			result:     []string{"0"},
		},
		{
			allegrosql: "select query from %s where time > '2020-01-26 21:51:00' and time < now()",
			result:     []string{"select 1;", "select 2;", "select 3;", "select 4;", "select 5;", "select 6;", "select 7;"},
		},
		// Test for different timezone.
		{
			prepareALLEGROSQL: "set @@time_zone = '+00:00'",
			allegrosql:        "select time from %s where time = '2020-02-17 10:00:05.000000'",
			result:            []string{"2020-02-17 10:00:05.000000"},
		},
		{
			prepareALLEGROSQL: "set @@time_zone = '+02:00'",
			allegrosql:        "select time from %s where time = '2020-02-17 12:00:05.000000'",
			result:            []string{"2020-02-17 12:00:05.000000"},
		},
		// Test for issue 17224
		{
			prepareALLEGROSQL: "set @@time_zone = '+08:00'",
			allegrosql:        "select time from %s where time = '2020-05-14 19:03:54.314615'",
			result:            []string{"2020-05-14 19:03:54.314615"},
		},
	}
	for _, cas := range cases {
		if len(cas.prepareALLEGROSQL) > 0 {
			tk.MustExec(cas.prepareALLEGROSQL)
		}
		allegrosql := fmt.Sprintf(cas.allegrosql, "slow_query")
		tk.MustQuery(allegrosql).Check(solitonutil.RowsWithSep("|", cas.result...))
		allegrosql = fmt.Sprintf(cas.allegrosql, "cluster_slow_query")
		tk.MustQuery(allegrosql).Check(solitonutil.RowsWithSep("|", cas.result...))
	}
}

func (s *testSuite1) TestIssue15718(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists tt;")
	tk.MustExec("create block tt(a decimal(10, 0), b varchar(1), c time);")
	tk.MustExec("insert into tt values(0, '2', null), (7, null, '1122'), (NULL, 'w', null), (NULL, '2', '3344'), (NULL, NULL, '0'), (7, 'f', '33');")
	tk.MustQuery("select a and b as d, a or c as e from tt;").Check(testkit.Rows("0 <nil>", "<nil> 1", "0 <nil>", "<nil> 1", "<nil> <nil>", "0 1"))

	tk.MustExec("drop block if exists tt;")
	tk.MustExec("create block tt(a decimal(10, 0), b varchar(1), c time);")
	tk.MustExec("insert into tt values(0, '2', '123'), (7, null, '1122'), (null, 'w', null);")
	tk.MustQuery("select a and b as d, a, b from tt order by d limit 1;").Check(testkit.Rows("<nil> 7 <nil>"))
	tk.MustQuery("select b or c as d, b, c from tt order by d limit 1;").Check(testkit.Rows("<nil> w <nil>"))

	tk.MustExec("drop block if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 FLOAT);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (NULL);")
	tk.MustQuery("SELECT * FROM t0 WHERE NOT(0 OR t0.c0);").Check(testkit.Rows())
}

func (s *testSuite1) TestIssue15767(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists tt;")
	tk.MustExec("create block t(a int, b char);")
	tk.MustExec("insert into t values (1,'s'),(2,'b'),(1,'c'),(2,'e'),(1,'a');")
	tk.MustExec("insert into t select * from t;")
	tk.MustExec("insert into t select * from t;")
	tk.MustExec("insert into t select * from t;")
	tk.MustQuery("select b, count(*) from ( select b from t order by a limit 20 offset 2) as s group by b order by b;").Check(testkit.Rows("a 6", "c 7", "s 7"))
}

func (s *testSuite1) TestIssue16025(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 NUMERIC PRIMARY KEY);")
	tk.MustExec("INSERT IGNORE INTO t0(c0) VALUES (NULL);")
	tk.MustQuery("SELECT * FROM t0 WHERE c0;").Check(testkit.Rows())
}

func (s *testSuite1) TestIssue16854(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("CREATE TABLE `t` (	`a` enum('WAITING','PRINTED','STOCKUP','CHECKED','OUTSTOCK','PICKEDUP','WILLBACK','BACKED') DEFAULT NULL)")
	tk.MustExec("insert into t values(1),(2),(3),(4),(5),(6),(7);")
	for i := 0; i < 7; i++ {
		tk.MustExec("insert into t select * from t;")
	}
	tk.MustExec("set @@milevadb_max_chunk_size=100;")
	tk.MustQuery("select distinct a from t order by a").Check(testkit.Rows("WAITING", "PRINTED", "STOCKUP", "CHECKED", "OUTSTOCK", "PICKEDUP", "WILLBACK"))
	tk.MustExec("drop block t")

	tk.MustExec("CREATE TABLE `t` (	`a` set('WAITING','PRINTED','STOCKUP','CHECKED','OUTSTOCK','PICKEDUP','WILLBACK','BACKED') DEFAULT NULL)")
	tk.MustExec("insert into t values(1),(2),(3),(4),(5),(6),(7);")
	for i := 0; i < 7; i++ {
		tk.MustExec("insert into t select * from t;")
	}
	tk.MustExec("set @@milevadb_max_chunk_size=100;")
	tk.MustQuery("select distinct a from t order by a").Check(testkit.Rows("WAITING", "PRINTED", "WAITING,PRINTED", "STOCKUP", "WAITING,STOCKUP", "PRINTED,STOCKUP", "WAITING,PRINTED,STOCKUP"))
	tk.MustExec("drop block t")
}

func (s *testSuite) TestIssue16921(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t (a float);")
	tk.MustExec("create index a on t(a);")
	tk.MustExec("insert into t values (1.0), (NULL), (0), (2.0);")
	tk.MustQuery("select `a` from `t` use index (a) where !`a`;").Check(testkit.Rows("0"))
	tk.MustQuery("select `a` from `t` ignore index (a) where !`a`;").Check(testkit.Rows("0"))
	tk.MustQuery("select `a` from `t` use index (a) where `a`;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select `a` from `t` ignore index (a) where `a`;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t use index (a) where not a is true;").Check(testkit.Rows("<nil>", "0"))
	tk.MustQuery("select a from t use index (a) where not not a is true;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t use index (a) where not not a;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t use index (a) where not not not a is true;").Check(testkit.Rows("<nil>", "0"))
	tk.MustQuery("select a from t use index (a) where not not not a;").Check(testkit.Rows("0"))
}

func (s *testSuite) TestIssue19100(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (c decimal);")
	tk.MustExec("create block t2 (c decimal, key(c));")
	tk.MustExec("insert into t1 values (null);")
	tk.MustExec("insert into t2 values (null);")
	tk.MustQuery("select count(*) from t1 where not c;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t2 where not c;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t1 where c;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t2 where c;").Check(testkit.Rows("0"))
}

// this is from jira issue #5856
func (s *testSuite1) TestInsertValuesWithSubQuery(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t2")
	tk.MustExec("create block t2(a int, b int, c int)")
	defer tk.MustExec("drop block if exists t2")

	// should not reference upper sINTERLOCKe
	c.Assert(tk.ExecToErr("insert into t2 values (11, 8, (select not b))"), NotNil)
	c.Assert(tk.ExecToErr("insert into t2 set a = 11, b = 8, c = (select b))"), NotNil)

	// subquery reference target block is allowed
	tk.MustExec("insert into t2 values(1, 1, (select b from t2))")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1 <nil>"))
	tk.MustExec("insert into t2 set a = 1, b = 1, c = (select b+1 from t2)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1 <nil>", "1 1 2"))

	// insert using defCausumn should work normally
	tk.MustExec("delete from t2")
	tk.MustExec("insert into t2 values(2, 4, a)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 4 2"))
	tk.MustExec("insert into t2 set a = 3, b = 5, c = b")
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 4 2", "3 5 5"))
}

func (s *testSuite1) TestDIVZeroInPartitionExpr(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(a int) partition by range (10 div a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	defer tk.MustExec("drop block if exists t1")

	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert into t1 values (NULL), (0), (1)")
	tk.MustExec("set @@sql_mode='STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO'")
	tk.MustGetErrCode("insert into t1 values (NULL), (0), (1)", allegrosql.ErrDivisionByZero)
}

func (s *testSuite1) TestInsertIntoGivenPartitionSet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t1")
	tk.MustExec(`create block t1(
	a int(11) DEFAULT NULL,
	b varchar(10) DEFAULT NULL,
	UNIQUE KEY idx_a (a)) PARTITION BY RANGE (a)
	(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,
	 PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,
	 PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB,
	 PARTITION p3 VALUES LESS THAN (40) ENGINE = InnoDB,
	 PARTITION p4 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)`)
	defer tk.MustExec("drop block if exists t1")

	// insert into
	tk.MustExec("insert into t1 partition(p0) values(1, 'a'), (2, 'b')")
	tk.MustQuery("select * from t1 partition(p0) order by a").Check(testkit.Rows("1 a", "2 b"))
	tk.MustExec("insert into t1 partition(p0, p1) values(3, 'c'), (4, 'd')")
	tk.MustQuery("select * from t1 partition(p1)").Check(testkit.Rows())

	err := tk.ExecToErr("insert into t1 values(1, 'a')")
	c.Assert(err.Error(), Equals, "[ekv:1062]Duplicate entry '1' for key 'idx_a'")

	err = tk.ExecToErr("insert into t1 partition(p0, p_non_exist) values(1, 'a')")
	c.Assert(err.Error(), Equals, "[block:1735]Unknown partition 'p_non_exist' in block 't1'")

	err = tk.ExecToErr("insert into t1 partition(p0, p1) values(40, 'a')")
	c.Assert(err.Error(), Equals, "[block:1748]Found a event not matching the given partition set")

	// replace into
	tk.MustExec("replace into t1 partition(p0) values(1, 'replace')")
	tk.MustExec("replace into t1 partition(p0, p1) values(3, 'replace'), (4, 'replace')")

	err = tk.ExecToErr("replace into t1 values(1, 'a')")
	tk.MustQuery("select * from t1 partition (p0) order by a").Check(testkit.Rows("1 a", "2 b", "3 replace", "4 replace"))

	err = tk.ExecToErr("replace into t1 partition(p0, p_non_exist) values(1, 'a')")
	c.Assert(err.Error(), Equals, "[block:1735]Unknown partition 'p_non_exist' in block 't1'")

	err = tk.ExecToErr("replace into t1 partition(p0, p1) values(40, 'a')")
	c.Assert(err.Error(), Equals, "[block:1748]Found a event not matching the given partition set")

	tk.MustExec("truncate block t1")

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b char(10))")
	defer tk.MustExec("drop block if exists t")

	// insert into general block
	err = tk.ExecToErr("insert into t partition(p0, p1) values(1, 'a')")
	c.Assert(err.Error(), Equals, "[planner:1747]PARTITION () clause on non partitioned block")

	// insert into from select
	tk.MustExec("insert into t values(1, 'a'), (2, 'b')")
	tk.MustExec("insert into t1 partition(p0) select * from t")
	tk.MustQuery("select * from t1 partition(p0) order by a").Check(testkit.Rows("1 a", "2 b"))

	tk.MustExec("truncate block t")
	tk.MustExec("insert into t values(3, 'c'), (4, 'd')")
	tk.MustExec("insert into t1 partition(p0, p1) select * from t")
	tk.MustQuery("select * from t1 partition(p1) order by a").Check(testkit.Rows())
	tk.MustQuery("select * from t1 partition(p0) order by a").Check(testkit.Rows("1 a", "2 b", "3 c", "4 d"))

	err = tk.ExecToErr("insert into t1 select 1, 'a'")
	c.Assert(err.Error(), Equals, "[ekv:1062]Duplicate entry '1' for key 'idx_a'")

	err = tk.ExecToErr("insert into t1 partition(p0, p_non_exist) select 1, 'a'")
	c.Assert(err.Error(), Equals, "[block:1735]Unknown partition 'p_non_exist' in block 't1'")

	err = tk.ExecToErr("insert into t1 partition(p0, p1) select 40, 'a'")
	c.Assert(err.Error(), Equals, "[block:1748]Found a event not matching the given partition set")

	// replace into from select
	tk.MustExec("replace into t1 partition(p0) select 1, 'replace'")
	tk.MustExec("truncate block t")
	tk.MustExec("insert into t values(3, 'replace'), (4, 'replace')")
	tk.MustExec("replace into t1 partition(p0, p1) select * from t")

	err = tk.ExecToErr("replace into t1 values select 1, 'a'")
	tk.MustQuery("select * from t1 partition (p0) order by a").Check(testkit.Rows("1 replace", "2 b", "3 replace", "4 replace"))

	err = tk.ExecToErr("replace into t1 partition(p0, p_non_exist) select 1, 'a'")
	c.Assert(err.Error(), Equals, "[block:1735]Unknown partition 'p_non_exist' in block 't1'")

	err = tk.ExecToErr("replace into t1 partition(p0, p1) select 40, 'a'")
	c.Assert(err.Error(), Equals, "[block:1748]Found a event not matching the given partition set")
}

func (s *testSuite1) TestUFIDelateGivenPartitionSet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t1,t2,t3")
	tk.MustExec(`create block t1(
	a int(11),
	b varchar(10) DEFAULT NULL,
	primary key idx_a (a)) PARTITION BY RANGE (a)
	(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,
	 PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,
	 PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB,
	 PARTITION p3 VALUES LESS THAN (40) ENGINE = InnoDB,
	 PARTITION p4 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)`)

	tk.MustExec(`create block t2(
	a int(11) DEFAULT NULL,
	b varchar(10) DEFAULT NULL) PARTITION BY RANGE (a)
	(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,
	 PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,
	 PARTITION p2 VALUES LESS THAN (30) ENGINE = InnoDB,
	 PARTITION p3 VALUES LESS THAN (40) ENGINE = InnoDB,
	 PARTITION p4 VALUES LESS THAN MAXVALUE ENGINE = InnoDB)`)

	tk.MustExec(`create block t3 (a int(11), b varchar(10) default null)`)

	defer tk.MustExec("drop block if exists t1,t2,t3")
	tk.MustExec("insert into t3 values(1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')")
	err := tk.ExecToErr("uFIDelate t3 partition(p0) set a = 40 where a = 2")
	c.Assert(err.Error(), Equals, "[planner:1747]PARTITION () clause on non partitioned block")

	// uFIDelate with primary key change
	tk.MustExec("insert into t1 values(1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')")
	err = tk.ExecToErr("uFIDelate t1 partition(p0, p1) set a = 40")
	c.Assert(err.Error(), Equals, "[block:1748]Found a event not matching the given partition set")
	err = tk.ExecToErr("uFIDelate t1 partition(p0) set a = 40 where a = 2")
	c.Assert(err.Error(), Equals, "[block:1748]Found a event not matching the given partition set")
	// test non-exist partition.
	err = tk.ExecToErr("uFIDelate t1 partition (p0, p_non_exist) set a = 40")
	c.Assert(err.Error(), Equals, "[block:1735]Unknown partition 'p_non_exist' in block 't1'")
	// test join.
	err = tk.ExecToErr("uFIDelate t1 partition (p0), t3 set t1.a = 40 where t3.a = 2")
	c.Assert(err.Error(), Equals, "[block:1748]Found a event not matching the given partition set")

	tk.MustExec("uFIDelate t1 partition(p0) set a = 3 where a = 2")
	tk.MustExec("uFIDelate t1 partition(p0, p3) set a = 33 where a = 1")

	// uFIDelate without partition change
	tk.MustExec("insert into t2 values(1, 'a'), (2, 'b'), (11, 'c'), (21, 'd')")
	err = tk.ExecToErr("uFIDelate t2 partition(p0, p1) set a = 40")
	c.Assert(err.Error(), Equals, "[block:1748]Found a event not matching the given partition set")
	err = tk.ExecToErr("uFIDelate t2 partition(p0) set a = 40 where a = 2")
	c.Assert(err.Error(), Equals, "[block:1748]Found a event not matching the given partition set")

	tk.MustExec("uFIDelate t2 partition(p0) set a = 3 where a = 2")
	tk.MustExec("uFIDelate t2 partition(p0, p3) set a = 33 where a = 1")
}

func (s *testSuiteP2) TestApplyCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a int);")
	tk.MustExec("insert into t values (1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("analyze block t;")
	result := tk.MustQuery("explain analyze SELECT count(1) FROM (SELECT (SELECT min(a) FROM t as t2 WHERE t2.a > t1.a) AS a from t as t1) t;")
	c.Assert(result.Rows()[1][0], Equals, "└─Apply_41")
	var (
		ind  int
		flag bool
	)
	value := (result.Rows()[1][5]).(string)
	for ind = 0; ind < len(value)-5; ind++ {
		if value[ind:ind+5] == "cache" {
			flag = true
			break
		}
	}
	c.Assert(flag, Equals, true)
	c.Assert(value[ind:], Equals, "cache:ON, cacheHitRatio:88.889%")

	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a int);")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5),(6),(7),(8),(9);")
	tk.MustExec("analyze block t;")
	result = tk.MustQuery("explain analyze SELECT count(1) FROM (SELECT (SELECT min(a) FROM t as t2 WHERE t2.a > t1.a) AS a from t as t1) t;")
	c.Assert(result.Rows()[1][0], Equals, "└─Apply_41")
	flag = false
	value = (result.Rows()[1][5]).(string)
	for ind = 0; ind < len(value)-5; ind++ {
		if value[ind:ind+5] == "cache" {
			flag = true
			break
		}
	}
	c.Assert(flag, Equals, true)
	c.Assert(value[ind:], Equals, "cache:OFF")
}

// For issue 17256
func (s *testSuite) TestGenerateDeferredCausetReplace(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (a int, b int as (a + 1) virtual not null, unique index idx(b));")
	tk.MustExec("REPLACE INTO `t1` (`a`) VALUES (2);")
	tk.MustExec("REPLACE INTO `t1` (`a`) VALUES (2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("2 3"))
	tk.MustExec("insert into `t1` (`a`) VALUES (2) on duplicate key uFIDelate a = 3;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("3 4"))
}

func (s *testSlowQuery) TestSlowQueryWithoutSlowLog(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Log.SlowQueryFile = "milevadb-slow-not-exist.log"
	newCfg.Log.SlowThreshold = math.MaxUint64
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()
	tk.MustQuery("select query from information_schema.slow_query").Check(testkit.Rows())
	tk.MustQuery("select query from information_schema.slow_query where time > '2020-09-15 12:16:39' and time < now()").Check(testkit.Rows())
}

func (s *testSlowQuery) TestSlowQuerySensitiveQuery(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg

	f, err := ioutil.TempFile("", "milevadb-slow-*.log")
	c.Assert(err, IsNil)
	f.Close()
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		tk.MustExec("set milevadb_slow_log_threshold=300;")
		config.StoreGlobalConfig(originCfg)
		os.Remove(newCfg.Log.SlowQueryFile)
	}()
	err = logutil.InitLogger(newCfg.Log.ToLogConfig())
	c.Assert(err, IsNil)

	tk.MustExec("set milevadb_slow_log_threshold=0;")
	tk.MustExec("drop user if exists user_sensitive;")
	tk.MustExec("create user user_sensitive identified by '123456789';")
	tk.MustExec("alter user 'user_sensitive'@'%' identified by 'abcdefg';")
	tk.MustExec("set password for 'user_sensitive'@'%' = 'xyzuvw';")
	tk.MustQuery("select query from `information_schema`.`slow_query` " +
		"where (query like 'set password%' or query like 'create user%' or query like 'alter user%') " +
		"and query like '%user_sensitive%' order by query;").
		Check(testkit.Rows(
			"alter user {user_sensitive@% password = ***};",
			"create user {user_sensitive@% password = ***};",
			"set password for user user_sensitive@%;",
		))
}

func (s *testSerialSuite) TestKillBlockReader(c *C) {
	var retry = "github.com/whtcorpsinc/milevadb/causetstore/einsteindb/mockRetrySendReqToRegion"
	defer func() {
		c.Assert(failpoint.Disable(retry), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int)")
	tk.MustExec("insert into t values (1),(2),(3)")
	tk.MustExec("set @@milevadb_distsql_scan_concurrency=1")
	atomic.StoreUint32(&tk.Se.GetStochastikVars().Killed, 0)
	c.Assert(failpoint.Enable(retry, `return(true)`), IsNil)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second)
		err := tk.QueryToErr("select * from t")
		c.Assert(err, NotNil)
		c.Assert(int(terror.ToALLEGROSQLError(errors.Cause(err).(*terror.Error)).Code), Equals, int(executor.ErrQueryInterrupted.Code()))
	}()
	atomic.StoreUint32(&tk.Se.GetStochastikVars().Killed, 1)
	wg.Wait()
}

func (s *testSerialSuite) TestPrevStmtDesensitization(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	oriCfg := config.GetGlobalConfig()
	defer config.StoreGlobalConfig(oriCfg)
	newCfg := *oriCfg
	newCfg.EnableRedactLog = 1
	config.StoreGlobalConfig(&newCfg)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int)")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1),(2)")
	c.Assert(tk.Se.GetStochastikVars().PrevStmt.String(), Equals, "insert into t values ( ? ) , ( ? )")
}

func (s *testSuite) TestIssue19372(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (c_int int, c_str varchar(40), key(c_str));")
	tk.MustExec("create block t2 like t1;")
	tk.MustExec("insert into t1 values (1, 'a'), (2, 'b'), (3, 'c');")
	tk.MustExec("insert into t2 select * from t1;")
	tk.MustQuery("select (select t2.c_str from t2 where t2.c_str <= t1.c_str and t2.c_int in (1, 2) order by t2.c_str limit 1) x from t1 order by c_int;").Check(testkit.Rows("a", "a", "a"))
}

func (s *testSerialSuite1) TestDefCauslectINTERLOCKRuntimeStats(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (a int, b int)")
	tk.MustExec("set milevadb_enable_defCauslect_execution_info=1;")
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbStoreRespResult", `return(true)`), IsNil)
	rows := tk.MustQuery("explain analyze select * from t1").Rows()
	c.Assert(len(rows), Equals, 2)
	explain := fmt.Sprintf("%v", rows[0])
	c.Assert(explain, Matches, ".*rpc_num: 2, .*regionMiss:.*")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbStoreRespResult"), IsNil)
}

func (s *testSuite) TestDefCauslectDMLRuntimeStats(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (a int, b int, unique index (a))")

	testALLEGROSQLs := []string{
		"insert ignore into t1 values (5,5);",
		"insert into t1 values (5,5) on duplicate key uFIDelate a=a+1;",
		"replace into t1 values (5,6),(6,7)",
		"uFIDelate t1 set a=a+1 where a=6;",
	}

	getRootStats := func() string {
		info := tk.Se.ShowProcess()
		c.Assert(info, NotNil)
		p, ok := info.Plan.(plannercore.Plan)
		c.Assert(ok, IsTrue)
		stats := tk.Se.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.GetRootStats(p.ID())
		return stats.String()
	}
	for _, allegrosql := range testALLEGROSQLs {
		tk.MustExec(allegrosql)
		c.Assert(getRootStats(), Matches, "time.*loops.*Get.*num_rpc.*total_time.*")
	}

	// Test for dagger keys stats.
	tk.MustExec("begin pessimistic")
	tk.MustExec("uFIDelate t1 set b=b+1")
	c.Assert(getRootStats(), Matches, "time.*lock_keys.*time.* region.* keys.* lock_rpc:.* rpc_count.*")
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1 for uFIDelate").Check(testkit.Rows("5 6", "7 7"))
	c.Assert(getRootStats(), Matches, "time.*lock_keys.*time.* region.* keys.* lock_rpc:.* rpc_count.*")
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert ignore into t1 values (9,9)")
	c.Assert(getRootStats(), Matches, "time:.*, loops:.*, BatchGet:{num_rpc:.*, total_time:.*}, lock_keys: {time:.*, region:.*, keys:.*, lock_rpc:.*, rpc_count:.*}")
	tk.MustExec("rollback")
}

func (s *testSuite) TestIssue13758(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1 (pk int(11) primary key, a int(11) not null, b int(11), key idx_b(b), key idx_a(a))")
	tk.MustExec("insert into `t1` values (1,1,0),(2,7,6),(3,2,null),(4,1,null),(5,4,5)")
	tk.MustExec("create block t2 (a int)")
	tk.MustExec("insert into t2 values (1),(null)")
	tk.MustQuery("select (select a from t1 use index(idx_a) where b >= t2.a order by a limit 1) as field from t2").Check(testkit.Rows(
		"4",
		"<nil>",
	))
}
