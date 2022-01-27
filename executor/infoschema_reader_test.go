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
	"crypto/tls"
	"fmt"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fn"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/helper"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/server"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/FIDelapi"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"google.golang.org/grpc"
)

var _ = Suite(&testschemaReplicantBlockSuite{})

// this SerialSuites is used to solve the data race caused by BlockStatsCacheExpiry,
// if your test not change the BlockStatsCacheExpiry variable, please use testschemaReplicantBlockSuite for test.
var _ = SerialSuites(&testschemaReplicantBlockSerialSuite{})

var _ = SerialSuites(&inspectionSuite{})

type testschemaReplicantBlockSuiteBase struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

type testschemaReplicantBlockSuite struct {
	testschemaReplicantBlockSuiteBase
}

type testschemaReplicantBlockSerialSuite struct {
	testschemaReplicantBlockSuiteBase
}

type inspectionSuite struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

func (s *testschemaReplicantBlockSuiteBase) SetUpSuite(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	s.dom = dom
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.OOMCausetAction = config.OOMCausetActionLog
	})
}

func (s *testschemaReplicantBlockSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *inspectionSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	stochastik.DisableStats4Test()
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *inspectionSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
	testleak.AfterTest(c)()
}

func (s *inspectionSuite) TestInspectionBlocks(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	instances := []string{
		"fidel,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash",
		"milevadb,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash",
		"einsteindb,127.0.0.1:11080,127.0.0.1:10080,mock-version,mock-githash",
	}
	fpName := "github.com/whtcorpsinc/milevadb/schemareplicant/mockClusterInfo"
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	c.Assert(failpoint.Enable(fpName, fpExpr), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Events(
		"fidel 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"milevadb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"einsteindb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
	))

	// enable inspection mode
	inspectionBlockCache := map[string]variable.BlockSnapshot{}
	tk.Se.GetStochastikVars().InspectionBlockCache = inspectionBlockCache
	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Events(
		"fidel 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"milevadb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"einsteindb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
	))
	c.Assert(inspectionBlockCache["cluster_info"].Err, IsNil)
	c.Assert(len(inspectionBlockCache["cluster_info"].Events), DeepEquals, 3)

	// check whether is obtain data from cache at the next time
	inspectionBlockCache["cluster_info"].Events[0][0].SetString("modified-fidel", allegrosql.DefaultDefCauslationName)
	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Events(
		"modified-fidel 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"milevadb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
		"einsteindb 127.0.0.1:11080 127.0.0.1:10080 mock-version mock-githash",
	))
	tk.Se.GetStochastikVars().InspectionBlockCache = nil
}

func (s *testschemaReplicantBlockSuite) TestProfiling(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select * from information_schema.profiling").Check(testkit.Events())
	tk.MustExec("set @@profiling=1")
	tk.MustQuery("select * from information_schema.profiling").Check(testkit.Events("0 0  0 0 0 0 0 0 0 0 0 0 0 0   0"))
}

func (s *testschemaReplicantBlockSuite) TestSchemataBlocks(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustQuery("select * from information_schema.SCHEMATA where schema_name='allegrosql';").Check(
		testkit.Events("def allegrosql utf8mb4 utf8mb4_bin <nil>"))

	// Test the privilege of new user for information_schema.schemata.
	tk.MustExec("create user schemata_tester")
	schemataTester := testkit.NewTestKit(c, s.causetstore)
	schemataTester.MustExec("use information_schema")
	c.Assert(schemataTester.Se.Auth(&auth.UserIdentity{
		Username: "schemata_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	schemataTester.MustQuery("select count(*) from information_schema.SCHEMATA;").Check(testkit.Events("1"))
	schemataTester.MustQuery("select * from information_schema.SCHEMATA where schema_name='allegrosql';").Check(
		[][]interface{}{})
	schemataTester.MustQuery("select * from information_schema.SCHEMATA where schema_name='INFORMATION_SCHEMA';").Check(
		testkit.Events("def INFORMATION_SCHEMA utf8mb4 utf8mb4_bin <nil>"))

	// Test the privilege of user with privilege of allegrosql for information_schema.schemata.
	tk.MustExec("CREATE ROLE r_mysql_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON allegrosql.* TO r_mysql_priv;")
	tk.MustExec("GRANT r_mysql_priv TO schemata_tester;")
	schemataTester.MustExec("set role r_mysql_priv")
	schemataTester.MustQuery("select count(*) from information_schema.SCHEMATA;").Check(testkit.Events("2"))
	schemataTester.MustQuery("select * from information_schema.SCHEMATA;").Check(
		testkit.Events("def INFORMATION_SCHEMA utf8mb4 utf8mb4_bin <nil>", "def allegrosql utf8mb4 utf8mb4_bin <nil>"))
}

func (s *testschemaReplicantBlockSuite) TestBlockIDAndIndexID(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop block if exists test.t")
	tk.MustExec("create block test.t (a int, b int, primary key(a), key k1(b))")
	tk.MustQuery("select index_id from information_schema.milevadb_indexes where block_schema = 'test' and block_name = 't'").Check(testkit.Events("0", "1"))
	tblID, err := strconv.Atoi(tk.MustQuery("select milevadb_block_id from information_schema.blocks where block_schema = 'test' and block_name = 't'").Events()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(tblID, Greater, 0)
}

func (s *testschemaReplicantBlockSuite) TestSchemataCharacterSet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("CREATE DATABASE `foo` DEFAULT CHARACTER SET = 'utf8mb4'")
	tk.MustQuery("select default_character_set_name, default_defCauslation_name FROM information_schema.SCHEMATA  WHERE schema_name = 'foo'").Check(
		testkit.Events("utf8mb4 utf8mb4_bin"))
	tk.MustExec("drop database `foo`")
}

func (s *testschemaReplicantBlockSuite) TestViews(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("CREATE DEFINER='root'@'localhost' VIEW test.v1 AS SELECT 1")
	tk.MustQuery("SELECT * FROM information_schema.views WHERE block_schema='test' AND block_name='v1'").Check(testkit.Events("def test v1 SELECT 1 CASCADED NO root@localhost DEFINER utf8mb4 utf8mb4_bin"))
	tk.MustQuery("SELECT block_catalog, block_schema, block_name, block_type, engine, version, row_format, block_rows, avg_row_length, data_length, max_data_length, index_length, data_free, auto_increment, uFIDelate_time, check_time, block_defCauslation, checksum, create_options, block_comment FROM information_schema.blocks WHERE block_schema='test' AND block_name='v1'").Check(testkit.Events("def test v1 VIEW <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> VIEW"))
}

func (s *testschemaReplicantBlockSuite) TestEngines(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select * from information_schema.ENGINES;").Check(testkit.Events("InnoDB DEFAULT Supports transactions, event-level locking, and foreign keys YES YES YES"))
}

func (s *testschemaReplicantBlockSuite) TestCharacterSetDefCauslations(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	// The description defCausumn is not important
	tk.MustQuery("SELECT default_defCauslate_name, maxlen FROM information_schema.character_sets ORDER BY character_set_name").Check(
		testkit.Events("ascii_bin 1", "binary 1", "latin1_bin 1", "utf8_bin 3", "utf8mb4_bin 4"))

	// The is_default defCausumn is not important
	// but the id's are used by client libraries and must be sblock
	tk.MustQuery("SELECT character_set_name, id, sortlen FROM information_schema.defCauslations ORDER BY defCauslation_name").Check(
		testkit.Events("ascii 65 1", "binary 63 1", "latin1 47 1", "utf8 83 1", "utf8mb4 46 1"))

	tk.MustQuery("select * from information_schema.COLLATION_CHARACTER_SET_APPLICABILITY where COLLATION_NAME='utf8mb4_bin';").Check(
		testkit.Events("utf8mb4_bin utf8mb4"))
}

func (s *testschemaReplicantBlockSuite) TestDBSJobs(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists test_dbs_jobs")
	tk.MustQuery("select db_name, job_type from information_schema.DBS_JOBS limit 1").Check(
		testkit.Events("test_dbs_jobs create schemaReplicant"))

	tk.MustExec("use test_dbs_jobs")
	tk.MustExec("create block t (a int);")
	tk.MustQuery("select db_name, block_name, job_type from information_schema.DBS_JOBS where block_name = 't'").Check(
		testkit.Events("test_dbs_jobs t create block"))

	tk.MustQuery("select job_type from information_schema.DBS_JOBS group by job_type having job_type = 'create block'").Check(
		testkit.Events("create block"))

	// Test the START_TIME and END_TIME field.
	tk.MustQuery("select distinct job_type from information_schema.DBS_JOBS where job_type = 'create block' and start_time > str_to_date('20190101','%Y%m%d%H%i%s')").Check(
		testkit.Events("create block"))

	// Test the privilege of new user for information_schema.DBS_JOBS.
	tk.MustExec("create user DBS_JOBS_tester")
	DBSJobsTester := testkit.NewTestKit(c, s.causetstore)
	DBSJobsTester.MustExec("use information_schema")
	c.Assert(DBSJobsTester.Se.Auth(&auth.UserIdentity{
		Username: "DBS_JOBS_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)

	// Test the privilege of user for information_schema.dbs_jobs.
	DBSJobsTester.MustQuery("select DB_NAME, TABLE_NAME from information_schema.DBS_JOBS where DB_NAME = 'test_dbs_jobs' and TABLE_NAME = 't';").Check(
		[][]interface{}{})
	tk.MustExec("CREATE ROLE r_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON test_dbs_jobs.* TO r_priv;")
	tk.MustExec("GRANT r_priv TO DBS_JOBS_tester;")
	DBSJobsTester.MustExec("set role r_priv")
	DBSJobsTester.MustQuery("select DB_NAME, TABLE_NAME from information_schema.DBS_JOBS where DB_NAME = 'test_dbs_jobs' and TABLE_NAME = 't';").Check(
		testkit.Events("test_dbs_jobs t"))
}

func (s *testschemaReplicantBlockSuite) TestKeyDeferredCausetUsage(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustQuery("select * from information_schema.KEY_COLUMN_USAGE where TABLE_NAME='stats_meta' and COLUMN_NAME='block_id';").Check(
		testkit.Events("def allegrosql tbl def allegrosql stats_meta block_id 1 <nil> <nil> <nil> <nil>"))

	//test the privilege of new user for information_schema.block_constraints
	tk.MustExec("create user key_defCausumn_tester")
	keyDeferredCausetTester := testkit.NewTestKit(c, s.causetstore)
	keyDeferredCausetTester.MustExec("use information_schema")
	c.Assert(keyDeferredCausetTester.Se.Auth(&auth.UserIdentity{
		Username: "key_defCausumn_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	keyDeferredCausetTester.MustQuery("select * from information_schema.KEY_COLUMN_USAGE;").Check([][]interface{}{})

	//test the privilege of user with privilege of allegrosql.gc_delete_range for information_schema.block_constraints
	tk.MustExec("CREATE ROLE r_stats_meta ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON allegrosql.stats_meta TO r_stats_meta;")
	tk.MustExec("GRANT r_stats_meta TO key_defCausumn_tester;")
	keyDeferredCausetTester.MustExec("set role r_stats_meta")
	c.Assert(len(keyDeferredCausetTester.MustQuery("select * from information_schema.KEY_COLUMN_USAGE where TABLE_NAME='stats_meta';").Events()), Greater, 0)
}

func (s *testschemaReplicantBlockSuite) TestUserPrivileges(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	//test the privilege of new user for information_schema.block_constraints
	tk.MustExec("create user constraints_tester")
	constraintsTester := testkit.NewTestKit(c, s.causetstore)
	constraintsTester.MustExec("use information_schema")
	c.Assert(constraintsTester.Se.Auth(&auth.UserIdentity{
		Username: "constraints_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS;").Check([][]interface{}{})

	//test the privilege of user with privilege of allegrosql.gc_delete_range for information_schema.block_constraints
	tk.MustExec("CREATE ROLE r_gc_delete_range ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON allegrosql.gc_delete_range TO r_gc_delete_range;")
	tk.MustExec("GRANT r_gc_delete_range TO constraints_tester;")
	constraintsTester.MustExec("set role r_gc_delete_range")
	c.Assert(len(constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='gc_delete_range';").Events()), Greater, 0)
	constraintsTester.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='blocks_priv';").Check([][]interface{}{})

	//test the privilege of new user for information_schema
	tk.MustExec("create user tester1")
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("use information_schema")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{
		Username: "tester1",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	tk1.MustQuery("select * from information_schema.STATISTICS;").Check([][]interface{}{})

	//test the privilege of user with some privilege for information_schema
	tk.MustExec("create user tester2")
	tk.MustExec("CREATE ROLE r_defCausumns_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON allegrosql.defCausumns_priv TO r_defCausumns_priv;")
	tk.MustExec("GRANT r_defCausumns_priv TO tester2;")
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustExec("use information_schema")
	c.Assert(tk2.Se.Auth(&auth.UserIdentity{
		Username: "tester2",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	tk2.MustExec("set role r_defCausumns_priv")
	result := tk2.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='defCausumns_priv' and COLUMN_NAME='Host';")
	c.Assert(len(result.Events()), Greater, 0)
	tk2.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='blocks_priv' and COLUMN_NAME='Host';").Check(
		[][]interface{}{})

	//test the privilege of user with all privilege for information_schema
	tk.MustExec("create user tester3")
	tk.MustExec("CREATE ROLE r_all_priv;")
	tk.MustExec("GRANT ALL PRIVILEGES ON allegrosql.* TO r_all_priv;")
	tk.MustExec("GRANT r_all_priv TO tester3;")
	tk3 := testkit.NewTestKit(c, s.causetstore)
	tk3.MustExec("use information_schema")
	c.Assert(tk3.Se.Auth(&auth.UserIdentity{
		Username: "tester3",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	tk3.MustExec("set role r_all_priv")
	result = tk3.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='defCausumns_priv' and COLUMN_NAME='Host';")
	c.Assert(len(result.Events()), Greater, 0)
	result = tk3.MustQuery("select * from information_schema.STATISTICS where TABLE_NAME='blocks_priv' and COLUMN_NAME='Host';")
	c.Assert(len(result.Events()), Greater, 0)
}

func (s *testschemaReplicantBlockSerialSuite) TestDataForBlockStatsField(c *C) {
	s.dom.SetStatsUFIDelating(true)
	oldExpiryTime := executor.BlockStatsCacheExpiry
	executor.BlockStatsCacheExpiry = 0
	defer func() { executor.BlockStatsCacheExpiry = oldExpiryTime }()
	do := s.dom
	h := do.StatsHandle()
	h.Clear()
	is := do.SchemaReplicant()
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c int, d int, e char(5), index idx(e))")
	h.HandleDBSEvent(<-h.DBSEventCh())
	tk.MustQuery("select block_rows, avg_row_length, data_length, index_length from information_schema.blocks where block_name='t'").Check(
		testkit.Events("0 0 0 0"))
	tk.MustExec(`insert into t(c, d, e) values(1, 2, "c"), (2, 3, "d"), (3, 4, "e")`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	tk.MustQuery("select block_rows, avg_row_length, data_length, index_length from information_schema.blocks where block_name='t'").Check(
		testkit.Events("3 18 54 6"))
	tk.MustExec(`insert into t(c, d, e) values(4, 5, "f")`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	tk.MustQuery("select block_rows, avg_row_length, data_length, index_length from information_schema.blocks where block_name='t'").Check(
		testkit.Events("4 18 72 8"))
	tk.MustExec("delete from t where c >= 3")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	tk.MustQuery("select block_rows, avg_row_length, data_length, index_length from information_schema.blocks where block_name='t'").Check(
		testkit.Events("2 18 36 4"))
	tk.MustExec("delete from t where c=3")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	tk.MustQuery("select block_rows, avg_row_length, data_length, index_length from information_schema.blocks where block_name='t'").Check(
		testkit.Events("2 18 36 4"))

	// Test partition block.
	tk.MustExec("drop block if exists t")
	tk.MustExec(`CREATE TABLE t (a int, b int, c varchar(5), primary key(a), index idx(c)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16))`)
	h.HandleDBSEvent(<-h.DBSEventCh())
	tk.MustExec(`insert into t(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e")`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	tk.MustQuery("select block_rows, avg_row_length, data_length, index_length from information_schema.blocks where block_name='t'").Check(
		testkit.Events("3 18 54 6"))
}

func (s *testschemaReplicantBlockSerialSuite) TestPartitionsBlock(c *C) {
	s.dom.SetStatsUFIDelating(true)
	oldExpiryTime := executor.BlockStatsCacheExpiry
	executor.BlockStatsCacheExpiry = 0
	defer func() { executor.BlockStatsCacheExpiry = oldExpiryTime }()
	do := s.dom
	h := do.StatsHandle()
	h.Clear()
	is := do.SchemaReplicant()

	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS `test_partitions`;")
	tk.MustExec(`CREATE TABLE test_partitions (a int, b int, c varchar(5), primary key(a), index idx(c)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16));`)
	err := h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	tk.MustExec(`insert into test_partitions(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e");`)

	tk.MustQuery("select PARTITION_NAME, PARTITION_DESCRIPTION from information_schema.PARTITIONS where block_name='test_partitions';").Check(
		testkit.Events("" +
			"p0 6]\n" +
			"[p1 11]\n" +
			"[p2 16"))

	tk.MustQuery("select block_rows, avg_row_length, data_length, index_length from information_schema.PARTITIONS where block_name='test_partitions';").Check(
		testkit.Events("" +
			"0 0 0 0]\n" +
			"[0 0 0 0]\n" +
			"[0 0 0 0"))
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	tk.MustQuery("select block_rows, avg_row_length, data_length, index_length from information_schema.PARTITIONS where block_name='test_partitions';").Check(
		testkit.Events("" +
			"1 18 18 2]\n" +
			"[1 18 18 2]\n" +
			"[1 18 18 2"))

	// Test for block has no partitions.
	tk.MustExec("DROP TABLE IF EXISTS `test_partitions_1`;")
	tk.MustExec(`CREATE TABLE test_partitions_1 (a int, b int, c varchar(5), primary key(a), index idx(c));`)
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	tk.MustExec(`insert into test_partitions_1(a, b, c) values(1, 2, "c"), (7, 3, "d"), (12, 4, "e");`)
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	tk.MustQuery("select PARTITION_NAME, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH from information_schema.PARTITIONS where block_name='test_partitions_1';").Check(
		testkit.Events("<nil> 3 18 54 6"))

	tk.MustExec("DROP TABLE `test_partitions`;")

	tk.MustExec(`CREATE TABLE test_partitions1 (id int, b int, c varchar(5), primary key(id), index idx(c)) PARTITION BY RANGE COLUMNS(id) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16));`)
	tk.MustQuery("select PARTITION_NAME,PARTITION_METHOD,PARTITION_EXPRESSION from information_schema.partitions where block_name = 'test_partitions1';").Check(testkit.Events("p0 RANGE COLUMNS id", "p1 RANGE COLUMNS id", "p2 RANGE COLUMNS id"))
	tk.MustExec("DROP TABLE test_partitions1")
}

func (s *testschemaReplicantBlockSuite) TestMetricBlocks(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	statistics.ClearHistoryJobs()
	tk.MustExec("use information_schema")
	tk.MustQuery("select count(*) > 0 from `METRICS_TABLES`").Check(testkit.Events("1"))
	tk.MustQuery("select * from `METRICS_TABLES` where block_name='milevadb_qps'").
		Check(solitonutil.EventsWithSep("|", "milevadb_qps|sum(rate(milevadb_server_query_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (result,type,instance)|instance,type,result|0|MilevaDB query processing numbers per second"))
}

func (s *testschemaReplicantBlockSuite) TestBlockConstraintsBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select * from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='gc_delete_range';").Check(testkit.Events("def allegrosql delete_range_index allegrosql gc_delete_range UNIQUE"))
}

func (s *testschemaReplicantBlockSuite) TestBlockStochastikVar(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select * from information_schema.SESSION_VARIABLES where VARIABLE_NAME='milevadb_retry_limit';").Check(testkit.Events("milevadb_retry_limit 10"))
}

func (s *testschemaReplicantBlockSuite) TestForAnalyzeStatus(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	statistics.ClearHistoryJobs()
	tk.MustExec("use test")
	tk.MustExec("drop block if exists analyze_test")
	tk.MustExec("create block analyze_test (a int, b int, index idx(a))")
	tk.MustExec("insert into analyze_test values (1,2),(3,4)")

	tk.MustQuery("select distinct TABLE_NAME from information_schema.analyze_status where TABLE_NAME='analyze_test'").Check([][]interface{}{})
	tk.MustExec("analyze block analyze_test")
	tk.MustQuery("select distinct TABLE_NAME from information_schema.analyze_status where TABLE_NAME='analyze_test'").Check(testkit.Events("analyze_test"))

	//test the privilege of new user for information_schema.analyze_status
	tk.MustExec("create user analyze_tester")
	analyzeTester := testkit.NewTestKit(c, s.causetstore)
	analyzeTester.MustExec("use information_schema")
	c.Assert(analyzeTester.Se.Auth(&auth.UserIdentity{
		Username: "analyze_tester",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	analyzeTester.MustQuery("show analyze status").Check([][]interface{}{})
	analyzeTester.MustQuery("select * from information_schema.ANALYZE_STATUS;").Check([][]interface{}{})

	//test the privilege of user with privilege of test.t1 for information_schema.analyze_status
	tk.MustExec("create block t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,2),(3,4)")
	tk.MustExec("analyze block t1")
	tk.MustExec("CREATE ROLE r_t1 ;")
	tk.MustExec("GRANT ALL PRIVILEGES ON test.t1 TO r_t1;")
	tk.MustExec("GRANT r_t1 TO analyze_tester;")
	analyzeTester.MustExec("set role r_t1")
	resultT1 := tk.MustQuery("select * from information_schema.analyze_status where TABLE_NAME='t1'").Sort()
	c.Assert(len(resultT1.Events()), Greater, 0)
}

func (s *testschemaReplicantBlockSerialSuite) TestForServersInfo(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	result := tk.MustQuery("select * from information_schema.MilevaDB_SERVERS_INFO")
	c.Assert(len(result.Events()), Equals, 1)

	info, err := infosync.GetServerInfo()
	c.Assert(err, IsNil)
	c.Assert(info, NotNil)
	c.Assert(result.Events()[0][0], Equals, info.ID)
	c.Assert(result.Events()[0][1], Equals, info.IP)
	c.Assert(result.Events()[0][2], Equals, strconv.FormatInt(int64(info.Port), 10))
	c.Assert(result.Events()[0][3], Equals, strconv.FormatInt(int64(info.StatusPort), 10))
	c.Assert(result.Events()[0][4], Equals, info.Lease)
	c.Assert(result.Events()[0][5], Equals, info.Version)
	c.Assert(result.Events()[0][6], Equals, info.GitHash)
	c.Assert(result.Events()[0][7], Equals, info.BinlogStatus)
	c.Assert(result.Events()[0][8], Equals, stringutil.BuildStringFromLabels(info.Labels))
}

func (s *testschemaReplicantBlockSerialSuite) TestForBlockTiFlashReplica(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount")

	tk := testkit.NewTestKit(c, s.causetstore)
	statistics.ClearHistoryJobs()
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b int, index idx(a))")
	tk.MustExec("alter block t set tiflash replica 2 location labels 'a','b';")
	tk.MustQuery("select TABLE_SCHEMA,TABLE_NAME,REPLICA_COUNT,LOCATION_LABELS,AVAILABLE, PROGRESS from information_schema.tiflash_replica").Check(testkit.Events("test t 2 a,b 0 0"))
	tbl, err := petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tbl.Meta().TiFlashReplica.Available = true
	tk.MustQuery("select TABLE_SCHEMA,TABLE_NAME,REPLICA_COUNT,LOCATION_LABELS,AVAILABLE, PROGRESS from information_schema.tiflash_replica").Check(testkit.Events("test t 2 a,b 1 1"))
}

var _ = SerialSuites(&testschemaReplicantClusterBlockSuite{testschemaReplicantBlockSuiteBase: &testschemaReplicantBlockSuiteBase{}})

type testschemaReplicantClusterBlockSuite struct {
	*testschemaReplicantBlockSuiteBase
	rpcserver  *grpc.Server
	httpServer *httptest.Server
	mockAddr   string
	listenAddr string
	startTime  time.Time
}

func (s *testschemaReplicantClusterBlockSuite) SetUpSuite(c *C) {
	s.testschemaReplicantBlockSuiteBase.SetUpSuite(c)
	s.rpcserver, s.listenAddr = s.setUpRPCService(c, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockFIDelHTTPServer()
	s.startTime = time.Now()
}

func (s *testschemaReplicantClusterBlockSuite) setUpRPCService(c *C, addr string) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	// Fix issue 9836
	sm := &mockStochastikManager{make(map[uint64]*soliton.ProcessInfo, 1)}
	sm.processInfoMap[1] = &soliton.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: allegrosql.ComQuery,
	}
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

func (s *testschemaReplicantClusterBlockSuite) setUpMockFIDelHTTPServer() (*httptest.Server, string) {
	// mock FIDel http server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	// mock causetstore stats stat
	mockAddr := strings.TrimPrefix(server.URL, "http://")
	router.Handle(FIDelapi.Stores, fn.Wrap(func() (*helper.StoresStat, error) {
		return &helper.StoresStat{
			Count: 1,
			Stores: []helper.StoreStat{
				{
					CausetStore: helper.StoreBaseStat{
						ID:             1,
						Address:        "127.0.0.1:20160",
						State:          0,
						StateName:      "Up",
						Version:        "4.0.0-alpha",
						StatusAddress:  mockAddr,
						GitHash:        "mock-einsteindb-githash",
						StartTimestamp: s.startTime.Unix(),
					},
				},
			},
		}, nil
	}))
	// mock FIDel API
	router.Handle(FIDelapi.ClusterVersion, fn.Wrap(func() (string, error) { return "4.0.0-alpha", nil }))
	router.Handle(FIDelapi.Status, fn.Wrap(func() (interface{}, error) {
		return struct {
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{
			GitHash:        "mock-fidel-githash",
			StartTimestamp: s.startTime.Unix(),
		}, nil
	}))
	var mockConfig = func() (map[string]interface{}, error) {
		configuration := map[string]interface{}{
			"key1": "value1",
			"key2": map[string]string{
				"nest1": "n-value1",
				"nest2": "n-value2",
			},
			"key3": map[string]interface{}{
				"nest1": "n-value1",
				"nest2": "n-value2",
				"key4": map[string]string{
					"nest3": "n-value4",
					"nest4": "n-value5",
				},
			},
		}
		return configuration, nil
	}
	// FIDel config.
	router.Handle(FIDelapi.Config, fn.Wrap(mockConfig))
	// MilevaDB/EinsteinDB config.
	router.Handle("/config", fn.Wrap(mockConfig))
	// FIDel region.
	router.Handle("/fidel/api/v1/stats/region", fn.Wrap(func() (*helper.FIDelRegionStats, error) {
		return &helper.FIDelRegionStats{
			Count:            1,
			EmptyCount:       1,
			StorageSize:      1,
			StorageKeys:      1,
			StoreLeaderCount: map[uint64]int{1: 1},
			StorePeerCount:   map[uint64]int{1: 1},
		}, nil
	}))
	return server, mockAddr
}

func (s *testschemaReplicantClusterBlockSuite) TearDownSuite(c *C) {
	if s.rpcserver != nil {
		s.rpcserver.Stop()
		s.rpcserver = nil
	}
	if s.httpServer != nil {
		s.httpServer.Close()
	}
	s.testschemaReplicantBlockSuiteBase.TearDownSuite(c)
}

type mockStochastikManager struct {
	processInfoMap map[uint64]*soliton.ProcessInfo
}

func (sm *mockStochastikManager) ShowProcessList() map[uint64]*soliton.ProcessInfo {
	return sm.processInfoMap
}

func (sm *mockStochastikManager) GetProcessInfo(id uint64) (*soliton.ProcessInfo, bool) {
	rs, ok := sm.processInfoMap[id]
	return rs, ok
}

func (sm *mockStochastikManager) Kill(connectionID uint64, query bool) {}

func (sm *mockStochastikManager) UFIDelateTLSConfig(cfg *tls.Config) {}

type mockStore struct {
	einsteindb.CausetStorage
	host string
}

func (s *mockStore) EtcdAddrs() ([]string, error) { return []string{s.host}, nil }
func (s *mockStore) TLSConfig() *tls.Config       { panic("not implemented") }
func (s *mockStore) StartGCWorker() error         { panic("not implemented") }

func (s *testschemaReplicantClusterBlockSuite) TestMilevaDBClusterInfo(c *C) {
	mockAddr := s.mockAddr
	causetstore := &mockStore{
		s.causetstore.(einsteindb.CausetStorage),
		mockAddr,
	}

	// information_schema.cluster_info
	tk := testkit.NewTestKit(c, causetstore)
	milevadbStatusAddr := fmt.Sprintf(":%d", config.GetGlobalConfig().Status.StatusPort)
	event := func(defcaus ...string) string { return strings.Join(defcaus, " ") }
	tk.MustQuery("select type, instance, status_address, version, git_hash from information_schema.cluster_info").Check(testkit.Events(
		event("milevadb", ":4000", milevadbStatusAddr, "None", "None"),
		event("fidel", mockAddr, mockAddr, "4.0.0-alpha", "mock-fidel-githash"),
		event("einsteindb", "store1", "", "", ""),
	))
	startTime := s.startTime.Format(time.RFC3339)
	tk.MustQuery("select type, instance, start_time from information_schema.cluster_info where type != 'milevadb'").Check(testkit.Events(
		event("fidel", mockAddr, startTime),
		event("einsteindb", "store1", ""),
	))

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockStoreTombstone", `return(true)`), IsNil)
	tk.MustQuery("select type, instance, start_time from information_schema.cluster_info where type = 'einsteindb'").Check(testkit.Events())
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockStoreTombstone"), IsNil)

	// information_schema.cluster_config
	instances := []string{
		"fidel,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash",
		"milevadb,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash",
		"einsteindb,127.0.0.1:11080," + mockAddr + ",mock-version,mock-githash",
	}
	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockClusterInfo", fpExpr), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockClusterInfo"), IsNil)
	}()
	tk.MustQuery("select * from information_schema.cluster_config").Check(testkit.Events(
		"fidel 127.0.0.1:11080 key1 value1",
		"fidel 127.0.0.1:11080 key2.nest1 n-value1",
		"fidel 127.0.0.1:11080 key2.nest2 n-value2",
		"fidel 127.0.0.1:11080 key3.key4.nest3 n-value4",
		"fidel 127.0.0.1:11080 key3.key4.nest4 n-value5",
		"fidel 127.0.0.1:11080 key3.nest1 n-value1",
		"fidel 127.0.0.1:11080 key3.nest2 n-value2",
		"milevadb 127.0.0.1:11080 key1 value1",
		"milevadb 127.0.0.1:11080 key2.nest1 n-value1",
		"milevadb 127.0.0.1:11080 key2.nest2 n-value2",
		"milevadb 127.0.0.1:11080 key3.key4.nest3 n-value4",
		"milevadb 127.0.0.1:11080 key3.key4.nest4 n-value5",
		"milevadb 127.0.0.1:11080 key3.nest1 n-value1",
		"milevadb 127.0.0.1:11080 key3.nest2 n-value2",
		"einsteindb 127.0.0.1:11080 key1 value1",
		"einsteindb 127.0.0.1:11080 key2.nest1 n-value1",
		"einsteindb 127.0.0.1:11080 key2.nest2 n-value2",
		"einsteindb 127.0.0.1:11080 key3.key4.nest3 n-value4",
		"einsteindb 127.0.0.1:11080 key3.key4.nest4 n-value5",
		"einsteindb 127.0.0.1:11080 key3.nest1 n-value1",
		"einsteindb 127.0.0.1:11080 key3.nest2 n-value2",
	))
	tk.MustQuery("select TYPE, `KEY`, VALUE from information_schema.cluster_config where `key`='key3.key4.nest4' order by type").Check(testkit.Events(
		"fidel key3.key4.nest4 n-value5",
		"milevadb key3.key4.nest4 n-value5",
		"einsteindb key3.key4.nest4 n-value5",
	))
}

func (s *testschemaReplicantClusterBlockSuite) TestBlockStorageStats(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	err := tk.QueryToErr("select * from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test'")
	c.Assert(err.Error(), Equals, "fidel unavailable")
	mockAddr := s.mockAddr
	causetstore := &mockStore{
		s.causetstore.(einsteindb.CausetStorage),
		mockAddr,
	}

	// Test information_schema.TABLE_STORAGE_STATS.
	tk = testkit.NewTestKit(c, causetstore)

	// Test not set the schemaReplicant.
	err = tk.QueryToErr("select * from information_schema.TABLE_STORAGE_STATS")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Please specify the 'block_schema'")

	// Test it would get null set when get the sys schemaReplicant.
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema';").Check([][]interface{}{})
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'allegrosql';").Check([][]interface{}{})
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA in ('allegrosql', 'metrics_schema');").Check([][]interface{}{})
	tk.MustQuery("select TABLE_NAME from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'information_schema' and TABLE_NAME='schemata';").Check([][]interface{}{})

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b int, index idx(a))")
	tk.MustQuery("select TABLE_NAME, TABLE_SIZE from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test' and TABLE_NAME='t';").Check(testkit.Events("t 1"))

	tk.MustExec("create block t1 (a int, b int, index idx(a))")
	tk.MustQuery("select TABLE_NAME, sum(TABLE_SIZE) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test' group by TABLE_NAME;").Sort().Check(testkit.Events(
		"t 1",
		"t1 1",
	))
	tk.MustQuery("select TABLE_SCHEMA, sum(TABLE_SIZE) from information_schema.TABLE_STORAGE_STATS where TABLE_SCHEMA = 'test' group by TABLE_SCHEMA;").Check(testkit.Events(
		"test 2",
	))
}

func (s *testschemaReplicantBlockSuite) TestSequences(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("CREATE SEQUENCE test.seq maxvalue 10000000")
	tk.MustQuery("SELECT * FROM information_schema.sequences WHERE sequence_schema='test' AND sequence_name='seq'").Check(testkit.Events("def test seq 1 1000 0 1 10000000 1 1 "))
	tk.MustExec("DROP SEQUENCE test.seq")
	tk.MustExec("CREATE SEQUENCE test.seq start = -1 minvalue -1 maxvalue 10 increment 1 cache 10")
	tk.MustQuery("SELECT * FROM information_schema.sequences WHERE sequence_schema='test' AND sequence_name='seq'").Check(testkit.Events("def test seq 1 10 0 1 10 -1 -1 "))
	tk.MustExec("CREATE SEQUENCE test.seq2 start = -9 minvalue -10 maxvalue 10 increment -1 cache 15")
	tk.MustQuery("SELECT * FROM information_schema.sequences WHERE sequence_schema='test' AND sequence_name='seq2'").Check(testkit.Events("def test seq2 1 15 0 -1 10 -10 -9 "))
}

func (s *testschemaReplicantBlockSuite) TestTiFlashSystemBlocks(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	err := tk.QueryToErr("select * from information_schema.TIFLASH_TABLES;")
	c.Assert(err.Error(), Equals, "Etcd addrs not found")
	err = tk.QueryToErr("select * from information_schema.TIFLASH_SEGMENTS;")
	c.Assert(err.Error(), Equals, "Etcd addrs not found")
}

func (s *testschemaReplicantBlockSuite) TestBlocksPKType(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("create block t_int (a int primary key, b int)")
	tk.MustQuery("SELECT MilevaDB_PK_TYPE FROM information_schema.blocks where block_schema = 'test' and block_name = 't_int'").Check(testkit.Events("INT CLUSTERED"))
	tk.MustExec("set @@milevadb_enable_clustered_index = 0")
	tk.MustExec("create block t_implicit (a varchar(64) primary key, b int)")
	tk.MustQuery("SELECT MilevaDB_PK_TYPE FROM information_schema.blocks where block_schema = 'test' and block_name = 't_implicit'").Check(testkit.Events("NON-CLUSTERED"))
	tk.MustExec("set @@milevadb_enable_clustered_index = 1")
	tk.MustExec("create block t_common (a varchar(64) primary key, b int)")
	tk.MustQuery("SELECT MilevaDB_PK_TYPE FROM information_schema.blocks where block_schema = 'test' and block_name = 't_common'").Check(testkit.Events("COMMON CLUSTERED"))
	tk.MustQuery("SELECT MilevaDB_PK_TYPE FROM information_schema.blocks where block_schema = 'INFORMATION_SCHEMA' and block_name = 'TABLES'").Check(testkit.Events("NON-CLUSTERED"))
}
