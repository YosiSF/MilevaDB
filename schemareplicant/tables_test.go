// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schemareplicant_test

import (
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fn"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causetstore/helper"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/server"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/FIDelapi"
	"github.com/whtcorpsinc/milevadb/soliton/ekvcache"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"google.golang.org/grpc"
)

var _ = Suite(&testBlockSuite{&testBlockSuiteBase{}})
var _ = SerialSuites(&testClusterBlockSuite{testBlockSuiteBase: &testBlockSuiteBase{}})

type testBlockSuite struct {
	*testBlockSuiteBase
}

type testBlockSuiteBase struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

func (s *testBlockSuiteBase) SetUpSuite(c *C) {
	testleak.BeforeTest()

	var err error
	s.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	stochastik.DisableStats4Test()
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *testBlockSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
	testleak.AfterTest(c)()
}

func (s *testBlockSuiteBase) newTestKitWithRoot(c *C) *testkit.TestKit {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	return tk
}

func (s *testBlockSuiteBase) newTestKitWithCausetCache(c *C) *testkit.TestKit {
	tk := testkit.NewTestKit(c, s.causetstore)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)
	tk.GetConnectionID()
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	return tk
}

type testClusterBlockSuite struct {
	*testBlockSuiteBase
	rpcserver  *grpc.Server
	httpServer *httptest.Server
	mockAddr   string
	listenAddr string
	startTime  time.Time
}

func (s *testClusterBlockSuite) SetUpSuite(c *C) {
	s.testBlockSuiteBase.SetUpSuite(c)
	s.rpcserver, s.listenAddr = s.setUpRPCService(c, "127.0.0.1:0")
	s.httpServer, s.mockAddr = s.setUpMockFIDelHTTPServer()
	s.startTime = time.Now()
}

func (s *testClusterBlockSuite) setUpRPCService(c *C, addr string) (*grpc.Server, string) {
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

func (s *testClusterBlockSuite) setUpMockFIDelHTTPServer() (*httptest.Server, string) {
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
	// fidel config
	router.Handle(FIDelapi.Config, fn.Wrap(mockConfig))
	// MilevaDB/EinsteinDB config
	router.Handle("/config", fn.Wrap(mockConfig))
	return server, mockAddr
}

func (s *testClusterBlockSuite) TearDownSuite(c *C) {
	if s.rpcserver != nil {
		s.rpcserver.Stop()
		s.rpcserver = nil
	}
	if s.httpServer != nil {
		s.httpServer.Close()
	}
	s.testBlockSuiteBase.TearDownSuite(c)
}

func (s *testBlockSuite) TestschemaReplicantFieldValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists numschema, timeschema")
	tk.MustInterDirc("create causet numschema(i int(2), f float(4,2), d decimal(4,3))")
	tk.MustInterDirc("create causet timeschema(d date, dt datetime(3), ts timestamp(3), t time(4), y year(4))")
	tk.MustInterDirc("create causet strschema(c char(3), c2 varchar(3), b blob(3), t text(3))")
	tk.MustInterDirc("create causet floatschema(a float, b double(7, 3))")

	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where block_name='numschema'").
		Check(testkit.Rows("<nil> <nil> 2 0 <nil>", "<nil> <nil> 4 2 <nil>", "<nil> <nil> 4 3 <nil>")) // FIXME: for allegrosql first one will be "<nil> <nil> 10 0 <nil>"
	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where block_name='timeschema'").
		Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>", "<nil> <nil> <nil> <nil> 3", "<nil> <nil> <nil> <nil> 3", "<nil> <nil> <nil> <nil> 4", "<nil> <nil> <nil> <nil> <nil>"))
	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where block_name='strschema'").
		Check(testkit.Rows("3 3 <nil> <nil> <nil>", "3 3 <nil> <nil> <nil>", "3 3 <nil> <nil> <nil>", "3 3 <nil> <nil> <nil>")) // FIXME: for allegrosql last two will be "255 255 <nil> <nil> <nil>", "255 255 <nil> <nil> <nil>"
	tk.MustQuery("select NUMERIC_SCALE from information_schema.COLUMNS where block_name='floatschema'").
		Check(testkit.Rows("<nil>", "3"))

	// Test for auto increment ID.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c int auto_increment primary key, d int)")
	tk.MustQuery("select auto_increment from information_schema.blocks where block_name='t'").Check(
		testkit.Rows("1"))
	tk.MustInterDirc("insert into t(c, d) values(1, 1)")
	tk.MustQuery("select auto_increment from information_schema.blocks where block_name='t'").Check(
		testkit.Rows("2"))

	tk.MustQuery("show create causet t").Check(
		testkit.Rows("" +
			"t CREATE TABLE `t` (\n" +
			"  `c` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `d` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`c`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=30002"))

	// Test auto_increment for causet without auto_increment defCausumn
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (d int)")
	tk.MustQuery("select auto_increment from information_schema.blocks where block_name='t'").Check(
		testkit.Rows("<nil>"))

	tk.MustInterDirc("create user xxx")

	// Test for length of enum and set
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t ( s set('a','bc','def','ghij') default NULL, e1 enum('a', 'ab', 'cdef'), s2 SET('1','2','3','4','1585','ONE','TWO','Y','N','THREE'))")
	tk.MustQuery("select defCausumn_name, character_maximum_length from information_schema.defCausumns where block_schema=Database() and block_name = 't' and defCausumn_name = 's'").Check(
		testkit.Rows("s 13"))
	tk.MustQuery("select defCausumn_name, character_maximum_length from information_schema.defCausumns where block_schema=Database() and block_name = 't' and defCausumn_name = 's2'").Check(
		testkit.Rows("s2 30"))
	tk.MustQuery("select defCausumn_name, character_maximum_length from information_schema.defCausumns where block_schema=Database() and block_name = 't' and defCausumn_name = 'e1'").Check(
		testkit.Rows("e1 4"))

	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustInterDirc("use test")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{
		Username: "xxx",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)

	tk1.MustQuery("select distinct(block_schema) from information_schema.blocks").Check(testkit.Rows("INFORMATION_SCHEMA"))

	// Fix issue 9836
	sm := &mockStochastikManager{make(map[uint64]*soliton.ProcessInfo, 1)}
	sm.processInfoMap[1] = &soliton.ProcessInfo{
		ID:      1,
		User:    "root",
		Host:    "127.0.0.1",
		Command: allegrosql.ComQuery,
		StmtCtx: tk.Se.GetStochastikVars().StmtCtx,
	}
	tk.Se.SetStochastikManager(sm)
	tk.MustQuery("SELECT user,host,command FROM information_schema.processlist;").Check(testkit.Rows("root 127.0.0.1 Query"))

	// Test for all system blocks `TABLE_TYPE` is `SYSTEM VIEW`.
	rows1 := tk.MustQuery("select count(*) from information_schema.blocks where block_schema in ('INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','METRICS_SCHEMA');").Rows()
	rows2 := tk.MustQuery("select count(*) from information_schema.blocks where block_schema in ('INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','METRICS_SCHEMA') and  block_type = 'SYSTEM VIEW';").Rows()
	c.Assert(rows1, DeepEquals, rows2)
	// Test for system causet default value
	tk.MustQuery("show create causet information_schema.PROCESSLIST").Check(
		testkit.Rows("" +
			"PROCESSLIST CREATE TABLE `PROCESSLIST` (\n" +
			"  `ID` bigint(21) unsigned NOT NULL DEFAULT 0,\n" +
			"  `USER` varchar(16) NOT NULL DEFAULT '',\n" +
			"  `HOST` varchar(64) NOT NULL DEFAULT '',\n" +
			"  `EDB` varchar(64) DEFAULT NULL,\n" +
			"  `COMMAND` varchar(16) NOT NULL DEFAULT '',\n" +
			"  `TIME` int(7) NOT NULL DEFAULT 0,\n" +
			"  `STATE` varchar(7) DEFAULT NULL,\n" +
			"  `INFO` longtext DEFAULT NULL,\n" +
			"  `DIGEST` varchar(64) DEFAULT '',\n" +
			"  `MEM` bigint(21) unsigned DEFAULT NULL,\n" +
			"  `TxnStart` varchar(64) NOT NULL DEFAULT ''\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create causet information_schema.cluster_log").Check(
		testkit.Rows("" +
			"CLUSTER_LOG CREATE TABLE `CLUSTER_LOG` (\n" +
			"  `TIME` varchar(32) DEFAULT NULL,\n" +
			"  `TYPE` varchar(64) DEFAULT NULL,\n" +
			"  `INSTANCE` varchar(64) DEFAULT NULL,\n" +
			"  `LEVEL` varchar(8) DEFAULT NULL,\n" +
			"  `MESSAGE` longtext DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func (s *testBlockSuite) TestCharacterSetDefCauslations(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	// Test charset/defCauslation in information_schema.COLUMNS causet.
	tk.MustInterDirc("DROP DATABASE IF EXISTS charset_defCauslate_test")
	tk.MustInterDirc("CREATE DATABASE charset_defCauslate_test; USE charset_defCauslate_test")

	// TODO: Specifying the charset for national char/varchar should not be supported.
	tk.MustInterDirc(`CREATE TABLE charset_defCauslate_defCaus_test(
		c_int int,
		c_float float,
		c_bit bit,
		c_bool bool,
		c_char char(1) charset ascii defCauslate ascii_bin,
		c_nchar national char(1) charset ascii defCauslate ascii_bin,
		c_binary binary,
		c_varchar varchar(1) charset ascii defCauslate ascii_bin,
		c_nvarchar national varchar(1) charset ascii defCauslate ascii_bin,
		c_varbinary varbinary(1),
		c_year year,
		c_date date,
		c_time time,
		c_datetime datetime,
		c_timestamp timestamp,
		c_blob blob,
		c_tinyblob tinyblob,
		c_mediumblob mediumblob,
		c_longblob longblob,
		c_text text charset ascii defCauslate ascii_bin,
		c_tinytext tinytext charset ascii defCauslate ascii_bin,
		c_mediumtext mediumtext charset ascii defCauslate ascii_bin,
		c_longtext longtext charset ascii defCauslate ascii_bin,
		c_json json,
		c_enum enum('1') charset ascii defCauslate ascii_bin,
		c_set set('1') charset ascii defCauslate ascii_bin
	)`)

	tk.MustQuery(`SELECT defCausumn_name, character_set_name, defCauslation_name
					FROM information_schema.COLUMNS
					WHERE block_schema = "charset_defCauslate_test" AND block_name = "charset_defCauslate_defCaus_test"
					ORDER BY defCausumn_name`,
	).Check(testkit.Rows(
		"c_binary <nil> <nil>",
		"c_bit <nil> <nil>",
		"c_blob <nil> <nil>",
		"c_bool <nil> <nil>",
		"c_char ascii ascii_bin",
		"c_date <nil> <nil>",
		"c_datetime <nil> <nil>",
		"c_enum ascii ascii_bin",
		"c_float <nil> <nil>",
		"c_int <nil> <nil>",
		"c_json <nil> <nil>",
		"c_longblob <nil> <nil>",
		"c_longtext ascii ascii_bin",
		"c_mediumblob <nil> <nil>",
		"c_mediumtext ascii ascii_bin",
		"c_nchar ascii ascii_bin",
		"c_nvarchar ascii ascii_bin",
		"c_set ascii ascii_bin",
		"c_text ascii ascii_bin",
		"c_time <nil> <nil>",
		"c_timestamp <nil> <nil>",
		"c_tinyblob <nil> <nil>",
		"c_tinytext ascii ascii_bin",
		"c_varbinary <nil> <nil>",
		"c_varchar ascii ascii_bin",
		"c_year <nil> <nil>",
	))
	tk.MustInterDirc("DROP DATABASE charset_defCauslate_test")
}

func (s *testBlockSuite) TestCurrentTimestampAsDefault(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("DROP DATABASE IF EXISTS default_time_test")
	tk.MustInterDirc("CREATE DATABASE default_time_test; USE default_time_test")

	tk.MustInterDirc(`CREATE TABLE default_time_block(
					c_datetime datetime,
					c_datetime_default datetime default current_timestamp,
					c_datetime_default_2 datetime(2) default current_timestamp(2),
					c_timestamp timestamp,
					c_timestamp_default timestamp default current_timestamp,
					c_timestamp_default_3 timestamp(3) default current_timestamp(3),
					c_varchar_default varchar(20) default "current_timestamp",
					c_varchar_default_3 varchar(20) default "current_timestamp(3)",
					c_varchar_default_on_uFIDelate datetime default current_timestamp on uFIDelate current_timestamp,
					c_varchar_default_on_uFIDelate_fsp datetime(3) default current_timestamp(3) on uFIDelate current_timestamp(3),
					c_varchar_default_with_case varchar(20) default "cUrrent_tImestamp"
				);`)

	tk.MustQuery(`SELECT defCausumn_name, defCausumn_default, extra
					FROM information_schema.COLUMNS
					WHERE block_schema = "default_time_test" AND block_name = "default_time_block"
					ORDER BY defCausumn_name`,
	).Check(testkit.Rows(
		"c_datetime <nil> ",
		"c_datetime_default CURRENT_TIMESTAMP ",
		"c_datetime_default_2 CURRENT_TIMESTAMP(2) ",
		"c_timestamp <nil> ",
		"c_timestamp_default CURRENT_TIMESTAMP ",
		"c_timestamp_default_3 CURRENT_TIMESTAMP(3) ",
		"c_varchar_default current_timestamp ",
		"c_varchar_default_3 current_timestamp(3) ",
		"c_varchar_default_on_uFIDelate CURRENT_TIMESTAMP DEFAULT_GENERATED on uFIDelate CURRENT_TIMESTAMP",
		"c_varchar_default_on_uFIDelate_fsp CURRENT_TIMESTAMP(3) DEFAULT_GENERATED on uFIDelate CURRENT_TIMESTAMP(3)",
		"c_varchar_default_with_case cUrrent_tImestamp ",
	))
	tk.MustInterDirc("DROP DATABASE default_time_test")
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

func (s *testBlockSuite) TestSomeBlocks(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.Se = se
	sm := &mockStochastikManager{make(map[uint64]*soliton.ProcessInfo, 2)}
	sm.processInfoMap[1] = &soliton.ProcessInfo{
		ID:      1,
		User:    "user-1",
		Host:    "localhost",
		EDB:     "information_schema",
		Command: byte(1),
		Digest:  "abc1",
		State:   1,
		Info:    "do something",
		StmtCtx: tk.Se.GetStochastikVars().StmtCtx,
	}
	sm.processInfoMap[2] = &soliton.ProcessInfo{
		ID:      2,
		User:    "user-2",
		Host:    "localhost",
		EDB:     "test",
		Command: byte(2),
		Digest:  "abc2",
		State:   2,
		Info:    strings.Repeat("x", 101),
		StmtCtx: tk.Se.GetStochastikVars().StmtCtx,
	}
	tk.Se.SetStochastikManager(sm)
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 ", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init EDB 9223372036 %s %s abc2 0 ", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("SHOW PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init EDB 9223372036 %s %s", "autocommit", strings.Repeat("x", 100)),
		))
	tk.MustQuery("SHOW FULL PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "do something"),
			fmt.Sprintf("2 user-2 localhost test Init EDB 9223372036 %s %s", "autocommit", strings.Repeat("x", 101)),
		))

	sm = &mockStochastikManager{make(map[uint64]*soliton.ProcessInfo, 2)}
	sm.processInfoMap[1] = &soliton.ProcessInfo{
		ID:      1,
		User:    "user-1",
		Host:    "localhost",
		EDB:     "information_schema",
		Command: byte(1),
		Digest:  "abc1",
		State:   1,
		StmtCtx: tk.Se.GetStochastikVars().StmtCtx,
	}
	sm.processInfoMap[2] = &soliton.ProcessInfo{
		ID:            2,
		User:          "user-2",
		Host:          "localhost",
		Command:       byte(2),
		Digest:        "abc2",
		State:         2,
		Info:          strings.Repeat("x", 101),
		StmtCtx:       tk.Se.GetStochastikVars().StmtCtx,
		CurTxnStartTS: 410090409861578752,
	}
	tk.Se.SetStochastikManager(sm)
	tk.Se.GetStochastikVars().TimeZone = time.UTC
	tk.MustQuery("select * from information_schema.PROCESSLIST order by ID;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 ", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init EDB 9223372036 %s %s abc2 0 07-29 03:26:05.158(410090409861578752)", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("SHOW PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init EDB 9223372036 %s %s", "autocommit", strings.Repeat("x", 100)),
		))
	tk.MustQuery("SHOW FULL PROCESSLIST;").Sort().Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s", "in transaction", "<nil>"),
			fmt.Sprintf("2 user-2 localhost <nil> Init EDB 9223372036 %s %s", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("select * from information_schema.PROCESSLIST where EDB is null;").Check(
		testkit.Rows(
			fmt.Sprintf("2 user-2 localhost <nil> Init EDB 9223372036 %s %s abc2 0 07-29 03:26:05.158(410090409861578752)", "autocommit", strings.Repeat("x", 101)),
		))
	tk.MustQuery("select * from information_schema.PROCESSLIST where Info is null;").Check(
		testkit.Rows(
			fmt.Sprintf("1 user-1 localhost information_schema Quit 9223372036 %s %s abc1 0 ", "in transaction", "<nil>"),
		))
}

func prepareSlowLogfile(c *C, slowLogFileName string) {
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	c.Assert(err, IsNil)
	_, err = f.Write([]byte(`# Time: 2020-02-12T19:33:56.571953+08:00
# Txn_start_ts: 406315658548871171
# User@Host: root[root] @ localhost [127.0.0.1]
# Conn_ID: 6
# InterDirc_retry_time: 0.12 InterDirc_retry_count: 57
# Query_time: 4.895492
# Parse_time: 0.4
# Compile_time: 0.2
# Rewrite_time: 0.000000003 Preproc_subqueries: 2 Preproc_subqueries_time: 0.000000002
# Optimize_time: 0.00000001
# Wait_TS: 0.000000003
# LockKeys_time: 1.71 Request_count: 1 Prewrite_time: 0.19 Wait_prewrite_binlog_time: 0.21 Commit_time: 0.01 Commit_backoff_time: 0.18 Backoff_types: [txnLock] Resolve_lock_time: 0.03 Write_keys: 15 Write_size: 480 Prewrite_region: 1 Txn_retry: 8
# Cop_time: 0.3824278 Process_time: 0.161 Request_count: 1 Total_keys: 100001 Process_keys: 100000
# Wait_time: 0.101
# Backoff_time: 0.092
# EDB: test
# Is_internal: false
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03 Cop_proc_addr: 127.0.0.1:20160
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8 Cop_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
# Disk_max: 65536
# Causet_from_cache: true
# Succ: true
# Causet: abcd
# Causet_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
# Prev_stmt: uFIDelate t set i = 2;
select * from t_slim;`))
	c.Assert(f.Close(), IsNil)
	c.Assert(err, IsNil)
}

func (s *testBlockSuite) TestBlockRowIDShardingInfo(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("DROP DATABASE IF EXISTS `sharding_info_test_db`")
	tk.MustInterDirc("CREATE DATABASE `sharding_info_test_db`")

	assertShardingInfo := func(blockName string, expectInfo interface{}) {
		queryALLEGROSQL := fmt.Sprintf("select milevadb_row_id_sharding_info from information_schema.blocks where block_schema = 'sharding_info_test_db' and block_name = '%s'", blockName)
		info := tk.MustQuery(queryALLEGROSQL).Rows()[0][0]
		if expectInfo == nil {
			c.Assert(info, Equals, "<nil>")
		} else {
			c.Assert(info, Equals, expectInfo)
		}
	}
	tk.MustInterDirc("CREATE TABLE `sharding_info_test_db`.`t1` (a int)")
	assertShardingInfo("t1", "NOT_SHARDED")

	tk.MustInterDirc("CREATE TABLE `sharding_info_test_db`.`t2` (a int key)")
	assertShardingInfo("t2", "NOT_SHARDED(PK_IS_HANDLE)")

	tk.MustInterDirc("CREATE TABLE `sharding_info_test_db`.`t3` (a int) SHARD_ROW_ID_BITS=4")
	assertShardingInfo("t3", "SHARD_BITS=4")

	tk.MustInterDirc("CREATE VIEW `sharding_info_test_db`.`tv` AS select 1")
	assertShardingInfo("tv", nil)

	testFunc := func(dbName string, expectInfo interface{}) {
		dbInfo := perceptron.DBInfo{Name: perceptron.NewCIStr(dbName)}
		blockInfo := perceptron.BlockInfo{}

		info := schemareplicant.GetShardingInfo(&dbInfo, &blockInfo)
		c.Assert(info, Equals, expectInfo)
	}

	testFunc("information_schema", nil)
	testFunc("allegrosql", nil)
	testFunc("performance_schema", nil)
	testFunc("uucc", "NOT_SHARDED")

	solitonutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer solitonutil.ConfigTestUtils.RestoreAutoRandomTestConfig()

	tk.MustInterDirc("CREATE TABLE `sharding_info_test_db`.`t4` (a bigint key auto_random)")
	assertShardingInfo("t4", "PK_AUTO_RANDOM_BITS=5")

	tk.MustInterDirc("CREATE TABLE `sharding_info_test_db`.`t5` (a bigint key auto_random(1))")
	assertShardingInfo("t5", "PK_AUTO_RANDOM_BITS=1")

	tk.MustInterDirc("DROP DATABASE `sharding_info_test_db`")
}

func (s *testBlockSuite) TestSlowQuery(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Prepare slow log file.
	slowLogFileName := "milevadb_slow.log"
	prepareSlowLogfile(c, slowLogFileName)
	defer os.Remove(slowLogFileName)

	tk.MustInterDirc(fmt.Sprintf("set @@milevadb_slow_query_file='%v'", slowLogFileName))
	tk.MustInterDirc("set time_zone = '+08:00';")
	re := tk.MustQuery("select * from information_schema.slow_query")
	re.Check(solitonutil.RowsWithSep("|",
		"2020-02-12 19:33:56.571953|406315658548871171|root|localhost|6|57|0.12|4.895492|0.4|0.2|0.000000003|2|0.000000002|0.00000001|0.000000003|0.19|0.21|0.01|0|0.18|[txnLock]|0.03|0|15|480|1|8|0.3824278|0.161|0.101|0.092|1.71|1|100001|100000|test||0|42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772|t1:1,t2:2|0.1|0.2|0.03|127.0.0.1:20160|0.05|0.6|0.8|0.0.0.0:20160|70724|65536|1|1|abcd|60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4|uFIDelate t set i = 2;|select * from t_slim;"))
	tk.MustInterDirc("set time_zone = '+00:00';")
	re = tk.MustQuery("select * from information_schema.slow_query")
	re.Check(solitonutil.RowsWithSep("|", "2020-02-12 11:33:56.571953|406315658548871171|root|localhost|6|57|0.12|4.895492|0.4|0.2|0.000000003|2|0.000000002|0.00000001|0.000000003|0.19|0.21|0.01|0|0.18|[txnLock]|0.03|0|15|480|1|8|0.3824278|0.161|0.101|0.092|1.71|1|100001|100000|test||0|42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772|t1:1,t2:2|0.1|0.2|0.03|127.0.0.1:20160|0.05|0.6|0.8|0.0.0.0:20160|70724|65536|1|1|abcd|60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4|uFIDelate t set i = 2;|select * from t_slim;"))

	// Test for long query.
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	c.Assert(err, IsNil)
	defer f.Close()
	_, err = f.Write([]byte(`
# Time: 2020-02-13T19:33:56.571953+08:00
`))
	c.Assert(err, IsNil)
	allegrosql := "select * from "
	for len(allegrosql) < 5000 {
		allegrosql += "abcdefghijklmnopqrstuvwxyz_1234567890_qwertyuiopasdfghjklzxcvbnm"
	}
	allegrosql += ";"
	_, err = f.Write([]byte(allegrosql))
	c.Assert(err, IsNil)
	c.Assert(f.Close(), IsNil)
	re = tk.MustQuery("select query from information_schema.slow_query order by time desc limit 1")
	rows := re.Rows()
	c.Assert(rows[0][0], Equals, allegrosql)
}

func (s *testBlockSuite) TestDeferredCausetStatistics(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select * from information_schema.defCausumn_statistics").Check(testkit.Rows())
}

func (s *testBlockSuite) TestReloadDroFIDelatabase(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database test_dbs")
	tk.MustInterDirc("use test_dbs")
	tk.MustInterDirc("create causet t1 (a int)")
	tk.MustInterDirc("create causet t2 (a int)")
	tk.MustInterDirc("create causet t3 (a int)")
	is := petri.GetPetri(tk.Se).SchemaReplicant()
	t2, err := is.BlockByName(perceptron.NewCIStr("test_dbs"), perceptron.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tk.MustInterDirc("drop database test_dbs")
	is = petri.GetPetri(tk.Se).SchemaReplicant()
	_, err = is.BlockByName(perceptron.NewCIStr("test_dbs"), perceptron.NewCIStr("t2"))
	c.Assert(terror.ErrorEqual(schemareplicant.ErrBlockNotExists, err), IsTrue)
	_, ok := is.BlockByID(t2.Meta().ID)
	c.Assert(ok, IsFalse)
}

func (s *testClusterBlockSuite) TestForClusterServerInfo(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	instances := []string{
		strings.Join([]string{"milevadb", s.listenAddr, s.listenAddr, "mock-version,mock-githash"}, ","),
		strings.Join([]string{"fidel", s.listenAddr, s.listenAddr, "mock-version,mock-githash"}, ","),
		strings.Join([]string{"einsteindb", s.listenAddr, s.listenAddr, "mock-version,mock-githash"}, ","),
	}

	fpExpr := `return("` + strings.Join(instances, ";") + `")`
	fpName := "github.com/whtcorpsinc/milevadb/schemareplicant/mockClusterInfo"
	c.Assert(failpoint.Enable(fpName, fpExpr), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	cases := []struct {
		allegrosql string
		types      set.StringSet
		addrs      set.StringSet
		names      set.StringSet
		skipOnOS   string
	}{
		{
			allegrosql: "select * from information_schema.CLUSTER_LOAD;",
			types:      set.NewStringSet("milevadb", "einsteindb", "fidel"),
			addrs:      set.NewStringSet(s.listenAddr),
			names:      set.NewStringSet("cpu", "memory", "net"),
		},
		{
			allegrosql: "select * from information_schema.CLUSTER_HARDWARE;",
			types:      set.NewStringSet("milevadb", "einsteindb", "fidel"),
			addrs:      set.NewStringSet(s.listenAddr),
			names:      set.NewStringSet("cpu", "memory", "net", "disk"),
			// The sysutil package will filter out all disk don't have /dev prefix.
			skipOnOS: "windows",
		},
		{
			allegrosql: "select * from information_schema.CLUSTER_SYSTEMINFO;",
			types:      set.NewStringSet("milevadb", "einsteindb", "fidel"),
			addrs:      set.NewStringSet(s.listenAddr),
			names:      set.NewStringSet("system"),
			// This test get empty result and fails on the windows platform.
			// Because the underlying implementation use `sysctl` command to get the result
			// and there is no such command on windows.
			// https://github.com/whtcorpsinc/sysutil/blob/2bfa6dc40bcd4c103bf684fba528ae4279c7ec9f/system_info.go#L50
			skipOnOS: "windows",
		},
	}

	for _, cas := range cases {
		if cas.skipOnOS == runtime.GOOS {
			continue
		}

		result := tk.MustQuery(cas.allegrosql)
		rows := result.Rows()
		c.Assert(len(rows), Greater, 0)

		gotTypes := set.StringSet{}
		gotAddrs := set.StringSet{}
		gotNames := set.StringSet{}

		for _, event := range rows {
			gotTypes.Insert(event[0].(string))
			gotAddrs.Insert(event[1].(string))
			gotNames.Insert(event[2].(string))
		}

		c.Assert(gotTypes, DeepEquals, cas.types, Commentf("allegrosql: %s", cas.allegrosql))
		c.Assert(gotAddrs, DeepEquals, cas.addrs, Commentf("allegrosql: %s", cas.allegrosql))
		c.Assert(gotNames, DeepEquals, cas.names, Commentf("allegrosql: %s", cas.allegrosql))
	}
}

func (s *testBlockSuite) TestSystemSchemaID(c *C) {
	uniqueIDMap := make(map[int64]string)
	s.checkSystemSchemaBlockID(c, "information_schema", autoid.InformationSchemaDBID, 1, 10000, uniqueIDMap)
	s.checkSystemSchemaBlockID(c, "performance_schema", autoid.PerformanceSchemaDBID, 10000, 20000, uniqueIDMap)
	s.checkSystemSchemaBlockID(c, "metrics_schema", autoid.MetricSchemaDBID, 20000, 30000, uniqueIDMap)
}

func (s *testBlockSuite) checkSystemSchemaBlockID(c *C, dbName string, dbID, start, end int64, uniqueIDMap map[int64]string) {
	is := s.dom.SchemaReplicant()
	c.Assert(is, NotNil)
	EDB, ok := is.SchemaByName(perceptron.NewCIStr(dbName))
	c.Assert(ok, IsTrue)
	c.Assert(EDB.ID, Equals, dbID)
	// Test for information_schema causet id.
	blocks := is.SchemaBlocks(perceptron.NewCIStr(dbName))
	c.Assert(len(blocks), Greater, 0)
	for _, tbl := range blocks {
		tid := tbl.Meta().ID
		comment := Commentf("causet name is %v", tbl.Meta().Name)
		c.Assert(tid&autoid.SystemSchemaIDFlag, Greater, int64(0), comment)
		c.Assert(tid&^autoid.SystemSchemaIDFlag, Greater, start, comment)
		c.Assert(tid&^autoid.SystemSchemaIDFlag, Less, end, comment)
		name, ok := uniqueIDMap[tid]
		c.Assert(ok, IsFalse, Commentf("schemaReplicant id of %v is duplicate with %v, both is %v", name, tbl.Meta().Name, tid))
		uniqueIDMap[tid] = tbl.Meta().Name.O
	}
}

func (s *testClusterBlockSuite) TestSelectClusterBlock(c *C) {
	tk := s.newTestKitWithRoot(c)
	slowLogFileName := "milevadb-slow.log"
	prepareSlowLogfile(c, slowLogFileName)
	defer os.Remove(slowLogFileName)
	for i := 0; i < 2; i++ {
		tk.MustInterDirc("use information_schema")
		tk.MustInterDirc(fmt.Sprintf("set @@milevadb_enable_streaming=%d", i))
		tk.MustInterDirc("set @@global.milevadb_enable_stmt_summary=1")
		tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("1"))
		tk.MustQuery("select time from `CLUSTER_SLOW_QUERY` where time='2020-02-12 19:33:56.571953'").Check(solitonutil.RowsWithSep("|", "2020-02-12 19:33:56.571953"))
		tk.MustQuery("select count(*) from `CLUSTER_PROCESSLIST`").Check(testkit.Rows("1"))
		tk.MustQuery("select * from `CLUSTER_PROCESSLIST`").Check(testkit.Rows(fmt.Sprintf(":10080 1 root 127.0.0.1 <nil> Query 9223372036 %s <nil>  0 ", "")))
		tk.MustQuery("select query_time, conn_id from `CLUSTER_SLOW_QUERY` order by time limit 1").Check(testkit.Rows("4.895492 6"))
		tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY` group by digest").Check(testkit.Rows("1"))
		tk.MustQuery("select digest, count(*) from `CLUSTER_SLOW_QUERY` group by digest").Check(testkit.Rows("42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772 1"))
		tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY` where time > now() group by digest").Check(testkit.Rows())
		re := tk.MustQuery("select * from `CLUSTER_memexs_summary`")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) > 0, IsTrue)
		// Test for MilevaDB issue 14915.
		re = tk.MustQuery("select sum(exec_count*avg_mem) from cluster_memexs_summary_history group by schema_name,digest,digest_text;")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) > 0, IsTrue)
		tk.MustQuery("select * from `CLUSTER_memexs_summary_history`")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) > 0, IsTrue)
		tk.MustInterDirc("set @@global.milevadb_enable_stmt_summary=0")
		re = tk.MustQuery("select * from `CLUSTER_memexs_summary`")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) == 0, IsTrue)
		tk.MustQuery("select * from `CLUSTER_memexs_summary_history`")
		c.Assert(re, NotNil)
		c.Assert(len(re.Rows()) == 0, IsTrue)
	}
}

func (s *testClusterBlockSuite) TestSelectClusterBlockPrivelege(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	slowLogFileName := "milevadb-slow.log"
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0644)
	c.Assert(err, IsNil)
	_, err = f.Write([]byte(
		`# Time: 2020-02-12T19:33:57.571953+08:00
# User@Host: user2 [user2] @ 127.0.0.1 [127.0.0.1]
select * from t2;
# Time: 2020-02-12T19:33:56.571953+08:00
# User@Host: user1 [user1] @ 127.0.0.1 [127.0.0.1]
select * from t1;
# Time: 2020-02-12T19:33:58.571953+08:00
# User@Host: user2 [user2] @ 127.0.0.1 [127.0.0.1]
select * from t3;
# Time: 2020-02-12T19:33:59.571953+08:00
select * from t3;
`))
	c.Assert(f.Close(), IsNil)
	c.Assert(err, IsNil)
	defer os.Remove(slowLogFileName)
	tk.MustInterDirc("use information_schema")
	tk.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from `SLOW_QUERY`").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*) from `CLUSTER_PROCESSLIST`").Check(testkit.Rows("1"))
	tk.MustQuery("select * from `CLUSTER_PROCESSLIST`").Check(testkit.Rows(fmt.Sprintf(":10080 1 root 127.0.0.1 <nil> Query 9223372036 %s <nil>  0 ", "")))
	tk.MustInterDirc("create user user1")
	tk.MustInterDirc("create user user2")
	user1 := testkit.NewTestKit(c, s.causetstore)
	user1.MustInterDirc("use information_schema")
	c.Assert(user1.Se.Auth(&auth.UserIdentity{
		Username: "user1",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	user1.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("1"))
	user1.MustQuery("select count(*) from `SLOW_QUERY`").Check(testkit.Rows("1"))
	user1.MustQuery("select user,query from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("user1 select * from t1;"))

	user2 := testkit.NewTestKit(c, s.causetstore)
	user2.MustInterDirc("use information_schema")
	c.Assert(user2.Se.Auth(&auth.UserIdentity{
		Username: "user2",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)
	user2.MustQuery("select count(*) from `CLUSTER_SLOW_QUERY`").Check(testkit.Rows("2"))
	user2.MustQuery("select user,query from `CLUSTER_SLOW_QUERY` order by query").Check(testkit.Rows("user2 select * from t2;", "user2 select * from t3;"))
}

func (s *testBlockSuite) TestSelectHiddenDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("DROP DATABASE IF EXISTS `test_hidden`;")
	tk.MustInterDirc("CREATE DATABASE `test_hidden`;")
	tk.MustInterDirc("USE test_hidden;")
	tk.MustInterDirc("CREATE TABLE hidden (a int , b int, c int);")
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where block_name = 'hidden'").Check(testkit.Rows("3"))
	tb, err := s.dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test_hidden"), perceptron.NewCIStr("hidden"))
	c.Assert(err, IsNil)
	defCausInfo := tb.Meta().DeferredCausets
	// Set defCausumn b to hidden
	defCausInfo[1].Hidden = true
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where block_name = 'hidden'").Check(testkit.Rows("2"))
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where block_name = 'hidden' and defCausumn_name = 'b'").Check(testkit.Rows("0"))
	// Set defCausumn b to visible
	defCausInfo[1].Hidden = false
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where block_name = 'hidden' and defCausumn_name = 'b'").Check(testkit.Rows("1"))
	// Set a, b ,c to hidden
	defCausInfo[0].Hidden = true
	defCausInfo[1].Hidden = true
	defCausInfo[2].Hidden = true
	tk.MustQuery("select count(*) from INFORMATION_SCHEMA.COLUMNS where block_name = 'hidden'").Check(testkit.Rows("0"))
}

func (s *testBlockSuite) TestFormatVersion(c *C) {
	// Test for defaultVersions.
	defaultVersions := []string{"5.7.25-MilevaDB-None", "5.7.25-MilevaDB-8.0.18", "5.7.25-MilevaDB-8.0.18-beta.1", "5.7.25-MilevaDB-v4.0.0-beta-446-g5268094af"}
	defaultRes := []string{"None", "8.0.18", "8.0.18-beta.1", "4.0.0-beta"}
	for i, v := range defaultVersions {
		version := schemareplicant.FormatVersion(v, true)
		c.Assert(version, Equals, defaultRes[i])
	}

	// Test for versions user set.
	versions := []string{"8.0.18", "5.7.25-MilevaDB", "8.0.18-MilevaDB-4.0.0-beta.1"}
	res := []string{"8.0.18", "5.7.25-MilevaDB", "8.0.18-MilevaDB-4.0.0-beta.1"}
	for i, v := range versions {
		version := schemareplicant.FormatVersion(v, false)
		c.Assert(version, Equals, res[i])
	}
}

// Test memexs_summary.
func (s *testBlockSuite) TestStmtSummaryBlock(c *C) {
	tk := s.newTestKitWithRoot(c)

	tk.MustInterDirc("set @@milevadb_enable_defCauslect_execution_info=0;")
	tk.MustQuery("select defCausumn_comment from information_schema.defCausumns " +
		"where block_name='STATEMENTS_SUMMARY' and defCausumn_name='STMT_TYPE'",
	).Check(testkit.Rows("Statement type"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b varchar(10), key k(a))")

	// Clear all memexs.
	tk.MustInterDirc("set stochastik milevadb_enable_stmt_summary = 0")
	tk.MustInterDirc("set stochastik milevadb_enable_stmt_summary = ''")

	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.milevadb_enable_stmt_summary").Check(testkit.Rows("1"))

	// Invalidate the cache manually so that milevadb_enable_stmt_summary works immediately.
	s.dom.GetGlobalVarsCache().Disable()
	// Disable refreshing summary.
	tk.MustInterDirc("set global milevadb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.milevadb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Create a new stochastik to test.
	tk = s.newTestKitWithRoot(c)

	// Test INSERT
	tk.MustInterDirc("insert into t values(1, 'a')")
	tk.MustInterDirc("insert into t    values(2, 'b')")
	tk.MustInterDirc("insert into t VALUES(3, 'c')")
	tk.MustInterDirc("/**/insert into t values(4, 'd')")
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text
		from information_schema.memexs_summary
		where digest_text like 'insert into t%'`,
	).Check(testkit.Rows("Insert test test.t <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into t values(1, 'a')"))

	// Test point get.
	tk.MustInterDirc("drop causet if exists p")
	tk.MustInterDirc("create causet p(a int primary key, b int)")
	for i := 1; i < 3; i++ {
		tk.MustQuery("select b from p where a=1")
		expectedResult := fmt.Sprintf("%d \tid         \ttask\testRows\toperator info\n\tPoint_Get_1\troot\t1      \tcauset:p, handle:1 %s", i, "test.p")
		// Also make sure that the plan digest is not empty
		tk.MustQuery(`select exec_count, plan, block_names
			from information_schema.memexs_summary
			where digest_text like 'select b from p%' and plan_digest != ''`,
		).Check(testkit.Rows(expectedResult))
	}

	// Point get another database.
	tk.MustQuery("select variable_value from allegrosql.milevadb where variable_name = 'system_tz'")
	tk.MustQuery(`select block_names
			from information_schema.memexs_summary
			where digest_text like 'select variable_value%' and schema_name='test'`,
	).Check(testkit.Rows("allegrosql.milevadb"))

	// Test `create database`.
	tk.MustInterDirc("create database if not exists test")
	tk.MustQuery(`select block_names
			from information_schema.memexs_summary
			where digest_text like 'create database%' and schema_name='test'`,
	).Check(testkit.Rows("<nil>"))

	// Test SELECT.
	const failpointName = "github.com/whtcorpsinc/milevadb/causet/embedded/mockCausetRowCount"
	c.Assert(failpoint.Enable(failpointName, "return(100)"), IsNil)
	defer func() { c.Assert(failpoint.Disable(failpointName), IsNil) }()
	tk.MustQuery("select * from t where a=2")
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.memexs_summary
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("Select test test.t t:k 1 2 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid            \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10\troot     \t100    \t\n" +
		"\t├─IndexScan_8 \tcop[einsteindb]\t100    \tcauset:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─BlockScan_9 \tcop[einsteindb]\t100    \tcauset:t, keep order:false, stats:pseudo"))

	// select ... order by
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text
		from information_schema.memexs_summary
		order by exec_count desc limit 1`,
	).Check(testkit.Rows("Insert test test.t <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into t values(1, 'a')"))

	// Test different plans with same digest.
	c.Assert(failpoint.Enable(failpointName, "return(1000)"), IsNil)
	tk.MustQuery("select * from t where a=3")
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.memexs_summary
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("Select test test.t t:k 2 4 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid            \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10\troot     \t100    \t\n" +
		"\t├─IndexScan_8 \tcop[einsteindb]\t100    \tcauset:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─BlockScan_9 \tcop[einsteindb]\t100    \tcauset:t, keep order:false, stats:pseudo"))

	// Disable it again.
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = false")
	tk.MustInterDirc("set stochastik milevadb_enable_stmt_summary = false")
	defer tk.MustInterDirc("set global milevadb_enable_stmt_summary = 1")
	defer tk.MustInterDirc("set stochastik milevadb_enable_stmt_summary = ''")
	tk.MustQuery("select @@global.milevadb_enable_stmt_summary").Check(testkit.Rows("0"))

	// Create a new stochastik to test
	tk = s.newTestKitWithRoot(c)

	// This memex shouldn't be summarized.
	tk.MustQuery("select * from t where a=2")

	// The causet should be cleared.
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.memexs_summary`,
	).Check(testkit.Rows())

	// Enable it in stochastik scope.
	tk.MustInterDirc("set stochastik milevadb_enable_stmt_summary = on")
	// It should work immediately.
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values(1, 'a')")
	tk.MustInterDirc("commit")
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, prev_sample_text
		from information_schema.memexs_summary
		where digest_text like 'insert into t%'`,
	).Check(testkit.Rows("Insert test test.t <nil> 1 0 0 0 0 0 0 0 0 0 1 insert into t values(1, 'a') "))
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, prev_sample_text
		from information_schema.memexs_summary
		where digest_text='commit'`,
	).Check(testkit.Rows("Commit test <nil> <nil> 1 0 0 0 0 0 2 2 1 1 0 commit insert into t values(1, 'a')"))

	tk.MustQuery("select * from t where a=2")
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.memexs_summary
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("Select test test.t t:k 1 2 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid            \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10\troot     \t1000   \t\n" +
		"\t├─IndexScan_8 \tcop[einsteindb]\t1000   \tcauset:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─BlockScan_9 \tcop[einsteindb]\t1000   \tcauset:t, keep order:false, stats:pseudo"))

	// Disable it in global scope.
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = false")

	// Create a new stochastik to test.
	tk = s.newTestKitWithRoot(c)

	tk.MustQuery("select * from t where a=2")

	// Statement summary is still enabled.
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.memexs_summary
		where digest_text like 'select * from t%'`,
	).Check(testkit.Rows("Select test test.t t:k 2 4 0 0 0 0 0 0 0 0 0 select * from t where a=2 \tid            \ttask     \testRows\toperator info\n" +
		"\tIndexLookUp_10\troot     \t1000   \t\n" +
		"\t├─IndexScan_8 \tcop[einsteindb]\t1000   \tcauset:t, index:k(a), range:[2,2], keep order:false, stats:pseudo\n" +
		"\t└─BlockScan_9 \tcop[einsteindb]\t1000   \tcauset:t, keep order:false, stats:pseudo"))

	// Unset stochastik variable.
	tk.MustInterDirc("set stochastik milevadb_enable_stmt_summary = ''")
	tk.MustQuery("select * from t where a=2")

	// Statement summary is disabled.
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.memexs_summary`,
	).Check(testkit.Rows())

	// Create a new stochastik to test
	tk = s.newTestKitWithRoot(c)

	tk.MustInterDirc("set global milevadb_enable_stmt_summary = on")
	tk.MustInterDirc("set global milevadb_stmt_summary_history_size = 24")

	// Create a new user to test memexs summary causet privilege
	tk.MustInterDirc("create user 'test_user'@'localhost'")
	tk.MustInterDirc("grant select on *.* to 'test_user'@'localhost'")
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "root",
		Hostname:     "%",
		AuthUsername: "root",
		AuthHostname: "%",
	}, nil, nil)
	tk.MustInterDirc("select * from t where a=1")
	result := tk.MustQuery(`select *
		from information_schema.memexs_summary
		where digest_text like 'select * from t%'`,
	)
	// Super user can query all records.
	c.Assert(len(result.Rows()), Equals, 1)
	result = tk.MustQuery(`select *
		from information_schema.memexs_summary_history
		where digest_text like 'select * from t%'`,
	)
	c.Assert(len(result.Rows()), Equals, 1)
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "test_user",
		Hostname:     "localhost",
		AuthUsername: "test_user",
		AuthHostname: "localhost",
	}, nil, nil)
	result = tk.MustQuery(`select *
		from information_schema.memexs_summary
		where digest_text like 'select * from t%'`,
	)
	// Ordinary users can not see others' records
	c.Assert(len(result.Rows()), Equals, 0)
	result = tk.MustQuery(`select *
		from information_schema.memexs_summary_history
		where digest_text like 'select * from t%'`,
	)
	c.Assert(len(result.Rows()), Equals, 0)
	tk.MustInterDirc("select * from t where a=1")
	result = tk.MustQuery(`select *
		from information_schema.memexs_summary
		where digest_text like 'select * from t%'`,
	)
	c.Assert(len(result.Rows()), Equals, 1)
	tk.MustInterDirc("select * from t where a=1")
	result = tk.MustQuery(`select *
		from information_schema.memexs_summary_history
		where digest_text like 'select * from t%'`,
	)
	c.Assert(len(result.Rows()), Equals, 1)
	// use root user to set variables back
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "root",
		Hostname:     "%",
		AuthUsername: "root",
		AuthHostname: "%",
	}, nil, nil)
}

func (s *testBlockSuite) TestIssue18845(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`CREATE USER 'user18845'@'localhost';`)
	tk.Se.Auth(&auth.UserIdentity{
		Username:     "user18845",
		Hostname:     "localhost",
		AuthUsername: "user18845",
		AuthHostname: "localhost",
	}, nil, nil)
	tk.MustQuery(`select count(*) from information_schema.defCausumns;`)
}

// Test memexs_summary_history.
func (s *testBlockSuite) TestStmtSummaryHistoryBlock(c *C) {
	tk := s.newTestKitWithRoot(c)
	tk.MustInterDirc("drop causet if exists test_summary")
	tk.MustInterDirc("create causet test_summary(a int, b varchar(10), key k(a))")

	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.milevadb_enable_stmt_summary").Check(testkit.Rows("1"))

	// Invalidate the cache manually so that milevadb_enable_stmt_summary works immediately.
	s.dom.GetGlobalVarsCache().Disable()
	// Disable refreshing summary.
	tk.MustInterDirc("set global milevadb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.milevadb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Create a new stochastik to test.
	tk = s.newTestKitWithRoot(c)

	// Test INSERT
	tk.MustInterDirc("insert into test_summary values(1, 'a')")
	tk.MustInterDirc("insert into test_summary    values(2, 'b')")
	tk.MustInterDirc("insert into TEST_SUMMARY VALUES(3, 'c')")
	tk.MustInterDirc("/**/insert into test_summary values(4, 'd')")
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text
		from information_schema.memexs_summary_history
		where digest_text like 'insert into test_summary%'`,
	).Check(testkit.Rows("Insert test test.test_summary <nil> 4 0 0 0 0 0 2 2 1 1 1 insert into test_summary values(1, 'a')"))

	tk.MustInterDirc("set global milevadb_stmt_summary_history_size = 0")
	tk.MustQuery(`select stmt_type, schema_name, block_names, index_names, exec_count, sum_cop_task_num, avg_total_keys,
		max_total_keys, avg_processed_keys, max_processed_keys, avg_write_keys, max_write_keys, avg_prewrite_regions,
		max_prewrite_regions, avg_affected_rows, query_sample_text, plan
		from information_schema.memexs_summary_history`,
	).Check(testkit.Rows())
}

// Test memexs_summary_history.
func (s *testBlockSuite) TestStmtSummaryInternalQuery(c *C) {
	tk := s.newTestKitWithRoot(c)

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b varchar(10), key k(a))")

	// We use the allegrosql binding evolve to check the internal query summary.
	tk.MustInterDirc("set @@milevadb_use_plan_baselines = 1")
	tk.MustInterDirc("set @@milevadb_evolve_plan_baselines = 1")
	tk.MustInterDirc("create global binding for select * from t where t.a = 1 using select * from t ignore index(k) where t.a = 1")
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.milevadb_enable_stmt_summary").Check(testkit.Rows("1"))
	// Invalidate the cache manually so that milevadb_enable_stmt_summary works immediately.
	s.dom.GetGlobalVarsCache().Disable()
	// Disable refreshing summary.
	tk.MustInterDirc("set global milevadb_stmt_summary_refresh_interval = 999999999")
	tk.MustQuery("select @@global.milevadb_stmt_summary_refresh_interval").Check(testkit.Rows("999999999"))

	// Test Internal

	// Create a new stochastik to test.
	tk = s.newTestKitWithRoot(c)

	tk.MustInterDirc("select * from t where t.a = 1")
	tk.MustQuery(`select exec_count, digest_text
		from information_schema.memexs_summary
		where digest_text like "select original_sql , bind_sql , default_db , status%"`).Check(testkit.Rows())

	// Enable internal query and evolve baseline.
	tk.MustInterDirc("set global milevadb_stmt_summary_internal_query = 1")
	defer tk.MustInterDirc("set global milevadb_stmt_summary_internal_query = false")

	// Create a new stochastik to test.
	tk = s.newTestKitWithRoot(c)

	tk.MustInterDirc("admin flush bindings")
	tk.MustInterDirc("admin evolve bindings")

	// `exec_count` may be bigger than 1 because other cases are also running.
	tk.MustQuery(`select digest_text
		from information_schema.memexs_summary
		where digest_text like "select original_sql , bind_sql , default_db , status%"`).Check(testkit.Rows(
		"select original_sql , bind_sql , default_db , status , create_time , uFIDelate_time , charset , defCauslation , source from allegrosql . bind_info" +
			" where uFIDelate_time > ? order by uFIDelate_time"))
}

// Test error count and warning count.
func (s *testBlockSuite) TestStmtSummaryErrorCount(c *C) {
	tk := s.newTestKitWithRoot(c)

	// Clear summaries.
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 0")
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 1")

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists stmt_summary_test")
	tk.MustInterDirc("create causet stmt_summary_test(id int primary key)")
	tk.MustInterDirc("insert into stmt_summary_test values(1)")
	_, err := tk.InterDirc("insert into stmt_summary_test values(1)")
	c.Assert(err, NotNil)

	tk.MustQuery(`select exec_count, sum_errors, sum_warnings
		from information_schema.memexs_summary
		where digest_text like "insert into stmt_summary_test%"`).Check(testkit.Rows("2 1 0"))

	tk.MustInterDirc("insert ignore into stmt_summary_test values(1)")
	tk.MustQuery(`select exec_count, sum_errors, sum_warnings
		from information_schema.memexs_summary
		where digest_text like "insert ignore into stmt_summary_test%"`).Check(testkit.Rows("1 0 1"))
}

func (s *testBlockSuite) TestStmtSummaryPreparedStatements(c *C) {
	tk := s.newTestKitWithRoot(c)

	// Clear summaries.
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 0")
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 1")

	tk.MustInterDirc("use test")
	tk.MustInterDirc("prepare stmt from 'select ?'")
	tk.MustInterDirc("set @number=1")
	tk.MustInterDirc("execute stmt using @number")

	tk.MustQuery(`select exec_count
		from information_schema.memexs_summary
		where digest_text like "prepare%"`).Check(testkit.Rows())
	tk.MustQuery(`select exec_count
		from information_schema.memexs_summary
		where digest_text like "select ?"`).Check(testkit.Rows("1"))
}

func (s *testBlockSuite) TestStmtSummarySensitiveQuery(c *C) {
	tk := s.newTestKitWithRoot(c)
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 0")
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 1")
	tk.MustInterDirc("drop user if exists user_sensitive;")
	tk.MustInterDirc("create user user_sensitive identified by '123456789';")
	tk.MustInterDirc("alter user 'user_sensitive'@'%' identified by 'abcdefg';")
	tk.MustInterDirc("set password for 'user_sensitive'@'%' = 'xyzuvw';")
	tk.MustQuery("select query_sample_text from `information_schema`.`STATEMENTS_SUMMARY` " +
		"where query_sample_text like '%user_sensitive%' and " +
		"(query_sample_text like 'set password%' or query_sample_text like 'create user%' or query_sample_text like 'alter user%') " +
		"order by query_sample_text;").
		Check(testkit.Rows(
			"alter user {user_sensitive@% password = ***}",
			"create user {user_sensitive@% password = ***}",
			"set password for user user_sensitive@%",
		))
}

func (s *testBlockSuite) TestPerformanceSchemaforCausetCache(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	causetembedded.SetPreparedCausetCache(true)

	tk := s.newTestKitWithCausetCache(c)

	// Clear summaries.
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 0")
	tk.MustInterDirc("set global milevadb_enable_stmt_summary = 1")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int)")
	tk.MustInterDirc("prepare stmt from 'select * from t'")
	tk.MustInterDirc("execute stmt")
	tk.MustQuery("select plan_cache_hits, plan_in_cache from information_schema.memexs_summary where digest_text='select * from t'").Check(
		testkit.Rows("0 0"))
	tk.MustInterDirc("execute stmt")
	tk.MustInterDirc("execute stmt")
	tk.MustInterDirc("execute stmt")
	tk.MustQuery("select plan_cache_hits, plan_in_cache from information_schema.memexs_summary where digest_text='select * from t'").Check(
		testkit.Rows("3 1"))
}
