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

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	berolinaAllegroSQLtypes "github.com/whtcorpsinc/berolinaAllegroSQL/types"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/petri"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/privilege/privileges"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

func (s *testSuite5) TestShowVisibility(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database showdatabase")
	tk.MustExec("use showdatabase")
	tk.MustExec("create block t1 (id int)")
	tk.MustExec("create block t2 (id int)")
	tk.MustExec(`create user 'show'@'%'`)

	tk1 := testkit.NewTestKit(c, s.causetstore)
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "show", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se

	// No ShowDatabases privilege, this user would see nothing except INFORMATION_SCHEMA.
	tk.MustQuery("show databases").Check(testkit.Events("INFORMATION_SCHEMA"))

	// After grant, the user can see the database.
	tk.MustExec(`grant select on showdatabase.t1 to 'show'@'%'`)
	tk1.MustQuery("show databases").Check(testkit.Events("INFORMATION_SCHEMA", "showdatabase"))

	// The user can see t1 but not t2.
	tk1.MustExec("use showdatabase")
	tk1.MustQuery("show blocks").Check(testkit.Events("t1"))

	// After revoke, show database result should be just except INFORMATION_SCHEMA.
	tk.MustExec(`revoke select on showdatabase.t1 from 'show'@'%'`)
	tk1.MustQuery("show databases").Check(testkit.Events("INFORMATION_SCHEMA"))

	// Grant any global privilege would make show databases available.
	tk.MustExec(`grant CREATE on *.* to 'show'@'%'`)
	rows := tk1.MustQuery("show databases").Events()
	c.Assert(len(rows), GreaterEqual, 2) // At least INFORMATION_SCHEMA and showdatabase

	tk.MustExec(`drop user 'show'@'%'`)
	tk.MustExec("drop database showdatabase")
}

func (s *testSuite5) TestShowDatabasesSchemaReplicantFirst(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("show databases").Check(testkit.Events("INFORMATION_SCHEMA"))
	tk.MustExec(`create user 'show'@'%'`)

	tk.MustExec(`create database AAAA`)
	tk.MustExec(`create database BBBB`)
	tk.MustExec(`grant select on AAAA.* to 'show'@'%'`)
	tk.MustExec(`grant select on BBBB.* to 'show'@'%'`)

	tk1 := testkit.NewTestKit(c, s.causetstore)
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "show", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se
	tk1.MustQuery("show databases").Check(testkit.Events("INFORMATION_SCHEMA", "AAAA", "BBBB"))

	tk.MustExec(`drop user 'show'@'%'`)
	tk.MustExec(`drop database AAAA`)
	tk.MustExec(`drop database BBBB`)
}

func (s *testSuite5) TestShowWarnings(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	testALLEGROSQL := `create block if not exists show_warnings (a int)`
	tk.MustExec(testALLEGROSQL)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("insert show_warnings values ('a')")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect FLOAT value: 'a'"))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(0))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect FLOAT value: 'a'"))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(0))

	// Test Warning level 'Error'
	testALLEGROSQL = `create block show_warnings (a int)`
	tk.Exec(testALLEGROSQL)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Error|1050|Block 'test.show_warnings' already exists"))
	tk.MustQuery("select @@error_count").Check(solitonutil.EventsWithSep("|", "1"))

	// Test Warning level 'Note'
	testALLEGROSQL = `create block show_warnings_2 (a int)`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `create block if not exists show_warnings_2 like show_warnings`
	tk.Exec(testALLEGROSQL)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Note|1050|Block 'test.show_warnings_2' already exists"))
	tk.MustQuery("select @@warning_count").Check(solitonutil.EventsWithSep("|", "1"))
	tk.MustQuery("select @@warning_count").Check(solitonutil.EventsWithSep("|", "0"))
}

func (s *testSuite5) TestShowErrors(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	testALLEGROSQL := `create block if not exists show_errors (a int)`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `create block show_errors (a int)`
	tk.Exec(testALLEGROSQL)

	tk.MustQuery("show errors").Check(solitonutil.EventsWithSep("|", "Error|1050|Block 'test.show_errors' already exists"))
}

func (s *testSuite5) TestShowGrantsPrivilege(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create user show_grants")
	tk.MustExec("show grants for show_grants")
	tk1 := testkit.NewTestKit(c, s.causetstore)
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "show_grants", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se
	err = tk1.QueryToErr("show grants for root")
	c.Assert(err.Error(), Equals, executor.ErrDBaccessDenied.GenWithStackByArgs("show_grants", "%", allegrosql.SystemDB).Error())
	// Test show grants for user with auth host name `%`.
	tk2 := testkit.NewTestKit(c, s.causetstore)
	se2, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se2.Auth(&auth.UserIdentity{Username: "show_grants", Hostname: "127.0.0.1", AuthUsername: "show_grants", AuthHostname: "%"}, nil, nil), IsTrue)
	tk2.Se = se2
	tk2.MustQuery("show grants")
}

func (s *testSuite5) TestShowStatsPrivilege(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create user show_stats")
	tk1 := testkit.NewTestKit(c, s.causetstore)
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "show_stats", Hostname: "%"}, nil, nil), IsTrue)
	tk1.Se = se
	eqErr := plannercore.ErrDBaccessDenied.GenWithStackByArgs("show_stats", "%", allegrosql.SystemDB)
	_, err = tk1.Exec("show stats_meta")
	c.Assert(err.Error(), Equals, eqErr.Error())
	_, err = tk1.Exec("SHOW STATS_BUCKETS")
	c.Assert(err.Error(), Equals, eqErr.Error())
	_, err = tk1.Exec("SHOW STATS_HEALTHY")
	c.Assert(err.Error(), Equals, eqErr.Error())
	_, err = tk1.Exec("SHOW STATS_HISTOGRAMS")
	c.Assert(err.Error(), Equals, eqErr.Error())
	tk.MustExec("grant select on allegrosql.* to show_stats")
	tk1.MustExec("show stats_meta")
	tk1.MustExec("SHOW STATS_BUCKETS")
	tk1.MustExec("SHOW STATS_HEALTHY")
	tk1.MustExec("SHOW STATS_HISTOGRAMS")
}

func (s *testSuite5) TestIssue18878(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil), IsTrue)
	tk.Se = se
	tk.MustQuery("select user()").Check(testkit.Events("root@127.0.0.1"))
	tk.MustQuery("show grants")
	tk.MustQuery("select user()").Check(testkit.Events("root@127.0.0.1"))
	err = tk.QueryToErr("show grants for root@127.0.0.1")
	c.Assert(err.Error(), Equals, privileges.ErrNonexistingGrant.FastGenByArgs("root", "127.0.0.1").Error())
	err = tk.QueryToErr("show grants for root@localhost")
	c.Assert(err.Error(), Equals, privileges.ErrNonexistingGrant.FastGenByArgs("root", "localhost").Error())
	err = tk.QueryToErr("show grants for root@1.1.1.1")
	c.Assert(err.Error(), Equals, privileges.ErrNonexistingGrant.FastGenByArgs("root", "1.1.1.1").Error())
	tk.MustExec("create user `show_grants`@`127.0.%`")
	err = tk.QueryToErr("show grants for `show_grants`@`127.0.0.1`")
	c.Assert(err.Error(), Equals, privileges.ErrNonexistingGrant.FastGenByArgs("show_grants", "127.0.0.1").Error())
	tk.MustQuery("show grants for `show_grants`@`127.0.%`")
}

func (s *testSuite5) TestIssue17794(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("CREATE USER 'root'@'8.8.%'")
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "9.9.9.9", AuthHostname: "%"}, nil, nil), IsTrue)
	tk.Se = se

	tk1 := testkit.NewTestKit(c, s.causetstore)
	se1, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se1.Auth(&auth.UserIdentity{Username: "root", Hostname: "8.8.8.8", AuthHostname: "8.8.%"}, nil, nil), IsTrue)
	tk1.Se = se1

	tk.MustQuery("show grants").Check(testkit.Events("GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"))
	tk1.MustQuery("show grants").Check(testkit.Events("GRANT USAGE ON *.* TO 'root'@'8.8.%'"))
}

func (s *testSuite5) TestIssue3641(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	_, err := tk.Exec("show blocks;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
	_, err = tk.Exec("show block status;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
}

func (s *testSuite5) TestIssue10549(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("CREATE DATABASE newdb;")
	tk.MustExec("CREATE ROLE 'app_developer';")
	tk.MustExec("GRANT ALL ON newdb.* TO 'app_developer';")
	tk.MustExec("CREATE USER 'dev';")
	tk.MustExec("GRANT 'app_developer' TO 'dev';")
	tk.MustExec("SET DEFAULT ROLE app_developer TO 'dev';")

	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "dev", Hostname: "%", AuthUsername: "dev", AuthHostname: "%"}, nil, nil), IsTrue)
	tk.MustQuery("SHOW DATABASES;").Check(testkit.Events("INFORMATION_SCHEMA", "newdb"))
	tk.MustQuery("SHOW GRANTS;").Check(testkit.Events("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT ALL PRIVILEGES ON newdb.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
	tk.MustQuery("SHOW GRANTS FOR CURRENT_USER").Check(testkit.Events("GRANT USAGE ON *.* TO 'dev'@'%'", "GRANT 'app_developer'@'%' TO 'dev'@'%'"))
}

func (s *testSuite5) TestIssue11165(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("CREATE ROLE 'r_manager';")
	tk.MustExec("CREATE USER 'manager'@'localhost';")
	tk.MustExec("GRANT 'r_manager' TO 'manager'@'localhost';")

	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "manager", Hostname: "localhost", AuthUsername: "manager", AuthHostname: "localhost"}, nil, nil), IsTrue)
	tk.MustExec("SET DEFAULT ROLE ALL TO 'manager'@'localhost';")
	tk.MustExec("SET DEFAULT ROLE NONE TO 'manager'@'localhost';")
	tk.MustExec("SET DEFAULT ROLE 'r_manager' TO 'manager'@'localhost';")
}

// TestShow2 is moved from stochastik_test
func (s *testSuite5) TestShow2(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("set global autocommit=0")
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Events("autocommit 0"))
	tk.MustExec("set global autocommit = 1")
	tk2 := testkit.NewTestKit(c, s.causetstore)
	// TODO: In MyALLEGROSQL, the result is "autocommit ON".
	tk2.MustQuery("show global variables where variable_name = 'autocommit'").Check(testkit.Events("autocommit 1"))

	// TODO: Specifying the charset for national char/varchar should not be supported.
	tk.MustExec("drop block if exists test_full_defCausumn")
	tk.MustExec(`create block test_full_defCausumn(
					c_int int,
					c_float float,
					c_bit bit,
					c_bool bool,
					c_char char(1) charset ascii defCauslate ascii_bin,
					c_nchar national char(1) charset ascii defCauslate ascii_bin,
					c_binary binary,
					c_varchar varchar(1) charset ascii defCauslate ascii_bin,
					c_varchar_default varchar(20) charset ascii defCauslate ascii_bin default 'cUrrent_tImestamp',
					c_nvarchar national varchar(1) charset ascii defCauslate ascii_bin,
					c_varbinary varbinary(1),
					c_year year,
					c_date date,
					c_time time,
					c_datetime datetime,
					c_datetime_default datetime default current_timestamp,
					c_datetime_default_2 datetime(2) default current_timestamp(2),
					c_timestamp timestamp,
					c_timestamp_default timestamp default current_timestamp,
					c_timestamp_default_3 timestamp(3) default current_timestamp(3),
					c_timestamp_default_4 timestamp(3) default current_timestamp(3) on uFIDelate current_timestamp(3),
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
				);`)

	tk.MustQuery(`show full defCausumns from test_full_defCausumn`).Check(testkit.Events(
		"" +
			"c_int int(11) <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_float float <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_bit bit(1) <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_bool tinyint(1) <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_char char(1) ascii_bin YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_nchar char(1) ascii_bin YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_binary binary(1) <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_varchar varchar(1) ascii_bin YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_varchar_default varchar(20) ascii_bin YES  cUrrent_tImestamp  select,insert,uFIDelate,references ]\n" +
			"[c_nvarchar varchar(1) ascii_bin YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_varbinary varbinary(1) <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_year year(4) <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_date date <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_time time <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_datetime datetime <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_datetime_default datetime <nil> YES  CURRENT_TIMESTAMP  select,insert,uFIDelate,references ]\n" +
			"[c_datetime_default_2 datetime(2) <nil> YES  CURRENT_TIMESTAMP(2)  select,insert,uFIDelate,references ]\n" +
			"[c_timestamp timestamp <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_timestamp_default timestamp <nil> YES  CURRENT_TIMESTAMP  select,insert,uFIDelate,references ]\n" +
			"[c_timestamp_default_3 timestamp(3) <nil> YES  CURRENT_TIMESTAMP(3)  select,insert,uFIDelate,references ]\n" +
			"[c_timestamp_default_4 timestamp(3) <nil> YES  CURRENT_TIMESTAMP(3) DEFAULT_GENERATED on uFIDelate CURRENT_TIMESTAMP(3) select,insert,uFIDelate,references ]\n" +
			"[c_blob blob <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_tinyblob tinyblob <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_mediumblob mediumblob <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_longblob longblob <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_text text ascii_bin YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_tinytext tinytext ascii_bin YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_mediumtext mediumtext ascii_bin YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_longtext longtext ascii_bin YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_json json <nil> YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_enum enum('1') ascii_bin YES  <nil>  select,insert,uFIDelate,references ]\n" +
			"[c_set set('1') ascii_bin YES  <nil>  select,insert,uFIDelate,references "))

	tk.MustExec("drop block if exists test_full_defCausumn")

	tk.MustExec("drop block if exists t")
	tk.MustExec(`create block if not exists t (c int) comment '注释'`)
	tk.MustExec("create or replace definer='root'@'localhost' view v as select * from t")
	tk.MustQuery(`show defCausumns from t`).Check(solitonutil.EventsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`describe t`).Check(solitonutil.EventsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`show defCausumns from v`).Check(solitonutil.EventsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery(`describe v`).Check(solitonutil.EventsWithSep(",", "c,int(11),YES,,<nil>,"))
	tk.MustQuery("show defCauslation where Charset = 'utf8' and DefCauslation = 'utf8_bin'").Check(solitonutil.EventsWithSep(",", "utf8_bin,utf8,83,Yes,Yes,1"))
	tk.MustExec(`drop sequence if exists seq`)
	tk.MustExec(`create sequence seq`)
	tk.MustQuery("show blocks").Check(testkit.Events("seq", "t", "v"))
	tk.MustQuery("show full blocks").Check(testkit.Events("seq SEQUENCE", "t BASE TABLE", "v VIEW"))

	// Bug 19427
	tk.MustQuery("SHOW FULL TABLES in INFORMATION_SCHEMA like 'VIEWS'").Check(testkit.Events("VIEWS SYSTEM VIEW"))
	tk.MustQuery("SHOW FULL TABLES in information_schema like 'VIEWS'").Check(testkit.Events("VIEWS SYSTEM VIEW"))
	tk.MustQuery("SHOW FULL TABLES in metrics_schema like 'uptime'").Check(testkit.Events("uptime SYSTEM VIEW"))

	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tblInfo, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	createTime := perceptron.TSConvert2Time(tblInfo.Meta().UFIDelateTS).Format("2006-01-02 15:04:05")

	// The Hostname is the actual host
	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	r := tk.MustQuery("show block status from test like 't'")
	r.Check(testkit.Events(fmt.Sprintf("t InnoDB 10 Compact 0 0 0 0 0 0 <nil> %s <nil> <nil> utf8mb4_bin   注释", createTime)))

	tk.MustQuery("show databases like 'test'").Check(testkit.Events("test"))

	tk.MustExec(`grant all on *.* to 'root'@'%'`)
	tk.MustQuery("show grants").Check(testkit.Events(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))

	tk.MustQuery("show grants for current_user()").Check(testkit.Events(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))
	tk.MustQuery("show grants for current_user").Check(testkit.Events(`GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION`))
}

func (s *testSuite5) TestShowCreateUser(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Create a new user.
	tk.MustExec(`CREATE USER 'test_show_create_user'@'%' IDENTIFIED BY 'root';`)
	tk.MustQuery("show create user 'test_show_create_user'@'%'").
		Check(testkit.Events(`CREATE USER 'test_show_create_user'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*81F5E21E35407D884A6CD4A731AEBFB6AF209E1B' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK`))

	tk.MustExec(`CREATE USER 'test_show_create_user'@'localhost' IDENTIFIED BY 'test';`)
	tk.MustQuery("show create user 'test_show_create_user'@'localhost';").
		Check(testkit.Events(`CREATE USER 'test_show_create_user'@'localhost' IDENTIFIED WITH 'mysql_native_password' AS '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK`))

	// Case: the user exists but the host portion doesn't match
	err := tk.QueryToErr("show create user 'test_show_create_user'@'asdf';")
	c.Assert(err.Error(), Equals, executor.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER", "'test_show_create_user'@'asdf'").Error())

	// Case: a user that doesn't exist
	err = tk.QueryToErr("show create user 'aaa'@'localhost';")
	c.Assert(err.Error(), Equals, executor.ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER", "'aaa'@'localhost'").Error())

	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, nil)
	rows := tk.MustQuery("show create user current_user")
	rows.Check(testkit.Events("CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	rows = tk.MustQuery("show create user current_user()")
	rows.Check(testkit.Events("CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	tk.MustExec("create user 'check_priv'")

	// "show create user" for other user requires the SELECT privilege on allegrosql database.
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("use allegrosql")
	succ := tk1.Se.Auth(&auth.UserIdentity{Username: "check_priv", Hostname: "127.0.0.1", AuthUsername: "test_show", AuthHostname: "asdf"}, nil, nil)
	c.Assert(succ, IsTrue)
	err = tk1.QueryToErr("show create user 'root'@'%'")
	c.Assert(err, NotNil)

	// "show create user" for current user doesn't check privileges.
	rows = tk1.MustQuery("show create user current_user")
	rows.Check(testkit.Events("CREATE USER 'check_priv'@'127.0.0.1' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))
}

func (s *testSuite5) TestUnprivilegedShow(c *C) {

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("CREATE DATABASE testshow")
	tk.MustExec("USE testshow")
	tk.MustExec("CREATE TABLE t1 (a int)")
	tk.MustExec("CREATE TABLE t2 (a int)")

	tk.MustExec(`CREATE USER 'lowprivuser'`) // no grants

	tk.Se.Auth(&auth.UserIdentity{Username: "lowprivuser", Hostname: "192.168.0.1", AuthUsername: "lowprivuser", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	rs, err := tk.Exec("SHOW TABLE STATUS FROM testshow")
	c.Assert(err, IsNil)
	c.Assert(rs, NotNil)

	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tk.MustExec("GRANT ALL ON testshow.t1 TO 'lowprivuser'")
	tk.Se.Auth(&auth.UserIdentity{Username: "lowprivuser", Hostname: "192.168.0.1", AuthUsername: "lowprivuser", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tblInfo, err := is.BlockByName(perceptron.NewCIStr("testshow"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	createTime := perceptron.TSConvert2Time(tblInfo.Meta().UFIDelateTS).Format("2006-01-02 15:04:05")

	tk.MustQuery("show block status from testshow").Check(testkit.Events(fmt.Sprintf("t1 InnoDB 10 Compact 0 0 0 0 0 0 <nil> %s <nil> <nil> utf8mb4_bin   ", createTime)))

}

func (s *testSuite5) TestDefCauslation(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	rs, err := tk.Exec("show defCauslation;")
	c.Assert(err, IsNil)
	fields := rs.Fields()
	c.Assert(fields[0].DeferredCauset.Tp, Equals, allegrosql.TypeVarchar)
	c.Assert(fields[1].DeferredCauset.Tp, Equals, allegrosql.TypeVarchar)
	c.Assert(fields[2].DeferredCauset.Tp, Equals, allegrosql.TypeLonglong)
	c.Assert(fields[3].DeferredCauset.Tp, Equals, allegrosql.TypeVarchar)
	c.Assert(fields[4].DeferredCauset.Tp, Equals, allegrosql.TypeVarchar)
	c.Assert(fields[5].DeferredCauset.Tp, Equals, allegrosql.TypeLonglong)
}

func (s *testSuite5) TestShowBlockStatus(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a bigint);`)

	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "192.168.0.1", AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	// It's not easy to test the result contents because every time the test runs, "Create_time" changed.
	tk.MustExec("show block status;")
	rs, err := tk.Exec("show block status;")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err := stochastik.GetEvents4Test(context.Background(), tk.Se, rs)
	c.Assert(errors.ErrorStack(err), Equals, "")
	err = rs.Close()
	c.Assert(errors.ErrorStack(err), Equals, "")

	for i := range rows {
		event := rows[i]
		c.Assert(event.GetString(0), Equals, "t")
		c.Assert(event.GetString(1), Equals, "InnoDB")
		c.Assert(event.GetInt64(2), Equals, int64(10))
		c.Assert(event.GetString(3), Equals, "Compact")
	}
	tk.MustExec(`drop block if exists tp;`)
	tk.MustExec(`create block tp (a int)
 		partition by range(a)
 		( partition p0 values less than (10),
		  partition p1 values less than (20),
		  partition p2 values less than (maxvalue)
  		);`)
	rs, err = tk.Exec("show block status from test like 'tp';")
	c.Assert(errors.ErrorStack(err), Equals, "")
	rows, err = stochastik.GetEvents4Test(context.Background(), tk.Se, rs)
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rows[0].GetString(16), Equals, "partitioned")
}

func (s *testSuite5) TestShowSlow(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// The test result is volatile, because
	// 1. Slow queries is stored in petri, which may be affected by other tests.
	// 2. DefCauslecting slow queries is a asynchronous process, check immediately may not get the expected result.
	// 3. Make slow query like "select sleep(1)" would slow the CI.
	// So, we just cover the code but do not check the result.
	tk.MustQuery(`admin show slow recent 3`)
	tk.MustQuery(`admin show slow top 3`)
	tk.MustQuery(`admin show slow top internal 3`)
	tk.MustQuery(`admin show slow top all 3`)
}

func (s *testSuite5) TestShowOpenBlocks(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("show open blocks")
	tk.MustQuery("show open blocks in test")
}

func (s *testSuite5) TestShowCreateBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(a int,b int)")
	tk.MustExec("drop view if exists v1")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select * from t1")
	tk.MustQuery("show create block v1").Check(solitonutil.EventsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v1` (`a`, `b`) AS SELECT `test`.`t1`.`a`,`test`.`t1`.`b` FROM `test`.`t1`  "))
	tk.MustQuery("show create view v1").Check(solitonutil.EventsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v1` (`a`, `b`) AS SELECT `test`.`t1`.`a`,`test`.`t1`.`b` FROM `test`.`t1`  "))
	tk.MustExec("drop view v1")
	tk.MustExec("drop block t1")

	tk.MustExec("drop view if exists v")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v as select JSON_MERGE('{}', '{}') as defCaus;")
	tk.MustQuery("show create view v").Check(solitonutil.EventsWithSep("|", "v|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v` (`defCaus`) AS SELECT JSON_MERGE('{}', '{}') AS `defCaus`  "))
	tk.MustExec("drop view if exists v")

	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(a int,b int)")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select avg(a),t1.* from t1 group by a")
	tk.MustQuery("show create view v1").Check(solitonutil.EventsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v1` (`avg(a)`, `a`, `b`) AS SELECT AVG(`a`),`test`.`t1`.`a`,`test`.`t1`.`b` FROM `test`.`t1` GROUP BY `a`  "))
	tk.MustExec("drop view v1")
	tk.MustExec("create or replace definer=`root`@`127.0.0.1` view v1 as select a+b, t1.* , a as c from t1")
	tk.MustQuery("show create view v1").Check(solitonutil.EventsWithSep("|", "v1|CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`127.0.0.1` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v1` (`a+b`, `a`, `b`, `c`) AS SELECT `a`+`b`,`test`.`t1`.`a`,`test`.`t1`.`b`,`a` AS `c` FROM `test`.`t1`  "))
	tk.MustExec("drop block t1")
	tk.MustExec("drop view v1")

	// For issue #9211
	tk.MustExec("create block t(c int, b int as (c + 1))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustQuery("show create block `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) GENERATED ALWAYS AS (`c` + 1) VIRTUAL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop block t")
	tk.MustExec("create block t(c int, b int as (c + 1) not null)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustQuery("show create block `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) GENERATED ALWAYS AS (`c` + 1) VIRTUAL NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop block t")
	tk.MustExec("create block t ( a char(10) charset utf8 defCauslate utf8_bin, b char(10) as (rtrim(a)));")
	tk.MustQuery("show create block `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` char(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"+
			"  `b` char(10) GENERATED ALWAYS AS (rtrim(`a`)) VIRTUAL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop block t")

	tk.MustExec(`drop block if exists different_charset`)
	tk.MustExec(`create block different_charset(ch1 varchar(10) charset utf8, ch2 varchar(10) charset binary);`)
	tk.MustQuery(`show create block different_charset`).Check(solitonutil.EventsWithSep("|",
		""+
			"different_charset CREATE TABLE `different_charset` (\n"+
			"  `ch1` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"+
			"  `ch2` varbinary(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block `t` (\n" +
		"`a` timestamp not null default current_timestamp,\n" +
		"`b` timestamp(3) default current_timestamp(3),\n" +
		"`c` datetime default current_timestamp,\n" +
		"`d` datetime(4) default current_timestamp(4),\n" +
		"`e` varchar(20) default 'cUrrent_tImestamp',\n" +
		"`f` datetime(2) default current_timestamp(2) on uFIDelate current_timestamp(2),\n" +
		"`g` timestamp(2) default current_timestamp(2) on uFIDelate current_timestamp(2))")
	tk.MustQuery("show create block `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"+
			"  `b` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),\n"+
			"  `c` datetime DEFAULT CURRENT_TIMESTAMP,\n"+
			"  `d` datetime(4) DEFAULT CURRENT_TIMESTAMP(4),\n"+
			"  `e` varchar(20) DEFAULT 'cUrrent_tImestamp',\n"+
			"  `f` datetime(2) DEFAULT CURRENT_TIMESTAMP(2) ON UFIDelATE CURRENT_TIMESTAMP(2),\n"+
			"  `g` timestamp(2) DEFAULT CURRENT_TIMESTAMP(2) ON UFIDelATE CURRENT_TIMESTAMP(2)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("drop block t")

	tk.MustExec("create block t (a int, b int) shard_row_id_bits = 4 pre_split_regions=3;")
	tk.MustQuery("show create block `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin/*!90000 SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 */",
	))
	tk.MustExec("drop block t")

	tk.MustExec("CREATE TABLE `log` (" +
		"`LOG_ID` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT," +
		"`ROUND_ID` bigint(20) UNSIGNED NOT NULL," +
		"`USER_ID` int(10) UNSIGNED NOT NULL," +
		"`USER_IP` int(10) UNSIGNED DEFAULT NULL," +
		"`END_TIME` datetime NOT NULL," +
		"`USER_TYPE` int(11) DEFAULT NULL," +
		"`APP_ID` int(11) DEFAULT NULL," +
		"PRIMARY KEY (`LOG_ID`,`END_TIME`)," +
		"KEY `IDX_EndTime` (`END_TIME`)," +
		"KEY `IDX_RoundId` (`ROUND_ID`)," +
		"KEY `IDX_UserId_EndTime` (`USER_ID`,`END_TIME`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=505488 " +
		"PARTITION BY RANGE ( month(`end_time`) ) (" +
		"PARTITION `p1` VALUES LESS THAN (2)," +
		"PARTITION `p2` VALUES LESS THAN (3)," +
		"PARTITION `p3` VALUES LESS THAN (4)," +
		"PARTITION `p4` VALUES LESS THAN (5)," +
		"PARTITION `p5` VALUES LESS THAN (6)," +
		"PARTITION `p6` VALUES LESS THAN (7)," +
		"PARTITION `p7` VALUES LESS THAN (8)," +
		"PARTITION `p8` VALUES LESS THAN (9)," +
		"PARTITION `p9` VALUES LESS THAN (10)," +
		"PARTITION `p10` VALUES LESS THAN (11)," +
		"PARTITION `p11` VALUES LESS THAN (12)," +
		"PARTITION `p12` VALUES LESS THAN (MAXVALUE))")
	tk.MustQuery("show create block log").Check(solitonutil.EventsWithSep("|",
		"log CREATE TABLE `log` (\n"+
			"  `LOG_ID` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n"+
			"  `ROUND_ID` bigint(20) unsigned NOT NULL,\n"+
			"  `USER_ID` int(10) unsigned NOT NULL,\n"+
			"  `USER_IP` int(10) unsigned DEFAULT NULL,\n"+
			"  `END_TIME` datetime NOT NULL,\n"+
			"  `USER_TYPE` int(11) DEFAULT NULL,\n"+
			"  `APP_ID` int(11) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`LOG_ID`,`END_TIME`),\n"+
			"  KEY `IDX_EndTime` (`END_TIME`),\n"+
			"  KEY `IDX_RoundId` (`ROUND_ID`),\n"+
			"  KEY `IDX_UserId_EndTime` (`USER_ID`,`END_TIME`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=505488\n"+
			"PARTITION BY RANGE ( MONTH(`end_time`) ) (\n"+
			"  PARTITION `p1` VALUES LESS THAN (2),\n"+
			"  PARTITION `p2` VALUES LESS THAN (3),\n"+
			"  PARTITION `p3` VALUES LESS THAN (4),\n"+
			"  PARTITION `p4` VALUES LESS THAN (5),\n"+
			"  PARTITION `p5` VALUES LESS THAN (6),\n"+
			"  PARTITION `p6` VALUES LESS THAN (7),\n"+
			"  PARTITION `p7` VALUES LESS THAN (8),\n"+
			"  PARTITION `p8` VALUES LESS THAN (9),\n"+
			"  PARTITION `p9` VALUES LESS THAN (10),\n"+
			"  PARTITION `p10` VALUES LESS THAN (11),\n"+
			"  PARTITION `p11` VALUES LESS THAN (12),\n"+
			"  PARTITION `p12` VALUES LESS THAN (MAXVALUE)\n"+
			")"))

	// for issue #11831
	tk.MustExec("create block ttt4(a varchar(123) default null defCauslate utf8mb4_unicode_ci)engine=innodb default charset=utf8mb4 defCauslate=utf8mb4_unicode_ci;")
	tk.MustQuery("show create block `ttt4`").Check(solitonutil.EventsWithSep("|",
		""+
			"ttt4 CREATE TABLE `ttt4` (\n"+
			"  `a` varchar(123) COLLATE utf8mb4_unicode_ci DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
	))
	tk.MustExec("create block ttt5(a varchar(123) default null)engine=innodb default charset=utf8mb4 defCauslate=utf8mb4_bin;")
	tk.MustQuery("show create block `ttt5`").Check(solitonutil.EventsWithSep("|",
		""+
			"ttt5 CREATE TABLE `ttt5` (\n"+
			"  `a` varchar(123) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// for expression index
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a int, b real);")
	tk.MustExec("alter block t add index expr_idx((a*b+1));")
	tk.MustQuery("show create block t;").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` double DEFAULT NULL,\n"+
			"  KEY `expr_idx` ((`a` * `b` + 1))\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Fix issue #15175, show create block sequence_name.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustQuery("show create block seq;").Check(testkit.Events("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	// Test for issue #15633, 'binary' defCauslation should be ignored in the result of 'show create block'.
	tk.MustExec(`drop block if exists binary_defCauslate`)
	tk.MustExec(`create block binary_defCauslate(a varchar(10)) default defCauslate=binary;`)
	tk.MustQuery(`show create block binary_defCauslate`).Check(solitonutil.EventsWithSep("|",
		""+
			"binary_defCauslate CREATE TABLE `binary_defCauslate` (\n"+
			"  `a` varbinary(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=binary", // binary defCauslate is ignored
	))
	tk.MustExec(`drop block if exists binary_defCauslate`)
	tk.MustExec(`create block binary_defCauslate(a varchar(10)) default charset=binary defCauslate=binary;`)
	tk.MustQuery(`show create block binary_defCauslate`).Check(solitonutil.EventsWithSep("|",
		""+
			"binary_defCauslate CREATE TABLE `binary_defCauslate` (\n"+
			"  `a` varbinary(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=binary", // binary defCauslate is ignored
	))
	tk.MustExec(`drop block if exists binary_defCauslate`)
	tk.MustExec(`create block binary_defCauslate(a varchar(10)) default charset=utf8mb4 defCauslate=utf8mb4_bin;`)
	tk.MustQuery(`show create block binary_defCauslate`).Check(solitonutil.EventsWithSep("|",
		""+
			"binary_defCauslate CREATE TABLE `binary_defCauslate` (\n"+
			"  `a` varchar(10) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", // non-binary defCauslate is kept.
	))
	// Test for issue #17 in bug competition, default num and sequence should be shown without quote.
	tk.MustExec(`drop block if exists default_num`)
	tk.MustExec("create block default_num(a int default 11)")
	tk.MustQuery("show create block default_num").Check(solitonutil.EventsWithSep("|",
		""+
			"default_num CREATE TABLE `default_num` (\n"+
			"  `a` int(11) DEFAULT 11\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec(`drop block if exists default_varchar`)
	tk.MustExec("create block default_varchar(a varchar(10) default \"haha\")")
	tk.MustQuery("show create block default_varchar").Check(solitonutil.EventsWithSep("|",
		""+
			"default_varchar CREATE TABLE `default_varchar` (\n"+
			"  `a` varchar(10) DEFAULT 'haha'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec(`drop block if exists default_sequence`)
	tk.MustExec("create block default_sequence(a int default nextval(seq))")
	tk.MustQuery("show create block default_sequence").Check(solitonutil.EventsWithSep("|",
		""+
			"default_sequence CREATE TABLE `default_sequence` (\n"+
			"  `a` int(11) DEFAULT nextval(`test`.`seq`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// MilevaDB defaults (and only supports) foreign_key_checks=0
	// This means that the child block can be created before the parent block.
	// This behavior is required for mysqldump restores.
	tk.MustExec(`DROP TABLE IF EXISTS parent, child`)
	tk.MustExec(`CREATE TABLE child (id INT NOT NULL PRIMARY KEY auto_increment, parent_id INT NOT NULL, INDEX par_ind (parent_id), CONSTRAINT child_ibfk_1 FOREIGN KEY (parent_id) REFERENCES parent(id))`)
	tk.MustExec(`CREATE TABLE parent ( id INT NOT NULL PRIMARY KEY auto_increment )`)
	tk.MustQuery(`show create block child`).Check(solitonutil.EventsWithSep("|",
		""+
			"child CREATE TABLE `child` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  `parent_id` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`id`),\n"+
			"  KEY `par_ind` (`parent_id`),\n"+
			"  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Test Foreign keys + ON DELETE / ON UFIDelATE
	tk.MustExec(`DROP TABLE child`)
	tk.MustExec(`CREATE TABLE child (id INT NOT NULL PRIMARY KEY auto_increment, parent_id INT NOT NULL, INDEX par_ind (parent_id), CONSTRAINT child_ibfk_1 FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL ON UFIDelATE CASCADE)`)
	tk.MustQuery(`show create block child`).Check(solitonutil.EventsWithSep("|",
		""+
			"child CREATE TABLE `child` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  `parent_id` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`id`),\n"+
			"  KEY `par_ind` (`parent_id`),\n"+
			"  CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE SET NULL ON UFIDelATE CASCADE\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

}

func (s *testAutoRandomSuite) TestShowCreateBlockAutoRandom(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	// Basic show create block.
	tk.MustExec("create block auto_random_tbl1 (a bigint primary key auto_random(3), b varchar(255))")
	tk.MustQuery("show create block `auto_random_tbl1`").Check(solitonutil.EventsWithSep("|",
		""+
			"auto_random_tbl1 CREATE TABLE `auto_random_tbl1` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(3) */,\n"+
			"  `b` varchar(255) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Implicit auto_random value should be shown explicitly.
	tk.MustExec("create block auto_random_tbl2 (a bigint auto_random primary key, b char)")
	tk.MustQuery("show create block auto_random_tbl2").Check(solitonutil.EventsWithSep("|",
		""+
			"auto_random_tbl2 CREATE TABLE `auto_random_tbl2` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` char(1) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Special version comment can be shown in MilevaDB with new version.
	tk.MustExec("create block auto_random_tbl3 (a bigint /*T![auto_rand] auto_random */ primary key)")
	tk.MustQuery("show create block auto_random_tbl3").Check(solitonutil.EventsWithSep("|",
		""+
			"auto_random_tbl3 CREATE TABLE `auto_random_tbl3` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	// Test show auto_random block option.
	tk.MustExec("create block auto_random_tbl4 (a bigint primary key auto_random(5), b varchar(255)) auto_random_base = 100")
	tk.MustQuery("show create block `auto_random_tbl4`").Check(solitonutil.EventsWithSep("|",
		""+
			"auto_random_tbl4 CREATE TABLE `auto_random_tbl4` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` varchar(255) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=100 */",
	))
	// Test implicit auto_random with auto_random block option.
	tk.MustExec("create block auto_random_tbl5 (a bigint auto_random primary key, b char) auto_random_base 50")
	tk.MustQuery("show create block auto_random_tbl5").Check(solitonutil.EventsWithSep("|",
		""+
			"auto_random_tbl5 CREATE TABLE `auto_random_tbl5` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` char(1) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=50 */",
	))
	// Test auto_random block option already with special comment.
	tk.MustExec("create block auto_random_tbl6 (a bigint /*T![auto_rand] auto_random */ primary key) auto_random_base 200")
	tk.MustQuery("show create block auto_random_tbl6").Check(solitonutil.EventsWithSep("|",
		""+
			"auto_random_tbl6 CREATE TABLE `auto_random_tbl6` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=200 */",
	))
}

// Override testAutoRandomSuite to test auto id cache.
func (s *testAutoRandomSuite) TestAutoIdCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int auto_increment key) auto_id_cache = 10")
	tk.MustQuery("show create block t").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=10 */",
	))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int auto_increment unique, b int key) auto_id_cache 100")
	tk.MustQuery("show create block t").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  `b` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`b`),\n"+
			"  UNIQUE KEY `a` (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=100 */",
	))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int key) auto_id_cache 5")
	tk.MustQuery("show create block t").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`a`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_id_cache] AUTO_ID_CACHE=5 */",
	))
}

func (s *testAutoRandomSuite) TestAutoRandomBase(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/meta/autoid/mockAutoIDChange"), IsNil)
	}()

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a bigint primary key auto_random(5), b int unique key auto_increment) auto_random_base = 100, auto_increment = 100")
	tk.MustQuery("show create block t").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`a`),\n"+
			"  UNIQUE KEY `b` (`b`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=100 /*T![auto_rand_base] AUTO_RANDOM_BASE=100 */",
	))

	tk.MustExec("insert into t(`a`) values (1000)")
	tk.MustQuery("show create block t").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `a` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,\n"+
			"  `b` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`a`),\n"+
			"  UNIQUE KEY `b` (`b`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=5100 /*T![auto_rand_base] AUTO_RANDOM_BASE=6001 */",
	))
}

func (s *testSuite5) TestShowEscape(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists `t``abl\"e`")
	tk.MustExec("create block `t``abl\"e`(`c``olum\"n` int(11) primary key)")
	tk.MustQuery("show create block `t``abl\"e`").Check(solitonutil.EventsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE `t``abl\"e` (\n"+
			"  `c``olum\"n` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`c``olum\"n`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// ANSI_QUOTES will change the SHOW output
	tk.MustExec("set @old_sql_mode=@@sql_mode")
	tk.MustExec("set sql_mode=ansi_quotes")
	tk.MustQuery("show create block \"t`abl\"\"e\"").Check(solitonutil.EventsWithSep("|",
		""+
			"t`abl\"e CREATE TABLE \"t`abl\"\"e\" (\n"+
			"  \"c`olum\"\"n\" int(11) NOT NULL,\n"+
			"  PRIMARY KEY (\"c`olum\"\"n\")\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("rename block \"t`abl\"\"e\" to t")
	tk.MustExec("set sql_mode=@old_sql_mode")
}

func (s *testSuite5) TestShowBuiltin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	res := tk.MustQuery("show builtins;")
	c.Assert(res, NotNil)
	rows := res.Events()
	c.Assert(268, Equals, len(rows))
	c.Assert("abs", Equals, rows[0][0].(string))
	c.Assert("yearweek", Equals, rows[267][0].(string))
}

func (s *testSuite5) TestShowClusterConfig(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	var confItems [][]types.Causet
	var confErr error
	var confFunc executor.TestShowClusterConfigFunc = func() ([][]types.Causet, error) {
		return confItems, confErr
	}
	tk.Se.SetValue(executor.TestShowClusterConfigKey, confFunc)
	strs2Items := func(strs ...string) []types.Causet {
		items := make([]types.Causet, 0, len(strs))
		for _, s := range strs {
			items = append(items, types.NewStringCauset(s))
		}
		return items
	}
	confItems = append(confItems, strs2Items("milevadb", "127.0.0.1:1111", "log.level", "info"))
	confItems = append(confItems, strs2Items("fidel", "127.0.0.1:2222", "log.level", "info"))
	confItems = append(confItems, strs2Items("einsteindb", "127.0.0.1:3333", "log.level", "info"))
	tk.MustQuery("show config").Check(testkit.Events(
		"milevadb 127.0.0.1:1111 log.level info",
		"fidel 127.0.0.1:2222 log.level info",
		"einsteindb 127.0.0.1:3333 log.level info"))
	tk.MustQuery("show config where type='milevadb'").Check(testkit.Events(
		"milevadb 127.0.0.1:1111 log.level info"))
	tk.MustQuery("show config where type like '%ti%'").Check(testkit.Events(
		"milevadb 127.0.0.1:1111 log.level info",
		"einsteindb 127.0.0.1:3333 log.level info"))

	confErr = fmt.Errorf("something unknown error")
	c.Assert(tk.QueryToErr("show config"), ErrorMatches, confErr.Error())
}

func (s *testSerialSuite1) TestShowCreateBlockWithIntegerDisplayLengthWarnings(c *C) {
	berolinaAllegroSQLtypes.MilevaDBStrictIntegerDisplayWidth = true
	defer func() {
		berolinaAllegroSQLtypes.MilevaDBStrictIntegerDisplayWidth = false
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int(2), b varchar(2))")
	tk.MustQuery("show warnings").Check(testkit.Events("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [berolinaAllegroSQL:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create block t").Check(testkit.Events("t CREATE TABLE `t` (\n" +
		"  `a` int DEFAULT NULL,\n" +
		"  `b` varchar(2) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a bigint(10), b bigint)")
	tk.MustQuery("show warnings").Check(testkit.Events("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [berolinaAllegroSQL:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create block t").Check(testkit.Events("t CREATE TABLE `t` (\n" +
		"  `a` bigint DEFAULT NULL,\n" +
		"  `b` bigint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a tinyint(5), b tinyint(2), c tinyint)")
	// Here it will occur 2 warnings.
	tk.MustQuery("show warnings").Check(testkit.Events("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [berolinaAllegroSQL:1681]Integer display width is deprecated and will be removed in a future release.",
		"Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [berolinaAllegroSQL:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create block t").Check(testkit.Events("t CREATE TABLE `t` (\n" +
		"  `a` tinyint DEFAULT NULL,\n" +
		"  `b` tinyint DEFAULT NULL,\n" +
		"  `c` tinyint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a smallint(5), b smallint)")
	tk.MustQuery("show warnings").Check(testkit.Events("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [berolinaAllegroSQL:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create block t").Check(testkit.Events("t CREATE TABLE `t` (\n" +
		"  `a` smallint DEFAULT NULL,\n" +
		"  `b` smallint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a mediumint(5), b mediumint)")
	tk.MustQuery("show warnings").Check(testkit.Events("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [berolinaAllegroSQL:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create block t").Check(testkit.Events("t CREATE TABLE `t` (\n" +
		"  `a` mediumint DEFAULT NULL,\n" +
		"  `b` mediumint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int1(1), b int2(2), c int3, d int4, e int8)")
	tk.MustQuery("show warnings").Check(testkit.Events("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [berolinaAllegroSQL:1681]Integer display width is deprecated and will be removed in a future release.",
		"Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [berolinaAllegroSQL:1681]Integer display width is deprecated and will be removed in a future release."))
	tk.MustQuery("show create block t").Check(testkit.Events("t CREATE TABLE `t` (\n" +
		"  `a` tinyint DEFAULT NULL,\n" +
		"  `b` smallint DEFAULT NULL,\n" +
		"  `c` mediumint DEFAULT NULL,\n" +
		"  `d` int DEFAULT NULL,\n" +
		"  `e` bigint DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}
