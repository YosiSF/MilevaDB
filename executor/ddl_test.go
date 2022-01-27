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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/dbs"
	dbsutil "github.com/whtcorpsinc/milevadb/dbs/soliton"
	dbssolitonutil "github.com/whtcorpsinc/milevadb/dbs/solitonutil"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/petri"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

func (s *testSuite6) TestTruncateBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists truncate_test;`)
	tk.MustExec(`create block truncate_test (a int)`)
	tk.MustExec(`insert truncate_test values (1),(2),(3)`)
	result := tk.MustQuery("select * from truncate_test")
	result.Check(testkit.Events("1", "2", "3"))
	tk.MustExec("truncate block truncate_test")
	result = tk.MustQuery("select * from truncate_test")
	result.Check(nil)
}

// TestInTxnExecDBSFail tests the following case:
//  1. Execute the ALLEGROALLEGROSQL of "begin";
//  2. A ALLEGROALLEGROSQL that will fail to execute;
//  3. Execute DBS.
func (s *testSuite6) TestInTxnExecDBSFail(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t (i int key);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1);")
	_, err := tk.Exec("truncate block t;")
	c.Assert(err.Error(), Equals, "[ekv:1062]Duplicate entry '1' for key 'PRIMARY'")
	result := tk.MustQuery("select count(*) from t")
	result.Check(testkit.Events("1"))
}

func (s *testSuite6) TestCreateBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	// Test create an exist database
	_, err := tk.Exec("CREATE database test")
	c.Assert(err, NotNil)

	// Test create an exist block
	tk.MustExec("CREATE TABLE create_test (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	_, err = tk.Exec("CREATE TABLE create_test (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	c.Assert(err, NotNil)

	// Test "if not exist"
	tk.MustExec("CREATE TABLE if not exists test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// Testcase for https://github.com/whtcorpsinc/milevadb/issues/312
	tk.MustExec(`create block issue312_1 (c float(24));`)
	tk.MustExec(`create block issue312_2 (c float(25));`)
	rs, err := tk.Exec(`desc issue312_1`)
	c.Assert(err, IsNil)
	ctx := context.Background()
	req := rs.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	for {
		err1 := rs.Next(ctx, req)
		c.Assert(err1, IsNil)
		if req.NumEvents() == 0 {
			break
		}
		for event := it.Begin(); event != it.End(); event = it.Next() {
			c.Assert(event.GetString(1), Equals, "float")
		}
	}
	rs, err = tk.Exec(`desc issue312_2`)
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	it = chunk.NewIterator4Chunk(req)
	for {
		err1 := rs.Next(ctx, req)
		c.Assert(err1, IsNil)
		if req.NumEvents() == 0 {
			break
		}
		for event := it.Begin(); event != it.End(); event = it.Next() {
			c.Assert(req.GetEvent(0).GetString(1), Equals, "double")
		}
	}

	// test multiple defCauslate specified in defCausumn when create.
	tk.MustExec("drop block if exists test_multiple_defCausumn_defCauslate;")
	tk.MustExec("create block test_multiple_defCausumn_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	t, err := petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("test_multiple_defCausumn_defCauslate"))
	c.Assert(err, IsNil)
	c.Assert(t.DefCauss()[0].Charset, Equals, "utf8")
	c.Assert(t.DefCauss()[0].DefCauslate, Equals, "utf8_general_ci")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().DefCauslate, Equals, "utf8mb4_bin")

	tk.MustExec("drop block if exists test_multiple_defCausumn_defCauslate;")
	tk.MustExec("create block test_multiple_defCausumn_defCauslate (a char(1) charset utf8 defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	t, err = petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("test_multiple_defCausumn_defCauslate"))
	c.Assert(err, IsNil)
	c.Assert(t.DefCauss()[0].Charset, Equals, "utf8")
	c.Assert(t.DefCauss()[0].DefCauslate, Equals, "utf8_general_ci")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().DefCauslate, Equals, "utf8mb4_bin")

	// test Err case for multiple defCauslate specified in defCausumn when create.
	tk.MustExec("drop block if exists test_err_multiple_defCauslate;")
	_, err = tk.Exec("create block test_err_multiple_defCauslate (a char(1) charset utf8mb4 defCauslate utf8_unicode_ci defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, dbs.ErrDefCauslationCharsetMismatch.GenWithStackByArgs("utf8_unicode_ci", "utf8mb4").Error())

	tk.MustExec("drop block if exists test_err_multiple_defCauslate;")
	_, err = tk.Exec("create block test_err_multiple_defCauslate (a char(1) defCauslate utf8_unicode_ci defCauslate utf8mb4_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, dbs.ErrDefCauslationCharsetMismatch.GenWithStackByArgs("utf8mb4_general_ci", "utf8").Error())

	// block option is auto-increment
	tk.MustExec("drop block if exists create_auto_increment_test;")
	tk.MustExec("create block create_auto_increment_test (id int not null auto_increment, name varchar(255), primary key(id)) auto_increment = 999;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('bb')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('cc')")
	r := tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Events("999 aa", "1000 bb", "1001 cc"))
	tk.MustExec("drop block create_auto_increment_test")
	tk.MustExec("create block create_auto_increment_test (id int not null auto_increment, name varchar(255), primary key(id)) auto_increment = 1999;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('bb')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('cc')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Events("1999 aa", "2000 bb", "2001 cc"))
	tk.MustExec("drop block create_auto_increment_test")
	tk.MustExec("create block create_auto_increment_test (id int not null auto_increment, name varchar(255), key(id)) auto_increment = 1000;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Events("1000 aa"))

	// Test for `drop block if exists`.
	tk.MustExec("drop block if exists t_if_exists;")
	tk.MustQuery("show warnings;").Check(testkit.Events("Note 1051 Unknown block 'test.t_if_exists'"))
	tk.MustExec("create block if not exists t1_if_exists(c int)")
	tk.MustExec("drop block if exists t1_if_exists,t2_if_exists,t3_if_exists")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Note|1051|Unknown block 'test.t2_if_exists'", "Note|1051|Unknown block 'test.t3_if_exists'"))
}

func (s *testSuite6) TestCreateView(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	//create an source block
	tk.MustExec("CREATE TABLE source_block (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	//test create a exist view
	tk.MustExec("CREATE VIEW view_t AS select id , name from source_block")
	defer tk.MustExec("DROP VIEW IF EXISTS view_t")
	_, err := tk.Exec("CREATE VIEW view_t AS select id , name from source_block")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1050]Block 'test.view_t' already exists")
	//create view on nonexistent block
	_, err = tk.Exec("create view v1 (c,d) as select a,b from t1")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.t1' doesn't exist")
	//simple view
	tk.MustExec("create block t1 (a int ,b int)")
	tk.MustExec("insert into t1 values (1,2), (1,3), (2,4), (2,5), (3,10)")
	//view with defCausList and SelectFieldExpr
	tk.MustExec("create view v1 (c) as select b+1 from t1")
	//view with SelectFieldExpr
	tk.MustExec("create view v2 as select b+1 from t1")
	//view with SelectFieldExpr and AsName
	tk.MustExec("create view v3 as select b+1 as c from t1")
	//view with defCausList , SelectField and AsName
	tk.MustExec("create view v4 (c) as select b+1 as d from t1")
	//view with select wild card
	tk.MustExec("create view v5 as select * from t1")
	tk.MustExec("create view v6 (c,d) as select * from t1")
	_, err = tk.Exec("create view v7 (c,d,e) as select * from t1")
	c.Assert(err.Error(), Equals, dbs.ErrViewWrongList.Error())
	//drop multiple views in a statement
	tk.MustExec("drop view v1,v2,v3,v4,v5,v6")
	//view with variable
	tk.MustExec("create view v1 (c,d) as select a,b+@@global.max_user_connections from t1")
	_, err = tk.Exec("create view v1 (c,d) as select a,b from t1 where a = @@global.max_user_connections")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1050]Block 'test.v1' already exists")
	tk.MustExec("drop view v1")
	//view with different defCaus counts
	_, err = tk.Exec("create view v1 (c,d,e) as select a,b from t1 ")
	c.Assert(err.Error(), Equals, dbs.ErrViewWrongList.Error())
	_, err = tk.Exec("create view v1 (c) as select a,b from t1 ")
	c.Assert(err.Error(), Equals, dbs.ErrViewWrongList.Error())
	//view with or_replace flag
	tk.MustExec("drop view if exists v1")
	tk.MustExec("create view v1 (c,d) as select a,b from t1")
	tk.MustExec("create or replace view v1 (c,d) as select a,b from t1 ")
	tk.MustExec("create block if not exists t1 (a int ,b int)")
	_, err = tk.Exec("create or replace view t1 as select * from t1")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "t1", "VIEW").Error())
	// create view using prepare
	tk.MustExec(`prepare stmt from "create view v10 (x) as select 1";`)
	tk.MustExec("execute stmt")

	// create view on union
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("drop view if exists v")
	_, err = tk.Exec("create view v as select * from t1 union select * from t2")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotExists), IsTrue)
	tk.MustExec("create block t1(a int, b int)")
	tk.MustExec("create block t2(a int, b int)")
	tk.MustExec("insert into t1 values(1,2), (1,1), (1,2)")
	tk.MustExec("insert into t2 values(1,1),(1,3)")
	tk.MustExec("create definer='root'@'localhost' view v as select * from t1 union select * from t2")
	tk.MustQuery("select * from v").Sort().Check(testkit.Events("1 1", "1 2", "1 3"))
	tk.MustExec("alter block t1 drop defCausumn a")
	_, err = tk.Exec("select * from v")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrViewInvalid), IsTrue)
	tk.MustExec("alter block t1 add defCausumn a int")
	tk.MustQuery("select * from v").Sort().Check(testkit.Events("1 1", "1 3", "<nil> 1", "<nil> 2"))
	tk.MustExec("alter block t1 drop defCausumn a")
	tk.MustExec("alter block t2 drop defCausumn b")
	_, err = tk.Exec("select * from v")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrViewInvalid), IsTrue)
	tk.MustExec("drop view v")

	tk.MustExec("create view v as (select * from t1)")
	tk.MustExec("drop view v")
	tk.MustExec("create view v as (select * from t1 union select * from t2)")
	tk.MustExec("drop view v")

	// Test for `drop view if exists`.
	tk.MustExec("drop view if exists v_if_exists;")
	tk.MustQuery("show warnings;").Check(testkit.Events("Note 1051 Unknown block 'test.v_if_exists'"))
	tk.MustExec("create view v1_if_exists as (select * from t1)")
	tk.MustExec("drop view if exists v1_if_exists,v2_if_exists,v3_if_exists")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Note|1051|Unknown block 'test.v2_if_exists'", "Note|1051|Unknown block 'test.v3_if_exists'"))

	// Test for create nested view.
	tk.MustExec("create block test_v_nested(a int)")
	tk.MustExec("create definer='root'@'localhost' view v_nested as select * from test_v_nested")
	tk.MustExec("create definer='root'@'localhost' view v_nested2 as select * from v_nested")
	_, err = tk.Exec("create or replace definer='root'@'localhost' view v_nested as select * from v_nested2")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrNoSuchBlock), IsTrue)
	tk.MustExec("drop block test_v_nested")
	tk.MustExec("drop view v_nested, v_nested2")
}

func (s *testSuite6) TestIssue16250(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block if not exists t(a int)")
	tk.MustExec("create view view_issue16250 as select * from t")
	_, err := tk.Exec("truncate block view_issue16250")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.view_issue16250' doesn't exist")
}

func (s testSuite6) TestTruncateSequence(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create sequence if not exists seq")
	_, err := tk.Exec("truncate block seq")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.seq' doesn't exist")
	tk.MustExec("create sequence if not exists seq1 start 10 increment 2 maxvalue 10000 cycle")
	_, err = tk.Exec("truncate block seq1")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.seq1' doesn't exist")
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("drop sequence if exists seq1")
}

func (s *testSuite6) TestCreateViewWithOverlongDefCausName(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a int)")
	defer tk.MustExec("drop block t")
	tk.MustExec("create view v as select distinct'" + strings.Repeat("a", 65) + "', " +
		"max('" + strings.Repeat("b", 65) + "'), " +
		"'cccccccccc', '" + strings.Repeat("d", 65) + "';")
	resultCreateStmt := "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v` (`name_exp_1`, `name_exp_2`, `cccccccccc`, `name_exp_4`) AS SELECT DISTINCT '" + strings.Repeat("a", 65) + "',MAX('" + strings.Repeat("b", 65) + "'),'cccccccccc','" + strings.Repeat("d", 65) + "'"
	tk.MustQuery("select * from v")
	tk.MustQuery("select name_exp_1, name_exp_2, cccccccccc, name_exp_4 from v")
	tk.MustQuery("show create view v").Check(testkit.Events("v " + resultCreateStmt + "  "))
	tk.MustExec("drop view v;")
	tk.MustExec(resultCreateStmt)

	tk.MustExec("drop view v ")
	tk.MustExec("create definer='root'@'localhost' view v as select 'a', '" + strings.Repeat("b", 65) + "' from t " +
		"union select '" + strings.Repeat("c", 65) + "', " +
		"count(distinct '" + strings.Repeat("b", 65) + "', " +
		"'c');")
	resultCreateStmt = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v` (`a`, `name_exp_2`) AS SELECT 'a','" + strings.Repeat("b", 65) + "' FROM `test`.`t` UNION SELECT '" + strings.Repeat("c", 65) + "',COUNT(DISTINCT '" + strings.Repeat("b", 65) + "', 'c')"
	tk.MustQuery("select * from v")
	tk.MustQuery("select a, name_exp_2 from v")
	tk.MustQuery("show create view v").Check(testkit.Events("v " + resultCreateStmt + "  "))
	tk.MustExec("drop view v;")
	tk.MustExec(resultCreateStmt)

	tk.MustExec("drop view v ")
	tk.MustExec("create definer='root'@'localhost' view v as select 'a' as '" + strings.Repeat("b", 65) + "' from t;")
	tk.MustQuery("select * from v")
	tk.MustQuery("select name_exp_1 from v")
	resultCreateStmt = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v` (`name_exp_1`) AS SELECT 'a' AS `" + strings.Repeat("b", 65) + "` FROM `test`.`t`"
	tk.MustQuery("show create view v").Check(testkit.Events("v " + resultCreateStmt + "  "))
	tk.MustExec("drop view v;")
	tk.MustExec(resultCreateStmt)

	tk.MustExec("drop view v ")
	err := tk.ExecToErr("create view v(`" + strings.Repeat("b", 65) + "`) as select a from t;")
	c.Assert(err.Error(), Equals, "[dbs:1059]Identifier name 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' is too long")
}

func (s *testSuite6) TestCreateDroFIDelatabase(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists drop_test;")
	tk.MustExec("drop database if exists drop_test;")
	tk.MustExec("create database drop_test;")
	tk.MustExec("use drop_test;")
	tk.MustExec("drop database drop_test;")
	_, err := tk.Exec("drop block t;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
	err = tk.ExecToErr("select * from t;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())

	_, err = tk.Exec("drop database allegrosql")
	c.Assert(err, NotNil)

	tk.MustExec("create database charset_test charset ascii;")
	tk.MustQuery("show create database charset_test;").Check(solitonutil.EventsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET ascii */",
	))
	tk.MustExec("drop database charset_test;")
	tk.MustExec("create database charset_test charset binary;")
	tk.MustQuery("show create database charset_test;").Check(solitonutil.EventsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET binary */",
	))
	tk.MustExec("drop database charset_test;")
	tk.MustExec("create database charset_test defCauslate utf8_general_ci;")
	tk.MustQuery("show create database charset_test;").Check(solitonutil.EventsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci */",
	))
	tk.MustExec("drop database charset_test;")
	tk.MustExec("create database charset_test charset utf8 defCauslate utf8_general_ci;")
	tk.MustQuery("show create database charset_test;").Check(solitonutil.EventsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci */",
	))
	tk.MustGetErrMsg("create database charset_test charset utf8 defCauslate utf8mb4_unicode_ci;", "[dbs:1253]COLLATION 'utf8mb4_unicode_ci' is not valid for CHARACTER SET 'utf8'")
}

func (s *testSuite6) TestCreateDropBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block if not exists drop_test (a int)")
	tk.MustExec("drop block if exists drop_test")
	tk.MustExec("create block drop_test (a int)")
	tk.MustExec("drop block drop_test")

	_, err := tk.Exec("drop block allegrosql.gc_delete_range")
	c.Assert(err, NotNil)
}

func (s *testSuite6) TestCreateDropView(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create or replace view drop_test as select 1,2")

	_, err := tk.Exec("drop block drop_test")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1051]Unknown block 'test.drop_test'")

	_, err = tk.Exec("drop view if exists drop_test")
	c.Assert(err, IsNil)

	_, err = tk.Exec("drop view allegrosql.gc_delete_range")
	c.Assert(err.Error(), Equals, "Drop milevadb system block 'allegrosql.gc_delete_range' is forbidden")

	_, err = tk.Exec("drop view drop_test")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1051]Unknown block 'test.drop_test'")

	tk.MustExec("create block t_v(a int)")
	_, err = tk.Exec("drop view t_v")
	c.Assert(err.Error(), Equals, "[dbs:1347]'test.t_v' is not VIEW")

	tk.MustExec("create block t_v1(a int, b int);")
	tk.MustExec("create block t_v2(a int, b int);")
	tk.MustExec("create view v as select * from t_v1;")
	tk.MustExec("create or replace view v  as select * from t_v2;")
	tk.MustQuery("select * from information_schema.views where block_name ='v';").Check(
		testkit.Events("def test v SELECT `test`.`t_v2`.`a`,`test`.`t_v2`.`b` FROM `test`.`t_v2` CASCADED NO @ DEFINER utf8mb4 utf8mb4_bin"))
}

func (s *testSuite6) TestCreateDropIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block if not exists drop_test (a int)")
	tk.MustExec("create index idx_a on drop_test (a)")
	tk.MustExec("drop index idx_a on drop_test")
	tk.MustExec("drop block drop_test")
}

func (s *testSuite6) TestAlterBlockAddDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block if not exists alter_test (c1 int)")
	tk.MustExec("insert into alter_test values(1)")
	tk.MustExec("alter block alter_test add defCausumn c2 timestamp default current_timestamp")
	time.Sleep(1 * time.Millisecond)
	now := time.Now().Add(-1 * time.Millisecond).Format(types.TimeFormat)
	r, err := tk.Exec("select c2 from alter_test")
	c.Assert(err, IsNil)
	req := r.NewChunk()
	err = r.Next(context.Background(), req)
	c.Assert(err, IsNil)
	event := req.GetEvent(0)
	c.Assert(event.Len(), Equals, 1)
	c.Assert(now, GreaterEqual, event.GetTime(0).String())
	r.Close()
	tk.MustExec("alter block alter_test add defCausumn c3 varchar(50) default 'CURRENT_TIMESTAMP'")
	tk.MustQuery("select c3 from alter_test").Check(testkit.Events("CURRENT_TIMESTAMP"))
	tk.MustExec("create or replace view alter_view as select c1,c2 from alter_test")
	_, err = tk.Exec("alter block alter_view add defCausumn c4 varchar(50)")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error())
	tk.MustExec("drop view alter_view")
	tk.MustExec("create sequence alter_seq")
	_, err = tk.Exec("alter block alter_seq add defCausumn c int")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_seq", "BASE TABLE").Error())
	tk.MustExec("drop sequence alter_seq")
}

func (s *testSuite6) TestAlterBlockAddDeferredCausets(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block if not exists alter_test (c1 int)")
	tk.MustExec("insert into alter_test values(1)")
	tk.MustExec("alter block alter_test add defCausumn c2 timestamp default current_timestamp, add defCausumn c8 varchar(50) default 'CURRENT_TIMESTAMP'")
	tk.MustExec("alter block alter_test add defCausumn (c7 timestamp default current_timestamp, c3 varchar(50) default 'CURRENT_TIMESTAMP')")
	r, err := tk.Exec("select c2 from alter_test")
	c.Assert(err, IsNil)
	req := r.NewChunk()
	err = r.Next(context.Background(), req)
	c.Assert(err, IsNil)
	event := req.GetEvent(0)
	c.Assert(event.Len(), Equals, 1)
	r.Close()
	tk.MustQuery("select c3 from alter_test").Check(testkit.Events("CURRENT_TIMESTAMP"))
	tk.MustExec("create or replace view alter_view as select c1,c2 from alter_test")
	_, err = tk.Exec("alter block alter_view add defCausumn (c4 varchar(50), c5 varchar(50))")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error())
	tk.MustExec("drop view alter_view")
	tk.MustExec("create sequence alter_seq")
	_, err = tk.Exec("alter block alter_seq add defCausumn (c1 int, c2 varchar(10))")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_seq", "BASE TABLE").Error())
	tk.MustExec("drop sequence alter_seq")
}

func (s *testSuite6) TestAddNotNullDeferredCausetNoDefault(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block nn (c1 int)")
	tk.MustExec("insert nn values (1), (2)")
	tk.MustExec("alter block nn add defCausumn c2 int not null")

	tbl, err := petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("nn"))
	c.Assert(err, IsNil)
	defCaus2 := tbl.Meta().DeferredCausets[1]
	c.Assert(defCaus2.DefaultValue, IsNil)
	c.Assert(defCaus2.OriginDefaultValue, Equals, "0")

	tk.MustQuery("select * from nn").Check(testkit.Events("1 0", "2 0"))
	_, err = tk.Exec("insert nn (c1) values (3)")
	c.Check(err, NotNil)
	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert nn (c1) values (3)")
	tk.MustQuery("select * from nn").Check(testkit.Events("1 0", "2 0", "3 0"))
}

func (s *testSuite6) TestAlterBlockModifyDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists mc")
	tk.MustExec("create block mc(c1 int, c2 varchar(10), c3 bit)")
	_, err := tk.Exec("alter block mc modify defCausumn c1 short")
	c.Assert(err, NotNil)
	tk.MustExec("alter block mc modify defCausumn c1 bigint")

	_, err = tk.Exec("alter block mc modify defCausumn c2 blob")
	c.Assert(err, NotNil)

	_, err = tk.Exec("alter block mc modify defCausumn c2 varchar(8)")
	c.Assert(err, NotNil)
	tk.MustExec("alter block mc modify defCausumn c2 varchar(11)")
	tk.MustExec("alter block mc modify defCausumn c2 text(13)")
	tk.MustExec("alter block mc modify defCausumn c2 text")
	tk.MustExec("alter block mc modify defCausumn c3 bit")
	result := tk.MustQuery("show create block mc")
	createALLEGROSQL := result.Events()[0][1]
	expected := "CREATE TABLE `mc` (\n  `c1` bigint(20) DEFAULT NULL,\n  `c2` text DEFAULT NULL,\n  `c3` bit(1) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createALLEGROSQL, Equals, expected)
	tk.MustExec("create or replace view alter_view as select c1,c2 from mc")
	_, err = tk.Exec("alter block alter_view modify defCausumn c2 text")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error())
	tk.MustExec("drop view alter_view")
	tk.MustExec("create sequence alter_seq")
	_, err = tk.Exec("alter block alter_seq modify defCausumn c int")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_seq", "BASE TABLE").Error())
	tk.MustExec("drop sequence alter_seq")

	// test multiple defCauslate modification in defCausumn.
	tk.MustExec("drop block if exists modify_defCausumn_multiple_defCauslate")
	tk.MustExec("create block modify_defCausumn_multiple_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	_, err = tk.Exec("alter block modify_defCausumn_multiple_defCauslate modify defCausumn a char(1) defCauslate utf8mb4_bin;")
	c.Assert(err, IsNil)
	t, err := petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("modify_defCausumn_multiple_defCauslate"))
	c.Assert(err, IsNil)
	c.Assert(t.DefCauss()[0].Charset, Equals, "utf8mb4")
	c.Assert(t.DefCauss()[0].DefCauslate, Equals, "utf8mb4_bin")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().DefCauslate, Equals, "utf8mb4_bin")

	tk.MustExec("drop block if exists modify_defCausumn_multiple_defCauslate;")
	tk.MustExec("create block modify_defCausumn_multiple_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	_, err = tk.Exec("alter block modify_defCausumn_multiple_defCauslate modify defCausumn a char(1) charset utf8mb4 defCauslate utf8mb4_bin;")
	c.Assert(err, IsNil)
	t, err = petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("modify_defCausumn_multiple_defCauslate"))
	c.Assert(err, IsNil)
	c.Assert(t.DefCauss()[0].Charset, Equals, "utf8mb4")
	c.Assert(t.DefCauss()[0].DefCauslate, Equals, "utf8mb4_bin")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().DefCauslate, Equals, "utf8mb4_bin")

	// test Err case for multiple defCauslate modification in defCausumn.
	tk.MustExec("drop block if exists err_modify_multiple_defCauslate;")
	tk.MustExec("create block err_modify_multiple_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	_, err = tk.Exec("alter block err_modify_multiple_defCauslate modify defCausumn a char(1) charset utf8mb4 defCauslate utf8_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, dbs.ErrDefCauslationCharsetMismatch.GenWithStackByArgs("utf8_bin", "utf8mb4").Error())

	tk.MustExec("drop block if exists err_modify_multiple_defCauslate;")
	tk.MustExec("create block err_modify_multiple_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	_, err = tk.Exec("alter block err_modify_multiple_defCauslate modify defCausumn a char(1) defCauslate utf8_bin defCauslate utf8mb4_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, dbs.ErrDefCauslationCharsetMismatch.GenWithStackByArgs("utf8mb4_bin", "utf8").Error())

}

func (s *testSuite6) TestDefaultDBAfterDropCurDB(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	testALLEGROSQL := `create database if not exists test_db CHARACTER SET latin1 COLLATE latin1_swedish_ci;`
	tk.MustExec(testALLEGROSQL)

	testALLEGROSQL = `use test_db;`
	tk.MustExec(testALLEGROSQL)
	tk.MustQuery(`select database();`).Check(testkit.Events("test_db"))
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Events("latin1"))
	tk.MustQuery(`select @@defCauslation_database;`).Check(testkit.Events("latin1_swedish_ci"))

	testALLEGROSQL = `drop database test_db;`
	tk.MustExec(testALLEGROSQL)
	tk.MustQuery(`select database();`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Events(allegrosql.DefaultCharset))
	tk.MustQuery(`select @@defCauslation_database;`).Check(testkit.Events(allegrosql.DefaultDefCauslationName))
}

func (s *testSuite6) TestDeferredCausetCharsetAndDefCauslate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	dbName := "defCaus_charset_defCauslate"
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tests := []struct {
		defCausType     string
		charset         string
		defCauslates    string
		exptCharset     string
		exptDefCauslate string
		errMsg          string
	}{
		{
			defCausType:     "varchar(10)",
			charset:         "charset utf8",
			defCauslates:    "defCauslate utf8_bin",
			exptCharset:     "utf8",
			exptDefCauslate: "utf8_bin",
			errMsg:          "",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset utf8mb4",
			defCauslates:    "",
			exptCharset:     "utf8mb4",
			exptDefCauslate: "utf8mb4_bin",
			errMsg:          "",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset utf16",
			defCauslates:    "",
			exptCharset:     "",
			exptDefCauslate: "",
			errMsg:          "Unknown charset utf16",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset latin1",
			defCauslates:    "",
			exptCharset:     "latin1",
			exptDefCauslate: "latin1_bin",
			errMsg:          "",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset binary",
			defCauslates:    "",
			exptCharset:     "binary",
			exptDefCauslate: "binary",
			errMsg:          "",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset ascii",
			defCauslates:    "",
			exptCharset:     "ascii",
			exptDefCauslate: "ascii_bin",
			errMsg:          "",
		},
	}
	sctx := tk.Se.(stochastikctx.Context)
	dm := petri.GetPetri(sctx)
	for i, tt := range tests {
		tblName := fmt.Sprintf("t%d", i)
		allegrosql := fmt.Sprintf("create block %s (a %s %s %s)", tblName, tt.defCausType, tt.charset, tt.defCauslates)
		if tt.errMsg == "" {
			tk.MustExec(allegrosql)
			is := dm.SchemaReplicant()
			c.Assert(is, NotNil)

			tb, err := is.BlockByName(perceptron.NewCIStr(dbName), perceptron.NewCIStr(tblName))
			c.Assert(err, IsNil)
			c.Assert(tb.Meta().DeferredCausets[0].Charset, Equals, tt.exptCharset, Commentf(allegrosql))
			c.Assert(tb.Meta().DeferredCausets[0].DefCauslate, Equals, tt.exptDefCauslate, Commentf(allegrosql))
		} else {
			_, err := tk.Exec(allegrosql)
			c.Assert(err, NotNil, Commentf(allegrosql))
		}
	}
	tk.MustExec("drop database " + dbName)
}

func (s *testSuite6) TestTooLargeIdentifierLength(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	// for database.
	dbName1, dbName2 := strings.Repeat("a", allegrosql.MaxDatabaseNameLength), strings.Repeat("a", allegrosql.MaxDatabaseNameLength+1)
	tk.MustExec(fmt.Sprintf("create database %s", dbName1))
	tk.MustExec(fmt.Sprintf("drop database %s", dbName1))
	_, err := tk.Exec(fmt.Sprintf("create database %s", dbName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", dbName2))

	// for block.
	tk.MustExec("use test")
	blockName1, blockName2 := strings.Repeat("b", allegrosql.MaxBlockNameLength), strings.Repeat("b", allegrosql.MaxBlockNameLength+1)
	tk.MustExec(fmt.Sprintf("create block %s(c int)", blockName1))
	tk.MustExec(fmt.Sprintf("drop block %s", blockName1))
	_, err = tk.Exec(fmt.Sprintf("create block %s(c int)", blockName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", blockName2))

	// for defCausumn.
	tk.MustExec("drop block if exists t;")
	defCausumnName1, defCausumnName2 := strings.Repeat("c", allegrosql.MaxDeferredCausetNameLength), strings.Repeat("c", allegrosql.MaxDeferredCausetNameLength+1)
	tk.MustExec(fmt.Sprintf("create block t(%s int)", defCausumnName1))
	tk.MustExec("drop block t")
	_, err = tk.Exec(fmt.Sprintf("create block t(%s int)", defCausumnName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", defCausumnName2))

	// for index.
	tk.MustExec("create block t(c int);")
	indexName1, indexName2 := strings.Repeat("d", allegrosql.MaxIndexIdentifierLen), strings.Repeat("d", allegrosql.MaxIndexIdentifierLen+1)
	tk.MustExec(fmt.Sprintf("create index %s on t(c)", indexName1))
	tk.MustExec(fmt.Sprintf("drop index %s on t", indexName1))
	_, err = tk.Exec(fmt.Sprintf("create index %s on t(c)", indexName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", indexName2))

	// for create block with index.
	tk.MustExec("drop block t;")
	_, err = tk.Exec(fmt.Sprintf("create block t(c int, index %s(c));", indexName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", indexName2))
}

func (s *testSuite8) TestShardEventIDBits(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("create block t (a int) shard_row_id_bits = 15")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?)", i)
	}

	dom := petri.GetPetri(tk.Se)
	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)

	assertCountAndShard := func(t block.Block, expectCount int) {
		var hasShardedID bool
		var count int
		c.Assert(tk.Se.NewTxn(context.Background()), IsNil)
		err = t.IterRecords(tk.Se, t.FirstKey(), nil, func(h ekv.Handle, rec []types.Causet, defcaus []*block.DeferredCauset) (more bool, err error) {
			c.Assert(h.IntValue(), GreaterEqual, int64(0))
			first8bits := h.IntValue() >> 56
			if first8bits > 0 {
				hasShardedID = true
			}
			count++
			return true, nil
		})
		c.Assert(err, IsNil)
		c.Assert(count, Equals, expectCount)
		c.Assert(hasShardedID, IsTrue)
	}

	assertCountAndShard(tbl, 100)

	// After PR 10759, shard_row_id_bits is supported with blocks with auto_increment defCausumn.
	tk.MustExec("create block auto (id int not null auto_increment unique) shard_row_id_bits = 4")
	tk.MustExec("alter block auto shard_row_id_bits = 5")
	tk.MustExec("drop block auto")
	tk.MustExec("create block auto (id int not null auto_increment unique) shard_row_id_bits = 0")
	tk.MustExec("alter block auto shard_row_id_bits = 5")
	tk.MustExec("drop block auto")
	tk.MustExec("create block auto (id int not null auto_increment unique)")
	tk.MustExec("alter block auto shard_row_id_bits = 5")
	tk.MustExec("drop block auto")
	tk.MustExec("create block auto (id int not null auto_increment unique) shard_row_id_bits = 4")
	tk.MustExec("alter block auto shard_row_id_bits = 0")
	tk.MustExec("drop block auto")

	// After PR 10759, shard_row_id_bits is not supported with pk_is_handle blocks.
	err = tk.ExecToErr("create block auto (id int not null auto_increment primary key, b int) shard_row_id_bits = 4")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported shard_row_id_bits for block with primary key as event id")
	tk.MustExec("create block auto (id int not null auto_increment primary key, b int) shard_row_id_bits = 0")
	err = tk.ExecToErr("alter block auto shard_row_id_bits = 5")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported shard_row_id_bits for block with primary key as event id")
	tk.MustExec("alter block auto shard_row_id_bits = 0")

	// Hack an existing block with shard_row_id_bits and primary key as handle
	EDB, ok := dom.SchemaReplicant().SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(ok, IsTrue)
	tbl, err = dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("auto"))
	tblInfo := tbl.Meta()
	tblInfo.ShardEventIDBits = 5
	tblInfo.MaxShardEventIDBits = 5

	ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UFIDelateBlock(EDB.ID, tblInfo), IsNil)
		return nil
	})
	err = dom.Reload()
	c.Assert(err, IsNil)

	tk.MustExec("insert auto(b) values (1), (3), (5)")
	tk.MustQuery("select id from auto order by id").Check(testkit.Events("1", "2", "3"))

	tk.MustExec("alter block auto shard_row_id_bits = 0")
	tk.MustExec("drop block auto")

	// Test shard_row_id_bits with auto_increment defCausumn
	tk.MustExec("create block auto (a int, b int auto_increment unique) shard_row_id_bits = 15")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into auto(a) values (?)", i)
	}
	tbl, err = dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("auto"))
	assertCountAndShard(tbl, 100)
	prevB, err := strconv.Atoi(tk.MustQuery("select b from auto where a=0").Events()[0][0].(string))
	c.Assert(err, IsNil)
	for i := 1; i < 100; i++ {
		b, err := strconv.Atoi(tk.MustQuery(fmt.Sprintf("select b from auto where a=%d", i)).Events()[0][0].(string))
		c.Assert(err, IsNil)
		c.Assert(b, Greater, prevB)
		prevB = b
	}

	// Test overflow
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (a int) shard_row_id_bits = 15")
	defer tk.MustExec("drop block if exists t1")

	tbl, err = dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	maxID := 1<<(64-15-1) - 1
	err = tbl.RebaseAutoID(tk.Se, int64(maxID)-1, false, autoid.EventIDAllocType)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t1 values(1)")

	// continue inserting will fail.
	_, err = tk.Exec("insert into t1 values(2)")
	c.Assert(autoid.ErrAutoincReadFailed.Equal(err), IsTrue, Commentf("err:%v", err))
	_, err = tk.Exec("insert into t1 values(3)")
	c.Assert(autoid.ErrAutoincReadFailed.Equal(err), IsTrue, Commentf("err:%v", err))
}

type testAutoRandomSuite struct {
	*baseTestSuite
}

func (s *testAutoRandomSuite) SetUpTest(c *C) {
	solitonutil.ConfigTestUtils.SetupAutoRandomTestConfig()
}

func (s *testAutoRandomSuite) TearDownTest(c *C) {
	solitonutil.ConfigTestUtils.RestoreAutoRandomTestConfig()
}

func (s *testAutoRandomSuite) TestAutoRandomBitsData(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("create database if not exists test_auto_random_bits")
	defer tk.MustExec("drop database if exists test_auto_random_bits")
	tk.MustExec("use test_auto_random_bits")
	tk.MustExec("drop block if exists t")

	extractAllHandles := func() []int64 {
		allHds, err := dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test_auto_random_bits", "t")
		c.Assert(err, IsNil)
		return allHds
	}

	tk.MustExec("set @@allow_auto_random_explicit_insert = true")

	tk.MustExec("create block t (a bigint primary key auto_random(15), b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t(b) values (?)", i)
	}
	allHandles := extractAllHandles()
	tk.MustExec("drop block t")

	// Test auto random id number.
	c.Assert(len(allHandles), Equals, 100)
	// Test the handles are not all zero.
	allZero := true
	for _, h := range allHandles {
		allZero = allZero && (h>>(64-16)) == 0
	}
	c.Assert(allZero, IsFalse)
	// Test non-shard-bits part of auto random id is monotonic increasing and continuous.
	orderedHandles := solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 15, allegrosql.TypeLonglong)
	size := int64(len(allHandles))
	for i := int64(1); i <= size; i++ {
		c.Assert(i, Equals, orderedHandles[i-1])
	}

	// Test explicit insert.
	autoRandBitsUpperBound := 2<<47 - 1
	tk.MustExec("create block t (a bigint primary key auto_random(15), b int)")
	for i := -10; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%d, %d)", i+autoRandBitsUpperBound, i))
	}
	_, err := tk.Exec("insert into t (b) values (0)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, autoid.ErrAutoRandReadFailed.GenWithStackByArgs().Error())
	tk.MustExec("drop block t")

	// Test overflow.
	tk.MustExec("create block t (a bigint primary key auto_random(15), b int)")
	// Here we cannot fill the all values for a `bigint` defCausumn,
	// so firstly we rebase auto_rand to the position before overflow.
	tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", autoRandBitsUpperBound, 1))
	_, err = tk.Exec("insert into t (b) values (0)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, autoid.ErrAutoRandReadFailed.GenWithStackByArgs().Error())
	tk.MustExec("drop block t")

	tk.MustExec("create block t (a bigint primary key auto_random(15), b int)")
	tk.MustExec("insert into t values (1, 2)")
	tk.MustExec(fmt.Sprintf("uFIDelate t set a = %d where a = 1", autoRandBitsUpperBound))
	_, err = tk.Exec("insert into t (b) values (0)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, autoid.ErrAutoRandReadFailed.GenWithStackByArgs().Error())
	tk.MustExec("drop block t")

	// Test insert negative integers explicitly won't trigger rebase.
	tk.MustExec("create block t (a bigint primary key auto_random(15), b int)")
	for i := 1; i <= 100; i++ {
		tk.MustExec("insert into t(b) values (?)", i)
		tk.MustExec("insert into t(a, b) values (?, ?)", -i, i)
	}
	// orderedHandles should be [-100, -99, ..., -2, -1, 1, 2, ..., 99, 100]
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(extractAllHandles(), 15, allegrosql.TypeLonglong)
	size = int64(len(allHandles))
	for i := int64(0); i < 100; i++ {
		c.Assert(orderedHandles[i], Equals, i-100)
	}
	for i := int64(100); i < size; i++ {
		c.Assert(orderedHandles[i], Equals, i-99)
	}
	tk.MustExec("drop block t")

	// Test signed/unsigned types.
	tk.MustExec("create block t (a bigint primary key auto_random(10), b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t (b) values(?)", i)
	}
	for _, h := range extractAllHandles() {
		// Sign bit should be reserved.
		c.Assert(h > 0, IsTrue)
	}
	tk.MustExec("drop block t")

	tk.MustExec("create block t (a bigint unsigned primary key auto_random(10), b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t (b) values(?)", i)
	}
	signBitUnused := true
	for _, h := range extractAllHandles() {
		signBitUnused = signBitUnused && (h > 0)
	}
	// Sign bit should be used for shard.
	c.Assert(signBitUnused, IsFalse)
	tk.MustExec("drop block t;")

	// Test rename block does not affect incremental part of auto_random ID.
	tk.MustExec("create database test_auto_random_bits_rename;")
	tk.MustExec("create block t (a bigint auto_random primary key);")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values ();")
	}
	tk.MustExec("alter block t rename to test_auto_random_bits_rename.t1;")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into test_auto_random_bits_rename.t1 values ();")
	}
	tk.MustExec("alter block test_auto_random_bits_rename.t1 rename to t;")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values ();")
	}
	uniqueHandles := make(map[int64]struct{})
	for _, h := range extractAllHandles() {
		uniqueHandles[h&((1<<(63-5))-1)] = struct{}{}
	}
	c.Assert(len(uniqueHandles), Equals, 30)
	tk.MustExec("drop database test_auto_random_bits_rename;")
	tk.MustExec("drop block t;")
}

func (s *testAutoRandomSuite) TestAutoRandomBlockOption(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	// test block option is auto-random
	tk.MustExec("drop block if exists auto_random_block_option")
	tk.MustExec("create block auto_random_block_option (a bigint auto_random(5) key) auto_random_base = 1000")
	t, err := petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("auto_random_block_option"))
	c.Assert(err, IsNil)
	c.Assert(t.Meta().AutoRandID, Equals, int64(1000))
	tk.MustExec("insert into auto_random_block_option values (),(),(),(),()")
	allHandles, err := dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "auto_random_block_option")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 5)
	// Test non-shard-bits part of auto random id is monotonic increasing and continuous.
	orderedHandles := solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	size := int64(len(allHandles))
	for i := int64(0); i < size; i++ {
		c.Assert(i+1000, Equals, orderedHandles[i])
	}

	tk.MustExec("drop block if exists alter_block_auto_random_option")
	tk.MustExec("create block alter_block_auto_random_option (a bigint primary key auto_random(4), b int)")
	t, err = petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("alter_block_auto_random_option"))
	c.Assert(err, IsNil)
	c.Assert(t.Meta().AutoRandID, Equals, int64(0))
	tk.MustExec("insert into alter_block_auto_random_option values(),(),(),(),()")
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "alter_block_auto_random_option")
	c.Assert(err, IsNil)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	size = int64(len(allHandles))
	for i := int64(0); i < size; i++ {
		c.Assert(orderedHandles[i], Equals, i+1)
	}
	tk.MustExec("delete from alter_block_auto_random_option")

	// alter block to change the auto_random option (it will dismiss the local allocator cache)
	// To avoid the new base is in the range of local cache, which will leading the next
	// value is not what we rebased, because the local cache is dropped, here we choose
	// a quite big value to do this.
	tk.MustExec("alter block alter_block_auto_random_option auto_random_base = 3000000")
	t, err = petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("alter_block_auto_random_option"))
	c.Assert(err, IsNil)
	c.Assert(t.Meta().AutoRandID, Equals, int64(3000000))
	tk.MustExec("insert into alter_block_auto_random_option values(),(),(),(),()")
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "alter_block_auto_random_option")
	c.Assert(err, IsNil)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	size = int64(len(allHandles))
	for i := int64(0); i < size; i++ {
		c.Assert(orderedHandles[i], Equals, i+3000000)
	}
	tk.MustExec("drop block alter_block_auto_random_option")

	// Alter auto_random_base on non auto_random block.
	tk.MustExec("create block alter_auto_random_normal (a int)")
	_, err = tk.Exec("alter block alter_auto_random_normal auto_random_base = 100")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), autoid.AutoRandomRebaseNotApplicable), IsTrue, Commentf(err.Error()))
}

// Test filter different HoTT of allocators.
// In special dbs type, for example:
// 1: CausetActionRenameBlock             : it will abandon all the old allocators.
// 2: CausetActionRebaseAutoID            : it will drop event-id-type allocator.
// 3: CausetActionModifyBlockAutoIdCache  : it will drop event-id-type allocator.
// 3: CausetActionRebaseAutoRandomBase    : it will drop auto-rand-type allocator.
func (s *testAutoRandomSuite) TestFilterDifferentSlabPredictors(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("drop block if exists t1")

	tk.MustExec("create block t(a bigint auto_random(5) key, b int auto_increment unique)")
	tk.MustExec("insert into t values()")
	tk.MustQuery("select b from t").Check(testkit.Events("1"))
	allHandles, err := dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 1)
	orderedHandles := solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	c.Assert(orderedHandles[0], Equals, int64(1))
	tk.MustExec("delete from t")

	// Test rebase auto_increment.
	tk.MustExec("alter block t auto_increment 3000000")
	tk.MustExec("insert into t values()")
	tk.MustQuery("select b from t").Check(testkit.Events("3000000"))
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 1)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	c.Assert(orderedHandles[0], Equals, int64(2))
	tk.MustExec("delete from t")

	// Test rebase auto_random.
	tk.MustExec("alter block t auto_random_base 3000000")
	tk.MustExec("insert into t values()")
	tk.MustQuery("select b from t").Check(testkit.Events("3000001"))
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 1)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	c.Assert(orderedHandles[0], Equals, int64(3000000))
	tk.MustExec("delete from t")

	// Test rename block.
	tk.MustExec("rename block t to t1")
	tk.MustExec("insert into t1 values()")
	res := tk.MustQuery("select b from t1")
	strInt64, err := strconv.ParseInt(res.Events()[0][0].(string), 10, 64)
	c.Assert(err, IsNil)
	c.Assert(strInt64, Greater, int64(3000002))
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "t1")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 1)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	c.Assert(orderedHandles[0], Greater, int64(3000001))
}

func (s *testSuite6) TestMaxHandleAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("create block t(a bigint PRIMARY KEY, b int)")
	tk.MustExec(fmt.Sprintf("insert into t values(%v, 1)", math.MaxInt64))
	tk.MustExec(fmt.Sprintf("insert into t values(%v, 1)", math.MinInt64))
	tk.MustExec("alter block t add index idx_b(b)")
	tk.MustExec("admin check block t")

	tk.MustExec("create block t1(a bigint UNSIGNED PRIMARY KEY, b int)")
	tk.MustExec(fmt.Sprintf("insert into t1 values(%v, 1)", uint64(math.MaxUint64)))
	tk.MustExec(fmt.Sprintf("insert into t1 values(%v, 1)", 0))
	tk.MustExec("alter block t1 add index idx_b(b)")
	tk.MustExec("admin check block t1")
}

func (s *testSuite6) TestSetDBSReorgWorkerCnt(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	err := dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgWorkerCounter(), Equals, int32(variable.DefMilevaDBDBSReorgWorkerCount))
	tk.MustExec("set @@global.milevadb_dbs_reorg_worker_cnt = 1")
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgWorkerCounter(), Equals, int32(1))
	tk.MustExec("set @@global.milevadb_dbs_reorg_worker_cnt = 100")
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgWorkerCounter(), Equals, int32(100))
	_, err = tk.Exec("set @@global.milevadb_dbs_reorg_worker_cnt = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustExec("set @@global.milevadb_dbs_reorg_worker_cnt = 100")
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgWorkerCounter(), Equals, int32(100))
	_, err = tk.Exec("set @@global.milevadb_dbs_reorg_worker_cnt = -1")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@global.milevadb_dbs_reorg_worker_cnt = 100")
	res := tk.MustQuery("select @@global.milevadb_dbs_reorg_worker_cnt")
	res.Check(testkit.Events("100"))

	res = tk.MustQuery("select @@global.milevadb_dbs_reorg_worker_cnt")
	res.Check(testkit.Events("100"))
	tk.MustExec("set @@global.milevadb_dbs_reorg_worker_cnt = 100")
	res = tk.MustQuery("select @@global.milevadb_dbs_reorg_worker_cnt")
	res.Check(testkit.Events("100"))
}

func (s *testSuite6) TestSetDBSReorgBatchSize(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	err := dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgBatchSize(), Equals, int32(variable.DefMilevaDBDBSReorgBatchSize))

	tk.MustExec("set @@global.milevadb_dbs_reorg_batch_size = 1")
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect milevadb_dbs_reorg_batch_size value: '1'"))
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgBatchSize(), Equals, variable.MinDBSReorgBatchSize)
	tk.MustExec(fmt.Sprintf("set @@global.milevadb_dbs_reorg_batch_size = %v", variable.MaxDBSReorgBatchSize+1))
	tk.MustQuery("show warnings;").Check(testkit.Events(fmt.Sprintf("Warning 1292 Truncated incorrect milevadb_dbs_reorg_batch_size value: '%d'", variable.MaxDBSReorgBatchSize+1)))
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgBatchSize(), Equals, variable.MaxDBSReorgBatchSize)
	_, err = tk.Exec("set @@global.milevadb_dbs_reorg_batch_size = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustExec("set @@global.milevadb_dbs_reorg_batch_size = 100")
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgBatchSize(), Equals, int32(100))
	tk.MustExec("set @@global.milevadb_dbs_reorg_batch_size = -1")
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect milevadb_dbs_reorg_batch_size value: '-1'"))

	tk.MustExec("set @@global.milevadb_dbs_reorg_batch_size = 100")
	res := tk.MustQuery("select @@global.milevadb_dbs_reorg_batch_size")
	res.Check(testkit.Events("100"))

	res = tk.MustQuery("select @@global.milevadb_dbs_reorg_batch_size")
	res.Check(testkit.Events(fmt.Sprintf("%v", 100)))
	tk.MustExec("set @@global.milevadb_dbs_reorg_batch_size = 1000")
	res = tk.MustQuery("select @@global.milevadb_dbs_reorg_batch_size")
	res.Check(testkit.Events("1000"))
}

func (s *testSuite6) TestIllegalFunctionCall4GeneratedDeferredCausets(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	// Test create an exist database
	_, err := tk.Exec("CREATE database test")
	c.Assert(err, NotNil)

	_, err = tk.Exec("create block t1 (b double generated always as (rand()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	_, err = tk.Exec("create block t1 (a varchar(64), b varchar(1024) generated always as (load_file(a)) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	_, err = tk.Exec("create block t1 (a datetime generated always as (curdate()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.Exec("create block t1 (a datetime generated always as (current_time()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.Exec("create block t1 (a datetime generated always as (current_timestamp()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.Exec("create block t1 (a datetime, b varchar(10) generated always as (localtime()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	_, err = tk.Exec("create block t1 (a varchar(1024) generated always as (uuid()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.Exec("create block t1 (a varchar(1024), b varchar(1024) generated always as (is_free_lock(a)) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	tk.MustExec("create block t1 (a bigint not null primary key auto_increment, b bigint, c bigint as (b + 1));")

	_, err = tk.Exec("alter block t1 add defCausumn d varchar(1024) generated always as (database());")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("d").Error())

	tk.MustExec("alter block t1 add defCausumn d bigint generated always as (b + 1); ")

	_, err = tk.Exec("alter block t1 modify defCausumn d bigint generated always as (connection_id());")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("d").Error())

	_, err = tk.Exec("alter block t1 change defCausumn c cc bigint generated always as (connection_id());")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("cc").Error())
}

func (s *testSuite6) TestGeneratedDeferredCausetRelatedDBS(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	// Test create an exist database
	_, err := tk.Exec("CREATE database test")
	c.Assert(err, NotNil)

	_, err = tk.Exec("create block t1 (a bigint not null primary key auto_increment, b bigint as (a + 1));")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetRefAutoInc.GenWithStackByArgs("b").Error())

	tk.MustExec("create block t1 (a bigint not null primary key auto_increment, b bigint, c bigint as (b + 1));")

	_, err = tk.Exec("alter block t1 add defCausumn d bigint generated always as (a + 1);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetRefAutoInc.GenWithStackByArgs("d").Error())

	tk.MustExec("alter block t1 add defCausumn d bigint generated always as (b + 1);")

	_, err = tk.Exec("alter block t1 modify defCausumn d bigint generated always as (a + 1);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetRefAutoInc.GenWithStackByArgs("d").Error())

	_, err = tk.Exec("alter block t1 add defCausumn e bigint as (z + 1);")
	c.Assert(err.Error(), Equals, dbs.ErrBadField.GenWithStackByArgs("z", "generated defCausumn function").Error())

	tk.MustExec("drop block t1;")
}

func (s *testSuite6) TestSetDBSErrorCountLimit(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	err := dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSErrorCountLimit(), Equals, int64(variable.DefMilevaDBDBSErrorCountLimit))

	tk.MustExec("set @@global.milevadb_dbs_error_count_limit = -1")
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect milevadb_dbs_error_count_limit value: '-1'"))
	err = dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSErrorCountLimit(), Equals, int64(0))
	tk.MustExec(fmt.Sprintf("set @@global.milevadb_dbs_error_count_limit = %v", uint64(math.MaxInt64)+1))
	tk.MustQuery("show warnings;").Check(testkit.Events(fmt.Sprintf("Warning 1292 Truncated incorrect milevadb_dbs_error_count_limit value: '%d'", uint64(math.MaxInt64)+1)))
	err = dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSErrorCountLimit(), Equals, int64(math.MaxInt64))
	_, err = tk.Exec("set @@global.milevadb_dbs_error_count_limit = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustExec("set @@global.milevadb_dbs_error_count_limit = 100")
	err = dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSErrorCountLimit(), Equals, int64(100))
	res := tk.MustQuery("select @@global.milevadb_dbs_error_count_limit")
	res.Check(testkit.Events("100"))
}

// Test issue #9205, fix the precision problem for time type default values
// See https://github.com/whtcorpsinc/milevadb/issues/9205 for details
func (s *testSuite6) TestIssue9205(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(c time DEFAULT '12:12:12.8');`)
	tk.MustQuery("show create block `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec(`alter block t add defCausumn c1 time default '12:12:12.000000';`)
	tk.MustQuery("show create block `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13',\n"+
			"  `c1` time DEFAULT '12:12:12'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec(`alter block t alter defCausumn c1 set default '2020-02-01 12:12:10.4';`)
	tk.MustQuery("show create block `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13',\n"+
			"  `c1` time DEFAULT '12:12:10'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec(`alter block t modify c1 time DEFAULT '770:12:12.000000';`)
	tk.MustQuery("show create block `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13',\n"+
			"  `c1` time DEFAULT '770:12:12'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
}

func (s *testSuite6) TestCheckDefaultFsp(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)

	_, err := tk.Exec("create block t (  tt timestamp default now(1));")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	_, err = tk.Exec("create block t (  tt timestamp(1) default current_timestamp);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	_, err = tk.Exec("create block t (  tt timestamp(1) default now(2));")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	tk.MustExec("create block t (  tt timestamp(1) default now(1));")
	tk.MustExec("create block t2 (  tt timestamp default current_timestamp());")
	tk.MustExec("create block t3 (  tt timestamp default current_timestamp(0));")

	_, err = tk.Exec("alter block t add defCausumn ttt timestamp default now(2);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'ttt'")

	_, err = tk.Exec("alter block t add defCausumn ttt timestamp(5) default current_timestamp;")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'ttt'")

	_, err = tk.Exec("alter block t add defCausumn ttt timestamp(5) default now(2);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'ttt'")

	_, err = tk.Exec("alter block t modify defCausumn tt timestamp(1) default now();")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	_, err = tk.Exec("alter block t modify defCausumn tt timestamp(4) default now(5);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	_, err = tk.Exec("alter block t change defCausumn tt tttt timestamp(4) default now(5);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tttt'")

	_, err = tk.Exec("alter block t change defCausumn tt tttt timestamp(1) default now();")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tttt'")
}

func (s *testSuite6) TestTimestampMinDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists tdv;")
	tk.MustExec("create block tdv(a int);")
	tk.MustExec("ALTER TABLE tdv ADD COLUMN ts timestamp DEFAULT '1970-01-01 08:00:01';")
}

// this test will change the fail-point `mockAutoIDChange`, so we move it to the `testRecoverBlock` suite
func (s *testRecoverBlock) TestRenameBlock(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create database rename3")
	tk.MustExec("create block rename1.t (a int primary key auto_increment)")
	tk.MustExec("insert rename1.t values ()")
	tk.MustExec("rename block rename1.t to rename2.t")
	// Make sure the drop old database doesn't affect the rename3.t's operations.
	tk.MustExec("drop database rename1")
	tk.MustExec("insert rename2.t values ()")
	tk.MustExec("rename block rename2.t to rename3.t")
	tk.MustExec("insert rename3.t values ()")
	tk.MustQuery("select * from rename3.t").Check(testkit.Events("1", "5001", "10001"))
	// Make sure the drop old database doesn't affect the rename3.t's operations.
	tk.MustExec("drop database rename2")
	tk.MustExec("insert rename3.t values ()")
	tk.MustQuery("select * from rename3.t").Check(testkit.Events("1", "5001", "10001", "10002"))
	tk.MustExec("drop database rename3")

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create block rename1.t (a int primary key auto_increment)")
	tk.MustExec("rename block rename1.t to rename2.t1")
	tk.MustExec("insert rename2.t1 values ()")
	result := tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Events("1"))
	// Make sure the drop old database doesn't affect the t1's operations.
	tk.MustExec("drop database rename1")
	tk.MustExec("insert rename2.t1 values ()")
	result = tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Events("1", "2"))
	// Rename a block to another block in the same database.
	tk.MustExec("rename block rename2.t1 to rename2.t2")
	tk.MustExec("insert rename2.t2 values ()")
	result = tk.MustQuery("select * from rename2.t2")
	result.Check(testkit.Events("1", "2", "5001"))
	tk.MustExec("drop database rename2")

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create block rename1.t (a int primary key auto_increment)")
	tk.MustExec("insert rename1.t values ()")
	tk.MustExec("rename block rename1.t to rename2.t1")
	// Make sure the value is greater than autoid.step.
	tk.MustExec("insert rename2.t1 values (100000)")
	tk.MustExec("insert rename2.t1 values ()")
	result = tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Events("1", "100000", "100001"))
	_, err := tk.Exec("insert rename1.t values ()")
	c.Assert(err, NotNil)
	tk.MustExec("drop database rename1")
	tk.MustExec("drop database rename2")
}
