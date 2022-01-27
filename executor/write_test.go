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
	"errors"
	"fmt"
	"sync"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

type testBypassSuite struct{}

func (s *testBypassSuite) SetUpSuite(c *C) {
}

func (s *testSuite) TestInsert(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	testALLEGROSQL := `drop block if exists insert_test;create block insert_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `insert insert_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("Records: 3  Duplicates: 0  Warnings: 0")

	errInsertSelectALLEGROSQL := `insert insert_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errInsertSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectALLEGROSQL = `insert insert_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectALLEGROSQL = `insert insert_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectALLEGROSQL = `insert insert_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	insertSetALLEGROSQL := `insert insert_test set c1 = 3;`
	tk.MustExec(insertSetALLEGROSQL)
	tk.CheckLastMessage("")

	errInsertSelectALLEGROSQL = `insert insert_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectALLEGROSQL = `insert insert_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	insertSelectALLEGROSQL := `create block insert_test_1 (id int, c1 int);`
	tk.MustExec(insertSelectALLEGROSQL)
	insertSelectALLEGROSQL = `insert insert_test_1 select id, c1 from insert_test;`
	tk.MustExec(insertSelectALLEGROSQL)
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")

	insertSelectALLEGROSQL = `create block insert_test_2 (id int, c1 int);`
	tk.MustExec(insertSelectALLEGROSQL)
	insertSelectALLEGROSQL = `insert insert_test_1 select id, c1 from insert_test union select id * 10, c1 * 10 from insert_test;`
	tk.MustExec(insertSelectALLEGROSQL)
	tk.CheckLastMessage("Records: 8  Duplicates: 0  Warnings: 0")

	errInsertSelectALLEGROSQL = `insert insert_test_1 select c1 from insert_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectALLEGROSQL = `insert insert_test_1 values(default, default, default, default, default)`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	// UFIDelating defCausumn is PK handle.
	// Make sure the record is "1, 1, nil, 1".
	r := tk.MustQuery("select * from insert_test where id = 1;")
	rowStr := fmt.Sprintf("%v %v %v %v", "1", "1", nil, "1")
	r.Check(testkit.Rows(rowStr))
	insertALLEGROSQL := `insert into insert_test (id, c3) values (1, 2) on duplicate key uFIDelate id=values(id), c2=10;`
	tk.MustExec(insertALLEGROSQL)
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "1")
	r.Check(testkit.Rows(rowStr))

	insertALLEGROSQL = `insert into insert_test (id, c2) values (1, 1) on duplicate key uFIDelate insert_test.c2=10;`
	tk.MustExec(insertALLEGROSQL)
	tk.CheckLastMessage("")

	_, err = tk.Exec(`insert into insert_test (id, c2) values(1, 1) on duplicate key uFIDelate t.c2 = 10`)
	c.Assert(err, NotNil)

	// for on duplicate key
	insertALLEGROSQL = `INSERT INTO insert_test (id, c3) VALUES (1, 2) ON DUPLICATE KEY UFIDelATE c3=values(c3)+c3+3;`
	tk.MustExec(insertALLEGROSQL)
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "6")
	r.Check(testkit.Rows(rowStr))

	// for on duplicate key with ignore
	insertALLEGROSQL = `INSERT IGNORE INTO insert_test (id, c3) VALUES (1, 2) ON DUPLICATE KEY UFIDelATE c3=values(c3)+c3+3;`
	tk.MustExec(insertALLEGROSQL)
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "11")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("create block insert_err (id int, c1 varchar(8))")
	_, err = tk.Exec("insert insert_err values (1, 'abcdabcdabcd')")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	_, err = tk.Exec("insert insert_err values (1, '你好，世界')")
	c.Assert(err, IsNil)

	tk.MustExec("create block TEST1 (ID INT NOT NULL, VALUE INT DEFAULT NULL, PRIMARY KEY (ID))")
	_, err = tk.Exec("INSERT INTO TEST1(id,value) VALUE(3,3) on DUPLICATE KEY UFIDelATE VALUE=4")
	c.Assert(err, IsNil)
	tk.CheckLastMessage("")

	tk.MustExec("create block t (id int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("uFIDelate t t1 set id = (select count(*) + 1 from t t2 where t1.id = t2.id)")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("2"))

	// issue 3235
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(c decimal(5, 5))")
	_, err = tk.Exec("insert into t value(0)")
	c.Assert(err, IsNil)
	_, err = tk.Exec("insert into t value(1)")
	c.Assert(types.ErrWarnDataOutOfRange.Equal(err), IsTrue)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(c binary(255))")
	_, err = tk.Exec("insert into t value(1)")
	c.Assert(err, IsNil)
	r = tk.MustQuery("select length(c) from t;")
	r.Check(testkit.Rows("255"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(c varbinary(255))")
	_, err = tk.Exec("insert into t value(1)")
	c.Assert(err, IsNil)
	r = tk.MustQuery("select length(c) from t;")
	r.Check(testkit.Rows("1"))

	// issue 3509
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(c int)")
	tk.MustExec("set @origin_time_zone = @@time_zone")
	tk.MustExec("set @@time_zone = '+08:00'")
	_, err = tk.Exec("insert into t value(Unix_timestamp('2002-10-27 01:00'))")
	c.Assert(err, IsNil)
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("1035651600"))
	tk.MustExec("set @@time_zone = @origin_time_zone")

	// issue 3832
	tk.MustExec("create block t1 (b char(0));")
	_, err = tk.Exec(`insert into t1 values ("");`)
	c.Assert(err, IsNil)

	// issue 3895
	tk = testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4,2));")
	tk.MustExec("INSERT INTO t VALUES (1.000001);")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect DECIMAL value: '1.000001'"))
	tk.MustExec("INSERT INTO t VALUES (1.000000);")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows())

	// issue 4653
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a datetime);")
	_, err = tk.Exec("INSERT INTO t VALUES('2020-00-00')")
	c.Assert(err, NotNil)
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("INSERT INTO t VALUES('2020-00-00')")
	r = tk.MustQuery("SELECT * FROM t;")
	r.Check(testkit.Rows("2020-00-00 00:00:00"))
	tk.MustExec("set sql_mode = 'strict_all_blocks';")
	r = tk.MustQuery("SELECT * FROM t;")
	r.Check(testkit.Rows("2020-00-00 00:00:00"))

	// test auto_increment with unsigned.
	tk.MustExec("drop block if exists test")
	tk.MustExec("CREATE TABLE test(id int(10) UNSIGNED NOT NULL AUTO_INCREMENT, p int(10) UNSIGNED NOT NULL, PRIMARY KEY(p), KEY(id))")
	tk.MustExec("insert into test(p) value(1)")
	tk.MustQuery("select * from test").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from test use index (id) where id = 1").Check(testkit.Rows("1 1"))
	tk.MustExec("insert into test values(NULL, 2)")
	tk.MustQuery("select * from test use index (id) where id = 2").Check(testkit.Rows("2 2"))
	tk.MustExec("insert into test values(2, 3)")
	tk.MustQuery("select * from test use index (id) where id = 2").Check(testkit.Rows("2 2", "2 3"))

	// issue 6360
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a bigint unsigned);")
	tk.MustExec(" set @orig_sql_mode = @@sql_mode; set @@sql_mode = 'strict_all_blocks';")
	_, err = tk.Exec("insert into t value (-1);")
	c.Assert(types.ErrWarnDataOutOfRange.Equal(err), IsTrue)
	tk.MustExec("set @@sql_mode = '';")
	tk.MustExec("insert into t value (-1);")
	// TODO: the following warning messages are not consistent with MyALLEGROSQL, fix them in the future PRs
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 constant -1 overflows bigint"))
	tk.MustExec("insert into t select -1;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 constant -1 overflows bigint"))
	tk.MustExec("insert into t select cast(-1 as unsigned);")
	tk.MustExec("insert into t value (-1.111);")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 constant -1.111 overflows bigint"))
	tk.MustExec("insert into t value ('-1.111');")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 BIGINT UNSIGNED value is out of range in '-1'"))
	tk.MustExec("uFIDelate t set a = -1 limit 1;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 constant -1 overflows bigint"))
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("0", "0", "18446744073709551615", "0", "0"))
	tk.MustExec("set @@sql_mode = @orig_sql_mode;")

	// issue 6424
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a time(6))")
	tk.MustExec("insert into t value('20070219173709.055870'), ('20070219173709.055'), ('20070219173709.055870123')")
	tk.MustQuery("select * from t").Check(testkit.Rows("17:37:09.055870", "17:37:09.055000", "17:37:09.055870"))
	tk.MustExec("truncate block t")
	tk.MustExec("insert into t value(20070219173709.055870), (20070219173709.055), (20070219173709.055870123)")
	tk.MustQuery("select * from t").Check(testkit.Rows("17:37:09.055870", "17:37:09.055000", "17:37:09.055870"))
	_, err = tk.Exec("insert into t value(-20070219173709.055870)")
	c.Assert(err.Error(), Equals, "[block:1366]Incorrect time value: '-20070219173709.055870' for defCausumn 'a' at event 1")

	tk.MustExec("drop block if exists t")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("create block t(a float unsigned, b double unsigned)")
	tk.MustExec("insert into t value(-1.1, -1.1), (-2.1, -2.1), (0, 0), (1.1, 1.1)")
	tk.MustQuery("show warnings").
		Check(testkit.Rows("Warning 1690 constant -1.1 overflows float", "Warning 1690 constant -1.1 overflows double",
			"Warning 1690 constant -2.1 overflows float", "Warning 1690 constant -2.1 overflows double"))
	tk.MustQuery("select * from t").Check(testkit.Rows("0 0", "0 0", "0 0", "1.1 1.1"))

	// issue 7061
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int default 1, b int default 2)")
	tk.MustExec("insert into t values(default, default)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("truncate block t")
	tk.MustExec("insert into t values(default(b), default(a))")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1"))
	tk.MustExec("truncate block t")
	tk.MustExec("insert into t (b) values(default)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("truncate block t")
	tk.MustExec("insert into t (b) values(default(a))")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))

	tk.MustExec("create view v as select * from t")
	_, err = tk.Exec("insert into v values(1,2)")
	c.Assert(err.Error(), Equals, "insert into view v is not supported now.")
	_, err = tk.Exec("replace into v values(1,2)")
	c.Assert(err.Error(), Equals, "replace into view v is not supported now.")
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	_, err = tk.Exec("insert into seq values()")
	c.Assert(err.Error(), Equals, "insert into sequence seq is not supported now.")
	_, err = tk.Exec("replace into seq values()")
	c.Assert(err.Error(), Equals, "replace into sequence seq is not supported now.")
	tk.MustExec("drop sequence seq")
}

func (s *testSuiteP2) TestMultiBatch(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t,t0")
	tk.MustExec("create block t0 (i int)")
	tk.MustExec("insert into t0 values (1), (1)")
	tk.MustExec("create block t (i int unique key)")
	tk.MustExec("set @@milevadb_dml_batch_size = 1")
	tk.MustExec("insert ignore into t select * from t0")
	tk.MustExec("admin check block t")
}

func (s *testSuite4) TestInsertAutoInc(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	createALLEGROSQL := `drop block if exists insert_autoinc_test; create block insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createALLEGROSQL)

	insertALLEGROSQL := `insert into insert_autoinc_test(c1) values (1), (2)`
	tk.MustExec(insertALLEGROSQL)
	tk.MustExec("begin")
	r := tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 := fmt.Sprintf("%v %v", "1", "1")
	rowStr2 := fmt.Sprintf("%v %v", "2", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (5,5)`
	tk.MustExec(insertALLEGROSQL)
	insertALLEGROSQL = `insert into insert_autoinc_test(c1) values (6)`
	tk.MustExec(insertALLEGROSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 := fmt.Sprintf("%v %v", "5", "5")
	rowStr4 := fmt.Sprintf("%v %v", "6", "6")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3, rowStr4))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (3,3)`
	tk.MustExec(insertALLEGROSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 := fmt.Sprintf("%v %v", "3", "3")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr5, rowStr3, rowStr4))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertALLEGROSQL = `insert into insert_autoinc_test(c1) values (7)`
	tk.MustExec(insertALLEGROSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 := fmt.Sprintf("%v %v", "7", "7")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr5, rowStr3, rowStr4, rowStr6))
	tk.MustExec("commit")

	// issue-962
	createALLEGROSQL = `drop block if exists insert_autoinc_test; create block insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createALLEGROSQL)
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (0.3, 1)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 = fmt.Sprintf("%v %v", "1", "1")
	r.Check(testkit.Rows(rowStr1))
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (-0.3, 2)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr2 = fmt.Sprintf("%v %v", "2", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (-3.3, 3)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 = fmt.Sprintf("%v %v", "-3", "3")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2))
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (4.3, 4)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr4 = fmt.Sprintf("%v %v", "4", "4")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4))
	insertALLEGROSQL = `insert into insert_autoinc_test(c1) values (5)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 = fmt.Sprintf("%v %v", "5", "5")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4, rowStr5))
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (null, 6)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 = fmt.Sprintf("%v %v", "6", "6")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4, rowStr5, rowStr6))

	// ALLEGROSQL_MODE=NO_AUTO_VALUE_ON_ZERO
	createALLEGROSQL = `drop block if exists insert_autoinc_test; create block insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createALLEGROSQL)
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (5, 1)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 = fmt.Sprintf("%v %v", "5", "1")
	r.Check(testkit.Rows(rowStr1))
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (0, 2)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr2 = fmt.Sprintf("%v %v", "6", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (0, 3)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 = fmt.Sprintf("%v %v", "7", "3")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3))
	tk.MustExec("set ALLEGROSQL_MODE=NO_AUTO_VALUE_ON_ZERO")
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (0, 4)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr4 = fmt.Sprintf("%v %v", "0", "4")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3))
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (0, 5)`
	_, err := tk.Exec(insertALLEGROSQL)
	// ERROR 1062 (23000): Duplicate entry '0' for key 'PRIMARY'
	c.Assert(err, NotNil)
	insertALLEGROSQL = `insert into insert_autoinc_test(c1) values (6)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 = fmt.Sprintf("%v %v", "8", "6")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5))
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (null, 7)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 = fmt.Sprintf("%v %v", "9", "7")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5, rowStr6))
	tk.MustExec("set ALLEGROSQL_MODE='';")
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (0, 8)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr7 := fmt.Sprintf("%v %v", "10", "8")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5, rowStr6, rowStr7))
	insertALLEGROSQL = `insert into insert_autoinc_test(id, c1) values (null, 9)`
	tk.MustExec(insertALLEGROSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr8 := fmt.Sprintf("%v %v", "11", "9")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5, rowStr6, rowStr7, rowStr8))
}

func (s *testSuite4) TestInsertIgnore(c *C) {
	var cfg ekv.InjectionConfig
	tk := testkit.NewTestKit(c, ekv.NewInjectedStore(s.causetstore, &cfg))
	tk.MustExec("use test")
	testALLEGROSQL := `drop block if exists t;
    create block t (id int PRIMARY KEY AUTO_INCREMENT, c1 int unique key);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `insert into t values (1, 2);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")

	r := tk.MustQuery("select * from t;")
	rowStr := fmt.Sprintf("%v %v", "1", "2")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("insert ignore into t values (1, 3), (2, 3)")
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 1")
	r = tk.MustQuery("select * from t;")
	rowStr1 := fmt.Sprintf("%v %v", "2", "3")
	r.Check(testkit.Rows(rowStr, rowStr1))

	tk.MustExec("insert ignore into t values (3, 4), (3, 4)")
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 1")
	r = tk.MustQuery("select * from t;")
	rowStr2 := fmt.Sprintf("%v %v", "3", "4")
	r.Check(testkit.Rows(rowStr, rowStr1, rowStr2))

	tk.MustExec("begin")
	tk.MustExec("insert ignore into t values (4, 4), (4, 5), (4, 6)")
	tk.CheckLastMessage("Records: 3  Duplicates: 2  Warnings: 2")
	r = tk.MustQuery("select * from t;")
	rowStr3 := fmt.Sprintf("%v %v", "4", "5")
	r.Check(testkit.Rows(rowStr, rowStr1, rowStr2, rowStr3))
	tk.MustExec("commit")

	cfg.SetGetError(errors.New("foo"))
	_, err := tk.Exec("insert ignore into t values (1, 3)")
	c.Assert(err, NotNil)
	cfg.SetGetError(nil)

	// for issue 4268
	testALLEGROSQL = `drop block if exists t;
	create block t (a bigint);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = "insert ignore into t select '1a';"
	_, err = tk.Exec(testALLEGROSQL)
	c.Assert(err, IsNil)
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '1a'"))
	testALLEGROSQL = "insert ignore into t values ('1a')"
	_, err = tk.Exec(testALLEGROSQL)
	c.Assert(err, IsNil)
	tk.CheckLastMessage("")
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '1a'"))

	// for duplicates with warning
	testALLEGROSQL = `drop block if exists t;
	create block t(a int primary key, b int);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = "insert ignore into t values (1,1);"
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	_, err = tk.Exec(testALLEGROSQL)
	tk.CheckLastMessage("")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))

	testALLEGROSQL = `drop block if exists test;
create block test (i int primary key, j int unique);
begin;
insert into test values (1,1);
insert ignore into test values (2,1);
commit;`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `select * from test;`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1 1"))

	testALLEGROSQL = `delete from test;
insert into test values (1, 1);
begin;
delete from test where i = 1;
insert ignore into test values (2, 1);
commit;`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `select * from test;`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("2 1"))

	testALLEGROSQL = `delete from test;
insert into test values (1, 1);
begin;
uFIDelate test set i = 2, j = 2 where i = 1;
insert ignore into test values (1, 3);
insert ignore into test values (2, 4);
commit;`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `select * from test order by i;`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1 3", "2 2"))

	testALLEGROSQL = `create block badnull (i int not null)`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `insert ignore into badnull values (null)`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 DeferredCauset 'i' cannot be null"))
	testALLEGROSQL = `select * from badnull`
	tk.MustQuery(testALLEGROSQL).Check(testkit.Rows("0"))
}

func (s *testSuite8) TestInsertOnDup(c *C) {
	var cfg ekv.InjectionConfig
	tk := testkit.NewTestKit(c, ekv.NewInjectedStore(s.causetstore, &cfg))
	tk.MustExec("use test")
	testALLEGROSQL := `drop block if exists t;
    create block t (i int unique key);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `insert into t values (1),(2);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")

	r := tk.MustQuery("select * from t;")
	rowStr1 := fmt.Sprintf("%v", "1")
	rowStr2 := fmt.Sprintf("%v", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))

	tk.MustExec("insert into t values (1), (2) on duplicate key uFIDelate i = values(i)")
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows(rowStr1, rowStr2))

	tk.MustExec("insert into t values (2), (3) on duplicate key uFIDelate i = 3")
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 0")
	r = tk.MustQuery("select * from t;")
	rowStr3 := fmt.Sprintf("%v", "3")
	r.Check(testkit.Rows(rowStr1, rowStr3))

	testALLEGROSQL = `drop block if exists t;
    create block t (i int primary key, j int unique key);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `insert into t values (-1, 1);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")

	r = tk.MustQuery("select * from t;")
	rowStr1 = fmt.Sprintf("%v %v", "-1", "1")
	r.Check(testkit.Rows(rowStr1))

	tk.MustExec("insert into t values (1, 1) on duplicate key uFIDelate j = values(j)")
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows(rowStr1))

	testALLEGROSQL = `drop block if exists test;
create block test (i int primary key, j int unique);
begin;
insert into test values (1,1);
insert into test values (2,1) on duplicate key uFIDelate i = -i, j = -j;
commit;`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `select * from test;`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("-1 -1"))

	testALLEGROSQL = `delete from test;
insert into test values (1, 1);
begin;
delete from test where i = 1;
insert into test values (2, 1) on duplicate key uFIDelate i = -i, j = -j;
commit;`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `select * from test;`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("2 1"))

	testALLEGROSQL = `delete from test;
insert into test values (1, 1);
begin;
uFIDelate test set i = 2, j = 2 where i = 1;
insert into test values (1, 3) on duplicate key uFIDelate i = -i, j = -j;
insert into test values (2, 4) on duplicate key uFIDelate i = -i, j = -j;
commit;`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `select * from test order by i;`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("-2 -2", "1 3"))

	testALLEGROSQL = `delete from test;
begin;
insert into test values (1, 3), (1, 3) on duplicate key uFIDelate i = values(i), j = values(j);
commit;`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `select * from test order by i;`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1 3"))

	testALLEGROSQL = `create block tmp (id int auto_increment, code int, primary key(id, code));
	create block m (id int primary key auto_increment, code int unique);
	insert tmp (code) values (1);
	insert tmp (code) values (1);
	set milevadb_init_chunk_size=1;
	insert m (code) select code from tmp on duplicate key uFIDelate code = values(code);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `select * from m;`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1 1"))

	// The following two cases are used for guaranteeing the last_insert_id
	// to be set as the value of on-duplicate-uFIDelate assigned.
	testALLEGROSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT AUTO_INCREMENT PRIMARY KEY,
	f2 VARCHAR(5) NOT NULL UNIQUE);
	INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UFIDelATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	testALLEGROSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1"))
	testALLEGROSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UFIDelATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	testALLEGROSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1"))

	testALLEGROSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT AUTO_INCREMENT UNIQUE,
	f2 VARCHAR(5) NOT NULL UNIQUE);
	INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UFIDelATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	testALLEGROSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1"))
	testALLEGROSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UFIDelATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	testALLEGROSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1"))
	testALLEGROSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UFIDelATE f1 = 2;`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	testALLEGROSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1"))

	testALLEGROSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT);
	INSERT t1 VALUES (1) ON DUPLICATE KEY UFIDelATE f1 = 1;`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	tk.MustQuery(`SELECT * FROM t1;`).Check(testkit.Rows("1"))

	testALLEGROSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT PRIMARY KEY, f2 INT NOT NULL UNIQUE);
	INSERT t1 VALUES (1, 1);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	tk.MustExec(`INSERT t1 VALUES (1, 1), (1, 1) ON DUPLICATE KEY UFIDelATE f1 = 2, f2 = 2;`)
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 1", "2 2"))
	_, err := tk.Exec(`INSERT t1 VALUES (1, 1) ON DUPLICATE KEY UFIDelATE f2 = null;`)
	c.Assert(err, NotNil)
	tk.MustExec(`INSERT IGNORE t1 VALUES (1, 1) ON DUPLICATE KEY UFIDelATE f2 = null;`)
	tk.CheckLastMessage("")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 DeferredCauset 'f2' cannot be null"))
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 0", "2 2"))

	tk.MustExec(`SET sql_mode='';`)
	_, err = tk.Exec(`INSERT t1 VALUES (1, 1) ON DUPLICATE KEY UFIDelATE f2 = null;`)
	c.Assert(err, NotNil)
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 0", "2 2"))
}

func (s *testSuite4) TestInsertIgnoreOnDup(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	testALLEGROSQL := `drop block if exists t;
    create block t (i int not null primary key, j int unique key);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `insert into t values (1, 1), (2, 2);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	testALLEGROSQL = `insert ignore into t values(1, 1) on duplicate key uFIDelate i = 2;`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	testALLEGROSQL = `select * from t;`
	r := tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1 1", "2 2"))
	testALLEGROSQL = `insert ignore into t values(1, 1) on duplicate key uFIDelate j = 2;`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("")
	testALLEGROSQL = `select * from t;`
	r = tk.MustQuery(testALLEGROSQL)
	r.Check(testkit.Rows("1 1", "2 2"))
}

func (s *testSuite4) TestInsertSetWithDefault(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	// Assign `DEFAULT` in `INSERT ... SET ...` statement
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (a int default 10, b int default 20);")
	tk.MustExec("insert into t1 set a=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set b=default, a=1;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set a=default(a);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set a=default(b), b=default(a)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("20 10"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set a=default(b)+default(a);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("30 20"))
	// With generated defCausumns
	tk.MustExec("create block t2 (a int default 10, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("insert into t2 set a=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("10 -10 -10"))
	tk.MustExec("delete from t2;")
	tk.MustExec("insert into t2 set a=2, b=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("2 -2 -2"))
	tk.MustExec("delete from t2;")
	tk.MustExec("insert into t2 set c=default, a=3;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("3 -3 -3"))
	tk.MustExec("delete from t2;")
	tk.MustExec("insert into t2 set a=default, b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("10 -10 -10"))
	tk.MustExec("delete from t2;")
	tk.MustExec("insert into t2 set a=default(a), b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("10 -10 -10"))
	tk.MustExec("delete from t2;")
	tk.MustGetErrCode("insert into t2 set b=default(a);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("insert into t2 set a=default(b), b=default(b);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("insert into t2 set a=default(a), c=default(c);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("insert into t2 set a=default(a), c=default(a);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustExec("drop block t1, t2")
}

func (s *testSuite4) TestInsertOnDupUFIDelateDefault(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	// Assign `DEFAULT` in `INSERT ... ON DUPLICATE KEY UFIDelATE ...` statement
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (a int unique, b int default 20, c int default 30);")
	tk.MustExec("insert into t1 values (1,default,default);")
	tk.MustExec("insert into t1 values (1,default,default) on duplicate key uFIDelate b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30"))
	tk.MustExec("insert into t1 values (1,default,default) on duplicate key uFIDelate c=default, b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30"))
	tk.MustExec("insert into t1 values (1,default,default) on duplicate key uFIDelate c=default, a=2")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("2 20 30"))
	tk.MustExec("insert into t1 values (2,default,default) on duplicate key uFIDelate c=default(b)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("2 20 20"))
	tk.MustExec("insert into t1 values (2,default,default) on duplicate key uFIDelate a=default(b)+default(c)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("50 20 20"))
	// With generated defCausumns
	tk.MustExec("create block t2 (a int unique, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("insert into t2 values (1,default,default);")
	tk.MustExec("insert into t2 values (1,default,default) on duplicate key uFIDelate a=2, b=default;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 -2 -2"))
	tk.MustExec("insert into t2 values (2,default,default) on duplicate key uFIDelate a=3, c=default;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("3 -3 -3"))
	tk.MustExec("insert into t2 values (3,default,default) on duplicate key uFIDelate c=default, b=default, a=4;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("4 -4 -4"))
	tk.MustExec("insert into t2 values (10,default,default) on duplicate key uFIDelate b=default, a=20, c=default;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("4 -4 -4", "10 -10 -10"))
	tk.MustGetErrCode("insert into t2 values (4,default,default) on duplicate key uFIDelate b=default(a);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("insert into t2 values (4,default,default) on duplicate key uFIDelate a=default(b), b=default(b);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("insert into t2 values (4,default,default) on duplicate key uFIDelate a=default(a), c=default(c);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("insert into t2 values (4,default,default) on duplicate key uFIDelate a=default(a), c=default(a);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustExec("drop block t1, t2")

	tk.MustExec("set @@milevadb_txn_mode = 'pessimistic'")
	tk.MustExec("create block t ( c_int int, c_string varchar(40) defCauslate utf8mb4_bin , primary key (c_string), unique key (c_int));")
	tk.MustExec("insert into t values (22, 'gold witch'), (24, 'gray singer'), (21, 'silver sight');")
	tk.MustExec("begin;")
	err := tk.ExecToErr("insert into t values (21,'black warlock'), (22, 'dark sloth'), (21,  'cyan song') on duplicate key uFIDelate c_int = c_int + 1, c_string = concat(c_int, ':', c_string);")
	c.Assert(ekv.ErrKeyExists.Equal(err), IsTrue)
	tk.MustExec("commit;")
	tk.MustQuery("select * from t order by c_int;").Check(solitonutil.RowsWithSep("|", "21|silver sight", "22|gold witch", "24|gray singer"))
	tk.MustExec("drop block t;")
}

func (s *testSuite4) TestReplace(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	testALLEGROSQL := `drop block if exists replace_test;
    create block replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("Records: 3  Duplicates: 0  Warnings: 0")

	errReplaceALLEGROSQL := `replace replace_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceALLEGROSQL = `replace replace_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceALLEGROSQL = `replace replace_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceALLEGROSQL = `replace replace_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceSetALLEGROSQL := `replace replace_test set c1 = 3;`
	tk.MustExec(replaceSetALLEGROSQL)
	tk.CheckLastMessage("")

	errReplaceSetALLEGROSQL := `replace replace_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSetALLEGROSQL = `replace replace_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceSelectALLEGROSQL := `create block replace_test_1 (id int, c1 int);`
	tk.MustExec(replaceSelectALLEGROSQL)
	replaceSelectALLEGROSQL = `replace replace_test_1 select id, c1 from replace_test;`
	tk.MustExec(replaceSelectALLEGROSQL)
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")

	replaceSelectALLEGROSQL = `create block replace_test_2 (id int, c1 int);`
	tk.MustExec(replaceSelectALLEGROSQL)
	replaceSelectALLEGROSQL = `replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`
	tk.MustExec(replaceSelectALLEGROSQL)
	tk.CheckLastMessage("Records: 8  Duplicates: 0  Warnings: 0")

	errReplaceSelectALLEGROSQL := `replace replace_test_1 select c1 from replace_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceUniqueIndexALLEGROSQL := `create block replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2));`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")

	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c1=1, c2=1;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))
	tk.CheckLastMessage("")

	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")

	replaceUniqueIndexALLEGROSQL = `create block replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2));`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")

	replacePrimaryKeyALLEGROSQL := `create block replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2));`
	tk.MustExec(replacePrimaryKeyALLEGROSQL)
	replacePrimaryKeyALLEGROSQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeyALLEGROSQL)
	replacePrimaryKeyALLEGROSQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeyALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")

	// For Issue989
	issue989ALLEGROSQL := `CREATE TABLE tIssue989 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue989ALLEGROSQL)
	issue989ALLEGROSQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989ALLEGROSQL)
	tk.CheckLastMessage("")
	issue989ALLEGROSQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989ALLEGROSQL)
	tk.CheckLastMessage("")
	r := tk.MustQuery("select * from tIssue989;")
	r.Check(testkit.Rows("111 2"))

	// For Issue1012
	issue1012ALLEGROSQL := `CREATE TABLE tIssue1012 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue1012ALLEGROSQL)
	issue1012ALLEGROSQL = `insert into tIssue1012 (a, b) values (1, 2);`
	tk.MustExec(issue1012ALLEGROSQL)
	issue1012ALLEGROSQL = `insert into tIssue1012 (a, b) values (2, 1);`
	tk.MustExec(issue1012ALLEGROSQL)
	issue1012ALLEGROSQL = `replace into tIssue1012(a, b) values (1, 1);`
	tk.MustExec(issue1012ALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(3))
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from tIssue1012;")
	r.Check(testkit.Rows("1 1"))

	// Test Replace with info message
	tk.MustExec(`drop block if exists t1`)
	tk.MustExec(`create block t1(a int primary key, b int);`)
	tk.MustExec(`insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	tk.MustExec(`replace into t1 values(1,1);`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`replace into t1 values(1,1),(2,2);`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	tk.MustExec(`replace into t1 values(4,14),(5,15),(6,16),(7,17),(8,18)`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(7))
	tk.CheckLastMessage("Records: 5  Duplicates: 2  Warnings: 0")
	tk.MustExec(`replace into t1 select * from (select 1, 2) as tmp;`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")

	// Assign `DEFAULT` in `REPLACE` statement
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (a int primary key, b int default 20, c int default 30);")
	tk.MustExec("insert into t1 value (1, 2, 3);")
	tk.MustExec("replace t1 set a=1, b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30"))
	tk.MustExec("replace t1 set a=2, b=default, c=default")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30", "2 20 30"))
	tk.MustExec("replace t1 set a=2, b=default(c), c=default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30", "2 30 20"))
	tk.MustExec("replace t1 set a=default(b)+default(c)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30", "2 30 20", "50 20 30"))
	// With generated defCausumns
	tk.MustExec("create block t2 (pk int primary key, a int default 1, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("replace t2 set pk=1, b=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1"))
	tk.MustExec("replace t2 set pk=2, a=10, b=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1", "2 10 -10 -10"))
	tk.MustExec("replace t2 set pk=2, c=default, a=20;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1", "2 20 -20 -20"))
	tk.MustExec("replace t2 set pk=2, a=default, b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1", "2 1 -1 -1"))
	tk.MustExec("replace t2 set pk=3, a=default(a), b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1", "2 1 -1 -1", "3 1 -1 -1"))
	tk.MustGetErrCode("replace t2 set b=default(a);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("replace t2 set a=default(b), b=default(b);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("replace t2 set a=default(a), c=default(c);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("replace t2 set a=default(a), c=default(a);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustExec("drop block t1, t2")
}

func (s *testSuite2) TestGeneratedDeferredCausetForInsert(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	// test cases for default behavior
	tk.MustExec(`drop block if exists t1;`)
	tk.MustExec(`create block t1(id int, id_gen int as(id + 42), b int, unique key id_gen(id_gen));`)
	tk.MustExec(`insert into t1 (id, b) values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	tk.MustExec(`replace into t1 (id, b) values(1,1);`)
	tk.MustExec(`replace into t1 (id, b) values(1,1),(2,2);`)
	tk.MustExec(`replace into t1 (id, b) values(6,16),(7,17),(8,18);`)
	tk.MustQuery("select * from t1;").Check(testkit.Rows(
		"1 43 1", "2 44 2", "3 45 3", "4 46 4", "5 47 5", "6 48 16", "7 49 17", "8 50 18"))
	tk.MustExec(`insert into t1 (id, b) values (6,18) on duplicate key uFIDelate id = -id;`)
	tk.MustExec(`insert into t1 (id, b) values (7,28) on duplicate key uFIDelate b = -values(b);`)
	tk.MustQuery("select * from t1;").Check(testkit.Rows(
		"1 43 1", "2 44 2", "3 45 3", "4 46 4", "5 47 5", "-6 36 16", "7 49 -28", "8 50 18"))

	// test cases for virtual and stored defCausumns in the same block
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t
	(i int as(k+1) stored, j int as(k+2) virtual, k int, unique key idx_i(i), unique key idx_j(j))`)
	tk.MustExec(`insert into t (k) values (1), (2)`)
	tk.MustExec(`replace into t (k) values (1), (2)`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2 3 1", "3 4 2"))

	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t
	(i int as(k+1) stored, j int as(k+2) virtual, k int, unique key idx_j(j))`)
	tk.MustExec(`insert into t (k) values (1), (2)`)
	tk.MustExec(`replace into t (k) values (1), (2)`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2 3 1", "3 4 2"))

	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t
	(i int as(k+1) stored, j int as(k+2) virtual, k int, unique key idx_i(i))`)
	tk.MustExec(`insert into t (k) values (1), (2)`)
	tk.MustExec(`replace into t (k) values (1), (2)`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2 3 1", "3 4 2"))
}

func (s *testSuite4) TestPartitionedBlockReplace(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	testALLEGROSQL := `drop block if exists replace_test;
		    create block replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)
			partition by range (id) (
			PARTITION p0 VALUES LESS THAN (3),
			PARTITION p1 VALUES LESS THAN (5),
			PARTITION p2 VALUES LESS THAN (7),
			PARTITION p3 VALUES LESS THAN (9));`
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("Records: 3  Duplicates: 0  Warnings: 0")

	errReplaceALLEGROSQL := `replace replace_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceALLEGROSQL = `replace replace_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceALLEGROSQL = `replace replace_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceALLEGROSQL = `replace replace_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceSetALLEGROSQL := `replace replace_test set c1 = 3;`
	tk.MustExec(replaceSetALLEGROSQL)
	tk.CheckLastMessage("")

	errReplaceSetALLEGROSQL := `replace replace_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSetALLEGROSQL = `replace replace_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	tk.MustExec(`drop block if exists replace_test_1`)
	tk.MustExec(`create block replace_test_1 (id int, c1 int) partition by range (id) (
			PARTITION p0 VALUES LESS THAN (4),
			PARTITION p1 VALUES LESS THAN (6),
			PARTITION p2 VALUES LESS THAN (8),
			PARTITION p3 VALUES LESS THAN (10),
			PARTITION p4 VALUES LESS THAN (100))`)
	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test;`)
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")

	tk.MustExec(`drop block if exists replace_test_2`)
	tk.MustExec(`create block replace_test_2 (id int, c1 int) partition by range (id) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN (50),
			PARTITION p2 VALUES LESS THAN (100),
			PARTITION p3 VALUES LESS THAN (300))`)
	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`)
	tk.CheckLastMessage("Records: 8  Duplicates: 0  Warnings: 0")

	errReplaceSelectALLEGROSQL := `replace replace_test_1 select c1 from replace_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	tk.MustExec(`drop block if exists replace_test_3`)
	replaceUniqueIndexALLEGROSQL := `create block replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2)) partition by range (c2) (
				    PARTITION p0 VALUES LESS THAN (4),
				    PARTITION p1 VALUES LESS THAN (7),
				    PARTITION p2 VALUES LESS THAN (11))`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c2=8;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c2=8;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c1=8, c2=8;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))
	tk.CheckLastMessage("")

	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")

	replaceUniqueIndexALLEGROSQL = `create block replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2)) partition by range (c1) (
				    PARTITION p0 VALUES LESS THAN (4),
				    PARTITION p1 VALUES LESS THAN (7),
				    PARTITION p2 VALUES LESS THAN (11));`
	tk.MustExec(`drop block if exists replace_test_4`)
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replacePrimaryKeyALLEGROSQL := `create block replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2)) partition by range (c2) (
				    PARTITION p0 VALUES LESS THAN (4),
				    PARTITION p1 VALUES LESS THAN (7),
				    PARTITION p2 VALUES LESS THAN (11));`
	tk.MustExec(replacePrimaryKeyALLEGROSQL)
	replacePrimaryKeyALLEGROSQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeyALLEGROSQL)
	replacePrimaryKeyALLEGROSQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeyALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	issue989ALLEGROSQL := `CREATE TABLE tIssue989 (a int, b int, KEY(a), UNIQUE KEY(b)) partition by range (b) (
			    PARTITION p1 VALUES LESS THAN (100),
			    PARTITION p2 VALUES LESS THAN (200))`
	tk.MustExec(issue989ALLEGROSQL)
	issue989ALLEGROSQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989ALLEGROSQL)
	issue989ALLEGROSQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989ALLEGROSQL)
	r := tk.MustQuery("select * from tIssue989;")
	r.Check(testkit.Rows("111 2"))
}

func (s *testSuite4) TestHashPartitionedBlockReplace(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = '1';")
	tk.MustExec("drop block if exists replace_test;")
	testALLEGROSQL := `create block replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)
			partition by hash(id) partitions 4;`
	tk.MustExec(testALLEGROSQL)

	testALLEGROSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testALLEGROSQL)

	errReplaceALLEGROSQL := `replace replace_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceALLEGROSQL = `replace replace_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceALLEGROSQL = `replace replace_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceALLEGROSQL = `replace replace_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSetALLEGROSQL := `replace replace_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSetALLEGROSQL = `replace replace_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	tk.MustExec(`replace replace_test set c1 = 3;`)
	tk.MustExec(`replace replace_test set c1 = 4;`)
	tk.MustExec(`replace replace_test set c1 = 5;`)
	tk.MustExec(`replace replace_test set c1 = 6;`)
	tk.MustExec(`replace replace_test set c1 = 7;`)

	tk.MustExec(`drop block if exists replace_test_1`)
	tk.MustExec(`create block replace_test_1 (id int, c1 int) partition by hash(id) partitions 5;`)
	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test;`)

	tk.MustExec(`drop block if exists replace_test_2`)
	tk.MustExec(`create block replace_test_2 (id int, c1 int) partition by hash(id) partitions 6;`)

	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`)

	errReplaceSelectALLEGROSQL := `replace replace_test_1 select c1 from replace_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSelectALLEGROSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	tk.MustExec(`drop block if exists replace_test_3`)
	replaceUniqueIndexALLEGROSQL := `create block replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2)) partition by hash(c2) partitions 7;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)

	tk.MustExec(`replace into replace_test_3 set c2=8;`)
	tk.MustExec(`replace into replace_test_3 set c2=8;`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.MustExec(`replace into replace_test_3 set c1=8, c2=8;`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))

	tk.MustExec(`replace into replace_test_3 set c2=NULL;`)
	tk.MustExec(`replace into replace_test_3 set c2=NULL;`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	for i := 0; i < 100; i++ {
		allegrosql := fmt.Sprintf("replace into replace_test_3 set c2=%d;", i)
		tk.MustExec(allegrosql)
	}
	result := tk.MustQuery("select count(*) from replace_test_3")
	result.Check(testkit.Rows("102"))

	replaceUniqueIndexALLEGROSQL = `create block replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2)) partition by hash(c1) partitions 8;`
	tk.MustExec(`drop block if exists replace_test_4`)
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	replaceUniqueIndexALLEGROSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replacePrimaryKeyALLEGROSQL := `create block replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2)) partition by hash (c2) partitions 9;`
	tk.MustExec(replacePrimaryKeyALLEGROSQL)
	replacePrimaryKeyALLEGROSQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeyALLEGROSQL)
	replacePrimaryKeyALLEGROSQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeyALLEGROSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	issue989ALLEGROSQL := `CREATE TABLE tIssue989 (a int, b int, KEY(a), UNIQUE KEY(b)) partition by hash (b) partitions 10;`
	tk.MustExec(issue989ALLEGROSQL)
	issue989ALLEGROSQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989ALLEGROSQL)
	issue989ALLEGROSQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989ALLEGROSQL)
	r := tk.MustQuery("select * from tIssue989;")
	r.Check(testkit.Rows("111 2"))
}

func (s *testSuite8) TestUFIDelate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	s.fillData(tk, "uFIDelate_test")

	uFIDelateStr := `UFIDelATE uFIDelate_test SET name = "abc" where id > 0;`
	tk.MustExec(uFIDelateStr)
	tk.CheckExecResult(2, 0)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")

	// select data
	tk.MustExec("begin")
	r := tk.MustQuery(`SELECT * from uFIDelate_test limit 2;`)
	r.Check(testkit.Rows("1 abc", "2 abc"))
	tk.MustExec("commit")

	tk.MustExec(`UFIDelATE uFIDelate_test SET name = "foo"`)
	tk.CheckExecResult(2, 0)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")

	// block option is auto-increment
	tk.MustExec("begin")
	tk.MustExec("drop block if exists uFIDelate_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create block uFIDelate_test(id int not null auto_increment, name varchar(255), primary key(id))")
	tk.MustExec("insert into uFIDelate_test(name) values ('aa')")
	tk.MustExec("uFIDelate uFIDelate_test set id = 8 where name = 'aa'")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("insert into uFIDelate_test(name) values ('bb')")
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from uFIDelate_test;")
	r.Check(testkit.Rows("8 aa", "9 bb"))
	tk.MustExec("commit")

	tk.MustExec("begin")
	tk.MustExec("drop block if exists uFIDelate_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create block uFIDelate_test(id int not null auto_increment, name varchar(255), index(id))")
	tk.MustExec("insert into uFIDelate_test(name) values ('aa')")
	_, err := tk.Exec("uFIDelate uFIDelate_test set id = null where name = 'aa'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), DeepEquals, "[block:1048]DeferredCauset 'id' cannot be null")

	tk.MustExec("drop block uFIDelate_test")
	tk.MustExec("create block uFIDelate_test(id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into uFIDelate_test(id) values (1)")
	tk.MustExec("uFIDelate uFIDelate_test set id = 2 where id = 1 limit 1")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("select * from uFIDelate_test;")
	r.Check(testkit.Rows("2"))
	tk.MustExec("commit")

	// Test that in a transaction, when a constraint failed in an uFIDelate statement, the record is not inserted.
	tk.MustExec("create block uFIDelate_unique (id int primary key, name int unique)")
	tk.MustExec("insert uFIDelate_unique values (1, 1), (2, 2);")
	tk.MustExec("begin")
	_, err = tk.Exec("uFIDelate uFIDelate_unique set name = 1 where id = 2")
	c.Assert(err, NotNil)
	tk.MustExec("commit")
	tk.MustQuery("select * from uFIDelate_unique").Check(testkit.Rows("1 1", "2 2"))

	// test uFIDelate ignore for pimary key
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a bigint, primary key (a));")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	_, err = tk.Exec("uFIDelate ignore t set a = 1 where a = 2;")
	c.Assert(err, IsNil)
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	// test uFIDelate ignore for truncate as warning
	_, err = tk.Exec("uFIDelate ignore t set a = 1 where a = (select '2a')")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))

	tk.MustExec("uFIDelate ignore t set a = 42 where a = 2;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "42"))

	// test uFIDelate ignore for unique key
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a bigint, unique key I_uniq (a));")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	_, err = tk.Exec("uFIDelate ignore t set a = 1 where a = 2;")
	c.Assert(err, IsNil)
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'I_uniq'"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(id integer auto_increment, t1 datetime, t2 datetime, primary key (id))")
	tk.MustExec("insert into t(t1, t2) values('2000-10-01 01:01:01', '2020-01-01 10:10:10')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2000-10-01 01:01:01 2020-01-01 10:10:10"))
	tk.MustExec("uFIDelate t set t1 = '2020-10-01 10:10:11', t2 = date_add(t1, INTERVAL 10 MINUTE) where id = 1")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2020-10-01 10:10:11 2020-10-01 10:20:11"))

	// for issue #5132
	tk.MustExec("CREATE TABLE `tt1` (" +
		"`a` int(11) NOT NULL," +
		"`b` varchar(32) DEFAULT NULL," +
		"`c` varchar(32) DEFAULT NULL," +
		"PRIMARY KEY (`a`)," +
		"UNIQUE KEY `b_idx` (`b`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")
	tk.MustExec("insert into tt1 values(1, 'a', 'a');")
	tk.MustExec("insert into tt1 values(2, 'd', 'b');")
	r = tk.MustQuery("select * from tt1;")
	r.Check(testkit.Rows("1 a a", "2 d b"))
	tk.MustExec("uFIDelate tt1 set a=5 where c='b';")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("select * from tt1;")
	r.Check(testkit.Rows("1 a a", "5 d b"))

	// Automatic UFIDelating for TIMESTAMP
	tk.MustExec("CREATE TABLE `tsup` (" +
		"`a` int," +
		"`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UFIDelATE CURRENT_TIMESTAMP," +
		"KEY `idx` (`ts`)" +
		");")
	tk.MustExec("insert into tsup values(1, '0000-00-00 00:00:00');")
	tk.MustExec("uFIDelate tsup set a=5;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r1 := tk.MustQuery("select ts from tsup use index (idx);")
	r2 := tk.MustQuery("select ts from tsup;")
	r1.Check(r2.Rows())
	tk.MustExec("uFIDelate tsup set ts='2020-01-01';")
	tk.MustQuery("select ts from tsup;").Check(testkit.Rows("2020-01-01 00:00:00"))

	// issue 5532
	tk.MustExec("create block decimals (a decimal(20, 0) not null)")
	tk.MustExec("insert into decimals values (201)")
	// A warning rather than data truncated error.
	tk.MustExec("uFIDelate decimals set a = a + 1.23;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect DECIMAL value: '202.23'"))
	r = tk.MustQuery("select * from decimals")
	r.Check(testkit.Rows("202"))

	tk.MustExec("drop block t")
	tk.MustExec("CREATE TABLE `t` (	`c1` year DEFAULT NULL, `c2` year DEFAULT NULL, `c3` date DEFAULT NULL, `c4` datetime DEFAULT NULL,	KEY `idx` (`c1`,`c2`))")
	_, err = tk.Exec("UFIDelATE t SET c2=16777215 WHERE c1>= -8388608 AND c1 < -9 ORDER BY c1 LIMIT 2")
	c.Assert(err.Error(), Equals, "cannot convert causet from bigint to type year.")

	tk.MustExec("uFIDelate (select * from t) t set c1 = 1111111")

	// test uFIDelate ignore for bad null error
	tk.MustExec("drop block if exists t;")
	tk.MustExec(`create block t (i int not null default 10)`)
	tk.MustExec("insert into t values (1)")
	tk.MustExec("uFIDelate ignore t set i = null;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1048 DeferredCauset 'i' cannot be null"))
	tk.MustQuery("select * from t").Check(testkit.Rows("0"))

	// issue 7237, uFIDelate subquery block should be forbidden
	tk.MustExec("drop block t")
	tk.MustExec("create block t (k int, v int)")
	_, err = tk.Exec("uFIDelate t, (select * from t) as b set b.k = t.k")
	c.Assert(err.Error(), Equals, "[planner:1288]The target block b of the UFIDelATE is not uFIDelablock")
	tk.MustExec("uFIDelate t, (select * from t) as b set t.k = b.k")

	// issue 8045
	tk.MustExec("drop block if exists t1")
	tk.MustExec(`CREATE TABLE t1 (c1 float)`)
	tk.MustExec("INSERT INTO t1 SET c1 = 1")
	tk.MustExec("UFIDelATE t1 SET c1 = 1.2 WHERE c1=1;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")

	// issue 8119
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t (c1 float(1,1));")
	tk.MustExec("insert into t values (0.0);")
	_, err = tk.Exec("uFIDelate t set c1 = 2.0;")
	c.Assert(types.ErrWarnDataOutOfRange.Equal(err), IsTrue)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a datetime not null, b datetime)")
	tk.MustExec("insert into t value('1999-12-12', '1999-12-13')")
	tk.MustExec(" set @orig_sql_mode=@@sql_mode; set @@sql_mode='';")
	tk.MustQuery("select * from t").Check(testkit.Rows("1999-12-12 00:00:00 1999-12-13 00:00:00"))
	tk.MustExec("uFIDelate t set a = ''")
	tk.MustQuery("select * from t").Check(testkit.Rows("0000-00-00 00:00:00 1999-12-13 00:00:00"))
	tk.MustExec("uFIDelate t set b = ''")
	tk.MustQuery("select * from t").Check(testkit.Rows("0000-00-00 00:00:00 0000-00-00 00:00:00"))
	tk.MustExec("set @@sql_mode=@orig_sql_mode;")

	tk.MustExec("create view v as select * from t")
	_, err = tk.Exec("uFIDelate v set a = '2000-11-11'")
	c.Assert(err.Error(), Equals, core.ErrViewInvalid.GenWithStackByArgs("test", "v").Error())
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	_, err = tk.Exec("uFIDelate seq set minvalue=1")
	c.Assert(err.Error(), Equals, "uFIDelate sequence seq is not supported now.")
	tk.MustExec("drop sequence seq")

	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a int, b int, c int, d int, e int, index idx(a))")
	tk.MustExec("create block t2(a int, b int, c int)")
	tk.MustExec("uFIDelate t1 join t2 on t1.a=t2.a set t1.a=1 where t2.b=1 and t2.c=2")

	// Assign `DEFAULT` in `UFIDelATE` statement
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (a int default 1, b int default 2);")
	tk.MustExec("insert into t1 values (10, 10), (20, 20);")
	tk.MustExec("uFIDelate t1 set a=default where b=10;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "20 20"))
	tk.MustExec("uFIDelate t1 set a=30, b=default where a=20;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "30 2"))
	tk.MustExec("uFIDelate t1 set a=default, b=default where a=30;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "1 2"))
	tk.MustExec("insert into t1 values (40, 40)")
	tk.MustExec("uFIDelate t1 set a=default, b=default")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2", "1 2", "1 2"))
	tk.MustExec("uFIDelate t1 set a=default(b), b=default(a)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("2 1", "2 1", "2 1"))
	// With generated defCausumns
	tk.MustExec("create block t2 (a int default 1, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("insert into t2 values (10, default, default), (20, default, default)")
	tk.MustExec("uFIDelate t2 set b=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("10 -10 -10", "20 -20 -20"))
	tk.MustExec("uFIDelate t2 set a=30, b=default where a=10;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("30 -30 -30", "20 -20 -20"))
	tk.MustExec("uFIDelate t2 set c=default, a=40 where c=-20;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("30 -30 -30", "40 -40 -40"))
	tk.MustExec("uFIDelate t2 set a=default, b=default, c=default where b=-30;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 -1 -1", "40 -40 -40"))
	tk.MustExec("uFIDelate t2 set a=default(a), b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 -1 -1", "1 -1 -1"))
	tk.MustGetErrCode("uFIDelate t2 set b=default(a);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("uFIDelate t2 set a=default(b), b=default(b);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("uFIDelate t2 set a=default(a), c=default(c);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustGetErrCode("uFIDelate t2 set a=default(a), c=default(a);", allegrosql.ErrBadGeneratedDeferredCauset)
	tk.MustExec("drop block t1, t2")
}

func (s *testSuite4) TestPartitionedBlockUFIDelate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec(`create block t (id int not null default 1, name varchar(255))
			PARTITION BY RANGE ( id ) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21))`)

	tk.MustExec(`insert INTO t VALUES (1, "hello");`)
	tk.CheckExecResult(1, 0)
	tk.MustExec(`insert INTO t VALUES (7, "hello");`)
	tk.CheckExecResult(1, 0)

	// uFIDelate non partition defCausumn
	tk.MustExec(`UFIDelATE t SET name = "abc" where id > 0;`)
	tk.CheckExecResult(2, 0)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
	r := tk.MustQuery(`SELECT * from t order by id limit 2;`)
	r.Check(testkit.Rows("1 abc", "7 abc"))

	// uFIDelate partition defCausumn
	tk.MustExec(`uFIDelate t set id = id + 1`)
	tk.CheckExecResult(2, 0)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
	r = tk.MustQuery(`SELECT * from t order by id limit 2;`)
	r.Check(testkit.Rows("2 abc", "8 abc"))

	// uFIDelate partition defCausumn, old and new record locates on different partitions
	tk.MustExec(`uFIDelate t set id = 20 where id = 8`)
	tk.CheckExecResult(2, 0)
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery(`SELECT * from t order by id limit 2;`)
	r.Check(testkit.Rows("2 abc", "20 abc"))

	// block option is auto-increment
	tk.MustExec("drop block if exists t;")
	tk.MustExec(`create block t (id int not null auto_increment, name varchar(255), primary key(id))
			PARTITION BY RANGE ( id ) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21))`)

	tk.MustExec("insert into t(name) values ('aa')")
	tk.MustExec("uFIDelate t set id = 8 where name = 'aa'")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("insert into t(name) values ('bb')")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("8 aa", "9 bb"))

	_, err := tk.Exec("uFIDelate t set id = null where name = 'aa'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), DeepEquals, "[block:1048]DeferredCauset 'id' cannot be null")

	// Test that in a transaction, when a constraint failed in an uFIDelate statement, the record is not inserted.
	tk.MustExec("drop block if exists t;")
	tk.MustExec(`create block t (id int, name int unique)
			PARTITION BY RANGE ( name ) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21))`)
	tk.MustExec("insert t values (1, 1), (2, 2);")
	_, err = tk.Exec("uFIDelate t set name = 1 where id = 2")
	c.Assert(err, NotNil)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "2 2"))

	// test uFIDelate ignore for pimary key
	tk.MustExec("drop block if exists t;")
	tk.MustExec(`create block t(a bigint, primary key (a))
			PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11))`)
	tk.MustExec("insert into t values (5)")
	tk.MustExec("insert into t values (7)")
	_, err = tk.Exec("uFIDelate ignore t set a = 5 where a = 7;")
	c.Assert(err, IsNil)
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '5' for key 'PRIMARY'"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("5", "7"))

	// test uFIDelate ignore for truncate as warning
	_, err = tk.Exec("uFIDelate ignore t set a = 1 where a = (select '2a')")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1292 Truncated incorrect FLOAT value: '2a'"))

	// test uFIDelate ignore for unique key
	tk.MustExec("drop block if exists t;")
	tk.MustExec(`create block t(a bigint, unique key I_uniq (a))
			PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11))`)
	tk.MustExec("insert into t values (5)")
	tk.MustExec("insert into t values (7)")
	_, err = tk.Exec("uFIDelate ignore t set a = 5 where a = 7;")
	c.Assert(err, IsNil)
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '5' for key 'I_uniq'"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("5", "7"))
}

// TestUFIDelateCastOnlyModifiedValues for issue #4514.
func (s *testSuite4) TestUFIDelateCastOnlyModifiedValues(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block uFIDelate_modified (defCaus_1 int, defCaus_2 enum('a', 'b'))")
	tk.MustExec("set ALLEGROSQL_MODE=''")
	tk.MustExec("insert into uFIDelate_modified values (0, 3)")
	r := tk.MustQuery("SELECT * FROM uFIDelate_modified")
	r.Check(testkit.Rows("0 "))
	tk.MustExec("set ALLEGROSQL_MODE=STRICT_ALL_TABLES")
	tk.MustExec("uFIDelate uFIDelate_modified set defCaus_1 = 1")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("SELECT * FROM uFIDelate_modified")
	r.Check(testkit.Rows("1 "))
	_, err := tk.Exec("uFIDelate uFIDelate_modified set defCaus_1 = 2, defCaus_2 = 'c'")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT * FROM uFIDelate_modified")
	r.Check(testkit.Rows("1 "))
	tk.MustExec("uFIDelate uFIDelate_modified set defCaus_1 = 3, defCaus_2 = 'a'")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("SELECT * FROM uFIDelate_modified")
	r.Check(testkit.Rows("3 a"))

	// Test uFIDelate a field with different defCausumn type.
	tk.MustExec(`CREATE TABLE uFIDelate_with_diff_type (a int, b JSON)`)
	tk.MustExec(`INSERT INTO uFIDelate_with_diff_type VALUES(3, '{"a": "测试"}')`)
	tk.MustExec(`UFIDelATE uFIDelate_with_diff_type SET a = '300'`)
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("SELECT a FROM uFIDelate_with_diff_type")
	r.Check(testkit.Rows("300"))
	tk.MustExec(`UFIDelATE uFIDelate_with_diff_type SET b = '{"a":   "\\u6d4b\\u8bd5"}'`)
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 0")
	r = tk.MustQuery("SELECT b FROM uFIDelate_with_diff_type")
	r.Check(testkit.Rows(`{"a": "测试"}`))
}

func (s *testSuite4) fillMultiBlockForUFIDelate(tk *testkit.TestKit) {
	// Create and fill block items
	tk.MustExec("CREATE TABLE items (id int, price TEXT);")
	tk.MustExec(`insert into items values (11, "items_price_11"), (12, "items_price_12"), (13, "items_price_13");`)
	tk.CheckExecResult(3, 0)
	// Create and fill block month
	tk.MustExec("CREATE TABLE month (mid int, mprice TEXT);")
	tk.MustExec(`insert into month values (11, "month_price_11"), (22, "month_price_22"), (13, "month_price_13");`)
	tk.CheckExecResult(3, 0)
}

func (s *testSuite4) TestMultipleBlockUFIDelate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	s.fillMultiBlockForUFIDelate(tk)

	tk.MustExec(`UFIDelATE items, month  SET items.price=month.mprice WHERE items.id=month.mid;`)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
	tk.MustExec("begin")
	r := tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 month_price_11", "12 items_price_12", "13 month_price_13"))
	tk.MustExec("commit")

	// Single-block syntax but with multiple blocks
	tk.MustExec(`UFIDelATE items join month on items.id=month.mid SET items.price=month.mid;`)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 11", "12 items_price_12", "13 13"))
	tk.MustExec("commit")

	// JoinBlock with alias block name.
	tk.MustExec(`UFIDelATE items T0 join month T1 on T0.id=T1.mid SET T0.price=T1.mprice;`)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 month_price_11", "12 items_price_12", "13 month_price_13"))
	tk.MustExec("commit")

	// fix https://github.com/whtcorpsinc/milevadb/issues/369
	testALLEGROSQL := `
		DROP TABLE IF EXISTS t1, t2;
		create block t1 (c int);
		create block t2 (c varchar(256));
		insert into t1 values (1), (2);
		insert into t2 values ("a"), ("b");
		uFIDelate t1, t2 set t1.c = 10, t2.c = "abc";`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("Rows matched: 4  Changed: 4  Warnings: 0")

	// fix https://github.com/whtcorpsinc/milevadb/issues/376
	testALLEGROSQL = `DROP TABLE IF EXISTS t1, t2;
		create block t1 (c1 int);
		create block t2 (c2 int);
		insert into t1 values (1), (2);
		insert into t2 values (1), (2);
		uFIDelate t1, t2 set t1.c1 = 10, t2.c2 = 2 where t2.c2 = 1;`
	tk.MustExec(testALLEGROSQL)
	tk.CheckLastMessage("Rows matched: 3  Changed: 3  Warnings: 0")

	r = tk.MustQuery("select * from t1")
	r.Check(testkit.Rows("10", "10"))

	// test https://github.com/whtcorpsinc/milevadb/issues/3604
	tk.MustExec("drop block if exists t, t")
	tk.MustExec("create block t (a int, b int)")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3)")
	tk.CheckLastMessage("Records: 3  Duplicates: 0  Warnings: 0")
	tk.MustExec("uFIDelate t m, t n set m.a = m.a + 1")
	tk.CheckLastMessage("Rows matched: 3  Changed: 3  Warnings: 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1", "3 2", "4 3"))
	tk.MustExec("uFIDelate t m, t n set n.a = n.a - 1, n.b = n.b + 1")
	tk.CheckLastMessage("Rows matched: 3  Changed: 3  Warnings: 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "2 3", "3 4"))
}

func (s *testSuite) TestDelete(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.fillData(tk, "delete_test")

	tk.MustExec(`uFIDelate delete_test set name = "abc" where id = 2;`)
	tk.CheckExecResult(1, 0)

	tk.MustExec(`delete from delete_test where id = 2 limit 1;`)
	tk.CheckExecResult(1, 0)

	// Test delete with false condition
	tk.MustExec(`delete from delete_test where 0;`)
	tk.CheckExecResult(0, 0)

	tk.MustExec("insert into delete_test values (2, 'abc')")
	tk.MustExec(`delete from delete_test where delete_test.id = 2 limit 1`)
	tk.CheckExecResult(1, 0)

	// Select data
	tk.MustExec("begin")
	rows := tk.MustQuery(`SELECT * from delete_test limit 2;`)
	rows.Check(testkit.Rows("1 hello"))
	tk.MustExec("commit")

	// Test delete ignore
	tk.MustExec("insert into delete_test values (2, 'abc')")
	_, err := tk.Exec("delete from delete_test where id = (select '2a')")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete ignore from delete_test where id = (select '2a')")
	c.Assert(err, IsNil)
	tk.CheckExecResult(1, 0)
	r := tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1292 Truncated incorrect FLOAT value: '2a'"))

	tk.MustExec(`delete from delete_test ;`)
	tk.CheckExecResult(1, 0)

	tk.MustExec("create view v as select * from delete_test")
	_, err = tk.Exec("delete from v where name = 'aaa'")
	c.Assert(err.Error(), Equals, core.ErrViewInvalid.GenWithStackByArgs("test", "v").Error())
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	_, err = tk.Exec("delete from seq")
	c.Assert(err.Error(), Equals, "delete sequence seq is not supported now.")
	tk.MustExec("drop sequence seq")
}

func (s *testSuite4) TestPartitionedBlockDelete(c *C) {
	createBlock := `CREATE TABLE test.t (id int not null default 1, name varchar(255), index(id))
			  PARTITION BY RANGE ( id ) (
			  PARTITION p0 VALUES LESS THAN (6),
			  PARTITION p1 VALUES LESS THAN (11),
			  PARTITION p2 VALUES LESS THAN (16),
			  PARTITION p3 VALUES LESS THAN (21))`

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec(createBlock)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, "hello")`, i))
	}

	tk.MustExec(`delete from t where id = 2 limit 1;`)
	tk.CheckExecResult(1, 0)

	// Test delete with false condition
	tk.MustExec(`delete from t where 0;`)
	tk.CheckExecResult(0, 0)

	tk.MustExec("insert into t values (2, 'abc')")
	tk.MustExec(`delete from t where t.id = 2 limit 1`)
	tk.CheckExecResult(1, 0)

	// Test delete ignore
	tk.MustExec("insert into t values (2, 'abc')")
	_, err := tk.Exec("delete from t where id = (select '2a')")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete ignore from t where id = (select '2a')")
	c.Assert(err, IsNil)
	tk.CheckExecResult(1, 0)
	r := tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1292 Truncated incorrect FLOAT value: '2a'"))

	// Test delete without using index, involve multiple partitions.
	tk.MustExec("delete from t ignore index(id) where id >= 13 and id <= 17")
	tk.CheckExecResult(5, 0)

	tk.MustExec("admin check block t")
	tk.MustExec(`delete from t;`)
	tk.CheckExecResult(14, 0)

	// Fix that partitioned block should not use PointGetPlan.
	tk.MustExec(`create block t1 (c1 bigint, c2 bigint, c3 bigint, primary key(c1)) partition by range (c1) (partition p0 values less than (3440))`)
	tk.MustExec("insert into t1 values (379, 379, 379)")
	tk.MustExec("delete from t1 where c1 = 379")
	tk.CheckExecResult(1, 0)
	tk.MustExec(`drop block t1;`)
}

func (s *testSuite4) fillDataMultiBlock(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2, t3")
	// Create and fill block t1
	tk.MustExec("create block t1 (id int, data int);")
	tk.MustExec("insert into t1 values (11, 121), (12, 122), (13, 123);")
	tk.CheckExecResult(3, 0)
	// Create and fill block t2
	tk.MustExec("create block t2 (id int, data int);")
	tk.MustExec("insert into t2 values (11, 221), (22, 222), (23, 223);")
	tk.CheckExecResult(3, 0)
	// Create and fill block t3
	tk.MustExec("create block t3 (id int, data int);")
	tk.MustExec("insert into t3 values (11, 321), (22, 322), (23, 323);")
	tk.CheckExecResult(3, 0)
}

func (s *testSuite4) TestMultiBlockDelete(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.fillDataMultiBlock(tk)

	tk.MustExec(`delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;`)
	tk.CheckExecResult(2, 0)

	// Select data
	r := tk.MustQuery("select * from t3")
	c.Assert(r.Rows(), HasLen, 3)
}

func (s *testSuite4) TestQualifiedDelete(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("drop block if exists t2")
	tk.MustExec("create block t1 (c1 int, c2 int, index (c1))")
	tk.MustExec("create block t2 (c1 int, c2 int)")
	tk.MustExec("insert into t1 values (1, 1), (2, 2)")

	// delete with index
	tk.MustExec("delete from t1 where t1.c1 = 1")
	tk.CheckExecResult(1, 0)

	// delete with no index
	tk.MustExec("delete from t1 where t1.c2 = 2")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("select * from t1")
	c.Assert(r.Rows(), HasLen, 0)

	tk.MustExec("insert into t1 values (1, 3)")
	tk.MustExec("delete from t1 as a where a.c1 = 1")
	tk.CheckExecResult(1, 0)

	tk.MustExec("insert into t1 values (1, 1), (2, 2)")
	tk.MustExec("insert into t2 values (2, 1), (3,1)")
	tk.MustExec("delete t1, t2 from t1 join t2 where t1.c1 = t2.c2")
	tk.CheckExecResult(3, 0)

	tk.MustExec("insert into t2 values (2, 1), (3,1)")
	tk.MustExec("delete a, b from t1 as a join t2 as b where a.c2 = b.c1")
	tk.CheckExecResult(2, 0)

	_, err := tk.Exec("delete t1, t2 from t1 as a join t2 as b where a.c2 = b.c1")
	c.Assert(err, NotNil)
}

func (s *testSuite8) TestLoadDataMissingDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	createALLEGROSQL := `create block load_data_missing (id int, t timestamp not null)`
	tk.MustExec(createALLEGROSQL)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into block load_data_missing")
	ctx := tk.Se.(stochastikctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)

	deleteALLEGROSQL := "delete from load_data_missing"
	selectALLEGROSQL := "select id, hour(t), minute(t) from load_data_missing;"
	_, reachLimit, err := ld.InsertData(context.Background(), nil, nil)
	c.Assert(err, IsNil)
	c.Assert(reachLimit, IsFalse)
	r := tk.MustQuery(selectALLEGROSQL)
	r.Check(nil)

	curTime := types.CurrentTime(allegrosql.TypeTimestamp)
	timeHour := curTime.Hour()
	timeMinute := curTime.Minute()
	tests := []testCase{
		{nil, []byte("12\n"), []string{fmt.Sprintf("12|%v|%v", timeHour, timeMinute)}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)

	tk.MustExec("alter block load_data_missing add defCausumn t2 timestamp null")
	curTime = types.CurrentTime(allegrosql.TypeTimestamp)
	timeHour = curTime.Hour()
	timeMinute = curTime.Minute()
	selectALLEGROSQL = "select id, hour(t), minute(t), t2 from load_data_missing;"
	tests = []testCase{
		{nil, []byte("12\n"), []string{fmt.Sprintf("12|%v|%v|<nil>", timeHour, timeMinute)}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)

}

func (s *testSuite4) TestLoadData(c *C) {
	trivialMsg := "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	createALLEGROSQL := `drop block if exists load_data_test;
		create block load_data_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 varchar(255) default "def", c3 int);`
	_, err := tk.Exec("load data local infile '/tmp/nonexistence.csv' into block load_data_test")
	c.Assert(err, NotNil)
	tk.MustExec(createALLEGROSQL)
	_, err = tk.Exec("load data infile '/tmp/nonexistence.csv' into block load_data_test")
	c.Assert(err, NotNil)
	_, err = tk.Exec("load data local infile '/tmp/nonexistence.csv' replace into block load_data_test")
	c.Assert(err, NotNil)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into block load_data_test")
	ctx := tk.Se.(stochastikctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)

	deleteALLEGROSQL := "delete from load_data_test"
	selectALLEGROSQL := "select * from load_data_test;"
	// data1 = nil, data2 = nil, fields and lines is default
	ctx.GetStochastikVars().StmtCtx.DupKeyAsWarning = true
	ctx.GetStochastikVars().StmtCtx.BadNullAsWarning = true
	_, reachLimit, err := ld.InsertData(context.Background(), nil, nil)
	c.Assert(err, IsNil)
	c.Assert(reachLimit, IsFalse)
	err = ld.CheckAndInsertOneBatch(context.Background(), ld.GetRows(), ld.GetCurBatchCnt())
	c.Assert(err, IsNil)
	ld.SetMaxRowsInBatch(20000)
	r := tk.MustQuery(selectALLEGROSQL)
	r.Check(nil)

	sc := ctx.GetStochastikVars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
	}()
	sc.IgnoreTruncate = false
	// fields and lines are default, InsertData returns data is nil
	tests := []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("\n"), []string{"1|<nil>|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("\t\n"), []string{"2|0|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 2"},
		{nil, []byte("3\t2\t3\t4\n"), []string{"3|2|3|4"}, nil, trivialMsg},
		{nil, []byte("3*1\t2\t3\t4\n"), []string{"3|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("4\t2\t\t3\t4\n"), []string{"4|2||3"}, nil, trivialMsg},
		{nil, []byte("\t1\t2\t3\t4\n"), []string{"5|1|2|3"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("6\t2\t3\n"), []string{"6|2|3|<nil>"}, nil, trivialMsg},
		{nil, []byte("\t2\t3\t4\n\t22\t33\t44\n"), []string{"7|2|3|4", "8|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{nil, []byte("7\t2\t3\t4\n7\t22\t33\t44\n"), []string{"7|2|3|4"}, nil, "Records: 2  Deleted: 0  Skipped: 1  Warnings: 1"},

		// data1 != nil, data2 = nil
		{[]byte("\t2\t3\t4"), nil, []string{"9|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// data1 != nil, data2 != nil
		{[]byte("\t2\t3"), []byte("\t4\t5\n"), []string{"10|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\t2\t3"), []byte("4\t5\n"), []string{"11|2|34|5"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// data1 != nil, data2 != nil, InsertData returns data isn't nil
		{[]byte("\t2\t3"), []byte("\t4\t5"), nil, []byte("\t2\t3\t4\t5"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)
	c.Assert(sc.WarningCount(), Equals, uint16(1))

	// lines starting symbol is "" and terminated symbol length is 2, InsertData returns data is nil
	ld.LinesInfo.Terminated = "||"
	tests = []testCase{
		// data1 != nil, data2 != nil
		{[]byte("0\t2\t3"), []byte("\t4\t5||"), []string{"12|2|3|4"}, nil, trivialMsg},
		{[]byte("1\t2\t3\t4\t5|"), []byte("|"), []string{"1|2|3|4"}, nil, trivialMsg},
		{[]byte("2\t2\t3\t4\t5|"), []byte("|3\t22\t33\t44\t55||"),
			[]string{"2|2|3|4", "3|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("3\t2\t3\t4\t5|"), []byte("|4\t22\t33||"), []string{
			"3|2|3|4", "4|22|33|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("4\t2\t3\t4\t5|"), []byte("|5\t22\t33||6\t222||"),
			[]string{"4|2|3|4", "5|22|33|<nil>", "6|222|<nil>|<nil>"}, nil, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("6\t2\t3"), []byte("4\t5||"), []string{"6|2|34|5"}, nil, trivialMsg},
	}
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)

	// fields and lines aren't default, InsertData returns data is nil
	ld.FieldsInfo.Terminated = "\\"
	ld.LinesInfo.Starting = "xxx"
	ld.LinesInfo.Terminated = "|!#^"
	tests = []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("xxx|!#^"), []string{"13|<nil>|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("xxx\\|!#^"), []string{"14|0|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 2"},
		{nil, []byte("xxx3\\2\\3\\4|!#^"), []string{"3|2|3|4"}, nil, trivialMsg},
		{nil, []byte("xxx4\\2\\\\3\\4|!#^"), []string{"4|2||3"}, nil, trivialMsg},
		{nil, []byte("xxx\\1\\2\\3\\4|!#^"), []string{"15|1|2|3"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("xxx6\\2\\3|!#^"), []string{"6|2|3|<nil>"}, nil, trivialMsg},
		{nil, []byte("xxx\\2\\3\\4|!#^xxx\\22\\33\\44|!#^"), []string{
			"16|2|3|4",
			"17|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{nil, []byte("\\2\\3\\4|!#^\\22\\33\\44|!#^xxx\\222\\333\\444|!#^"), []string{
			"18|222|333|444"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// data1 != nil, data2 = nil
		{[]byte("xxx\\2\\3\\4"), nil, []string{"19|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\\2\\3\\4|!#^"), nil, []string{}, nil, "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("\\2\\3\\4|!#^xxx18\\22\\33\\44|!#^"), nil,
			[]string{"18|22|33|44"}, nil, trivialMsg},

		// data1 != nil, data2 != nil
		{[]byte("xxx10\\2\\3"), []byte("\\4|!#^"),
			[]string{"10|2|3|4"}, nil, trivialMsg},
		{[]byte("10\\2\\3xx"), []byte("x11\\4\\5|!#^"),
			[]string{"11|4|5|<nil>"}, nil, trivialMsg},
		{[]byte("xxx21\\2\\3\\4\\5|!"), []byte("#^"),
			[]string{"21|2|3|4"}, nil, trivialMsg},
		{[]byte("xxx22\\2\\3\\4\\5|!"), []byte("#^xxx23\\22\\33\\44\\55|!#^"),
			[]string{"22|2|3|4", "23|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx23\\2\\3\\4\\5|!"), []byte("#^xxx24\\22\\33|!#^"),
			[]string{"23|2|3|4", "24|22|33|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx24\\2\\3\\4\\5|!"), []byte("#^xxx25\\22\\33|!#^xxx26\\222|!#^"),
			[]string{"24|2|3|4", "25|22|33|<nil>", "26|222|<nil>|<nil>"}, nil, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx25\\2\\3\\4\\5|!"), []byte("#^26\\22\\33|!#^xxx27\\222|!#^"),
			[]string{"25|2|3|4", "27|222|<nil>|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx\\2\\3"), []byte("4\\5|!#^"), []string{"28|2|34|5"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// InsertData returns data isn't nil
		{nil, []byte("\\2\\3\\4|!#^"), nil, []byte("#^"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{nil, []byte("\\4\\5"), nil, []byte("\\5"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("\\2\\3"), []byte("\\4\\5"), nil, []byte("\\5"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx1\\2\\3|"), []byte("!#^\\4\\5|!#"),
			[]string{"1|2|3|<nil>"}, []byte("!#"), trivialMsg},
		{[]byte("xxx1\\2\\3\\4\\5|!"), []byte("#^xxx2\\22\\33|!#^3\\222|!#^"),
			[]string{"1|2|3|4", "2|22|33|<nil>"}, []byte("#^"), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xx1\\2\\3"), []byte("\\4\\5|!#^"), nil, []byte("#^"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)

	// lines starting symbol is the same as terminated symbol, InsertData returns data is nil
	ld.LinesInfo.Terminated = "xxx"
	tests = []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("xxxxxx"), []string{"29|<nil>|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("xxx3\\2\\3\\4xxx"), []string{"3|2|3|4"}, nil, trivialMsg},
		{nil, []byte("xxx\\2\\3\\4xxxxxx\\22\\33\\44xxx"),
			[]string{"30|2|3|4", "31|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},

		// data1 != nil, data2 = nil
		{[]byte("xxx\\2\\3\\4"), nil, []string{"32|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// data1 != nil, data2 != nil
		{[]byte("xxx10\\2\\3"), []byte("\\4\\5xxx"), []string{"10|2|3|4"}, nil, trivialMsg},
		{[]byte("xxxxx10\\2\\3"), []byte("\\4\\5xxx"), []string{"33|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("xxx21\\2\\3\\4\\5xx"), []byte("x"), []string{"21|2|3|4"}, nil, trivialMsg},
		{[]byte("xxx32\\2\\3\\4\\5x"), []byte("xxxxx33\\22\\33\\44\\55xxx"),
			[]string{"32|2|3|4", "33|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx33\\2\\3\\4\\5xxx"), []byte("xxx34\\22\\33xxx"),
			[]string{"33|2|3|4", "34|22|33|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx34\\2\\3\\4\\5xx"), []byte("xxxx35\\22\\33xxxxxx36\\222xxx"),
			[]string{"34|2|3|4", "35|22|33|<nil>", "36|222|<nil>|<nil>"}, nil, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 0"},

		// InsertData returns data isn't nil
		{nil, []byte("\\2\\3\\4xxxx"), nil, []byte("xxxx"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("\\2\\3\\4xxx"), nil, []string{"37|<nil>|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\\2\\3\\4xxxxxx11\\22\\33\\44xxx"), nil,
			[]string{"38|<nil>|<nil>|<nil>", "39|<nil>|<nil>|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("xx10\\2\\3"), []byte("\\4\\5xxx"), nil, []byte("xxx"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx10\\2\\3"), []byte("\\4xxxx"), []string{"10|2|3|4"}, []byte("x"), trivialMsg},
		{[]byte("xxx10\\2\\3\\4\\5x"), []byte("xx11\\22\\33xxxxxx12\\222xxx"),
			[]string{"10|2|3|4", "40|<nil>|<nil>|<nil>"}, []byte("xxx"), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 1"},
	}
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)
}

func (s *testSuite4) TestLoadDataEscape(c *C) {
	trivialMsg := "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test; drop block if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into block load_data_test")
	ctx := tk.Se.(stochastikctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)
	// test escape
	tests := []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("1\ta string\n"), []string{"1|a string"}, nil, trivialMsg},
		{nil, []byte("2\tstr \\t\n"), []string{"2|str \t"}, nil, trivialMsg},
		{nil, []byte("3\tstr \\n\n"), []string{"3|str \n"}, nil, trivialMsg},
		{nil, []byte("4\tboth \\t\\n\n"), []string{"4|both \t\n"}, nil, trivialMsg},
		{nil, []byte("5\tstr \\\\\n"), []string{"5|str \\"}, nil, trivialMsg},
		{nil, []byte("6\t\\r\\t\\n\\0\\Z\\b\n"), []string{"6|" + string([]byte{'\r', '\t', '\n', 0, 26, '\b'})}, nil, trivialMsg},
		{nil, []byte("7\trtn0ZbN\n"), []string{"7|" + string([]byte{'r', 't', 'n', '0', 'Z', 'b', 'N'})}, nil, trivialMsg},
		{nil, []byte("8\trtn0Zb\\N\n"), []string{"8|" + string([]byte{'r', 't', 'n', '0', 'Z', 'b', 'N'})}, nil, trivialMsg},
		{nil, []byte("9\ttab\\	tab\n"), []string{"9|tab	tab"}, nil, trivialMsg},
	}
	deleteALLEGROSQL := "delete from load_data_test"
	selectALLEGROSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)
}

// TestLoadDataSpecifiedDeferredCausets reuse TestLoadDataEscape's test case :-)
func (s *testSuite4) TestLoadDataSpecifiedDeferredCausets(c *C) {
	trivialMsg := "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test; drop block if exists load_data_test;")
	tk.MustExec(`create block load_data_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 varchar(255) default "def", c3 int default 0);`)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into block load_data_test (c1, c2)")
	ctx := tk.Se.(stochastikctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)
	// test
	tests := []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("7\ta string\n"), []string{"1|7|a string|0"}, nil, trivialMsg},
		{nil, []byte("8\tstr \\t\n"), []string{"2|8|str \t|0"}, nil, trivialMsg},
		{nil, []byte("9\tstr \\n\n"), []string{"3|9|str \n|0"}, nil, trivialMsg},
		{nil, []byte("10\tboth \\t\\n\n"), []string{"4|10|both \t\n|0"}, nil, trivialMsg},
		{nil, []byte("11\tstr \\\\\n"), []string{"5|11|str \\|0"}, nil, trivialMsg},
		{nil, []byte("12\t\\r\\t\\n\\0\\Z\\b\n"), []string{"6|12|" + string([]byte{'\r', '\t', '\n', 0, 26, '\b'}) + "|0"}, nil, trivialMsg},
		{nil, []byte("\\N\ta string\n"), []string{"7|<nil>|a string|0"}, nil, trivialMsg},
	}
	deleteALLEGROSQL := "delete from load_data_test"
	selectALLEGROSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)
}

func (s *testSuite4) TestLoadDataIgnoreLines(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test; drop block if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into block load_data_test ignore 1 lines")
	ctx := tk.Se.(stochastikctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)
	tests := []testCase{
		{nil, []byte("1\tline1\n2\tline2\n"), []string{"2|line2"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"},
		{nil, []byte("1\tline1\n2\tline2\n3\tline3\n"), []string{"2|line2", "3|line3"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	deleteALLEGROSQL := "delete from load_data_test"
	selectALLEGROSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)
}

// TestLoadDataOverflowBigintUnsigned related to issue 6360
func (s *testSuite4) TestLoadDataOverflowBigintUnsigned(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test; drop block if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (a bigint unsigned);")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into block load_data_test")
	ctx := tk.Se.(stochastikctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)
	tests := []testCase{
		{nil, []byte("-1\n-18446744073709551615\n-18446744073709551616\n"), []string{"0", "0", "0"}, nil, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 3"},
		{nil, []byte("-9223372036854775809\n18446744073709551616\n"), []string{"0", "18446744073709551615"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
	}
	deleteALLEGROSQL := "delete from load_data_test"
	selectALLEGROSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectALLEGROSQL, deleteALLEGROSQL)
}

func (s *testSuite4) TestLoadDataIntoPartitionedBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block range_t (a int, b int) partition by range (a) ( " +
		"partition p0 values less than (4)," +
		"partition p1 values less than (7)," +
		"partition p2 values less than (11))")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into block range_t fields terminated by ','")
	ctx := tk.Se.(stochastikctx.Context)
	ld := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ctx.NewTxn(context.Background()), IsNil)

	_, _, err := ld.InsertData(context.Background(), nil, []byte("1,2\n3,4\n5,6\n7,8\n9,10\n"))
	c.Assert(err, IsNil)
	err = ld.CheckAndInsertOneBatch(context.Background(), ld.GetRows(), ld.GetCurBatchCnt())
	c.Assert(err, IsNil)
	ld.SetMaxRowsInBatch(20000)
	ld.SetMessage()
	ctx.StmtCommit()
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testSuite4) TestNullDefault(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test; drop block if exists test_null_default;")
	tk.MustExec("set timestamp = 1234")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("create block test_null_default (ts timestamp null default current_timestamp)")
	tk.MustExec("insert into test_null_default values (null)")
	tk.MustQuery("select * from test_null_default").Check(testkit.Rows("<nil>"))
	tk.MustExec("insert into test_null_default values ()")
	tk.MustQuery("select * from test_null_default").Check(testkit.Rows("<nil>", "1970-01-01 08:20:34"))
}

func (s *testSuite4) TestNotNullDefault(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test; drop block if exists t1,t2;")
	defer tk.MustExec("drop block t1,t2")
	tk.MustExec("create block t1 (a int not null default null default 1);")
	tk.MustExec("create block t2 (a int);")
	tk.MustExec("alter block  t2 change defCausumn a a int not null default null default 1;")
}

func (s *testBypassSuite) TestLatch(c *C) {
	causetstore, err := mockstore.NewMockStore(
		// Small latch slot size to make conflicts.
		mockstore.WithTxnLocalLatches(64),
	)
	c.Assert(err, IsNil)
	defer causetstore.Close()

	dom, err1 := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err1, IsNil)
	defer dom.Close()

	tk1 := testkit.NewTestKit(c, causetstore)
	tk1.MustExec("use test")
	tk1.MustExec("drop block if exists t")
	tk1.MustExec("create block t (id int)")
	tk1.MustExec("set @@milevadb_disable_txn_auto_retry = true")

	tk2 := testkit.NewTestKit(c, causetstore)
	tk2.MustExec("use test")
	tk1.MustExec("set @@milevadb_disable_txn_auto_retry = true")

	fn := func() {
		tk1.MustExec("begin")
		for i := 0; i < 100; i++ {
			tk1.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		}
		tk2.MustExec("begin")
		for i := 100; i < 200; i++ {
			tk1.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		}
		tk2.MustExec("commit")
	}

	// txn1 and txn2 data range do not overlap, using latches should not
	// result in txn conflict.
	fn()
	tk1.MustExec("commit")

	tk1.MustExec("truncate block t")
	fn()
	tk1.MustExec("commit")

	// Test the error type of latch and it could be retry if MilevaDB enable the retry.
	tk1.MustExec("begin")
	tk1.MustExec("uFIDelate t set id = id + 1")
	tk2.MustExec("uFIDelate t set id = id + 1")
	_, err = tk1.Exec("commit")
	c.Assert(ekv.ErrWriteConflictInMilevaDB.Equal(err), IsTrue)

	tk1.MustExec("set @@milevadb_disable_txn_auto_retry = 0")
	tk1.MustExec("uFIDelate t set id = id + 1")
	tk2.MustExec("uFIDelate t set id = id + 1")
	tk1.MustExec("commit")
}

// TestIssue4067 Test issue https://github.com/whtcorpsinc/milevadb/issues/4067
func (s *testSuite7) TestIssue4067(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(id int)")
	tk.MustExec("create block t2(id int)")
	tk.MustExec("insert into t1 values(123)")
	tk.MustExec("insert into t2 values(123)")
	tk.MustExec("delete from t1 where id not in (select id from t2)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("123"))
	tk.MustExec("delete from t1 where id in (select id from t2)")
	tk.MustQuery("select * from t1").Check(nil)
}

func (s *testSuite7) TestInsertCalculatedValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int)")
	tk.MustExec("insert into t set a=1, b=a+1")
	tk.MustQuery("select a, b from t").Check(testkit.Rows("1 2"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int default 100, b int)")
	tk.MustExec("insert into t set b=a+1, a=1")
	tk.MustQuery("select a, b from t").Check(testkit.Rows("1 101"))
	tk.MustExec("insert into t (b) value (a)")
	tk.MustQuery("select * from t where b = 100").Check(testkit.Rows("100 100"))
	tk.MustExec("insert into t set a=2, b=a+1")
	tk.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 3"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (c int)")
	tk.MustExec("insert into test.t set test.t.c = '1'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int default 1)")
	tk.MustExec("insert into t values (a)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b int, c int, d int)")
	tk.MustExec("insert into t value (1, 2, a+1, b+1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2 2 3"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int not null)")
	tk.MustExec("insert into t values (a+2)")
	tk.MustExec("insert into t values (a)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("0", "2"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a bigint not null, b bigint not null)")
	tk.MustExec("insert into t value(b + 1, a)")
	tk.MustExec("insert into t set a = b + a, b = a + 1")
	tk.MustExec("insert into t value(1000, a)")
	tk.MustExec("insert t set b = sqrt(a + 4), a = 10")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("0 1", "1 1", "10 2", "1000 1000"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(a)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil>"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a enum('a', 'b'))")
	tk.MustExec("insert into t values(a)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil>"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a enum('a', 'b') default 'a')")
	tk.MustExec("insert into t values(a)")
	tk.MustExec("insert into t values(a+1)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("a", "b"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a blob)")
	tk.MustExec("insert into t values(a)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil>"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a varchar(20) default 'a')")
	tk.MustExec("insert into t values(a)")
	tk.MustExec("insert into t values(upper(a))")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("A", "a"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a varchar(20) not null, b varchar(20))")
	tk.MustExec("insert into t value (a, b)")
	tk.MustQuery("select * from t").Check(testkit.Rows(" <nil>"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int)")
	tk.MustExec("insert into t values(a*b, b*b)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil> <nil>"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a json not null, b int)")
	tk.MustExec("insert into t value (a,a->'$')")
	tk.MustQuery("select * from t").Check(testkit.Rows("null 0"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a json, b int, c int as (a->'$.a'))")
	tk.MustExec("insert into t (a, b) value (a, a->'$.a'+1)")
	tk.MustExec("insert into t (b) value (a->'$.a'+1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil> <nil> <nil>", "<nil> <nil> <nil>"))
	tk.MustExec(`insert into t (a, b) value ('{"a": 1}', a->'$.a'+1)`)
	tk.MustQuery("select * from t where c = 1").Check(testkit.Rows(`{"a": 1} 2 1`))
	tk.MustExec("truncate block t")
	tk.MustExec("insert t set b = c + 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec("truncate block t")
	tk.MustExec(`insert t set a = '{"a": 1}', b = c`)
	tk.MustQuery("select * from t").Check(testkit.Rows(`{"a": 1} <nil> 1`))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int auto_increment key, b int)")
	tk.MustExec("insert into t (b) value (a)")
	tk.MustExec("insert into t value (a, a+1)")
	tk.MustExec("set ALLEGROSQL_MODE=NO_AUTO_VALUE_ON_ZERO")
	tk.MustExec("insert into t (b) value (a+1)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1 0", "2 1", "3 1"))

	tk.MustExec("set ALLEGROSQL_MODE=STRICT_ALL_TABLES")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int not null, b int, c int as (sqrt(a)))")
	tk.MustExec("insert t set b = a, a = 4")
	tk.MustQuery("select * from t").Check(testkit.Rows("4 0 2"))
}

func (s *testSuite7) TestDataTooLongErrMsg(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a varchar(2));")
	_, err := tk.Exec("insert into t values('123');")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(err.Error(), Equals, "[types:1406]Data too long for defCausumn 'a' at event 1")
	tk.MustExec("insert into t values('12')")
	_, err = tk.Exec("uFIDelate t set a = '123' where a = '12';")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(err.Error(), Equals, "[types:1406]Data too long for defCausumn 'a' at event 1")
}

func (s *testSuite7) TestUFIDelateSelect(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block msg (id varchar(8), b int, status int, primary key (id, b))")
	tk.MustExec("insert msg values ('abc', 1, 1)")
	tk.MustExec("create block detail (id varchar(8), start varchar(8), status int, index idx_start(start))")
	tk.MustExec("insert detail values ('abc', '123', 2)")
	tk.MustExec("UFIDelATE msg SET msg.status = (SELECT detail.status FROM detail WHERE msg.id = detail.id)")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("admin check block msg")
}

func (s *testSuite7) TestUFIDelateDelete(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE ttt (id bigint(20) NOT NULL, host varchar(30) NOT NULL, PRIMARY KEY (id), UNIQUE KEY i_host (host));")
	tk.MustExec("insert into ttt values (8,8),(9,9);")

	tk.MustExec("begin")
	tk.MustExec("uFIDelate ttt set id = 0, host='9' where id = 9 limit 1;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows("8 8"))
	tk.MustExec("uFIDelate ttt set id = 0, host='8' where id = 8 limit 1;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows())
	tk.MustExec("commit")
	tk.MustExec("admin check block ttt;")
	tk.MustExec("drop block ttt")
}

func (s *testSuite7) TestUFIDelateAffectRowCnt(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block a(id int auto_increment, a int default null, primary key(id))")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("uFIDelate a set id = id*10 where a = 1001")
	ctx := tk.Se.(stochastikctx.Context)
	c.Assert(ctx.GetStochastikVars().StmtCtx.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")

	tk.MustExec("drop block a")
	tk.MustExec("create block a ( a bigint, b bigint)")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("uFIDelate a set a = a*10 where b = 1001")
	ctx = tk.Se.(stochastikctx.Context)
	c.Assert(ctx.GetStochastikVars().StmtCtx.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
}

func (s *testSuite7) TestReplaceLog(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`create block testLog (a int not null primary key, b int unique key);`)

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	is := s.petri.SchemaReplicant()
	dbName := perceptron.NewCIStr("test")
	tblName := perceptron.NewCIStr("testLog")
	tbl, err := is.BlockByName(dbName, tblName)
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("b")
	indexOpr := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn.GetUnionStore(), types.MakeCausets(1), ekv.IntHandle(1))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	_, err = tk.Exec(`replace into testLog values (0, 0), (1, 1);`)
	c.Assert(err, NotNil)
	expErr := errors.New(`can not be duplicated event, due to old event not found. handle 1 not found`)
	c.Assert(expErr.Error() == err.Error(), IsTrue, Commentf("obtained error: (%s)\nexpected error: (%s)", err.Error(), expErr.Error()))

	tk.MustQuery(`admin cleanup index testLog b;`).Check(testkit.Rows("1"))
}

// TestRebaseIfNeeded is for issue 7422.
// There is no need to do the rebase when uFIDelating a record if the auto-increment ID not changed.
// This could make the auto ID increasing speed slower.
func (s *testSuite7) TestRebaseIfNeeded(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`create block t (a int not null primary key auto_increment, b int unique key);`)
	tk.MustExec(`insert into t (b) values (1);`)

	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
	tbl, err := s.petri.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(s.ctx.NewTxn(context.Background()), IsNil)
	// AddRecord directly here will skip to rebase the auto ID in the insert statement,
	// which could simulate another MilevaDB adds a large auto ID.
	_, err = tbl.AddRecord(s.ctx, types.MakeCausets(30001, 2))
	c.Assert(err, IsNil)
	txn, err := s.ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)

	tk.MustExec(`uFIDelate t set b = 3 where a = 30001;`)
	tk.MustExec(`insert into t (b) values (4);`)
	tk.MustQuery(`select a from t where b = 4;`).Check(testkit.Rows("2"))

	tk.MustExec(`insert into t set b = 3 on duplicate key uFIDelate a = a;`)
	tk.MustExec(`insert into t (b) values (5);`)
	tk.MustQuery(`select a from t where b = 5;`).Check(testkit.Rows("4"))

	tk.MustExec(`insert into t set b = 3 on duplicate key uFIDelate a = a + 1;`)
	tk.MustExec(`insert into t (b) values (6);`)
	tk.MustQuery(`select a from t where b = 6;`).Check(testkit.Rows("30003"))
}

func (s *testSuite7) TestDeferConstraintCheckForDelete(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("set milevadb_constraint_check_in_place = 0")
	tk.MustExec("set @@milevadb_txn_mode = 'optimistic'")
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t1, t2, t3, t4, t5")
	tk.MustExec("create block t1(i int primary key, j int)")
	tk.MustExec("insert into t1 values(1, 2)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(1, 3)")
	tk.MustExec("delete from t1 where j = 3")
	_, err := tk.Exec("commit")
	c.Assert(err.Error(), Equals, "previous statement: delete from t1 where j = 3: [ekv:1062]Duplicate entry '1' for key 'PRIMARY'")
	tk.MustExec("rollback")

	tk.MustExec("create block t2(i int, j int, unique index idx(i))")
	tk.MustExec("insert into t2 values(1, 2)")
	tk.MustExec("begin")
	tk.MustExec("insert into t2 values(1, 3)")
	tk.MustExec("delete from t2 where j = 3")
	_, err = tk.Exec("commit")
	c.Assert(err.Error(), Equals, "previous statement: delete from t2 where j = 3: [ekv:1062]Duplicate entry '1' for key 'idx'")
	tk.MustExec("admin check block t2")

	tk.MustExec("create block t3(i int, j int, primary key(i))")
	tk.MustExec("begin")
	tk.MustExec("insert into t3 values(1, 3)")
	tk.MustExec("delete from t3 where j = 3")
	tk.MustExec("commit")

	tk.MustExec("create block t4(i int, j int, primary key(i))")
	tk.MustExec("begin")
	tk.MustExec("insert into t4 values(1, 3)")
	tk.MustExec("delete from t4 where j = 3")
	tk.MustExec("insert into t4 values(2, 3)")
	tk.MustExec("commit")
	tk.MustExec("admin check block t4")
	tk.MustQuery("select * from t4").Check(testkit.Rows("2 3"))

	tk.MustExec("create block t5(i int, j int, primary key(i))")
	tk.MustExec("begin")
	tk.MustExec("insert into t5 values(1, 3)")
	tk.MustExec("delete from t5 where j = 3")
	tk.MustExec("insert into t5 values(1, 4)")
	tk.MustExec("commit")
	tk.MustExec("admin check block t5")
	tk.MustQuery("select * from t5").Check(testkit.Rows("1 4"))
}

func (s *testSuite7) TestDeferConstraintCheckForInsert(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test`)

	tk.MustExec(`drop block if exists t;create block t (a int primary key, b int);`)
	tk.MustExec(`insert into t values (1,2),(2,2)`)
	_, err := tk.Exec("uFIDelate t set a=a+1 where b=2")
	c.Assert(err, NotNil)

	tk.MustExec(`drop block if exists t;create block t (i int key);`)
	tk.MustExec(`insert t values (1);`)
	tk.MustExec(`set milevadb_constraint_check_in_place = 1;`)
	tk.MustExec(`begin;`)
	_, err = tk.Exec(`insert t values (1);`)
	c.Assert(err, NotNil)
	tk.MustExec(`uFIDelate t set i = 2 where i = 1;`)
	tk.MustExec(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows("2"))

	tk.MustExec(`set milevadb_constraint_check_in_place = 0;`)
	tk.MustExec("replace into t values (1),(2)")
	tk.MustExec("begin")
	_, err = tk.Exec("uFIDelate t set i = 2 where i = 1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values (1) on duplicate key uFIDelate i = i + 1")
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	tk.MustExec(`drop block t; create block t (id int primary key, v int unique);`)
	tk.MustExec(`insert into t values (1, 1)`)
	tk.MustExec(`set milevadb_constraint_check_in_place = 1;`)
	tk.MustExec(`set @@autocommit = 0;`)

	_, err = tk.Exec("insert into t values (3, 1)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values (1, 3)")
	c.Assert(err, NotNil)
	tk.MustExec("commit")

	tk.MustExec(`set milevadb_constraint_check_in_place = 0;`)
	tk.MustExec("insert into t values (3, 1)")
	tk.MustExec("insert into t values (1, 3)")
	_, err = tk.Exec("commit")
	c.Assert(err, NotNil)
}

func (s *testSuite7) TestPessimisticDeleteYourWrites(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustExec("drop block if exists x;")
	stochastik1.MustExec("create block x (id int primary key, c int);")

	stochastik1.MustExec("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustExec("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustExec("begin;")
	stochastik1.MustExec("insert into x select 1, 1")
	stochastik1.MustExec("delete from x where id = 1")
	stochastik2.MustExec("begin;")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		stochastik2.MustExec("insert into x select 1, 2")
		wg.Done()
	}()
	stochastik1.MustExec("commit;")
	wg.Wait()
	stochastik2.MustExec("commit;")
	stochastik2.MustQuery("select * from x").Check(testkit.Rows("1 2"))
}

func (s *testSuite7) TestDefEnumInsert(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block test (id int, prescription_type enum('a','b','c','d','e','f') NOT NULL, primary key(id));")
	tk.MustExec("insert into test (id)  values (1)")
	tk.MustQuery("select prescription_type from test").Check(testkit.Rows("a"))
}

func (s *testSuite7) TestIssue11059(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("create block t (pk int primary key, uk int unique, v int)")
	tk.MustExec("insert into t values (2, 11, 215)")
	tk.MustExec("insert into t values (3, 7, 2111)")
	_, err := tk.Exec("uFIDelate t set pk = 2 where uk = 7")
	c.Assert(err, NotNil)
}

func (s *testSuite7) TestSetWithRefGenDefCaus(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`create block t (i int, j int as (i+1) not null);`)
	tk.MustExec(`insert into t set i = j + 1;`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec(`insert into t set i = j + 100;`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "100 101"))

	tk.MustExec(`create block te (i int)`)
	tk.MustExec(`insert into te set i = i + 10;`)
	tk.MustQuery("select * from te").Check(testkit.Rows("<nil>"))
	tk.MustExec(`insert into te set i = i;`)
	tk.MustQuery("select * from te").Check(testkit.Rows("<nil>", "<nil>"))

	tk.MustExec(`create block tn (i int not null)`)
	tk.MustExec(`insert into tn set i = i;`)
	tk.MustQuery("select * from tn").Check(testkit.Rows("0"))
	tk.MustExec(`insert into tn set i = i + 10;`)
	tk.MustQuery("select * from tn").Check(testkit.Rows("0", "10"))

	//
	tk.MustExec(`create block t1 (j int(11) GENERATED ALWAYS AS (i + 1) stored, i int(11) DEFAULT '10');`)
	tk.MustExec(`insert into t1 values()`)
	tk.MustQuery("select * from t1").Check(testkit.Rows("11 10"))
	tk.MustExec(`insert into t1 values()`)
	tk.MustQuery("select * from t1").Check(testkit.Rows("11 10", "11 10"))

	tk.MustExec(`create block t2 (j int(11) GENERATED ALWAYS AS (i + 1) stored not null, i int(11) DEFAULT '5');`)
	tk.MustExec(`insert into t2 set i = j + 9`)
	tk.MustQuery("select * from t2").Check(testkit.Rows("10 9"))
	_, err := tk.Exec(`insert into t2 set j = i + 1`)
	c.Assert(err, NotNil)
	tk.MustExec(`insert into t2 set i = j + 100`)
	tk.MustQuery("select * from t2").Check(testkit.Rows("10 9", "101 100"))

	tk.MustExec(`create block t3(j int(11) GENERATED ALWAYS AS (i + 1) stored, i int(11) DEFAULT '5');`)
	tk.MustExec(`insert into t3 set i = j + 100`)
	tk.MustQuery("select * from t3").Check(testkit.Rows("<nil> <nil>"))
	_, err = tk.Exec(`insert into t3 set j = i + 1`)
	c.Assert(err, NotNil)
}

func (s *testSuite7) TestSetWithCurrentTimestampAndNow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists tbl;`)
	tk.MustExec(`create block t1(c1 timestamp default current_timestamp, c2 int, c3 timestamp default current_timestamp);`)
	//c1 insert using now() function result, c3 using default value calculation, should be same
	tk.MustExec(`insert into t1 set c1 = current_timestamp, c2 = sleep(2);`)
	tk.MustQuery("select c1 = c3 from t1").Check(testkit.Rows("1"))
	tk.MustExec(`insert into t1 set c1 = current_timestamp, c2 = sleep(1);`)
	tk.MustQuery("select c1 = c3 from t1").Check(testkit.Rows("1", "1"))
}

func (s *testSuite7) TestApplyWithPointAndBatchPointGet(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t ( c_int int, c_str varchar(40),c_datetime datetime, c_timestamp timestamp,
c_double double, c_decimal decimal(12, 6) , primary key(c_int, c_str) , unique key(c_int) , unique key(c_str) ,
unique key(c_decimal) , unique key(c_datetime) , key(c_timestamp) );`)
	tk.MustExec(`insert into t values (1, 'zen ardinghelli', '2020-02-03 18:15:17', '2020-03-11 05:47:11', 36.226534, 3.763),
(2, 'suspicious joliot', '2020-01-01 22:56:37', '2020-04-07 06:19:07', 62.756537, 5.567),
(3, 'keen zhukovsky', '2020-01-21 04:09:20', '2020-06-06 08:32:14', 33.115065, 1.381),
(4, 'crazy newton', '2020-02-14 21:37:56', '2020-04-28 08:33:48', 44.146318, 4.249),
(5, 'happy black', '2020-03-12 16:04:14', '2020-01-18 09:17:37', 41.962653, 5.959);`)
	tk.MustExec(`insert into t values (6, 'vigilant swartz', '2020-06-01 07:37:44', '2020-05-25 01:26:43', 56.352233, 2.202),
(7, 'suspicious germain', '2020-04-16 23:25:23', '2020-03-17 05:06:57', 55.897698, 3.460),
(8, 'festive chandrasekhar', '2020-02-11 23:40:29', '2020-04-08 10:13:04', 77.565691, 0.540),
(9, 'vigorous meninsky', '2020-02-17 10:03:17', '2020-01-02 15:02:02', 6.484815, 6.292),
(10, 'heuristic moser', '2020-04-20 12:18:49', '2020-06-20 20:20:18', 28.023822, 2.765);`)
	tk.MustExec(`insert into t values (11, 'sharp carver', '2020-03-01 11:23:41', '2020-03-23 17:59:05', 40.842442, 6.345),
(12, 'trusting noether', '2020-03-28 06:42:34', '2020-01-27 15:33:40', 49.544658, 4.811),
(13, 'objective ishizaka', '2020-01-28 17:30:55', '2020-04-02 17:45:39', 59.523930, 5.015),
(14, 'sad rhodes', '2020-03-30 21:43:37', '2020-06-09 06:53:53', 87.295753, 2.413),
(15, 'wonderful shockley', '2020-04-29 09:17:11', '2020-03-14 04:36:51', 6.778588, 8.497);`)
	tk.MustExec("begin pessimistic")
	tk.MustExec(`insert into t values (13, 'vibrant yalow', '2020-05-15 06:59:05', '2020-05-03 05:58:45', 43.721929, 8.066),
(14, 'xenodochial spence', '2020-02-13 17:28:07', '2020-04-01 12:18:30', 19.981331, 5.774),
(22, 'eloquent neumann', '2020-02-10 16:00:20', '2020-03-28 00:24:42', 10.702532, 7.618)
on duplicate key uFIDelate c_int=values(c_int), c_str=values(c_str), c_double=values(c_double), c_timestamp=values(c_timestamp);`)
	// Test pointGet.
	tk.MustQuery(`select sum((select t1.c_str from t t1 where t1.c_int = 11 and t1.c_str > t.c_str order by t1.c_decimal limit 1) is null) nulls
from t order by c_str;`).Check(testkit.Rows("10"))
	// Test batchPointGet
	tk.MustQuery(`select sum((select t1.c_str from t t1 where t1.c_int in (11, 10086) and t1.c_str > t.c_str order by t1.c_decimal limit 1) is null) nulls
from t order by c_str;`).Check(testkit.Rows("10"))
	tk.MustExec("commit")
	tk.MustQuery(`select sum((select t1.c_str from t t1 where t1.c_int = 11 and t1.c_str > t.c_str order by t1.c_decimal limit 1) is null) nulls
from t order by c_str;`).Check(testkit.Rows("10"))
	// Test batchPointGet
	tk.MustQuery(`select sum((select t1.c_str from t t1 where t1.c_int in (11, 10086) and t1.c_str > t.c_str order by t1.c_decimal limit 1) is null) nulls
from t order by c_str;`).Check(testkit.Rows("10"))
}
