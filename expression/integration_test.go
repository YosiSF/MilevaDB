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

package expression_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/petri"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/kvcache"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testIntegrationSuite{})
var _ = Suite(&testIntegrationSuite2{})
var _ = SerialSuites(&testIntegrationSerialSuite{})

type testIntegrationSuiteBase struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	ctx         stochastikctx.Context
}

type testIntegrationSuite struct {
	testIntegrationSuiteBase
}

type testIntegrationSuite2 struct {
	testIntegrationSuiteBase
}

type testIntegrationSerialSuite struct {
	testIntegrationSuiteBase
}

func (s *testIntegrationSuiteBase) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Events() {
		blockName := tb[0]
		tk.MustExec(fmt.Sprintf("drop block %v", blockName))
	}
}

func (s *testIntegrationSuiteBase) SetUpSuite(c *C) {
	var err error
	s.causetstore, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
}

func (s *testIntegrationSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *testIntegrationSuite) Test19654(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test;")

	// enum vs enum
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (b enum('a', 'b'));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create block t2 (b enum('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Events("a a"))

	// set vs set
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (b set('a', 'b'));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create block t2 (b set('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Events("a a"))

	// enum vs set
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (b enum('a', 'b'));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create block t2 (b set('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Events("a a"))

	// char vs enum
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (b char(10));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create block t2 (b enum('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Events("a a"))

	// char vs set
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("create block t1 (b char(10));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("create block t2 (b set('b','a') not null, unique(b));")
	tk.MustExec("insert into t2 values ('a');")
	tk.MustQuery("select /*+ inl_join(t2)*/ * from t1, t2 where t1.b=t2.b;").Check(testkit.Events("a a"))
}

func (s *testIntegrationSuite) TestFuncREPEAT(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS block_string;")
	tk.MustExec("CREATE TABLE block_string(a CHAR(20), b VARCHAR(20), c TINYTEXT, d TEXT(20), e MEDIUMTEXT, f LONGTEXT, g BIGINT);")
	tk.MustExec("INSERT INTO block_string (a, b, c, d, e, f, g) VALUES ('a', 'b', 'c', 'd', 'e', 'f', 2);")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("SELECT REPEAT(a, g), REPEAT(b, g), REPEAT(c, g), REPEAT(d, g), REPEAT(e, g), REPEAT(f, g) FROM block_string;")
	r.Check(testkit.Events("aa bb cc dd ee ff"))

	r = tk.MustQuery("SELECT REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g) FROM block_string;")
	r.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, NULL), REPEAT(b, NULL), REPEAT(c, NULL), REPEAT(d, NULL), REPEAT(e, NULL), REPEAT(f, NULL) FROM block_string;")
	r.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, 2), REPEAT(b, 2), REPEAT(c, 2), REPEAT(d, 2), REPEAT(e, 2), REPEAT(f, 2) FROM block_string;")
	r.Check(testkit.Events("aa bb cc dd ee ff"))

	r = tk.MustQuery("SELECT REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2) FROM block_string;")
	r.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, -1), REPEAT(b, -2), REPEAT(c, -2), REPEAT(d, -2), REPEAT(e, -2), REPEAT(f, -2) FROM block_string;")
	r.Check(testkit.Events("     "))

	r = tk.MustQuery("SELECT REPEAT(a, 0), REPEAT(b, 0), REPEAT(c, 0), REPEAT(d, 0), REPEAT(e, 0), REPEAT(f, 0) FROM block_string;")
	r.Check(testkit.Events("     "))

	r = tk.MustQuery("SELECT REPEAT(a, 16777217), REPEAT(b, 16777217), REPEAT(c, 16777217), REPEAT(d, 16777217), REPEAT(e, 16777217), REPEAT(f, 16777217) FROM block_string;")
	r.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil> <nil>"))
}

func (s *testIntegrationSuite) TestFuncLpadAndRpad(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustExec(`USE test;`)
	tk.MustExec(`DROP TABLE IF EXISTS t;`)
	tk.MustExec(`CREATE TABLE t(a BINARY(10), b CHAR(10));`)
	tk.MustExec(`INSERT INTO t SELECT "中文", "abc";`)
	result := tk.MustQuery(`SELECT LPAD(a, 11, "a"), LPAD(b, 2, "xx") FROM t;`)
	result.Check(testkit.Events("a中文\x00\x00\x00\x00 ab"))
	result = tk.MustQuery(`SELECT RPAD(a, 11, "a"), RPAD(b, 2, "xx") FROM t;`)
	result.Check(testkit.Events("中文\x00\x00\x00\x00a ab"))
	result = tk.MustQuery(`SELECT LPAD("中文", 5, "字符"), LPAD("中文", 1, "a");`)
	result.Check(testkit.Events("字符字中文 中"))
	result = tk.MustQuery(`SELECT RPAD("中文", 5, "字符"), RPAD("中文", 1, "a");`)
	result.Check(testkit.Events("中文字符字 中"))
	result = tk.MustQuery(`SELECT RPAD("中文", -5, "字符"), RPAD("中文", 10, "");`)
	result.Check(testkit.Events("<nil> <nil>"))
	result = tk.MustQuery(`SELECT LPAD("中文", -5, "字符"), LPAD("中文", 10, "");`)
	result.Check(testkit.Events("<nil> <nil>"))
}

func (s *testIntegrationSuite) TestMiscellaneousBuiltin(c *C) {
	ctx := context.Background()
	defer s.cleanEnv(c)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	// for uuid
	r := tk.MustQuery("select uuid(), uuid(), uuid(), uuid(), uuid(), uuid();")
	for _, it := range r.Events() {
		for _, item := range it {
			uuid, ok := item.(string)
			c.Assert(ok, Equals, true)
			list := strings.Split(uuid, "-")
			c.Assert(len(list), Equals, 5)
			c.Assert(len(list[0]), Equals, 8)
			c.Assert(len(list[1]), Equals, 4)
			c.Assert(len(list[2]), Equals, 4)
			c.Assert(len(list[3]), Equals, 4)
			c.Assert(len(list[4]), Equals, 12)
		}
	}
	tk.MustQuery("select sleep(1);").Check(testkit.Events("0"))
	tk.MustQuery("select sleep(0);").Check(testkit.Events("0"))
	tk.MustQuery("select sleep('a');").Check(testkit.Events("0"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect FLOAT value: 'a'"))
	rs, err := tk.Exec("select sleep(-1);")
	c.Assert(err, IsNil)
	c.Assert(rs, NotNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)

	tk.MustQuery("SELECT INET_ATON('10.0.5.9');").Check(testkit.Events("167773449"))
	tk.MustQuery("SELECT INET_NTOA(167773449);").Check(testkit.Events("10.0.5.9"))
	tk.MustQuery("SELECT HEX(INET6_ATON('fdfe::5a55:caff:fefa:9089'));").Check(testkit.Events("FDFE0000000000005A55CAFFFEFA9089"))
	tk.MustQuery("SELECT HEX(INET6_ATON('10.0.5.9'));").Check(testkit.Events("0A000509"))
	tk.MustQuery("SELECT INET6_NTOA(INET6_ATON('fdfe::5a55:caff:fefa:9089'));").Check(testkit.Events("fdfe::5a55:caff:fefa:9089"))
	tk.MustQuery("SELECT INET6_NTOA(INET6_ATON('10.0.5.9'));").Check(testkit.Events("10.0.5.9"))
	tk.MustQuery("SELECT INET6_NTOA(UNHEX('FDFE0000000000005A55CAFFFEFA9089'));").Check(testkit.Events("fdfe::5a55:caff:fefa:9089"))
	tk.MustQuery("SELECT INET6_NTOA(UNHEX('0A000509'));").Check(testkit.Events("10.0.5.9"))

	tk.MustQuery(`SELECT IS_IPV4('10.0.5.9'), IS_IPV4('10.0.5.256');`).Check(testkit.Events("1 0"))
	tk.MustQuery(`SELECT IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'));`).Check(testkit.Events("1"))
	tk.MustQuery(`SELECT IS_IPV4_COMPAT(INET6_ATON('::ffff:10.0.5.9'));`).Check(testkit.Events("0"))
	tk.MustQuery(`SELECT
	  IS_IPV4_COMPAT(INET6_ATON('::192.168.0.1')),
	  IS_IPV4_COMPAT(INET6_ATON('::c0a8:0001')),
	  IS_IPV4_COMPAT(INET6_ATON('::c0a8:1'));`).Check(testkit.Events("1 1 1"))
	tk.MustQuery(`SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'));`).Check(testkit.Events("0"))
	tk.MustQuery(`SELECT IS_IPV4_MAPPED(INET6_ATON('::ffff:10.0.5.9'));`).Check(testkit.Events("1"))
	tk.MustQuery(`SELECT
	  IS_IPV4_MAPPED(INET6_ATON('::ffff:192.168.0.1')),
	  IS_IPV4_MAPPED(INET6_ATON('::ffff:c0a8:0001')),
	  IS_IPV4_MAPPED(INET6_ATON('::ffff:c0a8:1'));`).Check(testkit.Events("1 1 1"))
	tk.MustQuery(`SELECT IS_IPV6('10.0.5.9'), IS_IPV6('::1');`).Check(testkit.Events("0 1"))

	tk.MustExec("drop block if exists t1;")
	tk.MustExec(`create block t1(
        a int,
        b int not null,
        c int not null default 0,
        d int default 0,
        unique key(b,c),
        unique key(b,d)
);`)
	tk.MustExec("insert into t1 (a,b) values(1,10),(1,20),(2,30),(2,40);")
	tk.MustQuery("select any_value(a), sum(b) from t1;").Check(testkit.Events("1 100"))
	tk.MustQuery("select a,any_value(b),sum(c) from t1 group by a order by a;").Check(testkit.Events("1 10 0", "2 30 0"))

	// for locks
	tk.MustExec(`set milevadb_enable_noop_functions=1;`)
	result := tk.MustQuery(`SELECT GET_LOCK('test_lock1', 10);`)
	result.Check(testkit.Events("1"))
	result = tk.MustQuery(`SELECT GET_LOCK('test_lock2', 10);`)
	result.Check(testkit.Events("1"))

	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock2');`)
	result.Check(testkit.Events("1"))
	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock1');`)
	result.Check(testkit.Events("1"))
}

func (s *testIntegrationSuite) TestConvertToBit(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t, t1")
	tk.MustExec("create block t (a bit(64))")
	tk.MustExec("create block t1 (a varchar(2))")
	tk.MustExec(`insert t1 value ('10')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Events("12592"))

	tk.MustExec("drop block if exists t, t1")
	tk.MustExec("create block t (a bit(64))")
	tk.MustExec("create block t1 (a binary(2))")
	tk.MustExec(`insert t1 value ('10')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Events("12592"))

	tk.MustExec("drop block if exists t, t1")
	tk.MustExec("create block t (a bit(64))")
	tk.MustExec("create block t1 (a datetime)")
	tk.MustExec(`insert t1 value ('09-01-01')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Events("20090101000000"))
}

func (s *testIntegrationSuite2) TestMathBuiltin(c *C) {
	ctx := context.Background()
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	// for degrees
	result := tk.MustQuery("select degrees(0), degrees(1)")
	result.Check(testkit.Events("0 57.29577951308232"))
	result = tk.MustQuery("select degrees(2), degrees(5)")
	result.Check(testkit.Events("114.59155902616465 286.4788975654116"))

	// for sin
	result = tk.MustQuery("select sin(0), sin(1.5707963267949)")
	result.Check(testkit.Events("0 1"))
	result = tk.MustQuery("select sin(1), sin(100)")
	result.Check(testkit.Events("0.8414709848078965 -0.5063656411097588"))
	result = tk.MustQuery("select sin('abcd')")
	result.Check(testkit.Events("0"))

	// for cos
	result = tk.MustQuery("select cos(0), cos(3.1415926535898)")
	result.Check(testkit.Events("1 -1"))
	result = tk.MustQuery("select cos('abcd')")
	result.Check(testkit.Events("1"))

	// for tan
	result = tk.MustQuery("select tan(0.00), tan(PI()/4)")
	result.Check(testkit.Events("0 1"))
	result = tk.MustQuery("select tan('abcd')")
	result.Check(testkit.Events("0"))

	// for log2
	result = tk.MustQuery("select log2(0.0)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select log2(4)")
	result.Check(testkit.Events("2"))
	result = tk.MustQuery("select log2('8.0abcd')")
	result.Check(testkit.Events("3"))
	result = tk.MustQuery("select log2(-1)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select log2(NULL)")
	result.Check(testkit.Events("<nil>"))

	// for log10
	result = tk.MustQuery("select log10(0.0)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select log10(100)")
	result.Check(testkit.Events("2"))
	result = tk.MustQuery("select log10('1000.0abcd')")
	result.Check(testkit.Events("3"))
	result = tk.MustQuery("select log10(-1)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select log10(NULL)")
	result.Check(testkit.Events("<nil>"))

	//for log
	result = tk.MustQuery("select log(0.0)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select log(100)")
	result.Check(testkit.Events("4.605170185988092"))
	result = tk.MustQuery("select log('100.0abcd')")
	result.Check(testkit.Events("4.605170185988092"))
	result = tk.MustQuery("select log(-1)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select log(NULL)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select log(NULL, NULL)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select log(1, 100)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select log(0.5, 0.25)")
	result.Check(testkit.Events("2"))
	result = tk.MustQuery("select log(-1, 0.25)")
	result.Check(testkit.Events("<nil>"))

	// for atan
	result = tk.MustQuery("select atan(0), atan(-1), atan(1), atan(1,2)")
	result.Check(testkit.Events("0 -0.7853981633974483 0.7853981633974483 0.4636476090008061"))
	result = tk.MustQuery("select atan('milevadb')")
	result.Check(testkit.Events("0"))

	// for asin
	result = tk.MustQuery("select asin(0), asin(-2), asin(2), asin(1)")
	result.Check(testkit.Events("0 <nil> <nil> 1.5707963267948966"))
	result = tk.MustQuery("select asin('milevadb')")
	result.Check(testkit.Events("0"))

	// for acos
	result = tk.MustQuery("select acos(0), acos(-2), acos(2), acos(1)")
	result.Check(testkit.Events("1.5707963267948966 <nil> <nil> 0"))
	result = tk.MustQuery("select acos('milevadb')")
	result.Check(testkit.Events("1.5707963267948966"))

	// for pi
	result = tk.MustQuery("select pi()")
	result.Check(testkit.Events("3.141592653589793"))

	// for floor
	result = tk.MustQuery("select floor(0), floor(null), floor(1.23), floor(-1.23), floor(1)")
	result.Check(testkit.Events("0 <nil> 1 -2 1"))
	result = tk.MustQuery("select floor('milevadb'), floor('1milevadb'), floor('milevadb1')")
	result.Check(testkit.Events("0 1 0"))
	result = tk.MustQuery("SELECT floor(t.c_datetime) FROM (select CAST('2020-07-19 00:00:00' AS DATETIME) AS c_datetime) AS t")
	result.Check(testkit.Events("20170719000000"))
	result = tk.MustQuery("SELECT floor(t.c_time) FROM (select CAST('12:34:56' AS TIME) AS c_time) AS t")
	result.Check(testkit.Events("123456"))
	result = tk.MustQuery("SELECT floor(t.c_time) FROM (select CAST('00:34:00' AS TIME) AS c_time) AS t")
	result.Check(testkit.Events("3400"))
	result = tk.MustQuery("SELECT floor(t.c_time) FROM (select CAST('00:00:00' AS TIME) AS c_time) AS t")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT floor(t.c_decimal) FROM (SELECT CAST('-10.01' AS DECIMAL(10,2)) AS c_decimal) AS t")
	result.Check(testkit.Events("-11"))
	result = tk.MustQuery("SELECT floor(t.c_decimal) FROM (SELECT CAST('-10.01' AS DECIMAL(10,1)) AS c_decimal) AS t")
	result.Check(testkit.Events("-10"))

	// for ceil/ceiling
	result = tk.MustQuery("select ceil(0), ceil(null), ceil(1.23), ceil(-1.23), ceil(1)")
	result.Check(testkit.Events("0 <nil> 2 -1 1"))
	result = tk.MustQuery("select ceiling(0), ceiling(null), ceiling(1.23), ceiling(-1.23), ceiling(1)")
	result.Check(testkit.Events("0 <nil> 2 -1 1"))
	result = tk.MustQuery("select ceil('milevadb'), ceil('1milevadb'), ceil('milevadb1'), ceiling('milevadb'), ceiling('1milevadb'), ceiling('milevadb1')")
	result.Check(testkit.Events("0 1 0 0 1 0"))
	result = tk.MustQuery("select ceil(t.c_datetime), ceiling(t.c_datetime) from (select cast('2020-07-20 00:00:00' as datetime) as c_datetime) as t")
	result.Check(testkit.Events("20170720000000 20170720000000"))
	result = tk.MustQuery("select ceil(t.c_time), ceiling(t.c_time) from (select cast('12:34:56' as time) as c_time) as t")
	result.Check(testkit.Events("123456 123456"))
	result = tk.MustQuery("select ceil(t.c_time), ceiling(t.c_time) from (select cast('00:34:00' as time) as c_time) as t")
	result.Check(testkit.Events("3400 3400"))
	result = tk.MustQuery("select ceil(t.c_time), ceiling(t.c_time) from (select cast('00:00:00' as time) as c_time) as t")
	result.Check(testkit.Events("0 0"))
	result = tk.MustQuery("select ceil(t.c_decimal), ceiling(t.c_decimal) from (select cast('-10.01' as decimal(10,2)) as c_decimal) as t")
	result.Check(testkit.Events("-10 -10"))
	result = tk.MustQuery("select ceil(t.c_decimal), ceiling(t.c_decimal) from (select cast('-10.01' as decimal(10,1)) as c_decimal) as t")
	result.Check(testkit.Events("-10 -10"))
	result = tk.MustQuery("select floor(18446744073709551615), ceil(18446744073709551615)")
	result.Check(testkit.Events("18446744073709551615 18446744073709551615"))
	result = tk.MustQuery("select floor(18446744073709551615.1233), ceil(18446744073709551615.1233)")
	result.Check(testkit.Events("18446744073709551615 18446744073709551616"))
	result = tk.MustQuery("select floor(-18446744073709551617), ceil(-18446744073709551617), floor(-18446744073709551617.11), ceil(-18446744073709551617.11)")
	result.Check(testkit.Events("-18446744073709551617 -18446744073709551617 -18446744073709551618 -18446744073709551617"))
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a decimal(40,20) UNSIGNED);")
	tk.MustExec("insert into t values(2.99999999900000000000), (12), (0);")
	tk.MustQuery("select a, ceil(a) from t where ceil(a) > 1;").Check(testkit.Events("2.99999999900000000000 3", "12.00000000000000000000 12"))
	tk.MustQuery("select a, ceil(a) from t;").Check(testkit.Events("2.99999999900000000000 3", "12.00000000000000000000 12", "0.00000000000000000000 0"))
	tk.MustQuery("select ceil(-29464);").Check(testkit.Events("-29464"))
	tk.MustQuery("select a, floor(a) from t where floor(a) > 1;").Check(testkit.Events("2.99999999900000000000 2", "12.00000000000000000000 12"))
	tk.MustQuery("select a, floor(a) from t;").Check(testkit.Events("2.99999999900000000000 2", "12.00000000000000000000 12", "0.00000000000000000000 0"))
	tk.MustQuery("select floor(-29464);").Check(testkit.Events("-29464"))

	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a decimal(40,20), b bigint);`)
	tk.MustExec(`insert into t values(-2.99999990000000000000, -1);`)
	tk.MustQuery(`select floor(a), floor(a), floor(a) from t;`).Check(testkit.Events(`-3 -3 -3`))
	tk.MustQuery(`select b, floor(b) from t;`).Check(testkit.Events(`-1 -1`))

	// for cot
	result = tk.MustQuery("select cot(1), cot(-1), cot(NULL)")
	result.Check(testkit.Events("0.6420926159343308 -0.6420926159343308 <nil>"))
	result = tk.MustQuery("select cot('1milevadb')")
	result.Check(testkit.Events("0.6420926159343308"))
	rs, err := tk.Exec("select cot(0)")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrDataOutOfRange))
	c.Assert(rs.Close(), IsNil)

	//for exp
	result = tk.MustQuery("select exp(0), exp(1), exp(-1), exp(1.2), exp(NULL)")
	result.Check(testkit.Events("1 2.718281828459045 0.36787944117144233 3.3201169227365472 <nil>"))
	result = tk.MustQuery("select exp('milevadb'), exp('1milevadb')")
	result.Check(testkit.Events("1 2.718281828459045"))
	rs, err = tk.Exec("select exp(1000000)")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrDataOutOfRange))
	c.Assert(rs.Close(), IsNil)
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a float)")
	tk.MustExec("insert into t values(1000000)")
	rs, err = tk.Exec("select exp(a) from t")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrDataOutOfRange))
	c.Assert(err.Error(), Equals, "[types:1690]DOUBLE value is out of range in 'exp(test.t.a)'")
	c.Assert(rs.Close(), IsNil)

	// for conv
	result = tk.MustQuery("SELECT CONV('a', 16, 2);")
	result.Check(testkit.Events("1010"))
	result = tk.MustQuery("SELECT CONV('6E', 18, 8);")
	result.Check(testkit.Events("172"))
	result = tk.MustQuery("SELECT CONV(-17, 10, -18);")
	result.Check(testkit.Events("-H"))
	result = tk.MustQuery("SELECT CONV(10+'10'+'10'+X'0a', 10, 10);")
	result.Check(testkit.Events("40"))
	result = tk.MustQuery("SELECT CONV('a', 1, 10);")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("SELECT CONV('a', 37, 10);")
	result.Check(testkit.Events("<nil>"))

	// for abs
	result = tk.MustQuery("SELECT ABS(-1);")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("SELECT ABS('abc');")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT ABS(18446744073709551615);")
	result.Check(testkit.Events("18446744073709551615"))
	result = tk.MustQuery("SELECT ABS(123.4);")
	result.Check(testkit.Events("123.4"))
	result = tk.MustQuery("SELECT ABS(-123.4);")
	result.Check(testkit.Events("123.4"))
	result = tk.MustQuery("SELECT ABS(1234E-1);")
	result.Check(testkit.Events("123.4"))
	result = tk.MustQuery("SELECT ABS(-9223372036854775807);")
	result.Check(testkit.Events("9223372036854775807"))
	result = tk.MustQuery("SELECT ABS(NULL);")
	result.Check(testkit.Events("<nil>"))
	rs, err = tk.Exec("SELECT ABS(-9223372036854775808);")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrDataOutOfRange))
	c.Assert(rs.Close(), IsNil)

	// for round
	result = tk.MustQuery("SELECT ROUND(2.5), ROUND(-2.5), ROUND(25E-1);")
	result.Check(testkit.Events("3 -3 3")) // TODO: Should be 3 -3 2
	result = tk.MustQuery("SELECT ROUND(2.5, NULL), ROUND(NULL, 4), ROUND(NULL, NULL), ROUND(NULL);")
	result.Check(testkit.Events("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT ROUND('123.4'), ROUND('123e-2');")
	result.Check(testkit.Events("123 1"))
	result = tk.MustQuery("SELECT ROUND(-9223372036854775808);")
	result.Check(testkit.Events("-9223372036854775808"))
	result = tk.MustQuery("SELECT ROUND(123.456, 0), ROUND(123.456, 1), ROUND(123.456, 2), ROUND(123.456, 3), ROUND(123.456, 4), ROUND(123.456, -1), ROUND(123.456, -2), ROUND(123.456, -3), ROUND(123.456, -4);")
	result.Check(testkit.Events("123 123.5 123.46 123.456 123.4560 120 100 0 0"))
	result = tk.MustQuery("SELECT ROUND(123456E-3, 0), ROUND(123456E-3, 1), ROUND(123456E-3, 2), ROUND(123456E-3, 3), ROUND(123456E-3, 4), ROUND(123456E-3, -1), ROUND(123456E-3, -2), ROUND(123456E-3, -3), ROUND(123456E-3, -4);")
	result.Check(testkit.Events("123 123.5 123.46 123.456 123.456 120 100 0 0")) // TODO: DeferredCauset 5 should be 123.4560

	// for truncate
	result = tk.MustQuery("SELECT truncate(123, -2), truncate(123, 2), truncate(123, 1), truncate(123, -1);")
	result.Check(testkit.Events("100 123 123 120"))
	result = tk.MustQuery("SELECT truncate(123.456, -2), truncate(123.456, 2), truncate(123.456, 1), truncate(123.456, 3), truncate(1.23, 100), truncate(123456E-3, 2);")
	result.Check(testkit.Events("100 123.45 123.4 123.456 1.230000000000000000000000000000 123.45"))
	result = tk.MustQuery("SELECT truncate(9223372036854775807, -7), truncate(9223372036854775808, -10), truncate(cast(-1 as unsigned), -10);")
	result.Check(testkit.Events("9223372036850000000 9223372030000000000 18446744070000000000"))
	// issue 17181,19390
	tk.MustQuery("select truncate(42, -9223372036854775808);").Check(testkit.Events("0"))
	tk.MustQuery("select truncate(42, 9223372036854775808);").Check(testkit.Events("42"))
	tk.MustQuery("select truncate(42, -2147483648);").Check(testkit.Events("0"))
	tk.MustQuery("select truncate(42, 2147483648);").Check(testkit.Events("42"))
	tk.MustQuery("select truncate(42, 18446744073709551615);").Check(testkit.Events("42"))
	tk.MustQuery("select truncate(42, 4294967295);").Check(testkit.Events("42"))
	tk.MustQuery("select truncate(42, -0);").Check(testkit.Events("42"))
	tk.MustQuery("select truncate(42, -307);").Check(testkit.Events("0"))
	tk.MustQuery("select truncate(42, -308);").Check(testkit.Events("0"))
	tk.MustQuery("select truncate(42, -309);").Check(testkit.Events("0"))
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec("create block t (a bigint unsigned);")
	tk.MustExec("insert into t values (18446744073709551615), (4294967295), (9223372036854775808), (2147483648);")
	tk.MustQuery("select truncate(42, a) from t;").Check(testkit.Events("42", "42", "42", "42"))

	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a date, b datetime, c timestamp, d varchar(20));`)
	tk.MustExec(`insert into t select "1234-12-29", "1234-12-29 16:24:13.9912", "2020-12-29 16:19:28", "12.34567";`)

	// NOTE: the actually result is: 12341220 12341229.0 12341200 12341229.00,
	// but Causet.ToString() don't format decimal length for float numbers.
	result = tk.MustQuery(`select truncate(a, -1), truncate(a, 1), truncate(a, -2), truncate(a, 2) from t;`)
	result.Check(testkit.Events("12341220 12341229 12341200 12341229"))

	// NOTE: the actually result is: 12341229162410 12341229162414.0 12341229162400 12341229162414.00,
	// but Causet.ToString() don't format decimal length for float numbers.
	result = tk.MustQuery(`select truncate(b, -1), truncate(b, 1), truncate(b, -2), truncate(b, 2) from t;`)
	result.Check(testkit.Events("12341229162410 12341229162414 12341229162400 12341229162414"))

	// NOTE: the actually result is: 20141229161920 20141229161928.0 20141229161900 20141229161928.00,
	// but Causet.ToString() don't format decimal length for float numbers.
	result = tk.MustQuery(`select truncate(c, -1), truncate(c, 1), truncate(c, -2), truncate(c, 2) from t;`)
	result.Check(testkit.Events("20141229161920 20141229161928 20141229161900 20141229161928"))

	result = tk.MustQuery(`select truncate(d, -1), truncate(d, 1), truncate(d, -2), truncate(d, 2) from t;`)
	result.Check(testkit.Events("10 12.3 0 12.34"))

	result = tk.MustQuery(`select truncate(json_array(), 1), truncate("cascasc", 1);`)
	result.Check(testkit.Events("0 0"))

	// for pow
	result = tk.MustQuery("SELECT POW('12', 2), POW(1.2e1, '2.0'), POW(12, 2.0);")
	result.Check(testkit.Events("144 144 144"))
	result = tk.MustQuery("SELECT POW(null, 2), POW(2, null), POW(null, null);")
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT POW(0, 0);")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("SELECT POW(0, 0.1), POW(0, 0.5), POW(0, 1);")
	result.Check(testkit.Events("0 0 0"))
	rs, err = tk.Exec("SELECT POW(0, -1);")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrDataOutOfRange))
	c.Assert(rs.Close(), IsNil)

	// for sign
	result = tk.MustQuery("SELECT SIGN('12'), SIGN(1.2e1), SIGN(12), SIGN(0.0000012);")
	result.Check(testkit.Events("1 1 1 1"))
	result = tk.MustQuery("SELECT SIGN('-12'), SIGN(-1.2e1), SIGN(-12), SIGN(-0.0000012);")
	result.Check(testkit.Events("-1 -1 -1 -1"))
	result = tk.MustQuery("SELECT SIGN('0'), SIGN('-0'), SIGN(0);")
	result.Check(testkit.Events("0 0 0"))
	result = tk.MustQuery("SELECT SIGN(NULL);")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("SELECT SIGN(-9223372036854775808), SIGN(9223372036854775808);")
	result.Check(testkit.Events("-1 1"))

	// for sqrt
	result = tk.MustQuery("SELECT SQRT(-10), SQRT(144), SQRT(4.84), SQRT(0.04), SQRT(0);")
	result.Check(testkit.Events("<nil> 12 2.2 0.2 0"))

	// for crc32
	result = tk.MustQuery("SELECT crc32(0), crc32(-0), crc32('0'), crc32('abc'), crc32('ABC'), crc32(NULL), crc32(''), crc32('hello world!')")
	result.Check(testkit.Events("4108050209 4108050209 4108050209 891568578 2743272264 <nil> 0 62177901"))

	// for radians
	result = tk.MustQuery("SELECT radians(1.0), radians(pi()), radians(pi()/2), radians(180), radians(1.009);")
	result.Check(testkit.Events("0.017453292519943295 0.05483113556160754 0.02741556778080377 3.141592653589793 0.01761037215262278"))

	// for rand
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(1),(2),(3)")
	tk.Se.GetStochastikVars().MaxChunkSize = 1
	tk.MustQuery("select rand(1) from t").Sort().Check(testkit.Events("0.1418603212962489", "0.40540353712197724", "0.8716141803857071"))
	tk.MustQuery("select rand(a) from t").Check(testkit.Events("0.40540353712197724", "0.6555866465490187", "0.9057697559760601"))
	tk.MustQuery("select rand(1), rand(2), rand(3)").Check(testkit.Events("0.40540353712197724 0.6555866465490187 0.9057697559760601"))
}

func (s *testIntegrationSuite2) TestStringBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	ctx := context.Background()
	var err error

	// for length
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2020-01-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)
	result := tk.MustQuery("select length(a), length(b), length(c), length(d), length(e), length(f), length(null) from t")
	result.Check(testkit.Events("1 3 19 8 6 2 <nil>"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(20))")
	tk.MustExec(`insert into t values("milevadb  "), (concat("a  ", "b  "))`)
	result = tk.MustQuery("select a, length(a) from t")
	result.Check(testkit.Events("milevadb 4", "a  b 4"))

	// for concat
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b double, c datetime, d time, e char(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2020-01-01 12:01:01", "12:01:01", "abcdef")`)
	result = tk.MustQuery("select concat(a, b, c, d, e) from t")
	result.Check(testkit.Events("11.12017-01-01 12:01:0112:01:01abcdef"))
	result = tk.MustQuery("select concat(null)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select concat(null, a, b) from t")
	result.Check(testkit.Events("<nil>"))
	tk.MustExec("drop block if exists t")
	// Fix issue 9123
	tk.MustExec("create block t(a char(32) not null, b float default '0') engine=innodb default charset=utf8mb4")
	tk.MustExec("insert into t value('0a6f9d012f98467f8e671e9870044528', 208.867)")
	result = tk.MustQuery("select concat_ws( ',', b) from t where a = '0a6f9d012f98467f8e671e9870044528';")
	result.Check(testkit.Events("208.867"))

	// for concat_ws
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b double, c datetime, d time, e char(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2020-01-01 12:01:01", "12:01:01", "abcdef")`)
	result = tk.MustQuery("select concat_ws('|', a, b, c, d, e) from t")
	result.Check(testkit.Events("1|1.1|2020-01-01 12:01:01|12:01:01|abcdef"))
	result = tk.MustQuery("select concat_ws(null, null)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select concat_ws(null, a, b) from t")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select concat_ws(',', 'a', 'b')")
	result.Check(testkit.Events("a,b"))
	result = tk.MustQuery("select concat_ws(',','First name',NULL,'Last Name')")
	result.Check(testkit.Events("First name,Last Name"))

	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a tinyint(2), b varchar(10));`)
	tk.MustExec(`insert into t values (1, 'a'), (12, 'a'), (126, 'a'), (127, 'a')`)
	tk.MustQuery(`select concat_ws('#', a, b) from t;`).Check(testkit.Events(
		`1#a`,
		`12#a`,
		`126#a`,
		`127#a`,
	))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a binary(3))")
	tk.MustExec("insert into t values('a')")
	result = tk.MustQuery(`select concat_ws(',', a, 'test') = 'a\0\0,test' from t`)
	result.Check(testkit.Events("1"))

	// for ascii
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time, f bit(4))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2020-01-01 12:01:01", "12:01:01", 0b1010)`)
	result = tk.MustQuery("select ascii(a), ascii(b), ascii(c), ascii(d), ascii(e), ascii(f) from t")
	result.Check(testkit.Events("50 50 50 50 49 10"))
	result = tk.MustQuery("select ascii('123'), ascii(123), ascii(''), ascii('你好'), ascii(NULL)")
	result.Check(testkit.Events("49 49 0 228 <nil>"))

	// for lower
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b double, c datetime, d time, e char(20), f binary(3), g binary(3))")
	tk.MustExec(`insert into t values(1, 1.1, "2020-01-01 12:01:01", "12:01:01", "abcdef", 'aa', 'BB')`)
	result = tk.MustQuery("select lower(a), lower(b), lower(c), lower(d), lower(e), lower(f), lower(g), lower(null) from t")
	result.Check(testkit.Events("1 1.1 2020-01-01 12:01:01 12:01:01 abcdef aa\x00 BB\x00 <nil>"))

	// for upper
	result = tk.MustQuery("select upper(a), upper(b), upper(c), upper(d), upper(e), upper(f), upper(g), upper(null) from t")
	result.Check(testkit.Events("1 1.1 2020-01-01 12:01:01 12:01:01 ABCDEF aa\x00 BB\x00 <nil>"))

	// for strcmp
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values("123", 123, 12.34, "2020-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select strcmp(a, "123"), strcmp(b, "123"), strcmp(c, "12.34"), strcmp(d, "2020-01-01 12:01:01"), strcmp(e, "12:01:01") from t`)
	result.Check(testkit.Events("0 0 0 0 0"))
	result = tk.MustQuery(`select strcmp("1", "123"), strcmp("123", "1"), strcmp("123", "45"), strcmp("123", null), strcmp(null, "123")`)
	result.Check(testkit.Events("-1 1 -1 <nil> <nil>"))
	result = tk.MustQuery(`select strcmp("", "123"), strcmp("123", ""), strcmp("", ""), strcmp("", null), strcmp(null, "")`)
	result.Check(testkit.Events("-1 1 0 <nil> <nil>"))

	// for left
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('abcde', 1234, 12.34, "2020-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery("select left(a, 2), left(b, 2), left(c, 2), left(d, 2), left(e, 2) from t")
	result.Check(testkit.Events("ab 12 12 20 12"))
	result = tk.MustQuery(`select left("abc", 0), left("abc", -1), left(NULL, 1), left("abc", NULL)`)
	result.Check(testkit.Events("  <nil> <nil>"))
	result = tk.MustQuery(`select left("abc", "a"), left("abc", 1.9), left("abc", 1.2)`)
	result.Check(testkit.Events(" ab a"))
	result = tk.MustQuery(`select left("中文abc", 2), left("中文abc", 3), left("中文abc", 4)`)
	result.Check(testkit.Events("中文 中文a 中文ab"))
	// for right, reuse the block created for left
	result = tk.MustQuery("select right(a, 3), right(b, 3), right(c, 3), right(d, 3), right(e, 3) from t")
	result.Check(testkit.Events("cde 234 .34 :01 :01"))
	result = tk.MustQuery(`select right("abcde", 0), right("abcde", -1), right("abcde", 100), right(NULL, 1), right("abcde", NULL)`)
	result.Check(testkit.Events("  abcde <nil> <nil>"))
	result = tk.MustQuery(`select right("abcde", "a"), right("abcde", 1.9), right("abcde", 1.2)`)
	result.Check(testkit.Events(" de e"))
	result = tk.MustQuery(`select right("中文abc", 2), right("中文abc", 4), right("中文abc", 5)`)
	result.Check(testkit.Events("bc 文abc 中文abc"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a binary(10))")
	tk.MustExec(`insert into t select "中文abc"`)
	result = tk.MustQuery(`select left(a, 3), left(a, 6), left(a, 7) from t`)
	result.Check(testkit.Events("中 中文 中文a"))
	result = tk.MustQuery(`select right(a, 2), right(a, 7) from t`)
	result.Check(testkit.Events("c\x00 文abc\x00"))

	// for ord
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2020-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "milevadb")`)
	result = tk.MustQuery("select ord(a), ord(b), ord(c), ord(d), ord(e), ord(f), ord(g), ord(h), ord(i) from t")
	result.Check(testkit.Events("50 50 50 50 49 10 53 52 116"))
	result = tk.MustQuery("select ord('123'), ord(123), ord(''), ord('你好'), ord(NULL), ord('👍')")
	result.Check(testkit.Events("49 49 0 14990752 <nil> 4036989325"))
	result = tk.MustQuery("select ord(X''), ord(X'6161'), ord(X'e4bd'), ord(X'e4bda0'), ord(_ascii'你'), ord(_latin1'你')")
	result.Check(testkit.Events("0 97 228 228 228 228"))

	// for space
	result = tk.MustQuery(`select space(0), space(2), space(-1), space(1.1), space(1.9)`)
	result.Check(solitonutil.EventsWithSep(",", ",  ,, ,  "))
	result = tk.MustQuery(`select space("abc"), space("2"), space("1.1"), space(''), space(null)`)
	result.Check(solitonutil.EventsWithSep(",", ",  , ,,<nil>"))

	// for replace
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(20), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('www.allegrosql.com', 1234, 12.34, "2020-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select replace(a, 'allegrosql', 'whtcorpsinc'), replace(b, 2, 55), replace(c, 34, 0), replace(d, '-', '/'), replace(e, '01', '22') from t`)
	result.Check(solitonutil.EventsWithSep(",", "www.whtcorpsinc.com,15534,12.0,2020/01/01 12:01:01,12:22:22"))
	result = tk.MustQuery(`select replace('aaa', 'a', ''), replace(null, 'a', 'b'), replace('a', null, 'b'), replace('a', 'b', null)`)
	result.Check(testkit.Events(" <nil> <nil> <nil>"))

	// for tobase64
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b double, c datetime, d time, e char(20), f bit(10), g binary(20), h blob(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2020-01-01 12:01:01", "12:01:01", "abcdef", 0b10101, "512", "abc")`)
	result = tk.MustQuery("select to_base64(a), to_base64(b), to_base64(c), to_base64(d), to_base64(e), to_base64(f), to_base64(g), to_base64(h), to_base64(null) from t")
	result.Check(testkit.Events("MQ== MS4x MjAxNy0wMS0wMSAxMjowMTowMQ== MTI6MDE6MDE= YWJjZGVm ABU= NTEyAAAAAAAAAAAAAAAAAAAAAAA= YWJj <nil>"))

	// for from_base64
	result = tk.MustQuery(`select from_base64("abcd"), from_base64("asc")`)
	result.Check(testkit.Events("i\xb7\x1d <nil>"))
	result = tk.MustQuery(`select from_base64("MQ=="), from_base64(1234)`)
	result.Check(testkit.Events("1 \xd7m\xf8"))

	// for substr
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('Sakila', 12345, 123.45, "2020-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select substr(a, 3), substr(b, 2, 3), substr(c, -3), substr(d, -8), substr(e, -3, 100) from t`)
	result.Check(testkit.Events("kila 234 .45 12:01:01 :01"))
	result = tk.MustQuery(`select substr('Sakila', 100), substr('Sakila', -100), substr('Sakila', -5, 3), substr('Sakila', 2, -1)`)
	result.Check(solitonutil.EventsWithSep(",", ",,aki,"))
	result = tk.MustQuery(`select substr('foobarbar' from 4), substr('Sakila' from -4 for 2)`)
	result.Check(testkit.Events("barbar ki"))
	result = tk.MustQuery(`select substr(null, 2, 3), substr('foo', null, 3), substr('foo', 2, null)`)
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select substr('中文abc', 2), substr('中文abc', 3), substr("中文abc", 1, 2)`)
	result.Check(testkit.Events("文abc abc 中文"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a binary(10))")
	tk.MustExec(`insert into t select "中文abc"`)
	result = tk.MustQuery(`select substr(a, 4), substr(a, 1, 3), substr(a, 1, 6) from t`)
	result.Check(testkit.Events("文abc\x00 中 中文"))
	result = tk.MustQuery(`select substr("string", -1), substr("string", -2), substr("中文", -1), substr("中文", -2) from t`)
	result.Check(testkit.Events("g ng 文 中文"))

	// for bit_length
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b double, c datetime, d time, e char(20), f bit(10), g binary(20), h varbinary(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2020-01-01 12:01:01", "12:01:01", "abcdef", 0b10101, "g", "h")`)
	result = tk.MustQuery("select bit_length(a), bit_length(b), bit_length(c), bit_length(d), bit_length(e), bit_length(f), bit_length(g), bit_length(h), bit_length(null) from t")
	result.Check(testkit.Events("8 24 152 64 48 16 160 8 <nil>"))

	// for substring_index
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(20), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('www.whtcorpsinc.com', 12345, 123.45, "2020-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select substring_index(a, '.', 2), substring_index(b, '.', 2), substring_index(c, '.', -1), substring_index(d, '-', 1), substring_index(e, ':', -2) from t`)
	result.Check(testkit.Events("www.whtcorpsinc 12345 45 2020 01:01"))
	result = tk.MustQuery(`select substring_index('www.whtcorpsinc.com', '.', 0), substring_index('www.whtcorpsinc.com', '.', 100), substring_index('www.whtcorpsinc.com', '.', -100)`)
	result.Check(testkit.Events(" www.whtcorpsinc.com www.whtcorpsinc.com"))
	tk.MustQuery(`select substring_index('xyz', 'abc', 9223372036854775808)`).Check(testkit.Events(``))
	result = tk.MustQuery(`select substring_index('www.whtcorpsinc.com', 'd', 1), substring_index('www.whtcorpsinc.com', '', 1), substring_index('', '.', 1)`)
	result.Check(solitonutil.EventsWithSep(",", "www.whtcorpsinc.com,,"))
	result = tk.MustQuery(`select substring_index(null, '.', 1), substring_index('www.whtcorpsinc.com', null, 1), substring_index('www.whtcorpsinc.com', '.', null)`)
	result.Check(testkit.Events("<nil> <nil> <nil>"))

	// for hex
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(20), b int, c double, d datetime, e time, f decimal(5, 2), g bit(4))")
	tk.MustExec(`insert into t values('www.whtcorpsinc.com', 12345, 123.45, "2020-01-01 12:01:01", "12:01:01", 123.45, 0b1100)`)
	result = tk.MustQuery(`select hex(a), hex(b), hex(c), hex(d), hex(e), hex(f), hex(g) from t`)
	result.Check(testkit.Events("7777772E70696E676361702E636F6D 3039 7B 323031372D30312D30312031323A30313A3031 31323A30313A3031 7B C"))
	result = tk.MustQuery(`select hex('abc'), hex('你好'), hex(12), hex(12.3), hex(12.8)`)
	result.Check(testkit.Events("616263 E4BDA0E5A5BD C C D"))
	result = tk.MustQuery(`select hex(-1), hex(-12.3), hex(-12.8), hex(0x12), hex(null)`)
	result.Check(testkit.Events("FFFFFFFFFFFFFFFF FFFFFFFFFFFFFFF4 FFFFFFFFFFFFFFF3 12 <nil>"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t(i int primary key auto_increment, a binary, b binary(0), c binary(20), d binary(255)) character set utf8 defCauslate utf8_bin;")
	tk.MustExec("insert into t(a, b, c, d) values ('a', NULL, 'a','a');")
	tk.MustQuery("select i, hex(a), hex(b), hex(c), hex(d) from t;").Check(testkit.Events("1 61 <nil> 6100000000000000000000000000000000000000 610000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"))

	// for unhex
	result = tk.MustQuery(`select unhex('4D7953514C'), unhex('313233'), unhex(313233), unhex('')`)
	result.Check(testkit.Events("MyALLEGROSQL 123 123 "))
	result = tk.MustQuery(`select unhex('string'), unhex('你好'), unhex(123.4), unhex(null)`)
	result.Check(testkit.Events("<nil> <nil> <nil> <nil>"))

	// for ltrim and rtrim
	result = tk.MustQuery(`select ltrim('   bar   '), ltrim('bar'), ltrim(''), ltrim(null)`)
	result.Check(solitonutil.EventsWithSep(",", "bar   ,bar,,<nil>"))
	result = tk.MustQuery(`select rtrim('   bar   '), rtrim('bar'), rtrim(''), rtrim(null)`)
	result.Check(solitonutil.EventsWithSep(",", "   bar,bar,,<nil>"))
	result = tk.MustQuery(`select ltrim("\t   bar   "), ltrim("   \tbar"), ltrim("\n  bar"), ltrim("\r  bar")`)
	result.Check(solitonutil.EventsWithSep(",", "\t   bar   ,\tbar,\n  bar,\r  bar"))
	result = tk.MustQuery(`select rtrim("   bar   \t"), rtrim("bar\t   "), rtrim("bar   \n"), rtrim("bar   \r")`)
	result.Check(solitonutil.EventsWithSep(",", "   bar   \t,bar\t,bar   \n,bar   \r"))

	// for reverse
	tk.MustExec(`DROP TABLE IF EXISTS t;`)
	tk.MustExec(`CREATE TABLE t(a BINARY(6));`)
	tk.MustExec(`INSERT INTO t VALUES("中文");`)
	result = tk.MustQuery(`SELECT a, REVERSE(a), REVERSE("中文"), REVERSE("123 ") FROM t;`)
	result.Check(testkit.Events("中文 \x87\x96歸\xe4 文中  321"))
	result = tk.MustQuery(`SELECT REVERSE(123), REVERSE(12.09) FROM t;`)
	result.Check(testkit.Events("321 90.21"))

	// for trim
	result = tk.MustQuery(`select trim('   bar   '), trim(leading 'x' from 'xxxbarxxx'), trim(trailing 'xyz' from 'barxxyz'), trim(both 'x' from 'xxxbarxxx')`)
	result.Check(testkit.Events("bar barxxx barx bar"))
	result = tk.MustQuery(`select trim('\t   bar\n   '), trim('   \rbar   \t')`)
	result.Check(solitonutil.EventsWithSep(",", "\t   bar\n,\rbar   \t"))
	result = tk.MustQuery(`select trim(leading from '   bar'), trim('x' from 'xxxbarxxx'), trim('x' from 'bar'), trim('' from '   bar   ')`)
	result.Check(solitonutil.EventsWithSep(",", "bar,bar,bar,   bar   "))
	result = tk.MustQuery(`select trim(''), trim('x' from '')`)
	result.Check(solitonutil.EventsWithSep(",", ","))
	result = tk.MustQuery(`select trim(null from 'bar'), trim('x' from null), trim(null), trim(leading null from 'bar')`)
	// FIXME: the result for trim(leading null from 'bar') should be <nil>, current is 'bar'
	result.Check(testkit.Events("<nil> <nil> <nil> bar"))

	// for locate
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(20), b int, c double, d datetime, e time, f binary(5))")
	tk.MustExec(`insert into t values('www.whtcorpsinc.com', 12345, 123.45, "2020-01-01 12:01:01", "12:01:01", "HelLo")`)
	result = tk.MustQuery(`select locate(".ping", a), locate(".ping", a, 5) from t`)
	result.Check(testkit.Events("4 0"))
	result = tk.MustQuery(`select locate("234", b), locate("235", b, 10) from t`)
	result.Check(testkit.Events("2 0"))
	result = tk.MustQuery(`select locate(".45", c), locate(".35", b) from t`)
	result.Check(testkit.Events("4 0"))
	result = tk.MustQuery(`select locate("El", f), locate("ll", f), locate("lL", f), locate("Lo", f), locate("lo", f) from t`)
	result.Check(testkit.Events("0 0 3 4 0"))
	result = tk.MustQuery(`select locate("01 12", d) from t`)
	result.Check(testkit.Events("9"))
	result = tk.MustQuery(`select locate("文", "中文字符串", 2)`)
	result.Check(testkit.Events("2"))
	result = tk.MustQuery(`select locate("文", "中文字符串", 3)`)
	result.Check(testkit.Events("0"))
	result = tk.MustQuery(`select locate("文", "中文字符串")`)
	result.Check(testkit.Events("2"))

	// for bin
	result = tk.MustQuery(`select bin(-1);`)
	result.Check(testkit.Events("1111111111111111111111111111111111111111111111111111111111111111"))
	result = tk.MustQuery(`select bin(5);`)
	result.Check(testkit.Events("101"))
	result = tk.MustQuery(`select bin("中文");`)
	result.Check(testkit.Events("0"))

	// for character_length
	result = tk.MustQuery(`select character_length(null), character_length("Hello"), character_length("a中b文c"),
	character_length(123), character_length(12.3456);`)
	result.Check(testkit.Events("<nil> 5 5 3 7"))

	// for char_length
	result = tk.MustQuery(`select char_length(null), char_length("Hello"), char_length("a中b文c"), char_length(123),char_length(12.3456);`)
	result.Check(testkit.Events("<nil> 5 5 3 7"))
	result = tk.MustQuery(`select char_length(null), char_length("Hello"), char_length("a 中 b 文 c"), char_length("НОЧЬ НА ОКРАИНЕ МОСКВЫ");`)
	result.Check(testkit.Events("<nil> 5 9 22"))
	// for char_length, binary string type
	result = tk.MustQuery(`select char_length(null), char_length(binary("Hello")), char_length(binary("a 中 b 文 c")), char_length(binary("НОЧЬ НА ОКРАИНЕ МОСКВЫ"));`)
	result.Check(testkit.Events("<nil> 5 13 41"))

	// for elt
	result = tk.MustQuery(`select elt(0, "abc", "def"), elt(2, "hello", "中文", "milevadb"), elt(4, "hello", "中文",
	"milevadb");`)
	result.Check(testkit.Events("<nil> 中文 <nil>"))

	// for instr
	result = tk.MustQuery(`select instr("中国", "国"), instr("中国", ""), instr("abc", ""), instr("", ""), instr("", "abc");`)
	result.Check(testkit.Events("2 1 1 1 0"))
	result = tk.MustQuery(`select instr("中国", null), instr(null, ""), instr(null, null);`)
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a binary(20), b char(20));`)
	tk.MustExec(`insert into t values("中国", cast("国" as binary)), ("中国", ""), ("abc", ""), ("", ""), ("", "abc");`)
	result = tk.MustQuery(`select instr(a, b) from t;`)
	result.Check(testkit.Events("4", "1", "1", "1", "0"))

	// for oct
	result = tk.MustQuery(`select oct("aaaa"), oct("-1.9"),  oct("-9999999999999999999999999"), oct("9999999999999999999999999");`)
	result.Check(testkit.Events("0 1777777777777777777777 1777777777777777777777 1777777777777777777777"))
	result = tk.MustQuery(`select oct(-1.9), oct(1.9), oct(-1), oct(1), oct(-9999999999999999999999999), oct(9999999999999999999999999);`)
	result.Check(testkit.Events("1777777777777777777777 1 1777777777777777777777 1 1777777777777777777777 1777777777777777777777"))

	// #issue 4356
	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (b BIT(8));")
	tk.MustExec(`INSERT INTO t SET b = b'11111111';`)
	tk.MustExec(`INSERT INTO t SET b = b'1010';`)
	tk.MustExec(`INSERT INTO t SET b = b'0101';`)
	result = tk.MustQuery(`SELECT b+0, BIN(b), OCT(b), HEX(b) FROM t;`)
	result.Check(testkit.Events("255 11111111 377 FF", "10 1010 12 A", "5 101 5 5"))

	// for find_in_set
	result = tk.MustQuery(`select find_in_set("", ""), find_in_set("", ","), find_in_set("中文", "字符串,中文"), find_in_set("b,", "a,b,c,d");`)
	result.Check(testkit.Events("0 1 2 0"))
	result = tk.MustQuery(`select find_in_set(NULL, ""), find_in_set("", NULL), find_in_set(1, "2,3,1");`)
	result.Check(testkit.Events("<nil> <nil> 3"))

	// for make_set
	result = tk.MustQuery(`select make_set(0, "12"), make_set(3, "aa", "11"), make_set(3, NULL, "中文"), make_set(NULL, "aa");`)
	result.Check(testkit.Events(" aa,11 中文 <nil>"))

	// for quote
	result = tk.MustQuery(`select quote("aaaa"), quote(""), quote("\"\""), quote("\n\n");`)
	result.Check(testkit.Events("'aaaa' '' '\"\"' '\n\n'"))
	result = tk.MustQuery(`select quote(0121), quote(0000), quote("中文"), quote(NULL);`)
	result.Check(testkit.Events("'121' '0' '中文' NULL"))
	tk.MustQuery(`select quote(null) is NULL;`).Check(testkit.Events(`0`))
	tk.MustQuery(`select quote(null) is NOT NULL;`).Check(testkit.Events(`1`))
	tk.MustQuery(`select length(quote(null));`).Check(testkit.Events(`4`))
	tk.MustQuery(`select quote(null) REGEXP binary 'null'`).Check(testkit.Events(`0`))
	tk.MustQuery(`select quote(null) REGEXP binary 'NULL'`).Check(testkit.Events(`1`))
	tk.MustQuery(`select quote(null) REGEXP 'NULL'`).Check(testkit.Events(`1`))
	tk.MustQuery(`select quote(null) REGEXP 'null'`).Check(testkit.Events(`0`))

	// for convert
	result = tk.MustQuery(`select convert("123" using "binary"), convert("中文" using "binary"), convert("中文" using "utf8"), convert("中文" using "utf8mb4"), convert(cast("中文" as binary) using "utf8");`)
	result.Check(testkit.Events("123 中文 中文 中文 中文"))
	// Charset 866 does not have a default defCauslation configured currently, so this will return error.
	err = tk.ExecToErr(`select convert("123" using "866");`)
	c.Assert(err.Error(), Equals, "[berolinaAllegroSQL:1115]Unknown character set: '866'")
	// Test case in issue #4436.
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a char(20));")
	err = tk.ExecToErr("select convert(a using a) from t;")
	c.Assert(err.Error(), Equals, "[berolinaAllegroSQL:1115]Unknown character set: 'a'")

	// for insert
	result = tk.MustQuery(`select insert("中文", 1, 1, cast("aaa" as binary)), insert("ba", -1, 1, "aaa"), insert("ba", 1, 100, "aaa"), insert("ba", 100, 1, "aaa");`)
	result.Check(testkit.Events("aaa文 ba aaa ba"))
	result = tk.MustQuery(`select insert("bb", NULL, 1, "aa"), insert("bb", 1, NULL, "aa"), insert(NULL, 1, 1, "aaa"), insert("bb", 1, 1, NULL);`)
	result.Check(testkit.Events("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`SELECT INSERT("bb", 0, 1, NULL), INSERT("bb", 0, NULL, "aaa");`)
	result.Check(testkit.Events("<nil> <nil>"))
	result = tk.MustQuery(`SELECT INSERT("中文", 0, 1, NULL), INSERT("中文", 0, NULL, "aaa");`)
	result.Check(testkit.Events("<nil> <nil>"))

	// for export_set
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", 65);`)
	result.Check(testkit.Events("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", -1);`)
	result.Check(testkit.Events("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",");`)
	result.Check(testkit.Events("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0");`)
	result.Check(testkit.Events("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(NULL, "1", "0", ",", 65);`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", 1);`)
	result.Check(testkit.Events("1"))

	// for format
	result = tk.MustQuery(`select format(12332.1, 4), format(12332.2, 0), format(12332.2, 2,'en_US');`)
	result.Check(testkit.Events("12,332.1000 12,332 12,332.20"))
	result = tk.MustQuery(`select format(NULL, 4), format(12332.2, NULL);`)
	result.Check(testkit.Events("<nil> <nil>"))
	rs, err := tk.Exec(`select format(12332.2, 2,'es_EC');`)
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "not support for the specific locale")
	c.Assert(rs.Close(), IsNil)

	// for field
	result = tk.MustQuery(`select field(1, 2, 1), field(1, 0, NULL), field(1, NULL, 2, 1), field(NULL, 1, 2, NULL);`)
	result.Check(testkit.Events("2 0 3 0"))
	result = tk.MustQuery(`select field("1", 2, 1), field(1, "0", NULL), field("1", NULL, 2, 1), field(NULL, 1, "2", NULL);`)
	result.Check(testkit.Events("2 0 3 0"))
	result = tk.MustQuery(`select field("1", 2, 1), field(1, "abc", NULL), field("1", NULL, 2, 1), field(NULL, 1, "2", NULL);`)
	result.Check(testkit.Events("2 0 3 0"))
	result = tk.MustQuery(`select field("abc", "a", 1), field(1.3, "1.3", 1.5);`)
	result.Check(testkit.Events("1 1"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a decimal(11, 8), b decimal(11,8))")
	tk.MustExec("insert into t values('114.57011441','38.04620115'), ('-38.04620119', '38.04620115');")
	result = tk.MustQuery("select a,b,concat_ws(',',a,b) from t")
	result.Check(testkit.Events("114.57011441 38.04620115 114.57011441,38.04620115",
		"-38.04620119 38.04620115 -38.04620119,38.04620115"))
}

func (s *testIntegrationSuite2) TestEncryptionBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	ctx := context.Background()

	// for password
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(41), b char(41), c char(41))")
	tk.MustExec(`insert into t values(NULL, '', 'abc')`)
	result := tk.MustQuery("select password(a) from t")
	result.Check(testkit.Events(""))
	result = tk.MustQuery("select password(b) from t")
	result.Check(testkit.Events(""))
	result = tk.MustQuery("select password(c) from t")
	result.Check(testkit.Events("*0D3CED9BEC10A777AEC23CCC353A8C08A633045E"))

	// for md5
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2020-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "milevadb")`)
	result = tk.MustQuery("select md5(a), md5(b), md5(c), md5(d), md5(e), md5(f), md5(g), md5(h), md5(i) from t")
	result.Check(testkit.Events("c81e728d9d4c2f636f067f89cc14862c c81e728d9d4c2f636f067f89cc14862c 1a18da63cbbfb49cb9616e6bfd35f662 bad2fa88e1f35919ec7584cc2623a310 991f84d41d7acff6471e536caa8d97db 68b329da9893e34099c7d8ad5cb9c940 5c9f0e9b3b36276731bfba852a73ccc6 642e92efb79421734881b53e1e1b18b6 c337e11bfca9f12ae9b1342901e04379"))
	result = tk.MustQuery("select md5('123'), md5(123), md5(''), md5('你好'), md5(NULL), md5('👍')")
	result.Check(testkit.Events(`202cb962ac59075b964b07152d234b70 202cb962ac59075b964b07152d234b70 d41d8cd98f00b204e9800998ecf8427e 7eca689f0d3389d9dea66ae112e5cfd7 <nil> 0215ac4dab1ecaf71d83f98af5726984`))

	// for sha/sha1
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2020-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "milevadb")`)
	result = tk.MustQuery("select sha1(a), sha1(b), sha1(c), sha1(d), sha1(e), sha1(f), sha1(g), sha1(h), sha1(i) from t")
	result.Check(testkit.Events("da4b9237bacccdf19c0760cab7aec4a8359010b0 da4b9237bacccdf19c0760cab7aec4a8359010b0 ce0d88c5002b6cf7664052f1fc7d652cbdadccec 6c6956de323692298e4e5ad3028ff491f7ad363c 1906f8aeb5a717ca0f84154724045839330b0ea9 adc83b19e793491b1c6ea0fd8b46cd9f32e592fc 9aadd14ceb737b28697b8026f205f4b3e31de147 64e095fe763fc62418378753f9402623bea9e227 4df56fc09a3e66b48fb896e90b0a6fc02c978e9e"))
	result = tk.MustQuery("select sha1('123'), sha1(123), sha1(''), sha1('你好'), sha1(NULL)")
	result.Check(testkit.Events(`40bd001563085fc35165329ea1ff5c5ecbdbbeef 40bd001563085fc35165329ea1ff5c5ecbdbbeef da39a3ee5e6b4b0d3255bfef95601890afd80709 440ee0853ad1e99f962b63e459ef992d7c211722 <nil>`))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2020-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "milevadb")`)
	result = tk.MustQuery("select sha(a), sha(b), sha(c), sha(d), sha(e), sha(f), sha(g), sha(h), sha(i) from t")
	result.Check(testkit.Events("da4b9237bacccdf19c0760cab7aec4a8359010b0 da4b9237bacccdf19c0760cab7aec4a8359010b0 ce0d88c5002b6cf7664052f1fc7d652cbdadccec 6c6956de323692298e4e5ad3028ff491f7ad363c 1906f8aeb5a717ca0f84154724045839330b0ea9 adc83b19e793491b1c6ea0fd8b46cd9f32e592fc 9aadd14ceb737b28697b8026f205f4b3e31de147 64e095fe763fc62418378753f9402623bea9e227 4df56fc09a3e66b48fb896e90b0a6fc02c978e9e"))
	result = tk.MustQuery("select sha('123'), sha(123), sha(''), sha('你好'), sha(NULL)")
	result.Check(testkit.Events(`40bd001563085fc35165329ea1ff5c5ecbdbbeef 40bd001563085fc35165329ea1ff5c5ecbdbbeef da39a3ee5e6b4b0d3255bfef95601890afd80709 440ee0853ad1e99f962b63e459ef992d7c211722 <nil>`))

	// for sha2
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2020-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "milevadb")`)
	result = tk.MustQuery("select sha2(a, 224), sha2(b, 0), sha2(c, 512), sha2(d, 256), sha2(e, 384), sha2(f, 0), sha2(g, 512), sha2(h, 256), sha2(i, 224) from t")
	result.Check(testkit.Events("58b2aaa0bfae7acc021b3260e941117b529b2e69de878fd7d45c61a9 d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35 42415572557b0ca47e14fa928e83f5746d33f90c74270172cc75c61a78db37fe1485159a4fd75f33ab571b154572a5a300938f7d25969bdd05d8ac9dd6c66123 8c2fa3f276952c92b0b40ed7d27454e44b8399a19769e6bceb40da236e45a20a b11d35f1a37e54d5800d210d8e6b80b42c9f6d20ea7ae548c762383ebaa12c5954c559223c6c7a428e37af96bb4f1e0d 01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b 9550da35ea1683abaf5bfa8de68fe02b9c6d756c64589d1ef8367544c254f5f09218a6466cadcee8d74214f0c0b7fb342d1a9f3bd4d406aacf7be59c327c9306 98010bd9270f9b100b6214a21754fd33bdc8d41b2bc9f9dd16ff54d3c34ffd71 a7cddb7346fbc66ab7f803e865b74cbd99aace8e7dabbd8884c148cb"))
	result = tk.MustQuery("select sha2('123', 512), sha2(123, 512), sha2('', 512), sha2('你好', 224), sha2(NULL, 256), sha2('foo', 123)")
	result.Check(testkit.Events(`3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2 3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2 cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e e91f006ed4e0882de2f6a3c96ec228a6a5c715f356d00091bce842b5 <nil> <nil>`))

	// for AES_ENCRYPT
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2020-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "milevadb")`)
	tk.MustExec("SET block_encryption_mode='aes-128-ecb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key')), HEX(AES_ENCRYPT(b, 'key')), HEX(AES_ENCRYPT(c, 'key')), HEX(AES_ENCRYPT(d, 'key')), HEX(AES_ENCRYPT(e, 'key')), HEX(AES_ENCRYPT(f, 'key')), HEX(AES_ENCRYPT(g, 'key')), HEX(AES_ENCRYPT(h, 'key')), HEX(AES_ENCRYPT(i, 'key')) from t")
	result.Check(testkit.Events("B3800B3A3CB4ECE2051A3E80FE373EAC B3800B3A3CB4ECE2051A3E80FE373EAC 9E018F7F2838DBA23C57F0E4CCF93287 E764D3E9D4AF8F926CD0979DDB1D0AF40C208B20A6C39D5D028644885280973A C452FFEEB76D3F5E9B26B8D48F7A228C 181BD5C81CBD36779A3C9DD5FF486B35 CE15F14AC7FF4E56ECCF148DE60E4BEDBDB6900AD51383970A5F32C59B3AC6E3 E1B29995CCF423C75519790F54A08CD2 84525677E95AC97698D22E1125B67E92"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar')), HEX(AES_ENCRYPT(123, 'foobar')), HEX(AES_ENCRYPT('', 'foobar')), HEX(AES_ENCRYPT('你好', 'foobar')), AES_ENCRYPT(NULL, 'foobar')")
	result.Check(testkit.Events(`45ABDD5C4802EFA6771A94C43F805208 45ABDD5C4802EFA6771A94C43F805208 791F1AEB6A6B796E6352BF381895CA0E D0147E2EB856186F146D9F6DE33F9546 <nil>`))
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', 'iv')), HEX(AES_ENCRYPT(b, 'key', 'iv')) from t")
	result.Check(testkit.Events("B3800B3A3CB4ECE2051A3E80FE373EAC B3800B3A3CB4ECE2051A3E80FE373EAC"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1618|<IV> option ignored", "Warning|1618|<IV> option ignored"))
	tk.MustExec("SET block_encryption_mode='aes-128-cbc';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Events("341672829F84CB6B0BE690FEC4C4DAE9 341672829F84CB6B0BE690FEC4C4DAE9 D43734E147A12BB96C6897C4BBABA283 16F2C972411948DCEF3659B726D2CCB04AD1379A1A367FA64242058A50211B67 41E71D0C58967C1F50EEC074523946D1 1117D292E2D39C3EAA3B435371BE56FC 8ACB7ECC0883B672D7BD1CFAA9FA5FAF5B731ADE978244CD581F114D591C2E7E D2B13C30937E3251AEDA73859BA32E4B 2CF4A6051FF248A67598A17AA2C17267"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Events(`80D5646F07B4654B05A02D9085759770 80D5646F07B4654B05A02D9085759770 B3C14BA15030D2D7E99376DBE011E752 0CD2936EE4FEC7A8CDF6208438B2BC05 <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-128-ofb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Events("40 40 40C35C 40DD5EBDFCAA397102386E27DDF97A39ECCEC5 43DF55BAE0A0386D 78 47DC5D8AD19A085C32094E16EFC34A08D6FEF459 46D5 06840BE8"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Events(`48E38A 48E38A  9D6C199101C3 <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-192-ofb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Events("4B 4B 4B573F 4B493D42572E6477233A429BF3E0AD39DB816D 484B36454B24656B 73 4C483E757A1E555A130B62AAC1DA9D08E1B15C47 4D41 0D106817"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Events(`3A76B0 3A76B0  EFF92304268E <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-256-ofb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Events("16 16 16D103 16CF01CBC95D33E2ED721CBD930262415A69AD 15CD0ACCD55732FE 2E 11CE02FCE46D02CFDD433C8CA138527060599C35 10C7 5096549E"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Events(`E842C5 E842C5  3DCD5646767D <nil>`))

	// for AES_DECRYPT
	tk.MustExec("SET block_encryption_mode='aes-128-ecb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar'), 'bar')")
	result.Check(testkit.Events("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('45ABDD5C4802EFA6771A94C43F805208'), 'foobar'), AES_DECRYPT(UNHEX('791F1AEB6A6B796E6352BF381895CA0E'), 'foobar'), AES_DECRYPT(UNHEX('D0147E2EB856186F146D9F6DE33F9546'), 'foobar'), AES_DECRYPT(NULL, 'foobar'), AES_DECRYPT('SOME_THING_STRANGE', 'foobar')")
	result.Check(testkit.Events(`123  你好 <nil> <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-128-cbc';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Events("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('80D5646F07B4654B05A02D9085759770'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('B3C14BA15030D2D7E99376DBE011E752'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('0CD2936EE4FEC7A8CDF6208438B2BC05'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456')")
	result.Check(testkit.Events(`123  你好 <nil> <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-128-ofb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Events("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('48E38A'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX(''), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('9D6C199101C3'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), HEX(AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456'))")
	result.Check(testkit.Events(`123  你好 <nil> 2A9EF431FB2ACB022D7F2E7C71EEC48C7D2B`))
	tk.MustExec("SET block_encryption_mode='aes-192-ofb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Events("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('3A76B0'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX(''), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('EFF92304268E'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), HEX(AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456'))")
	result.Check(testkit.Events(`123  你好 <nil> 580BCEA4DC67CF33FF2C7C570D36ECC89437`))
	tk.MustExec("SET block_encryption_mode='aes-256-ofb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Events("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('E842C5'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX(''), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('3DCD5646767D'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), HEX(AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456'))")
	result.Check(testkit.Events(`123  你好 <nil> 8A3FBBE68C9465834584430E3AEEBB04B1F5`))

	// for COMPRESS
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1(a VARCHAR(1000));")
	tk.MustExec("INSERT INTO t1 VALUES('12345'), ('23456');")
	result = tk.MustQuery("SELECT HEX(COMPRESS(a)) FROM t1;")
	result.Check(testkit.Events("05000000789C323432363105040000FFFF02F80100", "05000000789C323236313503040000FFFF03070105"))
	tk.MustExec("DROP TABLE IF EXISTS t2;")
	tk.MustExec("CREATE TABLE t2(a VARCHAR(1000), b VARBINARY(1000));")
	tk.MustExec("INSERT INTO t2 (a, b) SELECT a, COMPRESS(a) from t1;")
	result = tk.MustQuery("SELECT a, HEX(b) FROM t2;")
	result.Check(testkit.Events("12345 05000000789C323432363105040000FFFF02F80100", "23456 05000000789C323236313503040000FFFF03070105"))

	// for UNCOMPRESS
	result = tk.MustQuery("SELECT UNCOMPRESS(COMPRESS('123'))")
	result.Check(testkit.Events("123"))
	result = tk.MustQuery("SELECT UNCOMPRESS(UNHEX('03000000789C3334320600012D0097'))")
	result.Check(testkit.Events("123"))
	result = tk.MustQuery("SELECT UNCOMPRESS(UNHEX('03000000789C32343206040000FFFF012D0097'))")
	result.Check(testkit.Events("123"))
	tk.MustExec("INSERT INTO t2 VALUES ('12345', UNHEX('05000000789C3334323631050002F80100'))")
	result = tk.MustQuery("SELECT UNCOMPRESS(a), UNCOMPRESS(b) FROM t2;")
	result.Check(testkit.Events("<nil> 12345", "<nil> 23456", "<nil> 12345"))

	// for UNCOMPRESSED_LENGTH
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(COMPRESS('123'))")
	result.Check(testkit.Events("3"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(UNHEX('03000000789C3334320600012D0097'))")
	result.Check(testkit.Events("3"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(UNHEX('03000000789C32343206040000FFFF012D0097'))")
	result.Check(testkit.Events("3"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH('')")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(UNHEX('0100'))")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(a), UNCOMPRESSED_LENGTH(b) FROM t2;")
	result.Check(testkit.Events("875770417 5", "892613426 5", "875770417 5"))

	// for RANDOM_BYTES
	lengths := []int{0, -5, 1025, 4000}
	for _, len := range lengths {
		rs, err := tk.Exec(fmt.Sprintf("SELECT RANDOM_BYTES(%d);", len))
		c.Assert(err, IsNil, Commentf("%v", len))
		_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
		c.Assert(err, NotNil, Commentf("%v", len))
		terr := errors.Cause(err).(*terror.Error)
		c.Assert(terr.Code(), Equals, errors.ErrCode(allegrosql.ErrDataOutOfRange), Commentf("%v", len))
		c.Assert(rs.Close(), IsNil)
	}
	tk.MustQuery("SELECT RANDOM_BYTES('1');")
	tk.MustQuery("SELECT RANDOM_BYTES(1024);")
	result = tk.MustQuery("SELECT RANDOM_BYTES(NULL);")
	result.Check(testkit.Events("<nil>"))
}

func (s *testIntegrationSuite2) TestTimeBuiltin(c *C) {
	originALLEGROSQLMode := s.ctx.GetStochastikVars().StrictALLEGROSQLMode
	s.ctx.GetStochastikVars().StrictALLEGROSQLMode = true
	defer func() {
		s.ctx.GetStochastikVars().StrictALLEGROSQLMode = originALLEGROSQLMode
		s.cleanEnv(c)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	// for makeDate
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2020-01-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)
	result := tk.MustQuery("select makedate(a,a), makedate(b,b), makedate(c,c), makedate(d,d), makedate(e,e), makedate(f,f), makedate(null,null), makedate(a,b) from t")
	result.Check(testkit.Events("2001-01-01 2001-01-01 <nil> <nil> <nil> 2021-01-21 <nil> 2001-01-01"))

	// for date
	result = tk.MustQuery(`select date("2020-09-12"), date("2020-09-12 12:12:09"), date("2020-09-12 12:12:09.121212");`)
	result.Check(testkit.Events("2020-09-12 2020-09-12 2020-09-12"))
	result = tk.MustQuery(`select date("0000-00-00"), date("0000-00-00 12:12:09"), date("0000-00-00 00:00:00.121212"), date("0000-00-00 00:00:00.000000");`)
	result.Check(testkit.Events("<nil> 0000-00-00 0000-00-00 <nil>"))
	result = tk.MustQuery(`select date("aa"), date(12.1), date("");`)
	result.Check(testkit.Events("<nil> <nil> <nil>"))

	// for year
	result = tk.MustQuery(`select year("2020-01-09"), year("2020-00-09"), year("000-01-09"), year("1-01-09"), year("20131-01-09"), year(null);`)
	result.Check(testkit.Events("2020 2020 0 1 <nil> <nil>"))
	result = tk.MustQuery(`select year("2020-00-00"), year("2020-00-00 00:00:00"), year("0000-00-00 12:12:12"), year("2020-00-00 12:12:12");`)
	result.Check(testkit.Events("2020 2020 0 2020"))
	result = tk.MustQuery(`select year("aa"), year(2020), year(2012.09), year("1-01"), year("-09");`)
	result.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a bigint)`)
	_, err := tk.Exec(`insert into t select year("aa")`)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue, Commentf("err %v", err))
	tk.MustExec(`set sql_mode='STRICT_TRANS_TABLES'`) // without zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`) // with zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select year("0000-00-00 00:00:00");`)
	c.Assert(err, NotNil)
	c.Assert(types.ErrWrongValue.Equal(err), IsTrue, Commentf("err %v", err))
	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	_, err = tk.Exec(`uFIDelate t set a = year("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec(`delete from t where a = year("aa")`)
	// Only `code` can be used to compare because the error `class` information
	// will be lost after expression push-down
	c.Assert(errors.Cause(err).(*terror.Error).Code(), Equals, types.ErrWrongValue.Code(), Commentf("err %v", err))

	// for month
	result = tk.MustQuery(`select month("2020-01-09"), month("2020-00-09"), month("000-01-09"), month("1-01-09"), month("20131-01-09"), month(null);`)
	result.Check(testkit.Events("1 0 1 1 <nil> <nil>"))
	result = tk.MustQuery(`select month("2020-00-00"), month("2020-00-00 00:00:00"), month("0000-00-00 12:12:12"), month("2020-00-00 12:12:12");`)
	result.Check(testkit.Events("0 0 0 0"))
	result = tk.MustQuery(`select month("aa"), month(2020), month(2012.09), month("1-01"), month("-09");`)
	result.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select month("2020-012-09"), month("2020-0000000012-09"), month("2020-30-09"), month("000-41-09");`)
	result.Check(testkit.Events("12 12 <nil> <nil>"))
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a bigint)`)
	_, err = tk.Exec(`insert into t select month("aa")`)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue, Commentf("err: %v", err))
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`)
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select month("0000-00-00 00:00:00");`)
	c.Assert(err, NotNil)
	c.Assert(types.ErrWrongValue.Equal(err), IsTrue, Commentf("err %v", err))
	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`uFIDelate t set a = month("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue)
	_, err = tk.Exec(`delete from t where a = month("aa")`)
	c.Assert(errors.Cause(err).(*terror.Error).Code(), Equals, types.ErrWrongValue.Code(), Commentf("err %v", err))

	// for week
	result = tk.MustQuery(`select week("2012-12-22"), week("2012-12-22", -2), week("2012-12-22", 0), week("2012-12-22", 1), week("2012-12-22", 2), week("2012-12-22", 200);`)
	result.Check(testkit.Events("51 51 51 51 51 51"))
	result = tk.MustQuery(`select week("2008-02-20"), week("2008-02-20", 0), week("2008-02-20", 1), week("2009-02-20", 2), week("2008-02-20", 3), week("2008-02-20", 4);`)
	result.Check(testkit.Events("7 7 8 7 8 8"))
	result = tk.MustQuery(`select week("2008-02-20", 5), week("2008-02-20", 6), week("2009-02-20", 7), week("2008-02-20", 8), week("2008-02-20", 9);`)
	result.Check(testkit.Events("7 8 7 7 8"))
	result = tk.MustQuery(`select week("aa", 1), week(null, 2), week(11, 2), week(12.99, 2);`)
	result.Check(testkit.Events("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select week("aa"), week(null), week(11), week(12.99);`)
	result.Check(testkit.Events("<nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a datetime)`)
	_, err = tk.Exec(`insert into t select week("aa", 1)`)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue)
	tk.MustExec(`insert into t select now()`)
	_, err = tk.Exec(`uFIDelate t set a = week("aa", 1)`)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue)
	_, err = tk.Exec(`delete from t where a = week("aa", 1)`)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue)

	// for weekofyear
	result = tk.MustQuery(`select weekofyear("2012-12-22"), weekofyear("2008-02-20"), weekofyear("aa"), weekofyear(null), weekofyear(11), weekofyear(12.99);`)
	result.Check(testkit.Events("51 8 <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a bigint)`)
	_, err = tk.Exec(`insert into t select weekofyear("aa")`)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue)
	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`uFIDelate t set a = weekofyear("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue)
	_, err = tk.Exec(`delete from t where a = weekofyear("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue)

	// for weekday
	result = tk.MustQuery(`select weekday("2012-12-20"), weekday("2012-12-21"), weekday("2012-12-22"), weekday("2012-12-23"), weekday("2012-12-24"), weekday("2012-12-25"), weekday("2012-12-26"), weekday("2012-12-27");`)
	result.Check(testkit.Events("3 4 5 6 0 1 2 3"))
	result = tk.MustQuery(`select weekday("2012-12-90"), weekday("0000-00-00"), weekday("aa"), weekday(null), weekday(11), weekday(12.99);`)
	result.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil> <nil>"))

	// for quarter
	result = tk.MustQuery(`select quarter("2012-00-20"), quarter("2012-01-21"), quarter("2012-03-22"), quarter("2012-05-23"), quarter("2012-08-24"), quarter("2012-09-25"), quarter("2012-11-26"), quarter("2012-12-27");`)
	result.Check(testkit.Events("0 1 1 2 3 3 4 4"))
	result = tk.MustQuery(`select quarter("2012-14-20"), quarter("aa"), quarter(null), quarter(11), quarter(12.99);`)
	result.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select quarter("0000-00-00"), quarter("0000-00-00 00:00:00");`)
	result.Check(testkit.Events("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	result = tk.MustQuery(`select quarter(0), quarter(0.0), quarter(0e1), quarter(0.00);`)
	result.Check(testkit.Events("0 0 0 0"))
	tk.MustQuery("show warnings").Check(testkit.Events())

	// for from_days
	result = tk.MustQuery(`select from_days(0), from_days(-199), from_days(1111), from_days(120), from_days(1), from_days(1111111), from_days(9999999), from_days(22222);`)
	result.Check(testkit.Events("0000-00-00 0000-00-00 0003-01-16 0000-00-00 0000-00-00 3042-02-13 0000-00-00 0060-11-03"))
	result = tk.MustQuery(`select from_days("2012-14-20"), from_days("111a"), from_days("aa"), from_days(null), from_days("123asf"), from_days(12.99);`)
	result.Check(testkit.Events("0005-07-05 0000-00-00 0000-00-00 <nil> 0000-00-00 0000-00-00"))

	// Fix issue #3923
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:00' as time), '12:00:00');")
	result.Check(testkit.Events("00:00:00"))
	result = tk.MustQuery("select timediff('12:00:00', cast('2004-12-30 12:00:00' as time));")
	result.Check(testkit.Events("00:00:00"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:00' as time), '2004-12-30 12:00:00');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00', cast('2004-12-30 12:00:00' as time));")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '2004-12-30 12:00:00');")
	result.Check(testkit.Events("00:00:01"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Events("-00:00:01"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as time), '-34 00:00:00');")
	result.Check(testkit.Events("828:00:01"))
	result = tk.MustQuery("select timediff('-34 00:00:00', cast('2004-12-30 12:00:01' as time));")
	result.Check(testkit.Events("-828:00:01"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), cast('2004-12-30 11:00:01' as datetime));")
	result.Check(testkit.Events("01:00:00"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '2004-12-30 12:00:00.1');")
	result.Check(testkit.Events("00:00:00.9"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00.1', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Events("-00:00:00.9"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '-34 124:00:00');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff('-34 124:00:00', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as time), '-34 124:00:00');")
	result.Check(testkit.Events("838:59:59"))
	result = tk.MustQuery("select timediff('-34 124:00:00', cast('2004-12-30 12:00:01' as time));")
	result.Check(testkit.Events("-838:59:59"))
	result = tk.MustQuery("select timediff(cast('2004-12-30' as datetime), '12:00:00');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', cast('2004-12-30' as datetime));")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', '-34 12:00:00');")
	result.Check(testkit.Events("838:59:59"))
	result = tk.MustQuery("select timediff('12:00:00', '34 12:00:00');")
	result.Check(testkit.Events("-816:00:00"))
	result = tk.MustQuery("select timediff('2020-1-2 12:00:00', '-34 12:00:00');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff('-34 12:00:00', '2020-1-2 12:00:00');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff('2020-1-2 12:00:00', '12:00:00');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', '2020-1-2 12:00:00');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select timediff('2020-1-2 12:00:00', '2020-1-1 12:00:00');")
	result.Check(testkit.Events("24:00:00"))
	tk.MustQuery("select timediff(cast('10:10:10' as time), cast('10:10:11' as time))").Check(testkit.Events("-00:00:01"))

	result = tk.MustQuery("select timestampadd(MINUTE, 1, '2003-01-02'), timestampadd(WEEK, 1, '2003-01-02 23:59:59')" +
		", timestampadd(MICROSECOND, 1, 950501);")
	result.Check(testkit.Events("2003-01-02 00:01:00 2003-01-09 23:59:59 1995-05-01 00:00:00.000001"))
	result = tk.MustQuery("select timestampadd(day, 2, 950501), timestampadd(MINUTE, 37.5,'2003-01-02'), timestampadd(MINUTE, 37.49,'2003-01-02')," +
		" timestampadd(YeAr, 1, '2003-01-02');")
	result.Check(testkit.Events("1995-05-03 00:00:00 2003-01-02 00:38:00 2003-01-02 00:37:00 2004-01-02 00:00:00"))
	result = tk.MustQuery("select to_seconds(950501), to_seconds('2009-11-29'), to_seconds('2009-11-29 13:43:32'), to_seconds('09-11-29 13:43:32');")
	result.Check(testkit.Events("62966505600 63426672000 63426721412 63426721412"))
	result = tk.MustQuery("select to_days(950501), to_days('2007-10-07'), to_days('2007-10-07 00:00:59'), to_days('0000-01-01')")
	result.Check(testkit.Events("728779 733321 733321 1"))

	result = tk.MustQuery("select last_day('2003-02-05'), last_day('2004-02-05'), last_day('2004-01-01 01:01:01'), last_day(950501);")
	result.Check(testkit.Events("2003-02-28 2004-02-29 2004-01-31 1995-05-31"))

	tk.MustExec("SET ALLEGROSQL_MODE='';")
	result = tk.MustQuery("select last_day('0000-00-00');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select to_days('0000-00-00');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select to_seconds('0000-00-00');")
	result.Check(testkit.Events("<nil>"))

	result = tk.MustQuery("select timestamp('2003-12-31'), timestamp('2003-12-31 12:00:00','12:00:00');")
	result.Check(testkit.Events("2003-12-31 00:00:00 2004-01-01 00:00:00"))
	result = tk.MustQuery("select timestamp(20170118123950.123), timestamp(20170118123950.999);")
	result.Check(testkit.Events("2020-01-18 12:39:50.123 2020-01-18 12:39:50.999"))
	result = tk.MustQuery("select timestamp('2003-12-31', '01:01:01.01'), timestamp('2003-12-31 12:34', '01:01:01.01')," +
		" timestamp('2008-12-31','00:00:00.0'), timestamp('2008-12-31 00:00:00.000');")

	tk.MustQuery(`select timestampadd(second, 1, cast("2001-01-01" as date))`).Check(testkit.Events("2001-01-01 00:00:01"))
	tk.MustQuery(`select timestampadd(hour, 1, cast("2001-01-01" as date))`).Check(testkit.Events("2001-01-01 01:00:00"))
	tk.MustQuery(`select timestampadd(day, 1, cast("2001-01-01" as date))`).Check(testkit.Events("2001-01-02"))
	tk.MustQuery(`select timestampadd(month, 1, cast("2001-01-01" as date))`).Check(testkit.Events("2001-02-01"))
	tk.MustQuery(`select timestampadd(year, 1, cast("2001-01-01" as date))`).Check(testkit.Events("2002-01-01"))
	tk.MustQuery(`select timestampadd(second, 1, cast("2001-01-01" as datetime))`).Check(testkit.Events("2001-01-01 00:00:01"))
	tk.MustQuery(`select timestampadd(hour, 1, cast("2001-01-01" as datetime))`).Check(testkit.Events("2001-01-01 01:00:00"))
	tk.MustQuery(`select timestampadd(day, 1, cast("2001-01-01" as datetime))`).Check(testkit.Events("2001-01-02 00:00:00"))
	tk.MustQuery(`select timestampadd(month, 1, cast("2001-01-01" as datetime))`).Check(testkit.Events("2001-02-01 00:00:00"))
	tk.MustQuery(`select timestampadd(year, 1, cast("2001-01-01" as datetime))`).Check(testkit.Events("2002-01-01 00:00:00"))

	result.Check(testkit.Events("2003-12-31 01:01:01.01 2003-12-31 13:35:01.01 2008-12-31 00:00:00.0 2008-12-31 00:00:00.000"))
	result = tk.MustQuery("select timestamp('2003-12-31', 1), timestamp('2003-12-31', -1);")
	result.Check(testkit.Events("2003-12-31 00:00:01 2003-12-30 23:59:59"))
	result = tk.MustQuery("select timestamp('2003-12-31', '2000-12-12 01:01:01.01'), timestamp('2003-14-31','01:01:01.01');")
	result.Check(testkit.Events("<nil> <nil>"))

	result = tk.MustQuery("select TIMESTAMFIDelIFF(MONTH,'2003-02-01','2003-05-01'), TIMESTAMFIDelIFF(yEaR,'2002-05-01', " +
		"'2001-01-01'), TIMESTAMFIDelIFF(minute,binary('2003-02-01'),'2003-05-01 12:05:55'), TIMESTAMFIDelIFF(day," +
		"'1995-05-02', 950501);")
	result.Check(testkit.Events("3 -1 128885 -1"))

	result = tk.MustQuery("select datediff('2007-12-31 23:59:59','2007-12-30'), datediff('2010-11-30 23:59:59', " +
		"'2010-12-31'), datediff(950501,'2020-01-13'), datediff(950501.9,'2020-01-13'), datediff(binary(950501), '2020-01-13');")
	result.Check(testkit.Events("1 -31 -7562 -7562 -7562"))
	result = tk.MustQuery("select datediff('0000-01-01','0001-01-01'), datediff('0001-00-01', '0001-00-01'), datediff('0001-01-00','0001-01-00'), datediff('2020-01-01','2020-01-01');")
	result.Check(testkit.Events("-365 <nil> <nil> 0"))

	// for ADDTIME
	result = tk.MustQuery("select addtime('01:01:11', '00:00:01.013'), addtime('01:01:11.00', '00:00:01'), addtime" +
		"('2020-01-01 01:01:11.12', '00:00:01'), addtime('2020-01-01 01:01:11.12', '00:00:01.88');")
	result.Check(testkit.Events("01:01:12.013000 01:01:12 2020-01-01 01:01:12.120000 2020-01-01 01:01:13"))
	result = tk.MustQuery("select addtime(cast('01:01:11' as time(4)), '00:00:01.013'), addtime(cast('01:01:11.00' " +
		"as datetime(3)), '00:00:01')," + " addtime(cast('2020-01-01 01:01:11.12' as date), '00:00:01'), addtime(cast" +
		"(cast('2020-01-01 01:01:11.12' as date) as datetime(2)), '00:00:01.88');")
	result.Check(testkit.Events("01:01:12.0130 2001-01-11 00:00:01.000 00:00:01 2020-01-01 00:00:01.88"))
	result = tk.MustQuery("select addtime('2020-01-01 01:01:01', 5), addtime('2020-01-01 01:01:01', -5), addtime('2020-01-01 01:01:01', 0.0), addtime('2020-01-01 01:01:01', 1.34);")
	result.Check(testkit.Events("2020-01-01 01:01:06 2020-01-01 01:00:56 2020-01-01 01:01:01 2020-01-01 01:01:02.340000"))
	result = tk.MustQuery("select addtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time)), addtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time(5)))")
	result.Check(testkit.Events("2001-01-11 00:00:01.000 2001-01-11 00:00:01.00000"))
	result = tk.MustQuery("select addtime(cast('01:01:11.00' as date), cast('00:00:01' as time));")
	result.Check(testkit.Events("00:00:01"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a datetime, b timestamp, c time)")
	tk.MustExec(`insert into t values("2020-01-01 12:30:31", "2020-01-01 12:30:31", "01:01:01")`)
	result = tk.MustQuery("select addtime(a, b), addtime(cast(a as date), b), addtime(b,a), addtime(a,c), addtime(b," +
		"c), addtime(c,a), addtime(c,b)" +
		" from t;")
	result.Check(testkit.Events("<nil> <nil> <nil> 2020-01-01 13:31:32 2020-01-01 13:31:32 <nil> <nil>"))
	result = tk.MustQuery("select addtime('01:01:11', cast('1' as time))")
	result.Check(testkit.Events("01:01:12"))
	tk.MustQuery("select addtime(cast(null as char(20)), cast('1' as time))").Check(testkit.Events("<nil>"))
	c.Assert(tk.QueryToErr(`select addtime("01:01:11", cast('sdf' as time))`), IsNil)
	tk.MustQuery(`select addtime("01:01:11", cast(null as char(20)))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select addtime(cast(1 as time), cast(1 as time))`).Check(testkit.Events("00:00:02"))
	tk.MustQuery(`select addtime(cast(null as time), cast(1 as time))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select addtime(cast(1 as time), cast(null as time))`).Check(testkit.Events("<nil>"))

	// for SUBTIME
	result = tk.MustQuery("select subtime('01:01:11', '00:00:01.013'), subtime('01:01:11.00', '00:00:01'), subtime" +
		"('2020-01-01 01:01:11.12', '00:00:01'), subtime('2020-01-01 01:01:11.12', '00:00:01.88');")
	result.Check(testkit.Events("01:01:09.987000 01:01:10 2020-01-01 01:01:10.120000 2020-01-01 01:01:09.240000"))
	result = tk.MustQuery("select subtime(cast('01:01:11' as time(4)), '00:00:01.013'), subtime(cast('01:01:11.00' " +
		"as datetime(3)), '00:00:01')," + " subtime(cast('2020-01-01 01:01:11.12' as date), '00:00:01'), subtime(cast" +
		"(cast('2020-01-01 01:01:11.12' as date) as datetime(2)), '00:00:01.88');")
	result.Check(testkit.Events("01:01:09.9870 2001-01-10 23:59:59.000 -00:00:01 2020-12-31 23:59:58.12"))
	result = tk.MustQuery("select subtime('2020-01-01 01:01:01', 5), subtime('2020-01-01 01:01:01', -5), subtime('2020-01-01 01:01:01', 0.0), subtime('2020-01-01 01:01:01', 1.34);")
	result.Check(testkit.Events("2020-01-01 01:00:56 2020-01-01 01:01:06 2020-01-01 01:01:01 2020-01-01 01:00:59.660000"))
	result = tk.MustQuery("select subtime('01:01:11', '0:0:1.013'), subtime('01:01:11.00', '0:0:1'), subtime('2020-01-01 01:01:11.12', '0:0:1'), subtime('2020-01-01 01:01:11.12', '0:0:1.120000');")
	result.Check(testkit.Events("01:01:09.987000 01:01:10 2020-01-01 01:01:10.120000 2020-01-01 01:01:10"))
	result = tk.MustQuery("select subtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time)), subtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time(5)))")
	result.Check(testkit.Events("2001-01-10 23:59:59.000 2001-01-10 23:59:59.00000"))
	result = tk.MustQuery("select subtime(cast('01:01:11.00' as date), cast('00:00:01' as time));")
	result.Check(testkit.Events("-00:00:01"))
	result = tk.MustQuery("select subtime(a, b), subtime(cast(a as date), b), subtime(b,a), subtime(a,c), subtime(b," +
		"c), subtime(c,a), subtime(c,b) from t;")
	result.Check(testkit.Events("<nil> <nil> <nil> 2020-01-01 11:29:30 2020-01-01 11:29:30 <nil> <nil>"))
	tk.MustQuery("select subtime(cast('10:10:10' as time), cast('9:10:10' as time))").Check(testkit.Events("01:00:00"))
	tk.MustQuery("select subtime('10:10:10', cast('9:10:10' as time))").Check(testkit.Events("01:00:00"))

	// ADDTIME & SUBTIME issue #5966
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a datetime, b timestamp, c time, d date, e bit(1))")
	tk.MustExec(`insert into t values("2020-01-01 12:30:31", "2020-01-01 12:30:31", "01:01:01", "2020-01-01", 0b1)`)

	result = tk.MustQuery("select addtime(a, e), addtime(b, e), addtime(c, e), addtime(d, e) from t")
	result.Check(testkit.Events("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select addtime('2020-01-01 01:01:01', 0b1), addtime('2020-01-01', b'1'), addtime('01:01:01', 0b1011)")
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	result = tk.MustQuery("select addtime('2020-01-01', 1), addtime('2020-01-01 01:01:01', 1), addtime(cast('2020-01-01' as date), 1)")
	result.Check(testkit.Events("2020-01-01 00:00:01 2020-01-01 01:01:02 00:00:01"))
	result = tk.MustQuery("select subtime(a, e), subtime(b, e), subtime(c, e), subtime(d, e) from t")
	result.Check(testkit.Events("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select subtime('2020-01-01 01:01:01', 0b1), subtime('2020-01-01', b'1'), subtime('01:01:01', 0b1011)")
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	result = tk.MustQuery("select subtime('2020-01-01', 1), subtime('2020-01-01 01:01:01', 1), subtime(cast('2020-01-01' as date), 1)")
	result.Check(testkit.Events("2020-12-31 23:59:59 2020-01-01 01:01:00 -00:00:01"))

	result = tk.MustQuery("select addtime(-32073, 0), addtime(0, -32073);")
	result.Check(testkit.Events("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select addtime(-32073, c), addtime(c, -32073) from t;")
	result.Check(testkit.Events("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select addtime(a, -32073), addtime(b, -32073), addtime(d, -32073) from t;")
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))

	result = tk.MustQuery("select subtime(-32073, 0), subtime(0, -32073);")
	result.Check(testkit.Events("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select subtime(-32073, c), subtime(c, -32073) from t;")
	result.Check(testkit.Events("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select subtime(a, -32073), subtime(b, -32073), subtime(d, -32073) from t;")
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))

	// fixed issue #3986
	tk.MustExec("SET ALLEGROSQL_MODE='NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("SET TIME_ZONE='+03:00';")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t (ix TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UFIDelATE CURRENT_TIMESTAMP);")
	tk.MustExec("INSERT INTO t VALUES (0), (20030101010160), (20030101016001), (20030101240101), (20030132010101), (20031301010101), (20031200000000), (20030000000000);")
	result = tk.MustQuery("SELECT CAST(ix AS SIGNED) FROM t;")
	result.Check(testkit.Events("0", "0", "0", "0", "0", "0", "0", "0"))

	// test time
	result = tk.MustQuery("select time('2003-12-31 01:02:03')")
	result.Check(testkit.Events("01:02:03"))
	result = tk.MustQuery("select time('2003-12-31 01:02:03.000123')")
	result.Check(testkit.Events("01:02:03.000123"))
	result = tk.MustQuery("select time('01:02:03.000123')")
	result.Check(testkit.Events("01:02:03.000123"))
	result = tk.MustQuery("select time('01:02:03')")
	result.Check(testkit.Events("01:02:03"))
	result = tk.MustQuery("select time('-838:59:59.000000')")
	result.Check(testkit.Events("-838:59:59.000000"))
	result = tk.MustQuery("select time('-838:59:59.000001')")
	result.Check(testkit.Events("-838:59:59.000000"))
	result = tk.MustQuery("select time('-839:59:59.000000')")
	result.Check(testkit.Events("-838:59:59.000000"))
	result = tk.MustQuery("select time('840:59:59.000000')")
	result.Check(testkit.Events("838:59:59.000000"))
	// FIXME: #issue 4193
	// result = tk.MustQuery("select time('840:59:60.000000')")
	// result.Check(testkit.Events("<nil>"))
	// result = tk.MustQuery("select time('800:59:59.9999999')")
	// result.Check(testkit.Events("801:00:00.000000"))
	// result = tk.MustQuery("select time('12003-12-10 01:02:03.000123')")
	// result.Check(testkit.Events("<nil>")
	// result = tk.MustQuery("select time('')")
	// result.Check(testkit.Events("<nil>")
	// result = tk.MustQuery("select time('2003-12-10-10 01:02:03.000123')")
	// result.Check(testkit.Events("00:20:03")

	//for hour
	result = tk.MustQuery(`SELECT hour("12:13:14.123456"), hour("12:13:14.000010"), hour("272:59:55"), hour(020005), hour(null), hour("27aaaa2:59:55");`)
	result.Check(testkit.Events("12 12 272 2 <nil> <nil>"))

	// for hour, issue #4340
	result = tk.MustQuery(`SELECT HOUR(20171222020005);`)
	result.Check(testkit.Events("2"))
	result = tk.MustQuery(`SELECT HOUR(20171222020005.1);`)
	result.Check(testkit.Events("2"))
	result = tk.MustQuery(`SELECT HOUR(20171222020005.1e0);`)
	result.Check(testkit.Events("2"))
	result = tk.MustQuery(`SELECT HOUR("20171222020005");`)
	result.Check(testkit.Events("2"))
	result = tk.MustQuery(`SELECT HOUR("20171222020005.1");`)
	result.Check(testkit.Events("2"))
	result = tk.MustQuery(`select hour(20171222);`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select hour(8381222);`)
	result.Check(testkit.Events("838"))
	result = tk.MustQuery(`select hour(10000000000);`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select hour(10100000000);`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select hour(10001000000);`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select hour(10101000000);`)
	result.Check(testkit.Events("0"))

	// for minute
	result = tk.MustQuery(`SELECT minute("12:13:14.123456"), minute("12:13:14.000010"), minute("272:59:55"), minute(null), minute("27aaaa2:59:55");`)
	result.Check(testkit.Events("13 13 59 <nil> <nil>"))

	// for second
	result = tk.MustQuery(`SELECT second("12:13:14.123456"), second("12:13:14.000010"), second("272:59:55"), second(null), second("27aaaa2:59:55");`)
	result.Check(testkit.Events("14 14 55 <nil> <nil>"))

	// for microsecond
	result = tk.MustQuery(`SELECT microsecond("12:00:00.123456"), microsecond("12:00:00.000010"), microsecond(null), microsecond("27aaaa2:59:55");`)
	result.Check(testkit.Events("123456 10 <nil> <nil>"))

	// for period_add
	result = tk.MustQuery(`SELECT period_add(200807, 2), period_add(200807, -2);`)
	result.Check(testkit.Events("200809 200805"))
	result = tk.MustQuery(`SELECT period_add(NULL, 2), period_add(-191, NULL), period_add(NULL, NULL), period_add(12.09, -2), period_add("200207aa", "1aa");`)
	result.Check(testkit.Events("<nil> <nil> <nil> 200010 200208"))
	for _, errPeriod := range []string{
		"period_add(0, 20)", "period_add(0, 0)", "period_add(-1, 1)", "period_add(200013, 1)", "period_add(-200012, 1)", "period_add('', '')",
	} {
		err := tk.QueryToErr(fmt.Sprintf("SELECT %v;", errPeriod))
		c.Assert(err.Error(), Equals, "[expression:1210]Incorrect arguments to period_add")
	}

	// for period_diff
	result = tk.MustQuery(`SELECT period_diff(200807, 200705), period_diff(200807, 200908);`)
	result.Check(testkit.Events("14 -13"))
	result = tk.MustQuery(`SELECT period_diff(NULL, 2), period_diff(-191, NULL), period_diff(NULL, NULL), period_diff(12.09, 2), period_diff("12aa", "11aa");`)
	result.Check(testkit.Events("<nil> <nil> <nil> 10 1"))
	for _, errPeriod := range []string{
		"period_diff(-00013,1)", "period_diff(00013,1)", "period_diff(0, 0)", "period_diff(200013, 1)", "period_diff(5612, 4513)", "period_diff('', '')",
	} {
		err := tk.QueryToErr(fmt.Sprintf("SELECT %v;", errPeriod))
		c.Assert(err.Error(), Equals, "[expression:1210]Incorrect arguments to period_diff")
	}

	// TODO: fix `CAST(xx as duration)` and release the test below:
	// result = tk.MustQuery(`SELECT hour("aaa"), hour(123456), hour(1234567);`)
	// result = tk.MustQuery(`SELECT minute("aaa"), minute(123456), minute(1234567);`)
	// result = tk.MustQuery(`SELECT second("aaa"), second(123456), second(1234567);`)
	// result = tk.MustQuery(`SELECT microsecond("aaa"), microsecond(123456), microsecond(1234567);`)

	// for time_format
	result = tk.MustQuery("SELECT TIME_FORMAT('150:02:28', '%H:%i:%s %p');")
	result.Check(testkit.Events("150:02:28 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('bad string', '%H:%i:%s %p');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(null, '%H:%i:%s %p');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(123, '%H:%i:%s %p');")
	result.Check(testkit.Events("00:01:23 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('24:00:00', '%r');")
	result.Check(testkit.Events("12:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('25:00:00', '%r');")
	result.Check(testkit.Events("01:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('24:00:00', '%l %p');")
	result.Check(testkit.Events("12 AM"))

	// for date_format
	result = tk.MustQuery(`SELECT DATE_FORMAT('2020-06-15', '%W %M %e %Y %r %y');`)
	result.Check(testkit.Events("Thursday June 15 2020 12:00:00 AM 17"))
	result = tk.MustQuery(`SELECT DATE_FORMAT(151113102019.12, '%W %M %e %Y %r %y');`)
	result.Check(testkit.Events("Friday November 13 2020 10:20:19 AM 15"))
	result = tk.MustQuery(`SELECT DATE_FORMAT('0000-00-00', '%W %M %e %Y %r %y');`)
	result.Check(testkit.Events("<nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	result = tk.MustQuery(`SELECT DATE_FORMAT('0', '%W %M %e %Y %r %y'), DATE_FORMAT('0.0', '%W %M %e %Y %r %y'), DATE_FORMAT(0, 0);`)
	result.Check(testkit.Events("<nil> <nil> 0"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Incorrect time value: '0'",
		"Warning|1292|Incorrect time value: '0.0'"))
	result = tk.MustQuery(`SELECT DATE_FORMAT(0, '%W %M %e %Y %r %y'), DATE_FORMAT(0.0, '%W %M %e %Y %r %y');`)
	result.Check(testkit.Events("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.Events())

	// for yearweek
	result = tk.MustQuery(`select yearweek("2020-12-27"), yearweek("2020-29-27"), yearweek("2020-00-27"), yearweek("2020-12-27 12:38:32"), yearweek("2020-12-27 12:38:32.1111111"), yearweek("2020-12-27 12:90:32"), yearweek("2020-12-27 89:38:32.1111111");`)
	result.Check(testkit.Events("201451 <nil> <nil> 201451 201451 <nil> <nil>"))
	result = tk.MustQuery(`select yearweek(12121), yearweek(1.00009), yearweek("aaaaa"), yearweek(""), yearweek(NULL);`)
	result.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select yearweek("0000-00-00"), yearweek("2020-01-29", "aa"), yearweek("2011-01-01", null);`)
	result.Check(testkit.Events("<nil> 201904 201052"))

	// for dayOfWeek, dayOfMonth, dayOfYear
	result = tk.MustQuery(`select dayOfWeek(null), dayOfWeek("2020-08-12"), dayOfWeek("0000-00-00"), dayOfWeek("2020-00-00"), dayOfWeek("0000-00-00 12:12:12"), dayOfWeek("2020-00-00 12:12:12")`)
	result.Check(testkit.Events("<nil> 7 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfYear(null), dayOfYear("2020-08-12"), dayOfYear("0000-00-00"), dayOfYear("2020-00-00"), dayOfYear("0000-00-00 12:12:12"), dayOfYear("2020-00-00 12:12:12")`)
	result.Check(testkit.Events("<nil> 224 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfMonth(null), dayOfMonth("2020-08-12"), dayOfMonth("0000-00-00"), dayOfMonth("2020-00-00"), dayOfMonth("0000-00-00 12:12:12"), dayOfMonth("2020-00-00 12:12:12")`)
	result.Check(testkit.Events("<nil> 12 0 0 0 0"))

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	result = tk.MustQuery(`select dayOfWeek(null), dayOfWeek("2020-08-12"), dayOfWeek("0000-00-00"), dayOfWeek("2020-00-00"), dayOfWeek("0000-00-00 12:12:12"), dayOfWeek("2020-00-00 12:12:12")`)
	result.Check(testkit.Events("<nil> 7 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfYear(null), dayOfYear("2020-08-12"), dayOfYear("0000-00-00"), dayOfYear("2020-00-00"), dayOfYear("0000-00-00 12:12:12"), dayOfYear("2020-00-00 12:12:12")`)
	result.Check(testkit.Events("<nil> 224 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfMonth(null), dayOfMonth("2020-08-12"), dayOfMonth("0000-00-00"), dayOfMonth("2020-00-00"), dayOfMonth("0000-00-00 12:12:12"), dayOfMonth("2020-00-00 12:12:12")`)
	result.Check(testkit.Events("<nil> 12 <nil> 0 0 0"))

	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a bigint)`)
	tk.MustExec(`insert into t value(1)`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	_, err = tk.Exec("insert into t value(dayOfWeek('0000-00-00'))")
	c.Assert(block.ErrTruncatedWrongValueForField.Equal(err), IsTrue, Commentf("%v", err))
	_, err = tk.Exec(`uFIDelate t set a = dayOfWeek("0000-00-00")`)
	c.Assert(types.ErrWrongValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = dayOfWeek(123)`)
	c.Assert(err, IsNil)

	_, err = tk.Exec("insert into t value(dayOfMonth('2020-00-00'))")
	c.Assert(block.ErrTruncatedWrongValueForField.Equal(err), IsTrue)
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustExec(`uFIDelate t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE';")
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`uFIDelate t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE,STRICT_TRANS_TABLES';")
	_, err = tk.Exec("insert into t value(dayOfMonth('0000-00-00'))")
	c.Assert(block.ErrTruncatedWrongValueForField.Equal(err), IsTrue)
	tk.MustExec("insert into t value(0)")
	_, err = tk.Exec(`uFIDelate t set a = dayOfMonth("0000-00-00")`)
	c.Assert(types.ErrWrongValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = dayOfMonth(123)`)
	c.Assert(err, IsNil)

	_, err = tk.Exec("insert into t value(dayOfYear('0000-00-00'))")
	c.Assert(block.ErrTruncatedWrongValueForField.Equal(err), IsTrue)
	_, err = tk.Exec(`uFIDelate t set a = dayOfYear("0000-00-00")`)
	c.Assert(types.ErrWrongValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = dayOfYear(123)`)
	c.Assert(err, IsNil)

	tk.MustExec("set sql_mode = ''")

	// for unix_timestamp
	tk.MustExec("SET time_zone = '+00:00';")
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113);")
	result.Check(testkit.Events("1447372800"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(20151113);")
	result.Check(testkit.Events("1447372800"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019);")
	result.Check(testkit.Events("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019e0);")
	result.Check(testkit.Events("1447410019.000000"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(15111310201912e-2);")
	result.Check(testkit.Events("1447410019.120000"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019.12);")
	result.Check(testkit.Events("1447410019.12"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019.1234567);")
	result.Check(testkit.Events("1447410019.123457"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(20151113102019);")
	result.Check(testkit.Events("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2020-11-13 10:20:19');")
	result.Check(testkit.Events("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2020-11-13 10:20:19.012');")
	result.Check(testkit.Events("1447410019.012"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1969-12-31 23:59:59');")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-13-01 00:00:00');")
	// FIXME: MyALLEGROSQL returns 0 here.
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:07.999999');")
	result.Check(testkit.Events("2147483647.999999"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:08');")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(0);")
	result.Check(testkit.Events("0"))
	//result = tk.MustQuery("SELECT UNIX_TIMESTAMP(-1);")
	//result.Check(testkit.Events("0"))
	//result = tk.MustQuery("SELECT UNIX_TIMESTAMP(12345);")
	//result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2020-01-01')")
	result.Check(testkit.Events("1483228800"))
	// Test different time zone.
	tk.MustExec("SET time_zone = '+08:00';")
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 08:00:00');")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2020-11-13 18:20:19.012'), UNIX_TIMESTAMP('2020-11-13 18:20:19.0123');")
	result.Check(testkit.Events("1447410019.012 1447410019.0123"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 11:14:07.999999');")
	result.Check(testkit.Events("2147483647.999999"))

	result = tk.MustQuery("SELECT TIME_FORMAT('bad string', '%H:%i:%s %p');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(null, '%H:%i:%s %p');")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(123, '%H:%i:%s %p');")
	result.Check(testkit.Events("00:01:23 AM"))

	// for monthname
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a varchar(10))`)
	tk.MustExec(`insert into t value("abc")`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustExec(`uFIDelate t set a = monthname("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`uFIDelate t set a = monthname("0000-00-00")`)
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'")
	_, err = tk.Exec(`uFIDelate t set a = monthname("0000-00-00")`)
	c.Assert(types.ErrWrongValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = monthname(123)`)
	c.Assert(err, IsNil)
	result = tk.MustQuery(`select monthname("2020-12-01"), monthname("0000-00-00"), monthname("0000-01-00"), monthname("0000-01-00 00:00:00")`)
	result.Check(testkit.Events("December <nil> January January"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))

	// for dayname
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a varchar(10))`)
	tk.MustExec(`insert into t value("abc")`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	_, err = tk.Exec("insert into t value(dayname('0000-00-00'))")
	c.Assert(block.ErrTruncatedWrongValueForField.Equal(err), IsTrue)
	_, err = tk.Exec(`uFIDelate t set a = dayname("0000-00-00")`)
	c.Assert(types.ErrWrongValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = dayname(123)`)
	c.Assert(err, IsNil)
	result = tk.MustQuery(`select dayname("2020-12-01"), dayname("0000-00-00"), dayname("0000-01-00"), dayname("0000-01-00 00:00:00")`)
	result.Check(testkit.Events("Friday <nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'"))

	// for sec_to_time
	result = tk.MustQuery("select sec_to_time(NULL)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select sec_to_time(2378), sec_to_time(3864000), sec_to_time(-3864000)")
	result.Check(testkit.Events("00:39:38 838:59:59 -838:59:59"))
	result = tk.MustQuery("select sec_to_time(86401.4), sec_to_time(-86401.4), sec_to_time(864014e-1), sec_to_time(-864014e-1), sec_to_time('86401.4'), sec_to_time('-86401.4')")
	result.Check(testkit.Events("24:00:01.4 -24:00:01.4 24:00:01.400000 -24:00:01.400000 24:00:01.400000 -24:00:01.400000"))
	result = tk.MustQuery("select sec_to_time(86401.54321), sec_to_time(86401.543212345)")
	result.Check(testkit.Events("24:00:01.54321 24:00:01.543212"))
	result = tk.MustQuery("select sec_to_time('123.4'), sec_to_time('123.4567891'), sec_to_time('123')")
	result.Check(testkit.Events("00:02:03.400000 00:02:03.456789 00:02:03.000000"))

	// for time_to_sec
	result = tk.MustQuery("select time_to_sec(NULL)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select time_to_sec('22:23:00'), time_to_sec('00:39:38'), time_to_sec('23:00'), time_to_sec('00:00'), time_to_sec('00:00:00'), time_to_sec('23:59:59')")
	result.Check(testkit.Events("80580 2378 82800 0 0 86399"))
	result = tk.MustQuery("select time_to_sec('1:0'), time_to_sec('1:00'), time_to_sec('1:0:0'), time_to_sec('-02:00'), time_to_sec('-02:00:05'), time_to_sec('020005')")
	result.Check(testkit.Events("3600 3600 3600 -7200 -7205 7205"))
	result = tk.MustQuery("select time_to_sec('20171222020005'), time_to_sec(020005), time_to_sec(20171222020005), time_to_sec(171222020005)")
	result.Check(testkit.Events("7205 7205 7205 7205"))

	// for str_to_date
	result = tk.MustQuery("select str_to_date('01-01-2020', '%d-%m-%Y'), str_to_date('59:20:12 01-01-2020', '%s:%i:%H %d-%m-%Y'), str_to_date('59:20:12', '%s:%i:%H')")
	result.Check(testkit.Events("2020-01-01 2020-01-01 12:20:59 12:20:59"))
	result = tk.MustQuery("select str_to_date('aaa01-01-2020', 'aaa%d-%m-%Y'), str_to_date('59:20:12 aaa01-01-2020', '%s:%i:%H aaa%d-%m-%Y'), str_to_date('59:20:12aaa', '%s:%i:%Haaa')")
	result.Check(testkit.Events("2020-01-01 2020-01-01 12:20:59 12:20:59"))
	result = tk.MustQuery("select str_to_date('01-01-2020', '%d'), str_to_date('59', '%d-%Y')")
	// TODO: MyALLEGROSQL returns "<nil> <nil>".
	result.Check(testkit.Events("0000-00-01 <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00'"))
	result = tk.MustQuery("select str_to_date('2020-6-1', '%Y-%m-%d'), str_to_date('2020-6-1', '%Y-%c-%d'), str_to_date('59:20:1', '%s:%i:%k'), str_to_date('59:20:1', '%s:%i:%l')")
	result.Check(testkit.Events("2020-06-01 2020-06-01 01:20:59 01:20:59"))

	// for maketime
	tk.MustExec(`drop block if exists t`)
	tk.MustExec(`create block t(a double, b float, c decimal(10,4));`)
	tk.MustExec(`insert into t value(1.23, 2.34, 3.1415)`)
	result = tk.MustQuery("select maketime(1,1,a), maketime(2,2,b), maketime(3,3,c) from t;")
	result.Check(testkit.Events("01:01:01.230000 02:02:02.340000 03:03:03.1415"))
	result = tk.MustQuery("select maketime(12, 13, 14), maketime('12', '15', 30.1), maketime(0, 1, 59.1), maketime(0, 1, '59.1'), maketime(0, 1, 59.5)")
	result.Check(testkit.Events("12:13:14 12:15:30.1 00:01:59.1 00:01:59.100000 00:01:59.5"))
	result = tk.MustQuery("select maketime(12, 15, 60), maketime(12, 15, '60'), maketime(12, 60, 0), maketime(12, 15, null)")
	result.Check(testkit.Events("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select maketime('', '', ''), maketime('h', 'm', 's');")
	result.Check(testkit.Events("00:00:00.000000 00:00:00.000000"))

	// for get_format
	result = tk.MustQuery(`select GET_FORMAT(DATE,'USA'), GET_FORMAT(DATE,'JIS'), GET_FORMAT(DATE,'ISO'), GET_FORMAT(DATE,'EUR'),
	GET_FORMAT(DATE,'INTERNAL'), GET_FORMAT(DATETIME,'USA') , GET_FORMAT(DATETIME,'JIS'), GET_FORMAT(DATETIME,'ISO'),
	GET_FORMAT(DATETIME,'EUR') , GET_FORMAT(DATETIME,'INTERNAL'), GET_FORMAT(TIME,'USA') , GET_FORMAT(TIME,'JIS'),
	GET_FORMAT(TIME,'ISO'), GET_FORMAT(TIME,'EUR'), GET_FORMAT(TIME,'INTERNAL')`)
	result.Check(testkit.Events("%m.%d.%Y %Y-%m-%d %Y-%m-%d %d.%m.%Y %Y%m%d %Y-%m-%d %H.%i.%s %Y-%m-%d %H:%i:%s %Y-%m-%d %H:%i:%s %Y-%m-%d %H.%i.%s %Y%m%d%H%i%s %h:%i:%s %p %H:%i:%s %H:%i:%s %H.%i.%s %H%i%s"))

	// for convert_tz
	result = tk.MustQuery(`select convert_tz("2004-01-01 12:00:00", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00.01", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00.01234567", "+00:00", "+10:32");`)
	result.Check(testkit.Events("2004-01-01 22:32:00 2004-01-01 22:32:00.01 2004-01-01 22:32:00.012346"))
	result = tk.MustQuery(`select convert_tz(20040101, "+00:00", "+10:32"), convert_tz(20040101.01, "+00:00", "+10:32"), convert_tz(20040101.01234567, "+00:00", "+10:32");`)
	result.Check(testkit.Events("2004-01-01 10:32:00 2004-01-01 10:32:00.00 2004-01-01 10:32:00.000000"))
	result = tk.MustQuery(`select convert_tz(NULL, "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", NULL, "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", NULL);`)
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("a", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "a", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "a");`)
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "");`)
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("0", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "0", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "0");`)
	result.Check(testkit.Events("<nil> <nil> <nil>"))

	// for from_unixtime
	tk.MustExec(`set @@stochastik.time_zone = "+08:00"`)
	result = tk.MustQuery(`select from_unixtime(20170101), from_unixtime(20170101.9999999), from_unixtime(20170101.999), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x"), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x")`)
	result.Check(testkit.Events("1970-08-22 18:48:21 1970-08-22 18:48:22.000000 1970-08-22 18:48:21.999 1970 22nd August 06:48:21 1970 1970 22nd August 06:48:21 1970"))
	tk.MustExec(`set @@stochastik.time_zone = "+00:00"`)
	result = tk.MustQuery(`select from_unixtime(20170101), from_unixtime(20170101.9999999), from_unixtime(20170101.999), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x"), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x")`)
	result.Check(testkit.Events("1970-08-22 10:48:21 1970-08-22 10:48:22.000000 1970-08-22 10:48:21.999 1970 22nd August 10:48:21 1970 1970 22nd August 10:48:21 1970"))
	tk.MustExec(`set @@stochastik.time_zone = @@global.time_zone`)

	// for extract
	result = tk.MustQuery(`select extract(day from '800:12:12'), extract(hour from '800:12:12'), extract(month from 20170101), extract(day_second from '2020-01-01 12:12:12')`)
	result.Check(testkit.Events("12 800 1 1121212"))

	// for adddate, subdate
	dateArithmeticalTests := []struct {
		Date      string
		Interval  string
		Unit      string
		AddResult string
		SubResult string
	}{
		{"\"2011-11-11\"", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"NULL", "1", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "NULL", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11 10:10:10\"", "1000", "MICROSECOND", "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "SECOND", "2011-11-11 10:10:20", "2011-11-11 10:10:00"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "MINUTE", "2011-11-11 10:20:10", "2011-11-11 10:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "HOUR", "2011-11-11 20:10:10", "2011-11-11 00:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11\"", "DAY", "2011-11-22 10:10:10", "2011-10-31 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "WEEK", "2011-11-25 10:10:10", "2011-10-28 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "MONTH", "2012-01-11 10:10:10", "2011-09-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"4\"", "QUARTER", "2012-11-11 10:10:10", "2010-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "YEAR", "2020-11-11 10:10:10", "2009-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"10.00100000\"", "SECOND_MICROSECOND", "2011-11-11 10:10:20.100000", "2011-11-11 10:09:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10.0010000000\"", "SECOND_MICROSECOND", "2011-11-11 10:10:30", "2011-11-11 10:09:50"},
		{"\"2011-11-11 10:10:10\"", "\"10.0010000010\"", "SECOND_MICROSECOND", "2011-11-11 10:10:30.000010", "2011-11-11 10:09:49.999990"},
		{"\"2011-11-11 10:10:10\"", "\"10:10.100\"", "MINUTE_MICROSECOND", "2011-11-11 10:20:20.100000", "2011-11-11 09:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10:10\"", "MINUTE_SECOND", "2011-11-11 10:20:20", "2011-11-11 10:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"10:10:10.100\"", "HOUR_MICROSECOND", "2011-11-11 20:20:20.100000", "2011-11-10 23:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10:10:10\"", "HOUR_SECOND", "2011-11-11 20:20:20", "2011-11-11 00:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"10:10\"", "HOUR_MINUTE", "2011-11-11 20:20:10", "2011-11-11 00:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10:10.100\"", "DAY_MICROSECOND", "2011-11-22 20:20:20.100000", "2011-10-30 23:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10:10\"", "DAY_SECOND", "2011-11-22 20:20:20", "2011-10-31 00:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10\"", "DAY_MINUTE", "2011-11-22 20:20:10", "2011-10-31 00:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"11 10\"", "DAY_HOUR", "2011-11-22 20:10:10", "2011-10-31 00:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11-1\"", "YEAR_MONTH", "2022-12-11 10:10:10", "2000-10-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11-11\"", "YEAR_MONTH", "2023-10-11 10:10:10", "1999-12-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20\"", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "19.88", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"19.88\"", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"prefix19suffix\"", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20-11\"", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20,11\"", "daY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"1000\"", "dAy", "2020-08-07 10:10:10", "2009-02-14 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"true\"", "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "true", "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10"},
		{"\"2011-11-11\"", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"\"2011-11-11\"", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"\"2011-11-11\"", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"\"2011-11-11\"", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11\"", "\"10:10\"", "HOUR_MINUTE", "2011-11-11 10:10:00", "2011-11-10 13:50:00"},
		{"\"2011-11-11\"", "\"10:10:10\"", "HOUR_SECOND", "2011-11-11 10:10:10", "2011-11-10 13:49:50"},
		{"\"2011-11-11\"", "\"10:10:10.101010\"", "HOUR_MICROSECOND", "2011-11-11 10:10:10.101010", "2011-11-10 13:49:49.898990"},
		{"\"2011-11-11\"", "\"10:10\"", "MINUTE_SECOND", "2011-11-11 00:10:10", "2011-11-10 23:49:50"},
		{"\"2011-11-11\"", "\"10:10.101010\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.101010", "2011-11-10 23:49:49.898990"},
		{"\"2011-11-11\"", "\"10.101010\"", "SECOND_MICROSECOND", "2011-11-11 00:00:10.101010", "2011-11-10 23:59:49.898990"},
		{"\"2011-11-11 00:00:00\"", "1", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"\"2011-11-11 00:00:00\"", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"\"2011-11-11 00:00:00\"", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"\"2011-11-11 00:00:00\"", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"\"2011-11-11\"", "\"abc1000\"", "MICROSECOND", "2011-11-11 00:00:00", "2011-11-11 00:00:00"},
		{"\"20111111 10:10:10\"", "\"1\"", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "\"10\"", "SECOND_MICROSECOND", "2011-11-11 00:00:00.100000", "2011-11-10 23:59:59.900000"},
		{"\"2011-11-11\"", "\"10.0000\"", "MINUTE_MICROSECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11\"", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},

		{"cast(\"2011-11-11\" as datetime)", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "1", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"1\"", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11\" as date)", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11 00:00:00\" as date)", "\"1\"", "DAY", "2011-11-12", "2011-11-10"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		// interval decimal support
		{"\"2011-01-01 00:00:00\"", "10.10", "YEAR_MONTH", "2021-11-01 00:00:00", "2000-03-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_HOUR", "2011-01-11 10:00:00", "2010-12-21 14:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_MINUTE", "2011-01-01 10:10:00", "2010-12-31 13:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_MINUTE", "2011-01-01 10:10:00", "2010-12-31 13:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "SECOND_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "YEAR", "2021-01-01 00:00:00", "2001-01-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "QUARTER", "2020-07-01 00:00:00", "2008-07-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MONTH", "2011-11-01 00:00:00", "2010-03-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "WEEK", "2011-03-12 00:00:00", "2010-10-23 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY", "2011-01-11 00:00:00", "2010-12-22 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR", "2011-01-01 10:00:00", "2010-12-31 14:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE", "2011-01-01 00:10:00", "2010-12-31 23:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "SECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MICROSECOND", "2011-01-01 00:00:00.000010", "2010-12-31 23:59:59.999990"},
		{"\"2011-01-01 00:00:00\"", "10.90", "MICROSECOND", "2011-01-01 00:00:00.000011", "2010-12-31 23:59:59.999989"},

		{"\"2009-01-01\"", "6/4", "HOUR_MINUTE", "2009-01-04 12:20:00", "2008-12-28 11:40:00"},
		{"\"2009-01-01\"", "6/0", "HOUR_MINUTE", "<nil>", "<nil>"},
		{"\"1970-01-01 12:00:00\"", "CAST(6/4 AS DECIMAL(3,1))", "HOUR_MINUTE", "1970-01-01 13:05:00", "1970-01-01 10:55:00"},
		//for issue #8077
		{"\"2012-01-02\"", "\"prefix8\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"prefix8prefix\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"8:00\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"8:00:00\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
	}
	for _, tc := range dateArithmeticalTests {
		addDate := fmt.Sprintf("select adddate(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		subDate := fmt.Sprintf("select subdate(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		result = tk.MustQuery(addDate)
		result.Check(testkit.Events(tc.AddResult))
		result = tk.MustQuery(subDate)
		result.Check(testkit.Events(tc.SubResult))
	}
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast(1 as decimal))`).Check(testkit.Events("2000-01-31 00:00:00"))
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast(null as decimal))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select subdate(cast(null as datetime), cast(1 as decimal))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast("xxx" as decimal))`).Check(testkit.Events("2000-02-01 00:00:00"))
	tk.MustQuery(`select subdate(cast("xxx" as datetime), cast(1 as decimal))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast("1" as decimal))`).Check(testkit.Events("1999-12-31"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast("xxx" as decimal))`).Check(testkit.Events("2000-01-01"))
	tk.MustQuery(`select subdate(cast("abc" as SIGNED), cast("1" as decimal))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select subdate(cast(null as SIGNED), cast("1" as decimal))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast(null as decimal))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(1 as decimal))`).Check(testkit.Events("2000-02-02 00:00:00"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(null as decimal))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select adddate(cast(null as datetime), cast(1 as decimal))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast("xxx" as decimal))`).Check(testkit.Events("2000-02-01 00:00:00"))
	tk.MustQuery(`select adddate(cast("xxx" as datetime), cast(1 as decimal))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(1 as SIGNED))`).Check(testkit.Events("2000-02-02 00:00:00"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(null as SIGNED))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select adddate(cast(null as datetime), cast(1 as SIGNED))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast("xxx" as SIGNED))`).Check(testkit.Events("2000-02-01 00:00:00"))
	tk.MustQuery(`select adddate(cast("xxx" as datetime), cast(1 as SIGNED))`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select adddate(20100101, cast(1 as decimal))`).Check(testkit.Events("2010-01-02"))
	tk.MustQuery(`select adddate(cast('10:10:10' as time), 1)`).Check(testkit.Events("34:10:10"))
	tk.MustQuery(`select adddate(cast('10:10:10' as time), cast(1 as decimal))`).Check(testkit.Events("34:10:10"))

	// for localtime, localtimestamp
	result = tk.MustQuery(`select localtime() = now(), localtime = now(), localtimestamp() = now(), localtimestamp = now()`)
	result.Check(testkit.Events("1 1 1 1"))

	// for current_timestamp, current_timestamp()
	result = tk.MustQuery(`select current_timestamp() = now(), current_timestamp = now()`)
	result.Check(testkit.Events("1 1"))

	// for milevadb_parse_tso
	tk.MustExec("SET time_zone = '+00:00';")
	result = tk.MustQuery(`select milevadb_parse_tso(404411537129996288)`)
	result.Check(testkit.Events("2020-11-20 09:53:04.877000"))
	result = tk.MustQuery(`select milevadb_parse_tso("404411537129996288")`)
	result.Check(testkit.Events("2020-11-20 09:53:04.877000"))
	result = tk.MustQuery(`select milevadb_parse_tso(1)`)
	result.Check(testkit.Events("1970-01-01 00:00:00.000000"))
	result = tk.MustQuery(`select milevadb_parse_tso(0)`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select milevadb_parse_tso(-1)`)
	result.Check(testkit.Events("<nil>"))

	// fix issue 10308
	result = tk.MustQuery("select time(\"- -\");")
	result.Check(testkit.Events("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect time value: '- -'"))
	result = tk.MustQuery("select time(\"---1\");")
	result.Check(testkit.Events("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect time value: '---1'"))
	result = tk.MustQuery("select time(\"-- --1\");")
	result.Check(testkit.Events("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect time value: '-- --1'"))
}

func (s *testIntegrationSuite) TestOpBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	// for logicAnd
	result := tk.MustQuery("select 1 && 1, 1 && 0, 0 && 1, 0 && 0, 2 && -1, null && 1, '1a' && 'a'")
	result.Check(testkit.Events("1 0 0 0 1 <nil> 0"))
	// for bitNeg
	result = tk.MustQuery("select ~123, ~-123, ~null")
	result.Check(testkit.Events("18446744073709551492 122 <nil>"))
	// for logicNot
	result = tk.MustQuery("select !1, !123, !0, !null")
	result.Check(testkit.Events("0 0 1 <nil>"))
	// for logicalXor
	result = tk.MustQuery("select 1 xor 1, 1 xor 0, 0 xor 1, 0 xor 0, 2 xor -1, null xor 1, '1a' xor 'a'")
	result.Check(testkit.Events("0 1 1 0 0 <nil> 1"))
	// for bitAnd
	result = tk.MustQuery("select 123 & 321, -123 & 321, null & 1")
	result.Check(testkit.Events("65 257 <nil>"))
	// for bitOr
	result = tk.MustQuery("select 123 | 321, -123 | 321, null | 1")
	result.Check(testkit.Events("379 18446744073709551557 <nil>"))
	// for bitXor
	result = tk.MustQuery("select 123 ^ 321, -123 ^ 321, null ^ 1")
	result.Check(testkit.Events("314 18446744073709551300 <nil>"))
	// for leftShift
	result = tk.MustQuery("select 123 << 2, -123 << 2, null << 1")
	result.Check(testkit.Events("492 18446744073709551124 <nil>"))
	// for rightShift
	result = tk.MustQuery("select 123 >> 2, -123 >> 2, null >> 1")
	result.Check(testkit.Events("30 4611686018427387873 <nil>"))
	// for logicOr
	result = tk.MustQuery("select 1 || 1, 1 || 0, 0 || 1, 0 || 0, 2 || -1, null || 1, '1a' || 'a'")
	result.Check(testkit.Events("1 1 1 0 1 1 1"))
	// for unaryPlus
	result = tk.MustQuery(`select +1, +0, +(-9), +(-0.001), +0.999, +null, +"aaa"`)
	result.Check(testkit.Events("1 0 -9 -0.001 0.999 <nil> aaa"))
	// for unaryMinus
	tk.MustExec("drop block if exists f")
	tk.MustExec("create block f(a decimal(65,0))")
	tk.MustExec("insert into f value (-17000000000000000000)")
	result = tk.MustQuery("select a from f")
	result.Check(testkit.Events("-17000000000000000000"))
}

func (s *testIntegrationSuite) TestDatetimeOverflow(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("create block t1 (d date)")
	tk.MustExec("set sql_mode='traditional'")
	overflowALLEGROSQLs := []string{
		"insert into t1 (d) select date_add('2000-01-01',interval 8000 year)",
		"insert into t1 (d) select date_sub('2000-01-01', INTERVAL 2001 YEAR)",
		"insert into t1 (d) select date_add('9999-12-31',interval 1 year)",
		"insert into t1 (d) select date_sub('1000-01-01', INTERVAL 1 YEAR)",
		"insert into t1 (d) select date_add('9999-12-31',interval 1 day)",
		"insert into t1 (d) select date_sub('1000-01-01', INTERVAL 1 day)",
		"insert into t1 (d) select date_sub('1000-01-01', INTERVAL 1 second)",
	}

	for _, allegrosql := range overflowALLEGROSQLs {
		_, err := tk.Exec(allegrosql)
		c.Assert(err.Error(), Equals, "[types:1441]Datetime function: datetime field overflow")
	}

	tk.MustExec("set sql_mode=''")
	for _, allegrosql := range overflowALLEGROSQLs {
		tk.MustExec(allegrosql)
	}

	rows := make([]string, 0, len(overflowALLEGROSQLs))
	for range overflowALLEGROSQLs {
		rows = append(rows, "<nil>")
	}
	tk.MustQuery("select * from t1").Check(testkit.Events(rows...))

	//Fix ISSUE 11256
	tk.MustQuery(`select DATE_ADD('2000-04-13 07:17:02',INTERVAL -1465647104 YEAR);`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select DATE_ADD('2008-11-23 22:47:31',INTERVAL 266076160 QUARTER);`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select DATE_SUB('2000-04-13 07:17:02',INTERVAL 1465647104 YEAR);`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select DATE_SUB('2008-11-23 22:47:31',INTERVAL -266076160 QUARTER);`).Check(testkit.Events("<nil>"))
}

func (s *testIntegrationSuite2) TestBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	ctx := context.Background()

	// for is true && is false
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b int, index idx_b (b))")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (2, 2)")
	tk.MustExec("insert t values (3, 2)")
	result := tk.MustQuery("select * from t where b is true")
	result.Check(testkit.Events("1 1", "2 2", "3 2"))
	result = tk.MustQuery("select all + a from t where a = 1")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select * from t where a is false")
	result.Check(nil)
	result = tk.MustQuery("select * from t where a is not true")
	result.Check(nil)
	result = tk.MustQuery(`select 1 is true, 0 is true, null is true, "aaa" is true, "" is true, -12.00 is true, 0.0 is true, 0.0000001 is true;`)
	result.Check(testkit.Events("1 0 0 0 0 1 0 1"))
	result = tk.MustQuery(`select 1 is false, 0 is false, null is false, "aaa" is false, "" is false, -12.00 is false, 0.0 is false, 0.0000001 is false;`)
	result.Check(testkit.Events("0 1 0 1 1 0 1 0"))

	// for in
	result = tk.MustQuery("select * from t where b in (a)")
	result.Check(testkit.Events("1 1", "2 2"))
	result = tk.MustQuery("select * from t where b not in (a)")
	result.Check(testkit.Events("3 2"))

	// test cast
	result = tk.MustQuery("select cast(1 as decimal(3,2))")
	result.Check(testkit.Events("1.00"))
	result = tk.MustQuery("select cast('1991-09-05 11:11:11' as datetime)")
	result.Check(testkit.Events("1991-09-05 11:11:11"))
	result = tk.MustQuery("select cast(cast('1991-09-05 11:11:11' as datetime) as char)")
	result.Check(testkit.Events("1991-09-05 11:11:11"))
	result = tk.MustQuery("select cast('11:11:11' as time)")
	result.Check(testkit.Events("11:11:11"))
	result = tk.MustQuery("select * from t where a > cast(2 as decimal)")
	result.Check(testkit.Events("3 2"))
	result = tk.MustQuery("select cast(-1 as unsigned)")
	result.Check(testkit.Events("18446744073709551615"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a decimal(3, 1), b double, c datetime, d time, e int)")
	tk.MustExec("insert into t value(12.3, 1.23, '2020-01-01 12:12:12', '12:12:12', 123)")
	result = tk.MustQuery("select cast(a as json), cast(b as json), cast(c as json), cast(d as json), cast(e as json) from t")
	result.Check(testkit.Events(`12.3 1.23 "2020-01-01 12:12:12.000000" "12:12:12.000000" 123`))
	result = tk.MustQuery(`select cast(10101000000 as time);`)
	result.Check(testkit.Events("00:00:00"))
	result = tk.MustQuery(`select cast(10101001000 as time);`)
	result.Check(testkit.Events("00:10:00"))
	result = tk.MustQuery(`select cast(10000000000 as time);`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select cast(20171222020005 as time);`)
	result.Check(testkit.Events("02:00:05"))
	result = tk.MustQuery(`select cast(8380000 as time);`)
	result.Check(testkit.Events("838:00:00"))
	result = tk.MustQuery(`select cast(8390000 as time);`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select cast(8386000 as time);`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select cast(8385960 as time);`)
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery(`select cast(cast('2020-01-01 01:01:11.12' as date) as datetime(2));`)
	result.Check(testkit.Events("2020-01-01 00:00:00.00"))
	result = tk.MustQuery(`select cast(20170118.999 as datetime);`)
	result.Check(testkit.Events("2020-01-18 00:00:00"))
	tk.MustQuery(`select convert(a2.a, unsigned int) from (select cast('"9223372036854775808"' as json) as a) as a2;`)

	tk.MustExec(`create block tb5(a bigint(64) unsigned, b double);`)
	tk.MustExec(`insert into tb5 (a, b) values (9223372036854776000, 9223372036854776000);`)
	tk.MustExec(`insert into tb5 (a, b) select * from (select cast(a as json) as a1, b from tb5) as t where t.a1 = t.b;`)
	tk.MustExec(`drop block tb5;`)

	tk.MustExec(`create block tb5(a float(64));`)
	tk.MustExec(`insert into tb5(a) values (13835058055282163712);`)
	tk.MustQuery(`select convert(t.a1, signed int) from (select convert(a, json) as a1 from tb5) as t`)
	tk.MustExec(`drop block tb5;`)

	// test builtinCastIntAsIntSig
	// Cast MaxUint64 to unsigned should be -1
	tk.MustQuery("select cast(0xffffffffffffffff as signed);").Check(testkit.Events("-1"))
	tk.MustQuery("select cast(0x9999999999999999999999999999999999999999999 as signed);").Check(testkit.Events("-1"))
	tk.MustExec("create block tb5(a bigint);")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert into tb5(a) values (0xfffffffffffffffffffffffff);")
	tk.MustQuery("select * from tb5;").Check(testkit.Events("9223372036854775807"))
	tk.MustExec("drop block tb5;")

	tk.MustExec(`create block tb5(a double);`)
	tk.MustExec(`insert into test.tb5 (a) values (18446744073709551616);`)
	tk.MustExec(`insert into test.tb5 (a) values (184467440737095516160);`)
	result = tk.MustQuery(`select cast(a as unsigned) from test.tb5;`)
	// Note: MyALLEGROSQL will return 9223372036854775807, and it should be a bug.
	result.Check(testkit.Events("18446744073709551615", "18446744073709551615"))
	tk.MustExec(`drop block tb5;`)

	// test builtinCastIntAsDecimalSig
	tk.MustExec(`create block tb5(a bigint(64) unsigned, b decimal(64, 10));`)
	tk.MustExec(`insert into tb5 (a, b) values (9223372036854775808, 9223372036854775808);`)
	tk.MustExec(`insert into tb5 (select * from tb5 where a = b);`)
	result = tk.MustQuery(`select * from tb5;`)
	result.Check(testkit.Events("9223372036854775808 9223372036854775808.0000000000", "9223372036854775808 9223372036854775808.0000000000"))
	tk.MustExec(`drop block tb5;`)

	// test builtinCastIntAsRealSig
	tk.MustExec(`create block tb5(a bigint(64) unsigned, b double(64, 10));`)
	tk.MustExec(`insert into tb5 (a, b) values (13835058000000000000, 13835058000000000000);`)
	tk.MustExec(`insert into tb5 (select * from tb5 where a = b);`)
	result = tk.MustQuery(`select * from tb5;`)
	result.Check(testkit.Events("13835058000000000000 13835058000000000000", "13835058000000000000 13835058000000000000"))
	tk.MustExec(`drop block tb5;`)

	// test builtinCastRealAsIntSig
	tk.MustExec(`create block tb5(a double, b float);`)
	tk.MustExec(`insert into tb5 (a, b) values (184467440737095516160, 184467440737095516160);`)
	tk.MustQuery(`select * from tb5 where cast(a as unsigned int)=0;`).Check(testkit.Events())
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1690 constant 1.844674407370955e+20 overflows bigint"))
	_ = tk.MustQuery(`select * from tb5 where cast(b as unsigned int)=0;`)
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1690 constant 1.844674407370955e+20 overflows bigint"))
	tk.MustExec(`drop block tb5;`)
	tk.MustExec(`create block tb5(a double, b bigint unsigned);`)
	tk.MustExec(`insert into tb5 (a, b) values (18446744073709551616, 18446744073709551615);`)
	_ = tk.MustQuery(`select * from tb5 where cast(a as unsigned int)=b;`)
	// TODO `obtained string = "[18446744073709552000 18446744073709551615]`
	// result.Check(testkit.Events("18446744073709551616 18446744073709551615"))
	tk.MustQuery("show warnings;").Check(testkit.Events())
	tk.MustExec(`drop block tb5;`)

	// test builtinCastJSONAsIntSig
	tk.MustExec(`create block tb5(a json, b bigint unsigned);`)
	tk.MustExec(`insert into tb5 (a, b) values ('184467440737095516160', 18446744073709551615);`)
	_ = tk.MustQuery(`select * from tb5 where cast(a as unsigned int)=b;`)
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1690 constant 1.844674407370955e+20 overflows bigint"))
	_ = tk.MustQuery(`select * from tb5 where cast(b as unsigned int)=0;`)
	tk.MustQuery("show warnings;").Check(testkit.Events())
	tk.MustExec(`drop block tb5;`)
	tk.MustExec(`create block tb5(a json, b bigint unsigned);`)
	tk.MustExec(`insert into tb5 (a, b) values ('92233720368547758080', 18446744073709551615);`)
	_ = tk.MustQuery(`select * from tb5 where cast(a as signed int)=b;`)
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1690 constant 9.223372036854776e+19 overflows bigint"))
	tk.MustExec(`drop block tb5;`)

	// test builtinCastIntAsStringSig
	tk.MustExec(`create block tb5(a bigint(64) unsigned,b varchar(50));`)
	tk.MustExec(`insert into tb5(a, b) values (9223372036854775808, '9223372036854775808');`)
	tk.MustExec(`insert into tb5(select * from tb5 where a = b);`)
	result = tk.MustQuery(`select * from tb5;`)
	result.Check(testkit.Events("9223372036854775808 9223372036854775808", "9223372036854775808 9223372036854775808"))
	tk.MustExec(`drop block tb5;`)

	// test builtinCastIntAsDecimalSig
	tk.MustExec(`drop block if exists tb5`)
	tk.MustExec(`create block tb5 (a decimal(65), b bigint(64) unsigned);`)
	tk.MustExec(`insert into tb5 (a, b) values (9223372036854775808, 9223372036854775808);`)
	result = tk.MustQuery(`select cast(b as decimal(64)) from tb5 union all select b from tb5;`)
	result.Check(testkit.Events("9223372036854775808", "9223372036854775808"))
	tk.MustExec(`drop block tb5`)

	// test builtinCastIntAsRealSig
	tk.MustExec(`drop block if exists tb5`)
	tk.MustExec(`create block tb5 (a bigint(64) unsigned, b double(64, 10));`)
	tk.MustExec(`insert into tb5 (a, b) values (9223372036854775808, 9223372036854775808);`)
	result = tk.MustQuery(`select a from tb5 where a = b union all select b from tb5;`)
	result.Check(testkit.Events("9223372036854776000", "9223372036854776000"))
	tk.MustExec(`drop block tb5`)

	// Test corner cases of cast string as datetime
	result = tk.MustQuery(`select cast("170102034" as datetime);`)
	result.Check(testkit.Events("2020-01-02 03:04:00"))
	result = tk.MustQuery(`select cast("1701020304" as datetime);`)
	result.Check(testkit.Events("2020-01-02 03:04:00"))
	result = tk.MustQuery(`select cast("1701020304." as datetime);`)
	result.Check(testkit.Events("2020-01-02 03:04:00"))
	result = tk.MustQuery(`select cast("1701020304.1" as datetime);`)
	result.Check(testkit.Events("2020-01-02 03:04:01"))
	result = tk.MustQuery(`select cast("1701020304.111" as datetime);`)
	result.Check(testkit.Events("2020-01-02 03:04:11"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect datetime value: '1701020304.111'"))
	result = tk.MustQuery(`select cast("17011" as datetime);`)
	result.Check(testkit.Events("2020-01-01 00:00:00"))
	result = tk.MustQuery(`select cast("150101." as datetime);`)
	result.Check(testkit.Events("2020-01-01 00:00:00"))
	result = tk.MustQuery(`select cast("150101.a" as datetime);`)
	result.Check(testkit.Events("2020-01-01 00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect datetime value: '150101.a'"))
	result = tk.MustQuery(`select cast("150101.1a" as datetime);`)
	result.Check(testkit.Events("2020-01-01 01:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect datetime value: '150101.1a'"))
	result = tk.MustQuery(`select cast("150101.1a1" as datetime);`)
	result.Check(testkit.Events("2020-01-01 01:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect datetime value: '150101.1a1'"))
	result = tk.MustQuery(`select cast("1101010101.111" as datetime);`)
	result.Check(testkit.Events("2011-01-01 01:01:11"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect datetime value: '1101010101.111'"))
	result = tk.MustQuery(`select cast("1101010101.11aaaaa" as datetime);`)
	result.Check(testkit.Events("2011-01-01 01:01:11"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect datetime value: '1101010101.11aaaaa'"))
	result = tk.MustQuery(`select cast("1101010101.a1aaaaa" as datetime);`)
	result.Check(testkit.Events("2011-01-01 01:01:00"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect datetime value: '1101010101.a1aaaaa'"))
	result = tk.MustQuery(`select cast("1101010101.11" as datetime);`)
	result.Check(testkit.Events("2011-01-01 01:01:11"))
	tk.MustQuery("select @@warning_count;").Check(testkit.Events("0"))
	result = tk.MustQuery(`select cast("1101010101.111" as datetime);`)
	result.Check(testkit.Events("2011-01-01 01:01:11"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect datetime value: '1101010101.111'"))
	result = tk.MustQuery(`select cast("970101.111" as datetime);`)
	result.Check(testkit.Events("1997-01-01 11:01:00"))
	tk.MustQuery("select @@warning_count;").Check(testkit.Events("0"))
	result = tk.MustQuery(`select cast("970101.11111" as datetime);`)
	result.Check(testkit.Events("1997-01-01 11:11:01"))
	tk.MustQuery("select @@warning_count;").Check(testkit.Events("0"))
	result = tk.MustQuery(`select cast("970101.111a1" as datetime);`)
	result.Check(testkit.Events("1997-01-01 11:01:00"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect datetime value: '970101.111a1'"))

	// for ISNULL
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b int, c int, d char(10), e datetime, f float, g decimal(10, 3))")
	tk.MustExec("insert t values (1, 0, null, null, null, null, null)")
	result = tk.MustQuery("select ISNULL(a), ISNULL(b), ISNULL(c), ISNULL(d), ISNULL(e), ISNULL(f), ISNULL(g) from t")
	result.Check(testkit.Events("0 0 1 1 1 1 1"))

	// fix issue #3942
	result = tk.MustQuery("select cast('-24 100:00:00' as time);")
	result.Check(testkit.Events("-676:00:00"))
	result = tk.MustQuery("select cast('12:00:00.000000' as datetime);")
	result.Check(testkit.Events("2012-00-00 00:00:00"))
	result = tk.MustQuery("select cast('-34 100:00:00' as time);")
	result.Check(testkit.Events("-838:59:59"))

	// fix issue #4324. cast decimal/int/string to time compatibility.
	invalidTimes := []string{
		"10009010",
		"239010",
		"233070",
		"23:90:10",
		"23:30:70",
		"239010.2",
		"233070.8",
	}
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t (ix TIME);")
	tk.MustExec("SET ALLEGROSQL_MODE='';")
	for _, invalidTime := range invalidTimes {
		msg := fmt.Sprintf("Warning 1292 Truncated incorrect time value: '%s'", invalidTime)
		result = tk.MustQuery(fmt.Sprintf("select cast('%s' as time);", invalidTime))
		result.Check(testkit.Events("<nil>"))
		result = tk.MustQuery("show warnings")
		result.Check(testkit.Events(msg))
		_, err := tk.Exec(fmt.Sprintf("insert into t select cast('%s' as time);", invalidTime))
		c.Assert(err, IsNil)
		result = tk.MustQuery("show warnings")
		result.Check(testkit.Events(msg))
	}
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	for _, invalidTime := range invalidTimes {
		msg := fmt.Sprintf("Warning 1292 Truncated incorrect time value: '%s'", invalidTime)
		result = tk.MustQuery(fmt.Sprintf("select cast('%s' as time);", invalidTime))
		result.Check(testkit.Events("<nil>"))
		result = tk.MustQuery("show warnings")
		result.Check(testkit.Events(msg))
		_, err := tk.Exec(fmt.Sprintf("insert into t select cast('%s' as time);", invalidTime))
		c.Assert(err.Error(), Equals, fmt.Sprintf("[types:1292]Truncated incorrect time value: '%s'", invalidTime))
	}

	// Fix issue #3691, cast compatibility.
	result = tk.MustQuery("select cast('18446744073709551616' as unsigned);")
	result.Check(testkit.Events("18446744073709551615"))
	result = tk.MustQuery("select cast('18446744073709551616' as signed);")
	result.Check(testkit.Events("-1"))
	result = tk.MustQuery("select cast('9223372036854775808' as signed);")
	result.Check(testkit.Events("-9223372036854775808"))
	result = tk.MustQuery("select cast('9223372036854775809' as signed);")
	result.Check(testkit.Events("-9223372036854775807"))
	result = tk.MustQuery("select cast('9223372036854775807' as signed);")
	result.Check(testkit.Events("9223372036854775807"))
	result = tk.MustQuery("select cast('18446744073709551615' as signed);")
	result.Check(testkit.Events("-1"))
	result = tk.MustQuery("select cast('18446744073709551614' as signed);")
	result.Check(testkit.Events("-2"))
	result = tk.MustQuery("select cast(18446744073709551615 as unsigned);")
	result.Check(testkit.Events("18446744073709551615"))
	result = tk.MustQuery("select cast(18446744073709551616 as unsigned);")
	result.Check(testkit.Events("18446744073709551615"))
	result = tk.MustQuery("select cast(18446744073709551616 as signed);")
	result.Check(testkit.Events("9223372036854775807"))
	result = tk.MustQuery("select cast(18446744073709551617 as signed);")
	result.Check(testkit.Events("9223372036854775807"))
	result = tk.MustQuery("select cast(18446744073709551615 as signed);")
	result.Check(testkit.Events("-1"))
	result = tk.MustQuery("select cast(18446744073709551614 as signed);")
	result.Check(testkit.Events("-2"))
	result = tk.MustQuery("select cast(-18446744073709551616 as signed);")
	result.Check(testkit.Events("-9223372036854775808"))
	result = tk.MustQuery("select cast(18446744073709551614.9 as unsigned);") // Round up
	result.Check(testkit.Events("18446744073709551615"))
	result = tk.MustQuery("select cast(18446744073709551614.4 as unsigned);") // Round down
	result.Check(testkit.Events("18446744073709551614"))
	result = tk.MustQuery("select cast(-9223372036854775809 as signed);")
	result.Check(testkit.Events("-9223372036854775808"))
	result = tk.MustQuery("select cast(-9223372036854775809 as unsigned);")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("select cast(-9223372036854775808 as unsigned);")
	result.Check(testkit.Events("9223372036854775808"))
	result = tk.MustQuery("select cast('-9223372036854775809' as unsigned);")
	result.Check(testkit.Events("9223372036854775808"))
	result = tk.MustQuery("select cast('-9223372036854775807' as unsigned);")
	result.Check(testkit.Events("9223372036854775809"))
	result = tk.MustQuery("select cast('-2' as unsigned);")
	result.Check(testkit.Events("18446744073709551614"))
	result = tk.MustQuery("select cast(cast(1-2 as unsigned) as signed integer);")
	result.Check(testkit.Events("-1"))
	result = tk.MustQuery("select cast(1 as signed int)")
	result.Check(testkit.Events("1"))

	// test cast as double
	result = tk.MustQuery("select cast(1 as double)")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(cast(12345 as unsigned) as double)")
	result.Check(testkit.Events("12345"))
	result = tk.MustQuery("select cast(1.1 as double)")
	result.Check(testkit.Events("1.1"))
	result = tk.MustQuery("select cast(-1.1 as double)")
	result.Check(testkit.Events("-1.1"))
	result = tk.MustQuery("select cast('123.321' as double)")
	result.Check(testkit.Events("123.321"))
	result = tk.MustQuery("select cast('12345678901234567890' as double) = 1.2345678901234567e19")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(-1 as double)")
	result.Check(testkit.Events("-1"))
	result = tk.MustQuery("select cast(null as double)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select cast(12345678901234567890 as double) = 1.2345678901234567e19")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(cast(-1 as unsigned) as double) = 1.8446744073709552e19")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(1e100 as double) = 1e100")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(123456789012345678901234567890 as double) = 1.2345678901234568e29")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(0x12345678 as double)")
	result.Check(testkit.Events("305419896"))

	// test cast as float
	result = tk.MustQuery("select cast(1 as float)")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(cast(12345 as unsigned) as float)")
	result.Check(testkit.Events("12345"))
	result = tk.MustQuery("select cast(1.1 as float) = 1.1")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(-1.1 as float) = -1.1")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast('123.321' as float) =123.321")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast('12345678901234567890' as float) = 1.2345678901234567e19")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(-1 as float)")
	result.Check(testkit.Events("-1"))
	result = tk.MustQuery("select cast(null as float)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select cast(12345678901234567890 as float) = 1.2345678901234567e19")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(cast(-1 as unsigned) as float) = 1.8446744073709552e19")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(1e100 as float(40)) = 1e100")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(123456789012345678901234567890 as float(40)) = 1.2345678901234568e29")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(0x12345678 as float(40)) = 305419896")
	result.Check(testkit.Events("1"))

	// test cast as real
	result = tk.MustQuery("select cast(1 as real)")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(cast(12345 as unsigned) as real)")
	result.Check(testkit.Events("12345"))
	result = tk.MustQuery("select cast(1.1 as real) = 1.1")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(-1.1 as real) = -1.1")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast('123.321' as real) =123.321")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast('12345678901234567890' as real) = 1.2345678901234567e19")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(-1 as real)")
	result.Check(testkit.Events("-1"))
	result = tk.MustQuery("select cast(null as real)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select cast(12345678901234567890 as real) = 1.2345678901234567e19")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(cast(-1 as unsigned) as real) = 1.8446744073709552e19")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(1e100 as real) = 1e100")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(123456789012345678901234567890 as real) = 1.2345678901234568e29")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select cast(0x12345678 as real) = 305419896")
	result.Check(testkit.Events("1"))

	// test cast time as decimal overflow
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(s1 time);")
	tk.MustExec("insert into t1 values('11:11:11');")
	result = tk.MustQuery("select cast(s1 as decimal(7, 2)) from t1;")
	result.Check(testkit.Events("99999.99"))
	result = tk.MustQuery("select cast(s1 as decimal(8, 2)) from t1;")
	result.Check(testkit.Events("111111.00"))
	_, err := tk.Exec("insert into t1 values(cast('111111.00' as decimal(7, 2)));")
	c.Assert(err, NotNil)

	result = tk.MustQuery(`select CAST(0x8fffffffffffffff as signed) a,
	CAST(0xfffffffffffffffe as signed) b,
	CAST(0xffffffffffffffff as unsigned) c;`)
	result.Check(testkit.Events("-8070450532247928833 -2 18446744073709551615"))

	result = tk.MustQuery(`select cast("1:2:3" as TIME) = "1:02:03"`)
	result.Check(testkit.Events("0"))

	// fixed issue #3471
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a time(6));")
	tk.MustExec("insert into t value('12:59:59.999999')")
	result = tk.MustQuery("select cast(a as signed) from t")
	result.Check(testkit.Events("130000"))

	// fixed issue #3762
	result = tk.MustQuery("select -9223372036854775809;")
	result.Check(testkit.Events("-9223372036854775809"))
	result = tk.MustQuery("select --9223372036854775809;")
	result.Check(testkit.Events("9223372036854775809"))
	result = tk.MustQuery("select -9223372036854775808;")
	result.Check(testkit.Events("-9223372036854775808"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a bigint(30));")
	_, err = tk.Exec("insert into t values(-9223372036854775809)")
	c.Assert(err, NotNil)

	// test case decimal precision less than the scale.
	rs, err := tk.Exec("select cast(12.1 as decimal(3, 4));")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1427]For float(M,D), double(M,D) or decimal(M,D), M must be >= D (defCausumn '').")
	c.Assert(rs.Close(), IsNil)

	// test unhex and hex
	result = tk.MustQuery("select unhex('4D7953514C')")
	result.Check(testkit.Events("MyALLEGROSQL"))
	result = tk.MustQuery("select unhex(hex('string'))")
	result.Check(testkit.Events("string"))
	result = tk.MustQuery("select unhex('ggg')")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select unhex(-1)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select hex(unhex('1267'))")
	result.Check(testkit.Events("1267"))
	result = tk.MustQuery("select hex(unhex(1267))")
	result.Check(testkit.Events("1267"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a binary(8))")
	tk.MustExec(`insert into t values('test')`)
	result = tk.MustQuery("select hex(a) from t")
	result.Check(testkit.Events("7465737400000000"))
	result = tk.MustQuery("select unhex(a) from t")
	result.Check(testkit.Events("<nil>"))

	// select from_unixtime
	// NOTE (#17013): make from_unixtime sblock in different timezone: the result of from_unixtime
	// depends on the local time zone of the test environment, thus the result checking must
	// consider the time zone convert.
	tz := tk.Se.GetStochastikVars().StmtCtx.TimeZone
	result = tk.MustQuery("select from_unixtime(1451606400)")
	unixTime := time.Unix(1451606400, 0).In(tz).String()[:19]
	result.Check(testkit.Events(unixTime))
	result = tk.MustQuery("select from_unixtime(14516064000/10)")
	result.Check(testkit.Events(fmt.Sprintf("%s.0000", unixTime)))
	result = tk.MustQuery("select from_unixtime('14516064000'/10)")
	result.Check(testkit.Events(fmt.Sprintf("%s.000000", unixTime)))
	result = tk.MustQuery("select from_unixtime(cast(1451606400 as double))")
	result.Check(testkit.Events(fmt.Sprintf("%s.000000", unixTime)))
	result = tk.MustQuery("select from_unixtime(cast(cast(1451606400 as double) as DECIMAL))")
	result.Check(testkit.Events(unixTime))
	result = tk.MustQuery("select from_unixtime(cast(cast(1451606400 as double) as DECIMAL(65,1)))")
	result.Check(testkit.Events(fmt.Sprintf("%s.0", unixTime)))
	result = tk.MustQuery("select from_unixtime(1451606400.123456)")
	unixTime = time.Unix(1451606400, 123456000).In(tz).String()[:26]
	result.Check(testkit.Events(unixTime))
	result = tk.MustQuery("select from_unixtime(1451606400.1234567)")
	unixTime = time.Unix(1451606400, 123456700).In(tz).Round(time.Microsecond).Format("2006-01-02 15:04:05.000000")[:26]
	result.Check(testkit.Events(unixTime))
	result = tk.MustQuery("select from_unixtime(1451606400.999999)")
	unixTime = time.Unix(1451606400, 999999000).In(tz).String()[:26]
	result.Check(testkit.Events(unixTime))
	result = tk.MustQuery("select from_unixtime(1511247196661)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select from_unixtime('1451606400.123');")
	unixTime = time.Unix(1451606400, 0).In(tz).String()[:19]
	result.Check(testkit.Events(fmt.Sprintf("%s.123000", unixTime)))

	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a int);")
	tk.MustExec("insert into t value(1451606400);")
	result = tk.MustQuery("select from_unixtime(a) from t;")
	result.Check(testkit.Events(unixTime))

	// test strcmp
	result = tk.MustQuery("select strcmp('abc', 'def')")
	result.Check(testkit.Events("-1"))
	result = tk.MustQuery("select strcmp('abc', 'aba')")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select strcmp('abc', 'abc')")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("select substr(null, 1, 2)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select substr('123', null, 2)")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select substr('123', 1, null)")
	result.Check(testkit.Events("<nil>"))

	// for case
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a varchar(255), b int)")
	tk.MustExec("insert t values ('str1', 1)")
	result = tk.MustQuery("select * from t where a = case b when 1 then 'str1' when 2 then 'str2' end")
	result.Check(testkit.Events("str1 1"))
	result = tk.MustQuery("select * from t where a = case b when 1 then 'str2' when 2 then 'str3' end")
	result.Check(nil)
	tk.MustExec("insert t values ('str2', 2)")
	result = tk.MustQuery("select * from t where a = case b when 2 then 'str2' when 3 then 'str3' end")
	result.Check(testkit.Events("str2 2"))
	tk.MustExec("insert t values ('str3', 3)")
	result = tk.MustQuery("select * from t where a = case b when 4 then 'str4' when 5 then 'str5' else 'str3' end")
	result.Check(testkit.Events("str3 3"))
	result = tk.MustQuery("select * from t where a = case b when 4 then 'str4' when 5 then 'str5' else 'str6' end")
	result.Check(nil)
	result = tk.MustQuery("select * from t where a = case  when b then 'str3' when 1 then 'str1' else 'str2' end")
	result.Check(testkit.Events("str3 3"))
	tk.MustExec("delete from t")
	tk.MustExec("insert t values ('str2', 0)")
	result = tk.MustQuery("select * from t where a = case  when b then 'str3' when 0 then 'str1' else 'str2' end")
	result.Check(testkit.Events("str2 0"))
	tk.MustExec("insert t values ('str1', null)")
	result = tk.MustQuery("select * from t where a = case b when null then 'str3' when 10 then 'str1' else 'str2' end")
	result.Check(testkit.Events("str2 0"))
	result = tk.MustQuery("select * from t where a = case null when b then 'str3' when 10 then 'str1' else 'str2' end")
	result.Check(testkit.Events("str2 0"))
	tk.MustExec("insert t values (null, 4)")
	result = tk.MustQuery("select * from t where b < case a when null then 0 when 'str2' then 0 else 9 end")
	result.Check(testkit.Events("<nil> 4"))
	result = tk.MustQuery("select * from t where b = case when a is null then 4 when  a = 'str5' then 7 else 9 end")
	result.Check(testkit.Events("<nil> 4"))
	// test warnings
	tk.MustQuery("select case when b=0 then 1 else 1/b end from t")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select if(b=0, 1, 1/b) from t")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select ifnull(b, b/0) from t")
	tk.MustQuery("show warnings").Check(testkit.Events())

	tk.MustQuery("select case when 1 then 1 else 1/0 end")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery(" select if(1,1,1/0)")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select ifnull(1, 1/0)")
	tk.MustQuery("show warnings").Check(testkit.Events())

	tk.MustExec("delete from t")
	tk.MustExec("insert t values ('str2', 0)")
	tk.MustQuery("select case when b < 1 then 1 else 1/0 end from t")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select case when b < 1 then 1 when 1/0 then b else 1/0 end from t")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select if(b < 1 , 1, 1/0) from t")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select ifnull(b, 1/0) from t")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select COALESCE(1, b, b/0) from t")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select 0 and b/0 from t")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select 1 or b/0 from t")
	tk.MustQuery("show warnings").Check(testkit.Events())

	tk.MustQuery("select case 2.0 when 2.0 then 3.0 when 3.0 then 2.0 end").Check(testkit.Events("3.0"))
	tk.MustQuery("select case 2.0 when 3.0 then 2.0 when 4.0 then 3.0 else 5.0 end").Check(testkit.Events("5.0"))
	tk.MustQuery("select case cast('2011-01-01' as date) when cast('2011-01-01' as date) then cast('2011-02-02' as date) end").Check(testkit.Events("2011-02-02"))
	tk.MustQuery("select case cast('2012-01-01' as date) when cast('2011-01-01' as date) then cast('2011-02-02' as date) else cast('2011-03-03' as date) end").Check(testkit.Events("2011-03-03"))
	tk.MustQuery("select case cast('10:10:10' as time) when cast('10:10:10' as time) then cast('11:11:11' as time) end").Check(testkit.Events("11:11:11"))
	tk.MustQuery("select case cast('10:10:13' as time) when cast('10:10:10' as time) then cast('11:11:11' as time) else cast('22:22:22' as time) end").Check(testkit.Events("22:22:22"))

	// for cast
	result = tk.MustQuery("select cast(1234 as char(3))")
	result.Check(testkit.Events("123"))
	result = tk.MustQuery("select cast(1234 as char(0))")
	result.Check(testkit.Events(""))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Events("Warning 1406 Data Too Long, field len 0, data len 4"))
	result = tk.MustQuery("select CAST( - 8 AS DECIMAL ) * + 52 + 87 < - 86")
	result.Check(testkit.Events("1"))

	// for char
	result = tk.MustQuery("select char(97, 100, 256, 89)")
	result.Check(testkit.Events("ad\x01\x00Y"))
	result = tk.MustQuery("select char(97, null, 100, 256, 89)")
	result.Check(testkit.Events("ad\x01\x00Y"))
	result = tk.MustQuery("select char(97, null, 100, 256, 89 using utf8)")
	result.Check(testkit.Events("ad\x01\x00Y"))
	result = tk.MustQuery("select char(97, null, 100, 256, 89 using ascii)")
	result.Check(testkit.Events("ad\x01\x00Y"))
	err = tk.ExecToErr("select char(97, null, 100, 256, 89 using milevadb)")
	c.Assert(err.Error(), Equals, "[berolinaAllegroSQL:1115]Unknown character set: 'milevadb'")

	// issue 3884
	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (c1 date, c2 datetime, c3 timestamp, c4 time, c5 year);")
	tk.MustExec("INSERT INTO t values ('2000-01-01', '2000-01-01 12:12:12', '2000-01-01 12:12:12', '12:12:12', '2000');")
	tk.MustExec("INSERT INTO t values ('2000-02-01', '2000-02-01 12:12:12', '2000-02-01 12:12:12', '13:12:12', 2000);")
	tk.MustExec("INSERT INTO t values ('2000-03-01', '2000-03-01', '2000-03-01 12:12:12', '1 12:12:12', 2000);")
	tk.MustExec("INSERT INTO t SET c1 = '2000-04-01', c2 = '2000-04-01', c3 = '2000-04-01 12:12:12', c4 = '-1 13:12:12', c5 = 2000;")
	result = tk.MustQuery("SELECT c4 FROM t where c4 < '-13:12:12';")
	result.Check(testkit.Events("-37:12:12"))
	result = tk.MustQuery(`SELECT 1 DIV - - 28 + ( - SUM( - + 25 ) ) * - CASE - 18 WHEN 44 THEN NULL ELSE - 41 + 32 + + - 70 - + COUNT( - 95 ) * 15 END + 92`)
	result.Check(testkit.Events("2442"))

	// for regexp, rlike
	// https://github.com/whtcorpsinc/milevadb/issues/4080
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t (a char(10), b varchar(10), c binary(10), d varbinary(10));`)
	tk.MustExec(`insert into t values ('text','text','text','text');`)
	result = tk.MustQuery(`select a regexp 'xt' from t;`)
	result.Check(testkit.Events("1"))
	result = tk.MustQuery(`select b regexp 'xt' from t;`)
	result.Check(testkit.Events("1"))
	result = tk.MustQuery(`select b regexp binary 'Xt' from t;`)
	result.Check(testkit.Events("0"))
	result = tk.MustQuery(`select c regexp 'Xt' from t;`)
	result.Check(testkit.Events("0"))
	result = tk.MustQuery(`select d regexp 'Xt' from t;`)
	result.Check(testkit.Events("0"))
	result = tk.MustQuery(`select a rlike 'xt' from t;`)
	result.Check(testkit.Events("1"))
	result = tk.MustQuery(`select a rlike binary 'Xt' from t;`)
	result.Check(testkit.Events("0"))
	result = tk.MustQuery(`select b rlike 'xt' from t;`)
	result.Check(testkit.Events("1"))
	result = tk.MustQuery(`select c rlike 'Xt' from t;`)
	result.Check(testkit.Events("0"))
	result = tk.MustQuery(`select d rlike 'Xt' from t;`)
	result.Check(testkit.Events("0"))
	result = tk.MustQuery(`select 'a' regexp 'A', 'a' regexp binary 'A'`)
	result.Check(testkit.Events("0 0"))

	// testCase is for like and regexp
	type testCase struct {
		pattern string
		val     string
		result  int
	}
	patternMatching := func(c *C, tk *testkit.TestKit, queryOp string, data []testCase) {
		tk.MustExec("drop block if exists t")
		tk.MustExec("create block t (a varchar(255), b int)")
		for i, d := range data {
			tk.MustExec(fmt.Sprintf("insert into t values('%s', %d)", d.val, i))
			result = tk.MustQuery(fmt.Sprintf("select * from t where a %s '%s'", queryOp, d.pattern))
			if d.result == 1 {
				rowStr := fmt.Sprintf("%s %d", d.val, i)
				result.Check(testkit.Events(rowStr))
			} else {
				result.Check(nil)
			}
			tk.MustExec(fmt.Sprintf("delete from t where b = %d", i))
		}
	}
	// for like
	likeTests := []testCase{
		{"a", "a", 1},
		{"a", "b", 0},
		{"aA", "Aa", 0},
		{`aA%`, "aAab", 1},
		{"aA_", "Aaab", 0},
		{"Aa_", "Aab", 1},
		{"", "", 1},
		{"", "a", 0},
	}
	patternMatching(c, tk, "like", likeTests)
	// for regexp
	likeTests = []testCase{
		{"^$", "a", 0},
		{"a", "a", 1},
		{"a", "b", 0},
		{"aA", "aA", 1},
		{".", "a", 1},
		{"^.$", "ab", 0},
		{"..", "b", 0},
		{".ab", "aab", 1},
		{"ab.", "abcd", 1},
		{".*", "abcd", 1},
	}
	patternMatching(c, tk, "regexp", likeTests)

	// for #9838
	result = tk.MustQuery("select cast(1 as signed) + cast(9223372036854775807 as unsigned);")
	result.Check(testkit.Events("9223372036854775808"))
	result = tk.MustQuery("select cast(9223372036854775807 as unsigned) + cast(1 as signed);")
	result.Check(testkit.Events("9223372036854775808"))
	err = tk.QueryToErr("select cast(9223372036854775807 as signed) + cast(9223372036854775809 as unsigned);")
	c.Assert(err, NotNil)
	err = tk.QueryToErr("select cast(9223372036854775809 as unsigned) + cast(9223372036854775807 as signed);")
	c.Assert(err, NotNil)
	err = tk.QueryToErr("select cast(-9223372036854775807 as signed) + cast(9223372036854775806 as unsigned);")
	c.Assert(err, NotNil)
	err = tk.QueryToErr("select cast(9223372036854775806 as unsigned) + cast(-9223372036854775807 as signed);")
	c.Assert(err, NotNil)

	result = tk.MustQuery(`select 1 / '2007' div 1;`)
	result.Check(testkit.Events("0"))
}

func (s *testIntegrationSuite) TestInfoBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	// for last_insert_id
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (id int auto_increment, a int, PRIMARY KEY (id))")
	tk.MustExec("insert into t(a) values(1)")
	result := tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Events("1"))
	tk.MustExec("insert into t values(2, 1)")
	result = tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Events("1"))
	tk.MustExec("insert into t(a) values(1)")
	result = tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Events("3"))

	result = tk.MustQuery("select last_insert_id(5);")
	result.Check(testkit.Events("5"))
	result = tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Events("5"))

	// for found_rows
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int)")
	tk.MustQuery("select * from t") // Test XSelectBlockExec
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Events("1")) // Last query is found_rows(), it returns 1 event with value 0
	tk.MustExec("insert t values (1),(2),(2)")
	tk.MustQuery("select * from t")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Events("3"))
	tk.MustQuery("select * from t where a = 0")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Events("0"))
	tk.MustQuery("select * from t where a = 1")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Events("1"))
	tk.MustQuery("select * from t where a like '2'") // Test SelectionExec
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Events("2"))
	tk.MustQuery("show blocks like 't'")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Events("1"))
	tk.MustQuery("select count(*) from t") // Test ProjectionExec
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Events("1"))

	// for database
	result = tk.MustQuery("select database()")
	result.Check(testkit.Events("test"))
	tk.MustExec("drop database test")
	result = tk.MustQuery("select database()")
	result.Check(testkit.Events("<nil>"))
	tk.MustExec("create database test")
	tk.MustExec("use test")

	// for current_user
	stochastikVars := tk.Se.GetStochastikVars()
	originUser := stochastikVars.User
	stochastikVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "127.0.%%"}
	result = tk.MustQuery("select current_user()")
	result.Check(testkit.Events("root@127.0.%%"))
	stochastikVars.User = originUser

	// for user
	stochastikVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "127.0.%%"}
	result = tk.MustQuery("select user()")
	result.Check(testkit.Events("root@localhost"))
	stochastikVars.User = originUser

	// for connection_id
	originConnectionID := stochastikVars.ConnectionID
	stochastikVars.ConnectionID = uint64(1)
	result = tk.MustQuery("select connection_id()")
	result.Check(testkit.Events("1"))
	stochastikVars.ConnectionID = originConnectionID

	// for version
	result = tk.MustQuery("select version()")
	result.Check(testkit.Events(allegrosql.ServerVersion))

	// for row_count
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b int, PRIMARY KEY (a))")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Events("0"))
	tk.MustExec("insert into t(a, b) values(1, 11), (2, 22), (3, 33)")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Events("3"))
	tk.MustExec("select * from t")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Events("-1"))
	tk.MustExec("uFIDelate t set b=22 where a=1")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Events("1"))
	tk.MustExec("uFIDelate t set b=22 where a=1")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Events("0"))
	tk.MustExec("delete from t where a=2")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Events("-1"))

	// for benchmark
	success := testkit.Events("0")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int, b int)")
	result = tk.MustQuery(`select benchmark(3, benchmark(2, length("abc")))`)
	result.Check(success)
	err := tk.ExecToErr(`select benchmark(3, length("a", "b"))`)
	c.Assert(err, NotNil)
	// Quoted from https://dev.allegrosql.com/doc/refman/5.7/en/information-functions.html#function_benchmark
	// Although the expression can be a subquery, it must return a single defCausumn and at most a single event.
	// For example, BENCHMARK(10, (SELECT * FROM t)) will fail if the block t has more than one defCausumn or
	// more than one event.
	oneDeferredCausetQuery := "select benchmark(10, (select a from t))"
	twoDeferredCausetQuery := "select benchmark(10, (select * from t))"
	// rows * defCausumns:
	// 0 * 1, success;
	result = tk.MustQuery(oneDeferredCausetQuery)
	result.Check(success)
	// 0 * 2, error;
	err = tk.ExecToErr(twoDeferredCausetQuery)
	c.Assert(err, NotNil)
	// 1 * 1, success;
	tk.MustExec("insert t values (1, 2)")
	result = tk.MustQuery(oneDeferredCausetQuery)
	result.Check(success)
	// 1 * 2, error;
	err = tk.ExecToErr(twoDeferredCausetQuery)
	c.Assert(err, NotNil)
	// 2 * 1, error;
	tk.MustExec("insert t values (3, 4)")
	err = tk.ExecToErr(oneDeferredCausetQuery)
	c.Assert(err, NotNil)
	// 2 * 2, error.
	err = tk.ExecToErr(twoDeferredCausetQuery)
	c.Assert(err, NotNil)
}

func (s *testIntegrationSuite) TestControlBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	// for ifnull
	result := tk.MustQuery("select ifnull(1, 2)")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select ifnull(null, 2)")
	result.Check(testkit.Events("2"))
	result = tk.MustQuery("select ifnull(1, null)")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select ifnull(null, null)")
	result.Check(testkit.Events("<nil>"))

	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(a bigint not null)")
	result = tk.MustQuery("select ifnull(max(a),0) from t1")
	result.Check(testkit.Events("0"))

	tk.MustExec("drop block if exists t1")
	tk.MustExec("drop block if exists t2")
	tk.MustExec("create block t1(a decimal(20,4))")
	tk.MustExec("create block t2(a decimal(20,4))")
	tk.MustExec("insert into t1 select 1.2345")
	tk.MustExec("insert into t2 select 1.2345")

	result = tk.MustQuery(`select sum(ifnull(a, 0)) from (
	select ifnull(a, 0) as a from t1
	union all
	select ifnull(a, 0) as a from t2
	) t;`)
	result.Check(testkit.Events("2.4690"))

	// for if
	result = tk.MustQuery(`select IF(0,"ERROR","this"),IF(1,"is","ERROR"),IF(NULL,"ERROR","a"),IF(1,2,3)|0,IF(1,2.0,3.0)+0;`)
	result.Check(testkit.Events("this is a 2 2.0"))
	tk.MustExec("drop block if exists t1;")
	tk.MustExec("CREATE TABLE t1 (st varchar(255) NOT NULL, u int(11) NOT NULL);")
	tk.MustExec("INSERT INTO t1 VALUES ('a',1),('A',1),('aa',1),('AA',1),('a',1),('aaa',0),('BBB',0);")
	result = tk.MustQuery("select if(1,st,st) s from t1 order by s;")
	result.Check(testkit.Events("A", "AA", "BBB", "a", "a", "aa", "aaa"))
	result = tk.MustQuery("select if(u=1,st,st) s from t1 order by s;")
	result.Check(testkit.Events("A", "AA", "BBB", "a", "a", "aa", "aaa"))
	tk.MustExec("drop block if exists t1;")
	tk.MustExec("CREATE TABLE t1 (a varchar(255), b time, c int)")
	tk.MustExec("INSERT INTO t1 VALUE('abc', '12:00:00', 0)")
	tk.MustExec("INSERT INTO t1 VALUE('1abc', '00:00:00', 1)")
	tk.MustExec("INSERT INTO t1 VALUE('0abc', '12:59:59', 0)")
	result = tk.MustQuery("select if(a, b, c), if(b, a, c), if(c, a, b) from t1")
	result.Check(testkit.Events("0 abc 12:00:00", "00:00:00 1 1abc", "0 0abc 12:59:59"))
	result = tk.MustQuery("select if(1, 1.0, 1)")
	result.Check(testkit.Events("1.0"))
	// FIXME: MyALLEGROSQL returns `1.0`.
	result = tk.MustQuery("select if(1, 1, 1.0)")
	result.Check(testkit.Events("1"))
	tk.MustQuery("select if(count(*), cast('2000-01-01' as date), cast('2011-01-01' as date)) from t1").Check(testkit.Events("2000-01-01"))
	tk.MustQuery("select if(count(*)=0, cast('2000-01-01' as date), cast('2011-01-01' as date)) from t1").Check(testkit.Events("2011-01-01"))
	tk.MustQuery("select if(count(*), cast('[]' as json), cast('{}' as json)) from t1").Check(testkit.Events("[]"))
	tk.MustQuery("select if(count(*)=0, cast('[]' as json), cast('{}' as json)) from t1").Check(testkit.Events("{}"))

	result = tk.MustQuery("SELECT 79 + + + CASE -87 WHEN -30 THEN COALESCE(COUNT(*), +COALESCE(+15, -33, -12 ) + +72) WHEN +COALESCE(+AVG(DISTINCT(60)), 21) THEN NULL ELSE NULL END AS defCaus0;")
	result.Check(testkit.Events("<nil>"))

	result = tk.MustQuery("SELECT -63 + COALESCE ( - 83, - 61 + - + 72 * - CAST( NULL AS SIGNED ) + + 3 );")
	result.Check(testkit.Events("-146"))
}

func (s *testIntegrationSuite) TestArithmeticBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	ctx := context.Background()

	// for plus
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(1.09, 1.999), (-1.1, -0.1);")
	result := tk.MustQuery("SELECT a+b FROM t;")
	result.Check(testkit.Events("3.089", "-1.200"))
	result = tk.MustQuery("SELECT b+12, b+0.01, b+0.00001, b+12.00001 FROM t;")
	result.Check(testkit.Events("13.999 2.009 1.99901 13.99901", "11.900 -0.090 -0.09999 11.90001"))
	result = tk.MustQuery("SELECT 1+12, 21+0.01, 89+\"11\", 12+\"a\", 12+NULL, NULL+1, NULL+NULL;")
	result.Check(testkit.Events("13 21.01 100 12 <nil> <nil> <nil>"))
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT UNSIGNED, b BIGINT UNSIGNED);")
	tk.MustExec("INSERT INTO t SELECT 1<<63, 1<<63;")
	rs, err := tk.Exec("SELECT a+b FROM t;")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err := stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(test.t.a + test.t.b)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(-3 as signed) + cast(2 as unsigned);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(-3 + 2)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(2 as unsigned) + cast(-3 as signed);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(2 + -3)'")
	c.Assert(rs.Close(), IsNil)

	// for minus
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(1.09, 1.999), (-1.1, -0.1);")
	result = tk.MustQuery("SELECT a-b FROM t;")
	result.Check(testkit.Events("-0.909", "-1.000"))
	result = tk.MustQuery("SELECT b-12, b-0.01, b-0.00001, b-12.00001 FROM t;")
	result.Check(testkit.Events("-10.001 1.989 1.99899 -10.00101", "-12.100 -0.110 -0.10001 -12.10001"))
	result = tk.MustQuery("SELECT 1-12, 21-0.01, 89-\"11\", 12-\"a\", 12-NULL, NULL-1, NULL-NULL;")
	result.Check(testkit.Events("-11 20.99 78 12 <nil> <nil> <nil>"))
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT UNSIGNED, b BIGINT UNSIGNED);")
	tk.MustExec("INSERT INTO t SELECT 1, 4;")
	rs, err = tk.Exec("SELECT a-b FROM t;")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(test.t.a - test.t.b)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(-1 as signed) - cast(-1 as unsigned);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(-1 - 18446744073709551615)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(-1 as unsigned) - cast(-1 as signed);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(18446744073709551615 - -1)'")
	c.Assert(rs.Close(), IsNil)
	tk.MustQuery(`select cast(-3 as unsigned) - cast(-1 as signed);`).Check(testkit.Events("18446744073709551614"))
	tk.MustQuery("select 1.11 - 1.11;").Check(testkit.Events("0.00"))
	tk.MustExec(`create block tb5(a int(10));`)
	tk.MustExec(`insert into tb5 (a) values (10);`)
	e := tk.QueryToErr(`select * from tb5 where a - -9223372036854775808;`)
	c.Assert(e, NotNil)
	c.Assert(strings.HasSuffix(e.Error(), `BIGINT value is out of range in '(DeferredCauset#0 - -9223372036854775808)'`), IsTrue, Commentf("err: %v", err))
	tk.MustExec(`drop block tb5`)

	// for multiply
	tk.MustQuery("select 1234567890 * 1234567890").Check(testkit.Events("1524157875019052100"))
	rs, err = tk.Exec("select 1234567890 * 12345671890")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	tk.MustQuery("select cast(1234567890 as unsigned int) * 12345671890").Check(testkit.Events("15241570095869612100"))
	tk.MustQuery("select 123344532434234234267890.0 * 1234567118923479823749823749.230").Check(testkit.Events("152277104042296270209916846800130443726237424001224.7000"))
	rs, err = tk.Exec("select 123344532434234234267890.0 * 12345671189234798237498232384982309489238402830480239849238048239084749.230")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	// FIXME: There is something wrong in showing float number.
	//tk.MustQuery("select 1.797693134862315708145274237317043567981e+308 * 1").Check(testkit.Events("1.7976931348623157e308"))
	//tk.MustQuery("select 1.797693134862315708145274237317043567981e+308 * -1").Check(testkit.Events("-1.7976931348623157e308"))
	rs, err = tk.Exec("select 1.797693134862315708145274237317043567981e+308 * 1.1")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select 1.797693134862315708145274237317043567981e+308 * -1.1")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	tk.MustQuery("select 0.0 * -1;").Check(testkit.Events("0.0"))

	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(-1.09, 1.999);")
	result = tk.MustQuery("SELECT a/b, a/12, a/-0.01, b/12, b/-0.01, b/0.000, NULL/b, b/NULL, NULL/NULL FROM t;")
	result.Check(testkit.Events("-0.545273 -0.090833 109.000000 0.1665833 -199.9000000 <nil> <nil> <nil> <nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1365 Division by 0"))
	rs, err = tk.Exec("select 1e200/1e-200")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)

	// for intDiv
	result = tk.MustQuery("SELECT 13 DIV 12, 13 DIV 0.01, -13 DIV 2, 13 DIV NULL, NULL DIV 13, NULL DIV NULL;")
	result.Check(testkit.Events("1 1300 -6 <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT 2.4 div 1.1, 2.4 div 1.2, 2.4 div 1.3;")
	result.Check(testkit.Events("2 2 1"))
	result = tk.MustQuery("SELECT 1.175494351E-37 div 1.7976931348623157E+308, 1.7976931348623157E+308 div -1.7976931348623157E+307, 1 div 1e-82;")
	result.Check(testkit.Events("0 -1 <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Truncated incorrect DECIMAL value: '1.7976931348623157e+308'",
		"Warning|1292|Truncated incorrect DECIMAL value: '1.7976931348623157e+308'",
		"Warning|1292|Truncated incorrect DECIMAL value: '-1.7976931348623158e+307'",
		"Warning|1365|Division by 0"))
	rs, err = tk.Exec("select 1e300 DIV 1.5")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)

	tk.MustExec("drop block if exists t;")
	tk.MustExec("CREATE TABLE t (c_varchar varchar(255), c_time time, nonzero int, zero int, c_int_unsigned int unsigned, c_timestamp timestamp, c_enum enum('a','b','c'));")
	tk.MustExec("INSERT INTO t VALUE('abc', '12:00:00', 12, 0, 5, '2020-08-05 18:19:03', 'b');")
	result = tk.MustQuery("select c_varchar div nonzero, c_time div nonzero, c_time div zero, c_timestamp div nonzero, c_timestamp div zero, c_varchar div zero from t;")
	result.Check(testkit.Events("0 10000 <nil> 1680900431825 <nil> <nil>"))
	result = tk.MustQuery("select c_enum div nonzero from t;")
	result.Check(testkit.Events("0"))
	tk.MustQuery("select c_enum div zero from t").Check(testkit.Events("<nil>"))
	tk.MustQuery("select nonzero div zero from t").Check(testkit.Events("<nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1365 Division by 0"))
	result = tk.MustQuery("select c_time div c_enum, c_timestamp div c_time, c_timestamp div c_enum from t;")
	result.Check(testkit.Events("60000 168090043 10085402590951"))
	result = tk.MustQuery("select c_int_unsigned div nonzero, nonzero div c_int_unsigned, c_int_unsigned div zero from t;")
	result.Check(testkit.Events("0 2 <nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1365 Division by 0"))

	// for mod
	result = tk.MustQuery("SELECT CAST(1 AS UNSIGNED) MOD -9223372036854775808, -9223372036854775808 MOD CAST(1 AS UNSIGNED);")
	result.Check(testkit.Events("1 0"))
	result = tk.MustQuery("SELECT 13 MOD 12, 13 MOD 0.01, -13 MOD 2, 13 MOD NULL, NULL MOD 13, NULL DIV NULL;")
	result.Check(testkit.Events("1 0.00 -1 <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT 2.4 MOD 1.1, 2.4 MOD 1.2, 2.4 mod 1.30;")
	result.Check(testkit.Events("0.2 0.0 1.10"))
	tk.MustExec("drop block if exists t;")
	tk.MustExec("CREATE TABLE t (c_varchar varchar(255), c_time time, nonzero int, zero int, c_timestamp timestamp, c_enum enum('a','b','c'));")
	tk.MustExec("INSERT INTO t VALUE('abc', '12:00:00', 12, 0, '2020-08-05 18:19:03', 'b');")
	result = tk.MustQuery("select c_varchar MOD nonzero, c_time MOD nonzero, c_timestamp MOD nonzero, c_enum MOD nonzero from t;")
	result.Check(testkit.Events("0 0 3 2"))
	result = tk.MustQuery("select c_time MOD c_enum, c_timestamp MOD c_time, c_timestamp MOD c_enum from t;")
	result.Check(testkit.Events("0 21903 1"))
	tk.MustQuery("select c_enum MOD zero from t;").Check(testkit.Events("<nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1365 Division by 0"))
	tk.MustExec("SET ALLEGROSQL_MODE='ERROR_FOR_DIVISION_BY_ZERO,STRICT_ALL_TABLES';")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("CREATE TABLE t (v int);")
	tk.MustExec("INSERT IGNORE INTO t VALUE(12 MOD 0);")
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1365 Division by 0"))
	tk.MustQuery("select v from t;").Check(testkit.Events("<nil>"))
	tk.MustQuery("select 0.000 % 0.11234500000000000000;").Check(testkit.Events("0.00000000000000000000"))

	_, err = tk.Exec("INSERT INTO t VALUE(12 MOD 0);")
	c.Assert(terror.ErrorEqual(err, expression.ErrDivisionByZero), IsTrue)

	tk.MustQuery("select sum(1.2e2) * 0.1").Check(testkit.Events("12"))
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a double)")
	tk.MustExec("insert into t value(1.2)")
	tk.MustQuery("select sum(a) * 0.1 from t").Check(testkit.Events("0.12"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a double)")
	tk.MustExec("insert into t value(1.2)")
	result = tk.MustQuery("select * from t where a/0 > 1")
	result.Check(testkit.Events())
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1365|Division by 0"))

	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT, b DECIMAL(6, 2));")
	tk.MustExec("INSERT INTO t VALUES(0, 1.12), (1, 1.21);")
	tk.MustQuery("SELECT a/b FROM t;").Check(testkit.Events("0.0000", "0.8264"))
}

func (s *testIntegrationSuite) TestCompareBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	// compare as JSON
	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (pk int  NOT NULL PRIMARY KEY AUTO_INCREMENT, i INT, j JSON);")
	tk.MustExec(`INSERT INTO t(i, j) VALUES (0, NULL)`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (1, '{"a": 2}')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (2, '[1,2]')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (3, '{"a":"b", "c":"d","ab":"abc", "bc": ["x", "y"]}')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (4, '["here", ["I", "am"], "!!!"]')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (5, '"scalar string"')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (6, 'true')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (7, 'false')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (8, 'null')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (9, '-1')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (10, CAST(CAST(1 AS UNSIGNED) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (11, '32767')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (12, '32768')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (13, '-32768')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (14, '-32769')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (15, '2147483647')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (16, '2147483648')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (17, '-2147483648')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (18, '-2147483649')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (19, '18446744073709551615')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (20, '18446744073709551616')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (21, '3.14')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (22, '{}')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (23, '[]')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (24, CAST(CAST('2020-01-15 23:24:25' AS DATETIME) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (25, CAST(CAST('23:24:25' AS TIME) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (26, CAST(CAST('2020-01-15' AS DATE) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (27, CAST(TIMESTAMP('2020-01-15 23:24:25') AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (28, CAST('[]' AS CHAR CHARACTER SET 'ascii'))`)

	result := tk.MustQuery(`SELECT i,
		(j = '"scalar string"') AS c1,
		(j = 'scalar string') AS c2,
		(j = CAST('"scalar string"' AS JSON)) AS c3,
		(j = CAST(CAST(j AS CHAR CHARACTER SET 'utf8mb4') AS JSON)) AS c4,
		(j = CAST(NULL AS JSON)) AS c5,
		(j = NULL) AS c6,
		(j <=> NULL) AS c7,
		(j <=> CAST(NULL AS JSON)) AS c8,
		(j IN (-1, 2, 32768, 3.14)) AS c9,
		(j IN (CAST('[1, 2]' AS JSON), CAST('{}' AS JSON), CAST(3.14 AS JSON))) AS c10,
		(j = (SELECT j FROM t WHERE j = CAST('null' AS JSON))) AS c11,
		(j = (SELECT j FROM t WHERE j IS NULL)) AS c12,
		(j = (SELECT j FROM t WHERE 1<>1)) AS c13,
		(j = DATE('2020-01-15')) AS c14,
		(j = TIME('23:24:25')) AS c15,
		(j = TIMESTAMP('2020-01-15 23:24:25')) AS c16,
		(j = CURRENT_TIMESTAMP) AS c17,
		(JSON_EXTRACT(j, '$.a') = 2) AS c18
		FROM t
		ORDER BY i;`)
	result.Check(testkit.Events("0 <nil> <nil> <nil> <nil> <nil> <nil> 1 1 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
		"1 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 1",
		"2 0 0 0 1 <nil> <nil> 0 0 0 1 0 <nil> <nil> 0 0 0 0 <nil>",
		"3 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 0",
		"4 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"5 0 1 1 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"6 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"7 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"8 0 0 0 1 <nil> <nil> 0 0 0 0 1 <nil> <nil> 0 0 0 0 <nil>",
		"9 0 0 0 1 <nil> <nil> 0 0 1 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"10 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"11 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"12 0 0 0 1 <nil> <nil> 0 0 1 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"13 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"14 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"15 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"16 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"17 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"18 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"19 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"20 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"21 0 0 0 1 <nil> <nil> 0 0 1 1 0 <nil> <nil> 0 0 0 0 <nil>",
		"22 0 0 0 1 <nil> <nil> 0 0 0 1 0 <nil> <nil> 0 0 0 0 <nil>",
		"23 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"24 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 1 0 <nil>",
		"25 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 1 0 0 <nil>",
		"26 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 1 0 0 0 <nil>",
		"27 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 1 0 <nil>",
		"28 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>"))

	// for coalesce
	result = tk.MustQuery("select coalesce(NULL), coalesce(NULL, NULL), coalesce(NULL, NULL, NULL);")
	result.Check(testkit.Events("<nil> <nil> <nil>"))
	tk.MustQuery(`select coalesce(cast(1 as json), cast(2 as json));`).Check(testkit.Events(`1`))
	tk.MustQuery(`select coalesce(NULL, cast(2 as json));`).Check(testkit.Events(`2`))
	tk.MustQuery(`select coalesce(cast(1 as json), NULL);`).Check(testkit.Events(`1`))
	tk.MustQuery(`select coalesce(NULL, NULL);`).Check(testkit.Events(`<nil>`))

	tk.MustExec("drop block if exists t2")
	tk.MustExec("create block t2(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t2 values(1, 1.1, "2020-08-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)

	result = tk.MustQuery("select coalesce(NULL, a), coalesce(NULL, b, a), coalesce(c, NULL, a, b), coalesce(d, NULL), coalesce(d, c), coalesce(NULL, NULL, e, 1), coalesce(f), coalesce(1, a, b, c, d, e, f) from t2")
	result.Check(testkit.Events(fmt.Sprintf("1 1.1 2020-08-01 12:01:01 12:01:01 %s 12:01:01 abcdef 21 1", time.Now().In(tk.Se.GetStochastikVars().Location()).Format("2006-01-02"))))

	// nullif
	result = tk.MustQuery(`SELECT NULLIF(NULL, 1), NULLIF(1, NULL), NULLIF(1, 1), NULLIF(NULL, NULL);`)
	result.Check(testkit.Events("<nil> 1 <nil> <nil>"))

	result = tk.MustQuery(`SELECT NULLIF(1, 1.0), NULLIF(1, "1.0");`)
	result.Check(testkit.Events("<nil> <nil>"))

	result = tk.MustQuery(`SELECT NULLIF("abc", 1);`)
	result.Check(testkit.Events("abc"))

	result = tk.MustQuery(`SELECT NULLIF(1+2, 1);`)
	result.Check(testkit.Events("3"))

	result = tk.MustQuery(`SELECT NULLIF(1, 1+2);`)
	result.Check(testkit.Events("1"))

	result = tk.MustQuery(`SELECT NULLIF(2+3, 1+2);`)
	result.Check(testkit.Events("5"))

	result = tk.MustQuery(`SELECT HEX(NULLIF("abc", 1));`)
	result.Check(testkit.Events("616263"))

	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a date)")
	result = tk.MustQuery("desc select a = a from t")
	result.Check(testkit.Events(
		"Projection_3 10000.00 root  eq(test.t.a, test.t.a)->DeferredCauset#3",
		"└─BlockReader_5 10000.00 root  data:BlockFullScan_4",
		"  └─BlockFullScan_4 10000.00 INTERLOCK[einsteindb] block:t keep order:false, stats:pseudo",
	))

	// for interval
	result = tk.MustQuery(`select interval(null, 1, 2), interval(1, 2, 3), interval(2, 1, 3)`)
	result.Check(testkit.Events("-1 0 1"))
	result = tk.MustQuery(`select interval(3, 1, 2), interval(0, "b", "1", "2"), interval("a", "b", "1", "2")`)
	result.Check(testkit.Events("2 1 1"))
	result = tk.MustQuery(`select interval(23, 1, 23, 23, 23, 30, 44, 200), interval(23, 1.7, 15.3, 23.1, 30, 44, 200), interval(9007199254740992, 9007199254740993)`)
	result.Check(testkit.Events("4 2 0"))
	result = tk.MustQuery(`select interval(cast(9223372036854775808 as unsigned), cast(9223372036854775809 as unsigned)), interval(9223372036854775807, cast(9223372036854775808 as unsigned)), interval(-9223372036854775807, cast(9223372036854775808 as unsigned))`)
	result.Check(testkit.Events("0 0 0"))
	result = tk.MustQuery(`select interval(cast(9223372036854775806 as unsigned), 9223372036854775807), interval(cast(9223372036854775806 as unsigned), -9223372036854775807), interval("9007199254740991", "9007199254740992")`)
	result.Check(testkit.Events("0 1 0"))
	result = tk.MustQuery(`select interval(9007199254740992, "9007199254740993"), interval("9007199254740992", 9007199254740993), interval("9007199254740992", "9007199254740993")`)
	result.Check(testkit.Events("1 1 1"))
	result = tk.MustQuery(`select INTERVAL(100, NULL, NULL, NULL, NULL, NULL, 100);`)
	result.Check(testkit.Events("6"))

	// for greatest
	result = tk.MustQuery(`select greatest(1, 2, 3), greatest("a", "b", "c"), greatest(1.1, 1.2, 1.3), greatest("123a", 1, 2)`)
	result.Check(testkit.Events("3 c 1.3 123"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect FLOAT value: '123a'"))
	result = tk.MustQuery(`select greatest(cast("2020-01-01" as datetime), "123", "234", cast("2020-01-01" as date)), greatest(cast("2020-01-01" as date), "123", null)`)
	// todo: MyALLEGROSQL returns "2020-01-01 <nil>"
	result.Check(testkit.Events("2020-01-01 00:00:00 <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Incorrect time value: '123'", "Warning|1292|Incorrect time value: '234'", "Warning|1292|Incorrect time value: '123'"))
	// for least
	result = tk.MustQuery(`select least(1, 2, 3), least("a", "b", "c"), least(1.1, 1.2, 1.3), least("123a", 1, 2)`)
	result.Check(testkit.Events("1 a 1.1 1"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect FLOAT value: '123a'"))
	result = tk.MustQuery(`select least(cast("2020-01-01" as datetime), "123", "234", cast("2020-01-01" as date)), least(cast("2020-01-01" as date), "123", null)`)
	result.Check(testkit.Events("123 <nil>"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Incorrect time value: '123'", "Warning|1292|Incorrect time value: '234'", "Warning|1292|Incorrect time value: '123'"))
	tk.MustQuery(`select 1 < 17666000000000000000, 1 > 17666000000000000000, 1 = 17666000000000000000`).Check(testkit.Events("1 0 0"))

	tk.MustExec("drop block if exists t")
	// insert value at utc timezone
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create block t(a timestamp)")
	tk.MustExec("insert into t value('1991-05-06 04:59:28')")
	// check daylight saving time in Asia/Shanghai
	tk.MustExec("set time_zone='Asia/Shanghai'")
	tk.MustQuery("select * from t").Check(testkit.Events("1991-05-06 13:59:28"))
	// insert an nonexistent time
	tk.MustExec("set time_zone = 'America/Los_Angeles'")
	_, err := tk.Exec("insert into t value('2011-03-13 02:00:00')")
	c.Assert(err, NotNil)
	// reset timezone to a +8 offset
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustQuery("select * from t").Check(testkit.Events("1991-05-06 12:59:28"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a bigint unsigned)")
	tk.MustExec("insert into t value(17666000000000000000)")
	tk.MustQuery("select * from t where a = 17666000000000000000").Check(testkit.Events("17666000000000000000"))

	// test for compare event
	result = tk.MustQuery(`select event(1,2,3)=event(1,2,3)`)
	result.Check(testkit.Events("1"))
	result = tk.MustQuery(`select event(1,2,3)=event(1+3,2,3)`)
	result.Check(testkit.Events("0"))
	result = tk.MustQuery(`select event(1,2,3)<>event(1,2,3)`)
	result.Check(testkit.Events("0"))
	result = tk.MustQuery(`select event(1,2,3)<>event(1+3,2,3)`)
	result.Check(testkit.Events("1"))
	result = tk.MustQuery(`select event(1+3,2,3)<>event(1+3,2,3)`)
	result.Check(testkit.Events("0"))
}

func (s *testIntegrationSuite) TestAggregationBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a decimal(7, 6))")
	tk.MustExec("insert into t values(1.123456), (1.123456)")
	result := tk.MustQuery("select avg(a) from t")
	result.Check(testkit.Events("1.1234560000"))

	tk.MustExec("use test")
	tk.MustExec("drop block t")
	tk.MustExec("CREATE TABLE `t` (	`a` int, KEY `idx_a` (`a`))")
	result = tk.MustQuery("select avg(a) from t")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select max(a), min(a) from t")
	result.Check(testkit.Events("<nil> <nil>"))
	result = tk.MustQuery("select distinct a from t")
	result.Check(testkit.Events())
	result = tk.MustQuery("select sum(a) from t")
	result.Check(testkit.Events("<nil>"))
	result = tk.MustQuery("select count(a) from t")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Events("18446744073709551615"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinBitOr(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Events("0"))
	tk.MustExec("insert into t values(1);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Events("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Events("3"))
	tk.MustExec("insert into t values(4);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Events("7"))
	result = tk.MustQuery("select a, bit_or(a) from t group by a order by a")
	result.Check(testkit.Events("<nil> 0", "1 1", "2 2", "4 4"))
	tk.MustExec("insert into t values(-1);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Events("18446744073709551615"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinBitXor(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Events("0"))
	tk.MustExec("insert into t values(1);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Events("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Events("3"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Events("0"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Events("3"))
	result = tk.MustQuery("select a, bit_xor(a) from t group by a order by a")
	result.Check(testkit.Events("<nil> 0", "1 1", "2 2", "3 0"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinBitAnd(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Events("18446744073709551615"))
	tk.MustExec("insert into t values(7);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Events("7"))
	tk.MustExec("insert into t values(5);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Events("5"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Events("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Events("0"))
	result = tk.MustQuery("select a, bit_and(a) from t group by a order by a desc")
	result.Check(testkit.Events("7 7", "5 5", "3 3", "2 2", "<nil> 18446744073709551615"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinGroupConcat(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a varchar(100))")
	tk.MustExec("create block d(a varchar(100))")
	tk.MustExec("insert into t values('hello'), ('hello')")
	result := tk.MustQuery("select group_concat(a) from t")
	result.Check(testkit.Events("hello,hello"))

	tk.MustExec("set @@group_concat_max_len=7")
	result = tk.MustQuery("select group_concat(a) from t")
	result.Check(testkit.Events("hello,h"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning 1260 Some rows were cut by GROUPCONCAT(test.t.a)"))

	_, err := tk.Exec("insert into d select group_concat(a) from t")
	c.Assert(errors.Cause(err).(*terror.Error).Code(), Equals, errors.ErrCode(allegrosql.ErrCutValueGroupConcat))

	tk.Exec("set sql_mode=''")
	tk.MustExec("insert into d select group_concat(a) from t")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning 1260 Some rows were cut by GROUPCONCAT(test.t.a)"))
	tk.MustQuery("select * from d").Check(testkit.Events("hello,h"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinJSONObjectAgg(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t;")
	tk.MustExec(`CREATE TABLE t (
		a int(11),
		b varchar(100),
		c decimal(3,2),
		d json,
		e date,
		f time,
		g datetime DEFAULT '2012-01-01',
		h timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		i char(36),
		j text(50));`)

	tk.MustExec(`insert into t values(1, 'ab', 5.5, '{"id": 1}', '2020-01-10', '11:12:13', '2020-01-11', '0000-00-00 00:00:00', 'first', 'json_objectagg_test');`)

	result := tk.MustQuery("select json_objectagg(a, b) from t group by a order by a;")
	result.Check(testkit.Events(`{"1": "ab"}`))
	result = tk.MustQuery("select json_objectagg(b, c) from t group by b order by b;")
	result.Check(testkit.Events(`{"ab": 5.5}`))
	result = tk.MustQuery("select json_objectagg(e, f) from t group by e order by e;")
	result.Check(testkit.Events(`{"2020-01-10": "11:12:13"}`))
	result = tk.MustQuery("select json_objectagg(f, g) from t group by f order by f;")
	result.Check(testkit.Events(`{"11:12:13": "2020-01-11 00:00:00"}`))
	result = tk.MustQuery("select json_objectagg(g, h) from t group by g order by g;")
	result.Check(testkit.Events(`{"2020-01-11 00:00:00": "0000-00-00 00:00:00"}`))
	result = tk.MustQuery("select json_objectagg(h, i) from t group by h order by h;")
	result.Check(testkit.Events(`{"0000-00-00 00:00:00": "first"}`))
	result = tk.MustQuery("select json_objectagg(i, j) from t group by i order by i;")
	result.Check(testkit.Events(`{"first": "json_objectagg_test"}`))
	result = tk.MustQuery("select json_objectagg(a, null) from t group by a order by a;")
	result.Check(testkit.Events(`{"1": null}`))
}

func (s *testIntegrationSuite2) TestOtherBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b double, c varchar(20), d datetime, e time)")
	tk.MustExec("insert into t value(1, 2, 'string', '2020-01-01 12:12:12', '12:12:12')")

	// for in
	result := tk.MustQuery("select 1 in (a, b, c), 'string' in (a, b, c), '2020-01-01 12:12:12' in (c, d, e), '12:12:12' in (c, d, e) from t")
	result.Check(testkit.Events("1 1 1 1"))
	result = tk.MustQuery("select 1 in (null, c), 2 in (null, c) from t")
	result.Check(testkit.Events("<nil> <nil>"))
	result = tk.MustQuery("select 0 in (a, b, c), 0 in (a, b, c), 3 in (a, b, c), 4 in (a, b, c) from t")
	result.Check(testkit.Events("1 1 0 0"))
	result = tk.MustQuery("select (0,1) in ((0,1), (0,2)), (0,1) in ((0,0), (0,2))")
	result.Check(testkit.Events("1 0"))

	result = tk.MustQuery(`select bit_count(121), bit_count(-1), bit_count(null), bit_count("1231aaa");`)
	result.Check(testkit.Events("5 64 <nil> 7"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key, b time, c double, d varchar(10))")
	tk.MustExec(`insert into t values(1, '01:01:01', 1.1, "1"), (2, '02:02:02', 2.2, "2")`)
	tk.MustExec(`insert into t(a, b) values(1, '12:12:12') on duplicate key uFIDelate a = values(b)`)
	result = tk.MustQuery(`select a from t order by a`)
	result.Check(testkit.Events("2", "121212"))
	tk.MustExec(`insert into t values(2, '12:12:12', 1.1, "3.3") on duplicate key uFIDelate a = values(c) + values(d)`)
	result = tk.MustQuery(`select a from t order by a`)
	result.Check(testkit.Events("4", "121212"))

	// for setvar, getvar
	tk.MustExec(`set @varname = "Abc"`)
	result = tk.MustQuery(`select @varname, @VARNAME`)
	result.Check(testkit.Events("Abc Abc"))

	// for values
	tk.MustExec("drop block t")
	tk.MustExec("CREATE TABLE `t` (`id` varchar(32) NOT NULL, `count` decimal(18,2), PRIMARY KEY (`id`));")
	tk.MustExec("INSERT INTO t (id,count)VALUES('abc',2) ON DUPLICATE KEY UFIDelATE count=if(VALUES(count) > count,VALUES(count),count)")
	result = tk.MustQuery("select count from t where id = 'abc'")
	result.Check(testkit.Events("2.00"))
	tk.MustExec("INSERT INTO t (id,count)VALUES('abc',265.0) ON DUPLICATE KEY UFIDelATE count=if(VALUES(count) > count,VALUES(count),count)")
	result = tk.MustQuery("select count from t where id = 'abc'")
	result.Check(testkit.Events("265.00"))

	// for values(issue #4884)
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block test(id int not null, val text, primary key(id));")
	tk.MustExec("insert into test values(1,'hello');")
	result = tk.MustQuery("select * from test;")
	result.Check(testkit.Events("1 hello"))
	tk.MustExec("insert into test values(1, NULL) on duplicate key uFIDelate val = VALUES(val);")
	result = tk.MustQuery("select * from test;")
	result.Check(testkit.Events("1 <nil>"))

	tk.MustExec("drop block if exists test;")
	tk.MustExec(`create block test(
		id int not null,
		a text,
		b blob,
		c varchar(20),
		d int,
		e float,
		f DECIMAL(6,4),
		g JSON,
		primary key(id));`)

	tk.MustExec(`insert into test values(1,'txt hello', 'blb hello', 'vc hello', 1, 1.1, 1.0, '{"key1": "value1", "key2": "value2"}');`)
	tk.MustExec(`insert into test values(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
	on duplicate key uFIDelate
	a = values(a),
	b = values(b),
	c = values(c),
	d = values(d),
	e = values(e),
	f = values(f),
	g = values(g);`)

	result = tk.MustQuery("select * from test;")
	result.Check(testkit.Events("1 <nil> <nil> <nil> <nil> <nil> <nil> <nil>"))
}

func (s *testIntegrationSuite) TestDateBuiltin(c *C) {
	ctx := context.Background()
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("create block t (d date);")
	tk.MustExec("insert into t values ('1997-01-02')")
	tk.MustExec("insert into t values ('1998-01-02')")
	r := tk.MustQuery("select * from t where d < date '1998-01-01';")
	r.Check(testkit.Events("1997-01-02"))

	r = tk.MustQuery("select date'20171212'")
	r.Check(testkit.Events("2020-12-12"))

	r = tk.MustQuery("select date'2020/12/12'")
	r.Check(testkit.Events("2020-12-12"))

	r = tk.MustQuery("select date'2020/12-12'")
	r.Check(testkit.Events("2020-12-12"))

	tk.MustExec("set sql_mode = ''")
	r = tk.MustQuery("select date '0000-00-00';")
	r.Check(testkit.Events("0000-00-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_IN_DATE'")
	r = tk.MustQuery("select date '0000-00-00';")
	r.Check(testkit.Events("0000-00-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	rs, err := tk.Exec("select date '0000-00-00';")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "0000-00-00")), IsTrue)
	c.Assert(rs.Close(), IsNil)

	tk.MustExec("set sql_mode = ''")
	r = tk.MustQuery("select date '2007-10-00';")
	r.Check(testkit.Events("2007-10-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_IN_DATE'")
	rs, _ = tk.Exec("select date '2007-10-00';")
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "2020-10-00")), IsTrue)
	c.Assert(rs.Close(), IsNil)

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	r = tk.MustQuery("select date '2007-10-00';")
	r.Check(testkit.Events("2007-10-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_IN_DATE,NO_ZERO_DATE'")

	rs, _ = tk.Exec("select date '2007-10-00';")
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "2020-10-00")), IsTrue)
	c.Assert(rs.Close(), IsNil)

	rs, err = tk.Exec("select date '0000-00-00';")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "0000-00-00")), IsTrue)
	c.Assert(rs.Close(), IsNil)

	r = tk.MustQuery("select date'1998~01~02'")
	r.Check(testkit.Events("1998-01-02"))

	r = tk.MustQuery("select date'731124', date '011124'")
	r.Check(testkit.Events("1973-11-24 2001-11-24"))

	_, err = tk.Exec("select date '0000-00-00 00:00:00';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "0000-00-00 00:00:00")), IsTrue)

	_, err = tk.Exec("select date '2020-99-99';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue, Commentf("err: %v", err))

	_, err = tk.Exec("select date '2020-2-31';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue), IsTrue, Commentf("err: %v", err))

	_, err = tk.Exec("select date '201712-31';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "201712-31")), IsTrue, Commentf("err: %v", err))

	_, err = tk.Exec("select date 'abcdefg';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "abcdefg")), IsTrue, Commentf("err: %v", err))
}

func (s *testIntegrationSuite) TestJSONBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE `my_defCauslection` (	`doc` json DEFAULT NULL, `_id` varchar(32) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(doc,'$._id'))) STORED NOT NULL, PRIMARY KEY (`_id`))")
	_, err := tk.Exec("UFIDelATE `test`.`my_defCauslection` SET doc=JSON_SET(doc) WHERE (JSON_EXTRACT(doc,'$.name') = 'clare');")
	c.Assert(err, NotNil)

	r := tk.MustQuery("select json_valid(null);")
	r.Check(testkit.Events("<nil>"))

	r = tk.MustQuery(`select json_valid("null");`)
	r.Check(testkit.Events("1"))

	r = tk.MustQuery("select json_valid(0);")
	r.Check(testkit.Events("0"))

	r = tk.MustQuery(`select json_valid("0");`)
	r.Check(testkit.Events("1"))

	r = tk.MustQuery(`select json_valid("hello");`)
	r.Check(testkit.Events("0"))

	r = tk.MustQuery(`select json_valid('"hello"');`)
	r.Check(testkit.Events("1"))

	r = tk.MustQuery(`select json_valid('{"a":1}');`)
	r.Check(testkit.Events("1"))

	r = tk.MustQuery("select json_valid('{}');")
	r.Check(testkit.Events("1"))

	r = tk.MustQuery(`select json_valid('[]');`)
	r.Check(testkit.Events("1"))

	r = tk.MustQuery("select json_valid('2020-8-19');")
	r.Check(testkit.Events("0"))

	r = tk.MustQuery(`select json_valid('"2020-8-19"');`)
	r.Check(testkit.Events("1"))
}

func (s *testIntegrationSuite) TestTimeLiteral(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)

	r := tk.MustQuery("select time '117:01:12';")
	r.Check(testkit.Events("117:01:12"))

	r = tk.MustQuery("select time '01:00:00.999999';")
	r.Check(testkit.Events("01:00:00.999999"))

	r = tk.MustQuery("select time '1 01:00:00';")
	r.Check(testkit.Events("25:00:00"))

	r = tk.MustQuery("select time '110:00:00';")
	r.Check(testkit.Events("110:00:00"))

	r = tk.MustQuery("select time'-1:1:1.123454656';")
	r.Check(testkit.Events("-01:01:01.123455"))

	r = tk.MustQuery("select time '33:33';")
	r.Check(testkit.Events("33:33:00"))

	r = tk.MustQuery("select time '1.1';")
	r.Check(testkit.Events("00:00:01.1"))

	r = tk.MustQuery("select time '21';")
	r.Check(testkit.Events("00:00:21"))

	r = tk.MustQuery("select time '20 20:20';")
	r.Check(testkit.Events("500:20:00"))

	_, err := tk.Exec("select time '2020-01-01 00:00:00';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "2020-01-01 00:00:00")), IsTrue)

	_, err = tk.Exec("select time '071231235959.999999';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "071231235959.999999")), IsTrue)

	_, err = tk.Exec("select time '20171231235959.999999';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "20171231235959.999999")), IsTrue)

	_, err = tk.Exec("select ADDDATE('2008-01-34', -1);")
	c.Assert(err, IsNil)
	tk.MustQuery("Show warnings;").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Incorrect datetime value: '2008-01-34'"))
}

func (s *testIntegrationSuite) TestIssue13822(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustQuery("select ADDDATE(20111111, interval '-123' DAY);").Check(testkit.Events("2011-07-11"))
	tk.MustQuery("select SUBDATE(20111111, interval '-123' DAY);").Check(testkit.Events("2012-03-13"))
}

func (s *testIntegrationSuite) TestTimestampLiteral(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)

	r := tk.MustQuery("select timestamp '2020-01-01 00:00:00';")
	r.Check(testkit.Events("2020-01-01 00:00:00"))

	r = tk.MustQuery("select timestamp '2020@01@01 00:00:00';")
	r.Check(testkit.Events("2020-01-01 00:00:00"))

	r = tk.MustQuery("select timestamp '2020@01@01 00~00~00';")
	r.Check(testkit.Events("2020-01-01 00:00:00"))

	r = tk.MustQuery("select timestamp '2020@01@0001 00~00~00.333';")
	r.Check(testkit.Events("2020-01-01 00:00:00.333"))

	_, err := tk.Exec("select timestamp '00:00:00';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "00:00:00")), IsTrue)

	_, err = tk.Exec("select timestamp '1992-01-03';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "1992-01-03")), IsTrue)

	_, err = tk.Exec("select timestamp '20171231235959.999999';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, "20171231235959.999999")), IsTrue)
}

func (s *testIntegrationSuite) TestLiterals(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.causetstore)
	r := tk.MustQuery("SELECT LENGTH(b''), LENGTH(B''), b''+1, b''-1, B''+1;")
	r.Check(testkit.Events("0 0 1 -1 1"))
}

func (s *testIntegrationSuite) TestFuncJSON(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS block_json;")
	tk.MustExec("CREATE TABLE block_json(a json, b VARCHAR(255));")

	j1 := `{"\\"hello\\"": "world", "a": [1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}], "b": true, "c": ["d"]}`
	j2 := `[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`
	for _, j := range []string{j1, j2} {
		tk.MustExec(fmt.Sprintf(`INSERT INTO block_json values('%s', '%s')`, j, j))
	}

	r := tk.MustQuery(`select json_type(a), json_type(b) from block_json`)
	r.Check(testkit.Events("OBJECT OBJECT", "ARRAY ARRAY"))

	tk.MustGetErrCode("select json_quote();", allegrosql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_quote('abc', 'def');", allegrosql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_quote(NULL, 'def');", allegrosql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_quote('abc', NULL);", allegrosql.ErrWrongParamcountToNativeFct)

	tk.MustGetErrCode("select json_unquote();", allegrosql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_unquote('abc', 'def');", allegrosql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_unquote(NULL, 'def');", allegrosql.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select json_unquote('abc', NULL);", allegrosql.ErrWrongParamcountToNativeFct)

	tk.MustQuery("select json_quote(NULL);").Check(testkit.Events("<nil>"))
	tk.MustQuery("select json_unquote(NULL);").Check(testkit.Events("<nil>"))

	tk.MustQuery("select json_quote('abc');").Check(testkit.Events(`"abc"`))
	tk.MustQuery(`select json_quote(convert('"abc"' using ascii));`).Check(testkit.Events(`"\"abc\""`))
	tk.MustQuery(`select json_quote(convert('"abc"' using latin1));`).Check(testkit.Events(`"\"abc\""`))
	tk.MustQuery(`select json_quote(convert('"abc"' using utf8));`).Check(testkit.Events(`"\"abc\""`))
	tk.MustQuery(`select json_quote(convert('"abc"' using utf8mb4));`).Check(testkit.Events(`"\"abc\""`))

	tk.MustQuery("select json_unquote('abc');").Check(testkit.Events("abc"))
	tk.MustQuery(`select json_unquote('"abc"');`).Check(testkit.Events("abc"))
	tk.MustQuery(`select json_unquote(convert('"abc"' using ascii));`).Check(testkit.Events("abc"))
	tk.MustQuery(`select json_unquote(convert('"abc"' using latin1));`).Check(testkit.Events("abc"))
	tk.MustQuery(`select json_unquote(convert('"abc"' using utf8));`).Check(testkit.Events("abc"))
	tk.MustQuery(`select json_unquote(convert('"abc"' using utf8mb4));`).Check(testkit.Events("abc"))

	tk.MustQuery(`select json_quote('"');`).Check(testkit.Events(`"\""`))
	tk.MustQuery(`select json_unquote('"');`).Check(testkit.Events(`"`))

	tk.MustQuery(`select json_unquote('""');`).Check(testkit.Events(``))
	tk.MustQuery(`select char_length(json_unquote('""'));`).Check(testkit.Events(`0`))
	tk.MustQuery(`select json_unquote('"" ');`).Check(testkit.Events(`"" `))
	tk.MustQuery(`select json_unquote(cast(json_quote('abc') as json));`).Check(testkit.Events("abc"))

	tk.MustQuery(`select json_unquote(cast('{"abc": "foo"}' as json));`).Check(testkit.Events(`{"abc": "foo"}`))
	tk.MustQuery(`select json_unquote(json_extract(cast('{"abc": "foo"}' as json), '$.abc'));`).Check(testkit.Events("foo"))
	tk.MustQuery(`select json_unquote('["a", "b", "c"]');`).Check(testkit.Events(`["a", "b", "c"]`))
	tk.MustQuery(`select json_unquote(cast('["a", "b", "c"]' as json));`).Check(testkit.Events(`["a", "b", "c"]`))
	tk.MustQuery(`select json_quote(convert(X'e68891' using utf8));`).Check(testkit.Events(`"我"`))
	tk.MustQuery(`select json_quote(convert(X'e68891' using utf8mb4));`).Check(testkit.Events(`"我"`))
	tk.MustQuery(`select cast(json_quote(convert(X'e68891' using utf8)) as json);`).Check(testkit.Events(`"我"`))
	tk.MustQuery(`select json_unquote(convert(X'e68891' using utf8));`).Check(testkit.Events("我"))

	tk.MustQuery(`select json_quote(json_quote(json_quote('abc')));`).Check(testkit.Events(`"\"\\\"abc\\\"\""`))
	tk.MustQuery(`select json_unquote(json_unquote(json_unquote(json_quote(json_quote(json_quote('abc'))))));`).Check(testkit.Events("abc"))

	tk.MustGetErrCode("select json_quote(123)", allegrosql.ErrIncorrectType)
	tk.MustGetErrCode("select json_quote(-100)", allegrosql.ErrIncorrectType)
	tk.MustGetErrCode("select json_quote(123.123)", allegrosql.ErrIncorrectType)
	tk.MustGetErrCode("select json_quote(-100.000)", allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(true);`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(false);`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("{}" as JSON));`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("[]" as JSON));`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("2020-07-29" as date));`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("12:18:29.000000" as time));`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_quote(cast("2020-07-29 12:18:29.000000" as datetime));`, allegrosql.ErrIncorrectType)

	tk.MustGetErrCode("select json_unquote(123)", allegrosql.ErrIncorrectType)
	tk.MustGetErrCode("select json_unquote(-100)", allegrosql.ErrIncorrectType)
	tk.MustGetErrCode("select json_unquote(123.123)", allegrosql.ErrIncorrectType)
	tk.MustGetErrCode("select json_unquote(-100.000)", allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(true);`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(false);`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(cast("2020-07-29" as date));`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(cast("12:18:29.000000" as time));`, allegrosql.ErrIncorrectType)
	tk.MustGetErrCode(`select json_unquote(cast("2020-07-29 12:18:29.000000" as datetime));`, allegrosql.ErrIncorrectType)

	r = tk.MustQuery(`select json_extract(a, '$.a[1]'), json_extract(b, '$.b') from block_json`)
	r.Check(testkit.Events("\"2\" true", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_set(a, '$.a[1]', 3), '$.a[1]'), json_extract(json_set(b, '$.b', false), '$.b') from block_json`)
	r.Check(testkit.Events("3 false", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_insert(a, '$.a[1]', 3), '$.a[1]'), json_extract(json_insert(b, '$.b', false), '$.b') from block_json`)
	r.Check(testkit.Events("\"2\" true", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_replace(a, '$.a[1]', 3), '$.a[1]'), json_extract(json_replace(b, '$.b', false), '$.b') from block_json`)
	r.Check(testkit.Events("3 false", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_merge(a, cast(b as JSON)), '$[0].a[0]') from block_json`)
	r.Check(testkit.Events("1", "1"))

	r = tk.MustQuery(`select json_extract(json_array(1,2,3), '$[1]')`)
	r.Check(testkit.Events("2"))

	r = tk.MustQuery(`select json_extract(json_object(1,2,3,4), '$."1"')`)
	r.Check(testkit.Events("2"))

	tk.MustExec(`uFIDelate block_json set a=json_set(a,'$.a',json_object('a',1,'b',2)) where json_extract(a,'$.a[1]') = '2'`)
	r = tk.MustQuery(`select json_extract(a, '$.a.a'), json_extract(a, '$.a.b') from block_json`)
	r.Check(testkit.Events("1 2", "<nil> <nil>"))

	r = tk.MustQuery(`select json_contains(NULL, '1'), json_contains('1', NULL), json_contains('1', '1', NULL)`)
	r.Check(testkit.Events("<nil> <nil> <nil>"))
	r = tk.MustQuery(`select json_contains('{}','{}'), json_contains('[1]','1'), json_contains('[1]','"1"'), json_contains('[1,2,[1,[5,[3]]]]', '[1,3]', '$[2]'), json_contains('[1,2,[1,[5,{"a":[2,3]}]]]', '[1,{"a":[3]}]', "$[2]"), json_contains('{"a":1}', '{"a":1,"b":2}', "$")`)
	r.Check(testkit.Events("1 1 0 1 1 0"))
	r = tk.MustQuery(`select json_contains('{"a": 1}', '1', "$.c"), json_contains('{"a": [1, 2]}', '1', "$.a[2]"), json_contains('{"a": [1, {"a": 1}]}', '1', "$.a[1].b")`)
	r.Check(testkit.Events("<nil> <nil> <nil>"))
	rs, err := tk.Exec("select json_contains('1','1','$.*')")
	c.Assert(err, IsNil)
	c.Assert(rs, NotNil)
	_, err = stochastik.GetEvents4Test(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[json:3149]In this situation, path expressions may not contain the * and ** tokens.")

	r = tk.MustQuery(`select
		json_contains_path(NULL, 'one', "$.c"),
		json_contains_path(NULL, 'all', "$.c"),
		json_contains_path('{"a": 1}', NULL, "$.c"),
		json_contains_path('{"a": 1}', 'one', NULL),
		json_contains_path('{"a": 1}', 'all', NULL)
	`)
	r.Check(testkit.Events("<nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery(`select
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.c.d'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a.d'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.c.d'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.a.d')
	`)
	r.Check(testkit.Events("1 0 1 0"))

	r = tk.MustQuery(`select
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a', '$.e'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a', '$.b'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.a', '$.e'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.a', '$.b')
	`)
	r.Check(testkit.Events("1 1 0 1"))

	r = tk.MustQuery(`select
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.*'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$[*]'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.*'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$[*]')
	`)
	r.Check(testkit.Events("1 0 1 0"))

	r = tk.MustQuery(`select
		json_keys('[]'),
		json_keys('{}'),
		json_keys('{"a": 1, "b": 2}'),
		json_keys('{"a": {"c": 3}, "b": 2}'),
		json_keys('{"a": {"c": 3}, "b": 2}', "$.a")
	`)
	r.Check(testkit.Events(`<nil> [] ["a", "b"] ["a", "b"] ["c"]`))

	r = tk.MustQuery(`select
		json_length('1'),
		json_length('{}'),
		json_length('[]'),
		json_length('{"a": 1}'),
		json_length('{"a": 1, "b": 2}'),
		json_length('[1, 2, 3]')
	`)
	r.Check(testkit.Events("1 0 0 1 2 3"))

	// #16267
	tk.MustQuery(`select json_array(922337203685477580) =  json_array(922337203685477581);`).Check(testkit.Events("0"))
}

func (s *testIntegrationSuite) TestDeferredCausetInfoModified(c *C) {
	testKit := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists tab0")
	testKit.MustExec("CREATE TABLE tab0(defCaus0 INTEGER, defCaus1 INTEGER, defCaus2 INTEGER)")
	testKit.MustExec("SELECT + - (- CASE + defCaus0 WHEN + CAST( defCaus0 AS SIGNED ) THEN defCaus1 WHEN 79 THEN NULL WHEN + - defCaus1 THEN defCaus0 / + defCaus0 END ) * - 16 FROM tab0")
	ctx := testKit.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, _ := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("tab0"))
	defCaus := block.FindDefCaus(tbl.DefCauss(), "defCaus1")
	c.Assert(defCaus.Tp, Equals, allegrosql.TypeLong)
}

func (s *testIntegrationSuite) TestSetVariables(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	_, err := tk.Exec("set sql_mode='adfasdfadsfdasd';")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set @@sql_mode='adfasdfadsfdasd';")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set @@global.sql_mode='adfasdfadsfdasd';")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set @@stochastik.sql_mode='adfasdfadsfdasd';")
	c.Assert(err, NotNil)

	var r *testkit.Result
	_, err = tk.Exec("set @@stochastik.sql_mode=',NO_ZERO_DATE,ANSI,ANSI_QUOTES';")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@stochastik.sql_mode`)
	r.Check(testkit.Events("NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))
	r = tk.MustQuery(`show variables like 'sql_mode'`)
	r.Check(testkit.Events("sql_mode NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))

	// for invalid ALLEGROALLEGROSQL mode.
	tk.MustExec("use test")
	tk.MustExec("drop block if exists tab0")
	tk.MustExec("CREATE TABLE tab0(defCaus1 time)")
	_, err = tk.Exec("set sql_mode='STRICT_TRANS_TABLES';")
	c.Assert(err, IsNil)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1292]Truncated incorrect time value: '999:44:33'")
	_, err = tk.Exec("set sql_mode=' ,';")
	c.Assert(err, NotNil)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1292]Truncated incorrect time value: '999:44:33'")

	// issue #5478
	_, err = tk.Exec("set stochastik transaction read write;")
	c.Assert(err, IsNil)
	_, err = tk.Exec("set global transaction read write;")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@stochastik.tx_read_only, @@global.tx_read_only, @@stochastik.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Events("0 0 0 0"))

	_, err = tk.Exec("set stochastik transaction read only;")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@stochastik.tx_read_only, @@global.tx_read_only, @@stochastik.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Events("1 0 1 0"))
	_, err = tk.Exec("set global transaction read only;")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@stochastik.tx_read_only, @@global.tx_read_only, @@stochastik.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Events("1 1 1 1"))

	_, err = tk.Exec("set stochastik transaction read write;")
	c.Assert(err, IsNil)
	_, err = tk.Exec("set global transaction read write;")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@stochastik.tx_read_only, @@global.tx_read_only, @@stochastik.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Events("0 0 0 0"))

	_, err = tk.Exec("set @@global.max_user_connections='';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_user_connections").Error())
	_, err = tk.Exec("set @@global.max_prepared_stmt_count='';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_prepared_stmt_count").Error())
}

func (s *testIntegrationSuite) TestIssues(c *C) {
	// for issue #4954
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (a CHAR(5) CHARACTER SET latin1);")
	tk.MustExec("INSERT INTO t VALUES ('oe');")
	tk.MustExec("INSERT INTO t VALUES (0xf6);")
	r := tk.MustQuery(`SELECT * FROM t WHERE a= 'oe';`)
	r.Check(testkit.Events("oe"))
	r = tk.MustQuery(`SELECT HEX(a) FROM t WHERE a= 0xf6;`)
	r.Check(testkit.Events("F6"))

	// for issue #4006
	tk.MustExec(`drop block if exists tb`)
	tk.MustExec("create block tb(id int auto_increment primary key, v varchar(32));")
	tk.MustExec("insert into tb(v) (select v from tb);")
	r = tk.MustQuery(`SELECT * FROM tb;`)
	r.Check(testkit.Events())
	tk.MustExec(`insert into tb(v) values('hello');`)
	tk.MustExec("insert into tb(v) (select v from tb);")
	r = tk.MustQuery(`SELECT * FROM tb;`)
	r.Check(testkit.Events("1 hello", "2 hello"))

	// for issue #5111
	tk.MustExec(`drop block if exists t`)
	tk.MustExec("create block t(c varchar(32));")
	tk.MustExec("insert into t values('1e649'),('-1e649');")
	r = tk.MustQuery(`SELECT * FROM t where c < 1;`)
	r.Check(testkit.Events("-1e649"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Truncated incorrect DOUBLE value: '1e649'",
		"Warning|1292|Truncated incorrect DOUBLE value: '-1e649'"))
	r = tk.MustQuery(`SELECT * FROM t where c > 1;`)
	r.Check(testkit.Events("1e649"))
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|",
		"Warning|1292|Truncated incorrect DOUBLE value: '1e649'",
		"Warning|1292|Truncated incorrect DOUBLE value: '-1e649'"))

	// for issue #5293
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert t values (1)")
	tk.MustQuery("select * from t where cast(a as binary)").Check(testkit.Events("1"))

	// for issue #16351
	tk.MustExec("drop block if exists t2")
	tk.MustExec("create block t2(a int, b varchar(20))")
	tk.MustExec(`insert into t2 values(1,"1111"),(2,"2222"),(3,"3333"),(4,"4444"),(5,"5555"),(6,"6666"),(7,"7777"),(8,"8888"),(9,"9999"),(10,"0000")`)
	tk.MustQuery(`select (@j := case when substr(t2.b,1,3)=@i then 1 else @j+1 end) from t2, (select @j := 0, @i := "0") tt limit 10`).Check(testkit.Events(
		"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"))
}

func (s *testIntegrationSuite) TestInPredicate4UnsignedInt(c *C) {
	// for issue #6661
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (a bigint unsigned,key (a));")
	tk.MustExec("INSERT INTO t VALUES (0), (4), (5), (6), (7), (8), (9223372036854775810), (18446744073709551614), (18446744073709551615);")
	r := tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 18446744073709551615);`)
	r.Check(testkit.Events("0", "4", "5", "6", "7", "8", "9223372036854775810", "18446744073709551614"))
	r = tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 4, 9223372036854775810);`)
	r.Check(testkit.Events("0", "5", "6", "7", "8", "18446744073709551614", "18446744073709551615"))
	r = tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 0, 4, 18446744073709551614);`)
	r.Check(testkit.Events("5", "6", "7", "8", "9223372036854775810", "18446744073709551615"))

	// for issue #4473
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t1 (some_id smallint(5) unsigned,key (some_id) )")
	tk.MustExec("insert into t1 values (1),(2)")
	r = tk.MustQuery(`select some_id from t1 where some_id not in(2,-1);`)
	r.Check(testkit.Events("1"))
}

func (s *testIntegrationSuite) TestFilterExtractFromDNF(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, c int)")

	tests := []struct {
		exprStr string
		result  string
	}{
		{
			exprStr: "a = 1 or a = 1 or a = 1",
			result:  "[eq(test.t.a, 1)]",
		},
		{
			exprStr: "a = 1 or a = 1 or (a = 1 and b = 1)",
			result:  "[eq(test.t.a, 1)]",
		},
		{
			exprStr: "(a = 1 and a = 1) or a = 1 or b = 1",
			result:  "[or(or(and(eq(test.t.a, 1), eq(test.t.a, 1)), eq(test.t.a, 1)), eq(test.t.b, 1))]",
		},
		{
			exprStr: "(a = 1 and b = 2) or (a = 1 and b = 3) or (a = 1 and b = 4)",
			result:  "[eq(test.t.a, 1) or(eq(test.t.b, 2), or(eq(test.t.b, 3), eq(test.t.b, 4)))]",
		},
		{
			exprStr: "(a = 1 and b = 1 and c = 1) or (a = 1 and b = 1) or (a = 1 and b = 1 and c > 2 and c < 3)",
			result:  "[eq(test.t.a, 1) eq(test.t.b, 1)]",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		allegrosql := "select * from t where " + tt.exprStr
		sctx := tk.Se.(stochastikctx.Context)
		sc := sctx.GetStochastikVars().StmtCtx
		stmts, err := stochastik.Parse(sctx, allegrosql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := petri.GetPetri(sctx).SchemaReplicant()
		err = plannercore.Preprocess(sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, _, err := plannercore.BuildLogicalPlan(ctx, sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		conds := make([]expression.Expression, len(selection.Conditions))
		for i, cond := range selection.Conditions {
			conds[i] = expression.PushDownNot(sctx, cond)
		}
		afterFunc := expression.ExtractFiltersFromDNFs(sctx, conds)
		sort.Slice(afterFunc, func(i, j int) bool {
			return bytes.Compare(afterFunc[i].HashCode(sc), afterFunc[j].HashCode(sc)) < 0
		})
		c.Assert(fmt.Sprintf("%s", afterFunc), Equals, tt.result, Commentf("wrong result for expr: %s", tt.exprStr))
	}
}

func (s *testIntegrationSuite) testMilevaDBIsOwnerFunc(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	result := tk.MustQuery("select milevadb_is_dbs_owner()")
	dbsOwnerChecker := tk.Se.DBSOwnerChecker()
	c.Assert(dbsOwnerChecker, NotNil)
	var ret int64
	if dbsOwnerChecker.IsOwner() {
		ret = 1
	}
	result.Check(testkit.Events(fmt.Sprintf("%v", ret)))
}

func (s *testIntegrationSuite) TestMilevaDBDecodePlanFunc(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustQuery("select milevadb_decode_plan('')").Check(testkit.Events(""))
	tk.MustQuery("select milevadb_decode_plan('7APIMAk1XzEzCTAJMQlmdW5jczpjb3VudCgxKQoxCTE3XzE0CTAJMAlpbm5lciBqb2luLCBp" +
		"AQyQOlRhYmxlUmVhZGVyXzIxLCBlcXVhbDpbZXEoQ29sdW1uIzEsIA0KCDkpIBkXADIVFywxMCldCjIJMzFfMTgFZXhkYXRhOlNlbGVjdGlvbl" +
		"8xNwozCTFfMTcJMQkwCWx0HVlATlVMTCksIG5vdChpc251bGwVHAApUhcAUDIpKQo0CTEwXzE2CTEJMTAwMDAJdAHB2Dp0MSwgcmFuZ2U6Wy1p" +
		"bmYsK2luZl0sIGtlZXAgb3JkZXI6ZmFsc2UsIHN0YXRzOnBzZXVkbwoFtgAyAZcEMAk6tgAEMjAFtgQyMDq2AAg5LCBmtgAAMFa3AAA5FbcAO" +
		"T63AAAyzrcA')").Check(testkit.Events("" +
		"\tid                  \ttask\testEvents\toperator info\n" +
		"\tStreamAgg_13        \troot\t1      \tfuncs:count(1)\n" +
		"\t└─HashJoin_14       \troot\t0      \tinner join, inner:BlockReader_21, equal:[eq(DeferredCauset#1, DeferredCauset#9) eq(DeferredCauset#2, DeferredCauset#10)]\n" +
		"\t  ├─BlockReader_18  \troot\t0      \tdata:Selection_17\n" +
		"\t  │ └─Selection_17  \tINTERLOCK \t0      \tlt(DeferredCauset#1, NULL), not(isnull(DeferredCauset#1)), not(isnull(DeferredCauset#2))\n" +
		"\t  │   └─BlockScan_16\tINTERLOCK \t10000  \tblock:t1, range:[-inf,+inf], keep order:false, stats:pseudo\n" +
		"\t  └─BlockReader_21  \troot\t0      \tdata:Selection_20\n" +
		"\t    └─Selection_20  \tINTERLOCK \t0      \tlt(DeferredCauset#9, NULL), not(isnull(DeferredCauset#10)), not(isnull(DeferredCauset#9))\n" +
		"\t      └─BlockScan_19\tINTERLOCK \t10000  \tblock:t2, range:[-inf,+inf], keep order:false, stats:pseudo"))
	tk.MustQuery("select milevadb_decode_plan('rwPwcTAJNV8xNAkwCTEJZnVuY3M6bWF4KHRlc3QudC5hKS0+Q29sdW1uIzQJMQl0aW1lOj" +
		"IyMy45MzXCtXMsIGxvb3BzOjIJMTI4IEJ5dGVzCU4vQQoxCTE2XzE4CTAJMQlvZmZzZXQ6MCwgY291bnQ6MQkxCQlHFDE4LjQyMjJHAAhOL0" +
		"EBBCAKMgkzMl8yOAkBlEBpbmRleDpMaW1FIDelF8yNwkxCQ0+DDYuODUdPSwxLCBycGMgbnVtOiANDAUpGDE1MC44MjQFKjhwcm9jIGtleXM6MA" +
		"kxOTgdsgAzAbIAMgFearIAFDU3LjM5NgVKAGwN+BGxIDQJMTNfMjYJMQGgHGFibGU6dCwgCbqwaWR4KGEpLCByYW5nZTooMCwraW5mXSwga2" +
		"VlcCBvcmRlcjp0cnVlLCBkZXNjAT8kaW1lOjU2LjY2MR1rJDEJTi9BCU4vQQo=')").Check(testkit.Events("" +
		"\tid                  \ttask\testEvents\toperator info                                               \tactEvents\texecution info                                                       \tmemory   \tdisk\n" +
		"\tStreamAgg_14        \troot\t1      \tfuncs:max(test.t.a)->DeferredCauset#4                               \t1      \ttime:223.935µs, loops:2                                             \t128 Bytes\tN/A\n" +
		"\t└─Limit_18          \troot\t1      \toffset:0, count:1                                           \t1      \ttime:218.422µs, loops:2                                             \tN/A      \tN/A\n" +
		"\t  └─IndexReader_28  \troot\t1      \tindex:Limit_27                                              \t1      \ttime:216.85µs, loops:1, rpc num: 1, rpc time:150.824µs, proc keys:0\t198 Bytes\tN/A\n" +
		"\t    └─Limit_27      \tINTERLOCK \t1      \toffset:0, count:1                                           \t1      \ttime:57.396µs, loops:2                                              \tN/A      \tN/A\n" +
		"\t      └─IndexScan_26\tINTERLOCK \t1      \tblock:t, index:idx(a), range:(0,+inf], keep order:true, desc\t1      \ttime:56.661µs, loops:1                                              \tN/A      \tN/A"))
}

func (s *testIntegrationSuite) TestMilevaDBInternalFunc(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	var result *testkit.Result
	result = tk.MustQuery("select milevadb_decode_key( '74800000000000002B5F72800000000000A5D3' )")
	result.Check(testkit.Events("blockID=43, _milevadb_rowid=42451"))
	result = tk.MustQuery("select milevadb_decode_key( '7480000000000000325f7205bff199999999999a013131000000000000f9' )")
	result.Check(testkit.Events("blockID=50, clusterHandle={1.1, 11}"))

	result = tk.MustQuery("select milevadb_decode_key( '74800000000000019B5F698000000000000001015257303100000000FB013736383232313130FF3900000000000000F8010000000000000000F7' )")
	result.Check(testkit.Events("blockID=411, indexID=1, indexValues=RW01,768221109,"))
	result = tk.MustQuery("select milevadb_decode_key( '7480000000000000695F698000000000000001038000000000004E20' )")
	result.Check(testkit.Events("blockID=105, indexID=1, indexValues=20000"))

	// Test invalid record/index key.
	result = tk.MustQuery("select milevadb_decode_key( '7480000000000000FF2E5F728000000011FFE1A3000000000000' )")
	result.Check(testkit.Events("7480000000000000FF2E5F728000000011FFE1A3000000000000"))
	warns := tk.Se.GetStochastikVars().StmtCtx.GetWarnings()
	c.Assert(warns, HasLen, 1)
	c.Assert(warns[0].Err.Error(), Equals, "invalid record/index key: 7480000000000000FF2E5F728000000011FFE1A3000000000000")
}

func newStoreWithBootstrap() (ekv.CausetStorage, *petri.Petri, error) {
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, err
	}
	stochastik.SetSchemaLease(0)
	dom, err := stochastik.BootstrapStochastik(causetstore)
	return causetstore, dom, err
}

func (s *testIntegrationSuite) TestTwoDecimalTruncate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t1(a decimal(10,5), b decimal(10,1))")
	tk.MustExec("insert into t1 values(123.12345, 123.12345)")
	tk.MustExec("uFIDelate t1 set b = a")
	res := tk.MustQuery("select a, b from t1")
	res.Check(testkit.Events("123.12345 123.1"))
	res = tk.MustQuery("select 2.00000000000000000000000000000001 * 1.000000000000000000000000000000000000000000002")
	res.Check(testkit.Events("2.000000000000000000000000000000"))
}

func (s *testIntegrationSuite) TestPrefixIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
  			name varchar(12) DEFAULT NULL,
  			KEY pname (name(12))
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`)

	tk.MustExec("insert into t1 values('借款策略集_网页');")
	res := tk.MustQuery("select * from t1 where name = '借款策略集_网页';")
	res.Check(testkit.Events("借款策略集_网页"))

	tk.MustExec(`CREATE TABLE prefix (
		a int(11) NOT NULL,
		b varchar(55) DEFAULT NULL,
		c int(11) DEFAULT NULL,
		PRIMARY KEY (a),
		KEY prefix_index (b(2)),
		KEY prefix_complex (a,b(2))
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)

	tk.MustExec("INSERT INTO prefix VALUES(0, 'b', 2), (1, 'bbb', 3), (2, 'bbc', 4), (3, 'bbb', 5), (4, 'abc', 6), (5, 'abc', 7), (6, 'abc', 7), (7, 'ÿÿ', 8), (8, 'ÿÿ0', 9), (9, 'ÿÿÿ', 10);")
	res = tk.MustQuery("select c, b from prefix where b > 'ÿ' and b < 'ÿÿc'")
	res.Check(testkit.Events("8 ÿÿ", "9 ÿÿ0"))

	res = tk.MustQuery("select a, b from prefix where b LIKE 'ÿÿ%'")
	res.Check(testkit.Events("7 ÿÿ", "8 ÿÿ0", "9 ÿÿÿ"))
}

func (s *testIntegrationSuite) TestDecimalMul(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test")
	tk.MustExec("create block t(a decimal(38, 17));")
	tk.MustExec("insert into t select 0.5999991229316*0.918755041726043;")
	res := tk.MustQuery("select * from t;")
	res.Check(testkit.Events("0.55125221922461136"))
}

func (s *testIntegrationSuite) TestDecimalDiv(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select cast(1 as decimal(60,30)) / cast(1 as decimal(60,30)) / cast(1 as decimal(60, 30))").Check(testkit.Events("1.000000000000000000000000000000"))
	tk.MustQuery("select cast(1 as decimal(60,30)) / cast(3 as decimal(60,30)) / cast(7 as decimal(60, 30))").Check(testkit.Events("0.047619047619047619047619047619"))
	tk.MustQuery("select cast(1 as decimal(60,30)) / cast(3 as decimal(60,30)) / cast(7 as decimal(60, 30)) / cast(13 as decimal(60, 30))").Check(testkit.Events("0.003663003663003663003663003663"))
}

func (s *testIntegrationSuite) TestUnknowHintIgnore(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test")
	tk.MustExec("create block t(a int)")
	tk.MustQuery("select /*+ unknown_hint(c1)*/ 1").Check(testkit.Events("1"))
	tk.MustQuery("show warnings").Check(testkit.Events("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [berolinaAllegroSQL:8064]Optimizer hint syntax error at line 1 defCausumn 23 near \"unknown_hint(c1)*/\" "))
	_, err := tk.Exec("select 1 from /*+ test1() */ t")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TestValuesInNonInsertStmt(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a bigint, b double, c decimal, d varchar(20), e datetime, f time, g json);`)
	tk.MustExec(`insert into t values(1, 1.1, 2.2, "abc", "2020-10-24", NOW(), "12");`)
	res := tk.MustQuery(`select values(a), values(b), values(c), values(d), values(e), values(f), values(g) from t;`)
	res.Check(testkit.Events(`<nil> <nil> <nil> <nil> <nil> <nil> <nil>`))
}

func (s *testIntegrationSuite) TestForeignKeyVar(c *C) {

	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("SET FOREIGN_KEY_CHECKS=1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Events("Warning 8047 variable 'foreign_key_checks' does not yet support value: 1"))
}

func (s *testIntegrationSuite) TestUserVarMockWindFunc(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t (a int, b varchar (20), c varchar (20));`)
	tk.MustExec(`insert into t values
					(1,'key1-value1','insert_order1'),
    				(1,'key1-value2','insert_order2'),
    				(1,'key1-value3','insert_order3'),
    				(1,'key1-value4','insert_order4'),
    				(1,'key1-value5','insert_order5'),
    				(1,'key1-value6','insert_order6'),
    				(2,'key2-value1','insert_order1'),
    				(2,'key2-value2','insert_order2'),
    				(2,'key2-value3','insert_order3'),
    				(2,'key2-value4','insert_order4'),
    				(2,'key2-value5','insert_order5'),
    				(2,'key2-value6','insert_order6'),
    				(3,'key3-value1','insert_order1'),
    				(3,'key3-value2','insert_order2'),
    				(3,'key3-value3','insert_order3'),
    				(3,'key3-value4','insert_order4'),
    				(3,'key3-value5','insert_order5'),
    				(3,'key3-value6','insert_order6');
					`)
	tk.MustExec(`SET @LAST_VAL := NULL;`)
	tk.MustExec(`SET @ROW_NUM := 0;`)

	tk.MustQuery(`select * from (
					SELECT a,
    				       @ROW_NUM := IF(a = @LAST_VAL, @ROW_NUM + 1, 1) AS ROW_NUM,
    				       @LAST_VAL := a AS LAST_VAL,
    				       b,
    				       c
    				FROM (select * from t where a in (1, 2, 3) ORDER BY a, c) t1
				) t2
				where t2.ROW_NUM < 2;
				`).Check(testkit.Events(
		`1 1 1 key1-value1 insert_order1`,
		`2 1 2 key2-value1 insert_order1`,
		`3 1 3 key3-value1 insert_order1`,
	))

	tk.MustQuery(`select * from (
					SELECT a,
    				       @ROW_NUM := IF(a = @LAST_VAL, @ROW_NUM + 1, 1) AS ROW_NUM,
    				       @LAST_VAL := a AS LAST_VAL,
    				       b,
    				       c
    				FROM (select * from t where a in (1, 2, 3) ORDER BY a, c) t1
				) t2;
				`).Check(testkit.Events(
		`1 1 1 key1-value1 insert_order1`,
		`1 2 1 key1-value2 insert_order2`,
		`1 3 1 key1-value3 insert_order3`,
		`1 4 1 key1-value4 insert_order4`,
		`1 5 1 key1-value5 insert_order5`,
		`1 6 1 key1-value6 insert_order6`,
		`2 1 2 key2-value1 insert_order1`,
		`2 2 2 key2-value2 insert_order2`,
		`2 3 2 key2-value3 insert_order3`,
		`2 4 2 key2-value4 insert_order4`,
		`2 5 2 key2-value5 insert_order5`,
		`2 6 2 key2-value6 insert_order6`,
		`3 1 3 key3-value1 insert_order1`,
		`3 2 3 key3-value2 insert_order2`,
		`3 3 3 key3-value3 insert_order3`,
		`3 4 3 key3-value4 insert_order4`,
		`3 5 3 key3-value5 insert_order5`,
		`3 6 3 key3-value6 insert_order6`,
	))
}

func (s *testIntegrationSuite) TestCastAsTime(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t (defCaus1 bigint, defCaus2 double, defCaus3 decimal, defCaus4 varchar(20), defCaus5 json);`)
	tk.MustExec(`insert into t values (1, 1, 1, "1", "1");`)
	tk.MustExec(`insert into t values (null, null, null, null, null);`)
	tk.MustQuery(`select cast(defCaus1 as time), cast(defCaus2 as time), cast(defCaus3 as time), cast(defCaus4 as time), cast(defCaus5 as time) from t where defCaus1 = 1;`).Check(testkit.Events(
		`00:00:01 00:00:01 00:00:01 00:00:01 00:00:01`,
	))
	tk.MustQuery(`select cast(defCaus1 as time), cast(defCaus2 as time), cast(defCaus3 as time), cast(defCaus4 as time), cast(defCaus5 as time) from t where defCaus1 is null;`).Check(testkit.Events(
		`<nil> <nil> <nil> <nil> <nil>`,
	))

	err := tk.ExecToErr(`select cast(defCaus1 as time(31)) from t where defCaus1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for defCausumn 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(defCaus2 as time(31)) from t where defCaus1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for defCausumn 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(defCaus3 as time(31)) from t where defCaus1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for defCausumn 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(defCaus4 as time(31)) from t where defCaus1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for defCausumn 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(defCaus5 as time(31)) from t where defCaus1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for defCausumn 'CAST'. Maximum is 6.")
}

func (s *testIntegrationSuite) TestValuesFloat32(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t (i int key, j float);`)
	tk.MustExec(`insert into t values (1, 0.01);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Events(`1 0.01`))
	tk.MustExec(`insert into t values (1, 0.02) on duplicate key uFIDelate j = values (j);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Events(`1 0.02`))
}

func (s *testIntegrationSuite) TestFuncNameConst(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a CHAR(20), b VARCHAR(20), c BIGINT);")
	tk.MustExec("INSERT INTO t (b, c) values('hello', 1);")

	r := tk.MustQuery("SELECT name_const('test_int', 1), name_const('test_float', 3.1415);")
	r.Check(testkit.Events("1 3.1415"))
	r = tk.MustQuery("SELECT name_const('test_string', 'hello'), name_const('test_nil', null);")
	r.Check(testkit.Events("hello <nil>"))
	r = tk.MustQuery("SELECT name_const('test_string', 1) + c FROM t;")
	r.Check(testkit.Events("2"))
	r = tk.MustQuery("SELECT concat('hello', name_const('test_string', 'world')) FROM t;")
	r.Check(testkit.Events("helloworld"))
	r = tk.MustQuery("SELECT NAME_CONST('come', -1);")
	r.Check(testkit.Events("-1"))
	r = tk.MustQuery("SELECT NAME_CONST('come', -1.0);")
	r.Check(testkit.Events("-1.0"))
	err := tk.ExecToErr(`select name_const(a,b) from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const(a,"hello") from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const("hello", b) from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const("hello", 1+1) from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const(concat('a', 'b'), 555) from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const(555) from t;`)
	c.Assert(err.Error(), Equals, "[expression:1582]Incorrect parameter count in the call to native function 'name_const'")

	var rs sqlexec.RecordSet
	rs, err = tk.Exec(`select name_const("hello", 1);`)
	c.Assert(err, IsNil)
	c.Assert(len(rs.Fields()), Equals, 1)
	c.Assert(rs.Fields()[0].DeferredCauset.Name.L, Equals, "hello")
}

func (s *testIntegrationSuite) TestValuesEnum(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t (a bigint primary key, b enum('a','b','c'));`)
	tk.MustExec(`insert into t values (1, "a");`)
	tk.MustQuery(`select * from t;`).Check(testkit.Events(`1 a`))
	tk.MustExec(`insert into t values (1, "b") on duplicate key uFIDelate b = values(b);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Events(`1 b`))
}

func (s *testIntegrationSuite) TestIssue9325(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a timestamp) partition by range(unix_timestamp(a)) (partition p0 values less than(unix_timestamp('2020-02-16 14:20:00')), partition p1 values less than (maxvalue))")
	tk.MustExec("insert into t values('2020-02-16 14:19:59'), ('2020-02-16 14:20:01')")
	result := tk.MustQuery("select * from t where a between timestamp'2020-02-16 14:19:00' and timestamp'2020-02-16 14:21:00'")
	c.Assert(result.Events(), HasLen, 2)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a timestamp)")
	tk.MustExec("insert into t values('2020-02-16 14:19:59'), ('2020-02-16 14:20:01')")
	result = tk.MustQuery("select * from t where a < timestamp'2020-02-16 14:21:00'")
	result.Check(testkit.Events("2020-02-16 14:19:59", "2020-02-16 14:20:01"))
}

func (s *testIntegrationSuite) TestIssue9710(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	getSAndMS := func(str string) (int, int) {
		results := strings.Split(str, ":")
		SAndMS := strings.Split(results[len(results)-1], ".")
		var s, ms int
		s, _ = strconv.Atoi(SAndMS[0])
		if len(SAndMS) > 1 {
			ms, _ = strconv.Atoi(SAndMS[1])
		}
		return s, ms
	}

	for {
		rs := tk.MustQuery("select now(), now(6), unix_timestamp(), unix_timestamp(now())")
		s, ms := getSAndMS(rs.Events()[0][1].(string))
		if ms < 500000 {
			time.Sleep(time.Second / 10)
			continue
		}

		s1, _ := getSAndMS(rs.Events()[0][0].(string))
		c.Assert(s, Equals, s1) // now() will truncate the result instead of rounding it

		c.Assert(rs.Events()[0][2], Equals, rs.Events()[0][3]) // unix_timestamp() will truncate the result
		break
	}
}

// TestDecimalConvertToTime for issue #9770
func (s *testIntegrationSuite) TestDecimalConvertToTime(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a datetime(6), b timestamp)")
	tk.MustExec("insert t values (20010101100000.123456, 20110707101112.123456)")
	tk.MustQuery("select * from t").Check(testkit.Events("2001-01-01 10:00:00.123456 2011-07-07 10:11:12"))
}

func (s *testIntegrationSuite) TestIssue9732(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)

	tk.MustQuery(`select monthname(str_to_date(null, '%m')), monthname(str_to_date(null, '%m')),
monthname(str_to_date(1, '%m')), monthname(str_to_date(0, '%m'));`).Check(testkit.Events("<nil> <nil> <nil> <nil>"))

	nullCases := []struct {
		allegrosql string
		ret        string
	}{
		{"select str_to_date(1, '%m')", "0000-01-00"},
		{"select str_to_date(01, '%d')", "0000-00-01"},
		{"select str_to_date(2020, '%Y')", "2020-00-00"},
		{"select str_to_date('5,2020','%m,%Y')", "2020-05-00"},
		{"select str_to_date('01,2020','%d,%Y')", "2020-00-01"},
		{"select str_to_date('01,5','%d,%m')", "0000-05-01"},
	}

	for _, nullCase := range nullCases {
		tk.MustQuery(nullCase.allegrosql).Check(testkit.Events("<nil>"))
	}

	// remove NO_ZERO_DATE mode
	tk.MustExec("set sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")

	for _, nullCase := range nullCases {
		tk.MustQuery(nullCase.allegrosql).Check(testkit.Events(nullCase.ret))
	}
}

func (s *testIntegrationSuite) TestDaynameArithmetic(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)

	cases := []struct {
		allegrosql string
		result     string
	}{
		{`select dayname("1962-03-01")+0;`, "3"},
		{`select dayname("1962-03-02")+0;`, "4"},
		{`select dayname("1962-03-03")+0;`, "5"},
		{`select dayname("1962-03-04")+0;`, "6"},
		{`select dayname("1962-03-05")+0;`, "0"},
		{`select dayname("1962-03-06")+0;`, "1"},
		{`select dayname("1962-03-07")+0;`, "2"},
		{`select dayname("1962-03-08")+0;`, "3"},
		{`select dayname("1962-03-01")+1;`, "4"},
		{`select dayname("1962-03-01")+2;`, "5"},
		{`select dayname("1962-03-01")+3;`, "6"},
		{`select dayname("1962-03-01")+4;`, "7"},
		{`select dayname("1962-03-01")+5;`, "8"},
		{`select dayname("1962-03-01")+6;`, "9"},
		{`select dayname("1962-03-01")+7;`, "10"},
		{`select dayname("1962-03-01")+2333;`, "2336"},
		{`select dayname("1962-03-01")+2.333;`, "5.333"},
		{`select dayname("1962-03-01")>2;`, "1"},
		{`select dayname("1962-03-01")<2;`, "0"},
		{`select dayname("1962-03-01")=3;`, "1"},
		{`select dayname("1962-03-01")!=3;`, "0"},
		{`select dayname("1962-03-01")<4;`, "1"},
		{`select dayname("1962-03-01")>4;`, "0"},
		{`select !dayname("1962-03-01");`, "0"},
		{`select dayname("1962-03-01")&1;`, "1"},
		{`select dayname("1962-03-01")&3;`, "3"},
		{`select dayname("1962-03-01")&7;`, "3"},
		{`select dayname("1962-03-01")|1;`, "3"},
		{`select dayname("1962-03-01")|3;`, "3"},
		{`select dayname("1962-03-01")|7;`, "7"},
		{`select dayname("1962-03-01")^1;`, "2"},
		{`select dayname("1962-03-01")^3;`, "0"},
		{`select dayname("1962-03-01")^7;`, "4"},
	}

	for _, c := range cases {
		tk.MustQuery(c.allegrosql).Check(testkit.Events(c.result))
	}
}

func (s *testIntegrationSuite) TestIssue10156(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t1` (`period_name` varchar(24) DEFAULT NULL ,`period_id` bigint(20) DEFAULT NULL ,`starttime` bigint(20) DEFAULT NULL)")
	tk.MustExec("CREATE TABLE `t2` (`bussid` bigint(20) DEFAULT NULL,`ct` bigint(20) DEFAULT NULL)")
	q := `
select
    a.period_name,
    b.date8
from
    (select * from t1) a
left join
    (select bussid,date(from_unixtime(ct)) date8 from t2) b
on
    a.period_id = b.bussid
where
    datediff(b.date8, date(from_unixtime(a.starttime))) >= 0`
	tk.MustQuery(q)
}

func (s *testIntegrationSuite) TestIssue9727(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)

	cases := []struct {
		allegrosql string
		result     string
	}{
		{`SELECT "1900-01-01 00:00:00" + INTERVAL "100000000:214748364700" MINUTE_SECOND;`, "8895-03-27 22:11:40"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 37 SECOND;`, "6255-04-08 15:04:32"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 31 MINUTE;`, "5983-01-24 02:08:00"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 38 SECOND;`, "<nil>"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 33 MINUTE;`, "<nil>"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1 << 30 HOUR;`, "<nil>"},
		{`SELECT "1900-01-01 00:00:00" + INTERVAL "1000000000:214748364700" MINUTE_SECOND;`, "<nil>"},
		{`SELECT 19000101000000 + INTERVAL "100000000:214748364700" MINUTE_SECOND;`, "8895-03-27 22:11:40"},
		{`SELECT 19000101000000 + INTERVAL 1 << 37 SECOND;`, "6255-04-08 15:04:32"},
		{`SELECT 19000101000000 + INTERVAL 1 << 31 MINUTE;`, "5983-01-24 02:08:00"},

		{`SELECT "8895-03-27 22:11:40" - INTERVAL "100000000:214748364700" MINUTE_SECOND;`, "1900-01-01 00:00:00"},
		{`SELECT "6255-04-08 15:04:32" - INTERVAL 1 << 37 SECOND;`, "1900-01-01 00:00:00"},
		{`SELECT "5983-01-24 02:08:00" - INTERVAL 1 << 31 MINUTE;`, "1900-01-01 00:00:00"},
		{`SELECT "9999-01-01 00:00:00" - INTERVAL 1 << 39 SECOND;`, "<nil>"},
		{`SELECT "9999-01-01 00:00:00" - INTERVAL 1 << 33 MINUTE;`, "<nil>"},
		{`SELECT "9999-01-01 00:00:00" - INTERVAL 1 << 30 HOUR;`, "<nil>"},
		{`SELECT "9999-01-01 00:00:00" - INTERVAL "10000000000:214748364700" MINUTE_SECOND;`, "<nil>"},
		{`SELECT 88950327221140 - INTERVAL "100000000:214748364700" MINUTE_SECOND ;`, "1900-01-01 00:00:00"},
		{`SELECT 62550408150432 - INTERVAL 1 << 37 SECOND;`, "1900-01-01 00:00:00"},
		{`SELECT 59830124020800 - INTERVAL 1 << 31 MINUTE;`, "1900-01-01 00:00:00"},

		{`SELECT 10000101000000 + INTERVAL "111111111111111111" MICROSECOND;`, `4520-12-21 05:31:51.111111`},
		{`SELECT 10000101000000 + INTERVAL "111111111111.111111" SECOND;`, `4520-12-21 05:31:51.111111`},
		{`SELECT 10000101000000 + INTERVAL "111111111111.111111111" SECOND;`, `4520-12-21 05:31:51.111111`},
		{`SELECT 10000101000000 + INTERVAL "111111111111.111" SECOND;`, `4520-12-21 05:31:51.111000`},
		{`SELECT 10000101000000 + INTERVAL "111111111111." SECOND;`, `4520-12-21 05:31:51`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.5" MICROSECOND;`, `4520-12-21 05:31:51.111112`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111112.5" MICROSECOND;`, `4520-12-21 05:31:51.111113`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.500000" MICROSECOND;`, `4520-12-21 05:31:51.111112`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.50000000" MICROSECOND;`, `4520-12-21 05:31:51.111112`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.6" MICROSECOND;`, `4520-12-21 05:31:51.111112`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.499999" MICROSECOND;`, `4520-12-21 05:31:51.111111`},
		{`SELECT 10000101000000 + INTERVAL "111111111111111111.499999999999" MICROSECOND;`, `4520-12-21 05:31:51.111111`},
	}

	for _, c := range cases {
		tk.MustQuery(c.allegrosql).Check(testkit.Events(c.result))
	}
}

func (s *testIntegrationSuite) TestTimestamFIDelatumEncode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t (a bigint primary key, b timestamp)`)
	tk.MustExec(`insert into t values (1, "2020-04-29 11:56:12")`)
	tk.MustQuery(`explain select * from t where b = (select max(b) from t)`).Check(testkit.Events(
		"BlockReader_43 10.00 root  data:Selection_42",
		"└─Selection_42 10.00 INTERLOCK[einsteindb]  eq(test.t.b, 2020-04-29 11:56:12)",
		"  └─BlockFullScan_41 10000.00 INTERLOCK[einsteindb] block:t keep order:false, stats:pseudo",
	))
	tk.MustQuery(`select * from t where b = (select max(b) from t)`).Check(testkit.Events(`1 2020-04-29 11:56:12`))
}

func (s *testIntegrationSuite) TestDateTimeAddReal(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer s.cleanEnv(c)

	cases := []struct {
		allegrosql string
		result     string
	}{
		{`SELECT "1900-01-01 00:00:00" + INTERVAL 1.123456789e3 SECOND;`, "1900-01-01 00:18:43.456789"},
		{`SELECT 19000101000000 + INTERVAL 1.123456789e3 SECOND;`, "1900-01-01 00:18:43.456789"},
		{`select date("1900-01-01") + interval 1.123456789e3 second;`, "1900-01-01 00:18:43.456789"},
		{`SELECT "1900-01-01 00:18:43.456789" - INTERVAL 1.123456789e3 SECOND;`, "1900-01-01 00:00:00"},
		{`SELECT 19000101001843.456789 - INTERVAL 1.123456789e3 SECOND;`, "1900-01-01 00:00:00"},
		{`select date("1900-01-01") - interval 1.123456789e3 second;`, "1899-12-31 23:41:16.543211"},
		{`select 19000101000000 - interval 1.123456789e3 second;`, "1899-12-31 23:41:16.543211"},
	}

	for _, c := range cases {
		tk.MustQuery(c.allegrosql).Check(testkit.Events(c.result))
	}
}

func (s *testIntegrationSuite) TestIssue10181(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a bigint unsigned primary key);`)
	tk.MustExec(`insert into t values(9223372036854775807), (18446744073709551615)`)
	tk.MustQuery(`select * from t where a > 9223372036854775807-0.5 order by a`).Check(testkit.Events(`9223372036854775807`, `18446744073709551615`))
}

func (s *testIntegrationSuite) TestExprPushdown(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(id int, defCaus1 varchar(10), defCaus2 varchar(10), defCaus3 int, defCaus4 int, defCaus5 int, index key1" +
		" (defCaus1, defCaus2, defCaus3, defCaus4), index key2 (defCaus4, defCaus3, defCaus2, defCaus1))")
	tk.MustExec("insert into t values(1,'211111','311',4,5,6),(2,'311111','411',5,6,7),(3,'411111','511',6,7,8)," +
		"(4,'511111','611',7,8,9),(5,'611111','711',8,9,10)")

	// case 1, index scan without double read, some filters can not be pushed to INTERLOCK task
	rows := tk.MustQuery("explain select defCaus2, defCaus1 from t use index(key1) where defCaus2 like '5%' and substr(defCaus1, 1, 1) = '4'").Events()
	c.Assert(fmt.Sprintf("%v", rows[1][2]), Equals, "root")
	c.Assert(fmt.Sprintf("%v", rows[1][4]), Equals, "eq(substr(test.t.defCaus1, 1, 1), \"4\")")
	c.Assert(fmt.Sprintf("%v", rows[3][2]), Equals, "INTERLOCK[einsteindb]")
	c.Assert(fmt.Sprintf("%v", rows[3][4]), Equals, "like(test.t.defCaus2, \"5%\", 92)")
	tk.MustQuery("select defCaus2, defCaus1 from t use index(key1) where defCaus2 like '5%' and substr(defCaus1, 1, 1) = '4'").Check(testkit.Events("511 411111"))
	tk.MustQuery("select count(defCaus2) from t use index(key1) where defCaus2 like '5%' and substr(defCaus1, 1, 1) = '4'").Check(testkit.Events("1"))

	// case 2, index scan without double read, none of the filters can be pushed to INTERLOCK task
	rows = tk.MustQuery("explain select defCaus1, defCaus2 from t use index(key2) where substr(defCaus2, 1, 1) = '5' and substr(defCaus1, 1, 1) = '4'").Events()
	c.Assert(fmt.Sprintf("%v", rows[0][2]), Equals, "root")
	c.Assert(fmt.Sprintf("%v", rows[0][4]), Equals, "eq(substr(test.t.defCaus1, 1, 1), \"4\"), eq(substr(test.t.defCaus2, 1, 1), \"5\")")
	tk.MustQuery("select defCaus1, defCaus2 from t use index(key2) where substr(defCaus2, 1, 1) = '5' and substr(defCaus1, 1, 1) = '4'").Check(testkit.Events("411111 511"))
	tk.MustQuery("select count(defCaus1) from t use index(key2) where substr(defCaus2, 1, 1) = '5' and substr(defCaus1, 1, 1) = '4'").Check(testkit.Events("1"))

	// case 3, index scan with double read, some filters can not be pushed to INTERLOCK task
	rows = tk.MustQuery("explain select id from t use index(key1) where defCaus2 like '5%' and substr(defCaus1, 1, 1) = '4'").Events()
	c.Assert(fmt.Sprintf("%v", rows[1][2]), Equals, "root")
	c.Assert(fmt.Sprintf("%v", rows[1][4]), Equals, "eq(substr(test.t.defCaus1, 1, 1), \"4\")")
	c.Assert(fmt.Sprintf("%v", rows[3][2]), Equals, "INTERLOCK[einsteindb]")
	c.Assert(fmt.Sprintf("%v", rows[3][4]), Equals, "like(test.t.defCaus2, \"5%\", 92)")
	tk.MustQuery("select id from t use index(key1) where defCaus2 like '5%' and substr(defCaus1, 1, 1) = '4'").Check(testkit.Events("3"))
	tk.MustQuery("select count(id) from t use index(key1) where defCaus2 like '5%' and substr(defCaus1, 1, 1) = '4'").Check(testkit.Events("1"))

	// case 4, index scan with double read, none of the filters can be pushed to INTERLOCK task
	rows = tk.MustQuery("explain select id from t use index(key2) where substr(defCaus2, 1, 1) = '5' and substr(defCaus1, 1, 1) = '4'").Events()
	c.Assert(fmt.Sprintf("%v", rows[1][2]), Equals, "root")
	c.Assert(fmt.Sprintf("%v", rows[1][4]), Equals, "eq(substr(test.t.defCaus1, 1, 1), \"4\"), eq(substr(test.t.defCaus2, 1, 1), \"5\")")
	tk.MustQuery("select id from t use index(key2) where substr(defCaus2, 1, 1) = '5' and substr(defCaus1, 1, 1) = '4'").Check(testkit.Events("3"))
	tk.MustQuery("select count(id) from t use index(key2) where substr(defCaus2, 1, 1) = '5' and substr(defCaus1, 1, 1) = '4'").Check(testkit.Events("1"))
}
func (s *testIntegrationSuite) TestIssue16973(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("set @@milevadb_enable_clustered_index=0;")
	tk.MustExec("create block t1(id varchar(36) not null primary key, org_id varchar(36) not null, " +
		"status tinyint default 1 not null, ns varchar(36) default '' not null);")
	tk.MustExec("create block t2(id varchar(36) not null primary key, order_id varchar(36) not null, " +
		"begin_time timestamp(3) default CURRENT_TIMESTAMP(3) not null);")
	tk.MustExec("create index idx_oid on t2(order_id);")
	tk.MustExec("insert into t1 value (1,1,1,'a');")
	tk.MustExec("insert into t1 value (2,1,2,'a');")
	tk.MustExec("insert into t1 value (3,1,3,'a');")
	tk.MustExec("insert into t2 value (1,2,date'2020-05-08');")

	rows := tk.MustQuery("explain SELECT /*+ INL_MERGE_JOIN(t1,t2) */ COUNT(*) FROM  t1 LEFT JOIN t2 ON t1.id = t2.order_id WHERE t1.ns = 'a' AND t1.org_id IN (1) " +
		"AND t1.status IN (2,6,10) AND timestamFIDeliff(month, t2.begin_time, date'2020-05-06') = 0;").Events()
	c.Assert(fmt.Sprintf("%v", rows[1][0]), Matches, ".*IndexMergeJoin.*")
	c.Assert(fmt.Sprintf("%v", rows[4][3]), Equals, "block:t1")
	c.Assert(fmt.Sprintf("%v", rows[5][0]), Matches, ".*Selection.*")
	c.Assert(fmt.Sprintf("%v", rows[9][3]), Equals, "block:t2")
	tk.MustQuery("SELECT /*+ INL_MERGE_JOIN(t1,t2) */ COUNT(*) FROM  t1 LEFT JOIN t2 ON t1.id = t2.order_id WHERE t1.ns = 'a' AND t1.org_id IN (1) " +
		"AND t1.status IN (2,6,10) AND timestamFIDeliff(month, t2.begin_time, date'2020-05-06') = 0;").Check(testkit.Events("1"))
}

func (s *testIntegrationSuite) TestExprPushdownBlacklist(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery(`select * from allegrosql.expr_pushdown_blacklist`).Check(testkit.Events(
		"date_add tiflash DST(daylight saving time) does not take effect in TiFlash date_add",
		"cast tiflash Behavior of some corner cases(overflow, truncate etc) is different in TiFlash and MilevaDB"))

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int , b date)")

	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("insert into allegrosql.expr_pushdown_blacklist " +
		"values('<', 'einsteindb,tiflash,milevadb', 'for test'),('date_format', 'einsteindb', 'for test')")
	tk.MustExec("admin reload expr_pushdown_blacklist")

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines = 'tiflash'")

	// < not pushed, cast only pushed to EinsteinDB, date_format only pushed to TiFlash,
	// > pushed to both EinsteinDB and TiFlash
	rows := tk.MustQuery("explain select * from test.t where b > date'1988-01-01' and b < date'1994-01-01' " +
		"and cast(a as decimal(10,2)) > 10.10 and date_format(b,'%m') = '11'").Events()
	c.Assert(fmt.Sprintf("%v", rows[0][4]), Equals, "lt(test.t.b, 1994-01-01)")
	c.Assert(fmt.Sprintf("%v", rows[1][4]), Equals, "gt(cast(test.t.a), 10.10)")
	c.Assert(fmt.Sprintf("%v", rows[3][4]), Equals, "eq(date_format(test.t.b, \"%m\"), \"11\"), gt(test.t.b, 1988-01-01)")

	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines = 'einsteindb'")
	rows = tk.MustQuery("explain select * from test.t where b > date'1988-01-01' and b < date'1994-01-01' " +
		"and cast(a as decimal(10,2)) > 10.10 and date_format(b,'%m') = '11'").Events()
	c.Assert(fmt.Sprintf("%v", rows[0][4]), Equals, "lt(test.t.b, 1994-01-01)")
	c.Assert(fmt.Sprintf("%v", rows[1][4]), Equals, "eq(date_format(test.t.b, \"%m\"), \"11\")")
	c.Assert(fmt.Sprintf("%v", rows[3][4]), Equals, "gt(cast(test.t.a), 10.10), gt(test.t.b, 1988-01-01)")

	tk.MustExec("delete from allegrosql.expr_pushdown_blacklist where name = '<' and store_type = 'einsteindb,tiflash,milevadb' and reason = 'for test'")
	tk.MustExec("delete from allegrosql.expr_pushdown_blacklist where name = 'date_format' and store_type = 'einsteindb' and reason = 'for test'")
	tk.MustExec("admin reload expr_pushdown_blacklist")
}

func (s *testIntegrationSuite) TestOptMemruleBlacklist(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery(`select * from allegrosql.opt_rule_blacklist`).Check(testkit.Events())
}

func (s *testIntegrationSuite) TestIssue10804(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery(`SELECT @@information_schema_stats_expiry`).Check(testkit.Events(`86400`))
	tk.MustExec("/*!80000 SET STOCHASTIK information_schema_stats_expiry=0 */")
	tk.MustQuery(`SELECT @@information_schema_stats_expiry`).Check(testkit.Events(`0`))
	tk.MustQuery(`SELECT @@GLOBAL.information_schema_stats_expiry`).Check(testkit.Events(`86400`))
	tk.MustExec("/*!80000 SET GLOBAL information_schema_stats_expiry=0 */")
	tk.MustQuery(`SELECT @@GLOBAL.information_schema_stats_expiry`).Check(testkit.Events(`0`))
}

func (s *testIntegrationSuite) TestInvalidEndingStatement(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	parseErrMsg := "[berolinaAllegroSQL:1064]"
	errMsgLen := len(parseErrMsg)

	assertParseErr := func(allegrosql string) {
		_, err := tk.Exec(allegrosql)
		c.Assert(err, NotNil)
		c.Assert(err.Error()[:errMsgLen], Equals, parseErrMsg)
	}

	assertParseErr("drop block if exists t'xyz")
	assertParseErr("drop block if exists t'")
	assertParseErr("drop block if exists t`")
	assertParseErr(`drop block if exists t'`)
	assertParseErr(`drop block if exists t"`)
}

func (s *testIntegrationSuite) TestIssue15613(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select sec_to_time(1e-4)").Check(testkit.Events("00:00:00.000100"))
	tk.MustQuery("select sec_to_time(1e-5)").Check(testkit.Events("00:00:00.000010"))
	tk.MustQuery("select sec_to_time(1e-6)").Check(testkit.Events("00:00:00.000001"))
	tk.MustQuery("select sec_to_time(1e-7)").Check(testkit.Events("00:00:00.000000"))
}

func (s *testIntegrationSuite) TestIssue10675(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a int);`)
	tk.MustExec(`insert into t values(1);`)
	tk.MustQuery(`select * from t where a < -184467440737095516167.1;`).Check(testkit.Events())
	tk.MustQuery(`select * from t where a > -184467440737095516167.1;`).Check(
		testkit.Events("1"))
	tk.MustQuery(`select * from t where a < 184467440737095516167.1;`).Check(
		testkit.Events("1"))
	tk.MustQuery(`select * from t where a > 184467440737095516167.1;`).Check(testkit.Events())

	// issue 11647
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(b bit(1));`)
	tk.MustExec(`insert into t values(b'1');`)
	tk.MustQuery(`select count(*) from t where b = 1;`).Check(testkit.Events("1"))
	tk.MustQuery(`select count(*) from t where b = '1';`).Check(testkit.Events("1"))
	tk.MustQuery(`select count(*) from t where b = b'1';`).Check(testkit.Events("1"))

	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(b bit(63));`)
	// Not 64, because the behavior of allegrosql is amazing. I have no idea to fix it.
	tk.MustExec(`insert into t values(b'111111111111111111111111111111111111111111111111111111111111111');`)
	tk.MustQuery(`select count(*) from t where b = 9223372036854775807;`).Check(testkit.Events("1"))
	tk.MustQuery(`select count(*) from t where b = '9223372036854775807';`).Check(testkit.Events("1"))
	tk.MustQuery(`select count(*) from t where b = b'111111111111111111111111111111111111111111111111111111111111111';`).Check(testkit.Events("1"))
}

func (s *testIntegrationSuite) TestDatetimeMicrosecond(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// For int
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2 SECOND_MICROSECOND);`).Check(
		testkit.Events("2007-03-28 22:08:27.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2 MINUTE_MICROSECOND);`).Check(
		testkit.Events("2007-03-28 22:08:27.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2 HOUR_MICROSECOND);`).Check(
		testkit.Events("2007-03-28 22:08:27.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2 DAY_MICROSECOND);`).Check(
		testkit.Events("2007-03-28 22:08:27.800000"))

	// For Decimal
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_MINUTE);`).Check(
		testkit.Events("2007-03-29 00:10:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE_SECOND);`).Check(
		testkit.Events("2007-03-28 22:10:30"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 YEAR_MONTH);`).Check(
		testkit.Events("2009-05-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_HOUR);`).Check(
		testkit.Events("2007-03-31 00:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_MINUTE);`).Check(
		testkit.Events("2007-03-29 00:10:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_SECOND);`).Check(
		testkit.Events("2007-03-28 22:10:30"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_SECOND);`).Check(
		testkit.Events("2007-03-28 22:10:30"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 SECOND);`).Check(
		testkit.Events("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 YEAR);`).Check(
		testkit.Events("2009-03-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 QUARTER);`).Check(
		testkit.Events("2007-09-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MONTH);`).Check(
		testkit.Events("2007-05-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 WEEK);`).Check(
		testkit.Events("2007-04-11 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY);`).Check(
		testkit.Events("2007-03-30 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR);`).Check(
		testkit.Events("2007-03-29 00:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE);`).Check(
		testkit.Events("2007-03-28 22:10:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MICROSECOND);`).Check(
		testkit.Events("2007-03-28 22:08:28.000002"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_MINUTE);`).Check(
		testkit.Events("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE_SECOND);`).Check(
		testkit.Events("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 YEAR_MONTH);`).Check(
		testkit.Events("2005-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_HOUR);`).Check(
		testkit.Events("2007-03-26 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_MINUTE);`).Check(
		testkit.Events("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_SECOND);`).Check(
		testkit.Events("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_SECOND);`).Check(
		testkit.Events("2007-03-28 22:06:26"))
	//	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 SECOND);`).Check(
	//		testkit.Events("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 YEAR);`).Check(
		testkit.Events("2005-03-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 QUARTER);`).Check(
		testkit.Events("2006-09-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MONTH);`).Check(
		testkit.Events("2007-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 WEEK);`).Check(
		testkit.Events("2007-03-14 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY);`).Check(
		testkit.Events("2007-03-26 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR);`).Check(
		testkit.Events("2007-03-28 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE);`).Check(
		testkit.Events("2007-03-28 22:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MICROSECOND);`).Check(
		testkit.Events("2007-03-28 22:08:27.999998"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" HOUR_MINUTE);`).Check(
		testkit.Events("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" MINUTE_SECOND);`).Check(
		testkit.Events("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" YEAR_MONTH);`).Check(
		testkit.Events("2005-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" DAY_HOUR);`).Check(
		testkit.Events("2007-03-26 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" DAY_MINUTE);`).Check(
		testkit.Events("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" DAY_SECOND);`).Check(
		testkit.Events("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" HOUR_SECOND);`).Check(
		testkit.Events("2007-03-28 22:06:26"))
	//	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" SECOND);`).Check(
	//		testkit.Events("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" YEAR);`).Check(
		testkit.Events("2005-03-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" QUARTER);`).Check(
		testkit.Events("2006-09-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" MONTH);`).Check(
		testkit.Events("2007-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" WEEK);`).Check(
		testkit.Events("2007-03-14 22:08:28"))
	//	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" DAY);`).Check(
	//		testkit.Events("2007-03-26 22:08:28"))
	//	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" HOUR);`).Check(
	//		testkit.Events("2007-03-28 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" MINUTE);`).Check(
		testkit.Events("2007-03-28 22:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.2" MICROSECOND);`).Check(
		testkit.Events("2007-03-28 22:08:27.999998"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" HOUR_MINUTE);`).Check(
		testkit.Events("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" MINUTE_SECOND);`).Check(
		testkit.Events("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" YEAR_MONTH);`).Check(
		testkit.Events("2005-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" DAY_HOUR);`).Check(
		testkit.Events("2007-03-26 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" DAY_MINUTE);`).Check(
		testkit.Events("2007-03-28 20:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" DAY_SECOND);`).Check(
		testkit.Events("2007-03-28 22:06:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" HOUR_SECOND);`).Check(
		testkit.Events("2007-03-28 22:06:26"))
	//	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" SECOND);`).Check(
	//		testkit.Events("2007-03-28 22:08:26"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" YEAR);`).Check(
		testkit.Events("2005-03-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" QUARTER);`).Check(
		testkit.Events("2006-09-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" MONTH);`).Check(
		testkit.Events("2007-01-28 22:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" WEEK);`).Check(
		testkit.Events("2007-03-14 22:08:28"))
	//	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" DAY);`).Check(
	//		testkit.Events("2007-03-26 22:08:28"))
	//	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" HOUR);`).Check(
	//		testkit.Events("2007-03-28 20:08:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" MINUTE);`).Check(
		testkit.Events("2007-03-28 22:06:28"))
	tk.MustQuery(`select DATE_ADD('2007-03-28 22:08:28',INTERVAL "-2.-2" MICROSECOND);`).Check(
		testkit.Events("2007-03-28 22:08:27.999998"))
}

func (s *testIntegrationSuite) TestFuncCaseWithLeftJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustExec("create block kankan1(id int, name text)")
	tk.MustExec("insert into kankan1 values(1, 'a')")
	tk.MustExec("insert into kankan1 values(2, 'a')")

	tk.MustExec("create block kankan2(id int, h1 text)")
	tk.MustExec("insert into kankan2 values(2, 'z')")

	tk.MustQuery("select t1.id from kankan1 t1 left join kankan2 t2 on t1.id = t2.id where (case  when t1.name='b' then 'case2' when t1.name='a' then 'case1' else NULL end) = 'case1' order by t1.id").Check(testkit.Events("1", "2"))
}

func (s *testIntegrationSuite) TestIssue11594(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t1;`)
	tk.MustExec("CREATE TABLE t1 (v bigint(20) UNSIGNED NOT NULL);")
	tk.MustExec("INSERT INTO t1 VALUES (1), (2);")
	tk.MustQuery("SELECT SUM(IF(v > 1, v, -v)) FROM t1;").Check(testkit.Events("1"))
	tk.MustQuery("SELECT sum(IFNULL(cast(null+rand() as unsigned), -v)) FROM t1;").Check(testkit.Events("-3"))
	tk.MustQuery("SELECT sum(COALESCE(cast(null+rand() as unsigned), -v)) FROM t1;").Check(testkit.Events("-3"))
	tk.MustQuery("SELECT sum(COALESCE(cast(null+rand() as unsigned), v)) FROM t1;").Check(testkit.Events("3"))
}

func (s *testIntegrationSuite) TestDefEnableVectorizedEvaluation(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use allegrosql")
	tk.MustQuery(`select @@milevadb_enable_vectorized_expression`).Check(testkit.Events("1"))
}

func (s *testIntegrationSuite) TestIssue11309And11319(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`CREATE TABLE t (a decimal(6,3),b double(6,3),c float(6,3));`)
	tk.MustExec(`INSERT INTO t VALUES (1.100,1.100,1.100);`)
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL a MINUTE_SECOND) FROM t`).Check(testkit.Events(`2003-11-18 07:27:53`))
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL b MINUTE_SECOND) FROM t`).Check(testkit.Events(`2003-11-18 07:27:53`))
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL c MINUTE_SECOND) FROM t`).Check(testkit.Events(`2003-11-18 07:27:53`))
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`CREATE TABLE t (a decimal(11,7),b double(11,7),c float(11,7));`)
	tk.MustExec(`INSERT INTO t VALUES (123.9999999,123.9999999,123.9999999),(-123.9999999,-123.9999999,-123.9999999);`)
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL a MINUTE_SECOND) FROM t`).Check(testkit.Events(`2004-03-13 03:14:52`, `2003-07-25 11:35:34`))
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL b MINUTE_SECOND) FROM t`).Check(testkit.Events(`2004-03-13 03:14:52`, `2003-07-25 11:35:34`))
	tk.MustQuery(`SELECT DATE_ADD('2003-11-18 07:25:13',INTERVAL c MINUTE_SECOND) FROM t`).Check(testkit.Events(`2003-11-18 09:29:13`, `2003-11-18 05:21:13`))
	tk.MustExec(`drop block if exists t;`)

	// for https://github.com/whtcorpsinc/milevadb/issues/11319
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE_MICROSECOND)`).Check(testkit.Events("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 SECOND_MICROSECOND)`).Check(testkit.Events("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_MICROSECOND)`).Check(testkit.Events("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_MICROSECOND)`).Check(testkit.Events("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 SECOND)`).Check(testkit.Events("2007-03-28 22:08:25.800000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_SECOND)`).Check(testkit.Events("2007-03-28 22:06:26"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_SECOND)`).Check(testkit.Events("2007-03-28 22:06:26"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE_SECOND)`).Check(testkit.Events("2007-03-28 22:06:26"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 MINUTE)`).Check(testkit.Events("2007-03-28 22:06:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_MINUTE)`).Check(testkit.Events("2007-03-28 20:06:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 HOUR_MINUTE)`).Check(testkit.Events("2007-03-28 20:06:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 DAY_HOUR)`).Check(testkit.Events("2007-03-26 20:08:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL -2.2 YEAR_MONTH)`).Check(testkit.Events("2005-01-28 22:08:28"))

	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE_MICROSECOND)`).Check(testkit.Events("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 SECOND_MICROSECOND)`).Check(testkit.Events("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_MICROSECOND)`).Check(testkit.Events("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_MICROSECOND)`).Check(testkit.Events("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 SECOND)`).Check(testkit.Events("2007-03-28 22:08:30.200000"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_SECOND)`).Check(testkit.Events("2007-03-28 22:10:30"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_SECOND)`).Check(testkit.Events("2007-03-28 22:10:30"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE_SECOND)`).Check(testkit.Events("2007-03-28 22:10:30"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 MINUTE)`).Check(testkit.Events("2007-03-28 22:10:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_MINUTE)`).Check(testkit.Events("2007-03-29 00:10:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 HOUR_MINUTE)`).Check(testkit.Events("2007-03-29 00:10:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 DAY_HOUR)`).Check(testkit.Events("2007-03-31 00:08:28"))
	tk.MustQuery(`SELECT DATE_ADD('2007-03-28 22:08:28',INTERVAL 2.2 YEAR_MONTH)`).Check(testkit.Events("2009-05-28 22:08:28"))
}

func (s *testIntegrationSuite) TestIssue12301(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t (d decimal(19, 0), i bigint(11))")
	tk.MustExec("insert into t values (123456789012, 123456789012)")
	tk.MustQuery("select * from t where d = i").Check(testkit.Events("123456789012 123456789012"))
}

func (s *testIntegrationSerialSuite) TestIssue15315(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustQuery("select '0-3261554956'+0.0").Check(testkit.Events("0"))
	tk.MustQuery("select cast('0-1234' as real)").Check(testkit.Events("0"))
}

func (s *testIntegrationSuite) TestNotExistFunc(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	// current EDB is empty
	_, err := tk.Exec("SELECT xxx(1)")
	c.Assert(err.Error(), Equals, "[planner:1046]No database selected")

	_, err = tk.Exec("SELECT yyy()")
	c.Assert(err.Error(), Equals, "[planner:1046]No database selected")

	// current EDB is not empty
	tk.MustExec("use test")
	_, err = tk.Exec("SELECT xxx(1)")
	c.Assert(err.Error(), Equals, "[expression:1305]FUNCTION test.xxx does not exist")

	_, err = tk.Exec("SELECT yyy()")
	c.Assert(err.Error(), Equals, "[expression:1305]FUNCTION test.yyy does not exist")

	tk.MustExec("use test")
	_, err = tk.Exec("SELECT timestampliteral(rand())")
	c.Assert(err.Error(), Equals, "[expression:1305]FUNCTION test.timestampliteral does not exist")

}

func (s *testIntegrationSuite) TestDecodetoChunkReuse(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("create block chk (a int,b varchar(20))")
	for i := 0; i < 200; i++ {
		if i%5 == 0 {
			tk.MustExec(fmt.Sprintf("insert chk values (NULL,NULL)"))
			continue
		}
		tk.MustExec(fmt.Sprintf("insert chk values (%d,'%s')", i, strconv.Itoa(i)))
	}

	tk.Se.GetStochastikVars().SetDistALLEGROSQLScanConcurrency(1)
	tk.MustExec("set milevadb_init_chunk_size = 2")
	tk.MustExec("set milevadb_max_chunk_size = 32")
	defer func() {
		tk.MustExec(fmt.Sprintf("set milevadb_init_chunk_size = %d", variable.DefInitChunkSize))
		tk.MustExec(fmt.Sprintf("set milevadb_max_chunk_size = %d", variable.DefMaxChunkSize))
	}()
	rs, err := tk.Exec("select * from chk")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	var count int
	for {
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		numEvents := req.NumEvents()
		if numEvents == 0 {
			break
		}
		for i := 0; i < numEvents; i++ {
			if count%5 == 0 {
				c.Assert(req.GetEvent(i).IsNull(0), Equals, true)
				c.Assert(req.GetEvent(i).IsNull(1), Equals, true)
			} else {
				c.Assert(req.GetEvent(i).IsNull(0), Equals, false)
				c.Assert(req.GetEvent(i).IsNull(1), Equals, false)
				c.Assert(req.GetEvent(i).GetInt64(0), Equals, int64(count))
				c.Assert(req.GetEvent(i).GetString(1), Equals, strconv.Itoa(count))
			}
			count++
		}
	}
	c.Assert(count, Equals, 200)
	rs.Close()
}

func (s *testIntegrationSuite) TestInMeetsPrepareAndExecute(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("prepare pr1 from 'select ? in (1,?,?)'")
	tk.MustExec("set @a=1, @b=2, @c=3")
	tk.MustQuery("execute pr1 using @a,@b,@c").Check(testkit.Events("1"))

	tk.MustExec("prepare pr2 from 'select 3 in (1,?,?)'")
	tk.MustExec("set @a=2, @b=3")
	tk.MustQuery("execute pr2 using @a,@b").Check(testkit.Events("1"))

	tk.MustExec("prepare pr3 from 'select ? in (1,2,3)'")
	tk.MustExec("set @a=4")
	tk.MustQuery("execute pr3 using @a").Check(testkit.Events("0"))

	tk.MustExec("prepare pr4 from 'select ? in (?,?,?)'")
	tk.MustExec("set @a=1, @b=2, @c=3, @d=4")
	tk.MustQuery("execute pr4 using @a,@b,@c,@d").Check(testkit.Events("0"))
}

func (s *testIntegrationSuite) TestCastStrToInt(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	cases := []struct {
		allegrosql string
		result     int
	}{
		{"select cast('' as signed)", 0},
		{"select cast('12345abcde' as signed)", 12345},
		{"select cast('123e456' as signed)", 123},
		{"select cast('-12345abcde' as signed)", -12345},
		{"select cast('-123e456' as signed)", -123},
	}
	for _, ca := range cases {
		tk.Se.GetStochastikVars().StmtCtx.SetWarnings(nil)
		tk.MustQuery(ca.allegrosql).Check(testkit.Events(fmt.Sprintf("%v", ca.result)))
		c.Assert(terror.ErrorEqual(tk.Se.GetStochastikVars().StmtCtx.GetWarnings()[0].Err, types.ErrTruncatedWrongVal), IsTrue)
	}
}

func (s *testIntegrationSerialSuite) TestIssue16205(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("prepare stmt from 'select random_bytes(3)'")
	rows1 := tk.MustQuery("execute stmt").Events()
	c.Assert(len(rows1), Equals, 1)
	rows2 := tk.MustQuery("execute stmt").Events()
	c.Assert(len(rows2), Equals, 1)
	c.Assert(rows1[0][0].(string), Not(Equals), rows2[0][0].(string))
}

func (s *testIntegrationSerialSuite) TestEventCountPlanCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int auto_increment primary key)")
	tk.MustExec("prepare stmt from 'select row_count()';")
	tk.MustExec("insert into t values()")
	res := tk.MustQuery("execute stmt").Events()
	c.Assert(len(res), Equals, 1)
	c.Assert(res[0][0], Equals, "1")
	tk.MustExec("insert into t values(),(),()")
	res = tk.MustQuery("execute stmt").Events()
	c.Assert(len(res), Equals, 1)
	c.Assert(res[0][0], Equals, "3")
}

func (s *testIntegrationSuite) TestValuesForBinaryLiteral(c *C) {
	// See issue #15310
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("create block testValuesBinary(id int primary key auto_increment, a bit(1));")
	tk.MustExec("insert into testValuesBinary values(1,1);")
	err := tk.ExecToErr("insert into testValuesBinary values(1,1) on duplicate key uFIDelate id = values(id),a = values(a);")
	c.Assert(err, IsNil)
	tk.MustQuery("select a=0 from testValuesBinary;").Check(testkit.Events("0"))
	err = tk.ExecToErr("insert into testValuesBinary values(1,0) on duplicate key uFIDelate id = values(id),a = values(a);")
	c.Assert(err, IsNil)
	tk.MustQuery("select a=0 from testValuesBinary;").Check(testkit.Events("1"))
	tk.MustExec("drop block testValuesBinary;")
}

func (s *testIntegrationSuite) TestIssue14159(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE t (v VARCHAR(100))")
	tk.MustExec("INSERT INTO t VALUES ('3289742893213123732904809')")
	tk.MustQuery("SELECT * FROM t WHERE v").Check(testkit.Events("3289742893213123732904809"))
}

func (s *testIntegrationSuite) TestIssue14146(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block tt(a varchar(10))")
	tk.MustExec("insert into tt values(NULL)")
	tk.MustExec("analyze block tt;")
	tk.MustQuery("select * from tt").Check(testkit.Events("<nil>"))
}

func (s *testIntegrationSerialSuite) TestCacheRegexpr(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (a varchar(40))")
	tk.MustExec("insert into t1 values ('C1'),('R1')")
	tk.MustExec("prepare stmt1 from 'select a from t1 where a rlike ?'")
	tk.MustExec("set @a='^C.*'")
	tk.MustQuery("execute stmt1 using @a").Check(testkit.Events("C1"))
	tk.MustExec("set @a='^R.*'")
	tk.MustQuery("execute stmt1 using @a").Check(testkit.Events("R1"))
}

func (s *testIntegrationSerialSuite) TestCacheRefineArgs(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(defCaus_int int)")
	tk.MustExec("insert into t values(null)")
	tk.MustExec("prepare stmt from 'SELECT ((defCaus_int is true) = ?) AS res FROM t'")
	tk.MustExec("set @p0='0.8'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Events("0"))
	tk.MustExec("set @p0='0'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Events("1"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("prepare stmt from 'SELECT defCaus_int < ? FROM t'")
	tk.MustExec("set @p0='-184467440737095516167.1'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Events("0"))
}

func (s *testIntegrationSuite) TestOrderByFuncPlanCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("prepare stmt from 'SELECT * FROM t order by rand()'")
	tk.MustQuery("execute stmt").Check(testkit.Events())
	tk.MustExec("prepare stmt from 'SELECT * FROM t order by now()'")
	tk.MustQuery("execute stmt").Check(testkit.Events())
}

func (s *testIntegrationSuite) TestSelectLimitPlanCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(1), (2), (3)")
	tk.MustExec("set @@stochastik.sql_select_limit = 1")
	tk.MustExec("prepare stmt from 'SELECT * FROM t'")
	tk.MustQuery("execute stmt").Check(testkit.Events("1"))
	tk.MustExec("set @@stochastik.sql_select_limit = default")
	tk.MustQuery("execute stmt").Check(testkit.Events("1", "2", "3"))
	tk.MustExec("set @@stochastik.sql_select_limit = 2")
	tk.MustQuery("execute stmt").Check(testkit.Events("1", "2"))
	tk.MustExec("set @@stochastik.sql_select_limit = 1")
	tk.MustQuery("execute stmt").Check(testkit.Events("1"))
	tk.MustExec("set @@stochastik.sql_select_limit = default")
	tk.MustQuery("execute stmt").Check(testkit.Events("1", "2", "3"))
	tk.MustExec("set @@stochastik.sql_select_limit = 2")
	tk.MustQuery("execute stmt").Check(testkit.Events("1", "2"))
}

func (s *testIntegrationSuite) TestDefCauslation(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (utf8_bin_c varchar(10) charset utf8 defCauslate utf8_bin, utf8_gen_c varchar(10) charset utf8 defCauslate utf8_general_ci, bin_c binary, num_c int, " +
		"abin char defCauslate ascii_bin, lbin char defCauslate latin1_bin, u4bin char defCauslate utf8mb4_bin, u4ci char defCauslate utf8mb4_general_ci)")
	tk.MustExec("insert into t values ('a', 'b', 'c', 4, 'a', 'a', 'a', 'a')")
	tk.MustQuery("select defCauslation(null)").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(2)").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(2 + 'a')").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(2 + utf8_gen_c) from t").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(2 + utf8_bin_c) from t").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(concat(utf8_bin_c, 2)) from t").Check(testkit.Events("utf8_bin"))
	tk.MustQuery("select defCauslation(concat(utf8_gen_c, 'abc')) from t").Check(testkit.Events("utf8_general_ci"))
	tk.MustQuery("select defCauslation(concat(utf8_gen_c, null)) from t").Check(testkit.Events("utf8_general_ci"))
	tk.MustQuery("select defCauslation(concat(utf8_gen_c, num_c)) from t").Check(testkit.Events("utf8_general_ci"))
	tk.MustQuery("select defCauslation(concat(utf8_bin_c, utf8_gen_c)) from t").Check(testkit.Events("utf8_bin"))
	tk.MustQuery("select defCauslation(upper(utf8_bin_c)) from t").Check(testkit.Events("utf8_bin"))
	tk.MustQuery("select defCauslation(upper(utf8_gen_c)) from t").Check(testkit.Events("utf8_general_ci"))
	tk.MustQuery("select defCauslation(upper(bin_c)) from t").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(concat(abin, bin_c)) from t").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(concat(lbin, bin_c)) from t").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(concat(utf8_bin_c, bin_c)) from t").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(concat(utf8_gen_c, bin_c)) from t").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(concat(u4bin, bin_c)) from t").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(concat(u4ci, bin_c)) from t").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(concat(abin, u4bin)) from t").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select defCauslation(concat(lbin, u4bin)) from t").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select defCauslation(concat(utf8_bin_c, u4bin)) from t").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select defCauslation(concat(utf8_gen_c, u4bin)) from t").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select defCauslation(concat(u4ci, u4bin)) from t").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select defCauslation(concat(abin, u4ci)) from t").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustQuery("select defCauslation(concat(lbin, u4ci)) from t").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustQuery("select defCauslation(concat(utf8_bin_c, u4ci)) from t").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustQuery("select defCauslation(concat(utf8_gen_c, u4ci)) from t").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustQuery("select defCauslation(concat(abin, utf8_bin_c)) from t").Check(testkit.Events("utf8_bin"))
	tk.MustQuery("select defCauslation(concat(lbin, utf8_bin_c)) from t").Check(testkit.Events("utf8_bin"))
	tk.MustQuery("select defCauslation(concat(utf8_gen_c, utf8_bin_c)) from t").Check(testkit.Events("utf8_bin"))
	tk.MustQuery("select defCauslation(concat(abin, utf8_gen_c)) from t").Check(testkit.Events("utf8_general_ci"))
	tk.MustQuery("select defCauslation(concat(lbin, utf8_gen_c)) from t").Check(testkit.Events("utf8_general_ci"))
	tk.MustQuery("select defCauslation(concat(abin, lbin)) from t").Check(testkit.Events("latin1_bin"))

	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_bin")
	tk.MustQuery("select defCauslation('a')").Check(testkit.Events("utf8mb4_bin"))
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci")
	tk.MustQuery("select defCauslation('a')").Check(testkit.Events("utf8mb4_general_ci"))

	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci")
	tk.MustExec("set @test_defCauslate_var = 'a'")
	tk.MustQuery("select defCauslation(@test_defCauslate_var)").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci")
	tk.MustExec("set @test_defCauslate_var = 1")
	tk.MustQuery("select defCauslation(@test_defCauslate_var)").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustExec("set @test_defCauslate_var = concat(\"a\", \"b\" defCauslate utf8mb4_bin)")
	tk.MustQuery("select defCauslation(@test_defCauslate_var)").Check(testkit.Events("utf8mb4_bin"))
}

func (s *testIntegrationSuite) TestCoercibility(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	type testCase struct {
		expr   string
		result int
	}
	testFunc := func(cases []testCase, suffix string) {
		for _, tc := range cases {
			tk.MustQuery(fmt.Sprintf("select coercibility(%v) %v", tc.expr, suffix)).Check(testkit.Events(fmt.Sprintf("%v", tc.result)))
		}
	}
	testFunc([]testCase{
		// constants
		{"1", 5}, {"null", 6}, {"'abc'", 4},
		// sys-constants
		{"version()", 3}, {"user()", 3}, {"database()", 3},
		{"current_role()", 3}, {"current_user()", 3},
		// scalar functions after constant folding
		{"1+null", 5}, {"null+'abcde'", 5}, {"concat(null, 'abcde')", 4},
		// non-deterministic functions
		{"rand()", 5}, {"now()", 5}, {"sysdate()", 5},
	}, "")

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (i int, r real, d datetime, t timestamp, c char(10), vc varchar(10), b binary(10), vb binary(10))")
	tk.MustExec("insert into t values (null, null, null, null, null, null, null, null)")
	testFunc([]testCase{
		{"i", 5}, {"r", 5}, {"d", 5}, {"t", 5},
		{"c", 2}, {"b", 2}, {"vb", 2}, {"vc", 2},
		{"i+r", 5}, {"i*r", 5}, {"cos(r)+sin(i)", 5}, {"d+2", 5},
		{"t*10", 5}, {"concat(c, vc)", 2}, {"replace(c, 'x', 'y')", 2},
	}, "from t")

	tk.MustQuery("SELECT COERCIBILITY(@straaa);").Check(testkit.Events("2"))
}

func (s *testIntegrationSerialSuite) TestCacheConstEval(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(defCaus_double double)")
	tk.MustExec("insert into t values (1)")
	tk.Se.GetStochastikVars().EnableVectorizedExpression = false
	tk.MustExec("insert into allegrosql.expr_pushdown_blacklist values('cast', 'einsteindb,tiflash,milevadb', 'for test')")
	tk.MustExec("admin reload expr_pushdown_blacklist")
	tk.MustExec("prepare stmt from 'SELECT * FROM (SELECT defCaus_double AS c0 FROM t) t WHERE (ABS((REPEAT(?, ?) OR 5617780767323292672)) < LN(EXP(c0)) + (? ^ ?))'")
	tk.MustExec("set @a1 = 'JuvkBX7ykVux20zQlkwDK2DFelgn7'")
	tk.MustExec("set @a2 = 1")
	tk.MustExec("set @a3 = -112990.35179796701")
	tk.MustExec("set @a4 = 87997.92704840179")
	// Main purpose here is checking no error is reported. 1 is the result when plan cache is disabled, it is
	// incompatible with MyALLEGROSQL actually, uFIDelate the result after fixing it.
	tk.MustQuery("execute stmt using @a1, @a2, @a3, @a4").Check(testkit.Events("1"))
	tk.Se.GetStochastikVars().EnableVectorizedExpression = true
	tk.MustExec("delete from allegrosql.expr_pushdown_blacklist where name = 'cast' and store_type = 'einsteindb,tiflash,milevadb' and reason = 'for test'")
	tk.MustExec("admin reload expr_pushdown_blacklist")
}

func (s *testIntegrationSerialSuite) TestDefCauslationBasic(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk.MustExec("use test")
	tk.MustExec("create block t_ci(a varchar(10) defCauslate utf8mb4_general_ci, unique key(a))")
	tk.MustExec("insert into t_ci values ('a')")
	tk.MustQuery("select * from t_ci").Check(testkit.Events("a"))
	tk.MustQuery("select * from t_ci").Check(testkit.Events("a"))
	tk.MustQuery("select * from t_ci where a='a'").Check(testkit.Events("a"))
	tk.MustQuery("select * from t_ci where a='A'").Check(testkit.Events("a"))
	tk.MustQuery("select * from t_ci where a='a   '").Check(testkit.Events("a"))
	tk.MustQuery("select * from t_ci where a='a                    '").Check(testkit.Events("a"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a varchar(10) primary key,b int)")
	tk.MustExec("insert into t values ('a', 1), ('b', 3), ('a', 2) on duplicate key uFIDelate b = b + 1;")
	tk.MustExec("set autocommit=0")
	tk.MustExec("insert into t values ('a', 1), ('b', 3), ('a', 2) on duplicate key uFIDelate b = b + 1;")
	tk.MustQuery("select * from t").Check(testkit.Events("a 4", "b 4"))
	tk.MustExec("set autocommit=1")
	tk.MustQuery("select * from t").Check(testkit.Events("a 4", "b 4"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a varchar(10),b int, key tk (a))")
	tk.MustExec("insert into t values ('', 1), ('', 3)")
	tk.MustExec("set autocommit=0")
	tk.MustExec("uFIDelate t set b = b + 1")
	tk.MustQuery("select * from t").Check(testkit.Events(" 2", " 4"))
	tk.MustExec("set autocommit=1")
	tk.MustQuery("select * from t").Check(testkit.Events(" 2", " 4"))

	tk.MustExec("drop block t_ci")
	tk.MustExec("create block t_ci(id bigint primary key, a varchar(10) defCauslate utf8mb4_general_ci, unique key(a, id))")
	tk.MustExec("insert into t_ci values (1, 'a')")
	tk.MustQuery("select a from t_ci").Check(testkit.Events("a"))
	tk.MustQuery("select a from t_ci").Check(testkit.Events("a"))
	tk.MustQuery("select a from t_ci where a='a'").Check(testkit.Events("a"))
	tk.MustQuery("select a from t_ci where a='A'").Check(testkit.Events("a"))
	tk.MustQuery("select a from t_ci where a='a   '").Check(testkit.Events("a"))
	tk.MustQuery("select a from t_ci where a='a                    '").Check(testkit.Events("a"))
}

func (s *testIntegrationSerialSuite) TestWeightString(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	type testCase struct {
		input                        []string
		result                       []string
		resultAsChar1                []string
		resultAsChar3                []string
		resultAsBinary1              []string
		resultAsBinary5              []string
		resultExplicitDefCauslateBin []string
	}
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (id int, a varchar(20) defCauslate utf8mb4_general_ci)")
	cases := testCase{
		input:                        []string{"aAÁàãăâ", "a", "a  ", "中", "中 "},
		result:                       []string{"\x00A\x00A\x00A\x00A\x00A\x00A\x00A", "\x00A", "\x00A", "\x4E\x2D", "\x4E\x2D"},
		resultAsChar1:                []string{"\x00A", "\x00A", "\x00A", "\x4E\x2D", "\x4E\x2D"},
		resultAsChar3:                []string{"\x00A\x00A\x00A", "\x00A", "\x00A", "\x4E\x2D", "\x4E\x2D"},
		resultAsBinary1:              []string{"a", "a", "a", "\xE4", "\xE4"},
		resultAsBinary5:              []string{"aA\xc3\x81\xc3", "a\x00\x00\x00\x00", "a  \x00\x00", "中\x00\x00", "中 \x00"},
		resultExplicitDefCauslateBin: []string{"aAÁàãăâ", "a", "a", "中", "中"},
	}
	values := make([]string, len(cases.input))
	for i, input := range cases.input {
		values[i] = fmt.Sprintf("(%d, '%s')", i, input)
	}
	tk.MustExec("insert into t values " + strings.Join(values, ","))
	rows := tk.MustQuery("select weight_string(a) from t order by id").Events()
	for i, out := range cases.result {
		c.Assert(rows[i][0].(string), Equals, out)
	}
	rows = tk.MustQuery("select weight_string(a as char(1)) from t order by id").Events()
	for i, out := range cases.resultAsChar1 {
		c.Assert(rows[i][0].(string), Equals, out)
	}
	rows = tk.MustQuery("select weight_string(a as char(3)) from t order by id").Events()
	for i, out := range cases.resultAsChar3 {
		c.Assert(rows[i][0].(string), Equals, out)
	}
	rows = tk.MustQuery("select weight_string(a as binary(1)) from t order by id").Events()
	for i, out := range cases.resultAsBinary1 {
		c.Assert(rows[i][0].(string), Equals, out)
	}
	rows = tk.MustQuery("select weight_string(a as binary(5)) from t order by id").Events()
	for i, out := range cases.resultAsBinary5 {
		c.Assert(rows[i][0].(string), Equals, out)
	}
	c.Assert(tk.MustQuery("select weight_string(NULL);").Events()[0][0], Equals, "<nil>")
	c.Assert(tk.MustQuery("select weight_string(7);").Events()[0][0], Equals, "<nil>")
	c.Assert(tk.MustQuery("select weight_string(cast(7 as decimal(5)));").Events()[0][0], Equals, "<nil>")
	c.Assert(tk.MustQuery("select weight_string(cast(20190821 as date));").Events()[0][0], Equals, "2020-08-21")
	c.Assert(tk.MustQuery("select weight_string(cast(20190821 as date) as binary(5));").Events()[0][0], Equals, "2020-")
	c.Assert(tk.MustQuery("select weight_string(7.0);").Events()[0][0], Equals, "<nil>")
	c.Assert(tk.MustQuery("select weight_string(7 AS BINARY(2));").Events()[0][0], Equals, "7\x00")
	// test explicit defCauslation
	c.Assert(tk.MustQuery("select weight_string('中 ' defCauslate utf8mb4_general_ci);").Events()[0][0], Equals, "\x4E\x2D")
	c.Assert(tk.MustQuery("select weight_string('中 ' defCauslate utf8mb4_bin);").Events()[0][0], Equals, "中")
	c.Assert(tk.MustQuery("select weight_string('中 ' defCauslate utf8mb4_unicode_ci);").Events()[0][0], Equals, "\xFB\x40\xCE\x2D")
	c.Assert(tk.MustQuery("select defCauslation(a defCauslate utf8mb4_general_ci) from t order by id").Events()[0][0], Equals, "utf8mb4_general_ci")
	c.Assert(tk.MustQuery("select defCauslation('中 ' defCauslate utf8mb4_general_ci);").Events()[0][0], Equals, "utf8mb4_general_ci")
	rows = tk.MustQuery("select weight_string(a defCauslate utf8mb4_bin) from t order by id").Events()
	for i, out := range cases.resultExplicitDefCauslateBin {
		c.Assert(rows[i][0].(string), Equals, out)
	}
	tk.MustGetErrMsg("select weight_string(a defCauslate utf8_general_ci) from t order by id", "[dbs:1253]COLLATION 'utf8_general_ci' is not valid for CHARACTER SET 'utf8mb4'")
	tk.MustGetErrMsg("select weight_string('中' defCauslate utf8_bin)", "[dbs:1253]COLLATION 'utf8_bin' is not valid for CHARACTER SET 'utf8mb4'")
}

func (s *testIntegrationSerialSuite) TestDefCauslationCreateIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a varchar(10) defCauslate utf8mb4_general_ci);")
	tk.MustExec("insert into t values ('a');")
	tk.MustExec("insert into t values ('A');")
	tk.MustExec("insert into t values ('b');")
	tk.MustExec("insert into t values ('B');")
	tk.MustExec("insert into t values ('a');")
	tk.MustExec("insert into t values ('A');")
	tk.MustExec("insert into t values ('ß');")
	tk.MustExec("insert into t values ('sa');")
	tk.MustExec("create index idx on t(a);")
	tk.MustQuery("select * from t order by a").Check(testkit.Events("a", "A", "a", "A", "b", "B", "ß", "sa"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a varchar(10) defCauslate utf8mb4_unicode_ci);")
	tk.MustExec("insert into t values ('a');")
	tk.MustExec("insert into t values ('A');")
	tk.MustExec("insert into t values ('b');")
	tk.MustExec("insert into t values ('B');")
	tk.MustExec("insert into t values ('a');")
	tk.MustExec("insert into t values ('A');")
	tk.MustExec("insert into t values ('ß');")
	tk.MustExec("insert into t values ('sa');")
	tk.MustExec("create index idx on t(a);")
	tk.MustQuery("select * from t order by a").Check(testkit.Events("a", "A", "a", "A", "b", "B", "sa", "ß"))
}

func (s *testIntegrationSerialSuite) TestDefCauslateConstantPropagation(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a char(10) defCauslate utf8mb4_bin, b char(10) defCauslate utf8mb4_general_ci);")
	tk.MustExec("insert into t values ('a', 'A');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b='a' defCauslate utf8mb4_general_ci;").Check(nil)
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b>='a' defCauslate utf8mb4_general_ci;").Check(nil)
	tk.MustExec("drop block t;")
	tk.MustExec("create block t (a char(10) defCauslate utf8mb4_general_ci, b char(10) defCauslate utf8mb4_general_ci);")
	tk.MustExec("insert into t values ('A', 'a');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b='a' defCauslate utf8mb4_bin;").Check(testkit.Events("A a A a"))
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b>='a' defCauslate utf8mb4_bin;").Check(testkit.Events("A a A a"))
	tk.MustExec("drop block t;")
	tk.MustExec("set names utf8mb4")
	tk.MustExec("create block t (a char(10) defCauslate utf8mb4_general_ci, b char(10) defCauslate utf8_general_ci);")
	tk.MustExec("insert into t values ('a', 'A');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b='A'").Check(testkit.Events("a A a A"))
	tk.MustExec("drop block t;")
	tk.MustExec("create block t(a char defCauslate utf8_general_ci, b char defCauslate utf8mb4_general_ci, c char defCauslate utf8_bin);")
	tk.MustExec("insert into t values ('b', 'B', 'B');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b=t2.c;").Check(testkit.Events("b B B b B B"))
	tk.MustExec("drop block t;")
	tk.MustExec("create block t(a char defCauslate utf8_bin, b char defCauslate utf8_general_ci);")
	tk.MustExec("insert into t values ('a', 'A');")
	tk.MustQuery("select * from t t1, t t2 where t1.b=t2.b and t2.b=t1.a defCauslate utf8_general_ci;").Check(testkit.Events("a A a A"))
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci;")
	tk.MustExec("create block t1(a char, b varchar(10)) charset utf8mb4 defCauslate utf8mb4_general_ci;")
	tk.MustExec("create block t2(a char, b varchar(10)) charset utf8mb4 defCauslate utf8mb4_bin;")
	tk.MustExec("insert into t1 values ('A', 'a');")
	tk.MustExec("insert into t2 values ('a', 'a')")
	tk.MustQuery("select * from t1 left join t2 on t1.a = t2.a where t1.a = 'a';").Check(testkit.Events("A a <nil> <nil>"))
	tk.MustExec("drop block t;")
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci;")
	tk.MustExec("create block t(a char defCauslate utf8mb4_bin, b char defCauslate utf8mb4_general_ci);")
	tk.MustExec("insert into t values ('a', 'a');")
	tk.MustQuery("select * from t t1, t t2 where  t2.b = 'A' and lower(concat(t1.a , '' ))  = t2.b;").Check(testkit.Events("a a a a"))
	tk.MustExec("drop block t;")
	tk.MustExec("create block t(a char defCauslate utf8_unicode_ci, b char defCauslate utf8mb4_unicode_ci, c char defCauslate utf8_bin);")
	tk.MustExec("insert into t values ('b', 'B', 'B');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b=t2.c;").Check(testkit.Events("b B B b B B"))
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_unicode_ci;")
	tk.MustExec("create block t1(a char, b varchar(10)) charset utf8mb4 defCauslate utf8mb4_unicode_ci;")
	tk.MustExec("create block t2(a char, b varchar(10)) charset utf8mb4 defCauslate utf8mb4_bin;")
	tk.MustExec("insert into t1 values ('A', 'a');")
	tk.MustExec("insert into t2 values ('a', 'a')")
	tk.MustQuery("select * from t1 left join t2 on t1.a = t2.a where t1.a = 'a';").Check(testkit.Events("A a <nil> <nil>"))
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci;")
	tk.MustExec("create block t1(a char, b varchar(10)) charset utf8mb4 defCauslate utf8mb4_general_ci;")
	tk.MustExec("create block t2(a char, b varchar(10)) charset utf8mb4 defCauslate utf8mb4_unicode_ci;")
	tk.MustExec("insert into t1 values ('ß', 's');")
	tk.MustExec("insert into t2 values ('s', 's')")
	tk.MustQuery("select * from t1 left join t2 on t1.a = t2.a defCauslate utf8mb4_unicode_ci where t1.a = 's';").Check(testkit.Events("ß s <nil> <nil>"))
}

func (s *testIntegrationSerialSuite) TestMixDefCauslation(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk.MustGetErrMsg(`select 'a' defCauslate utf8mb4_bin = 'a' defCauslate utf8mb4_general_ci;`, "[expression:1267]Illegal mix of defCauslations (utf8mb4_bin,EXPLICIT) and (utf8mb4_general_ci,EXPLICIT) for operation 'eq'")

	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t;")
	tk.MustExec(`create block t (
			mb4general varchar(10) charset utf8mb4 defCauslate utf8mb4_general_ci,
			mb4unicode varchar(10) charset utf8mb4 defCauslate utf8mb4_unicode_ci,
			mb4bin     varchar(10) charset utf8mb4 defCauslate utf8mb4_bin,
			general    varchar(10) charset utf8 defCauslate utf8_general_ci,
			unicode    varchar(10) charset utf8 defCauslate utf8_unicode_ci,
			utfbin     varchar(10) charset utf8 defCauslate utf8_bin,
			bin        varchar(10) charset binary defCauslate binary,
			latin1_bin varchar(10) charset latin1 defCauslate latin1_bin,
			ascii_bin  varchar(10) charset ascii defCauslate ascii_bin,
    		i          int
	);`)
	tk.MustExec("insert into t values ('s', 's', 's', 's', 's', 's', 's', 's', 's', 1);")
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci;")

	tk.MustQuery("select * from t where mb4unicode = 's' defCauslate utf8mb4_unicode_ci;").Check(testkit.Events("s s s s s s s s s 1"))
	tk.MustQuery(`select * from t t1, t t2 where t1.mb4unicode = t2.mb4general defCauslate utf8mb4_general_ci;`).Check(testkit.Events("s s s s s s s s s 1 s s s s s s s s s 1"))
	tk.MustQuery(`select * from t t1, t t2 where t1.mb4general = t2.mb4unicode defCauslate utf8mb4_general_ci;`).Check(testkit.Events("s s s s s s s s s 1 s s s s s s s s s 1"))
	tk.MustQuery(`select * from t t1, t t2 where t1.mb4general = t2.mb4unicode defCauslate utf8mb4_unicode_ci;`).Check(testkit.Events("s s s s s s s s s 1 s s s s s s s s s 1"))
	tk.MustQuery(`select * from t t1, t t2 where t1.mb4unicode = t2.mb4general defCauslate utf8mb4_unicode_ci;`).Check(testkit.Events("s s s s s s s s s 1 s s s s s s s s s 1"))
	tk.MustQuery(`select * from t where mb4general = mb4bin defCauslate utf8mb4_general_ci;`).Check(testkit.Events("s s s s s s s s s 1"))
	tk.MustQuery(`select * from t where mb4unicode = mb4general defCauslate utf8mb4_unicode_ci;`).Check(testkit.Events("s s s s s s s s s 1"))
	tk.MustQuery(`select * from t where mb4general = mb4unicode defCauslate utf8mb4_unicode_ci;`).Check(testkit.Events("s s s s s s s s s 1"))
	tk.MustQuery(`select * from t where mb4unicode = 's' defCauslate utf8mb4_unicode_ci;`).Check(testkit.Events("s s s s s s s s s 1"))
	tk.MustQuery("select * from t where mb4unicode = mb4bin;").Check(testkit.Events("s s s s s s s s s 1"))
	tk.MustQuery("select * from t where general = mb4unicode;").Check(testkit.Events("s s s s s s s s s 1"))
	tk.MustQuery("select * from t where unicode = mb4unicode;").Check(testkit.Events("s s s s s s s s s 1"))
	tk.MustQuery("select * from t where mb4unicode = mb4unicode;").Check(testkit.Events("s s s s s s s s s 1"))

	tk.MustQuery("select defCauslation(concat(mb4unicode, mb4general defCauslate utf8mb4_unicode_ci)) from t;").Check(testkit.Events("utf8mb4_unicode_ci"))
	tk.MustQuery("select defCauslation(concat(mb4general, mb4unicode, mb4bin)) from t;").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4general, mb4unicode, mb4bin)) from t;").Check(testkit.Events("1"))
	tk.MustQuery("select defCauslation(concat(mb4unicode, mb4bin, concat(mb4general))) from t;").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4unicode, mb4bin)) from t;").Check(testkit.Events("2"))
	tk.MustQuery("select defCauslation(concat(mb4unicode, mb4bin)) from t;").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4bin, concat(mb4general))) from t;").Check(testkit.Events("2"))
	tk.MustQuery("select defCauslation(concaT(mb4bin, cOncAt(mb4general))) from t;").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4unicode, mb4bin, concat(mb4general))) from t;").Check(testkit.Events("2"))
	tk.MustQuery("select defCauslation(concat(mb4unicode, mb4bin, concat(mb4general))) from t;").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4unicode, mb4general)) from t;").Check(testkit.Events("1"))
	tk.MustQuery("select defCauslation(coalesce(mb4unicode, mb4general)) from t;").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select coercibility(coalesce(mb4unicode, mb4general)) from t;").Check(testkit.Events("1"))
	tk.MustQuery("select defCauslation(CONCAT(concat(mb4unicode), concat(mb4general))) from t;").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select coercibility(cONcat(unicode, general)) from t;").Check(testkit.Events("1"))
	tk.MustQuery("select defCauslation(concAt(unicode, general)) from t;").Check(testkit.Events("utf8_bin"))
	tk.MustQuery("select defCauslation(concat(bin, mb4general)) from t;").Check(testkit.Events("binary"))
	tk.MustQuery("select coercibility(concat(bin, mb4general)) from t;").Check(testkit.Events("2"))
	tk.MustQuery("select defCauslation(concat(mb4unicode, ascii_bin)) from t;").Check(testkit.Events("utf8mb4_unicode_ci"))
	tk.MustQuery("select coercibility(concat(mb4unicode, ascii_bin)) from t;").Check(testkit.Events("2"))
	tk.MustQuery("select defCauslation(concat(mb4unicode, mb4unicode)) from t;").Check(testkit.Events("utf8mb4_unicode_ci"))
	tk.MustQuery("select coercibility(concat(mb4unicode, mb4unicode)) from t;").Check(testkit.Events("2"))
	tk.MustQuery("select defCauslation(concat(bin, bin)) from t;").Check(testkit.Events("binary"))
	tk.MustQuery("select coercibility(concat(bin, bin)) from t;").Check(testkit.Events("2"))
	tk.MustQuery("select defCauslation(concat(latin1_bin, ascii_bin)) from t;").Check(testkit.Events("latin1_bin"))
	tk.MustQuery("select coercibility(concat(latin1_bin, ascii_bin)) from t;").Check(testkit.Events("2"))
	tk.MustQuery("select defCauslation(concat(mb4unicode, bin)) from t;").Check(testkit.Events("binary"))
	tk.MustQuery("select coercibility(concat(mb4unicode, bin)) from t;").Check(testkit.Events("2"))
	tk.MustQuery("select defCauslation(mb4general defCauslate utf8mb4_unicode_ci) from t;").Check(testkit.Events("utf8mb4_unicode_ci"))
	tk.MustQuery("select coercibility(mb4general defCauslate utf8mb4_unicode_ci) from t;").Check(testkit.Events("0"))
	tk.MustQuery("select defCauslation(concat(concat(mb4unicode, mb4general), concat(unicode, general))) from t;").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(concat(mb4unicode, mb4general), concat(unicode, general))) from t;").Check(testkit.Events("1"))
	tk.MustQuery("select defCauslation(concat(i, 1)) from t;").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustQuery("select coercibility(concat(i, 1)) from t;").Check(testkit.Events("4"))
	tk.MustQuery("select defCauslation(concat(i, user())) from t;").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustQuery("select coercibility(concat(i, user())) from t;").Check(testkit.Events("3"))
	tk.MustGetErrMsg("select * from t where mb4unicode = mb4general;", "[expression:1267]Illegal mix of defCauslations (utf8mb4_unicode_ci,IMPLICIT) and (utf8mb4_general_ci,IMPLICIT) for operation 'eq'")
	tk.MustGetErrMsg("select * from t where unicode = general;", "[expression:1267]Illegal mix of defCauslations (utf8_unicode_ci,IMPLICIT) and (utf8_general_ci,IMPLICIT) for operation 'eq'")
	tk.MustGetErrMsg("select concat(mb4general) = concat(mb4unicode) from t;", "[expression:1267]Illegal mix of defCauslations (utf8mb4_general_ci,IMPLICIT) and (utf8mb4_unicode_ci,IMPLICIT) for operation 'eq'")
	tk.MustGetErrMsg("select * from t t1, t t2 where t1.mb4unicode = t2.mb4general;", "[expression:1267]Illegal mix of defCauslations (utf8mb4_unicode_ci,IMPLICIT) and (utf8mb4_general_ci,IMPLICIT) for operation 'eq'")
	tk.MustGetErrMsg("select field('s', mb4general, mb4unicode, mb4bin) from t;", "[expression:1271]Illegal mix of defCauslations for operation 'field'")
	tk.MustGetErrMsg("select concat(mb4unicode, mb4general) = mb4unicode from t;", "[expression:1267]Illegal mix of defCauslations (utf8mb4_bin,NONE) and (utf8mb4_unicode_ci,IMPLICIT) for operation 'eq'")

	tk.MustExec("drop block t;")
}

func (s *testIntegrationSerialSuite) prepare4Join(c *C) *testkit.TestKit {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("drop block if exists t_bin")
	tk.MustExec("CREATE TABLE `t` ( `a` int(11) NOT NULL,`b` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL)")
	tk.MustExec("CREATE TABLE `t_bin` ( `a` int(11) NOT NULL,`b` varchar(5) CHARACTER SET binary)")
	tk.MustExec("insert into t values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustExec("insert into t_bin values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	return tk
}

func (s *testIntegrationSerialSuite) prepare4Join2(c *C) *testkit.TestKit {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("drop block if exists t2")
	tk.MustExec("create block t1 (id int, v varchar(5) character set binary, key(v))")
	tk.MustExec("create block t2 (v varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, key(v))")
	tk.MustExec("insert into t1 values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustExec("insert into t2 values ('a'), ('À'), ('á'), ('à'), ('b'), ('c'), (' ')")
	return tk
}

func (s *testIntegrationSerialSuite) TestDefCauslateHashJoin(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4Join(c)
	tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Events("1 a", "1 a", "1 a", "1 a", "2 À", "2 À", "2 À", "2 À", "3 á", "3 á", "3 á", "3 á", "4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Events("1 a", "2 À", "3 á", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Events("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Events("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Events("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Events("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Events("2 À", "3 á", "3 á", "4 à", "4 à", "4 à"))
	tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Events())
}

func (s *testIntegrationSerialSuite) TestDefCauslateHashJoin2(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4Join2(c)
	tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ * from t1, t2 where t1.v=t2.v order by t1.id").Check(
		testkit.Events("1 a a", "2 À À", "3 á á", "4 à à", "5 b b", "6 c c", "7    "))
}

func (s *testIntegrationSerialSuite) TestDefCauslateMergeJoin(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4Join(c)
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Events("1 a", "1 a", "1 a", "1 a", "2 À", "2 À", "2 À", "2 À", "3 á", "3 á", "3 á", "3 á", "4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Events("1 a", "2 À", "3 á", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Events("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Events("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Events("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Events("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Events("2 À", "3 á", "3 á", "4 à", "4 à", "4 à"))
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Events())
}

func (s *testIntegrationSerialSuite) TestDefCauslateMergeJoin2(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4Join2(c)
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ * from t1, t2 where t1.v=t2.v order by t1.id").Check(
		testkit.Events("1 a a", "2 À À", "3 á á", "4 à à", "5 b b", "6 c c", "7    "))
}

func (s *testIntegrationSerialSuite) TestDefCauslateIndexMergeJoin(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, b varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, key(a), key(b))")
	tk.MustExec("insert into t values ('a', 'x'), ('x', 'À'), ('á', 'x'), ('à', 'à'), ('à', 'x')")

	tk.MustExec("set milevadb_enable_index_merge=1")
	tk.MustQuery("select /*+ USE_INDEX_MERGE(t, a, b) */ * from t where a = 'a' or b = 'a'").Sort().Check(
		testkit.Events("a x", "x À", "à x", "à à", "á x"))
}

func (s *testIntegrationSerialSuite) prepare4DefCauslation(c *C, hasIndex bool) *testkit.TestKit {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("USE test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("drop block if exists t_bin")
	idxALLEGROSQL := ", key(v)"
	if !hasIndex {
		idxALLEGROSQL = ""
	}
	tk.MustExec(fmt.Sprintf("create block t (id int, v varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL %v)", idxALLEGROSQL))
	tk.MustExec(fmt.Sprintf("create block t_bin (id int, v varchar(5) CHARACTER SET binary %v)", idxALLEGROSQL))
	tk.MustExec("insert into t values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustExec("insert into t_bin values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	return tk
}

func (s *testIntegrationSerialSuite) TestDefCauslateSelection(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4DefCauslation(c, false)
	tk.MustQuery("select v from t where v='a' order by id").Check(testkit.Events("a", "À", "á", "à"))
	tk.MustQuery("select v from t_bin where v='a' order by id").Check(testkit.Events("a"))
	tk.MustQuery("select v from t where v<'b' and id<=3").Check(testkit.Events("a", "À", "á"))
	tk.MustQuery("select v from t_bin where v<'b' and id<=3").Check(testkit.Events("a"))
}

func (s *testIntegrationSerialSuite) TestDefCauslateSort(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4DefCauslation(c, false)
	tk.MustQuery("select id from t order by v, id").Check(testkit.Events("7", "1", "2", "3", "4", "5", "6"))
	tk.MustQuery("select id from t_bin order by v, id").Check(testkit.Events("7", "1", "5", "6", "2", "4", "3"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10) defCauslate utf8mb4_general_ci, key(a))")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustQuery("select * from t order by a defCauslate utf8mb4_bin").Check(testkit.Events("A", "A", "A", "a", "a", "a", "b", "b", "b"))
	tk.MustQuery("select * from t order by a defCauslate utf8mb4_general_ci").Check(testkit.Events("a", "A", "a", "A", "a", "A", "b", "b", "b"))
	tk.MustQuery("select * from t order by a defCauslate utf8mb4_unicode_ci").Check(testkit.Events("a", "A", "a", "A", "a", "A", "b", "b", "b"))
}

func (s *testIntegrationSerialSuite) TestDefCauslateHashAgg(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4DefCauslation(c, false)
	tk.HasPlan("select distinct(v) from t_bin", "HashAgg")
	tk.MustQuery("select distinct(v) from t_bin").Sort().Check(testkit.Events(" ", "a", "b", "c", "À", "à", "á"))
	tk.HasPlan("select distinct(v) from t", "HashAgg")
	tk.MustQuery("select distinct(v) from t").Sort().Check(testkit.Events(" ", "a", "b", "c"))
	tk.HasPlan("select v, count(*) from t_bin group by v", "HashAgg")
	tk.MustQuery("select v, count(*) from t_bin group by v").Sort().Check(testkit.Events("  1", "a 1", "b 1", "c 1", "À 1", "à 1", "á 1"))
	tk.HasPlan("select v, count(*) from t group by v", "HashAgg")
	tk.MustQuery("select v, count(*) from t group by v").Sort().Check(testkit.Events("  1", "a 4", "b 1", "c 1"))

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10) defCauslate utf8mb4_general_ci, key(a))")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('s'), ('ss'), ('ß')")
	tk.MustQuery("select count(1) from t group by a defCauslate utf8mb4_bin order by a defCauslate utf8mb4_bin").Check(testkit.Events("3", "3", "3", "1", "1", "1"))
	tk.MustQuery("select count(1) from t group by a defCauslate utf8mb4_unicode_ci order by a defCauslate utf8mb4_unicode_ci").Check(testkit.Events("6", "3", "1", "2"))
	tk.MustQuery("select count(1) from t group by a defCauslate utf8mb4_general_ci order by a defCauslate utf8mb4_general_ci").Check(testkit.Events("6", "3", "2", "1"))
}

func (s *testIntegrationSerialSuite) TestDefCauslateStreamAgg(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4DefCauslation(c, true)
	tk.HasPlan("select distinct(v) from t_bin", "StreamAgg")
	tk.MustQuery("select distinct(v) from t_bin").Sort().Check(testkit.Events(" ", "a", "b", "c", "À", "à", "á"))
	tk.HasPlan("select distinct(v) from t", "StreamAgg")
	tk.MustQuery("select distinct(v) from t").Sort().Check(testkit.Events(" ", "a", "b", "c"))
	tk.HasPlan("select v, count(*) from t_bin group by v", "StreamAgg")
	tk.MustQuery("select v, count(*) from t_bin group by v").Sort().Check(testkit.Events("  1", "a 1", "b 1", "c 1", "À 1", "à 1", "á 1"))
	tk.HasPlan("select v, count(*) from t group by v", "StreamAgg")
	tk.MustQuery("select v, count(*) from t group by v").Sort().Check(testkit.Events("  1", "a 4", "b 1", "c 1"))
}

func (s *testIntegrationSerialSuite) TestDefCauslateIndexReader(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4DefCauslation(c, true)
	tk.HasPlan("select v from t where v < 'b'  order by v", "IndexReader")
	tk.MustQuery("select v from t where v < 'b' order by v").Check(testkit.Events(" ", "a", "À", "á", "à"))
	tk.HasPlan("select v from t where v < 'b' and v > ' ' order by v", "IndexReader")
	tk.MustQuery("select v from t where v < 'b' and v > ' ' order by v").Check(testkit.Events("a", "À", "á", "à"))
	tk.HasPlan("select v from t_bin where v < 'b' order by v", "IndexReader")
	tk.MustQuery("select v from t_bin where v < 'b' order by v").Sort().Check(testkit.Events(" ", "a"))
	tk.HasPlan("select v from t_bin where v < 'b' and v > ' ' order by v", "IndexReader")
	tk.MustQuery("select v from t_bin where v < 'b' and v > ' ' order by v").Sort().Check(testkit.Events("a"))
}

func (s *testIntegrationSerialSuite) TestDefCauslateIndexLookup(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4DefCauslation(c, true)

	tk.HasPlan("select id from t where v < 'b'", "IndexLookUp")
	tk.MustQuery("select id from t where v < 'b'").Sort().Check(testkit.Events("1", "2", "3", "4", "7"))
	tk.HasPlan("select id from t where v < 'b' and v > ' '", "IndexLookUp")
	tk.MustQuery("select id from t where v < 'b' and v > ' '").Sort().Check(testkit.Events("1", "2", "3", "4"))
	tk.HasPlan("select id from t_bin where v < 'b'", "IndexLookUp")
	tk.MustQuery("select id from t_bin where v < 'b'").Sort().Check(testkit.Events("1", "7"))
	tk.HasPlan("select id from t_bin where v < 'b' and v > ' '", "IndexLookUp")
	tk.MustQuery("select id from t_bin where v < 'b' and v > ' '").Sort().Check(testkit.Events("1"))
}

func (s *testIntegrationSerialSuite) TestIssue16668(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists tx")
	tk.MustExec("CREATE TABLE `tx` ( `a` int(11) NOT NULL,`b` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL)")
	tk.MustExec("insert into tx values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustQuery("select count(distinct(b)) from tx").Check(testkit.Events("4"))
}

func (s *testIntegrationSerialSuite) TestDefCauslateStringFunction(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustQuery("select field('a', 'b', 'a');").Check(testkit.Events("2"))
	tk.MustQuery("select field('a', 'b', 'A');").Check(testkit.Events("0"))
	tk.MustQuery("select field('a', 'b', 'A' defCauslate utf8mb4_bin);").Check(testkit.Events("0"))
	tk.MustQuery("select field('a', 'b', 'a ' defCauslate utf8mb4_bin);").Check(testkit.Events("2"))
	tk.MustQuery("select field('a', 'b', 'A' defCauslate utf8mb4_unicode_ci);").Check(testkit.Events("2"))
	tk.MustQuery("select field('a', 'b', 'a ' defCauslate utf8mb4_unicode_ci);").Check(testkit.Events("2"))
	tk.MustQuery("select field('a', 'b', 'A' defCauslate utf8mb4_general_ci);").Check(testkit.Events("2"))
	tk.MustQuery("select field('a', 'b', 'a ' defCauslate utf8mb4_general_ci);").Check(testkit.Events("2"))

	tk.MustExec("USE test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char(10), b char (10)) defCauslate utf8mb4_general_ci")
	tk.MustExec("insert into t values ('a', 'A')")
	tk.MustQuery("select field(a, b) from t").Check(testkit.Events("1"))

	tk.MustQuery("select FIND_IN_SET('a','b,a,c,d');").Check(testkit.Events("2"))
	tk.MustQuery("select FIND_IN_SET('a','b,A,c,d');").Check(testkit.Events("0"))
	tk.MustQuery("select FIND_IN_SET('a','b,A,c,d' defCauslate utf8mb4_bin);").Check(testkit.Events("0"))
	tk.MustQuery("select FIND_IN_SET('a','b,a ,c,d' defCauslate utf8mb4_bin);").Check(testkit.Events("2"))
	tk.MustQuery("select FIND_IN_SET('a','b,A,c,d' defCauslate utf8mb4_general_ci);").Check(testkit.Events("2"))
	tk.MustQuery("select FIND_IN_SET('a','b,a ,c,d' defCauslate utf8mb4_general_ci);").Check(testkit.Events("2"))

	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci;")
	tk.MustQuery("select defCauslation(cast('a' as char));").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustQuery("select defCauslation(cast('a' as binary));").Check(testkit.Events("binary"))
	tk.MustQuery("select defCauslation(cast('a' defCauslate utf8mb4_bin as char));").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustQuery("select defCauslation(cast('a' defCauslate utf8mb4_bin as binary));").Check(testkit.Events("binary"))

	tk.MustQuery("select FIND_IN_SET('a','b,A,c,d' defCauslate utf8mb4_unicode_ci);").Check(testkit.Events("2"))
	tk.MustQuery("select FIND_IN_SET('a','b,a ,c,d' defCauslate utf8mb4_unicode_ci);").Check(testkit.Events("2"))

	tk.MustExec("select concat('a' defCauslate utf8mb4_bin, 'b' defCauslate utf8mb4_bin);")
	tk.MustGetErrMsg("select concat('a' defCauslate utf8mb4_bin, 'b' defCauslate utf8mb4_general_ci);", "[expression:1267]Illegal mix of defCauslations (utf8mb4_bin,EXPLICIT) and (utf8mb4_general_ci,EXPLICIT) for operation 'concat'")
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a char)")
	tk.MustGetErrMsg("select * from t t1 join t t2 on t1.a defCauslate utf8mb4_bin = t2.a defCauslate utf8mb4_general_ci;", "[expression:1267]Illegal mix of defCauslations (utf8mb4_bin,EXPLICIT) and (utf8mb4_general_ci,EXPLICIT) for operation 'eq'")

	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1 ( a int, p1 VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin,p2 VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci , p3 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,p4 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ,n1 VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin,n2 VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci , n3 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,n4 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci );")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values(1,'  0aA1!测试テストמבחן  ','  0aA1!测试テストמבחן 	','  0aA1!测试テストמבחן 	','  0aA1!测试テストמבחן 	','  0Aa1!测试テストמבחן  ','  0Aa1!测试テストמבחן 	','  0Aa1!测试テストמבחן 	','  0Aa1!测试テストמבחן 	');")

	tk.MustQuery("select INSTR(p1,n1) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p1,n2) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p1,n3) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p1,n4) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p2,n1) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p2,n2) from t1;").Check(testkit.Events("1"))
	tk.MustQuery("select INSTR(p2,n3) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p2,n4) from t1;").Check(testkit.Events("1"))
	tk.MustQuery("select INSTR(p3,n1) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p3,n2) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p3,n3) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p3,n4) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p4,n1) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p4,n2) from t1;").Check(testkit.Events("1"))
	tk.MustQuery("select INSTR(p4,n3) from t1;").Check(testkit.Events("0"))
	tk.MustQuery("select INSTR(p4,n4) from t1;").Check(testkit.Events("1"))

	tk.MustExec("truncate block t1;")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (1,'0aA1!测试テストמבחן  ','0aA1!测试テストמבחן 	','0aA1!测试テストמבחן 	','0aA1!测试テストמבחן 	','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (2,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (3,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ');")

	tk.MustQuery("select LOCATE(p1,n1) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p1,n2) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p1,n3) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p1,n4) from t1;").Check(testkit.Events("0", "1", "1"))
	tk.MustQuery("select LOCATE(p2,n1) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p2,n2) from t1;").Check(testkit.Events("0", "1", "1"))
	tk.MustQuery("select LOCATE(p2,n3) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p2,n4) from t1;").Check(testkit.Events("0", "1", "1"))
	tk.MustQuery("select LOCATE(p3,n1) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p3,n2) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p3,n3) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p3,n4) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p4,n1) from t1;").Check(testkit.Events("0", "1", "1"))
	tk.MustQuery("select LOCATE(p4,n2) from t1;").Check(testkit.Events("0", "1", "1"))
	tk.MustQuery("select LOCATE(p4,n3) from t1;").Check(testkit.Events("0", "0", "0"))
	tk.MustQuery("select LOCATE(p4,n4) from t1;").Check(testkit.Events("0", "1", "1"))

	tk.MustExec("truncate block t1;")
	tk.MustExec("insert into t1 (a) values (1);")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (2,'0aA1!测试テストמבחן  ','0aA1!测试テストמבחן       ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (3,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (4,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (5,'0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0Aa1!测试','0Aa1!测试','0Aa1!测试','0Aa1!测试');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (6,'0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试','0aA1!测试','0aA1!测试','0aA1!测试');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (7,'0aA1!测试テストמבחן  ','0aA1!测试テストמבחן       ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (8,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ');")

	tk.MustQuery("select p1 REGEXP n1 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p1 REGEXP n2 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p1 REGEXP n3 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p1 REGEXP n4 from t1;").Check(testkit.Events("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p2 REGEXP n1 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p2 REGEXP n2 from t1;").Check(testkit.Events("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p2 REGEXP n3 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p2 REGEXP n4 from t1;").Check(testkit.Events("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p3 REGEXP n1 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p3 REGEXP n2 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p3 REGEXP n3 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p3 REGEXP n4 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p4 REGEXP n1 from t1;").Check(testkit.Events("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p4 REGEXP n2 from t1;").Check(testkit.Events("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p4 REGEXP n3 from t1;").Check(testkit.Events("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p4 REGEXP n4 from t1;").Check(testkit.Events("<nil>", "1", "1", "0", "1", "1", "1", "0"))

	tk.MustExec("drop block t1;")
}

func (s *testIntegrationSerialSuite) TestDefCauslateLike(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci")
	tk.MustQuery("select 'a' like 'A'").Check(testkit.Events("1"))
	tk.MustQuery("select 'a' like 'A' defCauslate utf8mb4_general_ci").Check(testkit.Events("1"))
	tk.MustQuery("select 'a' like 'À'").Check(testkit.Events("1"))
	tk.MustQuery("select 'a' like '%À'").Check(testkit.Events("1"))
	tk.MustQuery("select 'a' like '%À '").Check(testkit.Events("0"))
	tk.MustQuery("select 'a' like 'À%'").Check(testkit.Events("1"))
	tk.MustQuery("select 'a' like 'À_'").Check(testkit.Events("0"))
	tk.MustQuery("select 'a' like '%À%'").Check(testkit.Events("1"))
	tk.MustQuery("select 'aaa' like '%ÀAa%'").Check(testkit.Events("1"))
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_bin")

	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t_like;")
	tk.MustExec("create block t_like(id int, b varchar(20) defCauslate utf8mb4_general_ci);")
	tk.MustExec("insert into t_like values (1, 'aaa'), (2, 'abc'), (3, 'aac');")
	tk.MustQuery("select b like 'AaÀ' from t_like order by id;").Check(testkit.Events("1", "0", "0"))
	tk.MustQuery("select b like 'Aa_' from t_like order by id;").Check(testkit.Events("1", "0", "1"))
	tk.MustQuery("select b like '_A_' from t_like order by id;").Check(testkit.Events("1", "0", "1"))
	tk.MustQuery("select b from t_like where b like 'Aa_' order by id;").Check(testkit.Events("aaa", "aac"))
	tk.MustQuery("select b from t_like where b like 'A%' order by id;").Check(testkit.Events("aaa", "abc", "aac"))
	tk.MustQuery("select b from t_like where b like '%A%' order by id;").Check(testkit.Events("aaa", "abc", "aac"))
	tk.MustExec("alter block t_like add index idx_b(b);")
	tk.MustQuery("select b from t_like use index(idx_b) where b like 'Aa_' order by id;").Check(testkit.Events("aaa", "aac"))
	tk.MustQuery("select b from t_like use index(idx_b) where b like 'A%' order by id;").Check(testkit.Events("aaa", "abc", "aac"))
	tk.MustQuery("select b from t_like use index(idx_b) where b like '%A%' order by id;").Check(testkit.Events("aaa", "abc", "aac"))
}

func (s *testIntegrationSerialSuite) TestDefCauslateSubQuery(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := s.prepare4DefCauslation(c, false)
	tk.MustQuery("select id from t where v in (select v from t_bin) order by id").Check(testkit.Events("1", "2", "3", "4", "5", "6", "7"))
	tk.MustQuery("select id from t_bin where v in (select v from t) order by id").Check(testkit.Events("1", "2", "3", "4", "5", "6", "7"))
	tk.MustQuery("select id from t where v not in (select v from t_bin) order by id").Check(testkit.Events())
	tk.MustQuery("select id from t_bin where v not in (select v from t) order by id").Check(testkit.Events())
	tk.MustQuery("select id from t where exists (select 1 from t_bin where t_bin.v=t.v) order by id").Check(testkit.Events("1", "2", "3", "4", "5", "6", "7"))
	tk.MustQuery("select id from t_bin where exists (select 1 from t where t_bin.v=t.v) order by id").Check(testkit.Events("1", "2", "3", "4", "5", "6", "7"))
	tk.MustQuery("select id from t where not exists (select 1 from t_bin where t_bin.v=t.v) order by id").Check(testkit.Events())
	tk.MustQuery("select id from t_bin where not exists (select 1 from t where t_bin.v=t.v) order by id").Check(testkit.Events())
}

func (s *testIntegrationSerialSuite) TestDefCauslateDBS(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database t;")
	tk.MustExec("use t;")
	tk.MustExec("drop database t;")
}

func (s *testIntegrationSerialSuite) TestNewDefCauslationCheckClusterIndexBlock(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("set milevadb_enable_clustered_index=1")
	tk.MustExec("create block t(name char(255) primary key, b int, c int, index idx(name), unique index uidx(name))")
	tk.MustExec("insert into t values(\"aaaa\", 1, 1), (\"bbb\", 2, 2), (\"ccc\", 3, 3)")
	tk.MustExec("admin check block t")
}

func (s *testIntegrationSuite) TestIssue15986(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 int)")
	tk.MustExec("INSERT INTO t0 VALUES (0)")
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE CHAR(204355900);").Check(testkit.Events("0"))
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE not CHAR(204355900);").Check(testkit.Events())
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE '.0';").Check(testkit.Events())
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE not '.0';").Check(testkit.Events("0"))
	// If the number does not exceed the range of float64 and its value is not 0, it will be converted to true.
	tk.MustQuery("select * from t0 where '.000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000009';").Check(testkit.Events("0"))
	tk.MustQuery("select * from t0 where not '.000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000009';").Check(testkit.Events())

	// If the number is truncated beyond the range of float64, it will be converted to true when the truncated result is 0.
	tk.MustQuery("select * from t0 where '.0000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000009';").Check(testkit.Events())
	tk.MustQuery("select * from t0 where not '.0000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		"00000000000000000000000000000000000000000000000000000000000000000000000000000000000009';").Check(testkit.Events("0"))
}

func (s *testIntegrationSuite) TestNegativeZeroForHashJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(c0 float);")
	tk.MustExec("CREATE TABLE t1(c0 float);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (0);")
	tk.MustQuery("SELECT t1.c0 FROM t1, t0 WHERE t0.c0=-t1.c0;").Check(testkit.Events("0"))
	tk.MustExec("drop TABLE t0;")
	tk.MustExec("drop block t1;")
}

func (s *testIntegrationSuite) TestIssue1223(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists testjson")
	tk.MustExec("CREATE TABLE testjson (j json DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	tk.MustExec(`INSERT INTO testjson SET j='{"test":3}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":0}';`)
	tk.MustExec(`insert into testjson set j='{"test":"0"}';`)
	tk.MustExec(`insert into testjson set j='{"test":0.0}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":"aaabbb"}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":3.1415}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":[]}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":[1,2]}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":["b","c"]}';`)
	tk.MustExec(`INSERT INTO testjson SET j='{"test":{"ke":"val"}}';`)
	tk.MustExec(`insert into testjson set j='{"test":"2020-07-27 09:43:47"}';`)
	tk.MustExec(`insert into testjson set j='{"test":"0000-00-00 00:00:00"}';`)
	tk.MustExec(`insert into testjson set j='{"test":"0778"}';`)
	tk.MustExec(`insert into testjson set j='{"test":"0000"}';`)
	tk.MustExec(`insert into testjson set j='{"test":null}';`)
	tk.MustExec(`insert into testjson set j=null;`)
	tk.MustExec(`insert into testjson set j='{"test":[null]}';`)
	tk.MustExec(`insert into testjson set j='{"test":true}';`)
	tk.MustExec(`insert into testjson set j='{"test":false}';`)
	tk.MustExec(`insert into testjson set j='""';`)
	tk.MustExec(`insert into testjson set j='null';`)
	tk.MustExec(`insert into testjson set j='0';`)
	tk.MustExec(`insert into testjson set j='"0"';`)
	tk.MustQuery("SELECT * FROM testjson WHERE JSON_EXTRACT(j,'$.test');").Check(testkit.Events(`{"test": 3}`,
		`{"test": "0"}`, `{"test": "aaabbb"}`, `{"test": 3.1415}`, `{"test": []}`, `{"test": [1, 2]}`,
		`{"test": ["b", "c"]}`, `{"test": {"ke": "val"}}`, `{"test": "2020-07-27 09:43:47"}`,
		`{"test": "0000-00-00 00:00:00"}`, `{"test": "0778"}`, `{"test": "0000"}`, `{"test": null}`,
		`{"test": [null]}`, `{"test": true}`, `{"test": false}`))
	tk.MustQuery("select * from testjson where j;").Check(testkit.Events(`{"test": 3}`, `{"test": 0}`,
		`{"test": "0"}`, `{"test": 0}`, `{"test": "aaabbb"}`, `{"test": 3.1415}`, `{"test": []}`, `{"test": [1, 2]}`,
		`{"test": ["b", "c"]}`, `{"test": {"ke": "val"}}`, `{"test": "2020-07-27 09:43:47"}`,
		`{"test": "0000-00-00 00:00:00"}`, `{"test": "0778"}`, `{"test": "0000"}`, `{"test": null}`,
		`{"test": [null]}`, `{"test": true}`, `{"test": false}`, `""`, "null", `"0"`))
	tk.MustExec("insert into allegrosql.expr_pushdown_blacklist values('json_extract','einsteindb','');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	tk.MustQuery("SELECT * FROM testjson WHERE JSON_EXTRACT(j,'$.test');").Check(testkit.Events("{\"test\": 3}",
		"{\"test\": \"0\"}", "{\"test\": \"aaabbb\"}", "{\"test\": 3.1415}", "{\"test\": []}", "{\"test\": [1, 2]}",
		"{\"test\": [\"b\", \"c\"]}", "{\"test\": {\"ke\": \"val\"}}", "{\"test\": \"2020-07-27 09:43:47\"}",
		"{\"test\": \"0000-00-00 00:00:00\"}", "{\"test\": \"0778\"}", "{\"test\": \"0000\"}", "{\"test\": null}",
		"{\"test\": [null]}", "{\"test\": true}", "{\"test\": false}"))
	tk.MustQuery("select * from testjson where j;").Check(testkit.Events(`{"test": 3}`, `{"test": 0}`,
		`{"test": "0"}`, `{"test": 0}`, `{"test": "aaabbb"}`, `{"test": 3.1415}`, `{"test": []}`, `{"test": [1, 2]}`,
		`{"test": ["b", "c"]}`, `{"test": {"ke": "val"}}`, `{"test": "2020-07-27 09:43:47"}`,
		`{"test": "0000-00-00 00:00:00"}`, `{"test": "0778"}`, `{"test": "0000"}`, `{"test": null}`,
		`{"test": [null]}`, `{"test": true}`, `{"test": false}`, `""`, "null", `"0"`))
}

func (s *testIntegrationSerialSuite) TestNewDefCauslationWithClusterIndex(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("set milevadb_enable_clustered_index=1")
	tk.MustExec("create block t(d double primary key, a int, name varchar(255), index idx(name(2)), index midx(a, name))")
	tk.MustExec("insert into t values(2.11, 1, \"aa\"), (-1, 0, \"abcd\"), (9.99, 0, \"aaaa\")")
	tk.MustQuery("select d from t use index(idx) where name=\"aa\"").Check(testkit.Events("2.11"))
}

func (s *testIntegrationSuite) TestIssue15743(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 int)")
	tk.MustExec("INSERT INTO t0 VALUES (1)")
	tk.MustQuery("SELECT * FROM t0 WHERE 1 AND 0.4").Check(testkit.Events("1"))
}

func (s *testIntegrationSuite) TestIssue15725(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(2)")
	tk.MustQuery("select * from t where (not not a) = a").Check(testkit.Events())
	tk.MustQuery("select * from t where (not not not not a) = a").Check(testkit.Events())
}

func (s *testIntegrationSuite) TestIssue15790(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (0);")
	tk.MustQuery("SELECT * FROM t0 WHERE -10000000000000000000 | t0.c0 UNION SELECT * FROM t0;").Check(testkit.Events("0"))
	tk.MustQuery("SELECT * FROM t0 WHERE -10000000000000000000 | t0.c0 UNION all SELECT * FROM t0;").Check(testkit.Events("0", "0"))
	tk.MustExec("drop block t0;")
}

func (s *testIntegrationSuite) TestIssue15990(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 TEXT(10));")
	tk.MustExec("INSERT INTO t0(c0) VALUES (1);")
	tk.MustQuery("SELECT * FROM t0 WHERE ('a' != t0.c0) AND t0.c0;").Check(testkit.Events("1"))
	tk.MustExec("CREATE INDEX i0 ON t0(c0(10));")
	tk.MustQuery("SELECT * FROM t0 WHERE ('a' != t0.c0) AND t0.c0;").Check(testkit.Events("1"))
	tk.MustExec("drop block t0;")
}

func (s *testIntegrationSuite) TestIssue15992(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT, c1 INT AS (c0));")
	tk.MustExec("CREATE INDEX i0 ON t0(c1);")
	tk.MustQuery("SELECT t0.c0 FROM t0 UNION ALL SELECT 0 FROM t0;").Check(testkit.Events())
	tk.MustExec("drop block t0;")
}

func (s *testIntegrationSuite) TestIssue16419(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t0")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 INT);")
	tk.MustQuery("SELECT * FROM t1 NATURAL LEFT JOIN t0 WHERE NOT t1.c0;").Check(testkit.Events())
	tk.MustExec("drop block t0, t1;")
}

func (s *testIntegrationSuite) TestIssue16029(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t0,t1;")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 INT);")
	tk.MustExec("INSERT INTO t0 VALUES (NULL), (1);")
	tk.MustExec("INSERT INTO t1 VALUES (0);")
	tk.MustQuery("SELECT t0.c0 FROM t0 JOIN t1 ON (t0.c0 REGEXP 1) | t1.c0  WHERE BINARY STRCMP(t1.c0, t0.c0);").Check(testkit.Events("1"))
	tk.MustExec("drop block t0;")
	tk.MustExec("drop block t1;")
}

func (s *testIntegrationSuite) TestIssue16426(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int)")
	tk.MustExec("insert into t values (42)")
	tk.MustQuery("select a from t where a/10000").Check(testkit.Events("42"))
	tk.MustQuery("select a from t where a/100000").Check(testkit.Events("42"))
	tk.MustQuery("select a from t where a/1000000").Check(testkit.Events("42"))
	tk.MustQuery("select a from t where a/10000000").Check(testkit.Events("42"))
}

func (s *testIntegrationSuite) TestIssue16505(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("CREATE TABLE t(c varchar(100), index idx(c(100)));")
	tk.MustExec("INSERT INTO t VALUES (NULL),('1'),('0'),(''),('aaabbb'),('0abc'),('123e456'),('0.0001deadsfeww');")
	tk.MustQuery("select * from t where c;").Sort().Check(testkit.Events("0.0001deadsfeww", "1", "123e456"))
	tk.MustQuery("select /*+ USE_INDEX(t, idx) */ * from t where c;").Sort().Check(testkit.Events("0.0001deadsfeww", "1", "123e456"))
	tk.MustQuery("select /*+ IGNORE_INDEX(t, idx) */* from t where c;").Sort().Check(testkit.Events("0.0001deadsfeww", "1", "123e456"))
	tk.MustExec("drop block t;")
}

func (s *testIntegrationSuite) TestIssue16779(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t0")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t0 (c0 int)")
	tk.MustExec("create block t1 (c0 int)")
	tk.MustQuery("SELECT * FROM t1 LEFT JOIN t0 ON TRUE WHERE BINARY EXPORT_SET(0, 0, 0 COLLATE 'binary', t0.c0, 0 COLLATE 'binary')")
}

func (s *testIntegrationSuite) TestIssue16697(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE t (v varchar(1024))")
	tk.MustExec("insert into t values (space(1024))")
	for i := 0; i < 5; i++ {
		tk.MustExec("insert into t select * from t")
	}
	rows := tk.MustQuery("explain analyze select * from t").Events()
	for _, event := range rows {
		line := fmt.Sprintf("%v", event)
		if strings.Contains(line, "Projection") {
			c.Assert(strings.Contains(line, "KB"), IsTrue)
			c.Assert(strings.Contains(line, "MB"), IsFalse)
			c.Assert(strings.Contains(line, "GB"), IsFalse)
		}
	}
}

func (s *testIntegrationSuite) TestIssue17045(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int,b varchar(20),c datetime,d double,e int,f int as(a+b),key(a),key(b),key(c),key(d),key(e),key(f));")
	tk.MustExec("insert into t(a,b,e) values(null,\"5\",null);")
	tk.MustExec("insert into t(a,b,e) values(\"5\",null,null);")
	tk.MustQuery("select /*+ use_index_merge(t)*/ * from t where t.e=5 or t.a=5;").Check(testkit.Events("5 <nil> <nil> <nil> <nil> <nil>"))
}

func (s *testIntegrationSuite) TestIssue17098(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1, t2")
	tk.MustExec("create block t1(a char) defCauslate utf8mb4_bin;")
	tk.MustExec("create block t2(a char) defCauslate utf8mb4_bin;;")
	tk.MustExec("insert into t1 values('a');")
	tk.MustExec("insert into t2 values('a');")
	tk.MustQuery("select defCauslation(t1.a) from t1 union select defCauslation(t2.a) from t2;").Check(testkit.Events("utf8mb4_bin"))
}

func (s *testIntegrationSerialSuite) TestIssue17176(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustGetErrMsg("create block t(a enum('a', 'a ')) charset utf8 defCauslate utf8_bin;", "[types:1291]DeferredCauset 'a' has duplicated value 'a' in ENUM")
	tk.MustGetErrMsg("create block t(a enum('a', 'Á')) charset utf8 defCauslate utf8_general_ci;", "[types:1291]DeferredCauset 'a' has duplicated value 'Á' in ENUM")
	tk.MustGetErrMsg("create block t(a enum('a', 'a ')) charset utf8mb4 defCauslate utf8mb4_bin;", "[types:1291]DeferredCauset 'a' has duplicated value 'a' in ENUM")
	tk.MustExec("create block t(a enum('a', 'A')) charset utf8 defCauslate utf8_bin;")
	tk.MustExec("drop block t;")
	tk.MustExec("create block t3(a enum('a', 'A')) charset utf8mb4 defCauslate utf8mb4_bin;")
}

func (s *testIntegrationSuite) TestIssue17115(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select defCauslation(user());").Check(testkit.Events("utf8mb4_bin"))
	tk.MustQuery("select defCauslation(compress('abc'));").Check(testkit.Events("binary"))
}

func (s *testIntegrationSuite) TestIndexedVirtualGeneratedDeferredCausetTruncate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t(a int, b tinyint as(a+100) unique key)")
	tk.MustExec("insert ignore into t values(200, default)")
	tk.MustExec("uFIDelate t set a=1 where a=200")
	tk.MustExec("admin check block t")
	tk.MustExec("delete from t")
	tk.MustExec("insert ignore into t values(200, default)")
	tk.MustExec("admin check block t")
	tk.MustExec("insert ignore into t values(200, default) on duplicate key uFIDelate a=100")
	tk.MustExec("admin check block t")
	tk.MustExec("delete from t")
	tk.MustExec("admin check block t")

	tk.MustExec("begin")
	tk.MustExec("insert ignore into t values(200, default)")
	tk.MustExec("uFIDelate t set a=1 where a=200")
	tk.MustExec("admin check block t")
	tk.MustExec("delete from t")
	tk.MustExec("insert ignore into t values(200, default)")
	tk.MustExec("admin check block t")
	tk.MustExec("insert ignore into t values(200, default) on duplicate key uFIDelate a=100")
	tk.MustExec("admin check block t")
	tk.MustExec("delete from t")
	tk.MustExec("admin check block t")
	tk.MustExec("commit")
	tk.MustExec("admin check block t")
}

func (s *testIntegrationSuite) TestIssue17287(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test;")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("set @@milevadb_enable_vectorized_expression = false;")
	tk.MustExec("create block t(a datetime);")
	tk.MustExec("insert into t values(from_unixtime(1589873945)), (from_unixtime(1589873946));")
	tk.MustExec("prepare stmt7 from 'SELECT unix_timestamp(a) FROM t WHERE a = from_unixtime(?);';")
	tk.MustExec("set @val1 = 1589873945;")
	tk.MustExec("set @val2 = 1589873946;")
	tk.MustQuery("execute stmt7 using @val1;").Check(testkit.Events("1589873945"))
	tk.MustQuery("execute stmt7 using @val2;").Check(testkit.Events("1589873946"))
}

func (s *testIntegrationSuite) TestIssue17898(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("drop block t0")
	tk.MustExec("create block t0(a char(10), b int as ((a)));")
	tk.MustExec("insert into t0(a) values(\"0.5\");")
	tk.MustQuery("select * from t0;").Check(testkit.Events("0.5 1"))
}

func (s *testIntegrationSuite) TestIssue17727(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test;")
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY auto_increment, a timestamp NOT NULL);")
	tk.MustExec("INSERT INTO t1 VALUES (null, '2020-05-30 20:30:00');")
	tk.MustExec("PREPARE mystmt FROM 'SELECT * FROM t1 WHERE UNIX_TIMESTAMP(a) >= ?';")
	tk.MustExec("SET @a=1590868800;")
	tk.MustQuery("EXECUTE mystmt USING @a;").Check(testkit.Events())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Events("0"))

	tk.MustExec("SET @a=1590868801;")
	tk.MustQuery("EXECUTE mystmt USING @a;").Check(testkit.Events())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Events("1"))

	tk.MustExec("prepare stmt from 'select unix_timestamp(?)';")
	tk.MustExec("set @a = '2020-05-30 20:30:00';")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Events("1590841800"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Events("0"))

	tk.MustExec("set @a = '2020-06-12 13:47:58';")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Events("1591940878"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Events("1"))
}

func (s *testIntegrationSerialSuite) TestIssue17891(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(id int, value set ('a','b','c') charset utf8mb4 defCauslate utf8mb4_bin default 'a,b ');")
	tk.MustExec("drop block t")
	tk.MustExec("create block test(id int, value set ('a','b','c') charset utf8mb4 defCauslate utf8mb4_general_ci default 'a,B ,C');")
}

func (s *testIntegrationSerialSuite) TestIssue17233(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists block_int")
	tk.MustExec(`CREATE TABLE block_int (
	  id_0 int(16) NOT NULL AUTO_INCREMENT,
	  defCaus_int_0 int(16) DEFAULT NULL,
	  PRIMARY KEY (id_0),
	  KEY fvclc (id_0,defCaus_int_0));`)
	tk.MustExec("INSERT INTO block_int VALUES (1,NULL),(2,NULL),(3,65535),(4,1),(5,0),(6,NULL),(7,-1),(8,65535),(9,NULL),(10,65535),(11,-1),(12,0),(13,-1),(14,1),(15,65535),(16,0),(17,1),(18,0),(19,0)")

	tk.MustExec("drop block if exists block_varchar")
	tk.MustExec(`CREATE TABLE block_varchar (
	  id_2 int(16) NOT NULL AUTO_INCREMENT,
	  defCaus_varchar_2 varchar(511) DEFAULT NULL,
	  PRIMARY KEY (id_2));`)
	tk.MustExec(`INSERT INTO block_varchar VALUES (1,''),(2,''),(3,''),(4,''),(5,''),(6,''),(7,''),(8,''),(9,''),(10,''),(11,''),(12,'');`)

	tk.MustExec("drop block if exists block_float_varchar")
	tk.MustExec(`CREATE TABLE block_int_float_varchar (
	  id_6 int(16) NOT NULL AUTO_INCREMENT,
	  defCaus_int_6 int(16) NOT NULL,
	  defCaus_float_6 float DEFAULT NULL,
	  defCaus_varchar_6 varchar(511) DEFAULT NULL,
	  PRIMARY KEY (id_6,defCaus_int_6)
	)
	PARTITION BY RANGE ( defCaus_int_6 ) (
	  PARTITION p0 VALUES LESS THAN (1),
	  PARTITION p2 VALUES LESS THAN (1000),
	  PARTITION p3 VALUES LESS THAN (10000),
	  PARTITION p5 VALUES LESS THAN (1000000),
	  PARTITION p7 VALUES LESS THAN (100000000),
	  PARTITION p9 VALUES LESS THAN (10000000000),
	  PARTITION p10 VALUES LESS THAN (100000000000),
	  PARTITION pn VALUES LESS THAN (MAXVALUE));`)
	tk.MustExec(`INSERT INTO block_int_float_varchar VALUES (1,-1,0.1,'0000-00-00 00:00:00'),(2,0,0,NULL),(3,-1,1,NULL),(4,0,NULL,NULL),(7,0,0.5,NULL),(8,0,0,NULL),(10,-1,0,'-1'),(5,1,-0.1,NULL),(6,1,0.1,NULL),(9,65535,0,'1');`)

	tk.MustExec("drop block if exists block_float")
	tk.MustExec(`CREATE TABLE block_float (
	  id_1 int(16) NOT NULL AUTO_INCREMENT,
	  defCaus_float_1 float DEFAULT NULL,
	  PRIMARY KEY (id_1),
	  KEY zbjus (id_1,defCaus_float_1));`)
	tk.MustExec(`INSERT INTO block_float VALUES (1,NULL),(2,-0.1),(3,-1),(4,NULL),(5,-0.1),(6,0),(7,0),(8,-1),(9,NULL),(10,NULL),(11,0.1),(12,-1);`)

	tk.MustExec("drop view if exists view_4")
	tk.MustExec(`CREATE DEFINER='root'@'127.0.0.1' VIEW view_4 (defCaus_1, defCaus_2, defCaus_3, defCaus_4, defCaus_5, defCaus_6, defCaus_7, defCaus_8, defCaus_9, defCaus_10) AS
    SELECT /*+ USE_INDEX(block_int fvclc, fvclc)*/
        tmp1.id_6 AS defCaus_1,
        tmp1.defCaus_int_6 AS defCaus_2,
        tmp1.defCaus_float_6 AS defCaus_3,
        tmp1.defCaus_varchar_6 AS defCaus_4,
        tmp2.id_2 AS defCaus_5,
        tmp2.defCaus_varchar_2 AS defCaus_6,
        tmp3.id_0 AS defCaus_7,
        tmp3.defCaus_int_0 AS defCaus_8,
        tmp4.id_1 AS defCaus_9,
        tmp4.defCaus_float_1 AS defCaus_10
    FROM ((
            test.block_int_float_varchar AS tmp1 LEFT JOIN
            test.block_varchar AS tmp2 ON ((NULL<=tmp2.defCaus_varchar_2)) IS NULL
        ) JOIN
        test.block_int AS tmp3 ON (1.117853833115198e-03!=tmp1.defCaus_int_6))
    JOIN
        test.block_float AS tmp4 ON !((1900370398268920328=0e+00)) WHERE ((''<='{Gm~PcZNb') OR (tmp2.id_2 OR tmp3.defCaus_int_0)) ORDER BY defCaus_1,defCaus_2,defCaus_3,defCaus_4,defCaus_5,defCaus_6,defCaus_7,defCaus_8,defCaus_9,defCaus_10 LIMIT 20580,5;`)

	tk.MustExec("drop view if exists view_10")
	tk.MustExec(`CREATE DEFINER='root'@'127.0.0.1' VIEW view_10 (defCaus_1, defCaus_2) AS
    SELECT  block_int.id_0 AS defCaus_1,
            block_int.defCaus_int_0 AS defCaus_2
    FROM test.block_int
    WHERE
        ((-1e+00=1) OR (0e+00>=block_int.defCaus_int_0))
    ORDER BY defCaus_1,defCaus_2
    LIMIT 5,9;`)

	tk.MustQuery("SELECT defCaus_1 FROM test.view_10").Sort().Check(testkit.Events("16", "18", "19"))
	tk.MustQuery("SELECT defCaus_1 FROM test.view_4").Sort().Check(testkit.Events("8", "8", "8", "8", "8"))
	tk.MustQuery("SELECT view_10.defCaus_1 FROM view_4 JOIN view_10").Check(testkit.Events("16", "16", "16", "16", "16", "18", "18", "18", "18", "18", "19", "19", "19", "19", "19"))
}

func (s *testIntegrationSuite) TestIssue18515(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b json, c int AS (JSON_EXTRACT(b, '$.population')), key(c));")
	tk.MustExec("select /*+ MilevaDB_INLJ(t2) */ t1.a, t1.c, t2.a from t t1, t t2 where t1.c=t2.c;")
}

func (s *testIntegrationSuite) TestIssue18525(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (defCaus0 BLOB, defCaus1 CHAR(74), defCaus2 DATE UNIQUE)")
	tk.MustExec("insert into t1 values ('l', '7a34bc7d-6786-461b-92d3-fd0a6cd88f39', '1000-01-03')")
	tk.MustExec("insert into t1 values ('l', NULL, '1000-01-04')")
	tk.MustExec("insert into t1 values ('b', NULL, '1000-01-02')")
	tk.MustQuery("select INTERVAL( ( CONVERT( -11752 USING utf8 ) ), 6558853612195285496, `defCaus1`) from t1").Check(testkit.Events("0", "0", "0"))

}

func (s *testIntegrationSerialSuite) TestIssue17989(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b tinyint as(a+1), c int as(b+1));")
	tk.MustExec("set sql_mode='';")
	tk.MustExec("insert into t(a) values(2000);")
	tk.MustExec("create index idx on t(c);")
	tk.MustQuery("select c from t;").Check(testkit.Events("128"))
	tk.MustExec("admin check block t")
}

func (s *testIntegrationSerialSuite) TestIssue18638(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a varchar(10) defCauslate utf8mb4_bin, b varchar(10) defCauslate utf8mb4_general_ci);")
	tk.MustExec("insert into t (a, b) values ('a', 'A');")
	tk.MustQuery("select * from t t1, t t2 where t1.a = t2.b defCauslate utf8mb4_general_ci;").Check(testkit.Events("a A a A"))
	tk.MustQuery("select * from t t1 left join t t2 on t1.a = t2.b defCauslate utf8mb4_general_ci;").Check(testkit.Events("a A a A"))
}

func (s *testIntegrationSuite) TestIssue18850(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t, t1")
	tk.MustExec("create block t(a int, b enum('A', 'B'));")
	tk.MustExec("create block t1(a1 int, b1 enum('B', 'A'));")
	tk.MustExec("insert into t values (1, 'A');")
	tk.MustExec("insert into t1 values (1, 'A');")
	tk.MustQuery("select /*+ HASH_JOIN(t, t1) */ * from t join t1 on t.b = t1.b1;").Check(testkit.Events("1 A 1 A"))

	tk.MustExec("drop block t, t1")
	tk.MustExec("create block t(a int, b set('A', 'B'));")
	tk.MustExec("create block t1(a1 int, b1 set('B', 'A'));")
	tk.MustExec("insert into t values (1, 'A');")
	tk.MustExec("insert into t1 values (1, 'A');")
	tk.MustQuery("select /*+ HASH_JOIN(t, t1) */ * from t join t1 on t.b = t1.b1;").Check(testkit.Events("1 A 1 A"))
}

func (s *testIntegrationSerialSuite) TestNullValueRange(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, index(a))")
	tk.MustExec("insert into t values (null, 0), (null, 1), (10, 11), (10, 12)")
	tk.MustQuery("select * from t use index(a) where a is null order by b").Check(testkit.Events("<nil> 0", "<nil> 1"))
	tk.MustQuery("select * from t use index(a) where a<=>null order by b").Check(testkit.Events("<nil> 0", "<nil> 1"))
	tk.MustQuery("select * from t use index(a) where a<=>10 order by b").Check(testkit.Events("10 11", "10 12"))

	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(a int, b int, c int, unique key(a, b, c))")
	tk.MustExec("insert into t1 values (1, null, 1), (1, null, 2), (1, null, 3), (1, null, 4)")
	tk.MustExec("insert into t1 values (1, 1, 1), (1, 2, 2), (1, 3, 33), (1, 4, 44)")
	tk.MustQuery("select c from t1 where a=1 and b<=>null and c>2 order by c").Check(testkit.Events("3", "4"))
	tk.MustQuery("select c from t1 where a=1 and b is null and c>2 order by c").Check(testkit.Events("3", "4"))
	tk.MustQuery("select c from t1 where a=1 and b is not null and c>2 order by c").Check(testkit.Events("33", "44"))
}

func (s *testIntegrationSerialSuite) TestIssue18652(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1 ( `pk` int not null primary key auto_increment, `defCaus_smallint_key_signed` smallint  , key (`defCaus_smallint_key_signed`))")
	tk.MustExec("INSERT INTO `t1` VALUES (1,0),(2,NULL),(3,NULL),(4,0),(5,0),(6,NULL),(7,NULL),(8,0),(9,0),(10,0)")
	tk.MustQuery("SELECT * FROM t1 WHERE ( LOG( `defCaus_smallint_key_signed`, -8297584758403770424 ) ) DIV 1").Check(testkit.Events())
}

func (s *testIntegrationSerialSuite) TestIssue18662(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a varchar(10) defCauslate utf8mb4_bin, b varchar(10) defCauslate utf8mb4_general_ci);")
	tk.MustExec("insert into t (a, b) values ('a', 'A');")
	tk.MustQuery("select * from t where field('A', a defCauslate utf8mb4_general_ci, b) > 1;").Check(testkit.Events())
	tk.MustQuery("select * from t where field('A', a, b defCauslate utf8mb4_general_ci) > 1;").Check(testkit.Events())
	tk.MustQuery("select * from t where field('A' defCauslate utf8mb4_general_ci, a, b) > 1;").Check(testkit.Events())
	tk.MustQuery("select * from t where field('A', a, b) > 1;").Check(testkit.Events("a A"))
}

func (s *testIntegrationSerialSuite) TestIssue19045(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t, t1, t2")
	tk.MustExec(`CREATE TABLE t (
  id int(11) NOT NULL AUTO_INCREMENT,
  a char(10) DEFAULT NULL,
  PRIMARY KEY (id)
);`)
	tk.MustExec(`CREATE TABLE t1 (
  id int(11) NOT NULL AUTO_INCREMENT,
  a char(10) DEFAULT NULL,
  b char(10) DEFAULT NULL,
  c char(10) DEFAULT NULL,
  PRIMARY KEY (id)
);`)
	tk.MustExec(`CREATE TABLE t2 (
  id int(11) NOT NULL AUTO_INCREMENT,
  a char(10) DEFAULT NULL,
  b char(10) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY b (b)
);`)
	tk.MustExec(`insert into t1(a,b,c) values('hs4_0004', "04", "101"), ('a01', "01", "101"),('a011', "02", "101");`)
	tk.MustExec(`insert into t2(a,b) values("02","03");`)
	tk.MustExec(`insert into t(a) values('101'),('101');`)
	tk.MustQuery(`select  ( SELECT t1.a FROM  t1,  t2 WHERE t1.b = t2.a AND  t2.b = '03' AND t1.c = a.a) invode from t a ;`).Check(testkit.Events("a011", "a011"))
}

func (s *testIntegrationSerialSuite) TestIssue19116(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("set names utf8mb4 defCauslate utf8mb4_general_ci;")
	tk.MustQuery("select defCauslation(concat(1 defCauslate `binary`));").Check(testkit.Events("binary"))
	tk.MustQuery("select coercibility(concat(1 defCauslate `binary`));").Check(testkit.Events("0"))
	tk.MustQuery("select defCauslation(concat(NULL,NULL));").Check(testkit.Events("binary"))
	tk.MustQuery("select coercibility(concat(NULL,NULL));").Check(testkit.Events("6"))
	tk.MustQuery("select defCauslation(concat(1,1));").Check(testkit.Events("utf8mb4_general_ci"))
	tk.MustQuery("select coercibility(concat(1,1));").Check(testkit.Events("4"))
	tk.MustQuery("select defCauslation(1);").Check(testkit.Events("binary"))
	tk.MustQuery("select coercibility(1);").Check(testkit.Events("5"))
	tk.MustQuery("select coercibility(1=1);").Check(testkit.Events("5"))
}

func (s *testIntegrationSerialSuite) TestIssue14448and19383(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1 (a INT NOT NULL PRIMARY KEY)")
	tk.MustExec("INSERT INTO t1 VALUES (1),(2),(3)")
	_, err := tk.Exec("SELECT ALLEGROSQL_CALC_FOUND_ROWS * FROM t1 LIMIT 1")
	message := `function ALLEGROSQL_CALC_FOUND_ROWS has only noop implementation in milevadb now, use milevadb_enable_noop_functions to enable these functions`
	c.Assert(strings.Contains(err.Error(), message), IsTrue)
	_, err = tk.Exec("SELECT * FROM t1 LOCK IN SHARE MODE")
	message = `function LOCK IN SHARE MODE has only noop implementation in milevadb now, use milevadb_enable_noop_functions to enable these functions`
	c.Assert(strings.Contains(err.Error(), message), IsTrue)
	tk.MustExec("SET milevadb_enable_noop_functions=1")
	tk.MustExec("SELECT ALLEGROSQL_CALC_FOUND_ROWS * FROM t1 LIMIT 1")
	tk.MustExec("SELECT * FROM t1 LOCK IN SHARE MODE")
}

func (s *testIntegrationSerialSuite) TestIssue19315(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("CREATE TABLE `t` (`a` bit(10) DEFAULT NULL,`b` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("INSERT INTO `t` VALUES (_binary '\\0',1),(_binary '\\0',2),(_binary '\\0',5),(_binary '\\0',4),(_binary '\\0',2),(_binary '\\0	',4)")
	tk.MustExec("CREATE TABLE `t1` (`a` int(11) DEFAULT NULL, `b` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("INSERT INTO `t1` VALUES (1,1),(1,5),(2,3),(2,4),(3,3)")
	err := tk.QueryToErr("select * from t where t.b > (select min(t1.b) from t1 where t1.a > t.a)")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSerialSuite) TestIssue18674(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select -1.0 % -1.0").Check(testkit.Events("0.0"))
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(`pk` int primary key,`defCaus_float_key_signed` float  ,key (`defCaus_float_key_signed`))")
	tk.MustExec("insert into t1 values (0, null), (1, 0), (2, -0), (3, 1), (-1,-1)")
	tk.MustQuery("select * from t1 where ( `defCaus_float_key_signed` % `defCaus_float_key_signed`) IS FALSE").Sort().Check(testkit.Events("-1 -1", "3 1"))
	tk.MustQuery("select  `defCaus_float_key_signed` , `defCaus_float_key_signed` % `defCaus_float_key_signed` from t1").Sort().Check(testkit.Events(
		"-1 -0", "0 <nil>", "0 <nil>", "1 0", "<nil> <nil>"))
	tk.MustQuery("select  `defCaus_float_key_signed` , (`defCaus_float_key_signed` % `defCaus_float_key_signed`) IS FALSE from t1").Sort().Check(testkit.Events(
		"-1 1", "0 0", "0 0", "1 1", "<nil> 0"))
}

func (s *testIntegrationSerialSuite) TestIssue17063(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec("create block t(a char, b char) defCauslate utf8mb4_general_ci;")
	tk.MustExec(`insert into t values('a', 'b');`)
	tk.MustExec(`insert into t values('a', 'B');`)
	tk.MustQuery(`select * from t where if(a='x', a, b) = 'b';`).Check(testkit.Events("a b", "a B"))
	tk.MustQuery(`select defCauslation(if(a='x', a, b)) from t;`).Check(testkit.Events("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery(`select coercibility(if(a='x', a, b)) from t;`).Check(testkit.Events("2", "2"))
	tk.MustQuery(`select defCauslation(lag(b, 1, 'B') over w) from t window w as (order by b);`).Check(testkit.Events("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery(`select coercibility(lag(b, 1, 'B') over w) from t window w as (order by b);`).Check(testkit.Events("2", "2"))
}

func (s *testIntegrationSuite) TestIssue19504(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1;")
	tk.MustExec("create block t1 (c_int int, primary key (c_int));")
	tk.MustExec("insert into t1 values (1), (2), (3);")
	tk.MustExec("drop block if exists t2;")
	tk.MustExec("create block t2 (c_int int, primary key (c_int));")
	tk.MustExec("insert into t2 values (1);")
	tk.MustQuery("select (select count(c_int) from t2 where c_int = t1.c_int) c1, (select count(1) from t2 where c_int = t1.c_int) c2 from t1;").
		Check(testkit.Events("1 1", "0 0", "0 0"))
}

func (s *testIntegrationSerialSuite) TestIssue19804(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a set('a', 'b', 'c'));`)
	tk.MustGetErrMsg("alter block t change a a set('a', 'b', 'c', 'c');", "[types:1291]DeferredCauset 'a' has duplicated value 'c' in SET")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a enum('a', 'b', 'c'));`)
	tk.MustGetErrMsg("alter block t change a a enum('a', 'b', 'c', 'c');", "[types:1291]DeferredCauset 'a' has duplicated value 'c' in ENUM")
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a set('a', 'b', 'c'));`)
	tk.MustExec(`alter block t change a a set('a', 'b', 'c', 'd');`)
	tk.MustGetErrMsg(`alter block t change a a set('a', 'b', 'c', 'e', 'f');`, "[dbs:8200]Unsupported modify defCausumn: cannot modify set defCausumn value d to e")
}

func (s *testIntegrationSerialSuite) TestIssue18949(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a enum('a ', 'b\t', ' c '), b set('a ', 'b\t', ' c '));`)
	result := tk.MustQuery("show create block t").Events()[0][1]
	c.Assert(result, Matches, `(?s).*enum\('a','b	',' c'\).*set\('a','b	',' c'\).*`)
	tk.MustExec(`alter block t change a aa enum('a   ', 'b\t', ' c ');`)
	result = tk.MustQuery("show create block t").Events()[0][1]
	c.Assert(result, Matches, `(?s).*enum\('a','b	',' c'\).*set\('a','b	',' c'\).*`)
}

func (s *testIntegrationSuite) TestIssue19596(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t (a int) partition by range(a) (PARTITION p0 VALUES LESS THAN (10));")
	tk.MustGetErrMsg("alter block t add partition (partition p1 values less than (a));", "[expression:1054]Unknown defCausumn 'a' in 'expression'")
	tk.MustQuery("select * from t;")
	tk.MustExec("drop block if exists t;")
	tk.MustGetErrMsg("create block t (a int) partition by range(a) (PARTITION p0 VALUES LESS THAN (a));", "[expression:1054]Unknown defCausumn 'a' in 'expression'")
}

func (s *testIntegrationSuite) TestIssue17476(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS `block_float`;")
	tk.MustExec("DROP TABLE IF EXISTS `block_int_float_varchar`;")
	tk.MustExec("CREATE TABLE `block_float` (`id_1` int(16) NOT NULL AUTO_INCREMENT,`defCaus_float_1` float DEFAULT NULL,PRIMARY KEY (`id_1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=97635;")
	tk.MustExec("CREATE TABLE `block_int_float_varchar` " +
		"(`id_6` int(16) NOT NULL AUTO_INCREMENT," +
		"`defCaus_int_6` int(16) DEFAULT NULL,`defCaus_float_6` float DEFAULT NULL," +
		"`defCaus_varchar_6` varchar(511) DEFAULT NULL,PRIMARY KEY (`id_6`)," +
		"KEY `vhyen` (`id_6`,`defCaus_int_6`,`defCaus_float_6`,`defCaus_varchar_6`(1))," +
		"KEY `zzylq` (`id_6`,`defCaus_int_6`,`defCaus_float_6`,`defCaus_varchar_6`(1))) " +
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=90818;")

	tk.MustExec("INSERT INTO `block_float` VALUES (1,NULL),(2,0.1),(3,0),(4,-0.1),(5,-0.1),(6,NULL),(7,0.5),(8,0),(9,0),(10,NULL),(11,1),(12,1.5),(13,NULL),(14,NULL);")
	tk.MustExec("INSERT INTO `block_int_float_varchar` VALUES (1,0,0.1,'true'),(2,-1,1.5,'2020-02-02 02:02:00'),(3,NULL,1.5,NULL),(4,65535,0.1,'true'),(5,NULL,0.1,'1'),(6,-1,1.5,'2020-02-02 02:02:00'),(7,-1,NULL,''),(8,NULL,-0.1,NULL),(9,NULL,-0.1,'1'),(10,-1,NULL,''),(11,NULL,1.5,'false'),(12,-1,0,NULL),(13,0,-0.1,NULL),(14,-1,NULL,'-0'),(15,65535,-1,'1'),(16,NULL,0.5,NULL),(17,-1,NULL,NULL);")
	tk.MustQuery(`select count(*) from block_float
 JOIN block_int_float_varchar AS tmp3 ON (tmp3.defCaus_varchar_6 AND NULL)
 IS NULL WHERE defCaus_int_6=0;`).Check(testkit.Events("14"))
	tk.MustQuery(`SELECT count(*) FROM (block_float JOIN block_int_float_varchar AS tmp3 ON (tmp3.defCaus_varchar_6 AND NULL) IS NULL);`).Check(testkit.Events("154"))
	tk.MustQuery(`SELECT * FROM (block_int_float_varchar AS tmp3) WHERE (defCaus_varchar_6 AND NULL) IS NULL AND defCaus_int_6=0;`).Check(testkit.Events("13 0 -0.1 <nil>"))
}
