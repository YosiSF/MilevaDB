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

package dbs_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	tmysql "github.com/whtcorpsinc/MilevaDB-Prod/errno"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/admin"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

func (s *testIntegrationSuite3) TestCreateBlockWithPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists tp;")
	tk.MustExec(`CREATE TABLE tp (a int) PARTITION BY RANGE(a) (
	PARTITION p0 VALUES LESS THAN (10),
	PARTITION p1 VALUES LESS THAN (20),
	PARTITION p2 VALUES LESS THAN (MAXVALUE)
	);`)
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().Partition, NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, perceptron.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`a`")
	for _, FIDelef := range part.Definitions {
		c.Assert(FIDelef.ID, Greater, int64(0))
	}
	c.Assert(part.Definitions, HasLen, 3)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "10")
	c.Assert(part.Definitions[0].Name.L, Equals, "p0")
	c.Assert(part.Definitions[1].LessThan[0], Equals, "20")
	c.Assert(part.Definitions[1].Name.L, Equals, "p1")
	c.Assert(part.Definitions[2].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[2].Name.L, Equals, "p2")

	tk.MustExec("drop block if exists employees;")
	sql1 := `create block employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p2 values less than (2001)
	);`
	tk.MustGetErrCode(sql1, tmysql.ErrSameNamePartition)

	sql2 := `create block employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`
	tk.MustGetErrCode(sql2, tmysql.ErrRangeNotIncreasing)

	sql3 := `create block employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than maxvalue,
		partition p3 values less than (2001)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrPartitionMaxvalue)

	sql4 := `create block t4 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than maxvalue,
		partition p2 values less than (1991),
		partition p3 values less than (1995)
	);`
	tk.MustGetErrCode(sql4, tmysql.ErrPartitionMaxvalue)

	_, err = tk.Exec(`CREATE TABLE rc (
		a INT NOT NULL,
		b INT NOT NULL,
		c INT NOT NULL
	)
	partition by range defCausumns(a,b,c) (
	partition p0 values less than (10,5,1),
	partition p2 values less than (50,maxvalue,10),
	partition p3 values less than (65,30,13),
	partition p4 values less than (maxvalue,30,40)
	);`)
	c.Assert(err, IsNil)

	sql6 := `create block employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		 partition p0 values less than (6 , 10)
	);`
	tk.MustGetErrCode(sql6, tmysql.ErrTooManyValues)

	sql7 := `create block t7 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than (1991),
		partition p2 values less than maxvalue,
		partition p3 values less than maxvalue,
		partition p4 values less than (1995),
		partition p5 values less than maxvalue
	);`
	tk.MustGetErrCode(sql7, tmysql.ErrPartitionMaxvalue)

	sql18 := `create block t8 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than (19xx91),
		partition p2 values less than maxvalue
	);`
	tk.MustGetErrCode(sql18, allegrosql.ErrBadField)

	sql9 := `create TABLE t9 (
	defCaus1 int
	)
	partition by range( case when defCaus1 > 0 then 10 else 20 end ) (
		partition p0 values less than (2),
		partition p1 values less than (6)
	);`
	tk.MustGetErrCode(sql9, tmysql.ErrPartitionFunctionIsNotAllowed)

	_, err = tk.Exec(`CREATE TABLE t9 (
		a INT NOT NULL,
		b INT NOT NULL,
		c INT NOT NULL
	)
	partition by range defCausumns(a) (
	partition p0 values less than (10),
	partition p2 values less than (20),
	partition p3 values less than (20)
	);`)
	c.Assert(dbs.ErrRangeNotIncreasing.Equal(err), IsTrue)

	tk.MustGetErrCode(`create TABLE t10 (c1 int,c2 int) partition by range(c1 / c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFunctionIsNotAllowed)

	tk.MustExec(`create TABLE t11 (c1 int,c2 int) partition by range(c1 div c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t12 (c1 int,c2 int) partition by range(c1 + c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t13 (c1 int,c2 int) partition by range(c1 - c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t14 (c1 int,c2 int) partition by range(c1 * c2 ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t15 (c1 int,c2 int) partition by range( abs(c1) ) (partition p0 values less than (2));`)
	tk.MustExec(`create TABLE t16 (c1 int) partition by range( c1) (partition p0 values less than (10));`)

	tk.MustGetErrCode(`create TABLE t17 (c1 int,c2 float) partition by range(c1 + c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	tk.MustGetErrCode(`create TABLE t18 (c1 int,c2 float) partition by range( floor(c2) ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	tk.MustExec(`create TABLE t19 (c1 int,c2 float) partition by range( floor(c1) ) (partition p0 values less than (2));`)

	tk.MustExec(`create TABLE t20 (c1 int,c2 bit(10)) partition by range(c2) (partition p0 values less than (10));`)
	tk.MustExec(`create TABLE t21 (c1 int,c2 year) partition by range( c2 ) (partition p0 values less than (2000));`)

	tk.MustGetErrCode(`create TABLE t24 (c1 float) partition by range( c1 ) (partition p0 values less than (2000));`, tmysql.ErrFieldTypeNotAllowedAsPartitionField)

	// test check order. The allegrosql below have 2 problem: 1. ErrFieldTypeNotAllowedAsPartitionField  2. ErrPartitionMaxvalue , allegrosql will return ErrPartitionMaxvalue.
	tk.MustGetErrCode(`create TABLE t25 (c1 float) partition by range( c1 ) (partition p1 values less than maxvalue,partition p0 values less than (2000));`, tmysql.ErrPartitionMaxvalue)

	// Fix issue 7362.
	tk.MustExec("create block test_partition(id bigint, name varchar(255), primary key(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8 PARTITION BY RANGE  COLUMNS(id) (PARTITION p1 VALUES LESS THAN (10) ENGINE = InnoDB);")

	// 'Less than' in partition expression could be a constant expression, notice that
	// the SHOW result changed.
	tk.MustExec(`create block t26 (a date)
			  partition by range(to_seconds(a))(
			  partition p0 values less than (to_seconds('2004-01-01')),
			  partition p1 values less than (to_seconds('2005-01-01')));`)
	tk.MustQuery("show create block t26").Check(
		testkit.Rows("t26 CREATE TABLE `t26` (\n  `a` date DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE ( TO_SECONDS(`a`) ) (\n  PARTITION `p0` VALUES LESS THAN (63240134400),\n  PARTITION `p1` VALUES LESS THAN (63271756800)\n)"))
	tk.MustExec(`create block t27 (a bigint unsigned not null)
		  partition by range(a) (
		  partition p0 values less than (10),
		  partition p1 values less than (100),
		  partition p2 values less than (1000),
		  partition p3 values less than (18446744073709551000),
		  partition p4 values less than (18446744073709551614)
		);`)
	tk.MustExec(`create block t28 (a bigint unsigned not null)
		  partition by range(a) (
		  partition p0 values less than (10),
		  partition p1 values less than (100),
		  partition p2 values less than (1000),
		  partition p3 values less than (18446744073709551000 + 1),
		  partition p4 values less than (18446744073709551000 + 10)
		);`)

	tk.MustExec("set @@milevadb_enable_block_partition = 1")
	tk.MustExec("set @@milevadb_enable_block_partition = 1")
	tk.MustExec(`create block t30 (
		  a int,
		  b float,
		  c varchar(30))
		  partition by range defCausumns (a, b)
		  (partition p0 values less than (10, 10.0))`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8200 Unsupported partition type, treat as normal block"))

	tk.MustGetErrCode(`create block t31 (a int not null) partition by range( a );`, tmysql.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create block t32 (a int not null) partition by range defCausumns( a );`, tmysql.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create block t33 (a int, b int) partition by hash(a) partitions 0;`, tmysql.ErrNoParts)
	tk.MustGetErrCode(`create block t33 (a timestamp, b int) partition by hash(a) partitions 30;`, tmysql.ErrFieldTypeNotAllowedAsPartitionField)
	tk.MustGetErrCode(`CREATE TABLE t34 (c0 INT) PARTITION BY HASH((CASE WHEN 0 THEN 0 ELSE c0 END )) PARTITIONS 1;`, tmysql.ErrPartitionFunctionIsNotAllowed)
	tk.MustGetErrCode(`CREATE TABLE t0(c0 INT) PARTITION BY HASH((c0<CURRENT_USER())) PARTITIONS 1;`, tmysql.ErrPartitionFunctionIsNotAllowed)
	// TODO: fix this one
	// tk.MustGetErrCode(`create block t33 (a timestamp, b int) partition by hash(unix_timestamp(a)) partitions 30;`, tmysql.ErrPartitionFuncNotAllowed)

	// Fix issue 8647
	tk.MustGetErrCode(`CREATE TABLE trb8 (
		id int(11) DEFAULT NULL,
		name varchar(50) DEFAULT NULL,
		purchased date DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
	PARTITION BY RANGE ( year(notexist.purchased) - 1 ) (
		PARTITION p0 VALUES LESS THAN (1990),
		PARTITION p1 VALUES LESS THAN (1995),
		PARTITION p2 VALUES LESS THAN (2000),
		PARTITION p3 VALUES LESS THAN (2005)
	);`, tmysql.ErrBadField)

	// Fix a timezone dependent check bug introduced in https://github.com/whtcorpsinc/MilevaDB-Prod/pull/10655
	tk.MustExec(`create block t34 (dt timestamp(3)) partition by range (floor(unix_timestamp(dt))) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`)

	tk.MustGetErrCode(`create block t34 (dt timestamp(3)) partition by range (unix_timestamp(date(dt))) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`, tmysql.ErrWrongExprInPartitionFunc)

	tk.MustGetErrCode(`create block t34 (dt datetime) partition by range (unix_timestamp(dt)) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`, tmysql.ErrWrongExprInPartitionFunc)

	// Fix https://github.com/whtcorpsinc/MilevaDB-Prod/issues/16333
	tk.MustExec(`create block t35 (dt timestamp) partition by range (unix_timestamp(dt))
(partition p0 values less than (unix_timestamp('2020-04-15 00:00:00')));`)

	tk.MustExec(`drop block if exists too_long_identifier`)
	tk.MustGetErrCode(`create block too_long_identifier(a int)
partition by range (a)
(partition p0pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp values less than (10));`, tmysql.ErrTooLongIdent)

	tk.MustExec(`drop block if exists too_long_identifier`)
	tk.MustExec("create block too_long_identifier(a int) partition by range(a) (partition p0 values less than(10))")
	tk.MustGetErrCode("alter block too_long_identifier add partition "+
		"(partition p0pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp values less than(20))", tmysql.ErrTooLongIdent)

	tk.MustExec(`create block t36 (a date, b datetime) partition by range (EXTRACT(YEAR_MONTH FROM a)) (
    partition p0 values less than (200),
    partition p1 values less than (300),
    partition p2 values less than maxvalue)`)
}

func (s *testIntegrationSuite2) TestCreateBlockWithHashPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists employees;")
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustExec(`
	create block employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash(store_id) partitions 4;`)

	tk.MustExec("drop block if exists employees;")
	tk.MustExec(`
	create block employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash( year(hired) ) partitions 4;`)

	// This query makes milevadb OOM without partition count check.
	tk.MustGetErrCode(`CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL DEFAULT '1970-01-01',
    separated DATE NOT NULL DEFAULT '9999-12-31',
    job_code INT,
    store_id INT
) PARTITION BY HASH(store_id) PARTITIONS 102400000000;`, tmysql.ErrTooManyPartitions)

	tk.MustExec("CREATE TABLE t_linear (a int, b varchar(128)) PARTITION BY LINEAR HASH(a) PARTITIONS 4")
	tk.MustGetErrCode("select * from t_linear partition (p0)", tmysql.ErrPartitionClauseOnNonpartitioned)

	tk.MustExec(`CREATE TABLE t_sub (a int, b varchar(128)) PARTITION BY RANGE( a ) SUBPARTITION BY HASH( a )
                                   SUBPARTITIONS 2 (
                                       PARTITION p0 VALUES LESS THAN (100),
                                       PARTITION p1 VALUES LESS THAN (200),
                                       PARTITION p2 VALUES LESS THAN MAXVALUE)`)
	tk.MustGetErrCode("select * from t_sub partition (p0)", tmysql.ErrPartitionClauseOnNonpartitioned)

	// Fix create partition block using extract() function as partition key.
	tk.MustExec("create block t2 (a date, b datetime) partition by hash (EXTRACT(YEAR_MONTH FROM a)) partitions 7")
}

func (s *testIntegrationSuite1) TestCreateBlockWithRangeDeferredCausetPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists log_message_1;")
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustExec(`
create block log_message_1 (
    add_time datetime not null default '2000-01-01 00:00:00',
    log_level int unsigned not null default '0',
    log_host varchar(32) not null,
    service_name varchar(32) not null,
    message varchar(2000)
) partition by range defCausumns(add_time)(
    partition p201403 values less than ('2020-04-01'),
    partition p201404 values less than ('2020-05-01'),
    partition p201405 values less than ('2020-06-01'),
    partition p201406 values less than ('2020-07-01'),
    partition p201407 values less than ('2020-08-01'),
    partition p201408 values less than ('2020-09-01'),
    partition p201409 values less than ('2020-10-01'),
    partition p201410 values less than ('2020-11-01')
)`)
	tk.MustExec("drop block if exists log_message_1;")
	tk.MustExec(`
	create block log_message_1 (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash( year(hired) ) partitions 4;`)

	tk.MustExec("drop block if exists t")

	type testCase struct {
		allegrosql string
		err        *terror.Error
	}

	cases := []testCase{
		{
			"create block t (id int) partition by range defCausumns (id);",
			ast.ErrPartitionsMustBeDefined,
		},
		{
			"create block t (id int) partition by range defCausumns (id) (partition p0 values less than (1, 2));",
			ast.ErrPartitionDeferredCausetList,
		},
		{
			"create block t (a int) partition by range defCausumns (b) (partition p0 values less than (1, 2));",
			ast.ErrPartitionDeferredCausetList,
		},
		{
			"create block t (a int) partition by range defCausumns (b) (partition p0 values less than (1));",
			dbs.ErrFieldNotFoundPart,
		},
		{
			"create block t (id timestamp) partition by range defCausumns (id) (partition p0 values less than ('2020-01-09 11:23:34'));",
			dbs.ErrNotAllowedTypeInPartition,
		},
		{
			`create block t29 (
				a decimal
			)
			partition by range defCausumns (a)
			(partition p0 values less than (0));`,
			dbs.ErrNotAllowedTypeInPartition,
		},
		{
			"create block t (id text) partition by range defCausumns (id) (partition p0 values less than ('abc'));",
			dbs.ErrNotAllowedTypeInPartition,
		},
		// create as normal block, warning.
		//	{
		//		"create block t (a int, b varchar(64)) partition by range defCausumns (a, b) (" +
		//			"partition p0 values less than (1, 'a')," +
		//			"partition p1 values less than (1, 'a'))",
		//		dbs.ErrRangeNotIncreasing,
		//	},
		{
			"create block t (a int, b varchar(64)) partition by range defCausumns ( b) (" +
				"partition p0 values less than ( 'a')," +
				"partition p1 values less than ('a'))",
			dbs.ErrRangeNotIncreasing,
		},
		// create as normal block, warning.
		//	{
		//		"create block t (a int, b varchar(64)) partition by range defCausumns (a, b) (" +
		//			"partition p0 values less than (1, 'b')," +
		//			"partition p1 values less than (1, 'a'))",
		//		dbs.ErrRangeNotIncreasing,
		//	},
		{
			"create block t (a int, b varchar(64)) partition by range defCausumns (b) (" +
				"partition p0 values less than ('b')," +
				"partition p1 values less than ('a'))",
			dbs.ErrRangeNotIncreasing,
		},
		// create as normal block, warning.
		//		{
		//			"create block t (a int, b varchar(64)) partition by range defCausumns (a, b) (" +
		//				"partition p0 values less than (1, maxvalue)," +
		//				"partition p1 values less than (1, 'a'))",
		//			dbs.ErrRangeNotIncreasing,
		//		},
		{
			"create block t (a int, b varchar(64)) partition by range defCausumns ( b) (" +
				"partition p0 values less than (  maxvalue)," +
				"partition p1 values less than ('a'))",
			dbs.ErrRangeNotIncreasing,
		},
		{
			"create block t (defCaus datetime not null default '2000-01-01')" +
				"partition by range defCausumns (defCaus) (" +
				"PARTITION p0 VALUES LESS THAN (20190905)," +
				"PARTITION p1 VALUES LESS THAN (20190906));",
			dbs.ErrWrongTypeDeferredCausetValue,
		},
	}
	for i, t := range cases {
		_, err := tk.Exec(t.allegrosql)
		c.Assert(t.err.Equal(err), IsTrue, Commentf(
			"case %d fail, allegrosql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, t.allegrosql, t.err, err,
		))
	}

	tk.MustExec("create block t1 (a int, b char(3)) partition by range defCausumns (a, b) (" +
		"partition p0 values less than (1, 'a')," +
		"partition p1 values less than (2, maxvalue))")

	tk.MustExec("create block t2 (a int, b char(3)) partition by range defCausumns (b) (" +
		"partition p0 values less than ( 'a')," +
		"partition p1 values less than (maxvalue))")
}

func (s *testIntegrationSuite3) TestCreateBlockWithKeyPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists tm1;")
	tk.MustExec(`create block tm1
	(
		s1 char(32) primary key
	)
	partition by key(s1) partitions 10;`)

	tk.MustExec(`drop block if exists tm2`)
	tk.MustExec(`create block tm2 (a char(5), unique key(a(5))) partition by key() partitions 5;`)
}

func (s *testIntegrationSuite5) TestAlterBlockAddPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists employees;")
	tk.MustExec(`create block employees (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	tk.MustExec(`alter block employees add partition (
    partition p4 values less than (2010),
    partition p5 values less than MAXVALUE
	);`)

	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("employees"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().Partition, NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, perceptron.PartitionTypeRange)

	c.Assert(part.Expr, Equals, "YEAR(`hired`)")
	c.Assert(part.Definitions, HasLen, 5)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "1991")
	c.Assert(part.Definitions[0].Name, Equals, perceptron.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "1996")
	c.Assert(part.Definitions[1].Name, Equals, perceptron.NewCIStr("p2"))
	c.Assert(part.Definitions[2].LessThan[0], Equals, "2001")
	c.Assert(part.Definitions[2].Name, Equals, perceptron.NewCIStr("p3"))
	c.Assert(part.Definitions[3].LessThan[0], Equals, "2010")
	c.Assert(part.Definitions[3].Name, Equals, perceptron.NewCIStr("p4"))
	c.Assert(part.Definitions[4].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[4].Name, Equals, perceptron.NewCIStr("p5"))

	tk.MustExec("drop block if exists block1;")
	tk.MustExec("create block block1(a int)")
	sql1 := `alter block block1 add partition (
		partition p1 values less than (2010),
		partition p2 values less than maxvalue
	);`
	tk.MustGetErrCode(sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)
	tk.MustExec(`create block block_MustBeDefined (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	sql2 := "alter block block_MustBeDefined add partition"
	tk.MustGetErrCode(sql2, tmysql.ErrPartitionsMustBeDefined)
	tk.MustExec("drop block if exists block2;")
	tk.MustExec(`create block block2 (

	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p2 values less than maxvalue
	);`)

	sql3 := `alter block block2 add partition (
		partition p3 values less than (2010)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrPartitionMaxvalue)

	tk.MustExec("drop block if exists block3;")
	tk.MustExec(`create block block3 (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p2 values less than (2001)
	);`)

	sql4 := `alter block block3 add partition (
		partition p3 values less than (1993)
	);`
	tk.MustGetErrCode(sql4, tmysql.ErrRangeNotIncreasing)

	sql5 := `alter block block3 add partition (
		partition p1 values less than (1993)
	);`
	tk.MustGetErrCode(sql5, tmysql.ErrSameNamePartition)

	sql6 := `alter block block3 add partition (
		partition p1 values less than (1993),
		partition p1 values less than (1995)
	);`
	tk.MustGetErrCode(sql6, tmysql.ErrSameNamePartition)

	sql7 := `alter block block3 add partition (
		partition p4 values less than (1993),
		partition p1 values less than (1995),
		partition p5 values less than maxvalue
	);`
	tk.MustGetErrCode(sql7, tmysql.ErrSameNamePartition)

	sql8 := "alter block block3 add partition (partition p6);"
	tk.MustGetErrCode(sql8, tmysql.ErrPartitionRequiresValues)

	sql9 := "alter block block3 add partition (partition p7 values in (2020));"
	tk.MustGetErrCode(sql9, tmysql.ErrPartitionWrongValues)

	sql10 := "alter block block3 add partition partitions 4;"
	tk.MustGetErrCode(sql10, tmysql.ErrPartitionsMustBeDefined)

	tk.MustExec("alter block block3 add partition (partition p3 values less than (2001 + 10))")

	// less than value can be negative or expression.
	tk.MustExec(`CREATE TABLE tt5 (
		c3 bigint(20) NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
	PARTITION BY RANGE ( c3 ) (
		PARTITION p0 VALUES LESS THAN (-3),
		PARTITION p1 VALUES LESS THAN (-2)
	);`)
	tk.MustExec(`ALTER TABLE tt5 add partition ( partition p2 values less than (-1) );`)
	tk.MustExec(`ALTER TABLE tt5 add partition ( partition p3 values less than (5-1) );`)

	// Test add partition for the block partition by range defCausumns.
	tk.MustExec("drop block if exists t;")
	tk.MustExec("create block t (a datetime) partition by range defCausumns (a) (partition p1 values less than ('2020-06-01'), partition p2 values less than ('2020-07-01'));")
	allegrosql := "alter block t add partition ( partition p3 values less than ('2020-07-01'));"
	tk.MustGetErrCode(allegrosql, tmysql.ErrRangeNotIncreasing)
	tk.MustExec("alter block t add partition ( partition p3 values less than ('2020-08-01'));")

	// Add partition value's type should be the same with the defCausumn's type.
	tk.MustExec("drop block if exists t;")
	tk.MustExec(`create block t (
		defCaus date not null default '2000-01-01')
                partition by range defCausumns (defCaus) (
		PARTITION p0 VALUES LESS THAN ('20190905'),
		PARTITION p1 VALUES LESS THAN ('20190906'));`)
	allegrosql = "alter block t add partition (partition p2 values less than (20190907));"
	tk.MustGetErrCode(allegrosql, tmysql.ErrWrongTypeDeferredCausetValue)
}

func (s *testIntegrationSuite5) TestAlterBlockDropPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists employees")
	tk.MustExec(`create block employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)

	tk.MustExec("alter block employees drop partition p3;")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("employees"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().GetPartitionInfo(), NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, perceptron.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`hired`")
	c.Assert(part.Definitions, HasLen, 2)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "1991")
	c.Assert(part.Definitions[0].Name, Equals, perceptron.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "1996")
	c.Assert(part.Definitions[1].Name, Equals, perceptron.NewCIStr("p2"))

	tk.MustExec("drop block if exists block1;")
	tk.MustExec("create block block1 (a int);")
	sql1 := "alter block block1 drop partition p10;"
	tk.MustGetErrCode(sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)

	tk.MustExec("drop block if exists block2;")
	tk.MustExec(`create block block2 (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	sql2 := "alter block block2 drop partition p10;"
	tk.MustGetErrCode(sql2, tmysql.ErrDropPartitionNonExistent)

	tk.MustExec("drop block if exists block3;")
	tk.MustExec(`create block block3 (
	id int not null
	)
	partition by range( id ) (
		partition p1 values less than (1991)
	);`)
	sql3 := "alter block block3 drop partition p1;"
	tk.MustGetErrCode(sql3, tmysql.ErrDropLastPartition)

	tk.MustExec("drop block if exists block4;")
	tk.MustExec(`create block block4 (
	id int not null
	)
	partition by range( id ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than MAXVALUE
	);`)

	tk.MustExec("alter block block4 drop partition p2;")
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("block4"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().GetPartitionInfo(), NotNil)
	part = tbl.Meta().Partition
	c.Assert(part.Type, Equals, perceptron.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`id`")
	c.Assert(part.Definitions, HasLen, 2)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "10")
	c.Assert(part.Definitions[0].Name, Equals, perceptron.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[1].Name, Equals, perceptron.NewCIStr("p3"))

	tk.MustExec("drop block if exists tr;")
	tk.MustExec(` create block tr(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2020)
   	);`)
	tk.MustExec(`INSERT INTO tr VALUES
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2020-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result := tk.MustQuery("select * from tr where purchased between '1995-01-01' and '1999-12-31';")
	result.Check(testkit.Rows(`2 alarm clock 1997-11-05`, `10 lava lamp 1998-12-25`))
	tk.MustExec("alter block tr drop partition p2;")
	result = tk.MustQuery("select * from tr where purchased between '1995-01-01' and '1999-12-31';")
	result.Check(testkit.Rows())

	result = tk.MustQuery("select * from tr where purchased between '2010-01-01' and '2020-12-31';")
	result.Check(testkit.Rows(`5 exercise bike 2020-05-09`, `7 espresso maker 2011-11-22`))
	tk.MustExec("alter block tr drop partition p5;")
	result = tk.MustQuery("select * from tr where purchased between '2010-01-01' and '2020-12-31';")
	result.Check(testkit.Rows())

	tk.MustExec("alter block tr drop partition p4;")
	result = tk.MustQuery("select * from tr where purchased between '2005-01-01' and '2009-12-31';")
	result.Check(testkit.Rows())

	tk.MustExec("drop block if exists block4;")
	tk.MustExec(`create block block4 (
		id int not null
	)
	partition by range( id ) (
		partition Par1 values less than (1991),
		partition pAR2 values less than (1992),
		partition Par3 values less than (1995),
		partition PaR5 values less than (1996)
	);`)
	tk.MustExec("alter block block4 drop partition Par2;")
	tk.MustExec("alter block block4 drop partition PAR5;")
	sql4 := "alter block block4 drop partition PAR0;"
	tk.MustGetErrCode(sql4, tmysql.ErrDropPartitionNonExistent)

	tk.MustExec("CREATE TABLE t1 (a int(11), b varchar(64)) PARTITION BY HASH(a) PARTITIONS 3")
	tk.MustGetErrCode("alter block t1 drop partition p2", tmysql.ErrOnlyOnRangeListPartition)
}

func (s *testIntegrationSuite5) TestMultiPartitionDropAndTruncate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists employees")
	tk.MustExec(`create block employees (
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001),
		partition p4 values less than (2006),
		partition p5 values less than (2011)
	);`)
	tk.MustExec(`INSERT INTO employees VALUES (1990), (1995), (2000), (2005), (2010)`)

	tk.MustExec("alter block employees drop partition p1, p2;")
	result := tk.MustQuery("select * from employees;")
	result.Sort().Check(testkit.Rows(`2000`, `2005`, `2010`))

	tk.MustExec("alter block employees truncate partition p3, p4")
	result = tk.MustQuery("select * from employees;")
	result.Check(testkit.Rows(`2010`))
}

func (s *testIntegrationSuite7) TestAlterBlockExchangePartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists e")
	tk.MustExec("drop block if exists e2")
	tk.MustExec(`CREATE TABLE e (
		id INT NOT NULL
	)
    PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (50),
        PARTITION p1 VALUES LESS THAN (100),
        PARTITION p2 VALUES LESS THAN (150),
        PARTITION p3 VALUES LESS THAN (MAXVALUE)
	);`)
	tk.MustExec(`CREATE TABLE e2 (
		id INT NOT NULL
	);`)
	tk.MustExec(`INSERT INTO e VALUES (1669),(337),(16),(2005)`)
	tk.MustExec("ALTER TABLE e EXCHANGE PARTITION p0 WITH TABLE e2")
	tk.MustQuery("select * from e2").Check(testkit.Rows("16"))
	tk.MustQuery("select * from e").Check(testkit.Rows("1669", "337", "2005"))
	// validation test for range partition
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p2 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p3 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)

	tk.MustExec("drop block if exists e3")

	tk.MustExec(`CREATE TABLE e3 (
		id int not null
	) PARTITION BY HASH (id)
	PARTITIONS 4;`)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e3;", tmysql.ErrPartitionExchangePartBlock)
	tk.MustExec("truncate block e2")
	tk.MustExec(`INSERT INTO e3 VALUES (1),(5)`)

	tk.MustExec("ALTER TABLE e3 EXCHANGE PARTITION p1 WITH TABLE e2;")
	tk.MustQuery("select * from e3 partition(p0)").Check(testkit.Rows())
	tk.MustQuery("select * from e2").Check(testkit.Rows("1", "5"))

	// validation test for hash partition
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p0 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p2 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p3 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)

	// without validation test
	tk.MustExec("ALTER TABLE e3 EXCHANGE PARTITION p0 with TABLE e2 WITHOUT VALIDATION")

	tk.MustQuery("select * from e3 partition(p0)").Check(testkit.Rows("1", "5"))
	tk.MustQuery("select * from e2").Check(testkit.Rows())

	// more boundary test of range partition
	// for partition p0
	tk.MustExec(`create block e4 (a int) partition by range(a) (
		partition p0 values less than (3),
		partition p1 values less than (6),
        PARTITION p2 VALUES LESS THAN (9),
        PARTITION p3 VALUES LESS THAN (MAXVALUE)
		);`)
	tk.MustExec(`create block e5(a int);`)

	tk.MustExec("insert into e5 values (1)")

	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p2 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p0 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p0)").Check(testkit.Rows("1"))

	// for partition p1
	tk.MustExec("insert into e5 values (3)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p2 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p1 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p1)").Check(testkit.Rows("3"))

	// for partition p2
	tk.MustExec("insert into e5 values (6)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p2 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p2)").Check(testkit.Rows("6"))

	// for partition p3
	tk.MustExec("insert into e5 values (9)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("alter block e4 exchange partition p2 with block e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustExec("ALTER TABLE e4 EXCHANGE PARTITION p3 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p3)").Check(testkit.Rows("9"))

	// for defCausumns range partition
	tk.MustExec(`create block e6 (a varchar(3)) partition by range defCausumns (a) (
		partition p0 values less than ('3'),
		partition p1 values less than ('6')
	);`)
	tk.MustExec(`create block e7 (a varchar(3));`)
	tk.MustExec(`insert into e6 values ('1');`)
	tk.MustExec(`insert into e7 values ('2');`)
	tk.MustExec("alter block e6 exchange partition p0 with block e7")

	tk.MustQuery("select * from e6 partition(p0)").Check(testkit.Rows("2"))
	tk.MustQuery("select * from e7").Check(testkit.Rows("1"))
	tk.MustGetErrCode("alter block e6 exchange partition p1 with block e7", tmysql.ErrRowDoesNotMatchPartition)

	// test exchange partition from different databases
	tk.MustExec("create block e8 (a int) partition by hash(a) partitions 2;")
	tk.MustExec("create database if not exists exchange_partition")
	tk.MustExec("insert into e8 values (1), (3), (5)")
	tk.MustExec("use exchange_partition;")
	tk.MustExec("create block e9 (a int);")
	tk.MustExec("insert into e9 values (7), (9)")
	tk.MustExec("alter block test.e8 exchange partition p1 with block e9")

	tk.MustExec("insert into e9 values (11)")
	tk.MustQuery("select * from e9").Check(testkit.Rows("1", "3", "5", "11"))
	tk.MustExec("insert into test.e8 values (11)")
	tk.MustQuery("select * from test.e8").Check(testkit.Rows("7", "9", "11"))

	tk.MustExec("use test")
	tk.MustExec("create block e10 (a int) partition by hash(a) partitions 2")
	tk.MustExec("insert into e10 values (0), (2), (4)")
	tk.MustExec("create block e11 (a int)")
	tk.MustExec("insert into e11 values (1), (3)")
	tk.MustExec("alter block e10 exchange partition p1 with block e11")
	tk.MustExec("insert into e11 values (5)")
	tk.MustQuery("select * from e11").Check(testkit.Rows("5"))
	tk.MustExec("insert into e10 values (5), (6)")
	tk.MustQuery("select * from e10 partition(p0)").Check(testkit.Rows("0", "2", "4", "6"))
	tk.MustQuery("select * from e10 partition(p1)").Check(testkit.Rows("1", "3", "5"))

	// test for defCausumn id
	tk.MustExec("create block e12 (a int(1), b int, index (a)) partition by hash(a) partitions 3")
	tk.MustExec("create block e13 (a int(8), b int, index (a));")
	tk.MustExec("alter block e13 drop defCausumn b")
	tk.MustExec("alter block e13 add defCausumn b int")
	tk.MustGetErrCode("alter block e12 exchange partition p0 with block e13", tmysql.ErrPartitionExchangeDifferentOption)
	// test for index id
	tk.MustExec("create block e14 (a int, b int, index(a));")
	tk.MustExec("alter block e12 drop index a")
	tk.MustExec("alter block e12 add index (a);")
	tk.MustGetErrCode("alter block e12 exchange partition p0 with block e14", tmysql.ErrPartitionExchangeDifferentOption)

	// test for tiflash replica
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant/mockTiFlashStoreCount")

	tk.MustExec("create block e15 (a int) partition by hash(a) partitions 1;")
	tk.MustExec("create block e16 (a int)")
	tk.MustExec("alter block e15 set tiflash replica 1;")
	tk.MustExec("alter block e16 set tiflash replica 2;")

	e15 := testGetBlockByName(c, s.ctx, "test", "e15")
	partition := e15.Meta().Partition

	err := petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)

	e16 := testGetBlockByName(c, s.ctx, "test", "e16")
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, e16.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustGetErrCode("alter block e15 exchange partition p0 with block e16", tmysql.ErrBlocksDifferentMetadata)
	tk.MustExec("drop block e15, e16")

	tk.MustExec("create block e15 (a int) partition by hash(a) partitions 1;")
	tk.MustExec("create block e16 (a int)")
	tk.MustExec("alter block e15 set tiflash replica 1;")
	tk.MustExec("alter block e16 set tiflash replica 1;")

	e15 = testGetBlockByName(c, s.ctx, "test", "e15")
	partition = e15.Meta().Partition

	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)

	e16 = testGetBlockByName(c, s.ctx, "test", "e16")
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, e16.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("alter block e15 exchange partition p0 with block e16")

	e15 = testGetBlockByName(c, s.ctx, "test", "e15")

	partition = e15.Meta().Partition

	c.Assert(e15.Meta().TiFlashReplica, NotNil)
	c.Assert(e15.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(e15.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID})

	e16 = testGetBlockByName(c, s.ctx, "test", "e16")
	c.Assert(e16.Meta().TiFlashReplica, NotNil)
	c.Assert(e16.Meta().TiFlashReplica.Available, IsTrue)

	tk.MustExec("drop block e15, e16")
	tk.MustExec("create block e15 (a int) partition by hash(a) partitions 1;")
	tk.MustExec("create block e16 (a int)")
	tk.MustExec("alter block e16 set tiflash replica 1;")

	tk.MustExec("alter block e15 set tiflash replica 1 location labels 'a', 'b';")

	tk.MustGetErrCode("alter block e15 exchange partition p0 with block e16", tmysql.ErrBlocksDifferentMetadata)

	tk.MustExec("alter block e16 set tiflash replica 1 location labels 'a', 'b';")

	e15 = testGetBlockByName(c, s.ctx, "test", "e15")
	partition = e15.Meta().Partition

	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)

	e16 = testGetBlockByName(c, s.ctx, "test", "e16")
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, e16.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustExec("alter block e15 exchange partition p0 with block e16")
}

func (s *testIntegrationSuite4) TestExchangePartitionBlockCompatiable(c *C) {
	type testCase struct {
		ptALLEGROSQL       string
		ntALLEGROSQL       string
		exchangeALLEGROSQL string
		err                *terror.Error
	}
	cases := []testCase{
		{
			"create block pt (id int not null) partition by hash (id) partitions 4;",
			"create block nt (id int(1) not null);",
			"alter block pt exchange partition p0 with block nt;",
			nil,
		},
		{
			"create block pt1 (id int not null, fname varchar(3)) partition by hash (id) partitions 4;",
			"create block nt1 (id int not null, fname varchar(4));",
			"alter block pt1 exchange partition p0 with block nt1;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt2 (id int not null, salary decimal) partition by hash(id) partitions 4;",
			"create block nt2 (id int not null, salary decimal(3,2));",
			"alter block pt2 exchange partition p0 with block nt2;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt3 (id int not null, salary decimal) partition by hash(id) partitions 1;",
			"create block nt3 (id int not null, salary decimal(10, 1));",
			"alter block pt3 exchange partition p0 with block nt3",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt4 (id int not null) partition by hash(id) partitions 1;",
			"create block nt4 (id1 int not null);",
			"alter block pt4 exchange partition p0 with block nt4;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt5 (id int not null, primary key (id)) partition by hash(id) partitions 1;",
			"create block nt5 (id int not null);",
			"alter block pt5 exchange partition p0 with block nt5;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt6 (id int not null, salary decimal, index idx (id, salary)) partition by hash(id) partitions 1;",
			"create block nt6 (id int not null, salary decimal, index idx (salary, id));",
			"alter block pt6 exchange partition p0 with block nt6;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt7 (id int not null, index idx (id) invisible) partition by hash(id) partitions 1;",
			"create block nt7 (id int not null, index idx (id));",
			"alter block pt7 exchange partition p0 with block nt7;",
			nil,
		},
		{
			"create block pt8 (id int not null, index idx (id)) partition by hash(id) partitions 1;",
			"create block nt8 (id int not null, index id_idx (id));",
			"alter block pt8 exchange partition p0 with block nt8;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// foreign key test
			// Partition block doesn't support to add foreign keys in allegrosql
			"create block pt9 (id int not null primary key auto_increment,t_id int not null) partition by hash(id) partitions 1;",
			"create block nt9 (id int not null primary key auto_increment, t_id int not null,foreign key fk_id (t_id) references pt5(id));",
			"alter block pt9 exchange partition p0 with block nt9;",
			dbs.ErrPartitionExchangeForeignKey,
		},
		{
			// Generated defCausumn (virtual)
			"create block pt10 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname,' ')) virtual) partition by hash(id) partitions 1;",
			"create block nt10 (id int not null, lname varchar(30), fname varchar(100));",
			"alter block pt10 exchange partition p0 with block nt10;",
			dbs.ErrUnsupportedOnGeneratedDeferredCauset,
		},
		{
			"create block pt11 (id int not null, lname varchar(30), fname varchar(100)) partition by hash(id) partitions 1;",
			"create block nt11 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter block pt11 exchange partition p0 with block nt11;",
			dbs.ErrUnsupportedOnGeneratedDeferredCauset,
		},
		{

			"create block pt12 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname,' ')) stored) partition by hash(id) partitions 1;",
			"create block nt12 (id int not null, lname varchar(30), fname varchar(100));",
			"alter block pt12 exchange partition p0 with block nt12;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt13 (id int not null, lname varchar(30), fname varchar(100)) partition by hash(id) partitions 1;",
			"create block nt13 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) stored);",
			"alter block pt13 exchange partition p0 with block nt13;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt14 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual) partition by hash(id) partitions 1;",
			"create block nt14 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter block pt14 exchange partition p0 with block nt14;",
			nil,
		},
		{
			// unique index
			"create block pt15 (id int not null, unique index uk_id (id)) partition by hash(id) partitions 1;",
			"create block nt15 (id int not null, index uk_id (id));",
			"alter block pt15 exchange partition p0 with block nt15",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// auto_increment
			"create block pt16 (id int not null primary key auto_increment) partition by hash(id) partitions 1;",
			"create block nt16 (id int not null primary key);",
			"alter block pt16 exchange partition p0 with block nt16;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// default
			"create block pt17 (id int not null default 1) partition by hash(id) partitions 1;",
			"create block nt17 (id int not null);",
			"alter block pt17 exchange partition p0 with block nt17;",
			nil,
		},
		{
			// view test
			"create block pt18 (id int not null) partition by hash(id) partitions 1;",
			"create view nt18 as select id from nt17;",
			"alter block pt18 exchange partition p0 with block nt18",
			dbs.ErrCheckNoSuchBlock,
		},
		{
			"create block pt19 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) stored) partition by hash(id) partitions 1;",
			"create block nt19 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter block pt19 exchange partition p0 with block nt19;",
			dbs.ErrUnsupportedOnGeneratedDeferredCauset,
		},
		{
			"create block pt20 (id int not null) partition by hash(id) partitions 1;",
			"create block nt20 (id int default null);",
			"alter block pt20 exchange partition p0 with block nt20;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// unsigned
			"create block pt21 (id int unsigned) partition by hash(id) partitions 1;",
			"create block nt21 (id int);",
			"alter block pt21 exchange partition p0 with block nt21;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// zerofill
			"create block pt22 (id int) partition by hash(id) partitions 1;",
			"create block nt22 (id int zerofill);",
			"alter block pt22 exchange partition p0 with block nt22;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt23 (id int, lname varchar(10) charset binary) partition by hash(id) partitions 1;",
			"create block nt23 (id int, lname varchar(10));",
			"alter block pt23 exchange partition p0 with block nt23;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt25 (id int, a datetime on uFIDelate current_timestamp) partition by hash(id) partitions 1;",
			"create block nt25 (id int, a datetime);",
			"alter block pt25 exchange partition p0 with block nt25;",
			nil,
		},
		{
			"create block pt26 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual) partition by hash(id) partitions 1;",
			"create block nt26 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(id, ' ')) virtual);",
			"alter block pt26 exchange partition p0 with block nt26;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create block pt27 (a int key, b int, index(a)) partition by hash(a) partitions 1;",
			"create block nt27 (a int not null, b int, index(a));",
			"alter block pt27 exchange partition p0 with block nt27;",
			dbs.ErrBlocksDifferentMetadata,
		},
	}

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	for i, t := range cases {
		tk.MustExec(t.ptALLEGROSQL)
		tk.MustExec(t.ntALLEGROSQL)
		if t.err != nil {
			_, err := tk.Exec(t.exchangeALLEGROSQL)
			c.Assert(terror.ErrorEqual(err, t.err), IsTrue, Commentf(
				"case %d fail, allegrosql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
				i, t.exchangeALLEGROSQL, t.err, err,
			))
		} else {
			tk.MustExec(t.exchangeALLEGROSQL)
		}
	}
}

func (s *testIntegrationSuite7) TestExchangePartitionExpressIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists pt1;")
	tk.MustExec("create block pt1(a int, b int, c int) PARTITION BY hash (a) partitions 1;")
	tk.MustExec("alter block pt1 add index idx((a+c));")

	tk.MustExec("drop block if exists nt1;")
	tk.MustExec("create block nt1(a int, b int, c int);")
	tk.MustGetErrCode("alter block pt1 exchange partition p0 with block nt1;", tmysql.ErrBlocksDifferentMetadata)

	tk.MustExec("alter block nt1 add defCausumn (`_V$_idx_0` bigint(20) generated always as (a+b) virtual);")
	tk.MustGetErrCode("alter block pt1 exchange partition p0 with block nt1;", tmysql.ErrBlocksDifferentMetadata)

	// test different expression index when expression returns same field type
	tk.MustExec("alter block nt1 drop defCausumn `_V$_idx_0`;")
	tk.MustExec("alter block nt1 add index idx((b-c));")
	tk.MustGetErrCode("alter block pt1 exchange partition p0 with block nt1;", tmysql.ErrBlocksDifferentMetadata)

	// test different expression index when expression returns different field type
	tk.MustExec("alter block nt1 drop index idx;")
	tk.MustExec("alter block nt1 add index idx((concat(a, b)));")
	tk.MustGetErrCode("alter block pt1 exchange partition p0 with block nt1;", tmysql.ErrBlocksDifferentMetadata)

	tk.MustExec("drop block if exists nt2;")
	tk.MustExec("create block nt2 (a int, b int, c int)")
	tk.MustExec("alter block nt2 add index idx((a+c))")
	tk.MustExec("alter block pt1 exchange partition p0 with block nt2")

}

func (s *testIntegrationSuite4) TestAddPartitionTooManyPartitions(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	count := dbs.PartitionCountLimit
	tk.MustExec("drop block if exists p1;")
	sql1 := `create block p1 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i <= count; i++ {
		sql1 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql1 += "partition p8193 values less than (8193) );"
	tk.MustGetErrCode(sql1, tmysql.ErrTooManyPartitions)

	tk.MustExec("drop block if exists p2;")
	sql2 := `create block p2 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i < count; i++ {
		sql2 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql2 += "partition p8192 values less than (8192) );"

	tk.MustExec(sql2)
	sql3 := `alter block p2 add partition (
	partition p8193 values less than (8193)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrTooManyPartitions)
}

func checkPartitionDelRangeDone(c *C, s *testIntegrationSuite, partitionPrefix ekv.Key) bool {
	hasOldPartitionData := true
	for i := 0; i < waitForCleanDataRound; i++ {
		err := ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
			it, err := txn.Iter(partitionPrefix, nil)
			if err != nil {
				return err
			}
			if !it.Valid() {
				hasOldPartitionData = false
			} else {
				hasOldPartitionData = it.Key().HasPrefix(partitionPrefix)
			}
			it.Close()
			return nil
		})
		c.Assert(err, IsNil)
		if !hasOldPartitionData {
			break
		}
		time.Sleep(waitForCleanDataInterval)
	}
	return hasOldPartitionData
}

func (s *testIntegrationSuite4) TestTruncatePartitionAndDropBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	// Test truncate common block.
	tk.MustExec("drop block if exists t1;")
	tk.MustExec("create block t1 (id int(11));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t1 values (?)", i)
	}
	result := tk.MustQuery("select count(*) from t1;")
	result.Check(testkit.Rows("100"))
	tk.MustExec("truncate block t1;")
	result = tk.MustQuery("select count(*) from t1")
	result.Check(testkit.Rows("0"))

	// Test drop common block.
	tk.MustExec("drop block if exists t2;")
	tk.MustExec("create block t2 (id int(11));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t2 values (?)", i)
	}
	result = tk.MustQuery("select count(*) from t2;")
	result.Check(testkit.Rows("100"))
	tk.MustExec("drop block t2;")
	tk.MustGetErrCode("select * from t2;", tmysql.ErrNoSuchBlock)

	// Test truncate block partition.
	tk.MustExec("drop block if exists t3;")
	tk.MustExec(`create block t3(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2020)
   	);`)
	tk.MustExec(`insert into t3 values
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2020-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result = tk.MustQuery("select count(*) from t3;")
	result.Check(testkit.Rows("10"))
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t3"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	oldPID := oldTblInfo.Meta().Partition.Definitions[0].ID
	tk.MustExec("truncate block t3;")
	partitionPrefix := blockcodec.EncodeBlockPrefix(oldPID)
	hasOldPartitionData := checkPartitionDelRangeDone(c, s.testIntegrationSuite, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)

	// Test drop block partition.
	tk.MustExec("drop block if exists t4;")
	tk.MustExec(`create block t4(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2020)
   	);`)
	tk.MustExec(`insert into t4 values
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2020-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result = tk.MustQuery("select count(*) from t4; ")
	result.Check(testkit.Rows("10"))
	is = petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t4"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	oldPID = oldTblInfo.Meta().Partition.Definitions[1].ID
	tk.MustExec("drop block t4;")
	partitionPrefix = blockcodec.EncodeBlockPrefix(oldPID)
	hasOldPartitionData = checkPartitionDelRangeDone(c, s.testIntegrationSuite, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)
	tk.MustGetErrCode("select * from t4;", tmysql.ErrNoSuchBlock)

	// Test truncate block partition reassigns new partitionIDs.
	tk.MustExec("drop block if exists t5;")
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition=1;")
	tk.MustExec(`create block t5(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2020)
   	);`)
	is = petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t5"))
	c.Assert(err, IsNil)
	oldPID = oldTblInfo.Meta().Partition.Definitions[0].ID

	tk.MustExec("truncate block t5;")
	is = petri.GetPetri(ctx).SchemaReplicant()
	newTblInfo, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t5"))
	c.Assert(err, IsNil)
	newPID := newTblInfo.Meta().Partition.Definitions[0].ID
	c.Assert(oldPID != newPID, IsTrue)

	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = 1;")
	tk.MustExec("drop block if exists clients;")
	tk.MustExec(`create block clients (
		id int,
		fname varchar(30),
		lname varchar(30),
		signed date
	)
	partition by hash( month(signed) )
	partitions 12;`)
	is = petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("clients"))
	c.Assert(err, IsNil)
	oldDefs := oldTblInfo.Meta().Partition.Definitions

	// Test truncate `hash partitioned block` reassigns new partitionIDs.
	tk.MustExec("truncate block clients;")
	is = petri.GetPetri(ctx).SchemaReplicant()
	newTblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("clients"))
	c.Assert(err, IsNil)
	newDefs := newTblInfo.Meta().Partition.Definitions
	for i := 0; i < len(oldDefs); i++ {
		c.Assert(oldDefs[i].ID != newDefs[i].ID, IsTrue)
	}
}

func (s *testIntegrationSuite5) TestPartitionUniqueKeyNeedAllFieldsInPf(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists part1;")
	tk.MustExec(`create block part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1, defCaus2)
	)
	partition by range( defCaus1 + defCaus2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop block if exists part2;")
	tk.MustExec(`create block part2 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1, defCaus2, defCaus3),
		unique key (defCaus3)
	)
	partition by range( defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop block if exists part3;")
	tk.MustExec(`create block part3 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus2)
	)
	partition by range( defCaus1 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop block if exists part4;")
	tk.MustExec(`create block part4 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus2),
		unique key(defCaus2)
	)
	partition by range( year(defCaus2)  ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop block if exists part5;")
	tk.MustExec(`create block part5 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus2, defCaus4),
		unique key(defCaus2, defCaus1)
	)
	partition by range( defCaus1 + defCaus2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustExec("drop block if exists Part1;")
	sql1 := `create block Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1, defCaus2)
	)
	partition by range( defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql1, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop block if exists Part1;")
	sql2 := `create block Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1),
		unique key (defCaus3)
	)
	partition by range( defCaus1 + defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql2, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop block if exists Part1;")
	sql3 := `create block Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1),
		unique key (defCaus3)
	)
	partition by range( defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop block if exists Part1;")
	sql4 := `create block Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1, defCaus2, defCaus3),
		unique key (defCaus3)
	)
	partition by range( defCaus1 + defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql4, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop block if exists Part1;")
	sql5 := `create block Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus2)
	)
	partition by range( defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql5, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop block if exists Part1;")
	sql6 := `create block Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus3),
		unique key(defCaus2)
	)
	partition by range( year(defCaus2)  ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql6, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop block if exists Part1;")
	sql7 := `create block Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus3, defCaus4),
		unique key(defCaus2, defCaus1)
	)
	partition by range( defCaus1 + defCaus2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql7, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustExec("drop block if exists part6;")
	sql8 := `create block part6 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		defCaus5 int not null,
		unique key(defCaus1, defCaus2),
		unique key(defCaus1, defCaus3)
	)
	partition by range( defCaus1 + defCaus2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql8, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql9 := `create block part7 (
		defCaus1 int not null,
		defCaus2 int not null,
		defCaus3 int not null unique,
		unique key(defCaus1, defCaus2)
	)
	partition by range (defCaus1 + defCaus2) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	)`
	tk.MustGetErrCode(sql9, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql10 := `create block part8 (
                 a int not null,
                 b int not null,
                 c int default null,
                 d int default null,
                 e int default null,
                 primary key (a, b),
                 unique key (c, d)
        )
        partition by range defCausumns (b) (
               partition p0 values less than (4),
               partition p1 values less than (7),
               partition p2 values less than (11)
        )`
	tk.MustGetErrCode(sql10, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql11 := `create block part9 (
                 a int not null,
                 b int not null,
                 c int default null,
                 d int default null,
                 e int default null,
                 primary key (a, b),
                 unique key (b, c, d)
        )
        partition by range defCausumns (b, c) (
               partition p0 values less than (4, 5),
               partition p1 values less than (7, 9),
               partition p2 values less than (11, 22)
        )`
	tk.MustGetErrCode(sql11, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql12 := `create block part12 (a varchar(20), b binary, unique index (a(5))) partition by range defCausumns (a) (
			partition p0 values less than ('aaaaa'),
			partition p1 values less than ('bbbbb'),
			partition p2 values less than ('ccccc'))`
	tk.MustGetErrCode(sql12, tmysql.ErrUniqueKeyNeedAllFieldsInPf)
	tk.MustExec(`create block part12 (a varchar(20), b binary) partition by range defCausumns (a) (
			partition p0 values less than ('aaaaa'),
			partition p1 values less than ('bbbbb'),
			partition p2 values less than ('ccccc'))`)
	tk.MustGetErrCode("alter block part12 add unique index (a(5))", tmysql.ErrUniqueKeyNeedAllFieldsInPf)
	sql13 := `create block part13 (a varchar(20), b varchar(10), unique index (a(5),b)) partition by range defCausumns (b) (
			partition p0 values less than ('aaaaa'),
			partition p1 values less than ('bbbbb'),
			partition p2 values less than ('ccccc'))`
	tk.MustExec(sql13)
}

func (s *testIntegrationSuite2) TestPartitionDropPrimaryKey(c *C) {
	idxName := "primary"
	addIdxALLEGROSQL := "alter block partition_drop_idx add primary key idx1 (c1);"
	dropIdxALLEGROSQL := "alter block partition_drop_idx drop primary key;"
	testPartitionDropIndex(c, s.causetstore, s.lease, idxName, addIdxALLEGROSQL, dropIdxALLEGROSQL)
}

func (s *testIntegrationSuite3) TestPartitionDropIndex(c *C) {
	idxName := "idx1"
	addIdxALLEGROSQL := "alter block partition_drop_idx add index idx1 (c1);"
	dropIdxALLEGROSQL := "alter block partition_drop_idx drop index idx1;"
	testPartitionDropIndex(c, s.causetstore, s.lease, idxName, addIdxALLEGROSQL, dropIdxALLEGROSQL)
}

func testPartitionDropIndex(c *C, causetstore ekv.CausetStorage, lease time.Duration, idxName, addIdxALLEGROSQL, dropIdxALLEGROSQL string) {
	tk := testkit.NewTestKit(c, causetstore)
	done := make(chan error, 1)
	tk.MustExec("use test_db")
	tk.MustExec("drop block if exists partition_drop_idx;")
	tk.MustExec(`create block partition_drop_idx (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (3),
    	partition p1 values less than (5),
    	partition p2 values less than (7),
    	partition p3 values less than (11),
    	partition p4 values less than (15),
    	partition p5 values less than (20),
		partition p6 values less than (maxvalue)
   	);`)

	num := 20
	for i := 0; i < num; i++ {
		tk.MustExec("insert into partition_drop_idx values (?, ?, ?)", i, i, i)
	}
	tk.MustExec(addIdxALLEGROSQL)

	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	t, err := is.BlockByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("partition_drop_idx"))
	c.Assert(err, IsNil)

	var idx1 block.Index
	for _, pidx := range t.Indices() {
		if pidx.Meta().Name.L == idxName {
			idx1 = pidx
			break
		}
	}
	c.Assert(idx1, NotNil)

	solitonutil.StochastikExecInGoroutine(c, causetstore, dropIdxALLEGROSQL, done)
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err = <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			step := 10
			rand.Seed(time.Now().Unix())
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustExec("uFIDelate partition_drop_idx set c2 = 1 where c1 = ?", n)
				tk.MustExec("insert into partition_drop_idx values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	is = petri.GetPetri(ctx).SchemaReplicant()
	t, err = is.BlockByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("partition_drop_idx"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	pid := t.Meta().Partition.Definitions[0].ID
	var idxn block.Index
	t.Indices()
	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == idxName {
			idxn = idx
			break
		}
	}
	c.Assert(idxn, IsNil)
	idx := blocks.NewIndex(pid, t.Meta(), idx1.Meta())
	checkDelRangeDone(c, ctx, idx)
	tk.MustExec("drop block partition_drop_idx;")
}

func (s *testIntegrationSuite2) TestPartitionCancelAddPrimaryKey(c *C) {
	idxName := "primary"
	addIdxALLEGROSQL := "alter block t1 add primary key c3_index (c1);"
	testPartitionCancelAddIndex(c, s.causetstore, s.dom.DBS(), s.lease, idxName, addIdxALLEGROSQL)
}

func (s *testIntegrationSuite4) TestPartitionCancelAddIndex(c *C) {
	idxName := "idx1"
	addIdxALLEGROSQL := "create unique index c3_index on t1 (c1)"
	testPartitionCancelAddIndex(c, s.causetstore, s.dom.DBS(), s.lease, idxName, addIdxALLEGROSQL)
}

func testPartitionCancelAddIndex(c *C, causetstore ekv.CausetStorage, d dbs.DBS, lease time.Duration, idxName, addIdxALLEGROSQL string) {
	tk := testkit.NewTestKit(c, causetstore)

	tk.MustExec("use test_db")
	tk.MustExec("drop block if exists t1;")
	tk.MustExec(`create block t1 (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (1024),
    	partition p1 values less than (2048),
    	partition p2 values less than (3072),
    	partition p3 values less than (4096),
		partition p4 values less than (maxvalue)
   	);`)
	count := defaultBatchSize * 32
	// add some rows
	for i := 0; i < count; i += defaultBatchSize {
		batchInsert(tk, "t1", i, i+defaultBatchSize)
	}

	var checkErr error
	var c3IdxInfo *perceptron.IndexInfo
	hook := &dbs.TestDBSCallback{}
	originBatchSize := tk.MustQuery("select @@global.milevadb_dbs_reorg_batch_size")
	// Set batch size to lower try to slow down add-index reorganization, This if for hook to cancel this dbs job.
	tk.MustExec("set @@global.milevadb_dbs_reorg_batch_size = 32")
	ctx := tk.Se.(stochastikctx.Context)
	defer tk.MustExec(fmt.Sprintf("set @@global.milevadb_dbs_reorg_batch_size = %v", originBatchSize.Rows()[0][0]))
	hook.OnJobUFIDelatedExported, c3IdxInfo, checkErr = backgroundExecOnJobUFIDelatedExported(c, causetstore, ctx, hook, idxName)
	originHook := d.GetHook()
	defer d.(dbs.DBSForTest).SetHook(originHook)
	d.(dbs.DBSForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundExec(causetstore, addIdxALLEGROSQL, done)

	times := 0
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 10
			rand.Seed(time.Now().Unix())
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				tk.MustExec("delete from t1 where c1 = ?", n)
				tk.MustExec("insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}

	t := testGetBlockByName(c, ctx, "test_db", "t1")
	// Only one partition id test is taken here.
	pid := t.Meta().Partition.Definitions[0].ID
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, "c3_index"), IsFalse)
	}

	idx := blocks.NewIndex(pid, t.Meta(), c3IdxInfo)
	checkDelRangeDone(c, ctx, idx)

	tk.MustExec("drop block t1")
}

func backgroundExecOnJobUFIDelatedExported(c *C, causetstore ekv.CausetStorage, ctx stochastikctx.Context, hook *dbs.TestDBSCallback, idxName string) (
	func(*perceptron.Job), *perceptron.IndexInfo, error) {
	var checkErr error
	first := true
	c3IdxInfo := &perceptron.IndexInfo{}
	hook.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		addIndexNotFirstReorg := (job.Type == perceptron.CausetActionAddIndex || job.Type == perceptron.CausetActionAddPrimaryKey) &&
			job.SchemaState == perceptron.StateWriteReorganization && job.SnapshotVer != 0
		// If the action is adding index and the state is writing reorganization, it want to test the case of cancelling the job when backfilling indexes.
		// When the job satisfies this case of addIndexNotFirstReorg, the worker will start to backfill indexes.
		if !addIndexNotFirstReorg {
			// Get the index's meta.
			if c3IdxInfo != nil {
				return
			}
			t := testGetBlockByName(c, ctx, "test_db", "t1")
			for _, index := range t.WriblockIndices() {
				if index.Meta().Name.L == idxName {
					c3IdxInfo = index.Meta()
				}
			}
			return
		}
		// The job satisfies the case of addIndexNotFirst for the first time, the worker hasn't finished a batch of backfill indexes.
		if first {
			first = false
			return
		}
		if checkErr != nil {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.CausetStore = causetstore
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		jobIDs := []int64{job.ID}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		errs, err := admin.CancelJobs(txn, jobIDs)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		// It only tests cancel one DBS job.
		if errs[0] != nil {
			checkErr = errors.Trace(errs[0])
			return
		}
		txn, err = hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		err = txn.Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}
	return hook.OnJobUFIDelatedExported, c3IdxInfo, checkErr
}

func (s *testIntegrationSuite5) TestPartitionAddPrimaryKey(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	testPartitionAddIndexOrPK(c, tk, "primary key")
}

func (s *testIntegrationSuite1) TestPartitionAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	testPartitionAddIndexOrPK(c, tk, "index")
}

func testPartitionAddIndexOrPK(c *C, tk *testkit.TestKit, key string) {
	tk.MustExec("use test")
	tk.MustExec(`create block partition_add_idx (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p3 values less than (2001),
	partition p4 values less than (2004),
	partition p5 values less than (2008),
	partition p6 values less than (2012),
	partition p7 values less than (2020)
	);`)
	testPartitionAddIndex(tk, c, key)

	// test hash partition block.
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = '1';")
	tk.MustExec("drop block if exists partition_add_idx")
	tk.MustExec(`create block partition_add_idx (
	id int not null,
	hired date not null
	) partition by hash( year(hired) ) partitions 4;`)
	testPartitionAddIndex(tk, c, key)

	// Test hash partition for pr 10475.
	tk.MustExec("drop block if exists t1")
	defer tk.MustExec("drop block if exists t1")
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = '1';")
	tk.MustExec("create block t1 (a int, b int, unique key(a)) partition by hash(a) partitions 5;")
	tk.MustExec("insert into t1 values (0,0),(1,1),(2,2),(3,3);")
	tk.MustExec(fmt.Sprintf("alter block t1 add %s idx(a)", key))
	tk.MustExec("admin check block t1;")

	// Test range partition for pr 10475.
	tk.MustExec("drop block t1")
	tk.MustExec("create block t1 (a int, b int, unique key(a)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20));")
	tk.MustExec("insert into t1 values (0,0);")
	tk.MustExec(fmt.Sprintf("alter block t1 add %s idx(a)", key))
	tk.MustExec("admin check block t1;")
}

func testPartitionAddIndex(tk *testkit.TestKit, c *C, key string) {
	idxName1 := "idx1"

	f := func(end int, isPK bool) string {
		dml := fmt.Sprintf("insert into partition_add_idx values")
		for i := 0; i < end; i++ {
			dVal := 1988 + rand.Intn(30)
			if isPK {
				dVal = 1518 + i
			}
			dml += fmt.Sprintf("(%d, '%d-01-01')", i, dVal)
			if i != end-1 {
				dml += ","
			}
		}
		return dml
	}
	var dml string
	if key == "primary key" {
		idxName1 = "primary"
		// For the primary key, hired must be unique.
		dml = f(500, true)
	} else {
		dml = f(500, false)
	}
	tk.MustExec(dml)

	tk.MustExec(fmt.Sprintf("alter block partition_add_idx add %s idx1 (hired)", key))
	tk.MustExec("alter block partition_add_idx add index idx2 (id, hired)")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	t, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("partition_add_idx"))
	c.Assert(err, IsNil)
	var idx1 block.Index
	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == idxName1 {
			idx1 = idx
			break
		}
	}
	c.Assert(idx1, NotNil)

	tk.MustQuery(fmt.Sprintf("select count(hired) from partition_add_idx use index(%s)", idxName1)).Check(testkit.Rows("500"))
	tk.MustQuery("select count(id) from partition_add_idx use index(idx2)").Check(testkit.Rows("500"))

	tk.MustExec("admin check block partition_add_idx")
	tk.MustExec("drop block partition_add_idx")
}

func (s *testIntegrationSuite5) TestDropSchemaWithPartitionBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_db_with_partition")
	tk.MustExec("create database test_db_with_partition")
	tk.MustExec("use test_db_with_partition")
	tk.MustExec(`create block t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	tk.MustExec("insert into t_part values (1),(2),(11),(12);")
	ctx := s.ctx
	tbl := testGetBlockByName(c, ctx, "test_db_with_partition", "t_part")

	// check records num before drop database.
	recordsNum := getPartitionBlockRecordsNum(c, ctx, tbl.(block.PartitionedBlock))
	c.Assert(recordsNum, Equals, 4)

	tk.MustExec("drop database if exists test_db_with_partition")

	// check job args.
	rs, err := tk.Exec("admin show dbs jobs")
	c.Assert(err, IsNil)
	rows, err := stochastik.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(err, IsNil)
	event := rows[0]
	c.Assert(event.GetString(3), Equals, "drop schemaReplicant")
	jobID := event.GetInt64(0)
	ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDBSJob(jobID)
		c.Assert(err, IsNil)
		var blockIDs []int64
		err = historyJob.DecodeArgs(&blockIDs)
		c.Assert(err, IsNil)
		// There is 2 partitions.
		c.Assert(len(blockIDs), Equals, 3)
		return nil
	})

	// check records num after drop database.
	for i := 0; i < waitForCleanDataRound; i++ {
		recordsNum = getPartitionBlockRecordsNum(c, ctx, tbl.(block.PartitionedBlock))
		if recordsNum != 0 {
			time.Sleep(waitForCleanDataInterval)
		} else {
			break
		}
	}
	c.Assert(recordsNum, Equals, 0)
}

func getPartitionBlockRecordsNum(c *C, ctx stochastikctx.Context, tbl block.PartitionedBlock) int {
	num := 0
	info := tbl.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.(block.PartitionedBlock).GetPartition(pid)
		startKey := partition.RecordKey(ekv.IntHandle(math.MinInt64))
		c.Assert(ctx.NewTxn(context.Background()), IsNil)
		err := partition.IterRecords(ctx, startKey, partition.DefCauss(),
			func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
				num++
				return true, nil
			})
		c.Assert(err, IsNil)
	}
	return num
}

func (s *testIntegrationSuite3) TestPartitionErrorCode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// add partition
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustExec("drop database if exists test_db_with_partition")
	tk.MustExec("create database test_db_with_partition")
	tk.MustExec("use test_db_with_partition")
	tk.MustExec(`create block employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash(store_id)
	partitions 4;`)
	_, err := tk.Exec("alter block employees add partition partitions 8;")
	c.Assert(dbs.ErrUnsupportedAddPartition.Equal(err), IsTrue)

	_, err = tk.Exec("alter block employees add partition (partition p5 values less than (42));")
	c.Assert(dbs.ErrUnsupportedAddPartition.Equal(err), IsTrue)

	// coalesce partition
	tk.MustExec(`create block clients (
		id int,
		fname varchar(30),
		lname varchar(30),
		signed date
	)
	partition by hash( month(signed) )
	partitions 12;`)
	_, err = tk.Exec("alter block clients coalesce partition 4;")
	c.Assert(dbs.ErrUnsupportedCoalescePartition.Equal(err), IsTrue)

	tk.MustExec(`create block t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	_, err = tk.Exec("alter block t_part coalesce partition 4;")
	c.Assert(dbs.ErrCoalesceOnlyOnHashPartition.Equal(err), IsTrue)

	tk.MustGetErrCode(`alter block t_part reorganize partition p0, p1 into (
			partition p0 values less than (1980));`, tmysql.ErrUnsupportedDBSOperation)

	tk.MustGetErrCode("alter block t_part check partition p0, p1;", tmysql.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter block t_part optimize partition p0,p1;", tmysql.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter block t_part rebuild partition p0,p1;", tmysql.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter block t_part remove partitioning;", tmysql.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter block t_part repair partition p1;", tmysql.ErrUnsupportedDBSOperation)
}

func (s *testIntegrationSuite5) TestConstAndTimezoneDepent(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// add partition
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustExec("drop database if exists test_db_with_partition_const")
	tk.MustExec("create database test_db_with_partition_const")
	tk.MustExec("use test_db_with_partition_const")

	sql1 := `create block t1 ( id int )
		partition by range(4) (
		partition p1 values less than (10)
		);`
	tk.MustGetErrCode(sql1, tmysql.ErrWrongExprInPartitionFunc)

	sql2 := `create block t2 ( time_recorded timestamp )
		partition by range(TO_DAYS(time_recorded)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql2, tmysql.ErrWrongExprInPartitionFunc)

	sql3 := `create block t3 ( id int )
		partition by range(DAY(id)) (
		partition p1 values less than (1)
		);`
	tk.MustGetErrCode(sql3, tmysql.ErrWrongExprInPartitionFunc)

	sql4 := `create block t4 ( id int )
		partition by hash(4) partitions 4
		;`
	tk.MustGetErrCode(sql4, tmysql.ErrWrongExprInPartitionFunc)

	sql5 := `create block t5 ( time_recorded timestamp )
		partition by range(to_seconds(time_recorded)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql5, tmysql.ErrWrongExprInPartitionFunc)

	sql6 := `create block t6 ( id int )
		partition by range(to_seconds(id)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql6, tmysql.ErrWrongExprInPartitionFunc)

	sql7 := `create block t7 ( time_recorded timestamp )
		partition by range(abs(time_recorded)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql7, tmysql.ErrWrongExprInPartitionFunc)

	sql8 := `create block t2332 ( time_recorded time )
         partition by range(TO_DAYS(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql8, tmysql.ErrWrongExprInPartitionFunc)

	sql9 := `create block t1 ( id int )
		partition by hash(4) partitions 4;`
	tk.MustGetErrCode(sql9, tmysql.ErrWrongExprInPartitionFunc)

	sql10 := `create block t1 ( id int )
		partition by hash(ed) partitions 4;`
	tk.MustGetErrCode(sql10, tmysql.ErrBadField)

	sql11 := `create block t2332 ( time_recorded time )
         partition by range(TO_SECONDS(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql11, tmysql.ErrWrongExprInPartitionFunc)

	sql12 := `create block t2332 ( time_recorded time )
         partition by range(TO_SECONDS(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql12, tmysql.ErrWrongExprInPartitionFunc)

	sql13 := `create block t2332 ( time_recorded time )
         partition by range(day(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql13, tmysql.ErrWrongExprInPartitionFunc)

	sql14 := `create block t2332 ( time_recorded timestamp )
         partition by range(day(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql14, tmysql.ErrWrongExprInPartitionFunc)
}

func (s *testIntegrationSuite5) TestConstAndTimezoneDepent2(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// add partition
	tk.MustExec("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustExec("drop database if exists test_db_with_partition_const")
	tk.MustExec("create database test_db_with_partition_const")
	tk.MustExec("use test_db_with_partition_const")

	tk.MustExec(`create block t1 ( time_recorded datetime )
	partition by range(TO_DAYS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustExec(`create block t2 ( time_recorded date )
	partition by range(TO_DAYS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustExec(`create block t3 ( time_recorded date )
	partition by range(TO_SECONDS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustExec(`create block t4 ( time_recorded date )
	partition by range(TO_SECONDS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustExec(`create block t5 ( time_recorded timestamp )
	partition by range(unix_timestamp(time_recorded)) (
		partition p1 values less than (1559192604)
	);`)
}

func (s *testIntegrationSuite3) TestUnsupportedPartitionManagementDBSs(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test;")
	tk.MustExec("drop block if exists test_1465;")
	tk.MustExec(`
		create block test_1465 (a int)
		partition by range(a) (
			partition p1 values less than (10),
			partition p2 values less than (20),
			partition p3 values less than (30)
		);
	`)

	_, err := tk.Exec("alter block test_1465 partition by hash(a)")
	c.Assert(err, ErrorMatches, ".*alter block partition is unsupported")
}

func (s *testIntegrationSuite7) TestCommitWhenSchemaChange(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec(`create block schema_change (a int, b timestamp)
			partition by range(a) (
			    partition p0 values less than (4),
			    partition p1 values less than (7),
			    partition p2 values less than (11)
			)`)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustExec("use test")

	tk.MustExec("begin")
	tk.MustExec("insert into schema_change values (1, '2020-12-25 13:27:42')")
	tk.MustExec("insert into schema_change values (3, '2020-12-25 13:27:43')")

	tk2.MustExec("alter block schema_change add index idx(b)")

	tk.MustExec("insert into schema_change values (5, '2020-12-25 13:27:43')")
	tk.MustExec("insert into schema_change values (9, '2020-12-25 13:27:44')")
	atomic.StoreUint32(&stochastik.SchemaChangedWithoutRetry, 1)
	defer func() {
		atomic.StoreUint32(&stochastik.SchemaChangedWithoutRetry, 0)
	}()
	_, err := tk.Se.Execute(context.Background(), "commit")
	c.Assert(petri.ErrSchemaReplicantChanged.Equal(err), IsTrue)

	// Cover a bug that schemaReplicant validator does not prevent transaction commit when
	// the schemaReplicant has changed on the partitioned block.
	// That bug will cause data and index inconsistency!
	tk.MustExec("admin check block schema_change")
	tk.MustQuery("select * from schema_change").Check(testkit.Rows())

	// Check inconsistency when exchanging partition
	tk.MustExec(`drop block if exists pt, nt;`)
	tk.MustExec(`create block pt (a int) partition by hash(a) partitions 2;`)
	tk.MustExec(`create block nt (a int);`)

	tk.MustExec("begin")
	tk.MustExec("insert into nt values (1), (3), (5);")
	tk2.MustExec("alter block pt exchange partition p1 with block nt;")
	tk.MustExec("insert into nt values (7), (9);")
	_, err = tk.Se.Execute(context.Background(), "commit")
	c.Assert(petri.ErrSchemaReplicantChanged.Equal(err), IsTrue)

	tk.MustExec("admin check block pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustExec("admin check block nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())

	tk.MustExec("begin")
	tk.MustExec("insert into pt values (1), (3), (5);")
	tk2.MustExec("alter block pt exchange partition p1 with block nt;")
	tk.MustExec("insert into pt values (7), (9);")
	_, err = tk.Se.Execute(context.Background(), "commit")
	c.Assert(petri.ErrSchemaReplicantChanged.Equal(err), IsTrue)

	tk.MustExec("admin check block pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustExec("admin check block nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())
}
