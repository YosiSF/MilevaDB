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

package core_test

import (
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testleak"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
)

var _ = Suite(&testValidatorSuite{})

type testValidatorSuite struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	se          stochastik.Stochastik
	ctx         stochastikctx.Context
	is          schemareplicant.SchemaReplicant
}

func (s *testValidatorSuite) SetUpTest(c *C) {
	var err error
	s.causetstore, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)

	s.se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	s.ctx = s.se.(stochastikctx.Context)

	s.is = schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{core.MockSignedBlock()})
}

func (s *testValidatorSuite) runALLEGROSQL(c *C, allegrosql string, inPrepare bool, terr error) {
	stmts, err1 := stochastik.Parse(s.ctx, allegrosql)
	c.Assert(err1, IsNil)
	c.Assert(stmts, HasLen, 1)
	stmt := stmts[0]
	var opts []core.PreprocessOpt
	if inPrepare {
		opts = append(opts, core.InPrepare)
	}
	err := core.Preprocess(s.ctx, stmt, s.is, opts...)
	c.Assert(terror.ErrorEqual(err, terr), IsTrue, Commentf("allegrosql: %s, err:%v", allegrosql, err))
}

func (s *testValidatorSuite) TestValidator(c *C) {
	defer testleak.AfterTest(c)()
	defer func() {
		s.dom.Close()
		s.causetstore.Close()
	}()
	tests := []struct {
		allegrosql string
		inPrepare  bool
		err        error
	}{
		{"select ?", false, berolinaAllegroSQL.ErrSyntax},
		{"select ?", true, nil},
		{"create block t(id int not null auto_increment default 2, key (id))", true,
			errors.New("Invalid default value for 'id'")},
		{"create block t(id int not null default 2 auto_increment, key (id))", true,
			errors.New("Invalid default value for 'id'")},
		// Default value can be null when the column is primary key in MyALLEGROSQL 5.6.
		// But it can't be null in MyALLEGROSQL 5.7.
		{"create block t(id int auto_increment default null, primary key (id))", true, nil},
		{"create block t(id int default null auto_increment, primary key (id))", true, nil},
		{"create block t(id int not null auto_increment)", true,
			errors.New("[autoid:1075]Incorrect block definition; there can be only one auto column and it must be defined as a key")},
		{"create block t(id int not null auto_increment, c int auto_increment, key (id, c))", true,
			errors.New("[autoid:1075]Incorrect block definition; there can be only one auto column and it must be defined as a key")},
		{"create block t(id int not null auto_increment, c int, key (c, id))", true,
			errors.New("[autoid:1075]Incorrect block definition; there can be only one auto column and it must be defined as a key")},
		{"create block t(id decimal auto_increment, key (id))", true,
			errors.New("Incorrect column specifier for column 'id'")},
		{"create block t(id float auto_increment, key (id))", true, nil},
		{"create block t(id int auto_increment) ENGINE=MYISAM", true, nil},
		{"create block t(a int primary key, b int, c varchar(10), d char(256));", true,
			errors.New("[types:1074]DeferredCauset length too big for column 'd' (max = 255); use BLOB or TEXT instead")},
		{"create index ib on t(b,a,b);", true, errors.New("[schemaReplicant:1060]Duplicate column name 'b'")},
		{"alter block t add index idx(a, b, A)", true, errors.New("[schemaReplicant:1060]Duplicate column name 'A'")},
		{"create block t (a int, b int, index(a, b, A))", true, errors.New("[schemaReplicant:1060]Duplicate column name 'A'")},
		{"create block t (a int, b int, key(a, b, A))", true, errors.New("[schemaReplicant:1060]Duplicate column name 'A'")},
		{"create block t (a int, b int, unique(a, b, A))", true, errors.New("[schemaReplicant:1060]Duplicate column name 'A'")},
		{"create block t (a int, b int, unique key(a, b, A))", true, errors.New("[schemaReplicant:1060]Duplicate column name 'A'")},
		{"create block t (a int, b int, unique index(a, b, A))", true, errors.New("[schemaReplicant:1060]Duplicate column name 'A'")},
		{"create block t(c1 int not null primary key, c2 int not null primary key)", true,
			errors.New("[schemaReplicant:1068]Multiple primary key defined")},
		{"create block t(c1 int not null primary key, c2 int not null, primary key(c1))", true,
			errors.New("[schemaReplicant:1068]Multiple primary key defined")},
		{"create block t(c1 int not null, c2 int not null, primary key(c1), primary key(c2))", true,
			errors.New("[schemaReplicant:1068]Multiple primary key defined")},
		{"alter block t auto_increment=1", true, nil},
		{"alter block t add column c int auto_increment key, auto_increment=10", true, nil},
		{"alter block t add column c int auto_increment key", true, nil},
		{"alter block t add column char4294967295 char(255)", true, nil},
		{"create block t (c float(53))", true, nil},
		{"alter block t add column c float(53)", true, nil},
		{"create block t (c set ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64'))", true, nil},
		{"alter block t add column c set ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64')", true, nil},
		{"create block t (c varchar(21845) CHARACTER SET utf8)", true, nil},
		{"create block t (c varchar(16383) CHARACTER SET utf8mb4)", true, nil},
		{"create block t (c varchar(65535) CHARACTER SET ascii)", true, nil},
		{"alter block t add column c varchar(21845) CHARACTER SET utf8", true, nil},
		{"alter block t add column c varchar(16383) CHARACTER SET utf8mb4", true, nil},
		{"alter block t add column c varchar(65535) CHARACTER SET ascii", true, nil},
		{"alter block t add column char4294967295 char(4294967295)", true,
			errors.New("[types:1074]DeferredCauset length too big for column 'char4294967295' (max = 255); use BLOB or TEXT instead")},
		{"alter block t add column char4294967296 char(4294967296)", true,
			errors.New("[types:1439]Display width out of range for column 'char4294967296' (max = 4294967295)")},
		{"create block t (c float(4294967296))", true,
			errors.New("[types:1439]Display width out of range for column 'c' (max = 4294967295)")},
		{"alter block t add column c float(4294967296)", true,
			errors.New("[types:1439]Display width out of range for column 'c' (max = 4294967295)")},
		{"create block t (set65 set ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65'))", true,
			errors.New("[types:1097]Too many strings for column set65 and SET")},
		{"alter block t add column set65 set ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65')", true,
			errors.New("[types:1097]Too many strings for column set65 and SET")},
		{"create block t (c varchar(4294967295) CHARACTER SET utf8)", true,
			errors.New("[types:1074]DeferredCauset length too big for column 'c' (max = 21845); use BLOB or TEXT instead")},
		{"create block t (c varchar(4294967295) CHARACTER SET utf8mb4)", true,
			errors.New("[types:1074]DeferredCauset length too big for column 'c' (max = 16383); use BLOB or TEXT instead")},
		{"create block t (c varchar(4294967295) CHARACTER SET ascii)", true,
			errors.New("[types:1074]DeferredCauset length too big for column 'c' (max = 65535); use BLOB or TEXT instead")},
		{"alter block t add column c varchar(4294967295) CHARACTER SET utf8", true,
			errors.New("[types:1074]DeferredCauset length too big for column 'c' (max = 21845); use BLOB or TEXT instead")},
		{"alter block t add column c varchar(4294967295) CHARACTER SET utf8mb4;", true,
			errors.New("[types:1074]DeferredCauset length too big for column 'c' (max = 16383); use BLOB or TEXT instead")},
		{"alter block t add column c varchar(4294967295) CHARACTER SET ascii", true,
			errors.New("[types:1074]DeferredCauset length too big for column 'c' (max = 65535); use BLOB or TEXT instead")},
		{"create block t", false, dbs.ErrBlockMustHaveDeferredCausets},
		{"create block t (unique(c))", false, dbs.ErrBlockMustHaveDeferredCausets},

		{"create block `t ` (a int)", true, errors.New("[dbs:1103]Incorrect block name 't '")},
		{"create block `` (a int)", true, errors.New("[dbs:1103]Incorrect block name ''")},
		{"create block t (`` int)", true, errors.New("[dbs:1166]Incorrect column name ''")},
		{"create block t (`a ` int)", true, errors.New("[dbs:1166]Incorrect column name 'a '")},
		{"drop block if exists ``", true, errors.New("[dbs:1103]Incorrect block name ''")},
		{"drop block `t `", true, errors.New("[dbs:1103]Incorrect block name 't '")},
		{"create database ``", true, errors.New("[dbs:1102]Incorrect database name ''")},
		{"create database `test `", true, errors.New("[dbs:1102]Incorrect database name 'test '")},
		{"alter database collate = 'utf8mb4_bin'", true, nil},
		{"alter database `` collate = 'utf8mb4_bin'", true, errors.New("[dbs:1102]Incorrect database name ''")},
		{"alter database `test ` collate = 'utf8mb4_bin'", true, errors.New("[dbs:1102]Incorrect database name 'test '")},
		{"drop database ``", true, errors.New("[dbs:1102]Incorrect database name ''")},
		{"drop database `test `", true, errors.New("[dbs:1102]Incorrect database name 'test '")},
		{"alter block `t ` add column c int", true, errors.New("[dbs:1103]Incorrect block name 't '")},
		{"alter block `` add column c int", true, errors.New("[dbs:1103]Incorrect block name ''")},
		{"alter block t rename `t ` ", true, errors.New("[dbs:1103]Incorrect block name 't '")},
		{"alter block t rename `` ", true, errors.New("[dbs:1103]Incorrect block name ''")},
		{"alter block t add column `c ` int", true, errors.New("[dbs:1166]Incorrect column name 'c '")},
		{"alter block t add column `` int", true, errors.New("[dbs:1166]Incorrect column name ''")},
		{"alter block t change column a `` int", true, errors.New("[dbs:1166]Incorrect column name ''")},
		{"alter block t change column a `a ` int", true, errors.New("[dbs:1166]Incorrect column name 'a '")},
		{"create index idx on `t ` (a)", true, errors.New("[dbs:1103]Incorrect block name 't '")},
		{"create index idx on  `` (a)", true, errors.New("[dbs:1103]Incorrect block name ''")},
		{"rename block t to ``", false, errors.New("[dbs:1103]Incorrect block name ''")},
		{"rename block `` to t", false, errors.New("[dbs:1103]Incorrect block name ''")},

		// issue 3844
		{`create block t (a set("a, b", "c, d"))`, true, errors.New("[types:1367]Illegal set 'a, b' value found during parsing")},
		{`alter block t add column a set("a, b", "c, d")`, true, errors.New("[types:1367]Illegal set 'a, b' value found during parsing")},
		// issue 3843
		{"create index `primary` on t (i)", true, errors.New("[dbs:1280]Incorrect index name 'primary'")},
		{"alter block t add index `primary` (i)", true, errors.New("[dbs:1280]Incorrect index name 'primary'")},

		// issue 2273
		{"create block t(a char, b char, c char, d char, e char, f char, g char, h char ,i char, j char, k int, l char ,m char , n char, o char , p char, q char, index(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q))", true, errors.New("[schemaReplicant:1070]Too many key parts specified; max 16 parts allowed")},

		// issue #4429
		{"CREATE TABLE `t` (`a` date DEFAULT now());", false, types.ErrInvalidDefault},
		{"CREATE TABLE `t` (`a` timestamp DEFAULT now());", false, nil},
		{"CREATE TABLE `t` (`a` datetime DEFAULT now());", false, nil},
		{"CREATE TABLE `t` (`a` int DEFAULT now());", false, types.ErrInvalidDefault},
		{"CREATE TABLE `t` (`a` float DEFAULT now());", false, types.ErrInvalidDefault},
		{"CREATE TABLE `t` (`a` varchar(10) DEFAULT now());", false, types.ErrInvalidDefault},
		{"CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );", false, nil},

		{`explain format = "xx" select 100;`, false, core.ErrUnknownExplainFormat.GenWithStackByArgs("xx")},

		// issue 4472
		{`select sum(distinct(if('a', (select adddate(elt(999, count(*)), interval 1 day)), .1))) as foo;`, true, nil},
		{`select sum(1 in (select count(1)))`, true, nil},

		// issue 5529
		{"CREATE TABLE `t` (`id` int(11) NOT NULL AUTO_INCREMENT, `a` decimal(100,4) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin", false, types.ErrTooBigPrecision},
		{"CREATE TABLE `t` (`id` int(11) NOT NULL AUTO_INCREMENT, `a` decimal(65,4) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin", true, nil},
		{"CREATE TABLE `t` (`id` int(11) NOT NULL AUTO_INCREMENT, `a` decimal(65,31) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin", false, types.ErrTooBigScale},
		{"CREATE TABLE `t` (`id` int(11) NOT NULL AUTO_INCREMENT, `a` decimal(66,31) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin", false, types.ErrTooBigScale},
		{"alter block t modify column a DECIMAL(66,30);", false, types.ErrTooBigPrecision},
		{"alter block t modify column a DECIMAL(65,31);", false, types.ErrTooBigScale},
		{"alter block t modify column a DECIMAL(65,30);", true, nil},

		{"CREATE TABLE t (a float(255, 30))", true, nil},
		{"CREATE TABLE t (a double(255, 30))", true, nil},
		{"CREATE TABLE t (a float(256, 30))", false, types.ErrTooBigPrecision},
		{"CREATE TABLE t (a float(255, 31))", false, types.ErrTooBigScale},
		{"CREATE TABLE t (a double(256, 30))", false, types.ErrTooBigPrecision},
		{"CREATE TABLE t (a double(255, 31))", false, types.ErrTooBigScale},

		// FIXME: temporary 'not implemented yet' test for 'CREATE TABLE ... SELECT' (issue 4754)
		{"CREATE TABLE t SELECT * FROM u", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},
		{"CREATE TABLE t (m int) SELECT * FROM u", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},
		{"CREATE TABLE t IGNORE SELECT * FROM u UNION SELECT * from v", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},
		{"CREATE TABLE t (m int) REPLACE AS (SELECT * FROM u) UNION (SELECT * FROM v)", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},

		{"select * from ( select 1 ) a, (select 2) a;", false, core.ErrNonUniqBlock},
		{"select * from ( select 1 ) a, (select 2) b, (select 3) a;", false, core.ErrNonUniqBlock},
		{"select * from ( select 1 ) a, (select 2) b, (select 3) A;", false, core.ErrNonUniqBlock},
		{"select * from ( select 1 ) a join (select 2) b join (select 3) a;", false, core.ErrNonUniqBlock},
		{"select person.id from person inner join person on person.id = person.id;", false, core.ErrNonUniqBlock},
		{"select * from ( select 1 ) a, (select 2) b;", true, nil},
		{"select * from (select * from ( select 1 ) a join (select 2) b) b join (select 3) a;", false, nil},
		{"select * from (select 1 ) a , (select 2) b, (select * from (select 3) a join (select 4) b) c;", false, nil},

		{"CREATE VIEW V (a,b,c) AS SELECT 1,1,3;", false, nil},
		{"CREATE VIEW V AS SELECT 5 INTO OUTFILE 'ttt'", true, dbs.ErrViewSelectClause.GenWithStackByArgs("INFO")},
		{"CREATE VIEW V AS SELECT 5 FOR UFIDelATE", false, nil},
		{"CREATE VIEW V AS SELECT 5 LOCK IN SHARE MODE", false, nil},

		// issue 9464
		{"CREATE TABLE t1 (id INT NOT NULL, c1 VARCHAR(20) AS ('foo') VIRTUAL KEY NULL, PRIMARY KEY (id));", false, core.ErrUnsupportedOnGeneratedDeferredCauset},
		{"CREATE TABLE t1 (id INT NOT NULL, c1 VARCHAR(20) AS ('foo') VIRTUAL KEY NOT NULL, PRIMARY KEY (id));", false, core.ErrUnsupportedOnGeneratedDeferredCauset},
		{"create block t (a DOUBLE NULL, b_sto DOUBLE GENERATED ALWAYS AS (a + 2) STORED UNIQUE KEY NOT NULL PRIMARY KEY);", false, nil},

		// issue 13032
		{"CREATE TABLE origin (a int primary key, b varchar(10), c int auto_increment);", false, autoid.ErrWrongAutoKey},
		{"CREATE TABLE origin (a int auto_increment, b int key);", false, autoid.ErrWrongAutoKey},
		{"CREATE TABLE origin (a int auto_increment, b int unique);", false, autoid.ErrWrongAutoKey},
		{"CREATE TABLE origin (a int primary key auto_increment, b int);", false, nil},
		{"CREATE TABLE origin (a int unique auto_increment, b int);", false, nil},
		{"CREATE TABLE origin (a int key auto_increment, b int);", false, nil},
	}

	_, err := s.se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	for _, tt := range tests {
		s.runALLEGROSQL(c, tt.allegrosql, tt.inPrepare, tt.err)
	}
}

func (s *testValidatorSuite) TestForeignKey(c *C) {
	defer testleak.AfterTest(c)()
	defer func() {
		s.dom.Close()
		s.causetstore.Close()
	}()

	_, err := s.se.Execute(context.Background(), "create block test.t1(a int, b int, c int)")
	c.Assert(err, IsNil)

	_, err = s.se.Execute(context.Background(), "create block test.t2(d int)")
	c.Assert(err, IsNil)

	_, err = s.se.Execute(context.Background(), "create database test2")
	c.Assert(err, IsNil)

	_, err = s.se.Execute(context.Background(), "create block test2.t(e int)")
	c.Assert(err, IsNil)

	s.is = s.dom.SchemaReplicant()

	s.runALLEGROSQL(c, "ALTER TABLE test.t1 ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES t2 (d)", false, nil)

	_, err = s.se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	s.runALLEGROSQL(c, "ALTER TABLE test.t1 ADD CONSTRAINT fk FOREIGN KEY (b) REFERENCES t2 (d)", false, nil)

	s.runALLEGROSQL(c, "ALTER TABLE test.t1 ADD CONSTRAINT fk FOREIGN KEY (c) REFERENCES test2.t (e)", false, nil)
}
