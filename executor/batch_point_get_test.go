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

package executor_test

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
)

type testBatchPointGetSuite struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

func newStoreWithBootstrap() (ekv.CausetStorage, *petri.Petri, error) {
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()

	dom, err := stochastik.BootstrapStochastik(causetstore)
	if err != nil {
		return nil, nil, err
	}
	return causetstore, dom, errors.Trace(err)
}

func (s *testBatchPointGetSuite) SetUpSuite(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	s.dom = dom
}

func (s *testBatchPointGetSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *testBatchPointGetSuite) TestBatchPointGetExec(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int primary key auto_increment not null, b int, c int, unique key idx_abc(a, b, c))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 5)")
	tk.MustQuery("select * from t").Check(testkit.Events(
		"1 1 1",
		"2 2 2",
		"3 3 3",
		"4 4 5",
	))
	tk.MustQuery("select a, b, c from t where (a, b, c) in ((1, 1, 1), (1, 1, 1), (1, 1, 1))").Check(testkit.Events(
		"1 1 1",
	))
	tk.MustQuery("select a, b, c from t where (a, b, c) in ((1, 1, 1), (2, 2, 2), (1, 1, 1))").Check(testkit.Events(
		"1 1 1",
		"2 2 2",
	))
	tk.MustQuery("select a, b, c from t where (a, b, c) in ((1, 1, 1), (2, 2, 2), (100, 1, 1))").Check(testkit.Events(
		"1 1 1",
		"2 2 2",
	))
	tk.MustQuery("select a, b, c from t where (a, b, c) in ((1, 1, 1), (2, 2, 2), (100, 1, 1), (4, 4, 5))").Check(testkit.Events(
		"1 1 1",
		"2 2 2",
		"4 4 5",
	))
	tk.MustQuery("select * from t where a in (1, 2, 4, 1, 2)").Check(testkit.Events(
		"1 1 1",
		"2 2 2",
		"4 4 5",
	))
	tk.MustQuery("select * from t where a in (1, 2, 4, 1, 2, 100)").Check(testkit.Events(
		"1 1 1",
		"2 2 2",
		"4 4 5",
	))
	tk.MustQuery("select a from t where a in (1, 2, 4, 1, 2, 100)").Check(testkit.Events(
		"1",
		"2",
		"4",
	))
}

func (s *testBatchPointGetSuite) TestBatchPointGetInTxn(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (id int primary key auto_increment, name varchar(30))")

	// Fix a bug that BatchPointGetExec doesn't consider membuffer data in a transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into t values (4, 'name')")
	tk.MustQuery("select * from t where id in (4)").Check(testkit.Events("4 name"))
	tk.MustQuery("select * from t where id in (4) for uFIDelate").Check(testkit.Events("4 name"))
	tk.MustExec("rollback")

	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t values (4, 'name')")
	tk.MustQuery("select * from t where id in (4)").Check(testkit.Events("4 name"))
	tk.MustQuery("select * from t where id in (4) for uFIDelate").Check(testkit.Events("4 name"))
	tk.MustExec("rollback")

	tk.MustExec("create block s (a int, b int, c int, primary key (a, b))")
	tk.MustExec("insert s values (1, 1, 1), (3, 3, 3), (5, 5, 5)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("uFIDelate s set c = 10 where a = 3")
	tk.MustQuery("select * from s where (a, b) in ((1, 1), (2, 2), (3, 3)) for uFIDelate").Check(testkit.Events("1 1 1", "3 3 10"))
	tk.MustExec("rollback")
}

func (s *testBatchPointGetSuite) TestBatchPointGetCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block customers (id int primary key, token varchar(255) unique)")
	tk.MustExec("INSERT INTO test.customers (id, token) VALUES (28, '07j')")
	tk.MustExec("INSERT INTO test.customers (id, token) VALUES (29, '03j')")
	tk.MustExec("BEGIN")
	tk.MustQuery("SELECT id, token FROM test.customers WHERE id IN (28)")
	tk.MustQuery("SELECT id, token FROM test.customers WHERE id IN (28, 29);").Check(testkit.Events("28 07j", "29 03j"))
}

func (s *testBatchPointGetSuite) TestIssue18843(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t18843 ( id bigint(10) primary key, f varchar(191) default null, unique key `idx_f` (`f`))")
	tk.MustExec("insert into t18843 values (1, '')")
	tk.MustQuery("select * from t18843 where f in (null)").Check(testkit.Events())

	tk.MustExec("insert into t18843 values (2, null)")
	tk.MustQuery("select * from t18843 where f in (null)").Check(testkit.Events())
	tk.MustQuery("select * from t18843 where f is null").Check(testkit.Events("2 <nil>"))
}
