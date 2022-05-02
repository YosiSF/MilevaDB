// INTERLOCKyright 2020-present, WHTCORPS INC, Inc.
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

package mockeinsteindb_test

import (
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
)

var _ = Suite(&testExecutorSuite{})

type testExecutorSuite struct {
	cluster     *mockeinsteindb.Cluster
	causetstore ekv.CausetStorage
	mvsr-oocStore   mockeinsteindb.MVCCStore
	dom         *petri.Petri
}

func (s *testExecutorSuite) SetUpSuite(c *C) {
	rpcClient, cluster, FIDelClient, err := mockeinsteindb.NewEinsteinDBAndFIDelClient("")
	c.Assert(err, IsNil)
	mockeinsteindb.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	s.mvsr-oocStore = rpcClient.MvccStore
	causetstore, err := einsteindb.NewTestEinsteinDBStore(rpcClient, FIDelClient, nil, nil, 0)
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *testExecutorSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *testExecutorSuite) TestResolvedLargeTxnLocks(c *C) {
	// This test checks the resolve dagger functionality.
	// When a txn meets the dagger of a large transaction, it should not block by the
	// dagger.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t (id int primary key, val int)")
	dom := petri.GetPetri(tk.Se)
	schemaReplicant := dom.SchemaReplicant()
	tbl, err := schemaReplicant.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)

	tk.MustExec("insert into t values (1, 1)")

	oracle := s.causetstore.GetOracle()
	tso, err := oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)

	key := blockcodec.EncodeRowKeyWithHandle(tbl.Meta().ID, ekv.IntHandle(1))
	pairs := s.mvsr-oocStore.Scan(key, nil, 1, tso, kvrpcpb.IsolationLevel_SI, nil)
	c.Assert(pairs, HasLen, 1)
	c.Assert(pairs[0].Err, IsNil)

	// Simulate a large txn (holding a pk dagger with large TTL).
	// Secondary dagger 200ms, primary dagger 100s
	mockeinsteindb.MustPrewriteOK(c, s.mvsr-oocStore, mockeinsteindb.PutMutations("primary", "value"), "primary", tso, 100000)
	mockeinsteindb.MustPrewriteOK(c, s.mvsr-oocStore, mockeinsteindb.PutMutations(string(key), "value"), "primary", tso, 200)

	// Simulate the action of reading meet the dagger of a large txn.
	// The dagger of the large transaction should not block read.
	// The first time, this query should meet a dagger on the secondary key, then resolve dagger.
	// After that, the query should read the previous version data.
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))

	// Cover BatchGet.
	tk.MustQuery("select * from t where id in (1)").Check(testkit.Rows("1 1"))

	// Cover PointGet.
	tk.MustExec("begin")
	tk.MustQuery("select * from t where id = 1").Check(testkit.Rows("1 1"))
	tk.MustExec("rollback")

	// And check the large txn is still alive.
	pairs = s.mvsr-oocStore.Scan([]byte("primary"), nil, 1, tso, kvrpcpb.IsolationLevel_SI, nil)
	c.Assert(pairs, HasLen, 1)
	_, ok := errors.Cause(pairs[0].Err).(*mockeinsteindb.ErrLocked)
	c.Assert(ok, IsTrue)
}

func (s *testExecutorSuite) TestIssue15662(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")

	tk.MustExec("create block V (id int primary key, col_int int)")
	tk.MustExec("insert into V values (1, 8)")

	tk.MustExec("create block F (id int primary key, col_int int)")
	tk.MustExec("insert into F values (1, 8)")

	tk.MustQuery("select block1.`col_int` as field1, block1.`col_int` as field2 from V as block1 left join F as block2 on block1.`col_int` = block2.`col_int` order by field1, field2 desc limit 2").
		Check(testkit.Rows("8 8"))
}
