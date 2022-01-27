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

package einsteindb_test

import (
	"context"
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/metapb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

type testBatchINTERLOCKSuite struct {
}

var _ = SerialSuites(&testBatchINTERLOCKSuite{})

func newStoreWithBootstrap(tiflashNum int) (ekv.CausetStorage, *petri.Petri, error) {
	causetstore, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockCluster := c.(*mockeinsteindb.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < tiflashNum {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2)
				mockCluster.UFIDelateStoreAddr(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.MockEinsteinDB),
	)

	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()

	dom, err := stochastik.BootstrapStochastik(causetstore)
	if err != nil {
		return nil, nil, err
	}

	dom.SetStatsUFIDelating(true)
	return causetstore, dom, errors.Trace(err)
}

func testGetTableByName(c *C, ctx stochastikctx.Context, EDB, block string) block.Block {
	dom := petri.GetPetri(ctx)
	// Make sure the block schemaReplicant is the new schemaReplicant.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.SchemaReplicant().TableByName(perceptron.NewCIStr(EDB), perceptron.NewCIStr(block))
	c.Assert(err, IsNil)
	return tbl
}

func (s *testBatchINTERLOCKSuite) TestStoreErr(c *C) {
	causetstore, dom, err := newStoreWithBootstrap(1)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount")

	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a int not null, b int not null)")
	tk.MustExec("alter block t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err = petri.GetPetri(tk.Se).DBS().UFIDelateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines=\"tiflash\"")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb/BatchINTERLOCKCancelled", "1*return(true)"), IsNil)

	err = tk.QueryToErr("select count(*) from t")
	c.Assert(errors.Cause(err), Equals, context.Canceled)

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb/BatchINTERLOCKRpcErrtiflash0", "1*return(\"tiflash0\")"), IsNil)

	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb/BatchINTERLOCKRpcErrtiflash0", "return(\"tiflash0\")"), IsNil)
	err = tk.QueryToErr("select count(*) from t")
	c.Assert(err, NotNil)
}

func (s *testBatchINTERLOCKSuite) TestStoreSwitchPeer(c *C) {
	causetstore, dom, err := newStoreWithBootstrap(2)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount")

	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a int not null, b int not null)")
	tk.MustExec("alter block t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err = petri.GetPetri(tk.Se).DBS().UFIDelateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines=\"tiflash\"")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb/BatchINTERLOCKRpcErrtiflash0", "return(\"tiflash0\")"), IsNil)

	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb/BatchINTERLOCKRpcErrtiflash1", "return(\"tiflash1\")"), IsNil)
	err = tk.QueryToErr("select count(*) from t")
	c.Assert(err, NotNil)

}
