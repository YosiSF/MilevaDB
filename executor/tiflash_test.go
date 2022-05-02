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
	"fmt"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/metapb"
)

type tiflashTestSuite struct {
	cluster     cluster.Cluster
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	*berolinaAllegroSQL.berolinaAllegroSQL
	ctx *mock.Context
}

func (s *tiflashTestSuite) SetUpSuite(c *C) {
	var err error
	s.causetstore, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockCluster := c.(*mockeinsteindb.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < 2 {
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

	c.Assert(err, IsNil)

	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()

	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	s.dom.SetStatsUFIDelating(true)
}

func (s *tiflashTestSuite) TestReadPartitionBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t(a int not null primary key, b int not null) partition by hash(a) partitions 2")
	tk.MustExec("alter block t set tiflash replica 1")
	tb := testGetBlockByName(c, tk.Se, "test", "t")
	err := petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("insert into t values(2,0)")
	tk.MustExec("insert into t values(3,0)")
	tk.MustExec("set @@stochastik.milevadb_isolation_read_engines=\"tiflash\"")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Events("3"))
	tk.MustQuery("select * from t order by a").Check(testkit.Events("1 0", "2 0", "3 0"))

	// test union scan
	tk.MustExec("begin")
	tk.MustExec("insert into t values(4,0)")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Events("4"))
	tk.MustExec("insert into t values(5,0)")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Events("5"))
	tk.MustExec("insert into t values(6,0)")
	tk.MustQuery("select /*+ STREAM_AGG() */ count(*) from t").Check(testkit.Events("6"))
}
