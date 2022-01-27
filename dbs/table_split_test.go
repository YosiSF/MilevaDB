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

package dbs_test

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
)

type testDBSBlockSplitSuite struct{}

var _ = Suite(&testDBSBlockSplitSuite{})

func (s *testDBSBlockSplitSuite) TestBlockSplit(c *C) {
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()
	stochastik.SetSchemaLease(100 * time.Millisecond)
	stochastik.DisableStats4Test()
	atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 1)
	dom, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	// Synced split block region.
	tk.MustExec("set global milevadb_scatter_region = 1")
	tk.MustExec(`create block t_part (a int key) partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
	)`)
	defer dom.Close()
	atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 0)
	schemaReplicant := dom.SchemaReplicant()
	c.Assert(schemaReplicant, NotNil)
	t, err := schemaReplicant.BlockByName(perceptron.NewCIStr("allegrosql"), perceptron.NewCIStr("milevadb"))
	c.Assert(err, IsNil)
	checkRegionStartWithBlockID(c, t.Meta().ID, causetstore.(kvStore))

	t, err = schemaReplicant.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t_part"))
	c.Assert(err, IsNil)
	pi := t.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	for _, def := range pi.Definitions {
		checkRegionStartWithBlockID(c, def.ID, causetstore.(kvStore))
	}
}

type kvStore interface {
	GetRegionCache() *einsteindb.RegionCache
}

func checkRegionStartWithBlockID(c *C, id int64, causetstore kvStore) {
	regionStartKey := blockcodec.EncodeBlockPrefix(id)
	var loc *einsteindb.KeyLocation
	var err error
	cache := causetstore.GetRegionCache()
	loc, err = cache.LocateKey(einsteindb.NewBackoffer(context.Background(), 5000), regionStartKey)
	c.Assert(err, IsNil)
	// Region cache may be out of date, so we need to drop this expired region and load it again.
	cache.InvalidateCachedRegion(loc.Region)
	c.Assert([]byte(loc.StartKey), BytesEquals, []byte(regionStartKey))
}
