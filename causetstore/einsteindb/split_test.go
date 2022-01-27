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

package einsteindb

import (
	"context"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/milevadb/ekv"
)

type testSplitSuite struct {
	OneByOneSuite
	cluster     cluster.Cluster
	causetstore *einsteindbStore
	bo          *Backoffer
}

var _ = Suite(&testSplitSuite{})

func (s *testSplitSuite) SetUpTest(c *C) {
	client, cluster, FIDelClient, err := mockeinsteindb.NewEinsteinDBAndFIDelClient("")
	c.Assert(err, IsNil)
	mockeinsteindb.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	causetstore, err := NewTestEinsteinDBStore(client, FIDelClient, nil, nil, 0)
	c.Assert(err, IsNil)

	// TODO: make this possible
	// causetstore, err := mockstore.NewMockStore(
	// 	mockstore.WithClusterInspector(func(c cluster.Cluster) {
	// 		mockstore.BootstrapWithSingleStore(c)
	// 		s.cluster = c
	// 	}),
	// )
	// c.Assert(err, IsNil)
	s.causetstore = causetstore.(*einsteindbStore)
	s.bo = NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testSplitSuite) begin(c *C) *einsteindbTxn {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	return txn.(*einsteindbTxn)
}

func (s *testSplitSuite) split(c *C, regionID uint64, key []byte) {
	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(regionID, newRegionID, key, []uint64{peerID}, peerID)
}

func (s *testSplitSuite) TestSplitBatchGet(c *C) {
	loc, err := s.causetstore.regionCache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)

	txn := s.begin(c)
	snapshot := newEinsteinDBSnapshot(s.causetstore, ekv.Version{Ver: txn.StartTS()}, 0)

	keys := [][]byte{{'a'}, {'b'}, {'c'}}
	_, region, err := s.causetstore.regionCache.GroupKeysByRegion(s.bo, keys, nil)
	c.Assert(err, IsNil)
	batch := batchKeys{
		region: region,
		keys:   keys,
	}

	s.split(c, loc.Region.id, []byte("b"))
	s.causetstore.regionCache.InvalidateCachedRegion(loc.Region)

	// mockeinsteindb will panic if it meets a not-in-region key.
	err = snapshot.batchGetSingleRegion(s.bo, batch, func([]byte, []byte) {})
	c.Assert(err, IsNil)
}

func (s *testSplitSuite) TestStaleEpoch(c *C) {
	mockFIDelClient := &mockFIDelClient{client: s.causetstore.regionCache.FIDelClient}
	s.causetstore.regionCache.FIDelClient = mockFIDelClient

	loc, err := s.causetstore.regionCache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)

	txn := s.begin(c)
	err = txn.Set([]byte("a"), []byte("a"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("c"), []byte("c"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Initiate a split and disable the FIDel client. If it still works, the
	// new region is uFIDelated from kvrpc.
	s.split(c, loc.Region.id, []byte("b"))
	mockFIDelClient.disable()

	txn = s.begin(c)
	_, err = txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	_, err = txn.Get(context.TODO(), []byte("c"))
	c.Assert(err, IsNil)
}
