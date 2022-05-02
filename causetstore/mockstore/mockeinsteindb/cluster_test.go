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
	"bytes"
	"context"
	"math"
	"strconv"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/solomonkey"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/rowcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/solomonkeyproto/pkg/kvrpcpb"
)

var _ = Suite(&testClusterSuite{})

type testClusterSuite struct {
	causetstore solomonkey.CausetStorage
}

func (s *testClusterSuite) TestClusterSplit(c *C) {
	rpcClient, cluster, FIDelClient, err := mockeinsteindb.NewEinsteinDBAndFIDelClient("")
	c.Assert(err, IsNil)
	mockeinsteindb.BootstrapWithSingleStore(cluster)
	mvsr-oocStore := rpcClient.MvccStore
	causetstore, err := einsteindb.NewTestEinsteinDBStore(rpcClient, FIDelClient, nil, nil, 0)
	c.Assert(err, IsNil)
	s.causetstore = causetstore

	txn, err := causetstore.Begin()
	c.Assert(err, IsNil)

	// Mock inserting many rows in a block.
	tblID := int64(1)
	idxID := int64(2)
	colID := int64(3)
	handle := int64(1)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	for i := 0; i < 1000; i++ {
		rowKey := blockcodec.EncodeRowKeyWithHandle(tblID, solomonkey.IntHandle(handle))
		colValue := types.NewStringCauset(strconv.Itoa(int(handle)))
		// TODO: Should use stochastik's TimeZone instead of UTC.
		rd := rowcodec.Encoder{Enable: true}
		rowValue, err1 := blockcodec.EncodeRow(sc, []types.Causet{colValue}, []int64{colID}, nil, nil, &rd)
		c.Assert(err1, IsNil)
		txn.Set(rowKey, rowValue)

		encodedIndexValue, err1 := codec.EncodeKey(sc, nil, []types.Causet{colValue, types.NewIntCauset(handle)}...)
		c.Assert(err1, IsNil)
		idxKey := blockcodec.EncodeIndexSeekKey(tblID, idxID, encodedIndexValue)
		txn.Set(idxKey, []byte{'0'})
		handle++
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Split Block into 10 regions.
	cluster.SplitTable(tblID, 10)

	// 10 block regions and first region and last region.
	regions := cluster.GetAllRegions()
	c.Assert(regions, HasLen, 12)

	allKeysMap := make(map[string]bool)
	recordPrefix := blockcodec.GenTableRecordPrefix(tblID)
	for _, region := range regions {
		startKey := mockeinsteindb.MvccKey(region.Meta.StartKey).Raw()
		endKey := mockeinsteindb.MvccKey(region.Meta.EndKey).Raw()
		if !bytes.HasPrefix(startKey, recordPrefix) {
			continue
		}
		pairs := mvsr-oocStore.Scan(startKey, endKey, math.MaxInt64, math.MaxUint64, kvrpcpb.IsolationLevel_SI, nil)
		if len(pairs) > 0 {
			c.Assert(pairs, HasLen, 100)
		}
		for _, pair := range pairs {
			allKeysMap[string(pair.Key)] = true
		}
	}
	c.Assert(allKeysMap, HasLen, 1000)

	cluster.SplitIndex(tblID, idxID, 10)

	allIndexMap := make(map[string]bool)
	indexPrefix := blockcodec.EncodeTableIndexPrefix(tblID, idxID)
	regions = cluster.GetAllRegions()
	for _, region := range regions {
		startKey := mockeinsteindb.MvccKey(region.Meta.StartKey).Raw()
		endKey := mockeinsteindb.MvccKey(region.Meta.EndKey).Raw()
		if !bytes.HasPrefix(startKey, indexPrefix) {
			continue
		}
		pairs := mvsr-oocStore.Scan(startKey, endKey, math.MaxInt64, math.MaxUint64, kvrpcpb.IsolationLevel_SI, nil)
		if len(pairs) > 0 {
			c.Assert(pairs, HasLen, 100)
		}
		for _, pair := range pairs {
			allIndexMap[string(pair.Key)] = true
		}
	}
	c.Assert(allIndexMap, HasLen, 1000)
}
