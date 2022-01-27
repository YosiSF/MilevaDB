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

package dbs

import (
	"context"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"go.uber.org/zap"
)

func splitPartitionBlockRegion(ctx stochastikctx.Context, causetstore ekv.SplitblockStore, tbInfo *perceptron.BlockInfo, pi *perceptron.PartitionInfo, scatter bool) {
	// Max partition count is 4096, should we sample and just choose some of the partition to split?
	regionIDs := make([]uint64, 0, len(pi.Definitions))
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ctx.GetStochastikVars().GetSplitRegionTimeout())
	defer cancel()
	if tbInfo.ShardRowIDBits > 0 && tbInfo.PreSplitRegions > 0 {
		for _, def := range pi.Definitions {
			regionIDs = append(regionIDs, preSplitPhysicalBlockByShardRowID(ctxWithTimeout, causetstore, tbInfo, def.ID, scatter)...)
		}
	} else {
		for _, def := range pi.Definitions {
			regionIDs = append(regionIDs, splitRecordRegion(ctxWithTimeout, causetstore, def.ID, scatter))
		}
	}
	if scatter {
		waitScatterRegionFinish(ctxWithTimeout, causetstore, regionIDs...)
	}
}

func splitBlockRegion(ctx stochastikctx.Context, causetstore ekv.SplitblockStore, tbInfo *perceptron.BlockInfo, scatter bool) {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ctx.GetStochastikVars().GetSplitRegionTimeout())
	defer cancel()
	var regionIDs []uint64
	if tbInfo.ShardRowIDBits > 0 && tbInfo.PreSplitRegions > 0 {
		regionIDs = preSplitPhysicalBlockByShardRowID(ctxWithTimeout, causetstore, tbInfo, tbInfo.ID, scatter)
	} else {
		regionIDs = append(regionIDs, splitRecordRegion(ctxWithTimeout, causetstore, tbInfo.ID, scatter))
	}
	if scatter {
		waitScatterRegionFinish(ctxWithTimeout, causetstore, regionIDs...)
	}
}

func preSplitPhysicalBlockByShardRowID(ctx context.Context, causetstore ekv.SplitblockStore, tbInfo *perceptron.BlockInfo, physicalID int64, scatter bool) []uint64 {
	// Example:
	// ShardRowIDBits = 4
	// PreSplitRegions = 2
	//
	// then will pre-split 2^2 = 4 regions.
	//
	// in this code:
	// max   = 1 << tblInfo.ShardRowIDBits = 16
	// step := int64(1 << (tblInfo.ShardRowIDBits - tblInfo.PreSplitRegions)) = 1 << (4-2) = 4;
	//
	// then split regionID is below:
	// 4  << 59 = 2305843009213693952
	// 8  << 59 = 4611686018427387904
	// 12 << 59 = 6917529027641081856
	//
	// The 4 pre-split regions range is below:
	// 0                   ~ 2305843009213693952
	// 2305843009213693952 ~ 4611686018427387904
	// 4611686018427387904 ~ 6917529027641081856
	// 6917529027641081856 ~ 9223372036854775807 ( (1 << 63) - 1 )
	//
	// And the max _milevadb_rowid is 9223372036854775807, it won't be negative number.

	// Split block region.
	step := int64(1 << (tbInfo.ShardRowIDBits - tbInfo.PreSplitRegions))
	max := int64(1 << tbInfo.ShardRowIDBits)
	splitBlockKeys := make([][]byte, 0, 1<<(tbInfo.PreSplitRegions))
	splitBlockKeys = append(splitBlockKeys, blockcodec.GenBlockPrefix(physicalID))
	for p := step; p < max; p += step {
		recordID := p << (64 - tbInfo.ShardRowIDBits - 1)
		recordPrefix := blockcodec.GenBlockRecordPrefix(physicalID)
		key := blockcodec.EncodeRecordKey(recordPrefix, ekv.IntHandle(recordID))
		splitBlockKeys = append(splitBlockKeys, key)
	}
	var err error
	regionIDs, err := causetstore.SplitRegions(ctx, splitBlockKeys, scatter, &tbInfo.ID)
	if err != nil {
		logutil.BgLogger().Warn("[dbs] pre split some block regions failed",
			zap.Stringer("block", tbInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	regionIDs = append(regionIDs, splitIndexRegion(causetstore, tbInfo, scatter)...)
	return regionIDs
}

func splitRecordRegion(ctx context.Context, causetstore ekv.SplitblockStore, blockID int64, scatter bool) uint64 {
	blockStartKey := blockcodec.GenBlockPrefix(blockID)
	regionIDs, err := causetstore.SplitRegions(ctx, [][]byte{blockStartKey}, scatter, &blockID)
	if err != nil {
		// It will be automatically split by EinsteinDB later.
		logutil.BgLogger().Warn("[dbs] split block region failed", zap.Error(err))
	}
	if len(regionIDs) == 1 {
		return regionIDs[0]
	}
	return 0
}

func splitIndexRegion(causetstore ekv.SplitblockStore, tblInfo *perceptron.BlockInfo, scatter bool) []uint64 {
	splitKeys := make([][]byte, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		indexPrefix := blockcodec.EncodeBlockIndexPrefix(tblInfo.ID, idx.ID)
		splitKeys = append(splitKeys, indexPrefix)
	}
	regionIDs, err := causetstore.SplitRegions(context.Background(), splitKeys, scatter, &tblInfo.ID)
	if err != nil {
		logutil.BgLogger().Warn("[dbs] pre split some block index regions failed",
			zap.Stringer("block", tblInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	return regionIDs
}

func waitScatterRegionFinish(ctx context.Context, causetstore ekv.SplitblockStore, regionIDs ...uint64) {
	for _, regionID := range regionIDs {
		err := causetstore.WaitScatterRegionFinish(ctx, regionID, 0)
		if err != nil {
			logutil.BgLogger().Warn("[dbs] wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
			// We don't break for FIDelError because it may caused by ScatterRegion request failed.
			if _, ok := errors.Cause(err).(*einsteindb.FIDelError); !ok {
				break
			}
		}
	}
}
