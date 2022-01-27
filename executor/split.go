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

package executor

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/ekvproto/pkg/metapb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/helper"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

// SplitIndexRegionExec represents a split index regions executor.
type SplitIndexRegionExec struct {
	baseExecutor

	blockInfo      *perceptron.BlockInfo
	partitionNames []perceptron.CIStr
	indexInfo      *perceptron.IndexInfo
	lower          []types.Causet
	upper          []types.Causet
	num            int
	handleDefCauss     core.HandleDefCauss
	valueLists     [][]types.Causet
	splitIdxKeys   [][]byte

	done bool
	splitRegionResult
}

type splitRegionResult struct {
	splitRegions     int
	finishScatterNum int
}

// Open implements the Executor Open interface.
func (e *SplitIndexRegionExec) Open(ctx context.Context) (err error) {
	e.splitIdxKeys, err = e.getSplitIdxKeys()
	return err
}

// Next implements the Executor Next interface.
func (e *SplitIndexRegionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true
	if err := e.splitIndexRegion(ctx); err != nil {
		return err
	}

	appendSplitRegionResultToChunk(chk, e.splitRegions, e.finishScatterNum)
	return nil
}

// checkScatterRegionFinishBackOff is the back off time that used to check if a region has finished scattering before split region timeout.
const checkScatterRegionFinishBackOff = 50

// splitIndexRegion is used to split index regions.
func (e *SplitIndexRegionExec) splitIndexRegion(ctx context.Context) error {
	causetstore := e.ctx.GetStore()
	s, ok := causetstore.(ekv.SplitblockStore)
	if !ok {
		return nil
	}

	start := time.Now()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, e.ctx.GetStochastikVars().GetSplitRegionTimeout())
	defer cancel()
	regionIDs, err := s.SplitRegions(ctxWithTimeout, e.splitIdxKeys, true, &e.blockInfo.ID)
	if err != nil {
		logutil.BgLogger().Warn("split block index region failed",
			zap.String("block", e.blockInfo.Name.L),
			zap.String("index", e.indexInfo.Name.L),
			zap.Error(err))
	}
	e.splitRegions = len(regionIDs)
	if e.splitRegions == 0 {
		return nil
	}

	if !e.ctx.GetStochastikVars().WaitSplitRegionFinish {
		return nil
	}
	e.finishScatterNum = waitScatterRegionFinish(ctxWithTimeout, e.ctx, start, s, regionIDs, e.blockInfo.Name.L, e.indexInfo.Name.L)
	return nil
}

func (e *SplitIndexRegionExec) getSplitIdxKeys() ([][]byte, error) {
	// Split index regions by user specified value lists.
	if len(e.valueLists) > 0 {
		return e.getSplitIdxKeysFromValueList()
	}

	return e.getSplitIdxKeysFromBound()
}

func (e *SplitIndexRegionExec) getSplitIdxKeysFromValueList() (keys [][]byte, err error) {
	pi := e.blockInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, len(e.valueLists)+1)
		return e.getSplitIdxPhysicalKeysFromValueList(e.blockInfo.ID, keys)
	}

	// Split for all block partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, (len(e.valueLists)+1)*len(pi.Definitions))
		for _, p := range pi.Definitions {
			keys, err = e.getSplitIdxPhysicalKeysFromValueList(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified block partitions.
	keys = make([][]byte, 0, (len(e.valueLists)+1)*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := blocks.FindPartitionByName(e.blockInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitIdxPhysicalKeysFromValueList(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalKeysFromValueList(physicalID int64, keys [][]byte) ([][]byte, error) {
	keys = e.getSplitIdxPhysicalStartAndOtherIdxKeys(physicalID, keys)
	index := blocks.NewIndex(physicalID, e.blockInfo, e.indexInfo)
	for _, v := range e.valueLists {
		idxKey, _, err := index.GenIndexKey(e.ctx.GetStochastikVars().StmtCtx, v, ekv.IntHandle(math.MinInt64), nil)
		if err != nil {
			return nil, err
		}
		keys = append(keys, idxKey)
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalStartAndOtherIdxKeys(physicalID int64, keys [][]byte) [][]byte {
	// 1. Split in the start key for the index if the index is not the first index.
	// For the first index, splitting the start key can produce the region [tid, tid_i_1), which is useless.
	if len(e.blockInfo.Indices) > 0 && e.blockInfo.Indices[0].ID != e.indexInfo.ID {
		startKey := blockcodec.EncodeBlockIndexPrefix(physicalID, e.indexInfo.ID)
		keys = append(keys, startKey)
	}

	// 2. Split in the end key.
	endKey := blockcodec.EncodeBlockIndexPrefix(physicalID, e.indexInfo.ID+1)
	keys = append(keys, endKey)
	return keys
}

func (e *SplitIndexRegionExec) getSplitIdxKeysFromBound() (keys [][]byte, err error) {
	pi := e.blockInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, e.num)
		return e.getSplitIdxPhysicalKeysFromBound(e.blockInfo.ID, keys)
	}

	// Split for all block partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, e.num*len(pi.Definitions))
		for _, p := range pi.Definitions {
			keys, err = e.getSplitIdxPhysicalKeysFromBound(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified block partitions.
	keys = make([][]byte, 0, e.num*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := blocks.FindPartitionByName(e.blockInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitIdxPhysicalKeysFromBound(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalKeysFromBound(physicalID int64, keys [][]byte) ([][]byte, error) {
	keys = e.getSplitIdxPhysicalStartAndOtherIdxKeys(physicalID, keys)
	index := blocks.NewIndex(physicalID, e.blockInfo, e.indexInfo)
	// Split index regions by lower, upper value and calculate the step by (upper - lower)/num.
	lowerIdxKey, _, err := index.GenIndexKey(e.ctx.GetStochastikVars().StmtCtx, e.lower, ekv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return nil, err
	}
	// Use math.MinInt64 as handle_id for the upper index key to avoid affecting calculate split point.
	// If use math.MaxInt64 here, test of `TestSplitIndex` will report error.
	upperIdxKey, _, err := index.GenIndexKey(e.ctx.GetStochastikVars().StmtCtx, e.upper, ekv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return nil, err
	}

	if bytes.Compare(lowerIdxKey, upperIdxKey) >= 0 {
		lowerStr, err1 := datumSliceToString(e.lower)
		upperStr, err2 := datumSliceToString(e.upper)
		if err1 != nil || err2 != nil {
			return nil, errors.Errorf("Split index `%v` region lower value %v should less than the upper value %v", e.indexInfo.Name, e.lower, e.upper)
		}
		return nil, errors.Errorf("Split index `%v` region lower value %v should less than the upper value %v", e.indexInfo.Name, lowerStr, upperStr)
	}
	return getValuesList(lowerIdxKey, upperIdxKey, e.num, keys), nil
}

// getValuesList is used to get `num` values between lower and upper value.
// To Simplify the explain, suppose lower and upper value type is int64, and lower=0, upper=100, num=10,
// then calculate the step=(upper-lower)/num=10, then the function should return 0+10, 10+10, 20+10... all together 9 (num-1) values.
// Then the function will return [10,20,30,40,50,60,70,80,90].
// The difference is the value type of upper, lower is []byte, So I use getUint64FromBytes to convert []byte to uint64.
func getValuesList(lower, upper []byte, num int, valuesList [][]byte) [][]byte {
	commonPrefixIdx := longestCommonPrefixLen(lower, upper)
	step := getStepValue(lower[commonPrefixIdx:], upper[commonPrefixIdx:], num)
	startV := getUint64FromBytes(lower[commonPrefixIdx:], 0)
	// To get `num` regions, only need to split `num-1` idx keys.
	buf := make([]byte, 8)
	for i := 0; i < num-1; i++ {
		value := make([]byte, 0, commonPrefixIdx+8)
		value = append(value, lower[:commonPrefixIdx]...)
		startV += step
		binary.BigEndian.PutUint64(buf, startV)
		value = append(value, buf...)
		valuesList = append(valuesList, value)
	}
	return valuesList
}

// longestCommonPrefixLen gets the longest common prefix byte length.
func longestCommonPrefixLen(s1, s2 []byte) int {
	l := mathutil.Min(len(s1), len(s2))
	i := 0
	for ; i < l; i++ {
		if s1[i] != s2[i] {
			break
		}
	}
	return i
}

// getStepValue gets the step of between the lower and upper value. step = (upper-lower)/num.
// Convert byte slice to uint64 first.
func getStepValue(lower, upper []byte, num int) uint64 {
	lowerUint := getUint64FromBytes(lower, 0)
	upperUint := getUint64FromBytes(upper, 0xff)
	return (upperUint - lowerUint) / uint64(num)
}

// getUint64FromBytes gets a uint64 from the `bs` byte slice.
// If len(bs) < 8, then padding with `pad`.
func getUint64FromBytes(bs []byte, pad byte) uint64 {
	buf := bs
	if len(buf) < 8 {
		buf = make([]byte, 0, 8)
		buf = append(buf, bs...)
		for i := len(buf); i < 8; i++ {
			buf = append(buf, pad)
		}
	}
	return binary.BigEndian.Uint64(buf)
}

func datumSliceToString(ds []types.Causet) (string, error) {
	str := "("
	for i, d := range ds {
		s, err := d.ToString()
		if err != nil {
			return str, err
		}
		if i > 0 {
			str += ","
		}
		str += s
	}
	str += ")"
	return str, nil
}

// SplitBlockRegionExec represents a split block regions executor.
type SplitBlockRegionExec struct {
	baseExecutor

	blockInfo      *perceptron.BlockInfo
	partitionNames []perceptron.CIStr
	lower          []types.Causet
	upper          []types.Causet
	num            int
	handleDefCauss     core.HandleDefCauss
	valueLists     [][]types.Causet
	splitKeys      [][]byte

	done bool
	splitRegionResult
}

// Open implements the Executor Open interface.
func (e *SplitBlockRegionExec) Open(ctx context.Context) (err error) {
	e.splitKeys, err = e.getSplitBlockKeys()
	return err
}

// Next implements the Executor Next interface.
func (e *SplitBlockRegionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true

	if err := e.splitBlockRegion(ctx); err != nil {
		return err
	}
	appendSplitRegionResultToChunk(chk, e.splitRegions, e.finishScatterNum)
	return nil
}

func (e *SplitBlockRegionExec) splitBlockRegion(ctx context.Context) error {
	causetstore := e.ctx.GetStore()
	s, ok := causetstore.(ekv.SplitblockStore)
	if !ok {
		return nil
	}

	start := time.Now()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, e.ctx.GetStochastikVars().GetSplitRegionTimeout())
	defer cancel()

	regionIDs, err := s.SplitRegions(ctxWithTimeout, e.splitKeys, true, &e.blockInfo.ID)
	if err != nil {
		logutil.BgLogger().Warn("split block region failed",
			zap.String("block", e.blockInfo.Name.L),
			zap.Error(err))
	}
	e.splitRegions = len(regionIDs)
	if e.splitRegions == 0 {
		return nil
	}

	if !e.ctx.GetStochastikVars().WaitSplitRegionFinish {
		return nil
	}

	e.finishScatterNum = waitScatterRegionFinish(ctxWithTimeout, e.ctx, start, s, regionIDs, e.blockInfo.Name.L, "")
	return nil
}

func waitScatterRegionFinish(ctxWithTimeout context.Context, sctx stochastikctx.Context, startTime time.Time, causetstore ekv.SplitblockStore, regionIDs []uint64, blockName, indexName string) int {
	remainMillisecond := 0
	finishScatterNum := 0
	for _, regionID := range regionIDs {
		if isCtxDone(ctxWithTimeout) {
			// Do not break here for checking remain regions scatter finished with a very short backoff time.
			// Consider this situation -  Regions 1, 2, and 3 are to be split.
			// Region 1 times out before scattering finishes, while Region 2 and Region 3 have finished scattering.
			// In this case, we should return 2 Regions, instead of 0, have finished scattering.
			remainMillisecond = checkScatterRegionFinishBackOff
		} else {
			remainMillisecond = int((sctx.GetStochastikVars().GetSplitRegionTimeout().Seconds() - time.Since(startTime).Seconds()) * 1000)
		}

		err := causetstore.WaitScatterRegionFinish(ctxWithTimeout, regionID, remainMillisecond)
		if err == nil {
			finishScatterNum++
		} else {
			if len(indexName) == 0 {
				logutil.BgLogger().Warn("wait scatter region failed",
					zap.Uint64("regionID", regionID),
					zap.String("block", blockName),
					zap.Error(err))
			} else {
				logutil.BgLogger().Warn("wait scatter region failed",
					zap.Uint64("regionID", regionID),
					zap.String("block", blockName),
					zap.String("index", indexName),
					zap.Error(err))
			}
		}
	}
	return finishScatterNum
}

func appendSplitRegionResultToChunk(chk *chunk.Chunk, totalRegions, finishScatterNum int) {
	chk.AppendInt64(0, int64(totalRegions))
	if finishScatterNum > 0 && totalRegions > 0 {
		chk.AppendFloat64(1, float64(finishScatterNum)/float64(totalRegions))
	} else {
		chk.AppendFloat64(1, float64(0))
	}
}

func isCtxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

var minRegionStepValue = int64(1000)

func (e *SplitBlockRegionExec) getSplitBlockKeys() ([][]byte, error) {
	if len(e.valueLists) > 0 {
		return e.getSplitBlockKeysFromValueList()
	}

	return e.getSplitBlockKeysFromBound()
}

func (e *SplitBlockRegionExec) getSplitBlockKeysFromValueList() ([][]byte, error) {
	var keys [][]byte
	pi := e.blockInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, len(e.valueLists))
		return e.getSplitBlockPhysicalKeysFromValueList(e.blockInfo.ID, keys)
	}

	// Split for all block partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, len(e.valueLists)*len(pi.Definitions))
		for _, p := range pi.Definitions {
			var err error
			keys, err = e.getSplitBlockPhysicalKeysFromValueList(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified block partitions.
	keys = make([][]byte, 0, len(e.valueLists)*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := blocks.FindPartitionByName(e.blockInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitBlockPhysicalKeysFromValueList(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitBlockRegionExec) getSplitBlockPhysicalKeysFromValueList(physicalID int64, keys [][]byte) ([][]byte, error) {
	recordPrefix := blockcodec.GenBlockRecordPrefix(physicalID)
	for _, v := range e.valueLists {
		handle, err := e.handleDefCauss.BuildHandleByCausets(v)
		if err != nil {
			return nil, err
		}
		key := blockcodec.EncodeRecordKey(recordPrefix, handle)
		keys = append(keys, key)
	}
	return keys, nil
}

func (e *SplitBlockRegionExec) getSplitBlockKeysFromBound() ([][]byte, error) {
	var keys [][]byte
	pi := e.blockInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, e.num)
		return e.getSplitBlockPhysicalKeysFromBound(e.blockInfo.ID, keys)
	}

	// Split for all block partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, e.num*len(pi.Definitions))
		for _, p := range pi.Definitions {
			var err error
			keys, err = e.getSplitBlockPhysicalKeysFromBound(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified block partitions.
	keys = make([][]byte, 0, e.num*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := blocks.FindPartitionByName(e.blockInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitBlockPhysicalKeysFromBound(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitBlockRegionExec) calculateIntBoundValue() (lowerValue int64, step int64, err error) {
	isUnsigned := false
	if e.blockInfo.PKIsHandle {
		if pkDefCaus := e.blockInfo.GetPkDefCausInfo(); pkDefCaus != nil {
			isUnsigned = allegrosql.HasUnsignedFlag(pkDefCaus.Flag)
		}
	}
	if isUnsigned {
		lowerRecordID := e.lower[0].GetUint64()
		upperRecordID := e.upper[0].GetUint64()
		if upperRecordID <= lowerRecordID {
			return 0, 0, errors.Errorf("Split block `%s` region lower value %v should less than the upper value %v", e.blockInfo.Name, lowerRecordID, upperRecordID)
		}
		step = int64((upperRecordID - lowerRecordID) / uint64(e.num))
		lowerValue = int64(lowerRecordID)
	} else {
		lowerRecordID := e.lower[0].GetInt64()
		upperRecordID := e.upper[0].GetInt64()
		if upperRecordID <= lowerRecordID {
			return 0, 0, errors.Errorf("Split block `%s` region lower value %v should less than the upper value %v", e.blockInfo.Name, lowerRecordID, upperRecordID)
		}
		step = (upperRecordID - lowerRecordID) / int64(e.num)
		lowerValue = lowerRecordID
	}
	if step < minRegionStepValue {
		return 0, 0, errors.Errorf("Split block `%s` region step value should more than %v, step %v is invalid", e.blockInfo.Name, minRegionStepValue, step)
	}
	return lowerValue, step, nil
}

func (e *SplitBlockRegionExec) getSplitBlockPhysicalKeysFromBound(physicalID int64, keys [][]byte) ([][]byte, error) {
	recordPrefix := blockcodec.GenBlockRecordPrefix(physicalID)
	// Split a separate region for index.
	containsIndex := len(e.blockInfo.Indices) > 0 && !(e.blockInfo.IsCommonHandle && len(e.blockInfo.Indices) == 1)
	if containsIndex {
		keys = append(keys, recordPrefix)
	}

	if e.handleDefCauss.IsInt() {
		low, step, err := e.calculateIntBoundValue()
		if err != nil {
			return nil, err
		}
		recordID := low
		for i := 1; i < e.num; i++ {
			recordID += step
			key := blockcodec.EncodeRecordKey(recordPrefix, ekv.IntHandle(recordID))
			keys = append(keys, key)
		}
		return keys, nil
	}
	lowerHandle, err := e.handleDefCauss.BuildHandleByCausets(e.lower)
	if err != nil {
		return nil, err
	}
	upperHandle, err := e.handleDefCauss.BuildHandleByCausets(e.upper)
	if err != nil {
		return nil, err
	}
	if lowerHandle.Compare(upperHandle) >= 0 {
		lowerStr, err1 := datumSliceToString(e.lower)
		upperStr, err2 := datumSliceToString(e.upper)
		if err1 != nil || err2 != nil {
			return nil, errors.Errorf("Split block `%v` region lower value %v should less than the upper value %v",
				e.blockInfo.Name.O, e.lower, e.upper)
		}
		return nil, errors.Errorf("Split block `%v` region lower value %v should less than the upper value %v",
			e.blockInfo.Name.O, lowerStr, upperStr)
	}
	low := blockcodec.EncodeRecordKey(recordPrefix, lowerHandle)
	up := blockcodec.EncodeRecordKey(recordPrefix, upperHandle)
	return getValuesList(low, up, e.num, keys), nil
}

// RegionMeta contains a region's peer detail
type regionMeta struct {
	region          *metapb.Region
	leaderID        uint64
	storeID         uint64 // storeID is the causetstore ID of the leader region.
	start           string
	end             string
	scattering      bool
	writtenBytes    int64
	readBytes       int64
	approximateSize int64
	approximateKeys int64
}

func getPhysicalBlockRegions(physicalBlockID int64, blockInfo *perceptron.BlockInfo, einsteindbStore einsteindb.CausetStorage, s ekv.SplitblockStore, uniqueRegionMap map[uint64]struct{}) ([]regionMeta, error) {
	if uniqueRegionMap == nil {
		uniqueRegionMap = make(map[uint64]struct{})
	}
	// for record
	startKey, endKey := blockcodec.GetBlockHandleKeyRange(physicalBlockID)
	regionCache := einsteindbStore.GetRegionCache()
	recordRegionMetas, err := regionCache.LoadRegionsInKeyRange(einsteindb.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
	if err != nil {
		return nil, err
	}
	recordPrefix := blockcodec.GenBlockRecordPrefix(physicalBlockID)
	blockPrefix := blockcodec.GenBlockPrefix(physicalBlockID)
	recordRegions, err := getRegionMeta(einsteindbStore, recordRegionMetas, uniqueRegionMap, blockPrefix, recordPrefix, nil, physicalBlockID, 0)
	if err != nil {
		return nil, err
	}

	regions := recordRegions
	// for indices
	for _, index := range blockInfo.Indices {
		if index.State != perceptron.StatePublic {
			continue
		}
		startKey, endKey := blockcodec.GetBlockIndexKeyRange(physicalBlockID, index.ID)
		regionMetas, err := regionCache.LoadRegionsInKeyRange(einsteindb.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
		if err != nil {
			return nil, err
		}
		indexPrefix := blockcodec.EncodeBlockIndexPrefix(physicalBlockID, index.ID)
		indexRegions, err := getRegionMeta(einsteindbStore, regionMetas, uniqueRegionMap, blockPrefix, recordPrefix, indexPrefix, physicalBlockID, index.ID)
		if err != nil {
			return nil, err
		}
		regions = append(regions, indexRegions...)
	}
	err = checkRegionsStatus(s, regions)
	if err != nil {
		return nil, err
	}
	return regions, nil
}

func getPhysicalIndexRegions(physicalBlockID int64, indexInfo *perceptron.IndexInfo, einsteindbStore einsteindb.CausetStorage, s ekv.SplitblockStore, uniqueRegionMap map[uint64]struct{}) ([]regionMeta, error) {
	if uniqueRegionMap == nil {
		uniqueRegionMap = make(map[uint64]struct{})
	}

	startKey, endKey := blockcodec.GetBlockIndexKeyRange(physicalBlockID, indexInfo.ID)
	regionCache := einsteindbStore.GetRegionCache()
	regions, err := regionCache.LoadRegionsInKeyRange(einsteindb.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
	if err != nil {
		return nil, err
	}
	recordPrefix := blockcodec.GenBlockRecordPrefix(physicalBlockID)
	blockPrefix := blockcodec.GenBlockPrefix(physicalBlockID)
	indexPrefix := blockcodec.EncodeBlockIndexPrefix(physicalBlockID, indexInfo.ID)
	indexRegions, err := getRegionMeta(einsteindbStore, regions, uniqueRegionMap, blockPrefix, recordPrefix, indexPrefix, physicalBlockID, indexInfo.ID)
	if err != nil {
		return nil, err
	}
	err = checkRegionsStatus(s, indexRegions)
	if err != nil {
		return nil, err
	}
	return indexRegions, nil
}

func checkRegionsStatus(causetstore ekv.SplitblockStore, regions []regionMeta) error {
	for i := range regions {
		scattering, err := causetstore.CheckRegionInScattering(regions[i].region.Id)
		if err != nil {
			return err
		}
		regions[i].scattering = scattering
	}
	return nil
}

func decodeRegionsKey(regions []regionMeta, blockPrefix, recordPrefix, indexPrefix []byte, physicalBlockID, indexID int64) {
	d := &regionKeyDecoder{
		physicalBlockID: physicalBlockID,
		blockPrefix:     blockPrefix,
		recordPrefix:    recordPrefix,
		indexPrefix:     indexPrefix,
		indexID:         indexID,
	}
	for i := range regions {
		regions[i].start = d.decodeRegionKey(regions[i].region.StartKey)
		regions[i].end = d.decodeRegionKey(regions[i].region.EndKey)
	}
}

type regionKeyDecoder struct {
	physicalBlockID int64
	blockPrefix     []byte
	recordPrefix    []byte
	indexPrefix     []byte
	indexID         int64
}

func (d *regionKeyDecoder) decodeRegionKey(key []byte) string {
	if len(d.indexPrefix) > 0 && bytes.HasPrefix(key, d.indexPrefix) {
		return fmt.Sprintf("t_%d_i_%d_%x", d.physicalBlockID, d.indexID, key[len(d.indexPrefix):])
	} else if len(d.recordPrefix) > 0 && bytes.HasPrefix(key, d.recordPrefix) {
		if len(d.recordPrefix) == len(key) {
			return fmt.Sprintf("t_%d_r", d.physicalBlockID)
		}
		isIntHandle := len(key)-len(d.recordPrefix) == 8
		if isIntHandle {
			_, handle, err := codec.DecodeInt(key[len(d.recordPrefix):])
			if err == nil {
				return fmt.Sprintf("t_%d_r_%d", d.physicalBlockID, handle)
			}
		}
		return fmt.Sprintf("t_%d_r_%x", d.physicalBlockID, key[len(d.recordPrefix):])
	}
	if len(d.blockPrefix) > 0 && bytes.HasPrefix(key, d.blockPrefix) {
		key = key[len(d.blockPrefix):]
		// Has index prefix.
		if !bytes.HasPrefix(key, []byte("_i")) {
			return fmt.Sprintf("t_%d_%x", d.physicalBlockID, key)
		}
		key = key[2:]
		// try to decode index ID.
		if _, indexID, err := codec.DecodeInt(key); err == nil {
			return fmt.Sprintf("t_%d_i_%d_%x", d.physicalBlockID, indexID, key[8:])
		}
		return fmt.Sprintf("t_%d_i__%x", d.physicalBlockID, key)
	}
	// Has block prefix.
	if bytes.HasPrefix(key, []byte("t")) {
		key = key[1:]
		// try to decode block ID.
		if _, blockID, err := codec.DecodeInt(key); err == nil {
			return fmt.Sprintf("t_%d_%x", blockID, key[8:])
		}
		return fmt.Sprintf("t_%x", key)
	}
	return fmt.Sprintf("%x", key)
}

func getRegionMeta(einsteindbStore einsteindb.CausetStorage, regionMetas []*einsteindb.Region, uniqueRegionMap map[uint64]struct{}, blockPrefix, recordPrefix, indexPrefix []byte, physicalBlockID, indexID int64) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(regionMetas))
	for _, r := range regionMetas {
		if _, ok := uniqueRegionMap[r.GetID()]; ok {
			continue
		}
		uniqueRegionMap[r.GetID()] = struct{}{}
		regions = append(regions, regionMeta{
			region:   r.GetMeta(),
			leaderID: r.GetLeaderPeerID(),
			storeID:  r.GetLeaderStoreID(),
		})
	}
	regions, err := getRegionInfo(einsteindbStore, regions)
	if err != nil {
		return regions, err
	}
	decodeRegionsKey(regions, blockPrefix, recordPrefix, indexPrefix, physicalBlockID, indexID)
	return regions, nil
}

func getRegionInfo(causetstore einsteindb.CausetStorage, regions []regionMeta) ([]regionMeta, error) {
	// check fidel server exists.
	etcd, ok := causetstore.(einsteindb.EtcdBackend)
	if !ok {
		return regions, nil
	}
	FIDelHosts, err := etcd.EtcdAddrs()
	if err != nil {
		return regions, err
	}
	if len(FIDelHosts) == 0 {
		return regions, nil
	}
	einsteindbHelper := &helper.Helper{
		CausetStore:       causetstore,
		RegionCache: causetstore.GetRegionCache(),
	}
	for i := range regions {
		regionInfo, err := einsteindbHelper.GetRegionInfoByID(regions[i].region.Id)
		if err != nil {
			return nil, err
		}
		regions[i].writtenBytes = regionInfo.WrittenBytes
		regions[i].readBytes = regionInfo.ReadBytes
		regions[i].approximateSize = regionInfo.ApproximateSize
		regions[i].approximateKeys = regionInfo.ApproximateKeys
	}
	return regions, nil
}
