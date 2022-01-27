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

package helper

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/FIDelapi"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
)

// Helper is a midbseware to get some information from einsteindb/fidel. It can be used for MilevaDB's http api or mem block.
type Helper struct {
	CausetStore einsteindb.CausetStorage
	RegionCache *einsteindb.RegionCache
}

// NewHelper get a Helper from CausetStorage
func NewHelper(causetstore einsteindb.CausetStorage) *Helper {
	return &Helper{
		CausetStore: causetstore,
		RegionCache: causetstore.GetRegionCache(),
	}
}

// GetMvccByEncodedKey get the MVCC value by the specific encoded key.
func (h *Helper) GetMvccByEncodedKey(encodedKey ekv.Key) (*kvrpcpb.MvccGetByKeyResponse, error) {
	keyLocation, err := h.RegionCache.LocateKey(einsteindb.NewBackofferWithVars(context.Background(), 500, nil), encodedKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	einsteindbReq := einsteindbrpc.NewRequest(einsteindbrpc.CmdMvccGetByKey, &kvrpcpb.MvccGetByKeyRequest{Key: encodedKey})
	kvResp, err := h.CausetStore.SendReq(einsteindb.NewBackofferWithVars(context.Background(), 500, nil), einsteindbReq, keyLocation.Region, time.Minute)
	if err != nil {
		logutil.BgLogger().Info("get MVCC by encoded key failed",
			zap.Stringer("encodeKey", encodedKey),
			zap.Reflect("region", keyLocation.Region),
			zap.Stringer("startKey", keyLocation.StartKey),
			zap.Stringer("endKey", keyLocation.EndKey),
			zap.Reflect("kvResp", kvResp),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	return kvResp.Resp.(*kvrpcpb.MvccGetByKeyResponse), nil
}

// StoreHotRegionInfos records all hog region stores.
// it's the response of FIDel.
type StoreHotRegionInfos struct {
	AsPeer   map[uint64]*HotRegionsStat `json:"as_peer"`
	AsLeader map[uint64]*HotRegionsStat `json:"as_leader"`
}

// HotRegionsStat records echo causetstore's hot region.
// it's the response of FIDel.
type HotRegionsStat struct {
	RegionsStat []RegionStat `json:"statistics"`
}

// RegionStat records each hot region's statistics
// it's the response of FIDel.
type RegionStat struct {
	RegionID  uint64  `json:"region_id"`
	FlowBytes float64 `json:"flow_bytes"`
	HotDegree int     `json:"hot_degree"`
}

// RegionMetric presents the final metric output entry.
type RegionMetric struct {
	FlowBytes    uint64 `json:"flow_bytes"`
	MaxHotDegree int    `json:"max_hot_degree"`
	Count        int    `json:"region_count"`
}

// ScrapeHotInfo gets the needed hot region information by the url given.
func (h *Helper) ScrapeHotInfo(rw string, allSchemas []*perceptron.DBInfo) ([]HotBlockIndex, error) {
	regionMetrics, err := h.FetchHotRegion(rw)
	if err != nil {
		return nil, err
	}
	return h.FetchRegionBlockIndex(regionMetrics, allSchemas)
}

// FetchHotRegion fetches the hot region information from FIDel's http api.
func (h *Helper) FetchHotRegion(rw string) (map[uint64]RegionMetric, error) {
	etcd, ok := h.CausetStore.(einsteindb.EtcdBackend)
	if !ok {
		return nil, errors.WithStack(errors.New("not implemented"))
	}
	FIDelHosts, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(FIDelHosts) == 0 {
		return nil, errors.New("fidel unavailable")
	}
	req, err := http.NewRequest("GET", soliton.InternalHTTPSchema()+"://"+FIDelHosts[0]+rw, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := soliton.InternalHTTPClient().Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()
	var regionResp StoreHotRegionInfos
	err = json.NewDecoder(resp.Body).Decode(&regionResp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metricCnt := 0
	for _, hotRegions := range regionResp.AsLeader {
		metricCnt += len(hotRegions.RegionsStat)
	}
	metric := make(map[uint64]RegionMetric, metricCnt)
	for _, hotRegions := range regionResp.AsLeader {
		for _, region := range hotRegions.RegionsStat {
			metric[region.RegionID] = RegionMetric{FlowBytes: uint64(region.FlowBytes), MaxHotDegree: region.HotDegree}
		}
	}
	return metric, nil
}

// TblIndex stores the things to index one block.
type TblIndex struct {
	DbName    string
	BlockName string
	BlockID   int64
	IndexName string
	IndexID   int64
}

// FrameItem includes a index's or record's meta data with block's info.
type FrameItem struct {
	DBName      string   `json:"db_name"`
	BlockName   string   `json:"block_name"`
	BlockID     int64    `json:"block_id"`
	IsRecord    bool     `json:"is_record"`
	RecordID    int64    `json:"record_id,omitempty"`
	IndexName   string   `json:"index_name,omitempty"`
	IndexID     int64    `json:"index_id,omitempty"`
	IndexValues []string `json:"index_values,omitempty"`
}

// RegionFrameRange contains a frame range info which the region covered.
type RegionFrameRange struct {
	First  *FrameItem              // start frame of the region
	Last   *FrameItem              // end frame of the region
	region *einsteindb.KeyLocation // the region
}

// HotBlockIndex contains region and its block/index info.
type HotBlockIndex struct {
	RegionID     uint64        `json:"region_id"`
	RegionMetric *RegionMetric `json:"region_metric"`
	DbName       string        `json:"db_name"`
	BlockName    string        `json:"block_name"`
	BlockID      int64         `json:"block_id"`
	IndexName    string        `json:"index_name"`
	IndexID      int64         `json:"index_id"`
}

// FetchRegionBlockIndex constructs a map that maps a block to its hot region information by the given raw hot RegionMetric metrics.
func (h *Helper) FetchRegionBlockIndex(metrics map[uint64]RegionMetric, allSchemas []*perceptron.DBInfo) ([]HotBlockIndex, error) {
	hotBlocks := make([]HotBlockIndex, 0, len(metrics))
	for regionID, regionMetric := range metrics {
		t := HotBlockIndex{RegionID: regionID, RegionMetric: &regionMetric}
		region, err := h.RegionCache.LocateRegionByID(einsteindb.NewBackofferWithVars(context.Background(), 500, nil), regionID)
		if err != nil {
			logutil.BgLogger().Error("locate region failed", zap.Error(err))
			continue
		}

		hotRange, err := NewRegionFrameRange(region)
		if err != nil {
			return nil, err
		}
		f := h.FindBlockIndexOfRegion(allSchemas, hotRange)
		if f != nil {
			t.DbName = f.DBName
			t.BlockName = f.BlockName
			t.BlockID = f.BlockID
			t.IndexName = f.IndexName
			t.IndexID = f.IndexID
		}
		hotBlocks = append(hotBlocks, t)
	}

	return hotBlocks, nil
}

// FindBlockIndexOfRegion finds what block is involved in this hot region. And constructs the new frame item for future use.
func (h *Helper) FindBlockIndexOfRegion(allSchemas []*perceptron.DBInfo, hotRange *RegionFrameRange) *FrameItem {
	for _, EDB := range allSchemas {
		for _, tbl := range EDB.Blocks {
			if f := findRangeInBlock(hotRange, EDB, tbl); f != nil {
				return f
			}
		}
	}
	return nil
}

func findRangeInBlock(hotRange *RegionFrameRange, EDB *perceptron.DBInfo, tbl *perceptron.BlockInfo) *FrameItem {
	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return findRangeInPhysicalBlock(hotRange, tbl.ID, EDB.Name.O, tbl.Name.O, tbl.Indices, tbl.IsCommonHandle)
	}

	for _, def := range pi.Definitions {
		blockPartition := fmt.Sprintf("%s(%s)", tbl.Name.O, def.Name)
		if f := findRangeInPhysicalBlock(hotRange, def.ID, EDB.Name.O, blockPartition, tbl.Indices, tbl.IsCommonHandle); f != nil {
			return f
		}
	}
	return nil
}

func findRangeInPhysicalBlock(hotRange *RegionFrameRange, physicalID int64, dbName, tblName string, indices []*perceptron.IndexInfo, isCommonHandle bool) *FrameItem {
	if f := hotRange.GetRecordFrame(physicalID, dbName, tblName, isCommonHandle); f != nil {
		return f
	}
	for _, idx := range indices {
		if f := hotRange.GetIndexFrame(physicalID, idx.ID, dbName, tblName, idx.Name.O); f != nil {
			return f
		}
	}
	return nil
}

// NewRegionFrameRange init a NewRegionFrameRange with region info.
func NewRegionFrameRange(region *einsteindb.KeyLocation) (idxRange *RegionFrameRange, err error) {
	var first, last *FrameItem
	// check and init first frame
	if len(region.StartKey) > 0 {
		first, err = NewFrameItemFromRegionKey(region.StartKey)
		if err != nil {
			return
		}
	} else { // empty startKey means start with -infinite
		first = &FrameItem{
			IndexID:  int64(math.MinInt64),
			IsRecord: false,
			BlockID:  int64(math.MinInt64),
		}
	}

	// check and init last frame
	if len(region.EndKey) > 0 {
		last, err = NewFrameItemFromRegionKey(region.EndKey)
		if err != nil {
			return
		}
	} else { // empty endKey means end with +infinite
		last = &FrameItem{
			BlockID:  int64(math.MaxInt64),
			IndexID:  int64(math.MaxInt64),
			IsRecord: true,
		}
	}

	idxRange = &RegionFrameRange{
		region: region,
		First:  first,
		Last:   last,
	}
	return idxRange, nil
}

// NewFrameItemFromRegionKey creates a FrameItem with region's startKey or endKey,
// returns err when key is illegal.
func NewFrameItemFromRegionKey(key []byte) (frame *FrameItem, err error) {
	frame = &FrameItem{}
	frame.BlockID, frame.IndexID, frame.IsRecord, err = blockcodec.DecodeKeyHead(key)
	if err == nil {
		if frame.IsRecord {
			var handle ekv.Handle
			_, handle, err = blockcodec.DecodeRecordKey(key)
			if err == nil {
				if handle.IsInt() {
					frame.RecordID = handle.IntValue()
				} else {
					data, err := handle.Data()
					if err != nil {
						return nil, err
					}
					frame.IndexName = "PRIMARY"
					frame.IndexValues = make([]string, 0, len(data))
					for _, causet := range data {
						str, err := causet.ToString()
						if err != nil {
							return nil, err
						}
						frame.IndexValues = append(frame.IndexValues, str)
					}
				}
			}
		} else {
			_, _, frame.IndexValues, err = blockcodec.DecodeIndexKey(key)
		}
		logutil.BgLogger().Warn("decode region key failed", zap.ByteString("key", key), zap.Error(err))
		// Ignore decode errors.
		err = nil
		return
	}
	if bytes.HasPrefix(key, blockcodec.BlockPrefix()) {
		// If SplitBlock is enabled, the key may be `t{id}`.
		if len(key) == blockcodec.BlockSplitKeyLen {
			frame.BlockID = blockcodec.DecodeBlockID(key)
			return frame, nil
		}
		return nil, errors.Trace(err)
	}

	// key start with blockPrefix must be either record key or index key
	// That's means block's record key and index key are always together
	// in the continuous interval. And for key with prefix smaller than
	// blockPrefix, is smaller than all blocks. While for key with prefix
	// bigger than blockPrefix, means is bigger than all blocks.
	err = nil
	if bytes.Compare(key, blockcodec.BlockPrefix()) < 0 {
		frame.BlockID = math.MinInt64
		frame.IndexID = math.MinInt64
		frame.IsRecord = false
		return
	}
	// bigger than blockPrefix, means is bigger than all blocks.
	frame.BlockID = math.MaxInt64
	frame.BlockID = math.MaxInt64
	frame.IsRecord = true
	return
}

// GetRecordFrame returns the record frame of a block. If the block's records
// are not covered by this frame range, it returns nil.
func (r *RegionFrameRange) GetRecordFrame(blockID int64, dbName, blockName string, isCommonHandle bool) (f *FrameItem) {
	if blockID == r.First.BlockID && r.First.IsRecord {
		r.First.DBName, r.First.BlockName = dbName, blockName
		f = r.First
	} else if blockID == r.Last.BlockID && r.Last.IsRecord {
		r.Last.DBName, r.Last.BlockName = dbName, blockName
		f = r.Last
	} else if blockID >= r.First.BlockID && blockID < r.Last.BlockID {
		f = &FrameItem{
			DBName:    dbName,
			BlockName: blockName,
			BlockID:   blockID,
			IsRecord:  true,
		}
	}
	if f != nil && f.IsRecord && isCommonHandle {
		f.IndexName = "PRIMARY"
	}
	return
}

// GetIndexFrame returns the indnex frame of a block. If the block's indices are
// not covered by this frame range, it returns nil.
func (r *RegionFrameRange) GetIndexFrame(blockID, indexID int64, dbName, blockName, indexName string) *FrameItem {
	if blockID == r.First.BlockID && !r.First.IsRecord && indexID == r.First.IndexID {
		r.First.DBName, r.First.BlockName, r.First.IndexName = dbName, blockName, indexName
		return r.First
	}
	if blockID == r.Last.BlockID && indexID == r.Last.IndexID {
		r.Last.DBName, r.Last.BlockName, r.Last.IndexName = dbName, blockName, indexName
		return r.Last
	}

	greaterThanFirst := blockID > r.First.BlockID || (blockID == r.First.BlockID && !r.First.IsRecord && indexID > r.First.IndexID)
	lessThanLast := blockID < r.Last.BlockID || (blockID == r.Last.BlockID && (r.Last.IsRecord || indexID < r.Last.IndexID))
	if greaterThanFirst && lessThanLast {
		return &FrameItem{
			DBName:    dbName,
			BlockName: blockName,
			BlockID:   blockID,
			IsRecord:  false,
			IndexName: indexName,
			IndexID:   indexID,
		}
	}
	return nil
}

// RegionPeer stores information of one peer.
type RegionPeer struct {
	ID        int64 `json:"id"`
	StoreID   int64 `json:"store_id"`
	IsLearner bool  `json:"is_learner"`
}

// RegionEpoch stores the information about its epoch.
type RegionEpoch struct {
	ConfVer int64 `json:"conf_ver"`
	Version int64 `json:"version"`
}

// RegionPeerStat stores one field `DownSec` which indicates how long it's down than `RegionPeer`.
type RegionPeerStat struct {
	RegionPeer
	DownSec int64 `json:"down_seconds"`
}

// RegionInfo stores the information of one region.
type RegionInfo struct {
	ID              int64            `json:"id"`
	StartKey        string           `json:"start_key"`
	EndKey          string           `json:"end_key"`
	Epoch           RegionEpoch      `json:"epoch"`
	Peers           []RegionPeer     `json:"peers"`
	Leader          RegionPeer       `json:"leader"`
	DownPeers       []RegionPeerStat `json:"down_peers"`
	PendingPeers    []RegionPeer     `json:"pending_peers"`
	WrittenBytes    int64            `json:"written_bytes"`
	ReadBytes       int64            `json:"read_bytes"`
	ApproximateSize int64            `json:"approximate_size"`
	ApproximateKeys int64            `json:"approximate_keys"`

	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
}

// RegionsInfo stores the information of regions.
type RegionsInfo struct {
	Count   int64        `json:"count"`
	Regions []RegionInfo `json:"regions"`
}

// ReplicationStatus represents the replication mode status of the region.
type ReplicationStatus struct {
	State   string `json:"state"`
	StateID int64  `json:"state_id"`
}

// BlockInfo stores the information of a block or an index
type BlockInfo struct {
	EDB     *perceptron.DBInfo
	Block   *perceptron.BlockInfo
	IsIndex bool
	Index   *perceptron.IndexInfo
}

type withKeyRange interface {
	getStartKey() string
	getEndKey() string
}

// isIntersecting returns true if x and y intersect.
func isIntersecting(x, y withKeyRange) bool {
	return isIntersectingKeyRange(x, y.getStartKey(), y.getEndKey())
}

// isIntersectingKeyRange returns true if [startKey, endKey) intersect with x.
func isIntersectingKeyRange(x withKeyRange, startKey, endKey string) bool {
	return !isBeforeKeyRange(x, startKey, endKey) && !isBehindKeyRange(x, startKey, endKey)
}

// isBehind returns true is x is behind y
func isBehind(x, y withKeyRange) bool {
	return isBehindKeyRange(x, y.getStartKey(), y.getEndKey())
}

// IsBefore returns true is x is before [startKey, endKey)
func isBeforeKeyRange(x withKeyRange, startKey, endKey string) bool {
	return x.getEndKey() != "" && x.getEndKey() <= startKey
}

// IsBehind returns true is x is behind [startKey, endKey)
func isBehindKeyRange(x withKeyRange, startKey, endKey string) bool {
	return endKey != "" && x.getStartKey() >= endKey
}

func (r *RegionInfo) getStartKey() string { return r.StartKey }
func (r *RegionInfo) getEndKey() string   { return r.EndKey }

// for sorting
type byRegionStartKey []*RegionInfo

func (xs byRegionStartKey) Len() int      { return len(xs) }
func (xs byRegionStartKey) Swap(i, j int) { xs[i], xs[j] = xs[j], xs[i] }
func (xs byRegionStartKey) Less(i, j int) bool {
	return xs[i].getStartKey() < xs[j].getStartKey()
}

// blockInfoWithKeyRange stores block or index informations with its key range.
type blockInfoWithKeyRange struct {
	*BlockInfo
	StartKey string
	EndKey   string
}

func (t blockInfoWithKeyRange) getStartKey() string { return t.StartKey }
func (t blockInfoWithKeyRange) getEndKey() string   { return t.EndKey }

// for sorting
type byBlockStartKey []blockInfoWithKeyRange

func (xs byBlockStartKey) Len() int      { return len(xs) }
func (xs byBlockStartKey) Swap(i, j int) { xs[i], xs[j] = xs[j], xs[i] }
func (xs byBlockStartKey) Less(i, j int) bool {
	return xs[i].getStartKey() < xs[j].getStartKey()
}

func newBlockWithKeyRange(EDB *perceptron.DBInfo, block *perceptron.BlockInfo) blockInfoWithKeyRange {
	sk, ek := blockcodec.GetBlockHandleKeyRange(block.ID)
	startKey := bytesKeyToHex(codec.EncodeBytes(nil, sk))
	endKey := bytesKeyToHex(codec.EncodeBytes(nil, ek))
	return blockInfoWithKeyRange{
		&BlockInfo{
			EDB:     EDB,
			Block:   block,
			IsIndex: false,
			Index:   nil,
		},
		startKey,
		endKey,
	}
}

func newIndexWithKeyRange(EDB *perceptron.DBInfo, block *perceptron.BlockInfo, index *perceptron.IndexInfo) blockInfoWithKeyRange {
	sk, ek := blockcodec.GetBlockIndexKeyRange(block.ID, index.ID)
	startKey := bytesKeyToHex(codec.EncodeBytes(nil, sk))
	endKey := bytesKeyToHex(codec.EncodeBytes(nil, ek))
	return blockInfoWithKeyRange{
		&BlockInfo{
			EDB:     EDB,
			Block:   block,
			IsIndex: true,
			Index:   index,
		},
		startKey,
		endKey,
	}
}

// GetRegionsBlockInfo returns a map maps region id to its blocks or indices.
// Assuming blocks or indices key ranges never intersect.
// Regions key ranges can intersect.
func (h *Helper) GetRegionsBlockInfo(regionsInfo *RegionsInfo, schemas []*perceptron.DBInfo) map[int64][]BlockInfo {
	blockInfos := make(map[int64][]BlockInfo, len(regionsInfo.Regions))

	regions := make([]*RegionInfo, 0, len(regionsInfo.Regions))
	for i := 0; i < len(regionsInfo.Regions); i++ {
		blockInfos[regionsInfo.Regions[i].ID] = []BlockInfo{}
		regions = append(regions, &regionsInfo.Regions[i])
	}

	blocks := []blockInfoWithKeyRange{}
	for _, EDB := range schemas {
		for _, block := range EDB.Blocks {
			blocks = append(blocks, newBlockWithKeyRange(EDB, block))
			for _, index := range block.Indices {
				blocks = append(blocks, newIndexWithKeyRange(EDB, block, index))
			}
		}
	}

	if len(blocks) == 0 || len(regions) == 0 {
		return blockInfos
	}

	sort.Sort(byRegionStartKey(regions))
	sort.Sort(byBlockStartKey(blocks))

	idx := 0
OutLoop:
	for _, region := range regions {
		id := region.ID
		for isBehind(region, &blocks[idx]) {
			idx++
			if idx >= len(blocks) {
				break OutLoop
			}
		}
		for i := idx; i < len(blocks) && isIntersecting(region, &blocks[i]); i++ {
			blockInfos[id] = append(blockInfos[id], *blocks[i].BlockInfo)
		}
	}

	return blockInfos
}

func bytesKeyToHex(key []byte) string {
	return strings.ToUpper(hex.EncodeToString(key))
}

// GetRegionsInfo gets the region information of current causetstore by using FIDel's api.
func (h *Helper) GetRegionsInfo() (*RegionsInfo, error) {
	var regionsInfo RegionsInfo
	err := h.requestFIDel("GET", FIDelapi.Regions, nil, &regionsInfo)
	return &regionsInfo, err
}

// GetRegionInfoByID gets the region information of the region ID by using FIDel's api.
func (h *Helper) GetRegionInfoByID(regionID uint64) (*RegionInfo, error) {
	var regionInfo RegionInfo
	err := h.requestFIDel("GET", FIDelapi.RegionByID+strconv.FormatUint(regionID, 10), nil, &regionInfo)
	return &regionInfo, err
}

// request FIDel API, decode the response body into res
func (h *Helper) requestFIDel(method, uri string, body io.Reader, res interface{}) error {
	etcd, ok := h.CausetStore.(einsteindb.EtcdBackend)
	if !ok {
		return errors.WithStack(errors.New("not implemented"))
	}
	FIDelHosts, err := etcd.EtcdAddrs()
	if err != nil {
		return err
	}
	if len(FIDelHosts) == 0 {
		return errors.New("fidel unavailable")
	}
	logutil.BgLogger().Debug("RequestFIDel URL", zap.String("url", soliton.InternalHTTPSchema()+"://"+FIDelHosts[0]+uri))
	req, err := http.NewRequest(method, soliton.InternalHTTPSchema()+"://"+FIDelHosts[0]+uri, body)
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := soliton.InternalHTTPClient().Do(req)
	if err != nil {
		return errors.Trace(err)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()

	err = json.NewDecoder(resp.Body).Decode(res)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// StoresStat stores all information get from FIDel's api.
type StoresStat struct {
	Count  int         `json:"count"`
	Stores []StoreStat `json:"stores"`
}

// StoreStat stores information of one causetstore.
type StoreStat struct {
	CausetStore StoreBaseStat   `json:"causetstore"`
	Status      StoreDetailStat `json:"status"`
}

// StoreBaseStat stores the basic information of one causetstore.
type StoreBaseStat struct {
	ID             int64        `json:"id"`
	Address        string       `json:"address"`
	State          int64        `json:"state"`
	StateName      string       `json:"state_name"`
	Version        string       `json:"version"`
	Labels         []StoreLabel `json:"labels"`
	StatusAddress  string       `json:"status_address"`
	GitHash        string       `json:"git_hash"`
	StartTimestamp int64        `json:"start_timestamp"`
}

// StoreLabel stores the information of one causetstore label.
type StoreLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// StoreDetailStat stores the detail information of one causetstore.
type StoreDetailStat struct {
	Capacity        string    `json:"capacity"`
	Available       string    `json:"available"`
	LeaderCount     int64     `json:"leader_count"`
	LeaderWeight    float64   `json:"leader_weight"`
	LeaderScore     float64   `json:"leader_score"`
	LeaderSize      int64     `json:"leader_size"`
	RegionCount     int64     `json:"region_count"`
	RegionWeight    float64   `json:"region_weight"`
	RegionScore     float64   `json:"region_score"`
	RegionSize      int64     `json:"region_size"`
	StartTs         time.Time `json:"start_ts"`
	LastHeartbeatTs time.Time `json:"last_heartbeat_ts"`
	Uptime          string    `json:"uptime"`
}

// GetStoresStat gets the EinsteinDB causetstore information by accessing FIDel's api.
func (h *Helper) GetStoresStat() (*StoresStat, error) {
	etcd, ok := h.CausetStore.(einsteindb.EtcdBackend)
	if !ok {
		return nil, errors.WithStack(errors.New("not implemented"))
	}
	FIDelHosts, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(FIDelHosts) == 0 {
		return nil, errors.New("fidel unavailable")
	}
	req, err := http.NewRequest("GET", soliton.InternalHTTPSchema()+"://"+FIDelHosts[0]+FIDelapi.Stores, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := soliton.InternalHTTPClient().Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()
	var storesStat StoresStat
	err = json.NewDecoder(resp.Body).Decode(&storesStat)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &storesStat, nil
}

// GetFIDelAddr return the FIDel Address.
func (h *Helper) GetFIDelAddr() ([]string, error) {
	etcd, ok := h.CausetStore.(einsteindb.EtcdBackend)
	if !ok {
		return nil, errors.New("not implemented")
	}
	FIDelAddrs, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(FIDelAddrs) == 0 {
		return nil, errors.New("fidel unavailable")
	}
	return FIDelAddrs, nil
}

// FIDelRegionStats is the json response from FIDel.
type FIDelRegionStats struct {
	Count            int            `json:"count"`
	EmptyCount       int            `json:"empty_count"`
	StorageSize      int64          `json:"storage_size"`
	StorageKeys      int64          `json:"storage_keys"`
	StoreLeaderCount map[uint64]int `json:"store_leader_count"`
	StorePeerCount   map[uint64]int `json:"store_peer_count"`
}

// GetFIDelRegionStats get the RegionStats by blockID.
func (h *Helper) GetFIDelRegionStats(blockID int64, stats *FIDelRegionStats) error {
	FIDelAddrs, err := h.GetFIDelAddr()
	if err != nil {
		return err
	}

	startKey := blockcodec.EncodeBlockPrefix(blockID)
	endKey := blockcodec.EncodeBlockPrefix(blockID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	statURL := fmt.Sprintf("%s://%s/fidel/api/v1/stats/region?start_key=%s&end_key=%s",
		soliton.InternalHTTPSchema(),
		FIDelAddrs[0],
		url.QueryEscape(string(startKey)),
		url.QueryEscape(string(endKey)))

	resp, err := soliton.InternalHTTPClient().Get(statURL)
	if err != nil {
		return err
	}

	defer func() {
		if err = resp.Body.Close(); err != nil {
			log.Error(err)
		}
	}()

	dec := json.NewDecoder(resp.Body)

	return dec.Decode(stats)
}
