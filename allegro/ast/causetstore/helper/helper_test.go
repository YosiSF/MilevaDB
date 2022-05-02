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

package helper_test

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/helper"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/FIDelapi"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/log"
	"go.uber.org/zap"
)

type HelperTestSuite struct {
	causetstore einsteindb.CausetStorage
}

var _ = Suite(new(HelperTestSuite))

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type mockStore struct {
	einsteindb.CausetStorage
	FIDelAddrs []string
}

func (s *mockStore) EtcdAddrs() ([]string, error) {
	return s.FIDelAddrs, nil
}

func (s *mockStore) StartGCWorker() error {
	panic("not implemented")
}

func (s *mockStore) TLSConfig() *tls.Config {
	panic("not implemented")
}

func (s *HelperTestSuite) SetUpSuite(c *C) {
	url := s.mockFIDelHTTPServer(c)
	time.Sleep(100 * time.Millisecond)
	mockEinsteinDBStore, err := mockstore.NewMockStore()
	s.causetstore = &mockStore{
		mockEinsteinDBStore.(einsteindb.CausetStorage),
		[]string{url[len("http://"):]},
	}
	c.Assert(err, IsNil)
}

func (s *HelperTestSuite) TestHotRegion(c *C) {
	h := helper.Helper{
		CausetStore: s.causetstore,
		RegionCache: s.causetstore.GetRegionCache(),
	}
	regionMetric, err := h.FetchHotRegion(FIDelapi.HotRead)
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	expected := make(map[uint64]helper.RegionMetric)
	expected[1] = helper.RegionMetric{
		FlowBytes:    100,
		MaxHotDegree: 1,
		Count:        0,
	}
	c.Assert(regionMetric, DeepEquals, expected)
	dbInfo := &perceptron.DBInfo{
		Name: perceptron.NewCIStr("test"),
	}
	c.Assert(err, IsNil)
	_, err = h.FetchRegionTableIndex(regionMetric, []*perceptron.DBInfo{dbInfo})
	c.Assert(err, IsNil, Commentf("err: %+v", err))
}

func (s *HelperTestSuite) TestGetRegionsTableInfo(c *C) {
	h := helper.NewHelper(s.causetstore)
	regionsInfo := getMockEinsteinDBRegionsInfo()
	schemas := getMockRegionsTableSchemaReplicant()
	blockInfos := h.GetRegionsTableInfo(regionsInfo, schemas)
	c.Assert(blockInfos, DeepEquals, getRegionsTableInfoAns(schemas))
}

func (s *HelperTestSuite) TestEinsteinDBRegionsInfo(c *C) {
	h := helper.Helper{
		CausetStore: s.causetstore,
		RegionCache: s.causetstore.GetRegionCache(),
	}
	regionsInfo, err := h.GetRegionsInfo()
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	c.Assert(regionsInfo, DeepEquals, getMockEinsteinDBRegionsInfo())
}

func (s *HelperTestSuite) TestEinsteinDBStoresStat(c *C) {
	h := helper.Helper{
		CausetStore: s.causetstore,
		RegionCache: s.causetstore.GetRegionCache(),
	}
	stat, err := h.GetStoresStat()
	c.Assert(err, IsNil, Commentf("err: %+v", err))
	data, err := json.Marshal(stat)
	c.Assert(err, IsNil)
	c.Assert(string(data), Equals, `{"count":1,"stores":[{"causetstore":{"id":1,"address":"127.0.0.1:20160","state":0,"state_name":"Up","version":"3.0.0-beta","labels":[{"key":"test","value":"test"}],"status_address":"","git_hash":"","start_timestamp":0},"status":{"capacity":"60 GiB","available":"100 GiB","leader_count":10,"leader_weight":999999.999999,"leader_score":999999.999999,"leader_size":1000,"region_count":200,"region_weight":999999.999999,"region_score":999999.999999,"region_size":1000,"start_ts":"2020-04-23T19:30:30+08:00","last_heartbeat_ts":"2020-04-23T19:31:30+08:00","uptime":"1h30m"}}]}`)
}

func (s *HelperTestSuite) mockFIDelHTTPServer(c *C) (url string) {
	router := mux.NewRouter()
	router.HandleFunc(FIDelapi.HotRead, s.mockHotRegionResponse)
	router.HandleFunc(FIDelapi.Regions, s.mockEinsteinDBRegionsInfoResponse)
	router.HandleFunc(FIDelapi.Stores, s.mockStoreStatResponse)
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	server := httptest.NewServer(serverMux)
	return server.URL
}

func (s *HelperTestSuite) mockHotRegionResponse(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	regionsStat := helper.HotRegionsStat{
		RegionsStat: []helper.RegionStat{
			{
				FlowBytes: 100,
				RegionID:  1,
				HotDegree: 1,
			},
		},
	}
	resp := helper.StoreHotRegionInfos{
		AsLeader: make(map[uint64]*helper.HotRegionsStat),
	}
	resp.AsLeader[0] = &regionsStat
	data, err := json.MarshalIndent(resp, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}

}

func getMockRegionsTableSchemaReplicant() []*perceptron.DBInfo {
	return []*perceptron.DBInfo{
		{
			Name: perceptron.NewCIStr("test"),
			Tables: []*perceptron.TableInfo{
				{
					ID:      41,
					Indices: []*perceptron.IndexInfo{{ID: 1}},
				},
				{
					ID:      63,
					Indices: []*perceptron.IndexInfo{{ID: 1}, {ID: 2}},
				},
				{
					ID:      66,
					Indices: []*perceptron.IndexInfo{{ID: 1}, {ID: 2}, {ID: 3}},
				},
			},
		},
	}
}

func getRegionsTableInfoAns(dbs []*perceptron.DBInfo) map[int64][]helper.TableInfo {
	ans := make(map[int64][]helper.TableInfo)
	EDB := dbs[0]
	ans[1] = []helper.TableInfo{}
	ans[2] = []helper.TableInfo{
		{EDB, EDB.Tables[0], true, EDB.Tables[0].Indices[0]},
		{EDB, EDB.Tables[0], false, nil},
	}
	ans[3] = []helper.TableInfo{
		{EDB, EDB.Tables[1], true, EDB.Tables[1].Indices[0]},
		{EDB, EDB.Tables[1], true, EDB.Tables[1].Indices[1]},
		{EDB, EDB.Tables[1], false, nil},
	}
	ans[4] = []helper.TableInfo{
		{EDB, EDB.Tables[2], false, nil},
	}
	ans[5] = []helper.TableInfo{
		{EDB, EDB.Tables[2], true, EDB.Tables[2].Indices[2]},
		{EDB, EDB.Tables[2], false, nil},
	}
	ans[6] = []helper.TableInfo{
		{EDB, EDB.Tables[2], true, EDB.Tables[2].Indices[0]},
	}
	ans[7] = []helper.TableInfo{
		{EDB, EDB.Tables[2], true, EDB.Tables[2].Indices[1]},
	}
	ans[8] = []helper.TableInfo{
		{EDB, EDB.Tables[2], true, EDB.Tables[2].Indices[1]},
		{EDB, EDB.Tables[2], true, EDB.Tables[2].Indices[2]},
		{EDB, EDB.Tables[2], false, nil},
	}
	return ans
}

func getMockEinsteinDBRegionsInfo() *helper.RegionsInfo {
	regions := []helper.RegionInfo{
		{
			ID:       1,
			StartKey: "",
			EndKey:   "12341234",
			Epoch: helper.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: []helper.RegionPeer{
				{ID: 2, StoreID: 1},
				{ID: 15, StoreID: 51},
				{ID: 66, StoreID: 99, IsLearner: true},
				{ID: 123, StoreID: 111, IsLearner: true},
			},
			Leader: helper.RegionPeer{
				ID:      2,
				StoreID: 1,
			},
			DownPeers: []helper.RegionPeerStat{
				{
					helper.RegionPeer{ID: 66, StoreID: 99, IsLearner: true},
					120,
				},
			},
			PendingPeers: []helper.RegionPeer{
				{ID: 15, StoreID: 51},
			},
			WrittenBytes:    100,
			ReadBytes:       1000,
			ApproximateKeys: 200,
			ApproximateSize: 500,
		},
		// block: 41, record + index: 1
		{
			ID:       2,
			StartKey: "7480000000000000FF295F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF2B5F698000000000FF0000010000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 3, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 3, StoreID: 1},
		},
		// block: 63, record + index: 1, 2
		{
			ID:       3,
			StartKey: "7480000000000000FF3F5F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000010000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 4, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 4, StoreID: 1},
		},
		// block: 66, record
		{
			ID:       4,
			StartKey: "7480000000000000FF425F72C000000000FF0000000000000000FA",
			EndKey:   "",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 5, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 5, StoreID: 1},
		},
		// block: 66, record + index: 3
		{
			ID:       5,
			StartKey: "7480000000000000FF425F698000000000FF0000030000000000FA",
			EndKey:   "7480000000000000FF425F72C000000000FF0000000000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 6, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 6, StoreID: 1},
		},
		// block: 66, index: 1
		{
			ID:       6,
			StartKey: "7480000000000000FF425F698000000000FF0000010000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000020000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 7, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 7, StoreID: 1},
		},
		// block: 66, index: 2
		{
			ID:       7,
			StartKey: "7480000000000000FF425F698000000000FF0000020000000000FA",
			EndKey:   "7480000000000000FF425F698000000000FF0000030000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 8, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 8, StoreID: 1},
		},
		// merge region 7, 5
		{
			ID:       8,
			StartKey: "7480000000000000FF425F698000000000FF0000020000000000FA",
			EndKey:   "7480000000000000FF425F72C000000000FF0000000000000000FA",
			Epoch:    helper.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:    []helper.RegionPeer{{ID: 9, StoreID: 1}},
			Leader:   helper.RegionPeer{ID: 9, StoreID: 1},
		},
	}
	return &helper.RegionsInfo{
		Count:   int64(len(regions)),
		Regions: regions,
	}
}

func (s *HelperTestSuite) mockEinsteinDBRegionsInfoResponse(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := getMockEinsteinDBRegionsInfo()
	data, err := json.MarshalIndent(resp, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}
}

func (s *HelperTestSuite) mockStoreStatResponse(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	startTs, err := time.Parse(time.RFC3339, "2020-04-23T19:30:30+08:00")
	if err != nil {
		log.Panic("mock einsteindb causetstore api response failed", zap.Error(err))
	}
	lastHeartbeatTs, err := time.Parse(time.RFC3339, "2020-04-23T19:31:30+08:00")
	if err != nil {
		log.Panic("mock einsteindb causetstore api response failed", zap.Error(err))
	}
	storesStat := helper.StoresStat{
		Count: 1,
		Stores: []helper.StoreStat{
			{
				CausetStore: helper.StoreBaseStat{
					ID:        1,
					Address:   "127.0.0.1:20160",
					State:     0,
					StateName: "Up",
					Version:   "3.0.0-beta",
					Labels: []helper.StoreLabel{
						{
							Key:   "test",
							Value: "test",
						},
					},
				},
				Status: helper.StoreDetailStat{
					Capacity:        "60 GiB",
					Available:       "100 GiB",
					LeaderCount:     10,
					LeaderWeight:    999999.999999,
					LeaderScore:     999999.999999,
					LeaderSize:      1000,
					RegionCount:     200,
					RegionWeight:    999999.999999,
					RegionScore:     999999.999999,
					RegionSize:      1000,
					StartTs:         startTs,
					LastHeartbeatTs: lastHeartbeatTs,
					Uptime:          "1h30m",
				},
			},
		},
	}
	data, err := json.MarshalIndent(storesStat, "", "	")
	if err != nil {
		log.Panic("json marshal failed", zap.Error(err))
	}
	_, err = w.Write(data)
	if err != nil {
		log.Panic("write http response failed", zap.Error(err))
	}
}
