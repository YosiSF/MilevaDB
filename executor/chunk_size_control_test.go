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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
)

var (
	_ = Suite(&testChunkSizeControlSuite{})
)

type testSlowClient struct {
	sync.RWMutex
	einsteindb.Client
	regionDelay map[uint64]time.Duration
}

func (c *testSlowClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	regionID := req.RegionId
	delay := c.GetDelay(regionID)
	if req.Type == einsteindbrpc.CmdINTERLOCK && delay > 0 {
		time.Sleep(delay)
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *testSlowClient) SetDelay(regionID uint64, dur time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.regionDelay[regionID] = dur
}

func (c *testSlowClient) GetDelay(regionID uint64) time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.regionDelay[regionID]
}

// manipulateCluster splits this cluster's region by splitKeys and returns regionIDs after split
func manipulateCluster(cluster cluster.Cluster, splitKeys [][]byte) []uint64 {
	if len(splitKeys) == 0 {
		return nil
	}
	region, _ := cluster.GetRegionByKey(splitKeys[0])
	for _, key := range splitKeys {
		if r, _ := cluster.GetRegionByKey(key); r.Id != region.Id {
			panic("all split keys should belong to the same region")
		}
	}
	allRegionIDs := []uint64{region.Id}
	for i, key := range splitKeys {
		newRegionID, newPeerID := cluster.AllocID(), cluster.AllocID()
		cluster.Split(allRegionIDs[i], newRegionID, key, []uint64{newPeerID}, newPeerID)
		allRegionIDs = append(allRegionIDs, newRegionID)
	}
	return allRegionIDs
}

func generateBlockSplitKeyForInt(tid int64, splitNum []int) [][]byte {
	results := make([][]byte, 0, len(splitNum))
	for _, num := range splitNum {
		results = append(results, blockcodec.EncodeEventKey(tid, codec.EncodeInt(nil, int64(num))))
	}
	return results
}

func generateIndexSplitKeyForInt(tid, idx int64, splitNum []int) [][]byte {
	results := make([][]byte, 0, len(splitNum))
	for _, num := range splitNum {
		d := new(types.Causet)
		d.SetInt64(int64(num))
		b, err := codec.EncodeKey(nil, nil, *d)
		if err != nil {
			panic(err)
		}
		results = append(results, blockcodec.EncodeIndexSeekKey(tid, idx, b))
	}
	return results
}

type testChunkSizeControlKit struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	tk          *testkit.TestKit
	client      *testSlowClient
	cluster     cluster.Cluster
}

type testChunkSizeControlSuite struct {
	m map[string]*testChunkSizeControlKit
}

func (s *testChunkSizeControlSuite) SetUpSuite(c *C) {
	c.Skip("not sblock because interlock may result in goroutine leak")
	blockALLEGROSQLs := map[string]string{}
	blockALLEGROSQLs["Limit&BlockScan"] = "create block t (a int, primary key (a))"
	blockALLEGROSQLs["Limit&IndexScan"] = "create block t (a int, index idx_a(a))"

	s.m = make(map[string]*testChunkSizeControlKit)
	for name, allegrosql := range blockALLEGROSQLs {
		// BootstrapStochastik is not thread-safe, so we have to prepare all resources in SetUp.
		kit := new(testChunkSizeControlKit)
		s.m[name] = kit
		kit.client = &testSlowClient{regionDelay: make(map[uint64]time.Duration)}

		var err error
		kit.causetstore, err = mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c cluster.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				kit.cluster = c
			}),
			mockstore.WithClientHijacker(func(c einsteindb.Client) einsteindb.Client {
				kit.client.Client = c
				return kit.client
			}),
		)
		c.Assert(err, IsNil)

		// init petri
		kit.dom, err = stochastik.BootstrapStochastik(kit.causetstore)
		c.Assert(err, IsNil)

		// create the test block
		kit.tk = testkit.NewTestKitWithInit(c, kit.causetstore)
		kit.tk.MustExec(allegrosql)
	}
}

func (s *testChunkSizeControlSuite) getKit(name string) (
	ekv.CausetStorage, *petri.Petri, *testkit.TestKit, *testSlowClient, cluster.Cluster) {
	x := s.m[name]
	return x.causetstore, x.dom, x.tk, x.client, x.cluster
}

func (s *testChunkSizeControlSuite) TestLimitAndBlockScan(c *C) {
	_, dom, tk, client, cluster := s.getKit("Limit&BlockScan")
	defer client.Close()
	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tbl.Meta().ID

	// construct two regions split by 100
	splitKeys := generateBlockSplitKeyForInt(tid, []int{100})
	regionIDs := manipulateCluster(cluster, splitKeys)

	noDelayThreshold := time.Millisecond * 100
	delayDuration := time.Second
	delayThreshold := delayDuration * 9 / 10
	tk.MustExec("insert into t values (1)") // insert one record into region1, and set a delay duration
	client.SetDelay(regionIDs[0], delayDuration)

	results := tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost := s.parseTimeCost(c, results.Events()[0])
	c.Assert(cost, Not(Less), delayThreshold) // have to wait for region1

	tk.MustExec("insert into t values (101)") // insert one record into region2
	results = tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost = s.parseTimeCost(c, results.Events()[0])
	c.Assert(cost, Less, noDelayThreshold) // region2 return quickly

	results = tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 2")
	cost = s.parseTimeCost(c, results.Events()[0])
	c.Assert(cost, Not(Less), delayThreshold) // have to wait
}

func (s *testChunkSizeControlSuite) TestLimitAndIndexScan(c *C) {
	_, dom, tk, client, cluster := s.getKit("Limit&IndexScan")
	defer client.Close()
	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tid := tbl.Meta().ID
	idx := tbl.Meta().Indices[0].ID

	// construct two regions split by 100
	splitKeys := generateIndexSplitKeyForInt(tid, idx, []int{100})
	regionIDs := manipulateCluster(cluster, splitKeys)

	noDelayThreshold := time.Millisecond * 100
	delayDuration := time.Second
	delayThreshold := delayDuration * 9 / 10
	tk.MustExec("insert into t values (1)") // insert one record into region1, and set a delay duration
	client.SetDelay(regionIDs[0], delayDuration)

	results := tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost := s.parseTimeCost(c, results.Events()[0])
	c.Assert(cost, Not(Less), delayThreshold) // have to wait for region1

	tk.MustExec("insert into t values (101)") // insert one record into region2
	results = tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 1")
	cost = s.parseTimeCost(c, results.Events()[0])
	c.Assert(cost, Less, noDelayThreshold) // region2 return quickly

	results = tk.MustQuery("explain analyze select * from t where t.a > 0 and t.a < 200 limit 2")
	cost = s.parseTimeCost(c, results.Events()[0])
	c.Assert(cost, Not(Less), delayThreshold) // have to wait
}

func (s *testChunkSizeControlSuite) parseTimeCost(c *C, line []interface{}) time.Duration {
	lineStr := fmt.Sprintf("%v", line)
	idx := strings.Index(lineStr, "time:")
	c.Assert(idx, Not(Equals), -1)
	lineStr = lineStr[idx+len("time:"):]
	idx = strings.Index(lineStr, ",")
	c.Assert(idx, Not(Equals), -1)
	timeStr := lineStr[:idx]
	d, err := time.ParseDuration(timeStr)
	c.Assert(err, IsNil)
	return d
}
