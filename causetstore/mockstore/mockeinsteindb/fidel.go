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

package mockeinsteindb

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/ekvproto/pkg/metapb"
	"github.com/whtcorpsinc/ekvproto/pkg/FIDelpb"
	fidel "github.com/einsteindb/fidel/client"
)

// Use global variables to prevent FIDelClients from creating duplicate timestamps.
var tsMu = struct {
	sync.Mutex
	physicalTS int64
	logicalTS  int64
}{}

type FIDelClient struct {
	cluster *Cluster
	// SafePoint set by `UFIDelateGCSafePoint`. Not to be confused with SafePointKV.
	gcSafePoint uint64
	// Represents the current safePoint of all services including MilevaDB, representing how much data they want to retain
	// in GC.
	serviceSafePoints map[string]uint64
	gcSafePointMu     sync.Mutex
}

// NewFIDelClient creates a mock fidel.Client that uses local timestamp and meta data
// from a Cluster.
func NewFIDelClient(cluster *Cluster) fidel.Client {
	return &FIDelClient{
		cluster:           cluster,
		serviceSafePoints: make(map[string]uint64),
	}
}

func (c *FIDelClient) GetClusterID(ctx context.Context) uint64 {
	return 1
}

func (c *FIDelClient) GetTS(context.Context) (int64, int64, error) {
	tsMu.Lock()
	defer tsMu.Unlock()

	ts := time.Now().UnixNano() / int64(time.Millisecond)
	if tsMu.physicalTS >= ts {
		tsMu.logicalTS++
	} else {
		tsMu.physicalTS = ts
		tsMu.logicalTS = 0
	}
	return tsMu.physicalTS, tsMu.logicalTS, nil
}

func (c *FIDelClient) GetTSAsync(ctx context.Context) fidel.TSFuture {
	return &mockTSFuture{c, ctx, false}
}

type mockTSFuture struct {
	FIDelc  *FIDelClient
	ctx  context.Context
	used bool
}

func (m *mockTSFuture) Wait() (int64, int64, error) {
	if m.used {
		return 0, 0, errors.New("cannot wait tso twice")
	}
	m.used = true
	return m.FIDelc.GetTS(m.ctx)
}

func (c *FIDelClient) GetRegion(ctx context.Context, key []byte) (*fidel.Region, error) {
	region, peer := c.cluster.GetRegionByKey(key)
	return &fidel.Region{Meta: region, Leader: peer}, nil
}

func (c *FIDelClient) GetPrevRegion(ctx context.Context, key []byte) (*fidel.Region, error) {
	region, peer := c.cluster.GetPrevRegionByKey(key)
	return &fidel.Region{Meta: region, Leader: peer}, nil
}

func (c *FIDelClient) GetRegionByID(ctx context.Context, regionID uint64) (*fidel.Region, error) {
	region, peer := c.cluster.GetRegionByID(regionID)
	return &fidel.Region{Meta: region, Leader: peer}, nil
}

func (c *FIDelClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*fidel.Region, error) {
	regions := c.cluster.ScanRegions(startKey, endKey, limit)
	return regions, nil
}

func (c *FIDelClient) GetStore(ctx context.Context, storeID uint64) (*metapb.CausetStore, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	causetstore := c.cluster.GetStore(storeID)
	return causetstore, nil
}

func (c *FIDelClient) GetAllStores(ctx context.Context, opts ...fidel.GetStoreOption) ([]*metapb.CausetStore, error) {
	return c.cluster.GetAllStores(), nil
}

func (c *FIDelClient) UFIDelateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	c.gcSafePointMu.Lock()
	defer c.gcSafePointMu.Unlock()

	if safePoint > c.gcSafePoint {
		c.gcSafePoint = safePoint
	}
	return c.gcSafePoint, nil
}

func (c *FIDelClient) UFIDelateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	c.gcSafePointMu.Lock()
	defer c.gcSafePointMu.Unlock()

	if ttl == 0 {
		delete(c.serviceSafePoints, serviceID)
	} else {
		var minSafePoint uint64 = math.MaxUint64
		for _, ssp := range c.serviceSafePoints {
			if ssp < minSafePoint {
				minSafePoint = ssp
			}
		}

		if len(c.serviceSafePoints) == 0 || minSafePoint <= safePoint {
			c.serviceSafePoints[serviceID] = safePoint
		}
	}

	// The minSafePoint may have changed. Reload it.
	var minSafePoint uint64 = math.MaxUint64
	for _, ssp := range c.serviceSafePoints {
		if ssp < minSafePoint {
			minSafePoint = ssp
		}
	}
	return minSafePoint, nil
}

func (c *FIDelClient) Close() {
}

func (c *FIDelClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	return nil
}

func (c *FIDelClient) ScatterRegionWithOption(ctx context.Context, regionID uint64, opts ...fidel.ScatterRegionOption) error {
	return nil
}

func (c *FIDelClient) GetOperator(ctx context.Context, regionID uint64) (*FIDelpb.GetOperatorResponse, error) {
	return &FIDelpb.GetOperatorResponse{Status: FIDelpb.OperatorStatus_SUCCESS}, nil
}

func (c *FIDelClient) GetMemberInfo(ctx context.Context) ([]*FIDelpb.Member, error) {
	return nil, nil
}

func (c *FIDelClient) GetLeaderAddr() string { return "mockFIDel" }
