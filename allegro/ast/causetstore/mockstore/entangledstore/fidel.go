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

package entangledstore

import (
	"errors"
	"math"
	"sync"

	fidel "github.com/einsteindb/fidel/client"
	us "github.com/ngaut/entangledstore/einsteindb"
	"github.com/whtcorpsinc/ekvproto/pkg/FIDelpb"
	"golang.org/x/net/context"
)

var _ fidel.Client = new(FIDelClient)

type FIDelClient struct {
	*us.MockFIDel

	serviceSafePoints map[string]uint64
	gcSafePointMu     sync.Mutex
}

func newFIDelClient(fidel *us.MockFIDel) *FIDelClient {
	return &FIDelClient{
		MockFIDel:         fidel,
		serviceSafePoints: make(map[string]uint64),
	}
}

func (c *FIDelClient) GetTSAsync(ctx context.Context) fidel.TSFuture {
	return &mockTSFuture{c, ctx, false}
}

type mockTSFuture struct {
	FIDelc *FIDelClient
	ctx    context.Context
	used   bool
}

func (m *mockTSFuture) Wait() (int64, int64, error) {
	if m.used {
		return 0, 0, errors.New("cannot wait tso twice")
	}
	m.used = true
	return m.FIDelc.GetTS(m.ctx)
}

func (c *FIDelClient) GetLeaderAddr() string { return "mockFIDel" }

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

func (c *FIDelClient) GetOperator(ctx context.Context, regionID uint64) (*FIDelpb.GetOperatorResponse, error) {
	return &FIDelpb.GetOperatorResponse{Status: FIDelpb.OperatorStatus_SUCCESS}, nil
}

func (c *FIDelClient) ScatterRegionWithOption(ctx context.Context, regionID uint64, opts ...fidel.ScatterRegionOption) error {
	return nil
}

func (c *FIDelClient) GetMemberInfo(ctx context.Context) ([]*FIDelpb.Member, error) {
	return nil, nil
}
