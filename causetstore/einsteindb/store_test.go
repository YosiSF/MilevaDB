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
	"sync"
	"time"

	fidel "github.com/einsteindb/fidel/client"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/FIDelpb"
	pb "github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/metapb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/causetstore/mockoracle"
	"github.com/whtcorpsinc/milevadb/ekv"
)

var errStopped = errors.New("stopped")

type testStoreSuite struct {
	testStoreSuiteBase
}

type testStoreSerialSuite struct {
	testStoreSuiteBase
}

type testStoreSuiteBase struct {
	OneByOneSuite
	causetstore *einsteindbStore
}

var _ = Suite(&testStoreSuite{})
var _ = SerialSuites(&testStoreSerialSuite{})

func (s *testStoreSuiteBase) SetUpTest(c *C) {
	s.causetstore = NewTestStore(c).(*einsteindbStore)
}

func (s *testStoreSuiteBase) TearDownTest(c *C) {
	c.Assert(s.causetstore.Close(), IsNil)
}

func (s *testStoreSuite) TestOracle(c *C) {
	o := &mockoracle.MockOracle{}
	s.causetstore.oracle = o

	ctx := context.Background()
	t1, err := s.causetstore.getTimestampWithRetry(NewBackofferWithVars(ctx, 100, nil))
	c.Assert(err, IsNil)
	t2, err := s.causetstore.getTimestampWithRetry(NewBackofferWithVars(ctx, 100, nil))
	c.Assert(err, IsNil)
	c.Assert(t1, Less, t2)

	t1, err = o.GetLowResolutionTimestamp(ctx)
	c.Assert(err, IsNil)
	t2, err = o.GetLowResolutionTimestamp(ctx)
	c.Assert(err, IsNil)
	c.Assert(t1, Less, t2)
	f := o.GetLowResolutionTimestampAsync(ctx)
	c.Assert(f, NotNil)
	_ = o.UntilExpired(0, 0)

	// Check retry.
	var wg sync.WaitGroup
	wg.Add(2)

	o.Disable()
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		o.Enable()
	}()

	go func() {
		defer wg.Done()
		t3, err := s.causetstore.getTimestampWithRetry(NewBackofferWithVars(ctx, tsoMaxBackoff, nil))
		c.Assert(err, IsNil)
		c.Assert(t2, Less, t3)
		expired := s.causetstore.oracle.IsExpired(t2, 50)
		c.Assert(expired, IsTrue)
	}()

	wg.Wait()
}

type mockFIDelClient struct {
	sync.RWMutex
	client fidel.Client
	stop   bool
}

func (c *mockFIDelClient) enable() {
	c.Lock()
	defer c.Unlock()
	c.stop = false
}

func (c *mockFIDelClient) disable() {
	c.Lock()
	defer c.Unlock()
	c.stop = true
}

func (c *mockFIDelClient) GetMemberInfo(ctx context.Context) ([]*FIDelpb.Member, error) {
	return nil, nil
}

func (c *mockFIDelClient) GetClusterID(context.Context) uint64 {
	return 1
}

func (c *mockFIDelClient) GetTS(ctx context.Context) (int64, int64, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return 0, 0, errors.Trace(errStopped)
	}
	return c.client.GetTS(ctx)
}

func (c *mockFIDelClient) GetTSAsync(ctx context.Context) fidel.TSFuture {
	return nil
}

func (c *mockFIDelClient) GetRegion(ctx context.Context, key []byte) (*fidel.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetRegion(ctx, key)
}

func (c *mockFIDelClient) GetPrevRegion(ctx context.Context, key []byte) (*fidel.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetPrevRegion(ctx, key)
}

func (c *mockFIDelClient) GetRegionByID(ctx context.Context, regionID uint64) (*fidel.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetRegionByID(ctx, regionID)
}

func (c *mockFIDelClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*fidel.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.ScanRegions(ctx, startKey, endKey, limit)
}

func (c *mockFIDelClient) GetStore(ctx context.Context, storeID uint64) (*metapb.CausetStore, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetStore(ctx, storeID)
}

func (c *mockFIDelClient) GetAllStores(ctx context.Context, opts ...fidel.GetStoreOption) ([]*metapb.CausetStore, error) {
	c.RLock()
	defer c.Unlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetAllStores(ctx)
}

func (c *mockFIDelClient) UFIDelateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *mockFIDelClient) UFIDelateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *mockFIDelClient) Close() {}

func (c *mockFIDelClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	return nil
}

func (c *mockFIDelClient) GetOperator(ctx context.Context, regionID uint64) (*FIDelpb.GetOperatorResponse, error) {
	return &FIDelpb.GetOperatorResponse{Status: FIDelpb.OperatorStatus_SUCCESS}, nil
}

func (c *mockFIDelClient) GetLeaderAddr() string { return "mockFIDel" }

func (c *mockFIDelClient) ScatterRegionWithOption(ctx context.Context, regionID uint64, opts ...fidel.ScatterRegionOption) error {
	return nil
}

type checkRequestClient struct {
	Client
	priority pb.CommandPri
}

func (c *checkRequestClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if c.priority != req.Priority {
		if resp.Resp != nil {
			if getResp, ok := resp.Resp.(*pb.GetResponse); ok {
				getResp.Error = &pb.KeyError{
					Abort: "request check error",
				}
			}
		}
	}
	return resp, err
}

func (s *testStoreSuite) TestRequestPriority(c *C) {
	client := &checkRequestClient{
		Client: s.causetstore.client,
	}
	s.causetstore.client = client

	// Cover 2PC commit.
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	client.priority = pb.CommandPri_High
	txn.SetOption(ekv.Priority, ekv.PriorityHigh)
	err = txn.Set([]byte("key"), []byte("value"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Cover the basic Get request.
	txn, err = s.causetstore.Begin()
	c.Assert(err, IsNil)
	client.priority = pb.CommandPri_Low
	txn.SetOption(ekv.Priority, ekv.PriorityLow)
	_, err = txn.Get(context.TODO(), []byte("key"))
	c.Assert(err, IsNil)

	// A counter example.
	client.priority = pb.CommandPri_Low
	txn.SetOption(ekv.Priority, ekv.PriorityNormal)
	_, err = txn.Get(context.TODO(), []byte("key"))
	// err is translated to "try again later" by backoffer, so doesn't check error value here.
	c.Assert(err, NotNil)

	// Cover Seek request.
	client.priority = pb.CommandPri_High
	txn.SetOption(ekv.Priority, ekv.PriorityHigh)
	iter, err := txn.Iter([]byte("key"), nil)
	c.Assert(err, IsNil)
	for iter.Valid() {
		c.Assert(iter.Next(), IsNil)
	}
	iter.Close()
}

func (s *testStoreSerialSuite) TestOracleChangeByFailpoint(c *C) {
	defer func() {
		failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle/changeTSFromFIDel")
	}()
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle/changeTSFromFIDel",
		"return(10000)"), IsNil)
	o := &mockoracle.MockOracle{}
	s.causetstore.oracle = o
	ctx := context.Background()
	t1, err := s.causetstore.getTimestampWithRetry(NewBackofferWithVars(ctx, 100, nil))
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle/changeTSFromFIDel"), IsNil)
	t2, err := s.causetstore.getTimestampWithRetry(NewBackofferWithVars(ctx, 100, nil))
	c.Assert(err, IsNil)
	c.Assert(t1, Greater, t2)
}
