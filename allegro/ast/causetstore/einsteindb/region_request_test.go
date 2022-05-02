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

package einsteindb

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/storeutil"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
	"github.com/whtcorpsinc/ekvproto/pkg/errorpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	"google.golang.org/grpc"
)

type testRegionRequestSuite struct {
	cluster             *mockeinsteindb.Cluster
	causetstore         uint64
	peer                uint64
	region              uint64
	cache               *RegionCache
	bo                  *Backoffer
	regionRequestSender *RegionRequestSender
	mvsr-oocStore           mockeinsteindb.MVCCStore
}

type testStoreLimitSuite struct {
	cluster             *mockeinsteindb.Cluster
	storeIDs            []uint64
	peerIDs             []uint64
	regionID            uint64
	leaderPeer          uint64
	cache               *RegionCache
	bo                  *Backoffer
	regionRequestSender *RegionRequestSender
	mvsr-oocStore           mockeinsteindb.MVCCStore
}

var _ = Suite(&testRegionRequestSuite{})
var _ = Suite(&testStoreLimitSuite{})

func (s *testRegionRequestSuite) SetUpTest(c *C) {
	s.cluster = mockeinsteindb.NewCluster(mockeinsteindb.MustNewMVCCStore())
	s.causetstore, s.peer, s.region = mockeinsteindb.BootstrapWithSingleStore(s.cluster)
	FIDelCli := &codecFIDelClient{mockeinsteindb.NewFIDelClient(s.cluster)}
	s.cache = NewRegionCache(FIDelCli)
	s.bo = NewNoopBackoff(context.Background())
	s.mvsr-oocStore = mockeinsteindb.MustNewMVCCStore()
	client := mockeinsteindb.NewRPCClient(s.cluster, s.mvsr-oocStore)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testStoreLimitSuite) SetUpTest(c *C) {
	s.cluster = mockeinsteindb.NewCluster(mockeinsteindb.MustNewMVCCStore())
	s.storeIDs, s.peerIDs, s.regionID, s.leaderPeer = mockeinsteindb.BootstrapWithMultiStores(s.cluster, 3)
	FIDelCli := &codecFIDelClient{mockeinsteindb.NewFIDelClient(s.cluster)}
	s.cache = NewRegionCache(FIDelCli)
	s.bo = NewNoopBackoff(context.Background())
	s.mvsr-oocStore = mockeinsteindb.MustNewMVCCStore()
	client := mockeinsteindb.NewRPCClient(s.cluster, s.mvsr-oocStore)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testRegionRequestSuite) TearDownTest(c *C) {
	s.cache.Close()
}

func (s *testStoreLimitSuite) TearDownTest(c *C) {
	s.cache.Close()
}

type fnClient struct {
	fn func(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error)
}

func (f *fnClient) Close() error {
	return nil
}

func (f *fnClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	return f.fn(ctx, addr, req, timeout)
}

func (s *testRegionRequestSuite) TestOnRegionError(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	// test stale command retry.
	func() {
		oc := s.regionRequestSender.client
		defer func() {
			s.regionRequestSender.client = oc
		}()
		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (response *einsteindbrpc.Response, err error) {
			staleResp := &einsteindbrpc.Response{Resp: &kvrpcpb.GetResponse{
				RegionError: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}},
			}}
			return staleResp, nil
		}}
		bo := NewBackofferWithVars(context.Background(), 5, nil)
		resp, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		c.Assert(err, NotNil)
		c.Assert(resp, IsNil)
	}()

}

func (s *testStoreLimitSuite) TestStoreTokenLimit(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{}, kvrpcpb.Context{})
	region, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	oldStoreLimit := storeutil.StoreLimit.Load()
	storeutil.StoreLimit.CausetStore(500)
	s.cache.getStoreByStoreID(s.storeIDs[0]).tokenCount.CausetStore(500)
	// cause there is only one region in this cluster, regionID maps this leader.
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)
	c.Assert(resp, IsNil)
	c.Assert(err.Error(), Equals, "[einsteindb:9008]CausetStore token is up to the limit, causetstore id = 1")
	storeutil.StoreLimit.CausetStore(oldStoreLimit)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithStoreRestart(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)

	// stop causetstore.
	s.cluster.StopStore(s.causetstore)
	_, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)

	// start causetstore.
	s.cluster.StartStore(s.causetstore)

	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithCloseKnownStoreThenUseNewOne(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})

	// add new store2 and make store2 as leader.
	store2 := s.cluster.AllocID()
	peer2 := s.cluster.AllocID()
	s.cluster.AddStore(store2, fmt.Sprintf("causetstore%d", store2))
	s.cluster.AddPeer(s.region, store2, peer2)
	s.cluster.ChangeLeader(s.region, peer2)

	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)

	// stop store2 and make store1 as new leader.
	s.cluster.StopStore(store2)
	s.cluster.ChangeLeader(s.region, s.peer)

	// send to store2 fail and send to new leader store1.
	bo2 := NewBackofferWithVars(context.Background(), 100, nil)
	resp, err = s.regionRequestSender.SendReq(bo2, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	regionErr, err := resp.GetRegionError()
	c.Assert(err, IsNil)
	c.Assert(regionErr, IsNil)
	c.Assert(resp.Resp, NotNil)
}

func (s *testRegionRequestSuite) TestSendReqCtx(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, ctx, err := s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, ekv.EinsteinDB)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	c.Assert(ctx, NotNil)
	req.ReplicaRead = true
	resp, ctx, err = s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, ekv.EinsteinDB)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	c.Assert(ctx, NotNil)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithCancelled(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)

	// set causetstore to cancel state.
	s.cluster.CancelStore(s.causetstore)
	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	_, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)
	c.Assert(errors.Cause(err), Equals, context.Canceled)

	// set causetstore to normal state.
	s.cluster.UnCancelStore(s.causetstore)
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
}

func (s *testRegionRequestSuite) TestNoReloadRegionWhenCtxCanceled(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	sender := s.regionRequestSender
	bo, cancel := s.bo.Fork()
	cancel()
	// Call SendKVReq with a canceled context.
	_, err = sender.SendReq(bo, req, region.Region, time.Second)
	// Check this HoTT of error won't cause region cache drop.
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(sender.regionCache.getRegionByIDFromCache(s.region), NotNil)
}

// cancelContextClient wraps rpcClient and always cancels context before sending requests.
type cancelContextClient struct {
	Client
	redirectAddr string
}

func (c *cancelContextClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	childCtx, cancel := context.WithCancel(ctx)
	cancel()
	return c.Client.SendRequest(childCtx, c.redirectAddr, req, timeout)
}

// mockEinsteinDBGrpcServer mock a einsteindb gprc server for testing.
type mockEinsteinDBGrpcServer struct{}

// KvGet commands with mvsr-ooc/txn supported.
func (s *mockEinsteinDBGrpcServer) KvGet(context.Context, *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvScan(context.Context, *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvPrewrite(context.Context, *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvCommit(context.Context, *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvImport(context.Context, *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvCleanup(context.Context, *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvBatchGet(context.Context, *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvBatchRollback(context.Context, *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvScanLock(context.Context, *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvResolveLock(context.Context, *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvPessimisticLock(context.Context, *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KVPessimisticRollback(context.Context, *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvCheckTxnStatus(ctx context.Context, in *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvCheckSecondaryLocks(ctx context.Context, in *kvrpcpb.CheckSecondaryLocksRequest) (*kvrpcpb.CheckSecondaryLocksResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvTxnHeartBeat(ctx context.Context, in *kvrpcpb.TxnHeartBeatRequest) (*kvrpcpb.TxnHeartBeatResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvGC(context.Context, *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KvDeleteRange(context.Context, *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawBatchGet(context.Context, *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawBatchPut(context.Context, *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawBatchDelete(context.Context, *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawDeleteRange(context.Context, *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawBatchScan(context.Context, *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) UnsafeDestroyRange(context.Context, *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RegisterLockObserver(context.Context, *kvrpcpb.RegisterLockObserverRequest) (*kvrpcpb.RegisterLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) CheckLockObserver(context.Context, *kvrpcpb.CheckLockObserverRequest) (*kvrpcpb.CheckLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RemoveLockObserver(context.Context, *kvrpcpb.RemoveLockObserverRequest) (*kvrpcpb.RemoveLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) PhysicalScanLock(context.Context, *kvrpcpb.PhysicalScanLockRequest) (*kvrpcpb.PhysicalScanLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) interlocking_directorate(context.Context, *interlock.Request) (*interlock.Response, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) Batchinterlocking_directorate(*interlock.BatchRequest, einsteindbpb.EinsteinDB_Batchinterlocking_directorateServer) error {
	return errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) Raft(einsteindbpb.EinsteinDB_RaftServer) error {
	return errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) BatchRaft(einsteindbpb.EinsteinDB_BatchRaftServer) error {
	return errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) Snapshot(einsteindbpb.EinsteinDB_SnapshotServer) error {
	return errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) MvccGetByKey(context.Context, *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) MvccGetByStartTs(context.Context, *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) SplitRegion(context.Context, *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) interlocking_directorateStream(*interlock.Request, einsteindbpb.EinsteinDB_interlocking_directorateStreamServer) error {
	return errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) BatchCommands(einsteindbpb.EinsteinDB_BatchCommandsServer) error {
	return errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) ReadIndex(context.Context, *kvrpcpb.ReadIndexRequest) (*kvrpcpb.ReadIndexResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerGet(context.Context, *kvrpcpb.VerGetRequest) (*kvrpcpb.VerGetResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerBatchGet(context.Context, *kvrpcpb.VerBatchGetRequest) (*kvrpcpb.VerBatchGetResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerMut(context.Context, *kvrpcpb.VerMutRequest) (*kvrpcpb.VerMutResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerBatchMut(context.Context, *kvrpcpb.VerBatchMutRequest) (*kvrpcpb.VerBatchMutResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerScan(context.Context, *kvrpcpb.VerScanRequest) (*kvrpcpb.VerScanResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerDeleteRange(context.Context, *kvrpcpb.VerDeleteRangeRequest) (*kvrpcpb.VerDeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *testRegionRequestSuite) TestNoReloadRegionForGrpcWhenCtxCanceled(c *C) {
	// prepare a mock einsteindb grpc server
	addr := "localhost:56341"
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	server := grpc.NewServer()
	einsteindbpb.RegisterEinsteinDBServer(server, &mockEinsteinDBGrpcServer{})
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		server.Serve(lis)
		wg.Done()
	}()

	client := newRPCClient(config.Security{})
	sender := NewRegionRequestSender(s.cache, client)
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)

	bo, cancel := s.bo.Fork()
	cancel()
	_, err = sender.SendReq(bo, req, region.Region, 3*time.Second)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(s.cache.getRegionByIDFromCache(s.region), NotNil)

	// Just for covering error code = codes.Canceled.
	client1 := &cancelContextClient{
		Client:       newRPCClient(config.Security{}),
		redirectAddr: addr,
	}
	sender = NewRegionRequestSender(s.cache, client1)
	sender.SendReq(s.bo, req, region.Region, 3*time.Second)

	// cleanup
	server.Stop()
	wg.Wait()
}

func (s *testRegionRequestSuite) TestOnMaxTimestampNotSyncedError(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	// test retry for max timestamp not synced
	func() {
		oc := s.regionRequestSender.client
		defer func() {
			s.regionRequestSender.client = oc
		}()
		count := 0
		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (response *einsteindbrpc.Response, err error) {
			count += 1
			var resp *einsteindbrpc.Response
			if count < 3 {
				resp = &einsteindbrpc.Response{Resp: &kvrpcpb.PrewriteResponse{
					RegionError: &errorpb.Error{MaxTimestampNotSynced: &errorpb.MaxTimestampNotSynced{}},
				}}
			} else {
				resp = &einsteindbrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}
			}
			return resp, nil
		}}
		bo := NewBackofferWithVars(context.Background(), 5, nil)
		resp, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		c.Assert(err, IsNil)
		c.Assert(resp, NotNil)
	}()
}
