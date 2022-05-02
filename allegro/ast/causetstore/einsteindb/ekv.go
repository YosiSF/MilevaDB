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
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	fidel "github.com/einsteindb/fidel/client"
	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/latch"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle/oracles"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/fastrand"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type storeCache struct {
	sync.Mutex
	cache map[string]*einsteindbStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

func createEtcdKV(addrs []string, tlsConfig *tls.Config) (*clientv3.Client, error) {
	cfg := config.GetGlobalConfig()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            addrs,
		AutoSyncInterval:     30 * time.Second,
		DialTimeout:          5 * time.Second,
		TLS:                  tlsConfig,
		DialKeepAliveTime:    time.Second * time.Duration(cfg.EinsteinDBClient.GrpcKeepAliveTime),
		DialKeepAliveTimeout: time.Second * time.Duration(cfg.EinsteinDBClient.GrpcKeepAliveTimeout),
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cli, nil
}

// Open opens or creates an EinsteinDB storage with given path.
// Path example: einsteindb://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false
func (d Driver) Open(path string) (ekv.CausetStorage, error) {
	mc.Lock()
	defer mc.Unlock()

	security := config.GetGlobalConfig().Security
	einsteindbConfig := config.GetGlobalConfig().EinsteinDBClient
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	etcdAddrs, disableGC, err := config.ParsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	FIDelCli, err := fidel.NewClient(etcdAddrs, fidel.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	}, fidel.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(einsteindbConfig.GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(einsteindbConfig.GrpcKeepAliveTimeout) * time.Second,
		}),
	))
	FIDelCli = execdetails.InterceptedFIDelClient{Client: FIDelCli}

	if err != nil {
		return nil, errors.Trace(err)
	}

	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("einsteindb-%v", FIDelCli.GetClusterID(context.TODO()))
	if causetstore, ok := mc.cache[uuid]; ok {
		return causetstore, nil
	}

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}

	spkv, err := NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	INTERLOCKrCacheConfig := &config.GetGlobalConfig().EinsteinDBClient.INTERLOCKrCache
	s, err := newEinsteinDBStore(uuid, &codecFIDelClient{FIDelCli}, spkv, newRPCClient(security), !disableGC, INTERLOCKrCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if txnLocalLatches.Enabled {
		s.EnableTxnLocalLatches(txnLocalLatches.Capacity)
	}
	s.etcdAddrs = etcdAddrs
	s.tlsConfig = tlsConfig

	mc.cache[uuid] = s
	return s, nil
}

// EtcdBackend is used for judging a storage is a real EinsteinDB.
type EtcdBackend interface {
	EtcdAddrs() ([]string, error)
	TLSConfig() *tls.Config
	StartGCWorker() error
}

// uFIDelate oracle's lastTS every 2000ms.
var oracleUFIDelateInterval = 2000

type einsteindbStore struct {
	clusterID       uint64
	uuid            string
	oracle          oracle.Oracle
	client          Client
	FIDelClient     fidel.Client
	regionCache     *RegionCache
	INTERLOCKrCache *INTERLOCKrCache
	lockResolver    *LockResolver
	txnLatches      *latch.LatchesScheduler
	gcWorker        GCHandler
	etcdAddrs       []string
	tlsConfig       *tls.Config
	mock            bool
	enableGC        bool

	ekv       SafePointKV
	safePoint uint64
	spTime    time.Time
	spMutex   sync.RWMutex  // this is used to uFIDelate safePoint and spTime
	closed    chan struct{} // this is used to nofity when the causetstore is closed

	replicaReadSeed uint32 // this is used to load balance followers / learners when replica read is enabled
}

func (s *einsteindbStore) UFIDelateSPCache(cachedSP uint64, cachedTime time.Time) {
	s.spMutex.Lock()
	s.safePoint = cachedSP
	s.spTime = cachedTime
	s.spMutex.Unlock()
}

func (s *einsteindbStore) CheckVisibility(startTime uint64) error {
	s.spMutex.RLock()
	cachedSafePoint := s.safePoint
	cachedTime := s.spTime
	s.spMutex.RUnlock()
	diff := time.Since(cachedTime)

	if diff > (GcSafePointCacheInterval - gcCPUTimeInaccuracyBound) {
		return ErrFIDelServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point")
	}

	if startTime < cachedSafePoint {
		t1 := oracle.GetTimeFromTS(startTime)
		t2 := oracle.GetTimeFromTS(cachedSafePoint)
		return ErrGCTooEarly.GenWithStackByArgs(t1, t2)
	}

	return nil
}

func newEinsteinDBStore(uuid string, FIDelClient fidel.Client, spkv SafePointKV, client Client, enableGC bool, INTERLOCKrCacheConfig *config.interlocking_directorateCache) (*einsteindbStore, error) {
	o, err := oracles.NewFIDelOracle(FIDelClient, time.Duration(oracleUFIDelateInterval)*time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}
	causetstore := &einsteindbStore{
		clusterID:       FIDelClient.GetClusterID(context.TODO()),
		uuid:            uuid,
		oracle:          o,
		client:          reqDefCauslapse{client},
		FIDelClient:     FIDelClient,
		regionCache:     NewRegionCache(FIDelClient),
		INTERLOCKrCache: nil,
		ekv:             spkv,
		safePoint:       0,
		spTime:          time.Now(),
		closed:          make(chan struct{}),
		replicaReadSeed: fastrand.Uint32(),
	}
	causetstore.lockResolver = newLockResolver(causetstore)
	causetstore.enableGC = enableGC

	INTERLOCKrCache, err := newINTERLOCKrCache(INTERLOCKrCacheConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	causetstore.INTERLOCKrCache = INTERLOCKrCache

	go causetstore.runSafePointChecker()

	return causetstore, nil
}

func (s *einsteindbStore) EnableTxnLocalLatches(size uint) {
	s.txnLatches = latch.NewScheduler(size)
}

// IsLatchEnabled is used by mockstore.TestConfig.
func (s *einsteindbStore) IsLatchEnabled() bool {
	return s.txnLatches != nil
}

func (s *einsteindbStore) EtcdAddrs() ([]string, error) {
	if s.etcdAddrs == nil {
		return nil, nil
	}
	ctx := context.Background()
	bo := NewBackoffer(ctx, GetMemberInfoBackoff)
	etcdAddrs := make([]string, 0)
	FIDelClient := s.FIDelClient
	if FIDelClient == nil {
		return nil, errors.New("Etcd client not found")
	}
	for {
		members, err := FIDelClient.GetMemberInfo(ctx)
		if err != nil {
			err := bo.Backoff(BoRegionMiss, err)
			if err != nil {
				return nil, err
			}
			continue
		}
		for _, member := range members {
			if len(member.ClientUrls) > 0 {
				u, err := url.Parse(member.ClientUrls[0])
				if err != nil {
					logutil.BgLogger().Error("fail to parse client url from fidel members", zap.String("client_url", member.ClientUrls[0]), zap.Error(err))
					return nil, err
				}
				etcdAddrs = append(etcdAddrs, u.Host)
			}
		}
		return etcdAddrs, nil
	}
}

func (s *einsteindbStore) TLSConfig() *tls.Config {
	return s.tlsConfig
}

// StartGCWorker starts GC worker, it's called in BootstrapStochastik, don't call this function more than once.
func (s *einsteindbStore) StartGCWorker() error {
	if !s.enableGC || NewGCHandlerFunc == nil {
		return nil
	}

	gcWorker, err := NewGCHandlerFunc(s, s.FIDelClient)
	if err != nil {
		return errors.Trace(err)
	}
	gcWorker.Start()
	s.gcWorker = gcWorker
	return nil
}

func (s *einsteindbStore) runSafePointChecker() {
	d := gcSafePointUFIDelateInterval
	for {
		select {
		case spCachedTime := <-time.After(d):
			cachedSafePoint, err := loadSafePoint(s.GetSafePointKV())
			if err == nil {
				metrics.EinsteinDBLoadSafepointCounter.WithLabelValues("ok").Inc()
				s.UFIDelateSPCache(cachedSafePoint, spCachedTime)
				d = gcSafePointUFIDelateInterval
			} else {
				metrics.EinsteinDBLoadSafepointCounter.WithLabelValues("fail").Inc()
				logutil.BgLogger().Error("fail to load safepoint from fidel", zap.Error(err))
				d = gcSafePointQuickRepeatInterval
			}
		case <-s.Closed():
			return
		}
	}
}

func (s *einsteindbStore) Begin() (ekv.Transaction, error) {
	txn, err := newEinsteinDBTxn(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

// BeginWithStartTS begins a transaction with startTS.
func (s *einsteindbStore) BeginWithStartTS(startTS uint64) (ekv.Transaction, error) {
	txn, err := newEinsteinDBTxnWithStartTS(s, startTS, s.nextReplicaReadSeed())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

func (s *einsteindbStore) GetSnapshot(ver ekv.Version) (ekv.Snapshot, error) {
	snapshot := newEinsteinDBSnapshot(s, ver, s.nextReplicaReadSeed())
	return snapshot, nil
}

func (s *einsteindbStore) Close() error {
	mc.Lock()
	defer mc.Unlock()

	delete(mc.cache, s.uuid)
	s.oracle.Close()
	s.FIDelClient.Close()
	if s.gcWorker != nil {
		s.gcWorker.Close()
	}

	close(s.closed)
	if err := s.client.Close(); err != nil {
		return errors.Trace(err)
	}

	if s.txnLatches != nil {
		s.txnLatches.Close()
	}
	s.regionCache.Close()
	return nil
}

func (s *einsteindbStore) UUID() string {
	return s.uuid
}

func (s *einsteindbStore) CurrentVersion() (ekv.Version, error) {
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	startTS, err := s.getTimestampWithRetry(bo)
	if err != nil {
		return ekv.NewVersion(0), errors.Trace(err)
	}
	return ekv.NewVersion(startTS), nil
}

func (s *einsteindbStore) getTimestampWithRetry(bo *Backoffer) (uint64, error) {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("einsteindbStore.getTimestampWithRetry", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	for {
		startTS, err := s.oracle.GetTimestamp(bo.ctx)
		// mockGetTSErrorInRetry should wait MockCommitErrorOnce first, then will run into retry() logic.
		// Then mockGetTSErrorInRetry will return retryable error when first retry.
		// Before PR #8743, we don't cleanup txn after meet error such as error like: FIDel server timeout
		// This may cause duplicate data to be written.
		failpoint.Inject("mockGetTSErrorInRetry", func(val failpoint.Value) {
			if val.(bool) && !ekv.IsMockCommitErrorEnable() {
				err = ErrFIDelServerTimeout.GenWithStackByArgs("mock FIDel timeout")
			}
		})

		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(BoFIDelRPC, errors.Errorf("get timestamp failed: %v", err))
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
}

func (s *einsteindbStore) nextReplicaReadSeed() uint32 {
	return atomic.AddUint32(&s.replicaReadSeed, 1)
}

func (s *einsteindbStore) GetClient() ekv.Client {
	return &INTERLOCKClient{
		causetstore:     s,
		replicaReadSeed: s.nextReplicaReadSeed(),
	}
}

func (s *einsteindbStore) GetOracle() oracle.Oracle {
	return s.oracle
}

func (s *einsteindbStore) Name() string {
	return "EinsteinDB"
}

func (s *einsteindbStore) Describe() string {
	return "EinsteinDB is a distributed transactional key-value database"
}

func (s *einsteindbStore) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, ekv.ErrNotImplemented
}

func (s *einsteindbStore) SupportDeleteRange() (supported bool) {
	return !s.mock
}

func (s *einsteindbStore) SendReq(bo *Backoffer, req *einsteindbrpc.Request, regionID RegionVerID, timeout time.Duration) (*einsteindbrpc.Response, error) {
	sender := NewRegionRequestSender(s.regionCache, s.client)
	return sender.SendReq(bo, req, regionID, timeout)
}

func (s *einsteindbStore) GetRegionCache() *RegionCache {
	return s.regionCache
}

func (s *einsteindbStore) GetLockResolver() *LockResolver {
	return s.lockResolver
}

func (s *einsteindbStore) GetGCHandler() GCHandler {
	return s.gcWorker
}

func (s *einsteindbStore) Closed() <-chan struct{} {
	return s.closed
}

func (s *einsteindbStore) GetSafePointKV() SafePointKV {
	return s.ekv
}

func (s *einsteindbStore) SetOracle(oracle oracle.Oracle) {
	s.oracle = oracle
}

func (s *einsteindbStore) SetEinsteinDBClient(client Client) {
	s.client = client
}

func (s *einsteindbStore) GetEinsteinDBClient() (client Client) {
	return s.client
}

func init() {
	mc.cache = make(map[string]*einsteindbStore)
	rand.Seed(time.Now().UnixNano())
}
