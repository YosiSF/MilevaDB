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

// Package einsteindb provides tcp connection to kvserver.
package einsteindb

import (
	"context"
	"io"
	"math"
	"runtime/trace"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-midbseware/tracing/opentracing"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/debugpb"
	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// MaxRecvMsgSize set max gRPC receive message size received from server. If any message size is larger than
// current value, an error will be reported from gRPC.
var MaxRecvMsgSize = math.MaxInt64

// Timeout durations.
var (
	dialTimeout               = 5 * time.Second
	readTimeoutShort          = 20 * time.Second   // For requests that read/write several key-values.
	ReadTimeoutMedium         = 60 * time.Second   // For requests that may need scan region.
	ReadTimeoutLong           = 150 * time.Second  // For requests that may need scan region multiple times.
	ReadTimeoutUltraLong      = 3600 * time.Second // For requests that may scan many regions for tiflash.
	GCTimeout                 = 5 * time.Minute
	UnsafeDestroyRangeTimeout = 5 * time.Minute
	AccessLockObserverTimeout = 10 * time.Second
)

const (
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// Close should release all data.
	Close() error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error)
}

type connArray struct {
	// The target host.
	target string

	index uint32
	v     []*grpc.ClientConn
	// streamTimeout binds with a background goroutine to process interlock streaming timeout.
	streamTimeout chan *einsteindbrpc.Lease
	dialTimeout   time.Duration
	// batchConn is not null when batch is enabled.
	*batchConn
	done chan struct{}
}

func newConnArray(maxSize uint, addr string, security config.Security, idleNotify *uint32, enableBatch bool, dialTimeout time.Duration) (*connArray, error) {
	a := &connArray{
		index:         0,
		v:             make([]*grpc.ClientConn, maxSize),
		streamTimeout: make(chan *einsteindbrpc.Lease, 1024),
		done:          make(chan struct{}),
		dialTimeout:   dialTimeout,
	}
	if err := a.Init(addr, security, idleNotify, enableBatch); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connArray) Init(addr string, security config.Security, idleNotify *uint32, enableBatch bool) error {
	a.target = addr

	opt := grpc.WithInsecure()
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	cfg := config.GetGlobalConfig()
	var (
		unaryInterceptor  grpc.UnaryClientInterceptor
		streamInterceptor grpc.StreamClientInterceptor
	)
	if cfg.OpenTracing.Enable {
		unaryInterceptor = grpc_opentracing.UnaryClientInterceptor()
		streamInterceptor = grpc_opentracing.StreamClientInterceptor()
	}

	allowBatch := (cfg.EinsteinDBClient.MaxBatchSize > 0) && enableBatch
	if allowBatch {
		a.batchConn = newBatchConn(uint(len(a.v)), cfg.EinsteinDBClient.MaxBatchSize, idleNotify)
		a.pendingRequests = metrics.EinsteinDBPendingBatchRequests.WithLabelValues(a.target)
	}
	keepAlive := cfg.EinsteinDBClient.GrpcKeepAliveTime
	keepAliveTimeout := cfg.EinsteinDBClient.GrpcKeepAliveTimeout
	for i := range a.v {
		ctx, cancel := context.WithTimeout(context.Background(), a.dialTimeout)
		conn, err := grpc.DialContext(
			ctx,
			addr,
			opt,
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithUnaryInterceptor(unaryInterceptor),
			grpc.WithStreamInterceptor(streamInterceptor),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxRecvMsgSize)),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond, // Default was 1s.
					Multiplier: 1.6,                    // Default
					Jitter:     0.2,                    // Default
					MaxDelay:   3 * time.Second,        // Default was 120s.
				},
				MinConnectTimeout: a.dialTimeout,
			}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                time.Duration(keepAlive) * time.Second,
				Timeout:             time.Duration(keepAliveTimeout) * time.Second,
				PermitWithoutStream: true,
			}),
		)
		cancel()
		if err != nil {
			// Cleanup if the initialization fails.
			a.Close()
			return errors.Trace(err)
		}
		a.v[i] = conn

		if allowBatch {
			batchClient := &batchCommandsClient{
				target:              a.target,
				conn:                conn,
				batched:             sync.Map{},
				idAlloc:             0,
				closed:              0,
				einsteindbClientCfg: cfg.EinsteinDBClient,
				einsteindbLoad:      &a.einsteindbTransportLayerLoad,
				dialTimeout:         a.dialTimeout,
			}
			a.batchCommandsClients = append(a.batchCommandsClients, batchClient)
		}
	}
	go einsteindbrpc.CheckStreamTimeoutLoop(a.streamTimeout, a.done)
	if allowBatch {
		go a.batchSendLoop(cfg.EinsteinDBClient)
	}

	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
	if a.batchConn != nil {
		a.batchConn.Close()
	}

	for i, c := range a.v {
		if c != nil {
			err := c.Close()
			terror.Log(errors.Trace(err))
			a.v[i] = nil
		}
	}

	close(a.done)
}

// rpcClient is RPC client struct.
// TODO: Add flow control between RPC clients in MilevaDB ond RPC servers in EinsteinDB.
// Since we use shared client connection to communicate to the same EinsteinDB, it's possible
// that there are too many concurrent requests which overload the service of EinsteinDB.
type rpcClient struct {
	sync.RWMutex

	conns    map[string]*connArray
	security config.Security

	idleNotify uint32
	// Periodically check whether there is any connection that is idle and then close and remove these connections.
	// Implement background cleanup.
	isClosed    bool
	dialTimeout time.Duration
}

func newRPCClient(security config.Security, opts ...func(c *rpcClient)) *rpcClient {
	cli := &rpcClient{
		conns:       make(map[string]*connArray),
		security:    security,
		dialTimeout: dialTimeout,
	}
	for _, opt := range opts {
		opt(cli)
	}
	return cli
}

// NewTestRPCClient is for some external tests.
func NewTestRPCClient(security config.Security) Client {
	return newRPCClient(security)
}

func (c *rpcClient) getConnArray(addr string, enableBatch bool, opt ...func(cfg *config.EinsteinDBClient)) (*connArray, error) {
	c.RLock()
	if c.isClosed {
		c.RUnlock()
		return nil, errors.Errorf("rpcClient is closed")
	}
	array, ok := c.conns[addr]
	c.RUnlock()
	if !ok {
		var err error
		array, err = c.createConnArray(addr, enableBatch, opt...)
		if err != nil {
			return nil, err
		}
	}
	return array, nil
}

func (c *rpcClient) createConnArray(addr string, enableBatch bool, opts ...func(cfg *config.EinsteinDBClient)) (*connArray, error) {
	c.Lock()
	defer c.Unlock()
	array, ok := c.conns[addr]
	if !ok {
		var err error
		client := config.GetGlobalConfig().EinsteinDBClient
		for _, opt := range opts {
			opt(&client)
		}
		array, err = newConnArray(client.GrpcConnectionCount, addr, c.security, &c.idleNotify, enableBatch, c.dialTimeout)
		if err != nil {
			return nil, err
		}
		c.conns[addr] = array
	}
	return array, nil
}

func (c *rpcClient) closeConns() {
	c.Lock()
	if !c.isClosed {
		c.isClosed = true
		// close all connections
		for _, array := range c.conns {
			array.Close()
		}
	}
	c.Unlock()
}

var sendReqHistCache sync.Map

type sendReqHistCacheKey struct {
	tp einsteindbrpc.CmdType
	id uint64
}

func (c *rpcClient) uFIDelateEinsteinDBSendReqHistogram(req *einsteindbrpc.Request, start time.Time) {
	key := sendReqHistCacheKey{
		req.Type,
		req.Context.GetPeer().GetStoreId(),
	}

	v, ok := sendReqHistCache.Load(key)
	if !ok {
		reqType := req.Type.String()
		storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
		v = metrics.EinsteinDBSendReqHistogram.WithLabelValues(reqType, storeID)
		sendReqHistCache.CausetStore(key, v)
	}

	v.(prometheus.Observer).Observe(time.Since(start).Seconds())
}

// SendRequest sends a Request to server and receives Response.
func (c *rpcClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("rpcClient.SendRequest", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	start := time.Now()
	defer func() {
		stmtExec := ctx.Value(execdetails.StmtExecDetailKey)
		if stmtExec != nil {
			detail := stmtExec.(*execdetails.StmtExecDetails)
			atomic.AddInt64(&detail.WaitKVResFIDeluration, int64(time.Since(start)))
		}
		c.uFIDelateEinsteinDBSendReqHistogram(req, start)
	}()

	if atomic.CompareAndSwapUint32(&c.idleNotify, 1, 0) {
		c.recycleIdleConnArray()
	}

	// MilevaDB will not send batch commands to TiFlash, to resolve the conflict with Batch Causet Request.
	enableBatch := req.StoreTp != ekv.MilevaDB && req.StoreTp != ekv.TiFlash
	connArray, err := c.getConnArray(addr, enableBatch)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// MilevaDB RPC server supports batch RPC, but batch connection will send heart beat, It's not necessary since
	// request to MilevaDB is not high frequency.
	if config.GetGlobalConfig().EinsteinDBClient.MaxBatchSize > 0 && enableBatch {
		if batchReq := req.ToBatchCommandsRequest(); batchReq != nil {
			defer trace.StartRegion(ctx, req.Type.String()).End()
			return sendBatchRequest(ctx, addr, connArray.batchConn, batchReq, timeout)
		}
	}

	clientConn := connArray.Get()
	if state := clientConn.GetState(); state == connectivity.TransientFailure {
		storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
		metrics.GRPCConnTransientFailureCounter.WithLabelValues(addr, storeID).Inc()
	}

	if req.IsDebugReq() {
		client := debugpb.NewDebugClient(clientConn)
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return einsteindbrpc.CallDebugRPC(ctx1, client, req)
	}

	client := einsteindbpb.NewEinsteinDBClient(clientConn)

	if req.Type == einsteindbrpc.CmdBatchINTERLOCK {
		return c.getBatchINTERLOCKStreamResponse(ctx, client, req, timeout, connArray)
	}

	if req.Type == einsteindbrpc.CmdINTERLOCKStream {
		return c.getINTERLOCKStreamResponse(ctx, client, req, timeout, connArray)
	}
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return einsteindbrpc.CallRPC(ctx1, client, req)
}

func (c *rpcClient) getINTERLOCKStreamResponse(ctx context.Context, client einsteindbpb.EinsteinDBClient, req *einsteindbrpc.Request, timeout time.Duration, connArray *connArray) (*einsteindbrpc.Response, error) {
	// interlocking_directorate streaming request.
	// Use context to support timeout for grpc streaming client.
	ctx1, cancel := context.WithCancel(ctx)
	// Should NOT call defer cancel() here because it will cancel further stream.Recv()
	// We put it in INTERLOCKStream.Lease.Cancel call this cancel at INTERLOCKStream.Close
	// TODO: add unit test for SendRequest.
	resp, err := einsteindbrpc.CallRPC(ctx1, client, req)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	// Put the lease object to the timeout channel, so it would be checked periodically.
	INTERLOCKStream := resp.Resp.(*einsteindbrpc.INTERLOCKStreamResponse)
	INTERLOCKStream.Timeout = timeout
	INTERLOCKStream.Lease.Cancel = cancel
	connArray.streamTimeout <- &INTERLOCKStream.Lease

	// Read the first streaming response to get INTERLOCKStreamResponse.
	// This can make error handling much easier, because SendReq() retry on
	// region error automatically.
	var first *interlock.Response
	first, err = INTERLOCKStream.Recv()
	if err != nil {
		if errors.Cause(err) != io.EOF {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Debug("INTERLOCKstream returns nothing for the request.")
	}
	INTERLOCKStream.Response = first
	return resp, nil

}

func (c *rpcClient) getBatchINTERLOCKStreamResponse(ctx context.Context, client einsteindbpb.EinsteinDBClient, req *einsteindbrpc.Request, timeout time.Duration, connArray *connArray) (*einsteindbrpc.Response, error) {
	// interlocking_directorate streaming request.
	// Use context to support timeout for grpc streaming client.
	ctx1, cancel := context.WithCancel(ctx)
	// Should NOT call defer cancel() here because it will cancel further stream.Recv()
	// We put it in INTERLOCKStream.Lease.Cancel call this cancel at INTERLOCKStream.Close
	// TODO: add unit test for SendRequest.
	resp, err := einsteindbrpc.CallRPC(ctx1, client, req)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	// Put the lease object to the timeout channel, so it would be checked periodically.
	INTERLOCKStream := resp.Resp.(*einsteindbrpc.BatchINTERLOCKStreamResponse)
	INTERLOCKStream.Timeout = timeout
	INTERLOCKStream.Lease.Cancel = cancel
	connArray.streamTimeout <- &INTERLOCKStream.Lease

	// Read the first streaming response to get INTERLOCKStreamResponse.
	// This can make error handling much easier, because SendReq() retry on
	// region error automatically.
	var first *interlock.BatchResponse
	first, err = INTERLOCKStream.Recv()
	if err != nil {
		if errors.Cause(err) != io.EOF {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Debug("batch INTERLOCKstream returns nothing for the request.")
	}
	INTERLOCKStream.BatchResponse = first
	return resp, nil

}

func (c *rpcClient) Close() error {
	// TODO: add a unit test for SendRequest After Closed
	c.closeConns()
	return nil
}
