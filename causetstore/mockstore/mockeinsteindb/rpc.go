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
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/debugpb"
	"github.com/whtcorpsinc/ekvproto/pkg/errorpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/metapb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/ekv"
)

// For gofail injection.
var undeterminedErr = terror.ErrResultUndetermined

const requestMaxSize = 8 * 1024 * 1024

func checkGoContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	if locked, ok := errors.Cause(err).(*ErrLocked); ok {
		return &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				Key:                locked.Key.Raw(),
				PrimaryLock:        locked.Primary,
				LockVersion:        locked.StartTS,
				LockTtl:            locked.TTL,
				TxnSize:            locked.TxnSize,
				LockType:           locked.LockType,
				LockForUFIDelateTs: locked.ForUFIDelateTS,
			},
		}
	}
	if alreadyExist, ok := errors.Cause(err).(*ErrKeyAlreadyExist); ok {
		return &kvrpcpb.KeyError{
			AlreadyExist: &kvrpcpb.AlreadyExist{
				Key: alreadyExist.Key,
			},
		}
	}
	if writeConflict, ok := errors.Cause(err).(*ErrConflict); ok {
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				Key:              writeConflict.Key,
				ConflictTs:       writeConflict.ConflictTS,
				ConflictCommitTs: writeConflict.ConflictCommitTS,
				StartTs:          writeConflict.StartTS,
			},
		}
	}
	if dead, ok := errors.Cause(err).(*ErrDeadlock); ok {
		return &kvrpcpb.KeyError{
			Deadlock: &kvrpcpb.Deadlock{
				LockTs:          dead.LockTS,
				LockKey:         dead.LockKey,
				DeadlockKeyHash: dead.DealockKeyHash,
			},
		}
	}
	if retryable, ok := errors.Cause(err).(ErrRetryable); ok {
		return &kvrpcpb.KeyError{
			Retryable: retryable.Error(),
		}
	}
	if expired, ok := errors.Cause(err).(*ErrCommitTSExpired); ok {
		return &kvrpcpb.KeyError{
			CommitTsExpired: &expired.CommitTsExpired,
		}
	}
	if tmp, ok := errors.Cause(err).(*ErrTxnNotFound); ok {
		return &kvrpcpb.KeyError{
			TxnNotFound: &tmp.TxnNotFound,
		}
	}
	return &kvrpcpb.KeyError{
		Abort: err.Error(),
	}
}

func convertToKeyErrors(errs []error) []*kvrpcpb.KeyError {
	var keyErrors = make([]*kvrpcpb.KeyError, 0)
	for _, err := range errs {
		if err != nil {
			keyErrors = append(keyErrors, convertToKeyError(err))
		}
	}
	return keyErrors
}

func convertToPbPairs(pairs []Pair) []*kvrpcpb.KvPair {
	kvPairs := make([]*kvrpcpb.KvPair, 0, len(pairs))
	for _, p := range pairs {
		var kvPair *kvrpcpb.KvPair
		if p.Err == nil {
			kvPair = &kvrpcpb.KvPair{
				Key:   p.Key,
				Value: p.Value,
			}
		} else {
			kvPair = &kvrpcpb.KvPair{
				Error: convertToKeyError(p.Err),
			}
		}
		kvPairs = append(kvPairs, kvPair)
	}
	return kvPairs
}

// rpcHandler mocks einsteindb's side handler behavior. In general, you may assume
// EinsteinDB just translate the logic from Go to Rust.
type rpcHandler struct {
	cluster   *Cluster
	mvccStore MVCCStore

	// storeID stores id for current request
	storeID uint64
	// startKey is used for handling normal request.
	startKey []byte
	endKey   []byte
	// rawStartKey is used for handling interlock request.
	rawStartKey []byte
	rawEndKey   []byte
	// isolationLevel is used for current request.
	isolationLevel kvrpcpb.IsolationLevel
	resolvedLocks  []uint64
}

func isTiFlashStore(causetstore *metapb.CausetStore) bool {
	for _, l := range causetstore.GetLabels() {
		if l.GetKey() == "engine" && l.GetValue() == "tiflash" {
			return true
		}
	}
	return false
}

func (h *rpcHandler) checkRequestContext(ctx *kvrpcpb.Context) *errorpb.Error {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != h.storeID {
		return &errorpb.Error{
			Message:       *proto.String("causetstore not match"),
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	region, leaderID := h.cluster.GetRegion(ctx.GetRegionId())
	// No region found.
	if region == nil {
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	var storePeer, leaderPeer *metapb.Peer
	for _, p := range region.Peers {
		if p.GetStoreId() == h.storeID {
			storePeer = p
		}
		if p.GetId() == leaderID {
			leaderPeer = p
		}
	}
	// The CausetStore does not contain a Peer of the Region.
	if storePeer == nil {
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// No leader.
	if leaderPeer == nil {
		return &errorpb.Error{
			Message: *proto.String("no leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// The Peer on the CausetStore is not leader. If it's tiflash causetstore , we pass this check.
	if storePeer.GetId() != leaderPeer.GetId() && !isTiFlashStore(h.cluster.GetStore(storePeer.GetStoreId())) {
		return &errorpb.Error{
			Message: *proto.String("not leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
				Leader:   leaderPeer,
			},
		}
	}
	// Region epoch does not match.
	if !proto.Equal(region.GetRegionEpoch(), ctx.GetRegionEpoch()) {
		nextRegion, _ := h.cluster.GetRegionByKey(region.GetEndKey())
		currentRegions := []*metapb.Region{region}
		if nextRegion != nil {
			currentRegions = append(currentRegions, nextRegion)
		}
		return &errorpb.Error{
			Message: *proto.String("epoch not match"),
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: currentRegions,
			},
		}
	}
	h.startKey, h.endKey = region.StartKey, region.EndKey
	h.isolationLevel = ctx.IsolationLevel
	h.resolvedLocks = ctx.ResolvedLocks
	return nil
}

func (h *rpcHandler) checkRequestSize(size int) *errorpb.Error {
	// EinsteinDB has a limitation on raft log size.
	// mockeinsteindb has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
}

func (h *rpcHandler) checkRequest(ctx *kvrpcpb.Context, size int) *errorpb.Error {
	if err := h.checkRequestContext(ctx); err != nil {
		return err
	}
	return h.checkRequestSize(size)
}

func (h *rpcHandler) checkKeyInRegion(key []byte) bool {
	return regionContains(h.startKey, h.endKey, NewMvccKey(key))
}

func (h *rpcHandler) handleKvGet(req *kvrpcpb.GetRequest) *kvrpcpb.GetResponse {
	if !h.checkKeyInRegion(req.Key) {
		panic("KvGet: key not in region")
	}

	val, err := h.mvccStore.Get(req.Key, req.GetVersion(), h.isolationLevel, req.Context.GetResolvedLocks())
	if err != nil {
		return &kvrpcpb.GetResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.GetResponse{
		Value: val,
	}
}

func (h *rpcHandler) handleKvScan(req *kvrpcpb.ScanRequest) *kvrpcpb.ScanResponse {
	endKey := MvccKey(h.endKey).Raw()
	var pairs []Pair
	if !req.Reverse {
		if !h.checkKeyInRegion(req.GetStartKey()) {
			panic("KvScan: startKey not in region")
		}
		if len(req.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.EndKey), h.endKey) < 0) {
			endKey = req.EndKey
		}
		pairs = h.mvccStore.Scan(req.GetStartKey(), endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel, req.Context.ResolvedLocks)
	} else {
		// EinsteinDB use range [end_key, start_key) for reverse scan.
		// Should use the req.EndKey to check in region.
		if !h.checkKeyInRegion(req.GetEndKey()) {
			panic("KvScan: startKey not in region")
		}

		// EinsteinDB use range [end_key, start_key) for reverse scan.
		// So the req.StartKey actually is the end_key.
		if len(req.StartKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.StartKey), h.endKey) < 0) {
			endKey = req.StartKey
		}

		pairs = h.mvccStore.ReverseScan(req.EndKey, endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel, req.Context.ResolvedLocks)
	}

	return &kvrpcpb.ScanResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleKvPrewrite(req *kvrpcpb.PrewriteRequest) *kvrpcpb.PrewriteResponse {
	regionID := req.Context.RegionId
	h.cluster.handleDelay(req.StartVersion, regionID)

	for _, m := range req.Mutations {
		if !h.checkKeyInRegion(m.Key) {
			panic("KvPrewrite: key not in region")
		}
	}
	errs := h.mvccStore.Prewrite(req)
	return &kvrpcpb.PrewriteResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h *rpcHandler) handleKvPessimisticLock(req *kvrpcpb.PessimisticLockRequest) *kvrpcpb.PessimisticLockResponse {
	for _, m := range req.Mutations {
		if !h.checkKeyInRegion(m.Key) {
			panic("KvPessimisticLock: key not in region")
		}
	}
	startTS := req.StartVersion
	regionID := req.Context.RegionId
	h.cluster.handleDelay(startTS, regionID)
	return h.mvccStore.PessimisticLock(req)
}

func simulateServerSideWaitLock(errs []error) {
	for _, err := range errs {
		if _, ok := err.(*ErrLocked); ok {
			time.Sleep(time.Millisecond * 5)
			break
		}
	}
}

func (h *rpcHandler) handleKvPessimisticRollback(req *kvrpcpb.PessimisticRollbackRequest) *kvrpcpb.PessimisticRollbackResponse {
	for _, key := range req.Keys {
		if !h.checkKeyInRegion(key) {
			panic("KvPessimisticRollback: key not in region")
		}
	}
	errs := h.mvccStore.PessimisticRollback(req.Keys, req.StartVersion, req.ForUFIDelateTs)
	return &kvrpcpb.PessimisticRollbackResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h *rpcHandler) handleKvCommit(req *kvrpcpb.CommitRequest) *kvrpcpb.CommitResponse {
	for _, k := range req.Keys {
		if !h.checkKeyInRegion(k) {
			panic("KvCommit: key not in region")
		}
	}
	var resp kvrpcpb.CommitResponse
	err := h.mvccStore.Commit(req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	return &resp
}

func (h *rpcHandler) handleKvCleanup(req *kvrpcpb.CleanupRequest) *kvrpcpb.CleanupResponse {
	if !h.checkKeyInRegion(req.Key) {
		panic("KvCleanup: key not in region")
	}
	var resp kvrpcpb.CleanupResponse
	err := h.mvccStore.Cleanup(req.Key, req.GetStartVersion(), req.GetCurrentTs())
	if err != nil {
		if commitTS, ok := errors.Cause(err).(ErrAlreadyCommitted); ok {
			resp.CommitVersion = uint64(commitTS)
		} else {
			resp.Error = convertToKeyError(err)
		}
	}
	return &resp
}

func (h *rpcHandler) handleKvCheckTxnStatus(req *kvrpcpb.CheckTxnStatusRequest) *kvrpcpb.CheckTxnStatusResponse {
	if !h.checkKeyInRegion(req.PrimaryKey) {
		panic("KvCheckTxnStatus: key not in region")
	}
	var resp kvrpcpb.CheckTxnStatusResponse
	ttl, commitTS, action, err := h.mvccStore.CheckTxnStatus(req.GetPrimaryKey(), req.GetLockTs(), req.GetCallerStartTs(), req.GetCurrentTs(), req.GetRollbackIfNotExist())
	if err != nil {
		resp.Error = convertToKeyError(err)
	} else {
		resp.LockTtl, resp.CommitVersion, resp.CausetAction = ttl, commitTS, action
	}
	return &resp
}

func (h *rpcHandler) handleTxnHeartBeat(req *kvrpcpb.TxnHeartBeatRequest) *kvrpcpb.TxnHeartBeatResponse {
	if !h.checkKeyInRegion(req.PrimaryLock) {
		panic("KvTxnHeartBeat: key not in region")
	}
	var resp kvrpcpb.TxnHeartBeatResponse
	ttl, err := h.mvccStore.TxnHeartBeat(req.PrimaryLock, req.StartVersion, req.AdviseLockTtl)
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	resp.LockTtl = ttl
	return &resp
}

func (h *rpcHandler) handleKvBatchGet(req *kvrpcpb.BatchGetRequest) *kvrpcpb.BatchGetResponse {
	for _, k := range req.Keys {
		if !h.checkKeyInRegion(k) {
			panic("KvBatchGet: key not in region")
		}
	}
	pairs := h.mvccStore.BatchGet(req.Keys, req.GetVersion(), h.isolationLevel, req.Context.GetResolvedLocks())
	return &kvrpcpb.BatchGetResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleMvccGetByKey(req *kvrpcpb.MvccGetByKeyRequest) *kvrpcpb.MvccGetByKeyResponse {
	debugger, ok := h.mvccStore.(MVCCDebugger)
	if !ok {
		return &kvrpcpb.MvccGetByKeyResponse{
			Error: "not implement",
		}
	}

	if !h.checkKeyInRegion(req.Key) {
		panic("MvccGetByKey: key not in region")
	}
	var resp kvrpcpb.MvccGetByKeyResponse
	resp.Info = debugger.MvccGetByKey(req.Key)
	return &resp
}

func (h *rpcHandler) handleMvccGetByStartTS(req *kvrpcpb.MvccGetByStartTsRequest) *kvrpcpb.MvccGetByStartTsResponse {
	debugger, ok := h.mvccStore.(MVCCDebugger)
	if !ok {
		return &kvrpcpb.MvccGetByStartTsResponse{
			Error: "not implement",
		}
	}
	var resp kvrpcpb.MvccGetByStartTsResponse
	resp.Info, resp.Key = debugger.MvccGetByStartTS(req.StartTs)
	return &resp
}

func (h *rpcHandler) handleKvBatchRollback(req *kvrpcpb.BatchRollbackRequest) *kvrpcpb.BatchRollbackResponse {
	err := h.mvccStore.Rollback(req.Keys, req.StartVersion)
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.BatchRollbackResponse{}
}

func (h *rpcHandler) handleKvScanLock(req *kvrpcpb.ScanLockRequest) *kvrpcpb.ScanLockResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	locks, err := h.mvccStore.ScanLock(startKey, endKey, req.GetMaxVersion())
	if err != nil {
		return &kvrpcpb.ScanLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ScanLockResponse{
		Locks: locks,
	}
}

func (h *rpcHandler) handleKvResolveLock(req *kvrpcpb.ResolveLockRequest) *kvrpcpb.ResolveLockResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	err := h.mvccStore.ResolveLock(startKey, endKey, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ResolveLockResponse{}
}

func (h *rpcHandler) handleKvGC(req *kvrpcpb.GCRequest) *kvrpcpb.GCResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	err := h.mvccStore.GC(startKey, endKey, req.GetSafePoint())
	if err != nil {
		return &kvrpcpb.GCResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.GCResponse{}
}

func (h *rpcHandler) handleKvDeleteRange(req *kvrpcpb.DeleteRangeRequest) *kvrpcpb.DeleteRangeResponse {
	if !h.checkKeyInRegion(req.StartKey) {
		panic("KvDeleteRange: key not in region")
	}
	var resp kvrpcpb.DeleteRangeResponse
	err := h.mvccStore.DeleteRange(req.StartKey, req.EndKey)
	if err != nil {
		resp.Error = err.Error()
	}
	return &resp
}

func (h *rpcHandler) handleKvRawGet(req *kvrpcpb.RawGetRequest) *kvrpcpb.RawGetResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawGetResponse{
			Error: "not implemented",
		}
	}
	return &kvrpcpb.RawGetResponse{
		Value: rawKV.RawGet(req.GetKey()),
	}
}

func (h *rpcHandler) handleKvRawBatchGet(req *kvrpcpb.RawBatchGetRequest) *kvrpcpb.RawBatchGetResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		// TODO should we add error ?
		return &kvrpcpb.RawBatchGetResponse{
			RegionError: &errorpb.Error{
				Message: "not implemented",
			},
		}
	}
	values := rawKV.RawBatchGet(req.Keys)
	kvPairs := make([]*kvrpcpb.KvPair, len(values))
	for i, key := range req.Keys {
		kvPairs[i] = &kvrpcpb.KvPair{
			Key:   key,
			Value: values[i],
		}
	}
	return &kvrpcpb.RawBatchGetResponse{
		Pairs: kvPairs,
	}
}

func (h *rpcHandler) handleKvRawPut(req *kvrpcpb.RawPutRequest) *kvrpcpb.RawPutResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawPutResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawPut(req.GetKey(), req.GetValue())
	return &kvrpcpb.RawPutResponse{}
}

func (h *rpcHandler) handleKvRawBatchPut(req *kvrpcpb.RawBatchPutRequest) *kvrpcpb.RawBatchPutResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawBatchPutResponse{
			Error: "not implemented",
		}
	}
	keys := make([][]byte, 0, len(req.Pairs))
	values := make([][]byte, 0, len(req.Pairs))
	for _, pair := range req.Pairs {
		keys = append(keys, pair.Key)
		values = append(values, pair.Value)
	}
	rawKV.RawBatchPut(keys, values)
	return &kvrpcpb.RawBatchPutResponse{}
}

func (h *rpcHandler) handleKvRawDelete(req *kvrpcpb.RawDeleteRequest) *kvrpcpb.RawDeleteResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawDeleteResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawDelete(req.GetKey())
	return &kvrpcpb.RawDeleteResponse{}
}

func (h *rpcHandler) handleKvRawBatchDelete(req *kvrpcpb.RawBatchDeleteRequest) *kvrpcpb.RawBatchDeleteResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawBatchDeleteResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawBatchDelete(req.Keys)
	return &kvrpcpb.RawBatchDeleteResponse{}
}

func (h *rpcHandler) handleKvRawDeleteRange(req *kvrpcpb.RawDeleteRangeRequest) *kvrpcpb.RawDeleteRangeResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawDeleteRangeResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawDeleteRange(req.GetStartKey(), req.GetEndKey())
	return &kvrpcpb.RawDeleteRangeResponse{}
}

func (h *rpcHandler) handleKvRawScan(req *kvrpcpb.RawScanRequest) *kvrpcpb.RawScanResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		errStr := "not implemented"
		return &kvrpcpb.RawScanResponse{
			RegionError: &errorpb.Error{
				Message: errStr,
			},
		}
	}

	var pairs []Pair
	if req.Reverse {
		lowerBound := h.startKey
		if bytes.Compare(req.EndKey, lowerBound) > 0 {
			lowerBound = req.EndKey
		}
		pairs = rawKV.RawReverseScan(
			req.StartKey,
			lowerBound,
			int(req.GetLimit()),
		)
	} else {
		upperBound := h.endKey
		if len(req.EndKey) > 0 && (len(upperBound) == 0 || bytes.Compare(req.EndKey, upperBound) < 0) {
			upperBound = req.EndKey
		}
		pairs = rawKV.RawScan(
			req.StartKey,
			upperBound,
			int(req.GetLimit()),
		)
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleSplitRegion(req *kvrpcpb.SplitRegionRequest) *kvrpcpb.SplitRegionResponse {
	keys := req.GetSplitKeys()
	resp := &kvrpcpb.SplitRegionResponse{Regions: make([]*metapb.Region, 0, len(keys)+1)}
	for i, key := range keys {
		k := NewMvccKey(key)
		region, _ := h.cluster.GetRegionByKey(k)
		if bytes.Equal(region.GetStartKey(), key) {
			continue
		}
		if i == 0 {
			// Set the leftmost region.
			resp.Regions = append(resp.Regions, region)
		}
		newRegionID, newPeerIDs := h.cluster.AllocID(), h.cluster.AllocIDs(len(region.Peers))
		newRegion := h.cluster.SplitRaw(region.GetId(), newRegionID, k, newPeerIDs, newPeerIDs[0])
		resp.Regions = append(resp.Regions, newRegion)
	}
	return resp
}

func drainRowsFromExecutor(ctx context.Context, e executor, req *fidelpb.PosetDagRequest) (fidelpb.Chunk, error) {
	var chunk fidelpb.Chunk
	for {
		event, err := e.Next(ctx)
		if err != nil {
			return chunk, errors.Trace(err)
		}
		if event == nil {
			return chunk, nil
		}
		for _, offset := range req.OutputOffsets {
			chunk.RowsData = append(chunk.RowsData, event[offset]...)
		}
	}
}

func (h *rpcHandler) handleBatchINTERLOCKRequest(ctx context.Context, req *interlock.BatchRequest) (*mockBatchCoFIDelataClient, error) {
	client := &mockBatchCoFIDelataClient{}
	for _, ri := range req.Regions {
		INTERLOCK := interlock.Request{
			Tp:      ekv.ReqTypePosetDag,
			Data:    req.Data,
			StartTs: req.StartTs,
			Ranges:  ri.Ranges,
		}
		_, exec, posetPosetDagReq, err := h.buildPosetDagExecutor(&INTERLOCK)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chunk, err := drainRowsFromExecutor(ctx, exec, posetPosetDagReq)
		if err != nil {
			return nil, errors.Trace(err)
		}
		client.chunks = append(client.chunks, chunk)
	}
	return client, nil
}

// Client is a client that sends RPC.
// This is same with einsteindb.Client, define again for avoid circle import.
type Client interface {
	// Close should release all data.
	Close() error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error)
}

// RPCClient sends ekv RPC calls to mock cluster. RPCClient mocks the behavior of
// a rpc client at einsteindb's side.
type RPCClient struct {
	Cluster       *Cluster
	MvccStore     MVCCStore
	streamTimeout chan *einsteindbrpc.Lease
	done          chan struct{}
	// rpcCli uses to redirects RPC request to MilevaDB rpc server, It is only use for test.
	// Mock MilevaDB rpc service will have circle import problem, so just use a real RPC client to send this RPC  server.
	// sync.Once uses to avoid concurrency initialize rpcCli.
	sync.Once
	rpcCli Client
}

// NewRPCClient creates an RPCClient.
// Note that close the RPCClient may close the underlying MvccStore.
func NewRPCClient(cluster *Cluster, mvccStore MVCCStore) *RPCClient {
	ch := make(chan *einsteindbrpc.Lease, 1024)
	done := make(chan struct{})
	go einsteindbrpc.CheckStreamTimeoutLoop(ch, done)
	return &RPCClient{
		Cluster:       cluster,
		MvccStore:     mvccStore,
		streamTimeout: ch,
		done:          done,
	}
}

func (c *RPCClient) getAndCheckStoreByAddr(addr string) (*metapb.CausetStore, error) {
	causetstore, err := c.Cluster.GetAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	if causetstore == nil {
		return nil, errors.New("connect fail")
	}
	if causetstore.GetState() == metapb.StoreState_Offline ||
		causetstore.GetState() == metapb.StoreState_Tombstone {
		return nil, errors.New("connection refused")
	}
	return causetstore, nil
}

func (c *RPCClient) checkArgs(ctx context.Context, addr string) (*rpcHandler, error) {
	if err := checkGoContext(ctx); err != nil {
		return nil, err
	}

	causetstore, err := c.getAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	handler := &rpcHandler{
		cluster:   c.Cluster,
		mvccStore: c.MvccStore,
		// set causetstore id for current request
		storeID: causetstore.GetId(),
	}
	return handler, nil
}

// GRPCClientFactory is the GRPC client factory.
// Use global variable to avoid circle import.
// TODO: remove this global variable.
var GRPCClientFactory func() Client

// redirectRequestToRPCServer redirects RPC request to MilevaDB rpc server, It is only use for test.
// Mock MilevaDB rpc service will have circle import problem, so just use a real RPC client to send this RPC  server.
func (c *RPCClient) redirectRequestToRPCServer(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	c.Once.Do(func() {
		if GRPCClientFactory != nil {
			c.rpcCli = GRPCClientFactory()
		}
	})
	if c.rpcCli == nil {
		return nil, errors.Errorf("GRPCClientFactory is nil")
	}
	return c.rpcCli.SendRequest(ctx, addr, req, timeout)
}

// SendRequest sends a request to mock cluster.
func (c *RPCClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("RPCClient.SendRequest", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	failpoint.Inject("rpcServerBusy", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(einsteindbrpc.GenRegionErrorResp(req, &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}))
		}
	})

	// increase coverage for mock einsteindb
	_ = req.Type.String()
	_ = req.ToBatchCommandsRequest()

	reqCtx := &req.Context
	resp := &einsteindbrpc.Response{}
	// When the causetstore type is MilevaDB, the request should handle over to MilevaDB rpc server to handle.
	if req.StoreTp == ekv.MilevaDB {
		return c.redirectRequestToRPCServer(ctx, addr, req, timeout)
	}

	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	switch req.Type {
	case einsteindbrpc.CmdGet:
		r := req.Get()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.GetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvGet(r)
	case einsteindbrpc.CmdScan:
		r := req.Scan()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.ScanResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvScan(r)

	case einsteindbrpc.CmdPrewrite:
		failpoint.Inject("rpcPrewriteResult", func(val failpoint.Value) {
			switch val.(string) {
			case "notLeader":
				failpoint.Return(&einsteindbrpc.Response{
					Resp: &kvrpcpb.PrewriteResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			}
		})

		r := req.Prewrite()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.PrewriteResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvPrewrite(r)
	case einsteindbrpc.CmdPessimisticLock:
		r := req.PessimisticLock()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.PessimisticLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvPessimisticLock(r)
	case einsteindbrpc.CmdPessimisticRollback:
		r := req.PessimisticRollback()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.PessimisticRollbackResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvPessimisticRollback(r)
	case einsteindbrpc.CmdCommit:
		failpoint.Inject("rpcCommitResult", func(val failpoint.Value) {
			switch val.(string) {
			case "timeout":
				failpoint.Return(nil, errors.New("timeout"))
			case "notLeader":
				failpoint.Return(&einsteindbrpc.Response{
					Resp: &kvrpcpb.CommitResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			case "keyError":
				failpoint.Return(&einsteindbrpc.Response{
					Resp: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{}},
				}, nil)
			}
		})

		r := req.Commit()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.CommitResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvCommit(r)
		failpoint.Inject("rpcCommitTimeout", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(nil, undeterminedErr)
			}
		})
	case einsteindbrpc.CmdCleanup:
		r := req.Cleanup()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.CleanupResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvCleanup(r)
	case einsteindbrpc.CmdCheckTxnStatus:
		r := req.CheckTxnStatus()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.CheckTxnStatusResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvCheckTxnStatus(r)
	case einsteindbrpc.CmdTxnHeartBeat:
		r := req.TxnHeartBeat()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.TxnHeartBeatResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleTxnHeartBeat(r)
	case einsteindbrpc.CmdBatchGet:
		r := req.BatchGet()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.BatchGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvBatchGet(r)
	case einsteindbrpc.CmdBatchRollback:
		r := req.BatchRollback()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.BatchRollbackResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvBatchRollback(r)
	case einsteindbrpc.CmdScanLock:
		r := req.ScanLock()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.ScanLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvScanLock(r)
	case einsteindbrpc.CmdResolveLock:
		r := req.ResolveLock()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.ResolveLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvResolveLock(r)
	case einsteindbrpc.CmdGC:
		r := req.GC()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.GCResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvGC(r)
	case einsteindbrpc.CmdDeleteRange:
		r := req.DeleteRange()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.DeleteRangeResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvDeleteRange(r)
	case einsteindbrpc.CmdRawGet:
		r := req.RawGet()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawGet(r)
	case einsteindbrpc.CmdRawBatchGet:
		r := req.RawBatchGet()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawBatchGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawBatchGet(r)
	case einsteindbrpc.CmdRawPut:
		r := req.RawPut()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawPutResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawPut(r)
	case einsteindbrpc.CmdRawBatchPut:
		r := req.RawBatchPut()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawBatchPutResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawBatchPut(r)
	case einsteindbrpc.CmdRawDelete:
		r := req.RawDelete()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawDeleteResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawDelete(r)
	case einsteindbrpc.CmdRawBatchDelete:
		r := req.RawBatchDelete()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawBatchDeleteResponse{RegionError: err}
		}
		resp.Resp = handler.handleKvRawBatchDelete(r)
	case einsteindbrpc.CmdRawDeleteRange:
		r := req.RawDeleteRange()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawDeleteRangeResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawDeleteRange(r)
	case einsteindbrpc.CmdRawScan:
		r := req.RawScan()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawScanResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleKvRawScan(r)
	case einsteindbrpc.CmdUnsafeDestroyRange:
		panic("unimplemented")
	case einsteindbrpc.CmdRegisterLockObserver:
		return nil, errors.New("unimplemented")
	case einsteindbrpc.CmdCheckLockObserver:
		return nil, errors.New("unimplemented")
	case einsteindbrpc.CmdRemoveLockObserver:
		return nil, errors.New("unimplemented")
	case einsteindbrpc.CmdPhysicalScanLock:
		return nil, errors.New("unimplemented")
	case einsteindbrpc.CmdINTERLOCK:
		r := req.Causet()
		if err := handler.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &interlock.Response{RegionError: err}
			return resp, nil
		}
		handler.rawStartKey = MvccKey(handler.startKey).Raw()
		handler.rawEndKey = MvccKey(handler.endKey).Raw()
		var res *interlock.Response
		switch r.GetTp() {
		case ekv.ReqTypePosetDag:
			res = handler.handleCoFIDelAGRequest(r)
		case ekv.ReqTypeAnalyze:
			res = handler.handleINTERLOCKAnalyzeRequest(r)
		case ekv.ReqTypeChecksum:
			res = handler.handleINTERLOCKChecksumRequest(r)
		default:
			panic(fmt.Sprintf("unknown interlock request type: %v", r.GetTp()))
		}
		resp.Resp = res
	case einsteindbrpc.CmdBatchINTERLOCK:
		failpoint.Inject("BatchINTERLOCKCancelled", func(value failpoint.Value) {
			if value.(bool) {
				failpoint.Return(nil, context.Canceled)
			}
		})

		failpoint.Inject("BatchINTERLOCKRpcErr"+addr, func(value failpoint.Value) {
			if value.(string) == addr {
				failpoint.Return(nil, errors.New("rpc error"))
			}
		})
		r := req.BatchINTERLOCK()
		if err := handler.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &einsteindbrpc.BatchINTERLOCKStreamResponse{
				EinsteinDB_Batchinterlocking_directorateClient: &mockBathINTERLOCKErrClient{Error: err},
				BatchResponse: &interlock.BatchResponse{
					OtherError: err.Message,
				},
			}
			return resp, nil
		}
		ctx1, cancel := context.WithCancel(ctx)
		batchINTERLOCKStream, err := handler.handleBatchINTERLOCKRequest(ctx1, r)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}
		batchResp := &einsteindbrpc.BatchINTERLOCKStreamResponse{EinsteinDB_Batchinterlocking_directorateClient: batchINTERLOCKStream}
		batchResp.Lease.Cancel = cancel
		batchResp.Timeout = timeout
		c.streamTimeout <- &batchResp.Lease

		first, err := batchResp.Recv()
		if err != nil {
			return nil, errors.Trace(err)
		}
		batchResp.BatchResponse = first
		resp.Resp = batchResp
	case einsteindbrpc.CmdINTERLOCKStream:
		r := req.Causet()
		if err := handler.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &einsteindbrpc.INTERLOCKStreamResponse{
				EinsteinDB_interlocking_directorateStreamClient: &mockINTERLOCKStreamErrClient{Error: err},
				Response: &interlock.Response{
					RegionError: err,
				},
			}
			return resp, nil
		}
		handler.rawStartKey = MvccKey(handler.startKey).Raw()
		handler.rawEndKey = MvccKey(handler.endKey).Raw()
		ctx1, cancel := context.WithCancel(ctx)
		INTERLOCKStream, err := handler.handleINTERLOCKStream(ctx1, r)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}

		streamResp := &einsteindbrpc.INTERLOCKStreamResponse{
			EinsteinDB_interlocking_directorateStreamClient: INTERLOCKStream,
		}
		streamResp.Lease.Cancel = cancel
		streamResp.Timeout = timeout
		c.streamTimeout <- &streamResp.Lease

		first, err := streamResp.Recv()
		if err != nil {
			return nil, errors.Trace(err)
		}
		streamResp.Response = first
		resp.Resp = streamResp
	case einsteindbrpc.CmdMvccGetByKey:
		r := req.MvccGetByKey()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.MvccGetByKeyResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleMvccGetByKey(r)
	case einsteindbrpc.CmdMvccGetByStartTs:
		r := req.MvccGetByStartTs()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.MvccGetByStartTsResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleMvccGetByStartTS(r)
	case einsteindbrpc.CmdSplitRegion:
		r := req.SplitRegion()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.SplitRegionResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleSplitRegion(r)
	// DebugGetRegionProperties is for fast analyze in mock einsteindb.
	case einsteindbrpc.CmdDebugGetRegionProperties:
		r := req.DebugGetRegionProperties()
		region, _ := c.Cluster.GetRegion(r.RegionId)
		var reqCtx kvrpcpb.Context
		scanResp := handler.handleKvScan(&kvrpcpb.ScanRequest{
			Context:  &reqCtx,
			StartKey: MvccKey(region.StartKey).Raw(),
			EndKey:   MvccKey(region.EndKey).Raw(),
			Version:  math.MaxUint64,
			Limit:    math.MaxUint32})
		resp.Resp = &debugpb.GetRegionPropertiesResponse{
			Props: []*debugpb.Property{{
				Name:  "mvcc.num_rows",
				Value: strconv.Itoa(len(scanResp.Pairs)),
			}}}
	default:
		return nil, errors.Errorf("unsupported this request type %v", req.Type)
	}
	return resp, nil
}

// Close closes the client.
func (c *RPCClient) Close() error {
	close(c.done)

	var err error
	if c.MvccStore != nil {
		err = c.MvccStore.Close()
		if err != nil {
			return err
		}
	}

	if c.rpcCli != nil {
		err = c.rpcCli.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
