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

package MilevaDB

import (
	"context"
	"sync"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
)

// Transaction options
const (
	// BinlogInfo contains the binlog data and client.
	BinlogInfo Option = iota + 1
	// SchemaChecker is used for checking schemaReplicant-validity.
	SchemaChecker
	// IsolationLevel sets isolation level for current transaction. The default level is SI.
	IsolationLevel
	// Priority marks the priority of this transaction.
	Priority
	// NotFillCache makes this request do not touch the LRU cache of the underlying storage.
	NotFillCache
	// SyncLog decides whether the WAL(write-ahead log) of this request should be synchronized.
	SyncLog
	// KeyOnly retrieve only keys, it can be used in scan now.
	KeyOnly
	// Pessimistic is defined for pessimistic dagger
	Pessimistic
	// SnapshotTS is defined to set snapshot ts.
	SnapshotTS
	// Set replica read
	ReplicaRead
	// Set task ID
	TaskID
	// SchemaReplicant is schemaReplicant version used by txn startTS.
	SchemaReplicant
	// DefCauslectRuntimeStats is used to enable collect runtime stats.
	DefCauslectRuntimeStats
	// SchemaAmender is used to amend mutations for pessimistic transactions
	SchemaAmender
	// SampleStep skips 'SampleStep - 1' number of keys after each returned key.
	SampleStep
	// CommitHook is a callback function called right after the transaction gets committed
	CommitHook
)

// Priority value for transaction priority.
const (
	PriorityNormal = iota
	PriorityLow
	PriorityHigh
)

// UnCommitIndexKVFlag uses to indicate the index key/value is no need to commit.
// This is used in the situation of the index key/value was unchanged when do uFIDelate.
// Usage:
// 1. For non-unique index: normally, the index value is '0'.
// Change the value to '1' indicate the index key/value is no need to commit.
// 2. For unique index: normally, the index value is the record handle ID, 8 bytes.
// Append UnCommitIndexKVFlag to the value indicate the index key/value is no need to commit.
const UnCommitIndexKVFlag byte = '1'

// MaxTxnTimeUse is the max time a Txn may use (in ms) from its begin to commit.
// We use it to abort the transaction to guarantee GC worker will not influence it.
const MaxTxnTimeUse = 24 * 60 * 60 * 1000

// IsoLevel is the transaction's isolation level.
type IsoLevel int

const (
	// SI stands for 'snapshot isolation'.
	SI IsoLevel = iota
	// RC stands for 'read committed'.
	RC
)

// ReplicaReadType is the type of replica to read data from
type ReplicaReadType byte

const (
	// ReplicaReadLeader stands for 'read from leader'.
	ReplicaReadLeader ReplicaReadType = 1 << iota
	// ReplicaReadFollower stands for 'read from follower'.
	ReplicaReadFollower
	// ReplicaReadMixed stands for 'read from leader and follower and learner'.
	ReplicaReadMixed
)

// IsFollowerRead checks if leader is going to be used to read data.
func (r ReplicaReadType) IsFollowerRead() bool {
	// In some cases the default value is 0, which should be treated as `ReplicaReadLeader`.
	return r != ReplicaReadLeader && r != 0
}

// Those limits is enforced to make sure the transaction can be well handled by EinsteinDB.
var (
	// TxnEntrySizeLimit is limit of single entry size (len(key) + len(value)).
	TxnEntrySizeLimit uint64 = config.DefTxnEntrySizeLimit
	// TxnTotalSizeLimit is limit of the sum of all entry size.
	TxnTotalSizeLimit uint64 = config.DefTxnTotalSizeLimit
)

// Getter is the interface for the Get method.
type Getter interface {
	// Get gets the value for key k from ekv causetstore.
	// If corresponding ekv pair does not exist, it returns nil and ErrNotExist.
	Get(ctx context.Context, k Key) ([]byte, error)
}

// Retriever is the interface wraps the basic Get and Seek methods.
type Retriever interface {
	Getter
	// Iter creates an Iterator positioned on the first entry that k <= entry's key.
	// If such entry is not found, it returns an invalid Iterator with no error.
	// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
	// The Iterator must be Closed after use.
	Iter(k Key, upperBound Key) (Iterator, error)

	// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
	// The returned iterator will iterate from greater key to smaller key.
	// If k is nil, the returned iterator will be positioned at the last key.
	// TODO: Add lower bound limit
	IterReverse(k Key) (Iterator, error)
}

// Mutator is the interface wraps the basic Set and Delete methods.
type Mutator interface {
	// Set sets the value for key k as v into ekv causetstore.
	// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
	Set(k Key, v []byte) error
	// Delete removes the entry for key k from ekv causetstore.
	Delete(k Key) error
}

// StagingHandle is the reference of a staging buffer.
type StagingHandle int

var (
	// InvalidStagingHandle is an invalid handler, MemBuffer will check handler to ensure safety.
	InvalidStagingHandle StagingHandle = 0
	// LastActiveStagingHandle is an special handler which always point to the last active staging buffer.
	LastActiveStagingHandle StagingHandle = -1
)

// RetrieverMutator is the interface that groups Retriever and Mutator interfaces.
type RetrieverMutator interface {
	Retriever
	Mutator
}

// MemBufferIterator is an Iterator with KeyFlags related functions.
type MemBufferIterator interface {
	Iterator
	HasValue() bool
	Flags() KeyFlags
}

// MemBuffer is an in-memory ekv collection, can be used to buffer write operations.
type MemBuffer interface {
	RetrieverMutator

	// RLock locks the MemBuffer for shared read.
	// In the most case, MemBuffer will only used by single goroutine,
	// but it will be read by multiple goroutine when combined with executor.UnionScanExec.
	// To avoid race introduced by executor.UnionScanExec, MemBuffer expose read dagger for it.
	RLock()
	// RUnlock unlocks the MemBuffer.
	RUnlock()

	// GetFlags returns the latest flags associated with key.
	GetFlags(Key) (KeyFlags, error)
	// IterWithFlags returns a MemBufferIterator.
	IterWithFlags(k Key, upperBound Key) MemBufferIterator
	// IterReverseWithFlags returns a reversed MemBufferIterator.
	IterReverseWithFlags(k Key) MemBufferIterator
	// SetWithFlags put key-value into the last active staging buffer with the given KeyFlags.
	SetWithFlags(Key, []byte, ...FlagsOp) error
	// UFIDelateFlags uFIDelate the flags associated with key.
	UFIDelateFlags(Key, ...FlagsOp)

	// Reset reset the MemBuffer to initial states.
	Reset()
	// DiscardValues releases the memory used by all values.
	// NOTE: any operation need value will panic after this function.
	DiscardValues()

	// Staging create a new staging buffer inside the MemBuffer.
	// Subsequent writes will be temporarily stored in this new staging buffer.
	// When you think all modifications looks good, you can call `Release` to public all of them to the upper level buffer.
	Staging() StagingHandle
	// Release publish all modifications in the latest staging buffer to upper level.
	Release(StagingHandle)
	// Cleanup cleanup the resources referenced by the StagingHandle.
	// If the changes are not published by `Release`, they will be discarded.
	Cleanup(StagingHandle)
	// InspectStage used to inspect the value uFIDelates in the given stage.
	InspectStage(StagingHandle, func(Key, KeyFlags, []byte))

	// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
	SnapshotGetter() Getter
	// SnapshotIter returns a Iterator for a snapshot of MemBuffer.
	SnapshotIter(k, upperbound Key) Iterator

	// Size returns sum of keys and values length.
	Size() int
	// Len returns the number of entries in the EDB.
	Len() int
	// Dirty returns whether the root staging buffer is uFIDelated.
	Dirty() bool
}

// Transaction defines the interface for operations inside a Transaction.
// This is not thread safe.
type Transaction interface {
	RetrieverMutator
	// Size returns sum of keys and values length.
	Size() int
	// Len returns the number of entries in the EDB.
	Len() int
	// Reset reset the Transaction to initial states.
	Reset()
	// Commit commits the transaction operations to KV causetstore.
	Commit(context.Context) error
	// Rollback undoes the transaction operations to KV causetstore.
	Rollback() error
	// String implements fmt.Stringer interface.
	String() string
	// LockKeys tries to dagger the entries with the keys in KV causetstore.
	LockKeys(ctx context.Context, lockCtx *LockCtx, keys ...Key) error
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
	// IsReadOnly checks if the transaction has only performed read operations.
	IsReadOnly() bool
	// StartTS returns the transaction start timestamp.
	StartTS() uint64
	// Valid returns if the transaction is valid.
	// A transaction become invalid after commit or rollback.
	Valid() bool
	// GetMemBuffer return the MemBuffer binding to this transaction.
	GetMemBuffer() MemBuffer
	// GetSnapshot returns the Snapshot binding to this transaction.
	GetSnapshot() Snapshot
	// GetUnionStore returns the UnionStore binding to this transaction.
	GetUnionStore() UnionStore
	// SetVars sets variables to the transaction.
	SetVars(vars *Variables)
	// GetVars gets variables from the transaction.
	GetVars() *Variables
	// BatchGet gets ekv from the memory buffer of statement and transaction, and the ekv storage.
	// Do not use len(value) == 0 or value == nil to represent non-exist.
	// If a key doesn't exist, there shouldn't be any corresponding entry in the result map.
	BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error)
	IsPessimistic() bool
}

// LockCtx contains information for LockKeys method.
type LockCtx struct {
	Killed                *uint32
	ForUFIDelateTS        uint64
	LockWaitTime          int64
	WaitStartTime         time.Time
	PessimisticLockWaited *int32
	LockKeysDuration      *int64
	LockKeysCount         *int32
	ReturnValues          bool
	Values                map[string]ReturnedValue
	ValuesLock            sync.Mutex
	LockExpired           *uint32
	Stats                 *execdetails.LockKeysDetails
}

// ReturnedValue pairs the Value and AlreadyLocked flag for PessimisticLock return values result.
type ReturnedValue struct {
	Value         []byte
	AlreadyLocked bool
}

// Client is used to send request to KV layer.
type Client interface {
	// Send sends request to KV layer, returns a Response.
	Send(ctx context.Context, req *Request, vars *Variables) Response

	// IsRequestTypeSupported checks if reqType and subType is supported.
	IsRequestTypeSupported(reqType, subType int64) bool
}

// ReqTypes.
const (
	ReqTypeSelect   = 101
	ReqTypeIndex    = 102
	ReqTypePosetDag = 103
	ReqTypeAnalyze  = 104
	ReqTypeChecksum = 105

	ReqSubTypeBasic          = 0
	ReqSubTypeDesc           = 10000
	ReqSubTypeGroupBy        = 10001
	ReqSubTypeTopN           = 10002
	ReqSubTypeSignature      = 10003
	ReqSubTypeAnalyzeIdx     = 10004
	ReqSubTypeAnalyzeDefCaus = 10005
)

// StoreType represents the type of a causetstore.
type StoreType uint8

const (
	// EinsteinDB means the type of a causetstore is EinsteinDB.
	EinsteinDB StoreType = iota
	// TiFlash means the type of a causetstore is TiFlash.
	TiFlash
	// MilevaDB means the type of a causetstore is MilevaDB.
	MilevaDB
	// UnSpecified means the causetstore type is unknown
	UnSpecified = 255
)

// Name returns the name of causetstore type.
func (t StoreType) Name() string {
	if t == TiFlash {
		return "tiflash"
	} else if t == MilevaDB {
		return "milevadb"
	} else if t == EinsteinDB {
		return "einsteindb"
	}
	return "unspecified"
}

// Request represents a ekv request.
type Request struct {
	// Tp is the request type.
	Tp        int64
	StartTs   uint64
	Data      []byte
	KeyRanges []KeyRange

	// Concurrency is 1, if it only sends the request to a single storage unit when
	// ResponseIterator.Next is called. If concurrency is greater than 1, the request will be
	// sent to multiple storage units concurrently.
	Concurrency int
	// IsolationLevel is the isolation level, default is SI.
	IsolationLevel IsoLevel
	// Priority is the priority of this KV request, its value may be PriorityNormal/PriorityLow/PriorityHigh.
	Priority int
	// memTracker is used to trace and control memory usage in co-processor layer.
	MemTracker *memory.Tracker
	// KeepOrder is true, if the response should be returned in order.
	KeepOrder bool
	// Desc is true, if the request is sent in descending order.
	Desc bool
	// NotFillCache makes this request do not touch the LRU cache of the underlying storage.
	NotFillCache bool
	// SyncLog decides whether the WAL(write-ahead log) of this request should be synchronized.
	SyncLog bool
	// Streaming indicates using streaming API for this request, result in that one Next()
	// call would not corresponds to a whole region result.
	Streaming bool
	// ReplicaRead is used for reading data from replicas, only follower is supported at this time.
	ReplicaRead ReplicaReadType
	// StoreType represents this request is sent to the which type of causetstore.
	StoreType StoreType
	// Cacheable is true if the request can be cached. Currently only deterministic PosetDag requests can be cached.
	Cacheable bool
	// SchemaVer is for any schemaReplicant-ful storage to validate schemaReplicant correctness if necessary.
	SchemaVar int64
	// BatchINTERLOCK indicates whether send batch interlock request to tiflash.
	BatchINTERLOCK bool
	// TaskID is an unique ID for an execution of a statement
	TaskID uint64
}

// ResultSubset represents a result subset from a single storage unit.
// TODO: Find a better interface for ResultSubset that can reuse bytes.
type ResultSubset interface {
	// GetData gets the data.
	GetData() []byte
	// GetStartKey gets the start key.
	GetStartKey() Key
	// MemSize returns how many bytes of memory this result use for tracing memory usage.
	MemSize() int64
	// RespTime returns the response time for the request.
	RespTime() time.Duration
}

// Response represents the response returned from KV layer.
type Response interface {
	// Next returns a resultSubset from a single storage unit.
	// When full result set is returned, nil is returned.
	Next(ctx context.Context) (resultSubset ResultSubset, err error)
	// Close response.
	Close() error
}

// Snapshot defines the interface for the snapshot fetched from KV causetstore.
type Snapshot interface {
	Retriever
	// BatchGet gets a batch of values from snapshot.
	BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error)
	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option. Only ReplicaRead is supported for snapshot
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
}

// BatchGetter is the interface for BatchGet.
type BatchGetter interface {
	// BatchGet gets a batch of values.
	BatchGet(ctx context.Context, keys []Key) (map[string][]byte, error)
}

// Driver is the interface that must be implemented by a KV storage.
type Driver interface {
	// Open returns a new CausetStorage.
	// The path is the string for storage specific format.
	Open(path string) (CausetStorage, error)
}

// CausetStorage defines the interface for storage.
// Isolation should be at least SI(SNAPSHOT ISOLATION)
type CausetStorage interface {
	// Begin transaction
	Begin() (Transaction, error)
	// BeginWithStartTS begins transaction with startTS.
	BeginWithStartTS(startTS uint64) (Transaction, error)
	// GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
	// if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
	GetSnapshot(ver Version) (Snapshot, error)
	// GetClient gets a client instance.
	GetClient() Client
	// Close causetstore
	Close() error
	// UUID return a unique ID which represents a CausetStorage.
	UUID() string
	// CurrentVersion returns current max committed version.
	CurrentVersion() (Version, error)
	// GetOracle gets a timestamp oracle client.
	GetOracle() oracle.Oracle
	// SupportDeleteRange gets the storage support delete range or not.
	SupportDeleteRange() (supported bool)
	// Name gets the name of the storage engine
	Name() string
	// Describe returns of brief introduction of the storage
	Describe() string
	// ShowStatus returns the specified status of the storage
	ShowStatus(ctx context.Context, key string) (interface{}, error)
}

// FnKeyCmp is the function for iterator the keys
type FnKeyCmp func(key Key) bool

// Iterator is the interface for a iterator on KV causetstore.
type Iterator interface {
	Valid() bool
	Key() Key
	Value() []byte
	Next() error
	Close()
}

// SplitblockStore is the ekv causetstore which supports split regions.
type SplitblockStore interface {
	SplitRegions(ctx context.Context, splitKey [][]byte, scatter bool, blockID *int64) (regionID []uint64, err error)
	WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error
	CheckRegionInScattering(regionID uint64) (bool, error)
}

// Used for pessimistic dagger wait time
// these two constants are special for dagger protocol with einsteindb
// 0 means always wait, -1 means nowait, others meaning dagger wait in milliseconds
var (
	LockAlwaysWait = int64(0)
	LockNoWait     = int64(-1)
)
