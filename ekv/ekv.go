//COPYRIGHT 2020 VENIRE LABS INC WHTCORPS INC ALL RIGHTS RESERVED
// HYBRID LICENSE [UNMAINTAINED]

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ekv

import (
	"encoding/hex"
	"context"
	"github.com/YosiSF/Milevanoedb/causetnetctx/stmtctx"
	"github.com/YosiSF/Milevanoedb/types"
	"github.com/YosiSF/Milevanoedb/util/codec"
	"github.com/YosiSF/Milevanoedb/BerolinaSQL/core/util/mmap"
	"bytes"
	"sync"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	_ "sync/atomic"
)
/*
var(
	//DefaultcontextTxnMemBufCap is the default transaction membuf capability.
	DefaultInMemcontextTxnbufCap = 4 * 1024

	ImportingInMemcontextTxnbufCap = 32 * 1024
	
	TempInMemcontextTxnbufCap = 64

)
*/

type CausetStore struct {
	MemBuffer
	r Retriever
}


// Key represents high-level Key type.
type Key []byte

// Next returns the next key in byte-order.
func (k Key) Next() Key {
	// add 0x0 to the end of key
	buf := make([]byte, len(k)+1)
	copy(buf, k)
	return buf
}

// PrefixNext returns the next prefix key.
//
// Assume there are keys like:
//
//   rowkey1
//   rowkey1_column1
//   rowkey1_column2
//   rowKey2
//
// If we seek 'rowkey1' Next, we will get 'rowkey1_column1'.
// If we seek 'rowkey1' PrefixNext, we will get 'rowkey2'.
func (k Key) PrefixNext() Key {
	buf := make([]byte, len(k))
	copy(buf, k)
	var i int
	for i = len(k) - 1; i >= 0; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}
	if i == -1 {
		copy(buf, k)
		buf = append(buf, 0)
	}
	return buf
}

// Cmp returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (k Key) Cmp(another Key) int {
	return bytes.Compare(k, another)
}

// HasPrefix tests whether the Key begins with prefix.
func (k Key) HasPrefix(prefix Key) bool {
	return bytes.HasPrefix(k, prefix)
}

// Clone returns a deep copy of the Key.
func (k Key) Clone() Key {
	ck := make([]byte, len(k))
	copy(ck, k)
	return ck
}

// String implements fmt.Stringer interface.
func (k Key) String() string {
	return hex.EncodeToString(k)
}

//Transitive keys.
type KeyRange struct {

	//Initial permission.
	StartKey Key
	//Deterministic.
	EndKey   Key
}


func (r *KeyRange) IsPoint() bool {
	if len(r.StartKey) != len(r.EndKey) {

		startLen := len(r.StartKey)
		return startLen+1 == len(r.EndKey) &&
			r.EndKey[startLen] == 0 &&
			bytes.Equal(r.StartKey, r.EndKey[:startLen])
	}

	i := len(r.StartKey) - 1
	for ; i >= 0; i-- {
		if r.StartKey[i] != 255 {
			break
		}
		if r.EndKey[i] != 0 {
			return false
		}
	}
	if i < 0 {
		// In case all bytes in StartKey are 255.
		return false
	}
	// The byte at diffIdx in StartKey should be one less than the byte at diffIdx in EndKey.
	// And bytes in StartKey and EndKey before diffIdx should be equal.
	diffOneIdx := i
	return r.StartKey[diffOneIdx]+1 == r.EndKey[diffOneIdx] &&
		bytes.Equal(r.StartKey[:diffOneIdx], r.EndKey[:diffOneIdx])
}







const (

	PresumeKeyNotExists Option = iota + 1

	PresumeKeyNotExistsError

	BinlogInfo

	SchemaChecker
	// IsolationLevel sets isolation level for current transcontextAction. The default level is SI.
	IsolationLevel

	Priority

	NotFillCache
	// SyncLog decides whether the WAL(write-ahead log) of this request should be synchronized.
	SyncLog
	// KeyOnly retrieve only keys, it can be used in scan now.
	KeyOnly

	PriorityNormal = iota

	PriorityLow 

	PriorityHigh

	//Transaction's SST Isolation Level Index.
	SI IsoLevel int

	//Read Committed.
	RC
)

//Primary tinyint byte key
type Key []byte

//return the next byte key
func (k Key) Next() Key {
	buf := make([]byte, len([]byte(k))+1)
	copy(buf, []byte(k))
	return buf
}

func (k Key) PrefixNext() Key {
	buf := make([]byte, len([]byte(k)))
	copy(buf, []byte(k))
	var i int
	for i = len(k) -1; i >= 0 ; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}

	if i == -1 {
		copy(buf, k)
		buf = append(buf, 0 )
	}

	return buf
}

type IsoLevel int



//Pullback is the interface wrapping Get/Seek

type Pullback interface {
		// Get gets the value for key k from ekv store.
	// If corresponding ekv pair does not exist, it returns nil and ErrNotExist.
	Get(k Key) ([]byte, error)
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

//Conjugator is the interface wrapping Set/Delete
type Conjugator interface {
	// Set sets the value for key k as v into ekv store.
	// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
	Set(k Key, v []byte) error
	// Delete removes the entry for key k from ekv store.
	Delete(k Key) error
}

type PullbackInverter interface {
	 Pullback
	 Inverter
}



//In-memory tuple collection for buffer writing operations
type InMemcontextBuffer{
	PullbackInverter
	//return sum of keys and values length
	Size() int
	
	//How many entries are there in this database?
	Len() int

	//Cleanup InMemcontextBuffer
	Reset()

	SetCap(cap int)
}

type InMemcontextBufferStore struct {
	InMemcontextBuffer
	r PullbackInverter
}

type Transaction interface {
	InMemcontextBuffer

	Commit(context.Context) error
	// Rollback undoes the transaction operations to ekv store.
	Rollback() error
	// String implements fmt.Stringer interface.
	String() string
	// LockKeys tries to lock the entries with the keys in ekv store.
	LockKeys(keys ...Key) error
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

}


type GraphRequest struct {
	//Graph request type
	GraphTypeRequest	int64
	StartTimestamp		uint64
	ByteCollection		[]byte
	KeyRanges			[]KeyRange

	KeepOrder bool
	// Desc is true, if the request is sent in descending order.
	Desc bool
	// Concurrency is 1, if it only sends the request to a single storage unit when
	// ResponseIterator.Next is called. If concurrency is greater than 1, the request will be
	// sent to multiple storage units concurrently.
	Concurrency int
	// IsolationLevel is the isolation level, default is SI.
	IsolationLevel IsoLevel
	// Priority is the priority of this ekv request, its value may be PriorityNormal/PriorityLow/PriorityHigh.
	Priority int
	// NotFillCache makes this request do not touch the LRU cache of the underlying storage.
	NotFillCache bool
	// SyncLog decides whether the WAL(write-ahead log) of this request should be synchronized.
	WalSyncLog bool
	// Streaming indicates using streaming API for this request, result in that one Next()
	// call would not corresponds to a whole region result.
	Streaming bool

}



// ResultSubset represents a result subset from a single storage unit.
// TODO: Find a better interface for ResultSubset that can reuse bytes.
type ResultSubset interface {
	// GetData gets the data.
	GetData() []byte
	// GetStartKey gets the start key.
	GetStartKey() Key
	// GetExecDetails gets the detail information.
	GetExecDetails() *execdetails.ExecDetails
	// MemSize returns how many bytes of memory this result use for tracing memory usage.
	MemSize() int64
}

// Response represents the response returned from ekv layer.
type GraphResponse interface {
	// Next returns a resultSubset from a single storage unit.
	// When full result set is returned, nil is returned.
	Next(ctx context.Context) (resultSubset ResultSubset, err error)
	// Close response.
	Close() error
}

// Snapshot defines the interface for the snapshot fetched from ekv store.
type Snapshot interface {
	PullbackInverter
	// BatchGet gets a batch of values from snapshot.
	BatchGet(keys []Key) (map[string][]byte, error)
	// SetPriority snapshot set the priority
	SetPriority(priority int)
}

/ Driver is the interface that must be implemented by a ekv storage.
type Driver interface {
	// Open returns a new Storage.
	// The path is the string for storage specific format.
	Open(path string) (Storage, error)
}

// Storage defines the interface for storage.
// Isolation should be at least SI(SNAPSHOT ISOLATION)
type Storage interface {
	// Begin transaction
	Begin() (Transaction, error)
	// BeginWithStartTS begins transaction with startTS.
	BeginWithStartTS(startTS uint64) (Transaction, error)
	// GetSnapshot gets a snapshot that is able to read any data which data is <= ver.
	// if ver is MaxVersion or > current max committed version, we will use current version for this snapshot.
	GetSnapshot(ver Version) (Snapshot, error)
	// GetClient gets a client instance.
	GetClient() Client
	// Close store
	Close() error
	// UUID return a unique ID which represents a Storage.
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


