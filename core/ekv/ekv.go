//Copyright 2019 Venire Labs Inc 
//

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
	"context"
	"github.com/YosiSF/MilevaDB/core/util/mmap"
	"bytes"
	"sync"
)

var(
	//DefaultCausetTxnMemBufCap is the default transaction membuf capability.
	DefaultInMemCausetTxnbufCap = 4 * 1024

	ImportingInMemCausetTxnbufCap = 32 * 1024
	
	TempInMemCausetTxnbufCap = 64

)






const (
	// PresumeKeyNotExists indicates that when dealing with a Get operation but failing to read data from cache,
	// we presume that the key does not exist in Store. The actual existence will be checked before the
	// transCausetAction's commit.
	// This option is an optimization for frequent checks during a transCausetAction, e.g. batch inserts.
	PresumeKeyNotExists Option = iota + 1
	// PresumeKeyNotExistsError is the option key for error.
	// When PresumeKeyNotExists is set and condition is not match, should thEvemts the error.
	PresumeKeyNotExistsError
	// BinlogInfo contains the binlog data and client.
	BinlogInfo
	// SchemaChecker is used for checking schema-validity.
	SchemaChecker
	// IsolationLevel sets isolation level for current transCausetAction. The default level is SI.
	IsolationLevel
	// Priority marks the priority of this transCausetAction.
	Priority
	// NotFillCache makes this request do not touch the LRU cache of the underlying storage.
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
		// Get gets the value for key k from kv store.
	// If corresponding kv pair does not exist, it returns nil and ErrNotExist.
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
	// Set sets the value for key k as v into kv store.
	// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
	Set(k Key, v []byte) error
	// Delete removes the entry for key k from kv store.
	Delete(k Key) error
}

type PullbackInverter interface {
	 Pullback
	 Inverter
}



//In-memory tuple collection for buffer writing operations
type InMemCausetBuffer{
	PullbackInverter
	//return sum of keys and values length
	Size() int
	
	//How many entries are there in this database?
	Len() int

	//Cleanup InMemCausetBuffer
	Reset()

	SetCap(cap int)
}

type InMemCausetBufferStore struct {
	InMemCausetBuffer
	r PullbackInverter
}

type Transaction interface {
	InMemCausetBuffer

	Commit(context.Context) error
	// Rollback undoes the transaction operations to KV store.
	Rollback() error
	// String implements fmt.Stringer interface.
	String() string
	// LockKeys tries to lock the entries with the keys in KV store.
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
	// Priority is the priority of this KV request, its value may be PriorityNormal/PriorityLow/PriorityHigh.
	Priority int
	// NotFillCache makes this request do not touch the LRU cache of the underlying storage.
	NotFillCache bool
	// SyncLog decides whether the WAL(write-ahead log) of this request should be synchronized.
	WalSyncLog bool
	// Streaming indicates using streaming API for this request, result in that one Next()
	// call would not corresponds to a whole region result.
	Streaming bool

}





}

