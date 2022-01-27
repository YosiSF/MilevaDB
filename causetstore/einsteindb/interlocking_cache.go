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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/dgraph-io/ristretto"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/errors"
)

type INTERLOCKrCache struct {
	cache                   *ristretto.Cache
	admissionMaxSize        int
	admissionMinProcessTime time.Duration
}

type INTERLOCKrCacheValue struct {
	Key               []byte
	Data              []byte
	TimeStamp         uint64
	RegionID          uint64
	RegionDataVersion uint64
}

func (v *INTERLOCKrCacheValue) String() string {
	return fmt.Sprintf("{ Ts = %d, RegionID = %d, RegionDataVersion = %d, len(Data) = %d }",
		v.TimeStamp,
		v.RegionID,
		v.RegionDataVersion,
		len(v.Data))
}

const INTERLOCKrCacheValueSize = int(unsafe.Sizeof(INTERLOCKrCacheValue{}))

func (v *INTERLOCKrCacheValue) Len() int {
	return INTERLOCKrCacheValueSize + len(v.Key) + len(v.Data)
}

func newINTERLOCKrCache(config *config.interlocking_directorateCache) (*INTERLOCKrCache, error) {
	if config == nil || !config.Enable {
		return nil, nil
	}
	capacityInBytes := int64(config.CapacityMB * 1024.0 * 1024.0)
	if capacityInBytes == 0 {
		return nil, errors.New("Capacity must be > 0 to enable the cache")
	}
	maxEntityInBytes := int64(config.AdmissionMaxResultMB * 1024.0 * 1024.0)
	if maxEntityInBytes == 0 {
		return nil, errors.New("AdmissionMaxResultMB must be > 0 to enable the cache")
	}
	estimatedEntities := capacityInBytes / maxEntityInBytes * 2
	if estimatedEntities < 10 {
		estimatedEntities = 10
	}
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: estimatedEntities * 10,
		MaxCost:     capacityInBytes,
		BufferItems: 64,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := INTERLOCKrCache{
		cache:                   cache,
		admissionMaxSize:        int(maxEntityInBytes),
		admissionMinProcessTime: time.Duration(config.AdmissionMinProcessMs) * time.Millisecond,
	}
	return &c, nil
}

func INTERLOCKrCacheBuildKey(INTERLOCKReq *interlock.Request) ([]byte, error) {
	// Calculate amount of space to allocate
	if INTERLOCKReq.Tp > math.MaxUint8 {
		return nil, errors.New("Request Tp too big")
	}
	if len(INTERLOCKReq.Data) > math.MaxUint32 {
		return nil, errors.New("Cache data too big")
	}
	totalLength := 1 + 4 + len(INTERLOCKReq.Data)
	for _, r := range INTERLOCKReq.Ranges {
		if len(r.Start) > math.MaxUint16 {
			return nil, errors.New("Cache start key too big")
		}
		if len(r.End) > math.MaxUint16 {
			return nil, errors.New("Cache end key too big")
		}
		totalLength += 2 + len(r.Start) + 2 + len(r.End)
	}

	key := make([]byte, totalLength)

	// 1 byte Tp
	key[0] = uint8(INTERLOCKReq.Tp)
	dest := 1

	// 4 bytes Data len
	binary.LittleEndian.PutUint32(key[dest:], uint32(len(INTERLOCKReq.Data)))
	dest += 4

	// N bytes Data
	INTERLOCKy(key[dest:], INTERLOCKReq.Data)
	dest += len(INTERLOCKReq.Data)

	for _, r := range INTERLOCKReq.Ranges {
		// 2 bytes Key len
		binary.LittleEndian.PutUint16(key[dest:], uint16(len(r.Start)))
		dest += 2

		// N bytes Key
		INTERLOCKy(key[dest:], r.Start)
		dest += len(r.Start)

		// 2 bytes Key len
		binary.LittleEndian.PutUint16(key[dest:], uint16(len(r.End)))
		dest += 2

		// N bytes Key
		INTERLOCKy(key[dest:], r.End)
		dest += len(r.End)
	}

	return key, nil
}

// Get gets a cache item according to cache key.
func (c *INTERLOCKrCache) Get(key []byte) *INTERLOCKrCacheValue {
	if c == nil {
		return nil
	}
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.(*INTERLOCKrCacheValue)
	// ristretto does not handle hash collision, so check the key equality after getting a value.
	if !bytes.Equal(typedValue.Key, key) {
		return nil
	}
	return typedValue
}

// CheckAdmission checks whether an item is worth caching.
func (c *INTERLOCKrCache) CheckAdmission(dataSize int, processTime time.Duration) bool {
	if c == nil {
		return false
	}
	if dataSize == 0 || dataSize > c.admissionMaxSize {
		return false
	}
	if processTime < c.admissionMinProcessTime {
		return false
	}
	return true
}

// Set inserts an item to the cache.
// It is recommended to call `CheckAdmission` before inserting the item to the cache.
func (c *INTERLOCKrCache) Set(key []byte, value *INTERLOCKrCacheValue) bool {
	if c == nil {
		return false
	}
	// Always ensure that the `Key` in `value` is the `key` we received.
	value.Key = key
	return c.cache.Set(key, value, int64(value.Len()))
}
