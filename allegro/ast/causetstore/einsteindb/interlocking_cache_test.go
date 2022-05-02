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
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
)

type testinterlocking_directorateCacheSuite struct {
	OneByOneSuite
}

var _ = Suite(&testinterlocking_directorateCacheSuite{})

func (s *testinterlocking_directorateSuite) TestBuildCacheKey(c *C) {
	req := interlock.Request{
		Tp:      0xAB,
		StartTs: 0xAABBCC,
		Data:    []uint8{0x18, 0x0, 0x20, 0x0, 0x40, 0x0, 0x5a, 0x0},
		Ranges: []*interlock.KeyRange{
			{
				Start: ekv.Key{0x01},
				End:   ekv.Key{0x01, 0x02},
			},
			{
				Start: ekv.Key{0x01, 0x01, 0x02},
				End:   ekv.Key{0x01, 0x01, 0x03},
			},
		},
	}

	key, err := INTERLOCKrCacheBuildKey(&req)
	c.Assert(err, IsNil)
	expectKey := ""
	expectKey += "\xab"                             // 1 byte Tp
	expectKey += "\x08\x00\x00\x00"                 // 4 bytes Data len
	expectKey += "\x18\x00\x20\x00\x40\x00\x5a\x00" // Data
	expectKey += "\x01\x00"                         // 2 bytes StartKey len
	expectKey += "\x01"                             // StartKey
	expectKey += "\x02\x00"                         // 2 bytes EndKey len
	expectKey += "\x01\x02"                         // EndKey
	expectKey += "\x03\x00"                         // 2 bytes StartKey len
	expectKey += "\x01\x01\x02"                     // StartKey
	expectKey += "\x03\x00"                         // 2 bytes EndKey len
	expectKey += "\x01\x01\x03"                     // EndKey
	c.Assert(key, DeepEquals, []byte(expectKey))

	req = interlock.Request{
		Tp:      0xABCC, // Tp too big
		StartTs: 0xAABBCC,
		Data:    []uint8{0x18},
		Ranges:  []*interlock.KeyRange{},
	}

	_, err = INTERLOCKrCacheBuildKey(&req)
	c.Assert(err, NotNil)
}

func (s *testinterlocking_directorateSuite) TestDisable(c *C) {
	cache, err := newINTERLOCKrCache(&config.interlocking_directorateCache{Enable: false})
	c.Assert(err, IsNil)
	c.Assert(cache, IsNil)

	v := cache.Set([]byte("foo"), &INTERLOCKrCacheValue{})
	c.Assert(v, Equals, false)

	v2 := cache.Get([]byte("foo"))
	c.Assert(v2, IsNil)

	v = cache.CheckAdmission(1024, time.Second*5)
	c.Assert(v, Equals, false)

	cache, err = newINTERLOCKrCache(&config.interlocking_directorateCache{Enable: true, CapacityMB: 0, AdmissionMaxResultMB: 1})
	c.Assert(err, NotNil)
	c.Assert(cache, IsNil)

	cache, err = newINTERLOCKrCache(&config.interlocking_directorateCache{Enable: true, CapacityMB: 0.001})
	c.Assert(err, NotNil)
	c.Assert(cache, IsNil)

	cache, err = newINTERLOCKrCache(&config.interlocking_directorateCache{Enable: true, CapacityMB: 0.001, AdmissionMaxResultMB: 1})
	c.Assert(err, IsNil)
	c.Assert(cache, NotNil)
}

func (s *testinterlocking_directorateSuite) TestAdmission(c *C) {
	cache, err := newINTERLOCKrCache(&config.interlocking_directorateCache{Enable: true, AdmissionMinProcessMs: 5, AdmissionMaxResultMB: 1, CapacityMB: 1})
	c.Assert(err, IsNil)
	c.Assert(cache, NotNil)

	v := cache.CheckAdmission(0, 0)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(0, 4*time.Millisecond)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(0, 5*time.Millisecond)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(1, 0)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(1, 4*time.Millisecond)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(1, 5*time.Millisecond)
	c.Assert(v, Equals, true)

	v = cache.CheckAdmission(1024, 5*time.Millisecond)
	c.Assert(v, Equals, true)

	v = cache.CheckAdmission(1024*1024, 5*time.Millisecond)
	c.Assert(v, Equals, true)

	v = cache.CheckAdmission(1024*1024+1, 5*time.Millisecond)
	c.Assert(v, Equals, false)

	v = cache.CheckAdmission(1024*1024+1, 4*time.Millisecond)
	c.Assert(v, Equals, false)
}

func (s *testinterlocking_directorateSuite) TestCacheValueLen(c *C) {
	v := INTERLOCKrCacheValue{
		TimeStamp:         0x123,
		RegionID:          0x1,
		RegionDataVersion: 0x3,
	}
	// 72 = (8 byte pointer + 8 byte for length + 8 byte for cap) * 2 + 8 byte * 3
	c.Assert(v.Len(), Equals, 72)

	v = INTERLOCKrCacheValue{
		Key:               []byte("foobar"),
		Data:              []byte("12345678"),
		TimeStamp:         0x123,
		RegionID:          0x1,
		RegionDataVersion: 0x3,
	}
	c.Assert(v.Len(), Equals, 72+6+8)
}

func (s *testinterlocking_directorateSuite) TestGetSet(c *C) {
	cache, err := newINTERLOCKrCache(&config.interlocking_directorateCache{Enable: true, AdmissionMinProcessMs: 5, AdmissionMaxResultMB: 1, CapacityMB: 1})
	c.Assert(err, IsNil)
	c.Assert(cache, NotNil)

	v := cache.Get([]byte("foo"))
	c.Assert(v, IsNil)

	v2 := cache.Set([]byte("foo"), &INTERLOCKrCacheValue{
		Data:              []byte("bar"),
		TimeStamp:         0x123,
		RegionID:          0x1,
		RegionDataVersion: 0x3,
	})
	c.Assert(v2, Equals, true)

	// See https://github.com/dgraph-io/ristretto/blob/83508260cb49a2c3261c2774c991870fd18b5a1b/cache_test.go#L13
	// Changed from 10ms to 50ms to resist from unsblock CI environment.
	time.Sleep(time.Millisecond * 50)

	v = cache.Get([]byte("foo"))
	c.Assert(v, NotNil)
	c.Assert(v.Data, DeepEquals, []byte("bar"))
}
