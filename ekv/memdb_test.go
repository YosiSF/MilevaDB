MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// INTERLOCKyright 2020 Wenbin Xiao
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
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	. "github.com/whtcorpsinc/check"
	leveldb "github.com/whtcorpsinc/goleveldb/leveldb/memdb"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testleak"
)

func init() {
	testMode = true
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var (
	_ = Suite(&testKVSuite{})
	_ = Suite(&testMemDBSuite{})
)

type testMemDBSuite struct{}

// DeleteKey is used in test to verify the `deleteNode` used in `vlog.revertToCheckpoint`.
func (EDB *memdb) DeleteKey(key []byte) {
	x := EDB.traverse(key, false)
	if x.isNull() {
		return
	}
	EDB.size -= len(EDB.vlog.getValue(x.vptr))
	EDB.deleteNode(x)
}

func (s *testMemDBSuite) TestGetSet(c *C) {
	const cnt = 10000
	p := s.fillDB(cnt)

	var buf [4]byte
	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v, err := p.Get(context.TODO(), buf[:])
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, buf[:])
	}
}

func (s *testMemDBSuite) TestBigKV(c *C) {
	EDB := newMemDB()
	EDB.entrySizeLimit = math.MaxUint64
	EDB.bufferSizeLimit = math.MaxUint64
	EDB.Set([]byte{1}, make([]byte, 80<<20))
	c.Assert(EDB.vlog.blockSize, Equals, maxBlockSize)
	c.Assert(len(EDB.vlog.blocks), Equals, 1)
	h := EDB.Staging()
	EDB.Set([]byte{2}, make([]byte, 127<<20))
	EDB.Release(h)
	c.Assert(EDB.vlog.blockSize, Equals, maxBlockSize)
	c.Assert(len(EDB.vlog.blocks), Equals, 2)
	c.Assert(func() { EDB.Set([]byte{3}, make([]byte, maxBlockSize+1)) }, Panics, "alloc size is larger than max block size")
}

func (s *testMemDBSuite) TestIterator(c *C) {
	const cnt = 10000
	EDB := s.fillDB(cnt)

	var buf [4]byte
	var i int

	for it, _ := EDB.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		c.Assert(it.Value(), BytesEquals, buf[:])
		i++
	}
	c.Assert(i, Equals, cnt)

	i--
	for it, _ := EDB.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		c.Assert(it.Value(), BytesEquals, buf[:])
		i--
	}
	c.Assert(i, Equals, -1)
}

func (s *testMemDBSuite) TestDiscard(c *C) {
	const cnt = 10000
	EDB := newMemDB()
	base := s.deriveAndFill(0, cnt, 0, EDB)
	sz := EDB.Size()

	EDB.Cleanup(s.deriveAndFill(0, cnt, 1, EDB))
	c.Assert(EDB.Len(), Equals, cnt)
	c.Assert(EDB.Size(), Equals, sz)

	var buf [4]byte

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v, err := EDB.Get(context.TODO(), buf[:])
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, buf[:])
	}

	var i int
	for it, _ := EDB.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		c.Assert(it.Value(), BytesEquals, buf[:])
		i++
	}
	c.Assert(i, Equals, cnt)

	i--
	for it, _ := EDB.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		c.Assert(it.Value(), BytesEquals, buf[:])
		i--
	}
	c.Assert(i, Equals, -1)

	EDB.Cleanup(base)
	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		_, err := EDB.Get(context.TODO(), buf[:])
		c.Assert(err, NotNil)
	}
	it1, _ := EDB.Iter(nil, nil)
	it := it1.(*memdbIterator)
	it.seekToFirst()
	c.Assert(it.Valid(), IsFalse)
	it.seekToLast()
	c.Assert(it.Valid(), IsFalse)
	it.seek([]byte{0xff})
	c.Assert(it.Valid(), IsFalse)
}

func (s *testMemDBSuite) TestFlushOverwrite(c *C) {
	const cnt = 10000
	EDB := newMemDB()
	EDB.Release(s.deriveAndFill(0, cnt, 0, EDB))
	sz := EDB.Size()

	EDB.Release(s.deriveAndFill(0, cnt, 1, EDB))

	c.Assert(EDB.Len(), Equals, cnt)
	c.Assert(EDB.Size(), Equals, sz)

	var kbuf, vbuf [4]byte

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		v, err := EDB.Get(context.TODO(), kbuf[:])
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, vbuf[:])
	}

	var i int
	for it, _ := EDB.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		c.Assert([]byte(it.Key()), BytesEquals, kbuf[:])
		c.Assert(it.Value(), BytesEquals, vbuf[:])
		i++
	}
	c.Assert(i, Equals, cnt)

	i--
	for it, _ := EDB.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		c.Assert([]byte(it.Key()), BytesEquals, kbuf[:])
		c.Assert(it.Value(), BytesEquals, vbuf[:])
		i--
	}
	c.Assert(i, Equals, -1)
}

func (s *testMemDBSuite) TestComplexUFIDelate(c *C) {
	const (
		keep      = 3000
		overwrite = 6000
		insert    = 9000
	)

	EDB := newMemDB()
	EDB.Release(s.deriveAndFill(0, overwrite, 0, EDB))
	c.Assert(EDB.Len(), Equals, overwrite)
	EDB.Release(s.deriveAndFill(keep, insert, 1, EDB))
	c.Assert(EDB.Len(), Equals, insert)

	var kbuf, vbuf [4]byte

	for i := 0; i < insert; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i >= keep {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		v, err := EDB.Get(context.TODO(), kbuf[:])
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, vbuf[:])
	}
}

func (s *testMemDBSuite) TestNestedSandbox(c *C) {
	EDB := newMemDB()
	h0 := s.deriveAndFill(0, 200, 0, EDB)
	h1 := s.deriveAndFill(0, 100, 1, EDB)
	h2 := s.deriveAndFill(50, 150, 2, EDB)
	h3 := s.deriveAndFill(100, 120, 3, EDB)
	h4 := s.deriveAndFill(0, 150, 4, EDB)
	EDB.Cleanup(h4) // Discard (0..150 -> 4)
	EDB.Release(h3) // Flush (100..120 -> 3)
	EDB.Cleanup(h2) // Discard (100..120 -> 3) & (50..150 -> 2)
	EDB.Release(h1) // Flush (0..100 -> 1)
	EDB.Release(h0) // Flush (0..100 -> 1) & (0..200 -> 0)
	// The final result should be (0..100 -> 1) & (101..200 -> 0)

	var kbuf, vbuf [4]byte

	for i := 0; i < 200; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		v, err := EDB.Get(context.TODO(), kbuf[:])
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, vbuf[:])
	}

	var i int

	for it, _ := EDB.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		c.Assert([]byte(it.Key()), BytesEquals, kbuf[:])
		c.Assert(it.Value(), BytesEquals, vbuf[:])
		i++
	}
	c.Assert(i, Equals, 200)

	i--
	for it, _ := EDB.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		c.Assert([]byte(it.Key()), BytesEquals, kbuf[:])
		c.Assert(it.Value(), BytesEquals, vbuf[:])
		i--
	}
	c.Assert(i, Equals, -1)
}

func (s *testMemDBSuite) TestOverwrite(c *C) {
	const cnt = 10000
	EDB := s.fillDB(cnt)
	var buf [4]byte

	sz := EDB.Size()
	for i := 0; i < cnt; i += 3 {
		var newBuf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		binary.BigEndian.PutUint32(newBuf[:], uint32(i*10))
		EDB.Set(buf[:], newBuf[:])
	}
	c.Assert(EDB.Len(), Equals, cnt)
	c.Assert(EDB.Size(), Equals, sz)

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		val, _ := EDB.Get(context.TODO(), buf[:])
		v := binary.BigEndian.Uint32(val)
		if i%3 == 0 {
			c.Assert(v, Equals, uint32(i*10))
		} else {
			c.Assert(v, Equals, uint32(i))
		}
	}

	var i int

	for it, _ := EDB.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		v := binary.BigEndian.Uint32(it.Value())
		if i%3 == 0 {
			c.Assert(v, Equals, uint32(i*10))
		} else {
			c.Assert(v, Equals, uint32(i))
		}
		i++
	}
	c.Assert(i, Equals, cnt)

	i--
	for it, _ := EDB.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		c.Assert([]byte(it.Key()), BytesEquals, buf[:])
		v := binary.BigEndian.Uint32(it.Value())
		if i%3 == 0 {
			c.Assert(v, Equals, uint32(i*10))
		} else {
			c.Assert(v, Equals, uint32(i))
		}
		i--
	}
	c.Assert(i, Equals, -1)
}

func (s *testMemDBSuite) TestKVLargeThanBlock(c *C) {
	EDB := newMemDB()
	EDB.Set([]byte{1}, make([]byte, 1))
	EDB.Set([]byte{2}, make([]byte, 4096))
	c.Assert(len(EDB.vlog.blocks), Equals, 2)
	EDB.Set([]byte{3}, make([]byte, 3000))
	c.Assert(len(EDB.vlog.blocks), Equals, 2)
	val, err := EDB.Get(context.TODO(), []byte{3})
	c.Assert(err, IsNil)
	c.Assert(len(val), Equals, 3000)
}

func (s *testMemDBSuite) TestEmptyDB(c *C) {
	EDB := newMemDB()
	_, err := EDB.Get(context.TODO(), []byte{0})
	c.Assert(err, NotNil)
	it1, _ := EDB.Iter(nil, nil)
	it := it1.(*memdbIterator)
	it.seekToFirst()
	c.Assert(it.Valid(), IsFalse)
	it.seekToLast()
	c.Assert(it.Valid(), IsFalse)
	it.seek([]byte{0xff})
	c.Assert(it.Valid(), IsFalse)
}

func (s *testMemDBSuite) TestReset(c *C) {
	EDB := s.fillDB(1000)
	EDB.Reset()
	_, err := EDB.Get(context.TODO(), []byte{0, 0, 0, 0})
	c.Assert(err, NotNil)
	it1, _ := EDB.Iter(nil, nil)
	it := it1.(*memdbIterator)
	it.seekToFirst()
	c.Assert(it.Valid(), IsFalse)
	it.seekToLast()
	c.Assert(it.Valid(), IsFalse)
	it.seek([]byte{0xff})
	c.Assert(it.Valid(), IsFalse)
}

func (s *testMemDBSuite) TestInspectStage(c *C) {
	EDB := newMemDB()
	h1 := s.deriveAndFill(0, 1000, 0, EDB)
	h2 := s.deriveAndFill(500, 1000, 1, EDB)
	for i := 500; i < 1500; i++ {
		var kbuf [4]byte
		// don't uFIDelate in place
		var vbuf [5]byte
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+2))
		EDB.Set(kbuf[:], vbuf[:])
	}
	h3 := s.deriveAndFill(1000, 2000, 3, EDB)

	EDB.InspectStage(h3, func(key Key, _ KeyFlags, val []byte) {
		k := int(binary.BigEndian.Uint32(key))
		v := int(binary.BigEndian.Uint32(val))

		c.Assert(k >= 1000 && k < 2000, IsTrue)
		c.Assert(v-k, DeepEquals, 3)
	})

	EDB.InspectStage(h2, func(key Key, _ KeyFlags, val []byte) {
		k := int(binary.BigEndian.Uint32(key))
		v := int(binary.BigEndian.Uint32(val))

		c.Assert(k >= 500 && k < 2000, IsTrue)
		if k < 1000 {
			c.Assert(v-k, Equals, 2)
		} else {
			c.Assert(v-k, Equals, 3)
		}
	})

	EDB.Cleanup(h3)
	EDB.Release(h2)

	EDB.InspectStage(h1, func(key Key, _ KeyFlags, val []byte) {
		k := int(binary.BigEndian.Uint32(key))
		v := int(binary.BigEndian.Uint32(val))

		c.Assert(k >= 0 && k < 1500, IsTrue)
		if k < 500 {
			c.Assert(v-k, Equals, 0)
		} else {
			c.Assert(v-k, Equals, 2)
		}
	})

	EDB.Release(h1)
}

func (s *testMemDBSuite) TestDirty(c *C) {
	EDB := newMemDB()
	EDB.Set([]byte{1}, []byte{1})
	c.Assert(EDB.Dirty(), IsTrue)

	EDB = newMemDB()
	h := EDB.Staging()
	EDB.Set([]byte{1}, []byte{1})
	EDB.Cleanup(h)
	c.Assert(EDB.Dirty(), IsFalse)

	h = EDB.Staging()
	EDB.Set([]byte{1}, []byte{1})
	EDB.Release(h)
	c.Assert(EDB.Dirty(), IsTrue)

	// persistent flags will make memdb dirty.
	EDB = newMemDB()
	h = EDB.Staging()
	EDB.SetWithFlags([]byte{1}, []byte{1}, SetKeyLocked)
	EDB.Cleanup(h)
	c.Assert(EDB.Dirty(), IsTrue)

	// non-persistent flags will not make memdb dirty.
	EDB = newMemDB()
	h = EDB.Staging()
	EDB.SetWithFlags([]byte{1}, []byte{1}, SetPresumeKeyNotExists)
	EDB.Cleanup(h)
	c.Assert(EDB.Dirty(), IsFalse)
}

func (s *testMemDBSuite) TestFlags(c *C) {
	const cnt = 10000
	EDB := newMemDB()
	h := EDB.Staging()
	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		if i%2 == 0 {
			EDB.SetWithFlags(buf[:], buf[:], SetPresumeKeyNotExists, SetKeyLocked)
		} else {
			EDB.SetWithFlags(buf[:], buf[:], SetPresumeKeyNotExists)
		}
	}
	EDB.Cleanup(h)

	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		_, err := EDB.Get(context.TODO(), buf[:])
		c.Assert(err, NotNil)
		flags, err := EDB.GetFlags(buf[:])
		if i%2 == 0 {
			c.Assert(err, IsNil)
			c.Assert(flags.HasLocked(), IsTrue)
			c.Assert(flags.HasPresumeKeyNotExists(), IsFalse)
		} else {
			c.Assert(err, NotNil)
		}
	}

	c.Assert(EDB.Len(), Equals, 5000)
	c.Assert(EDB.Size(), Equals, 20000)

	it1, _ := EDB.Iter(nil, nil)
	it := it1.(*memdbIterator)
	c.Assert(it.Valid(), IsFalse)

	it.includeFlags = true
	it.init()

	for ; it.Valid(); it.Next() {
		k := binary.BigEndian.Uint32(it.Key())
		c.Assert(k%2 == 0, IsTrue)
	}

	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		EDB.UFIDelateFlags(buf[:], DelKeyLocked)
	}
	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		_, err := EDB.Get(context.TODO(), buf[:])
		c.Assert(err, NotNil)

		// UFIDelateFlags will create missing node.
		flags, err := EDB.GetFlags(buf[:])
		c.Assert(err, IsNil)
		c.Assert(flags.HasLocked(), IsFalse)
	}
}

func (s *testMemDBSuite) checkConsist(c *C, p1 *memdb, p2 *leveldb.EDB) {
	c.Assert(p1.Len(), Equals, p2.Len())
	c.Assert(p1.Size(), Equals, p2.Size())

	it1, _ := p1.Iter(nil, nil)
	it2 := p2.NewIterator(nil)

	var prevKey, prevVal []byte
	for it2.First(); it2.Valid(); it2.Next() {
		v, err := p1.Get(context.TODO(), it2.Key())
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, it2.Value())

		c.Assert([]byte(it1.Key()), BytesEquals, it2.Key())
		c.Assert(it1.Value(), BytesEquals, it2.Value())

		it, _ := p1.Iter(it2.Key(), nil)
		c.Assert([]byte(it.Key()), BytesEquals, it2.Key())
		c.Assert(it.Value(), BytesEquals, it2.Value())

		if prevKey != nil {
			it, _ = p1.IterReverse(it2.Key())
			c.Assert([]byte(it.Key()), BytesEquals, prevKey)
			c.Assert(it.Value(), BytesEquals, prevVal)
		}

		it1.Next()
		prevKey = it2.Key()
		prevVal = it2.Value()
	}

	it1, _ = p1.IterReverse(nil)
	for it2.Last(); it2.Valid(); it2.Prev() {
		c.Assert([]byte(it1.Key()), BytesEquals, it2.Key())
		c.Assert(it1.Value(), BytesEquals, it2.Value())
		it1.Next()
	}
}

func (s *testMemDBSuite) fillDB(cnt int) *memdb {
	EDB := newMemDB()
	h := s.deriveAndFill(0, cnt, 0, EDB)
	EDB.Release(h)
	return EDB
}

func (s *testMemDBSuite) deriveAndFill(start, end, valueBase int, EDB *memdb) StagingHandle {
	h := EDB.Staging()
	var kbuf, vbuf [4]byte
	for i := start; i < end; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+valueBase))
		EDB.Set(kbuf[:], vbuf[:])
	}
	return h
}

const (
	startIndex = 0
	testCount  = 2
	indexStep  = 2
)

type testKVSuite struct {
	bs []MemBuffer
}

func (s *testKVSuite) SetUpSuite(c *C) {
	s.bs = make([]MemBuffer, 1)
	s.bs[0] = newMemDB()
}

func (s *testKVSuite) ResetMembuffers() {
	s.bs[0] = newMemDB()
}

func insertData(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		err := buffer.Set(val, val)
		c.Assert(err, IsNil)
	}
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%010d", n))
}

func decodeInt(s []byte) int {
	var n int
	fmt.Sscanf(string(s), "%010d", &n)
	return n
}

func valToStr(c *C, iter Iterator) string {
	val := iter.Value()
	return string(val)
}

func checkNewIterator(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		iter, err := buffer.Iter(val, nil)
		c.Assert(err, IsNil)
		c.Assert([]byte(iter.Key()), BytesEquals, val)
		c.Assert(decodeInt([]byte(valToStr(c, iter))), Equals, i*indexStep)
		iter.Close()
	}

	// Test iterator Next()
	for i := startIndex; i < testCount-1; i++ {
		val := encodeInt(i * indexStep)
		iter, err := buffer.Iter(val, nil)
		c.Assert(err, IsNil)
		c.Assert([]byte(iter.Key()), BytesEquals, val)
		c.Assert(valToStr(c, iter), Equals, string(val))

		err = iter.Next()
		c.Assert(err, IsNil)
		c.Assert(iter.Valid(), IsTrue)

		val = encodeInt((i + 1) * indexStep)
		c.Assert([]byte(iter.Key()), BytesEquals, val)
		c.Assert(valToStr(c, iter), Equals, string(val))
		iter.Close()
	}

	// Non exist and beyond maximum seek test
	iter, err := buffer.Iter(encodeInt(testCount*indexStep), nil)
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)

	// Non exist but between existing keys seek test,
	// it returns the smallest key that larger than the one we are seeking
	inBetween := encodeInt((testCount-1)*indexStep - 1)
	last := encodeInt((testCount - 1) * indexStep)
	iter, err = buffer.Iter(inBetween, nil)
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsTrue)
	c.Assert([]byte(iter.Key()), Not(BytesEquals), inBetween)
	c.Assert([]byte(iter.Key()), BytesEquals, last)
	iter.Close()
}

func mustGet(c *C, buffer MemBuffer) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		val, err := buffer.Get(context.TODO(), s)
		c.Assert(err, IsNil)
		c.Assert(string(val), Equals, string(s))
	}
}

func (s *testKVSuite) TestGetSet(c *C) {
	defer testleak.AfterTest(c)()
	for _, buffer := range s.bs {
		insertData(c, buffer)
		mustGet(c, buffer)
	}
	s.ResetMembuffers()
}

func (s *testKVSuite) TestNewIterator(c *C) {
	defer testleak.AfterTest(c)()
	for _, buffer := range s.bs {
		// should be invalid
		iter, err := buffer.Iter(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(iter.Valid(), IsFalse)

		insertData(c, buffer)
		checkNewIterator(c, buffer)
	}
	s.ResetMembuffers()
}

func (s *testKVSuite) TestIterNextUntil(c *C) {
	defer testleak.AfterTest(c)()
	buffer := newMemDB()
	insertData(c, buffer)

	iter, err := buffer.Iter(nil, nil)
	c.Assert(err, IsNil)

	err = NextUntil(iter, func(k Key) bool {
		return false
	})
	c.Assert(err, IsNil)
	c.Assert(iter.Valid(), IsFalse)
}

func (s *testKVSuite) TestBasicNewIterator(c *C) {
	defer testleak.AfterTest(c)()
	for _, buffer := range s.bs {
		it, err := buffer.Iter([]byte("2"), nil)
		c.Assert(err, IsNil)
		c.Assert(it.Valid(), IsFalse)
	}
}

func (s *testKVSuite) TestNewIteratorMin(c *C) {
	defer testleak.AfterTest(c)()
	kvs := []struct {
		key   string
		value string
	}{
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001", "dagger-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0002", "1"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0003", "hello"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002", "dagger-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0002", "2"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0003", "hello"},
	}
	for _, buffer := range s.bs {
		for _, ekv := range kvs {
			err := buffer.Set([]byte(ekv.key), []byte(ekv.value))
			c.Assert(err, IsNil)
		}

		cnt := 0
		it, err := buffer.Iter(nil, nil)
		c.Assert(err, IsNil)
		for it.Valid() {
			cnt++
			err := it.Next()
			c.Assert(err, IsNil)
		}
		c.Assert(cnt, Equals, 6)

		it, err = buffer.Iter([]byte("DATA_test_main_db_tbl_tbl_test_record__00000000000000000000"), nil)
		c.Assert(err, IsNil)
		c.Assert(string([]byte(it.Key())), Equals, "DATA_test_main_db_tbl_tbl_test_record__00000000000000000001")
	}
	s.ResetMembuffers()
}

func (s *testKVSuite) TestMemDBStaging(c *C) {
	buffer := newMemDB()
	err := buffer.Set([]byte("x"), make([]byte, 2))
	c.Assert(err, IsNil)

	h1 := buffer.Staging()
	err = buffer.Set([]byte("x"), make([]byte, 3))
	c.Assert(err, IsNil)

	h2 := buffer.Staging()
	err = buffer.Set([]byte("yz"), make([]byte, 1))
	c.Assert(err, IsNil)

	v, _ := buffer.Get(context.Background(), []byte("x"))
	c.Assert(len(v), Equals, 3)

	buffer.Release(h2)

	v, _ = buffer.Get(context.Background(), []byte("yz"))
	c.Assert(len(v), Equals, 1)

	buffer.Cleanup(h1)

	v, _ = buffer.Get(context.Background(), []byte("x"))
	c.Assert(len(v), Equals, 2)
}

func (s *testKVSuite) TestBufferLimit(c *C) {
	buffer := newMemDB()
	buffer.bufferSizeLimit = 1000
	buffer.entrySizeLimit = 500

	err := buffer.Set([]byte("x"), make([]byte, 500))
	c.Assert(err, NotNil) // entry size limit

	err = buffer.Set([]byte("x"), make([]byte, 499))
	c.Assert(err, IsNil)
	err = buffer.Set([]byte("yz"), make([]byte, 499))
	c.Assert(err, NotNil) // buffer size limit

	err = buffer.Delete(make([]byte, 499))
	c.Assert(err, IsNil)

	err = buffer.Delete(make([]byte, 500))
	c.Assert(err, NotNil)
}

func (s *testKVSuite) TestBufferBatchGetter(c *C) {
	snap := &mockSnapshot{causetstore: newMemDB()}
	ka := []byte("a")
	kb := []byte("b")
	kc := []byte("c")
	kd := []byte("d")
	snap.causetstore.Set(ka, ka)
	snap.causetstore.Set(kb, kb)
	snap.causetstore.Set(kc, kc)
	snap.causetstore.Set(kd, kd)

	// midbse value is the same as snap
	midbse := newMemDB()
	midbse.Set(ka, []byte("a1"))
	midbse.Set(kc, []byte("c1"))

	buffer := newMemDB()
	buffer.Set(ka, []byte("a2"))
	buffer.Delete(kb)

	batchGetter := NewBufferBatchGetter(buffer, midbse, snap)
	result, err := batchGetter.BatchGet(context.Background(), []Key{ka, kb, kc, kd})
	c.Assert(err, IsNil)
	c.Assert(len(result), Equals, 3)
	c.Assert(string(result[string(ka)]), Equals, "a2")
	c.Assert(string(result[string(kc)]), Equals, "c1")
	c.Assert(string(result[string(kd)]), Equals, "d")
}
