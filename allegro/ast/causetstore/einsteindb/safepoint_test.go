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
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
)

type testSafePointSuite struct {
	OneByOneSuite
	causetstore *einsteindbStore
	prefix      string
}

var _ = Suite(&testSafePointSuite{})

func (s *testSafePointSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.causetstore = NewTestStore(c).(*einsteindbStore)
	s.prefix = fmt.Sprintf("seek_%d", time.Now().Unix())
}

func (s *testSafePointSuite) TearDownSuite(c *C) {
	err := s.causetstore.Close()
	c.Assert(err, IsNil)
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testSafePointSuite) beginTxn(c *C) *einsteindbTxn {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	return txn.(*einsteindbTxn)
}

func mymakeKeys(rowNum int, prefix string) []ekv.Key {
	keys := make([]ekv.Key, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		k := encodeKey(prefix, s08d("key", i))
		keys = append(keys, k)
	}
	return keys
}

func (s *testSafePointSuite) waitUntilErrorPlugIn(t uint64) {
	for {
		saveSafePoint(s.causetstore.GetSafePointKV(), t+10)
		cachedTime := time.Now()
		newSafePoint, err := loadSafePoint(s.causetstore.GetSafePointKV())
		if err == nil {
			s.causetstore.UFIDelateSPCache(newSafePoint, cachedTime)
			break
		}
		time.Sleep(time.Second)
	}
}

func (s *testSafePointSuite) TestSafePoint(c *C) {
	txn := s.beginTxn(c)
	for i := 0; i < 10; i++ {
		err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		c.Assert(err, IsNil)
	}
	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// for txn get
	txn2 := s.beginTxn(c)
	_, err = txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	c.Assert(err, IsNil)

	s.waitUntilErrorPlugIn(txn2.startTS)

	_, geterr2 := txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	c.Assert(geterr2, NotNil)
	isFallBehind := terror.ErrorEqual(errors.Cause(geterr2), ErrGCTooEarly)
	isMayFallBehind := terror.ErrorEqual(errors.Cause(geterr2), ErrFIDelServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point"))
	isBehind := isFallBehind || isMayFallBehind
	c.Assert(isBehind, IsTrue)

	// for txn seek
	txn3 := s.beginTxn(c)

	s.waitUntilErrorPlugIn(txn3.startTS)

	_, seekerr := txn3.Iter(encodeKey(s.prefix, ""), nil)
	c.Assert(seekerr, NotNil)
	isFallBehind = terror.ErrorEqual(errors.Cause(geterr2), ErrGCTooEarly)
	isMayFallBehind = terror.ErrorEqual(errors.Cause(geterr2), ErrFIDelServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point"))
	isBehind = isFallBehind || isMayFallBehind
	c.Assert(isBehind, IsTrue)

	// for snapshot batchGet
	keys := mymakeKeys(10, s.prefix)
	txn4 := s.beginTxn(c)

	s.waitUntilErrorPlugIn(txn4.startTS)

	snapshot := newEinsteinDBSnapshot(s.causetstore, ekv.Version{Ver: txn4.StartTS()}, 0)
	_, batchgeterr := snapshot.BatchGet(context.Background(), keys)
	c.Assert(batchgeterr, NotNil)
	isFallBehind = terror.ErrorEqual(errors.Cause(geterr2), ErrGCTooEarly)
	isMayFallBehind = terror.ErrorEqual(errors.Cause(geterr2), ErrFIDelServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point"))
	isBehind = isFallBehind || isMayFallBehind
	c.Assert(isBehind, IsTrue)
}
