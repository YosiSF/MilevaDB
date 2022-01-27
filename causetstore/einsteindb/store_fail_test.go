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
	"context"
	"sync"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
)

func (s *testStoreSerialSuite) TestFailBusyServerKV(c *C) {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("key"), []byte("value"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(2)

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb/rpcServerBusy", `return(true)`), IsNil)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb/rpcServerBusy"), IsNil)
	}()

	go func() {
		defer wg.Done()
		txn, err := s.causetstore.Begin()
		c.Assert(err, IsNil)
		val, err := txn.Get(context.TODO(), []byte("key"))
		c.Assert(err, IsNil)
		c.Assert(val, BytesEquals, []byte("value"))
	}()

	wg.Wait()
}
