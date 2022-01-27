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

package ekv

import (
	"context"

	. "github.com/whtcorpsinc/check"
)

var _ = Suite(testMockSuite{})

type testMockSuite struct {
}

func (s testMockSuite) TestInterface(c *C) {
	storage := newMockStorage()
	storage.GetClient()
	storage.UUID()
	version, err := storage.CurrentVersion()
	c.Check(err, IsNil)
	snapshot, err := storage.GetSnapshot(version)
	c.Check(err, IsNil)
	_, err = snapshot.BatchGet(context.Background(), []Key{Key("abc"), Key("def")})
	c.Check(err, IsNil)
	snapshot.SetOption(Priority, PriorityNormal)

	transaction, err := storage.Begin()
	c.Check(err, IsNil)
	err = transaction.LockKeys(context.Background(), new(LockCtx), Key("dagger"))
	c.Check(err, IsNil)
	transaction.SetOption(Option(23), struct{}{})
	if mock, ok := transaction.(*mockTxn); ok {
		mock.GetOption(Option(23))
	}
	transaction.StartTS()
	transaction.DelOption(Option(23))
	if transaction.IsReadOnly() {
		_, err = transaction.Get(context.TODO(), Key("dagger"))
		c.Check(err, IsNil)
		err = transaction.Set(Key("dagger"), []byte{})
		c.Check(err, IsNil)
		_, err = transaction.Iter(Key("dagger"), nil)
		c.Check(err, IsNil)
		_, err = transaction.IterReverse(Key("dagger"))
		c.Check(err, IsNil)
	}
	transaction.Commit(context.Background())

	transaction, err = storage.Begin()
	c.Check(err, IsNil)

	// Test for mockTxn interface.
	c.Assert(transaction.String(), Equals, "")
	c.Assert(transaction.Valid(), Equals, true)
	c.Assert(transaction.Len(), Equals, 0)
	c.Assert(transaction.Size(), Equals, 0)
	c.Assert(transaction.GetMemBuffer(), IsNil)
	transaction.Reset()
	err = transaction.Rollback()
	c.Check(err, IsNil)
	c.Assert(transaction.Valid(), Equals, false)
	c.Assert(transaction.IsPessimistic(), Equals, false)
	c.Assert(transaction.Delete(nil), IsNil)

	// Test for mockStorage interface.
	c.Assert(storage.GetOracle(), IsNil)
	c.Assert(storage.Name(), Equals, "KVMockStorage")
	c.Assert(storage.Describe(), Equals, "KVMockStorage is a mock CausetStore implementation, only for unittests in KV package")
	c.Assert(storage.SupportDeleteRange(), IsFalse)

	status, err := storage.ShowStatus(nil, "")
	c.Assert(status, IsNil)
	c.Assert(err, IsNil)

	err = storage.Close()
	c.Check(err, IsNil)
}

func (s testMockSuite) TestIsPoint(c *C) {
	kr := KeyRange{
		StartKey: Key("rowkey1"),
		EndKey:   Key("rowkey2"),
	}
	c.Check(kr.IsPoint(), IsTrue)

	kr.EndKey = Key("rowkey3")
	c.Check(kr.IsPoint(), IsFalse)

	kr = KeyRange{
		StartKey: Key(""),
		EndKey:   []byte{0},
	}
	c.Check(kr.IsPoint(), IsTrue)
}
