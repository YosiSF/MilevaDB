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

package dbs

import (
	"context"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	. "github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

type testCtxKeyType int

func (k testCtxKeyType) String() string {
	return "test_ctx_key"
}

const testCtxKey testCtxKeyType = 0

func (s *testDBSSuite) TestReorg(c *C) {
	causetstore := testCreateStore(c, "test_reorg")
	defer causetstore.Close()

	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()

	time.Sleep(testLease)

	ctx := testNewContext(d)

	ctx.SetValue(testCtxKey, 1)
	c.Assert(ctx.Value(testCtxKey), Equals, 1)
	ctx.ClearValue(testCtxKey)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Set([]byte("a"), []byte("b"))
	c.Assert(err, IsNil)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Set([]byte("a"), []byte("b"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	rowCount := int64(10)
	handle := s.NewHandle().Int(100).Common("a", 100, "string")
	f := func() error {
		d.generalWorker().reorgCtx.setRowCount(rowCount)
		d.generalWorker().reorgCtx.setNextHandle(handle)
		time.Sleep(1*ReorgWaitTimeout + 100*time.Millisecond)
		return nil
	}
	job := &perceptron.Job{
		ID:          1,
		SnapshotVer: 1, // Make sure it is not zero. So the reorgInfo's first is false.
	}
	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	rInfo := &reorgInfo{
		Job: job,
	}
	mockTbl := blocks.MockBlockFromMeta(&perceptron.BlockInfo{IsCommonHandle: s.IsCommonHandle})
	err = d.generalWorker().runReorgJob(m, rInfo, mockTbl.Meta(), d.lease, f)
	c.Assert(err, NotNil)

	// The longest to wait for 5 seconds to make sure the function of f is returned.
	for i := 0; i < 1000; i++ {
		time.Sleep(5 * time.Millisecond)
		err = d.generalWorker().runReorgJob(m, rInfo, mockTbl.Meta(), d.lease, f)
		if err == nil {
			c.Assert(job.RowCount, Equals, rowCount)
			c.Assert(d.generalWorker().reorgCtx.rowCount, Equals, int64(0))

			// Test whether reorgInfo's Handle is uFIDelate.
			err = txn.Commit(context.Background())
			c.Assert(err, IsNil)
			err = ctx.NewTxn(context.Background())
			c.Assert(err, IsNil)

			m = meta.NewMeta(txn)
			info, err1 := getReorgInfo(d.dbsCtx, m, job, mockTbl)
			c.Assert(err1, IsNil)
			c.Assert(info.StartHandle, HandleEquals, handle)
			_, doneHandle := d.generalWorker().reorgCtx.getRowCountAndHandle()
			c.Assert(doneHandle, IsNil)
			break
		}
	}
	c.Assert(err, IsNil)

	job = &perceptron.Job{
		ID:          2,
		SchemaID:    1,
		Type:        perceptron.CausetActionCreateSchema,
		Args:        []interface{}{perceptron.NewCIStr("test")},
		SnapshotVer: 1, // Make sure it is not zero. So the reorgInfo's first is false.
	}

	var info *reorgInfo
	startHandle := s.NewHandle().Int(1).Common(100, "string")
	endHandle := s.NewHandle().Int(0).Common(101, "string")
	err = ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		info, err1 = getReorgInfo(d.dbsCtx, t, job, mockTbl)
		c.Assert(err1, IsNil)
		err1 = info.UFIDelateReorgMeta(txn, startHandle, endHandle, 1)
		c.Assert(err1, IsNil)
		return nil
	})
	c.Assert(err, IsNil)

	err = ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		info, err1 = getReorgInfo(d.dbsCtx, t, job, mockTbl)
		c.Assert(err1, IsNil)
		c.Assert(info.StartHandle, HandleEquals, startHandle)
		c.Assert(info.EndHandle, HandleEquals, endHandle)
		return nil
	})
	c.Assert(err, IsNil)

	d.Stop()
	err = d.generalWorker().runReorgJob(m, rInfo, mockTbl.Meta(), d.lease, func() error {
		time.Sleep(4 * testLease)
		return nil
	})
	c.Assert(err, NotNil)
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	s.RerunWithCommonHandleEnabled(c, s.TestReorg)
}

func (s *testDBSSuite) TestReorgOwner(c *C) {
	causetstore := testCreateStore(c, "test_reorg_owner")
	defer causetstore.Close()

	d1 := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d1.Stop()

	ctx := testNewContext(d1)

	testCheckOwner(c, d1, true)

	d2 := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d2.Stop()

	dbInfo := testSchemaInfo(c, d1, "test")
	testCreateSchema(c, ctx, d1, dbInfo)

	tblInfo := testBlockInfo(c, d1, "t", 3)
	testCreateBlock(c, ctx, d1, dbInfo, tblInfo)
	t := testGetBlock(c, d1, dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err := t.AddRecord(ctx, types.MakeCausets(i, i, i))
		c.Assert(err, IsNil)
	}

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	tc := &TestDBSCallback{}
	tc.onJobRunBefore = func(job *perceptron.Job) {
		if job.SchemaState == perceptron.StateDeleteReorganization {
			d1.Stop()
		}
	}

	d1.SetHook(tc)

	testDropSchema(c, ctx, d1, dbInfo)

	err = ekv.RunInNewTxn(d1.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		EDB, err1 := t.GetDatabase(dbInfo.ID)
		c.Assert(err1, IsNil)
		c.Assert(EDB, IsNil)
		return nil
	})
	c.Assert(err, IsNil)
}
