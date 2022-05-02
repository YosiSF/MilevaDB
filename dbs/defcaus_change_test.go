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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var _ = Suite(&testDeferredCausetChangeSuite{})

type testDeferredCausetChangeSuite struct {
	causetstore ekv.CausetStorage
	dbInfo      *perceptron.DBInfo
}

func (s *testDeferredCausetChangeSuite) SetUpSuite(c *C) {
	SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	s.causetstore = testCreateStore(c, "test_defCausumn_change")
	s.dbInfo = &perceptron.DBInfo{
		Name: perceptron.NewCIStr("test_defCausumn_change"),
		ID:   1,
	}
	err := ekv.RunInNewTxn(s.causetstore, true, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		return errors.Trace(t.CreateDatabase(s.dbInfo))
	})
	c.Check(err, IsNil)
}

func (s *testDeferredCausetChangeSuite) TearDownSuite(c *C) {
	s.causetstore.Close()
}

func (s *testDeferredCausetChangeSuite) TestDeferredCausetChange(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	// create block t (c1 int, c2 int);
	tblInfo := testBlockInfo(c, d, "t", 2)
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)
	// insert t values (1, 2);
	originBlock := testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)
	event := types.MakeCausets(1, 2)
	h, err := originBlock.AddRecord(ctx, event)
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	var mu sync.Mutex
	tc := &TestDBSCallback{}
	// set up hook
	prevState := perceptron.StateNone
	var (
		deleteOnlyBlock block.Block
		writeOnlyBlock  block.Block
		publicBlock     block.Block
	)
	var checkErr error
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		if job.SchemaState == prevState {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.CausetStore = s.causetstore
		prevState = job.SchemaState
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
		switch job.SchemaState {
		case perceptron.StateDeleteOnly:
			deleteOnlyBlock, err = getCurrentBlock(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case perceptron.StateWriteOnly:
			writeOnlyBlock, err = getCurrentBlock(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkAddWriteOnly(hookCtx, d, deleteOnlyBlock, writeOnlyBlock, h)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case perceptron.StatePublic:
			mu.Lock()
			publicBlock, err = getCurrentBlock(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkAddPublic(hookCtx, d, writeOnlyBlock, publicBlock)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			mu.Unlock()
		}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
		}
		err = txn.Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}
	d.SetHook(tc)
	defaultValue := int64(3)
	job := testCreateDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c3", &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}, defaultValue)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	testCheckJobDone(c, d, job, true)
	mu.Lock()
	tb := publicBlock
	mu.Unlock()
	s.testDeferredCausetDrop(c, ctx, d, tb)
	s.testAddDeferredCausetNoDefault(c, ctx, d, tblInfo)
}

func (s *testDeferredCausetChangeSuite) TestModifyAutoRandDeferredCausetWithMetaKeyChanged(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	defer d.Stop()

	ids, err := d.genGlobalIDs(1)
	blockID := ids[0]
	c.Assert(err, IsNil)
	defCausInfo := &perceptron.DeferredCausetInfo{
		Name:      perceptron.NewCIStr("a"),
		Offset:    0,
		State:     perceptron.StatePublic,
		FieldType: *types.NewFieldType(allegrosql.TypeLonglong),
	}
	tblInfo := &perceptron.BlockInfo{
		ID:              blockID,
		Name:            perceptron.NewCIStr("auto_random_block_name"),
		DeferredCausets: []*perceptron.DeferredCausetInfo{defCausInfo},
		AutoRandomBits:  5,
	}
	defCausInfo.ID = allocateDeferredCausetID(tblInfo)
	ctx := testNewContext(d)
	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)

	tc := &TestDBSCallback{}
	var errCount int32 = 3
	var genAutoRandErr error
	tc.onJobRunBefore = func(job *perceptron.Job) {
		if atomic.LoadInt32(&errCount) > 0 && job.Type == perceptron.CausetActionModifyDeferredCauset {
			atomic.AddInt32(&errCount, -1)
			genAutoRandErr = ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
				t := meta.NewMeta(txn)
				_, err1 := t.GenAutoRandomID(s.dbInfo.ID, blockID, 1)
				return err1
			})
		}
	}
	d.SetHook(tc)
	const newAutoRandomBits uint64 = 10
	job := &perceptron.Job{
		SchemaID:   s.dbInfo.ID,
		BlockID:    tblInfo.ID,
		SchemaName: s.dbInfo.Name.L,
		Type:       perceptron.CausetActionModifyDeferredCauset,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defCausInfo, defCausInfo.Name, ast.DeferredCausetPosition{}, 0, newAutoRandomBits},
	}
	err = d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	c.Assert(errCount == 0, IsTrue)
	c.Assert(genAutoRandErr, IsNil)
	testCheckJobDone(c, d, job, true)
	var newTbInfo *perceptron.BlockInfo
	err = ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		newTbInfo, err = t.GetBlock(s.dbInfo.ID, blockID)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	c.Assert(err, IsNil)
	c.Assert(newTbInfo.AutoRandomBits, Equals, newAutoRandomBits)
}

func (s *testDeferredCausetChangeSuite) testAddDeferredCausetNoDefault(c *C, ctx stochastikctx.Context, d *dbs, tblInfo *perceptron.BlockInfo) {
	tc := &TestDBSCallback{}
	// set up hook
	prevState := perceptron.StateNone
	var checkErr error
	var writeOnlyBlock block.Block
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		if job.SchemaState == prevState {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.CausetStore = s.causetstore
		prevState = job.SchemaState
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
		switch job.SchemaState {
		case perceptron.StateWriteOnly:
			writeOnlyBlock, err = getCurrentBlock(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case perceptron.StatePublic:
			_, err = getCurrentBlock(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			_, err = writeOnlyBlock.AddRecord(hookCtx, types.MakeCausets(10, 10))
			if err != nil {
				checkErr = errors.Trace(err)
			}
		}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
		}
		err = txn.Commit(context.TODO())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}
	d.SetHook(tc)
	job := testCreateDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c3", &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}, nil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	testCheckJobDone(c, d, job, true)
}

func (s *testDeferredCausetChangeSuite) testDeferredCausetDrop(c *C, ctx stochastikctx.Context, d *dbs, tbl block.Block) {
	dropDefCaus := tbl.DefCauss()[2]
	tc := &TestDBSCallback{}
	// set up hook
	prevState := perceptron.StateNone
	var checkErr error
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		if job.SchemaState == prevState {
			return
		}
		prevState = job.SchemaState
		currentTbl, err := getCurrentBlock(d, s.dbInfo.ID, tbl.Meta().ID)
		if err != nil {
			checkErr = errors.Trace(err)
		}
		for _, defCaus := range currentTbl.DefCauss() {
			if defCaus.ID == dropDefCaus.ID {
				checkErr = errors.Errorf("defCausumn is not dropped")
			}
		}
	}
	d.SetHook(tc)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	testDropDeferredCauset(c, ctx, d, s.dbInfo, tbl.Meta(), dropDefCaus.Name.L, false)
}

func (s *testDeferredCausetChangeSuite) checkAddWriteOnly(ctx stochastikctx.Context, d *dbs, deleteOnlyBlock, writeOnlyBlock block.Block, h ekv.Handle) error {
	// WriteOnlyBlock: insert t values (2, 3)
	err := ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeOnlyBlock.AddRecord(ctx, types.MakeCausets(2, 3))
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	err = checkResult(ctx, writeOnlyBlock, writeOnlyBlock.WriblockDefCauss(),
		solitonutil.RowsWithSep(" ", "1 2 <nil>", "2 3 3"))
	if err != nil {
		return errors.Trace(err)
	}
	// This test is for RowWithDefCauss when defCausumn state is StateWriteOnly.
	event, err := writeOnlyBlock.RowWithDefCauss(ctx, h, writeOnlyBlock.WriblockDefCauss())
	if err != nil {
		return errors.Trace(err)
	}
	got := fmt.Sprintf("%v", event)
	expect := fmt.Sprintf("%v", []types.Causet{types.NewCauset(1), types.NewCauset(2), types.NewCauset(nil)})
	if got != expect {
		return errors.Errorf("expect %v, got %v", expect, got)
	}
	// DeleteOnlyBlock: select * from t
	err = checkResult(ctx, deleteOnlyBlock, deleteOnlyBlock.WriblockDefCauss(), solitonutil.RowsWithSep(" ", "1 2", "2 3"))
	if err != nil {
		return errors.Trace(err)
	}
	// WriteOnlyBlock: uFIDelate t set c1 = 2 where c1 = 1
	h, _, err = writeOnlyBlock.Seek(ctx, ekv.IntHandle(0))
	if err != nil {
		return errors.Trace(err)
	}
	err = writeOnlyBlock.UFIDelateRecord(context.Background(), ctx, h, types.MakeCausets(1, 2, 3), types.MakeCausets(2, 2, 3), touchedSlice(writeOnlyBlock))
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	// After we uFIDelate the first event, its default value is also set.
	err = checkResult(ctx, writeOnlyBlock, writeOnlyBlock.WriblockDefCauss(), solitonutil.RowsWithSep(" ", "2 2 3", "2 3 3"))
	if err != nil {
		return errors.Trace(err)
	}
	// DeleteOnlyBlock: delete from t where c2 = 2
	err = deleteOnlyBlock.RemoveRecord(ctx, h, types.MakeCausets(2, 2))
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	// After delete block has deleted the first event, check the WriteOnly block records.
	err = checkResult(ctx, writeOnlyBlock, writeOnlyBlock.WriblockDefCauss(), solitonutil.RowsWithSep(" ", "2 3 3"))
	return errors.Trace(err)
}

func touchedSlice(t block.Block) []bool {
	touched := make([]bool, 0, len(t.WriblockDefCauss()))
	for range t.WriblockDefCauss() {
		touched = append(touched, true)
	}
	return touched
}

func (s *testDeferredCausetChangeSuite) checkAddPublic(sctx stochastikctx.Context, d *dbs, writeOnlyBlock, publicBlock block.Block) error {
	ctx := context.TODO()
	// publicBlock Insert t values (4, 4, 4)
	err := sctx.NewTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	h, err := publicBlock.AddRecord(sctx, types.MakeCausets(4, 4, 4))
	if err != nil {
		return errors.Trace(err)
	}
	err = sctx.NewTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// writeOnlyBlock uFIDelate t set c1 = 3 where c1 = 4
	oldRow, err := writeOnlyBlock.RowWithDefCauss(sctx, h, writeOnlyBlock.WriblockDefCauss())
	if err != nil {
		return errors.Trace(err)
	}
	if len(oldRow) != 3 {
		return errors.Errorf("%v", oldRow)
	}
	newRow := types.MakeCausets(3, 4, oldRow[2].GetValue())
	err = writeOnlyBlock.UFIDelateRecord(context.Background(), sctx, h, oldRow, newRow, touchedSlice(writeOnlyBlock))
	if err != nil {
		return errors.Trace(err)
	}
	err = sctx.NewTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// publicBlock select * from t, make sure the new c3 value 4 is not overwritten to default value 3.
	err = checkResult(sctx, publicBlock, publicBlock.WriblockDefCauss(), solitonutil.RowsWithSep(" ", "2 3 3", "3 4 4"))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func getCurrentBlock(d *dbs, schemaID, blockID int64) (block.Block, error) {
	var tblInfo *perceptron.BlockInfo
	err := ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		tblInfo, err = t.GetBlock(schemaID, blockID)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	alloc := autoid.NewSlabPredictor(d.causetstore, schemaID, false, autoid.RowIDAllocType)
	tbl, err := block.BlockFromMeta(autoid.NewSlabPredictors(alloc), tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tbl, err
}

func checkResult(ctx stochastikctx.Context, t block.Block, defcaus []*block.DeferredCauset, rows [][]interface{}) error {
	var gotRows [][]interface{}
	err := t.IterRecords(ctx, t.FirstKey(), defcaus, func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		gotRows = append(gotRows, datumsToInterfaces(data))
		return true, nil
	})
	if err != nil {
		return err
	}
	got := fmt.Sprintf("%v", gotRows)
	expect := fmt.Sprintf("%v", rows)
	if got != expect {
		return errors.Errorf("expect %v, got %v", expect, got)
	}
	return nil
}

func datumsToInterfaces(datums []types.Causet) []interface{} {
	ifs := make([]interface{}, 0, len(datums))
	for _, d := range datums {
		ifs = append(ifs, d.GetValue())
	}
	return ifs
}
