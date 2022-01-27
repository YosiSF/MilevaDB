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

package dbs

import (
	"context"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testIndexChangeSuite{})

type testIndexChangeSuite struct {
	causetstore ekv.CausetStorage
	dbInfo      *perceptron.DBInfo
}

func (s *testIndexChangeSuite) SetUpSuite(c *C) {
	s.causetstore = testCreateStore(c, "test_index_change")
	s.dbInfo = &perceptron.DBInfo{
		Name: perceptron.NewCIStr("test_index_change"),
		ID:   1,
	}
	err := ekv.RunInNewTxn(s.causetstore, true, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		return errors.Trace(t.CreateDatabase(s.dbInfo))
	})
	c.Check(err, IsNil, Commentf("err %v", errors.ErrorStack(err)))
}

func (s *testIndexChangeSuite) TearDownSuite(c *C) {
	s.causetstore.Close()
}

func (s *testIndexChangeSuite) TestIndexChange(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	// create block t (c1 int primary key, c2 int);
	tblInfo := testBlockInfo(c, d, "t", 2)
	tblInfo.DeferredCausets[0].Flag = allegrosql.PriKeyFlag | allegrosql.NotNullFlag
	tblInfo.PKIsHandle = true
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)
	originBlock := testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)

	// insert t values (1, 1), (2, 2), (3, 3)
	_, err = originBlock.AddRecord(ctx, types.MakeCausets(1, 1))
	c.Assert(err, IsNil)
	_, err = originBlock.AddRecord(ctx, types.MakeCausets(2, 2))
	c.Assert(err, IsNil)
	_, err = originBlock.AddRecord(ctx, types.MakeCausets(3, 3))
	c.Assert(err, IsNil)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	tc := &TestDBSCallback{}
	// set up hook
	prevState := perceptron.StateNone
	addIndexDone := false
	var (
		deleteOnlyBlock block.Block
		writeOnlyBlock  block.Block
		publicBlock     block.Block
		checkErr        error
	)
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		if job.SchemaState == prevState {
			return
		}
		ctx1 := testNewContext(d)
		prevState = job.SchemaState
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
			err = s.checkAddWriteOnly(d, ctx1, deleteOnlyBlock, writeOnlyBlock)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case perceptron.StatePublic:
			if job.GetRowCount() != 3 {
				checkErr = errors.Errorf("job's event count %d != 3", job.GetRowCount())
			}
			publicBlock, err = getCurrentBlock(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkAddPublic(d, ctx1, writeOnlyBlock, publicBlock)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			if job.State == perceptron.JobStateSynced {
				addIndexDone = true
			}
		}
	}
	d.SetHook(tc)
	testCreateIndex(c, ctx, d, s.dbInfo, originBlock.Meta(), false, "c2", "c2")
	// We need to make sure onJobUFIDelated is called in the first hook.
	// After testCreateIndex(), onJobUFIDelated() may not be called when job.state is Sync.
	// If we skip this check, prevState may wrongly set to StatePublic.
	for i := 0; i <= 10; i++ {
		if addIndexDone {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	prevState = perceptron.StateNone
	var noneBlock block.Block
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		if job.SchemaState == prevState {
			return
		}
		prevState = job.SchemaState
		var err error
		ctx1 := testNewContext(d)
		switch job.SchemaState {
		case perceptron.StateWriteOnly:
			writeOnlyBlock, err = getCurrentBlock(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkDropWriteOnly(d, ctx1, publicBlock, writeOnlyBlock)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case perceptron.StateDeleteOnly:
			deleteOnlyBlock, err = getCurrentBlock(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkDroFIDeleleteOnly(d, ctx1, writeOnlyBlock, deleteOnlyBlock)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case perceptron.StateNone:
			noneBlock, err = getCurrentBlock(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			if len(noneBlock.Indices()) != 0 {
				checkErr = errors.New("index should have been dropped")
			}
		}
	}
	testDropIndex(c, ctx, d, s.dbInfo, publicBlock.Meta(), "c2")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
}

func checHoTTexExists(ctx stochastikctx.Context, tbl block.Block, indexValue interface{}, handle int64, exists bool) error {
	idx := tbl.Indices()[0]
	txn, err := ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	doesExist, _, err := idx.Exist(ctx.GetStochastikVars().StmtCtx, txn.GetUnionStore(), types.MakeCausets(indexValue), ekv.IntHandle(handle))
	if err != nil {
		return errors.Trace(err)
	}
	if exists != doesExist {
		if exists {
			return errors.New("index should exists")
		}
		return errors.New("index should not exists")
	}
	return nil
}

func (s *testIndexChangeSuite) checkAddWriteOnly(d *dbs, ctx stochastikctx.Context, delOnlyTbl, writeOnlyTbl block.Block) error {
	// DeleteOnlyBlock: insert t values (4, 4);
	err := ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = delOnlyTbl.AddRecord(ctx, types.MakeCausets(4, 4))
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, writeOnlyTbl, 4, 4, false)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyBlock: insert t values (5, 5);
	_, err = writeOnlyTbl.AddRecord(ctx, types.MakeCausets(5, 5))
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, writeOnlyTbl, 5, 5, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyBlock: uFIDelate t set c2 = 1 where c1 = 4 and c2 = 4
	err = writeOnlyTbl.UFIDelateRecord(context.Background(), ctx, ekv.IntHandle(4), types.MakeCausets(4, 4), types.MakeCausets(4, 1), touchedSlice(writeOnlyTbl))
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, writeOnlyTbl, 1, 4, true)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyBlock: uFIDelate t set c2 = 3 where c1 = 4 and c2 = 1
	err = delOnlyTbl.UFIDelateRecord(context.Background(), ctx, ekv.IntHandle(4), types.MakeCausets(4, 1), types.MakeCausets(4, 3), touchedSlice(writeOnlyTbl))
	if err != nil {
		return errors.Trace(err)
	}
	// old value index not exists.
	err = checHoTTexExists(ctx, writeOnlyTbl, 1, 4, false)
	if err != nil {
		return errors.Trace(err)
	}
	// new value index not exists.
	err = checHoTTexExists(ctx, writeOnlyTbl, 3, 4, false)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyBlock: delete t where c1 = 4 and c2 = 3
	err = writeOnlyTbl.RemoveRecord(ctx, ekv.IntHandle(4), types.MakeCausets(4, 3))
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, writeOnlyTbl, 3, 4, false)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyBlock: delete t where c1 = 5
	err = delOnlyTbl.RemoveRecord(ctx, ekv.IntHandle(5), types.MakeCausets(5, 5))
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, writeOnlyTbl, 5, 5, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testIndexChangeSuite) checkAddPublic(d *dbs, ctx stochastikctx.Context, writeTbl, publicTbl block.Block) error {
	// WriteOnlyBlock: insert t values (6, 6)
	err := ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeTbl.AddRecord(ctx, types.MakeCausets(6, 6))
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, publicTbl, 6, 6, true)
	if err != nil {
		return errors.Trace(err)
	}
	// PublicBlock: insert t values (7, 7)
	_, err = publicTbl.AddRecord(ctx, types.MakeCausets(7, 7))
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, publicTbl, 7, 7, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyBlock: uFIDelate t set c2 = 5 where c1 = 7 and c2 = 7
	err = writeTbl.UFIDelateRecord(context.Background(), ctx, ekv.IntHandle(7), types.MakeCausets(7, 7), types.MakeCausets(7, 5), touchedSlice(writeTbl))
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, publicTbl, 5, 7, true)
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, publicTbl, 7, 7, false)
	if err != nil {
		return errors.Trace(err)
	}
	// WriteOnlyBlock: delete t where c1 = 6
	err = writeTbl.RemoveRecord(ctx, ekv.IntHandle(6), types.MakeCausets(6, 6))
	if err != nil {
		return errors.Trace(err)
	}
	err = checHoTTexExists(ctx, publicTbl, 6, 6, false)
	if err != nil {
		return errors.Trace(err)
	}

	var rows [][]types.Causet
	publicTbl.IterRecords(ctx, publicTbl.FirstKey(), publicTbl.DefCauss(),
		func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
			rows = append(rows, data)
			return true, nil
		})
	if len(rows) == 0 {
		return errors.New("block is empty")
	}
	for _, event := range rows {
		idxVal := event[1].GetInt64()
		handle := event[0].GetInt64()
		err = checHoTTexExists(ctx, publicTbl, idxVal, handle, true)
		if err != nil {
			return errors.Trace(err)
		}
	}
	txn, err := ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	return txn.Commit(context.Background())
}

func (s *testIndexChangeSuite) checkDropWriteOnly(d *dbs, ctx stochastikctx.Context, publicTbl, writeTbl block.Block) error {
	// WriteOnlyBlock insert t values (8, 8)
	err := ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeTbl.AddRecord(ctx, types.MakeCausets(8, 8))
	if err != nil {
		return errors.Trace(err)
	}

	err = checHoTTexExists(ctx, publicTbl, 8, 8, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyBlock uFIDelate t set c2 = 7 where c1 = 8 and c2 = 8
	err = writeTbl.UFIDelateRecord(context.Background(), ctx, ekv.IntHandle(8), types.MakeCausets(8, 8), types.MakeCausets(8, 7), touchedSlice(writeTbl))
	if err != nil {
		return errors.Trace(err)
	}

	err = checHoTTexExists(ctx, publicTbl, 7, 8, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyBlock delete t where c1 = 8
	err = writeTbl.RemoveRecord(ctx, ekv.IntHandle(8), types.MakeCausets(8, 7))
	if err != nil {
		return errors.Trace(err)
	}

	err = checHoTTexExists(ctx, publicTbl, 7, 8, false)
	if err != nil {
		return errors.Trace(err)
	}
	txn, err := ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	return txn.Commit(context.Background())
}

func (s *testIndexChangeSuite) checkDroFIDeleleteOnly(d *dbs, ctx stochastikctx.Context, writeTbl, delTbl block.Block) error {
	// WriteOnlyBlock insert t values (9, 9)
	err := ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeTbl.AddRecord(ctx, types.MakeCausets(9, 9))
	if err != nil {
		return errors.Trace(err)
	}

	err = checHoTTexExists(ctx, writeTbl, 9, 9, true)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyBlock insert t values (10, 10)
	_, err = delTbl.AddRecord(ctx, types.MakeCausets(10, 10))
	if err != nil {
		return errors.Trace(err)
	}

	err = checHoTTexExists(ctx, writeTbl, 10, 10, false)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyBlock uFIDelate t set c2 = 10 where c1 = 9
	err = delTbl.UFIDelateRecord(context.Background(), ctx, ekv.IntHandle(9), types.MakeCausets(9, 9), types.MakeCausets(9, 10), touchedSlice(delTbl))
	if err != nil {
		return errors.Trace(err)
	}

	err = checHoTTexExists(ctx, writeTbl, 9, 9, false)
	if err != nil {
		return errors.Trace(err)
	}

	err = checHoTTexExists(ctx, writeTbl, 10, 9, false)
	if err != nil {
		return errors.Trace(err)
	}
	txn, err := ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	return txn.Commit(context.Background())
}
