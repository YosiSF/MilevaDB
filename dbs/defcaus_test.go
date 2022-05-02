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
	"reflect"
	"sync"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var _ = Suite(&testDeferredCausetSuite{})

type testDeferredCausetSuite struct {
	causetstore ekv.CausetStorage
	dbInfo      *perceptron.DBInfo
}

func (s *testDeferredCausetSuite) SetUpSuite(c *C) {
	s.causetstore = testCreateStore(c, "test_defCausumn")
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)

	s.dbInfo = testSchemaInfo(c, d, "test_defCausumn")
	testCreateSchema(c, testNewContext(d), d, s.dbInfo)
	d.Stop()
}

func (s *testDeferredCausetSuite) TearDownSuite(c *C) {
	err := s.causetstore.Close()
	c.Assert(err, IsNil)
}

func buildCreateDeferredCausetJob(dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, defCausName string,
	pos *ast.DeferredCausetPosition, defaultValue interface{}) *perceptron.Job {
	defCaus := &perceptron.DeferredCausetInfo{
		Name:               perceptron.NewCIStr(defCausName),
		Offset:             len(tblInfo.DeferredCausets),
		DefaultValue:       defaultValue,
		OriginDefaultValue: defaultValue,
	}
	defCaus.ID = allocateDeferredCausetID(tblInfo)
	defCaus.FieldType = *types.NewFieldType(allegrosql.TypeLong)

	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionAddDeferredCauset,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defCaus, pos, 0},
	}
	return job
}

func testCreateDeferredCauset(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo,
	defCausName string, pos *ast.DeferredCausetPosition, defaultValue interface{}) *perceptron.Job {
	job := buildCreateDeferredCausetJob(dbInfo, tblInfo, defCausName, pos, defaultValue)
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildCreateDeferredCausetsJob(dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, defCausNames []string,
	positions []*ast.DeferredCausetPosition, defaultValue interface{}) *perceptron.Job {
	defCausInfos := make([]*perceptron.DeferredCausetInfo, len(defCausNames))
	offsets := make([]int, len(defCausNames))
	ifNotExists := make([]bool, len(defCausNames))
	for i, defCausName := range defCausNames {
		defCaus := &perceptron.DeferredCausetInfo{
			Name:               perceptron.NewCIStr(defCausName),
			Offset:             len(tblInfo.DeferredCausets),
			DefaultValue:       defaultValue,
			OriginDefaultValue: defaultValue,
		}
		defCaus.ID = allocateDeferredCausetID(tblInfo)
		defCaus.FieldType = *types.NewFieldType(allegrosql.TypeLong)
		defCausInfos[i] = defCaus
	}

	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionAddDeferredCausets,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defCausInfos, positions, offsets, ifNotExists},
	}
	return job
}

func testCreateDeferredCausets(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo,
	defCausNames []string, positions []*ast.DeferredCausetPosition, defaultValue interface{}) *perceptron.Job {
	job := buildCreateDeferredCausetsJob(dbInfo, tblInfo, defCausNames, positions, defaultValue)
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildDropDeferredCausetJob(dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, defCausName string) *perceptron.Job {
	return &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionDropDeferredCauset,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{perceptron.NewCIStr(defCausName)},
	}
}

func testDropDeferredCauset(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, defCausName string, isError bool) *perceptron.Job {
	job := buildDropDeferredCausetJob(dbInfo, tblInfo, defCausName)
	err := d.doDBSJob(ctx, job)
	if isError {
		c.Assert(err, NotNil)
		return nil
	}
	c.Assert(errors.ErrorStack(err), Equals, "")
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildDropDeferredCausetsJob(dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, defCausNames []string) *perceptron.Job {
	defCausumnNames := make([]perceptron.CIStr, len(defCausNames))
	ifExists := make([]bool, len(defCausNames))
	for i, defCausName := range defCausNames {
		defCausumnNames[i] = perceptron.NewCIStr(defCausName)
	}
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionDropDeferredCausets,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defCausumnNames, ifExists},
	}
	return job
}

func testDropDeferredCausets(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, defCausNames []string, isError bool) *perceptron.Job {
	job := buildDropDeferredCausetsJob(dbInfo, tblInfo, defCausNames)
	err := d.doDBSJob(ctx, job)
	if isError {
		c.Assert(err, NotNil)
		return nil
	}
	c.Assert(errors.ErrorStack(err), Equals, "")
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func (s *testDeferredCausetSuite) TestDeferredCauset(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	defer d.Stop()

	tblInfo := testBlockInfo(c, d, "t1", 3)
	ctx := testNewContext(d)

	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err := t.AddRecord(ctx, types.MakeCausets(i, 10*i, 100*i))
		c.Assert(err, IsNil)
	}

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		c.Assert(data, HasLen, 3)
		c.Assert(data[0].GetInt64(), Equals, i)
		c.Assert(data[1].GetInt64(), Equals, 10*i)
		c.Assert(data[2].GetInt64(), Equals, 100*i)
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(num))

	c.Assert(block.FindDefCaus(t.DefCauss(), "c4"), IsNil)

	job := testCreateDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c4", &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionAfter, RelativeDeferredCauset: &ast.DeferredCausetName{Name: perceptron.NewCIStr("c3")}}, 100)
	testCheckJobDone(c, d, job, true)

	t = testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)
	c.Assert(block.FindDefCaus(t.DefCauss(), "c4"), NotNil)

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(),
		func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
			c.Assert(data, HasLen, 4)
			c.Assert(data[0].GetInt64(), Equals, i)
			c.Assert(data[1].GetInt64(), Equals, 10*i)
			c.Assert(data[2].GetInt64(), Equals, 100*i)
			c.Assert(data[3].GetInt64(), Equals, int64(100))
			i++
			return true, nil
		})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(num))

	h, err := t.AddRecord(ctx, types.MakeCausets(11, 12, 13, 14))
	c.Assert(err, IsNil)
	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	values, err := t.EventWithDefCauss(ctx, h, t.DefCauss())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 4)
	c.Assert(values[3].GetInt64(), Equals, int64(14))

	job = testDropDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c4", false)
	testCheckJobDone(c, d, job, false)

	t = testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.EventWithDefCauss(ctx, h, t.DefCauss())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 3)
	c.Assert(values[2].GetInt64(), Equals, int64(13))

	job = testCreateDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c4", &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}, 111)
	testCheckJobDone(c, d, job, true)

	t = testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.EventWithDefCauss(ctx, h, t.DefCauss())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 4)
	c.Assert(values[3].GetInt64(), Equals, int64(111))

	job = testCreateDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c5", &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}, 101)
	testCheckJobDone(c, d, job, true)

	t = testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.EventWithDefCauss(ctx, h, t.DefCauss())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 5)
	c.Assert(values[4].GetInt64(), Equals, int64(101))

	job = testCreateDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c6", &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionFirst}, 202)
	testCheckJobDone(c, d, job, true)

	t = testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)
	defcaus := t.DefCauss()
	c.Assert(defcaus, HasLen, 6)
	c.Assert(defcaus[0].Offset, Equals, 0)
	c.Assert(defcaus[0].Name.L, Equals, "c6")
	c.Assert(defcaus[1].Offset, Equals, 1)
	c.Assert(defcaus[1].Name.L, Equals, "c1")
	c.Assert(defcaus[2].Offset, Equals, 2)
	c.Assert(defcaus[2].Name.L, Equals, "c2")
	c.Assert(defcaus[3].Offset, Equals, 3)
	c.Assert(defcaus[3].Name.L, Equals, "c3")
	c.Assert(defcaus[4].Offset, Equals, 4)
	c.Assert(defcaus[4].Name.L, Equals, "c4")
	c.Assert(defcaus[5].Offset, Equals, 5)
	c.Assert(defcaus[5].Name.L, Equals, "c5")

	values, err = t.EventWithDefCauss(ctx, h, defcaus)
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 6)
	c.Assert(values[0].GetInt64(), Equals, int64(202))
	c.Assert(values[5].GetInt64(), Equals, int64(101))

	job = testDropDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c2", false)
	testCheckJobDone(c, d, job, false)

	t = testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)

	values, err = t.EventWithDefCauss(ctx, h, t.DefCauss())
	c.Assert(err, IsNil)
	c.Assert(values, HasLen, 5)
	c.Assert(values[0].GetInt64(), Equals, int64(202))
	c.Assert(values[4].GetInt64(), Equals, int64(101))

	job = testDropDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c1", false)
	testCheckJobDone(c, d, job, false)

	job = testDropDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c3", false)
	testCheckJobDone(c, d, job, false)

	job = testDropDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c4", false)
	testCheckJobDone(c, d, job, false)

	job = testCreateIndex(c, ctx, d, s.dbInfo, tblInfo, false, "c5_idx", "c5")
	testCheckJobDone(c, d, job, true)

	job = testDropDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c5", false)
	testCheckJobDone(c, d, job, false)

	testDropDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c6", true)

	testDropBlock(c, ctx, d, s.dbInfo, tblInfo)
}

func (s *testDeferredCausetSuite) checkDeferredCausetKVExist(ctx stochastikctx.Context, t block.Block, handle ekv.Handle, defCaus *block.DeferredCauset, defCausumnValue interface{}, isExist bool) error {
	err := ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if txn, err1 := ctx.Txn(true); err1 == nil {
			txn.Commit(context.Background())
		}
	}()
	key := t.RecordKey(handle)
	txn, err := ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	data, err := txn.Get(context.TODO(), key)
	if !isExist {
		if terror.ErrorEqual(err, ekv.ErrNotExist) {
			return nil
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	defCausMap := make(map[int64]*types.FieldType)
	defCausMap[defCaus.ID] = &defCaus.FieldType
	rowMap, err := blockcodec.DecodeEventToCausetMap(data, defCausMap, ctx.GetStochaseinstein_dbars().Location())
	if err != nil {
		return errors.Trace(err)
	}
	val, ok := rowMap[defCaus.ID]
	if isExist {
		if !ok || val.GetValue() != defCausumnValue {
			return errors.Errorf("%v is not equal to %v", val.GetValue(), defCausumnValue)
		}
	} else {
		if ok {
			return errors.Errorf("defCausumn value should not exists")
		}
	}
	return nil
}

func (s *testDeferredCausetSuite) checkNoneDeferredCauset(ctx stochastikctx.Context, d *dbs, tblInfo *perceptron.BlockInfo, handle ekv.Handle, defCaus *block.DeferredCauset, defCausumnValue interface{}) error {
	t, err := testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.checkDeferredCausetKVExist(ctx, t, handle, defCaus, defCausumnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetDeferredCauset(t, defCaus.Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testDeferredCausetSuite) checkDeleteOnlyDeferredCauset(ctx stochastikctx.Context, d *dbs, tblInfo *perceptron.BlockInfo, handle ekv.Handle, defCaus *block.DeferredCauset, event []types.Causet, defCausumnValue interface{}) error {
	t, err := testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if !reflect.DeepEqual(data, event) {
			return false, errors.Errorf("%v not equal to %v", data, event)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}
	err = s.checkDeferredCausetKVExist(ctx, t, handle, defCaus, defCausumnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	// Test add a new event.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	newEvent := types.MakeCausets(int64(11), int64(22), int64(33))
	newHandle, err := t.AddRecord(ctx, newEvent)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	rows := [][]types.Causet{event, newEvent}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if !reflect.DeepEqual(data, rows[i]) {
			return false, errors.Errorf("%v not equal to %v", data, rows[i])
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 2 {
		return errors.Errorf("expect 2, got %v", i)
	}

	err = s.checkDeferredCausetKVExist(ctx, t, handle, defCaus, defCausumnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	// Test remove a event.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	err = t.RemoveRecord(ctx, newHandle, newEvent)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}
	err = s.checkDeferredCausetKVExist(ctx, t, newHandle, defCaus, defCausumnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetDeferredCauset(t, defCaus.Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testDeferredCausetSuite) checkWriteOnlyDeferredCauset(ctx stochastikctx.Context, d *dbs, tblInfo *perceptron.BlockInfo, handle ekv.Handle, defCaus *block.DeferredCauset, event []types.Causet, defCausumnValue interface{}) error {
	t, err := testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if !reflect.DeepEqual(data, event) {
			return false, errors.Errorf("%v not equal to %v", data, event)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}

	err = s.checkDeferredCausetKVExist(ctx, t, handle, defCaus, defCausumnValue, false)
	if err != nil {
		return errors.Trace(err)
	}

	// Test add a new event.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	newEvent := types.MakeCausets(int64(11), int64(22), int64(33))
	newHandle, err := t.AddRecord(ctx, newEvent)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	rows := [][]types.Causet{event, newEvent}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if !reflect.DeepEqual(data, rows[i]) {
			return false, errors.Errorf("%v not equal to %v", data, rows[i])
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 2 {
		return errors.Errorf("expect 2, got %v", i)
	}

	err = s.checkDeferredCausetKVExist(ctx, t, newHandle, defCaus, defCausumnValue, true)
	if err != nil {
		return errors.Trace(err)
	}
	// Test remove a event.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	err = t.RemoveRecord(ctx, newHandle, newEvent)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}

	err = s.checkDeferredCausetKVExist(ctx, t, newHandle, defCaus, defCausumnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetDeferredCauset(t, defCaus.Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testDeferredCausetSuite) checkReorganizationDeferredCauset(ctx stochastikctx.Context, d *dbs, tblInfo *perceptron.BlockInfo, defCaus *block.DeferredCauset, event []types.Causet, defCausumnValue interface{}) error {
	t, err := testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if !reflect.DeepEqual(data, event) {
			return false, errors.Errorf("%v not equal to %v", data, event)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1 got %v", i)
	}

	// Test add a new event.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	newEvent := types.MakeCausets(int64(11), int64(22), int64(33))
	newHandle, err := t.AddRecord(ctx, newEvent)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	rows := [][]types.Causet{event, newEvent}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if !reflect.DeepEqual(data, rows[i]) {
			return false, errors.Errorf("%v not equal to %v", data, rows[i])
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 2 {
		return errors.Errorf("expect 2, got %v", i)
	}

	err = s.checkDeferredCausetKVExist(ctx, t, newHandle, defCaus, defCausumnValue, true)
	if err != nil {
		return errors.Trace(err)
	}

	// Test remove a event.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	err = t.RemoveRecord(ctx, newHandle, newEvent)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}
	err = s.testGetDeferredCauset(t, defCaus.Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testDeferredCausetSuite) checkPublicDeferredCauset(ctx stochastikctx.Context, d *dbs, tblInfo *perceptron.BlockInfo, newDefCaus *block.DeferredCauset, oldEvent []types.Causet, defCausumnValue interface{}) error {
	t, err := testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i := int64(0)
	uFIDelatedEvent := append(oldEvent, types.NewCauset(defCausumnValue))
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if !reflect.DeepEqual(data, uFIDelatedEvent) {
			return false, errors.Errorf("%v not equal to %v", data, uFIDelatedEvent)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}

	// Test add a new event.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	newEvent := types.MakeCausets(int64(11), int64(22), int64(33), int64(44))
	handle, err := t.AddRecord(ctx, newEvent)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	rows := [][]types.Causet{uFIDelatedEvent, newEvent}

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if !reflect.DeepEqual(data, rows[i]) {
			return false, errors.Errorf("%v not equal to %v", data, rows[i])
		}
		i++
		return true, nil
	})
	if i != 2 {
		return errors.Errorf("expect 2, got %v", i)
	}

	// Test remove a event.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	err = t.RemoveRecord(ctx, handle, newEvent)
	if err != nil {
		return errors.Trace(err)
	}

	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if !reflect.DeepEqual(data, uFIDelatedEvent) {
			return false, errors.Errorf("%v not equal to %v", data, uFIDelatedEvent)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}

	err = s.testGetDeferredCauset(t, newDefCaus.Name.L, true)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testDeferredCausetSuite) checkAddDeferredCauset(state perceptron.SchemaState, d *dbs, tblInfo *perceptron.BlockInfo, handle ekv.Handle, newDefCaus *block.DeferredCauset, oldEvent []types.Causet, defCausumnValue interface{}) error {
	ctx := testNewContext(d)
	var err error
	switch state {
	case perceptron.StateNone:
		err = errors.Trace(s.checkNoneDeferredCauset(ctx, d, tblInfo, handle, newDefCaus, defCausumnValue))
	case perceptron.StateDeleteOnly:
		err = errors.Trace(s.checkDeleteOnlyDeferredCauset(ctx, d, tblInfo, handle, newDefCaus, oldEvent, defCausumnValue))
	case perceptron.StateWriteOnly:
		err = errors.Trace(s.checkWriteOnlyDeferredCauset(ctx, d, tblInfo, handle, newDefCaus, oldEvent, defCausumnValue))
	case perceptron.StateWriteReorganization, perceptron.StateDeleteReorganization:
		err = errors.Trace(s.checkReorganizationDeferredCauset(ctx, d, tblInfo, newDefCaus, oldEvent, defCausumnValue))
	case perceptron.StatePublic:
		err = errors.Trace(s.checkPublicDeferredCauset(ctx, d, tblInfo, newDefCaus, oldEvent, defCausumnValue))
	}
	return err
}

func (s *testDeferredCausetSuite) testGetDeferredCauset(t block.Block, name string, isExist bool) error {
	defCaus := block.FindDefCaus(t.DefCauss(), name)
	if isExist {
		if defCaus == nil {
			return errors.Errorf("defCausumn should not be nil")
		}
	} else {
		if defCaus != nil {
			return errors.Errorf("defCausumn should be nil")
		}
	}
	return nil
}

func (s *testDeferredCausetSuite) TestAddDeferredCauset(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	tblInfo := testBlockInfo(c, d, "t", 3)
	ctx := testNewContext(d)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)

	oldEvent := types.MakeCausets(int64(1), int64(2), int64(3))
	handle, err := t.AddRecord(ctx, oldEvent)
	c.Assert(err, IsNil)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	newDefCausName := "c4"
	defaultDefCausValue := int64(4)

	var mu sync.Mutex
	var hookErr error
	checkOK := false

	tc := &TestDBSCallback{}
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		mu.Lock()
		defer mu.Unlock()
		if checkOK {
			return
		}

		t, err1 := testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}
		newDefCaus := block.FindDefCaus(t.(*blocks.BlockCommon).DeferredCausets, newDefCausName)
		if newDefCaus == nil {
			return
		}

		err1 = s.checkAddDeferredCauset(newDefCaus.State, d, tblInfo, handle, newDefCaus, oldEvent, defaultDefCausValue)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}

		if newDefCaus.State == perceptron.StatePublic {
			checkOK = true
		}
	}

	d.SetHook(tc)

	job := testCreateDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, newDefCausName, &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}, defaultDefCausValue)

	testCheckJobDone(c, d, job, true)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(errors.ErrorStack(hErr), Equals, "")
	c.Assert(ok, IsTrue)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	job = testDropBlock(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	d.Stop()
}

func (s *testDeferredCausetSuite) TestAddDeferredCausets(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	tblInfo := testBlockInfo(c, d, "t", 3)
	ctx := testNewContext(d)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)

	oldEvent := types.MakeCausets(int64(1), int64(2), int64(3))
	handle, err := t.AddRecord(ctx, oldEvent)
	c.Assert(err, IsNil)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	newDefCausNames := []string{"c4,c5,c6"}
	positions := make([]*ast.DeferredCausetPosition, 3)
	for i := range positions {
		positions[i] = &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}
	}
	defaultDefCausValue := int64(4)

	var mu sync.Mutex
	var hookErr error
	checkOK := false

	tc := &TestDBSCallback{}
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		mu.Lock()
		defer mu.Unlock()
		if checkOK {
			return
		}

		t, err1 := testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}
		for _, newDefCausName := range newDefCausNames {
			newDefCaus := block.FindDefCaus(t.(*blocks.BlockCommon).DeferredCausets, newDefCausName)
			if newDefCaus == nil {
				return
			}

			err1 = s.checkAddDeferredCauset(newDefCaus.State, d, tblInfo, handle, newDefCaus, oldEvent, defaultDefCausValue)
			if err1 != nil {
				hookErr = errors.Trace(err1)
				return
			}

			if newDefCaus.State == perceptron.StatePublic {
				checkOK = true
			}
		}
	}

	d.SetHook(tc)

	job := testCreateDeferredCausets(c, ctx, d, s.dbInfo, tblInfo, newDefCausNames, positions, defaultDefCausValue)

	testCheckJobDone(c, d, job, true)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(errors.ErrorStack(hErr), Equals, "")
	c.Assert(ok, IsTrue)

	job = testDropBlock(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)
	d.Stop()
}

func (s *testDeferredCausetSuite) TestDropDeferredCauset(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	tblInfo := testBlockInfo(c, d, "t2", 4)
	ctx := testNewContext(d)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)

	defCausName := "c4"
	defaultDefCausValue := int64(4)
	event := types.MakeCausets(int64(1), int64(2), int64(3))
	_, err = t.AddRecord(ctx, append(event, types.NewCauset(defaultDefCausValue)))
	c.Assert(err, IsNil)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	checkOK := false
	var hookErr error
	var mu sync.Mutex

	tc := &TestDBSCallback{}
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		mu.Lock()
		defer mu.Unlock()
		if checkOK {
			return
		}
		t, err1 := testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}
		defCaus := block.FindDefCaus(t.(*blocks.BlockCommon).DeferredCausets, defCausName)
		if defCaus == nil {
			checkOK = true
			return
		}
	}

	d.SetHook(tc)

	job := testDropDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, defCausName, false)
	testCheckJobDone(c, d, job, false)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(hErr, IsNil)
	c.Assert(ok, IsTrue)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	job = testDropBlock(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	d.Stop()
}

func (s *testDeferredCausetSuite) TestDropDeferredCausets(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	tblInfo := testBlockInfo(c, d, "t2", 4)
	ctx := testNewContext(d)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)

	defCausNames := []string{"c3", "c4"}
	defaultDefCausValue := int64(4)
	event := types.MakeCausets(int64(1), int64(2), int64(3))
	_, err = t.AddRecord(ctx, append(event, types.NewCauset(defaultDefCausValue)))
	c.Assert(err, IsNil)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	checkOK := false
	var hookErr error
	var mu sync.Mutex

	tc := &TestDBSCallback{}
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		mu.Lock()
		defer mu.Unlock()
		if checkOK {
			return
		}
		t, err1 := testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}
		for _, defCausName := range defCausNames {
			defCaus := block.FindDefCaus(t.(*blocks.BlockCommon).DeferredCausets, defCausName)
			if defCaus == nil {
				checkOK = true
				return
			}
		}
	}

	d.SetHook(tc)

	job := testDropDeferredCausets(c, ctx, d, s.dbInfo, tblInfo, defCausNames, false)
	testCheckJobDone(c, d, job, false)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(hErr, IsNil)
	c.Assert(ok, IsTrue)

	job = testDropBlock(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)
	d.Stop()
}

func (s *testDeferredCausetSuite) TestModifyDeferredCauset(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	ctx := testNewContext(d)
	defer d.Stop()
	tests := []struct {
		origin string
		to     string
		err    error
	}{
		{"int", "bigint", nil},
		{"int", "int unsigned", errUnsupportedModifyDeferredCauset.GenWithStackByArgs("length 10 is less than origin 11, and milevadb_enable_change_defCausumn_type is false")},
		{"varchar(10)", "text", nil},
		{"varbinary(10)", "blob", nil},
		{"text", "blob", errUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8mb4 to binary")},
		{"varchar(10)", "varchar(8)", errUnsupportedModifyDeferredCauset.GenWithStackByArgs("length 8 is less than origin 10")},
		{"varchar(10)", "varchar(11)", nil},
		{"varchar(10) character set utf8 defCauslate utf8_bin", "varchar(10) character set utf8", nil},
		{"decimal(2,1)", "decimal(3,2)", errUnsupportedModifyDeferredCauset.GenWithStackByArgs("can't change decimal defCausumn precision")},
		{"decimal(2,1)", "decimal(2,2)", errUnsupportedModifyDeferredCauset.GenWithStackByArgs("can't change decimal defCausumn precision")},
		{"decimal(2,1)", "decimal(2,1)", nil},
		{"decimal(2,1)", "int", errUnsupportedModifyDeferredCauset.GenWithStackByArgs("type int(11) not match origin decimal(2,1)")},
		{"decimal", "int", errUnsupportedModifyDeferredCauset.GenWithStackByArgs("type int(11) not match origin decimal(11,0)")},
		{"decimal(2,1)", "bigint", errUnsupportedModifyDeferredCauset.GenWithStackByArgs("type bigint(20) not match origin decimal(2,1)")},
	}
	for _, tt := range tests {
		ftA := s.defCausDefStrToFieldType(c, tt.origin)
		ftB := s.defCausDefStrToFieldType(c, tt.to)
		err := checkModifyTypes(ctx, ftA, ftB, false)
		if err == nil {
			c.Assert(tt.err, IsNil, Commentf("origin:%v, to:%v", tt.origin, tt.to))
		} else {
			c.Assert(err.Error(), Equals, tt.err.Error())
		}
	}
}

func (s *testDeferredCausetSuite) defCausDefStrToFieldType(c *C, str string) *types.FieldType {
	sqlA := "alter block t modify defCausumn a " + str
	stmt, err := berolinaAllegroSQL.New().ParseOneStmt(sqlA, "", "")
	c.Assert(err, IsNil)
	defCausDef := stmt.(*ast.AlterBlockStmt).Specs[0].NewDeferredCausets[0]
	chs, defCausl := charset.GetDefaultCharsetAndDefCauslate()
	defCaus, _, err := buildDeferredCausetAndConstraint(nil, 0, defCausDef, nil, chs, defCausl)
	c.Assert(err, IsNil)
	return &defCaus.FieldType
}

func (s *testDeferredCausetSuite) TestFieldCase(c *C) {
	var fields = []string{"field", "Field"}
	defCausObjects := make([]*perceptron.DeferredCausetInfo, len(fields))
	for i, name := range fields {
		defCausObjects[i] = &perceptron.DeferredCausetInfo{
			Name: perceptron.NewCIStr(name),
		}
	}
	err := checkDuplicateDeferredCauset(defCausObjects)
	c.Assert(err.Error(), Equals, schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs("Field").Error())
}
