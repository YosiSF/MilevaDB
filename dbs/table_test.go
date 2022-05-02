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
	"bytes"
	"context"
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	causetstore ekv.CausetStorage
	dbInfo      *perceptron.DBInfo

	d *dbs
}

func testTableInfoWith2IndexOnFirstDeferredCauset(c *C, d *dbs, name string, num int) *perceptron.TableInfo {
	normalInfo := testTableInfo(c, d, name, num)
	idxs := make([]*perceptron.IndexInfo, 0, 2)
	for i := range idxs {
		idx := &perceptron.IndexInfo{
			Name:            perceptron.NewCIStr(fmt.Sprintf("i%d", i+1)),
			State:           perceptron.StatePublic,
			DeferredCausets: []*perceptron.IndexDeferredCauset{{Name: perceptron.NewCIStr("c1")}},
		}
		idxs = append(idxs, idx)
	}
	normalInfo.Indices = idxs
	normalInfo.DeferredCausets[0].FieldType.Flen = 11
	return normalInfo
}

// testTableInfo creates a test block with num int columns and with no index.
func testTableInfo(c *C, d *dbs, name string, num int) *perceptron.TableInfo {
	tblInfo := &perceptron.TableInfo{
		Name: perceptron.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	tblInfo.ID = genIDs[0]

	defcaus := make([]*perceptron.DeferredCausetInfo, num)
	for i := range defcaus {
		col := &perceptron.DeferredCausetInfo{
			Name:         perceptron.NewCIStr(fmt.Sprintf("c%d", i+1)),
			Offset:       i,
			DefaultValue: i + 1,
			State:        perceptron.StatePublic,
		}

		col.FieldType = *types.NewFieldType(allegrosql.TypeLong)
		col.ID = allocateDeferredCausetID(tblInfo)
		defcaus[i] = col
	}
	tblInfo.DeferredCausets = defcaus
	tblInfo.Charset = "utf8"
	tblInfo.DefCauslate = "utf8_bin"
	return tblInfo
}

// testTableInfoWithPartition creates a test block with num int columns and with no index.
func testTableInfoWithPartition(c *C, d *dbs, name string, num int) *perceptron.TableInfo {
	tblInfo := testTableInfo(c, d, name, num)
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	pid := genIDs[0]
	tblInfo.Partition = &perceptron.PartitionInfo{
		Type:   perceptron.PartitionTypeRange,
		Expr:   tblInfo.DeferredCausets[0].Name.L,
		Enable: true,
		Definitions: []perceptron.PartitionDefinition{{
			ID:       pid,
			Name:     perceptron.NewCIStr("p0"),
			LessThan: []string{"maxvalue"},
		}},
	}

	return tblInfo
}

// testTableInfoWithPartitionLessThan creates a test block with num int columns and one partition specified with lessthan.
func testTableInfoWithPartitionLessThan(c *C, d *dbs, name string, num int, lessthan string) *perceptron.TableInfo {
	tblInfo := testTableInfoWithPartition(c, d, name, num)
	tblInfo.Partition.Definitions[0].LessThan = []string{lessthan}
	return tblInfo
}

func testAddedNewTablePartitionInfo(c *C, d *dbs, tblInfo *perceptron.TableInfo, partName, lessthan string) *perceptron.PartitionInfo {
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	pid := genIDs[0]
	// the new added partition should change the partition state to state none at the beginning.
	return &perceptron.PartitionInfo{
		Type:   perceptron.PartitionTypeRange,
		Expr:   tblInfo.DeferredCausets[0].Name.L,
		Enable: true,
		Definitions: []perceptron.PartitionDefinition{{
			ID:       pid,
			Name:     perceptron.NewCIStr(partName),
			LessThan: []string{lessthan},
		}},
	}
}

// testViewInfo creates a test view with num int columns.
func testViewInfo(c *C, d *dbs, name string, num int) *perceptron.TableInfo {
	tblInfo := &perceptron.TableInfo{
		Name: perceptron.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	tblInfo.ID = genIDs[0]

	defcaus := make([]*perceptron.DeferredCausetInfo, num)
	viewDefCauss := make([]perceptron.CIStr, num)

	var stmtBuffer bytes.Buffer
	stmtBuffer.WriteString("SELECT ")
	for i := range defcaus {
		col := &perceptron.DeferredCausetInfo{
			Name:   perceptron.NewCIStr(fmt.Sprintf("c%d", i+1)),
			Offset: i,
			State:  perceptron.StatePublic,
		}

		col.ID = allocateDeferredCausetID(tblInfo)
		defcaus[i] = col
		viewDefCauss[i] = col.Name
		stmtBuffer.WriteString(defcaus[i].Name.L + ",")
	}
	stmtBuffer.WriteString("1 FROM t")

	view := perceptron.ViewInfo{DefCauss: viewDefCauss, Security: perceptron.SecurityDefiner, Algorithm: perceptron.AlgorithmMerge,
		SelectStmt: stmtBuffer.String(), CheckOption: perceptron.CheckOptionCascaded, Definer: &auth.UserIdentity{CurrentUser: true}}

	tblInfo.View = &view
	tblInfo.DeferredCausets = defcaus

	return tblInfo
}

func testCreateTable(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.TableInfo) *perceptron.Job {
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       perceptron.CausetActionCreateTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.State = perceptron.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = perceptron.StateNone
	return job
}

func testCreateView(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.TableInfo) *perceptron.Job {
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       perceptron.CausetActionCreateView,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{tblInfo, false, 0},
	}

	c.Assert(tblInfo.IsView(), IsTrue)
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.State = perceptron.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = perceptron.StateNone
	return job
}

func testRenameTable(c *C, ctx stochastikctx.Context, d *dbs, newSchemaID, oldSchemaID int64, tblInfo *perceptron.TableInfo) *perceptron.Job {
	job := &perceptron.Job{
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       perceptron.CausetActionRenameTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{oldSchemaID, tblInfo.Name},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.State = perceptron.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = perceptron.StateNone
	return job
}

func testLockTable(c *C, ctx stochastikctx.Context, d *dbs, newSchemaID int64, tblInfo *perceptron.TableInfo, lockTp perceptron.TableLockType) *perceptron.Job {
	arg := &lockTablesArg{
		LockTables: []perceptron.TableLockTpInfo{{SchemaID: newSchemaID, TableID: tblInfo.ID, Tp: lockTp}},
		StochastikInfo: perceptron.StochastikInfo{
			ServerID:     d.GetID(),
			StochastikID: ctx.GetStochaseinstein_dbars().ConnectionID,
		},
	}
	job := &perceptron.Job{
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       perceptron.CausetActionLockTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{arg},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v})
	return job
}

func checkTableLockedTest(c *C, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.TableInfo, serverID string, stochastikID uint64, lockTp perceptron.TableLockType) {
	err := ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		info, err := t.GetTable(dbInfo.ID, tblInfo.ID)
		c.Assert(err, IsNil)

		c.Assert(info, NotNil)
		c.Assert(info.Lock, NotNil)
		c.Assert(len(info.Lock.Stochastiks) == 1, IsTrue)
		c.Assert(info.Lock.Stochastiks[0].ServerID, Equals, serverID)
		c.Assert(info.Lock.Stochastiks[0].StochastikID, Equals, stochastikID)
		c.Assert(info.Lock.Tp, Equals, lockTp)
		c.Assert(info.Lock.State, Equals, perceptron.TableLockStatePublic)
		return nil
	})
	c.Assert(err, IsNil)
}

func testDropTable(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.TableInfo) *perceptron.Job {
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       perceptron.CausetActionDropTable,
		BinlogInfo: &perceptron.HistoryInfo{},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testTruncateTable(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.TableInfo) *perceptron.Job {
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	newTableID := genIDs[0]
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       perceptron.CausetActionTruncateTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{newTableID},
	}
	err = d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.ID = newTableID
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testCheckTableState(c *C, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.TableInfo, state perceptron.SchemaState) {
	err := ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		info, err := t.GetTable(dbInfo.ID, tblInfo.ID)
		c.Assert(err, IsNil)

		if state == perceptron.StateNone {
			c.Assert(info, IsNil)
			return nil
		}

		c.Assert(info.Name, DeepEquals, tblInfo.Name)
		c.Assert(info.State, Equals, state)
		return nil
	})
	c.Assert(err, IsNil)
}

func testGetTable(c *C, d *dbs, schemaID int64, blockID int64) block.Block {
	tbl, err := testGetTableWithError(d, schemaID, blockID)
	c.Assert(err, IsNil)
	return tbl
}

func testGetTableWithError(d *dbs, schemaID, blockID int64) (block.Block, error) {
	var tblInfo *perceptron.TableInfo
	err := ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		tblInfo, err1 = t.GetTable(schemaID, blockID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil {
		return nil, errors.New("block not found")
	}
	alloc := autoid.NewSlabPredictor(d.causetstore, schemaID, false, autoid.RowIDAllocType)
	tbl, err := block.TableFromMeta(autoid.NewSlabPredictors(alloc), tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tbl, nil
}

func (s *testTableSuite) SetUpSuite(c *C) {
	s.causetstore = testCreateStore(c, "test_block")
	s.d = testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)

	s.dbInfo = testSchemaInfo(c, s.d, "test")
	testCreateSchema(c, testNewContext(s.d), s.d, s.dbInfo)
}

func (s *testTableSuite) TearDownSuite(c *C) {
	testDropSchema(c, testNewContext(s.d), s.d, s.dbInfo)
	s.d.Stop()
	s.causetstore.Close()
}

func (s *testTableSuite) TestTable(c *C) {
	d := s.d

	ctx := testNewContext(d)

	tblInfo := testTableInfo(c, d, "t", 3)
	job := testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(c, d, s.dbInfo, tblInfo, perceptron.StatePublic)
	testCheckJobDone(c, d, job, true)

	// Create an existing block.
	newTblInfo := testTableInfo(c, d, "t", 3)
	doDBSJobErr(c, s.dbInfo.ID, newTblInfo.ID, perceptron.CausetActionCreateTable, []interface{}{newTblInfo}, ctx, d)

	count := 2000
	tbl := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	for i := 1; i <= count; i++ {
		_, err := tbl.AddRecord(ctx, types.MakeCausets(i, i, i))
		c.Assert(err, IsNil)
	}

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	// for truncate block
	tblInfo = testTableInfo(c, d, "tt", 3)
	job = testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(c, d, s.dbInfo, tblInfo, perceptron.StatePublic)
	testCheckJobDone(c, d, job, true)
	job = testTruncateTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(c, d, s.dbInfo, tblInfo, perceptron.StatePublic)
	testCheckJobDone(c, d, job, true)

	// for rename block
	dbInfo1 := testSchemaInfo(c, s.d, "test_rename_block")
	testCreateSchema(c, testNewContext(s.d), s.d, dbInfo1)
	job = testRenameTable(c, ctx, d, dbInfo1.ID, s.dbInfo.ID, tblInfo)
	testCheckTableState(c, d, dbInfo1, tblInfo, perceptron.StatePublic)
	testCheckJobDone(c, d, job, true)

	job = testLockTable(c, ctx, d, dbInfo1.ID, tblInfo, perceptron.TableLockWrite)
	testCheckTableState(c, d, dbInfo1, tblInfo, perceptron.StatePublic)
	testCheckJobDone(c, d, job, true)
	checkTableLockedTest(c, d, dbInfo1, tblInfo, d.GetID(), ctx.GetStochaseinstein_dbars().ConnectionID, perceptron.TableLockWrite)
}
