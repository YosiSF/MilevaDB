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

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testSchemaSuite{})

type testSchemaSuite struct{}

func (s *testSchemaSuite) SetUpSuite(c *C) {
}

func (s *testSchemaSuite) TearDownSuite(c *C) {
}

func testSchemaInfo(c *C, d *dbs, name string) *perceptron.DBInfo {
	dbInfo := &perceptron.DBInfo{
		Name: perceptron.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	dbInfo.ID = genIDs[0]
	return dbInfo
}

func testCreateSchema(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo) *perceptron.Job {
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		Type:       perceptron.CausetActionCreateSchema,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	dbInfo.State = perceptron.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, EDB: dbInfo})
	dbInfo.State = perceptron.StateNone
	return job
}

func buildDropSchemaJob(dbInfo *perceptron.DBInfo) *perceptron.Job {
	return &perceptron.Job{
		SchemaID:   dbInfo.ID,
		Type:       perceptron.CausetActionDropSchema,
		BinlogInfo: &perceptron.HistoryInfo{},
	}
}

func testDropSchema(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo) (*perceptron.Job, int64) {
	job := buildDropSchemaJob(dbInfo)
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	ver := getSchemaVer(c, ctx)
	return job, ver
}

func isDBSJobDone(c *C, t *meta.Meta) bool {
	job, err := t.GetDBSJobByIdx(0)
	c.Assert(err, IsNil)
	if job == nil {
		return true
	}

	time.Sleep(testLease)
	return false
}

func testCheckSchemaState(c *C, d *dbs, dbInfo *perceptron.DBInfo, state perceptron.SchemaState) {
	isDropped := true

	for {
		ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
			t := meta.NewMeta(txn)
			info, err := t.GetDatabase(dbInfo.ID)
			c.Assert(err, IsNil)

			if state == perceptron.StateNone {
				isDropped = isDBSJobDone(c, t)
				if !isDropped {
					return nil
				}
				c.Assert(info, IsNil)
				return nil
			}

			c.Assert(info.Name, DeepEquals, dbInfo.Name)
			c.Assert(info.State, Equals, state)
			return nil
		})

		if isDropped {
			break
		}
	}
}

func (s *testSchemaSuite) TestSchema(c *C) {
	causetstore := testCreateStore(c, "test_schema")
	defer causetstore.Close()
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)
	dbInfo := testSchemaInfo(c, d, "test")

	// create a database.
	job := testCreateSchema(c, ctx, d, dbInfo)
	testCheckSchemaState(c, d, dbInfo, perceptron.StatePublic)
	testCheckJobDone(c, d, job, true)

	/*** to drop the schemaReplicant with two blocks. ***/
	// create block t with 100 records.
	tblInfo1 := testBlockInfo(c, d, "t", 3)
	tJob1 := testCreateBlock(c, ctx, d, dbInfo, tblInfo1)
	testCheckBlockState(c, d, dbInfo, tblInfo1, perceptron.StatePublic)
	testCheckJobDone(c, d, tJob1, true)
	tbl1 := testGetBlock(c, d, dbInfo.ID, tblInfo1.ID)
	for i := 1; i <= 100; i++ {
		_, err := tbl1.AddRecord(ctx, types.MakeCausets(i, i, i))
		c.Assert(err, IsNil)
	}
	// create block t1 with 1034 records.
	tblInfo2 := testBlockInfo(c, d, "t1", 3)
	tJob2 := testCreateBlock(c, ctx, d, dbInfo, tblInfo2)
	testCheckBlockState(c, d, dbInfo, tblInfo2, perceptron.StatePublic)
	testCheckJobDone(c, d, tJob2, true)
	tbl2 := testGetBlock(c, d, dbInfo.ID, tblInfo2.ID)
	for i := 1; i <= 1034; i++ {
		_, err := tbl2.AddRecord(ctx, types.MakeCausets(i, i, i))
		c.Assert(err, IsNil)
	}
	job, v := testDropSchema(c, ctx, d, dbInfo)
	testCheckSchemaState(c, d, dbInfo, perceptron.StateNone)
	ids := make(map[int64]struct{})
	ids[tblInfo1.ID] = struct{}{}
	ids[tblInfo2.ID] = struct{}{}
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, EDB: dbInfo, tblIDs: ids})

	// Drop a non-existent database.
	job = &perceptron.Job{
		SchemaID:   dbInfo.ID,
		Type:       perceptron.CausetActionDropSchema,
		BinlogInfo: &perceptron.HistoryInfo{},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrDatabaseDropExists), IsTrue, Commentf("err %v", err))

	// Drop a database without a block.
	dbInfo1 := testSchemaInfo(c, d, "test1")
	job = testCreateSchema(c, ctx, d, dbInfo1)
	testCheckSchemaState(c, d, dbInfo1, perceptron.StatePublic)
	testCheckJobDone(c, d, job, true)
	job, _ = testDropSchema(c, ctx, d, dbInfo1)
	testCheckSchemaState(c, d, dbInfo1, perceptron.StateNone)
	testCheckJobDone(c, d, job, false)
}

func (s *testSchemaSuite) TestSchemaWaitJob(c *C) {
	causetstore := testCreateStore(c, "test_schema_wait")
	defer causetstore.Close()

	d1 := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d1.Stop()

	testCheckOwner(c, d1, true)

	d2 := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease*4),
	)
	defer d2.Stop()
	ctx := testNewContext(d2)

	// d2 must not be owner.
	d2.ownerManager.RetireOwner()

	dbInfo := testSchemaInfo(c, d2, "test")
	testCreateSchema(c, ctx, d2, dbInfo)
	testCheckSchemaState(c, d2, dbInfo, perceptron.StatePublic)

	// d2 must not be owner.
	c.Assert(d2.ownerManager.IsOwner(), IsFalse)

	genIDs, err := d2.genGlobalIDs(1)
	c.Assert(err, IsNil)
	schemaID := genIDs[0]
	doDBSJobErr(c, schemaID, 0, perceptron.CausetActionCreateSchema, []interface{}{dbInfo}, ctx, d2)
}

func testGetSchemaInfoWithError(d *dbs, schemaID int64) (*perceptron.DBInfo, error) {
	var dbInfo *perceptron.DBInfo
	err := ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		dbInfo, err1 = t.GetDatabase(schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return dbInfo, nil
}
