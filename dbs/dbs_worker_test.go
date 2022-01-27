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
	"sync"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testDBSSuite{})
var _ = SerialSuites(&testDBSSerialSuite{})

type testDBSSuite struct {
	solitonutil.CommonHandleSuite
}
type testDBSSerialSuite struct{}

const testLease = 5 * time.Millisecond

func (s *testDBSSerialSuite) SetUpSuite(c *C) {
	SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)

	// We hope that this test is serially executed. So put it here.
	s.testRunWorker(c)
}

func (s *testDBSSuite) TestCheckOwner(c *C) {
	causetstore := testCreateStore(c, "test_owner")
	defer causetstore.Close()

	d1 := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d1.Stop()
	time.Sleep(testLease)
	testCheckOwner(c, d1, true)

	c.Assert(d1.GetLease(), Equals, testLease)
}

// testRunWorker tests no job is handled when the value of RunWorker is false.
func (s *testDBSSerialSuite) testRunWorker(c *C) {
	causetstore := testCreateStore(c, "test_run_worker")
	defer causetstore.Close()

	RunWorker = false
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	testCheckOwner(c, d, false)
	defer d.Stop()

	// Make sure the DBS worker is nil.
	worker := d.generalWorker()
	c.Assert(worker, IsNil)
	// Make sure the DBS job can be done and exit that goroutine.
	RunWorker = true
	d1 := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	testCheckOwner(c, d1, true)
	defer d1.Stop()
	worker = d1.generalWorker()
	c.Assert(worker, NotNil)
}

func (s *testDBSSuite) TestSchemaError(c *C) {
	causetstore := testCreateStore(c, "test_schema_error")
	defer causetstore.Close()

	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	doDBSJobErr(c, 1, 0, perceptron.CausetActionCreateSchema, []interface{}{1}, ctx, d)
}

func (s *testDBSSuite) TestBlockError(c *C) {
	causetstore := testCreateStore(c, "test_block_error")
	defer causetstore.Close()

	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	// Schema ID is wrong, so dropping block is failed.
	doDBSJobErr(c, -1, 1, perceptron.CausetActionDropBlock, nil, ctx, d)
	// Block ID is wrong, so dropping block is failed.
	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	job := doDBSJobErr(c, dbInfo.ID, -1, perceptron.CausetActionDropBlock, nil, ctx, d)

	// Block ID or schemaReplicant ID is wrong, so getting block is failed.
	tblInfo := testBlockInfo(c, d, "t", 3)
	testCreateBlock(c, ctx, d, dbInfo, tblInfo)
	err := ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
		job.SchemaID = -1
		job.BlockID = -1
		t := meta.NewMeta(txn)
		_, err1 := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
		c.Assert(err1, NotNil)
		job.SchemaID = dbInfo.ID
		_, err1 = getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
		c.Assert(err1, NotNil)
		return nil
	})
	c.Assert(err, IsNil)

	// Args is wrong, so creating block is failed.
	doDBSJobErr(c, 1, 1, perceptron.CausetActionCreateBlock, []interface{}{1}, ctx, d)
	// Schema ID is wrong, so creating block is failed.
	doDBSJobErr(c, -1, tblInfo.ID, perceptron.CausetActionCreateBlock, []interface{}{tblInfo}, ctx, d)
	// Block exists, so creating block is failed.
	tblInfo.ID = tblInfo.ID + 1
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionCreateBlock, []interface{}{tblInfo}, ctx, d)

}

func (s *testDBSSuite) TestViewError(c *C) {
	causetstore := testCreateStore(c, "test_view_error")
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
	testCreateSchema(c, testNewContext(d), d, dbInfo)

	// Block ID or schemaReplicant ID is wrong, so getting block is failed.
	tblInfo := testViewInfo(c, d, "t", 3)
	testCreateView(c, ctx, d, dbInfo, tblInfo)

	// Args is wrong, so creating view is failed.
	doDBSJobErr(c, 1, 1, perceptron.CausetActionCreateView, []interface{}{1}, ctx, d)
	// Schema ID is wrong and orReplace is false, so creating view is failed.
	doDBSJobErr(c, -1, tblInfo.ID, perceptron.CausetActionCreateView, []interface{}{tblInfo, false}, ctx, d)
	// View exists and orReplace is false, so creating view is failed.
	tblInfo.ID = tblInfo.ID + 1
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionCreateView, []interface{}{tblInfo, false}, ctx, d)

}

func (s *testDBSSuite) TestInvalidDBSJob(c *C) {
	causetstore := testCreateStore(c, "test_invalid_dbs_job_type_error")
	defer causetstore.Close()
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	job := &perceptron.Job{
		SchemaID:   0,
		BlockID:    0,
		Type:       perceptron.CausetActionNone,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err.Error(), Equals, "[dbs:8204]invalid dbs job type: none")
}

func (s *testDBSSuite) TestForeignKeyError(c *C) {
	causetstore := testCreateStore(c, "test_foreign_key_error")
	defer causetstore.Close()

	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	doDBSJobErr(c, -1, 1, perceptron.CausetActionAddForeignKey, nil, ctx, d)
	doDBSJobErr(c, -1, 1, perceptron.CausetActionDropForeignKey, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testBlockInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateBlock(c, ctx, d, dbInfo, tblInfo)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionDropForeignKey, []interface{}{perceptron.NewCIStr("c1_foreign_key")}, ctx, d)
}

func (s *testDBSSuite) TestIndexError(c *C) {
	causetstore := testCreateStore(c, "test_index_error")
	defer causetstore.Close()

	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)

	// Schema ID is wrong.
	doDBSJobErr(c, -1, 1, perceptron.CausetActionAddIndex, nil, ctx, d)
	doDBSJobErr(c, -1, 1, perceptron.CausetActionDropIndex, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testBlockInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateBlock(c, ctx, d, dbInfo, tblInfo)

	// for adding index
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddIndex, []interface{}{1}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddIndex,
		[]interface{}{false, perceptron.NewCIStr("t"), 1,
			[]*ast.IndexPartSpecification{{DeferredCauset: &ast.DeferredCausetName{Name: perceptron.NewCIStr("c")}, Length: 256}}}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddIndex,
		[]interface{}{false, perceptron.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{DeferredCauset: &ast.DeferredCausetName{Name: perceptron.NewCIStr("c")}, Length: 256}}}, ctx, d)
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "c1_index", "c1")
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddIndex,
		[]interface{}{false, perceptron.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{DeferredCauset: &ast.DeferredCausetName{Name: perceptron.NewCIStr("c1")}, Length: 256}}}, ctx, d)

	// for dropping index
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionDropIndex, []interface{}{1}, ctx, d)
	testDropIndex(c, ctx, d, dbInfo, tblInfo, "c1_index")
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionDropIndex, []interface{}{perceptron.NewCIStr("c1_index")}, ctx, d)
}

func (s *testDBSSuite) TestDeferredCausetError(c *C) {
	causetstore := testCreateStore(c, "test_defCausumn_error")
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
	tblInfo := testBlockInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateBlock(c, ctx, d, dbInfo, tblInfo)
	defCaus := &perceptron.DeferredCausetInfo{
		Name:         perceptron.NewCIStr("c4"),
		Offset:       len(tblInfo.DeferredCausets),
		DefaultValue: 0,
	}
	defCaus.ID = allocateDeferredCausetID(tblInfo)
	defCaus.FieldType = *types.NewFieldType(allegrosql.TypeLong)
	pos := &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionAfter, RelativeDeferredCauset: &ast.DeferredCausetName{Name: perceptron.NewCIStr("c5")}}

	defcaus := &[]*perceptron.DeferredCausetInfo{defCaus}
	positions := &[]*ast.DeferredCausetPosition{pos}

	// for adding defCausumn
	doDBSJobErr(c, -1, tblInfo.ID, perceptron.CausetActionAddDeferredCauset, []interface{}{defCaus, pos, 0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, -1, perceptron.CausetActionAddDeferredCauset, []interface{}{defCaus, pos, 0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCauset, []interface{}{0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCauset, []interface{}{defCaus, pos, 0}, ctx, d)

	// for dropping defCausumn
	doDBSJobErr(c, -1, tblInfo.ID, perceptron.CausetActionDropDeferredCauset, []interface{}{defCaus, pos, 0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, -1, perceptron.CausetActionDropDeferredCauset, []interface{}{defCaus, pos, 0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionDropDeferredCauset, []interface{}{0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionDropDeferredCauset, []interface{}{perceptron.NewCIStr("c5")}, ctx, d)

	// for adding defCausumns
	doDBSJobErr(c, -1, tblInfo.ID, perceptron.CausetActionAddDeferredCausets, []interface{}{defcaus, positions, 0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, -1, perceptron.CausetActionAddDeferredCausets, []interface{}{defcaus, positions, 0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCausets, []interface{}{0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCausets, []interface{}{defcaus, positions, 0}, ctx, d)

	// for dropping defCausumns
	doDBSJobErr(c, -1, tblInfo.ID, perceptron.CausetActionDropDeferredCausets, []interface{}{defCaus, pos, 0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, -1, perceptron.CausetActionDropDeferredCausets, []interface{}{defCaus, pos, 0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionDropDeferredCausets, []interface{}{0}, ctx, d)
	doDBSJobErr(c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionDropDeferredCausets, []interface{}{[]perceptron.CIStr{perceptron.NewCIStr("c5"), perceptron.NewCIStr("c6")}, make([]bool, 2)}, ctx, d)
}

func testCheckOwner(c *C, d *dbs, expectedVal bool) {
	c.Assert(d.isOwner(), Equals, expectedVal)
}

func testCheckJobDone(c *C, d *dbs, job *perceptron.Job, isAdd bool) {
	ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDBSJob(job.ID)
		c.Assert(err, IsNil)
		checkHistoryJob(c, historyJob)
		if isAdd {
			c.Assert(historyJob.SchemaState, Equals, perceptron.StatePublic)
		} else {
			c.Assert(historyJob.SchemaState, Equals, perceptron.StateNone)
		}

		return nil
	})
}

func testCheckJobCancelled(c *C, d *dbs, job *perceptron.Job, state *perceptron.SchemaState) {
	ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDBSJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.IsCancelled() || historyJob.IsRollbackDone(), IsTrue, Commentf("history job %s", historyJob))
		if state != nil {
			c.Assert(historyJob.SchemaState, Equals, *state)
		}
		return nil
	})
}

func doDBSJobErrWithSchemaState(ctx stochastikctx.Context, d *dbs, c *C, schemaID, blockID int64, tp perceptron.CausetActionType,
	args []interface{}, state *perceptron.SchemaState) *perceptron.Job {
	job := &perceptron.Job{
		SchemaID:   schemaID,
		BlockID:    blockID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &perceptron.HistoryInfo{},
	}
	err := d.doDBSJob(ctx, job)
	// TODO: Add the detail error check.
	c.Assert(err, NotNil, Commentf("err:%v", err))
	testCheckJobCancelled(c, d, job, state)

	return job
}

func doDBSJobSuccess(ctx stochastikctx.Context, d *dbs, c *C, schemaID, blockID int64, tp perceptron.CausetActionType,
	args []interface{}) {
	job := &perceptron.Job{
		SchemaID:   schemaID,
		BlockID:    blockID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &perceptron.HistoryInfo{},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
}

func doDBSJobErr(c *C, schemaID, blockID int64, tp perceptron.CausetActionType, args []interface{},
	ctx stochastikctx.Context, d *dbs) *perceptron.Job {
	return doDBSJobErrWithSchemaState(ctx, d, c, schemaID, blockID, tp, args, nil)
}

func checkCancelState(txn ekv.Transaction, job *perceptron.Job, test *testCancelJob) error {
	var checkErr error
	addIndexFirstReorg := (test.act == perceptron.CausetActionAddIndex || test.act == perceptron.CausetActionAddPrimaryKey) &&
		job.SchemaState == perceptron.StateWriteReorganization && job.SnapshotVer == 0
	// If the action is adding index and the state is writing reorganization, it wants to test the case of cancelling the job when backfilling indexes.
	// When the job satisfies this case of addIndexFirstReorg, the worker hasn't started to backfill indexes.
	if test.cancelState == job.SchemaState && !addIndexFirstReorg && !job.IsRollingback() {
		errs, err := admin.CancelJobs(txn, test.jobIDs)
		if err != nil {
			checkErr = errors.Trace(err)
			return checkErr
		}
		// It only tests cancel one DBS job.
		if !terror.ErrorEqual(errs[0], test.cancelRetErrs[0]) {
			checkErr = errors.Trace(errs[0])
			return checkErr
		}
	}
	return checkErr
}

type testCancelJob struct {
	jobIDs        []int64
	cancelRetErrs []error                     // cancelRetErrs is the first return value of CancelJobs.
	act           perceptron.CausetActionType // act is the job action.
	cancelState   perceptron.SchemaState
}

func buildCancelJobTests(firstID int64) []testCancelJob {
	noErrs := []error{nil}
	tests := []testCancelJob{
		{act: perceptron.CausetActionAddIndex, jobIDs: []int64{firstID + 1}, cancelRetErrs: noErrs, cancelState: perceptron.StateDeleteOnly},
		{act: perceptron.CausetActionAddIndex, jobIDs: []int64{firstID + 2}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteOnly},
		{act: perceptron.CausetActionAddIndex, jobIDs: []int64{firstID + 3}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteReorganization},
		{act: perceptron.CausetActionAddIndex, jobIDs: []int64{firstID + 4}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 4)}, cancelState: perceptron.StatePublic},

		// Test cancel drop index job , see TestCancelDropIndex.
		{act: perceptron.CausetActionAddDeferredCauset, jobIDs: []int64{firstID + 5}, cancelRetErrs: noErrs, cancelState: perceptron.StateDeleteOnly},
		{act: perceptron.CausetActionAddDeferredCauset, jobIDs: []int64{firstID + 6}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteOnly},
		{act: perceptron.CausetActionAddDeferredCauset, jobIDs: []int64{firstID + 7}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteReorganization},
		{act: perceptron.CausetActionAddDeferredCauset, jobIDs: []int64{firstID + 8}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 8)}, cancelState: perceptron.StatePublic},

		// Test create block, watch out, block id will alloc a globalID.
		{act: perceptron.CausetActionCreateBlock, jobIDs: []int64{firstID + 10}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		// Test create database, watch out, database id will alloc a globalID.
		{act: perceptron.CausetActionCreateSchema, jobIDs: []int64{firstID + 12}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},

		{act: perceptron.CausetActionDropDeferredCauset, jobIDs: []int64{firstID + 13}, cancelRetErrs: []error{admin.ErrCannotCancelDBSJob.GenWithStackByArgs(firstID + 13)}, cancelState: perceptron.StateDeleteOnly},
		{act: perceptron.CausetActionDropDeferredCauset, jobIDs: []int64{firstID + 14}, cancelRetErrs: []error{admin.ErrCannotCancelDBSJob.GenWithStackByArgs(firstID + 14)}, cancelState: perceptron.StateWriteOnly},
		{act: perceptron.CausetActionDropDeferredCauset, jobIDs: []int64{firstID + 15}, cancelRetErrs: []error{admin.ErrCannotCancelDBSJob.GenWithStackByArgs(firstID + 15)}, cancelState: perceptron.StateWriteReorganization},
		{act: perceptron.CausetActionRebaseAutoID, jobIDs: []int64{firstID + 16}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionShardRowID, jobIDs: []int64{firstID + 17}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},

		{act: perceptron.CausetActionModifyDeferredCauset, jobIDs: []int64{firstID + 18}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionModifyDeferredCauset, jobIDs: []int64{firstID + 19}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 19)}, cancelState: perceptron.StateDeleteOnly},

		{act: perceptron.CausetActionAddForeignKey, jobIDs: []int64{firstID + 20}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionAddForeignKey, jobIDs: []int64{firstID + 21}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 21)}, cancelState: perceptron.StatePublic},
		{act: perceptron.CausetActionDropForeignKey, jobIDs: []int64{firstID + 22}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionDropForeignKey, jobIDs: []int64{firstID + 23}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 23)}, cancelState: perceptron.StatePublic},

		{act: perceptron.CausetActionRenameBlock, jobIDs: []int64{firstID + 24}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionRenameBlock, jobIDs: []int64{firstID + 25}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 25)}, cancelState: perceptron.StatePublic},

		{act: perceptron.CausetActionModifyBlockCharsetAndDefCauslate, jobIDs: []int64{firstID + 26}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionModifyBlockCharsetAndDefCauslate, jobIDs: []int64{firstID + 27}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 27)}, cancelState: perceptron.StatePublic},
		{act: perceptron.CausetActionTruncateBlockPartition, jobIDs: []int64{firstID + 28}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionTruncateBlockPartition, jobIDs: []int64{firstID + 29}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 29)}, cancelState: perceptron.StatePublic},
		{act: perceptron.CausetActionModifySchemaCharsetAndDefCauslate, jobIDs: []int64{firstID + 31}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionModifySchemaCharsetAndDefCauslate, jobIDs: []int64{firstID + 32}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 32)}, cancelState: perceptron.StatePublic},

		{act: perceptron.CausetActionAddPrimaryKey, jobIDs: []int64{firstID + 33}, cancelRetErrs: noErrs, cancelState: perceptron.StateDeleteOnly},
		{act: perceptron.CausetActionAddPrimaryKey, jobIDs: []int64{firstID + 34}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteOnly},
		{act: perceptron.CausetActionAddPrimaryKey, jobIDs: []int64{firstID + 35}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteReorganization},
		{act: perceptron.CausetActionAddPrimaryKey, jobIDs: []int64{firstID + 36}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 36)}, cancelState: perceptron.StatePublic},
		{act: perceptron.CausetActionDropPrimaryKey, jobIDs: []int64{firstID + 37}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteOnly},
		{act: perceptron.CausetActionDropPrimaryKey, jobIDs: []int64{firstID + 38}, cancelRetErrs: []error{admin.ErrCannotCancelDBSJob.GenWithStackByArgs(firstID + 38)}, cancelState: perceptron.StateDeleteOnly},

		{act: perceptron.CausetActionAddDeferredCausets, jobIDs: []int64{firstID + 39}, cancelRetErrs: noErrs, cancelState: perceptron.StateDeleteOnly},
		{act: perceptron.CausetActionAddDeferredCausets, jobIDs: []int64{firstID + 40}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteOnly},
		{act: perceptron.CausetActionAddDeferredCausets, jobIDs: []int64{firstID + 41}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteReorganization},
		{act: perceptron.CausetActionAddDeferredCausets, jobIDs: []int64{firstID + 42}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 42)}, cancelState: perceptron.StatePublic},

		{act: perceptron.CausetActionDropDeferredCausets, jobIDs: []int64{firstID + 43}, cancelRetErrs: []error{admin.ErrCannotCancelDBSJob.GenWithStackByArgs(firstID + 43)}, cancelState: perceptron.StateDeleteOnly},
		{act: perceptron.CausetActionDropDeferredCausets, jobIDs: []int64{firstID + 44}, cancelRetErrs: []error{admin.ErrCannotCancelDBSJob.GenWithStackByArgs(firstID + 44)}, cancelState: perceptron.StateWriteOnly},
		{act: perceptron.CausetActionDropDeferredCausets, jobIDs: []int64{firstID + 45}, cancelRetErrs: []error{admin.ErrCannotCancelDBSJob.GenWithStackByArgs(firstID + 45)}, cancelState: perceptron.StateWriteReorganization},

		{act: perceptron.CausetActionAlterIndexVisibility, jobIDs: []int64{firstID + 47}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionAlterIndexVisibility, jobIDs: []int64{firstID + 48}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 48)}, cancelState: perceptron.StatePublic},

		{act: perceptron.CausetActionExchangeBlockPartition, jobIDs: []int64{firstID + 54}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionExchangeBlockPartition, jobIDs: []int64{firstID + 55}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob.GenWithStackByArgs(firstID + 55)}, cancelState: perceptron.StatePublic},

		{act: perceptron.CausetActionAddBlockPartition, jobIDs: []int64{firstID + 60}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionAddBlockPartition, jobIDs: []int64{firstID + 61}, cancelRetErrs: noErrs, cancelState: perceptron.StateReplicaOnly},
		{act: perceptron.CausetActionAddBlockPartition, jobIDs: []int64{firstID + 62}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob}, cancelState: perceptron.StatePublic},

		// modify defCausumn has two different types, normal-type and reorg-type. The latter has 5 states and it can be cancelled except the public state.
		{act: perceptron.CausetActionModifyDeferredCauset, jobIDs: []int64{firstID + 65}, cancelRetErrs: noErrs, cancelState: perceptron.StateNone},
		{act: perceptron.CausetActionModifyDeferredCauset, jobIDs: []int64{firstID + 66}, cancelRetErrs: noErrs, cancelState: perceptron.StateDeleteOnly},
		{act: perceptron.CausetActionModifyDeferredCauset, jobIDs: []int64{firstID + 67}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteOnly},
		{act: perceptron.CausetActionModifyDeferredCauset, jobIDs: []int64{firstID + 68}, cancelRetErrs: noErrs, cancelState: perceptron.StateWriteReorganization},
		{act: perceptron.CausetActionModifyDeferredCauset, jobIDs: []int64{firstID + 69}, cancelRetErrs: []error{admin.ErrCancelFinishedDBSJob}, cancelState: perceptron.StatePublic},
	}

	return tests
}

func (s *testDBSSerialSuite) checkDropIdx(c *C, d *dbs, schemaID int64, blockID int64, idxName string, success bool) {
	checkIdxExist(c, d, schemaID, blockID, idxName, !success)
}

func (s *testDBSSerialSuite) checkAddIdx(c *C, d *dbs, schemaID int64, blockID int64, idxName string, success bool) {
	checkIdxExist(c, d, schemaID, blockID, idxName, success)
}

func checkIdxExist(c *C, d *dbs, schemaID int64, blockID int64, idxName string, expectedExist bool) {
	changedBlock := testGetBlock(c, d, schemaID, blockID)
	var found bool
	for _, idxInfo := range changedBlock.Meta().Indices {
		if idxInfo.Name.O == idxName {
			found = true
			break
		}
	}
	c.Assert(found, Equals, expectedExist)
}

func (s *testDBSSerialSuite) checkAddDeferredCausets(c *C, d *dbs, schemaID int64, blockID int64, defCausNames []string, success bool) {
	changedBlock := testGetBlock(c, d, schemaID, blockID)
	found := !checkDeferredCausetsNotFound(changedBlock, defCausNames)
	c.Assert(found, Equals, success)
}

func (s *testDBSSerialSuite) checkCancelDropDeferredCausets(c *C, d *dbs, schemaID int64, blockID int64, defCausNames []string, success bool) {
	changedBlock := testGetBlock(c, d, schemaID, blockID)
	notFound := checkDeferredCausetsNotFound(changedBlock, defCausNames)
	c.Assert(notFound, Equals, success)
}

func checkDeferredCausetsNotFound(t block.Block, defCausNames []string) bool {
	notFound := true
	for _, defCausName := range defCausNames {
		for _, defCausInfo := range t.Meta().DeferredCausets {
			if defCausInfo.Name.O == defCausName {
				notFound = false
			}
		}
	}
	return notFound
}

func checkIdxVisibility(changedBlock block.Block, idxName string, expected bool) bool {
	for _, idxInfo := range changedBlock.Meta().Indices {
		if idxInfo.Name.O == idxName && idxInfo.Invisible == expected {
			return true
		}
	}
	return false
}

func (s *testDBSSerialSuite) TestCancelJob(c *C) {
	causetstore := testCreateStore(c, "test_cancel_job")
	defer causetstore.Close()
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	dbInfo := testSchemaInfo(c, d, "test_cancel_job")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	// create a partition block.
	partitionTblInfo := testBlockInfoWithPartition(c, d, "t_partition", 5)
	// Skip using sessPool. Make sure adding primary key can be successful.
	partitionTblInfo.DeferredCausets[0].Flag |= allegrosql.NotNullFlag
	// create block t (c1 int, c2 int, c3 int, c4 int, c5 int);
	tblInfo := testBlockInfo(c, d, "t", 5)
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	testCreateBlock(c, ctx, d, dbInfo, partitionTblInfo)
	blockAutoID := int64(100)
	shardRowIDBits := uint64(5)
	tblInfo.AutoIncID = blockAutoID
	tblInfo.ShardRowIDBits = shardRowIDBits
	job := testCreateBlock(c, ctx, d, dbInfo, tblInfo)
	// insert t values (1, 2, 3, 4, 5);
	originBlock := testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	event := types.MakeCausets(1, 2, 3, 4, 5)
	_, err = originBlock.AddRecord(ctx, event)
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	tc := &TestDBSCallback{}
	// set up hook
	firstJobID := job.ID
	tests := buildCancelJobTests(firstJobID)
	var checkErr error
	var mu sync.Mutex
	var test *testCancelJob
	uFIDelateTest := func(t *testCancelJob) {
		mu.Lock()
		test = t
		mu.Unlock()
	}
	hookCancelFunc := func(job *perceptron.Job) {
		if job.State == perceptron.JobStateSynced || job.State == perceptron.JobStateCancelled || job.State == perceptron.JobStateCancelling {
			return
		}
		// This hook only valid for the related test job.
		// This is use to avoid parallel test fail.
		mu.Lock()
		if len(test.jobIDs) > 0 && test.jobIDs[0] != job.ID {
			mu.Unlock()
			return
		}
		mu.Unlock()
		if checkErr != nil {
			return
		}

		hookCtx := mock.NewContext()
		hookCtx.CausetStore = causetstore
		err1 := hookCtx.NewTxn(context.Background())
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
		txn, err1 = hookCtx.Txn(true)
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
		mu.Lock()
		checkErr = checkCancelState(txn, job, test)
		mu.Unlock()
		if checkErr != nil {
			return
		}
		err1 = txn.Commit(context.Background())
		if err1 != nil {
			checkErr = errors.Trace(err1)
			return
		}
	}
	tc.onJobUFIDelated = hookCancelFunc
	tc.onJobRunBefore = hookCancelFunc
	d.SetHook(tc)

	// for adding index
	uFIDelateTest(&tests[0])
	idxOrigName := "idx"
	validArgs := []interface{}{false, perceptron.NewCIStr(idxOrigName),
		[]*ast.IndexPartSpecification{{
			DeferredCauset: &ast.DeferredCausetName{Name: perceptron.NewCIStr("c1")},
			Length:         -1,
		}}, nil}

	// When the job satisfies this test case, the option will be rollback, so the job's schemaReplicant state is none.
	cancelState := perceptron.StateNone
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	uFIDelateTest(&tests[1])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	uFIDelateTest(&tests[2])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	uFIDelateTest(&tests[3])
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "idx", "c2")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for add defCausumn
	uFIDelateTest(&tests[4])
	addingDefCausName := "defCausA"
	newDeferredCausetDef := &ast.DeferredCausetDef{
		Name:    &ast.DeferredCausetName{Name: perceptron.NewCIStr(addingDefCausName)},
		Tp:      &types.FieldType{Tp: allegrosql.TypeLonglong},
		Options: []*ast.DeferredCausetOption{},
	}
	chs, defCausl := charset.GetDefaultCharsetAndDefCauslate()
	defCaus, _, err := buildDeferredCausetAndConstraint(ctx, 2, newDeferredCausetDef, nil, chs, defCausl)
	c.Assert(err, IsNil)

	addDeferredCausetArgs := []interface{}{defCaus, &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}, 0}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCauset, addDeferredCausetArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{addingDefCausName}, false)

	uFIDelateTest(&tests[5])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCauset, addDeferredCausetArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{addingDefCausName}, false)

	uFIDelateTest(&tests[6])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCauset, addDeferredCausetArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{addingDefCausName}, false)

	uFIDelateTest(&tests[7])
	testAddDeferredCauset(c, ctx, d, dbInfo, tblInfo, addDeferredCausetArgs)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{addingDefCausName}, true)

	// for create block
	tblInfo1 := testBlockInfo(c, d, "t1", 2)
	uFIDelateTest(&tests[8])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo1.ID, perceptron.CausetActionCreateBlock, []interface{}{tblInfo1}, &cancelState)
	c.Check(checkErr, IsNil)
	testCheckBlockState(c, d, dbInfo, tblInfo1, perceptron.StateNone)

	// for create database
	dbInfo1 := testSchemaInfo(c, d, "test_cancel_job1")
	uFIDelateTest(&tests[9])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo1.ID, 0, perceptron.CausetActionCreateSchema, []interface{}{dbInfo1}, &cancelState)
	c.Check(checkErr, IsNil)
	testCheckSchemaState(c, d, dbInfo1, perceptron.StateNone)

	// for drop defCausumn.
	uFIDelateTest(&tests[10])
	dropDefCausName := "c3"
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{dropDefCausName}, false)
	testDropDeferredCauset(c, ctx, d, dbInfo, tblInfo, dropDefCausName, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{dropDefCausName}, true)

	uFIDelateTest(&tests[11])
	dropDefCausName = "c4"
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{dropDefCausName}, false)
	testDropDeferredCauset(c, ctx, d, dbInfo, tblInfo, dropDefCausName, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{dropDefCausName}, true)

	uFIDelateTest(&tests[12])
	dropDefCausName = "c5"
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{dropDefCausName}, false)
	testDropDeferredCauset(c, ctx, d, dbInfo, tblInfo, dropDefCausName, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, []string{dropDefCausName}, true)

	// cancel rebase auto id
	uFIDelateTest(&tests[13])
	rebaseIDArgs := []interface{}{int64(200)}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionRebaseAutoID, rebaseIDArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	changedBlock := testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedBlock.Meta().AutoIncID, Equals, blockAutoID)

	// cancel shard bits
	uFIDelateTest(&tests[14])
	shardRowIDArgs := []interface{}{uint64(7)}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionShardRowID, shardRowIDArgs, &cancelState)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedBlock.Meta().ShardRowIDBits, Equals, shardRowIDBits)

	// modify none-state defCausumn
	defCaus.DefaultValue = "1"
	uFIDelateTest(&tests[15])
	modifyDeferredCausetArgs := []interface{}{defCaus, defCaus.Name, &ast.DeferredCausetPosition{}, byte(0)}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyDeferredCausetArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	changedDefCaus := perceptron.FindDeferredCausetInfo(changedBlock.Meta().DeferredCausets, defCaus.Name.L)
	c.Assert(changedDefCaus.DefaultValue, IsNil)

	// modify delete-only-state defCausumn,
	defCaus.FieldType.Tp = allegrosql.TypeTiny
	defCaus.FieldType.Flen = defCaus.FieldType.Flen - 1
	uFIDelateTest(&tests[16])
	modifyDeferredCausetArgs = []interface{}{defCaus, defCaus.Name, &ast.DeferredCausetPosition{}, byte(0)}
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyDeferredCausetArgs)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	changedDefCaus = perceptron.FindDeferredCausetInfo(changedBlock.Meta().DeferredCausets, defCaus.Name.L)
	c.Assert(changedDefCaus.FieldType.Tp, Equals, allegrosql.TypeTiny)
	c.Assert(changedDefCaus.FieldType.Flen, Equals, defCaus.FieldType.Flen)
	defCaus.FieldType.Flen++

	// Test add foreign key failed cause by canceled.
	uFIDelateTest(&tests[17])
	addForeignKeyArgs := []interface{}{perceptron.FKInfo{Name: perceptron.NewCIStr("fk1")}}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, addForeignKeyArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(len(changedBlock.Meta().ForeignKeys), Equals, 0)

	// Test add foreign key successful.
	uFIDelateTest(&tests[18])
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, addForeignKeyArgs)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(len(changedBlock.Meta().ForeignKeys), Equals, 1)
	c.Assert(changedBlock.Meta().ForeignKeys[0].Name, Equals, addForeignKeyArgs[0].(perceptron.FKInfo).Name)

	// Test drop foreign key failed cause by canceled.
	uFIDelateTest(&tests[19])
	dropForeignKeyArgs := []interface{}{addForeignKeyArgs[0].(perceptron.FKInfo).Name}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, dropForeignKeyArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(len(changedBlock.Meta().ForeignKeys), Equals, 1)
	c.Assert(changedBlock.Meta().ForeignKeys[0].Name, Equals, dropForeignKeyArgs[0].(perceptron.CIStr))

	// Test drop foreign key successful.
	uFIDelateTest(&tests[20])
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, dropForeignKeyArgs)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(len(changedBlock.Meta().ForeignKeys), Equals, 0)

	// test rename block failed caused by canceled.
	test = &tests[21]
	renameBlockArgs := []interface{}{dbInfo.ID, perceptron.NewCIStr("t2")}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, renameBlockArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedBlock.Meta().Name.L, Equals, "t")

	// test rename block successful.
	test = &tests[22]
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, renameBlockArgs)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedBlock.Meta().Name.L, Equals, "t2")

	// test modify block charset failed caused by canceled.
	test = &tests[23]
	modifyBlockCharsetArgs := []interface{}{"utf8mb4", "utf8mb4_bin"}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyBlockCharsetArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedBlock.Meta().Charset, Equals, "utf8")
	c.Assert(changedBlock.Meta().DefCauslate, Equals, "utf8_bin")

	// test modify block charset successfully.
	test = &tests[24]
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, modifyBlockCharsetArgs)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(changedBlock.Meta().Charset, Equals, "utf8mb4")
	c.Assert(changedBlock.Meta().DefCauslate, Equals, "utf8mb4_bin")

	// test truncate block partition failed caused by canceled.
	test = &tests[25]
	truncateTblPartitionArgs := []interface{}{[]int64{partitionTblInfo.Partition.Definitions[0].ID}}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, partitionTblInfo.ID, test.act, truncateTblPartitionArgs, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, partitionTblInfo.ID)
	c.Assert(changedBlock.Meta().Partition.Definitions[0].ID == partitionTblInfo.Partition.Definitions[0].ID, IsTrue)

	// test truncate block partition charset successfully.
	test = &tests[26]
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, partitionTblInfo.ID, test.act, truncateTblPartitionArgs)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, partitionTblInfo.ID)
	c.Assert(changedBlock.Meta().Partition.Definitions[0].ID == partitionTblInfo.Partition.Definitions[0].ID, IsFalse)

	// test modify schemaReplicant charset failed caused by canceled.
	test = &tests[27]
	charsetAndDefCauslate := []interface{}{"utf8mb4", "utf8mb4_bin"}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, charsetAndDefCauslate, &test.cancelState)
	c.Check(checkErr, IsNil)
	dbInfo, err = testGetSchemaInfoWithError(d, dbInfo.ID)
	c.Assert(err, IsNil)
	c.Assert(dbInfo.Charset, Equals, "")
	c.Assert(dbInfo.DefCauslate, Equals, "")

	// test modify block charset successfully.
	test = &tests[28]
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, charsetAndDefCauslate)
	c.Check(checkErr, IsNil)
	dbInfo, err = testGetSchemaInfoWithError(d, dbInfo.ID)
	c.Assert(err, IsNil)
	c.Assert(dbInfo.Charset, Equals, "utf8mb4")
	c.Assert(dbInfo.DefCauslate, Equals, "utf8mb4_bin")

	// for adding primary key
	tblInfo = changedBlock.Meta()
	uFIDelateTest(&tests[29])
	idxOrigName = "primary"
	validArgs = []interface{}{false, perceptron.NewCIStr(idxOrigName),
		[]*ast.IndexPartSpecification{{
			DeferredCauset: &ast.DeferredCausetName{Name: perceptron.NewCIStr("c1")},
			Length:         -1,
		}}, nil}
	cancelState = perceptron.StateNone
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddPrimaryKey, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	uFIDelateTest(&tests[30])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddPrimaryKey, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	uFIDelateTest(&tests[31])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddPrimaryKey, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	uFIDelateTest(&tests[32])
	testCreatePrimaryKey(c, ctx, d, dbInfo, tblInfo, "c1")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for dropping primary key
	uFIDelateTest(&tests[33])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionDropPrimaryKey, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkDropIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, false)
	uFIDelateTest(&tests[34])
	testDropIndex(c, ctx, d, dbInfo, tblInfo, idxOrigName)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkDropIdx(c, d, dbInfo.ID, tblInfo.ID, idxOrigName, true)

	// for add defCausumns
	uFIDelateTest(&tests[35])
	addingDefCausNames := []string{"defCausA", "defCausB", "defCausC", "defCausD", "defCausE", "defCausF"}
	defcaus := make([]*block.DeferredCauset, len(addingDefCausNames))
	for i, addingDefCausName := range addingDefCausNames {
		newDeferredCausetDef := &ast.DeferredCausetDef{
			Name:    &ast.DeferredCausetName{Name: perceptron.NewCIStr(addingDefCausName)},
			Tp:      &types.FieldType{Tp: allegrosql.TypeLonglong},
			Options: []*ast.DeferredCausetOption{},
		}
		defCaus, _, err := buildDeferredCausetAndConstraint(ctx, 0, newDeferredCausetDef, nil, allegrosql.DefaultCharset, "")
		c.Assert(err, IsNil)
		defcaus[i] = defCaus
	}
	offsets := make([]int, len(defcaus))
	positions := make([]*ast.DeferredCausetPosition, len(defcaus))
	for i := range positions {
		positions[i] = &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}
	}
	ifNotExists := make([]bool, len(defcaus))

	addDeferredCausetArgs = []interface{}{defcaus, positions, offsets, ifNotExists}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCausets, addDeferredCausetArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, addingDefCausNames, false)

	uFIDelateTest(&tests[36])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCausets, addDeferredCausetArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, addingDefCausNames, false)

	uFIDelateTest(&tests[37])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, perceptron.CausetActionAddDeferredCausets, addDeferredCausetArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, addingDefCausNames, false)

	uFIDelateTest(&tests[38])
	testAddDeferredCausets(c, ctx, d, dbInfo, tblInfo, addDeferredCausetArgs)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkAddDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, addingDefCausNames, true)

	// for drop defCausumns
	uFIDelateTest(&tests[39])
	dropDefCausNames := []string{"defCausA", "defCausB"}
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, dropDefCausNames, false)
	testDropDeferredCausets(c, ctx, d, dbInfo, tblInfo, dropDefCausNames, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, dropDefCausNames, true)

	uFIDelateTest(&tests[40])
	dropDefCausNames = []string{"defCausC", "defCausD"}
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, dropDefCausNames, false)
	testDropDeferredCausets(c, ctx, d, dbInfo, tblInfo, dropDefCausNames, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, dropDefCausNames, true)

	uFIDelateTest(&tests[41])
	dropDefCausNames = []string{"defCausE", "defCausF"}
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, dropDefCausNames, false)
	testDropDeferredCausets(c, ctx, d, dbInfo, tblInfo, dropDefCausNames, false)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	s.checkCancelDropDeferredCausets(c, d, dbInfo.ID, tblInfo.ID, dropDefCausNames, true)

	// test alter index visibility failed caused by canceled.
	indexName := "idx_c3"
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, indexName, "c3")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	s.checkAddIdx(c, d, dbInfo.ID, tblInfo.ID, indexName, true)

	uFIDelateTest(&tests[42])
	alterIndexVisibility := []interface{}{perceptron.NewCIStr(indexName), true}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, alterIndexVisibility, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(checkIdxVisibility(changedBlock, indexName, false), IsTrue)

	// cancel alter index visibility successfully
	uFIDelateTest(&tests[43])
	alterIndexVisibility = []interface{}{perceptron.NewCIStr(indexName), true}
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, tblInfo.ID, test.act, alterIndexVisibility)
	c.Check(checkErr, IsNil)
	changedBlock = testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	c.Assert(checkIdxVisibility(changedBlock, indexName, true), IsTrue)

	// test exchange partition failed caused by canceled
	pt := testBlockInfoWithPartition(c, d, "pt", 5)
	nt := testBlockInfo(c, d, "nt", 5)
	testCreateBlock(c, ctx, d, dbInfo, pt)
	testCreateBlock(c, ctx, d, dbInfo, nt)

	uFIDelateTest(&tests[44])
	defID := pt.Partition.Definitions[0].ID
	exchangeBlockPartition := []interface{}{defID, dbInfo.ID, pt.ID, "p0", true}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, nt.ID, test.act, exchangeBlockPartition, &test.cancelState)
	c.Check(checkErr, IsNil)
	changedNtBlock := testGetBlock(c, d, dbInfo.ID, nt.ID)
	changedPtBlock := testGetBlock(c, d, dbInfo.ID, pt.ID)
	c.Assert(changedNtBlock.Meta().ID == nt.ID, IsTrue)
	c.Assert(changedPtBlock.Meta().Partition.Definitions[0].ID == pt.Partition.Definitions[0].ID, IsTrue)

	// cancel exchange partition successfully
	uFIDelateTest(&tests[45])
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, nt.ID, test.act, exchangeBlockPartition)
	c.Check(checkErr, IsNil)
	changedNtBlock = testGetBlock(c, d, dbInfo.ID, pt.Partition.Definitions[0].ID)
	changedPtBlock = testGetBlock(c, d, dbInfo.ID, pt.ID)
	c.Assert(changedNtBlock.Meta().ID == nt.ID, IsFalse)
	c.Assert(changedPtBlock.Meta().Partition.Definitions[0].ID == nt.ID, IsTrue)

	// Cancel add block partition.
	baseBlockInfo := testBlockInfoWithPartitionLessThan(c, d, "empty_block", 5, "1000")
	testCreateBlock(c, ctx, d, dbInfo, baseBlockInfo)

	cancelState = perceptron.StateNone
	uFIDelateTest(&tests[46])
	addedPartInfo := testAddedNewBlockPartitionInfo(c, d, baseBlockInfo, "p1", "maxvalue")
	addPartitionArgs := []interface{}{addedPartInfo}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, baseBlockInfo.ID, test.act, addPartitionArgs, &cancelState)
	c.Check(checkErr, IsNil)
	baseBlock := testGetBlock(c, d, dbInfo.ID, baseBlockInfo.ID)
	c.Assert(len(baseBlock.Meta().Partition.Definitions), Equals, 1)

	uFIDelateTest(&tests[47])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, baseBlockInfo.ID, test.act, addPartitionArgs, &cancelState)
	c.Check(checkErr, IsNil)
	baseBlock = testGetBlock(c, d, dbInfo.ID, baseBlockInfo.ID)
	c.Assert(len(baseBlock.Meta().Partition.Definitions), Equals, 1)

	uFIDelateTest(&tests[48])
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, baseBlockInfo.ID, test.act, addPartitionArgs)
	c.Check(checkErr, IsNil)
	baseBlock = testGetBlock(c, d, dbInfo.ID, baseBlockInfo.ID)
	c.Assert(len(baseBlock.Meta().Partition.Definitions), Equals, 2)
	c.Assert(baseBlock.Meta().Partition.Definitions[1].ID, Equals, addedPartInfo.Definitions[0].ID)
	c.Assert(baseBlock.Meta().Partition.Definitions[1].LessThan[0], Equals, addedPartInfo.Definitions[0].LessThan[0])

	// Cancel modify defCausumn which should reorg the data.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/skipMockContextDoExec", `return(true)`), IsNil)
	baseBlockInfo = testBlockInfoWith2IndexOnFirstDeferredCauset(c, d, "modify-block", 2)
	// This will cost 2 global id, one for block id, the other for the job id.
	testCreateBlock(c, ctx, d, dbInfo, baseBlockInfo)

	cancelState = perceptron.StateNone
	newDefCaus := baseBlockInfo.DeferredCausets[0].Clone()
	// change type from long to tinyint.
	newDefCaus.FieldType = *types.NewFieldType(allegrosql.TypeTiny)
	// change from null to not null
	newDefCaus.FieldType.Flag |= allegrosql.NotNullFlag
	newDefCaus.FieldType.Flen = 2

	originDefCausName := baseBlockInfo.DeferredCausets[0].Name
	pos := &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}

	uFIDelateTest(&tests[49])
	modifyDeferredCausetArgs = []interface{}{&newDefCaus, originDefCausName, pos, allegrosql.TypeNull, 0}
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, baseBlockInfo.ID, test.act, modifyDeferredCausetArgs, &cancelState)
	c.Check(checkErr, IsNil)
	baseBlock = testGetBlock(c, d, dbInfo.ID, baseBlockInfo.ID)
	c.Assert(baseBlock.Meta().DeferredCausets[0].FieldType.Tp, Equals, allegrosql.TypeLong)
	c.Assert(allegrosql.HasNotNullFlag(baseBlock.Meta().DeferredCausets[0].FieldType.Flag), Equals, false)

	uFIDelateTest(&tests[50])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, baseBlockInfo.ID, test.act, modifyDeferredCausetArgs, &cancelState)
	c.Check(checkErr, IsNil)
	baseBlock = testGetBlock(c, d, dbInfo.ID, baseBlockInfo.ID)
	c.Assert(baseBlock.Meta().DeferredCausets[0].FieldType.Tp, Equals, allegrosql.TypeLong)
	c.Assert(baseBlock.Meta().DeferredCausets[0].FieldType.Flag&allegrosql.NotNullFlag, Equals, uint(0))

	uFIDelateTest(&tests[51])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, baseBlockInfo.ID, test.act, modifyDeferredCausetArgs, &cancelState)
	c.Check(checkErr, IsNil)
	baseBlock = testGetBlock(c, d, dbInfo.ID, baseBlockInfo.ID)
	c.Assert(baseBlock.Meta().DeferredCausets[0].FieldType.Tp, Equals, allegrosql.TypeLong)
	c.Assert(baseBlock.Meta().DeferredCausets[0].FieldType.Flag&allegrosql.NotNullFlag, Equals, uint(0))

	uFIDelateTest(&tests[52])
	doDBSJobErrWithSchemaState(ctx, d, c, dbInfo.ID, baseBlockInfo.ID, test.act, modifyDeferredCausetArgs, &cancelState)
	c.Check(checkErr, IsNil)
	baseBlock = testGetBlock(c, d, dbInfo.ID, baseBlockInfo.ID)
	c.Assert(baseBlock.Meta().DeferredCausets[0].FieldType.Tp, Equals, allegrosql.TypeLong)
	c.Assert(baseBlock.Meta().DeferredCausets[0].FieldType.Flag&allegrosql.NotNullFlag, Equals, uint(0))

	uFIDelateTest(&tests[53])
	doDBSJobSuccess(ctx, d, c, dbInfo.ID, baseBlockInfo.ID, test.act, modifyDeferredCausetArgs)
	c.Check(checkErr, IsNil)
	baseBlock = testGetBlock(c, d, dbInfo.ID, baseBlockInfo.ID)
	c.Assert(baseBlock.Meta().DeferredCausets[0].FieldType.Tp, Equals, allegrosql.TypeTiny)
	c.Assert(baseBlock.Meta().DeferredCausets[0].FieldType.Flag&allegrosql.NotNullFlag, Equals, uint(1))
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/skipMockContextDoExec"), IsNil)
}

func (s *testDBSSuite) TestIgnorableSpec(c *C) {
	specs := []ast.AlterBlockType{
		ast.AlterBlockOption,
		ast.AlterBlockAddDeferredCausets,
		ast.AlterBlockAddConstraint,
		ast.AlterBlockDropDeferredCauset,
		ast.AlterBlockDropPrimaryKey,
		ast.AlterBlockDropIndex,
		ast.AlterBlockDropForeignKey,
		ast.AlterBlockModifyDeferredCauset,
		ast.AlterBlockChangeDeferredCauset,
		ast.AlterBlockRenameBlock,
		ast.AlterBlockAlterDeferredCauset,
	}
	for _, spec := range specs {
		c.Assert(isIgnorableSpec(spec), IsFalse)
	}

	ignorableSpecs := []ast.AlterBlockType{
		ast.AlterBlockLock,
		ast.AlterBlockAlgorithm,
	}
	for _, spec := range ignorableSpecs {
		c.Assert(isIgnorableSpec(spec), IsTrue)
	}
}

func (s *testDBSSuite) TestBuildJobDependence(c *C) {
	causetstore := testCreateStore(c, "test_set_job_relation")
	defer causetstore.Close()

	// Add some non-add-index jobs.
	job1 := &perceptron.Job{ID: 1, BlockID: 1, Type: perceptron.CausetActionAddDeferredCauset}
	job2 := &perceptron.Job{ID: 2, BlockID: 1, Type: perceptron.CausetActionCreateBlock}
	job3 := &perceptron.Job{ID: 3, BlockID: 2, Type: perceptron.CausetActionDropDeferredCauset}
	job6 := &perceptron.Job{ID: 6, BlockID: 1, Type: perceptron.CausetActionDropBlock}
	job7 := &perceptron.Job{ID: 7, BlockID: 2, Type: perceptron.CausetActionModifyDeferredCauset}
	job9 := &perceptron.Job{ID: 9, SchemaID: 111, Type: perceptron.CausetActionDropSchema}
	job11 := &perceptron.Job{ID: 11, BlockID: 2, Type: perceptron.CausetActionRenameBlock, Args: []interface{}{int64(111), "old EDB name"}}
	ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.EnQueueDBSJob(job1)
		c.Assert(err, IsNil)
		err = t.EnQueueDBSJob(job2)
		c.Assert(err, IsNil)
		err = t.EnQueueDBSJob(job3)
		c.Assert(err, IsNil)
		err = t.EnQueueDBSJob(job6)
		c.Assert(err, IsNil)
		err = t.EnQueueDBSJob(job7)
		c.Assert(err, IsNil)
		err = t.EnQueueDBSJob(job9)
		c.Assert(err, IsNil)
		err = t.EnQueueDBSJob(job11)
		c.Assert(err, IsNil)
		return nil
	})
	job4 := &perceptron.Job{ID: 4, BlockID: 1, Type: perceptron.CausetActionAddIndex}
	ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job4)
		c.Assert(err, IsNil)
		c.Assert(job4.DependencyID, Equals, int64(2))
		return nil
	})
	job5 := &perceptron.Job{ID: 5, BlockID: 2, Type: perceptron.CausetActionAddIndex}
	ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job5)
		c.Assert(err, IsNil)
		c.Assert(job5.DependencyID, Equals, int64(3))
		return nil
	})
	job8 := &perceptron.Job{ID: 8, BlockID: 3, Type: perceptron.CausetActionAddIndex}
	ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job8)
		c.Assert(err, IsNil)
		c.Assert(job8.DependencyID, Equals, int64(0))
		return nil
	})
	job10 := &perceptron.Job{ID: 10, SchemaID: 111, BlockID: 3, Type: perceptron.CausetActionAddIndex}
	ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job10)
		c.Assert(err, IsNil)
		c.Assert(job10.DependencyID, Equals, int64(9))
		return nil
	})
	job12 := &perceptron.Job{ID: 12, SchemaID: 112, BlockID: 2, Type: perceptron.CausetActionAddIndex}
	ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job12)
		c.Assert(err, IsNil)
		c.Assert(job12.DependencyID, Equals, int64(11))
		return nil
	})
}

func addDBSJob(c *C, d *dbs, job *perceptron.Job) {
	task := &limitJobTask{job, make(chan error)}
	d.limitJobCh <- task
	err := <-task.err
	c.Assert(err, IsNil)
}

func (s *testDBSSuite) TestParallelDBS(c *C) {
	causetstore := testCreateStore(c, "test_parallel_dbs")
	defer causetstore.Close()
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	/*
		build structure:
			DBs -> {
			 db1: test_parallel_dbs_1
			 db2: test_parallel_dbs_2
			}
			Blocks -> {
			 db1.t1 (c1 int, c2 int)
			 db1.t2 (c1 int primary key, c2 int, c3 int)
			 db2.t3 (c1 int, c2 int, c3 int, c4 int)
			}
			Data -> {
			 t1: (10, 10), (20, 20)
			 t2: (1, 1, 1), (2, 2, 2), (3, 3, 3)
			 t3: (11, 22, 33, 44)
			}
	*/
	// create database test_parallel_dbs_1;
	dbInfo1 := testSchemaInfo(c, d, "test_parallel_dbs_1")
	testCreateSchema(c, ctx, d, dbInfo1)
	// create block t1 (c1 int, c2 int);
	tblInfo1 := testBlockInfo(c, d, "t1", 2)
	testCreateBlock(c, ctx, d, dbInfo1, tblInfo1)
	// insert t1 values (10, 10), (20, 20)
	tbl1 := testGetBlock(c, d, dbInfo1.ID, tblInfo1.ID)
	_, err = tbl1.AddRecord(ctx, types.MakeCausets(1, 1))
	c.Assert(err, IsNil)
	_, err = tbl1.AddRecord(ctx, types.MakeCausets(2, 2))
	c.Assert(err, IsNil)
	// create block t2 (c1 int primary key, c2 int, c3 int);
	tblInfo2 := testBlockInfo(c, d, "t2", 3)
	tblInfo2.DeferredCausets[0].Flag = allegrosql.PriKeyFlag | allegrosql.NotNullFlag
	tblInfo2.PKIsHandle = true
	testCreateBlock(c, ctx, d, dbInfo1, tblInfo2)
	// insert t2 values (1, 1), (2, 2), (3, 3)
	tbl2 := testGetBlock(c, d, dbInfo1.ID, tblInfo2.ID)
	_, err = tbl2.AddRecord(ctx, types.MakeCausets(1, 1, 1))
	c.Assert(err, IsNil)
	_, err = tbl2.AddRecord(ctx, types.MakeCausets(2, 2, 2))
	c.Assert(err, IsNil)
	_, err = tbl2.AddRecord(ctx, types.MakeCausets(3, 3, 3))
	c.Assert(err, IsNil)
	// create database test_parallel_dbs_2;
	dbInfo2 := testSchemaInfo(c, d, "test_parallel_dbs_2")
	testCreateSchema(c, ctx, d, dbInfo2)
	// create block t3 (c1 int, c2 int, c3 int, c4 int);
	tblInfo3 := testBlockInfo(c, d, "t3", 4)
	testCreateBlock(c, ctx, d, dbInfo2, tblInfo3)
	// insert t3 values (11, 22, 33, 44)
	tbl3 := testGetBlock(c, d, dbInfo2.ID, tblInfo3.ID)
	_, err = tbl3.AddRecord(ctx, types.MakeCausets(11, 22, 33, 44))
	c.Assert(err, IsNil)

	// set hook to execute jobs after all jobs are in queue.
	jobCnt := int64(11)
	tc := &TestDBSCallback{}
	once := sync.Once{}
	var checkErr error
	tc.onJobRunBefore = func(job *perceptron.Job) {
		// TODO: extract a unified function for other tests.
		once.Do(func() {
			qLen1 := int64(0)
			qLen2 := int64(0)
			for {
				checkErr = ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
					m := meta.NewMeta(txn)
					qLen1, err = m.DBSJobQueueLen()
					if err != nil {
						return err
					}
					qLen2, err = m.DBSJobQueueLen(meta.AddIndexJobListKey)
					if err != nil {
						return err
					}
					return nil
				})
				if checkErr != nil {
					break
				}
				if qLen1+qLen2 == jobCnt {
					if qLen2 != 5 {
						checkErr = errors.Errorf("add index jobs cnt %v != 5", qLen2)
					}
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
	d.SetHook(tc)
	c.Assert(checkErr, IsNil)

	/*
		prepare jobs:
		/	job no.	/	database no.	/	block no.	/	action type	 /
		/     1		/	 	1			/		1		/	add index	 /
		/     2		/	 	1			/		1		/	add defCausumn	 /
		/     3		/	 	1			/		1		/	add index	 /
		/     4		/	 	1			/		2		/	drop defCausumn	 /
		/     5		/	 	1			/		1		/	drop index 	 /
		/     6		/	 	1			/		2		/	add index	 /
		/     7		/	 	2			/		3		/	drop defCausumn	 /
		/     8		/	 	2			/		3		/	rebase autoID/
		/     9		/	 	1			/		1		/	add index	 /
		/     10	/	 	2			/		null   	/	drop schemaReplicant  /
		/     11	/	 	2			/		2		/	add index	 /
	*/
	job1 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx1", "c1")
	addDBSJob(c, d, job1)
	job2 := buildCreateDeferredCausetJob(dbInfo1, tblInfo1, "c3", &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}, nil)
	addDBSJob(c, d, job2)
	job3 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx2", "c3")
	addDBSJob(c, d, job3)
	job4 := buildDropDeferredCausetJob(dbInfo1, tblInfo2, "c3")
	addDBSJob(c, d, job4)
	job5 := buildDropIdxJob(dbInfo1, tblInfo1, "db1_idx1")
	addDBSJob(c, d, job5)
	job6 := buildCreateIdxJob(dbInfo1, tblInfo2, false, "db2_idx1", "c2")
	addDBSJob(c, d, job6)
	job7 := buildDropDeferredCausetJob(dbInfo2, tblInfo3, "c4")
	addDBSJob(c, d, job7)
	job8 := buildRebaseAutoIDJobJob(dbInfo2, tblInfo3, 1024)
	addDBSJob(c, d, job8)
	job9 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx3", "c2")
	addDBSJob(c, d, job9)
	job10 := buildDropSchemaJob(dbInfo2)
	addDBSJob(c, d, job10)
	job11 := buildCreateIdxJob(dbInfo2, tblInfo3, false, "db3_idx1", "c2")
	addDBSJob(c, d, job11)
	// TODO: add rename block job

	// check results.
	isChecked := false
	for !isChecked {
		ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
			m := meta.NewMeta(txn)
			lastJob, err := m.GetHistoryDBSJob(job11.ID)
			c.Assert(err, IsNil)
			// all jobs are finished.
			if lastJob != nil {
				finishedJobs, err := m.GetAllHistoryDBSJobs()
				c.Assert(err, IsNil)
				// get the last 11 jobs completed.
				finishedJobs = finishedJobs[len(finishedJobs)-11:]
				// check some jobs are ordered because of the dependence.
				c.Assert(finishedJobs[0].ID, Equals, job1.ID)
				c.Assert(finishedJobs[1].ID, Equals, job2.ID)
				c.Assert(finishedJobs[2].ID, Equals, job3.ID)
				c.Assert(finishedJobs[4].ID, Equals, job5.ID)
				c.Assert(finishedJobs[10].ID, Equals, job11.ID)
				// check the jobs are ordered in the adding-index-job queue or general-job queue.
				addIdxJobID := int64(0)
				generalJobID := int64(0)
				for _, job := range finishedJobs {
					// check jobs' order.
					if job.Type == perceptron.CausetActionAddIndex {
						c.Assert(job.ID, Greater, addIdxJobID)
						addIdxJobID = job.ID
					} else {
						c.Assert(job.ID, Greater, generalJobID)
						generalJobID = job.ID
					}
					// check jobs' state.
					if job.ID == lastJob.ID {
						c.Assert(job.State, Equals, perceptron.JobStateCancelled, Commentf("job: %v", job))
					} else {
						c.Assert(job.State, Equals, perceptron.JobStateSynced, Commentf("job: %v", job))
					}
				}

				isChecked = true
			}
			return nil
		})
		time.Sleep(10 * time.Millisecond)
	}

	tc = &TestDBSCallback{}
	d.SetHook(tc)
}

func (s *testDBSSuite) TestDBSPackageExecuteALLEGROSQL(c *C) {
	causetstore := testCreateStore(c, "test_run_sql")
	defer causetstore.Close()

	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	testCheckOwner(c, d, true)
	defer d.Stop()
	worker := d.generalWorker()
	c.Assert(worker, NotNil)

	// In test environment, worker.ctxPool will be nil, and get will return mock.Context.
	// We just test that can use it to call sqlexec.ALLEGROSQLExecutor.Execute.
	sess, err := worker.sessPool.get()
	c.Assert(err, IsNil)
	defer worker.sessPool.put(sess)
	se := sess.(sqlexec.ALLEGROSQLExecutor)
	_, _ = se.Execute(context.Background(), "create block t(a int);")
}
