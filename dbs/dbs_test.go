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
	"os"
	"testing"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

type DBSForTest interface {
	// SetHook sets the hook.
	SetHook(h Callback)
	// SetInterceptor sets the interceptor.
	SetInterceptor(h Interceptor)
}

// SetHook implements DBS.SetHook interface.
func (d *dbs) SetHook(h Callback) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.hook = h
}

// SetInterceptor implements DBS.SetInterceptor interface.
func (d *dbs) SetInterceptor(i Interceptor) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.interceptor = i
}

// generalWorker returns the general worker.
func (d *dbs) generalWorker() *worker {
	return d.workers[generalWorker]
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	autoid.SetStep(5000)
	ReorgWaitTimeout = 30 * time.Millisecond
	batchInsertDeleteRangeSize = 2

	config.UFIDelateGlobal(func(conf *config.Config) {
		// Test for block dagger.
		conf.EnableBlockLock = true
		conf.Log.SlowThreshold = 10000
		// Test for add/drop primary key.
		conf.AlterPrimaryKey = true
	})

	_, err := infosync.GlobalInfoSyncerInit(context.Background(), "t", nil, true)
	if err != nil {
		t.Fatal(err)
	}

	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

func testNewDBSAndStart(ctx context.Context, c *C, options ...Option) *dbs {
	d := newDBS(ctx, options...)
	err := d.Start(nil)
	c.Assert(err, IsNil)

	return d
}

func testCreateStore(c *C, name string) ekv.CausetStorage {
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	return causetstore
}

func testNewContext(d *dbs) stochastikctx.Context {
	ctx := mock.NewContext()
	ctx.CausetStore = d.causetstore
	return ctx
}

func getSchemaVer(c *C, ctx stochastikctx.Context) int64 {
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	ver, err := m.GetSchemaVersion()
	c.Assert(err, IsNil)
	return ver
}

type historyJobArgs struct {
	ver    int64
	EDB    *perceptron.DBInfo
	tbl    *perceptron.BlockInfo
	tblIDs map[int64]struct{}
}

func checkEqualBlock(c *C, t1, t2 *perceptron.BlockInfo) {
	c.Assert(t1.ID, Equals, t2.ID)
	c.Assert(t1.Name, Equals, t2.Name)
	c.Assert(t1.Charset, Equals, t2.Charset)
	c.Assert(t1.DefCauslate, Equals, t2.DefCauslate)
	c.Assert(t1.PKIsHandle, DeepEquals, t2.PKIsHandle)
	c.Assert(t1.Comment, DeepEquals, t2.Comment)
	c.Assert(t1.AutoIncID, DeepEquals, t2.AutoIncID)
}

func checkHistoryJob(c *C, job *perceptron.Job) {
	c.Assert(job.State, Equals, perceptron.JobStateSynced)
}

func checkHistoryJobArgs(c *C, ctx stochastikctx.Context, id int64, args *historyJobArgs) {
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	historyJob, err := t.GetHistoryDBSJob(id)
	c.Assert(err, IsNil)
	c.Assert(historyJob.BinlogInfo.FinishedTS, Greater, uint64(0))

	if args.tbl != nil {
		c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
		checkEqualBlock(c, historyJob.BinlogInfo.BlockInfo, args.tbl)
		return
	}

	// for handling schemaReplicant job
	c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
	c.Assert(historyJob.BinlogInfo.DBInfo, DeepEquals, args.EDB)
	// only for creating schemaReplicant job
	if args.EDB != nil && len(args.tblIDs) == 0 {
		return
	}
}

func buildCreateIdxJob(dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, unique bool, indexName string, defCausName string) *perceptron.Job {
	return &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionAddIndex,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args: []interface{}{unique, perceptron.NewCIStr(indexName),
			[]*ast.IndexPartSpecification{{
				DeferredCauset: &ast.DeferredCausetName{Name: perceptron.NewCIStr(defCausName)},
				Length:         types.UnspecifiedLength}}},
	}
}

func testCreatePrimaryKey(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, defCausName string) *perceptron.Job {
	job := buildCreateIdxJob(dbInfo, tblInfo, true, "primary", defCausName)
	job.Type = perceptron.CausetActionAddPrimaryKey
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testCreateIndex(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, unique bool, indexName string, defCausName string) *perceptron.Job {
	job := buildCreateIdxJob(dbInfo, tblInfo, unique, indexName, defCausName)
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testAddDeferredCauset(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, args []interface{}) *perceptron.Job {
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionAddDeferredCauset,
		Args:       args,
		BinlogInfo: &perceptron.HistoryInfo{},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testAddDeferredCausets(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, args []interface{}) *perceptron.Job {
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionAddDeferredCausets,
		Args:       args,
		BinlogInfo: &perceptron.HistoryInfo{},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildDropIdxJob(dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, indexName string) *perceptron.Job {
	tp := perceptron.CausetActionDropIndex
	if indexName == "primary" {
		tp = perceptron.CausetActionDropPrimaryKey
	}
	return &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       tp,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{perceptron.NewCIStr(indexName)},
	}
}

func testDropIndex(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, indexName string) *perceptron.Job {
	job := buildDropIdxJob(dbInfo, tblInfo, indexName)
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildRebaseAutoIDJobJob(dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, newBaseID int64) *perceptron.Job {
	return &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionRebaseAutoID,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{newBaseID},
	}
}
