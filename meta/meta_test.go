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

package meta_test

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	. "github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	CommonHandleSuite
}

func (s *testSuite) TestMeta(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()

	txn, err := causetstore.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	t := meta.NewMeta(txn)

	n, err := t.GenGlobalID()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = t.GetGlobalID()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ids, err := t.GenGlobalIDs(3)
		c.Assert(err, IsNil)
		anyMatch(c, ids, []int64{2, 3, 4}, []int64{6, 7, 8})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ids, err := t.GenGlobalIDs(4)
		c.Assert(err, IsNil)
		anyMatch(c, ids, []int64{5, 6, 7, 8}, []int64{2, 3, 4, 5})
	}()
	wg.Wait()

	n, err = t.GetSchemaVersion()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	n, err = t.GenSchemaVersion()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = t.GetSchemaVersion()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	dbInfo := &perceptron.DBInfo{
		ID:   1,
		Name: perceptron.NewCIStr("a"),
	}
	err = t.CreateDatabase(dbInfo)
	c.Assert(err, IsNil)

	err = t.CreateDatabase(dbInfo)
	c.Assert(err, NotNil)
	c.Assert(meta.ErrDBExists.Equal(err), IsTrue)

	v, err := t.GetDatabase(1)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, dbInfo)

	dbInfo.Name = perceptron.NewCIStr("aa")
	err = t.UFIDelateDatabase(dbInfo)
	c.Assert(err, IsNil)

	v, err = t.GetDatabase(1)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, dbInfo)

	dbs, err := t.ListDatabases()
	c.Assert(err, IsNil)
	c.Assert(dbs, DeepEquals, []*perceptron.DBInfo{dbInfo})

	tbInfo := &perceptron.BlockInfo{
		ID:   1,
		Name: perceptron.NewCIStr("t"),
	}
	err = t.CreateBlockOrView(1, tbInfo)
	c.Assert(err, IsNil)

	n, err = t.GenAutoBlockID(1, 1, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	n, err = t.GetAutoBlockID(1, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	err = t.CreateBlockOrView(1, tbInfo)
	c.Assert(err, NotNil)
	c.Assert(meta.ErrBlockExists.Equal(err), IsTrue)

	tbInfo.Name = perceptron.NewCIStr("tt")
	err = t.UFIDelateBlock(1, tbInfo)
	c.Assert(err, IsNil)

	block, err := t.GetBlock(1, 1)
	c.Assert(err, IsNil)
	c.Assert(block, DeepEquals, tbInfo)

	block, err = t.GetBlock(1, 2)
	c.Assert(err, IsNil)
	c.Assert(block, IsNil)

	tbInfo2 := &perceptron.BlockInfo{
		ID:   2,
		Name: perceptron.NewCIStr("bb"),
	}
	err = t.CreateBlockOrView(1, tbInfo2)
	c.Assert(err, IsNil)

	blocks, err := t.ListBlocks(1)
	c.Assert(err, IsNil)
	c.Assert(blocks, DeepEquals, []*perceptron.BlockInfo{tbInfo, tbInfo2})
	// Generate an auto id.
	n, err = t.GenAutoBlockID(1, 2, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))
	// Make sure the auto id key-value entry is there.
	n, err = t.GetAutoBlockID(1, 2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))

	err = t.DropBlockOrView(1, tbInfo2.ID, true)
	c.Assert(err, IsNil)
	// Make sure auto id key-value entry is gone.
	n, err = t.GetAutoBlockID(1, 2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	blocks, err = t.ListBlocks(1)
	c.Assert(err, IsNil)
	c.Assert(blocks, DeepEquals, []*perceptron.BlockInfo{tbInfo})

	// Test case for drop a block without delete auto id key-value entry.
	tid := int64(100)
	tbInfo100 := &perceptron.BlockInfo{
		ID:   tid,
		Name: perceptron.NewCIStr("t_rename"),
	}
	// Create block.
	err = t.CreateBlockOrView(1, tbInfo100)
	c.Assert(err, IsNil)
	// UFIDelate auto ID.
	currentDBID := int64(1)
	n, err = t.GenAutoBlockID(currentDBID, tid, 10)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(10))
	// Fail to uFIDelate auto ID.
	// The block ID doesn't exist.
	nonExistentID := int64(1234)
	_, err = t.GenAutoBlockID(currentDBID, nonExistentID, 10)
	c.Assert(err, NotNil)
	c.Assert(meta.ErrBlockNotExists.Equal(err), IsTrue)
	// Fail to uFIDelate auto ID.
	// The current database ID doesn't exist.
	currentDBID = nonExistentID
	_, err = t.GenAutoBlockID(currentDBID, tid, 10)
	c.Assert(err, NotNil)
	c.Assert(meta.ErrDBNotExists.Equal(err), IsTrue)
	// Test case for CreateBlockAndSetAutoID.
	tbInfo3 := &perceptron.BlockInfo{
		ID:   3,
		Name: perceptron.NewCIStr("tbl3"),
	}
	err = t.CreateBlockAndSetAutoID(1, tbInfo3, 123, 0)
	c.Assert(err, IsNil)
	id, err := t.GetAutoBlockID(1, tbInfo3.ID)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int64(123))
	// Test case for GenAutoBlockIDKeyValue.
	key, val := t.GenAutoBlockIDKeyValue(1, tbInfo3.ID, 1234)
	c.Assert(val, DeepEquals, []byte(strconv.FormatInt(1234, 10)))
	c.Assert(key, DeepEquals, []byte{0x6d, 0x44, 0x42, 0x3a, 0x31, 0x0, 0x0, 0x0, 0x0, 0xfb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x33, 0x0, 0x0, 0x0, 0xfc})

	err = t.DroFIDelatabase(1)
	c.Assert(err, IsNil)
	err = t.DroFIDelatabase(currentDBID)
	c.Assert(err, IsNil)

	dbs, err = t.ListDatabases()
	c.Assert(err, IsNil)
	c.Assert(dbs, HasLen, 0)

	bootstrapVer, err := t.GetBootstrapVersion()
	c.Assert(err, IsNil)
	c.Assert(bootstrapVer, Equals, int64(0))

	err = t.FinishBootstrap(int64(1))
	c.Assert(err, IsNil)

	bootstrapVer, err = t.GetBootstrapVersion()
	c.Assert(err, IsNil)
	c.Assert(bootstrapVer, Equals, int64(1))

	// Test case for meta.FinishBootstrap with a version.
	err = t.FinishBootstrap(int64(10))
	c.Assert(err, IsNil)
	bootstrapVer, err = t.GetBootstrapVersion()
	c.Assert(err, IsNil)
	c.Assert(bootstrapVer, Equals, int64(10))

	// Test case for SchemaDiff.
	schemaDiff := &perceptron.SchemaDiff{
		Version:    100,
		SchemaID:   1,
		Type:       perceptron.CausetActionTruncateBlock,
		BlockID:    2,
		OldBlockID: 3,
	}
	err = t.SetSchemaDiff(schemaDiff)
	c.Assert(err, IsNil)
	readDiff, err := t.GetSchemaDiff(schemaDiff.Version)
	c.Assert(err, IsNil)
	c.Assert(readDiff, DeepEquals, schemaDiff)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Test for DBSJobHistoryKey.
	key = meta.DBSJobHistoryKey(t, 888)
	c.Assert(key, DeepEquals, []byte{0x6d, 0x44, 0x44, 0x4c, 0x4a, 0x6f, 0x62, 0x48, 0x69, 0xff, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x78, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7})
}

func (s *testSuite) TestSnapshot(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()

	txn, _ := causetstore.Begin()
	m := meta.NewMeta(txn)
	m.GenGlobalID()
	n, _ := m.GetGlobalID()
	c.Assert(n, Equals, int64(1))
	txn.Commit(context.Background())

	ver1, _ := causetstore.CurrentVersion()
	time.Sleep(time.Millisecond)
	txn, _ = causetstore.Begin()
	m = meta.NewMeta(txn)
	m.GenGlobalID()
	n, _ = m.GetGlobalID()
	c.Assert(n, Equals, int64(2))
	txn.Commit(context.Background())

	snapshot, _ := causetstore.GetSnapshot(ver1)
	snapMeta := meta.NewSnapshotMeta(snapshot)
	n, _ = snapMeta.GetGlobalID()
	c.Assert(n, Equals, int64(1))
	_, err = snapMeta.GenGlobalID()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[structure:8220]write on snapshot")
}

func (s *testSuite) TestDBS(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()

	txn, err := causetstore.Begin()
	c.Assert(err, IsNil)

	defer txn.Rollback()

	t := meta.NewMeta(txn)

	job := &perceptron.Job{ID: 1}
	err = t.EnQueueDBSJob(job)
	c.Assert(err, IsNil)
	n, err := t.DBSJobQueueLen()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	v, err := t.GetDBSJobByIdx(0)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)
	v, err = t.GetDBSJobByIdx(1)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
	job.ID = 2
	err = t.UFIDelateDBSJob(0, job, true)
	c.Assert(err, IsNil)

	// There are 3 meta key relate to index reorganization:
	// start_handle, end_handle and physical_block_id.
	// Only start_handle is initialized.
	err = t.UFIDelateDBSReorgStartHandle(job, ekv.IntHandle(1))
	c.Assert(err, IsNil)

	// Since physical_block_id is uninitialized, we simulate older MilevaDB version that doesn't causetstore them.
	// In this case GetDBSReorgHandle always return maxInt64 as end_handle.
	i, j, k, err := t.GetDBSReorgHandle(job, false)
	c.Assert(err, IsNil)
	c.Assert(i, HandleEquals, ekv.IntHandle(1))
	c.Assert(j, HandleEquals, ekv.IntHandle(math.MaxInt64))
	c.Assert(k, Equals, int64(0))

	startHandle := s.NewHandle().Int(1).Common("abc", 1222, "string")
	endHandle := s.NewHandle().Int(2).Common("dddd", 1222, "string")
	err = t.UFIDelateDBSReorgHandle(job, startHandle, endHandle, 3)
	c.Assert(err, IsNil)

	i, j, k, err = t.GetDBSReorgHandle(job, s.IsCommonHandle)
	c.Assert(err, IsNil)
	c.Assert(i, HandleEquals, startHandle)
	c.Assert(j, HandleEquals, endHandle)
	c.Assert(k, Equals, int64(3))

	err = t.RemoveDBSReorgHandle(job)
	c.Assert(err, IsNil)

	// new MilevaDB binary running on old MilevaDB DBS reorg data.
	i, j, k, err = t.GetDBSReorgHandle(job, s.IsCommonHandle)
	c.Assert(err, IsNil)
	c.Assert(i, IsNil)
	// The default value for endHandle is MaxInt64, not 0.
	c.Assert(j, HandleEquals, ekv.IntHandle(math.MaxInt64))
	c.Assert(k, Equals, int64(0))

	// Test GetDBSReorgHandle failed.
	_, _, _, err = t.GetDBSReorgHandle(job, s.IsCommonHandle)
	c.Assert(err, IsNil)

	v, err = t.DeQueueDBSJob()
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)

	err = t.AddHistoryDBSJob(job, true)
	c.Assert(err, IsNil)
	v, err = t.GetHistoryDBSJob(2)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)

	// Add multiple history jobs.
	arg := "test arg"
	historyJob1 := &perceptron.Job{ID: 1234}
	historyJob1.Args = append(job.Args, arg)
	err = t.AddHistoryDBSJob(historyJob1, true)
	c.Assert(err, IsNil)
	historyJob2 := &perceptron.Job{ID: 123}
	historyJob2.Args = append(job.Args, arg)
	err = t.AddHistoryDBSJob(historyJob2, false)
	c.Assert(err, IsNil)
	all, err := t.GetAllHistoryDBSJobs()
	c.Assert(err, IsNil)
	var lastID int64
	for _, job := range all {
		c.Assert(job.ID, Greater, lastID)
		lastID = job.ID
		arg1 := ""
		job.DecodeArgs(&arg1)
		if job.ID == historyJob1.ID {
			c.Assert(*(job.Args[0].(*string)), Equals, historyJob1.Args[0])
		} else {
			c.Assert(job.Args, HasLen, 0)
		}
	}

	// Test for get last N history dbs jobs.
	historyJobs, err := t.GetLastNHistoryDBSJobs(2)
	c.Assert(err, IsNil)
	c.Assert(len(historyJobs), Equals, 2)
	c.Assert(historyJobs[0].ID == 1234, IsTrue)
	c.Assert(historyJobs[1].ID == 123, IsTrue)

	// Test GetAllDBSJobsInQueue.
	err = t.EnQueueDBSJob(job)
	c.Assert(err, IsNil)
	job1 := &perceptron.Job{ID: 2}
	err = t.EnQueueDBSJob(job1)
	c.Assert(err, IsNil)
	jobs, err := t.GetAllDBSJobsInQueue()
	c.Assert(err, IsNil)
	expectJobs := []*perceptron.Job{job, job1}
	c.Assert(jobs, DeepEquals, expectJobs)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Test for add index job.
	txn1, err := causetstore.Begin()
	c.Assert(err, IsNil)
	defer txn1.Rollback()

	m := meta.NewMeta(txn1, meta.AddIndexJobListKey)
	err = m.EnQueueDBSJob(job)
	c.Assert(err, IsNil)
	job.ID = 123
	err = m.UFIDelateDBSJob(0, job, true, meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	v, err = m.GetDBSJobByIdx(0, meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, job)
	l, err := m.DBSJobQueueLen(meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(1))
	jobs, err = m.GetAllDBSJobsInQueue(meta.AddIndexJobListKey)
	c.Assert(err, IsNil)
	expectJobs = []*perceptron.Job{job}
	c.Assert(jobs, DeepEquals, expectJobs)

	err = txn1.Commit(context.Background())
	c.Assert(err, IsNil)

	s.RerunWithCommonHandleEnabled(c, s.TestDBS)
}

func (s *testSuite) BenchmarkGenGlobalIDs(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()

	txn, err := causetstore.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	t := meta.NewMeta(txn)

	c.ResetTimer()
	var ids []int64
	for i := 0; i < c.N; i++ {
		ids, _ = t.GenGlobalIDs(10)
	}
	c.Assert(ids, HasLen, 10)
	c.Assert(ids[9], Equals, int64(c.N)*10)
}

func (s *testSuite) BenchmarkGenGlobalIDOneByOne(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()

	txn, err := causetstore.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	t := meta.NewMeta(txn)

	c.ResetTimer()
	var id int64
	for i := 0; i < c.N; i++ {
		for j := 0; j < 10; j++ {
			id, _ = t.GenGlobalID()
		}
	}
	c.Assert(id, Equals, int64(c.N)*10)
}

func anyMatch(c *C, ids []int64, candidates ...[]int64) {
	var match bool
OUTER:
	for _, cand := range candidates {
		if len(ids) != len(cand) {
			continue
		}
		for i, v := range cand {
			if ids[i] != v {
				continue OUTER
			}
		}
		match = true
		break
	}
	c.Assert(match, IsTrue)
}

func mustNewCommonHandle(c *C, values ...interface{}) *ekv.CommonHandle {
	encoded, err := codec.EncodeKey(new(stmtctx.StatementContext), nil, types.MakeCausets(values...)...)
	c.Assert(err, IsNil)
	ch, err := ekv.NewCommonHandle(encoded)
	c.Assert(err, IsNil)
	return ch
}
