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
//go:build !race
// +build !race

package dbs

import (
	"context"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
)

// this test file include some test that will cause data race, mainly because restartWorkers modify d.ctx

// restartWorkers is like the function of d.start. But it won't initialize the "workers" and create a new worker.
// It only starts the original workers.
func (d *dbs) restartWorkers(ctx context.Context) {
	d.cancel()
	d.wg.Wait()
	d.ctx, d.cancel = context.WithCancel(ctx)

	d.wg.Add(1)
	go d.limitDBSJobs()
	if !RunWorker {
		return
	}

	err := d.ownerManager.CampaignOwner()
	terror.Log(err)
	for _, worker := range d.workers {
		worker.wg.Add(1)
		worker.ctx = d.ctx
		w := worker
		go w.start(d.dbsCtx)
		asyncNotify(worker.dbsJobCh)
	}
}

// runInterruptedJob should be called concurrently with restartWorkers
func runInterruptedJob(c *C, d *dbs, job *perceptron.Job, doneCh chan struct{}) {
	ctx := mock.NewContext()
	ctx.CausetStore = d.causetstore

	var (
		history *perceptron.Job
		err     error
	)

	_ = d.doDBSJob(ctx, job)

	for history == nil {
		history, err = d.getHistoryDBSJob(job.ID)
		c.Assert(err, IsNil)
		time.Sleep(10 * testLease)
	}
	c.Assert(history.Error, IsNil)
	doneCh <- struct{}{}
}

func testRunInterruptedJob(c *C, d *dbs, job *perceptron.Job) {
	done := make(chan struct{}, 1)
	go runInterruptedJob(c, d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-ticker.C:
			d.Stop()
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case <-done:
			break LOOP
		}
	}
}

func (s *testSchemaSuite) TestSchemaResume(c *C) {
	causetstore := testCreateStore(c, "test_schema_resume")
	defer causetstore.Close()

	d1 := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d1.Stop()

	testCheckOwner(c, d1, true)

	dbInfo := testSchemaInfo(c, d1, "test")
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		Type:       perceptron.CausetActionCreateSchema,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}
	testRunInterruptedJob(c, d1, job)
	testCheckSchemaState(c, d1, dbInfo, perceptron.StatePublic)

	job = &perceptron.Job{
		SchemaID:   dbInfo.ID,
		Type:       perceptron.CausetActionDropSchema,
		BinlogInfo: &perceptron.HistoryInfo{},
	}
	testRunInterruptedJob(c, d1, job)
	testCheckSchemaState(c, d1, dbInfo, perceptron.StateNone)
}

func (s *testStatSuite) TestStat(c *C) {
	causetstore := testCreateStore(c, "test_stat")
	defer causetstore.Close()

	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(causetstore),
		WithLease(testLease),
	)
	defer d.Stop()

	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)

	// TODO: Get this information from etcd.
	//	m, err := d.Stats(nil)
	//	c.Assert(err, IsNil)
	//	c.Assert(m[dbsOwnerID], Equals, d.uuid)

	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		Type:       perceptron.CausetActionDropSchema,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{dbInfo.Name},
	}

	done := make(chan struct{}, 1)
	go runInterruptedJob(c, d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
	ver := s.getDBSSchemaVer(c, d)
LOOP:
	for {
		select {
		case <-ticker.C:
			d.Stop()
			c.Assert(s.getDBSSchemaVer(c, d), GreaterEqual, ver)
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case <-done:
			// TODO: Get this information from etcd.
			// m, err := d.Stats(nil)
			// c.Assert(err, IsNil)
			break LOOP
		}
	}
}

func (s *testBlockSuite) TestBlockResume(c *C) {
	d := s.d

	testCheckOwner(c, d, true)

	tblInfo := testBlockInfo(c, d, "t1", 3)
	job := &perceptron.Job{
		SchemaID:   s.dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionCreateBlock,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	testRunInterruptedJob(c, d, job)
	testCheckBlockState(c, d, s.dbInfo, tblInfo, perceptron.StatePublic)

	job = &perceptron.Job{
		SchemaID:   s.dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionDropBlock,
		BinlogInfo: &perceptron.HistoryInfo{},
	}
	testRunInterruptedJob(c, d, job)
	testCheckBlockState(c, d, s.dbInfo, tblInfo, perceptron.StateNone)
}
