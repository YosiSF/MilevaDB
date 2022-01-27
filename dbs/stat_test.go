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

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testStatSuite{})
var _ = SerialSuites(&testSerialStatSuite{})

type testStatSuite struct {
}

func (s *testStatSuite) SetUpSuite(c *C) {
}

func (s *testStatSuite) TearDownSuite(c *C) {
}

type testSerialStatSuite struct {
}

func (s *testStatSuite) getDBSSchemaVer(c *C, d *dbs) int64 {
	m, err := d.Stats(nil)
	c.Assert(err, IsNil)
	v := m[dbsSchemaVersion]
	return v.(int64)
}

func (s *testSerialStatSuite) TestDBSStatsInfo(c *C) {
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
	tblInfo := testBlockInfo(c, d, "t", 2)
	ctx := testNewContext(d)
	testCreateBlock(c, ctx, d, dbInfo, tblInfo)

	t := testGetBlock(c, d, dbInfo.ID, tblInfo.ID)
	// insert t values (1, 1), (2, 2), (3, 3)
	_, err := t.AddRecord(ctx, types.MakeCausets(1, 1))
	c.Assert(err, IsNil)
	_, err = t.AddRecord(ctx, types.MakeCausets(2, 2))
	c.Assert(err, IsNil)
	_, err = t.AddRecord(ctx, types.MakeCausets(3, 3))
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	job := buildCreateIdxJob(dbInfo, tblInfo, true, "idx", "c1")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/checkBackfillWorkerNum", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/checkBackfillWorkerNum"), IsNil)
	}()

	done := make(chan error, 1)
	go func() {
		done <- d.doDBSJob(ctx, job)
	}()

	exit := false
	for !exit {
		select {
		case err := <-done:
			c.Assert(err, IsNil)
			exit = true
		case <-TestCheckWorkerNumCh:
			varMap, err := d.Stats(nil)
			c.Assert(err, IsNil)
			c.Assert(varMap[dbsJobReorgHandle], Equals, "1")
		}
	}
}
