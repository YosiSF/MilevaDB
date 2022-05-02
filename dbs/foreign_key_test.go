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
	"strings"
	"sync"

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
)

var _ = Suite(&testForeignKeySuite{})

type testForeignKeySuite struct {
	causetstore ekv.CausetStorage
	dbInfo      *perceptron.DBInfo
	d           *dbs
	ctx         stochastikctx.Context
}

func (s *testForeignKeySuite) SetUpSuite(c *C) {
	s.causetstore = testCreateStore(c, "test_foreign")
}

func (s *testForeignKeySuite) TearDownSuite(c *C) {
	err := s.causetstore.Close()
	c.Assert(err, IsNil)
}

func (s *testForeignKeySuite) testCreateForeignKey(c *C, tblInfo *perceptron.BlockInfo, fkName string, keys []string, refBlock string, refKeys []string, onDelete ast.ReferOptionType, onUFIDelate ast.ReferOptionType) *perceptron.Job {
	FKName := perceptron.NewCIStr(fkName)
	Keys := make([]perceptron.CIStr, len(keys))
	for i, key := range keys {
		Keys[i] = perceptron.NewCIStr(key)
	}

	RefBlock := perceptron.NewCIStr(refBlock)
	RefKeys := make([]perceptron.CIStr, len(refKeys))
	for i, key := range refKeys {
		RefKeys[i] = perceptron.NewCIStr(key)
	}

	fkInfo := &perceptron.FKInfo{
		Name:        FKName,
		RefBlock:    RefBlock,
		RefDefCauss: RefKeys,
		DefCauss:    Keys,
		OnDelete:    int(onDelete),
		OnUFIDelate: int(onUFIDelate),
		State:       perceptron.StateNone,
	}

	job := &perceptron.Job{
		SchemaID:   s.dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionAddForeignKey,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{fkInfo},
	}
	err := s.ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	err = s.d.doDBSJob(s.ctx, job)
	c.Assert(err, IsNil)
	return job
}

func testDropForeignKey(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, foreignKeyName string) *perceptron.Job {
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionDropForeignKey,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{perceptron.NewCIStr(foreignKeyName)},
	}
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func getForeignKey(t block.Block, name string) *perceptron.FKInfo {
	for _, fk := range t.Meta().ForeignKeys {
		// only public foreign key can be read.
		if fk.State != perceptron.StatePublic {
			continue
		}
		if fk.Name.L == strings.ToLower(name) {
			return fk
		}
	}
	return nil
}

func (s *testForeignKeySuite) TestForeignKey(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	s.d = d
	s.dbInfo = testSchemaInfo(c, d, "test_foreign")
	ctx := testNewContext(d)
	s.ctx = ctx
	testCreateSchema(c, ctx, d, s.dbInfo)
	tblInfo := testBlockInfo(c, d, "t", 3)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// fix data race
	var mu sync.Mutex
	checkOK := false
	var hookErr error
	tc := &TestDBSCallback{}
	tc.onJobUFIDelated = func(job *perceptron.Job) {
		if job.State != perceptron.JobStateDone {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		var t block.Block
		t, err = testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err != nil {
			hookErr = errors.Trace(err)
			return
		}
		fk := getForeignKey(t, "c1_fk")
		if fk == nil {
			hookErr = errors.New("foreign key not exists")
			return
		}
		checkOK = true
	}
	originalHook := d.GetHook()
	defer d.SetHook(originalHook)
	d.SetHook(tc)

	job := s.testCreateForeignKey(c, tblInfo, "c1_fk", []string{"c1"}, "t2", []string{"c1"}, ast.ReferOptionCascade, ast.ReferOptionSetNull)
	testCheckJobDone(c, d, job, true)
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(hErr, IsNil)
	c.Assert(ok, IsTrue)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})

	mu.Lock()
	checkOK = false
	mu.Unlock()
	// fix data race pr/#9491
	tc2 := &TestDBSCallback{}
	tc2.onJobUFIDelated = func(job *perceptron.Job) {
		if job.State != perceptron.JobStateDone {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		var t block.Block
		t, err = testGetBlockWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err != nil {
			hookErr = errors.Trace(err)
			return
		}
		fk := getForeignKey(t, "c1_fk")
		if fk != nil {
			hookErr = errors.New("foreign key has not been dropped")
			return
		}
		checkOK = true
	}
	d.SetHook(tc2)

	job = testDropForeignKey(c, ctx, d, s.dbInfo, tblInfo, "c1_fk")
	testCheckJobDone(c, d, job, false)
	mu.Lock()
	hErr = hookErr
	ok = checkOK
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
}
