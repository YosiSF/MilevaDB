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

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

func (s *testDeferredCausetChangeSuite) TestFailBeforeDecodeArgs(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	// create block t_fail (c1 int, c2 int);
	tblInfo := testBlockInfo(c, d, "t_fail", 2)
	ctx := testNewContext(d)
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	testCreateBlock(c, ctx, d, s.dbInfo, tblInfo)
	// insert t_fail values (1, 2);
	originBlock := testGetBlock(c, d, s.dbInfo.ID, tblInfo.ID)
	event := types.MakeCausets(1, 2)
	_, err = originBlock.AddRecord(ctx, event)
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	tc := &TestDBSCallback{}
	first := true
	stateCnt := 0
	tc.onJobRunBefore = func(job *perceptron.Job) {
		// It can be other schemaReplicant states except failed schemaReplicant state.
		// This schemaReplicant state can only appear once.
		if job.SchemaState == perceptron.StateWriteOnly {
			stateCnt++
		} else if job.SchemaState == perceptron.StateWriteReorganization {
			if first {
				c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/errorBeforeDecodeArgs", `return(true)`), IsNil)
				first = false
			} else {
				c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/errorBeforeDecodeArgs"), IsNil)
			}
		}
	}
	d.SetHook(tc)
	defaultValue := int64(3)
	job := testCreateDeferredCauset(c, ctx, d, s.dbInfo, tblInfo, "c3", &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}, defaultValue)
	// Make sure the schemaReplicant state only appears once.
	c.Assert(stateCnt, Equals, 1)
	testCheckJobDone(c, d, job, true)
}
