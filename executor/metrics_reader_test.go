// INTERLOCKyright 2020-present WHTCORPS INC, Inc.
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

package executor_test

import (
	"context"
	"fmt"

	"github.com/whtcorpsinc/MilevaDB-Prod/executor"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	. "github.com/whtcorpsinc/check"
)

func (s *testSuite7) TestStmtLabel(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block label (c1 int primary key, c2 int, c3 int, index (c2))")
	for i := 0; i < 10; i++ {
		allegrosql := fmt.Sprintf("insert into label values (%d, %d, %d)", i, i, i)
		tk.MustExec(allegrosql)
	}
	tk.MustExec("analyze block label")

	tests := []struct {
		allegrosql string
		label      string
	}{
		{"select 1", "Select"},
		{"select * from label t1, label t2", "Select"},
		{"select * from label t1 where t1.c3 > (select count(t1.c1 = t2.c1) = 0 from label t2)", "Select"},
		{"select count(*) from label", "Select"},
		{"select * from label where c2 = 1", "Select"},
		{"select c1, c2 from label where c2 = 1", "Select"},
		{"select * from label where c1 > 1", "Select"},
		{"select * from label where c1 > 1", "Select"},
		{"select * from label order by c3 limit 1", "Select"},
		{"delete from label", "Delete"},
		{"delete from label where c1 = 1", "Delete"},
		{"delete from label where c2 = 1", "Delete"},
		{"delete from label where c2 = 1 order by c3 limit 1", "Delete"},
		{"uFIDelate label set c3 = 3", "UFIDelate"},
		{"uFIDelate label set c3 = 3 where c1 = 1", "UFIDelate"},
		{"uFIDelate label set c3 = 3 where c2 = 1", "UFIDelate"},
		{"uFIDelate label set c3 = 3 where c2 = 1 order by c3 limit 1", "UFIDelate"},
	}
	for _, tt := range tests {
		stmtNode, err := berolinaAllegroSQL.New().ParseOneStmt(tt.allegrosql, "", "")
		c.Check(err, IsNil)
		is := schemareplicant.GetSchemaReplicant(tk.Se)
		err = plannercore.Preprocess(tk.Se.(stochastikctx.Context), stmtNode, is)
		c.Assert(err, IsNil)
		_, _, err = planner.Optimize(context.TODO(), tk.Se, stmtNode, is)
		c.Assert(err, IsNil)
		c.Assert(executor.GetStmtLabel(stmtNode), Equals, tt.label)
	}
}
