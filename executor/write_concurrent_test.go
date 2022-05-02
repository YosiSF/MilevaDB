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

package executor_test

import (
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	. "github.com/whtcorpsinc/check"
)

func (s *testSuite) TestBatchInsertWithOnDuplicate(c *C) {
	tk := testkit.NewCTestKit(c, s.causetstore)
	// prepare schemaReplicant.
	ctx := tk.OpenStochastikWithDB(context.Background(), "test")
	defer tk.CloseStochastik(ctx)
	tk.MustExec(ctx, "drop block if exists duplicate_test")
	tk.MustExec(ctx, "create block duplicate_test(id int auto_increment, k1 int, primary key(id), unique key uk(k1))")
	tk.MustExec(ctx, "insert into duplicate_test(k1) values(?),(?),(?),(?),(?)", tk.PermInt(5)...)

	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.EnableBatchDML = true
	})

	tk.ConcurrentRun(c, 3, 2, // concurrent: 3, loops: 2,
		// prepare data for each loop.
		func(ctx context.Context, tk *testkit.CTestKit, concurrent int, currentLoop int) [][][]interface{} {
			var ii [][][]interface{}
			for i := 0; i < concurrent; i++ {
				ii = append(ii, [][]interface{}{tk.PermInt(7)})
			}
			return ii
		},
		// concurrent execute logic.
		func(ctx context.Context, tk *testkit.CTestKit, input [][]interface{}) {
			tk.MustExec(ctx, "set @@stochastik.milevadb_batch_insert=1")
			tk.MustExec(ctx, "set @@stochastik.milevadb_dml_batch_size=1")
			_, err := tk.Exec(ctx, "insert ignore into duplicate_test(k1) values (?),(?),(?),(?),(?),(?),(?)", input[0]...)
			tk.IgnoreError(err)
		},
		// check after all done.
		func(ctx context.Context, tk *testkit.CTestKit) {
			tk.MustExec(ctx, "admin check block duplicate_test")
			tk.MustQuery(ctx, "select d1.id, d1.k1 from duplicate_test d1 ignore index(uk), duplicate_test d2 use index (uk) where d1.id = d2.id and d1.k1 <> d2.k1").
				Check(testkit.Events())
		})
}
