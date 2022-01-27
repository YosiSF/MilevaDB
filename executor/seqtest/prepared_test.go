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

package executor_test

import (
	"context"
	"crypto/tls"
	"math"
	"time"

	dto "github.com/prometheus/client_perceptron/go"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/metrics"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/kvcache"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/types"
)

func (s *seqTestSuite) TestPrepared(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()

	flags := []bool{false, true}
	ctx := context.Background()
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)

		tk := testkit.NewTestKit(c, s.causetstore)
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists prepare_test")
		tk.MustExec("create block prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
		tk.MustExec("insert prepare_test (c1) values (1),(2),(NULL)")

		tk.MustExec(`prepare stmt_test_1 from 'select id from prepare_test where id > ?'; set @a = 1; execute stmt_test_1 using @a;`)
		tk.MustExec(`prepare stmt_test_2 from 'select 1'`)
		// Prepare multiple statement is not allowed.
		_, err = tk.Exec(`prepare stmt_test_3 from 'select id from prepare_test where id > ?;select id from prepare_test where id > ?;'`)
		c.Assert(executor.ErrPrepareMulti.Equal(err), IsTrue)
		// The variable count does not match.
		_, err = tk.Exec(`prepare stmt_test_4 from 'select id from prepare_test where id > ? and id < ?'; set @a = 1; execute stmt_test_4 using @a;`)
		c.Assert(plannercore.ErrWrongParamCount.Equal(err), IsTrue)
		// Prepare and deallocate prepared statement immediately.
		tk.MustExec(`prepare stmt_test_5 from 'select id from prepare_test where id > ?'; deallocate prepare stmt_test_5;`)

		// Statement not found.
		_, err = tk.Exec("deallocate prepare stmt_test_5")
		c.Assert(plannercore.ErrStmtNotFound.Equal(err), IsTrue)

		// incorrect ALLEGROSQLs in prepare. issue #3738, ALLEGROALLEGROSQL in prepare stmt is parsed in DoPrepare.
		_, err = tk.Exec(`prepare p from "delete from t where a = 7 or 1=1/*' and b = 'p'";`)
		c.Assert(err.Error(), Equals, `[berolinaAllegroSQL:1064]You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use near '/*' and b = 'p'' at line 1`)

		// The `stmt_test5` should not be found.
		_, err = tk.Exec(`set @a = 1; execute stmt_test_5 using @a;`)
		c.Assert(plannercore.ErrStmtNotFound.Equal(err), IsTrue)

		// Use parameter marker with argument will run prepared statement.
		result := tk.MustQuery("select distinct c1, c2 from prepare_test where c1 = ?", 1)
		result.Check(testkit.Events("1 <nil>"))

		// Call Stochastik PrepareStmt directly to get stmtID.
		query := "select c1, c2 from prepare_test where c1 = ?"
		stmtID, _, _, err := tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		rs, err := tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("1 <nil>"))

		tk.MustExec("delete from prepare_test")
		query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)

		tk1 := testkit.NewTestKit(c, s.causetstore)
		tk1.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk1.MustExec("use test")
		tk1.MustExec("insert prepare_test (c1) values (3)")
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("3"))

		tk.MustExec("delete from prepare_test")
		query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		c.Assert(rs.Close(), IsNil)
		tk1.MustExec("insert prepare_test (c1) values (3)")
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("3"))

		tk.MustExec("delete from prepare_test")
		query = "select c1 from prepare_test where c1 in (select c1 from prepare_test where c1 = ?)"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		c.Assert(rs.Close(), IsNil)
		tk1.MustExec("insert prepare_test (c1) values (3)")
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("3"))

		tk.MustExec("begin")
		tk.MustExec("insert prepare_test (c1) values (4)")
		query = "select c1, c2 from prepare_test where c1 = ?"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		tk.MustExec("rollback")
		rs, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(4)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events())

		// Check that ast.Statement created by executor.CompileExecutePreparedStmt has query text.
		stmt, err := executor.CompileExecutePreparedStmt(context.TODO(), tk.Se, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)
		c.Assert(stmt.OriginText(), Equals, query)

		// Check that rebuild plan works.
		tk.Se.PrepareTxnCtx(ctx)
		_, err = stmt.RebuildPlan(ctx)
		c.Assert(err, IsNil)
		rs, err = stmt.Exec(ctx)
		c.Assert(err, IsNil)
		req := rs.NewChunk()
		err = rs.Next(ctx, req)
		c.Assert(err, IsNil)
		c.Assert(rs.Close(), IsNil)

		// Make schemaReplicant change.
		tk.MustExec("drop block if exists prepare2")
		tk.Exec("create block prepare2 (a int)")

		// Should success as the changed schemaReplicant do not affect the prepared statement.
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)

		// Drop a defCausumn so the prepared statement become invalid.
		query = "select c1, c2 from prepare_test where c1 = ?"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		tk.MustExec("alter block prepare_test drop defCausumn c2")

		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(plannercore.ErrUnknownDeferredCauset.Equal(err), IsTrue)

		tk.MustExec("drop block prepare_test")
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(plannercore.ErrSchemaChanged.Equal(err), IsTrue)

		// issue 3381
		tk.MustExec("drop block if exists prepare3")
		tk.MustExec("create block prepare3 (a decimal(1))")
		tk.MustExec("prepare stmt from 'insert into prepare3 value(123)'")
		_, err = tk.Exec("execute stmt")
		c.Assert(err, NotNil)

		_, _, fields, err := tk.Se.PrepareStmt("select a from prepare3")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].BlockAsName.L, Equals, "prepare3")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("select a from prepare3 where ?")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].BlockAsName.L, Equals, "prepare3")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("select (1,1) in (select 1,1)")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "")
		c.Assert(fields[0].BlockAsName.L, Equals, "")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "(1,1) in (select 1,1)")

		_, _, fields, err = tk.Se.PrepareStmt("select a from prepare3 where a = (" +
			"select a from prepare2 where a = ?)")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].BlockAsName.L, Equals, "prepare3")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("select * from prepare3 as t1 join prepare3 as t2")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].BlockAsName.L, Equals, "t1")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "a")
		c.Assert(fields[1].DBName.L, Equals, "test")
		c.Assert(fields[1].BlockAsName.L, Equals, "t2")
		c.Assert(fields[1].DeferredCausetAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("uFIDelate prepare3 set a = ?")
		c.Assert(err, IsNil)
		c.Assert(len(fields), Equals, 0)

		// issue 8074
		tk.MustExec("drop block if exists prepare1;")
		tk.MustExec("create block prepare1 (a decimal(1))")
		tk.MustExec("insert into prepare1 values(1);")
		_, err = tk.Exec("prepare stmt FROM @sql1")
		c.Assert(err.Error(), Equals, "[berolinaAllegroSQL:1064]You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use line 1 defCausumn 4 near \"NULL\" ")
		tk.MustExec("SET @allegrosql = 'uFIDelate prepare1 set a=5 where a=?';")
		_, err = tk.Exec("prepare stmt FROM @allegrosql")
		c.Assert(err, IsNil)
		tk.MustExec("set @var=1;")
		_, err = tk.Exec("execute stmt using @var")
		c.Assert(err, IsNil)
		tk.MustQuery("select a from prepare1;").Check(testkit.Events("5"))

		// issue 19371
		tk.MustExec("SET @allegrosql = 'uFIDelate prepare1 set a=a+1';")
		_, err = tk.Exec("prepare stmt FROM @ALLEGROALLEGROSQL")
		c.Assert(err, IsNil)
		_, err = tk.Exec("execute stmt")
		c.Assert(err, IsNil)
		tk.MustQuery("select a from prepare1;").Check(testkit.Events("6"))
		_, err = tk.Exec("prepare stmt FROM @Sql")
		c.Assert(err, IsNil)
		_, err = tk.Exec("execute stmt")
		c.Assert(err, IsNil)
		tk.MustQuery("select a from prepare1;").Check(testkit.Events("7"))

		// Coverage.
		exec := &executor.ExecuteExec{}
		exec.Next(ctx, nil)
		exec.Close()

		// issue 8065
		stmtID, _, _, err = tk.Se.PrepareStmt("select ? from dual")
		c.Assert(err, IsNil)
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)
		stmtID, _, _, err = tk.Se.PrepareStmt("uFIDelate prepare1 set a = ? where a = ?")
		c.Assert(err, IsNil)
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1), types.NewCauset(1)})
		c.Assert(err, IsNil)
	}
}

func (s *seqTestSuite) TestPreparedLimitOffset(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	flags := []bool{false, true}
	ctx := context.Background()
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)

		tk := testkit.NewTestKit(c, s.causetstore)
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists prepare_test")
		tk.MustExec("create block prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
		tk.MustExec("insert prepare_test (c1) values (1),(2),(NULL)")
		tk.MustExec(`prepare stmt_test_1 from 'select id from prepare_test limit ? offset ?'; set @a = 1, @b=1;`)
		r := tk.MustQuery(`execute stmt_test_1 using @a, @b;`)
		r.Check(testkit.Events("2"))

		tk.MustExec(`set @a=1.1`)
		r = tk.MustQuery(`execute stmt_test_1 using @a, @b;`)
		r.Check(testkit.Events("2"))

		tk.MustExec(`set @c="-1"`)
		_, err = tk.Exec("execute stmt_test_1 using @c, @c")
		c.Assert(plannercore.ErrWrongArguments.Equal(err), IsTrue)

		stmtID, _, _, err := tk.Se.PrepareStmt("select id from prepare_test limit ?")
		c.Assert(err, IsNil)
		_, err = tk.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)
	}
}

func (s *seqTestSuite) TestPreparedNullParam(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		plannercore.SetPreparedPlanCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists t")
		tk.MustExec("create block t (id int, KEY id (id))")
		tk.MustExec("insert into t values (1), (2), (3)")
		tk.MustExec(`prepare stmt from 'select * from t use index(id) where id = ?'`)

		r := tk.MustQuery(`execute stmt using @id;`)
		r.Check(nil)

		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(nil)

		tk.MustExec(`set @id="1"`)
		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Events("1"))

		r = tk.MustQuery(`execute stmt using @id2;`)
		r.Check(nil)

		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Events("1"))
	}
}

func (s *seqTestSuite) TestPrepareWithAggregation(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		plannercore.SetPreparedPlanCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists t")
		tk.MustExec("create block t (id int primary key)")
		tk.MustExec("insert into t values (1), (2), (3)")
		tk.MustExec(`prepare stmt from 'select sum(id) from t where id = ?'`)

		tk.MustExec(`set @id="1"`)
		r := tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Events("1"))

		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Events("1"))
	}
}

func (s *seqTestSuite) TestPreparedIssue7579(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		plannercore.SetPreparedPlanCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists t")
		tk.MustExec("create block t (a int, b int, index a_idx(a))")
		tk.MustExec("insert into t values (1,1), (2,2), (null,3)")

		r := tk.MustQuery("select a, b from t order by b asc;")
		r.Check(testkit.Events("1 1", "2 2", "<nil> 3"))

		tk.MustExec(`prepare stmt from 'select a, b from t where ? order by b asc'`)

		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(nil)

		tk.MustExec(`set @param = true`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 1", "2 2", "<nil> 3"))

		tk.MustExec(`set @param = false`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(nil)

		tk.MustExec(`set @param = 1`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 1", "2 2", "<nil> 3"))

		tk.MustExec(`set @param = 0`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(nil)
	}
}

func (s *seqTestSuite) TestPreparedInsert(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	metrics.ResetblockPlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		plannercore.SetPreparedPlanCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists prepare_test")
		tk.MustExec("create block prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustExec(`prepare stmt_insert from 'insert into prepare_test values (?, ?)'`)
		tk.MustExec(`set @a=1,@b=1; execute stmt_insert using @a, @b;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(0))
		}
		tk.MustExec(`set @a=2,@b=2; execute stmt_insert using @a, @b;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(1))
		}
		tk.MustExec(`set @a=3,@b=3; execute stmt_insert using @a, @b;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(testkit.Events("1 1"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(testkit.Events("2 2"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(testkit.Events("3 3"))

		tk.MustExec(`prepare stmt_insert_select from 'insert into prepare_test (id, c1) select id + 100, c1 + 100 from prepare_test where id = ?'`)
		tk.MustExec(`set @a=1; execute stmt_insert_select using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}
		tk.MustExec(`set @a=2; execute stmt_insert_select using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(3))
		}
		tk.MustExec(`set @a=3; execute stmt_insert_select using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(4))
		}

		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 101)
		result.Check(testkit.Events("101 101"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 102)
		result.Check(testkit.Events("102 102"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 103)
		result.Check(testkit.Events("103 103"))
	}
}

func (s *seqTestSuite) TestPreparedUFIDelate(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	metrics.ResetblockPlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		plannercore.SetPreparedPlanCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists prepare_test")
		tk.MustExec("create block prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustExec(`insert into prepare_test values (1, 1)`)
		tk.MustExec(`insert into prepare_test values (2, 2)`)
		tk.MustExec(`insert into prepare_test values (3, 3)`)

		tk.MustExec(`prepare stmt_uFIDelate from 'uFIDelate prepare_test set c1 = c1 + ? where id = ?'`)
		tk.MustExec(`set @a=1,@b=100; execute stmt_uFIDelate using @b,@a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}
		tk.MustExec(`set @a=2,@b=200; execute stmt_uFIDelate using @b,@a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(3))
		}
		tk.MustExec(`set @a=3,@b=300; execute stmt_uFIDelate using @b,@a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(4))
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(testkit.Events("1 101"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(testkit.Events("2 202"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(testkit.Events("3 303"))
	}
}

func (s *seqTestSuite) TestPreparedDelete(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	metrics.ResetblockPlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		plannercore.SetPreparedPlanCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists prepare_test")
		tk.MustExec("create block prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustExec(`insert into prepare_test values (1, 1)`)
		tk.MustExec(`insert into prepare_test values (2, 2)`)
		tk.MustExec(`insert into prepare_test values (3, 3)`)

		tk.MustExec(`prepare stmt_delete from 'delete from prepare_test where id = ?'`)
		tk.MustExec(`set @a=1; execute stmt_delete using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(0))
		}
		tk.MustExec(`set @a=2; execute stmt_delete using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(1))
		}
		tk.MustExec(`set @a=3; execute stmt_delete using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(nil)
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(nil)
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(nil)
	}
}

func (s *seqTestSuite) TestPrepareDealloc(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)

	tk := testkit.NewTestKit(c, s.causetstore)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(3, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists prepare_test")
	tk.MustExec("create block prepare_test (id int PRIMARY KEY, c1 int)")

	c.Assert(tk.Se.PreparedPlanCache().Size(), Equals, 0)
	tk.MustExec(`prepare stmt1 from 'select * from prepare_test'`)
	tk.MustExec("execute stmt1")
	tk.MustExec(`prepare stmt2 from 'select * from prepare_test'`)
	tk.MustExec("execute stmt2")
	tk.MustExec(`prepare stmt3 from 'select * from prepare_test'`)
	tk.MustExec("execute stmt3")
	tk.MustExec(`prepare stmt4 from 'select * from prepare_test'`)
	tk.MustExec("execute stmt4")
	c.Assert(tk.Se.PreparedPlanCache().Size(), Equals, 3)

	tk.MustExec("deallocate prepare stmt1")
	c.Assert(tk.Se.PreparedPlanCache().Size(), Equals, 3)
	tk.MustExec("deallocate prepare stmt2")
	tk.MustExec("deallocate prepare stmt3")
	tk.MustExec("deallocate prepare stmt4")
	c.Assert(tk.Se.PreparedPlanCache().Size(), Equals, 0)
}

func (s *seqTestSuite) TestPreparedIssue8153(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		var err error
		plannercore.SetPreparedPlanCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists t")
		tk.MustExec("create block t (a int, b int)")
		tk.MustExec("insert into t (a, b) values (1,3), (2,2), (3,1)")

		tk.MustExec(`prepare stmt from 'select * from t order by ? asc'`)
		r := tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 3", "2 2", "3 1"))

		tk.MustExec(`set @param = 1`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 3", "2 2", "3 1"))

		tk.MustExec(`set @param = 2`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("3 1", "2 2", "1 3"))

		tk.MustExec(`set @param = 3`)
		_, err = tk.Exec(`execute stmt using @param;`)
		c.Assert(err.Error(), Equals, "[planner:1054]Unknown defCausumn '?' in 'order clause'")

		tk.MustExec(`set @param = '##'`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 3", "2 2", "3 1"))

		tk.MustExec("insert into t (a, b) values (1,1), (1,2), (2,1), (2,3), (3,2), (3,3)")
		tk.MustExec(`prepare stmt from 'select ?, sum(a) from t group by ?'`)

		tk.MustExec(`set @a=1,@b=1`)
		r = tk.MustQuery(`execute stmt using @a,@b;`)
		r.Check(testkit.Events("1 18"))

		tk.MustExec(`set @a=1,@b=2`)
		_, err = tk.Exec(`execute stmt using @a,@b;`)
		c.Assert(err.Error(), Equals, "[planner:1056]Can't group on 'sum(a)'")
	}
}

func (s *seqTestSuite) TestPreparedIssue8644(c *C) {
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		plannercore.SetPreparedPlanCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustExec("use test")
		tk.MustExec("drop block if exists t")
		tk.MustExec("create block t(data mediumblob)")

		tk.MustExec(`prepare stmt1 from 'insert t (data) values (?)'`)

		tk.MustExec(`set @a = 'a'`)
		tk.MustExec(`execute stmt1 using @a;`)

		tk.MustExec(`set @b = 'aaaaaaaaaaaaaaaaaa'`)
		tk.MustExec(`execute stmt1 using @b;`)

		r := tk.MustQuery(`select * from t`)
		r.Check(testkit.Events("a", "aaaaaaaaaaaaaaaaaa"))
	}
}

// mockStochastikManager is a mocked stochastik manager which is used for test.
type mockStochastikManager1 struct {
	Se stochastik.Stochastik
}

// ShowProcessList implements the StochastikManager.ShowProcessList interface.
func (msm *mockStochastikManager1) ShowProcessList() map[uint64]*soliton.ProcessInfo {
	ret := make(map[uint64]*soliton.ProcessInfo)
	return ret
}

func (msm *mockStochastikManager1) GetProcessInfo(id uint64) (*soliton.ProcessInfo, bool) {
	pi := msm.Se.ShowProcess()
	return pi, true
}

// Kill implements the StochastikManager.Kill interface.
func (msm *mockStochastikManager1) Kill(cid uint64, query bool) {}

func (msm *mockStochastikManager1) UFIDelateTLSConfig(cfg *tls.Config) {}

func (s *seqTestSuite) TestPreparedIssue17419(c *C) {
	ctx := context.Background()
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int)")
	tk.MustExec("insert into t (a) values (1), (2), (3)")

	tk1 := testkit.NewTestKit(c, s.causetstore)

	var err error
	tk1.Se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	tk1.GetConnectionID()

	query := "select * from test.t"
	stmtID, _, _, err := tk1.Se.PrepareStmt(query)
	c.Assert(err, IsNil)

	sm := &mockStochastikManager1{
		Se: tk1.Se,
	}
	tk1.Se.SetStochastikManager(sm)
	s.petri.ExpensiveQueryHandle().SetStochastikManager(sm)

	rs, err := tk1.Se.ExecutePreparedStmt(ctx, stmtID, []types.Causet{})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("1", "2", "3"))
	tk1.Se.SetProcessInfo("", time.Now(), allegrosql.ComStmtExecute, 0)

	s.petri.ExpensiveQueryHandle().LogOnQueryExceedMemQuota(tk.Se.GetStochastikVars().ConnectionID)

	// After entirely fixing https://github.com/whtcorpsinc/milevadb/issues/17419
	// c.Assert(tk1.Se.ShowProcess().Plan, NotNil)
	// _, ok := tk1.Se.ShowProcess().Plan.(*plannercore.Execute)
	// c.Assert(ok, IsTrue)
}
