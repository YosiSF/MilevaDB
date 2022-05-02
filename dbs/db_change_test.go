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

package dbs_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/executor"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/admin"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/gcutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/sqlexec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"go.uber.org/zap"
)

var _ = Suite(&testStateChangeSuite{})
var _ = SerialSuites(&serialTestStateChangeSuite{})

type serialTestStateChangeSuite struct {
	testStateChangeSuiteBase
}

type testStateChangeSuite struct {
	testStateChangeSuiteBase
}

type testStateChangeSuiteBase struct {
	lease         time.Duration
	causetstore   ekv.CausetStorage
	dom           *petri.Petri
	se            stochastik.Stochastik
	p             *berolinaAllegroSQL.berolinaAllegroSQL
	preALLEGROSQL string
}

func (s *testStateChangeSuiteBase) SetUpSuite(c *C) {
	s.lease = 200 * time.Millisecond
	dbs.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	var err error
	s.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	stochastik.SetSchemaLease(s.lease)
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	s.se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create database test_db_state default charset utf8 default defCauslate utf8_bin")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	s.p = berolinaAllegroSQL.New()
}

func (s *testStateChangeSuiteBase) TearDownSuite(c *C) {
	s.se.Execute(context.Background(), "drop database if exists test_db_state")
	s.se.Close()
	s.dom.Close()
	s.causetstore.Close()
}

// TestShowCreateBlock tests the result of "show create block" when we are running "add index" or "add defCausumn".
func (s *serialTestStateChangeSuite) TestShowCreateBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t (id int)")
	tk.MustExec("create block t2 (a int, b varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci")
	// tkInternal is used to execute additional allegrosql (here show create block) in dbs change callback.
	// Using same `tk` in different goroutines may lead to data race.
	tkInternal := testkit.NewTestKit(c, s.causetstore)
	tkInternal.MustExec("use test")

	var checkErr error
	testCases := []struct {
		allegrosql  string
		expectedRet string
	}{
		{"alter block t add index idx(id)",
			"CREATE TABLE `t` (\n  `id` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"},
		{"alter block t add index idx1(id)",
			"CREATE TABLE `t` (\n  `id` int(11) DEFAULT NULL,\n  KEY `idx` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"},
		{"alter block t add defCausumn c int",
			"CREATE TABLE `t` (\n  `id` int(11) DEFAULT NULL,\n  KEY `idx` (`id`),\n  KEY `idx1` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"},
		{"alter block t2 add defCausumn c varchar(1)",
			"CREATE TABLE `t2` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(10) COLLATE utf8mb4_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"},
		{"alter block t2 add defCausumn d varchar(1)",
			"CREATE TABLE `t2` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(10) COLLATE utf8mb4_general_ci DEFAULT NULL,\n  `c` varchar(1) COLLATE utf8mb4_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"},
	}
	prevState := perceptron.StateNone
	callback := &dbs.TestDBSCallback{}
	currTestCaseOffset := 0
	callback.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		if job.SchemaState == prevState || checkErr != nil {
			return
		}
		if job.State == perceptron.JobStateDone {
			currTestCaseOffset++
		}
		if job.SchemaState != perceptron.StatePublic {
			var result sqlexec.RecordSet
			tbl2 := testGetBlockByName(c, tkInternal.Se, "test", "t2")
			if job.BlockID == tbl2.Meta().ID {
				// Try to do not use mustQuery in hook func, cause assert fail in mustQuery will cause dbs job hung.
				result, checkErr = tkInternal.Exec("show create block t2")
				if checkErr != nil {
					return
				}
			} else {
				result, checkErr = tkInternal.Exec("show create block t")
				if checkErr != nil {
					return
				}
			}
			req := result.NewChunk()
			checkErr = result.Next(context.Background(), req)
			if checkErr != nil {
				return
			}
			got := req.GetRow(0).GetString(1)
			expected := testCases[currTestCaseOffset].expectedRet
			if got != expected {
				checkErr = errors.Errorf("got %s, expected %s", got, expected)
			}
			terror.Log(result.Close())
		}
	}
	d := s.dom.DBS()
	originalCallback := d.GetHook()
	defer d.(dbs.DBSForTest).SetHook(originalCallback)
	d.(dbs.DBSForTest).SetHook(callback)
	for _, tc := range testCases {
		tk.MustExec(tc.allegrosql)
		c.Assert(checkErr, IsNil)
	}
}

// TestDropNotNullDeferredCauset is used to test issue #8654.
func (s *testStateChangeSuite) TestDropNotNullDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t (id int, a int not null default 11)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("create block t1 (id int, b varchar(255) not null)")
	tk.MustExec("insert into t1 values(2, '')")
	tk.MustExec("create block t2 (id int, c time not null)")
	tk.MustExec("insert into t2 values(3, '11:22:33')")
	tk.MustExec("create block t3 (id int, d json not null)")
	tk.MustExec("insert into t3 values(4, d)")
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustExec("use test")

	var checkErr error
	d := s.dom.DBS()
	originalCallback := d.GetHook()
	callback := &dbs.TestDBSCallback{}
	sqlNum := 0
	callback.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		if checkErr != nil {
			return
		}
		originalCallback.OnChanged(nil)
		if job.SchemaState == perceptron.StateWriteOnly {
			switch sqlNum {
			case 0:
				_, checkErr = tk1.Exec("insert into t set id = 1")
			case 1:
				_, checkErr = tk1.Exec("insert into t1 set id = 2")
			case 2:
				_, checkErr = tk1.Exec("insert into t2 set id = 3")
			case 3:
				_, checkErr = tk1.Exec("insert into t3 set id = 4")
			}
		}
	}

	d.(dbs.DBSForTest).SetHook(callback)
	tk.MustExec("alter block t drop defCausumn a")
	c.Assert(checkErr, IsNil)
	sqlNum++
	tk.MustExec("alter block t1 drop defCausumn b")
	c.Assert(checkErr, IsNil)
	sqlNum++
	tk.MustExec("alter block t2 drop defCausumn c")
	c.Assert(checkErr, IsNil)
	sqlNum++
	tk.MustExec("alter block t3 drop defCausumn d")
	c.Assert(checkErr, IsNil)
	d.(dbs.DBSForTest).SetHook(originalCallback)
	tk.MustExec("drop block t, t1, t2, t3")
}

func (s *testStateChangeSuite) TestTwoStates(c *C) {
	cnt := 5
	// New the testExecInfo.
	testInfo := &testExecInfo{
		execCases: cnt,
		sqlInfos:  make([]*sqlInfo, 4),
	}
	for i := 0; i < len(testInfo.sqlInfos); i++ {
		sqlInfo := &sqlInfo{cases: make([]*stateCase, cnt)}
		for j := 0; j < cnt; j++ {
			sqlInfo.cases[j] = new(stateCase)
		}
		testInfo.sqlInfos[i] = sqlInfo
	}
	err := testInfo.createStochastiks(s.causetstore, "test_db_state")
	c.Assert(err, IsNil)
	// Fill the ALLEGROSQLs and expected error messages.
	testInfo.sqlInfos[0].allegrosql = "insert into t (c1, c2, c3, c4) value(2, 'b', 'N', '2020-07-02')"
	testInfo.sqlInfos[1].allegrosql = "insert into t (c1, c2, c3, d3, c4) value(3, 'b', 'N', 'a', '2020-07-03')"
	unknownDefCausErr := "[planner:1054]Unknown defCausumn 'd3' in 'field list'"
	testInfo.sqlInfos[1].cases[0].expectedCompileErr = unknownDefCausErr
	testInfo.sqlInfos[1].cases[1].expectedCompileErr = unknownDefCausErr
	testInfo.sqlInfos[1].cases[2].expectedCompileErr = unknownDefCausErr
	testInfo.sqlInfos[1].cases[3].expectedCompileErr = unknownDefCausErr
	testInfo.sqlInfos[2].allegrosql = "uFIDelate t set c2 = 'c2_uFIDelate'"
	testInfo.sqlInfos[3].allegrosql = "replace into t values(5, 'e', 'N', '2020-07-05')"
	testInfo.sqlInfos[3].cases[4].expectedCompileErr = "[planner:1136]DeferredCauset count doesn't match value count at event 1"
	alterBlockALLEGROSQL := "alter block t add defCausumn d3 enum('a', 'b') not null default 'a' after c3"
	s.test(c, "", alterBlockALLEGROSQL, testInfo)
	// TODO: Add more DBS statements.
}

func (s *testStateChangeSuite) test(c *C, blockName, alterBlockALLEGROSQL string, testInfo *testExecInfo) {
	_, err := s.se.Execute(context.Background(), `create block t (
		c1 int,
		c2 varchar(64),
		c3 enum('N','Y') not null default 'N',
		c4 timestamp on uFIDelate current_timestamp,
		key(c1, c2))`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block t")
	_, err = s.se.Execute(context.Background(), "insert into t values(1, 'a', 'N', '2020-07-01')")
	c.Assert(err, IsNil)

	callback := &dbs.TestDBSCallback{}
	prevState := perceptron.StateNone
	var checkErr error
	err = testInfo.parseALLEGROSQLs(s.p)
	c.Assert(err, IsNil, Commentf("error stack %v", errors.ErrorStack(err)))
	times := 0
	callback.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		if job.SchemaState == prevState || checkErr != nil || times >= 3 {
			return
		}
		times++
		switch job.SchemaState {
		case perceptron.StateDeleteOnly:
			// This state we execute every sqlInfo one time using the first stochastik and other information.
			err = testInfo.compileALLEGROSQL(0)
			if err != nil {
				checkErr = err
				break
			}
			err = testInfo.execALLEGROSQL(0)
			if err != nil {
				checkErr = err
			}
		case perceptron.StateWriteOnly:
			// This state we put the schemaReplicant information to the second case.
			err = testInfo.compileALLEGROSQL(1)
			if err != nil {
				checkErr = err
			}
		case perceptron.StateWriteReorganization:
			// This state we execute every sqlInfo one time using the third stochastik and other information.
			err = testInfo.compileALLEGROSQL(2)
			if err != nil {
				checkErr = err
				break
			}
			err = testInfo.execALLEGROSQL(2)
			if err != nil {
				checkErr = err
				break
			}
			// Mock the server is in `write only` state.
			err = testInfo.execALLEGROSQL(1)
			if err != nil {
				checkErr = err
				break
			}
			// This state we put the schemaReplicant information to the fourth case.
			err = testInfo.compileALLEGROSQL(3)
			if err != nil {
				checkErr = err
			}
		}
	}
	d := s.dom.DBS()
	originalCallback := d.GetHook()
	defer d.(dbs.DBSForTest).SetHook(originalCallback)
	d.(dbs.DBSForTest).SetHook(callback)
	_, err = s.se.Execute(context.Background(), alterBlockALLEGROSQL)
	c.Assert(err, IsNil)
	err = testInfo.compileALLEGROSQL(4)
	c.Assert(err, IsNil)
	err = testInfo.execALLEGROSQL(4)
	c.Assert(err, IsNil)
	// Mock the server is in `write reorg` state.
	err = testInfo.execALLEGROSQL(3)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
}

type stateCase struct {
	stochastik         stochastik.Stochastik
	rawStmt            ast.StmtNode
	stmt               sqlexec.Statement
	expectedExecErr    string
	expectedCompileErr string
}

type sqlInfo struct {
	allegrosql string
	// cases is multiple stateCases.
	// Every case need to be executed with the different schemaReplicant state.
	cases []*stateCase
}

// testExecInfo contains some ALLEGROALLEGROSQL information and the number of times each ALLEGROALLEGROSQL is executed
// in a DBS statement.
type testExecInfo struct {
	// execCases represents every ALLEGROALLEGROSQL need to be executed execCases times.
	// And the schemaReplicant state is different at each execution.
	execCases int
	// sqlInfos represents this test information has multiple ALLEGROSQLs to test.
	sqlInfos []*sqlInfo
}

func (t *testExecInfo) createStochastiks(causetstore ekv.CausetStorage, useDB string) error {
	var err error
	for i, info := range t.sqlInfos {
		for j, c := range info.cases {
			c.stochastik, err = stochastik.CreateStochastik4Test(causetstore)
			if err != nil {
				return errors.Trace(err)
			}
			_, err = c.stochastik.Execute(context.Background(), "use "+useDB)
			if err != nil {
				return errors.Trace(err)
			}
			// It's used to debug.
			c.stochastik.SetConnectionID(uint64(i*10 + j))
		}
	}
	return nil
}

func (t *testExecInfo) parseALLEGROSQLs(p *berolinaAllegroSQL.berolinaAllegroSQL) error {
	if t.execCases <= 0 {
		return nil
	}
	var err error
	for _, sqlInfo := range t.sqlInfos {
		seVars := sqlInfo.cases[0].stochastik.GetStochaseinstein_dbars()
		charset, defCauslation := seVars.GetCharsetInfo()
		for j := 0; j < t.execCases; j++ {
			sqlInfo.cases[j].rawStmt, err = p.ParseOneStmt(sqlInfo.allegrosql, charset, defCauslation)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (t *testExecInfo) compileALLEGROSQL(idx int) (err error) {
	for _, info := range t.sqlInfos {
		c := info.cases[idx]
		compiler := executor.Compiler{Ctx: c.stochastik}
		se := c.stochastik
		ctx := context.TODO()
		se.PrepareTxnCtx(ctx)
		sctx := se.(stochastikctx.Context)
		if err = executor.ResetContextOfStmt(sctx, c.rawStmt); err != nil {
			return errors.Trace(err)
		}
		c.stmt, err = compiler.Compile(ctx, c.rawStmt)
		if c.expectedCompileErr != "" {
			if err == nil {
				err = errors.Errorf("expected error %s but got nil", c.expectedCompileErr)
			} else if err.Error() == c.expectedCompileErr {
				err = nil
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *testExecInfo) execALLEGROSQL(idx int) error {
	for _, sqlInfo := range t.sqlInfos {
		c := sqlInfo.cases[idx]
		if c.expectedCompileErr != "" {
			continue
		}
		_, err := c.stmt.Exec(context.TODO())
		if c.expectedExecErr != "" {
			if err == nil {
				err = errors.Errorf("expected error %s but got nil", c.expectedExecErr)
			} else if err.Error() == c.expectedExecErr {
				err = nil
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
		err = c.stochastik.CommitTxn(context.TODO())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type sqlWithErr struct {
	allegrosql string
	expectErr  error
}

type expectQuery struct {
	allegrosql string
	rows       []string
}

func (s *testStateChangeSuite) TestAppendEnum(c *C) {
	_, err := s.se.Execute(context.Background(), `create block t (
			c1 varchar(64),
			c2 enum('N','Y') not null default 'N',
			c3 timestamp on uFIDelate current_timestamp,
			c4 int primary key,
			unique key idx2 (c2, c3))`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block t")
	_, err = s.se.Execute(context.Background(), "insert into t values('a', 'N', '2020-07-01', 8)")
	c.Assert(err, IsNil)
	// Make sure these sqls use the the plan of index scan.
	_, err = s.se.Execute(context.Background(), "drop stats t")
	c.Assert(err, IsNil)
	se, err := stochastik.CreateStochastik(s.causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	_, err = s.se.Execute(context.Background(), "insert into t values('a', 'A', '2020-09-19', 9)")
	c.Assert(err.Error(), Equals, "[types:1265]Data truncated for defCausumn 'c2' at event 1")
	failAlterBlockALLEGROSQL1 := "alter block t change c2 c2 enum('N') DEFAULT 'N'"
	_, err = s.se.Execute(context.Background(), failAlterBlockALLEGROSQL1)
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: the number of enum defCausumn's elements is less than the original: 2")
	failAlterBlockALLEGROSQL2 := "alter block t change c2 c2 int default 0"
	_, err = s.se.Execute(context.Background(), failAlterBlockALLEGROSQL2)
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: cannot modify enum type defCausumn's to type int(11)")
	alterBlockALLEGROSQL := "alter block t change c2 c2 enum('N','Y','A') DEFAULT 'A'"
	_, err = s.se.Execute(context.Background(), alterBlockALLEGROSQL)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "insert into t values('a', 'A', '2020-09-20', 10)")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "insert into t (c1, c3, c4) values('a', '2020-09-21', 11)")
	c.Assert(err, IsNil)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test_db_state")
	result, err := s.execQuery(tk, "select c4, c2 from t order by c4 asc")
	c.Assert(err, IsNil)
	expected := []string{"8 N", "10 A", "11 A"}
	checkResult(result, testkit.Rows(expected...))

	_, err = s.se.Execute(context.Background(), "uFIDelate t set c2='N' where c4 = 10")
	c.Assert(err, IsNil)
	result, err = s.execQuery(tk, "select c2 from t where c4 = 10")
	c.Assert(err, IsNil)
	expected = []string{"8 N", "10 N", "11 A"}
	checkResult(result, testkit.Rows(expected...))
}

// https://github.com/whtcorpsinc/MilevaDB-Prod/pull/6249 fixes the following two test cases.
func (s *testStateChangeSuite) TestWriteOnlyWriteNULL(c *C) {
	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_new', c3 = '2020-02-12', c4 = 8 on duplicate key uFIDelate c1 = values(c1)", nil}
	addDeferredCausetALLEGROSQL := "alter block t add defCausumn c5 int not null default 1 after c4"
	expectQuery := &expectQuery{"select c4, c5 from t", []string{"8 1"}}
	s.runTestInSchemaState(c, perceptron.StateWriteOnly, true, addDeferredCausetALLEGROSQL, sqls, expectQuery)
}

func (s *testStateChangeSuite) TestWriteOnlyOnDupUFIDelate(c *C) {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t", nil}
	sqls[1] = sqlWithErr{"insert t set c1 = 'c1_dup', c3 = '2020-02-12', c4 = 2 on duplicate key uFIDelate c1 = values(c1)", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_new', c3 = '2020-02-12', c4 = 2 on duplicate key uFIDelate c1 = values(c1)", nil}
	addDeferredCausetALLEGROSQL := "alter block t add defCausumn c5 int not null default 1 after c4"
	expectQuery := &expectQuery{"select c4, c5 from t", []string{"2 1"}}
	s.runTestInSchemaState(c, perceptron.StateWriteOnly, true, addDeferredCausetALLEGROSQL, sqls, expectQuery)
}

func (s *testStateChangeSuite) TestWriteOnlyOnDupUFIDelateForAddDeferredCausets(c *C) {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t", nil}
	sqls[1] = sqlWithErr{"insert t set c1 = 'c1_dup', c3 = '2020-02-12', c4 = 2 on duplicate key uFIDelate c1 = values(c1)", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_new', c3 = '2020-02-12', c4 = 2 on duplicate key uFIDelate c1 = values(c1)", nil}
	addDeferredCausetsALLEGROSQL := "alter block t add defCausumn c5 int not null default 1 after c4, add defCausumn c44 int not null default 1"
	expectQuery := &expectQuery{"select c4, c5, c44 from t", []string{"2 1 1"}}
	s.runTestInSchemaState(c, perceptron.StateWriteOnly, true, addDeferredCausetsALLEGROSQL, sqls, expectQuery)
}

type idxType byte

const (
	noneIdx    idxType = 0
	uniqIdx    idxType = 1
	primaryIdx idxType = 2
)

// TestWriteReorgForModifyDeferredCauset tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *serialTestStateChangeSuite) TestWriteReorgForModifyDeferredCauset(c *C) {
	modifyDeferredCausetALLEGROSQL := "alter block tt change defCausumn c cc tinyint not null default 1 first"
	s.testModifyDeferredCauset(c, perceptron.StateWriteReorganization, modifyDeferredCausetALLEGROSQL, noneIdx)
}

// TestWriteReorgForModifyDeferredCausetWithUniqIdx tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *serialTestStateChangeSuite) TestWriteReorgForModifyDeferredCausetWithUniqIdx(c *C) {
	modifyDeferredCausetALLEGROSQL := "alter block tt change defCausumn c cc tinyint unsigned not null default 1 first"
	s.testModifyDeferredCauset(c, perceptron.StateWriteReorganization, modifyDeferredCausetALLEGROSQL, uniqIdx)
}

// TestWriteReorgForModifyDeferredCausetWithPKIsHandle tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *serialTestStateChangeSuite) TestWriteReorgForModifyDeferredCausetWithPKIsHandle(c *C) {
	modifyDeferredCausetALLEGROSQL := "alter block tt change defCausumn c cc tinyint unsigned not null default 1 first"
	enableChangeDeferredCausetType := s.se.GetStochaseinstein_dbars().EnableChangeDeferredCausetType
	s.se.GetStochaseinstein_dbars().EnableChangeDeferredCausetType = true
	defer func() {
		s.se.GetStochaseinstein_dbars().EnableChangeDeferredCausetType = enableChangeDeferredCausetType
		config.RestoreFunc()()
	}()

	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = false
	})

	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create block tt (a int not null, b int default 1, c int not null default 0, unique index idx(c), primary key idx1(a), index idx2(a, c))`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tt (a, c) values(-1, -11)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tt (a, c) values(1, 11)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block tt")

	sqls := make([]sqlWithErr, 12)
	sqls[0] = sqlWithErr{"delete from tt where c = -11", nil}
	sqls[1] = sqlWithErr{"uFIDelate tt use index(idx2) set a = 12, c = 555 where c = 11", errors.Errorf("[types:1690]constant 555 overflows tinyint")}
	sqls[2] = sqlWithErr{"uFIDelate tt use index(idx2) set a = 12, c = 10 where c = 11", nil}
	sqls[3] = sqlWithErr{"insert into tt (a, c) values(2, 22)", nil}
	sqls[4] = sqlWithErr{"uFIDelate tt use index(idx2) set a = 21, c = 2 where c = 22", nil}
	sqls[5] = sqlWithErr{"uFIDelate tt use index(idx2) set a = 23 where c = 2", nil}
	sqls[6] = sqlWithErr{"insert tt set a = 31, c = 333", errors.Errorf("[types:1690]constant 333 overflows tinyint")}
	sqls[7] = sqlWithErr{"insert tt set a = 32, c = 123", nil}
	sqls[8] = sqlWithErr{"insert tt set a = 33", nil}
	sqls[9] = sqlWithErr{"insert into tt select * from tt order by c limit 1 on duplicate key uFIDelate c = 44;", nil}
	sqls[10] = sqlWithErr{"replace into tt values(5, 55, 56)", nil}
	sqls[11] = sqlWithErr{"replace into tt values(6, 66, 56)", nil}

	query := &expectQuery{allegrosql: "admin check block tt;", rows: nil}
	s.runTestInSchemaState(c, perceptron.StateWriteReorganization, false, modifyDeferredCausetALLEGROSQL, sqls, query)
}

// TestWriteReorgForModifyDeferredCausetWithPrimaryIdx tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *serialTestStateChangeSuite) TestWriteReorgForModifyDeferredCausetWithPrimaryIdx(c *C) {
	modifyDeferredCausetALLEGROSQL := "alter block tt change defCausumn c cc tinyint not null default 1 first"
	s.testModifyDeferredCauset(c, perceptron.StateWriteReorganization, modifyDeferredCausetALLEGROSQL, uniqIdx)
}

// TestWriteReorgForModifyDeferredCausetWithoutFirst tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *serialTestStateChangeSuite) TestWriteReorgForModifyDeferredCausetWithoutFirst(c *C) {
	modifyDeferredCausetALLEGROSQL := "alter block tt change defCausumn c cc tinyint not null default 1"
	s.testModifyDeferredCauset(c, perceptron.StateWriteReorganization, modifyDeferredCausetALLEGROSQL, noneIdx)
}

// TestWriteReorgForModifyDeferredCausetWithoutDefaultVal tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *serialTestStateChangeSuite) TestWriteReorgForModifyDeferredCausetWithoutDefaultVal(c *C) {
	modifyDeferredCausetALLEGROSQL := "alter block tt change defCausumn c cc tinyint first"
	s.testModifyDeferredCauset(c, perceptron.StateWriteReorganization, modifyDeferredCausetALLEGROSQL, noneIdx)
}

// TestDeleteOnlyForModifyDeferredCausetWithoutDefaultVal tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *serialTestStateChangeSuite) TestDeleteOnlyForModifyDeferredCausetWithoutDefaultVal(c *C) {
	modifyDeferredCausetALLEGROSQL := "alter block tt change defCausumn c cc tinyint first"
	s.testModifyDeferredCauset(c, perceptron.StateDeleteOnly, modifyDeferredCausetALLEGROSQL, noneIdx)
}

func (s *serialTestStateChangeSuite) testModifyDeferredCauset(c *C, state perceptron.SchemaState, modifyDeferredCausetALLEGROSQL string, idx idxType) {
	enableChangeDeferredCausetType := s.se.GetStochaseinstein_dbars().EnableChangeDeferredCausetType
	s.se.GetStochaseinstein_dbars().EnableChangeDeferredCausetType = true
	defer func() {
		s.se.GetStochaseinstein_dbars().EnableChangeDeferredCausetType = enableChangeDeferredCausetType
	}()

	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	switch idx {
	case uniqIdx:
		_, err = s.se.Execute(context.Background(), `create block tt  (a varchar(64), b int default 1, c int not null default 0, unique index idx(c), unique index idx1(a), index idx2(a, c))`)
	case primaryIdx:
		// TODO: Support modify/change defCausumn with the primary key.
		_, err = s.se.Execute(context.Background(), `create block tt  (a varchar(64), b int default 1, c int not null default 0, index idx(c), primary index idx1(a), index idx2(a, c))`)
	default:
		_, err = s.se.Execute(context.Background(), `create block tt  (a varchar(64), b int default 1, c int not null default 0, index idx(c), index idx1(a), index idx2(a, c))`)
	}
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tt (a, c) values('a', 11)")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tt (a, c) values('b', 22)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block tt")

	sqls := make([]sqlWithErr, 13)
	sqls[0] = sqlWithErr{"delete from tt where c = 11", nil}
	if state == perceptron.StateWriteReorganization {
		sqls[1] = sqlWithErr{"uFIDelate tt use index(idx2) set a = 'a_uFIDelate', c = 555 where c = 22", errors.Errorf("[types:1690]constant 555 overflows tinyint")}
		sqls[4] = sqlWithErr{"insert tt set a = 'a_insert', c = 333", errors.Errorf("[types:1690]constant 333 overflows tinyint")}
	} else {
		sqls[1] = sqlWithErr{"uFIDelate tt use index(idx2) set a = 'a_uFIDelate', c = 2 where c = 22", nil}
		sqls[4] = sqlWithErr{"insert tt set a = 'a_insert', b = 123, c = 111", nil}
	}
	sqls[2] = sqlWithErr{"uFIDelate tt use index(idx2) set a = 'a_uFIDelate', c = 2 where c = 22", nil}
	sqls[3] = sqlWithErr{"uFIDelate tt use index(idx2) set a = 'a_uFIDelate_1' where c = 2", nil}
	if idx == noneIdx {
		sqls[5] = sqlWithErr{"insert tt set a = 'a_insert', c = 111", nil}
	} else {
		sqls[5] = sqlWithErr{"insert tt set a = 'a_insert_1', c = 123", nil}
	}
	sqls[6] = sqlWithErr{"insert tt set a = 'a_insert_2'", nil}
	sqls[7] = sqlWithErr{"insert into tt select * from tt order by c limit 1 on duplicate key uFIDelate c = 44;", nil}
	sqls[8] = sqlWithErr{"insert ignore into tt values('a_insert_2', 2, 0), ('a_insert_ignore_1', 1, 123), ('a_insert_ignore_1', 1, 33)", nil}
	sqls[9] = sqlWithErr{"insert ignore into tt values('a_insert_ignore_2', 1, 123) on duplicate key uFIDelate c = 33 ", nil}
	sqls[10] = sqlWithErr{"insert ignore into tt values('a_insert_ignore_3', 1, 123) on duplicate key uFIDelate c = 66 ", nil}
	sqls[11] = sqlWithErr{"replace into tt values('a_replace_1', 55, 56)", nil}
	sqls[12] = sqlWithErr{"replace into tt values('a_replace_2', 77, 56)", nil}

	query := &expectQuery{allegrosql: "admin check block tt;", rows: nil}
	s.runTestInSchemaState(c, state, false, modifyDeferredCausetALLEGROSQL, sqls, query)
}

// TestWriteOnly tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestWriteOnly(c *C) {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t where c1 = 'a'", nil}
	sqls[1] = sqlWithErr{"uFIDelate t use index(idx2) set c1 = 'c1_uFIDelate' where c1 = 'a'", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2020-02-12', c4 = 1", nil}
	addDeferredCausetALLEGROSQL := "alter block t add defCausumn c5 int not null default 1 first"
	s.runTestInSchemaState(c, perceptron.StateWriteOnly, true, addDeferredCausetALLEGROSQL, sqls, nil)
}

// TestWriteOnlyForAddDeferredCausets tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestWriteOnlyForAddDeferredCausets(c *C) {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t where c1 = 'a'", nil}
	sqls[1] = sqlWithErr{"uFIDelate t use index(idx2) set c1 = 'c1_uFIDelate' where c1 = 'a'", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2020-02-12', c4 = 1", nil}
	addDeferredCausetsALLEGROSQL := "alter block t add defCausumn c5 int not null default 1 first, add defCausumn c6 int not null default 1"
	s.runTestInSchemaState(c, perceptron.StateWriteOnly, true, addDeferredCausetsALLEGROSQL, sqls, nil)
}

// TestDeleteOnly tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestDeleteOnly(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create block tt (c varchar(64), c4 int)`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tt (c, c4) values('a', 8)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block tt")

	sqls := make([]sqlWithErr, 5)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2020-02-12', c4 = 1",
		errors.Errorf("Can't find defCausumn c1")}
	sqls[1] = sqlWithErr{"uFIDelate t set c1 = 'c1_insert', c3 = '2020-02-12', c4 = 1",
		errors.Errorf("[planner:1054]Unknown defCausumn 'c1' in 'field list'")}
	sqls[2] = sqlWithErr{"delete from t where c1='a'",
		errors.Errorf("[planner:1054]Unknown defCausumn 'c1' in 'where clause'")}
	sqls[3] = sqlWithErr{"delete t, tt from tt inner join t on t.c4=tt.c4 where tt.c='a' and t.c1='a'",
		errors.Errorf("[planner:1054]Unknown defCausumn 't.c1' in 'where clause'")}
	sqls[4] = sqlWithErr{"delete t, tt from tt inner join t on t.c1=tt.c where tt.c='a'",
		errors.Errorf("[planner:1054]Unknown defCausumn 't.c1' in 'on clause'")}
	query := &expectQuery{allegrosql: "select * from t;", rows: []string{"N 2020-07-01 00:00:00 8"}}
	dropDeferredCausetALLEGROSQL := "alter block t drop defCausumn c1"
	s.runTestInSchemaState(c, perceptron.StateDeleteOnly, true, dropDeferredCausetALLEGROSQL, sqls, query)
}

// TestDeleteOnlyForDropExpressionIndex tests for deleting data when the hidden defCausumn is delete-only state.
func (s *serialTestStateChangeSuite) TestDeleteOnlyForDropExpressionIndex(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create block tt (a int, b int)`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `alter block tt add index expr_idx((a+1))`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tt (a, b) values(8, 8)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block tt")

	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"delete from tt where b=8", nil}
	dropIdxALLEGROSQL := "alter block tt drop index expr_idx"
	s.runTestInSchemaState(c, perceptron.StateDeleteOnly, true, dropIdxALLEGROSQL, sqls, nil)

	_, err = s.se.Execute(context.Background(), "admin check block tt")
	c.Assert(err, IsNil)
}

// TestDeleteOnlyForDropDeferredCausets tests whether the correct defCausumns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestDeleteOnlyForDropDeferredCausets(c *C) {
	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2020-02-12', c4 = 1",
		errors.Errorf("Can't find defCausumn c1")}
	dropDeferredCausetsALLEGROSQL := "alter block t drop defCausumn c1, drop defCausumn c3"
	s.runTestInSchemaState(c, perceptron.StateDeleteOnly, true, dropDeferredCausetsALLEGROSQL, sqls, nil)
}

func (s *testStateChangeSuite) TestWriteOnlyForDropDeferredCauset(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create block tt (c1 int, c4 int)`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tt (c1, c4) values(8, 8)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block tt")

	sqls := make([]sqlWithErr, 2)
	sqls[0] = sqlWithErr{"uFIDelate t set c1='5', c3='2020-03-01';", errors.New("[planner:1054]Unknown defCausumn 'c3' in 'field list'")}
	sqls[1] = sqlWithErr{"uFIDelate t t1, tt t2 set t1.c1='5', t1.c3='2020-03-01', t2.c1='10' where t1.c4=t2.c4",
		errors.New("[planner:1054]Unknown defCausumn 'c3' in 'field list'")}
	// TODO: Fix the case of sqls[2].
	// sqls[2] = sqlWithErr{"uFIDelate t set c1='5' where c3='2020-07-01';", errors.New("[planner:1054]Unknown defCausumn 'c3' in 'field list'")}
	dropDeferredCausetALLEGROSQL := "alter block t drop defCausumn c3"
	query := &expectQuery{allegrosql: "select * from t;", rows: []string{"a N 8"}}
	s.runTestInSchemaState(c, perceptron.StateWriteOnly, false, dropDeferredCausetALLEGROSQL, sqls, query)
}

func (s *testStateChangeSuite) TestWriteOnlyForDropDeferredCausets(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create block t_drop_defCausumns (c1 int, c4 int)`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into t_drop_defCausumns (c1, c4) values(8, 8)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block t_drop_defCausumns")

	sqls := make([]sqlWithErr, 2)
	sqls[0] = sqlWithErr{"uFIDelate t set c1='5', c3='2020-03-01';", errors.New("[planner:1054]Unknown defCausumn 'c3' in 'field list'")}
	sqls[1] = sqlWithErr{"uFIDelate t t1, t_drop_defCausumns t2 set t1.c1='5', t1.c3='2020-03-01', t2.c1='10' where t1.c4=t2.c4",
		errors.New("[planner:1054]Unknown defCausumn 'c3' in 'field list'")}
	// TODO: Fix the case of sqls[2].
	// sqls[2] = sqlWithErr{"uFIDelate t set c1='5' where c3='2020-07-01';", errors.New("[planner:1054]Unknown defCausumn 'c3' in 'field list'")}
	dropDeferredCausetsALLEGROSQL := "alter block t drop defCausumn c3, drop defCausumn c1"
	query := &expectQuery{allegrosql: "select * from t;", rows: []string{"N 8"}}
	s.runTestInSchemaState(c, perceptron.StateWriteOnly, false, dropDeferredCausetsALLEGROSQL, sqls, query)
}

func (s *testStateChangeSuiteBase) runTestInSchemaState(c *C, state perceptron.SchemaState, isOnJobUFIDelated bool, alterBlockALLEGROSQL string,
	sqlWithErrs []sqlWithErr, expectQuery *expectQuery) {
	_, err := s.se.Execute(context.Background(), `create block t (
	 	c1 varchar(64),
	 	c2 enum('N','Y') not null default 'N',
	 	c3 timestamp on uFIDelate current_timestamp,
	 	c4 int primary key,
	 	unique key idx2 (c2))`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block t")
	_, err = s.se.Execute(context.Background(), "insert into t values('a', 'N', '2020-07-01', 8)")
	c.Assert(err, IsNil)
	// Make sure these sqls use the the plan of index scan.
	_, err = s.se.Execute(context.Background(), "drop stats t")
	c.Assert(err, IsNil)

	callback := &dbs.TestDBSCallback{}
	prevState := perceptron.StateNone
	var checkErr error
	times := 0
	se, err := stochastik.CreateStochastik(s.causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	cbFunc := func(job *perceptron.Job) {
		if job.SchemaState == prevState || checkErr != nil || times >= 3 {
			return
		}
		times++
		if job.SchemaState != state {
			return
		}
		for _, sqlWithErr := range sqlWithErrs {
			_, err = se.Execute(context.Background(), sqlWithErr.allegrosql)
			if !terror.ErrorEqual(err, sqlWithErr.expectErr) {
				checkErr = errors.Errorf("allegrosql: %s, expect err: %v, got err: %v", sqlWithErr.allegrosql, sqlWithErr.expectErr, err)
				break
			}
		}
	}
	if isOnJobUFIDelated {
		callback.OnJobUFIDelatedExported = cbFunc
	} else {
		callback.OnJobRunBeforeExported = cbFunc
	}
	d := s.dom.DBS()
	originalCallback := d.GetHook()
	d.(dbs.DBSForTest).SetHook(callback)
	_, err = s.se.Execute(context.Background(), alterBlockALLEGROSQL)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	d.(dbs.DBSForTest).SetHook(originalCallback)

	if expectQuery != nil {
		tk := testkit.NewTestKit(c, s.causetstore)
		tk.MustExec("use test_db_state")
		result, err := s.execQuery(tk, expectQuery.allegrosql)
		c.Assert(err, IsNil)
		if expectQuery.rows == nil {
			c.Assert(result, IsNil)
			return
		}
		err = checkResult(result, testkit.Rows(expectQuery.rows...))
		c.Assert(err, IsNil)
	}
}

func (s *testStateChangeSuiteBase) execQuery(tk *testkit.TestKit, allegrosql string, args ...interface{}) (*testkit.Result, error) {
	comment := Commentf("allegrosql:%s, args:%v", allegrosql, args)
	rs, err := tk.Exec(allegrosql, args...)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	result := tk.ResultSetToResult(rs, comment)
	return result, nil
}

func checkResult(result *testkit.Result, expected [][]interface{}) error {
	got := fmt.Sprintf("%s", result.Rows())
	need := fmt.Sprintf("%s", expected)
	if got != need {
		return fmt.Errorf("need %v, but got %v", need, got)
	}
	return nil
}

func (s *testStateChangeSuiteBase) CheckResult(tk *testkit.TestKit, allegrosql string, args ...interface{}) (*testkit.Result, error) {
	comment := Commentf("allegrosql:%s, args:%v", allegrosql, args)
	rs, err := tk.Exec(allegrosql, args...)
	if err != nil {
		return nil, err
	}
	result := tk.ResultSetToResult(rs, comment)
	return result, nil
}

func (s *testStateChangeSuite) TestShowIndex(c *C) {
	_, err := s.se.Execute(context.Background(), `create block t(c1 int primary key, c2 int)`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block t")

	callback := &dbs.TestDBSCallback{}
	prevState := perceptron.StateNone
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test_db_state")
	showIndexALLEGROSQL := `show index from t`
	var checkErr error
	callback.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		if job.SchemaState == prevState || checkErr != nil {
			return
		}
		switch job.SchemaState {
		case perceptron.StateDeleteOnly, perceptron.StateWriteOnly, perceptron.StateWriteReorganization:
			result, err1 := s.execQuery(tk, showIndexALLEGROSQL)
			if err1 != nil {
				checkErr = err1
				break
			}
			checkErr = checkResult(result, testkit.Rows("t 0 PRIMARY 1 c1 A 0 <nil> <nil>  BTREE   YES NULL"))
		}
	}

	d := s.dom.DBS()
	originalCallback := d.GetHook()
	d.(dbs.DBSForTest).SetHook(callback)
	alterBlockALLEGROSQL := `alter block t add index c2(c2)`
	_, err = s.se.Execute(context.Background(), alterBlockALLEGROSQL)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")

	result, err := s.execQuery(tk, showIndexALLEGROSQL)
	c.Assert(err, IsNil)
	err = checkResult(result, testkit.Rows(
		"t 0 PRIMARY 1 c1 A 0 <nil> <nil>  BTREE   YES NULL",
		"t 1 c2 1 c2 A 0 <nil> <nil> YES BTREE   YES NULL",
	))
	c.Assert(err, IsNil)
	d.(dbs.DBSForTest).SetHook(originalCallback)

	c.Assert(err, IsNil)

	_, err = s.se.Execute(context.Background(), `create block tr(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2020)
   	);`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block tr")
	_, err = s.se.Execute(context.Background(), "create index idx1 on tr (purchased);")
	c.Assert(err, IsNil)
	result, err = s.execQuery(tk, "show index from tr;")
	c.Assert(err, IsNil)
	err = checkResult(result, testkit.Rows("tr 1 idx1 1 purchased A 0 <nil> <nil> YES BTREE   YES NULL"))
	c.Assert(err, IsNil)
}

func (s *testStateChangeSuite) TestParallelAlterModifyDeferredCauset(c *C) {
	allegrosql := "ALTER TABLE t MODIFY COLUMN b int FIRST;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
		_, err := s.se.Execute(context.Background(), "select * from t")
		c.Assert(err, IsNil)
	}
	s.testControlParallelExecALLEGROSQL(c, allegrosql, allegrosql, f)
}

func (s *serialTestStateChangeSuite) TestParallelAlterModifyDeferredCausetAndAddPK(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
	})

	_, err := s.se.Execute(context.Background(), "set global milevadb_enable_change_defCausumn_type = 1")
	c.Assert(err, IsNil)
	defer func() {
		_, err = s.se.Execute(context.Background(), "set global milevadb_enable_change_defCausumn_type = 0")
		c.Assert(err, IsNil)
	}()
	petri.GetPetri(s.se).GetGlobalVarsCache().Disable()

	sql1 := "ALTER TABLE t ADD PRIMARY KEY (b);"
	sql2 := "ALTER TABLE t MODIFY COLUMN b tinyint;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: milevadb_enable_change_defCausumn_type is true and this defCausumn has primary key flag")
		_, err := s.se.Execute(context.Background(), "select * from t")
		c.Assert(err, IsNil)
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

// TODO: This test is not a test that performs two DBSs in parallel.
// So we should not use the function of testControlParallelExecALLEGROSQL. We will handle this test in the next PR.
// func (s *testStateChangeSuite) TestParallelDeferredCausetModifyingDefinition(c *C) {
// 	sql1 := "insert into t(b) values (null);"
// 	sql2 := "alter block t change b b2 bigint not null;"
// 	f := func(c *C, err1, err2 error) {
// 		c.Assert(err1, IsNil)
// 		if err2 != nil {
// 			c.Assert(err2.Error(), Equals, "[dbs:1265]Data truncated for defCausumn 'b2' at event 1")
// 		}
// 	}
// 	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
// }

func (s *testStateChangeSuite) TestParallelAddDefCausumAndSetDefaultValue(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create block tx (
		c1 varchar(64),
		c2 enum('N','Y') not null default 'N',
		primary key idx2 (c2, c1))`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tx values('a', 'N')")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop block tx")

	sql1 := "alter block tx add defCausumn cx int after c1"
	sql2 := "alter block tx alter c2 set default 'N'"

	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
		_, err := s.se.Execute(context.Background(), "delete from tx where c1='a'")
		c.Assert(err, IsNil)
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelChangeDeferredCausetName(c *C) {
	sql1 := "ALTER TABLE t CHANGE a aa int;"
	sql2 := "ALTER TABLE t CHANGE b aa int;"
	f := func(c *C, err1, err2 error) {
		// Make sure only a DBS encounters the error of 'duplicate defCausumn name'.
		var oneErr error
		if (err1 != nil && err2 == nil) || (err1 == nil && err2 != nil) {
			if err1 != nil {
				oneErr = err1
			} else {
				oneErr = err2
			}
		}
		c.Assert(oneErr.Error(), Equals, "[schemaReplicant:1060]Duplicate defCausumn name 'aa'")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelAlterAddIndex(c *C) {
	sql1 := "ALTER TABLE t add index index_b(b);"
	sql2 := "CREATE INDEX index_b ON t (c);"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[dbs:1061]index already exist index_b")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

func (s *serialTestStateChangeSuite) TestParallelAlterAddExpressionIndex(c *C) {
	sql1 := "ALTER TABLE t add index expr_index_b((b+1));"
	sql2 := "CREATE INDEX expr_index_b ON t ((c+1));"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[dbs:1061]index already exist expr_index_b")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelAddPrimaryKey(c *C) {
	sql1 := "ALTER TABLE t add primary key index_b(b);"
	sql2 := "ALTER TABLE t add primary key index_b(c);"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[schemaReplicant:1068]Multiple primary key defined")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelAlterAddPartition(c *C) {
	sql1 := `alter block t_part add partition (
    partition p2 values less than (30)
   );`
	sql2 := `alter block t_part add partition (
    partition p3 values less than (30)
   );`
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[dbs:1493]VALUES LESS THAN value must be strictly increasing for each partition")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelDropDeferredCauset(c *C) {
	allegrosql := "ALTER TABLE t drop COLUMN c ;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[dbs:1091]defCausumn c doesn't exist")
	}
	s.testControlParallelExecALLEGROSQL(c, allegrosql, allegrosql, f)
}

func (s *testStateChangeSuite) TestParallelDropDeferredCausets(c *C) {
	allegrosql := "ALTER TABLE t drop COLUMN b, drop COLUMN c;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[dbs:1091]defCausumn b doesn't exist")
	}
	s.testControlParallelExecALLEGROSQL(c, allegrosql, allegrosql, f)
}

func (s *testStateChangeSuite) TestParallelDropIfExistsDeferredCausets(c *C) {
	allegrosql := "ALTER TABLE t drop COLUMN if exists b, drop COLUMN if exists c;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
	}
	s.testControlParallelExecALLEGROSQL(c, allegrosql, allegrosql, f)
}

func (s *testStateChangeSuite) TestParallelDropIndex(c *C) {
	sql1 := "alter block t drop index idx1 ;"
	sql2 := "alter block t drop index idx2 ;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[autoid:1075]Incorrect block definition; there can be only one auto defCausumn and it must be defined as a key")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelDropPrimaryKey(c *C) {
	s.preALLEGROSQL = "ALTER TABLE t add primary key index_b(c);"
	defer func() {
		s.preALLEGROSQL = ""
	}()
	sql1 := "alter block t drop primary key;"
	sql2 := "alter block t drop primary key;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[dbs:1091]index PRIMARY doesn't exist")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelCreateAndRename(c *C) {
	sql1 := "create block t_exists(c int);"
	sql2 := "alter block t rename to t_exists;"
	defer s.se.Execute(context.Background(), "drop block t_exists")
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[schemaReplicant:1050]Block 't_exists' already exists")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelAlterAndDropSchema(c *C) {
	_, err := s.se.Execute(context.Background(), "create database db_drop_db")
	c.Assert(err, IsNil)
	sql1 := "DROP SCHEMA db_drop_db"
	sql2 := "ALTER SCHEMA db_drop_db CHARSET utf8mb4 COLLATE utf8mb4_general_ci"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, NotNil)
		c.Assert(err2.Error(), Equals, "[schemaReplicant:1008]Can't drop database ''; database doesn't exist")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

type checkRet func(c *C, err1, err2 error)

func (s *testStateChangeSuiteBase) prepareTestControlParallelExecALLEGROSQL(c *C) (stochastik.Stochastik, stochastik.Stochastik, chan struct{}, dbs.Callback) {
	callback := &dbs.TestDBSCallback{}
	times := 0
	callback.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		if times != 0 {
			return
		}
		var qLen int
		for {
			ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
				jobs, err1 := admin.GetDBSJobs(txn)
				if err1 != nil {
					return err1
				}
				qLen = len(jobs)
				return nil
			})
			if qLen == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		times++
	}
	d := s.dom.DBS()
	originalCallback := d.GetHook()
	d.(dbs.DBSForTest).SetHook(callback)

	se, err := stochastik.CreateStochastik(s.causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	se1, err := stochastik.CreateStochastik(s.causetstore)
	c.Assert(err, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	ch := make(chan struct{})
	// Make sure the sql1 is put into the DBSJobQueue.
	go func() {
		var qLen int
		for {
			ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
				jobs, err3 := admin.GetDBSJobs(txn)
				if err3 != nil {
					return err3
				}
				qLen = len(jobs)
				return nil
			})
			if qLen == 1 {
				// Make sure sql2 is executed after the sql1.
				close(ch)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return se, se1, ch, originalCallback
}

func (s *testStateChangeSuiteBase) testControlParallelExecALLEGROSQL(c *C, sql1, sql2 string, f checkRet) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create block t(a int, b int, c int, d int auto_increment,e int, index idx1(d), index idx2(d,e))")
	c.Assert(err, IsNil)
	if len(s.preALLEGROSQL) != 0 {
		_, err := s.se.Execute(context.Background(), s.preALLEGROSQL)
		c.Assert(err, IsNil)
	}
	defer s.se.Execute(context.Background(), "drop block t")

	_, err = s.se.Execute(context.Background(), "drop database if exists t_part")
	c.Assert(err, IsNil)
	s.se.Execute(context.Background(), `create block t_part (a int key)
	 partition by range(a) (
	 partition p0 values less than (10),
	 partition p1 values less than (20)
	 );`)

	se, se1, ch, originalCallback := s.prepareTestControlParallelExecALLEGROSQL(c)
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originalCallback)

	var err1 error
	var err2 error
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err1 = se.Execute(context.Background(), sql1)
	}()
	go func() {
		defer wg.Done()
		<-ch
		_, err2 = se1.Execute(context.Background(), sql2)
	}()

	wg.Wait()
	f(c, err1, err2)
}

func (s *serialTestStateChangeSuite) TestParallelUFIDelateBlockReplica(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant/mockTiFlashStoreCount")

	ctx := context.Background()
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(ctx, "drop block if exists t1;")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(ctx, "create block t1 (a int);")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(ctx, "alter block t1 set tiflash replica 3 location labels 'a','b';")
	c.Assert(err, IsNil)

	se, se1, ch, originalCallback := s.prepareTestControlParallelExecALLEGROSQL(c)
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originalCallback)

	t1 := testGetBlockByName(c, se, "test_db_state", "t1")

	var err1 error
	var err2 error
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Mock for block tiflash replica was available.
		err1 = petri.GetPetri(se).DBS().UFIDelateBlockReplicaInfo(se, t1.Meta().ID, true)
	}()
	go func() {
		defer wg.Done()
		<-ch
		// Mock for block tiflash replica was available.
		err2 = petri.GetPetri(se1).DBS().UFIDelateBlockReplicaInfo(se1, t1.Meta().ID, true)
	}()
	wg.Wait()
	c.Assert(err1, IsNil)
	c.Assert(err2.Error(), Equals, "[dbs:-1]the replica available status of block t1 is already uFIDelated")
}

func (s *testStateChangeSuite) testParallelExecALLEGROSQL(c *C, allegrosql string) {
	se, err := stochastik.CreateStochastik(s.causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	se1, err1 := stochastik.CreateStochastik(s.causetstore)
	c.Assert(err1, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	var err2, err3 error
	wg := sync.WaitGroup{}

	callback := &dbs.TestDBSCallback{}
	once := sync.Once{}
	callback.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		// sleep a while, let other job enqueue.
		once.Do(func() {
			time.Sleep(time.Millisecond * 10)
		})
	}

	d := s.dom.DBS()
	originalCallback := d.GetHook()
	defer d.(dbs.DBSForTest).SetHook(originalCallback)
	d.(dbs.DBSForTest).SetHook(callback)

	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err2 = se.Execute(context.Background(), allegrosql)
	}()

	go func() {
		defer wg.Done()
		_, err3 = se1.Execute(context.Background(), allegrosql)
	}()
	wg.Wait()
	c.Assert(err2, IsNil)
	c.Assert(err3, IsNil)
}

// TestCreateBlockIfNotExists parallel exec create block if not exists xxx. No error returns is expected.
func (s *testStateChangeSuite) TestCreateBlockIfNotExists(c *C) {
	defer s.se.Execute(context.Background(), "drop block test_not_exists")
	s.testParallelExecALLEGROSQL(c, "create block if not exists test_not_exists(a int);")
}

// TestCreateDBIfNotExists parallel exec create database if not exists xxx. No error returns is expected.
func (s *testStateChangeSuite) TestCreateDBIfNotExists(c *C) {
	defer s.se.Execute(context.Background(), "drop database test_not_exists")
	s.testParallelExecALLEGROSQL(c, "create database if not exists test_not_exists;")
}

// TestDBSIfNotExists parallel exec some DBSs with `if not exists` clause. No error returns is expected.
func (s *testStateChangeSuite) TestDBSIfNotExists(c *C) {
	defer s.se.Execute(context.Background(), "drop block test_not_exists")
	_, err := s.se.Execute(context.Background(), "create block if not exists test_not_exists(a int)")
	c.Assert(err, IsNil)

	// ADD COLUMN
	s.testParallelExecALLEGROSQL(c, "alter block test_not_exists add defCausumn if not exists b int")

	// ADD COLUMNS
	s.testParallelExecALLEGROSQL(c, "alter block test_not_exists add defCausumn if not exists (c11 int, d11 int)")

	// ADD INDEX
	s.testParallelExecALLEGROSQL(c, "alter block test_not_exists add index if not exists idx_b (b)")

	// CREATE INDEX
	s.testParallelExecALLEGROSQL(c, "create index if not exists idx_b on test_not_exists (b)")
}

// TestDBSIfExists parallel exec some DBSs with `if exists` clause. No error returns is expected.
func (s *testStateChangeSuite) TestDBSIfExists(c *C) {
	defer func() {
		s.se.Execute(context.Background(), "drop block test_exists")
		s.se.Execute(context.Background(), "drop block test_exists_2")
	}()
	_, err := s.se.Execute(context.Background(), "create block if not exists test_exists (a int key, b int)")
	c.Assert(err, IsNil)

	// DROP COLUMNS
	s.testParallelExecALLEGROSQL(c, "alter block test_exists drop defCausumn if exists c, drop defCausumn if exists d")

	// DROP COLUMN
	s.testParallelExecALLEGROSQL(c, "alter block test_exists drop defCausumn if exists b") // only `a` exists now

	// CHANGE COLUMN
	s.testParallelExecALLEGROSQL(c, "alter block test_exists change defCausumn if exists a c int") // only, `c` exists now

	// MODIFY COLUMN
	s.testParallelExecALLEGROSQL(c, "alter block test_exists modify defCausumn if exists a bigint")

	// DROP INDEX
	_, err = s.se.Execute(context.Background(), "alter block test_exists add index idx_c (c)")
	c.Assert(err, IsNil)
	s.testParallelExecALLEGROSQL(c, "alter block test_exists drop index if exists idx_c")

	// DROP PARTITION (ADD PARTITION tested in TestParallelAlterAddPartition)
	_, err = s.se.Execute(context.Background(), "create block test_exists_2 (a int key) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	c.Assert(err, IsNil)
	s.testParallelExecALLEGROSQL(c, "alter block test_exists_2 drop partition if exists p1")
}

// TestParallelDBSBeforeRunDBSJob tests a stochastik to execute DBS with an outdated information schemaReplicant.
// This test is used to simulate the following conditions:
// In a cluster, MilevaDB "a" executes the DBS.
// MilevaDB "b" fails to load schemaReplicant, then MilevaDB "b" executes the DBS statement associated with the DBS statement executed by "a".
func (s *testStateChangeSuite) TestParallelDBSBeforeRunDBSJob(c *C) {
	defer s.se.Execute(context.Background(), "drop block test_block")
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create block test_block (c1 int, c2 int default 1, index (c1))")
	c.Assert(err, IsNil)

	// Create two stochastik.
	se, err := stochastik.CreateStochastik(s.causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	se1, err := stochastik.CreateStochastik(s.causetstore)
	c.Assert(err, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	intercept := &dbs.TestInterceptor{}
	firstConnID := uint64(1)
	finishedCnt := int32(0)
	interval := 5 * time.Millisecond
	var stochastikCnt int32 // stochastikCnt is the number of stochastik that goes into the function of OnGetSchemaReplicant.
	intercept.OnGetSchemaReplicantExported = func(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant) schemareplicant.SchemaReplicant {
		// The following code is for testing.
		// Make sure the two stochastik get the same information schemaReplicant before executing DBS.
		// After the first stochastik executes its DBS, then the second stochastik executes its DBS.
		var info schemareplicant.SchemaReplicant
		atomic.AddInt32(&stochastikCnt, 1)
		for {
			// Make sure there are two stochastik running here.
			if atomic.LoadInt32(&stochastikCnt) == 2 {
				info = is
				break
			}
			// Print log to notify if TestParallelDBSBeforeRunDBSJob hang up
			log.Info("sleep in TestParallelDBSBeforeRunDBSJob", zap.String("interval", interval.String()))
			time.Sleep(interval)
		}

		currID := ctx.GetStochaseinstein_dbars().ConnectionID
		for {
			seCnt := atomic.LoadInt32(&stochastikCnt)
			// Make sure the two stochastik have got the same information schemaReplicant. And the first stochastik can continue to go on,
			// or the first stochastik finished this ALLEGROALLEGROSQL(seCnt = finishedCnt), then other stochastik can continue to go on.
			if currID == firstConnID || seCnt == finishedCnt {
				break
			}
			// Print log to notify if TestParallelDBSBeforeRunDBSJob hang up
			log.Info("sleep in TestParallelDBSBeforeRunDBSJob", zap.String("interval", interval.String()))
			time.Sleep(interval)
		}

		return info
	}
	d := s.dom.DBS()
	d.(dbs.DBSForTest).SetInterceptor(intercept)

	// Make sure the connection 1 executes a ALLEGROALLEGROSQL before the connection 2.
	// And the connection 2 executes a ALLEGROALLEGROSQL with an outdated information schemaReplicant.
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()

		se.SetConnectionID(firstConnID)
		_, err1 := se.Execute(context.Background(), "alter block test_block drop defCausumn c2")
		c.Assert(err1, IsNil)
		// Sleep a while to make sure the connection 2 break out the first for loop in OnGetSchemaReplicantExported, otherwise atomic.LoadInt32(&stochastikCnt) == 2 will be false forever.
		time.Sleep(100 * time.Millisecond)
		atomic.StoreInt32(&stochastikCnt, finishedCnt)
	}()
	go func() {
		defer wg.Done()

		se1.SetConnectionID(2)
		_, err2 := se1.Execute(context.Background(), "alter block test_block add defCausumn c2 int")
		c.Assert(err2, NotNil)
		c.Assert(strings.Contains(err2.Error(), "Information schemaReplicant is changed"), IsTrue)
	}()

	wg.Wait()

	intercept = &dbs.TestInterceptor{}
	d.(dbs.DBSForTest).SetInterceptor(intercept)
}

func (s *testStateChangeSuite) TestParallelAlterSchemaCharsetAndDefCauslate(c *C) {
	allegrosql := "ALTER SCHEMA test_db_state CHARSET utf8mb4 COLLATE utf8mb4_general_ci"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
	}
	s.testControlParallelExecALLEGROSQL(c, allegrosql, allegrosql, f)
	allegrosql = `SELECT default_character_set_name, default_defCauslation_name
			FROM information_schema.schemata
			WHERE schema_name='test_db_state'`
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery(allegrosql).Check(testkit.Rows("utf8mb4 utf8mb4_general_ci"))
}

// TestParallelTruncateBlockAndAddDeferredCauset tests add defCausumn when truncate block.
func (s *testStateChangeSuite) TestParallelTruncateBlockAndAddDeferredCauset(c *C) {
	sql1 := "truncate block t"
	sql2 := "alter block t add defCausumn c3 int"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, NotNil)
		c.Assert(err2.Error(), Equals, "[petri:8028]Information schemaReplicant is changed during the execution of the statement(for example, block definition may be uFIDelated by other DBS ran in parallel). If you see this error often, try increasing `milevadb_max_delta_schema_count`. [try again later]")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

// TestParallelTruncateBlockAndAddDeferredCausets tests add defCausumns when truncate block.
func (s *testStateChangeSuite) TestParallelTruncateBlockAndAddDeferredCausets(c *C) {
	sql1 := "truncate block t"
	sql2 := "alter block t add defCausumn c3 int, add defCausumn c4 int"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, NotNil)
		c.Assert(err2.Error(), Equals, "[petri:8028]Information schemaReplicant is changed during the execution of the statement(for example, block definition may be uFIDelated by other DBS ran in parallel). If you see this error often, try increasing `milevadb_max_delta_schema_count`. [try again later]")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

// TestParallelFlashbackBlock tests parallel flashback block.
func (s *serialTestStateChangeSuite) TestParallelFlashbackBlock(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func(originGC bool) {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid/mockAutoIDChange"), IsNil)
		if originGC {
			dbs.EmulatorGCEnable()
		} else {
			dbs.EmulatorGCDisable()
		}
	}(dbs.IsEmulatorGCEnable())

	// disable emulator GC.
	// Disable emulator GC, otherwise, emulator GC will delete block record as soon as possible after executing drop block DBS.
	dbs.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointALLEGROSQL := `INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('einsteindb_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UFIDelATE variable_value = '%[1]s'`
	tk := testkit.NewTestKit(c, s.causetstore)
	// clear GC variables first.
	tk.MustExec("delete from allegrosql.milevadb where variable_name in ( 'einsteindb_gc_safe_point','einsteindb_gc_enable' )")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))
	// set GC enable.
	err := gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// prepare dropped block.
	tk.MustExec("use test_db_state")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (a int);")
	tk.MustExec("drop block if exists t")
	// Test parallel flashback block.
	sql1 := "flashback block t to t_flashback"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, NotNil)
		c.Assert(err2.Error(), Equals, "[schemaReplicant:1050]Block 't_flashback' already exists")
	}
	s.testControlParallelExecALLEGROSQL(c, sql1, sql1, f)

	// Test parallel flashback block with different name
	tk.MustExec("drop block t_flashback")
	sql1 = "flashback block t_flashback"
	sql2 := "flashback block t_flashback to t_flashback2"
	s.testControlParallelExecALLEGROSQL(c, sql1, sql2, f)
}

// TestModifyDeferredCausetTypeArgs test job raw args won't be uFIDelated when error occurs in `uFIDelateVersionAndBlockInfo`.
func (s *serialTestStateChangeSuite) TestModifyDeferredCausetTypeArgs(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockUFIDelateVersionAndBlockInfoErr", `return(2)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockUFIDelateVersionAndBlockInfoErr"), IsNil)
	}()

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t_modify_defCausumn_args")
	tk.MustExec("create block t_modify_defCausumn_args(a int, unique(a))")

	enableChangeDeferredCausetType := tk.Se.GetStochaseinstein_dbars().EnableChangeDeferredCausetType
	tk.Se.GetStochaseinstein_dbars().EnableChangeDeferredCausetType = true
	defer func() {
		tk.Se.GetStochaseinstein_dbars().EnableChangeDeferredCausetType = enableChangeDeferredCausetType
	}()

	_, err := tk.Exec("alter block t_modify_defCausumn_args modify defCausumn a tinyint")
	c.Assert(err, NotNil)
	// error goes like `mock uFIDelate version and blockInfo error,jobID=xx`
	strs := strings.Split(err.Error(), ",")
	c.Assert(strs[0], Equals, "[dbs:-1]mock uFIDelate version and blockInfo error")
	jobID := strings.Split(strs[1], "=")[1]

	tbl := testGetBlockByName(c, tk.Se, "test", "t_modify_defCausumn_args")
	c.Assert(len(tbl.Meta().DeferredCausets), Equals, 1)
	c.Assert(len(tbl.Meta().Indices), Equals, 1)

	ID, err := strconv.Atoi(jobID)
	c.Assert(err, IsNil)
	var historyJob *perceptron.Job
	err = ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err = t.GetHistoryDBSJob(int64(ID))
		if err != nil {
			return err
		}
		return nil
	})
	c.Assert(err, IsNil)
	c.Assert(historyJob, NotNil)

	var (
		newDefCaus               *perceptron.DeferredCausetInfo
		oldDefCausName           *perceptron.CIStr
		modifyDeferredCausetTp   byte
		uFIDelatedAutoRandomBits uint64
		changingDefCaus          *perceptron.DeferredCausetInfo
		changingIdxs             []*perceptron.IndexInfo
	)
	pos := &ast.DeferredCausetPosition{}
	err = historyJob.DecodeArgs(&newDefCaus, &oldDefCausName, pos, &modifyDeferredCausetTp, &uFIDelatedAutoRandomBits, &changingDefCaus, &changingIdxs)
	c.Assert(err, IsNil)
	c.Assert(changingDefCaus, IsNil)
	c.Assert(changingIdxs, IsNil)
}
