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

package core_test

import (
	"context"
	"fmt"

	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/executor"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/hint"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testleak"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
)

var _ = Suite(&testPlanSuite{})
var _ = SerialSuites(&testPlanSerialSuite{})

type testPlanSuiteBase struct {
	*berolinaAllegroSQL.berolinaAllegroSQL
	is schemareplicant.SchemaReplicant
}

func (s *testPlanSuiteBase) SetUpSuite(c *C) {
	s.is = schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{core.MockSignedBlock(), core.MockUnsignedBlock()})
	s.berolinaAllegroSQL = berolinaAllegroSQL.New()
	s.berolinaAllegroSQL.EnableWindowFunc(true)
}

type testPlanSerialSuite struct {
	testPlanSuiteBase
}

type testPlanSuite struct {
	testPlanSuiteBase

	testData solitonutil.TestData
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.testPlanSuiteBase.SetUpSuite(c)

	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "plan_suite")
	c.Assert(err, IsNil)
}

func (s *testPlanSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testPlanSuite) TestPosetDagPlanBuilderSimpleCase(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestPosetDagPlanBuilderJoin(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	ctx := se.(stochastikctx.Context)
	stochaseinstein_dbars := ctx.GetStochaseinstein_dbars()
	stochaseinstein_dbars.ExecutorConcurrency = 4
	stochaseinstein_dbars.SetDistALLEGROSQLScanConcurrency(15)
	stochaseinstein_dbars.SetHashJoinConcurrency(5)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestPosetDagPlanBuilderSubquery(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	ctx := se.(stochastikctx.Context)
	stochaseinstein_dbars := ctx.GetStochaseinstein_dbars()
	stochaseinstein_dbars.SetHashAggFinalConcurrency(1)
	stochaseinstein_dbars.SetHashAggPartialConcurrency(1)
	stochaseinstein_dbars.SetHashJoinConcurrency(5)
	stochaseinstein_dbars.SetDistALLEGROSQLScanConcurrency(15)
	stochaseinstein_dbars.ExecutorConcurrency = 4
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestPosetDagPlanTopN(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestPosetDagPlanBuilderBasePhysicalPlan(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)

	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		core.Preprocess(se, stmt, s.is)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestPosetDagPlanBuilderUnion(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestPosetDagPlanBuilderUnionScan(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		// Make txn not read only.
		txn, err := se.Txn(true)
		c.Assert(err, IsNil)
		txn.Set(ekv.Key("AAA"), []byte("BBB"))
		se.StmtCommit()
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestPosetDagPlanBuilderAgg(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	se.Execute(context.Background(), "use test")
	se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	ctx := se.(stochastikctx.Context)
	stochaseinstein_dbars := ctx.GetStochaseinstein_dbars()
	stochaseinstein_dbars.SetHashAggFinalConcurrency(1)
	stochaseinstein_dbars.SetHashAggPartialConcurrency(1)
	stochaseinstein_dbars.SetDistALLEGROSQLScanConcurrency(15)
	stochaseinstein_dbars.ExecutorConcurrency = 4

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testPlanSuite) TestRefine(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(stochastikctx.Context).GetStochaseinstein_dbars().StmtCtx
		sc.IgnoreTruncate = false
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestAggEliminator(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(stochastikctx.Context).GetStochaseinstein_dbars().StmtCtx
		sc.IgnoreTruncate = false
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

type overrideStore struct{ ekv.CausetStorage }

func (causetstore overrideStore) GetClient() ekv.Client {
	cli := causetstore.CausetStorage.GetClient()
	return overrideClient{cli}
}

type overrideClient struct{ ekv.Client }

func (cli overrideClient) IsRequestTypeSupported(reqType, subType int64) bool {
	return false
}

func (s *testPlanSuite) TestRequestTypeSupportedOff(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(overrideStore{causetstore})
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	allegrosql := "select * from t where a in (1, 10, 20)"
	expect := "BlockReader(Block(t))->Sel([in(test.t.a, 1, 10, 20)])"

	stmt, err := s.ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil)
	p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
	c.Assert(err, IsNil)
	c.Assert(core.ToString(p), Equals, expect, Commentf("for %s", allegrosql))
}

func (s *testPlanSuite) TestIndexJoinUnionScan(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk.MustExec("use test")
	var input [][]string
	var output []struct {
		ALLEGROALLEGROSQL []string
		Plan              []string
	}
	tk.MustExec("create block t (a int primary key, b int, index idx(a))")
	tk.MustExec("create block tt (a int primary key) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200))")

	tk.MustExec(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)

	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		tk.MustExec("begin")
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				output[i].ALLEGROALLEGROSQL = ts
				if j == len(ts)-1 {
					output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			}
		}
		tk.MustExec("rollback")
	}
}

func (s *testPlanSuite) TestDoSubquery(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	tests := []struct {
		allegrosql string
		best       string
	}{
		{
			allegrosql: "do 1 in (select a from t)",
			best:       "LeftHashJoin{Dual->PointGet(Handle(t.a)1)}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.allegrosql)
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestIndexLookupCartesianJoin(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	allegrosql := "select /*+ MilevaDB_INLJ(t1, t2) */ * from t t1 join t t2"
	stmt, err := s.ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil)
	p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
	c.Assert(err, IsNil)
	c.Assert(core.ToString(p), Equals, "LeftHashJoin{BlockReader(Block(t))->BlockReader(Block(t))}")
	warnings := se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
	lastWarn := warnings[len(warnings)-1]
	err = core.ErrInternal.GenWithStack("MilevaDB_INLJ hint is inapplicable without column equal ON condition")
	c.Assert(terror.ErrorEqual(err, lastWarn.Err), IsTrue)
}

func (s *testPlanSuite) TestSemiJoinToInner(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best)
	}
}

func (s *testPlanSuite) TestUnmatchedBlockInHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Warning           string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, test := range input {
		se.GetStochaseinstein_dbars().StmtCtx.SetWarnings(nil)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil)
		_, _, err = planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0)
		} else {
			c.Assert(len(warnings), Equals, 1)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning)
		}
	}
}

func (s *testPlanSuite) TestHintSINTERLOCKe(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(context.Background(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best)

		warnings := se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		c.Assert(warnings, HasLen, 0, comment)
	}
}

func (s *testPlanSuite) TestJoinHints(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		Warning           string
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		se.GetStochaseinstein_dbars().StmtCtx.SetWarnings(nil)
		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()

		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			output[i].Best = core.ToString(p)
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Best)
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0)
		} else {
			c.Assert(len(warnings), Equals, 1, Commentf("%v", warnings))
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning)
		}
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestAggregationHints(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	stochaseinstein_dbars := se.(stochastikctx.Context).GetStochaseinstein_dbars()
	stochaseinstein_dbars.SetHashAggFinalConcurrency(1)
	stochaseinstein_dbars.SetHashAggPartialConcurrency(1)

	var input []struct {
		ALLEGROALLEGROSQL string
		AggPushDown       bool
	}
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		Warning           string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		se.GetStochaseinstein_dbars().StmtCtx.SetWarnings(nil)
		se.GetStochaseinstein_dbars().AllowAggPushDown = test.AggPushDown

		stmt, err := s.ParseOneStmt(test.ALLEGROALLEGROSQL, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()

		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test.ALLEGROALLEGROSQL
			output[i].Best = core.ToString(p)
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning, comment)
		}
	}
}

func (s *testPlanSuite) TestAggToINTERLOCKHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists ta")
	tk.MustExec("create block ta(a int, b int, index(a))")

	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Best              string
			Warning           string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	is := petri.GetPetri(tk.Se).SchemaReplicant()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
		})
		c.Assert(test, Equals, output[i].ALLEGROALLEGROSQL, comment)

		tk.Se.GetStochaseinstein_dbars().StmtCtx.SetWarnings(nil)

		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, tk.Se, stmt, is)
		c.Assert(err, IsNil)
		planString := core.ToString(p)
		s.testData.OnRecord(func() {
			output[i].Best = planString
		})
		c.Assert(planString, Equals, output[i].Best, comment)

		warnings := tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		s.testData.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning, comment)
		}
	}
}

func (s *testPlanSuite) TestLimitToINTERLOCKHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists tn")
	tk.MustExec("create block tn(a int, b int, c int, d int, key (a, b, c, d))")

	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Plan              []string
			Warning           string
		}
	)

	s.testData.GetTestCases(c, &input, &output)

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Plan...))

		comment := Commentf("case:%v allegrosql:%s", i, ts)
		warnings := tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		s.testData.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning, comment)
		}
	}
}

func (s *testPlanSuite) TestPushdownDistinctEnable(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Plan              []string
			Result            []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@stochastik.%s = 1", variable.MilevaDBOptDistinctAggPushDown),
		"set stochastik milevadb_opt_agg_push_down = 1",
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testPlanSuite) TestPushdownDistinctDisable(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Plan              []string
			Result            []string
		}
	)

	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@stochastik.%s = 0", variable.MilevaDBOptDistinctAggPushDown),
		"set stochastik milevadb_opt_agg_push_down = 1",
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testPlanSuite) TestPushdownDistinctEnableAggPushDownDisable(c *C) {
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Plan              []string
			Result            []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@stochastik.%s = 1", variable.MilevaDBOptDistinctAggPushDown),
		"set stochastik milevadb_opt_agg_push_down = 0",
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testPlanSuite) doTestPushdownDistinct(c *C, vars, input []string, output []struct {
	ALLEGROALLEGROSQL string
	Plan              []string
	Result            []string
}) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int, b int, c int, index(c))")
	tk.MustExec("insert into t values (1, 1, 1), (1, 1, 3), (1, 2, 3), (2, 1, 3), (1, 2, NULL);")

	tk.MustExec("drop block if exists pt")
	tk.MustExec(`CREATE TABLE pt (a int, b int) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (2),
		PARTITION p1 VALUES LESS THAN (100)
	);`)

	tk.MustExec("drop block if exists ta")
	tk.MustExec("create block ta(a int);")
	tk.MustExec("insert into ta values(1), (1);")
	tk.MustExec("drop block if exists tb")
	tk.MustExec("create block tb(a int);")
	tk.MustExec("insert into tb values(1), (1);")

	tk.MustExec("set stochastik sql_mode=''")
	tk.MustExec(fmt.Sprintf("set stochastik %s=1", variable.MilevaDBHashAggPartialConcurrency))
	tk.MustExec(fmt.Sprintf("set stochastik %s=1", variable.MilevaDBHashAggFinalConcurrency))

	tk.MustExec(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)

	for _, v := range vars {
		tk.MustExec(v)
	}

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPlanSuite) TestGroupConcatOrderby(c *C) {
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Plan              []string
			Result            []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists test;")
	tk.MustExec("create block test(id int, name int)")
	tk.MustExec("insert into test values(1, 10);")
	tk.MustExec("insert into test values(1, 20);")
	tk.MustExec("insert into test values(1, 30);")
	tk.MustExec("insert into test values(2, 20);")
	tk.MustExec("insert into test values(3, 200);")
	tk.MustExec("insert into test values(3, 500);")

	tk.MustExec("drop block if exists ptest;")
	tk.MustExec("CREATE TABLE ptest (id int,name int) PARTITION BY RANGE ( id ) " +
		"(PARTITION `p0` VALUES LESS THAN (2), PARTITION `p1` VALUES LESS THAN (11))")
	tk.MustExec("insert into ptest select * from test;")
	tk.MustExec(fmt.Sprintf("set stochastik milevadb_opt_distinct_agg_push_down = %v", 1))
	tk.MustExec(fmt.Sprintf("set stochastik milevadb_opt_agg_push_down = %v", 1))

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPlanSuite) TestHintAlias(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	tests := []struct {
		sql1 string
		sql2 string
	}{
		{
			sql1: "select /*+ MilevaDB_SMJ(t1) */ t1.a, t1.b from t t1, (select /*+ MilevaDB_INLJ(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ MERGE_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ INL_JOIN(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ MilevaDB_HJ(t1) */ t1.a, t1.b from t t1, (select /*+ MilevaDB_SMJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ HASH_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ MERGE_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ MilevaDB_INLJ(t1) */ t1.a, t1.b from t t1, (select /*+ MilevaDB_HJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ INL_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ HASH_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql1:%s sql2:%s", i, tt.sql1, tt.sql2)
		stmt1, err := s.ParseOneStmt(tt.sql1, "", "")
		c.Assert(err, IsNil, comment)
		stmt2, err := s.ParseOneStmt(tt.sql2, "", "")
		c.Assert(err, IsNil, comment)

		p1, _, err := planner.Optimize(ctx, se, stmt1, s.is)
		c.Assert(err, IsNil)
		p2, _, err := planner.Optimize(ctx, se, stmt2, s.is)
		c.Assert(err, IsNil)

		c.Assert(core.ToString(p1), Equals, core.ToString(p2))
	}
}

func (s *testPlanSuite) TestIndexHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		HasWarn           bool
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		se.GetStochaseinstein_dbars().StmtCtx.SetWarnings(nil)

		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			output[i].Best = core.ToString(p)
			output[i].HasWarn = len(se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()) > 0
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
		warnings := se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		if output[i].HasWarn {
			c.Assert(warnings, HasLen, 1, comment)
		} else {
			c.Assert(warnings, HasLen, 0, comment)
		}
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestIndexMergeHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		HasWarn           bool
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		se.GetStochaseinstein_dbars().StmtCtx.SetWarnings(nil)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)
		sctx := se.(stochastikctx.Context)
		err = executor.ResetContextOfStmt(sctx, stmt)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			output[i].Best = core.ToString(p)
			output[i].HasWarn = len(se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()) > 0
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
		warnings := se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		if output[i].HasWarn {
			c.Assert(warnings, HasLen, 1, comment)
		} else {
			c.Assert(warnings, HasLen, 0, comment)
		}
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestQueryBlockHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              string
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.TODO()
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = core.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Plan, comment)
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestInlineProjection(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `drop block if exists test.t1, test.t2;`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create block test.t1(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create block test.t2(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              string
		Hints             string
	}
	is := petri.GetPetri(se).SchemaReplicant()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := planner.Optimize(ctx, se, stmt, is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = core.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p))
		})
		c.Assert(core.ToString(p), Equals, output[i].Plan, comment)
		c.Assert(hint.RestoreOptimizerHints(core.GenHintsFromPhysicalPlan(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testPlanSuite) TestPosetDagPlanBuilderSplitAvg(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	tests := []struct {
		allegrosql string
		plan       string
	}{
		{
			allegrosql: "select avg(a),avg(b),avg(c) from t",
			plan:       "BlockReader(Block(t)->StreamAgg)->StreamAgg",
		},
		{
			allegrosql: "select /*+ HASH_AGG() */ avg(a),avg(b),avg(c) from t",
			plan:       "BlockReader(Block(t)->HashAgg)->HashAgg",
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.allegrosql)
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil, comment)

		core.Preprocess(se, stmt, s.is)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)

		c.Assert(core.ToString(p), Equals, tt.plan, comment)
		root, ok := p.(core.PhysicalPlan)
		if !ok {
			continue
		}
		testPosetDagPlanBuilderSplitAvg(c, root)
	}
}

func testPosetDagPlanBuilderSplitAvg(c *C, root core.PhysicalPlan) {
	if p, ok := root.(*core.PhysicalBlockReader); ok {
		if p.BlockPlans != nil {
			baseAgg := p.BlockPlans[len(p.BlockPlans)-1]
			if agg, ok := baseAgg.(*core.PhysicalHashAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					c.Assert(agg.Schema().DeferredCausets[i].RetType, Equals, aggfunc.RetTp)
				}
			}
			if agg, ok := baseAgg.(*core.PhysicalStreamAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					c.Assert(agg.Schema().DeferredCausets[i].RetType, Equals, aggfunc.RetTp)
				}
			}
		}
	}

	childs := root.Children()
	if childs == nil {
		return
	}
	for _, son := range childs {
		testPosetDagPlanBuilderSplitAvg(c, son)
	}
}

func (s *testPlanSuite) TestIndexJoinHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `drop block if exists test.t1, test.t2, test.t;`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create block test.t1(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create block test.t2(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, "CREATE TABLE `t` ( `a` bigint(20) NOT NULL, `b` tinyint(1) DEFAULT NULL, `c` datetime DEFAULT NULL, `d` int(10) unsigned DEFAULT NULL, `e` varchar(20) DEFAULT NULL, `f` double DEFAULT NULL, `g` decimal(30,5) DEFAULT NULL, `h` float DEFAULT NULL, `i` date DEFAULT NULL, `j` timestamp NULL DEFAULT NULL, PRIMARY KEY (`a`), UNIQUE KEY `b` (`b`), KEY `c` (`c`,`d`,`e`), KEY `f` (`f`), KEY `g` (`g`,`h`), KEY `g_2` (`g`), UNIQUE KEY `g_3` (`g`), KEY `i` (`i`) );")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              string
	}
	is := petri.GetPetri(se).SchemaReplicant()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := planner.Optimize(ctx, se, stmt, is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Plan, comment)
	}
}

func (s *testPlanSuite) TestPosetDagPlanBuilderWindow(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		"set @@stochastik.milevadb_window_concurrency = 1",
	}
	s.doTestPosetDagPlanBuilderWindow(c, vars, input, output)
}

func (s *testPlanSuite) TestPosetDagPlanBuilderWindowParallel(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		"set @@stochastik.milevadb_window_concurrency = 4",
	}
	s.doTestPosetDagPlanBuilderWindow(c, vars, input, output)
}

func (s *testPlanSuite) doTestPosetDagPlanBuilderWindow(c *C, vars, input []string, output []struct {
	ALLEGROALLEGROSQL string
	Best              string
}) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)

	for _, v := range vars {
		_, err = se.Execute(ctx, v)
		c.Assert(err, IsNil)
	}

	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testPlanSuite) TestNominalSort(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustExec("use test")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
		Result            []string
	}
	tk.MustExec("create block t (a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("insert into t values(1, 2)")
	tk.MustExec("insert into t values(2, 4)")
	tk.MustExec("insert into t values(3, 5)")
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testPlanSuite) TestHintFromDiffDatabase(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `drop block if exists test.t1`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create block test.t1(a bigint, index idx_a(a));`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create block test.t2(a bigint, index idx_a(a));`)
	c.Assert(err, IsNil)

	_, err = se.Execute(ctx, "drop database if exists test2")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, "create database test2")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, "use test2")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              string
	}
	is := petri.GetPetri(se).SchemaReplicant()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := planner.Optimize(ctx, se, stmt, is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Plan = core.ToString(p)
		})
		c.Assert(core.ToString(p), Equals, output[i].Plan, comment)
	}
}

func (s *testPlanSuite) TestNthPlanHintWithExplain(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.Execute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `drop block if exists test.tt`)
	c.Assert(err, IsNil)
	_, err = se.Execute(ctx, `create block test.tt (a int,b int, index(a), index(b));`)
	c.Assert(err, IsNil)

	_, err = se.Execute(ctx, "insert into tt values (1, 1), (2, 2), (3, 4)")
	c.Assert(err, IsNil)

	tk.MustExec(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Plan              []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Plan...))
	}

	// This assert makes sure a query with or without nth_plan() hint output exactly the same plan(including plan ID).
	// The query below is the same as queries in the testdata except for nth_plan() hint.
	// Currently its output is the same as the second test case in the testdata, which is `output[1]`. If this doesn't
	// hold in the future, you may need to modify this.
	tk.MustQuery("explain select * from test.tt where a=1 and b=1").Check(testkit.Rows(output[1].Plan...))
}
