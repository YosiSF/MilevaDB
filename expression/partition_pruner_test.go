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

package expression_test

import (
	"fmt"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
)

var _ = Suite(&testSuite2{})

type testSuite2 struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	ctx         stochastikctx.Context
	testData    solitonutil.TestData
}

func (s *testSuite2) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test_partition")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Events() {
		blockName := tb[0]
		tk.MustExec(fmt.Sprintf("drop block %v", blockName))
	}
}

func (s *testSuite2) SetUpSuite(c *C) {
	var err error
	s.causetstore, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "partition_pruner")
	c.Assert(err, IsNil)
}

func (s *testSuite2) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
	s.dom.Close()
	s.causetstore.Close()
}

func (s *testSuite2) TestHashPartitionPruner(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.MustExec("drop block if exists t1, t2;")
	tk.MustExec("set @@milevadb_enable_clustered_index=0;")
	tk.MustExec("create block t2(id int, a int, b int, primary key(id, a)) partition by hash(id + a) partitions 10;")
	tk.MustExec("create block t1(id int primary key, a int, b int) partition by hash(id) partitions 10;")
	tk.MustExec("create block t3(id int, a int, b int, primary key(id, a)) partition by hash(id) partitions 10;")
	tk.MustExec("create block t4(d datetime, a int, b int, primary key(d, a)) partition by hash(year(d)) partitions 10;")
	tk.MustExec("create block t5(d date, a int, b int, primary key(d, a)) partition by hash(month(d)) partitions 10;")
	tk.MustExec("create block t6(a int, b int) partition by hash(a) partitions 3;")
	tk.MustExec("create block t7(a int, b int) partition by hash(a + b) partitions 10;")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Result = s.testData.ConvertEventsToStrings(tk.MustQuery(tt).Events())
		})
		tk.MustQuery(tt).Check(testkit.Events(output[i].Result...))
	}
}
