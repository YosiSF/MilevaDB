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

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = SerialSuites(&inspectionSummarySuite{})

type inspectionSummarySuite struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

func (s *inspectionSummarySuite) SetUpSuite(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	s.dom = dom
}

func (s *inspectionSummarySuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *inspectionSummarySuite) TestValidInspectionSummaryMemrules(c *C) {
	for rule, tbls := range executor.InspectionSummaryMemrules {
		blocks := set.StringSet{}
		for _, t := range tbls {
			c.Assert(blocks.Exist(t), IsFalse, Commentf("duplicate block name: %v in rule: %v", t, rule))
			blocks.Insert(t)

			_, found := schemareplicant.MetricBlockMap[t]
			c.Assert(found, IsTrue, Commentf("metric block %v not define", t))
		}
	}
}

func (s *inspectionSummarySuite) TestInspectionSummary(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	fpName := "github.com/whtcorpsinc/milevadb/executor/mockMetricsBlockData"
	c.Assert(failpoint.Enable(fpName, "return"), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetStochastikVars().StmtCtx, s, allegrosql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	// construct some mock data
	mockData := map[string][][]types.Causet{
		// defCausumns: time, instance, type, result, value
		"milevadb_qps": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "milevadb-0", "Query", "OK", 0.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "milevadb-0", "Query", "Error", 1.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "milevadb-1", "Quit", "Error", 5.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "milevadb-1", "Quit", "Error", 9.0),
		},
		// defCausumns: time, instance, sql_type, quantile, value
		"milevadb_query_duration": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "einsteindb-0", "Select", 0.99, 0.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "einsteindb-1", "UFIDelate", 0.99, 1.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "einsteindb-1", "UFIDelate", 0.99, 3.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "einsteindb-2", "Delete", 0.99, 5.0),
		},
	}

	ctx := context.WithValue(context.Background(), "__mockMetricsBlockData", mockData)
	ctx = failpoint.WithHook(ctx, func(_ context.Context, fpname string) bool {
		return fpName == fpname
	})

	rs, err := tk.Se.Execute(ctx, "select * from information_schema.inspection_summary where rule='query-summary' and metrics_name in ('milevadb_qps', 'milevadb_query_duration')")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect ALLEGROALLEGROSQL failed"))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetStochastikVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Events(
		"query-summary einsteindb-0 milevadb_query_duration Select 0.99 0 0 0 The quantile of MilevaDB query durations(second)",
		"query-summary einsteindb-1 milevadb_query_duration UFIDelate 0.99 2 1 3 The quantile of MilevaDB query durations(second)",
		"query-summary einsteindb-2 milevadb_query_duration Delete 0.99 5 5 5 The quantile of MilevaDB query durations(second)",
		"query-summary milevadb-0 milevadb_qps Query, Error <nil> 1 1 1 MilevaDB query processing numbers per second",
		"query-summary milevadb-0 milevadb_qps Query, OK <nil> 0 0 0 MilevaDB query processing numbers per second",
		"query-summary milevadb-1 milevadb_qps Quit, Error <nil> 7 5 9 MilevaDB query processing numbers per second",
	))

	// Test for select * from information_schema.inspection_summary without specify rules.
	rs, err = tk.Se.Execute(ctx, "select * from information_schema.inspection_summary where metrics_name = 'milevadb_qps'")
	c.Assert(err, IsNil)
	result = tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect ALLEGROALLEGROSQL failed"))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetStochastikVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Events(
		"query-summary milevadb-0 milevadb_qps Query, Error <nil> 1 1 1 MilevaDB query processing numbers per second",
		"query-summary milevadb-0 milevadb_qps Query, OK <nil> 0 0 0 MilevaDB query processing numbers per second",
		"query-summary milevadb-1 milevadb_qps Quit, Error <nil> 7 5 9 MilevaDB query processing numbers per second",
	))
}
