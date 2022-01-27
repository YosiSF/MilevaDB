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

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
)

func (s *inspectionSummarySuite) TestInspectionMemrules(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	inspectionCount := len(executor.InspectionMemrules)
	summaryCount := len(executor.InspectionSummaryMemrules)
	var cases = []struct {
		allegrosql string
		ruleCount  int
	}{
		{
			allegrosql: "select * from information_schema.inspection_rules",
			ruleCount:  inspectionCount + summaryCount,
		},
		{
			allegrosql: "select * from information_schema.inspection_rules where type='inspection'",
			ruleCount:  inspectionCount,
		},
		{
			allegrosql: "select * from information_schema.inspection_rules where type='summary'",
			ruleCount:  summaryCount,
		},
		{
			allegrosql: "select * from information_schema.inspection_rules where type='inspection' and type='summary'",
			ruleCount:  0,
		},
	}

	for _, ca := range cases {
		rs, err := tk.Exec(ca.allegrosql)
		c.Assert(err, IsNil)
		rules, err := stochastik.ResultSetToStringSlice(context.Background(), tk.Se, rs)
		c.Assert(err, IsNil)
		c.Assert(len(rules), Equals, ca.ruleCount)
	}
}
