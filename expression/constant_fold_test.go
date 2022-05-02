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
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
)

func (s *testIntegrationSuite) TestFoldIfNull(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop block if exists t;`)
	tk.MustExec(`create block t(a bigint, b bigint);`)
	tk.MustExec(`insert into t values(1, 1);`)
	tk.MustQuery(`desc select ifnull("aaaa", a) from t;`).Check(testkit.Events(
		`Projection_3 10000.00 root  aaaa->DeferredCauset#4`,
		`└─BlockReader_5 10000.00 root  data:BlockFullScan_4`,
		`  └─BlockFullScan_4 10000.00 INTERLOCK[einsteindb] block:t keep order:false, stats:pseudo`,
	))
	tk.MustQuery(`show warnings;`).Check(testkit.Events())
	tk.MustQuery(`select ifnull("aaaa", a) from t;`).Check(testkit.Events("aaaa"))
	tk.MustQuery(`show warnings;`).Check(testkit.Events())
}
