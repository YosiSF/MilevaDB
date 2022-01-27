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
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testSuiteP2) TestQueryTime(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	costTime := time.Since(tk.Se.GetStochastikVars().StartTime)
	c.Assert(costTime < 1*time.Second, IsTrue)

	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a int)")
	tk.MustExec("insert into t values(1), (1), (1), (1), (1)")
	tk.MustExec("select * from t t1 join t t2 on t1.a = t2.a")

	costTime = time.Since(tk.Se.GetStochastikVars().StartTime)
	c.Assert(costTime < 1*time.Second, IsTrue)
}
