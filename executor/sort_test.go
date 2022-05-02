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
	"fmt"
	"os"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
)

func (s *testSerialSuite1) TestSortInDisk(c *C) {
	s.testSortInDisk(c, false)
	s.testSortInDisk(c, true)
}

func (s *testSerialSuite1) testSortInDisk(c *C, removeDir bool) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testSortedEventContainerSpill", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testSortedEventContainerSpill"), IsNil)
	}()

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	sm := &mockStochastikManager1{
		PS: make([]*soliton.ProcessInfo, 0),
	}
	tk.Se.SetStochastikManager(sm)
	s.petri.ExpensiveQueryHandle().SetStochastikManager(sm)

	if removeDir {
		c.Assert(os.RemoveAll(config.GetGlobalConfig().TempStoragePath), IsNil)
		defer func() {
			_, err := os.Stat(config.GetGlobalConfig().TempStoragePath)
			if err != nil {
				c.Assert(os.IsExist(err), IsTrue)
			}
		}()
	}

	tk.MustExec("set @@milevadb_mem_quota_query=1;")
	tk.MustExec("set @@milevadb_max_chunk_size=32;")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(c1 int, c2 int, c3 int)")
	for i := 0; i < 5; i++ {
		for j := i; j < 1024; j += 5 {
			tk.MustExec(fmt.Sprintf("insert into t values(%v, %v, %v)", j, j, j))
		}
	}
	result := tk.MustQuery("select * from t order by c1")
	for i := 0; i < 1024; i++ {
		c.Assert(result.Events()[i][0].(string), Equals, fmt.Sprint(i))
		c.Assert(result.Events()[i][1].(string), Equals, fmt.Sprint(i))
		c.Assert(result.Events()[i][2].(string), Equals, fmt.Sprint(i))
	}
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.MemTracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.MemTracker.MaxConsumed(), Greater, int64(0))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.DiskTracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.DiskTracker.MaxConsumed(), Greater, int64(0))
}

func (s *testSerialSuite1) TestIssue16696(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testSortedEventContainerSpill", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testSortedEventContainerSpill"), IsNil)
	}()
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testEventContainerSpill", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testEventContainerSpill"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("CREATE TABLE `t` (`a` int(11) DEFAULT NULL,`b` int(11) DEFAULT NULL)")
	tk.MustExec("insert into t values (1, 1)")
	for i := 0; i < 6; i++ {
		tk.MustExec("insert into t select * from t")
	}
	tk.MustExec("set milevadb_mem_quota_query = 1;")
	rows := tk.MustQuery("explain analyze  select t1.a, t1.a +1 from t t1 join t t2 join t t3 order by t1.a").Events()
	for _, event := range rows {
		length := len(event)
		line := fmt.Sprintf("%v", event)
		disk := fmt.Sprintf("%v", event[length-1])
		if strings.Contains(line, "Sort") || strings.Contains(line, "HashJoin") {
			c.Assert(strings.Contains(disk, "0 Bytes"), IsFalse)
			c.Assert(strings.Contains(disk, "MB") ||
				strings.Contains(disk, "KB") ||
				strings.Contains(disk, "Bytes"), IsTrue)
		}
	}
}
