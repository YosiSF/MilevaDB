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

// Note: All the tests in this file will be executed sequentially.

package oomtest

import (
	"os"
	"strings"
	"sync"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ = SerialSuites(&testOOMSuite{})

type testOOMSuite struct {
	causetstore ekv.CausetStorage
	do          *petri.Petri
	oom         *oomCapturer
}

func (s *testOOMSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.registerHook()
	var err error
	s.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	petri.RunAutoAnalyze = false
	s.do, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.OOMCausetAction = config.OOMCausetActionLog
	})
}

func (s *testOOMSuite) TearDownSuite(c *C) {
	s.do.Close()
	s.causetstore.Close()
}

func (s *testOOMSuite) registerHook() {
	conf := &log.Config{Level: os.Getenv("log_level"), File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	s.oom = &oomCapturer{r.Core, "", sync.Mutex{}}
	lg := zap.New(s.oom)
	log.ReplaceGlobals(lg, r)
}

func (s *testOOMSuite) TestMemTracker4UFIDelateExec(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t_MemTracker4UFIDelateExec (id int, a int, b int, index idx_a(`a`))")

	log.SetLevel(zap.InfoLevel)
	s.oom.tracker = ""
	tk.MustExec("insert into t_MemTracker4UFIDelateExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetStochastikVars().MemQuotaQuery = 244
	tk.MustExec("uFIDelate t_MemTracker4UFIDelateExec set a = 4")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
}

func (s *testOOMSuite) TestMemTracker4InsertAndReplaceExec(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t (id int, a int, b int, index idx_a(`a`))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3)")

	tk.MustExec("create block t_MemTracker4InsertAndReplaceExec (id int, a int, b int, index idx_a(`a`))")

	log.SetLevel(zap.InfoLevel)
	s.oom.tracker = ""
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetStochastikVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetStochastikVars().MemQuotaQuery = -1

	s.oom.tracker = ""
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetStochastikVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetStochastikVars().MemQuotaQuery = -1

	s.oom.tracker = ""
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec select * from t")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetStochastikVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec select * from t")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetStochastikVars().MemQuotaQuery = -1

	s.oom.tracker = ""
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec select * from t")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetStochastikVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec select * from t")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetStochastikVars().MemQuotaQuery = -1

	tk.Se.GetStochastikVars().DMLBatchSize = 1
	tk.Se.GetStochastikVars().BatchInsert = true
	s.oom.tracker = ""
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetStochastikVars().MemQuotaQuery = 1
	tk.MustExec("insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetStochastikVars().MemQuotaQuery = -1

	s.oom.tracker = ""
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Equals, "")
	tk.Se.GetStochastikVars().MemQuotaQuery = 1
	tk.MustExec("replace into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")
	tk.Se.GetStochastikVars().MemQuotaQuery = -1
}

func (s *testOOMSuite) TestMemTracker4DeleteExec(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block MemTracker4DeleteExec1 (id int, a int, b int, index idx_a(`a`))")
	tk.MustExec("create block MemTracker4DeleteExec2 (id int, a int, b int, index idx_a(`a`))")

	// delete from single block
	log.SetLevel(zap.InfoLevel)
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	s.oom.tracker = ""
	tk.MustExec("delete from MemTracker4DeleteExec1")
	c.Assert(s.oom.tracker, Equals, "")
	tk.MustExec("insert into MemTracker4DeleteExec1 values (1,1,1), (2,2,2), (3,3,3)")
	tk.Se.GetStochastikVars().MemQuotaQuery = 1
	tk.MustExec("delete from MemTracker4DeleteExec1")
	c.Assert(s.oom.tracker, Matches, "expensive_query during bootstrap phase")

	// delete from multiple block
	tk.Se.GetStochastikVars().MemQuotaQuery = 100000
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1)")
	tk.MustExec("insert into MemTracker4DeleteExec2 values(1,1,1)")
	s.oom.tracker = ""
	tk.MustExec("delete MemTracker4DeleteExec1, MemTracker4DeleteExec2 from MemTracker4DeleteExec1 join MemTracker4DeleteExec2 on MemTracker4DeleteExec1.a=MemTracker4DeleteExec2.a")
	c.Assert(s.oom.tracker, Equals, "")
	tk.MustExec("insert into MemTracker4DeleteExec1 values(1,1,1)")
	tk.MustExec("insert into MemTracker4DeleteExec2 values(1,1,1)")
	s.oom.tracker = ""
	tk.Se.GetStochastikVars().MemQuotaQuery = 10000
	tk.MustExec("delete MemTracker4DeleteExec1, MemTracker4DeleteExec2 from MemTracker4DeleteExec1 join MemTracker4DeleteExec2 on MemTracker4DeleteExec1.a=MemTracker4DeleteExec2.a")
	c.Assert(s.oom.tracker, Equals, "expensive_query during bootstrap phase")
}

type oomCapturer struct {
	zapcore.Core
	tracker string
	mu      sync.Mutex
}

func (h *oomCapturer) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	if strings.Contains(entry.Message, "memory exceeds quota") {
		err, _ := fields[0].Interface.(error)
		str := err.Error()
		begin := strings.Index(str, "8001]")
		if begin == -1 {
			panic("begin not found")
		}
		end := strings.Index(str, " holds")
		if end == -1 {
			panic("end not found")
		}
		h.tracker = str[begin+len("8001]") : end]
		return nil
	}

	h.mu.Lock()
	h.tracker = entry.Message
	h.mu.Unlock()
	return nil
}

func (h *oomCapturer) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if h.Enabled(e.Level) {
		return ce.AddCore(e, h)
	}
	return ce
}
