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
	"context"
	"fmt"
	"runtime"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/executor"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	. "github.com/whtcorpsinc/check"
)

var _ = SerialSuites(&testMemoryLeak{})

type testMemoryLeak struct {
	causetstore ekv.CausetStorage
	petri       *petri.Petri
}

func (s *testMemoryLeak) SetUpSuite(c *C) {
	var err error
	s.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	s.petri, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *testMemoryLeak) TearDownSuite(c *C) {
	s.petri.Close()
	c.Assert(s.causetstore.Close(), IsNil)
}

func (s *testMemoryLeak) TestPBMemoryLeak(c *C) {
	c.Skip("too slow")

	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "create database test_mem")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_mem")
	c.Assert(err, IsNil)

	// prepare data
	totalSize := uint64(256 << 20) // 256MB
	blockSize := uint64(8 << 10)   // 8KB
	delta := totalSize / 5
	numEvents := totalSize / blockSize
	_, err = se.Execute(context.Background(), fmt.Sprintf("create block t (c varchar(%v))", blockSize))
	c.Assert(err, IsNil)
	defer func() {
		_, err = se.Execute(context.Background(), "drop block t")
		c.Assert(err, IsNil)
	}()
	allegrosql := fmt.Sprintf("insert into t values (space(%v))", blockSize)
	for i := uint64(0); i < numEvents; i++ {
		_, err = se.Execute(context.Background(), allegrosql)
		c.Assert(err, IsNil)
	}

	// read data
	runtime.GC()
	allocatedBegin, inUseBegin := s.readMem()
	records, err := se.Execute(context.Background(), "select * from t")
	c.Assert(err, IsNil)
	record := records[0]
	rowCnt := 0
	chk := record.NewChunk()
	for {
		c.Assert(record.Next(context.Background(), chk), IsNil)
		rowCnt += chk.NumEvents()
		if chk.NumEvents() == 0 {
			break
		}
	}
	c.Assert(rowCnt, Equals, int(numEvents))

	// check memory before close
	runtime.GC()
	allocatedAfter, inUseAfter := s.readMem()
	c.Assert(allocatedAfter-allocatedBegin, GreaterEqual, totalSize)
	c.Assert(s.memDiff(inUseAfter, inUseBegin), Less, delta)

	se.Close()
	runtime.GC()
	allocatedFinal, inUseFinal := s.readMem()
	c.Assert(allocatedFinal-allocatedAfter, Less, delta)
	c.Assert(s.memDiff(inUseFinal, inUseAfter), Less, delta)
}

func (s *testMemoryLeak) readMem() (allocated, heapInUse uint64) {
	var stat runtime.MemStats
	runtime.ReadMemStats(&stat)
	return stat.TotalAlloc, stat.HeapInuse
}

func (s *testMemoryLeak) memDiff(m1, m2 uint64) uint64 {
	if m1 > m2 {
		return m1 - m2
	}
	return m2 - m1
}

func (s *testMemoryLeak) TestGlobalMemoryTrackerOnCleanUp(c *C) {
	// TODO: assert the memory consume has happened in another way
	originConsume := executor.GlobalMemoryUsageTracker.BytesConsumed()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t (id int)")

	// assert insert
	tk.MustExec("insert t (id) values (1)")
	tk.MustExec("insert t (id) values (2)")
	tk.MustExec("insert t (id) values (3)")
	afterConsume := executor.GlobalMemoryUsageTracker.BytesConsumed()
	c.Assert(originConsume, Equals, afterConsume)

	// assert uFIDelate
	tk.MustExec("uFIDelate t set id = 4 where id = 1")
	tk.MustExec("uFIDelate t set id = 5 where id = 2")
	tk.MustExec("uFIDelate t set id = 6 where id = 3")
	afterConsume = executor.GlobalMemoryUsageTracker.BytesConsumed()
	c.Assert(originConsume, Equals, afterConsume)
}
