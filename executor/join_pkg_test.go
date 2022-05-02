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

package executor

import (
	"context"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
)

func (s *pkgTestSerialSuite) TestJoinExec(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testEventContainerSpill", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/executor/testEventContainerSpill"), IsNil)
	}()
	defCausTypes := []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeDouble),
	}
	casTest := defaultHashJoinTestCase(defCausTypes, 0, false)

	runTest := func() {
		opt1 := mockDataSourceParameters{
			rows: casTest.rows,
			ctx:  casTest.ctx,
			genDataFunc: func(event int, typ *types.FieldType) interface{} {
				switch typ.Tp {
				case allegrosql.TypeLong, allegrosql.TypeLonglong:
					return int64(event)
				case allegrosql.TypeDouble:
					return float64(event)
				default:
					panic("not implement")
				}
			},
		}
		opt2 := opt1
		opt1.schemaReplicant = expression.NewSchema(casTest.defCausumns()...)
		opt2.schemaReplicant = expression.NewSchema(casTest.defCausumns()...)
		dataSource1 := buildMockDataSource(opt1)
		dataSource2 := buildMockDataSource(opt2)
		dataSource1.prepareChunks()
		dataSource2.prepareChunks()

		exec := prepare4HashJoin(casTest, dataSource1, dataSource2)
		result := newFirstChunk(exec)
		{
			ctx := context.Background()
			chk := newFirstChunk(exec)
			err := exec.Open(ctx)
			c.Assert(err, IsNil)
			for {
				err = exec.Next(ctx, chk)
				c.Assert(err, IsNil)
				if chk.NumEvents() == 0 {
					break
				}
				result.Append(chk, 0, chk.NumEvents())
			}
			c.Assert(exec.rowContainer.alreadySpilledSafeForTest(), Equals, casTest.disk)
			err = exec.Close()
			c.Assert(err, IsNil)
		}

		c.Assert(result.NumDefCauss(), Equals, 4)
		c.Assert(result.NumEvents(), Equals, casTest.rows)
		visit := make(map[int64]bool, casTest.rows)
		for i := 0; i < casTest.rows; i++ {
			val := result.DeferredCauset(0).Int64s()[i]
			c.Assert(result.DeferredCauset(1).Float64s()[i], Equals, float64(val))
			c.Assert(result.DeferredCauset(2).Int64s()[i], Equals, val)
			c.Assert(result.DeferredCauset(3).Float64s()[i], Equals, float64(val))
			visit[val] = true
		}
		for i := 0; i < casTest.rows; i++ {
			c.Assert(visit[int64(i)], IsTrue)
		}
	}

	concurrency := []int{1, 4}
	rows := []int{3, 1024, 4096}
	disk := []bool{false, true}
	for _, concurrency := range concurrency {
		for _, rows := range rows {
			for _, disk := range disk {
				casTest.concurrency = concurrency
				casTest.rows = rows
				casTest.disk = disk
				runTest()
			}
		}
	}
}

func (s *pkgTestSuite) TestHashJoinRuntimeStats(c *C) {
	stats := &hashJoinRuntimeStats{
		fetchAndBuildHashBlock: 2 * time.Second,
		hashStat: hashStatistic{
			probeDefCauslision: 1,
			buildBlockElapse:   time.Millisecond * 100,
		},
		fetchAndProbe:    int64(5 * time.Second),
		probe:            int64(4 * time.Second),
		concurrent:       4,
		maxFetchAndProbe: int64(2 * time.Second),
	}
	c.Assert(stats.String(), Equals, "build_hash_block:{total:2s, fetch:1.9s, build:100ms}, probe:{concurrency:4, total:5s, max:2s, probe:4s, fetch:1s, probe_defCauslision:1}")
	c.Assert(stats.String(), Equals, stats.Clone().String())
	stats.Merge(stats.Clone())
	c.Assert(stats.String(), Equals, "build_hash_block:{total:4s, fetch:3.8s, build:200ms}, probe:{concurrency:4, total:10s, max:2s, probe:8s, fetch:2s, probe_defCauslision:2}")
}

func (s *pkgTestSuite) TestIndexJoinRuntimeStats(c *C) {
	stats := indexLookUpJoinRuntimeStats{
		concurrency: 5,
		probe:       int64(time.Second),
		innerWorker: innerWorkerRuntimeStats{
			totalTime: int64(time.Second * 5),
			task:      16,
			construct: int64(100 * time.Millisecond),
			fetch:     int64(300 * time.Millisecond),
			build:     int64(250 * time.Millisecond),
			join:      int64(150 * time.Millisecond),
		},
	}
	c.Assert(stats.String(), Equals, "inner:{total:5s, concurrency:5, task:16, construct:100ms, fetch:300ms, build:250ms, join:150ms}, probe:1s")
	c.Assert(stats.String(), Equals, stats.Clone().String())
	stats.Merge(stats.Clone())
	c.Assert(stats.String(), Equals, "inner:{total:10s, concurrency:5, task:32, construct:200ms, fetch:600ms, build:500ms, join:300ms}, probe:2s")
}
