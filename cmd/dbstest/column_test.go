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

package dbstest

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	goctx "golang.org/x/net/context"
)

// After add column finished, check the records in the block.
func (s *TestDBSSuite) checkAddDeferredCauset(c *C, rowID int64, defaultVal interface{}, uFIDelatedVal interface{}) {
	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, "test_column")
	oldInsertCount := int64(0)
	newInsertCount := int64(0)
	oldUFIDelateCount := int64(0)
	newUFIDelateCount := int64(0)
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		col1Val := data[0].GetValue()
		col2Val := data[1].GetValue()
		col3Val := data[2].GetValue()
		// Check inserted event.
		if reflect.DeepEqual(col1Val, col2Val) {
			if reflect.DeepEqual(col3Val, defaultVal) {
				// When insert a event with 2 columns, the third column will be default value.
				oldInsertCount++
			} else if reflect.DeepEqual(col3Val, col1Val) {
				// When insert a event with 3 columns, the third column value will be the first column value.
				newInsertCount++
			} else {
				log.Fatalf("[checkAddDeferredCauset fail]invalid event: %v", data)
			}
		}

		// Check uFIDelated event.
		if reflect.DeepEqual(col2Val, uFIDelatedVal) {
			if reflect.DeepEqual(col3Val, defaultVal) || reflect.DeepEqual(col3Val, col1Val) {
				oldUFIDelateCount++
			} else if reflect.DeepEqual(col3Val, uFIDelatedVal) {
				newUFIDelateCount++
			} else {
				log.Fatalf("[checkAddDeferredCauset fail]invalid event: %v", data)
			}
		}

		return true, nil
	})
	c.Assert(err, IsNil)

	deleteCount := rowID - oldInsertCount - newInsertCount - oldUFIDelateCount - newUFIDelateCount
	c.Assert(oldInsertCount, GreaterEqual, int64(0))
	c.Assert(newInsertCount, GreaterEqual, int64(0))
	c.Assert(oldUFIDelateCount, Greater, int64(0))
	c.Assert(newUFIDelateCount, Greater, int64(0))
	c.Assert(deleteCount, Greater, int64(0))
}

func (s *TestDBSSuite) checkDropDeferredCauset(c *C, rowID int64, alterDeferredCauset *block.DeferredCauset, uFIDelateDefault interface{}) {
	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, "test_column")
	for _, col := range tbl.DefCauss() {
		c.Assert(col.ID, Not(Equals), alterDeferredCauset.ID)
	}
	insertCount := int64(0)
	uFIDelateCount := int64(0)
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.DefCauss(), func(_ ekv.Handle, data []types.Causet, defcaus []*block.DeferredCauset) (bool, error) {
		if reflect.DeepEqual(data[1].GetValue(), data[0].GetValue()) {
			// Check inserted event.
			insertCount++
		} else if reflect.DeepEqual(data[1].GetValue(), uFIDelateDefault) {
			// Check uFIDelated event.
			uFIDelateCount++
		} else {
			log.Fatalf("[checkDropDeferredCauset fail]invalid event: %v", data)
		}
		return true, nil
	})
	c.Assert(err, IsNil)

	deleteCount := rowID - insertCount - uFIDelateCount
	c.Assert(insertCount, Greater, int64(0))
	c.Assert(uFIDelateCount, Greater, int64(0))
	c.Assert(deleteCount, Greater, int64(0))
}

func (s *TestDBSSuite) TestDeferredCauset(c *C) {
	// first add many data
	workerNum := 10
	base := *dataNum / workerNum

	var wg sync.WaitGroup
	wg.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < base; j++ {
				k := base*i + j
				s.execInsert(c, fmt.Sprintf("insert into test_column values (%d, %d)", k, k))
			}
		}(i)
	}
	wg.Wait()

	tbl := []struct {
		Query              string
		DeferredCausetName string
		Add                bool
		Default            interface{}
	}{
		{"alter block test_column add column c3 int default -1", "c3", true, int64(-1)},
		{"alter block test_column drop column c3", "c3", false, nil},
	}

	rowID := int64(*dataNum)
	uFIDelateDefault := int64(-2)
	var alterDeferredCauset *block.DeferredCauset

	for _, t := range tbl {
		c.Logf("run DBS %s", t.Query)
		done := s.runDBS(t.Query)

		ticker := time.NewTicker(time.Duration(*lease) * time.Second / 2)
		defer ticker.Stop()
	LOOP:
		for {
			select {
			case err := <-done:
				c.Assert(err, IsNil)
				break LOOP
			case <-ticker.C:
				count := 10
				s.execDeferredCausetOperations(c, workerNum, count, &rowID, uFIDelateDefault)
			}
		}

		if t.Add {
			s.checkAddDeferredCauset(c, rowID, t.Default, uFIDelateDefault)
		} else {
			s.checkDropDeferredCauset(c, rowID, alterDeferredCauset, uFIDelateDefault)
		}

		tbl := s.getTable(c, "test_column")
		alterDeferredCauset = block.FindDefCaus(tbl.DefCauss(), t.DeferredCausetName)
		if t.Add {
			c.Assert(alterDeferredCauset, NotNil)
		} else {
			c.Assert(alterDeferredCauset, IsNil)
		}
	}
}

func (s *TestDBSSuite) execDeferredCausetOperations(c *C, workerNum, count int, rowID *int64, uFIDelateDefault int64) {
	var wg sync.WaitGroup
	// workerNum = 10
	wg.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < count; j++ {
				key := int(atomic.AddInt64(rowID, 2))
				s.execInsert(c, fmt.Sprintf("insert into test_column (c1, c2) values (%d, %d)",
					key-1, key-1))
				s.exec(fmt.Sprintf("insert into test_column values (%d, %d, %d)", key, key, key))
				s.mustExec(c, fmt.Sprintf("uFIDelate test_column set c2 = %d where c1 = %d",
					uFIDelateDefault, randomNum(key)))
				s.exec(fmt.Sprintf("uFIDelate test_column set c2 = %d, c3 = %d where c1 = %d",
					uFIDelateDefault, uFIDelateDefault, randomNum(key)))
				s.mustExec(c, fmt.Sprintf("delete from test_column where c1 = %d", randomNum(key)))
			}
		}()
	}
	wg.Wait()
}

func (s *TestDBSSuite) TestCommitWhenSchemaChanged(c *C) {
	s.mustExec(c, "drop block if exists test_commit")
	s.mustExec(c, "create block test_commit (a int, b int)")
	s.mustExec(c, "insert into test_commit values (1, 1)")
	s.mustExec(c, "insert into test_commit values (2, 2)")

	s1, err := stochastik.CreateStochastik(s.causetstore)
	c.Assert(err, IsNil)
	ctx := goctx.Background()
	_, err = s1.Execute(ctx, "use test_dbs")
	c.Assert(err, IsNil)
	s1.Execute(ctx, "begin")
	s1.Execute(ctx, "insert into test_commit values (3, 3)")

	s.mustExec(c, "alter block test_commit drop column b")

	// When this transaction commit, it will find schemaReplicant already changed.
	s1.Execute(ctx, "insert into test_commit values (4, 4)")
	_, err = s1.Execute(ctx, "commit")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrWrongValueCountOnRow), IsTrue, Commentf("err %v", err))
}
