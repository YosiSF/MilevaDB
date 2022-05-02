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

package einsteindb_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
)

var _ = Suite(&testALLEGROSQLSuite{})
var _ = SerialSuites(&testALLEGROSQLSerialSuite{})

type testALLEGROSQLSuite struct {
	testALLEGROSQLSuiteBase
}

type testALLEGROSQLSerialSuite struct {
	testALLEGROSQLSuiteBase
}

type testALLEGROSQLSuiteBase struct {
	OneByOneSuite
	causetstore CausetStorage
	dom         *petri.Petri
}

func (s *testALLEGROSQLSuiteBase) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	var err error
	s.causetstore = NewTestStore(c).(CausetStorage)
	// actual this is better done in `OneByOneSuite.SetUpSuite`, but this would cause circle dependency
	if *WithEinsteinDB {
		stochastik.ResetStoreForWithEinsteinDBTest(s.causetstore)
	}

	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *testALLEGROSQLSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testALLEGROSQLSerialSuite) TestFailBusyServerINTERLOCK(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(2)

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/mockeinsteindb/rpcServerBusy", `return(true)`), IsNil)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/mockeinsteindb/rpcServerBusy"), IsNil)
	}()

	go func() {
		defer wg.Done()
		rs, err := se.Execute(context.Background(), `SELECT variable_value FROM allegrosql.milevadb WHERE variable_name="bootstrapped"`)
		if len(rs) > 0 {
			defer terror.Call(rs[0].Close)
		}
		c.Assert(err, IsNil)
		req := rs[0].NewChunk()
		err = rs[0].Next(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(req.NumRows() == 0, IsFalse)
		c.Assert(req.GetRow(0).GetString(0), Equals, "True")
	}()

	wg.Wait()
}

func TestMain(m *testing.M) {
	ReadTimeoutMedium = 2 * time.Second
	os.Exit(m.Run())
}

func (s *testALLEGROSQLSuite) Testinterlocking_directorateStreamRecvTimeout(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block INTERLOCK_stream_timeout (id int)")
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into INTERLOCK_stream_timeout values (%d)", i))
	}
	tk.Se.GetStochaseinstein_dbars().EnableStreaming = true

	{
		enable := true
		visited := make(chan int, 1)
		timeouted := false
		timeout := ReadTimeoutMedium + 100*time.Second
		ctx := context.WithValue(context.Background(), mock.HookKeyForTest("mockEinsteinDBStreamRecvHook"), func(ctx context.Context) {
			if !enable {
				return
			}
			visited <- 1

			select {
			case <-ctx.Done():
			case <-time.After(timeout):
				timeouted = true
			}
			enable = false
		})

		res, err := tk.Se.Execute(ctx, "select * from INTERLOCK_stream_timeout")
		c.Assert(err, IsNil)

		req := res[0].NewChunk()
		for i := 0; ; i++ {
			err := res[0].Next(ctx, req)
			c.Assert(err, IsNil)
			if req.NumRows() == 0 {
				break
			}
			req.Reset()
		}
		select {
		case <-visited:
			// run with mockeinsteindb
			c.Assert(timeouted, IsFalse)
		default:
			// run with real einsteindb
		}
	}

	{
		enable := true
		visited := make(chan int, 1)
		timeouted := false
		timeout := 1 * time.Millisecond
		ctx := context.WithValue(context.Background(), mock.HookKeyForTest("mockEinsteinDBStreamRecvHook"), func(ctx context.Context) {
			if !enable {
				return
			}
			visited <- 1

			select {
			case <-ctx.Done():
			case <-time.After(timeout):
				timeouted = true
			}
			enable = false
		})

		res, err := tk.Se.Execute(ctx, "select * from INTERLOCK_stream_timeout")
		c.Assert(err, IsNil)

		req := res[0].NewChunk()
		for i := 0; ; i++ {
			err := res[0].Next(ctx, req)
			c.Assert(err, IsNil)
			if req.NumRows() == 0 {
				break
			}
			req.Reset()
		}
		select {
		case <-visited:
			// run with mockeinsteindb
			c.Assert(timeouted, IsTrue)
		default:
			// run with real einsteindb
		}
	}
}
