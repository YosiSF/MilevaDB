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

package einsteindb

import (
	"context"
	"fmt"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/config"
)

type testClientFailSuite struct {
	OneByOneSuite
}

func (s *testClientFailSuite) SetUpSuite(_ *C) {
	// This dagger make testClientFailSuite runs exclusively.
	withEinsteinDBGlobalLock.Lock()
}

func (s testClientFailSuite) TearDownSuite(_ *C) {
	withEinsteinDBGlobalLock.Unlock()
}

func (s *testClientFailSuite) TestPanicInRecvLoop(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/panicInFailPendingRequests", `panic`), IsNil)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/gotErrorInRecvLoop", `return("0")`), IsNil)

	server, port := startMockEinsteinDBService()
	c.Assert(port > 0, IsTrue)
	defer server.Stop()

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	rpcClient := newRPCClient(config.Security{}, func(c *rpcClient) {
		c.dialTimeout = time.Second / 3
	})

	// Start batchRecvLoop, and it should panic in `failPendingRequests`.
	_, err := rpcClient.getConnArray(addr, true, func(cfg *config.EinsteinDBClient) { cfg.GrpcConnectionCount = 1 })
	c.Assert(err, IsNil, Commentf("cannot establish local connection due to env problems(e.g. heavy load in test machine), please retry again"))

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdEmpty, &einsteindbpb.BatchCommandsEmptyRequest{})
	_, err = rpcClient.SendRequest(context.Background(), addr, req, time.Second/2)
	c.Assert(err, NotNil)

	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/gotErrorInRecvLoop"), IsNil)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/panicInFailPendingRequests"), IsNil)
	time.Sleep(time.Second * 2)

	req = einsteindbrpc.NewRequest(einsteindbrpc.CmdEmpty, &einsteindbpb.BatchCommandsEmptyRequest{})
	_, err = rpcClient.SendRequest(context.Background(), addr, req, time.Second*4)
	c.Assert(err, IsNil)
}
