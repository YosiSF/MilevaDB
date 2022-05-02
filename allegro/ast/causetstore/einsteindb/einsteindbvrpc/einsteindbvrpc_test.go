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

package einsteindbrpc

import (
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testBatchCommand struct{}

var _ = Suite(&testBatchCommand{})

func (s *testBatchCommand) TestBatchResponse(c *C) {
	resp := &einsteindbpb.BatchCommandsResponse_Response{}
	batchResp, err := FromBatchCommandsResponse(resp)
	c.Assert(batchResp == nil, IsTrue)
	c.Assert(err != nil, IsTrue)
}
