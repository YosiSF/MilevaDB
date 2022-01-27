// INTERLOCKyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

var _ = Suite(&testCacheSuite{})

type testCacheSuite struct {
	ctx stochastikctx.Context
}

func (s *testCacheSuite) SetUpSuite(c *C) {
	ctx := MockContext()
	ctx.GetStochastikVars().SnapshotTS = 0
	ctx.GetStochastikVars().ALLEGROSQLMode = allegrosql.ModeNone
	ctx.GetStochastikVars().TimeZone = time.UTC
	ctx.GetStochastikVars().ConnectionID = 0
	s.ctx = ctx
}

func (s *testCacheSuite) TestCacheKey(c *C) {
	defer testleak.AfterTest(c)()
	key := NewPSTMTPlanCacheKey(s.ctx.GetStochastikVars(), 1, 1)
	c.Assert(key.Hash(), DeepEquals, []byte{0x74, 0x65, 0x73, 0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x74, 0x69, 0x64, 0x62, 0x74, 0x69, 0x6b, 0x76, 0x74, 0x69, 0x66, 0x6c, 0x61, 0x73, 0x68, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
}
