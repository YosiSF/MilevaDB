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
	"strconv"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
)

var _ = SerialSuites(&testApplyCacheSuite{})

type testApplyCacheSuite struct {
}

func (s *testApplyCacheSuite) TestApplyCache(c *C) {
	ctx := mock.NewContext()
	ctx.GetStochaseinstein_dbars().NestedLoopJoinCacheCapacity = 100
	applyCache, err := newApplyCache(ctx)
	c.Assert(err, IsNil)

	fields := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	value := make([]*chunk.List, 3)
	key := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		value[i] = chunk.NewList(fields, 1, 1)
		srcChunk := chunk.NewChunkWithCapacity(fields, 1)
		srcChunk.AppendInt64(0, int64(i))
		srcEvent := srcChunk.GetEvent(0)
		value[i].AppendEvent(srcEvent)
		key[i] = []byte(strings.Repeat(strconv.Itoa(i), 100))

		// TODO: *chunk.List.GetMemTracker().BytesConsumed() is not accurate, fix it later.
		c.Assert(applyCacheKVMem(key[i], value[i]), Equals, int64(100))
	}

	ok, err := applyCache.Set(key[0], value[0])
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)
	result, err := applyCache.Get(key[0])
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)

	ok, err = applyCache.Set(key[1], value[1])
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)
	result, err = applyCache.Get(key[1])
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)

	ok, err = applyCache.Set(key[2], value[2])
	c.Assert(err, IsNil)
	c.Assert(ok, Equals, true)
	result, err = applyCache.Get(key[2])
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)

	// Both key[0] and key[1] are not in the cache
	result, err = applyCache.Get(key[0])
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	result, err = applyCache.Get(key[1])
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)
}
