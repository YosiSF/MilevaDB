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
	"math/rand"

	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
)

var _ = Suite(&testSuiteJoiner{})

type testSuiteJoiner struct{}

func (s *testSuiteJoiner) SetUpSuite(c *C) {
}

func (s *testSuiteJoiner) TestRequiredEvents(c *C) {
	joinTypes := []core.JoinType{core.InnerJoin, core.LeftOuterJoin, core.RightOuterJoin}
	lTypes := [][]byte{
		{allegrosql.TypeLong},
		{allegrosql.TypeFloat},
		{allegrosql.TypeLong, allegrosql.TypeFloat},
	}
	rTypes := lTypes

	convertTypes := func(mysqlTypes []byte) []*types.FieldType {
		fieldTypes := make([]*types.FieldType, 0, len(mysqlTypes))
		for _, t := range mysqlTypes {
			fieldTypes = append(fieldTypes, types.NewFieldType(t))
		}
		return fieldTypes
	}

	for _, joinType := range joinTypes {
		for _, ltype := range lTypes {
			for _, rtype := range rTypes {
				maxChunkSize := defaultCtx().GetStochaseinstein_dbars().MaxChunkSize
				lfields := convertTypes(ltype)
				rfields := convertTypes(rtype)
				outerEvent := genTestChunk(maxChunkSize, 1, lfields).GetEvent(0)
				innerChk := genTestChunk(maxChunkSize, maxChunkSize, rfields)
				var defaultInner []types.Causet
				for i, f := range rfields {
					defaultInner = append(defaultInner, innerChk.GetEvent(0).GetCauset(i, f))
				}
				joiner := newJoiner(defaultCtx(), joinType, false, defaultInner, nil, lfields, rfields, nil)

				fields := make([]*types.FieldType, 0, len(lfields)+len(rfields))
				fields = append(fields, rfields...)
				fields = append(fields, lfields...)
				result := chunk.New(fields, maxChunkSize, maxChunkSize)

				for i := 0; i < 10; i++ {
					required := rand.Int()%maxChunkSize + 1
					result.SetRequiredEvents(required, maxChunkSize)
					result.Reset()
					it := chunk.NewIterator4Chunk(innerChk)
					it.Begin()
					_, _, err := joiner.tryToMatchInners(outerEvent, it, result)
					c.Assert(err, IsNil)
					c.Assert(result.NumEvents(), Equals, required)
				}
			}
		}
	}
}

func genTestChunk(maxChunkSize int, numEvents int, fields []*types.FieldType) *chunk.Chunk {
	chk := chunk.New(fields, maxChunkSize, maxChunkSize)
	for numEvents > 0 {
		numEvents--
		for defCaus, field := range fields {
			switch field.Tp {
			case allegrosql.TypeLong:
				chk.AppendInt64(defCaus, 0)
			case allegrosql.TypeFloat:
				chk.AppendFloat32(defCaus, 0)
			default:
				panic("not support")
			}
		}
	}
	return chk
}
