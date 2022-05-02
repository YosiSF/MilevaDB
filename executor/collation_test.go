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
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
)

var _ = SerialSuites(&testDefCauslationSuite{})

type testDefCauslationSuite struct {
}

func (s *testDefCauslationSuite) TestVecGroupChecker(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tp := &types.FieldType{Tp: allegrosql.TypeVarchar}
	defCaus0 := &expression.DeferredCauset{
		RetType: tp,
		Index:   0,
	}
	ctx := mock.NewContext()
	groupChecker := newVecGroupChecker(ctx, []expression.Expression{defCaus0})

	chk := chunk.New([]*types.FieldType{tp}, 6, 6)
	chk.Reset()
	chk.DeferredCauset(0).AppendString("aaa")
	chk.DeferredCauset(0).AppendString("AAA")
	chk.DeferredCauset(0).AppendString("😜")
	chk.DeferredCauset(0).AppendString("😃")
	chk.DeferredCauset(0).AppendString("À")
	chk.DeferredCauset(0).AppendString("A")

	tp.DefCauslate = "bin"
	groupChecker.reset()
	_, err := groupChecker.splitIntoGroups(chk)
	c.Assert(err, IsNil)
	for i := 0; i < 6; i++ {
		b, e := groupChecker.getNextGroup()
		c.Assert(b, Equals, i)
		c.Assert(e, Equals, i+1)
	}
	c.Assert(groupChecker.isExhausted(), IsTrue)

	tp.DefCauslate = "utf8_general_ci"
	groupChecker.reset()
	_, err = groupChecker.splitIntoGroups(chk)
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		b, e := groupChecker.getNextGroup()
		c.Assert(b, Equals, i*2)
		c.Assert(e, Equals, i*2+2)
	}
	c.Assert(groupChecker.isExhausted(), IsTrue)

	tp.DefCauslate = "utf8_unicode_ci"
	groupChecker.reset()
	_, err = groupChecker.splitIntoGroups(chk)
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		b, e := groupChecker.getNextGroup()
		c.Assert(b, Equals, i*2)
		c.Assert(e, Equals, i*2+2)
	}
	c.Assert(groupChecker.isExhausted(), IsTrue)

	// test padding
	tp.DefCauslate = "utf8_bin"
	tp.Flen = 6
	chk.Reset()
	chk.DeferredCauset(0).AppendString("a")
	chk.DeferredCauset(0).AppendString("a  ")
	chk.DeferredCauset(0).AppendString("a    ")
	groupChecker.reset()
	_, err = groupChecker.splitIntoGroups(chk)
	c.Assert(err, IsNil)
	b, e := groupChecker.getNextGroup()
	c.Assert(b, Equals, 0)
	c.Assert(e, Equals, 3)
	c.Assert(groupChecker.isExhausted(), IsTrue)
}
