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

package core

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/collate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testleak"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

var _ = SerialSuites(&testDistsqlSuite{})

type testDistsqlSuite struct{}

func (s *testDistsqlSuite) TestDeferredCausetToProto(c *C) {
	defer testleak.AfterTest(c)()
	// Make sure the Flag is set in fidelpb.DeferredCausetInfo
	tp := types.NewFieldType(allegrosql.TypeLong)
	tp.Flag = 10
	tp.DefCauslate = "utf8_bin"
	col := &perceptron.DeferredCausetInfo{
		FieldType: *tp,
	}
	pc := soliton.DeferredCausetToProto(col)
	expect := &fidelpb.DeferredCausetInfo{DeferredCausetId: 0, Tp: 3, DefCauslation: 83, DeferredCausetLen: -1, Decimal: -1, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false, XXX_unrecognized: []uint8(nil)}
	c.Assert(pc, DeepEquals, expect)

	defcaus := []*perceptron.DeferredCausetInfo{col, col}
	pcs := soliton.DeferredCausetsToProto(defcaus, false)
	for _, v := range pcs {
		c.Assert(v.GetFlag(), Equals, int32(10))
	}
	pcs = soliton.DeferredCausetsToProto(defcaus, true)
	for _, v := range pcs {
		c.Assert(v.GetFlag(), Equals, int32(10))
	}

	// Make sure the collation ID is successfully set.
	tp = types.NewFieldType(allegrosql.TypeVarchar)
	tp.Flag = 10
	tp.DefCauslate = "latin1_swedish_ci"
	col1 := &perceptron.DeferredCausetInfo{
		FieldType: *tp,
	}
	pc = soliton.DeferredCausetToProto(col1)
	c.Assert(pc.DefCauslation, Equals, int32(8))

	collate.SetNewDefCauslationEnabledForTest(true)
	defer collate.SetNewDefCauslationEnabledForTest(false)

	pc = soliton.DeferredCausetToProto(col)
	expect = &fidelpb.DeferredCausetInfo{DeferredCausetId: 0, Tp: 3, DefCauslation: -83, DeferredCausetLen: -1, Decimal: -1, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false, XXX_unrecognized: []uint8(nil)}
	c.Assert(pc, DeepEquals, expect)
	pcs = soliton.DeferredCausetsToProto(defcaus, true)
	for _, v := range pcs {
		c.Assert(v.DefCauslation, Equals, int32(-83))
	}
	pc = soliton.DeferredCausetToProto(col1)
	c.Assert(pc.DefCauslation, Equals, int32(-8))
}
