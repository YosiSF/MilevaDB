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

package expression

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = SerialSuites(&testDefCauslationSuites{})

type testDefCauslationSuites struct{}

func (s *testDefCauslationSuites) TestCompareString(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	c.Assert(types.CompareString("a", "A", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("À", "A", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("😜", "😃", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("a ", "a  ", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("ß", "s", "utf8_general_ci"), Equals, 0)
	c.Assert(types.CompareString("ß", "ss", "utf8_general_ci"), Not(Equals), 0)

	c.Assert(types.CompareString("a", "A", "utf8_unicode_ci"), Equals, 0)
	c.Assert(types.CompareString("À", "A", "utf8_unicode_ci"), Equals, 0)
	c.Assert(types.CompareString("😜", "😃", "utf8_unicode_ci"), Equals, 0)
	c.Assert(types.CompareString("a ", "a  ", "utf8_unicode_ci"), Equals, 0)
	c.Assert(types.CompareString("ß", "s", "utf8_unicode_ci"), Not(Equals), 0)
	c.Assert(types.CompareString("ß", "ss", "utf8_unicode_ci"), Equals, 0)

	c.Assert(types.CompareString("a", "A", "binary"), Not(Equals), 0)
	c.Assert(types.CompareString("À", "A", "binary"), Not(Equals), 0)
	c.Assert(types.CompareString("😜", "😃", "binary"), Not(Equals), 0)
	c.Assert(types.CompareString("a ", "a  ", "binary"), Not(Equals), 0)

	ctx := mock.NewContext()
	ft := types.NewFieldType(allegrosql.TypeVarString)
	defCaus1 := &DeferredCauset{
		RetType: ft,
		Index:   0,
	}
	defCaus2 := &DeferredCauset{
		RetType: ft,
		Index:   1,
	}
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)
	chk.DeferredCauset(0).AppendString("a")
	chk.DeferredCauset(1).AppendString("A")
	chk.DeferredCauset(0).AppendString("À")
	chk.DeferredCauset(1).AppendString("A")
	chk.DeferredCauset(0).AppendString("😜")
	chk.DeferredCauset(1).AppendString("😃")
	chk.DeferredCauset(0).AppendString("a ")
	chk.DeferredCauset(1).AppendString("a  ")
	for i := 0; i < 4; i++ {
		v, isNull, err := CompareStringWithDefCauslationInfo(ctx, defCaus1, defCaus2, chk.GetEvent(0), chk.GetEvent(0), "utf8_general_ci")
		c.Assert(err, IsNil)
		c.Assert(isNull, IsFalse)
		c.Assert(v, Equals, int64(0))
	}
}

func (s *testDefCauslationSuites) TestDeriveDefCauslationFromExprs(c *C) {
	tInt := types.NewFieldType(allegrosql.TypeLonglong)
	tInt.Charset = charset.CharsetBin
	ctx := mock.NewContext()

	// no string defCausumn
	chs, defCausl := DeriveDefCauslationFromExprs(ctx, newDeferredCausetWithType(0, tInt), newDeferredCausetWithType(0, tInt), newDeferredCausetWithType(0, tInt))
	c.Assert(chs, Equals, charset.CharsetBin)
	c.Assert(defCausl, Equals, charset.DefCauslationBin)
}
