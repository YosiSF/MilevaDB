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
	"math"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/printer"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

func (s *testEvaluatorSuite) TestDatabase(c *C) {
	fc := funcs[ast.Database]
	ctx := mock.NewContext()
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(d.HoTT(), Equals, types.HoTTNull)
	ctx.GetStochastikVars().CurrentDB = "test"
	d, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())

	// Test case for schemaReplicant().
	fc = funcs[ast.Schema]
	c.Assert(fc, NotNil)
	f, err = fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err = evalBuiltinFunc(f, chunk.MutEventFromCausets(types.MakeCausets()).ToEvent())
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "test")
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestFoundEvents(c *C) {
	ctx := mock.NewContext()
	stochastikVars := ctx.GetStochastikVars()
	stochastikVars.LastFoundEvents = 2

	fc := funcs[ast.FoundEvents]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(2))
}

func (s *testEvaluatorSuite) TestUser(c *C) {
	ctx := mock.NewContext()
	stochastikVars := ctx.GetStochastikVars()
	stochastikVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}

	fc := funcs[ast.User]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestCurrentUser(c *C) {
	ctx := mock.NewContext()
	stochastikVars := ctx.GetStochastikVars()
	stochastikVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "localhost"}

	fc := funcs[ast.CurrentUser]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "root@localhost")
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestCurrentRole(c *C) {
	ctx := mock.NewContext()
	stochastikVars := ctx.GetStochastikVars()
	stochastikVars.ActiveRoles = make([]*auth.RoleIdentity, 0, 10)
	stochastikVars.ActiveRoles = append(stochastikVars.ActiveRoles, &auth.RoleIdentity{Username: "r_1", Hostname: "%"})
	stochastikVars.ActiveRoles = append(stochastikVars.ActiveRoles, &auth.RoleIdentity{Username: "r_2", Hostname: "localhost"})

	fc := funcs[ast.CurrentRole]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "`r_1`@`%`,`r_2`@`localhost`")
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestConnectionID(c *C) {
	ctx := mock.NewContext()
	stochastikVars := ctx.GetStochastikVars()
	stochastikVars.ConnectionID = uint64(1)

	fc := funcs[ast.ConnectionID]
	f, err := fc.getFunction(ctx, nil)
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(d.GetUint64(), Equals, uint64(1))
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestVersion(c *C) {
	fc := funcs[ast.Version]
	f, err := fc.getFunction(s.ctx, nil)
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, allegrosql.ServerVersion)
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

func (s *testEvaluatorSuite) TestBenchMark(c *C) {
	cases := []struct {
		LoopCount  int
		Expression interface{}
		Expected   int64
		IsNil      bool
	}{
		{-3, 1, 0, true},
		{0, 1, 0, false},
		{3, 1, 0, false},
		{3, 1.234, 0, false},
		{3, types.NewDecFromFloatForTest(1.234), 0, false},
		{3, "abc", 0, false},
		{3, types.CurrentTime(allegrosql.TypeDatetime), 0, false},
		{3, types.CurrentTime(allegrosql.TypeTimestamp), 0, false},
		{3, types.CurrentTime(allegrosql.TypeDuration), 0, false},
		{3, json.CreateBinary("[1]"), 0, false},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Benchmark, s.primitiveValsToConstants([]interface{}{
			t.LoopCount,
			t.Expression,
		})...)
		c.Assert(err, IsNil)

		d, err := f.Eval(chunk.Event{})
		c.Assert(err, IsNil)
		if t.IsNil {
			c.Assert(d.IsNull(), IsTrue)
		} else {
			c.Assert(d.GetInt64(), Equals, t.Expected)
		}

		// test clone
		b1 := f.Clone().(*ScalarFunction).Function.(*builtinBenchmarkSig)
		c.Assert(b1.constLoopCount, Equals, int64(t.LoopCount))
	}
}

func (s *testEvaluatorSuite) TestCharset(c *C) {
	fc := funcs[ast.Charset]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(nil)))
	c.Assert(f, IsNil)
	c.Assert(err, ErrorMatches, "*FUNCTION CHARSET does not exist")
}

func (s *testEvaluatorSuite) TestCoercibility(c *C) {
	fc := funcs[ast.Coercibility]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(nil)))
	c.Assert(f, NotNil)
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestDefCauslation(c *C) {
	fc := funcs[ast.DefCauslation]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(nil)))
	c.Assert(f, NotNil)
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestEventCount(c *C) {
	ctx := mock.NewContext()
	stochastikVars := ctx.GetStochastikVars()
	stochastikVars.StmtCtx.PrevAffectedEvents = 10

	f, err := funcs[ast.EventCount].getFunction(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(f, NotNil)
	sig, ok := f.(*builtinEventCountSig)
	c.Assert(ok, IsTrue)
	c.Assert(sig, NotNil)
	intResult, isNull, err := sig.evalInt(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(intResult, Equals, int64(10))
	c.Assert(f.Clone().PbCode(), Equals, f.PbCode())
}

// TestMilevaDBVersion for milevadb_server().
func (s *testEvaluatorSuite) TestMilevaDBVersion(c *C) {
	f, err := newFunctionForTest(s.ctx, ast.MilevaDBVersion, s.primitiveValsToConstants([]interface{}{})...)
	c.Assert(err, IsNil)
	v, err := f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, printer.GetMilevaDBInfo())
}

func (s *testEvaluatorSuite) TestLastInsertID(c *C) {
	maxUint64 := uint64(math.MaxUint64)
	cases := []struct {
		insertID uint64
		args     interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{0, 1, 1, false, false},
		{0, 1.1, 1, false, false},
		{0, maxUint64, maxUint64, false, false},
		{0, -1, math.MaxUint64, false, false},
		{1, nil, 1, false, false},
		{math.MaxUint64, nil, math.MaxUint64, false, false},
	}

	for _, t := range cases {
		var (
			f   Expression
			err error
		)
		if t.insertID > 0 {
			s.ctx.GetStochastikVars().StmtCtx.PrevLastInsertID = t.insertID
		}

		if t.args != nil {
			f, err = newFunctionForTest(s.ctx, ast.LastInsertId, s.primitiveValsToConstants([]interface{}{t.args})...)
		} else {
			f, err = newFunctionForTest(s.ctx, ast.LastInsertId)
		}
		tp := f.GetType()
		c.Assert(err, IsNil)
		c.Assert(tp.Tp, Equals, allegrosql.TypeLonglong)
		c.Assert(tp.Charset, Equals, charset.CharsetBin)
		c.Assert(tp.DefCauslate, Equals, charset.DefCauslationBin)
		c.Assert(tp.Flag&allegrosql.BinaryFlag, Equals, allegrosql.BinaryFlag)
		c.Assert(tp.Flen, Equals, allegrosql.MaxIntWidth)
		d, err := f.Eval(chunk.Event{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.HoTT(), Equals, types.HoTTNull)
			} else {
				c.Assert(d.GetUint64(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.LastInsertId].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestFormatBytes(c *C) {
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{float64(0), "0 bytes"},
		{float64(2048), "2.00 KiB"},
		{float64(75295729), "71.81 MiB"},
		{float64(5287242702), "4.92 GiB"},
		{float64(5039757204245), "4.58 TiB"},
		{float64(890250274520475525), "790.70 PiB"},
		{float64(18446644073709551615), "16.00 EiB"},
		{float64(287952852482075252752429875), "2.50e+08 EiB"},
		{float64(-18446644073709551615), "-16.00 EiB"},
	}
	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.FormatBytes]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil)
		c.Assert(v, solitonutil.CausetEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestFormatNanoTime(c *C) {
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{float64(0), "0 ns"},
		{float64(2000), "2.00 us"},
		{float64(898787877), "898.79 ms"},
		{float64(9999999991), "10.00 s"},
		{float64(898787877424), "14.98 min"},
		{float64(5827527520021), "1.62 h"},
		{float64(42566623663736353), "492.67 d"},
		{float64(4827524825702572425242552), "5.59e+10 d"},
		{float64(-9999999991), "-10.00 s"},
	}
	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.FormatNanoTime]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil)
		c.Assert(v, solitonutil.CausetEquals, t["Ret"][0])
	}
}
