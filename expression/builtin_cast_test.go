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

package expression

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
)

func (s *testEvaluatorSuite) TestCastXXX(c *C) {
	ctx, sc := s.ctx, s.ctx.GetStochaseinstein_dbars().StmtCtx

	// Test `cast as char[(N)]` and `cast as binary[(N)]`.
	originIgnoreTruncate := sc.IgnoreTruncate
	originTruncateAsWarning := sc.TruncateAsWarning
	sc.IgnoreTruncate = false
	sc.TruncateAsWarning = true
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
		sc.TruncateAsWarning = originTruncateAsWarning
	}()

	tp := types.NewFieldType(allegrosql.TypeString)
	tp.Flen = 5

	// cast(str as char(N)), N < len([]rune(str)).
	// cast("你好world" as char(5))
	tp.Charset = charset.CharsetUTF8
	f := BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("你好world"), RetType: tp}, tp)
	res, err := f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.GetString(), Equals, "你好wor")

	// cast(str as char(N)), N > len([]rune(str)).
	// cast("a" as char(5))
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("a"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(len(res.GetString()), Equals, 1)
	c.Assert(res.GetString(), Equals, "a")

	// cast(str as binary(N)), N < len(str).
	// cast("你好world" as binary(5))
	str := "你好world"
	tp.Flag |= allegrosql.BinaryFlag
	tp.Charset = charset.CharsetBin
	tp.DefCauslate = charset.DefCauslationBin
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset(str), RetType: types.NewFieldType(allegrosql.TypeString)}, tp)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.GetString(), Equals, str[:5])

	// cast(str as binary(N)), N > len([]byte(str)).
	// cast("a" as binary(5))
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("a"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(len(res.GetString()), Equals, 5)
	c.Assert(res.GetString(), Equals, string([]byte{'a', 0x00, 0x00, 0x00, 0x00}))

	// cast(str as binary(N)), N > len([]byte(str)).
	// cast("a" as binary(4294967295))
	tp.Flen = 4294967295
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("a"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.IsNull(), IsTrue)
	warnings := sc.GetWarnings()
	lastWarn := warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

	origSc := sc
	oldInSelectStmt := sc.InSelectStmt
	sc.InSelectStmt = true
	defer func() {
		sc.InSelectStmt = oldInSelectStmt
	}()
	sc.OverflowAsWarning = true

	// cast('18446744073709551616' as unsigned);
	tp1 := &types.FieldType{
		Tp:          allegrosql.TypeLonglong,
		Flag:        allegrosql.BinaryFlag,
		Charset:     charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
		Flen:        allegrosql.MaxIntWidth,
	}
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("18446744073709551616"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.GetUint64() == math.MaxUint64, IsTrue)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

	originFlag := tp1.Flag
	tp1.Flag |= allegrosql.UnsignedFlag
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("-1"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.GetUint64() == 18446744073709551615, IsTrue)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrCastNegIntAsUnsigned, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))
	tp1.Flag = originFlag

	previousWarnings := len(sc.GetWarnings())
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("-1"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.GetInt64() == -1, IsTrue)
	c.Assert(len(sc.GetWarnings()) == previousWarnings, IsTrue)

	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("-18446744073709551616"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	t := math.MinInt64
	// 9223372036854775808
	c.Assert(res.GetUint64() == uint64(t), IsTrue)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

	// cast('125e342.83' as unsigned)
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("125e342.83"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.GetUint64() == 125, IsTrue)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

	// cast('1e9223372036854775807' as unsigned)
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("1e9223372036854775807"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.GetUint64() == 1, IsTrue)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

	// cast('18446744073709551616' as signed);
	mask := ^allegrosql.UnsignedFlag
	tp1.Flag &= mask
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("18446744073709551616"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Check(res.GetInt64(), Equals, int64(-1))

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

	// cast('18446744073709551614' as signed);
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("18446744073709551614"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Check(res.GetInt64(), Equals, int64(-2))

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrCastAsSignedOverflow, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

	// cast('125e342.83' as signed)
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("125e342.83"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.GetInt64() == 125, IsTrue)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

	// cast('1e9223372036854775807' as signed)
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset("1e9223372036854775807"), RetType: types.NewFieldType(allegrosql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(res.GetInt64() == 1, IsTrue)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

	// create block t1(s1 time);
	// insert into t1 values('11:11:11');
	// select cast(s1 as decimal(7, 2)) from t1;
	ft := &types.FieldType{
		Tp:          allegrosql.TypeNewDecimal,
		Flag:        allegrosql.BinaryFlag | allegrosql.UnsignedFlag,
		Charset:     charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
		Flen:        7,
		Decimal:     2,
	}
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: timeCauset, RetType: types.NewFieldType(allegrosql.TypeDatetime)}, ft)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	resDecimal := new(types.MyDecimal)
	resDecimal.FromString([]byte("99999.99"))
	c.Assert(res.GetMysqlDecimal().Compare(resDecimal), Equals, 0)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrOverflow, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))
	sc = origSc

	// create block tt(a bigint unsigned);
	// insert into tt values(18446744073709551615);
	// select cast(a as decimal(65, 0)) from tt;
	ft = &types.FieldType{
		Tp:          allegrosql.TypeNewDecimal,
		Flag:        allegrosql.BinaryFlag,
		Charset:     charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
		Flen:        65,
		Decimal:     0,
	}
	rt := types.NewFieldType(allegrosql.TypeLonglong)
	rt.Flag = allegrosql.BinaryFlag | allegrosql.UnsignedFlag
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewUintCauset(18446744073709551615), RetType: rt}, ft)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	u, err := res.GetMysqlDecimal().ToUint()
	c.Assert(err, IsNil)
	c.Assert(u == 18446744073709551615, IsTrue)

	// cast(bad_string as decimal)
	for _, s := range []string{"hello", ""} {
		f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset(s), RetType: types.NewFieldType(allegrosql.TypeNewDecimal)}, tp)
		res, err = f.Eval(chunk.Event{})
		c.Assert(err, IsNil)
	}

	// cast(1234 as char(0))
	tp.Flen = 0
	tp.Charset = charset.CharsetUTF8
	f = BuildCastFunction(ctx, &CouplingConstantWithRadix{Value: types.NewCauset(1234), RetType: types.NewFieldType(allegrosql.TypeString)}, tp)
	res, err = f.Eval(chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(len(res.GetString()), Equals, 0)
	c.Assert(res.GetString(), Equals, "")
}

var (
	year, month, day     = time.Now().In(time.UTC).Date()
	curDateInt           = int64(year*10000 + int(month)*100 + day)
	curTimeInt           = curDateInt*1000000 + 125959
	curTimeWithFspReal   = float64(curTimeInt) + 0.555
	curTimeString        = fmt.Sprintf("%4d-%02d-%02d 12:59:59", year, int(month), day)
	curTimeWithFspString = fmt.Sprintf("%4d-%02d-%02d 12:59:59.555000", year, int(month), day)
	tm                   = types.NewTime(types.FromDate(year, int(month), day, 12, 59, 59, 0), allegrosql.TypeDatetime, types.DefaultFsp)
	tmWithFsp            = types.NewTime(types.FromDate(year, int(month), day, 12, 59, 59, 555000), allegrosql.TypeDatetime, types.MaxFsp)
	// timeCauset indicates datetime "curYear-curMonth-curDay 12:59:59".
	timeCauset = types.NewCauset(tm)
	// timeWithFsFIDelatum indicates datetime "curYear-curMonth-curDay 12:59:59.555000".
	timeWithFsFIDelatum = types.NewCauset(tmWithFsp)
	duration            = types.Duration{
		Duration: 12*time.Hour + 59*time.Minute + 59*time.Second,
		Fsp:      types.DefaultFsp}
	// durationCauset indicates duration "12:59:59".
	durationCauset  = types.NewCauset(duration)
	durationWithFsp = types.Duration{
		Duration: 12*time.Hour + 59*time.Minute + 59*time.Second + 555*time.Millisecond,
		Fsp:      3}
	// durationWithFsFIDelatum indicates duration "12:59:59.555"
	durationWithFsFIDelatum = types.NewCauset(durationWithFsp)
	dt                      = types.NewTime(types.FromDate(year, int(month), day, 0, 0, 0, 0), allegrosql.TypeDate, types.DefaultFsp)

	// jsonInt indicates json(3)
	jsonInt = types.NewCauset(json.CreateBinary(int64(3)))

	// jsonTime indicates "CURRENT_DAY 12:59:59"
	jsonTime = types.NewCauset(json.CreateBinary(tm.String()))

	// jsonDuration indicates
	jsonDuration = types.NewCauset(json.CreateBinary(duration.String()))
)

func (s *testEvaluatorSuite) TestCastFuncSig(c *C) {
	ctx, sc := s.ctx, s.ctx.GetStochaseinstein_dbars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	originTZ := sc.TimeZone
	sc.IgnoreTruncate = true
	sc.TimeZone = time.UTC
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
		sc.TimeZone = originTZ
	}()
	var sig builtinFunc

	durationDeferredCauset := &DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0}
	durationDeferredCauset.RetType.Decimal = int(types.DefaultFsp)
	// Test cast as Decimal.
	castToDecCases := []struct {
		before *DeferredCauset
		after  *types.MyDecimal
		event  chunk.MutEvent
	}{
		// cast int as decimal.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0},
			types.NewDecFromInt(1),
			chunk.MutEventFromCausets([]types.Causet{types.NewIntCauset(1)}),
		},
		// cast string as decimal.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			types.NewDecFromInt(1),
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("1")}),
		},
		// cast real as decimal.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			types.NewDecFromInt(1),
			chunk.MutEventFromCausets([]types.Causet{types.NewFloat64Causet(1)}),
		},
		// cast Time as decimal.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			types.NewDecFromInt(curTimeInt),
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
		},
		// cast Duration as decimal.
		{
			durationDeferredCauset,
			types.NewDecFromInt(125959),
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
		},
	}
	for i, t := range castToDecCases {
		args := []Expression{t.before}
		b, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		decFunc := newBaseBuiltinCastFunc(b, false)
		decFunc.tp = types.NewFieldType(allegrosql.TypeNewDecimal)
		switch i {
		case 0:
			sig = &builtinCastIntAsDecimalSig{decFunc}
		case 1:
			sig = &builtinCastStringAsDecimalSig{decFunc}
		case 2:
			sig = &builtinCastRealAsDecimalSig{decFunc}
		case 3:
			sig = &builtinCastTimeAsDecimalSig{decFunc}
		case 4:
			sig = &builtinCastDurationAsDecimalSig{decFunc}
		case 5:
			sig = &builtinCastDecimalAsDecimalSig{decFunc}
		}
		res, isNull, err := sig.evalDecimal(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.Compare(t.after), Equals, 0)
	}

	durationDeferredCauset.RetType.Decimal = 1
	castToDecCases2 := []struct {
		before  *DeferredCauset
		flen    int
		decimal int
		after   *types.MyDecimal
		event   chunk.MutEvent
	}{
		// cast int as decimal.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0},
			7,
			3,
			types.NewDecFromStringForTest("1234.000"),
			chunk.MutEventFromCausets([]types.Causet{types.NewIntCauset(1234)}),
		},
		// cast string as decimal.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			7,
			3,
			types.NewDecFromStringForTest("1234.000"),
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("1234")}),
		},
		// cast real as decimal.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			8,
			4,
			types.NewDecFromStringForTest("1234.1230"),
			chunk.MutEventFromCausets([]types.Causet{types.NewFloat64Causet(1234.123)}),
		},
		// cast Time as decimal.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			15,
			1,
			types.NewDecFromStringForTest(strconv.FormatInt(curTimeInt, 10) + ".0"),
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
		},
		// cast Duration as decimal.
		{
			durationDeferredCauset,
			7,
			1,
			types.NewDecFromStringForTest("125959.0"),
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
		},
		// cast decimal as decimal.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			7,
			3,
			types.NewDecFromStringForTest("1234.000"),
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromStringForTest("1234"))}),
		},
	}

	for i, t := range castToDecCases2 {
		args := []Expression{t.before}
		tp := types.NewFieldType(allegrosql.TypeNewDecimal)
		tp.Flen, tp.Decimal = t.flen, t.decimal
		b, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		decFunc := newBaseBuiltinCastFunc(b, false)
		decFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastIntAsDecimalSig{decFunc}
		case 1:
			sig = &builtinCastStringAsDecimalSig{decFunc}
		case 2:
			sig = &builtinCastRealAsDecimalSig{decFunc}
		case 3:
			sig = &builtinCastTimeAsDecimalSig{decFunc}
		case 4:
			sig = &builtinCastDurationAsDecimalSig{decFunc}
		case 5:
			sig = &builtinCastDecimalAsDecimalSig{decFunc}
		}
		res, isNull, err := sig.evalDecimal(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.ToString(), DeepEquals, t.after.ToString())
	}

	durationDeferredCauset.RetType.Decimal = 0
	// Test cast as int.
	castToIntCases := []struct {
		before *DeferredCauset
		after  int64
		event  chunk.MutEvent
	}{
		// cast string as int.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			1,
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("1")}),
		},
		// cast decimal as int.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			1,
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromInt(1))}),
		},
		// cast real as int.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			1,
			chunk.MutEventFromCausets([]types.Causet{types.NewFloat64Causet(1)}),
		},
		// cast Time as int.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			curTimeInt,
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
		},
		// cast Duration as int.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0},
			125959,
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
		},
		// cast JSON as int.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeJSON), Index: 0},
			3,
			chunk.MutEventFromCausets([]types.Causet{jsonInt}),
		},
	}
	for i, t := range castToIntCases {
		args := []Expression{t.before}
		b, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		intFunc := newBaseBuiltinCastFunc(b, false)
		switch i {
		case 0:
			sig = &builtinCastStringAsIntSig{intFunc}
		case 1:
			sig = &builtinCastDecimalAsIntSig{intFunc}
		case 2:
			sig = &builtinCastRealAsIntSig{intFunc}
		case 3:
			sig = &builtinCastTimeAsIntSig{intFunc}
		case 4:
			sig = &builtinCastDurationAsIntSig{intFunc}
		case 5:
			sig = &builtinCastJSONAsIntSig{intFunc}
		}
		res, isNull, err := sig.evalInt(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.after)
	}

	// Test cast as real.
	castToRealCases := []struct {
		before *DeferredCauset
		after  float64
		event  chunk.MutEvent
	}{
		// cast string as real.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			1.1,
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("1.1")}),
		},
		// cast decimal as real.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			1.1,
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromFloatForTest(1.1))}),
		},
		// cast int as real.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0},
			1,
			chunk.MutEventFromCausets([]types.Causet{types.NewIntCauset(1)}),
		},
		// cast Time as real.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			float64(curTimeInt),
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
		},
		// cast Duration as real.
		{
			durationDeferredCauset,
			125959,
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
		},
		// cast JSON as real.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeJSON), Index: 0},
			3.0,
			chunk.MutEventFromCausets([]types.Causet{jsonInt}),
		},
	}
	for i, t := range castToRealCases {
		args := []Expression{t.before}
		b, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		realFunc := newBaseBuiltinCastFunc(b, false)
		switch i {
		case 0:
			sig = &builtinCastStringAsRealSig{realFunc}
		case 1:
			sig = &builtinCastDecimalAsRealSig{realFunc}
		case 2:
			sig = &builtinCastIntAsRealSig{realFunc}
		case 3:
			sig = &builtinCastTimeAsRealSig{realFunc}
		case 4:
			sig = &builtinCastDurationAsRealSig{realFunc}
		case 5:
			sig = &builtinCastJSONAsRealSig{realFunc}
		}
		res, isNull, err := sig.evalReal(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.after)
	}

	// Test cast as string.
	castToStringCases := []struct {
		before *DeferredCauset
		after  string
		event  chunk.MutEvent
	}{
		// cast real as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			"1",
			chunk.MutEventFromCausets([]types.Causet{types.NewFloat64Causet(1)}),
		},
		// cast decimal as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			"1",
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromInt(1))}),
		},
		// cast int as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0},
			"1",
			chunk.MutEventFromCausets([]types.Causet{types.NewIntCauset(1)}),
		},
		// cast time as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			curTimeString,
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
		},
		// cast duration as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0},
			"12:59:59",
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
		},
		// cast JSON as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeJSON), Index: 0},
			"3",
			chunk.MutEventFromCausets([]types.Causet{jsonInt}),
		},
		// cast string as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			"1234",
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("1234")}),
		},
	}
	for i, t := range castToStringCases {
		tp := types.NewFieldType(allegrosql.TypeVarString)
		tp.Charset = charset.CharsetBin
		args := []Expression{t.before}
		stringFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		stringFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsStringSig{stringFunc}
		case 1:
			sig = &builtinCastDecimalAsStringSig{stringFunc}
		case 2:
			sig = &builtinCastIntAsStringSig{stringFunc}
		case 3:
			sig = &builtinCastTimeAsStringSig{stringFunc}
		case 4:
			sig = &builtinCastDurationAsStringSig{stringFunc}
		case 5:
			sig = &builtinCastJSONAsStringSig{stringFunc}
		case 6:
			sig = &builtinCastStringAsStringSig{stringFunc}
		}
		res, isNull, err := sig.evalString(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.after)
	}

	// Test cast as string.
	castToStringCases2 := []struct {
		before *DeferredCauset
		after  string
		flen   int
		event  chunk.MutEvent
	}{
		// cast real as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			"123",
			3,
			chunk.MutEventFromCausets([]types.Causet{types.NewFloat64Causet(1234.123)}),
		},
		// cast decimal as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			"123",
			3,
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromStringForTest("1234.123"))}),
		},
		// cast int as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0},
			"123",
			3,
			chunk.MutEventFromCausets([]types.Causet{types.NewIntCauset(1234)}),
		},
		// cast time as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			curTimeString[:3],
			3,
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
		},
		// cast duration as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0},
			"12:",
			3,
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
		},
		// cast string as string.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			"你好w",
			3,
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("你好world")}),
		},
	}
	for i, t := range castToStringCases2 {
		args := []Expression{t.before}
		tp := types.NewFieldType(allegrosql.TypeVarString)
		tp.Flen, tp.Charset = t.flen, charset.CharsetBin
		stringFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		stringFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsStringSig{stringFunc}
		case 1:
			sig = &builtinCastDecimalAsStringSig{stringFunc}
		case 2:
			sig = &builtinCastIntAsStringSig{stringFunc}
		case 3:
			sig = &builtinCastTimeAsStringSig{stringFunc}
		case 4:
			sig = &builtinCastDurationAsStringSig{stringFunc}
		case 5:
			stringFunc.tp.Charset = charset.CharsetUTF8
			sig = &builtinCastStringAsStringSig{stringFunc}
		}
		res, isNull, err := sig.evalString(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.after)
	}

	castToTimeCases := []struct {
		before *DeferredCauset
		after  types.Time
		event  chunk.MutEvent
	}{
		// cast real as Time.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			tm,
			chunk.MutEventFromCausets([]types.Causet{types.NewFloat64Causet(float64(curTimeInt))}),
		},
		// cast decimal as Time.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			tm,
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromInt(curTimeInt))}),
		},
		// cast int as Time.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0},
			tm,
			chunk.MutEventFromCausets([]types.Causet{types.NewIntCauset(curTimeInt)}),
		},
		// cast string as Time.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			tm,
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset(curTimeString)}),
		},
		// cast Duration as Time.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0},
			tm,
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
		},
		// cast JSON as Time.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeJSON), Index: 0},
			tm,
			chunk.MutEventFromCausets([]types.Causet{jsonTime}),
		},
		// cast Time as Time.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			tm,
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
		},
	}
	for i, t := range castToTimeCases {
		args := []Expression{t.before}
		tp := types.NewFieldType(allegrosql.TypeDatetime)
		tp.Decimal = int(types.DefaultFsp)
		timeFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		timeFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsTimeSig{timeFunc}
		case 1:
			sig = &builtinCastDecimalAsTimeSig{timeFunc}
		case 2:
			sig = &builtinCastIntAsTimeSig{timeFunc}
		case 3:
			sig = &builtinCastStringAsTimeSig{timeFunc}
		case 4:
			sig = &builtinCastDurationAsTimeSig{timeFunc}
		case 5:
			sig = &builtinCastJSONAsTimeSig{timeFunc}
		case 6:
			sig = &builtinCastTimeAsTimeSig{timeFunc}
		}
		res, isNull, err := sig.evalTime(t.event.ToEvent())
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(res.String(), Equals, t.after.String())
	}

	castToTimeCases2 := []struct {
		before *DeferredCauset
		after  types.Time
		fsp    int8
		tp     byte
		event  chunk.MutEvent
	}{
		// cast real as Time(0).
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			dt,
			types.DefaultFsp,
			allegrosql.TypeDate,
			chunk.MutEventFromCausets([]types.Causet{types.NewFloat64Causet(float64(curTimeInt))}),
		},
		// cast decimal as Date.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			dt,
			types.DefaultFsp,
			allegrosql.TypeDate,
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromInt(curTimeInt))}),
		},
		// cast int as Datetime(6).
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0},
			tm,
			types.MaxFsp,
			allegrosql.TypeDatetime,
			chunk.MutEventFromCausets([]types.Causet{types.NewIntCauset(curTimeInt)}),
		},
		// cast string as Datetime(6).
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			tm,
			types.MaxFsp,
			allegrosql.TypeDatetime,
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset(curTimeString)}),
		},
		// cast Duration as Date.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0},
			dt,
			types.DefaultFsp,
			allegrosql.TypeDate,
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
		},
		// cast Time as Date.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			dt,
			types.DefaultFsp,
			allegrosql.TypeDate,
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
		},
	}
	for i, t := range castToTimeCases2 {
		args := []Expression{t.before}
		tp := types.NewFieldType(t.tp)
		tp.Decimal = int(t.fsp)
		timeFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		timeFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsTimeSig{timeFunc}
		case 1:
			sig = &builtinCastDecimalAsTimeSig{timeFunc}
		case 2:
			sig = &builtinCastIntAsTimeSig{timeFunc}
		case 3:
			sig = &builtinCastStringAsTimeSig{timeFunc}
		case 4:
			sig = &builtinCastDurationAsTimeSig{timeFunc}
		case 5:
			sig = &builtinCastTimeAsTimeSig{timeFunc}
		}
		res, isNull, err := sig.evalTime(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		resAfter := t.after.String()
		if t.fsp > 0 {
			resAfter += "."
			for i := 0; i < int(t.fsp); i++ {
				resAfter += "0"
			}
		}
		c.Assert(res.String(), Equals, resAfter)
	}

	castToDurationCases := []struct {
		before *DeferredCauset
		after  types.Duration
		event  chunk.MutEvent
	}{
		// cast real as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{types.NewFloat64Causet(125959)}),
		},
		// cast decimal as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromInt(125959))}),
		},
		// cast int as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{types.NewIntCauset(125959)}),
		},
		// cast string as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("12:59:59")}),
		},
		// cast Time as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
		},
		// cast JSON as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeJSON), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{jsonDuration}),
		},
		// cast Duration as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
		},
	}
	for i, t := range castToDurationCases {
		args := []Expression{t.before}
		tp := types.NewFieldType(allegrosql.TypeDuration)
		tp.Decimal = int(types.DefaultFsp)
		durationFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		durationFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsDurationSig{durationFunc}
		case 1:
			sig = &builtinCastDecimalAsDurationSig{durationFunc}
		case 2:
			sig = &builtinCastIntAsDurationSig{durationFunc}
		case 3:
			sig = &builtinCastStringAsDurationSig{durationFunc}
		case 4:
			sig = &builtinCastTimeAsDurationSig{durationFunc}
		case 5:
			sig = &builtinCastJSONAsDurationSig{durationFunc}
		case 6:
			sig = &builtinCastDurationAsDurationSig{durationFunc}
		}
		res, isNull, err := sig.evalDuration(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.String(), Equals, t.after.String())
	}

	castToDurationCases2 := []struct {
		before *DeferredCauset
		after  types.Duration
		event  chunk.MutEvent
		fsp    int
	}{
		// cast real as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{types.NewFloat64Causet(125959)}),
			1,
		},
		// cast decimal as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromInt(125959))}),
			2,
		},
		// cast int as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{types.NewIntCauset(125959)}),
			3,
		},
		// cast string as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("12:59:59")}),
			4,
		},
		// cast Time as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
			5,
		},
		// cast Duration as Duration.
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0},
			duration,
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
			6,
		},
	}
	for i, t := range castToDurationCases2 {
		args := []Expression{t.before}
		tp := types.NewFieldType(allegrosql.TypeDuration)
		tp.Decimal = t.fsp
		durationFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		c.Assert(err, IsNil)
		durationFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsDurationSig{durationFunc}
		case 1:
			sig = &builtinCastDecimalAsDurationSig{durationFunc}
		case 2:
			sig = &builtinCastIntAsDurationSig{durationFunc}
		case 3:
			sig = &builtinCastStringAsDurationSig{durationFunc}
		case 4:
			sig = &builtinCastTimeAsDurationSig{durationFunc}
		case 5:
			sig = &builtinCastDurationAsDurationSig{durationFunc}
		}
		res, isNull, err := sig.evalDuration(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		resAfter := t.after.String()
		if t.fsp > 0 {
			resAfter += "."
			for j := 0; j < t.fsp; j++ {
				resAfter += "0"
			}
		}
		c.Assert(res.String(), Equals, resAfter)
	}

	// null case
	args := []Expression{&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0}}
	event := chunk.MutEventFromCausets([]types.Causet{types.NewCauset(nil)})
	bf, err := newBaseBuiltinFunc(ctx, "", args, 0)
	c.Assert(err, IsNil)
	bf.tp = types.NewFieldType(allegrosql.TypeVarString)
	sig = &builtinCastRealAsStringSig{bf}
	sRes, isNull, err := sig.evalString(event.ToEvent())
	c.Assert(sRes, Equals, "")
	c.Assert(isNull, Equals, true)
	c.Assert(err, IsNil)

	// test hybridType case.
	args = []Expression{&CouplingConstantWithRadix{Value: types.NewCauset(types.Enum{Name: "a", Value: 0}), RetType: types.NewFieldType(allegrosql.TypeEnum)}}
	b, err := newBaseBuiltinFunc(ctx, "", args, 0)
	c.Assert(err, IsNil)
	sig = &builtinCastStringAsIntSig{newBaseBuiltinCastFunc(b, false)}
	iRes, isNull, err := sig.evalInt(chunk.Event{})
	c.Assert(isNull, Equals, false)
	c.Assert(err, IsNil)
	c.Assert(iRes, Equals, int64(0))
}

func (s *testEvaluatorSuite) TestCastJSONAsDecimalSig(c *C) {
	ctx, sc := s.ctx, s.ctx.GetStochaseinstein_dbars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
	}()

	defCaus := &DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeJSON), Index: 0}
	b, err := newBaseBuiltinFunc(ctx, "", []Expression{defCaus}, 0)
	c.Assert(err, IsNil)
	decFunc := newBaseBuiltinCastFunc(b, false)
	decFunc.tp = types.NewFieldType(allegrosql.TypeNewDecimal)
	decFunc.tp.Flen = 60
	decFunc.tp.Decimal = 2
	sig := &builtinCastJSONAsDecimalSig{decFunc}

	var tests = []struct {
		In  string
		Out *types.MyDecimal
	}{
		{`{}`, types.NewDecFromStringForTest("0")},
		{`[]`, types.NewDecFromStringForTest("0")},
		{`3`, types.NewDecFromStringForTest("3")},
		{`-3`, types.NewDecFromStringForTest("-3")},
		{`4.5`, types.NewDecFromStringForTest("4.5")},
		{`"1234"`, types.NewDecFromStringForTest("1234")},
		// test truncate
		{`"1234.1234"`, types.NewDecFromStringForTest("1234.12")},
		{`"1234.4567"`, types.NewDecFromStringForTest("1234.46")},
		// test big decimal
		{`"1234567890123456789012345678901234567890123456789012345"`, types.NewDecFromStringForTest("1234567890123456789012345678901234567890123456789012345")},
	}
	for _, tt := range tests {
		j, err := json.ParseBinaryFromString(tt.In)
		c.Assert(err, IsNil)
		event := chunk.MutEventFromCausets([]types.Causet{types.NewCauset(j)})
		res, isNull, err := sig.evalDecimal(event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.Compare(tt.Out), Equals, 0)
	}
}

// TestWrapWithCastAsTypesClasses tests WrapWithCastAsInt/Real/String/Decimal.
func (s *testEvaluatorSuite) TestWrapWithCastAsTypesClasses(c *C) {
	ctx := s.ctx

	durationDeferredCauset0 := &DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0}
	durationDeferredCauset0.RetType.Decimal = int(types.DefaultFsp)
	durationDeferredCauset3 := &DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDuration), Index: 0}
	durationDeferredCauset3.RetType.Decimal = 3
	cases := []struct {
		expr      Expression
		event     chunk.MutEvent
		intRes    int64
		realRes   float64
		decRes    *types.MyDecimal
		stringRes string
	}{
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLong), Index: 0},
			chunk.MutEventFromCausets([]types.Causet{types.NewCauset(123)}),
			123, 123, types.NewDecFromInt(123), "123",
		},
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			chunk.MutEventFromCausets([]types.Causet{types.NewCauset(123.555)}),
			124, 123.555, types.NewDecFromFloatForTest(123.555), "123.555",
		},
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDouble), Index: 0},
			chunk.MutEventFromCausets([]types.Causet{types.NewCauset(123.123)}),
			123, 123.123, types.NewDecFromFloatForTest(123.123), "123.123",
		},
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromStringForTest("123.123"))}),
			123, 123.123, types.NewDecFromFloatForTest(123.123), "123.123",
		},
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Index: 0},
			chunk.MutEventFromCausets([]types.Causet{types.NewDecimalCauset(types.NewDecFromStringForTest("123.555"))}),
			124, 123.555, types.NewDecFromFloatForTest(123.555), "123.555",
		},
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeVarString), Index: 0},
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("123.123")}),
			123, 123.123, types.NewDecFromStringForTest("123.123"), "123.123",
		},
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			chunk.MutEventFromCausets([]types.Causet{timeCauset}),
			curTimeInt, float64(curTimeInt), types.NewDecFromInt(curTimeInt), curTimeString,
		},
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeDatetime), Index: 0},
			chunk.MutEventFromCausets([]types.Causet{timeWithFsFIDelatum}),
			curDateInt*1000000 + 130000, curTimeWithFspReal, types.NewDecFromFloatForTest(curTimeWithFspReal), curTimeWithFspString,
		},
		{
			durationDeferredCauset0,
			chunk.MutEventFromCausets([]types.Causet{durationCauset}),
			125959, 125959, types.NewDecFromFloatForTest(125959), "12:59:59",
		},
		{
			durationDeferredCauset3,
			chunk.MutEventFromCausets([]types.Causet{durationWithFsFIDelatum}),
			130000, 125959.555, types.NewDecFromFloatForTest(125959.555), "12:59:59.555",
		},
		{
			&DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeEnum), Index: 0},
			chunk.MutEventFromCausets([]types.Causet{types.NewCauset(types.Enum{Name: "a", Value: 123})}),
			123, 123, types.NewDecFromStringForTest("123"), "a",
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeVarString), Value: types.NewBinaryLiteralCauset(types.NewBinaryLiteralFromUint(0x61, -1))},
			chunk.MutEventFromCausets([]types.Causet{types.NewCauset(nil)}),
			97, 97, types.NewDecFromInt(0x61), "a",
		},
	}
	for i, t := range cases {
		// Test wrapping with CastAsInt.
		intExpr := WrapWithCastAsInt(ctx, t.expr)
		c.Assert(intExpr.GetType().EvalType(), Equals, types.CausetEDN)
		intRes, isNull, err := intExpr.EvalInt(ctx, t.event.ToEvent())
		c.Assert(err, IsNil, Commentf("cast[%v]: %#v", i, t))
		c.Assert(isNull, Equals, false)
		c.Assert(intRes, Equals, t.intRes)

		// Test wrapping with CastAsReal.
		realExpr := WrapWithCastAsReal(ctx, t.expr)
		c.Assert(realExpr.GetType().EvalType(), Equals, types.ETReal)
		realRes, isNull, err := realExpr.EvalReal(ctx, t.event.ToEvent())
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(realRes, Equals, t.realRes, Commentf("cast[%v]: %#v", i, t))

		// Test wrapping with CastAsDecimal.
		decExpr := WrapWithCastAsDecimal(ctx, t.expr)
		c.Assert(decExpr.GetType().EvalType(), Equals, types.ETDecimal)
		decRes, isNull, err := decExpr.EvalDecimal(ctx, t.event.ToEvent())
		c.Assert(err, IsNil, Commentf("case[%v]: %#v\n", i, t))
		c.Assert(isNull, Equals, false)
		c.Assert(decRes.Compare(t.decRes), Equals, 0, Commentf("case[%v]: %#v\n", i, t))

		// Test wrapping with CastAsString.
		strExpr := WrapWithCastAsString(ctx, t.expr)
		c.Assert(strExpr.GetType().EvalType().IsStringHoTT(), IsTrue)
		strRes, isNull, err := strExpr.EvalString(ctx, t.event.ToEvent())
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(strRes, Equals, t.stringRes)
	}

	unsignedIntExpr := &DeferredCauset{RetType: &types.FieldType{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag, Flen: allegrosql.MaxIntWidth, Decimal: 0}, Index: 0}

	// test cast unsigned int as string.
	strExpr := WrapWithCastAsString(ctx, unsignedIntExpr)
	c.Assert(strExpr.GetType().EvalType().IsStringHoTT(), IsTrue)
	strRes, isNull, err := strExpr.EvalString(ctx, chunk.MutEventFromCausets([]types.Causet{types.NewUintCauset(math.MaxUint64)}).ToEvent())
	c.Assert(err, IsNil)
	c.Assert(strRes, Equals, strconv.FormatUint(math.MaxUint64, 10))
	c.Assert(isNull, Equals, false)

	strRes, isNull, err = strExpr.EvalString(ctx, chunk.MutEventFromCausets([]types.Causet{types.NewUintCauset(1234)}).ToEvent())
	c.Assert(err, IsNil)
	c.Assert(isNull, Equals, false)
	c.Assert(strRes, Equals, strconv.FormatUint(uint64(1234), 10))

	// test cast unsigned int as decimal.
	decExpr := WrapWithCastAsDecimal(ctx, unsignedIntExpr)
	c.Assert(decExpr.GetType().EvalType(), Equals, types.ETDecimal)
	decRes, isNull, err := decExpr.EvalDecimal(ctx, chunk.MutEventFromCausets([]types.Causet{types.NewUintCauset(uint64(1234))}).ToEvent())
	c.Assert(err, IsNil)
	c.Assert(isNull, Equals, false)
	c.Assert(decRes.Compare(types.NewDecFromUint(uint64(1234))), Equals, 0)

	// test cast unsigned int as Time.
	timeExpr := WrapWithCastAsTime(ctx, unsignedIntExpr, types.NewFieldType(allegrosql.TypeDatetime))
	c.Assert(timeExpr.GetType().Tp, Equals, allegrosql.TypeDatetime)
	timeRes, isNull, err := timeExpr.EvalTime(ctx, chunk.MutEventFromCausets([]types.Causet{types.NewUintCauset(uint64(curTimeInt))}).ToEvent())
	c.Assert(err, IsNil)
	c.Assert(isNull, Equals, false)
	c.Assert(timeRes.Compare(tm), Equals, 0)
}

func (s *testEvaluatorSuite) TestWrapWithCastAsTime(c *C) {
	sc := s.ctx.GetStochaseinstein_dbars().StmtCtx
	save := sc.TimeZone
	sc.TimeZone = time.UTC
	defer func() {
		sc.TimeZone = save
	}()
	cases := []struct {
		expr Expression
		tp   *types.FieldType
		res  types.Time
	}{
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeLong), Value: types.NewIntCauset(curTimeInt)},
			types.NewFieldType(allegrosql.TypeDate),
			dt,
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeDouble), Value: types.NewFloat64Causet(float64(curTimeInt))},
			types.NewFieldType(allegrosql.TypeDatetime),
			tm,
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Value: types.NewDecimalCauset(types.NewDecFromInt(curTimeInt))},
			types.NewFieldType(allegrosql.TypeDate),
			dt,
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeVarString), Value: types.NewStringCauset(curTimeString)},
			types.NewFieldType(allegrosql.TypeDatetime),
			tm,
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeDatetime), Value: timeCauset},
			types.NewFieldType(allegrosql.TypeDate),
			dt,
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeDuration), Value: durationCauset},
			types.NewFieldType(allegrosql.TypeDatetime),
			tm,
		},
	}
	for d, t := range cases {
		expr := WrapWithCastAsTime(s.ctx, t.expr, t.tp)
		res, isNull, err := expr.EvalTime(s.ctx, chunk.Event{})
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(res.Type(), Equals, t.tp.Tp)
		c.Assert(res.Compare(t.res), Equals, 0, Commentf("case %d res = %s, expect = %s", d, res, t.res))
	}
}

func (s *testEvaluatorSuite) TestWrapWithCastAsDuration(c *C) {
	cases := []struct {
		expr Expression
	}{
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeLong), Value: types.NewIntCauset(125959)},
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeDouble), Value: types.NewFloat64Causet(125959)},
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeNewDecimal), Value: types.NewDecimalCauset(types.NewDecFromInt(125959))},
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeVarString), Value: types.NewStringCauset("125959")},
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeDatetime), Value: timeCauset},
		},
		{
			&CouplingConstantWithRadix{RetType: types.NewFieldType(allegrosql.TypeDuration), Value: durationCauset},
		},
	}
	for _, t := range cases {
		expr := WrapWithCastAsDuration(s.ctx, t.expr)
		res, isNull, err := expr.EvalDuration(s.ctx, chunk.Event{})
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(res.Compare(duration), Equals, 0)
	}
}

func (s *testEvaluatorSuite) TestWrapWithCastAsJSON(c *C) {
	input := &DeferredCauset{RetType: &types.FieldType{Tp: allegrosql.TypeJSON}}
	expr := WrapWithCastAsJSON(s.ctx, input)

	output, ok := expr.(*DeferredCauset)
	c.Assert(ok, IsTrue)
	c.Assert(output, Equals, input)
}

func (s *testEvaluatorSuite) TestCastIntAsIntVec(c *C) {
	cast, input, result := genCastIntAsInt()
	c.Assert(cast.vecEvalInt(input, result), IsNil)
	i64s := result.Int64s()
	it := chunk.NewIterator4Chunk(input)
	i := 0
	for event := it.Begin(); event != it.End(); event = it.Next() {
		v, _, err := cast.evalInt(event)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, i64s[i])
		i++
	}

	cast.inUnion = true
	cast.getRetTp().Flag |= allegrosql.UnsignedFlag
	c.Assert(cast.vecEvalInt(input, result), IsNil)
	i64s = result.Int64s()
	it = chunk.NewIterator4Chunk(input)
	i = 0
	for event := it.Begin(); event != it.End(); event = it.Next() {
		v, _, err := cast.evalInt(event)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, i64s[i])
		i++
	}
}

// for issue https://github.com/whtcorpsinc/MilevaDB-Prod/issues/16825
func (s *testEvaluatorSuite) TestCastStringAsDecimalSigWithUnsignedFlagInUnion(c *C) {
	defCaus := &DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString), Index: 0}
	b, err := newBaseBuiltinFunc(mock.NewContext(), "", []Expression{defCaus}, 0)
	c.Assert(err, IsNil)
	// set `inUnion` to `true`
	decFunc := newBaseBuiltinCastFunc(b, true)
	decFunc.tp = types.NewFieldType(allegrosql.TypeNewDecimal)
	// set the `UnsignedFlag` bit
	decFunc.tp.Flag |= allegrosql.UnsignedFlag
	cast := &builtinCastStringAsDecimalSig{decFunc}

	cases := []struct {
		event chunk.MutEvent
		res   *types.MyDecimal
	}{
		// if `inUnion` is `true`, the result of cast a positive decimal string to unsigned decimal should be normal
		{
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("1")}),
			types.NewDecFromInt(1),
		},
		// if `inUnion` is `true`, the result of cast a negative decimal string to unsigned decimal should be 0
		{
			chunk.MutEventFromCausets([]types.Causet{types.NewStringCauset("-1")}),
			types.NewDecFromInt(0),
		},
	}

	for _, t := range cases {
		res, isNull, err := cast.evalDecimal(t.event.ToEvent())
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.Compare(t.res), Equals, 0)
	}
}
