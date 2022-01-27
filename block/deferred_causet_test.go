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

package block

import (
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func (t *testBlockSuite) TestString(c *C) {
	defer testleak.AfterTest(c)()
	defCaus := ToDeferredCauset(&perceptron.DeferredCausetInfo{
		FieldType: *types.NewFieldType(allegrosql.TypeTiny),
		State:     perceptron.StatePublic,
	})
	defCaus.Flen = 2
	defCaus.Decimal = 1
	defCaus.Charset = allegrosql.DefaultCharset
	defCaus.DefCauslate = allegrosql.DefaultDefCauslationName
	defCaus.Flag |= allegrosql.ZerofillFlag | allegrosql.UnsignedFlag | allegrosql.BinaryFlag | allegrosql.AutoIncrementFlag | allegrosql.NotNullFlag

	c.Assert(defCaus.GetTypeDesc(), Equals, "tinyint(2) unsigned zerofill")
	defCaus.ToInfo()
	tbInfo := &perceptron.BlockInfo{}
	c.Assert(defCaus.IsPKHandleDeferredCauset(tbInfo), Equals, false)
	tbInfo.PKIsHandle = true
	defCaus.Flag |= allegrosql.PriKeyFlag
	c.Assert(defCaus.IsPKHandleDeferredCauset(tbInfo), Equals, true)

	cs := defCaus.String()
	c.Assert(len(cs), Greater, 0)

	defCaus.Tp = allegrosql.TypeEnum
	defCaus.Flag = 0
	defCaus.Elems = []string{"a", "b"}

	c.Assert(defCaus.GetTypeDesc(), Equals, "enum('a','b')")

	defCaus.Elems = []string{"'a'", "b"}
	c.Assert(defCaus.GetTypeDesc(), Equals, "enum('''a''','b')")

	defCaus.Tp = allegrosql.TypeFloat
	defCaus.Flen = 8
	defCaus.Decimal = -1
	c.Assert(defCaus.GetTypeDesc(), Equals, "float")

	defCaus.Decimal = 1
	c.Assert(defCaus.GetTypeDesc(), Equals, "float(8,1)")

	defCaus.Tp = allegrosql.TypeDatetime
	defCaus.Decimal = 6
	c.Assert(defCaus.GetTypeDesc(), Equals, "datetime(6)")

	defCaus.Decimal = 0
	c.Assert(defCaus.GetTypeDesc(), Equals, "datetime")

	defCaus.Decimal = -1
	c.Assert(defCaus.GetTypeDesc(), Equals, "datetime")
}

func (t *testBlockSuite) TestFind(c *C) {
	defer testleak.AfterTest(c)()
	defcaus := []*DeferredCauset{
		newDefCaus("a"),
		newDefCaus("b"),
		newDefCaus("c"),
	}
	FindDefCauss(defcaus, []string{"a"}, true)
	FindDefCauss(defcaus, []string{"d"}, true)
	defcaus[0].Flag |= allegrosql.OnUFIDelateNowFlag
	FindOnUFIDelateDefCauss(defcaus)
}

func (t *testBlockSuite) TestCheck(c *C) {
	defer testleak.AfterTest(c)()
	defCaus := newDefCaus("a")
	defCaus.Flag = allegrosql.AutoIncrementFlag
	defcaus := []*DeferredCauset{defCaus, defCaus}
	CheckOnce(defcaus)
	defcaus = defcaus[:1]
	CheckNotNull(defcaus, types.MakeCausets(nil))
	defcaus[0].Flag |= allegrosql.NotNullFlag
	CheckNotNull(defcaus, types.MakeCausets(nil))
	CheckOnce([]*DeferredCauset{})
}

func (t *testBlockSuite) TestHandleBadNull(c *C) {
	defCaus := newDefCaus("a")
	sc := new(stmtctx.StatementContext)
	d := types.Causet{}
	err := defCaus.HandleBadNull(&d, sc)
	c.Assert(err, IsNil)
	cmp, err := d.CompareCauset(sc, &types.Causet{})
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	defCaus.Flag |= allegrosql.NotNullFlag
	err = defCaus.HandleBadNull(&types.Causet{}, sc)
	c.Assert(err, NotNil)

	sc.BadNullAsWarning = true
	err = defCaus.HandleBadNull(&types.Causet{}, sc)
	c.Assert(err, IsNil)
}

func (t *testBlockSuite) TestDesc(c *C) {
	defer testleak.AfterTest(c)()
	defCaus := newDefCaus("a")
	defCaus.Flag = allegrosql.AutoIncrementFlag | allegrosql.NotNullFlag | allegrosql.PriKeyFlag
	NewDefCausDesc(defCaus)
	defCaus.Flag = allegrosql.MultipleKeyFlag
	NewDefCausDesc(defCaus)
	defCaus.Flag = allegrosql.UniqueKeyFlag | allegrosql.OnUFIDelateNowFlag
	desc := NewDefCausDesc(defCaus)
	c.Assert(desc.Extra, Equals, "DEFAULT_GENERATED on uFIDelate CURRENT_TIMESTAMP")
	defCaus.Flag = 0
	defCaus.GeneratedExprString = "test"
	defCaus.GeneratedStored = true
	desc = NewDefCausDesc(defCaus)
	c.Assert(desc.Extra, Equals, "STORED GENERATED")
	defCaus.GeneratedStored = false
	desc = NewDefCausDesc(defCaus)
	c.Assert(desc.Extra, Equals, "VIRTUAL GENERATED")
	DefCausDescFieldNames(false)
	DefCausDescFieldNames(true)
}

func (t *testBlockSuite) TestGetZeroValue(c *C) {
	tests := []struct {
		ft    *types.FieldType
		value types.Causet
	}{
		{
			types.NewFieldType(allegrosql.TypeLong),
			types.NewIntCauset(0),
		},
		{
			&types.FieldType{
				Tp:   allegrosql.TypeLonglong,
				Flag: allegrosql.UnsignedFlag,
			},
			types.NewUintCauset(0),
		},
		{
			types.NewFieldType(allegrosql.TypeFloat),
			types.NewFloat32Causet(0),
		},
		{
			types.NewFieldType(allegrosql.TypeDouble),
			types.NewFloat64Causet(0),
		},
		{
			types.NewFieldType(allegrosql.TypeNewDecimal),
			types.NewDecimalCauset(types.NewDecFromInt(0)),
		},
		{
			types.NewFieldType(allegrosql.TypeVarchar),
			types.NewStringCauset(""),
		},
		{
			types.NewFieldType(allegrosql.TypeBlob),
			types.NewBytesCauset([]byte{}),
		},
		{
			types.NewFieldType(allegrosql.TypeDuration),
			types.NewDurationCauset(types.ZeroDuration),
		},
		{
			types.NewFieldType(allegrosql.TypeDatetime),
			types.NewCauset(types.ZeroDatetime),
		},
		{
			types.NewFieldType(allegrosql.TypeTimestamp),
			types.NewCauset(types.ZeroTimestamp),
		},
		{
			types.NewFieldType(allegrosql.TypeDate),
			types.NewCauset(types.ZeroDate),
		},
		{
			types.NewFieldType(allegrosql.TypeBit),
			types.NewMysqlBitCauset(types.ZeroBinaryLiteral),
		},
		{
			types.NewFieldType(allegrosql.TypeSet),
			types.NewCauset(types.Set{}),
		},
		{
			types.NewFieldType(allegrosql.TypeEnum),
			types.NewCauset(types.Enum{}),
		},
		{
			&types.FieldType{
				Tp:          allegrosql.TypeString,
				Flen:        2,
				Charset:     charset.CharsetBin,
				DefCauslate: charset.DefCauslationBin,
			},
			types.NewCauset(make([]byte, 2)),
		},
		{
			&types.FieldType{
				Tp:          allegrosql.TypeString,
				Flen:        2,
				Charset:     charset.CharsetUTF8MB4,
				DefCauslate: charset.DefCauslationBin,
			},
			types.NewCauset(""),
		},
		{
			types.NewFieldType(allegrosql.TypeJSON),
			types.NewCauset(json.CreateBinary(nil)),
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		defCausInfo := &perceptron.DeferredCausetInfo{FieldType: *tt.ft}
		zv := GetZeroValue(defCausInfo)
		c.Assert(zv.HoTT(), Equals, tt.value.HoTT())
		cmp, err := zv.CompareCauset(sc, &tt.value)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (t *testBlockSuite) TestCastValue(c *C) {
	ctx := mock.NewContext()
	defCausInfo := perceptron.DeferredCausetInfo{
		FieldType: *types.NewFieldType(allegrosql.TypeLong),
		State:     perceptron.StatePublic,
	}
	defCausInfo.Charset = allegrosql.UTF8Charset
	val, err := CastValue(ctx, types.Causet{}, &defCausInfo, false, false)
	c.Assert(err, Equals, nil)
	c.Assert(val.GetInt64(), Equals, int64(0))

	val, err = CastValue(ctx, types.NewCauset("test"), &defCausInfo, false, false)
	c.Assert(err, Not(Equals), nil)
	c.Assert(val.GetInt64(), Equals, int64(0))

	defCausInfoS := perceptron.DeferredCausetInfo{
		FieldType: *types.NewFieldType(allegrosql.TypeString),
		State:     perceptron.StatePublic,
	}
	val, err = CastValue(ctx, types.NewCauset("test"), &defCausInfoS, false, false)
	c.Assert(err, IsNil)
	c.Assert(val, NotNil)

	defCausInfoS.Charset = allegrosql.UTF8Charset
	_, err = CastValue(ctx, types.NewCauset([]byte{0xf0, 0x9f, 0x8c, 0x80}), &defCausInfoS, false, false)
	c.Assert(err, NotNil)

	defCausInfoS.Charset = allegrosql.UTF8Charset
	_, err = CastValue(ctx, types.NewCauset([]byte{0xf0, 0x9f, 0x8c, 0x80}), &defCausInfoS, false, true)
	c.Assert(err, IsNil)

	defCausInfoS.Charset = allegrosql.UTF8MB4Charset
	_, err = CastValue(ctx, types.NewCauset([]byte{0xf0, 0x9f, 0x80}), &defCausInfoS, false, false)
	c.Assert(err, NotNil)

	defCausInfoS.Charset = allegrosql.UTF8MB4Charset
	_, err = CastValue(ctx, types.NewCauset([]byte{0xf0, 0x9f, 0x80}), &defCausInfoS, false, true)
	c.Assert(err, IsNil)

	defCausInfoS.Charset = charset.CharsetASCII
	_, err = CastValue(ctx, types.NewCauset([]byte{0x32, 0xf0}), &defCausInfoS, false, false)
	c.Assert(err, NotNil)

	defCausInfoS.Charset = charset.CharsetASCII
	_, err = CastValue(ctx, types.NewCauset([]byte{0x32, 0xf0}), &defCausInfoS, false, true)
	c.Assert(err, IsNil)
}

func (t *testBlockSuite) TestGetDefaultValue(c *C) {
	var nilDt types.Causet
	nilDt.SetNull()
	ctx := mock.NewContext()
	zeroTimestamp := types.ZeroTimestamp
	timestampValue := types.NewTime(types.FromDate(2020, 5, 6, 12, 48, 49, 0), allegrosql.TypeTimestamp, types.DefaultFsp)
	tests := []struct {
		defCausInfo *perceptron.DeferredCausetInfo
		strict      bool
		val         types.Causet
		err         error
	}{
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp:   allegrosql.TypeLonglong,
					Flag: allegrosql.NotNullFlag,
				},
				OriginDefaultValue: 1.0,
				DefaultValue:       1.0,
			},
			false,
			types.NewIntCauset(1),
			nil,
		},
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp:   allegrosql.TypeLonglong,
					Flag: allegrosql.NotNullFlag,
				},
			},
			false,
			types.NewIntCauset(0),
			nil,
		},
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp: allegrosql.TypeLonglong,
				},
			},
			false,
			types.Causet{},
			nil,
		},
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp:          allegrosql.TypeEnum,
					Flag:        allegrosql.NotNullFlag,
					Elems:       []string{"abc", "def"},
					DefCauslate: allegrosql.DefaultDefCauslationName,
				},
			},
			false,
			types.NewMysqlEnumCauset(types.Enum{Name: "abc", Value: 1}),
			nil,
		},
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp:   allegrosql.TypeTimestamp,
					Flag: allegrosql.TimestampFlag,
				},
				OriginDefaultValue: "0000-00-00 00:00:00",
				DefaultValue:       "0000-00-00 00:00:00",
			},
			false,
			types.NewCauset(zeroTimestamp),
			nil,
		},
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp:   allegrosql.TypeTimestamp,
					Flag: allegrosql.TimestampFlag,
				},
				OriginDefaultValue: timestampValue.String(),
				DefaultValue:       timestampValue.String(),
			},
			true,
			types.NewCauset(timestampValue),
			nil,
		},
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp:   allegrosql.TypeTimestamp,
					Flag: allegrosql.TimestampFlag,
				},
				OriginDefaultValue: "not valid date",
				DefaultValue:       "not valid date",
			},
			true,
			types.NewCauset(zeroTimestamp),
			errGetDefaultFailed,
		},
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp:   allegrosql.TypeLonglong,
					Flag: allegrosql.NotNullFlag,
				},
			},
			true,
			types.NewCauset(zeroTimestamp),
			ErrNoDefaultValue,
		},
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp:   allegrosql.TypeLonglong,
					Flag: allegrosql.NotNullFlag | allegrosql.AutoIncrementFlag,
				},
			},
			true,
			types.NewIntCauset(0),
			nil,
		},
		{
			&perceptron.DeferredCausetInfo{
				FieldType: types.FieldType{
					Tp:   allegrosql.TypeLonglong,
					Flag: allegrosql.NotNullFlag,
				},
				DefaultIsExpr: true,
				DefaultValue:  "1",
			},
			false,
			nilDt,
			nil,
		},
	}

	exp := expression.EvalAstExpr
	expression.EvalAstExpr = func(sctx stochastikctx.Context, expr ast.ExprNode) (types.Causet, error) {
		return types.NewIntCauset(1), nil
	}
	defer func() {
		expression.EvalAstExpr = exp
	}()

	for _, tt := range tests {
		ctx.GetStochastikVars().StmtCtx.BadNullAsWarning = !tt.strict
		val, err := GetDefCausDefaultValue(ctx, tt.defCausInfo)
		if err != nil {
			c.Assert(tt.err, NotNil, Commentf("%v", err))
			continue
		}
		if tt.defCausInfo.DefaultIsExpr {
			c.Assert(val, DeepEquals, types.NewIntCauset(1))
		} else {
			c.Assert(val, DeepEquals, tt.val)
		}
	}

	for _, tt := range tests {
		ctx.GetStochastikVars().StmtCtx.BadNullAsWarning = !tt.strict
		val, err := GetDefCausOriginDefaultValue(ctx, tt.defCausInfo)
		if err != nil {
			c.Assert(tt.err, NotNil, Commentf("%v", err))
			continue
		}
		if !tt.defCausInfo.DefaultIsExpr {
			c.Assert(val, DeepEquals, tt.val)
		}
	}
}

func newDefCaus(name string) *DeferredCauset {
	return ToDeferredCauset(&perceptron.DeferredCausetInfo{
		Name:  perceptron.NewCIStr(name),
		State: perceptron.StatePublic,
	})
}
