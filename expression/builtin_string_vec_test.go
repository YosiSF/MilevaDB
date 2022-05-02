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
	"math/rand"
	"testing"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

type randSpaceStrGener struct {
	lenBegin int
	lenEnd   int
}

func (g *randSpaceStrGener) gen() interface{} {
	n := rand.Intn(g.lenEnd-g.lenBegin) + g.lenBegin
	buf := make([]byte, n)
	for i := range buf {
		x := rand.Intn(150)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else if x-10 < 26 {
			buf[i] = byte('a' + x - 10)
		} else if x < 62 {
			buf[i] = byte('A' + x - 10 - 26)
		} else {
			buf[i] = byte(' ')
		}
	}
	return string(buf)
}

var vecBuiltinStringCases = map[string][]vecExprBenchCase{
	ast.Length: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newDefaultGener(0.2, types.ETString)}},
	},
	ast.ASCII: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newDefaultGener(0.2, types.ETString)}},
	},
	ast.Concat: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}},
	},
	ast.ConcatWS: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString, types.ETString},
			geners:        []dataGenerator{&constStrGener{","}},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString, types.ETString},
			geners:        []dataGenerator{newDefaultGener(1, types.ETString)},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString},
			geners: []dataGenerator{
				&constStrGener{"<------------------>"},
				&constStrGener{"1413006"},
				&constStrGener{"idlfmv"},
			},
		},
	},
	ast.Convert: {
		{
			retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			constants: []*CouplingConstantWithRadix{nil, {Value: types.NewCauset("utf8"), RetType: types.NewFieldType(allegrosql.TypeString)}},
		},
		{
			retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			constants: []*CouplingConstantWithRadix{nil, {Value: types.NewCauset("binary"), RetType: types.NewFieldType(allegrosql.TypeString)}},
		},
		{
			retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			constants: []*CouplingConstantWithRadix{nil, {Value: types.NewCauset("utf8mb4"), RetType: types.NewFieldType(allegrosql.TypeString)}},
		},
		{
			retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			constants: []*CouplingConstantWithRadix{nil, {Value: types.NewCauset("ascii"), RetType: types.NewFieldType(allegrosql.TypeString)}},
		},
		{
			retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			constants: []*CouplingConstantWithRadix{nil, {Value: types.NewCauset("latin1"), RetType: types.NewFieldType(allegrosql.TypeString)}},
		},
	},
	ast.Substring: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.CausetEDN},
			geners:        []dataGenerator{newRandLenStrGener(0, 20), newRangeInt64Gener(-25, 25)},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.CausetEDN, types.CausetEDN},
			geners:        []dataGenerator{newRandLenStrGener(0, 20), newRangeInt64Gener(-25, 25), newRangeInt64Gener(-25, 25)},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.CausetEDN, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
				{Tp: allegrosql.TypeLonglong}, {Tp: allegrosql.TypeLonglong}},
			geners: []dataGenerator{newRandLenStrGener(0, 20), newRangeInt64Gener(-25, 25), newRangeInt64Gener(-25, 25)},
		},
	},
	ast.SubstringIndex: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.CausetEDN},
			geners:        []dataGenerator{newRandLenStrGener(0, 20), newRandLenStrGener(0, 2), newRangeInt64Gener(-4, 4)},
		},
	},
	ast.Locate: {
		{
			retEvalType:   types.CausetEDN,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{newRandLenStrGener(0, 10), newRandLenStrGener(0, 20)},
		},
		{
			retEvalType:   types.CausetEDN,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{newRandLenStrGener(1, 2), newRandLenStrGener(0, 20)},
		},
		{
			retEvalType:   types.CausetEDN,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{newSelectStringGener([]string{"01", "10", "001", "110", "0001", "1110"}), newSelectStringGener([]string{"010010001000010", "101101110111101"})},
		},
		{
			retEvalType:   types.CausetEDN,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.CausetEDN},
			geners:        []dataGenerator{newRandLenStrGener(0, 10), newRandLenStrGener(0, 20), newRangeInt64Gener(-10, 20)},
		},
		{
			retEvalType:   types.CausetEDN,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.CausetEDN},
			geners:        []dataGenerator{newRandLenStrGener(1, 2), newRandLenStrGener(0, 10), newRangeInt64Gener(0, 8)},
		},
		{
			retEvalType:   types.CausetEDN,
			childrenTypes: []types.EvalType{types.ETString, types.ETString},
			geners:        []dataGenerator{newSelectStringGener([]string{"01", "10", "001", "110", "0001", "1110"}), newSelectStringGener([]string{"010010001000010", "101101110111101"})},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{nil, {Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newRandLenStrGener(0, 10), newRandLenStrGener(0, 20)},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{nil, {Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newRandLenStrGener(1, 2), newRandLenStrGener(0, 20)},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{nil, {Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newSelectStringGener([]string{"01", "10", "001", "110", "0001", "1110"}), newSelectStringGener([]string{"010010001000010", "101101110111101"})},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, nil},
			geners:             []dataGenerator{newRandLenStrGener(0, 10), newRandLenStrGener(0, 20)},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, nil},
			geners:             []dataGenerator{newRandLenStrGener(1, 2), newRandLenStrGener(0, 20)},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, nil},
			geners:             []dataGenerator{newSelectStringGener([]string{"01", "10", "001", "110", "0001", "1110"}), newSelectStringGener([]string{"010010001000010", "101101110111101"})},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, {Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newRandLenStrGener(0, 10), newRandLenStrGener(0, 20)},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, {Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newRandLenStrGener(1, 2), newRandLenStrGener(0, 20)},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, {Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newSelectStringGener([]string{"01", "10", "001", "110", "0001", "1110"}), newSelectStringGener([]string{"010010001000010", "101101110111101"})},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, {Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, {Tp: allegrosql.TypeInt24}},
			geners:             []dataGenerator{newRandLenStrGener(0, 10), newRandLenStrGener(0, 20), newRangeInt64Gener(-10, 20)},
		},
		{
			retEvalType:        types.CausetEDN,
			childrenTypes:      []types.EvalType{types.ETString, types.ETString, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, {Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}, {Tp: allegrosql.TypeInt24}},
			geners:             []dataGenerator{newSelectStringGener([]string{"01", "10", "001", "110", "0001", "1110"}), newSelectStringGener([]string{"010010001000010", "101101110111101"}), newRangeInt64Gener(-10, 20)},
		},
	},
	ast.Hex: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newRandHexStrGener(10, 100)}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.CausetEDN}},
	},
	ast.Unhex: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newRandHexStrGener(10, 100)}},
	},
	ast.Trim: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randSpaceStrGener{10, 100}}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{newRandLenStrGener(10, 20), newRandLenStrGener(5, 25)}},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.CausetEDN},
			geners:        []dataGenerator{newRandLenStrGener(10, 20), newRandLenStrGener(5, 25), nil},
			constants:     []*CouplingConstantWithRadix{nil, nil, {Value: types.NewCauset(ast.TrimBoth), RetType: types.NewFieldType(allegrosql.TypeLonglong)}},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.CausetEDN},
			geners:        []dataGenerator{newRandLenStrGener(10, 20), newRandLenStrGener(5, 25), nil},
			constants:     []*CouplingConstantWithRadix{nil, nil, {Value: types.NewCauset(ast.TrimLeading), RetType: types.NewFieldType(allegrosql.TypeLonglong)}},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.ETString, types.CausetEDN},
			geners:        []dataGenerator{newRandLenStrGener(10, 20), newRandLenStrGener(5, 25), nil},
			constants:     []*CouplingConstantWithRadix{nil, nil, {Value: types.NewCauset(ast.TrimTrailing), RetType: types.NewFieldType(allegrosql.TypeLonglong)}},
		},
	},
	ast.LTrim: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randSpaceStrGener{10, 100}}},
	},
	ast.RTrim: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&randSpaceStrGener{10, 100}}},
	},
	ast.Lpad: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.CausetEDN, types.ETString},
			geners:        []dataGenerator{newRandLenStrGener(0, 20), newRangeInt64Gener(168435456, 368435456), newRandLenStrGener(0, 10)},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.CausetEDN, types.ETString},
			geners:        []dataGenerator{newDefaultGener(0.2, types.ETString), newDefaultGener(0.2, types.CausetEDN), newDefaultGener(0.2, types.ETString)},
		},
		{
			retEvalType:        types.ETString,
			childrenTypes:      []types.EvalType{types.ETString, types.CausetEDN, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newRandLenStrGener(0, 20), newRangeInt64Gener(168435456, 368435456), newRandLenStrGener(0, 10)},
		},
		{
			retEvalType:        types.ETString,
			childrenTypes:      []types.EvalType{types.ETString, types.CausetEDN, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newDefaultGener(0.2, types.ETString), newDefaultGener(0.2, types.CausetEDN), newDefaultGener(0.2, types.ETString)},
		},
	},
	ast.Rpad: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.CausetEDN, types.ETString},
			geners:        []dataGenerator{newRandLenStrGener(0, 20), newRangeInt64Gener(168435456, 368435456), newRandLenStrGener(0, 10)},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString, types.CausetEDN, types.ETString},
			geners:        []dataGenerator{newDefaultGener(0.2, types.ETString), newDefaultGener(0.2, types.CausetEDN), newDefaultGener(0.2, types.ETString)},
		},
		{
			retEvalType:        types.ETString,
			childrenTypes:      []types.EvalType{types.ETString, types.CausetEDN, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newRandLenStrGener(0, 20), newRangeInt64Gener(168435456, 368435456), newRandLenStrGener(0, 10)},
		},
		{
			retEvalType:        types.ETString,
			childrenTypes:      []types.EvalType{types.ETString, types.CausetEDN, types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
			geners:             []dataGenerator{newDefaultGener(0.2, types.ETString), newDefaultGener(0.2, types.CausetEDN), newDefaultGener(0.2, types.ETString)},
		},
	},
	ast.CharLength: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
		},
	},
	ast.BitLength: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.CharFunc: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.CausetEDN, types.CausetEDN, types.CausetEDN, types.ETString},
			geners:        []dataGenerator{&charInt64Gener{}, &charInt64Gener{}, &charInt64Gener{}, nil},
			constants:     []*CouplingConstantWithRadix{nil, nil, nil, {Value: types.NewCauset("ascii"), RetType: types.NewFieldType(allegrosql.TypeString)}},
		},
	},
	ast.FindInSet: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&constStrGener{"case"}, &constStrGener{"test,case"}}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&constStrGener{""}, &constStrGener{"test,case"}}},
	},
	ast.MakeSet: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.CausetEDN, types.ETString, types.ETString, types.ETString, types.ETString, types.ETString, types.ETString, types.ETString, types.ETString}},
	},
	ast.Oct: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.CausetEDN}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{&numStrGener{*newRangeInt64Gener(-10, 10)}}},
	},
	ast.Quote: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.Ord: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString}},
	},
	ast.Bin: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.CausetEDN}},
	},
	ast.ToBase64: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newRandLenStrGener(0, 10)}},
	},
	ast.FromBase64: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newRandLenStrGener(10, 100)}},
	},
	ast.ExportSet: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.CausetEDN, types.ETString, types.ETString},
			geners:        []dataGenerator{newRangeInt64Gener(10, 100), &constStrGener{"Y"}, &constStrGener{"N"}},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.CausetEDN, types.ETString, types.ETString, types.ETString},
			geners:        []dataGenerator{newRangeInt64Gener(10, 100), &constStrGener{"Y"}, &constStrGener{"N"}, &constStrGener{","}},
		},
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.CausetEDN, types.ETString, types.ETString, types.ETString, types.CausetEDN},
			geners:        []dataGenerator{newRangeInt64Gener(10, 100), &constStrGener{"Y"}, &constStrGener{"N"}, &constStrGener{","}, newRangeInt64Gener(-10, 70)},
		},
	},
	ast.Repeat: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.CausetEDN}, geners: []dataGenerator{newRandLenStrGener(10, 20), newRangeInt64Gener(-10, 10)}},
	},
	ast.Lower: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newSelectStringGener([]string{"one week’s time TEST", "one week's time TEST", "ABC测试DEF", "ABCテストABC"})}},
	},
	ast.IsNull: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newRandLenStrGener(10, 20)}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newDefaultGener(0.2, types.ETString)}},
	},
	ast.Upper: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newSelectStringGener([]string{"one week’s time TEST", "one week's time TEST", "abc测试DeF", "AbCテストAbC"})}},
	},
	ast.Right: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.CausetEDN}},
		// need to add BinaryFlag for the Binary func
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
				{Tp: allegrosql.TypeLonglong}},
			geners: []dataGenerator{
				newRandLenStrGener(10, 20),
				newRangeInt64Gener(-10, 20),
			},
		},
	},
	ast.Left: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.CausetEDN}},
		// need to add BinaryFlag for the Binary func
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.CausetEDN},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
				{Tp: allegrosql.TypeLonglong}},
			geners: []dataGenerator{
				newRandLenStrGener(10, 20),
				newRangeInt64Gener(-10, 20),
			},
		},
	},
	ast.Space: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.CausetEDN}, geners: []dataGenerator{newRangeInt64Gener(-10, 2000)}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.CausetEDN}, geners: []dataGenerator{newRangeInt64Gener(5, 10)}},
	},
	ast.Reverse: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newRandLenStrGener(10, 20)}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString}, geners: []dataGenerator{newDefaultGener(0.2, types.ETString)}},
		// need to add BinaryFlag for the Binary func
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString},
			childrenFieldTypes: []*types.FieldType{{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin}},
		},
	},
	ast.Instr: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&constStrGener{"test,case"}, &constStrGener{"case"}}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&constStrGener{"test,case"}, &constStrGener{"testcase"}}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{
				{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
				{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
			},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{
				{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
				{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
			},
			geners: []dataGenerator{&constStrGener{"test,case"}, &constStrGener{"case"}},
		},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString},
			childrenFieldTypes: []*types.FieldType{
				{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
				{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
			},
			geners: []dataGenerator{&constStrGener{"test,case"}, &constStrGener{""}},
		},
	},
	ast.Replace: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{newRandLenStrGener(10, 20), newRandLenStrGener(0, 10), newRandLenStrGener(0, 10)}},
	},
	ast.InsertFunc: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.CausetEDN, types.CausetEDN, types.ETString}, geners: []dataGenerator{newRandLenStrGener(10, 20), newRangeInt64Gener(-10, 20), newRangeInt64Gener(0, 100), newRandLenStrGener(0, 10)}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.CausetEDN, types.CausetEDN, types.ETString},
			childrenFieldTypes: []*types.FieldType{
				{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
				{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeLonglong},
				{Tp: allegrosql.TypeString, Flag: allegrosql.BinaryFlag, DefCauslate: charset.DefCauslationBin},
			},
		},
	},
	ast.Elt: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.CausetEDN, types.ETString, types.ETString, types.ETString}, geners: []dataGenerator{newRangeInt64Gener(-1, 5)}},
	},
	ast.FromUnixTime: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDecimal, types.ETString},
			geners: []dataGenerator{
				gener{*newDefaultGener(0.9, types.ETDecimal)},
				&constStrGener{"%y-%m-%d"},
			},
		},
	},
	ast.Strcmp: {
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString}},
		{retEvalType: types.CausetEDN, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{
			newSelectStringGener(
				[]string{
					"test",
				},
			),
			newSelectStringGener(
				[]string{
					"test",
				},
			),
		}},
	},
	ast.Format: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDecimal, types.CausetEDN}, geners: []dataGenerator{
			newRangeDecimalGener(-10000, 10000, 0),
			newRangeInt64Gener(-10, 40),
		}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETReal, types.CausetEDN}, geners: []dataGenerator{
			newRangeRealGener(-10000, 10000, 0),
			newRangeInt64Gener(-10, 40),
		}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDecimal, types.CausetEDN}, geners: []dataGenerator{
			newRangeDecimalGener(-10000, 10000, 1),
			newRangeInt64Gener(-10, 40),
		}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETReal, types.CausetEDN}, geners: []dataGenerator{
			newRangeRealGener(-10000, 10000, 1),
			newRangeInt64Gener(-10, 40),
		}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{
			newRealStringGener(),
			&numStrGener{*newRangeInt64Gener(-10, 40)},
		}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETDecimal, types.CausetEDN, types.ETString}, geners: []dataGenerator{
			newRangeDecimalGener(-10000, 10000, 0.5),
			newRangeInt64Gener(-10, 40),
			newNullWrappedGener(0.1, &constStrGener{"en_US"}),
		}},
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{types.ETReal, types.CausetEDN, types.ETString}, geners: []dataGenerator{
			newRangeRealGener(-10000, 10000, 0.5),
			newRangeInt64Gener(-10, 40),
			newNullWrappedGener(0.1, &constStrGener{"en_US"}),
		}},
	},
}

func (s *testVectorizeSuite1) TestVectorizedBuiltinStringEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecBuiltinStringCases)
}

func (s *testVectorizeSuite1) TestVectorizedBuiltinStringFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinStringCases)
}

func BenchmarkVectorizedBuiltinStringEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltinStringCases)
}

func BenchmarkVectorizedBuiltinStringFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinStringCases)
}
