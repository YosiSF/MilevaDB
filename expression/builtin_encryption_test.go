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
	"encoding/hex"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/replog"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var cryptTests = []struct {
	origin   interface{}
	password interface{}
	crypt    interface{}
}{
	{"", "", ""},
	{"whtcorpsinc", "1234567890123456", "2C35B5A4ADF391"},
	{"whtcorpsinc", "asdfjasfwefjfjkj", "351CC412605905"},
	{"WHTCORPS INC123", "123456789012345678901234", "7698723DC6DFE7724221"},
	{"whtcorpsinc#%$%^", "*^%YTu1234567", "8634B9C55FF55E5B6328F449"},
	{"whtcorpsinc", "", "4A77B524BD2C5C"},
	{"分布式データベース", "pass1234@#$%%^^&", "80CADC8D328B3026D04FB285F36FED04BBCA0CC685BF78B1E687CE"},
	{"分布式データベース", "分布式7782734adgwy1242", "0E24CFEF272EE32B6E0BFBDB89F29FB43B4B30DAA95C3F914444BC"},
	{"whtcorpsinc", "密匙", "CE5C02A5010010"},
	{"WHTCORPS INC数据库", "数据库passwd12345667", "36D5F90D3834E30E396BE3226E3B4ED3"},
	{"数据库5667", 123.435, "B22196D0569386237AE12F8AAB"},
	{nil, "数据库passwd12345667", nil},
}

func (s *testEvaluatorSuite) TestALLEGROSQLDecode(c *C) {
	fc := funcs[ast.Decode]
	for _, tt := range cryptTests {
		str := types.NewCauset(tt.origin)
		password := types.NewCauset(tt.password)

		f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{str, password}))
		c.Assert(err, IsNil)
		crypt, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil)
		c.Assert(toHex(crypt), DeepEquals, types.NewCauset(tt.crypt))
	}
	s.testNullInput(c, ast.Decode)
}

func (s *testEvaluatorSuite) TestALLEGROSQLEncode(c *C) {
	fc := funcs[ast.Encode]
	for _, test := range cryptTests {
		password := types.NewCauset(test.password)
		cryptStr := fromHex(test.crypt)

		f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{cryptStr, password}))
		c.Assert(err, IsNil)
		str, err := evalBuiltinFunc(f, chunk.Event{})

		c.Assert(err, IsNil)
		c.Assert(str, DeepEquals, types.NewCauset(test.origin))
	}
	s.testNullInput(c, ast.Encode)
}

var aesTests = []struct {
	mode   string
	origin interface{}
	params []interface{}
	crypt  interface{}
}{
	// test for ecb
	{"aes-128-ecb", "whtcorpsinc", []interface{}{"1234567890123456"}, "697BFE9B3F8C2F289DD82C88C7BC95C4"},
	{"aes-128-ecb", "WHTCORPS INC123", []interface{}{"1234567890123456"}, "CEC348F4EF5F84D3AA6C4FA184C65766"},
	{"aes-128-ecb", "whtcorpsinc", []interface{}{"123456789012345678901234"}, "6F1589686860C8E8C7A40A78B25FF2C0"},
	{"aes-128-ecb", "whtcorpsinc", []interface{}{"123"}, "996E0CA8688D7AD20819B90B273E01C6"},
	{"aes-128-ecb", "whtcorpsinc", []interface{}{123}, "996E0CA8688D7AD20819B90B273E01C6"},
	{"aes-128-ecb", nil, []interface{}{123}, nil},
	{"aes-192-ecb", "whtcorpsinc", []interface{}{"1234567890123456"}, "9B139FD002E6496EA2D5C73A2265E661"},
	{"aes-256-ecb", "whtcorpsinc", []interface{}{"1234567890123456"}, "F80DCDEDDBE5663BDB68F74AEDDB8EE3"},
	// test for cbc
	{"aes-128-cbc", "whtcorpsinc", []interface{}{"1234567890123456", "1234567890123456"}, "2ECA0077C5EA5768A0485AA522774792"},
	{"aes-128-cbc", "whtcorpsinc", []interface{}{"123456789012345678901234", "1234567890123456"}, "483788634DA8817423BA0934FD2C096E"},
	{"aes-192-cbc", "whtcorpsinc", []interface{}{"1234567890123456", "1234567890123456"}, "516391DB38E908ECA93AAB22870EC787"},
	{"aes-256-cbc", "whtcorpsinc", []interface{}{"1234567890123456", "1234567890123456"}, "5D0E22C1E77523AEF5C3E10B65653C8F"},
	{"aes-256-cbc", "whtcorpsinc", []interface{}{"12345678901234561234567890123456", "1234567890123456"}, "A26BA27CA4BE9D361D545AA84A17002D"},
	{"aes-256-cbc", "whtcorpsinc", []interface{}{"1234567890123456", "12345678901234561234567890123456"}, "5D0E22C1E77523AEF5C3E10B65653C8F"},
	// test for ofb
	{"aes-128-ofb", "whtcorpsinc", []interface{}{"1234567890123456", "1234567890123456"}, "0515A36BBF3DE0"},
	{"aes-128-ofb", "whtcorpsinc", []interface{}{"123456789012345678901234", "1234567890123456"}, "C2A93A93818546"},
	{"aes-192-ofb", "whtcorpsinc", []interface{}{"1234567890123456", "1234567890123456"}, "FE09DCCF14D458"},
	{"aes-256-ofb", "whtcorpsinc", []interface{}{"1234567890123456", "1234567890123456"}, "2E70FCAC0C0834"},
	{"aes-256-ofb", "whtcorpsinc", []interface{}{"12345678901234561234567890123456", "1234567890123456"}, "83E2B30A71F011"},
	{"aes-256-ofb", "whtcorpsinc", []interface{}{"1234567890123456", "12345678901234561234567890123456"}, "2E70FCAC0C0834"},
	// test for cfb
	{"aes-128-cfb", "whtcorpsinc", []interface{}{"1234567890123456", "1234567890123456"}, "0515A36BBF3DE0"},
	{"aes-128-cfb", "whtcorpsinc", []interface{}{"123456789012345678901234", "1234567890123456"}, "C2A93A93818546"},
	{"aes-192-cfb", "whtcorpsinc", []interface{}{"1234567890123456", "1234567890123456"}, "FE09DCCF14D458"},
	{"aes-256-cfb", "whtcorpsinc", []interface{}{"1234567890123456", "1234567890123456"}, "2E70FCAC0C0834"},
	{"aes-256-cfb", "whtcorpsinc", []interface{}{"12345678901234561234567890123456", "1234567890123456"}, "83E2B30A71F011"},
	{"aes-256-cfb", "whtcorpsinc", []interface{}{"1234567890123456", "12345678901234561234567890123456"}, "2E70FCAC0C0834"},
}

func (s *testEvaluatorSuite) TestAESEncrypt(c *C) {
	fc := funcs[ast.AesEncrypt]
	for _, tt := range aesTests {
		variable.SetStochastikSystemVar(s.ctx.GetStochaseinstein_dbars(), variable.BlockEncryptionMode, types.NewCauset(tt.mode))
		args := []types.Causet{types.NewCauset(tt.origin)}
		for _, param := range tt.params {
			args = append(args, types.NewCauset(param))
		}
		f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs(args))
		c.Assert(err, IsNil)
		crypt, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil)
		c.Assert(toHex(crypt), DeepEquals, types.NewCauset(tt.crypt))
	}
	variable.SetStochastikSystemVar(s.ctx.GetStochaseinstein_dbars(), variable.BlockEncryptionMode, types.NewCauset("aes-128-ecb"))
	s.testNullInput(c, ast.AesEncrypt)
	s.testAmbiguousInput(c, ast.AesEncrypt)
}

func (s *testEvaluatorSuite) TestAESDecrypt(c *C) {
	fc := funcs[ast.AesDecrypt]
	for _, tt := range aesTests {
		variable.SetStochastikSystemVar(s.ctx.GetStochaseinstein_dbars(), variable.BlockEncryptionMode, types.NewCauset(tt.mode))
		args := []types.Causet{fromHex(tt.crypt)}
		for _, param := range tt.params {
			args = append(args, types.NewCauset(param))
		}
		f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs(args))
		c.Assert(err, IsNil)
		str, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil)
		if tt.origin == nil {
			c.Assert(str.IsNull(), IsTrue)
			continue
		}
		c.Assert(str, DeepEquals, types.NewDefCauslationStringCauset(tt.origin.(string), charset.DefCauslationBin, defCauslate.DefaultLen))
	}
	variable.SetStochastikSystemVar(s.ctx.GetStochaseinstein_dbars(), variable.BlockEncryptionMode, types.NewCauset("aes-128-ecb"))
	s.testNullInput(c, ast.AesDecrypt)
	s.testAmbiguousInput(c, ast.AesDecrypt)
}

func (s *testEvaluatorSuite) testNullInput(c *C, fnName string) {
	variable.SetStochastikSystemVar(s.ctx.GetStochaseinstein_dbars(), variable.BlockEncryptionMode, types.NewCauset("aes-128-ecb"))
	fc := funcs[fnName]
	arg := types.NewStringCauset("str")
	var argNull types.Causet
	f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{arg, argNull}))
	c.Assert(err, IsNil)
	crypt, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(crypt.IsNull(), IsTrue)

	f, err = fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{argNull, arg}))
	c.Assert(err, IsNil)
	crypt, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(crypt.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) testAmbiguousInput(c *C, fnName string) {
	fc := funcs[fnName]
	arg := types.NewStringCauset("str")
	// test for modes that require init_vector
	variable.SetStochastikSystemVar(s.ctx.GetStochaseinstein_dbars(), variable.BlockEncryptionMode, types.NewCauset("aes-128-cbc"))
	_, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{arg, arg}))
	c.Assert(err, NotNil)
	f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{arg, arg, types.NewStringCauset("iv < 16 bytes")}))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, NotNil)

	// test for modes that do not require init_vector
	variable.SetStochastikSystemVar(s.ctx.GetStochaseinstein_dbars(), variable.BlockEncryptionMode, types.NewCauset("aes-128-ecb"))
	f, err = fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{arg, arg, arg}))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	warnings := s.ctx.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), GreaterEqual, 1)
}

func toHex(d types.Causet) (h types.Causet) {
	if d.IsNull() {
		return
	}
	x, _ := d.ToString()
	h.SetString(strings.ToUpper(hex.EncodeToString(replog.Slice(x))), allegrosql.DefaultDefCauslationName)
	return
}

func fromHex(str interface{}) (d types.Causet) {
	if str == nil {
		return
	}
	if s, ok := str.(string); ok {
		h, _ := hex.DecodeString(s)
		d.SetBytes(h)
	}
	return d
}

var sha1Tests = []struct {
	origin interface{}
	crypt  string
}{
	{"test", "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"},
	{"c4pt0r", "034923dcabf099fc4c8917c0ab91ffcd4c2578a6"},
	{"whtcorpsinc", "73bf9ef43a44f42e2ea2894d62f0917af149a006"},
	{"foobar", "8843d7f92416211de9ebb963ff4ce28125932878"},
	{1024, "128351137a9c47206c4507dcf2e6fbeeca3a9079"},
	{123.45, "22f8b438ad7e89300b51d88684f3f0b9fa1d7a32"},
}

func (s *testEvaluatorSuite) TestSha1Hash(c *C) {
	fc := funcs[ast.SHA]
	for _, tt := range sha1Tests {
		in := types.NewCauset(tt.origin)
		f, _ := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{in}))
		crypt, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil)
		res, err := crypt.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, tt.crypt)
	}
	// test NULL input for sha
	var argNull types.Causet
	f, _ := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{argNull}))
	crypt, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(crypt.IsNull(), IsTrue)
}

var sha2Tests = []struct {
	origin     interface{}
	hashLength interface{}
	crypt      interface{}
	validCase  bool
}{
	{"whtcorpsinc", 0, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a", true},
	{"whtcorpsinc", 224, "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7", true},
	{"whtcorpsinc", 256, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a", true},
	{"whtcorpsinc", 384, "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63db51", true},
	{"whtcorpsinc", 512, "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2cf07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80", true},
	{13572468, 0, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe", true},
	{13572468, 224, "8ad67735bbf49576219f364f4640d595357a440358d15bf6815a16e4", true},
	{13572468, 256, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe", true},
	{13572468.123, 384, "3b4ee302435dc1e15251efd9f3982b1ca6fe4ac778d3260b7bbf3bea613849677eda830239420e448e4c6dc7c2649d89", true},
	{13572468.123, 512, "4820aa3f2760836557dc1f2d44a0ba7596333fdb60c8a1909481862f4ab0921c00abb23d57b7e67a970363cc3fcb78b25b6a0d45cdcac0e87aa0c96bc51f7f96", true},
	{nil, 224, nil, false},
	{"whtcorpsinc", nil, nil, false},
	{"whtcorpsinc", 123, nil, false},
}

func (s *testEvaluatorSuite) TestSha2Hash(c *C) {
	fc := funcs[ast.SHA2]
	for _, tt := range sha2Tests {
		str := types.NewCauset(tt.origin)
		hashLength := types.NewCauset(tt.hashLength)
		f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{str, hashLength}))
		c.Assert(err, IsNil)
		crypt, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil)
		if tt.validCase {
			res, err := crypt.ToString()
			c.Assert(err, IsNil)
			c.Assert(res, Equals, tt.crypt)
		} else {
			c.Assert(crypt.IsNull(), IsTrue)
		}
	}
}

func (s *testEvaluatorSuite) TestMD5Hash(c *C) {
	cases := []struct {
		args     interface{}
		expected string
		isNil    bool
		getErr   bool
	}{
		{"", "d41d8cd98f00b204e9800998ecf8427e", false, false},
		{"a", "0cc175b9c0f1b6a831c399e269772661", false, false},
		{"ab", "187ef4436122d1cc2f40dc2b92f0eba0", false, false},
		{"abc", "900150983cd24fb0d6963f7d28e17f72", false, false},
		{123, "202cb962ac59075b964b07152d234b70", false, false},
		{"123", "202cb962ac59075b964b07152d234b70", false, false},
		{123.123, "46ddc40585caa8abc07c460b3485781e", false, false},
		{nil, "", true, false},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.MD5, s.primitiveValsToCouplingConstantWithRadixs([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Event{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.HoTT(), Equals, types.HoTTNull)
			} else {
				c.Assert(d.GetString(), Equals, t.expected)
			}
		}
	}
	_, err := funcs[ast.MD5].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)

}

func (s *testEvaluatorSuite) TestRandomBytes(c *C) {
	fc := funcs[ast.RandomBytes]
	f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{types.NewCauset(32)}))
	c.Assert(err, IsNil)
	out, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(len(out.GetBytes()), Equals, 32)

	f, err = fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{types.NewCauset(1025)}))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, NotNil)
	f, err = fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{types.NewCauset(-32)}))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, NotNil)
	f, err = fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{types.NewCauset(0)}))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, NotNil)

	f, err = fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{types.NewCauset(nil)}))
	c.Assert(err, IsNil)
	out, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(len(out.GetBytes()), Equals, 0)
}

func decodeHex(str string) []byte {
	ret, err := hex.DecodeString(str)
	if err != nil {
		panic(err)
	}
	return ret
}

func (s *testEvaluatorSuite) TestCompress(c *C) {
	tests := []struct {
		in     interface{}
		expect interface{}
	}{
		{"hello world", string(decodeHex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D"))},
		{"", ""},
		{nil, nil},
	}

	fc := funcs[ast.Compress]
	for _, test := range tests {
		arg := types.NewCauset(test.in)
		f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{arg}))
		c.Assert(err, IsNil, Commentf("%v", test))
		out, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil, Commentf("%v", test))
		if test.expect == nil {
			c.Assert(out.IsNull(), IsTrue, Commentf("%v", test))
			continue
		}
		c.Assert(out, DeepEquals, types.NewDefCauslationStringCauset(test.expect.(string), charset.DefCauslationBin, defCauslate.DefaultLen), Commentf("%v", test))
	}
}

func (s *testEvaluatorSuite) TestUncompress(c *C) {
	tests := []struct {
		in     interface{}
		expect interface{}
	}{
		{decodeHex("0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D"), "hello world"},         // zlib result from MyALLEGROSQL
		{decodeHex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D"), "hello world"}, // zlib result from MilevaDB
		{decodeHex("02000000789CCB48CDC9C95728CF2FCA4901001A0B045D"), nil},                   // wrong length in the first four bytes
		{decodeHex(""), ""},
		{"1", nil},
		{"1234", nil},
		{"12345", nil},
		{decodeHex("0B"), nil},
		{decodeHex("0B000000"), nil},
		{decodeHex("0B0000001234"), nil},
		{12345, nil},
		{nil, nil},
	}

	fc := funcs[ast.Uncompress]
	for _, test := range tests {
		arg := types.NewCauset(test.in)
		f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{arg}))
		c.Assert(err, IsNil, Commentf("%v", test))
		out, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil, Commentf("%v", test))
		if test.expect == nil {
			c.Assert(out.IsNull(), IsTrue, Commentf("%v", test))
			continue
		}
		c.Assert(out, DeepEquals, types.NewDefCauslationStringCauset(test.expect.(string), charset.DefCauslationBin, defCauslate.DefaultLen), Commentf("%v", test))
	}
}

func (s *testEvaluatorSuite) TestUncompressLength(c *C) {
	tests := []struct {
		in     interface{}
		expect interface{}
	}{
		{decodeHex("0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D"), int64(11)},         // zlib result from MyALLEGROSQL
		{decodeHex("0B000000789CCA48CDC9C95728CF2FCA4901040000FFFF1A0B045D"), int64(11)}, // zlib result from MilevaDB
		{decodeHex(""), int64(0)},
		{"1", int64(0)},
		{"123", int64(0)},
		{decodeHex("0B"), int64(0)},
		{decodeHex("0B00"), int64(0)},
		{decodeHex("0B000000"), int64(0x0)},
		{decodeHex("0B0000001234"), int64(0x0B)},
		{12345, int64(875770417)},
		{nil, nil},
	}

	fc := funcs[ast.UncompressedLength]
	for _, test := range tests {
		arg := types.NewCauset(test.in)
		f, err := fc.getFunction(s.ctx, s.datumsToCouplingConstantWithRadixs([]types.Causet{arg}))
		c.Assert(err, IsNil, Commentf("%v", test))
		out, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil, Commentf("%v", test))
		c.Assert(out, DeepEquals, types.NewCauset(test.expect), Commentf("%v", test))
	}
}

func (s *testEvaluatorSuite) TestPassword(c *C) {
	cases := []struct {
		args     interface{}
		expected string
		isNil    bool
		getErr   bool
		getWarn  bool
	}{
		{nil, "", false, false, false},
		{"", "", false, false, false},
		{"abc", "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E", false, false, true},
		{123, "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257", false, false, true},
		{1.23, "*A589EEBA8D3F9E1A34A7EE518FAC4566BFAD5BB6", false, false, true},
		{types.NewDecFromFloatForTest(123.123), "*B15B84262DB34BFB2C817A45A55C405DC7C52BB1", false, false, true},
	}

	warnCount := len(s.ctx.GetStochaseinstein_dbars().StmtCtx.GetWarnings())
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.PasswordFunc, s.primitiveValsToCouplingConstantWithRadixs([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Event{})
		c.Assert(err, IsNil)
		if t.isNil {
			c.Assert(d.HoTT(), Equals, types.HoTTNull)
		} else {
			c.Assert(d.GetString(), Equals, t.expected)
		}

		warnings := s.ctx.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		if t.getWarn {
			c.Assert(len(warnings), Equals, warnCount+1)

			lastWarn := warnings[len(warnings)-1]
			c.Assert(terror.ErrorEqual(errDeprecatedSyntaxNoRememristed, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))

			warnCount = len(warnings)
		} else {
			c.Assert(len(warnings), Equals, warnCount)
		}
	}

	_, err := funcs[ast.PasswordFunc].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}
