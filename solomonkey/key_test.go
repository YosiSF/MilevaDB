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

package MilevaDB

import (
	"bytes"
	"errors"
	"testing"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testleak"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var _ = Suite(&testKeySuite{})

type testKeySuite struct {
}

func (s *testKeySuite) TestPartialNext(c *C) {
	defer testleak.AfterTest(c)()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	// keyA represents a multi column index.
	keyA, err := codec.EncodeValue(sc, nil, types.NewCauset("abc"), types.NewCauset("def"))
	c.Check(err, IsNil)
	keyB, err := codec.EncodeValue(sc, nil, types.NewCauset("abca"), types.NewCauset("def"))
	c.Check(err, IsNil)

	// We only use first column value to seek.
	seekKey, err := codec.EncodeValue(sc, nil, types.NewCauset("abc"))
	c.Check(err, IsNil)

	nextKey := Key(seekKey).Next()
	cmp := bytes.Compare(nextKey, keyA)
	c.Assert(cmp, Equals, -1)

	// Use next partial key, we can skip all index keys with first column value equal to "abc".
	nextPartialKey := Key(seekKey).PrefixNext()
	cmp = bytes.Compare(nextPartialKey, keyA)
	c.Assert(cmp, Equals, 1)

	cmp = bytes.Compare(nextPartialKey, keyB)
	c.Assert(cmp, Equals, -1)
}

func (s *testKeySuite) TestIsPoint(c *C) {
	tests := []struct {
		start   []byte
		end     []byte
		isPoint bool
	}{
		{
			start:   Key("rowkey1"),
			end:     Key("rowkey2"),
			isPoint: true,
		},
		{
			start:   Key("rowkey1"),
			end:     Key("rowkey3"),
			isPoint: false,
		},
		{
			start:   Key(""),
			end:     []byte{0},
			isPoint: true,
		},
		{
			start:   []byte{123, 123, 255, 255},
			end:     []byte{123, 124, 0, 0},
			isPoint: true,
		},
		{
			start:   []byte{123, 123, 255, 255},
			end:     []byte{123, 124, 0, 1},
			isPoint: false,
		},
		{
			start:   []byte{123, 123},
			end:     []byte{123, 123, 0},
			isPoint: true,
		},
		{
			start:   []byte{255},
			end:     []byte{0},
			isPoint: false,
		},
	}
	for _, tt := range tests {
		kr := KeyRange{
			StartKey: tt.start,
			EndKey:   tt.end,
		}
		c.Check(kr.IsPoint(), Equals, tt.isPoint)
	}
}

func (s *testKeySuite) TestBasicFunc(c *C) {
	c.Assert(IsTxnRetryableError(nil), IsFalse)
	c.Assert(IsTxnRetryableError(ErrTxnRetryable), IsTrue)
	c.Assert(IsTxnRetryableError(errors.New("test")), IsFalse)
}

func (s *testKeySuite) TestHandle(c *C) {
	ih := IntHandle(100)
	c.Assert(ih.IsInt(), IsTrue)
	_, iv, _ := codec.DecodeInt(ih.Encoded())
	c.Assert(iv, Equals, ih.IntValue())
	ih2 := ih.Next()
	c.Assert(ih2.IntValue(), Equals, int64(101))
	c.Assert(ih.Equal(ih2), IsFalse)
	c.Assert(ih.Compare(ih2), Equals, -1)
	c.Assert(ih.String(), Equals, "100")
	ch := mustNewCommonHandle(c, 100, "abc")
	c.Assert(ch.IsInt(), IsFalse)
	ch2 := ch.Next()
	c.Assert(ch.Equal(ch2), IsFalse)
	c.Assert(ch.Compare(ch2), Equals, -1)
	c.Assert(ch2.Encoded(), HasLen, len(ch.Encoded()))
	c.Assert(ch.NumDefCauss(), Equals, 2)
	_, d, err := codec.DecodeOne(ch.EncodedDefCaus(0))
	c.Assert(err, IsNil)
	c.Assert(d.GetInt64(), Equals, int64(100))
	_, d, err = codec.DecodeOne(ch.EncodedDefCaus(1))
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "abc")
	c.Assert(ch.String(), Equals, "{100, abc}")
}

func (s *testKeySuite) TestPaddingHandle(c *C) {
	dec := types.NewDecFromInt(1)
	encoded, err := codec.EncodeKey(new(stmtctx.StatementContext), nil, types.NewDecimalCauset(dec))
	c.Assert(err, IsNil)
	c.Assert(len(encoded), Less, 9)
	handle, err := NewCommonHandle(encoded)
	c.Assert(err, IsNil)
	c.Assert(handle.Encoded(), HasLen, 9)
	c.Assert(handle.EncodedDefCaus(0), BytesEquals, encoded)
	newHandle, err := NewCommonHandle(handle.Encoded())
	c.Assert(err, IsNil)
	c.Assert(newHandle.EncodedDefCaus(0), BytesEquals, handle.EncodedDefCaus(0))
}

func (s *testKeySuite) TestHandleMap(c *C) {
	m := NewHandleMap()
	h := IntHandle(1)
	m.Set(h, 1)
	v, ok := m.Get(h)
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, 1)
	m.Delete(h)
	v, ok = m.Get(h)
	c.Assert(ok, IsFalse)
	c.Assert(v, IsNil)
	ch := mustNewCommonHandle(c, 100, "abc")
	m.Set(ch, "a")
	v, ok = m.Get(ch)
	c.Assert(ok, IsTrue)
	c.Assert(v, Equals, "a")
	m.Delete(ch)
	v, ok = m.Get(ch)
	c.Assert(ok, IsFalse)
	c.Assert(v, IsNil)
	m.Set(ch, "a")
	ch2 := mustNewCommonHandle(c, 101, "abc")
	m.Set(ch2, "b")
	ch3 := mustNewCommonHandle(c, 99, "def")
	m.Set(ch3, "c")
	c.Assert(m.Len(), Equals, 3)
	cnt := 0
	m.Range(func(h Handle, val interface{}) bool {
		cnt++
		if h.Equal(ch) {
			c.Assert(val, Equals, "a")
		} else if h.Equal(ch2) {
			c.Assert(val, Equals, "b")
		} else {
			c.Assert(val, Equals, "c")
		}
		if cnt == 2 {
			return false
		}
		return true
	})
	c.Assert(cnt, Equals, 2)
}

func mustNewCommonHandle(c *C, values ...interface{}) *CommonHandle {
	encoded, err := codec.EncodeKey(new(stmtctx.StatementContext), nil, types.MakeCausets(values...)...)
	c.Assert(err, IsNil)
	ch, err := NewCommonHandle(encoded)
	c.Assert(err, IsNil)
	return ch
}

func BenchmarkIsPoint(b *testing.B) {
	b.ReportAllocs()
	kr := KeyRange{
		StartKey: []byte("rowkey1"),
		EndKey:   []byte("rowkey2"),
	}
	for i := 0; i < b.N; i++ {
		kr.IsPoint()
	}
}
