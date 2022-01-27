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

package blocks_test

import (
	"context"
	"io"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testIndexSuite{})

type testIndexSuite struct {
	s   ekv.CausetStorage
	dom *petri.Petri
}

func (s *testIndexSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	s.s = causetstore
	s.dom, err = stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
}

func (s *testIndexSuite) TearDownSuite(c *C) {
	s.dom.Close()
	err := s.s.Close()
	c.Assert(err, IsNil)
	testleak.AfterTest(c)()
}

func (s *testIndexSuite) TestIndex(c *C) {
	tblInfo := &perceptron.BlockInfo{
		ID: 1,
		Indices: []*perceptron.IndexInfo{
			{
				ID:   2,
				Name: perceptron.NewCIStr("test"),
				DeferredCausets: []*perceptron.IndexDeferredCauset{
					{Offset: 0},
					{Offset: 1},
				},
			},
		},
		DeferredCausets: []*perceptron.DeferredCausetInfo{
			{ID: 1, Name: perceptron.NewCIStr("c2"), State: perceptron.StatePublic, Offset: 0, FieldType: *types.NewFieldType(allegrosql.TypeVarchar)},
			{ID: 2, Name: perceptron.NewCIStr("c2"), State: perceptron.StatePublic, Offset: 1, FieldType: *types.NewFieldType(allegrosql.TypeString)},
		},
	}
	index := blocks.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])

	// Test ununiq index.
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	values := types.MakeCausets(1, 2)
	mockCtx := mock.NewContext()
	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, ekv.IntHandle(1))
	c.Assert(err, IsNil)

	it, err := index.SeekFirst(txn)
	c.Assert(err, IsNil)

	getValues, h, err := it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInt64(), Equals, int64(1))
	c.Assert(getValues[1].GetInt64(), Equals, int64(2))
	c.Assert(h.IntValue(), Equals, int64(1))
	it.Close()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	exist, _, err := index.Exist(sc, txn.GetUnionStore(), values, ekv.IntHandle(100))
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	exist, _, err = index.Exist(sc, txn.GetUnionStore(), values, ekv.IntHandle(1))
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	err = index.Delete(sc, txn, values, ekv.IntHandle(1))
	c.Assert(err, IsNil)

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue, Commentf("err %v", err))
	it.Close()

	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, ekv.IntHandle(0))
	c.Assert(err, IsNil)

	_, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, hit, err := index.Seek(sc, txn, values)
	c.Assert(err, IsNil)
	c.Assert(hit, IsFalse)

	err = index.Drop(txn.GetUnionStore())
	c.Assert(err, IsNil)

	it, hit, err = index.Seek(sc, txn, values)
	c.Assert(err, IsNil)
	c.Assert(hit, IsFalse)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue, Commentf("err %v", err))
	it.Close()

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue, Commentf("err %v", err))
	it.Close()

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	tblInfo = &perceptron.BlockInfo{
		ID: 2,
		Indices: []*perceptron.IndexInfo{
			{
				ID:     3,
				Name:   perceptron.NewCIStr("test"),
				Unique: true,
				DeferredCausets: []*perceptron.IndexDeferredCauset{
					{Offset: 0},
					{Offset: 1},
				},
			},
		},
		DeferredCausets: []*perceptron.DeferredCausetInfo{
			{ID: 1, Name: perceptron.NewCIStr("c2"), State: perceptron.StatePublic, Offset: 0, FieldType: *types.NewFieldType(allegrosql.TypeVarchar)},
			{ID: 2, Name: perceptron.NewCIStr("c2"), State: perceptron.StatePublic, Offset: 1, FieldType: *types.NewFieldType(allegrosql.TypeString)},
		},
	}
	index = blocks.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])

	// Test uniq index.
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, ekv.IntHandle(1))
	c.Assert(err, IsNil)

	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, ekv.IntHandle(2))
	c.Assert(err, NotNil)

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	getValues, h, err = it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInt64(), Equals, int64(1))
	c.Assert(getValues[1].GetInt64(), Equals, int64(2))
	c.Assert(h.IntValue(), Equals, int64(1))
	it.Close()

	exist, h, err = index.Exist(sc, txn.GetUnionStore(), values, ekv.IntHandle(1))
	c.Assert(err, IsNil)
	c.Assert(h.IntValue(), Equals, int64(1))
	c.Assert(exist, IsTrue)

	exist, h, err = index.Exist(sc, txn.GetUnionStore(), values, ekv.IntHandle(2))
	c.Assert(err, NotNil)
	c.Assert(h.IntValue(), Equals, int64(1))
	c.Assert(exist, IsTrue)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	_, err = index.FetchValues(make([]types.Causet, 0), nil)
	c.Assert(err, NotNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	// Test the function of Next when the value of unique key is nil.
	values2 := types.MakeCausets(nil, nil)
	_, err = index.Create(mockCtx, txn.GetUnionStore(), values2, ekv.IntHandle(2))
	c.Assert(err, IsNil)
	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)
	getValues, h, err = it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInterface(), Equals, nil)
	c.Assert(getValues[1].GetInterface(), Equals, nil)
	c.Assert(h.IntValue(), Equals, int64(2))
	it.Close()

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testIndexSuite) TestCombineIndexSeek(c *C) {
	tblInfo := &perceptron.BlockInfo{
		ID: 1,
		Indices: []*perceptron.IndexInfo{
			{
				ID:   2,
				Name: perceptron.NewCIStr("test"),
				DeferredCausets: []*perceptron.IndexDeferredCauset{
					{Offset: 1},
					{Offset: 2},
				},
			},
		},
		DeferredCausets: []*perceptron.DeferredCausetInfo{
			{Offset: 0},
			{Offset: 1},
			{Offset: 2},
		},
	}
	index := blocks.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	mockCtx := mock.NewContext()
	values := types.MakeCausets("abc", "def")
	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, ekv.IntHandle(1))
	c.Assert(err, IsNil)

	index2 := blocks.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	iter, hit, err := index2.Seek(sc, txn, types.MakeCausets("abc", nil))
	c.Assert(err, IsNil)
	defer iter.Close()
	c.Assert(hit, IsFalse)
	_, h, err := iter.Next()
	c.Assert(err, IsNil)
	c.Assert(h.IntValue(), Equals, int64(1))
}

func (s *testIndexSuite) TestSingleDeferredCausetCommonHandle(c *C) {
	tblInfo := buildBlockInfo(c, "create block t (a varchar(255) primary key, u int unique, nu int, index nu (nu))")
	var idxUnique, idxNonUnique block.Index
	for _, idxInfo := range tblInfo.Indices {
		idx := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo)
		if idxInfo.Name.L == "u" {
			idxUnique = idx
		} else if idxInfo.Name.L == "nu" {
			idxNonUnique = idx
		}
	}
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	mockCtx := mock.NewContext()
	sc := mockCtx.GetStochastikVars().StmtCtx
	// create index for "insert t values ('abc', 1, 1)"
	idxDefCausVals := types.MakeCausets(1)
	handleDefCausVals := types.MakeCausets("abc")
	encodedHandle, err := codec.EncodeKey(sc, nil, handleDefCausVals...)
	c.Assert(err, IsNil)
	commonHandle, err := ekv.NewCommonHandle(encodedHandle)
	c.Assert(err, IsNil)

	for _, idx := range []block.Index{idxUnique, idxNonUnique} {
		key, _, err := idx.GenIndexKey(sc, idxDefCausVals, commonHandle, nil)
		c.Assert(err, IsNil)
		_, err = idx.Create(mockCtx, txn.GetUnionStore(), idxDefCausVals, commonHandle)
		c.Assert(err, IsNil)
		val, err := txn.Get(context.Background(), key)
		c.Assert(err, IsNil)
		defCausVals, err := blockcodec.DecodeIndexKV(key, val, 1, blockcodec.HandleDefault,
			createRowcodecDefCausInfo(tblInfo, idx.Meta()))
		c.Assert(err, IsNil)
		c.Assert(defCausVals, HasLen, 2)
		_, d, err := codec.DecodeOne(defCausVals[0])
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, int64(1))
		_, d, err = codec.DecodeOne(defCausVals[1])
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, "abc")
		handle, err := blockcodec.DecodeIndexHandle(key, val, 1)
		c.Assert(err, IsNil)
		c.Assert(handle.IsInt(), IsFalse)
		c.Assert(handle.Encoded(), BytesEquals, commonHandle.Encoded())

		unTouchedVal := append([]byte{1}, val[1:]...)
		unTouchedVal = append(unTouchedVal, ekv.UnCommitIndexKVFlag)
		_, err = blockcodec.DecodeIndexKV(key, unTouchedVal, 1, blockcodec.HandleDefault,
			createRowcodecDefCausInfo(tblInfo, idx.Meta()))
		c.Assert(err, IsNil)
	}
}

func (s *testIndexSuite) TestMultiDeferredCausetCommonHandle(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tblInfo := buildBlockInfo(c, "create block t (a int, b int, u varchar(64) unique, nu varchar(64), primary key (a, b), index nu (nu))")
	var idxUnique, idxNonUnique block.Index
	for _, idxInfo := range tblInfo.Indices {
		idx := blocks.NewIndex(tblInfo.ID, tblInfo, idxInfo)
		if idxInfo.Name.L == "u" {
			idxUnique = idx
		} else if idxInfo.Name.L == "nu" {
			idxNonUnique = idx
		}
	}

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	mockCtx := mock.NewContext()
	sc := mockCtx.GetStochastikVars().StmtCtx
	// create index for "insert t values (3, 2, "abc", "abc")
	idxDefCausVals := types.MakeCausets("abc")
	handleDefCausVals := types.MakeCausets(3, 2)
	encodedHandle, err := codec.EncodeKey(sc, nil, handleDefCausVals...)
	c.Assert(err, IsNil)
	commonHandle, err := ekv.NewCommonHandle(encodedHandle)
	c.Assert(err, IsNil)
	_ = idxNonUnique
	for _, idx := range []block.Index{idxUnique, idxNonUnique} {
		key, _, err := idx.GenIndexKey(sc, idxDefCausVals, commonHandle, nil)
		c.Assert(err, IsNil)
		_, err = idx.Create(mockCtx, txn.GetUnionStore(), idxDefCausVals, commonHandle)
		c.Assert(err, IsNil)
		val, err := txn.Get(context.Background(), key)
		c.Assert(err, IsNil)
		defCausVals, err := blockcodec.DecodeIndexKV(key, val, 1, blockcodec.HandleDefault,
			createRowcodecDefCausInfo(tblInfo, idx.Meta()))
		c.Assert(err, IsNil)
		c.Assert(defCausVals, HasLen, 3)
		_, d, err := codec.DecodeOne(defCausVals[0])
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, "abc")
		_, d, err = codec.DecodeOne(defCausVals[1])
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, int64(3))
		_, d, err = codec.DecodeOne(defCausVals[2])
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, int64(2))
		handle, err := blockcodec.DecodeIndexHandle(key, val, 1)
		c.Assert(err, IsNil)
		c.Assert(handle.IsInt(), IsFalse)
		c.Assert(handle.Encoded(), BytesEquals, commonHandle.Encoded())
	}
}

func buildBlockInfo(c *C, allegrosql string) *perceptron.BlockInfo {
	stmt, err := berolinaAllegroSQL.New().ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil)
	tblInfo, err := dbs.BuildBlockInfoFromAST(stmt.(*ast.CreateBlockStmt))
	c.Assert(err, IsNil)
	return tblInfo
}

func createRowcodecDefCausInfo(block *perceptron.BlockInfo, index *perceptron.IndexInfo) []rowcodec.DefCausInfo {
	defCausInfos := make([]rowcodec.DefCausInfo, 0, len(index.DeferredCausets))
	for _, idxDefCaus := range index.DeferredCausets {
		defCaus := block.DeferredCausets[idxDefCaus.Offset]
		defCausInfos = append(defCausInfos, rowcodec.DefCausInfo{
			ID:         defCaus.ID,
			IsPKHandle: block.PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.Flag),
			Ft:         rowcodec.FieldTypeFromPerceptronDeferredCauset(defCaus),
		})
	}
	return defCausInfos
}
