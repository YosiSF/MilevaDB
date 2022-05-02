// INTERLOCKyright 2020-present WHTCORPS INC, Inc.
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

package INTERLOCKhandler

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngaut/entangledstore/einsteindb/dbreader"
	"github.com/ngaut/entangledstore/einsteindb/mvsr-ooc"
	"github.com/ngaut/entangledstore/lockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/rowcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/badger"
	"github.com/whtcorpsinc/badger/y"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testSuite struct{}

func (ts testSuite) SetUpSuite(c *C) {}

func (ts testSuite) TearDownSuite(c *C) {}

var _ = Suite(testSuite{})

const (
	keyNumber                   = 3
	blockID                     = 0
	startTs                     = 10
	ttl                         = 60000
	posetPosetDagRequestStartTs = 100
)

// wrapper of test data, including encoded data, column types etc.
type data struct {
	encodedTestKVDatas []*encodedTestKVData
	colInfos           []*fidelpb.DeferredCausetInfo
	rows               map[int64][]types.Causet   // handle -> event
	colTypes           map[int64]*types.FieldType // colId -> fieldType
}

type encodedTestKVData struct {
	encodedRowKey   []byte
	encodedRowValue []byte
}

func initTestData(causetstore *testStore, encodedKVDatas []*encodedTestKVData) []error {
	i := 0
	for _, kvData := range encodedKVDatas {
		mutation := makeATestMutaion(kvrpcpb.Op_Put, kvData.encodedRowKey,
			kvData.encodedRowValue)
		req := &kvrpcpb.PrewriteRequest{
			Mutations:    []*kvrpcpb.Mutation{mutation},
			PrimaryLock:  kvData.encodedRowKey,
			StartVersion: uint64(startTs + i),
			LockTtl:      ttl,
		}
		causetstore.prewrite(req)
		commitError := causetstore.commit([][]byte{kvData.encodedRowKey},
			uint64(startTs+i), uint64(startTs+i+1))
		if commitError != nil {
			return []error{commitError}
		}
		i += 2
	}
	return nil
}

func makeATestMutaion(op kvrpcpb.Op, key []byte, value []byte) *kvrpcpb.Mutation {
	return &kvrpcpb.Mutation{
		Op:    op,
		Key:   key,
		Value: value,
	}
}

func prepareTestTableData(c *C, keyNumber int, blockID int64) *data {
	stmtCtx := new(stmtctx.StatementContext)
	colIds := []int64{1, 2, 3}
	colTypes := []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeString),
		types.NewFieldType(allegrosql.TypeDouble),
	}
	colInfos := make([]*fidelpb.DeferredCausetInfo, 3)
	colTypeMap := map[int64]*types.FieldType{}
	for i := 0; i < 3; i++ {
		colInfos[i] = &fidelpb.DeferredCausetInfo{
			DeferredCausetId: colIds[i],
			Tp:               int32(colTypes[i].Tp),
		}
		colTypeMap[colIds[i]] = colTypes[i]
	}
	rows := map[int64][]types.Causet{}
	encodedTestKVDatas := make([]*encodedTestKVData, keyNumber)
	encoder := &rowcodec.Encoder{Enable: true}
	for i := 0; i < keyNumber; i++ {
		causet := types.MakeCausets(i, "abc", 10.0)
		rows[int64(i)] = causet
		rowEncodedData, err := blockcodec.EncodeRow(stmtCtx, causet, colIds, nil, nil, encoder)
		c.Assert(err, IsNil)
		rowKeyEncodedData := blockcodec.EncodeRowKeyWithHandle(blockID, ekv.IntHandle(i))
		encodedTestKVDatas[i] = &encodedTestKVData{encodedRowKey: rowKeyEncodedData, encodedRowValue: rowEncodedData}
	}
	return &data{
		colInfos:           colInfos,
		encodedTestKVDatas: encodedTestKVDatas,
		rows:               rows,
		colTypes:           colTypeMap,
	}
}

func getTestPointRange(blockID int64, handle int64) ekv.KeyRange {
	startKey := blockcodec.EncodeRowKeyWithHandle(blockID, ekv.IntHandle(handle))
	endKey := make([]byte, len(startKey))
	INTERLOCKy(endKey, startKey)
	convertToPrefixNext(endKey)
	return ekv.KeyRange{
		StartKey: startKey,
		EndKey:   endKey,
	}
}

// convert this key to the smallest key which is larger than the key given.
// see einsteindb/src/interlock/soliton.rs for more detail.
func convertToPrefixNext(key []byte) []byte {
	if len(key) == 0 {
		return []byte{0}
	}
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == 255 {
			key[i] = 0
		} else {
			key[i] += 1
			return key
		}
	}
	for i := 0; i < len(key); i++ {
		key[i] = 255
	}
	return append(key, 0)
}

// return whether these two keys are equal.
func isPrefixNext(key []byte, expected []byte) bool {
	key = convertToPrefixNext(key)
	if len(key) != len(expected) {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] != expected[i] {
			return false
		}
	}
	return true
}

// return a posetPosetDag context according to posetPosetDagReq and key ranges.
func newPosetPosetDagContext(causetstore *testStore, keyRanges []ekv.KeyRange, posetPosetDagReq *fidelpb.PosetDagRequest, startTs uint64) *posetPosetDagContext {
	sc := flagsToStatementContext(posetPosetDagReq.Flags)
	txn := causetstore.EDB.NewTransaction(false)
	posetPosetDagCtx := &posetPosetDagContext{
		evalContext:      &evalContext{sc: sc},
		dbReader:         dbreader.NewDBReader(nil, []byte{255}, txn),
		lockStore:        causetstore.locks,
		posetPosetDagReq: posetPosetDagReq,
		startTS:          startTs,
	}
	if posetPosetDagReq.Executors[0].Tp == fidelpb.ExecType_TypeTableScan {
		posetPosetDagCtx.setDeferredCausetInfo(posetPosetDagReq.Executors[0].TblScan.DeferredCausets)
	} else {
		posetPosetDagCtx.setDeferredCausetInfo(posetPosetDagReq.Executors[0].IdxScan.DeferredCausets)
	}
	posetPosetDagCtx.keyRanges = make([]*interlock.KeyRange, len(keyRanges))
	for i, keyRange := range keyRanges {
		posetPosetDagCtx.keyRanges[i] = &interlock.KeyRange{
			Start: keyRange.StartKey,
			End:   keyRange.EndKey,
		}
	}
	return posetPosetDagCtx
}

// build and execute the executors according to the posetPosetDagRequest and posetPosetDagContext,
// return the result chunk data, rows count and err if occurs.
func buildExecutorsAndExecute(posetPosetDagRequest *fidelpb.PosetDagRequest,
	posetPosetDagCtx *posetPosetDagContext) ([]fidelpb.Chunk, int, error) {
	closureExec, err := buildClosureExecutor(posetPosetDagCtx, posetPosetDagRequest)
	if err != nil {
		return nil, 0, err
	}
	if closureExec != nil {
		chunks, err := closureExec.execute()
		if err != nil {
			return nil, 0, err
		}
		return chunks, closureExec.rowCount, nil
	}
	return nil, 0, errors.New("closureExec creation failed")
}

// posetPosetDagBuilder is used to build posetPosetDag request
type posetPosetDagBuilder struct {
	startTs       uint64
	executors     []*fidelpb.Executor
	outputOffsets []uint32
}

// return a default posetPosetDagBuilder
func newPosetPosetDagBuilder() *posetPosetDagBuilder {
	return &posetPosetDagBuilder{executors: make([]*fidelpb.Executor, 0)}
}

func (posetPosetDagBuilder *posetPosetDagBuilder) setStartTs(startTs uint64) *posetPosetDagBuilder {
	posetPosetDagBuilder.startTs = startTs
	return posetPosetDagBuilder
}

func (posetPosetDagBuilder *posetPosetDagBuilder) setOutputOffsets(outputOffsets []uint32) *posetPosetDagBuilder {
	posetPosetDagBuilder.outputOffsets = outputOffsets
	return posetPosetDagBuilder
}

func (posetPosetDagBuilder *posetPosetDagBuilder) addTableScan(colInfos []*fidelpb.DeferredCausetInfo, blockID int64) *posetPosetDagBuilder {
	posetPosetDagBuilder.executors = append(posetPosetDagBuilder.executors, &fidelpb.Executor{
		Tp: fidelpb.ExecType_TypeTableScan,
		TblScan: &fidelpb.TableScan{
			DeferredCausets: colInfos,
			TableId:         blockID,
		},
	})
	return posetPosetDagBuilder
}

func (posetPosetDagBuilder *posetPosetDagBuilder) addSelection(expr *fidelpb.Expr) *posetPosetDagBuilder {
	posetPosetDagBuilder.executors = append(posetPosetDagBuilder.executors, &fidelpb.Executor{
		Tp: fidelpb.ExecType_TypeSelection,
		Selection: &fidelpb.Selection{
			Conditions:       []*fidelpb.Expr{expr},
			XXX_unrecognized: nil,
		},
	})
	return posetPosetDagBuilder
}

func (posetPosetDagBuilder *posetPosetDagBuilder) addLimit(limit uint64) *posetPosetDagBuilder {
	posetPosetDagBuilder.executors = append(posetPosetDagBuilder.executors, &fidelpb.Executor{
		Tp:    fidelpb.ExecType_TypeLimit,
		Limit: &fidelpb.Limit{Limit: limit},
	})
	return posetPosetDagBuilder
}

func (posetPosetDagBuilder *posetPosetDagBuilder) build() *fidelpb.PosetDagRequest {
	return &fidelpb.PosetDagRequest{
		Executors:     posetPosetDagBuilder.executors,
		OutputOffsets: posetPosetDagBuilder.outputOffsets,
	}
}

// see einsteindb/src/interlock/soliton.rs for more detail
func (ts testSuite) TestIsPrefixNext(c *C) {
	c.Assert(isPrefixNext([]byte{}, []byte{0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0}, []byte{1}), IsTrue)
	c.Assert(isPrefixNext([]byte{1}, []byte{2}), IsTrue)
	c.Assert(isPrefixNext([]byte{255}, []byte{255, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{255, 255, 255}, []byte{255, 255, 255, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{1, 255}, []byte{2, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 1, 255}, []byte{0, 2, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 1, 255, 5}, []byte{0, 1, 255, 6}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 1, 5, 255}, []byte{0, 1, 6, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 1, 255, 255}, []byte{0, 2, 0, 0}), IsTrue)
	c.Assert(isPrefixNext([]byte{0, 255, 255, 255}, []byte{1, 0, 0, 0}), IsTrue)
}

func (ts testSuite) TestPointGet(c *C) {
	// here would build mvsr-oocStore and server, and prepare
	// three rows data, just like the test data of block_scan.rs.
	// then init the causetstore with the generated data.
	data := prepareTestTableData(c, keyNumber, blockID)
	causetstore, err := newTestStore("INTERLOCK_handler_test_db", "INTERLOCK_handler_test_log")
	defer cleanTestStore(causetstore)
	c.Assert(err, IsNil)
	errors := initTestData(causetstore, data.encodedTestKVDatas)
	c.Assert(errors, IsNil)

	// point get should return nothing when handle is math.MinInt64
	handle := int64(math.MinInt64)
	posetPosetDagRequest := newPosetPosetDagBuilder().
		setStartTs(posetPosetDagRequestStartTs).
		addTableScan(data.colInfos, blockID).
		setOutputOffsets([]uint32{0, 1}).
		build()
	posetPosetDagCtx := newPosetPosetDagContext(causetstore, []ekv.KeyRange{getTestPointRange(blockID, handle)},
		posetPosetDagRequest, posetPosetDagRequestStartTs)
	chunks, rowCount, err := buildExecutorsAndExecute(posetPosetDagRequest, posetPosetDagCtx)
	c.Assert(len(chunks), Equals, 0)
	c.Assert(err, IsNil)
	c.Assert(rowCount, Equals, 0)

	// point get should return one event when handle = 0
	handle = 0
	posetPosetDagRequest = newPosetPosetDagBuilder().
		setStartTs(posetPosetDagRequestStartTs).
		addTableScan(data.colInfos, blockID).
		setOutputOffsets([]uint32{0, 1}).
		build()
	posetPosetDagCtx = newPosetPosetDagContext(causetstore, []ekv.KeyRange{getTestPointRange(blockID, handle)},
		posetPosetDagRequest, posetPosetDagRequestStartTs)
	chunks, rowCount, err = buildExecutorsAndExecute(posetPosetDagRequest, posetPosetDagCtx)
	c.Assert(err, IsNil)
	c.Assert(rowCount, Equals, 1)
	returnedRow, err := codec.Decode(chunks[0].RowsData, 2)
	c.Assert(err, IsNil)
	// returned event should has 2 defcaus
	c.Assert(len(returnedRow), Equals, 2)

	// verify the returned rows value as input
	expectedRow := data.rows[handle]
	eq, err := returnedRow[0].CompareCauset(nil, &expectedRow[0])
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, 0)
	eq, err = returnedRow[1].CompareCauset(nil, &expectedRow[1])
	c.Assert(err, IsNil)
	c.Assert(eq, Equals, 0)
}

func (ts testSuite) TestClosureExecutor(c *C) {
	data := prepareTestTableData(c, keyNumber, blockID)
	causetstore, err := newTestStore("INTERLOCK_handler_test_db", "INTERLOCK_handler_test_log")
	defer cleanTestStore(causetstore)
	c.Assert(err, IsNil)
	errors := initTestData(causetstore, data.encodedTestKVDatas)
	c.Assert(errors, IsNil)

	posetPosetDagRequest := newPosetPosetDagBuilder().
		setStartTs(posetPosetDagRequestStartTs).
		addTableScan(data.colInfos, blockID).
		addSelection(buildEQIntExpr(1, -1)).
		addLimit(1).
		setOutputOffsets([]uint32{0, 1}).
		build()

	posetPosetDagCtx := newPosetPosetDagContext(causetstore, []ekv.KeyRange{getTestPointRange(blockID, 1)},
		posetPosetDagRequest, posetPosetDagRequestStartTs)
	_, rowCount, err := buildExecutorsAndExecute(posetPosetDagRequest, posetPosetDagCtx)
	c.Assert(err, IsNil)
	c.Assert(rowCount, Equals, 0)
}

func buildEQIntExpr(colID, val int64) *fidelpb.Expr {
	return &fidelpb.Expr{
		Tp:        fidelpb.ExprType_ScalarFunc,
		Sig:       fidelpb.ScalarFuncSig_EQInt,
		FieldType: expression.ToPBFieldType(types.NewFieldType(allegrosql.TypeLonglong)),
		Children: []*fidelpb.Expr{
			{
				Tp:        fidelpb.ExprType_DeferredCausetRef,
				Val:       codec.EncodeInt(nil, colID),
				FieldType: expression.ToPBFieldType(types.NewFieldType(allegrosql.TypeLonglong)),
			},
			{
				Tp:        fidelpb.ExprType_Int64,
				Val:       codec.EncodeInt(nil, val),
				FieldType: expression.ToPBFieldType(types.NewFieldType(allegrosql.TypeLonglong)),
			},
		},
	}
}

type testStore struct {
	EDB     *badger.EDB
	locks   *lockstore.MemStore
	dbPath  string
	logPath string
}

func (ts *testStore) prewrite(req *kvrpcpb.PrewriteRequest) {
	for _, m := range req.Mutations {
		dagger := &mvsr-ooc.MvccLock{
			MvccLockHdr: mvsr-ooc.MvccLockHdr{
				StartTS:        req.StartVersion,
				ForUFIDelateTS: req.ForUFIDelateTs,
				TTL:            uint32(req.LockTtl),
				PrimaryLen:     uint16(len(req.PrimaryLock)),
				MinCommitTS:    req.MinCommitTs,
				Op:             uint8(m.Op),
			},
			Primary: req.PrimaryLock,
			Value:   m.Value,
		}
		ts.locks.Put(m.Key, dagger.MarshalBinary())
	}
}

func (ts *testStore) commit(keys [][]byte, startTS, commitTS uint64) error {
	return ts.EDB.UFIDelate(func(txn *badger.Txn) error {
		for _, key := range keys {
			dagger := mvsr-ooc.DecodeLock(ts.locks.Get(key, nil))
			userMeta := mvsr-ooc.NewDBUserMeta(startTS, commitTS)
			err := txn.SetEntry(&badger.Entry{
				Key:      y.KeyWithTs(key, commitTS),
				Value:    dagger.Value,
				UserMeta: userMeta,
			})
			if err != nil {
				return err
			}
			ts.locks.Delete(key)
		}
		return nil
	})
}

func newTestStore(dbPrefix string, logPrefix string) (*testStore, error) {
	dbPath, err := ioutil.TemFIDelir("", dbPrefix)
	if err != nil {
		return nil, err
	}
	LogPath, err := ioutil.TemFIDelir("", logPrefix)
	if err != nil {
		return nil, err
	}
	EDB, err := createTestDB(dbPath, LogPath)
	if err != nil {
		return nil, err
	}
	// Some raft causetstore path problems could not be found using simple causetstore in tests
	// writer := NewDBWriter(dbBundle, safePoint)
	kvPath := filepath.Join(dbPath, "ekv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")
	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)
	return &testStore{
		EDB:     EDB,
		locks:   lockstore.NewMemStore(4096),
		dbPath:  dbPath,
		logPath: LogPath,
	}, nil
}

func createTestDB(dbPath, LogPath string) (*badger.EDB, error) {
	subPath := fmt.Sprintf("/%d", 0)
	opts := badger.DefaultOptions
	opts.Dir = dbPath + subPath
	opts.ValueDir = LogPath + subPath
	opts.ManagedTxns = true
	return badger.Open(opts)
}

func cleanTestStore(causetstore *testStore) {
	os.RemoveAll(causetstore.dbPath)
	os.RemoveAll(causetstore.logPath)
}
