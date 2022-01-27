// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schemareplicant_test

import (
	"sync"
	"testing"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestT(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()
	// Make sure it calls perfschema.Init().
	dom, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer dom.Close()

	handle := schemareplicant.NewHandle(causetstore)
	dbName := perceptron.NewCIStr("Test")
	tbName := perceptron.NewCIStr("T")
	defCausName := perceptron.NewCIStr("A")
	idxName := perceptron.NewCIStr("idx")
	noexist := perceptron.NewCIStr("noexist")

	defCausID, err := genGlobalID(causetstore)
	c.Assert(err, IsNil)
	defCausInfo := &perceptron.DeferredCausetInfo{
		ID:        defCausID,
		Name:      defCausName,
		Offset:    0,
		FieldType: *types.NewFieldType(allegrosql.TypeLonglong),
		State:     perceptron.StatePublic,
	}

	idxInfo := &perceptron.IndexInfo{
		Name:  idxName,
		Block: tbName,
		DeferredCausets: []*perceptron.IndexDeferredCauset{
			{
				Name:   defCausName,
				Offset: 0,
				Length: 10,
			},
		},
		Unique:  true,
		Primary: true,
		State:   perceptron.StatePublic,
	}

	tbID, err := genGlobalID(causetstore)
	c.Assert(err, IsNil)
	tblInfo := &perceptron.BlockInfo{
		ID:              tbID,
		Name:            tbName,
		DeferredCausets: []*perceptron.DeferredCausetInfo{defCausInfo},
		Indices:         []*perceptron.IndexInfo{idxInfo},
		State:           perceptron.StatePublic,
	}

	dbID, err := genGlobalID(causetstore)
	c.Assert(err, IsNil)
	dbInfo := &perceptron.DBInfo{
		ID:     dbID,
		Name:   dbName,
		Blocks: []*perceptron.BlockInfo{tblInfo},
		State:  perceptron.StatePublic,
	}

	dbInfos := []*perceptron.DBInfo{dbInfo}
	err = ekv.RunInNewTxn(causetstore, true, func(txn ekv.Transaction) error {
		spacetime.NewMeta(txn).CreateDatabase(dbInfo)
		return errors.Trace(err)
	})
	c.Assert(err, IsNil)

	builder, err := schemareplicant.NewBuilder(handle).InitWithDBInfos(dbInfos, 1)
	c.Assert(err, IsNil)

	txn, err := causetstore.Begin()
	c.Assert(err, IsNil)
	checkApplyCreateNonExistsSchemaDoesNotPanic(c, txn, builder)
	checkApplyCreateNonExistsBlockDoesNotPanic(c, txn, builder, dbID)
	txn.Rollback()

	builder.Build()
	is := handle.Get()

	schemaNames := is.AllSchemaNames()
	c.Assert(schemaNames, HasLen, 4)
	c.Assert(solitonutil.CompareUnorderedStringSlice(schemaNames, []string{soliton.InformationSchemaName.O, soliton.MetricSchemaName.O, soliton.PerformanceSchemaName.O, "Test"}), IsTrue)

	schemas := is.AllSchemas()
	c.Assert(schemas, HasLen, 4)
	schemas = is.Clone()
	c.Assert(schemas, HasLen, 4)

	c.Assert(is.SchemaExists(dbName), IsTrue)
	c.Assert(is.SchemaExists(noexist), IsFalse)

	schemaReplicant, ok := is.SchemaByID(dbID)
	c.Assert(ok, IsTrue)
	c.Assert(schemaReplicant, NotNil)

	schemaReplicant, ok = is.SchemaByID(tbID)
	c.Assert(ok, IsFalse)
	c.Assert(schemaReplicant, IsNil)

	schemaReplicant, ok = is.SchemaByName(dbName)
	c.Assert(ok, IsTrue)
	c.Assert(schemaReplicant, NotNil)

	schemaReplicant, ok = is.SchemaByName(noexist)
	c.Assert(ok, IsFalse)
	c.Assert(schemaReplicant, IsNil)

	schemaReplicant, ok = is.SchemaByBlock(tblInfo)
	c.Assert(ok, IsTrue)
	c.Assert(schemaReplicant, NotNil)

	noexistTblInfo := &perceptron.BlockInfo{ID: 12345, Name: tblInfo.Name}
	schemaReplicant, ok = is.SchemaByBlock(noexistTblInfo)
	c.Assert(ok, IsFalse)
	c.Assert(schemaReplicant, IsNil)

	c.Assert(is.BlockExists(dbName, tbName), IsTrue)
	c.Assert(is.BlockExists(dbName, noexist), IsFalse)
	c.Assert(is.BlockIsView(dbName, tbName), IsFalse)
	c.Assert(is.BlockIsSequence(dbName, tbName), IsFalse)

	tb, ok := is.BlockByID(tbID)
	c.Assert(ok, IsTrue)
	c.Assert(tb, NotNil)

	tb, ok = is.BlockByID(dbID)
	c.Assert(ok, IsFalse)
	c.Assert(tb, IsNil)

	alloc, ok := is.AllocByID(tbID)
	c.Assert(ok, IsTrue)
	c.Assert(alloc, NotNil)

	tb, err = is.BlockByName(dbName, tbName)
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)

	_, err = is.BlockByName(dbName, noexist)
	c.Assert(err, NotNil)

	tbs := is.SchemaBlocks(dbName)
	c.Assert(tbs, HasLen, 1)

	tbs = is.SchemaBlocks(noexist)
	c.Assert(tbs, HasLen, 0)

	// Make sure partitions causet exists
	tb, err = is.BlockByName(perceptron.NewCIStr("information_schema"), perceptron.NewCIStr("partitions"))
	c.Assert(err, IsNil)
	c.Assert(tb, NotNil)

	err = ekv.RunInNewTxn(causetstore, true, func(txn ekv.Transaction) error {
		spacetime.NewMeta(txn).CreateBlockOrView(dbID, tblInfo)
		return errors.Trace(err)
	})
	c.Assert(err, IsNil)
	txn, err = causetstore.Begin()
	c.Assert(err, IsNil)
	_, err = builder.ApplyDiff(spacetime.NewMeta(txn), &perceptron.SchemaDiff{Type: perceptron.CausetActionRenameBlock, SchemaID: dbID, BlockID: tbID, OldSchemaID: dbID})
	c.Assert(err, IsNil)
	txn.Rollback()
	builder.Build()
	is = handle.Get()
	schemaReplicant, ok = is.SchemaByID(dbID)
	c.Assert(ok, IsTrue)
	c.Assert(len(schemaReplicant.Blocks), Equals, 1)

	emptyHandle := handle.EmptyClone()
	c.Assert(emptyHandle.Get(), IsNil)
}

func (testSuite) TestMockSchemaReplicant(c *C) {
	tblID := int64(1234)
	tblName := perceptron.NewCIStr("tbl_m")
	blockInfo := &perceptron.BlockInfo{
		ID:    tblID,
		Name:  tblName,
		State: perceptron.StatePublic,
	}
	defCausInfo := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    0,
		Name:      perceptron.NewCIStr("h"),
		FieldType: *types.NewFieldType(allegrosql.TypeLong),
		ID:        1,
	}
	blockInfo.DeferredCausets = []*perceptron.DeferredCausetInfo{defCausInfo}
	is := schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{blockInfo})
	tbl, ok := is.BlockByID(tblID)
	c.Assert(ok, IsTrue)
	c.Assert(tbl.Meta().Name, Equals, tblName)
	c.Assert(tbl.DefCauss()[0].DeferredCausetInfo, Equals, defCausInfo)
}

func checkApplyCreateNonExistsSchemaDoesNotPanic(c *C, txn ekv.Transaction, builder *schemareplicant.Builder) {
	m := spacetime.NewMeta(txn)
	_, err := builder.ApplyDiff(m, &perceptron.SchemaDiff{Type: perceptron.CausetActionCreateSchema, SchemaID: 999})
	c.Assert(schemareplicant.ErrDatabaseNotExists.Equal(err), IsTrue)
}

func checkApplyCreateNonExistsBlockDoesNotPanic(c *C, txn ekv.Transaction, builder *schemareplicant.Builder, dbID int64) {
	m := spacetime.NewMeta(txn)
	_, err := builder.ApplyDiff(m, &perceptron.SchemaDiff{Type: perceptron.CausetActionCreateBlock, SchemaID: dbID, BlockID: 999})
	c.Assert(schemareplicant.ErrBlockNotExists.Equal(err), IsTrue)
}

// TestConcurrent makes sure it is safe to concurrently create handle on multiple stores.
func (testSuite) TestConcurrent(c *C) {
	defer testleak.AfterTest(c)()
	storeCount := 5
	stores := make([]ekv.CausetStorage, storeCount)
	for i := 0; i < storeCount; i++ {
		causetstore, err := mockstore.NewMockStore()
		c.Assert(err, IsNil)
		stores[i] = causetstore
	}
	defer func() {
		for _, causetstore := range stores {
			causetstore.Close()
		}
	}()
	var wg sync.WaitGroup
	wg.Add(storeCount)
	for _, causetstore := range stores {
		go func(s ekv.CausetStorage) {
			defer wg.Done()
			_ = schemareplicant.NewHandle(s)
		}(causetstore)
	}
	wg.Wait()
}

// TestInfoBlocks makes sure that all blocks of information_schema could be found in schemareplicant handle.
func (*testSuite) TestInfoBlocks(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()
	handle := schemareplicant.NewHandle(causetstore)
	builder, err := schemareplicant.NewBuilder(handle).InitWithDBInfos(nil, 0)
	c.Assert(err, IsNil)
	builder.Build()
	is := handle.Get()
	c.Assert(is, NotNil)

	infoBlocks := []string{
		"SCHEMATA",
		"TABLES",
		"COLUMNS",
		"STATISTICS",
		"CHARACTER_SETS",
		"COLLATIONS",
		"FILES",
		"PROFILING",
		"PARTITIONS",
		"KEY_COLUMN_USAGE",
		"REFERENTIAL_CONSTRAINTS",
		"SESSION_VARIABLES",
		"PLUGINS",
		"TABLE_CONSTRAINTS",
		"TRIGGERS",
		"USER_PRIVILEGES",
		"ENGINES",
		"VIEWS",
		"ROUTINES",
		"SCHEMA_PRIVILEGES",
		"COLUMN_PRIVILEGES",
		"TABLE_PRIVILEGES",
		"PARAMETERS",
		"EVENTS",
		"GLOBAL_STATUS",
		"GLOBAL_VARIABLES",
		"SESSION_STATUS",
		"OPTIMIZER_TRACE",
		"TABLESPACES",
		"COLLATION_CHARACTER_SET_APPLICABILITY",
		"PROCESSLIST",
	}
	for _, t := range infoBlocks {
		tb, err1 := is.BlockByName(soliton.InformationSchemaName, perceptron.NewCIStr(t))
		c.Assert(err1, IsNil)
		c.Assert(tb, NotNil)
	}
}

func genGlobalID(causetstore ekv.CausetStorage) (int64, error) {
	var globalID int64
	err := ekv.RunInNewTxn(causetstore, true, func(txn ekv.Transaction) error {
		var err error
		globalID, err = spacetime.NewMeta(txn).GenGlobalID()
		return errors.Trace(err)
	})
	return globalID, errors.Trace(err)
}
