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

package dbs_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	dbsutil "github.com/whtcorpsinc/MilevaDB-Prod/dbs/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/errno"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/admin"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/gcutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	. "github.com/whtcorpsinc/MilevaDB-Prod/soliton/solitonutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
)

// Make it serial because config is modified in test cases.
var _ = SerialSuites(&testSerialSuite{})

type testSerialSuite struct {
	CommonHandleSuite
	causetstore ekv.CausetStorage
	cluster     cluster.Cluster
	dom         *petri.Petri
}

func (s *testSerialSuite) SetUpSuite(c *C) {
	stochastik.SetSchemaLease(200 * time.Millisecond)
	stochastik.DisableStats4Test()
	config.UFIDelateGlobal(func(conf *config.Config) {
		// Test for add/drop primary key.
		conf.AlterPrimaryKey = false
	})

	dbs.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)

	var err error
	s.causetstore, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)

	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *testSerialSuite) TearDownSuite(c *C) {
	if s.dom != nil {
		s.dom.Close()
	}
	if s.causetstore != nil {
		s.causetstore.Close()
	}
}

func (s *testSerialSuite) TestChangeMaxIndexLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.MaxIndexLength = config.DefMaxOfMaxIndexLength
	})

	tk.MustExec("drop block if exists t;")
	tk.MustExec("drop block if exists t1;")

	tk.MustExec("create block t (c1 varchar(3073), index(c1)) charset = ascii;")
	tk.MustExec(fmt.Sprintf("create block t1 (c1 varchar(%d), index(c1)) charset = ascii;", config.DefMaxOfMaxIndexLength))
	_, err := tk.Exec(fmt.Sprintf("create block t2 (c1 varchar(%d), index(c1)) charset = ascii;", config.DefMaxOfMaxIndexLength+1))
	c.Assert(err.Error(), Equals, "[dbs:1071]Specified key was too long; max key length is 12288 bytes")
	tk.MustExec("drop block t, t1")
}

func (s *testSerialSuite) TestPrimaryKey(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("set @@milevadb_enable_clustered_index = 0")

	tk.MustExec("create block primary_key_test (a int, b varchar(10))")
	tk.MustExec("create block primary_key_test_1 (a int, b varchar(10), primary key(a))")
	_, err := tk.Exec("alter block primary_key_test add primary key(a)")
	c.Assert(dbs.ErrUnsupportedModifyPrimaryKey.Equal(err), IsTrue)
	_, err = tk.Exec("alter block primary_key_test drop primary key")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported drop primary key when alter-primary-key is false")
	// for "drop index `primary` on ..." syntax
	_, err = tk.Exec("drop index `primary` on primary_key_test")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported drop primary key when alter-primary-key is false")
	_, err = tk.Exec("drop index `primary` on primary_key_test_1")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported drop primary key when alter-primary-key is false")

	// Change the value of AlterPrimaryKey.
	tk.MustExec("create block primary_key_test1 (a int, b varchar(10), primary key(a))")
	tk.MustExec("create block primary_key_test2 (a int, b varchar(10), primary key(b))")
	tk.MustExec("create block primary_key_test3 (a int, b varchar(10))")
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
	})

	_, err = tk.Exec("alter block primary_key_test2 add primary key(a)")
	c.Assert(schemareplicant.ErrMultiplePriKey.Equal(err), IsTrue)
	// We can't add a primary key when the block's pk_is_handle is true.
	_, err = tk.Exec("alter block primary_key_test1 add primary key(a)")
	c.Assert(schemareplicant.ErrMultiplePriKey.Equal(err), IsTrue)
	_, err = tk.Exec("alter block primary_key_test1 add primary key(b)")
	c.Assert(schemareplicant.ErrMultiplePriKey.Equal(err), IsTrue)

	_, err = tk.Exec("alter block primary_key_test1 drop primary key")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported drop primary key when the block's pkIsHandle is true")
	tk.MustExec("alter block primary_key_test2 drop primary key")
	_, err = tk.Exec("alter block primary_key_test3 drop primary key")
	c.Assert(err.Error(), Equals, "[dbs:1091]Can't DROP 'PRIMARY'; check that defCausumn/key exists")

	// for "drop index `primary` on ..." syntax
	tk.MustExec("create block primary_key_test4 (a int, b varchar(10), primary key(a))")
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = false
	})
	_, err = tk.Exec("drop index `primary` on primary_key_test4")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported drop primary key when alter-primary-key is false")
	// for the index name is `primary`
	tk.MustExec("create block tt(`primary` int);")
	tk.MustExec("alter block tt add index (`primary`);")
	_, err = tk.Exec("drop index `primary` on tt")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported drop primary key when alter-primary-key is false")

	// The primary key cannot be invisible, for the case pk_is_handle.
	tk.MustExec("drop block if exists t1, t2;")
	_, err = tk.Exec("create block t1(c1 int not null, primary key(c1) invisible);")
	c.Assert(dbs.ErrPHoTTexCantBeInvisible.Equal(err), IsTrue)
	tk.MustExec("create block t2 (a int, b int not null, primary key(a), unique(b) invisible);")

	// Test drop clustered primary key.
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = false
	})
	tk.MustExec("drop block if exists t;")
	tk.MustExec("set milevadb_enable_clustered_index=1")
	tk.MustExec("create block t(a int, b varchar(64), primary key(b));")
	tk.MustExec("insert into t values(1,'a'), (2, 'b');")
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
	})
	errMsg := "[dbs:8200]Unsupported drop primary key when the block is using clustered index"
	tk.MustGetErrMsg("alter block t drop primary key;", errMsg)
}

func (s *testSerialSuite) TestDropAutoIncrementIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (a int(11) not null auto_increment key, b int(11), c bigint, unique key (a, b, c))")
	tk.MustExec("alter block t1 drop index a")
}

func (s *testSerialSuite) TestMultiRegionGetBlockEndHandle(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")

	tk.MustExec("create block t(a bigint PRIMARY KEY, b int)")
	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v, %v),", i, i)
	}
	allegrosql := builder.String()
	tk.MustExec(allegrosql[:len(allegrosql)-1])

	// Get block ID for split.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test_get_endhandle"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DBS()
	testCtx := newTestMaxBlockRowIDContext(c, d, tbl)

	// Split the block.
	s.cluster.SplitBlock(tblID, 100)

	maxHandle, emptyBlock := getMaxBlockHandle(testCtx, s.causetstore)
	c.Assert(emptyBlock, IsFalse)
	c.Assert(maxHandle, Equals, ekv.IntHandle(999))

	tk.MustExec("insert into t values(10000, 1000)")
	maxHandle, emptyBlock = getMaxBlockHandle(testCtx, s.causetstore)
	c.Assert(emptyBlock, IsFalse)
	c.Assert(maxHandle, Equals, ekv.IntHandle(10000))

	tk.MustExec("insert into t values(-1, 1000)")
	maxHandle, emptyBlock = getMaxBlockHandle(testCtx, s.causetstore)
	c.Assert(emptyBlock, IsFalse)
	c.Assert(maxHandle, Equals, ekv.IntHandle(10000))
}

func (s *testSerialSuite) TestGetBlockEndHandle(c *C) {
	// TestGetBlockEndHandle test dbs.GetBlockMaxHandle method, which will return the max event id of the block.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	// Test PK is handle.
	tk.MustExec("create block t(a bigint PRIMARY KEY, b int)")

	is := s.dom.SchemaReplicant()
	d := s.dom.DBS()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test_get_endhandle"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)

	testCtx := newTestMaxBlockRowIDContext(c, d, tbl)
	// test empty block
	checkGetMaxBlockRowID(testCtx, s.causetstore, true, nil)

	tk.MustExec("insert into t values(-1, 1)")
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, ekv.IntHandle(-1))

	tk.MustExec("insert into t values(9223372036854775806, 1)")
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, ekv.IntHandle(9223372036854775806))

	tk.MustExec("insert into t values(9223372036854775807, 1)")
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, ekv.IntHandle(9223372036854775807))

	tk.MustExec("insert into t values(10, 1)")
	tk.MustExec("insert into t values(102149142, 1)")
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, ekv.IntHandle(9223372036854775807))

	tk.MustExec("create block t1(a bigint PRIMARY KEY, b int)")

	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t1 values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v, %v),", i, i)
	}
	allegrosql := builder.String()
	tk.MustExec(allegrosql[:len(allegrosql)-1])

	is = s.dom.SchemaReplicant()
	testCtx.tbl, err = is.BlockByName(perceptron.NewCIStr("test_get_endhandle"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, ekv.IntHandle(999))

	// Test PK is not handle
	tk.MustExec("create block t2(a varchar(255))")

	is = s.dom.SchemaReplicant()
	testCtx.tbl, err = is.BlockByName(perceptron.NewCIStr("test_get_endhandle"), perceptron.NewCIStr("t2"))
	c.Assert(err, IsNil)
	checkGetMaxBlockRowID(testCtx, s.causetstore, true, nil)

	builder.Reset()
	fmt.Fprintf(&builder, "insert into t2 values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v),", i)
	}
	allegrosql = builder.String()
	tk.MustExec(allegrosql[:len(allegrosql)-1])

	result := tk.MustQuery("select MAX(_milevadb_rowid) from t2")
	maxHandle, emptyBlock := getMaxBlockHandle(testCtx, s.causetstore)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyBlock, IsFalse)

	tk.MustExec("insert into t2 values(100000)")
	result = tk.MustQuery("select MAX(_milevadb_rowid) from t2")
	maxHandle, emptyBlock = getMaxBlockHandle(testCtx, s.causetstore)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyBlock, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64-1))
	result = tk.MustQuery("select MAX(_milevadb_rowid) from t2")
	maxHandle, emptyBlock = getMaxBlockHandle(testCtx, s.causetstore)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyBlock, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64))
	result = tk.MustQuery("select MAX(_milevadb_rowid) from t2")
	maxHandle, emptyBlock = getMaxBlockHandle(testCtx, s.causetstore)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyBlock, IsFalse)

	tk.MustExec("insert into t2 values(100)")
	result = tk.MustQuery("select MAX(_milevadb_rowid) from t2")
	maxHandle, emptyBlock = getMaxBlockHandle(testCtx, s.causetstore)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyBlock, IsFalse)
}

func (s *testSerialSuite) TestMultiRegionGetBlockEndCommonHandle(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	tk.MustExec("set @@milevadb_enable_clustered_index = true")

	tk.MustExec("create block t(a varchar(20), b int, c float, d bigint, primary key (a, b, c))")
	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "('%v', %v, %v, %v),", i, i, i, i)
	}
	allegrosql := builder.String()
	tk.MustExec(allegrosql[:len(allegrosql)-1])

	// Get block ID for split.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test_get_endhandle"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DBS()
	testCtx := newTestMaxBlockRowIDContext(c, d, tbl)

	// Split the block.
	s.cluster.SplitBlock(tblID, 100)

	maxHandle, emptyBlock := getMaxBlockHandle(testCtx, s.causetstore)
	c.Assert(emptyBlock, IsFalse)
	c.Assert(maxHandle, HandleEquals, MustNewCommonHandle(c, "999", 999, 999))

	tk.MustExec("insert into t values('a', 1, 1, 1)")
	maxHandle, emptyBlock = getMaxBlockHandle(testCtx, s.causetstore)
	c.Assert(emptyBlock, IsFalse)
	c.Assert(maxHandle, HandleEquals, MustNewCommonHandle(c, "a", 1, 1))

	tk.MustExec("insert into t values('0000', 1, 1, 1)")
	maxHandle, emptyBlock = getMaxBlockHandle(testCtx, s.causetstore)
	c.Assert(emptyBlock, IsFalse)
	c.Assert(maxHandle, HandleEquals, MustNewCommonHandle(c, "a", 1, 1))
}

func (s *testSerialSuite) TestGetBlockEndCommonHandle(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	tk.MustExec("set @@milevadb_enable_clustered_index = true")

	tk.MustExec("create block t(a varchar(15), b bigint, c int, primary key (a, b))")
	tk.MustExec("create block t1(a varchar(15), b bigint, c int, primary key (a(2), b))")

	is := s.dom.SchemaReplicant()
	d := s.dom.DBS()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test_get_endhandle"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	testCtx := newTestMaxBlockRowIDContext(c, d, tbl)

	// test empty block
	checkGetMaxBlockRowID(testCtx, s.causetstore, true, nil)
	tk.MustExec("insert into t values('abc', 1, 10)")
	expectedHandle := MustNewCommonHandle(c, "abc", 1)
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, expectedHandle)
	tk.MustExec("insert into t values('abchzzzzzzzz', 1, 10)")
	expectedHandle = MustNewCommonHandle(c, "abchzzzzzzzz", 1)
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, expectedHandle)
	tk.MustExec("insert into t values('a', 1, 10)")
	tk.MustExec("insert into t values('ab', 1, 10)")
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, expectedHandle)

	// Test MaxBlockRowID with prefixed primary key.
	tbl, err = is.BlockByName(perceptron.NewCIStr("test_get_endhandle"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	is = s.dom.SchemaReplicant()
	d = s.dom.DBS()
	testCtx = newTestMaxBlockRowIDContext(c, d, tbl)
	checkGetMaxBlockRowID(testCtx, s.causetstore, true, nil)
	tk.MustExec("insert into t1 values('abccccc', 1, 10)")
	expectedHandle = MustNewCommonHandle(c, "ab", 1)
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, expectedHandle)
	tk.MustExec("insert into t1 values('azzzz', 1, 10)")
	expectedHandle = MustNewCommonHandle(c, "az", 1)
	checkGetMaxBlockRowID(testCtx, s.causetstore, false, expectedHandle)
}

func (s *testSerialSuite) TestCreateBlockWithLike(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// for the same database
	tk.MustExec("create database ctwl_db")
	tk.MustExec("use ctwl_db")
	tk.MustExec("create block tt(id int primary key)")
	tk.MustExec("create block t (c1 int not null auto_increment, c2 int, constraint cc foreign key (c2) references tt(id), primary key(c1)) auto_increment = 10")
	tk.MustExec("insert into t set c2=1")
	tk.MustExec("create block t1 like ctwl_db.t")
	tk.MustExec("insert into t1 set c2=11")
	tk.MustExec("create block t2 (like ctwl_db.t1)")
	tk.MustExec("insert into t2 set c2=12")
	tk.MustQuery("select * from t").Check(testkit.Rows("10 1"))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 12"))
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl1, err := is.BlockByName(perceptron.NewCIStr("ctwl_db"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	tbl1Info := tbl1.Meta()
	c.Assert(tbl1Info.ForeignKeys, IsNil)
	c.Assert(tbl1Info.PKIsHandle, Equals, true)
	defCaus := tbl1Info.DeferredCausets[0]
	hasNotNull := allegrosql.HasNotNullFlag(defCaus.Flag)
	c.Assert(hasNotNull, IsTrue)
	tbl2, err := is.BlockByName(perceptron.NewCIStr("ctwl_db"), perceptron.NewCIStr("t2"))
	c.Assert(err, IsNil)
	tbl2Info := tbl2.Meta()
	c.Assert(tbl2Info.ForeignKeys, IsNil)
	c.Assert(tbl2Info.PKIsHandle, Equals, true)
	c.Assert(allegrosql.HasNotNullFlag(tbl2Info.DeferredCausets[0].Flag), IsTrue)

	// for different databases
	tk.MustExec("create database ctwl_db1")
	tk.MustExec("use ctwl_db1")
	tk.MustExec("create block t1 like ctwl_db.t")
	tk.MustExec("insert into t1 set c2=11")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl1, err = is.BlockByName(perceptron.NewCIStr("ctwl_db1"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(tbl1.Meta().ForeignKeys, IsNil)

	// for block partition
	tk.MustExec("use ctwl_db")
	tk.MustExec("create block pt1 (id int) partition by range defCausumns (id) (partition p0 values less than (10))")
	tk.MustExec("insert into pt1 values (1),(2),(3),(4);")
	tk.MustExec("create block ctwl_db1.pt1 like ctwl_db.pt1;")
	tk.MustQuery("select * from ctwl_db1.pt1").Check(testkit.Rows())

	// Test create block like for partition block.
	atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 1)
	tk.MustExec("use test")
	tk.MustExec("set @@global.milevadb_scatter_region=1;")
	tk.MustExec("drop block if exists partition_t;")
	tk.MustExec("create block partition_t (a int, b int,index(a)) partition by hash (a) partitions 3")
	tk.MustExec("drop block if exists t1;")
	tk.MustExec("create block t1 like partition_t")
	re := tk.MustQuery("show block t1 regions")
	rows := re.Rows()
	c.Assert(len(rows), Equals, 3)
	tbl := testGetBlockByName(c, tk.Se, "test", "t1")
	partitionDef := tbl.Meta().GetPartitionInfo().Definitions
	c.Assert(rows[0][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[0].ID))
	c.Assert(rows[1][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[1].ID))
	c.Assert(rows[2][1], Matches, fmt.Sprintf("t_%d_.*", partitionDef[2].ID))

	// Test pre-split block region when create block like.
	tk.MustExec("drop block if exists t_pre")
	tk.MustExec("create block t_pre (a int, b int) shard_row_id_bits = 2 pre_split_regions=2;")
	tk.MustExec("drop block if exists t2;")
	tk.MustExec("create block t2 like t_pre")
	re = tk.MustQuery("show block t2 regions")
	rows = re.Rows()
	// Block t2 which create like t_pre should have 4 regions now.
	c.Assert(len(rows), Equals, 4)
	tbl = testGetBlockByName(c, tk.Se, "test", "t2")
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_2305843009213693952", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_4611686018427387904", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_6917529027641081856", tbl.Meta().ID))
	// Test after truncate block the region is also splited.
	tk.MustExec("truncate block t2")
	re = tk.MustQuery("show block t2 regions")
	rows = re.Rows()
	c.Assert(len(rows), Equals, 4)
	tbl = testGetBlockByName(c, tk.Se, "test", "t2")
	c.Assert(rows[1][1], Equals, fmt.Sprintf("t_%d_r_2305843009213693952", tbl.Meta().ID))
	c.Assert(rows[2][1], Equals, fmt.Sprintf("t_%d_r_4611686018427387904", tbl.Meta().ID))
	c.Assert(rows[3][1], Equals, fmt.Sprintf("t_%d_r_6917529027641081856", tbl.Meta().ID))

	defer atomic.StoreUint32(&dbs.EnableSplitBlockRegion, 0)

	// for failure block cases
	tk.MustExec("use ctwl_db")
	failALLEGROSQL := fmt.Sprintf("create block t1 like test_not_exist.t")
	tk.MustGetErrCode(failALLEGROSQL, allegrosql.ErrNoSuchBlock)
	failALLEGROSQL = fmt.Sprintf("create block t1 like test.t_not_exist")
	tk.MustGetErrCode(failALLEGROSQL, allegrosql.ErrNoSuchBlock)
	failALLEGROSQL = fmt.Sprintf("create block t1 (like test_not_exist.t)")
	tk.MustGetErrCode(failALLEGROSQL, allegrosql.ErrNoSuchBlock)
	failALLEGROSQL = fmt.Sprintf("create block test_not_exis.t1 like ctwl_db.t")
	tk.MustGetErrCode(failALLEGROSQL, allegrosql.ErrBadDB)
	failALLEGROSQL = fmt.Sprintf("create block t1 like ctwl_db.t")
	tk.MustGetErrCode(failALLEGROSQL, allegrosql.ErrBlockExists)

	// test failure for wrong object cases
	tk.MustExec("drop view if exists v")
	tk.MustExec("create view v as select 1 from dual")
	tk.MustGetErrCode("create block viewBlock like v", allegrosql.ErrWrongObject)
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustGetErrCode("create block sequenceBlock like seq", allegrosql.ErrWrongObject)

	tk.MustExec("drop database ctwl_db")
	tk.MustExec("drop database ctwl_db1")
}

// TestCancelAddIndex1 tests canceling dbs job when the add index worker is not started.
func (s *testSerialSuite) TestCancelAddIndexPanic(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/errorMockPanic", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/errorMockPanic"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(c1 int, c2 int)")
	defer tk.MustExec("drop block t;")
	for i := 0; i < 5; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	var checkErr error
	oldReorgWaitTimeout := dbs.ReorgWaitTimeout
	dbs.ReorgWaitTimeout = 50 * time.Millisecond
	defer func() { dbs.ReorgWaitTimeout = oldReorgWaitTimeout }()
	hook := &dbs.TestDBSCallback{}
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == perceptron.CausetActionAddIndex && job.State == perceptron.JobStateRunning && job.SchemaState == perceptron.StateWriteReorganization && job.SnapshotVer != 0 {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.CausetStore = s.causetstore
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			txn, err = hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	origHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(origHook)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	rs, err := tk.Exec("alter block t add index idx_c2(c2)")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
}

func (s *testSerialSuite) TestRecoverBlockByJobID(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop block if exists t_recover")
	tk.MustExec("create block t_recover (a int);")
	defer func(originGC bool) {
		if originGC {
			dbs.EmulatorGCEnable()
		} else {
			dbs.EmulatorGCDisable()
		}
	}(dbs.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete block record as soon as possible after execute drop block dbs.
	dbs.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	timeAfterDrop := time.Now().Add(48 * 60 * 60 * time.Second).Format(gcTimeFormat)
	safePointALLEGROSQL := `INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('einsteindb_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UFIDelATE variable_value = '%[1]s'`
	// clear GC variables first.
	tk.MustExec("delete from allegrosql.milevadb where variable_name in ( 'einsteindb_gc_safe_point','einsteindb_gc_enable' )")

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop block t_recover")

	getDBSJobID := func(block, tp string) int64 {
		rs, err := tk.Exec("admin show dbs jobs")
		c.Assert(err, IsNil)
		rows, err := stochastik.GetRows4Test(context.Background(), tk.Se, rs)
		c.Assert(err, IsNil)
		for _, event := range rows {
			if event.GetString(1) == block && event.GetString(3) == tp {
				return event.GetInt64(0)
			}
		}
		c.Errorf("can't find %s block of %s", tp, block)
		return -1
	}
	jobID := getDBSJobID("test_recover", "drop block")

	// if GC safe point is not exists in allegrosql.milevadb
	_, err := tk.Exec(fmt.Sprintf("recover block by job %d", jobID))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "can not get 'einsteindb_gc_safe_point'")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))

	// if GC enable is not exists in allegrosql.milevadb
	_, err = tk.Exec(fmt.Sprintf("recover block by job %d", jobID))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:-1]can not get 'einsteindb_gc_enable'")

	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// recover job is before GC safe point
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeAfterDrop))
	_, err = tk.Exec(fmt.Sprintf("recover block by job %d", jobID))
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "snapshot is older than GC safe point"), Equals, true)

	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))
	// if there is a new block with the same name, should return failed.
	tk.MustExec("create block t_recover (a int);")
	_, err = tk.Exec(fmt.Sprintf("recover block by job %d", jobID))
	c.Assert(err.Error(), Equals, schemareplicant.ErrBlockExists.GenWithStackByArgs("t_recover").Error())

	// drop the new block with the same name, then recover block.
	tk.MustExec("drop block t_recover")

	// do recover block.
	tk.MustExec(fmt.Sprintf("recover block by job %d", jobID))

	// check recover block meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover block autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))

	// recover block by none exits job.
	_, err = tk.Exec(fmt.Sprintf("recover block by job %d", 10000000))
	c.Assert(err, NotNil)

	// Disable GC by manual first, then after recover block, the GC enable status should also be disabled.
	err = gcutil.DisableGC(tk.Se)
	c.Assert(err, IsNil)

	tk.MustExec("delete from t_recover where a > 1")
	tk.MustExec("drop block t_recover")
	jobID = getDBSJobID("test_recover", "drop block")

	tk.MustExec(fmt.Sprintf("recover block by job %d", jobID))

	// check recover block meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1"))
	// check recover block autoID.
	tk.MustExec("insert into t_recover values (7),(8),(9)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9"))

	// Test for recover truncate block.
	tk.MustExec("truncate block t_recover")
	tk.MustExec("rename block t_recover to t_recover_new")
	jobID = getDBSJobID("test_recover", "truncate block")
	tk.MustExec(fmt.Sprintf("recover block by job %d", jobID))
	tk.MustExec("insert into t_recover values (10)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "7", "8", "9", "10"))

	gcEnable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(gcEnable, Equals, false)
}

func (s *testSerialSuite) TestRecoverBlockByJobIDFail(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop block if exists t_recover")
	tk.MustExec("create block t_recover (a int);")
	defer func(originGC bool) {
		if originGC {
			dbs.EmulatorGCEnable()
		} else {
			dbs.EmulatorGCDisable()
		}
	}(dbs.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete block record as soon as possible after execute drop block dbs.
	dbs.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointALLEGROSQL := `INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('einsteindb_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UFIDelATE variable_value = '%[1]s'`

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop block t_recover")

	rs, err := tk.Exec("admin show dbs jobs")
	c.Assert(err, IsNil)
	rows, err := stochastik.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(err, IsNil)
	event := rows[0]
	c.Assert(event.GetString(1), Equals, "test_recover")
	c.Assert(event.GetString(3), Equals, "drop block")
	jobID := event.GetInt64(0)

	// enableGC first
	err = gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))

	// set hook
	hook := &dbs.TestDBSCallback{}
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == perceptron.CausetActionRecoverBlock {
			c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/mockCommitError", `return(true)`), IsNil)
			c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockRecoverBlockCommitErr", `return(true)`), IsNil)
		}
	}
	origHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(origHook)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)

	// do recover block.
	tk.MustExec(fmt.Sprintf("recover block by job %d", jobID))
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/mockCommitError"), IsNil)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockRecoverBlockCommitErr"), IsNil)

	// make sure enable GC after recover block.
	enable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(enable, Equals, true)

	// check recover block meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover block autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
}

func (s *testSerialSuite) TestRecoverBlockByBlockNameFail(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop block if exists t_recover")
	tk.MustExec("create block t_recover (a int);")
	defer func(originGC bool) {
		if originGC {
			dbs.EmulatorGCEnable()
		} else {
			dbs.EmulatorGCDisable()
		}
	}(dbs.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete block record as soon as possible after execute drop block dbs.
	dbs.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointALLEGROSQL := `INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('einsteindb_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UFIDelATE variable_value = '%[1]s'`

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop block t_recover")

	// enableGC first
	err := gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)
	tk.MustExec(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))

	// set hook
	hook := &dbs.TestDBSCallback{}
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == perceptron.CausetActionRecoverBlock {
			c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/mockCommitError", `return(true)`), IsNil)
			c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockRecoverBlockCommitErr", `return(true)`), IsNil)
		}
	}
	origHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(origHook)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)

	// do recover block.
	tk.MustExec("recover block t_recover")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/mockCommitError"), IsNil)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockRecoverBlockCommitErr"), IsNil)

	// make sure enable GC after recover block.
	enable, err := gcutil.CheckGCEnable(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(enable, Equals, true)

	// check recover block meta and data record.
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3"))
	// check recover block autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover;").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
}

func (s *testSerialSuite) TestCancelJobByErrorCountLimit(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockExceedErrorLimit", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockExceedErrorLimit"), IsNil)
	}()
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")

	limit := variable.GetDBSErrorCountLimit()
	tk.MustExec("set @@global.milevadb_dbs_error_count_limit = 16")
	err := dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	defer tk.MustExec(fmt.Sprintf("set @@global.milevadb_dbs_error_count_limit = %d", limit))

	_, err = tk.Exec("create block t (a int)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:-1]DBS job rollback, error msg: mock do job error")
}

func (s *testSerialSuite) TestTruncateBlockUFIDelateSchemaVersionErr(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockTruncateBlockUFIDelateVersionError", `return(true)`), IsNil)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")

	limit := variable.GetDBSErrorCountLimit()
	tk.MustExec("set @@global.milevadb_dbs_error_count_limit = 5")
	err := dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	defer tk.MustExec(fmt.Sprintf("set @@global.milevadb_dbs_error_count_limit = %d", limit))

	tk.MustExec("create block t (a int)")
	_, err = tk.Exec("truncate block t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:-1]DBS job rollback, error msg: mock uFIDelate version error")
	// Disable fail point.
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/mockTruncateBlockUFIDelateVersionError"), IsNil)
	tk.MustExec("truncate block t")
}

func (s *testSerialSuite) TestCanceledJobTakeTime(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block t_cjtt(a int)")

	hook := &dbs.TestDBSCallback{}
	once := sync.Once{}
	hook.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		once.Do(func() {
			err := ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
				t := meta.NewMeta(txn)
				return t.DropBlockOrView(job.SchemaID, job.BlockID, true)
			})
			c.Assert(err, IsNil)
		})
	}
	origHook := s.dom.DBS().GetHook()
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(origHook)

	originalWT := dbs.GetWaitTimeWhenErrorOccurred()
	dbs.SetWaitTimeWhenErrorOccurred(1 * time.Second)
	defer func() { dbs.SetWaitTimeWhenErrorOccurred(originalWT) }()
	startTime := time.Now()
	tk.MustGetErrCode("alter block t_cjtt add defCausumn b int", allegrosql.ErrNoSuchBlock)
	sub := time.Since(startTime)
	c.Assert(sub, Less, dbs.GetWaitTimeWhenErrorOccurred())
}

func (s *testSerialSuite) TestBlockLocksEnable(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	defer tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (a int)")

	// Test for enable block dagger config.
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.EnableBlockLock = false
	})

	tk.MustExec("dagger blocks t1 write")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockNone)
}

func (s *testSerialSuite) TestAutoRandom(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists auto_random_db")
	defer tk.MustExec("drop database if exists auto_random_db")
	tk.MustExec("use auto_random_db")
	databaseName, blockName := "auto_random_db", "t"
	tk.MustExec("drop block if exists t")
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")

	assertInvalidAutoRandomErr := func(allegrosql string, errMsg string, args ...interface{}) {
		_, err := tk.Exec(allegrosql)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, dbs.ErrInvalidAutoRandom.GenWithStackByArgs(fmt.Sprintf(errMsg, args...)).Error())
	}

	assertPKIsNotHandle := func(allegrosql, errDefCaus string) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomPKisNotHandleErrMsg, errDefCaus)
	}
	assertAlterValue := func(allegrosql string) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomAlterErrMsg)
	}
	assertDecreaseBitErr := func(allegrosql string) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomDecreaseBitErrMsg)
	}
	assertWithAutoInc := func(allegrosql string) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
	}
	assertOverflow := func(allegrosql, defCausName string, maxAutoRandBits, actualAutoRandBits uint64) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomOverflowErrMsg, maxAutoRandBits, actualAutoRandBits, defCausName)
	}
	assertMaxOverflow := func(allegrosql, defCausName string, autoRandBits uint64) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomOverflowErrMsg, autoid.MaxAutoRandomBits, autoRandBits, defCausName)
	}
	assertModifyDefCausType := func(allegrosql string) {
		tk.MustGetErrCode(allegrosql, errno.ErrUnsupportedDBSOperation)
	}
	assertDefault := func(allegrosql string) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
	}
	assertNonPositive := func(allegrosql string) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomNonPositive)
	}
	assertBigIntOnly := func(allegrosql, defCausType string) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomOnNonBigIntDeferredCauset, defCausType)
	}
	assertAddDeferredCauset := func(allegrosql, defCausName string) {
		{
			assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomAlterAddDeferredCauset, defCausName, databaseName, blockName)
		}
	}
	mustExecAndDrop := func(allegrosql string, fns ...func()) {
		tk.MustExec(allegrosql)
		for _, f := range fns {
			f()
		}
		tk.MustExec("drop block t")
	}

	ConfigTestUtils.SetupAutoRandomTestConfig()
	defer ConfigTestUtils.RestoreAutoRandomTestConfig()

	// Only bigint defCausumn can set auto_random
	assertBigIntOnly("create block t (a char primary key auto_random(3), b int)", "char")
	assertBigIntOnly("create block t (a varchar(255) primary key auto_random(3), b int)", "varchar")
	assertBigIntOnly("create block t (a timestamp primary key auto_random(3), b int)", "timestamp")

	// PKIsHandle, but auto_random is defined on non-primary key.
	assertPKIsNotHandle("create block t (a bigint auto_random (3) primary key, b bigint auto_random (3))", "b")
	assertPKIsNotHandle("create block t (a bigint auto_random (3), b bigint auto_random(3), primary key(a))", "b")
	assertPKIsNotHandle("create block t (a bigint auto_random (3), b bigint auto_random(3) primary key)", "a")

	// PKIsNotHandle: no primary key.
	assertPKIsNotHandle("create block t (a bigint auto_random(3), b int)", "a")
	// PKIsNotHandle: primary key is not a single defCausumn.
	assertPKIsNotHandle("create block t (a bigint auto_random(3), b bigint, primary key (a, b))", "a")
	assertPKIsNotHandle("create block t (a bigint auto_random(3), b int, c char, primary key (a, c))", "a")

	// PKIsNotHandle: block is created when alter-primary-key = true.
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
	})
	assertPKIsNotHandle("create block t (a bigint auto_random(3) primary key, b int)", "a")
	assertPKIsNotHandle("create block t (a bigint auto_random(3) primary key, b int)", "a")
	assertPKIsNotHandle("create block t (a int, b bigint auto_random(3) primary key)", "b")
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = false
	})

	// Can not set auto_random along with auto_increment.
	assertWithAutoInc("create block t (a bigint auto_random(3) primary key auto_increment)")
	assertWithAutoInc("create block t (a bigint primary key auto_increment auto_random(3))")
	assertWithAutoInc("create block t (a bigint auto_increment primary key auto_random(3))")
	assertWithAutoInc("create block t (a bigint auto_random(3) auto_increment, primary key (a))")

	// Can not set auto_random along with default.
	assertDefault("create block t (a bigint auto_random primary key default 3)")
	assertDefault("create block t (a bigint auto_random(2) primary key default 5)")
	mustExecAndDrop("create block t (a bigint auto_random primary key)", func() {
		assertDefault("alter block t modify defCausumn a bigint auto_random default 3")
		assertDefault("alter block t alter defCausumn a set default 3")
	})

	// Overflow data type max length.
	assertMaxOverflow("create block t (a bigint auto_random(64) primary key)", "a", 64)
	assertMaxOverflow("create block t (a bigint auto_random(16) primary key)", "a", 16)
	mustExecAndDrop("create block t (a bigint auto_random(5) primary key)", func() {
		assertMaxOverflow("alter block t modify a bigint auto_random(64)", "a", 64)
		assertMaxOverflow("alter block t modify a bigint auto_random(16)", "a", 16)
	})

	assertNonPositive("create block t (a bigint auto_random(0) primary key)")
	tk.MustGetErrMsg("create block t (a bigint auto_random(-1) primary key)",
		`[berolinaAllegroSQL:1064]You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use line 1 defCausumn 38 near "-1) primary key)" `)

	// Basic usage.
	mustExecAndDrop("create block t (a bigint auto_random(1) primary key)")
	mustExecAndDrop("create block t (a bigint auto_random(4) primary key)")
	mustExecAndDrop("create block t (a bigint auto_random(15) primary key)")
	mustExecAndDrop("create block t (a bigint primary key auto_random(4))")
	mustExecAndDrop("create block t (a bigint auto_random(4), primary key (a))")

	// Increase auto_random bits.
	mustExecAndDrop("create block t (a bigint auto_random(5) primary key)", func() {
		tk.MustExec("alter block t modify a bigint auto_random(8)")
		tk.MustExec("alter block t modify a bigint auto_random(10)")
		tk.MustExec("alter block t modify a bigint auto_random(12)")
	})

	// Auto_random can occur multiple times like other defCausumn attributes.
	mustExecAndDrop("create block t (a bigint auto_random(3) auto_random(2) primary key)")
	mustExecAndDrop("create block t (a bigint, b bigint auto_random(3) primary key auto_random(2))")
	mustExecAndDrop("create block t (a bigint auto_random(1) auto_random(2) auto_random(3), primary key (a))")

	// Add/drop the auto_random attribute is not allowed.
	mustExecAndDrop("create block t (a bigint auto_random(3) primary key)", func() {
		assertAlterValue("alter block t modify defCausumn a bigint")
		assertAlterValue("alter block t modify defCausumn a bigint auto_random(0)")
		assertAlterValue("alter block t change defCausumn a b bigint")
	})
	mustExecAndDrop("create block t (a bigint, b char, c bigint auto_random(3), primary key(c))", func() {
		assertAlterValue("alter block t modify defCausumn c bigint")
		assertAlterValue("alter block t change defCausumn c d bigint")
	})
	mustExecAndDrop("create block t (a bigint primary key)", func() {
		assertAlterValue("alter block t modify defCausumn a bigint auto_random(3)")
	})
	mustExecAndDrop("create block t (a bigint, b bigint, primary key(a, b))", func() {
		assertAlterValue("alter block t modify defCausumn a bigint auto_random(3)")
		assertAlterValue("alter block t modify defCausumn b bigint auto_random(3)")
	})

	// Add auto_random defCausumn is not allowed.
	mustExecAndDrop("create block t (a bigint)", func() {
		assertAddDeferredCauset("alter block t add defCausumn b int auto_random", "b")
		assertAddDeferredCauset("alter block t add defCausumn b bigint auto_random", "b")
		assertAddDeferredCauset("alter block t add defCausumn b bigint auto_random primary key", "b")
	})
	mustExecAndDrop("create block t (a bigint, b bigint primary key)", func() {
		assertAddDeferredCauset("alter block t add defCausumn c int auto_random", "c")
		assertAddDeferredCauset("alter block t add defCausumn c bigint auto_random", "c")
		assertAddDeferredCauset("alter block t add defCausumn c bigint auto_random primary key", "c")
	})

	// Decrease auto_random bits is not allowed.
	mustExecAndDrop("create block t (a bigint auto_random(10) primary key)", func() {
		assertDecreaseBitErr("alter block t modify defCausumn a bigint auto_random(6)")
	})
	mustExecAndDrop("create block t (a bigint auto_random(10) primary key)", func() {
		assertDecreaseBitErr("alter block t modify defCausumn a bigint auto_random(1)")
	})

	originStep := autoid.GetStep()
	autoid.SetStep(1)
	// Increase auto_random bits but it will overlap with incremental bits.
	mustExecAndDrop("create block t (a bigint unsigned auto_random(5) primary key)", func() {
		const alterTryCnt, rebaseOffset = 3, 1
		insertALLEGROSQL := fmt.Sprintf("insert into t values (%d)", ((1<<(64-10))-1)-rebaseOffset-alterTryCnt)
		tk.MustExec(insertALLEGROSQL)
		// Try to rebase to 0..0011..1111 (54 `1`s).
		tk.MustExec("alter block t modify a bigint unsigned auto_random(6)")
		tk.MustExec("alter block t modify a bigint unsigned auto_random(10)")
		assertOverflow("alter block t modify a bigint unsigned auto_random(11)", "a", 10, 11)
	})
	autoid.SetStep(originStep)

	// Modifying the field type of a auto_random defCausumn is not allowed.
	// Here the throw error is `ERROR 8200 (HY000): Unsupported modify defCausumn: length 11 is less than origin 20`,
	// instead of `ERROR 8216 (HY000): Invalid auto random: modifying the auto_random defCausumn type is not supported`
	// Because the origin defCausumn is `bigint`, it can not change to any other defCausumn type in MilevaDB limitation.
	mustExecAndDrop("create block t (a bigint primary key auto_random(3), b int)", func() {
		assertModifyDefCausType("alter block t modify defCausumn a int auto_random(3)")
		assertModifyDefCausType("alter block t modify defCausumn a mediumint auto_random(3)")
		assertModifyDefCausType("alter block t modify defCausumn a smallint auto_random(3)")
		tk.MustExec("alter block t modify defCausumn b int")
		tk.MustExec("alter block t modify defCausumn b bigint")
		tk.MustExec("alter block t modify defCausumn a bigint auto_random(3)")
	})

	// Test show warnings when create auto_random block.
	assertShowWarningCorrect := func(allegrosql string, times int) {
		mustExecAndDrop(allegrosql, func() {
			note := fmt.Sprintf(autoid.AutoRandomAvailableAllocTimesNote, times)
			result := fmt.Sprintf("Note|1105|%s", note)
			tk.MustQuery("show warnings").Check(RowsWithSep("|", result))
			c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Equals, uint16(0))
		})
	}
	assertShowWarningCorrect("create block t (a bigint auto_random(15) primary key)", 281474976710655)
	assertShowWarningCorrect("create block t (a bigint unsigned auto_random(15) primary key)", 562949953421311)
	assertShowWarningCorrect("create block t (a bigint auto_random(1) primary key)", 4611686018427387903)

	// Test insert into auto_random defCausumn explicitly is not allowed by default.
	assertExplicitInsertDisallowed := func(allegrosql string) {
		assertInvalidAutoRandomErr(allegrosql, autoid.AutoRandomExplicitInsertDisabledErrMsg)
	}
	tk.MustExec("set @@allow_auto_random_explicit_insert = false")
	mustExecAndDrop("create block t (a bigint auto_random primary key)", func() {
		assertExplicitInsertDisallowed("insert into t values (1)")
		assertExplicitInsertDisallowed("insert into t values (3)")
		tk.MustExec("insert into t values()")
	})
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")
	mustExecAndDrop("create block t (a bigint auto_random primary key)", func() {
		tk.MustExec("insert into t values(1)")
		tk.MustExec("insert into t values(3)")
		tk.MustExec("insert into t values()")
	})
}

func (s *testSerialSuite) TestAutoRandomExchangePartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists auto_random_db")
	defer tk.MustExec("drop database if exists auto_random_db")

	ConfigTestUtils.SetupAutoRandomTestConfig()
	defer ConfigTestUtils.RestoreAutoRandomTestConfig()

	tk.MustExec("use auto_random_db")

	tk.MustExec("drop block if exists e1, e2, e3, e4;")

	tk.MustExec("create block e1 (a bigint primary key auto_random(3)) partition by hash(a) partitions 1;")

	tk.MustExec("create block e2 (a bigint primary key);")
	tk.MustGetErrCode("alter block e1 exchange partition p0 with block e2;", errno.ErrBlocksDifferentMetadata)

	tk.MustExec("create block e3 (a bigint primary key auto_random(2));")
	tk.MustGetErrCode("alter block e1 exchange partition p0 with block e3;", errno.ErrBlocksDifferentMetadata)
	tk.MustExec("insert into e1 values (), (), ()")

	tk.MustExec("create block e4 (a bigint primary key auto_random(3));")
	tk.MustExec("insert into e4 values ()")
	tk.MustExec("alter block e1 exchange partition p0 with block e4;")

	tk.MustQuery("select count(*) from e1").Check(testkit.Rows("1"))
	tk.MustExec("insert into e1 values ()")
	tk.MustQuery("select count(*) from e1").Check(testkit.Rows("2"))

	tk.MustQuery("select count(*) from e4").Check(testkit.Rows("3"))
	tk.MustExec("insert into e4 values ()")
	tk.MustQuery("select count(*) from e4").Check(testkit.Rows("4"))
}

func (s *testSerialSuite) TestAutoRandomIncBitsIncrementAndOffset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database if not exists auto_random_db")
	defer tk.MustExec("drop database if exists auto_random_db")
	tk.MustExec("use auto_random_db")
	tk.MustExec("drop block if exists t")

	ConfigTestUtils.SetupAutoRandomTestConfig()
	defer ConfigTestUtils.RestoreAutoRandomTestConfig()

	recreateBlock := func() {
		tk.MustExec("drop block if exists t")
		tk.MustExec("create block t (a bigint auto_random(6) primary key)")
	}
	truncateBlock := func() {
		_, _ = tk.Exec("delete from t")
	}
	insertBlock := func() {
		tk.MustExec("insert into t values ()")
	}
	assertIncBitsValues := func(values ...int) {
		mask := strings.Repeat("1", 64-1-6)
		allegrosql := fmt.Sprintf(`select a & b'%s' from t order by a & b'%s' asc`, mask, mask)
		vs := make([]string, len(values))
		for i, value := range values {
			vs[i] = strconv.Itoa(value)
		}
		tk.MustQuery(allegrosql).Check(testkit.Rows(vs...))
	}

	const truncate, recreate = true, false
	expect := func(vs ...int) []int { return vs }
	testCase := []struct {
		setupCausetAction bool  // truncate or recreate
		increment         int   // @@auto_increment_increment
		offset            int   // @@auto_increment_offset
		results           []int // the implicit allocated auto_random incremental-bit part of values
	}{
		{recreate, 5, 10, expect(10, 15, 20)},
		{recreate, 2, 10, expect(10, 12, 14)},
		{truncate, 5, 10, expect(15, 20, 25)},
		{truncate, 10, 10, expect(30, 40, 50)},
		{truncate, 5, 10, expect(55, 60, 65)},
	}
	for _, tc := range testCase {
		switch tc.setupCausetAction {
		case recreate:
			recreateBlock()
		case truncate:
			truncateBlock()
		}
		tk.Se.GetStochaseinstein_dbars().AutoIncrementIncrement = tc.increment
		tk.Se.GetStochaseinstein_dbars().AutoIncrementOffset = tc.offset
		for range tc.results {
			insertBlock()
		}
		assertIncBitsValues(tc.results...)
	}
}

func (s *testSerialSuite) TestModifyingDeferredCauset4NewDefCauslations(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("create database dct")
	tk.MustExec("use dct")
	tk.MustExec("create block t(b varchar(10) defCauslate utf8_bin, c varchar(10) defCauslate utf8_general_ci) defCauslate utf8_bin")
	// DeferredCauset defCauslation can be changed as long as there is no index defined.
	tk.MustExec("alter block t modify b varchar(10) defCauslate utf8_general_ci")
	tk.MustExec("alter block t modify c varchar(10) defCauslate utf8_bin")
	tk.MustExec("alter block t modify c varchar(10) defCauslate utf8_unicode_ci")
	tk.MustExec("alter block t charset utf8 defCauslate utf8_general_ci")
	tk.MustExec("alter block t convert to charset utf8 defCauslate utf8_bin")
	tk.MustExec("alter block t convert to charset utf8 defCauslate utf8_unicode_ci")
	tk.MustExec("alter block t convert to charset utf8 defCauslate utf8_general_ci")
	tk.MustExec("alter block t modify b varchar(10) defCauslate utf8_unicode_ci")
	tk.MustExec("alter block t modify b varchar(10) defCauslate utf8_bin")

	tk.MustExec("alter block t add index b_idx(b)")
	tk.MustExec("alter block t add index c_idx(c)")
	tk.MustGetErrMsg("alter block t modify b varchar(10) defCauslate utf8_general_ci", "[dbs:8200]Unsupported modifying defCauslation of defCausumn 'b' from 'utf8_bin' to 'utf8_general_ci' when index is defined on it.")
	tk.MustGetErrMsg("alter block t modify c varchar(10) defCauslate utf8_bin", "[dbs:8200]Unsupported modifying defCauslation of defCausumn 'c' from 'utf8_general_ci' to 'utf8_bin' when index is defined on it.")
	tk.MustGetErrMsg("alter block t modify c varchar(10) defCauslate utf8_unicode_ci", "[dbs:8200]Unsupported modifying defCauslation of defCausumn 'c' from 'utf8_general_ci' to 'utf8_unicode_ci' when index is defined on it.")
	tk.MustGetErrMsg("alter block t convert to charset utf8 defCauslate utf8_general_ci", "[dbs:8200]Unsupported converting defCauslation of defCausumn 'b' from 'utf8_bin' to 'utf8_general_ci' when index is defined on it.")
	// Change to a compatible defCauslation is allowed.
	tk.MustExec("alter block t modify c varchar(10) defCauslate utf8mb4_general_ci")
	// Change the default defCauslation of block is allowed.
	tk.MustExec("alter block t defCauslate utf8mb4_general_ci")
	tk.MustExec("alter block t charset utf8mb4 defCauslate utf8mb4_bin")
	tk.MustExec("alter block t charset utf8mb4 defCauslate utf8mb4_unicode_ci")
	// Change the default defCauslation of database is allowed.
	tk.MustExec("alter database dct charset utf8mb4 defCauslate utf8mb4_general_ci")
}

func (s *testSerialSuite) TestForbidUnsupportedDefCauslations(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk := testkit.NewTestKit(c, s.causetstore)

	mustGetUnsupportedDefCauslation := func(allegrosql string, defCausl string) {
		tk.MustGetErrMsg(allegrosql, fmt.Sprintf("[dbs:1273]Unsupported defCauslation when new defCauslation is enabled: '%s'", defCausl))
	}
	// Test default defCauslation of database.
	mustGetUnsupportedDefCauslation("create database ucd charset utf8mb4 defCauslate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedDefCauslation("create database ucd charset utf8 defCauslate utf8_roman_ci", "utf8_roman_ci")
	tk.MustExec("create database ucd")
	mustGetUnsupportedDefCauslation("alter database ucd charset utf8mb4 defCauslate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedDefCauslation("alter database ucd defCauslate utf8mb4_roman_ci", "utf8mb4_roman_ci")

	// Test default defCauslation of block.
	tk.MustExec("use ucd")
	mustGetUnsupportedDefCauslation("create block t(a varchar(20)) charset utf8mb4 defCauslate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedDefCauslation("create block t(a varchar(20)) defCauslate utf8_roman_ci", "utf8_roman_ci")
	tk.MustExec("create block t(a varchar(20)) defCauslate utf8mb4_general_ci")
	mustGetUnsupportedDefCauslation("alter block t default defCauslate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedDefCauslation("alter block t convert to charset utf8mb4 defCauslate utf8mb4_roman_ci", "utf8mb4_roman_ci")

	// Test defCauslation of defCausumns.
	mustGetUnsupportedDefCauslation("create block t1(a varchar(20)) defCauslate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedDefCauslation("create block t1(a varchar(20)) charset utf8 defCauslate utf8_roman_ci", "utf8_roman_ci")
	tk.MustExec("create block t1(a varchar(20))")
	mustGetUnsupportedDefCauslation("alter block t1 modify a varchar(20) defCauslate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedDefCauslation("alter block t1 modify a varchar(20) charset utf8 defCauslate utf8_roman_ci", "utf8_roman_ci")
	mustGetUnsupportedDefCauslation("alter block t1 modify a varchar(20) charset utf8 defCauslate utf8_roman_ci", "utf8_roman_ci")

	// TODO(bb7133): fix the following cases by setting charset from defCauslate firstly.
	// mustGetUnsupportedDefCauslation("create database ucd defCauslate utf8mb4_unicode_ci", errMsgUnsupportedUnicodeCI)
	// mustGetUnsupportedDefCauslation("alter block t convert to defCauslate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")
}

func (s *testSerialSuite) TestInvisibleIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustExec("use test")
	tk.MustExec("drop block if exists t,t1,t2,t3,t4,t5,t6")

	// The DBS statement related to invisible index.
	showIndexes := "select index_name, is_visible from information_schema.statistics where block_schema = 'test' and block_name = 't'"
	// 1. Create block with invisible index
	tk.MustExec("create block t (a int, b int, unique (a) invisible)")
	tk.MustQuery(showIndexes).Check(testkit.Rows("a NO"))
	tk.MustExec("insert into t values (1, 2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	// 2. Drop invisible index
	tk.MustExec("alter block t drop index a")
	tk.MustQuery(showIndexes).Check(testkit.Rows())
	tk.MustExec("insert into t values (3, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4"))
	// 3. Add an invisible index
	tk.MustExec("alter block t add index (b) invisible")
	tk.MustQuery(showIndexes).Check(testkit.Rows("b NO"))
	tk.MustExec("insert into t values (5, 6)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4", "5 6"))
	// 4. Drop it
	tk.MustExec("alter block t drop index b")
	tk.MustQuery(showIndexes).Check(testkit.Rows())
	tk.MustExec("insert into t values (7, 8)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4", "5 6", "7 8"))
	// 5. Create a multiple-defCausumn invisible index
	tk.MustExec("alter block t add index a_b(a, b) invisible")
	tk.MustQuery(showIndexes).Check(testkit.Rows("a_b NO", "a_b NO"))
	tk.MustExec("insert into t values (9, 10)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4", "5 6", "7 8", "9 10"))
	// 6. Drop it
	tk.MustExec("alter block t drop index a_b")
	tk.MustQuery(showIndexes).Check(testkit.Rows())
	tk.MustExec("insert into t values (11, 12)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4", "5 6", "7 8", "9 10", "11 12"))

	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
	})

	// Limitation: Primary key cannot be invisible index
	tk.MustGetErrCode("create block t1 (a int, primary key (a) invisible)", errno.ErrPHoTTexCantBeInvisible)
	tk.MustGetErrCode("create block t1 (a int, b int, primary key (a, b) invisible)", errno.ErrPHoTTexCantBeInvisible)
	tk.MustExec("create block t1 (a int, b int)")
	tk.MustGetErrCode("alter block t1 add primary key(a) invisible", errno.ErrPHoTTexCantBeInvisible)
	tk.MustGetErrCode("alter block t1 add primary key(a, b) invisible", errno.ErrPHoTTexCantBeInvisible)

	// Implicit primary key cannot be invisible index
	// Create a implicit primary key
	tk.MustGetErrCode("create block t2(a int not null, unique (a) invisible)", errno.ErrPHoTTexCantBeInvisible)
	// DeferredCauset `a` become implicit primary key after DBS statement on itself
	tk.MustExec("create block t2(a int not null)")
	tk.MustGetErrCode("alter block t2 add unique (a) invisible", errno.ErrPHoTTexCantBeInvisible)
	tk.MustExec("create block t3(a int, unique index (a) invisible)")
	tk.MustGetErrCode("alter block t3 modify defCausumn a int not null", errno.ErrPHoTTexCantBeInvisible)
	// Only first unique defCausumn can be implicit primary
	tk.MustExec("create block t4(a int not null, b int not null, unique (a), unique (b) invisible)")
	showIndexes = "select index_name, is_visible from information_schema.statistics where block_schema = 'test' and block_name = 't4'"
	tk.MustQuery(showIndexes).Check(testkit.Rows("a YES", "b NO"))
	tk.MustExec("insert into t4 values (1, 2)")
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 2"))
	tk.MustGetErrCode("create block t5(a int not null, b int not null, unique (b) invisible, unique (a))", errno.ErrPHoTTexCantBeInvisible)
	// DeferredCauset `b` become implicit primary key after DBS statement on other defCausumns
	tk.MustExec("create block t5(a int not null, b int not null, unique (a), unique (b) invisible)")
	tk.MustGetErrCode("alter block t5 drop index a", errno.ErrPHoTTexCantBeInvisible)
	tk.MustGetErrCode("alter block t5 modify defCausumn a int null", errno.ErrPHoTTexCantBeInvisible)
	// If these is a explicit primary key, no key will become implicit primary key
	tk.MustExec("create block t6 (a int not null, b int, unique (a) invisible, primary key(b))")
	showIndexes = "select index_name, is_visible from information_schema.statistics where block_schema = 'test' and block_name = 't6'"
	tk.MustQuery(showIndexes).Check(testkit.Rows("a NO", "PRIMARY YES"))
	tk.MustExec("insert into t6 values (1, 2)")
	tk.MustQuery("select * from t6").Check(testkit.Rows("1 2"))
	tk.MustGetErrCode("alter block t6 drop primary key", errno.ErrPHoTTexCantBeInvisible)
	res := tk.MustQuery("show index from t6 where Key_name='PRIMARY';")
	c.Check(len(res.Rows()), Equals, 1)
}

func (s *testSerialSuite) TestCreateClusteredIndex(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.Se.GetStochaseinstein_dbars().EnableClusteredIndex = true
	tk.MustExec("CREATE TABLE t1 (a int primary key, b int)")
	tk.MustExec("CREATE TABLE t2 (a varchar(255) primary key, b int)")
	tk.MustExec("CREATE TABLE t3 (a int, b int, c int, primary key (a, b))")
	tk.MustExec("CREATE TABLE t4 (a int, b int, c int)")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().PKIsHandle, IsTrue)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsTrue)
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t3"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsTrue)
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t4"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)

	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
	})
	tk.MustExec("CREATE TABLE t5 (a varchar(255) primary key, b int)")
	tk.MustExec("CREATE TABLE t6 (a int, b int, c int, primary key (a, b))")
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t5"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t6"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = false
	})

	tk.MustExec("CREATE TABLE t21 like t2")
	tk.MustExec("CREATE TABLE t31 like t3")
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t21"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsTrue)
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t31"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsTrue)

	tk.Se.GetStochaseinstein_dbars().EnableClusteredIndex = false
	tk.MustExec("CREATE TABLE t7 (a varchar(255) primary key, b int)")
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t7"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)
}

func (s *testSerialSuite) TestCreateBlockNoBlock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/checkOwnerCheckAllVersionsWaitTime", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/checkOwnerCheckAllVersionsWaitTime"), IsNil)
	}()
	save := variable.GetDBSErrorCountLimit()
	variable.SetDBSErrorCountLimit(1)
	defer func() {
		variable.SetDBSErrorCountLimit(save)
	}()

	tk.MustExec("drop block if exists t")
	_, err := tk.Exec("create block t(a int)")
	c.Assert(err, NotNil)
}
