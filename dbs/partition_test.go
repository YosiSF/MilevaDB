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

package dbs

import (
	"context"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var _ = Suite(&testPartitionSuite{})

type testPartitionSuite struct {
	causetstore ekv.CausetStorage
}

func (s *testPartitionSuite) SetUpSuite(c *C) {
	s.causetstore = testCreateStore(c, "test_store")
}

func (s *testPartitionSuite) TearDownSuite(c *C) {
	err := s.causetstore.Close()
	c.Assert(err, IsNil)
}

func (s *testPartitionSuite) TestDropAndTruncatePartition(c *C) {
	d := testNewDBSAndStart(
		context.Background(),
		c,
		WithStore(s.causetstore),
		WithLease(testLease),
	)
	defer d.Stop()
	dbInfo := testSchemaInfo(c, d, "test_partition")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	// generate 5 partition in blockInfo.
	tblInfo, partIDs := buildBlockInfoWithPartition(c, d)
	ctx := testNewContext(d)
	testCreateBlock(c, ctx, d, dbInfo, tblInfo)

	testDropPartition(c, ctx, d, dbInfo, tblInfo, []string{"p0", "p1"})

	testTruncatePartition(c, ctx, d, dbInfo, tblInfo, []int64{partIDs[3], partIDs[4]})
}

func buildBlockInfoWithPartition(c *C, d *dbs) (*perceptron.BlockInfo, []int64) {
	tbl := &perceptron.BlockInfo{
		Name: perceptron.NewCIStr("t"),
	}
	defCaus := &perceptron.DeferredCausetInfo{
		Name:      perceptron.NewCIStr("c"),
		Offset:    0,
		State:     perceptron.StatePublic,
		FieldType: *types.NewFieldType(allegrosql.TypeLong),
		ID:        allocateDeferredCausetID(tbl),
	}
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	tbl.ID = genIDs[0]
	tbl.DeferredCausets = []*perceptron.DeferredCausetInfo{defCaus}
	tbl.Charset = "utf8"
	tbl.DefCauslate = "utf8_bin"

	partIDs, err := d.genGlobalIDs(5)
	c.Assert(err, IsNil)
	partInfo := &perceptron.PartitionInfo{
		Type:   perceptron.PartitionTypeRange,
		Expr:   tbl.DeferredCausets[0].Name.L,
		Enable: true,
		Definitions: []perceptron.PartitionDefinition{
			{
				ID:       partIDs[0],
				Name:     perceptron.NewCIStr("p0"),
				LessThan: []string{"100"},
			},
			{
				ID:       partIDs[1],
				Name:     perceptron.NewCIStr("p1"),
				LessThan: []string{"200"},
			},
			{
				ID:       partIDs[2],
				Name:     perceptron.NewCIStr("p2"),
				LessThan: []string{"300"},
			},
			{
				ID:       partIDs[3],
				Name:     perceptron.NewCIStr("p3"),
				LessThan: []string{"400"},
			},
			{
				ID:       partIDs[4],
				Name:     perceptron.NewCIStr("p4"),
				LessThan: []string{"500"},
			},
		},
	}
	tbl.Partition = partInfo
	return tbl, partIDs
}

func buildDropPartitionJob(dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, partNames []string) *perceptron.Job {
	return &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionDropBlockPartition,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{partNames},
	}
}

func testDropPartition(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, partNames []string) *perceptron.Job {
	job := buildDropPartitionJob(dbInfo, tblInfo, partNames)
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildTruncatePartitionJob(dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, pids []int64) *perceptron.Job {
	return &perceptron.Job{
		SchemaID:   dbInfo.ID,
		BlockID:    tblInfo.ID,
		Type:       perceptron.CausetActionTruncateBlockPartition,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{pids},
	}
}

func testTruncatePartition(c *C, ctx stochastikctx.Context, d *dbs, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, pids []int64) *perceptron.Job {
	job := buildTruncatePartitionJob(dbInfo, tblInfo, pids)
	err := d.doDBSJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}
