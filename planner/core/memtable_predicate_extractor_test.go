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

package core_test

import (
	"context"
	"regexp"
	"sort"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/stochastik"
)

var _ = Suite(&extractorSuite{})

type extractorSuite struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

func (s *extractorSuite) SetUpSuite(c *C) {
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	c.Assert(causetstore, NotNil)

	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()
	dom, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	c.Assert(dom, NotNil)

	s.causetstore = causetstore
	s.dom = dom
}

func (s *extractorSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *extractorSuite) getLogicalMemBlock(c *C, se stochastik.Stochastik, berolinaAllegroSQL *berolinaAllegroSQL.berolinaAllegroSQL, allegrosql string) *plannercore.LogicalMemBlock {
	stmt, err := berolinaAllegroSQL.ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil)

	ctx := context.Background()
	builder := plannercore.NewPlanBuilder(se, s.dom.SchemaReplicant(), &hint.BlockHintProcessor{})
	plan, err := builder.Build(ctx, stmt)
	c.Assert(err, IsNil)

	logicalPlan, err := plannercore.LogicalOptimize(ctx, builder.GetOptFlag(), plan.(plannercore.LogicalPlan))
	c.Assert(err, IsNil)

	// Obtain the leaf plan
	leafPlan := logicalPlan
	for len(leafPlan.Children()) > 0 {
		leafPlan = leafPlan.Children()[0]
	}

	logicalMemBlock := leafPlan.(*plannercore.LogicalMemBlock)
	return logicalMemBlock
}

func (s *extractorSuite) TestClusterConfigBlockExtractor(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	berolinaAllegroSQL := berolinaAllegroSQL.New()
	var cases = []struct {
		allegrosql  string
		nodeTypes   set.StringSet
		instances   set.StringSet
		skipRequest bool
	}{
		{
			allegrosql: "select * from information_schema.cluster_config",
			nodeTypes:  nil,
			instances:  nil,
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='einsteindb'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where 'einsteindb'=type",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where 'EinsteinDB'=type",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where 'einsteindb'=type",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where 'EinsteinDB'=type or type='milevadb'",
			nodeTypes:  set.NewStringSet("einsteindb", "milevadb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where 'EinsteinDB'=type or type='milevadb' or type='fidel'",
			nodeTypes:  set.NewStringSet("einsteindb", "milevadb", "fidel"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where (type='milevadb' or type='fidel') and (instance='123.1.1.2:1234' or instance='123.1.1.4:1234')",
			nodeTypes:  set.NewStringSet("milevadb", "fidel"),
			instances:  set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type in ('einsteindb', 'fidel')",
			nodeTypes:  set.NewStringSet("einsteindb", "fidel"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type in ('einsteindb', 'fidel') and instance='123.1.1.2:1234'",
			nodeTypes:  set.NewStringSet("einsteindb", "fidel"),
			instances:  set.NewStringSet("123.1.1.2:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type in ('einsteindb', 'fidel') and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes:  set.NewStringSet("einsteindb", "fidel"),
			instances:  set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='einsteindb' and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='einsteindb' and instance='123.1.1.4:1234'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='einsteindb' and instance='123.1.1.4:1234'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='einsteindb' and instance='cNs2dm.einsteindb.whtcorpsinc.com:1234'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("cNs2dm.einsteindb.whtcorpsinc.com:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='EinsteinDB' and instance='cNs2dm.einsteindb.whtcorpsinc.com:1234'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("cNs2dm.einsteindb.whtcorpsinc.com:1234"),
		},
		{
			allegrosql:  "select * from information_schema.cluster_config where type='einsteindb' and type='fidel'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='einsteindb' and type in ('fidel', 'einsteindb')",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql:  "select * from information_schema.cluster_config where type='einsteindb' and type in ('fidel', 'milevadb')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type in ('einsteindb', 'milevadb') and type in ('fidel', 'milevadb')",
			nodeTypes:  set.NewStringSet("milevadb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql:  "select * from information_schema.cluster_config where instance='123.1.1.4:1234' and instance='123.1.1.5:1234'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_config where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.4:1234')",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet("123.1.1.4:1234"),
		},
		{
			allegrosql:  "select * from information_schema.cluster_config where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_config where instance in ('123.1.1.5:1234', '123.1.1.4:1234') and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet("123.1.1.5:1234"),
		},
		{
			allegrosql: `select * from information_schema.cluster_config
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and type in ('einsteindb', 'milevadb')
				  and type in ('fidel', 'milevadb')`,
			nodeTypes: set.NewStringSet("milevadb"),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			allegrosql: `select * from information_schema.cluster_config
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and instance in ('123.1.1.6:1234', '123.1.1.7:1234')
				  and instance in ('123.1.1.7:1234', '123.1.1.8:1234')`,
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
	}
	for _, ca := range cases {
		logicalMemBlock := s.getLogicalMemBlock(c, se, berolinaAllegroSQL, ca.allegrosql)
		c.Assert(logicalMemBlock.Extractor, NotNil)

		clusterConfigExtractor := logicalMemBlock.Extractor.(*plannercore.ClusterBlockExtractor)
		c.Assert(clusterConfigExtractor.NodeTypes, DeepEquals, ca.nodeTypes, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		c.Assert(clusterConfigExtractor.Instances, DeepEquals, ca.instances, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		c.Assert(clusterConfigExtractor.SkipRequest, DeepEquals, ca.skipRequest, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
	}
}

func timestamp(c *C, s string) int64 {
	t, err := time.ParseInLocation("2006-01-02 15:04:05.999", s, time.Local)
	c.Assert(err, IsNil)
	return t.UnixNano() / int64(time.Millisecond)
}

func (s *extractorSuite) TestClusterLogBlockExtractor(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	berolinaAllegroSQL := berolinaAllegroSQL.New()
	var cases = []struct {
		allegrosql         string
		nodeTypes          set.StringSet
		instances          set.StringSet
		skipRequest        bool
		startTime, endTime int64
		patterns           []string
		level              set.StringSet
	}{
		{
			allegrosql: "select * from information_schema.cluster_log",
			nodeTypes:  nil,
			instances:  nil,
		},
		{
			// Test for invalid time.
			allegrosql: "select * from information_schema.cluster_log where time='2020-10-10 10::10'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type='einsteindb'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where 'einsteindb'=type",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where 'EinsteinDB'=type",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where 'EinsteinDB'=type or type='milevadb'",
			nodeTypes:  set.NewStringSet("einsteindb", "milevadb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where 'EinsteinDB'=type or type='milevadb' or type='fidel'",
			nodeTypes:  set.NewStringSet("einsteindb", "milevadb", "fidel"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where (type='milevadb' or type='fidel') and (instance='123.1.1.2:1234' or instance='123.1.1.4:1234')",
			nodeTypes:  set.NewStringSet("milevadb", "fidel"),
			instances:  set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type in ('einsteindb', 'fidel')",
			nodeTypes:  set.NewStringSet("einsteindb", "fidel"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type in ('einsteindb', 'fidel') and instance='123.1.1.2:1234'",
			nodeTypes:  set.NewStringSet("einsteindb", "fidel"),
			instances:  set.NewStringSet("123.1.1.2:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type in ('einsteindb', 'fidel') and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes:  set.NewStringSet("einsteindb", "fidel"),
			instances:  set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type='einsteindb' and instance in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type='einsteindb' and instance='123.1.1.4:1234'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type='einsteindb' and instance='123.1.1.4:1234'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type='einsteindb' and instance='cNs2dm.einsteindb.whtcorpsinc.com:1234'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("cNs2dm.einsteindb.whtcorpsinc.com:1234"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type='EinsteinDB' and instance='cNs2dm.einsteindb.whtcorpsinc.com:1234'",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet("cNs2dm.einsteindb.whtcorpsinc.com:1234"),
		},
		{
			allegrosql:  "select * from information_schema.cluster_log where type='einsteindb' and type='fidel'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type='einsteindb' and type in ('fidel', 'einsteindb')",
			nodeTypes:  set.NewStringSet("einsteindb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql:  "select * from information_schema.cluster_log where type='einsteindb' and type in ('fidel', 'milevadb')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_log where type in ('einsteindb', 'milevadb') and type in ('fidel', 'milevadb')",
			nodeTypes:  set.NewStringSet("milevadb"),
			instances:  set.NewStringSet(),
		},
		{
			allegrosql:  "select * from information_schema.cluster_log where instance='123.1.1.4:1234' and instance='123.1.1.5:1234'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_log where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.4:1234')",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet("123.1.1.4:1234"),
		},
		{
			allegrosql:  "select * from information_schema.cluster_log where instance='123.1.1.4:1234' and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_log where instance in ('123.1.1.5:1234', '123.1.1.4:1234') and instance in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet("123.1.1.5:1234"),
		},
		{
			allegrosql: `select * from information_schema.cluster_log
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and type in ('einsteindb', 'milevadb')
				  and type in ('fidel', 'milevadb')`,
			nodeTypes: set.NewStringSet("milevadb"),
			instances: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			allegrosql: `select * from information_schema.cluster_log
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and instance in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and instance in ('123.1.1.6:1234', '123.1.1.7:1234')
				  and instance in ('123.1.1.7:1234', '123.1.1.8:1234')`,
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_log where time='2020-10-10 10:10:10'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			startTime:  timestamp(c, "2020-10-10 10:10:10"),
			endTime:    timestamp(c, "2020-10-10 10:10:10"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where time>='2020-10-10 10:10:10' and time<='2020-10-11 10:10:10'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			startTime:  timestamp(c, "2020-10-10 10:10:10"),
			endTime:    timestamp(c, "2020-10-11 10:10:10"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where time>'2020-10-10 10:10:10' and time<'2020-10-11 10:10:10'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			startTime:  timestamp(c, "2020-10-10 10:10:10") + 1,
			endTime:    timestamp(c, "2020-10-11 10:10:10") - 1,
		},
		{
			allegrosql: "select * from information_schema.cluster_log where time>='2020-10-10 10:10:10' and time<'2020-10-11 10:10:10'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			startTime:  timestamp(c, "2020-10-10 10:10:10"),
			endTime:    timestamp(c, "2020-10-11 10:10:10") - 1,
		},
		{
			allegrosql:  "select * from information_schema.cluster_log where time>='2020-10-12 10:10:10' and time<'2020-10-11 10:10:10'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			startTime:   timestamp(c, "2020-10-12 10:10:10"),
			endTime:     timestamp(c, "2020-10-11 10:10:10") - 1,
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_log where time>='2020-10-10 10:10:10'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			startTime:  timestamp(c, "2020-10-10 10:10:10"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where time>='2020-10-10 10:10:10' and  time>='2020-10-11 10:10:10' and  time>='2020-10-12 10:10:10'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			startTime:  timestamp(c, "2020-10-12 10:10:10"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where time>='2020-10-10 10:10:10' and  time>='2020-10-11 10:10:10' and  time>='2020-10-12 10:10:10' and time='2020-10-13 10:10:10'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			startTime:  timestamp(c, "2020-10-13 10:10:10"),
			endTime:    timestamp(c, "2020-10-13 10:10:10"),
		},
		{
			allegrosql:  "select * from information_schema.cluster_log where time<='2020-10-10 10:10:10' and time='2020-10-13 10:10:10'",
			nodeTypes:   set.NewStringSet(),
			instances:   set.NewStringSet(),
			startTime:   timestamp(c, "2020-10-13 10:10:10"),
			endTime:     timestamp(c, "2020-10-10 10:10:10"),
			skipRequest: true,
		},
		{
			allegrosql: "select * from information_schema.cluster_log where time='2020-10-10 10:10:10' and time<='2020-10-13 10:10:10'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			startTime:  timestamp(c, "2020-10-10 10:10:10"),
			endTime:    timestamp(c, "2020-10-10 10:10:10"),
		},
		{
			allegrosql: "select * from information_schema.cluster_log where time>='2020-10-10 10:10:10' and message like '%a%'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			startTime:  timestamp(c, "2020-10-10 10:10:10"),
			patterns:   []string{".*a.*"},
		},
		{
			allegrosql: "select * from information_schema.cluster_log where message like '%a%' and message regexp '^b'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			patterns:   []string{".*a.*", "^b"},
		},
		{
			allegrosql: "select * from information_schema.cluster_log where message='gc'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			patterns:   []string{"^gc$"},
		},
		{
			allegrosql: "select * from information_schema.cluster_log where message='.*txn.*'",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			patterns:   []string{"^" + regexp.QuoteMeta(".*txn.*") + "$"},
		},
		{
			allegrosql: `select * from information_schema.cluster_log
				where instance in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and (type='milevadb' or type='fidel')
				  and message like '%interlock%'
				  and message regexp '.*txn=123.*'
				  and level in ('debug', 'info', 'ERROR')`,
			nodeTypes: set.NewStringSet("milevadb", "fidel"),
			instances: set.NewStringSet("123.1.1.5:1234", "123.1.1.4:1234"),
			level:     set.NewStringSet("debug", "info", "error"),
			patterns:  []string{".*interlock.*", ".*txn=123.*"},
		},
		{
			allegrosql: "select * from information_schema.cluster_log where (message regexp '.*fidel.*' or message regexp '.*milevadb.*' or message like '%einsteindb%')",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			patterns:   []string{".*fidel.*|.*milevadb.*|.*einsteindb.*"},
		},
		{
			allegrosql: "select * from information_schema.cluster_log where (level = 'debug' or level = 'ERROR')",
			nodeTypes:  set.NewStringSet(),
			instances:  set.NewStringSet(),
			level:      set.NewStringSet("debug", "error"),
		},
	}
	for _, ca := range cases {
		logicalMemBlock := s.getLogicalMemBlock(c, se, berolinaAllegroSQL, ca.allegrosql)
		c.Assert(logicalMemBlock.Extractor, NotNil)

		clusterConfigExtractor := logicalMemBlock.Extractor.(*plannercore.ClusterLogBlockExtractor)
		c.Assert(clusterConfigExtractor.NodeTypes, DeepEquals, ca.nodeTypes, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		c.Assert(clusterConfigExtractor.Instances, DeepEquals, ca.instances, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		c.Assert(clusterConfigExtractor.SkipRequest, DeepEquals, ca.skipRequest, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		if ca.startTime > 0 {
			c.Assert(clusterConfigExtractor.StartTime, Equals, ca.startTime, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		if ca.endTime > 0 {
			c.Assert(clusterConfigExtractor.EndTime, Equals, ca.endTime, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		c.Assert(clusterConfigExtractor.Patterns, DeepEquals, ca.patterns, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		if len(ca.level) > 0 {
			c.Assert(clusterConfigExtractor.LogLevels, DeepEquals, ca.level, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
	}
}

func (s *extractorSuite) TestMetricBlockExtractor(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	parseTime := func(c *C, s string) time.Time {
		t, err := time.ParseInLocation(plannercore.MetricBlockTimeFormat, s, time.Local)
		c.Assert(err, IsNil)
		return t
	}

	berolinaAllegroSQL := berolinaAllegroSQL.New()
	var cases = []struct {
		allegrosql         string
		skipRequest        bool
		startTime, endTime time.Time
		labelConditions    map[string]set.StringSet
		quantiles          []float64
		promQL             string
	}{
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration",
			promQL:     "histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where instance='127.0.0.1:10080'",
			promQL:     `histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{instance="127.0.0.1:10080"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080"),
			},
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where instance='127.0.0.1:10080' or instance='127.0.0.1:10081'",
			promQL:     `histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{instance=~"127.0.0.1:10080|127.0.0.1:10081"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080", "127.0.0.1:10081"),
			},
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where instance='127.0.0.1:10080' and sql_type='general'",
			promQL:     `histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{instance="127.0.0.1:10080",sql_type="general"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080"),
				"sql_type": set.NewStringSet("general"),
			},
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where instance='127.0.0.1:10080' or sql_type='general'",
			promQL:     `histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where instance='127.0.0.1:10080' and sql_type='UFIDelate' and time='2020-10-10 10:10:10'",
			promQL:     `histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{instance="127.0.0.1:10080",sql_type="UFIDelate"}[60s])) by (le,sql_type,instance))`,
			labelConditions: map[string]set.StringSet{
				"instance": set.NewStringSet("127.0.0.1:10080"),
				"sql_type": set.NewStringSet("UFIDelate"),
			},
			startTime: parseTime(c, "2020-10-10 10:10:10"),
			endTime:   parseTime(c, "2020-10-10 10:10:10"),
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where time>'2020-10-10 10:10:10' and time<'2020-10-11 10:10:10'",
			promQL:     `histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
			startTime:  parseTime(c, "2020-10-10 10:10:10.001"),
			endTime:    parseTime(c, "2020-10-11 10:10:09.999"),
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where time>='2020-10-10 10:10:10'",
			promQL:     `histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))`,
			startTime:  parseTime(c, "2020-10-10 10:10:10"),
			endTime:    parseTime(c, "2020-10-10 10:20:10"),
		},
		{
			allegrosql:  "select * from metrics_schema.milevadb_query_duration where time>='2020-10-10 10:10:10' and time<='2020-10-09 10:10:10'",
			promQL:      "",
			startTime:   parseTime(c, "2020-10-10 10:10:10"),
			endTime:     parseTime(c, "2020-10-09 10:10:10"),
			skipRequest: true,
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where time<='2020-10-09 10:10:10'",
			promQL:     "histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			startTime:  parseTime(c, "2020-10-09 10:00:10"),
			endTime:    parseTime(c, "2020-10-09 10:10:10"),
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where quantile=0.9 or quantile=0.8",
			promQL: "histogram_quantile(0.8, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))," +
				"histogram_quantile(0.9, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			quantiles: []float64{0.8, 0.9},
		},
		{
			allegrosql: "select * from metrics_schema.milevadb_query_duration where quantile=0",
			promQL:     "histogram_quantile(0, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{}[60s])) by (le,sql_type,instance))",
			quantiles:  []float64{0},
		},
	}
	se.GetStochastikVars().StmtCtx.TimeZone = time.Local
	for _, ca := range cases {
		logicalMemBlock := s.getLogicalMemBlock(c, se, berolinaAllegroSQL, ca.allegrosql)
		c.Assert(logicalMemBlock.Extractor, NotNil)
		metricBlockExtractor := logicalMemBlock.Extractor.(*plannercore.MetricBlockExtractor)
		if len(ca.labelConditions) > 0 {
			c.Assert(metricBlockExtractor.LabelConditions, DeepEquals, ca.labelConditions, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		c.Assert(metricBlockExtractor.SkipRequest, DeepEquals, ca.skipRequest, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		if len(metricBlockExtractor.Quantiles) > 0 {
			c.Assert(metricBlockExtractor.Quantiles, DeepEquals, ca.quantiles)
		}
		if !ca.skipRequest {
			promQL := metricBlockExtractor.GetMetricBlockPromQL(se, "milevadb_query_duration")
			c.Assert(promQL, DeepEquals, ca.promQL, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
			start, end := metricBlockExtractor.StartTime, metricBlockExtractor.EndTime
			c.Assert(start.UnixNano() <= end.UnixNano(), IsTrue)
			if ca.startTime.Unix() > 0 {
				c.Assert(metricBlockExtractor.StartTime, DeepEquals, ca.startTime, Commentf("ALLEGROALLEGROSQL: %v, start_time: %v", ca.allegrosql, metricBlockExtractor.StartTime))
			}
			if ca.endTime.Unix() > 0 {
				c.Assert(metricBlockExtractor.EndTime, DeepEquals, ca.endTime, Commentf("ALLEGROALLEGROSQL: %v, end_time: %v", ca.allegrosql, metricBlockExtractor.EndTime))
			}
		}
	}
}

func (s *extractorSuite) TestMetricsSummaryBlockExtractor(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	var cases = []struct {
		allegrosql  string
		names       set.StringSet
		quantiles   []float64
		skipRequest bool
	}{
		{
			allegrosql: "select * from information_schema.metrics_summary",
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where quantile='0.999'",
			quantiles:  []float64{0.999},
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where '0.999'=quantile or quantile='0.95'",
			quantiles:  []float64{0.999, 0.95},
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where '0.999'=quantile or quantile='0.95' or quantile='0.99'",
			quantiles:  []float64{0.999, 0.95, 0.99},
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where (quantile='0.95' or quantile='0.99') and (metrics_name='metric_name3' or metrics_name='metric_name1')",
			quantiles:  []float64{0.95, 0.99},
			names:      set.NewStringSet("metric_name3", "metric_name1"),
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where quantile in ('0.999', '0.99')",
			quantiles:  []float64{0.999, 0.99},
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where quantile in ('0.999', '0.99') and metrics_name='metric_name1'",
			quantiles:  []float64{0.999, 0.99},
			names:      set.NewStringSet("metric_name1"),
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where quantile in ('0.999', '0.99') and metrics_name in ('metric_name1', 'metric_name2')",
			quantiles:  []float64{0.999, 0.99},
			names:      set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where quantile='0.999' and metrics_name in ('metric_name1', 'metric_name2')",
			quantiles:  []float64{0.999},
			names:      set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where quantile='0.999' and metrics_name='metric_NAME3'",
			quantiles:  []float64{0.999},
			names:      set.NewStringSet("metric_name3"),
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where quantile='0.999' and quantile in ('0.99', '0.999')",
			quantiles:  []float64{0.999},
		},
		{
			allegrosql: "select * from information_schema.metrics_summary where quantile in ('0.999', '0.95') and quantile in ('0.99', '0.95')",
			quantiles:  []float64{0.95},
		},
		{
			allegrosql: `select * from information_schema.metrics_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and quantile in ('0.999', '0.95')
				  and quantile in ('0.99', '0.95')
				  and quantile in (0.80, 0.90)`,
			skipRequest: true,
		},
		{
			allegrosql: `select * from information_schema.metrics_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name1')
				  and metrics_name in ('metric_name1', 'metric_name3')`,
			skipRequest: true,
		},
	}
	berolinaAllegroSQL := berolinaAllegroSQL.New()
	for _, ca := range cases {
		sort.Float64s(ca.quantiles)

		logicalMemBlock := s.getLogicalMemBlock(c, se, berolinaAllegroSQL, ca.allegrosql)
		c.Assert(logicalMemBlock.Extractor, NotNil)

		extractor := logicalMemBlock.Extractor.(*plannercore.MetricSummaryBlockExtractor)
		if len(ca.quantiles) > 0 {
			c.Assert(extractor.Quantiles, DeepEquals, ca.quantiles, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		if len(ca.names) > 0 {
			c.Assert(extractor.MetricsNames, DeepEquals, ca.names, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		c.Assert(extractor.SkipRequest, Equals, ca.skipRequest, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
	}
}

func (s *extractorSuite) TestInspectionResultBlockExtractor(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	var cases = []struct {
		allegrosql     string
		rules          set.StringSet
		items          set.StringSet
		skipInspection bool
	}{
		{
			allegrosql: "select * from information_schema.inspection_result",
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule='dbs'",
			rules:      set.NewStringSet("dbs"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where 'dbs'=rule",
			rules:      set.NewStringSet("dbs"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where 'dbs'=rule",
			rules:      set.NewStringSet("dbs"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where 'dbs'=rule or rule='config'",
			rules:      set.NewStringSet("dbs", "config"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where 'dbs'=rule or rule='config' or rule='slow_query'",
			rules:      set.NewStringSet("dbs", "config", "slow_query"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where (rule='config' or rule='slow_query') and (item='dbs.owner' or item='dbs.lease')",
			rules:      set.NewStringSet("config", "slow_query"),
			items:      set.NewStringSet("dbs.owner", "dbs.lease"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule in ('dbs', 'slow_query')",
			rules:      set.NewStringSet("dbs", "slow_query"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule in ('dbs', 'slow_query') and item='dbs.lease'",
			rules:      set.NewStringSet("dbs", "slow_query"),
			items:      set.NewStringSet("dbs.lease"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule in ('dbs', 'slow_query') and item in ('dbs.lease', '123.1.1.4:1234')",
			rules:      set.NewStringSet("dbs", "slow_query"),
			items:      set.NewStringSet("dbs.lease", "123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule='dbs' and item in ('dbs.lease', '123.1.1.4:1234')",
			rules:      set.NewStringSet("dbs"),
			items:      set.NewStringSet("dbs.lease", "123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule='dbs' and item='123.1.1.4:1234'",
			rules:      set.NewStringSet("dbs"),
			items:      set.NewStringSet("123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule='dbs' and item='123.1.1.4:1234'",
			rules:      set.NewStringSet("dbs"),
			items:      set.NewStringSet("123.1.1.4:1234"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule='dbs' and item='DBS.lease'",
			rules:      set.NewStringSet("dbs"),
			items:      set.NewStringSet("dbs.lease"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule='dbs' and item='dbs.OWNER'",
			rules:      set.NewStringSet("dbs"),
			items:      set.NewStringSet("dbs.owner"),
		},
		{
			allegrosql:     "select * from information_schema.inspection_result where rule='dbs' and rule='slow_query'",
			skipInspection: true,
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule='dbs' and rule in ('slow_query', 'dbs')",
			rules:      set.NewStringSet("dbs"),
		},
		{
			allegrosql: "select * from information_schema.inspection_result where rule in ('dbs', 'config') and rule in ('slow_query', 'config')",
			rules:      set.NewStringSet("config"),
		},
		{
			allegrosql:     "select * from information_schema.inspection_result where item='dbs.lease' and item='raftstore.threadpool'",
			skipInspection: true,
		},
		{
			allegrosql: "select * from information_schema.inspection_result where item='raftstore.threadpool' and item in ('raftstore.threadpool', 'dbs.lease')",
			items:      set.NewStringSet("raftstore.threadpool"),
		},
		{
			allegrosql:     "select * from information_schema.inspection_result where item='raftstore.threadpool' and item in ('dbs.lease', 'scheduler.limit')",
			skipInspection: true,
		},
		{
			allegrosql: "select * from information_schema.inspection_result where item in ('dbs.lease', 'scheduler.limit') and item in ('raftstore.threadpool', 'scheduler.limit')",
			items:      set.NewStringSet("scheduler.limit"),
		},
		{
			allegrosql: `select * from information_schema.inspection_result
				where item in ('dbs.lease', 'scheduler.limit')
				  and item in ('raftstore.threadpool', 'scheduler.limit')
				  and rule in ('dbs', 'config')
				  and rule in ('slow_query', 'config')`,
			rules: set.NewStringSet("config"),
			items: set.NewStringSet("scheduler.limit"),
		},
		{
			allegrosql: `select * from information_schema.inspection_result
				where item in ('dbs.lease', 'scheduler.limit')
				  and item in ('raftstore.threadpool', 'scheduler.limit')
				  and item in ('raftstore.threadpool', 'dbs.lease')
				  and item in ('dbs.lease', 'dbs.owner')`,
			skipInspection: true,
		},
	}
	berolinaAllegroSQL := berolinaAllegroSQL.New()
	for _, ca := range cases {
		logicalMemBlock := s.getLogicalMemBlock(c, se, berolinaAllegroSQL, ca.allegrosql)
		c.Assert(logicalMemBlock.Extractor, NotNil)

		clusterConfigExtractor := logicalMemBlock.Extractor.(*plannercore.InspectionResultBlockExtractor)
		if len(ca.rules) > 0 {
			c.Assert(clusterConfigExtractor.Memrules, DeepEquals, ca.rules, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		if len(ca.items) > 0 {
			c.Assert(clusterConfigExtractor.Items, DeepEquals, ca.items, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		c.Assert(clusterConfigExtractor.SkipInspection, Equals, ca.skipInspection, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
	}
}

func (s *extractorSuite) TestInspectionSummaryBlockExtractor(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	var cases = []struct {
		allegrosql     string
		rules          set.StringSet
		names          set.StringSet
		quantiles      set.Float64Set
		skipInspection bool
	}{
		{
			allegrosql: "select * from information_schema.inspection_summary",
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where rule='dbs'",
			rules:      set.NewStringSet("dbs"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where 'dbs'=rule or rule='config'",
			rules:      set.NewStringSet("dbs", "config"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where 'dbs'=rule or rule='config' or rule='slow_query'",
			rules:      set.NewStringSet("dbs", "config", "slow_query"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where (rule='config' or rule='slow_query') and (metrics_name='metric_name3' or metrics_name='metric_name1')",
			rules:      set.NewStringSet("config", "slow_query"),
			names:      set.NewStringSet("metric_name3", "metric_name1"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where rule in ('dbs', 'slow_query')",
			rules:      set.NewStringSet("dbs", "slow_query"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where rule in ('dbs', 'slow_query') and metrics_name='metric_name1'",
			rules:      set.NewStringSet("dbs", "slow_query"),
			names:      set.NewStringSet("metric_name1"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where rule in ('dbs', 'slow_query') and metrics_name in ('metric_name1', 'metric_name2')",
			rules:      set.NewStringSet("dbs", "slow_query"),
			names:      set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where rule='dbs' and metrics_name in ('metric_name1', 'metric_name2')",
			rules:      set.NewStringSet("dbs"),
			names:      set.NewStringSet("metric_name1", "metric_name2"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where rule='dbs' and metrics_name='metric_NAME3'",
			rules:      set.NewStringSet("dbs"),
			names:      set.NewStringSet("metric_name3"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where rule='dbs' and rule in ('slow_query', 'dbs')",
			rules:      set.NewStringSet("dbs"),
		},
		{
			allegrosql: "select * from information_schema.inspection_summary where rule in ('dbs', 'config') and rule in ('slow_query', 'config')",
			rules:      set.NewStringSet("config"),
		},
		{
			allegrosql: `select * from information_schema.inspection_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and rule in ('dbs', 'config')
				  and rule in ('slow_query', 'config')
				  and quantile in (0.80, 0.90)`,
			rules:     set.NewStringSet("config"),
			names:     set.NewStringSet("metric_name4"),
			quantiles: set.NewFloat64Set(0.80, 0.90),
		},
		{
			allegrosql: `select * from information_schema.inspection_summary
				where metrics_name in ('metric_name1', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name4')
				  and metrics_name in ('metric_name5', 'metric_name1')
				  and metrics_name in ('metric_name1', 'metric_name3')`,
			skipInspection: true,
		},
	}
	berolinaAllegroSQL := berolinaAllegroSQL.New()
	for _, ca := range cases {
		logicalMemBlock := s.getLogicalMemBlock(c, se, berolinaAllegroSQL, ca.allegrosql)
		c.Assert(logicalMemBlock.Extractor, NotNil)

		clusterConfigExtractor := logicalMemBlock.Extractor.(*plannercore.InspectionSummaryBlockExtractor)
		if len(ca.rules) > 0 {
			c.Assert(clusterConfigExtractor.Memrules, DeepEquals, ca.rules, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		if len(ca.names) > 0 {
			c.Assert(clusterConfigExtractor.MetricNames, DeepEquals, ca.names, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		c.Assert(clusterConfigExtractor.SkipInspection, Equals, ca.skipInspection, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
	}
}

func (s *extractorSuite) TestInspectionMemruleBlockExtractor(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	var cases = []struct {
		allegrosql string
		tps        set.StringSet
		skip       bool
	}{
		{
			allegrosql: "select * from information_schema.inspection_rules",
		},
		{
			allegrosql: "select * from information_schema.inspection_rules where type='inspection'",
			tps:        set.NewStringSet("inspection"),
		},
		{
			allegrosql: "select * from information_schema.inspection_rules where type='inspection' or type='summary'",
			tps:        set.NewStringSet("inspection", "summary"),
		},
		{
			allegrosql: "select * from information_schema.inspection_rules where type='inspection' and type='summary'",
			skip:       true,
		},
	}
	berolinaAllegroSQL := berolinaAllegroSQL.New()
	for _, ca := range cases {
		logicalMemBlock := s.getLogicalMemBlock(c, se, berolinaAllegroSQL, ca.allegrosql)
		c.Assert(logicalMemBlock.Extractor, NotNil)

		clusterConfigExtractor := logicalMemBlock.Extractor.(*plannercore.InspectionMemruleBlockExtractor)
		if len(ca.tps) > 0 {
			c.Assert(clusterConfigExtractor.Types, DeepEquals, ca.tps, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
		}
		c.Assert(clusterConfigExtractor.SkipRequest, Equals, ca.skip, Commentf("ALLEGROALLEGROSQL: %v", ca.allegrosql))
	}
}
