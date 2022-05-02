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

package executor_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
)

var _ = SerialSuites(&inspectionResultSuite{&testClusterBlockBase{}})

type inspectionResultSuite struct{ *testClusterBlockBase }

func (s *inspectionResultSuite) TestInspectionResult(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	mockData := map[string]variable.BlockSnapshot{}
	// mock configuration inconsistent
	mockData[schemareplicant.BlockClusterConfig] = variable.BlockSnapshot{
		Events: [][]types.Causet{
			types.MakeCausets("milevadb", "192.168.3.22:4000", "dbs.lease", "1"),
			types.MakeCausets("milevadb", "192.168.3.23:4000", "dbs.lease", "2"),
			types.MakeCausets("milevadb", "192.168.3.24:4000", "dbs.lease", "1"),
			types.MakeCausets("milevadb", "192.168.3.25:4000", "dbs.lease", "1"),
			types.MakeCausets("milevadb", "192.168.3.24:4000", "status.status-port", "10080"),
			types.MakeCausets("milevadb", "192.168.3.25:4000", "status.status-port", "10081"),
			types.MakeCausets("milevadb", "192.168.3.24:4000", "log.slow-threshold", "0"),
			types.MakeCausets("milevadb", "192.168.3.25:4000", "log.slow-threshold", "1"),
			types.MakeCausets("einsteindb", "192.168.3.32:26600", "interlock.high", "8"),
			types.MakeCausets("einsteindb", "192.168.3.33:26600", "interlock.high", "8"),
			types.MakeCausets("einsteindb", "192.168.3.34:26600", "interlock.high", "7"),
			types.MakeCausets("einsteindb", "192.168.3.35:26600", "interlock.high", "7"),
			types.MakeCausets("einsteindb", "192.168.3.35:26600", "raftstore.sync-log", "false"),
			types.MakeCausets("fidel", "192.168.3.32:2379", "scheduler.limit", "3"),
			types.MakeCausets("fidel", "192.168.3.33:2379", "scheduler.limit", "3"),
			types.MakeCausets("fidel", "192.168.3.34:2379", "scheduler.limit", "3"),
			types.MakeCausets("fidel", "192.168.3.35:2379", "scheduler.limit", "3"),
			types.MakeCausets("fidel", "192.168.3.34:2379", "advertise-client-urls", "0"),
			types.MakeCausets("fidel", "192.168.3.35:2379", "advertise-client-urls", "1"),
			types.MakeCausets("fidel", "192.168.3.34:2379", "advertise-peer-urls", "0"),
			types.MakeCausets("fidel", "192.168.3.35:2379", "advertise-peer-urls", "1"),
			types.MakeCausets("fidel", "192.168.3.34:2379", "client-urls", "0"),
			types.MakeCausets("fidel", "192.168.3.35:2379", "client-urls", "1"),
			types.MakeCausets("fidel", "192.168.3.34:2379", "log.file.filename", "0"),
			types.MakeCausets("fidel", "192.168.3.35:2379", "log.file.filename", "1"),
			types.MakeCausets("fidel", "192.168.3.34:2379", "metric.job", "0"),
			types.MakeCausets("fidel", "192.168.3.35:2379", "metric.job", "1"),
			types.MakeCausets("fidel", "192.168.3.34:2379", "name", "0"),
			types.MakeCausets("fidel", "192.168.3.35:2379", "name", "1"),
			types.MakeCausets("fidel", "192.168.3.34:2379", "peer-urls", "0"),
			types.MakeCausets("fidel", "192.168.3.35:2379", "peer-urls", "1"),
		},
	}
	// mock version inconsistent
	mockData[schemareplicant.BlockClusterInfo] = variable.BlockSnapshot{
		Events: [][]types.Causet{
			types.MakeCausets("milevadb", "192.168.1.11:1234", "192.168.1.11:1234", "4.0", "a234c"),
			types.MakeCausets("milevadb", "192.168.1.12:1234", "192.168.1.11:1234", "4.0", "a234d"),
			types.MakeCausets("milevadb", "192.168.1.13:1234", "192.168.1.11:1234", "4.0", "a234e"),
			types.MakeCausets("einsteindb", "192.168.1.21:1234", "192.168.1.21:1234", "4.0", "c234d"),
			types.MakeCausets("einsteindb", "192.168.1.22:1234", "192.168.1.22:1234", "4.0", "c234d"),
			types.MakeCausets("einsteindb", "192.168.1.23:1234", "192.168.1.23:1234", "4.0", "c234e"),
			types.MakeCausets("fidel", "192.168.1.31:1234", "192.168.1.31:1234", "4.0", "m234c"),
			types.MakeCausets("fidel", "192.168.1.32:1234", "192.168.1.32:1234", "4.0", "m234d"),
			types.MakeCausets("fidel", "192.168.1.33:1234", "192.168.1.33:1234", "4.0", "m234e"),
		},
	}
	mockData[schemareplicant.BlockClusterHardware] = variable.BlockSnapshot{
		Events: [][]types.Causet{
			types.MakeCausets("einsteindb", "192.168.1.22:1234", "disk", "sda", "used-percent", "80"),
			types.MakeCausets("einsteindb", "192.168.1.23:1234", "disk", "sdb", "used-percent", "50"),
			types.MakeCausets("fidel", "192.168.1.31:1234", "cpu", "cpu", "cpu-logical-cores", "1"),
			types.MakeCausets("fidel", "192.168.1.32:1234", "cpu", "cpu", "cpu-logical-cores", "4"),
			types.MakeCausets("fidel", "192.168.1.33:1234", "cpu", "cpu", "cpu-logical-cores", "10"),
		},
	}

	datetime := func(str string) types.Time {
		return s.parseTime(c, tk.Se, str)
	}
	// construct some mock abnormal data
	mockMetric := map[string][][]types.Causet{
		"node_total_memory": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "192.168.3.33:26600", 50.0*1024*1024*1024),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "192.168.3.34:26600", 50.0*1024*1024*1024),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "192.168.3.35:26600", 50.0*1024*1024*1024),
		},
	}

	ctx := s.setupForInspection(c, mockMetric, mockData)
	defer s.tearDownForInspection(c)

	cases := []struct {
		allegrosql string
		rows       []string
	}{
		{
			allegrosql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule in ('config', 'version')",
			rows: []string{
				"config interlock.high einsteindb inconsistent consistent warning 192.168.3.32:26600,192.168.3.33:26600 config value is 8\n192.168.3.34:26600,192.168.3.35:26600 config value is 7",
				"config dbs.lease milevadb inconsistent consistent warning 192.168.3.22:4000,192.168.3.24:4000,192.168.3.25:4000 config value is 1\n192.168.3.23:4000 config value is 2",
				"config log.slow-threshold milevadb 0 not 0 warning slow-threshold = 0 will record every query to slow log, it may affect performance",
				"config log.slow-threshold milevadb inconsistent consistent warning 192.168.3.24:4000 config value is 0\n192.168.3.25:4000 config value is 1",
				"config raftstore.sync-log einsteindb false not false warning sync-log should be true to avoid recover region when the machine breaks down",
				"version git_hash fidel inconsistent consistent critical the cluster has 3 different fidel versions, execute the allegrosql to see more detail: select * from information_schema.cluster_info where type='fidel'",
				"version git_hash milevadb inconsistent consistent critical the cluster has 3 different milevadb versions, execute the allegrosql to see more detail: select * from information_schema.cluster_info where type='milevadb'",
				"version git_hash einsteindb inconsistent consistent critical the cluster has 2 different einsteindb versions, execute the allegrosql to see more detail: select * from information_schema.cluster_info where type='einsteindb'",
			},
		},
		{
			allegrosql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule in ('config', 'version') and item in ('interlock.high', 'git_hash') and type='einsteindb'",
			rows: []string{
				"config interlock.high einsteindb inconsistent consistent warning 192.168.3.32:26600,192.168.3.33:26600 config value is 8\n192.168.3.34:26600,192.168.3.35:26600 config value is 7",
				"version git_hash einsteindb inconsistent consistent critical the cluster has 2 different einsteindb versions, execute the allegrosql to see more detail: select * from information_schema.cluster_info where type='einsteindb'",
			},
		},
		{
			allegrosql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule='config'",
			rows: []string{
				"config interlock.high einsteindb inconsistent consistent warning 192.168.3.32:26600,192.168.3.33:26600 config value is 8\n192.168.3.34:26600,192.168.3.35:26600 config value is 7",
				"config dbs.lease milevadb inconsistent consistent warning 192.168.3.22:4000,192.168.3.24:4000,192.168.3.25:4000 config value is 1\n192.168.3.23:4000 config value is 2",
				"config log.slow-threshold milevadb 0 not 0 warning slow-threshold = 0 will record every query to slow log, it may affect performance",
				"config log.slow-threshold milevadb inconsistent consistent warning 192.168.3.24:4000 config value is 0\n192.168.3.25:4000 config value is 1",
				"config raftstore.sync-log einsteindb false not false warning sync-log should be true to avoid recover region when the machine breaks down",
			},
		},
		{
			allegrosql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule='version' and item='git_hash' and type in ('fidel', 'milevadb')",
			rows: []string{
				"version git_hash fidel inconsistent consistent critical the cluster has 3 different fidel versions, execute the allegrosql to see more detail: select * from information_schema.cluster_info where type='fidel'",
				"version git_hash milevadb inconsistent consistent critical the cluster has 3 different milevadb versions, execute the allegrosql to see more detail: select * from information_schema.cluster_info where type='milevadb'",
			},
		},
	}

	for _, cs := range cases {
		rs, err := tk.Se.Execute(ctx, cs.allegrosql)
		c.Assert(err, IsNil)
		result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("ALLEGROALLEGROSQL: %v", cs.allegrosql))
		warnings := tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, 0, Commentf("expected no warning, got: %+v", warnings))
		result.Check(testkit.Events(cs.rows...))
	}
}

func (s *inspectionResultSuite) parseTime(c *C, se stochastik.Stochastik, str string) types.Time {
	t, err := types.ParseTime(se.GetStochaseinstein_dbars().StmtCtx, str, allegrosql.TypeDatetime, types.MaxFsp)
	c.Assert(err, IsNil)
	return t
}

func (s *inspectionResultSuite) tearDownForInspection(c *C) {
	fpName := "github.com/whtcorpsinc/MilevaDB-Prod/executor/mockMergeMockInspectionBlocks"
	c.Assert(failpoint.Disable(fpName), IsNil)

	fpName2 := "github.com/whtcorpsinc/MilevaDB-Prod/executor/mockMetricsBlockData"
	c.Assert(failpoint.Disable(fpName2), IsNil)
}

func (s *inspectionResultSuite) setupForInspection(c *C, mockData map[string][][]types.Causet, configurations map[string]variable.BlockSnapshot) context.Context {
	// mock einsteindb configuration.
	if configurations == nil {
		configurations = map[string]variable.BlockSnapshot{}
		configurations[schemareplicant.BlockClusterConfig] = variable.BlockSnapshot{
			Events: [][]types.Causet{
				types.MakeCausets("einsteindb", "einsteindb-0", "raftstore.apply-pool-size", "2"),
				types.MakeCausets("einsteindb", "einsteindb-0", "raftstore.causetstore-pool-size", "2"),
				types.MakeCausets("einsteindb", "einsteindb-0", "readpool.interlock.high-concurrency", "4"),
				types.MakeCausets("einsteindb", "einsteindb-0", "readpool.interlock.low-concurrency", "4"),
				types.MakeCausets("einsteindb", "einsteindb-0", "readpool.interlock.normal-concurrency", "4"),
				types.MakeCausets("einsteindb", "einsteindb-1", "readpool.interlock.normal-concurrency", "8"),
				types.MakeCausets("einsteindb", "einsteindb-0", "readpool.storage.high-concurrency", "4"),
				types.MakeCausets("einsteindb", "einsteindb-0", "readpool.storage.low-concurrency", "4"),
				types.MakeCausets("einsteindb", "einsteindb-0", "readpool.storage.normal-concurrency", "4"),
				types.MakeCausets("einsteindb", "einsteindb-0", "server.grpc-concurrency", "8"),
				types.MakeCausets("einsteindb", "einsteindb-0", "storage.scheduler-worker-pool-size", "6"),
			},
		}
		// mock cluster information
		configurations[schemareplicant.BlockClusterInfo] = variable.BlockSnapshot{
			Events: [][]types.Causet{
				types.MakeCausets("fidel", "fidel-0", "fidel-0", "4.0", "a234c", "", ""),
				types.MakeCausets("milevadb", "milevadb-0", "milevadb-0s", "4.0", "a234c", "", ""),
				types.MakeCausets("milevadb", "milevadb-1", "milevadb-1s", "4.0", "a234c", "", ""),
				types.MakeCausets("einsteindb", "einsteindb-0", "einsteindb-0s", "4.0", "a234c", "", ""),
				types.MakeCausets("einsteindb", "einsteindb-1", "einsteindb-1s", "4.0", "a234c", "", ""),
				types.MakeCausets("einsteindb", "einsteindb-2", "einsteindb-2s", "4.0", "a234c", "", ""),
			},
		}
	}
	fpName := "github.com/whtcorpsinc/MilevaDB-Prod/executor/mockMergeMockInspectionBlocks"
	c.Assert(failpoint.Enable(fpName, "return"), IsNil)

	// Mock for metric block data.
	fpName2 := "github.com/whtcorpsinc/MilevaDB-Prod/executor/mockMetricsBlockData"
	c.Assert(failpoint.Enable(fpName2, "return"), IsNil)

	ctx := context.WithValue(context.Background(), "__mockInspectionBlocks", configurations)
	ctx = context.WithValue(ctx, "__mockMetricsBlockData", mockData)
	ctx = failpoint.WithHook(ctx, func(_ context.Context, currName string) bool {
		return fpName2 == currName || currName == fpName
	})
	return ctx
}

func (s *inspectionResultSuite) TestThresholdCheckInspection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	datetime := func(str string) types.Time {
		return s.parseTime(c, tk.Se, str)
	}
	// construct some mock abnormal data
	mockData := map[string][][]types.Causet{
		// defCausumns: time, instance, name, value
		"einsteindb_thread_cpu": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "INTERLOCK_normal0", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "INTERLOCK_normal1", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-1s", "INTERLOCK_normal0", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "INTERLOCK_high1", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "INTERLOCK_high2", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "einsteindb-0s", "INTERLOCK_high1", 5.0),
			types.MakeCausets(datetime("2020-02-14 05:22:00"), "einsteindb-0s", "INTERLOCK_high1", 1.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "INTERLOCK_low1", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "grpc_1", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "raftstore_1", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "apply_0", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "store_read_norm1", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "store_read_high2", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "store_read_low0", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "sched_2", 10.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "split_check", 10.0),
		},
		"FIDel_tso_wait_duration":                   {},
		"milevadb_get_token_duration":               {},
		"milevadb_load_schema_duration":             {},
		"einsteindb_scheduler_command_duration":     {},
		"einsteindb_handle_snapshot_duration":       {},
		"einsteindb_storage_async_request_duration": {},
		"einsteindb_engine_write_duration":          {},
		"einsteindb_engine_max_get_duration":        {},
		"einsteindb_engine_max_seek_duration":       {},
		"einsteindb_scheduler_pending_commands":     {},
		"einsteindb_block_index_cache_hit":          {},
		"einsteindb_block_data_cache_hit":           {},
		"einsteindb_block_filter_cache_hit":         {},
		"FIDel_scheduler_store_status":              {},
		"FIDel_region_health":                       {},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, "select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, type, instance,status_address, value, reference, details from information_schema.inspection_result where rule='threshold-check' order by item")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect ALLEGROALLEGROSQL failed"))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()))
	result.Check(testkit.Events(
		"apply-cpu einsteindb einsteindb-0 einsteindb-0s 10.00 < 1.60, config: raftstore.apply-pool-size=2 the 'apply-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"interlock-high-cpu einsteindb einsteindb-0 einsteindb-0s 20.00 < 3.60, config: readpool.interlock.high-concurrency=4 the 'interlock-high-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"interlock-low-cpu einsteindb einsteindb-0 einsteindb-0s 10.00 < 3.60, config: readpool.interlock.low-concurrency=4 the 'interlock-low-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"interlock-normal-cpu einsteindb einsteindb-0 einsteindb-0s 20.00 < 3.60, config: readpool.interlock.normal-concurrency=4 the 'interlock-normal-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"interlock-normal-cpu einsteindb einsteindb-1 einsteindb-1s 10.00 < 7.20, config: readpool.interlock.normal-concurrency=8 the 'interlock-normal-cpu' max cpu-usage of einsteindb-1s einsteindb is too high",
		"grpc-cpu einsteindb einsteindb-0 einsteindb-0s 10.00 < 7.20, config: server.grpc-concurrency=8 the 'grpc-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"raftstore-cpu einsteindb einsteindb-0 einsteindb-0s 10.00 < 1.60, config: raftstore.causetstore-pool-size=2 the 'raftstore-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"scheduler-worker-cpu einsteindb einsteindb-0 einsteindb-0s 10.00 < 5.10, config: storage.scheduler-worker-pool-size=6 the 'scheduler-worker-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"split-check-cpu einsteindb einsteindb-0 einsteindb-0s 10.00 < 0.00 the 'split-check-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"storage-readpool-high-cpu einsteindb einsteindb-0 einsteindb-0s 10.00 < 3.60, config: readpool.storage.high-concurrency=4 the 'storage-readpool-high-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"storage-readpool-low-cpu einsteindb einsteindb-0 einsteindb-0s 10.00 < 3.60, config: readpool.storage.low-concurrency=4 the 'storage-readpool-low-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
		"storage-readpool-normal-cpu einsteindb einsteindb-0 einsteindb-0s 10.00 < 3.60, config: readpool.storage.normal-concurrency=4 the 'storage-readpool-normal-cpu' max cpu-usage of einsteindb-0s einsteindb is too high",
	))

	// construct some mock normal data
	mockData["einsteindb_thread_cpu"] = [][]types.Causet{
		// defCausumns: time, instance, name, value
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "INTERLOCK_normal0", 1.0),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "INTERLOCK_high1", 0.1),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "INTERLOCK_low1", 1.0),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "grpc_1", 7.21),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "grpc_2", 0.21),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "raftstore_1", 1.0),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "apply_0", 1.0),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "store_read_norm1", 1.0),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "store_read_high2", 1.0),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "store_read_low0", 1.0),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "sched_2", 0.3),
		types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "split_check", 0.5),
	}

	ctx = context.WithValue(ctx, "__mockMetricsBlockData", mockData)
	rs, err = tk.Se.Execute(ctx, "select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, type, instance,status_address, value, reference from information_schema.inspection_result where rule='threshold-check' order by item")
	c.Assert(err, IsNil)
	result = tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect ALLEGROALLEGROSQL failed"))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()))
	result.Check(testkit.Events("grpc-cpu einsteindb einsteindb-0 einsteindb-0s 7.42 < 7.20, config: server.grpc-concurrency=8"))
}

func (s *inspectionResultSuite) TestThresholdCheckInspection2(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetStochaseinstein_dbars().StmtCtx, s, allegrosql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	// construct some mock abnormal data
	mockData := map[string][][]types.Causet{
		"FIDel_tso_wait_duration": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", 0.999, 0.06),
		},
		"milevadb_get_token_duration": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "milevadb-0s", 0.999, 0.02*10e5),
		},
		"milevadb_load_schema_duration": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "milevadb-0s", 0.99, 2.0),
		},
		"einsteindb_scheduler_command_duration": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "get", 0.99, 2.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "write", 0.99, 5.0),
		},
		"einsteindb_handle_snapshot_duration": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "gen", 0.999, 40.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "read", 0.999, 10.0),
		},
		"einsteindb_storage_async_request_duration": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "write", 0.999, 0.2),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "snapshot", 0.999, 0.06),
		},
		"einsteindb_engine_write_duration": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "write_max", "ekv", 0.2*10e5),
		},
		"einsteindb_engine_max_get_duration": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "get_max", "ekv", 0.06*10e5),
		},
		"einsteindb_engine_max_seek_duration": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "seek_max", "raft", 0.06*10e5),
		},
		"einsteindb_scheduler_pending_commands": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", 1001.0),
		},
		"einsteindb_block_index_cache_hit": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "ekv", 0.94),
		},
		"einsteindb_block_data_cache_hit": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "ekv", 0.79),
		},
		"einsteindb_block_filter_cache_hit": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "einsteindb-0s", "ekv", 0.93),
		},
		"einsteindb_thread_cpu":        {},
		"FIDel_scheduler_store_status": {},
		"FIDel_region_health":          {},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, "select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, type, instance, status_address, value, reference, details from information_schema.inspection_result where rule='threshold-check' order by item")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect ALLEGROALLEGROSQL failed"))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()))
	result.Check(testkit.Events(
		"data-block-cache-hit einsteindb einsteindb-0 einsteindb-0s 0.790 > 0.800 min data-block-cache-hit rate of einsteindb-0s einsteindb is too low",
		"filter-block-cache-hit einsteindb einsteindb-0 einsteindb-0s 0.930 > 0.950 min filter-block-cache-hit rate of einsteindb-0s einsteindb is too low",
		"get-token-duration milevadb milevadb-0 milevadb-0s 0.020 < 0.001 max duration of milevadb-0s milevadb get-token-duration is too slow",
		"handle-snapshot-duration einsteindb einsteindb-0 einsteindb-0s 40.000 < 30.000 max duration of einsteindb-0s einsteindb handle-snapshot-duration is too slow",
		"index-block-cache-hit einsteindb einsteindb-0 einsteindb-0s 0.940 > 0.950 min index-block-cache-hit rate of einsteindb-0s einsteindb is too low",
		"load-schemaReplicant-duration milevadb milevadb-0 milevadb-0s 2.000 < 1.000 max duration of milevadb-0s milevadb load-schemaReplicant-duration is too slow",
		"lmdb-get-duration einsteindb einsteindb-0 einsteindb-0s 0.060 < 0.050 max duration of einsteindb-0s einsteindb lmdb-get-duration is too slow",
		"lmdb-seek-duration einsteindb einsteindb-0 einsteindb-0s 0.060 < 0.050 max duration of einsteindb-0s einsteindb lmdb-seek-duration is too slow",
		"lmdb-write-duration einsteindb einsteindb-0 einsteindb-0s 0.200 < 0.100 max duration of einsteindb-0s einsteindb lmdb-write-duration is too slow",
		"scheduler-cmd-duration einsteindb einsteindb-0 einsteindb-0s 5.000 < 0.100 max duration of einsteindb-0s einsteindb scheduler-cmd-duration is too slow",
		"scheduler-pending-cmd-count einsteindb einsteindb-0 einsteindb-0s 1001.000 < 1000.000  einsteindb-0s einsteindb scheduler has too many pending commands",
		"storage-snapshot-duration einsteindb einsteindb-0 einsteindb-0s 0.060 < 0.050 max duration of einsteindb-0s einsteindb storage-snapshot-duration is too slow",
		"storage-write-duration einsteindb einsteindb-0 einsteindb-0s 0.200 < 0.100 max duration of einsteindb-0s einsteindb storage-write-duration is too slow",
		"tso-duration milevadb fidel-0 fidel-0 0.060 < 0.050 max duration of fidel-0 milevadb tso-duration is too slow",
	))
}

func (s *inspectionResultSuite) TestThresholdCheckInspection3(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetStochaseinstein_dbars().StmtCtx, s, allegrosql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	// construct some mock abnormal data
	mockData := map[string][][]types.Causet{
		"FIDel_scheduler_store_status": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-0", "0", "leader_score", 100.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-1", "1", "leader_score", 50.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "fidel-0", "einsteindb-0", "0", "leader_score", 99.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "fidel-0", "einsteindb-1", "1", "leader_score", 51.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-0", "0", "region_score", 100.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-1", "1", "region_score", 90.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-0", "0", "store_available", 100.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-1", "1", "store_available", 70.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-0", "0", "region_count", 20001.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-0", "0", "leader_count", 10000.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "fidel-0", "einsteindb-0", "0", "leader_count", 5000.0),
			types.MakeCausets(datetime("2020-02-14 05:22:00"), "fidel-0", "einsteindb-0", "0", "leader_count", 5000.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-1", "0", "leader_count", 5000.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "fidel-0", "einsteindb-1", "0", "leader_count", 10000.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "einsteindb-2", "0", "leader_count", 10000.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "fidel-0", "einsteindb-2", "0", "leader_count", 0.0),
		},
		"FIDel_region_health": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "extra-peer-region-count", 40.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "learner-peer-region-count", 40.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "fidel-0", "pending-peer-region-count", 30.0),
		},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, `select /*+ time_range('2020-02-14 04:20:00','2020-02-14 05:23:00') */
		item, type, instance,status_address, value, reference, details from information_schema.inspection_result
		where rule='threshold-check' and item in ('leader-score-balance','region-score-balance','region-count','region-health','causetstore-available-balance','leader-drop')
		order by item`)
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect ALLEGROALLEGROSQL failed"))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()))
	result.Check(testkit.Events(
		"leader-drop einsteindb einsteindb-2 einsteindb-2s 10000 <= 50 einsteindb-2 einsteindb has too many leader-drop around time 2020-02-14 05:21:00.000000, leader count from 10000 drop to 0",
		"leader-drop einsteindb einsteindb-0 einsteindb-0s 5000 <= 50 einsteindb-0 einsteindb has too many leader-drop around time 2020-02-14 05:21:00.000000, leader count from 10000 drop to 5000",
		"leader-score-balance einsteindb einsteindb-1 einsteindb-1s 50.00% < 5.00% einsteindb-0 max leader_score is 100.00, much more than einsteindb-1 min leader_score 50.00",
		"region-count einsteindb einsteindb-0 einsteindb-0s 20001.00 <= 20000 einsteindb-0 einsteindb has too many regions",
		"region-health fidel fidel-0 fidel-0 110.00 < 100 the count of extra-perr and learner-peer and pending-peer are 110, it means the scheduling is too frequent or too slow",
		"region-score-balance einsteindb einsteindb-1 einsteindb-1s 10.00% < 5.00% einsteindb-0 max region_score is 100.00, much more than einsteindb-1 min region_score 90.00",
		"causetstore-available-balance einsteindb einsteindb-1 einsteindb-1s 30.00% < 20.00% einsteindb-0 max store_available is 100.00, much more than einsteindb-1 min store_available 70.00"))
}

func (s *inspectionResultSuite) TestCriticalErrorInspection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	testServers := s.setupClusterGRPCServer(c)
	defer func() {
		for _, s := range testServers {
			s.server.Stop()
		}
	}()

	var servers []string
	for _, s := range testServers {
		servers = append(servers, strings.Join([]string{s.typ, s.address, s.address}, ","))
	}
	fpName2 := "github.com/whtcorpsinc/MilevaDB-Prod/executor/mockClusterLogServerInfo"
	fpExpr := strings.Join(servers, ";")
	c.Assert(failpoint.Enable(fpName2, fmt.Sprintf(`return("%s")`, fpExpr)), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName2), IsNil) }()

	datetime := func(str string) types.Time {
		return s.parseTime(c, tk.Se, str)
	}

	// construct some mock data
	mockData := map[string][][]types.Causet{
		// defCausumns: time, instance, type, value
		"einsteindb_critical_error_total_count": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "einsteindb-0s", "type1", 0.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "einsteindb-1s", "type1", 1.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "einsteindb-2s", "type2", 5.0),
		},
		// defCausumns: time, instance, value
		"milevadb_panic_count_total_count": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "milevadb-0s", 4.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "milevadb-0s", 0.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "milevadb-1s", 1.0),
		},
		// defCausumns: time, instance, value
		"milevadb_binlog_error_total_count": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "milevadb-1s", 4.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "milevadb-2s", 0.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "milevadb-3s", 1.0),
		},
		// defCausumns: time, instance, EDB, type, stage, value
		"einsteindb_scheduler_is_busy_total_count": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "einsteindb-0s", "db1", "type1", "stage1", 1.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "einsteindb-0s", "db2", "type1", "stage2", 2.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "einsteindb-1s", "db1", "type2", "stage1", 3.0),
			types.MakeCausets(datetime("2020-02-12 10:38:00"), "einsteindb-0s", "db1", "type1", "stage2", 4.0),
			types.MakeCausets(datetime("2020-02-12 10:39:00"), "einsteindb-0s", "db2", "type1", "stage1", 5.0),
			types.MakeCausets(datetime("2020-02-12 10:40:00"), "einsteindb-1s", "db1", "type2", "stage2", 6.0),
		},
		// defCausumns: time, instance, EDB, value
		"einsteindb_interlocking_directorate_is_busy_total_count": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "einsteindb-0s", "db1", 1.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "einsteindb-0s", "db2", 2.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "einsteindb-1s", "db1", 3.0),
			types.MakeCausets(datetime("2020-02-12 10:38:00"), "einsteindb-0s", "db1", 4.0),
			types.MakeCausets(datetime("2020-02-12 10:39:00"), "einsteindb-0s", "db2", 5.0),
			types.MakeCausets(datetime("2020-02-12 10:40:00"), "einsteindb-1s", "db1", 6.0),
		},
		// defCausumns: time, instance, EDB, type, value
		"einsteindb_channel_full_total_count": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "einsteindb-0s", "db1", "type1", 1.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "einsteindb-0s", "db2", "type1", 2.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "einsteindb-1s", "db1", "type2", 3.0),
			types.MakeCausets(datetime("2020-02-12 10:38:00"), "einsteindb-0s", "db1", "type1", 4.0),
			types.MakeCausets(datetime("2020-02-12 10:39:00"), "einsteindb-0s", "db2", "type1", 5.0),
			types.MakeCausets(datetime("2020-02-12 10:40:00"), "einsteindb-1s", "db1", "type2", 6.0),
		},
		// defCausumns: time, instance, EDB, value
		"einsteindb_engine_write_stall": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "einsteindb-0s", "ekv", 1.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "einsteindb-0s", "raft", 2.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "einsteindb-1s", "reason3", 3.0),
		},
		// defCausumns: time, instance, job, value
		"up": {
			types.MakeCausets(datetime("2020-02-12 10:35:00"), "einsteindb-0s", "einsteindb", 1.0),
			types.MakeCausets(datetime("2020-02-12 10:36:00"), "einsteindb-0s", "einsteindb", 0.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "milevadb-0s", "milevadb", 0.0),
			types.MakeCausets(datetime("2020-02-12 10:37:00"), "milevadb-1s", "milevadb", 0.0),
			types.MakeCausets(datetime("2020-02-12 10:38:00"), "milevadb-1s", "milevadb", 1.0),
		},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, "select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, instance,status_address, value, details from information_schema.inspection_result where rule='critical-error'")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect ALLEGROALLEGROSQL failed"))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()))
	result.Check(testkit.Events(
		"server-down einsteindb-0 einsteindb-0s  einsteindb einsteindb-0s disconnect with prometheus around time '2020-02-12 10:36:00.000000'",
		"server-down milevadb-1 milevadb-1s  milevadb milevadb-1s disconnect with prometheus around time '2020-02-12 10:37:00.000000'",
		"channel-is-full einsteindb-1 einsteindb-1s 9.00(db1, type2) the total number of errors about 'channel-is-full' is too many",
		"interlock-is-busy einsteindb-1 einsteindb-1s 9.00(db1) the total number of errors about 'interlock-is-busy' is too many",
		"channel-is-full einsteindb-0 einsteindb-0s 7.00(db2, type1) the total number of errors about 'channel-is-full' is too many",
		"interlock-is-busy einsteindb-0 einsteindb-0s 7.00(db2) the total number of errors about 'interlock-is-busy' is too many",
		"scheduler-is-busy einsteindb-1 einsteindb-1s 6.00(db1, type2, stage2) the total number of errors about 'scheduler-is-busy' is too many",
		"channel-is-full einsteindb-0 einsteindb-0s 5.00(db1, type1) the total number of errors about 'channel-is-full' is too many",
		"interlock-is-busy einsteindb-0 einsteindb-0s 5.00(db1) the total number of errors about 'interlock-is-busy' is too many",
		"critical-error einsteindb-2 einsteindb-2s 5.00(type2) the total number of errors about 'critical-error' is too many",
		"scheduler-is-busy einsteindb-0 einsteindb-0s 5.00(db2, type1, stage1) the total number of errors about 'scheduler-is-busy' is too many",
		"binlog-error milevadb-1 milevadb-1s 4.00 the total number of errors about 'binlog-error' is too many",
		"panic-count milevadb-0 milevadb-0s 4.00 the total number of errors about 'panic-count' is too many",
		"scheduler-is-busy einsteindb-0 einsteindb-0s 4.00(db1, type1, stage2) the total number of errors about 'scheduler-is-busy' is too many",
		"scheduler-is-busy einsteindb-1 einsteindb-1s 3.00(db1, type2, stage1) the total number of errors about 'scheduler-is-busy' is too many",
		"einsteindb_engine_write_stall einsteindb-1 einsteindb-1s 3.00(reason3) the total number of errors about 'einsteindb_engine_write_stall' is too many",
		"scheduler-is-busy einsteindb-0 einsteindb-0s 2.00(db2, type1, stage2) the total number of errors about 'scheduler-is-busy' is too many",
		"einsteindb_engine_write_stall einsteindb-0 einsteindb-0s 2.00(raft) the total number of errors about 'einsteindb_engine_write_stall' is too many",
		"binlog-error  milevadb-3s 1.00 the total number of errors about 'binlog-error' is too many",
		"critical-error einsteindb-1 einsteindb-1s 1.00(type1) the total number of errors about 'critical-error' is too many",
		"panic-count milevadb-1 milevadb-1s 1.00 the total number of errors about 'panic-count' is too many",
		"scheduler-is-busy einsteindb-0 einsteindb-0s 1.00(db1, type1, stage1) the total number of errors about 'scheduler-is-busy' is too many",
		"einsteindb_engine_write_stall einsteindb-0 einsteindb-0s 1.00(ekv) the total number of errors about 'einsteindb_engine_write_stall' is too many",
	))
}

func (s *inspectionResultSuite) TestNodeLoadInspection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetStochaseinstein_dbars().StmtCtx, s, allegrosql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	// construct some mock abnormal data
	mockData := map[string][][]types.Causet{
		// defCausumns: time, instance, value
		"node_load1": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "node-0", 28.1),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "node-1", 13.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-0", 10.0),
		},
		// defCausumns: time, instance, value
		"node_load5": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "node-0", 27.9),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "node-1", 14.1),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-0", 0.0),
		},
		// defCausumns: time, instance, value
		"node_load15": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "node-0", 30.0),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "node-1", 14.1),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-0", 20.0),
		},
		// defCausumns: time, instance, value
		"node_virtual_cpus": {
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-0", 40.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-1", 20.0),
		},
		// defCausumns: time, instance, value
		"node_memory_usage": {
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-0", 80.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-1", 60.0),
			types.MakeCausets(datetime("2020-02-14 05:22:00"), "node-0", 60.0),
		},
		// defCausumns: time, instance, value
		"node_memory_swap_used": {
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-0", 0.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-1", 1.0),
			types.MakeCausets(datetime("2020-02-14 05:22:00"), "node-1", 0.0),
		},
		// defCausumns: time, instance, device, value
		"node_disk_usage": {
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-0", "/dev/nvme0", 80.0),
			types.MakeCausets(datetime("2020-02-14 05:22:00"), "node-0", "/dev/nvme0", 50.0),
			types.MakeCausets(datetime("2020-02-14 05:21:00"), "node-0", "tmpfs", 80.0),
			types.MakeCausets(datetime("2020-02-14 05:22:00"), "node-0", "tmpfs", 50.0),
		},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, `select /*+ time_range('2020-02-14 04:20:00','2020-02-14 05:23:00') */
		item, type, instance, value, reference, details from information_schema.inspection_result
		where rule='node-load' order by item, value`)
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect ALLEGROALLEGROSQL failed"))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()))
	result.Check(testkit.Events(
		"cpu-load1 node node-0 28.1 < 28.0 cpu-load1 should less than (cpu_logical_cores * 0.7)",
		"cpu-load15 node node-1 14.1 < 14.0 cpu-load15 should less than (cpu_logical_cores * 0.7)",
		"cpu-load15 node node-0 30.0 < 28.0 cpu-load15 should less than (cpu_logical_cores * 0.7)",
		"cpu-load5 node node-1 14.1 < 14.0 cpu-load5 should less than (cpu_logical_cores * 0.7)",
		"disk-usage node node-0 80.0% < 70% the disk-usage of /dev/nvme0 is too high",
		"swap-memory-used node node-1 1.0 0 ",
		"virtual-memory-usage node node-0 80.0% < 70% the memory-usage is too high",
	))
}

func (s *inspectionResultSuite) TestConfigCheckOfStorageBlockCacheSize(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetStochaseinstein_dbars().StmtCtx, s, allegrosql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	configurations := map[string]variable.BlockSnapshot{}
	configurations[schemareplicant.BlockClusterConfig] = variable.BlockSnapshot{
		Events: [][]types.Causet{
			types.MakeCausets("einsteindb", "192.168.3.33:26600", "storage.block-cache.capacity", "10GiB"),
			types.MakeCausets("einsteindb", "192.168.3.33:26700", "storage.block-cache.capacity", "20GiB"),
			types.MakeCausets("einsteindb", "192.168.3.34:26600", "storage.block-cache.capacity", "1TiB"),
			types.MakeCausets("einsteindb", "192.168.3.35:26700", "storage.block-cache.capacity", "20GiB"),
		},
	}

	// construct some mock abnormal data
	mockData := map[string][][]types.Causet{
		"node_total_memory": {
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "192.168.3.33:26600", 50.0*1024*1024*1024),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "192.168.3.34:26600", 50.0*1024*1024*1024),
			types.MakeCausets(datetime("2020-02-14 05:20:00"), "192.168.3.35:26600", 50.0*1024*1024*1024),
		},
	}

	ctx := s.setupForInspection(c, mockData, configurations)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, "select  /*+ time_range('2020-02-14 04:20:00','2020-02-14 05:23:00') */ * from information_schema.inspection_result where rule='config' and item='storage.block-cache.capacity' order by value")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect ALLEGROALLEGROSQL failed"))
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetStochaseinstein_dbars().StmtCtx.GetWarnings()))
	result.Check(testkit.Events(
		"config storage.block-cache.capacity einsteindb 192.168.3.34  1099511627776 < 24159191040 warning There are 1 EinsteinDB server in 192.168.3.34 node, the total 'storage.block-cache.capacity' of EinsteinDB is more than (0.45 * total node memory)",
		"config storage.block-cache.capacity einsteindb 192.168.3.33  32212254720 < 24159191040 warning There are 2 EinsteinDB server in 192.168.3.33 node, the total 'storage.block-cache.capacity' of EinsteinDB is more than (0.45 * total node memory)",
	))
}
