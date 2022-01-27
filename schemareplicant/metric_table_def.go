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

package schemareplicant

// MetricBlockMap records the metric causet definition, export for test.
// TODO: read from system causet.
var MetricBlockMap = map[string]MetricBlockDef{
	"milevadb_query_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(milevadb_server_handle_query_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,sql_type,instance))`,
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.90,
		Comment:  "The quantile of MilevaDB query durations(second)",
	},
	"milevadb_qps": {
		PromQL:  `sum(rate(milevadb_server_query_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (result,type,instance)`,
		Labels:  []string{"instance", "type", "result"},
		Comment: "MilevaDB query processing numbers per second",
	},
	"milevadb_qps_ideal": {
		PromQL: `sum(milevadb_server_connections) * sum(rate(milevadb_server_handle_query_duration_seconds_count[$RANGE_DURATION])) / sum(rate(milevadb_server_handle_query_duration_seconds_sum[$RANGE_DURATION]))`,
	},
	"milevadb_ops_memex": {
		PromQL:  `sum(rate(milevadb_interlock_memex_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "MilevaDB memex statistics",
	},
	"milevadb_failed_query_opm": {
		PromQL:  `sum(increase(milevadb_server_execute_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type, instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "MilevaDB failed query opm",
	},
	"milevadb_slow_query_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_server_slow_query_process_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.90,
		Comment:  "The quantile of MilevaDB slow query statistics with slow query time(second)",
	},
	"milevadb_slow_query_cop_process_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_server_slow_query_cop_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.90,
		Comment:  "The quantile of MilevaDB slow query statistics with slow query total cop process time(second)",
	},
	"milevadb_slow_query_cop_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_server_slow_query_wait_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.90,
		Comment:  "The quantile of MilevaDB slow query statistics with slow query total cop wait time(second)",
	},
	"milevadb_ops_internal": {
		PromQL:  "sum(rate(milevadb_stochastik_restricted_sql_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "MilevaDB internal ALLEGROALLEGROSQL is used by MilevaDB itself.",
	},
	"milevadb_process_mem_usage": {
		PromQL:  "process_resident_memory_bytes{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "process rss memory usage",
	},
	"go_heap_mem_usage": {
		PromQL:  "go_memstats_heap_alloc_bytes{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "MilevaDB heap memory size in use",
	},
	"process_cpu_usage": {
		PromQL: "rate(process_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels: []string{"instance", "job"},
	},
	"milevadb_connection_count": {
		PromQL:  "milevadb_server_connections{$LABEL_CONDITIONS}",
		Labels:  []string{"instance"},
		Comment: "MilevaDB current connection counts",
	},
	"node_process_open_fd_count": {
		PromQL:  "process_open_fds{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "Process opened file descriptors count",
	},
	"goroutines_count": {
		PromQL:  " go_goroutines{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "Process current goroutines count)",
	},
	"go_gc_duration": {
		PromQL:  "rate(go_gc_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "job"},
		Comment: "Go garbage defCauslection STW pause duration(second)",
	},
	"go_threads": {
		PromQL:  "go_threads{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "Total threads MilevaDB/FIDel process created currently",
	},
	"go_gc_count": {
		PromQL:  " rate(go_gc_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "job"},
		Comment: "The Go garbage defCauslection counts per second",
	},
	"go_gc_cpu_usage": {
		PromQL:  "go_memstats_gc_cpu_fraction{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "job"},
		Comment: "The fraction of MilevaDB/FIDel available CPU time used by the GC since the program started.",
	},
	"milevadb_event_opm": {
		PromQL:  "increase(milevadb_server_event_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "type"},
		Comment: "MilevaDB Server critical events total, including start/close/shutdown/hang etc",
	},
	"milevadb_keep_alive_opm": {
		PromQL:  "sum(increase(milevadb_monitor_keep_alive_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "MilevaDB instance monitor average keep alive times",
	},
	"milevadb_prepared_memex_count": {
		PromQL:  "milevadb_server_prepared_stmts{$LABEL_CONDITIONS}",
		Labels:  []string{"instance"},
		Comment: "MilevaDB prepare memexs count",
	},
	"milevadb_time_jump_back_ops": {
		PromQL:  "sum(increase(milevadb_monitor_time_jump_back_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "MilevaDB monitor time jump back count",
	},
	"milevadb_panic_count": {
		Comment: "MilevaDB instance panic count",
		PromQL:  "increase(milevadb_server_panic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance"},
	},
	"milevadb_panic_count_total_count": {
		Comment: "The total count of MilevaDB instance panic",
		PromQL:  "sum(increase(milevadb_server_panic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"milevadb_binlog_error_count": {
		Comment: "MilevaDB write binlog error, skip binlog count",
		PromQL:  "milevadb_server_critical_error_total{$LABEL_CONDITIONS}",
		Labels:  []string{"instance"},
	},
	"milevadb_binlog_error_total_count": {
		PromQL:  "sum(increase(milevadb_server_critical_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB write binlog error and skip binlog",
	},
	"milevadb_get_token_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_server_get_token_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  " The quantile of Duration (us) for getting token, it should be small until concurrency limit is reached(microsecond)",
	},
	"milevadb_handshake_error_opm": {
		PromQL:  "sum(increase(milevadb_server_handshake_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The OPM of MilevaDB processing handshake error",
	},
	"milevadb_handshake_error_total_count": {
		PromQL:  "sum(increase(milevadb_server_handshake_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB processing handshake error",
	},

	"milevadb_transaction_ops": {
		PromQL:  "sum(rate(milevadb_stochastik_transaction_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "MilevaDB transaction processing counts by type and source. Internal means MilevaDB inner transaction calls",
	},
	"milevadb_transaction_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_stochastik_transaction_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,sql_type,instance))",
		Labels:   []string{"instance", "type", "sql_type"},
		Quantile: 0.95,
		Comment:  "The quantile of transaction execution durations, including retry(second)",
	},
	"milevadb_transaction_retry_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_stochastik_retry_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Comment:  "The quantile of MilevaDB transaction retry num",
		Quantile: 0.95,
	},
	"milevadb_transaction_memex_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_stochastik_transaction_memex_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,sql_type))",
		Labels:   []string{"instance", "sql_type"},
		Comment:  "The quantile of MilevaDB memexs numbers within one transaction. Internal means MilevaDB inner transaction",
		Quantile: 0.95,
	},
	"milevadb_transaction_retry_error_ops": {
		PromQL:  "sum(rate(milevadb_stochastik_retry_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "Error numbers of transaction retry",
	},
	"milevadb_transaction_retry_error_total_count": {
		PromQL:  "sum(increase(milevadb_stochastik_retry_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,sql_type,instance)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "The total count of transaction retry",
	},
	"milevadb_transaction_local_latch_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_local_latch_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Comment:  "The quantile of MilevaDB transaction latch wait time on key value storage(second)",
		Quantile: 0.95,
	},
	"milevadb_parse_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_stochastik_parse_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,sql_type,instance))",
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.95,
		Comment:  "The quantile time cost of parsing ALLEGROALLEGROSQL to AST(second)",
	},
	"milevadb_compile_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_stochastik_compile_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, sql_type,instance))",
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.95,
		Comment:  "The quantile time cost of building the query plan(second)",
	},
	"milevadb_execute_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_stochastik_execute_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, sql_type, instance))",
		Labels:   []string{"instance", "sql_type"},
		Quantile: 0.95,
		Comment:  "The quantile time cost of executing the ALLEGROALLEGROSQL which does not include the time to get the results of the query(second)",
	},
	"milevadb_expensive_interlocks_ops": {
		Comment: "MilevaDB interlocks using more cpu and memory resources",
		PromQL:  "sum(rate(milevadb_interlock_expensive_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_query_using_plan_cache_ops": {
		PromQL:  "sum(rate(milevadb_server_plan_cache_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "MilevaDB plan cache hit ops",
	},
	"milevadb_allegrosql_execution_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_allegrosql_handle_query_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
		Comment:  "The quantile durations of allegrosql execution(second)",
	},
	"milevadb_allegrosql_qps": {
		PromQL:  "sum(rate(milevadb_allegrosql_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance", "type"},
		Comment: "allegrosql query handling durations per second",
	},
	"milevadb_allegrosql_partial_qps": {
		PromQL:  "sum(rate(milevadb_allegrosql_scan_keys_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
		Comment: "the numebr of allegrosql partial scan numbers",
	},
	"milevadb_allegrosql_scan_key_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_allegrosql_scan_keys_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "The quantile numebr of allegrosql scan numbers",
	},
	"milevadb_allegrosql_partial_scan_key_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_allegrosql_scan_keys_partial_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "The quantile numebr of allegrosql partial scan key numbers",
	},
	"milevadb_allegrosql_partial_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_allegrosql_partial_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "The quantile of allegrosql partial numbers per query",
	},
	"milevadb_cop_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_cop_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "The quantile of ekv storage interlock processing durations",
	},
	"milevadb_ekv_backoff_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_backoff_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
		Comment:  "The quantile of ekv backoff time durations(second)",
	},
	"milevadb_ekv_backoff_ops": {
		PromQL:  "sum(rate(milevadb_einsteindbclient_backoff_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "ekv storage backoff times",
	},
	"milevadb_ekv_region_error_ops": {
		PromQL:  "sum(rate(milevadb_einsteindbclient_region_err_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "ekv region error times",
	},
	"milevadb_ekv_region_error_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_region_err_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of ekv region error",
	},
	"milevadb_lock_resolver_ops": {
		PromQL:  "sum(rate(milevadb_einsteindbclient_lock_resolver_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "dagger resolve times",
	},
	"milevadb_lock_resolver_total_num": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_lock_resolver_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "The total number of dagger resolve",
	},
	"milevadb_lock_cleanup_fail_ops": {
		PromQL:  "sum(rate(milevadb_einsteindbclient_lock_cleanup_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "dagger cleanup failed ops",
	},
	"milevadb_load_safepoint_fail_ops": {
		PromQL:  "sum(rate(milevadb_einsteindbclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance", "type"},
		Comment: "safe point uFIDelate ops",
	},
	"milevadb_ekv_request_ops": {
		Comment: "ekv request total by instance and command type",
		PromQL:  "sum(rate(milevadb_einsteindbclient_request_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_ekv_request_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_request_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,causetstore,instance))",
		Labels:   []string{"instance", "type", "causetstore"},
		Quantile: 0.95,
		Comment:  "The quantile of ekv requests durations by causetstore",
	},
	"milevadb_ekv_txn_ops": {
		Comment: "MilevaDB total ekv transaction counts",
		PromQL:  "sum(rate(milevadb_einsteindbclient_txn_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"milevadb_ekv_write_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_txn_write_ekv_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 1,
		Comment:  "The quantile of ekv write count per transaction execution",
	},
	"milevadb_ekv_write_size": {
		Comment:  "The quantile of ekv write size per transaction execution",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_txn_write_size_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 1,
	},
	"milevadb_txn_region_num": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_txn_regions_num_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Comment:  "The quantile of regions transaction operates on count",
		Quantile: 0.95,
	},
	"milevadb_load_safepoint_ops": {
		PromQL:  "sum(rate(milevadb_einsteindbclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The OPS of load safe point loading",
	},
	"milevadb_load_safepoint_total_num": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_load_safepoint_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of safe point loading",
	},
	"milevadb_ekv_snapshot_ops": {
		Comment: "using snapshots total",
		PromQL:  "sum(rate(milevadb_einsteindbclient_snapshot_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"FIDel_client_cmd_ops": {
		PromQL:  "sum(rate(FIDel_client_cmd_handle_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "fidel client command ops",
	},
	"FIDel_client_cmd_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(FIDel_client_cmd_handle_cmds_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
		Comment:  "The quantile of fidel client command durations",
	},
	"FIDel_cmd_fail_ops": {
		PromQL:  "sum(rate(FIDel_client_cmd_handle_failed_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "fidel client command fail count",
	},
	"FIDel_cmd_fail_total_count": {
		PromQL:  "sum(increase(FIDel_client_cmd_handle_failed_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of fidel client command fail",
	},
	"FIDel_request_rpc_ops": {
		PromQL:  "sum(rate(FIDel_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance", "type"},
		Comment: "fidel client handle request operation per second",
	},
	"FIDel_request_rpc_duration": {
		Comment:  "The quantile of fidel client handle request duration(second)",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(FIDel_client_request_handle_requests_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.999,
	},
	"FIDel_tso_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(FIDel_client_cmd_handle_cmds_duration_seconds_bucket{type=\"wait\"}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.999,
		Comment:  "The quantile duration of a client starting to wait for the TS until received the TS result.",
	},
	"FIDel_tso_rpc_duration": {
		Comment:  "The quantile duration of a client sending TSO request until received the response.",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(FIDel_client_request_handle_requests_duration_seconds_bucket{type=\"tso\"}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.999,
	},
	"FIDel_start_tso_wait_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_FIDelclient_ts_future_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.999,
		Comment:  "The quantile duration of the waiting time for getting the start timestamp oracle",
	},
	"milevadb_load_schema_duration": {
		Comment:  "The quantile of MilevaDB loading schemaReplicant time durations by instance",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_petri_load_schema_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"milevadb_load_schema_ops": {
		Comment: "MilevaDB loading schemaReplicant times including both failed and successful ones",
		PromQL:  "sum(rate(milevadb_petri_load_schema_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_schema_lease_error_opm": {
		Comment: "MilevaDB schemaReplicant lease error counts",
		PromQL:  "sum(increase(milevadb_stochastik_schema_lease_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"milevadb_schema_lease_error_total_count": {
		Comment: "The total count of MilevaDB schemaReplicant lease error",
		PromQL:  "sum(increase(milevadb_stochastik_schema_lease_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
	},
	"milevadb_load_privilege_ops": {
		Comment: "MilevaDB load privilege counts",
		PromQL:  "sum(rate(milevadb_petri_load_privilege_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_dbs_duration": {
		Comment:  "The quantile of MilevaDB DBS duration statistics",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_dbs_handle_job_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"milevadb_dbs_batch_add_index_duration": {
		Comment:  "The quantile of MilevaDB batch add index durations by histogram buckets",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_dbs_batch_add_idx_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"milevadb_dbs_add_index_speed": {
		Comment: "MilevaDB add index speed",
		PromQL:  "sum(rate(milevadb_dbs_add_index_total[$RANGE_DURATION])) by (type)",
	},
	"milevadb_dbs_waiting_jobs_num": {
		Comment: "MilevaDB dbs request in queue",
		PromQL:  "milevadb_dbs_waiting_jobs{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_dbs_spacetime_opm": {
		Comment: "MilevaDB different dbs worker numbers",
		PromQL:  "increase(milevadb_dbs_worker_operation_total{$LABEL_CONDITIONS}[$RANGE_DURATION])",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_dbs_worker_duration": {
		Comment:  "The quantile of MilevaDB dbs worker duration",
		PromQL:   "histogram_quantile($QUANTILE, sum(increase(milevadb_dbs_worker_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, action, result,instance))",
		Labels:   []string{"instance", "type", "result", "action"},
		Quantile: 0.95,
	},
	"milevadb_dbs_deploy_syncer_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_dbs_deploy_syncer_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, result,instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
		Comment:  "The quantile of MilevaDB dbs schemaReplicant syncer statistics, including init, start, watch, clear function call time cost",
	},
	"milevadb_tenant_handle_syncer_duration": {
		Comment:  "The quantile of MilevaDB dbs tenant time operations on etcd duration statistics ",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_dbs_tenant_handle_syncer_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type, result,instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
	},
	"milevadb_dbs_uFIDelate_self_version_duration": {
		Comment:  "The quantile of MilevaDB schemaReplicant syncer version uFIDelate time duration",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_dbs_uFIDelate_self_ver_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, result,instance))",
		Labels:   []string{"instance", "result"},
		Quantile: 0.95,
	},
	"milevadb_dbs_opm": {
		Comment: "The quantile of executed DBS jobs per minute",
		PromQL:  "sum(rate(milevadb_dbs_handle_job_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_statistics_auto_analyze_duration": {
		Comment:  "The quantile of MilevaDB auto analyze time durations within 95 percent histogram buckets",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_statistics_auto_analyze_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"milevadb_statistics_auto_analyze_ops": {
		Comment: "MilevaDB auto analyze query per second",
		PromQL:  "sum(rate(milevadb_statistics_auto_analyze_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_statistics_stats_inaccuracy_rate": {
		Comment:  "The quantile of MilevaDB statistics inaccurate rate",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_statistics_stats_inaccuracy_rate_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"milevadb_statistics_pseudo_estimation_ops": {
		Comment: "MilevaDB optimizer using pseudo estimation counts",
		PromQL:  "sum(rate(milevadb_statistics_pseudo_estimation_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"milevadb_statistics_pseudo_estimation_total_count": {
		Comment: "The total count of MilevaDB optimizer using pseudo estimation",
		PromQL:  "sum(increase(milevadb_statistics_pseudo_estimation_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"milevadb_statistics_dump_feedback_ops": {
		Comment: "MilevaDB dumping statistics back to ekv storage times",
		PromQL:  "sum(rate(milevadb_statistics_dump_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_statistics_dump_feedback_total_count": {
		Comment: "The total count of operations that MilevaDB dumping statistics back to ekv storage",
		PromQL:  "sum(increase(milevadb_statistics_dump_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_statistics_store_query_feedback_qps": {
		Comment: "MilevaDB causetstore quering feedback counts",
		PromQL:  "sum(rate(milevadb_statistics_store_query_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) ",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_statistics_store_query_feedback_total_count": {
		Comment: "The total count of MilevaDB causetstore quering feedback",
		PromQL:  "sum(increase(milevadb_statistics_store_query_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) ",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_statistics_significant_feedback": {
		Comment: "Counter of query feedback whose actual count is much different than calculated by current statistics",
		PromQL:  "sum(rate(milevadb_statistics_high_error_rate_feedback_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"milevadb_statistics_uFIDelate_stats_ops": {
		Comment: "MilevaDB uFIDelating statistics using feed back counts",
		PromQL:  "sum(rate(milevadb_statistics_uFIDelate_stats_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_statistics_uFIDelate_stats_total_count": {
		Comment: "The total count of MilevaDB uFIDelating statistics using feed back",
		PromQL:  "sum(increase(milevadb_statistics_uFIDelate_stats_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_statistics_fast_analyze_status": {
		Comment:  "The quantile of MilevaDB fast analyze statistics ",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_statistics_fast_analyze_status_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"milevadb_new_etcd_stochastik_duration": {
		Comment:  "The quantile of MilevaDB new stochastik durations for new etcd stochastik",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_tenant_new_stochastik_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,result, instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
	},
	"milevadb_tenant_watcher_ops": {
		Comment: "MilevaDB tenant watcher counts",
		PromQL:  "sum(rate(milevadb_tenant_watch_tenant_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type, result, instance)",
		Labels:  []string{"instance", "type", "result"},
	},
	"milevadb_auto_id_qps": {
		Comment: "MilevaDB auto id requests per second including single causet/global auto id processing and single causet auto id rebase processing",
		PromQL:  "sum(rate(milevadb_autoid_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))",
		Labels:  []string{"instance"},
	},
	"milevadb_auto_id_request_duration": {
		Comment:  "The quantile of MilevaDB auto id requests durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_autoid_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"milevadb_region_cache_ops": {
		Comment: "MilevaDB region cache operations count",
		PromQL:  "sum(rate(milevadb_einsteindbclient_region_cache_operations_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,result,instance)",
		Labels:  []string{"instance", "type", "result"},
	},
	"milevadb_spacetime_operation_duration": {
		Comment:  "The quantile of MilevaDB spacetime operation durations including get/set schemaReplicant and dbs jobs",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_spacetime_operation_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, type,result,instance))",
		Labels:   []string{"instance", "type", "result"},
		Quantile: 0.95,
	},
	"milevadb_gc_worker_action_opm": {
		Comment: "ekv storage garbage defCauslection counts by type",
		PromQL:  "sum(increase(milevadb_einsteindbclient_gc_worker_actions_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_gc_duration": {
		Comment:  "The quantile of ekv storage garbage defCauslection time durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_gc_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"milevadb_gc_config": {
		Comment: "ekv storage garbage defCauslection config including gc_life_time and gc_run_interval",
		PromQL:  "milevadb_einsteindbclient_gc_config{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_gc_fail_opm": {
		Comment: "ekv storage garbage defCauslection failing counts",
		PromQL:  "sum(increase(milevadb_einsteindbclient_gc_failure{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_gc_delete_range_fail_opm": {
		Comment: "ekv storage unsafe destroy range failed counts",
		PromQL:  "sum(increase(milevadb_einsteindbclient_gc_unsafe_destroy_range_failures{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_gc_too_many_locks_opm": {
		Comment: "ekv storage region garbage defCauslection clean too many locks count",
		PromQL:  "sum(increase(milevadb_einsteindbclient_gc_region_too_many_locks[$RANGE_DURATION]))",
	},
	"milevadb_gc_action_result_opm": {
		Comment: "ekv storage garbage defCauslection results including failed and successful ones",
		PromQL:  "sum(increase(milevadb_einsteindbclient_gc_action_result{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)",
		Labels:  []string{"instance", "type"},
	},
	"milevadb_gc_delete_range_task_status": {
		Comment: "ekv storage delete range task execution status by type",
		PromQL:  "sum(milevadb_einsteindbclient_range_task_stats{$LABEL_CONDITIONS}) by (type, result,instance)",
		Labels:  []string{"instance", "type", "result"},
	},
	"milevadb_gc_push_task_duration": {
		Comment:  "The quantile of ekv storage range worker processing one task duration",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_range_task_push_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))",
		Labels:   []string{"instance", "type"},
		Quantile: 0.95,
	},
	"milevadb_batch_client_pending_req_count": {
		Comment: "ekv storage batch requests in queue",
		PromQL:  "sum(milevadb_einsteindbclient_pending_batch_requests{$LABEL_CONDITIONS}) by (causetstore,instance)",
		Labels:  []string{"instance", "causetstore"},
	},
	"milevadb_batch_client_wait_duration": {
		Comment:  "The quantile of ekv storage batch processing durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_batch_wait_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"milevadb_batch_client_wait_conn_duration": {
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_batch_client_wait_connection_establish_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
		Comment:  "The quantile of batch client wait new connection establish durations",
	},
	"milevadb_batch_client_wait_conn_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_batch_client_wait_connection_establish_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of batch client wait new connection establish",
	},
	"milevadb_batch_client_wait_conn_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_batch_client_wait_connection_establish_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of batch client wait new connection establish",
	},
	"milevadb_batch_client_unavailable_duration": {
		Comment:  "The quantile of ekv storage batch processing unvailable durations",
		PromQL:   "histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_batch_client_unavailable_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))",
		Labels:   []string{"instance"},
		Quantile: 0.95,
	},
	"uptime": {
		PromQL:  "(time() - process_start_time_seconds{$LABEL_CONDITIONS})",
		Labels:  []string{"instance", "job"},
		Comment: "MilevaDB uptime since last restart(second)",
	},
	"up": {
		PromQL:  `up{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "job"},
		Comment: "whether the instance is up. 1 is up, 0 is down(off-line)",
	},
	"FIDel_role": {
		PromQL:  `delta(FIDel_tso_events{type="save"}[$RANGE_DURATION]) > bool 0`,
		Labels:  []string{"instance"},
		Comment: "It indicates whether the current FIDel is the leader or a follower.",
	},
	"normal_stores": {
		PromQL:  `sum(FIDel_cluster_status{type="store_up_count"}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The count of healthy stores",
	},
	"abnormal_stores": {
		PromQL: `sum(FIDel_cluster_status{ type=~"store_disconnected_count|store_unhealth_count|store_low_space_count|store_down_count|store_offline_count|store_tombstone_count"})`,
		Labels: []string{"instance", "type"},
	},
	"FIDel_scheduler_config": {
		PromQL: `FIDel_config_status{$LABEL_CONDITIONS}`,
		Labels: []string{"type"},
	},
	"FIDel_region_label_isolation_level": {
		PromQL: `FIDel_regions_label_level{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"FIDel_label_distribution": {
		PromQL: `FIDel_cluster_memristed_status{$LABEL_CONDITIONS}`,
		Labels: []string{"name"},
	},
	"FIDel_cluster_status": {
		PromQL: `sum(FIDel_cluster_status{$LABEL_CONDITIONS}) by (instance, type)`,
		Labels: []string{"instance", "type"},
	},
	"FIDel_cluster_spacetimedata": {
		PromQL: `FIDel_cluster_spacetimedata{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"FIDel_region_health": {
		PromQL:  `sum(FIDel_regions_status{$LABEL_CONDITIONS}) by (instance, type)`,
		Labels:  []string{"instance", "type"},
		Comment: "It records the unusual Regions' count which may include pending peers, down peers, extra peers, offline peers, missing peers or learner peers",
	},
	"FIDel_schedule_operator": {
		PromQL:  `sum(delta(FIDel_schedule_operators_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,event,instance)`,
		Labels:  []string{"instance", "type", "event"},
		Comment: "The number of different operators",
	},
	"FIDel_schedule_operator_total_num": {
		PromQL:  `sum(increase(FIDel_schedule_operators_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,event,instance)`,
		Labels:  []string{"instance", "type", "event"},
		Comment: "The total number of different operators",
	},
	"FIDel_operator_finish_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(FIDel_schedule_finish_operators_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type))`,
		Labels:   []string{"type"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when the operator is finished",
	},
	"FIDel_operator_step_finish_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(FIDel_schedule_finish_operator_steps_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type))`,
		Labels:   []string{"type"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when the operator step is finished",
	},
	"FIDel_scheduler_store_status": {
		PromQL: `FIDel_scheduler_store_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "address", "causetstore", "type"},
	},
	"store_available_ratio": {
		PromQL:  `sum(FIDel_scheduler_store_status{type="store_available"}) by (address, causetstore) / sum(FIDel_scheduler_store_status{type="store_capacity"}) by (address, causetstore)`,
		Labels:  []string{"address", "causetstore"},
		Comment: "It is equal to CausetStore available capacity size over CausetStore capacity size for each EinsteinDB instance",
	},
	"store_size_amplification": {
		PromQL:  `sum(FIDel_scheduler_store_status{type="region_size"}) by (address, causetstore) / sum(FIDel_scheduler_store_status{type="store_used"}) by (address, causetstore) * 2^20`,
		Labels:  []string{"address", "causetstore"},
		Comment: "The size amplification, which is equal to CausetStore Region size over CausetStore used capacity size, of each EinsteinDB instance",
	},
	"FIDel_scheduler_op_influence": {
		PromQL: `FIDel_scheduler_op_influence{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "scheduler", "causetstore", "type"},
	},
	"FIDel_scheduler_tolerant_resource": {
		PromQL: `FIDel_scheduler_tolerant_resource{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "scheduler", "source", "target"},
	},
	"FIDel_hotspot_status": {
		PromQL: `FIDel_hotspot_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "address", "causetstore", "type"},
	},
	"FIDel_scheduler_status": {
		PromQL: `FIDel_scheduler_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "HoTT", "type"},
	},
	"FIDel_scheduler_balance_leader": {
		PromQL:  `sum(delta(FIDel_scheduler_balance_leader{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,causetstore,instance,type)`,
		Labels:  []string{"instance", "address", "causetstore", "type"},
		Comment: "The leader movement details among EinsteinDB instances",
	},
	"FIDel_scheduler_balance_region": {
		PromQL:  `sum(delta(FIDel_scheduler_balance_region{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,causetstore,instance,type)`,
		Labels:  []string{"instance", "address", "causetstore", "type"},
		Comment: "The Region movement details among EinsteinDB instances",
	},
	"FIDel_balance_scheduler_status": {
		PromQL:  `sum(delta(FIDel_scheduler_event_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,name)`,
		Labels:  []string{"instance", "name", "type"},
		Comment: "The inner status of balance leader scheduler",
	},
	"FIDel_checker_event_count": {
		PromQL:  `sum(delta(FIDel_checker_event_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance,type)`,
		Labels:  []string{"instance", "name", "type"},
		Comment: "The replica/region checker's status",
	},
	"FIDel_schedule_filter": {
		PromQL: `sum(delta(FIDel_schedule_filter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (causetstore, type, scope, instance)`,
		Labels: []string{"instance", "scope", "causetstore", "type"},
	},
	"FIDel_scheduler_balance_direction": {
		PromQL: `sum(delta(FIDel_scheduler_balance_direction{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,source,target,instance)`,
		Labels: []string{"instance", "source", "target", "type"},
	},
	"FIDel_schedule_store_limit": {
		PromQL: `FIDel_schedule_store_limit{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "causetstore", "type"},
	},

	"FIDel_grpc_completed_commands_rate": {
		PromQL:  `sum(rate(grpc_server_handling_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (grpc_method,instance)`,
		Labels:  []string{"instance", "grpc_method"},
		Comment: "The rate of completing each HoTT of gRPC commands",
	},
	"FIDel_grpc_completed_commands_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(grpc_server_handling_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,grpc_method,instance))`,
		Labels:   []string{"instance", "grpc_method"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed of completing each HoTT of gRPC commands",
	},
	"FIDel_handle_transactions_rate": {
		PromQL:  `sum(rate(FIDel_txn_handle_txns_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, result)`,
		Labels:  []string{"instance", "result"},
		Comment: "The rate of handling etcd transactions",
	},
	"FIDel_handle_transactions_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(FIDel_txn_handle_txns_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance, result))`,
		Labels:   []string{"instance", "result"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed of handling etcd transactions",
	},
	"etcd_wal_fsync_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(etcd_disk_wal_fsync_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed of writing WAL into the persistent storage",
	},
	"FIDel_peer_round_trip_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(etcd_network_peer_round_trip_time_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,To))`,
		Labels:   []string{"instance", "To"},
		Quantile: 0.99,
		Comment:  "The quantile latency of the network in .99",
	},
	"etcd_disk_wal_fsync_rate": {
		PromQL:  `delta(etcd_disk_wal_fsync_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels:  []string{"instance"},
		Comment: "The rate of writing WAL into the persistent storage",
	},
	"FIDel_server_etcd_state": {
		PromQL:  `FIDel_server_etcd_state{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "type"},
		Comment: "The current term of Raft",
	},
	"FIDel_request_rpc_duration_avg": {
		PromQL: `avg(rate(FIDel_client_request_handle_requests_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type) / avg(rate(FIDel_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)`,
		Labels: []string{"type"},
	},
	"FIDel_region_heartbeat_duration": {
		PromQL:   `round(histogram_quantile($QUANTILE, sum(rate(FIDel_scheduler_region_heartbeat_latency_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,address, causetstore)), 1000)`,
		Labels:   []string{"address", "causetstore"},
		Quantile: 0.99,
		Comment:  "The quantile of heartbeat latency of each EinsteinDB instance in",
	},
	"FIDel_scheduler_region_heartbeat": {
		PromQL: `sum(rate(FIDel_scheduler_region_heartbeat{$LABEL_CONDITIONS}[$RANGE_DURATION])*60) by (address,instance, causetstore, status,type)`,
		Labels: []string{"instance", "address", "status", "causetstore", "type"},
	},
	"FIDel_region_syncer_status": {
		PromQL: `FIDel_region_syncer_status{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"einsteindb_engine_size": {
		PromQL:  `sum(einsteindb_engine_size_bytes{$LABEL_CONDITIONS}) by (instance, type, EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The storage size per EinsteinDB instance",
	},
	"einsteindb_store_size": {
		PromQL:  `sum(einsteindb_store_size_bytes{$LABEL_CONDITIONS}) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The available or capacity size of each EinsteinDB instance",
	},
	"einsteindb_thread_cpu": {
		PromQL:  `sum(rate(einsteindb_thread_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,name)`,
		Labels:  []string{"instance", "name"},
		Comment: "The CPU usage of each EinsteinDB instance",
	},
	"einsteindb_memory": {
		PromQL:  `avg(process_resident_memory_bytes{$LABEL_CONDITIONS}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The memory usage per EinsteinDB instance",
	},
	"einsteindb_io_utilization": {
		PromQL:  `rate(node_disk_io_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels:  []string{"instance", "device"},
		Comment: "The I/O utilization per EinsteinDB instance",
	},
	"einsteindb_flow_mbps": {
		PromQL:  `sum(rate(einsteindb_engine_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The total bytes of read and write in each EinsteinDB instance",
	},
	"einsteindb_grpc_qps": {
		PromQL:  `sum(rate(einsteindb_grpc_msg_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The QPS per command in each EinsteinDB instance",
	},
	"einsteindb_grpc_errors": {
		PromQL:  `sum(rate(einsteindb_grpc_msg_fail_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The OPS of the gRPC message failures",
	},
	"einsteindb_grpc_error_total_count": {
		PromQL:  `sum(increase(einsteindb_grpc_msg_fail_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total count of the gRPC message failures",
	},
	"einsteindb_critical_error": {
		PromQL:  `sum(rate(einsteindb_critical_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The OPS of the EinsteinDB critical error",
	},
	"einsteindb_critical_error_total_count": {
		PromQL:  `sum(increase(einsteindb_critical_error_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of the EinsteinDB critical error",
	},
	"einsteindb_FIDel_heartbeat": {
		PromQL:  `sum(delta(einsteindb_FIDel_heartbeat_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of the gRPC message failures",
	},
	"einsteindb_region_count": {
		PromQL:  `sum(einsteindb_raftstore_region_count{$LABEL_CONDITIONS}) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of regions on each EinsteinDB instance",
	},
	"einsteindb_scheduler_is_busy": {
		PromQL:  `sum(rate(einsteindb_scheduler_too_busy_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,EDB,type,stage)`,
		Labels:  []string{"instance", "EDB", "type", "stage"},
		Comment: "Indicates occurrences of Scheduler Busy events that make the EinsteinDB instance unavailable temporarily",
	},
	"einsteindb_scheduler_is_busy_total_count": {
		PromQL:  `sum(increase(einsteindb_scheduler_too_busy_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,EDB,type,stage)`,
		Labels:  []string{"instance", "EDB", "type", "stage"},
		Comment: "The total count of Scheduler Busy events that make the EinsteinDB instance unavailable temporarily",
	},
	"einsteindb_channel_full": {
		PromQL:  `sum(rate(einsteindb_channel_full_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,EDB)`,
		Labels:  []string{"instance", "EDB", "type"},
		Comment: "The ops of channel full errors on each EinsteinDB instance, it will make the EinsteinDB instance unavailable temporarily",
	},
	"einsteindb_channel_full_total_count": {
		PromQL:  `sum(increase(einsteindb_channel_full_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,EDB)`,
		Labels:  []string{"instance", "EDB", "type"},
		Comment: "The total number of channel full errors on each EinsteinDB instance, it will make the EinsteinDB instance unavailable temporarily",
	},
	"einsteindb_coprocessor_is_busy": {
		PromQL:  `sum(rate(einsteindb_coprocessor_request_error{type='full'}[$RANGE_DURATION])) by (instance,EDB,type)`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The ops of Coprocessor Full events that make the EinsteinDB instance unavailable temporarily",
	},
	"einsteindb_coprocessor_is_busy_total_count": {
		PromQL:  `sum(increase(einsteindb_coprocessor_request_error{type='full'}[$RANGE_DURATION])) by (instance,EDB,type)`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The total count of Coprocessor Full events that make the EinsteinDB instance unavailable temporarily",
	},
	"einsteindb_engine_write_stall": {
		PromQL:  `avg(einsteindb_engine_write_stall{type="write_stall_percentile99"}) by (instance, EDB)`,
		Labels:  []string{"instance", "EDB"},
		Comment: "Indicates occurrences of Write Stall events that make the EinsteinDB instance unavailable temporarily",
	},
	"einsteindb_server_report_failures": {
		PromQL:  `sum(rate(einsteindb_server_report_failure_msg_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,store_id)`,
		Labels:  []string{"instance", "store_id", "type"},
		Comment: "The total number of reported failure messages",
	},
	"einsteindb_server_report_failures_total_count": {
		PromQL:  `sum(increase(einsteindb_server_report_failure_msg_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,store_id)`,
		Labels:  []string{"instance", "store_id", "type"},
		Comment: "The total number of reported failure messages",
	},
	"einsteindb_storage_async_requests": {
		PromQL:  `sum(rate(einsteindb_storage_engine_async_request_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, status, type)`,
		Labels:  []string{"instance", "status", "type"},
		Comment: "The number of different raftstore errors on each EinsteinDB instance",
	},
	"einsteindb_storage_async_requests_total_count": {
		PromQL:  `sum(increase(einsteindb_storage_engine_async_request_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, status, type)`,
		Labels:  []string{"instance", "status", "type"},
		Comment: "The total number of different raftstore errors on each EinsteinDB instance",
	},
	"einsteindb_scheduler_stage": {
		PromQL:  `sum(rate(einsteindb_scheduler_stage_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, stage,type)`,
		Labels:  []string{"instance", "stage", "type"},
		Comment: "The number of scheduler state on each EinsteinDB instance",
	},
	"einsteindb_scheduler_stage_total_num": {
		PromQL:  `sum(increase(einsteindb_scheduler_stage_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, stage,type)`,
		Labels:  []string{"instance", "stage", "type"},
		Comment: "The total number of scheduler state on each EinsteinDB instance",
	},

	"einsteindb_coprocessor_request_error": {
		PromQL:  `sum(rate(einsteindb_coprocessor_request_error{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, reason)`,
		Labels:  []string{"instance", "reason"},
		Comment: "The number of different interlock errors on each EinsteinDB instance",
	},
	"einsteindb_coprocessor_request_error_total_count": {
		PromQL:  `sum(increase(einsteindb_coprocessor_request_error{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, reason)`,
		Labels:  []string{"instance", "reason"},
		Comment: "The total number of different interlock errors on each EinsteinDB instance",
	},
	"einsteindb_region_change": {
		PromQL:  `sum(delta(einsteindb_raftstore_region_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The count of region change per EinsteinDB instance",
	},
	"einsteindb_leader_missing": {
		PromQL:  `sum(einsteindb_raftstore_leader_missing{$LABEL_CONDITIONS}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The count of missing leaders per EinsteinDB instance",
	},
	"einsteindb_active_written_leaders": {
		PromQL:  `sum(rate(einsteindb_region_written_keys_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of leaders being written on each EinsteinDB instance",
	},
	"einsteindb_approximate_region_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_region_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile of approximate Region size, the default value is P99",
	},
	"einsteindb_approximate_avg_region_size": {
		PromQL:  `sum(rate(einsteindb_raftstore_region_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_raftstore_region_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance"},
		Comment: "The avg approximate Region size",
	},
	"einsteindb_approximate_region_size_histogram": {
		PromQL: `sum(rate(einsteindb_raftstore_region_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"einsteindb_region_average_written_bytes": {
		PromQL:  `sum(rate(einsteindb_region_written_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance) / sum(rate(einsteindb_region_written_bytes_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The average rate of writing bytes to Regions per EinsteinDB instance",
	},
	"einsteindb_region_written_bytes": {
		PromQL: `sum(rate(einsteindb_region_written_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"einsteindb_region_average_written_keys": {
		PromQL:  `sum(rate(einsteindb_region_written_keys_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance) / sum(rate(einsteindb_region_written_keys_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The average rate of written keys to Regions per EinsteinDB instance",
	},
	"einsteindb_region_written_keys": {
		PromQL: `sum(rate(einsteindb_region_written_keys_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"einsteindb_request_batch_avg": {
		PromQL:  `sum(rate(einsteindb_server_request_batch_ratio_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(einsteindb_server_request_batch_ratio_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The ratio of request batch output to input per EinsteinDB instance",
	},
	"einsteindb_request_batch_ratio": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_server_request_batch_ratio_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile ratio of request batch output to input per EinsteinDB instance",
	},
	"einsteindb_request_batch_size_avg": {
		PromQL:  `sum(rate(einsteindb_server_request_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(einsteindb_server_request_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The avg size of requests into request batch per EinsteinDB instance",
	},
	"einsteindb_request_batch_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_server_request_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile size of requests into request batch per EinsteinDB instance",
	},

	"einsteindb_grpc_message_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_grpc_msg_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile execution time of gRPC message",
	},
	"einsteindb_average_grpc_messge_duration": {
		PromQL: `sum(rate(einsteindb_grpc_msg_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(einsteindb_grpc_msg_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"einsteindb_grpc_req_batch_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_server_grpc_req_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"einsteindb_grpc_resp_batch_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_server_grpc_resp_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"einsteindb_grpc_avg_req_batch_size": {
		PromQL: `sum(rate(einsteindb_server_grpc_req_batch_size_sum[$RANGE_DURATION])) / sum(rate(einsteindb_server_grpc_req_batch_size_count[$RANGE_DURATION]))`,
	},
	"einsteindb_grpc_avg_resp_batch_size": {
		PromQL: `sum(rate(einsteindb_server_grpc_resp_batch_size_sum[$RANGE_DURATION])) / sum(rate(einsteindb_server_grpc_resp_batch_size_count[$RANGE_DURATION]))`,
	},
	"einsteindb_raft_message_batch_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_server_raft_message_batch_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"einsteindb_raft_message_avg_batch_size": {
		PromQL: `sum(rate(einsteindb_server_raft_message_batch_size_sum[$RANGE_DURATION])) / sum(rate(einsteindb_server_raft_message_batch_size_count[$RANGE_DURATION]))`,
	},
	"einsteindb_FIDel_request_ops": {
		PromQL:  `sum(rate(einsteindb_FIDel_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The OPS of requests that EinsteinDB sends to FIDel",
	},
	"einsteindb_FIDel_request_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_FIDel_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
	},
	"einsteindb_FIDel_request_total_count": {
		PromQL:  `sum(increase(einsteindb_FIDel_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The count of requests that EinsteinDB sends to FIDel",
	},
	"einsteindb_FIDel_request_total_time": {
		PromQL:  `sum(increase(einsteindb_FIDel_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The count of requests that EinsteinDB sends to FIDel",
	},
	"einsteindb_FIDel_request_avg_duration": {
		PromQL:  `sum(rate(einsteindb_FIDel_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type ,instance) / sum(rate(einsteindb_FIDel_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The time consumed by requests that EinsteinDB sends to FIDel",
	},
	"einsteindb_FIDel_heartbeats": {
		PromQL:  `sum(rate(einsteindb_FIDel_heartbeat_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type ,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: " The total number of FIDel heartbeat messages",
	},
	"einsteindb_FIDel_validate_peers": {
		PromQL:  `sum(rate(einsteindb_FIDel_validate_peer_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type ,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of peers validated by the FIDel worker",
	},
	"einsteindb_raftstore_apply_log_avg_duration": {
		PromQL:  `sum(rate(einsteindb_raftstore_apply_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_raftstore_apply_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance"},
		Comment: "The average time consumed when Raft applies log",
	},
	"einsteindb_raftstore_apply_log_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_apply_log_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when Raft applies log",
	},
	"einsteindb_raftstore_append_log_avg_duration": {
		PromQL:  `sum(rate(einsteindb_raftstore_append_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_raftstore_append_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The avg time consumed when Raft appends log",
	},
	"einsteindb_raftstore_append_log_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_append_log_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when Raft appends log",
	},
	"einsteindb_raftstore_commit_log_avg_duration": {
		PromQL:  `sum(rate(einsteindb_raftstore_commit_log_duration_seconds_sum[$RANGE_DURATION])) / sum(rate(einsteindb_raftstore_commit_log_duration_seconds_count[$RANGE_DURATION]))`,
		Comment: "The time consumed when Raft commits log",
	},
	"einsteindb_raftstore_commit_log_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_commit_log_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed when Raft commits log",
	},
	"einsteindb_ready_handled": {
		PromQL:  `sum(rate(einsteindb_raftstore_raft_ready_handled_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance"},
		Comment: "The count of ready handled of Raft",
	},
	"einsteindb_raftstore_process_handled": {
		PromQL:  `sum(rate(einsteindb_raftstore_raft_process_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance", "type"},
		Comment: "The count of different process type of Raft",
	},
	"einsteindb_raftstore_process_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_raft_process_duration_secs_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed for peer processes in Raft",
	},
	"einsteindb_raft_store_events_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_event_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile time consumed by raftstore events (P99).99",
	},
	"einsteindb_raft_sent_messages": {
		PromQL:  `sum(rate(einsteindb_raftstore_raft_sent_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of Raft messages sent by each EinsteinDB instance",
	},
	"einsteindb_raft_sent_messages_total_num": {
		PromQL:  `sum(increase(einsteindb_raftstore_raft_sent_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of Raft messages sent by each EinsteinDB instance",
	},
	"einsteindb_flush_messages": {
		PromQL:  `sum(rate(einsteindb_server_raft_message_flush_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of Raft messages flushed by each EinsteinDB instance",
	},
	"einsteindb_flush_messages_total_num": {
		PromQL:  `sum(increase(einsteindb_server_raft_message_flush_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The total number of Raft messages flushed by each EinsteinDB instance",
	},

	"einsteindb_receive_messages": {
		PromQL:  `sum(rate(einsteindb_server_raft_message_recv_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of Raft messages received by each EinsteinDB instance",
	},
	"einsteindb_receive_messages_total_num": {
		PromQL:  `sum(increase(einsteindb_server_raft_message_recv_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The total number of Raft messages received by each EinsteinDB instance",
	},
	"einsteindb_raft_dropped_messages": {
		PromQL:  `sum(rate(einsteindb_raftstore_raft_dropped_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of dropped Raft messages per type",
	},
	"einsteindb_raft_dropped_messages_total": {
		PromQL:  `sum(increase(einsteindb_raftstore_raft_dropped_message_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of dropped Raft messages per type",
	},
	"einsteindb_raft_proposals_per_ready": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_apply_proposal_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile proposal count of all Regions in a mio tick",
	},
	"einsteindb_raft_proposals": {
		PromQL:  `sum(rate(einsteindb_raftstore_proposal_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of proposals per type in raft",
	},
	"einsteindb_raft_proposals_total_num": {
		PromQL:  `sum(increase(einsteindb_raftstore_proposal_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of proposals per type in raft",
	},
	"einsteindb_raftstore_propose_wait_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_request_wait_time_duration_secs_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile wait time of each proposal",
	},
	"einsteindb_propose_avg_wait_duration": {
		PromQL:  `sum(rate(einsteindb_raftstore_request_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_raftstore_request_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The average wait time of each proposal",
	},
	"einsteindb_raftstore_apply_wait_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_apply_wait_time_duration_secs_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"einsteindb_apply_avg_wait_duration": {
		PromQL: `sum(rate(einsteindb_raftstore_apply_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_raftstore_apply_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"einsteindb_raft_log_speed": {
		PromQL:  `avg(rate(einsteindb_raftstore_propose_log_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The rate at which peers propose logs",
	},
	"einsteindb_admin_apply": {
		PromQL:  `sum(rate(einsteindb_raftstore_admin_cmd_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,status,instance)`,
		Labels:  []string{"instance", "type", "status"},
		Comment: "The number of the processed apply command",
	},
	"einsteindb_check_split": {
		PromQL:  `sum(rate(einsteindb_raftstore_check_split_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of raftstore split checks",
	},
	"einsteindb_check_split_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_check_split_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le, instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.9999,
		Comment:  "The quantile of time consumed when running split check in .9999",
	},
	"einsteindb_local_reader_reject_requests": {
		PromQL:  `sum(rate(einsteindb_raftstore_local_read_reject_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, reason)`,
		Labels:  []string{"instance", "reason"},
		Comment: "The number of rejections from the local read thread",
	},
	"einsteindb_local_reader_execute_requests": {
		PromQL:  `sum(rate(einsteindb_raftstore_local_read_executed_requests{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of total requests from the local read thread",
	},
	"einsteindb_storage_command_ops": {
		PromQL:  `sum(rate(einsteindb_storage_command_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total count of different HoTTs of commands received per seconds",
	},
	"einsteindb_storage_async_request_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_storage_engine_async_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed by processing asynchronous snapshot requests",
	},
	"einsteindb_storage_async_request_avg_duration": {
		PromQL:  `sum(rate(einsteindb_storage_engine_async_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_storage_engine_async_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance", "type"},
		Comment: "The time consumed by processing asynchronous snapshot requests",
	},
	"einsteindb_scheduler_writing_bytes": {
		PromQL:  `sum(einsteindb_scheduler_writing_bytes{$LABEL_CONDITIONS}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The total writing bytes of commands on each stage",
	},
	"einsteindb_scheduler_priority_commands": {
		PromQL:  `sum(rate(einsteindb_scheduler_commands_pri_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (priority,instance)`,
		Labels:  []string{"instance", "priority"},
		Comment: "The count of different priority commands",
	},

	"einsteindb_scheduler_pending_commands": {
		PromQL:  `sum(einsteindb_scheduler_contex_total{$LABEL_CONDITIONS}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The count of pending commands per EinsteinDB instance",
	},
	"einsteindb_scheduler_command_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_scheduler_command_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed when executing command",
	},
	"einsteindb_scheduler_command_avg_duration": {
		PromQL:  `sum(rate(einsteindb_scheduler_command_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_scheduler_command_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance", "type"},
		Comment: "The average time consumed when executing command",
	},
	"einsteindb_scheduler_latch_wait_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_scheduler_latch_wait_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile time which is caused by latch wait in command",
	},
	"einsteindb_scheduler_latch_wait_avg_duration": {
		PromQL:  `sum(rate(einsteindb_scheduler_latch_wait_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_scheduler_latch_wait_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance", "type"},
		Comment: "The average time which is caused by latch wait in command",
	},
	"einsteindb_scheduler_processing_read_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_scheduler_processing_read_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile time of scheduler processing read in command",
	},
	"einsteindb_scheduler_processing_read_total_count": {
		PromQL:  "sum(increase(einsteindb_scheduler_processing_read_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of scheduler processing read in command",
	},
	"einsteindb_scheduler_processing_read_total_time": {
		PromQL:  "sum(increase(einsteindb_scheduler_processing_read_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of scheduler processing read in command",
	},

	"einsteindb_scheduler_keys_read": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_scheduler_ekv_command_key_read_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile count of keys read by command",
	},
	"einsteindb_scheduler_keys_read_avg": {
		PromQL:  `sum(rate(einsteindb_scheduler_ekv_command_key_read_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_scheduler_ekv_command_key_read_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance", "type"},
		Comment: "The average count of keys read by command",
	},
	"einsteindb_scheduler_keys_written": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_scheduler_ekv_command_key_write_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile of count of keys written by a command",
	},
	"einsteindb_scheduler_keys_written_avg": {
		PromQL:  `sum(rate(einsteindb_scheduler_ekv_command_key_write_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_scheduler_ekv_command_key_write_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) `,
		Labels:  []string{"instance", "type"},
		Comment: "The average count of keys written by a command",
	},
	"einsteindb_scheduler_scan_details": {
		PromQL:  `sum(rate(einsteindb_scheduler_ekv_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,instance,req,cf)`,
		Labels:  []string{"instance", "tag", "req", "cf"},
		Comment: "The keys scan details of each CF when executing command",
	},
	"einsteindb_scheduler_scan_details_total_num": {
		PromQL: `sum(increase(einsteindb_scheduler_ekv_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,instance,req,cf)`,
		Labels: []string{"instance", "tag", "req", "cf"},
	},
	"einsteindb_mvcc_versions": {
		PromQL:  `sum(rate(einsteindb_storage_mvcc_versions_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The number of versions for each key",
	},
	"einsteindb_mvcc_delete_versions": {
		PromQL:  `sum(rate(einsteindb_storage_mvcc_gc_delete_versions_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "The number of versions deleted by GC for each key",
	},
	"einsteindb_gc_tasks_ops": {
		PromQL:  `sum(rate(einsteindb_gcworker_gc_tasks_vec{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)`,
		Labels:  []string{"instance", "task"},
		Comment: "The count of GC total tasks processed by gc_worker per second",
	},
	"einsteindb_gc_skipped_tasks": {
		PromQL:  `sum(rate(einsteindb_storage_gc_skipped_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)`,
		Labels:  []string{"instance", "task"},
		Comment: "The count of GC skipped tasks processed by gc_worker",
	},
	"einsteindb_gc_fail_tasks": {
		PromQL:  `sum(rate(einsteindb_gcworker_gc_task_fail_vec{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)`,
		Labels:  []string{"instance", "task"},
		Comment: "The count of GC tasks processed fail by gc_worker",
	},
	"einsteindb_gc_too_busy": {
		PromQL:  `sum(rate(einsteindb_gc_worker_too_busy{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The count of GC worker too busy",
	},
	"einsteindb_gc_tasks_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_gcworker_gc_task_duration_vec_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,task,instance))`,
		Labels:   []string{"instance", "task"},
		Quantile: 1,
		Comment:  "The quantile of time consumed when executing GC tasks",
	},
	"einsteindb_gc_tasks_avg_duration": {
		PromQL:  `sum(rate(einsteindb_gcworker_gc_task_duration_vec_sum{}[$RANGE_DURATION])) by (task,instance) / sum(rate(einsteindb_gcworker_gc_task_duration_vec_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (task,instance)`,
		Labels:  []string{"instance", "task"},
		Comment: "The time consumed when executing GC tasks",
	},
	"einsteindb_gc_keys": {
		PromQL:  `sum(rate(einsteindb_gcworker_gc_keys{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,cf,instance)`,
		Labels:  []string{"instance", "tag", "cf"},
		Comment: "The count of keys in write CF affected during GC",
	},
	"einsteindb_gc_keys_total_num": {
		PromQL:  `sum(increase(einsteindb_gcworker_gc_keys{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,cf,instance)`,
		Labels:  []string{"instance", "tag", "cf"},
		Comment: "The total number of keys in write CF affected during GC",
	},
	"einsteindb_gc_speed": {
		PromQL:  `sum(rate(einsteindb_storage_mvcc_gc_delete_versions_sum[$RANGE_DURATION]))`,
		Comment: "The GC keys per second",
	},
	"einsteindb_auto_gc_working": {
		PromQL: `sum(max_over_time(einsteindb_gcworker_autogc_status{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,state)`,
		Labels: []string{"instance", "state"},
	},
	"einsteindb_client_task_progress": {
		PromQL:  `max(milevadb_einsteindbclient_range_task_stats{$LABEL_CONDITIONS}) by (result,type)`,
		Labels:  []string{"result", "type"},
		Comment: "The progress of einsteindb client task",
	},
	"einsteindb_auto_gc_progress": {
		PromQL:  `sum(einsteindb_gcworker_autogc_processed_regions{type="scan"}) by (instance,type) / sum(einsteindb_raftstore_region_count{type="region"}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "Progress of EinsteinDB's GC",
	},
	"einsteindb_auto_gc_safepoint": {
		PromQL:  `max(einsteindb_gcworker_autogc_safe_point{$LABEL_CONDITIONS}) by (instance) / (2^18)`,
		Labels:  []string{"instance"},
		Comment: "SafePoint used for EinsteinDB's Auto GC",
	},
	"einsteindb_send_snapshot_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_server_send_snapshot_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed when sending snapshots",
	},
	"einsteindb_handle_snapshot_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_raftstore_snapshot_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,type))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed when handling snapshots",
	},
	"einsteindb_snapshot_state_count": {
		PromQL:  `sum(einsteindb_raftstore_snapshot_traffic_total{$LABEL_CONDITIONS}) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of snapshots in different states",
	},
	"einsteindb_snapshot_state_total_count": {
		PromQL:  `sum(delta(einsteindb_raftstore_snapshot_traffic_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The total number of snapshots in different states",
	},
	"einsteindb_snapshot_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_snapshot_size_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.9999,
		Comment:  "The quantile of snapshot size",
	},
	"einsteindb_snapshot_ekv_count": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_snapshot_ekv_count_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.9999,
		Comment:  "The quantile of number of KV within a snapshot",
	},
	"einsteindb_worker_handled_tasks": {
		PromQL:  `sum(rate(einsteindb_worker_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "The number of tasks handled by worker",
	},
	"einsteindb_worker_handled_tasks_total_num": {
		PromQL:  `sum(increase(einsteindb_worker_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Total number of tasks handled by worker",
	},
	"einsteindb_worker_pending_tasks": {
		PromQL:  `sum(rate(einsteindb_worker_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Current pending and running tasks of worker",
	},
	"einsteindb_worker_pending_tasks_total_num": {
		PromQL:  `sum(increase(einsteindb_worker_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Total pending and running tasks of worker",
	},
	"einsteindb_futurepool_handled_tasks": {
		PromQL:  `sum(rate(einsteindb_futurepool_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "The number of tasks handled by future_pool",
	},
	"einsteindb_futurepool_handled_tasks_total_num": {
		PromQL:  `sum(increase(einsteindb_futurepool_handled_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Total number of tasks handled by future_pool",
	},
	"einsteindb_futurepool_pending_tasks": {
		PromQL:  `sum(rate(einsteindb_futurepool_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Current pending and running tasks of future_pool",
	},
	"einsteindb_futurepool_pending_tasks_total_num": {
		PromQL:  `sum(increase(einsteindb_futurepool_pending_task_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,instance)`,
		Labels:  []string{"instance", "name"},
		Comment: "Total pending and running tasks of future_pool",
	},
	"einsteindb_cop_request_durations": {
		PromQL:  `sum(rate(einsteindb_coprocessor_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,req)`,
		Labels:  []string{"instance", "req"},
		Comment: "The time consumed to handle interlock read requests",
	},
	"einsteindb_cop_request_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_coprocessor_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance))`,
		Labels:   []string{"instance", "req"},
		Quantile: 1,
		Comment:  "The quantile of time consumed to handle interlock read requests",
	},
	"einsteindb_cop_requests_ops": {
		PromQL: `sum(rate(einsteindb_coprocessor_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (req,instance)`,
		Labels: []string{"instance", "req"},
	},
	"einsteindb_cop_scan_keys_num": {
		PromQL: `sum(rate(einsteindb_coprocessor_scan_keys_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (req,instance)`,
		Labels: []string{"instance", "req"},
	},
	"einsteindb_cop_ekv_cursor_operations": {
		PromQL:   `histogram_quantile($QUANTILE, avg(rate(einsteindb_coprocessor_scan_keys_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance)) `,
		Labels:   []string{"instance", "req"},
		Quantile: 1,
	},
	"einsteindb_cop_total_lmdb_perf_statistics": {
		PromQL: `sum(rate(einsteindb_coprocessor_lmdb_perf{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (req,metric,instance)`,
		Labels: []string{"instance", "req", "metric"},
	},
	"einsteindb_cop_total_response_size_per_seconds": {
		PromQL: `sum(rate(einsteindb_coprocessor_response_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels: []string{"instance"},
	},
	"einsteindb_cop_handle_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_coprocessor_request_handle_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,instance))`,
		Labels:   []string{"instance", "req"},
		Quantile: 1,
		Comment:  "The quantile of time consumed when handling interlock requests",
	},
	"einsteindb_cop_wait_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_coprocessor_request_wait_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,req,type,instance))`,
		Labels:   []string{"instance", "req", "type"},
		Quantile: 1,
		Comment:  "The quantile of time consumed when interlock requests are wait for being handled",
	},
	"einsteindb_cop_posetPosetDag_requests_ops": {
		PromQL: `sum(rate(einsteindb_coprocessor_posetPosetDag_request_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (vec_type,instance)`,
		Labels: []string{"instance", "vec_type"},
	},
	"einsteindb_cop_posetPosetDag_interlocks_ops": {
		PromQL:  `sum(rate(einsteindb_coprocessor_interlock_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The number of PosetDag interlocks per seconds",
	},
	"einsteindb_cop_scan_details": {
		PromQL: `sum(rate(einsteindb_coprocessor_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,req,cf,instance)`,
		Labels: []string{"instance", "tag", "req", "cf"},
	},
	"einsteindb_cop_scan_details_total": {
		PromQL: `sum(increase(einsteindb_coprocessor_scan_details{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (tag,req,cf,instance)`,
		Labels: []string{"instance", "tag", "req", "cf"},
	},

	"einsteindb_threads_state": {
		PromQL: `sum(einsteindb_threads_state{$LABEL_CONDITIONS}) by (instance,state)`,
		Labels: []string{"instance", "state"},
	},
	"einsteindb_threads_io": {
		PromQL: `sum(rate(einsteindb_threads_io_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (name,io,instance)`,
		Labels: []string{"instance", "io", "name"},
	},
	"einsteindb_thread_voluntary_context_switches": {
		PromQL: `sum(rate(einsteindb_thread_voluntary_context_switches{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, name)`,
		Labels: []string{"instance", "name"},
	},
	"einsteindb_thread_nonvoluntary_context_switches": {
		PromQL: `sum(rate(einsteindb_thread_nonvoluntary_context_switches{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, name)`,
		Labels: []string{"instance", "name"},
	},
	"einsteindb_engine_get_cpu_cache_operations": {
		PromQL:  `sum(rate(einsteindb_engine_get_served{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The count of get l0,l1,l2 operations",
	},
	"einsteindb_engine_get_block_cache_operations": {
		PromQL:  `sum(rate(einsteindb_engine_cache_efficiency{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The count of get memblock operations",
	},

	"einsteindb_engine_get_memblock_operations": {
		PromQL:  `sum(rate(einsteindb_engine_memblock_efficiency{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The count of get memblock operations",
	},
	"einsteindb_engine_max_get_duration": {
		PromQL:  `max(einsteindb_engine_get_micro_seconds{$LABEL_CONDITIONS}) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The max time consumed when executing get operations, the unit is microsecond",
	},
	"einsteindb_engine_avg_get_duration": {
		PromQL:  `avg(einsteindb_engine_get_micro_seconds{$LABEL_CONDITIONS}) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The average time consumed when executing get operations, the unit is microsecond",
	},
	"einsteindb_engine_seek_operations": {
		PromQL:  `sum(rate(einsteindb_engine_locate{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The count of seek operations",
	},
	"einsteindb_engine_max_seek_duration": {
		PromQL:  `max(einsteindb_engine_seek_micro_seconds{$LABEL_CONDITIONS}) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The time consumed when executing seek operation, the unit is microsecond",
	},
	"einsteindb_engine_avg_seek_duration": {
		PromQL:  `avg(einsteindb_engine_seek_micro_seconds{$LABEL_CONDITIONS}) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The time consumed when executing seek operation, the unit is microsecond",
	},
	"einsteindb_engine_write_operations": {
		PromQL:  `sum(rate(einsteindb_engine_write_served{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The count of write operations",
	},
	"einsteindb_engine_write_duration": {
		PromQL:  `avg(einsteindb_engine_write_micro_seconds{$LABEL_CONDITIONS}) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The time consumed when executing write operation, the unit is microsecond",
	},
	"einsteindb_engine_write_max_duration": {
		PromQL:  `max(einsteindb_engine_write_micro_seconds{$LABEL_CONDITIONS}) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The time consumed when executing write operation, the unit is microsecond",
	},
	"einsteindb_engine_wal_sync_operations": {
		PromQL:  `sum(rate(einsteindb_engine_wal_file_synced{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,instance)`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The count of WAL sync operations",
	},

	"einsteindb_wal_sync_max_duration": {
		PromQL:  `max(einsteindb_engine_wal_file_sync_micro_seconds{$LABEL_CONDITIONS}) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The max time consumed when executing WAL sync operation, the unit is microsecond",
	},
	"einsteindb_wal_sync_duration": {
		PromQL:  `avg(einsteindb_engine_wal_file_sync_micro_seconds{$LABEL_CONDITIONS}) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type"},
		Comment: "The time consumed when executing WAL sync operation, the unit is microsecond",
	},
	"einsteindb_compaction_operations": {
		PromQL:  `sum(rate(einsteindb_engine_event_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The count of compaction and flush operations",
	},
	"einsteindb_compaction_max_duration": {
		PromQL:  `max(einsteindb_engine_compaction_time{$LABEL_CONDITIONS}) by (type,instance,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The time consumed when executing the compaction and flush operations",
	},
	"einsteindb_compaction_duration": {
		PromQL:  `avg(einsteindb_engine_compaction_time{$LABEL_CONDITIONS}) by (type,instance,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The time consumed when executing the compaction and flush operations",
	},
	"einsteindb_sst_read_max_duration": {
		PromQL:  `max(einsteindb_engine_sst_read_micros{$LABEL_CONDITIONS}) by (type,instance,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The max time consumed when reading SST files",
	},
	"einsteindb_sst_read_duration": {
		PromQL:  `avg(einsteindb_engine_sst_read_micros{$LABEL_CONDITIONS}) by (type,instance,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The time consumed when reading SST files",
	},
	"einsteindb_write_stall_max_duration": {
		PromQL:  `max(einsteindb_engine_write_stall{$LABEL_CONDITIONS}) by (type,instance,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The time which is caused by write stall",
	},
	"einsteindb_write_stall_avg_duration": {
		PromQL:  `avg(einsteindb_engine_write_stall{$LABEL_CONDITIONS}) by (type,instance,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The time which is caused by write stall",
	},
	"einsteindb_memblock_size": {
		PromQL:  `avg(einsteindb_engine_memory_bytes{$LABEL_CONDITIONS}) by (type,instance,EDB,cf)`,
		Labels:  []string{"instance", "cf", "type", "EDB"},
		Comment: "The memblock size of each defCausumn family",
	},
	"einsteindb_memblock_hit": {
		PromQL:  `sum(rate(einsteindb_engine_memblock_efficiency{type="memblock_hit"}[$RANGE_DURATION])) by (instance,EDB) / (sum(rate(einsteindb_engine_memblock_efficiency{}[$RANGE_DURATION])) by (instance,EDB) + sum(rate(einsteindb_engine_memblock_efficiency{}[$RANGE_DURATION])) by (instance,EDB))`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The hit rate of memblock",
	},
	"einsteindb_block_cache_size": {
		PromQL:  `topk(20, avg(einsteindb_engine_block_cache_size_bytes{$LABEL_CONDITIONS}) by(cf, instance, EDB))`,
		Labels:  []string{"instance", "cf", "EDB"},
		Comment: "The causet cache size. Broken down by defCausumn family if shared causet cache is disabled.",
	},
	"einsteindb_block_all_cache_hit": {
		PromQL:  `sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_hit"}[$RANGE_DURATION])) by (EDB,instance) / (sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_hit"}[$RANGE_DURATION])) by (EDB,instance) + sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_miss"}[$RANGE_DURATION])) by (EDB,instance))`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The hit rate of all causet cache",
	},
	"einsteindb_block_data_cache_hit": {
		PromQL:  `sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_data_hit"}[$RANGE_DURATION])) by (EDB,instance) / (sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_data_hit"}[$RANGE_DURATION])) by (EDB,instance) + sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_data_miss"}[$RANGE_DURATION])) by (EDB,instance))`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The hit rate of data causet cache",
	},
	"einsteindb_block_filter_cache_hit": {
		PromQL:  `sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_filter_hit"}[$RANGE_DURATION])) by (EDB,instance) / (sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_filter_hit"}[$RANGE_DURATION])) by (EDB,instance) + sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_filter_miss"}[$RANGE_DURATION])) by (EDB,instance))`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The hit rate of data causet cache",
	},
	"einsteindb_block_index_cache_hit": {
		PromQL:  `sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_index_hit"}[$RANGE_DURATION])) by (EDB,instance) / (sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_index_hit"}[$RANGE_DURATION])) by (EDB,instance) + sum(rate(einsteindb_engine_cache_efficiency{type="block_cache_index_miss"}[$RANGE_DURATION])) by (EDB,instance))`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The hit rate of data causet cache",
	},
	"einsteindb_block_bloom_prefix_cache_hit": {
		PromQL:  `sum(rate(einsteindb_engine_bloom_efficiency{type="bloom_prefix_useful"}[$RANGE_DURATION])) by (EDB,instance) / sum(rate(einsteindb_engine_bloom_efficiency{type="bloom_prefix_checked"}[$RANGE_DURATION])) by (EDB,instance)`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The hit rate of data causet cache",
	},
	"einsteindb_corrrput_keys_flow": {
		PromQL:  `sum(rate(einsteindb_engine_compaction_num_corrupt_keys{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,cf,instance)`,
		Labels:  []string{"instance", "EDB", "cf"},
		Comment: "The flow of corrupt operations on keys",
	},
	"einsteindb_total_keys": {
		PromQL:  `sum(einsteindb_engine_estimate_num_keys{$LABEL_CONDITIONS}) by (EDB,cf,instance)`,
		Labels:  []string{"instance", "EDB", "cf"},
		Comment: "The count of keys in each defCausumn family",
	},
	"einsteindb_per_read_max_bytes": {
		PromQL:  `max(einsteindb_engine_bytes_per_read{$LABEL_CONDITIONS}) by (type,EDB,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The max bytes per read",
	},
	"einsteindb_per_read_avg_bytes": {
		PromQL:  `avg(einsteindb_engine_bytes_per_read{$LABEL_CONDITIONS}) by (type,EDB,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The avg bytes per read",
	},
	"einsteindb_per_write_max_bytes": {
		PromQL:  `max(einsteindb_engine_bytes_per_write{$LABEL_CONDITIONS}) by (type,EDB,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The max bytes per write",
	},
	"einsteindb_per_write_avg_bytes": {
		PromQL:  `avg(einsteindb_engine_bytes_per_write{$LABEL_CONDITIONS}) by (type,EDB,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The avg bytes per write",
	},
	"einsteindb_engine_compaction_flow_bytes": {
		PromQL:  `sum(rate(einsteindb_engine_compaction_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "The flow rate of compaction operations per type",
	},
	"einsteindb_compaction_pending_bytes": {
		PromQL:  `sum(rate(einsteindb_engine_pending_compaction_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (cf,instance,EDB)`,
		Labels:  []string{"instance", "cf", "EDB"},
		Comment: "The pending bytes to be compacted",
	},
	"einsteindb_read_amplication": {
		PromQL:  `sum(rate(einsteindb_engine_read_amp_flow_bytes{type="read_amp_total_read_bytes"}[$RANGE_DURATION])) by (instance,EDB) / sum(rate(einsteindb_engine_read_amp_flow_bytes{type="read_amp_estimate_useful_bytes"}[$RANGE_DURATION])) by (instance,EDB)`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The read amplification per EinsteinDB instance",
	},
	"einsteindb_compression_ratio": {
		PromQL:  `avg(einsteindb_engine_compression_ratio{$LABEL_CONDITIONS}) by (level,instance,EDB)`,
		Labels:  []string{"instance", "level", "EDB"},
		Comment: "The compression ratio of each level",
	},
	"einsteindb_number_of_snapshots": {
		PromQL:  `einsteindb_engine_num_snapshots{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The number of snapshot of each EinsteinDB instance",
	},
	"einsteindb_oldest_snapshots_duration": {
		PromQL:  `einsteindb_engine_oldest_snapshot_duration{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "EDB"},
		Comment: "The time that the oldest unreleased snapshot survivals",
	},
	"einsteindb_number_files_at_each_level": {
		PromQL:  `avg(einsteindb_engine_num_files_at_level{$LABEL_CONDITIONS}) by (cf, level,EDB,instance)`,
		Labels:  []string{"instance", "cf", "level", "EDB"},
		Comment: "The number of SST files for different defCausumn families in each level",
	},
	"einsteindb_ingest_sst_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_snapshot_ingest_sst_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance,EDB))`,
		Labels:   []string{"instance", "EDB"},
		Quantile: 0.99,
		Comment:  "The quantile of time consumed when ingesting SST files",
	},
	"einsteindb_ingest_sst_avg_duration": {
		PromQL:  `sum(rate(einsteindb_snapshot_ingest_sst_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_snapshot_ingest_sst_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance"},
		Comment: "The average time consumed when ingesting SST files",
	},
	"einsteindb_stall_conditions_changed_of_each_cf": {
		PromQL:  `einsteindb_engine_stall_conditions_changed{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "cf", "type", "EDB"},
		Comment: "Stall conditions changed of each defCausumn family",
	},
	"einsteindb_write_stall_reason": {
		PromQL: `sum(increase(einsteindb_engine_write_stall_reason{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,type,instance)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_compaction_reason": {
		PromQL: `sum(rate(einsteindb_engine_compaction_reason{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,cf,reason,instance)`,
		Labels: []string{"instance", "cf", "reason", "EDB"},
	},
	"einsteindb_engine_blob_key_max_size": {
		PromQL: `max(einsteindb_engine_blob_key_size{$LABEL_CONDITIONS}) by (EDB,instance,type)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_blob_key_avg_size": {
		PromQL: `avg(einsteindb_engine_blob_key_size{$LABEL_CONDITIONS}) by (EDB,instance,type)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_blob_value_avg_size": {
		PromQL: `avg(einsteindb_engine_blob_value_size{$LABEL_CONDITIONS}) by (EDB,instance,type)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_blob_value_max_size": {
		PromQL: `max(einsteindb_engine_blob_value_size{$LABEL_CONDITIONS}) by (EDB,instance,type)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_blob_seek_duration": {
		PromQL:  `avg(einsteindb_engine_blob_seek_micros_seconds{$LABEL_CONDITIONS}) by (EDB,type,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "the unit is microsecond",
	},
	"einsteindb_engine_blob_seek_operations": {
		PromQL: `sum(rate(einsteindb_engine_blob_locate{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (EDB,type,instance)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_blob_get_duration": {
		PromQL:  `avg(einsteindb_engine_blob_get_micros_seconds{$LABEL_CONDITIONS}) by (type,EDB,instance)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "the unit is microsecond",
	},
	"einsteindb_engine_blob_bytes_flow": {
		PromQL: `sum(rate(einsteindb_engine_blob_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,EDB)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_blob_file_read_duration": {
		PromQL:  `avg(einsteindb_engine_blob_file_read_micros_seconds{$LABEL_CONDITIONS}) by (type,instance,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "the unit is microsecond",
	},
	"einsteindb_engine_blob_file_write_duration": {
		PromQL:  `avg(einsteindb_engine_blob_file_write_micros_seconds{$LABEL_CONDITIONS}) by (type,instance,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "the unit is microsecond",
	},
	"einsteindb_engine_blob_file_sync_operations": {
		PromQL: `sum(rate(einsteindb_engine_blob_file_synced{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"einsteindb_engine_blob_file_sync_duration": {
		PromQL:  `avg(einsteindb_engine_blob_file_sync_micros_seconds{$LABEL_CONDITIONS}) by (instance,type,EDB)`,
		Labels:  []string{"instance", "type", "EDB"},
		Comment: "the unit is microsecond",
	},
	"einsteindb_engine_blob_file_count": {
		PromQL: `avg(einsteindb_engine_titandb_num_obsolete_blob_file{$LABEL_CONDITIONS}) by (instance,EDB)`,
		Labels: []string{"instance", "EDB"},
	},
	"einsteindb_engine_blob_file_size": {
		PromQL: `avg(einsteindb_engine_titandb_obsolete_blob_file_size{$LABEL_CONDITIONS}) by (instance,EDB)`,
		Labels: []string{"instance", "EDB"},
	},
	"einsteindb_engine_blob_gc_file": {
		PromQL: `sum(rate(einsteindb_engine_blob_gc_file_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,EDB)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_blob_gc_duration": {
		PromQL: `avg(einsteindb_engine_blob_gc_micros_seconds{$LABEL_CONDITIONS}) by (EDB,instance,type)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_blob_gc_bytes_flow": {
		PromQL: `sum(rate(einsteindb_engine_blob_gc_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,EDB)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_blob_gc_keys_flow": {
		PromQL: `sum(rate(einsteindb_engine_blob_gc_flow_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance,EDB)`,
		Labels: []string{"instance", "type", "EDB"},
	},
	"einsteindb_engine_live_blob_size": {
		PromQL: `avg(einsteindb_engine_titandb_live_blob_size{$LABEL_CONDITIONS}) by (instance,EDB)`,
		Labels: []string{"instance", "EDB"},
	},

	"einsteindb_lock_manager_handled_tasks": {
		PromQL: `sum(rate(einsteindb_lock_manager_task_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"einsteindb_lock_manager_waiter_lifetime_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_lock_manager_waiter_lifetime_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"einsteindb_lock_manager_waiter_lifetime_avg_duration": {
		PromQL: `sum(rate(einsteindb_lock_manager_waiter_lifetime_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_lock_manager_waiter_lifetime_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance", "type"},
	},
	"einsteindb_lock_manager_wait_block": {
		PromQL: `sum(max_over_time(einsteindb_lock_manager_wait_block_status{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"einsteindb_lock_manager_deadlock_detect_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_lock_manager_detect_duration_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"einsteindb_lock_manager_deadlock_detect_avg_duration": {
		PromQL: `sum(rate(einsteindb_lock_manager_detect_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_lock_manager_detect_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},
	"einsteindb_lock_manager_detect_error": {
		PromQL: `sum(rate(einsteindb_lock_manager_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"einsteindb_lock_manager_detect_error_total_count": {
		PromQL: `sum(increase(einsteindb_lock_manager_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"einsteindb_lock_manager_deadlock_detector_leader": {
		PromQL: `sum(max_over_time(einsteindb_lock_manager_detector_leader_heartbeat{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels: []string{"instance"},
	},
	"einsteindb_allocator_stats": {
		PromQL: `einsteindb_allocator_stats{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "type"},
	},
	"einsteindb_backup_range_size": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_backup_range_size_bytes_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,cf,instance))`,
		Labels:   []string{"instance", "cf"},
		Quantile: 0.99,
	},
	"einsteindb_backup_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_backup_request_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))`,
		Labels:   []string{"instance"},
		Quantile: 0.99,
	},
	"einsteindb_backup_avg_duration": {
		PromQL: `sum(rate(einsteindb_backup_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) / sum(rate(einsteindb_backup_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels: []string{"instance"},
	},

	"einsteindb_backup_flow": {
		PromQL: `sum(rate(einsteindb_backup_range_size_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels: []string{"instance"},
	},
	"einsteindb_disk_read_bytes": {
		PromQL: `sum(irate(node_disk_read_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,device)`,
		Labels: []string{"instance", "device"},
	},
	"einsteindb_disk_write_bytes": {
		PromQL: `sum(irate(node_disk_written_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,device)`,
		Labels: []string{"instance", "device"},
	},
	"einsteindb_backup_range_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(einsteindb_backup_range_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.99,
	},
	"einsteindb_backup_range_avg_duration": {
		PromQL: `sum(rate(einsteindb_backup_range_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance) / sum(rate(einsteindb_backup_range_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type,instance)`,
		Labels: []string{"instance", "type"},
	},
	"einsteindb_backup_errors": {
		PromQL: `rate(einsteindb_backup_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "error"},
	},
	"einsteindb_backup_errors_total_count": {
		PromQL: `sum(increase(einsteindb_backup_error_counter{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,error)`,
		Labels: []string{"instance", "error"},
	},
	"node_virtual_cpus": {
		PromQL:  `count(node_cpu_seconds_total{mode="user"}) by (instance)`,
		Labels:  []string{"instance"},
		Comment: "node virtual cpu count",
	},
	"node_total_memory": {
		PromQL:  `node_memory_MemTotal_bytes{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "total memory in node",
	},
	"node_memory_available": {
		PromQL: `node_memory_MemAvailable_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_usage": {
		PromQL: `100* (1-(node_memory_MemAvailable_bytes{$LABEL_CONDITIONS}/node_memory_MemTotal_bytes{$LABEL_CONDITIONS}))`,
		Labels: []string{"instance"},
	},
	"node_memory_swap_used": {
		PromQL:  `node_memory_SwapTotal_bytes{$LABEL_CONDITIONS} - node_memory_SwapFree_bytes{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "bytes used of node swap memory",
	},
	"node_uptime": {
		PromQL:  `node_time_seconds{$LABEL_CONDITIONS} - node_boot_time_seconds{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "node uptime, units are seconds",
	},
	"node_load1": {
		PromQL:  `node_load1{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "1 minute load averages in node",
	},
	"node_load5": {
		PromQL:  `node_load5{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "5 minutes load averages in node",
	},
	"node_load15": {
		PromQL:  `node_load15{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance"},
		Comment: "15 minutes load averages in node",
	},
	"node_kernel_interrupts": {
		PromQL: `rate(node_intr_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_intr_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance"},
	},
	"node_kernel_forks": {
		PromQL: `rate(node_forks_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_forks_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance"},
	},
	"node_kernel_context_switches": {
		PromQL: `rate(node_context_switches_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_context_switches_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance"},
	},
	"node_cpu_usage": {
		PromQL: `sum(rate(node_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (mode,instance) * 100 / count(node_cpu_seconds_total{$LABEL_CONDITIONS}) by (mode,instance) or sum(irate(node_cpu_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (mode,instance) * 100 / count(node_cpu_seconds_total{$LABEL_CONDITIONS}) by (mode,instance)`,
		Labels: []string{"instance", "mode"},
	},
	"node_memory_free": {
		PromQL: `node_memory_MemFree_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_buffers": {
		PromQL: `node_memory_Buffers_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_cached": {
		PromQL: `node_memory_Cached_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_active": {
		PromQL: `node_memory_Active_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_inactive": {
		PromQL: `node_memory_Inactive_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_writeback": {
		PromQL: `node_memory_Writeback_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_writeback_tmp": {
		PromQL: `node_memory_WritebackTmp_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_dirty": {
		PromQL: `node_memory_Dirty_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_shared": {
		PromQL: `node_memory_Shmem_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_memory_mapped": {
		PromQL: `node_memory_Mapped_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_disk_size": {
		PromQL: `node_filesystem_size_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "device", "fstype", "mountpoint"},
	},
	"node_disk_available_size": {
		PromQL: `node_filesystem_avail_bytes{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "device", "fstype", "mountpoint"},
	},
	"node_disk_state": {
		PromQL: `node_filesystem_readonly{$LABEL_CONDITIONS}`,
		Labels: []string{"instance", "device", "fstype", "mountpoint"},
	},
	"node_disk_io_util": {
		PromQL: `rate(node_disk_io_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_disk_io_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_disk_iops": {
		PromQL: `sum(rate(node_disk_reads_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) + rate(node_disk_writes_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,device)`,
		Labels: []string{"instance", "device"},
	},
	"node_disk_write_latency": {
		PromQL:  `(rate(node_disk_write_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])/ rate(node_disk_writes_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance", "device"},
		Comment: "node disk write latency",
	},
	"node_disk_read_latency": {
		PromQL:  `(rate(node_disk_read_time_seconds_total{$LABEL_CONDITIONS}[$RANGE_DURATION])/ rate(node_disk_reads_completed_total{$LABEL_CONDITIONS}[$RANGE_DURATION]))`,
		Labels:  []string{"instance", "device"},
		Comment: "node disk read latency",
	},
	"node_disk_throughput": {
		PromQL:  `irate(node_disk_read_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) + irate(node_disk_written_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels:  []string{"instance", "device"},
		Comment: "Units is byte",
	},
	"node_disk_usage": {
		PromQL:  `((node_filesystem_size_bytes{$LABEL_CONDITIONS} - node_filesystem_avail_bytes{$LABEL_CONDITIONS}) / node_filesystem_size_bytes{$LABEL_CONDITIONS}) * 100`,
		Labels:  []string{"instance", "device"},
		Comment: "Filesystem used space. If is > 80% then is Critical.",
	},
	"node_file_descriptor_allocated": {
		PromQL: `node_filefd_allocated{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_network_in_drops": {
		PromQL: `rate(node_network_receive_drop_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) `,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_drops": {
		PromQL: `rate(node_network_transmit_drop_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_in_errors": {
		PromQL: `rate(node_network_receive_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_in_errors_total_count": {
		PromQL: `sum(increase(node_network_receive_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by(instance, device)`,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_errors": {
		PromQL: `rate(node_network_transmit_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_errors_total_count": {
		PromQL: `sum(increase(node_network_transmit_errs_total{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance, device)`,
		Labels: []string{"instance", "device"},
	},
	"node_network_in_traffic": {
		PromQL: `rate(node_network_receive_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_receive_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_traffic": {
		PromQL: `rate(node_network_transmit_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_transmit_bytes_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_in_packets": {
		PromQL: `rate(node_network_receive_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_receive_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_out_packets": {
		PromQL: `rate(node_network_transmit_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_network_transmit_packets_total{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance", "device"},
	},
	"node_network_interface_speed": {
		PromQL:  `node_network_transmit_queue_length{$LABEL_CONDITIONS}`,
		Labels:  []string{"instance", "device"},
		Comment: "node_network_transmit_queue_length = transmit_queue_length value of /sys/class/net/<iface>.",
	},
	"node_network_utilization_in_hourly": {
		PromQL: `sum(increase(node_network_receive_bytes_total{$LABEL_CONDITIONS}[1h]))`,
		Labels: []string{"instance", "device"},
	},
	"node_network_utilization_out_hourly": {
		PromQL: `sum(increase(node_network_transmit_bytes_total{$LABEL_CONDITIONS}[1h]))`,
		Labels: []string{"instance", "device"},
	},
	"node_tcp_in_use": {
		PromQL: `node_sockstat_TCP_inuse{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_tcp_segments_retransmitted": {
		PromQL: `rate(node_netstat_Tcp_RetransSegs{$LABEL_CONDITIONS}[$RANGE_DURATION]) or irate(node_netstat_Tcp_RetransSegs{$LABEL_CONDITIONS}[$RANGE_DURATION])`,
		Labels: []string{"instance"},
	},
	"node_tcp_connections": {
		PromQL: `node_netstat_Tcp_CurrEstab{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_processes_running": {
		PromQL: `node_procs_running{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},
	"node_processes_blocked": {
		PromQL: `node_procs_blocked{$LABEL_CONDITIONS}`,
		Labels: []string{"instance"},
	},

	"etcd_wal_fsync_total_count": {
		PromQL:  "sum(increase(etcd_disk_wal_fsync_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of writing WAL into the persistent storage",
	},
	"etcd_wal_fsync_total_time": {
		PromQL:  "sum(increase(etcd_disk_wal_fsync_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of writing WAL into the persistent storage",
	},
	"FIDel_client_cmd_total_count": {
		PromQL:  "sum(increase(FIDel_client_cmd_handle_cmds_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of fidel client command durations",
	},
	"FIDel_client_cmd_total_time": {
		PromQL:  "sum(increase(FIDel_client_cmd_handle_cmds_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of fidel client command durations",
	},
	"FIDel_grpc_completed_commands_total_count": {
		PromQL:  "sum(increase(grpc_server_handling_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,grpc_method)",
		Labels:  []string{"instance", "grpc_method"},
		Comment: "The total count of completing each HoTT of gRPC commands",
	},
	"FIDel_grpc_completed_commands_total_time": {
		PromQL:  "sum(increase(grpc_server_handling_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,grpc_method)",
		Labels:  []string{"instance", "grpc_method"},
		Comment: "The total time of completing each HoTT of gRPC commands",
	},
	"FIDel_request_rpc_total_count": {
		PromQL:  "sum(increase(FIDel_client_request_handle_requests_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of fidel client handle request duration(second)",
	},
	"FIDel_request_rpc_total_time": {
		PromQL:  "sum(increase(FIDel_client_request_handle_requests_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of fidel client handle request duration(second)",
	},
	"FIDel_handle_transactions_total_count": {
		PromQL:  "sum(increase(FIDel_txn_handle_txns_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
		Labels:  []string{"instance", "result"},
		Comment: "The total count of handling etcd transactions",
	},
	"FIDel_handle_transactions_total_time": {
		PromQL:  "sum(increase(FIDel_txn_handle_txns_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
		Labels:  []string{"instance", "result"},
		Comment: "The total time of handling etcd transactions",
	},
	"FIDel_operator_finish_total_count": {
		PromQL:  "sum(increase(FIDel_schedule_finish_operators_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
		Labels:  []string{"type"},
		Comment: "The total count of the operator is finished",
	},
	"FIDel_operator_finish_total_time": {
		PromQL:  "sum(increase(FIDel_schedule_finish_operators_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
		Labels:  []string{"type"},
		Comment: "The total time consumed when the operator is finished",
	},
	"FIDel_operator_step_finish_total_count": {
		PromQL:  "sum(increase(FIDel_schedule_finish_operator_steps_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
		Labels:  []string{"type"},
		Comment: "The total count of the operator step is finished",
	},
	"FIDel_operator_step_finish_total_time": {
		PromQL:  "sum(increase(FIDel_schedule_finish_operator_steps_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (type)",
		Labels:  []string{"type"},
		Comment: "The total time consumed when the operator step is finished",
	},
	"FIDel_peer_round_trip_total_count": {
		PromQL:  "sum(increase(etcd_network_peer_round_trip_time_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,To)",
		Labels:  []string{"instance", "To"},
		Comment: "The total count of the network in .99",
	},
	"FIDel_peer_round_trip_total_time": {
		PromQL:  "sum(increase(etcd_network_peer_round_trip_time_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,To)",
		Labels:  []string{"instance", "To"},
		Comment: "The total time of latency of the network in .99",
	},
	"FIDel_region_heartbeat_total_count": {
		PromQL:  "sum(increase(FIDel_scheduler_region_heartbeat_latency_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,causetstore)",
		Labels:  []string{"address", "causetstore"},
		Comment: "The total count of heartbeat latency of each EinsteinDB instance in",
	},
	"FIDel_region_heartbeat_total_time": {
		PromQL:  "sum(increase(FIDel_scheduler_region_heartbeat_latency_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (address,causetstore)",
		Labels:  []string{"address", "causetstore"},
		Comment: "The total time of heartbeat latency of each EinsteinDB instance in",
	},
	"FIDel_start_tso_wait_total_count": {
		PromQL:  "sum(increase(milevadb_FIDelclient_ts_future_wait_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of the waiting for getting the start timestamp oracle",
	},
	"FIDel_start_tso_wait_total_time": {
		PromQL:  "sum(increase(milevadb_FIDelclient_ts_future_wait_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of duration of the waiting time for getting the start timestamp oracle",
	},
	"FIDel_tso_rpc_total_count": {
		PromQL:  "sum(increase(FIDel_client_request_handle_requests_duration_seconds_count{type=\"tso\"}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of a client sending TSO request until received the response.",
	},
	"FIDel_tso_rpc_total_time": {
		PromQL:  "sum(increase(FIDel_client_request_handle_requests_duration_seconds_sum{type=\"tso\"}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of a client sending TSO request until received the response.",
	},
	"FIDel_tso_wait_total_count": {
		PromQL:  "sum(increase(FIDel_client_cmd_handle_cmds_duration_seconds_count{type=\"wait\"}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of a client starting to wait for the TS until received the TS result.",
	},
	"FIDel_tso_wait_total_time": {
		PromQL:  "sum(increase(FIDel_client_cmd_handle_cmds_duration_seconds_sum{type=\"wait\"}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of a client starting to wait for the TS until received the TS result.",
	},
	"milevadb_auto_id_request_total_count": {
		PromQL:  "sum(increase(milevadb_autoid_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of MilevaDB auto id requests durations",
	},
	"milevadb_auto_id_request_total_time": {
		PromQL:  "sum(increase(milevadb_autoid_operation_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of MilevaDB auto id requests durations",
	},
	"milevadb_batch_client_unavailable_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_batch_client_unavailable_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of ekv storage batch processing unvailable durations",
	},
	"milevadb_batch_client_unavailable_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_batch_client_unavailable_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of ekv storage batch processing unvailable durations",
	},
	"milevadb_batch_client_wait_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_batch_wait_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of ekv storage batch processing durations",
	},
	"milevadb_batch_client_wait_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_batch_wait_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of ekv storage batch processing durations",
	},
	"milevadb_compile_total_count": {
		PromQL:  "sum(increase(milevadb_stochastik_compile_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of building the query plan(second)",
	},
	"milevadb_compile_total_time": {
		PromQL:  "sum(increase(milevadb_stochastik_compile_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total time of cost of building the query plan(second)",
	},
	"milevadb_cop_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_cop_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of ekv storage interlock processing durations",
	},
	"milevadb_cop_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_cop_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of ekv storage interlock processing durations",
	},
	"milevadb_dbs_batch_add_index_total_count": {
		PromQL:  "sum(increase(milevadb_dbs_batch_add_idx_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of MilevaDB batch add index durations by histogram buckets",
	},
	"milevadb_dbs_batch_add_index_total_time": {
		PromQL:  "sum(increase(milevadb_dbs_batch_add_idx_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of MilevaDB batch add index durations by histogram buckets",
	},
	"milevadb_dbs_deploy_syncer_total_count": {
		PromQL:  "sum(increase(milevadb_dbs_deploy_syncer_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total count of MilevaDB dbs schemaReplicant syncer statistics, including init, start, watch, clear function call",
	},
	"milevadb_dbs_deploy_syncer_total_time": {
		PromQL:  "sum(increase(milevadb_dbs_deploy_syncer_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total time of MilevaDB dbs schemaReplicant syncer statistics, including init, start, watch, clear function call time cost",
	},
	"milevadb_dbs_total_count": {
		PromQL:  "sum(increase(milevadb_dbs_handle_job_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of MilevaDB DBS duration statistics",
	},
	"milevadb_dbs_total_time": {
		PromQL:  "sum(increase(milevadb_dbs_handle_job_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of MilevaDB DBS duration statistics",
	},
	"milevadb_dbs_uFIDelate_self_version_total_count": {
		PromQL:  "sum(increase(milevadb_dbs_uFIDelate_self_ver_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
		Labels:  []string{"instance", "result"},
		Comment: "The total count of MilevaDB schemaReplicant syncer version uFIDelate",
	},
	"milevadb_dbs_uFIDelate_self_version_total_time": {
		PromQL:  "sum(increase(milevadb_dbs_uFIDelate_self_ver_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,result)",
		Labels:  []string{"instance", "result"},
		Comment: "The total time of MilevaDB schemaReplicant syncer version uFIDelate time duration",
	},
	"milevadb_dbs_worker_total_count": {
		PromQL:  "sum(increase(milevadb_dbs_worker_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result,action)",
		Labels:  []string{"instance", "type", "result", "action"},
		Comment: "The total count of MilevaDB dbs worker duration",
	},
	"milevadb_dbs_worker_total_time": {
		PromQL:  "sum(increase(milevadb_dbs_worker_operation_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result,action)",
		Labels:  []string{"instance", "type", "result", "action"},
		Comment: "The total time of MilevaDB dbs worker duration",
	},
	"milevadb_allegrosql_execution_total_count": {
		PromQL:  "sum(increase(milevadb_allegrosql_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of allegrosql execution(second)",
	},
	"milevadb_allegrosql_execution_total_time": {
		PromQL:  "sum(increase(milevadb_allegrosql_handle_query_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of allegrosql execution(second)",
	},
	"milevadb_execute_total_count": {
		PromQL:  "sum(increase(milevadb_stochastik_execute_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of of MilevaDB executing the ALLEGROALLEGROSQL",
	},
	"milevadb_execute_total_time": {
		PromQL:  "sum(increase(milevadb_stochastik_execute_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total time cost of executing the ALLEGROALLEGROSQL which does not include the time to get the results of the query(second)",
	},
	"milevadb_gc_push_task_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_range_task_push_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of ekv storage range worker processing one task duration",
	},
	"milevadb_gc_push_task_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_range_task_push_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of ekv storage range worker processing one task duration",
	},
	"milevadb_gc_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_gc_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of ekv storage garbage defCauslection",
	},
	"milevadb_gc_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_gc_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of ekv storage garbage defCauslection time durations",
	},
	"milevadb_get_token_total_count": {
		PromQL:  "sum(increase(milevadb_server_get_token_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of Duration (us) for getting token, it should be small until concurrency limit is reached",
	},
	"milevadb_get_token_total_time": {
		PromQL:  "sum(increase(milevadb_server_get_token_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of Duration (us) for getting token, it should be small until concurrency limit is reached(microsecond)",
	},
	"milevadb_ekv_backoff_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_backoff_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of ekv backoff",
	},
	"milevadb_ekv_backoff_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_backoff_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of ekv backoff time durations(second)",
	},
	"milevadb_ekv_request_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_request_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,causetstore)",
		Labels:  []string{"instance", "type", "causetstore"},
		Comment: "The total count of ekv requests durations by causetstore",
	},
	"milevadb_ekv_request_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_request_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,causetstore)",
		Labels:  []string{"instance", "type", "causetstore"},
		Comment: "The total time of ekv requests durations by causetstore",
	},
	"milevadb_load_schema_total_count": {
		PromQL:  "sum(increase(milevadb_petri_load_schema_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB loading schemaReplicant by instance",
	},
	"milevadb_load_schema_total_time": {
		PromQL:  "sum(increase(milevadb_petri_load_schema_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of MilevaDB loading schemaReplicant time durations by instance",
	},
	"milevadb_spacetime_operation_total_count": {
		PromQL:  "sum(increase(milevadb_spacetime_operation_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total count of MilevaDB spacetime operation durations including get/set schemaReplicant and dbs jobs",
	},
	"milevadb_spacetime_operation_total_time": {
		PromQL:  "sum(increase(milevadb_spacetime_operation_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total time of MilevaDB spacetime operation durations including get/set schemaReplicant and dbs jobs",
	},
	"milevadb_new_etcd_stochastik_total_count": {
		PromQL:  "sum(increase(milevadb_tenant_new_stochastik_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total count of MilevaDB new stochastik durations for new etcd stochastik",
	},
	"milevadb_new_etcd_stochastik_total_time": {
		PromQL:  "sum(increase(milevadb_tenant_new_stochastik_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total time of MilevaDB new stochastik durations for new etcd stochastik",
	},
	"milevadb_tenant_handle_syncer_total_count": {
		PromQL:  "sum(increase(milevadb_dbs_tenant_handle_syncer_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total count of MilevaDB dbs tenant operations on etcd ",
	},
	"milevadb_tenant_handle_syncer_total_time": {
		PromQL:  "sum(increase(milevadb_dbs_tenant_handle_syncer_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,result)",
		Labels:  []string{"instance", "type", "result"},
		Comment: "The total time of MilevaDB dbs tenant time operations on etcd duration statistics ",
	},
	"milevadb_parse_total_count": {
		PromQL:  "sum(increase(milevadb_stochastik_parse_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of parsing ALLEGROALLEGROSQL to AST(second)",
	},
	"milevadb_parse_total_time": {
		PromQL:  "sum(increase(milevadb_stochastik_parse_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total time cost of parsing ALLEGROALLEGROSQL to AST(second)",
	},
	"milevadb_query_total_count": {
		PromQL:  "sum(increase(milevadb_server_handle_query_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of MilevaDB query durations(second)",
	},
	"milevadb_query_total_time": {
		PromQL:  "sum(increase(milevadb_server_handle_query_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total time of MilevaDB query durations(second)",
	},
	"milevadb_txn_cmd_duration": {
		PromQL:   `histogram_quantile($QUANTILE, sum(rate(milevadb_einsteindbclient_txn_cmd_duration_seconds_bucket{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,type,instance))`,
		Labels:   []string{"instance", "type"},
		Quantile: 0.90,
		Comment:  "The quantile of MilevaDB transaction command durations(second)",
	},
	"milevadb_txn_cmd_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_txn_cmd_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of MilevaDB transaction command",
	},
	"milevadb_txn_cmd_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_txn_cmd_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of MilevaDB transaction command",
	},
	"milevadb_slow_query_cop_process_total_count": {
		PromQL:  "sum(increase(milevadb_server_slow_query_cop_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB slow query cop process",
	},
	"milevadb_slow_query_cop_process_total_time": {
		PromQL:  "sum(increase(milevadb_server_slow_query_cop_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of MilevaDB slow query statistics with slow query total cop process time(second)",
	},
	"milevadb_slow_query_cop_wait_total_count": {
		PromQL:  "sum(increase(milevadb_server_slow_query_wait_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB slow query cop wait",
	},
	"milevadb_slow_query_cop_wait_total_time": {
		PromQL:  "sum(increase(milevadb_server_slow_query_wait_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of MilevaDB slow query statistics with slow query total cop wait time(second)",
	},
	"milevadb_slow_query_total_count": {
		PromQL:  "sum(increase(milevadb_server_slow_query_process_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB slow query",
	},
	"milevadb_slow_query_total_time": {
		PromQL:  "sum(increase(milevadb_server_slow_query_process_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of MilevaDB slow query statistics with slow query time(second)",
	},
	"milevadb_statistics_auto_analyze_total_count": {
		PromQL:  "sum(increase(milevadb_statistics_auto_analyze_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB auto analyze",
	},
	"milevadb_statistics_auto_analyze_total_time": {
		PromQL:  "sum(increase(milevadb_statistics_auto_analyze_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of MilevaDB auto analyze time durations within 95 percent histogram buckets",
	},
	"milevadb_transaction_local_latch_wait_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_local_latch_wait_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB transaction latch wait on key value storage(second)",
	},
	"milevadb_transaction_local_latch_wait_total_time": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_local_latch_wait_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of MilevaDB transaction latch wait time on key value storage(second)",
	},
	"milevadb_transaction_total_count": {
		PromQL:  "sum(increase(milevadb_stochastik_transaction_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,sql_type)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "The total count of transaction execution durations, including retry(second)",
	},
	"milevadb_transaction_total_time": {
		PromQL:  "sum(increase(milevadb_stochastik_transaction_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type,sql_type)",
		Labels:  []string{"instance", "type", "sql_type"},
		Comment: "The total time of transaction execution durations, including retry(second)",
	},
	"einsteindb_raftstore_append_log_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_append_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of Raft appends log",
	},
	"einsteindb_raftstore_append_log_total_time": {
		PromQL:  "sum(increase(einsteindb_raftstore_append_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of Raft appends log",
	},
	"einsteindb_raftstore_apply_log_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_apply_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of Raft applies log",
	},
	"einsteindb_raftstore_apply_log_total_time": {
		PromQL:  "sum(increase(einsteindb_raftstore_apply_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of Raft applies log",
	},
	"einsteindb_raftstore_apply_wait_total_count": {
		PromQL: "sum(increase(einsteindb_raftstore_apply_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_raftstore_apply_wait_total_time": {
		PromQL: "sum(increase(einsteindb_raftstore_apply_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_backup_range_total_count": {
		PromQL: "sum(increase(einsteindb_backup_range_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels: []string{"instance", "type"},
	},
	"einsteindb_backup_range_total_time": {
		PromQL: "sum(increase(einsteindb_backup_range_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels: []string{"instance", "type"},
	},
	"einsteindb_backup_total_count": {
		PromQL: "sum(increase(einsteindb_backup_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_backup_total_time": {
		PromQL: "sum(increase(einsteindb_backup_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_check_split_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_check_split_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of running split check",
	},
	"einsteindb_check_split_total_time": {
		PromQL:  "sum(increase(einsteindb_raftstore_check_split_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of time consumed when running split check in .9999",
	},
	"einsteindb_raftstore_commit_log_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_commit_log_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of Raft commits log",
	},
	"einsteindb_raftstore_commit_log_total_time": {
		PromQL:  "sum(increase(einsteindb_raftstore_commit_log_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of Raft commits log",
	},
	"einsteindb_cop_handle_total_count": {
		PromQL:  "sum(increase(einsteindb_coprocessor_request_handle_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total count of einsteindb interlock handling interlock requests",
	},
	"einsteindb_cop_handle_total_time": {
		PromQL:  "sum(increase(einsteindb_coprocessor_request_handle_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total time of time consumed when handling interlock requests",
	},
	"einsteindb_cop_request_total_count": {
		PromQL:  "sum(increase(einsteindb_coprocessor_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total count of einsteindb handle interlock read requests",
	},
	"einsteindb_cop_request_total_time": {
		PromQL:  "sum(increase(einsteindb_coprocessor_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels:  []string{"instance", "req"},
		Comment: "The total time of time consumed to handle interlock read requests",
	},
	"einsteindb_cop_wait_total_count": {
		PromQL:  "sum(increase(einsteindb_coprocessor_request_wait_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req,type)",
		Labels:  []string{"instance", "req", "type"},
		Comment: "The total count of interlock requests that wait for being handled",
	},
	"einsteindb_cop_wait_total_time": {
		PromQL:  "sum(increase(einsteindb_coprocessor_request_wait_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req,type)",
		Labels:  []string{"instance", "req", "type"},
		Comment: "The total time of time consumed when interlock requests are wait for being handled",
	},
	"einsteindb_raft_store_events_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_event_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of raftstore events (P99).99",
	},
	"einsteindb_raft_store_events_total_time": {
		PromQL:  "sum(increase(einsteindb_raftstore_event_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of raftstore events (P99).99",
	},
	"einsteindb_gc_tasks_total_count": {
		PromQL:  "sum(increase(einsteindb_gcworker_gc_task_duration_vec_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,task)",
		Labels:  []string{"instance", "task"},
		Comment: "The total count of executing GC tasks",
	},
	"einsteindb_gc_tasks_total_time": {
		PromQL:  "sum(increase(einsteindb_gcworker_gc_task_duration_vec_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,task)",
		Labels:  []string{"instance", "task"},
		Comment: "The total time of time consumed when executing GC tasks",
	},
	"einsteindb_grpc_message_total_count": {
		PromQL:  "sum(increase(einsteindb_grpc_msg_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of einsteindb execution gRPC message",
	},
	"einsteindb_grpc_message_total_time": {
		PromQL:  "sum(increase(einsteindb_grpc_msg_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of execution time of gRPC message",
	},
	"einsteindb_handle_snapshot_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_snapshot_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of einsteindb handling snapshots",
	},
	"einsteindb_handle_snapshot_total_time": {
		PromQL:  "sum(increase(einsteindb_raftstore_snapshot_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of time consumed when handling snapshots",
	},
	"einsteindb_ingest_sst_total_count": {
		PromQL:  "sum(increase(einsteindb_snapshot_ingest_sst_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,EDB)",
		Labels:  []string{"instance", "EDB"},
		Comment: "The total count of ingesting SST files",
	},
	"einsteindb_ingest_sst_total_time": {
		PromQL:  "sum(increase(einsteindb_snapshot_ingest_sst_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,EDB)",
		Labels:  []string{"instance", "EDB"},
		Comment: "The total time of time consumed when ingesting SST files",
	},
	"einsteindb_lock_manager_deadlock_detect_total_count": {
		PromQL: "sum(increase(einsteindb_lock_manager_detect_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_lock_manager_deadlock_detect_total_time": {
		PromQL: "sum(increase(einsteindb_lock_manager_detect_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_lock_manager_waiter_lifetime_total_count": {
		PromQL: "sum(increase(einsteindb_lock_manager_waiter_lifetime_duration_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_lock_manager_waiter_lifetime_total_time": {
		PromQL: "sum(increase(einsteindb_lock_manager_waiter_lifetime_duration_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_raftstore_process_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_raft_process_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of peer processes in Raft",
	},
	"einsteindb_raftstore_process_total_time": {
		PromQL:  "sum(increase(einsteindb_raftstore_raft_process_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of peer processes in Raft",
	},
	"einsteindb_raftstore_propose_wait_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_request_wait_time_duration_secs_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of each proposal",
	},
	"einsteindb_raftstore_propose_wait_total_time": {
		PromQL:  "sum(increase(einsteindb_raftstore_request_wait_time_duration_secs_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of wait time of each proposal",
	},
	"einsteindb_scheduler_command_total_count": {
		PromQL:  "sum(increase(einsteindb_scheduler_command_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of einsteindb scheduler executing command",
	},
	"einsteindb_scheduler_command_total_time": {
		PromQL:  "sum(increase(einsteindb_scheduler_command_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of time consumed when executing command",
	},
	"einsteindb_scheduler_latch_wait_total_count": {
		PromQL:  "sum(increase(einsteindb_scheduler_latch_wait_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count which is caused by latch wait in command",
	},
	"einsteindb_scheduler_latch_wait_total_time": {
		PromQL:  "sum(increase(einsteindb_scheduler_latch_wait_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time which is caused by latch wait in command",
	},
	"einsteindb_send_snapshot_total_count": {
		PromQL:  "sum(increase(einsteindb_server_send_snapshot_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of sending snapshots",
	},
	"einsteindb_send_snapshot_total_time": {
		PromQL:  "sum(increase(einsteindb_server_send_snapshot_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of time consumed when sending snapshots",
	},
	"einsteindb_storage_async_request_total_count": {
		PromQL:  "sum(increase(einsteindb_storage_engine_async_request_duration_seconds_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of processing asynchronous snapshot requests",
	},
	"einsteindb_storage_async_request_total_time": {
		PromQL:  "sum(increase(einsteindb_storage_engine_async_request_duration_seconds_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of time consumed by processing asynchronous snapshot requests",
	},

	"milevadb_allegrosql_partial_num_total_count": {
		PromQL:  "sum(increase(milevadb_allegrosql_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of allegrosql partial numbers per query",
	},
	"milevadb_allegrosql_partial_scan_key_num_total_count": {
		PromQL:  "sum(increase(milevadb_allegrosql_scan_keys_partial_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of allegrosql partial scan key numbers",
	},
	"milevadb_allegrosql_partial_scan_key_total_num": {
		PromQL:  "sum(increase(milevadb_allegrosql_scan_keys_partial_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of allegrosql partial scan key numbers",
	},
	"milevadb_allegrosql_partial_total_num": {
		PromQL:  "sum(increase(milevadb_allegrosql_partial_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of allegrosql partial numbers per query",
	},
	"milevadb_allegrosql_scan_key_num_total_count": {
		PromQL:  "sum(increase(milevadb_allegrosql_scan_keys_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of allegrosql scan numbers",
	},
	"milevadb_allegrosql_scan_key_total_num": {
		PromQL:  "sum(increase(milevadb_allegrosql_scan_keys_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of allegrosql scan numbers",
	},
	"milevadb_ekv_write_num_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_txn_write_ekv_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of ekv write in transaction execution",
	},
	"milevadb_ekv_write_size_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_txn_write_size_bytes_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of ekv write size per transaction execution",
	},
	"milevadb_ekv_write_total_num": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_txn_write_ekv_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of ekv write in transaction execution",
	},
	"milevadb_ekv_write_total_size": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_txn_write_size_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total ekv write size in transaction execution",
	},
	"milevadb_statistics_fast_analyze_status_total_count": {
		PromQL:  "sum(increase(milevadb_statistics_fast_analyze_status_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of MilevaDB fast analyze statistics ",
	},
	"milevadb_statistics_fast_analyze_total_status": {
		PromQL:  "sum(increase(milevadb_statistics_fast_analyze_status_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total time of MilevaDB fast analyze statistics ",
	},
	"milevadb_statistics_stats_inaccuracy_rate_total_count": {
		PromQL:  "sum(increase(milevadb_statistics_stats_inaccuracy_rate_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB statistics inaccurate rate",
	},
	"milevadb_statistics_stats_inaccuracy_total_rate": {
		PromQL:  "sum(increase(milevadb_statistics_stats_inaccuracy_rate_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total time of MilevaDB statistics inaccurate rate",
	},
	"milevadb_transaction_retry_num_total_count": {
		PromQL:  "sum(increase(milevadb_stochastik_retry_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of MilevaDB transaction retry num",
	},
	"milevadb_transaction_retry_total_num": {
		PromQL:  "sum(increase(milevadb_stochastik_retry_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of MilevaDB transaction retry num",
	},
	"milevadb_transaction_memex_num_total_count": {
		PromQL:  "sum(increase(milevadb_stochastik_transaction_memex_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total count of MilevaDB memexs numbers within one transaction. Internal means MilevaDB inner transaction",
	},
	"milevadb_transaction_memex_total_num": {
		PromQL:  "sum(increase(milevadb_stochastik_transaction_memex_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,sql_type)",
		Labels:  []string{"instance", "sql_type"},
		Comment: "The total num of MilevaDB memexs numbers within one transaction. Internal means MilevaDB inner transaction",
	},
	"milevadb_txn_region_num_total_count": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_txn_regions_num_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of regions transaction operates on count",
	},
	"milevadb_txn_region_total_num": {
		PromQL:  "sum(increase(milevadb_einsteindbclient_txn_regions_num_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total num of regions transaction operates on count",
	},
	"einsteindb_approximate_region_size_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_region_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of approximate Region size, the default value is P99",
	},
	"einsteindb_approximate_region_total_size": {
		PromQL:  "sum(increase(einsteindb_raftstore_region_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total size of approximate Region size",
	},
	"einsteindb_backup_range_size_total_count": {
		PromQL: "sum(increase(einsteindb_backup_range_size_bytes_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,cf)",
		Labels: []string{"instance", "cf"},
	},
	"einsteindb_backup_range_total_size": {
		PromQL: "sum(increase(einsteindb_backup_range_size_bytes_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,cf)",
		Labels: []string{"instance", "cf"},
	},
	"einsteindb_cop_ekv_cursor_operations_total_count": {
		PromQL: "sum(increase(einsteindb_coprocessor_scan_keys_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels: []string{"instance", "req"},
	},
	"einsteindb_cop_scan_keys_total_num": {
		PromQL: "sum(increase(einsteindb_coprocessor_scan_keys_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,req)",
		Labels: []string{"instance", "req"},
	},
	"einsteindb_cop_total_response_total_size": {
		PromQL: `sum(increase(einsteindb_coprocessor_response_bytes{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)`,
		Labels: []string{"instance"},
	},
	"einsteindb_grpc_req_batch_size_total_count": {
		PromQL: "sum(increase(einsteindb_server_grpc_req_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_grpc_req_batch_total_size": {
		PromQL: "sum(increase(einsteindb_server_grpc_req_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_grpc_resp_batch_size_total_count": {
		PromQL: "sum(increase(einsteindb_server_grpc_resp_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_grpc_resp_batch_total_size": {
		PromQL: "sum(increase(einsteindb_server_grpc_resp_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_raft_message_batch_size_total_count": {
		PromQL: "sum(increase(einsteindb_server_raft_message_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_raft_message_batch_total_size": {
		PromQL: "sum(increase(einsteindb_server_raft_message_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels: []string{"instance"},
	},
	"einsteindb_raft_proposals_per_ready_total_count": {
		PromQL:  "sum(increase(einsteindb_raftstore_apply_proposal_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of proposal count of all Regions in a mio tick",
	},
	"einsteindb_raft_proposals_per_total_ready": {
		PromQL:  "sum(increase(einsteindb_raftstore_apply_proposal_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total proposal count of all Regions in a mio tick",
	},
	"einsteindb_request_batch_ratio_total_count": {
		PromQL:  "sum(increase(einsteindb_server_request_batch_ratio_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of request batch output to input per EinsteinDB instance",
	},
	"einsteindb_request_batch_size_total_count": {
		PromQL:  "sum(increase(einsteindb_server_request_batch_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of request batch per EinsteinDB instance",
	},
	"einsteindb_request_batch_total_ratio": {
		PromQL:  "sum(increase(einsteindb_server_request_batch_ratio_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total ratio of request batch output to input per EinsteinDB instance",
	},
	"einsteindb_request_batch_total_size": {
		PromQL:  "sum(increase(einsteindb_server_request_batch_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total size of requests into request batch per EinsteinDB instance",
	},
	"einsteindb_scheduler_keys_read_total_count": {
		PromQL:  "sum(increase(einsteindb_scheduler_ekv_command_key_read_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of keys read by a command",
	},
	"einsteindb_scheduler_keys_total_read": {
		PromQL:  "sum(increase(einsteindb_scheduler_ekv_command_key_read_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of keys read by command",
	},
	"einsteindb_scheduler_keys_total_written": {
		PromQL:  "sum(increase(einsteindb_scheduler_ekv_command_key_write_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of keys written by a command",
	},
	"einsteindb_scheduler_keys_written_total_count": {
		PromQL:  "sum(increase(einsteindb_scheduler_ekv_command_key_write_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance,type)",
		Labels:  []string{"instance", "type"},
		Comment: "The total count of keys written by a command",
	},
	"einsteindb_snapshot_ekv_count_total_count": {
		PromQL:  "sum(increase(einsteindb_snapshot_ekv_count_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of number of KV within a snapshot",
	},
	"einsteindb_snapshot_ekv_total_count": {
		PromQL:  "sum(increase(einsteindb_snapshot_ekv_count_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total number of KV within a snapshot",
	},
	"einsteindb_snapshot_size_total_count": {
		PromQL:  "sum(increase(einsteindb_snapshot_size_count{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total count of snapshot size",
	},
	"einsteindb_snapshot_total_size": {
		PromQL:  "sum(increase(einsteindb_snapshot_size_sum{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (instance)",
		Labels:  []string{"instance"},
		Comment: "The total size of snapshot size",
	},
	"einsteindb_config_lmdb": {
		PromQL:  "einsteindb_config_lmdb{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "cf", "name"},
		Comment: "EinsteinDB lmdb config value",
	},
	"einsteindb_config_raftstore": {
		PromQL:  "einsteindb_config_raftstore{$LABEL_CONDITIONS}",
		Labels:  []string{"instance", "name"},
		Comment: "EinsteinDB lmdb config value",
	},
}
