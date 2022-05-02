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

package executor

import (
	"context"
	"fmt"
	"strings"

	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/sqlexec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
)

type inspectionSummaryRetriever struct {
	dummyCloser
	retrieved bool
	block     *perceptron.BlockInfo
	extractor *plannercore.InspectionSummaryBlockExtractor
	timeRange plannercore.QueryTimeRange
}

// inspectionSummaryMemrules is used to maintain
var inspectionSummaryMemrules = map[string][]string{
	"query-summary": {
		"milevadb_connection_count",
		"milevadb_query_duration",
		"milevadb_qps_ideal",
		"milevadb_qps",
		"milevadb_ops_internal",
		"milevadb_ops_statement",
		"milevadb_failed_query_opm",
		"milevadb_slow_query_duration",
		"milevadb_slow_query_INTERLOCK_wait_duration",
		"milevadb_slow_query_INTERLOCK_process_duration",
	},
	"wait-events": {
		"milevadb_get_token_duration",
		"milevadb_load_schema_duration",
		"milevadb_query_duration",
		"milevadb_parse_duration",
		"milevadb_compile_duration",
		"milevadb_execute_duration",
		"milevadb_auto_id_request_duration",
		"FIDel_tso_wait_duration",
		"FIDel_tso_rpc_duration",
		"milevadb_distsql_execution_duration",
		"FIDel_start_tso_wait_duration",
		"milevadb_transaction_local_latch_wait_duration",
		"milevadb_transaction_duration",
		"FIDel_request_rpc_duration",
		"milevadb_INTERLOCK_duration",
		"milevadb_batch_client_wait_duration",
		"milevadb_batch_client_unavailable_duration",
		"milevadb_kv_backoff_duration",
		"milevadb_kv_request_duration",
		"FIDel_client_cmd_duration",
		"einsteindb_grpc_message_duration",
		"einsteindb_average_grpc_messge_duration",
		"einsteindb_channel_full",
		"einsteindb_scheduler_is_busy",
		"einsteindb_interlocking_directorate_is_busy",
		"einsteindb_engine_write_stall",
		"einsteindb_raftstore_apply_log_avg_duration",
		"einsteindb_raftstore_apply_log_duration",
		"einsteindb_raftstore_append_log_avg_duration",
		"einsteindb_raftstore_append_log_duration",
		"einsteindb_raftstore_commit_log_avg_duration",
		"einsteindb_raftstore_commit_log_duration",
		"einsteindb_raftstore_process_duration",
		"einsteindb_raftstore_propose_wait_duration",
		"einsteindb_propose_avg_wait_duration",
		"einsteindb_raftstore_apply_wait_duration",
		"einsteindb_apply_avg_wait_duration",
		"einsteindb_check_split_duration",
		"einsteindb_storage_async_request_duration",
		"einsteindb_storage_async_request_avg_duration",
		"einsteindb_scheduler_command_duration",
		"einsteindb_scheduler_command_avg_duration",
		"einsteindb_scheduler_latch_wait_duration",
		"einsteindb_scheduler_latch_wait_avg_duration",
		"einsteindb_send_snapshot_duration",
		"einsteindb_handle_snapshot_duration",
		"einsteindb_INTERLOCK_request_durations",
		"einsteindb_INTERLOCK_request_duration",
		"einsteindb_INTERLOCK_handle_duration",
		"einsteindb_INTERLOCK_wait_duration",
		"einsteindb_engine_max_get_duration",
		"einsteindb_engine_avg_get_duration",
		"einsteindb_engine_avg_seek_duration",
		"einsteindb_engine_write_duration",
		"einsteindb_wal_sync_max_duration",
		"einsteindb_wal_sync_duration",
		"einsteindb_compaction_max_duration",
		"einsteindb_compaction_duration",
		"einsteindb_sst_read_max_duration",
		"einsteindb_sst_read_duration",
		"einsteindb_write_stall_max_duration",
		"einsteindb_write_stall_avg_duration",
		"einsteindb_oldest_snapshots_duration",
		"einsteindb_ingest_sst_duration",
		"einsteindb_ingest_sst_avg_duration",
		"einsteindb_engine_blob_seek_duration",
		"einsteindb_engine_blob_get_duration",
		"einsteindb_engine_blob_file_read_duration",
		"einsteindb_engine_blob_file_write_duration",
		"einsteindb_engine_blob_file_sync_duration",
		"einsteindb_lock_manager_waiter_lifetime_avg_duration",
		"einsteindb_lock_manager_deadlock_detect_duration",
		"einsteindb_lock_manager_deadlock_detect_avg_duration",
	},
	"read-link": {
		"milevadb_get_token_duration",
		"milevadb_parse_duration",
		"milevadb_compile_duration",
		"FIDel_tso_rpc_duration",
		"FIDel_tso_wait_duration",
		"milevadb_execute_duration",
		"milevadb_expensive_executors_ops",
		"milevadb_query_using_plan_cache_ops",
		"milevadb_distsql_execution_duration",
		"milevadb_distsql_partial_num",
		"milevadb_distsql_partial_qps",
		"milevadb_distsql_partial_scan_key_num",
		"milevadb_distsql_qps",
		"milevadb_distsql_scan_key_num",
		"milevadb_region_cache_ops",
		"milevadb_batch_client_pending_req_count",
		"milevadb_batch_client_unavailable_duration",
		"milevadb_batch_client_wait_duration",
		"milevadb_kv_backoff_duration",
		"milevadb_kv_backoff_ops",
		"milevadb_kv_region_error_ops",
		"milevadb_kv_request_duration",
		"milevadb_kv_request_ops",
		"milevadb_kv_snapshot_ops",
		"milevadb_kv_txn_ops",
		"einsteindb_average_grpc_messge_duration",
		"einsteindb_grpc_avg_req_batch_size",
		"einsteindb_grpc_avg_resp_batch_size",
		"einsteindb_grpc_errors",
		"einsteindb_grpc_message_duration",
		"einsteindb_grpc_qps",
		"einsteindb_grpc_req_batch_size",
		"einsteindb_grpc_resp_batch_size",
		"milevadb_INTERLOCK_duration",
		"einsteindb_INTERLOCK_wait_duration",
		"einsteindb_interlocking_directorate_is_busy",
		"einsteindb_interlocking_directorate_request_error",
		"einsteindb_INTERLOCK_handle_duration",
		"einsteindb_INTERLOCK_kv_cursor_operations",
		"einsteindb_INTERLOCK_request_duration",
		"einsteindb_INTERLOCK_request_durations",
		"einsteindb_INTERLOCK_scan_details",
		"einsteindb_INTERLOCK_posetPosetDag_executors_ops",
		"einsteindb_INTERLOCK_posetPosetDag_requests_ops",
		"einsteindb_INTERLOCK_scan_keys_num",
		"einsteindb_INTERLOCK_requests_ops",
		"einsteindb_INTERLOCK_total_response_size_per_seconds",
		"einsteindb_INTERLOCK_total_lmdb_perf_statistics",
		"einsteindb_channel_full",
		"einsteindb_engine_avg_get_duration",
		"einsteindb_engine_avg_seek_duration",
		"einsteindb_handle_snapshot_duration",
		"einsteindb_block_all_cache_hit",
		"einsteindb_block_bloom_prefix_cache_hit",
		"einsteindb_block_cache_size",
		"einsteindb_block_data_cache_hit",
		"einsteindb_block_filter_cache_hit",
		"einsteindb_block_index_cache_hit",
		"einsteindb_engine_get_block_cache_operations",
		"einsteindb_engine_get_cpu_cache_operations",
		"einsteindb_engine_get_memblock_operations",
		"einsteindb_per_read_avg_bytes",
		"einsteindb_per_read_max_bytes",
	},
	"write-link": {
		"milevadb_get_token_duration",
		"milevadb_parse_duration",
		"milevadb_compile_duration",
		"FIDel_tso_rpc_duration",
		"FIDel_tso_wait_duration",
		"milevadb_execute_duration",
		"milevadb_transaction_duration",
		"milevadb_transaction_local_latch_wait_duration",
		"milevadb_transaction_ops",
		"milevadb_transaction_retry_error_ops",
		"milevadb_transaction_retry_num",
		"milevadb_transaction_statement_num",
		"milevadb_auto_id_qps",
		"milevadb_auto_id_request_duration",
		"milevadb_region_cache_ops",
		"milevadb_kv_backoff_duration",
		"milevadb_kv_backoff_ops",
		"milevadb_kv_region_error_ops",
		"milevadb_kv_request_duration",
		"milevadb_kv_request_ops",
		"milevadb_kv_snapshot_ops",
		"milevadb_kv_txn_ops",
		"milevadb_kv_write_num",
		"milevadb_kv_write_size",
		"einsteindb_average_grpc_messge_duration",
		"einsteindb_grpc_avg_req_batch_size",
		"einsteindb_grpc_avg_resp_batch_size",
		"einsteindb_grpc_errors",
		"einsteindb_grpc_message_duration",
		"einsteindb_grpc_qps",
		"einsteindb_grpc_req_batch_size",
		"einsteindb_grpc_resp_batch_size",
		"einsteindb_scheduler_command_avg_duration",
		"einsteindb_scheduler_command_duration",
		"einsteindb_scheduler_is_busy",
		"einsteindb_scheduler_keys_read_avg",
		"einsteindb_scheduler_keys_read",
		"einsteindb_scheduler_keys_written_avg",
		"einsteindb_scheduler_keys_written",
		"einsteindb_scheduler_latch_wait_avg_duration",
		"einsteindb_scheduler_latch_wait_duration",
		"einsteindb_scheduler_pending_commands",
		"einsteindb_scheduler_priority_commands",
		"einsteindb_scheduler_scan_details",
		"einsteindb_scheduler_stage",
		"einsteindb_scheduler_writing_bytes",
		"einsteindb_propose_avg_wait_duration",
		"einsteindb_raftstore_propose_wait_duration",
		"einsteindb_raftstore_append_log_avg_duration",
		"einsteindb_raftstore_append_log_duration",
		"einsteindb_raftstore_commit_log_avg_duration",
		"einsteindb_raftstore_commit_log_duration",
		"einsteindb_apply_avg_wait_duration",
		"einsteindb_raftstore_apply_log_avg_duration",
		"einsteindb_raftstore_apply_log_duration",
		"einsteindb_raftstore_apply_wait_duration",
		"einsteindb_engine_wal_sync_operations",
		"einsteindb_engine_write_duration",
		"einsteindb_engine_write_operations",
		"einsteindb_engine_write_stall",
		"einsteindb_write_stall_avg_duration",
		"einsteindb_write_stall_max_duration",
		"einsteindb_write_stall_reason",
	},
	"dbs": {
		"milevadb_dbs_add_index_speed",
		"milevadb_dbs_batch_add_index_duration",
		"milevadb_dbs_deploy_syncer_duration",
		"milevadb_dbs_duration",
		"milevadb_dbs_meta_opm",
		"milevadb_dbs_opm",
		"milevadb_dbs_uFIDelate_self_version_duration",
		"milevadb_dbs_waiting_jobs_num",
		"milevadb_dbs_worker_duration",
	},
	"stats": {
		"milevadb_statistics_auto_analyze_duration",
		"milevadb_statistics_auto_analyze_ops",
		"milevadb_statistics_dump_feedback_ops",
		"milevadb_statistics_fast_analyze_status",
		"milevadb_statistics_pseudo_estimation_ops",
		"milevadb_statistics_significant_feedback",
		"milevadb_statistics_stats_inaccuracy_rate",
		"milevadb_statistics_store_query_feedback_qps",
		"milevadb_statistics_uFIDelate_stats_ops",
	},
	"gc": {
		"milevadb_gc_action_result_opm",
		"milevadb_gc_config",
		"milevadb_gc_delete_range_fail_opm",
		"milevadb_gc_delete_range_task_status",
		"milevadb_gc_duration",
		"milevadb_gc_fail_opm",
		"milevadb_gc_push_task_duration",
		"milevadb_gc_too_many_locks_opm",
		"milevadb_gc_worker_action_opm",
		"einsteindb_engine_blob_gc_duration",
		"einsteindb_auto_gc_progress",
		"einsteindb_auto_gc_safepoint",
		"einsteindb_auto_gc_working",
		"einsteindb_gc_fail_tasks",
		"einsteindb_gc_keys",
		"einsteindb_gc_skipped_tasks",
		"einsteindb_gc_speed",
		"einsteindb_gc_tasks_avg_duration",
		"einsteindb_gc_tasks_duration",
		"einsteindb_gc_too_busy",
		"einsteindb_gc_tasks_ops",
	},
	"lmdb": {
		"einsteindb_compaction_duration",
		"einsteindb_compaction_max_duration",
		"einsteindb_compaction_operations",
		"einsteindb_compaction_pending_bytes",
		"einsteindb_compaction_reason",
		"einsteindb_write_stall_avg_duration",
		"einsteindb_write_stall_max_duration",
		"einsteindb_write_stall_reason",
		"store_available_ratio",
		"store_size_amplification",
		"einsteindb_engine_avg_get_duration",
		"einsteindb_engine_avg_seek_duration",
		"einsteindb_engine_blob_bytes_flow",
		"einsteindb_engine_blob_file_count",
		"einsteindb_engine_blob_file_read_duration",
		"einsteindb_engine_blob_file_size",
		"einsteindb_engine_blob_file_sync_duration",
		"einsteindb_engine_blob_file_sync_operations",
		"einsteindb_engine_blob_file_write_duration",
		"einsteindb_engine_blob_gc_bytes_flow",
		"einsteindb_engine_blob_gc_duration",
		"einsteindb_engine_blob_gc_file",
		"einsteindb_engine_blob_gc_keys_flow",
		"einsteindb_engine_blob_get_duration",
		"einsteindb_engine_blob_key_avg_size",
		"einsteindb_engine_blob_key_max_size",
		"einsteindb_engine_blob_seek_duration",
		"einsteindb_engine_blob_seek_operations",
		"einsteindb_engine_blob_value_avg_size",
		"einsteindb_engine_blob_value_max_size",
		"einsteindb_engine_compaction_flow_bytes",
		"einsteindb_engine_get_block_cache_operations",
		"einsteindb_engine_get_cpu_cache_operations",
		"einsteindb_engine_get_memblock_operations",
		"einsteindb_engine_live_blob_size",
		"einsteindb_engine_max_get_duration",
		"einsteindb_engine_max_seek_duration",
		"einsteindb_engine_seek_operations",
		"einsteindb_engine_size",
		"einsteindb_engine_wal_sync_operations",
		"einsteindb_engine_write_duration",
		"einsteindb_engine_write_operations",
		"einsteindb_engine_write_stall",
	},
	"fidel": {
		"FIDel_scheduler_balance_region",
		"FIDel_balance_scheduler_status",
		"FIDel_checker_event_count",
		"FIDel_client_cmd_duration",
		"FIDel_client_cmd_ops",
		"FIDel_cluster_metadata",
		"FIDel_cluster_status",
		"FIDel_grpc_completed_commands_duration",
		"FIDel_grpc_completed_commands_rate",
		"FIDel_request_rpc_duration",
		"FIDel_request_rpc_ops",
		"FIDel_request_rpc_duration_avg",
		"FIDel_handle_transactions_duration",
		"FIDel_handle_transactions_rate",
		"FIDel_hotspot_status",
		"FIDel_label_distribution",
		"FIDel_operator_finish_duration",
		"FIDel_operator_step_finish_duration",
		"FIDel_peer_round_trip_duration",
		"FIDel_region_health",
		"FIDel_region_heartbeat_duration",
		"FIDel_region_label_isolation_level",
		"FIDel_region_syncer_status",
		"FIDel_role",
		"FIDel_schedule_filter",
		"FIDel_schedule_operator",
		"FIDel_schedule_store_limit",
		"FIDel_scheduler_balance_direction",
		"FIDel_scheduler_balance_leader",
		"FIDel_scheduler_config",
		"FIDel_scheduler_op_influence",
		"FIDel_scheduler_region_heartbeat",
		"FIDel_scheduler_status",
		"FIDel_scheduler_store_status",
		"FIDel_scheduler_tolerant_resource",
		"FIDel_server_etcd_state",
		"FIDel_start_tso_wait_duration",
	},
	"raftstore": {
		"einsteindb_approximate_avg_region_size",
		"einsteindb_approximate_region_size_histogram",
		"einsteindb_approximate_region_size",
		"einsteindb_raftstore_append_log_avg_duration",
		"einsteindb_raftstore_append_log_duration",
		"einsteindb_raftstore_commit_log_avg_duration",
		"einsteindb_raftstore_commit_log_duration",
		"einsteindb_apply_avg_wait_duration",
		"einsteindb_raftstore_apply_log_avg_duration",
		"einsteindb_raftstore_apply_log_duration",
		"einsteindb_raftstore_apply_wait_duration",
		"einsteindb_raftstore_process_duration",
		"einsteindb_raftstore_process_handled",
		"einsteindb_propose_avg_wait_duration",
		"einsteindb_raftstore_propose_wait_duration",
		"einsteindb_raft_dropped_messages",
		"einsteindb_raft_log_speed",
		"einsteindb_raft_message_avg_batch_size",
		"einsteindb_raft_message_batch_size",
		"einsteindb_raft_proposals_per_ready",
		"einsteindb_raft_proposals",
		"einsteindb_raft_sent_messages",
	},
}

func (e *inspectionSummaryRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.retrieved || e.extractor.SkipInspection {
		return nil, nil
	}
	e.retrieved = true

	rules := inspectionFilter{set: e.extractor.Memrules}
	names := inspectionFilter{set: e.extractor.MetricNames}

	condition := e.timeRange.Condition()
	var finalEvents [][]types.Causet
	for rule, blocks := range inspectionSummaryMemrules {
		if len(rules.set) != 0 && !rules.set.Exist(rule) {
			continue
		}
		for _, name := range blocks {
			if !names.enable(name) {
				continue
			}
			def, found := schemareplicant.MetricBlockMap[name]
			if !found {
				sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("metrics block: %s not found", name))
				continue
			}
			defcaus := def.Labels
			comment := def.Comment
			cond := condition
			if def.Quantile > 0 {
				defcaus = append(defcaus, "quantile")
				if len(e.extractor.Quantiles) > 0 {
					qs := make([]string, len(e.extractor.Quantiles))
					for i, q := range e.extractor.Quantiles {
						qs[i] = fmt.Sprintf("%f", q)
					}
					cond += " and quantile in (" + strings.Join(qs, ",") + ")"
				} else {
					cond += " and quantile=0.99"
				}
			}
			var allegrosql string
			if len(defcaus) > 0 {
				allegrosql = fmt.Sprintf("select avg(value),min(value),max(value),`%s` from `%s`.`%s` %s group by `%[1]s` order by `%[1]s`",
					strings.Join(defcaus, "`,`"), soliton.MetricSchemaName.L, name, cond)
			} else {
				allegrosql = fmt.Sprintf("select avg(value),min(value),max(value) from `%s`.`%s` %s",
					soliton.MetricSchemaName.L, name, cond)
			}
			rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
			if err != nil {
				return nil, errors.Errorf("execute '%s' failed: %v", allegrosql, err)
			}
			nonInstanceLabelIndex := 0
			if len(def.Labels) > 0 && def.Labels[0] == "instance" {
				nonInstanceLabelIndex = 1
			}
			// skip min/max/avg
			const skipDefCauss = 3
			for _, event := range rows {
				instance := ""
				if nonInstanceLabelIndex > 0 {
					instance = event.GetString(skipDefCauss) // skip min/max/avg
				}
				var labels []string
				for i, label := range def.Labels[nonInstanceLabelIndex:] {
					// skip min/max/avg/instance
					val := event.GetString(skipDefCauss + nonInstanceLabelIndex + i)
					if label == "causetstore" || label == "store_id" {
						val = fmt.Sprintf("store_id:%s", val)
					}
					labels = append(labels, val)
				}
				var quantile interface{}
				if def.Quantile > 0 {
					quantile = event.GetFloat64(event.Len() - 1) // quantile will be the last defCausumn
				}
				finalEvents = append(finalEvents, types.MakeCausets(
					rule,
					instance,
					name,
					strings.Join(labels, ", "),
					quantile,
					event.GetFloat64(0), // avg
					event.GetFloat64(1), // min
					event.GetFloat64(2), // max
					comment,
				))
			}
		}
	}
	return finalEvents, nil
}
