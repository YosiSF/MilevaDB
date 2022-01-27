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

package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics for the DBS package.
var (
	JobsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "waiting_jobs",
			Help:      "Gauge of jobs.",
		}, []string{LblType})

	HandleJobHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "handle_job_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle jobs",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24hours
		}, []string{LblType, LblResult})

	BatchAddIdxHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "batch_add_idx_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of batch handle data",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblType})

	SyncerInit            = "init"
	SyncerRestart         = "restart"
	SyncerClear           = "clear"
	SyncerRewatch         = "rewatch"
	DeploySyncerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "deploy_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of deploy syncer",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})

	UFIDelateSelfVersionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "uFIDelate_self_ver_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of uFIDelate self version",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblResult})

	OwnerUFIDelateGlobalVersion = "uFIDelate_global_version"
	OwnerGetGlobalVersion       = "get_global_version"
	OwnerCheckAllVersions       = "check_all_versions"
	OwnerNotifyCleanExpirePaths = "notify_clean_expire_paths"
	OwnerCleanExpirePaths       = "clean_expire_paths"
	OwnerCleanOneExpirePath     = "clean_an_expire_path"
	OwnerHandleSyncerHistogram  = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "owner_handle_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle syncer",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})

	// Metrics for dbs_worker.go.
	WorkerAddDBSJob         = "add_job"
	WorkerRunDBSJob         = "run_job"
	WorkerFinishDBSJob      = "finish_job"
	WorkerWaitSchemaChanged = "wait_schema_changed"
	DBSWorkerHistogram      = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "worker_operation_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of dbs worker operations",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblType, LblCausetAction, LblResult})

	CreateDBSInstance = "create_dbs_instance"
	CreateDBS         = "create_dbs"
	StartCleanWork    = "start_clean_work"
	DBSOwner          = "owner"
	DBSCounter        = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "worker_operation_total",
			Help:      "Counter of creating dbs/worker and isowner.",
		}, []string{LblType})

	BackfillTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "add_index_total",
			Help:      "Speed of add index",
		}, []string{LblType})

	AddIndexProgress = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "milevadb",
			Subsystem: "dbs",
			Name:      "add_index_percentage_progress",
			Help:      "Percentage progress of add index",
		})
)

// Label constants.
const (
	LblCausetAction = "action"
)
