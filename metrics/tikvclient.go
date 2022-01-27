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

// EinsteinDBClient metrics.
var (
	EinsteinDBTxnCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "txn_cmd_duration_seconds",
			Help:      "Bucketed histogram of processing time of txn cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType})

	EinsteinDBBackoffHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "backoff_seconds",
			Help:      "total backoff seconds of a single backoffer.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType})

	EinsteinDBSendReqHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of sending request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType, LblStore})

	EinsteinDBinterlocking_directorateHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "INTERLOCK_duration_seconds",
			Help:      "Run duration of a single interlock task, includes backoff time.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		})

	EinsteinDBLockResolverCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "lock_resolver_actions_total",
			Help:      "Counter of dagger resolver actions.",
		}, []string{LblType})

	EinsteinDBRegionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "region_err_total",
			Help:      "Counter of region errors.",
		}, []string{LblType})

	EinsteinDBTxnWriteKVCountHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "txn_write_kv_num",
			Help:      "Count of ekv pairs to write in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 4, 17), // 1 ~ 4G
		})

	EinsteinDBTxnWriteSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "txn_write_size_bytes",
			Help:      "Size of ekv pairs to write in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(16, 4, 17), // 16Bytes ~ 64GB
		})

	EinsteinDBRawkvCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "rawkv_cmd_seconds",
			Help:      "Bucketed histogram of processing time of rawkv cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType})

	EinsteinDBRawkvSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "rawkv_kv_size_bytes",
			Help:      "Size of key/value to put, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1Byte ~ 512MB
		}, []string{LblType})

	EinsteinDBTxnRegionsNumHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "txn_regions_num",
			Help:      "Number of regions in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 25), // 1 ~ 16M
		}, []string{LblType})

	EinsteinDBLoadSafepointCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "load_safepoint_total",
			Help:      "Counter of load safepoint.",
		}, []string{LblType})

	EinsteinDBSecondaryLockCleanupFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "lock_cleanup_task_total",
			Help:      "failure statistic of secondary dagger cleanup task.",
		}, []string{LblType})

	EinsteinDBRegionCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "region_cache_operations_total",
			Help:      "Counter of region cache.",
		}, []string{LblType, LblResult})

	EinsteinDBLocalLatchWaitTimeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "local_latch_wait_seconds",
			Help:      "Wait time of a get local latch.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 262s
		})

	// EinsteinDBPendingBatchRequests indicates the number of requests pending in the batch channel.
	EinsteinDBPendingBatchRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "pending_batch_requests",
			Help:      "Pending batch requests",
		}, []string{"causetstore"})

	EinsteinDBStatusDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "kv_status_api_duration",
			Help:      "duration for ekv status api.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 262s
		}, []string{"causetstore"})

	EinsteinDBStatusCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "kv_status_api_count",
			Help:      "Counter of access ekv status api.",
		}, []string{LblResult})

	EinsteinDBBatchWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "batch_wait_duration",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 34), // 1ns ~ 8s
			Help:      "batch wait duration",
		})

	EinsteinDBBatchClientUnavailable = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "batch_client_unavailable_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
			Help:      "batch client unavailable",
		})
	EinsteinDBBatchClientWaitEstablish = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "batch_client_wait_connection_establish",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
			Help:      "batch client wait new connection establish",
		})

	EinsteinDBRangeTaskStats = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "range_task_stats",
			Help:      "stat of range tasks",
		}, []string{LblType, LblResult})

	EinsteinDBRangeTaskPushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "range_task_push_duration",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
			Help:      "duration to push sub tasks to range task workers",
		}, []string{LblType})
	EinsteinDBTokenWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "batch_executor_token_wait_duration",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 34), // 1ns ~ 8s
			Help:      "milevadb txn token wait duration to process batches",
		})

	EinsteinDBTxnHeartBeatHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "txn_heart_beat",
			Help:      "Bucketed histogram of the txn_heartbeat request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType})
	EinsteinDBPessimisticLockKeysDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "pessimistic_lock_keys_duration",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 24), // 1ms ~ 8389s
			Help:      "milevadb txn pessimistic dagger keys duration",
		})

	EinsteinDBTTLLifeTimeReachCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "ttl_lifetime_reach_total",
			Help:      "Counter of ttlManager live too long.",
		})

	EinsteinDBNoAvailableConnectionCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "einsteindbclient",
			Name:      "batch_client_no_available_connection_total",
			Help:      "Counter of no available batch client.",
		})
)
