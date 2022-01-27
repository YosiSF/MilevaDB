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

// Stochastik metrics.
var (
	StochastikExecuteParseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "parse_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in parse ALLEGROALLEGROSQL.",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 28), // 40us ~ 1.5h
		}, []string{LblALLEGROSQLType})
	StochastikExecuteCompileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "compile_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in query optimize.",
			// Build plan may execute the statement, or allocate block ID, so it might take a long time.
			Buckets: prometheus.ExponentialBuckets(0.00004, 2, 28), // 40us ~ 1.5h
		}, []string{LblALLEGROSQLType})
	StochastikExecuteRunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "execute_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in running executor.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 30), // 100us ~ 15h
		}, []string{LblALLEGROSQLType})
	SchemaLeaseErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "schema_lease_error_total",
			Help:      "Counter of schemaReplicant lease error",
		}, []string{LblType})
	StochastikRetry = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "retry_num",
			Help:      "Bucketed histogram of stochastik retry count.",
			Buckets:   prometheus.LinearBuckets(0, 1, 21), // 0 ~ 20
		})
	StochastikRetryErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "retry_error_total",
			Help:      "Counter of stochastik retry error.",
		}, []string{LblALLEGROSQLType, LblType})

	StochastikRestrictedALLEGROSQLCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "restricted_sql_total",
			Help:      "Counter of internal restricted allegrosql.",
		})

	StatementPerTransaction = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "transaction_statement_num",
			Help:      "Bucketed histogram of statements count in each transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16), // 1 ~ 32768
		}, []string{LbTxnMode, LblType})

	TransactionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "transaction_duration_seconds",
			Help:      "Bucketed histogram of a transaction execution duration, including retry.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LbTxnMode, LblType})

	StatementDeadlockDetectDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "statement_deadlock_detect_duration_seconds",
			Help:      "Bucketed histogram of a statement deadlock detect duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		},
	)

	StatementPessimisticRetryCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "statement_pessimistic_retry_count",
			Help:      "Bucketed histogram of statement pessimistic retry count",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16), // 1 ~ 32768
		})

	StatementLockKeysCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "stochastik",
			Name:      "statement_lock_keys_count",
			Help:      "Keys locking for a single statement",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 21), // 1 ~ 1048576
		})
)

// Label constants.
const (
	LblUnretryable    = "unretryable"
	LblReachMax       = "reach_max"
	LblOK             = "ok"
	LblError          = "error"
	LblCommit         = "commit"
	LblAbort          = "abort"
	LblRollback       = "rollback"
	LblType           = "type"
	LblDb             = "EDB"
	LblResult         = "result"
	LblALLEGROSQLType = "sql_type"
	LblGeneral        = "general"
	LblInternal       = "internal"
	LbTxnMode         = "txn_mode"
	LblPessimistic    = "pessimistic"
	LblOptimistic     = "optimistic"
	LblStore          = "causetstore"
	LblAddress        = "address"
	LblBatchGet       = "batch_get"
	LblGet            = "get"
)
