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

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
)

var (
	// ResetblockPlanCacheCounterFortTest be used to support reset counter in test.
	ResetblockPlanCacheCounterFortTest = false
)

// Metrics
var (
	PacketIOHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "packet_io_bytes",
			Help:      "Bucketed histogram of packet IO bytes.",
			Buckets:   prometheus.ExponentialBuckets(4, 4, 21), // 4Bytes ~ 4TB
		}, []string{LblType})

	QueryDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "handle_query_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblALLEGROSQLType})

	QueryTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "query_total",
			Help:      "Counter of queries.",
		}, []string{LblType, LblResult})

	ConnGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "connections",
			Help:      "Number of connections.",
		})

	DisconnectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "disconnection_total",
			Help:      "Counter of connections disconnected.",
		}, []string{LblResult})

	PreparedStmtGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "milevadb",
		Subsystem: "server",
		Name:      "prepared_stmts",
		Help:      "number of prepared statements.",
	})

	ExecuteErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "execute_error_total",
			Help:      "Counter of execute errors.",
		}, []string{LblType})

	CriticalErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "critical_error_total",
			Help:      "Counter of critical errors.",
		})

	EventStart        = "start"
	EventGracefulDown = "graceful_shutdown"
	// Eventkill occurs when the server.Kill() function is called.
	EventKill          = "kill"
	EventClose         = "close"
	ServerEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "event_total",
			Help:      "Counter of milevadb-server event.",
		}, []string{LblType})

	TimeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "monitor",
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})

	KeepAliveCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "monitor",
			Name:      "keep_alive_total",
			Help:      "Counter of MilevaDB keep alive.",
		})

	PlanCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "plan_cache_total",
			Help:      "Counter of query using plan cache.",
		}, []string{LblType})

	HandShakeErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "handshake_error_total",
			Help:      "Counter of hand shake error.",
		},
	)

	GetTokenDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "get_token_duration_seconds",
			Help:      "Duration (us) for getting token, it should be small until concurrency limit is reached.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1us ~ 528s
		})

	TotalQueryProcHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "slow_query_process_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of of slow queries.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		})
	TotalINTERLOCKProcHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "slow_query_INTERLOCK_duration_seconds",
			Help:      "Bucketed histogram of all INTERLOCK processing time (s) of of slow queries.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		})
	TotalINTERLOCKWaitHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "slow_query_wait_duration_seconds",
			Help:      "Bucketed histogram of all INTERLOCK waiting time (s) of of slow queries.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		})

	MaxProcs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "maxprocs",
			Help:      "The value of GOMAXPROCS.",
		})
)

// ExecuteErrorToLabel converts an execute error to label.
func ExecuteErrorToLabel(err error) string {
	err = errors.Cause(err)
	switch x := err.(type) {
	case *terror.Error:
		return string(x.RFCCode())
	default:
		return "unknown"
	}
}
