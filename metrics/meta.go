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
)

// Metrics
var (
	GlobalAutoID      = "global"
	TableAutoIDAlloc  = "alloc"
	TableAutoIDRebase = "rebase"
	AutoIDHistogram   = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "autoid",
			Name:      "operation_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled autoid.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType, LblResult})

	GetSchemaDiff    = "get_schema_diff"
	SetSchemaDiff    = "set_schema_diff"
	GetDBSJobByIdx   = "get_dbs_job"
	UFIDelateDBSJob  = "uFIDelate_dbs_job"
	GetHistoryDBSJob = "get_history_dbs_job"

	MetaHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "meta",
			Name:      "operation_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of milevadb meta data operations.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType, LblResult})
)
