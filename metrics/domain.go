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

// Metrics for the petri package.
var (
	// LoadSchemaCounter records the counter of load schemaReplicant.
	LoadSchemaCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "petri",
			Name:      "load_schema_total",
			Help:      "Counter of load schemaReplicant",
		}, []string{LblType})

	// LoadSchemaDuration records the duration of load schemaReplicant.
	LoadSchemaDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "petri",
			Name:      "load_schema_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in load schemaReplicant.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		})

	// LoadPrivilegeCounter records the counter of load privilege.
	LoadPrivilegeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "petri",
			Name:      "load_privilege_total",
			Help:      "Counter of load privilege",
		}, []string{LblType})

	SchemaValidatorStop       = "stop"
	SchemaValidatorRestart    = "restart"
	SchemaValidatorReset      = "reset"
	SchemaValidatorCacheEmpty = "cache_empty"
	SchemaValidatorCacheMiss  = "cache_miss"
	// HandleSchemaValidate records the counter of handling schemaReplicant validate.
	HandleSchemaValidate = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "petri",
			Name:      "handle_schema_validate",
			Help:      "Counter of handle schemaReplicant validate",
		}, []string{LblType})
)
