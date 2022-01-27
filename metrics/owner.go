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
	NewStochastikHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "owner",
			Name:      "new_stochastik_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of new stochastik.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22), // 0.5ms ~ 1048s
		}, []string{LblType, LblResult})

	WatcherClosed     = "watcher_closed"
	Cancelled         = "cancelled"
	Deleted           = "deleted"
	StochastikDone    = "stochastik_done"
	CtxDone           = "context_done"
	WatchOwnerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "owner",
			Name:      "watch_owner_total",
			Help:      "Counter of watch owner.",
		}, []string{LblType, LblResult})

	NoLongerOwner        = "no_longer_owner"
	CampaignOwnerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "owner",
			Name:      "campaign_owner_total",
			Help:      "Counter of campaign owner.",
		}, []string{LblType, LblResult})
)
