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

var (
	// PanicCounter measures the count of panics.
	PanicCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "server",
			Name:      "panic_total",
			Help:      "Counter of panic.",
		}, []string{LblType})
)

// metrics labels.
const (
	LabelStochastik = "stochastik"
	LabelPetri      = "petri"
	LabelDBSOwner   = "dbs-owner"
	LabelDBS        = "dbs"
	LabelDBSWorker  = "dbs-worker"
	LabelDBSSyncer  = "dbs-syncer"
	LabelGCWorker   = "gcworker"
	LabelAnalyze    = "analyze"

	LabelBatchRecvLoop = "batch-recv-loop"
	LabelBatchSendLoop = "batch-send-loop"

	opSucc   = "ok"
	opFailed = "err"

	LabelSINTERLOCKe      = "sINTERLOCKe"
	SINTERLOCKeGlobal     = "global"
	SINTERLOCKeStochastik = "stochastik"
)

// RetLabel returns "ok" when err == nil and "err" when err != nil.
// This could be useful when you need to observe the operation result.
func RetLabel(err error) string {
	if err == nil {
		return opSucc
	}
	return opFailed
}

// RegisterMetrics registers the metrics which are ONLY used in MilevaDB server.
func RegisterMetrics() {
	prometheus.MustRegister(AutoAnalyzeCounter)
	prometheus.MustRegister(AutoAnalyzeHistogram)
	prometheus.MustRegister(AutoIDHistogram)
	prometheus.MustRegister(BatchAddIdxHistogram)
	prometheus.MustRegister(BindUsageCounter)
	prometheus.MustRegister(BindTotalGauge)
	prometheus.MustRegister(BindMemoryUsage)
	prometheus.MustRegister(CampaignOwnerCounter)
	prometheus.MustRegister(ConnGauge)
	prometheus.MustRegister(DisconnectionCounter)
	prometheus.MustRegister(PreparedStmtGauge)
	prometheus.MustRegister(CriticalErrorCounter)
	prometheus.MustRegister(DBSCounter)
	prometheus.MustRegister(BackfillTotalCounter)
	prometheus.MustRegister(AddIndexProgress)
	prometheus.MustRegister(DBSWorkerHistogram)
	prometheus.MustRegister(DeploySyncerHistogram)
	prometheus.MustRegister(DistALLEGROSQLPartialCountHistogram)
	prometheus.MustRegister(DistALLEGROSQLQueryHistogram)
	prometheus.MustRegister(DistALLEGROSQLScanKeysHistogram)
	prometheus.MustRegister(DistALLEGROSQLScanKeysPartialHistogram)
	prometheus.MustRegister(DumpFeedbackCounter)
	prometheus.MustRegister(ExecuteErrorCounter)
	prometheus.MustRegister(ExecutorCounter)
	prometheus.MustRegister(GetTokenDurationHistogram)
	prometheus.MustRegister(HandShakeErrorCounter)
	prometheus.MustRegister(HandleJobHistogram)
	prometheus.MustRegister(SignificantFeedbackCounter)
	prometheus.MustRegister(FastAnalyzeHistogram)
	prometheus.MustRegister(JobsGauge)
	prometheus.MustRegister(KeepAliveCounter)
	prometheus.MustRegister(LoadPrivilegeCounter)
	prometheus.MustRegister(LoadSchemaCounter)
	prometheus.MustRegister(LoadSchemaDuration)
	prometheus.MustRegister(MetaHistogram)
	prometheus.MustRegister(NewStochastikHistogram)
	prometheus.MustRegister(OwnerHandleSyncerHistogram)
	prometheus.MustRegister(PanicCounter)
	prometheus.MustRegister(PlanCacheCounter)
	prometheus.MustRegister(PseudoEstimation)
	prometheus.MustRegister(PacketIOHistogram)
	prometheus.MustRegister(QueryDurationHistogram)
	prometheus.MustRegister(QueryTotalCounter)
	prometheus.MustRegister(SchemaLeaseErrorCounter)
	prometheus.MustRegister(ServerEventCounter)
	prometheus.MustRegister(StochastikExecuteCompileDuration)
	prometheus.MustRegister(StochastikExecuteParseDuration)
	prometheus.MustRegister(StochastikExecuteRunDuration)
	prometheus.MustRegister(StochastikRestrictedALLEGROSQLCounter)
	prometheus.MustRegister(StochastikRetry)
	prometheus.MustRegister(StochastikRetryErrorCounter)
	prometheus.MustRegister(StatementPerTransaction)
	prometheus.MustRegister(StatsInaccuracyRate)
	prometheus.MustRegister(StmtNodeCounter)
	prometheus.MustRegister(DbStmtNodeCounter)
	prometheus.MustRegister(StoreQueryFeedbackCounter)
	prometheus.MustRegister(GetStoreLimitErrorCounter)
	prometheus.MustRegister(EinsteinDBBackoffHistogram)
	prometheus.MustRegister(EinsteinDBinterlocking_directorateHistogram)
	prometheus.MustRegister(EinsteinDBLoadSafepointCounter)
	prometheus.MustRegister(EinsteinDBLockResolverCounter)
	prometheus.MustRegister(EinsteinDBRawkvCmdHistogram)
	prometheus.MustRegister(EinsteinDBRawkvSizeHistogram)
	prometheus.MustRegister(EinsteinDBRegionCacheCounter)
	prometheus.MustRegister(EinsteinDBRegionErrorCounter)
	prometheus.MustRegister(EinsteinDBSecondaryLockCleanupFailureCounter)
	prometheus.MustRegister(EinsteinDBSendReqHistogram)
	prometheus.MustRegister(EinsteinDBTxnCmdHistogram)
	prometheus.MustRegister(EinsteinDBTxnRegionsNumHistogram)
	prometheus.MustRegister(EinsteinDBTxnWriteKVCountHistogram)
	prometheus.MustRegister(EinsteinDBTxnWriteSizeHistogram)
	prometheus.MustRegister(EinsteinDBLocalLatchWaitTimeHistogram)
	prometheus.MustRegister(TimeJumpBackCounter)
	prometheus.MustRegister(TransactionDuration)
	prometheus.MustRegister(StatementDeadlockDetectDuration)
	prometheus.MustRegister(StatementPessimisticRetryCount)
	prometheus.MustRegister(StatementLockKeysCount)
	prometheus.MustRegister(UFIDelateSelfVersionHistogram)
	prometheus.MustRegister(UFIDelateStatsCounter)
	prometheus.MustRegister(WatchOwnerCounter)
	prometheus.MustRegister(GCCausetActionRegionResultCounter)
	prometheus.MustRegister(GCConfigGauge)
	prometheus.MustRegister(GCHistogram)
	prometheus.MustRegister(GCJobFailureCounter)
	prometheus.MustRegister(GCRegionTooManyLocksCounter)
	prometheus.MustRegister(GCWorkerCounter)
	prometheus.MustRegister(GCUnsafeDestroyRangeFailuresCounterVec)
	prometheus.MustRegister(TSFutureWaitDuration)
	prometheus.MustRegister(TotalQueryProcHistogram)
	prometheus.MustRegister(TotalINTERLOCKProcHistogram)
	prometheus.MustRegister(TotalINTERLOCKWaitHistogram)
	prometheus.MustRegister(EinsteinDBPendingBatchRequests)
	prometheus.MustRegister(EinsteinDBStatusDuration)
	prometheus.MustRegister(EinsteinDBStatusCounter)
	prometheus.MustRegister(EinsteinDBBatchWaitDuration)
	prometheus.MustRegister(EinsteinDBBatchClientUnavailable)
	prometheus.MustRegister(EinsteinDBBatchClientWaitEstablish)
	prometheus.MustRegister(EinsteinDBRangeTaskStats)
	prometheus.MustRegister(EinsteinDBRangeTaskPushDuration)
	prometheus.MustRegister(HandleSchemaValidate)
	prometheus.MustRegister(EinsteinDBTokenWaitDuration)
	prometheus.MustRegister(EinsteinDBTxnHeartBeatHistogram)
	prometheus.MustRegister(EinsteinDBPessimisticLockKeysDuration)
	prometheus.MustRegister(GRPCConnTransientFailureCounter)
	prometheus.MustRegister(EinsteinDBTTLLifeTimeReachCounter)
	prometheus.MustRegister(EinsteinDBNoAvailableConnectionCounter)
	prometheus.MustRegister(MaxProcs)
}
