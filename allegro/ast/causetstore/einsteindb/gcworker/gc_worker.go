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

package gcworker

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fidel "github.com/einsteindb/fidel/client"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri/infosync"
	"github.com/whtcorpsinc/MilevaDB-Prod/privilege"
	milevadbutil "github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/errorpb"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/metapb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"go.uber.org/zap"
)

// GCWorker periodically triggers GC process on einsteindb server.
type GCWorker struct {
	uuid         string
	desc         string
	causetstore  einsteindb.CausetStorage
	FIDelClient  fidel.Client
	gcIsRunning  bool
	lastFinish   time.Time
	cancel       context.CancelFunc
	done         chan error
	testingKnobs struct {
		scanLocks    func(key []byte) []*einsteindb.Lock
		resolveLocks func(regionID einsteindb.RegionVerID) (ok bool, err error)
	}
}

// NewGCWorker creates a GCWorker instance.
func NewGCWorker(causetstore einsteindb.CausetStorage, FIDelClient fidel.Client) (einsteindb.GCHandler, error) {
	ver, err := causetstore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		causetstore: causetstore,
		FIDelClient: FIDelClient,
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}
	return worker, nil
}

// Start starts the worker.
func (w *GCWorker) Start() {
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go w.start(ctx, &wg)
	wg.Wait() // Wait create stochastik finish in worker, some test code depend on this to avoid race.
}

// Close stops background goroutines.
func (w *GCWorker) Close() {
	w.cancel()
}

const (
	booleanTrue  = "true"
	booleanFalse = "false"

	gcWorkerTickInterval = time.Minute
	gcWorkerLease        = time.Minute * 2
	gcLeaderUUIDKey      = "einsteindb_gc_leader_uuid"
	gcLeaderDescKey      = "einsteindb_gc_leader_desc"
	gcLeaderLeaseKey     = "einsteindb_gc_leader_lease"

	gcLastRunTimeKey       = "einsteindb_gc_last_run_time"
	gcRunIntervalKey       = "einsteindb_gc_run_interval"
	gcDefaultRunInterval   = time.Minute * 10
	gcWaitTime             = time.Minute * 1
	gcRedoDeleteRangeDelay = 24 * time.Hour

	gcLifeTimeKey        = "einsteindb_gc_life_time"
	gcDefaultLifeTime    = time.Minute * 10
	gcMinLifeTime        = time.Minute * 10
	gcSafePointKey       = "einsteindb_gc_safe_point"
	gcConcurrencyKey     = "einsteindb_gc_concurrency"
	gcDefaultConcurrency = 2
	gcMinConcurrency     = 1
	gcMaxConcurrency     = 128
	// We don't want gc to sweep out the cached info belong to other processes, like interlock.
	gcScanLockLimit = einsteindb.ResolvedCacheSize / 2

	gcEnableKey          = "einsteindb_gc_enable"
	gcDefaultEnableValue = true

	gcModeKey         = "einsteindb_gc_mode"
	gcModeCentral     = "central"
	gcModeDistributed = "distributed"
	gcModeDefault     = gcModeDistributed

	gcScanLockModeKey      = "einsteindb_gc_scan_lock_mode"
	gcScanLockModeLegacy   = "legacy"
	gcScanLockModePhysical = "physical"
	gcScanLockModeDefault  = gcScanLockModePhysical

	gcAutoConcurrencyKey     = "einsteindb_gc_auto_concurrency"
	gcDefaultAutoConcurrency = true

	gcWorkerServiceSafePointID = "gc_worker"
)

var gcSafePointCacheInterval = einsteindb.GcSafePointCacheInterval

var gcVariableComments = map[string]string{
	gcLeaderUUIDKey:      "Current GC worker leader UUID. (DO NOT EDIT)",
	gcLeaderDescKey:      "Host name and pid of current GC leader. (DO NOT EDIT)",
	gcLeaderLeaseKey:     "Current GC worker leader lease. (DO NOT EDIT)",
	gcLastRunTimeKey:     "The time when last GC starts. (DO NOT EDIT)",
	gcRunIntervalKey:     "GC run interval, at least 10m, in Go format.",
	gcLifeTimeKey:        "All versions within life time will not be collected by GC, at least 10m, in Go format.",
	gcSafePointKey:       "All versions after safe point can be accessed. (DO NOT EDIT)",
	gcConcurrencyKey:     "How many goroutines used to do GC parallel, [1, 128], default 2",
	gcEnableKey:          "Current GC enable status",
	gcModeKey:            "Mode of GC, \"central\" or \"distributed\"",
	gcAutoConcurrencyKey: "Let MilevaDB pick the concurrency automatically. If set false, einsteindb_gc_concurrency will be used",
	gcScanLockModeKey:    "Mode of scanning locks, \"physical\" or \"legacy\"",
}

func (w *GCWorker) start(ctx context.Context, wg *sync.WaitGroup) {
	logutil.Logger(ctx).Info("[gc worker] start",
		zap.String("uuid", w.uuid))

	w.tick(ctx) // Immediately tick once to initialize configs.
	wg.Done()

	ticker := time.NewTicker(gcWorkerTickInterval)
	defer ticker.Stop()
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("gcWorker",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelGCWorker).Inc()
		}
	}()
	for {
		select {
		case <-ticker.C:
			w.tick(ctx)
		case err := <-w.done:
			w.gcIsRunning = false
			w.lastFinish = time.Now()
			if err != nil {
				logutil.Logger(ctx).Error("[gc worker] runGCJob", zap.Error(err))
			}
		case <-ctx.Done():
			logutil.Logger(ctx).Info("[gc worker] quit", zap.String("uuid", w.uuid))
			return
		}
	}
}

func createStochastik(causetstore ekv.CausetStorage) stochastik.Stochastik {
	for {
		se, err := stochastik.CreateStochastik(causetstore)
		if err != nil {
			logutil.BgLogger().Warn("[gc worker] create stochastik", zap.Error(err))
			continue
		}
		// Disable privilege check for gc worker stochastik.
		privilege.BindPrivilegeManager(se, nil)
		se.GetStochaseinstein_dbars().InRestrictedALLEGROSQL = true
		return se
	}
}

func (w *GCWorker) tick(ctx context.Context) {
	isLeader, err := w.checkLeader()
	if err != nil {
		logutil.Logger(ctx).Warn("[gc worker] check leader", zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_leader").Inc()
		return
	}
	if isLeader {
		err = w.leaderTick(ctx)
		if err != nil {
			logutil.Logger(ctx).Warn("[gc worker] leader tick", zap.Error(err))
		}
	} else {
		// Config metrics should always be uFIDelated by leader, set them to 0 when current instance is not leader.
		metrics.GCConfigGauge.WithLabelValues(gcRunIntervalKey).Set(0)
		metrics.GCConfigGauge.WithLabelValues(gcLifeTimeKey).Set(0)
	}
}

// leaderTick of GC worker checks if it should start a GC job every tick.
func (w *GCWorker) leaderTick(ctx context.Context) error {
	if w.gcIsRunning {
		logutil.Logger(ctx).Info("[gc worker] there's already a gc job running, skipped",
			zap.String("leaderTick on", w.uuid))
		return nil
	}

	ok, safePoint, err := w.prepare()
	if err != nil || !ok {
		if err != nil {
			metrics.GCJobFailureCounter.WithLabelValues("prepare").Inc()
		}
		return errors.Trace(err)
	}
	// When the worker is just started, or an old GC job has just finished,
	// wait a while before starting a new job.
	if time.Since(w.lastFinish) < gcWaitTime {
		logutil.Logger(ctx).Info("[gc worker] another gc job has just finished, skipped.",
			zap.String("leaderTick on ", w.uuid))
		return nil
	}

	concurrency, err := w.getGCConcurrency(ctx)
	if err != nil {
		logutil.Logger(ctx).Info("[gc worker] failed to get gc concurrency.",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		return errors.Trace(err)
	}

	w.gcIsRunning = true
	logutil.Logger(ctx).Info("[gc worker] starts the whole job",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency))
	go func() {
		w.done <- w.runGCJob(ctx, safePoint, concurrency)
	}()
	return nil
}

// prepare checks preconditions for starting a GC job. It returns a bool
// that indicates whether the GC job should start and the new safePoint.
func (w *GCWorker) prepare() (bool, uint64, error) {
	// Add a transaction here is to prevent following situations:
	// 1. GC check gcEnable is true, continue to do GC
	// 2. The user sets gcEnable to false
	// 3. The user gets `einsteindb_gc_safe_point` value is t1, then the user thinks the data after time t1 won't be clean by GC.
	// 4. GC uFIDelate `einsteindb_gc_safe_point` value to t2, continue do GC in this round.
	// Then the data record that has been dropped between time t1 and t2, will be cleaned by GC, but the user thinks the data after t1 won't be clean by GC.
	ctx := context.Background()
	se := createStochastik(w.causetstore)
	defer se.Close()
	_, err := se.Execute(ctx, "BEGIN")
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	doGC, safePoint, err := w.checkPrepare(ctx)
	if doGC {
		err = se.CommitTxn(ctx)
		if err != nil {
			return false, 0, errors.Trace(err)
		}
	} else {
		se.RollbackTxn(ctx)
	}
	return doGC, safePoint, errors.Trace(err)
}

func (w *GCWorker) checkPrepare(ctx context.Context) (bool, uint64, error) {
	enable, err := w.checkGCEnable()
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	if !enable {
		logutil.Logger(ctx).Warn("[gc worker] gc status is disabled.")
		return false, 0, nil
	}
	now, err := w.getOracleTime()
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	ok, err := w.checkGCInterval(now)
	if err != nil || !ok {
		return false, 0, errors.Trace(err)
	}
	newSafePoint, newSafePointValue, err := w.calculateNewSafePoint(ctx, now)
	if err != nil || newSafePoint == nil {
		return false, 0, errors.Trace(err)
	}
	err = w.saveTime(gcLastRunTimeKey, now)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	err = w.saveTime(gcSafePointKey, *newSafePoint)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	return true, newSafePointValue, nil
}

// calculateNewSafePoint uses the current global transaction min start timestamp to calculate the new safe point.
func (w *GCWorker) calSafePointByMinStartTS(ctx context.Context, safePoint time.Time) time.Time {
	kvs, err := w.causetstore.GetSafePointKV().GetWithPrefix(infosync.ServerMinStartTSPath)
	if err != nil {
		logutil.Logger(ctx).Warn("get all minStartTS failed", zap.Error(err))
		return safePoint
	}

	var globalMinStartTS uint64 = math.MaxUint64
	for _, v := range kvs {
		minStartTS, err := strconv.ParseUint(string(v.Value), 10, 64)
		if err != nil {
			logutil.Logger(ctx).Warn("parse minStartTS failed", zap.Error(err))
			continue
		}
		if minStartTS < globalMinStartTS {
			globalMinStartTS = minStartTS
		}
	}

	safePointTS := variable.GoTimeToTS(safePoint)
	if globalMinStartTS < safePointTS {
		safePoint = time.Unix(0, oracle.ExtractPhysical(globalMinStartTS)*1e6)
		logutil.Logger(ctx).Info("[gc worker] gc safepoint blocked by a running stochastik",
			zap.String("uuid", w.uuid),
			zap.Uint64("globalMinStartTS", globalMinStartTS),
			zap.Time("safePoint", safePoint))
	}
	return safePoint
}

func (w *GCWorker) getOracleTime() (time.Time, error) {
	currentVer, err := w.causetstore.CurrentVersion()
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	physical := oracle.ExtractPhysical(currentVer.Ver)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	return time.Unix(sec, nsec), nil
}

func (w *GCWorker) checkGCEnable() (bool, error) {
	return w.loadBooleanWithDefault(gcEnableKey, gcDefaultEnableValue)
}

func (w *GCWorker) checkUseAutoConcurrency() (bool, error) {
	return w.loadBooleanWithDefault(gcAutoConcurrencyKey, gcDefaultAutoConcurrency)
}

func (w *GCWorker) loadBooleanWithDefault(key string, defaultValue bool) (bool, error) {
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		return false, errors.Trace(err)
	}
	if str == "" {
		// Save default value for gc enable key. The default value is always true.
		defaultValueStr := booleanFalse
		if defaultValue {
			defaultValueStr = booleanTrue
		}
		err = w.saveValueToSysTable(key, defaultValueStr)
		if err != nil {
			return defaultValue, errors.Trace(err)
		}
		return defaultValue, nil
	}
	return strings.EqualFold(str, booleanTrue), nil
}

func (w *GCWorker) getGCConcurrency(ctx context.Context) (int, error) {
	useAutoConcurrency, err := w.checkUseAutoConcurrency()
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to load config gc_auto_concurrency. use default value.",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		useAutoConcurrency = gcDefaultAutoConcurrency
	}
	if !useAutoConcurrency {
		return w.loadGCConcurrencyWithDefault()
	}

	stores, err := w.getStoresForGC(ctx)
	concurrency := len(stores)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to get up stores to calculate concurrency. use config.",
			zap.String("uuid", w.uuid),
			zap.Error(err))

		concurrency, err = w.loadGCConcurrencyWithDefault()
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] failed to load gc concurrency from config. use default value.",
				zap.String("uuid", w.uuid),
				zap.Error(err))
			concurrency = gcDefaultConcurrency
		}
	}

	if concurrency == 0 {
		logutil.Logger(ctx).Error("[gc worker] no causetstore is up",
			zap.String("uuid", w.uuid))
		return 0, errors.New("[gc worker] no causetstore is up")
	}

	return concurrency, nil
}

func (w *GCWorker) checkGCInterval(now time.Time) (bool, error) {
	runInterval, err := w.loadDurationWithDefault(gcRunIntervalKey, gcDefaultRunInterval)
	if err != nil {
		return false, errors.Trace(err)
	}
	metrics.GCConfigGauge.WithLabelValues(gcRunIntervalKey).Set(runInterval.Seconds())
	lastRun, err := w.loadTime(gcLastRunTimeKey)
	if err != nil {
		return false, errors.Trace(err)
	}

	if lastRun != nil && lastRun.Add(*runInterval).After(now) {
		logutil.BgLogger().Debug("[gc worker] skipping garbage collection because gc interval hasn't elapsed since last run",
			zap.String("leaderTick on", w.uuid),
			zap.Duration("interval", *runInterval),
			zap.Time("last run", *lastRun))
		return false, nil
	}

	return true, nil
}

// validateGCLifeTime checks whether life time is small than min gc life time.
func (w *GCWorker) validateGCLifeTime(lifeTime time.Duration) (time.Duration, error) {
	if lifeTime >= gcMinLifeTime {
		return lifeTime, nil
	}

	logutil.BgLogger().Info("[gc worker] invalid gc life time",
		zap.Duration("get gc life time", lifeTime),
		zap.Duration("min gc life time", gcMinLifeTime))

	err := w.saveDuration(gcLifeTimeKey, gcMinLifeTime)
	return gcMinLifeTime, err
}

func (w *GCWorker) calculateNewSafePoint(ctx context.Context, now time.Time) (*time.Time, uint64, error) {
	lifeTime, err := w.loadDurationWithDefault(gcLifeTimeKey, gcDefaultLifeTime)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	*lifeTime, err = w.validateGCLifeTime(*lifeTime)
	if err != nil {
		return nil, 0, err
	}
	metrics.GCConfigGauge.WithLabelValues(gcLifeTimeKey).Set(lifeTime.Seconds())
	lastSafePoint, err := w.loadTime(gcSafePointKey)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	safePoint := w.calSafePointByMinStartTS(ctx, now.Add(-*lifeTime))

	safePointValue := oracle.ComposeTS(oracle.GetPhysical(safePoint), 0)
	safePointValue, err = w.setGCWorkerServiceSafePoint(ctx, safePointValue)
	safePoint = oracle.GetTimeFromTS(safePointValue)

	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	// We should never decrease safePoint.
	if lastSafePoint != nil && safePoint.Before(*lastSafePoint) {
		logutil.BgLogger().Info("[gc worker] last safe point is later than current one."+
			"No need to gc."+
			"This might be caused by manually enlarging gc lifetime",
			zap.String("leaderTick on", w.uuid),
			zap.Time("last safe point", *lastSafePoint),
			zap.Time("current safe point", safePoint))
		return nil, 0, nil
	}
	return &safePoint, safePointValue, nil
}

// setGCWorkerServiceSafePoint sets the given safePoint as MilevaDB's service safePoint to FIDel, and returns the current minimal
// service safePoint among all services.
func (w *GCWorker) setGCWorkerServiceSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	// Sets TTL to MAX to make it permanently valid.
	minSafePoint, err := w.FIDelClient.UFIDelateServiceGCSafePoint(ctx, gcWorkerServiceSafePointID, math.MaxInt64, safePoint)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to uFIDelate service safe point",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("uFIDelate_service_safe_point").Inc()
		return 0, errors.Trace(err)
	}
	if minSafePoint < safePoint {
		logutil.Logger(ctx).Info("[gc worker] there's another service in the cluster requires an earlier safe point. "+
			"gc will continue with the earlier one",
			zap.String("uuid", w.uuid),
			zap.Uint64("ourSafePoint", safePoint),
			zap.Uint64("minSafePoint", minSafePoint),
		)
		safePoint = minSafePoint
	}
	return safePoint, nil
}

func (w *GCWorker) runGCJob(ctx context.Context, safePoint uint64, concurrency int) error {
	failpoint.Inject("mockRunGCJobFail", func() {
		failpoint.Return(errors.New("mock failure of runGCJoB"))
	})
	metrics.GCWorkerCounter.WithLabelValues("run_job").Inc()
	usePhysical, err := w.checkUsePhysicalScanLock()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = w.resolveLocks(ctx, safePoint, concurrency, usePhysical)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] resolve locks returns an error",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("resolve_lock").Inc()
		return errors.Trace(err)
	}

	// Save safe point to fidel.
	err = w.saveSafePoint(w.causetstore.GetSafePointKV(), safePoint)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to save safe point to FIDel",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("save_safe_point").Inc()
		return errors.Trace(err)
	}
	// Sleep to wait for all other milevadb instances uFIDelate their safepoint cache.
	time.Sleep(gcSafePointCacheInterval)

	err = w.deleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] delete range returns an error",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("delete_range").Inc()
		return errors.Trace(err)
	}
	err = w.redoDeleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] redo-delete range returns an error",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("redo_delete_range").Inc()
		return errors.Trace(err)
	}

	useDistributedGC, err := w.checkUseDistributedGC()
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to load gc mode, fall back to central mode.",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_gc_mode").Inc()
		useDistributedGC = false
	}

	if useDistributedGC {
		err = w.uploadSafePointToFIDel(ctx, safePoint)
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] failed to upload safe point to FIDel",
				zap.String("uuid", w.uuid),
				zap.Error(err))
			metrics.GCJobFailureCounter.WithLabelValues("upload_safe_point").Inc()
			return errors.Trace(err)
		}
	} else {
		err = w.doGC(ctx, safePoint, concurrency)
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] do GC returns an error",
				zap.String("uuid", w.uuid),
				zap.Error(err))
			metrics.GCJobFailureCounter.WithLabelValues("gc").Inc()
			return errors.Trace(err)
		}
	}

	return nil
}

// deleteRanges processes all delete range records whose ts < safePoint in block `gc_delete_range`
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
func (w *GCWorker) deleteRanges(ctx context.Context, safePoint uint64, concurrency int) error {
	metrics.GCWorkerCounter.WithLabelValues("delete_range").Inc()

	se := createStochastik(w.causetstore)
	ranges, err := soliton.LoadDeleteRanges(se, safePoint)
	se.Close()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("[gc worker] start delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("ranges", len(ranges)))
	startTime := time.Now()
	for _, r := range ranges {
		startKey, endKey := r.Range()

		err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey, concurrency)
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] delete range failed on range",
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			continue
		}

		se := createStochastik(w.causetstore)
		err = soliton.CompleteDeleteRange(se, r)
		se.Close()
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] failed to mark delete range task done",
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save").Inc()
		}
	}
	logutil.Logger(ctx).Info("[gc worker] finish delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

// redoDeleteRanges checks all deleted ranges whose ts is at least `lifetime + 24h` ago. See EinsteinDB RFC #2.
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
func (w *GCWorker) redoDeleteRanges(ctx context.Context, safePoint uint64, concurrency int) error {
	metrics.GCWorkerCounter.WithLabelValues("redo_delete_range").Inc()

	// We check delete range records that are deleted about 24 hours ago.
	redoDeleteRangesTs := safePoint - oracle.ComposeTS(int64(gcRedoDeleteRangeDelay.Seconds())*1000, 0)

	se := createStochastik(w.causetstore)
	ranges, err := soliton.LoadDoneDeleteRanges(se, redoDeleteRangesTs)
	se.Close()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("[gc worker] start redo-delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)))
	startTime := time.Now()
	for _, r := range ranges {
		startKey, endKey := r.Range()

		err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey, concurrency)
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] redo-delete range failed on range",
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			continue
		}

		se := createStochastik(w.causetstore)
		err := soliton.DeleteDoneRecord(se, r)
		se.Close()
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] failed to remove delete_range_done record",
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save_redo").Inc()
		}
	}
	logutil.Logger(ctx).Info("[gc worker] finish redo-delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("redo_delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) doUnsafeDestroyRangeRequest(ctx context.Context, startKey []byte, endKey []byte, concurrency int) error {
	// Get all stores every time deleting a region. So the causetstore list is less probably to be stale.
	stores, err := w.getStoresForGC(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] delete ranges: got an error while trying to get causetstore list from FIDel",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("get_stores").Inc()
		return errors.Trace(err)
	}

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	})

	var wg sync.WaitGroup
	errChan := make(chan error, len(stores))

	for _, causetstore := range stores {
		address := causetstore.Address
		storeID := causetstore.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err1 := w.causetstore.GetEinsteinDBClient().SendRequest(ctx, address, req, einsteindb.UnsafeDestroyRangeTimeout)
			if err1 == nil {
				if resp == nil || resp.Resp == nil {
					err1 = errors.Errorf("unsafe destroy range returns nil response from causetstore %v", storeID)
				} else {
					errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
					if len(errStr) > 0 {
						err1 = errors.Errorf("unsafe destroy range failed on causetstore %v: %s", storeID, errStr)
					}
				}
			}

			if err1 != nil {
				metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("send").Inc()
			}
			errChan <- err1
		}()
	}

	var errs []string
	for range stores {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Errorf("[gc worker] destroy range finished with errors: %v", errs)
	}

	// Notify all affected regions in the range that UnsafeDestroyRange occurs.
	notifyTask := einsteindb.NewNotifyDeleteRangeTask(w.causetstore, startKey, endKey, concurrency)
	err = notifyTask.Execute(ctx)
	if err != nil {
		return errors.Annotate(err, "[gc worker] failed notifying regions affected by UnsafeDestroyRange")
	}

	return nil
}

const (
	engineLabelKey        = "engine"
	engineLabelTiFlash    = "tiflash"
	engineLabelEinsteinDB = "einsteindb"
)

// needsGINTERLOCKerationForStore checks if the causetstore-level requests related to GC needs to be sent to the causetstore. The causetstore-level
// requests includes UnsafeDestroyRange, PhysicalScanLock, etc.
func needsGINTERLOCKerationForStore(causetstore *metapb.CausetStore) (bool, error) {
	// TombStone means the causetstore has been removed from the cluster and there isn't any peer on the causetstore, so needn't do GC for it.
	// Offline means the causetstore is being removed from the cluster and it becomes tombstone after all peers are removed from it,
	// so we need to do GC for it.
	if causetstore.State == metapb.StoreState_Tombstone {
		return false, nil
	}

	engineLabel := ""
	for _, label := range causetstore.GetLabels() {
		if label.GetKey() == engineLabelKey {
			engineLabel = label.GetValue()
			break
		}
	}

	switch engineLabel {
	case engineLabelTiFlash:
		// For a TiFlash node, it uses other approach to delete dropped blocks, so it's safe to skip sending
		// UnsafeDestroyRange requests; it has only learner peers and their data must exist in EinsteinDB, so it's safe to
		// skip physical resolve locks for it.
		return false, nil

	case "":
		// If no engine label is set, it should be a EinsteinDB node.
		fallthrough
	case engineLabelEinsteinDB:
		return true, nil

	default:
		return true, errors.Errorf("unsupported causetstore engine \"%v\" with storeID %v, addr %v",
			engineLabel,
			causetstore.GetId(),
			causetstore.GetAddress())
	}
}

// getStoresForGC gets the list of stores that needs to be processed during GC.
func (w *GCWorker) getStoresForGC(ctx context.Context) ([]*metapb.CausetStore, error) {
	stores, err := w.FIDelClient.GetAllStores(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	upStores := make([]*metapb.CausetStore, 0, len(stores))
	for _, causetstore := range stores {
		needsGINTERLOCK, err := needsGINTERLOCKerationForStore(causetstore)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if needsGINTERLOCK {
			upStores = append(upStores, causetstore)
		}
	}
	return upStores, nil
}

func (w *GCWorker) getStoresMapForGC(ctx context.Context) (map[uint64]*metapb.CausetStore, error) {
	stores, err := w.getStoresForGC(ctx)
	if err != nil {
		return nil, err
	}

	storesMap := make(map[uint64]*metapb.CausetStore, len(stores))
	for _, causetstore := range stores {
		storesMap[causetstore.Id] = causetstore
	}

	return storesMap, nil
}

func (w *GCWorker) loadGCConcurrencyWithDefault() (int, error) {
	str, err := w.loadValueFromSysTable(gcConcurrencyKey)
	if err != nil {
		return gcDefaultConcurrency, errors.Trace(err)
	}
	if str == "" {
		err = w.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcDefaultConcurrency))
		if err != nil {
			return gcDefaultConcurrency, errors.Trace(err)
		}
		return gcDefaultConcurrency, nil
	}

	jobConcurrency, err := strconv.Atoi(str)
	if err != nil {
		return gcDefaultConcurrency, err
	}

	if jobConcurrency < gcMinConcurrency {
		jobConcurrency = gcMinConcurrency
	}

	if jobConcurrency > gcMaxConcurrency {
		jobConcurrency = gcMaxConcurrency
	}

	return jobConcurrency, nil
}

func (w *GCWorker) checkUseDistributedGC() (bool, error) {
	str, err := w.loadValueFromSysTable(gcModeKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if str == "" {
		err = w.saveValueToSysTable(gcModeKey, gcModeDefault)
		if err != nil {
			return false, errors.Trace(err)
		}
		str = gcModeDefault
	}
	if strings.EqualFold(str, gcModeDistributed) {
		return true, nil
	}
	if strings.EqualFold(str, gcModeCentral) {
		return false, nil
	}
	logutil.BgLogger().Warn("[gc worker] distributed mode will be used",
		zap.String("invalid gc mode", str))
	return true, nil
}

func (w *GCWorker) checkUsePhysicalScanLock() (bool, error) {
	str, err := w.loadValueFromSysTable(gcScanLockModeKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if str == "" {
		err = w.saveValueToSysTable(gcScanLockModeKey, gcScanLockModeDefault)
		if err != nil {
			return false, errors.Trace(err)
		}
		str = gcScanLockModeDefault
	}
	if strings.EqualFold(str, gcScanLockModePhysical) {
		return true, nil
	}
	if strings.EqualFold(str, gcScanLockModeLegacy) {
		return false, nil
	}
	logutil.BgLogger().Warn("[gc worker] legacy scan dagger mode will be used",
		zap.String("invalid scan dagger mode", str))
	return false, nil
}

func (w *GCWorker) resolveLocks(ctx context.Context, safePoint uint64, concurrency int, usePhysical bool) (bool, error) {
	if !usePhysical {
		return false, w.legacyResolveLocks(ctx, safePoint, concurrency)
	}

	// First try resolve locks with physical scan
	err := w.resolveLocksPhysical(ctx, safePoint)
	if err == nil {
		return true, nil
	}

	logutil.Logger(ctx).Error("[gc worker] resolve locks with physical scan failed, trying fallback to legacy resolve dagger",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Error(err))

	return false, w.legacyResolveLocks(ctx, safePoint, concurrency)
}

func (w *GCWorker) legacyResolveLocks(ctx context.Context, safePoint uint64, concurrency int) error {
	metrics.GCWorkerCounter.WithLabelValues("resolve_locks").Inc()
	logutil.Logger(ctx).Info("[gc worker] start resolve locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency))
	startTime := time.Now()

	handler := func(ctx context.Context, r ekv.KeyRange) (einsteindb.RangeTaskStat, error) {
		return w.resolveLocksForRange(ctx, safePoint, r.StartKey, r.EndKey)
	}

	runner := einsteindb.NewRangeTaskRunner("resolve-locks-runner", w.causetstore, concurrency, handler)
	// Run resolve dagger on the whole EinsteinDB cluster. Empty keys means the range is unbounded.
	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] resolve locks failed",
			zap.String("uuid", w.uuid),
			zap.Uint64("safePoint", safePoint),
			zap.Error(err))
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("[gc worker] finish resolve locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("regions", runner.CompletedRegions()))
	metrics.GCHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) resolveLocksForRange(ctx context.Context, safePoint uint64, startKey []byte, endKey []byte) (einsteindb.RangeTaskStat, error) {
	// for scan dagger request, we must return all locks even if they are generated
	// by the same transaction. because gc worker need to make sure all locks have been
	// cleaned.
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdScanLock, &kvrpcpb.ScanLockRequest{
		MaxVersion: safePoint,
		Limit:      gcScanLockLimit,
	})

	var stat einsteindb.RangeTaskStat
	key := startKey
	bo := einsteindb.NewBackofferWithVars(ctx, einsteindb.GcResolveLockMaxBackoff, nil)
	failpoint.Inject("setGcResolveMaxBackoff", func(v failpoint.Value) {
		sleep := v.(int)
		// cooperate with github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/invalidCacheAndRetry
		ctx = context.WithValue(ctx, "injectedBackoff", struct{}{})
		bo = einsteindb.NewBackofferWithVars(ctx, sleep, nil)
	})
retryScanAndResolve:
	for {
		select {
		case <-ctx.Done():
			return stat, errors.New("[gc worker] gc job canceled")
		default:
		}

		req.ScanLock().StartKey = key
		loc, err := w.causetstore.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return stat, errors.Trace(err)
		}
		resp, err := w.causetstore.SendReq(bo, req, loc.Region, einsteindb.ReadTimeoutMedium)
		if err != nil {
			return stat, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return stat, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(einsteindb.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return stat, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return stat, errors.Trace(einsteindb.ErrBodyMissing)
		}
		locksResp := resp.Resp.(*kvrpcpb.ScanLockResponse)
		if locksResp.GetError() != nil {
			return stat, errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		locksInfo := locksResp.GetLocks()
		locks := make([]*einsteindb.Lock, len(locksInfo))
		for i := range locksInfo {
			locks[i] = einsteindb.NewLock(locksInfo[i])
		}
		if w.testingKnobs.scanLocks != nil {
			locks = append(locks, w.testingKnobs.scanLocks(key)...)
		}
		for {
			ok, err1 := w.causetstore.GetLockResolver().BatchResolveLocks(bo, locks, loc.Region)
			if w.testingKnobs.resolveLocks != nil {
				ok, err1 = w.testingKnobs.resolveLocks(loc.Region)
			}
			if err1 != nil {
				return stat, errors.Trace(err1)
			}
			if !ok {
				err = bo.Backoff(einsteindb.BoTxnLock, errors.Errorf("remain locks: %d", len(locks)))
				if err != nil {
					return stat, errors.Trace(err)
				}
				stillInSame, refreshedLoc, err := w.tryRelocateLocksRegion(bo, locks)
				if err != nil {
					return stat, errors.Trace(err)
				}
				if stillInSame {
					loc = refreshedLoc
					continue
				}
				continue retryScanAndResolve
			}
			break
		}
		if len(locks) < gcScanLockLimit {
			stat.CompletedRegions++
			key = loc.EndKey
		} else {
			logutil.Logger(ctx).Info("[gc worker] region has more than limit locks",
				zap.String("uuid", w.uuid),
				zap.Uint64("region", loc.Region.GetID()),
				zap.Int("scan dagger limit", gcScanLockLimit))
			metrics.GCRegionTooManyLocksCounter.Inc()
			key = locks[len(locks)-1].Key
		}

		if len(key) == 0 || (len(endKey) != 0 && bytes.Compare(key, endKey) >= 0) {
			break
		}
		bo = einsteindb.NewBackofferWithVars(ctx, einsteindb.GcResolveLockMaxBackoff, nil)
		failpoint.Inject("setGcResolveMaxBackoff", func(v failpoint.Value) {
			sleep := v.(int)
			bo = einsteindb.NewBackofferWithVars(ctx, sleep, nil)
		})
	}
	return stat, nil
}

func (w *GCWorker) tryRelocateLocksRegion(bo *einsteindb.Backoffer, locks []*einsteindb.Lock) (stillInSameRegion bool, refreshedLoc *einsteindb.KeyLocation, err error) {
	if len(locks) == 0 {
		return
	}
	refreshedLoc, err = w.causetstore.GetRegionCache().LocateKey(bo, locks[0].Key)
	if err != nil {
		return
	}
	stillInSameRegion = refreshedLoc.Contains(locks[len(locks)-1].Key)
	return
}

// resolveLocksPhysical uses EinsteinDB's `PhysicalScanLock` to scan stale locks in the cluster and resolve them. It tries to
// ensure no dagger whose ts <= safePoint is left.
func (w *GCWorker) resolveLocksPhysical(ctx context.Context, safePoint uint64) error {
	metrics.GCWorkerCounter.WithLabelValues("resolve_locks_physical").Inc()
	logutil.Logger(ctx).Info("[gc worker] start resolve locks with physical scan locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint))
	startTime := time.Now()

	registeredStores := make(map[uint64]*metapb.CausetStore)
	defer w.removeLockObservers(ctx, safePoint, registeredStores)

	dirtyStores, err := w.getStoresMapForGC(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	for retry := 0; retry < 3; retry++ {
		err = w.registerLockObservers(ctx, safePoint, dirtyStores)
		if err != nil {
			return errors.Trace(err)
		}
		for id, causetstore := range dirtyStores {
			registeredStores[id] = causetstore
		}

		resolvedStores, err := w.physicalScanAndResolveLocks(ctx, safePoint, dirtyStores)
		if err != nil {
			return errors.Trace(err)
		}

		failpoint.Inject("beforeCheckLockObservers", func() {})

		stores, err := w.getStoresMapForGC(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		checkedStores, err := w.checkLockObservers(ctx, safePoint, stores)
		if err != nil {
			return errors.Trace(err)
		}

		for causetstore := range stores {
			if _, ok := checkedStores[causetstore]; ok {
				// The causetstore is resolved and checked.
				if _, ok := resolvedStores[causetstore]; ok {
					delete(stores, causetstore)
				}
				// The causetstore is checked and has been resolved before.
				if _, ok := dirtyStores[causetstore]; !ok {
					delete(stores, causetstore)
				}
				// If the causetstore is checked and not resolved, we can retry to resolve it again, so leave it in dirtyStores.
			} else if _, ok := registeredStores[causetstore]; ok {
				// The causetstore has been registered and it's dirty due to too many collected locks. Fall back to legacy mode.
				// We can't remove the dagger observer from the causetstore and retry the whole procedure because if the causetstore
				// receives duplicated remove and register requests during resolving locks, the causetstore will be cleaned
				// when checking but the dagger observer drops some locks. It may results in missing locks.
				return errors.Errorf("causetstore %v is dirty", causetstore)
			}
		}
		dirtyStores = stores

		// If there are still dirty stores, continue the loop to clean them again.
		// Only dirty stores will be scanned in the next loop.
		if len(dirtyStores) == 0 {
			break
		}
	}

	if len(dirtyStores) != 0 {
		return errors.Errorf("still has %d dirty stores after physical resolve locks", len(dirtyStores))
	}

	logutil.Logger(ctx).Info("[gc worker] finish resolve locks with physical scan locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Duration("takes", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) registerLockObservers(ctx context.Context, safePoint uint64, stores map[uint64]*metapb.CausetStore) error {
	logutil.Logger(ctx).Info("[gc worker] registering dagger observers to einsteindb",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint))

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRegisterLockObserver, &kvrpcpb.RegisterLockObserverRequest{
		MaxTs: safePoint,
	})

	for _, causetstore := range stores {
		address := causetstore.Address

		resp, err := w.causetstore.GetEinsteinDBClient().SendRequest(ctx, address, req, einsteindb.AccessLockObserverTimeout)
		if err != nil {
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(einsteindb.ErrBodyMissing)
		}
		errStr := resp.Resp.(*kvrpcpb.RegisterLockObserverResponse).Error
		if len(errStr) > 0 {
			return errors.Errorf("register dagger observer on causetstore %v returns error: %v", causetstore.Id, errStr)
		}
	}

	return nil
}

// checkLockObservers checks the state of each causetstore's dagger observer. If any dagger collected by the observers, resolve
// them. Returns ids of clean stores.
func (w *GCWorker) checkLockObservers(ctx context.Context, safePoint uint64, stores map[uint64]*metapb.CausetStore) (map[uint64]interface{}, error) {
	logutil.Logger(ctx).Info("[gc worker] checking dagger observers",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint))

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdCheckLockObserver, &kvrpcpb.CheckLockObserverRequest{
		MaxTs: safePoint,
	})
	cleanStores := make(map[uint64]interface{}, len(stores))

	logError := func(causetstore *metapb.CausetStore, err error) {
		logutil.Logger(ctx).Error("[gc worker] failed to check dagger observer for causetstore",
			zap.String("uuid", w.uuid),
			zap.Any("causetstore", causetstore),
			zap.Error(err))
	}

	// When error occurs, this function doesn't fail immediately, but continues without adding the failed causetstore to
	// cleanStores set.
	for _, causetstore := range stores {
		address := causetstore.Address

		resp, err := w.causetstore.GetEinsteinDBClient().SendRequest(ctx, address, req, einsteindb.AccessLockObserverTimeout)
		if err != nil {
			logError(causetstore, err)
			continue
		}
		if resp.Resp == nil {
			logError(causetstore, einsteindb.ErrBodyMissing)
			continue
		}
		respInner := resp.Resp.(*kvrpcpb.CheckLockObserverResponse)
		if len(respInner.Error) > 0 {
			err = errors.Errorf("check dagger observer on causetstore %v returns error: %v", causetstore.Id, respInner.Error)
			logError(causetstore, err)
			continue
		}

		// No need to resolve observed locks on uncleaned stores.
		if !respInner.IsClean {
			logutil.Logger(ctx).Warn("[gc worker] check dagger observer: causetstore is not clean",
				zap.String("uuid", w.uuid),
				zap.Any("causetstore", causetstore))
			continue
		}

		if len(respInner.Locks) > 0 {
			// Resolve the observed locks.
			locks := make([]*einsteindb.Lock, len(respInner.Locks))
			for i, lockInfo := range respInner.Locks {
				locks[i] = einsteindb.NewLock(lockInfo)
			}
			sort.Slice(locks, func(i, j int) bool {
				return bytes.Compare(locks[i].Key, locks[j].Key) < 0
			})
			err = w.resolveLocksAcrossRegions(ctx, locks)

			if err != nil {
				err = errors.Errorf("check dagger observer on causetstore %v returns error: %v", causetstore.Id, respInner.Error)
				logError(causetstore, err)
				continue
			}
		}
		cleanStores[causetstore.Id] = nil
	}

	return cleanStores, nil
}

func (w *GCWorker) removeLockObservers(ctx context.Context, safePoint uint64, stores map[uint64]*metapb.CausetStore) {
	logutil.Logger(ctx).Info("[gc worker] removing dagger observers",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint))

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRemoveLockObserver, &kvrpcpb.RemoveLockObserverRequest{
		MaxTs: safePoint,
	})

	logError := func(causetstore *metapb.CausetStore, err error) {
		logutil.Logger(ctx).Warn("[gc worker] failed to remove dagger observer from causetstore",
			zap.String("uuid", w.uuid),
			zap.Any("causetstore", causetstore),
			zap.Error(err))
	}

	for _, causetstore := range stores {
		address := causetstore.Address

		resp, err := w.causetstore.GetEinsteinDBClient().SendRequest(ctx, address, req, einsteindb.AccessLockObserverTimeout)
		if err != nil {
			logError(causetstore, err)
			continue
		}
		if resp.Resp == nil {
			logError(causetstore, einsteindb.ErrBodyMissing)
			continue
		}
		errStr := resp.Resp.(*kvrpcpb.RemoveLockObserverResponse).Error
		if len(errStr) > 0 {
			err = errors.Errorf("remove dagger observer on causetstore %v returns error: %v", causetstore.Id, errStr)
			logError(causetstore, err)
		}
	}
}

// physicalScanAndResolveLocks performs physical scan dagger and resolves these locks. Returns successful stores
func (w *GCWorker) physicalScanAndResolveLocks(ctx context.Context, safePoint uint64, stores map[uint64]*metapb.CausetStore) (map[uint64]interface{}, error) {
	ctx, cancel := context.WithCancel(ctx)
	// Cancel all spawned goroutines for dagger scanning and resolving.
	defer cancel()

	scanner := newMergeLockScanner(safePoint, w.causetstore.GetEinsteinDBClient(), stores)
	err := scanner.Start(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	taskCh := make(chan []*einsteindb.Lock, len(stores))
	errCh := make(chan error, len(stores))

	wg := &sync.WaitGroup{}
	for range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case locks, ok := <-taskCh:
					if !ok {
						// All locks have been resolved.
						return
					}
					err := w.resolveLocksAcrossRegions(ctx, locks)
					if err != nil {
						logutil.Logger(ctx).Error("resolve locks failed", zap.Error(err))
						errCh <- err
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	for {
		locks := scanner.NextBatch(128)
		if len(locks) == 0 {
			break
		}

		select {
		case taskCh <- locks:
		case err := <-errCh:
			return nil, errors.Trace(err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	close(taskCh)
	// Wait for all locks resolved.
	wg.Wait()

	select {
	case err := <-errCh:
		return nil, errors.Trace(err)
	default:
	}

	return scanner.GetSucceededStores(), nil
}

func (w *GCWorker) resolveLocksAcrossRegions(ctx context.Context, locks []*einsteindb.Lock) error {
	failpoint.Inject("resolveLocksAcrossRegionsErr", func(v failpoint.Value) {
		ms := v.(int)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		failpoint.Return(errors.New("injectedError"))
	})

	bo := einsteindb.NewBackofferWithVars(ctx, einsteindb.GcResolveLockMaxBackoff, nil)

	for {
		if len(locks) == 0 {
			break
		}

		key := locks[0].Key
		loc, err := w.causetstore.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}

		locksInRegion := make([]*einsteindb.Lock, 0)

		for _, dagger := range locks {
			if loc.Contains(dagger.Key) {
				locksInRegion = append(locksInRegion, dagger)
			} else {
				break
			}
		}

		ok, err := w.causetstore.GetLockResolver().BatchResolveLocks(bo, locksInRegion, loc.Region)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			err = bo.Backoff(einsteindb.BoTxnLock, errors.Errorf("remain locks: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}

		// Recreate backoffer for next region
		bo = einsteindb.NewBackofferWithVars(ctx, einsteindb.GcResolveLockMaxBackoff, nil)
		locks = locks[len(locksInRegion):]
	}

	return nil
}

func (w *GCWorker) uploadSafePointToFIDel(ctx context.Context, safePoint uint64) error {
	var newSafePoint uint64
	var err error

	bo := einsteindb.NewBackofferWithVars(ctx, einsteindb.GcOneRegionMaxBackoff, nil)
	for {
		newSafePoint, err = w.FIDelClient.UFIDelateGCSafePoint(ctx, safePoint)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return errors.Trace(err)
			}
			err = bo.Backoff(einsteindb.BoFIDelRPC, errors.Errorf("failed to upload safe point to FIDel, err: %v", err))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		break
	}

	if newSafePoint != safePoint {
		logutil.Logger(ctx).Warn("[gc worker] FIDel rejected safe point",
			zap.String("uuid", w.uuid),
			zap.Uint64("our safe point", safePoint),
			zap.Uint64("using another safe point", newSafePoint))
		return errors.Errorf("FIDel rejected our safe point %v but is using another safe point %v", safePoint, newSafePoint)
	}
	logutil.Logger(ctx).Info("[gc worker] sent safe point to FIDel",
		zap.String("uuid", w.uuid),
		zap.Uint64("safe point", safePoint))
	return nil
}

func (w *GCWorker) doGCForRange(ctx context.Context, startKey []byte, endKey []byte, safePoint uint64) (einsteindb.RangeTaskStat, error) {
	var stat einsteindb.RangeTaskStat
	defer func() {
		metrics.GCCausetActionRegionResultCounter.WithLabelValues("success").Add(float64(stat.CompletedRegions))
		metrics.GCCausetActionRegionResultCounter.WithLabelValues("fail").Add(float64(stat.FailedRegions))
	}()
	key := startKey
	for {
		bo := einsteindb.NewBackofferWithVars(ctx, einsteindb.GcOneRegionMaxBackoff, nil)
		loc, err := w.causetstore.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return stat, errors.Trace(err)
		}

		var regionErr *errorpb.Error
		regionErr, err = w.doGCForRegion(bo, safePoint, loc.Region)

		// we check regionErr here first, because we know 'regionErr' and 'err' should not return together, to keep it to
		// make the process correct.
		if regionErr != nil {
			err = bo.Backoff(einsteindb.BoRegionMiss, errors.New(regionErr.String()))
			if err == nil {
				continue
			}
		}

		if err != nil {
			logutil.BgLogger().Warn("[gc worker]",
				zap.String("uuid", w.uuid),
				zap.String("gc for range", fmt.Sprintf("[%d, %d)", startKey, endKey)),
				zap.Uint64("safePoint", safePoint),
				zap.Error(err))
			stat.FailedRegions++
		} else {
			stat.CompletedRegions++
		}

		key = loc.EndKey
		if len(key) == 0 || bytes.Compare(key, endKey) >= 0 {
			break
		}
	}

	return stat, nil
}

// doGCForRegion used for gc for region.
// these two errors should not return together, for more, see the func 'doGC'
func (w *GCWorker) doGCForRegion(bo *einsteindb.Backoffer, safePoint uint64, region einsteindb.RegionVerID) (*errorpb.Error, error) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdGC, &kvrpcpb.GCRequest{
		SafePoint: safePoint,
	})

	resp, err := w.causetstore.SendReq(bo, req, region, einsteindb.GCTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if regionErr != nil {
		return regionErr, nil
	}

	if resp.Resp == nil {
		return nil, errors.Trace(einsteindb.ErrBodyMissing)
	}
	gcResp := resp.Resp.(*kvrpcpb.GCResponse)
	if gcResp.GetError() != nil {
		return nil, errors.Errorf("unexpected gc error: %s", gcResp.GetError())
	}

	return nil, nil
}

func (w *GCWorker) doGC(ctx context.Context, safePoint uint64, concurrency int) error {
	metrics.GCWorkerCounter.WithLabelValues("do_gc").Inc()
	logutil.Logger(ctx).Info("[gc worker] start doing gc for all keys",
		zap.String("uuid", w.uuid),
		zap.Int("concurrency", concurrency),
		zap.Uint64("safePoint", safePoint))
	startTime := time.Now()

	runner := einsteindb.NewRangeTaskRunner(
		"gc-runner",
		w.causetstore,
		concurrency,
		func(ctx context.Context, r ekv.KeyRange) (einsteindb.RangeTaskStat, error) {
			return w.doGCForRange(ctx, r.StartKey, r.EndKey, safePoint)
		})

	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		logutil.Logger(ctx).Warn("[gc worker] failed to do gc for all keys",
			zap.String("uuid", w.uuid),
			zap.Int("concurrency", concurrency),
			zap.Error(err))
		return errors.Trace(err)
	}

	successRegions := runner.CompletedRegions()
	failedRegions := runner.FailedRegions()

	logutil.Logger(ctx).Info("[gc worker] finished doing gc for all keys",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("successful regions", successRegions),
		zap.Int("failed regions", failedRegions),
		zap.Duration("total cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("do_gc").Observe(time.Since(startTime).Seconds())

	return nil
}

func (w *GCWorker) checkLeader() (bool, error) {
	metrics.GCWorkerCounter.WithLabelValues("check_leader").Inc()
	se := createStochastik(w.causetstore)
	defer se.Close()

	ctx := context.Background()
	_, err := se.Execute(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	leader, err := w.loadValueFromSysTable(gcLeaderUUIDKey)
	if err != nil {
		se.RollbackTxn(ctx)
		return false, errors.Trace(err)
	}
	logutil.BgLogger().Debug("[gc worker] got leader", zap.String("uuid", leader))
	if leader == w.uuid {
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = se.CommitTxn(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}

	se.RollbackTxn(ctx)

	_, err = se.Execute(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	lease, err := w.loadTime(gcLeaderLeaseKey)
	if err != nil {
		se.RollbackTxn(ctx)
		return false, errors.Trace(err)
	}
	if lease == nil || lease.Before(time.Now()) {
		logutil.BgLogger().Debug("[gc worker] register as leader",
			zap.String("uuid", w.uuid))
		metrics.GCWorkerCounter.WithLabelValues("register_leader").Inc()

		err = w.saveValueToSysTable(gcLeaderUUIDKey, w.uuid)
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = w.saveValueToSysTable(gcLeaderDescKey, w.desc)
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = se.CommitTxn(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	se.RollbackTxn(ctx)
	return false, nil
}

func (w *GCWorker) saveSafePoint(ekv einsteindb.SafePointKV, t uint64) error {
	s := strconv.FormatUint(t, 10)
	err := ekv.Put(einsteindb.GcSavedSafePoint, s)
	if err != nil {
		logutil.BgLogger().Error("save safepoint failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (w *GCWorker) saveTime(key string, t time.Time) error {
	err := w.saveValueToSysTable(key, t.Format(milevadbutil.GCTimeFormat))
	return errors.Trace(err)
}

func (w *GCWorker) loadTime(key string) (*time.Time, error) {
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if str == "" {
		return nil, nil
	}
	t, err := milevadbutil.CompatibleParseGCTime(str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &t, nil
}

func (w *GCWorker) saveDuration(key string, d time.Duration) error {
	err := w.saveValueToSysTable(key, d.String())
	return errors.Trace(err)
}

func (w *GCWorker) loadDuration(key string) (*time.Duration, error) {
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if str == "" {
		return nil, nil
	}
	d, err := time.ParseDuration(str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &d, nil
}

func (w *GCWorker) loadDurationWithDefault(key string, def time.Duration) (*time.Duration, error) {
	d, err := w.loadDuration(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if d == nil {
		err = w.saveDuration(key, def)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &def, nil
	}
	return d, nil
}

func (w *GCWorker) loadValueFromSysTable(key string) (string, error) {
	ctx := context.Background()
	se := createStochastik(w.causetstore)
	defer se.Close()
	stmt := fmt.Sprintf(`SELECT HIGH_PRIORITY (variable_value) FROM allegrosql.milevadb WHERE variable_name='%s' FOR UFIDelATE`, key)
	rs, err := se.Execute(ctx, stmt)
	if len(rs) > 0 {
		defer terror.Call(rs[0].Close)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	req := rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	if err != nil {
		return "", errors.Trace(err)
	}
	if req.NumRows() == 0 {
		logutil.BgLogger().Debug("[gc worker] load ekv",
			zap.String("key", key))
		return "", nil
	}
	value := req.GetRow(0).GetString(0)
	logutil.BgLogger().Debug("[gc worker] load ekv",
		zap.String("key", key),
		zap.String("value", value))
	return value, nil
}

func (w *GCWorker) saveValueToSysTable(key, value string) error {
	stmt := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('%[1]s', '%[2]s', '%[3]s')
			       ON DUPLICATE KEY
			       UFIDelATE variable_value = '%[2]s', comment = '%[3]s'`,
		key, value, gcVariableComments[key])
	se := createStochastik(w.causetstore)
	defer se.Close()
	_, err := se.Execute(context.Background(), stmt)
	logutil.BgLogger().Debug("[gc worker] save ekv",
		zap.String("key", key),
		zap.String("value", value),
		zap.Error(err))
	return errors.Trace(err)
}

// RunGCJob sends GC command to KV. It is exported for ekv api, do not use it with GCWorker at the same time.
func RunGCJob(ctx context.Context, s einsteindb.CausetStorage, fidel fidel.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		causetstore: s,
		uuid:        identifier,
		FIDelClient: fidel,
	}

	if concurrency <= 0 {
		return errors.Errorf("[gc worker] gc concurrency should greater than 0, current concurrency: %v", concurrency)
	}

	safePoint, err := gcWorker.setGCWorkerServiceSafePoint(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = gcWorker.resolveLocks(ctx, safePoint, concurrency, false)
	if err != nil {
		return errors.Trace(err)
	}

	err = gcWorker.saveSafePoint(gcWorker.causetstore.GetSafePointKV(), safePoint)
	if err != nil {
		return errors.Trace(err)
	}
	// Sleep to wait for all other milevadb instances uFIDelate their safepoint cache.
	time.Sleep(gcSafePointCacheInterval)
	err = gcWorker.doGC(ctx, safePoint, concurrency)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunDistributedGCJob notifies EinsteinDBs to do GC. It is exported for ekv api, do not use it with GCWorker at the same time.
// This function may not finish immediately because it may take some time to do resolveLocks.
// Param concurrency specifies the concurrency of resolveLocks phase.
func RunDistributedGCJob(ctx context.Context, s einsteindb.CausetStorage, fidel fidel.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		causetstore: s,
		uuid:        identifier,
		FIDelClient: fidel,
	}

	safePoint, err := gcWorker.setGCWorkerServiceSafePoint(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = gcWorker.resolveLocks(ctx, safePoint, concurrency, false)
	if err != nil {
		return errors.Trace(err)
	}

	// Save safe point to fidel.
	err = gcWorker.saveSafePoint(gcWorker.causetstore.GetSafePointKV(), safePoint)
	if err != nil {
		return errors.Trace(err)
	}
	// Sleep to wait for all other milevadb instances uFIDelate their safepoint cache.
	time.Sleep(gcSafePointCacheInterval)

	err = gcWorker.uploadSafePointToFIDel(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunResolveLocks resolves all locks before the safePoint and returns whether the physical scan mode is used.
// It is exported only for test, do not use it in the production environment.
func RunResolveLocks(ctx context.Context, s einsteindb.CausetStorage, fidel fidel.Client, safePoint uint64, identifier string, concurrency int, usePhysical bool) (bool, error) {
	gcWorker := &GCWorker{
		causetstore: s,
		uuid:        identifier,
		FIDelClient: fidel,
	}
	return gcWorker.resolveLocks(ctx, safePoint, concurrency, usePhysical)
}

// MockGCWorker is for test.
type MockGCWorker struct {
	worker *GCWorker
}

// NewMockGCWorker creates a MockGCWorker instance ONLY for test.
func NewMockGCWorker(causetstore einsteindb.CausetStorage) (*MockGCWorker, error) {
	ver, err := causetstore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		causetstore: causetstore,
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}
	return &MockGCWorker{worker: worker}, nil
}

// DeleteRanges calls deleteRanges internally, just for test.
func (w *MockGCWorker) DeleteRanges(ctx context.Context, safePoint uint64) error {
	logutil.Logger(ctx).Error("deleteRanges is called")
	return w.worker.deleteRanges(ctx, safePoint, 1)
}

const scanLockResultBufferSize = 128

// mergeLockScanner is used to scan specified stores by using PhysicalScanLock. For multiple stores, the scanner will
// merge the scan results of each causetstore, and remove the duplicating items from different stores.
type mergeLockScanner struct {
	safePoint     uint64
	client        einsteindb.Client
	stores        map[uint64]*metapb.CausetStore
	receivers     mergeReceiver
	currentLock   *einsteindb.Lock
	scanLockLimit uint32
}

type receiver struct {
	Ch       <-chan scanLockResult
	StoreID  uint64
	NextLock *einsteindb.Lock
	Err      error
}

func (r *receiver) PeekNextLock() *einsteindb.Lock {
	if r.NextLock != nil {
		return r.NextLock
	}
	result, ok := <-r.Ch
	if !ok {
		return nil
	}
	r.Err = result.Err
	r.NextLock = result.Lock
	return r.NextLock
}

func (r *receiver) TakeNextLock() *einsteindb.Lock {
	dagger := r.PeekNextLock()
	r.NextLock = nil
	return dagger
}

// mergeReceiver is a list of receivers
type mergeReceiver []*receiver

func (r mergeReceiver) Len() int {
	return len(r)
}

func (r mergeReceiver) Less(i, j int) bool {
	lhs := r[i].PeekNextLock()
	rhs := r[j].PeekNextLock()
	// nil which means the receiver has finished should be the greatest one.
	if lhs == nil {
		// lhs >= rhs
		return false
	}
	if rhs == nil {
		// lhs != nil, so lhs < rhs
		return true
	}
	ord := bytes.Compare(lhs.Key, rhs.Key)
	return ord < 0 || (ord == 0 && lhs.TxnID < rhs.TxnID)
}

func (r mergeReceiver) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r *mergeReceiver) Push(x interface{}) {
	*r = append(*r, x.(*receiver))
}

func (r *mergeReceiver) Pop() interface{} {
	receivers := *r
	res := receivers[len(receivers)-1]
	*r = receivers[:len(receivers)-1]
	return res
}

type scanLockResult struct {
	Lock *einsteindb.Lock
	Err  error
}

func newMergeLockScanner(safePoint uint64, client einsteindb.Client, stores map[uint64]*metapb.CausetStore) *mergeLockScanner {
	return &mergeLockScanner{
		safePoint:     safePoint,
		client:        client,
		stores:        stores,
		scanLockLimit: gcScanLockLimit,
	}
}

// Start initializes the scanner and enables retrieving items from the scanner.
func (s *mergeLockScanner) Start(ctx context.Context) error {
	receivers := make([]*receiver, 0, len(s.stores))

	for storeID, causetstore := range s.stores {
		ch := make(chan scanLockResult, scanLockResultBufferSize)
		store1 := causetstore
		go func() {
			defer close(ch)

			err := s.physicalScanLocksForStore(ctx, s.safePoint, store1, ch)
			if err != nil {
				logutil.Logger(ctx).Error("physical scan dagger for causetstore encountered error",
					zap.Uint64("safePoint", s.safePoint),
					zap.Any("causetstore", store1),
					zap.Error(err))

				select {
				case ch <- scanLockResult{Err: err}:
				case <-ctx.Done():
				}
			}
		}()
		receivers = append(receivers, &receiver{Ch: ch, StoreID: storeID})
	}

	s.startWithReceivers(receivers)

	return nil
}

func (s *mergeLockScanner) startWithReceivers(receivers []*receiver) {
	s.receivers = receivers
	heap.Init(&s.receivers)
}

func (s *mergeLockScanner) Next() *einsteindb.Lock {
	for {
		nextReceiver := s.receivers[0]
		nextLock := nextReceiver.TakeNextLock()
		heap.Fix(&s.receivers, 0)

		if nextLock == nil {
			return nil
		}
		if s.currentLock == nil || !bytes.Equal(s.currentLock.Key, nextLock.Key) || s.currentLock.TxnID != nextLock.TxnID {
			s.currentLock = nextLock
			return nextLock
		}
	}
}

func (s *mergeLockScanner) NextBatch(batchSize int) []*einsteindb.Lock {
	result := make([]*einsteindb.Lock, 0, batchSize)
	for len(result) < batchSize {
		dagger := s.Next()
		if dagger == nil {
			break
		}
		result = append(result, dagger)
	}
	return result
}

// GetSucceededStores gets a set of successfully scanned stores. Only call this after finishing scanning all locks.
func (s *mergeLockScanner) GetSucceededStores() map[uint64]interface{} {
	stores := make(map[uint64]interface{}, len(s.receivers))
	for _, receiver := range s.receivers {
		if receiver.Err == nil {
			stores[receiver.StoreID] = nil
		}
	}
	return stores
}

func (s *mergeLockScanner) physicalScanLocksForStore(ctx context.Context, safePoint uint64, causetstore *metapb.CausetStore, lockCh chan<- scanLockResult) error {
	address := causetstore.Address
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdPhysicalScanLock, &kvrpcpb.PhysicalScanLockRequest{
		MaxTs: safePoint,
		Limit: s.scanLockLimit,
	})

	nextKey := make([]byte, 0)

	for {
		req.PhysicalScanLock().StartKey = nextKey

		response, err := s.client.SendRequest(ctx, address, req, einsteindb.ReadTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		if response.Resp == nil {
			return errors.Trace(einsteindb.ErrBodyMissing)
		}
		resp := response.Resp.(*kvrpcpb.PhysicalScanLockResponse)
		if len(resp.Error) > 0 {
			return errors.Errorf("physical scan dagger received error from causetstore: %v", resp.Error)
		}

		if len(resp.Locks) == 0 {
			break
		}

		nextKey = resp.Locks[len(resp.Locks)-1].Key
		nextKey = append(nextKey, 0)

		for _, lockInfo := range resp.Locks {
			select {
			case lockCh <- scanLockResult{Lock: einsteindb.NewLock(lockInfo)}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if len(resp.Locks) < int(s.scanLockLimit) {
			break
		}
	}

	return nil
}
