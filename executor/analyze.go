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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/distsql"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/ranger"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/sqlexec"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"go.uber.org/zap"
)

var _ Executor = &AnalyzeExec{}

// AnalyzeExec represents Analyze executor.
type AnalyzeExec struct {
	baseExecutor
	tasks []*analyzeTask
	wg    *sync.WaitGroup
}

var (
	// RandSeed is the seed for randing package.
	// It's public for test.
	RandSeed = int64(1)
)

const (
	maxRegionSampleSize = 1000
	maxSketchSize       = 10000
)

// Next implements the Executor Next interface.
func (e *AnalyzeExec) Next(ctx context.Context, req *chunk.Chunk) error {
	concurrency, err := getBuildStatsConcurrency(e.ctx)
	if err != nil {
		return err
	}
	taskCh := make(chan *analyzeTask, len(e.tasks))
	resultCh := make(chan analyzeResult, len(e.tasks))
	e.wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.analyzeWorker(taskCh, resultCh, i == 0)
	}
	for _, task := range e.tasks {
		statistics.AddNewAnalyzeJob(task.job)
	}
	for _, task := range e.tasks {
		taskCh <- task
	}
	close(taskCh)
	statsHandle := petri.GetPetri(e.ctx).StatsHandle()
	panicCnt := 0
	for panicCnt < concurrency {
		result, ok := <-resultCh
		if !ok {
			break
		}
		if result.Err != nil {
			err = result.Err
			if err == errAnalyzeWorkerPanic {
				panicCnt++
			} else {
				logutil.Logger(ctx).Error("analyze failed", zap.Error(err))
			}
			result.job.Finish(true)
			continue
		}
		for i, hg := range result.Hist {
			err1 := statsHandle.SaveStatsToStorage(result.BlockID.PersistID, result.Count, result.IsIndex, hg, result.Cms[i], 1)
			if err1 != nil {
				err = err1
				logutil.Logger(ctx).Error("save stats to storage failed", zap.Error(err))
				result.job.Finish(true)
				continue
			}
		}
		if err1 := statsHandle.SaveExtendedStatsToStorage(result.BlockID.PersistID, result.ExtStats, false); err1 != nil {
			err = err1
			logutil.Logger(ctx).Error("save extended stats to storage failed", zap.Error(err))
			result.job.Finish(true)
		} else {
			result.job.Finish(false)
		}
	}
	for _, task := range e.tasks {
		statistics.MoveToHistory(task.job)
	}
	if err != nil {
		return err
	}
	return statsHandle.UFIDelate(schemareplicant.GetSchemaReplicant(e.ctx))
}

func getBuildStatsConcurrency(ctx stochastikctx.Context) (int, error) {
	stochaseinstein_dbars := ctx.GetStochaseinstein_dbars()
	concurrency, err := variable.GetStochastikSystemVar(stochaseinstein_dbars, variable.MilevaDBBuildStatsConcurrency)
	if err != nil {
		return 0, err
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), err
}

type taskType int

const (
	defCausTask taskType = iota
	idxTask
	fastTask
	pkIncrementalTask
	idxIncrementalTask
)

type analyzeTask struct {
	taskType               taskType
	idxExec                *AnalyzeIndexExec
	defCausExec            *AnalyzeDeferredCausetsExec
	fastExec               *AnalyzeFastExec
	idxIncrementalExec     *analyzeIndexIncrementalExec
	defCausIncrementalExec *analyzePKIncrementalExec
	job                    *statistics.AnalyzeJob
}

var errAnalyzeWorkerPanic = errors.New("analyze worker panic")

func (e *AnalyzeExec) analyzeWorker(taskCh <-chan *analyzeTask, resultCh chan<- analyzeResult, isCloseChanThread bool) {
	var task *analyzeTask
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Error("analyze worker panicked", zap.String("stack", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- analyzeResult{
				Err: errAnalyzeWorkerPanic,
				job: task.job,
			}
		}
		e.wg.Done()
		if isCloseChanThread {
			e.wg.Wait()
			close(resultCh)
		}
	}()
	for {
		var ok bool
		task, ok = <-taskCh
		if !ok {
			break
		}
		task.job.Start()
		switch task.taskType {
		case defCausTask:
			task.defCausExec.job = task.job
			resultCh <- analyzeDeferredCausetsPushdown(task.defCausExec)
		case idxTask:
			task.idxExec.job = task.job
			resultCh <- analyzeIndexPushdown(task.idxExec)
		case fastTask:
			task.fastExec.job = task.job
			task.job.Start()
			for _, result := range analyzeFastExec(task.fastExec) {
				resultCh <- result
			}
		case pkIncrementalTask:
			task.defCausIncrementalExec.job = task.job
			resultCh <- analyzePKIncremental(task.defCausIncrementalExec)
		case idxIncrementalTask:
			task.idxIncrementalExec.job = task.job
			resultCh <- analyzeIndexIncremental(task.idxIncrementalExec)
		}
	}
}

func analyzeIndexPushdown(idxExec *AnalyzeIndexExec) analyzeResult {
	ranges := ranger.FullRange()
	// For single-defCausumn index, we do not load null rows from EinsteinDB, so the built histogram would not include
	// null values, and its `NullCount` would be set by result of another distsql call to get null rows.
	// For multi-defCausumn index, we cannot define null for the rows, so we still use full range, and the rows
	// containing null fields would exist in built histograms. Note that, the `NullCount` of histograms for
	// multi-defCausumn index is always 0 then.
	if len(idxExec.idxInfo.DeferredCausets) == 1 {
		ranges = ranger.FullNotNullRange()
	}
	hist, cms, err := idxExec.buildStats(ranges, true)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	result := analyzeResult{
		BlockID: idxExec.blockID,
		Hist:    []*statistics.Histogram{hist},
		Cms:     []*statistics.CMSketch{cms},
		IsIndex: 1,
		job:     idxExec.job,
	}
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

// AnalyzeIndexExec represents analyze index push down executor.
type AnalyzeIndexExec struct {
	ctx            stochastikctx.Context
	blockID        core.AnalyzeBlockID
	idxInfo        *perceptron.IndexInfo
	isCommonHandle bool
	concurrency    int
	priority       int
	analyzePB      *fidelpb.AnalyzeReq
	result         distsql.SelectResult
	countNullRes   distsql.SelectResult
	opts           map[ast.AnalyzeOptionType]uint64
	job            *statistics.AnalyzeJob
}

// fetchAnalyzeResult builds and dispatches the `ekv.Request` from given ranges, and stores the `SelectResult`
// in corresponding fields based on the input `isNullRange` argument, which indicates if the range is the
// special null range for single-defCausumn index to get the null count.
func (e *AnalyzeIndexExec) fetchAnalyzeResult(ranges []*ranger.Range, isNullRange bool) error {
	var builder distsql.RequestBuilder
	var kvReqBuilder *distsql.RequestBuilder
	if e.isCommonHandle && e.idxInfo.Primary {
		kvReqBuilder = builder.SetCommonHandleRanges(e.ctx.GetStochaseinstein_dbars().StmtCtx, e.blockID.DefCauslectIDs[0], ranges)
	} else {
		kvReqBuilder = builder.SetIndexRanges(e.ctx.GetStochaseinstein_dbars().StmtCtx, e.blockID.DefCauslectIDs[0], e.idxInfo.ID, ranges)
	}
	kvReq, err := kvReqBuilder.
		SetAnalyzeRequest(e.analyzePB).
		SetStartTS(math.MaxUint64).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		Build()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetStochaseinstein_dbars().KVVars, e.ctx.GetStochaseinstein_dbars().InRestrictedALLEGROSQL)
	if err != nil {
		return err
	}
	result.Fetch(ctx)
	if isNullRange {
		e.countNullRes = result
	} else {
		e.result = result
	}
	return nil
}

func (e *AnalyzeIndexExec) open(ranges []*ranger.Range, considerNull bool) error {
	err := e.fetchAnalyzeResult(ranges, false)
	if err != nil {
		return err
	}
	if considerNull && len(e.idxInfo.DeferredCausets) == 1 {
		ranges = ranger.NullRange()
		err = e.fetchAnalyzeResult(ranges, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *AnalyzeIndexExec) buildStatsFromResult(result distsql.SelectResult, needCMS bool) (*statistics.Histogram, *statistics.CMSketch, error) {
	failpoint.Inject("buildStatsFromResult", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, nil, errors.New("mock buildStatsFromResult error"))
		}
	})
	hist := &statistics.Histogram{}
	var cms *statistics.CMSketch
	if needCMS {
		cms = statistics.NewCMSketch(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]))
	}
	for {
		data, err := result.NextRaw(context.TODO())
		if err != nil {
			return nil, nil, err
		}
		if data == nil {
			break
		}
		resp := &fidelpb.AnalyzeIndexResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, nil, err
		}
		respHist := statistics.HistogramFromProto(resp.Hist)
		e.job.UFIDelate(int64(respHist.TotalEventCount()))
		hist, err = statistics.MergeHistograms(e.ctx.GetStochaseinstein_dbars().StmtCtx, hist, respHist, int(e.opts[ast.AnalyzeOptNumBuckets]))
		if err != nil {
			return nil, nil, err
		}
		if needCMS {
			if resp.Cms == nil {
				logutil.Logger(context.TODO()).Warn("nil CMS in response", zap.String("block", e.idxInfo.Block.O), zap.String("index", e.idxInfo.Name.O))
			} else if err := cms.MergeCMSketch(statistics.CMSketchFromProto(resp.Cms), 0); err != nil {
				return nil, nil, err
			}
		}
	}
	err := hist.ExtractTopN(cms, len(e.idxInfo.DeferredCausets), uint32(e.opts[ast.AnalyzeOptNumTopN]))
	if needCMS && cms != nil {
		cms.CalcDefaultValForAnalyze(uint64(hist.NDV))
	}
	return hist, cms, err
}

func (e *AnalyzeIndexExec) buildStats(ranges []*ranger.Range, considerNull bool) (hist *statistics.Histogram, cms *statistics.CMSketch, err error) {
	if err = e.open(ranges, considerNull); err != nil {
		return nil, nil, err
	}
	defer func() {
		err1 := closeAll(e.result, e.countNullRes)
		if err == nil {
			err = err1
		}
	}()
	hist, cms, err = e.buildStatsFromResult(e.result, true)
	if err != nil {
		return nil, nil, err
	}
	if e.countNullRes != nil {
		nullHist, _, err := e.buildStatsFromResult(e.countNullRes, false)
		if err != nil {
			return nil, nil, err
		}
		if l := nullHist.Len(); l > 0 {
			hist.NullCount = nullHist.Buckets[l-1].Count
		}
	}
	hist.ID = e.idxInfo.ID
	return hist, cms, nil
}

func analyzeDeferredCausetsPushdown(defCausExec *AnalyzeDeferredCausetsExec) analyzeResult {
	var ranges []*ranger.Range
	if hc := defCausExec.handleDefCauss; hc != nil {
		if hc.IsInt() {
			ranges = ranger.FullIntRange(allegrosql.HasUnsignedFlag(hc.GetDefCaus(0).RetType.Flag))
		} else {
			ranges = ranger.FullNotNullRange()
		}
	} else {
		ranges = ranger.FullIntRange(false)
	}
	hists, cms, extStats, err := defCausExec.buildStats(ranges, true)
	if err != nil {
		return analyzeResult{Err: err, job: defCausExec.job}
	}
	result := analyzeResult{
		BlockID:  defCausExec.blockID,
		Hist:     hists,
		Cms:      cms,
		ExtStats: extStats,
		job:      defCausExec.job,
	}
	hist := hists[0]
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

// AnalyzeDeferredCausetsExec represents Analyze defCausumns push down executor.
type AnalyzeDeferredCausetsExec struct {
	ctx            stochastikctx.Context
	blockID        core.AnalyzeBlockID
	defcausInfo    []*perceptron.DeferredCausetInfo
	handleDefCauss core.HandleDefCauss
	concurrency    int
	priority       int
	analyzePB      *fidelpb.AnalyzeReq
	resultHandler  *blockResultHandler
	opts           map[ast.AnalyzeOptionType]uint64
	job            *statistics.AnalyzeJob
}

func (e *AnalyzeDeferredCausetsExec) open(ranges []*ranger.Range) error {
	e.resultHandler = &blockResultHandler{}
	firstPartRanges, secondPartRanges := splitRanges(ranges, true, false)
	firstResult, err := e.buildResp(firstPartRanges)
	if err != nil {
		return err
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(secondPartRanges)
	if err != nil {
		return err
	}
	e.resultHandler.open(firstResult, secondResult)

	return nil
}

func (e *AnalyzeDeferredCausetsExec) buildResp(ranges []*ranger.Range) (distsql.SelectResult, error) {
	var builder distsql.RequestBuilder
	var reqBuilder *distsql.RequestBuilder
	if e.handleDefCauss != nil && !e.handleDefCauss.IsInt() {
		reqBuilder = builder.SetCommonHandleRanges(e.ctx.GetStochaseinstein_dbars().StmtCtx, e.blockID.DefCauslectIDs[0], ranges)
	} else {
		reqBuilder = builder.SetBlockRanges(e.blockID.DefCauslectIDs[0], ranges, nil)
	}
	// Always set KeepOrder of the request to be true, in order to compute
	// correct `correlation` of defCausumns.
	kvReq, err := reqBuilder.
		SetAnalyzeRequest(e.analyzePB).
		SetStartTS(math.MaxUint64).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		Build()
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetStochaseinstein_dbars().KVVars, e.ctx.GetStochaseinstein_dbars().InRestrictedALLEGROSQL)
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	return result, nil
}

func (e *AnalyzeDeferredCausetsExec) buildStats(ranges []*ranger.Range, needExtStats bool) (hists []*statistics.Histogram, cms []*statistics.CMSketch, extStats *statistics.ExtendedStatsDefCausl, err error) {
	if err = e.open(ranges); err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		if err1 := e.resultHandler.Close(); err1 != nil {
			hists = nil
			cms = nil
			extStats = nil
			err = err1
		}
	}()
	pkHist := &statistics.Histogram{}
	defCauslectors := make([]*statistics.SampleDefCauslector, len(e.defcausInfo))
	for i := range defCauslectors {
		defCauslectors[i] = &statistics.SampleDefCauslector{
			IsMerger:      true,
			FMSketch:      statistics.NewFMSketch(maxSketchSize),
			MaxSampleSize: int64(e.opts[ast.AnalyzeOptNumSamples]),
			CMSketch:      statistics.NewCMSketch(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth])),
		}
	}
	for {
		data, err1 := e.resultHandler.nextRaw(context.TODO())
		if err1 != nil {
			return nil, nil, nil, err1
		}
		if data == nil {
			break
		}
		resp := &fidelpb.AnalyzeDeferredCausetsResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, nil, nil, err
		}
		sc := e.ctx.GetStochaseinstein_dbars().StmtCtx
		rowCount := int64(0)
		if hasPkHist(e.handleDefCauss) {
			respHist := statistics.HistogramFromProto(resp.PkHist)
			rowCount = int64(respHist.TotalEventCount())
			pkHist, err = statistics.MergeHistograms(sc, pkHist, respHist, int(e.opts[ast.AnalyzeOptNumBuckets]))
			if err != nil {
				return nil, nil, nil, err
			}
		}
		for i, rc := range resp.DefCauslectors {
			respSample := statistics.SampleDefCauslectorFromProto(rc)
			rowCount = respSample.Count + respSample.NullCount
			defCauslectors[i].MergeSampleDefCauslector(sc, respSample)
		}
		e.job.UFIDelate(rowCount)
	}
	timeZone := e.ctx.GetStochaseinstein_dbars().Location()
	if hasPkHist(e.handleDefCauss) {
		pkInfo := e.handleDefCauss.GetDefCaus(0)
		pkHist.ID = pkInfo.ID
		err = pkHist.DecodeTo(pkInfo.RetType, timeZone)
		if err != nil {
			return nil, nil, nil, err
		}
		hists = append(hists, pkHist)
		cms = append(cms, nil)
	}
	for i, defCaus := range e.defcausInfo {
		err := defCauslectors[i].ExtractTopN(uint32(e.opts[ast.AnalyzeOptNumTopN]), e.ctx.GetStochaseinstein_dbars().StmtCtx, &defCaus.FieldType, timeZone)
		if err != nil {
			return nil, nil, nil, err
		}
		for j, s := range defCauslectors[i].Samples {
			defCauslectors[i].Samples[j].Ordinal = j
			defCauslectors[i].Samples[j].Value, err = blockcodec.DecodeDeferredCausetValue(s.Value.GetBytes(), &defCaus.FieldType, timeZone)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		hg, err := statistics.BuildDeferredCauset(e.ctx, int64(e.opts[ast.AnalyzeOptNumBuckets]), defCaus.ID, defCauslectors[i], &defCaus.FieldType)
		if err != nil {
			return nil, nil, nil, err
		}
		hists = append(hists, hg)
		defCauslectors[i].CMSketch.CalcDefaultValForAnalyze(uint64(hg.NDV))
		cms = append(cms, defCauslectors[i].CMSketch)
	}
	if needExtStats {
		statsHandle := petri.GetPetri(e.ctx).StatsHandle()
		extStats, err = statsHandle.BuildExtendedStats(e.blockID.PersistID, e.defcausInfo, defCauslectors)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return hists, cms, extStats, nil
}

func hasPkHist(handleDefCauss core.HandleDefCauss) bool {
	return handleDefCauss != nil && handleDefCauss.IsInt()
}

func pkDefCaussCount(handleDefCauss core.HandleDefCauss) int {
	if handleDefCauss == nil {
		return 0
	}
	return handleDefCauss.NumDefCauss()
}

var (
	fastAnalyzeHistogramSample        = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "sample")
	fastAnalyzeHistogramAccessRegions = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "access_regions")
	fastAnalyzeHistogramScanKeys      = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "scan_keys")
)

func analyzeFastExec(exec *AnalyzeFastExec) []analyzeResult {
	hists, cms, err := exec.buildStats()
	if err != nil {
		return []analyzeResult{{Err: err, job: exec.job}}
	}
	var results []analyzeResult
	pkDefCausCount := pkDefCaussCount(exec.handleDefCauss)
	if len(exec.idxsInfo) > 0 {
		for i := pkDefCausCount + len(exec.defcausInfo); i < len(hists); i++ {
			idxResult := analyzeResult{
				BlockID: exec.blockID,
				Hist:    []*statistics.Histogram{hists[i]},
				Cms:     []*statistics.CMSketch{cms[i]},
				IsIndex: 1,
				Count:   hists[i].NullCount,
				job:     exec.job,
			}
			if hists[i].Len() > 0 {
				idxResult.Count += hists[i].Buckets[hists[i].Len()-1].Count
			}
			if exec.rowCount != 0 {
				idxResult.Count = exec.rowCount
			}
			results = append(results, idxResult)
		}
	}
	hist := hists[0]
	defCausResult := analyzeResult{
		BlockID: exec.blockID,
		Hist:    hists[:pkDefCausCount+len(exec.defcausInfo)],
		Cms:     cms[:pkDefCausCount+len(exec.defcausInfo)],
		Count:   hist.NullCount,
		job:     exec.job,
	}
	if hist.Len() > 0 {
		defCausResult.Count += hist.Buckets[hist.Len()-1].Count
	}
	if exec.rowCount != 0 {
		defCausResult.Count = exec.rowCount
	}
	results = append(results, defCausResult)
	return results
}

// AnalyzeFastExec represents Fast Analyze executor.
type AnalyzeFastExec struct {
	ctx            stochastikctx.Context
	blockID        core.AnalyzeBlockID
	handleDefCauss core.HandleDefCauss
	defcausInfo    []*perceptron.DeferredCausetInfo
	idxsInfo       []*perceptron.IndexInfo
	concurrency    int
	opts           map[ast.AnalyzeOptionType]uint64
	tblInfo        *perceptron.BlockInfo
	cache          *einsteindb.RegionCache
	wg             *sync.WaitGroup
	rowCount       int64
	sampCursor     int32
	sampTasks      []*einsteindb.KeyLocation
	scanTasks      []*einsteindb.KeyLocation
	defCauslectors []*statistics.SampleDefCauslector
	randSeed       int64
	job            *statistics.AnalyzeJob
	estSampStep    uint32
}

func (e *AnalyzeFastExec) calculateEstimateSampleStep() (err error) {
	allegrosql := fmt.Sprintf("select flag from allegrosql.stats_histograms where block_id = %d;", e.blockID.PersistID)
	var rows []chunk.Event
	rows, _, err = e.ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return
	}
	var historyEventCount uint64
	hasBeenAnalyzed := len(rows) != 0 && rows[0].GetInt64(0) == statistics.AnalyzeFlag
	if hasBeenAnalyzed {
		historyEventCount = uint64(petri.GetPetri(e.ctx).StatsHandle().GetPartitionStats(e.tblInfo, e.blockID.PersistID).Count)
	} else {
		dbInfo, ok := petri.GetPetri(e.ctx).SchemaReplicant().SchemaByBlock(e.tblInfo)
		if !ok {
			err = errors.Errorf("database not found for block '%s'", e.tblInfo.Name)
			return
		}
		var rollbackFn func() error
		rollbackFn, err = e.activateTxnForEventCount()
		if err != nil {
			return
		}
		defer func() {
			if rollbackFn != nil {
				err = rollbackFn()
			}
		}()
		var partition string
		if e.tblInfo.ID != e.blockID.PersistID {
			for _, definition := range e.tblInfo.Partition.Definitions {
				if definition.ID == e.blockID.PersistID {
					partition = fmt.Sprintf(" partition(%s)", definition.Name.L)
					break
				}
			}
		}
		allegrosql := fmt.Sprintf("select count(*) from %s.%s", dbInfo.Name.L, e.tblInfo.Name.L)
		if len(partition) > 0 {
			allegrosql += partition
		}
		var recordSets []sqlexec.RecordSet
		recordSets, err = e.ctx.(sqlexec.ALLEGROSQLExecutor).ExecuteInternal(context.TODO(), allegrosql)
		if err != nil || len(recordSets) == 0 {
			return
		}
		if len(recordSets) == 0 {
			err = errors.Trace(errors.Errorf("empty record set"))
			return
		}
		defer func() {
			for _, r := range recordSets {
				terror.Call(r.Close)
			}
		}()
		chk := recordSets[0].NewChunk()
		err = recordSets[0].Next(context.TODO(), chk)
		if err != nil {
			return
		}
		e.rowCount = chk.GetEvent(0).GetInt64(0)
		historyEventCount = uint64(e.rowCount)
	}
	totalSampSize := e.opts[ast.AnalyzeOptNumSamples]
	e.estSampStep = uint32(historyEventCount / totalSampSize)
	return
}

func (e *AnalyzeFastExec) activateTxnForEventCount() (rollbackFn func() error, err error) {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		if ekv.ErrInvalidTxn.Equal(err) {
			_, err := e.ctx.(sqlexec.ALLEGROSQLExecutor).ExecuteInternal(context.TODO(), "begin")
			if err != nil {
				return nil, errors.Trace(err)
			}
			rollbackFn = func() error {
				_, err := e.ctx.(sqlexec.ALLEGROSQLExecutor).ExecuteInternal(context.TODO(), "rollback")
				return err
			}
		} else {
			return nil, errors.Trace(err)
		}
	}
	txn.SetOption(ekv.Priority, ekv.PriorityLow)
	txn.SetOption(ekv.IsolationLevel, ekv.RC)
	txn.SetOption(ekv.NotFillCache, true)
	return nil, nil
}

// buildSampTask build sample tasks.
func (e *AnalyzeFastExec) buildSampTask() (err error) {
	bo := einsteindb.NewBackofferWithVars(context.Background(), 500, nil)
	causetstore, _ := e.ctx.GetStore().(einsteindb.CausetStorage)
	e.cache = causetstore.GetRegionCache()
	startKey, endKey := blockcodec.GetBlockHandleKeyRange(e.blockID.DefCauslectIDs[0])
	targetKey := startKey
	accessRegionsCounter := 0
	for {
		// Search for the region which contains the targetKey.
		loc, err := e.cache.LocateKey(bo, targetKey)
		if err != nil {
			return err
		}
		if bytes.Compare(endKey, loc.StartKey) < 0 {
			break
		}
		accessRegionsCounter++

		// Set the next search key.
		targetKey = loc.EndKey

		// If the KV pairs in the region all belonging to the block, add it to the sample task.
		if bytes.Compare(startKey, loc.StartKey) <= 0 && len(loc.EndKey) != 0 && bytes.Compare(loc.EndKey, endKey) <= 0 {
			e.sampTasks = append(e.sampTasks, loc)
			continue
		}

		e.scanTasks = append(e.scanTasks, loc)
		if bytes.Compare(loc.StartKey, startKey) < 0 {
			loc.StartKey = startKey
		}
		if bytes.Compare(endKey, loc.EndKey) < 0 || len(loc.EndKey) == 0 {
			loc.EndKey = endKey
			break
		}
	}
	fastAnalyzeHistogramAccessRegions.Observe(float64(accessRegionsCounter))

	return nil
}

func (e *AnalyzeFastExec) decodeValues(handle ekv.Handle, sValue []byte, wantDefCauss map[int64]*types.FieldType) (values map[int64]types.Causet, err error) {
	loc := e.ctx.GetStochaseinstein_dbars().Location()
	values, err = blockcodec.DecodeEventToCausetMap(sValue, wantDefCauss, loc)
	if err != nil || e.handleDefCauss == nil {
		return values, err
	}
	wantDefCauss = make(map[int64]*types.FieldType, e.handleDefCauss.NumDefCauss())
	handleDefCausIDs := make([]int64, e.handleDefCauss.NumDefCauss())
	for i := 0; i < e.handleDefCauss.NumDefCauss(); i++ {
		c := e.handleDefCauss.GetDefCaus(i)
		handleDefCausIDs[i] = c.ID
		wantDefCauss[c.ID] = c.RetType
	}
	return blockcodec.DecodeHandleToCausetMap(handle, handleDefCausIDs, wantDefCauss, loc, values)
}

func (e *AnalyzeFastExec) getValueByInfo(defCausInfo *perceptron.DeferredCausetInfo, values map[int64]types.Causet) (types.Causet, error) {
	val, ok := values[defCausInfo.ID]
	if !ok {
		return block.GetDefCausOriginDefaultValue(e.ctx, defCausInfo)
	}
	return val, nil
}

func (e *AnalyzeFastExec) uFIDelateDefCauslectorSamples(sValue []byte, sKey ekv.Key, samplePos int32) (err error) {
	var handle ekv.Handle
	handle, err = blockcodec.DecodeEventKey(sKey)
	if err != nil {
		return err
	}

	// Decode defcaus for analyze block
	wantDefCauss := make(map[int64]*types.FieldType, len(e.defcausInfo))
	for _, defCaus := range e.defcausInfo {
		wantDefCauss[defCaus.ID] = &defCaus.FieldType
	}

	// Pre-build index->defcaus relationship and refill wantDefCauss if not exists(analyze index)
	index2DefCauss := make([][]*perceptron.DeferredCausetInfo, len(e.idxsInfo))
	for i, idxInfo := range e.idxsInfo {
		for _, idxDefCaus := range idxInfo.DeferredCausets {
			defCausInfo := e.tblInfo.DeferredCausets[idxDefCaus.Offset]
			index2DefCauss[i] = append(index2DefCauss[i], defCausInfo)
			wantDefCauss[defCausInfo.ID] = &defCausInfo.FieldType
		}
	}

	// Decode the defcaus value in order.
	var values map[int64]types.Causet
	values, err = e.decodeValues(handle, sValue, wantDefCauss)
	if err != nil {
		return err
	}
	// UFIDelate the primary key defCauslector.
	pkDefCaussCount := pkDefCaussCount(e.handleDefCauss)
	for i := 0; i < pkDefCaussCount; i++ {
		defCaus := e.handleDefCauss.GetDefCaus(i)
		v, ok := values[defCaus.ID]
		if !ok {
			return errors.Trace(errors.Errorf("Primary key defCausumn not found"))
		}
		if e.defCauslectors[i].Samples[samplePos] == nil {
			e.defCauslectors[i].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.defCauslectors[i].Samples[samplePos].Handle = handle
		e.defCauslectors[i].Samples[samplePos].Value = v
	}

	// UFIDelate the defCausumns' defCauslectors.
	for j, defCausInfo := range e.defcausInfo {
		v, err := e.getValueByInfo(defCausInfo, values)
		if err != nil {
			return err
		}
		if e.defCauslectors[pkDefCaussCount+j].Samples[samplePos] == nil {
			e.defCauslectors[pkDefCaussCount+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.defCauslectors[pkDefCaussCount+j].Samples[samplePos].Handle = handle
		e.defCauslectors[pkDefCaussCount+j].Samples[samplePos].Value = v
	}
	// UFIDelate the indexes' defCauslectors.
	for j, idxInfo := range e.idxsInfo {
		idxVals := make([]types.Causet, 0, len(idxInfo.DeferredCausets))
		defcaus := index2DefCauss[j]
		for _, defCausInfo := range defcaus {
			v, err := e.getValueByInfo(defCausInfo, values)
			if err != nil {
				return err
			}
			idxVals = append(idxVals, v)
		}
		var bytes []byte
		bytes, err = codec.EncodeKey(e.ctx.GetStochaseinstein_dbars().StmtCtx, bytes, idxVals...)
		if err != nil {
			return err
		}
		if e.defCauslectors[len(e.defcausInfo)+pkDefCaussCount+j].Samples[samplePos] == nil {
			e.defCauslectors[len(e.defcausInfo)+pkDefCaussCount+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.defCauslectors[len(e.defcausInfo)+pkDefCaussCount+j].Samples[samplePos].Handle = handle
		e.defCauslectors[len(e.defcausInfo)+pkDefCaussCount+j].Samples[samplePos].Value = types.NewBytesCauset(bytes)
	}
	return nil
}

func (e *AnalyzeFastExec) handleBatchSeekResponse(kvMap map[string][]byte) (err error) {
	length := int32(len(kvMap))
	newCursor := atomic.AddInt32(&e.sampCursor, length)
	samplePos := newCursor - length
	for sKey, sValue := range kvMap {
		exceedNeededSampleCounts := uint64(samplePos) >= e.opts[ast.AnalyzeOptNumSamples]
		if exceedNeededSampleCounts {
			atomic.StoreInt32(&e.sampCursor, int32(e.opts[ast.AnalyzeOptNumSamples]))
			break
		}
		err = e.uFIDelateDefCauslectorSamples(sValue, ekv.Key(sKey), samplePos)
		if err != nil {
			return err
		}
		samplePos++
	}
	return nil
}

func (e *AnalyzeFastExec) handleScanIter(iter ekv.Iterator) (scanKeysSize int, err error) {
	rander := rand.New(rand.NewSource(e.randSeed))
	sampleSize := int64(e.opts[ast.AnalyzeOptNumSamples])
	for ; iter.Valid() && err == nil; err = iter.Next() {
		// reservoir sampling
		scanKeysSize++
		randNum := rander.Int63n(int64(e.sampCursor) + int64(scanKeysSize))
		if randNum > sampleSize && e.sampCursor == int32(sampleSize) {
			continue
		}

		p := rander.Int31n(int32(sampleSize))
		if e.sampCursor < int32(sampleSize) {
			p = e.sampCursor
			e.sampCursor++
		}

		err = e.uFIDelateDefCauslectorSamples(iter.Value(), iter.Key(), p)
		if err != nil {
			return
		}
	}
	return
}

func (e *AnalyzeFastExec) handleScanTasks(bo *einsteindb.Backoffer) (keysSize int, err error) {
	snapshot, err := e.ctx.GetStore().(einsteindb.CausetStorage).GetSnapshot(ekv.MaxVersion)
	if err != nil {
		return 0, err
	}
	if e.ctx.GetStochaseinstein_dbars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(ekv.ReplicaRead, ekv.ReplicaReadFollower)
	}
	for _, t := range e.scanTasks {
		iter, err := snapshot.Iter(t.StartKey, t.EndKey)
		if err != nil {
			return keysSize, err
		}
		size, err := e.handleScanIter(iter)
		keysSize += size
		if err != nil {
			return keysSize, err
		}
	}
	return keysSize, nil
}

func (e *AnalyzeFastExec) handleSampTasks(workID int, step uint32, err *error) {
	defer e.wg.Done()
	var snapshot ekv.Snapshot
	snapshot, *err = e.ctx.GetStore().(einsteindb.CausetStorage).GetSnapshot(ekv.MaxVersion)
	if *err != nil {
		return
	}
	snapshot.SetOption(ekv.NotFillCache, true)
	snapshot.SetOption(ekv.IsolationLevel, ekv.RC)
	snapshot.SetOption(ekv.Priority, ekv.PriorityLow)
	if e.ctx.GetStochaseinstein_dbars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(ekv.ReplicaRead, ekv.ReplicaReadFollower)
	}

	rander := rand.New(rand.NewSource(e.randSeed))
	for i := workID; i < len(e.sampTasks); i += e.concurrency {
		task := e.sampTasks[i]
		// randomize the estimate step in range [step - 2 * sqrt(step), step]
		if step > 4 { // 2*sqrt(x) < x
			lower, upper := step-uint32(2*math.Sqrt(float64(step))), step
			step = uint32(rander.Intn(int(upper-lower))) + lower
		}
		snapshot.SetOption(ekv.SampleStep, step)
		kvMap := make(map[string][]byte)
		var iter ekv.Iterator
		iter, *err = snapshot.Iter(task.StartKey, task.EndKey)
		if *err != nil {
			return
		}
		for iter.Valid() {
			kvMap[string(iter.Key())] = iter.Value()
			*err = iter.Next()
			if *err != nil {
				return
			}
		}
		fastAnalyzeHistogramSample.Observe(float64(len(kvMap)))

		*err = e.handleBatchSeekResponse(kvMap)
		if *err != nil {
			return
		}
	}
}

func (e *AnalyzeFastExec) buildDeferredCausetStats(ID int64, defCauslector *statistics.SampleDefCauslector, tp *types.FieldType, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, error) {
	data := make([][]byte, 0, len(defCauslector.Samples))
	for i, sample := range defCauslector.Samples {
		sample.Ordinal = i
		if sample.Value.IsNull() {
			defCauslector.NullCount++
			continue
		}
		bytes, err := blockcodec.EncodeValue(e.ctx.GetStochaseinstein_dbars().StmtCtx, nil, sample.Value)
		if err != nil {
			return nil, nil, err
		}
		data = append(data, bytes)
	}
	// Build CMSketch.
	cmSketch, ndv, scaleRatio := statistics.NewCMSketchWithTopN(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]), data, uint32(e.opts[ast.AnalyzeOptNumTopN]), uint64(rowCount))
	// Build Histogram.
	hist, err := statistics.BuildDeferredCausetHist(e.ctx, int64(e.opts[ast.AnalyzeOptNumBuckets]), ID, defCauslector, tp, rowCount, int64(ndv), defCauslector.NullCount*int64(scaleRatio))
	return hist, cmSketch, err
}

func (e *AnalyzeFastExec) buildIndexStats(idxInfo *perceptron.IndexInfo, defCauslector *statistics.SampleDefCauslector, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, error) {
	data := make([][][]byte, len(idxInfo.DeferredCausets))
	for _, sample := range defCauslector.Samples {
		var preLen int
		remained := sample.Value.GetBytes()
		// We need to insert each prefix values into CM Sketch.
		for i := 0; i < len(idxInfo.DeferredCausets); i++ {
			var err error
			var value []byte
			value, remained, err = codec.CutOne(remained)
			if err != nil {
				return nil, nil, err
			}
			preLen += len(value)
			data[i] = append(data[i], sample.Value.GetBytes()[:preLen])
		}
	}
	numTop := uint32(e.opts[ast.AnalyzeOptNumTopN])
	cmSketch, ndv, scaleRatio := statistics.NewCMSketchWithTopN(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]), data[0], numTop, uint64(rowCount))
	// Build CM Sketch for each prefix and merge them into one.
	for i := 1; i < len(idxInfo.DeferredCausets); i++ {
		var curCMSketch *statistics.CMSketch
		// `ndv` should be the ndv of full index, so just rewrite it here.
		curCMSketch, ndv, scaleRatio = statistics.NewCMSketchWithTopN(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]), data[i], numTop, uint64(rowCount))
		err := cmSketch.MergeCMSketch(curCMSketch, numTop)
		if err != nil {
			return nil, nil, err
		}
	}
	// Build Histogram.
	hist, err := statistics.BuildDeferredCausetHist(e.ctx, int64(e.opts[ast.AnalyzeOptNumBuckets]), idxInfo.ID, defCauslector, types.NewFieldType(allegrosql.TypeBlob), rowCount, int64(ndv), defCauslector.NullCount*int64(scaleRatio))
	return hist, cmSketch, err
}

func (e *AnalyzeFastExec) runTasks() ([]*statistics.Histogram, []*statistics.CMSketch, error) {
	errs := make([]error, e.concurrency)
	pkDefCausCount := pkDefCaussCount(e.handleDefCauss)
	// defCauslect defCausumn samples and primary key samples and index samples.
	length := len(e.defcausInfo) + pkDefCausCount + len(e.idxsInfo)
	e.defCauslectors = make([]*statistics.SampleDefCauslector, length)
	for i := range e.defCauslectors {
		e.defCauslectors[i] = &statistics.SampleDefCauslector{
			MaxSampleSize: int64(e.opts[ast.AnalyzeOptNumSamples]),
			Samples:       make([]*statistics.SampleItem, e.opts[ast.AnalyzeOptNumSamples]),
		}
	}

	e.wg.Add(e.concurrency)
	bo := einsteindb.NewBackofferWithVars(context.Background(), 500, nil)
	for i := 0; i < e.concurrency; i++ {
		go e.handleSampTasks(i, e.estSampStep, &errs[i])
	}
	e.wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, nil, err
		}
	}

	scanKeysSize, err := e.handleScanTasks(bo)
	fastAnalyzeHistogramScanKeys.Observe(float64(scanKeysSize))
	if err != nil {
		return nil, nil, err
	}

	stats := petri.GetPetri(e.ctx).StatsHandle()
	var rowCount int64 = 0
	if stats.Lease() > 0 {
		if t := stats.GetPartitionStats(e.tblInfo, e.blockID.PersistID); !t.Pseudo {
			rowCount = t.Count
		}
	}
	hists, cms := make([]*statistics.Histogram, length), make([]*statistics.CMSketch, length)
	for i := 0; i < length; i++ {
		// Build defCauslector properties.
		defCauslector := e.defCauslectors[i]
		defCauslector.Samples = defCauslector.Samples[:e.sampCursor]
		sort.Slice(defCauslector.Samples, func(i, j int) bool {
			return defCauslector.Samples[i].Handle.Compare(defCauslector.Samples[j].Handle) < 0
		})
		defCauslector.CalcTotalSize()
		// Adjust the event count in case the count of `tblStats` is not accurate and too small.
		rowCount = mathutil.MaxInt64(rowCount, int64(len(defCauslector.Samples)))
		// Scale the total defCausumn size.
		if len(defCauslector.Samples) > 0 {
			defCauslector.TotalSize *= rowCount / int64(len(defCauslector.Samples))
		}
		if i < pkDefCausCount {
			pkDefCaus := e.handleDefCauss.GetDefCaus(i)
			hists[i], cms[i], err = e.buildDeferredCausetStats(pkDefCaus.ID, e.defCauslectors[i], pkDefCaus.RetType, rowCount)
		} else if i < pkDefCausCount+len(e.defcausInfo) {
			hists[i], cms[i], err = e.buildDeferredCausetStats(e.defcausInfo[i-pkDefCausCount].ID, e.defCauslectors[i], &e.defcausInfo[i-pkDefCausCount].FieldType, rowCount)
		} else {
			hists[i], cms[i], err = e.buildIndexStats(e.idxsInfo[i-pkDefCausCount-len(e.defcausInfo)], e.defCauslectors[i], rowCount)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return hists, cms, nil
}

func (e *AnalyzeFastExec) buildStats() (hists []*statistics.Histogram, cms []*statistics.CMSketch, err error) {
	// To set rand seed, it's for unit test.
	// To ensure that random sequences are different in non-test environments, RandSeed must be set time.Now().
	if RandSeed == 1 {
		e.randSeed = time.Now().UnixNano()
	} else {
		e.randSeed = RandSeed
	}

	err = e.buildSampTask()
	if err != nil {
		return nil, nil, err
	}

	return e.runTasks()
}

// AnalyzeTestFastExec is for fast sample in unit test.
type AnalyzeTestFastExec struct {
	AnalyzeFastExec
	Ctx             stochastikctx.Context
	PhysicalBlockID int64
	HandleDefCauss  core.HandleDefCauss
	DefCaussInfo    []*perceptron.DeferredCausetInfo
	IdxsInfo        []*perceptron.IndexInfo
	Concurrency     int
	DefCauslectors  []*statistics.SampleDefCauslector
	TblInfo         *perceptron.BlockInfo
	Opts            map[ast.AnalyzeOptionType]uint64
}

// TestFastSample only test the fast sample in unit test.
func (e *AnalyzeTestFastExec) TestFastSample() error {
	e.ctx = e.Ctx
	e.handleDefCauss = e.HandleDefCauss
	e.defcausInfo = e.DefCaussInfo
	e.idxsInfo = e.IdxsInfo
	e.concurrency = e.Concurrency
	e.blockID = core.AnalyzeBlockID{PersistID: e.PhysicalBlockID, DefCauslectIDs: []int64{e.PhysicalBlockID}}
	e.wg = &sync.WaitGroup{}
	e.job = &statistics.AnalyzeJob{}
	e.tblInfo = e.TblInfo
	e.opts = e.Opts
	_, _, err := e.buildStats()
	e.DefCauslectors = e.defCauslectors
	return err
}

type analyzeIndexIncrementalExec struct {
	AnalyzeIndexExec
	oldHist *statistics.Histogram
	oldCMS  *statistics.CMSketch
}

func analyzeIndexIncremental(idxExec *analyzeIndexIncrementalExec) analyzeResult {
	startPos := idxExec.oldHist.GetUpper(idxExec.oldHist.Len() - 1)
	values, _, err := codec.DecodeRange(startPos.GetBytes(), len(idxExec.idxInfo.DeferredCausets), nil, nil)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	ran := ranger.Range{LowVal: values, HighVal: []types.Causet{types.MaxValueCauset()}}
	hist, cms, err := idxExec.buildStats([]*ranger.Range{&ran}, false)
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	hist, err = statistics.MergeHistograms(idxExec.ctx.GetStochaseinstein_dbars().StmtCtx, idxExec.oldHist, hist, int(idxExec.opts[ast.AnalyzeOptNumBuckets]))
	if err != nil {
		return analyzeResult{Err: err, job: idxExec.job}
	}
	if idxExec.oldCMS != nil && cms != nil {
		err = cms.MergeCMSketch4IncrementalAnalyze(idxExec.oldCMS, uint32(idxExec.opts[ast.AnalyzeOptNumTopN]))
		if err != nil {
			return analyzeResult{Err: err, job: idxExec.job}
		}
		cms.CalcDefaultValForAnalyze(uint64(hist.NDV))
	}
	result := analyzeResult{
		BlockID: idxExec.blockID,
		Hist:    []*statistics.Histogram{hist},
		Cms:     []*statistics.CMSketch{cms},
		IsIndex: 1,
		job:     idxExec.job,
	}
	result.Count = hist.NullCount
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

type analyzePKIncrementalExec struct {
	AnalyzeDeferredCausetsExec
	oldHist *statistics.Histogram
}

func analyzePKIncremental(defCausExec *analyzePKIncrementalExec) analyzeResult {
	var maxVal types.Causet
	pkInfo := defCausExec.handleDefCauss.GetDefCaus(0)
	if allegrosql.HasUnsignedFlag(pkInfo.RetType.Flag) {
		maxVal = types.NewUintCauset(math.MaxUint64)
	} else {
		maxVal = types.NewIntCauset(math.MaxInt64)
	}
	startPos := *defCausExec.oldHist.GetUpper(defCausExec.oldHist.Len() - 1)
	ran := ranger.Range{LowVal: []types.Causet{startPos}, LowExclude: true, HighVal: []types.Causet{maxVal}}
	hists, _, _, err := defCausExec.buildStats([]*ranger.Range{&ran}, false)
	if err != nil {
		return analyzeResult{Err: err, job: defCausExec.job}
	}
	hist := hists[0]
	hist, err = statistics.MergeHistograms(defCausExec.ctx.GetStochaseinstein_dbars().StmtCtx, defCausExec.oldHist, hist, int(defCausExec.opts[ast.AnalyzeOptNumBuckets]))
	if err != nil {
		return analyzeResult{Err: err, job: defCausExec.job}
	}
	result := analyzeResult{
		BlockID: defCausExec.blockID,
		Hist:    []*statistics.Histogram{hist},
		Cms:     []*statistics.CMSketch{nil},
		job:     defCausExec.job,
	}
	if hist.Len() > 0 {
		result.Count += hist.Buckets[hist.Len()-1].Count
	}
	return result
}

// analyzeResult is used to represent analyze result.
type analyzeResult struct {
	BlockID  core.AnalyzeBlockID
	Hist     []*statistics.Histogram
	Cms      []*statistics.CMSketch
	ExtStats *statistics.ExtendedStatsDefCausl
	Count    int64
	IsIndex  int
	Err      error
	job      *statistics.AnalyzeJob
}
