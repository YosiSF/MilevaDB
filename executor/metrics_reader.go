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
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	pperceptron "github.com/prometheus/common/perceptron"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri/infosync"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/sqlexec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
)

const promReadTimeout = time.Second * 10

// MetricRetriever uses to read metric data.
type MetricRetriever struct {
	dummyCloser
	block     *perceptron.BlockInfo
	tblDef    *schemareplicant.MetricBlockDef
	extractor *plannercore.MetricBlockExtractor
	timeRange plannercore.QueryTimeRange
	retrieved bool
}

func (e *MetricRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true

	failpoint.InjectContext(ctx, "mockMetricsBlockData", func() {
		m, ok := ctx.Value("__mockMetricsBlockData").(map[string][][]types.Causet)
		if ok && m[e.block.Name.L] != nil {
			failpoint.Return(m[e.block.Name.L], nil)
		}
	})

	tblDef, err := schemareplicant.GetMetricBlockDef(e.block.Name.L)
	if err != nil {
		return nil, err
	}
	e.tblDef = tblDef
	queryRange := e.getQueryRange(sctx)
	totalEvents := make([][]types.Causet, 0)
	quantiles := e.extractor.Quantiles
	if len(quantiles) == 0 {
		quantiles = []float64{tblDef.Quantile}
	}
	for _, quantile := range quantiles {
		var queryValue pperceptron.Value
		queryValue, err = e.queryMetric(ctx, sctx, queryRange, quantile)
		if err != nil {
			if err1, ok := err.(*promv1.Error); ok {
				return nil, errors.Errorf("query metric error, msg: %v, detail: %v", err1.Msg, err1.Detail)
			}
			return nil, errors.Errorf("query metric error: %v", err.Error())
		}
		partEvents := e.genEvents(queryValue, quantile)
		totalEvents = append(totalEvents, partEvents...)
	}
	return totalEvents, nil
}

func (e *MetricRetriever) queryMetric(ctx context.Context, sctx stochastikctx.Context, queryRange promv1.Range, quantile float64) (result pperceptron.Value, err error) {
	failpoint.InjectContext(ctx, "mockMetricsPromData", func() {
		failpoint.Return(ctx.Value("__mockMetricsPromData").(pperceptron.Matrix), nil)
	})

	// Add retry to avoid network error.
	var prometheusAddr string
	for i := 0; i < 5; i++ {
		//TODO: the prometheus will be Integrated into the FIDel, then we need to query the prometheus in FIDel directly, which need change the quire API
		prometheusAddr, err = infosync.GetPrometheusAddr()
		if err == nil || err == infosync.ErrPrometheusAddrIsNotSet {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return nil, err
	}
	promClient, err := api.NewClient(api.Config{
		Address: prometheusAddr,
	})
	if err != nil {
		return nil, err
	}
	promQLAPI := promv1.NewAPI(promClient)
	ctx, cancel := context.WithTimeout(ctx, promReadTimeout)
	defer cancel()
	promQL := e.tblDef.GenPromQL(sctx, e.extractor.LabelConditions, quantile)

	// Add retry to avoid network error.
	for i := 0; i < 5; i++ {
		result, _, err = promQLAPI.QueryRange(ctx, promQL, queryRange)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return result, err
}

type promQLQueryRange = promv1.Range

func (e *MetricRetriever) getQueryRange(sctx stochastikctx.Context) promQLQueryRange {
	startTime, endTime := e.extractor.StartTime, e.extractor.EndTime
	step := time.Second * time.Duration(sctx.GetStochaseinstein_dbars().MetricSchemaStep)
	return promQLQueryRange{Start: startTime, End: endTime, Step: step}
}

func (e *MetricRetriever) genEvents(value pperceptron.Value, quantile float64) [][]types.Causet {
	var rows [][]types.Causet
	switch value.Type() {
	case pperceptron.ValMatrix:
		matrix := value.(pperceptron.Matrix)
		for _, m := range matrix {
			for _, v := range m.Values {
				record := e.genRecord(m.Metric, v, quantile)
				rows = append(rows, record)
			}
		}
	}
	return rows
}

func (e *MetricRetriever) genRecord(metric pperceptron.Metric, pair pperceptron.SamplePair, quantile float64) []types.Causet {
	record := make([]types.Causet, 0, 2+len(e.tblDef.Labels)+1)
	// Record order should keep same with genDeferredCausetInfos.
	record = append(record, types.NewTimeCauset(types.NewTime(
		types.FromGoTime(time.Unix(int64(pair.Timestamp/1000), int64(pair.Timestamp%1000)*1e6)),
		allegrosql.TypeDatetime,
		types.MaxFsp,
	)))
	for _, label := range e.tblDef.Labels {
		v := ""
		if metric != nil {
			v = string(metric[pperceptron.LabelName(label)])
		}
		if len(v) == 0 {
			v = schemareplicant.GenLabelConditionValues(e.extractor.LabelConditions[strings.ToLower(label)])
		}
		record = append(record, types.NewStringCauset(v))
	}
	if e.tblDef.Quantile > 0 {
		record = append(record, types.NewFloat64Causet(quantile))
	}
	if math.IsNaN(float64(pair.Value)) {
		record = append(record, types.NewCauset(nil))
	} else {
		record = append(record, types.NewFloat64Causet(float64(pair.Value)))
	}
	return record
}

// MetricsSummaryRetriever uses to read metric data.
type MetricsSummaryRetriever struct {
	dummyCloser
	block     *perceptron.BlockInfo
	extractor *plannercore.MetricSummaryBlockExtractor
	timeRange plannercore.QueryTimeRange
	retrieved bool
}

func (e *MetricsSummaryRetriever) retrieve(_ context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true
	totalEvents := make([][]types.Causet, 0, len(schemareplicant.MetricBlockMap))
	blocks := make([]string, 0, len(schemareplicant.MetricBlockMap))
	for name := range schemareplicant.MetricBlockMap {
		blocks = append(blocks, name)
	}
	sort.Strings(blocks)

	filter := inspectionFilter{set: e.extractor.MetricsNames}
	condition := e.timeRange.Condition()
	for _, name := range blocks {
		if !filter.enable(name) {
			continue
		}
		def, found := schemareplicant.MetricBlockMap[name]
		if !found {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("metrics block: %s not found", name))
			continue
		}
		var allegrosql string
		if def.Quantile > 0 {
			var qs []string
			if len(e.extractor.Quantiles) > 0 {
				for _, q := range e.extractor.Quantiles {
					qs = append(qs, fmt.Sprintf("%f", q))
				}
			} else {
				qs = []string{"0.99"}
			}
			allegrosql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value),quantile from `%[2]s`.`%[1]s` %[3]s and quantile in (%[4]s) group by quantile order by quantile",
				name, soliton.MetricSchemaName.L, condition, strings.Join(qs, ","))
		} else {
			allegrosql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value) from `%[2]s`.`%[1]s` %[3]s",
				name, soliton.MetricSchemaName.L, condition)
		}

		rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
		if err != nil {
			return nil, errors.Errorf("execute '%s' failed: %v", allegrosql, err)
		}
		for _, event := range rows {
			var quantile interface{}
			if def.Quantile > 0 {
				quantile = event.GetFloat64(event.Len() - 1)
			}
			totalEvents = append(totalEvents, types.MakeCausets(
				name,
				quantile,
				event.GetFloat64(0),
				event.GetFloat64(1),
				event.GetFloat64(2),
				event.GetFloat64(3),
				def.Comment,
			))
		}
	}
	return totalEvents, nil
}

// MetricsSummaryByLabelRetriever uses to read metric detail data.
type MetricsSummaryByLabelRetriever struct {
	dummyCloser
	block     *perceptron.BlockInfo
	extractor *plannercore.MetricSummaryBlockExtractor
	timeRange plannercore.QueryTimeRange
	retrieved bool
}

func (e *MetricsSummaryByLabelRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true
	totalEvents := make([][]types.Causet, 0, len(schemareplicant.MetricBlockMap))
	blocks := make([]string, 0, len(schemareplicant.MetricBlockMap))
	for name := range schemareplicant.MetricBlockMap {
		blocks = append(blocks, name)
	}
	sort.Strings(blocks)

	filter := inspectionFilter{set: e.extractor.MetricsNames}
	condition := e.timeRange.Condition()
	for _, name := range blocks {
		if !filter.enable(name) {
			continue
		}
		def, found := schemareplicant.MetricBlockMap[name]
		if !found {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("metrics block: %s not found", name))
			continue
		}
		defcaus := def.Labels
		cond := condition
		if def.Quantile > 0 {
			defcaus = append(defcaus, "quantile")
			if len(e.extractor.Quantiles) > 0 {
				qs := make([]string, len(e.extractor.Quantiles))
				for i, q := range e.extractor.Quantiles {
					qs[i] = fmt.Sprintf("%f", q)
				}
				cond += " and quantile in (" + strings.Join(qs, ",") + ")"
			} else {
				cond += " and quantile=0.99"
			}
		}
		var allegrosql string
		if len(defcaus) > 0 {
			allegrosql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value),`%s` from `%s`.`%s` %s group by `%[1]s` order by `%[1]s`",
				strings.Join(defcaus, "`,`"), soliton.MetricSchemaName.L, name, cond)
		} else {
			allegrosql = fmt.Sprintf("select sum(value),avg(value),min(value),max(value) from `%s`.`%s` %s",
				soliton.MetricSchemaName.L, name, cond)
		}
		rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
		if err != nil {
			return nil, errors.Errorf("execute '%s' failed: %v", allegrosql, err)
		}
		nonInstanceLabelIndex := 0
		if len(def.Labels) > 0 && def.Labels[0] == "instance" {
			nonInstanceLabelIndex = 1
		}
		// skip sum/avg/min/max
		const skipDefCauss = 4
		for _, event := range rows {
			instance := ""
			if nonInstanceLabelIndex > 0 {
				instance = event.GetString(skipDefCauss) // sum/avg/min/max
			}
			var labels []string
			for i, label := range def.Labels[nonInstanceLabelIndex:] {
				// skip min/max/avg/instance
				val := event.GetString(skipDefCauss + nonInstanceLabelIndex + i)
				if label == "causetstore" || label == "store_id" {
					val = fmt.Sprintf("store_id:%s", val)
				}
				labels = append(labels, val)
			}
			var quantile interface{}
			if def.Quantile > 0 {
				quantile = event.GetFloat64(event.Len() - 1) // quantile will be the last defCausumn
			}
			totalEvents = append(totalEvents, types.MakeCausets(
				instance,
				name,
				strings.Join(labels, ", "),
				quantile,
				event.GetFloat64(0), // sum
				event.GetFloat64(1), // avg
				event.GetFloat64(2), // min
				event.GetFloat64(3), // max
				def.Comment,
			))
		}
	}
	return totalEvents, nil
}
