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

package core

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/set"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/stringutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// MemBlockPredicateExtractor is used to extract some predicates from `WHERE` clause
// and push the predicates down to the data retrieving on reading memory block stage.
//
// e.g:
// SELECT * FROM cluster_config WHERE type='einsteindb' AND instance='192.168.1.9:2379'
// We must request all components in the cluster via HTTP API for retrieving
// configurations and filter them by `type/instance` columns.
//
// The purpose of defining a `MemBlockPredicateExtractor` is to optimize this
// 1. Define a `ClusterConfigBlockPredicateExtractor`
// 2. Extract the `type/instance` columns on the logic optimizing stage and save them via fields.
// 3. Passing the extractor to the `ClusterReaderExecExec` executor
// 4. Executor sends requests to the target components instead of all of the components
type MemBlockPredicateExtractor interface {
	// Extracts predicates which can be pushed down and returns the remained predicates
	Extract(stochastikctx.Context, *expression.Schema, []*types.FieldName, []expression.Expression) (remained []expression.Expression)
	explainInfo(p *PhysicalMemBlock) string
}

// extractHelper contains some common utililty functions for all extractor.
// define an individual struct instead of a bunch of un-exported functions
// to avoid polluting the global sINTERLOCKe of current package.
type extractHelper struct{}

func (helper extractHelper) extractDefCausInConsExpr(extractDefCauss map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Causet) {
	args := expr.GetArgs()
	col, isDefCaus := args[0].(*expression.DeferredCauset)
	if !isDefCaus {
		return "", nil
	}
	name, found := extractDefCauss[col.UniqueID]
	if !found {
		return "", nil
	}
	// All expressions in IN must be a constant
	// SELECT * FROM t1 WHERE c IN ('1', '2')
	var results []types.Causet
	for _, arg := range args[1:] {
		constant, ok := arg.(*expression.CouplingConstantWithRadix)
		if !ok || constant.DeferredExpr != nil || constant.ParamMarker != nil {
			return "", nil
		}
		results = append(results, constant.Value)
	}
	return name.DefCausName.L, results
}

func (helper extractHelper) extractDefCausBinaryOpConsExpr(extractDefCauss map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Causet) {
	args := expr.GetArgs()
	var col *expression.DeferredCauset
	var colIdx int
	// c = 'rhs'
	// 'lhs' = c
	for i := 0; i < 2; i++ {
		var isDefCaus bool
		col, isDefCaus = args[i].(*expression.DeferredCauset)
		if isDefCaus {
			colIdx = i
			break
		}
	}
	if col == nil {
		return "", nil
	}

	name, found := extractDefCauss[col.UniqueID]
	if !found {
		return "", nil
	}
	// The `lhs/rhs` of EQ expression must be a constant
	// SELECT * FROM t1 WHERE c='rhs'
	// SELECT * FROM t1 WHERE 'lhs'=c
	constant, ok := args[1-colIdx].(*expression.CouplingConstantWithRadix)
	if !ok || constant.DeferredExpr != nil || constant.ParamMarker != nil {
		return "", nil
	}
	return name.DefCausName.L, []types.Causet{constant.Value}
}

// extract the OR expression, e.g:
// SELECT * FROM t1 WHERE c1='a' OR c1='b' OR c1='c'
func (helper extractHelper) extractDefCausOrExpr(extractDefCauss map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Causet) {
	args := expr.GetArgs()
	lhs, ok := args[0].(*expression.ScalarFunction)
	if !ok {
		return "", nil
	}
	rhs, ok := args[1].(*expression.ScalarFunction)
	if !ok {
		return "", nil
	}
	// Define an inner function to avoid populate the outer sINTERLOCKe
	var extract = func(extractDefCauss map[int64]*types.FieldName, fn *expression.ScalarFunction) (string, []types.Causet) {
		switch fn.FuncName.L {
		case ast.EQ:
			return helper.extractDefCausBinaryOpConsExpr(extractDefCauss, fn)
		case ast.LogicOr:
			return helper.extractDefCausOrExpr(extractDefCauss, fn)
		case ast.In:
			return helper.extractDefCausInConsExpr(extractDefCauss, fn)
		default:
			return "", nil
		}
	}
	lhsDefCausName, lhsCausets := extract(extractDefCauss, lhs)
	if lhsDefCausName == "" {
		return "", nil
	}
	rhsDefCausName, rhsCausets := extract(extractDefCauss, rhs)
	if lhsDefCausName == rhsDefCausName {
		return lhsDefCausName, append(lhsCausets, rhsCausets...)
	}
	return "", nil
}

// merges `lhs` and `datums` with CNF logic
// 1. Returns `datums` set if the `lhs` is an empty set
// 2. Returns the intersection of `datums` and `lhs` if the `lhs` is not an empty set
func (helper extractHelper) merge(lhs set.StringSet, datums []types.Causet, toLower bool) set.StringSet {
	tmpNodeTypes := set.NewStringSet()
	for _, causet := range datums {
		s, err := causet.ToString()
		if err != nil {
			return nil
		}
		if toLower {
			s = strings.ToLower(s)
		}
		tmpNodeTypes.Insert(s)
	}
	if len(lhs) > 0 {
		return lhs.Intersection(tmpNodeTypes)
	}
	return tmpNodeTypes
}

func (helper extractHelper) extractDefCaus(
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	extractDefCausName string,
	valueToLower bool,
) (
	remained []expression.Expression,
	skipRequest bool,
	result set.StringSet,
) {
	remained = make([]expression.Expression, 0, len(predicates))
	result = set.NewStringSet()
	extractDefCauss := helper.findDeferredCauset(schemaReplicant, names, extractDefCausName)
	if len(extractDefCauss) == 0 {
		return predicates, false, result
	}

	// We should use INTERSECTION of sets because of the predicates is CNF array
	for _, expr := range predicates {
		fn, ok := expr.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		var colName string
		var datums []types.Causet // the memory of datums should not be reused, they will be put into result.
		switch fn.FuncName.L {
		case ast.EQ:
			colName, datums = helper.extractDefCausBinaryOpConsExpr(extractDefCauss, fn)
		case ast.In:
			colName, datums = helper.extractDefCausInConsExpr(extractDefCauss, fn)
		case ast.LogicOr:
			colName, datums = helper.extractDefCausOrExpr(extractDefCauss, fn)
		}
		if colName == extractDefCausName {
			result = helper.merge(result, datums, valueToLower)
			skipRequest = len(result) == 0
		} else {
			remained = append(remained, expr)
		}
		// There are no data if the low-level executor skip request, so the filter can be droped
		if skipRequest {
			remained = remained[:0]
			break
		}
	}
	return
}

// extracts the string pattern column, e.g:
// SELECT * FROM t WHERE c LIKE '%a%'
// SELECT * FROM t WHERE c LIKE '%a%' AND c REGEXP '.*xxx.*'
// SELECT * FROM t WHERE c LIKE '%a%' OR c REGEXP '.*xxx.*'
func (helper extractHelper) extractLikePatternDefCaus(
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	extractDefCausName string,
) (
	remained []expression.Expression,
	patterns []string,
) {
	remained = make([]expression.Expression, 0, len(predicates))
	extractDefCauss := helper.findDeferredCauset(schemaReplicant, names, extractDefCausName)
	if len(extractDefCauss) == 0 {
		return predicates, nil
	}

	// We use a string array to save multiple patterns because the Golang and Rust don't
	// support perl-like CNF regular expression: (?=expr1)(?=expr2).
	// e.g:
	// SELECT * FROM t WHERE c LIKE '%a%' AND c LIKE '%b%' AND c REGEXP 'gc.*[0-9]{10,20}'
	for _, expr := range predicates {
		fn, ok := expr.(*expression.ScalarFunction)
		if !ok {
			remained = append(remained, expr)
			continue
		}

		var canBuildPattern bool
		var pattern string
		// We use '|' to combine DNF regular expression: .*a.*|.*b.*
		// e.g:
		// SELECT * FROM t WHERE c LIKE '%a%' OR c LIKE '%b%'
		if fn.FuncName.L == ast.LogicOr {
			canBuildPattern, pattern = helper.extractOrLikePattern(fn, extractDefCausName, extractDefCauss)
		} else {
			canBuildPattern, pattern = helper.extractLikePattern(fn, extractDefCausName, extractDefCauss)
		}
		if canBuildPattern {
			patterns = append(patterns, pattern)
		} else {
			remained = append(remained, expr)
		}
	}
	return
}

func (helper extractHelper) extractOrLikePattern(
	orFunc *expression.ScalarFunction,
	extractDefCausName string,
	extractDefCauss map[int64]*types.FieldName,
) (
	ok bool,
	pattern string,
) {
	predicates := expression.SplitDNFItems(orFunc)
	if len(predicates) == 0 {
		return false, ""
	}

	patternBuilder := make([]string, 0, len(predicates))
	for _, predicate := range predicates {
		fn, ok := predicate.(*expression.ScalarFunction)
		if !ok {
			return false, ""
		}

		ok, partPattern := helper.extractLikePattern(fn, extractDefCausName, extractDefCauss)
		if !ok {
			return false, ""
		}
		patternBuilder = append(patternBuilder, partPattern)
	}
	return true, strings.Join(patternBuilder, "|")
}

func (helper extractHelper) extractLikePattern(
	fn *expression.ScalarFunction,
	extractDefCausName string,
	extractDefCauss map[int64]*types.FieldName,
) (
	ok bool,
	pattern string,
) {
	var colName string
	var datums []types.Causet
	switch fn.FuncName.L {
	case ast.EQ, ast.Like, ast.Regexp:
		colName, datums = helper.extractDefCausBinaryOpConsExpr(extractDefCauss, fn)
	}
	if colName == extractDefCausName {
		switch fn.FuncName.L {
		case ast.EQ:
			return true, "^" + regexp.QuoteMeta(datums[0].GetString()) + "$"
		case ast.Like:
			return true, stringutil.CompileLike2Regexp(datums[0].GetString())
		case ast.Regexp:
			return true, datums[0].GetString()
		default:
			return false, ""
		}
	} else {
		return false, ""
	}
}

func (helper extractHelper) findDeferredCauset(schemaReplicant *expression.Schema, names []*types.FieldName, colName string) map[int64]*types.FieldName {
	extractDefCauss := make(map[int64]*types.FieldName)
	for i, name := range names {
		if name.DefCausName.L == colName {
			extractDefCauss[schemaReplicant.DeferredCausets[i].UniqueID] = name
		}
	}
	return extractDefCauss
}

// getTimeFunctionName is used to get the (time) function name.
// For the expression that push down to the interlock, the function name is different with normal compare function,
// Then getTimeFunctionName will do a sample function name convert.
// Currently, this is used to support query `CLUSTER_SLOW_QUERY` at any time.
func (helper extractHelper) getTimeFunctionName(fn *expression.ScalarFunction) string {
	switch fn.Function.PbCode() {
	case fidelpb.ScalarFuncSig_GTTime:
		return ast.GT
	case fidelpb.ScalarFuncSig_GETime:
		return ast.GE
	case fidelpb.ScalarFuncSig_LTTime:
		return ast.LT
	case fidelpb.ScalarFuncSig_LETime:
		return ast.LE
	case fidelpb.ScalarFuncSig_EQTime:
		return ast.EQ
	default:
		return fn.FuncName.L
	}
}

// extracts the time range column, e.g:
// SELECT * FROM t WHERE time='2020-10-10 10:10:10'
// SELECT * FROM t WHERE time>'2020-10-10 10:10:10' AND time<'2020-10-11 10:10:10'
func (helper extractHelper) extractTimeRange(
	ctx stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	extractDefCausName string,
	timezone *time.Location,
) (
	remained []expression.Expression,
	// unix timestamp in nanoseconds
	startTime int64,
	endTime int64,
) {
	remained = make([]expression.Expression, 0, len(predicates))
	extractDefCauss := helper.findDeferredCauset(schemaReplicant, names, extractDefCausName)
	if len(extractDefCauss) == 0 {
		return predicates, startTime, endTime
	}

	for _, expr := range predicates {
		fn, ok := expr.(*expression.ScalarFunction)
		if !ok {
			remained = append(remained, expr)
			continue
		}

		var colName string
		var datums []types.Causet
		fnName := helper.getTimeFunctionName(fn)
		switch fnName {
		case ast.GT, ast.GE, ast.LT, ast.LE, ast.EQ:
			colName, datums = helper.extractDefCausBinaryOpConsExpr(extractDefCauss, fn)
		}

		if colName == extractDefCausName {
			timeType := types.NewFieldType(allegrosql.TypeDatetime)
			timeType.Decimal = 6
			timeCauset, err := datums[0].ConvertTo(ctx.GetStochaseinstein_dbars().StmtCtx, timeType)
			if err != nil || timeCauset.HoTT() == types.HoTTNull {
				remained = append(remained, expr)
				continue
			}

			mysqlTime := timeCauset.GetMysqlTime()
			timestamp := time.Date(mysqlTime.Year(),
				time.Month(mysqlTime.Month()),
				mysqlTime.Day(),
				mysqlTime.Hour(),
				mysqlTime.Minute(),
				mysqlTime.Second(),
				mysqlTime.Microsecond()*1000,
				timezone,
			).UnixNano()

			switch fnName {
			case ast.EQ:
				startTime = mathutil.MaxInt64(startTime, timestamp)
				if endTime == 0 {
					endTime = timestamp
				} else {
					endTime = mathutil.MinInt64(endTime, timestamp)
				}
			case ast.GT:
				// FixMe: add 1ms is not absolutely correct here, just because the log search precision is millisecond.
				startTime = mathutil.MaxInt64(startTime, timestamp+int64(time.Millisecond))
			case ast.GE:
				startTime = mathutil.MaxInt64(startTime, timestamp)
			case ast.LT:
				if endTime == 0 {
					endTime = timestamp - int64(time.Millisecond)
				} else {
					endTime = mathutil.MinInt64(endTime, timestamp-int64(time.Millisecond))
				}
			case ast.LE:
				if endTime == 0 {
					endTime = timestamp
				} else {
					endTime = mathutil.MinInt64(endTime, timestamp)
				}
			default:
				remained = append(remained, expr)
			}
		} else {
			remained = append(remained, expr)
		}
	}
	return
}

func (helper extractHelper) parseQuantiles(quantileSet set.StringSet) []float64 {
	quantiles := make([]float64, 0, len(quantileSet))
	for k := range quantileSet {
		v, err := strconv.ParseFloat(k, 64)
		if err != nil {
			// ignore the parse error won't affect result.
			continue
		}
		quantiles = append(quantiles, v)
	}
	sort.Float64s(quantiles)
	return quantiles
}

func (helper extractHelper) extractDefCauss(
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
	excludeDefCauss set.StringSet,
	valueToLower bool) ([]expression.Expression, bool, map[string]set.StringSet) {
	defcaus := map[string]set.StringSet{}
	remained := predicates
	skipRequest := false
	// Extract the label columns.
	for _, name := range names {
		if excludeDefCauss.Exist(name.DefCausName.L) {
			continue
		}
		var values set.StringSet
		remained, skipRequest, values = helper.extractDefCaus(schemaReplicant, names, remained, name.DefCausName.L, valueToLower)
		if skipRequest {
			return nil, true, nil
		}
		if len(values) == 0 {
			continue
		}
		defcaus[name.DefCausName.L] = values
	}
	return remained, skipRequest, defcaus
}

func (helper extractHelper) convertToTime(t int64) time.Time {
	if t == 0 || t == math.MaxInt64 {
		return time.Now()
	}
	return time.Unix(0, t)
}

// ClusterBlockExtractor is used to extract some predicates of cluster block.
type ClusterBlockExtractor struct {
	extractHelper

	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest bool

	// NodeTypes represents all components types we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_config WHERE type='einsteindb'
	// 2. SELECT * FROM cluster_config WHERE type in ('einsteindb', 'milevadb')
	NodeTypes set.StringSet

	// Instances represents all components instances we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_config WHERE instance='192.168.1.7:2379'
	// 2. SELECT * FROM cluster_config WHERE type in ('192.168.1.7:2379', '192.168.1.9:2379')
	Instances set.StringSet
}

// Extract implements the MemBlockPredicateExtractor Extract interface
func (e *ClusterBlockExtractor) Extract(_ stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	remained, typeSkipRequest, nodeTypes := e.extractDefCaus(schemaReplicant, names, predicates, "type", true)
	remained, addrSkipRequest, instances := e.extractDefCaus(schemaReplicant, names, remained, "instance", false)
	e.SkipRequest = typeSkipRequest || addrSkipRequest
	e.NodeTypes = nodeTypes
	e.Instances = instances
	return remained
}

func (e *ClusterBlockExtractor) explainInfo(p *PhysicalMemBlock) string {
	if e.SkipRequest {
		return "skip_request:true"
	}
	r := new(bytes.Buffer)
	if len(e.NodeTypes) > 0 {
		r.WriteString(fmt.Sprintf("node_types:[%s], ", extractStringFromStringSet(e.NodeTypes)))
	}
	if len(e.Instances) > 0 {
		r.WriteString(fmt.Sprintf("instances:[%s], ", extractStringFromStringSet(e.Instances)))
	}
	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// ClusterLogBlockExtractor is used to extract some predicates of `cluster_config`
type ClusterLogBlockExtractor struct {
	extractHelper

	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest bool

	// NodeTypes represents all components types we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_log WHERE type='einsteindb'
	// 2. SELECT * FROM cluster_log WHERE type in ('einsteindb', 'milevadb')
	NodeTypes set.StringSet

	// Instances represents all components instances we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_log WHERE instance='192.168.1.7:2379'
	// 2. SELECT * FROM cluster_log WHERE instance in ('192.168.1.7:2379', '192.168.1.9:2379')
	Instances set.StringSet

	// StartTime represents the beginning time of log message
	// e.g: SELECT * FROM cluster_log WHERE time>'2020-10-10 10:10:10.999'
	StartTime int64
	// EndTime represents the ending time of log message
	// e.g: SELECT * FROM cluster_log WHERE time<'2020-10-11 10:10:10.999'
	EndTime int64
	// Pattern is used to filter the log message
	// e.g:
	// 1. SELECT * FROM cluster_log WHERE message like '%gc%'
	// 2. SELECT * FROM cluster_log WHERE message regexp '.*'
	Patterns  []string
	LogLevels set.StringSet
}

// Extract implements the MemBlockPredicateExtractor Extract interface
func (e *ClusterLogBlockExtractor) Extract(
	ctx stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	// Extract the `type/instance` columns
	remained, typeSkipRequest, nodeTypes := e.extractDefCaus(schemaReplicant, names, predicates, "type", true)
	remained, addrSkipRequest, instances := e.extractDefCaus(schemaReplicant, names, remained, "instance", false)
	remained, levlSkipRequest, logLevels := e.extractDefCaus(schemaReplicant, names, remained, "level", true)
	e.SkipRequest = typeSkipRequest || addrSkipRequest || levlSkipRequest
	e.NodeTypes = nodeTypes
	e.Instances = instances
	e.LogLevels = logLevels
	if e.SkipRequest {
		return nil
	}

	remained, startTime, endTime := e.extractTimeRange(ctx, schemaReplicant, names, remained, "time", time.Local)
	// The time unit for search log is millisecond.
	startTime = startTime / int64(time.Millisecond)
	endTime = endTime / int64(time.Millisecond)
	e.StartTime = startTime
	e.EndTime = endTime
	if startTime != 0 && endTime != 0 {
		e.SkipRequest = startTime > endTime
	}

	if e.SkipRequest {
		return nil
	}

	remained, patterns := e.extractLikePatternDefCaus(schemaReplicant, names, remained, "message")
	e.Patterns = patterns
	return remained
}

func (e *ClusterLogBlockExtractor) explainInfo(p *PhysicalMemBlock) string {
	if e.SkipRequest {
		return "skip_request: true"
	}
	r := new(bytes.Buffer)
	st, et := e.StartTime, e.EndTime
	if st > 0 {
		st := time.Unix(0, st*1e6)
		r.WriteString(fmt.Sprintf("start_time:%v, ", st.In(p.ctx.GetStochaseinstein_dbars().StmtCtx.TimeZone).Format(MetricBlockTimeFormat)))
	}
	if et > 0 {
		et := time.Unix(0, et*1e6)
		r.WriteString(fmt.Sprintf("end_time:%v, ", et.In(p.ctx.GetStochaseinstein_dbars().StmtCtx.TimeZone).Format(MetricBlockTimeFormat)))
	}
	if len(e.NodeTypes) > 0 {
		r.WriteString(fmt.Sprintf("node_types:[%s], ", extractStringFromStringSet(e.NodeTypes)))
	}
	if len(e.Instances) > 0 {
		r.WriteString(fmt.Sprintf("instances:[%s], ", extractStringFromStringSet(e.Instances)))
	}
	if len(e.LogLevels) > 0 {
		r.WriteString(fmt.Sprintf("log_levels:[%s], ", extractStringFromStringSet(e.LogLevels)))
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// MetricBlockExtractor is used to extract some predicates of metrics_schema blocks.
type MetricBlockExtractor struct {
	extractHelper
	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest bool
	// StartTime represents the beginning time of metric data.
	StartTime time.Time
	// EndTime represents the ending time of metric data.
	EndTime time.Time
	// LabelConditions represents the label conditions of metric data.
	LabelConditions map[string]set.StringSet
	Quantiles       []float64
}

func newMetricBlockExtractor() *MetricBlockExtractor {
	e := &MetricBlockExtractor{}
	e.StartTime, e.EndTime = e.getTimeRange(0, 0)
	return e
}

// Extract implements the MemBlockPredicateExtractor Extract interface
func (e *MetricBlockExtractor) Extract(
	ctx stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	// Extract the `quantile` columns
	remained, skipRequest, quantileSet := e.extractDefCaus(schemaReplicant, names, predicates, "quantile", true)
	e.Quantiles = e.parseQuantiles(quantileSet)
	e.SkipRequest = skipRequest
	if e.SkipRequest {
		return nil
	}

	// Extract the `time` columns
	remained, startTime, endTime := e.extractTimeRange(ctx, schemaReplicant, names, remained, "time", ctx.GetStochaseinstein_dbars().StmtCtx.TimeZone)
	e.StartTime, e.EndTime = e.getTimeRange(startTime, endTime)
	e.SkipRequest = e.StartTime.After(e.EndTime)
	if e.SkipRequest {
		return nil
	}

	excludeDefCauss := set.NewStringSet("quantile", "time", "value")
	_, skipRequest, extractDefCauss := e.extractDefCauss(schemaReplicant, names, remained, excludeDefCauss, false)
	e.SkipRequest = skipRequest
	if e.SkipRequest {
		return nil
	}
	e.LabelConditions = extractDefCauss
	// For some metric, the metric reader can't use the predicate, so keep all label conditions remained.
	return remained
}

func (e *MetricBlockExtractor) getTimeRange(start, end int64) (time.Time, time.Time) {
	const defaultMetricQueryDuration = 10 * time.Minute
	var startTime, endTime time.Time
	if start == 0 && end == 0 {
		endTime = time.Now()
		return endTime.Add(-defaultMetricQueryDuration), endTime
	}
	if start != 0 {
		startTime = e.convertToTime(start)
	}
	if end != 0 {
		endTime = e.convertToTime(end)
	}
	if start == 0 {
		startTime = endTime.Add(-defaultMetricQueryDuration)
	}
	if end == 0 {
		endTime = startTime.Add(defaultMetricQueryDuration)
	}
	return startTime, endTime
}

func (e *MetricBlockExtractor) explainInfo(p *PhysicalMemBlock) string {
	if e.SkipRequest {
		return "skip_request: true"
	}
	promQL := e.GetMetricBlockPromQL(p.ctx, p.Block.Name.L)
	startTime, endTime := e.StartTime, e.EndTime
	step := time.Second * time.Duration(p.ctx.GetStochaseinstein_dbars().MetricSchemaStep)
	return fmt.Sprintf("PromQL:%v, start_time:%v, end_time:%v, step:%v",
		promQL,
		startTime.In(p.ctx.GetStochaseinstein_dbars().StmtCtx.TimeZone).Format(MetricBlockTimeFormat),
		endTime.In(p.ctx.GetStochaseinstein_dbars().StmtCtx.TimeZone).Format(MetricBlockTimeFormat),
		step,
	)
}

// GetMetricBlockPromQL uses to get the promQL of metric block.
func (e *MetricBlockExtractor) GetMetricBlockPromQL(sctx stochastikctx.Context, lowerBlockName string) string {
	quantiles := e.Quantiles
	def, err := schemareplicant.GetMetricBlockDef(lowerBlockName)
	if err != nil {
		return ""
	}
	if len(quantiles) == 0 {
		quantiles = []float64{def.Quantile}
	}
	var buf bytes.Buffer
	for i, quantile := range quantiles {
		promQL := def.GenPromQL(sctx, e.LabelConditions, quantile)
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(promQL)
	}
	return buf.String()
}

// MetricSummaryBlockExtractor is used to extract some predicates of metrics_schema blocks.
type MetricSummaryBlockExtractor struct {
	extractHelper
	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest  bool
	MetricsNames set.StringSet
	Quantiles    []float64
}

// Extract implements the MemBlockPredicateExtractor Extract interface
func (e *MetricSummaryBlockExtractor) Extract(
	_ stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	remained, quantileSkip, quantiles := e.extractDefCaus(schemaReplicant, names, predicates, "quantile", false)
	remained, metricsNameSkip, metricsNames := e.extractDefCaus(schemaReplicant, names, predicates, "metrics_name", true)
	e.SkipRequest = quantileSkip || metricsNameSkip
	e.Quantiles = e.parseQuantiles(quantiles)
	e.MetricsNames = metricsNames
	return remained
}

func (e *MetricSummaryBlockExtractor) explainInfo(p *PhysicalMemBlock) string {
	return ""
}

// InspectionResultBlockExtractor is used to extract some predicates of `inspection_result`
type InspectionResultBlockExtractor struct {
	extractHelper
	// SkipInspection means the where clause always false, we don't need to request any component
	SkipInspection bool
	// Memrules represents rules applied to, and we should apply all inspection rules if there is no rules specified
	// e.g: SELECT * FROM inspection_result WHERE rule in ('dbs', 'config')
	Memrules set.StringSet
	// Items represents items applied to, and we should apply all inspection item if there is no rules specified
	// e.g: SELECT * FROM inspection_result WHERE item in ('dbs.lease', 'raftstore.threadpool')
	Items set.StringSet
}

// Extract implements the MemBlockPredicateExtractor Extract interface
func (e *InspectionResultBlockExtractor) Extract(
	_ stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	// Extract the `rule/item` columns
	remained, ruleSkip, rules := e.extractDefCaus(schemaReplicant, names, predicates, "rule", true)
	remained, itemSkip, items := e.extractDefCaus(schemaReplicant, names, remained, "item", true)
	e.SkipInspection = ruleSkip || itemSkip
	e.Memrules = rules
	e.Items = items
	return remained
}

func (e *InspectionResultBlockExtractor) explainInfo(p *PhysicalMemBlock) string {
	if e.SkipInspection {
		return "skip_inspection:true"
	}
	s := make([]string, 0, 2)
	s = append(s, fmt.Sprintf("rules:[%s]", extractStringFromStringSet(e.Memrules)))
	s = append(s, fmt.Sprintf("items:[%s]", extractStringFromStringSet(e.Items)))
	return strings.Join(s, ", ")
}

// InspectionSummaryBlockExtractor is used to extract some predicates of `inspection_summary`
type InspectionSummaryBlockExtractor struct {
	extractHelper
	// SkipInspection means the where clause always false, we don't need to request any component
	SkipInspection bool
	// Memrules represents rules applied to, and we should apply all inspection rules if there is no rules specified
	// e.g: SELECT * FROM inspection_summary WHERE rule in ('dbs', 'config')
	Memrules    set.StringSet
	MetricNames set.StringSet
	Quantiles   []float64
}

// Extract implements the MemBlockPredicateExtractor Extract interface
func (e *InspectionSummaryBlockExtractor) Extract(
	_ stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	// Extract the `rule` columns
	_, ruleSkip, rules := e.extractDefCaus(schemaReplicant, names, predicates, "rule", true)
	// Extract the `metric_name` columns
	_, metricNameSkip, metricNames := e.extractDefCaus(schemaReplicant, names, predicates, "metrics_name", true)
	// Extract the `quantile` columns
	remained, quantileSkip, quantileSet := e.extractDefCaus(schemaReplicant, names, predicates, "quantile", false)
	e.SkipInspection = ruleSkip || quantileSkip || metricNameSkip
	e.Memrules = rules
	e.Quantiles = e.parseQuantiles(quantileSet)
	e.MetricNames = metricNames
	return remained
}

func (e *InspectionSummaryBlockExtractor) explainInfo(p *PhysicalMemBlock) string {
	if e.SkipInspection {
		return "skip_inspection: true"
	}

	r := new(bytes.Buffer)
	if len(e.Memrules) > 0 {
		r.WriteString(fmt.Sprintf("rules:[%s], ", extractStringFromStringSet(e.Memrules)))
	}
	if len(e.MetricNames) > 0 {
		r.WriteString(fmt.Sprintf("metric_names:[%s], ", extractStringFromStringSet(e.MetricNames)))
	}
	if len(e.Quantiles) > 0 {
		r.WriteString("quantiles:[")
		for i, quantile := range e.Quantiles {
			if i > 0 {
				r.WriteByte(',')
			}
			r.WriteString(fmt.Sprintf("%f", quantile))
		}
		r.WriteString("], ")
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// InspectionMemruleBlockExtractor is used to extract some predicates of `inspection_rules`
type InspectionMemruleBlockExtractor struct {
	extractHelper

	SkipRequest bool
	Types       set.StringSet
}

// Extract implements the MemBlockPredicateExtractor Extract interface
func (e *InspectionMemruleBlockExtractor) Extract(
	_ stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) (remained []expression.Expression) {
	// Extract the `type` columns
	remained, tpSkip, tps := e.extractDefCaus(schemaReplicant, names, predicates, "type", true)
	e.SkipRequest = tpSkip
	e.Types = tps
	return remained
}

func (e *InspectionMemruleBlockExtractor) explainInfo(p *PhysicalMemBlock) string {
	if e.SkipRequest {
		return "skip_request: true"
	}

	r := new(bytes.Buffer)
	if len(e.Types) > 0 {
		r.WriteString(fmt.Sprintf("node_types:[%s]", extractStringFromStringSet(e.Types)))
	}
	return r.String()
}

// SlowQueryExtractor is used to extract some predicates of `slow_query`
type SlowQueryExtractor struct {
	extractHelper

	SkipRequest bool
	StartTime   time.Time
	EndTime     time.Time
	// Enable is true means the executor should use the time range to locate the slow-log file that need to be parsed.
	// Enable is false, means the executor should keep the behavior compatible with before, which is only parse the
	// current slow-log file.
	Enable bool
}

// Extract implements the MemBlockPredicateExtractor Extract interface
func (e *SlowQueryExtractor) Extract(
	ctx stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	remained, startTime, endTime := e.extractTimeRange(ctx, schemaReplicant, names, predicates, "time", ctx.GetStochaseinstein_dbars().StmtCtx.TimeZone)
	e.setTimeRange(startTime, endTime)
	e.SkipRequest = e.Enable && e.StartTime.After(e.EndTime)
	if e.SkipRequest {
		return nil
	}
	return remained
}

func (e *SlowQueryExtractor) setTimeRange(start, end int64) {
	const defaultSlowQueryDuration = 24 * time.Hour
	var startTime, endTime time.Time
	if start == 0 && end == 0 {
		return
	}
	if start != 0 {
		startTime = e.convertToTime(start)
	}
	if end != 0 {
		endTime = e.convertToTime(end)
	}
	if start == 0 {
		startTime = endTime.Add(-defaultSlowQueryDuration)
	}
	if end == 0 {
		endTime = startTime.Add(defaultSlowQueryDuration)
	}
	e.StartTime, e.EndTime = startTime, endTime
	e.Enable = true
}

// BlockStorageStatsExtractor is used to extract some predicates of `disk_usage`.
type BlockStorageStatsExtractor struct {
	extractHelper
	// SkipRequest means the where clause always false, we don't need to request any component.
	SkipRequest bool
	// BlockSchema represents blockSchema applied to, and we should apply all block disk usage if there is no schemaReplicant specified.
	// e.g: SELECT * FROM information_schema.disk_usage WHERE block_schema in ('test', 'information_schema').
	BlockSchema set.StringSet
	// BlockName represents blockName applied to, and we should apply all block disk usage if there is no block specified.
	// e.g: SELECT * FROM information_schema.disk_usage WHERE block in ('schemata', 'blocks').
	BlockName set.StringSet
}

// Extract implements the MemBlockPredicateExtractor Extract interface.
func (e *BlockStorageStatsExtractor) Extract(
	_ stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	// Extract the `block_schema` columns.
	remained, schemaSkip, blockSchema := e.extractDefCaus(schemaReplicant, names, predicates, "block_schema", true)
	// Extract the `block_name` columns.
	remained, blockSkip, blockName := e.extractDefCaus(schemaReplicant, names, remained, "block_name", true)
	e.SkipRequest = schemaSkip || blockSkip
	if e.SkipRequest {
		return nil
	}
	e.BlockSchema = blockSchema
	e.BlockName = blockName
	return remained
}

func (e *BlockStorageStatsExtractor) explainInfo(p *PhysicalMemBlock) string {
	if e.SkipRequest {
		return "skip_request: true"
	}

	r := new(bytes.Buffer)
	if len(e.BlockSchema) > 0 {
		r.WriteString(fmt.Sprintf("schemaReplicant:[%s]", extractStringFromStringSet(e.BlockSchema)))
	}
	if r.Len() > 0 && len(e.BlockName) > 0 {
		r.WriteString(", ")
	}
	if len(e.BlockName) > 0 {
		r.WriteString(fmt.Sprintf("block:[%s]", extractStringFromStringSet(e.BlockName)))
	}
	return r.String()
}

func (e *SlowQueryExtractor) explainInfo(p *PhysicalMemBlock) string {
	if e.SkipRequest {
		return "skip_request: true"
	}
	if !e.Enable {
		return fmt.Sprintf("only search in the current '%v' file", p.ctx.GetStochaseinstein_dbars().SlowQueryFile)
	}
	startTime := e.StartTime.In(p.ctx.GetStochaseinstein_dbars().StmtCtx.TimeZone)
	endTime := e.EndTime.In(p.ctx.GetStochaseinstein_dbars().StmtCtx.TimeZone)
	return fmt.Sprintf("start_time:%v, end_time:%v",
		types.NewTime(types.FromGoTime(startTime), allegrosql.TypeDatetime, types.MaxFsp).String(),
		types.NewTime(types.FromGoTime(endTime), allegrosql.TypeDatetime, types.MaxFsp).String())
}

// TiFlashSystemBlockExtractor is used to extract some predicates of tiflash system block.
type TiFlashSystemBlockExtractor struct {
	extractHelper

	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest bool
	// TiFlashInstances represents all tiflash instances we should send request to.
	// e.g:
	// 1. SELECT * FROM information_schema.<block_name> WHERE tiflash_instance='192.168.1.7:3930'
	// 2. SELECT * FROM information_schema.<block_name> WHERE tiflash_instance in ('192.168.1.7:3930', '192.168.1.9:3930')
	TiFlashInstances set.StringSet
	// MilevaDBDatabases represents milevadbDatabases applied to, and we should apply all milevadb database if there is no database specified.
	// e.g: SELECT * FROM information_schema.<block_name> WHERE milevadb_database in ('test', 'test2').
	MilevaDBDatabases string
	// MilevaDBBlocks represents milevadbBlocks applied to, and we should apply all milevadb block if there is no block specified.
	// e.g: SELECT * FROM information_schema.<block_name> WHERE milevadb_block in ('t', 't2').
	MilevaDBBlocks string
}

// Extract implements the MemBlockPredicateExtractor Extract interface
func (e *TiFlashSystemBlockExtractor) Extract(_ stochastikctx.Context,
	schemaReplicant *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	// Extract the `tiflash_instance` columns.
	remained, instanceSkip, tiflashInstances := e.extractDefCaus(schemaReplicant, names, predicates, "tiflash_instance", false)
	// Extract the `milevadb_database` columns.
	remained, databaseSkip, milevadbDatabases := e.extractDefCaus(schemaReplicant, names, remained, "milevadb_database", true)
	// Extract the `milevadb_block` columns.
	remained, blockSkip, milevadbBlocks := e.extractDefCaus(schemaReplicant, names, remained, "milevadb_block", true)
	e.SkipRequest = instanceSkip || databaseSkip || blockSkip
	if e.SkipRequest {
		return nil
	}
	e.TiFlashInstances = tiflashInstances
	e.MilevaDBDatabases = extractStringFromStringSet(milevadbDatabases)
	e.MilevaDBBlocks = extractStringFromStringSet(milevadbBlocks)
	return remained
}

func (e *TiFlashSystemBlockExtractor) explainInfo(p *PhysicalMemBlock) string {
	if e.SkipRequest {
		return "skip_request:true"
	}
	r := new(bytes.Buffer)
	if len(e.TiFlashInstances) > 0 {
		r.WriteString(fmt.Sprintf("tiflash_instances:[%s], ", extractStringFromStringSet(e.TiFlashInstances)))
	}
	if len(e.MilevaDBDatabases) > 0 {
		r.WriteString(fmt.Sprintf("milevadb_databases:[%s], ", e.MilevaDBDatabases))
	}
	if len(e.MilevaDBBlocks) > 0 {
		r.WriteString(fmt.Sprintf("milevadb_blocks:[%s], ", e.MilevaDBBlocks))
	}
	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}
