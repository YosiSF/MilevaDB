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
	"strconv"
	"strings"

	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/set"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/sqlexec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
)

type (
	// inspectionResult represents a abnormal diagnosis result
	inspectionResult struct {
		tp            string
		instance      string
		statusAddress string
		// represents the diagnostics item, e.g: `dbs.lease` `raftstore.cpuusage`
		item string
		// diagnosis result value base on current cluster status
		actual   string
		expected string
		severity string
		detail   string
		// degree only used for sort.
		degree float64
	}

	inspectionName string

	inspectionFilter struct {
		set       set.StringSet
		timeRange plannercore.QueryTimeRange
	}

	inspectionMemrule interface {
		name() string
		inspect(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult
	}
)

func (n inspectionName) name() string {
	return string(n)
}

func (f inspectionFilter) enable(name string) bool {
	return len(f.set) == 0 || f.set.Exist(name)
}

func (f inspectionFilter) exist(name string) bool {
	return len(f.set) > 0 && f.set.Exist(name)
}

type (
	// configInspection is used to check whether a same configuration item has a
	// different value between different instance in the cluster
	configInspection struct{ inspectionName }

	// versionInspection is used to check whether the same component has different
	// version in the cluster
	versionInspection struct{ inspectionName }

	// nodeLoadInspection is used to check the node load of memory/disk/cpu
	// have reached a high-level threshold
	nodeLoadInspection struct{ inspectionName }

	// criticalErrorInspection is used to check are there some critical errors
	// occurred in the past
	criticalErrorInspection struct{ inspectionName }

	// thresholdCheckInspection is used to check some threshold value, like CPU usage, leader count change.
	thresholdCheckInspection struct{ inspectionName }
)

var inspectionMemrules = []inspectionMemrule{
	&configInspection{inspectionName: "config"},
	&versionInspection{inspectionName: "version"},
	&nodeLoadInspection{inspectionName: "node-load"},
	&criticalErrorInspection{inspectionName: "critical-error"},
	&thresholdCheckInspection{inspectionName: "threshold-check"},
}

type inspectionResultRetriever struct {
	dummyCloser
	retrieved               bool
	extractor               *plannercore.InspectionResultBlockExtractor
	timeRange               plannercore.QueryTimeRange
	instanceToStatusAddress map[string]string
	statusToInstanceAddress map[string]string
}

func (e *inspectionResultRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.retrieved || e.extractor.SkipInspection {
		return nil, nil
	}
	e.retrieved = true

	// Some data of cluster-level memory blocks will be retrieved many times in different inspection rules,
	// and the cost of retrieving some data is expensive. We use the `BlockSnapshot` to cache those data
	// and obtain them lazily, and provide a consistent view of inspection blocks for each inspection rules.
	// All cached snapshots should be released at the end of retrieving.
	sctx.GetStochaseinstein_dbars().InspectionBlockCache = map[string]variable.BlockSnapshot{}
	defer func() { sctx.GetStochaseinstein_dbars().InspectionBlockCache = nil }()

	failpoint.InjectContext(ctx, "mockMergeMockInspectionBlocks", func() {
		// Merge mock snapshots injected from failpoint for test purpose
		mockBlocks, ok := ctx.Value("__mockInspectionBlocks").(map[string]variable.BlockSnapshot)
		if ok {
			for name, snap := range mockBlocks {
				sctx.GetStochaseinstein_dbars().InspectionBlockCache[strings.ToLower(name)] = snap
			}
		}
	})

	if e.instanceToStatusAddress == nil {
		// Get cluster info.
		e.instanceToStatusAddress = make(map[string]string)
		e.statusToInstanceAddress = make(map[string]string)
		allegrosql := "select instance,status_address from information_schema.cluster_info;"
		rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
		if err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("get cluster info failed: %v", err))
		}
		for _, event := range rows {
			if event.Len() < 2 {
				continue
			}
			e.instanceToStatusAddress[event.GetString(0)] = event.GetString(1)
			e.statusToInstanceAddress[event.GetString(1)] = event.GetString(0)
		}
	}

	rules := inspectionFilter{set: e.extractor.Memrules}
	items := inspectionFilter{set: e.extractor.Items, timeRange: e.timeRange}
	var finalEvents [][]types.Causet
	for _, r := range inspectionMemrules {
		name := r.name()
		if !rules.enable(name) {
			continue
		}
		results := r.inspect(ctx, sctx, items)
		if len(results) == 0 {
			continue
		}
		// make result sblock
		sort.Slice(results, func(i, j int) bool {
			if results[i].degree != results[j].degree {
				return results[i].degree > results[j].degree
			}
			if lhs, rhs := results[i].item, results[j].item; lhs != rhs {
				return lhs < rhs
			}
			if results[i].actual != results[j].actual {
				return results[i].actual < results[j].actual
			}
			if lhs, rhs := results[i].tp, results[j].tp; lhs != rhs {
				return lhs < rhs
			}
			return results[i].instance < results[j].instance
		})
		for _, result := range results {
			if len(result.instance) == 0 {
				result.instance = e.statusToInstanceAddress[result.statusAddress]
			}
			if len(result.statusAddress) == 0 {
				result.statusAddress = e.instanceToStatusAddress[result.instance]
			}
			finalEvents = append(finalEvents, types.MakeCausets(
				name,
				result.item,
				result.tp,
				result.instance,
				result.statusAddress,
				result.actual,
				result.expected,
				result.severity,
				result.detail,
			))
		}
	}
	return finalEvents, nil
}

func (c configInspection) inspect(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	var results []inspectionResult
	results = append(results, c.inspectDiffConfig(ctx, sctx, filter)...)
	results = append(results, c.inspectCheckConfig(ctx, sctx, filter)...)
	return results
}

func (configInspection) inspectDiffConfig(_ context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	// check the configuration consistent
	ignoreConfigKey := []string{
		// MilevaDB
		"port",
		"status.status-port",
		"host",
		"path",
		"advertise-address",
		"status.status-port",
		"log.file.filename",
		"log.slow-query-file",
		"tmp-storage-path",

		// FIDel
		"advertise-client-urls",
		"advertise-peer-urls",
		"client-urls",
		"data-dir",
		"log-file",
		"log.file.filename",
		"metric.job",
		"name",
		"peer-urls",

		// EinsteinDB
		"server.addr",
		"server.advertise-addr",
		"server.status-addr",
		"log-file",
		"raftstore.raftdb-path",
		"storage.data-dir",
		"storage.block-cache.capacity",
	}
	allegrosql := fmt.Sprintf("select type, `key`, count(distinct value) as c from information_schema.cluster_config where `key` not in ('%s') group by type, `key` having c > 1",
		strings.Join(ignoreConfigKey, "','"))
	rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("check configuration consistency failed: %v", err))
	}

	generateDetail := func(tp, item string) string {
		query := fmt.Sprintf("select value, instance from information_schema.cluster_config where type='%s' and `key`='%s';", tp, item)
		rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(query)
		if err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("check configuration consistency failed: %v", err))
			return fmt.Sprintf("the cluster has different config value of %[2]s, execute the allegrosql to see more detail: select * from information_schema.cluster_config where type='%[1]s' and `key`='%[2]s'",
				tp, item)
		}
		m := make(map[string][]string)
		for _, event := range rows {
			value := event.GetString(0)
			instance := event.GetString(1)
			m[value] = append(m[value], instance)
		}
		groups := make([]string, 0, len(m))
		for k, v := range m {
			sort.Strings(v)
			groups = append(groups, fmt.Sprintf("%s config value is %s", strings.Join(v, ","), k))
		}
		sort.Strings(groups)
		return strings.Join(groups, "\n")
	}

	var results []inspectionResult
	for _, event := range rows {
		if filter.enable(event.GetString(1)) {
			detail := generateDetail(event.GetString(0), event.GetString(1))
			results = append(results, inspectionResult{
				tp:       event.GetString(0),
				instance: "",
				item:     event.GetString(1), // key
				actual:   "inconsistent",
				expected: "consistent",
				severity: "warning",
				detail:   detail,
			})
		}
	}
	return results
}

func (c configInspection) inspectCheckConfig(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	// check the configuration in reason.
	cases := []struct {
		tp     string
		key    string
		value  string
		detail string
	}{
		{
			tp:     "milevadb",
			key:    "log.slow-threshold",
			value:  "0",
			detail: "slow-threshold = 0 will record every query to slow log, it may affect performance",
		},
		{
			tp:     "einsteindb",
			key:    "raftstore.sync-log",
			value:  "false",
			detail: "sync-log should be true to avoid recover region when the machine breaks down",
		},
	}

	var results []inspectionResult
	for _, cas := range cases {
		if !filter.enable(cas.key) {
			continue
		}
		allegrosql := fmt.Sprintf("select instance from information_schema.cluster_config where type = '%s' and `key` = '%s' and value = '%s'",
			cas.tp, cas.key, cas.value)
		rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
		if err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("check configuration in reason failed: %v", err))
		}

		for _, event := range rows {
			results = append(results, inspectionResult{
				tp:       cas.tp,
				instance: event.GetString(0),
				item:     cas.key,
				actual:   cas.value,
				expected: "not " + cas.value,
				severity: "warning",
				detail:   cas.detail,
			})
		}
	}
	results = append(results, c.checkEinsteinDBBlockCacheSizeConfig(ctx, sctx, filter)...)
	return results
}

func (c configInspection) checkEinsteinDBBlockCacheSizeConfig(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	item := "storage.block-cache.capacity"
	if !filter.enable(item) {
		return nil
	}
	allegrosql := "select instance,value from information_schema.cluster_config where type='einsteindb' and `key` = 'storage.block-cache.capacity'"
	rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
	if err != nil {
		sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("check configuration in reason failed: %v", err))
	}
	extractIP := func(addr string) string {
		if idx := strings.Index(addr, ":"); idx > -1 {
			return addr[0:idx]
		}
		return addr
	}

	ipToBlockSize := make(map[string]uint64)
	ipToCount := make(map[string]int)
	for _, event := range rows {
		ip := extractIP(event.GetString(0))
		size, err := c.convertReadableSizeToByteSize(event.GetString(1))
		if err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("check EinsteinDB block-cache configuration in reason failed: %v", err))
			return nil
		}
		ipToBlockSize[ip] += size
		ipToCount[ip]++
	}

	allegrosql = "select instance, value from metrics_schema.node_total_memory where time=now()"
	rows, _, err = sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
	if err != nil {
		sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("check configuration in reason failed: %v", err))
	}
	ipToMemorySize := make(map[string]float64)
	for _, event := range rows {
		ip := extractIP(event.GetString(0))
		size := event.GetFloat64(1)
		ipToMemorySize[ip] += size
	}

	var results []inspectionResult
	for ip, blockSize := range ipToBlockSize {
		if memorySize, ok := ipToMemorySize[ip]; ok {
			if float64(blockSize) > memorySize*0.45 {
				detail := fmt.Sprintf("There are %v EinsteinDB server in %v node, the total 'storage.block-cache.capacity' of EinsteinDB is more than (0.45 * total node memory)",
					ipToCount[ip], ip)
				results = append(results, inspectionResult{
					tp:       "einsteindb",
					instance: ip,
					item:     item,
					actual:   fmt.Sprintf("%v", blockSize),
					expected: fmt.Sprintf("< %.0f", memorySize*0.45),
					severity: "warning",
					detail:   detail,
				})
			}
		}
	}
	return results
}

func (configInspection) convertReadableSizeToByteSize(sizeStr string) (uint64, error) {
	const KB = uint64(1024)
	const MB = KB * 1024
	const GB = MB * 1024
	const TB = GB * 1024
	const PB = TB * 1024

	rate := uint64(1)
	if strings.HasSuffix(sizeStr, "KiB") {
		rate = KB
	} else if strings.HasSuffix(sizeStr, "MiB") {
		rate = MB
	} else if strings.HasSuffix(sizeStr, "GiB") {
		rate = GB
	} else if strings.HasSuffix(sizeStr, "TiB") {
		rate = TB
	} else if strings.HasSuffix(sizeStr, "PiB") {
		rate = PB
	}
	if rate != 1 && len(sizeStr) > 3 {
		sizeStr = sizeStr[:len(sizeStr)-3]
	}
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return uint64(size) * rate, nil
}

func (versionInspection) inspect(_ context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	// check the configuration consistent
	allegrosql := "select type, count(distinct git_hash) as c from information_schema.cluster_info group by type having c > 1;"
	rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("check version consistency failed: %v", err))
	}

	const name = "git_hash"
	var results []inspectionResult
	for _, event := range rows {
		if filter.enable(name) {
			results = append(results, inspectionResult{
				tp:       event.GetString(0),
				instance: "",
				item:     name,
				actual:   "inconsistent",
				expected: "consistent",
				severity: "critical",
				detail:   fmt.Sprintf("the cluster has %[1]v different %[2]s versions, execute the allegrosql to see more detail: select * from information_schema.cluster_info where type='%[2]s'", event.GetUint64(1), event.GetString(0)),
			})
		}
	}
	return results
}

func (c nodeLoadInspection) inspect(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []ruleChecker{
		inspectCPULoad{item: "load1", tbl: "node_load1"},
		inspectCPULoad{item: "load5", tbl: "node_load5"},
		inspectCPULoad{item: "load15", tbl: "node_load15"},
		inspectVirtualMemUsage{},
		inspectSwapMemoryUsed{},
		inspectDiskUsage{},
	}
	return checkMemrules(ctx, sctx, filter, rules)
}

type inspectVirtualMemUsage struct{}

func (inspectVirtualMemUsage) genALLEGROSQL(timeRange plannercore.QueryTimeRange) string {
	allegrosql := fmt.Sprintf("select instance, max(value) as max_usage from metrics_schema.node_memory_usage %s group by instance having max_usage >= 70", timeRange.Condition())
	return allegrosql
}

func (i inspectVirtualMemUsage) genResult(allegrosql string, event chunk.Event) inspectionResult {
	return inspectionResult{
		tp:       "node",
		instance: event.GetString(0),
		item:     i.getItem(),
		actual:   fmt.Sprintf("%.1f%%", event.GetFloat64(1)),
		expected: "< 70%",
		severity: "warning",
		detail:   "the memory-usage is too high",
	}
}

func (inspectVirtualMemUsage) getItem() string {
	return "virtual-memory-usage"
}

type inspectSwapMemoryUsed struct{}

func (inspectSwapMemoryUsed) genALLEGROSQL(timeRange plannercore.QueryTimeRange) string {
	allegrosql := fmt.Sprintf("select instance, max(value) as max_used from metrics_schema.node_memory_swap_used %s group by instance having max_used > 0", timeRange.Condition())
	return allegrosql
}

func (i inspectSwapMemoryUsed) genResult(allegrosql string, event chunk.Event) inspectionResult {
	return inspectionResult{
		tp:       "node",
		instance: event.GetString(0),
		item:     i.getItem(),
		actual:   fmt.Sprintf("%.1f", event.GetFloat64(1)),
		expected: "0",
		severity: "warning",
	}
}

func (inspectSwapMemoryUsed) getItem() string {
	return "swap-memory-used"
}

type inspectDiskUsage struct{}

func (inspectDiskUsage) genALLEGROSQL(timeRange plannercore.QueryTimeRange) string {
	allegrosql := fmt.Sprintf("select instance, device, max(value) as max_usage from metrics_schema.node_disk_usage %v and device like '/%%' group by instance, device having max_usage >= 70", timeRange.Condition())
	return allegrosql
}

func (i inspectDiskUsage) genResult(allegrosql string, event chunk.Event) inspectionResult {
	return inspectionResult{
		tp:       "node",
		instance: event.GetString(0),
		item:     i.getItem(),
		actual:   fmt.Sprintf("%.1f%%", event.GetFloat64(2)),
		expected: "< 70%",
		severity: "warning",
		detail:   "the disk-usage of " + event.GetString(1) + " is too high",
	}
}

func (inspectDiskUsage) getItem() string {
	return "disk-usage"
}

type inspectCPULoad struct {
	item string
	tbl  string
}

func (i inspectCPULoad) genALLEGROSQL(timeRange plannercore.QueryTimeRange) string {
	allegrosql := fmt.Sprintf(`select t1.instance, t1.max_load , 0.7*t2.cpu_count from
			(select instance,max(value) as max_load  from metrics_schema.%[1]s %[2]s group by instance) as t1 join
			(select instance,max(value) as cpu_count from metrics_schema.node_virtual_cpus %[2]s group by instance) as t2
			on t1.instance=t2.instance where t1.max_load>(0.7*t2.cpu_count);`, i.tbl, timeRange.Condition())
	return allegrosql
}

func (i inspectCPULoad) genResult(allegrosql string, event chunk.Event) inspectionResult {
	return inspectionResult{
		tp:       "node",
		instance: event.GetString(0),
		item:     "cpu-" + i.item,
		actual:   fmt.Sprintf("%.1f", event.GetFloat64(1)),
		expected: fmt.Sprintf("< %.1f", event.GetFloat64(2)),
		severity: "warning",
		detail:   i.getItem() + " should less than (cpu_logical_cores * 0.7)",
	}
}

func (i inspectCPULoad) getItem() string {
	return "cpu-" + i.item
}

func (c criticalErrorInspection) inspect(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	results := c.inspectError(ctx, sctx, filter)
	results = append(results, c.inspectForServerDown(ctx, sctx, filter)...)
	return results
}
func (criticalErrorInspection) inspectError(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []struct {
		tp   string
		item string
		tbl  string
	}{
		{tp: "einsteindb", item: "critical-error", tbl: "einsteindb_critical_error_total_count"},
		{tp: "milevadb", item: "panic-count", tbl: "milevadb_panic_count_total_count"},
		{tp: "milevadb", item: "binlog-error", tbl: "milevadb_binlog_error_total_count"},
		{tp: "einsteindb", item: "scheduler-is-busy", tbl: "einsteindb_scheduler_is_busy_total_count"},
		{tp: "einsteindb", item: "interlock-is-busy", tbl: "einsteindb_interlocking_directorate_is_busy_total_count"},
		{tp: "einsteindb", item: "channel-is-full", tbl: "einsteindb_channel_full_total_count"},
		{tp: "einsteindb", item: "einsteindb_engine_write_stall", tbl: "einsteindb_engine_write_stall"},
	}

	condition := filter.timeRange.Condition()
	var results []inspectionResult
	for _, rule := range rules {
		if filter.enable(rule.item) {
			def, found := schemareplicant.MetricBlockMap[rule.tbl]
			if !found {
				sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("metrics block: %s not found", rule.tbl))
				continue
			}
			allegrosql := fmt.Sprintf("select `%[1]s`,sum(value) as total from `%[2]s`.`%[3]s` %[4]s group by `%[1]s` having total>=1.0",
				strings.Join(def.Labels, "`,`"), soliton.MetricSchemaName.L, rule.tbl, condition)
			rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
			if err != nil {
				sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", allegrosql, err))
				continue
			}
			for _, event := range rows {
				var actual, detail string
				var degree float64
				if rest := def.Labels[1:]; len(rest) > 0 {
					values := make([]string, 0, len(rest))
					// `i+1` and `1+len(rest)` means skip the first field `instance`
					for i := range rest {
						values = append(values, event.GetString(i+1))
					}
					// TODO: find a better way to construct the `actual` field
					actual = fmt.Sprintf("%.2f(%s)", event.GetFloat64(1+len(rest)), strings.Join(values, ", "))
					degree = event.GetFloat64(1 + len(rest))
				} else {
					actual = fmt.Sprintf("%.2f", event.GetFloat64(1))
					degree = event.GetFloat64(1)
				}
				detail = fmt.Sprintf("the total number of errors about '%s' is too many", rule.item)
				result := inspectionResult{
					tp: rule.tp,
					// NOTE: all blocks which can be inspected here whose first label must be `instance`
					statusAddress: event.GetString(0),
					item:          rule.item,
					actual:        actual,
					expected:      "0",
					severity:      "critical",
					detail:        detail,
					degree:        degree,
				}
				results = append(results, result)
			}
		}
	}
	return results
}

func (criticalErrorInspection) inspectForServerDown(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	item := "server-down"
	if !filter.enable(item) {
		return nil
	}
	condition := filter.timeRange.Condition()
	allegrosql := fmt.Sprintf(`select t1.job,t1.instance, t2.min_time from
		(select instance,job from metrics_schema.up %[1]s group by instance,job having max(value)-min(value)>0) as t1 join
		(select instance,min(time) as min_time from metrics_schema.up %[1]s and value=0 group by instance,job) as t2 on t1.instance=t2.instance order by job`, condition)
	rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
	if err != nil {
		sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", allegrosql, err))
	}
	var results []inspectionResult
	for _, event := range rows {
		if event.Len() < 3 {
			continue
		}
		detail := fmt.Sprintf("%s %s disconnect with prometheus around time '%s'", event.GetString(0), event.GetString(1), event.GetTime(2))
		result := inspectionResult{
			tp:            event.GetString(0),
			statusAddress: event.GetString(1),
			item:          item,
			actual:        "",
			expected:      "",
			severity:      "critical",
			detail:        detail,
			degree:        10000 + float64(len(results)),
		}
		results = append(results, result)
	}
	// Check from log.
	allegrosql = fmt.Sprintf("select type,instance,time from information_schema.cluster_log %s and level = 'info' and message like '%%Welcome to'", condition)
	rows, _, err = sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
	if err != nil {
		sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", allegrosql, err))
	}
	for _, event := range rows {
		if event.Len() < 3 {
			continue
		}
		detail := fmt.Sprintf("%s %s restarted at time '%s'", event.GetString(0), event.GetString(1), event.GetString(2))
		result := inspectionResult{
			tp:       event.GetString(0),
			instance: event.GetString(1),
			item:     item,
			actual:   "",
			expected: "",
			severity: "critical",
			detail:   detail,
			degree:   10000 + float64(len(results)),
		}
		results = append(results, result)
	}
	return results
}

func (c thresholdCheckInspection) inspect(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	inspects := []func(context.Context, stochastikctx.Context, inspectionFilter) []inspectionResult{
		c.inspectThreshold1,
		c.inspectThreshold2,
		c.inspectThreshold3,
		c.inspectForLeaderDrop,
	}
	var results []inspectionResult
	for _, inspect := range inspects {
		re := inspect(ctx, sctx, filter)
		results = append(results, re...)
	}
	return results
}

func (thresholdCheckInspection) inspectThreshold1(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []struct {
		item      string
		component string
		configKey string
		threshold float64
	}{
		{
			item:      "interlock-normal-cpu",
			component: "INTERLOCK_normal%",
			configKey: "readpool.interlock.normal-concurrency",
			threshold: 0.9},
		{
			item:      "interlock-high-cpu",
			component: "INTERLOCK_high%",
			configKey: "readpool.interlock.high-concurrency",
			threshold: 0.9,
		},
		{
			item:      "interlock-low-cpu",
			component: "INTERLOCK_low%",
			configKey: "readpool.interlock.low-concurrency",
			threshold: 0.9,
		},
		{
			item:      "grpc-cpu",
			component: "grpc%",
			configKey: "server.grpc-concurrency",
			threshold: 0.9,
		},
		{
			item:      "raftstore-cpu",
			component: "raftstore_%",
			configKey: "raftstore.causetstore-pool-size",
			threshold: 0.8,
		},
		{
			item:      "apply-cpu",
			component: "apply_%",
			configKey: "raftstore.apply-pool-size",
			threshold: 0.8,
		},
		{
			item:      "storage-readpool-normal-cpu",
			component: "store_read_norm%",
			configKey: "readpool.storage.normal-concurrency",
			threshold: 0.9,
		},
		{
			item:      "storage-readpool-high-cpu",
			component: "store_read_high%",
			configKey: "readpool.storage.high-concurrency",
			threshold: 0.9,
		},
		{
			item:      "storage-readpool-low-cpu",
			component: "store_read_low%",
			configKey: "readpool.storage.low-concurrency",
			threshold: 0.9,
		},
		{
			item:      "scheduler-worker-cpu",
			component: "sched_%",
			configKey: "storage.scheduler-worker-pool-size",
			threshold: 0.85,
		},
		{
			item:      "split-check-cpu",
			component: "split_check",
			threshold: 0.9,
		},
	}

	condition := filter.timeRange.Condition()
	var results []inspectionResult
	for _, rule := range rules {
		if !filter.enable(rule.item) {
			continue
		}

		var allegrosql string
		if len(rule.configKey) > 0 {
			allegrosql = fmt.Sprintf("select t1.status_address, t1.cpu, (t2.value * %[2]f) as threshold, t2.value from "+
				"(select status_address, max(sum_value) as cpu from (select instance as status_address, sum(value) as sum_value from metrics_schema.einsteindb_thread_cpu %[4]s and name like '%[1]s' group by instance, time) as tmp group by tmp.status_address) as t1 join "+
				"(select instance, value from information_schema.cluster_config where type='einsteindb' and `key` = '%[3]s') as t2 join "+
				"(select instance,status_address from information_schema.cluster_info where type='einsteindb') as t3 "+
				"on t1.status_address=t3.status_address and t2.instance=t3.instance where t1.cpu > (t2.value * %[2]f)", rule.component, rule.threshold, rule.configKey, condition)
		} else {
			allegrosql = fmt.Sprintf("select t1.instance, t1.cpu, %[2]f from "+
				"(select instance, max(value) as cpu from metrics_schema.einsteindb_thread_cpu %[3]s and name like '%[1]s' group by instance) as t1 "+
				"where t1.cpu > %[2]f;", rule.component, rule.threshold, condition)
		}
		rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
		if err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", allegrosql, err))
			continue
		}
		for _, event := range rows {
			actual := fmt.Sprintf("%.2f", event.GetFloat64(1))
			degree := math.Abs(event.GetFloat64(1)-event.GetFloat64(2)) / math.Max(event.GetFloat64(1), event.GetFloat64(2))
			expected := ""
			if len(rule.configKey) > 0 {
				expected = fmt.Sprintf("< %.2f, config: %v=%v", event.GetFloat64(2), rule.configKey, event.GetString(3))
			} else {
				expected = fmt.Sprintf("< %.2f", event.GetFloat64(2))
			}
			detail := fmt.Sprintf("the '%s' max cpu-usage of %s einsteindb is too high", rule.item, event.GetString(0))
			result := inspectionResult{
				tp:            "einsteindb",
				statusAddress: event.GetString(0),
				item:          rule.item,
				actual:        actual,
				expected:      expected,
				severity:      "warning",
				detail:        detail,
				degree:        degree,
			}
			results = append(results, result)
		}
	}
	return results
}

func (thresholdCheckInspection) inspectThreshold2(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []struct {
		tp        string
		item      string
		tbl       string
		condition string
		threshold float64
		factor    float64
		isMin     bool
		detail    string
	}{
		{
			tp:        "milevadb",
			item:      "tso-duration",
			tbl:       "FIDel_tso_wait_duration",
			condition: "quantile=0.999",
			threshold: 0.05,
		},
		{
			tp:        "milevadb",
			item:      "get-token-duration",
			tbl:       "milevadb_get_token_duration",
			condition: "quantile=0.999",
			threshold: 0.001,
			factor:    10e5, // the unit is microsecond
		},
		{
			tp:        "milevadb",
			item:      "load-schemaReplicant-duration",
			tbl:       "milevadb_load_schema_duration",
			condition: "quantile=0.99",
			threshold: 1,
		},
		{
			tp:        "einsteindb",
			item:      "scheduler-cmd-duration",
			tbl:       "einsteindb_scheduler_command_duration",
			condition: "quantile=0.99",
			threshold: 0.1,
		},
		{
			tp:        "einsteindb",
			item:      "handle-snapshot-duration",
			tbl:       "einsteindb_handle_snapshot_duration",
			threshold: 30,
		},
		{
			tp:        "einsteindb",
			item:      "storage-write-duration",
			tbl:       "einsteindb_storage_async_request_duration",
			condition: "type='write'",
			threshold: 0.1,
		},
		{
			tp:        "einsteindb",
			item:      "storage-snapshot-duration",
			tbl:       "einsteindb_storage_async_request_duration",
			condition: "type='snapshot'",
			threshold: 0.05,
		},
		{
			tp:        "einsteindb",
			item:      "lmdb-write-duration",
			tbl:       "einsteindb_engine_write_duration",
			condition: "type='write_max'",
			threshold: 0.1,
			factor:    10e5, // the unit is microsecond
		},
		{
			tp:        "einsteindb",
			item:      "lmdb-get-duration",
			tbl:       "einsteindb_engine_max_get_duration",
			condition: "type='get_max'",
			threshold: 0.05,
			factor:    10e5,
		},
		{
			tp:        "einsteindb",
			item:      "lmdb-seek-duration",
			tbl:       "einsteindb_engine_max_seek_duration",
			condition: "type='seek_max'",
			threshold: 0.05,
			factor:    10e5, // the unit is microsecond
		},
		{
			tp:        "einsteindb",
			item:      "scheduler-pending-cmd-count",
			tbl:       "einsteindb_scheduler_pending_commands",
			threshold: 1000,
			detail:    " %s einsteindb scheduler has too many pending commands",
		},
		{
			tp:        "einsteindb",
			item:      "index-block-cache-hit",
			tbl:       "einsteindb_block_index_cache_hit",
			condition: "value > 0",
			threshold: 0.95,
			isMin:     true,
		},
		{
			tp:        "einsteindb",
			item:      "filter-block-cache-hit",
			tbl:       "einsteindb_block_filter_cache_hit",
			condition: "value > 0",
			threshold: 0.95,
			isMin:     true,
		},
		{
			tp:        "einsteindb",
			item:      "data-block-cache-hit",
			tbl:       "einsteindb_block_data_cache_hit",
			condition: "value > 0",
			threshold: 0.80,
			isMin:     true,
		},
	}

	condition := filter.timeRange.Condition()
	var results []inspectionResult
	for _, rule := range rules {
		if !filter.enable(rule.item) {
			continue
		}
		var allegrosql string
		cond := condition
		if len(rule.condition) > 0 {
			cond = fmt.Sprintf("%s and %s", cond, rule.condition)
		}
		if rule.factor == 0 {
			rule.factor = 1
		}
		if rule.isMin {
			allegrosql = fmt.Sprintf("select instance, min(value)/%.0f as min_value from metrics_schema.%s %s group by instance having min_value < %f;", rule.factor, rule.tbl, cond, rule.threshold)
		} else {
			allegrosql = fmt.Sprintf("select instance, max(value)/%.0f as max_value from metrics_schema.%s %s group by instance having max_value > %f;", rule.factor, rule.tbl, cond, rule.threshold)
		}
		rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
		if err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", allegrosql, err))
			continue
		}
		for _, event := range rows {
			actual := fmt.Sprintf("%.3f", event.GetFloat64(1))
			degree := math.Abs(event.GetFloat64(1)-rule.threshold) / math.Max(event.GetFloat64(1), rule.threshold)
			expected := ""
			if rule.isMin {
				expected = fmt.Sprintf("> %.3f", rule.threshold)
			} else {
				expected = fmt.Sprintf("< %.3f", rule.threshold)
			}
			detail := rule.detail
			if len(detail) == 0 {
				if strings.HasSuffix(rule.item, "duration") {
					detail = fmt.Sprintf("max duration of %s %s %s is too slow", event.GetString(0), rule.tp, rule.item)
				} else if strings.HasSuffix(rule.item, "hit") {
					detail = fmt.Sprintf("min %s rate of %s %s is too low", rule.item, event.GetString(0), rule.tp)
				}
			} else {
				detail = fmt.Sprintf(detail, event.GetString(0))
			}
			result := inspectionResult{
				tp:            rule.tp,
				statusAddress: event.GetString(0),
				item:          rule.item,
				actual:        actual,
				expected:      expected,
				severity:      "warning",
				detail:        detail,
				degree:        degree,
			}
			results = append(results, result)
		}
	}
	return results
}

type ruleChecker interface {
	genALLEGROSQL(timeRange plannercore.QueryTimeRange) string
	genResult(allegrosql string, event chunk.Event) inspectionResult
	getItem() string
}

type compareStoreStatus struct {
	item      string
	tp        string
	threshold float64
}

func (c compareStoreStatus) genALLEGROSQL(timeRange plannercore.QueryTimeRange) string {
	condition := fmt.Sprintf(`where t1.time>='%[1]s' and t1.time<='%[2]s' and
		 t2.time>='%[1]s' and t2.time<='%[2]s'`, timeRange.From.Format(plannercore.MetricBlockTimeFormat),
		timeRange.To.Format(plannercore.MetricBlockTimeFormat))
	return fmt.Sprintf(`
		SELECT t1.address,
        	max(t1.value),
        	t2.address,
        	min(t2.value),
         	max((t1.value-t2.value)/t1.value) AS ratio
		FROM metrics_schema.FIDel_scheduler_store_status t1
		JOIN metrics_schema.FIDel_scheduler_store_status t2 %s
        	AND t1.type='%s'
        	AND t1.time = t2.time
        	AND t1.type=t2.type
        	AND t1.address != t2.address
        	AND (t1.value-t2.value)/t1.value>%v
        	AND t1.value > 0
		GROUP BY  t1.address,t2.address
		ORDER BY  ratio desc`, condition, c.tp, c.threshold)
}

func (c compareStoreStatus) genResult(_ string, event chunk.Event) inspectionResult {
	addr1 := event.GetString(0)
	value1 := event.GetFloat64(1)
	addr2 := event.GetString(2)
	value2 := event.GetFloat64(3)
	ratio := event.GetFloat64(4)
	detail := fmt.Sprintf("%v max %s is %.2f, much more than %v min %s %.2f", addr1, c.tp, value1, addr2, c.tp, value2)
	return inspectionResult{
		tp:       "einsteindb",
		instance: addr2,
		item:     c.item,
		actual:   fmt.Sprintf("%.2f%%", ratio*100),
		expected: fmt.Sprintf("< %.2f%%", c.threshold*100),
		severity: "warning",
		detail:   detail,
		degree:   ratio,
	}
}

func (c compareStoreStatus) getItem() string {
	return c.item
}

type checkRegionHealth struct{}

func (c checkRegionHealth) genALLEGROSQL(timeRange plannercore.QueryTimeRange) string {
	condition := timeRange.Condition()
	return fmt.Sprintf(`select instance, sum(value) as sum_value from metrics_schema.FIDel_region_health %s and
		type in ('extra-peer-region-count','learner-peer-region-count','pending-peer-region-count') having sum_value>100`, condition)
}

func (c checkRegionHealth) genResult(_ string, event chunk.Event) inspectionResult {
	detail := fmt.Sprintf("the count of extra-perr and learner-peer and pending-peer are %v, it means the scheduling is too frequent or too slow", event.GetFloat64(1))
	actual := fmt.Sprintf("%.2f", event.GetFloat64(1))
	degree := math.Abs(event.GetFloat64(1)-100) / math.Max(event.GetFloat64(1), 100)
	return inspectionResult{
		tp:       "fidel",
		instance: event.GetString(0),
		item:     c.getItem(),
		actual:   actual,
		expected: "< 100",
		severity: "warning",
		detail:   detail,
		degree:   degree,
	}
}

func (c checkRegionHealth) getItem() string {
	return "region-health"
}

type checkStoreRegionTooMuch struct{}

func (c checkStoreRegionTooMuch) genALLEGROSQL(timeRange plannercore.QueryTimeRange) string {
	condition := timeRange.Condition()
	return fmt.Sprintf(`select address, max(value) from metrics_schema.FIDel_scheduler_store_status %s and type='region_count' and value > 20000 group by address`, condition)
}

func (c checkStoreRegionTooMuch) genResult(allegrosql string, event chunk.Event) inspectionResult {
	actual := fmt.Sprintf("%.2f", event.GetFloat64(1))
	degree := math.Abs(event.GetFloat64(1)-20000) / math.Max(event.GetFloat64(1), 20000)
	return inspectionResult{
		tp:       "einsteindb",
		instance: event.GetString(0),
		item:     c.getItem(),
		actual:   actual,
		expected: "<= 20000",
		severity: "warning",
		detail:   fmt.Sprintf("%s einsteindb has too many regions", event.GetString(0)),
		degree:   degree,
	}
}

func (c checkStoreRegionTooMuch) getItem() string {
	return "region-count"
}

func (thresholdCheckInspection) inspectThreshold3(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	var rules = []ruleChecker{
		compareStoreStatus{
			item:      "leader-score-balance",
			tp:        "leader_score",
			threshold: 0.05,
		},
		compareStoreStatus{
			item:      "region-score-balance",
			tp:        "region_score",
			threshold: 0.05,
		},
		compareStoreStatus{
			item:      "causetstore-available-balance",
			tp:        "store_available",
			threshold: 0.2,
		},
		checkRegionHealth{},
		checkStoreRegionTooMuch{},
	}
	return checkMemrules(ctx, sctx, filter, rules)
}

func checkMemrules(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter, rules []ruleChecker) []inspectionResult {
	var results []inspectionResult
	for _, rule := range rules {
		if !filter.enable(rule.getItem()) {
			continue
		}
		allegrosql := rule.genALLEGROSQL(filter.timeRange)
		rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
		if err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", allegrosql, err))
			continue
		}
		for _, event := range rows {
			results = append(results, rule.genResult(allegrosql, event))
		}
	}
	return results
}

func (c thresholdCheckInspection) inspectForLeaderDrop(ctx context.Context, sctx stochastikctx.Context, filter inspectionFilter) []inspectionResult {
	condition := filter.timeRange.Condition()
	threshold := 50.0
	allegrosql := fmt.Sprintf(`select address,min(value) as mi,max(value) as mx from metrics_schema.FIDel_scheduler_store_status %s and type='leader_count' group by address having mx-mi>%v`, condition, threshold)
	rows, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
	if err != nil {
		sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", allegrosql, err))
		return nil
	}
	var results []inspectionResult
	for _, event := range rows {
		address := event.GetString(0)
		allegrosql := fmt.Sprintf(`select time, value from metrics_schema.FIDel_scheduler_store_status %s and type='leader_count' and address = '%s' order by time`, condition, address)
		subEvents, _, err := sctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithContext(ctx, allegrosql)
		if err != nil {
			sctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(fmt.Errorf("execute '%s' failed: %v", allegrosql, err))
			continue
		}
		lastValue := float64(0)
		for i, subEvents := range subEvents {
			v := subEvents.GetFloat64(1)
			if i == 0 {
				lastValue = v
				continue
			}
			if lastValue-v > threshold {
				level := "warning"
				if v == 0 {
					level = "critical"
				}
				results = append(results, inspectionResult{
					tp:       "einsteindb",
					instance: address,
					item:     "leader-drop",
					actual:   fmt.Sprintf("%.0f", lastValue-v),
					expected: fmt.Sprintf("<= %.0f", threshold),
					severity: level,
					detail:   fmt.Sprintf("%s einsteindb has too many leader-drop around time %s, leader count from %.0f drop to %.0f", address, subEvents.GetTime(0), lastValue, v),
					degree:   lastValue - v,
				})
				break
			}
			lastValue = v
		}
	}
	return results
}
