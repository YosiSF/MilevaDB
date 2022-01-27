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

package executor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

//slowQueryRetriever is used to read slow log data.
type slowQueryRetriever struct {
	block       *perceptron.BlockInfo
	outputDefCauss  []*perceptron.DeferredCausetInfo
	initialized bool
	extractor   *plannercore.SlowQueryExtractor
	files       []logFile
	fileIdx     int
	fileLine    int
	checker     *slowLogChecker

	parsedSlowLogCh chan parsedSlowLog
}

func (e *slowQueryRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if !e.initialized {
		err := e.initialize(sctx)
		if err != nil {
			return nil, err
		}
		e.initializeAsyncParsing(ctx, sctx)
	}
	rows, retrieved, err := e.dataForSlowLog(ctx)
	if err != nil {
		return nil, err
	}
	if retrieved {
		return nil, nil
	}
	if len(e.outputDefCauss) == len(e.block.DeferredCausets) {
		return rows, nil
	}
	retEvents := make([][]types.Causet, len(rows))
	for i, fullEvent := range rows {
		event := make([]types.Causet, len(e.outputDefCauss))
		for j, defCaus := range e.outputDefCauss {
			event[j] = fullEvent[defCaus.Offset]
		}
		retEvents[i] = event
	}
	return retEvents, nil
}

func (e *slowQueryRetriever) initialize(sctx stochastikctx.Context) error {
	var err error
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		hasProcessPriv = pm.RequestVerification(sctx.GetStochastikVars().ActiveRoles, "", "", "", allegrosql.ProcessPriv)
	}
	e.checker = &slowLogChecker{
		hasProcessPriv: hasProcessPriv,
		user:           sctx.GetStochastikVars().User,
	}
	if e.extractor != nil {
		e.checker.enableTimeCheck = e.extractor.Enable
		e.checker.startTime = types.NewTime(types.FromGoTime(e.extractor.StartTime), allegrosql.TypeDatetime, types.MaxFsp)
		e.checker.endTime = types.NewTime(types.FromGoTime(e.extractor.EndTime), allegrosql.TypeDatetime, types.MaxFsp)
	}
	e.initialized = true
	e.files, err = e.getAllFiles(sctx, sctx.GetStochastikVars().SlowQueryFile)
	return err
}

func (e *slowQueryRetriever) close() error {
	for _, f := range e.files {
		err := f.file.Close()
		if err != nil {
			logutil.BgLogger().Error("close slow log file failed.", zap.Error(err))
		}
	}
	return nil
}

type parsedSlowLog struct {
	rows [][]types.Causet
	err  error
}

func (e *slowQueryRetriever) parseDataForSlowLog(ctx context.Context, sctx stochastikctx.Context) {
	if len(e.files) == 0 {
		close(e.parsedSlowLogCh)
		return
	}
	reader := bufio.NewReader(e.files[0].file)
	e.parseSlowLog(ctx, sctx, reader, 64)
	close(e.parsedSlowLogCh)
}

func (e *slowQueryRetriever) dataForSlowLog(ctx context.Context) ([][]types.Causet, bool, error) {
	var (
		slowLog parsedSlowLog
		ok      bool
	)
	for {
		select {
		case slowLog, ok = <-e.parsedSlowLogCh:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
		if !ok {
			return nil, true, nil
		}
		rows, err := slowLog.rows, slowLog.err
		if err != nil {
			return nil, false, err
		}
		if len(rows) == 0 {
			continue
		}
		if e.block.Name.L == strings.ToLower(schemareplicant.ClusterBlockSlowLog) {
			rows, err := schemareplicant.AppendHostInfoToEvents(rows)
			return rows, false, err
		}
		return rows, false, nil
	}
}

type slowLogChecker struct {
	// Below fields is used to check privilege.
	hasProcessPriv bool
	user           *auth.UserIdentity
	// Below fields is used to check slow log time valid.
	enableTimeCheck bool
	startTime       types.Time
	endTime         types.Time
}

func (sc *slowLogChecker) hasPrivilege(userName string) bool {
	return sc.hasProcessPriv || sc.user == nil || userName == sc.user.Username
}

func (sc *slowLogChecker) isTimeValid(t types.Time) bool {
	if sc.enableTimeCheck && (t.Compare(sc.startTime) < 0 || t.Compare(sc.endTime) > 0) {
		return false
	}
	return true
}

func getOneLine(reader *bufio.Reader) ([]byte, error) {
	var resByte []byte
	lineByte, isPrefix, err := reader.ReadLine()
	if isPrefix {
		// Need to read more data.
		resByte = make([]byte, len(lineByte), len(lineByte)*2)
	} else {
		resByte = make([]byte, len(lineByte))
	}
	// Use INTERLOCKy here to avoid shallow INTERLOCKy problem.
	INTERLOCKy(resByte, lineByte)
	if err != nil {
		return resByte, err
	}

	var tempLine []byte
	for isPrefix {
		tempLine, isPrefix, err = reader.ReadLine()
		resByte = append(resByte, tempLine...)
		// Use the max value of max_allowed_packet to check the single line length.
		if len(resByte) > int(variable.MaxOfMaxAllowedPacket) {
			return resByte, errors.Errorf("single line length exceeds limit: %v", variable.MaxOfMaxAllowedPacket)
		}
		if err != nil {
			return resByte, err
		}
	}
	return resByte, err
}

type offset struct {
	offset int
	length int
}

func (e *slowQueryRetriever) getBatchLog(reader *bufio.Reader, offset *offset, num int) ([]string, error) {
	var line string
	log := make([]string, 0, num)
	var err error
	for i := 0; i < num; i++ {
		for {
			e.fileLine++
			lineByte, err := getOneLine(reader)
			if err != nil {
				if err == io.EOF {
					e.fileIdx++
					e.fileLine = 0
					if e.fileIdx >= len(e.files) {
						return log, nil
					}
					offset.length = len(log)
					reader.Reset(e.files[e.fileIdx].file)
					continue
				}
				return log, err
			}
			line = string(replog.String(lineByte))
			log = append(log, line)
			if strings.HasSuffix(line, variable.SlowLogALLEGROSQLSuffixStr) {
				if strings.HasPrefix(line, "use") {
					continue
				}
				break
			}
		}
	}
	return log, err
}

func (e *slowQueryRetriever) parseSlowLog(ctx context.Context, sctx stochastikctx.Context, reader *bufio.Reader, logNum int) {
	var wg sync.WaitGroup
	offset := offset{offset: 0, length: 0}
	// To limit the num of go routine
	ch := make(chan int, sctx.GetStochastikVars().Concurrency.DistALLEGROSQLScanConcurrency())
	defer close(ch)
	for {
		log, err := e.getBatchLog(reader, &offset, logNum)
		if err != nil {
			e.parsedSlowLogCh <- parsedSlowLog{nil, err}
			break
		}
		start := offset
		wg.Add(1)
		ch <- 1
		go func() {
			defer wg.Done()
			result, err := e.parseLog(sctx, log, start)
			if err != nil {
				e.parsedSlowLogCh <- parsedSlowLog{nil, err}
			} else {
				e.parsedSlowLogCh <- parsedSlowLog{result, err}
			}
			<-ch
		}()
		// Read the next file, offset = 0
		if e.fileIdx >= len(e.files) {
			break
		}
		offset.offset = e.fileLine
		offset.length = 0
		select {
		case <-ctx.Done():
			break
		default:
		}
	}
	wg.Wait()
}

func getLineIndex(offset offset, index int) int {
	var fileLine int
	if offset.length <= index {
		fileLine = index - offset.length + 1
	} else {
		fileLine = offset.offset + index + 1
	}
	return fileLine
}

func (e *slowQueryRetriever) parseLog(ctx stochastikctx.Context, log []string, offset offset) (data [][]types.Causet, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
		}
	}()
	failpoint.Inject("errorMockParseSlowLogPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})
	var st *slowQueryTuple
	tz := ctx.GetStochastikVars().Location()
	startFlag := false
	for index, line := range log {
		fileLine := getLineIndex(offset, index)
		if !startFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			st = &slowQueryTuple{}
			valid, err := st.setFieldValue(tz, variable.SlowLogTimeStr, line[len(variable.SlowLogStartPrefixStr):], fileLine, e.checker)
			if err != nil {
				ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
				continue
			}
			if valid {
				startFlag = true
			}
			continue
		}
		if startFlag {
			if strings.HasPrefix(line, variable.SlowLogEventPrefixStr) {
				line = line[len(variable.SlowLogEventPrefixStr):]
				if strings.HasPrefix(line, variable.SlowLogPrevStmtPrefix) {
					st.prevStmt = line[len(variable.SlowLogPrevStmtPrefix):]
				} else if strings.HasPrefix(line, variable.SlowLogUserAndHostStr+variable.SlowLogSpaceMarkStr) {
					value := line[len(variable.SlowLogUserAndHostStr+variable.SlowLogSpaceMarkStr):]
					valid, err := st.setFieldValue(tz, variable.SlowLogUserAndHostStr, value, fileLine, e.checker)
					if err != nil {
						ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
						continue
					}
					if !valid {
						startFlag = false
					}
				} else {
					fieldValues := strings.Split(line, " ")
					for i := 0; i < len(fieldValues)-1; i += 2 {
						field := fieldValues[i]
						if strings.HasSuffix(field, ":") {
							field = field[:len(field)-1]
						}
						valid, err := st.setFieldValue(tz, field, fieldValues[i+1], fileLine, e.checker)
						if err != nil {
							ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
							continue
						}
						if !valid {
							startFlag = false
						}
					}
				}
			} else if strings.HasSuffix(line, variable.SlowLogALLEGROSQLSuffixStr) {
				if strings.HasPrefix(line, "use") {
					// `use EDB` statements in the slow log is used to keep it be compatible with MyALLEGROSQL,
					// since we already get the current EDB from the `# EDB` field, we can ignore it here,
					// please see https://github.com/whtcorpsinc/milevadb/issues/17846 for more details.
					continue
				}
				// Get the allegrosql string, and mark the start flag to false.
				_, err := st.setFieldValue(tz, variable.SlowLogQueryALLEGROSQLStr, string(replog.Slice(line)), fileLine, e.checker)
				if err != nil {
					ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
					continue
				}
				if e.checker.hasPrivilege(st.user) {
					data = append(data, st.convertToCausetEvent())
				}
				startFlag = false
			} else {
				startFlag = false
			}
		}
	}
	return data, nil
}

type slowQueryTuple struct {
	time                   types.Time
	txnStartTs             uint64
	user                   string
	host                   string
	connID                 uint64
	execRetryCount         uint64
	execRetryTime          float64
	queryTime              float64
	parseTime              float64
	compileTime            float64
	rewriteTime            float64
	preprocSubqueries      uint64
	preprocSubQueryTime    float64
	optimizeTime           float64
	waitTSTime             float64
	preWriteTime           float64
	waitPrewriteBinlogTime float64
	commitTime             float64
	getCommitTSTime        float64
	commitBackoffTime      float64
	backoffTypes           string
	resolveLockTime        float64
	localLatchWaitTime     float64
	writeKeys              uint64
	writeSize              uint64
	prewriteRegion         uint64
	txnRetry               uint64
	INTERLOCKTime                float64
	processTime            float64
	waitTime               float64
	backOffTime            float64
	lockKeysTime           float64
	requestCount           uint64
	totalKeys              uint64
	processKeys            uint64
	EDB                     string
	indexIDs               string
	digest                 string
	statsInfo              string
	avgProcessTime         float64
	p90ProcessTime         float64
	maxProcessTime         float64
	maxProcessAddress      string
	avgWaitTime            float64
	p90WaitTime            float64
	maxWaitTime            float64
	maxWaitAddress         string
	memMax                 int64
	diskMax                int64
	prevStmt               string
	allegrosql                    string
	isInternal             bool
	succ                   bool
	planFromCache          bool
	plan                   string
	planDigest             string
}

func (st *slowQueryTuple) setFieldValue(tz *time.Location, field, value string, lineNum int, checker *slowLogChecker) (valid bool, err error) {
	valid = true
	switch field {
	case variable.SlowLogTimeStr:
		var t time.Time
		t, err = ParseTime(value)
		if err != nil {
			break
		}
		st.time = types.NewTime(types.FromGoTime(t), allegrosql.TypeTimestamp, types.MaxFsp)
		if checker != nil {
			valid = checker.isTimeValid(st.time)
		}
		if t.Location() != tz {
			t = t.In(tz)
			st.time = types.NewTime(types.FromGoTime(t), allegrosql.TypeTimestamp, types.MaxFsp)
		}
	case variable.SlowLogTxnStartTSStr:
		st.txnStartTs, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogUserStr:
		// the old User format is kept for compatibility
		fields := strings.SplitN(value, "@", 2)
		if len(field) > 0 {
			st.user = fields[0]
		}
		if len(field) > 1 {
			st.host = fields[1]
		}
		if checker != nil {
			valid = checker.hasPrivilege(st.user)
		}
	case variable.SlowLogUserAndHostStr:
		// the new User&Host format: root[root] @ localhost [127.0.0.1]
		fields := strings.SplitN(value, "@", 2)
		if len(fields) > 0 {
			tmp := strings.Split(fields[0], "[")
			st.user = strings.TrimSpace(tmp[0])
		}
		if len(fields) > 1 {
			tmp := strings.Split(fields[1], "[")
			st.host = strings.TrimSpace(tmp[0])
		}
		if checker != nil {
			valid = checker.hasPrivilege(st.user)
		}
	case variable.SlowLogConnIDStr:
		st.connID, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogExecRetryCount:
		st.execRetryCount, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogExecRetryTime:
		st.execRetryTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogQueryTimeStr:
		st.queryTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogParseTimeStr:
		st.parseTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogCompileTimeStr:
		st.compileTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogOptimizeTimeStr:
		st.optimizeTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogWaitTSTimeStr:
		st.waitTSTime, err = strconv.ParseFloat(value, 64)
	case execdetails.PreWriteTimeStr:
		st.preWriteTime, err = strconv.ParseFloat(value, 64)
	case execdetails.WaitPrewriteBinlogTimeStr:
		st.waitPrewriteBinlogTime, err = strconv.ParseFloat(value, 64)
	case execdetails.CommitTimeStr:
		st.commitTime, err = strconv.ParseFloat(value, 64)
	case execdetails.GetCommitTSTimeStr:
		st.getCommitTSTime, err = strconv.ParseFloat(value, 64)
	case execdetails.CommitBackoffTimeStr:
		st.commitBackoffTime, err = strconv.ParseFloat(value, 64)
	case execdetails.BackoffTypesStr:
		st.backoffTypes = value
	case execdetails.ResolveLockTimeStr:
		st.resolveLockTime, err = strconv.ParseFloat(value, 64)
	case execdetails.LocalLatchWaitTimeStr:
		st.localLatchWaitTime, err = strconv.ParseFloat(value, 64)
	case execdetails.WriteKeysStr:
		st.writeKeys, err = strconv.ParseUint(value, 10, 64)
	case execdetails.WriteSizeStr:
		st.writeSize, err = strconv.ParseUint(value, 10, 64)
	case execdetails.PrewriteRegionStr:
		st.prewriteRegion, err = strconv.ParseUint(value, 10, 64)
	case execdetails.TxnRetryStr:
		st.txnRetry, err = strconv.ParseUint(value, 10, 64)
	case execdetails.INTERLOCKTimeStr:
		st.INTERLOCKTime, err = strconv.ParseFloat(value, 64)
	case execdetails.ProcessTimeStr:
		st.processTime, err = strconv.ParseFloat(value, 64)
	case execdetails.WaitTimeStr:
		st.waitTime, err = strconv.ParseFloat(value, 64)
	case execdetails.BackoffTimeStr:
		st.backOffTime, err = strconv.ParseFloat(value, 64)
	case execdetails.LockKeysTimeStr:
		st.lockKeysTime, err = strconv.ParseFloat(value, 64)
	case execdetails.RequestCountStr:
		st.requestCount, err = strconv.ParseUint(value, 10, 64)
	case execdetails.TotalKeysStr:
		st.totalKeys, err = strconv.ParseUint(value, 10, 64)
	case execdetails.ProcessKeysStr:
		st.processKeys, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogDBStr:
		st.EDB = value
	case variable.SlowLogIndexNamesStr:
		st.indexIDs = value
	case variable.SlowLogIsInternalStr:
		st.isInternal = value == "true"
	case variable.SlowLogDigestStr:
		st.digest = value
	case variable.SlowLogStatsInfoStr:
		st.statsInfo = value
	case variable.SlowLogINTERLOCKProcAvg:
		st.avgProcessTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogINTERLOCKProcP90:
		st.p90ProcessTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogINTERLOCKProcMax:
		st.maxProcessTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogINTERLOCKProcAddr:
		st.maxProcessAddress = value
	case variable.SlowLogINTERLOCKWaitAvg:
		st.avgWaitTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogINTERLOCKWaitP90:
		st.p90WaitTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogINTERLOCKWaitMax:
		st.maxWaitTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogINTERLOCKWaitAddr:
		st.maxWaitAddress = value
	case variable.SlowLogMemMax:
		st.memMax, err = strconv.ParseInt(value, 10, 64)
	case variable.SlowLogSucc:
		st.succ, err = strconv.ParseBool(value)
	case variable.SlowLogPlanFromCache:
		st.planFromCache, err = strconv.ParseBool(value)
	case variable.SlowLogPlan:
		st.plan = value
	case variable.SlowLogPlanDigest:
		st.planDigest = value
	case variable.SlowLogQueryALLEGROSQLStr:
		st.allegrosql = value
	case variable.SlowLogDiskMax:
		st.diskMax, err = strconv.ParseInt(value, 10, 64)
	case variable.SlowLogRewriteTimeStr:
		st.rewriteTime, err = strconv.ParseFloat(value, 64)
	case variable.SlowLogPreprocSubQueriesStr:
		st.preprocSubqueries, err = strconv.ParseUint(value, 10, 64)
	case variable.SlowLogPreProcSubQueryTimeStr:
		st.preprocSubQueryTime, err = strconv.ParseFloat(value, 64)
	}
	if err != nil {
		return valid, fmt.Errorf("Parse slow log at line " + strconv.FormatInt(int64(lineNum), 10) + " failed. Field: `" + field + "`, error: " + err.Error())
	}
	return valid, err
}

func (st *slowQueryTuple) convertToCausetEvent() []types.Causet {
	// Build the slow query result
	record := make([]types.Causet, 0, 64)
	record = append(record, types.NewTimeCauset(st.time))
	record = append(record, types.NewUintCauset(st.txnStartTs))
	record = append(record, types.NewStringCauset(st.user))
	record = append(record, types.NewStringCauset(st.host))
	record = append(record, types.NewUintCauset(st.connID))
	record = append(record, types.NewUintCauset(st.execRetryCount))
	record = append(record, types.NewFloat64Causet(st.execRetryTime))
	record = append(record, types.NewFloat64Causet(st.queryTime))
	record = append(record, types.NewFloat64Causet(st.parseTime))
	record = append(record, types.NewFloat64Causet(st.compileTime))
	record = append(record, types.NewFloat64Causet(st.rewriteTime))
	record = append(record, types.NewUintCauset(st.preprocSubqueries))
	record = append(record, types.NewFloat64Causet(st.preprocSubQueryTime))
	record = append(record, types.NewFloat64Causet(st.optimizeTime))
	record = append(record, types.NewFloat64Causet(st.waitTSTime))
	record = append(record, types.NewFloat64Causet(st.preWriteTime))
	record = append(record, types.NewFloat64Causet(st.waitPrewriteBinlogTime))
	record = append(record, types.NewFloat64Causet(st.commitTime))
	record = append(record, types.NewFloat64Causet(st.getCommitTSTime))
	record = append(record, types.NewFloat64Causet(st.commitBackoffTime))
	record = append(record, types.NewStringCauset(st.backoffTypes))
	record = append(record, types.NewFloat64Causet(st.resolveLockTime))
	record = append(record, types.NewFloat64Causet(st.localLatchWaitTime))
	record = append(record, types.NewUintCauset(st.writeKeys))
	record = append(record, types.NewUintCauset(st.writeSize))
	record = append(record, types.NewUintCauset(st.prewriteRegion))
	record = append(record, types.NewUintCauset(st.txnRetry))
	record = append(record, types.NewFloat64Causet(st.INTERLOCKTime))
	record = append(record, types.NewFloat64Causet(st.processTime))
	record = append(record, types.NewFloat64Causet(st.waitTime))
	record = append(record, types.NewFloat64Causet(st.backOffTime))
	record = append(record, types.NewFloat64Causet(st.lockKeysTime))
	record = append(record, types.NewUintCauset(st.requestCount))
	record = append(record, types.NewUintCauset(st.totalKeys))
	record = append(record, types.NewUintCauset(st.processKeys))
	record = append(record, types.NewStringCauset(st.EDB))
	record = append(record, types.NewStringCauset(st.indexIDs))
	record = append(record, types.NewCauset(st.isInternal))
	record = append(record, types.NewStringCauset(st.digest))
	record = append(record, types.NewStringCauset(st.statsInfo))
	record = append(record, types.NewFloat64Causet(st.avgProcessTime))
	record = append(record, types.NewFloat64Causet(st.p90ProcessTime))
	record = append(record, types.NewFloat64Causet(st.maxProcessTime))
	record = append(record, types.NewStringCauset(st.maxProcessAddress))
	record = append(record, types.NewFloat64Causet(st.avgWaitTime))
	record = append(record, types.NewFloat64Causet(st.p90WaitTime))
	record = append(record, types.NewFloat64Causet(st.maxWaitTime))
	record = append(record, types.NewStringCauset(st.maxWaitAddress))
	record = append(record, types.NewIntCauset(st.memMax))
	record = append(record, types.NewIntCauset(st.diskMax))
	if st.succ {
		record = append(record, types.NewIntCauset(1))
	} else {
		record = append(record, types.NewIntCauset(0))
	}
	if st.planFromCache {
		record = append(record, types.NewIntCauset(1))
	} else {
		record = append(record, types.NewIntCauset(0))
	}
	record = append(record, types.NewStringCauset(parsePlan(st.plan)))
	record = append(record, types.NewStringCauset(st.planDigest))
	record = append(record, types.NewStringCauset(st.prevStmt))
	record = append(record, types.NewStringCauset(st.allegrosql))
	return record
}

func parsePlan(planString string) string {
	if len(planString) <= len(variable.SlowLogPlanPrefix)+len(variable.SlowLogPlanSuffix) {
		return planString
	}
	planString = planString[len(variable.SlowLogPlanPrefix) : len(planString)-len(variable.SlowLogPlanSuffix)]
	decodePlanString, err := plancodec.DecodePlan(planString)
	if err == nil {
		planString = decodePlanString
	} else {
		logutil.BgLogger().Error("decode plan in slow log failed", zap.String("plan", planString), zap.Error(err))
	}
	return planString
}

// ParseTime exports for testing.
func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse(logutil.SlowLogTimeFormat, s)
	if err != nil {
		// This is for compatibility.
		t, err = time.Parse(logutil.OldSlowLogTimeFormat, s)
		if err != nil {
			err = errors.Errorf("string \"%v\" doesn't has a prefix that matches format \"%v\", err: %v", s, logutil.SlowLogTimeFormat, err)
		}
	}
	return t, err
}

type logFile struct {
	file       *os.File  // The opened file handle
	start, end time.Time // The start/end time of the log file
}

// getAllFiles is used to get all slow-log needed to parse, it is exported for test.
func (e *slowQueryRetriever) getAllFiles(sctx stochastikctx.Context, logFilePath string) ([]logFile, error) {
	if e.extractor == nil || !e.extractor.Enable {
		file, err := os.Open(logFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
		return []logFile{{file: file}}, nil
	}
	var logFiles []logFile
	logDir := filepath.Dir(logFilePath)
	ext := filepath.Ext(logFilePath)
	prefix := logFilePath[:len(logFilePath)-len(ext)]
	handleErr := func(err error) error {
		// Ignore the error and append warning for usability.
		if err != io.EOF {
			sctx.GetStochastikVars().StmtCtx.AppendWarning(err)
		}
		return nil
	}
	err := filepath.Walk(logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return handleErr(err)
		}
		if info.IsDir() {
			return nil
		}
		// All rotated log files have the same prefix with the original file.
		if !strings.HasPrefix(path, prefix) {
			return nil
		}
		file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return handleErr(err)
		}
		skip := false
		defer func() {
			if !skip {
				terror.Log(file.Close())
			}
		}()
		// Get the file start time.
		fileStartTime, err := e.getFileStartTime(file)
		if err != nil {
			return handleErr(err)
		}
		start := types.NewTime(types.FromGoTime(fileStartTime), allegrosql.TypeDatetime, types.MaxFsp)
		if start.Compare(e.checker.endTime) > 0 {
			return nil
		}

		// Get the file end time.
		fileEndTime, err := e.getFileEndTime(file)
		if err != nil {
			return handleErr(err)
		}
		end := types.NewTime(types.FromGoTime(fileEndTime), allegrosql.TypeDatetime, types.MaxFsp)
		if end.Compare(e.checker.startTime) < 0 {
			return nil
		}
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			return handleErr(err)
		}
		logFiles = append(logFiles, logFile{
			file:  file,
			start: fileStartTime,
			end:   fileEndTime,
		})
		skip = true
		return nil
	})
	// Sort by start time
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].start.Before(logFiles[j].start)
	})
	return logFiles, err
}

func (e *slowQueryRetriever) getFileStartTime(file *os.File) (time.Time, error) {
	var t time.Time
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return t, err
	}
	reader := bufio.NewReader(file)
	maxNum := 128
	for {
		lineByte, err := getOneLine(reader)
		if err != nil {
			return t, err
		}
		line := string(lineByte)
		if strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			return ParseTime(line[len(variable.SlowLogStartPrefixStr):])
		}
		maxNum -= 1
		if maxNum <= 0 {
			break
		}
	}
	return t, errors.Errorf("malform slow query file %v", file.Name())
}
func (e *slowQueryRetriever) getFileEndTime(file *os.File) (time.Time, error) {
	var t time.Time
	stat, err := file.Stat()
	if err != nil {
		return t, err
	}
	fileSize := stat.Size()
	cursor := int64(0)
	line := make([]byte, 0, 64)
	maxLineNum := 128
	tryGetTime := func(line []byte) string {
		for i, j := 0, len(line)-1; i < j; i, j = i+1, j-1 {
			line[i], line[j] = line[j], line[i]
		}
		lineStr := string(line)
		lineStr = strings.TrimSpace(lineStr)
		if strings.HasPrefix(lineStr, variable.SlowLogStartPrefixStr) {
			return lineStr[len(variable.SlowLogStartPrefixStr):]
		}
		return ""
	}
	for {
		cursor -= 1
		_, err := file.Seek(cursor, io.SeekEnd)
		if err != nil {
			return t, err
		}

		char := make([]byte, 1)
		_, err = file.Read(char)
		if err != nil {
			return t, err
		}
		// If find a line.
		if cursor != -1 && (char[0] == '\n' || char[0] == '\r') {
			if timeStr := tryGetTime(line); len(timeStr) > 0 {
				return ParseTime(timeStr)
			}
			line = line[:0]
			maxLineNum -= 1
		}
		line = append(line, char[0])
		if cursor == -fileSize || maxLineNum <= 0 {
			if timeStr := tryGetTime(line); len(timeStr) > 0 {
				return ParseTime(timeStr)
			}
			return t, errors.Errorf("malform slow query file %v", file.Name())
		}
	}
}

func (e *slowQueryRetriever) initializeAsyncParsing(ctx context.Context, sctx stochastikctx.Context) {
	e.parsedSlowLogCh = make(chan parsedSlowLog, 100)
	go e.parseDataForSlowLog(ctx, sctx)
}
