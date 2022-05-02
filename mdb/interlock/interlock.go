//INTERLOCKyright 2020 WHTCORPS INC ALL RIGHTS RESERVED
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

package interlock

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/ast"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/auth"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/mysql"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/serial"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/terror"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetnetctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetnetctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetnetctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid"
	"github.com/whtcorpsinc/MilevaDB-Prod/namespace"
	"github.com/whtcorpsinc/MilevaDB-Prod/namespace/infosync"
	"github.com/whtcorpsinc/MilevaDB-Prod/privilege"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/rel-planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/table"
	"github.com/whtcorpsinc/MilevaDB-Prod/table/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/tablecodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/util"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/admin"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/disk"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/stringutil"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

var (
	_ Interlock = &baseInterlock{}
	_ Interlock = &CheckTableExec{}
	_ Interlock = &HashAggExec{}
	_ Interlock = &HashJoinExec{}
	_ Interlock = &IndexLookUpInterlock{}
	_ Interlock = &IndexReaderInterlock{}
	_ Interlock = &LimitExec{}
	_ Interlock = &MaxOneRowExec{}
	_ Interlock = &MergeJoinExec{}
	_ Interlock = &ProjectionExec{}
	_ Interlock = &SelectionExec{}
	_ Interlock = &SelectLockExec{}
	_ Interlock = &ShowNextRowIDExec{}
	_ Interlock = &ShowDBSExec{}
	_ Interlock = &ShowDBSJobsExec{}
	_ Interlock = &ShowDBSJobQueriesExec{}
	_ Interlock = &SortExec{}
	_ Interlock = &StreamAggExec{}
	_ Interlock = &TableDualExec{}
	_ Interlock = &TableReaderInterlock{}
	_ Interlock = &TableScanExec{}
	_ Interlock = &TopNExec{}
	_ Interlock = &UnionExec{}

	// GlobalMemoryUsageTracker is the ancestor of all the Interlocks' memory tracker and GlobalMemory Tracker
	GlobalMemoryUsageTracker *memory.Tracker
	// GlobalDiskUsageTracker is the ancestor of all the Interlocks' disk tracker
	GlobalDiskUsageTracker *disk.Tracker
)

type baseInterlock struct {
	ctx           causetnetctx.Context
	id            fmt.Stringer
	schema        *expression.Schema // output schema
	initCap       int
	maxChunkSize  int
	children      []Interlock
	retFieldTypes []*types.FieldType
	runtimeStats  *execdetails.RuntimeStats
}

const (
	// globalStorageLabel represents the label of the GlobalDiskUsageTracker
	globalStorageLabel string = "GlobalStorageLabel"
	// globalMemoryLabel represents the label of the GlobalMemoryUsageTracker
	globalMemoryLabel string = "GlobalMemoryLabel"
	// globalPanicStorageExceed represents the panic message when out of storage quota.
	globalPanicStorageExceed string = "Out Of Global Storage Quota!"
	// globalPanicMemoryExceed represents the panic message when out of memory limit.
	globalPanicMemoryExceed string = "Out Of Global Memory Limit!"
)

// globalPanicOnExceed panics when GlobalDisTracker storage usage exceeds storage quota.
type globalPanicOnExceed struct {
	mutex sync.Mutex // For synchronization.
}

func init() {
	action := &globalPanicOnExceed{}
	GlobalMemoryUsageTracker = memory.NewGlobalTracker(stringutil.StringerStr(globalMemoryLabel), -1)
	GlobalMemoryUsageTracker.SetActionOnExceed(action)
	GlobalDiskUsageTracker = disk.NewGlobalTrcaker(stringutil.StringerStr(globalStorageLabel), -1)
	GlobalDiskUsageTracker.SetActionOnExceed(action)
}

// SetLogHook sets a hook for PanicOnExceed.
func (a *globalPanicOnExceed) SetLogHook(hook func(uint64)) {}

// Action panics when storage usage exceeds storage quota.
func (a *globalPanicOnExceed) Action(t *memory.Tracker) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	msg := ""
	switch t.Label().String() {
	case globalStorageLabel:
		msg = globalPanicStorageExceed
	case globalMemoryLabel:
		msg = globalPanicMemoryExceed
	default:
		msg = "Out of Unknown Resource Quota!"
	}
	panic(msg)
}

// SetFallback sets a fallback action.
func (a *globalPanicOnExceed) SetFallback(memory.ActionOnExceed) {}

// base returns the baseInterlock of an Interlock, don't override this method!
func (e *baseInterlock) base() *baseInterlock {
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseInterlock) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes all Interlocks and release all resources.
func (e *baseInterlock) Close() error {
	var firstErr error
	for _, src := range e.children {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Schema returns the current baseInterlock's schema. If it is nil, then create and return a new one.
func (e *baseInterlock) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

// newFirstChunk creates a new chunk to buffer current Interlock's result.
func newFirstChunk(e Interlock) *chunk.Chunk {
	base := e.base()
	return chunk.New(base.retFieldTypes, base.initCap, base.maxChunkSize)
}

// newList creates a new List to buffer current Interlock's result.
func newList(e Interlock) *chunk.List {
	base := e.base()
	return chunk.NewList(base.retFieldTypes, base.initCap, base.maxChunkSize)
}

// retTypes returns all output column types.
func retTypes(e Interlock) []*types.FieldType {
	base := e.base()
	return base.retFieldTypes
}

// Next fills multiple rows into a chunk.
func (e *baseInterlock) Next(ctx context.Context, req *chunk.Chunk) error {
	return nil
}

func newBaseInterlock(ctx causetnetctx.Context, schema *expression.Schema, id fmt.Stringer, children ...Interlock) baseInterlock {
	e := baseInterlock{
		children:     children,
		ctx:          ctx,
		id:           id,
		schema:       schema,
		initCap:      ctx.GetCausetNetVars().InitChunkSize,
		maxChunkSize: ctx.GetCausetNetVars().MaxChunkSize,
	}
	if ctx.GetCausetNetVars().StmtCtx.RuntimeStatsColl != nil {
		if e.id != nil {
			e.runtimeStats = e.ctx.GetCausetNetVars().StmtCtx.RuntimeStatsColl.GetRootStats(e.id.String())
		}
	}
	if schema != nil {
		cols := schema.Columns
		e.retFieldTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			e.retFieldTypes[i] = cols[i].RetType
		}
	}
	return e
}

// Interlock is the physical implementation of a algebra operator.
//
// In MilevaDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution serial, a "Next" function call in MilevaDB will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Interlocks must call "chk.Reset()" before appending their results to it.
type Interlock interface {
	base() *baseInterlock
	Open(context.Context) error
	Next(ctx context.Context, req *chunk.Chunk) error
	Close() error
	Schema() *expression.Schema
}

// Next is a wrapper function on e.Next(), it handles some common codes.
func Next(ctx context.Context, e Interlock, req *chunk.Chunk) error {
	base := e.base()
	if base.runtimeStats != nil {
		start := time.Now()
		defer func() { base.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	sessVars := base.ctx.GetCausetNetVars()
	if atomic.LoadUint32(&sessVars.Killed) == 1 {
		return ErrQueryInterrupted
	}
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("%T.Next", e), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	err := e.Next(ctx, req)

	if err != nil {
		return err
	}
	// recheck whether the CausetNet/query is killed during the Next()
	if atomic.LoadUint32(&sessVars.Killed) == 1 {
		err = ErrQueryInterrupted
	}
	return err
}

// CancelDBSJobsExec represents a cancel DBS jobs Interlock.
type CancelDBSJobsExec struct {
	baseInterlock

	cursor int
	jobIDs []int64
	errs   []error
}

// Next implements the Interlock Next interface.
func (e *CancelDBSJobsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobIDs) {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		req.AppendString(0, fmt.Sprintf("%d", e.jobIDs[i]))
		if e.errs[i] != nil {
			req.AppendString(1, fmt.Sprintf("error: %v", e.errs[i]))
		} else {
			req.AppendString(1, "successful")
		}
	}
	e.cursor += numCurBatch
	return nil
}

// ShowNextRowIDExec represents a show the next row ID Interlock.
type ShowNextRowIDExec struct {
	baseInterlock
	tblName *ast.TableName
	done    bool
}

// Next implements the Interlock Next interface.
func (e *ShowNextRowIDExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	is := namespace.GetNamespace(e.ctx).schemareplicant()
	tbl, err := is.TableByName(e.tblName.Schema, e.tblName.Name)
	if err != nil {
		return err
	}
	tblMeta := tbl.Meta()

	allocators := tbl.Allocators(e.ctx)
	for _, alloc := range allocators {
		nextGlobalID, err := alloc.NextGlobalAutoID(tblMeta.ID)
		if err != nil {
			return err
		}

		var colName, idType string
		switch alloc.GetType() {
		case autoid.RowIDAllocType, autoid.AutoIncrementType:
			idType = "AUTO_INCREMENT"
			if col := tblMeta.GetAutoIncrementColInfo(); col != nil {
				colName = col.Name.O
			} else {
				colName = serial.ExtraHandleName.O
			}
		case autoid.AutoRandomType:
			idType = "AUTO_RANDOM"
			colName = tblMeta.GetPkName().O
		case autoid.SequenceType:
			idType = "SEQUENCE"
			colName = ""
		default:
			return autoid.ErrInvalidAllocatorType.GenWithStackByArgs()
		}

		req.AppendString(0, e.tblName.Schema.O)
		req.AppendString(1, e.tblName.Name.O)
		req.AppendString(2, colName)
		req.AppendInt64(3, nextGlobalID)
		req.AppendString(4, idType)
	}

	e.done = true
	return nil
}

// ShowDBSExec represents a show DBS Interlock.
type ShowDBSExec struct {
	baseInterlock

	dbsOwnerID string
	selfID     string
	dbsInfo    *admin.DBSInfo
	done       bool
}

// Next implements the Interlock Next interface.
func (e *ShowDBSExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}

	dbsJobs := ""
	query := ""
	l := len(e.dbsInfo.Jobs)
	for i, job := range e.dbsInfo.Jobs {
		dbsJobs += job.String()
		query += job.Query
		if i != l-1 {
			dbsJobs += "\n"
			query += "\n"
		}
	}

	serverInfo, err := infosync.GetServerInfoByID(ctx, e.dbsOwnerID)
	if err != nil {
		return err
	}

	serverAddress := serverInfo.IP + ":" +
		strconv.FormatUint(uint64(serverInfo.Port), 10)

	req.AppendInt64(0, e.dbsInfo.SchemaVer)
	req.AppendString(1, e.dbsOwnerID)
	req.AppendString(2, serverAddress)
	req.AppendString(3, dbsJobs)
	req.AppendString(4, e.selfID)
	req.AppendString(5, query)

	e.done = true
	return nil
}

// ShowDBSJobsExec represent a show DBS jobs Interlock.
type ShowDBSJobsExec struct {
	baseInterlock
	DBSJobRetriever

	jobNumber int
	is        schemareplicant.schemareplicant
	done      bool
}

// DBSJobRetriever retrieve the DBSJobs.
type DBSJobRetriever struct {
	runningJobs    []*serial.Job
	historyJobIter *meta.LastJobIterator
	cursor         int
	is             schemareplicant.schemareplicant
	activeRoles    []*auth.RoleIdentity
	cacheJobs      []*serial.Job
}

func (e *DBSJobRetriever) initial(txn ekv.Transaction) error {
	jobs, err := admin.GetDBSJobs(txn)
	if err != nil {
		return err
	}
	m := meta.NewMeta(txn)
	e.historyJobIter, err = m.GetLastHistoryDBSJobsIterator()
	if err != nil {
		return err
	}
	e.runningJobs = jobs
	e.cursor = 0
	return nil
}

func (e *DBSJobRetriever) appendJobToChunk(req *chunk.Chunk, job *serial.Job, checker privilege.Manager) {
	schemaName := job.SchemaName
	tableName := ""
	finishTS := uint64(0)
	if job.BinlogInfo != nil {
		finishTS = job.BinlogInfo.FinishedTS
		if job.BinlogInfo.TableInfo != nil {
			tableName = job.BinlogInfo.TableInfo.Name.L
		}
		if len(schemaName) == 0 && job.BinlogInfo.DBInfo != nil {
			schemaName = job.BinlogInfo.DBInfo.Name.L
		}
	}
	// For compatibility, the old version of DBS Job wasn't store the schema name and table name.
	if len(schemaName) == 0 {
		schemaName = getSchemaName(e.is, job.SchemaID)
	}
	if len(tableName) == 0 {
		tableName = getTableName(e.is, job.TableID)
	}

	startTime := ts2Time(job.StartTS)
	finishTime := ts2Time(finishTS)

	// Check the privilege.
	if checker != nil && !checker.RequestVerification(e.activeRoles, strings.ToLower(schemaName), strings.ToLower(tableName), "", mysql.AllPrivMask) {
		return
	}

	req.AppendInt64(0, job.ID)
	req.AppendString(1, schemaName)
	req.AppendString(2, tableName)
	req.AppendString(3, job.Type.String())
	req.AppendString(4, job.SchemaState.String())
	req.AppendInt64(5, job.SchemaID)
	req.AppendInt64(6, job.TableID)
	req.AppendInt64(7, job.RowCount)
	req.AppendTime(8, startTime)
	if finishTS > 0 {
		req.AppendTime(9, finishTime)
	} else {
		req.AppendNull(9)
	}
	req.AppendString(10, job.State.String())
}

func ts2Time(timestamp uint64) types.Time {
	duration := time.Duration(math.Pow10(9-int(types.DefaultFsp))) * time.Nanosecond
	t := serial.TSConvert2Time(timestamp)
	t.Truncate(duration)
	return types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, types.DefaultFsp)
}

// ShowDBSJobQueriesExec represents a show DBS job queries Interlock.
// The jobs id that is given by 'admin show dbs job queries' statement,
// only be searched in the latest 10 history jobs
type ShowDBSJobQueriesExec struct {
	baseInterlock

	cursor int
	jobs   []*serial.Job
	jobIDs []int64
}

// Open implements the Interlock Open interface.
func (e *ShowDBSJobQueriesExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	jobs, err := admin.GetDBSJobs(txn)
	if err != nil {
		return err
	}
	historyJobs, err := admin.GetHistoryDBSJobs(txn, admin.DefNumHistoryJobs)
	if err != nil {
		return err
	}

	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)

	return nil
}

// Next implements the Interlock Next interface.
func (e *ShowDBSJobQueriesExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobs) {
		return nil
	}
	if len(e.jobIDs) >= len(e.jobs) {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobs)-e.cursor)
	for _, id := range e.jobIDs {
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			if id == e.jobs[i].ID {
				req.AppendString(0, e.jobs[i].Query)
			}
		}
	}
	e.cursor += numCurBatch
	return nil
}

// Open implements the Interlock Open interface.
func (e *ShowDBSJobsExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	e.DBSJobRetriever.is = e.is
	if e.jobNumber == 0 {
		e.jobNumber = admin.DefNumHistoryJobs
	}
	err = e.DBSJobRetriever.initial(txn)
	if err != nil {
		return err
	}
	return nil
}

// Next implements the Interlock Next interface.
func (e *ShowDBSJobsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if (e.cursor - len(e.runningJobs)) >= e.jobNumber {
		return nil
	}
	count := 0

	// Append running dbs jobs.
	if e.cursor < len(e.runningJobs) {
		numCurBatch := mathutil.Min(req.Capacity(), len(e.runningJobs)-e.cursor)
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			e.appendJobToChunk(req, e.runningJobs[i], nil)
		}
		e.cursor += numCurBatch
		count += numCurBatch
	}

	// Append history dbs jobs.
	var err error
	if count < req.Capacity() {
		num := req.Capacity() - count
		remainNum := e.jobNumber - (e.cursor - len(e.runningJobs))
		num = mathutil.Min(num, remainNum)
		e.cacheJobs, err = e.historyJobIter.GetLastJobs(num, e.cacheJobs)
		if err != nil {
			return err
		}
		for _, job := range e.cacheJobs {
			e.appendJobToChunk(req, job, nil)
		}
		e.cursor += len(e.cacheJobs)
	}
	return nil
}

func getSchemaName(is schemareplicant.schemareplicant, id int64) string {
	var schemaName string
	DBInfo, ok := is.SchemaByID(id)
	if ok {
		schemaName = DBInfo.Name.O
		return schemaName
	}

	return schemaName
}

func getTableName(is schemareplicant.schemareplicant, id int64) string {
	var tableName string
	table, ok := is.TableByID(id)
	if ok {
		tableName = table.Meta().Name.O
		return tableName
	}

	return tableName
}

// CheckTableExec represents a check table Interlock.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	baseInterlock

	dbName     string
	table      table.Block
	indexInfos []*serial.IndexInfo
	srcs       []*IndexLookUpInterlock
	done       bool
	is         schemareplicant.schemareplicant
	exitCh     chan struct{}
	retCh      chan error
	checkIndex bool
}

// Open implements the Interlock Open interface.
func (e *CheckTableExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}
	for _, src := range e.srcs {
		if err := src.Open(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	e.done = false
	return nil
}

// Close implements the Interlock Close interface.
func (e *CheckTableExec) Close() error {
	var firstErr error
	for _, src := range e.srcs {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *CheckTableExec) checkTableIndexHandle(ctx context.Context, idxInfo *serial.IndexInfo) error {
	// For partition table, there will be multi same index indexLookUpReaders on different partitions.
	for _, src := range e.srcs {
		if src.index.Name.L == idxInfo.Name.L {
			err := e.checkIndexHandle(ctx, src)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *CheckTableExec) checkIndexHandle(ctx context.Context, src *IndexLookUpInterlock) error {
	cols := src.schema.Columns
	retFieldTypes := make([]*types.FieldType, len(cols))
	for i := range cols {
		retFieldTypes[i] = cols[i].RetType
	}
	chk := chunk.New(retFieldTypes, e.initCap, e.maxChunkSize)

	var err error
	for {
		err = Next(ctx, src, chk)
		if err != nil {
			break
		}
		if chk.NumRows() == 0 {
			break
		}

		select {
		case <-e.exitCh:
			return nil
		default:
		}
	}
	e.retCh <- errors.Trace(err)
	return errors.Trace(err)
}

func (e *CheckTableExec) handlePanic(r interface{}) {
	if r != nil {
		e.retCh <- errors.Errorf("%v", r)
	}
}

// Next implements the Interlock Next interface.
func (e *CheckTableExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done || len(e.srcs) == 0 {
		return nil
	}
	defer func() { e.done = true }()

	if e.table.Meta().IsCommonHandle {
		// TODO: fix me to support cluster index table admin check table.
		// https://github.com/whtcorpsinc/MilevaDB-Prod/projects/45#card-39562229
		return nil
	}

	idxNames := make([]string, 0, len(e.indexInfos))
	for _, idx := range e.indexInfos {
		idxNames = append(idxNames, idx.Name.O)
	}
	greater, idxOffset, err := admin.CheckIndicesCount(e.ctx, e.dbName, e.table.Meta().Name.O, idxNames)
	if err != nil {
		// For admin check index statement, for speed up and compatibility, doesn't do below checks.
		if e.checkIndex {
			return errors.Trace(err)
		}
		if greater == admin.IdxCntGreater {
			err = e.checkTableIndexHandle(ctx, e.indexInfos[idxOffset])
		} else if greater == admin.TblCntGreater {
			err = e.checkTableRecord(idxOffset)
		}
		if err != nil && admin.ErrDataInConsistent.Equal(err) {
			return ErrAdminCheckTable.GenWithStack("%v err:%v", e.table.Meta().Name, err)
		}
		return errors.Trace(err)
	}

	// The number of table rows is equal to the number of index rows.
	// TODO: Make the value of concurrency adjustable. And we can consider the number of records.
	concurrency := 3
	wg := sync.WaitGroup{}
	for i := range e.srcs {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			util.WithRecovery(func() {
				err1 := e.checkIndexHandle(ctx, e.srcs[num])
				if err1 != nil {
					logutil.Logger(ctx).Info("check index handle failed", zap.Error(err1))
				}
			}, e.handlePanic)
		}(i)

		if (i+1)%concurrency == 0 {
			wg.Wait()
		}
	}

	for i := 0; i < len(e.srcs); i++ {
		err = <-e.retCh
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *CheckTableExec) checkTableRecord(idxOffset int) error {
	idxInfo := e.indexInfos[idxOffset]
	// TODO: Fix me later, can not use genExprs in indexLookUpReader, because the schema of expression is different.
	genExprs := e.srcs[idxOffset].genExprs
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	if e.table.Meta().GetPartitionInfo() == nil {
		idx := blocks.NewIndex(e.table.Meta().ID, e.table.Meta(), idxInfo)
		return admin.CheckRecordAndIndex(e.ctx, txn, e.table, idx, genExprs)
	}

	info := e.table.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := e.table.(table.PartitionedTable).GetPartition(pid)
		idx := blocks.NewIndex(def.ID, e.table.Meta(), idxInfo)
		if err := admin.CheckRecordAndIndex(e.ctx, txn, partition, idx, genExprs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ShowSlowExec represents the Interlock of showing the slow queries.
// It is build from the "admin show slow" statement:
//	admin show slow top [internal | all] N
//	admin show slow recent N
type ShowSlowExec struct {
	baseInterlock

	ShowSlow *ast.ShowSlow
	result   []*namespace.SlowQueryInfo
	cursor   int
}

// Open implements the Interlock Open interface.
func (e *ShowSlowExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}

	dom := namespace.GetNamespace(e.ctx)
	e.result = dom.ShowSlowQuery(e.ShowSlow)
	return nil
}

// Next implements the Interlock Next interface.
func (e *ShowSlowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= len(e.result) {
		return nil
	}

	for e.cursor < len(e.result) && req.NumRows() < e.maxChunkSize {
		slow := e.result[e.cursor]
		req.AppendString(0, slow.SQL)
		req.AppendTime(1, types.NewTime(types.FromGoTime(slow.Start), mysql.TypeTimestamp, types.MaxFsp))
		req.AppendDuration(2, types.Duration{Duration: slow.Duration, Fsp: types.MaxFsp})
		req.AppendString(3, slow.Detail.String())
		if slow.Succ {
			req.AppendInt64(4, 1)
		} else {
			req.AppendInt64(4, 0)
		}
		req.AppendUint64(5, slow.ConnID)
		req.AppendUint64(6, slow.TxnTS)
		req.AppendString(7, slow.User)
		req.AppendString(8, slow.DB)
		req.AppendString(9, slow.TableIDs)
		req.AppendString(10, slow.IndexNames)
		if slow.Internal {
			req.AppendInt64(11, 1)
		} else {
			req.AppendInt64(11, 0)
		}
		req.AppendString(12, slow.Digest)
		e.cursor++
	}
	return nil
}

// SelectLockExec represents a select dagger Interlock.
// It is built from the "SELECT .. FOR UPDATE" or the "SELECT .. LOCK IN SHARE MODE" statement.
// For "SELECT .. FOR UPDATE" statement, it locks every row key from source Interlock.
// After the execution, the keys are buffered in transaction, and will be sent to KV
// when doing commit. If there is any key already locked by another transaction,
// the transaction will rollback and retry.
type SelectLockExec struct {
	baseInterlock

	Lock ast.SelectLockType
	keys []ekv.Key

	tblID2Handle     map[int64][]plannercore.HandleCols
	partitionedTable []table.PartitionedTable

	// tblID2Table is cached to reduce cost.
	tblID2Table map[int64]table.PartitionedTable
}

// Open implements the Interlock Open interface.
func (e *SelectLockExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}

	if len(e.tblID2Handle) > 0 && len(e.partitionedTable) > 0 {
		e.tblID2Table = make(map[int64]table.PartitionedTable, len(e.partitionedTable))
		for id := range e.tblID2Handle {
			for _, p := range e.partitionedTable {
				if id == p.Meta().ID {
					e.tblID2Table[id] = p
				}
			}
		}
	}

	return nil
}

// Next implements the Interlock Next interface.
func (e *SelectLockExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}
	// If there's no handle or it's not a `SELECT FOR UPDATE` statement.
	if len(e.tblID2Handle) == 0 || (e.Lock != ast.SelectLockForUpdate && e.Lock != ast.SelectLockForUpdateNoWait) {
		return nil
	}

	if req.NumRows() > 0 {
		iteron := chunk.NewIterator4Chunk(req)
		for row := iteron.Begin(); row != iteron.End(); row = iteron.Next() {
			for id, cols := range e.tblID2Handle {
				physicalID := id
				if pt, ok := e.tblID2Table[id]; ok {
					// On a partitioned table, we have to use physical ID to encode the dagger key!
					p, err := pt.GetPartitionByRow(e.ctx, row.GetCausetObjectQLRow(e.base().retFieldTypes))
					if err != nil {
						return err
					}
					physicalID = p.GetPhysicalID()
				}

				for _, col := range cols {
					handle, err := col.BuildHandle(row)
					if err != nil {
						return err
					}
					e.keys = append(e.keys, tablecodec.EncodeRowKeyWithHandle(physicalID, handle))
				}
			}
		}
		return nil
	}
	lockWaitTime := e.ctx.GetCausetNetVars().LockWaitTimeout
	if e.Lock == ast.SelectLockForUpdateNoWait {
		lockWaitTime = ekv.LockNoWait
	}

	if len(e.keys) > 0 {
		// This operation is only for schema validator check.
		for id := range e.tblID2Handle {
			e.ctx.GetCausetNetVars().TxnCtx.UpdateDeltaForTable(id, 0, 0, map[int64]int64{})
		}
	}

	return doLockKeys(ctx, e.ctx, newLockCtx(e.ctx.GetCausetNetVars(), lockWaitTime), e.keys...)
}

func newLockCtx(seVars *variable.CausetNetVars, lockWaitTime int64) *ekv.LockCtx {
	return &ekv.LockCtx{
		Killed:                &seVars.Killed,
		ForUpdateTS:           seVars.TxnCtx.GetForUpdateTS(),
		LockWaitTime:          lockWaitTime,
		WaitStartTime:         seVars.StmtCtx.GetLockWaitStartTime(),
		PessimisticLockWaited: &seVars.StmtCtx.PessimisticLockWaited,
		LockKeysDuration:      &seVars.StmtCtx.LockKeysDuration,
		LockKeysCount:         &seVars.StmtCtx.LockKeysCount,
		LockExpired:           &seVars.TxnCtx.LockExpire,
	}
}

// doLockKeys is the main entry for pessimistic dagger keys
// waitTime means the dagger operation will wait in milliseconds if target key is already
// locked by others. used for (select for update nowait) situation
// except 0 means alwaysWait 1 means nowait
func doLockKeys(ctx context.Context, se causetnetctx.Context, lockCtx *ekv.LockCtx, keys ...ekv.Key) error {
	sctx := se.GetCausetNetVars().StmtCtx
	if !sctx.InUpdateStmt && !sctx.InDeleteStmt {
		se.GetCausetNetVars().TxnCtx.ForUpdate = true
	}
	// Lock keys only once when finished fetching all results.
	txn, err := se.Txn(true)
	if err != nil {
		return err
	}
	return txn.LockKeys(causetnetctx.SetCommitCtx(ctx, se), lockCtx, keys...)
}

// LimitExec represents limit Interlock
// It ignores 'Offset' rows from src, then returns 'Count' rows at maximum.
type LimitExec struct {
	baseInterlock

	begin  uint64
	end    uint64
	cursor uint64

	// meetFirstBatch represents whether we have met the first valid Chunk from child.
	meetFirstBatch bool

	childResult *chunk.Chunk
}

// Next implements the Interlock Next interface.
func (e *LimitExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		// transfer req's requiredRows to childResult and then adjust it in childResult
		e.childResult = e.childResult.SetRequiredRows(req.RequiredRows(), e.maxChunkSize)
		err := Next(ctx, e.children[0], e.adjustRequiredRows(e.childResult))
		if err != nil {
			return err
		}
		batchSize := uint64(e.childResult.NumRows())
		// no more data.
		if batchSize == 0 {
			return nil
		}
		if newCursor := e.cursor + batchSize; newCursor >= e.begin {
			e.meetFirstBatch = true
			begin, end := e.begin-e.cursor, batchSize
			if newCursor > e.end {
				end = e.end - e.cursor
			}
			e.cursor += end
			if begin == end {
				break
			}
			req.Append(e.childResult, int(begin), int(end))
			return nil
		}
		e.cursor += batchSize
	}
	e.adjustRequiredRows(req)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}
	batchSize := uint64(req.NumRows())
	// no more data.
	if batchSize == 0 {
		return nil
	}
	if e.cursor+batchSize > e.end {
		req.TruncateTo(int(e.end - e.cursor))
		batchSize = e.end - e.cursor
	}
	e.cursor += batchSize
	return nil
}

// Open implements the Interlock Open interface.
func (e *LimitExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}
	e.childResult = newFirstChunk(e.children[0])
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	return nil
}

// Close implements the Interlock Close interface.
func (e *LimitExec) Close() error {
	e.childResult = nil
	return e.baseInterlock.Close()
}

func (e *LimitExec) adjustRequiredRows(chk *chunk.Chunk) *chunk.Chunk {
	// the limit of maximum number of rows the LimitExec should read
	limitTotal := int(e.end - e.cursor)

	var limitRequired int
	if e.cursor < e.begin {
		// if cursor is less than begin, it have to read (begin-cursor) rows to ignore
		// and then read chk.RequiredRows() rows to return,
		// so the limit is (begin-cursor)+chk.RequiredRows().
		limitRequired = int(e.begin) - int(e.cursor) + chk.RequiredRows()
	} else {
		// if cursor is equal or larger than begin, just read chk.RequiredRows() rows to return.
		limitRequired = chk.RequiredRows()
	}

	return chk.SetRequiredRows(mathutil.Min(limitTotal, limitRequired), e.maxChunkSize)
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the Interlock package because of the dependency cycle.
	// So we assign a function implemented in the Interlock package to the plan package to avoid the dependency cycle.
	plannercore.EvalSubqueryFirstRow = func(ctx context.Context, p plannercore.PhysicalPlan, is schemareplicant.schemareplicant, sctx causetnetctx.Context) ([]types.CausetObjectQL, error) {
		defer func(begin time.Time) {
			s := sctx.GetCausetNetVars()
			s.RewritePhaseInfo.PreprocessSubQueries++
			s.RewritePhaseInfo.DurationPreprocessSubQuery += time.Since(begin)
		}(time.Now())

		if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
			span1 := span.Tracer().StartSpan("Interlock.EvalSubQuery", opentracing.ChildOf(span.Context()))
			defer span1.Finish()
			ctx = opentracing.ContextWithSpan(ctx, span1)
		}

		e := &InterlockBuilder{is: is, ctx: sctx}
		exec := e.build(p)
		if e.err != nil {
			return nil, e.err
		}
		err := exec.Open(ctx)
		defer terror.Call(exec.Close)
		if err != nil {
			return nil, err
		}
		chk := newFirstChunk(exec)
		for {
			err = Next(ctx, exec, chk)
			if err != nil {
				return nil, err
			}
			if chk.NumRows() == 0 {
				return nil, nil
			}
			row := chk.GetRow(0).GetCausetObjectQLRow(retTypes(exec))
			return row, err
		}
	}
}

// TableDualExec represents a dual table Interlock.
type TableDualExec struct {
	baseInterlock

	// numDualRows can only be 0 or 1.
	numDualRows int
	numReturned int
}

// Open implements the Interlock Open interface.
func (e *TableDualExec) Open(ctx context.Context) error {
	e.numReturned = 0
	return nil
}

// Next implements the Interlock Next interface.
func (e *TableDualExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.numReturned >= e.numDualRows {
		return nil
	}
	if e.Schema().Len() == 0 {
		req.SetNumVirtualRows(1)
	} else {
		for i := range e.Schema().Columns {
			req.AppendNull(i)
		}
	}
	e.numReturned = e.numDualRows
	return nil
}

// SelectionExec represents a filter Interlock.
type SelectionExec struct {
	baseInterlock

	batched     bool
	filters     []expression.Expression
	selected    []bool
	inputIter   *chunk.Iterator4Chunk
	inputRow    chunk.Row
	childResult *chunk.Chunk

	memTracker *memory.Tracker
}

// Open implements the Interlock Open interface.
func (e *SelectionExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}
	return e.open(ctx)
}

func (e *SelectionExec) open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetCausetNetVars().StmtCtx.MemTracker)
	e.childResult = newFirstChunk(e.children[0])
	e.memTracker.Consume(e.childResult.MemoryUsage())
	e.batched = expression.Vectorizable(e.filters)
	if e.batched {
		e.selected = make([]bool, 0, chunk.InitialCapacity)
	}
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()
	return nil
}

// Close implements plannercore.Plan Close interface.
func (e *SelectionExec) Close() error {
	e.memTracker.Consume(-e.childResult.MemoryUsage())
	e.childResult = nil
	e.selected = nil
	return e.baseInterlock.Close()
}

// Next implements the Interlock Next interface.
func (e *SelectionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)

	if !e.batched {
		return e.unBatchedNext(ctx, req)
	}

	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			if !e.selected[e.inputRow.Idx()] {
				continue
			}
			if req.IsFull() {
				return nil
			}
			req.AppendRow(e.inputRow)
		}
		mSize := e.childResult.MemoryUsage()
		err := Next(ctx, e.children[0], e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
		e.selected, err = expression.VectorizedFilter(e.ctx, e.filters, e.inputIter, e.selected)
		if err != nil {
			return err
		}
		e.inputRow = e.inputIter.Begin()
	}
}

// unBatchedNext filters input rows one by one and returns once an input row is selected.
// For sql with "SETVAR" in filter and "GETVAR" in projection, for example: "SELECT @a FROM t WHERE (@a := 2) > 0",
// we have to set batch size to 1 to do the evaluation of filter and projection.
func (e *SelectionExec) unBatchedNext(ctx context.Context, chk *chunk.Chunk) error {
	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			selected, _, err := expression.EvalBool(e.ctx, e.filters, e.inputRow)
			if err != nil {
				return err
			}
			if selected {
				chk.AppendRow(e.inputRow)
				e.inputRow = e.inputIter.Next()
				return nil
			}
		}
		mSize := e.childResult.MemoryUsage()
		err := Next(ctx, e.children[0], e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}
		e.inputRow = e.inputIter.Begin()
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
	}
}

// TableScanExec is a table scan Interlock without result fields.
type TableScanExec struct {
	baseInterlock

	t                     table.Block
	columns               []*serial.ColumnInfo
	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

// Next implements the Interlock Next interface.
func (e *TableScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	return e.nextChunk4schemareplicant(ctx, req)
}

func (e *TableScanExec) nextChunk4schemareplicant(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	if e.virtualTableChunkList == nil {
		e.virtualTableChunkList = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
		columns := make([]*table.Column, e.schema.Len())
		for i, colInfo := range e.columns {
			columns[i] = table.ToColumn(colInfo)
		}
		mutableRow := chunk.MutRowFromTypes(retTypes(e))
		err := e.t.IterRecords(e.ctx, nil, columns, func(_ ekv.Handle, rec []types.CausetObjectQL, cols []*table.Column) (bool, error) {
			mutableRow.SetCausetObjectQLs(rec...)
			e.virtualTableChunkList.AppendRow(mutableRow.ToRow())
			return true, nil
		})
		if err != nil {
			return err
		}
	}
	// no more data.
	if e.virtualTableChunkIdx >= e.virtualTableChunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.virtualTableChunkList.GetChunk(e.virtualTableChunkIdx)
	e.virtualTableChunkIdx++
	chk.SwapColumns(virtualTableChunk)
	return nil
}

// Open implements the Interlock Open interface.
func (e *TableScanExec) Open(ctx context.Context) error {
	e.virtualTableChunkList = nil
	return nil
}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	baseInterlock

	evaluated bool
}

// Open implements the Interlock Open interface.
func (e *MaxOneRowExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}
	e.evaluated = false
	return nil
}

// Next implements the Interlock Next interface.
func (e *MaxOneRowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.evaluated {
		return nil
	}
	e.evaluated = true
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}

	if num := req.NumRows(); num == 0 {
		for i := range e.schema.Columns {
			req.AppendNull(i)
		}
		return nil
	} else if num != 1 {
		return errors.New("subquery returns more than 1 row")
	}

	childChunk := newFirstChunk(e.children[0])
	err = Next(ctx, e.children[0], childChunk)
	if err != nil {
		return err
	}
	if childChunk.NumRows() != 0 {
		return errors.New("subquery returns more than 1 row")
	}

	return nil
}

// UnionExec pulls all it's children's result and returns to its parent directly.
// A "resultPuller" is started for every child to pull result from that child and push it to the "resultPool", the used
// "Chunk" is obtained from the corresponding "resourcePool". All resultPullers are running concurrently.
//                             +----------------+
//   +---> resourcePool 1 ---> | resultPuller 1 |-----+
//   |                         +----------------+     |
//   |                                                |
//   |                         +----------------+     v
//   +---> resourcePool 2 ---> | resultPuller 2 |-----> resultPool ---+
//   |                         +----------------+     ^               |
//   |                               ......           |               |
//   |                         +----------------+     |               |
//   +---> resourcePool n ---> | resultPuller n |-----+               |
//   |                         +----------------+                     |
//   |                                                                |
//   |                          +-------------+                       |
//   |--------------------------| main thread | <---------------------+
//                              +-------------+
type UnionExec struct {
	baseInterlock

	stopFetchData atomic.Value

	finished      chan struct{}
	resourcePools []chan *chunk.Chunk
	resultPool    chan *unionleasee_parity_filterResult

	childrenResults []*chunk.Chunk
	wg              sync.WaitGroup
	initialized     bool
}

// unionleasee_parity_filterResult stores the result for a union leasee_parity_filter.
// A "resultPuller" is started for every child to pull result from that child, unionleasee_parity_filterResult is used to store that pulled result.
// "src" is used for Chunk reuse: after pulling result from "resultPool", main-thread must push a valid unused Chunk to "src" to
// enable the corresponding "resultPuller" continue to work.
type unionleasee_parity_filterResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

func (e *UnionExec) waitAllFinished() {
	e.wg.Wait()
	close(e.resultPool)
}

// Open implements the Interlock Open interface.
func (e *UnionExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}
	for _, child := range e.children {
		e.childrenResults = append(e.childrenResults, newFirstChunk(child))
	}
	e.stopFetchData.Store(false)
	e.initialized = false
	e.finished = make(chan struct{})
	return nil
}

func (e *UnionExec) initialize(ctx context.Context) {
	e.resultPool = make(chan *unionleasee_parity_filterResult, len(e.children))
	e.resourcePools = make([]chan *chunk.Chunk, len(e.children))
	for i := range e.children {
		e.resourcePools[i] = make(chan *chunk.Chunk, 1)
		e.resourcePools[i] <- e.childrenResults[i]
		e.wg.Add(1)
		go e.resultPuller(ctx, i)
	}
	go e.waitAllFinished()
}

func (e *UnionExec) resultPuller(ctx context.Context, childID int) {
	result := &unionleasee_parity_filterResult{
		err: nil,
		chk: nil,
		src: e.resourcePools[childID],
	}
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("resultPuller panicked", zap.String("stack", string(buf)))
			result.err = errors.Errorf("%v", r)
			e.resultPool <- result
			e.stopFetchData.Store(true)
		}
		e.wg.Done()
	}()
	for {
		if e.stopFetchData.Load().(bool) {
			return
		}
		select {
		case <-e.finished:
			return
		case result.chk = <-e.resourcePools[childID]:
		}
		result.err = Next(ctx, e.children[childID], result.chk)
		if result.err == nil && result.chk.NumRows() == 0 {
			return
		}
		e.resultPool <- result
		if result.err != nil {
			e.stopFetchData.Store(true)
			return
		}
	}
}

// Next implements the Interlock Next interface.
func (e *UnionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if !e.initialized {
		e.initialize(ctx)
		e.initialized = true
	}
	result, ok := <-e.resultPool
	if !ok {
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}

	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the Interlock Close interface.
func (e *UnionExec) Close() error {
	if e.finished != nil {
		close(e.finished)
	}
	e.childrenResults = nil
	if e.resultPool != nil {
		for range e.resultPool {
		}
	}
	e.resourcePools = nil
	return e.baseInterlock.Close()
}

// ResetContextOfStmt resets the StmtContext and CausetNet variables.
// Before every execution, we must clear statement context.
func ResetContextOfStmt(ctx causetnetctx.Context, s ast.StmtNode) (err error) {
	vars := ctx.GetCausetNetVars()
	// Detach the Memory and disk tracker for the previous stmtCtx from GlobalMemoryUsageTracker and GlobalDiskUsageTracker
	if stmtCtx := vars.StmtCtx; stmtCtx != nil {
		if stmtCtx.DiskTracker != nil {
			stmtCtx.DiskTracker.DetachFromGlobalTracker()
		}
		if stmtCtx.MemTracker != nil {
			stmtCtx.MemTracker.DetachFromGlobalTracker()
		}
	}
	sc := &stmtctx.StatementContext{
		TimeZone:    vars.Location(),
		MemTracker:  memory.NewTracker(stringutil.MemoizeStr(s.Text), vars.MemQuotaQuery),
		DiskTracker: disk.NewTracker(stringutil.MemoizeStr(s.Text), -1),
		TaskID:      stmtctx.AllocateTaskID(),
	}
	sc.MemTracker.AttachToGlobalTracker(GlobalMemoryUsageTracker)
	if config.GetGlobalConfig().OOMUseTmpStorage && GlobalDiskUsageTracker != nil {
		sc.DiskTracker.AttachToGlobalTracker(GlobalDiskUsageTracker)
	}
	switch config.GetGlobalConfig().OOMAction {
	case config.OOMActionCancel:
		action := &memory.PanicOnExceed{ConnID: ctx.GetCausetNetVars().ConnectionID}
		action.SetLogHook(namespace.GetNamespace(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		sc.MemTracker.SetActionOnExceed(action)
	case config.OOMActionLog:
		fallthrough
	default:
		action := &memory.LogOnExceed{ConnID: ctx.GetCausetNetVars().ConnectionID}
		action.SetLogHook(namespace.GetNamespace(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		sc.MemTracker.SetActionOnExceed(action)
	}
	if execStmt, ok := s.(*ast.ExecuteStmt); ok {
		s, err = getPreparedStmt(execStmt, vars)
		if err != nil {
			return
		}
	}
	// execute missed stmtID uses empty sql
	sc.OriginalSQL = s.Text()
	if explainStmt, ok := s.(*ast.ExplainStmt); ok {
		sc.InExplainStmt = true
		s = explainStmt.Stmt
	}
	if _, ok := s.(*ast.ExplainForStmt); ok {
		sc.InExplainStmt = true
	}
	// TODO: Many same bool variables here.
	// We should set only two variables (
	// IgnoreErr and StrictSQLMode) to avoid setting the same bool variables and
	// pushing them down to EinsteinDB as flags.
	switch stmt := s.(type) {
	case *ast.UpdateStmt:
		ResetUpdateStmtCtx(sc, stmt, vars)
	case *ast.DeleteStmt:
		sc.InDeleteStmt = true
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.InsertStmt:
		sc.InInsertStmt = true
		// For insert statement (not for update statement), disabling the StrictSQLMode
		// should make TruncateAsWarning and DividedByZeroAsWarning,
		// but should not make DupKeyAsWarning or BadNullAsWarning,
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		// Make sure the sql_mode is strict when checking column default value.
	case *ast.LoadDataStmt:
		sc.DupKeyAsWarning = true
		sc.BadNullAsWarning = true
		sc.TruncateAsWarning = !vars.StrictSQLMode
		sc.InLoadDataStmt = true
	case *ast.SelectStmt:
		sc.InSelectStmt = true

		// see https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sql-mode-strict
		// said "For statements such as SELECT that do not change data, invalid values
		// generate a warning in strict mode, not an error."
		// and https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
		sc.OverflowAsWarning = true

		// Return warning for truncate error in selection.
		sc.TruncateAsWarning = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		if opts := stmt.SelectStmtOpts; opts != nil {
			sc.Priority = opts.Priority
			sc.NotFillCache = !opts.SQLCache
		}
	case *ast.UnionStmt:
		sc.InSelectStmt = true
		sc.OverflowAsWarning = true
		sc.TruncateAsWarning = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	case *ast.ShowStmt:
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		if stmt.Tp == ast.ShowWarnings || stmt.Tp == ast.ShowErrors {
			sc.InShowWarning = true
			sc.SetWarnings(vars.StmtCtx.GetWarnings())
		}
	case *ast.SplitRegionStmt:
		sc.IgnoreTruncate = false
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	default:
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	}
	vars.PreparedParams = vars.PreparedParams[:0]
	if priority := mysql.PriorityEnum(atomic.LoadInt32(&variable.ForcePriority)); priority != mysql.NoPriority {
		sc.Priority = priority
	}
	if vars.StmtCtx.LastInsertID > 0 {
		sc.PrevLastInsertID = vars.StmtCtx.LastInsertID
	} else {
		sc.PrevLastInsertID = vars.StmtCtx.PrevLastInsertID
	}
	sc.PrevAffectedRows = 0
	if vars.StmtCtx.InUpdateStmt || vars.StmtCtx.InDeleteStmt || vars.StmtCtx.InInsertStmt {
		sc.PrevAffectedRows = int64(vars.StmtCtx.AffectedRows())
	} else if vars.StmtCtx.InSelectStmt {
		sc.PrevAffectedRows = -1
	}
	sc.TblInfo2UnionScan = make(map[*serial.TableInfo]bool)
	errCount, warnCount := vars.StmtCtx.NumErrorWarnings()
	vars.SysErrorCount = errCount
	vars.SysWarningCount = warnCount
	vars.StmtCtx = sc
	vars.PrevFoundInPlanCache = vars.FoundInPlanCache
	vars.FoundInPlanCache = false
	return
}

// ResetUpdateStmtCtx resets statement context for UpdateStmt.
func ResetUpdateStmtCtx(sc *stmtctx.StatementContext, stmt *ast.UpdateStmt, vars *variable.CausetNetVars) {
	sc.InUpdateStmt = true
	sc.DupKeyAsWarning = stmt.IgnoreErr
	sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
	sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
	sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
	sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
	sc.Priority = stmt.Priority
}

// FillVirtualColumnValue will calculate the virtual column value by evaluating generated
// expression using rows from a chunk, and then fill this value into the chunk
func FillVirtualColumnValue(virtualRetTypes []*types.FieldType, virtualColumnIndex []int,
	schema *expression.Schema, columns []*serial.ColumnInfo, sctx causetnetctx.Context, req *chunk.Chunk) error {
	virCols := chunk.NewChunkWithCapacity(virtualRetTypes, req.Capacity())
	iteron := chunk.NewIterator4Chunk(req)
	for i, idx := range virtualColumnIndex {
		for row := iteron.Begin(); row != iteron.End(); row = iteron.Next() {
			datum, err := schema.Columns[idx].EvalVirtualColumn(row)
			if err != nil {
				return err
			}
			// Because the expression might return different type from
			// the generated column, we should wrap a CAST on the result.
			castCausetObjectQL, err := table.CastValue(sctx, datum, columns[idx], false, true)
			if err != nil {
				return err
			}
			virCols.AppendCausetObjectQL(i, &castCausetObjectQL)
		}
		req.SetCol(idx, virCols.Column(i))
	}
	return nil
}
