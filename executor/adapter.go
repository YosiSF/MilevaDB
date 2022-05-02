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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/plugin"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/plancodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/sqlexec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/stmtsummary"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/stringutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// processinfoSetter is the interface use to set current running process info.
type processinfoSetter interface {
	SetProcessInfo(string, time.Time, byte, uint64)
}

// recordSet wraps an executor, implements sqlexec.RecordSet interface
type recordSet struct {
	fields     []*ast.ResultField
	executor   Executor
	stmt       *ExecStmt
	lastErr    error
	txnStartTS uint64
}

func (a *recordSet) Fields() []*ast.ResultField {
	if len(a.fields) == 0 {
		a.fields = defCausNames2ResultFields(a.executor.Schema(), a.stmt.OutputNames, a.stmt.Ctx.GetStochaseinstein_dbars().CurrentDB)
	}
	return a.fields
}

func defCausNames2ResultFields(schemaReplicant *expression.Schema, names []*types.FieldName, defaultDB string) []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, schemaReplicant.Len())
	defaultDBCIStr := perceptron.NewCIStr(defaultDB)
	for i := 0; i < schemaReplicant.Len(); i++ {
		dbName := names[i].DBName
		if dbName.L == "" && names[i].TblName.L != "" {
			dbName = defaultDBCIStr
		}
		origDefCausName := names[i].OrigDefCausName
		if origDefCausName.L == "" {
			origDefCausName = names[i].DefCausName
		}
		rf := &ast.ResultField{
			DeferredCauset:       &perceptron.DeferredCausetInfo{Name: origDefCausName, FieldType: *schemaReplicant.DeferredCausets[i].RetType},
			DeferredCausetAsName: names[i].DefCausName,
			Block:                &perceptron.BlockInfo{Name: names[i].OrigTblName},
			BlockAsName:          names[i].TblName,
			DBName:               dbName,
		}
		// This is for compatibility.
		// See issue https://github.com/whtcorpsinc/MilevaDB-Prod/issues/10513 .
		if len(rf.DeferredCausetAsName.O) > allegrosql.MaxAliasIdentifierLen {
			rf.DeferredCausetAsName.O = rf.DeferredCausetAsName.O[:allegrosql.MaxAliasIdentifierLen]
		}
		// Usually the length of O equals the length of L.
		// Add this len judgement to avoid panic.
		if len(rf.DeferredCausetAsName.L) > allegrosql.MaxAliasIdentifierLen {
			rf.DeferredCausetAsName.L = rf.DeferredCausetAsName.L[:allegrosql.MaxAliasIdentifierLen]
		}
		rfs = append(rfs, rf)
	}
	return rfs
}

// Next use uses recordSet's executor to get next available chunk for later usage.
// If chunk does not contain any rows, then we uFIDelate last query found rows in stochastik variable as current found rows.
// The reason we need uFIDelate is that chunk with 0 rows indicating we already finished current query, we need prepare for
// next query.
// If stmt is not nil and chunk with some rows inside, we simply uFIDelate last query found rows by the number of event in chunk.
func (a *recordSet) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute allegrosql panic", zap.String("allegrosql", a.stmt.GetTextToLog()), zap.Stack("stack"))
	}()

	err = Next(ctx, a.executor, req)
	if err != nil {
		a.lastErr = err
		return err
	}
	numEvents := req.NumEvents()
	if numEvents == 0 {
		if a.stmt != nil {
			a.stmt.Ctx.GetStochaseinstein_dbars().LastFoundEvents = a.stmt.Ctx.GetStochaseinstein_dbars().StmtCtx.FoundEvents()
		}
		return nil
	}
	if a.stmt != nil {
		a.stmt.Ctx.GetStochaseinstein_dbars().StmtCtx.AddFoundEvents(uint64(numEvents))
	}
	return nil
}

// NewChunk create a chunk base on top-level executor's newFirstChunk().
func (a *recordSet) NewChunk() *chunk.Chunk {
	return newFirstChunk(a.executor)
}

func (a *recordSet) Close() error {
	err := a.executor.Close()
	a.stmt.CloseRecordSet(a.txnStartTS, a.lastErr)
	return err
}

// OnFetchReturned implements commandLifeCycle#OnFetchReturned
func (a *recordSet) OnFetchReturned() {
	a.stmt.LogSlowQuery(a.txnStartTS, a.lastErr == nil, true)
}

// ExecStmt implements the sqlexec.Statement interface, it builds a planner.Plan to an sqlexec.Statement.
type ExecStmt struct {
	// GoCtx stores parent go context.Context for a stmt.
	GoCtx context.Context
	// SchemaReplicant stores a reference to the schemaReplicant information.
	SchemaReplicant schemareplicant.SchemaReplicant
	// Plan stores a reference to the final physical plan.
	Plan plannercore.Plan
	// Text represents the origin query text.
	Text string

	StmtNode ast.StmtNode

	Ctx stochastikctx.Context

	// LowerPriority represents whether to lower the execution priority of a query.
	LowerPriority        bool
	isPreparedStmt       bool
	isSelectForUFIDelate bool
	retryCount           uint
	retryStartTime       time.Time

	// OutputNames will be set if using cached plan
	OutputNames []*types.FieldName
	PsStmt      *plannercore.CachedPrepareStmt
}

// PointGet short path for point exec directly from plan, keep only necessary steps
func (a *ExecStmt) PointGet(ctx context.Context, is schemareplicant.SchemaReplicant) (*recordSet, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("ExecStmt.PointGet", opentracing.ChildOf(span.Context()))
		span1.LogKV("allegrosql", a.OriginText())
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	startTs := uint64(math.MaxUint64)
	err := a.Ctx.InitTxnWithStartTS(startTs)
	if err != nil {
		return nil, err
	}
	a.Ctx.GetStochaseinstein_dbars().StmtCtx.Priority = ekv.PriorityHigh

	// try to reuse point get executor
	if a.PsStmt.Executor != nil {
		exec, ok := a.PsStmt.Executor.(*PointGetExecutor)
		if !ok {
			logutil.Logger(ctx).Error("invalid executor type, not PointGetExecutor for point get path")
			a.PsStmt.Executor = nil
		} else {
			// CachedPlan type is already checked in last step
			pointGetPlan := a.PsStmt.PreparedAst.CachedPlan.(*plannercore.PointGetPlan)
			exec.Init(pointGetPlan, startTs)
			a.PsStmt.Executor = exec
		}
	}
	if a.PsStmt.Executor == nil {
		b := newExecutorBuilder(a.Ctx, is)
		newExecutor := b.build(a.Plan)
		if b.err != nil {
			return nil, b.err
		}
		a.PsStmt.Executor = newExecutor
	}
	pointExecutor := a.PsStmt.Executor.(*PointGetExecutor)
	if err = pointExecutor.Open(ctx); err != nil {
		terror.Call(pointExecutor.Close)
		return nil, err
	}
	return &recordSet{
		executor:   pointExecutor,
		stmt:       a,
		txnStartTS: startTs,
	}, nil
}

// OriginText returns original statement as a string.
func (a *ExecStmt) OriginText() string {
	return a.Text
}

// IsPrepared returns true if stmt is a prepare statement.
func (a *ExecStmt) IsPrepared() bool {
	return a.isPreparedStmt
}

// IsReadOnly returns true if a statement is read only.
// If current StmtNode is an ExecuteStmt, we can get its prepared stmt,
// then using ast.IsReadOnly function to determine a statement is read only or not.
func (a *ExecStmt) IsReadOnly(vars *variable.Stochaseinstein_dbars) bool {
	return planner.IsReadOnly(a.StmtNode, vars)
}

// RebuildPlan rebuilds current execute statement plan.
// It returns the current information schemaReplicant version that 'a' is using.
func (a *ExecStmt) RebuildPlan(ctx context.Context) (int64, error) {
	is := schemareplicant.GetSchemaReplicant(a.Ctx)
	a.SchemaReplicant = is
	if err := plannercore.Preprocess(a.Ctx, a.StmtNode, is, plannercore.InTxnRetry); err != nil {
		return 0, err
	}
	p, names, err := planner.Optimize(ctx, a.Ctx, a.StmtNode, is)
	if err != nil {
		return 0, err
	}
	a.OutputNames = names
	a.Plan = p
	return is.SchemaMetaVersion(), nil
}

// Exec builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UFIDelATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned sqlexec.RecordSet Next method.
func (a *ExecStmt) Exec(ctx context.Context) (_ sqlexec.RecordSet, err error) {
	defer func() {
		r := recover()
		if r == nil {
			if a.retryCount > 0 {
				metrics.StatementPessimisticRetryCount.Observe(float64(a.retryCount))
			}
			lockKeysCnt := a.Ctx.GetStochaseinstein_dbars().StmtCtx.LockKeysCount
			if lockKeysCnt > 0 {
				metrics.StatementLockKeysCount.Observe(float64(lockKeysCnt))
			}
			return
		}
		if str, ok := r.(string); !ok || !strings.HasPrefix(str, memory.PanicMemoryExceed) {
			panic(r)
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute allegrosql panic", zap.String("allegrosql", a.GetTextToLog()), zap.Stack("stack"))
	}()

	sctx := a.Ctx
	ctx = stochastikctx.SetCommitCtx(ctx, sctx)
	if _, ok := a.Plan.(*plannercore.Analyze); ok && sctx.GetStochaseinstein_dbars().InRestrictedALLEGROSQL {
		oriStats, _ := sctx.GetStochaseinstein_dbars().GetSystemVar(variable.MilevaDBBuildStatsConcurrency)
		oriScan := sctx.GetStochaseinstein_dbars().DistALLEGROSQLScanConcurrency()
		oriIndex := sctx.GetStochaseinstein_dbars().IndexSerialScanConcurrency()
		oriIso, _ := sctx.GetStochaseinstein_dbars().GetSystemVar(variable.TxnIsolation)
		terror.Log(sctx.GetStochaseinstein_dbars().SetSystemVar(variable.MilevaDBBuildStatsConcurrency, "1"))
		sctx.GetStochaseinstein_dbars().SetDistALLEGROSQLScanConcurrency(1)
		sctx.GetStochaseinstein_dbars().SetIndexSerialScanConcurrency(1)
		terror.Log(sctx.GetStochaseinstein_dbars().SetSystemVar(variable.TxnIsolation, ast.ReadCommitted))
		defer func() {
			terror.Log(sctx.GetStochaseinstein_dbars().SetSystemVar(variable.MilevaDBBuildStatsConcurrency, oriStats))
			sctx.GetStochaseinstein_dbars().SetDistALLEGROSQLScanConcurrency(oriScan)
			sctx.GetStochaseinstein_dbars().SetIndexSerialScanConcurrency(oriIndex)
			terror.Log(sctx.GetStochaseinstein_dbars().SetSystemVar(variable.TxnIsolation, oriIso))
		}()
	}

	if sctx.GetStochaseinstein_dbars().StmtCtx.HasMemQuotaHint {
		sctx.GetStochaseinstein_dbars().StmtCtx.MemTracker.SetBytesLimit(sctx.GetStochaseinstein_dbars().StmtCtx.MemQuotaQuery)
	}

	e, err := a.buildExecutor()
	if err != nil {
		return nil, err
	}

	if err = e.Open(ctx); err != nil {
		terror.Call(e.Close)
		return nil, err
	}

	cmd32 := atomic.LoadUint32(&sctx.GetStochaseinstein_dbars().CommandValue)
	cmd := byte(cmd32)
	var pi processinfoSetter
	if raw, ok := sctx.(processinfoSetter); ok {
		pi = raw
		allegrosql := a.OriginText()
		if simple, ok := a.Plan.(*plannercore.Simple); ok && simple.Statement != nil {
			if ss, ok := simple.Statement.(ast.SensitiveStmtNode); ok {
				// Use SecureText to avoid leak password information.
				allegrosql = ss.SecureText()
			}
		}
		maxExecutionTime := getMaxExecutionTime(sctx)
		// UFIDelate processinfo, ShowProcess() will use it.
		pi.SetProcessInfo(allegrosql, time.Now(), cmd, maxExecutionTime)
		if a.Ctx.GetStochaseinstein_dbars().StmtCtx.StmtType == "" {
			a.Ctx.GetStochaseinstein_dbars().StmtCtx.StmtType = GetStmtLabel(a.StmtNode)
		}
	}

	isPessimistic := sctx.GetStochaseinstein_dbars().TxnCtx.IsPessimistic

	// Special handle for "select for uFIDelate statement" in pessimistic transaction.
	if isPessimistic && a.isSelectForUFIDelate {
		return a.handlePessimisticSelectForUFIDelate(ctx, e)
	}

	if handled, result, err := a.handleNoDelay(ctx, e, isPessimistic); handled {
		return result, err
	}

	var txnStartTS uint64
	txn, err := sctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn.Valid() {
		txnStartTS = txn.StartTS()
	}
	return &recordSet{
		executor:   e,
		stmt:       a,
		txnStartTS: txnStartTS,
	}, nil
}

func (a *ExecStmt) handleNoDelay(ctx context.Context, e Executor, isPessimistic bool) (handled bool, rs sqlexec.RecordSet, err error) {
	sc := a.Ctx.GetStochaseinstein_dbars().StmtCtx
	defer func() {
		// If the stmt have no rs like `insert`, The stochastik tracker detachment will be directly
		// done in the `defer` function. If the rs is not nil, the detachment will be done in
		// `rs.Close` in `handleStmt`
		if sc != nil && rs == nil {
			if sc.MemTracker != nil {
				sc.MemTracker.DetachFromGlobalTracker()
			}
			if sc.DiskTracker != nil {
				sc.DiskTracker.DetachFromGlobalTracker()
			}
		}
	}()

	toCheck := e
	if explain, ok := e.(*ExplainExec); ok {
		if explain.analyzeExec != nil {
			toCheck = explain.analyzeExec
		}
	}

	// If the executor doesn't return any result to the client, we execute it without delay.
	if toCheck.Schema().Len() == 0 {
		if isPessimistic {
			return true, nil, a.handlePessimisticDML(ctx, e)
		}
		r, err := a.handleNoDelayExecutor(ctx, e)
		return true, r, err
	} else if proj, ok := toCheck.(*ProjectionExec); ok && proj.calculateNoDelay {
		// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
		// the Projection has two expressions and two defCausumns in the schemaReplicant, but we should
		// not return the result of the two expressions.
		r, err := a.handleNoDelayExecutor(ctx, e)
		return true, r, err
	}

	return false, nil, nil
}

// getMaxExecutionTime get the max execution timeout value.
func getMaxExecutionTime(sctx stochastikctx.Context) uint64 {
	if sctx.GetStochaseinstein_dbars().StmtCtx.HasMaxExecutionTime {
		return sctx.GetStochaseinstein_dbars().StmtCtx.MaxExecutionTime
	}
	return sctx.GetStochaseinstein_dbars().MaxExecutionTime
}

type chunkEventRecordSet struct {
	rows     []chunk.Event
	idx      int
	fields   []*ast.ResultField
	e        Executor
	execStmt *ExecStmt
}

func (c *chunkEventRecordSet) Fields() []*ast.ResultField {
	return c.fields
}

func (c *chunkEventRecordSet) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for !chk.IsFull() && c.idx < len(c.rows) {
		chk.AppendEvent(c.rows[c.idx])
		c.idx++
	}
	return nil
}

func (c *chunkEventRecordSet) NewChunk() *chunk.Chunk {
	return newFirstChunk(c.e)
}

func (c *chunkEventRecordSet) Close() error {
	c.execStmt.CloseRecordSet(c.execStmt.Ctx.GetStochaseinstein_dbars().TxnCtx.StartTS, nil)
	return nil
}

func (a *ExecStmt) handlePessimisticSelectForUFIDelate(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	for {
		rs, err := a.runPessimisticSelectForUFIDelate(ctx, e)
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			return nil, err
		}
		if e == nil {
			return rs, nil
		}
	}
}

func (a *ExecStmt) runPessimisticSelectForUFIDelate(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	defer func() {
		terror.Log(e.Close())
	}()
	var rows []chunk.Event
	var err error
	req := newFirstChunk(e)
	for {
		err = Next(ctx, e, req)
		if err != nil {
			// Handle 'write conflict' error.
			break
		}
		if req.NumEvents() == 0 {
			fields := defCausNames2ResultFields(e.Schema(), a.OutputNames, a.Ctx.GetStochaseinstein_dbars().CurrentDB)
			return &chunkEventRecordSet{rows: rows, fields: fields, e: e, execStmt: a}, nil
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, a.Ctx.GetStochaseinstein_dbars().MaxChunkSize)
	}
	return nil, err
}

func (a *ExecStmt) handleNoDelayExecutor(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	sctx := a.Ctx
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("executor.handleNoDelayExecutor", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Check if "milevadb_snapshot" is set for the write executors.
	// In history read mode, we can not do write operations.
	switch e.(type) {
	case *DeleteExec, *InsertExec, *UFIDelateExec, *ReplaceExec, *LoadDataExec, *DBSExec:
		snapshotTS := sctx.GetStochaseinstein_dbars().SnapshotTS
		if snapshotTS != 0 {
			return nil, errors.New("can not execute write statement when 'milevadb_snapshot' is set")
		}
		lowResolutionTSO := sctx.GetStochaseinstein_dbars().LowResolutionTSO
		if lowResolutionTSO {
			return nil, errors.New("can not execute write statement when 'milevadb_low_resolution_tso' is set")
		}
	}

	var err error
	defer func() {
		terror.Log(e.Close())
		a.logAudit()
	}()

	err = Next(ctx, e, newFirstChunk(e))
	if err != nil {
		return nil, err
	}
	return nil, err
}

func (a *ExecStmt) handlePessimisticDML(ctx context.Context, e Executor) error {
	sctx := a.Ctx
	// Do not active the transaction here.
	// When autocommit = 0 and transaction in pessimistic mode,
	// statements like set xxx = xxx; should not active the transaction.
	txn, err := sctx.Txn(false)
	if err != nil {
		return err
	}
	txnCtx := sctx.GetStochaseinstein_dbars().TxnCtx
	for {
		startPointGetLocking := time.Now()
		_, err = a.handleNoDelayExecutor(ctx, e)
		if !txn.Valid() {
			return err
		}
		if err != nil {
			// It is possible the DML has point get plan that locks the key.
			e, err = a.handlePessimisticLockError(ctx, err)
			if err != nil {
				if ErrDeadlock.Equal(err) {
					metrics.StatementDeadlockDetectDuration.Observe(time.Since(startPointGetLocking).Seconds())
				}
				return err
			}
			continue
		}
		keys, err1 := txn.(pessimisticTxn).KeysNeedToLock()
		if err1 != nil {
			return err1
		}
		keys = txnCtx.DefCauslectUnchangedEventKeys(keys)
		if len(keys) == 0 {
			return nil
		}
		seVars := sctx.GetStochaseinstein_dbars()
		lockCtx := newLockCtx(seVars, seVars.LockWaitTimeout)
		var lockKeyStats *execdetails.LockKeysDetails
		ctx = context.WithValue(ctx, execdetails.LockKeysDetailCtxKey, &lockKeyStats)
		startLocking := time.Now()
		err = txn.LockKeys(ctx, lockCtx, keys...)
		if lockKeyStats != nil {
			seVars.StmtCtx.MergeLockKeysExecDetails(lockKeyStats)
		}
		if err == nil {
			return nil
		}
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			if ErrDeadlock.Equal(err) {
				metrics.StatementDeadlockDetectDuration.Observe(time.Since(startLocking).Seconds())
			}
			return err
		}
	}
}

// UFIDelateForUFIDelateTS uFIDelates the ForUFIDelateTS, if newForUFIDelateTS is 0, it obtain a new TS from FIDel.
func UFIDelateForUFIDelateTS(seCtx stochastikctx.Context, newForUFIDelateTS uint64) error {
	txn, err := seCtx.Txn(false)
	if err != nil {
		return err
	}
	if !txn.Valid() {
		return errors.Trace(ekv.ErrInvalidTxn)
	}
	if newForUFIDelateTS == 0 {
		version, err := seCtx.GetStore().CurrentVersion()
		if err != nil {
			return err
		}
		newForUFIDelateTS = version.Ver
	}
	seCtx.GetStochaseinstein_dbars().TxnCtx.SetForUFIDelateTS(newForUFIDelateTS)
	txn.SetOption(ekv.SnapshotTS, seCtx.GetStochaseinstein_dbars().TxnCtx.GetForUFIDelateTS())
	return nil
}

// handlePessimisticLockError uFIDelates TS and rebuild executor if the err is write conflict.
func (a *ExecStmt) handlePessimisticLockError(ctx context.Context, err error) (Executor, error) {
	txnCtx := a.Ctx.GetStochaseinstein_dbars().TxnCtx
	var newForUFIDelateTS uint64
	if deadlock, ok := errors.Cause(err).(*einsteindb.ErrDeadlock); ok {
		if !deadlock.IsRetryable {
			return nil, ErrDeadlock
		}
		logutil.Logger(ctx).Info("single statement deadlock, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("lockTS", deadlock.LockTs),
			zap.Stringer("lockKey", ekv.Key(deadlock.LockKey)),
			zap.Uint64("deadlockKeyHash", deadlock.DeadlockKeyHash))
	} else if terror.ErrorEqual(ekv.ErrWriteConflict, err) {
		errStr := err.Error()
		forUFIDelateTS := txnCtx.GetForUFIDelateTS()
		logutil.Logger(ctx).Debug("pessimistic write conflict, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("forUFIDelateTS", forUFIDelateTS),
			zap.String("err", errStr))
		// Always uFIDelate forUFIDelateTS by getting a new timestamp from FIDel.
		// If we use the conflict commitTS as the new forUFIDelateTS and async commit
		// is used, the commitTS of this transaction may exceed the max timestamp
		// that FIDel allocates. Then, the change may be invisible to a new transaction,
		// which means linearizability is broken.
	} else {
		// this branch if err not nil, always uFIDelate forUFIDelateTS to avoid problem described below
		// for nowait, when ErrLock happened, ErrLockAcquireFailAndNoWaitSet will be returned, and in the same txn
		// the select for uFIDelateTs must be uFIDelated, otherwise there maybe rollback problem.
		// begin;  select for uFIDelate key1(here ErrLocked or other errors(or max_execution_time like soliton),
		//         key1 dagger not get and async rollback key1 is raised)
		//         select for uFIDelate key1 again(this time dagger succ(maybe dagger released by others))
		//         the async rollback operation rollbacked the dagger just acquired
		if err != nil {
			tsErr := UFIDelateForUFIDelateTS(a.Ctx, 0)
			if tsErr != nil {
				logutil.Logger(ctx).Warn("UFIDelateForUFIDelateTS failed", zap.Error(tsErr))
			}
		}
		return nil, err
	}
	if a.retryCount >= config.GetGlobalConfig().PessimisticTxn.MaxRetryCount {
		return nil, errors.New("pessimistic dagger retry limit reached")
	}
	a.retryCount++
	a.retryStartTime = time.Now()
	err = UFIDelateForUFIDelateTS(a.Ctx, newForUFIDelateTS)
	if err != nil {
		return nil, err
	}
	e, err := a.buildExecutor()
	if err != nil {
		return nil, err
	}
	// Rollback the statement change before retry it.
	a.Ctx.StmtRollback()
	a.Ctx.GetStochaseinstein_dbars().StmtCtx.ResetForRetry()

	if err = e.Open(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

func extractConflictCommitTS(errStr string) uint64 {
	strs := strings.Split(errStr, "conflictCommitTS=")
	if len(strs) != 2 {
		return 0
	}
	tsPart := strs[1]
	length := strings.IndexByte(tsPart, ',')
	if length < 0 {
		return 0
	}
	tsStr := tsPart[:length]
	ts, err := strconv.ParseUint(tsStr, 10, 64)
	if err != nil {
		return 0
	}
	return ts
}

type pessimisticTxn interface {
	ekv.Transaction
	// KeysNeedToLock returns the keys need to be locked.
	KeysNeedToLock() ([]ekv.Key, error)
}

// buildExecutor build a executor from plan, prepared statement may need additional procedure.
func (a *ExecStmt) buildExecutor() (Executor, error) {
	ctx := a.Ctx
	stmtCtx := ctx.GetStochaseinstein_dbars().StmtCtx
	if _, ok := a.Plan.(*plannercore.Execute); !ok {
		// Do not sync transaction for Execute statement, because the real optimization work is done in
		// "ExecuteExec.Build".
		useMaxTS, err := plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, a.Plan)
		if err != nil {
			return nil, err
		}
		if useMaxTS {
			logutil.BgLogger().Debug("init txnStartTS with MaxUint64", zap.Uint64("conn", ctx.GetStochaseinstein_dbars().ConnectionID), zap.String("text", a.Text))
			err = ctx.InitTxnWithStartTS(math.MaxUint64)
		} else if ctx.GetStochaseinstein_dbars().SnapshotTS != 0 {
			if _, ok := a.Plan.(*plannercore.CheckBlock); ok {
				err = ctx.InitTxnWithStartTS(ctx.GetStochaseinstein_dbars().SnapshotTS)
			}
		}
		if err != nil {
			return nil, err
		}

		if stmtPri := stmtCtx.Priority; stmtPri == allegrosql.NoPriority {
			switch {
			case useMaxTS:
				stmtCtx.Priority = ekv.PriorityHigh
			case a.LowerPriority:
				stmtCtx.Priority = ekv.PriorityLow
			}
		}
	}
	if _, ok := a.Plan.(*plannercore.Analyze); ok && ctx.GetStochaseinstein_dbars().InRestrictedALLEGROSQL {
		ctx.GetStochaseinstein_dbars().StmtCtx.Priority = ekv.PriorityLow
	}

	b := newExecutorBuilder(ctx, a.SchemaReplicant)
	e := b.build(a.Plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	// ExecuteExec is not a real Executor, we only use it to build another Executor from a prepared statement.
	if executorExec, ok := e.(*ExecuteExec); ok {
		err := executorExec.Build(b)
		if err != nil {
			return nil, err
		}
		a.Ctx.SetValue(stochastikctx.QueryString, executorExec.stmt.Text())
		a.OutputNames = executorExec.outputNames
		a.isPreparedStmt = true
		a.Plan = executorExec.plan
		if executorExec.lowerPriority {
			ctx.GetStochaseinstein_dbars().StmtCtx.Priority = ekv.PriorityLow
		}
		e = executorExec.stmtExec
	}
	a.isSelectForUFIDelate = b.hasLock && (!stmtCtx.InDeleteStmt && !stmtCtx.InUFIDelateStmt)
	return e, nil
}

// QueryReplacer replaces new line and tab for grep result including query string.
var QueryReplacer = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

func (a *ExecStmt) logAudit() {
	sessVars := a.Ctx.GetStochaseinstein_dbars()
	if sessVars.InRestrictedALLEGROSQL {
		return
	}
	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		audit := plugin.DeclareAuditManifest(p.Manifest)
		if audit.OnGeneralEvent != nil {
			cmd := allegrosql.Command2Str[byte(atomic.LoadUint32(&a.Ctx.GetStochaseinstein_dbars().CommandValue))]
			ctx := context.WithValue(context.Background(), plugin.ExecStartTimeCtxKey, a.Ctx.GetStochaseinstein_dbars().StartTime)
			audit.OnGeneralEvent(ctx, sessVars, plugin.Log, cmd)
		}
		return nil
	})
	if err != nil {
		log.Error("log audit log failure", zap.Error(err))
	}
}

// FormatALLEGROSQL is used to format the original ALLEGROALLEGROSQL, e.g. truncating long ALLEGROALLEGROSQL, appending prepared arguments.
func FormatALLEGROSQL(allegrosql string, pps variable.PreparedParams) stringutil.StringerFunc {
	return func() string {
		cfg := config.GetGlobalConfig()
		length := len(allegrosql)
		if maxQueryLen := atomic.LoadUint64(&cfg.Log.QueryLogMaxLen); uint64(length) > maxQueryLen {
			allegrosql = fmt.Sprintf("%.*q(len:%d)", maxQueryLen, allegrosql, length)
		}
		return QueryReplacer.Replace(allegrosql) + pps.String()
	}
}

var (
	stochastikExecuteRunDurationInternal = metrics.StochastikExecuteRunDuration.WithLabelValues(metrics.LblInternal)
	stochastikExecuteRunDurationGeneral  = metrics.StochastikExecuteRunDuration.WithLabelValues(metrics.LblGeneral)
)

// FinishExecuteStmt is used to record some information after `ExecStmt` execution finished:
// 1. record slow log if needed.
// 2. record summary statement.
// 3. record execute duration metric.
// 4. uFIDelate the `PrevStmt` in stochastik variable.
func (a *ExecStmt) FinishExecuteStmt(txnTS uint64, succ bool, hasMoreResults bool) {
	sessVars := a.Ctx.GetStochaseinstein_dbars()
	execDetail := sessVars.StmtCtx.GetExecDetails()
	// Attach commit/lockKeys runtime stats to executor runtime stats.
	if (execDetail.CommitDetail != nil || execDetail.LockKeysDetail != nil) && sessVars.StmtCtx.RuntimeStatsDefCausl != nil {
		statsWithCommit := &execdetails.RuntimeStatsWithCommit{
			Commit:   execDetail.CommitDetail,
			LockKeys: execDetail.LockKeysDetail,
		}
		sessVars.StmtCtx.RuntimeStatsDefCausl.RegisterStats(a.Plan.ID(), statsWithCommit)
	}
	// `LowSlowQuery` and `SummaryStmt` must be called before recording `PrevStmt`.
	a.LogSlowQuery(txnTS, succ, hasMoreResults)
	a.SummaryStmt(succ)
	prevStmt := a.GetTextToLog()
	if config.RedactLogEnabled() {
		sessVars.PrevStmt = FormatALLEGROSQL(prevStmt, nil)
	} else {
		pps := types.CloneEvent(sessVars.PreparedParams)
		sessVars.PrevStmt = FormatALLEGROSQL(prevStmt, pps)
	}

	executeDuration := time.Since(sessVars.StartTime) - sessVars.DurationCompile
	if sessVars.InRestrictedALLEGROSQL {
		stochastikExecuteRunDurationInternal.Observe(executeDuration.Seconds())
	} else {
		stochastikExecuteRunDurationGeneral.Observe(executeDuration.Seconds())
	}
}

// CloseRecordSet will finish the execution of current statement and do some record work
func (a *ExecStmt) CloseRecordSet(txnStartTS uint64, lastErr error) {
	a.FinishExecuteStmt(txnStartTS, lastErr == nil, false)
	a.logAudit()
	// Detach the Memory and disk tracker for the previous stmtCtx from GlobalMemoryUsageTracker and GlobalDiskUsageTracker
	if stmtCtx := a.Ctx.GetStochaseinstein_dbars().StmtCtx; stmtCtx != nil {
		if stmtCtx.DiskTracker != nil {
			stmtCtx.DiskTracker.DetachFromGlobalTracker()
		}
		if stmtCtx.MemTracker != nil {
			stmtCtx.MemTracker.DetachFromGlobalTracker()
		}
	}
}

// LogSlowQuery is used to print the slow query in the log files.
func (a *ExecStmt) LogSlowQuery(txnTS uint64, succ bool, hasMoreResults bool) {
	sessVars := a.Ctx.GetStochaseinstein_dbars()
	level := log.GetLevel()
	cfg := config.GetGlobalConfig()
	costTime := time.Since(sessVars.StartTime) + sessVars.DurationParse
	threshold := time.Duration(atomic.LoadUint64(&cfg.Log.SlowThreshold)) * time.Millisecond
	enable := cfg.Log.EnableSlowLog
	// if the level is Debug, print slow logs anyway
	if (!enable || costTime < threshold) && level > zapcore.DebugLevel {
		return
	}
	var allegrosql stringutil.StringerFunc
	normalizedALLEGROSQL, digest := sessVars.StmtCtx.ALLEGROSQLDigest()
	if config.RedactLogEnabled() {
		allegrosql = FormatALLEGROSQL(normalizedALLEGROSQL, nil)
	} else if sensitiveStmt, ok := a.StmtNode.(ast.SensitiveStmtNode); ok {
		allegrosql = FormatALLEGROSQL(sensitiveStmt.SecureText(), nil)
	} else {
		allegrosql = FormatALLEGROSQL(a.Text, sessVars.PreparedParams)
	}

	var blockIDs, indexNames string
	if len(sessVars.StmtCtx.BlockIDs) > 0 {
		blockIDs = strings.Replace(fmt.Sprintf("%v", sessVars.StmtCtx.BlockIDs), " ", ",", -1)
	}
	if len(sessVars.StmtCtx.IndexNames) > 0 {
		indexNames = strings.Replace(fmt.Sprintf("%v", sessVars.StmtCtx.IndexNames), " ", ",", -1)
	}
	var stmtDetail execdetails.StmtExecDetails
	stmtDetailRaw := a.GoCtx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*execdetails.StmtExecDetails))
	}
	execDetail := sessVars.StmtCtx.GetExecDetails()
	INTERLOCKTaskInfo := sessVars.StmtCtx.CausetTasksDetails()
	statsInfos := plannercore.GetStatsInfo(a.Plan)
	memMax := sessVars.StmtCtx.MemTracker.MaxConsumed()
	diskMax := sessVars.StmtCtx.DiskTracker.MaxConsumed()
	_, planDigest := getPlanDigest(a.Ctx, a.Plan)
	slowItems := &variable.SlowQueryLogItems{
		TxnTS:                    txnTS,
		ALLEGROALLEGROSQL:        allegrosql.String(),
		Digest:                   digest,
		TimeTotal:                costTime,
		TimeParse:                sessVars.DurationParse,
		TimeCompile:              sessVars.DurationCompile,
		TimeOptimize:             sessVars.DurationOptimization,
		TimeWaitTS:               sessVars.DurationWaitTS,
		IndexNames:               indexNames,
		StatsInfos:               statsInfos,
		CausetTasks:              INTERLOCKTaskInfo,
		ExecDetail:               execDetail,
		MemMax:                   memMax,
		DiskMax:                  diskMax,
		Succ:                     succ,
		Plan:                     getPlanTree(a.Plan),
		PlanDigest:               planDigest,
		Prepared:                 a.isPreparedStmt,
		HasMoreResults:           hasMoreResults,
		PlanFromCache:            sessVars.FoundInPlanCache,
		RewriteInfo:              sessVars.RewritePhaseInfo,
		KVTotal:                  time.Duration(atomic.LoadInt64(&stmtDetail.WaitKVResFIDeluration)),
		FIDelTotal:               time.Duration(atomic.LoadInt64(&stmtDetail.WaitFIDelResFIDeluration)),
		BackoffTotal:             time.Duration(atomic.LoadInt64(&stmtDetail.BackoffDuration)),
		WriteALLEGROSQLRespTotal: stmtDetail.WriteALLEGROSQLResFIDeluration,
		ExecRetryCount:           a.retryCount,
	}
	if a.retryCount > 0 {
		slowItems.ExecRetryTime = costTime - sessVars.DurationParse - sessVars.DurationCompile - time.Since(a.retryStartTime)
	}
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok {
		slowItems.PrevStmt = sessVars.PrevStmt.String()
	}
	if costTime < threshold {
		logutil.SlowQueryLogger.Debug(sessVars.SlowLogFormat(slowItems))
	} else {
		logutil.SlowQueryLogger.Warn(sessVars.SlowLogFormat(slowItems))
		metrics.TotalQueryProcHistogram.Observe(costTime.Seconds())
		metrics.TotalINTERLOCKProcHistogram.Observe(execDetail.ProcessTime.Seconds())
		metrics.TotalINTERLOCKWaitHistogram.Observe(execDetail.WaitTime.Seconds())
		var userString string
		if sessVars.User != nil {
			userString = sessVars.User.String()
		}
		petri.GetPetri(a.Ctx).LogSlowQuery(&petri.SlowQueryInfo{
			ALLEGROALLEGROSQL: allegrosql.String(),
			Digest:            digest,
			Start:             sessVars.StartTime,
			Duration:          costTime,
			Detail:            sessVars.StmtCtx.GetExecDetails(),
			Succ:              succ,
			ConnID:            sessVars.ConnectionID,
			TxnTS:             txnTS,
			User:              userString,
			EDB:               sessVars.CurrentDB,
			BlockIDs:          blockIDs,
			IndexNames:        indexNames,
			Internal:          sessVars.InRestrictedALLEGROSQL,
		})
	}
}

// getPlanTree will try to get the select plan tree if the plan is select or the select plan of delete/uFIDelate/insert statement.
func getPlanTree(p plannercore.Plan) string {
	cfg := config.GetGlobalConfig()
	if atomic.LoadUint32(&cfg.Log.RecordPlanInSlowLog) == 0 {
		return ""
	}
	planTree := plannercore.EncodePlan(p)
	if len(planTree) == 0 {
		return planTree
	}
	return variable.SlowLogPlanPrefix + planTree + variable.SlowLogPlanSuffix
}

// getPlanDigest will try to get the select plan tree if the plan is select or the select plan of delete/uFIDelate/insert statement.
func getPlanDigest(sctx stochastikctx.Context, p plannercore.Plan) (normalized, planDigest string) {
	normalized, planDigest = sctx.GetStochaseinstein_dbars().StmtCtx.GetPlanDigest()
	if len(normalized) > 0 {
		return
	}
	normalized, planDigest = plannercore.NormalizePlan(p)
	sctx.GetStochaseinstein_dbars().StmtCtx.SetPlanDigest(normalized, planDigest)
	return
}

// SummaryStmt defCauslects statements for information_schema.statements_summary
func (a *ExecStmt) SummaryStmt(succ bool) {
	sessVars := a.Ctx.GetStochaseinstein_dbars()
	var userString string
	if sessVars.User != nil {
		userString = sessVars.User.Username
	}

	// Internal ALLEGROSQLs must also be recorded to keep the consistency of `PrevStmt` and `PrevStmtDigest`.
	if !stmtsummary.StmtSummaryByDigestMap.Enabled() || ((sessVars.InRestrictedALLEGROSQL || len(userString) == 0) && !stmtsummary.StmtSummaryByDigestMap.EnabledInternal()) {
		sessVars.SetPrevStmtDigest("")
		return
	}
	// Ignore `PREPARE` statements, but record `EXECUTE` statements.
	if _, ok := a.StmtNode.(*ast.PrepareStmt); ok {
		return
	}
	stmtCtx := sessVars.StmtCtx
	normalizedALLEGROSQL, digest := stmtCtx.ALLEGROSQLDigest()
	costTime := time.Since(sessVars.StartTime) + sessVars.DurationParse

	var prevALLEGROSQL, prevALLEGROSQLDigest string
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok {
		// If prevALLEGROSQLDigest is not recorded, it means this `commit` is the first ALLEGROALLEGROSQL once stmt summary is enabled,
		// so it's OK just to ignore it.
		if prevALLEGROSQLDigest = sessVars.GetPrevStmtDigest(); len(prevALLEGROSQLDigest) == 0 {
			return
		}
		prevALLEGROSQL = sessVars.PrevStmt.String()
	}
	sessVars.SetPrevStmtDigest(digest)

	// No need to encode every time, so encode lazily.
	planGenerator := func() string {
		return plannercore.EncodePlan(a.Plan)
	}
	// Generating plan digest is slow, only generate it once if it's 'Point_Get'.
	// If it's a point get, different ALLEGROSQLs leads to different plans, so ALLEGROALLEGROSQL digest
	// is enough to distinguish different plans in this case.
	var planDigest string
	var planDigestGen func() string
	if a.Plan.TP() == plancodec.TypePointGet {
		planDigestGen = func() string {
			_, planDigest := getPlanDigest(a.Ctx, a.Plan)
			return planDigest
		}
	} else {
		_, planDigest = getPlanDigest(a.Ctx, a.Plan)
	}

	execDetail := stmtCtx.GetExecDetails()
	INTERLOCKTaskInfo := stmtCtx.CausetTasksDetails()
	memMax := stmtCtx.MemTracker.MaxConsumed()
	diskMax := stmtCtx.DiskTracker.MaxConsumed()
	allegrosql := a.GetTextToLog()
	stmtExecInfo := &stmtsummary.StmtExecInfo{
		SchemaName:           strings.ToLower(sessVars.CurrentDB),
		OriginalALLEGROSQL:   allegrosql,
		NormalizedALLEGROSQL: normalizedALLEGROSQL,
		Digest:               digest,
		PrevALLEGROSQL:       prevALLEGROSQL,
		PrevALLEGROSQLDigest: prevALLEGROSQLDigest,
		PlanGenerator:        planGenerator,
		PlanDigest:           planDigest,
		PlanDigestGen:        planDigestGen,
		User:                 userString,
		TotalLatency:         costTime,
		ParseLatency:         sessVars.DurationParse,
		CompileLatency:       sessVars.DurationCompile,
		StmtCtx:              stmtCtx,
		CausetTasks:          INTERLOCKTaskInfo,
		ExecDetail:           &execDetail,
		MemMax:               memMax,
		DiskMax:              diskMax,
		StartTime:            sessVars.StartTime,
		IsInternal:           sessVars.InRestrictedALLEGROSQL,
		Succeed:              succ,
		PlanInCache:          sessVars.FoundInPlanCache,
		ExecRetryCount:       a.retryCount,
	}
	if a.retryCount > 0 {
		stmtExecInfo.ExecRetryTime = costTime - sessVars.DurationParse - sessVars.DurationCompile - time.Since(a.retryStartTime)
	}
	stmtsummary.StmtSummaryByDigestMap.AddStatement(stmtExecInfo)
}

// GetTextToLog return the query text to log.
func (a *ExecStmt) GetTextToLog() string {
	var allegrosql string
	if config.RedactLogEnabled() {
		allegrosql, _ = a.Ctx.GetStochaseinstein_dbars().StmtCtx.ALLEGROSQLDigest()
	} else if sensitiveStmt, ok := a.StmtNode.(ast.SensitiveStmtNode); ok {
		allegrosql = sensitiveStmt.SecureText()
	} else {
		allegrosql = a.Text
	}
	return allegrosql
}
