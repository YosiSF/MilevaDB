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
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/distsql"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/executor/aggfuncs"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression/aggregation"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	plannerutil "github.com/whtcorpsinc/MilevaDB-Prod/planner/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/admin"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/ranger"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/rowcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/timeutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/ekvproto/pkg/diagnosticspb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"go.uber.org/zap"
)

var (
	executorCounterMergeJoinExec            = metrics.ExecutorCounter.WithLabelValues("MergeJoinExec")
	executorCountHashJoinExec               = metrics.ExecutorCounter.WithLabelValues("HashJoinExec")
	executorCounterHashAggExec              = metrics.ExecutorCounter.WithLabelValues("HashAggExec")
	executorStreamAggExec                   = metrics.ExecutorCounter.WithLabelValues("StreamAggExec")
	executorCounterSortExec                 = metrics.ExecutorCounter.WithLabelValues("SortExec")
	executorCounterTopNExec                 = metrics.ExecutorCounter.WithLabelValues("TopNExec")
	executorCounterNestedLoopApplyExec      = metrics.ExecutorCounter.WithLabelValues("NestedLoopApplyExec")
	executorCounterIndexLookUpJoin          = metrics.ExecutorCounter.WithLabelValues("IndexLookUpJoin")
	executorCounterIndexLookUpExecutor      = metrics.ExecutorCounter.WithLabelValues("IndexLookUpExecutor")
	executorCounterIndexMergeReaderExecutor = metrics.ExecutorCounter.WithLabelValues("IndexMergeReaderExecutor")
)

// executorBuilder builds an Executor from a Plan.
// The SchemaReplicant must not change during execution.
type executorBuilder struct {
	ctx        stochastikctx.Context
	is         schemareplicant.SchemaReplicant
	snapshotTS uint64 // The consistent snapshot timestamp for the executor to read data.
	err        error  // err is set when there is error happened during Executor building process.
	hasLock    bool
}

func newExecutorBuilder(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant) *executorBuilder {
	return &executorBuilder{
		ctx: ctx,
		is:  is,
	}
}

// MockPhysicalPlan is used to return a specified executor in when build.
// It is mainly used for testing.
type MockPhysicalPlan interface {
	plannercore.PhysicalPlan
	GetExecutor() Executor
}

func (b *executorBuilder) build(p plannercore.Plan) Executor {
	switch v := p.(type) {
	case nil:
		return nil
	case *plannercore.Change:
		return b.buildChange(v)
	case *plannercore.CheckBlock:
		return b.buildCheckBlock(v)
	case *plannercore.RecoverIndex:
		return b.buildRecoverIndex(v)
	case *plannercore.CleanupIndex:
		return b.buildCleanupIndex(v)
	case *plannercore.ChecHoTTexRange:
		return b.buildChecHoTTexRange(v)
	case *plannercore.ChecksumBlock:
		return b.buildChecksumBlock(v)
	case *plannercore.ReloadExprPushdownBlacklist:
		return b.buildReloadExprPushdownBlacklist(v)
	case *plannercore.ReloadOptMemruleBlacklist:
		return b.buildReloadOptMemruleBlacklist(v)
	case *plannercore.AdminPlugins:
		return b.buildAdminPlugins(v)
	case *plannercore.DBS:
		return b.buildDBS(v)
	case *plannercore.Deallocate:
		return b.buildDeallocate(v)
	case *plannercore.Delete:
		return b.buildDelete(v)
	case *plannercore.Execute:
		return b.buildExecute(v)
	case *plannercore.Trace:
		return b.buildTrace(v)
	case *plannercore.Explain:
		return b.buildExplain(v)
	case *plannercore.PointGetPlan:
		return b.buildPointGet(v)
	case *plannercore.BatchPointGetPlan:
		return b.buildBatchPointGet(v)
	case *plannercore.Insert:
		return b.buildInsert(v)
	case *plannercore.LoadData:
		return b.buildLoadData(v)
	case *plannercore.LoadStats:
		return b.buildLoadStats(v)
	case *plannercore.IndexAdvise:
		return b.buildIndexAdvise(v)
	case *plannercore.PhysicalLimit:
		return b.buildLimit(v)
	case *plannercore.Prepare:
		return b.buildPrepare(v)
	case *plannercore.PhysicalLock:
		return b.buildSelectLock(v)
	case *plannercore.CancelDBSJobs:
		return b.buildCancelDBSJobs(v)
	case *plannercore.ShowNextEventID:
		return b.buildShowNextEventID(v)
	case *plannercore.ShowDBS:
		return b.buildShowDBS(v)
	case *plannercore.PhysicalShowDBSJobs:
		return b.buildShowDBSJobs(v)
	case *plannercore.ShowDBSJobQueries:
		return b.buildShowDBSJobQueries(v)
	case *plannercore.ShowSlow:
		return b.buildShowSlow(v)
	case *plannercore.PhysicalShow:
		return b.buildShow(v)
	case *plannercore.Simple:
		return b.buildSimple(v)
	case *plannercore.Set:
		return b.buildSet(v)
	case *plannercore.SetConfig:
		return b.buildSetConfig(v)
	case *plannercore.PhysicalSort:
		return b.buildSort(v)
	case *plannercore.PhysicalTopN:
		return b.buildTopN(v)
	case *plannercore.PhysicalUnionAll:
		return b.buildUnionAll(v)
	case *plannercore.UFIDelate:
		return b.buildUFIDelate(v)
	case *plannercore.PhysicalUnionScan:
		return b.buildUnionScanExec(v)
	case *plannercore.PhysicalHashJoin:
		return b.buildHashJoin(v)
	case *plannercore.PhysicalMergeJoin:
		return b.buildMergeJoin(v)
	case *plannercore.PhysicalIndexJoin:
		return b.buildIndexLookUpJoin(v)
	case *plannercore.PhysicalIndexMergeJoin:
		return b.buildIndexLookUpMergeJoin(v)
	case *plannercore.PhysicalIndexHashJoin:
		return b.buildIndexNestedLoopHashJoin(v)
	case *plannercore.PhysicalSelection:
		return b.buildSelection(v)
	case *plannercore.PhysicalHashAgg:
		return b.buildHashAgg(v)
	case *plannercore.PhysicalStreamAgg:
		return b.buildStreamAgg(v)
	case *plannercore.PhysicalProjection:
		return b.buildProjection(v)
	case *plannercore.PhysicalMemBlock:
		return b.buildMemBlock(v)
	case *plannercore.PhysicalBlockDual:
		return b.buildBlockDual(v)
	case *plannercore.PhysicalApply:
		return b.buildApply(v)
	case *plannercore.PhysicalMaxOneEvent:
		return b.buildMaxOneEvent(v)
	case *plannercore.Analyze:
		return b.buildAnalyze(v)
	case *plannercore.PhysicalBlockReader:
		return b.buildBlockReader(v)
	case *plannercore.PhysicalIndexReader:
		return b.buildIndexReader(v)
	case *plannercore.PhysicalIndexLookUpReader:
		return b.buildIndexLookUpReader(v)
	case *plannercore.PhysicalWindow:
		return b.buildWindow(v)
	case *plannercore.PhysicalShuffle:
		return b.buildShuffle(v)
	case *plannercore.PhysicalShuffleDataSourceStub:
		return b.buildShuffleDataSourceStub(v)
	case *plannercore.ALLEGROSQLBindPlan:
		return b.buildALLEGROSQLBindExec(v)
	case *plannercore.SplitRegion:
		return b.buildSplitRegion(v)
	case *plannercore.PhysicalIndexMergeReader:
		return b.buildIndexMergeReader(v)
	case *plannercore.SelectInto:
		return b.buildSelectInto(v)
	case *plannercore.AdminShowTelemetry:
		return b.buildAdminShowTelemetry(v)
	case *plannercore.AdminResetTelemetryID:
		return b.buildAdminResetTelemetryID(v)
	default:
		if mp, ok := p.(MockPhysicalPlan); ok {
			return mp.GetExecutor()
		}

		b.err = ErrUnknownPlan.GenWithStack("Unknown Plan %T", p)
		return nil
	}
}

func (b *executorBuilder) buildCancelDBSJobs(v *plannercore.CancelDBSJobs) Executor {
	e := &CancelDBSJobsExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		jobIDs:       v.JobIDs,
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		b.err = err
		return nil
	}

	e.errs, b.err = admin.CancelJobs(txn, e.jobIDs)
	if b.err != nil {
		return nil
	}
	return e
}

func (b *executorBuilder) buildChange(v *plannercore.Change) Executor {
	return &ChangeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		ChangeStmt:   v.ChangeStmt,
	}
}

func (b *executorBuilder) buildShowNextEventID(v *plannercore.ShowNextEventID) Executor {
	e := &ShowNextEventIDExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tblName:      v.BlockName,
	}
	return e
}

func (b *executorBuilder) buildShowDBS(v *plannercore.ShowDBS) Executor {
	// We get DBSInfo here because for Executors that returns result set,
	// next will be called after transaction has been committed.
	// We need the transaction to get DBSInfo.
	e := &ShowDBSExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
	}

	var err error
	ownerManager := petri.GetPetri(e.ctx).DBS().OwnerManager()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	e.dbsOwnerID, err = ownerManager.GetOwnerID(ctx)
	cancel()
	if err != nil {
		b.err = err
		return nil
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		b.err = err
		return nil
	}

	dbsInfo, err := admin.GetDBSInfo(txn)
	if err != nil {
		b.err = err
		return nil
	}
	e.dbsInfo = dbsInfo
	e.selfID = ownerManager.ID()
	return e
}

func (b *executorBuilder) buildShowDBSJobs(v *plannercore.PhysicalShowDBSJobs) Executor {
	e := &ShowDBSJobsExec{
		jobNumber:    int(v.JobNumber),
		is:           b.is,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
	}
	return e
}

func (b *executorBuilder) buildShowDBSJobQueries(v *plannercore.ShowDBSJobQueries) Executor {
	e := &ShowDBSJobQueriesExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		jobIDs:       v.JobIDs,
	}
	return e
}

func (b *executorBuilder) buildShowSlow(v *plannercore.ShowSlow) Executor {
	e := &ShowSlowExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		ShowSlow:     v.ShowSlow,
	}
	return e
}

// buildIndexLookUpChecker builds check information to IndexLookUpReader.
func buildIndexLookUpChecker(b *executorBuilder, p *plannercore.PhysicalIndexLookUpReader,
	e *IndexLookUpExecutor) {
	is := p.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	fullDefCausLen := len(is.Index.DeferredCausets) + len(p.CommonHandleDefCauss)
	if !e.isCommonHandle() {
		fullDefCausLen += 1
	}
	e.posetPosetDagPB.OutputOffsets = make([]uint32, fullDefCausLen)
	for i := 0; i < fullDefCausLen; i++ {
		e.posetPosetDagPB.OutputOffsets[i] = uint32(i)
	}

	ts := p.BlockPlans[0].(*plannercore.PhysicalBlockScan)
	e.handleIdx = ts.HandleIdx

	e.ranges = ranger.FullRange()

	tps := make([]*types.FieldType, 0, fullDefCausLen)
	for _, defCaus := range is.DeferredCausets {
		tps = append(tps, &defCaus.FieldType)
	}

	if !e.isCommonHandle() {
		tps = append(tps, types.NewFieldType(allegrosql.TypeLonglong))
	}

	e.checHoTTexValue = &checHoTTexValue{idxDefCausTps: tps}

	defCausNames := make([]string, 0, len(is.IdxDefCauss))
	for i := range is.IdxDefCauss {
		defCausNames = append(defCausNames, is.DeferredCausets[i].Name.O)
	}
	if defcaus, missingDefCausName := block.FindDefCauss(e.block.DefCauss(), defCausNames, true); missingDefCausName != "" {
		b.err = plannercore.ErrUnknownDeferredCauset.GenWithStack("Unknown defCausumn %s", missingDefCausName)
	} else {
		e.idxTblDefCauss = defcaus
	}
}

func (b *executorBuilder) buildCheckBlock(v *plannercore.CheckBlock) Executor {
	readerExecs := make([]*IndexLookUpExecutor, 0, len(v.IndexLookUpReaders))
	for _, readerPlan := range v.IndexLookUpReaders {
		readerExec, err := buildNoRangeIndexLookUpReader(b, readerPlan)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		buildIndexLookUpChecker(b, readerPlan, readerExec)

		readerExecs = append(readerExecs, readerExec)
	}

	e := &CheckBlockExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		dbName:       v.DBName,
		block:        v.Block,
		indexInfos:   v.IndexInfos,
		is:           b.is,
		srcs:         readerExecs,
		exitCh:       make(chan struct{}),
		retCh:        make(chan error, len(readerExecs)),
		checHoTTex:   v.ChecHoTTex,
	}
	return e
}

func buildIdxDefCaussConcatHandleDefCauss(tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo) []*perceptron.DeferredCausetInfo {
	handleLen := 1
	var pkDefCauss []*perceptron.IndexDeferredCauset
	if tblInfo.IsCommonHandle {
		pkIdx := blocks.FindPrimaryIndex(tblInfo)
		pkDefCauss = pkIdx.DeferredCausets
		handleLen = len(pkIdx.DeferredCausets)
	}
	defCausumns := make([]*perceptron.DeferredCausetInfo, 0, len(indexInfo.DeferredCausets)+handleLen)
	for _, idxDefCaus := range indexInfo.DeferredCausets {
		defCausumns = append(defCausumns, tblInfo.DeferredCausets[idxDefCaus.Offset])
	}
	if tblInfo.IsCommonHandle {
		for _, c := range pkDefCauss {
			defCausumns = append(defCausumns, tblInfo.DeferredCausets[c.Offset])
		}
		return defCausumns
	}
	handleOffset := len(defCausumns)
	handleDefCaussInfo := &perceptron.DeferredCausetInfo{
		ID:     perceptron.ExtraHandleID,
		Name:   perceptron.ExtraHandleName,
		Offset: handleOffset,
	}
	handleDefCaussInfo.FieldType = *types.NewFieldType(allegrosql.TypeLonglong)
	defCausumns = append(defCausumns, handleDefCaussInfo)
	return defCausumns
}

func (b *executorBuilder) buildRecoverIndex(v *plannercore.RecoverIndex) Executor {
	tblInfo := v.Block.BlockInfo
	t, err := b.is.BlockByName(v.Block.Schema, tblInfo.Name)
	if err != nil {
		b.err = err
		return nil
	}
	idxName := strings.ToLower(v.IndexName)
	index := blocks.GetWriblockIndexByName(idxName, t)
	if index == nil {
		b.err = errors.Errorf("index `%v` is not found in block `%v`.", v.IndexName, v.Block.Name.O)
		return nil
	}
	e := &RecoverIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		defCausumns:  buildIdxDefCaussConcatHandleDefCauss(tblInfo, index.Meta()),
		index:        index,
		block:        t,
		physicalID:   t.Meta().ID,
	}
	sessCtx := e.ctx.GetStochaseinstein_dbars().StmtCtx
	e.handleDefCauss = buildHandleDefCaussForExec(sessCtx, tblInfo, index.Meta(), e.defCausumns)
	return e
}

func buildHandleDefCaussForExec(sctx *stmtctx.StatementContext, tblInfo *perceptron.BlockInfo,
	idxInfo *perceptron.IndexInfo, allDefCausInfo []*perceptron.DeferredCausetInfo) plannercore.HandleDefCauss {
	if !tblInfo.IsCommonHandle {
		extraDefCausPos := len(allDefCausInfo) - 1
		intDefCaus := &expression.DeferredCauset{
			Index:   extraDefCausPos,
			RetType: types.NewFieldType(allegrosql.TypeLonglong),
		}
		return plannercore.NewIntHandleDefCauss(intDefCaus)
	}
	tblDefCauss := make([]*expression.DeferredCauset, len(tblInfo.DeferredCausets))
	for i := 0; i < len(tblInfo.DeferredCausets); i++ {
		c := tblInfo.DeferredCausets[i]
		tblDefCauss[i] = &expression.DeferredCauset{
			RetType: &c.FieldType,
			ID:      c.ID,
		}
	}
	pkIdx := blocks.FindPrimaryIndex(tblInfo)
	for i, c := range pkIdx.DeferredCausets {
		tblDefCauss[c.Offset].Index = len(idxInfo.DeferredCausets) + i
	}
	return plannercore.NewCommonHandleDefCauss(sctx, tblInfo, pkIdx, tblDefCauss)
}

func (b *executorBuilder) buildCleanupIndex(v *plannercore.CleanupIndex) Executor {
	tblInfo := v.Block.BlockInfo
	t, err := b.is.BlockByName(v.Block.Schema, tblInfo.Name)
	if err != nil {
		b.err = err
		return nil
	}
	idxName := strings.ToLower(v.IndexName)
	var index block.Index
	for _, idx := range t.Indices() {
		if idx.Meta().State != perceptron.StatePublic {
			continue
		}
		if idxName == idx.Meta().Name.L {
			index = idx
			break
		}
	}

	if index == nil {
		b.err = errors.Errorf("index `%v` is not found in block `%v`.", v.IndexName, v.Block.Name.O)
		return nil
	}
	e := &CleanupIndexExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		defCausumns:  buildIdxDefCaussConcatHandleDefCauss(tblInfo, index.Meta()),
		index:        index,
		block:        t,
		physicalID:   t.Meta().ID,
		batchSize:    20000,
	}
	sessCtx := e.ctx.GetStochaseinstein_dbars().StmtCtx
	e.handleDefCauss = buildHandleDefCaussForExec(sessCtx, tblInfo, index.Meta(), e.defCausumns)
	return e
}

func (b *executorBuilder) buildChecHoTTexRange(v *plannercore.ChecHoTTexRange) Executor {
	tb, err := b.is.BlockByName(v.Block.Schema, v.Block.Name)
	if err != nil {
		b.err = err
		return nil
	}
	e := &ChecHoTTexRangeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		handleRanges: v.HandleRanges,
		block:        tb.Meta(),
		is:           b.is,
	}
	idxName := strings.ToLower(v.IndexName)
	for _, idx := range tb.Indices() {
		if idx.Meta().Name.L == idxName {
			e.index = idx.Meta()
			e.startKey = make([]types.Causet, len(e.index.DeferredCausets))
			break
		}
	}
	return e
}

func (b *executorBuilder) buildChecksumBlock(v *plannercore.ChecksumBlock) Executor {
	e := &ChecksumBlockExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		blocks:       make(map[int64]*checksumContext),
		done:         false,
	}
	startTs, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	for _, t := range v.Blocks {
		e.blocks[t.BlockInfo.ID] = newChecksumContext(t.DBInfo, t.BlockInfo, startTs)
	}
	return e
}

func (b *executorBuilder) buildReloadExprPushdownBlacklist(v *plannercore.ReloadExprPushdownBlacklist) Executor {
	return &ReloadExprPushdownBlacklistExec{baseExecutor{ctx: b.ctx}}
}

func (b *executorBuilder) buildReloadOptMemruleBlacklist(v *plannercore.ReloadOptMemruleBlacklist) Executor {
	return &ReloadOptMemruleBlacklistExec{baseExecutor{ctx: b.ctx}}
}

func (b *executorBuilder) buildAdminPlugins(v *plannercore.AdminPlugins) Executor {
	return &AdminPluginsExec{baseExecutor: baseExecutor{ctx: b.ctx}, CausetAction: v.CausetAction, Plugins: v.Plugins}
}

func (b *executorBuilder) buildDeallocate(v *plannercore.Deallocate) Executor {
	base := newBaseExecutor(b.ctx, nil, v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &DeallocateExec{
		baseExecutor: base,
		Name:         v.Name,
	}
	return e
}

func (b *executorBuilder) buildSelectLock(v *plannercore.PhysicalLock) Executor {
	b.hasLock = true
	if b.err = b.uFIDelateForUFIDelateTSIfNeeded(v.Children()[0]); b.err != nil {
		return nil
	}
	// Build 'select for uFIDelate' using the 'for uFIDelate' ts.
	b.snapshotTS = b.ctx.GetStochaseinstein_dbars().TxnCtx.GetForUFIDelateTS()

	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	if !b.ctx.GetStochaseinstein_dbars().InTxn() {
		// Locking of rows for uFIDelate using SELECT FOR UFIDelATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See https://dev.allegrosql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	e := &SelectLockExec{
		baseExecutor:     newBaseExecutor(b.ctx, v.Schema(), v.ID(), src),
		Lock:             v.Lock,
		tblID2Handle:     v.TblID2Handle,
		partitionedBlock: v.PartitionedBlock,
	}
	return e
}

func (b *executorBuilder) buildLimit(v *plannercore.PhysicalLimit) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	n := int(mathutil.MinUint64(v.Count, uint64(b.ctx.GetStochaseinstein_dbars().MaxChunkSize)))
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	base.initCap = n
	e := &LimitExec{
		baseExecutor: base,
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}
	return e
}

func (b *executorBuilder) buildPrepare(v *plannercore.Prepare) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	return &PrepareExec{
		baseExecutor: base,
		is:           b.is,
		name:         v.Name,
		sqlText:      v.ALLEGROSQLText,
	}
}

func (b *executorBuilder) buildExecute(v *plannercore.Execute) Executor {
	e := &ExecuteExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		is:           b.is,
		name:         v.Name,
		usingVars:    v.UsingVars,
		id:           v.ExecID,
		stmt:         v.Stmt,
		plan:         v.Plan,
		outputNames:  v.OutputNames(),
	}
	return e
}

func (b *executorBuilder) buildShow(v *plannercore.PhysicalShow) Executor {
	e := &ShowExec{
		baseExecutor:      newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		Tp:                v.Tp,
		DBName:            perceptron.NewCIStr(v.DBName),
		Block:             v.Block,
		DeferredCauset:    v.DeferredCauset,
		IndexName:         v.IndexName,
		Flag:              v.Flag,
		Roles:             v.Roles,
		User:              v.User,
		is:                b.is,
		Full:              v.Full,
		IfNotExists:       v.IfNotExists,
		GlobalSINTERLOCKe: v.GlobalSINTERLOCKe,
		Extended:          v.Extended,
	}
	if e.Tp == ast.ShowGrants && e.User == nil {
		// The input is a "show grants" statement, fulfill the user and roles field.
		// Note: "show grants" result are different from "show grants for current_user",
		// The former determine privileges with roles, while the later doesn't.
		vars := e.ctx.GetStochaseinstein_dbars()
		e.User = &auth.UserIdentity{Username: vars.User.AuthUsername, Hostname: vars.User.AuthHostname}
		e.Roles = vars.ActiveRoles
	}
	if e.Tp == ast.ShowMasterStatus {
		// show master status need start ts.
		if _, err := e.ctx.Txn(true); err != nil {
			b.err = err
		}
	}
	return e
}

func (b *executorBuilder) buildSimple(v *plannercore.Simple) Executor {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		return b.buildRevoke(s)
	case *ast.BRIEStmt:
		return b.buildBRIE(s, v.Schema())
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &SimpleExec{
		baseExecutor: base,
		Statement:    v.Statement,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildSet(v *plannercore.Set) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &SetExecutor{
		baseExecutor: base,
		vars:         v.VarAssigns,
	}
	return e
}

func (b *executorBuilder) buildSetConfig(v *plannercore.SetConfig) Executor {
	return &SetConfigExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		p:            v,
	}
}

func (b *executorBuilder) buildInsert(v *plannercore.Insert) Executor {
	if v.SelectPlan != nil {
		// Try to uFIDelate the forUFIDelateTS for insert/replace into select statements.
		// Set the selectPlan parameter to nil to make it always uFIDelate the forUFIDelateTS.
		if b.err = b.uFIDelateForUFIDelateTSIfNeeded(nil); b.err != nil {
			return nil
		}
	}
	b.snapshotTS = b.ctx.GetStochaseinstein_dbars().TxnCtx.GetForUFIDelateTS()
	selectExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	var baseExec baseExecutor
	if selectExec != nil {
		baseExec = newBaseExecutor(b.ctx, nil, v.ID(), selectExec)
	} else {
		baseExec = newBaseExecutor(b.ctx, nil, v.ID())
	}
	baseExec.initCap = chunk.ZeroCapacity

	ivs := &InsertValues{
		baseExecutor:    baseExec,
		Block:           v.Block,
		DeferredCausets: v.DeferredCausets,
		Lists:           v.Lists,
		SetList:         v.SetList,
		GenExprs:        v.GenDefCauss.Exprs,
		allAssignmentsAreCouplingConstantWithRadix: v.AllAssignmentsAreCouplingConstantWithRadix,
		hasRefDefCauss: v.NeedFillDefaultValue,
		SelectExec:     selectExec,
	}
	err := ivs.initInsertDeferredCausets()
	if err != nil {
		b.err = err
		return nil
	}

	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertExec{
		InsertValues: ivs,
		OnDuplicate:  append(v.OnDuplicate, v.GenDefCauss.OnDuplicates...),
	}
	return insert
}

func (b *executorBuilder) buildLoadData(v *plannercore.LoadData) Executor {
	tbl, ok := b.is.BlockByID(v.Block.BlockInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get block %d", v.Block.BlockInfo.ID)
		return nil
	}
	insertVal := &InsertValues{
		baseExecutor:    newBaseExecutor(b.ctx, nil, v.ID()),
		Block:           tbl,
		DeferredCausets: v.DeferredCausets,
		GenExprs:        v.GenDefCauss.Exprs,
	}
	loadDataInfo := &LoadDataInfo{
		event:                      make([]types.Causet, 0, len(insertVal.insertDeferredCausets)),
		InsertValues:               insertVal,
		Path:                       v.Path,
		Block:                      tbl,
		FieldsInfo:                 v.FieldsInfo,
		LinesInfo:                  v.LinesInfo,
		IgnoreLines:                v.IgnoreLines,
		DeferredCausetAssignments:  v.DeferredCausetAssignments,
		DeferredCausetsAndUserVars: v.DeferredCausetsAndUserVars,
		Ctx:                        b.ctx,
	}
	defCausumnNames := loadDataInfo.initFieldMappings()
	err := loadDataInfo.initLoadDeferredCausets(defCausumnNames)
	if err != nil {
		b.err = err
		return nil
	}
	loadDataExec := &LoadDataExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ID()),
		IsLocal:      v.IsLocal,
		OnDuplicate:  v.OnDuplicate,
		loadDataInfo: loadDataInfo,
	}
	var defaultLoadDataBatchCnt uint64 = 20000 // TODO this will be changed to variable in another pr
	loadDataExec.loadDataInfo.InitQueues()
	loadDataExec.loadDataInfo.SetMaxEventsInBatch(defaultLoadDataBatchCnt)

	return loadDataExec
}

func (b *executorBuilder) buildLoadStats(v *plannercore.LoadStats) Executor {
	e := &LoadStatsExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ID()),
		info:         &LoadStatsInfo{v.Path, b.ctx},
	}
	return e
}

func (b *executorBuilder) buildIndexAdvise(v *plannercore.IndexAdvise) Executor {
	e := &IndexAdviseExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, v.ID()),
		IsLocal:      v.IsLocal,
		indexAdviseInfo: &IndexAdviseInfo{
			Path:        v.Path,
			MaxMinutes:  v.MaxMinutes,
			MaxIndexNum: v.MaxIndexNum,
			LinesInfo:   v.LinesInfo,
			Ctx:         b.ctx,
		},
	}
	return e
}

func (b *executorBuilder) buildReplace(vals *InsertValues) Executor {
	replaceExec := &ReplaceExec{
		InsertValues: vals,
	}
	return replaceExec
}

func (b *executorBuilder) buildGrant(grant *ast.GrantStmt) Executor {
	e := &GrantExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, 0),
		Privs:        grant.Privs,
		ObjectType:   grant.ObjectType,
		Level:        grant.Level,
		Users:        grant.Users,
		WithGrant:    grant.WithGrant,
		TLSOptions:   grant.TLSOptions,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildRevoke(revoke *ast.RevokeStmt) Executor {
	e := &RevokeExec{
		baseExecutor: newBaseExecutor(b.ctx, nil, 0),
		ctx:          b.ctx,
		Privs:        revoke.Privs,
		ObjectType:   revoke.ObjectType,
		Level:        revoke.Level,
		Users:        revoke.Users,
		is:           b.is,
	}
	return e
}

func (b *executorBuilder) buildDBS(v *plannercore.DBS) Executor {
	e := &DBSExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		stmt:         v.Statement,
		is:           b.is,
	}
	return e
}

// buildTrace builds a TraceExec for future executing. This method will be called
// at build().
func (b *executorBuilder) buildTrace(v *plannercore.Trace) Executor {
	t := &TraceExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		stmtNode:     v.StmtNode,
		builder:      b,
		format:       v.Format,
	}
	if t.format == plannercore.TraceFormatLog {
		return &SortExec{
			baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), t),
			ByItems: []*plannerutil.ByItems{
				{Expr: &expression.DeferredCauset{
					Index:   0,
					RetType: types.NewFieldType(allegrosql.TypeTimestamp),
				}},
			},
			schemaReplicant: v.Schema(),
		}
	}
	return t
}

// buildExplain builds a explain executor. `e.rows` defCauslects final result to `ExplainExec`.
func (b *executorBuilder) buildExplain(v *plannercore.Explain) Executor {
	explainExec := &ExplainExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		explain:      v,
	}
	if v.Analyze {
		if b.ctx.GetStochaseinstein_dbars().StmtCtx.RuntimeStatsDefCausl == nil {
			b.ctx.GetStochaseinstein_dbars().StmtCtx.RuntimeStatsDefCausl = execdetails.NewRuntimeStatsDefCausl()
		}
		explainExec.analyzeExec = b.build(v.TargetPlan)
	}
	return explainExec
}

func (b *executorBuilder) buildSelectInto(v *plannercore.SelectInto) Executor {
	child := b.build(v.TargetPlan)
	if b.err != nil {
		return nil
	}
	return &SelectIntoExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), child),
		intoOpt:      v.IntoOpt,
	}
}

func (b *executorBuilder) buildUnionScanExec(v *plannercore.PhysicalUnionScan) Executor {
	reader := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	return b.buildUnionScanFromReader(reader, v)
}

// buildUnionScanFromReader builds union scan executor from child executor.
// Note that this function may be called by inner workers of index lookup join concurrently.
// Be careful to avoid data race.
func (b *executorBuilder) buildUnionScanFromReader(reader Executor, v *plannercore.PhysicalUnionScan) Executor {
	// Adjust UnionScan->PartitionBlock->Reader
	// to PartitionBlock->UnionScan->Reader
	// The build of UnionScan executor is delay to the nextPartition() function
	// because the Reader executor is available there.
	if x, ok := reader.(*PartitionBlockExecutor); ok {
		nextPartitionForReader := x.nextPartition
		x.nextPartition = nextPartitionForUnionScan{
			b:     b,
			us:    v,
			child: nextPartitionForReader,
		}
		return x
	}
	// If reader is union, it means a partitiont block and we should transfer as above.
	if x, ok := reader.(*UnionExec); ok {
		for i, child := range x.children {
			x.children[i] = b.buildUnionScanFromReader(child, v)
			if b.err != nil {
				return nil
			}
		}
		return x
	}
	us := &UnionScanExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), reader)}
	// Get the handle defCausumn index of the below Plan.
	us.belowHandleDefCauss = v.HandleDefCauss
	us.mublockEvent = chunk.MutEventFromTypes(retTypes(us))

	// If the push-downed condition contains virtual defCausumn, we may build a selection upon reader
	originReader := reader
	if sel, ok := reader.(*SelectionExec); ok {
		reader = sel.children[0]
	}

	switch x := reader.(type) {
	case *BlockReaderExecutor:
		us.desc = x.desc
		// Union scan can only be in a write transaction, so DirtyDB should has non-nil value now, thus
		// GetDirtyDB() is safe here. If this block has been modified in the transaction, non-nil DirtyBlock
		// can be found in DirtyDB now, so GetDirtyBlock is safe; if this block has not been modified in the
		// transaction, empty DirtyBlock would be inserted into DirtyDB, it does not matter when multiple
		// goroutines write empty DirtyBlock to DirtyDB for this block concurrently. Although the DirtyDB looks
		// safe for data race in all the cases, the map of golang will throw panic when it's accessed in parallel.
		// So we dagger it when getting dirty block.
		us.conditions, us.conditionsWithVirDefCaus = plannercore.SplitSelCondsWithVirtualDeferredCauset(v.Conditions)
		us.defCausumns = x.defCausumns
		us.block = x.block
		us.virtualDeferredCausetIndex = x.virtualDeferredCausetIndex
	case *IndexReaderExecutor:
		us.desc = x.desc
		for _, ic := range x.index.DeferredCausets {
			for i, defCaus := range x.defCausumns {
				if defCaus.Name.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.conditions, us.conditionsWithVirDefCaus = plannercore.SplitSelCondsWithVirtualDeferredCauset(v.Conditions)
		us.defCausumns = x.defCausumns
		us.block = x.block
	case *IndexLookUpExecutor:
		us.desc = x.desc
		for _, ic := range x.index.DeferredCausets {
			for i, defCaus := range x.defCausumns {
				if defCaus.Name.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.conditions, us.conditionsWithVirDefCaus = plannercore.SplitSelCondsWithVirtualDeferredCauset(v.Conditions)
		us.defCausumns = x.defCausumns
		us.block = x.block
		us.virtualDeferredCausetIndex = buildVirtualDeferredCausetIndex(us.Schema(), us.defCausumns)
	default:
		// The mem block will not be written by allegrosql directly, so we can omit the union scan to avoid err reporting.
		return originReader
	}
	return us
}

// buildMergeJoin builds MergeJoinExec executor.
func (b *executorBuilder) buildMergeJoin(v *plannercore.PhysicalMergeJoin) Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	defaultValues := v.DefaultValues
	if defaultValues == nil {
		if v.JoinType == plannercore.RightOuterJoin {
			defaultValues = make([]types.Causet, leftExec.Schema().Len())
		} else {
			defaultValues = make([]types.Causet, rightExec.Schema().Len())
		}
	}

	e := &MergeJoinExec{
		stmtCtx:      b.ctx.GetStochaseinstein_dbars().StmtCtx,
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		compareFuncs: v.CompareFuncs,
		joiner: newJoiner(
			b.ctx,
			v.JoinType,
			v.JoinType == plannercore.RightOuterJoin,
			defaultValues,
			v.OtherConditions,
			retTypes(leftExec),
			retTypes(rightExec),
			markChildrenUsedDefCauss(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema()),
		),
		isOuterJoin: v.JoinType.IsOuterJoin(),
		desc:        v.Desc,
	}

	leftBlock := &mergeJoinBlock{
		childIndex: 0,
		joinKeys:   v.LeftJoinKeys,
		filters:    v.LeftConditions,
	}
	rightBlock := &mergeJoinBlock{
		childIndex: 1,
		joinKeys:   v.RightJoinKeys,
		filters:    v.RightConditions,
	}

	if v.JoinType == plannercore.RightOuterJoin {
		e.innerBlock = leftBlock
		e.outerBlock = rightBlock
	} else {
		e.innerBlock = rightBlock
		e.outerBlock = leftBlock
	}
	e.innerBlock.isInner = true

	// optimizer should guarantee that filters on inner block are pushed down
	// to einsteindb or extracted to a Selection.
	if len(e.innerBlock.filters) != 0 {
		b.err = errors.Annotate(ErrBuildExecutor, "merge join's inner filter should be empty.")
		return nil
	}

	executorCounterMergeJoinExec.Inc()
	return e
}

func (b *executorBuilder) buildSideEstCount(v *plannercore.PhysicalHashJoin) float64 {
	buildSide := v.Children()[v.InnerChildIdx]
	if v.UseOuterToBuild {
		buildSide = v.Children()[1-v.InnerChildIdx]
	}
	if buildSide.Stats().HistDefCausl == nil || buildSide.Stats().HistDefCausl.Pseudo {
		return 0.0
	}
	return buildSide.StatsCount()
}

func (b *executorBuilder) buildHashJoin(v *plannercore.PhysicalHashJoin) Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	e := &HashJoinExec{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		concurrency:     v.Concurrency,
		joinType:        v.JoinType,
		isOuterJoin:     v.JoinType.IsOuterJoin(),
		useOuterToBuild: v.UseOuterToBuild,
	}
	defaultValues := v.DefaultValues
	lhsTypes, rhsTypes := retTypes(leftExec), retTypes(rightExec)
	if v.InnerChildIdx == 1 {
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}

	// consider defCauslations
	leftTypes := make([]*types.FieldType, 0, len(retTypes(leftExec)))
	for _, tp := range retTypes(leftExec) {
		leftTypes = append(leftTypes, tp.Clone())
	}
	rightTypes := make([]*types.FieldType, 0, len(retTypes(rightExec)))
	for _, tp := range retTypes(rightExec) {
		rightTypes = append(rightTypes, tp.Clone())
	}
	leftIsBuildSide := true

	e.isNullEQ = v.IsNullEQ
	if v.UseOuterToBuild {
		// uFIDelate the buildSideEstCount due to changing the build side
		if v.InnerChildIdx == 1 {
			e.buildSideExec, e.buildKeys = leftExec, v.LeftJoinKeys
			e.probeSideExec, e.probeKeys = rightExec, v.RightJoinKeys
			e.outerFilter = v.LeftConditions
		} else {
			e.buildSideExec, e.buildKeys = rightExec, v.RightJoinKeys
			e.probeSideExec, e.probeKeys = leftExec, v.LeftJoinKeys
			e.outerFilter = v.RightConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Causet, e.probeSideExec.Schema().Len())
		}
	} else {
		if v.InnerChildIdx == 0 {
			e.buildSideExec, e.buildKeys = leftExec, v.LeftJoinKeys
			e.probeSideExec, e.probeKeys = rightExec, v.RightJoinKeys
			e.outerFilter = v.RightConditions
		} else {
			e.buildSideExec, e.buildKeys = rightExec, v.RightJoinKeys
			e.probeSideExec, e.probeKeys = leftExec, v.LeftJoinKeys
			e.outerFilter = v.LeftConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Causet, e.buildSideExec.Schema().Len())
		}
	}
	e.buildSideEstCount = b.buildSideEstCount(v)
	childrenUsedSchema := markChildrenUsedDefCauss(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema())
	e.joiners = make([]joiner, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joiners[i] = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues,
			v.OtherConditions, lhsTypes, rhsTypes, childrenUsedSchema)
	}
	executorCountHashJoinExec.Inc()

	for i := range v.EqualConditions {
		chs, defCausl := v.EqualConditions[i].CharsetAndDefCauslation(e.ctx)
		bt := leftTypes[v.LeftJoinKeys[i].Index]
		bt.Charset, bt.DefCauslate = chs, defCausl
		pt := rightTypes[v.RightJoinKeys[i].Index]
		pt.Charset, pt.DefCauslate = chs, defCausl
	}
	if leftIsBuildSide {
		e.buildTypes, e.probeTypes = leftTypes, rightTypes
	} else {
		e.buildTypes, e.probeTypes = rightTypes, leftTypes
	}
	return e
}

func (b *executorBuilder) buildHashAgg(v *plannercore.PhysicalHashAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	stochaseinstein_dbars := b.ctx.GetStochaseinstein_dbars()
	e := &HashAggExec{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ID(), src),
		sc:              stochaseinstein_dbars.StmtCtx,
		PartialAggFuncs: make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
		GroupByItems:    v.GroupByItems,
	}
	// We take `create block t(a int, b int);` as example.
	//
	// 1. If all the aggregation functions are FIRST_ROW, we do not need to set the defaultVal for them:
	// e.g.
	// allegrosql> select distinct a, b from t;
	// 0 rows in set (0.00 sec)
	//
	// 2. If there exists group by items, we do not need to set the defaultVal for them either:
	// e.g.
	// allegrosql> select avg(a) from t group by b;
	// Empty set (0.00 sec)
	//
	// allegrosql> select avg(a) from t group by a;
	// +--------+
	// | avg(a) |
	// +--------+
	// |  NULL  |
	// +--------+
	// 1 event in set (0.00 sec)
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstEvent(v.AggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(retTypes(e), 1)
	}
	for _, aggDesc := range v.AggFuncs {
		if aggDesc.HasDistinct || len(aggDesc.OrderByItems) > 0 {
			e.isUnparallelExec = true
		}
	}
	// When we set both milevadb_hashagg_final_concurrency and milevadb_hashagg_partial_concurrency to 1,
	// we do not need to parallelly execute hash agg,
	// and this action can be a workaround when meeting some unexpected situation using parallelExec.
	if finalCon, partialCon := stochaseinstein_dbars.HashAggFinalConcurrency(), stochaseinstein_dbars.HashAggPartialConcurrency(); finalCon <= 0 || partialCon <= 0 || finalCon == 1 && partialCon == 1 {
		e.isUnparallelExec = true
	}
	partialOrdinal := 0
	for i, aggDesc := range v.AggFuncs {
		if e.isUnparallelExec {
			e.PartialAggFuncs = append(e.PartialAggFuncs, aggfuncs.Build(b.ctx, aggDesc, i))
		} else {
			ordinal := []int{partialOrdinal}
			partialOrdinal++
			if aggDesc.Name == ast.AggFuncAvg {
				ordinal = append(ordinal, partialOrdinal+1)
				partialOrdinal++
			}
			partialAggDesc, finalDesc := aggDesc.Split(ordinal)
			partialAggFunc := aggfuncs.Build(b.ctx, partialAggDesc, i)
			finalAggFunc := aggfuncs.Build(b.ctx, finalDesc, i)
			e.PartialAggFuncs = append(e.PartialAggFuncs, partialAggFunc)
			e.FinalAggFuncs = append(e.FinalAggFuncs, finalAggFunc)
			if partialAggDesc.Name == ast.AggFuncGroupConcat {
				// For group_concat, finalAggFunc and partialAggFunc need shared `truncate` flag to do duplicate.
				finalAggFunc.(interface{ SetTruncated(t *int32) }).SetTruncated(
					partialAggFunc.(interface{ GetTruncated() *int32 }).GetTruncated(),
				)
			}
		}
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendCauset(i, &value)
		}
	}

	executorCounterHashAggExec.Inc()
	return e
}

func (b *executorBuilder) buildStreamAgg(v *plannercore.PhysicalStreamAgg) Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &StreamAggExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), src),
		groupChecker: newVecGroupChecker(b.ctx, v.GroupByItems),
		aggFuncs:     make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
	}
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstEvent(v.AggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(retTypes(e), 1)
	}
	for i, aggDesc := range v.AggFuncs {
		aggFunc := aggfuncs.Build(b.ctx, aggDesc, i)
		e.aggFuncs = append(e.aggFuncs, aggFunc)
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendCauset(i, &value)
		}
	}

	executorStreamAggExec.Inc()
	return e
}

func (b *executorBuilder) buildSelection(v *plannercore.PhysicalSelection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &SelectionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		filters:      v.Conditions,
	}
	return e
}

func (b *executorBuilder) buildProjection(v *plannercore.PhysicalProjection) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &ProjectionExec{
		baseExecutor:     newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		numWorkers:       int64(b.ctx.GetStochaseinstein_dbars().ProjectionConcurrency()),
		evaluatorSuit:    expression.NewEvaluatorSuite(v.Exprs, v.AvoidDeferredCausetEvaluator),
		calculateNoDelay: v.CalculateNoDelay,
	}

	// If the calculation event count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(b.ctx.GetStochaseinstein_dbars().MaxChunkSize) {
		e.numWorkers = 0
	}
	return e
}

func (b *executorBuilder) buildBlockDual(v *plannercore.PhysicalBlockDual) Executor {
	if v.EventCount != 0 && v.EventCount != 1 {
		b.err = errors.Errorf("buildBlockDual failed, invalid event count for dual block: %v", v.EventCount)
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = v.EventCount
	e := &BlockDualExec{
		baseExecutor:  base,
		numDualEvents: v.EventCount,
	}
	return e
}

func (b *executorBuilder) getSnapshotTS() (uint64, error) {
	if b.snapshotTS != 0 {
		// Return the cached value.
		return b.snapshotTS, nil
	}

	snapshotTS := b.ctx.GetStochaseinstein_dbars().SnapshotTS
	txn, err := b.ctx.Txn(true)
	if err != nil {
		return 0, err
	}
	if snapshotTS == 0 {
		snapshotTS = txn.StartTS()
	}
	b.snapshotTS = snapshotTS
	if b.snapshotTS == 0 {
		return 0, errors.Trace(ErrGetStartTS)
	}
	return snapshotTS, nil
}

func (b *executorBuilder) buildMemBlock(v *plannercore.PhysicalMemBlock) Executor {
	switch v.DBName.L {
	case soliton.MetricSchemaName.L:
		return &MemBlockReaderExec{
			baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
			block:        v.Block,
			retriever: &MetricRetriever{
				block:     v.Block,
				extractor: v.Extractor.(*plannercore.MetricBlockExtractor),
			},
		}
	case soliton.InformationSchemaName.L:
		switch v.Block.Name.L {
		case strings.ToLower(schemareplicant.BlockClusterConfig):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &clusterConfigRetriever{
					extractor: v.Extractor.(*plannercore.ClusterBlockExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockClusterLoad):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterBlockExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_LoadInfo,
				},
			}
		case strings.ToLower(schemareplicant.BlockClusterHardware):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterBlockExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_HardwareInfo,
				},
			}
		case strings.ToLower(schemareplicant.BlockClusterSystemInfo):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*plannercore.ClusterBlockExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_SystemInfo,
				},
			}
		case strings.ToLower(schemareplicant.BlockClusterLog):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &clusterLogRetriever{
					extractor: v.Extractor.(*plannercore.ClusterLogBlockExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockInspectionResult):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &inspectionResultRetriever{
					extractor: v.Extractor.(*plannercore.InspectionResultBlockExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(schemareplicant.BlockInspectionSummary):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &inspectionSummaryRetriever{
					block:     v.Block,
					extractor: v.Extractor.(*plannercore.InspectionSummaryBlockExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(schemareplicant.BlockInspectionMemrules):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &inspectionMemruleRetriever{
					extractor: v.Extractor.(*plannercore.InspectionMemruleBlockExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockMetricSummary):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &MetricsSummaryRetriever{
					block:     v.Block,
					extractor: v.Extractor.(*plannercore.MetricSummaryBlockExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(schemareplicant.BlockMetricSummaryByLabel):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &MetricsSummaryByLabelRetriever{
					block:     v.Block,
					extractor: v.Extractor.(*plannercore.MetricSummaryBlockExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(schemareplicant.BlockSchemata),
			strings.ToLower(schemareplicant.BlockStatistics),
			strings.ToLower(schemareplicant.BlockMilevaDBIndexes),
			strings.ToLower(schemareplicant.BlockViews),
			strings.ToLower(schemareplicant.BlockBlocks),
			strings.ToLower(schemareplicant.BlockSequences),
			strings.ToLower(schemareplicant.BlockPartitions),
			strings.ToLower(schemareplicant.BlockEngines),
			strings.ToLower(schemareplicant.BlockDefCauslations),
			strings.ToLower(schemareplicant.BlockAnalyzeStatus),
			strings.ToLower(schemareplicant.BlockClusterInfo),
			strings.ToLower(schemareplicant.BlockProfiling),
			strings.ToLower(schemareplicant.BlockCharacterSets),
			strings.ToLower(schemareplicant.BlockKeyDeferredCauset),
			strings.ToLower(schemareplicant.BlockUserPrivileges),
			strings.ToLower(schemareplicant.BlockMetricBlocks),
			strings.ToLower(schemareplicant.BlockDefCauslationCharacterSetApplicability),
			strings.ToLower(schemareplicant.BlockProcesslist),
			strings.ToLower(schemareplicant.ClusterBlockProcesslist),
			strings.ToLower(schemareplicant.BlockEinsteinDBRegionStatus),
			strings.ToLower(schemareplicant.BlockEinsteinDBRegionPeers),
			strings.ToLower(schemareplicant.BlockMilevaDBHotRegions),
			strings.ToLower(schemareplicant.BlockStochaseinstein_dbar),
			strings.ToLower(schemareplicant.BlockConstraints),
			strings.ToLower(schemareplicant.BlockTiFlashReplica),
			strings.ToLower(schemareplicant.BlockMilevaDBServersInfo),
			strings.ToLower(schemareplicant.BlockEinsteinDBStoreStatus),
			strings.ToLower(schemareplicant.BlockStatementsSummary),
			strings.ToLower(schemareplicant.BlockStatementsSummaryHistory),
			strings.ToLower(schemareplicant.ClusterBlockStatementsSummary),
			strings.ToLower(schemareplicant.ClusterBlockStatementsSummaryHistory):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &memblockRetriever{
					block:       v.Block,
					defCausumns: v.DeferredCausets,
				},
			}
		case strings.ToLower(schemareplicant.BlockDeferredCausets):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &hugeMemBlockRetriever{
					block:       v.Block,
					defCausumns: v.DeferredCausets,
				},
			}

		case strings.ToLower(schemareplicant.BlockSlowQuery), strings.ToLower(schemareplicant.ClusterBlockSlowLog):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &slowQueryRetriever{
					block:          v.Block,
					outputDefCauss: v.DeferredCausets,
					extractor:      v.Extractor.(*plannercore.SlowQueryExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockStorageStats):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &blockStorageStatsRetriever{
					block:          v.Block,
					outputDefCauss: v.DeferredCausets,
					extractor:      v.Extractor.(*plannercore.BlockStorageStatsExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockDBSJobs):
			return &DBSJobsReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				is:           b.is,
			}
		case strings.ToLower(schemareplicant.BlockTiFlashBlocks),
			strings.ToLower(schemareplicant.BlockTiFlashSegments):
			return &MemBlockReaderExec{
				baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
				block:        v.Block,
				retriever: &TiFlashSystemBlockRetriever{
					block:          v.Block,
					outputDefCauss: v.DeferredCausets,
					extractor:      v.Extractor.(*plannercore.TiFlashSystemBlockExtractor),
				},
			}
		}
	}
	tb, _ := b.is.BlockByID(v.Block.ID)
	return &BlockScanExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		t:            tb,
		defCausumns:  v.DeferredCausets,
	}
}

func (b *executorBuilder) buildSort(v *plannercore.PhysicalSort) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := SortExec{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		ByItems:         v.ByItems,
		schemaReplicant: v.Schema(),
	}
	executorCounterSortExec.Inc()
	return &sortExec
}

func (b *executorBuilder) buildTopN(v *plannercore.PhysicalTopN) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := SortExec{
		baseExecutor:    newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		ByItems:         v.ByItems,
		schemaReplicant: v.Schema(),
	}
	executorCounterTopNExec.Inc()
	return &TopNExec{
		SortExec: sortExec,
		limit:    &plannercore.PhysicalLimit{Count: v.Count, Offset: v.Offset},
	}
}

func (b *executorBuilder) buildApply(v *plannercore.PhysicalApply) Executor {
	var (
		innerPlan plannercore.PhysicalPlan
		outerPlan plannercore.PhysicalPlan
	)
	if v.InnerChildIdx == 0 {
		innerPlan = v.Children()[0]
		outerPlan = v.Children()[1]
	} else {
		innerPlan = v.Children()[1]
		outerPlan = v.Children()[0]
	}
	v.OuterSchema = plannercore.ExtractCorDeferredCausetsBySchema4PhysicalPlan(innerPlan, outerPlan.Schema())
	leftChild := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	rightChild := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}
	otherConditions := append(expression.ScalarFuncs2Exprs(v.EqualConditions), v.OtherConditions...)
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Causet, v.Children()[v.InnerChildIdx].Schema().Len())
	}
	outerExec, innerExec := leftChild, rightChild
	outerFilter, innerFilter := v.LeftConditions, v.RightConditions
	if v.InnerChildIdx == 0 {
		outerExec, innerExec = rightChild, leftChild
		outerFilter, innerFilter = v.RightConditions, v.LeftConditions
	}
	tupleJoiner := newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
		defaultValues, otherConditions, retTypes(leftChild), retTypes(rightChild), nil)
	serialExec := &NestedLoopApplyExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec, innerExec),
		innerExec:    innerExec,
		outerExec:    outerExec,
		outerFilter:  outerFilter,
		innerFilter:  innerFilter,
		outer:        v.JoinType != plannercore.InnerJoin,
		joiner:       tupleJoiner,
		outerSchema:  v.OuterSchema,
		ctx:          b.ctx,
		canUseCache:  v.CanUseCache,
	}
	executorCounterNestedLoopApplyExec.Inc()

	// try parallel mode
	if v.Concurrency > 1 {
		innerExecs := make([]Executor, 0, v.Concurrency)
		innerFilters := make([]expression.CNFExprs, 0, v.Concurrency)
		corDefCauss := make([][]*expression.CorrelatedDeferredCauset, 0, v.Concurrency)
		joiners := make([]joiner, 0, v.Concurrency)
		for i := 0; i < v.Concurrency; i++ {
			clonedInnerPlan, err := plannercore.SafeClone(innerPlan)
			if err != nil {
				b.err = nil
				return serialExec
			}
			corDefCaus := plannercore.ExtractCorDeferredCausetsBySchema4PhysicalPlan(clonedInnerPlan, outerPlan.Schema())
			clonedInnerExec := b.build(clonedInnerPlan)
			if b.err != nil {
				b.err = nil
				return serialExec
			}
			innerExecs = append(innerExecs, clonedInnerExec)
			corDefCauss = append(corDefCauss, corDefCaus)
			innerFilters = append(innerFilters, innerFilter.Clone())
			joiners = append(joiners, newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
				defaultValues, otherConditions, retTypes(leftChild), retTypes(rightChild), nil))
		}

		allExecs := append([]Executor{outerExec}, innerExecs...)

		return &ParallelNestedLoopApplyExec{
			baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), allExecs...),
			innerExecs:   innerExecs,
			outerExec:    outerExec,
			outerFilter:  outerFilter,
			innerFilter:  innerFilters,
			outer:        v.JoinType != plannercore.InnerJoin,
			joiners:      joiners,
			corDefCauss:  corDefCauss,
			concurrency:  v.Concurrency,
			useCache:     true,
		}
	}
	return serialExec
}

func (b *executorBuilder) buildMaxOneEvent(v *plannercore.PhysicalMaxOneEvent) Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	base.initCap = 2
	base.maxChunkSize = 2
	e := &MaxOneEventExec{baseExecutor: base}
	return e
}

func (b *executorBuilder) buildUnionAll(v *plannercore.PhysicalUnionAll) Executor {
	childExecs := make([]Executor, len(v.Children()))
	for i, child := range v.Children() {
		childExecs[i] = b.build(child)
		if b.err != nil {
			return nil
		}
	}
	e := &UnionExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExecs...),
		concurrency:  b.ctx.GetStochaseinstein_dbars().UnionConcurrency(),
	}
	return e
}

func buildHandleDefCaussForSplit(sc *stmtctx.StatementContext, tbInfo *perceptron.BlockInfo) plannercore.HandleDefCauss {
	if tbInfo.IsCommonHandle {
		primaryIdx := blocks.FindPrimaryIndex(tbInfo)
		blockDefCauss := make([]*expression.DeferredCauset, len(tbInfo.DeferredCausets))
		for i, defCaus := range tbInfo.DeferredCausets {
			blockDefCauss[i] = &expression.DeferredCauset{
				ID:      defCaus.ID,
				RetType: &defCaus.FieldType,
			}
		}
		for i, pkDefCaus := range primaryIdx.DeferredCausets {
			blockDefCauss[pkDefCaus.Offset].Index = i
		}
		return plannercore.NewCommonHandleDefCauss(sc, tbInfo, primaryIdx, blockDefCauss)
	}
	intDefCaus := &expression.DeferredCauset{
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	return plannercore.NewIntHandleDefCauss(intDefCaus)
}

func (b *executorBuilder) buildSplitRegion(v *plannercore.SplitRegion) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = 1
	base.maxChunkSize = 1
	if v.IndexInfo != nil {
		return &SplitIndexRegionExec{
			baseExecutor:   base,
			blockInfo:      v.BlockInfo,
			partitionNames: v.PartitionNames,
			indexInfo:      v.IndexInfo,
			lower:          v.Lower,
			upper:          v.Upper,
			num:            v.Num,
			valueLists:     v.ValueLists,
		}
	}
	handleDefCauss := buildHandleDefCaussForSplit(b.ctx.GetStochaseinstein_dbars().StmtCtx, v.BlockInfo)
	if len(v.ValueLists) > 0 {
		return &SplitBlockRegionExec{
			baseExecutor:   base,
			blockInfo:      v.BlockInfo,
			partitionNames: v.PartitionNames,
			handleDefCauss: handleDefCauss,
			valueLists:     v.ValueLists,
		}
	}
	return &SplitBlockRegionExec{
		baseExecutor:   base,
		blockInfo:      v.BlockInfo,
		partitionNames: v.PartitionNames,
		handleDefCauss: handleDefCauss,
		lower:          v.Lower,
		upper:          v.Upper,
		num:            v.Num,
	}
}

func (b *executorBuilder) buildUFIDelate(v *plannercore.UFIDelate) Executor {
	tblID2block := make(map[int64]block.Block, len(v.TblDefCausPosInfos))
	for _, info := range v.TblDefCausPosInfos {
		tbl, _ := b.is.BlockByID(info.TblID)
		tblID2block[info.TblID] = tbl
		if len(v.PartitionedBlock) > 0 {
			// The v.PartitionedBlock defCauslects the partitioned block.
			// Replace the original block with the partitioned block to support partition selection.
			// e.g. uFIDelate t partition (p0, p1), the new values are not belong to the given set p0, p1
			// Using the block in v.PartitionedBlock returns a proper error, while using the original block can't.
			for _, p := range v.PartitionedBlock {
				if info.TblID == p.Meta().ID {
					tblID2block[info.TblID] = p
				}
			}
		}
	}
	if b.err = b.uFIDelateForUFIDelateTSIfNeeded(v.SelectPlan); b.err != nil {
		return nil
	}
	b.snapshotTS = b.ctx.GetStochaseinstein_dbars().TxnCtx.GetForUFIDelateTS()
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), selExec)
	base.initCap = chunk.ZeroCapacity
	uFIDelateExec := &UFIDelateExec{
		baseExecutor: base,
		OrderedList:  v.OrderedList,
		allAssignmentsAreCouplingConstantWithRadix: v.AllAssignmentsAreCouplingConstantWithRadix,
		tblID2block:        tblID2block,
		tblDefCausPosInfos: v.TblDefCausPosInfos,
	}
	return uFIDelateExec
}

func (b *executorBuilder) buildDelete(v *plannercore.Delete) Executor {
	tblID2block := make(map[int64]block.Block, len(v.TblDefCausPosInfos))
	for _, info := range v.TblDefCausPosInfos {
		tblID2block[info.TblID], _ = b.is.BlockByID(info.TblID)
	}
	if b.err = b.uFIDelateForUFIDelateTSIfNeeded(v.SelectPlan); b.err != nil {
		return nil
	}
	b.snapshotTS = b.ctx.GetStochaseinstein_dbars().TxnCtx.GetForUFIDelateTS()
	selExec := b.build(v.SelectPlan)
	if b.err != nil {
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), selExec)
	base.initCap = chunk.ZeroCapacity
	deleteExec := &DeleteExec{
		baseExecutor:       base,
		tblID2Block:        tblID2block,
		IsMultiBlock:       v.IsMultiBlock,
		tblDefCausPosInfos: v.TblDefCausPosInfos,
	}
	return deleteExec
}

// uFIDelateForUFIDelateTSIfNeeded uFIDelates the ForUFIDelateTS for a pessimistic transaction if needed.
// PointGet executor will get conflict error if the ForUFIDelateTS is older than the latest commitTS,
// so we don't need to uFIDelate now for better latency.
func (b *executorBuilder) uFIDelateForUFIDelateTSIfNeeded(selectPlan plannercore.PhysicalPlan) error {
	txnCtx := b.ctx.GetStochaseinstein_dbars().TxnCtx
	if !txnCtx.IsPessimistic {
		return nil
	}
	if _, ok := selectPlan.(*plannercore.PointGetPlan); ok {
		return nil
	}
	// Activate the invalid txn, use the txn startTS as newForUFIDelateTS
	txn, err := b.ctx.Txn(false)
	if err != nil {
		return err
	}
	if !txn.Valid() {
		_, err := b.ctx.Txn(true)
		if err != nil {
			return err
		}
		return nil
	}
	// The Repeablock Read transaction use Read Committed level to read data for writing (insert, uFIDelate, delete, select for uFIDelate),
	// We should always uFIDelate/refresh the for-uFIDelate-ts no matter the isolation level is RR or RC.
	if b.ctx.GetStochaseinstein_dbars().IsPessimisticReadConsistency() {
		return b.refreshForUFIDelateTSForRC()
	}
	return UFIDelateForUFIDelateTS(b.ctx, 0)
}

// refreshForUFIDelateTSForRC is used to refresh the for-uFIDelate-ts for reading data at read consistency level in pessimistic transaction.
// It could use the cached tso from the statement future to avoid get tso many times.
func (b *executorBuilder) refreshForUFIDelateTSForRC() error {
	defer func() {
		b.snapshotTS = b.ctx.GetStochaseinstein_dbars().TxnCtx.GetForUFIDelateTS()
	}()
	future := b.ctx.GetStochaseinstein_dbars().TxnCtx.GetStmtFutureForRC()
	if future == nil {
		return nil
	}
	newForUFIDelateTS, waitErr := future.Wait()
	if waitErr != nil {
		logutil.BgLogger().Warn("wait tso failed",
			zap.Uint64("startTS", b.ctx.GetStochaseinstein_dbars().TxnCtx.StartTS),
			zap.Error(waitErr))
	}
	b.ctx.GetStochaseinstein_dbars().TxnCtx.SetStmtFutureForRC(nil)
	// If newForUFIDelateTS is 0, it will force to get a new for-uFIDelate-ts from FIDel.
	return UFIDelateForUFIDelateTS(b.ctx, newForUFIDelateTS)
}

func (b *executorBuilder) buildAnalyzeIndexPushdown(task plannercore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64, autoAnalyze string) *analyzeTask {
	_, offset := timeutil.Zone(b.ctx.GetStochaseinstein_dbars().Location())
	sc := b.ctx.GetStochaseinstein_dbars().StmtCtx
	e := &AnalyzeIndexExec{
		ctx:            b.ctx,
		blockID:        task.BlockID,
		isCommonHandle: task.TblInfo.IsCommonHandle,
		idxInfo:        task.IndexInfo,
		concurrency:    b.ctx.GetStochaseinstein_dbars().IndexSerialScanConcurrency(),
		analyzePB: &fidelpb.AnalyzeReq{
			Tp:             fidelpb.AnalyzeType_TypeIndex,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts: opts,
	}
	e.analyzePB.IdxReq = &fidelpb.AnalyzeIndexReq{
		BucketSize:         int64(opts[ast.AnalyzeOptNumBuckets]),
		NumDeferredCausets: int32(len(task.IndexInfo.DeferredCausets)),
	}
	if e.isCommonHandle && e.idxInfo.Primary {
		e.analyzePB.Tp = fidelpb.AnalyzeType_TypeCommonHandle
	}
	depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
	width := int32(opts[ast.AnalyzeOptCMSketchWidth])
	e.analyzePB.IdxReq.CmsketchDepth = &depth
	e.analyzePB.IdxReq.CmsketchWidth = &width
	job := &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze index " + task.IndexInfo.Name.O}
	return &analyzeTask{taskType: idxTask, idxExec: e, job: job}
}

func (b *executorBuilder) buildAnalyzeIndexIncremental(task plannercore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64) *analyzeTask {
	h := petri.GetPetri(b.ctx).StatsHandle()
	statsTbl := h.GetPartitionStats(&perceptron.BlockInfo{}, task.BlockID.PersistID)
	analyzeTask := b.buildAnalyzeIndexPushdown(task, opts, "")
	if statsTbl.Pseudo {
		return analyzeTask
	}
	idx, ok := statsTbl.Indices[task.IndexInfo.ID]
	if !ok || idx.Len() == 0 || idx.LastAnalyzePos.IsNull() {
		return analyzeTask
	}
	var oldHist *statistics.Histogram
	if statistics.IsAnalyzed(idx.Flag) {
		exec := analyzeTask.idxExec
		if idx.CMSketch != nil {
			width, depth := idx.CMSketch.GetWidthAndDepth()
			exec.analyzePB.IdxReq.CmsketchWidth = &width
			exec.analyzePB.IdxReq.CmsketchDepth = &depth
		}
		oldHist = idx.Histogram.INTERLOCKy()
	} else {
		_, bktID := idx.LessEventCountWithBktIdx(idx.LastAnalyzePos)
		if bktID == 0 {
			return analyzeTask
		}
		oldHist = idx.TruncateHistogram(bktID)
	}
	oldHist = oldHist.RemoveUpperBound()
	analyzeTask.taskType = idxIncrementalTask
	analyzeTask.idxIncrementalExec = &analyzeIndexIncrementalExec{AnalyzeIndexExec: *analyzeTask.idxExec, oldHist: oldHist, oldCMS: idx.CMSketch}
	analyzeTask.job = &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: "analyze incremental index " + task.IndexInfo.Name.O}
	return analyzeTask
}

func (b *executorBuilder) buildAnalyzeDeferredCausetsPushdown(task plannercore.AnalyzeDeferredCausetsTask, opts map[ast.AnalyzeOptionType]uint64, autoAnalyze string) *analyzeTask {
	defcaus := task.DefCaussInfo
	if hasPkHist(task.HandleDefCauss) {
		defCausInfo := task.TblInfo.DeferredCausets[task.HandleDefCauss.GetDefCaus(0).Index]
		defcaus = append([]*perceptron.DeferredCausetInfo{defCausInfo}, defcaus...)
	} else if task.HandleDefCauss != nil && !task.HandleDefCauss.IsInt() {
		defcaus = make([]*perceptron.DeferredCausetInfo, 0, len(task.DefCaussInfo)+task.HandleDefCauss.NumDefCauss())
		for i := 0; i < task.HandleDefCauss.NumDefCauss(); i++ {
			defcaus = append(defcaus, task.TblInfo.DeferredCausets[task.HandleDefCauss.GetDefCaus(i).Index])
		}
		defcaus = append(defcaus, task.DefCaussInfo...)
		task.DefCaussInfo = defcaus
	}

	_, offset := timeutil.Zone(b.ctx.GetStochaseinstein_dbars().Location())
	sc := b.ctx.GetStochaseinstein_dbars().StmtCtx
	e := &AnalyzeDeferredCausetsExec{
		ctx:            b.ctx,
		blockID:        task.BlockID,
		defcausInfo:    task.DefCaussInfo,
		handleDefCauss: task.HandleDefCauss,
		concurrency:    b.ctx.GetStochaseinstein_dbars().DistALLEGROSQLScanConcurrency(),
		analyzePB: &fidelpb.AnalyzeReq{
			Tp:             fidelpb.AnalyzeType_TypeDeferredCauset,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts: opts,
	}
	depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
	width := int32(opts[ast.AnalyzeOptCMSketchWidth])
	e.analyzePB.DefCausReq = &fidelpb.AnalyzeDeferredCausetsReq{
		BucketSize:          int64(opts[ast.AnalyzeOptNumBuckets]),
		SampleSize:          maxRegionSampleSize,
		SketchSize:          maxSketchSize,
		DeferredCausetsInfo: soliton.DeferredCausetsToProto(defcaus, task.HandleDefCauss != nil && task.HandleDefCauss.IsInt()),
		CmsketchDepth:       &depth,
		CmsketchWidth:       &width,
	}
	if task.TblInfo != nil {
		e.analyzePB.DefCausReq.PrimaryDeferredCausetIds = blocks.TryGetCommonPkDeferredCausetIds(task.TblInfo)
	}
	b.err = plannercore.SetPBDeferredCausetsDefaultValue(b.ctx, e.analyzePB.DefCausReq.DeferredCausetsInfo, defcaus)
	job := &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze defCausumns"}
	return &analyzeTask{taskType: defCausTask, defCausExec: e, job: job}
}

func (b *executorBuilder) buildAnalyzePKIncremental(task plannercore.AnalyzeDeferredCausetsTask, opts map[ast.AnalyzeOptionType]uint64) *analyzeTask {
	h := petri.GetPetri(b.ctx).StatsHandle()
	statsTbl := h.GetPartitionStats(&perceptron.BlockInfo{}, task.BlockID.PersistID)
	analyzeTask := b.buildAnalyzeDeferredCausetsPushdown(task, opts, "")
	if statsTbl.Pseudo {
		return analyzeTask
	}
	if task.HandleDefCauss == nil || !task.HandleDefCauss.IsInt() {
		return analyzeTask
	}
	defCaus, ok := statsTbl.DeferredCausets[task.HandleDefCauss.GetDefCaus(0).ID]
	if !ok || defCaus.Len() == 0 || defCaus.LastAnalyzePos.IsNull() {
		return analyzeTask
	}
	var oldHist *statistics.Histogram
	if statistics.IsAnalyzed(defCaus.Flag) {
		oldHist = defCaus.Histogram.INTERLOCKy()
	} else {
		d, err := defCaus.LastAnalyzePos.ConvertTo(b.ctx.GetStochaseinstein_dbars().StmtCtx, defCaus.Tp)
		if err != nil {
			b.err = err
			return nil
		}
		_, bktID := defCaus.LessEventCountWithBktIdx(d)
		if bktID == 0 {
			return analyzeTask
		}
		oldHist = defCaus.TruncateHistogram(bktID)
		oldHist.NDV = int64(oldHist.TotalEventCount())
	}
	exec := analyzeTask.defCausExec
	analyzeTask.taskType = pkIncrementalTask
	analyzeTask.defCausIncrementalExec = &analyzePKIncrementalExec{AnalyzeDeferredCausetsExec: *exec, oldHist: oldHist}
	analyzeTask.job = &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: "analyze incremental primary key"}
	return analyzeTask
}

func (b *executorBuilder) buildAnalyzeFastDeferredCauset(e *AnalyzeExec, task plannercore.AnalyzeDeferredCausetsTask, opts map[ast.AnalyzeOptionType]uint64) {
	findTask := false
	for _, eTask := range e.tasks {
		if eTask.fastExec != nil && eTask.fastExec.blockID.Equals(&task.BlockID) {
			eTask.fastExec.defcausInfo = append(eTask.fastExec.defcausInfo, task.DefCaussInfo...)
			findTask = true
			break
		}
	}
	if !findTask {
		var concurrency int
		concurrency, b.err = getBuildStatsConcurrency(e.ctx)
		if b.err != nil {
			return
		}
		fastExec := &AnalyzeFastExec{
			ctx:            b.ctx,
			blockID:        task.BlockID,
			defcausInfo:    task.DefCaussInfo,
			handleDefCauss: task.HandleDefCauss,
			opts:           opts,
			tblInfo:        task.TblInfo,
			concurrency:    concurrency,
			wg:             &sync.WaitGroup{},
		}
		b.err = fastExec.calculateEstimateSampleStep()
		if b.err != nil {
			return
		}
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: fastTask,
			fastExec: fastExec,
			job:      &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: "fast analyze defCausumns"},
		})
	}
}

func (b *executorBuilder) buildAnalyzeFastIndex(e *AnalyzeExec, task plannercore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64) {
	findTask := false
	for _, eTask := range e.tasks {
		if eTask.fastExec != nil && eTask.fastExec.blockID.Equals(&task.BlockID) {
			eTask.fastExec.idxsInfo = append(eTask.fastExec.idxsInfo, task.IndexInfo)
			findTask = true
			break
		}
	}
	if !findTask {
		var concurrency int
		concurrency, b.err = getBuildStatsConcurrency(e.ctx)
		if b.err != nil {
			return
		}
		fastExec := &AnalyzeFastExec{
			ctx:         b.ctx,
			blockID:     task.BlockID,
			idxsInfo:    []*perceptron.IndexInfo{task.IndexInfo},
			opts:        opts,
			tblInfo:     task.TblInfo,
			concurrency: concurrency,
			wg:          &sync.WaitGroup{},
		}
		b.err = fastExec.calculateEstimateSampleStep()
		if b.err != nil {
			return
		}
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: fastTask,
			fastExec: fastExec,
			job:      &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: "fast analyze index " + task.IndexInfo.Name.O},
		})
	}
}

func (b *executorBuilder) buildAnalyze(v *plannercore.Analyze) Executor {
	e := &AnalyzeExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tasks:        make([]*analyzeTask, 0, len(v.DefCausTasks)+len(v.IdxTasks)),
		wg:           &sync.WaitGroup{},
	}
	enableFastAnalyze := b.ctx.GetStochaseinstein_dbars().EnableFastAnalyze
	autoAnalyze := ""
	if b.ctx.GetStochaseinstein_dbars().InRestrictedALLEGROSQL {
		autoAnalyze = "auto "
	}
	for _, task := range v.DefCausTasks {
		if task.Incremental {
			e.tasks = append(e.tasks, b.buildAnalyzePKIncremental(task, v.Opts))
		} else {
			if enableFastAnalyze {
				b.buildAnalyzeFastDeferredCauset(e, task, v.Opts)
			} else {
				e.tasks = append(e.tasks, b.buildAnalyzeDeferredCausetsPushdown(task, v.Opts, autoAnalyze))
			}
		}
		if b.err != nil {
			return nil
		}
	}
	for _, task := range v.IdxTasks {
		if task.Incremental {
			e.tasks = append(e.tasks, b.buildAnalyzeIndexIncremental(task, v.Opts))
		} else {
			if enableFastAnalyze {
				b.buildAnalyzeFastIndex(e, task, v.Opts)
			} else {
				e.tasks = append(e.tasks, b.buildAnalyzeIndexPushdown(task, v.Opts, autoAnalyze))
			}
		}
		if b.err != nil {
			return nil
		}
	}
	return e
}

func constructDistExec(sctx stochastikctx.Context, plans []plannercore.PhysicalPlan) ([]*fidelpb.Executor, bool, error) {
	streaming := true
	executors := make([]*fidelpb.Executor, 0, len(plans))
	for _, p := range plans {
		execPB, err := p.ToPB(sctx, ekv.EinsteinDB)
		if err != nil {
			return nil, false, err
		}
		if !plannercore.SupportStreaming(p) {
			streaming = false
		}
		executors = append(executors, execPB)
	}
	return executors, streaming, nil
}

// markChildrenUsedDefCauss compares each child with the output schemaReplicant, and mark
// each defCausumn of the child is used by output or not.
func markChildrenUsedDefCauss(outputSchema *expression.Schema, childSchema ...*expression.Schema) (childrenUsed [][]bool) {
	for _, child := range childSchema {
		used := expression.GetUsedList(outputSchema.DeferredCausets, child)
		childrenUsed = append(childrenUsed, used)
	}
	return
}

func constructDistExecForTiFlash(sctx stochastikctx.Context, p plannercore.PhysicalPlan) ([]*fidelpb.Executor, bool, error) {
	execPB, err := p.ToPB(sctx, ekv.TiFlash)
	return []*fidelpb.Executor{execPB}, false, err

}

func (b *executorBuilder) constructPosetDagReq(plans []plannercore.PhysicalPlan, storeType ekv.StoreType) (posetPosetDagReq *fidelpb.PosetDagRequest, streaming bool, err error) {
	posetPosetDagReq = &fidelpb.PosetDagRequest{}
	posetPosetDagReq.TimeZoneName, posetPosetDagReq.TimeZoneOffset = timeutil.Zone(b.ctx.GetStochaseinstein_dbars().Location())
	sc := b.ctx.GetStochaseinstein_dbars().StmtCtx
	if sc.RuntimeStatsDefCausl != nil {
		defCauslExec := true
		posetPosetDagReq.DefCauslectExecutionSummaries = &defCauslExec
	}
	posetPosetDagReq.Flags = sc.PushDownFlags()
	if storeType == ekv.TiFlash {
		var executors []*fidelpb.Executor
		executors, streaming, err = constructDistExecForTiFlash(b.ctx, plans[0])
		posetPosetDagReq.RootExecutor = executors[0]
	} else {
		posetPosetDagReq.Executors, streaming, err = constructDistExec(b.ctx, plans)
	}

	distsql.SetEncodeType(b.ctx, posetPosetDagReq)
	return posetPosetDagReq, streaming, err
}

func (b *executorBuilder) corDefCausInDistPlan(plans []plannercore.PhysicalPlan) bool {
	for _, p := range plans {
		x, ok := p.(*plannercore.PhysicalSelection)
		if !ok {
			continue
		}
		for _, cond := range x.Conditions {
			if len(expression.ExtractCorDeferredCausets(cond)) > 0 {
				return true
			}
		}
	}
	return false
}

// corDefCausInAccess checks whether there's correlated defCausumn in access conditions.
func (b *executorBuilder) corDefCausInAccess(p plannercore.PhysicalPlan) bool {
	var access []expression.Expression
	switch x := p.(type) {
	case *plannercore.PhysicalBlockScan:
		access = x.AccessCondition
	case *plannercore.PhysicalIndexScan:
		access = x.AccessCondition
	}
	for _, cond := range access {
		if len(expression.ExtractCorDeferredCausets(cond)) > 0 {
			return true
		}
	}
	return false
}

func (b *executorBuilder) buildIndexLookUpJoin(v *plannercore.PhysicalIndexJoin) Executor {
	outerExec := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := retTypes(outerExec)
	innerPlan := v.Children()[v.InnerChildIdx]
	innerTypes := make([]*types.FieldType, innerPlan.Schema().Len())
	for i, defCaus := range innerPlan.Schema().DeferredCausets {
		innerTypes[i] = defCaus.RetType
	}

	var (
		outerFilter           []expression.Expression
		leftTypes, rightTypes []*types.FieldType
	)

	if v.InnerChildIdx == 0 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Causet, len(innerTypes))
	}
	hasPrefixDefCaus := false
	for _, l := range v.IdxDefCausLens {
		if l != types.UnspecifiedLength {
			hasPrefixDefCaus = true
			break
		}
	}
	e := &IndexLookUpJoin{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec),
		outerCtx: outerCtx{
			rowTypes: outerTypes,
			filter:   outerFilter,
		},
		innerCtx: innerCtx{
			readerBuilder:    &dataReaderBuilder{Plan: innerPlan, executorBuilder: b},
			rowTypes:         innerTypes,
			defCausLens:      v.IdxDefCausLens,
			hasPrefixDefCaus: hasPrefixDefCaus,
		},
		workerWg:          new(sync.WaitGroup),
		isOuterJoin:       v.JoinType.IsOuterJoin(),
		indexRanges:       v.Ranges,
		keyOff2IdxOff:     v.KeyOff2IdxOff,
		lastDefCausHelper: v.CompareFilters,
	}
	childrenUsedSchema := markChildrenUsedDefCauss(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema())
	e.joiner = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema)
	outerKeyDefCauss := make([]int, len(v.OuterJoinKeys))
	for i := 0; i < len(v.OuterJoinKeys); i++ {
		outerKeyDefCauss[i] = v.OuterJoinKeys[i].Index
	}
	e.outerCtx.keyDefCauss = outerKeyDefCauss
	innerKeyDefCauss := make([]int, len(v.InnerJoinKeys))
	for i := 0; i < len(v.InnerJoinKeys); i++ {
		innerKeyDefCauss[i] = v.InnerJoinKeys[i].Index
	}
	e.innerCtx.keyDefCauss = innerKeyDefCauss
	e.joinResult = newFirstChunk(e)
	executorCounterIndexLookUpJoin.Inc()
	return e
}

func (b *executorBuilder) buildIndexLookUpMergeJoin(v *plannercore.PhysicalIndexMergeJoin) Executor {
	outerExec := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := retTypes(outerExec)
	innerPlan := v.Children()[v.InnerChildIdx]
	innerTypes := make([]*types.FieldType, innerPlan.Schema().Len())
	for i, defCaus := range innerPlan.Schema().DeferredCausets {
		innerTypes[i] = defCaus.RetType
	}
	var (
		outerFilter           []expression.Expression
		leftTypes, rightTypes []*types.FieldType
	)
	if v.InnerChildIdx == 0 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Causet, len(innerTypes))
	}
	outerKeyDefCauss := make([]int, len(v.OuterJoinKeys))
	for i := 0; i < len(v.OuterJoinKeys); i++ {
		outerKeyDefCauss[i] = v.OuterJoinKeys[i].Index
	}
	innerKeyDefCauss := make([]int, len(v.InnerJoinKeys))
	for i := 0; i < len(v.InnerJoinKeys); i++ {
		innerKeyDefCauss[i] = v.InnerJoinKeys[i].Index
	}
	executorCounterIndexLookUpJoin.Inc()

	e := &IndexLookUpMergeJoin{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec),
		outerMergeCtx: outerMergeCtx{
			rowTypes:      outerTypes,
			filter:        outerFilter,
			joinKeys:      v.OuterJoinKeys,
			keyDefCauss:   outerKeyDefCauss,
			needOuterSort: v.NeedOuterSort,
			compareFuncs:  v.OuterCompareFuncs,
		},
		innerMergeCtx: innerMergeCtx{
			readerBuilder:           &dataReaderBuilder{Plan: innerPlan, executorBuilder: b},
			rowTypes:                innerTypes,
			joinKeys:                v.InnerJoinKeys,
			keyDefCauss:             innerKeyDefCauss,
			compareFuncs:            v.CompareFuncs,
			defCausLens:             v.IdxDefCausLens,
			desc:                    v.Desc,
			keyOff2KeyOffOrderByIdx: v.KeyOff2KeyOffOrderByIdx,
		},
		workerWg:          new(sync.WaitGroup),
		isOuterJoin:       v.JoinType.IsOuterJoin(),
		indexRanges:       v.Ranges,
		keyOff2IdxOff:     v.KeyOff2IdxOff,
		lastDefCausHelper: v.CompareFilters,
	}
	childrenUsedSchema := markChildrenUsedDefCauss(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema())
	joiners := make([]joiner, e.ctx.GetStochaseinstein_dbars().IndexLookupJoinConcurrency())
	for i := 0; i < len(joiners); i++ {
		joiners[i] = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema)
	}
	e.joiners = joiners
	return e
}

func (b *executorBuilder) buildIndexNestedLoopHashJoin(v *plannercore.PhysicalIndexHashJoin) Executor {
	e := b.buildIndexLookUpJoin(&(v.PhysicalIndexJoin)).(*IndexLookUpJoin)
	idxHash := &IndexNestedLoopHashJoin{
		IndexLookUpJoin: *e,
		keepOuterOrder:  v.KeepOuterOrder,
	}
	concurrency := e.ctx.GetStochaseinstein_dbars().IndexLookupJoinConcurrency()
	idxHash.joiners = make([]joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		idxHash.joiners[i] = e.joiner.Clone()
	}
	return idxHash
}

// containsLimit tests if the execs contains Limit because we do not know whether `Limit` has consumed all of its' source,
// so the feedback may not be accurate.
func containsLimit(execs []*fidelpb.Executor) bool {
	for _, exec := range execs {
		if exec.Limit != nil {
			return true
		}
	}
	return false
}

// When allow batch INTERLOCK is 1, only agg / topN uses batch INTERLOCK.
// When allow batch INTERLOCK is 2, every query uses batch INTERLOCK.
func (e *BlockReaderExecutor) setBatchINTERLOCK(v *plannercore.PhysicalBlockReader) {
	if e.storeType != ekv.TiFlash || e.keepOrder {
		return
	}
	switch e.ctx.GetStochaseinstein_dbars().AllowBatchINTERLOCK {
	case 1:
		for _, p := range v.BlockPlans {
			switch p.(type) {
			case *plannercore.PhysicalHashAgg, *plannercore.PhysicalStreamAgg, *plannercore.PhysicalTopN, *plannercore.PhysicalBroadCastJoin:
				e.batchINTERLOCK = true
			}
		}
	case 2:
		e.batchINTERLOCK = true
	}
	return
}

func buildNoRangeBlockReader(b *executorBuilder, v *plannercore.PhysicalBlockReader) (*BlockReaderExecutor, error) {
	blockPlans := v.BlockPlans
	if v.StoreType == ekv.TiFlash {
		blockPlans = []plannercore.PhysicalPlan{v.GetBlockPlan()}
	}
	posetPosetDagReq, streaming, err := b.constructPosetDagReq(blockPlans, v.StoreType)
	if err != nil {
		return nil, err
	}
	ts := v.GetBlockScan()
	tbl, _ := b.is.BlockByID(ts.Block.ID)
	isPartition, physicalBlockID := ts.IsPartition()
	if isPartition {
		pt := tbl.(block.PartitionedBlock)
		tbl = pt.GetPartition(physicalBlockID)
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &BlockReaderExecutor{
		baseExecutor:       newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		posetPosetDagPB:    posetPosetDagReq,
		startTS:            startTS,
		block:              tbl,
		keepOrder:          ts.KeepOrder,
		desc:               ts.Desc,
		defCausumns:        ts.DeferredCausets,
		streaming:          streaming,
		corDefCausInFilter: b.corDefCausInDistPlan(v.BlockPlans),
		corDefCausInAccess: b.corDefCausInAccess(v.BlockPlans[0]),
		plans:              v.BlockPlans,
		blockPlan:          v.GetBlockPlan(),
		storeType:          v.StoreType,
	}
	e.setBatchINTERLOCK(v)
	e.buildVirtualDeferredCausetInfo()
	if containsLimit(posetPosetDagReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, ts.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(getPhysicalBlockID(tbl), ts.Hist, int64(ts.StatsCount()), ts.Desc)
	}
	defCauslect := statistics.DefCauslectFeedback(b.ctx.GetStochaseinstein_dbars().StmtCtx, e.feedback, len(ts.Ranges))
	if !defCauslect {
		e.feedback.Invalidate()
	}
	e.posetPosetDagPB.DefCauslectRangeCounts = &defCauslect
	if v.StoreType == ekv.MilevaDB && b.ctx.GetStochaseinstein_dbars().User != nil {
		// User info is used to do privilege check. It is only used in MilevaDB cluster memory block.
		e.posetPosetDagPB.User = &fidelpb.UserIdentity{
			UserName: b.ctx.GetStochaseinstein_dbars().User.Username,
			UserHost: b.ctx.GetStochaseinstein_dbars().User.Hostname,
		}
	}

	for i := range v.Schema().DeferredCausets {
		posetPosetDagReq.OutputOffsets = append(posetPosetDagReq.OutputOffsets, uint32(i))
	}

	return e, nil
}

// buildBlockReader builds a block reader executor. It first build a no range block reader,
// and then uFIDelate it ranges from block scan plan.
func (b *executorBuilder) buildBlockReader(v *plannercore.PhysicalBlockReader) Executor {
	if b.ctx.GetStochaseinstein_dbars().IsPessimisticReadConsistency() {
		if err := b.refreshForUFIDelateTSForRC(); err != nil {
			b.err = err
			return nil
		}
	}
	ret, err := buildNoRangeBlockReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	ts := v.GetBlockScan()
	ret.ranges = ts.Ranges
	sctx := b.ctx.GetStochaseinstein_dbars().StmtCtx
	sctx.BlockIDs = append(sctx.BlockIDs, ts.Block.ID)

	if !b.ctx.GetStochaseinstein_dbars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := ts.Block.GetPartitionInfo(); pi == nil {
		return ret
	}

	if v.StoreType == ekv.TiFlash {
		tmp, _ := b.is.BlockByID(ts.Block.ID)
		tbl := tmp.(block.PartitionedBlock)
		partitions, err := partitionPruning(b.ctx, tbl, v.PartitionInfo.PruningConds, v.PartitionInfo.PartitionNames, v.PartitionInfo.DeferredCausets, v.PartitionInfo.DeferredCausetNames)
		if err != nil {
			b.err = err
			return nil
		}
		partsExecutor := make([]Executor, 0, len(partitions))
		for _, part := range partitions {
			exec, err := buildNoRangeBlockReader(b, v)
			if err != nil {
				b.err = err
				return nil
			}
			exec.ranges = ts.Ranges
			nexec, err := nextPartitionForBlockReader{exec: exec}.nextPartition(context.Background(), part)
			if err != nil {
				b.err = err
				return nil
			}
			partsExecutor = append(partsExecutor, nexec)
		}
		if len(partsExecutor) == 0 {
			return &BlockDualExec{baseExecutor: *ret.base()}
		}
		if len(partsExecutor) == 1 {
			return partsExecutor[0]
		}
		return &UnionExec{
			baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID(), partsExecutor...),
			concurrency:  b.ctx.GetStochaseinstein_dbars().UnionConcurrency(),
		}
	}

	nextPartition := nextPartitionForBlockReader{ret}
	exec, err := buildPartitionBlock(b, ts.Block, &v.PartitionInfo, ret, nextPartition)
	if err != nil {
		b.err = err
		return nil
	}
	return exec
}

func buildPartitionBlock(b *executorBuilder, tblInfo *perceptron.BlockInfo, partitionInfo *plannercore.PartitionInfo, e Executor, n nextPartition) (Executor, error) {
	tmp, _ := b.is.BlockByID(tblInfo.ID)
	tbl := tmp.(block.PartitionedBlock)
	partitions, err := partitionPruning(b.ctx, tbl, partitionInfo.PruningConds, partitionInfo.PartitionNames, partitionInfo.DeferredCausets, partitionInfo.DeferredCausetNames)
	if err != nil {
		return nil, err
	}

	if len(partitions) == 0 {
		return &BlockDualExec{baseExecutor: *e.base()}, nil
	}
	return &PartitionBlockExecutor{
		baseExecutor:  *e.base(),
		partitions:    partitions,
		nextPartition: n,
	}, nil
}

func buildNoRangeIndexReader(b *executorBuilder, v *plannercore.PhysicalIndexReader) (*IndexReaderExecutor, error) {
	posetPosetDagReq, streaming, err := b.constructPosetDagReq(v.IndexPlans, ekv.EinsteinDB)
	if err != nil {
		return nil, err
	}
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	tbl, _ := b.is.BlockByID(is.Block.ID)
	isPartition, physicalBlockID := is.IsPartition()
	if isPartition {
		pt := tbl.(block.PartitionedBlock)
		tbl = pt.GetPartition(physicalBlockID)
	} else {
		physicalBlockID = is.Block.ID
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &IndexReaderExecutor{
		baseExecutor:          newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		posetPosetDagPB:       posetPosetDagReq,
		startTS:               startTS,
		physicalBlockID:       physicalBlockID,
		block:                 tbl,
		index:                 is.Index,
		keepOrder:             is.KeepOrder,
		desc:                  is.Desc,
		defCausumns:           is.DeferredCausets,
		streaming:             streaming,
		corDefCausInFilter:    b.corDefCausInDistPlan(v.IndexPlans),
		corDefCausInAccess:    b.corDefCausInAccess(v.IndexPlans[0]),
		idxDefCauss:           is.IdxDefCauss,
		defCausLens:           is.IdxDefCausLens,
		plans:                 v.IndexPlans,
		outputDeferredCausets: v.OutputDeferredCausets,
	}
	if containsLimit(posetPosetDagReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(e.physicalBlockID, is.Hist, int64(is.StatsCount()), is.Desc)
	}
	defCauslect := statistics.DefCauslectFeedback(b.ctx.GetStochaseinstein_dbars().StmtCtx, e.feedback, len(is.Ranges))
	if !defCauslect {
		e.feedback.Invalidate()
	}
	e.posetPosetDagPB.DefCauslectRangeCounts = &defCauslect

	for _, defCaus := range v.OutputDeferredCausets {
		posetPosetDagReq.OutputOffsets = append(posetPosetDagReq.OutputOffsets, uint32(defCaus.Index))
	}

	return e, nil
}

func (b *executorBuilder) buildIndexReader(v *plannercore.PhysicalIndexReader) Executor {
	if b.ctx.GetStochaseinstein_dbars().IsPessimisticReadConsistency() {
		if err := b.refreshForUFIDelateTSForRC(); err != nil {
			b.err = err
			return nil
		}
	}
	ret, err := buildNoRangeIndexReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	ret.ranges = is.Ranges
	sctx := b.ctx.GetStochaseinstein_dbars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Block.Name.O+":"+is.Index.Name.O)

	if !b.ctx.GetStochaseinstein_dbars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := is.Block.GetPartitionInfo(); pi == nil {
		return ret
	}

	nextPartition := nextPartitionForIndexReader{exec: ret}
	exec, err := buildPartitionBlock(b, is.Block, &v.PartitionInfo, ret, nextPartition)
	if err != nil {
		b.err = err
	}
	return exec
}

func buildBlockReq(b *executorBuilder, schemaLen int, plans []plannercore.PhysicalPlan) (posetPosetDagReq *fidelpb.PosetDagRequest, streaming bool, val block.Block, err error) {
	blockReq, blockStreaming, err := b.constructPosetDagReq(plans, ekv.EinsteinDB)
	if err != nil {
		return nil, false, nil, err
	}
	for i := 0; i < schemaLen; i++ {
		blockReq.OutputOffsets = append(blockReq.OutputOffsets, uint32(i))
	}
	ts := plans[0].(*plannercore.PhysicalBlockScan)
	tbl, _ := b.is.BlockByID(ts.Block.ID)
	isPartition, physicalBlockID := ts.IsPartition()
	if isPartition {
		pt := tbl.(block.PartitionedBlock)
		tbl = pt.GetPartition(physicalBlockID)
	}
	return blockReq, blockStreaming, tbl, err
}

func buildIndexReq(b *executorBuilder, schemaLen, handleLen int, plans []plannercore.PhysicalPlan) (posetPosetDagReq *fidelpb.PosetDagRequest, streaming bool, err error) {
	indexReq, indexStreaming, err := b.constructPosetDagReq(plans, ekv.EinsteinDB)
	if err != nil {
		return nil, false, err
	}
	indexReq.OutputOffsets = []uint32{}
	for i := 0; i < handleLen; i++ {
		indexReq.OutputOffsets = append(indexReq.OutputOffsets, uint32(schemaLen+i))
	}
	if len(indexReq.OutputOffsets) == 0 {
		indexReq.OutputOffsets = []uint32{uint32(schemaLen)}
	}
	return indexReq, indexStreaming, err
}

func buildNoRangeIndexLookUpReader(b *executorBuilder, v *plannercore.PhysicalIndexLookUpReader) (*IndexLookUpExecutor, error) {
	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	indexReq, indexStreaming, err := buildIndexReq(b, len(is.Index.DeferredCausets), len(v.CommonHandleDefCauss), v.IndexPlans)
	if err != nil {
		return nil, err
	}
	blockReq, blockStreaming, tbl, err := buildBlockReq(b, v.Schema().Len(), v.BlockPlans)
	if err != nil {
		return nil, err
	}
	ts := v.BlockPlans[0].(*plannercore.PhysicalBlockScan)
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &IndexLookUpExecutor{
		baseExecutor:        newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		posetPosetDagPB:     indexReq,
		startTS:             startTS,
		block:               tbl,
		index:               is.Index,
		keepOrder:           is.KeepOrder,
		desc:                is.Desc,
		blockRequest:        blockReq,
		defCausumns:         ts.DeferredCausets,
		indexStreaming:      indexStreaming,
		blockStreaming:      blockStreaming,
		dataReaderBuilder:   &dataReaderBuilder{executorBuilder: b},
		corDefCausInIdxSide: b.corDefCausInDistPlan(v.IndexPlans),
		corDefCausInTblSide: b.corDefCausInDistPlan(v.BlockPlans),
		corDefCausInAccess:  b.corDefCausInAccess(v.IndexPlans[0]),
		idxDefCauss:         is.IdxDefCauss,
		defCausLens:         is.IdxDefCausLens,
		idxPlans:            v.IndexPlans,
		tblPlans:            v.BlockPlans,
		PushedLimit:         v.PushedLimit,
	}

	if containsLimit(indexReq.Executors) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(getPhysicalBlockID(tbl), is.Hist, int64(is.StatsCount()), is.Desc)
	}
	// Do not defCauslect the feedback for block request.
	defCauslectBlock := false
	e.blockRequest.DefCauslectRangeCounts = &defCauslectBlock
	defCauslectIndex := statistics.DefCauslectFeedback(b.ctx.GetStochaseinstein_dbars().StmtCtx, e.feedback, len(is.Ranges))
	if !defCauslectIndex {
		e.feedback.Invalidate()
	}
	e.posetPosetDagPB.DefCauslectRangeCounts = &defCauslectIndex
	if v.ExtraHandleDefCaus != nil {
		e.handleIdx = append(e.handleIdx, v.ExtraHandleDefCaus.Index)
		e.handleDefCauss = []*expression.DeferredCauset{v.ExtraHandleDefCaus}
	} else {
		for _, handleDefCaus := range v.CommonHandleDefCauss {
			e.handleIdx = append(e.handleIdx, handleDefCaus.Index)
		}
		e.handleDefCauss = v.CommonHandleDefCauss
		e.primaryKeyIndex = blocks.FindPrimaryIndex(tbl.Meta())
	}
	return e, nil
}

func (b *executorBuilder) buildIndexLookUpReader(v *plannercore.PhysicalIndexLookUpReader) Executor {
	if b.ctx.GetStochaseinstein_dbars().IsPessimisticReadConsistency() {
		if err := b.refreshForUFIDelateTSForRC(); err != nil {
			b.err = err
			return nil
		}
	}
	ret, err := buildNoRangeIndexLookUpReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	is := v.IndexPlans[0].(*plannercore.PhysicalIndexScan)
	ts := v.BlockPlans[0].(*plannercore.PhysicalBlockScan)

	ret.ranges = is.Ranges
	executorCounterIndexLookUpExecutor.Inc()

	sctx := b.ctx.GetStochaseinstein_dbars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Block.Name.O+":"+is.Index.Name.O)
	sctx.BlockIDs = append(sctx.BlockIDs, ts.Block.ID)

	if !b.ctx.GetStochaseinstein_dbars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := is.Block.GetPartitionInfo(); pi == nil {
		return ret
	}

	nextPartition := nextPartitionForIndexLookUp{exec: ret}
	exec, err := buildPartitionBlock(b, ts.Block, &v.PartitionInfo, ret, nextPartition)
	if err != nil {
		b.err = err
		return nil
	}
	return exec
}

func buildNoRangeIndexMergeReader(b *executorBuilder, v *plannercore.PhysicalIndexMergeReader) (*IndexMergeReaderExecutor, error) {
	partialPlanCount := len(v.PartialPlans)
	partialReqs := make([]*fidelpb.PosetDagRequest, 0, partialPlanCount)
	partialStreamings := make([]bool, 0, partialPlanCount)
	indexes := make([]*perceptron.IndexInfo, 0, partialPlanCount)
	keepOrders := make([]bool, 0, partialPlanCount)
	descs := make([]bool, 0, partialPlanCount)
	feedbacks := make([]*statistics.QueryFeedback, 0, partialPlanCount)
	ts := v.BlockPlans[0].(*plannercore.PhysicalBlockScan)
	for i := 0; i < partialPlanCount; i++ {
		var tempReq *fidelpb.PosetDagRequest
		var tempStreaming bool
		var err error

		feedback := statistics.NewQueryFeedback(0, nil, 0, ts.Desc)
		feedback.Invalidate()
		feedbacks = append(feedbacks, feedback)

		if is, ok := v.PartialPlans[i][0].(*plannercore.PhysicalIndexScan); ok {
			tempReq, tempStreaming, err = buildIndexReq(b, len(is.Index.DeferredCausets), ts.HandleDefCauss.NumDefCauss(), v.PartialPlans[i])
			keepOrders = append(keepOrders, is.KeepOrder)
			descs = append(descs, is.Desc)
			indexes = append(indexes, is.Index)
		} else {
			ts := v.PartialPlans[i][0].(*plannercore.PhysicalBlockScan)
			tempReq, tempStreaming, _, err = buildBlockReq(b, len(ts.DeferredCausets), v.PartialPlans[i])
			keepOrders = append(keepOrders, ts.KeepOrder)
			descs = append(descs, ts.Desc)
			indexes = append(indexes, nil)
		}
		if err != nil {
			return nil, err
		}
		defCauslect := false
		tempReq.DefCauslectRangeCounts = &defCauslect
		partialReqs = append(partialReqs, tempReq)
		partialStreamings = append(partialStreamings, tempStreaming)
	}
	blockReq, blockStreaming, tblInfo, err := buildBlockReq(b, v.Schema().Len(), v.BlockPlans)
	if err != nil {
		return nil, err
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &IndexMergeReaderExecutor{
		baseExecutor:      newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		posetPosetDagPBs:  partialReqs,
		startTS:           startTS,
		block:             tblInfo,
		indexes:           indexes,
		descs:             descs,
		blockRequest:      blockReq,
		defCausumns:       ts.DeferredCausets,
		partialStreamings: partialStreamings,
		blockStreaming:    blockStreaming,
		partialPlans:      v.PartialPlans,
		tblPlans:          v.BlockPlans,
		dataReaderBuilder: &dataReaderBuilder{executorBuilder: b},
		feedbacks:         feedbacks,
		handleDefCauss:    ts.HandleDefCauss,
	}
	defCauslectBlock := false
	e.blockRequest.DefCauslectRangeCounts = &defCauslectBlock
	return e, nil
}

func (b *executorBuilder) buildIndexMergeReader(v *plannercore.PhysicalIndexMergeReader) Executor {
	ret, err := buildNoRangeIndexMergeReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}
	ret.ranges = make([][]*ranger.Range, 0, len(v.PartialPlans))
	sctx := b.ctx.GetStochaseinstein_dbars().StmtCtx
	for i := 0; i < len(v.PartialPlans); i++ {
		if is, ok := v.PartialPlans[i][0].(*plannercore.PhysicalIndexScan); ok {
			ret.ranges = append(ret.ranges, is.Ranges)
			sctx.IndexNames = append(sctx.IndexNames, is.Block.Name.O+":"+is.Index.Name.O)
		} else {
			ret.ranges = append(ret.ranges, v.PartialPlans[i][0].(*plannercore.PhysicalBlockScan).Ranges)
			if ret.block.Meta().IsCommonHandle {
				tblInfo := ret.block.Meta()
				sctx.IndexNames = append(sctx.IndexNames, tblInfo.Name.O+":"+blocks.FindPrimaryIndex(tblInfo).Name.O)
			}
		}
	}
	ts := v.BlockPlans[0].(*plannercore.PhysicalBlockScan)
	sctx.BlockIDs = append(sctx.BlockIDs, ts.Block.ID)
	executorCounterIndexMergeReaderExecutor.Inc()

	if !b.ctx.GetStochaseinstein_dbars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := ts.Block.GetPartitionInfo(); pi == nil {
		return ret
	}

	nextPartition := nextPartitionForIndexMerge{ret}
	exec, err := buildPartitionBlock(b, ts.Block, &v.PartitionInfo, ret, nextPartition)
	if err != nil {
		b.err = err
		return nil
	}
	return exec
}

// dataReaderBuilder build an executor.
// The executor can be used to read data in the ranges which are constructed by datums.
// Differences from executorBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result executor is already opened.
type dataReaderBuilder struct {
	plannercore.Plan
	*executorBuilder

	selectResultHook // for testing
}

type mockPhysicalIndexReader struct {
	plannercore.PhysicalPlan

	e Executor
}

func (builder *dataReaderBuilder) buildExecutorForIndexJoin(ctx context.Context, lookUpContents []*indexJoinLookUpContent,
	IndexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.DefCausWithCmpFuncManager) (Executor, error) {
	return builder.buildExecutorForIndexJoinInternal(ctx, builder.Plan, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
}

func (builder *dataReaderBuilder) buildExecutorForIndexJoinInternal(ctx context.Context, plan plannercore.Plan, lookUpContents []*indexJoinLookUpContent,
	IndexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.DefCausWithCmpFuncManager) (Executor, error) {
	switch v := plan.(type) {
	case *plannercore.PhysicalBlockReader:
		return builder.buildBlockReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	case *plannercore.PhysicalIndexReader:
		return builder.buildIndexReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	case *plannercore.PhysicalIndexLookUpReader:
		return builder.buildIndexLookUpReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	case *plannercore.PhysicalUnionScan:
		return builder.buildUnionScanForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	// The inner child of IndexJoin might be Projection when a combination of the following conditions is true:
	// 	1. The inner child fetch data using indexLookupReader
	// 	2. PK is not handle
	// 	3. The inner child needs to keep order
	// In this case, an extra defCausumn milevadb_rowid will be appended in the output result of IndexLookupReader(see INTERLOCKTask.doubleReadNeedProj).
	// Then we need a Projection upon IndexLookupReader to prune the redundant defCausumn.
	case *plannercore.PhysicalProjection:
		return builder.buildProjectionForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	// Need to support physical selection because after PR 16389, MilevaDB will push down all the expr supported by EinsteinDB or TiFlash
	// in predicate push down stage, so if there is an expr which only supported by TiFlash, a physical selection will be added after index read
	case *plannercore.PhysicalSelection:
		childExec, err := builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		exec := &SelectionExec{
			baseExecutor: newBaseExecutor(builder.ctx, v.Schema(), v.ID(), childExec),
			filters:      v.Conditions,
		}
		err = exec.open(ctx)
		return exec, err
	case *mockPhysicalIndexReader:
		return v.e, nil
	}
	return nil, errors.New("Wrong plan type for dataReaderBuilder")
}

func (builder *dataReaderBuilder) buildUnionScanForIndexJoin(ctx context.Context, v *plannercore.PhysicalUnionScan,
	values []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.DefCausWithCmpFuncManager) (Executor, error) {
	childBuilder := &dataReaderBuilder{Plan: v.Children()[0], executorBuilder: builder.executorBuilder}
	reader, err := childBuilder.buildExecutorForIndexJoin(ctx, values, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}

	ret := builder.buildUnionScanFromReader(reader, v)
	if us, ok := ret.(*UnionScanExec); ok {
		err = us.open(ctx)
	}
	return ret, err
}

func (builder *dataReaderBuilder) buildBlockReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalBlockReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.DefCausWithCmpFuncManager) (Executor, error) {
	e, err := buildNoRangeBlockReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}
	tbInfo := e.block.Meta()
	if v.IsCommonHandle {
		kvRanges, err := buildKvRangesForIndexJoin(e.ctx, getPhysicalBlockID(e.block), -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		if tbInfo.GetPartitionInfo() == nil {
			return builder.buildBlockReaderFromKvRanges(ctx, e, kvRanges)
		}
		e.kvRangeBuilder = kvRangeBuilderFromFunc(func(pid int64) ([]ekv.KeyRange, error) {
			return buildKvRangesForIndexJoin(e.ctx, pid, -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		})
		nextPartition := nextPartitionForBlockReader{e}
		return buildPartitionBlock(builder.executorBuilder, tbInfo, &v.PartitionInfo, e, nextPartition)
	}
	handles := make([]ekv.Handle, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		isValidHandle := true
		handle := ekv.IntHandle(content.keys[0].GetInt64())
		for _, key := range content.keys {
			if handle.IntValue() != key.GetInt64() {
				isValidHandle = false
				break
			}
		}
		if isValidHandle {
			handles = append(handles, handle)
		}
	}

	if tbInfo.GetPartitionInfo() == nil {
		return builder.buildBlockReaderFromHandles(ctx, e, handles)
	}
	if !builder.ctx.GetStochaseinstein_dbars().UseDynamicPartitionPrune() {
		return builder.buildBlockReaderFromHandles(ctx, e, handles)
	}

	e.kvRangeBuilder = kvRangeBuilderFromHandles(handles)
	nextPartition := nextPartitionForBlockReader{e}
	return buildPartitionBlock(builder.executorBuilder, tbInfo, &v.PartitionInfo, e, nextPartition)
}

type kvRangeBuilderFromFunc func(pid int64) ([]ekv.KeyRange, error)

func (h kvRangeBuilderFromFunc) buildKeyRange(pid int64) ([]ekv.KeyRange, error) {
	return h(pid)
}

type kvRangeBuilderFromHandles []ekv.Handle

func (h kvRangeBuilderFromHandles) buildKeyRange(pid int64) ([]ekv.KeyRange, error) {
	handles := []ekv.Handle(h)
	sort.Slice(handles, func(i, j int) bool {
		return handles[i].Compare(handles[j]) < 0
	})
	return distsql.BlockHandlesToKVRanges(pid, handles), nil
}

func (builder *dataReaderBuilder) buildBlockReaderBase(ctx context.Context, e *BlockReaderExecutor, reqBuilderWithRange distsql.RequestBuilder) (*BlockReaderExecutor, error) {
	startTS, err := builder.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	kvReq, err := reqBuilderWithRange.
		SetPosetDagRequest(e.posetPosetDagPB).
		SetStartTS(startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromStochaseinstein_dbars(e.ctx.GetStochaseinstein_dbars()).
		Build()
	if err != nil {
		return nil, err
	}
	e.kvRanges = append(e.kvRanges, kvReq.KeyRanges...)
	e.resultHandler = &blockResultHandler{}
	result, err := builder.SelectResult(ctx, builder.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	e.resultHandler.open(nil, result)
	return e, nil
}

func (builder *dataReaderBuilder) buildBlockReaderFromHandles(ctx context.Context, e *BlockReaderExecutor, handles []ekv.Handle) (*BlockReaderExecutor, error) {
	sort.Slice(handles, func(i, j int) bool {
		return handles[i].Compare(handles[j]) < 0
	})
	var b distsql.RequestBuilder
	b.SetBlockHandles(getPhysicalBlockID(e.block), handles)
	return builder.buildBlockReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildBlockReaderFromKvRanges(ctx context.Context, e *BlockReaderExecutor, ranges []ekv.KeyRange) (Executor, error) {
	var b distsql.RequestBuilder
	b.SetKeyRanges(ranges)
	return builder.buildBlockReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildIndexReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.DefCausWithCmpFuncManager) (Executor, error) {
	e, err := buildNoRangeIndexReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}
	tbInfo := e.block.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetStochaseinstein_dbars().UseDynamicPartitionPrune() {
		kvRanges, err := buildKvRangesForIndexJoin(e.ctx, e.physicalBlockID, e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		err = e.open(ctx, kvRanges)
		return e, err
	}

	e.ranges, err = buildRangesForIndexJoin(e.ctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}
	nextPartition := nextPartitionForIndexReader{exec: e}
	ret, err := buildPartitionBlock(builder.executorBuilder, tbInfo, &v.PartitionInfo, e, nextPartition)
	if err != nil {
		return nil, err
	}
	err = ret.Open(ctx)
	return ret, err
}

func (builder *dataReaderBuilder) buildIndexLookUpReaderForIndexJoin(ctx context.Context, v *plannercore.PhysicalIndexLookUpReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.DefCausWithCmpFuncManager) (Executor, error) {
	e, err := buildNoRangeIndexLookUpReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}

	tbInfo := e.block.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetStochaseinstein_dbars().UseDynamicPartitionPrune() {
		e.kvRanges, err = buildKvRangesForIndexJoin(e.ctx, getPhysicalBlockID(e.block), e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		err = e.open(ctx)
		return e, err
	}

	e.ranges, err = buildRangesForIndexJoin(e.ctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}
	nextPartition := nextPartitionForIndexLookUp{exec: e}
	ret, err := buildPartitionBlock(builder.executorBuilder, tbInfo, &v.PartitionInfo, e, nextPartition)
	if err != nil {
		return nil, err
	}
	err = ret.Open(ctx)
	return ret, err
}

func (builder *dataReaderBuilder) buildProjectionForIndexJoin(ctx context.Context, v *plannercore.PhysicalProjection,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.DefCausWithCmpFuncManager) (Executor, error) {
	physicalIndexLookUp, isDoubleRead := v.Children()[0].(*plannercore.PhysicalIndexLookUpReader)
	if !isDoubleRead {
		return nil, errors.Errorf("inner child of Projection should be IndexLookupReader, but got %T", v)
	}
	childExec, err := builder.buildIndexLookUpReaderForIndexJoin(ctx, physicalIndexLookUp, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}

	e := &ProjectionExec{
		baseExecutor:     newBaseExecutor(builder.ctx, v.Schema(), v.ID(), childExec),
		numWorkers:       int64(builder.ctx.GetStochaseinstein_dbars().ProjectionConcurrency()),
		evaluatorSuit:    expression.NewEvaluatorSuite(v.Exprs, v.AvoidDeferredCausetEvaluator),
		calculateNoDelay: v.CalculateNoDelay,
	}

	// If the calculation event count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(builder.ctx.GetStochaseinstein_dbars().MaxChunkSize) {
		e.numWorkers = 0
	}
	err = e.open(ctx)

	return e, err
}

// buildRangesForIndexJoin builds ekv ranges for index join when the inner plan is index scan plan.
func buildRangesForIndexJoin(ctx stochastikctx.Context, lookUpContents []*indexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.DefCausWithCmpFuncManager) ([]*ranger.Range, error) {
	retRanges := make([]*ranger.Range, 0, len(ranges)*len(lookUpContents))
	lastPos := len(ranges[0].LowVal) - 1
	tmFIDelatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.keys[keyOff]
				ran.HighVal[idxOff] = content.keys[keyOff]
			}
		}
		if cwc == nil {
			// A deep INTERLOCKy is need here because the old []*range.Range is overwriten
			for _, ran := range ranges {
				retRanges = append(retRanges, ran.Clone())
			}
			continue
		}
		nextDefCausRanges, err := cwc.BuildRangesByEvent(ctx, content.event)
		if err != nil {
			return nil, err
		}
		for _, nextDefCausRan := range nextDefCausRanges {
			for _, ran := range ranges {
				ran.LowVal[lastPos] = nextDefCausRan.LowVal[0]
				ran.HighVal[lastPos] = nextDefCausRan.HighVal[0]
				ran.LowExclude = nextDefCausRan.LowExclude
				ran.HighExclude = nextDefCausRan.HighExclude
				tmFIDelatumRanges = append(tmFIDelatumRanges, ran.Clone())
			}
		}
	}

	if cwc == nil {
		return retRanges, nil
	}

	return ranger.UnionRanges(ctx.GetStochaseinstein_dbars().StmtCtx, tmFIDelatumRanges, true)
}

// buildKvRangesForIndexJoin builds ekv ranges for index join when the inner plan is index scan plan.
func buildKvRangesForIndexJoin(ctx stochastikctx.Context, blockID, indexID int64, lookUpContents []*indexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *plannercore.DefCausWithCmpFuncManager) (_ []ekv.KeyRange, err error) {
	kvRanges := make([]ekv.KeyRange, 0, len(ranges)*len(lookUpContents))
	lastPos := len(ranges[0].LowVal) - 1
	sc := ctx.GetStochaseinstein_dbars().StmtCtx
	tmFIDelatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.keys[keyOff]
				ran.HighVal[idxOff] = content.keys[keyOff]
			}
		}
		if cwc == nil {
			// Index id is -1 means it's a common handle.
			var tmpKvRanges []ekv.KeyRange
			var err error
			if indexID == -1 {
				tmpKvRanges, err = distsql.CommonHandleRangesToKVRanges(sc, blockID, ranges)
			} else {
				tmpKvRanges, err = distsql.IndexRangesToKVRanges(sc, blockID, indexID, ranges, nil)
			}
			if err != nil {
				return nil, err
			}
			kvRanges = append(kvRanges, tmpKvRanges...)
			continue
		}
		nextDefCausRanges, err := cwc.BuildRangesByEvent(ctx, content.event)
		if err != nil {
			return nil, err
		}
		for _, nextDefCausRan := range nextDefCausRanges {
			for _, ran := range ranges {
				ran.LowVal[lastPos] = nextDefCausRan.LowVal[0]
				ran.HighVal[lastPos] = nextDefCausRan.HighVal[0]
				ran.LowExclude = nextDefCausRan.LowExclude
				ran.HighExclude = nextDefCausRan.HighExclude
				tmFIDelatumRanges = append(tmFIDelatumRanges, ran.Clone())
			}
		}
	}

	if cwc == nil {
		sort.Slice(kvRanges, func(i, j int) bool {
			return bytes.Compare(kvRanges[i].StartKey, kvRanges[j].StartKey) < 0
		})
		return kvRanges, nil
	}

	tmFIDelatumRanges, err = ranger.UnionRanges(ctx.GetStochaseinstein_dbars().StmtCtx, tmFIDelatumRanges, true)
	if err != nil {
		return nil, err
	}
	// Index id is -1 means it's a common handle.
	if indexID == -1 {
		return distsql.CommonHandleRangesToKVRanges(ctx.GetStochaseinstein_dbars().StmtCtx, blockID, tmFIDelatumRanges)
	}
	return distsql.IndexRangesToKVRanges(ctx.GetStochaseinstein_dbars().StmtCtx, blockID, indexID, tmFIDelatumRanges, nil)
}

func (b *executorBuilder) buildWindow(v *plannercore.PhysicalWindow) *WindowExec {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	groupByItems := make([]expression.Expression, 0, len(v.PartitionBy))
	for _, item := range v.PartitionBy {
		groupByItems = append(groupByItems, item.DefCaus)
	}
	orderByDefCauss := make([]*expression.DeferredCauset, 0, len(v.OrderBy))
	for _, item := range v.OrderBy {
		orderByDefCauss = append(orderByDefCauss, item.DefCaus)
	}
	windowFuncs := make([]aggfuncs.AggFunc, 0, len(v.WindowFuncDescs))
	partialResults := make([]aggfuncs.PartialResult, 0, len(v.WindowFuncDescs))
	resultDefCausIdx := v.Schema().Len() - len(v.WindowFuncDescs)
	for _, desc := range v.WindowFuncDescs {
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx, desc.Name, desc.Args, false)
		if err != nil {
			b.err = err
			return nil
		}
		agg := aggfuncs.BuildWindowFunctions(b.ctx, aggDesc, resultDefCausIdx, orderByDefCauss)
		windowFuncs = append(windowFuncs, agg)
		partialResult, _ := agg.AllocPartialResult()
		partialResults = append(partialResults, partialResult)
		resultDefCausIdx++
	}
	var processor windowProcessor
	if v.Frame == nil {
		processor = &aggWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
	} else if v.Frame.Type == ast.Events {
		processor = &rowFrameWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
			start:          v.Frame.Start,
			end:            v.Frame.End,
		}
	} else {
		cmpResult := int64(-1)
		if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
			cmpResult = 1
		}
		processor = &rangeFrameWindowProcessor{
			windowFuncs:       windowFuncs,
			partialResults:    partialResults,
			start:             v.Frame.Start,
			end:               v.Frame.End,
			orderByDefCauss:   orderByDefCauss,
			expectedCmpResult: cmpResult,
		}
	}
	return &WindowExec{baseExecutor: base,
		processor:      processor,
		groupChecker:   newVecGroupChecker(b.ctx, groupByItems),
		numWindowFuncs: len(v.WindowFuncDescs),
	}
}

func (b *executorBuilder) buildShuffle(v *plannercore.PhysicalShuffle) *ShuffleExec {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	shuffle := &ShuffleExec{baseExecutor: base,
		concurrency: v.Concurrency,
	}

	switch v.SplitterType {
	case plannercore.PartitionHashSplitterType:
		shuffle.splitter = &partitionHashSplitter{
			byItems:    v.HashByItems,
			numWorkers: shuffle.concurrency,
		}
	default:
		panic("Not implemented. Should not reach here.")
	}

	shuffle.dataSource = b.build(v.DataSource)
	if b.err != nil {
		return nil
	}

	// head & tail of physical plans' chain within "partition".
	var head, tail = v.Children()[0], v.Tail

	shuffle.workers = make([]*shuffleWorker, shuffle.concurrency)
	for i := range shuffle.workers {
		w := &shuffleWorker{
			baseExecutor: newBaseExecutor(b.ctx, v.DataSource.Schema(), v.DataSource.ID()),
		}

		stub := plannercore.PhysicalShuffleDataSourceStub{
			Worker: (unsafe.Pointer)(w),
		}.Init(b.ctx, v.DataSource.Stats(), v.DataSource.SelectBlockOffset(), nil)
		stub.SetSchema(v.DataSource.Schema())

		tail.SetChildren(stub)
		w.childExec = b.build(head)
		if b.err != nil {
			return nil
		}

		shuffle.workers[i] = w
	}

	return shuffle
}

func (b *executorBuilder) buildShuffleDataSourceStub(v *plannercore.PhysicalShuffleDataSourceStub) *shuffleWorker {
	return (*shuffleWorker)(v.Worker)
}

func (b *executorBuilder) buildALLEGROSQLBindExec(v *plannercore.ALLEGROSQLBindPlan) Executor {
	base := newBaseExecutor(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity

	e := &ALLEGROSQLBindExec{
		baseExecutor:        base,
		sqlBindOp:           v.ALLEGROSQLBindOp,
		normdOrigALLEGROSQL: v.NormdOrigALLEGROSQL,
		bindALLEGROSQL:      v.BindALLEGROSQL,
		charset:             v.Charset,
		defCauslation:       v.DefCauslation,
		EDB:                 v.EDB,
		isGlobal:            v.IsGlobal,
		bindAst:             v.BindStmt,
	}
	return e
}

// NewEventDecoder creates a chunk decoder for new event format event value decode.
func NewEventDecoder(ctx stochastikctx.Context, schemaReplicant *expression.Schema, tbl *perceptron.BlockInfo) *rowcodec.ChunkDecoder {
	getDefCausInfoByID := func(tbl *perceptron.BlockInfo, defCausID int64) *perceptron.DeferredCausetInfo {
		for _, defCaus := range tbl.DeferredCausets {
			if defCaus.ID == defCausID {
				return defCaus
			}
		}
		return nil
	}
	var pkDefCauss []int64
	reqDefCauss := make([]rowcodec.DefCausInfo, len(schemaReplicant.DeferredCausets))
	for i := range schemaReplicant.DeferredCausets {
		idx, defCaus := i, schemaReplicant.DeferredCausets[i]
		isPK := (tbl.PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.RetType.Flag)) || defCaus.ID == perceptron.ExtraHandleID
		if isPK {
			pkDefCauss = append(pkDefCauss, defCaus.ID)
		}
		isGeneratedDefCaus := false
		if defCaus.VirtualExpr != nil {
			isGeneratedDefCaus = true
		}
		reqDefCauss[idx] = rowcodec.DefCausInfo{
			ID:                defCaus.ID,
			VirtualGenDefCaus: isGeneratedDefCaus,
			Ft:                defCaus.RetType,
		}
	}
	if len(pkDefCauss) == 0 {
		pkDefCauss = blocks.TryGetCommonPkDeferredCausetIds(tbl)
		if len(pkDefCauss) == 0 {
			pkDefCauss = []int64{0}
		}
	}
	defVal := func(i int, chk *chunk.Chunk) error {
		ci := getDefCausInfoByID(tbl, reqDefCauss[i].ID)
		d, err := block.GetDefCausOriginDefaultValue(ctx, ci)
		if err != nil {
			return err
		}
		chk.AppendCauset(i, &d)
		return nil
	}
	return rowcodec.NewChunkDecoder(reqDefCauss, pkDefCauss, defVal, ctx.GetStochaseinstein_dbars().TimeZone)
}

func (b *executorBuilder) buildBatchPointGet(plan *plannercore.BatchPointGetPlan) Executor {
	if b.ctx.GetStochaseinstein_dbars().IsPessimisticReadConsistency() {
		if err := b.refreshForUFIDelateTSForRC(); err != nil {
			b.err = err
			return nil
		}
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	decoder := NewEventDecoder(b.ctx, plan.Schema(), plan.TblInfo)
	e := &BatchPointGetExec{
		baseExecutor: newBaseExecutor(b.ctx, plan.Schema(), plan.ID()),
		tblInfo:      plan.TblInfo,
		idxInfo:      plan.IndexInfo,
		rowDecoder:   decoder,
		startTS:      startTS,
		keepOrder:    plan.KeepOrder,
		desc:         plan.Desc,
		dagger:       plan.Lock,
		waitTime:     plan.LockWaitTime,
		partPos:      plan.PartitionDefCausPos,
		defCausumns:  plan.DeferredCausets,
	}
	if e.dagger {
		b.hasLock = true
	}
	var capacity int
	if plan.IndexInfo != nil && !isCommonHandleRead(plan.TblInfo, plan.IndexInfo) {
		e.idxVals = plan.IndexValues
		capacity = len(e.idxVals)
	} else {
		// `SELECT a FROM t WHERE a IN (1, 1, 2, 1, 2)` should not return duplicated rows
		handles := make([]ekv.Handle, 0, len(plan.Handles))
		dedup := ekv.NewHandleMap()
		if plan.IndexInfo == nil {
			for _, handle := range plan.Handles {
				if _, found := dedup.Get(handle); found {
					continue
				}
				dedup.Set(handle, true)
				handles = append(handles, handle)
			}
		} else {
			for _, value := range plan.IndexValues {
				handleBytes, err := EncodeUniqueIndexValuesForKey(e.ctx, e.tblInfo, plan.IndexInfo, value)
				if err != nil {
					b.err = err
					return nil
				}
				handle, err := ekv.NewCommonHandle(handleBytes)
				if err != nil {
					b.err = err
					return nil
				}
				if _, found := dedup.Get(handle); found {
					continue
				}
				dedup.Set(handle, true)
				handles = append(handles, handle)
			}
		}
		e.handles = handles
		capacity = len(e.handles)
	}
	e.base().initCap = capacity
	e.base().maxChunkSize = capacity
	e.buildVirtualDeferredCausetInfo()
	return e
}

func isCommonHandleRead(tbl *perceptron.BlockInfo, idx *perceptron.IndexInfo) bool {
	return tbl.IsCommonHandle && idx.Primary
}

func getPhysicalBlockID(t block.Block) int64 {
	if p, ok := t.(block.PhysicalBlock); ok {
		return p.GetPhysicalID()
	}
	return t.Meta().ID
}

func (b *executorBuilder) buildAdminShowTelemetry(v *plannercore.AdminShowTelemetry) Executor {
	return &AdminShowTelemetryExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID())}
}

func (b *executorBuilder) buildAdminResetTelemetryID(v *plannercore.AdminResetTelemetryID) Executor {
	return &AdminResetTelemetryIDExec{baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID())}
}

func partitionPruning(ctx stochastikctx.Context, tbl block.PartitionedBlock, conds []expression.Expression, partitionNames []perceptron.CIStr,
	defCausumns []*expression.DeferredCauset, defCausumnNames types.NameSlice) ([]block.PhysicalBlock, error) {
	idxArr, err := plannercore.PartitionPruning(ctx, tbl, conds, partitionNames, defCausumns, defCausumnNames)
	if err != nil {
		return nil, err
	}

	pi := tbl.Meta().GetPartitionInfo()
	var ret []block.PhysicalBlock
	if fullRangePartition(idxArr) {
		ret = make([]block.PhysicalBlock, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			p := tbl.GetPartition(def.ID)
			ret = append(ret, p)
		}
	} else {
		ret = make([]block.PhysicalBlock, 0, len(idxArr))
		for _, idx := range idxArr {
			pid := pi.Definitions[idx].ID
			p := tbl.GetPartition(pid)
			ret = append(ret, p)
		}
	}
	return ret, nil
}

func fullRangePartition(idxArr []int) bool {
	return len(idxArr) == 1 && idxArr[0] == plannercore.FullRange
}
