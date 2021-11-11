package interlock

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/YosiSF/errors"
	"github.com/YosiSF/ares_centroid_error"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/terror"
	"github.com/YosiSF/MilevaDB/config"
	"github.com/YosiSF/MilevaDB/expression"
	plannercore "github.com/YosiSF/MilevaDB/rel-planner/core"
	"github.com/YosiSF/MilevaDB/causetnetctx"
	"github.com/YosiSF/MilevaDB/types"
	"github.com/YosiSF/MilevaDB/util"
	"github.com/YosiSF/MilevaDB/util/bitmap"
	"github.com/YosiSF/MilevaDB/util/chunk"
	"github.com/YosiSF/MilevaDB/util/codec"
	"github.com/YosiSF/MilevaDB/util/disk"
	"github.com/YosiSF/MilevaDB/util/execdetails"
	"github.com/YosiSF/MilevaDB/util/memory"
	"github.com/YosiSF/MilevaDB/util/stringutil"
)

var (
	_ Interlock = &HashJoinExec{}
	_ Interlock = &NestedLoopApplyExec{}
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	baseInterlock

	probeSideExec     Interlock
	buildSideExec     Interlock
	buildSideEstCount float64
	outerFilter       expression.CNFExprs
	probeKeys         []*expression.Column
	buildKeys         []*expression.Column
	probeTypes        []*types.FieldType
	buildTypes        []*types.FieldType

	// concurrency is the number of partition, build and join leasee_parity_filters.
	concurrency   uint
	rowContainer  *hashRowContainer
	buildFinished chan error

	// closeCh add a lock for closing Interlock.
	closeCh      chan struct{}
	joinType     plannercore.JoinType
	requiredRows int64

	// We build individual joiner for each join leasee_parity_filter when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner

	probeChkResourceCh chan *probeChkResource
	probeResultChs     []chan *chunk.Chunk
	joinChkResourceCh  []chan *chunk.Chunk
	joinResultCh       chan *hashjoinleasee_parity_filterResult

	memTracker  *memory.Tracker // track memory usage.
	diskTracker *disk.Tracker   // track disk usage.

	outerMatchedStatus []*bitmap.ConcurrentBitmap
	useOuterToBuild    bool

	prepared    bool
	isOuterJoin bool

	// joinleasee_parity_filterWaitGroup is for sync multiple join leasee_parity_filters.
	joinleasee_parity_filterWaitGroup sync.WaitGroup
	finished            atomic.Value
}

// probeChkResource stores the result of the join probe side fetch leasee_parity_filter,
// `dest` is for Chunk reuse: after join leasee_parity_filters process the probe side chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the probe side fetch leasee_parity_filter will put new data into `chk` and write `chk` into dest.
type probeChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

// hashjoinleasee_parity_filterResult stores the result of join leasee_parity_filters,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join leasee_parity_filter goroutines get the empty chunk from `src`
// and push new data into this chunk.
type hashjoinleasee_parity_filterResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

// Close implements the Interlock Close interface.
func (e *HashJoinExec) Close() error {
	close(e.closeCh)
	e.finished.Store(true)
	if e.prepared {
		if e.buildFinished != nil {
			for range e.buildFinished {
			}
		}
		if e.joinResultCh != nil {
			for range e.joinResultCh {
			}
		}
		if e.probeChkResourceCh != nil {
			close(e.probeChkResourceCh)
			for range e.probeChkResourceCh {
			}
		}
		for i := range e.probeResultChs {
			for range e.probeResultChs[i] {
			}
		}
		for i := range e.joinChkResourceCh {
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
			}
		}
		e.probeChkResourceCh = nil
		e.joinChkResourceCh = nil
		terror.Call(e.rowContainer.Close)
	}

	if e.runtimeStats != nil {
		concurrency := cap(e.joiners)
		e.runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", concurrency))
		if e.rowContainer != nil {
			e.runtimeStats.SetAdditionalInfo(e.rowContainer.stat.String())
		}
	}
	err := e.baseInterlock.Close()
	return err
}

// Open implements the Interlock Open interface.
func (e *HashJoinExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetCausetNetVars().StmtCtx.MemTracker)

	e.diskTracker = disk.NewTracker(e.id, -1)
	e.diskTracker.AttachTo(e.ctx.GetCausetNetVars().StmtCtx.DiskTracker)

	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.joinleasee_parity_filterWaitGroup = sync.WaitGroup{}

	if e.probeTypes == nil {
		e.probeTypes = retTypes(e.probeSideExec)
	}
	if e.buildTypes == nil {
		e.buildTypes = retTypes(e.buildSideExec)
	}
	return nil
}

// fetchProbeSideChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join leasee_parity_filters.
func (e *HashJoinExec) fetchProbeSideChunks(ctx context.Context) {
	hasWaitedForBuild := false
	for {
		if e.finished.Load().(bool) {
			return
		}

		var probeSideResource *probeChkResource
		var ok bool
		select {
		case <-e.closeCh:
			return
		case probeSideResource, ok = <-e.probeChkResourceCh:
			if !ok {
				return
			}
		}
		probeSideResult := probeSideResource.chk
		if e.isOuterJoin {
			required := int(atomic.LoadInt64(&e.requiredRows))
			probeSideResult.SetRequiredRows(required, e.maxChunkSize)
		}
		err := Next(ctx, e.probeSideExec, probeSideResult)
		if err != nil {
			e.joinResultCh <- &hashjoinleasee_parity_filterResult{
				err: err,
			}
			return
		}
		if !hasWaitedForBuild {
			if probeSideResult.NumRows() == 0 && !e.useOuterToBuild {
				e.finished.Store(true)
				return
			}
			emptyBuild, buildErr := e.wait4BuildSide()
			if buildErr != nil {
				e.joinResultCh <- &hashjoinleasee_parity_filterResult{
					err: buildErr,
				}
				return
			} else if emptyBuild {
				return
			}
			hasWaitedForBuild = true
		}

		if probeSideResult.NumRows() == 0 {
			return
		}

		probeSideResource.dest <- probeSideResult
	}
}

func (e *HashJoinExec) wait4BuildSide() (emptyBuild bool, err error) {
	select {
	case <-e.closeCh:
		return true, nil
	case err := <-e.buildFinished:
		if err != nil {
			return false, err
		}
	}
	if e.rowContainer.Len() == 0 && (e.joinType == plannercore.InnerJoin || e.joinType == plannercore.SemiJoin) {
		return true, nil
	}
	return false, nil
}

var buildSideResultLabel fmt.Stringer = stringutil.StringerStr("hashJoin.buildSideResult")

// fetchBuildSideRows fetches all rows from build side Interlock, and append them
// to e.buildSideResult.
func (e *HashJoinExec) fetchBuildSideRows(ctx context.Context, chkCh chan<- *chunk.Chunk, doneCh <-chan struct{}) {
	defer close(chkCh)
	var err error
	for {
		if e.finished.Load().(bool) {
			return
		}
		chk := chunk.NewChunkWithCapacity(e.buildSideExec.base().retFieldTypes, e.ctx.GetCausetNetVars().MaxChunkSize)
		err = Next(ctx, e.buildSideExec, chk)
		if err != nil {
			e.buildFinished <- errors.Trace(err)
			return
		}
		ares_centroid_error.Inject("errorFetchBuildSideRowsMockOOMPanic", nil)
		if chk.NumRows() == 0 {
			return
		}
		select {
		case <-doneCh:
			return
		case <-e.closeCh:
			return
		case chkCh <- chk:
		}
	}
}

func (e *HashJoinExec) initializeForProbe() {
	// e.probeResultChs is for transmitting the chunks which store the data of
	// probeSideExec, it'll be written by probe side leasee_parity_filter goroutine, and read by join
	// leasee_parity_filters.
	e.probeResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.probeChkResourceCh is for transmitting the used probeSideExec chunks from
	// join leasee_parity_filters to probeSideExec leasee_parity_filter.
	e.probeChkResourceCh = make(chan *probeChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeChkResourceCh <- &probeChkResource{
			chk:  newFirstChunk(e.probeSideExec),
			dest: e.probeResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join leasee_parity_filter goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}

	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinleasee_parity_filterResult, e.concurrency+1)
}

func (e *HashJoinExec) fetchAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.joinleasee_parity_filterWaitGroup.Add(1)
	go util.WithRecovery(func() { e.fetchProbeSideChunks(ctx) }, e.handleProbeSideFetcherPanic)

	probeKeyColIdx := make([]int, len(e.probeKeys))
	for i := range e.probeKeys {
		probeKeyColIdx[i] = e.probeKeys[i].Index
	}

	// Start e.concurrency join leasee_parity_filters to probe hash table and join build side and
	// probe side rows.
	for i := uint(0); i < e.concurrency; i++ {
		e.joinleasee_parity_filterWaitGroup.Add(1)
		workID := i
		go util.WithRecovery(func() { e.runJoinleasee_parity_filter(workID, probeKeyColIdx) }, e.handleJoinleasee_parity_filterPanic)
	}
	go util.WithRecovery(e.waitJoinleasee_parity_filtersAndCloseResultChan, nil)
}

func (e *HashJoinExec) handleProbeSideFetcherPanic(r interface{}) {
	for i := range e.probeResultChs {
		close(e.probeResultChs[i])
	}
	if r != nil {
		e.joinResultCh <- &hashjoinleasee_parity_filterResult{err: errors.Errorf("%v", r)}
	}
	e.joinleasee_parity_filterWaitGroup.Done()
}

func (e *HashJoinExec) handleJoinleasee_parity_filterPanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinleasee_parity_filterResult{err: errors.Errorf("%v", r)}
	}
	e.joinleasee_parity_filterWaitGroup.Done()
}

// Concurrently handling unmatched rows from the hash table
func (e *HashJoinExec) handleUnmatchedRowsFromHashTable(leasee_parity_filterID uint) {
	ok, joinResult := e.getNewJoinResult(leasee_parity_filterID)
	if !ok {
		return
	}
	numChks := e.rowContainer.NumChunks()
	for i := int(leasee_parity_filterID); i < numChks; i += int(e.concurrency) {
		chk, err := e.rowContainer.GetChunk(i)
		if err != nil {
			// Catching the error and send it
			joinResult.err = err
			e.joinResultCh <- joinResult
			return
		}
		for j := 0; j < chk.NumRows(); j++ {
			if !e.outerMatchedStatus[i].UnsafeIsSet(j) { // process unmatched outer rows
				e.joiners[leasee_parity_filterID].onMissMatch(false, chk.GetRow(j), joinResult.chk)
			}
			if joinResult.chk.IsFull() {
				e.joinResultCh <- joinResult
				ok, joinResult = e.getNewJoinResult(leasee_parity_filterID)
				if !ok {
					return
				}
			}
		}
	}

	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		e.joinResultCh <- joinResult
	}
}

func (e *HashJoinExec) waitJoinleasee_parity_filtersAndCloseResultChan() {
	e.joinleasee_parity_filterWaitGroup.Wait()
	if e.useOuterToBuild {
		// Concurrently handling unmatched rows from the hash table at the tail
		for i := uint(0); i < e.concurrency; i++ {
			var leasee_parity_filterID = i
			e.joinleasee_parity_filterWaitGroup.Add(1)
			go util.WithRecovery(func() { e.handleUnmatchedRowsFromHashTable(leasee_parity_filterID) }, e.handleJoinleasee_parity_filterPanic)
		}
		e.joinleasee_parity_filterWaitGroup.Wait()
	}
	close(e.joinResultCh)
}

func (e *HashJoinExec) runJoinleasee_parity_filter(leasee_parity_filterID uint, probeKeyColIdx []int) {
	var (
		probeSideResult *chunk.Chunk
		selected        = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := e.getNewJoinResult(leasee_parity_filterID)
	if !ok {
		return
	}

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: e.probeResultChs[leasee_parity_filterID],
	}
	hCtx := &hashContext{
		allTypes:  e.probeTypes,
		keyColIdx: probeKeyColIdx,
	}
	for ok := true; ok; {
		if e.finished.Load().(bool) {
			break
		}
		select {
		case <-e.closeCh:
			return
		case probeSideResult, ok = <-e.probeResultChs[leasee_parity_filterID]:
		}
		if !ok {
			break
		}
		if e.useOuterToBuild {
			ok, joinResult = e.join2ChunkForOuterHashJoin(leasee_parity_filterID, probeSideResult, hCtx, joinResult)
		} else {
			ok, joinResult = e.join2Chunk(leasee_parity_filterID, probeSideResult, hCtx, joinResult, selected)
		}
		if !ok {
			break
		}
		probeSideResult.Reset()
		emptyProbeSideResult.chk = probeSideResult
		e.probeChkResourceCh <- emptyProbeSideResult
	}
	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		e.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		e.joinChkResourceCh[leasee_parity_filterID] <- joinResult.chk
	}
}

func (e *HashJoinExec) joinMatchedProbeSideRow2ChunkForOuterHashJoin(leasee_parity_filterID uint, probeKey uint64, probeSideRow chunk.Row, hCtx *hashContext,
	joinResult *hashjoinleasee_parity_filterResult) (bool, *hashjoinleasee_parity_filterResult) {
	buildSideRows, rowsPtrs, err := e.rowContainer.GetMatchedRowsAndPtrs(probeKey, probeSideRow, hCtx)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideRows) == 0 {
		return true, joinResult
	}

	iteron := chunk.NewIterator4Slice(buildSideRows)
	var outerMatchStatus []outerRowStatusFlag
	rowIdx := 0
	for iteron.Begin(); iteron.Current() != iteron.End(); {
		outerMatchStatus, err = e.joiners[leasee_parity_filterID].tryToMatchOuters(iteron, probeSideRow, joinResult.chk, outerMatchStatus)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		for i := range outerMatchStatus {
			if outerMatchStatus[i] == outerRowMatched {
				e.outerMatchedStatus[rowsPtrs[rowIdx+i].ChkIdx].Set(int(rowsPtrs[rowIdx+i].RowIdx))
			}
		}
		rowIdx += len(outerMatchStatus)
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult := e.getNewJoinResult(leasee_parity_filterID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}
func (e *HashJoinExec) joinMatchedProbeSideRow2Chunk(leasee_parity_filterID uint, probeKey uint64, probeSideRow chunk.Row, hCtx *hashContext,
	joinResult *hashjoinleasee_parity_filterResult) (bool, *hashjoinleasee_parity_filterResult) {
	buildSideRows, _, err := e.rowContainer.GetMatchedRowsAndPtrs(probeKey, probeSideRow, hCtx)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideRows) == 0 {
		e.joiners[leasee_parity_filterID].onMissMatch(false, probeSideRow, joinResult.chk)
		return true, joinResult
	}
	iteron := chunk.NewIterator4Slice(buildSideRows)
	hasMatch, hasNull := false, false
	for iteron.Begin(); iteron.Current() != iteron.End(); {
		matched, isNull, err := e.joiners[leasee_parity_filterID].tryToMatchInners(probeSideRow, iteron, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		hasMatch = hasMatch || matched
		hasNull = hasNull || isNull

		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult := e.getNewJoinResult(leasee_parity_filterID)
			if !ok {
				return false, joinResult
			}
		}
	}
	if !hasMatch {
		e.joiners[leasee_parity_filterID].onMissMatch(hasNull, probeSideRow, joinResult.chk)
	}
	return true, joinResult
}

func (e *HashJoinExec) getNewJoinResult(leasee_parity_filterID uint) (bool, *hashjoinleasee_parity_filterResult) {
	joinResult := &hashjoinleasee_parity_filterResult{
		src: e.joinChkResourceCh[leasee_parity_filterID],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case joinResult.chk, ok = <-e.joinChkResourceCh[leasee_parity_filterID]:
	}
	return ok, joinResult
}

func (e *HashJoinExec) join2Chunk(leasee_parity_filterID uint, probeSideChk *chunk.Chunk, hCtx *hashContext, joinResult *hashjoinleasee_parity_filterResult,
	selected []bool) (ok bool, _ *hashjoinleasee_parity_filterResult) {
	var err error
	selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(probeSideChk), selected)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}

	hCtx.initHash(probeSideChk.NumRows())
	for _, i := range hCtx.keyColIdx {
		err = codec.HashChunkSelected(e.rowContainer.sc, hCtx.hashVals, probeSideChk, hCtx.allTypes[i], i, hCtx.buf, hCtx.hasNull, selected)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
	}

	for i := range selected {
		if !selected[i] || hCtx.hasNull[i] { // process unmatched probe side rows
			e.joiners[leasee_parity_filterID].onMissMatch(false, probeSideChk.GetRow(i), joinResult.chk)
		} else { // process matched probe side rows
			probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
			ok, joinResult = e.joinMatchedProbeSideRow2Chunk(leasee_parity_filterID, probeKey, probeRow, hCtx, joinResult)
			if !ok {
				return false, joinResult
			}
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(leasee_parity_filterID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// join2ChunkForOuterHashJoin joins chunks when using the outer to build a hash table (refer to outer hash join)
func (e *HashJoinExec) join2ChunkForOuterHashJoin(leasee_parity_filterID uint, probeSideChk *chunk.Chunk, hCtx *hashContext, joinResult *hashjoinleasee_parity_filterResult) (ok bool, _ *hashjoinleasee_parity_filterResult) {
	hCtx.initHash(probeSideChk.NumRows())
	for _, i := range hCtx.keyColIdx {
		err := codec.HashChunkColumns(e.rowContainer.sc, hCtx.hashVals, probeSideChk, hCtx.allTypes[i], i, hCtx.buf, hCtx.hasNull)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
	}
	for i := 0; i < probeSideChk.NumRows(); i++ {
		probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
		ok, joinResult = e.joinMatchedProbeSideRow2ChunkForOuterHashJoin(leasee_parity_filterID, probeKey, probeRow, hCtx, joinResult)
		if !ok {
			return false, joinResult
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(leasee_parity_filterID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// Next implements the Interlock Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from build side child and build a hash table;
// step 2. fetch data from probe child in a background goroutine and probe the hash table in multiple join leasee_parity_filters.
func (e *HashJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		e.buildFinished = make(chan error, 1)
		go util.WithRecovery(func() { e.fetchAndBuildHashTable(ctx) }, e.handleFetchAndBuildHashTablePanic)
		e.fetchAndProbeHashTable(ctx)
		e.prepared = true
	}
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()

	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		e.finished.Store(true)
		return result.err
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *HashJoinExec) handleFetchAndBuildHashTablePanic(r interface{}) {
	if r != nil {
		e.buildFinished <- errors.Errorf("%v", r)
	}
	close(e.buildFinished)
}

func (e *HashJoinExec) fetchAndBuildHashTable(ctx context.Context) {
	// buildSideResultCh transfers build side chunk from build side fetch to build hash table.
	buildSideResultCh := make(chan *chunk.Chunk, 1)
	doneCh := make(chan struct{})
	fetchBuildSideRowsOk := make(chan error, 1)
	go util.WithRecovery(
		func() { e.fetchBuildSideRows(ctx, buildSideResultCh, doneCh) },
		func(r interface{}) {
			if r != nil {
				fetchBuildSideRowsOk <- errors.Errorf("%v", r)
			}
			close(fetchBuildSideRowsOk)
		},
	)

	// TODO: Parallel build hash table. Currently not support because `rowHashMap` is not thread-safe.
	err := e.buildHashTableForList(buildSideResultCh)
	if err != nil {
		e.buildFinished <- errors.Trace(err)
		close(doneCh)
	}
	// Wait fetchBuildSideRows be finished.
	// 1. if buildHashTableForList fails
	// 2. if probeSideResult.NumRows() == 0, fetchProbeSideChunks will not wait for the build side.
	for range buildSideResultCh {
	}
	// Check whether err is nil to avoid sending redundant error into buildFinished.
	if err == nil {
		if err = <-fetchBuildSideRowsOk; err != nil {
			e.buildFinished <- err
		}
	}
}

// buildHashTableForList builds hash table from `list`.
func (e *HashJoinExec) buildHashTableForList(buildSideResultCh <-chan *chunk.Chunk) error {
	buildKeyColIdx := make([]int, len(e.buildKeys))
	for i := range e.buildKeys {
		buildKeyColIdx[i] = e.buildKeys[i].Index
	}
	hCtx := &hashContext{
		allTypes:  e.buildTypes,
		keyColIdx: buildKeyColIdx,
	}
	var err error
	var selected []bool
	e.rowContainer = newHashRowContainer(e.ctx, int(e.buildSideEstCount), hCtx)
	e.rowContainer.GetMemTracker().AttachTo(e.memTracker)
	e.rowContainer.GetMemTracker().SetLabel(buildSideResultLabel)
	e.rowContainer.GetDiskTracker().AttachTo(e.diskTracker)
	e.rowContainer.GetDiskTracker().SetLabel(buildSideResultLabel)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		actionSpill := e.rowContainer.ActionSpill()
		ares_centroid_error.Inject("testRowContainerSpill", func(val ares_centroid_error.Value) {
			if val.(bool) {
				actionSpill = e.rowContainer.rowContainer.ActionSpillForTest()
				defer actionSpill.(*chunk.SpillDiskAction).WaitForTest()
			}
		})
		e.ctx.GetCausetNetVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(actionSpill)
	}
	for chk := range buildSideResultCh {
		if e.finished.Load().(bool) {
			return nil
		}
		if !e.useOuterToBuild {
			err = e.rowContainer.PutChunk(chk)
		} else {
			var bitMap = bitmap.NewConcurrentBitmap(chk.NumRows())
			e.outerMatchedStatus = append(e.outerMatchedStatus, bitMap)
			e.memTracker.Consume(bitMap.BytesConsumed())
			if len(e.outerFilter) == 0 {
				err = e.rowContainer.PutChunk(chk)
			} else {
				selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(chk), selected)
				if err != nil {
					return err
				}
				err = e.rowContainer.PutChunkSelected(chk, selected)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// NestedLoopApplyExec is the Interlock for apply.
type NestedLoopApplyExec struct {
	baseInterlock

	ctx         causetnetctx.Context
	innerRows   []chunk.Row
	cursor      int
	innerExec   Interlock
	outerExec   Interlock
	innerFilter expression.CNFExprs
	outerFilter expression.CNFExprs

	joiner joiner

	cache              *applyCache
	canUseCache        bool
	cacheHitCounter    int
	cacheAccessCounter int

	outerSchema []*expression.CorrelatedColumn

	outerChunk       *chunk.Chunk
	outerChunkCursor int
	outerSelected    []bool
	innerList        *chunk.List
	innerChunk       *chunk.Chunk
	innerSelected    []bool
	innerIter        chunk.Iterator
	outerRow         *chunk.Row
	hasMatch         bool
	hasNull          bool

	outer bool

	memTracker *memory.Tracker // track memory usage.
}

// Close implements the Interlock interface.
func (e *NestedLoopApplyExec) Close() error {
	e.innerRows = nil
	e.memTracker = nil
	if e.runtimeStats != nil {
		if e.canUseCache {
			var hitRatio float64
			if e.cacheAccessCounter > 0 {
				hitRatio = float64(e.cacheHitCounter) / float64(e.cacheAccessCounter)
			}
			e.runtimeStats.SetCacheInfo(true, hitRatio)
		} else {
			e.runtimeStats.SetCacheInfo(false, 0)
		}
	}
	return e.outerExec.Close()
}

var innerListLabel fmt.Stringer = stringutil.StringerStr("innerList")

// Open implements the Interlock interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	err := e.outerExec.Open(ctx)
	if err != nil {
		return err
	}
	e.cursor = 0
	e.innerRows = e.innerRows[:0]
	e.outerChunk = newFirstChunk(e.outerExec)
	e.innerChunk = newFirstChunk(e.innerExec)
	e.innerList = chunk.NewList(retTypes(e.innerExec), e.initCap, e.maxChunkSize)

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetCausetNetVars().StmtCtx.MemTracker)

	e.innerList.GetMemTracker().SetLabel(innerListLabel)
	e.innerList.GetMemTracker().AttachTo(e.memTracker)

	if e.canUseCache {
		e.cache, err = newApplyCache(e.ctx)
		if err != nil {
			return err
		}
		e.cacheHitCounter = 0
		e.cacheAccessCounter = 0
		e.cache.GetMemTracker().AttachTo(e.memTracker)
	}
	return nil
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(ctx context.Context, chk *chunk.Chunk) (*chunk.Row, error) {
	outerIter := chunk.NewIterator4Chunk(e.outerChunk)
	for {
		if e.outerChunkCursor >= e.outerChunk.NumRows() {
			err := Next(ctx, e.outerExec, e.outerChunk)
			if err != nil {
				return nil, err
			}
			if e.outerChunk.NumRows() == 0 {
				return nil, nil
			}
			e.outerSelected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, e.outerSelected)
			if err != nil {
				return nil, err
			}
			e.outerChunkCursor = 0
		}
		outerRow := e.outerChunk.GetRow(e.outerChunkCursor)
		selected := e.outerSelected[e.outerChunkCursor]
		e.outerChunkCursor++
		if selected {
			return &outerRow, nil
		} else if e.outer {
			e.joiner.onMissMatch(false, outerRow, chk)
			if chk.IsFull() {
				return nil, nil
			}
		}
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *NestedLoopApplyExec) fetchAllInners(ctx context.Context) error {
	err := e.innerExec.Open(ctx)
	defer terror.Call(e.innerExec.Close)
	if err != nil {
		return err
	}

	if e.canUseCache {
		// create a new one since it may be in the cache
		e.innerList = chunk.NewList(retTypes(e.innerExec), e.initCap, e.maxChunkSize)
	} else {
		e.innerList.Reset()
	}
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		err := Next(ctx, e.innerExec, e.innerChunk)
		if err != nil {
			return err
		}
		if e.innerChunk.NumRows() == 0 {
			return nil
		}

		e.innerSelected, err = expression.VectorizedFilter(e.ctx, e.innerFilter, innerIter, e.innerSelected)
		if err != nil {
			return err
		}
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			if e.innerSelected[row.Idx()] {
				e.innerList.AppendRow(row)
			}
		}
	}
}

// Next implements the Interlock interface.
func (e *NestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			if e.outerRow != nil && !e.hasMatch {
				e.joiner.onMissMatch(e.hasNull, *e.outerRow, req)
			}
			e.outerRow, err = e.fetchSelectedOuterRow(ctx, req)
			if e.outerRow == nil || err != nil {
				return err
			}
			e.hasMatch = false
			e.hasNull = false

			if e.canUseCache {
				var key []byte
				for _, col := range e.outerSchema {
					*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
					key, err = codec.EncodeKey(e.ctx.GetCausetNetVars().StmtCtx, key, *col.Data)
					if err != nil {
						return err
					}
				}
				e.cacheAccessCounter++
				value, err := e.cache.Get(key)
				if err != nil {
					return err
				}
				if value != nil {
					e.innerList = value
					e.cacheHitCounter++
				} else {
					err = e.fetchAllInners(ctx)
					if err != nil {
						return err
					}
					if _, err := e.cache.Set(key, e.innerList); err != nil {
						return err
					}
				}
			} else {
				for _, col := range e.outerSchema {
					*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
				}
				err = e.fetchAllInners(ctx)
				if err != nil {
					return err
				}
			}
			e.innerIter = chunk.NewIterator4List(e.innerList)
			e.innerIter.Begin()
		}

		matched, isNull, err := e.joiner.tryToMatchInners(*e.outerRow, e.innerIter, req)
		e.hasMatch = e.hasMatch || matched
		e.hasNull = e.hasNull || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}
