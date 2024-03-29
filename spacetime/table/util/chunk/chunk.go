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

package chunk

import (
	"context"
	"sync"

	"github.com/cznic/mathutil"
	"github.com/twmb/murmur3"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/mysql"
	"github.com/whtcorpsinc/MilevaDB-Prod/Interlock/aggfuncs"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetnetctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetnetctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/execdetails"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/memory"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/set"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

type aggPartialResultMapper map[string][]aggfuncs.PartialResult

// baseHashAggleasee_parity_filter stores the common attributes of HashAggFinalleasee_parity_filter and HashAggPartialleasee_parity_filter.
type baseHashAggleasee_parity_filter struct {
	ctx          causetnetctx.Context
	finishCh     <-chan struct{}
	aggFuncs     []aggfuncs.AggFunc
	maxChunkSize int
}

func newBaseHashAggleasee_parity_filter(ctx causetnetctx.Context, finishCh <-chan struct{}, aggFuncs []aggfuncs.AggFunc, maxChunkSize int) baseHashAggleasee_parity_filter {
	return baseHashAggleasee_parity_filter{
		ctx:          ctx,
		finishCh:     finishCh,
		aggFuncs:     aggFuncs,
		maxChunkSize: maxChunkSize,
	}
}

// HashAggPartialleasee_parity_filter indicates the partial leasee_parity_filters of parallel hash agg execution,
// the number of the leasee_parity_filter can be set by `MilevaDB_hashagg_partial_concurrency`.
type HashAggPartialleasee_parity_filter struct {
	baseHashAggleasee_parity_filter

	inputCh           chan *chunk.Chunk
	outputChs         []chan *HashAggIntermData
	globalOutputCh    chan *AfFinalResult
	giveBackCh        chan<- *HashAggInput
	partialResultsMap aggPartialResultMapper
	groupByItems      []expression.Expression
	groupKey          [][]byte
	// chk stores the input data from child,
	// and is reused by childExec and partial leasee_parity_filter.
	chk        *chunk.Chunk
	memTracker *memory.Tracker
}

// HashAggFinalleasee_parity_filter indicates the final leasee_parity_filters of parallel hash agg execution,
// the number of the leasee_parity_filter can be set by `MilevaDB_hashagg_final_concurrency`.
type HashAggFinalleasee_parity_filter struct {
	baseHashAggleasee_parity_filter

	rowBuffer           []types.CausetObjectQL
	mutableRow          chunk.MutRow
	partialResultMap    aggPartialResultMapper
	groupSet            set.StringSet
	inputCh             chan *HashAggIntermData
	outputCh            chan *AfFinalResult
	finalResultHolderCh chan *chunk.Chunk
	groupKeys           [][]byte
}

// AfFinalResult indicates aggregation functions final result.
type AfFinalResult struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// HashAggExec deals with all the aggregate functions.
// It is built from the Aggregate Plan. When Next() is called, it reads all the data from Src
// and updates all the items in PartialAggFuncs.
// The parallel execution flow is as the following graph shows:
//
//                            +-------------+
//                            | Main Thread |
//                            +------+------+
//                                   ^
//                                   |
//                                   +
//                              +-+-            +-+
//                              | |    ......   | |  finalOutputCh
//                              +++-            +-+
//                               ^
//                               |
//                               +---------------+
//                               |               |
//                 +--------------+             +--------------+
//                 | final leasee_parity_filter |     ......  | final leasee_parity_filter |
//                 +------------+-+             +-+------------+
//                              ^                 ^
//                              |                 |
//                             +-+  +-+  ......  +-+
//                             | |  | |          | |
//                             ...  ...          ...    partialOutputChs
//                             | |  | |          | |
//                             +++  +++          +++
//                              ^    ^            ^
//          +-+                 |    |            |
//          | |        +--------o----+            |
// inputCh  +-+        |        +-----------------+---+
//          | |        |                              |
//          ...    +---+------------+            +----+-----------+
//          | |    | partial leasee_parity_filter |   ......   | partial leasee_parity_filter |
//          +++    +--------------+-+            +-+--------------+
//           |                     ^                ^
//           |                     |                |
//      +----v---------+          +++ +-+          +++
//      | data fetcher | +------> | | | |  ......  | |   partialInputChs
//      +--------------+          +-+ +-+          +-+
type HashAggExec struct {
	baseInterlock

	sc               *stmtctx.StatementContext
	PartialAggFuncs  []aggfuncs.AggFunc
	FinalAggFuncs    []aggfuncs.AggFunc
	partialResultMap aggPartialResultMapper
	groupSet         set.StringSet
	groupKeys        []string
	cursor4GroupKey  int
	GroupByItems     []expression.Expression
	groupKeyBuffer   [][]byte

	finishCh                     chan struct{}
	finalOutputCh                chan *AfFinalResult
	partialOutputChs             []chan *HashAggIntermData
	inputCh                      chan *HashAggInput
	partialInputChs              []chan *chunk.Chunk
	partialleasee_parity_filters []HashAggPartialleasee_parity_filter
	finalleasee_parity_filters   []HashAggFinalleasee_parity_filter
	defaultVal                   *chunk.Chunk
	childResult                  *chunk.Chunk

	// isChildReturnEmpty indicates whether the child Interlock only returns an empty input.
	isChildReturnEmpty bool
	// After we support parallel execution for aggregation functions with distinct,
	// we can remove this attribute.
	isUnparallelExec bool
	prepared         bool
	executed         bool

	memTracker *memory.Tracker // track memory usage.
}

// HashAggInput indicates the input of hash agg exec.
type HashAggInput struct {
	chk *chunk.Chunk
	// giveBackCh is bound with specific partial leasee_parity_filter,
	// it's used to reuse the `chk`,
	// and tell the data-fetcher which partial leasee_parity_filter it should send data to.
	giveBackCh chan<- *chunk.Chunk
}

// HashAggIntermData indicates the intermediate data of aggregation execution.
type HashAggIntermData struct {
	groupKeys        []string
	cursor           int
	partialResultMap aggPartialResultMapper
}

// getPartialResultBatch fetches a batch of partial results from HashAggIntermData.
func (d *HashAggIntermData) getPartialResultBatch(sc *stmtctx.StatementContext, prs [][]aggfuncs.PartialResult, aggFuncs []aggfuncs.AggFunc, maxChunkSize int) (_ [][]aggfuncs.PartialResult, groupKeys []string, reachEnd bool) {
	keyStart := d.cursor
	for ; d.cursor < len(d.groupKeys) && len(prs) < maxChunkSize; d.cursor++ {
		prs = append(prs, d.partialResultMap[d.groupKeys[d.cursor]])
	}
	if d.cursor == len(d.groupKeys) {
		reachEnd = true
	}
	return prs, d.groupKeys[keyStart:d.cursor], reachEnd
}

// Close implements the Interlock Close interface.
func (e *HashAggExec) Close() error {
	if e.isUnparallelExec {
		e.memTracker.Consume(-e.childResult.MemoryUsage())
		e.childResult = nil
		e.groupSet = nil
		e.partialResultMap = nil
		return e.baseInterlock.Close()
	}
	// `Close` may be called after `Open` without calling `Next` in test.
	if !e.prepared {
		close(e.inputCh)
		for _, ch := range e.partialOutputChs {
			close(ch)
		}
		for _, ch := range e.partialInputChs {
			close(ch)
		}
		close(e.finalOutputCh)
	}
	close(e.finishCh)
	for _, ch := range e.partialOutputChs {
		for range ch {
		}
	}
	for _, ch := range e.partialInputChs {
		for chk := range ch {
			e.memTracker.Consume(-chk.MemoryUsage())
		}
	}
	for range e.finalOutputCh {
	}
	e.executed = false

	if e.runtimeStats != nil {
		var partialConcurrency, finalConcurrency int
		if e.isUnparallelExec {
			partialConcurrency = 0
			finalConcurrency = 0
		} else {
			partialConcurrency = cap(e.partialleasee_parity_filters)
			finalConcurrency = cap(e.finalleasee_parity_filters)
		}
		partialConcurrencyInfo := execdetails.NewConcurrencyInfo("PartialConcurrency", partialConcurrency)
		finalConcurrencyInfo := execdetails.NewConcurrencyInfo("FinalConcurrency", finalConcurrency)
		e.runtimeStats.SetConcurrencyInfo(partialConcurrencyInfo, finalConcurrencyInfo)
	}
	return e.baseInterlock.Close()
}

// Open implements the Interlock Open interface.
func (e *HashAggExec) Open(ctx context.Context) error {
	if err := e.baseInterlock.Open(ctx); err != nil {
		return err
	}
	e.prepared = false

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetCausetNetVars().StmtCtx.MemTracker)

	if e.isUnparallelExec {
		e.initForUnparallelExec()
		return nil
	}
	e.initForParallelExec(e.ctx)
	return nil
}

func (e *HashAggExec) initForUnparallelExec() {
	e.groupSet = set.NewStringSet()
	e.partialResultMap = make(aggPartialResultMapper)
	e.groupKeyBuffer = make([][]byte, 0, 8)
	e.childResult = newFirstChunk(e.children[0])
	e.memTracker.Consume(e.childResult.MemoryUsage())
}

func (e *HashAggExec) initForParallelExec(ctx causetnetctx.Context) {
	CausetNetVars := e.ctx.GetCausetNetVars()
	finalConcurrency := CausetNetVars.HashAggFinalConcurrency()
	partialConcurrency := CausetNetVars.HashAggPartialConcurrency()
	e.isChildReturnEmpty = true
	e.finalOutputCh = make(chan *AfFinalResult, finalConcurrency)
	e.inputCh = make(chan *HashAggInput, partialConcurrency)
	e.finishCh = make(chan struct{}, 1)

	e.partialInputChs = make([]chan *chunk.Chunk, partialConcurrency)
	for i := range e.partialInputChs {
		e.partialInputChs[i] = make(chan *chunk.Chunk, 1)
	}
	e.partialOutputChs = make([]chan *HashAggIntermData, finalConcurrency)
	for i := range e.partialOutputChs {
		e.partialOutputChs[i] = make(chan *HashAggIntermData, partialConcurrency)
	}

	e.partialleasee_parity_filters = make([]HashAggPartialleasee_parity_filter, partialConcurrency)
	e.finalleasee_parity_filters = make([]HashAggFinalleasee_parity_filter, finalConcurrency)

	// Init partial leasee_parity_filters.
	for i := 0; i < partialConcurrency; i++ {
		w := HashAggPartialleasee_parity_filter{
			baseHashAggleasee_parity_filter: newBaseHashAggleasee_parity_filter(e.ctx, e.finishCh, e.PartialAggFuncs, e.maxChunkSize),
			inputCh:                         e.partialInputChs[i],
			outputChs:                       e.partialOutputChs,
			giveBackCh:                      e.inputCh,
			globalOutputCh:                  e.finalOutputCh,
			partialResultsMap:               make(aggPartialResultMapper),
			groupByItems:                    e.GroupByItems,
			chk:                             newFirstChunk(e.children[0]),
			groupKey:                        make([][]byte, 0, 8),
			memTracker:                      e.memTracker,
		}
		e.memTracker.Consume(w.chk.MemoryUsage())
		e.partialleasee_parity_filters[i] = w

		input := &HashAggInput{
			chk:        newFirstChunk(e.children[0]),
			giveBackCh: w.inputCh,
		}
		e.memTracker.Consume(input.chk.MemoryUsage())
		e.inputCh <- input
	}

	// Init final leasee_parity_filters.
	for i := 0; i < finalConcurrency; i++ {
		e.finalleasee_parity_filters[i] = HashAggFinalleasee_parity_filter{
			baseHashAggleasee_parity_filter: newBaseHashAggleasee_parity_filter(e.ctx, e.finishCh, e.FinalAggFuncs, e.maxChunkSize),
			partialResultMap:                make(aggPartialResultMapper),
			groupSet:                        set.NewStringSet(),
			inputCh:                         e.partialOutputChs[i],
			outputCh:                        e.finalOutputCh,
			finalResultHolderCh:             make(chan *chunk.Chunk, 1),
			rowBuffer:                       make([]types.CausetObjectQL, 0, e.Schema().Len()),
			mutableRow:                      chunk.MutRowFromTypes(retTypes(e)),
			groupKeys:                       make([][]byte, 0, 8),
		}
		e.finalleasee_parity_filters[i].finalResultHolderCh <- newFirstChunk(e)
	}
}

func (w *HashAggPartialleasee_parity_filter) getChildInput() bool {
	select {
	case <-w.finishCh:
		return false
	case chk, ok := <-w.inputCh:
		if !ok {
			return false
		}
		w.chk.SwapColumns(chk)
		w.giveBackCh <- &HashAggInput{
			chk:        chk,
			giveBackCh: w.inputCh,
		}
	}
	return true
}

func recoveryHashAgg(output chan *AfFinalResult, r interface{}) {
	err := errors.Errorf("%v", r)
	output <- &AfFinalResult{err: errors.Errorf("%v", r)}
	logutil.BgLogger().Error("parallel hash aggregation panicked", zap.Error(err), zap.Stack("stack"))
}

func (w *HashAggPartialleasee_parity_filter) run(ctx causetnetctx.Context, waitGroup *sync.WaitGroup, finalConcurrency int) {
	needShuffle, sc := false, ctx.GetCausetNetVars().StmtCtx
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.globalOutputCh, r)
		}
		if needShuffle {
			w.shuffleIntermData(sc, finalConcurrency)
		}
		w.memTracker.Consume(-w.chk.MemoryUsage())
		waitGroup.Done()
	}()
	for {
		if !w.getChildInput() {
			return
		}
		if err := w.updatePartialResult(ctx, sc, w.chk, len(w.partialResultsMap)); err != nil {
			w.globalOutputCh <- &AfFinalResult{err: err}
			return
		}
		// The intermData can be promised to be not empty if reaching here,
		// so we set needShuffle to be true.
		needShuffle = true
	}
}

func (w *HashAggPartialleasee_parity_filter) updatePartialResult(ctx causetnetctx.Context, sc *stmtctx.StatementContext, chk *chunk.Chunk, finalConcurrency int) (err error) {
	w.groupKey, err = getGroupKey(w.ctx, chk, w.groupKey, w.groupByItems)
	if err != nil {
		return err
	}

	partialResults := w.getPartialResult(sc, w.groupKey, w.partialResultsMap)
	numRows := chk.NumRows()
	rows := make([]chunk.Row, 1)
	for i := 0; i < numRows; i++ {
		for j, af := range w.aggFuncs {
			rows[0] = chk.GetRow(i)
			if _, err := af.UpdatePartialResult(ctx, rows, partialResults[i][j]); err != nil {
				return err
			}
		}
	}
	return nil
}

// shuffleIntermData shuffles the intermediate data of partial leasee_parity_filters to corresponded final leasee_parity_filters.
// We only support parallel execution for single-machine, so process of encode and decode can be skipped.
func (w *HashAggPartialleasee_parity_filter) shuffleIntermData(sc *stmtctx.StatementContext, finalConcurrency int) {
	groupKeysSlice := make([][]string, finalConcurrency)
	for groupKey := range w.partialResultsMap {
		finalleasee_parity_filterIdx := int(murmur3.Sum32([]byte(groupKey))) % finalConcurrency
		if groupKeysSlice[finalleasee_parity_filterIdx] == nil {
			groupKeysSlice[finalleasee_parity_filterIdx] = make([]string, 0, len(w.partialResultsMap)/finalConcurrency)
		}
		groupKeysSlice[finalleasee_parity_filterIdx] = append(groupKeysSlice[finalleasee_parity_filterIdx], groupKey)
	}

	for i := range groupKeysSlice {
		if groupKeysSlice[i] == nil {
			continue
		}
		w.outputChs[i] <- &HashAggIntermData{
			groupKeys:        groupKeysSlice[i],
			partialResultMap: w.partialResultsMap,
		}
	}
}

// getGroupKey evaluates the group items and args of aggregate functions.
func getGroupKey(ctx causetnetctx.Context, input *chunk.Chunk, groupKey [][]byte, groupByItems []expression.Expression) ([][]byte, error) {
	numRows := input.NumRows()
	avlGroupKeyLen := mathutil.Min(len(groupKey), numRows)
	for i := 0; i < avlGroupKeyLen; i++ {
		groupKey[i] = groupKey[i][:0]
	}
	for i := avlGroupKeyLen; i < numRows; i++ {
		groupKey = append(groupKey, make([]byte, 0, 10*len(groupByItems)))
	}

	for _, item := range groupByItems {
		tp := item.GetType()
		buf, err := expression.GetColumn(tp.EvalType(), numRows)
		if err != nil {
			return nil, err
		}

		if err := expression.EvalExpr(ctx, item, input, buf); err != nil {
			expression.PutColumn(buf)
			return nil, err
		}
		// This check is used to avoid error during the execution of `EncodeDecimal`.
		if item.GetType().Tp == mysql.TypeNewDecimal {
			newTp := *tp
			newTp.Flen = 0
			tp = &newTp
		}
		groupKey, err = codec.HashGroupKey(ctx.GetCausetNetVars().StmtCtx, input.NumRows(), buf, groupKey, tp)
		if err != nil {
			expression.PutColumn(buf)
			return nil, err
		}
		expression.PutColumn(buf)
	}
	return groupKey, nil
}

func (w baseHashAggleasee_parity_filter) getPartialResult(sc *stmtctx.StatementContext, groupKey [][]byte, mapper aggPartialResultMapper) [][]aggfuncs.PartialResult {
	n := len(groupKey)
	partialResults := make([][]aggfuncs.PartialResult, n)
	for i := 0; i < n; i++ {
		var ok bool
		if partialResults[i], ok = mapper[string(groupKey[i])]; ok {
			continue
		}
		for _, af := range w.aggFuncs {
			partialResult, _ := af.AllocPartialResult()
			partialResults[i] = append(partialResults[i], partialResult)
		}
		mapper[string(groupKey[i])] = partialResults[i]
	}
	return partialResults
}

func (w *HashAggFinalleasee_parity_filter) getPartialInput() (input *HashAggIntermData, ok bool) {
	select {
	case <-w.finishCh:
		return nil, false
	case input, ok = <-w.inputCh:
		if !ok {
			return nil, false
		}
	}
	return
}

func (w *HashAggFinalleasee_parity_filter) consumeIntermData(sctx causetnetctx.Context) (err error) {
	var (
		input            *HashAggIntermData
		ok               bool
		intermDataBuffer [][]aggfuncs.PartialResult
		groupKeys        []string
		sc               = sctx.GetCausetNetVars().StmtCtx
	)
	for {
		if input, ok = w.getPartialInput(); !ok {
			return nil
		}
		if intermDataBuffer == nil {
			intermDataBuffer = make([][]aggfuncs.PartialResult, 0, w.maxChunkSize)
		}
		// Consume input in batches, size of every batch is less than w.maxChunkSize.
		for reachEnd := false; !reachEnd; {
			intermDataBuffer, groupKeys, reachEnd = input.getPartialResultBatch(sc, intermDataBuffer[:0], w.aggFuncs, w.maxChunkSize)
			groupKeysLen := len(groupKeys)
			w.groupKeys = w.groupKeys[:0]
			for i := 0; i < groupKeysLen; i++ {
				w.groupKeys = append(w.groupKeys, []byte(groupKeys[i]))
			}
			finalPartialResults := w.getPartialResult(sc, w.groupKeys, w.partialResultMap)
			for i, groupKey := range groupKeys {
				if !w.groupSet.Exist(groupKey) {
					w.groupSet.Insert(groupKey)
				}
				prs := intermDataBuffer[i]
				for j, af := range w.aggFuncs {
					if _, err = af.MergePartialResult(sctx, prs[j], finalPartialResults[i][j]); err != nil {
						return err
					}
				}
			}
		}
	}
}
