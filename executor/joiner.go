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
	"github.com/whtcorpsinc/milevadb/expression"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

var (
	_ joiner = &semiJoiner{}
	_ joiner = &antiSemiJoiner{}
	_ joiner = &leftOuterSemiJoiner{}
	_ joiner = &antiLeftOuterSemiJoiner{}
	_ joiner = &leftOuterJoiner{}
	_ joiner = &rightOuterJoiner{}
	_ joiner = &innerJoiner{}
)

// joiner is used to generate join results according to the join type.
// A typical instruction flow is:
//
//     hasMatch, hasNull := false, false
//     for innerIter.Current() != innerIter.End() {
//         matched, isNull, err := j.tryToMatchInners(outer, innerIter, chk)
//         // handle err
//         hasMatch = hasMatch || matched
//         hasNull = hasNull || isNull
//     }
//     if !hasMatch {
//         j.onMissMatch(hasNull, outer, chk)
//     }
//
// NOTE: This interface is **not** thread-safe.
// TODO: unit test
// for all join type
//     1. no filter, no inline projection
//     2. no filter, inline projection
//     3. no filter, inline projection to empty defCausumn
//     4. filter, no inline projection
//     5. filter, inline projection
//     6. filter, inline projection to empty defCausumn
type joiner interface {
	// tryToMatchInners tries to join an outer event with a batch of inner rows. When
	// 'inners.Len != 0' but all the joined rows are filtered, the outer event is
	// considered unmatched. Otherwise, the outer event is matched and some joined
	// rows are appended to `chk`. The size of `chk` is limited to MaxChunkSize.
	// Note that when the outer event is considered unmatched, we need to differentiate
	// whether the join conditions return null or false, because that matters for
	// AntiSemiJoin/LeftOuterSemiJoin/AntiLeftOuterSemiJoin, by setting the return
	// value isNull; for other join types, isNull is always false.
	//
	// NOTE: Callers need to call this function multiple times to consume all
	// the inner rows for an outer event, and decide whether the outer event can be
	// matched with at lease one inner event.
	tryToMatchInners(outer chunk.Event, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, isNull bool, err error)

	// tryToMatchOuters tries to join a batch of outer rows with one inner event.
	// It's used when the join is an outer join and the hash block is built
	// using the outer side.
	tryToMatchOuters(outer chunk.Iterator, inner chunk.Event, chk *chunk.Chunk, outerEventStatus []outerEventStatusFlag) (_ []outerEventStatusFlag, err error)

	// onMissMatch operates on the unmatched outer event according to the join
	// type. An outer event can be considered miss matched if:
	//   1. it can not pass the filter on the outer block side.
	//   2. there is no inner event with the same join key.
	//   3. all the joined rows can not pass the filter on the join result.
	//
	// On these conditions, the caller calls this function to handle the
	// unmatched outer rows according to the current join type:
	//   1. 'SemiJoin': ignores the unmatched outer event.
	//   2. 'AntiSemiJoin': appends the unmatched outer event to the result buffer.
	//   3. 'LeftOuterSemiJoin': concats the unmatched outer event with 0 and
	//      appends it to the result buffer.
	//   4. 'AntiLeftOuterSemiJoin': concats the unmatched outer event with 1 and
	//      appends it to the result buffer.
	//   5. 'LeftOuterJoin': concats the unmatched outer event with a event of NULLs
	//      and appends it to the result buffer.
	//   6. 'RightOuterJoin': concats the unmatched outer event with a event of NULLs
	//      and appends it to the result buffer.
	//   7. 'InnerJoin': ignores the unmatched outer event.
	//
	// Note that, for LeftOuterSemiJoin, AntiSemiJoin and AntiLeftOuterSemiJoin,
	// we need to know the reason of outer event being treated as unmatched:
	// whether the join condition returns false, or returns null, because
	// it decides if this outer event should be outputted, hence we have a `hasNull`
	// parameter passed to `onMissMatch`.
	onMissMatch(hasNull bool, outer chunk.Event, chk *chunk.Chunk)

	// Clone deep INTERLOCKies a joiner.
	Clone() joiner
}

// JoinerType returns the join type of a Joiner.
func JoinerType(j joiner) plannercore.JoinType {
	switch j.(type) {
	case *semiJoiner:
		return plannercore.SemiJoin
	case *antiSemiJoiner:
		return plannercore.AntiSemiJoin
	case *leftOuterSemiJoiner:
		return plannercore.LeftOuterSemiJoin
	case *antiLeftOuterSemiJoiner:
		return plannercore.AntiLeftOuterSemiJoin
	case *leftOuterJoiner:
		return plannercore.LeftOuterJoin
	case *rightOuterJoiner:
		return plannercore.RightOuterJoin
	default:
		return plannercore.InnerJoin
	}
}

func newJoiner(ctx stochastikctx.Context, joinType plannercore.JoinType,
	outerIsRight bool, defaultInner []types.Causet, filter []expression.Expression,
	lhsDefCausTypes, rhsDefCausTypes []*types.FieldType, childrenUsed [][]bool) joiner {
	base := baseJoiner{
		ctx:          ctx,
		conditions:   filter,
		outerIsRight: outerIsRight,
		maxChunkSize: ctx.GetStochastikVars().MaxChunkSize,
	}
	base.selected = make([]bool, 0, chunk.InitialCapacity)
	base.isNull = make([]bool, 0, chunk.InitialCapacity)
	if childrenUsed != nil {
		base.lUsed = make([]int, 0, len(childrenUsed[0])) // make it non-nil
		for i, used := range childrenUsed[0] {
			if used {
				base.lUsed = append(base.lUsed, i)
			}
		}
		base.rUsed = make([]int, 0, len(childrenUsed[1])) // make it non-nil
		for i, used := range childrenUsed[1] {
			if used {
				base.rUsed = append(base.rUsed, i)
			}
		}
		logutil.BgLogger().Debug("InlineProjection",
			zap.Ints("lUsed", base.lUsed), zap.Ints("rUsed", base.rUsed),
			zap.Int("lCount", len(lhsDefCausTypes)), zap.Int("rCount", len(rhsDefCausTypes)))
	}
	if joinType == plannercore.LeftOuterJoin || joinType == plannercore.RightOuterJoin {
		innerDefCausTypes := lhsDefCausTypes
		if !outerIsRight {
			innerDefCausTypes = rhsDefCausTypes
		}
		base.initDefaultInner(innerDefCausTypes, defaultInner)
	}
	// shallowEventType may be different with the output defCausumns because output defCausumns may
	// be pruned inline, while shallow event should not be because each defCausumn may need
	// be used in filter.
	shallowEventType := make([]*types.FieldType, 0, len(lhsDefCausTypes)+len(rhsDefCausTypes))
	shallowEventType = append(shallowEventType, lhsDefCausTypes...)
	shallowEventType = append(shallowEventType, rhsDefCausTypes...)
	switch joinType {
	case plannercore.SemiJoin:
		base.shallowEvent = chunk.MutEventFromTypes(shallowEventType)
		return &semiJoiner{base}
	case plannercore.AntiSemiJoin:
		base.shallowEvent = chunk.MutEventFromTypes(shallowEventType)
		return &antiSemiJoiner{base}
	case plannercore.LeftOuterSemiJoin:
		base.shallowEvent = chunk.MutEventFromTypes(shallowEventType)
		return &leftOuterSemiJoiner{base}
	case plannercore.AntiLeftOuterSemiJoin:
		base.shallowEvent = chunk.MutEventFromTypes(shallowEventType)
		return &antiLeftOuterSemiJoiner{base}
	case plannercore.LeftOuterJoin, plannercore.RightOuterJoin, plannercore.InnerJoin:
		if len(base.conditions) > 0 {
			base.chk = chunk.NewChunkWithCapacity(shallowEventType, ctx.GetStochastikVars().MaxChunkSize)
		}
		switch joinType {
		case plannercore.LeftOuterJoin:
			return &leftOuterJoiner{base}
		case plannercore.RightOuterJoin:
			return &rightOuterJoiner{base}
		case plannercore.InnerJoin:
			return &innerJoiner{base}
		}
	}
	panic("unsupported join type in func newJoiner()")
}

type outerEventStatusFlag byte

const (
	outerEventUnmatched outerEventStatusFlag = iota
	outerEventMatched
	outerEventHasNull
)

type baseJoiner struct {
	ctx          stochastikctx.Context
	conditions   []expression.Expression
	defaultInner chunk.Event
	outerIsRight bool
	chk          *chunk.Chunk
	shallowEvent   chunk.MutEvent
	selected     []bool
	isNull       []bool
	maxChunkSize int

	// lUsed/rUsed show which defCausumns are used by father for left child and right child.
	// NOTE:
	// 1. every defCausumns are used if lUsed/rUsed is nil.
	// 2. no defCausumns are used if lUsed/rUsed is not nil but the size of lUsed/rUsed is 0.
	lUsed, rUsed []int
}

func (j *baseJoiner) initDefaultInner(innerTypes []*types.FieldType, defaultInner []types.Causet) {
	mublockEvent := chunk.MutEventFromTypes(innerTypes)
	mublockEvent.SetCausets(defaultInner[:len(innerTypes)]...)
	j.defaultInner = mublockEvent.ToEvent()
}

func (j *baseJoiner) makeJoinEventToChunk(chk *chunk.Chunk, lhs, rhs chunk.Event, lUsed, rUsed []int) {
	// Call AppendEvent() first to increment the virtual rows.
	// Fix: https://github.com/whtcorpsinc/milevadb/issues/5771
	lWide := chk.AppendEventByDefCausIdxs(lhs, lUsed)
	chk.AppendPartialEventByDefCausIdxs(lWide, rhs, rUsed)
}

// makeShallowJoinEvent shallow INTERLOCKies `inner` and `outer` into `shallowEvent`.
// It should not consider `j.lUsed` and `j.rUsed`, because the defCausumns which
// need to be used in `j.conditions` may not exist in outputs.
func (j *baseJoiner) makeShallowJoinEvent(isRightJoin bool, inner, outer chunk.Event) {
	if !isRightJoin {
		inner, outer = outer, inner
	}
	j.shallowEvent.ShallowINTERLOCKyPartialEvent(0, inner)
	j.shallowEvent.ShallowINTERLOCKyPartialEvent(inner.Len(), outer)
}

// filter is used to filter the result constructed by tryToMatchInners, the result is
// built by one outer event and multiple inner rows. The returned bool value
// indicates whether the outer event matches any inner rows.
func (j *baseJoiner) filter(
	input, output *chunk.Chunk, outerDefCausLen int,
	lUsed, rUsed []int) (bool, error) {

	var err error
	j.selected, err = expression.VectorizedFilter(j.ctx, j.conditions, chunk.NewIterator4Chunk(input), j.selected)
	if err != nil {
		return false, err
	}
	// Batch INTERLOCKies selected rows to output chunk.
	innerDefCausOffset, outerDefCausOffset := 0, input.NumDefCauss()-outerDefCausLen
	innerDefCausLen := input.NumDefCauss() - outerDefCausLen
	if !j.outerIsRight {
		innerDefCausOffset, outerDefCausOffset = outerDefCausLen, 0
	}
	if lUsed != nil || rUsed != nil {
		lSize := outerDefCausOffset
		if !j.outerIsRight {
			lSize = innerDefCausOffset
		}
		used := make([]int, len(lUsed)+len(rUsed))
		INTERLOCKy(used, lUsed)
		for i := range rUsed {
			used[i+len(lUsed)] = rUsed[i] + lSize
		}
		input = input.Prune(used)

		innerDefCausOffset, outerDefCausOffset = 0, len(lUsed)
		innerDefCausLen, outerDefCausLen = len(lUsed), len(rUsed)
		if !j.outerIsRight {
			innerDefCausOffset, outerDefCausOffset = len(lUsed), 0
			innerDefCausLen, outerDefCausLen = outerDefCausLen, innerDefCausLen
		}

	}
	return chunk.INTERLOCKySelectedJoinEventsWithSameOuterEvents(input, innerDefCausOffset, innerDefCausLen, outerDefCausOffset, outerDefCausLen, j.selected, output)
}

// filterAndCheckOuterEventStatus is used to filter the result constructed by
// tryToMatchOuters, the result is built by multiple outer rows and one inner
// event. The returned outerEventStatusFlag slice value indicates the status of
// each outer event (matched/unmatched/hasNull).
func (j *baseJoiner) filterAndCheckOuterEventStatus(
	input, output *chunk.Chunk, innerDefCaussLen int, outerEventStatus []outerEventStatusFlag,
	lUsed, rUsed []int) ([]outerEventStatusFlag, error) {

	var err error
	j.selected, j.isNull, err = expression.VectorizedFilterConsiderNull(j.ctx, j.conditions, chunk.NewIterator4Chunk(input), j.selected, j.isNull)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(j.selected); i++ {
		if j.isNull[i] {
			outerEventStatus[i] = outerEventHasNull
		} else if !j.selected[i] {
			outerEventStatus[i] = outerEventUnmatched
		}
	}

	if lUsed != nil || rUsed != nil {
		lSize := innerDefCaussLen
		if !j.outerIsRight {
			lSize = input.NumDefCauss() - innerDefCaussLen
		}
		used := make([]int, len(lUsed)+len(rUsed))
		INTERLOCKy(used, lUsed)
		for i := range rUsed {
			used[i+len(lUsed)] = rUsed[i] + lSize
		}
		input = input.Prune(used)
	}
	// Batch INTERLOCKies selected rows to output chunk.
	_, err = chunk.INTERLOCKySelectedJoinEventsDirect(input, j.selected, output)
	return outerEventStatus, err
}

func (j *baseJoiner) Clone() baseJoiner {
	base := baseJoiner{
		ctx:          j.ctx,
		conditions:   make([]expression.Expression, 0, len(j.conditions)),
		outerIsRight: j.outerIsRight,
		maxChunkSize: j.maxChunkSize,
		selected:     make([]bool, 0, len(j.selected)),
		isNull:       make([]bool, 0, len(j.isNull)),
	}
	for _, con := range j.conditions {
		base.conditions = append(base.conditions, con.Clone())
	}
	if j.chk != nil {
		base.chk = j.chk.INTERLOCKyConstruct()
	} else {
		base.shallowEvent = j.shallowEvent.Clone()
	}
	if !j.defaultInner.IsEmpty() {
		base.defaultInner = j.defaultInner.INTERLOCKyConstruct()
	}
	if j.lUsed != nil {
		base.lUsed = make([]int, len(j.lUsed))
		INTERLOCKy(base.lUsed, j.lUsed)
	}
	if j.rUsed != nil {
		base.rUsed = make([]int, len(j.rUsed))
		INTERLOCKy(base.rUsed, j.rUsed)
	}
	return base
}

type semiJoiner struct {
	baseJoiner
}

func (j *semiJoiner) tryToMatchInners(outer chunk.Event, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		chk.AppendEventByDefCausIdxs(outer, j.lUsed) // TODO: need test numVirtualEvent
		inners.ReachEnd()
		return true, false, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinEvent(j.outerIsRight, inner, outer)

		// For SemiJoin, we can safely treat null result of join conditions as false,
		// so we ignore the nullness returned by EvalBool here.
		matched, _, err = expression.EvalBool(j.ctx, j.conditions, j.shallowEvent.ToEvent())
		if err != nil {
			return false, false, err
		}
		if matched {
			chk.AppendEventByDefCausIdxs(outer, j.lUsed)
			inners.ReachEnd()
			return true, false, nil
		}
	}
	err = inners.Error()
	return false, false, err
}

func (j *semiJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Event, chk *chunk.Chunk, outerEventStatus []outerEventStatusFlag) (_ []outerEventStatusFlag, err error) {
	outerEventStatus = outerEventStatus[:0]
	outer, numToAppend := outers.Current(), chk.RequiredEvents()-chk.NumEvents()
	if len(j.conditions) == 0 {
		for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
			chk.AppendEventByDefCausIdxs(outer, j.lUsed)
			outerEventStatus = append(outerEventStatus, outerEventMatched)
		}
		return outerEventStatus, nil
	}
	for outer := outers.Current(); outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
		j.makeShallowJoinEvent(j.outerIsRight, inner, outer)
		// For SemiJoin, we can safely treat null result of join conditions as false,
		// so we ignore the nullness returned by EvalBool here.
		matched, _, err := expression.EvalBool(j.ctx, j.conditions, j.shallowEvent.ToEvent())
		if err != nil {
			return outerEventStatus, err
		}
		if matched {
			outerEventStatus = append(outerEventStatus, outerEventMatched)
			chk.AppendEventByDefCausIdxs(outer, j.lUsed)
		} else {
			outerEventStatus = append(outerEventStatus, outerEventUnmatched)
		}
	}
	err = outers.Error()
	return outerEventStatus, err
}

func (j *semiJoiner) onMissMatch(_ bool, outer chunk.Event, chk *chunk.Chunk) {
}

// Clone implements joiner interface.
func (j *semiJoiner) Clone() joiner {
	return &semiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type antiSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *antiSemiJoiner) tryToMatchInners(outer chunk.Event, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		inners.ReachEnd()
		return true, false, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinEvent(j.outerIsRight, inner, outer)

		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowEvent.ToEvent())
		if err != nil {
			return false, false, err
		}
		if matched {
			inners.ReachEnd()
			return true, false, nil
		}
		hasNull = hasNull || isNull
	}
	err = inners.Error()
	return false, hasNull, err
}

func (j *antiSemiJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Event, chk *chunk.Chunk, outerEventStatus []outerEventStatusFlag) (_ []outerEventStatusFlag, err error) {
	outerEventStatus = outerEventStatus[:0]
	numToAppend := chk.RequiredEvents() - chk.NumEvents()
	if len(j.conditions) == 0 {
		for ; outers.Current() != outers.End(); outers.Next() {
			outerEventStatus = append(outerEventStatus, outerEventMatched)
		}
		return outerEventStatus, nil
	}
	for outer := outers.Current(); outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
		j.makeShallowJoinEvent(j.outerIsRight, inner, outer)
		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowEvent.ToEvent())
		if err != nil {
			return outerEventStatus, err
		}
		if matched {
			outerEventStatus = append(outerEventStatus, outerEventMatched)
		} else if isNull {
			outerEventStatus = append(outerEventStatus, outerEventHasNull)
		} else {
			outerEventStatus = append(outerEventStatus, outerEventUnmatched)
		}
	}
	err = outers.Error()
	return outerEventStatus, err
}

func (j *antiSemiJoiner) onMissMatch(hasNull bool, outer chunk.Event, chk *chunk.Chunk) {
	if !hasNull {
		chk.AppendEventByDefCausIdxs(outer, j.lUsed)
	}
}

func (j *antiSemiJoiner) Clone() joiner {
	return &antiSemiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type leftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *leftOuterSemiJoiner) tryToMatchInners(outer chunk.Event, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, false, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinEvent(false, inner, outer)

		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowEvent.ToEvent())
		if err != nil {
			return false, false, err
		}
		if matched {
			j.onMatch(outer, chk)
			inners.ReachEnd()
			return true, false, nil
		}
		hasNull = hasNull || isNull
	}
	err = inners.Error()
	return false, hasNull, err
}

func (j *leftOuterSemiJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Event, chk *chunk.Chunk, outerEventStatus []outerEventStatusFlag) (_ []outerEventStatusFlag, err error) {
	outerEventStatus = outerEventStatus[:0]
	outer, numToAppend := outers.Current(), chk.RequiredEvents()-chk.NumEvents()
	if len(j.conditions) == 0 {
		for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
			j.onMatch(outer, chk)
			outerEventStatus = append(outerEventStatus, outerEventMatched)
		}
		return outerEventStatus, nil
	}

	for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
		j.makeShallowJoinEvent(false, inner, outer)
		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowEvent.ToEvent())
		if err != nil {
			return nil, err
		}
		if matched {
			j.onMatch(outer, chk)
			outerEventStatus = append(outerEventStatus, outerEventMatched)
		} else if isNull {
			outerEventStatus = append(outerEventStatus, outerEventHasNull)
		} else {
			outerEventStatus = append(outerEventStatus, outerEventUnmatched)
		}
	}
	err = outers.Error()
	return outerEventStatus, err
}

func (j *leftOuterSemiJoiner) onMatch(outer chunk.Event, chk *chunk.Chunk) {
	lWide := chk.AppendEventByDefCausIdxs(outer, j.lUsed)
	chk.AppendInt64(lWide, 1)
}

func (j *leftOuterSemiJoiner) onMissMatch(hasNull bool, outer chunk.Event, chk *chunk.Chunk) {
	lWide := chk.AppendEventByDefCausIdxs(outer, j.lUsed)
	if hasNull {
		chk.AppendNull(lWide)
	} else {
		chk.AppendInt64(lWide, 0)
	}
}

func (j *leftOuterSemiJoiner) Clone() joiner {
	return &leftOuterSemiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type antiLeftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *antiLeftOuterSemiJoiner) tryToMatchInners(outer chunk.Event, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, false, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinEvent(false, inner, outer)

		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowEvent.ToEvent())
		if err != nil {
			return false, false, err
		}
		if matched {
			j.onMatch(outer, chk)
			inners.ReachEnd()
			return true, false, nil
		}
		hasNull = hasNull || isNull
	}
	err = inners.Error()
	return false, hasNull, err
}

func (j *antiLeftOuterSemiJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Event, chk *chunk.Chunk, outerEventStatus []outerEventStatusFlag) (_ []outerEventStatusFlag, err error) {
	outerEventStatus = outerEventStatus[:0]
	outer, numToAppend := outers.Current(), chk.RequiredEvents()-chk.NumEvents()
	if len(j.conditions) == 0 {
		for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
			j.onMatch(outer, chk)
			outerEventStatus = append(outerEventStatus, outerEventMatched)
		}
		return outerEventStatus, nil
	}

	for i := 0; outer != outers.End() && numToAppend > 0; outer, numToAppend, i = outers.Next(), numToAppend-1, i+1 {
		j.makeShallowJoinEvent(false, inner, outer)
		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowEvent.ToEvent())
		if err != nil {
			return nil, err
		}
		if matched {
			j.onMatch(outer, chk)
			outerEventStatus = append(outerEventStatus, outerEventMatched)
		} else if isNull {
			outerEventStatus = append(outerEventStatus, outerEventHasNull)
		} else {
			outerEventStatus = append(outerEventStatus, outerEventUnmatched)
		}
	}
	err = outers.Error()
	if err != nil {
		return
	}
	return outerEventStatus, nil
}

func (j *antiLeftOuterSemiJoiner) onMatch(outer chunk.Event, chk *chunk.Chunk) {
	lWide := chk.AppendEventByDefCausIdxs(outer, j.lUsed)
	chk.AppendInt64(lWide, 0)
}

func (j *antiLeftOuterSemiJoiner) onMissMatch(hasNull bool, outer chunk.Event, chk *chunk.Chunk) {
	lWide := chk.AppendEventByDefCausIdxs(outer, j.lUsed)
	if hasNull {
		chk.AppendNull(lWide)
	} else {
		chk.AppendInt64(lWide, 1)
	}
}

func (j *antiLeftOuterSemiJoiner) Clone() joiner {
	return &antiLeftOuterSemiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type leftOuterJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *leftOuterJoiner) tryToMatchInners(outer chunk.Event, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	numToAppend := chk.RequiredEvents() - chk.NumEvents()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinEventToChunk(chkForJoin, outer, inners.Current(), lUsed, rUsed)
		inners.Next()
	}
	err = inners.Error()
	if err != nil {
		return false, false, err
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err = j.filter(chkForJoin, chk, outer.Len(), lUsedForFilter, rUsedForFilter)
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *leftOuterJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Event, chk *chunk.Chunk, outerEventStatus []outerEventStatusFlag) (_ []outerEventStatusFlag, err error) {
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredEvents()-chk.NumEvents(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		j.makeJoinEventToChunk(chkForJoin, outer, inner, lUsed, rUsed)
	}
	err = outers.Error()
	if err != nil {
		return
	}
	outerEventStatus = outerEventStatus[:0]
	for i := 0; i < cursor; i++ {
		outerEventStatus = append(outerEventStatus, outerEventMatched)
	}
	if len(j.conditions) == 0 {
		return outerEventStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterEventStatus(chkForJoin, chk, inner.Len(), outerEventStatus, lUsedForFilter, rUsedForFilter)
}

func (j *leftOuterJoiner) onMissMatch(_ bool, outer chunk.Event, chk *chunk.Chunk) {
	lWide := chk.AppendEventByDefCausIdxs(outer, j.lUsed)
	chk.AppendPartialEventByDefCausIdxs(lWide, j.defaultInner, j.rUsed)
}

func (j *leftOuterJoiner) Clone() joiner {
	return &leftOuterJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type rightOuterJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *rightOuterJoiner) tryToMatchInners(outer chunk.Event, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	numToAppend := chk.RequiredEvents() - chk.NumEvents()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinEventToChunk(chkForJoin, inners.Current(), outer, lUsed, rUsed)
		inners.Next()
	}
	err = inners.Error()
	if err != nil {
		return false, false, err
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	matched, err = j.filter(chkForJoin, chk, outer.Len(), lUsedForFilter, rUsedForFilter)
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *rightOuterJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Event, chk *chunk.Chunk, outerEventStatus []outerEventStatusFlag) (_ []outerEventStatusFlag, err error) {
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredEvents()-chk.NumEvents(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		j.makeJoinEventToChunk(chkForJoin, inner, outer, lUsed, rUsed)
	}

	outerEventStatus = outerEventStatus[:0]
	for i := 0; i < cursor; i++ {
		outerEventStatus = append(outerEventStatus, outerEventMatched)
	}
	if len(j.conditions) == 0 {
		return outerEventStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterEventStatus(chkForJoin, chk, inner.Len(), outerEventStatus, lUsedForFilter, rUsedForFilter)
}

func (j *rightOuterJoiner) onMissMatch(_ bool, outer chunk.Event, chk *chunk.Chunk) {
	lWide := chk.AppendEventByDefCausIdxs(j.defaultInner, j.lUsed)
	chk.AppendPartialEventByDefCausIdxs(lWide, outer, j.rUsed)
}

func (j *rightOuterJoiner) Clone() joiner {
	return &rightOuterJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type innerJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *innerJoiner) tryToMatchInners(outer chunk.Event, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	inner, numToAppend := inners.Current(), chk.RequiredEvents()-chk.NumEvents()
	for ; inner != inners.End() && numToAppend > 0; inner, numToAppend = inners.Next(), numToAppend-1 {
		if j.outerIsRight {
			j.makeJoinEventToChunk(chkForJoin, inner, outer, lUsed, rUsed)
		} else {
			j.makeJoinEventToChunk(chkForJoin, outer, inner, lUsed, rUsed)
		}
	}
	err = inners.Error()
	if err != nil {
		return false, false, err
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err = j.filter(chkForJoin, chk, outer.Len(), lUsedForFilter, rUsedForFilter)
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *innerJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Event, chk *chunk.Chunk, outerEventStatus []outerEventStatusFlag) (_ []outerEventStatusFlag, err error) {
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredEvents()-chk.NumEvents(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		if j.outerIsRight {
			j.makeJoinEventToChunk(chkForJoin, inner, outer, lUsed, rUsed)
		} else {
			j.makeJoinEventToChunk(chkForJoin, outer, inner, lUsed, rUsed)
		}
	}
	err = outers.Error()
	if err != nil {
		return
	}
	outerEventStatus = outerEventStatus[:0]
	for i := 0; i < cursor; i++ {
		outerEventStatus = append(outerEventStatus, outerEventMatched)
	}
	if len(j.conditions) == 0 {
		return outerEventStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterEventStatus(chkForJoin, chk, inner.Len(), outerEventStatus, lUsedForFilter, rUsedForFilter)
}

func (j *innerJoiner) onMissMatch(_ bool, outer chunk.Event, chk *chunk.Chunk) {
}

func (j *innerJoiner) Clone() joiner {
	return &innerJoiner{baseJoiner: j.baseJoiner.Clone()}
}
