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

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/executor/aggfuncs"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/errors"
)

// WindowExec is the executor for window functions.
type WindowExec struct {
	baseExecutor

	groupChecker *vecGroupChecker
	// childResult stores the child chunk
	childResult *chunk.Chunk
	// executed indicates the child executor is drained or something unexpected happened.
	executed bool
	// resultChunks stores the chunks to return
	resultChunks []*chunk.Chunk
	// remainingEventsInChunk indicates how many rows the resultChunks[i] is not prepared.
	remainingEventsInChunk []int

	numWindowFuncs int
	processor      windowProcessor
}

// Close implements the Executor Close interface.
func (e *WindowExec) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *WindowExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for !e.executed && !e.preparedChunkAvailable() {
		err := e.consumeOneGroup(ctx)
		if err != nil {
			e.executed = true
			return err
		}
	}
	if len(e.resultChunks) > 0 {
		chk.SwapDeferredCausets(e.resultChunks[0])
		e.resultChunks[0] = nil // GC it. TODO: Reuse it.
		e.resultChunks = e.resultChunks[1:]
		e.remainingEventsInChunk = e.remainingEventsInChunk[1:]
	}
	return nil
}

func (e *WindowExec) preparedChunkAvailable() bool {
	return len(e.resultChunks) > 0 && e.remainingEventsInChunk[0] == 0
}

func (e *WindowExec) consumeOneGroup(ctx context.Context) error {
	var groupEvents []chunk.Event
	if e.groupChecker.isExhausted() {
		eof, err := e.fetchChild(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if eof {
			e.executed = true
			return e.consumeGroupEvents(groupEvents)
		}
		_, err = e.groupChecker.splitIntoGroups(e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
	}
	begin, end := e.groupChecker.getNextGroup()
	for i := begin; i < end; i++ {
		groupEvents = append(groupEvents, e.childResult.GetEvent(i))
	}

	for meetLastGroup := end == e.childResult.NumEvents(); meetLastGroup; {
		meetLastGroup = false
		eof, err := e.fetchChild(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if eof {
			e.executed = true
			return e.consumeGroupEvents(groupEvents)
		}

		isFirstGroupSameAsPrev, err := e.groupChecker.splitIntoGroups(e.childResult)
		if err != nil {
			return errors.Trace(err)
		}

		if isFirstGroupSameAsPrev {
			begin, end = e.groupChecker.getNextGroup()
			for i := begin; i < end; i++ {
				groupEvents = append(groupEvents, e.childResult.GetEvent(i))
			}
			meetLastGroup = end == e.childResult.NumEvents()
		}
	}
	return e.consumeGroupEvents(groupEvents)
}

func (e *WindowExec) consumeGroupEvents(groupEvents []chunk.Event) (err error) {
	remainingEventsInGroup := len(groupEvents)
	if remainingEventsInGroup == 0 {
		return nil
	}
	for i := 0; i < len(e.resultChunks); i++ {
		remained := mathutil.Min(e.remainingEventsInChunk[i], remainingEventsInGroup)
		e.remainingEventsInChunk[i] -= remained
		remainingEventsInGroup -= remained

		// TODO: Combine these three methods.
		// The old implementation needs the processor has these three methods
		// but now it does not have to.
		groupEvents, err = e.processor.consumeGroupEvents(e.ctx, groupEvents)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = e.processor.appendResult2Chunk(e.ctx, groupEvents, e.resultChunks[i], remained)
		if err != nil {
			return errors.Trace(err)
		}
		if remainingEventsInGroup == 0 {
			e.processor.resetPartialResult()
			break
		}
	}
	return nil
}

func (e *WindowExec) fetchChild(ctx context.Context) (EOF bool, err error) {
	childResult := newFirstChunk(e.children[0])
	err = Next(ctx, e.children[0], childResult)
	if err != nil {
		return false, errors.Trace(err)
	}
	// No more data.
	numEvents := childResult.NumEvents()
	if numEvents == 0 {
		return true, nil
	}

	resultChk := chunk.New(e.retFieldTypes, 0, numEvents)
	err = e.INTERLOCKyChk(childResult, resultChk)
	if err != nil {
		return false, err
	}
	e.resultChunks = append(e.resultChunks, resultChk)
	e.remainingEventsInChunk = append(e.remainingEventsInChunk, numEvents)

	e.childResult = childResult
	return false, nil
}

func (e *WindowExec) INTERLOCKyChk(src, dst *chunk.Chunk) error {
	defCausumns := e.Schema().DeferredCausets[:len(e.Schema().DeferredCausets)-e.numWindowFuncs]
	for i, defCaus := range defCausumns {
		if err := dst.MakeRefTo(i, src, defCaus.Index); err != nil {
			return err
		}
	}
	return nil
}

// windowProcessor is the interface for processing different HoTTs of windows.
type windowProcessor interface {
	// consumeGroupEvents uFIDelates the result for an window function using the input rows
	// which belong to the same partition.
	consumeGroupEvents(ctx stochastikctx.Context, rows []chunk.Event) ([]chunk.Event, error)
	// appendResult2Chunk appends the final results to chunk.
	// It is called when there are no more rows in current partition.
	appendResult2Chunk(ctx stochastikctx.Context, rows []chunk.Event, chk *chunk.Chunk, remained int) ([]chunk.Event, error)
	// resetPartialResult resets the partial result to the original state for a specific window function.
	resetPartialResult()
}

type aggWindowProcessor struct {
	windowFuncs    []aggfuncs.AggFunc
	partialResults []aggfuncs.PartialResult
}

func (p *aggWindowProcessor) consumeGroupEvents(ctx stochastikctx.Context, rows []chunk.Event) ([]chunk.Event, error) {
	for i, windowFunc := range p.windowFuncs {
		// @todo Add memory trace
		_, err := windowFunc.UFIDelatePartialResult(ctx, rows, p.partialResults[i])
		if err != nil {
			return nil, err
		}
	}
	rows = rows[:0]
	return rows, nil
}

func (p *aggWindowProcessor) appendResult2Chunk(ctx stochastikctx.Context, rows []chunk.Event, chk *chunk.Chunk, remained int) ([]chunk.Event, error) {
	for remained > 0 {
		for i, windowFunc := range p.windowFuncs {
			// TODO: We can extend the agg func interface to avoid the `for` loop  here.
			err := windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
		}
		remained--
	}
	return rows, nil
}

func (p *aggWindowProcessor) resetPartialResult() {
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
}

type rowFrameWindowProcessor struct {
	windowFuncs    []aggfuncs.AggFunc
	partialResults []aggfuncs.PartialResult
	start          *core.FrameBound
	end            *core.FrameBound
	curEventIdx    uint64
}

func (p *rowFrameWindowProcessor) getStartOffset(numEvents uint64) uint64 {
	if p.start.UnBounded {
		return 0
	}
	switch p.start.Type {
	case ast.Preceding:
		if p.curEventIdx >= p.start.Num {
			return p.curEventIdx - p.start.Num
		}
		return 0
	case ast.Following:
		offset := p.curEventIdx + p.start.Num
		if offset >= numEvents {
			return numEvents
		}
		return offset
	case ast.CurrentEvent:
		return p.curEventIdx
	}
	// It will never reach here.
	return 0
}

func (p *rowFrameWindowProcessor) getEndOffset(numEvents uint64) uint64 {
	if p.end.UnBounded {
		return numEvents
	}
	switch p.end.Type {
	case ast.Preceding:
		if p.curEventIdx >= p.end.Num {
			return p.curEventIdx - p.end.Num + 1
		}
		return 0
	case ast.Following:
		offset := p.curEventIdx + p.end.Num
		if offset >= numEvents {
			return numEvents
		}
		return offset + 1
	case ast.CurrentEvent:
		return p.curEventIdx + 1
	}
	// It will never reach here.
	return 0
}

func (p *rowFrameWindowProcessor) consumeGroupEvents(ctx stochastikctx.Context, rows []chunk.Event) ([]chunk.Event, error) {
	return rows, nil
}

func (p *rowFrameWindowProcessor) appendResult2Chunk(ctx stochastikctx.Context, rows []chunk.Event, chk *chunk.Chunk, remained int) ([]chunk.Event, error) {
	numEvents := uint64(len(rows))
	var (
		err                      error
		initializedSlidingWindow bool
		start                    uint64
		end                      uint64
		lastStart                uint64
		lastEnd                  uint64
		shiftStart               uint64
		shiftEnd                 uint64
	)
	slidingWindowAggFuncs := make([]aggfuncs.SlidingWindowAggFunc, len(p.windowFuncs))
	for i, windowFunc := range p.windowFuncs {
		if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
			slidingWindowAggFuncs[i] = slidingWindowAggFunc
		}
	}
	for ; remained > 0; lastStart, lastEnd = start, end {
		start = p.getStartOffset(numEvents)
		end = p.getEndOffset(numEvents)
		p.curEventIdx++
		remained--
		shiftStart = start - lastStart
		shiftEnd = end - lastEnd
		if start >= end {
			for i, windowFunc := range p.windowFuncs {
				slidingWindowAggFunc := slidingWindowAggFuncs[i]
				if slidingWindowAggFunc != nil && initializedSlidingWindow {
					err = slidingWindowAggFunc.Slide(ctx, rows, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
					if err != nil {
						return nil, err
					}
				}
				err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		for i, windowFunc := range p.windowFuncs {
			slidingWindowAggFunc := slidingWindowAggFuncs[i]
			if slidingWindowAggFunc != nil && initializedSlidingWindow {
				err = slidingWindowAggFunc.Slide(ctx, rows, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
			} else {
				_, err = windowFunc.UFIDelatePartialResult(ctx, rows[start:end], p.partialResults[i])
			}
			if err != nil {
				return nil, err
			}
			err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
			if slidingWindowAggFunc == nil {
				windowFunc.ResetPartialResult(p.partialResults[i])
			}
		}
		if !initializedSlidingWindow {
			initializedSlidingWindow = true
		}
	}
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
	return rows, nil
}

func (p *rowFrameWindowProcessor) resetPartialResult() {
	p.curEventIdx = 0
}

type rangeFrameWindowProcessor struct {
	windowFuncs     []aggfuncs.AggFunc
	partialResults  []aggfuncs.PartialResult
	start           *core.FrameBound
	end             *core.FrameBound
	curEventIdx     uint64
	lastStartOffset uint64
	lastEndOffset   uint64
	orderByDefCauss []*expression.DeferredCauset
	// expectedCmpResult is used to decide if one value is included in the frame.
	expectedCmpResult int64
}

func (p *rangeFrameWindowProcessor) getStartOffset(ctx stochastikctx.Context, rows []chunk.Event) (uint64, error) {
	if p.start.UnBounded {
		return 0, nil
	}
	numEvents := uint64(len(rows))
	for ; p.lastStartOffset < numEvents; p.lastStartOffset++ {
		var res int64
		var err error
		for i := range p.orderByDefCauss {
			res, _, err = p.start.CmpFuncs[i](ctx, p.orderByDefCauss[i], p.start.CalcFuncs[i], rows[p.lastStartOffset], rows[p.curEventIdx])
			if err != nil {
				return 0, err
			}
			if res != 0 {
				break
			}
		}
		// For asc, break when the current value is greater or equal to the calculated result;
		// For desc, break when the current value is less or equal to the calculated result.
		if res != p.expectedCmpResult {
			break
		}
	}
	return p.lastStartOffset, nil
}

func (p *rangeFrameWindowProcessor) getEndOffset(ctx stochastikctx.Context, rows []chunk.Event) (uint64, error) {
	numEvents := uint64(len(rows))
	if p.end.UnBounded {
		return numEvents, nil
	}
	for ; p.lastEndOffset < numEvents; p.lastEndOffset++ {
		var res int64
		var err error
		for i := range p.orderByDefCauss {
			res, _, err = p.end.CmpFuncs[i](ctx, p.end.CalcFuncs[i], p.orderByDefCauss[i], rows[p.curEventIdx], rows[p.lastEndOffset])
			if err != nil {
				return 0, err
			}
			if res != 0 {
				break
			}
		}
		// For asc, break when the calculated result is greater than the current value.
		// For desc, break when the calculated result is less than the current value.
		if res == p.expectedCmpResult {
			break
		}
	}
	return p.lastEndOffset, nil
}

func (p *rangeFrameWindowProcessor) appendResult2Chunk(ctx stochastikctx.Context, rows []chunk.Event, chk *chunk.Chunk, remained int) ([]chunk.Event, error) {
	var (
		err                      error
		initializedSlidingWindow bool
		start                    uint64
		end                      uint64
		lastStart                uint64
		lastEnd                  uint64
		shiftStart               uint64
		shiftEnd                 uint64
	)
	slidingWindowAggFuncs := make([]aggfuncs.SlidingWindowAggFunc, len(p.windowFuncs))
	for i, windowFunc := range p.windowFuncs {
		if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
			slidingWindowAggFuncs[i] = slidingWindowAggFunc
		}
	}
	for ; remained > 0; lastStart, lastEnd = start, end {
		start, err = p.getStartOffset(ctx, rows)
		if err != nil {
			return nil, err
		}
		end, err = p.getEndOffset(ctx, rows)
		if err != nil {
			return nil, err
		}
		p.curEventIdx++
		remained--
		shiftStart = start - lastStart
		shiftEnd = end - lastEnd
		if start >= end {
			for i, windowFunc := range p.windowFuncs {
				slidingWindowAggFunc := slidingWindowAggFuncs[i]
				if slidingWindowAggFunc != nil && initializedSlidingWindow {
					err = slidingWindowAggFunc.Slide(ctx, rows, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
					if err != nil {
						return nil, err
					}
				}
				err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		for i, windowFunc := range p.windowFuncs {
			slidingWindowAggFunc := slidingWindowAggFuncs[i]
			if slidingWindowAggFunc != nil && initializedSlidingWindow {
				err = slidingWindowAggFunc.Slide(ctx, rows, lastStart, lastEnd, shiftStart, shiftEnd, p.partialResults[i])
			} else {
				_, err = windowFunc.UFIDelatePartialResult(ctx, rows[start:end], p.partialResults[i])
			}
			if err != nil {
				return nil, err
			}
			err = windowFunc.AppendFinalResult2Chunk(ctx, p.partialResults[i], chk)
			if err != nil {
				return nil, err
			}
			if slidingWindowAggFunc == nil {
				windowFunc.ResetPartialResult(p.partialResults[i])
			}
		}
		if !initializedSlidingWindow {
			initializedSlidingWindow = true
		}
	}
	for i, windowFunc := range p.windowFuncs {
		windowFunc.ResetPartialResult(p.partialResults[i])
	}
	return rows, nil
}

func (p *rangeFrameWindowProcessor) consumeGroupEvents(ctx stochastikctx.Context, rows []chunk.Event) ([]chunk.Event, error) {
	return rows, nil
}

func (p *rangeFrameWindowProcessor) resetPartialResult() {
	p.curEventIdx = 0
	p.lastStartOffset = 0
	p.lastEndOffset = 0
}
