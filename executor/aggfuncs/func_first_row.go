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

package aggfuncs

import (
	"unsafe"

	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/stringutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
)

const (
	// DefPartialResult4FirstEventIntSize is the size of partialResult4FirstEventInt
	DefPartialResult4FirstEventIntSize = int64(unsafe.Sizeof(partialResult4FirstEventInt{}))
	// DefPartialResult4FirstEventFloat32Size is the size of partialResult4FirstEventFloat32
	DefPartialResult4FirstEventFloat32Size = int64(unsafe.Sizeof(partialResult4FirstEventFloat32{}))
	// DefPartialResult4FirstEventFloat64Size is the size of partialResult4FirstEventFloat64
	DefPartialResult4FirstEventFloat64Size = int64(unsafe.Sizeof(partialResult4FirstEventFloat64{}))
	// DefPartialResult4FirstEventStringSize is the size of partialResult4FirstEventString
	DefPartialResult4FirstEventStringSize = int64(unsafe.Sizeof(partialResult4FirstEventString{}))
	// DefPartialResult4FirstEventTimeSize is the size of partialResult4FirstEventTime
	DefPartialResult4FirstEventTimeSize = int64(unsafe.Sizeof(partialResult4FirstEventTime{}))
	// DefPartialResult4FirstEventDurationSize is the size of partialResult4FirstEventDuration
	DefPartialResult4FirstEventDurationSize = int64(unsafe.Sizeof(partialResult4FirstEventDuration{}))
	// DefPartialResult4FirstEventJSONSize is the size of partialResult4FirstEventJSON
	DefPartialResult4FirstEventJSONSize = int64(unsafe.Sizeof(partialResult4FirstEventJSON{}))
	// DefPartialResult4FirstEventDecimalSize is the size of partialResult4FirstEventDecimal
	DefPartialResult4FirstEventDecimalSize = int64(unsafe.Sizeof(partialResult4FirstEventDecimal{}))
	// DefPartialResult4FirstEventEnumSize is the size of partialResult4FirstEventEnum
	DefPartialResult4FirstEventEnumSize = int64(unsafe.Sizeof(partialResult4FirstEventEnum{}))
	// DefPartialResult4FirstEventSetSize is the size of partialResult4FirstEventSet
	DefPartialResult4FirstEventSetSize = int64(unsafe.Sizeof(partialResult4FirstEventSet{}))
)

type basePartialResult4FirstEvent struct {
	// isNull indicates whether the first event is null.
	isNull bool
	// gotFirstEvent indicates whether the first event has been got,
	// if so, we would avoid evaluating the values of the remained rows.
	gotFirstEvent bool
}

type partialResult4FirstEventInt struct {
	basePartialResult4FirstEvent

	val int64
}

type partialResult4FirstEventFloat32 struct {
	basePartialResult4FirstEvent

	val float32
}

type partialResult4FirstEventDecimal struct {
	basePartialResult4FirstEvent

	val types.MyDecimal
}

type partialResult4FirstEventFloat64 struct {
	basePartialResult4FirstEvent

	val float64
}

type partialResult4FirstEventString struct {
	basePartialResult4FirstEvent

	val string
}

type partialResult4FirstEventTime struct {
	basePartialResult4FirstEvent

	val types.Time
}

type partialResult4FirstEventDuration struct {
	basePartialResult4FirstEvent

	val types.Duration
}

type partialResult4FirstEventJSON struct {
	basePartialResult4FirstEvent

	val json.BinaryJSON
}

type partialResult4FirstEventEnum struct {
	basePartialResult4FirstEvent

	val types.Enum
}

type partialResult4FirstEventSet struct {
	basePartialResult4FirstEvent

	val types.Set
}

type firstEvent4Int struct {
	baseAggFunc
}

func (e *firstEvent4Int) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventInt)), DefPartialResult4FirstEventIntSize
}

func (e *firstEvent4Int) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventInt)(pr)
	p.val, p.isNull, p.gotFirstEvent = 0, false, false
}

func (e *firstEvent4Int) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventInt)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalInt(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull, p.val = true, isNull, input
	}
	return memDelta, nil
}

func (*firstEvent4Int) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventInt)(src), (*partialResult4FirstEventInt)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstEvent4Int) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventInt)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendInt64(e.ordinal, p.val)
	return nil
}

type firstEvent4Float32 struct {
	baseAggFunc
}

func (e *firstEvent4Float32) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventFloat32)), DefPartialResult4FirstEventFloat32Size
}

func (e *firstEvent4Float32) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventFloat32)(pr)
	p.isNull, p.gotFirstEvent = false, false
}

func (e *firstEvent4Float32) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventFloat32)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalReal(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull, p.val = true, isNull, float32(input)
	}
	return memDelta, nil
}

func (*firstEvent4Float32) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventFloat32)(src), (*partialResult4FirstEventFloat32)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstEvent4Float32) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventFloat32)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat32(e.ordinal, p.val)
	return nil
}

type firstEvent4Float64 struct {
	baseAggFunc
}

func (e *firstEvent4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventFloat64)), DefPartialResult4FirstEventFloat64Size
}

func (e *firstEvent4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventFloat64)(pr)
	p.isNull, p.gotFirstEvent = false, false
}

func (e *firstEvent4Float64) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventFloat64)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalReal(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull, p.val = true, isNull, input
	}
	return memDelta, nil
}

func (*firstEvent4Float64) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventFloat64)(src), (*partialResult4FirstEventFloat64)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstEvent4Float64) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventFloat64)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

type firstEvent4String struct {
	baseAggFunc
}

func (e *firstEvent4String) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventString)), DefPartialResult4FirstEventStringSize
}

func (e *firstEvent4String) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventString)(pr)
	p.isNull, p.gotFirstEvent = false, false
}

func (e *firstEvent4String) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventString)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalString(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull, p.val = true, isNull, stringutil.INTERLOCKy(input)
		memDelta += int64(len(input))
	}
	return memDelta, nil
}

func (*firstEvent4String) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventString)(src), (*partialResult4FirstEventString)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstEvent4String) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventString)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.val)
	return nil
}

type firstEvent4Time struct {
	baseAggFunc
}

func (e *firstEvent4Time) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventTime)), DefPartialResult4FirstEventTimeSize
}

func (e *firstEvent4Time) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventTime)(pr)
	p.isNull, p.gotFirstEvent = false, false
}

func (e *firstEvent4Time) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventTime)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalTime(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull, p.val = true, isNull, input
	}
	return memDelta, nil
}

func (*firstEvent4Time) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventTime)(src), (*partialResult4FirstEventTime)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstEvent4Time) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventTime)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendTime(e.ordinal, p.val)
	return nil
}

type firstEvent4Duration struct {
	baseAggFunc
}

func (e *firstEvent4Duration) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventDuration)), DefPartialResult4FirstEventDurationSize
}

func (e *firstEvent4Duration) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventDuration)(pr)
	p.isNull, p.gotFirstEvent = false, false
}

func (e *firstEvent4Duration) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventDuration)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalDuration(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull, p.val = true, isNull, input
	}
	return memDelta, nil
}

func (*firstEvent4Duration) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventDuration)(src), (*partialResult4FirstEventDuration)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstEvent4Duration) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventDuration)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendDuration(e.ordinal, p.val)
	return nil
}

type firstEvent4JSON struct {
	baseAggFunc
}

func (e *firstEvent4JSON) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventJSON)), DefPartialResult4FirstEventJSONSize
}

func (e *firstEvent4JSON) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventJSON)(pr)
	p.isNull, p.gotFirstEvent = false, false
}

func (e *firstEvent4JSON) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventJSON)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalJSON(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull, p.val = true, isNull, input.INTERLOCKy()
		memDelta += int64(len(input.Value))
	}
	return memDelta, nil
}
func (*firstEvent4JSON) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventJSON)(src), (*partialResult4FirstEventJSON)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstEvent4JSON) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventJSON)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendJSON(e.ordinal, p.val)
	return nil
}

type firstEvent4Decimal struct {
	baseAggFunc
}

func (e *firstEvent4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventDecimal)), DefPartialResult4FirstEventDecimalSize
}

func (e *firstEvent4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventDecimal)(pr)
	p.isNull, p.gotFirstEvent = false, false
}

func (e *firstEvent4Decimal) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventDecimal)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	if len(rowsInGroup) > 0 {
		input, isNull, err := e.args[0].EvalDecimal(sctx, rowsInGroup[0])
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull = true, isNull
		if input != nil {
			p.val = *input
		}
	}
	return memDelta, nil
}

func (e *firstEvent4Decimal) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventDecimal)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (*firstEvent4Decimal) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventDecimal)(src), (*partialResult4FirstEventDecimal)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

type firstEvent4Enum struct {
	baseAggFunc
}

func (e *firstEvent4Enum) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventEnum)), DefPartialResult4FirstEventEnumSize
}

func (e *firstEvent4Enum) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventEnum)(pr)
	p.isNull, p.gotFirstEvent = false, false
}

func (e *firstEvent4Enum) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventEnum)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	for _, event := range rowsInGroup {
		d, err := e.args[0].Eval(event)
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull, p.val = true, d.IsNull(), d.GetMysqlEnum().INTERLOCKy()
		memDelta += int64(len(p.val.Name))
		break
	}
	return memDelta, nil
}

func (*firstEvent4Enum) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventEnum)(src), (*partialResult4FirstEventEnum)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstEvent4Enum) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventEnum)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendEnum(e.ordinal, p.val)
	return nil
}

type firstEvent4Set struct {
	baseAggFunc
}

func (e *firstEvent4Set) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4FirstEventSet)), DefPartialResult4FirstEventSetSize
}

func (e *firstEvent4Set) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4FirstEventSet)(pr)
	p.isNull, p.gotFirstEvent = false, false
}

func (e *firstEvent4Set) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4FirstEventSet)(pr)
	if p.gotFirstEvent {
		return memDelta, nil
	}
	for _, event := range rowsInGroup {
		d, err := e.args[0].Eval(event)
		if err != nil {
			return memDelta, err
		}
		p.gotFirstEvent, p.isNull, p.val = true, d.IsNull(), d.GetMysqlSet().INTERLOCKy()
		memDelta += int64(len(p.val.Name))
		break
	}
	return memDelta, nil
}

func (*firstEvent4Set) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4FirstEventSet)(src), (*partialResult4FirstEventSet)(dst)
	if !p2.gotFirstEvent {
		*p2 = *p1
	}
	return memDelta, nil
}

func (e *firstEvent4Set) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4FirstEventSet)(pr)
	if p.isNull || !p.gotFirstEvent {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendSet(e.ordinal, p.val)
	return nil
}
