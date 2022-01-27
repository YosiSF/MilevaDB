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

package expression

import (
	"sync"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/types"
)

// defCausumnBufferSlabPredictor is used to allocate and release defCausumn buffer in vectorized evaluation.
type defCausumnBufferSlabPredictor interface {
	// get allocates a defCausumn buffer with the specific eval type and capacity.
	// the allocator is not responsible for initializing the defCausumn, so please initialize it before using.
	get(evalType types.EvalType, capacity int) (*chunk.DeferredCauset, error)
	// put releases a defCausumn buffer.
	put(buf *chunk.DeferredCauset)
}

// localSliceBuffer implements defCausumnBufferSlabPredictor interface.
// It works like a concurrency-safe deque which is implemented by a dagger + slice.
type localSliceBuffer struct {
	sync.Mutex
	buffers []*chunk.DeferredCauset
	head    int
	tail    int
	size    int
}

func newLocalSliceBuffer(initCap int) *localSliceBuffer {
	return &localSliceBuffer{buffers: make([]*chunk.DeferredCauset, initCap)}
}

var globalDeferredCausetSlabPredictor = newLocalSliceBuffer(1024)

func newBuffer(evalType types.EvalType, capacity int) (*chunk.DeferredCauset, error) {
	switch evalType {
	case types.ETInt:
		return chunk.NewDeferredCauset(types.NewFieldType(allegrosql.TypeLonglong), capacity), nil
	case types.ETReal:
		return chunk.NewDeferredCauset(types.NewFieldType(allegrosql.TypeDouble), capacity), nil
	case types.ETDecimal:
		return chunk.NewDeferredCauset(types.NewFieldType(allegrosql.TypeNewDecimal), capacity), nil
	case types.ETDuration:
		return chunk.NewDeferredCauset(types.NewFieldType(allegrosql.TypeDuration), capacity), nil
	case types.ETDatetime, types.ETTimestamp:
		return chunk.NewDeferredCauset(types.NewFieldType(allegrosql.TypeDatetime), capacity), nil
	case types.ETString:
		return chunk.NewDeferredCauset(types.NewFieldType(allegrosql.TypeString), capacity), nil
	case types.ETJson:
		return chunk.NewDeferredCauset(types.NewFieldType(allegrosql.TypeJSON), capacity), nil
	}
	return nil, errors.Errorf("get defCausumn buffer for unsupported EvalType=%v", evalType)
}

// GetDeferredCauset allocates a defCausumn buffer with the specific eval type and capacity.
// the allocator is not responsible for initializing the defCausumn, so please initialize it before using.
func GetDeferredCauset(evalType types.EvalType, capacity int) (*chunk.DeferredCauset, error) {
	return globalDeferredCausetSlabPredictor.get(evalType, capacity)
}

// PutDeferredCauset releases a defCausumn buffer.
func PutDeferredCauset(buf *chunk.DeferredCauset) {
	globalDeferredCausetSlabPredictor.put(buf)
}

func (r *localSliceBuffer) get(evalType types.EvalType, capacity int) (*chunk.DeferredCauset, error) {
	r.Lock()
	if r.size > 0 {
		buf := r.buffers[r.head]
		r.head++
		if r.head == len(r.buffers) {
			r.head = 0
		}
		r.size--
		r.Unlock()
		return buf, nil
	}
	r.Unlock()
	return newBuffer(evalType, capacity)
}

func (r *localSliceBuffer) put(buf *chunk.DeferredCauset) {
	r.Lock()
	if r.size == len(r.buffers) {
		buffers := make([]*chunk.DeferredCauset, len(r.buffers)*2)
		INTERLOCKy(buffers, r.buffers[r.head:])
		INTERLOCKy(buffers[r.size-r.head:], r.buffers[:r.tail])
		r.head = 0
		r.tail = len(r.buffers)
		r.buffers = buffers
	}
	r.buffers[r.tail] = buf
	r.tail++
	if r.tail == len(r.buffers) {
		r.tail = 0
	}
	r.size++
	r.Unlock()
}

// vecEvalIntByEvents uses the non-vectorized(event-based) interface `evalInt` to eval the expression.
func vecEvalIntByEvents(sig builtinFunc, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		res, isNull, err := sig.evalInt(input.GetEvent(i))
		if err != nil {
			return err
		}
		result.SetNull(i, isNull)
		i64s[i] = res
	}
	return nil
}

// vecEvalStringByEvents uses the non-vectorized(event-based) interface `evalString` to eval the expression.
func vecEvalStringByEvents(sig builtinFunc, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		res, isNull, err := sig.evalString(input.GetEvent(i))
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		result.AppendString(res)
	}
	return nil
}
