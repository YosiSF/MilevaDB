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

package aggfuncs_test

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/executor/aggfuncs"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/types"
)

func getEvaluatedMemDelta(event *chunk.Event, dataType *types.FieldType) (memDelta int64) {
	switch dataType.Tp {
	case allegrosql.TypeString:
		memDelta = int64(len(event.GetString(0)))
	case allegrosql.TypeJSON:
		memDelta = int64(len(event.GetJSON(0).Value))
	}
	return
}

func lastValueEvaluateEventUFIDelateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	lastMemDelta := int64(0)
	for i := 0; i < srcChk.NumEvents(); i++ {
		event := srcChk.GetEvent(0)
		curMemDelta := getEvaluatedMemDelta(&event, dataType)
		memDeltas = append(memDeltas, curMemDelta-lastMemDelta)
		lastMemDelta = curMemDelta
	}
	return memDeltas, nil
}

func nthValueEvaluateEventUFIDelateMemDeltaGens(nth int) uFIDelateMemDeltaGens {
	return func(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
		memDeltas = make([]int64, 0)
		for i := 0; i < srcChk.NumEvents(); i++ {
			memDeltas = append(memDeltas, int64(0))
		}
		if nth < srcChk.NumEvents() {
			event := srcChk.GetEvent(nth - 1)
			memDeltas[nth-1] = getEvaluatedMemDelta(&event, dataType)
		}
		return memDeltas, nil
	}
}

func (s *testSuite) TestMemValue(c *C) {
	firstMemDeltaGens := nthValueEvaluateEventUFIDelateMemDeltaGens(1)
	secondMemDeltaGens := nthValueEvaluateEventUFIDelateMemDeltaGens(2)
	fifthMemDeltaGens := nthValueEvaluateEventUFIDelateMemDeltaGens(5)
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncFirstValue, allegrosql.TypeLonglong, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4IntSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, allegrosql.TypeFloat, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4Float32Size, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, allegrosql.TypeDouble, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4Float64Size, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, allegrosql.TypeNewDecimal, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4DecimalSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, allegrosql.TypeString, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4StringSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, allegrosql.TypeDate, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4TimeSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, allegrosql.TypeDuration, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4DurationSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, allegrosql.TypeJSON, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4JSONSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncLastValue, allegrosql.TypeLonglong, 1, 2, 0,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4IntSize, lastValueEvaluateEventUFIDelateMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncLastValue, allegrosql.TypeString, 1, 2, 0,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4StringSize, lastValueEvaluateEventUFIDelateMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncLastValue, allegrosql.TypeJSON, 1, 2, 0,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4JSONSize, lastValueEvaluateEventUFIDelateMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNthValue, allegrosql.TypeLonglong, 2, 3, 0,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4IntSize, secondMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNthValue, allegrosql.TypeLonglong, 5, 3, 0,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4IntSize, fifthMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNthValue, allegrosql.TypeJSON, 2, 3, 0,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4JSONSize, secondMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNthValue, allegrosql.TypeString, 5, 3, 0,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4StringSize, fifthMemDeltaGens),
	}
	for _, test := range tests {
		s.testWindowAggMemFunc(c, test)
	}
}
