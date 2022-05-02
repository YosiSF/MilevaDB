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

package aggfuncs_test

import (
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"

	"github.com/whtcorpsinc/MilevaDB-Prod/executor/aggfuncs"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
)

func (s *testSuite) TestMergePartialResult4FirstEvent(c *C) {
	elems := []string{"a", "b", "c", "d", "e"}
	enumA, _ := types.ParseEnumName(elems, "a", allegrosql.DefaultDefCauslationName)
	enumC, _ := types.ParseEnumName(elems, "c", allegrosql.DefaultDefCauslationName)

	setA, _ := types.ParseSetName(elems, "a", allegrosql.DefaultDefCauslationName)
	setAB, _ := types.ParseSetName(elems, "a,b", allegrosql.DefaultDefCauslationName)

	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeLonglong, 5, 0, 2, 0),
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeFloat, 5, 0.0, 2.0, 0.0),
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeDouble, 5, 0.0, 2.0, 0.0),
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeNewDecimal, 5, types.NewDecFromInt(0), types.NewDecFromInt(2), types.NewDecFromInt(0)),
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeString, 5, "0", "2", "0"),
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeDate, 5, types.TimeFromDays(365), types.TimeFromDays(367), types.TimeFromDays(365)),
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeDuration, 5, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(2)}, types.Duration{Duration: time.Duration(0)}),
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeJSON, 5, json.CreateBinary(int64(0)), json.CreateBinary(int64(2)), json.CreateBinary(int64(0))),
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeEnum, 5, enumA, enumC, enumA),
		builPosetDaggTester(ast.AggFuncFirstEvent, allegrosql.TypeSet, 5, setA, setAB, setA),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestMemFirstEvent(c *C) {
	tests := []aggMemTest{
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4FirstEventIntSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeFloat, 5,
			aggfuncs.DefPartialResult4FirstEventFloat32Size, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4FirstEventFloat64Size, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4FirstEventDecimalSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeString, 5,
			aggfuncs.DefPartialResult4FirstEventStringSize, firstEventUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeDate, 5,
			aggfuncs.DefPartialResult4FirstEventTimeSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeDuration, 5,
			aggfuncs.DefPartialResult4FirstEventDurationSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeJSON, 5,
			aggfuncs.DefPartialResult4FirstEventJSONSize, firstEventUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeEnum, 5,
			aggfuncs.DefPartialResult4FirstEventEnumSize, firstEventUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncFirstEvent, allegrosql.TypeSet, 5,
			aggfuncs.DefPartialResult4FirstEventSetSize, firstEventUFIDelateMemDeltaGens, false),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}

func firstEventUFIDelateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	for i := 0; i < srcChk.NumEvents(); i++ {
		event := srcChk.GetEvent(i)
		if i > 0 {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		switch dataType.Tp {
		case allegrosql.TypeString:
			val := event.GetString(0)
			memDeltas = append(memDeltas, int64(len(val)))
		case allegrosql.TypeJSON:
			jsonVal := event.GetJSON(0)
			memDeltas = append(memDeltas, int64(len(string(jsonVal.Value))))
		case allegrosql.TypeEnum:
			enum := event.GetEnum(0)
			memDeltas = append(memDeltas, int64(len(enum.Name)))
		case allegrosql.TypeSet:
			typeSet := event.GetSet(0)
			memDeltas = append(memDeltas, int64(len(typeSet.Name)))
		}
	}
	return memDeltas, nil
}
