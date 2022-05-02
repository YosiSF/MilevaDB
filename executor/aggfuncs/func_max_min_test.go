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
	"fmt"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/executor/aggfuncs"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
)

func maxMinUFIDelateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType, isMax bool) (memDeltas []int64, err error) {
	memDeltas = make([]int64, srcChk.NumEvents())
	var (
		preStringVal string
		preJSONVal   string
		preEnumVal   types.Enum
		preSetVal    types.Set
	)

	for i := 0; i < srcChk.NumEvents(); i++ {
		event := srcChk.GetEvent(i)
		if event.IsNull(0) {
			continue
		}
		switch dataType.Tp {
		case allegrosql.TypeString:
			curVal := event.GetString(0)
			if i == 0 {
				memDeltas[i] = int64(len(curVal))
				preStringVal = curVal
			} else if isMax && curVal > preStringVal || !isMax && curVal < preStringVal {
				memDeltas[i] = int64(len(curVal)) - int64(len(preStringVal))
				preStringVal = curVal
			}
		case allegrosql.TypeJSON:
			curVal := event.GetJSON(0)
			curStringVal := string(curVal.Value)
			if i == 0 {
				memDeltas[i] = int64(len(curStringVal))
				preJSONVal = curStringVal
			} else if isMax && curStringVal > preJSONVal || !isMax && curStringVal < preJSONVal {
				memDeltas[i] = int64(len(curStringVal)) - int64(len(preJSONVal))
				preJSONVal = curStringVal
			}
		case allegrosql.TypeEnum:
			curVal := event.GetEnum(0)
			if i == 0 {
				memDeltas[i] = int64(len(curVal.Name))
				preEnumVal = curVal
			} else if isMax && curVal.Value > preEnumVal.Value || !isMax && curVal.Value < preEnumVal.Value {
				memDeltas[i] = int64(len(curVal.Name)) - int64(len(preEnumVal.Name))
				preEnumVal = curVal
			}
		case allegrosql.TypeSet:
			curVal := event.GetSet(0)
			if i == 0 {
				memDeltas[i] = int64(len(curVal.Name))
				preSetVal = curVal
			} else if isMax && curVal.Value > preSetVal.Value || !isMax && curVal.Value < preSetVal.Value {
				memDeltas[i] = int64(len(curVal.Name)) - int64(len(preSetVal.Name))
				preSetVal = curVal
			}
		}
	}
	return memDeltas, nil
}

func maxUFIDelateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	return maxMinUFIDelateMemDeltaGens(srcChk, dataType, true)
}

func minUFIDelateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	return maxMinUFIDelateMemDeltaGens(srcChk, dataType, false)
}

func (s *testSuite) TestMergePartialResult4MaxMin(c *C) {
	elems := []string{"a", "b", "c", "d", "e"}
	enumA, _ := types.ParseEnumName(elems, "a", allegrosql.DefaultDefCauslationName)
	enumC, _ := types.ParseEnumName(elems, "c", allegrosql.DefaultDefCauslationName)
	enumE, _ := types.ParseEnumName(elems, "e", allegrosql.DefaultDefCauslationName)

	setA, _ := types.ParseSetName(elems, "a", allegrosql.DefaultDefCauslationName)    // setA.Value == 1
	setAB, _ := types.ParseSetName(elems, "a,b", allegrosql.DefaultDefCauslationName) // setAB.Value == 3
	setAC, _ := types.ParseSetName(elems, "a,c", allegrosql.DefaultDefCauslationName) // setAC.Value == 5

	unsignedType := types.NewFieldType(allegrosql.TypeLonglong)
	unsignedType.Flag |= allegrosql.UnsignedFlag
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeLonglong, 5, 4, 4, 4),
		builPosetDaggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, 4, 4, 4),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeFloat, 5, 4.0, 4.0, 4.0),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeDouble, 5, 4.0, 4.0, 4.0),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeNewDecimal, 5, types.NewDecFromInt(4), types.NewDecFromInt(4), types.NewDecFromInt(4)),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeString, 5, "4", "4", "4"),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeDate, 5, types.TimeFromDays(369), types.TimeFromDays(369), types.TimeFromDays(369)),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeDuration, 5, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeJSON, 5, json.CreateBinary(int64(4)), json.CreateBinary(int64(4)), json.CreateBinary(int64(4))),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeEnum, 5, enumE, enumE, enumE),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeSet, 5, setAC, setAC, setAC),

		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeLonglong, 5, 0, 2, 0),
		builPosetDaggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, 0, 2, 0),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeFloat, 5, 0.0, 2.0, 0.0),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeDouble, 5, 0.0, 2.0, 0.0),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeNewDecimal, 5, types.NewDecFromInt(0), types.NewDecFromInt(2), types.NewDecFromInt(0)),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeString, 5, "0", "2", "0"),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeDate, 5, types.TimeFromDays(365), types.TimeFromDays(367), types.TimeFromDays(365)),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeDuration, 5, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(2)}, types.Duration{Duration: time.Duration(0)}),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeJSON, 5, json.CreateBinary(int64(0)), json.CreateBinary(int64(2)), json.CreateBinary(int64(0))),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeEnum, 5, enumA, enumC, enumA),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeSet, 5, setA, setAB, setA),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestMaxMin(c *C) {
	unsignedType := types.NewFieldType(allegrosql.TypeLonglong)
	unsignedType.Flag |= allegrosql.UnsignedFlag
	tests := []aggTest{
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeLonglong, 5, nil, 4),
		builPosetDaggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, nil, 4),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeFloat, 5, nil, 4.0),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeDouble, 5, nil, 4.0),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeNewDecimal, 5, nil, types.NewDecFromInt(4)),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeString, 5, nil, "4", "4"),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeDate, 5, nil, types.TimeFromDays(369)),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeDuration, 5, nil, types.Duration{Duration: time.Duration(4)}),
		builPosetDaggTester(ast.AggFuncMax, allegrosql.TypeJSON, 5, nil, json.CreateBinary(int64(4))),

		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeLonglong, 5, nil, 0),
		builPosetDaggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, nil, 0),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeFloat, 5, nil, 0.0),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeDouble, 5, nil, 0.0),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeNewDecimal, 5, nil, types.NewDecFromInt(0)),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeString, 5, nil, "0"),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeDate, 5, nil, types.TimeFromDays(365)),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeDuration, 5, nil, types.Duration{Duration: time.Duration(0)}),
		builPosetDaggTester(ast.AggFuncMin, allegrosql.TypeJSON, 5, nil, json.CreateBinary(int64(0))),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}

func (s *testSuite) TestMemMaxMin(c *C) {
	tests := []aggMemTest{
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinIntSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinUintSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4MaxMinDecimalSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeFloat, 5,
			aggfuncs.DefPartialResult4MaxMinFloat32Size, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4MaxMinFloat64Size, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeDate, 5,
			aggfuncs.DefPartialResult4TimeSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeDuration, 5,
			aggfuncs.DefPartialResult4MaxMinDurationSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeString, 99,
			aggfuncs.DefPartialResult4MaxMinStringSize, maxUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeJSON, 99,
			aggfuncs.DefPartialResult4MaxMinJSONSize, maxUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeEnum, 99,
			aggfuncs.DefPartialResult4MaxMinEnumSize, maxUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMax, allegrosql.TypeSet, 99,
			aggfuncs.DefPartialResult4MaxMinSetSize, maxUFIDelateMemDeltaGens, false),

		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinIntSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinUintSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4MaxMinDecimalSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeFloat, 5,
			aggfuncs.DefPartialResult4MaxMinFloat32Size, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeDouble, 5,
			aggfuncs.DefPartialResult4MaxMinFloat64Size, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeDate, 5,
			aggfuncs.DefPartialResult4TimeSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeDuration, 5,
			aggfuncs.DefPartialResult4MaxMinDurationSize, defaultUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeString, 99,
			aggfuncs.DefPartialResult4MaxMinStringSize, minUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeJSON, 99,
			aggfuncs.DefPartialResult4MaxMinJSONSize, minUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeEnum, 99,
			aggfuncs.DefPartialResult4MaxMinEnumSize, minUFIDelateMemDeltaGens, false),
		builPosetDaggMemTester(ast.AggFuncMin, allegrosql.TypeSet, 99,
			aggfuncs.DefPartialResult4MaxMinSetSize, minUFIDelateMemDeltaGens, false),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}

type maxSlidingWindowTestCase struct {
	rowType       string
	insertValue   string
	expect        []string
	orderByExpect []string
	orderBy       bool
	frameType     ast.FrameType
}

func testMaxSlidingWindow(tk *testkit.TestKit, tc maxSlidingWindowTestCase) {
	tk.MustExec(fmt.Sprintf("CREATE TABLE t (a %s);", tc.rowType))
	tk.MustExec(fmt.Sprintf("insert into t values %s;", tc.insertValue))
	var orderBy string
	if tc.orderBy {
		orderBy = "ORDER BY a"
	}
	var result *testkit.Result
	switch tc.frameType {
	case ast.Events:
		result = tk.MustQuery(fmt.Sprintf("SELECT max(a) OVER (%s ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;", orderBy))
	case ast.Ranges:
		result = tk.MustQuery(fmt.Sprintf("SELECT max(a) OVER (%s RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;", orderBy))
	default:
		result = tk.MustQuery(fmt.Sprintf("SELECT max(a) OVER (%s) FROM t;", orderBy))
		if tc.orderBy {
			result.Check(testkit.Events(tc.orderByExpect...))
			return
		}
	}
	result.Check(testkit.Events(tc.expect...))
}

func (s *testSuite) TestMaxSlidingWindow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	testCases := []maxSlidingWindowTestCase{
		{
			rowType:       "bigint",
			insertValue:   "(1), (3), (2)",
			expect:        []string{"3", "3", "3"},
			orderByExpect: []string{"1", "2", "3"},
		},
		{
			rowType:       "float",
			insertValue:   "(1.1), (3.3), (2.2)",
			expect:        []string{"3.3", "3.3", "3.3"},
			orderByExpect: []string{"1.1", "2.2", "3.3"},
		},
		{
			rowType:       "double",
			insertValue:   "(1.1), (3.3), (2.2)",
			expect:        []string{"3.3", "3.3", "3.3"},
			orderByExpect: []string{"1.1", "2.2", "3.3"},
		},
		{
			rowType:       "decimal(5, 2)",
			insertValue:   "(1.1), (3.3), (2.2)",
			expect:        []string{"3.30", "3.30", "3.30"},
			orderByExpect: []string{"1.10", "2.20", "3.30"},
		},
		{
			rowType:       "text",
			insertValue:   "('1.1'), ('3.3'), ('2.2')",
			expect:        []string{"3.3", "3.3", "3.3"},
			orderByExpect: []string{"1.1", "2.2", "3.3"},
		},
		{
			rowType:       "time",
			insertValue:   "('00:00:00'), ('03:00:00'), ('02:00:00')",
			expect:        []string{"03:00:00", "03:00:00", "03:00:00"},
			orderByExpect: []string{"00:00:00", "02:00:00", "03:00:00"},
		},
		{
			rowType:       "date",
			insertValue:   "('2020-09-08'), ('2022-09-10'), ('2020-09-10')",
			expect:        []string{"2022-09-10", "2022-09-10", "2022-09-10"},
			orderByExpect: []string{"2020-09-08", "2020-09-10", "2022-09-10"},
		},
		{
			rowType:       "datetime",
			insertValue:   "('2020-09-08 02:00:00'), ('2022-09-10 00:00:00'), ('2020-09-10 00:00:00')",
			expect:        []string{"2022-09-10 00:00:00", "2022-09-10 00:00:00", "2022-09-10 00:00:00"},
			orderByExpect: []string{"2020-09-08 02:00:00", "2020-09-10 00:00:00", "2022-09-10 00:00:00"},
		},
	}

	orderBy := []bool{false, true}
	frameType := []ast.FrameType{ast.Events, ast.Ranges, -1}
	for _, o := range orderBy {
		for _, f := range frameType {
			for _, tc := range testCases {
				tc.frameType = f
				tc.orderBy = o
				tk.MustExec("drop block if exists t;")
				testMaxSlidingWindow(tk, tc)
			}
		}
	}
}
