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

package aggregation

import (
	"math"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
)

var _ = Suite(&testAggFuncSuit{})

type testAggFuncSuit struct {
	ctx       stochastikctx.Context
	rows      []chunk.Event
	nullEvent chunk.Event
}

func generateEventData() []chunk.Event {
	rows := make([]chunk.Event, 0, 5050)
	for i := 1; i <= 100; i++ {
		for j := 0; j < i; j++ {
			rows = append(rows, chunk.MutEventFromCausets(types.MakeCausets(i)).ToEvent())
		}
	}
	return rows
}

func (s *testAggFuncSuit) SetUpSuite(c *C) {
	s.ctx = mock.NewContext()
	s.ctx.GetStochaseinstein_dbars().GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	s.rows = generateEventData()
	s.nullEvent = chunk.MutEventFromCausets([]types.Causet{{}}).ToEvent()
}

func (s *testAggFuncSuit) TestAvg(c *C) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{defCaus}, false)
	c.Assert(err, IsNil)
	avgFunc := desc.GetAggFunc(ctx)
	evalCtx := avgFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	result := avgFunc.GetResult(evalCtx)
	c.Assert(result.IsNull(), IsTrue)

	for _, event := range s.rows {
		err := avgFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
		c.Assert(err, IsNil)
	}
	result = avgFunc.GetResult(evalCtx)
	needed := types.NewDecFromStringForTest("67.000000000000000000000000000000")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
	err = avgFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, s.nullEvent)
	c.Assert(err, IsNil)
	result = avgFunc.GetResult(evalCtx)
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)

	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{defCaus}, true)
	c.Assert(err, IsNil)
	distinctAvgFunc := desc.GetAggFunc(ctx)
	evalCtx = distinctAvgFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)
	for _, event := range s.rows {
		err := distinctAvgFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
		c.Assert(err, IsNil)
	}
	result = distinctAvgFunc.GetResult(evalCtx)
	needed = types.NewDecFromStringForTest("50.500000000000000000000000000000")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
	partialResult := distinctAvgFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(100))
	needed = types.NewDecFromStringForTest("5050")
	c.Assert(partialResult[1].GetMysqlDecimal().Compare(needed) == 0, IsTrue, Commentf("%v, %v ", result.GetMysqlDecimal(), needed))
}

func (s *testAggFuncSuit) TestAvgFinalMode(c *C) {
	rows := make([][]types.Causet, 0, 100)
	for i := 1; i <= 100; i++ {
		rows = append(rows, types.MakeCausets(i, types.NewDecFromInt(int64(i*i))))
	}
	ctx := mock.NewContext()
	cntDefCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	sumDefCaus := &expression.DeferredCauset{
		Index:   1,
		RetType: types.NewFieldType(allegrosql.TypeNewDecimal),
	}
	aggFunc, err := NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{cntDefCaus, sumDefCaus}, false)
	c.Assert(err, IsNil)
	aggFunc.Mode = FinalMode
	avgFunc := aggFunc.GetAggFunc(ctx)
	evalCtx := avgFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	for _, event := range rows {
		err := avgFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, chunk.MutEventFromCausets(event).ToEvent())
		c.Assert(err, IsNil)
	}
	result := avgFunc.GetResult(evalCtx)
	needed := types.NewDecFromStringForTest("67.000000000000000000000000000000")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
}

func (s *testAggFuncSuit) TestSum(c *C) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncSum, []expression.Expression{defCaus}, false)
	c.Assert(err, IsNil)
	sumFunc := desc.GetAggFunc(ctx)
	evalCtx := sumFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	result := sumFunc.GetResult(evalCtx)
	c.Assert(result.IsNull(), IsTrue)

	for _, event := range s.rows {
		err := sumFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
		c.Assert(err, IsNil)
	}
	result = sumFunc.GetResult(evalCtx)
	needed := types.NewDecFromStringForTest("338350")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
	err = sumFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, s.nullEvent)
	c.Assert(err, IsNil)
	result = sumFunc.GetResult(evalCtx)
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
	partialResult := sumFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetMysqlDecimal().Compare(needed) == 0, IsTrue)

	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncSum, []expression.Expression{defCaus}, true)
	c.Assert(err, IsNil)
	distinctSumFunc := desc.GetAggFunc(ctx)
	evalCtx = distinctSumFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)
	for _, event := range s.rows {
		err := distinctSumFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
		c.Assert(err, IsNil)
	}
	result = distinctSumFunc.GetResult(evalCtx)
	needed = types.NewDecFromStringForTest("5050")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
}

func (s *testAggFuncSuit) TestBitAnd(c *C) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncBitAnd, []expression.Expression{defCaus}, false)
	c.Assert(err, IsNil)
	bitAndFunc := desc.GetAggFunc(ctx)
	evalCtx := bitAndFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	result := bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(math.MaxUint64))

	event := chunk.MutEventFromCausets(types.MakeCausets(1)).ToEvent()
	err = bitAndFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = bitAndFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, s.nullEvent)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	event = chunk.MutEventFromCausets(types.MakeCausets(1)).ToEvent()
	err = bitAndFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	event = chunk.MutEventFromCausets(types.MakeCausets(3)).ToEvent()
	err = bitAndFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	event = chunk.MutEventFromCausets(types.MakeCausets(2)).ToEvent()
	err = bitAndFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))
	partialResult := bitAndFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(0))

	// test bit_and( decimal )
	defCaus.RetType = types.NewFieldType(allegrosql.TypeNewDecimal)
	bitAndFunc.ResetContext(s.ctx.GetStochaseinstein_dbars().StmtCtx, evalCtx)

	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(math.MaxUint64))

	var dec types.MyDecimal
	err = dec.FromString([]byte("1.234"))
	c.Assert(err, IsNil)
	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitAndFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = dec.FromString([]byte("3.012"))
	c.Assert(err, IsNil)
	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitAndFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = dec.FromString([]byte("2.12345678"))
	c.Assert(err, IsNil)
	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitAndFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))
}

func (s *testAggFuncSuit) TestBitOr(c *C) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncBitOr, []expression.Expression{defCaus}, false)
	c.Assert(err, IsNil)
	bitOrFunc := desc.GetAggFunc(ctx)
	evalCtx := bitOrFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	result := bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	event := chunk.MutEventFromCausets(types.MakeCausets(1)).ToEvent()
	err = bitOrFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = bitOrFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, s.nullEvent)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	event = chunk.MutEventFromCausets(types.MakeCausets(1)).ToEvent()
	err = bitOrFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	event = chunk.MutEventFromCausets(types.MakeCausets(3)).ToEvent()
	err = bitOrFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(3))

	event = chunk.MutEventFromCausets(types.MakeCausets(2)).ToEvent()
	err = bitOrFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(3))
	partialResult := bitOrFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(3))

	// test bit_or( decimal )
	defCaus.RetType = types.NewFieldType(allegrosql.TypeNewDecimal)
	bitOrFunc.ResetContext(s.ctx.GetStochaseinstein_dbars().StmtCtx, evalCtx)

	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	var dec types.MyDecimal
	err = dec.FromString([]byte("12.234"))
	c.Assert(err, IsNil)
	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitOrFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(12))

	err = dec.FromString([]byte("1.012"))
	c.Assert(err, IsNil)
	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitOrFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(13))
	err = dec.FromString([]byte("15.12345678"))
	c.Assert(err, IsNil)

	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitOrFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(15))

	err = dec.FromString([]byte("16.00"))
	c.Assert(err, IsNil)
	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitOrFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(31))
}

func (s *testAggFuncSuit) TestBitXor(c *C) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncBitXor, []expression.Expression{defCaus}, false)
	c.Assert(err, IsNil)
	bitXorFunc := desc.GetAggFunc(ctx)
	evalCtx := bitXorFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	result := bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	event := chunk.MutEventFromCausets(types.MakeCausets(1)).ToEvent()
	err = bitXorFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = bitXorFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, s.nullEvent)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	event = chunk.MutEventFromCausets(types.MakeCausets(1)).ToEvent()
	err = bitXorFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	event = chunk.MutEventFromCausets(types.MakeCausets(3)).ToEvent()
	err = bitXorFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(3))

	event = chunk.MutEventFromCausets(types.MakeCausets(2)).ToEvent()
	err = bitXorFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))
	partialResult := bitXorFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(1))

	// test bit_xor( decimal )
	defCaus.RetType = types.NewFieldType(allegrosql.TypeNewDecimal)
	bitXorFunc.ResetContext(s.ctx.GetStochaseinstein_dbars().StmtCtx, evalCtx)

	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	var dec types.MyDecimal
	err = dec.FromString([]byte("1.234"))
	c.Assert(err, IsNil)
	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitXorFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = dec.FromString([]byte("1.012"))
	c.Assert(err, IsNil)
	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitXorFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	err = dec.FromString([]byte("2.12345678"))
	c.Assert(err, IsNil)
	event = chunk.MutEventFromCausets(types.MakeCausets(&dec)).ToEvent()
	err = bitXorFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(2))
}

func (s *testAggFuncSuit) TestCount(c *C) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncCount, []expression.Expression{defCaus}, false)
	c.Assert(err, IsNil)
	countFunc := desc.GetAggFunc(ctx)
	evalCtx := countFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	result := countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(0))

	for _, event := range s.rows {
		err := countFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
		c.Assert(err, IsNil)
	}
	result = countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(5050))
	err = countFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, s.nullEvent)
	c.Assert(err, IsNil)
	result = countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(5050))
	partialResult := countFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(5050))

	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncCount, []expression.Expression{defCaus}, true)
	c.Assert(err, IsNil)
	distinctCountFunc := desc.GetAggFunc(ctx)
	evalCtx = distinctCountFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	for _, event := range s.rows {
		err := distinctCountFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
		c.Assert(err, IsNil)
	}
	result = distinctCountFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(100))
}

func (s *testAggFuncSuit) TestConcat(c *C) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	sep := &expression.DeferredCauset{
		Index:   1,
		RetType: types.NewFieldType(allegrosql.TypeVarchar),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncGroupConcat, []expression.Expression{defCaus, sep}, false)
	c.Assert(err, IsNil)
	concatFunc := desc.GetAggFunc(ctx)
	evalCtx := concatFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	result := concatFunc.GetResult(evalCtx)
	c.Assert(result.IsNull(), IsTrue)

	event := chunk.MutEventFromCausets(types.MakeCausets(1, "x"))
	err = concatFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = concatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1")

	event.SetCauset(0, types.NewIntCauset(2))
	err = concatFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = concatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1x2")

	event.SetCauset(0, types.NewCauset(nil))
	err = concatFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = concatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1x2")
	partialResult := concatFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetString(), Equals, "1x2")

	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncGroupConcat, []expression.Expression{defCaus, sep}, true)
	c.Assert(err, IsNil)
	distinctConcatFunc := desc.GetAggFunc(ctx)
	evalCtx = distinctConcatFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	event.SetCauset(0, types.NewIntCauset(1))
	err = distinctConcatFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = distinctConcatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1")

	event.SetCauset(0, types.NewIntCauset(1))
	err = distinctConcatFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = distinctConcatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1")
}

func (s *testAggFuncSuit) TestFirstEvent(c *C) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}

	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncFirstEvent, []expression.Expression{defCaus}, false)
	c.Assert(err, IsNil)
	firstEventFunc := desc.GetAggFunc(ctx)
	evalCtx := firstEventFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	event := chunk.MutEventFromCausets(types.MakeCausets(1)).ToEvent()
	err = firstEventFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result := firstEventFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	event = chunk.MutEventFromCausets(types.MakeCausets(2)).ToEvent()
	err = firstEventFunc.UFIDelate(evalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event)
	c.Assert(err, IsNil)
	result = firstEventFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))
	partialResult := firstEventFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(1))
}

func (s *testAggFuncSuit) TestMaxMin(c *C) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}

	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncMax, []expression.Expression{defCaus}, false)
	c.Assert(err, IsNil)
	maxFunc := desc.GetAggFunc(ctx)
	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncMin, []expression.Expression{defCaus}, false)
	c.Assert(err, IsNil)
	minFunc := desc.GetAggFunc(ctx)
	maxEvalCtx := maxFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)
	minEvalCtx := minFunc.CreateContext(s.ctx.GetStochaseinstein_dbars().StmtCtx)

	result := maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.IsNull(), IsTrue)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.IsNull(), IsTrue)

	event := chunk.MutEventFromCausets(types.MakeCausets(2))
	err = maxFunc.UFIDelate(maxEvalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))
	err = minFunc.UFIDelate(minEvalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))

	event.SetCauset(0, types.NewIntCauset(3))
	err = maxFunc.UFIDelate(maxEvalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.UFIDelate(minEvalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))

	event.SetCauset(0, types.NewIntCauset(1))
	err = maxFunc.UFIDelate(maxEvalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.UFIDelate(minEvalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(1))

	event.SetCauset(0, types.NewCauset(nil))
	err = maxFunc.UFIDelate(maxEvalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.UFIDelate(minEvalCtx, s.ctx.GetStochaseinstein_dbars().StmtCtx, event.ToEvent())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(1))
	partialResult := minFunc.GetPartialResult(minEvalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(1))
}
