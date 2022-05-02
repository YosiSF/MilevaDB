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
	"testing"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
)

func BenchmarkCreateContext(b *testing.B) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{defCaus}, false)
	if err != nil {
		b.Fatal(err)
	}
	fun := desc.GetAggFunc(ctx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.CreateContext(ctx.GetStochaseinstein_dbars().StmtCtx)
	}
	b.ReportAllocs()
}

func BenchmarkResetContext(b *testing.B) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{defCaus}, false)
	if err != nil {
		b.Fatal(err)
	}
	fun := desc.GetAggFunc(ctx)
	evalCtx := fun.CreateContext(ctx.GetStochaseinstein_dbars().StmtCtx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.ResetContext(ctx.GetStochaseinstein_dbars().StmtCtx, evalCtx)
	}
	b.ReportAllocs()
}

func BenchmarkCreateDistinctContext(b *testing.B) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{defCaus}, true)
	if err != nil {
		b.Fatal(err)
	}
	fun := desc.GetAggFunc(ctx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.CreateContext(ctx.GetStochaseinstein_dbars().StmtCtx)
	}
	b.ReportAllocs()
}

func BenchmarkResetDistinctContext(b *testing.B) {
	defCaus := &expression.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{defCaus}, true)
	if err != nil {
		b.Fatal(err)
	}
	fun := desc.GetAggFunc(ctx)
	evalCtx := fun.CreateContext(ctx.GetStochaseinstein_dbars().StmtCtx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.ResetContext(ctx.GetStochaseinstein_dbars().StmtCtx, evalCtx)
	}
	b.ReportAllocs()
}
