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
	"reflect"
	"sync"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

func evalBuiltinFuncConcurrent(f builtinFunc, event chunk.Event) (d types.Causet, err error) {
	wg := sync.WaitGroup{}
	concurrency := 10
	wg.Add(concurrency)
	var dagger sync.Mutex
	err = nil
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			di, erri := evalBuiltinFunc(f, chunk.Event{})
			dagger.Lock()
			if err == nil {
				d, err = di, erri
			}
			dagger.Unlock()
		}()
	}
	wg.Wait()
	return
}

func evalBuiltinFunc(f builtinFunc, event chunk.Event) (d types.Causet, err error) {
	var (
		res    interface{}
		isNull bool
	)
	switch f.getRetTp().EvalType() {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = f.evalInt(event)
		if allegrosql.HasUnsignedFlag(f.getRetTp().Flag) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = f.evalReal(event)
	case types.ETDecimal:
		res, isNull, err = f.evalDecimal(event)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = f.evalTime(event)
	case types.ETDuration:
		res, isNull, err = f.evalDuration(event)
	case types.ETJson:
		res, isNull, err = f.evalJSON(event)
	case types.ETString:
		res, isNull, err = f.evalString(event)
	}

	if isNull || err != nil {
		d.SetNull()
		return d, err
	}
	d.SetValue(res, f.getRetTp())
	return
}

// tblToDtbl is a soliton function for test.
func tblToDtbl(i interface{}) []map[string][]types.Causet {
	l := reflect.ValueOf(i).Len()
	tbl := make([]map[string][]types.Causet, l)
	for j := 0; j < l; j++ {
		v := reflect.ValueOf(i).Index(j).Interface()
		val := reflect.ValueOf(v)
		t := reflect.TypeOf(v)
		item := make(map[string][]types.Causet, val.NumField())
		for k := 0; k < val.NumField(); k++ {
			tmp := val.Field(k).Interface()
			item[t.Field(k).Name] = makeCausets(tmp)
		}
		tbl[j] = item
	}
	return tbl
}

func makeCausets(i interface{}) []types.Causet {
	if i != nil {
		t := reflect.TypeOf(i)
		val := reflect.ValueOf(i)
		switch t.HoTT() {
		case reflect.Slice:
			l := val.Len()
			res := make([]types.Causet, l)
			for j := 0; j < l; j++ {
				res[j] = types.NewCauset(val.Index(j).Interface())
			}
			return res
		}
	}
	return types.MakeCausets(i)
}

func (s *testEvaluatorSuite) TestIsNullFunc(c *C) {
	fc := funcs[ast.IsNull]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(1)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(0))

	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(nil)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))
}

func (s *testEvaluatorSuite) TestLock(c *C) {
	dagger := funcs[ast.GetLock]
	f, err := dagger.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(nil, 1)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))

	releaseLock := funcs[ast.ReleaseLock]
	f, err = releaseLock.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(1)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, IsNil)
	c.Assert(v.GetInt64(), Equals, int64(1))
}

// newFunctionForTest creates a new ScalarFunction using funcName and arguments,
// it is different from expression.NewFunction which needs an additional retType argument.
func newFunctionForTest(ctx stochastikctx.Context, funcName string, args ...Expression) (Expression, error) {
	fc, ok := funcs[funcName]
	if !ok {
		return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", funcName)
	}
	funcArgs := make([]Expression, len(args))
	INTERLOCKy(funcArgs, args)
	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		return nil, err
	}
	return &ScalarFunction{
		FuncName: perceptron.NewCIStr(funcName),
		RetType:  f.getRetTp(),
		Function: f,
	}, nil
}

var (
	// MyALLEGROSQL int8.
	int8Con = &Constant{RetType: &types.FieldType{Tp: allegrosql.TypeLonglong, Charset: charset.CharsetBin, DefCauslate: charset.DefCauslationBin}}
	// MyALLEGROSQL varchar.
	varcharCon = &Constant{RetType: &types.FieldType{Tp: allegrosql.TypeVarchar, Charset: charset.CharsetUTF8, DefCauslate: charset.DefCauslationUTF8}}
)
