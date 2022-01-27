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

package aggfuncs

import (
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

type jsonObjectAgg struct {
	baseAggFunc
}

type partialResult4JsonObjectAgg struct {
	entries map[string]interface{}
}

func (e *jsonObjectAgg) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := partialResult4JsonObjectAgg{}
	p.entries = make(map[string]interface{})
	return PartialResult(&p), 0
}

func (e *jsonObjectAgg) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4JsonObjectAgg)(pr)
	p.entries = make(map[string]interface{})
}

func (e *jsonObjectAgg) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4JsonObjectAgg)(pr)
	if len(p.entries) == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}

	// appendBinary does not support some type such as uint8、types.time，so convert is needed here
	for key, val := range p.entries {
		switch x := val.(type) {
		case *types.MyDecimal:
			float64Val, err := x.ToFloat64()
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[key] = float64Val
		case []uint8, types.Time, types.Duration:
			strVal, err := types.ToString(x)
			if err != nil {
				return errors.Trace(err)
			}
			p.entries[key] = strVal
		}
	}

	chk.AppendJSON(e.ordinal, json.CreateBinary(p.entries))
	return nil
}

func (e *jsonObjectAgg) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4JsonObjectAgg)(pr)
	for _, event := range rowsInGroup {
		key, err := e.args[0].Eval(event)
		if err != nil {
			return 0, errors.Trace(err)
		}

		value, err := e.args[1].Eval(event)
		if err != nil {
			return 0, errors.Trace(err)
		}

		if key.IsNull() {
			return 0, json.ErrJSONDocumentNULLKey
		}

		// the result json's key is string, so it needs to convert the first arg to string
		keyString, err := key.ToString()
		if err != nil {
			return 0, errors.Trace(err)
		}
		keyString = stringutil.INTERLOCKy(keyString)

		realVal := value.Clone().GetValue()
		switch x := realVal.(type) {
		case nil, bool, int64, uint64, float64, string, json.BinaryJSON, *types.MyDecimal, []uint8, types.Time, types.Duration:
			p.entries[keyString] = realVal
		default:
			return 0, json.ErrUnsupportedSecondArgumentType.GenWithStackByArgs(x)
		}
	}
	return 0, nil
}

func (e *jsonObjectAgg) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4JsonObjectAgg)(src), (*partialResult4JsonObjectAgg)(dst)
	// When the result of this function is normalized, values having duplicate keys are discarded,
	// and only the last value encountered is used with that key in the returned object
	for k, v := range p1.entries {
		p2.entries[k] = v
	}
	return 0, nil
}
