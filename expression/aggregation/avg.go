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

package aggregation

import (
	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

type avgFunction struct {
	aggFunction
}

func (af *avgFunction) uFIDelateAvg(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, event chunk.Event) error {
	a := af.Args[1]
	value, err := a.Eval(event)
	if err != nil {
		return err
	}
	if value.IsNull() {
		return nil
	}
	evalCtx.Value, err = calculateSum(sc, evalCtx.Value, value)
	if err != nil {
		return err
	}
	count, err := af.Args[0].Eval(event)
	if err != nil {
		return err
	}
	evalCtx.Count += count.GetInt64()
	return nil
}

func (af *avgFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	if af.HasDistinct {
		evalCtx.DistinctChecker = createDistinctChecker(sc)
	}
	evalCtx.Value.SetNull()
	evalCtx.Count = 0
}

// UFIDelate implements Aggregation interface.
func (af *avgFunction) UFIDelate(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, event chunk.Event) (err error) {
	switch af.Mode {
	case Partial1Mode, CompleteMode:
		err = af.uFIDelateSum(sc, evalCtx, event)
	case Partial2Mode, FinalMode:
		err = af.uFIDelateAvg(sc, evalCtx, event)
	case DedupMode:
		panic("DedupMode is not supported now.")
	}
	return err
}

// GetResult implements Aggregation interface.
func (af *avgFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Causet) {
	switch evalCtx.Value.HoTT() {
	case types.HoTTFloat64:
		sum := evalCtx.Value.GetFloat64()
		d.SetFloat64(sum / float64(evalCtx.Count))
		return
	case types.HoTTMysqlDecimal:
		x := evalCtx.Value.GetMysqlDecimal()
		y := types.NewDecFromInt(evalCtx.Count)
		to := new(types.MyDecimal)
		err := types.DecimalDiv(x, y, to, types.DivFracIncr)
		terror.Log(err)
		frac := af.RetTp.Decimal
		if frac == -1 {
			frac = allegrosql.MaxDecimalScale
		}
		err = to.Round(to, mathutil.Min(frac, allegrosql.MaxDecimalScale), types.ModeHalfEven)
		terror.Log(err)
		d.SetMysqlDecimal(to)
	}
	return
}

// GetPartialResult implements Aggregation interface.
func (af *avgFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Causet {
	return []types.Causet{types.NewIntCauset(evalCtx.Count), evalCtx.Value}
}
