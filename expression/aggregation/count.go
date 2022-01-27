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
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

type countFunction struct {
	aggFunction
}

// UFIDelate implements Aggregation interface.
func (cf *countFunction) UFIDelate(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, event chunk.Event) error {
	var datumBuf []types.Causet
	if cf.HasDistinct {
		datumBuf = make([]types.Causet, 0, len(cf.Args))
	}
	for _, a := range cf.Args {
		value, err := a.Eval(event)
		if err != nil {
			return err
		}
		if value.IsNull() {
			return nil
		}
		if cf.Mode == FinalMode || cf.Mode == Partial2Mode {
			evalCtx.Count += value.GetInt64()
		}
		if cf.HasDistinct {
			datumBuf = append(datumBuf, value)
		}
	}
	if cf.HasDistinct {
		d, err := evalCtx.DistinctChecker.Check(datumBuf)
		if err != nil {
			return err
		}
		if !d {
			return nil
		}
	}
	if cf.Mode == CompleteMode || cf.Mode == Partial1Mode {
		evalCtx.Count++
	}
	return nil
}

func (cf *countFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	if cf.HasDistinct {
		evalCtx.DistinctChecker = createDistinctChecker(sc)
	}
	evalCtx.Count = 0
}

// GetResult implements Aggregation interface.
func (cf *countFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Causet) {
	d.SetInt64(evalCtx.Count)
	return d
}

// GetPartialResult implements Aggregation interface.
func (cf *countFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Causet {
	return []types.Causet{cf.GetResult(evalCtx)}
}
