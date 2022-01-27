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
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

type firstEventFunction struct {
	aggFunction
}

// UFIDelate implements Aggregation interface.
func (ff *firstEventFunction) UFIDelate(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, event chunk.Event) error {
	if evalCtx.GotFirstEvent {
		return nil
	}
	if len(ff.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncFirstEvent")
	}
	value, err := ff.Args[0].Eval(event)
	if err != nil {
		return err
	}
	value.INTERLOCKy(&evalCtx.Value)
	evalCtx.GotFirstEvent = true
	return nil
}

// GetResult implements Aggregation interface.
func (ff *firstEventFunction) GetResult(evalCtx *AggEvaluateContext) types.Causet {
	return evalCtx.Value
}

func (ff *firstEventFunction) ResetContext(_ *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	evalCtx.GotFirstEvent = false
}

// GetPartialResult implements Aggregation interface.
func (ff *firstEventFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Causet {
	return []types.Causet{ff.GetResult(evalCtx)}
}
