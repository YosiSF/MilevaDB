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
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

type sumFunction struct {
	aggFunction
}

// UFIDelate implements Aggregation interface.
func (sf *sumFunction) UFIDelate(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, event chunk.Event) error {
	return sf.uFIDelateSum(sc, evalCtx, event)
}

// GetResult implements Aggregation interface.
func (sf *sumFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Causet) {
	return evalCtx.Value
}

// GetPartialResult implements Aggregation interface.
func (sf *sumFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Causet {
	return []types.Causet{sf.GetResult(evalCtx)}
}
