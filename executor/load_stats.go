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

package executor

import (
	"context"
	"encoding/json"

	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics/handle"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/errors"
)

var _ Executor = &LoadStatsExec{}

// LoadStatsExec represents a load statistic executor.
type LoadStatsExec struct {
	baseExecutor
	info *LoadStatsInfo
}

// LoadStatsInfo saves the information of loading statistic operation.
type LoadStatsInfo struct {
	Path string
	Ctx  stochastikctx.Context
}

// loadStatsVarKeyType is a dummy type to avoid naming defCauslision in context.
type loadStatsVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadStatsVarKeyType) String() string {
	return "load_stats_var"
}

// LoadStatsVarKey is a variable key for load statistic.
const LoadStatsVarKey loadStatsVarKeyType = 0

// Next implements the Executor Next interface.
func (e *LoadStatsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if len(e.info.Path) == 0 {
		return errors.New("Load Stats: file path is empty")
	}
	val := e.ctx.Value(LoadStatsVarKey)
	if val != nil {
		e.ctx.SetValue(LoadStatsVarKey, nil)
		return errors.New("Load Stats: previous load stats option isn't closed normally")
	}
	e.ctx.SetValue(LoadStatsVarKey, e.info)
	return nil
}

// Close implements the Executor Close interface.
func (e *LoadStatsExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadStatsExec) Open(ctx context.Context) error {
	return nil
}

// UFIDelate uFIDelates the stats of the corresponding block according to the data.
func (e *LoadStatsInfo) UFIDelate(data []byte) error {
	jsonTbl := &handle.JSONBlock{}
	if err := json.Unmarshal(data, jsonTbl); err != nil {
		return errors.Trace(err)
	}
	do := petri.GetPetri(e.Ctx)
	h := do.StatsHandle()
	if h == nil {
		return errors.New("Load Stats: handle is nil")
	}
	return h.LoadStatsFromJSON(schemareplicant.GetSchemaReplicant(e.Ctx), jsonTbl)
}
