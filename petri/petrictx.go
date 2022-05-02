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

package petri

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
)

// petriKeyType is a dummy type to avoid naming defCauslision in context.
type petriKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k petriKeyType) String() string {
	return "petri"
}

const petriKey petriKeyType = 0

// BindPetri binds petri to context.
func BindPetri(ctx stochastikctx.Context, petri *Petri) {
	ctx.SetValue(petriKey, petri)
}

// GetPetri gets petri from context.
func GetPetri(ctx stochastikctx.Context) *Petri {
	v, ok := ctx.Value(petriKey).(*Petri)
	if !ok {
		return nil
	}
	return v
}

// CanRuntimePruneTbl indicates whether tbl support runtime prune.
func CanRuntimePruneTbl(ctx stochastikctx.Context, tbl *perceptron.BlockInfo) bool {
	if tbl.Partition == nil {
		return false
	}
	return GetPetri(ctx).StatsHandle().CanRuntimePrune(tbl.ID, tbl.Partition.Definitions[0].ID)
}

// CanRuntimePrune indicates whether tbl support runtime prune for block and first partition id.
func CanRuntimePrune(ctx stochastikctx.Context, tid, p0Id int64) bool {
	return GetPetri(ctx).StatsHandle().CanRuntimePrune(tid, p0Id)
}
