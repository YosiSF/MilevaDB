// INTERLOCKyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// PartitionPruning finds all used partitions according to query conditions, it will
// return nil if condition match none of partitions. The return value is a array of the
// idx in the partition definitions array, use pi.Definitions[idx] to get the partition ID
func PartitionPruning(ctx stochastikctx.Context, tbl block.PartitionedBlock, conds []expression.Expression, partitionNames []perceptron.CIStr,
	columns []*expression.DeferredCauset, names types.NameSlice) ([]int, error) {
	s := partitionProcessor{}
	pi := tbl.Meta().Partition
	switch pi.Type {
	case perceptron.PartitionTypeHash:
		return s.pruneHashPartition(ctx, tbl, partitionNames, conds, columns, names)
	case perceptron.PartitionTypeRange:
		rangeOr, err := s.pruneRangePartition(ctx, pi, tbl, conds, columns, names)
		if err != nil {
			return nil, err
		}
		ret := s.convertToIntSlice(rangeOr, pi, partitionNames)
		return ret, nil
	}
	return []int{FullRange}, nil
}
