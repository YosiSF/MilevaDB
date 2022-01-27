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

package soliton

import (
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
)

// Event is an event that a dbs operation happened.
type Event struct {
	Tp                  perceptron.CausetActionType
	BlockInfo           *perceptron.BlockInfo
	PartInfo            *perceptron.PartitionInfo
	DeferredCausetInfos []*perceptron.DeferredCausetInfo
	IndexInfo           *perceptron.IndexInfo
}

// String implements fmt.Stringer interface.
func (e *Event) String() string {
	ret := fmt.Sprintf("(Event Type: %s", e.Tp)
	if e.BlockInfo != nil {
		ret += fmt.Sprintf(", Block ID: %d, Block Name %s", e.BlockInfo.ID, e.BlockInfo.Name)
	}
	if e.PartInfo != nil {
		ids := make([]int64, 0, len(e.PartInfo.Definitions))
		for _, def := range e.PartInfo.Definitions {
			ids = append(ids, def.ID)
		}
		ret += fmt.Sprintf(", Partition IDs: %v", ids)
	}
	for _, columnInfo := range e.DeferredCausetInfos {
		ret += fmt.Sprintf(", DeferredCauset ID: %d, DeferredCauset Name %s", columnInfo.ID, columnInfo.Name)
	}
	if e.IndexInfo != nil {
		ret += fmt.Sprintf(", Index ID: %d, Index Name %s", e.IndexInfo.ID, e.IndexInfo.Name)
	}
	return ret
}
