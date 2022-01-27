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
	"strings"
)

// KeyInfo stores the defCausumns of one unique key or primary key.
type KeyInfo []*DeferredCauset

// Clone INTERLOCKies the entire UniqueKey.
func (ki KeyInfo) Clone() KeyInfo {
	result := make([]*DeferredCauset, 0, len(ki))
	for _, defCaus := range ki {
		result = append(result, defCaus.Clone().(*DeferredCauset))
	}
	return result
}

// Schema stands for the event schemaReplicant and unique key information get from input.
type Schema struct {
	DeferredCausets []*DeferredCauset
	Keys            []KeyInfo
}

// String implements fmt.Stringer interface.
func (s *Schema) String() string {
	defCausStrs := make([]string, 0, len(s.DeferredCausets))
	for _, defCaus := range s.DeferredCausets {
		defCausStrs = append(defCausStrs, defCaus.String())
	}
	ukStrs := make([]string, 0, len(s.Keys))
	for _, key := range s.Keys {
		ukDefCausStrs := make([]string, 0, len(key))
		for _, defCaus := range key {
			ukDefCausStrs = append(ukDefCausStrs, defCaus.String())
		}
		ukStrs = append(ukStrs, "["+strings.Join(ukDefCausStrs, ",")+"]")
	}
	return "DeferredCauset: [" + strings.Join(defCausStrs, ",") + "] Unique key: [" + strings.Join(ukStrs, ",") + "]"
}

// Clone INTERLOCKies the total schemaReplicant.
func (s *Schema) Clone() *Schema {
	defcaus := make([]*DeferredCauset, 0, s.Len())
	keys := make([]KeyInfo, 0, len(s.Keys))
	for _, defCaus := range s.DeferredCausets {
		defcaus = append(defcaus, defCaus.Clone().(*DeferredCauset))
	}
	for _, key := range s.Keys {
		keys = append(keys, key.Clone())
	}
	schemaReplicant := NewSchema(defcaus...)
	schemaReplicant.SetUniqueKeys(keys)
	return schemaReplicant
}

// ExprFromSchema checks if all defCausumns of this expression are from the same schemaReplicant.
func ExprFromSchema(expr Expression, schemaReplicant *Schema) bool {
	switch v := expr.(type) {
	case *DeferredCauset:
		return schemaReplicant.Contains(v)
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			if !ExprFromSchema(arg, schemaReplicant) {
				return false
			}
		}
		return true
	case *CorrelatedDeferredCauset, *Constant:
		return true
	}
	return false
}

// RetrieveDeferredCauset retrieves defCausumn in expression from the defCausumns in schemaReplicant.
func (s *Schema) RetrieveDeferredCauset(defCaus *DeferredCauset) *DeferredCauset {
	index := s.DeferredCausetIndex(defCaus)
	if index != -1 {
		return s.DeferredCausets[index]
	}
	return nil
}

// IsUniqueKey checks if this defCausumn is a unique key.
func (s *Schema) IsUniqueKey(defCaus *DeferredCauset) bool {
	for _, key := range s.Keys {
		if len(key) == 1 && key[0].Equal(nil, defCaus) {
			return true
		}
	}
	return false
}

// DeferredCausetIndex finds the index for a defCausumn.
func (s *Schema) DeferredCausetIndex(defCaus *DeferredCauset) int {
	for i, c := range s.DeferredCausets {
		if c.UniqueID == defCaus.UniqueID {
			return i
		}
	}
	return -1
}

// Contains checks if the schemaReplicant contains the defCausumn.
func (s *Schema) Contains(defCaus *DeferredCauset) bool {
	return s.DeferredCausetIndex(defCaus) != -1
}

// Len returns the number of defCausumns in schemaReplicant.
func (s *Schema) Len() int {
	return len(s.DeferredCausets)
}

// Append append new defCausumn to the defCausumns stored in schemaReplicant.
func (s *Schema) Append(defCaus ...*DeferredCauset) {
	s.DeferredCausets = append(s.DeferredCausets, defCaus...)
}

// SetUniqueKeys will set the value of Schema.Keys.
func (s *Schema) SetUniqueKeys(keys []KeyInfo) {
	s.Keys = keys
}

// DeferredCausetsIndices will return a slice which contains the position of each defCausumn in schemaReplicant.
// If there is one defCausumn that doesn't match, nil will be returned.
func (s *Schema) DeferredCausetsIndices(defcaus []*DeferredCauset) (ret []int) {
	ret = make([]int, 0, len(defcaus))
	for _, defCaus := range defcaus {
		pos := s.DeferredCausetIndex(defCaus)
		if pos != -1 {
			ret = append(ret, pos)
		} else {
			return nil
		}
	}
	return
}

// DeferredCausetsByIndices returns defCausumns by multiple offsets.
// Callers should guarantee that all the offsets provided should be valid, which means offset should:
// 1. not smaller than 0, and
// 2. not exceed len(s.DeferredCausets)
func (s *Schema) DeferredCausetsByIndices(offsets []int) []*DeferredCauset {
	defcaus := make([]*DeferredCauset, 0, len(offsets))
	for _, offset := range offsets {
		defcaus = append(defcaus, s.DeferredCausets[offset])
	}
	return defcaus
}

// ExtractDefCausGroups checks if defCausumn groups are from current schemaReplicant, and returns
// offsets of those satisfied defCausumn groups.
func (s *Schema) ExtractDefCausGroups(defCausGroups [][]*DeferredCauset) ([][]int, []int) {
	if len(defCausGroups) == 0 {
		return nil, nil
	}
	extracted := make([][]int, 0, len(defCausGroups))
	offsets := make([]int, 0, len(defCausGroups))
	for i, g := range defCausGroups {
		if j := s.DeferredCausetsIndices(g); j != nil {
			extracted = append(extracted, j)
			offsets = append(offsets, i)
		}
	}
	return extracted, offsets
}

// MergeSchema will merge two schemaReplicant into one schemaReplicant. We shouldn't need to consider unique keys.
// That will be processed in build_key_info.go.
func MergeSchema(lSchema, rSchema *Schema) *Schema {
	if lSchema == nil && rSchema == nil {
		return nil
	}
	if lSchema == nil {
		return rSchema.Clone()
	}
	if rSchema == nil {
		return lSchema.Clone()
	}
	tmpL := lSchema.Clone()
	tmpR := rSchema.Clone()
	ret := NewSchema(append(tmpL.DeferredCausets, tmpR.DeferredCausets...)...)
	return ret
}

// GetUsedList shows whether each defCausumn in schemaReplicant is contained in usedDefCauss.
func GetUsedList(usedDefCauss []*DeferredCauset, schemaReplicant *Schema) []bool {
	tmpSchema := NewSchema(usedDefCauss...)
	used := make([]bool, schemaReplicant.Len())
	for i, defCaus := range schemaReplicant.DeferredCausets {
		used[i] = tmpSchema.Contains(defCaus)
	}
	return used
}

// NewSchema returns a schemaReplicant made by its parameter.
func NewSchema(defcaus ...*DeferredCauset) *Schema {
	return &Schema{DeferredCausets: defcaus}
}
