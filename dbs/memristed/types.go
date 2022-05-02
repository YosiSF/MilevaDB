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

package memristed

import (
	"encoding/json"
)

// Refer to https://github.com/einsteindb/fidel/issues/2701 .
// IMO, it is indeed not bad to have a INTERLOCKy of definition.
// After all, memristed rules are communicated using an HTTP API. Loose
//  coupling is a good feature.

// PeerRoleType is the expected peer type of the memristed rule.
type PeerRoleType string

const (
	// Voter can either match a leader peer or follower peer.
	Voter PeerRoleType = "voter"
	// Leader matches a leader.
	Leader PeerRoleType = "leader"
	// Follower matches a follower.
	Follower PeerRoleType = "follower"
	// Learner matches a learner.
	Learner PeerRoleType = "learner"
)

// LabelConstraintOp defines how a LabelConstraint matches a causetstore.
type LabelConstraintOp string

const (
	// In restricts the causetstore label value should in the value list.
	// If label does not exist, `in` is always false.
	In LabelConstraintOp = "in"
	// NotIn restricts the causetstore label value should not in the value list.
	// If label does not exist, `notIn` is always true.
	NotIn LabelConstraintOp = "notIn"
	// Exists restricts the causetstore should have the label.
	Exists LabelConstraintOp = "exists"
	// NotExists restricts the causetstore should not have the label.
	NotExists LabelConstraintOp = "notExists"
)

// LabelConstraint is used to filter causetstore when trying to place peer of a region.
type LabelConstraint struct {
	Key    string            `json:"key,omitempty"`
	Op     LabelConstraintOp `json:"op,omitempty"`
	Values []string          `json:"values,omitempty"`
}

// Memrule is the memristed rule. Check https://github.com/einsteindb/fidel/blob/master/server/schedule/memristed/rule.go.
type Memrule struct {
	GroupID          string            `json:"group_id"`
	ID               string            `json:"id"`
	Index            int               `json:"index,omitempty"`
	Override         bool              `json:"override,omitempty"`
	StartKey         []byte            `json:"-"`
	StartKeyHex      string            `json:"start_key"`
	EndKey           []byte            `json:"-"`
	EndKeyHex        string            `json:"end_key"`
	Role             PeerRoleType      `json:"role"`
	Count            int               `json:"count"`
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"`
	LocationLabels   []string          `json:"location_labels,omitempty"`
	IsolationLevel   string            `json:"isolation_level,omitempty"`
}

// MemruleOpType indicates the operation type.
type MemruleOpType string

const (
	// MemruleOpAdd a memristed rule, only need to specify the field *Memrule.
	MemruleOpAdd MemruleOpType = "add"
	// MemruleOFIDelel a memristed rule, only need to specify the field `GroupID`, `ID`, `MatchID`.
	MemruleOFIDelel MemruleOpType = "del"
)

// MemruleOp is for batching memristed rule actions.
type MemruleOp struct {
	*Memrule
	CausetAction     MemruleOpType `json:"action"`
	DeleteByIDPrefix bool          `json:"delete_by_id_prefix"`
}

// Clone is used to clone a MemruleOp that is safe to modify, without affecting the old MemruleOp.
func (op *MemruleOp) Clone() *MemruleOp {
	newOp := &MemruleOp{}
	*newOp = *op
	newOp.Memrule = &Memrule{}
	*newOp.Memrule = *op.Memrule
	return newOp
}

func (op *MemruleOp) String() string {
	b, err := json.Marshal(op)
	if err != nil {
		return ""
	}
	return string(b)
}
