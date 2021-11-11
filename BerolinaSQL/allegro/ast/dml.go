//2020 WHTCORPS INC ALL RIGHTS RESERVED
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast


var (
	_ DMLNode = &DeleteStmt{}
	_ DMLNode = &InsertStmt{}
	_ DMLNode = &UnionStmt{}
	_ DMLNode = &UpdateStmt{}
	_ DMLNode = &SelectStmt{}
	_ DMLNode = &ShowStmt{}
	_ DMLNode = &LoadDataStmt{}
	_ DMLNode = &SplitRegionStmt{}

	_ Node = &Assignment{}
	_ Node = &ByItem{}
	_ Node = &FieldList{}
	_ Node = &GroupByClause{}
	_ Node = &HavingClause{}
	_ Node = &Join{}
	_ Node = &Limit{}
	_ Node = &OnCondition{}
	_ Node = &OrderByClause{}
	_ Node = &SelectField{}
	_ Node = &TableName{}
	_ Node = &TableRefsClause{}
	_ Node = &TableSource{}
	_ Node = &UnionSelectList{}
	_ Node = &WildCardField{}
	_ Node = &WindowSpec{}
	_ Node = &PartitionByClause{}
	_ Node = &FrameClause{}
	_ Node = &FrameBound{}
)

// JoinType is join type, including cross/left/right/full.
type JoinType int

const (
	// CrossJoin is cross join type.
	CrossJoin JoinType = iota + 1
	// LeftJoin is left Join type.
	LeftJoin
	// RightJoin is right Join type.
	RightJoin
)

// Join represents table join.
type Join struct {
	node
	resultSetNode

	// Left table can be TableSource or JoinNode.
	Left ResultSetNode
	// Right table can be TableSource or JoinNode or nil.
	Right ResultSetNode
	// Tp represents join type.
	Tp JoinType
	// On represents join on condition.
	On *OnCondition
	// Using represents join using clause.
	Using []*ColumnName
	// NaturalJoin represents join is natural join.
	NaturalJoin bool
	// StraightJoin represents a straight join.
	StraightJoin bool
}
