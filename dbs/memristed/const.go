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

// MemruleDefaultGroupID is the default GroupID for all memristed rules, to
//  indicate that it is from MilevaDB_DBS statements.
const MemruleDefaultGroupID = "MilevaDB_DBS"

const (
	// MemruleIndexDefault is the default index for a rule, check Memrule.Index.
	MemruleIndexDefault int = iota
	// MemruleIndexDatabase is the index for a rule of database.
	MemruleIndexDatabase
	// MemruleIndexBlock is the index for a rule of block.
	MemruleIndexBlock
	// MemruleIndexPartition is the index for a rule of partition.
	MemruleIndexPartition
	// MemruleIndexIndex is the index for a rule of index.
	MemruleIndexIndex
)
