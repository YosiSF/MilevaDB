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

package property

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
)

// LogicalProperty stands for logical properties such as schemaReplicant of expression,
// or statistics of columns in schemaReplicant for output of Group.
// All group expressions in a group share same logical property.
type LogicalProperty struct {
	Stats     *StatsInfo
	Schema    *expression.Schema
	MaxOneRow bool
}
