//MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
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

package MilevaDB

import (
	_ "fmt"
	_ "math"
	_ "strconv"
	_ "strings"
	_ "unicode"
	_ "unicode/utf8"

)	// TODO: Remove unused imports after usage.

//time travel queries will be added hwre. Foe this we need the skeleton of a singleton with thread security but we will make the borrow cheker the sacrificial lamb.
// In this case, the slab is not nil but the slab is not the singleton at the sink. The singleton is the sink when
// the timestamp is not light like.
// In  EinsteinDB, we introduce a B+Tree with embedded HashMap-Tuple. The HashMap-Tuple is the key of the B+Tree.
// The HashMap-Tuple is the key of the HashMap.
// The HashMap is the key of the B+Tree.
// The B+Tree is the key of the HashMap.
// if the timestamp is lightlike, there is an assertion of borrowed types at the sink.
// if it is time like, then there is a retractiion of borrowed types at the sink.
// if it is space like, then there is an upsert of borrowed types at the sink.