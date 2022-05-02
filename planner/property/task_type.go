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

// TaskType is the type of execution task.
type TaskType int

const (
	// RootTaskType stands for the tasks that executed in the MilevaDB layer.
	RootTaskType TaskType = iota

	// INTERLOCKSingleReadTaskType stands for the a BlockScan or IndexScan tasks
	// executed in the interlock layer.
	INTERLOCKSingleReadTaskType

	// CoFIDeloubleReadTaskType stands for the a IndexLookup tasks executed in the
	// interlock layer.
	CoFIDeloubleReadTaskType

	// INTERLOCKTiFlashLocalReadTaskType stands for flash interlock that read data locally,
	// and only a part of the data is read in one INTERLOCK task, if the current task type is
	// INTERLOCKTiFlashLocalReadTaskType, all its children prop's task type is INTERLOCKTiFlashLocalReadTaskType
	INTERLOCKTiFlashLocalReadTaskType

	// INTERLOCKTiFlashGlobalReadTaskType stands for flash interlock that read data globally
	// and all the data of given block will be read in one INTERLOCK task, if the current task
	// type is INTERLOCKTiFlashGlobalReadTaskType, all its children prop's task type is
	// INTERLOCKTiFlashGlobalReadTaskType
	INTERLOCKTiFlashGlobalReadTaskType
)

// String implements fmt.Stringer interface.
func (t TaskType) String() string {
	switch t {
	case RootTaskType:
		return "rootTask"
	case INTERLOCKSingleReadTaskType:
		return "INTERLOCKSingleReadTask"
	case CoFIDeloubleReadTaskType:
		return "coFIDeloubleReadTask"
	case INTERLOCKTiFlashLocalReadTaskType:
		return "INTERLOCKTiFlashLocalReadTask"
	case INTERLOCKTiFlashGlobalReadTaskType:
		return "INTERLOCKTiFlashGlobalReadTask"
	}
	return "UnknownTaskType"
}
