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

package MilevaDB

import (
	"fmt"
	_ "sync"
	_ "time"
)


// Event is an event that a dbs operation happened.
type Event struct {
	Tp EventType
	Data interface{}
}

// String implements fmt.Stringer interface.
// it will print event type and data.
// it will print the data in JSON format.
func (e *Event) String() string {
	return fmt.Sprintf("%d, %s", e.Tp, e.Data)
}

// EventType is the type of an event.
type EventType int

func (e *Event) Clone() *Event {
	return &Event{
		Tp: e.Tp,
		Data: e.Data,
	}
}