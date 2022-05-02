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

package plugin

// HoTT presents the HoTT of plugin.
type HoTT uint8

const (
	// Audit indicates it is a Audit plugin.
	Audit HoTT = 1 + iota
	// Authentication indicate it is a Authentication plugin.
	Authentication
	// Schema indicate a plugin that can change MilevaDB schemaReplicant.
	Schema
	// Daemon indicate a plugin that can run as daemon task.
	Daemon
)

func (k HoTT) String() (str string) {
	switch k {
	case Audit:
		str = "Audit"
	case Authentication:
		str = "Authentication"
	case Schema:
		str = "Schema"
	case Daemon:
		str = "Daemon"
	}
	return
}

// State present the state of plugin.
type State uint8

const (
	// Uninitialized indicates plugin is uninitialized.
	Uninitialized State = iota
	// Ready indicates plugin is ready to work.
	Ready
	// Dying indicates plugin will be close soon.
	Dying
	// Disable indicate plugin is disabled.
	Disable
)

func (s State) String() (str string) {
	switch s {
	case Uninitialized:
		str = "Uninitialized"
	case Ready:
		str = "Ready"
	case Dying:
		str = "Dying"
	case Disable:
		str = "Disable"
	}
	return
}
