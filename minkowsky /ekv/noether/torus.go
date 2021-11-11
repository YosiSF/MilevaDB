//COPYRIGHT 2020 VENIRE LABS INC WHTCORPS INC ALL RIGHTS RESERVED
// HYBRID LICENSE [UNMAINTAINED]

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

package noether

import (
	"encoding/binary"
	"github.com/YosiSF/milevadb/ekv/noether/fs"
)

const (
	daggersPerTori = 31 //Potential N=1 Supersymmetry doublet
	toriSize = 512
	)

//atomize the event
type dagger struct {
	hash        uint32
	segmentID	uint16
	keySize     uint16
	valueSize   uint32
	offset		uint32	//holographic stop
}

func(da dagger) ekvSize() uint32 {
	return uint32(da.keySize) + da.valueSize
}

//torus is an array of daggers
type torus struct {
	daggers [daggersPerTori]
}