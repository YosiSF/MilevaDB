// INTERLOCKyright 2019 Venire Labs Inc
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


package merkle

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/errors/binlog"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/core/eekv"
)

//Hash pair for (field, value)
type HashPair struct {
	Field []byte
	Value []byte
}

type hasSpacetime struct {
	FieldCount int64
}

func (spacetime hashSpacetime) Value() []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(spacetime.FieldCount))
	return buf
}

func (t *TxMerkle)
