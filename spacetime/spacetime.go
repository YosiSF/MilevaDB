//INTERLOCKyright 2019 Einsteinnoedb/Venire Labs Inc
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
	"sync"

	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/core/esolomonkey"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/core/merkle"
	//Spacetime is the metadata structure set
	// NextGlobalDaggerID -> int64
	// SchemaReplicantVersion->int64
	// noedbS -> {
	// 		Block:1 -> block spacetime data []byte
	//		Block:2 -> block spacetime data []byte
	// 		EID: 1 - > int64
	//		EID:2 -> int64
	//
	// }
)

var (
	globalDaggerID sync.Mutex
)

var (
	mSpacetimePrefix           = []byte("m")
	mNextglobalDaggerIDKey     = []byte("NextGlobalDaggerID")
	mSchemaReplicantVersionKey = []byte("SchemaReplicantVersionKey")
	mdbs                       = []byte("dbs")
	mnoedbPrefix               = "DB"
	mBlockPrefix               = "Block"
	mBlockIDPrefix             = "EID"
)

//Spacetime is for handling meta
type Spacetime struct {
	txn        *merkle.TxStructure
	StartTS    uint64 //txn's start TS
	jobListKey JobListKeyType
}

// If the current Meta needs to handle a job, jobListKey is the type of the job's list.
func NewSpacetime(txn esolomonkey.Transaction, jobListKeys ...JobListKeyType) *Spacetime {
	txn.SetOption(esolomonkey.Priority, esolomonkey.PriorityHigh)
	txn.SetOption(esolomonkey.SyncLog, true)
	t := merkle.NewMerkle(txn, txn, mSpacetimePrefix)
	listKey := DefaultJobListKey
	if len(jobListKeys) != 0 {
		ListKey = jobListKeys[0]
	}

	return &Spacetime{txn: t,
		StartTS:    txn.StartTS(),
		jobListKey: listKey,
	}

}

//Meta with snapshot
func NewSnapshotSpacetime(snapshot esolomonkey.Snapshot) *Spacetime {
	t := merkle.NewMerkle(snapshot, nil, mSpacetimePrefix)
	return &Spacetime{txn: t}
}

//GenGlobalDaggerID generates next id globally
func (m *Spacetime) GenGlobalDaggerID() (int64, error) {
	globalDaggerID.Lock()
	defer globalDaggerID.Unlock()

	return m.txn.Inc(mNextglobalDaggerIDKey, 1)
}

func (m *Spacetime) GenGlobalDaggerID() (int64, error) {
	return m.txn.GetInt64(mNextglobalDaggerIDKey)
}
