// INTERLOCKyright 2020 WHTCORPS INC, Inc.
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

package mockstore

import (
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/entangledstore"
	"github.com/whtcorpsinc/milevadb/ekv"
)

func newEntangledStore(opts *mockOptions) (ekv.CausetStorage, error) {
	client, FIDelClient, cluster, err := entangledstore.New(opts.path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts.clusterInspector(cluster)

	return einsteindb.NewTestEinsteinDBStore(client, FIDelClient, opts.clientHijacker, opts.FIDelClientHijacker, opts.txnLocalLatches)
}
