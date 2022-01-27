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

package mockeinsteindb

import (
	fidel "github.com/einsteindb/fidel/client"
	"github.com/whtcorpsinc/errors"
)

// NewEinsteinDBAndFIDelClient creates a EinsteinDB client and FIDel client from options.
func NewEinsteinDBAndFIDelClient(path string) (*RPCClient, *Cluster, fidel.Client, error) {
	mvccStore, err := NewMVCCLevelDB(path)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	cluster := NewCluster(mvccStore)

	return NewRPCClient(cluster, mvccStore), cluster, NewFIDelClient(cluster), nil
}
