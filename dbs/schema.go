//Copyright 2020 WHTCORPS INC

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

package dbs

func onCreateSchema(d *dbsCtx, t *spacetime.Spacetime, Batch *container.Batch) (ver int64, _ error) {
	schemaID := Batch.SchemaID
	dbInfo := &container.DBInfo{}
	if err := Batch.DecodeArgs(dbInfo); err != nil {
		//Invalid args
		Batch.State = container.BatchStateCancelled
	}
}
