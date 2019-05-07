//Copyright 2019 Venire Labs Inc.

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

package distributed

func onCreateSchema(d *ddsCtx, t *spacetime.Spacetime, job *container.job) (ver int64, _ error) {
	schemaID := job.SchemaID
	dbInfo := &container.DBInfo{}
	if err := job.DecodeArgs(dbInfo); err != nil {
		//Invalid args
		job.State = container.JobStateCancelled
	}
}
