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

package dbs

import (
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
)

var (
	serverID           = "server_id"
	dbsSchemaVersion   = "dbs_schema_version"
	dbsJobID           = "dbs_job_id"
	dbsJobCausetAction = "dbs_job_action"
	dbsJobStartTS      = "dbs_job_start_ts"
	dbsJobState        = "dbs_job_state"
	dbsJobError        = "dbs_job_error"
	dbsJobRows         = "dbs_job_row_count"
	dbsJobSchemaState  = "dbs_job_schema_state"
	dbsJobSchemaID     = "dbs_job_schema_id"
	dbsJobBlockID      = "dbs_job_block_id"
	dbsJobSnapshotVer  = "dbs_job_snapshot_ver"
	dbsJobReorgHandle  = "dbs_job_reorg_handle"
	dbsJobArgs         = "dbs_job_args"
)

// GetSINTERLOCKe gets the status variables sINTERLOCKe.
func (d *dbs) GetSINTERLOCKe(status string) variable.SINTERLOCKeFlag {
	// Now dbs status variables sINTERLOCKe are all default sINTERLOCKe.
	return variable.DefaultStatusVarSINTERLOCKeFlag
}

// Stats returns the DBS statistics.
func (d *dbs) Stats(vars *variable.StochastikVars) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	m[serverID] = d.uuid
	var dbsInfo *admin.DBSInfo

	err := ekv.RunInNewTxn(d.causetstore, false, func(txn ekv.Transaction) error {
		var err1 error
		dbsInfo, err1 = admin.GetDBSInfo(txn)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return errors.Trace(err1)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	m[dbsSchemaVersion] = dbsInfo.SchemaVer
	// TODO: Get the owner information.
	if len(dbsInfo.Jobs) == 0 {
		return m, nil
	}
	// TODO: Add all job information if needed.
	job := dbsInfo.Jobs[0]
	m[dbsJobID] = job.ID
	m[dbsJobCausetAction] = job.Type.String()
	m[dbsJobStartTS] = job.StartTS / 1e9 // unit: second
	m[dbsJobState] = job.State.String()
	m[dbsJobRows] = job.RowCount
	if job.Error == nil {
		m[dbsJobError] = ""
	} else {
		m[dbsJobError] = job.Error.Error()
	}
	m[dbsJobSchemaState] = job.SchemaState.String()
	m[dbsJobSchemaID] = job.SchemaID
	m[dbsJobBlockID] = job.BlockID
	m[dbsJobSnapshotVer] = job.SnapshotVer
	m[dbsJobReorgHandle] = toString(dbsInfo.ReorgHandle)
	m[dbsJobArgs] = job.Args
	return m, nil
}
