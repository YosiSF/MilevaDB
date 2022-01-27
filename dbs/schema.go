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
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
)

func onCreateSchema(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	dbInfo := &perceptron.DBInfo{}
	if err := job.DecodeArgs(dbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo.ID = schemaID
	dbInfo.State = perceptron.StateNone

	err := checkSchemaNotExists(d, t, schemaID, dbInfo)
	if err != nil {
		if schemareplicant.ErrDatabaseExists.Equal(err) {
			// The database already exists, can't create it, we should cancel this job now.
			job.State = perceptron.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	ver, err = uFIDelateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch dbInfo.State {
	case perceptron.StateNone:
		// none -> public
		dbInfo.State = perceptron.StatePublic
		err = t.CreateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(perceptron.JobStateDone, perceptron.StatePublic, ver, dbInfo)
		return ver, nil
	default:
		// We can't enter here.
		return ver, errors.Errorf("invalid EDB state %v", dbInfo.State)
	}
}

func checkSchemaNotExists(d *dbsCtx, t *meta.Meta, schemaID int64, dbInfo *perceptron.DBInfo) error {
	// d.infoHandle maybe nil in some test.
	if d.infoHandle == nil {
		return checkSchemaNotExistsFromStore(t, schemaID, dbInfo)
	}
	// Try to use memory schemaReplicant info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoHandle.Get()
	if is.SchemaMetaVersion() == currVer {
		return checkSchemaNotExistsFromSchemaReplicant(is, schemaID, dbInfo)
	}
	return checkSchemaNotExistsFromStore(t, schemaID, dbInfo)
}

func checkSchemaNotExistsFromSchemaReplicant(is schemareplicant.SchemaReplicant, schemaID int64, dbInfo *perceptron.DBInfo) error {
	// Check database exists by name.
	if is.SchemaExists(dbInfo.Name) {
		return schemareplicant.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	// Check database exists by ID.
	if _, ok := is.SchemaByID(schemaID); ok {
		return schemareplicant.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	return nil
}

func checkSchemaNotExistsFromStore(t *meta.Meta, schemaID int64, dbInfo *perceptron.DBInfo) error {
	dbs, err := t.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, EDB := range dbs {
		if EDB.Name.L == dbInfo.Name.L {
			if EDB.ID != schemaID {
				return schemareplicant.ErrDatabaseExists.GenWithStackByArgs(EDB.Name)
			}
			dbInfo = EDB
		}
	}
	return nil
}

func onModifySchemaCharsetAndDefCauslate(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	var toCharset, toDefCauslate string
	if err := job.DecodeArgs(&toCharset, &toDefCauslate); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if dbInfo.Charset == toCharset && dbInfo.DefCauslate == toDefCauslate {
		job.FinishDBJob(perceptron.JobStateDone, perceptron.StatePublic, ver, dbInfo)
		return ver, nil
	}

	dbInfo.Charset = toCharset
	dbInfo.DefCauslate = toDefCauslate

	if err = t.UFIDelateDatabase(dbInfo); err != nil {
		return ver, errors.Trace(err)
	}
	if ver, err = uFIDelateSchemaVersion(t, job); err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(perceptron.JobStateDone, perceptron.StatePublic, ver, dbInfo)
	return ver, nil
}

func onDropSchema(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = uFIDelateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch dbInfo.State {
	case perceptron.StatePublic:
		// public -> write only
		job.SchemaState = perceptron.StateWriteOnly
		dbInfo.State = perceptron.StateWriteOnly
		err = t.UFIDelateDatabase(dbInfo)
	case perceptron.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = perceptron.StateDeleteOnly
		dbInfo.State = perceptron.StateDeleteOnly
		err = t.UFIDelateDatabase(dbInfo)
	case perceptron.StateDeleteOnly:
		dbInfo.State = perceptron.StateNone
		var blocks []*perceptron.BlockInfo
		blocks, err = t.ListBlocks(job.SchemaID)
		if err != nil {
			return ver, errors.Trace(err)
		}

		err = t.UFIDelateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = t.DroFIDelatabase(dbInfo.ID); err != nil {
			break
		}

		// Finish this job.
		if len(blocks) > 0 {
			job.Args = append(job.Args, getIDs(blocks))
		}
		job.FinishDBJob(perceptron.JobStateDone, perceptron.StateNone, ver, dbInfo)
	default:
		// We can't enter here.
		err = errors.Errorf("invalid EDB state %v", dbInfo.State)
	}

	return ver, errors.Trace(err)
}

func checkSchemaExistAndCancelNotExistJob(t *meta.Meta, job *perceptron.Job) (*perceptron.DBInfo, error) {
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if dbInfo == nil {
		job.State = perceptron.JobStateCancelled
		return nil, schemareplicant.ErrDatabaseDropExists.GenWithStackByArgs("")
	}
	return dbInfo, nil
}

func getIDs(blocks []*perceptron.BlockInfo) []int64 {
	ids := make([]int64, 0, len(blocks))
	for _, t := range blocks {
		ids = append(ids, t.ID)
		if t.GetPartitionInfo() != nil {
			ids = append(ids, getPartitionIDs(t)...)
		}
	}

	return ids
}
