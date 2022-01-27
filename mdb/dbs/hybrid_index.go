//INTERLOCKyright 2020 WHTCORPS INC ALL RIGHTS RESERVED
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions.

func onCreateForeignKey(t *meta.Meta, job *serial.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var fkInfo serial.FKInfo
	err = job.DecodeArgs(&fkInfo)
	if err != nil {
		job.State = serial.JobStateCancelled
		return ver, errors.Trace(err)
	}
	fkInfo.ID = allocateIndexID(tblInfo)
	tblInfo.ForeignKeys = append(tblInfo.ForeignKeys, &fkInfo)

	originalState := fkInfo.State
	switch fkInfo.State {
	case serial.StateNone:
		// We just support record the foreign key, so we just make it public.
		// none -> public
		fkInfo.State = serial.StatePublic
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != fkInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(serial.JobStateDone, serial.StatePublic, ver, tblInfo)
		return ver, nil
	default:
		return ver, ErrInvaliddbsState.GenWithStack("foreign key", fkInfo.State)
	}
}

func onDropForeignKey(t *meta.Meta, job *serial.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		fkName serial.CIStr
		found  bool
		fkInfo serial.FKInfo
	)
	err = job.DecodeArgs(&fkName)
	if err != nil {
		job.State = serial.JobStateCancelled
		return ver, errors.Trace(err)
	}

	for _, fk := range tblInfo.ForeignKeys {
		if fk.Name.L == fkName.L {
			found = true
			fkInfo = *fk
		}
	}

	if !found {
		job.State = serial.JobStateCancelled
		return ver, schemaReplicant.ErrForeignKeyNotExists.GenWithStackByArgs(fkName)
	}

	nfks := tblInfo.ForeignKeys[:0]
	for _, fk := range tblInfo.ForeignKeys {
		if fk.Name.L != fkName.L {
			nfks = append(nfks, fk)
		}
	}
	tblInfo.ForeignKeys = nfks

	originalState := fkInfo.State
	switch fkInfo.State {
	case serial.StatePublic:
		// We just support record the foreign key, so we just make it none.
		// public -> none
		fkInfo.State = serial.StateNone
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != fkInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(serial.JobStateDone, serial.StateNone, ver, tblInfo)
		return ver, nil
	default:
		return ver, ErrInvaliddbsState.GenWithStackByArgs("foreign key", fkInfo.State)
	}

}
