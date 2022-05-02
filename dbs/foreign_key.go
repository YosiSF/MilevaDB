MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
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
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
)

func onCreateForeignKey(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var fkInfo perceptron.FKInfo
	err = job.DecodeArgs(&fkInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	fkInfo.ID = allocateIndexID(tblInfo)
	tblInfo.ForeignKeys = append(tblInfo.ForeignKeys, &fkInfo)

	originalState := fkInfo.State
	switch fkInfo.State {
	case perceptron.StateNone:
		// We just support record the foreign key, so we just make it public.
		// none -> public
		fkInfo.State = perceptron.StatePublic
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != fkInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
		return ver, nil
	default:
		return ver, ErrInvalidDBSState.GenWithStack("foreign key", fkInfo.State)
	}
}

func onDropForeignKey(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		fkName perceptron.CIStr
		found  bool
		fkInfo perceptron.FKInfo
	)
	err = job.DecodeArgs(&fkName)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	for _, fk := range tblInfo.ForeignKeys {
		if fk.Name.L == fkName.L {
			found = true
			fkInfo = *fk
		}
	}

	if !found {
		job.State = perceptron.JobStateCancelled
		return ver, schemareplicant.ErrForeignKeyNotExists.GenWithStackByArgs(fkName)
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
	case perceptron.StatePublic:
		// We just support record the foreign key, so we just make it none.
		// public -> none
		fkInfo.State = perceptron.StateNone
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != fkInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StateNone, ver, tblInfo)
		return ver, nil
	default:
		return ver, ErrInvalidDBSState.GenWithStackByArgs("foreign key", fkInfo.State)
	}

}
