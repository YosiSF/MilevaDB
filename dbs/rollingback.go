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
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"go.uber.org/zap"
)

func uFIDelateDefCaussNull2NotNull(tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo) error {
	nullDefCauss, err := getNullDefCausInfos(tblInfo, indexInfo)
	if err != nil {
		return errors.Trace(err)
	}

	for _, defCaus := range nullDefCauss {
		defCaus.Flag |= allegrosql.NotNullFlag
		defCaus.Flag = defCaus.Flag &^ allegrosql.PreventNullInsertFlag
	}
	return nil
}

func convertAddIdxJob2RollbackJob(t *meta.Meta, job *perceptron.Job, tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo, err error) (int64, error) {
	job.State = perceptron.JobStateRollingback

	if indexInfo.Primary {
		nullDefCauss, err := getNullDefCausInfos(tblInfo, indexInfo)
		if err != nil {
			return 0, errors.Trace(err)
		}
		for _, defCaus := range nullDefCauss {
			// Field PreventNullInsertFlag flag reset.
			defCaus.Flag = defCaus.Flag &^ allegrosql.PreventNullInsertFlag
		}
	}

	// the second args will be used in onDropIndex.
	job.Args = []interface{}{indexInfo.Name, getPartitionIDs(tblInfo)}
	// If add index job rollbacks in write reorganization state, its need to delete all keys which has been added.
	// Its work is the same as drop index job do.
	// The write reorganization state in add index job that likes write only state in drop index job.
	// So the next state is delete only state.
	originalState := indexInfo.State
	indexInfo.State = perceptron.StateDeleteOnly
	// Change dependent hidden defCausumns if necessary.
	uFIDelateHiddenDeferredCausets(tblInfo, indexInfo, perceptron.StateDeleteOnly)
	job.SchemaState = perceptron.StateDeleteOnly
	ver, err1 := uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != indexInfo.State)
	if err1 != nil {
		return ver, errors.Trace(err1)
	}

	if ekv.ErrKeyExists.Equal(err) {
		return ver, ekv.ErrKeyExists.GenWithStackByArgs("", indexInfo.Name.O)
	}

	return ver, errors.Trace(err)
}

// convertNotStartAddIdxJob2RollbackJob converts the add index job that are not started workers to rollingbackJob,
// to rollback add index operations. job.SnapshotVer == 0 indicates the workers are not started.
func convertNotStartAddIdxJob2RollbackJob(t *meta.Meta, job *perceptron.Job, occuredErr error) (ver int64, err error) {
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		unique                  bool
		indexName               perceptron.CIStr
		indexPartSpecifications []*ast.IndexPartSpecification
		indexOption             *ast.IndexOption
	)
	err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo == nil {
		job.State = perceptron.JobStateCancelled
		return ver, errCancelledDBSJob
	}
	return convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, occuredErr)
}

// rollingbackModifyDeferredCauset change the modifying-defCausumn job into rolling back state.
// Since modifying defCausumn job has two types: normal-type and reorg-type, we should handle it respectively.
// normal-type has only two states:    None -> Public
// reorg-type has five states:         None -> Delete-only -> Write-only -> Write-org -> Public
func rollingbackModifyDeferredCauset(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	_, tblInfo, oldDefCaus, jp, err := getModifyDeferredCausetInfo(t, job)
	if err != nil {
		return ver, err
	}
	if !needChangeDeferredCausetData(oldDefCaus, jp.newDefCaus) {
		// Normal-type rolling back
		if job.SchemaState == perceptron.StateNone {
			// When change null to not null, although state is unchanged with none, the oldDefCaus flag's has been changed to preNullInsertFlag.
			// To roll back this HoTT of normal job, it is necessary to mark the state as JobStateRollingback to restore the old defCaus's flag.
			if jp.modifyDeferredCausetTp == allegrosql.TypeNull && tblInfo.DeferredCausets[oldDefCaus.Offset].Flag|allegrosql.PreventNullInsertFlag != 0 {
				job.State = perceptron.JobStateRollingback
				return ver, errCancelledDBSJob
			}
			// Normal job with stateNone can be cancelled directly.
			job.State = perceptron.JobStateCancelled
			return ver, errCancelledDBSJob
		}
		// StatePublic couldn't be cancelled.
		job.State = perceptron.JobStateRunning
		return ver, nil
	}
	// reorg-type rolling back
	if jp.changingDefCaus == nil {
		// The job hasn't been handled and we cancel it directly.
		job.State = perceptron.JobStateCancelled
		return ver, errCancelledDBSJob
	}
	// The job has been in it's midbse state and we roll it back.
	job.State = perceptron.JobStateRollingback
	return ver, errCancelledDBSJob
}

func rollingbackAddDeferredCauset(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	job.State = perceptron.JobStateRollingback
	tblInfo, defCausumnInfo, defCaus, _, _, err := checkAddDeferredCauset(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if defCausumnInfo == nil {
		job.State = perceptron.JobStateCancelled
		return ver, errCancelledDBSJob
	}

	originalState := defCausumnInfo.State
	defCausumnInfo.State = perceptron.StateDeleteOnly
	job.SchemaState = perceptron.StateDeleteOnly

	job.Args = []interface{}{defCaus.Name}
	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausumnInfo.State)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return ver, errCancelledDBSJob
}

func rollingbackAddDeferredCausets(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	job.State = perceptron.JobStateRollingback
	tblInfo, defCausumnInfos, _, _, _, _, err := checkAddDeferredCausets(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(defCausumnInfos) == 0 {
		job.State = perceptron.JobStateCancelled
		return ver, errCancelledDBSJob
	}

	defCausNames := make([]perceptron.CIStr, len(defCausumnInfos))
	originalState := defCausumnInfos[0].State
	for i, defCausumnInfo := range defCausumnInfos {
		defCausumnInfos[i].State = perceptron.StateDeleteOnly
		defCausNames[i] = defCausumnInfo.Name
	}
	ifExists := make([]bool, len(defCausumnInfos))

	job.SchemaState = perceptron.StateDeleteOnly
	job.Args = []interface{}{defCausNames, ifExists}
	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausumnInfos[0].State)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return ver, errCancelledDBSJob
}

func rollingbackDropDeferredCauset(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	tblInfo, defCausInfo, idxInfos, err := checkDropDeferredCauset(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	for _, indexInfo := range idxInfos {
		switch indexInfo.State {
		case perceptron.StateWriteOnly, perceptron.StateDeleteOnly, perceptron.StateDeleteReorganization, perceptron.StateNone:
			// We can not rollback now, so just continue to drop index.
			// In function isJobRollbackable will let job rollback when state is StateNone.
			// When there is no index related to the drop defCausumn job it is OK, but when there has indices, we should
			// make sure the job is not rollback.
			job.State = perceptron.JobStateRunning
			return ver, nil
		case perceptron.StatePublic:
		default:
			return ver, ErrInvalidDBSState.GenWithStackByArgs("index", indexInfo.State)
		}
	}

	// StatePublic means when the job is not running yet.
	if defCausInfo.State == perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		job.FinishBlockJob(perceptron.JobStateRollbackDone, perceptron.StatePublic, ver, tblInfo)
		return ver, errCancelledDBSJob
	}
	// In the state of drop defCausumn `write only -> delete only -> reorganization`,
	// We can not rollback now, so just continue to drop defCausumn.
	job.State = perceptron.JobStateRunning
	return ver, nil
}

func rollingbackDropDeferredCausets(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	tblInfo, defCausInfos, _, idxInfos, err := checkDropDeferredCausets(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	for _, indexInfo := range idxInfos {
		switch indexInfo.State {
		case perceptron.StateWriteOnly, perceptron.StateDeleteOnly, perceptron.StateDeleteReorganization, perceptron.StateNone:
			// We can not rollback now, so just continue to drop index.
			// In function isJobRollbackable will let job rollback when state is StateNone.
			// When there is no index related to the drop defCausumns job it is OK, but when there has indices, we should
			// make sure the job is not rollback.
			job.State = perceptron.JobStateRunning
			return ver, nil
		case perceptron.StatePublic:
		default:
			return ver, ErrInvalidDBSState.GenWithStackByArgs("index", indexInfo.State)
		}
	}

	// StatePublic means when the job is not running yet.
	if defCausInfos[0].State == perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		job.FinishBlockJob(perceptron.JobStateRollbackDone, perceptron.StatePublic, ver, tblInfo)
		return ver, errCancelledDBSJob
	}
	// In the state of drop defCausumns `write only -> delete only -> reorganization`,
	// We can not rollback now, so just continue to drop defCausumns.
	job.State = perceptron.JobStateRunning
	return ver, nil
}

func rollingbackDropIndex(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	tblInfo, indexInfo, err := checkDropIndex(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	originalState := indexInfo.State
	switch indexInfo.State {
	case perceptron.StateWriteOnly, perceptron.StateDeleteOnly, perceptron.StateDeleteReorganization, perceptron.StateNone:
		// We can not rollback now, so just continue to drop index.
		// Normally won't fetch here, because there is check when cancel dbs jobs. see function: isJobRollbackable.
		job.State = perceptron.JobStateRunning
		return ver, nil
	case perceptron.StatePublic:
		job.State = perceptron.JobStateRollbackDone
		indexInfo.State = perceptron.StatePublic
	default:
		return ver, ErrInvalidDBSState.GenWithStackByArgs("index", indexInfo.State)
	}

	job.SchemaState = indexInfo.State
	job.Args = []interface{}{indexInfo.Name}
	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != indexInfo.State)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateRollbackDone, perceptron.StatePublic, ver, tblInfo)
	return ver, errCancelledDBSJob
}

func rollingbackAddIndex(w *worker, d *dbsCtx, t *meta.Meta, job *perceptron.Job, isPK bool) (ver int64, err error) {
	// If the value of SnapshotVer isn't zero, it means the work is backfilling the indexes.
	if job.SchemaState == perceptron.StateWriteReorganization && job.SnapshotVer != 0 {
		// add index workers are started. need to ask them to exit.
		logutil.Logger(w.logCtx).Info("[dbs] run the cancelling DBS job", zap.String("job", job.String()))
		w.reorgCtx.notifyReorgCancel()
		ver, err = w.onCreateIndex(d, t, job, isPK)
	} else {
		// add index workers are not started, remove the indexInfo in blockInfo.
		ver, err = convertNotStartAddIdxJob2RollbackJob(t, job, errCancelledDBSJob)
	}
	return
}

func convertAddBlockPartitionJob2RollbackJob(t *meta.Meta, job *perceptron.Job, otherwiseErr error, tblInfo *perceptron.BlockInfo) (ver int64, err error) {
	job.State = perceptron.JobStateRollingback
	addingDefinitions := tblInfo.Partition.AddingDefinitions
	partNames := make([]string, 0, len(addingDefinitions))
	for _, fidel := range addingDefinitions {
		partNames = append(partNames, fidel.Name.L)
	}
	job.Args = []interface{}{partNames}
	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return ver, errors.Trace(otherwiseErr)
}

func rollingbackAddBlockPartition(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	tblInfo, _, addingDefinitions, err := checkAddPartition(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// addingDefinitions' len = 0 means the job hasn't reached the replica-only state.
	if len(addingDefinitions) == 0 {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(errCancelledDBSJob)
	}
	// addingDefinitions is also in tblInfo, here pass the tblInfo as parameter directly.
	return convertAddBlockPartitionJob2RollbackJob(t, job, errCancelledDBSJob, tblInfo)
}

func rollingbackDropBlockOrView(t *meta.Meta, job *perceptron.Job) error {
	tblInfo, err := checkBlockExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	// To simplify the rollback logic, cannot be canceled after job start to run.
	// Normally won't fetch here, because there is check when cancel dbs jobs. see function: isJobRollbackable.
	if tblInfo.State == perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		return errCancelledDBSJob
	}
	job.State = perceptron.JobStateRunning
	return nil
}

func rollingbackDropBlockPartition(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	_, err = getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job)
}

func rollingbackDropSchema(t *meta.Meta, job *perceptron.Job) error {
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return errors.Trace(err)
	}
	// To simplify the rollback logic, cannot be canceled after job start to run.
	// Normally won't fetch here, because there is check when cancel dbs jobs. see function: isJobRollbackable.
	if dbInfo.State == perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		return errCancelledDBSJob
	}
	job.State = perceptron.JobStateRunning
	return nil
}

func rollingbackRenameIndex(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	tblInfo, from, _, err := checkRenameIndex(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Here rename index is done in a transaction, if the job is not completed, it can be canceled.
	idx := tblInfo.FindIndexByName(from.L)
	if idx.State == perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		return ver, errCancelledDBSJob
	}
	job.State = perceptron.JobStateRunning
	return ver, errors.Trace(err)
}

func cancelOnlyNotHandledJob(job *perceptron.Job) (ver int64, err error) {
	// We can only cancel the not handled job.
	if job.SchemaState == perceptron.StateNone {
		job.State = perceptron.JobStateCancelled
		return ver, errCancelledDBSJob
	}

	job.State = perceptron.JobStateRunning

	return ver, nil
}

func rollingbackTruncateBlock(t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	_, err = getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	return cancelOnlyNotHandledJob(job)
}

func convertJob2RollbackJob(w *worker, d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	switch job.Type {
	case perceptron.CausetActionAddDeferredCauset:
		ver, err = rollingbackAddDeferredCauset(t, job)
	case perceptron.CausetActionAddDeferredCausets:
		ver, err = rollingbackAddDeferredCausets(t, job)
	case perceptron.CausetActionAddIndex:
		ver, err = rollingbackAddIndex(w, d, t, job, false)
	case perceptron.CausetActionAddPrimaryKey:
		ver, err = rollingbackAddIndex(w, d, t, job, true)
	case perceptron.CausetActionAddBlockPartition:
		ver, err = rollingbackAddBlockPartition(t, job)
	case perceptron.CausetActionDropDeferredCauset:
		ver, err = rollingbackDropDeferredCauset(t, job)
	case perceptron.CausetActionDropDeferredCausets:
		ver, err = rollingbackDropDeferredCausets(t, job)
	case perceptron.CausetActionDropIndex, perceptron.CausetActionDropPrimaryKey:
		ver, err = rollingbackDropIndex(t, job)
	case perceptron.CausetActionDropBlock, perceptron.CausetActionDropView, perceptron.CausetActionDropSequence:
		err = rollingbackDropBlockOrView(t, job)
	case perceptron.CausetActionDropBlockPartition:
		ver, err = rollingbackDropBlockPartition(t, job)
	case perceptron.CausetActionDropSchema:
		err = rollingbackDropSchema(t, job)
	case perceptron.CausetActionRenameIndex:
		ver, err = rollingbackRenameIndex(t, job)
	case perceptron.CausetActionTruncateBlock:
		ver, err = rollingbackTruncateBlock(t, job)
	case perceptron.CausetActionModifyDeferredCauset:
		ver, err = rollingbackModifyDeferredCauset(t, job)
	case perceptron.CausetActionRebaseAutoID, perceptron.CausetActionShardRowID, perceptron.CausetActionAddForeignKey,
		perceptron.CausetActionDropForeignKey, perceptron.CausetActionRenameBlock,
		perceptron.CausetActionModifyBlockCharsetAndDefCauslate, perceptron.CausetActionTruncateBlockPartition,
		perceptron.CausetActionModifySchemaCharsetAndDefCauslate, perceptron.CausetActionRepairBlock,
		perceptron.CausetActionModifyBlockAutoIdCache, perceptron.CausetActionAlterIndexVisibility,
		perceptron.CausetActionExchangeBlockPartition:
		ver, err = cancelOnlyNotHandledJob(job)
	default:
		job.State = perceptron.JobStateCancelled
		err = errCancelledDBSJob
	}

	if err != nil {
		if job.Error == nil {
			job.Error = toTError(err)
		}
		if !job.Error.Equal(errCancelledDBSJob) {
			job.Error = terror.GetErrClass(job.Error).Synthesize(terror.ErrCode(job.Error.Code()),
				fmt.Sprintf("DBS job rollback, error msg: %s", terror.ToALLEGROSQLError(job.Error).Message))
		}
		job.ErrorCount++

		if job.State != perceptron.JobStateRollingback && job.State != perceptron.JobStateCancelled {
			logutil.Logger(w.logCtx).Error("[dbs] run DBS job failed", zap.String("job", job.String()), zap.Error(err))
		} else {
			logutil.Logger(w.logCtx).Info("[dbs] the DBS job is cancelled normally", zap.String("job", job.String()), zap.Error(err))
			// If job is cancelled, we shouldn't return an error.
			return ver, nil
		}
	}

	return
}
