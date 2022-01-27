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
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	field_types "github.com/whtcorpsinc/berolinaAllegroSQL/types"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/dbs/soliton"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/gcutil"
)

const tiflashCheckMilevaDBHTTPAPIHalfInterval = 2500 * time.Millisecond

func onCreateBlock(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("mock do job error"))
		}
	})

	schemaID := job.SchemaID
	tbInfo := &perceptron.BlockInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tbInfo.State = perceptron.StateNone
	err := checkBlockNotExists(d, t, schemaID, tbInfo.Name.L)
	if err != nil {
		if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockExists.Equal(err) {
			job.State = perceptron.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	ver, err = uFIDelateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch tbInfo.State {
	case perceptron.StateNone:
		// none -> public
		tbInfo.State = perceptron.StatePublic
		tbInfo.UFIDelateTS = t.StartTS
		err = createBlockOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		failpoint.Inject("checkOwnerCheckAllVersionsWaitTime", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(ver, errors.New("mock create block error"))
			}
		})

		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tbInfo)
		asyncNotifyEvent(d, &soliton.Event{Tp: perceptron.CausetActionCreateBlock, BlockInfo: tbInfo})
		return ver, nil
	default:
		return ver, ErrInvalidDBSState.GenWithStackByArgs("block", tbInfo.State)
	}
}

func createBlockOrViewWithCheck(t *meta.Meta, job *perceptron.Job, schemaID int64, tbInfo *perceptron.BlockInfo) error {
	err := checkBlockInfoValid(tbInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return errors.Trace(err)
	}
	return t.CreateBlockOrView(schemaID, tbInfo)
}

func repairBlockOrViewWithCheck(t *meta.Meta, job *perceptron.Job, schemaID int64, tbInfo *perceptron.BlockInfo) error {
	err := checkBlockInfoValid(tbInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return errors.Trace(err)
	}
	return t.UFIDelateBlock(schemaID, tbInfo)
}

func onCreateView(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tbInfo := &perceptron.BlockInfo{}
	var orReplace bool
	var oldTbInfoID int64
	if err := job.DecodeArgs(tbInfo, &orReplace, &oldTbInfoID); err != nil {
		// Invalid arguments, cancel this job.
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tbInfo.State = perceptron.StateNone
	err := checkBlockNotExists(d, t, schemaID, tbInfo.Name.L)
	if err != nil {
		if schemareplicant.ErrDatabaseNotExists.Equal(err) {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		} else if schemareplicant.ErrBlockExists.Equal(err) {
			if !orReplace {
				job.State = perceptron.JobStateCancelled
				return ver, errors.Trace(err)
			}
		} else {
			return ver, errors.Trace(err)
		}
	}
	ver, err = uFIDelateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch tbInfo.State {
	case perceptron.StateNone:
		// none -> public
		tbInfo.State = perceptron.StatePublic
		tbInfo.UFIDelateTS = t.StartTS
		if oldTbInfoID > 0 && orReplace {
			err = t.DropBlockOrView(schemaID, oldTbInfoID, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		err = createBlockOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tbInfo)
		asyncNotifyEvent(d, &soliton.Event{Tp: perceptron.CausetActionCreateView, BlockInfo: tbInfo})
		return ver, nil
	default:
		return ver, ErrInvalidDBSState.GenWithStackByArgs("block", tbInfo.State)
	}
}

func onDropBlockOrView(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	tblInfo, err := checkBlockExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	originalState := job.SchemaState
	switch tblInfo.State {
	case perceptron.StatePublic:
		// public -> write only
		job.SchemaState = perceptron.StateWriteOnly
		tblInfo.State = perceptron.StateWriteOnly
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != tblInfo.State)
	case perceptron.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = perceptron.StateDeleteOnly
		tblInfo.State = perceptron.StateDeleteOnly
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != tblInfo.State)
	case perceptron.StateDeleteOnly:
		tblInfo.State = perceptron.StateNone
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if tblInfo.IsSequence() {
			if err = t.DropSequence(job.SchemaID, job.BlockID, true); err != nil {
				break
			}
		} else {
			if err = t.DropBlockOrView(job.SchemaID, job.BlockID, true); err != nil {
				break
			}
		}
		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StateNone, ver, tblInfo)
		startKey := blockcodec.EncodeBlockPrefix(job.BlockID)
		job.Args = append(job.Args, startKey, getPartitionIDs(tblInfo))
	default:
		err = ErrInvalidDBSState.GenWithStackByArgs("block", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

const (
	recoverBlockCheckFlagNone int64 = iota
	recoverBlockCheckFlagEnableGC
	recoverBlockCheckFlagDisableGC
)

func (w *worker) onRecoverBlock(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	schemaID := job.SchemaID
	tblInfo := &perceptron.BlockInfo{}
	var autoIncID, autoRandID, dropJobID, recoverBlockCheckFlag int64
	var snapshotTS uint64
	const checkFlagIndexInJobArgs = 4 // The index of `recoverBlockCheckFlag` in job arg list.
	if err = job.DecodeArgs(tblInfo, &autoIncID, &dropJobID, &snapshotTS, &recoverBlockCheckFlag, &autoRandID); err != nil {
		// Invalid arguments, cancel this job.
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// check GC and safe point
	gcEnable, err := checkGCEnable(w)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = checkBlockNotExists(d, t, schemaID, tblInfo.Name.L)
	if err != nil {
		if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockExists.Equal(err) {
			job.State = perceptron.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	err = checkBlockIDNotExists(t, schemaID, tblInfo.ID)
	if err != nil {
		if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockExists.Equal(err) {
			job.State = perceptron.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	// Recover block divide into 2 steps:
	// 1. Check GC enable status, to decided whether enable GC after recover block.
	//     a. Why not disable GC before put the job to DBS job queue?
	//        Think about concurrency problem. If a recover job-1 is doing and already disabled GC,
	//        then, another recover block job-2 check GC enable will get disable before into the job queue.
	//        then, after recover block job-2 finished, the GC will be disabled.
	//     b. Why split into 2 steps? 1 step also can finish this job: check GC -> disable GC -> recover block -> finish job.
	//        What if the transaction commit failed? then, the job will retry, but the GC already disabled when first running.
	//        So, after this job retry succeed, the GC will be disabled.
	// 2. Do recover block job.
	//     a. Check whether GC enabled, if enabled, disable GC first.
	//     b. Check GC safe point. If drop block time if after safe point time, then can do recover.
	//        otherwise, can't recover block, because the records of the block may already delete by gc.
	//     c. Remove GC task of the block from gc_delete_range block.
	//     d. Create block and rebase block auto ID.
	//     e. Finish.
	switch tblInfo.State {
	case perceptron.StateNone:
		// none -> write only
		// check GC enable and uFIDelate flag.
		if gcEnable {
			job.Args[checkFlagIndexInJobArgs] = recoverBlockCheckFlagEnableGC
		} else {
			job.Args[checkFlagIndexInJobArgs] = recoverBlockCheckFlagDisableGC
		}

		job.SchemaState = perceptron.StateWriteOnly
		tblInfo.State = perceptron.StateWriteOnly
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, false)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case perceptron.StateWriteOnly:
		// write only -> public
		// do recover block.
		if gcEnable {
			err = disableGC(w)
			if err != nil {
				job.State = perceptron.JobStateCancelled
				return ver, errors.Errorf("disable gc failed, try again later. err: %v", err)
			}
		}
		// check GC safe point
		err = checkSafePoint(w, snapshotTS)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// Remove dropped block DBS job from gc_delete_range block.
		var tids []int64
		if tblInfo.GetPartitionInfo() != nil {
			tids = getPartitionIDs(tblInfo)
		} else {
			tids = []int64{tblInfo.ID}
		}
		err = w.delRangeManager.removeFromGCDeleteRange(dropJobID, tids)
		if err != nil {
			return ver, errors.Trace(err)
		}

		tblInfo.State = perceptron.StatePublic
		tblInfo.UFIDelateTS = t.StartTS
		err = t.CreateBlockAndSetAutoID(schemaID, tblInfo, autoIncID, autoRandID)
		if err != nil {
			return ver, errors.Trace(err)
		}

		failpoint.Inject("mockRecoverBlockCommitErr", func(val failpoint.Value) {
			if val.(bool) && atomic.CompareAndSwapUint32(&mockRecoverBlockCommitErrOnce, 0, 1) {
				ekv.MockCommitErrorEnable()
			}
		})

		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	default:
		return ver, ErrInvalidDBSState.GenWithStackByArgs("block", tblInfo.State)
	}
	return ver, nil
}

// mockRecoverBlockCommitErrOnce uses to make sure
// `mockRecoverBlockCommitErr` only mock error once.
var mockRecoverBlockCommitErrOnce uint32

func enableGC(w *worker) error {
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	return gcutil.EnableGC(ctx)
}

func disableGC(w *worker) error {
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	return gcutil.DisableGC(ctx)
}

func checkGCEnable(w *worker) (enable bool, err error) {
	ctx, err := w.sessPool.get()
	if err != nil {
		return false, errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	return gcutil.CheckGCEnable(ctx)
}

func checkSafePoint(w *worker, snapshotTS uint64) error {
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	return gcutil.ValidateSnapshot(ctx, snapshotTS)
}

func getBlock(causetstore ekv.CausetStorage, schemaID int64, tblInfo *perceptron.BlockInfo) (block.Block, error) {
	allocs := autoid.NewSlabPredictorsFromTblInfo(causetstore, schemaID, tblInfo)
	tbl, err := block.BlockFromMeta(allocs, tblInfo)
	return tbl, errors.Trace(err)
}

func getBlockInfoAndCancelFaultJob(t *meta.Meta, job *perceptron.Job, schemaID int64) (*perceptron.BlockInfo, error) {
	tblInfo, err := checkBlockExistAndCancelNonExistJob(t, job, schemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if tblInfo.State != perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		return nil, ErrInvalidDBSState.GenWithStack("block %s is not in public, but %s", tblInfo.Name, tblInfo.State)
	}

	return tblInfo, nil
}

func checkBlockExistAndCancelNonExistJob(t *meta.Meta, job *perceptron.Job, schemaID int64) (*perceptron.BlockInfo, error) {
	tblInfo, err := getBlockInfo(t, job.BlockID, schemaID)
	if err == nil {
		return tblInfo, nil
	}
	if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockNotExists.Equal(err) {
		job.State = perceptron.JobStateCancelled
	}
	return nil, err
}

func getBlockInfo(t *meta.Meta, blockID, schemaID int64) (*perceptron.BlockInfo, error) {
	// Check this block's database.
	tblInfo, err := t.GetBlock(schemaID, blockID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return nil, errors.Trace(schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", schemaID),
			))
		}
		return nil, errors.Trace(err)
	}

	// Check the block.
	if tblInfo == nil {
		return nil, errors.Trace(schemareplicant.ErrBlockNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
			fmt.Sprintf("(Block ID %d)", blockID),
		))
	}
	return tblInfo, nil
}

// onTruncateBlock delete old block meta, and creates a new block identical to old block except for block ID.
// As all the old data is encoded with old block ID, it can not be accessed any more.
// A background job will be created to delete old data.
func onTruncateBlock(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	blockID := job.BlockID
	var newBlockID int64
	err := job.DecodeArgs(&newBlockID)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.IsView() || tblInfo.IsSequence() {
		job.State = perceptron.JobStateCancelled
		return ver, schemareplicant.ErrBlockNotExists.GenWithStackByArgs(job.SchemaName, tblInfo.Name.O)
	}

	err = t.DropBlockOrView(schemaID, tblInfo.ID, true)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	failpoint.Inject("truncateBlockErr", func(val failpoint.Value) {
		if val.(bool) {
			job.State = perceptron.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after dropping block"))
		}
	})

	var oldPartitionIDs []int64
	if tblInfo.GetPartitionInfo() != nil {
		oldPartitionIDs = getPartitionIDs(tblInfo)
		// We use the new partition ID because all the old data is encoded with the old partition ID, it can not be accessed anymore.
		err = truncateBlockByReassignPartitionIDs(t, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	// Clear the tiflash replica available status.
	if tblInfo.TiFlashReplica != nil {
		tblInfo.TiFlashReplica.AvailablePartitionIDs = nil
		tblInfo.TiFlashReplica.Available = false
	}

	tblInfo.ID = newBlockID
	err = t.CreateBlockOrView(schemaID, tblInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("mockTruncateBlockUFIDelateVersionError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("mock uFIDelate version error"))
		}
	})

	ver, err = uFIDelateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	asyncNotifyEvent(d, &soliton.Event{Tp: perceptron.CausetActionTruncateBlock, BlockInfo: tblInfo})
	startKey := blockcodec.EncodeBlockPrefix(blockID)
	job.Args = []interface{}{startKey, oldPartitionIDs}
	return ver, nil
}

func onRebaseRowIDType(causetstore ekv.CausetStorage, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	return onRebaseAutoID(causetstore, t, job, autoid.RowIDAllocType)
}

func onRebaseAutoRandomType(causetstore ekv.CausetStorage, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	return onRebaseAutoID(causetstore, t, job, autoid.AutoRandomType)
}

func onRebaseAutoID(causetstore ekv.CausetStorage, t *meta.Meta, job *perceptron.Job, tp autoid.SlabPredictorType) (ver int64, _ error) {
	schemaID := job.SchemaID
	var newBase int64
	err := job.DecodeArgs(&newBase)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// No need to check `newBase` again, because `RebaseAutoID` will do this check.
	if tp == autoid.RowIDAllocType {
		tblInfo.AutoIncID = newBase
	} else {
		tblInfo.AutoRandID = newBase
	}

	tbl, err := getBlock(causetstore, schemaID, tblInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if alloc := tbl.SlabPredictors(nil).Get(tp); alloc != nil {
		// The next value to allocate is `newBase`.
		newEnd := newBase - 1
		err = alloc.Rebase(tblInfo.ID, newEnd, false)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}
	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyBlockAutoIDCache(t *meta.Meta, job *perceptron.Job) (int64, error) {
	var cache int64
	if err := job.DecodeArgs(&cache); err != nil {
		job.State = perceptron.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}

	tblInfo.AutoIdCache = cache
	ver, err := uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onShardRowID(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	var shardRowIDBits uint64
	err := job.DecodeArgs(&shardRowIDBits)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if shardRowIDBits < tblInfo.ShardRowIDBits {
		tblInfo.ShardRowIDBits = shardRowIDBits
	} else {
		tbl, err := getBlock(d.causetstore, job.SchemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = verifyNoOverflowShardBits(w.sessPool, tbl, shardRowIDBits)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, err
		}
		tblInfo.ShardRowIDBits = shardRowIDBits
		// MaxShardRowIDBits use to check the overflow of auto ID.
		tblInfo.MaxShardRowIDBits = shardRowIDBits
	}
	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func verifyNoOverflowShardBits(s *stochastikPool, tbl block.Block, shardRowIDBits uint64) error {
	ctx, err := s.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.put(ctx)

	// Check next global max auto ID first.
	autoIncID, err := tbl.SlabPredictors(ctx).Get(autoid.RowIDAllocType).NextGlobalAutoID(tbl.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	if blocks.OverflowShardBits(autoIncID, shardRowIDBits, autoid.RowIDBitLength, true) {
		return autoid.ErrAutoincReadFailed.GenWithStack("shard_row_id_bits %d will cause next global auto ID %v overflow", shardRowIDBits, autoIncID)
	}
	return nil
}

func onRenameBlock(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	var oldSchemaID int64
	var blockName perceptron.CIStr
	if err := job.DecodeArgs(&oldSchemaID, &blockName); err != nil {
		// Invalid arguments, cancel this job.
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, oldSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	newSchemaID := job.SchemaID
	err = checkBlockNotExists(d, t, newSchemaID, blockName.L)
	if err != nil {
		if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockExists.Equal(err) {
			job.State = perceptron.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	var autoBlockID int64
	var autoRandID int64
	shouldDelAutoID := false
	if newSchemaID != oldSchemaID {
		shouldDelAutoID = true
		autoBlockID, err = t.GetAutoBlockID(tblInfo.GetDBID(oldSchemaID), tblInfo.ID)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
		autoRandID, err = t.GetAutoRandomID(tblInfo.GetDBID(oldSchemaID), tblInfo.ID)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// It's compatible with old version.
		// TODO: Remove it.
		tblInfo.OldSchemaID = 0
	}

	err = t.DropBlockOrView(oldSchemaID, tblInfo.ID, shouldDelAutoID)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("renameBlockErr", func(val failpoint.Value) {
		if val.(bool) {
			job.State = perceptron.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after renaming block"))
		}
	})

	tblInfo.Name = blockName
	err = t.CreateBlockOrView(newSchemaID, tblInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// UFIDelate the block's auto-increment ID.
	if newSchemaID != oldSchemaID {
		_, err = t.GenAutoBlockID(newSchemaID, tblInfo.ID, autoBlockID)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
		_, err = t.GenAutoRandomID(newSchemaID, tblInfo.ID, autoRandID)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	ver, err = uFIDelateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyBlockComment(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	var comment string
	if err := job.DecodeArgs(&comment); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo.Comment = comment
	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyBlockCharsetAndDefCauslate(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	var toCharset, toDefCauslate string
	var needsOverwriteDefCauss bool
	if err := job.DecodeArgs(&toCharset, &toDefCauslate, &needsOverwriteDefCauss); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// double check.
	_, err = checkAlterBlockCharset(tblInfo, dbInfo, toCharset, toDefCauslate, needsOverwriteDefCauss)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo.Charset = toCharset
	tblInfo.DefCauslate = toDefCauslate

	if needsOverwriteDefCauss {
		// uFIDelate defCausumn charset.
		for _, defCaus := range tblInfo.DeferredCausets {
			if field_types.HasCharset(&defCaus.FieldType) {
				defCaus.Charset = toCharset
				defCaus.DefCauslate = toDefCauslate
			} else {
				defCaus.Charset = charset.CharsetBin
				defCaus.DefCauslate = charset.CharsetBin
			}
		}
	}

	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onSetBlockFlashReplica(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	var replicaInfo ast.TiFlashReplicaSpec
	if err := job.DecodeArgs(&replicaInfo); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = w.checkTiFlashReplicaCount(replicaInfo.Count)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if replicaInfo.Count > 0 {
		tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
			Count:          replicaInfo.Count,
			LocationLabels: replicaInfo.Labels,
		}
	} else {
		tblInfo.TiFlashReplica = nil
	}

	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) checkTiFlashReplicaCount(replicaCount uint64) error {
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	return checkTiFlashReplicaCount(ctx, replicaCount)
}

func onUFIDelateFlashReplicaStatus(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	var available bool
	var physicalID int64
	if err := job.DecodeArgs(&available, &physicalID); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TiFlashReplica == nil || (tblInfo.ID == physicalID && tblInfo.TiFlashReplica.Available == available) ||
		(tblInfo.ID != physicalID && available == tblInfo.TiFlashReplica.IsPartitionAvailable(physicalID)) {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Errorf("the replica available status of block %s is already uFIDelated", tblInfo.Name.String())
	}

	if tblInfo.ID == physicalID {
		tblInfo.TiFlashReplica.Available = available
	} else if pi := tblInfo.GetPartitionInfo(); pi != nil {
		// Partition replica become available.
		if available {
			allAvailable := true
			for _, p := range pi.Definitions {
				if p.ID == physicalID {
					tblInfo.TiFlashReplica.AvailablePartitionIDs = append(tblInfo.TiFlashReplica.AvailablePartitionIDs, physicalID)
				}
				allAvailable = allAvailable && tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID)
			}
			tblInfo.TiFlashReplica.Available = allAvailable
		} else {
			// Partition replica become unavailable.
			for i, id := range tblInfo.TiFlashReplica.AvailablePartitionIDs {
				if id == physicalID {
					newIDs := tblInfo.TiFlashReplica.AvailablePartitionIDs[:i]
					newIDs = append(newIDs, tblInfo.TiFlashReplica.AvailablePartitionIDs[i+1:]...)
					tblInfo.TiFlashReplica.AvailablePartitionIDs = newIDs
					tblInfo.TiFlashReplica.Available = false
					break
				}
			}
		}
	} else {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Errorf("unknown physical ID %v in block %v", physicalID, tblInfo.Name.O)
	}

	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func checkBlockNotExists(d *dbsCtx, t *meta.Meta, schemaID int64, blockName string) error {
	// d.infoHandle maybe nil in some test.
	if d.infoHandle == nil || !d.infoHandle.IsValid() {
		return checkBlockNotExistsFromStore(t, schemaID, blockName)
	}
	// Try to use memory schemaReplicant info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoHandle.Get()
	if is.SchemaMetaVersion() == currVer {
		return checkBlockNotExistsFromSchemaReplicant(is, schemaID, blockName)
	}

	return checkBlockNotExistsFromStore(t, schemaID, blockName)
}

func checkBlockIDNotExists(t *meta.Meta, schemaID, blockID int64) error {
	tbl, err := t.GetBlock(schemaID, blockID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return errors.Trace(err)
	}
	if tbl != nil {
		return schemareplicant.ErrBlockExists.GenWithStackByArgs(tbl.Name)
	}
	return nil
}

func checkBlockNotExistsFromSchemaReplicant(is schemareplicant.SchemaReplicant, schemaID int64, blockName string) error {
	// Check this block's database.
	schemaReplicant, ok := is.SchemaByID(schemaID)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	if is.BlockExists(schemaReplicant.Name, perceptron.NewCIStr(blockName)) {
		return schemareplicant.ErrBlockExists.GenWithStackByArgs(blockName)
	}
	return nil
}

func checkBlockNotExistsFromStore(t *meta.Meta, schemaID int64, blockName string) error {
	// Check this block's database.
	blocks, err := t.ListBlocks(schemaID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return errors.Trace(err)
	}

	// Check the block.
	for _, tbl := range blocks {
		if tbl.Name.L == blockName {
			return schemareplicant.ErrBlockExists.GenWithStackByArgs(tbl.Name)
		}
	}

	return nil
}

// uFIDelateVersionAndBlockInfoWithCheck checks block info validate and uFIDelates the schemaReplicant version and the block information
func uFIDelateVersionAndBlockInfoWithCheck(t *meta.Meta, job *perceptron.Job, tblInfo *perceptron.BlockInfo, shouldUFIDelateVer bool) (
	ver int64, err error) {
	err = checkBlockInfoValid(tblInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	return uFIDelateVersionAndBlockInfo(t, job, tblInfo, shouldUFIDelateVer)
}

// uFIDelateVersionAndBlockInfo uFIDelates the schemaReplicant version and the block information.
func uFIDelateVersionAndBlockInfo(t *meta.Meta, job *perceptron.Job, tblInfo *perceptron.BlockInfo, shouldUFIDelateVer bool) (
	ver int64, err error) {
	failpoint.Inject("mockUFIDelateVersionAndBlockInfoErr", func(val failpoint.Value) {
		switch val.(int) {
		case 1:
			failpoint.Return(ver, errors.New("mock uFIDelate version and blockInfo error"))
		case 2:
			// We change it cancelled directly here, because we want to get the original error with the job id appended.
			// The job ID will be used to get the job from history queue and we will assert it's args.
			job.State = perceptron.JobStateCancelled
			failpoint.Return(ver, errors.New("mock uFIDelate version and blockInfo error, jobID="+strconv.Itoa(int(job.ID))))
		default:
		}
	})
	if shouldUFIDelateVer {
		ver, err = uFIDelateSchemaVersion(t, job)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	if tblInfo.State == perceptron.StatePublic {
		tblInfo.UFIDelateTS = t.StartTS
	}
	return ver, t.UFIDelateBlock(job.SchemaID, tblInfo)
}

func onRepairBlock(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo := &perceptron.BlockInfo{}

	if err := job.DecodeArgs(tblInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo.State = perceptron.StateNone

	// Check the old EDB and old block exist.
	_, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// When in repair mode, the repaired block in a server is not access to user,
	// the block after repairing will be removed from repair list. Other server left
	// behind alive may need to restart to get the latest schemaReplicant version.
	ver, err = uFIDelateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch tblInfo.State {
	case perceptron.StateNone:
		// none -> public
		tblInfo.State = perceptron.StatePublic
		tblInfo.UFIDelateTS = t.StartTS
		err = repairBlockOrViewWithCheck(t, job, schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &soliton.Event{Tp: perceptron.CausetActionRepairBlock, BlockInfo: tblInfo})
		return ver, nil
	default:
		return ver, ErrInvalidDBSState.GenWithStackByArgs("block", tblInfo.State)
	}
}
