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
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/dbs/soliton"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"go.uber.org/zap"
)

const (
	insertDeleteRangeALLEGROSQLPrefix = `INSERT IGNORE INTO allegrosql.gc_delete_range VALUES `
	insertDeleteRangeALLEGROSQLValue  = `("%d", "%d", "%s", "%s", "%d")`
	insertDeleteRangeALLEGROSQL       = insertDeleteRangeALLEGROSQLPrefix + insertDeleteRangeALLEGROSQLValue

	delBatchSize = 65536
	delBackLog   = 128
)

var (
	// enableEmulatorGC means whether to enable emulator GC. The default is enable.
	// In some unit tests, we want to stop emulator GC, then wen can set enableEmulatorGC to 0.
	emulatorGCEnable = int32(1)
	// batchInsertDeleteRangeSize is the maximum size for each batch insert statement in the delete-range.
	batchInsertDeleteRangeSize = 256
)

type delRangeManager interface {
	// addDelRangeJob add a DBS job into gc_delete_range block.
	addDelRangeJob(job *perceptron.Job) error
	// removeFromGCDeleteRange removes the deleting block job from gc_delete_range block by jobID and blockID.
	// It's use for recover the block that was mistakenly deleted.
	removeFromGCDeleteRange(jobID int64, blockID []int64) error
	start()
	clear()
}

type delRange struct {
	causetstore ekv.CausetStorage
	sessPool    *stochastikPool
	emulatorCh  chan struct{}
	keys        []ekv.Key
	quitCh      chan struct{}

	wait         sync.WaitGroup // wait is only used when storeSupport is false.
	storeSupport bool
}

// newDelRangeManager returns a delRangeManager.
func newDelRangeManager(causetstore ekv.CausetStorage, sessPool *stochastikPool) delRangeManager {
	dr := &delRange{
		causetstore:  causetstore,
		sessPool:     sessPool,
		storeSupport: causetstore.SupportDeleteRange(),
		quitCh:       make(chan struct{}),
	}
	if !dr.storeSupport {
		dr.emulatorCh = make(chan struct{}, delBackLog)
		dr.keys = make([]ekv.Key, 0, delBatchSize)
	}
	return dr
}

// addDelRangeJob implements delRangeManager interface.
func (dr *delRange) addDelRangeJob(job *perceptron.Job) error {
	ctx, err := dr.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer dr.sessPool.put(ctx)

	err = insertJobIntoDeleteRangeBlock(ctx, job)
	if err != nil {
		logutil.BgLogger().Error("[dbs] add job into delete-range block failed", zap.Int64("jobID", job.ID), zap.String("jobType", job.Type.String()), zap.Error(err))
		return errors.Trace(err)
	}
	if !dr.storeSupport {
		dr.emulatorCh <- struct{}{}
	}
	logutil.BgLogger().Info("[dbs] add job into delete-range block", zap.Int64("jobID", job.ID), zap.String("jobType", job.Type.String()))
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (dr *delRange) removeFromGCDeleteRange(jobID int64, blockIDs []int64) error {
	ctx, err := dr.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer dr.sessPool.put(ctx)
	err = soliton.RemoveMultiFromGCDeleteRange(ctx, jobID, blockIDs)
	return errors.Trace(err)
}

// start implements delRangeManager interface.
func (dr *delRange) start() {
	if !dr.storeSupport {
		dr.wait.Add(1)
		go dr.startEmulator()
	}
}

// clear implements delRangeManager interface.
func (dr *delRange) clear() {
	logutil.BgLogger().Info("[dbs] closing delRange")
	close(dr.quitCh)
	dr.wait.Wait()
}

// startEmulator is only used for those storage engines which don't support
// delete-range. The emulator fetches records from gc_delete_range block and
// deletes all keys in each DelRangeTask.
func (dr *delRange) startEmulator() {
	defer dr.wait.Done()
	logutil.BgLogger().Info("[dbs] start delRange emulator")
	for {
		select {
		case <-dr.emulatorCh:
		case <-dr.quitCh:
			return
		}
		if IsEmulatorGCEnable() {
			err := dr.doDelRangeWork()
			terror.Log(errors.Trace(err))
		}
	}
}

// EmulatorGCEnable enables emulator gc. It exports for testing.
func EmulatorGCEnable() {
	atomic.StoreInt32(&emulatorGCEnable, 1)
}

// EmulatorGCDisable disables emulator gc. It exports for testing.
func EmulatorGCDisable() {
	atomic.StoreInt32(&emulatorGCEnable, 0)
}

// IsEmulatorGCEnable indicates whether emulator GC enabled. It exports for testing.
func IsEmulatorGCEnable() bool {
	return atomic.LoadInt32(&emulatorGCEnable) == 1
}

func (dr *delRange) doDelRangeWork() error {
	ctx, err := dr.sessPool.get()
	if err != nil {
		logutil.BgLogger().Error("[dbs] delRange emulator get stochastik failed", zap.Error(err))
		return errors.Trace(err)
	}
	defer dr.sessPool.put(ctx)

	ranges, err := soliton.LoadDeleteRanges(ctx, math.MaxInt64)
	if err != nil {
		logutil.BgLogger().Error("[dbs] delRange emulator load tasks failed", zap.Error(err))
		return errors.Trace(err)
	}

	for _, r := range ranges {
		if err := dr.doTask(ctx, r); err != nil {
			logutil.BgLogger().Error("[dbs] delRange emulator do task failed", zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

func (dr *delRange) doTask(ctx stochastikctx.Context, r soliton.DelRangeTask) error {
	var oldStartKey, newStartKey ekv.Key
	oldStartKey = r.StartKey
	for {
		finish := true
		dr.keys = dr.keys[:0]
		err := ekv.RunInNewTxn(dr.causetstore, false, func(txn ekv.Transaction) error {
			iter, err := txn.Iter(oldStartKey, r.EndKey)
			if err != nil {
				return errors.Trace(err)
			}
			defer iter.Close()

			for i := 0; i < delBatchSize; i++ {
				if !iter.Valid() {
					break
				}
				finish = false
				dr.keys = append(dr.keys, iter.Key().Clone())
				newStartKey = iter.Key().Next()

				if err := iter.Next(); err != nil {
					return errors.Trace(err)
				}
			}

			for _, key := range dr.keys {
				err := txn.Delete(key)
				if err != nil && !ekv.ErrNotExist.Equal(err) {
					return errors.Trace(err)
				}
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		if finish {
			if err := soliton.CompleteDeleteRange(ctx, r); err != nil {
				logutil.BgLogger().Error("[dbs] delRange emulator complete task failed", zap.Error(err))
				return errors.Trace(err)
			}
			logutil.BgLogger().Info("[dbs] delRange emulator complete task", zap.Int64("jobID", r.JobID), zap.Int64("elementID", r.ElementID))
			break
		}
		if err := soliton.UFIDelateDeleteRange(ctx, r, newStartKey, oldStartKey); err != nil {
			logutil.BgLogger().Error("[dbs] delRange emulator uFIDelate task failed", zap.Error(err))
		}
		oldStartKey = newStartKey
	}
	return nil
}

// insertJobIntoDeleteRangeBlock parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range block. The primary key is
// job ID, so we ignore key conflict error.
func insertJobIntoDeleteRangeBlock(ctx stochastikctx.Context, job *perceptron.Job) error {
	now, err := getNowTSO(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	s := ctx.(sqlexec.ALLEGROSQLExecutor)
	switch job.Type {
	case perceptron.CausetActionDropSchema:
		var blockIDs []int64
		if err := job.DecodeArgs(&blockIDs); err != nil {
			return errors.Trace(err)
		}
		for i := 0; i < len(blockIDs); i += batchInsertDeleteRangeSize {
			batchEnd := len(blockIDs)
			if batchEnd > i+batchInsertDeleteRangeSize {
				batchEnd = i + batchInsertDeleteRangeSize
			}
			if err := doBatchInsert(s, job.ID, blockIDs[i:batchEnd], now); err != nil {
				return errors.Trace(err)
			}
		}
	case perceptron.CausetActionDropBlock, perceptron.CausetActionTruncateBlock:
		blockID := job.BlockID
		// The startKey here is for compatibility with previous versions, old version did not endKey so don't have to deal with.
		var startKey ekv.Key
		var physicalBlockIDs []int64
		if err := job.DecodeArgs(&startKey, &physicalBlockIDs); err != nil {
			return errors.Trace(err)
		}
		if len(physicalBlockIDs) > 0 {
			for _, pid := range physicalBlockIDs {
				startKey = blockcodec.EncodeBlockPrefix(pid)
				endKey := blockcodec.EncodeBlockPrefix(pid + 1)
				if err := doInsert(s, job.ID, pid, startKey, endKey, now); err != nil {
					return errors.Trace(err)
				}
			}
			return nil
		}
		startKey = blockcodec.EncodeBlockPrefix(blockID)
		endKey := blockcodec.EncodeBlockPrefix(blockID + 1)
		return doInsert(s, job.ID, blockID, startKey, endKey, now)
	case perceptron.CausetActionDropBlockPartition, perceptron.CausetActionTruncateBlockPartition:
		var physicalBlockIDs []int64
		if err := job.DecodeArgs(&physicalBlockIDs); err != nil {
			return errors.Trace(err)
		}
		for _, physicalBlockID := range physicalBlockIDs {
			startKey := blockcodec.EncodeBlockPrefix(physicalBlockID)
			endKey := blockcodec.EncodeBlockPrefix(physicalBlockID + 1)
			if err := doInsert(s, job.ID, physicalBlockID, startKey, endKey, now); err != nil {
				return errors.Trace(err)
			}
		}
	// CausetActionAddIndex, CausetActionAddPrimaryKey needs do it, because it needs to be rolled back when it's canceled.
	case perceptron.CausetActionAddIndex, perceptron.CausetActionAddPrimaryKey:
		blockID := job.BlockID
		var indexID int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexID, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(partitionIDs) > 0 {
			for _, pid := range partitionIDs {
				startKey := blockcodec.EncodeBlockIndexPrefix(pid, indexID)
				endKey := blockcodec.EncodeBlockIndexPrefix(pid, indexID+1)
				if err := doInsert(s, job.ID, indexID, startKey, endKey, now); err != nil {
					return errors.Trace(err)
				}
			}
		} else {
			startKey := blockcodec.EncodeBlockIndexPrefix(blockID, indexID)
			endKey := blockcodec.EncodeBlockIndexPrefix(blockID, indexID+1)
			return doInsert(s, job.ID, indexID, startKey, endKey, now)
		}
	case perceptron.CausetActionDropIndex, perceptron.CausetActionDropPrimaryKey:
		blockID := job.BlockID
		var indexName interface{}
		var indexID int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexName, &indexID, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(partitionIDs) > 0 {
			for _, pid := range partitionIDs {
				startKey := blockcodec.EncodeBlockIndexPrefix(pid, indexID)
				endKey := blockcodec.EncodeBlockIndexPrefix(pid, indexID+1)
				if err := doInsert(s, job.ID, indexID, startKey, endKey, now); err != nil {
					return errors.Trace(err)
				}
			}
		} else {
			startKey := blockcodec.EncodeBlockIndexPrefix(blockID, indexID)
			endKey := blockcodec.EncodeBlockIndexPrefix(blockID, indexID+1)
			return doInsert(s, job.ID, indexID, startKey, endKey, now)
		}
	case perceptron.CausetActionDropDeferredCauset:
		var defCausName perceptron.CIStr
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&defCausName, &indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(indexIDs) > 0 {
			if len(partitionIDs) > 0 {
				for _, pid := range partitionIDs {
					if err := doBatchDeleteIndiceRange(s, job.ID, pid, indexIDs, now); err != nil {
						return errors.Trace(err)
					}
				}
			} else {
				return doBatchDeleteIndiceRange(s, job.ID, job.BlockID, indexIDs, now)
			}
		}
	case perceptron.CausetActionDropDeferredCausets:
		var defCausNames []perceptron.CIStr
		var ifExists []bool
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&defCausNames, &ifExists, &indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(indexIDs) > 0 {
			if len(partitionIDs) > 0 {
				for _, pid := range partitionIDs {
					if err := doBatchDeleteIndiceRange(s, job.ID, pid, indexIDs, now); err != nil {
						return errors.Trace(err)
					}
				}
			} else {
				return doBatchDeleteIndiceRange(s, job.ID, job.BlockID, indexIDs, now)
			}
		}
	case perceptron.CausetActionModifyDeferredCauset:
		var indexIDs []int64
		var partitionIDs []int64
		if err := job.DecodeArgs(&indexIDs, &partitionIDs); err != nil {
			return errors.Trace(err)
		}
		if len(indexIDs) == 0 {
			return nil
		}
		if len(partitionIDs) == 0 {
			return doBatchDeleteIndiceRange(s, job.ID, job.BlockID, indexIDs, now)
		}
		for _, pid := range partitionIDs {
			if err := doBatchDeleteIndiceRange(s, job.ID, pid, indexIDs, now); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func doBatchDeleteIndiceRange(s sqlexec.ALLEGROSQLExecutor, jobID, blockID int64, indexIDs []int64, ts uint64) error {
	logutil.BgLogger().Info("[dbs] batch insert into delete-range indices", zap.Int64("jobID", jobID), zap.Int64s("elementIDs", indexIDs))
	allegrosql := insertDeleteRangeALLEGROSQLPrefix
	for i, indexID := range indexIDs {
		startKey := blockcodec.EncodeBlockIndexPrefix(blockID, indexID)
		endKey := blockcodec.EncodeBlockIndexPrefix(blockID, indexID+1)
		startKeyEncoded := hex.EncodeToString(startKey)
		endKeyEncoded := hex.EncodeToString(endKey)
		allegrosql += fmt.Sprintf(insertDeleteRangeALLEGROSQLValue, jobID, indexID, startKeyEncoded, endKeyEncoded, ts)
		if i != len(indexIDs)-1 {
			allegrosql += ","
		}
	}
	_, err := s.Execute(context.Background(), allegrosql)
	return errors.Trace(err)
}

func doInsert(s sqlexec.ALLEGROSQLExecutor, jobID int64, elementID int64, startKey, endKey ekv.Key, ts uint64) error {
	logutil.BgLogger().Info("[dbs] insert into delete-range block", zap.Int64("jobID", jobID), zap.Int64("elementID", elementID))
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	allegrosql := fmt.Sprintf(insertDeleteRangeALLEGROSQL, jobID, elementID, startKeyEncoded, endKeyEncoded, ts)
	_, err := s.Execute(context.Background(), allegrosql)
	return errors.Trace(err)
}

func doBatchInsert(s sqlexec.ALLEGROSQLExecutor, jobID int64, blockIDs []int64, ts uint64) error {
	logutil.BgLogger().Info("[dbs] batch insert into delete-range block", zap.Int64("jobID", jobID), zap.Int64s("elementIDs", blockIDs))
	allegrosql := insertDeleteRangeALLEGROSQLPrefix
	for i, blockID := range blockIDs {
		startKey := blockcodec.EncodeBlockPrefix(blockID)
		endKey := blockcodec.EncodeBlockPrefix(blockID + 1)
		startKeyEncoded := hex.EncodeToString(startKey)
		endKeyEncoded := hex.EncodeToString(endKey)
		allegrosql += fmt.Sprintf(insertDeleteRangeALLEGROSQLValue, jobID, blockID, startKeyEncoded, endKeyEncoded, ts)
		if i != len(blockIDs)-1 {
			allegrosql += ","
		}
	}
	_, err := s.Execute(context.Background(), allegrosql)
	return errors.Trace(err)
}

// getNowTS gets the current timestamp, in TSO.
func getNowTSO(ctx stochastikctx.Context) (uint64, error) {
	currVer, err := ctx.GetStore().CurrentVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	return currVer.Ver, nil
}
