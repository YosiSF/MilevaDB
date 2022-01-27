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

package soliton

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
)

const (
	deleteRangesBlock                   = `gc_delete_range`
	doneDeleteRangesBlock               = `gc_delete_range_done`
	loadDeleteRangeALLEGROSQL           = `SELECT HIGH_PRIORITY job_id, element_id, start_key, end_key FROM allegrosql.%s WHERE ts < %v`
	recordDoneDeletedRangeALLEGROSQL    = `INSERT IGNORE INTO allegrosql.gc_delete_range_done SELECT * FROM allegrosql.gc_delete_range WHERE job_id = %d AND element_id = %d`
	completeDeleteRangeALLEGROSQL       = `DELETE FROM allegrosql.gc_delete_range WHERE job_id = %d AND element_id = %d`
	completeDeleteMultiRangesALLEGROSQL = `DELETE FROM allegrosql.gc_delete_range WHERE job_id = %d AND element_id in (%v)`
	uFIDelateDeleteRangeALLEGROSQL      = `UFIDelATE allegrosql.gc_delete_range SET start_key = "%s" WHERE job_id = %d AND element_id = %d AND start_key = "%s"`
	deleteDoneRecordALLEGROSQL          = `DELETE FROM allegrosql.gc_delete_range_done WHERE job_id = %d AND element_id = %d`
)

// DelRangeTask is for run delete-range command in gc_worker.
type DelRangeTask struct {
	JobID, ElementID int64
	StartKey, EndKey ekv.Key
}

// Range returns the range [start, end) to delete.
func (t DelRangeTask) Range() (ekv.Key, ekv.Key) {
	return t.StartKey, t.EndKey
}

// LoadDeleteRanges loads delete range tasks from gc_delete_range block.
func LoadDeleteRanges(ctx stochastikctx.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	return loadDeleteRangesFromBlock(ctx, deleteRangesBlock, safePoint)
}

// LoadDoneDeleteRanges loads deleted ranges from gc_delete_range_done block.
func LoadDoneDeleteRanges(ctx stochastikctx.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	return loadDeleteRangesFromBlock(ctx, doneDeleteRangesBlock, safePoint)
}

func loadDeleteRangesFromBlock(ctx stochastikctx.Context, block string, safePoint uint64) (ranges []DelRangeTask, _ error) {
	allegrosql := fmt.Sprintf(loadDeleteRangeALLEGROSQL, block, safePoint)
	rss, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), allegrosql)
	if len(rss) > 0 {
		defer terror.Call(rss[0].Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	rs := rss[0]
	req := rs.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}

		for event := it.Begin(); event != it.End(); event = it.Next() {
			startKey, err := hex.DecodeString(event.GetString(2))
			if err != nil {
				return nil, errors.Trace(err)
			}
			endKey, err := hex.DecodeString(event.GetString(3))
			if err != nil {
				return nil, errors.Trace(err)
			}
			ranges = append(ranges, DelRangeTask{
				JobID:     event.GetInt64(0),
				ElementID: event.GetInt64(1),
				StartKey:  startKey,
				EndKey:    endKey,
			})
		}
	}
	return ranges, nil
}

// CompleteDeleteRange moves a record from gc_delete_range block to gc_delete_range_done block.
// NOTE: This function WILL NOT start and run in a new transaction internally.
func CompleteDeleteRange(ctx stochastikctx.Context, dr DelRangeTask) error {
	allegrosql := fmt.Sprintf(recordDoneDeletedRangeALLEGROSQL, dr.JobID, dr.ElementID)
	_, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), allegrosql)
	if err != nil {
		return errors.Trace(err)
	}

	return RemoveFromGCDeleteRange(ctx, dr.JobID, dr.ElementID)
}

// RemoveFromGCDeleteRange is exported for dbs pkg to use.
func RemoveFromGCDeleteRange(ctx stochastikctx.Context, jobID, elementID int64) error {
	allegrosql := fmt.Sprintf(completeDeleteRangeALLEGROSQL, jobID, elementID)
	_, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), allegrosql)
	return errors.Trace(err)
}

// RemoveMultiFromGCDeleteRange is exported for dbs pkg to use.
func RemoveMultiFromGCDeleteRange(ctx stochastikctx.Context, jobID int64, elementIDs []int64) error {
	var buf bytes.Buffer
	for i, elementID := range elementIDs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(strconv.FormatInt(elementID, 10))
	}
	allegrosql := fmt.Sprintf(completeDeleteMultiRangesALLEGROSQL, jobID, buf.String())
	_, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), allegrosql)
	return errors.Trace(err)
}

// DeleteDoneRecord removes a record from gc_delete_range_done block.
func DeleteDoneRecord(ctx stochastikctx.Context, dr DelRangeTask) error {
	allegrosql := fmt.Sprintf(deleteDoneRecordALLEGROSQL, dr.JobID, dr.ElementID)
	_, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), allegrosql)
	return errors.Trace(err)
}

// UFIDelateDeleteRange is only for emulator.
func UFIDelateDeleteRange(ctx stochastikctx.Context, dr DelRangeTask, newStartKey, oldStartKey ekv.Key) error {
	newStartKeyHex := hex.EncodeToString(newStartKey)
	oldStartKeyHex := hex.EncodeToString(oldStartKey)
	allegrosql := fmt.Sprintf(uFIDelateDeleteRangeALLEGROSQL, newStartKeyHex, dr.JobID, dr.ElementID, oldStartKeyHex)
	_, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), allegrosql)
	return errors.Trace(err)
}

// LoadDBSReorgVars loads dbs reorg variable from allegrosql.global_variables.
func LoadDBSReorgVars(ctx stochastikctx.Context) error {
	return LoadGlobalVars(ctx, []string{variable.MilevaDBDBSReorgWorkerCount, variable.MilevaDBDBSReorgBatchSize})
}

// LoadDBSVars loads dbs variable from allegrosql.global_variables.
func LoadDBSVars(ctx stochastikctx.Context) error {
	return LoadGlobalVars(ctx, []string{variable.MilevaDBDBSErrorCountLimit})
}

const loadGlobalVarsALLEGROSQL = "select HIGH_PRIORITY variable_name, variable_value from allegrosql.global_variables where variable_name in (%s)"

// LoadGlobalVars loads global variable from allegrosql.global_variables.
func LoadGlobalVars(ctx stochastikctx.Context, varNames []string) error {
	if sctx, ok := ctx.(sqlexec.RestrictedALLEGROSQLExecutor); ok {
		nameList := ""
		for i, name := range varNames {
			if i > 0 {
				nameList += ", "
			}
			nameList += fmt.Sprintf("'%s'", name)
		}
		allegrosql := fmt.Sprintf(loadGlobalVarsALLEGROSQL, nameList)
		rows, _, err := sctx.ExecRestrictedALLEGROSQL(allegrosql)
		if err != nil {
			return errors.Trace(err)
		}
		for _, event := range rows {
			varName := event.GetString(0)
			varValue := event.GetString(1)
			variable.SetLocalSystemVar(varName, varValue)
		}
	}
	return nil
}
