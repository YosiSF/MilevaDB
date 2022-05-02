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
	"math"
	"math/bits"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	dbsutil "github.com/whtcorpsinc/MilevaDB-Prod/dbs/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	decoder "github.com/whtcorpsinc/MilevaDB-Prod/soliton/rowDecoder"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/sqlexec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/timeutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"go.uber.org/zap"
)

// adjustDeferredCausetInfoInAddDeferredCauset is used to set the correct position of defCausumn info when adding defCausumn.
// 1. The added defCausumn was append at the end of tblInfo.DeferredCausets, due to dbs state was not public then.
//    It should be moved to the correct position when the dbs state to be changed to public.
// 2. The offset of defCausumn should also to be set to the right value.
func adjustDeferredCausetInfoInAddDeferredCauset(tblInfo *perceptron.BlockInfo, offset int) {
	oldDefCauss := tblInfo.DeferredCausets
	newDefCauss := make([]*perceptron.DeferredCausetInfo, 0, len(oldDefCauss))
	newDefCauss = append(newDefCauss, oldDefCauss[:offset]...)
	newDefCauss = append(newDefCauss, oldDefCauss[len(oldDefCauss)-1])
	newDefCauss = append(newDefCauss, oldDefCauss[offset:len(oldDefCauss)-1]...)
	// Adjust defCausumn offset.
	offsetChanged := make(map[int]int, len(newDefCauss)-offset-1)
	for i := offset + 1; i < len(newDefCauss); i++ {
		offsetChanged[newDefCauss[i].Offset] = i
		newDefCauss[i].Offset = i
	}
	newDefCauss[offset].Offset = offset
	// UFIDelate index defCausumn offset info.
	// TODO: There may be some corner cases for index defCausumn offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		for _, defCaus := range idx.DeferredCausets {
			newOffset, ok := offsetChanged[defCaus.Offset]
			if ok {
				defCaus.Offset = newOffset
			}
		}
	}
	tblInfo.DeferredCausets = newDefCauss
}

// adjustDeferredCausetInfoInDropDeferredCauset is used to set the correct position of defCausumn info when dropping defCausumn.
// 1. The offset of defCausumn should to be set to the last of the defCausumns.
// 2. The dropped defCausumn is moved to the end of tblInfo.DeferredCausets, due to it was not public any more.
func adjustDeferredCausetInfoInDropDeferredCauset(tblInfo *perceptron.BlockInfo, offset int) {
	oldDefCauss := tblInfo.DeferredCausets
	// Adjust defCausumn offset.
	offsetChanged := make(map[int]int, len(oldDefCauss)-offset-1)
	for i := offset + 1; i < len(oldDefCauss); i++ {
		offsetChanged[oldDefCauss[i].Offset] = i - 1
		oldDefCauss[i].Offset = i - 1
	}
	oldDefCauss[offset].Offset = len(oldDefCauss) - 1
	// For expression index, we drop hidden defCausumns and index simultaneously.
	// So we need to change the offset of expression index.
	offsetChanged[offset] = len(oldDefCauss) - 1
	// UFIDelate index defCausumn offset info.
	// TODO: There may be some corner cases for index defCausumn offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		for _, defCaus := range idx.DeferredCausets {
			newOffset, ok := offsetChanged[defCaus.Offset]
			if ok {
				defCaus.Offset = newOffset
			}
		}
	}
	newDefCauss := make([]*perceptron.DeferredCausetInfo, 0, len(oldDefCauss))
	newDefCauss = append(newDefCauss, oldDefCauss[:offset]...)
	newDefCauss = append(newDefCauss, oldDefCauss[offset+1:]...)
	newDefCauss = append(newDefCauss, oldDefCauss[offset])
	tblInfo.DeferredCausets = newDefCauss
}

func createDeferredCausetInfo(tblInfo *perceptron.BlockInfo, defCausInfo *perceptron.DeferredCausetInfo, pos *ast.DeferredCausetPosition) (*perceptron.DeferredCausetInfo, *ast.DeferredCausetPosition, int, error) {
	// Check defCausumn name duplicate.
	defcaus := tblInfo.DeferredCausets
	offset := len(defcaus)
	// Should initialize pos when it is nil.
	if pos == nil {
		pos = &ast.DeferredCausetPosition{}
	}
	// Get defCausumn offset.
	if pos.Tp == ast.DeferredCausetPositionFirst {
		offset = 0
	} else if pos.Tp == ast.DeferredCausetPositionAfter {
		c := perceptron.FindDeferredCausetInfo(defcaus, pos.RelativeDeferredCauset.Name.L)
		if c == nil {
			return nil, pos, 0, schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(pos.RelativeDeferredCauset, tblInfo.Name)
		}

		// Insert offset is after the mentioned defCausumn.
		offset = c.Offset + 1
	}
	defCausInfo.ID = allocateDeferredCausetID(tblInfo)
	defCausInfo.State = perceptron.StateNone
	// To support add defCausumn asynchronous, we should mark its offset as the last defCausumn.
	// So that we can use origin defCausumn offset to get value from event.
	defCausInfo.Offset = len(defcaus)

	// Append the defCausumn info to the end of the tblInfo.DeferredCausets.
	// It will reorder to the right offset in "DeferredCausets" when it state change to public.
	tblInfo.DeferredCausets = append(defcaus, defCausInfo)
	return defCausInfo, pos, offset, nil
}

func checkAddDeferredCauset(t *meta.Meta, job *perceptron.Job) (*perceptron.BlockInfo, *perceptron.DeferredCausetInfo, *perceptron.DeferredCausetInfo, *ast.DeferredCausetPosition, int, error) {
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, nil, 0, errors.Trace(err)
	}
	defCaus := &perceptron.DeferredCausetInfo{}
	pos := &ast.DeferredCausetPosition{}
	offset := 0
	err = job.DecodeArgs(defCaus, pos, &offset)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, nil, nil, 0, errors.Trace(err)
	}

	defCausumnInfo := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, defCaus.Name.L)
	if defCausumnInfo != nil {
		if defCausumnInfo.State == perceptron.StatePublic {
			// We already have a defCausumn with the same defCausumn name.
			job.State = perceptron.JobStateCancelled
			return nil, nil, nil, nil, 0, schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(defCaus.Name)
		}
	}
	return tblInfo, defCausumnInfo, defCaus, pos, offset, nil
}

func onAddDeferredCauset(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropDeferredCauset(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	failpoint.Inject("errorBeforeDecodeArgs", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("occur an error before decode args"))
		}
	})

	tblInfo, defCausumnInfo, defCaus, pos, offset, err := checkAddDeferredCauset(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if defCausumnInfo == nil {
		defCausumnInfo, _, offset, err = createDeferredCausetInfo(tblInfo, defCaus, pos)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
		logutil.BgLogger().Info("[dbs] run add defCausumn job", zap.String("job", job.String()), zap.Reflect("defCausumnInfo", *defCausumnInfo), zap.Int("offset", offset))
		// Set offset arg to job.
		if offset != 0 {
			job.Args = []interface{}{defCausumnInfo, pos, offset}
		}
		if err = checkAddDeferredCausetTooManyDeferredCausets(len(tblInfo.DeferredCausets)); err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	originalState := defCausumnInfo.State
	switch defCausumnInfo.State {
	case perceptron.StateNone:
		// none -> delete only
		job.SchemaState = perceptron.StateDeleteOnly
		defCausumnInfo.State = perceptron.StateDeleteOnly
		ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, originalState != defCausumnInfo.State)
	case perceptron.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = perceptron.StateWriteOnly
		defCausumnInfo.State = perceptron.StateWriteOnly
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausumnInfo.State)
	case perceptron.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = perceptron.StateWriteReorganization
		defCausumnInfo.State = perceptron.StateWriteReorganization
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausumnInfo.State)
	case perceptron.StateWriteReorganization:
		// reorganization -> public
		// Adjust block defCausumn offset.
		adjustDeferredCausetInfoInAddDeferredCauset(tblInfo, offset)
		defCausumnInfo.State = perceptron.StatePublic
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausumnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &dbsutil.Event{Tp: perceptron.CausetActionAddDeferredCauset, BlockInfo: tblInfo, DeferredCausetInfos: []*perceptron.DeferredCausetInfo{defCausumnInfo}})
	default:
		err = ErrInvalidDBSState.GenWithStackByArgs("defCausumn", defCausumnInfo.State)
	}

	return ver, errors.Trace(err)
}

func checkAddDeferredCausets(t *meta.Meta, job *perceptron.Job) (*perceptron.BlockInfo, []*perceptron.DeferredCausetInfo, []*perceptron.DeferredCausetInfo, []*ast.DeferredCausetPosition, []int, []bool, error) {
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, errors.Trace(err)
	}
	defCausumns := []*perceptron.DeferredCausetInfo{}
	positions := []*ast.DeferredCausetPosition{}
	offsets := []int{}
	ifNotExists := []bool{}
	err = job.DecodeArgs(&defCausumns, &positions, &offsets, &ifNotExists)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, nil, nil, nil, nil, errors.Trace(err)
	}

	defCausumnInfos := make([]*perceptron.DeferredCausetInfo, 0, len(defCausumns))
	newDeferredCausets := make([]*perceptron.DeferredCausetInfo, 0, len(defCausumns))
	newPositions := make([]*ast.DeferredCausetPosition, 0, len(defCausumns))
	newOffsets := make([]int, 0, len(defCausumns))
	newIfNotExists := make([]bool, 0, len(defCausumns))
	for i, defCaus := range defCausumns {
		defCausumnInfo := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, defCaus.Name.L)
		if defCausumnInfo != nil {
			if defCausumnInfo.State == perceptron.StatePublic {
				// We already have a defCausumn with the same defCausumn name.
				if ifNotExists[i] {
					// TODO: Should return a warning.
					logutil.BgLogger().Warn("[dbs] check add defCausumns, duplicate defCausumn", zap.Stringer("defCaus", defCaus.Name))
					continue
				}
				job.State = perceptron.JobStateCancelled
				return nil, nil, nil, nil, nil, nil, schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(defCaus.Name)
			}
			defCausumnInfos = append(defCausumnInfos, defCausumnInfo)
		}
		newDeferredCausets = append(newDeferredCausets, defCausumns[i])
		newPositions = append(newPositions, positions[i])
		newOffsets = append(newOffsets, offsets[i])
		newIfNotExists = append(newIfNotExists, ifNotExists[i])
	}
	return tblInfo, defCausumnInfos, newDeferredCausets, newPositions, newOffsets, newIfNotExists, nil
}

func setDeferredCausetsState(defCausumnInfos []*perceptron.DeferredCausetInfo, state perceptron.SchemaState) {
	for i := range defCausumnInfos {
		defCausumnInfos[i].State = state
	}
}

func setIndicesState(indexInfos []*perceptron.IndexInfo, state perceptron.SchemaState) {
	for _, indexInfo := range indexInfos {
		indexInfo.State = state
	}
}

func onAddDeferredCausets(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropDeferredCausets(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	failpoint.Inject("errorBeforeDecodeArgs", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("occur an error before decode args"))
		}
	})

	tblInfo, defCausumnInfos, defCausumns, positions, offsets, ifNotExists, err := checkAddDeferredCausets(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(defCausumnInfos) == 0 {
		if len(defCausumns) == 0 {
			job.State = perceptron.JobStateCancelled
			return ver, nil
		}
		for i := range defCausumns {
			defCausumnInfo, pos, offset, err := createDeferredCausetInfo(tblInfo, defCausumns[i], positions[i])
			if err != nil {
				job.State = perceptron.JobStateCancelled
				return ver, errors.Trace(err)
			}
			logutil.BgLogger().Info("[dbs] run add defCausumns job", zap.String("job", job.String()), zap.Reflect("defCausumnInfo", *defCausumnInfo), zap.Int("offset", offset))
			positions[i] = pos
			offsets[i] = offset
			if err = checkAddDeferredCausetTooManyDeferredCausets(len(tblInfo.DeferredCausets)); err != nil {
				job.State = perceptron.JobStateCancelled
				return ver, errors.Trace(err)
			}
			defCausumnInfos = append(defCausumnInfos, defCausumnInfo)
		}
		// Set arg to job.
		job.Args = []interface{}{defCausumnInfos, positions, offsets, ifNotExists}
	}

	originalState := defCausumnInfos[0].State
	switch defCausumnInfos[0].State {
	case perceptron.StateNone:
		// none -> delete only
		job.SchemaState = perceptron.StateDeleteOnly
		setDeferredCausetsState(defCausumnInfos, perceptron.StateDeleteOnly)
		ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, originalState != defCausumnInfos[0].State)
	case perceptron.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = perceptron.StateWriteOnly
		setDeferredCausetsState(defCausumnInfos, perceptron.StateWriteOnly)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausumnInfos[0].State)
	case perceptron.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = perceptron.StateWriteReorganization
		setDeferredCausetsState(defCausumnInfos, perceptron.StateWriteReorganization)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausumnInfos[0].State)
	case perceptron.StateWriteReorganization:
		// reorganization -> public
		// Adjust block defCausumn offsets.
		oldDefCauss := tblInfo.DeferredCausets[:len(tblInfo.DeferredCausets)-len(offsets)]
		newDefCauss := tblInfo.DeferredCausets[len(tblInfo.DeferredCausets)-len(offsets):]
		tblInfo.DeferredCausets = oldDefCauss
		for i := range offsets {
			// For multiple defCausumns with after position, should adjust offsets.
			// e.g. create block t(a int);
			// alter block t add defCausumn b int after a, add defCausumn c int after a;
			// alter block t add defCausumn a1 int after a, add defCausumn b1 int after b, add defCausumn c1 int after c;
			// alter block t add defCausumn a1 int after a, add defCausumn b1 int first;
			if positions[i].Tp == ast.DeferredCausetPositionAfter {
				for j := 0; j < i; j++ {
					if (positions[j].Tp == ast.DeferredCausetPositionAfter && offsets[j] < offsets[i]) || positions[j].Tp == ast.DeferredCausetPositionFirst {
						offsets[i]++
					}
				}
			}
			tblInfo.DeferredCausets = append(tblInfo.DeferredCausets, newDefCauss[i])
			adjustDeferredCausetInfoInAddDeferredCauset(tblInfo, offsets[i])
		}
		setDeferredCausetsState(defCausumnInfos, perceptron.StatePublic)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausumnInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &dbsutil.Event{Tp: perceptron.CausetActionAddDeferredCausets, BlockInfo: tblInfo, DeferredCausetInfos: defCausumnInfos})
	default:
		err = ErrInvalidDBSState.GenWithStackByArgs("defCausumn", defCausumnInfos[0].State)
	}

	return ver, errors.Trace(err)
}

func onDropDeferredCausets(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	tblInfo, defCausInfos, delCount, idxInfos, err := checkDropDeferredCausets(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(defCausInfos) == 0 {
		job.State = perceptron.JobStateCancelled
		return ver, nil
	}

	originalState := defCausInfos[0].State
	switch defCausInfos[0].State {
	case perceptron.StatePublic:
		// public -> write only
		job.SchemaState = perceptron.StateWriteOnly
		setDeferredCausetsState(defCausInfos, perceptron.StateWriteOnly)
		setIndicesState(idxInfos, perceptron.StateWriteOnly)
		for _, defCausInfo := range defCausInfos {
			err = checkDropDeferredCausetForStatePublic(tblInfo, defCausInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, originalState != defCausInfos[0].State)
	case perceptron.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = perceptron.StateDeleteOnly
		setDeferredCausetsState(defCausInfos, perceptron.StateDeleteOnly)
		setIndicesState(idxInfos, perceptron.StateDeleteOnly)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausInfos[0].State)
	case perceptron.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = perceptron.StateDeleteReorganization
		setDeferredCausetsState(defCausInfos, perceptron.StateDeleteReorganization)
		setIndicesState(idxInfos, perceptron.StateDeleteReorganization)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausInfos[0].State)
	case perceptron.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this defCausumn.
		if len(idxInfos) > 0 {
			newIndices := make([]*perceptron.IndexInfo, 0, len(tblInfo.Indices))
			for _, idx := range tblInfo.Indices {
				if !indexInfoContains(idx.ID, idxInfos) {
					newIndices = append(newIndices, idx)
				}
			}
			tblInfo.Indices = newIndices
		}

		indexIDs := indexInfosToIDList(idxInfos)
		tblInfo.DeferredCausets = tblInfo.DeferredCausets[:len(tblInfo.DeferredCausets)-delCount]
		setDeferredCausetsState(defCausInfos, perceptron.StateNone)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishBlockJob(perceptron.JobStateRollbackDone, perceptron.StateNone, ver, tblInfo)
		} else {
			job.FinishBlockJob(perceptron.JobStateDone, perceptron.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
		}
	default:
		err = errInvalidDBSJob.GenWithStackByArgs("block", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropDeferredCausets(t *meta.Meta, job *perceptron.Job) (*perceptron.BlockInfo, []*perceptron.DeferredCausetInfo, int, []*perceptron.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, 0, nil, errors.Trace(err)
	}

	var defCausNames []perceptron.CIStr
	var ifExists []bool
	err = job.DecodeArgs(&defCausNames, &ifExists)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, 0, nil, errors.Trace(err)
	}

	newDefCausNames := make([]perceptron.CIStr, 0, len(defCausNames))
	defCausInfos := make([]*perceptron.DeferredCausetInfo, 0, len(defCausNames))
	newIfExists := make([]bool, 0, len(defCausNames))
	indexInfos := make([]*perceptron.IndexInfo, 0)
	for i, defCausName := range defCausNames {
		defCausInfo := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, defCausName.L)
		if defCausInfo == nil || defCausInfo.Hidden {
			if ifExists[i] {
				// TODO: Should return a warning.
				logutil.BgLogger().Warn(fmt.Sprintf("defCausumn %s doesn't exist", defCausName))
				continue
			}
			job.State = perceptron.JobStateCancelled
			return nil, nil, 0, nil, ErrCantDropFieldOrKey.GenWithStack("defCausumn %s doesn't exist", defCausName)
		}
		if err = isDroppableDeferredCauset(tblInfo, defCausName); err != nil {
			job.State = perceptron.JobStateCancelled
			return nil, nil, 0, nil, errors.Trace(err)
		}
		newDefCausNames = append(newDefCausNames, defCausName)
		newIfExists = append(newIfExists, ifExists[i])
		defCausInfos = append(defCausInfos, defCausInfo)
		idxInfos := listIndicesWithDeferredCauset(defCausName.L, tblInfo.Indices)
		indexInfos = append(indexInfos, idxInfos...)
	}
	job.Args = []interface{}{newDefCausNames, newIfExists}
	return tblInfo, defCausInfos, len(defCausInfos), indexInfos, nil
}

func checkDropDeferredCausetForStatePublic(tblInfo *perceptron.BlockInfo, defCausInfo *perceptron.DeferredCausetInfo) (err error) {
	// Set this defCausumn's offset to the last and reset all following defCausumns' offsets.
	adjustDeferredCausetInfoInDropDeferredCauset(tblInfo, defCausInfo.Offset)
	// When the dropping defCausumn has not-null flag and it hasn't the default value, we can backfill the defCausumn value like "add defCausumn".
	// NOTE: If the state of StateWriteOnly can be rollbacked, we'd better reconsider the original default value.
	// And we need consider the defCausumn without not-null flag.
	if defCausInfo.OriginDefaultValue == nil && allegrosql.HasNotNullFlag(defCausInfo.Flag) {
		// If the defCausumn is timestamp default current_timestamp, and DBS owner is new version MilevaDB that set defCausumn.Version to 1,
		// then old MilevaDB uFIDelate record in the defCausumn write only stage will uses the wrong default value of the dropping defCausumn.
		// Because new version of the defCausumn default value is UTC time, but old version MilevaDB will think the default value is the time in system timezone.
		// But currently will be ok, because we can't cancel the drop defCausumn job when the job is running,
		// so the defCausumn will be dropped succeed and client will never see the wrong default value of the dropped defCausumn.
		// More info about this problem, see PR#9115.
		defCausInfo.OriginDefaultValue, err = generateOriginDefaultValue(defCausInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func onDropDeferredCauset(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	tblInfo, defCausInfo, idxInfos, err := checkDropDeferredCauset(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	originalState := defCausInfo.State
	switch defCausInfo.State {
	case perceptron.StatePublic:
		// public -> write only
		job.SchemaState = perceptron.StateWriteOnly
		defCausInfo.State = perceptron.StateWriteOnly
		setIndicesState(idxInfos, perceptron.StateWriteOnly)
		err = checkDropDeferredCausetForStatePublic(tblInfo, defCausInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, originalState != defCausInfo.State)
	case perceptron.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = perceptron.StateDeleteOnly
		defCausInfo.State = perceptron.StateDeleteOnly
		setIndicesState(idxInfos, perceptron.StateDeleteOnly)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausInfo.State)
	case perceptron.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = perceptron.StateDeleteReorganization
		defCausInfo.State = perceptron.StateDeleteReorganization
		setIndicesState(idxInfos, perceptron.StateDeleteReorganization)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausInfo.State)
	case perceptron.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this defCausumn.
		if len(idxInfos) > 0 {
			newIndices := make([]*perceptron.IndexInfo, 0, len(tblInfo.Indices))
			for _, idx := range tblInfo.Indices {
				if !indexInfoContains(idx.ID, idxInfos) {
					newIndices = append(newIndices, idx)
				}
			}
			tblInfo.Indices = newIndices
		}

		indexIDs := indexInfosToIDList(idxInfos)
		tblInfo.DeferredCausets = tblInfo.DeferredCausets[:len(tblInfo.DeferredCausets)-1]
		defCausInfo.State = perceptron.StateNone
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != defCausInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishBlockJob(perceptron.JobStateRollbackDone, perceptron.StateNone, ver, tblInfo)
		} else {
			// We should set related index IDs for job
			job.FinishBlockJob(perceptron.JobStateDone, perceptron.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
		}
	default:
		err = errInvalidDBSJob.GenWithStackByArgs("block", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropDeferredCauset(t *meta.Meta, job *perceptron.Job) (*perceptron.BlockInfo, *perceptron.DeferredCausetInfo, []*perceptron.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	var defCausName perceptron.CIStr
	err = job.DecodeArgs(&defCausName)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, nil, errors.Trace(err)
	}

	defCausInfo := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, defCausName.L)
	if defCausInfo == nil || defCausInfo.Hidden {
		job.State = perceptron.JobStateCancelled
		return nil, nil, nil, ErrCantDropFieldOrKey.GenWithStack("defCausumn %s doesn't exist", defCausName)
	}
	if err = isDroppableDeferredCauset(tblInfo, defCausName); err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, nil, errors.Trace(err)
	}
	idxInfos := listIndicesWithDeferredCauset(defCausName.L, tblInfo.Indices)
	if len(idxInfos) > 0 {
		for _, idxInfo := range idxInfos {
			err = checkDropIndexOnAutoIncrementDeferredCauset(tblInfo, idxInfo)
			if err != nil {
				job.State = perceptron.JobStateCancelled
				return nil, nil, nil, err
			}
		}
	}
	return tblInfo, defCausInfo, idxInfos, nil
}

func onSetDefaultValue(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	newDefCaus := &perceptron.DeferredCausetInfo{}
	err := job.DecodeArgs(newDefCaus)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return uFIDelateDeferredCausetDefaultValue(t, job, newDefCaus, &newDefCaus.Name)
}

func needChangeDeferredCausetData(oldDefCaus, newDefCaus *perceptron.DeferredCausetInfo) bool {
	toUnsigned := allegrosql.HasUnsignedFlag(newDefCaus.Flag)
	originUnsigned := allegrosql.HasUnsignedFlag(oldDefCaus.Flag)
	if newDefCaus.Flen > 0 && newDefCaus.Flen < oldDefCaus.Flen || toUnsigned != originUnsigned {
		return true
	}

	return false
}

type modifyDeferredCausetJobParameter struct {
	newDefCaus               *perceptron.DeferredCausetInfo
	oldDefCausName           *perceptron.CIStr
	modifyDeferredCausetTp   byte
	uFIDelatedAutoRandomBits uint64
	changingDefCaus          *perceptron.DeferredCausetInfo
	changingIdxs             []*perceptron.IndexInfo
	pos                      *ast.DeferredCausetPosition
}

func getModifyDeferredCausetInfo(t *meta.Meta, job *perceptron.Job) (*perceptron.DBInfo, *perceptron.BlockInfo, *perceptron.DeferredCausetInfo, *modifyDeferredCausetJobParameter, error) {
	jobParam := &modifyDeferredCausetJobParameter{pos: &ast.DeferredCausetPosition{}}
	err := job.DecodeArgs(&jobParam.newDefCaus, &jobParam.oldDefCausName, jobParam.pos, &jobParam.modifyDeferredCausetTp, &jobParam.uFIDelatedAutoRandomBits, &jobParam.changingDefCaus, &jobParam.changingIdxs)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, nil, jobParam, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return nil, nil, nil, jobParam, errors.Trace(err)
	}

	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return nil, nil, nil, jobParam, errors.Trace(err)
	}

	oldDefCaus := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, jobParam.oldDefCausName.L)
	if oldDefCaus == nil || oldDefCaus.State != perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		return nil, nil, nil, jobParam, errors.Trace(schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(*(jobParam.oldDefCausName), tblInfo.Name))
	}

	return dbInfo, tblInfo, oldDefCaus, jobParam, errors.Trace(err)
}

func (w *worker) onModifyDeferredCauset(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	dbInfo, tblInfo, oldDefCaus, jobParam, err := getModifyDeferredCausetInfo(t, job)
	if err != nil {
		return ver, err
	}

	if job.IsRollingback() {
		// For those defCausumn-type-change jobs which don't reorg the data.
		if !needChangeDeferredCausetData(oldDefCaus, jobParam.newDefCaus) {
			return rollbackModifyDeferredCausetJob(t, tblInfo, job, oldDefCaus, jobParam.modifyDeferredCausetTp)
		}
		// For those defCausumn-type-change jobs which reorg the data.
		return rollbackModifyDeferredCausetJobWithData(t, tblInfo, job, oldDefCaus, jobParam)
	}

	// If we want to rename the defCausumn name, we need to check whether it already exists.
	if jobParam.newDefCaus.Name.L != jobParam.oldDefCausName.L {
		c := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, jobParam.newDefCaus.Name.L)
		if c != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(jobParam.newDefCaus.Name))
		}
	}

	failpoint.Inject("uninitializedOffsetAndState", func(val failpoint.Value) {
		if val.(bool) {
			if jobParam.newDefCaus.State != perceptron.StatePublic {
				failpoint.Return(ver, errors.New("the defCausumn state is wrong"))
			}
		}
	})

	if jobParam.uFIDelatedAutoRandomBits > 0 {
		if err := checkAndApplyNewAutoRandomBits(job, t, tblInfo, jobParam.newDefCaus, jobParam.oldDefCausName, jobParam.uFIDelatedAutoRandomBits); err != nil {
			return ver, errors.Trace(err)
		}
	}

	if !needChangeDeferredCausetData(oldDefCaus, jobParam.newDefCaus) {
		return w.doModifyDeferredCauset(t, job, dbInfo, tblInfo, jobParam.newDefCaus, oldDefCaus, jobParam.pos)
	}

	if jobParam.changingDefCaus == nil {
		changingDefCausPos := &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}
		newDefCausName := perceptron.NewCIStr(fmt.Sprintf("%s%s", changingDeferredCausetPrefix, oldDefCaus.Name.O))
		if allegrosql.HasPriKeyFlag(oldDefCaus.Flag) {
			job.State = perceptron.JobStateCancelled
			msg := "milevadb_enable_change_defCausumn_type is true and this defCausumn has primary key flag"
			return ver, errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
		}
		// TODO: Check whether we need to check OriginDefaultValue.
		jobParam.changingDefCaus = jobParam.newDefCaus.Clone()
		jobParam.changingDefCaus.Name = newDefCausName
		jobParam.changingDefCaus.ChangeStateInfo = &perceptron.ChangeStateInfo{DependencyDeferredCausetOffset: oldDefCaus.Offset}
		_, _, _, err := createDeferredCausetInfo(tblInfo, jobParam.changingDefCaus, changingDefCausPos)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}

		idxInfos, offsets := findIndexesByDefCausName(tblInfo.Indices, oldDefCaus.Name.L)
		jobParam.changingIdxs = make([]*perceptron.IndexInfo, 0, len(idxInfos))
		for i, idxInfo := range idxInfos {
			newIdxInfo := idxInfo.Clone()
			newIdxInfo.Name = perceptron.NewCIStr(fmt.Sprintf("%s%s", changingIndexPrefix, newIdxInfo.Name.O))
			newIdxInfo.ID = allocateIndexID(tblInfo)
			newIdxInfo.DeferredCausets[offsets[i]].Name = newDefCausName
			newIdxInfo.DeferredCausets[offsets[i]].Offset = jobParam.changingDefCaus.Offset
			jobParam.changingIdxs = append(jobParam.changingIdxs, newIdxInfo)
		}
		tblInfo.Indices = append(tblInfo.Indices, jobParam.changingIdxs...)
	} else {
		tblInfo.DeferredCausets[len(tblInfo.DeferredCausets)-1] = jobParam.changingDefCaus
		INTERLOCKy(tblInfo.Indices[len(tblInfo.Indices)-len(jobParam.changingIdxs):], jobParam.changingIdxs)
	}

	return w.doModifyDeferredCausetTypeWithData(d, t, job, dbInfo, tblInfo, jobParam.changingDefCaus, oldDefCaus, jobParam.newDefCaus.Name, jobParam.pos, jobParam.changingIdxs)
}

// rollbackModifyDeferredCausetJobWithData is used to rollback modify-defCausumn job which need to reorg the data.
func rollbackModifyDeferredCausetJobWithData(t *meta.Meta, tblInfo *perceptron.BlockInfo, job *perceptron.Job, oldDefCaus *perceptron.DeferredCausetInfo, jobParam *modifyDeferredCausetJobParameter) (ver int64, err error) {
	// If the not-null change is included, we should clean the flag info in oldDefCaus.
	if jobParam.modifyDeferredCausetTp == allegrosql.TypeNull {
		// Reset NotNullFlag flag.
		tblInfo.DeferredCausets[oldDefCaus.Offset].Flag = oldDefCaus.Flag &^ allegrosql.NotNullFlag
		// Reset PreventNullInsertFlag flag.
		tblInfo.DeferredCausets[oldDefCaus.Offset].Flag = oldDefCaus.Flag &^ allegrosql.PreventNullInsertFlag
	}
	if jobParam.changingDefCaus != nil {
		// changingDefCaus isn't nil means the job has been in the mid state. These appended changingDefCaus and changingIndex should
		// be removed from the blockInfo as well.
		tblInfo.DeferredCausets = tblInfo.DeferredCausets[:len(tblInfo.DeferredCausets)-1]
		tblInfo.Indices = tblInfo.Indices[:len(tblInfo.Indices)-len(jobParam.changingIdxs)]
	}
	ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateRollbackDone, perceptron.StateNone, ver, tblInfo)
	// Refactor the job args to add the abandoned temporary index ids into delete range block.
	idxIDs := make([]int64, 0, len(jobParam.changingIdxs))
	for _, idx := range jobParam.changingIdxs {
		idxIDs = append(idxIDs, idx.ID)
	}
	job.Args = []interface{}{idxIDs, getPartitionIDs(tblInfo)}
	return ver, nil
}

func (w *worker) doModifyDeferredCausetTypeWithData(
	d *dbsCtx, t *meta.Meta, job *perceptron.Job,
	dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, changingDefCaus, oldDefCaus *perceptron.DeferredCausetInfo,
	defCausName perceptron.CIStr, pos *ast.DeferredCausetPosition, changingIdxs []*perceptron.IndexInfo) (ver int64, _ error) {
	var err error
	originalState := changingDefCaus.State
	switch changingDefCaus.State {
	case perceptron.StateNone:
		// DeferredCauset from null to not null.
		if !allegrosql.HasNotNullFlag(oldDefCaus.Flag) && allegrosql.HasNotNullFlag(changingDefCaus.Flag) {
			// Introduce the `allegrosql.PreventNullInsertFlag` flag to prevent users from inserting or uFIDelating null values.
			err := modifyDefCaussFromNull2NotNull(w, dbInfo, tblInfo, []*perceptron.DeferredCausetInfo{oldDefCaus}, oldDefCaus.Name, oldDefCaus.Tp != changingDefCaus.Tp)
			if err != nil {
				if ErrWarnDataTruncated.Equal(err) || errInvalidUseOfNull.Equal(err) {
					job.State = perceptron.JobStateRollingback
				}
				return ver, err
			}
		}
		// none -> delete only
		uFIDelateChangingInfo(changingDefCaus, changingIdxs, perceptron.StateDeleteOnly)
		failpoint.Inject("mockInsertValueAfterCheckNull", func(val failpoint.Value) {
			if valStr, ok := val.(string); ok {
				var ctx stochastikctx.Context
				ctx, err := w.sessPool.get()
				if err != nil {
					failpoint.Return(ver, err)
				}
				defer w.sessPool.put(ctx)

				_, _, err = ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(valStr)
				if err != nil {
					job.State = perceptron.JobStateCancelled
					failpoint.Return(ver, err)
				}
			}
		})
		ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, originalState != changingDefCaus.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Make sure job args change after `uFIDelateVersionAndBlockInfoWithCheck`, otherwise, the job args will
		// be uFIDelated in `uFIDelateDBSJob` even if it meets an error in `uFIDelateVersionAndBlockInfoWithCheck`.
		job.SchemaState = perceptron.StateDeleteOnly
		job.Args = append(job.Args, changingDefCaus, changingIdxs)
	case perceptron.StateDeleteOnly:
		// DeferredCauset from null to not null.
		if !allegrosql.HasNotNullFlag(oldDefCaus.Flag) && allegrosql.HasNotNullFlag(changingDefCaus.Flag) {
			// Introduce the `allegrosql.PreventNullInsertFlag` flag to prevent users from inserting or uFIDelating null values.
			err := modifyDefCaussFromNull2NotNull(w, dbInfo, tblInfo, []*perceptron.DeferredCausetInfo{oldDefCaus}, oldDefCaus.Name, oldDefCaus.Tp != changingDefCaus.Tp)
			if err != nil {
				if ErrWarnDataTruncated.Equal(err) || errInvalidUseOfNull.Equal(err) {
					job.State = perceptron.JobStateRollingback
				}
				return ver, err
			}
		}
		// delete only -> write only
		uFIDelateChangingInfo(changingDefCaus, changingIdxs, perceptron.StateWriteOnly)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != changingDefCaus.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = perceptron.StateWriteOnly
	case perceptron.StateWriteOnly:
		// write only -> reorganization
		uFIDelateChangingInfo(changingDefCaus, changingIdxs, perceptron.StateWriteReorganization)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != changingDefCaus.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = perceptron.StateWriteReorganization
	case perceptron.StateWriteReorganization:
		tbl, err := getBlock(d.causetstore, dbInfo.ID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		reorgInfo, err := getReorgInfo(d, t, job, tbl)
		if err != nil || reorgInfo.first {
			// If we run reorg firstly, we should uFIDelate the job snapshot version
			// and then run the reorg next time.
			return ver, errors.Trace(err)
		}

		err = w.runReorgJob(t, reorgInfo, tbl.Meta(), d.lease, func() (addIndexErr error) {
			defer soliton.Recover(metrics.LabelDBS, "onModifyDeferredCauset",
				func() {
					addIndexErr = errCancelledDBSJob.GenWithStack("modify block `%v` defCausumn `%v` panic", tblInfo.Name, oldDefCaus.Name)
				}, false)
			return w.uFIDelateDeferredCausetAndIndexes(tbl, oldDefCaus, changingDefCaus, changingIdxs, reorgInfo)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// If timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if ekv.ErrKeyExists.Equal(err) || errCancelledDBSJob.Equal(err) || errCantDecodeRecord.Equal(err) || types.ErrOverflow.Equal(err) {
				logutil.BgLogger().Warn("[dbs] run modify defCausumn job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
				// When encounter these error above, we change the job to rolling back job directly.
				job.State = perceptron.JobStateRollingback
				return ver, errors.Trace(err)
			}
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			w.reorgCtx.cleanNotifyReorgCancel()
			return ver, errors.Trace(err)
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		w.reorgCtx.cleanNotifyReorgCancel()

		// Remove the old defCausumn and indexes. UFIDelate the relative defCausumn name and index names.
		oldIdxIDs := make([]int64, 0, len(changingIdxs))
		tblInfo.DeferredCausets = tblInfo.DeferredCausets[:len(tblInfo.DeferredCausets)-1]
		for _, cIdx := range changingIdxs {
			idxName := strings.TrimPrefix(cIdx.Name.O, changingIndexPrefix)
			for i, idx := range tblInfo.Indices {
				if strings.EqualFold(idxName, idx.Name.L) {
					cIdx.Name = perceptron.NewCIStr(idxName)
					tblInfo.Indices[i] = cIdx
					oldIdxIDs = append(oldIdxIDs, idx.ID)
					break
				}
			}
		}
		changingDefCaus.Name = defCausName
		changingDefCaus.ChangeStateInfo = nil
		tblInfo.Indices = tblInfo.Indices[:len(tblInfo.Indices)-len(changingIdxs)]
		// Adjust block defCausumn offset.
		if err = adjustDeferredCausetInfoInModifyDeferredCauset(job, tblInfo, changingDefCaus, oldDefCaus, pos); err != nil {
			// TODO: Do rollback.
			return ver, errors.Trace(err)
		}
		uFIDelateChangingInfo(changingDefCaus, changingIdxs, perceptron.StatePublic)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != changingDefCaus.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
		// Refactor the job args to add the old index ids into delete range block.
		job.Args = []interface{}{oldIdxIDs, getPartitionIDs(tblInfo)}
		// TODO: Change defCausumn ID.
		// asyncNotifyEvent(d, &soliton.Event{Tp: perceptron.CausetActionAddDeferredCauset, BlockInfo: tblInfo, DeferredCausetInfos: []*perceptron.DeferredCausetInfo{changingDefCaus}})
	default:
		err = ErrInvalidDBSState.GenWithStackByArgs("defCausumn", changingDefCaus.State)
	}

	return ver, errors.Trace(err)
}

func (w *worker) uFIDelatePhysicalBlockRow(t block.PhysicalBlock, oldDefCausInfo, defCausInfo *perceptron.DeferredCausetInfo, reorgInfo *reorgInfo) error {
	logutil.BgLogger().Info("[dbs] start to uFIDelate block event", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writePhysicalBlockRecord(t.(block.PhysicalBlock), typeUFIDelateDeferredCausetWorker, nil, oldDefCausInfo, defCausInfo, reorgInfo)
}

// uFIDelateDeferredCausetAndIndexes handles the modify defCausumn reorganization state for a block.
func (w *worker) uFIDelateDeferredCausetAndIndexes(t block.Block, oldDefCaus, defCaus *perceptron.DeferredCausetInfo, idxes []*perceptron.IndexInfo, reorgInfo *reorgInfo) error {
	// TODO: Consider rebuild ReorgInfo key to mDBSJobReorgKey_jobID_elementID(defCausID/idxID).
	// TODO: Support partition blocks.
	err := w.uFIDelatePhysicalBlockRow(t.(block.PhysicalBlock), oldDefCaus, defCaus, reorgInfo)
	if err != nil {
		return errors.Trace(err)
	}

	for _, idx := range idxes {
		err = w.addBlockIndex(t, idx, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type uFIDelateDeferredCausetWorker struct {
	*backfillWorker
	oldDefCausInfo *perceptron.DeferredCausetInfo
	newDefCausInfo *perceptron.DeferredCausetInfo
	metricCounter  prometheus.Counter

	// The following attributes are used to reduce memory allocation.
	rowRecords []*rowRecord
	rowDecoder *decoder.RowDecoder

	rowMap map[int64]types.Causet
}

func newUFIDelateDeferredCausetWorker(sessCtx stochastikctx.Context, worker *worker, id int, t block.PhysicalBlock, oldDefCaus, newDefCaus *perceptron.DeferredCausetInfo, decodeDefCausMap map[int64]decoder.DeferredCauset) *uFIDelateDeferredCausetWorker {
	rowDecoder := decoder.NewRowDecoder(t, t.WriblockDefCauss(), decodeDefCausMap)
	return &uFIDelateDeferredCausetWorker{
		backfillWorker: newBackfillWorker(sessCtx, worker, id, t),
		oldDefCausInfo: oldDefCaus,
		newDefCausInfo: newDefCaus,
		metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("uFIDelate_defCaus_speed"),
		rowDecoder:     rowDecoder,
		rowMap:         make(map[int64]types.Causet, len(decodeDefCausMap)),
	}
}

func (w *uFIDelateDeferredCausetWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

type rowRecord struct {
	key  []byte // It's used to dagger a record. Record it to reduce the encoding time.
	vals []byte // It's the record.
}

// getNextHandle gets next handle of entry that we are going to process.
func (w *uFIDelateDeferredCausetWorker) getNextHandle(taskRange reorgBackfillTask, taskDone bool, lastAccessedHandle ekv.Handle) (nextHandle ekv.Handle) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		return lastAccessedHandle.Next()
	}

	// The task is done. So we need to choose a handle outside this range.
	// Some corner cases should be considered:
	// - The end of task range is MaxInt64.
	// - The end of the task is excluded in the range.
	if (taskRange.endHandle.IsInt() && taskRange.endHandle.IntValue() == math.MaxInt64) || !taskRange.endIncluded {
		return taskRange.endHandle
	}

	return taskRange.endHandle.Next()
}

func (w *uFIDelateDeferredCausetWorker) fetchRowDefCausVals(txn ekv.Transaction, taskRange reorgBackfillTask) ([]*rowRecord, ekv.Handle, bool, error) {
	w.rowRecords = w.rowRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	var lastAccessedHandle ekv.Handle
	oprStartTime := startTime
	err := iterateSnapshotRows(w.sessCtx.GetStore(), w.priority, w.block, txn.StartTS(), taskRange.startHandle, taskRange.endHandle, taskRange.endIncluded,
		func(handle ekv.Handle, recordKey ekv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotRows in uFIDelateDeferredCausetWorker fetchRowDefCausVals", 0)
			oprStartTime = oprEndTime

			if !taskRange.endIncluded {
				taskDone = handle.Compare(taskRange.endHandle) >= 0
			} else {
				taskDone = handle.Compare(taskRange.endHandle) > 0
			}

			if taskDone || len(w.rowRecords) >= w.batchCnt {
				return false, nil
			}

			if err1 := w.getRowRecord(handle, recordKey, rawRow); err1 != nil {
				return false, errors.Trace(err1)
			}
			lastAccessedHandle = handle
			if handle.Equal(taskRange.endHandle) {
				// If taskRange.endIncluded == false, we will not reach here when handle == taskRange.endHandle.
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.rowRecords) == 0 {
		taskDone = true
	}

	logutil.BgLogger().Debug("[dbs] txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()), zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.rowRecords, w.getNextHandle(taskRange, taskDone, lastAccessedHandle), taskDone, errors.Trace(err)
}

func (w *uFIDelateDeferredCausetWorker) getRowRecord(handle ekv.Handle, recordKey []byte, rawRow []byte) error {
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.sessCtx, handle, rawRow, time.UTC, timeutil.SystemLocation(), w.rowMap)
	if err != nil {
		return errors.Trace(errCantDecodeRecord.GenWithStackByArgs("defCausumn", err))
	}

	if _, ok := w.rowMap[w.newDefCausInfo.ID]; ok {
		// The defCausumn is already added by uFIDelate or insert statement, skip it.
		w.cleanRowMap()
		return nil
	}

	newDefCausVal, err := block.CastValue(w.sessCtx, w.rowMap[w.oldDefCausInfo.ID], w.newDefCausInfo, false, false)
	// TODO: Consider sql_mode and the error msg(encounter this error check whether to rollback).
	if err != nil {
		return errors.Trace(err)
	}
	w.rowMap[w.newDefCausInfo.ID] = newDefCausVal
	newDeferredCausetIDs := make([]int64, 0, len(w.rowMap))
	newRow := make([]types.Causet, 0, len(w.rowMap))
	for defCausID, val := range w.rowMap {
		newDeferredCausetIDs = append(newDeferredCausetIDs, defCausID)
		newRow = append(newRow, val)
	}
	sctx, rd := w.sessCtx.GetStochaseinstein_dbars().StmtCtx, &w.sessCtx.GetStochaseinstein_dbars().RowEncoder
	newRowVal, err := blockcodec.EncodeRow(sctx, newRow, newDeferredCausetIDs, nil, nil, rd)
	if err != nil {
		return errors.Trace(err)
	}

	w.rowRecords = append(w.rowRecords, &rowRecord{key: recordKey, vals: newRowVal})
	w.cleanRowMap()
	return nil
}

func (w *uFIDelateDeferredCausetWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

// BackfillDataInTxn will backfill the block record in a transaction, dagger corresponding rowKey, if the value of rowKey is changed.
func (w *uFIDelateDeferredCausetWorker) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	errInTxn = ekv.RunInNewTxn(w.sessCtx.GetStore(), true, func(txn ekv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(ekv.Priority, w.priority)

		rowRecords, nextHandle, taskDone, err := w.fetchRowDefCausVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextHandle = nextHandle
		taskCtx.done = taskDone

		for _, rowRecord := range rowRecords {
			taskCtx.scanCount++

			err = txn.Set(rowRecord.key, rowRecord.vals)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}

		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "BackfillDataInTxn", 3000)

	return
}

func uFIDelateChangingInfo(changingDefCaus *perceptron.DeferredCausetInfo, changingIdxs []*perceptron.IndexInfo, schemaState perceptron.SchemaState) {
	changingDefCaus.State = schemaState
	for _, idx := range changingIdxs {
		idx.State = schemaState
	}
}

// doModifyDeferredCauset uFIDelates the defCausumn information and reorders all defCausumns. It does not support modifying defCausumn data.
func (w *worker) doModifyDeferredCauset(
	t *meta.Meta, job *perceptron.Job, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo,
	newDefCaus, oldDefCaus *perceptron.DeferredCausetInfo, pos *ast.DeferredCausetPosition) (ver int64, _ error) {
	// DeferredCauset from null to not null.
	if !allegrosql.HasNotNullFlag(oldDefCaus.Flag) && allegrosql.HasNotNullFlag(newDefCaus.Flag) {
		noPreventNullFlag := !allegrosql.HasPreventNullInsertFlag(oldDefCaus.Flag)
		// Introduce the `allegrosql.PreventNullInsertFlag` flag to prevent users from inserting or uFIDelating null values.
		err := modifyDefCaussFromNull2NotNull(w, dbInfo, tblInfo, []*perceptron.DeferredCausetInfo{oldDefCaus}, newDefCaus.Name, oldDefCaus.Tp != newDefCaus.Tp)
		if err != nil {
			if ErrWarnDataTruncated.Equal(err) || errInvalidUseOfNull.Equal(err) {
				job.State = perceptron.JobStateRollingback
			}
			return ver, err
		}
		// The defCausumn should get into prevent null status first.
		if noPreventNullFlag {
			return uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, true)
		}
	}

	if err := adjustDeferredCausetInfoInModifyDeferredCauset(job, tblInfo, newDefCaus, oldDefCaus, pos); err != nil {
		return ver, errors.Trace(err)
	}

	ver, err := uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, true)
	if err != nil {
		// Modified the type definition of 'null' to 'not null' before this, so rollBack the job when an error occurs.
		job.State = perceptron.JobStateRollingback
		return ver, errors.Trace(err)
	}

	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	// For those defCausumn-type-change type which doesn't need reorg data, we should also mock the job args for delete range.
	job.Args = []interface{}{[]int64{}, []int64{}}
	return ver, nil
}

func adjustDeferredCausetInfoInModifyDeferredCauset(
	job *perceptron.Job, tblInfo *perceptron.BlockInfo, newDefCaus, oldDefCaus *perceptron.DeferredCausetInfo, pos *ast.DeferredCausetPosition) error {
	// We need the latest defCausumn's offset and state. This information can be obtained from the causetstore.
	newDefCaus.Offset = oldDefCaus.Offset
	newDefCaus.State = oldDefCaus.State
	// Calculate defCausumn's new position.
	oldPos, newPos := oldDefCaus.Offset, oldDefCaus.Offset
	if pos.Tp == ast.DeferredCausetPositionAfter {
		// TODO: The check of "RelativeDeferredCauset" can be checked in advance. When "EnableChangeDeferredCausetType" is true, unnecessary state changes can be reduced.
		if oldDefCaus.Name.L == pos.RelativeDeferredCauset.Name.L {
			// `alter block blockName modify defCausumn b int after b` will return ver,ErrDeferredCausetNotExists.
			// Modified the type definition of 'null' to 'not null' before this, so rollback the job when an error occurs.
			job.State = perceptron.JobStateRollingback
			return schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(oldDefCaus.Name, tblInfo.Name)
		}

		relative := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, pos.RelativeDeferredCauset.Name.L)
		if relative == nil || relative.State != perceptron.StatePublic {
			job.State = perceptron.JobStateRollingback
			return schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(pos.RelativeDeferredCauset, tblInfo.Name)
		}

		if relative.Offset < oldPos {
			newPos = relative.Offset + 1
		} else {
			newPos = relative.Offset
		}
	} else if pos.Tp == ast.DeferredCausetPositionFirst {
		newPos = 0
	}

	defCausumnChanged := make(map[string]*perceptron.DeferredCausetInfo)
	defCausumnChanged[oldDefCaus.Name.L] = newDefCaus

	if newPos == oldPos {
		tblInfo.DeferredCausets[newPos] = newDefCaus
	} else {
		defcaus := tblInfo.DeferredCausets

		// Reorder defCausumns in place.
		if newPos < oldPos {
			// ******** +(new) ****** -(old) ********
			// [newPos:old-1] should shift one step to the right.
			INTERLOCKy(defcaus[newPos+1:], defcaus[newPos:oldPos])
		} else {
			// ******** -(old) ****** +(new) ********
			// [old+1:newPos] should shift one step to the left.
			INTERLOCKy(defcaus[oldPos:], defcaus[oldPos+1:newPos+1])
		}
		defcaus[newPos] = newDefCaus

		for i, defCaus := range tblInfo.DeferredCausets {
			if defCaus.Offset != i {
				defCausumnChanged[defCaus.Name.L] = defCaus
				defCaus.Offset = i
			}
		}
	}

	// Change offset and name in indices.
	for _, idx := range tblInfo.Indices {
		for _, c := range idx.DeferredCausets {
			cName := strings.ToLower(strings.TrimPrefix(c.Name.O, changingDeferredCausetPrefix))
			if newDefCaus, ok := defCausumnChanged[cName]; ok {
				c.Name = newDefCaus.Name
				c.Offset = newDefCaus.Offset
			}
		}
	}
	return nil
}

func checkAndApplyNewAutoRandomBits(job *perceptron.Job, t *meta.Meta, tblInfo *perceptron.BlockInfo,
	newDefCaus *perceptron.DeferredCausetInfo, oldName *perceptron.CIStr, newAutoRandBits uint64) error {
	schemaID := job.SchemaID
	newLayout := autoid.NewAutoRandomIDLayout(&newDefCaus.FieldType, newAutoRandBits)

	// GenAutoRandomID first to prevent concurrent uFIDelate.
	_, err := t.GenAutoRandomID(schemaID, tblInfo.ID, 1)
	if err != nil {
		return err
	}
	currentIncBitsVal, err := t.GetAutoRandomID(schemaID, tblInfo.ID)
	if err != nil {
		return err
	}
	// Find the max number of available shard bits by
	// counting leading zeros in current inc part of auto_random ID.
	availableBits := bits.LeadingZeros64(uint64(currentIncBitsVal))
	isOccupyingIncBits := newLayout.TypeBitsLength-newLayout.IncrementalBits > uint64(availableBits)
	if isOccupyingIncBits {
		availableBits := mathutil.Min(autoid.MaxAutoRandomBits, availableBits)
		errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg, availableBits, newAutoRandBits, oldName.O)
		job.State = perceptron.JobStateCancelled
		return ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
	}
	tblInfo.AutoRandomBits = newAutoRandBits
	return nil
}

// checkForNullValue ensure there are no null values of the defCausumn of this block.
// `isDataTruncated` indicates whether the new field and the old field type are the same, in order to be compatible with allegrosql.
func checkForNullValue(ctx stochastikctx.Context, isDataTruncated bool, schemaReplicant, block, newDefCaus perceptron.CIStr, oldDefCauss ...*perceptron.DeferredCausetInfo) error {
	defcausStr := ""
	for i, defCaus := range oldDefCauss {
		if i == 0 {
			defcausStr += "`" + defCaus.Name.L + "` is null"
		} else {
			defcausStr += " or `" + defCaus.Name.L + "` is null"
		}
	}
	allegrosql := fmt.Sprintf("select 1 from `%s`.`%s` where %s limit 1;", schemaReplicant.L, block.L, defcausStr)
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		if isDataTruncated {
			return ErrWarnDataTruncated.GenWithStackByArgs(newDefCaus.L, rowCount)
		}
		return errInvalidUseOfNull
	}
	return nil
}

func uFIDelateDeferredCausetDefaultValue(t *meta.Meta, job *perceptron.Job, newDefCaus *perceptron.DeferredCausetInfo, oldDefCausName *perceptron.CIStr) (ver int64, _ error) {
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	oldDefCaus := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, oldDefCausName.L)
	if oldDefCaus == nil || oldDefCaus.State != perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		return ver, schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(newDefCaus.Name, tblInfo.Name)
	}
	// The newDefCaus's offset may be the value of the old schemaReplicant version, so we can't use newDefCaus directly.
	oldDefCaus.DefaultValue = newDefCaus.DefaultValue
	oldDefCaus.DefaultValueBit = newDefCaus.DefaultValueBit
	oldDefCaus.Flag = newDefCaus.Flag

	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func isDeferredCausetWithIndex(defCausName string, indices []*perceptron.IndexInfo) bool {
	for _, indexInfo := range indices {
		for _, defCaus := range indexInfo.DeferredCausets {
			if defCaus.Name.L == defCausName {
				return true
			}
		}
	}
	return false
}

func isDeferredCausetCanDropWithIndex(defCausName string, indices []*perceptron.IndexInfo) bool {
	for _, indexInfo := range indices {
		if indexInfo.Primary || len(indexInfo.DeferredCausets) > 1 {
			for _, defCaus := range indexInfo.DeferredCausets {
				if defCaus.Name.L == defCausName {
					return false
				}
			}
		}
	}
	return true
}

func listIndicesWithDeferredCauset(defCausName string, indices []*perceptron.IndexInfo) []*perceptron.IndexInfo {
	ret := make([]*perceptron.IndexInfo, 0)
	for _, indexInfo := range indices {
		if len(indexInfo.DeferredCausets) == 1 && defCausName == indexInfo.DeferredCausets[0].Name.L {
			ret = append(ret, indexInfo)
		}
	}
	return ret
}

func getDeferredCausetForeignKeyInfo(defCausName string, fkInfos []*perceptron.FKInfo) *perceptron.FKInfo {
	for _, fkInfo := range fkInfos {
		for _, defCaus := range fkInfo.DefCauss {
			if defCaus.L == defCausName {
				return fkInfo
			}
		}
	}
	return nil
}

func allocateDeferredCausetID(tblInfo *perceptron.BlockInfo) int64 {
	tblInfo.MaxDeferredCausetID++
	return tblInfo.MaxDeferredCausetID
}

func checkAddDeferredCausetTooManyDeferredCausets(defCausNum int) error {
	if uint32(defCausNum) > atomic.LoadUint32(&BlockDeferredCausetCountLimit) {
		return errTooManyFields
	}
	return nil
}

// rollbackModifyDeferredCausetJob rollbacks the job when an error occurs.
func rollbackModifyDeferredCausetJob(t *meta.Meta, tblInfo *perceptron.BlockInfo, job *perceptron.Job, oldDefCaus *perceptron.DeferredCausetInfo, modifyDeferredCausetTp byte) (ver int64, _ error) {
	var err error
	if modifyDeferredCausetTp == allegrosql.TypeNull {
		// field NotNullFlag flag reset.
		tblInfo.DeferredCausets[oldDefCaus.Offset].Flag = oldDefCaus.Flag &^ allegrosql.NotNullFlag
		// field PreventNullInsertFlag flag reset.
		tblInfo.DeferredCausets[oldDefCaus.Offset].Flag = oldDefCaus.Flag &^ allegrosql.PreventNullInsertFlag
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}
	job.FinishBlockJob(perceptron.JobStateRollbackDone, perceptron.StateNone, ver, tblInfo)
	// For those defCausumn-type-change type which doesn't need reorg data, we should also mock the job args for delete range.
	job.Args = []interface{}{[]int64{}, []int64{}}
	return ver, nil
}

// modifyDefCaussFromNull2NotNull modifies the type definitions of 'null' to 'not null'.
// Introduce the `allegrosql.PreventNullInsertFlag` flag to prevent users from inserting or uFIDelating null values.
func modifyDefCaussFromNull2NotNull(w *worker, dbInfo *perceptron.DBInfo, tblInfo *perceptron.BlockInfo, defcaus []*perceptron.DeferredCausetInfo,
	newDefCausName perceptron.CIStr, isModifiedType bool) error {
	// Get stochastikctx from context resource pool.
	var ctx stochastikctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	skipCheck := false
	failpoint.Inject("skipMockContextDoExec", func(val failpoint.Value) {
		if val.(bool) {
			skipCheck = true
		}
	})
	if !skipCheck {
		// If there is a null value inserted, it cannot be modified and needs to be rollback.
		err = checkForNullValue(ctx, isModifiedType, dbInfo.Name, tblInfo.Name, newDefCausName, defcaus...)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Prevent this field from inserting null values.
	for _, defCaus := range defcaus {
		defCaus.Flag |= allegrosql.PreventNullInsertFlag
	}
	return nil
}

func generateOriginDefaultValue(defCaus *perceptron.DeferredCausetInfo) (interface{}, error) {
	var err error
	odValue := defCaus.GetDefaultValue()
	if odValue == nil && allegrosql.HasNotNullFlag(defCaus.Flag) {
		zeroVal := block.GetZeroValue(defCaus)
		odValue, err = zeroVal.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if odValue == strings.ToUpper(ast.CurrentTimestamp) {
		if defCaus.Tp == allegrosql.TypeTimestamp {
			odValue = time.Now().UTC().Format(types.TimeFormat)
		} else if defCaus.Tp == allegrosql.TypeDatetime {
			odValue = time.Now().Format(types.TimeFormat)
		}
	}
	return odValue, nil
}

func findDeferredCausetInIndexDefCauss(c string, defcaus []*perceptron.IndexDeferredCauset) *perceptron.IndexDeferredCauset {
	for _, c1 := range defcaus {
		if c == c1.Name.L {
			return c1
		}
	}
	return nil
}

func getDeferredCausetInfoByName(tbInfo *perceptron.BlockInfo, defCausumn string) *perceptron.DeferredCausetInfo {
	for _, defCausInfo := range tbInfo.DefCauss() {
		if defCausInfo.Name.L == defCausumn {
			return defCausInfo
		}
	}
	return nil
}

// isVirtualGeneratedDeferredCauset checks the defCausumn if it is virtual.
func isVirtualGeneratedDeferredCauset(defCaus *perceptron.DeferredCausetInfo) bool {
	if defCaus.IsGenerated() && !defCaus.GeneratedStored {
		return true
	}
	return false
}

func indexInfoContains(idxID int64, idxInfos []*perceptron.IndexInfo) bool {
	for _, idxInfo := range idxInfos {
		if idxID == idxInfo.ID {
			return true
		}
	}
	return false
}

func indexInfosToIDList(idxInfos []*perceptron.IndexInfo) []int64 {
	ids := make([]int64, 0, len(idxInfos))
	for _, idxInfo := range idxInfos {
		ids = append(ids, idxInfo.ID)
	}
	return ids
}
