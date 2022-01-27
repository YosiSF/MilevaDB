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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/format"
	"github.com/whtcorpsinc/berolinaAllegroSQL/opcode"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/ekvproto/pkg/metapb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/dbs/memristed"
	"github.com/whtcorpsinc/milevadb/dbs/soliton"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

const (
	partitionMaxValue = "MAXVALUE"
)

func checkAddPartition(t *meta.Meta, job *perceptron.Job) (*perceptron.BlockInfo, *perceptron.PartitionInfo, []perceptron.PartitionDefinition, error) {
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	partInfo := &perceptron.PartitionInfo{}
	err = job.DecodeArgs(&partInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, nil, errors.Trace(err)
	}
	if len(tblInfo.Partition.AddingDefinitions) > 0 {
		return tblInfo, partInfo, tblInfo.Partition.AddingDefinitions, nil
	}
	return tblInfo, partInfo, []perceptron.PartitionDefinition{}, nil
}

func onAddBlockPartition(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	// Handle the rolling back job
	if job.IsRollingback() {
		ver, err := onDropBlockPartition(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	tblInfo, partInfo, addingDefinitions, err := checkAddPartition(t, job)
	if err != nil {
		return ver, err
	}

	// In order to skip maintaining the state check in partitionDefinition, MilevaDB use addingDefinition instead of state field.
	// So here using `job.SchemaState` to judge what the stage of this job is.
	switch job.SchemaState {
	case perceptron.StateNone:
		// job.SchemaState == perceptron.StateNone means the job is in the initial state of add partition.
		// Here should use partInfo from job directly and do some check action.
		err = checkAddPartitionTooManyPartitions(uint64(len(tblInfo.Partition.Definitions) + len(partInfo.Definitions)))
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkAddPartitionValue(tblInfo, partInfo)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkAddPartitionNameUnique(tblInfo, partInfo)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// none -> replica only
		job.SchemaState = perceptron.StateReplicaOnly
		// move the adding definition into blockInfo.
		uFIDelateAddingPartitionInfo(partInfo, tblInfo)
		ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, true)
	case perceptron.StateReplicaOnly:
		// replica only -> public
		// Here need do some tiflash replica complement check.
		// TODO: If a block is with no TiFlashReplica or it is not available, the replica-only state can be eliminated.
		if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
			// For available state, the new added partition should wait it's replica to
			// be finished. Otherwise the query to this partition will be blocked.
			needWait, err := checkPartitionReplica(addingDefinitions, d)
			if err != nil {
				ver, err = convertAddBlockPartitionJob2RollbackJob(t, job, err, tblInfo)
				return ver, err
			}
			if needWait {
				// The new added partition hasn't been replicated.
				// Do nothing to the job this time, wait next worker round.
				time.Sleep(tiflashCheckMilevaDBHTTPAPIHalfInterval)
				return ver, nil
			}
		}

		// For normal and replica finished block, move the `addingDefinitions` into `Definitions`.
		uFIDelatePartitionInfo(tblInfo)

		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &soliton.Event{Tp: perceptron.CausetActionAddBlockPartition, BlockInfo: tblInfo, PartInfo: partInfo})
	default:
		err = ErrInvalidDBSState.GenWithStackByArgs("partition", job.SchemaState)
	}

	return ver, errors.Trace(err)
}

// uFIDelatePartitionInfo merge `addingDefinitions` into `Definitions` in the blockInfo.
func uFIDelatePartitionInfo(tblInfo *perceptron.BlockInfo) {
	parInfo := &perceptron.PartitionInfo{}
	oldDefs, newDefs := tblInfo.Partition.Definitions, tblInfo.Partition.AddingDefinitions
	parInfo.Definitions = make([]perceptron.PartitionDefinition, 0, len(newDefs)+len(oldDefs))
	parInfo.Definitions = append(parInfo.Definitions, oldDefs...)
	parInfo.Definitions = append(parInfo.Definitions, newDefs...)
	tblInfo.Partition.Definitions = parInfo.Definitions
	tblInfo.Partition.AddingDefinitions = nil
}

// uFIDelateAddingPartitionInfo write adding partitions into `addingDefinitions` field in the blockInfo.
func uFIDelateAddingPartitionInfo(partitionInfo *perceptron.PartitionInfo, tblInfo *perceptron.BlockInfo) {
	newDefs := partitionInfo.Definitions
	tblInfo.Partition.AddingDefinitions = make([]perceptron.PartitionDefinition, 0, len(newDefs))
	tblInfo.Partition.AddingDefinitions = append(tblInfo.Partition.AddingDefinitions, newDefs...)
}

// rollbackAddingPartitionInfo remove the `addingDefinitions` in the blockInfo.
func rollbackAddingPartitionInfo(tblInfo *perceptron.BlockInfo) []int64 {
	physicalBlockIDs := make([]int64, 0, len(tblInfo.Partition.AddingDefinitions))
	for _, one := range tblInfo.Partition.AddingDefinitions {
		physicalBlockIDs = append(physicalBlockIDs, one.ID)
	}
	tblInfo.Partition.AddingDefinitions = nil
	return physicalBlockIDs
}

// checkAddPartitionValue values less than value must be strictly increasing for each partition.
func checkAddPartitionValue(meta *perceptron.BlockInfo, part *perceptron.PartitionInfo) error {
	if meta.Partition.Type == perceptron.PartitionTypeRange && len(meta.Partition.DeferredCausets) == 0 {
		newDefs, oldDefs := part.Definitions, meta.Partition.Definitions
		rangeValue := oldDefs[len(oldDefs)-1].LessThan[0]
		if strings.EqualFold(rangeValue, "MAXVALUE") {
			return errors.Trace(ErrPartitionMaxvalue)
		}

		currentRangeValue, err := strconv.Atoi(rangeValue)
		if err != nil {
			return errors.Trace(err)
		}

		for i := 0; i < len(newDefs); i++ {
			ifMaxvalue := strings.EqualFold(newDefs[i].LessThan[0], "MAXVALUE")
			if ifMaxvalue && i == len(newDefs)-1 {
				return nil
			} else if ifMaxvalue && i != len(newDefs)-1 {
				return errors.Trace(ErrPartitionMaxvalue)
			}

			nextRangeValue, err := strconv.Atoi(newDefs[i].LessThan[0])
			if err != nil {
				return errors.Trace(err)
			}
			if nextRangeValue <= currentRangeValue {
				return errors.Trace(ErrRangeNotIncreasing)
			}
			currentRangeValue = nextRangeValue
		}
	}
	return nil
}

func checkPartitionReplica(addingDefinitions []perceptron.PartitionDefinition, d *dbsCtx) (needWait bool, err error) {
	ctx := context.Background()
	FIDelCli := d.causetstore.(einsteindb.CausetStorage).GetRegionCache().FIDelClient()
	stores, err := FIDelCli.GetAllStores(ctx)
	if err != nil {
		return needWait, errors.Trace(err)
	}
	for _, fidel := range addingDefinitions {
		startKey, endKey := blockcodec.GetBlockHandleKeyRange(fidel.ID)
		regions, err := FIDelCli.ScanRegions(ctx, startKey, endKey, -1)
		if err != nil {
			return needWait, errors.Trace(err)
		}
		// For every region in the partition, if it has some corresponding peers and
		// no pending peers, that means the replication has completed.
		for _, region := range regions {
			regionState, err := FIDelCli.GetRegionByID(ctx, region.Meta.Id)
			if err != nil {
				return needWait, errors.Trace(err)
			}
			tiflashPeerAtLeastOne := checkTiFlashPeerStoreAtLeastOne(stores, regionState.Meta.Peers)
			// It's unnecessary to wait all tiflash peer to be replicated.
			// Here only make sure that tiflash peer count > 0 (at least one).
			if tiflashPeerAtLeastOne {
				continue
			}
			needWait = true
			logutil.BgLogger().Info("[dbs] partition replicas check failed in replica-only DBS state", zap.Int64("pID", fidel.ID), zap.Uint64("wait region ID", region.Meta.Id), zap.Bool("tiflash peer at least one", tiflashPeerAtLeastOne), zap.Time("check time", time.Now()))
			return needWait, nil
		}
	}
	logutil.BgLogger().Info("[dbs] partition replicas check ok in replica-only DBS state")
	return needWait, nil
}

func checkTiFlashPeerStoreAtLeastOne(stores []*metapb.CausetStore, peers []*metapb.Peer) bool {
	for _, peer := range peers {
		for _, causetstore := range stores {
			if peer.StoreId == causetstore.Id && storeHasEngineTiFlashLabel(causetstore) {
				return true
			}
		}
	}
	return false
}

func storeHasEngineTiFlashLabel(causetstore *metapb.CausetStore) bool {
	for _, label := range causetstore.Labels {
		if label.Key == "engine" && label.Value == "tiflash" {
			return true
		}
	}
	return false
}

// buildBlockPartitionInfo builds partition info and checks for some errors.
func buildBlockPartitionInfo(ctx stochastikctx.Context, s *ast.CreateBlockStmt) (*perceptron.PartitionInfo, error) {
	if s.Partition == nil {
		return nil, nil
	}

	if ctx.GetStochastikVars().EnableBlockPartition == "off" {
		ctx.GetStochastikVars().StmtCtx.AppendWarning(errBlockPartitionDisabled)
		return nil, nil
	}

	var enable bool
	// When milevadb_enable_block_partition is 'on' or 'auto'.
	if s.Partition.Tp == perceptron.PartitionTypeRange {
		if s.Partition.Sub == nil {
			// Partition by range expression is enabled by default.
			if s.Partition.DeferredCausetNames == nil {
				enable = true
			}
			// Partition by range columns and just one column.
			if len(s.Partition.DeferredCausetNames) == 1 {
				enable = true
			}
		}
	}
	// Partition by hash is enabled by default.
	// Note that linear hash is not enabled.
	if s.Partition.Tp == perceptron.PartitionTypeHash {
		if !s.Partition.Linear && s.Partition.Sub == nil {
			enable = true
		}
	}

	if !enable {
		ctx.GetStochastikVars().StmtCtx.AppendWarning(errUnsupportedCreatePartition)
		return nil, nil
	}

	pi := &perceptron.PartitionInfo{
		Type:   s.Partition.Tp,
		Enable: enable,
		Num:    s.Partition.Num,
	}
	if s.Partition.Expr != nil {
		buf := new(bytes.Buffer)
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		if err := s.Partition.Expr.Restore(restoreCtx); err != nil {
			return nil, err
		}
		pi.Expr = buf.String()
	} else if s.Partition.DeferredCausetNames != nil {
		// TODO: Support multiple columns for 'PARTITION BY RANGE COLUMNS'.
		if len(s.Partition.DeferredCausetNames) != 1 {
			pi.Enable = false
			ctx.GetStochastikVars().StmtCtx.AppendWarning(ErrUnsupportedPartitionByRangeDeferredCausets)
		}
		pi.DeferredCausets = make([]perceptron.CIStr, 0, len(s.Partition.DeferredCausetNames))
		for _, cn := range s.Partition.DeferredCausetNames {
			pi.DeferredCausets = append(pi.DeferredCausets, cn.Name)
		}
	}

	if s.Partition.Tp == perceptron.PartitionTypeRange {
		if err := buildRangePartitionDefinitions(ctx, s, pi); err != nil {
			return nil, errors.Trace(err)
		}
	} else if s.Partition.Tp == perceptron.PartitionTypeHash {
		if err := buildHashPartitionDefinitions(ctx, s, pi); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return pi, nil
}

func buildHashPartitionDefinitions(ctx stochastikctx.Context, s *ast.CreateBlockStmt, pi *perceptron.PartitionInfo) error {
	if err := checkAddPartitionTooManyPartitions(pi.Num); err != nil {
		return err
	}

	defs := make([]perceptron.PartitionDefinition, pi.Num)
	for i := 0; i < len(defs); i++ {
		if len(s.Partition.Definitions) == 0 {
			defs[i].Name = perceptron.NewCIStr(fmt.Sprintf("p%v", i))
		} else {
			def := s.Partition.Definitions[i]
			defs[i].Name = def.Name
			defs[i].Comment, _ = def.Comment()
		}
	}
	pi.Definitions = defs
	return nil
}

func buildRangePartitionDefinitions(ctx stochastikctx.Context, s *ast.CreateBlockStmt, pi *perceptron.PartitionInfo) (err error) {
	for _, def := range s.Partition.Definitions {
		comment, _ := def.Comment()
		err = checkTooLongBlock(def.Name)
		if err != nil {
			return err
		}
		piDef := perceptron.PartitionDefinition{
			Name:    def.Name,
			Comment: comment,
		}

		buf := new(bytes.Buffer)
		// Range columns partitions support multi-column partitions.
		for _, expr := range def.Clause.(*ast.PartitionDefinitionClauseLessThan).Exprs {
			expr.Format(buf)
			piDef.LessThan = append(piDef.LessThan, buf.String())
			buf.Reset()
		}
		pi.Definitions = append(pi.Definitions, piDef)
	}
	return nil
}

func checkPartitionNameUnique(pi *perceptron.PartitionInfo) error {
	newPars := pi.Definitions
	partNames := make(map[string]struct{}, len(newPars))
	for _, newPar := range newPars {
		if _, ok := partNames[newPar.Name.L]; ok {
			return ErrSameNamePartition.GenWithStackByArgs(newPar.Name)
		}
		partNames[newPar.Name.L] = struct{}{}
	}
	return nil
}

func checkAddPartitionNameUnique(tbInfo *perceptron.BlockInfo, pi *perceptron.PartitionInfo) error {
	partNames := make(map[string]struct{})
	if tbInfo.Partition != nil {
		oldPars := tbInfo.Partition.Definitions
		for _, oldPar := range oldPars {
			partNames[oldPar.Name.L] = struct{}{}
		}
	}
	newPars := pi.Definitions
	for _, newPar := range newPars {
		if _, ok := partNames[newPar.Name.L]; ok {
			return ErrSameNamePartition.GenWithStackByArgs(newPar.Name)
		}
		partNames[newPar.Name.L] = struct{}{}
	}
	return nil
}

func checkAndOverridePartitionID(newBlockInfo, oldBlockInfo *perceptron.BlockInfo) error {
	// If any old partitionInfo has lost, that means the partition ID lost too, so did the data, repair failed.
	if newBlockInfo.Partition == nil {
		return nil
	}
	if oldBlockInfo.Partition == nil {
		return ErrRepairBlockFail.GenWithStackByArgs("Old block doesn't have partitions")
	}
	if newBlockInfo.Partition.Type != oldBlockInfo.Partition.Type {
		return ErrRepairBlockFail.GenWithStackByArgs("Partition type should be the same")
	}
	// Check whether partitionType is hash partition.
	if newBlockInfo.Partition.Type == perceptron.PartitionTypeHash {
		if newBlockInfo.Partition.Num != oldBlockInfo.Partition.Num {
			return ErrRepairBlockFail.GenWithStackByArgs("Hash partition num should be the same")
		}
	}
	for i, newOne := range newBlockInfo.Partition.Definitions {
		found := false
		for _, oldOne := range oldBlockInfo.Partition.Definitions {
			// Fix issue 17952 which wanna substitute partition range expr.
			// So eliminate stringSliceEqual(newOne.LessThan, oldOne.LessThan) here.
			if newOne.Name.L == oldOne.Name.L {
				newBlockInfo.Partition.Definitions[i].ID = oldOne.ID
				found = true
				break
			}
		}
		if !found {
			return ErrRepairBlockFail.GenWithStackByArgs("Partition " + newOne.Name.L + " has lost")
		}
	}
	return nil
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	// Accelerate the compare by eliminate index bound check.
	b = b[:len(a)]
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// hasTimestampField derives from https://github.com/allegrosql/allegrosql-server/blob/5.7/allegrosql/item_func.h#L387
func hasTimestampField(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, expr ast.ExprNode) (bool, error) {
	partDefCauss, err := checkPartitionDeferredCausets(tblInfo, expr)
	if err != nil {
		return false, err
	}

	for _, c := range partDefCauss {
		if c.FieldType.Tp == allegrosql.TypeTimestamp {
			return true, nil
		}
	}

	return false, nil
}

// hasDateField derives from https://github.com/allegrosql/allegrosql-server/blob/5.7/allegrosql/item_func.h#L399
func hasDateField(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, expr ast.ExprNode) (bool, error) {
	partDefCauss, err := checkPartitionDeferredCausets(tblInfo, expr)
	if err != nil {
		return false, err
	}

	for _, c := range partDefCauss {
		if c.FieldType.Tp == allegrosql.TypeDate || c.FieldType.Tp == allegrosql.TypeDatetime {
			return true, nil
		}
	}

	return false, nil
}

// hasTimeField derives from https://github.com/allegrosql/allegrosql-server/blob/5.7/allegrosql/item_func.h#L412
func hasTimeField(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, expr ast.ExprNode) (bool, error) {
	partDefCauss, err := checkPartitionDeferredCausets(tblInfo, expr)
	if err != nil {
		return false, err
	}

	for _, c := range partDefCauss {
		if c.FieldType.Tp == allegrosql.TypeDatetime || c.FieldType.Tp == allegrosql.TypeDuration {
			return true, nil
		}
	}

	return false, nil
}

// defaultTimezoneDependent derives from https://github.com/allegrosql/allegrosql-server/blob/5.7/allegrosql/item_func.h#L445
// We assume the result of any function that has a TIMESTAMP argument to be
// timezone-dependent, since a TIMESTAMP value in both numeric and string
// contexts is interpreted according to the current timezone.
// The only exception is UNIX_TIMESTAMP() which returns the internal
// representation of a TIMESTAMP argument verbatim, and thus does not depend on
// the timezone.
func defaultTimezoneDependent(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, expr ast.ExprNode) (bool, error) {
	v, err := hasTimestampField(ctx, tblInfo, expr)
	if err != nil {
		return false, err
	}

	return !v, nil
}

func checkPartitionFuncCallValid(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, expr *ast.FuncCallExpr) error {
	// We assume the result of any function that has a TIMESTAMP argument to be
	// timezone-dependent, since a TIMESTAMP value in both numeric and string
	// contexts is interpreted according to the current timezone.
	// The only exception is UNIX_TIMESTAMP() which returns the internal
	// representation of a TIMESTAMP argument verbatim, and thus does not depend on
	// the timezone.
	// See https://github.com/allegrosql/allegrosql-server/blob/5.7/allegrosql/item_func.h#L445
	if expr.FnName.L != ast.UnixTimestamp {
		for _, arg := range expr.Args {
			if colName, ok := arg.(*ast.DeferredCausetNameExpr); ok {
				col := findDeferredCausetByName(colName.Name.Name.L, tblInfo)
				if col == nil {
					return ErrBadField.GenWithStackByArgs(colName.Name.Name.O, "expression")
				}

				if ok && col.FieldType.Tp == allegrosql.TypeTimestamp {
					return errors.Trace(errWrongExprInPartitionFunc)
				}
			}
		}
	}

	// check function which allowed in partitioning expressions
	// see https://dev.allegrosql.com/doc/allegrosql-partitioning-excerpt/5.7/en/partitioning-limitations-functions.html
	switch expr.FnName.L {
	// Mysql don't allow creating partitions with expressions with non matching
	// arguments as a (sub)partitioning function,
	// but we want to allow such expressions when opening existing blocks for
	// easier maintenance. This exception should be deprecated at some point in future so that we always throw an error.
	// See https://github.com/allegrosql/allegrosql-server/blob/5.7/allegrosql/sql_partition.cc#L1072
	case ast.Day, ast.DayOfMonth, ast.DayOfWeek, ast.DayOfYear, ast.Month, ast.Quarter, ast.ToDays, ast.ToSeconds,
		ast.Weekday, ast.Year, ast.YearWeek:
		return checkResultOK(hasDateField(ctx, tblInfo, expr))
	case ast.Hour, ast.MicroSecond, ast.Minute, ast.Second, ast.TimeToSec:
		return checkResultOK(hasTimeField(ctx, tblInfo, expr))
	case ast.UnixTimestamp:
		if len(expr.Args) != 1 {
			return errors.Trace(errWrongExprInPartitionFunc)
		}
		col, err := expression.RewriteSimpleExprWithBlockInfo(ctx, tblInfo, expr.Args[0])
		if err != nil {
			return errors.Trace(err)
		}
		if col.GetType().Tp != allegrosql.TypeTimestamp {
			return errors.Trace(errWrongExprInPartitionFunc)
		}
		return nil
	case ast.Abs, ast.Ceiling, ast.DateDiff, ast.Extract, ast.Floor, ast.Mod:
		for _, arg := range expr.Args {
			if err := checkPartitionExprValid(ctx, tblInfo, arg); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.Trace(ErrPartitionFunctionIsNotAllowed)
}

// checkPartitionExprValid checks partition expression validly.
func checkPartitionExprValid(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, expr ast.ExprNode) error {
	switch v := expr.(type) {
	case *ast.FuncCastExpr, *ast.CaseExpr, *ast.SubqueryExpr, *ast.WindowFuncExpr, *ast.RowExpr, *ast.DefaultExpr, *ast.ValuesExpr:
		return errors.Trace(ErrPartitionFunctionIsNotAllowed)
	case *ast.FuncCallExpr:
		return checkPartitionFuncCallValid(ctx, tblInfo, v)
	case *ast.BinaryOperationExpr:
		// The DIV operator (opcode.IntDiv) is also supported; the / operator ( opcode.Div ) is not permitted.
		// see https://dev.allegrosql.com/doc/refman/5.7/en/partitioning-limitations.html
		switch v.Op {
		case opcode.Or, opcode.And, opcode.Xor, opcode.LeftShift, opcode.RightShift, opcode.BitNeg, opcode.Div:
			return errors.Trace(ErrPartitionFunctionIsNotAllowed)
		default:
			if err := checkPartitionExprValid(ctx, tblInfo, v.L); err != nil {
				return errors.Trace(err)
			}
			if err := checkPartitionExprValid(ctx, tblInfo, v.R); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	case *ast.UnaryOperationExpr:
		if v.Op == opcode.BitNeg {
			return errors.Trace(ErrPartitionFunctionIsNotAllowed)
		}
		if err := checkPartitionExprValid(ctx, tblInfo, v.V); err != nil {
			return errors.Trace(err)
		}
		return nil
	case *ast.ParenthesesExpr:
		return checkPartitionExprValid(ctx, tblInfo, v.Expr)
	}
	return nil
}

// checkPartitionFuncValid checks partition function validly.
func checkPartitionFuncValid(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, expr ast.ExprNode) error {
	err := checkPartitionExprValid(ctx, tblInfo, expr)
	if err != nil {
		return err
	}
	// check constant.
	_, err = checkPartitionDeferredCausets(tblInfo, expr)
	return err
}

// checkResultOK derives from https://github.com/allegrosql/allegrosql-server/blob/5.7/allegrosql/item_timefunc
// For partition blocks, allegrosql do not support Constant, random or timezone-dependent expressions
// Based on allegrosql code to check whether field is valid, every time related type has check_valid_arguments_processor function.
func checkResultOK(ok bool, err error) error {
	if err != nil {
		return err
	}

	if !ok {
		return errors.Trace(errWrongExprInPartitionFunc)
	}

	return nil
}

func checkPartitionDeferredCausets(tblInfo *perceptron.BlockInfo, expr ast.ExprNode) ([]*perceptron.DeferredCausetInfo, error) {
	var buf strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
	err := expr.Restore(restoreCtx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	partDefCauss, err := extractPartitionDeferredCausets(buf.String(), tblInfo)
	if err != nil {
		return nil, err
	}

	if len(partDefCauss) == 0 {
		return nil, errors.Trace(errWrongExprInPartitionFunc)
	}

	return partDefCauss, nil
}

// checkPartitionFuncType checks partition function return type.
func checkPartitionFuncType(ctx stochastikctx.Context, s *ast.CreateBlockStmt, tblInfo *perceptron.BlockInfo) error {
	if s.Partition.Expr == nil {
		return nil
	}
	var buf strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
	if err := s.Partition.Expr.Restore(restoreCtx); err != nil {
		return errors.Trace(err)
	}
	exprStr := buf.String()
	if s.Partition.Tp == perceptron.PartitionTypeRange || s.Partition.Tp == perceptron.PartitionTypeHash {
		// if partition by columnExpr, check the column type
		if _, ok := s.Partition.Expr.(*ast.DeferredCausetNameExpr); ok {
			for _, col := range tblInfo.DeferredCausets {
				name := strings.Replace(col.Name.String(), ".", "`.`", -1)
				// Range partitioning key supported types: tinyint, smallint, mediumint, int and bigint.
				if !validRangePartitionType(col) && fmt.Sprintf("`%s`", name) == exprStr {
					return errors.Trace(ErrNotAllowedTypeInPartition.GenWithStackByArgs(exprStr))
				}
			}
		}
	}

	e, err := expression.ParseSimpleExprWithBlockInfo(ctx, exprStr, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	if e.GetType().EvalType() == types.ETInt {
		return nil
	}
	if s.Partition.Tp == perceptron.PartitionTypeHash {
		if _, ok := s.Partition.Expr.(*ast.DeferredCausetNameExpr); ok {
			return ErrNotAllowedTypeInPartition.GenWithStackByArgs(exprStr)
		}
	}

	return ErrPartitionFuncNotAllowed.GenWithStackByArgs("PARTITION")
}

// checkCreatePartitionValue checks whether `less than value` is strictly increasing for each partition.
// Side effect: it may simplify the partition range definition from a constant expression to an integer.
func checkCreatePartitionValue(ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo) error {
	pi := tblInfo.Partition
	defs := pi.Definitions
	if len(defs) == 0 {
		return nil
	}

	defcaus := tblInfo.DeferredCausets
	if strings.EqualFold(defs[len(defs)-1].LessThan[0], partitionMaxValue) {
		defs = defs[:len(defs)-1]
	}
	isUnsignedBigint := isRangePartitionDefCausUnsignedBigint(defcaus, pi)
	var prevRangeValue interface{}
	for i := 0; i < len(defs); i++ {
		if strings.EqualFold(defs[i].LessThan[0], partitionMaxValue) {
			return errors.Trace(ErrPartitionMaxvalue)
		}

		currentRangeValue, fromExpr, err := getRangeValue(ctx, defs[i].LessThan[0], isUnsignedBigint)
		if err != nil {
			return errors.Trace(err)
		}
		if fromExpr {
			// Constant fold the expression.
			defs[i].LessThan[0] = fmt.Sprintf("%d", currentRangeValue)
		}

		if i == 0 {
			prevRangeValue = currentRangeValue
			continue
		}

		if isUnsignedBigint {
			if currentRangeValue.(uint64) <= prevRangeValue.(uint64) {
				return errors.Trace(ErrRangeNotIncreasing)
			}
		} else {
			if currentRangeValue.(int64) <= prevRangeValue.(int64) {
				return errors.Trace(ErrRangeNotIncreasing)
			}
		}
		prevRangeValue = currentRangeValue
	}
	return nil
}

// getRangeValue gets an integer from the range value string.
// The returned boolean value indicates whether the input string is a constant expression.
func getRangeValue(ctx stochastikctx.Context, str string, unsignedBigint bool) (interface{}, bool, error) {
	// Unsigned bigint was converted to uint64 handle.
	if unsignedBigint {
		if value, err := strconv.ParseUint(str, 10, 64); err == nil {
			return value, false, nil
		}

		e, err1 := expression.ParseSimpleExprWithBlockInfo(ctx, str, &perceptron.BlockInfo{})
		if err1 != nil {
			return 0, false, err1
		}
		res, isNull, err2 := e.EvalInt(ctx, chunk.Row{})
		if err2 == nil && !isNull {
			return uint64(res), true, nil
		}
	} else {
		if value, err := strconv.ParseInt(str, 10, 64); err == nil {
			return value, false, nil
		}
		// The range value maybe not an integer, it could be a constant expression.
		// For example, the following two cases are the same:
		// PARTITION p0 VALUES LESS THAN (TO_SECONDS('2004-01-01'))
		// PARTITION p0 VALUES LESS THAN (63340531200)
		e, err1 := expression.ParseSimpleExprWithBlockInfo(ctx, str, &perceptron.BlockInfo{})
		if err1 != nil {
			return 0, false, err1
		}
		res, isNull, err2 := e.EvalInt(ctx, chunk.Row{})
		if err2 == nil && !isNull {
			return res, true, nil
		}
	}
	return 0, false, ErrNotAllowedTypeInPartition.GenWithStackByArgs(str)
}

// validRangePartitionType checks the type supported by the range partitioning key.
func validRangePartitionType(col *perceptron.DeferredCausetInfo) bool {
	switch col.FieldType.EvalType() {
	case types.ETInt:
		return true
	default:
		return false
	}
}

// checkDropBlockPartition checks if the partition exists and does not allow deleting the last existing partition in the block.
func checkDropBlockPartition(meta *perceptron.BlockInfo, partLowerNames []string) error {
	pi := meta.Partition
	if pi.Type != perceptron.PartitionTypeRange && pi.Type != perceptron.PartitionTypeList {
		return errOnlyOnRangeListPartition.GenWithStackByArgs("DROP")
	}
	oldDefs := pi.Definitions
	for _, pn := range partLowerNames {
		found := false
		for _, def := range oldDefs {
			if def.Name.L == pn {
				found = true
				break
			}
		}
		if !found {
			return errors.Trace(ErrDropPartitionNonExistent.GenWithStackByArgs(pn))
		}
	}
	if len(oldDefs) == len(partLowerNames) {
		return errors.Trace(ErrDropLastPartition)
	}
	return nil
}

// removePartitionInfo each dbs job deletes a partition.
func removePartitionInfo(tblInfo *perceptron.BlockInfo, partLowerNames []string) []int64 {
	oldDefs := tblInfo.Partition.Definitions
	newDefs := make([]perceptron.PartitionDefinition, 0, len(oldDefs)-len(partLowerNames))
	pids := make([]int64, 0, len(partLowerNames))

	// consider using a map to probe partLowerNames if too many partLowerNames
	for i := range oldDefs {
		found := false
		for _, partName := range partLowerNames {
			if oldDefs[i].Name.L == partName {
				found = true
				break
			}
		}
		if found {
			pids = append(pids, oldDefs[i].ID)
		} else {
			newDefs = append(newDefs, oldDefs[i])
		}
	}

	tblInfo.Partition.Definitions = newDefs
	return pids
}

func getPartitionDef(tblInfo *perceptron.BlockInfo, partName string) (index int, def *perceptron.PartitionDefinition, _ error) {
	defs := tblInfo.Partition.Definitions
	for i := 0; i < len(defs); i++ {
		if strings.EqualFold(defs[i].Name.L, strings.ToLower(partName)) {
			return i, &(defs[i]), nil
		}
	}
	return index, nil, block.ErrUnknownPartition.GenWithStackByArgs(partName, tblInfo.Name.O)
}

func buildPlacementDropMemrules(schemaID, blockID int64, partitionIDs []int64) []*memristed.MemruleOp {
	rules := make([]*memristed.MemruleOp, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		rules = append(rules, &memristed.MemruleOp{
			CausetAction:     memristed.MemruleOFIDelel,
			DeleteByIDPrefix: true,
			Memrule: &memristed.Memrule{
				GroupID: memristed.MemruleDefaultGroupID,
				ID:      fmt.Sprintf("%d_t%d_p%d", schemaID, blockID, partitionID),
			},
		})
	}
	return rules
}

// onDropBlockPartition deletes old partition meta.
func onDropBlockPartition(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	var partNames []string
	if err := job.DecodeArgs(&partNames); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	var physicalBlockIDs []int64
	if job.Type == perceptron.CausetActionAddBlockPartition {
		// It is rollbacked from adding block partition, just remove addingDefinitions from blockInfo.
		physicalBlockIDs = rollbackAddingPartitionInfo(tblInfo)
	} else {
		// If an error occurs, it returns that it cannot delete all partitions or that the partition doesn't exist.
		err = checkDropBlockPartition(tblInfo, partNames)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
		physicalBlockIDs = removePartitionInfo(tblInfo, partNames)
	}

	rules := buildPlacementDropMemrules(job.SchemaID, tblInfo.ID, physicalBlockIDs)
	err = infosync.UFIDelatePlacementMemrules(nil, rules)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify FIDel the memristed rules")
	}

	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	if job.IsRollingback() {
		job.FinishBlockJob(perceptron.JobStateRollbackDone, perceptron.StateNone, ver, tblInfo)
	} else {
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StateNone, ver, tblInfo)
	}

	// A background job will be created to delete old partition data.
	job.Args = []interface{}{physicalBlockIDs}
	return ver, nil
}

func buildPlacementTruncateMemrules(rules []*memristed.MemruleOp, schemaID, blockID, jobID int64, oldIDs []int64, newPartitions []perceptron.PartitionDefinition) []*memristed.MemruleOp {
	newMemrules := make([]*memristed.MemruleOp, 0, len(oldIDs))
	for i, oldID := range oldIDs {
		prefix := fmt.Sprintf("%d_t%d_p%d", schemaID, blockID, oldID)
		for _, rule := range rules {
			if strings.HasPrefix(rule.ID, prefix) {
				// delete the old rule
				newMemrules = append(newMemrules, &memristed.MemruleOp{
					CausetAction: memristed.MemruleOFIDelel,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						ID:      rule.ID,
					},
				})

				// add the new rule
				rule.CausetAction = memristed.MemruleOpAdd
				rule.ID = fmt.Sprintf("%d_t%d_p%d_%s_%d_%d", schemaID, blockID, newPartitions[i].ID, rule.Role, jobID, i)
				newMemrules = append(newMemrules, rule)
				break
			}
		}
	}
	return newMemrules
}

// onTruncateBlockPartition truncates old partition meta.
func onTruncateBlockPartition(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (int64, error) {
	var ver int64
	var oldIDs []int64
	if err := job.DecodeArgs(&oldIDs); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return ver, errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	newPartitions := make([]perceptron.PartitionDefinition, 0, len(oldIDs))
	for _, oldID := range oldIDs {
		for i := 0; i < len(pi.Definitions); i++ {
			def := &pi.Definitions[i]
			if def.ID == oldID {
				pid, err1 := t.GenGlobalID()
				if err != nil {
					return ver, errors.Trace(err1)
				}
				def.ID = pid
				// Shallow INTERLOCKy only use the def.ID in event handle.
				newPartitions = append(newPartitions, *def)
				break
			}
		}
	}
	if len(newPartitions) == 0 {
		return ver, block.ErrUnknownPartition.GenWithStackByArgs("drop?", tblInfo.Name.O)
	}

	// Clear the tiflash replica available status.
	if tblInfo.TiFlashReplica != nil {
		tblInfo.TiFlashReplica.Available = false
		// Set partition replica become unavailable.
		for _, oldID := range oldIDs {
			for i, id := range tblInfo.TiFlashReplica.AvailablePartitionIDs {
				if id == oldID {
					newIDs := tblInfo.TiFlashReplica.AvailablePartitionIDs[:i]
					newIDs = append(newIDs, tblInfo.TiFlashReplica.AvailablePartitionIDs[i+1:]...)
					tblInfo.TiFlashReplica.AvailablePartitionIDs = newIDs
					break
				}
			}
		}
	}

	var rules []*memristed.MemruleOp

	// TODO: maybe add a midbse state
	rules, err = infosync.GetPlacementMemrules(nil)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to retrieve memristed rules from FIDel")
	}

	// TODO: simplify the definition and logic use new FIDel group bundle API
	rules = buildPlacementTruncateMemrules(rules, job.SchemaID, tblInfo.ID, job.ID, oldIDs, newPartitions)

	err = infosync.UFIDelatePlacementMemrules(nil, rules)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify FIDel the memristed rules")
	}

	ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Finish this job.
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StateNone, ver, tblInfo)
	asyncNotifyEvent(d, &soliton.Event{Tp: perceptron.CausetActionTruncateBlockPartition, BlockInfo: tblInfo, PartInfo: &perceptron.PartitionInfo{Definitions: newPartitions}})
	// A background job will be created to delete old partition data.
	job.Args = []interface{}{oldIDs}
	return ver, nil
}

// onExchangeBlockPartition exchange partition data
func (w *worker) onExchangeBlockPartition(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	var (
		// defID only for uFIDelateSchemaVersion
		defID          int64
		ptSchemaID     int64
		ptID           int64
		partName       string
		withValidation bool
	)

	if err := job.DecodeArgs(&defID, &ptSchemaID, &ptID, &partName, &withValidation); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ntDbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	nt, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	pt, err := getBlockInfo(t, ptID, ptSchemaID)
	if err != nil {
		if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockNotExists.Equal(err) {
			job.State = perceptron.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	if pt.State != perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		return ver, ErrInvalidDBSState.GenWithStack("block %s is not in public, but %s", pt.Name, pt.State)
	}

	err = checkExchangePartition(pt, nt)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = checkBlockDefCompatible(pt, nt)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	index, _, err := getPartitionDef(pt, partName)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if withValidation {
		err = checkExchangePartitionRecordValidation(w, pt, index, ntDbInfo.Name, nt.Name)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	// partition block base auto id
	ptBaseID, err := t.GetAutoBlockID(ptSchemaID, pt.ID)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ptRandID, err := t.GetAutoRandomID(ptSchemaID, pt.ID)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// non-partition block base auto id
	ntBaseID, err := t.GetAutoBlockID(job.SchemaID, nt.ID)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ntRandID, err := t.GetAutoRandomID(job.SchemaID, nt.ID)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	_, partDef, err := getPartitionDef(pt, partName)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tempID := partDef.ID
	// exchange block meta id
	partDef.ID = nt.ID

	if pt.TiFlashReplica != nil {
		for i, id := range pt.TiFlashReplica.AvailablePartitionIDs {
			if id == tempID {
				pt.TiFlashReplica.AvailablePartitionIDs[i] = partDef.ID
				break
			}
		}
	}

	err = t.UFIDelateBlock(ptSchemaID, pt)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("exchangePartitionErr", func(val failpoint.Value) {
		if val.(bool) {
			job.State = perceptron.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after uFIDelating partition id"))
		}
	})

	// recreate non-partition block meta info
	err = t.DropBlockOrView(job.SchemaID, nt.ID, true)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	nt.ID = tempID

	err = t.CreateBlockOrView(job.SchemaID, nt)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// both pt and nt set the maximum auto_id between ntBaseID and ptBaseID
	if ntBaseID > ptBaseID {
		_, err = t.GenAutoBlockID(ptSchemaID, pt.ID, ntBaseID-ptBaseID)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	_, err = t.GenAutoBlockID(job.SchemaID, nt.ID, mathutil.MaxInt64(ptBaseID, ntBaseID))
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if ntRandID != 0 || ptRandID != 0 {
		if ntRandID > ptRandID {
			_, err = t.GenAutoRandomID(ptSchemaID, pt.ID, ntRandID-ptRandID)
			if err != nil {
				job.State = perceptron.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}

		_, err = t.GenAutoRandomID(job.SchemaID, nt.ID, mathutil.MaxInt64(ptRandID, ntRandID))
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	ver, err = uFIDelateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StateNone, ver, pt)
	return ver, nil
}

func checkExchangePartitionRecordValidation(w *worker, pt *perceptron.BlockInfo, index int, schemaName, blockName perceptron.CIStr) error {
	var allegrosql string

	pi := pt.Partition

	switch pi.Type {
	case perceptron.PartitionTypeHash:
		if pi.Num == 1 {
			return nil
		}
		allegrosql = fmt.Sprintf("select 1 from `%s`.`%s` where mod(%s, %d) != %d limit 1", schemaName.L, blockName.L, pi.Expr, pi.Num, index)
	case perceptron.PartitionTypeRange:
		// Block has only one partition and has the maximum value
		if len(pi.Definitions) == 1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
			return nil
		}
		// For range expression and range columns
		if len(pi.DeferredCausets) == 0 {
			allegrosql = buildCheckALLEGROSQLForRangeExprPartition(pi, index, schemaName, blockName)
		} else if len(pi.DeferredCausets) == 1 {
			allegrosql = buildCheckALLEGROSQLForRangeDeferredCausetsPartition(pi, index, schemaName, blockName)
		}
	default:
		return errUnsupportedPartitionType.GenWithStackByArgs(pt.Name.O)
	}

	var ctx stochastikctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		return errors.Trace(ErrRowDoesNotMatchPartition)
	}
	return nil
}

func buildCheckALLEGROSQLForRangeExprPartition(pi *perceptron.PartitionInfo, index int, schemaName, blockName perceptron.CIStr) string {
	if index == 0 {
		return fmt.Sprintf("select 1 from `%s`.`%s` where %s >= %s limit 1", schemaName.L, blockName.L, pi.Expr, pi.Definitions[index].LessThan[0])
	} else if index == len(pi.Definitions)-1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
		return fmt.Sprintf("select 1 from `%s`.`%s` where %s < %s limit 1", schemaName.L, blockName.L, pi.Expr, pi.Definitions[index-1].LessThan[0])
	} else {
		return fmt.Sprintf("select 1 from `%s`.`%s` where %s < %s or %s >= %s limit 1", schemaName.L, blockName.L, pi.Expr, pi.Definitions[index-1].LessThan[0], pi.Expr, pi.Definitions[index].LessThan[0])
	}
}

func buildCheckALLEGROSQLForRangeDeferredCausetsPartition(pi *perceptron.PartitionInfo, index int, schemaName, blockName perceptron.CIStr) string {
	colName := pi.DeferredCausets[0].L
	if index == 0 {
		return fmt.Sprintf("select 1 from `%s`.`%s` where `%s` >= %s limit 1", schemaName.L, blockName.L, colName, pi.Definitions[index].LessThan[0])
	} else if index == len(pi.Definitions)-1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
		return fmt.Sprintf("select 1 from `%s`.`%s` where `%s` < %s limit 1", schemaName.L, blockName.L, colName, pi.Definitions[index-1].LessThan[0])
	} else {
		return fmt.Sprintf("select 1 from `%s`.`%s` where `%s` < %s or `%s` >= %s limit 1", schemaName.L, blockName.L, colName, pi.Definitions[index-1].LessThan[0], colName, pi.Definitions[index].LessThan[0])
	}
}

func checkAddPartitionTooManyPartitions(piDefs uint64) error {
	if piDefs > uint64(PartitionCountLimit) {
		return errors.Trace(ErrTooManyPartitions)
	}
	return nil
}

func checkNoHashPartitions(ctx stochastikctx.Context, partitionNum uint64) error {
	if partitionNum == 0 {
		return ast.ErrNoParts.GenWithStackByArgs("partitions")
	}
	return nil
}

func checkNoRangePartitions(partitionNum int) error {
	if partitionNum == 0 {
		return ast.ErrPartitionsMustBeDefined.GenWithStackByArgs("RANGE")
	}
	return nil
}

func getPartitionIDs(block *perceptron.BlockInfo) []int64 {
	if block.GetPartitionInfo() == nil {
		return []int64{}
	}
	physicalBlockIDs := make([]int64, 0, len(block.Partition.Definitions))
	for _, def := range block.Partition.Definitions {
		physicalBlockIDs = append(physicalBlockIDs, def.ID)
	}
	return physicalBlockIDs
}

// checkPartitioningKeysConstraints checks that the range partitioning key is included in the block constraint.
func checkPartitioningKeysConstraints(sctx stochastikctx.Context, s *ast.CreateBlockStmt, tblInfo *perceptron.BlockInfo) error {
	// Returns directly if there are no unique keys in the block.
	if len(tblInfo.Indices) == 0 && !tblInfo.PKIsHandle {
		return nil
	}

	var partDefCauss stringSlice
	if s.Partition.Expr != nil {
		// Parse partitioning key, extract the column names in the partitioning key to slice.
		buf := new(bytes.Buffer)
		s.Partition.Expr.Format(buf)
		partDeferredCausets, err := extractPartitionDeferredCausets(buf.String(), tblInfo)
		if err != nil {
			return err
		}
		partDefCauss = columnInfoSlice(partDeferredCausets)
	} else if len(s.Partition.DeferredCausetNames) > 0 {
		partDefCauss = columnNameSlice(s.Partition.DeferredCausetNames)
	} else {
		// TODO: Check keys constraints for list, key partition type and so on.
		return nil
	}

	// Checks that the partitioning key is included in the constraint.
	// Every unique key on the block must use every column in the block's partitioning expression.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	for _, index := range tblInfo.Indices {
		if index.Unique && !checkUniqueKeyIncludePartKey(partDefCauss, index.DeferredCausets) {
			if index.Primary {
				return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY KEY")
			}
			return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("UNIQUE INDEX")
		}
	}
	// when PKIsHandle, tblInfo.Indices will not contain the primary key.
	if tblInfo.PKIsHandle {
		indexDefCauss := []*perceptron.IndexDeferredCauset{{
			Name:   tblInfo.GetPkName(),
			Length: types.UnspecifiedLength,
		}}
		if !checkUniqueKeyIncludePartKey(partDefCauss, indexDefCauss) {
			return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY KEY")
		}
	}
	return nil
}

func checkPartitionKeysConstraint(pi *perceptron.PartitionInfo, indexDeferredCausets []*perceptron.IndexDeferredCauset, tblInfo *perceptron.BlockInfo) (bool, error) {
	var (
		partDefCauss []*perceptron.DeferredCausetInfo
		err          error
	)
	// The expr will be an empty string if the partition is defined by:
	// CREATE TABLE t (...) PARTITION BY RANGE COLUMNS(...)
	if partExpr := pi.Expr; partExpr != "" {
		// Parse partitioning key, extract the column names in the partitioning key to slice.
		partDefCauss, err = extractPartitionDeferredCausets(partExpr, tblInfo)
		if err != nil {
			return false, err
		}
	} else {
		partDefCauss = make([]*perceptron.DeferredCausetInfo, 0, len(pi.DeferredCausets))
		for _, col := range pi.DeferredCausets {
			colInfo := getDeferredCausetInfoByName(tblInfo, col.L)
			if colInfo == nil {
				return false, schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(col, tblInfo.Name)
			}
			partDefCauss = append(partDefCauss, colInfo)
		}
	}

	// In MyALLEGROSQL, every unique key on the block must use every column in the block's partitioning expression.(This
	// also includes the block's primary key.)
	// In MilevaDB, global index will be built when this constraint is not satisfied and EnableGlobalIndex is set.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	return checkUniqueKeyIncludePartKey(columnInfoSlice(partDefCauss), indexDeferredCausets), nil
}

type columnNameExtractor struct {
	extractedDeferredCausets []*perceptron.DeferredCausetInfo
	tblInfo                  *perceptron.BlockInfo
	err                      error
}

func (cne *columnNameExtractor) Enter(node ast.Node) (ast.Node, bool) {
	return node, false
}

func (cne *columnNameExtractor) Leave(node ast.Node) (ast.Node, bool) {
	if c, ok := node.(*ast.DeferredCausetNameExpr); ok {
		info := findDeferredCausetByName(c.Name.Name.L, cne.tblInfo)
		if info != nil {
			cne.extractedDeferredCausets = append(cne.extractedDeferredCausets, info)
			return node, true
		}
		cne.err = ErrBadField.GenWithStackByArgs(c.Name.Name.O, "expression")
		return nil, false
	}
	return node, true
}

func findDeferredCausetByName(colName string, tblInfo *perceptron.BlockInfo) *perceptron.DeferredCausetInfo {
	for _, info := range tblInfo.DeferredCausets {
		if info.Name.L == colName {
			return info
		}
	}
	return nil
}

func extractPartitionDeferredCausets(partExpr string, tblInfo *perceptron.BlockInfo) ([]*perceptron.DeferredCausetInfo, error) {
	partExpr = "select " + partExpr
	stmts, _, err := berolinaAllegroSQL.New().Parse(partExpr, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	extractor := &columnNameExtractor{
		tblInfo:                  tblInfo,
		extractedDeferredCausets: make([]*perceptron.DeferredCausetInfo, 0),
	}
	stmts[0].Accept(extractor)
	if extractor.err != nil {
		return nil, errors.Trace(extractor.err)
	}
	return extractor.extractedDeferredCausets, nil
}

// stringSlice is defined for checkUniqueKeyIncludePartKey.
// if Go supports covariance, the code shouldn't be so complex.
type stringSlice interface {
	Len() int
	At(i int) string
}

// checkUniqueKeyIncludePartKey checks that the partitioning key is included in the constraint.
func checkUniqueKeyIncludePartKey(partDefCauss stringSlice, idxDefCauss []*perceptron.IndexDeferredCauset) bool {
	for i := 0; i < partDefCauss.Len(); i++ {
		partDefCaus := partDefCauss.At(i)
		idxDefCaus := findDeferredCausetInIndexDefCauss(partDefCaus, idxDefCauss)
		if idxDefCaus == nil {
			// Partition column is not found in the index columns.
			return false
		}
		if idxDefCaus.Length > 0 {
			// The partition column is found in the index columns, but the index column is a prefix index
			return false
		}
	}
	return true
}

// columnInfoSlice implements the stringSlice interface.
type columnInfoSlice []*perceptron.DeferredCausetInfo

func (cis columnInfoSlice) Len() int {
	return len(cis)
}

func (cis columnInfoSlice) At(i int) string {
	return cis[i].Name.L
}

// columnNameSlice implements the stringSlice interface.
type columnNameSlice []*ast.DeferredCausetName

func (cns columnNameSlice) Len() int {
	return len(cns)
}

func (cns columnNameSlice) At(i int) string {
	return cns[i].Name.L
}

// isRangePartitionDefCausUnsignedBigint returns true if the partitioning key column type is unsigned bigint type.
func isRangePartitionDefCausUnsignedBigint(defcaus []*perceptron.DeferredCausetInfo, pi *perceptron.PartitionInfo) bool {
	for _, col := range defcaus {
		isUnsigned := col.Tp == allegrosql.TypeLonglong && allegrosql.HasUnsignedFlag(col.Flag)
		if isUnsigned && strings.Contains(strings.ToLower(pi.Expr), col.Name.L) {
			return true
		}
	}
	return false
}

// truncateBlockByReassignPartitionIDs reassigns new partition ids.
func truncateBlockByReassignPartitionIDs(t *meta.Meta, tblInfo *perceptron.BlockInfo) error {
	newDefs := make([]perceptron.PartitionDefinition, 0, len(tblInfo.Partition.Definitions))
	for _, def := range tblInfo.Partition.Definitions {
		pid, err := t.GenGlobalID()
		if err != nil {
			return errors.Trace(err)
		}
		newDef := def
		newDef.ID = pid
		newDefs = append(newDefs, newDef)
	}
	tblInfo.Partition.Definitions = newDefs
	return nil
}

func onAlterBlockPartition(t *meta.Meta, job *perceptron.Job) (int64, error) {
	var partitionID int64
	var rules []*memristed.MemruleOp
	err := job.DecodeArgs(&partitionID, &rules)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	ptInfo := tblInfo.GetPartitionInfo()
	if ptInfo.GetNameByID(partitionID) == "" {
		job.State = perceptron.JobStateCancelled
		return 0, errors.Trace(block.ErrUnknownPartition.GenWithStackByArgs("drop?", tblInfo.Name.O))
	}

	for i, rule := range rules {
		if rule.CausetAction == memristed.MemruleOFIDelel {
			rule.ID = fmt.Sprintf("%d_t%d_p%d_%s", job.SchemaID, tblInfo.ID, partitionID, rule.Role)
		} else {
			rule.ID = fmt.Sprintf("%d_t%d_p%d_%s_%d_%d", job.SchemaID, tblInfo.ID, partitionID, rule.Role, job.ID, i)
		}
	}

	ver, err := t.GetSchemaVersion()
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = infosync.UFIDelatePlacementMemrules(nil, rules)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify FIDel the memristed rules")
	}

	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}
