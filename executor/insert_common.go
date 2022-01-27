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

package executor

import (
	"context"
	"math"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

// InsertValues is the data to insert.
type InsertValues struct {
	baseExecutor

	rowCount         uint64
	curBatchCnt      uint64
	maxEventsInBatch uint64
	lastInsertID     uint64

	SelectExec Executor

	Block           block.Block
	DeferredCausets []*ast.DeferredCausetName
	Lists           [][]expression.Expression
	SetList         []*expression.Assignment

	GenExprs []expression.Expression

	insertDeferredCausets []*block.DeferredCauset

	// defCausDefaultVals is used to causetstore casted default value.
	// Because not every insert statement needs defCausDefaultVals, so we will init the buffer lazily.
	defCausDefaultVals []defaultVal
	evalBuffer         chunk.MutEvent
	evalBufferTypes    []*types.FieldType

	allAssignmentsAreConstant bool

	hasRefDefCauss bool
	hasExtraHandle bool

	// Fill the autoID lazily to causet. This is used for being compatible with JDBC using getGeneratedKeys().
	// `insert|replace values` can guarantee consecutive autoID in a batch.
	// Other statements like `insert select from` don't guarantee consecutive autoID.
	// https://dev.allegrosql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html
	lazyFillAutoID bool
	memTracker     *memory.Tracker

	stats *runtimeStatsWithSnapshot
}

type defaultVal struct {
	val types.Causet
	// valid indicates whether the val is evaluated. We evaluate the default value lazily.
	valid bool
}

type insertCommon interface {
	insertCommon() *InsertValues
	exec(ctx context.Context, rows [][]types.Causet) error
}

func (e *InsertValues) insertCommon() *InsertValues {
	return e
}

func (e *InsertValues) exec(_ context.Context, _ [][]types.Causet) error {
	panic("derived should overload exec function")
}

// initInsertDeferredCausets sets the explicitly specified defCausumns of an insert statement. There are three cases:
// There are three types of insert statements:
// 1 insert ... values(...)  --> name type defCausumn
// 2 insert ... set x=y...   --> set type defCausumn
// 3 insert ... (select ..)  --> name type defCausumn
// See https://dev.allegrosql.com/doc/refman/5.7/en/insert.html
func (e *InsertValues) initInsertDeferredCausets() error {
	var defcaus []*block.DeferredCauset
	var missingDefCausName string
	var err error

	blockDefCauss := e.Block.DefCauss()

	if len(e.SetList) > 0 {
		// Process `set` type defCausumn.
		defCausumns := make([]string, 0, len(e.SetList))
		for _, v := range e.SetList {
			defCausumns = append(defCausumns, v.DefCausName.O)
		}
		defcaus, missingDefCausName = block.FindDefCauss(blockDefCauss, defCausumns, e.Block.Meta().PKIsHandle)
		if missingDefCausName != "" {
			return errors.Errorf("INSERT INTO %s: unknown defCausumn %s", e.Block.Meta().Name.O, missingDefCausName)
		}
		if len(defcaus) == 0 {
			return errors.Errorf("INSERT INTO %s: empty defCausumn", e.Block.Meta().Name.O)
		}
	} else if len(e.DeferredCausets) > 0 {
		// Process `name` type defCausumn.
		defCausumns := make([]string, 0, len(e.DeferredCausets))
		for _, v := range e.DeferredCausets {
			defCausumns = append(defCausumns, v.Name.O)
		}
		defcaus, missingDefCausName = block.FindDefCauss(blockDefCauss, defCausumns, e.Block.Meta().PKIsHandle)
		if missingDefCausName != "" {
			return errors.Errorf("INSERT INTO %s: unknown defCausumn %s", e.Block.Meta().Name.O, missingDefCausName)
		}
	} else {
		// If e.DeferredCausets are empty, use all defCausumns instead.
		defcaus = blockDefCauss
	}
	for _, defCaus := range defcaus {
		if !defCaus.IsGenerated() {
			e.insertDeferredCausets = append(e.insertDeferredCausets, defCaus)
		}
		if defCaus.Name.L == perceptron.ExtraHandleName.L {
			if !e.ctx.GetStochastikVars().AllowWriteEventID {
				return errors.Errorf("insert, uFIDelate and replace statements for _milevadb_rowid are not supported.")
			}
			e.hasExtraHandle = true
			break
		}
	}

	// Check defCausumn whether is specified only once.
	err = block.CheckOnce(defcaus)
	if err != nil {
		return err
	}
	return nil
}

func (e *InsertValues) initEvalBuffer() {
	numDefCauss := len(e.Block.DefCauss())
	if e.hasExtraHandle {
		numDefCauss++
	}
	e.evalBufferTypes = make([]*types.FieldType, numDefCauss)
	for i, defCaus := range e.Block.DefCauss() {
		e.evalBufferTypes[i] = &defCaus.FieldType
	}
	if e.hasExtraHandle {
		e.evalBufferTypes[len(e.evalBufferTypes)-1] = types.NewFieldType(allegrosql.TypeLonglong)
	}
	e.evalBuffer = chunk.MutEventFromTypes(e.evalBufferTypes)
}

func (e *InsertValues) lazilyInitDefCausDefaultValBuf() (ok bool) {
	if e.defCausDefaultVals != nil {
		return true
	}

	// only if values count of insert statement is more than one, use defCausDefaultVals to causetstore
	// casted default values has benefits.
	if len(e.Lists) > 1 {
		e.defCausDefaultVals = make([]defaultVal, len(e.Block.DefCauss()))
		return true
	}

	return false
}

func (e *InsertValues) processSetList() error {
	if len(e.SetList) > 0 {
		if len(e.Lists) > 0 {
			return errors.Errorf("INSERT INTO %s: set type should not use values", e.Block)
		}
		l := make([]expression.Expression, 0, len(e.SetList))
		for _, v := range e.SetList {
			l = append(l, v.Expr)
		}
		e.Lists = append(e.Lists, l)
	}
	return nil
}

// insertEvents processes `insert|replace into values ()` or `insert|replace into set x=y`
func insertEvents(ctx context.Context, base insertCommon) (err error) {
	e := base.insertCommon()
	// For `insert|replace into set x=y`, process the set list here.
	if err = e.processSetList(); err != nil {
		return err
	}
	sessVars := e.ctx.GetStochastikVars()
	batchSize := sessVars.DMLBatchSize
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn() && config.GetGlobalConfig().EnableBatchDML && batchSize > 0

	e.lazyFillAutoID = true
	evalEventFunc := e.fastEvalEvent
	if !e.allAssignmentsAreConstant {
		evalEventFunc = e.evalEvent
	}

	rows := make([][]types.Causet, 0, len(e.Lists))
	memUsageOfEvents := int64(0)
	memTracker := e.memTracker
	for i, list := range e.Lists {
		e.rowCount++
		var event []types.Causet
		event, err = evalEventFunc(ctx, list, i)
		if err != nil {
			return err
		}
		rows = append(rows, event)
		if batchInsert && e.rowCount%uint64(batchSize) == 0 {
			memUsageOfEvents = types.EstimatedMemUsage(rows[0], len(rows))
			memTracker.Consume(memUsageOfEvents)
			// Before batch insert, fill the batch allocated autoIDs.
			rows, err = e.lazyAdjustAutoIncrementCauset(ctx, rows)
			if err != nil {
				return err
			}
			if err = base.exec(ctx, rows); err != nil {
				return err
			}
			rows = rows[:0]
			memTracker.Consume(-memUsageOfEvents)
			memUsageOfEvents = 0
			if err = e.doBatchInsert(ctx); err != nil {
				return err
			}
		}
	}
	if len(rows) != 0 {
		memUsageOfEvents = types.EstimatedMemUsage(rows[0], len(rows))
		memTracker.Consume(memUsageOfEvents)
	}
	// Fill the batch allocated autoIDs.
	rows, err = e.lazyAdjustAutoIncrementCauset(ctx, rows)
	if err != nil {
		return err
	}
	err = base.exec(ctx, rows)
	if err != nil {
		return err
	}
	memTracker.Consume(-memUsageOfEvents)
	return nil
}

func (e *InsertValues) handleErr(defCaus *block.DeferredCauset, val *types.Causet, rowIdx int, err error) error {
	if err == nil {
		return nil
	}

	// Convert the error with full messages.
	var (
		defCausTp   byte
		defCausName string
	)
	if defCaus != nil {
		defCausTp = defCaus.Tp
		defCausName = defCaus.Name.String()
	}

	if types.ErrDataTooLong.Equal(err) {
		err = resetErrDataTooLong(defCausName, rowIdx+1, err)
	} else if types.ErrOverflow.Equal(err) {
		err = types.ErrWarnDataOutOfRange.GenWithStackByArgs(defCausName, rowIdx+1)
	} else if types.ErrTruncated.Equal(err) {
		err = types.ErrTruncated.GenWithStackByArgs(defCausName, rowIdx+1)
	} else if types.ErrTruncatedWrongVal.Equal(err) || types.ErrWrongValue.Equal(err) {
		valStr, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Warn("truncate value failed", zap.Error(err1))
		}
		err = block.ErrTruncatedWrongValueForField.GenWithStackByArgs(types.TypeStr(defCausTp), valStr, defCausName, rowIdx+1)
	}

	if !e.ctx.GetStochastikVars().StmtCtx.DupKeyAsWarning {
		return err
	}
	// TODO: should not filter all types of errors here.
	e.handleWarning(err)
	return nil
}

// evalEvent evaluates a to-be-inserted event. The value of the defCausumn may base on another defCausumn,
// so we use setValueForRefDeferredCauset to fill the empty event some default values when needFillDefaultValues is true.
func (e *InsertValues) evalEvent(ctx context.Context, list []expression.Expression, rowIdx int) ([]types.Causet, error) {
	rowLen := len(e.Block.DefCauss())
	if e.hasExtraHandle {
		rowLen++
	}
	event := make([]types.Causet, rowLen)
	hasValue := make([]bool, rowLen)

	// For statements like `insert into t set a = b + 1`.
	if e.hasRefDefCauss {
		if err := e.setValueForRefDeferredCauset(event, hasValue); err != nil {
			return nil, err
		}
	}

	e.evalBuffer.SetCausets(event...)
	for i, expr := range list {
		val, err := expr.Eval(e.evalBuffer.ToEvent())
		if err = e.handleErr(e.insertDeferredCausets[i], &val, rowIdx, err); err != nil {
			return nil, err
		}
		val1, err := block.CastValue(e.ctx, val, e.insertDeferredCausets[i].ToInfo(), false, false)
		if err = e.handleErr(e.insertDeferredCausets[i], &val, rowIdx, err); err != nil {
			return nil, err
		}

		offset := e.insertDeferredCausets[i].Offset
		val1.INTERLOCKy(&event[offset])
		hasValue[offset] = true
		e.evalBuffer.SetCauset(offset, val1)
	}
	// Event may lack of generated defCausumn, autoIncrement defCausumn, empty defCausumn here.
	return e.fillEvent(ctx, event, hasValue)
}

var emptyEvent chunk.Event

func (e *InsertValues) fastEvalEvent(ctx context.Context, list []expression.Expression, rowIdx int) ([]types.Causet, error) {
	rowLen := len(e.Block.DefCauss())
	if e.hasExtraHandle {
		rowLen++
	}
	event := make([]types.Causet, rowLen)
	hasValue := make([]bool, rowLen)
	for i, expr := range list {
		con := expr.(*expression.Constant)
		val, err := con.Eval(emptyEvent)
		if err = e.handleErr(e.insertDeferredCausets[i], &val, rowIdx, err); err != nil {
			return nil, err
		}
		val1, err := block.CastValue(e.ctx, val, e.insertDeferredCausets[i].ToInfo(), false, false)
		if err = e.handleErr(e.insertDeferredCausets[i], &val, rowIdx, err); err != nil {
			return nil, err
		}
		offset := e.insertDeferredCausets[i].Offset
		event[offset], hasValue[offset] = val1, true
	}
	return e.fillEvent(ctx, event, hasValue)
}

// setValueForRefDeferredCauset set some default values for the event to eval the event value with other defCausumns,
// it follows these rules:
//     1. for nullable and no default value defCausumn, use NULL.
//     2. for nullable and have default value defCausumn, use it's default value.
//     3. for not null defCausumn, use zero value even in strict mode.
//     4. for auto_increment defCausumn, use zero value.
//     5. for generated defCausumn, use NULL.
func (e *InsertValues) setValueForRefDeferredCauset(event []types.Causet, hasValue []bool) error {
	for i, c := range e.Block.DefCauss() {
		d, err := e.getDefCausDefaultValue(i, c)
		if err == nil {
			event[i] = d
			if !allegrosql.HasAutoIncrementFlag(c.Flag) {
				// It is an interesting behavior in MyALLEGROSQL.
				// If the value of auto ID is not explicit, MyALLEGROSQL use 0 value for auto ID when it is
				// evaluated by another defCausumn, but it should be used once only.
				// When we fill it as an auto ID defCausumn, it should be set as it used to be.
				// So just keep `hasValue` false for auto ID, and the others set true.
				hasValue[c.Offset] = true
			}
		} else if block.ErrNoDefaultValue.Equal(err) {
			event[i] = block.GetZeroValue(c.ToInfo())
			hasValue[c.Offset] = false
		} else if e.handleErr(c, &d, 0, err) != nil {
			return err
		}
	}
	return nil
}

func insertEventsFromSelect(ctx context.Context, base insertCommon) error {
	// process `insert|replace into ... select ... from ...`
	e := base.insertCommon()
	selectExec := e.children[0]
	fields := retTypes(selectExec)
	chk := newFirstChunk(selectExec)
	iter := chunk.NewIterator4Chunk(chk)
	rows := make([][]types.Causet, 0, chk.Capacity())

	sessVars := e.ctx.GetStochastikVars()
	if !sessVars.StrictALLEGROSQLMode {
		// If StrictALLEGROSQLMode is disabled and it is a insert-select statement, it also handle BadNullAsWarning.
		sessVars.StmtCtx.BadNullAsWarning = true
	}
	batchSize := sessVars.DMLBatchSize
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn() && config.GetGlobalConfig().EnableBatchDML && batchSize > 0
	memUsageOfEvents := int64(0)
	memTracker := e.memTracker
	for {
		err := Next(ctx, selectExec, chk)
		if err != nil {
			return err
		}
		if chk.NumEvents() == 0 {
			break
		}
		chkMemUsage := chk.MemoryUsage()
		memTracker.Consume(chkMemUsage)
		for innerChunkEvent := iter.Begin(); innerChunkEvent != iter.End(); innerChunkEvent = iter.Next() {
			innerEvent := innerChunkEvent.GetCausetEvent(fields)
			e.rowCount++
			event, err := e.getEvent(ctx, innerEvent)
			if err != nil {
				return err
			}
			rows = append(rows, event)
			if batchInsert && e.rowCount%uint64(batchSize) == 0 {
				memUsageOfEvents = types.EstimatedMemUsage(rows[0], len(rows))
				memTracker.Consume(memUsageOfEvents)
				if err = base.exec(ctx, rows); err != nil {
					return err
				}
				rows = rows[:0]
				memTracker.Consume(-memUsageOfEvents)
				memUsageOfEvents = 0
				if err = e.doBatchInsert(ctx); err != nil {
					return err
				}
			}
		}

		if len(rows) != 0 {
			memUsageOfEvents = types.EstimatedMemUsage(rows[0], len(rows))
			memTracker.Consume(memUsageOfEvents)
		}
		err = base.exec(ctx, rows)
		if err != nil {
			return err
		}
		rows = rows[:0]
		memTracker.Consume(-memUsageOfEvents)
		memTracker.Consume(-chkMemUsage)
	}
	return nil
}

func (e *InsertValues) doBatchInsert(ctx context.Context) error {
	e.ctx.StmtCommit()
	if err := e.ctx.NewTxn(ctx); err != nil {
		// We should return a special error for batch insert.
		return ErrBatchInsertFail.GenWithStack("BatchInsert failed with error: %v", err)
	}
	return nil
}

// getEvent gets the event which from `insert into select from` or `load data`.
// The input values from these two statements are datums instead of
// expressions which are used in `insert into set x=y`.
func (e *InsertValues) getEvent(ctx context.Context, vals []types.Causet) ([]types.Causet, error) {
	event := make([]types.Causet, len(e.Block.DefCauss()))
	hasValue := make([]bool, len(e.Block.DefCauss()))
	for i, v := range vals {
		casted, err := block.CastValue(e.ctx, v, e.insertDeferredCausets[i].ToInfo(), false, false)
		if e.handleErr(nil, &v, 0, err) != nil {
			return nil, err
		}

		offset := e.insertDeferredCausets[i].Offset
		event[offset] = casted
		hasValue[offset] = true
	}

	return e.fillEvent(ctx, event, hasValue)
}

// getDefCausDefaultValue gets the defCausumn default value.
func (e *InsertValues) getDefCausDefaultValue(idx int, defCaus *block.DeferredCauset) (d types.Causet, err error) {
	if !defCaus.DefaultIsExpr && e.defCausDefaultVals != nil && e.defCausDefaultVals[idx].valid {
		return e.defCausDefaultVals[idx].val, nil
	}

	var defaultVal types.Causet
	if defCaus.DefaultIsExpr && defCaus.DefaultExpr != nil {
		defaultVal, err = block.EvalDefCausDefaultExpr(e.ctx, defCaus.ToInfo(), defCaus.DefaultExpr)
	} else {
		defaultVal, err = block.GetDefCausDefaultValue(e.ctx, defCaus.ToInfo())
	}
	if err != nil {
		return types.Causet{}, err
	}
	if initialized := e.lazilyInitDefCausDefaultValBuf(); initialized && !defCaus.DefaultIsExpr {
		e.defCausDefaultVals[idx].val = defaultVal
		e.defCausDefaultVals[idx].valid = true
	}

	return defaultVal, nil
}

// fillDefCausValue fills the defCausumn value if it is not set in the insert statement.
func (e *InsertValues) fillDefCausValue(ctx context.Context, causet types.Causet, idx int, defCausumn *block.DeferredCauset, hasValue bool) (types.Causet,
	error) {
	if allegrosql.HasAutoIncrementFlag(defCausumn.Flag) {
		if e.lazyFillAutoID {
			// Handle hasValue info in autoIncrement defCausumn previously for lazy handle.
			if !hasValue {
				causet.SetNull()
			}
			// CausetStore the plain causet of autoIncrement defCausumn directly for lazy handle.
			return causet, nil
		}
		d, err := e.adjustAutoIncrementCauset(ctx, causet, hasValue, defCausumn)
		if err != nil {
			return types.Causet{}, err
		}
		return d, nil
	}
	tblInfo := e.Block.Meta()
	if dbs.IsAutoRandomDeferredCausetID(tblInfo, defCausumn.ID) {
		d, err := e.adjustAutoRandomCauset(ctx, causet, hasValue, defCausumn)
		if err != nil {
			return types.Causet{}, err
		}
		return d, nil
	}
	if !hasValue {
		d, err := e.getDefCausDefaultValue(idx, defCausumn)
		if e.handleErr(defCausumn, &causet, 0, err) != nil {
			return types.Causet{}, err
		}
		return d, nil
	}
	return causet, nil
}

// fillEvent fills generated defCausumns, auto_increment defCausumn and empty defCausumn.
// For NOT NULL defCausumn, it will return error or use zero value based on sql_mode.
// When lazyFillAutoID is true, fill event will lazily handle auto increment causet for lazy batch allocation.
// `insert|replace values` can guarantee consecutive autoID in a batch.
// Other statements like `insert select from` don't guarantee consecutive autoID.
// https://dev.allegrosql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html
func (e *InsertValues) fillEvent(ctx context.Context, event []types.Causet, hasValue []bool) ([]types.Causet, error) {
	gDefCauss := make([]*block.DeferredCauset, 0)
	for i, c := range e.Block.DefCauss() {
		var err error
		// Evaluate the generated defCausumns later after real defCausumns set
		if c.IsGenerated() {
			gDefCauss = append(gDefCauss, c)
		} else {
			// Get the default value for all no value defCausumns, the auto increment defCausumn is different from the others.
			if event[i], err = e.fillDefCausValue(ctx, event[i], i, c, hasValue[i]); err != nil {
				return nil, err
			}
			if !e.lazyFillAutoID || (e.lazyFillAutoID && !allegrosql.HasAutoIncrementFlag(c.Flag)) {
				if err = c.HandleBadNull(&event[i], e.ctx.GetStochastikVars().StmtCtx); err != nil {
					return nil, err
				}
			}
		}
	}
	for i, gDefCaus := range gDefCauss {
		defCausIdx := gDefCaus.DeferredCausetInfo.Offset
		val, err := e.GenExprs[i].Eval(chunk.MutEventFromCausets(event).ToEvent())
		if e.handleErr(gDefCaus, &val, 0, err) != nil {
			return nil, err
		}
		event[defCausIdx], err = block.CastValue(e.ctx, val, gDefCaus.ToInfo(), false, false)
		if err != nil {
			return nil, err
		}
		// Handle the bad null error.
		if err = gDefCaus.HandleBadNull(&event[defCausIdx], e.ctx.GetStochastikVars().StmtCtx); err != nil {
			return nil, err
		}
	}
	return event, nil
}

// isAutoNull can help judge whether a causet is AutoIncrement Null quickly.
// This used to help lazyFillAutoIncrement to find consecutive N causet backwards for batch autoID alloc.
func (e *InsertValues) isAutoNull(ctx context.Context, d types.Causet, defCaus *block.DeferredCauset) bool {
	var err error
	var recordID int64
	if !d.IsNull() {
		recordID, err = getAutoRecordID(d, &defCaus.FieldType, true)
		if err != nil {
			return false
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		return false
	}
	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero ALLEGROALLEGROSQL mode is not set.
	if d.IsNull() || e.ctx.GetStochastikVars().ALLEGROSQLMode&allegrosql.ModeNoAutoValueOnZero == 0 {
		return true
	}
	return false
}

func (e *InsertValues) hasAutoIncrementDeferredCauset() (int, bool) {
	defCausIdx := -1
	for i, c := range e.Block.DefCauss() {
		if allegrosql.HasAutoIncrementFlag(c.Flag) {
			defCausIdx = i
			break
		}
	}
	return defCausIdx, defCausIdx != -1
}

func (e *InsertValues) lazyAdjustAutoIncrementCausetInRetry(ctx context.Context, rows [][]types.Causet, defCausIdx int) ([][]types.Causet, error) {
	// Get the autoIncrement defCausumn.
	defCaus := e.Block.DefCauss()[defCausIdx]
	// Consider the defCausIdx of autoIncrement in event are the same.
	length := len(rows)
	for i := 0; i < length; i++ {
		autoCauset := rows[i][defCausIdx]

		// autoID can be found in RetryInfo.
		retryInfo := e.ctx.GetStochastikVars().RetryInfo
		if retryInfo.Retrying {
			id, err := retryInfo.GetCurrAutoIncrementID()
			if err != nil {
				return nil, err
			}
			autoCauset.SetAutoID(id, defCaus.Flag)

			if err = defCaus.HandleBadNull(&autoCauset, e.ctx.GetStochastikVars().StmtCtx); err != nil {
				return nil, err
			}
			rows[i][defCausIdx] = autoCauset
		}
	}
	return rows, nil
}

// lazyAdjustAutoIncrementCauset is quite similar to adjustAutoIncrementCauset
// except it will cache auto increment causet previously for lazy batch allocation of autoID.
func (e *InsertValues) lazyAdjustAutoIncrementCauset(ctx context.Context, rows [][]types.Causet) ([][]types.Causet, error) {
	// Not in lazyFillAutoID mode means no need to fill.
	if !e.lazyFillAutoID {
		return rows, nil
	}
	// No autoIncrement defCausumn means no need to fill.
	defCausIdx, ok := e.hasAutoIncrementDeferredCauset()
	if !ok {
		return rows, nil
	}
	// autoID can be found in RetryInfo.
	retryInfo := e.ctx.GetStochastikVars().RetryInfo
	if retryInfo.Retrying {
		return e.lazyAdjustAutoIncrementCausetInRetry(ctx, rows, defCausIdx)
	}
	// Get the autoIncrement defCausumn.
	defCaus := e.Block.DefCauss()[defCausIdx]
	// Consider the defCausIdx of autoIncrement in event are the same.
	length := len(rows)
	for i := 0; i < length; i++ {
		autoCauset := rows[i][defCausIdx]

		var err error
		var recordID int64
		if !autoCauset.IsNull() {
			recordID, err = getAutoRecordID(autoCauset, &defCaus.FieldType, true)
			if err != nil {
				return nil, err
			}
		}
		// Use the value if it's not null and not 0.
		if recordID != 0 {
			err = e.Block.RebaseAutoID(e.ctx, recordID, true, autoid.EventIDAllocType)
			if err != nil {
				return nil, err
			}
			e.ctx.GetStochastikVars().StmtCtx.InsertID = uint64(recordID)
			retryInfo.AddAutoIncrementID(recordID)
			rows[i][defCausIdx] = autoCauset
			continue
		}

		// Change NULL to auto id.
		// Change value 0 to auto id, if NoAutoValueOnZero ALLEGROALLEGROSQL mode is not set.
		if autoCauset.IsNull() || e.ctx.GetStochastikVars().ALLEGROSQLMode&allegrosql.ModeNoAutoValueOnZero == 0 {
			// Find consecutive num.
			start := i
			cnt := 1
			for i+1 < length && e.isAutoNull(ctx, rows[i+1][defCausIdx], defCaus) {
				i++
				cnt++
			}
			// AllocBatchAutoIncrementValue allocates batch N consecutive autoIDs.
			// The max value can be derived from adding the increment value to min for cnt-1 times.
			min, increment, err := block.AllocBatchAutoIncrementValue(ctx, e.Block, e.ctx, cnt)
			if e.handleErr(defCaus, &autoCauset, cnt, err) != nil {
				return nil, err
			}
			// It's compatible with allegrosql setting the first allocated autoID to lastInsertID.
			// Cause autoID may be specified by user, judge only the first event is not suiblock.
			if e.lastInsertID == 0 {
				e.lastInsertID = uint64(min)
			}
			// Assign autoIDs to rows.
			for j := 0; j < cnt; j++ {
				offset := j + start
				d := rows[offset][defCausIdx]

				id := int64(uint64(min) + uint64(j)*uint64(increment))
				d.SetAutoID(id, defCaus.Flag)
				retryInfo.AddAutoIncrementID(id)

				// The value of d is adjusted by auto ID, so we need to cast it again.
				d, err := block.CastValue(e.ctx, d, defCaus.ToInfo(), false, false)
				if err != nil {
					return nil, err
				}
				rows[offset][defCausIdx] = d
			}
			continue
		}

		autoCauset.SetAutoID(recordID, defCaus.Flag)
		retryInfo.AddAutoIncrementID(recordID)

		// the value of d is adjusted by auto ID, so we need to cast it again.
		autoCauset, err = block.CastValue(e.ctx, autoCauset, defCaus.ToInfo(), false, false)
		if err != nil {
			return nil, err
		}
		rows[i][defCausIdx] = autoCauset
	}
	return rows, nil
}

func (e *InsertValues) adjustAutoIncrementCauset(ctx context.Context, d types.Causet, hasValue bool, c *block.DeferredCauset) (types.Causet, error) {
	retryInfo := e.ctx.GetStochastikVars().RetryInfo
	if retryInfo.Retrying {
		id, err := retryInfo.GetCurrAutoIncrementID()
		if err != nil {
			return types.Causet{}, err
		}
		d.SetAutoID(id, c.Flag)
		return d, nil
	}

	var err error
	var recordID int64
	if !hasValue {
		d.SetNull()
	}
	if !d.IsNull() {
		recordID, err = getAutoRecordID(d, &c.FieldType, true)
		if err != nil {
			return types.Causet{}, err
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		err = e.Block.RebaseAutoID(e.ctx, recordID, true, autoid.EventIDAllocType)
		if err != nil {
			return types.Causet{}, err
		}
		e.ctx.GetStochastikVars().StmtCtx.InsertID = uint64(recordID)
		retryInfo.AddAutoIncrementID(recordID)
		return d, nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero ALLEGROALLEGROSQL mode is not set.
	if d.IsNull() || e.ctx.GetStochastikVars().ALLEGROSQLMode&allegrosql.ModeNoAutoValueOnZero == 0 {
		recordID, err = block.AllocAutoIncrementValue(ctx, e.Block, e.ctx)
		if e.handleErr(c, &d, 0, err) != nil {
			return types.Causet{}, err
		}
		// It's compatible with allegrosql setting the first allocated autoID to lastInsertID.
		// Cause autoID may be specified by user, judge only the first event is not suiblock.
		if e.lastInsertID == 0 {
			e.lastInsertID = uint64(recordID)
		}
	}

	d.SetAutoID(recordID, c.Flag)
	retryInfo.AddAutoIncrementID(recordID)

	// the value of d is adjusted by auto ID, so we need to cast it again.
	casted, err := block.CastValue(e.ctx, d, c.ToInfo(), false, false)
	if err != nil {
		return types.Causet{}, err
	}
	return casted, nil
}

func getAutoRecordID(d types.Causet, target *types.FieldType, isInsert bool) (int64, error) {
	var recordID int64

	switch target.Tp {
	case allegrosql.TypeFloat, allegrosql.TypeDouble:
		f := d.GetFloat64()
		if isInsert {
			recordID = int64(math.Round(f))
		} else {
			recordID = int64(f)
		}
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong:
		recordID = d.GetInt64()
	default:
		return 0, errors.Errorf("unexpected field type [%v]", target.Tp)
	}

	return recordID, nil
}

func (e *InsertValues) adjustAutoRandomCauset(ctx context.Context, d types.Causet, hasValue bool, c *block.DeferredCauset) (types.Causet, error) {
	retryInfo := e.ctx.GetStochastikVars().RetryInfo
	if retryInfo.Retrying {
		autoRandomID, err := retryInfo.GetCurrAutoRandomID()
		if err != nil {
			return types.Causet{}, err
		}
		d.SetAutoID(autoRandomID, c.Flag)
		return d, nil
	}

	var err error
	var recordID int64
	if !hasValue {
		d.SetNull()
	}
	if !d.IsNull() {
		recordID, err = getAutoRecordID(d, &c.FieldType, true)
		if err != nil {
			return types.Causet{}, err
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		if !e.ctx.GetStochastikVars().AllowAutoRandExplicitInsert {
			return types.Causet{}, dbs.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomExplicitInsertDisabledErrMsg)
		}
		err = e.rebaseAutoRandomID(recordID, &c.FieldType)
		if err != nil {
			return types.Causet{}, err
		}
		e.ctx.GetStochastikVars().StmtCtx.InsertID = uint64(recordID)
		d.SetAutoID(recordID, c.Flag)
		retryInfo.AddAutoRandomID(recordID)
		return d, nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero ALLEGROALLEGROSQL mode is not set.
	if d.IsNull() || e.ctx.GetStochastikVars().ALLEGROSQLMode&allegrosql.ModeNoAutoValueOnZero == 0 {
		_, err := e.ctx.Txn(true)
		if err != nil {
			return types.Causet{}, errors.Trace(err)
		}
		recordID, err = e.allocAutoRandomID(&c.FieldType)
		if err != nil {
			return types.Causet{}, err
		}
		// It's compatible with allegrosql setting the first allocated autoID to lastInsertID.
		// Cause autoID may be specified by user, judge only the first event is not suiblock.
		if e.lastInsertID == 0 {
			e.lastInsertID = uint64(recordID)
		}
	}

	d.SetAutoID(recordID, c.Flag)
	retryInfo.AddAutoRandomID(recordID)

	casted, err := block.CastValue(e.ctx, d, c.ToInfo(), false, false)
	if err != nil {
		return types.Causet{}, err
	}
	return casted, nil
}

// allocAutoRandomID allocates a random id for primary key defCausumn. It assumes blockInfo.AutoRandomBits > 0.
func (e *InsertValues) allocAutoRandomID(fieldType *types.FieldType) (int64, error) {
	alloc := e.Block.SlabPredictors(e.ctx).Get(autoid.AutoRandomType)
	blockInfo := e.Block.Meta()
	increment := e.ctx.GetStochastikVars().AutoIncrementIncrement
	offset := e.ctx.GetStochastikVars().AutoIncrementOffset
	_, autoRandomID, err := alloc.Alloc(blockInfo.ID, 1, int64(increment), int64(offset))
	if err != nil {
		return 0, err
	}

	layout := autoid.NewAutoRandomIDLayout(fieldType, blockInfo.AutoRandomBits)
	if blocks.OverflowShardBits(autoRandomID, blockInfo.AutoRandomBits, layout.TypeBitsLength, layout.HasSignBit) {
		return 0, autoid.ErrAutoRandReadFailed
	}
	shard := e.ctx.GetStochastikVars().TxnCtx.GetShard(blockInfo.AutoRandomBits, layout.TypeBitsLength, layout.HasSignBit, 1)
	autoRandomID |= shard
	return autoRandomID, nil
}

func (e *InsertValues) rebaseAutoRandomID(recordID int64, fieldType *types.FieldType) error {
	if recordID < 0 {
		return nil
	}
	alloc := e.Block.SlabPredictors(e.ctx).Get(autoid.AutoRandomType)
	blockInfo := e.Block.Meta()

	layout := autoid.NewAutoRandomIDLayout(fieldType, blockInfo.AutoRandomBits)
	autoRandomID := layout.IncrementalMask() & recordID

	return alloc.Rebase(blockInfo.ID, autoRandomID, true)
}

func (e *InsertValues) handleWarning(err error) {
	sc := e.ctx.GetStochastikVars().StmtCtx
	sc.AppendWarning(err)
}

func (e *InsertValues) defCauslectRuntimeStatsEnabled() bool {
	if e.runtimeStats != nil {
		if e.stats == nil {
			snapshotStats := &einsteindb.SnapshotRuntimeStats{}
			e.stats = &runtimeStatsWithSnapshot{
				SnapshotRuntimeStats: snapshotStats,
			}
			e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, e.stats)
		}
		return true
	}
	return false
}

// batchCheckAndInsert checks rows with duplicate errors.
// All duplicate rows will be ignored and appended as duplicate warnings.
func (e *InsertValues) batchCheckAndInsert(ctx context.Context, rows [][]types.Causet, addRecord func(ctx context.Context, event []types.Causet) error) error {
	// all the rows will be checked, so it is safe to set BatchCheck = true
	e.ctx.GetStochastikVars().StmtCtx.BatchCheck = true

	// Get keys need to be checked.
	toBeCheckedEvents, err := getKeysNeedCheck(ctx, e.ctx, e.Block, rows)
	if err != nil {
		return err
	}

	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}

	if e.defCauslectRuntimeStatsEnabled() {
		if snapshot := txn.GetSnapshot(); snapshot != nil {
			snapshot.SetOption(ekv.DefCauslectRuntimeStats, e.stats.SnapshotRuntimeStats)
			defer snapshot.DelOption(ekv.DefCauslectRuntimeStats)
		}
	}

	// Fill cache using BatchGet, the following Get requests don't need to visit EinsteinDB.
	if _, err = prefetchUniqueIndices(ctx, txn, toBeCheckedEvents); err != nil {
		return err
	}

	// append warnings and get no duplicated error rows
	for i, r := range toBeCheckedEvents {
		skip := false
		if r.handleKey != nil {
			_, err := txn.Get(ctx, r.handleKey.newKey)
			if err == nil {
				e.ctx.GetStochastikVars().StmtCtx.AppendWarning(r.handleKey.dupErr)
				continue
			}
			if !ekv.IsErrNotFound(err) {
				return err
			}
		}
		for _, uk := range r.uniqueKeys {
			_, err := txn.Get(ctx, uk.newKey)
			if err == nil {
				// If duplicate keys were found in BatchGet, mark event = nil.
				e.ctx.GetStochastikVars().StmtCtx.AppendWarning(uk.dupErr)
				skip = true
				break
			}
			if !ekv.IsErrNotFound(err) {
				return err
			}
		}
		// If event was checked with no duplicate keys,
		// it should be add to values map for the further event check.
		// There may be duplicate keys inside the insert statement.
		if !skip {
			e.ctx.GetStochastikVars().StmtCtx.AddINTERLOCKiedEvents(1)
			err = addRecord(ctx, rows[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *InsertValues) addRecord(ctx context.Context, event []types.Causet) error {
	return e.addRecordWithAutoIDHint(ctx, event, 0)
}

func (e *InsertValues) addRecordWithAutoIDHint(ctx context.Context, event []types.Causet, reserveAutoIDCount int) (err error) {
	vars := e.ctx.GetStochastikVars()
	if !vars.ConstraintCheckInPlace {
		vars.PresumeKeyNotExists = true
	}
	if reserveAutoIDCount > 0 {
		_, err = e.Block.AddRecord(e.ctx, event, block.WithCtx(ctx), block.WithReserveAutoIDHint(reserveAutoIDCount))
	} else {
		_, err = e.Block.AddRecord(e.ctx, event, block.WithCtx(ctx))
	}
	vars.PresumeKeyNotExists = false
	if err != nil {
		return err
	}
	if e.lastInsertID != 0 {
		vars.SetLastInsertID(e.lastInsertID)
	}
	return nil
}
