//Copyright 2020 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package dbs


import(

"context"
"math"
"strconv"
"sync/atomic"
"time"

"github.com/YosiSF/Milevanoedb/BerolinaSQL/errors"
"github.com/YosiSF/failpoint"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/BerolinaSQL/ast"
"github.com/YosiSF/BerolinaSQL/charset"
"github.com/YosiSF/BerolinaSQL/serial"
"github.com/YosiSF/BerolinaSQL/mysql"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/config"
dbsutil "github.com/YosiSF/Milevanoedb/BerolinaSQL/dbs/util"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/expression"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/schemaReplicant"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/eekv"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/meta"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/meta/autoid"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/metrics"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/causetnetctx"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/causetnetctx/variable"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/store/Einsteinnoedb"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/store/Einsteinnoedb/oracle"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/table"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/table/tables"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/tablecodec"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/types"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/util"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/util/logutil"
decoder "github.com/YosiSF/Milevanoedb/BerolinaSQL/util/rowDecoder"
"github.com/YosiSF/Milevanoedb/BerolinaSQL/util/timeutil"
"go.uber.org/zap"

)

const (
	// MaxCommentLength is exported for testing.
	MaxCommentLength = 1024
)

func buildIndexColumns(columns []*serial.ColumnInfo, indexPartSpecifications []*ast.IndexPartSpecification) ([]*serial.IndexColumn, error) {
	// Build offsets.
	idxParts := make([]*serial.IndexColumn, 0, len(indexPartSpecifications))
	var col *serial.ColumnInfo

	// The sum of length of all index columns.
	sumLength := 0
	for _, ip := range indexPartSpecifications {
		col = serial.FindColumnInfo(columns, ip.Column.Name.L)
		if col == nil {
			return nil, errKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ip.Column.Name)
		}

		if err := checkIndexColumn(col, ip); err != nil {
			return nil, err
		}

		indexColumnLength, err := getIndexColumnLength(col, ip.Length)
		if err != nil {
			return nil, err
		}
		sumLength += indexColumnLength

		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > config.GetGlobalConfig().MaxIndexLength {
			return nil, errTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
		}

		idxParts = append(idxParts, &serial.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ip.Length,
		})
	}

	return idxParts, nil
}

func checkPKOnGeneratedColumn(tblInfo *serial.TableInfo, indexPartSpecifications []*ast.IndexPartSpecification) (*serial.ColumnInfo, error) {
	var lastCol *serial.ColumnInfo
	for _, colName := range indexPartSpecifications {
		lastCol = getColumnInfoByName(tblInfo, colName.Column.Name.L)
		if lastCol == nil {
			return nil, errKeyColumnDoesNotExits.GenWithStackByArgs(colName.Column.Name)
		}
		// Virtual columns cannot be used in primary key.
		if lastCol.IsGenerated() && !lastCol.GeneratedStored {
			return nil, errUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
		}
	}

	return lastCol, nil
}

func checkIndexPrefixLength(columns []*serial.ColumnInfo, idxColumns []*serial.IndexColumn) error {
	// The sum of length of all index columns.
	sumLength := 0
	for _, ic := range idxColumns {
		col := serial.FindColumnInfo(columns, ic.Name.L)
		if col == nil {
			return errKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Name)
		}

		indexColumnLength, err := getIndexColumnLength(col, ic.Length)
		if err != nil {
			return err
		}
		sumLength += indexColumnLength
		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > config.GetGlobalConfig().MaxIndexLength {
			return errTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
		}
	}
	return nil
}

func checkIndexColumn(col *serial.ColumnInfo, ic *ast.IndexPartSpecification) error {
	if col.Flen == 0 && (types.IsTypeChar(col.FieldType.Tp) || types.IsTypeVarchar(col.FieldType.Tp)) {
		return errors.Trace(errWrongKeyColumn.GenWithStackByArgs(ic.Column.Name))
	}

	// JSON column cannot index.
	if col.FieldType.Tp == mysql.TypeJSON {
		return errors.Trace(errJSONUsedAsKey.GenWithStackByArgs(col.Name.O))
	}

	// Length must be specified and non-zero for BLOB and TEXT column indexes.
	if types.IsTypeBlob(col.FieldType.Tp) {
		if ic.Length == types.UnspecifiedLength {
			return errors.Trace(errBlobKeyWithoutLength.GenWithStackByArgs(col.Name.O))
		}
		if ic.Length == types.ErrorLength {
			return errors.Trace(errKeyPart0.GenWithStackByArgs(col.Name.O))
		}
	}

	// Length can only be specified for specifiable types.
	if ic.Length != types.UnspecifiedLength && !types.IsTypePrefixable(col.FieldType.Tp) {
		return errors.Trace(errIncorrectPrefixKey)
	}

	// Key length must be shorter or equal to the column length.
	if ic.Length != types.UnspecifiedLength &&
		types.IsTypeChar(col.FieldType.Tp) {
		if col.Flen < ic.Length {
			return errors.Trace(errIncorrectPrefixKey)
		}
		// Length must be non-zero for char.
		if ic.Length == types.ErrorLength {
			return errors.Trace(errKeyPart0.GenWithStackByArgs(col.Name.O))
		}
	}

	// Specified length must be shorter than the max length for prefix.
	if ic.Length > config.GetGlobalConfig().MaxIndexLength {
		return errTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
	}
	return nil
}

// getIndexColumnLength calculate the bytes number required in an index column.
func getIndexColumnLength(col *serial.ColumnInfo, colLen int) (int, error) {
	length := types.UnspecifiedLength
	if colLen != types.UnspecifiedLength {
		length = colLen
	} else if col.Flen != types.UnspecifiedLength {
		length = col.Flen
	}

	switch col.Tp {
	case mysql.TypeBit:
		return (length + 7) >> 3, nil
	case mysql.TypeVarchar, mysql.TypeString:
		// Different charsets occupy different numbers of bytes on each character.
		desc, err := charset.GetCharsetDesc(col.Charset)
		if err != nil {
			return 0, errUnsupportedCharset.GenWithStackByArgs(col.Charset, col.Collate)
		}
		return desc.Maxlen * length, nil
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return length, nil
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeShort:
		return mysql.DefaultLengthOfMysqlTypes[col.Tp], nil
	case mysql.TypeFloat:
		if length <= mysql.MaxFloatPrecisionLength {
			return mysql.DefaultLengthOfMysqlTypes[mysql.TypeFloat], nil
		}
		return mysql.DefaultLengthOfMysqlTypes[mysql.TypeDouble], nil
	case mysql.TypeDecimal, mysql.TypeNewDecimal:
		return calcBytesLengthForDecimal(length), nil
	case mysql.TypeYear, mysql.TypeDate, mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeTimestamp:
		return mysql.DefaultLengthOfMysqlTypes[col.Tp], nil
	default:
		return length, nil
	}
}

// Decimal using a binary format that packs nine decimal (base 10) digits into four bytes.
func calcBytesLengthForDecimal(m int) int {
	return (m / 9 * 4) + ((m%9)+1)/2
}

func buildIndexInfo(tblInfo *serial.TableInfo, indexName serial.CIStr, indexPartSpecifications []*ast.IndexPartSpecification, state serial.SchemaState) (*serial.IndexInfo, error) {
	if err := checkTooLongIndex(indexName); err != nil {
		return nil, errors.Trace(err)
	}

	idxColumns, err := buildIndexColumns(tblInfo.Columns, indexPartSpecifications)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create index info.
	idxInfo := &serial.IndexInfo{
		Name:    indexName,
		Columns: idxColumns,
		State:   state,
	}
	return idxInfo, nil
}

func addIndexColumnFlag(tblInfo *serial.TableInfo, indexInfo *serial.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.Columns {
			tblInfo.Columns[col.Offset].Flag |= mysql.PriKeyFlag
		}
		return
	}

	col := indexInfo.Columns[0]
	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].Flag |= mysql.UniqueKeyFlag
	} else {
		tblInfo.Columns[col.Offset].Flag |= mysql.MultipleKeyFlag
	}
}

func dropIndexColumnFlag(tblInfo *serial.TableInfo, indexInfo *serial.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.Columns {
			tblInfo.Columns[col.Offset].Flag &= ^mysql.PriKeyFlag
		}
	} else if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[indexInfo.Columns[0].Offset].Flag &= ^mysql.UniqueKeyFlag
	} else {
		tblInfo.Columns[indexInfo.Columns[0].Offset].Flag &= ^mysql.MultipleKeyFlag
	}

	col := indexInfo.Columns[0]
	// other index may still cover this col
	for _, index := range tblInfo.Indices {
		if index.Name.L == indexInfo.Name.L {
			continue
		}

		if index.Columns[0].Name.L != col.Name.L {
			continue
		}

		addIndexColumnFlag(tblInfo, index)
	}
}

func validateRenameIndex(from, to serial.CIStr, tbl *serial.TableInfo) (ignore bool, err error) {
	if fromIdx := tbl.FindIndexByName(from.L); fromIdx == nil {
		return false, errors.Trace(schemaReplicant.ErrKeyNotExists.GenWithStackByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	if from.O == to.O {
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	if toIdx := tbl.FindIndexByName(to.L); toIdx != nil && from.L != to.L {
		return false, errors.Trace(schemaReplicant.ErrKeyNameDuplicate.GenWithStackByArgs(toIdx.Name.O))
	}
	return false, nil
}

func onRenameIndex(t *meta.Meta, job *serial.Job) (ver int64, _ error) {
	tblInfo, from, to, err := checkRenameIndex(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	idx := tblInfo.FindIndexByName(from.L)
	idx.Name = to
	if ver, err = updateVersionAndTableInfo(t, job, tblInfo, true); err != nil {
		job.State = serial.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(serial.JobStateDone, serial.StatePublic, ver, tblInfo)
	return ver, nil
}

func getNullColInfos(tblInfo *serial.TableInfo, indexInfo *serial.IndexInfo) ([]*serial.ColumnInfo, error) {
	nullCols := make([]*serial.ColumnInfo, 0, len(indexInfo.Columns))
	for _, colName := range indexInfo.Columns {
		col := serial.FindColumnInfo(tblInfo.Columns, colName.Name.L)
		if !mysql.HasNotNullFlag(col.Flag) || mysql.HasPreventNullInsertFlag(col.Flag) {
			nullCols = append(nullCols, col)
		}
	}
	return nullCols, nil
}

func checkPrimaryKeyNotNull(w *worker, sqlMode mysql.SQLMode, t *meta.Meta, job *serial.Job,
	tblInfo *serial.TableInfo, indexInfo *serial.IndexInfo) (warnings []string, err error) {
	if !indexInfo.Primary {
		return nil, nil
	}

	noedbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return nil, err
	}
	nullCols, err := getNullColInfos(tblInfo, indexInfo)
	if err != nil {
		return nil, err
	}
	if len(nullCols) == 0 {
		return nil, nil
	}

	err = modifyColsFromNull2NotNull(w, noedbInfo, tblInfo, nullCols, serial.NewCIStr(""), false)
	if err == nil {
		return nil, nil
	}
	_, err = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, err)
	// TODO: Support non-strict mode.
	// warnings = append(warnings, ErrWarnDataTruncated.GenWithStackByArgs(oldCol.Name.L, 0).Error())
	return nil, err
}

func updateHiddenColumns(tblInfo *serial.TableInfo, idxInfo *serial.IndexInfo, state serial.SchemaState) {
	for _, col := range idxInfo.Columns {
		if tblInfo.Columns[col.Offset].Hidden {
			tblInfo.Columns[col.Offset].State = state
		}
	}
}

func (w *worker) onCreateIndex(d *dbsCtx, t *meta.Meta, job *serial.Job, isPK bool) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		unique                  bool
		indexName               serial.CIStr
		indexPartSpecifications []*ast.IndexPartSpecification
		indexOption             *ast.IndexOption
		sqlMode                 mysql.SQLMode
		warnings                []string
		hiddenCols              []*serial.ColumnInfo
	)
	if isPK {
		// Notice: sqlMode and warnings is used to support non-strict mode.
		err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption, &sqlMode, &warnings)
	} else {
		err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption, &hiddenCols)
	}
	if err != nil {
		job.State = serial.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo != nil && indexInfo.State == serial.StatePublic {
		job.State = serial.JobStateCancelled
		err = ErrDupKeyName.GenWithStack("index already exist %s", indexName)
		if isPK {
			err = schemaReplicant.ErrMultiplePriKey
		}
		return ver, err
	}
	for _, hiddenCol := range hiddenCols {
		columnInfo := serial.FindColumnInfo(tblInfo.Columns, hiddenCol.Name.L)
		if columnInfo != nil && columnInfo.State == serial.StatePublic {
			// We already have a column with the same column name.
			job.State = serial.JobStateCancelled
			// TODO: refine the error message
			return ver, schemaReplicant.ErrColumnExists.GenWithStackByArgs(hiddenCol.Name)
		}
	}

	if indexInfo == nil {
		if len(hiddenCols) > 0 {
			pos := &ast.ColumnPosition{Tp: ast.ColumnPositionNone}
			for _, hiddenCol := range hiddenCols {
				_, _, _, err = createColumnInfo(tblInfo, hiddenCol, pos)
				if err != nil {
					job.State = serial.JobStateCancelled
					return ver, errors.Trace(err)
				}
			}
		}
		if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
			job.State = serial.JobStateCancelled
			return ver, errors.Trace(err)
		}
		indexInfo, err = buildIndexInfo(tblInfo, indexName, indexPartSpecifications, serial.StateNone)
		if err != nil {
			job.State = serial.JobStateCancelled
			return ver, errors.Trace(err)
		}
		if indexOption != nil {
			indexInfo.Comment = indexOption.Comment
			if indexOption.Visibility == ast.IndexVisibilityInvisible {
				indexInfo.Invisible = true
			}
			if indexOption.Tp == serial.IndexTypeInvalid {
				// Use btree as default index type.
				indexInfo.Tp = serial.IndexTypeBtree
			} else {
				indexInfo.Tp = indexOption.Tp
			}
		} else {
			// Use btree as default index type.
			indexInfo.Tp = serial.IndexTypeBtree
		}
		indexInfo.Primary = false
		if isPK {
			if _, err = checkPKOnGeneratedColumn(tblInfo, indexPartSpecifications); err != nil {
				job.State = serial.JobStateCancelled
				return ver, err
			}
			indexInfo.Primary = true
		}
		indexInfo.Unique = unique
		indexInfo.ID = allocateIndexID(tblInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
		logutil.BgLogger().Info("[dbs] run add index job", zap.String("job", job.String()), zap.Reflect("indexInfo", indexInfo))
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case serial.StateNone:
		// none -> delete only
		job.SchemaState = serial.StateDeleteOnly
		indexInfo.State = serial.StateDeleteOnly
		updateHiddenColumns(tblInfo, indexInfo, serial.StateDeleteOnly)
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != indexInfo.State)
		metrics.AddIndexProgress.Set(0)
	case serial.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = serial.StateWriteOnly
		indexInfo.State = serial.StateWriteOnly
		updateHiddenColumns(tblInfo, indexInfo, serial.StateWriteOnly)
		_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case serial.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = serial.StateWriteReorganization
		indexInfo.State = serial.StateWriteReorganization
		updateHiddenColumns(tblInfo, indexInfo, serial.StateWriteReorganization)
		_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case serial.StateWriteReorganization:
		// reorganization -> public
		updateHiddenColumns(tblInfo, indexInfo, serial.StatePublic)
		tbl, err := getTable(d.store, schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		reorgInfo, err := getReorgInfo(d, t, job, tbl)
		if err != nil || reorgInfo.first {
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return ver, errors.Trace(err)
		}

		err = w.runReorgJob(t, reorgInfo, tbl.Meta(), d.lease, func() (addIndexErr error) {
			defer func() {
				r := recover()
				if r != nil {
					buf := util.GetStack()
					logutil.BgLogger().Error("[dbs] add table index panic", zap.Any("panic", r), zap.String("stack", string(buf)))
					metrics.PanicCounter.WithLabelValues(metrics.Labeldbs).Inc()
					addIndexErr = errCancelleddbsJob.GenWithStack("add table `%v` index `%v` panic", tblInfo.Name, indexInfo.Name)
				}
			}()
			return w.addTableIndex(tbl, indexInfo, reorgInfo)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// if timeout, we should return, check for the keywatcher and re-wait job done.
				return ver, nil
			}
			if ekv.ErrKeyExists.Equal(err) || errCancelleddbsJob.Equal(err) || errCantDecodeIndex.Equal(err) {
				logutil.BgLogger().Warn("[dbs] run add index job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
				ver, err = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, err)
			}
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			w.reorgCtx.cleanNotifyReorgCancel()
			return ver, errors.Trace(err)
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		w.reorgCtx.cleanNotifyReorgCancel()

		indexInfo.State = serial.StatePublic
		// Set column index flag.
		addIndexColumnFlag(tblInfo, indexInfo)
		if isPK {
			if err = updateColsNull2NotNull(tblInfo, indexInfo); err != nil {
				return ver, errors.Trace(err)
			}
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(serial.JobStateDone, serial.StatePublic, ver, tblInfo)
	default:
		err = ErrInvaliddbsState.GenWithStackByArgs("index", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func onDropIndex(t *meta.Meta, job *serial.Job) (ver int64, _ error) {
	tblInfo, indexInfo, err := checkDropIndex(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	dependentHiddenCols := make([]*serial.ColumnInfo, 0)
	for _, indexColumn := range indexInfo.Columns {
		if tblInfo.Columns[indexColumn.Offset].Hidden {
			dependentHiddenCols = append(dependentHiddenCols, tblInfo.Columns[indexColumn.Offset])
		}
	}

	originalState := indexInfo.State
	switch indexInfo.State {
	case serial.StatePublic:
		// public -> write only
		job.SchemaState = serial.StateWriteOnly
		indexInfo.State = serial.StateWriteOnly
		if len(dependentHiddenCols) > 0 {
			firstHiddenOffset := dependentHiddenCols[0].Offset
			for i := 0; i < len(dependentHiddenCols); i++ {
				tblInfo.Columns[firstHiddenOffset].State = serial.StateWriteOnly
				// Set this column's offset to the last and reset all following columns' offsets.
				adjustColumnInfoIncontext(tblInfo, firstHiddenOffset)
			}
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case serial.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = serial.StateDeleteOnly
		indexInfo.State = serial.StateDeleteOnly
		updateHiddenColumns(tblInfo, indexInfo, serial.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case serial.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = serial.StateDeleteReorganization
		indexInfo.State = serial.StateDeleteReorganization
		updateHiddenColumns(tblInfo, indexInfo, serial.StateDeleteReorganization)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case serial.StateDeleteReorganization:
		// reorganization -> absent
		newIndices := make([]*serial.IndexInfo, 0, len(tblInfo.Indices))
		for _, idx := range tblInfo.Indices {
			if idx.Name.L != indexInfo.Name.L {
				newIndices = append(newIndices, idx)
			}
		}
		tblInfo.Indices = newIndices
		// Set column index flag.
		dropIndexColumnFlag(tblInfo, indexInfo)

		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-len(dependentHiddenCols)]

		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != serial.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(serial.JobStateRollbackDone, serial.StateNone, ver, tblInfo)
			job.Args[0] = indexInfo.ID
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
		} else {
			job.FinishTableJob(serial.JobStateDone, serial.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexInfo.ID, getPartitionIDs(tblInfo))
		}
	default:
		err = ErrInvaliddbsState.GenWithStackByArgs("index", indexInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropIndex(t *meta.Meta, job *serial.Job) (*serial.TableInfo, *serial.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var indexName serial.CIStr
	if err = job.DecodeArgs(&indexName); err != nil {
		job.State = serial.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo == nil {
		job.State = serial.JobStateCancelled
		return nil, nil, ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	// Double check for drop index on auto_increment column.
	err = checkDropIndexOnAutoIncrementColumn(tblInfo, indexInfo)
	if err != nil {
		job.State = serial.JobStateCancelled
		return nil, nil, autoid.ErrWrongAutoKey
	}

	return tblInfo, indexInfo, nil
}

func checkDropIndexOnAutoIncrementColumn(tblInfo *serial.TableInfo, indexInfo *serial.IndexInfo) error {
	cols := tblInfo.Columns
	for _, idxCol := range indexInfo.Columns {
		flag := cols[idxCol.Offset].Flag
		if !mysql.HasAutoIncrementFlag(flag) {
			continue
		}
		// check the count of index on auto_increment column.
		count := 0
		for _, idx := range tblInfo.Indices {
			for _, c := range idx.Columns {
				if c.Name.L == idxCol.Name.L {
					count++
					break
				}
			}
		}
		if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(flag) {
			count++
		}
		if count < 2 {
			return autoid.ErrWrongAutoKey
		}
	}
	return nil
}

func checkRenameIndex(t *meta.Meta, job *serial.Job) (*serial.TableInfo, serial.CIStr, serial.CIStr, error) {
	var from, to serial.CIStr
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, from, to, errors.Trace(err)
	}

	if err := job.DecodeArgs(&from, &to); err != nil {
		job.State = serial.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}

	// Double check. See function `RenameIndex` in dbs_api.go
	duplicate, err := validateRenameIndex(from, to, tblInfo)
	if duplicate {
		return nil, from, to, nil
	}
	if err != nil {
		job.State = serial.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	return tblInfo, from, to, errors.Trace(err)
}

const (
	// DefaultTaskHandleCnt is default batch size of adding indices.
	DefaultTaskHandleCnt = 128
)

// indexRecord is the record information of an index.
type indexRecord struct {
	handle int64
	key    []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals   []types.Datum // It's the index values.
	skip   bool          // skip indicates that the index key is already exists, we should not add it.
}

type addIndexWorker struct {
	id        int
	dbsWorker *worker
	batchCnt  int
	sessCtx   causetnetctx.contextctx
	taskCh    chan *reorgIndexTask
	resultCh  chan *addIndexResult
	index     table.Index
	table     table.Table
	closed    bool
	priority  int

	// The following attributes are used to reduce memory allocation.
	defaultVals        []types.Datum
	idxRecords         []*indexRecord
	rowMap             map[int64]types.Datum
	rowDecoder         *decoder.RowDecoder
	idxKeyBufs         [][]byte
	batchCheckKeys     []ekv.Key
	distinctCheckFlags []bool
}

type reorgIndexTask struct {
	physicalTableID int64
	startHandle     int64
	endHandle       int64
	// endIncluded indicates whether the range include the endHandle.
	// When the last handle is math.MaxInt64, set endIncluded to true to
	// tell worker backfilling index of endHandle.
	endIncluded bool
}

func (r *reorgIndexTask) String() string {
	rightParenthesis := ")"
	if r.endIncluded {
		rightParenthesis = "]"
	}
	return "physicalTableID" + strconv.FormatInt(r.physicalTableID, 10) + "_" + "[" + strconv.FormatInt(r.startHandle, 10) + "," + strconv.FormatInt(r.endHandle, 10) + rightParenthesis
}

type addIndexResult struct {
	addedCount int
	scanCount  int
	nextHandle int64
	err        error
}

// addIndexTaskcontextctx is the context of the batch adding indices.
// After finishing the batch adding indices, result in addIndexTaskcontextctx will be merged into addIndexResult.
type addIndexTaskcontextctx struct {
	nextHandle int64
	done       bool
	addedCount int
	scanCount  int
}

// mergeAddIndexCtxToResult merge partial result in taskCtx into result.
func mergeAddIndexCtxToResult(taskCtx *addIndexTaskcontextctx, result *addIndexResult) {
	result.nextHandle = taskCtx.nextHandle
	result.addedCount += taskCtx.addedCount
	result.scanCount += taskCtx.scanCount
}

func newAddIndexWorker(sessCtx causetnetctx.contextctx, worker *worker, id int, t table.PhysicalTable, indexInfo *serial.IndexInfo, decodeColMap map[int64]decoder.Column) *addIndexWorker {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, decodeColMap)
	return &addIndexWorker{
		id:          id,
		dbsWorker:   worker,
		batchCnt:    int(variable.GetdbsReorgBatchSize()),
		sessCtx:     sessCtx,
		taskCh:      make(chan *reorgIndexTask, 1),
		resultCh:    make(chan *addIndexResult, 1),
		index:       index,
		table:       t,
		rowDecoder:  rowDecoder,
		priority:    ekv.PriorityLow,
		defaultVals: make([]types.Datum, len(t.Cols())),
		rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
	}
}

func (w *addIndexWorker) close() {
	if !w.closed {
		w.closed = true
		close(w.taskCh)
	}
}

// getIndexRecord gets index columns values from raw binary value row.
func (w *addIndexWorker) getIndexRecord(handle int64, recordKey []byte, rawRecord []byte) (*indexRecord, error) {
	t := w.table
	cols := t.Cols()
	idxInfo := w.index.Meta()
	sysZone := timeutil.SystemLocation()
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.sessCtx, ekv.IntHandle(handle), rawRecord, time.UTC, sysZone, w.rowMap)
	if err != nil {
		return nil, errors.Trace(errCantDecodeIndex.GenWithStackByArgs(err))
	}
	idxVal := make([]types.Datum, len(idxInfo.Columns))
	for j, v := range idxInfo.Columns {
		col := cols[v.Offset]
		if col.IsPKHandleColumn(t.Meta()) {
			if mysql.HasUnsignedFlag(col.Flag) {
				idxVal[j].SetUint64(uint64(handle))
			} else {
				idxVal[j].SetInt64(handle)
			}
			continue
		}
		idxColumnVal, ok := w.rowMap[col.ID]
		if ok {
			idxVal[j] = idxColumnVal
			// Make sure there is no dirty data.
			delete(w.rowMap, col.ID)
			continue
		}
		idxColumnVal, err = tables.GetColDefaultValue(w.sessCtx, col, w.defaultVals)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if idxColumnVal.Kind() == types.KindMysqlTime {
			t := idxColumnVal.GetMysqlTime()
			if t.Type() == mysql.TypeTimestamp && sysZone != time.UTC {
				err := t.ConvertTimeZone(sysZone, time.UTC)
				if err != nil {
					return nil, errors.Trace(err)
				}
				idxColumnVal.SetMysqlTime(t)
			}
		}
		idxVal[j] = idxColumnVal
	}
	// If there are generated column, rowDecoder will use column value that not in idxInfo.Columns to calculate
	// the generated value, so we need to clear up the reusing map.
	w.cleanRowMap()
	idxRecord := &indexRecord{handle: handle, key: recordKey, vals: idxVal}
	return idxRecord, nil
}

func (w *addIndexWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

// getNextHandle gets next handle of entry that we are going to process.
func (w *addIndexWorker) getNextHandle(taskRange reorgIndexTask, taskDone bool) (nextHandle int64) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		return w.idxRecords[len(w.idxRecords)-1].handle + 1
	}

	// The task is done. So we need to choose a handle outside this range.
	// Some corner cases should be considered:
	// - The end of task range is MaxInt64.
	// - The end of the task is excluded in the range.
	if taskRange.endHandle == math.MaxInt64 || !taskRange.endIncluded {
		return taskRange.endHandle
	}

	return taskRange.endHandle + 1
}

// fetchRowColVals fetch w.batchCnt count rows that need to backfill indices, and build the corresponding indexRecord slice.
// fetchRowColVals returns:
// 1. The corresponding indexRecord slice.
// 2. Next handle of entry that we need to process.
// 3. Boolean indicates whether the task is done.
// 4. error occurs in fetchRowColVals. nil if no error occurs.
func (w *addIndexWorker) fetchRowColVals(txn ekv.Transaction, taskRange reorgIndexTask) ([]*indexRecord, int64, bool, error) {
	// TODO: use tableScan to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotRows(w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startHandle, taskRange.endHandle, taskRange.endIncluded,
		func(handle int64, recordKey ekv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			w.logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotRows in fetchRowColVals", 0)
			oprStartTime = oprEndTime

			if !taskRange.endIncluded {
				taskDone = handle >= taskRange.endHandle
			} else {
				taskDone = handle > taskRange.endHandle
			}

			if taskDone || len(w.idxRecords) >= w.batchCnt {
				return false, nil
			}

			idxRecord, err1 := w.getIndexRecord(handle, recordKey, rawRow)
			if err1 != nil {
				return false, errors.Trace(err1)
			}

			w.idxRecords = append(w.idxRecords, idxRecord)
			if handle == taskRange.endHandle {
				// If taskRange.endIncluded == false, we will not reach here when handle == taskRange.endHandle
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.idxRecords) == 0 {
		taskDone = true
	}

	logutil.BgLogger().Debug("[dbs] txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()), zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.idxRecords, w.getNextHandle(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexWorker) logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	if threshold == 0 {
		threshold = atomic.LoadUint32(&variable.dbsSlowOprThreshold)
	}

	if elapsed >= time.Duration(threshold)*time.Millisecond {
		logutil.BgLogger().Info("[dbs] slow operations", zap.Duration("takeTimes", elapsed), zap.String("msg", slowMsg))
	}
}

func (w *addIndexWorker) initBatchCheckBufs(batchCount int) {
	if len(w.idxKeyBufs) < batchCount {
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]
}

func (w *addIndexWorker) batchCheckUniqueKey(txn ekv.Transaction, idxRecords []*indexRecord) error {
	idxInfo := w.index.Meta()
	if !idxInfo.Unique {
		// non-unique key need not to check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	w.initBatchCheckBufs(len(idxRecords))
	stmtCtx := w.sessCtx.GetSessionVars().StmtCtx
	for i, record := range idxRecords {
		idxKey, distinct, err := w.index.GenIndexKey(stmtCtx, record.vals, ekv.IntHandle(record.handle), w.idxKeyBufs[i])
		if err != nil {
			return errors.Trace(err)
		}
		// save the buffer to reduce memory allocations.
		w.idxKeyBufs[i] = idxKey

		w.batchCheckKeys = append(w.batchCheckKeys, idxKey)
		w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
	}

	batchVals, err := txn.BatchGet(context.Background(), w.batchCheckKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key/primary-key is duplicate and the handle is equal, skip it.
	// 2. unique-key/primary-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.batchCheckKeys {
		if val, found := batchVals[string(key)]; found {
			if w.distinctCheckFlags[i] {
				handle, err1 := tables.DecodeHandleInUniqueIndexValue(val)
				if err1 != nil {
					return errors.Trace(err1)
				}

				if handle != idxRecords[i].handle {
					return errors.Trace(ekv.ErrKeyExists)
				}
			}
			idxRecords[i].skip = true
		} else {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			if w.distinctCheckFlags[i] {
				batchVals[string(key)] = tables.EncodeHandleInUniqueIndexValue(idxRecords[i].handle)
			}
		}
	}
	// Constrains is already checked.
	stmtCtx.BatchCheck = true
	return nil
}

// backfillIndexInTxn will backfill table index in a transaction, lock corresponding rowKey, if the value of rowKey is changed,
// indicate that index columns values may changed, index is not allowed to be added, so the txn will rollback and retry.
// backfillIndexInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
// TODO: make w.batchCnt can be modified by system variable.
func (w *addIndexWorker) backfillIndexInTxn(handleRange reorgIndexTask) (taskCtx addIndexTaskcontextctx, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	errInTxn = ekv.RunInNewTxn(w.sessCtx.GetStore(), true, func(txn ekv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(ekv.Priority, w.priority)

		idxRecords, nextHandle, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextHandle = nextHandle
		taskCtx.done = taskDone

		err = w.batchCheckUniqueKey(txn, idxRecords)
		if err != nil {
			return errors.Trace(err)
		}

		for _, idxRecord := range idxRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, milevadb can handle it correctly.
			if idxRecord.skip {
				continue
			}
