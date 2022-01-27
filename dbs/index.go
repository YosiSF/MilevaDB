// INTERLOCKyright 2020 WHTCORPS INC
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
	"math"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	decoder "github.com/whtcorpsinc/milevadb/soliton/rowDecoder"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

const (
	// MaxCommentLength is exported for testing.
	MaxCommentLength = 1024
)

func buildIndexDeferredCausets(columns []*perceptron.DeferredCausetInfo, indexPartSpecifications []*ast.IndexPartSpecification) ([]*perceptron.IndexDeferredCauset, error) {
	// Build offsets.
	idxParts := make([]*perceptron.IndexDeferredCauset, 0, len(indexPartSpecifications))
	var col *perceptron.DeferredCausetInfo

	// The sum of length of all index columns.
	sumLength := 0
	for _, ip := range indexPartSpecifications {
		col = perceptron.FindDeferredCausetInfo(columns, ip.DeferredCauset.Name.L)
		if col == nil {
			return nil, errKeyDeferredCausetDoesNotExits.GenWithStack("column does not exist: %s", ip.DeferredCauset.Name)
		}

		if err := checHoTTexDeferredCauset(col, ip); err != nil {
			return nil, err
		}

		indexDeferredCausetLength, err := getIndexDeferredCausetLength(col, ip.Length)
		if err != nil {
			return nil, err
		}
		sumLength += indexDeferredCausetLength

		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > config.GetGlobalConfig().MaxIndexLength {
			return nil, errTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
		}

		idxParts = append(idxParts, &perceptron.IndexDeferredCauset{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ip.Length,
		})
	}

	return idxParts, nil
}

func checkPKOnGeneratedDeferredCauset(tblInfo *perceptron.BlockInfo, indexPartSpecifications []*ast.IndexPartSpecification) (*perceptron.DeferredCausetInfo, error) {
	var lastDefCaus *perceptron.DeferredCausetInfo
	for _, colName := range indexPartSpecifications {
		lastDefCaus = getDeferredCausetInfoByName(tblInfo, colName.DeferredCauset.Name.L)
		if lastDefCaus == nil {
			return nil, errKeyDeferredCausetDoesNotExits.GenWithStackByArgs(colName.DeferredCauset.Name)
		}
		// Virtual columns cannot be used in primary key.
		if lastDefCaus.IsGenerated() && !lastDefCaus.GeneratedStored {
			return nil, ErrUnsupportedOnGeneratedDeferredCauset.GenWithStackByArgs("Defining a virtual generated column as primary key")
		}
	}

	return lastDefCaus, nil
}

func checHoTTexPrefixLength(columns []*perceptron.DeferredCausetInfo, idxDeferredCausets []*perceptron.IndexDeferredCauset) error {
	// The sum of length of all index columns.
	sumLength := 0
	for _, ic := range idxDeferredCausets {
		col := perceptron.FindDeferredCausetInfo(columns, ic.Name.L)
		if col == nil {
			return errKeyDeferredCausetDoesNotExits.GenWithStack("column does not exist: %s", ic.Name)
		}

		indexDeferredCausetLength, err := getIndexDeferredCausetLength(col, ic.Length)
		if err != nil {
			return err
		}
		sumLength += indexDeferredCausetLength
		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > config.GetGlobalConfig().MaxIndexLength {
			return errTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
		}
	}
	return nil
}

func checHoTTexDeferredCauset(col *perceptron.DeferredCausetInfo, ic *ast.IndexPartSpecification) error {
	if col.Flen == 0 && (types.IsTypeChar(col.FieldType.Tp) || types.IsTypeVarchar(col.FieldType.Tp)) {
		return errors.Trace(errWrongKeyDeferredCauset.GenWithStackByArgs(ic.DeferredCauset.Name))
	}

	// JSON column cannot index.
	if col.FieldType.Tp == allegrosql.TypeJSON {
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

// getIndexDeferredCausetLength calculate the bytes number required in an index column.
func getIndexDeferredCausetLength(col *perceptron.DeferredCausetInfo, colLen int) (int, error) {
	length := types.UnspecifiedLength
	if colLen != types.UnspecifiedLength {
		length = colLen
	} else if col.Flen != types.UnspecifiedLength {
		length = col.Flen
	}

	switch col.Tp {
	case allegrosql.TypeBit:
		return (length + 7) >> 3, nil
	case allegrosql.TypeVarchar, allegrosql.TypeString:
		// Different charsets occupy different numbers of bytes on each character.
		desc, err := charset.GetCharsetDesc(col.Charset)
		if err != nil {
			return 0, errUnsupportedCharset.GenWithStackByArgs(col.Charset, col.DefCauslate)
		}
		return desc.Maxlen * length, nil
	case allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeBlob, allegrosql.TypeLongBlob:
		return length, nil
	case allegrosql.TypeTiny, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeDouble, allegrosql.TypeShort:
		return allegrosql.DefaultLengthOfMysqlTypes[col.Tp], nil
	case allegrosql.TypeFloat:
		if length <= allegrosql.MaxFloatPrecisionLength {
			return allegrosql.DefaultLengthOfMysqlTypes[allegrosql.TypeFloat], nil
		}
		return allegrosql.DefaultLengthOfMysqlTypes[allegrosql.TypeDouble], nil
	case allegrosql.TypeNewDecimal:
		return calcBytesLengthForDecimal(length), nil
	case allegrosql.TypeYear, allegrosql.TypeDate, allegrosql.TypeDuration, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		return allegrosql.DefaultLengthOfMysqlTypes[col.Tp], nil
	default:
		return length, nil
	}
}

// Decimal using a binary format that packs nine decimal (base 10) digits into four bytes.
func calcBytesLengthForDecimal(m int) int {
	return (m / 9 * 4) + ((m%9)+1)/2
}

func buildIndexInfo(tblInfo *perceptron.BlockInfo, indexName perceptron.CIStr, indexPartSpecifications []*ast.IndexPartSpecification, state perceptron.SchemaState) (*perceptron.IndexInfo, error) {
	if err := checkTooLongIndex(indexName); err != nil {
		return nil, errors.Trace(err)
	}

	idxDeferredCausets, err := buildIndexDeferredCausets(tblInfo.DeferredCausets, indexPartSpecifications)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create index info.
	idxInfo := &perceptron.IndexInfo{
		Name:            indexName,
		DeferredCausets: idxDeferredCausets,
		State:           state,
	}
	return idxInfo, nil
}

func addIndexDeferredCausetFlag(tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.DeferredCausets {
			tblInfo.DeferredCausets[col.Offset].Flag |= allegrosql.PriKeyFlag
		}
		return
	}

	col := indexInfo.DeferredCausets[0]
	if indexInfo.Unique && len(indexInfo.DeferredCausets) == 1 {
		tblInfo.DeferredCausets[col.Offset].Flag |= allegrosql.UniqueKeyFlag
	} else {
		tblInfo.DeferredCausets[col.Offset].Flag |= allegrosql.MultipleKeyFlag
	}
}

func dropIndexDeferredCausetFlag(tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.DeferredCausets {
			tblInfo.DeferredCausets[col.Offset].Flag &= ^allegrosql.PriKeyFlag
		}
	} else if indexInfo.Unique && len(indexInfo.DeferredCausets) == 1 {
		tblInfo.DeferredCausets[indexInfo.DeferredCausets[0].Offset].Flag &= ^allegrosql.UniqueKeyFlag
	} else {
		tblInfo.DeferredCausets[indexInfo.DeferredCausets[0].Offset].Flag &= ^allegrosql.MultipleKeyFlag
	}

	col := indexInfo.DeferredCausets[0]
	// other index may still cover this col
	for _, index := range tblInfo.Indices {
		if index.Name.L == indexInfo.Name.L {
			continue
		}

		if index.DeferredCausets[0].Name.L != col.Name.L {
			continue
		}

		addIndexDeferredCausetFlag(tblInfo, index)
	}
}

func validateRenameIndex(from, to perceptron.CIStr, tbl *perceptron.BlockInfo) (ignore bool, err error) {
	if fromIdx := tbl.FindIndexByName(from.L); fromIdx == nil {
		return false, errors.Trace(schemareplicant.ErrKeyNotExists.GenWithStackByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	if from.O == to.O {
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	if toIdx := tbl.FindIndexByName(to.L); toIdx != nil && from.L != to.L {
		return false, errors.Trace(schemareplicant.ErrKeyNameDuplicate.GenWithStackByArgs(toIdx.Name.O))
	}
	return false, nil
}

func onRenameIndex(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	tblInfo, from, to, err := checkRenameIndex(t, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}

	idx := tblInfo.FindIndexByName(from.L)
	idx.Name = to
	if ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, true); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func validateAlterIndexVisibility(indexName perceptron.CIStr, invisible bool, tbl *perceptron.BlockInfo) (bool, error) {
	if idx := tbl.FindIndexByName(indexName.L); idx == nil {
		return false, errors.Trace(schemareplicant.ErrKeyNotExists.GenWithStackByArgs(indexName.O, tbl.Name))
	} else if idx.Invisible == invisible {
		return true, nil
	}
	return false, nil
}

func onAlterIndexVisibility(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	tblInfo, from, invisible, err := checkAlterIndexVisibility(t, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}
	idx := tblInfo.FindIndexByName(from.L)
	idx.Invisible = invisible
	if ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, true); err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	return ver, nil
}

func getNullDefCausInfos(tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo) ([]*perceptron.DeferredCausetInfo, error) {
	nullDefCauss := make([]*perceptron.DeferredCausetInfo, 0, len(indexInfo.DeferredCausets))
	for _, colName := range indexInfo.DeferredCausets {
		col := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, colName.Name.L)
		if !allegrosql.HasNotNullFlag(col.Flag) || allegrosql.HasPreventNullInsertFlag(col.Flag) {
			nullDefCauss = append(nullDefCauss, col)
		}
	}
	return nullDefCauss, nil
}

func checkPrimaryKeyNotNull(w *worker, sqlMode allegrosql.ALLEGROSQLMode, t *meta.Meta, job *perceptron.Job,
	tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo) (warnings []string, err error) {
	if !indexInfo.Primary {
		return nil, nil
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return nil, err
	}
	nullDefCauss, err := getNullDefCausInfos(tblInfo, indexInfo)
	if err != nil {
		return nil, err
	}
	if len(nullDefCauss) == 0 {
		return nil, nil
	}

	err = modifyDefCaussFromNull2NotNull(w, dbInfo, tblInfo, nullDefCauss, perceptron.NewCIStr(""), false)
	if err == nil {
		return nil, nil
	}
	_, err = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, err)
	// TODO: Support non-strict mode.
	// warnings = append(warnings, ErrWarnDataTruncated.GenWithStackByArgs(oldDefCaus.Name.L, 0).Error())
	return nil, err
}

func uFIDelateHiddenDeferredCausets(tblInfo *perceptron.BlockInfo, idxInfo *perceptron.IndexInfo, state perceptron.SchemaState) {
	for _, col := range idxInfo.DeferredCausets {
		if tblInfo.DeferredCausets[col.Offset].Hidden {
			tblInfo.DeferredCausets[col.Offset].State = state
		}
	}
}

func (w *worker) onCreateIndex(d *dbsCtx, t *meta.Meta, job *perceptron.Job, isPK bool) (ver int64, err error) {
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
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		unique                  bool
		global                  bool
		indexName               perceptron.CIStr
		indexPartSpecifications []*ast.IndexPartSpecification
		indexOption             *ast.IndexOption
		sqlMode                 allegrosql.ALLEGROSQLMode
		warnings                []string
		hiddenDefCauss          []*perceptron.DeferredCausetInfo
	)
	if isPK {
		// Notice: sqlMode and warnings is used to support non-strict mode.
		err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption, &sqlMode, &warnings, &global)
	} else {
		err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption, &hiddenDefCauss, &global)
	}
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo != nil && indexInfo.State == perceptron.StatePublic {
		job.State = perceptron.JobStateCancelled
		err = ErrDupKeyName.GenWithStack("index already exist %s", indexName)
		if isPK {
			err = schemareplicant.ErrMultiplePriKey
		}
		return ver, err
	}
	for _, hiddenDefCaus := range hiddenDefCauss {
		columnInfo := perceptron.FindDeferredCausetInfo(tblInfo.DeferredCausets, hiddenDefCaus.Name.L)
		if columnInfo != nil && columnInfo.State == perceptron.StatePublic {
			// We already have a column with the same column name.
			job.State = perceptron.JobStateCancelled
			// TODO: refine the error message
			return ver, schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(hiddenDefCaus.Name)
		}
	}

	if indexInfo == nil {
		if len(hiddenDefCauss) > 0 {
			pos := &ast.DeferredCausetPosition{Tp: ast.DeferredCausetPositionNone}
			for _, hiddenDefCaus := range hiddenDefCauss {
				_, _, _, err = createDeferredCausetInfo(tblInfo, hiddenDefCaus, pos)
				if err != nil {
					job.State = perceptron.JobStateCancelled
					return ver, errors.Trace(err)
				}
			}
		}
		if err = checkAddDeferredCausetTooManyDeferredCausets(len(tblInfo.DeferredCausets)); err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
		indexInfo, err = buildIndexInfo(tblInfo, indexName, indexPartSpecifications, perceptron.StateNone)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, errors.Trace(err)
		}
		if indexOption != nil {
			indexInfo.Comment = indexOption.Comment
			if indexOption.Visibility == ast.IndexVisibilityInvisible {
				indexInfo.Invisible = true
			}
			if indexOption.Tp == perceptron.IndexTypeInvalid {
				// Use btree as default index type.
				indexInfo.Tp = perceptron.IndexTypeBtree
			} else {
				indexInfo.Tp = indexOption.Tp
			}
		} else {
			// Use btree as default index type.
			indexInfo.Tp = perceptron.IndexTypeBtree
		}
		indexInfo.Primary = false
		if isPK {
			if _, err = checkPKOnGeneratedDeferredCauset(tblInfo, indexPartSpecifications); err != nil {
				job.State = perceptron.JobStateCancelled
				return ver, err
			}
			indexInfo.Primary = true
		}
		indexInfo.Unique = unique
		indexInfo.Global = global
		indexInfo.ID = allocateIndexID(tblInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)

		// Here we need do this check before set state to `DeleteOnly`,
		// because if hidden columns has been set to `DeleteOnly`,
		// the `DeleteOnly` columns are missing when we do this check.
		if err := checkInvisibleIndexOnPK(tblInfo); err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, err
		}
		logutil.BgLogger().Info("[dbs] run add index job", zap.String("job", job.String()), zap.Reflect("indexInfo", indexInfo))
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case perceptron.StateNone:
		// none -> delete only
		indexInfo.State = perceptron.StateDeleteOnly
		uFIDelateHiddenDeferredCausets(tblInfo, indexInfo, perceptron.StateDeleteOnly)
		ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = perceptron.StateDeleteOnly
		metrics.AddIndexProgress.Set(0)
	case perceptron.StateDeleteOnly:
		// delete only -> write only
		indexInfo.State = perceptron.StateWriteOnly
		uFIDelateHiddenDeferredCausets(tblInfo, indexInfo, perceptron.StateWriteOnly)
		_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = perceptron.StateWriteOnly
	case perceptron.StateWriteOnly:
		// write only -> reorganization
		indexInfo.State = perceptron.StateWriteReorganization
		uFIDelateHiddenDeferredCausets(tblInfo, indexInfo, perceptron.StateWriteReorganization)
		_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = perceptron.StateWriteReorganization
	case perceptron.StateWriteReorganization:
		// reorganization -> public
		uFIDelateHiddenDeferredCausets(tblInfo, indexInfo, perceptron.StatePublic)
		tbl, err := getBlock(d.causetstore, schemaID, tblInfo)
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
			defer soliton.Recover(metrics.LabelDBS, "onCreateIndex",
				func() {
					addIndexErr = errCancelledDBSJob.GenWithStack("add block `%v` index `%v` panic", tblInfo.Name, indexInfo.Name)
				}, false)
			return w.addBlockIndex(tbl, indexInfo, reorgInfo)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// if timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if ekv.ErrKeyExists.Equal(err) || errCancelledDBSJob.Equal(err) || errCantDecodeRecord.Equal(err) {
				logutil.BgLogger().Warn("[dbs] run add index job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
				ver, err = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, err)
			}
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			w.reorgCtx.cleanNotifyReorgCancel()
			return ver, errors.Trace(err)
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		w.reorgCtx.cleanNotifyReorgCancel()

		indexInfo.State = perceptron.StatePublic
		// Set column index flag.
		addIndexDeferredCausetFlag(tblInfo, indexInfo)
		if isPK {
			if err = uFIDelateDefCaussNull2NotNull(tblInfo, indexInfo); err != nil {
				return ver, errors.Trace(err)
			}
		}
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tblInfo)
	default:
		err = ErrInvalidDBSState.GenWithStackByArgs("index", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func onDropIndex(t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	tblInfo, indexInfo, err := checkDropIndex(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	dependentHiddenDefCauss := make([]*perceptron.DeferredCausetInfo, 0)
	for _, indexDeferredCauset := range indexInfo.DeferredCausets {
		if tblInfo.DeferredCausets[indexDeferredCauset.Offset].Hidden {
			dependentHiddenDefCauss = append(dependentHiddenDefCauss, tblInfo.DeferredCausets[indexDeferredCauset.Offset])
		}
	}

	originalState := indexInfo.State
	switch indexInfo.State {
	case perceptron.StatePublic:
		// public -> write only
		job.SchemaState = perceptron.StateWriteOnly
		indexInfo.State = perceptron.StateWriteOnly
		if len(dependentHiddenDefCauss) > 0 {
			firstHiddenOffset := dependentHiddenDefCauss[0].Offset
			for i := 0; i < len(dependentHiddenDefCauss); i++ {
				tblInfo.DeferredCausets[firstHiddenOffset].State = perceptron.StateWriteOnly
				// Set this column's offset to the last and reset all following columns' offsets.
				adjustDeferredCausetInfoInDropDeferredCauset(tblInfo, firstHiddenOffset)
			}
		}
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != indexInfo.State)
	case perceptron.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = perceptron.StateDeleteOnly
		indexInfo.State = perceptron.StateDeleteOnly
		uFIDelateHiddenDeferredCausets(tblInfo, indexInfo, perceptron.StateDeleteOnly)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != indexInfo.State)
	case perceptron.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = perceptron.StateDeleteReorganization
		indexInfo.State = perceptron.StateDeleteReorganization
		uFIDelateHiddenDeferredCausets(tblInfo, indexInfo, perceptron.StateDeleteReorganization)
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tblInfo, originalState != indexInfo.State)
	case perceptron.StateDeleteReorganization:
		// reorganization -> absent
		newIndices := make([]*perceptron.IndexInfo, 0, len(tblInfo.Indices))
		for _, idx := range tblInfo.Indices {
			if idx.Name.L != indexInfo.Name.L {
				newIndices = append(newIndices, idx)
			}
		}
		tblInfo.Indices = newIndices
		// Set column index flag.
		dropIndexDeferredCausetFlag(tblInfo, indexInfo)

		tblInfo.DeferredCausets = tblInfo.DeferredCausets[:len(tblInfo.DeferredCausets)-len(dependentHiddenDefCauss)]

		ver, err = uFIDelateVersionAndBlockInfoWithCheck(t, job, tblInfo, originalState != perceptron.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishBlockJob(perceptron.JobStateRollbackDone, perceptron.StateNone, ver, tblInfo)
			job.Args[0] = indexInfo.ID
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
		} else {
			job.FinishBlockJob(perceptron.JobStateDone, perceptron.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexInfo.ID, getPartitionIDs(tblInfo))
		}
	default:
		err = ErrInvalidDBSState.GenWithStackByArgs("index", indexInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropIndex(t *meta.Meta, job *perceptron.Job) (*perceptron.BlockInfo, *perceptron.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var indexName perceptron.CIStr
	if err = job.DecodeArgs(&indexName); err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo == nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	// Double check for drop index on auto_increment column.
	err = checkDropIndexOnAutoIncrementDeferredCauset(tblInfo, indexInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, autoid.ErrWrongAutoKey
	}

	// Check that drop primary index will not cause invisible implicit primary index.
	newIndices := make([]*perceptron.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if idx.Name.L != indexInfo.Name.L {
			newIndices = append(newIndices, idx)
		}
	}
	newTbl := tblInfo.Clone()
	newTbl.Indices = newIndices
	err = checkInvisibleIndexOnPK(newTbl)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	return tblInfo, indexInfo, nil
}

func checkDropIndexOnAutoIncrementDeferredCauset(tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo) error {
	defcaus := tblInfo.DeferredCausets
	for _, idxDefCaus := range indexInfo.DeferredCausets {
		flag := defcaus[idxDefCaus.Offset].Flag
		if !allegrosql.HasAutoIncrementFlag(flag) {
			continue
		}
		// check the count of index on auto_increment column.
		count := 0
		for _, idx := range tblInfo.Indices {
			for _, c := range idx.DeferredCausets {
				if c.Name.L == idxDefCaus.Name.L {
					count++
					break
				}
			}
		}
		if tblInfo.PKIsHandle && allegrosql.HasPriKeyFlag(flag) {
			count++
		}
		if count < 2 {
			return autoid.ErrWrongAutoKey
		}
	}
	return nil
}

func checkRenameIndex(t *meta.Meta, job *perceptron.Job) (*perceptron.BlockInfo, perceptron.CIStr, perceptron.CIStr, error) {
	var from, to perceptron.CIStr
	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, from, to, errors.Trace(err)
	}

	if err := job.DecodeArgs(&from, &to); err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}

	// Double check. See function `RenameIndex` in dbs_api.go
	duplicate, err := validateRenameIndex(from, to, tblInfo)
	if duplicate {
		return nil, from, to, nil
	}
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	return tblInfo, from, to, errors.Trace(err)
}

func checkAlterIndexVisibility(t *meta.Meta, job *perceptron.Job) (*perceptron.BlockInfo, perceptron.CIStr, bool, error) {
	var (
		indexName perceptron.CIStr
		invisible bool
	)

	schemaID := job.SchemaID
	tblInfo, err := getBlockInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, indexName, invisible, errors.Trace(err)
	}

	if err := job.DecodeArgs(&indexName, &invisible); err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}

	skip, err := validateAlterIndexVisibility(indexName, invisible, tblInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	if skip {
		return nil, indexName, invisible, nil
	}
	return tblInfo, indexName, invisible, nil
}

// indexRecord is the record information of an index.
type indexRecord struct {
	handle ekv.Handle
	key    []byte         // It's used to dagger a record. Record it to reduce the encoding time.
	vals   []types.Causet // It's the index values.
	skip   bool           // skip indicates that the index key is already exists, we should not add it.
}

type addIndexWorker struct {
	*backfillWorker
	index         block.Index
	metricCounter prometheus.Counter

	// The following attributes are used to reduce memory allocation.
	defaultVals        []types.Causet
	idxRecords         []*indexRecord
	rowMap             map[int64]types.Causet
	rowDecoder         *decoder.RowDecoder
	idxKeyBufs         [][]byte
	batchCheckKeys     []ekv.Key
	distinctCheckFlags []bool
}

func newAddIndexWorker(sessCtx stochastikctx.Context, worker *worker, id int, t block.PhysicalBlock, indexInfo *perceptron.IndexInfo, decodeDefCausMap map[int64]decoder.DeferredCauset) *addIndexWorker {
	index := blocks.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, t.WriblockDefCauss(), decodeDefCausMap)
	return &addIndexWorker{
		backfillWorker: newBackfillWorker(sessCtx, worker, id, t),
		index:          index,
		metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("add_idx_speed"),
		rowDecoder:     rowDecoder,
		defaultVals:    make([]types.Causet, len(t.WriblockDefCauss())),
		rowMap:         make(map[int64]types.Causet, len(decodeDefCausMap)),
	}
}

func (w *addIndexWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

// getIndexRecord gets index columns values from raw binary value event.
func (w *addIndexWorker) getIndexRecord(handle ekv.Handle, recordKey []byte, rawRecord []byte) (*indexRecord, error) {
	t := w.block
	defcaus := t.WriblockDefCauss()
	idxInfo := w.index.Meta()
	sysZone := timeutil.SystemLocation()
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.sessCtx, handle, rawRecord, time.UTC, sysZone, w.rowMap)
	if err != nil {
		return nil, errors.Trace(errCantDecodeRecord.GenWithStackByArgs("index", err))
	}
	idxVal := make([]types.Causet, len(idxInfo.DeferredCausets))
	for j, v := range idxInfo.DeferredCausets {
		col := defcaus[v.Offset]
		idxDeferredCausetVal, ok := w.rowMap[col.ID]
		if ok {
			idxVal[j] = idxDeferredCausetVal
			continue
		}
		idxDeferredCausetVal, err = blocks.GetDefCausDefaultValue(w.sessCtx, col, w.defaultVals)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if idxDeferredCausetVal.HoTT() == types.HoTTMysqlTime {
			t := idxDeferredCausetVal.GetMysqlTime()
			if t.Type() == allegrosql.TypeTimestamp && sysZone != time.UTC {
				err := t.ConvertTimeZone(sysZone, time.UTC)
				if err != nil {
					return nil, errors.Trace(err)
				}
				idxDeferredCausetVal.SetMysqlTime(t)
			}
		}
		idxVal[j] = idxDeferredCausetVal
	}
	// If there are generated column, rowDecoder will use column value that not in idxInfo.DeferredCausets to calculate
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
func (w *addIndexWorker) getNextHandle(taskRange reorgBackfillTask, taskDone bool) (nextHandle ekv.Handle) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		return w.idxRecords[len(w.idxRecords)-1].handle.Next()
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

// fetchRowDefCausVals fetch w.batchCnt count rows that need to backfill indices, and build the corresponding indexRecord slice.
// fetchRowDefCausVals returns:
// 1. The corresponding indexRecord slice.
// 2. Next handle of entry that we need to process.
// 3. Boolean indicates whether the task is done.
// 4. error occurs in fetchRowDefCausVals. nil if no error occurs.
func (w *addIndexWorker) fetchRowDefCausVals(txn ekv.Transaction, taskRange reorgBackfillTask) ([]*indexRecord, ekv.Handle, bool, error) {
	// TODO: use blockScan to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotRows(w.sessCtx.GetStore(), w.priority, w.block, txn.StartTS(), taskRange.startHandle, taskRange.endHandle, taskRange.endIncluded,
		func(handle ekv.Handle, recordKey ekv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotRows in addIndexWorker fetchRowDefCausVals", 0)
			oprStartTime = oprEndTime

			if !taskRange.endIncluded {
				taskDone = handle.Compare(taskRange.endHandle) >= 0
			} else {
				taskDone = handle.Compare(taskRange.endHandle) > 0
			}

			if taskDone || len(w.idxRecords) >= w.batchCnt {
				return false, nil
			}

			idxRecord, err1 := w.getIndexRecord(handle, recordKey, rawRow)
			if err1 != nil {
				return false, errors.Trace(err1)
			}

			w.idxRecords = append(w.idxRecords, idxRecord)
			if handle.Equal(taskRange.endHandle) {
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
	stmtCtx := w.sessCtx.GetStochastikVars().StmtCtx
	for i, record := range idxRecords {
		idxKey, distinct, err := w.index.GenIndexKey(stmtCtx, record.vals, record.handle, w.idxKeyBufs[i])
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
				handle, err1 := blockcodec.DecodeHandleInUniqueIndexValue(val, w.block.Meta().IsCommonHandle)
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
				batchVals[string(key)] = blockcodec.EncodeHandleInUniqueIndexValue(idxRecords[i].handle, false)
			}
		}
	}
	// Constrains is already checked.
	stmtCtx.BatchCheck = true
	return nil
}

// BackfillDataInTxn will backfill block index in a transaction, dagger corresponding rowKey, if the value of rowKey is changed,
// indicate that index columns values may changed, index is not allowed to be added, so the txn will rollback and retry.
// BackfillDataInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
// TODO: make w.batchCnt can be modified by system variable.
func (w *addIndexWorker) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
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

		idxRecords, nextHandle, taskDone, err := w.fetchRowDefCausVals(txn, handleRange)
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
			// The following uFIDelate, delete, insert on these rows, MilevaDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// Lock the event key to notify us that someone delete or uFIDelate the event,
			// then we should not backfill the index of it, otherwise the adding index is redundant.
			err := txn.LockKeys(context.Background(), new(ekv.LockCtx), idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}

			// Create the index.
			handle, err := w.index.Create(w.sessCtx, txn.GetUnionStore(), idxRecord.vals, idxRecord.handle)
			if err != nil {
				if ekv.ErrKeyExists.Equal(err) && idxRecord.handle.Equal(handle) {
					// Index already exists, skip it.
					continue
				}

				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}

		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexBackfillDataInTxn", 3000)

	return
}

func (w *worker) addPhysicalBlockIndex(t block.PhysicalBlock, indexInfo *perceptron.IndexInfo, reorgInfo *reorgInfo) error {
	logutil.BgLogger().Info("[dbs] start to add block index", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writePhysicalBlockRecord(t.(block.PhysicalBlock), typeAddIndexWorker, indexInfo, nil, nil, reorgInfo)
}

// addBlockIndex handles the add index reorganization state for a block.
func (w *worker) addBlockIndex(t block.Block, idx *perceptron.IndexInfo, reorgInfo *reorgInfo) error {
	var err error
	if tbl, ok := t.(block.PartitionedBlock); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalBlockID)
			if p == nil {
				return errCancelledDBSJob.GenWithStack("Can not find partition id %d for block %d", reorgInfo.PhysicalBlockID, t.Meta().ID)
			}
			err = w.addPhysicalBlockIndex(p, idx, reorgInfo)
			if err != nil {
				break
			}
			finish, err = w.uFIDelateReorgInfo(tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		err = w.addPhysicalBlockIndex(t.(block.PhysicalBlock), idx, reorgInfo)
	}
	return errors.Trace(err)
}

// uFIDelateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or block t is not a partitioned block, returns true to
// indicate that the reorganize work is finished.
func (w *worker) uFIDelateReorgInfo(t block.PartitionedBlock, reorg *reorgInfo) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	pid, err := findNextPartitionID(reorg.PhysicalBlockID, pi.Definitions)
	if err != nil {
		// Fatal error, should not run here.
		logutil.BgLogger().Error("[dbs] find next partition ID failed", zap.Reflect("block", t), zap.Error(err))
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}

	failpoint.Inject("mockUFIDelateCachedSafePoint", func(val failpoint.Value) {
		if val.(bool) {
			// 18 is for the logical time.
			ts := oracle.GetPhysical(time.Now()) << 18
			s := reorg.d.causetstore.(einsteindb.CausetStorage)
			s.UFIDelateSPCache(uint64(ts), time.Now())
			time.Sleep(time.Millisecond * 3)
		}
	})
	currentVer, err := getValidCurrentVersion(reorg.d.causetstore)
	if err != nil {
		return false, errors.Trace(err)
	}
	start, end, err := getBlockRange(reorg.d, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	reorg.StartHandle, reorg.EndHandle, reorg.PhysicalBlockID = start, end, pid

	// Write the reorg info to causetstore so the whole reorganize process can recover from panic.
	err = ekv.RunInNewTxn(reorg.d.causetstore, true, func(txn ekv.Transaction) error {
		return errors.Trace(reorg.UFIDelateReorgMeta(txn, reorg.StartHandle, reorg.EndHandle, reorg.PhysicalBlockID))
	})
	logutil.BgLogger().Info("[dbs] job uFIDelate reorgInfo", zap.Int64("jobID", reorg.Job.ID),
		zap.Int64("partitionBlockID", pid), zap.String("startHandle", toString(start)),
		zap.String("endHandle", toString(end)), zap.Error(err))
	return false, errors.Trace(err)
}

// findNextPartitionID finds the next partition ID in the PartitionDefinition array.
// Returns 0 if current partition is already the last one.
func findNextPartitionID(currentPartition int64, defs []perceptron.PartitionDefinition) (int64, error) {
	for i, def := range defs {
		if currentPartition == def.ID {
			if i == len(defs)-1 {
				return 0, nil
			}
			return defs[i+1].ID, nil
		}
	}
	return 0, errors.Errorf("partition id not found %d", currentPartition)
}

func allocateIndexID(tblInfo *perceptron.BlockInfo) int64 {
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

func getIndexInfoByNameAndDeferredCauset(oldBlockInfo *perceptron.BlockInfo, newOne *perceptron.IndexInfo) *perceptron.IndexInfo {
	for _, oldOne := range oldBlockInfo.Indices {
		if newOne.Name.L == oldOne.Name.L && indexDeferredCausetSliceEqual(newOne.DeferredCausets, oldOne.DeferredCausets) {
			return oldOne
		}
	}
	return nil
}

func indexDeferredCausetSliceEqual(a, b []*perceptron.IndexDeferredCauset) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		logutil.BgLogger().Warn("[dbs] admin repair block : index's columns length equal to 0")
		return true
	}
	// Accelerate the compare by eliminate index bound check.
	b = b[:len(a)]
	for i, v := range a {
		if v.Name.L != b[i].Name.L {
			return false
		}
	}
	return true
}

func findIndexesByDefCausName(indexes []*perceptron.IndexInfo, colName string) ([]*perceptron.IndexInfo, []int) {
	idxInfos := make([]*perceptron.IndexInfo, 0, len(indexes))
	offsets := make([]int, 0, len(indexes))
	for _, idxInfo := range indexes {
		for i, c := range idxInfo.DeferredCausets {
			if strings.EqualFold(colName, c.Name.L) {
				idxInfos = append(idxInfos, idxInfo)
				offsets = append(offsets, i)
				break
			}
		}
	}
	return idxInfos, offsets
}
