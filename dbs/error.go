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

	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	allegrosql "github.com/whtcorpsinc/MilevaDB-Prod/errno"
)

var (
	// errWorkerClosed means we have already closed the DBS worker.
	errInvalidWorker = terror.ClassDBS.New(allegrosql.ErrInvalidDBSWorker, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidDBSWorker])
	// errNotOwner means we are not owner and can't handle DBS jobs.
	errNotOwner              = terror.ClassDBS.New(allegrosql.ErrNotOwner, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNotOwner])
	errCantDecodeRecord      = terror.ClassDBS.New(allegrosql.ErrCantDecodeRecord, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantDecodeRecord])
	errInvalidDBSJob         = terror.ClassDBS.New(allegrosql.ErrInvalidDBSJob, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidDBSJob])
	errCancelledDBSJob       = terror.ClassDBS.New(allegrosql.ErrCancelledDBSJob, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCancelledDBSJob])
	errFileNotFound          = terror.ClassDBS.New(allegrosql.ErrFileNotFound, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFileNotFound])
	errRunMultiSchemaChanges = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "multi schemaReplicant change"))
	errWaitReorgTimeout      = terror.ClassDBS.New(allegrosql.ErrLockWaitTimeout, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWaitReorgTimeout])
	errInvalidStoreVer       = terror.ClassDBS.New(allegrosql.ErrInvalidStoreVersion, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidStoreVersion])
	// ErrRepairBlockFail is used to repair blockInfo in repair mode.
	ErrRepairBlockFail = terror.ClassDBS.New(allegrosql.ErrRepairBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrRepairBlock])

	// We don't support dropping column with index covered now.
	errCantDropDefCausWithIndex               = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "drop column with index"))
	errUnsupportedAddDeferredCauset           = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "add column"))
	errUnsupportedModifyDeferredCauset        = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "modify column: %s"))
	errUnsupportedModifyCharset               = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "modify %s"))
	errUnsupportedModifyDefCauslation         = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "modifying collation from %s to %s"))
	errUnsupportedPKHandle                    = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "drop integer primary key"))
	errUnsupportedCharset                     = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "charset %s and collate %s"))
	errUnsupportedShardRowIDBits              = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "shard_row_id_bits for block with primary key as event id"))
	errUnsupportedAlterBlockWithValidation    = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, "ALTER TABLE WITH VALIDATION is currently unsupported")
	errUnsupportedAlterBlockWithoutValidation = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, "ALTER TABLE WITHOUT VALIDATION is currently unsupported")
	errBlobKeyWithoutLength                   = terror.ClassDBS.New(allegrosql.ErrBlobKeyWithoutLength, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlobKeyWithoutLength])
	errKeyPart0                               = terror.ClassDBS.New(allegrosql.ErrKeyPart0, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrKeyPart0])
	errIncorrectPrefixKey                     = terror.ClassDBS.New(allegrosql.ErrWrongSubKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongSubKey])
	errTooLongKey                             = terror.ClassDBS.New(allegrosql.ErrTooLongKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooLongKey])
	errKeyDeferredCausetDoesNotExits          = terror.ClassDBS.New(allegrosql.ErrKeyDeferredCausetDoesNotExits, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrKeyDeferredCausetDoesNotExits])
	errUnknownTypeLength                      = terror.ClassDBS.New(allegrosql.ErrUnknownTypeLength, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownTypeLength])
	errUnknownFractionLength                  = terror.ClassDBS.New(allegrosql.ErrUnknownFractionLength, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownFractionLength])
	errInvalidDBSJobVersion                   = terror.ClassDBS.New(allegrosql.ErrInvalidDBSJobVersion, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidDBSJobVersion])
	errInvalidUseOfNull                       = terror.ClassDBS.New(allegrosql.ErrInvalidUseOfNull, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidUseOfNull])
	errTooManyFields                          = terror.ClassDBS.New(allegrosql.ErrTooManyFields, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooManyFields])
	errInvalidSplitRegionRanges               = terror.ClassDBS.New(allegrosql.ErrInvalidSplitRegionRanges, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidSplitRegionRanges])
	errReorgPanic                             = terror.ClassDBS.New(allegrosql.ErrReorgPanic, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrReorgPanic])
	errFkDeferredCausetCannotDrop             = terror.ClassDBS.New(allegrosql.ErrFkDeferredCausetCannotDrop, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFkDeferredCausetCannotDrop])
	errFKIncompatibleDeferredCausets          = terror.ClassDBS.New(allegrosql.ErrFKIncompatibleDeferredCausets, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFKIncompatibleDeferredCausets])

	errOnlyOnRangeListPartition = terror.ClassDBS.New(allegrosql.ErrOnlyOnRangeListPartition, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrOnlyOnRangeListPartition])
	// errWrongKeyDeferredCauset is for block column cannot be indexed.
	errWrongKeyDeferredCauset = terror.ClassDBS.New(allegrosql.ErrWrongKeyDeferredCauset, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongKeyDeferredCauset])
	// errWrongFKOptionForGeneratedDeferredCauset is for wrong foreign key reference option on generated columns.
	errWrongFKOptionForGeneratedDeferredCauset = terror.ClassDBS.New(allegrosql.ErrWrongFKOptionForGeneratedDeferredCauset, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongFKOptionForGeneratedDeferredCauset])
	// ErrUnsupportedOnGeneratedDeferredCauset is for unsupported actions on generated columns.
	ErrUnsupportedOnGeneratedDeferredCauset = terror.ClassDBS.New(allegrosql.ErrUnsupportedOnGeneratedDeferredCauset, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedOnGeneratedDeferredCauset])
	// errGeneratedDeferredCausetNonPrior forbids to refer generated column non prior to it.
	errGeneratedDeferredCausetNonPrior = terror.ClassDBS.New(allegrosql.ErrGeneratedDeferredCausetNonPrior, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrGeneratedDeferredCausetNonPrior])
	// errDependentByGeneratedDeferredCauset forbids to delete columns which are dependent by generated columns.
	errDependentByGeneratedDeferredCauset = terror.ClassDBS.New(allegrosql.ErrDependentByGeneratedDeferredCauset, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDependentByGeneratedDeferredCauset])
	// errJSONUsedAsKey forbids to use JSON as key or index.
	errJSONUsedAsKey = terror.ClassDBS.New(allegrosql.ErrJSONUsedAsKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrJSONUsedAsKey])
	// errBlobCantHaveDefault forbids to give not null default value to TEXT/BLOB/JSON.
	errBlobCantHaveDefault = terror.ClassDBS.New(allegrosql.ErrBlobCantHaveDefault, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlobCantHaveDefault])
	errTooLongIndexComment = terror.ClassDBS.New(allegrosql.ErrTooLongIndexComment, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooLongIndexComment])
	// ErrInvalidDefaultValue returns for invalid default value for columns.
	ErrInvalidDefaultValue = terror.ClassDBS.New(allegrosql.ErrInvalidDefault, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidDefault])
	// ErrGeneratedDeferredCausetRefAutoInc forbids to refer generated columns to auto-increment columns .
	ErrGeneratedDeferredCausetRefAutoInc = terror.ClassDBS.New(allegrosql.ErrGeneratedDeferredCausetRefAutoInc, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrGeneratedDeferredCausetRefAutoInc])
	// ErrUnsupportedAddPartition returns for does not support add partitions.
	ErrUnsupportedAddPartition = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "add partitions"))
	// ErrUnsupportedCoalescePartition returns for does not support coalesce partitions.
	ErrUnsupportedCoalescePartition   = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "coalesce partitions"))
	errUnsupportedReorganizePartition = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "reorganize partition"))
	errUnsupportedCheckPartition      = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "check partition"))
	errUnsupportedOptimizePartition   = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "optimize partition"))
	errUnsupportedRebuildPartition    = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "rebuild partition"))
	errUnsupportedRemovePartition     = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "remove partitioning"))
	errUnsupportedRepairPartition     = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "repair partition"))
	errUnsupportedExchangePartition   = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "exchange partition"))
	// ErrGeneratedDeferredCausetFunctionIsNotAllowed returns for unsupported functions for generated columns.
	ErrGeneratedDeferredCausetFunctionIsNotAllowed = terror.ClassDBS.New(allegrosql.ErrGeneratedDeferredCausetFunctionIsNotAllowed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrGeneratedDeferredCausetFunctionIsNotAllowed])
	// ErrUnsupportedPartitionByRangeDeferredCausets returns for does unsupported partition by range columns.
	ErrUnsupportedPartitionByRangeDeferredCausets = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "partition by range columns"))
	errUnsupportedCreatePartition                 = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "partition type, treat as normal block"))
	errBlockPartitionDisabled                     = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, "Partitions are ignored because Block Partition is disabled, please set 'milevadb_enable_block_partition' if you need to need to enable it")
	errUnsupportedIndexType                       = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "index type"))

	// ErrDupKeyName returns for duplicated key name
	ErrDupKeyName = terror.ClassDBS.New(allegrosql.ErrDupKeyName, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDupKeyName])
	// ErrInvalidDBSState returns for invalid dbs perceptron object state.
	ErrInvalidDBSState = terror.ClassDBS.New(allegrosql.ErrInvalidDBSState, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidDBSState]))
	// ErrUnsupportedModifyPrimaryKey returns an error when add or drop the primary key.
	// It's exported for testing.
	ErrUnsupportedModifyPrimaryKey = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "%s primary key"))
	// ErrPHoTTexCantBeInvisible return an error when primary key is invisible index
	ErrPHoTTexCantBeInvisible = terror.ClassDBS.New(allegrosql.ErrPHoTTexCantBeInvisible, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPHoTTexCantBeInvisible])

	// ErrDeferredCausetBadNull returns for a bad null value.
	ErrDeferredCausetBadNull = terror.ClassDBS.New(allegrosql.ErrBadNull, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadNull])
	// ErrBadField forbids to refer to unknown column.
	ErrBadField = terror.ClassDBS.New(allegrosql.ErrBadField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadField])
	// ErrCantRemoveAllFields returns for deleting all columns.
	ErrCantRemoveAllFields = terror.ClassDBS.New(allegrosql.ErrCantRemoveAllFields, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantRemoveAllFields])
	// ErrCantDropFieldOrKey returns for dropping a non-existent field or key.
	ErrCantDropFieldOrKey = terror.ClassDBS.New(allegrosql.ErrCantDropFieldOrKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantDropFieldOrKey])
	// ErrInvalidOnUFIDelate returns for invalid ON UFIDelATE clause.
	ErrInvalidOnUFIDelate = terror.ClassDBS.New(allegrosql.ErrInvalidOnUFIDelate, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidOnUFIDelate])
	// ErrTooLongIdent returns for too long name of database/block/column/index.
	ErrTooLongIdent = terror.ClassDBS.New(allegrosql.ErrTooLongIdent, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooLongIdent])
	// ErrWrongDBName returns for wrong database name.
	ErrWrongDBName = terror.ClassDBS.New(allegrosql.ErrWrongDBName, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongDBName])
	// ErrWrongBlockName returns for wrong block name.
	ErrWrongBlockName = terror.ClassDBS.New(allegrosql.ErrWrongBlockName, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongBlockName])
	// ErrWrongDeferredCausetName returns for wrong column name.
	ErrWrongDeferredCausetName = terror.ClassDBS.New(allegrosql.ErrWrongDeferredCausetName, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongDeferredCausetName])
	// ErrInvalidGroupFuncUse returns for using invalid group functions.
	ErrInvalidGroupFuncUse = terror.ClassDBS.New(allegrosql.ErrInvalidGroupFuncUse, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidGroupFuncUse])
	// ErrBlockMustHaveDeferredCausets returns for missing column when creating a block.
	ErrBlockMustHaveDeferredCausets = terror.ClassDBS.New(allegrosql.ErrBlockMustHaveDeferredCausets, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockMustHaveDeferredCausets])
	// ErrWrongNameForIndex returns for wrong index name.
	ErrWrongNameForIndex = terror.ClassDBS.New(allegrosql.ErrWrongNameForIndex, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongNameForIndex])
	// ErrUnknownCharacterSet returns unknown character set.
	ErrUnknownCharacterSet = terror.ClassDBS.New(allegrosql.ErrUnknownCharacterSet, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownCharacterSet])
	// ErrUnknownDefCauslation returns unknown collation.
	ErrUnknownDefCauslation = terror.ClassDBS.New(allegrosql.ErrUnknownDefCauslation, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownDefCauslation])
	// ErrDefCauslationCharsetMismatch returns when collation not match the charset.
	ErrDefCauslationCharsetMismatch = terror.ClassDBS.New(allegrosql.ErrDefCauslationCharsetMismatch, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDefCauslationCharsetMismatch])
	// ErrConflictingDeclarations return conflict declarations.
	ErrConflictingDeclarations = terror.ClassDBS.New(allegrosql.ErrConflictingDeclarations, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrConflictingDeclarations], "CHARACTER SET ", "%s", "CHARACTER SET ", "%s"))
	// ErrPrimaryCantHaveNull returns All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
	ErrPrimaryCantHaveNull = terror.ClassDBS.New(allegrosql.ErrPrimaryCantHaveNull, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPrimaryCantHaveNull])
	// ErrErrorOnRename returns error for wrong database name in alter block rename
	ErrErrorOnRename = terror.ClassDBS.New(allegrosql.ErrErrorOnRename, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrErrorOnRename])
	// ErrViewSelectClause returns error for create view with select into clause
	ErrViewSelectClause = terror.ClassDBS.New(allegrosql.ErrViewSelectClause, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrViewSelectClause])

	// ErrNotAllowedTypeInPartition returns not allowed type error when creating block partition with unsupported expression type.
	ErrNotAllowedTypeInPartition = terror.ClassDBS.New(allegrosql.ErrFieldTypeNotAllowedAsPartitionField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFieldTypeNotAllowedAsPartitionField])
	// ErrPartitionMgmtOnNonpartitioned returns it's not a partition block.
	ErrPartitionMgmtOnNonpartitioned = terror.ClassDBS.New(allegrosql.ErrPartitionMgmtOnNonpartitioned, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPartitionMgmtOnNonpartitioned])
	// ErrDropPartitionNonExistent returns error in list of partition.
	ErrDropPartitionNonExistent = terror.ClassDBS.New(allegrosql.ErrDropPartitionNonExistent, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDropPartitionNonExistent])
	// ErrSameNamePartition returns duplicate partition name.
	ErrSameNamePartition = terror.ClassDBS.New(allegrosql.ErrSameNamePartition, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSameNamePartition])
	// ErrRangeNotIncreasing returns values less than value must be strictly increasing for each partition.
	ErrRangeNotIncreasing = terror.ClassDBS.New(allegrosql.ErrRangeNotIncreasing, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrRangeNotIncreasing])
	// ErrPartitionMaxvalue returns maxvalue can only be used in last partition definition.
	ErrPartitionMaxvalue = terror.ClassDBS.New(allegrosql.ErrPartitionMaxvalue, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPartitionMaxvalue])
	//ErrDropLastPartition returns cannot remove all partitions, use drop block instead.
	ErrDropLastPartition = terror.ClassDBS.New(allegrosql.ErrDropLastPartition, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDropLastPartition])
	//ErrTooManyPartitions returns too many partitions were defined.
	ErrTooManyPartitions = terror.ClassDBS.New(allegrosql.ErrTooManyPartitions, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooManyPartitions])
	//ErrPartitionFunctionIsNotAllowed returns this partition function is not allowed.
	ErrPartitionFunctionIsNotAllowed = terror.ClassDBS.New(allegrosql.ErrPartitionFunctionIsNotAllowed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPartitionFunctionIsNotAllowed])
	// ErrPartitionFuncNotAllowed returns partition function returns the wrong type.
	ErrPartitionFuncNotAllowed = terror.ClassDBS.New(allegrosql.ErrPartitionFuncNotAllowed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPartitionFuncNotAllowed])
	// ErrUniqueKeyNeedAllFieldsInPf returns must include all columns in the block's partitioning function.
	ErrUniqueKeyNeedAllFieldsInPf = terror.ClassDBS.New(allegrosql.ErrUniqueKeyNeedAllFieldsInPf, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUniqueKeyNeedAllFieldsInPf])
	errWrongExprInPartitionFunc   = terror.ClassDBS.New(allegrosql.ErrWrongExprInPartitionFunc, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongExprInPartitionFunc])
	// ErrWarnDataTruncated returns data truncated error.
	ErrWarnDataTruncated = terror.ClassDBS.New(allegrosql.WarnDataTruncated, allegrosql.MyALLEGROSQLErrName[allegrosql.WarnDataTruncated])
	// ErrCoalesceOnlyOnHashPartition returns coalesce partition can only be used on hash/key partitions.
	ErrCoalesceOnlyOnHashPartition = terror.ClassDBS.New(allegrosql.ErrCoalesceOnlyOnHashPartition, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCoalesceOnlyOnHashPartition])
	// ErrViewWrongList returns create view must include all columns in the select clause
	ErrViewWrongList = terror.ClassDBS.New(allegrosql.ErrViewWrongList, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrViewWrongList])
	// ErrAlterOperationNotSupported returns when alter operations is not supported.
	ErrAlterOperationNotSupported = terror.ClassDBS.New(allegrosql.ErrAlterOperationNotSupportedReason, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrAlterOperationNotSupportedReason])
	// ErrWrongObject returns for wrong object.
	ErrWrongObject = terror.ClassDBS.New(allegrosql.ErrWrongObject, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongObject])
	// ErrBlockCantHandleFt returns FULLTEXT keys are not supported by block type
	ErrBlockCantHandleFt = terror.ClassDBS.New(allegrosql.ErrBlockCantHandleFt, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockCantHandleFt])
	// ErrFieldNotFoundPart returns an error when 'partition by columns' are not found in block columns.
	ErrFieldNotFoundPart = terror.ClassDBS.New(allegrosql.ErrFieldNotFoundPart, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFieldNotFoundPart])
	// ErrWrongTypeDeferredCausetValue returns 'Partition column values of incorrect type'
	ErrWrongTypeDeferredCausetValue = terror.ClassDBS.New(allegrosql.ErrWrongTypeDeferredCausetValue, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongTypeDeferredCausetValue])
	// ErrFunctionalIndexPrimaryKey returns 'The primary key cannot be a functional index'
	ErrFunctionalIndexPrimaryKey = terror.ClassDBS.New(allegrosql.ErrFunctionalIndexPrimaryKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFunctionalIndexPrimaryKey])
	// ErrFunctionalIndexOnField returns 'Functional index on a column is not supported. Consider using a regular index instead'
	ErrFunctionalIndexOnField = terror.ClassDBS.New(allegrosql.ErrFunctionalIndexOnField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFunctionalIndexOnField])
	// ErrInvalidAutoRandom returns when auto_random is used incorrectly.
	ErrInvalidAutoRandom = terror.ClassDBS.New(allegrosql.ErrInvalidAutoRandom, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidAutoRandom])
	// ErrUnsupportedConstraintCheck returns when use ADD CONSTRAINT CHECK
	ErrUnsupportedConstraintCheck = terror.ClassDBS.New(allegrosql.ErrUnsupportedConstraintCheck, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedConstraintCheck])

	// ErrSequenceRunOut returns when the sequence has been run out.
	ErrSequenceRunOut = terror.ClassDBS.New(allegrosql.ErrSequenceRunOut, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSequenceRunOut])
	// ErrSequenceInvalidData returns when sequence values are conflicting.
	ErrSequenceInvalidData = terror.ClassDBS.New(allegrosql.ErrSequenceInvalidData, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSequenceInvalidData])
	// ErrSequenceAccessFail returns when sequences are not able to access.
	ErrSequenceAccessFail = terror.ClassDBS.New(allegrosql.ErrSequenceAccessFail, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSequenceAccessFail])
	// ErrNotSequence returns when object is not a sequence.
	ErrNotSequence = terror.ClassDBS.New(allegrosql.ErrNotSequence, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNotSequence])
	// ErrUnknownSequence returns when drop / alter unknown sequence.
	ErrUnknownSequence = terror.ClassDBS.New(allegrosql.ErrUnknownSequence, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownSequence])
	// ErrSequenceUnsupportedBlockOption returns when unsupported block option exists in sequence.
	ErrSequenceUnsupportedBlockOption = terror.ClassDBS.New(allegrosql.ErrSequenceUnsupportedBlockOption, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSequenceUnsupportedBlockOption])
	// ErrDeferredCausetTypeUnsupportedNextValue is returned when sequence next value is assigned to unsupported column type.
	ErrDeferredCausetTypeUnsupportedNextValue = terror.ClassDBS.New(allegrosql.ErrDeferredCausetTypeUnsupportedNextValue, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDeferredCausetTypeUnsupportedNextValue])
	// ErrAddDeferredCausetWithSequenceAsDefault is returned when the new added column with sequence's nextval as it's default value.
	ErrAddDeferredCausetWithSequenceAsDefault = terror.ClassDBS.New(allegrosql.ErrAddDeferredCausetWithSequenceAsDefault, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrAddDeferredCausetWithSequenceAsDefault])
	// ErrUnsupportedExpressionIndex is returned when create an expression index without allow-expression-index.
	ErrUnsupportedExpressionIndex = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "creating expression index without allow-expression-index in config"))
	// ErrPartitionExchangePartBlock is returned when exchange block partition with another block is partitioned.
	ErrPartitionExchangePartBlock = terror.ClassDBS.New(allegrosql.ErrPartitionExchangePartBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPartitionExchangePartBlock])
	// ErrBlocksDifferentMetadata is returned when exchanges blocks is not compatible.
	ErrBlocksDifferentMetadata = terror.ClassDBS.New(allegrosql.ErrBlocksDifferentMetadata, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlocksDifferentMetadata])
	// ErrRowDoesNotMatchPartition is returned when the event record of exchange block does not match the partition rule.
	ErrRowDoesNotMatchPartition = terror.ClassDBS.New(allegrosql.ErrRowDoesNotMatchPartition, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrRowDoesNotMatchPartition])
	// ErrPartitionExchangeForeignKey is returned when exchanged normal block has foreign keys.
	ErrPartitionExchangeForeignKey = terror.ClassDBS.New(allegrosql.ErrPartitionExchangeForeignKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPartitionExchangeForeignKey])
	// ErrCheckNoSuchBlock is returned when exchanged normal block is view or sequence.
	ErrCheckNoSuchBlock         = terror.ClassDBS.New(allegrosql.ErrCheckNoSuchBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCheckNoSuchBlock])
	errUnsupportedPartitionType = terror.ClassDBS.New(allegrosql.ErrUnsupportedDBSOperation, fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedDBSOperation], "partition type of block %s when exchanging partition"))
	// ErrPartitionExchangeDifferentOption is returned when attribute does not match between partition block and normal block.
	ErrPartitionExchangeDifferentOption = terror.ClassDBS.New(allegrosql.ErrPartitionExchangeDifferentOption, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPartitionExchangeDifferentOption])
	// ErrBlockOptionUnionUnsupported is returned when create/alter block with union option.
	ErrBlockOptionUnionUnsupported = terror.ClassDBS.New(allegrosql.ErrBlockOptionUnionUnsupported, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockOptionUnionUnsupported])
	// ErrBlockOptionInsertMethodUnsupported is returned when create/alter block with insert method option.
	ErrBlockOptionInsertMethodUnsupported = terror.ClassDBS.New(allegrosql.ErrBlockOptionInsertMethodUnsupported, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockOptionInsertMethodUnsupported])

	// ErrInvalidPlacementSpec is returned when add/alter an invalid memristed rule
	ErrInvalidPlacementSpec = terror.ClassDBS.New(allegrosql.ErrInvalidPlacementSpec, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidPlacementSpec])
)
