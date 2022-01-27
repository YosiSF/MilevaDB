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

package ekv

import (
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/soliton/redact"
)

// TxnRetryableMark is used to uniform the commit error messages which could retry the transaction.
// *WARNING*: changing this string will affect the backward compatibility.
const TxnRetryableMark = "[try again later]"

var (
	// ErrNotExist is used when try to get an entry with an unexist key from KV causetstore.
	ErrNotExist = terror.ClassKV.New(allegrosql.ErrNotExist, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNotExist])
	// ErrTxnRetryable is used when KV causetstore occurs retryable error which ALLEGROALLEGROSQL layer can safely retry the transaction.
	// When using EinsteinDB as the storage node, the error is returned ONLY when dagger not found (txnLockNotFound) in Commit,
	// subject to change it in the future.
	ErrTxnRetryable = terror.ClassKV.New(allegrosql.ErrTxnRetryable,
		allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTxnRetryable]+TxnRetryableMark)
	// ErrCannotSetNilValue is the error when sets an empty value.
	ErrCannotSetNilValue = terror.ClassKV.New(allegrosql.ErrCannotSetNilValue, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCannotSetNilValue])
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = terror.ClassKV.New(allegrosql.ErrInvalidTxn, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidTxn])
	// ErrTxnTooLarge is the error when transaction is too large, dagger time reached the maximum value.
	ErrTxnTooLarge = terror.ClassKV.New(allegrosql.ErrTxnTooLarge, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTxnTooLarge])
	// ErrEntryTooLarge is the error when a key value entry is too large.
	ErrEntryTooLarge = terror.ClassKV.New(allegrosql.ErrEntryTooLarge, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrEntryTooLarge])
	// ErrKeyExists returns when key is already exist.
	ErrKeyExists = redact.NewRedactError(terror.ClassKV.New(allegrosql.ErrDupEntry, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDupEntry]), 0, 1)
	// ErrNotImplemented returns when a function is not implemented yet.
	ErrNotImplemented = terror.ClassKV.New(allegrosql.ErrNotImplemented, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNotImplemented])
	// ErrWriteConflict is the error when the commit meets an write conflict error.
	ErrWriteConflict = terror.ClassKV.New(allegrosql.ErrWriteConflict,
		allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWriteConflict]+" "+TxnRetryableMark)
	// ErrWriteConflictInMilevaDB is the error when the commit meets an write conflict error when local latch is enabled.
	ErrWriteConflictInMilevaDB = terror.ClassKV.New(allegrosql.ErrWriteConflictInMilevaDB,
		allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWriteConflictInMilevaDB]+" "+TxnRetryableMark)
)

// IsTxnRetryableError checks if the error could safely retry the transaction.
func IsTxnRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if ErrTxnRetryable.Equal(err) || ErrWriteConflict.Equal(err) || ErrWriteConflictInMilevaDB.Equal(err) {
		return true
	}

	return false
}

// IsErrNotFound checks if err is a HoTT of NotFound error.
func IsErrNotFound(err error) bool {
	return ErrNotExist.Equal(err)
}

// GetDuplicateErrorHandleString is used to concat the handle columns data with '-'.
// This is consistent with MyALLEGROSQL.
func GetDuplicateErrorHandleString(handle Handle) string {
	dt, err := handle.Data()
	if err != nil {
		return err.Error()
	}
	var sb strings.Builder
	for i, d := range dt {
		if i != 0 {
			sb.WriteString("-")
		}
		s, err := d.ToString()
		if err != nil {
			return err.Error()
		}
		sb.WriteString(s)
	}
	return sb.String()
}
