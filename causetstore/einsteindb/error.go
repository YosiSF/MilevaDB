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

package einsteindb

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/FIDelpb"
	"github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/errors"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
)

var (
	// ErrBodyMissing response body is missing error
	ErrBodyMissing = errors.New("response body is missing")
	// When MilevaDB is closing and send request to einsteindb fail, do not retry, return this error.
	errMilevaDBShuttingDown = errors.New("milevadb server shutting down")
)

// mismatchClusterID represents the message that the cluster ID of the FIDel client does not match the FIDel.
const mismatchClusterID = "mismatch cluster id"

// MyALLEGROSQL error instances.
var (
	ErrEinsteinDBServerTimeout         = terror.ClassEinsteinDB.New(allegrosql.ErrEinsteinDBServerTimeout, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrEinsteinDBServerTimeout])
	ErrResolveLockTimeout              = terror.ClassEinsteinDB.New(allegrosql.ErrResolveLockTimeout, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrResolveLockTimeout])
	ErrFIDelServerTimeout              = terror.ClassEinsteinDB.New(allegrosql.ErrFIDelServerTimeout, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFIDelServerTimeout])
	ErrRegionUnavailable               = terror.ClassEinsteinDB.New(allegrosql.ErrRegionUnavailable, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrRegionUnavailable])
	ErrEinsteinDBServerBusy            = terror.ClassEinsteinDB.New(allegrosql.ErrEinsteinDBServerBusy, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrEinsteinDBServerBusy])
	ErrEinsteinDBStaleCommand          = terror.ClassEinsteinDB.New(allegrosql.ErrEinsteinDBStaleCommand, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrEinsteinDBStaleCommand])
	ErrEinsteinDBMaxTimestampNotSynced = terror.ClassEinsteinDB.New(allegrosql.ErrEinsteinDBMaxTimestampNotSynced, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrEinsteinDBMaxTimestampNotSynced])
	ErrGCTooEarly                      = terror.ClassEinsteinDB.New(allegrosql.ErrGCTooEarly, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrGCTooEarly])
	ErrQueryInterrupted                = terror.ClassEinsteinDB.New(allegrosql.ErrQueryInterrupted, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrQueryInterrupted])
	ErrLockAcquireFailAndNoWaitSet     = terror.ClassEinsteinDB.New(allegrosql.ErrLockAcquireFailAndNoWaitSet, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrLockAcquireFailAndNoWaitSet])
	ErrLockWaitTimeout                 = terror.ClassEinsteinDB.New(allegrosql.ErrLockWaitTimeout, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrLockWaitTimeout])
	ErrTokenLimit                      = terror.ClassEinsteinDB.New(allegrosql.ErrEinsteinDBStoreLimit, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrEinsteinDBStoreLimit])
	ErrLockExpire                      = terror.ClassEinsteinDB.New(allegrosql.ErrLockExpire, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrLockExpire])
	ErrUnknown                         = terror.ClassEinsteinDB.New(allegrosql.ErrUnknown, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknown])
)

// Registers error returned from EinsteinDB.
var (
	_ = terror.ClassEinsteinDB.NewStd(allegrosql.ErrDataOutOfRange)
	_ = terror.ClassEinsteinDB.NewStd(allegrosql.ErrTruncatedWrongValue)
	_ = terror.ClassEinsteinDB.NewStd(allegrosql.ErrDivisionByZero)
)

// ErrDeadlock wraps *kvrpcpb.Deadlock to implement the error interface.
// It also marks if the deadlock is retryable.
type ErrDeadlock struct {
	*kvrpcpb.Deadlock
	IsRetryable bool
}

func (d *ErrDeadlock) Error() string {
	return d.Deadlock.String()
}

// FIDelError wraps *FIDelpb.Error to implement the error interface.
type FIDelError struct {
	Err *FIDelpb.Error
}

func (d *FIDelError) Error() string {
	return d.Err.String()
}
