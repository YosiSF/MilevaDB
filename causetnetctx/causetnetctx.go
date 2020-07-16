//Copyright 2020 WHTCORPS INC ALL RIGHTS RESERVED
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions.

package causetnetctx

import (
	"context"
	"fmt"

	"github.com/YosiSF/MilevaDB/BerolinaSQL/serial"
	"github.com/YosiSF/MilevaDB/causetnetctx/variable"
	"github.com/YosiSF/MilevaDB/ekv"
	"github.com/YosiSF/MilevaDB/keywatcher"
	"github.com/YosiSF/MilevaDB/util"
	"github.com/YosiSF/MilevaDB/util/kvcache"
	"github.com/YosiSF/MilevaDB/util/memory"

)

// Context is an interface for transaction and executive args environment.
type Context interface {
	// NewTxn creates a new transaction for further execution.
	// If old transaction is valid, it is committed first.
	// It's used in BEGIN statement and DDL statements to commit old transaction.
	NewTxn(context.Context) error

	// Txn returns the current transaction which is created before executing a statement.
	// The returned ekv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	Txn(active bool) (ekv.Transaction, error)

	// GetClient gets a ekv.Client.
	GetClient() ekv.Client

	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}

	// ClearValue clears the value associated with this context for key.
	ClearValue(key fmt.Stringer)

	GetCausetNetVars() *variable.CausetNetVars

	GetCausetNetManager() util.CausetNetManager

	// RefreshTxnCtx commits old transaction without retry,
	// and creates a new transaction.
	// now just for load data and batch insert.
	RefreshTxnCtx(context.Context) error

	// InitTxnWithStartTS initializes a transaction with startTS.
	// It should be called right before we builds an executor.
	InitTxnWithStartTS(startTS uint64) error

	// GetStore returns the store of CausetNet.
	GetStore() ekv.Storage

	// PreparedPlanCache returns the cache of the physical plan
	PreparedPlanCache() *kvcache.SimpleLRUCache

	// StoreQueryFeedback stores the query feedback.
	StoreQueryFeedback(feedback interface{})

	// HasDirtyContent checks whether there's dirty update on the given table.
	HasDirtyContent(tid int64) bool

	// StmtCommit flush all changes by the statement to the underlying transaction.
	StmtCommit(tracker *memory.Tracker) error
	// StmtRollback provides statement level rollback.
	StmtRollback()
	// StmtGetMutation gets the binlog mutation for current statement.
	StmtGetMutation(int64) *binlog.TableMutation
	// StmtAddDirtyTableOP adds the dirty table operation for current statement.
	StmtAddDirtyTableOP(op int, physicalID int64, handle ekv.Handle)
	// DDLOwnerChecker returns keywatcher.DDLOwnerChecker.
	DDLOwnerChecker() keywatcher.DDLOwnerChecker
	// AddTableLock adds table lock to the CausetNet lock map.
	AddTableLock([]serial.TableLockTpInfo)
	// ReleaseTableLocks releases table locks in the CausetNet lock map.
	ReleaseTableLocks(locks []serial.TableLockTpInfo)
	// ReleaseTableLockByTableID releases table locks in the CausetNet lock map by table ID.
	ReleaseTableLockByTableIDs(tableIDs []int64)
	// CheckTableLocked checks the table lock.
	CheckTableLocked(tblID int64) (bool, serial.TableLockType)
	// GetAllTableLocks gets all table locks table id and db id hold by the CausetNet.
	GetAllTableLocks() []serial.TableLockTpInfo
	// ReleaseAllTableLocks releases all table locks hold by the CausetNet.
	ReleaseAllTableLocks()
	// HasLockedTables uses to check whether this CausetNet locked any tables.
	HasLockedTables() bool
	// PrepareTSFuture uses to prepare timestamp by future.
	PrepareTSFuture(ctx context.Context)
}

type basicCtxType int

func (t basicCtxType) String() string {
	switch t {
	case QueryString:
		return "query_string"
	case Initing:
		return "initing"
	case LastExecuteDBS:
		return "last_execute_dbs"
	}
	return "unknown"
}

// Context keys.
const (
	// QueryString is the key for original query string.
	QueryString basicCtxType = 1
	// Initing is the key for indicating if the server is running bootstrap or upgrade job.
	Initing basicCtxType = 2
	// LastExecuteDDL is the key for whether the CausetNet execute a ddl command last time.
	LastExecuteDBS basicCtxType = 3
)

type connIDCtxKeyType struct{}

// ConnID is the key in context.
var ConnID = connIDCtxKeyType{}

// SetCommitCtx sets connection id into context
func SetCommitCtx(ctx context.Context, sessCtx Context) context.Context {
	return context.WithValue(ctx, ConnID, sessCtx.GetCausetNetVars().ConnectionID)
}
