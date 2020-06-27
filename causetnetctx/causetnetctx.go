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


)

// contextctx is an interface for transaction and executive args environment.
type contextctx interface {
	// NewTxn creates a new transaction for further execution.
	// If old transaction is valid, it is committed first.
	// It's used in BEGIN statement and dbs statements to commit old transaction.
	NewTxn(context.contextctx) error

	// Txn returns the current transaction which is created before executing a statement.
	// The returned kv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	Txn(active bool) (kv.Transaction, error)

	// GetClient gets a kv.Client.
	GetClient() kv.Client

	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}

	// ClearValue clears the value associated with this context for key.
	ClearValue(key fmt.Stringer)

	GetSessionVars() *variable.SessionVars

	GetSessionManager() util.SessionManager

	// RefreshTxnCtx commits old transaction without retry,
	// and creates a new transaction.
	// now just for load data and batch insert.
	RefreshTxnCtx(context.contextctx) error

	// InitTxnWithStartTS initializes a transaction with startTS.
	// It should be called right before we builds an Interlock.
	InitTxnWithStartTS(startTS uint64) error

	// GetStore returns the store of session.
	GetStore() kv.Storage

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
	StmtAddDirtyTableOP(op int, physicalID int64, handle kv.Handle)
	// dbsOwnerChecker returns owner.dbsOwnerChecker.
	dbsOwnerChecker() owner.dbsOwnerChecker
	// AddTableLock adds table lock to the session lock map.
	AddTableLock([]serial.TableLockTpInfo)
	// ReleaseTableLocks releases table locks in the session lock map.
	ReleaseTableLocks(locks []serial.TableLockTpInfo)
	// ReleaseTableLockByTableID releases table locks in the session lock map by table ID.
	ReleaseTableLockByTableIDs(tableIDs []int64)
	// CheckTableLocked checks the table lock.
	CheckTableLocked(tblID int64) (bool, serial.TableLockType)
	// GetAllTableLocks gets all table locks table id and db id hold by the session.
	GetAllTableLocks() []serial.TableLockTpInfo
	// ReleaseAllTableLocks releases all table locks hold by the session.
	ReleaseAllTableLocks()
	// HasLockedTables uses to check whether this session locked any tables.
	HasLockedTables() bool
	// PrepareTSFuture uses to prepare timestamp by future.
	PrepareTSFuture(ctx context.contextctx)
}