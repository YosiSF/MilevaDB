//INTERLOCKyright 2020 WHTCORPS INC ALL RIGHTS RESERVED
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions.

package contextnet

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/MilevaDB/BerolinaSQL"
	"github.com/whtcorpsinc/MilevaDB/BerolinaSQL/ast"
	"github.com/whtcorpsinc/MilevaDB/BerolinaSQL/mysql"
	"github.com/whtcorpsinc/MilevaDB/BerolinaSQL/terror"
	"github.com/whtcorpsinc/MilevaDB/Interlock"
	"github.com/whtcorpsinc/MilevaDB/causetnetctx"
	"github.com/whtcorpsinc/MilevaDB/causetnetctx/variable"
	"github.com/whtcorpsinc/MilevaDB/config"
	"github.com/whtcorpsinc/MilevaDB/ekv"
	"github.com/whtcorpsinc/MilevaDB/errno"
	"github.com/whtcorpsinc/MilevaDB/namespace"
	"github.com/whtcorpsinc/MilevaDB/util"
	"github.com/whtcorpsinc/MilevaDB/util/chunk"
	"github.com/whtcorpsinc/MilevaDB/util/logutil"
	"github.com/whtcorpsinc/MilevaDB/util/sqlexec"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

type domainMap struct {
	domains map[string]*namespace.Domain
	mu      sync.Mutex
}

func (dm *domainMap) Get(store ekv.Storage) (d *namespace.Domain, err error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// If this is the only namespace instance, and the caller doesn't provide store.
	if len(dm.domains) == 1 && store == nil {
		for _, r := range dm.domains {
			return r, nil
		}
	}

	key := store.UUID()
	d = dm.domains[key]
	if d != nil {
		return
	}

	dbsLease := time.Duration(atomic.LoadInt64(&schemaLease))
	statisticLease := time.Duration(atomic.LoadInt64(&statsLease))
	err = util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (retry bool, err1 error) {
		logutil.BgLogger().Info("new namespace",
			zap.String("store", store.UUID()),
			zap.Stringer("dbs lease", dbsLease),
			zap.Stringer("stats lease", statisticLease))
		factory := createCausetnetFunc(store)
		sysFactory := createCausetnetWithDomainFunc(store)
		d = namespace.NewDomain(store, dbsLease, statisticLease, factory)
		err1 = d.Init(dbsLease, sysFactory)
		if err1 != nil {
			// If we don't clean it, there are some dirty data when retrying the function of Init.
			d.Close()
			logutil.BgLogger().Error("[dbs] init namespace failed",
				zap.Error(err1))
		}
		return true, err1
	})
	if err != nil {
		return nil, err
	}
	dm.domains[key] = d

	return
}

func (dm *domainMap) Delete(store ekv.Storage) {
	dm.mu.Lock()
	delete(dm.domains, store.UUID())
	dm.mu.Unlock()
}

var (
	domap = &domainMap{
		domains: map[string]*namespace.Domain{},
	}
	// store.UUID()-> IfBootstrapped
	storeBootstrapped     = make(map[string]bool)
	storeBootstrappedLock sync.Mutex

	// schemaLease is the time for re-updating remote schema.
	// In online DBS, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schema.
	// Default schema lease time is 1 second, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	// For production, you should set a big schema lease, like 300s+.
	schemaLease = int64(1 * time.Second)

	// statsLease is the time for reload stats table.
	statsLease = int64(3 * time.Second)
)

// ResetStoreForWithEinsteinDBTest is only used in the test code.
// TODO: Remove domap and storeBootstrapped. Use store.SetOption() to do it.
func ResetStoreForWithEinsteinDBTest(store ekv.Storage) {
	domap.Delete(store)
	unsetStoreBootstrapped(store.UUID())
}

func setStoreBootstrapped(storeUUID string) {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	storeBootstrapped[storeUUID] = true
}

// unsetStoreBootstrapped delete store uuid from stored bootstrapped map.
// currently this function only used for test.
func unsetStoreBootstrapped(storeUUID string) {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	delete(storeBootstrapped, storeUUID)
}

// SetSchemaLease changes the default schema lease time for DBS.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	atomic.StoreInt64(&schemaLease, int64(lease))
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	atomic.StoreInt64(&statsLease, int64(lease))
}

// DisableStats4Test disables the stats for tests.
func DisableStats4Test() {
	SetStatsLease(-1)
}

// Parse parses a query string to raw ast.StmtNode.
func Parse(ctx causetnetctx.Context, src string) ([]ast.StmtNode, error) {
	logutil.BgLogger().Debug("compiling", zap.String("source", src))
	charset, collation := ctx.GetCausetNetVars().GetCharsetInfo()
	p := BerolinaSQL.New()
	p.EnableWindowFunc(ctx.GetCausetNetVars().EnableWindowFunction)
	p.SetSQLMode(ctx.GetCausetNetVars().SQLMode)
	stmts, warns, err := p.Parse(src, charset, collation)
	for _, warn := range warns {
		ctx.GetCausetNetVars().StmtCtx.AppendWarning(warn)
	}
	if err != nil {
		logutil.BgLogger().Warn("compiling",
			zap.String("source", src),
			zap.Error(err))
		return nil, err
	}
	return stmts, nil
}

// Compile is safe for concurrent use by multiple goroutines.
func Compile(ctx context.Context, sctx causetnetctx.Context, stmtNode ast.StmtNode) (sqlexec.Statement, error) {
	compiler := Interlock.Compiler{Ctx: sctx}
	stmt, err := compiler.Compile(ctx, stmtNode)
	return stmt, err
}

func recordAbortTxnDuration(sessVars *variable.CausetNetVars) {
	duration := time.Since(sessVars.TxnCtx.CreateTime).Seconds()
	if sessVars.TxnCtx.IsPessimistic {
		transactionDurationPessimisticAbort.Observe(duration)
	} else {
		transactionDurationOptimisticAbort.Observe(duration)
	}
}

func finishStmt(ctx context.Context, se *causetnet, meetsErr error, sql sqlexec.Statement) error {
	err := autoCommitAfterStmt(ctx, se, meetsErr, sql)
	if se.txn.pending() {
		// After run statement finish, txn state is still pending means the
		// statement never need a Txn(), such as:
		//
		// set @@MilevaDB_general_log = 1
		// set @@autocommit = 0
		// select 1
		//
		// Reset txn state to invalid to dispose the pending start ts.
		se.txn.changeToInvalid()
	}
	if err != nil {
		return err
	}
	return checkStmtLimit(ctx, se)
}

func autoCommitAfterStmt(ctx context.Context, se *causetnet, meetsErr error, sql sqlexec.Statement) error {
	sessVars := se.causetnetVars
	if meetsErr != nil {
		if !sessVars.InTxn() {
			logutil.BgLogger().Info("rollbackTxn for dbs/autocommit failed")
			se.RollbackTxn(ctx)
			recordAbortTxnDuration(sessVars)
		} else if se.txn.Valid() && se.txn.IsPessimistic() && Interlock.ErrDeadlock.Equal(meetsErr) {
			logutil.BgLogger().Info("rollbackTxn for deadlock", zap.Uint64("txn", se.txn.StartTS()))
			se.RollbackTxn(ctx)
			recordAbortTxnDuration(sessVars)
		}
		return meetsErr
	}

	if !sessVars.InTxn() {
		if err := se.CommitTxn(ctx); err != nil {
			if _, ok := sql.(*Interlock.ExecStmt).StmtNode.(*ast.CommitStmt); ok {
				err = errors.Annotatef(err, "previous statement: %s", se.GetCausetNetVars().PrevStmt)
			}
			return err
		}
		return nil
	}
	return nil
}

func checkStmtLimit(ctx context.Context, se *causetnet) error {
	// If the user insert, insert, insert ... but never commit, MilevaDB would OOM.
	// So we limit the statement count in a transaction here.
	var err error
	sessVars := se.GetCausetNetVars()
	history := GetHistory(se)
	if history.Count() > int(config.GetGlobalConfig().Performance.StmtCountLimit) {
		if !sessVars.BatchCommit {
			se.RollbackTxn(ctx)
			return errors.Errorf("statement count %d exceeds the transaction limitation, autocommit = %t",
				history.Count(), sessVars.IsAutocommit())
		}
		err = se.NewTxn(ctx)
		// The transaction does not committed yet, we need to keep it in transaction.
		// The last history could not be "commit"/"rollback" statement.
		// It means it is impossible to start a new transaction at the end of the transaction.
		// Because after the server executed "commit"/"rollback" statement, the causetnet is out of the transaction.
		sessVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
	}
	return err
}

// GetHistory get all stmtHistory in current txn. Exported only for test.
func GetHistory(ctx causetnetctx.Context) *StmtHistory {
	hist, ok := ctx.GetCausetNetVars().TxnCtx.History.(*StmtHistory)
	if ok {
		return hist
	}
	hist = new(StmtHistory)
	ctx.GetCausetNetVars().TxnCtx.History = hist
	return hist
}

// GetRows4Test gets all the rows from a RecordSet, only used for test.
func GetRows4Test(ctx context.Context, sctx causetnetctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	req := rs.NewChunk()
	// Must reuse `req` for imitating server.(*clientConn).writeChunks
	for {
		err := rs.Next(ctx, req)
		if err != nil {
			return nil, err
		}
		if req.NumRows() == 0 {
			break
		}

		iteron := chunk.NewIterator4Chunk(req.INTERLOCKyConstruct())
		for row := iteron.Begin(); row != iteron.End(); row = iteron.Next() {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// ResultSetToStringSlice changes the RecordSet to [][]string.
func ResultSetToStringSlice(ctx context.Context, s Causetnet, rs sqlexec.RecordSet) ([][]string, error) {
	rows, err := GetRows4Test(ctx, s, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i := range rows {
		row := rows[i]
		iRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				iRow[j], err = d.ToString()
				if err != nil {
					return nil, err
				}
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}

// Causetnet errors.
var (
	ErrForUpdateCantRetry = terror.ClassCausetnet.New(errno.ErrForUpdateCantRetry, errno.MySQLErrName[errno.ErrForUpdateCantRetry])
)
