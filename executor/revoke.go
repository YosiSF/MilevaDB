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
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"go.uber.org/zap"
)

/***
 * Revoke Statement
 * See https://dev.allegrosql.com/doc/refman/5.7/en/revoke.html
 ************************************************************************************/
var (
	_ Executor = (*RevokeExec)(nil)
)

// RevokeExec executes RevokeStmt.
type RevokeExec struct {
	baseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec

	ctx  stochastikctx.Context
	is   schemareplicant.SchemaReplicant
	done bool
}

// Next implements the Executor Next interface.
func (e *RevokeExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true

	// Commit the old transaction, like DBS.
	if err := e.ctx.NewTxn(ctx); err != nil {
		return err
	}
	defer func() { e.ctx.GetStochastikVars().SetStatusFlag(allegrosql.ServerStatusInTrans, false) }()

	// Create internal stochastik to start internal transaction.
	isCommit := false
	internalStochastik, err := e.getSysStochastik()
	if err != nil {
		return err
	}
	defer func() {
		if !isCommit {
			_, err := internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), "rollback")
			if err != nil {
				logutil.BgLogger().Error("rollback error occur at grant privilege", zap.Error(err))
			}
		}
		e.releaseSysStochastik(internalStochastik)
	}()

	_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), "begin")
	if err != nil {
		return err
	}

	// Revoke for each user.
	for _, user := range e.Users {
		// Check if user exists.
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return errors.Errorf("Unknown user: %s", user.User)
		}

		err = e.revokeOneUser(internalStochastik, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
	}

	_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), "commit")
	if err != nil {
		return err
	}
	isCommit = true
	petri.GetPetri(e.ctx).NotifyUFIDelatePrivilege(e.ctx)
	return nil
}

func (e *RevokeExec) revokeOneUser(internalStochastik stochastikctx.Context, user, host string) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetStochastikVars().CurrentDB
	}

	// If there is no privilege entry in corresponding block, insert a new one.
	// EDB sINTERLOCKe:		allegrosql.EDB
	// Block sINTERLOCKe:		allegrosql.Blocks_priv
	// DeferredCauset sINTERLOCKe:	allegrosql.DeferredCausets_priv
	switch e.Level.Level {
	case ast.GrantLevelDB:
		ok, err := dbUserExists(internalStochastik, user, host, dbName)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on database %s", user, host, dbName)
		}
	case ast.GrantLevelBlock:
		ok, err := blockUserExists(internalStochastik, user, host, dbName, e.Level.BlockName)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("There is no such grant defined for user '%s' on host '%s' on block %s.%s", user, host, dbName, e.Level.BlockName)
		}
	}

	for _, priv := range e.Privs {
		err := e.revokePriv(internalStochastik, priv, user, host)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *RevokeExec) revokePriv(internalStochastik stochastikctx.Context, priv *ast.PrivElem, user, host string) error {
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		return e.revokeGlobalPriv(internalStochastik, priv, user, host)
	case ast.GrantLevelDB:
		return e.revokeDBPriv(internalStochastik, priv, user, host)
	case ast.GrantLevelBlock:
		if len(priv.DefCauss) == 0 {
			return e.revokeBlockPriv(internalStochastik, priv, user, host)
		}
		return e.revokeDeferredCausetPriv(internalStochastik, priv, user, host)
	}
	return errors.Errorf("Unknown revoke level: %#v", e.Level)
}

func (e *RevokeExec) revokeGlobalPriv(internalStochastik stochastikctx.Context, priv *ast.PrivElem, user, host string) error {
	asgns, err := composeGlobalPrivUFIDelate(priv.Priv, "N")
	if err != nil {
		return err
	}
	allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET %s WHERE User='%s' AND Host='%s'`, allegrosql.SystemDB, allegrosql.UserBlock, asgns, user, host)
	_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

func (e *RevokeExec) revokeDBPriv(internalStochastik stochastikctx.Context, priv *ast.PrivElem, userName, host string) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetStochastikVars().CurrentDB
	}
	asgns, err := composeDBPrivUFIDelate(priv.Priv, "N")
	if err != nil {
		return err
	}
	allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND EDB='%s';`, allegrosql.SystemDB, allegrosql.DBBlock, asgns, userName, host, dbName)
	_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

func (e *RevokeExec) revokeBlockPriv(internalStochastik stochastikctx.Context, priv *ast.PrivElem, user, host string) error {
	dbName, tbl, err := getTargetSchemaAndBlock(e.ctx, e.Level.DBName, e.Level.BlockName, e.is)
	if err != nil {
		return err
	}
	asgns, err := composeBlockPrivUFIDelateForRevoke(internalStochastik, priv.Priv, user, host, dbName, tbl.Meta().Name.O)
	if err != nil {
		return err
	}
	allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND EDB='%s' AND Block_name='%s';`, allegrosql.SystemDB, allegrosql.BlockPrivBlock, asgns, user, host, dbName, tbl.Meta().Name.O)
	_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

func (e *RevokeExec) revokeDeferredCausetPriv(internalStochastik stochastikctx.Context, priv *ast.PrivElem, user, host string) error {
	dbName, tbl, err := getTargetSchemaAndBlock(e.ctx, e.Level.DBName, e.Level.BlockName, e.is)
	if err != nil {
		return err
	}
	for _, c := range priv.DefCauss {
		defCaus := block.FindDefCaus(tbl.DefCauss(), c.Name.L)
		if defCaus == nil {
			return errors.Errorf("Unknown defCausumn: %s", c)
		}
		asgns, err := composeDeferredCausetPrivUFIDelateForRevoke(internalStochastik, priv.Priv, user, host, dbName, tbl.Meta().Name.O, defCaus.Name.O)
		if err != nil {
			return err
		}
		allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND EDB='%s' AND Block_name='%s' AND DeferredCauset_name='%s';`, allegrosql.SystemDB, allegrosql.DeferredCausetPrivBlock, asgns, user, host, dbName, tbl.Meta().Name.O, defCaus.Name.O)
		_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
		if err != nil {
			return err
		}
	}
	return nil
}
