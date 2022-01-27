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
	"os"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/plugin"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"go.uber.org/zap"
)

var (
	transactionDurationPessimisticRollback = metrics.TransactionDuration.WithLabelValues(metrics.LblPessimistic, metrics.LblRollback)
	transactionDurationOptimisticRollback  = metrics.TransactionDuration.WithLabelValues(metrics.LblOptimistic, metrics.LblRollback)
)

// SimpleExec represents simple statement executor.
// For statements do simple execution.
// includes `UseStmt`, 'SetStmt`, `DoStmt`,
// `BeginStmt`, `CommitStmt`, `RollbackStmt`.
// TODO: list all simple statements.
type SimpleExec struct {
	baseExecutor

	Statement ast.StmtNode
	done      bool
	is        schemareplicant.SchemaReplicant
}

func (e *baseExecutor) getSysStochastik() (stochastikctx.Context, error) {
	dom := petri.GetPetri(e.ctx)
	sysStochastikPool := dom.SysStochastikPool()
	ctx, err := sysStochastikPool.Get()
	if err != nil {
		return nil, err
	}
	restrictedCtx := ctx.(stochastikctx.Context)
	restrictedCtx.GetStochastikVars().InRestrictedALLEGROSQL = true
	return restrictedCtx, nil
}

func (e *baseExecutor) releaseSysStochastik(ctx stochastikctx.Context) {
	if ctx == nil {
		return
	}
	dom := petri.GetPetri(e.ctx)
	sysStochastikPool := dom.SysStochastikPool()
	if _, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), "rollback"); err != nil {
		ctx.(pools.Resource).Close()
		return
	}
	sysStochastikPool.Put(ctx.(pools.Resource))
}

// Next implements the Executor Next interface.
func (e *SimpleExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}

	if e.autoNewTxn() {
		// Commit the old transaction, like DBS.
		if err := e.ctx.NewTxn(ctx); err != nil {
			return err
		}
		defer func() { e.ctx.GetStochastikVars().SetStatusFlag(allegrosql.ServerStatusInTrans, false) }()
	}

	switch x := e.Statement.(type) {
	case *ast.GrantRoleStmt:
		err = e.executeGrantRole(x)
	case *ast.UseStmt:
		err = e.executeUse(x)
	case *ast.FlushStmt:
		err = e.executeFlush(x)
	case *ast.AlterInstanceStmt:
		err = e.executeAlterInstance(x)
	case *ast.BeginStmt:
		err = e.executeBegin(ctx, x)
	case *ast.CommitStmt:
		e.executeCommit(x)
	case *ast.RollbackStmt:
		err = e.executeRollback(x)
	case *ast.CreateUserStmt:
		err = e.executeCreateUser(ctx, x)
	case *ast.AlterUserStmt:
		err = e.executeAlterUser(x)
	case *ast.DropUserStmt:
		err = e.executeDropUser(x)
	case *ast.SetPwdStmt:
		err = e.executeSetPwd(x)
	case *ast.KillStmt:
		err = e.executeKillStmt(x)
	case *ast.BinlogStmt:
		// We just ignore it.
		return nil
	case *ast.DropStatsStmt:
		err = e.executeDropStats(x)
	case *ast.SetRoleStmt:
		err = e.executeSetRole(x)
	case *ast.RevokeRoleStmt:
		err = e.executeRevokeRole(x)
	case *ast.SetDefaultRoleStmt:
		err = e.executeSetDefaultRole(x)
	case *ast.ShutdownStmt:
		err = e.executeShutdown(x)
	case *ast.CreateStatisticsStmt:
		err = e.executeCreateStatistics(x)
	case *ast.DropStatisticsStmt:
		err = e.executeDropStatistics(x)
	case *ast.AdminStmt:
		err = e.executeAdminReloadStatistics(x)
	}
	e.done = true
	return err
}

func (e *SimpleExec) setDefaultRoleNone(s *ast.SetDefaultRoleStmt) error {
	restrictedCtx, err := e.getSysStochastik()
	if err != nil {
		return err
	}
	defer e.releaseSysStochastik(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.ALLEGROSQLExecutor)
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}
	for _, u := range s.UserList {
		if u.Hostname == "" {
			u.Hostname = "%"
		}
		allegrosql := fmt.Sprintf("DELETE IGNORE FROM allegrosql.default_roles WHERE USER='%s' AND HOST='%s';", u.Username, u.Hostname)
		if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", allegrosql))
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleRegular(s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	for _, role := range s.RoleList {
		exists, err := userExists(e.ctx, role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", role.String())
		}
	}

	restrictedCtx, err := e.getSysStochastik()
	if err != nil {
		return err
	}
	defer e.releaseSysStochastik(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.ALLEGROSQLExecutor)
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}
	for _, user := range s.UserList {
		if user.Hostname == "" {
			user.Hostname = "%"
		}
		allegrosql := fmt.Sprintf("DELETE IGNORE FROM allegrosql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
		if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", allegrosql))
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		for _, role := range s.RoleList {
			allegrosql := fmt.Sprintf("INSERT IGNORE INTO allegrosql.default_roles values('%s', '%s', '%s', '%s');", user.Hostname, user.Username, role.Hostname, role.Username)
			checker := privilege.GetPrivilegeManager(e.ctx)
			ok := checker.FindEdge(e.ctx, role, user)
			if ok {
				if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
					logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", allegrosql))
					if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
						return rollbackErr
					}
					return err
				}
			} else {
				if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
					return rollbackErr
				}
				return ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleAll(s *ast.SetDefaultRoleStmt) error {
	for _, user := range s.UserList {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("SET DEFAULT ROLE", user.String())
		}
	}
	restrictedCtx, err := e.getSysStochastik()
	if err != nil {
		return err
	}
	defer e.releaseSysStochastik(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.ALLEGROSQLExecutor)
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}
	for _, user := range s.UserList {
		if user.Hostname == "" {
			user.Hostname = "%"
		}
		allegrosql := fmt.Sprintf("DELETE IGNORE FROM allegrosql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
		if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", allegrosql))
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		allegrosql = fmt.Sprintf("INSERT IGNORE INTO allegrosql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) "+
			"SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM allegrosql.role_edges WHERE TO_HOST='%s' AND TO_USER='%s';", user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) setDefaultRoleForCurrentUser(s *ast.SetDefaultRoleStmt) (err error) {
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, allegrosql := s.UserList[0], ""
	if user.Hostname == "" {
		user.Hostname = "%"
	}
	switch s.SetRoleOpt {
	case ast.SetRoleNone:
		allegrosql = fmt.Sprintf("DELETE IGNORE FROM allegrosql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
	case ast.SetRoleAll:
		allegrosql = fmt.Sprintf("INSERT IGNORE INTO allegrosql.default_roles(HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) "+
			"SELECT TO_HOST,TO_USER,FROM_HOST,FROM_USER FROM allegrosql.role_edges WHERE TO_HOST='%s' AND TO_USER='%s';", user.Hostname, user.Username)
	case ast.SetRoleRegular:
		allegrosql = "INSERT IGNORE INTO allegrosql.default_roles values"
		for i, role := range s.RoleList {
			ok := checker.FindEdge(e.ctx, role, user)
			if !ok {
				return ErrRoleNotGranted.GenWithStackByArgs(role.String(), user.String())
			}
			allegrosql += fmt.Sprintf("('%s', '%s', '%s', '%s')", user.Hostname, user.Username, role.Hostname, role.Username)
			if i != len(s.RoleList)-1 {
				allegrosql += ","
			}
		}
	}

	restrictedCtx, err := e.getSysStochastik()
	if err != nil {
		return err
	}
	defer e.releaseSysStochastik(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.ALLEGROSQLExecutor)

	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}

	deleteALLEGROSQL := fmt.Sprintf("DELETE IGNORE FROM allegrosql.default_roles WHERE USER='%s' AND HOST='%s';", user.Username, user.Hostname)
	if _, err := sqlExecutor.Execute(context.Background(), deleteALLEGROSQL); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", allegrosql))
		if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}

	if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", allegrosql))
		if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	return nil
}

func (e *SimpleExec) executeSetDefaultRole(s *ast.SetDefaultRoleStmt) (err error) {
	stochastikVars := e.ctx.GetStochastikVars()
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}

	if len(s.UserList) == 1 && stochastikVars.User != nil {
		u, h := s.UserList[0].Username, s.UserList[0].Hostname
		if u == stochastikVars.User.Username && h == stochastikVars.User.AuthHostname {
			err = e.setDefaultRoleForCurrentUser(s)
			petri.GetPetri(e.ctx).NotifyUFIDelatePrivilege(e.ctx)
			return
		}
	}

	activeRoles := stochastikVars.ActiveRoles
	if !checker.RequestVerification(activeRoles, allegrosql.SystemDB, allegrosql.DefaultRoleBlock, "", allegrosql.UFIDelatePriv) {
		if !checker.RequestVerification(activeRoles, "", "", "", allegrosql.CreateUserPriv) {
			return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
		}
	}

	switch s.SetRoleOpt {
	case ast.SetRoleAll:
		err = e.setDefaultRoleAll(s)
	case ast.SetRoleNone:
		err = e.setDefaultRoleNone(s)
	case ast.SetRoleRegular:
		err = e.setDefaultRoleRegular(s)
	}
	if err != nil {
		return
	}
	petri.GetPetri(e.ctx).NotifyUFIDelatePrivilege(e.ctx)
	return
}

func (e *SimpleExec) setRoleRegular(s *ast.SetRoleStmt) error {
	// Deal with ALLEGROALLEGROSQL like `SET ROLE role1, role2;`
	checkDup := make(map[string]*auth.RoleIdentity, len(s.RoleList))
	// Check whether RoleNameList contain duplicate role name.
	for _, r := range s.RoleList {
		key := r.String()
		checkDup[key] = r
	}
	roleList := make([]*auth.RoleIdentity, 0, 10)
	for _, v := range checkDup {
		roleList = append(roleList, v)
	}

	checker := privilege.GetPrivilegeManager(e.ctx)
	ok, roleName := checker.ActiveRoles(e.ctx, roleList)
	if !ok {
		u := e.ctx.GetStochastikVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleAll(s *ast.SetRoleStmt) error {
	// Deal with ALLEGROALLEGROSQL like `SET ROLE ALL;`
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, host := e.ctx.GetStochastikVars().User.AuthUsername, e.ctx.GetStochastikVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)
	ok, roleName := checker.ActiveRoles(e.ctx, roles)
	if !ok {
		u := e.ctx.GetStochastikVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleAllExcept(s *ast.SetRoleStmt) error {
	// Deal with ALLEGROALLEGROSQL like `SET ROLE ALL EXCEPT role1, role2;`
	for _, r := range s.RoleList {
		if r.Hostname == "" {
			r.Hostname = "%"
		}
	}
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, host := e.ctx.GetStochastikVars().User.AuthUsername, e.ctx.GetStochastikVars().User.AuthHostname
	roles := checker.GetAllRoles(user, host)

	filter := func(arr []*auth.RoleIdentity, f func(*auth.RoleIdentity) bool) []*auth.RoleIdentity {
		i, j := 0, 0
		for i = 0; i < len(arr); i++ {
			if f(arr[i]) {
				arr[j] = arr[i]
				j++
			}
		}
		return arr[:j]
	}
	banned := func(r *auth.RoleIdentity) bool {
		for _, ban := range s.RoleList {
			if ban.Hostname == r.Hostname && ban.Username == r.Username {
				return false
			}
		}
		return true
	}

	afterExcept := filter(roles, banned)
	ok, roleName := checker.ActiveRoles(e.ctx, afterExcept)
	if !ok {
		u := e.ctx.GetStochastikVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleDefault(s *ast.SetRoleStmt) error {
	// Deal with ALLEGROALLEGROSQL like `SET ROLE DEFAULT;`
	checker := privilege.GetPrivilegeManager(e.ctx)
	user, host := e.ctx.GetStochastikVars().User.AuthUsername, e.ctx.GetStochastikVars().User.AuthHostname
	roles := checker.GetDefaultRoles(user, host)
	ok, roleName := checker.ActiveRoles(e.ctx, roles)
	if !ok {
		u := e.ctx.GetStochastikVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) setRoleNone(s *ast.SetRoleStmt) error {
	// Deal with ALLEGROALLEGROSQL like `SET ROLE NONE;`
	checker := privilege.GetPrivilegeManager(e.ctx)
	roles := make([]*auth.RoleIdentity, 0)
	ok, roleName := checker.ActiveRoles(e.ctx, roles)
	if !ok {
		u := e.ctx.GetStochastikVars().User
		return ErrRoleNotGranted.GenWithStackByArgs(roleName, u.String())
	}
	return nil
}

func (e *SimpleExec) executeSetRole(s *ast.SetRoleStmt) error {
	switch s.SetRoleOpt {
	case ast.SetRoleRegular:
		return e.setRoleRegular(s)
	case ast.SetRoleAll:
		return e.setRoleAll(s)
	case ast.SetRoleAllExcept:
		return e.setRoleAllExcept(s)
	case ast.SetRoleNone:
		return e.setRoleNone(s)
	case ast.SetRoleDefault:
		return e.setRoleDefault(s)
	}
	return nil
}

func (e *SimpleExec) dbAccessDenied(dbname string) error {
	user := e.ctx.GetStochastikVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return ErrDBaccessDenied.GenWithStackByArgs(u, h, dbname)
}

func (e *SimpleExec) executeUse(s *ast.UseStmt) error {
	dbname := perceptron.NewCIStr(s.DBName)

	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetStochastikVars().User != nil {
		if !checker.DBIsVisible(e.ctx.GetStochastikVars().ActiveRoles, dbname.String()) {
			return e.dbAccessDenied(dbname.O)
		}
	}

	dbinfo, exists := e.is.SchemaByName(dbname)
	if !exists {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(dbname)
	}
	e.ctx.GetStochastikVars().CurrentDBChanged = dbname.O != e.ctx.GetStochastikVars().CurrentDB
	e.ctx.GetStochastikVars().CurrentDB = dbname.O
	// character_set_database is the character set used by the default database.
	// The server sets this variable whenever the default database changes.
	// See http://dev.allegrosql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_character_set_database
	stochastikVars := e.ctx.GetStochastikVars()
	err := stochastikVars.SetSystemVar(variable.CharsetDatabase, dbinfo.Charset)
	if err != nil {
		return err
	}
	dbDefCauslate := dbinfo.DefCauslate
	if dbDefCauslate == "" {
		// Since we have checked the charset, the dbDefCauslate here shouldn't be "".
		dbDefCauslate = getDefaultDefCauslate(dbinfo.Charset)
	}
	return stochastikVars.SetSystemVar(variable.DefCauslationDatabase, dbDefCauslate)
}

func (e *SimpleExec) executeBegin(ctx context.Context, s *ast.BeginStmt) error {
	// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
	// need to call NewTxn, which commits the existing transaction and begins a new one.
	txnCtx := e.ctx.GetStochastikVars().TxnCtx
	if txnCtx.History != nil {
		err := e.ctx.NewTxn(ctx)
		if err != nil {
			return err
		}
	}
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK. The autocommit mode then
	// reverts to its previous state.
	e.ctx.GetStochastikVars().SetStatusFlag(allegrosql.ServerStatusInTrans, true)
	// Call ctx.Txn(true) to active pending txn.
	txnMode := s.Mode
	if txnMode == "" {
		txnMode = e.ctx.GetStochastikVars().TxnMode
	}
	if txnMode == ast.Pessimistic {
		e.ctx.GetStochastikVars().TxnCtx.IsPessimistic = true
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	if e.ctx.GetStochastikVars().TxnCtx.IsPessimistic {
		txn.SetOption(ekv.Pessimistic, true)
	}
	return nil
}

func (e *SimpleExec) executeRevokeRole(s *ast.RevokeRoleStmt) error {
	for _, role := range s.Roles {
		exists, err := userExists(e.ctx, role.Username, role.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
		}
	}

	restrictedCtx, err := e.getSysStochastik()
	if err != nil {
		return err
	}
	defer e.releaseSysStochastik(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.ALLEGROSQLExecutor)

	// begin a transaction to insert role graph edges.
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return errors.Trace(err)
	}
	for _, user := range s.Users {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			return errors.Trace(err)
		}
		if !exists {
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return errors.Trace(err)
			}
			return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", user.String())
		}
		for _, role := range s.Roles {
			if role.Hostname == "" {
				role.Hostname = "%"
			}
			allegrosql := fmt.Sprintf(`DELETE IGNORE FROM %s.%s WHERE FROM_HOST='%s' and FROM_USER='%s' and TO_HOST='%s' and TO_USER='%s'`, allegrosql.SystemDB, allegrosql.RoleEdgeBlock, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
				if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
					return errors.Trace(err)
				}
				return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}
			allegrosql = fmt.Sprintf(`DELETE IGNORE FROM %s.%s WHERE DEFAULT_ROLE_HOST='%s' and DEFAULT_ROLE_USER='%s' and HOST='%s' and USER='%s'`, allegrosql.SystemDB, allegrosql.DefaultRoleBlock, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
				if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
					return errors.Trace(err)
				}
				return ErrCannotUser.GenWithStackByArgs("REVOKE ROLE", role.String())
			}
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	petri.GetPetri(e.ctx).NotifyUFIDelatePrivilege(e.ctx)
	return nil
}

func (e *SimpleExec) executeCommit(s *ast.CommitStmt) {
	e.ctx.GetStochastikVars().SetStatusFlag(allegrosql.ServerStatusInTrans, false)
}

func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	sessVars := e.ctx.GetStochastikVars()
	logutil.BgLogger().Debug("execute rollback statement", zap.Uint64("conn", sessVars.ConnectionID))
	sessVars.SetStatusFlag(allegrosql.ServerStatusInTrans, false)
	txn, err := e.ctx.Txn(false)
	if err != nil {
		return err
	}
	if txn.Valid() {
		duration := time.Since(sessVars.TxnCtx.CreateTime).Seconds()
		if sessVars.TxnCtx.IsPessimistic {
			transactionDurationPessimisticRollback.Observe(duration)
		} else {
			transactionDurationOptimisticRollback.Observe(duration)
		}
		sessVars.TxnCtx.ClearDelta()
		return txn.Rollback()
	}
	return nil
}

func (e *SimpleExec) executeCreateUser(ctx context.Context, s *ast.CreateUserStmt) error {
	// Check `CREATE USER` privilege.
	if !config.GetGlobalConfig().Security.SkipGrantBlock {
		checker := privilege.GetPrivilegeManager(e.ctx)
		if checker == nil {
			return errors.New("miss privilege checker")
		}
		activeRoles := e.ctx.GetStochastikVars().ActiveRoles
		if !checker.RequestVerification(activeRoles, allegrosql.SystemDB, allegrosql.UserBlock, "", allegrosql.InsertPriv) {
			if s.IsCreateRole {
				if !checker.RequestVerification(activeRoles, "", "", "", allegrosql.CreateRolePriv) &&
					!checker.RequestVerification(activeRoles, "", "", "", allegrosql.CreateUserPriv) {
					return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE ROLE or CREATE USER")
				}
			}
			if !s.IsCreateRole && !checker.RequestVerification(activeRoles, "", "", "", allegrosql.CreateUserPriv) {
				return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE User")
			}
		}
	}

	privData, err := tlsOption2GlobalPriv(s.TLSOptions)
	if err != nil {
		return err
	}

	users := make([]string, 0, len(s.Specs))
	privs := make([]string, 0, len(s.Specs))
	for _, spec := range s.Specs {
		exists, err1 := userExists(e.ctx, spec.User.Username, spec.User.Hostname)
		if err1 != nil {
			return err1
		}
		if exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			if !s.IfNotExists {
				if s.IsCreateRole {
					return ErrCannotUser.GenWithStackByArgs("CREATE ROLE", user)
				}
				return ErrCannotUser.GenWithStackByArgs("CREATE USER", user)
			}
			err := schemareplicant.ErrUserAlreadyExists.GenWithStackByArgs(user)
			e.ctx.GetStochastikVars().StmtCtx.AppendNote(err)
			continue
		}
		pwd, ok := spec.EncodedPassword()
		if !ok {
			return errors.Trace(ErrPasswordFormat)
		}
		user := fmt.Sprintf(`('%s', '%s', '%s')`, spec.User.Hostname, spec.User.Username, pwd)
		if s.IsCreateRole {
			user = fmt.Sprintf(`('%s', '%s', '%s', 'Y')`, spec.User.Hostname, spec.User.Username, pwd)
		}
		users = append(users, user)

		if len(privData) != 0 {
			priv := fmt.Sprintf(`('%s', '%s', '%s')`, spec.User.Hostname, spec.User.Username, replog.String(privData))
			privs = append(privs, priv)
		}
	}
	if len(users) == 0 {
		return nil
	}

	allegrosql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, authentication_string) VALUES %s;`, allegrosql.SystemDB, allegrosql.UserBlock, strings.Join(users, ", "))
	if s.IsCreateRole {
		allegrosql = fmt.Sprintf(`INSERT INTO %s.%s (Host, User, authentication_string, Account_locked) VALUES %s;`, allegrosql.SystemDB, allegrosql.UserBlock, strings.Join(users, ", "))
	}

	restrictedCtx, err := e.getSysStochastik()
	if err != nil {
		return err
	}
	defer e.releaseSysStochastik(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.ALLEGROSQLExecutor)

	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return errors.Trace(err)
	}
	_, err = sqlExecutor.Execute(context.Background(), allegrosql)
	if err != nil {
		if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	if len(privs) != 0 {
		allegrosql = fmt.Sprintf("INSERT IGNORE INTO %s.%s (Host, User, Priv) VALUES %s", allegrosql.SystemDB, allegrosql.GlobalPrivBlock, strings.Join(privs, ", "))
		_, err = sqlExecutor.Execute(context.Background(), allegrosql)
		if err != nil {
			if _, rollbackErr := sqlExecutor.Execute(context.Background(), "rollback"); rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return errors.Trace(err)
	}
	petri.GetPetri(e.ctx).NotifyUFIDelatePrivilege(e.ctx)
	return err
}

func (e *SimpleExec) executeAlterUser(s *ast.AlterUserStmt) error {
	if s.CurrentAuth != nil {
		user := e.ctx.GetStochastikVars().User
		if user == nil {
			return errors.New("Stochastik user is empty")
		}
		// Use AuthHostname to search the user record, set Hostname as AuthHostname.
		userINTERLOCKy := *user
		userINTERLOCKy.Hostname = userINTERLOCKy.AuthHostname
		spec := &ast.UserSpec{
			User:    &userINTERLOCKy,
			AuthOpt: s.CurrentAuth,
		}
		s.Specs = []*ast.UserSpec{spec}
	}

	privData, err := tlsOption2GlobalPriv(s.TLSOptions)
	if err != nil {
		return err
	}

	failedUsers := make([]string, 0, len(s.Specs))
	for _, spec := range s.Specs {
		if spec.User.CurrentUser {
			user := e.ctx.GetStochastikVars().User
			spec.User.Username = user.Username
			spec.User.Hostname = user.AuthHostname
		}

		exists, err := userExists(e.ctx, spec.User.Username, spec.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			user := fmt.Sprintf(`'%s'@'%s'`, spec.User.Username, spec.User.Hostname)
			failedUsers = append(failedUsers, user)
			continue
		}
		pwd, ok := spec.EncodedPassword()
		if !ok {
			return errors.Trace(ErrPasswordFormat)
		}
		allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET authentication_string = '%s' WHERE Host = '%s' and User = '%s';`,
			allegrosql.SystemDB, allegrosql.UserBlock, pwd, spec.User.Hostname, spec.User.Username)
		_, _, err = e.ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
		if err != nil {
			failedUsers = append(failedUsers, spec.User.String())
		}

		if len(privData) > 0 {
			allegrosql = fmt.Sprintf("INSERT INTO %s.%s (Host, User, Priv) VALUES ('%s','%s','%s') ON DUPLICATE KEY UFIDelATE Priv = values(Priv)",
				allegrosql.SystemDB, allegrosql.GlobalPrivBlock, spec.User.Hostname, spec.User.Username, replog.String(privData))
			_, _, err = e.ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
			if err != nil {
				failedUsers = append(failedUsers, spec.User.String())
			}
		}
	}
	if len(failedUsers) > 0 {
		// Commit the transaction even if we returns error
		txn, err := e.ctx.Txn(true)
		if err != nil {
			return err
		}
		err = txn.Commit(stochastikctx.SetCommitCtx(context.Background(), e.ctx))
		if err != nil {
			return err
		}
		if !s.IfExists {
			return ErrCannotUser.GenWithStackByArgs("ALTER USER", strings.Join(failedUsers, ","))
		}
		for _, user := range failedUsers {
			err := schemareplicant.ErrUserDropExists.GenWithStackByArgs(user)
			e.ctx.GetStochastikVars().StmtCtx.AppendNote(err)
		}
	}
	petri.GetPetri(e.ctx).NotifyUFIDelatePrivilege(e.ctx)
	return nil
}

func (e *SimpleExec) executeGrantRole(s *ast.GrantRoleStmt) error {
	stochastikVars := e.ctx.GetStochastikVars()
	for i, user := range s.Users {
		if user.CurrentUser {
			s.Users[i].Username = stochastikVars.User.AuthUsername
			s.Users[i].Hostname = stochastikVars.User.AuthHostname
		}
	}

	for _, role := range s.Roles {
		exists, err := userExists(e.ctx, role.Username, role.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", role.String())
		}
	}
	for _, user := range s.Users {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
		}
	}

	restrictedCtx, err := e.getSysStochastik()
	if err != nil {
		return err
	}
	defer e.releaseSysStochastik(restrictedCtx)
	sqlExecutor := restrictedCtx.(sqlexec.ALLEGROSQLExecutor)

	// begin a transaction to insert role graph edges.
	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}

	for _, user := range s.Users {
		for _, role := range s.Roles {
			allegrosql := fmt.Sprintf(`INSERT IGNORE INTO %s.%s (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ('%s','%s','%s','%s')`, allegrosql.SystemDB, allegrosql.RoleEdgeBlock, role.Hostname, role.Username, user.Hostname, user.Username)
			if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
				logutil.BgLogger().Error(fmt.Sprintf("Error occur when executing %s", allegrosql))
				if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
					return err
				}
				return ErrCannotUser.GenWithStackByArgs("GRANT ROLE", user.String())
			}
		}
	}
	if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
		return err
	}
	petri.GetPetri(e.ctx).NotifyUFIDelatePrivilege(e.ctx)
	return nil
}

func (e *SimpleExec) executeDropUser(s *ast.DropUserStmt) error {
	// Check privileges.
	// Check `CREATE USER` privilege.
	if !config.GetGlobalConfig().Security.SkipGrantBlock {
		checker := privilege.GetPrivilegeManager(e.ctx)
		if checker == nil {
			return errors.New("miss privilege checker")
		}
		activeRoles := e.ctx.GetStochastikVars().ActiveRoles
		if !checker.RequestVerification(activeRoles, allegrosql.SystemDB, allegrosql.UserBlock, "", allegrosql.DeletePriv) {
			if s.IsDropRole {
				if !checker.RequestVerification(activeRoles, "", "", "", allegrosql.DropRolePriv) &&
					!checker.RequestVerification(activeRoles, "", "", "", allegrosql.CreateUserPriv) {
					return core.ErrSpecificAccessDenied.GenWithStackByArgs("DROP ROLE or CREATE USER")
				}
			}
			if !s.IsDropRole && !checker.RequestVerification(activeRoles, "", "", "", allegrosql.CreateUserPriv) {
				return core.ErrSpecificAccessDenied.GenWithStackByArgs("CREATE USER")
			}
		}
	}

	failedUsers := make([]string, 0, len(s.UserList))
	sysStochastik, err := e.getSysStochastik()
	defer e.releaseSysStochastik(sysStochastik)
	if err != nil {
		return err
	}
	sqlExecutor := sysStochastik.(sqlexec.ALLEGROSQLExecutor)

	if _, err := sqlExecutor.Execute(context.Background(), "begin"); err != nil {
		return err
	}

	for _, user := range s.UserList {
		exists, err := userExists(e.ctx, user.Username, user.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			if s.IfExists {
				e.ctx.GetStochastikVars().StmtCtx.AppendNote(schemareplicant.ErrUserDropExists.GenWithStackByArgs(user))
			} else {
				failedUsers = append(failedUsers, user.String())
				break
			}
		}

		// begin a transaction to delete a user.
		allegrosql := fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, allegrosql.SystemDB, allegrosql.UserBlock, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from allegrosql.global_priv
		allegrosql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, allegrosql.SystemDB, allegrosql.GlobalPrivBlock, user.Hostname, user.Username)
		if _, err := sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			failedUsers = append(failedUsers, user.String())
			if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
				return err
			}
			continue
		}

		// delete privileges from allegrosql.EDB
		allegrosql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, allegrosql.SystemDB, allegrosql.DBBlock, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete privileges from allegrosql.blocks_priv
		allegrosql = fmt.Sprintf(`DELETE FROM %s.%s WHERE Host = '%s' and User = '%s';`, allegrosql.SystemDB, allegrosql.BlockPrivBlock, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from allegrosql.role_edges
		allegrosql = fmt.Sprintf(`DELETE FROM %s.%s WHERE TO_HOST = '%s' and TO_USER = '%s';`, allegrosql.SystemDB, allegrosql.RoleEdgeBlock, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		allegrosql = fmt.Sprintf(`DELETE FROM %s.%s WHERE FROM_HOST = '%s' and FROM_USER = '%s';`, allegrosql.SystemDB, allegrosql.RoleEdgeBlock, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		// delete relationship from allegrosql.default_roles
		allegrosql = fmt.Sprintf(`DELETE FROM %s.%s WHERE DEFAULT_ROLE_HOST = '%s' and DEFAULT_ROLE_USER = '%s';`, allegrosql.SystemDB, allegrosql.DefaultRoleBlock, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}

		allegrosql = fmt.Sprintf(`DELETE FROM %s.%s WHERE HOST = '%s' and USER = '%s';`, allegrosql.SystemDB, allegrosql.DefaultRoleBlock, user.Hostname, user.Username)
		if _, err = sqlExecutor.Execute(context.Background(), allegrosql); err != nil {
			failedUsers = append(failedUsers, user.String())
			break
		}
		//TODO: need delete defCausumns_priv once we implement defCausumns_priv functionality.
	}

	if len(failedUsers) == 0 {
		if _, err := sqlExecutor.Execute(context.Background(), "commit"); err != nil {
			return err
		}
	} else {
		if _, err := sqlExecutor.Execute(context.Background(), "rollback"); err != nil {
			return err
		}
		if s.IsDropRole {
			return ErrCannotUser.GenWithStackByArgs("DROP ROLE", strings.Join(failedUsers, ","))
		}
		return ErrCannotUser.GenWithStackByArgs("DROP USER", strings.Join(failedUsers, ","))
	}
	petri.GetPetri(e.ctx).NotifyUFIDelatePrivilege(e.ctx)
	return nil
}

func userExists(ctx stochastikctx.Context, name string, host string) (bool, error) {
	allegrosql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s';`, allegrosql.SystemDB, allegrosql.UserBlock, name, host)
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func (e *SimpleExec) executeSetPwd(s *ast.SetPwdStmt) error {
	var u, h string
	if s.User == nil {
		if e.ctx.GetStochastikVars().User == nil {
			return errors.New("Stochastik error is empty")
		}
		u = e.ctx.GetStochastikVars().User.AuthUsername
		h = e.ctx.GetStochastikVars().User.AuthHostname
	} else {
		checker := privilege.GetPrivilegeManager(e.ctx)
		activeRoles := e.ctx.GetStochastikVars().ActiveRoles
		if checker != nil && !checker.RequestVerification(activeRoles, "", "", "", allegrosql.SuperPriv) {
			return ErrDBaccessDenied.GenWithStackByArgs(u, h, "allegrosql")
		}
		u = s.User.Username
		h = s.User.Hostname
	}
	exists, err := userExists(e.ctx, u, h)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Trace(ErrPasswordNoMatch)
	}

	// uFIDelate allegrosql.user
	allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET authentication_string='%s' WHERE User='%s' AND Host='%s';`, allegrosql.SystemDB, allegrosql.UserBlock, auth.EncodePassword(s.Password), u, h)
	_, _, err = e.ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	petri.GetPetri(e.ctx).NotifyUFIDelatePrivilege(e.ctx)
	return err
}

func (e *SimpleExec) executeKillStmt(s *ast.KillStmt) error {
	conf := config.GetGlobalConfig()
	if s.MilevaDBExtension || conf.CompatibleKillQuery {
		sm := e.ctx.GetStochastikManager()
		if sm == nil {
			return nil
		}
		sm.Kill(s.ConnectionID, s.Query)
	} else {
		err := errors.New("Invalid operation. Please use 'KILL MilevaDB [CONNECTION | QUERY] connectionID' instead")
		e.ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
	}
	return nil
}

func (e *SimpleExec) executeFlush(s *ast.FlushStmt) error {
	switch s.Tp {
	case ast.FlushBlocks:
		if s.ReadLock {
			return errors.New("FLUSH TABLES WITH READ LOCK is not supported.  Please use @@milevadb_snapshot")
		}
	case ast.FlushPrivileges:
		// If skip-grant-block is configured, do not flush privileges.
		// Because LoadPrivilegeLoop does not run and the privilege Handle is nil,
		// Call dom.PrivilegeHandle().UFIDelate would panic.
		if config.GetGlobalConfig().Security.SkipGrantBlock {
			return nil
		}

		dom := petri.GetPetri(e.ctx)
		sysStochastikPool := dom.SysStochastikPool()
		ctx, err := sysStochastikPool.Get()
		if err != nil {
			return err
		}
		defer sysStochastikPool.Put(ctx)
		err = dom.PrivilegeHandle().UFIDelate(ctx.(stochastikctx.Context))
		return err
	case ast.FlushMilevaDBPlugin:
		dom := petri.GetPetri(e.ctx)
		for _, pluginName := range s.Plugins {
			err := plugin.NotifyFlush(dom, pluginName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *SimpleExec) executeAlterInstance(s *ast.AlterInstanceStmt) error {
	if s.ReloadTLS {
		logutil.BgLogger().Info("execute reload tls", zap.Bool("NoRollbackOnError", s.NoRollbackOnError))
		sm := e.ctx.GetStochastikManager()
		tlsCfg, err := soliton.LoadTLSCertificates(
			variable.SysVars["ssl_ca"].Value,
			variable.SysVars["ssl_key"].Value,
			variable.SysVars["ssl_cert"].Value,
		)
		if err != nil {
			if !s.NoRollbackOnError || config.GetGlobalConfig().Security.RequireSecureTransport {
				return err
			}
			logutil.BgLogger().Warn("reload TLS fail but keep working without TLS due to 'no rollback on error'")
		}
		sm.UFIDelateTLSConfig(tlsCfg)
	}
	return nil
}

func (e *SimpleExec) executeDropStats(s *ast.DropStatsStmt) error {
	h := petri.GetPetri(e.ctx).StatsHandle()
	err := h.DeleteBlockStatsFromKV(s.Block.BlockInfo.ID)
	if err != nil {
		return err
	}
	return h.UFIDelate(schemareplicant.GetSchemaReplicant(e.ctx))
}

func (e *SimpleExec) autoNewTxn() bool {
	switch e.Statement.(type) {
	case *ast.CreateUserStmt, *ast.AlterUserStmt, *ast.DropUserStmt:
		return true
	}
	return false
}

func (e *SimpleExec) executeShutdown(s *ast.ShutdownStmt) error {
	sessVars := e.ctx.GetStochastikVars()
	logutil.BgLogger().Info("execute shutdown statement", zap.Uint64("conn", sessVars.ConnectionID))
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}

	// Call with async
	go asyncDelayShutdown(p, time.Second)

	return nil
}

// #14239 - https://github.com/whtcorpsinc/milevadb/issues/14239
// Need repair 'shutdown' command behavior.
// Response of MilevaDB is different to MyALLEGROSQL.
// This function need to run with async perceptron, otherwise it will block main coroutine
func asyncDelayShutdown(p *os.Process, delay time.Duration) {
	time.Sleep(delay)
	err := p.Kill()
	if err != nil {
		panic(err)
	}
}

func (e *SimpleExec) executeCreateStatistics(s *ast.CreateStatisticsStmt) (err error) {
	// Not support Cardinality and Dependency statistics type for now.
	if s.StatsType == ast.StatsTypeCardinality || s.StatsType == ast.StatsTypeDependency {
		return terror.ClassOptimizer.New(allegrosql.ErrInternal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInternal]).GenWithStack("Cardinality and Dependency statistics types are not supported")
	}
	if _, ok := e.is.SchemaByName(s.Block.Schema); !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(s.Block.Schema)
	}
	t, err := e.is.BlockByName(s.Block.Schema, s.Block.Name)
	if err != nil {
		return schemareplicant.ErrBlockNotExists.GenWithStackByArgs(s.Block.Schema, s.Block.Name)
	}
	tblInfo := t.Meta()
	defCausIDs := make([]int64, 0, 2)
	// Check whether defCausumns exist.
	for _, defCausName := range s.DeferredCausets {
		defCaus := block.FindDefCaus(t.VisibleDefCauss(), defCausName.Name.L)
		if defCaus == nil {
			return terror.ClassDBS.New(allegrosql.ErrKeyDeferredCausetDoesNotExits, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrKeyDeferredCausetDoesNotExits]).GenWithStack("defCausumn does not exist: %s", defCausName.Name.L)
		}
		if s.StatsType == ast.StatsTypeCorrelation && tblInfo.PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.Flag) {
			warn := errors.New("No need to create correlation statistics on the integer primary key defCausumn")
			e.ctx.GetStochastikVars().StmtCtx.AppendWarning(warn)
			return nil
		}
		defCausIDs = append(defCausIDs, defCaus.ID)
	}
	if len(defCausIDs) != 2 && (s.StatsType == ast.StatsTypeCorrelation || s.StatsType == ast.StatsTypeDependency) {
		return terror.ClassOptimizer.New(allegrosql.ErrInternal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInternal]).GenWithStack("Only support Correlation and Dependency statistics types on 2 defCausumns")
	}
	if len(defCausIDs) < 1 && s.StatsType == ast.StatsTypeCardinality {
		return terror.ClassOptimizer.New(allegrosql.ErrInternal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInternal]).GenWithStack("Only support Cardinality statistics type on at least 2 defCausumns")
	}
	// TODO: check whether covering index exists for cardinality / dependency types.

	// Call utilities of statistics.Handle to modify system blocks instead of doing DML directly,
	// because locking in Handle can guarantee the correctness of `version` in system blocks.
	return petri.GetPetri(e.ctx).StatsHandle().InsertExtendedStats(s.StatsName, s.Block.Schema.L, defCausIDs, int(s.StatsType), tblInfo.ID, s.IfNotExists)
}

func (e *SimpleExec) executeDropStatistics(s *ast.DropStatisticsStmt) error {
	EDB := e.ctx.GetStochastikVars().CurrentDB
	if EDB == "" {
		return core.ErrNoDB
	}
	// Call utilities of statistics.Handle to modify system blocks instead of doing DML directly,
	// because locking in Handle can guarantee the correctness of `version` in system blocks.
	return petri.GetPetri(e.ctx).StatsHandle().MarkExtendedStatsDeleted(s.StatsName, EDB, -1)
}

func (e *SimpleExec) executeAdminReloadStatistics(s *ast.AdminStmt) error {
	if s.Tp != ast.AdminReloadStatistics {
		return terror.ClassOptimizer.New(allegrosql.ErrInternal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInternal]).GenWithStack("This AdminStmt is not ADMIN RELOAD STATISTICS")
	}
	return petri.GetPetri(e.ctx).StatsHandle().ReloadExtendedStatistics()
}
