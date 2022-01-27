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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/privilege/privileges"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"go.uber.org/zap"
)

/***
 * Grant Statement
 * See https://dev.allegrosql.com/doc/refman/5.7/en/grant.html
 ************************************************************************************/
var (
	_ Executor = (*GrantExec)(nil)
)

// GrantExec executes GrantStmt.
type GrantExec struct {
	baseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec
	TLSOptions []*ast.TLSOption

	is        schemareplicant.SchemaReplicant
	WithGrant bool
	done      bool
}

// Next implements the Executor Next interface.
func (e *GrantExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true

	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetStochastikVars().CurrentDB
	}

	// Make sure the block exist.
	if e.Level.Level == ast.GrantLevelBlock {
		dbNameStr := perceptron.NewCIStr(dbName)
		schemaReplicant := schemareplicant.GetSchemaReplicant(e.ctx)
		tbl, err := schemaReplicant.BlockByName(dbNameStr, perceptron.NewCIStr(e.Level.BlockName))
		if err != nil {
			return err
		}
		err = schemareplicant.ErrBlockNotExists.GenWithStackByArgs(dbName, e.Level.BlockName)
		// Note the block name compare is case sensitive here.
		if tbl.Meta().Name.String() != e.Level.BlockName {
			return err
		}
		if len(e.Level.DBName) > 0 {
			// The database name should also match.
			EDB, succ := schemaReplicant.SchemaByName(dbNameStr)
			if !succ || EDB.Name.String() != dbName {
				return err
			}
		}
	}

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

	// Check which user is not exist.
	for _, user := range e.Users {
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
		if !exists && e.ctx.GetStochastikVars().ALLEGROSQLMode.HasNoAutoCreateUserMode() {
			return ErrCantCreateUserWithGrant
		} else if !exists {
			pwd, ok := user.EncodedPassword()
			if !ok {
				return errors.Trace(ErrPasswordFormat)
			}
			user := fmt.Sprintf(`('%s', '%s', '%s')`, user.User.Hostname, user.User.Username, pwd)
			allegrosql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, authentication_string) VALUES %s;`, allegrosql.SystemDB, allegrosql.UserBlock, user)
			_, err := internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(ctx, allegrosql)
			if err != nil {
				return err
			}
		}
	}

	// Grant for each user
	for _, user := range e.Users {
		// If there is no privilege entry in corresponding block, insert a new one.
		// Global sINTERLOCKe:		allegrosql.global_priv
		// EDB sINTERLOCKe:			allegrosql.EDB
		// Block sINTERLOCKe:			allegrosql.Blocks_priv
		// DeferredCauset sINTERLOCKe:		allegrosql.DeferredCausets_priv
		if e.TLSOptions != nil {
			err = checkAndInitGlobalPriv(internalStochastik, user.User.Username, user.User.Hostname)
			if err != nil {
				return err
			}
		}
		switch e.Level.Level {
		case ast.GrantLevelDB:
			err := checkAndInitDBPriv(internalStochastik, dbName, e.is, user.User.Username, user.User.Hostname)
			if err != nil {
				return err
			}
		case ast.GrantLevelBlock:
			err := checkAndInitBlockPriv(internalStochastik, dbName, e.Level.BlockName, e.is, user.User.Username, user.User.Hostname)
			if err != nil {
				return err
			}
		}
		privs := e.Privs
		if e.WithGrant {
			privs = append(privs, &ast.PrivElem{Priv: allegrosql.GrantPriv})
		}

		// Grant global priv to user.
		err = e.grantGlobalPriv(internalStochastik, user)
		if err != nil {
			return err
		}
		// Grant each priv to the user.
		for _, priv := range privs {
			if len(priv.DefCauss) > 0 {
				// Check defCausumn sINTERLOCKe privilege entry.
				// TODO: Check validity before insert new entry.
				err := e.checkAndInitDeferredCausetPriv(user.User.Username, user.User.Hostname, priv.DefCauss, internalStochastik)
				if err != nil {
					return err
				}
			}
			err := e.grantLevelPriv(priv, user, internalStochastik)
			if err != nil {
				return err
			}
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

// checkAndInitGlobalPriv checks if global sINTERLOCKe privilege entry exists in allegrosql.global_priv.
// If not exists, insert a new one.
func checkAndInitGlobalPriv(ctx stochastikctx.Context, user string, host string) error {
	ok, err := globalPrivEntryExists(ctx, user, host)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	// Entry does not exist for user-host-EDB. Insert a new entry.
	return initGlobalPrivEntry(ctx, user, host)
}

// checkAndInitDBPriv checks if EDB sINTERLOCKe privilege entry exists in allegrosql.EDB.
// If unexists, insert a new one.
func checkAndInitDBPriv(ctx stochastikctx.Context, dbName string, is schemareplicant.SchemaReplicant, user string, host string) error {
	ok, err := dbUserExists(ctx, user, host, dbName)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	// Entry does not exist for user-host-EDB. Insert a new entry.
	return initDBPrivEntry(ctx, user, host, dbName)
}

// checkAndInitBlockPriv checks if block sINTERLOCKe privilege entry exists in allegrosql.Blocks_priv.
// If unexists, insert a new one.
func checkAndInitBlockPriv(ctx stochastikctx.Context, dbName, tblName string, is schemareplicant.SchemaReplicant, user string, host string) error {
	ok, err := blockUserExists(ctx, user, host, dbName, tblName)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	// Entry does not exist for user-host-EDB-tbl. Insert a new entry.
	return initBlockPrivEntry(ctx, user, host, dbName, tblName)
}

// checkAndInitDeferredCausetPriv checks if defCausumn sINTERLOCKe privilege entry exists in allegrosql.DeferredCausets_priv.
// If unexists, insert a new one.
func (e *GrantExec) checkAndInitDeferredCausetPriv(user string, host string, defcaus []*ast.DeferredCausetName, internalStochastik stochastikctx.Context) error {
	dbName, tbl, err := getTargetSchemaAndBlock(e.ctx, e.Level.DBName, e.Level.BlockName, e.is)
	if err != nil {
		return err
	}
	for _, c := range defcaus {
		defCaus := block.FindDefCaus(tbl.DefCauss(), c.Name.L)
		if defCaus == nil {
			return errors.Errorf("Unknown defCausumn: %s", c.Name.O)
		}
		ok, err := defCausumnPrivEntryExists(internalStochastik, user, host, dbName, tbl.Meta().Name.O, defCaus.Name.O)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		// Entry does not exist for user-host-EDB-tbl-defCaus. Insert a new entry.
		err = initDeferredCausetPrivEntry(internalStochastik, user, host, dbName, tbl.Meta().Name.O, defCaus.Name.O)
		if err != nil {
			return err
		}
	}
	return nil
}

// initGlobalPrivEntry inserts a new event into allegrosql.EDB with empty privilege.
func initGlobalPrivEntry(ctx stochastikctx.Context, user string, host string) error {
	allegrosql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, PRIV) VALUES ('%s', '%s', '%s')`, allegrosql.SystemDB, allegrosql.GlobalPrivBlock, host, user, "{}")
	_, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

// initDBPrivEntry inserts a new event into allegrosql.EDB with empty privilege.
func initDBPrivEntry(ctx stochastikctx.Context, user string, host string, EDB string) error {
	allegrosql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, EDB) VALUES ('%s', '%s', '%s')`, allegrosql.SystemDB, allegrosql.DBBlock, host, user, EDB)
	_, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

// initBlockPrivEntry inserts a new event into allegrosql.Blocks_priv with empty privilege.
func initBlockPrivEntry(ctx stochastikctx.Context, user string, host string, EDB string, tbl string) error {
	allegrosql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, EDB, Block_name, Block_priv, DeferredCauset_priv) VALUES ('%s', '%s', '%s', '%s', '', '')`, allegrosql.SystemDB, allegrosql.BlockPrivBlock, host, user, EDB, tbl)
	_, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

// initDeferredCausetPrivEntry inserts a new event into allegrosql.DeferredCausets_priv with empty privilege.
func initDeferredCausetPrivEntry(ctx stochastikctx.Context, user string, host string, EDB string, tbl string, defCaus string) error {
	allegrosql := fmt.Sprintf(`INSERT INTO %s.%s (Host, User, EDB, Block_name, DeferredCauset_name, DeferredCauset_priv) VALUES ('%s', '%s', '%s', '%s', '%s', '')`, allegrosql.SystemDB, allegrosql.DeferredCausetPrivBlock, host, user, EDB, tbl, defCaus)
	_, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

// grantGlobalPriv grants priv to user in global sINTERLOCKe.
func (e *GrantExec) grantGlobalPriv(ctx stochastikctx.Context, user *ast.UserSpec) error {
	if len(e.TLSOptions) == 0 {
		return nil
	}
	priv, err := tlsOption2GlobalPriv(e.TLSOptions)
	if err != nil {
		return errors.Trace(err)
	}
	allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET PRIV = '%s' WHERE User='%s' AND Host='%s'`, allegrosql.SystemDB, allegrosql.GlobalPrivBlock, priv, user.User.Username, user.User.Hostname)
	_, err = ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

func tlsOption2GlobalPriv(tlsOptions []*ast.TLSOption) (priv []byte, err error) {
	if len(tlsOptions) == 0 {
		priv = []byte("{}")
		return
	}
	dupSet := make(map[int]struct{})
	for _, opt := range tlsOptions {
		if _, dup := dupSet[opt.Type]; dup {
			var typeName string
			switch opt.Type {
			case ast.Cipher:
				typeName = "CIPHER"
			case ast.Issuer:
				typeName = "ISSUER"
			case ast.Subject:
				typeName = "SUBJECT"
			case ast.SAN:
				typeName = "SAN"
			}
			err = errors.Errorf("Duplicate require %s clause", typeName)
			return
		}
		dupSet[opt.Type] = struct{}{}
	}
	gp := privileges.GlobalPrivValue{SSLType: privileges.SslTypeNotSpecified}
	for _, tlsOpt := range tlsOptions {
		switch tlsOpt.Type {
		case ast.TslNone:
			gp.SSLType = privileges.SslTypeNone
		case ast.Ssl:
			gp.SSLType = privileges.SslTypeAny
		case ast.X509:
			gp.SSLType = privileges.SslTypeX509
		case ast.Cipher:
			gp.SSLType = privileges.SslTypeSpecified
			if len(tlsOpt.Value) > 0 {
				if _, ok := soliton.SupportCipher[tlsOpt.Value]; !ok {
					err = errors.Errorf("Unsupported cipher suit: %s", tlsOpt.Value)
					return
				}
				gp.SSLCipher = tlsOpt.Value
			}
		case ast.Issuer:
			err = soliton.CheckSupportX509NameOneline(tlsOpt.Value)
			if err != nil {
				return
			}
			gp.SSLType = privileges.SslTypeSpecified
			gp.X509Issuer = tlsOpt.Value
		case ast.Subject:
			err = soliton.CheckSupportX509NameOneline(tlsOpt.Value)
			if err != nil {
				return
			}
			gp.SSLType = privileges.SslTypeSpecified
			gp.X509Subject = tlsOpt.Value
		case ast.SAN:
			gp.SSLType = privileges.SslTypeSpecified
			_, err = soliton.ParseAndCheckSAN(tlsOpt.Value)
			if err != nil {
				return
			}
			gp.SAN = tlsOpt.Value
		default:
			err = errors.Errorf("Unknown ssl type: %#v", tlsOpt.Type)
			return
		}
	}
	if gp.SSLType == privileges.SslTypeNotSpecified && len(gp.SSLCipher) == 0 &&
		len(gp.X509Issuer) == 0 && len(gp.X509Subject) == 0 && len(gp.SAN) == 0 {
		return
	}
	priv, err = json.Marshal(&gp)
	if err != nil {
		return
	}
	return
}

// grantLevelPriv grants priv to user in s.Level sINTERLOCKe.
func (e *GrantExec) grantLevelPriv(priv *ast.PrivElem, user *ast.UserSpec, internalStochastik stochastikctx.Context) error {
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		return e.grantGlobalLevel(priv, user, internalStochastik)
	case ast.GrantLevelDB:
		return e.grantDBLevel(priv, user, internalStochastik)
	case ast.GrantLevelBlock:
		if len(priv.DefCauss) == 0 {
			return e.grantBlockLevel(priv, user, internalStochastik)
		}
		return e.grantDeferredCausetLevel(priv, user, internalStochastik)
	default:
		return errors.Errorf("Unknown grant level: %#v", e.Level)
	}
}

// grantGlobalLevel manipulates allegrosql.user block.
func (e *GrantExec) grantGlobalLevel(priv *ast.PrivElem, user *ast.UserSpec, internalStochastik stochastikctx.Context) error {
	if priv.Priv == 0 {
		return nil
	}
	asgns, err := composeGlobalPrivUFIDelate(priv.Priv, "Y")
	if err != nil {
		return err
	}
	allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET %s WHERE User='%s' AND Host='%s'`, allegrosql.SystemDB, allegrosql.UserBlock, asgns, user.User.Username, user.User.Hostname)
	_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

// grantDBLevel manipulates allegrosql.EDB block.
func (e *GrantExec) grantDBLevel(priv *ast.PrivElem, user *ast.UserSpec, internalStochastik stochastikctx.Context) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetStochastikVars().CurrentDB
	}
	asgns, err := composeDBPrivUFIDelate(priv.Priv, "Y")
	if err != nil {
		return err
	}
	allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND EDB='%s';`, allegrosql.SystemDB, allegrosql.DBBlock, asgns, user.User.Username, user.User.Hostname, dbName)
	_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

// grantBlockLevel manipulates allegrosql.blocks_priv block.
func (e *GrantExec) grantBlockLevel(priv *ast.PrivElem, user *ast.UserSpec, internalStochastik stochastikctx.Context) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetStochastikVars().CurrentDB
	}
	tblName := e.Level.BlockName
	asgns, err := composeBlockPrivUFIDelateForGrant(internalStochastik, priv.Priv, user.User.Username, user.User.Hostname, dbName, tblName)
	if err != nil {
		return err
	}
	allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND EDB='%s' AND Block_name='%s';`, allegrosql.SystemDB, allegrosql.BlockPrivBlock, asgns, user.User.Username, user.User.Hostname, dbName, tblName)
	_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	return err
}

// grantDeferredCausetLevel manipulates allegrosql.blocks_priv block.
func (e *GrantExec) grantDeferredCausetLevel(priv *ast.PrivElem, user *ast.UserSpec, internalStochastik stochastikctx.Context) error {
	dbName, tbl, err := getTargetSchemaAndBlock(e.ctx, e.Level.DBName, e.Level.BlockName, e.is)
	if err != nil {
		return err
	}

	for _, c := range priv.DefCauss {
		defCaus := block.FindDefCaus(tbl.DefCauss(), c.Name.L)
		if defCaus == nil {
			return errors.Errorf("Unknown defCausumn: %s", c)
		}
		asgns, err := composeDeferredCausetPrivUFIDelateForGrant(internalStochastik, priv.Priv, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, defCaus.Name.O)
		if err != nil {
			return err
		}
		allegrosql := fmt.Sprintf(`UFIDelATE %s.%s SET %s WHERE User='%s' AND Host='%s' AND EDB='%s' AND Block_name='%s' AND DeferredCauset_name='%s';`, allegrosql.SystemDB, allegrosql.DeferredCausetPrivBlock, asgns, user.User.Username, user.User.Hostname, dbName, tbl.Meta().Name.O, defCaus.Name.O)
		_, err = internalStochastik.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
		if err != nil {
			return err
		}
	}
	return nil
}

// composeGlobalPrivUFIDelate composes uFIDelate stmt assignment list string for global sINTERLOCKe privilege uFIDelate.
func composeGlobalPrivUFIDelate(priv allegrosql.PrivilegeType, value string) (string, error) {
	if priv == allegrosql.AllPriv {
		strs := make([]string, 0, len(allegrosql.Priv2UserDefCaus))
		for _, v := range allegrosql.AllGlobalPrivs {
			strs = append(strs, fmt.Sprintf(`%s='%s'`, allegrosql.Priv2UserDefCaus[v], value))
		}
		return strings.Join(strs, ", "), nil
	}
	defCaus, ok := allegrosql.Priv2UserDefCaus[priv]
	if !ok {
		return "", errors.Errorf("Unknown priv: %v", priv)
	}
	return fmt.Sprintf(`%s='%s'`, defCaus, value), nil
}

// composeDBPrivUFIDelate composes uFIDelate stmt assignment list for EDB sINTERLOCKe privilege uFIDelate.
func composeDBPrivUFIDelate(priv allegrosql.PrivilegeType, value string) (string, error) {
	if priv == allegrosql.AllPriv {
		strs := make([]string, 0, len(allegrosql.AllDBPrivs))
		for _, p := range allegrosql.AllDBPrivs {
			v, ok := allegrosql.Priv2UserDefCaus[p]
			if !ok {
				return "", errors.Errorf("Unknown EDB privilege %v", priv)
			}
			strs = append(strs, fmt.Sprintf(`%s='%s'`, v, value))
		}
		return strings.Join(strs, ", "), nil
	}
	defCaus, ok := allegrosql.Priv2UserDefCaus[priv]
	if !ok {
		return "", errors.Errorf("Unknown priv: %v", priv)
	}
	return fmt.Sprintf(`%s='%s'`, defCaus, value), nil
}

// composeBlockPrivUFIDelateForGrant composes uFIDelate stmt assignment list for block sINTERLOCKe privilege uFIDelate.
func composeBlockPrivUFIDelateForGrant(ctx stochastikctx.Context, priv allegrosql.PrivilegeType, name string, host string, EDB string, tbl string) (string, error) {
	var newBlockPriv, newDeferredCausetPriv string
	if priv == allegrosql.AllPriv {
		for _, p := range allegrosql.AllBlockPrivs {
			v, ok := allegrosql.Priv2SetStr[p]
			if !ok {
				return "", errors.Errorf("Unknown block privilege %v", p)
			}
			newBlockPriv = addToSet(newBlockPriv, v)
		}
		for _, p := range allegrosql.AllDeferredCausetPrivs {
			v, ok := allegrosql.Priv2SetStr[p]
			if !ok {
				return "", errors.Errorf("Unknown defCausumn privilege %v", p)
			}
			newDeferredCausetPriv = addToSet(newDeferredCausetPriv, v)
		}
	} else {
		currBlockPriv, currDeferredCausetPriv, err := getBlockPriv(ctx, name, host, EDB, tbl)
		if err != nil {
			return "", err
		}
		p, ok := allegrosql.Priv2SetStr[priv]
		if !ok {
			return "", errors.Errorf("Unknown priv: %v", priv)
		}
		newBlockPriv = addToSet(currBlockPriv, p)

		for _, cp := range allegrosql.AllDeferredCausetPrivs {
			if priv == cp {
				newDeferredCausetPriv = addToSet(currDeferredCausetPriv, p)
				break
			}
		}
	}
	return fmt.Sprintf(`Block_priv='%s', DeferredCauset_priv='%s', Grantor='%s'`, newBlockPriv, newDeferredCausetPriv, ctx.GetStochastikVars().User), nil
}

func composeBlockPrivUFIDelateForRevoke(ctx stochastikctx.Context, priv allegrosql.PrivilegeType, name string, host string, EDB string, tbl string) (string, error) {
	var newBlockPriv, newDeferredCausetPriv string
	if priv == allegrosql.AllPriv {
		newBlockPriv = ""
		newDeferredCausetPriv = ""
	} else {
		currBlockPriv, currDeferredCausetPriv, err := getBlockPriv(ctx, name, host, EDB, tbl)
		if err != nil {
			return "", err
		}
		p, ok := allegrosql.Priv2SetStr[priv]
		if !ok {
			return "", errors.Errorf("Unknown priv: %v", priv)
		}
		newBlockPriv = deleteFromSet(currBlockPriv, p)

		for _, cp := range allegrosql.AllDeferredCausetPrivs {
			if priv == cp {
				newDeferredCausetPriv = deleteFromSet(currDeferredCausetPriv, p)
				break
			}
		}
	}
	return fmt.Sprintf(`Block_priv='%s', DeferredCauset_priv='%s', Grantor='%s'`, newBlockPriv, newDeferredCausetPriv, ctx.GetStochastikVars().User), nil
}

// addToSet add a value to the set, e.g:
// addToSet("Select,Insert", "UFIDelate") returns "Select,Insert,UFIDelate".
func addToSet(set string, value string) string {
	if set == "" {
		return value
	}
	return fmt.Sprintf("%s,%s", set, value)
}

// deleteFromSet delete the value from the set, e.g:
// deleteFromSet("Select,Insert,UFIDelate", "UFIDelate") returns "Select,Insert".
func deleteFromSet(set string, value string) string {
	sets := strings.Split(set, ",")
	res := make([]string, 0, len(sets))
	for _, v := range sets {
		if v != value {
			res = append(res, v)
		}
	}
	return strings.Join(res, ",")
}

// composeDeferredCausetPrivUFIDelateForGrant composes uFIDelate stmt assignment list for defCausumn sINTERLOCKe privilege uFIDelate.
func composeDeferredCausetPrivUFIDelateForGrant(ctx stochastikctx.Context, priv allegrosql.PrivilegeType, name string, host string, EDB string, tbl string, defCaus string) (string, error) {
	newDeferredCausetPriv := ""
	if priv == allegrosql.AllPriv {
		for _, p := range allegrosql.AllDeferredCausetPrivs {
			v, ok := allegrosql.Priv2SetStr[p]
			if !ok {
				return "", errors.Errorf("Unknown defCausumn privilege %v", p)
			}
			newDeferredCausetPriv = addToSet(newDeferredCausetPriv, v)
		}
	} else {
		currDeferredCausetPriv, err := getDeferredCausetPriv(ctx, name, host, EDB, tbl, defCaus)
		if err != nil {
			return "", err
		}
		p, ok := allegrosql.Priv2SetStr[priv]
		if !ok {
			return "", errors.Errorf("Unknown priv: %v", priv)
		}
		newDeferredCausetPriv = addToSet(currDeferredCausetPriv, p)
	}
	return fmt.Sprintf(`DeferredCauset_priv='%s'`, newDeferredCausetPriv), nil
}

func composeDeferredCausetPrivUFIDelateForRevoke(ctx stochastikctx.Context, priv allegrosql.PrivilegeType, name string, host string, EDB string, tbl string, defCaus string) (string, error) {
	newDeferredCausetPriv := ""
	if priv == allegrosql.AllPriv {
		newDeferredCausetPriv = ""
	} else {
		currDeferredCausetPriv, err := getDeferredCausetPriv(ctx, name, host, EDB, tbl, defCaus)
		if err != nil {
			return "", err
		}
		p, ok := allegrosql.Priv2SetStr[priv]
		if !ok {
			return "", errors.Errorf("Unknown priv: %v", priv)
		}
		newDeferredCausetPriv = deleteFromSet(currDeferredCausetPriv, p)
	}
	return fmt.Sprintf(`DeferredCauset_priv='%s'`, newDeferredCausetPriv), nil
}

// recordExists is a helper function to check if the allegrosql returns any event.
func recordExists(ctx stochastikctx.Context, allegrosql string) (bool, error) {
	recordSets, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	if err != nil {
		return false, err
	}
	rows, _, err := getEventsAndFields(ctx, recordSets)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// globalPrivEntryExists checks if there is an entry with key user-host in allegrosql.global_priv.
func globalPrivEntryExists(ctx stochastikctx.Context, name string, host string) (bool, error) {
	allegrosql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s';`, allegrosql.SystemDB, allegrosql.GlobalPrivBlock, name, host)
	return recordExists(ctx, allegrosql)
}

// dbUserExists checks if there is an entry with key user-host-EDB in allegrosql.EDB.
func dbUserExists(ctx stochastikctx.Context, name string, host string, EDB string) (bool, error) {
	allegrosql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s' AND EDB='%s';`, allegrosql.SystemDB, allegrosql.DBBlock, name, host, EDB)
	return recordExists(ctx, allegrosql)
}

// blockUserExists checks if there is an entry with key user-host-EDB-tbl in allegrosql.Blocks_priv.
func blockUserExists(ctx stochastikctx.Context, name string, host string, EDB string, tbl string) (bool, error) {
	allegrosql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s' AND EDB='%s' AND Block_name='%s';`, allegrosql.SystemDB, allegrosql.BlockPrivBlock, name, host, EDB, tbl)
	return recordExists(ctx, allegrosql)
}

// defCausumnPrivEntryExists checks if there is an entry with key user-host-EDB-tbl-defCaus in allegrosql.DeferredCausets_priv.
func defCausumnPrivEntryExists(ctx stochastikctx.Context, name string, host string, EDB string, tbl string, defCaus string) (bool, error) {
	allegrosql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s' AND EDB='%s' AND Block_name='%s' AND DeferredCauset_name='%s';`, allegrosql.SystemDB, allegrosql.DeferredCausetPrivBlock, name, host, EDB, tbl, defCaus)
	return recordExists(ctx, allegrosql)
}

// getBlockPriv gets current block sINTERLOCKe privilege set from allegrosql.Blocks_priv.
// Return Block_priv and DeferredCauset_priv.
func getBlockPriv(ctx stochastikctx.Context, name string, host string, EDB string, tbl string) (string, string, error) {
	allegrosql := fmt.Sprintf(`SELECT Block_priv, DeferredCauset_priv FROM %s.%s WHERE User='%s' AND Host='%s' AND EDB='%s' AND Block_name='%s';`, allegrosql.SystemDB, allegrosql.BlockPrivBlock, name, host, EDB, tbl)
	rs, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	if err != nil {
		return "", "", err
	}
	if len(rs) < 1 {
		return "", "", errors.Errorf("get block privilege fail for %s %s %s %s", name, host, EDB, tbl)
	}
	var tPriv, cPriv string
	rows, fields, err := getEventsAndFields(ctx, rs)
	if err != nil {
		return "", "", err
	}
	if len(rows) < 1 {
		return "", "", errors.Errorf("get block privilege fail for %s %s %s %s", name, host, EDB, tbl)
	}
	event := rows[0]
	if fields[0].DeferredCauset.Tp == allegrosql.TypeSet {
		blockPriv := event.GetSet(0)
		tPriv = blockPriv.Name
	}
	if fields[1].DeferredCauset.Tp == allegrosql.TypeSet {
		defCausumnPriv := event.GetSet(1)
		cPriv = defCausumnPriv.Name
	}
	return tPriv, cPriv, nil
}

// getDeferredCausetPriv gets current defCausumn sINTERLOCKe privilege set from allegrosql.DeferredCausets_priv.
// Return DeferredCauset_priv.
func getDeferredCausetPriv(ctx stochastikctx.Context, name string, host string, EDB string, tbl string, defCaus string) (string, error) {
	allegrosql := fmt.Sprintf(`SELECT DeferredCauset_priv FROM %s.%s WHERE User='%s' AND Host='%s' AND EDB='%s' AND Block_name='%s' AND DeferredCauset_name='%s';`, allegrosql.SystemDB, allegrosql.DeferredCausetPrivBlock, name, host, EDB, tbl, defCaus)
	rs, err := ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.Background(), allegrosql)
	if err != nil {
		return "", err
	}
	if len(rs) < 1 {
		return "", errors.Errorf("get defCausumn privilege fail for %s %s %s %s", name, host, EDB, tbl)
	}
	rows, fields, err := getEventsAndFields(ctx, rs)
	if err != nil {
		return "", err
	}
	if len(rows) < 1 {
		return "", errors.Errorf("get defCausumn privilege fail for %s %s %s %s %s", name, host, EDB, tbl, defCaus)
	}
	cPriv := ""
	if fields[0].DeferredCauset.Tp == allegrosql.TypeSet {
		setVal := rows[0].GetSet(0)
		cPriv = setVal.Name
	}
	return cPriv, nil
}

// getTargetSchemaAndBlock finds the schemaReplicant and block by dbName and blockName.
func getTargetSchemaAndBlock(ctx stochastikctx.Context, dbName, blockName string, is schemareplicant.SchemaReplicant) (string, block.Block, error) {
	if len(dbName) == 0 {
		dbName = ctx.GetStochastikVars().CurrentDB
		if len(dbName) == 0 {
			return "", nil, errors.New("miss EDB name for grant privilege")
		}
	}
	name := perceptron.NewCIStr(blockName)
	tbl, err := is.BlockByName(perceptron.NewCIStr(dbName), name)
	if err != nil {
		return "", nil, err
	}
	return dbName, tbl, nil
}

// getEventsAndFields is used to extract rows from record sets.
func getEventsAndFields(ctx stochastikctx.Context, recordSets []sqlexec.RecordSet) ([]chunk.Event, []*ast.ResultField, error) {
	var (
		rows   []chunk.Event
		fields []*ast.ResultField
	)

	for i, rs := range recordSets {
		tmp, err := getEventFromRecordSet(context.Background(), ctx, rs)
		if err != nil {
			return nil, nil, err
		}
		if err = rs.Close(); err != nil {
			return nil, nil, err
		}

		if i == 0 {
			rows = tmp
			fields = rs.Fields()
		}
	}
	return rows, fields, nil
}

func getEventFromRecordSet(ctx context.Context, se stochastikctx.Context, rs sqlexec.RecordSet) ([]chunk.Event, error) {
	var rows []chunk.Event
	req := rs.NewChunk()
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumEvents() == 0 {
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, se.GetStochastikVars().MaxChunkSize)
	}
}
