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

package interlock

import (
	"context"
	"fmt"
	"strings"

	"github.com/YosiSF/errors"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/ast"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/charset"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/mysql"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/terror"
	"github.com/YosiSF/MilevaDB/domain"
	"github.com/YosiSF/MilevaDB/expression"
	"github.com/YosiSF/MilevaDB/plugin"
	"github.com/YosiSF/MilevaDB/causetnetctx/variable"
	"github.com/YosiSF/MilevaDB/types"
	"github.com/YosiSF/MilevaDB/util/chunk"
	"github.com/YosiSF/MilevaDB/util/collate"
	"github.com/YosiSF/MilevaDB/util/gcutil"
	"github.com/YosiSF/MilevaDB/util/logutil"
	"github.com/YosiSF/MilevaDB/util/stmtsummary"
	"github.com/YosiSF/MilevaDB/util/stringutil"
	"go.uber.org/zap"
)

// SetInterlock executes set statement.
type SetInterlock struct {
	baseInterlock

	vars []*expression.VarAssignment
	done bool
}

// Next implements the Interlock Next interface.
func (e *SetInterlock) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	sessionVars := e.ctx.GetSessionVars()
	for _, v := range e.vars {
		// Variable is case insensitive, we use lower case.
		if v.Name == ast.SetNames || v.Name == ast.SetCharset {
			// This is set charset stmt.
			if v.IsDefault {
				err := e.setCharset(mysql.DefaultCharset, "", v.Name == ast.SetNames)
				if err != nil {
					return err
				}
				continue
			}
			dt, err := v.Expr.(*expression.Constant).Eval(chunk.Row{})
			if err != nil {
				return err
			}
			cs := dt.GetString()
			var co string
			if v.ExtendValue != nil {
				co = v.ExtendValue.Value.GetString()
			}
			err = e.setCharset(cs, co, v.Name == ast.SetNames)
			if err != nil {
				return err
			}
			continue
		}
		name := strings.ToLower(v.Name)
		if !v.IsSystem {
			// Set user variable.
			value, err := v.Expr.Eval(chunk.Row{})
			if err != nil {
				return err
			}

			if value.IsNull() {
				delete(sessionVars.Users, name)
			} else {
				svalue, err1 := value.ToString()
				if err1 != nil {
					return err1
				}

				sessionVars.SetUserVar(name, stringutil.Copy(svalue), value.Collation())
			}
			continue
		}

		syns := e.getSynonyms(name)
		// Set system variable
		for _, n := range syns {
			err := e.setSysVariable(n, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *SetInterlock) getSynonyms(varName string) []string {
	synonyms, ok := variable.SynonymsSysVariables[varName]
	if ok {
		return synonyms
	}

	synonyms = []string{varName}
	return synonyms
}

func (e *SetInterlock) setSysVariable(name string, v *expression.VarAssignment) error {
	sessionVars := e.ctx.GetSessionVars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if sysVar.Scope == variable.ScopeNone {
		return errors.Errorf("Variable '%s' is a read only variable", name)
	}
	var valStr string
	if v.IsGlobal {
		// Set global scope system variable.
		if sysVar.Scope&variable.ScopeGlobal == 0 {
			return errors.Errorf("Variable '%s' is a SESSION variable and can't be used with SET GLOBAL", name)
		}
		value, err := e.getVarValue(v, sysVar)
		if err != nil {
			return err
		}
		if value.IsNull() {
			value.SetString("", mysql.DefaultCollationName)
		}
		valStr, err = value.ToString()
		if err != nil {
			return err
		}
		err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(name, valStr)
		if err != nil {
			return err
		}
		err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
			auditPlugin := plugin.DeclareAuditManifest(p.Manifest)
			if auditPlugin.OnGlobalVariableEvent != nil {
				auditPlugin.OnGlobalVariableEvent(context.Background(), e.ctx.GetSessionVars(), name, valStr)
			}
			return nil
		})
		if err != nil {
			return err
		}
	} else {
		// Set session scope system variable.
		if sysVar.Scope&variable.ScopeSession == 0 {
			return errors.Errorf("Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL", name)
		}
		value, err := e.getVarValue(v, nil)
		if err != nil {
			return err
		}
		oldSnapshotTS := sessionVars.SnapshotTS
		if name == variable.TxnIsolationOneShot && sessionVars.InTxn() {
			return errors.Trace(ErrCantChangeTxCharacteristics)
		}
		if name == variable.MilevaDBFoundInPlanCache {
			sessionVars.StmtCtx.AppendWarning(fmt.Errorf("Set operation for '%s' will not take effect", variable.MilevaDBFoundInPlanCache))
			return nil
		}
		err = variable.SetSessionSystemVar(sessionVars, name, value)
		if err != nil {
			return err
		}
		newSnapshotIsSet := sessionVars.SnapshotTS > 0 && sessionVars.SnapshotTS != oldSnapshotTS
		if newSnapshotIsSet {
			err = gcutil.ValidateSnapshot(e.ctx, sessionVars.SnapshotTS)
			if err != nil {
				sessionVars.SnapshotTS = oldSnapshotTS
				return err
			}
		}
		err = e.loadSnapshotschemareplicantIfNeeded(name)
		if err != nil {
			sessionVars.SnapshotTS = oldSnapshotTS
			return err
		}
		if value.IsNull() {
			valStr = "NULL"
		} else {
			var err error
			valStr, err = value.ToString()
			terror.Log(err)
		}
		if name != variable.AutoCommit {
			logutil.BgLogger().Info("set session var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
		} else {
			// Some applications will set `autocommit` variable before query.
			// This will print too many unnecessary log info.
			logutil.BgLogger().Debug("set session var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
		}
	}

	switch name {
	case variable.MilevaDBEnableStmtSummary:
		return stmtsummary.StmtSummaryByDigestMap.SetEnabled(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryInternalQuery:
		return stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryRefreshInterval:
		return stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryHistorySize:
		return stmtsummary.StmtSummaryByDigestMap.SetHistorySize(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryMaxStmtCount:
		return stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryMaxSQLLength:
		return stmtsummary.StmtSummaryByDigestMap.SetMaxSQLLength(valStr, !v.IsGlobal)
	case variable.MilevaDBCapturePlanBaseline:
		variable.CapturePlanBaseline.Set(strings.ToLower(valStr), !v.IsGlobal)
	}

	return nil
}

func (e *SetInterlock) setCharset(cs, co string, isSetName bool) error {
	var err error
	if len(co) == 0 {
		if co, err = charset.GetDefaultCollation(cs); err != nil {
			return err
		}
	} else {
		var coll *charset.Collation
		if coll, err = collate.GetCollationByName(co); err != nil {
			return err
		}
		if coll.CharsetName != cs {
			return charset.ErrCollationCharsetMismatch.GenWithStackByArgs(coll.Name, cs)
		}
	}
	sessionVars := e.ctx.GetSessionVars()
	if isSetName {
		for _, v := range variable.SetNamesVariables {
			if err = sessionVars.SetSystemVar(v, cs); err != nil {
				return errors.Trace(err)
			}
		}
		return errors.Trace(sessionVars.SetSystemVar(variable.CollationConnection, co))
	}
	// Set charset statement, see also https://dev.mysql.com/doc/refman/8.0/en/set-character-set.html.
	for _, v := range variable.SetCharsetVariables {
		if err = sessionVars.SetSystemVar(v, cs); err != nil {
			return errors.Trace(err)
		}
	}
	csDb, err := sessionVars.GlobalVarsAccessor.GetGlobalSysVar(variable.CharsetDatabase)
	if err != nil {
		return err
	}
	coDb, err := sessionVars.GlobalVarsAccessor.GetGlobalSysVar(variable.CollationDatabase)
	if err != nil {
		return err
	}
	err = sessionVars.SetSystemVar(variable.CharacterSetConnection, csDb)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(sessionVars.SetSystemVar(variable.CollationConnection, coDb))
}

func (e *SetInterlock) getVarValue(v *expression.VarAssignment, sysVar *variable.SysVar) (value types.Datum, err error) {
	if v.IsDefault {
		// To set a SESSION variable to the GLOBAL value or a GLOBAL value
		// to the compiled-in MySQL default value, use the DEFAULT keyword.
		// See http://dev.mysql.com/doc/refman/5.7/en/set-statement.html
		if sysVar != nil {
			value = types.NewStringDatum(sysVar.Value)
		} else {
			s, err1 := variable.GetGlobalSystemVar(e.ctx.GetSessionVars(), v.Name)
			if err1 != nil {
				return value, err1
			}
			value = types.NewStringDatum(s)
		}
		return
	}
	value, err = v.Expr.Eval(chunk.Row{})
	return value, err
}

func (e *SetInterlock) loadSnapshotschemareplicantIfNeeded(name string) error {
	if name != variable.MilevaDBSnapshot {
		return nil
	}
	vars := e.ctx.GetSessionVars()
	if vars.SnapshotTS == 0 {
		vars.Snapshotschemareplicant = nil
		return nil
	}
	logutil.BgLogger().Info("load snapshot info schema", zap.Uint64("conn", vars.ConnectionID), zap.Uint64("SnapshotTS", vars.SnapshotTS))
	dom := domain.GetDomain(e.ctx)
	snapInfo, err := dom.GetSnapshotschemareplicant(vars.SnapshotTS)
	if err != nil {
		return err
	}
	vars.Snapshotschemareplicant = snapInfo
	return nil
}
