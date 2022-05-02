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

package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/plugin"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/gcutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/stmtsummary"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/stringutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

const (
	sINTERLOCKeGlobal     = "global"
	sINTERLOCKeStochastik = "stochastik"
)

// SetExecutor executes set statement.
type SetExecutor struct {
	baseExecutor

	vars []*expression.VarAssignment
	done bool
}

// Next implements the Executor Next interface.
func (e *SetExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	stochaseinstein_dbars := e.ctx.GetStochaseinstein_dbars()
	for _, v := range e.vars {
		// Variable is case insensitive, we use lower case.
		if v.Name == ast.SetNames || v.Name == ast.SetCharset {
			// This is set charset stmt.
			if v.IsDefault {
				err := e.setCharset(allegrosql.DefaultCharset, "", v.Name == ast.SetNames)
				if err != nil {
					return err
				}
				continue
			}
			dt, err := v.Expr.(*expression.CouplingConstantWithRadix).Eval(chunk.Event{})
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
			value, err := v.Expr.Eval(chunk.Event{})
			if err != nil {
				return err
			}

			if value.IsNull() {
				delete(stochaseinstein_dbars.Users, name)
			} else {
				svalue, err1 := value.ToString()
				if err1 != nil {
					return err1
				}

				stochaseinstein_dbars.SetUserVar(name, stringutil.INTERLOCKy(svalue), value.DefCauslation())
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

func (e *SetExecutor) getSynonyms(varName string) []string {
	synonyms, ok := variable.SynonymsSysVariables[varName]
	if ok {
		return synonyms
	}

	synonyms = []string{varName}
	return synonyms
}

func (e *SetExecutor) setSysVariable(name string, v *expression.VarAssignment) error {
	stochaseinstein_dbars := e.ctx.GetStochaseinstein_dbars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if sysVar.SINTERLOCKe == variable.SINTERLOCKeNone {
		return errors.Errorf("Variable '%s' is a read only variable", name)
	}
	var valStr string
	var sINTERLOCKeStr string
	if v.IsGlobal {
		sINTERLOCKeStr = sINTERLOCKeGlobal
		// Set global sINTERLOCKe system variable.
		if sysVar.SINTERLOCKe&variable.SINTERLOCKeGlobal == 0 {
			return errors.Errorf("Variable '%s' is a STOCHASTIK variable and can't be used with SET GLOBAL", name)
		}
		value, err := e.getVarValue(v, sysVar)
		if err != nil {
			return err
		}
		if value.IsNull() {
			value.SetString("", allegrosql.DefaultDefCauslationName)
		}
		valStr, err = value.ToString()
		if err != nil {
			return err
		}
		err = stochaseinstein_dbars.GlobalVarsAccessor.SetGlobalSysVar(name, valStr)
		if err != nil {
			return err
		}
		err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
			auditPlugin := plugin.DeclareAuditManifest(p.Manifest)
			if auditPlugin.OnGlobalVariableEvent != nil {
				auditPlugin.OnGlobalVariableEvent(context.Background(), e.ctx.GetStochaseinstein_dbars(), name, valStr)
			}
			return nil
		})
		if err != nil {
			return err
		}
	} else {
		sINTERLOCKeStr = sINTERLOCKeStochastik
		// Set stochastik sINTERLOCKe system variable.
		if sysVar.SINTERLOCKe&variable.SINTERLOCKeStochastik == 0 {
			return errors.Errorf("Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL", name)
		}
		value, err := e.getVarValue(v, nil)
		if err != nil {
			return err
		}
		oldSnapshotTS := stochaseinstein_dbars.SnapshotTS
		if name == variable.TxnIsolationOneShot && stochaseinstein_dbars.InTxn() {
			return errors.Trace(ErrCantChangeTxCharacteristics)
		}
		if name == variable.MilevaDBFoundInPlanCache {
			stochaseinstein_dbars.StmtCtx.AppendWarning(fmt.Errorf("Set operation for '%s' will not take effect", variable.MilevaDBFoundInPlanCache))
			return nil
		}
		err = variable.SetStochastikSystemVar(stochaseinstein_dbars, name, value)
		if err != nil {
			return err
		}
		newSnapshotIsSet := stochaseinstein_dbars.SnapshotTS > 0 && stochaseinstein_dbars.SnapshotTS != oldSnapshotTS
		if newSnapshotIsSet {
			err = gcutil.ValidateSnapshot(e.ctx, stochaseinstein_dbars.SnapshotTS)
			if err != nil {
				stochaseinstein_dbars.SnapshotTS = oldSnapshotTS
				return err
			}
		}
		err = e.loadSnapshotSchemaReplicantIfNeeded(name)
		if err != nil {
			stochaseinstein_dbars.SnapshotTS = oldSnapshotTS
			return err
		}
		if value.IsNull() {
			valStr = "NULL"
		} else {
			var err error
			valStr, err = value.ToString()
			terror.Log(err)
		}
	}
	if sINTERLOCKeStr == sINTERLOCKeGlobal {
		logutil.BgLogger().Info(fmt.Sprintf("set %s var", sINTERLOCKeStr), zap.Uint64("conn", stochaseinstein_dbars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
	} else {
		// Clients are often noisy in setting stochastik variables such as
		// autocommit, timezone, query cache
		logutil.BgLogger().Debug(fmt.Sprintf("set %s var", sINTERLOCKeStr), zap.Uint64("conn", stochaseinstein_dbars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
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
	case variable.MilevaDBStmtSummaryMaxALLEGROSQLLength:
		return stmtsummary.StmtSummaryByDigestMap.SetMaxALLEGROSQLLength(valStr, !v.IsGlobal)
	case variable.MilevaDBCapturePlanBaseline:
		variable.CapturePlanBaseline.Set(strings.ToLower(valStr), !v.IsGlobal)
	}

	return nil
}

func (e *SetExecutor) setCharset(cs, co string, isSetName bool) error {
	var err error
	if len(co) == 0 {
		if co, err = charset.GetDefaultDefCauslation(cs); err != nil {
			return err
		}
	} else {
		var defCausl *charset.DefCauslation
		if defCausl, err = defCauslate.GetDefCauslationByName(co); err != nil {
			return err
		}
		if defCausl.CharsetName != cs {
			return charset.ErrDefCauslationCharsetMismatch.GenWithStackByArgs(defCausl.Name, cs)
		}
	}
	stochaseinstein_dbars := e.ctx.GetStochaseinstein_dbars()
	if isSetName {
		for _, v := range variable.SetNamesVariables {
			if err = stochaseinstein_dbars.SetSystemVar(v, cs); err != nil {
				return errors.Trace(err)
			}
		}
		return errors.Trace(stochaseinstein_dbars.SetSystemVar(variable.DefCauslationConnection, co))
	}
	// Set charset statement, see also https://dev.allegrosql.com/doc/refman/8.0/en/set-character-set.html.
	for _, v := range variable.SetCharsetVariables {
		if err = stochaseinstein_dbars.SetSystemVar(v, cs); err != nil {
			return errors.Trace(err)
		}
	}
	csDb, err := stochaseinstein_dbars.GlobalVarsAccessor.GetGlobalSysVar(variable.CharsetDatabase)
	if err != nil {
		return err
	}
	coDb, err := stochaseinstein_dbars.GlobalVarsAccessor.GetGlobalSysVar(variable.DefCauslationDatabase)
	if err != nil {
		return err
	}
	err = stochaseinstein_dbars.SetSystemVar(variable.CharacterSetConnection, csDb)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(stochaseinstein_dbars.SetSystemVar(variable.DefCauslationConnection, coDb))
}

func (e *SetExecutor) getVarValue(v *expression.VarAssignment, sysVar *variable.SysVar) (value types.Causet, err error) {
	if v.IsDefault {
		// To set a STOCHASTIK variable to the GLOBAL value or a GLOBAL value
		// to the compiled-in MyALLEGROSQL default value, use the DEFAULT keyword.
		// See http://dev.allegrosql.com/doc/refman/5.7/en/set-statement.html
		if sysVar != nil {
			value = types.NewStringCauset(sysVar.Value)
		} else {
			s, err1 := variable.GetGlobalSystemVar(e.ctx.GetStochaseinstein_dbars(), v.Name)
			if err1 != nil {
				return value, err1
			}
			value = types.NewStringCauset(s)
		}
		return
	}
	value, err = v.Expr.Eval(chunk.Event{})
	return value, err
}

func (e *SetExecutor) loadSnapshotSchemaReplicantIfNeeded(name string) error {
	if name != variable.MilevaDBSnapshot {
		return nil
	}
	vars := e.ctx.GetStochaseinstein_dbars()
	if vars.SnapshotTS == 0 {
		vars.SnapshotschemaReplicant = nil
		return nil
	}
	logutil.BgLogger().Info("load snapshot info schemaReplicant", zap.Uint64("conn", vars.ConnectionID), zap.Uint64("SnapshotTS", vars.SnapshotTS))
	dom := petri.GetPetri(e.ctx)
	snapInfo, err := dom.GetSnapshotSchemaReplicant(vars.SnapshotTS)
	if err != nil {
		return err
	}
	vars.SnapshotschemaReplicant = snapInfo
	return nil
}
