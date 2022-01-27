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

package expression

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// Error instances.
var (
	// All the exported errors are defined here:
	ErrIncorrectParameterCount     = terror.ClassExpression.New(allegrosql.ErrWrongParamcountToNativeFct, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongParamcountToNativeFct])
	ErrDivisionByZero              = terror.ClassExpression.New(allegrosql.ErrDivisionByZero, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDivisionByZero])
	ErrRegexp                      = terror.ClassExpression.New(allegrosql.ErrRegexp, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrRegexp])
	ErrOperandDeferredCausets      = terror.ClassExpression.New(allegrosql.ErrOperandDeferredCausets, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrOperandDeferredCausets])
	ErrCutValueGroupConcat         = terror.ClassExpression.New(allegrosql.ErrCutValueGroupConcat, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCutValueGroupConcat])
	ErrFunctionsNoopImpl           = terror.ClassExpression.New(allegrosql.ErrNotSupportedYet, "function %s has only noop implementation in milevadb now, use milevadb_enable_noop_functions to enable these functions")
	ErrInvalidArgumentForLogarithm = terror.ClassExpression.New(allegrosql.ErrInvalidArgumentForLogarithm, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidArgumentForLogarithm])
	ErrIncorrectType               = terror.ClassExpression.New(allegrosql.ErrIncorrectType, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrIncorrectType])

	// All the un-exported errors are defined here:
	errFunctionNotExists             = terror.ClassExpression.New(allegrosql.ErrSFIDeloesNotExist, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSFIDeloesNotExist])
	errZlibZData                     = terror.ClassExpression.New(allegrosql.ErrZlibZData, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrZlibZData])
	errZlibZBuf                      = terror.ClassExpression.New(allegrosql.ErrZlibZBuf, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrZlibZBuf])
	errIncorrectArgs                 = terror.ClassExpression.New(allegrosql.ErrWrongArguments, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongArguments])
	errUnknownCharacterSet           = terror.ClassExpression.New(allegrosql.ErrUnknownCharacterSet, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownCharacterSet])
	errDefaultValue                  = terror.ClassExpression.New(allegrosql.ErrInvalidDefault, "invalid default value")
	errDeprecatedSyntaxNoRememristed = terror.ClassExpression.New(allegrosql.ErrWarnDeprecatedSyntaxNoRememristed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWarnDeprecatedSyntaxNoRememristed])
	errBadField                      = terror.ClassExpression.New(allegrosql.ErrBadField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadField])
	errWarnAllowedPacketOverflowed   = terror.ClassExpression.New(allegrosql.ErrWarnAllowedPacketOverflowed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWarnAllowedPacketOverflowed])
	errWarnOptionIgnored             = terror.ClassExpression.New(allegrosql.WarnOptionIgnored, allegrosql.MyALLEGROSQLErrName[allegrosql.WarnOptionIgnored])
	errTruncatedWrongValue           = terror.ClassExpression.New(allegrosql.ErrTruncatedWrongValue, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTruncatedWrongValue])
	errUnknownLocale                 = terror.ClassExpression.New(allegrosql.ErrUnknownLocale, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownLocale])
	errNonUniq                       = terror.ClassExpression.New(allegrosql.ErrNonUniq, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNonUniq])

	// Sequence usage privilege check.
	errSequenceAccessDenied = terror.ClassExpression.New(allegrosql.ErrBlockaccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockaccessDenied])
)

// handleInvalidTimeError reports error or warning depend on the context.
func handleInvalidTimeError(ctx stochastikctx.Context, err error) error {
	if err == nil || !(types.ErrWrongValue.Equal(err) ||
		types.ErrTruncatedWrongVal.Equal(err) || types.ErrInvalidWeekModeFormat.Equal(err) ||
		types.ErrDatetimeFunctionOverflow.Equal(err)) {
		return err
	}
	sc := ctx.GetStochastikVars().StmtCtx
	err = sc.HandleTruncate(err)
	if ctx.GetStochastikVars().StrictALLEGROSQLMode && (sc.InInsertStmt || sc.InUFIDelateStmt || sc.InDeleteStmt) {
		return err
	}
	return nil
}

// handleDivisionByZeroError reports error or warning depend on the context.
func handleDivisionByZeroError(ctx stochastikctx.Context) error {
	sc := ctx.GetStochastikVars().StmtCtx
	if sc.InInsertStmt || sc.InUFIDelateStmt || sc.InDeleteStmt {
		if !ctx.GetStochastikVars().ALLEGROSQLMode.HasErrorForDivisionByZeroMode() {
			return nil
		}
		if ctx.GetStochastikVars().StrictALLEGROSQLMode && !sc.DividedByZeroAsWarning {
			return ErrDivisionByZero
		}
	}
	sc.AppendWarning(ErrDivisionByZero)
	return nil
}
