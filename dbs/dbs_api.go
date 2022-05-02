Copuright 2021 Whtcorps Inc; EinsteinDB and MilevaDB aithors; Licensed Under Apache 2.0. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package dbs

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/format"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	field_types "github.com/whtcorpsinc/berolinaAllegroSQL/types"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/block/blocks"
	"github.com/whtcorpsinc/MilevaDB-Prod/blockcodec"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs/memristed"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta/autoid"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/petriutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/set"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	driver "github.com/whtcorpsinc/MilevaDB-Prod/types/berolinaAllegroSQL_driver"
	"go.uber.org/zap"
)

const (
	expressionIndexPrefix        = "_V$"
	changingDeferredCausetPrefix = "_DefCaus$_"
	changingIndexPrefix          = "_Idx$_"
)

func (d *dbs) CreateSchema(ctx stochastikctx.Context, schemaReplicant perceptron.CIStr, charsetInfo *ast.CharsetOpt) error {
	dbInfo := &perceptron.DBInfo{Name: schemaReplicant}
	if charsetInfo != nil {
		chs, defCausl, err := ResolveCharsetDefCauslation(ast.CharsetOpt{Chs: charsetInfo.Chs, DefCaus: charsetInfo.DefCaus})
		if err != nil {
			return errors.Trace(err)
		}
		dbInfo.Charset = chs
		dbInfo.DefCauslate = defCausl
	} else {
		dbInfo.Charset, dbInfo.DefCauslate = charset.GetDefaultCharsetAndDefCauslate()
	}
	return d.CreateSchemaWithInfo(ctx, dbInfo, OnExistError, false /*tryRetainID*/)
}

func (d *dbs) CreateSchemaWithInfo(
	ctx stochastikctx.Context,
	dbInfo *perceptron.DBInfo,
	onExist OnExist,
	tryRetainID bool,
) error {
	is := d.GetSchemaReplicantWithInterceptor(ctx)
	_, ok := is.SchemaByName(dbInfo.Name)
	if ok {
		err := schemareplicant.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
		switch onExist {
		case OnExistIgnore:
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return nil
		case OnExistError, OnExistReplace:
			// FIXME: can we implement MariaDB's CREATE OR REPLACE SCHEMA?
			return err
		}
	}

	if err := checkTooLongSchema(dbInfo.Name); err != nil {
		return errors.Trace(err)
	}

	if err := checkCharsetAndDefCauslation(dbInfo.Charset, dbInfo.DefCauslate); err != nil {
		return errors.Trace(err)
	}

	// FIXME: support `tryRetainID`.
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	dbInfo.ID = genIDs[0]

	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		Type:       perceptron.CausetActionCreateSchema,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) AlterSchema(ctx stochastikctx.Context, stmt *ast.AlterDatabaseStmt) (err error) {
	// Resolve target charset and defCauslation from options.
	var toCharset, toDefCauslate string
	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			if toCharset == "" {
				toCharset = val.Value
			} else if toCharset != val.Value {
				return ErrConflictingDeclarations.GenWithStackByArgs(toCharset, val.Value)
			}
		case ast.DatabaseOptionDefCauslate:
			info, err := defCauslate.GetDefCauslationByName(val.Value)
			if err != nil {
				return errors.Trace(err)
			}
			if toCharset == "" {
				toCharset = info.CharsetName
			} else if toCharset != info.CharsetName {
				return ErrConflictingDeclarations.GenWithStackByArgs(toCharset, info.CharsetName)
			}
			toDefCauslate = info.Name
		}
	}
	if toDefCauslate == "" {
		if toDefCauslate, err = charset.GetDefaultDefCauslation(toCharset); err != nil {
			return errors.Trace(err)
		}
	}

	// Check if need to change charset/defCauslation.
	dbName := perceptron.NewCIStr(stmt.Name)
	is := d.GetSchemaReplicantWithInterceptor(ctx)
	dbInfo, ok := is.SchemaByName(dbName)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
	}
	if dbInfo.Charset == toCharset && dbInfo.DefCauslate == toDefCauslate {
		return nil
	}

	// Do the DBS job.
	job := &perceptron.Job{
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		Type:       perceptron.CausetActionModifySchemaCharsetAndDefCauslate,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{toCharset, toDefCauslate},
	}
	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) DropSchema(ctx stochastikctx.Context, schemaReplicant perceptron.CIStr) (err error) {
	is := d.GetSchemaReplicantWithInterceptor(ctx)
	old, ok := is.SchemaByName(schemaReplicant)
	if !ok {
		return errors.Trace(schemareplicant.ErrDatabaseNotExists)
	}
	job := &perceptron.Job{
		SchemaID:   old.ID,
		SchemaName: old.Name.L,
		Type:       perceptron.CausetActionDropSchema,
		BinlogInfo: &perceptron.HistoryInfo{},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	if err != nil {
		return errors.Trace(err)
	}
	if !config.TableLockEnabled() {
		return nil
	}
	// Clear block locks hold by the stochastik.
	tbs := is.SchemaTables(schemaReplicant)
	lockTableIDs := make([]int64, 0)
	for _, tb := range tbs {
		if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok {
			lockTableIDs = append(lockTableIDs, tb.Meta().ID)
		}
	}
	ctx.ReleaseTableLockByTableIDs(lockTableIDs)
	return nil
}

func checkTooLongSchema(schemaReplicant perceptron.CIStr) error {
	if len(schemaReplicant.L) > allegrosql.MaxDatabaseNameLength {
		return ErrTooLongIdent.GenWithStackByArgs(schemaReplicant)
	}
	return nil
}

func checkTooLongTable(block perceptron.CIStr) error {
	if len(block.L) > allegrosql.MaxTableNameLength {
		return ErrTooLongIdent.GenWithStackByArgs(block)
	}
	return nil
}

func checkTooLongIndex(index perceptron.CIStr) error {
	if len(index.L) > allegrosql.MaxIndexIdentifierLen {
		return ErrTooLongIdent.GenWithStackByArgs(index)
	}
	return nil
}

func setDeferredCausetFlagWithConstraint(defCausMap map[string]*block.DeferredCauset, v *ast.Constraint) {
	switch v.Tp {
	case ast.ConstraintPrimaryKey:
		for _, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := defCausMap[key.DeferredCauset.Name.L]
			if !ok {
				continue
			}
			c.Flag |= allegrosql.PriKeyFlag
			// Primary key can not be NULL.
			c.Flag |= allegrosql.NotNullFlag
		}
	case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		for i, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := defCausMap[key.DeferredCauset.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first defCausumn can be set
				// if unique index has multi defCausumns,
				// the flag should be MultipleKeyFlag.
				// See https://dev.allegrosql.com/doc/refman/5.7/en/show-defCausumns.html
				if len(v.Keys) > 1 {
					c.Flag |= allegrosql.MultipleKeyFlag
				} else {
					c.Flag |= allegrosql.UniqueKeyFlag
				}
			}
		}
	case ast.ConstraintKey, ast.ConstraintIndex:
		for i, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := defCausMap[key.DeferredCauset.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first defCausumn can be set.
				c.Flag |= allegrosql.MultipleKeyFlag
			}
		}
	}
}

func buildDeferredCausetsAndConstraints(
	ctx stochastikctx.Context,
	defCausDefs []*ast.DeferredCausetDef,
	constraints []*ast.Constraint,
	tblCharset string,
	tblDefCauslate string,
) ([]*block.DeferredCauset, []*ast.Constraint, error) {
	defCausMap := map[string]*block.DeferredCauset{}
	// outPriKeyConstraint is the primary key constraint out of defCausumn definition. such as: create block t1 (id int , age int, primary key(id));
	var outPriKeyConstraint *ast.Constraint
	for _, v := range constraints {
		if v.Tp == ast.ConstraintPrimaryKey {
			outPriKeyConstraint = v
			break
		}
	}
	defcaus := make([]*block.DeferredCauset, 0, len(defCausDefs))
	for i, defCausDef := range defCausDefs {
		defCaus, cts, err := buildDeferredCausetAndConstraint(ctx, i, defCausDef, outPriKeyConstraint, tblCharset, tblDefCauslate)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		defCaus.State = perceptron.StatePublic
		constraints = append(constraints, cts...)
		defcaus = append(defcaus, defCaus)
		defCausMap[defCausDef.Name.Name.L] = defCaus
	}
	// Traverse block Constraints and set defCaus.flag.
	for _, v := range constraints {
		setDeferredCausetFlagWithConstraint(defCausMap, v)
	}
	return defcaus, constraints, nil
}

// ResolveCharsetDefCauslation will resolve the charset and defCauslate by the order of parameters:
// * If any given ast.CharsetOpt is not empty, the resolved charset and defCauslate will be returned.
// * If all ast.CharsetOpts are empty, the default charset and defCauslate will be returned.
func ResolveCharsetDefCauslation(charsetOpts ...ast.CharsetOpt) (string, string, error) {
	for _, v := range charsetOpts {
		if v.DefCaus != "" {
			defCauslation, err := defCauslate.GetDefCauslationByName(v.DefCaus)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			if v.Chs != "" && defCauslation.CharsetName != v.Chs {
				return "", "", charset.ErrDefCauslationCharsetMismatch.GenWithStackByArgs(v.DefCaus, v.Chs)
			}
			return defCauslation.CharsetName, v.DefCaus, nil
		}
		if v.Chs != "" {
			defCausl, err := charset.GetDefaultDefCauslation(v.Chs)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			return v.Chs, defCausl, err
		}
	}
	chs, defCausl := charset.GetDefaultCharsetAndDefCauslate()
	return chs, defCausl, nil
}

func typesNeedCharset(tp byte) bool {
	switch tp {
	case allegrosql.TypeString, allegrosql.TypeVarchar, allegrosql.TypeVarString,
		allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		allegrosql.TypeEnum, allegrosql.TypeSet:
		return true
	}
	return false
}

func setCharsetDefCauslationFlenDecimal(tp *types.FieldType, defCausCharset, defCausDefCauslate string) error {
	if typesNeedCharset(tp.Tp) {
		tp.Charset = defCausCharset
		tp.DefCauslate = defCausDefCauslate
	} else {
		tp.Charset = charset.CharsetBin
		tp.DefCauslate = charset.CharsetBin
	}

	// Use default value for flen or decimal when they are unspecified.
	defaultFlen, defaultDecimal := allegrosql.GetDefaultFieldLengthAndDecimal(tp.Tp)
	if tp.Flen == types.UnspecifiedLength {
		tp.Flen = defaultFlen
		if allegrosql.HasUnsignedFlag(tp.Flag) && tp.Tp != allegrosql.TypeLonglong && allegrosql.IsIntegerType(tp.Tp) {
			// Issue #4684: the flen of unsigned integer(except bigint) is 1 digit shorter than signed integer
			// because it has no prefix "+" or "-" character.
			tp.Flen--
		}
	}
	if tp.Decimal == types.UnspecifiedLength {
		tp.Decimal = defaultDecimal
	}
	return nil
}

// buildDeferredCausetAndConstraint builds block.DeferredCauset and ast.Constraint from the parameters.
// outPriKeyConstraint is the primary key constraint out of defCausumn definition. For example:
// `create block t1 (id int , age int, primary key(id));`
func buildDeferredCausetAndConstraint(
	ctx stochastikctx.Context,
	offset int,
	defCausDef *ast.DeferredCausetDef,
	outPriKeyConstraint *ast.Constraint,
	tblCharset string,
	tblDefCauslate string,
) (*block.DeferredCauset, []*ast.Constraint, error) {
	if defCausName := defCausDef.Name.Name.L; defCausName == perceptron.ExtraHandleName.L {
		return nil, nil, ErrWrongDeferredCausetName.GenWithStackByArgs(defCausName)
	}

	// specifiedDefCauslate refers to the last defCauslate specified in defCausDef.Options.
	chs, defCausl, err := getCharsetAndDefCauslateInDeferredCausetDef(defCausDef)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	chs, defCausl, err = ResolveCharsetDefCauslation(
		ast.CharsetOpt{Chs: chs, DefCaus: defCausl},
		ast.CharsetOpt{Chs: tblCharset, DefCaus: tblDefCauslate},
	)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if err := setCharsetDefCauslationFlenDecimal(defCausDef.Tp, chs, defCausl); err != nil {
		return nil, nil, errors.Trace(err)
	}
	defCaus, cts, err := defCausumnDefToDefCaus(ctx, offset, defCausDef, outPriKeyConstraint)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return defCaus, cts, nil
}

// checkDeferredCausetDefaultValue checks the default value of the defCausumn.
// In non-strict ALLEGROALLEGROSQL mode, if the default value of the defCausumn is an empty string, the default value can be ignored.
// In strict ALLEGROALLEGROSQL mode, TEXT/BLOB/JSON can't have not null default values.
// In NO_ZERO_DATE ALLEGROALLEGROSQL mode, TIMESTAMP/DATE/DATETIME type can't have zero date like '0000-00-00' or '0000-00-00 00:00:00'.
func checkDeferredCausetDefaultValue(ctx stochastikctx.Context, defCaus *block.DeferredCauset, value interface{}) (bool, interface{}, error) {
	hasDefaultValue := true
	if value != nil && (defCaus.Tp == allegrosql.TypeJSON ||
		defCaus.Tp == allegrosql.TypeTinyBlob || defCaus.Tp == allegrosql.TypeMediumBlob ||
		defCaus.Tp == allegrosql.TypeLongBlob || defCaus.Tp == allegrosql.TypeBlob) {
		// In non-strict ALLEGROALLEGROSQL mode.
		if !ctx.GetStochaseinstein_dbars().ALLEGROSQLMode.HasStrictMode() && value == "" {
			if defCaus.Tp == allegrosql.TypeBlob || defCaus.Tp == allegrosql.TypeLongBlob {
				// The TEXT/BLOB default value can be ignored.
				hasDefaultValue = false
			}
			// In non-strict ALLEGROALLEGROSQL mode, if the defCausumn type is json and the default value is null, it is initialized to an empty array.
			if defCaus.Tp == allegrosql.TypeJSON {
				value = `null`
			}
			sc := ctx.GetStochaseinstein_dbars().StmtCtx
			sc.AppendWarning(errBlobCantHaveDefault.GenWithStackByArgs(defCaus.Name.O))
			return hasDefaultValue, value, nil
		}
		// In strict ALLEGROALLEGROSQL mode or default value is not an empty string.
		return hasDefaultValue, value, errBlobCantHaveDefault.GenWithStackByArgs(defCaus.Name.O)
	}
	if value != nil && ctx.GetStochaseinstein_dbars().ALLEGROSQLMode.HasNoZeroDateMode() &&
		ctx.GetStochaseinstein_dbars().ALLEGROSQLMode.HasStrictMode() && types.IsTypeTime(defCaus.Tp) {
		if vv, ok := value.(string); ok {
			timeValue, err := expression.GetTimeValue(ctx, vv, defCaus.Tp, int8(defCaus.Decimal))
			if err != nil {
				return hasDefaultValue, value, errors.Trace(err)
			}
			if timeValue.GetMysqlTime().CoreTime() == types.ZeroCoreTime {
				return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(defCaus.Name.O)
			}
		}
	}
	return hasDefaultValue, value, nil
}

func checkSequenceDefaultValue(defCaus *block.DeferredCauset) error {
	if allegrosql.IsIntegerType(defCaus.Tp) {
		return nil
	}
	return ErrDeferredCausetTypeUnsupportedNextValue.GenWithStackByArgs(defCaus.DeferredCausetInfo.Name.O)
}

func convertTimestamFIDelefaultValToUTC(ctx stochastikctx.Context, defaultVal interface{}, defCaus *block.DeferredCauset) (interface{}, error) {
	if defaultVal == nil || defCaus.Tp != allegrosql.TypeTimestamp {
		return defaultVal, nil
	}
	if vv, ok := defaultVal.(string); ok {
		if vv != types.ZeroDatetimeStr && !strings.EqualFold(vv, ast.CurrentTimestamp) {
			t, err := types.ParseTime(ctx.GetStochaseinstein_dbars().StmtCtx, vv, defCaus.Tp, int8(defCaus.Decimal))
			if err != nil {
				return defaultVal, errors.Trace(err)
			}
			err = t.ConvertTimeZone(ctx.GetStochaseinstein_dbars().Location(), time.UTC)
			if err != nil {
				return defaultVal, errors.Trace(err)
			}
			defaultVal = t.String()
		}
	}
	return defaultVal, nil
}

// isExplicitTimeStamp is used to check if explicit_defaults_for_timestamp is on or off.
// Check out this link for more details.
// https://dev.allegrosql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_explicit_defaults_for_timestamp
func isExplicitTimeStamp() bool {
	// TODO: implement the behavior as MyALLEGROSQL when explicit_defaults_for_timestamp = off, then this function could return false.
	return true
}

// processDeferredCausetFlags is used by defCausumnDefToDefCaus and processDeferredCausetOptions. It is intended to unify behaviors on `create/add` and `modify/change` statements. Check milevadb#issue#19342.
func processDeferredCausetFlags(defCaus *block.DeferredCauset) {
	if defCaus.FieldType.EvalType().IsStringHoTT() && defCaus.Charset == charset.CharsetBin {
		defCaus.Flag |= allegrosql.BinaryFlag
	}
	if defCaus.Tp == allegrosql.TypeBit {
		// For BIT field, it's charset is binary but does not have binary flag.
		defCaus.Flag &= ^allegrosql.BinaryFlag
		defCaus.Flag |= allegrosql.UnsignedFlag
	}
	if defCaus.Tp == allegrosql.TypeYear {
		// For Year field, it's charset is binary but does not have binary flag.
		defCaus.Flag &= ^allegrosql.BinaryFlag
		defCaus.Flag |= allegrosql.ZerofillFlag
	}

	// If you specify ZEROFILL for a numeric defCausumn, MyALLEGROSQL automatically adds the UNSIGNED attribute to the defCausumn.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/numeric-type-overview.html for more details.
	// But some types like bit and year, won't show its unsigned flag in `show create block`.
	if allegrosql.HasZerofillFlag(defCaus.Flag) {
		defCaus.Flag |= allegrosql.UnsignedFlag
	}
}

// defCausumnDefToDefCaus converts DeferredCausetDef to DefCaus and TableConstraints.
// outPriKeyConstraint is the primary key constraint out of defCausumn definition. such as: create block t1 (id int , age int, primary key(id));
func defCausumnDefToDefCaus(ctx stochastikctx.Context, offset int, defCausDef *ast.DeferredCausetDef, outPriKeyConstraint *ast.Constraint) (*block.DeferredCauset, []*ast.Constraint, error) {
	var constraints = make([]*ast.Constraint, 0)
	defCaus := block.ToDeferredCauset(&perceptron.DeferredCausetInfo{
		Offset:    offset,
		Name:      defCausDef.Name.Name,
		FieldType: *defCausDef.Tp,
		// TODO: remove this version field after there is no old version.
		Version: perceptron.CurrLatestDeferredCausetInfoVersion,
	})

	if !isExplicitTimeStamp() {
		// Check and set TimestampFlag, OnUFIDelateNowFlag and NotNullFlag.
		if defCaus.Tp == allegrosql.TypeTimestamp {
			defCaus.Flag |= allegrosql.TimestampFlag
			defCaus.Flag |= allegrosql.OnUFIDelateNowFlag
			defCaus.Flag |= allegrosql.NotNullFlag
		}
	}
	var err error
	setOnUFIDelateNow := false
	hasDefaultValue := false
	hasNullFlag := false
	if defCausDef.Options != nil {
		length := types.UnspecifiedLength

		keys := []*ast.IndexPartSpecification{
			{
				DeferredCauset: defCausDef.Name,
				Length:         length,
			},
		}

		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

		for _, v := range defCausDef.Options {
			switch v.Tp {
			case ast.DeferredCausetOptionNotNull:
				defCaus.Flag |= allegrosql.NotNullFlag
			case ast.DeferredCausetOptionNull:
				defCaus.Flag &= ^allegrosql.NotNullFlag
				removeOnUFIDelateNowFlag(defCaus)
				hasNullFlag = true
			case ast.DeferredCausetOptionAutoIncrement:
				defCaus.Flag |= allegrosql.AutoIncrementFlag
			case ast.DeferredCausetOptionPrimaryKey:
				// Check PriKeyFlag first to avoid extra duplicate constraints.
				if defCaus.Flag&allegrosql.PriKeyFlag == 0 {
					constraint := &ast.Constraint{Tp: ast.ConstraintPrimaryKey, Keys: keys}
					constraints = append(constraints, constraint)
					defCaus.Flag |= allegrosql.PriKeyFlag
				}
			case ast.DeferredCausetOptionUniqKey:
				// Check UniqueFlag first to avoid extra duplicate constraints.
				if defCaus.Flag&allegrosql.UniqueFlag == 0 {
					constraint := &ast.Constraint{Tp: ast.ConstraintUniqKey, Keys: keys}
					constraints = append(constraints, constraint)
					defCaus.Flag |= allegrosql.UniqueKeyFlag
				}
			case ast.DeferredCausetOptionDefaultValue:
				hasDefaultValue, err = setDefaultValue(ctx, defCaus, v)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				removeOnUFIDelateNowFlag(defCaus)
			case ast.DeferredCausetOptionOnUFIDelate:
				// TODO: Support other time functions.
				if defCaus.Tp == allegrosql.TypeTimestamp || defCaus.Tp == allegrosql.TypeDatetime {
					if !expression.IsValidCurrentTimestampExpr(v.Expr, defCausDef.Tp) {
						return nil, nil, ErrInvalidOnUFIDelate.GenWithStackByArgs(defCaus.Name)
					}
				} else {
					return nil, nil, ErrInvalidOnUFIDelate.GenWithStackByArgs(defCaus.Name)
				}
				defCaus.Flag |= allegrosql.OnUFIDelateNowFlag
				setOnUFIDelateNow = true
			case ast.DeferredCausetOptionComment:
				err := setDeferredCausetComment(ctx, defCaus, v)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
			case ast.DeferredCausetOptionGenerated:
				sb.Reset()
				err = v.Expr.Restore(restoreCtx)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				defCaus.GeneratedExprString = sb.String()
				defCaus.GeneratedStored = v.Stored
				_, dependDefCausNames := findDependedDeferredCausetNames(defCausDef)
				defCaus.Dependences = dependDefCausNames
			case ast.DeferredCausetOptionDefCauslate:
				if field_types.HasCharset(defCausDef.Tp) {
					defCaus.FieldType.DefCauslate = v.StrValue
				}
			case ast.DeferredCausetOptionFulltext:
				ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(ErrTableCantHandleFt)
			case ast.DeferredCausetOptionCheck:
				ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(ErrUnsupportedConstraintCheck.GenWithStackByArgs("DeferredCauset check"))
			}
		}
	}

	processDefaultValue(defCaus, hasDefaultValue, setOnUFIDelateNow)

	processDeferredCausetFlags(defCaus)

	err = checkPriKeyConstraint(defCaus, hasDefaultValue, hasNullFlag, outPriKeyConstraint)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = checkDeferredCausetValueConstraint(defCaus, defCaus.DefCauslate)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = checkDefaultValue(ctx, defCaus, hasDefaultValue)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = checkDeferredCausetFieldLength(defCaus)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return defCaus, constraints, nil
}

// getDefaultValue will get the default value for defCausumn.
// 1: get the expr restored string for the defCausumn which uses sequence next value as default value.
// 2: get specific default value for the other defCausumn.
func getDefaultValue(ctx stochastikctx.Context, defCaus *block.DeferredCauset, c *ast.DeferredCausetOption) (interface{}, bool, error) {
	tp, fsp := defCaus.FieldType.Tp, defCaus.FieldType.Decimal
	if tp == allegrosql.TypeTimestamp || tp == allegrosql.TypeDatetime {
		switch x := c.Expr.(type) {
		case *ast.FuncCallExpr:
			if x.FnName.L == ast.CurrentTimestamp {
				defaultFsp := 0
				if len(x.Args) == 1 {
					if val := x.Args[0].(*driver.ValueExpr); val != nil {
						defaultFsp = int(val.GetInt64())
					}
				}
				if defaultFsp != fsp {
					return nil, false, ErrInvalidDefaultValue.GenWithStackByArgs(defCaus.Name.O)
				}
			}
		}
		vd, err := expression.GetTimeValue(ctx, c.Expr, tp, int8(fsp))
		value := vd.GetValue()
		if err != nil {
			return nil, false, ErrInvalidDefaultValue.GenWithStackByArgs(defCaus.Name.O)
		}

		// Value is nil means `default null`.
		if value == nil {
			return nil, false, nil
		}

		// If value is types.Time, convert it to string.
		if vv, ok := value.(types.Time); ok {
			return vv.String(), false, nil
		}

		return value, false, nil
	}
	// handle default next value of sequence. (keep the expr string)
	str, isSeqExpr, err := tryToGetSequenceDefaultValue(c)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	if isSeqExpr {
		return str, true, nil
	}

	// evaluate the non-sequence expr to a certain value.
	v, err := expression.EvalAstExpr(ctx, c.Expr)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if v.IsNull() {
		return nil, false, nil
	}

	if v.HoTT() == types.HoTTBinaryLiteral || v.HoTT() == types.HoTTMysqlBit {
		if tp == allegrosql.TypeBit ||
			tp == allegrosql.TypeString || tp == allegrosql.TypeVarchar || tp == allegrosql.TypeVarString ||
			tp == allegrosql.TypeBlob || tp == allegrosql.TypeLongBlob || tp == allegrosql.TypeMediumBlob || tp == allegrosql.TypeTinyBlob ||
			tp == allegrosql.TypeJSON {
			// For BinaryLiteral / string fields, when getting default value we cast the value into BinaryLiteral{}, thus we return
			// its raw string content here.
			return v.GetBinaryLiteral().ToString(), false, nil
		}
		// For other HoTT of fields (e.g. INT), we supply its integer as string value.
		value, err := v.GetBinaryLiteral().ToInt(ctx.GetStochaseinstein_dbars().StmtCtx)
		if err != nil {
			return nil, false, err
		}
		return strconv.FormatUint(value, 10), false, nil
	}

	switch tp {
	case allegrosql.TypeSet:
		val, err := setSetDefaultValue(v, defCaus)
		return val, false, err
	case allegrosql.TypeDuration:
		if v, err = v.ConvertTo(ctx.GetStochaseinstein_dbars().StmtCtx, &defCaus.FieldType); err != nil {
			return "", false, errors.Trace(err)
		}
	case allegrosql.TypeBit:
		if v.HoTT() == types.HoTTInt64 || v.HoTT() == types.HoTTUint64 {
			// For BIT fields, convert int into BinaryLiteral.
			return types.NewBinaryLiteralFromUint(v.GetUint64(), -1).ToString(), false, nil
		}
	}

	val, err := v.ToString()
	return val, false, err
}

func tryToGetSequenceDefaultValue(c *ast.DeferredCausetOption) (expr string, isExpr bool, err error) {
	if f, ok := c.Expr.(*ast.FuncCallExpr); ok && f.FnName.L == ast.NextVal {
		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		if err := c.Expr.Restore(restoreCtx); err != nil {
			return "", true, err
		}
		return sb.String(), true, nil
	}
	return "", false, nil
}

// setSetDefaultValue sets the default value for the set type. See https://dev.allegrosql.com/doc/refman/5.7/en/set.html.
func setSetDefaultValue(v types.Causet, defCaus *block.DeferredCauset) (string, error) {
	if v.HoTT() == types.HoTTInt64 {
		setCnt := len(defCaus.Elems)
		maxLimit := int64(1<<uint(setCnt) - 1)
		val := v.GetInt64()
		if val < 1 || val > maxLimit {
			return "", ErrInvalidDefaultValue.GenWithStackByArgs(defCaus.Name.O)
		}
		setVal, err := types.ParseSetValue(defCaus.Elems, uint64(val))
		if err != nil {
			return "", errors.Trace(err)
		}
		v.SetMysqlSet(setVal, defCaus.DefCauslate)
		return v.ToString()
	}

	str, err := v.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	if str == "" {
		return str, nil
	}

	ctor := defCauslate.GetDefCauslator(defCaus.DefCauslate)
	valMap := make(map[string]struct{}, len(defCaus.Elems))
	dVals := strings.Split(str, ",")
	for _, dv := range dVals {
		valMap[string(ctor.Key(dv))] = struct{}{}
	}
	var existCnt int
	for dv := range valMap {
		for i := range defCaus.Elems {
			e := string(ctor.Key(defCaus.Elems[i]))
			if e == dv {
				existCnt++
				break
			}
		}
	}
	if existCnt != len(valMap) {
		return "", ErrInvalidDefaultValue.GenWithStackByArgs(defCaus.Name.O)
	}
	setVal, err := types.ParseSetName(defCaus.Elems, str, defCaus.DefCauslate)
	if err != nil {
		return "", ErrInvalidDefaultValue.GenWithStackByArgs(defCaus.Name.O)
	}
	v.SetMysqlSet(setVal, defCaus.DefCauslate)

	return v.ToString()
}

func removeOnUFIDelateNowFlag(c *block.DeferredCauset) {
	// For timestamp DefCaus, if it is set null or default value,
	// OnUFIDelateNowFlag should be removed.
	if allegrosql.HasTimestampFlag(c.Flag) {
		c.Flag &= ^allegrosql.OnUFIDelateNowFlag
	}
}

func processDefaultValue(c *block.DeferredCauset, hasDefaultValue bool, setOnUFIDelateNow bool) {
	setTimestamFIDelefaultValue(c, hasDefaultValue, setOnUFIDelateNow)

	setYearDefaultValue(c, hasDefaultValue)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(c, hasDefaultValue)
}

func setYearDefaultValue(c *block.DeferredCauset, hasDefaultValue bool) {
	if hasDefaultValue {
		return
	}

	if c.Tp == allegrosql.TypeYear && allegrosql.HasNotNullFlag(c.Flag) {
		if err := c.SetDefaultValue("0000"); err != nil {
			logutil.BgLogger().Error("set default value failed", zap.Error(err))
		}
	}
}

func setTimestamFIDelefaultValue(c *block.DeferredCauset, hasDefaultValue bool, setOnUFIDelateNow bool) {
	if hasDefaultValue {
		return
	}

	// For timestamp DefCaus, if is not set default value or not set null, use current timestamp.
	if allegrosql.HasTimestampFlag(c.Flag) && allegrosql.HasNotNullFlag(c.Flag) {
		if setOnUFIDelateNow {
			if err := c.SetDefaultValue(types.ZeroDatetimeStr); err != nil {
				logutil.BgLogger().Error("set default value failed", zap.Error(err))
			}
		} else {
			if err := c.SetDefaultValue(strings.ToUpper(ast.CurrentTimestamp)); err != nil {
				logutil.BgLogger().Error("set default value failed", zap.Error(err))
			}
		}
	}
}

func setNoDefaultValueFlag(c *block.DeferredCauset, hasDefaultValue bool) {
	if hasDefaultValue {
		return
	}

	if !allegrosql.HasNotNullFlag(c.Flag) {
		return
	}

	// Check if it is an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	if !allegrosql.HasAutoIncrementFlag(c.Flag) && !allegrosql.HasTimestampFlag(c.Flag) {
		c.Flag |= allegrosql.NoDefaultValueFlag
	}
}

func checkDefaultValue(ctx stochastikctx.Context, c *block.DeferredCauset, hasDefaultValue bool) error {
	if !hasDefaultValue {
		return nil
	}

	if c.GetDefaultValue() != nil {
		if c.DefaultIsExpr {
			return nil
		}
		if _, err := block.GetDefCausDefaultValue(ctx, c.ToInfo()); err != nil {
			return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
		}
		return nil
	}
	// Primary key default null is invalid.
	if allegrosql.HasPriKeyFlag(c.Flag) {
		return ErrPrimaryCantHaveNull
	}

	// Set not null but default null is invalid.
	if allegrosql.HasNotNullFlag(c.Flag) {
		return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
	}

	return nil
}

// checkPriKeyConstraint check all parts of a PRIMARY KEY must be NOT NULL
func checkPriKeyConstraint(defCaus *block.DeferredCauset, hasDefaultValue, hasNullFlag bool, outPriKeyConstraint *ast.Constraint) error {
	// Primary key should not be null.
	if allegrosql.HasPriKeyFlag(defCaus.Flag) && hasDefaultValue && defCaus.GetDefaultValue() == nil {
		return types.ErrInvalidDefault.GenWithStackByArgs(defCaus.Name)
	}
	// Set primary key flag for outer primary key constraint.
	// Such as: create block t1 (id int , age int, primary key(id))
	if !allegrosql.HasPriKeyFlag(defCaus.Flag) && outPriKeyConstraint != nil {
		for _, key := range outPriKeyConstraint.Keys {
			if key.Expr == nil && key.DeferredCauset.Name.L != defCaus.Name.L {
				continue
			}
			defCaus.Flag |= allegrosql.PriKeyFlag
			break
		}
	}
	// Primary key should not be null.
	if allegrosql.HasPriKeyFlag(defCaus.Flag) && hasNullFlag {
		return ErrPrimaryCantHaveNull
	}
	return nil
}

func checkDeferredCausetValueConstraint(defCaus *block.DeferredCauset, defCauslation string) error {
	if defCaus.Tp != allegrosql.TypeEnum && defCaus.Tp != allegrosql.TypeSet {
		return nil
	}
	valueMap := make(map[string]bool, len(defCaus.Elems))
	ctor := defCauslate.GetDefCauslator(defCauslation)
	for i := range defCaus.Elems {
		val := string(ctor.Key(defCaus.Elems[i]))
		if _, ok := valueMap[val]; ok {
			tpStr := "ENUM"
			if defCaus.Tp == allegrosql.TypeSet {
				tpStr = "SET"
			}
			return types.ErrDuplicatedValueInType.GenWithStackByArgs(defCaus.Name, defCaus.Elems[i], tpStr)
		}
		valueMap[val] = true
	}
	return nil
}

func checkDuplicateDeferredCauset(defcaus []*perceptron.DeferredCausetInfo) error {
	defCausNames := set.StringSet{}
	for _, defCaus := range defcaus {
		defCausName := defCaus.Name
		if defCausNames.Exist(defCausName.L) {
			return schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(defCausName.O)
		}
		defCausNames.Insert(defCausName.L)
	}
	return nil
}

func containsDeferredCausetOption(defCausDef *ast.DeferredCausetDef, opTp ast.DeferredCausetOptionType) bool {
	for _, option := range defCausDef.Options {
		if option.Tp == opTp {
			return true
		}
	}
	return false
}

// IsAutoRandomDeferredCausetID returns true if the given defCausumn ID belongs to an auto_random defCausumn.
func IsAutoRandomDeferredCausetID(tblInfo *perceptron.TableInfo, defCausID int64) bool {
	return tblInfo.PKIsHandle && tblInfo.ContainsAutoRandomBits() && tblInfo.GetPkDefCausInfo().ID == defCausID
}

func checkGeneratedDeferredCauset(defCausDefs []*ast.DeferredCausetDef) error {
	var defCausName2Generation = make(map[string]defCausumnGenerationInDBS, len(defCausDefs))
	var exists bool
	var autoIncrementDeferredCauset string
	for i, defCausDef := range defCausDefs {
		for _, option := range defCausDef.Options {
			if option.Tp == ast.DeferredCausetOptionGenerated {
				if err := checkIllegalFn4GeneratedDeferredCauset(defCausDef.Name.Name.L, option.Expr); err != nil {
					return errors.Trace(err)
				}
			}
		}
		if containsDeferredCausetOption(defCausDef, ast.DeferredCausetOptionAutoIncrement) {
			exists, autoIncrementDeferredCauset = true, defCausDef.Name.Name.L
		}
		generated, depDefCauss := findDependedDeferredCausetNames(defCausDef)
		if !generated {
			defCausName2Generation[defCausDef.Name.Name.L] = defCausumnGenerationInDBS{
				position:  i,
				generated: false,
			}
		} else {
			defCausName2Generation[defCausDef.Name.Name.L] = defCausumnGenerationInDBS{
				position:    i,
				generated:   true,
				dependences: depDefCauss,
			}
		}
	}

	// Check whether the generated defCausumn refers to any auto-increment defCausumns
	if exists {
		for defCausName, generated := range defCausName2Generation {
			if _, found := generated.dependences[autoIncrementDeferredCauset]; found {
				return ErrGeneratedDeferredCausetRefAutoInc.GenWithStackByArgs(defCausName)
			}
		}
	}

	for _, defCausDef := range defCausDefs {
		defCausName := defCausDef.Name.Name.L
		if err := verifyDeferredCausetGeneration(defCausName2Generation, defCausName); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func checkTooLongDeferredCauset(defcaus []*perceptron.DeferredCausetInfo) error {
	for _, defCaus := range defcaus {
		defCausName := defCaus.Name.O
		if len(defCausName) > allegrosql.MaxDeferredCausetNameLength {
			return ErrTooLongIdent.GenWithStackByArgs(defCausName)
		}
	}
	return nil
}

func checkTooManyDeferredCausets(defCausDefs []*perceptron.DeferredCausetInfo) error {
	if uint32(len(defCausDefs)) > atomic.LoadUint32(&TableDeferredCausetCountLimit) {
		return errTooManyFields
	}
	return nil
}

// checkDeferredCausetsAttributes checks attributes for multiple defCausumns.
func checkDeferredCausetsAttributes(defCausDefs []*perceptron.DeferredCausetInfo) error {
	for _, defCausDef := range defCausDefs {
		if err := checkDeferredCausetAttributes(defCausDef.Name.O, &defCausDef.FieldType); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func checkDeferredCausetFieldLength(defCaus *block.DeferredCauset) error {
	if defCaus.Tp == allegrosql.TypeVarchar {
		if err := IsTooBigFieldLength(defCaus.Flen, defCaus.Name.O, defCaus.Charset); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// IsTooBigFieldLength check if the varchar type defCausumn exceeds the maximum length limit.
func IsTooBigFieldLength(defCausDefTpFlen int, defCausDefName, setCharset string) error {
	desc, err := charset.GetCharsetDesc(setCharset)
	if err != nil {
		return errors.Trace(err)
	}
	maxFlen := allegrosql.MaxFieldVarCharLength
	maxFlen /= desc.Maxlen
	if defCausDefTpFlen != types.UnspecifiedLength && defCausDefTpFlen > maxFlen {
		return types.ErrTooBigFieldLength.GenWithStack("DeferredCauset length too big for defCausumn '%s' (max = %d); use BLOB or TEXT instead", defCausDefName, maxFlen)
	}
	return nil
}

// checkDeferredCausetAttributes check attributes for single defCausumn.
func checkDeferredCausetAttributes(defCausName string, tp *types.FieldType) error {
	switch tp.Tp {
	case allegrosql.TypeNewDecimal, allegrosql.TypeDouble, allegrosql.TypeFloat:
		if tp.Flen < tp.Decimal {
			return types.ErrMBiggerThanD.GenWithStackByArgs(defCausName)
		}
	case allegrosql.TypeDatetime, allegrosql.TypeDuration, allegrosql.TypeTimestamp:
		if tp.Decimal != int(types.UnspecifiedFsp) && (tp.Decimal < int(types.MinFsp) || tp.Decimal > int(types.MaxFsp)) {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.Decimal, defCausName, types.MaxFsp)
		}
	}
	return nil
}

func checkDuplicateConstraint(namesMap map[string]bool, name string, foreign bool) error {
	if name == "" {
		return nil
	}
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		if foreign {
			return schemareplicant.ErrCannotAddForeign
		}
		return ErrDupKeyName.GenWithStack("duplicate key name %s", name)
	}
	namesMap[nameLower] = true
	return nil
}

func setEmptyConstraintName(namesMap map[string]bool, constr *ast.Constraint, foreign bool) {
	if constr.Name == "" && len(constr.Keys) > 0 {
		var defCausName string
		for _, keyPart := range constr.Keys {
			if keyPart.Expr != nil {
				defCausName = "expression_index"
			}
		}
		if defCausName == "" {
			defCausName = constr.Keys[0].DeferredCauset.Name.L
		}
		constrName := defCausName
		i := 2
		if strings.EqualFold(constrName, allegrosql.PrimaryKeyName) {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			// We loop forever until we find constrName that haven't been used.
			if foreign {
				constrName = fmt.Sprintf("fk_%s_%d", defCausName, i)
			} else {
				constrName = fmt.Sprintf("%s_%d", defCausName, i)
			}
			i++
		}
		constr.Name = constrName
		namesMap[constrName] = true
	}
}

func checkConstraintNames(constraints []*ast.Constraint) error {
	constrNames := map[string]bool{}
	fkNames := map[string]bool{}

	// Check not empty constraint name whether is duplicated.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			err := checkDuplicateConstraint(fkNames, constr.Name, true)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			err := checkDuplicateConstraint(constrNames, constr.Name, false)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// Set empty constraint names.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			setEmptyConstraintName(fkNames, constr, true)
		} else {
			setEmptyConstraintName(constrNames, constr, false)
		}
	}

	return nil
}

// checkInvisibleIndexOnPK check if primary key is invisible index.
// Note: PKIsHandle == true means the block already has a visible primary key,
// we do not need do a check for this case and return directly,
// because whether primary key is invisible has been check when creating block.
func checkInvisibleIndexOnPK(tblInfo *perceptron.TableInfo) error {
	if tblInfo.PKIsHandle {
		return nil
	}
	pk := getPrimaryKey(tblInfo)
	if pk != nil && pk.Invisible {
		return ErrPHoTTexCantBeInvisible
	}
	return nil
}

// getPrimaryKey extract the primary key in a block and return `IndexInfo`
// The returned primary key could be explicit or implicit.
// If there is no explicit primary key in block,
// the first UNIQUE INDEX on NOT NULL defCausumns will be the implicit primary key.
// For more information about implicit primary key, see
// https://dev.allegrosql.com/doc/refman/8.0/en/invisible-indexes.html
func getPrimaryKey(tblInfo *perceptron.TableInfo) *perceptron.IndexInfo {
	var implicitPK *perceptron.IndexInfo

	for _, key := range tblInfo.Indices {
		if key.Primary {
			// block has explicit primary key
			return key
		}
		// The case index without any defCausumns should never happen, but still do a check here
		if len(key.DeferredCausets) == 0 {
			continue
		}
		// find the first unique key with NOT NULL defCausumns
		if implicitPK == nil && key.Unique {
			// ensure all defCausumns in unique key have NOT NULL flag
			allDefCausNotNull := true
			skip := false
			for _, idxDefCaus := range key.DeferredCausets {
				defCaus := perceptron.FindDeferredCausetInfo(tblInfo.DefCauss(), idxDefCaus.Name.L)
				// This index has a defCausumn in DeleteOnly state,
				// or it is expression index (it defined on a hidden defCausumn),
				// it can not be implicit PK, go to next index iterator
				if defCaus == nil || defCaus.Hidden {
					skip = true
					break
				}
				if !allegrosql.HasNotNullFlag(defCaus.Flag) {
					allDefCausNotNull = false
					break
				}
			}
			if skip {
				continue
			}
			if allDefCausNotNull {
				implicitPK = key
			}
		}
	}
	return implicitPK
}

func setTableAutoRandomBits(ctx stochastikctx.Context, tbInfo *perceptron.TableInfo, defCausDefs []*ast.DeferredCausetDef) error {
	pkDefCausName := tbInfo.GetPkName()
	for _, defCaus := range defCausDefs {
		if containsDeferredCausetOption(defCaus, ast.DeferredCausetOptionAutoRandom) {
			if defCaus.Tp.Tp != allegrosql.TypeLonglong {
				return ErrInvalidAutoRandom.GenWithStackByArgs(
					fmt.Sprintf(autoid.AutoRandomOnNonBigIntDeferredCauset, types.TypeStr(defCaus.Tp.Tp)))
			}
			if !tbInfo.PKIsHandle || defCaus.Name.Name.L != pkDefCausName.L {
				errMsg := fmt.Sprintf(autoid.AutoRandomPKisNotHandleErrMsg, defCaus.Name.Name.O)
				return ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
			}
			if containsDeferredCausetOption(defCaus, ast.DeferredCausetOptionAutoIncrement) {
				return ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
			}
			if containsDeferredCausetOption(defCaus, ast.DeferredCausetOptionDefaultValue) {
				return ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
			}

			autoRandBits, err := extractAutoRandomBitsFromDefCausDef(defCaus)
			if err != nil {
				return errors.Trace(err)
			}

			layout := autoid.NewAutoRandomIDLayout(defCaus.Tp, autoRandBits)
			if autoRandBits == 0 {
				return ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomNonPositive)
			} else if autoRandBits > autoid.MaxAutoRandomBits {
				errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg,
					autoid.MaxAutoRandomBits, autoRandBits, defCaus.Name.Name.O)
				return ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
			}
			tbInfo.AutoRandomBits = autoRandBits

			msg := fmt.Sprintf(autoid.AutoRandomAvailableAllocTimesNote, layout.IncrementalBitsCapacity())
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(errors.Errorf(msg))
		}
	}
	return nil
}

func extractAutoRandomBitsFromDefCausDef(defCausDef *ast.DeferredCausetDef) (uint64, error) {
	for _, op := range defCausDef.Options {
		if op.Tp == ast.DeferredCausetOptionAutoRandom {
			return convertAutoRandomBitsToUnsigned(op.AutoRandomBitLength)
		}
	}
	return 0, nil
}

func convertAutoRandomBitsToUnsigned(autoRandomBits int) (uint64, error) {
	if autoRandomBits == types.UnspecifiedLength {
		return autoid.DefaultAutoRandomBits, nil
	} else if autoRandomBits < 0 {
		return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomNonPositive)
	}
	return uint64(autoRandomBits), nil
}

func buildTableInfo(
	ctx stochastikctx.Context,
	blockName perceptron.CIStr,
	defcaus []*block.DeferredCauset,
	constraints []*ast.Constraint,
	charset string,
	defCauslate string) (tbInfo *perceptron.TableInfo, err error) {
	tbInfo = &perceptron.TableInfo{
		Name:        blockName,
		Version:     perceptron.CurrLatestTableInfoVersion,
		Charset:     charset,
		DefCauslate: defCauslate,
	}
	tblDeferredCausets := make([]*block.DeferredCauset, 0, len(defcaus))
	for _, v := range defcaus {
		v.ID = allocateDeferredCausetID(tbInfo)
		tbInfo.DeferredCausets = append(tbInfo.DeferredCausets, v.ToInfo())
		tblDeferredCausets = append(tblDeferredCausets, block.ToDeferredCauset(v.ToInfo()))
	}
	for _, constr := range constraints {
		// Build hidden defCausumns if necessary.
		hiddenDefCauss, err := buildHiddenDeferredCausetInfo(ctx, constr.Keys, perceptron.NewCIStr(constr.Name), tbInfo, tblDeferredCausets)
		if err != nil {
			return nil, err
		}
		for _, hiddenDefCaus := range hiddenDefCauss {
			hiddenDefCaus.State = perceptron.StatePublic
			hiddenDefCaus.ID = allocateDeferredCausetID(tbInfo)
			hiddenDefCaus.Offset = len(tbInfo.DeferredCausets)
			tbInfo.DeferredCausets = append(tbInfo.DeferredCausets, hiddenDefCaus)
			tblDeferredCausets = append(tblDeferredCausets, block.ToDeferredCauset(hiddenDefCaus))
		}
		if constr.Tp == ast.ConstraintForeignKey {
			for _, fk := range tbInfo.ForeignKeys {
				if fk.Name.L == strings.ToLower(constr.Name) {
					return nil, schemareplicant.ErrCannotAddForeign
				}
			}
			fk, err := buildFKInfo(perceptron.NewCIStr(constr.Name), constr.Keys, constr.Refer, defcaus, tbInfo)
			if err != nil {
				return nil, err
			}
			fk.State = perceptron.StatePublic

			tbInfo.ForeignKeys = append(tbInfo.ForeignKeys, fk)
			continue
		}
		if constr.Tp == ast.ConstraintPrimaryKey {
			lastDefCaus, err := checkPKOnGeneratedDeferredCauset(tbInfo, constr.Keys)
			if err != nil {
				return nil, err
			}
			if !config.GetGlobalConfig().AlterPrimaryKey {
				singleIntPK := isSingleIntPK(constr, lastDefCaus)
				clusteredIdx := ctx.GetStochaseinstein_dbars().EnableClusteredIndex
				if singleIntPK || clusteredIdx {
					// Primary key cannot be invisible.
					if constr.Option != nil && constr.Option.Visibility == ast.IndexVisibilityInvisible {
						return nil, ErrPHoTTexCantBeInvisible
					}
				}
				if singleIntPK {
					tbInfo.PKIsHandle = true
					// Avoid creating index for PK handle defCausumn.
					continue
				}
				if clusteredIdx {
					tbInfo.IsCommonHandle = true
				}
			}
		}

		if constr.Tp == ast.ConstraintFulltext {
			sc := ctx.GetStochaseinstein_dbars().StmtCtx
			sc.AppendWarning(ErrTableCantHandleFt)
			continue
		}
		// build index info.
		idxInfo, err := buildIndexInfo(tbInfo, perceptron.NewCIStr(constr.Name), constr.Keys, perceptron.StatePublic)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(hiddenDefCauss) > 0 {
			addIndexDeferredCausetFlag(tbInfo, idxInfo)
		}
		// check if the index is primary or unique.
		switch constr.Tp {
		case ast.ConstraintPrimaryKey:
			idxInfo.Primary = true
			idxInfo.Unique = true
			idxInfo.Name = perceptron.NewCIStr(allegrosql.PrimaryKeyName)
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			idxInfo.Unique = true
		}
		// set index type.
		if constr.Option != nil {
			idxInfo.Comment, err = validateCommentLength(ctx.GetStochaseinstein_dbars(), idxInfo.Name.String(), constr.Option)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if constr.Option.Visibility == ast.IndexVisibilityInvisible {
				idxInfo.Invisible = true
			}
			if constr.Option.Tp == perceptron.IndexTypeInvalid {
				// Use btree as default index type.
				idxInfo.Tp = perceptron.IndexTypeBtree
			} else {
				idxInfo.Tp = constr.Option.Tp
			}
		} else {
			// Use btree as default index type.
			idxInfo.Tp = perceptron.IndexTypeBtree
		}
		idxInfo.ID = allocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}
	return
}

func isSingleIntPK(constr *ast.Constraint, lastDefCaus *perceptron.DeferredCausetInfo) bool {
	if len(constr.Keys) != 1 {
		return false
	}
	switch lastDefCaus.Tp {
	case allegrosql.TypeLong, allegrosql.TypeLonglong,
		allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24:
		return true
	}
	return false
}

// checkTableInfoValidExtra is like checkTableInfoValid, but also assumes the
// block info comes from untrusted source and performs further checks such as
// name length and defCausumn count.
// (checkTableInfoValid is also used in repairing objects which don't perform
// these checks. Perhaps the two functions should be merged together regardless?)
func checkTableInfoValidExtra(tbInfo *perceptron.TableInfo) error {
	if err := checkTooLongTable(tbInfo.Name); err != nil {
		return err
	}

	if err := checkDuplicateDeferredCauset(tbInfo.DeferredCausets); err != nil {
		return err
	}
	if err := checkTooLongDeferredCauset(tbInfo.DeferredCausets); err != nil {
		return err
	}
	if err := checkTooManyDeferredCausets(tbInfo.DeferredCausets); err != nil {
		return errors.Trace(err)
	}
	if err := checkDeferredCausetsAttributes(tbInfo.DeferredCausets); err != nil {
		return errors.Trace(err)
	}

	// FIXME: perform checkConstraintNames
	if err := checkCharsetAndDefCauslation(tbInfo.Charset, tbInfo.DefCauslate); err != nil {
		return errors.Trace(err)
	}

	oldState := tbInfo.State
	tbInfo.State = perceptron.StatePublic
	err := checkTableInfoValid(tbInfo)
	tbInfo.State = oldState
	return err
}

func checkTableInfoValidWithStmt(ctx stochastikctx.Context, tbInfo *perceptron.TableInfo, s *ast.CreateTableStmt) error {
	// All of these rely on the AST structure of expressions, which were
	// lost in the perceptron (got serialized into strings).
	if err := checkGeneratedDeferredCauset(s.DefCauss); err != nil {
		return errors.Trace(err)
	}
	if s.Partition != nil {
		err := checkPartitionExprValid(ctx, tbInfo, s.Partition.Expr)
		if err != nil {
			return errors.Trace(err)
		}

		pi := tbInfo.Partition
		if pi != nil {
			switch pi.Type {
			case perceptron.PartitionTypeRange:
				err = checkPartitionByRange(ctx, tbInfo, s)
			case perceptron.PartitionTypeHash:
				err = checkPartitionByHash(ctx, tbInfo, s)
			}
			if err != nil {
				return errors.Trace(err)
			}
		}

		if err = checkPartitioningKeysConstraints(ctx, s, tbInfo); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkTableInfoValid uses to check block info valid. This is used to validate block info.
func checkTableInfoValid(tblInfo *perceptron.TableInfo) error {
	_, err := blocks.TableFromMeta(nil, tblInfo)
	if err != nil {
		return err
	}
	return checkInvisibleIndexOnPK(tblInfo)
}

func buildTableInfoWithLike(ident ast.Ident, referTblInfo *perceptron.TableInfo) (*perceptron.TableInfo, error) {
	// Check the referred block is a real block object.
	if referTblInfo.IsSequence() || referTblInfo.IsView() {
		return nil, ErrWrongObject.GenWithStackByArgs(ident.Schema, referTblInfo.Name, "BASE TABLE")
	}
	tblInfo := *referTblInfo
	// Check non-public defCausumn and adjust defCausumn offset.
	newDeferredCausets := referTblInfo.DefCauss()
	newIndices := make([]*perceptron.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if idx.State == perceptron.StatePublic {
			newIndices = append(newIndices, idx)
		}
	}
	tblInfo.DeferredCausets = newDeferredCausets
	tblInfo.Indices = newIndices
	tblInfo.Name = ident.Name
	tblInfo.AutoIncID = 0
	tblInfo.ForeignKeys = nil
	if tblInfo.TiFlashReplica != nil {
		replica := *tblInfo.TiFlashReplica
		// Keep the tiflash replica setting, remove the replica available status.
		replica.AvailablePartitionIDs = nil
		replica.Available = false
		tblInfo.TiFlashReplica = &replica
	}
	if referTblInfo.Partition != nil {
		pi := *referTblInfo.Partition
		pi.Definitions = make([]perceptron.PartitionDefinition, len(referTblInfo.Partition.Definitions))
		INTERLOCKy(pi.Definitions, referTblInfo.Partition.Definitions)
		tblInfo.Partition = &pi
	}
	return &tblInfo, nil
}

// BuildTableInfoFromAST builds perceptron.TableInfo from a ALLEGROALLEGROSQL statement.
// Note: TableID and PartitionID are left as uninitialized value.
func BuildTableInfoFromAST(s *ast.CreateTableStmt) (*perceptron.TableInfo, error) {
	return buildTableInfoWithCheck(mock.NewContext(), s, allegrosql.DefaultCharset, "")
}

// buildTableInfoWithCheck builds perceptron.TableInfo from a ALLEGROALLEGROSQL statement.
// Note: TableID and PartitionIDs are left as uninitialized value.
func buildTableInfoWithCheck(ctx stochastikctx.Context, s *ast.CreateTableStmt, dbCharset, dbDefCauslate string) (*perceptron.TableInfo, error) {
	tbInfo, err := buildTableInfoWithStmt(ctx, s, dbCharset, dbDefCauslate)
	if err != nil {
		return nil, err
	}
	// Fix issue 17952 which will cause partition range expr can't be parsed as Int.
	// checkTableInfoValidWithStmt will do the constant fold the partition expression first,
	// then checkTableInfoValidExtra will pass the blockInfo check successfully.
	if err = checkTableInfoValidWithStmt(ctx, tbInfo, s); err != nil {
		return nil, err
	}
	if err = checkTableInfoValidExtra(tbInfo); err != nil {
		return nil, err
	}
	return tbInfo, nil
}

// buildTableInfoWithStmt builds perceptron.TableInfo from a ALLEGROALLEGROSQL statement without validity check
func buildTableInfoWithStmt(ctx stochastikctx.Context, s *ast.CreateTableStmt, dbCharset, dbDefCauslate string) (*perceptron.TableInfo, error) {
	defCausDefs := s.DefCauss
	blockCharset, blockDefCauslate, err := getCharsetAndDefCauslateInTableOption(0, s.Options)
	if err != nil {
		return nil, errors.Trace(err)
	}
	blockCharset, blockDefCauslate, err = ResolveCharsetDefCauslation(
		ast.CharsetOpt{Chs: blockCharset, DefCaus: blockDefCauslate},
		ast.CharsetOpt{Chs: dbCharset, DefCaus: dbDefCauslate},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// The defCausumn charset haven't been resolved here.
	defcaus, newConstraints, err := buildDeferredCausetsAndConstraints(ctx, defCausDefs, s.Constraints, blockCharset, blockDefCauslate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = checkConstraintNames(newConstraints)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tbInfo *perceptron.TableInfo
	tbInfo, err = buildTableInfo(ctx, s.Block.Name, defcaus, newConstraints, blockCharset, blockDefCauslate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = setTableAutoRandomBits(ctx, tbInfo, defCausDefs); err != nil {
		return nil, errors.Trace(err)
	}

	tbInfo.Partition, err = buildTablePartitionInfo(ctx, s)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err = handleTableOptions(s.Options, tbInfo); err != nil {
		return nil, errors.Trace(err)
	}
	return tbInfo, nil
}

func (d *dbs) assignTableID(tbInfo *perceptron.TableInfo) error {
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo.ID = genIDs[0]
	return nil
}

func (d *dbs) assignPartitionIDs(tbInfo *perceptron.TableInfo) error {
	if tbInfo.Partition == nil {
		return nil
	}
	partitionDefs := tbInfo.Partition.Definitions
	genIDs, err := d.genGlobalIDs(len(partitionDefs))
	if err != nil {
		return errors.Trace(err)
	}
	for i := range partitionDefs {
		partitionDefs[i].ID = genIDs[i]
	}
	return nil
}

func (d *dbs) CreateTable(ctx stochastikctx.Context, s *ast.CreateTableStmt) (err error) {
	ident := ast.Ident{Schema: s.Block.Schema, Name: s.Block.Name}
	is := d.GetSchemaReplicantWithInterceptor(ctx)
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	var referTbl block.Block
	if s.ReferTable != nil {
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		_, ok := is.SchemaByName(referIdent.Schema)
		if !ok {
			return schemareplicant.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		referTbl, err = is.TableByName(referIdent.Schema, referIdent.Name)
		if err != nil {
			return schemareplicant.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
	}

	// build blockInfo
	var tbInfo *perceptron.TableInfo
	if s.ReferTable != nil {
		tbInfo, err = buildTableInfoWithLike(ident, referTbl.Meta())
	} else {
		tbInfo, err = buildTableInfoWithStmt(ctx, s, schemaReplicant.Charset, schemaReplicant.DefCauslate)
	}
	if err != nil {
		return errors.Trace(err)
	}

	if err = checkTableInfoValidWithStmt(ctx, tbInfo, s); err != nil {
		return err
	}

	onExist := OnExistError
	if s.IfNotExists {
		onExist = OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, schemaReplicant.Name, tbInfo, onExist, false /*tryRetainID*/)
}

func (d *dbs) CreateTableWithInfo(
	ctx stochastikctx.Context,
	dbName perceptron.CIStr,
	tbInfo *perceptron.TableInfo,
	onExist OnExist,
	tryRetainID bool,
) (err error) {
	is := d.GetSchemaReplicantWithInterceptor(ctx)
	schemaReplicant, ok := is.SchemaByName(dbName)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	var oldViewTblID int64
	if oldTable, err := is.TableByName(schemaReplicant.Name, tbInfo.Name); err == nil {
		err = schemareplicant.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: schemaReplicant.Name, Name: tbInfo.Name})
		switch onExist {
		case OnExistIgnore:
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return nil
		case OnExistReplace:
			// only CREATE OR REPLACE VIEW is supported at the moment.
			if tbInfo.View != nil {
				if oldTable.Meta().IsView() {
					oldViewTblID = oldTable.Meta().ID
					break
				}
				// The object to replace isn't a view.
				return ErrWrongObject.GenWithStackByArgs(dbName, tbInfo.Name, "VIEW")
			}
			return err
		default:
			return err
		}
	}

	// FIXME: Implement `tryRetainID`
	if err := d.assignTableID(tbInfo); err != nil {
		return errors.Trace(err)
	}
	if err := d.assignPartitionIDs(tbInfo); err != nil {
		return errors.Trace(err)
	}

	if err := checkTableInfoValidExtra(tbInfo); err != nil {
		return err
	}

	var actionType perceptron.CausetActionType
	args := []interface{}{tbInfo}
	switch {
	case tbInfo.View != nil:
		actionType = perceptron.CausetActionCreateView
		args = append(args, onExist == OnExistReplace, oldViewTblID)
	case tbInfo.Sequence != nil:
		actionType = perceptron.CausetActionCreateSequence
	default:
		actionType = perceptron.CausetActionCreateTable
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tbInfo.ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       actionType,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       args,
	}

	err = d.doDBSJob(ctx, job)
	if err != nil {
		// block exists, but if_not_exists flags is true, so we ignore this error.
		if onExist == OnExistIgnore && schemareplicant.ErrTableExists.Equal(err) {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			err = nil
		}
	} else if actionType == perceptron.CausetActionCreateTable {
		d.preSplitAndScatter(ctx, tbInfo, tbInfo.GetPartitionInfo())
		if tbInfo.AutoIncID > 1 {
			// Default blockAutoIncID base is 0.
			// If the first ID is expected to greater than 1, we need to do rebase.
			newEnd := tbInfo.AutoIncID - 1
			if err = d.handleAutoIncID(tbInfo, schemaReplicant.ID, newEnd, autoid.RowIDAllocType); err != nil {
				return errors.Trace(err)
			}
		}
		if tbInfo.AutoRandID > 1 {
			// Default blockAutoRandID base is 0.
			// If the first ID is expected to greater than 1, we need to do rebase.
			newEnd := tbInfo.AutoRandID - 1
			err = d.handleAutoIncID(tbInfo, schemaReplicant.ID, newEnd, autoid.AutoRandomType)
		}
	}

	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// preSplitAndScatter performs pre-split and scatter of the block's regions.
// If `pi` is not nil, will only split region for `pi`, this is used when add partition.
func (d *dbs) preSplitAndScatter(ctx stochastikctx.Context, tbInfo *perceptron.TableInfo, pi *perceptron.PartitionInfo) {
	sp, ok := d.causetstore.(ekv.SplitblockStore)
	if !ok || atomic.LoadUint32(&EnableSplitTableRegion) == 0 {
		return
	}
	var (
		preSplit      func()
		scatterRegion bool
	)
	val, err := variable.GetGlobalSystemVar(ctx.GetStochaseinstein_dbars(), variable.MilevaDBScatterRegion)
	if err != nil {
		logutil.BgLogger().Warn("[dbs] won't scatter region", zap.Error(err))
	} else {
		scatterRegion = variable.MilevaDBOptOn(val)
	}
	if pi != nil {
		preSplit = func() { splitPartitionTableRegion(ctx, sp, tbInfo, pi, scatterRegion) }
	} else {
		preSplit = func() { splitTableRegion(ctx, sp, tbInfo, scatterRegion) }
	}
	if scatterRegion {
		preSplit()
	} else {
		go preSplit()
	}
}

func (d *dbs) RecoverTable(ctx stochastikctx.Context, recoverInfo *RecoverInfo) (err error) {
	is := d.GetSchemaReplicantWithInterceptor(ctx)
	schemaID, tbInfo := recoverInfo.SchemaID, recoverInfo.TableInfo
	// Check schemaReplicant exist.
	schemaReplicant, ok := is.SchemaByID(schemaID)
	if !ok {
		return errors.Trace(schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
		))
	}
	// Check not exist block with same name.
	if ok := is.TableExists(schemaReplicant.Name, tbInfo.Name); ok {
		return schemareplicant.ErrTableExists.GenWithStackByArgs(tbInfo.Name)
	}

	tbInfo.State = perceptron.StateNone
	job := &perceptron.Job{
		SchemaID:   schemaID,
		TableID:    tbInfo.ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionRecoverTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args: []interface{}{tbInfo, recoverInfo.CurAutoIncID, recoverInfo.DropJobID,
			recoverInfo.SnapshotTS, recoverTableCheckFlagNone, recoverInfo.CurAutoRandID},
	}
	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) CreateView(ctx stochastikctx.Context, s *ast.CreateViewStmt) (err error) {
	viewInfo, err := buildViewInfo(ctx, s)
	if err != nil {
		return err
	}

	defcaus := make([]*block.DeferredCauset, len(s.DefCauss))
	for i, v := range s.DefCauss {
		defcaus[i] = block.ToDeferredCauset(&perceptron.DeferredCausetInfo{
			Name:   v,
			ID:     int64(i),
			Offset: i,
			State:  perceptron.StatePublic,
		})
	}

	tblCharset := ""
	tblDefCauslate := ""
	if v, ok := ctx.GetStochaseinstein_dbars().GetSystemVar("character_set_client"); ok {
		tblCharset = v
	}
	if v, ok := ctx.GetStochaseinstein_dbars().GetSystemVar("defCauslation_connection"); ok {
		tblDefCauslate = v
	}

	tbInfo, err := buildTableInfo(ctx, s.ViewName.Name, defcaus, nil, tblCharset, tblDefCauslate)
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	onExist := OnExistError
	if s.OrReplace {
		onExist = OnExistReplace
	}

	return d.CreateTableWithInfo(ctx, s.ViewName.Schema, tbInfo, onExist, false /*tryRetainID*/)
}

func buildViewInfo(ctx stochastikctx.Context, s *ast.CreateViewStmt) (*perceptron.ViewInfo, error) {
	// Always Use `format.RestoreNameBackQuotes` to restore `SELECT` statement despite the `ANSI_QUOTES` ALLEGROALLEGROSQL Mode is enabled or not.
	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var sb strings.Builder
	if err := s.Select.Restore(format.NewRestoreCtx(restoreFlag, &sb)); err != nil {
		return nil, err
	}

	return &perceptron.ViewInfo{Definer: s.Definer, Algorithm: s.Algorithm,
		Security: s.Security, SelectStmt: sb.String(), CheckOption: s.CheckOption, DefCauss: nil}, nil
}

func checkPartitionByHash(ctx stochastikctx.Context, tbInfo *perceptron.TableInfo, s *ast.CreateTableStmt) error {
	pi := tbInfo.Partition
	if err := checkAddPartitionTooManyPartitions(pi.Num); err != nil {
		return err
	}
	if err := checkNoHashPartitions(ctx, pi.Num); err != nil {
		return err
	}
	if err := checkPartitionFuncValid(ctx, tbInfo, s.Partition.Expr); err != nil {
		return err
	}
	return checkPartitionFuncType(ctx, s, tbInfo)
}

// checkPartitionByRange checks validity of a "BY RANGE" partition.
func checkPartitionByRange(ctx stochastikctx.Context, tbInfo *perceptron.TableInfo, s *ast.CreateTableStmt) error {
	pi := tbInfo.Partition
	if err := checkPartitionNameUnique(pi); err != nil {
		return err
	}

	if err := checkAddPartitionTooManyPartitions(uint64(len(pi.Definitions))); err != nil {
		return err
	}

	if err := checkNoRangePartitions(len(pi.Definitions)); err != nil {
		return err
	}

	if len(pi.DeferredCausets) == 0 {
		if err := checkCreatePartitionValue(ctx, tbInfo); err != nil {
			return err
		}

		// s maybe nil when add partition.
		if s == nil {
			return nil
		}

		if err := checkPartitionFuncValid(ctx, tbInfo, s.Partition.Expr); err != nil {
			return err
		}
		return checkPartitionFuncType(ctx, s, tbInfo)
	}

	// Check for range defCausumns partition.
	if err := checkRangeDeferredCausetsPartitionType(tbInfo); err != nil {
		return err
	}

	if s != nil {
		for _, def := range s.Partition.Definitions {
			exprs := def.Clause.(*ast.PartitionDefinitionClauseLessThan).Exprs
			if err := checkRangeDeferredCausetsTypeAndValuesMatch(ctx, tbInfo, exprs); err != nil {
				return err
			}
		}
	}

	return checkRangeDeferredCausetsPartitionValue(ctx, tbInfo)
}

func checkRangeDeferredCausetsPartitionType(tbInfo *perceptron.TableInfo) error {
	for _, defCaus := range tbInfo.Partition.DeferredCausets {
		defCausInfo := getDeferredCausetInfoByName(tbInfo, defCaus.L)
		if defCausInfo == nil {
			return errors.Trace(ErrFieldNotFoundPart)
		}
		// The permitted data types are shown in the following list:
		// All integer types
		// DATE and DATETIME
		// CHAR, VARCHAR, BINARY, and VARBINARY
		// See https://dev.allegrosql.com/doc/allegrosql-partitioning-excerpt/5.7/en/partitioning-defCausumns.html
		switch defCausInfo.FieldType.Tp {
		case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong:
		case allegrosql.TypeDate, allegrosql.TypeDatetime:
		case allegrosql.TypeVarchar, allegrosql.TypeString:
		default:
			return ErrNotAllowedTypeInPartition.GenWithStackByArgs(defCaus.O)
		}
	}
	return nil
}

func checkRangeDeferredCausetsPartitionValue(ctx stochastikctx.Context, tbInfo *perceptron.TableInfo) error {
	// Range defCausumns partition key supports multiple data types with integer、datetime、string.
	pi := tbInfo.Partition
	defs := pi.Definitions
	if len(defs) < 1 {
		return ast.ErrPartitionsMustBeDefined.GenWithStackByArgs("RANGE")
	}

	curr := &defs[0]
	if len(curr.LessThan) != len(pi.DeferredCausets) {
		return errors.Trace(ast.ErrPartitionDeferredCausetList)
	}
	var prev *perceptron.PartitionDefinition
	for i := 1; i < len(defs); i++ {
		prev, curr = curr, &defs[i]
		succ, err := checkTwoRangeDeferredCausets(ctx, curr, prev, pi, tbInfo)
		if err != nil {
			return err
		}
		if !succ {
			return errors.Trace(ErrRangeNotIncreasing)
		}
	}
	return nil
}

func checkTwoRangeDeferredCausets(ctx stochastikctx.Context, curr, prev *perceptron.PartitionDefinition, pi *perceptron.PartitionInfo, tbInfo *perceptron.TableInfo) (bool, error) {
	if len(curr.LessThan) != len(pi.DeferredCausets) {
		return false, errors.Trace(ast.ErrPartitionDeferredCausetList)
	}
	for i := 0; i < len(pi.DeferredCausets); i++ {
		// Special handling for MAXVALUE.
		if strings.EqualFold(curr.LessThan[i], partitionMaxValue) {
			// If current is maxvalue, it certainly >= previous.
			return true, nil
		}
		if strings.EqualFold(prev.LessThan[i], partitionMaxValue) {
			// Current is not maxvalue, and previous is maxvalue.
			return false, nil
		}

		// Current and previous is the same.
		if strings.EqualFold(curr.LessThan[i], prev.LessThan[i]) {
			continue
		}

		// The tuples of defCausumn values used to define the partitions are strictly increasing:
		// PARTITION p0 VALUES LESS THAN (5,10,'ggg')
		// PARTITION p1 VALUES LESS THAN (10,20,'mmm')
		// PARTITION p2 VALUES LESS THAN (15,30,'sss')
		succ, err := parseAndEvalBoolExpr(ctx, fmt.Sprintf("(%s) > (%s)", curr.LessThan[i], prev.LessThan[i]), tbInfo)
		if err != nil {
			return false, err
		}

		if succ {
			return true, nil
		}
	}
	return false, nil
}

func parseAndEvalBoolExpr(ctx stochastikctx.Context, expr string, tbInfo *perceptron.TableInfo) (bool, error) {
	e, err := expression.ParseSimpleExprWithTableInfo(ctx, expr, tbInfo)
	if err != nil {
		return false, err
	}
	res, _, err1 := e.EvalInt(ctx, chunk.Row{})
	if err1 != nil {
		return false, err1
	}
	return res > 0, nil
}

func checkCharsetAndDefCauslation(cs string, co string) error {
	if !charset.ValidCharsetAndDefCauslation(cs, co) {
		return ErrUnknownCharacterSet.GenWithStackByArgs(cs)
	}
	if co != "" {
		if _, err := defCauslate.GetDefCauslationByName(co); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// handleAutoIncID handles auto_increment option in DBS. It creates a ID counter for the block and initiates the counter to a proper value.
// For example if the option sets auto_increment to 10. The counter will be set to 9. So the next allocated ID will be 10.
func (d *dbs) handleAutoIncID(tbInfo *perceptron.TableInfo, schemaID int64, newEnd int64, tp autoid.SlabPredictorType) error {
	allocs := autoid.NewSlabPredictorsFromTblInfo(d.causetstore, schemaID, tbInfo)
	if alloc := allocs.Get(tp); alloc != nil {
		err := alloc.Rebase(tbInfo.ID, newEnd, false)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// handleTableOptions uFIDelates blockInfo according to block options.
func handleTableOptions(options []*ast.TableOption, tbInfo *perceptron.TableInfo) error {
	for _, op := range options {
		switch op.Tp {
		case ast.TableOptionAutoIncrement:
			tbInfo.AutoIncID = int64(op.UintValue)
		case ast.TableOptionAutoIdCache:
			if op.UintValue > uint64(math.MaxInt64) {
				// TODO: Refine this error.
				return errors.New("block option auto_id_cache overflows int64")
			}
			tbInfo.AutoIdCache = int64(op.UintValue)
		case ast.TableOptionAutoRandomBase:
			tbInfo.AutoRandID = int64(op.UintValue)
		case ast.TableOptionComment:
			tbInfo.Comment = op.StrValue
		case ast.TableOptionCompression:
			tbInfo.Compression = op.StrValue
		case ast.TableOptionShardRowID:
			if op.UintValue > 0 && tbInfo.PKIsHandle {
				return errUnsupportedShardRowIDBits
			}
			tbInfo.ShardRowIDBits = op.UintValue
			if tbInfo.ShardRowIDBits > shardRowIDBitsMax {
				tbInfo.ShardRowIDBits = shardRowIDBitsMax
			}
			tbInfo.MaxShardRowIDBits = tbInfo.ShardRowIDBits
		case ast.TableOptionPreSplitRegion:
			tbInfo.PreSplitRegions = op.UintValue
		case ast.TableOptionCharset, ast.TableOptionDefCauslate:
			// We don't handle charset and defCauslate here since they're handled in `getCharsetAndDefCauslateInTableOption`.
		}
	}
	if tbInfo.PreSplitRegions > tbInfo.ShardRowIDBits {
		tbInfo.PreSplitRegions = tbInfo.ShardRowIDBits
	}
	return nil
}

// isIgnorableSpec checks if the spec type is ignorable.
// Some specs are parsed by ignored. This is for compatibility.
func isIgnorableSpec(tp ast.AlterTableType) bool {
	// AlterTableLock/AlterTableAlgorithm are ignored.
	return tp == ast.AlterTableLock || tp == ast.AlterTableAlgorithm
}

// getCharsetAndDefCauslateInDeferredCausetDef will iterate defCauslate in the options, validate it by checking the charset
// of defCausumn definition. If there's no defCauslate in the option, the default defCauslate of defCausumn's charset will be used.
func getCharsetAndDefCauslateInDeferredCausetDef(def *ast.DeferredCausetDef) (chs, defCausl string, err error) {
	chs = def.Tp.Charset
	defCausl = def.Tp.DefCauslate
	if chs != "" && defCausl == "" {
		if defCausl, err = charset.GetDefaultDefCauslation(chs); err != nil {
			return "", "", errors.Trace(err)
		}
	}
	for _, opt := range def.Options {
		if opt.Tp == ast.DeferredCausetOptionDefCauslate {
			info, err := defCauslate.GetDefCauslationByName(opt.StrValue)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			if chs == "" {
				chs = info.CharsetName
			} else if chs != info.CharsetName {
				return "", "", ErrDefCauslationCharsetMismatch.GenWithStackByArgs(info.Name, chs)
			}
			defCausl = info.Name
		}
	}
	return
}

// getCharsetAndDefCauslateInTableOption will iterate the charset and defCauslate in the options,
// and returns the last charset and defCauslate in options. If there is no charset in the options,
// the returns charset will be "", the same as defCauslate.
func getCharsetAndDefCauslateInTableOption(startIdx int, options []*ast.TableOption) (chs, defCausl string, err error) {
	for i := startIdx; i < len(options); i++ {
		opt := options[i]
		// we set the charset to the last option. example: alter block t charset latin1 charset utf8 defCauslate utf8_bin;
		// the charset will be utf8, defCauslate will be utf8_bin
		switch opt.Tp {
		case ast.TableOptionCharset:
			info, err := charset.GetCharsetDesc(opt.StrValue)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = info.Name
			} else if chs != info.Name {
				return "", "", ErrConflictingDeclarations.GenWithStackByArgs(chs, info.Name)
			}
			if len(defCausl) == 0 {
				defCausl = info.DefaultDefCauslation
			}
		case ast.TableOptionDefCauslate:
			info, err := defCauslate.GetDefCauslationByName(opt.StrValue)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = info.CharsetName
			} else if chs != info.CharsetName {
				return "", "", ErrDefCauslationCharsetMismatch.GenWithStackByArgs(info.Name, chs)
			}
			defCausl = info.Name
		}
	}
	return
}

func needToOverwriteDefCausCharset(options []*ast.TableOption) bool {
	for i := len(options) - 1; i >= 0; i-- {
		opt := options[i]
		switch opt.Tp {
		case ast.TableOptionCharset:
			// Only overwrite defCausumns charset if the option contains `CONVERT TO`.
			return opt.UintValue == ast.TableOptionCharsetWithConvertTo
		}
	}
	return false
}

// resolveAlterTableSpec resolves alter block algorithm and removes ignore block spec in specs.
// returns valied specs, and the occurred error.
func resolveAlterTableSpec(ctx stochastikctx.Context, specs []*ast.AlterTableSpec) ([]*ast.AlterTableSpec, error) {
	validSpecs := make([]*ast.AlterTableSpec, 0, len(specs))
	algorithm := ast.AlgorithmTypeDefault
	for _, spec := range specs {
		if spec.Tp == ast.AlterTableAlgorithm {
			// Find the last AlterTableAlgorithm.
			algorithm = spec.Algorithm
		}
		if isIgnorableSpec(spec.Tp) {
			continue
		}
		validSpecs = append(validSpecs, spec)
	}

	// Verify whether the algorithm is supported.
	for _, spec := range validSpecs {
		resolvedAlgorithm, err := ResolveAlterAlgorithm(spec, algorithm)
		if err != nil {
			// If MilevaDB failed to choose a better algorithm, report the error
			if resolvedAlgorithm == ast.AlgorithmTypeDefault {
				return nil, errors.Trace(err)
			}
			// For the compatibility, we return warning instead of error when a better algorithm is chosed by MilevaDB
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendError(err)
		}

		spec.Algorithm = resolvedAlgorithm
	}

	// Only handle valid specs.
	return validSpecs, nil
}

func isSameTypeMultiSpecs(specs []*ast.AlterTableSpec) bool {
	specType := specs[0].Tp
	for _, spec := range specs {
		if spec.Tp != specType {
			return false
		}
	}
	return true
}

func (d *dbs) AlterTable(ctx stochastikctx.Context, ident ast.Ident, specs []*ast.AlterTableSpec) (err error) {
	validSpecs, err := resolveAlterTableSpec(ctx, specs)
	if err != nil {
		return errors.Trace(err)
	}

	is := d.infoHandle.Get()
	if is.TableIsView(ident.Schema, ident.Name) || is.TableIsSequence(ident.Schema, ident.Name) {
		return ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "BASE TABLE")
	}

	if len(validSpecs) > 1 {
		if isSameTypeMultiSpecs(validSpecs) {
			switch validSpecs[0].Tp {
			case ast.AlterTableAddDeferredCausets:
				err = d.AddDeferredCausets(ctx, ident, validSpecs)
			case ast.AlterTableDropDeferredCauset:
				err = d.DropDeferredCausets(ctx, ident, validSpecs)
			default:
				return errRunMultiSchemaChanges
			}
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		}
		return errRunMultiSchemaChanges
	}

	for _, spec := range validSpecs {
		var handledCharsetOrDefCauslate bool
		switch spec.Tp {
		case ast.AlterTableAddDeferredCausets:
			if len(spec.NewDeferredCausets) != 1 {
				err = d.AddDeferredCausets(ctx, ident, []*ast.AlterTableSpec{spec})
			} else {
				err = d.AddDeferredCauset(ctx, ident, spec)
			}
		case ast.AlterTableAddPartitions:
			err = d.AddTablePartitions(ctx, ident, spec)
		case ast.AlterTableCoalescePartitions:
			err = d.CoalescePartitions(ctx, ident, spec)
		case ast.AlterTableReorganizePartition:
			err = errors.Trace(errUnsupportedReorganizePartition)
		case ast.AlterTableCheckPartitions:
			err = errors.Trace(errUnsupportedCheckPartition)
		case ast.AlterTableRebuildPartition:
			err = errors.Trace(errUnsupportedRebuildPartition)
		case ast.AlterTableOptimizePartition:
			err = errors.Trace(errUnsupportedOptimizePartition)
		case ast.AlterTableRemovePartitioning:
			err = errors.Trace(errUnsupportedRemovePartition)
		case ast.AlterTableRepairPartition:
			err = errors.Trace(errUnsupportedRepairPartition)
		case ast.AlterTableDropDeferredCauset:
			err = d.DropDeferredCauset(ctx, ident, spec)
		case ast.AlterTableDropIndex:
			err = d.DropIndex(ctx, ident, perceptron.NewCIStr(spec.Name), spec.IfExists)
		case ast.AlterTableDropPrimaryKey:
			err = d.DropIndex(ctx, ident, perceptron.NewCIStr(allegrosql.PrimaryKeyName), spec.IfExists)
		case ast.AlterTableRenameIndex:
			err = d.RenameIndex(ctx, ident, spec)
		case ast.AlterTableDropPartition:
			err = d.DropTablePartition(ctx, ident, spec)
		case ast.AlterTableTruncatePartition:
			err = d.TruncateTablePartition(ctx, ident, spec)
		case ast.AlterTableExchangePartition:
			err = d.ExchangeTablePartition(ctx, ident, spec)
		case ast.AlterTableAddConstraint:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				err = d.CreateIndex(ctx, ident, ast.IndexKeyTypeNone, perceptron.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, constr.IfNotExists)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				err = d.CreateIndex(ctx, ident, ast.IndexKeyTypeUnique, perceptron.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, false) // IfNotExists should be not applied
			case ast.ConstraintForeignKey:
				// NOTE: we do not handle `symbol` and `index_name` well in the berolinaAllegroSQL and we do not check ForeignKey already exists,
				// so we just also ignore the `if not exists` check.
				err = d.CreateForeignKey(ctx, ident, perceptron.NewCIStr(constr.Name), spec.Constraint.Keys, spec.Constraint.Refer)
			case ast.ConstraintPrimaryKey:
				err = d.CreatePrimaryKey(ctx, ident, perceptron.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintFulltext:
				ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(ErrTableCantHandleFt)
			case ast.ConstraintCheck:
				ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(ErrUnsupportedConstraintCheck.GenWithStackByArgs("ADD CONSTRAINT CHECK"))
			default:
				// Nothing to do now.
			}
		case ast.AlterTableDropForeignKey:
			// NOTE: we do not check `if not exists` and `if exists` for ForeignKey now.
			err = d.DropForeignKey(ctx, ident, perceptron.NewCIStr(spec.Name))
		case ast.AlterTableModifyDeferredCauset:
			err = d.ModifyDeferredCauset(ctx, ident, spec)
		case ast.AlterTableChangeDeferredCauset:
			err = d.ChangeDeferredCauset(ctx, ident, spec)
		case ast.AlterTableRenameDeferredCauset:
			err = d.RenameDeferredCauset(ctx, ident, spec)
		case ast.AlterTableAlterDeferredCauset:
			err = d.AlterDeferredCauset(ctx, ident, spec)
		case ast.AlterTableRenameTable:
			newIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
			isAlterTable := true
			err = d.RenameTable(ctx, ident, newIdent, isAlterTable)
		case ast.AlterTableAlterPartition:
			err = d.AlterTablePartition(ctx, ident, spec)
		case ast.AlterTablePartition:
			// Prevent silent succeed if user executes ALTER TABLE x PARTITION BY ...
			err = errors.New("alter block partition is unsupported")
		case ast.AlterTableOption:
			for i, opt := range spec.Options {
				switch opt.Tp {
				case ast.TableOptionShardRowID:
					if opt.UintValue > shardRowIDBitsMax {
						opt.UintValue = shardRowIDBitsMax
					}
					err = d.ShardRowID(ctx, ident, opt.UintValue)
				case ast.TableOptionAutoIncrement:
					err = d.RebaseAutoID(ctx, ident, int64(opt.UintValue), autoid.RowIDAllocType)
				case ast.TableOptionAutoIdCache:
					if opt.UintValue > uint64(math.MaxInt64) {
						// TODO: Refine this error.
						return errors.New("block option auto_id_cache overflows int64")
					}
					err = d.AlterTableAutoIDCache(ctx, ident, int64(opt.UintValue))
				case ast.TableOptionAutoRandomBase:
					err = d.RebaseAutoID(ctx, ident, int64(opt.UintValue), autoid.AutoRandomType)
				case ast.TableOptionComment:
					spec.Comment = opt.StrValue
					err = d.AlterTableComment(ctx, ident, spec)
				case ast.TableOptionCharset, ast.TableOptionDefCauslate:
					// getCharsetAndDefCauslateInTableOption will get the last charset and defCauslate in the options,
					// so it should be handled only once.
					if handledCharsetOrDefCauslate {
						continue
					}
					var toCharset, toDefCauslate string
					toCharset, toDefCauslate, err = getCharsetAndDefCauslateInTableOption(i, spec.Options)
					if err != nil {
						return err
					}
					needsOverwriteDefCauss := needToOverwriteDefCausCharset(spec.Options)
					err = d.AlterTableCharsetAndDefCauslate(ctx, ident, toCharset, toDefCauslate, needsOverwriteDefCauss)
					handledCharsetOrDefCauslate = true
				}

				if err != nil {
					return errors.Trace(err)
				}
			}
		case ast.AlterTableSetTiFlashReplica:
			err = d.AlterTableSetTiFlashReplica(ctx, ident, spec.TiFlashReplica)
		case ast.AlterTableOrderByDeferredCausets:
			err = d.OrderByDeferredCausets(ctx, ident)
		case ast.AlterTableIndexInvisible:
			err = d.AlterIndexVisibility(ctx, ident, spec.IndexName, spec.Visibility)
		case ast.AlterTableAlterCheck:
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(ErrUnsupportedConstraintCheck.GenWithStackByArgs("ALTER CHECK"))
		case ast.AlterTableDropCheck:
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(ErrUnsupportedConstraintCheck.GenWithStackByArgs("DROP CHECK"))
		case ast.AlterTableWithValidation:
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(errUnsupportedAlterTableWithValidation)
		case ast.AlterTableWithoutValidation:
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(errUnsupportedAlterTableWithoutValidation)
		default:
			// Nothing to do now.
		}

		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *dbs) RebaseAutoID(ctx stochastikctx.Context, ident ast.Ident, newBase int64, tp autoid.SlabPredictorType) error {
	schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	var actionType perceptron.CausetActionType
	switch tp {
	case autoid.AutoRandomType:
		tbInfo := t.Meta()
		if tbInfo.AutoRandomBits == 0 {
			return errors.Trace(ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomRebaseNotApplicable))
		}
		var autoRandDefCausTp types.FieldType
		for _, c := range tbInfo.DeferredCausets {
			if allegrosql.HasPriKeyFlag(c.Flag) {
				autoRandDefCausTp = c.FieldType
				break
			}
		}
		layout := autoid.NewAutoRandomIDLayout(&autoRandDefCausTp, tbInfo.AutoRandomBits)
		if layout.IncrementalMask()&newBase != newBase {
			errMsg := fmt.Sprintf(autoid.AutoRandomRebaseOverflow, newBase, layout.IncrementalBitsCapacity())
			return errors.Trace(ErrInvalidAutoRandom.GenWithStackByArgs(errMsg))
		}
		actionType = perceptron.CausetActionRebaseAutoRandomBase
	case autoid.RowIDAllocType:
		actionType = perceptron.CausetActionRebaseAutoID
	}

	if alloc := t.SlabPredictors(ctx).Get(tp); alloc != nil {
		autoID, err := alloc.NextGlobalAutoID(t.Meta().ID)
		if err != nil {
			return errors.Trace(err)
		}
		// If newBase < autoID, we need to do a rebase before returning.
		// Assume there are 2 MilevaDB servers: MilevaDB-A with allocator range of 0 ~ 30000; MilevaDB-B with allocator range of 30001 ~ 60000.
		// If the user sends ALLEGROALLEGROSQL `alter block t1 auto_increment = 100` to MilevaDB-B,
		// and MilevaDB-B finds 100 < 30001 but returns without any handling,
		// then MilevaDB-A may still allocate 99 for auto_increment defCausumn. This doesn't make sense for the user.
		newBase = int64(mathutil.MaxUint64(uint64(newBase), uint64(autoID)))
	}
	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       actionType,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{newBase},
	}
	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// ShardRowID shards the implicit event ID by adding shard value to the event ID's first few bits.
func (d *dbs) ShardRowID(ctx stochastikctx.Context, blockIdent ast.Ident, uVal uint64) error {
	schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, blockIdent)
	if err != nil {
		return errors.Trace(err)
	}
	if uVal == t.Meta().ShardRowIDBits {
		// Nothing need to do.
		return nil
	}
	if uVal > 0 && t.Meta().PKIsHandle {
		return errUnsupportedShardRowIDBits
	}
	err = verifyNoOverflowShardBits(d.sessPool, t, uVal)
	if err != nil {
		return err
	}
	job := &perceptron.Job{
		Type:       perceptron.CausetActionShardRowID,
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{uVal},
	}
	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) getSchemaAndTableByIdent(ctx stochastikctx.Context, blockIdent ast.Ident) (dbInfo *perceptron.DBInfo, t block.Block, err error) {
	is := d.GetSchemaReplicantWithInterceptor(ctx)
	schemaReplicant, ok := is.SchemaByName(blockIdent.Schema)
	if !ok {
		return nil, nil, schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(blockIdent.Schema)
	}
	t, err = is.TableByName(blockIdent.Schema, blockIdent.Name)
	if err != nil {
		return nil, nil, schemareplicant.ErrTableNotExists.GenWithStackByArgs(blockIdent.Schema, blockIdent.Name)
	}
	return schemaReplicant, t, nil
}

func checkUnsupportedDeferredCausetConstraint(defCaus *ast.DeferredCausetDef, ti ast.Ident) error {
	for _, constraint := range defCaus.Options {
		switch constraint.Tp {
		case ast.DeferredCausetOptionAutoIncrement:
			return errUnsupportedAddDeferredCauset.GenWithStack("unsupported add defCausumn '%s' constraint AUTO_INCREMENT when altering '%s.%s'", defCaus.Name, ti.Schema, ti.Name)
		case ast.DeferredCausetOptionPrimaryKey:
			return errUnsupportedAddDeferredCauset.GenWithStack("unsupported add defCausumn '%s' constraint PRIMARY KEY when altering '%s.%s'", defCaus.Name, ti.Schema, ti.Name)
		case ast.DeferredCausetOptionUniqKey:
			return errUnsupportedAddDeferredCauset.GenWithStack("unsupported add defCausumn '%s' constraint UNIQUE KEY when altering '%s.%s'", defCaus.Name, ti.Schema, ti.Name)
		case ast.DeferredCausetOptionAutoRandom:
			errMsg := fmt.Sprintf(autoid.AutoRandomAlterAddDeferredCauset, defCaus.Name, ti.Schema, ti.Name)
			return ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
		}
	}

	return nil
}

func checkAndCreateNewDeferredCauset(ctx stochastikctx.Context, ti ast.Ident, schemaReplicant *perceptron.DBInfo, spec *ast.AlterTableSpec, t block.Block, specNewDeferredCauset *ast.DeferredCausetDef) (*block.DeferredCauset, error) {
	err := checkUnsupportedDeferredCausetConstraint(specNewDeferredCauset, ti)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defCausName := specNewDeferredCauset.Name.Name.O
	// Check whether added defCausumn has existed.
	defCaus := block.FindDefCaus(t.DefCauss(), defCausName)
	if defCaus != nil {
		err = schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(defCausName)
		if spec.IfNotExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return nil, nil
		}
		return nil, err
	}
	if err = checkDeferredCausetAttributes(defCausName, specNewDeferredCauset.Tp); err != nil {
		return nil, errors.Trace(err)
	}
	if len(defCausName) > allegrosql.MaxDeferredCausetNameLength {
		return nil, ErrTooLongIdent.GenWithStackByArgs(defCausName)
	}

	// If new defCausumn is a generated defCausumn, do validation.
	// NOTE: we do check whether the defCausumn refers other generated
	// defCausumns occurring later in a block, but we don't handle the defCaus offset.
	for _, option := range specNewDeferredCauset.Options {
		if option.Tp == ast.DeferredCausetOptionGenerated {
			if err := checkIllegalFn4GeneratedDeferredCauset(specNewDeferredCauset.Name.Name.L, option.Expr); err != nil {
				return nil, errors.Trace(err)
			}

			if option.Stored {
				return nil, ErrUnsupportedOnGeneratedDeferredCauset.GenWithStackByArgs("Adding generated stored defCausumn through ALTER TABLE")
			}

			_, dependDefCausNames := findDependedDeferredCausetNames(specNewDeferredCauset)
			if err = checkAutoIncrementRef(specNewDeferredCauset.Name.Name.L, dependDefCausNames, t.Meta()); err != nil {
				return nil, errors.Trace(err)
			}
			duplicateDefCausNames := make(map[string]struct{}, len(dependDefCausNames))
			for k := range dependDefCausNames {
				duplicateDefCausNames[k] = struct{}{}
			}
			defcaus := t.DefCauss()

			if err = checkDependedDefCausExist(dependDefCausNames, defcaus); err != nil {
				return nil, errors.Trace(err)
			}

			if err = verifyDeferredCausetGenerationSingle(duplicateDefCausNames, defcaus, spec.Position); err != nil {
				return nil, errors.Trace(err)
			}
		}
		// Specially, since sequence has been supported, if a newly added defCausumn has a
		// sequence nextval function as it's default value option, it won't fill the
		// known rows with specific sequence next value under current add defCausumn logic.
		// More explanation can refer: TestSequenceDefaultLogic's comment in sequence_test.go
		if option.Tp == ast.DeferredCausetOptionDefaultValue {
			_, isSeqExpr, err := tryToGetSequenceDefaultValue(option)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if isSeqExpr {
				return nil, errors.Trace(ErrAddDeferredCausetWithSequenceAsDefault.GenWithStackByArgs(specNewDeferredCauset.Name.Name.O))
			}
		}
	}

	blockCharset, blockDefCauslate, err := ResolveCharsetDefCauslation(
		ast.CharsetOpt{Chs: t.Meta().Charset, DefCaus: t.Meta().DefCauslate},
		ast.CharsetOpt{Chs: schemaReplicant.Charset, DefCaus: schemaReplicant.DefCauslate},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Ignore block constraints now, they will be checked later.
	// We use length(t.DefCauss()) as the default offset firstly, we will change the defCausumn's offset later.
	defCaus, _, err = buildDeferredCausetAndConstraint(
		ctx,
		len(t.DefCauss()),
		specNewDeferredCauset,
		nil,
		blockCharset,
		blockDefCauslate,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defCaus.OriginDefaultValue, err = generateOriginDefaultValue(defCaus.ToInfo())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return defCaus, nil
}

// AddDeferredCauset will add a new defCausumn to the block.
func (d *dbs) AddDeferredCauset(ctx stochastikctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	specNewDeferredCauset := spec.NewDeferredCausets[0]
	schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	if err = checkAddDeferredCausetTooManyDeferredCausets(len(t.DefCauss()) + 1); err != nil {
		return errors.Trace(err)
	}
	defCaus, err := checkAndCreateNewDeferredCauset(ctx, ti, schemaReplicant, spec, t, specNewDeferredCauset)
	if err != nil {
		return errors.Trace(err)
	}
	// Added defCausumn has existed and if_not_exists flag is true.
	if defCaus == nil {
		return nil
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionAddDeferredCauset,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defCaus, spec.Position, 0},
	}

	err = d.doDBSJob(ctx, job)
	// defCausumn exists, but if_not_exists flags is true, so we ignore this error.
	if schemareplicant.ErrDeferredCausetExists.Equal(err) && spec.IfNotExists {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AddDeferredCausets will add multi new defCausumns to the block.
func (d *dbs) AddDeferredCausets(ctx stochastikctx.Context, ti ast.Ident, specs []*ast.AlterTableSpec) error {
	schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	// Check all the defCausumns at once.
	addingDeferredCausetNames := make(map[string]bool)
	dupDeferredCausetNames := make(map[string]bool)
	for _, spec := range specs {
		for _, specNewDeferredCauset := range spec.NewDeferredCausets {
			if !addingDeferredCausetNames[specNewDeferredCauset.Name.Name.L] {
				addingDeferredCausetNames[specNewDeferredCauset.Name.Name.L] = true
				continue
			}
			if !spec.IfNotExists {
				return errors.Trace(schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(specNewDeferredCauset.Name.Name.O))
			}
			dupDeferredCausetNames[specNewDeferredCauset.Name.Name.L] = true
		}
	}
	defCausumns := make([]*block.DeferredCauset, 0, len(addingDeferredCausetNames))
	positions := make([]*ast.DeferredCausetPosition, 0, len(addingDeferredCausetNames))
	offsets := make([]int, 0, len(addingDeferredCausetNames))
	ifNotExists := make([]bool, 0, len(addingDeferredCausetNames))
	newDeferredCausetsCount := 0
	// Check the defCausumns one by one.
	for _, spec := range specs {
		for _, specNewDeferredCauset := range spec.NewDeferredCausets {
			if spec.IfNotExists && dupDeferredCausetNames[specNewDeferredCauset.Name.Name.L] {
				err = schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(specNewDeferredCauset.Name.Name.O)
				ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
				continue
			}
			defCaus, err := checkAndCreateNewDeferredCauset(ctx, ti, schemaReplicant, spec, t, specNewDeferredCauset)
			if err != nil {
				return errors.Trace(err)
			}
			// Added defCausumn has existed and if_not_exists flag is true.
			if defCaus == nil && spec.IfNotExists {
				continue
			}
			defCausumns = append(defCausumns, defCaus)
			positions = append(positions, spec.Position)
			offsets = append(offsets, 0)
			ifNotExists = append(ifNotExists, spec.IfNotExists)
			newDeferredCausetsCount++
		}
	}
	if newDeferredCausetsCount == 0 {
		return nil
	}
	if err = checkAddDeferredCausetTooManyDeferredCausets(len(t.DefCauss()) + newDeferredCausetsCount); err != nil {
		return errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionAddDeferredCausets,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defCausumns, positions, offsets, ifNotExists},
	}

	err = d.doDBSJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AddTablePartitions will add a new partition to the block.
func (d *dbs) AddTablePartitions(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(schemaReplicant))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	partInfo, err := buildPartitionInfo(ctx, meta, d, spec)
	if err != nil {
		return errors.Trace(err)
	}

	// partInfo contains only the new added partition, we have to combine it with the
	// old partitions to check all partitions is strictly increasing.
	tmp := *partInfo
	tmp.Definitions = append(pi.Definitions, tmp.Definitions...)
	meta.Partition = &tmp
	err = checkPartitionByRange(ctx, meta, nil)
	meta.Partition = pi
	if err != nil {
		if ErrSameNamePartition.Equal(err) && spec.IfNotExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    meta.ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionAddTablePartition,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{partInfo},
	}

	err = d.doDBSJob(ctx, job)
	if ErrSameNamePartition.Equal(err) && spec.IfNotExists {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
		return nil
	}
	if err == nil {
		d.preSplitAndScatter(ctx, meta, partInfo)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// CoalescePartitions coalesce partitions can be used with a block that is partitioned by hash or key to reduce the number of partitions by number.
func (d *dbs) CoalescePartitions(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(schemaReplicant))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	switch meta.Partition.Type {
	// We don't support coalesce partitions hash type partition now.
	case perceptron.PartitionTypeHash:
		return errors.Trace(ErrUnsupportedCoalescePartition)

	// Key type partition cannot be constructed currently, ignoring it for now.
	case perceptron.PartitionTypeKey:

	// Coalesce partition can only be used on hash/key partitions.
	default:
		return errors.Trace(ErrCoalesceOnlyOnHashPartition)
	}

	return errors.Trace(err)
}

func (d *dbs) TruncateTablePartition(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(schemaReplicant))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	pids := make([]int64, len(spec.PartitionNames))
	for i, name := range spec.PartitionNames {
		pid, err := blocks.FindPartitionByName(meta, name.L)
		if err != nil {
			return errors.Trace(err)
		}
		pids[i] = pid
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    meta.ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionTruncateTablePartition,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{pids},
	}

	err = d.doDBSJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) DropTablePartition(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(schemaReplicant))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	partNames := make([]string, len(spec.PartitionNames))
	for i, partCIName := range spec.PartitionNames {
		partNames[i] = partCIName.L
	}
	err = checkDropTablePartition(meta, partNames)
	if err != nil {
		if ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    meta.ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionDropTablePartition,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{partNames},
	}

	err = d.doDBSJob(ctx, job)
	if err != nil {
		if ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkFieldTypeCompatible(ft *types.FieldType, other *types.FieldType) bool {
	// int(1) could match the type with int(8)
	partialEqual := ft.Tp == other.Tp &&
		ft.Decimal == other.Decimal &&
		ft.Charset == other.Charset &&
		ft.DefCauslate == other.DefCauslate &&
		(ft.Flen == other.Flen || ft.StorageLength() != types.VarStorageLen) &&
		allegrosql.HasUnsignedFlag(ft.Flag) == allegrosql.HasUnsignedFlag(other.Flag) &&
		allegrosql.HasAutoIncrementFlag(ft.Flag) == allegrosql.HasAutoIncrementFlag(other.Flag) &&
		allegrosql.HasNotNullFlag(ft.Flag) == allegrosql.HasNotNullFlag(other.Flag) &&
		allegrosql.HasZerofillFlag(ft.Flag) == allegrosql.HasZerofillFlag(other.Flag) &&
		allegrosql.HasBinaryFlag(ft.Flag) == allegrosql.HasBinaryFlag(other.Flag) &&
		allegrosql.HasPriKeyFlag(ft.Flag) == allegrosql.HasPriKeyFlag(other.Flag)
	if !partialEqual || len(ft.Elems) != len(other.Elems) {
		return false
	}
	for i := range ft.Elems {
		if ft.Elems[i] != other.Elems[i] {
			return false
		}
	}
	return true
}

func checkTiFlashReplicaCompatible(source *perceptron.TiFlashReplicaInfo, target *perceptron.TiFlashReplicaInfo) bool {
	if source == target {
		return true
	}
	if source == nil || target == nil {
		return false
	}
	if source.Count != target.Count ||
		source.Available != target.Available || len(source.LocationLabels) != len(target.LocationLabels) {
		return false
	}
	for i, lable := range source.LocationLabels {
		if target.LocationLabels[i] != lable {
			return false
		}
	}
	return true
}

func checkTableDefCompatible(source *perceptron.TableInfo, target *perceptron.TableInfo) error {
	// check auto_random
	if source.AutoRandomBits != target.AutoRandomBits ||
		source.Charset != target.Charset ||
		source.DefCauslate != target.DefCauslate ||
		source.ShardRowIDBits != target.ShardRowIDBits ||
		source.MaxShardRowIDBits != target.MaxShardRowIDBits ||
		!checkTiFlashReplicaCompatible(source.TiFlashReplica, target.TiFlashReplica) {
		return errors.Trace(ErrTablesDifferentMetadata)
	}
	if len(source.DefCauss()) != len(target.DefCauss()) {
		return errors.Trace(ErrTablesDifferentMetadata)
	}
	// DefCaus compatible check
	for i, sourceDefCaus := range source.DefCauss() {
		targetDefCaus := target.DefCauss()[i]
		if isVirtualGeneratedDeferredCauset(sourceDefCaus) != isVirtualGeneratedDeferredCauset(targetDefCaus) {
			return ErrUnsupportedOnGeneratedDeferredCauset.GenWithStackByArgs("Exchanging partitions for non-generated defCausumns")
		}
		// It should strictyle compare expressions for generated defCausumns
		if sourceDefCaus.Name.L != targetDefCaus.Name.L ||
			sourceDefCaus.Hidden != targetDefCaus.Hidden ||
			!checkFieldTypeCompatible(&sourceDefCaus.FieldType, &targetDefCaus.FieldType) ||
			sourceDefCaus.GeneratedExprString != targetDefCaus.GeneratedExprString {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		if sourceDefCaus.State != perceptron.StatePublic ||
			targetDefCaus.State != perceptron.StatePublic {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		if sourceDefCaus.ID != targetDefCaus.ID {
			return ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("defCausumn: %s", sourceDefCaus.Name))
		}
	}
	if len(source.Indices) != len(target.Indices) {
		return errors.Trace(ErrTablesDifferentMetadata)
	}
	for _, sourceIdx := range source.Indices {
		var compatIdx *perceptron.IndexInfo
		for _, targetIdx := range target.Indices {
			if strings.EqualFold(sourceIdx.Name.L, targetIdx.Name.L) {
				compatIdx = targetIdx
			}
		}
		// No match index
		if compatIdx == nil {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		// Index type is not compatible
		if sourceIdx.Tp != compatIdx.Tp ||
			sourceIdx.Unique != compatIdx.Unique ||
			sourceIdx.Primary != compatIdx.Primary {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		// The index defCausumn
		if len(sourceIdx.DeferredCausets) != len(compatIdx.DeferredCausets) {
			return errors.Trace(ErrTablesDifferentMetadata)
		}
		for i, sourceIdxDefCaus := range sourceIdx.DeferredCausets {
			compatIdxDefCaus := compatIdx.DeferredCausets[i]
			if sourceIdxDefCaus.Length != compatIdxDefCaus.Length ||
				sourceIdxDefCaus.Name.L != compatIdxDefCaus.Name.L {
				return errors.Trace(ErrTablesDifferentMetadata)
			}
		}
		if sourceIdx.ID != compatIdx.ID {
			return ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("index: %s", sourceIdx.Name))
		}
	}

	return nil
}

func checkExchangePartition(pt *perceptron.TableInfo, nt *perceptron.TableInfo) error {
	if nt.IsView() || nt.IsSequence() {
		return errors.Trace(ErrCheckNoSuchTable)
	}
	if pt.GetPartitionInfo() == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}
	if nt.GetPartitionInfo() != nil {
		return errors.Trace(ErrPartitionExchangePartTable.GenWithStackByArgs(nt.Name))
	}

	if nt.ForeignKeys != nil {
		return errors.Trace(ErrPartitionExchangeForeignKey.GenWithStackByArgs(nt.Name))
	}

	// NOTE: if nt is temporary block, it should be checked
	return nil
}

func (d *dbs) ExchangeTablePartition(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	ptSchema, pt, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	ptMeta := pt.Meta()

	ntIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
	ntSchema, nt, err := d.getSchemaAndTableByIdent(ctx, ntIdent)
	if err != nil {
		return errors.Trace(err)
	}

	ntMeta := nt.Meta()

	err = checkExchangePartition(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	partName := spec.PartitionNames[0].L

	// NOTE: if pt is subPartitioned, it should be checked

	defID, err := blocks.FindPartitionByName(ptMeta, partName)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkTableDefCompatible(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   ntSchema.ID,
		TableID:    ntMeta.ID,
		SchemaName: ntSchema.Name.L,
		Type:       perceptron.CausetActionExchangeTablePartition,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defID, ptSchema.ID, ptMeta.ID, partName, spec.WithValidation},
	}

	err = d.doDBSJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropDeferredCauset will drop a defCausumn from the block, now we don't support drop the defCausumn with index covered.
func (d *dbs) DropDeferredCauset(ctx stochastikctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	isDropable, err := checkIsDroppableDeferredCauset(ctx, t, spec)
	if err != nil {
		return err
	}
	if !isDropable {
		return nil
	}
	defCausName := spec.OldDeferredCausetName.Name

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionDropDeferredCauset,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defCausName},
	}

	err = d.doDBSJob(ctx, job)
	// defCausumn not exists, but if_exists flags is true, so we ignore this error.
	if ErrCantDropFieldOrKey.Equal(err) && spec.IfExists {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropDeferredCausets will drop multi-defCausumns from the block, now we don't support drop the defCausumn with index covered.
func (d *dbs) DropDeferredCausets(ctx stochastikctx.Context, ti ast.Ident, specs []*ast.AlterTableSpec) error {
	schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo := t.Meta()

	dropingDeferredCausetNames := make(map[string]bool)
	dupDeferredCausetNames := make(map[string]bool)
	for _, spec := range specs {
		if !dropingDeferredCausetNames[spec.OldDeferredCausetName.Name.L] {
			dropingDeferredCausetNames[spec.OldDeferredCausetName.Name.L] = true
		} else {
			if spec.IfExists {
				dupDeferredCausetNames[spec.OldDeferredCausetName.Name.L] = true
				continue
			}
			return errors.Trace(ErrCantDropFieldOrKey.GenWithStack("defCausumn %s doesn't exist", spec.OldDeferredCausetName.Name.O))
		}
	}

	ifExists := make([]bool, 0, len(specs))
	defCausNames := make([]perceptron.CIStr, 0, len(specs))
	for _, spec := range specs {
		if spec.IfExists && dupDeferredCausetNames[spec.OldDeferredCausetName.Name.L] {
			err = ErrCantDropFieldOrKey.GenWithStack("defCausumn %s doesn't exist", spec.OldDeferredCausetName.Name.L)
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			continue
		}
		isDropable, err := checkIsDroppableDeferredCauset(ctx, t, spec)
		if err != nil {
			return err
		}
		// DeferredCauset can't drop and if_exists flag is true.
		if !isDropable && spec.IfExists {
			continue
		}
		defCausNames = append(defCausNames, spec.OldDeferredCausetName.Name)
		ifExists = append(ifExists, spec.IfExists)
	}
	if len(defCausNames) == 0 {
		return nil
	}
	if len(tblInfo.DeferredCausets) == len(defCausNames) {
		return ErrCantRemoveAllFields.GenWithStack("can't drop all defCausumns in block %s",
			tblInfo.Name)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionDropDeferredCausets,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defCausNames, ifExists},
	}

	err = d.doDBSJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkIsDroppableDeferredCauset(ctx stochastikctx.Context, t block.Block, spec *ast.AlterTableSpec) (isDrapable bool, err error) {
	tblInfo := t.Meta()
	// Check whether dropped defCausumn has existed.
	defCausName := spec.OldDeferredCausetName.Name
	defCaus := block.FindDefCaus(t.VisibleDefCauss(), defCausName.L)
	if defCaus == nil {
		err = ErrCantDropFieldOrKey.GenWithStack("defCausumn %s doesn't exist", defCausName)
		if spec.IfExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return false, nil
		}
		return false, err
	}

	if err = isDroppableDeferredCauset(tblInfo, defCausName); err != nil {
		return false, errors.Trace(err)
	}
	// We don't support dropping defCausumn with PK handle covered now.
	if defCaus.IsPKHandleDeferredCauset(tblInfo) {
		return false, errUnsupportedPKHandle
	}
	return true, nil
}

// checkModifyCharsetAndDefCauslation returns error when the charset or defCauslation is not modifiable.
// needRewriteDefCauslationData is used when trying to modify the defCauslation of a defCausumn, it is true when the defCausumn is with
// index because index of a string defCausumn is defCauslation-aware.
func checkModifyCharsetAndDefCauslation(toCharset, toDefCauslate, origCharset, origDefCauslate string, needRewriteDefCauslationData bool) error {
	if !charset.ValidCharsetAndDefCauslation(toCharset, toDefCauslate) {
		return ErrUnknownCharacterSet.GenWithStack("Unknown character set: '%s', defCauslation: '%s'", toCharset, toDefCauslate)
	}

	if needRewriteDefCauslationData && defCauslate.NewDefCauslationEnabled() && !defCauslate.CompatibleDefCauslate(origDefCauslate, toDefCauslate) {
		return errUnsupportedModifyDefCauslation.GenWithStackByArgs(origDefCauslate, toDefCauslate)
	}

	if (origCharset == charset.CharsetUTF8 && toCharset == charset.CharsetUTF8MB4) ||
		(origCharset == charset.CharsetUTF8 && toCharset == charset.CharsetUTF8) ||
		(origCharset == charset.CharsetUTF8MB4 && toCharset == charset.CharsetUTF8MB4) {
		// MilevaDB only allow utf8 to be changed to utf8mb4, or changing the defCauslation when the charset is utf8/utf8mb4.
		return nil
	}

	if toCharset != origCharset {
		msg := fmt.Sprintf("charset from %s to %s", origCharset, toCharset)
		return errUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	if toDefCauslate != origDefCauslate {
		msg := fmt.Sprintf("change defCauslate from %s to %s", origDefCauslate, toDefCauslate)
		return errUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	return nil
}

// CheckModifyTypeCompatible checks whether changes defCausumn type to another is compatible considering
// field length and precision.
func CheckModifyTypeCompatible(origin *types.FieldType, to *types.FieldType) (allowedChangeDeferredCausetValueMsg string, err error) {
	unsupportedMsg := fmt.Sprintf("type %v not match origin %v", to.CompactStr(), origin.CompactStr())
	var isIntType bool
	switch origin.Tp {
	case allegrosql.TypeVarchar, allegrosql.TypeString, allegrosql.TypeVarString,
		allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		switch to.Tp {
		case allegrosql.TypeVarchar, allegrosql.TypeString, allegrosql.TypeVarString,
			allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		default:
			return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(unsupportedMsg)
		}
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong:
		switch to.Tp {
		case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong:
			isIntType = true
		default:
			return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(unsupportedMsg)
		}
	case allegrosql.TypeEnum, allegrosql.TypeSet:
		var typeVar string
		if origin.Tp == allegrosql.TypeEnum {
			typeVar = "enum"
		} else {
			typeVar = "set"
		}
		if origin.Tp != to.Tp {
			msg := fmt.Sprintf("cannot modify %s type defCausumn's to type %s", typeVar, to.String())
			return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
		}
		if len(to.Elems) < len(origin.Elems) {
			msg := fmt.Sprintf("the number of %s defCausumn's elements is less than the original: %d", typeVar, len(origin.Elems))
			return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
		}
		for index, originElem := range origin.Elems {
			toElem := to.Elems[index]
			if originElem != toElem {
				msg := fmt.Sprintf("cannot modify %s defCausumn value %s to %s", typeVar, originElem, toElem)
				return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
			}
		}
	case allegrosql.TypeNewDecimal:
		if origin.Tp != to.Tp {
			return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(unsupportedMsg)
		}
		// The root cause is modifying decimal precision needs to rewrite binary representation of that decimal.
		if to.Flen != origin.Flen || to.Decimal != origin.Decimal {
			return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs("can't change decimal defCausumn precision")
		}
	default:
		if origin.Tp != to.Tp {
			return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(unsupportedMsg)
		}
	}

	if to.Flen > 0 && to.Flen < origin.Flen {
		msg := fmt.Sprintf("length %d is less than origin %d", to.Flen, origin.Flen)
		if isIntType {
			return msg, errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
		}
		return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
	}
	if to.Decimal > 0 && to.Decimal < origin.Decimal {
		msg := fmt.Sprintf("decimal %d is less than origin %d", to.Decimal, origin.Decimal)
		return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
	}

	toUnsigned := allegrosql.HasUnsignedFlag(to.Flag)
	originUnsigned := allegrosql.HasUnsignedFlag(origin.Flag)
	if originUnsigned != toUnsigned {
		msg := fmt.Sprintf("can't change unsigned integer to signed or vice versa")
		if isIntType {
			return msg, errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
		}
		return "", errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
	}
	return "", nil
}

// checkModifyTypes checks if the 'origin' type can be modified to 'to' type with out the need to
// change or check existing data in the block.
// It returns error if the two types has incompatible Charset and DefCauslation, different sign, different
// digital/string types, or length of new Flen and Decimal is less than origin.
func checkModifyTypes(ctx stochastikctx.Context, origin *types.FieldType, to *types.FieldType, needRewriteDefCauslationData bool) error {
	changeDeferredCausetValueMsg, err := CheckModifyTypeCompatible(origin, to)
	if err != nil {
		enableChangeDeferredCausetType := ctx.GetStochaseinstein_dbars().EnableChangeDeferredCausetType
		if len(changeDeferredCausetValueMsg) == 0 {
			return errors.Trace(err)
		}

		if !enableChangeDeferredCausetType {
			msg := fmt.Sprintf("%s, and milevadb_enable_change_defCausumn_type is false", changeDeferredCausetValueMsg)
			return errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
		} else if allegrosql.HasPriKeyFlag(origin.Flag) {
			msg := "milevadb_enable_change_defCausumn_type is true and this defCausumn has primary key flag"
			return errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
		}
	}

	err = checkModifyCharsetAndDefCauslation(to.Charset, to.DefCauslate, origin.Charset, origin.DefCauslate, needRewriteDefCauslationData)
	return errors.Trace(err)
}

func setDefaultValue(ctx stochastikctx.Context, defCaus *block.DeferredCauset, option *ast.DeferredCausetOption) (bool, error) {
	hasDefaultValue := false
	value, isSeqExpr, err := getDefaultValue(ctx, defCaus, option)
	if err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	if isSeqExpr {
		if err := checkSequenceDefaultValue(defCaus); err != nil {
			return false, errors.Trace(err)
		}
		defCaus.DefaultIsExpr = isSeqExpr
	}

	if hasDefaultValue, value, err = checkDeferredCausetDefaultValue(ctx, defCaus, value); err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	value, err = convertTimestamFIDelefaultValToUTC(ctx, value, defCaus)
	if err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	err = defCaus.SetDefaultValue(value)
	if err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	return hasDefaultValue, nil
}

func setDeferredCausetComment(ctx stochastikctx.Context, defCaus *block.DeferredCauset, option *ast.DeferredCausetOption) error {
	value, err := expression.EvalAstExpr(ctx, option.Expr)
	if err != nil {
		return errors.Trace(err)
	}
	defCaus.Comment, err = value.ToString()
	return errors.Trace(err)
}

// processDeferredCausetOptions is only used in getModifiableDeferredCausetJob.
func processDeferredCausetOptions(ctx stochastikctx.Context, defCaus *block.DeferredCauset, options []*ast.DeferredCausetOption) error {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	var hasDefaultValue, setOnUFIDelateNow bool
	var err error
	for _, opt := range options {
		switch opt.Tp {
		case ast.DeferredCausetOptionDefaultValue:
			hasDefaultValue, err = setDefaultValue(ctx, defCaus, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.DeferredCausetOptionComment:
			err := setDeferredCausetComment(ctx, defCaus, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.DeferredCausetOptionNotNull:
			defCaus.Flag |= allegrosql.NotNullFlag
		case ast.DeferredCausetOptionNull:
			defCaus.Flag &= ^allegrosql.NotNullFlag
		case ast.DeferredCausetOptionAutoIncrement:
			defCaus.Flag |= allegrosql.AutoIncrementFlag
		case ast.DeferredCausetOptionPrimaryKey, ast.DeferredCausetOptionUniqKey:
			return errUnsupportedModifyDeferredCauset.GenWithStack("can't change defCausumn constraint - %v", opt.Tp)
		case ast.DeferredCausetOptionOnUFIDelate:
			// TODO: Support other time functions.
			if defCaus.Tp == allegrosql.TypeTimestamp || defCaus.Tp == allegrosql.TypeDatetime {
				if !expression.IsValidCurrentTimestampExpr(opt.Expr, &defCaus.FieldType) {
					return ErrInvalidOnUFIDelate.GenWithStackByArgs(defCaus.Name)
				}
			} else {
				return ErrInvalidOnUFIDelate.GenWithStackByArgs(defCaus.Name)
			}
			defCaus.Flag |= allegrosql.OnUFIDelateNowFlag
			setOnUFIDelateNow = true
		case ast.DeferredCausetOptionGenerated:
			sb.Reset()
			err = opt.Expr.Restore(restoreCtx)
			if err != nil {
				return errors.Trace(err)
			}
			defCaus.GeneratedExprString = sb.String()
			defCaus.GeneratedStored = opt.Stored
			defCaus.Dependences = make(map[string]struct{})
			defCaus.GeneratedExpr = opt.Expr
			for _, defCausName := range findDeferredCausetNamesInExpr(opt.Expr) {
				defCaus.Dependences[defCausName.Name.L] = struct{}{}
			}
		case ast.DeferredCausetOptionDefCauslate:
			defCaus.DefCauslate = opt.StrValue
		case ast.DeferredCausetOptionReference:
			return errors.Trace(errUnsupportedModifyDeferredCauset.GenWithStackByArgs("can't modify with references"))
		case ast.DeferredCausetOptionFulltext:
			return errors.Trace(errUnsupportedModifyDeferredCauset.GenWithStackByArgs("can't modify with full text"))
		case ast.DeferredCausetOptionCheck:
			return errors.Trace(errUnsupportedModifyDeferredCauset.GenWithStackByArgs("can't modify with check"))
		// Ignore DeferredCausetOptionAutoRandom. It will be handled later.
		case ast.DeferredCausetOptionAutoRandom:
		default:
			return errors.Trace(errUnsupportedModifyDeferredCauset.GenWithStackByArgs(fmt.Sprintf("unknown defCausumn option type: %d", opt.Tp)))
		}
	}

	processDefaultValue(defCaus, hasDefaultValue, setOnUFIDelateNow)

	processDeferredCausetFlags(defCaus)

	if hasDefaultValue {
		return errors.Trace(checkDefaultValue(ctx, defCaus, true))
	}

	return nil
}

func (d *dbs) getModifiableDeferredCausetJob(ctx stochastikctx.Context, ident ast.Ident, originalDefCausName perceptron.CIStr,
	spec *ast.AlterTableSpec) (*perceptron.Job, error) {
	specNewDeferredCauset := spec.NewDeferredCausets[0]
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return nil, errors.Trace(schemareplicant.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return nil, errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	defCaus := block.FindDefCaus(t.DefCauss(), originalDefCausName.L)
	if defCaus == nil {
		return nil, schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(originalDefCausName, ident.Name)
	}
	newDefCausName := specNewDeferredCauset.Name.Name
	if newDefCausName.L == perceptron.ExtraHandleName.L {
		return nil, ErrWrongDeferredCausetName.GenWithStackByArgs(newDefCausName.L)
	}
	// If we want to rename the defCausumn name, we need to check whether it already exists.
	if newDefCausName.L != originalDefCausName.L {
		c := block.FindDefCaus(t.DefCauss(), newDefCausName.L)
		if c != nil {
			return nil, schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(newDefCausName)
		}
	}
	// Check the defCausumn with foreign key.
	if fkInfo := getDeferredCausetForeignKeyInfo(originalDefCausName.L, t.Meta().ForeignKeys); fkInfo != nil {
		return nil, errFKIncompatibleDeferredCausets.GenWithStackByArgs(originalDefCausName, fkInfo.Name)
	}

	// Constraints in the new defCausumn means adding new constraints. Errors should thrown,
	// which will be done by `processDeferredCausetOptions` later.
	if specNewDeferredCauset.Tp == nil {
		// Make sure the defCausumn definition is simple field type.
		return nil, errors.Trace(errUnsupportedModifyDeferredCauset)
	}

	if err = checkDeferredCausetAttributes(specNewDeferredCauset.Name.OrigDefCausName(), specNewDeferredCauset.Tp); err != nil {
		return nil, errors.Trace(err)
	}

	newDefCaus := block.ToDeferredCauset(&perceptron.DeferredCausetInfo{
		ID: defCaus.ID,
		// We use this PR(https://github.com/whtcorpsinc/MilevaDB-Prod/pull/6274) as the dividing line to define whether it is a new version or an old version MilevaDB.
		// The old version MilevaDB initializes the defCausumn's offset and state here.
		// The new version MilevaDB doesn't initialize the defCausumn's offset and state, and it will do the initialization in run DBS function.
		// When we do the rolling upgrade the following may happen:
		// a new version MilevaDB builds the DBS job that doesn't be set the defCausumn's offset and state,
		// and the old version MilevaDB is the DBS owner, it doesn't get offset and state from the causetstore. Then it will encounter errors.
		// So here we set offset and state to support the rolling upgrade.
		Offset:             defCaus.Offset,
		State:              defCaus.State,
		OriginDefaultValue: defCaus.OriginDefaultValue,
		FieldType:          *specNewDeferredCauset.Tp,
		Name:               newDefCausName,
		Version:            defCaus.Version,
	})

	var chs, defCausl string
	// TODO: Remove it when all block versions are greater than or equal to TableInfoVersion1.
	// If newDefCaus's charset is empty and the block's version less than TableInfoVersion1,
	// we will not modify the charset of the defCausumn. This behavior is not compatible with MyALLEGROSQL.
	if len(newDefCaus.FieldType.Charset) == 0 && t.Meta().Version < perceptron.TableInfoVersion1 {
		chs = defCaus.FieldType.Charset
		defCausl = defCaus.FieldType.DefCauslate
	} else {
		chs, defCausl, err = getCharsetAndDefCauslateInDeferredCausetDef(specNewDeferredCauset)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chs, defCausl, err = ResolveCharsetDefCauslation(
			ast.CharsetOpt{Chs: chs, DefCaus: defCausl},
			ast.CharsetOpt{Chs: t.Meta().Charset, DefCaus: t.Meta().DefCauslate},
			ast.CharsetOpt{Chs: schemaReplicant.Charset, DefCaus: schemaReplicant.DefCauslate},
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if err = setCharsetDefCauslationFlenDecimal(&newDefCaus.FieldType, chs, defCausl); err != nil {
		return nil, errors.Trace(err)
	}

	if err = processDeferredCausetOptions(ctx, newDefCaus, specNewDeferredCauset.Options); err != nil {
		return nil, errors.Trace(err)
	}

	if err = checkDeferredCausetValueConstraint(newDefCaus, newDefCaus.DefCauslate); err != nil {
		return nil, errors.Trace(err)
	}

	if err = checkModifyTypes(ctx, &defCaus.FieldType, &newDefCaus.FieldType, isDeferredCausetWithIndex(defCaus.Name.L, t.Meta().Indices)); err != nil {
		if strings.Contains(err.Error(), "Unsupported modifying defCauslation") {
			defCausErrMsg := "Unsupported modifying defCauslation of defCausumn '%s' from '%s' to '%s' when index is defined on it."
			err = errUnsupportedModifyDefCauslation.GenWithStack(defCausErrMsg, defCaus.Name.L, defCaus.DefCauslate, newDefCaus.DefCauslate)
		}
		return nil, errors.Trace(err)
	}
	if ctx.GetStochaseinstein_dbars().EnableChangeDeferredCausetType && needChangeDeferredCausetData(defCaus.DeferredCausetInfo, newDefCaus.DeferredCausetInfo) {
		if newDefCaus.IsGenerated() || defCaus.IsGenerated() {
			// TODO: Make it compatible with MyALLEGROSQL error.
			msg := fmt.Sprintf("milevadb_enable_change_defCausumn_type is true, newDefCaus IsGenerated %v, oldDefCaus IsGenerated %v", newDefCaus.IsGenerated(), defCaus.IsGenerated())
			return nil, errUnsupportedModifyDeferredCauset.GenWithStackByArgs(msg)
		} else if t.Meta().Partition != nil {
			return nil, errUnsupportedModifyDeferredCauset.GenWithStackByArgs("milevadb_enable_change_defCausumn_type is true, block is partition block")
		}
	}

	// INTERLOCKy index related options to the new spec.
	indexFlags := defCaus.FieldType.Flag & (allegrosql.PriKeyFlag | allegrosql.UniqueKeyFlag | allegrosql.MultipleKeyFlag)
	newDefCaus.FieldType.Flag |= indexFlags
	if allegrosql.HasPriKeyFlag(defCaus.FieldType.Flag) {
		newDefCaus.FieldType.Flag |= allegrosql.NotNullFlag
		// TODO: If user explicitly set NULL, we should throw error ErrPrimaryCantHaveNull.
	}

	// We don't support modifying defCausumn from not_auto_increment to auto_increment.
	if !allegrosql.HasAutoIncrementFlag(defCaus.Flag) && allegrosql.HasAutoIncrementFlag(newDefCaus.Flag) {
		return nil, errUnsupportedModifyDeferredCauset.GenWithStackByArgs("can't set auto_increment")
	}
	// Disallow modifying defCausumn from auto_increment to not auto_increment if the stochastik variable `AllowRemoveAutoInc` is false.
	if !ctx.GetStochaseinstein_dbars().AllowRemoveAutoInc && allegrosql.HasAutoIncrementFlag(defCaus.Flag) && !allegrosql.HasAutoIncrementFlag(newDefCaus.Flag) {
		return nil, errUnsupportedModifyDeferredCauset.GenWithStackByArgs("can't remove auto_increment without @@milevadb_allow_remove_auto_inc enabled")
	}

	// We support modifying the type definitions of 'null' to 'not null' now.
	var modifyDeferredCausetTp byte
	if !allegrosql.HasNotNullFlag(defCaus.Flag) && allegrosql.HasNotNullFlag(newDefCaus.Flag) {
		if err = checkForNullValue(ctx, defCaus.Tp != newDefCaus.Tp, ident.Schema, ident.Name, newDefCaus.Name, defCaus.DeferredCausetInfo); err != nil {
			return nil, errors.Trace(err)
		}
		// `modifyDeferredCausetTp` indicates that there is a type modification.
		modifyDeferredCausetTp = allegrosql.TypeNull
	}

	if err = checkDeferredCausetFieldLength(newDefCaus); err != nil {
		return nil, err
	}

	if err = checkDeferredCausetWithIndexConstraint(t.Meta(), defCaus.DeferredCausetInfo, newDefCaus.DeferredCausetInfo); err != nil {
		return nil, err
	}

	// As same with MyALLEGROSQL, we don't support modifying the stored status for generated defCausumns.
	if err = checkModifyGeneratedDeferredCauset(t, defCaus, newDefCaus, specNewDeferredCauset); err != nil {
		return nil, errors.Trace(err)
	}

	var newAutoRandBits uint64
	if newAutoRandBits, err = checkAutoRandom(t.Meta(), defCaus, specNewDeferredCauset); err != nil {
		return nil, errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionModifyDeferredCauset,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{&newDefCaus, originalDefCausName, spec.Position, modifyDeferredCausetTp, newAutoRandBits},
	}
	return job, nil
}

// checkDeferredCausetWithIndexConstraint is used to check the related index constraint of the modified defCausumn.
// Index has a max-prefix-length constraint. eg: a varchar(100), index idx(a), modifying defCausumn a to a varchar(4000)
// will cause index idx to break the max-prefix-length constraint.
func checkDeferredCausetWithIndexConstraint(tbInfo *perceptron.TableInfo, originalDefCaus, newDefCaus *perceptron.DeferredCausetInfo) error {
	var defCausumns []*perceptron.DeferredCausetInfo
	for _, indexInfo := range tbInfo.Indices {
		containDeferredCauset := false
		for _, defCaus := range indexInfo.DeferredCausets {
			if defCaus.Name.L == originalDefCaus.Name.L {
				containDeferredCauset = true
				break
			}
		}
		if !containDeferredCauset {
			continue
		}
		if defCausumns == nil {
			defCausumns = make([]*perceptron.DeferredCausetInfo, 0, len(tbInfo.DeferredCausets))
			defCausumns = append(defCausumns, tbInfo.DeferredCausets...)
			// replace old defCausumn with new defCausumn.
			for i, defCaus := range defCausumns {
				if defCaus.Name.L != originalDefCaus.Name.L {
					continue
				}
				defCausumns[i] = newDefCaus.Clone()
				defCausumns[i].Name = originalDefCaus.Name
				break
			}
		}
		err := checHoTTexPrefixLength(defCausumns, indexInfo.DeferredCausets)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkAutoRandom(blockInfo *perceptron.TableInfo, originDefCaus *block.DeferredCauset, specNewDeferredCauset *ast.DeferredCausetDef) (uint64, error) {
	// Disallow add/drop actions on auto_random.
	var oldRandBits uint64
	if blockInfo.PKIsHandle && (blockInfo.GetPkName().L == originDefCaus.Name.L) {
		oldRandBits = blockInfo.AutoRandomBits
	}
	newRandBits, err := extractAutoRandomBitsFromDefCausDef(specNewDeferredCauset)
	if err != nil {
		return 0, errors.Trace(err)
	}
	switch {
	case oldRandBits == newRandBits:
		break
	case oldRandBits == 0 || newRandBits == 0:
		return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterErrMsg)
	case autoid.MaxAutoRandomBits < newRandBits:
		errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg,
			autoid.MaxAutoRandomBits, newRandBits, specNewDeferredCauset.Name.Name.O)
		return 0, ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
	case oldRandBits < newRandBits:
		break // Increasing auto_random shard bits is allowed.
	case oldRandBits > newRandBits:
		return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomDecreaseBitErrMsg)
	}

	if oldRandBits != 0 {
		// Disallow changing the defCausumn field type.
		if originDefCaus.Tp != specNewDeferredCauset.Tp.Tp {
			return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomModifyDefCausTypeErrMsg)
		}
		// Disallow changing auto_increment on auto_random defCausumn.
		if containsDeferredCausetOption(specNewDeferredCauset, ast.DeferredCausetOptionAutoIncrement) != allegrosql.HasAutoIncrementFlag(originDefCaus.Flag) {
			return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
		}
		// Disallow specifying a default value on auto_random defCausumn.
		if containsDeferredCausetOption(specNewDeferredCauset, ast.DeferredCausetOptionDefaultValue) {
			return 0, ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
	}
	return newRandBits, nil
}

// ChangeDeferredCauset renames an existing defCausumn and modifies the defCausumn's definition,
// currently we only support limited HoTT of changes
// that do not need to change or check data on the block.
func (d *dbs) ChangeDeferredCauset(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewDeferredCauset := spec.NewDeferredCausets[0]
	if len(specNewDeferredCauset.Name.Schema.O) != 0 && ident.Schema.L != specNewDeferredCauset.Name.Schema.L {
		return ErrWrongDBName.GenWithStackByArgs(specNewDeferredCauset.Name.Schema.O)
	}
	if len(spec.OldDeferredCausetName.Schema.O) != 0 && ident.Schema.L != spec.OldDeferredCausetName.Schema.L {
		return ErrWrongDBName.GenWithStackByArgs(spec.OldDeferredCausetName.Schema.O)
	}
	if len(specNewDeferredCauset.Name.Block.O) != 0 && ident.Name.L != specNewDeferredCauset.Name.Block.L {
		return ErrWrongTableName.GenWithStackByArgs(specNewDeferredCauset.Name.Block.O)
	}
	if len(spec.OldDeferredCausetName.Block.O) != 0 && ident.Name.L != spec.OldDeferredCausetName.Block.L {
		return ErrWrongTableName.GenWithStackByArgs(spec.OldDeferredCausetName.Block.O)
	}

	job, err := d.getModifiableDeferredCausetJob(ctx, ident, spec.OldDeferredCausetName.Name, spec)
	if err != nil {
		if schemareplicant.ErrDeferredCausetNotExists.Equal(err) && spec.IfExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(spec.OldDeferredCausetName.Name, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	err = d.doDBSJob(ctx, job)
	// defCausumn not exists, but if_exists flags is true, so we ignore this error.
	if schemareplicant.ErrDeferredCausetNotExists.Equal(err) && spec.IfExists {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// RenameDeferredCauset renames an existing defCausumn.
func (d *dbs) RenameDeferredCauset(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	oldDefCausName := spec.OldDeferredCausetName.Name
	newDefCausName := spec.NewDeferredCausetName.Name
	if oldDefCausName.L == newDefCausName.L {
		return nil
	}
	if newDefCausName.L == perceptron.ExtraHandleName.L {
		return ErrWrongDeferredCausetName.GenWithStackByArgs(newDefCausName.L)
	}

	schemaReplicant, tbl, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	oldDefCaus := block.FindDefCaus(tbl.VisibleDefCauss(), oldDefCausName.L)
	if oldDefCaus == nil {
		return schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(oldDefCausName, ident.Name)
	}

	allDefCauss := tbl.DefCauss()
	defCausWithNewNameAlreadyExist := block.FindDefCaus(allDefCauss, newDefCausName.L) != nil
	if defCausWithNewNameAlreadyExist {
		return schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(newDefCausName)
	}

	if fkInfo := getDeferredCausetForeignKeyInfo(oldDefCausName.L, tbl.Meta().ForeignKeys); fkInfo != nil {
		return errFKIncompatibleDeferredCausets.GenWithStackByArgs(oldDefCausName, fkInfo.Name)
	}

	// Check generated expression.
	for _, defCaus := range allDefCauss {
		if defCaus.GeneratedExpr == nil {
			continue
		}
		dependedDefCausNames := findDeferredCausetNamesInExpr(defCaus.GeneratedExpr)
		for _, name := range dependedDefCausNames {
			if name.Name.L == oldDefCausName.L {
				return ErrBadField.GenWithStackByArgs(oldDefCausName.O, "generated defCausumn function")
			}
		}
	}

	newDefCaus := oldDefCaus.Clone()
	newDefCaus.Name = newDefCausName
	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tbl.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionModifyDeferredCauset,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{&newDefCaus, oldDefCausName, spec.Position, 0},
	}
	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// ModifyDeferredCauset does modification on an existing defCausumn, currently we only support limited HoTT of changes
// that do not need to change or check data on the block.
func (d *dbs) ModifyDeferredCauset(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewDeferredCauset := spec.NewDeferredCausets[0]
	if len(specNewDeferredCauset.Name.Schema.O) != 0 && ident.Schema.L != specNewDeferredCauset.Name.Schema.L {
		return ErrWrongDBName.GenWithStackByArgs(specNewDeferredCauset.Name.Schema.O)
	}
	if len(specNewDeferredCauset.Name.Block.O) != 0 && ident.Name.L != specNewDeferredCauset.Name.Block.L {
		return ErrWrongTableName.GenWithStackByArgs(specNewDeferredCauset.Name.Block.O)
	}

	originalDefCausName := specNewDeferredCauset.Name.Name
	job, err := d.getModifiableDeferredCausetJob(ctx, ident, originalDefCausName, spec)
	if err != nil {
		if schemareplicant.ErrDeferredCausetNotExists.Equal(err) && spec.IfExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(schemareplicant.ErrDeferredCausetNotExists.GenWithStackByArgs(originalDefCausName, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	err = d.doDBSJob(ctx, job)
	// defCausumn not exists, but if_exists flags is true, so we ignore this error.
	if schemareplicant.ErrDeferredCausetNotExists.Equal(err) && spec.IfExists {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) AlterDeferredCauset(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewDeferredCauset := spec.NewDeferredCausets[0]
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}

	defCausName := specNewDeferredCauset.Name.Name
	// Check whether alter defCausumn has existed.
	defCaus := block.FindDefCaus(t.DefCauss(), defCausName.L)
	if defCaus == nil {
		return ErrBadField.GenWithStackByArgs(defCausName, ident.Name)
	}

	// Clean the NoDefaultValueFlag value.
	defCaus.Flag &= ^allegrosql.NoDefaultValueFlag
	if len(specNewDeferredCauset.Options) == 0 {
		err = defCaus.SetDefaultValue(nil)
		if err != nil {
			return errors.Trace(err)
		}
		setNoDefaultValueFlag(defCaus, false)
	} else {
		if IsAutoRandomDeferredCausetID(t.Meta(), defCaus.ID) {
			return ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
		hasDefaultValue, err := setDefaultValue(ctx, defCaus, specNewDeferredCauset.Options[0])
		if err != nil {
			return errors.Trace(err)
		}
		if err = checkDefaultValue(ctx, defCaus, hasDefaultValue); err != nil {
			return errors.Trace(err)
		}
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionSetDefaultValue,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{defCaus},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableComment uFIDelates the block comment information.
func (d *dbs) AlterTableComment(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionModifyTableComment,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{spec.Comment},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableAutoIDCache uFIDelates the block comment information.
func (d *dbs) AlterTableAutoIDCache(ctx stochastikctx.Context, ident ast.Ident, newCache int64) error {
	schemaReplicant, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionModifyTableAutoIdCache,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{newCache},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableCharsetAndDefCauslate changes the block charset and defCauslate.
func (d *dbs) AlterTableCharsetAndDefCauslate(ctx stochastikctx.Context, ident ast.Ident, toCharset, toDefCauslate string, needsOverwriteDefCauss bool) error {
	// use the last one.
	if toCharset == "" && toDefCauslate == "" {
		return ErrUnknownCharacterSet.GenWithStackByArgs(toCharset)
	}

	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	if toCharset == "" {
		// charset does not change.
		toCharset = tb.Meta().Charset
	}

	if toDefCauslate == "" {
		// get the default defCauslation of the charset.
		toDefCauslate, err = charset.GetDefaultDefCauslation(toCharset)
		if err != nil {
			return errors.Trace(err)
		}
	}
	doNothing, err := checkAlterTableCharset(tb.Meta(), schemaReplicant, toCharset, toDefCauslate, needsOverwriteDefCauss)
	if err != nil {
		return err
	}
	if doNothing {
		return nil
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionModifyTableCharsetAndDefCauslate,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{toCharset, toDefCauslate, needsOverwriteDefCauss},
	}
	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// AlterTableSetTiFlashReplica sets the TiFlash replicas info.
func (d *dbs) AlterTableSetTiFlashReplica(ctx stochastikctx.Context, ident ast.Ident, replicaInfo *ast.TiFlashReplicaSpec) error {
	schemaReplicant, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	tbReplicaInfo := tb.Meta().TiFlashReplica
	if tbReplicaInfo != nil && tbReplicaInfo.Count == replicaInfo.Count &&
		len(tbReplicaInfo.LocationLabels) == len(replicaInfo.Labels) {
		changed := false
		for i, lable := range tbReplicaInfo.LocationLabels {
			if replicaInfo.Labels[i] != lable {
				changed = true
				break
			}
		}
		if !changed {
			return nil
		}
	}

	err = checkTiFlashReplicaCount(ctx, replicaInfo.Count)
	if err != nil {
		return errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionSetTiFlashReplica,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{*replicaInfo},
	}
	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkTiFlashReplicaCount(ctx stochastikctx.Context, replicaCount uint64) error {
	// Check the tiflash replica count should be less than the total tiflash stores.
	tiflashStoreCnt, err := schemareplicant.GetTiFlashStoreCount(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if replicaCount > tiflashStoreCnt {
		return errors.Errorf("the tiflash replica count: %d should be less than the total tiflash server count: %d", replicaCount, tiflashStoreCnt)
	}
	return nil
}

// UFIDelateTableReplicaInfo uFIDelates the block flash replica infos.
func (d *dbs) UFIDelateTableReplicaInfo(ctx stochastikctx.Context, physicalID int64, available bool) error {
	is := d.infoHandle.Get()
	tb, ok := is.TableByID(physicalID)
	if !ok {
		tb, _ = is.FindTableByPartitionID(physicalID)
		if tb == nil {
			return schemareplicant.ErrTableNotExists.GenWithStack("Block which ID = %d does not exist.", physicalID)
		}
	}
	tbInfo := tb.Meta()
	if tbInfo.TiFlashReplica == nil || (tbInfo.ID == physicalID && tbInfo.TiFlashReplica.Available == available) ||
		(tbInfo.ID != physicalID && available == tbInfo.TiFlashReplica.IsPartitionAvailable(physicalID)) {
		return nil
	}

	EDB, ok := is.SchemaByTable(tbInfo)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStack("Database of block `%s` does not exist.", tb.Meta().Name)
	}

	job := &perceptron.Job{
		SchemaID:   EDB.ID,
		TableID:    tb.Meta().ID,
		SchemaName: EDB.Name.L,
		Type:       perceptron.CausetActionUFIDelateTiFlashReplicaStatus,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{available, physicalID},
	}
	err := d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// checkAlterTableCharset uses to check is it possible to change the charset of block.
// This function returns 2 variable:
// doNothing: if doNothing is true, means no need to change any more, because the target charset is same with the charset of block.
// err: if err is not nil, means it is not possible to change block charset to target charset.
func checkAlterTableCharset(tblInfo *perceptron.TableInfo, dbInfo *perceptron.DBInfo, toCharset, toDefCauslate string, needsOverwriteDefCauss bool) (doNothing bool, err error) {
	origCharset := tblInfo.Charset
	origDefCauslate := tblInfo.DefCauslate
	// Old version schemaReplicant charset maybe modified when load schemaReplicant if TreatOldVersionUTF8AsUTF8MB4 was enable.
	// So even if the origCharset equal toCharset, we still need to do the dbs for old version schemaReplicant.
	if origCharset == toCharset && origDefCauslate == toDefCauslate && tblInfo.Version >= perceptron.TableInfoVersion2 {
		// nothing to do.
		doNothing = true
		for _, defCaus := range tblInfo.DeferredCausets {
			if defCaus.Charset == charset.CharsetBin {
				continue
			}
			if defCaus.Charset == toCharset && defCaus.DefCauslate == toDefCauslate {
				continue
			}
			doNothing = false
		}
		if doNothing {
			return doNothing, nil
		}
	}

	// The block charset may be "", if the block is create in old MilevaDB version, such as v2.0.8.
	// This DBS will uFIDelate the block charset to default charset.
	origCharset, origDefCauslate, err = ResolveCharsetDefCauslation(
		ast.CharsetOpt{Chs: origCharset, DefCaus: origDefCauslate},
		ast.CharsetOpt{Chs: dbInfo.Charset, DefCaus: dbInfo.DefCauslate},
	)
	if err != nil {
		return doNothing, err
	}

	if err = checkModifyCharsetAndDefCauslation(toCharset, toDefCauslate, origCharset, origDefCauslate, false); err != nil {
		return doNothing, err
	}
	if !needsOverwriteDefCauss {
		// If we don't change the charset and defCauslation of defCausumns, skip the next checks.
		return doNothing, nil
	}

	for _, defCaus := range tblInfo.DeferredCausets {
		if defCaus.Tp == allegrosql.TypeVarchar {
			if err = IsTooBigFieldLength(defCaus.Flen, defCaus.Name.O, toCharset); err != nil {
				return doNothing, err
			}
		}
		if defCaus.Charset == charset.CharsetBin {
			continue
		}
		if len(defCaus.Charset) == 0 {
			continue
		}
		if err = checkModifyCharsetAndDefCauslation(toCharset, toDefCauslate, defCaus.Charset, defCaus.DefCauslate, isDeferredCausetWithIndex(defCaus.Name.L, tblInfo.Indices)); err != nil {
			if strings.Contains(err.Error(), "Unsupported modifying defCauslation") {
				defCausErrMsg := "Unsupported converting defCauslation of defCausumn '%s' from '%s' to '%s' when index is defined on it."
				err = errUnsupportedModifyDefCauslation.GenWithStack(defCausErrMsg, defCaus.Name.L, defCaus.DefCauslate, toDefCauslate)
			}
			return doNothing, err
		}
	}
	return doNothing, nil
}

// RenameIndex renames an index.
// In MilevaDB, indexes are case-insensitive (so index 'a' and 'A" are considered the same index),
// but index names are case-sensitive (we can rename index 'a' to 'A')
func (d *dbs) RenameIndex(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	duplicate, err := validateRenameIndex(spec.FromKey, spec.ToKey, tb.Meta())
	if duplicate {
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionRenameIndex,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{spec.FromKey, spec.ToKey},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropTable will proceed even if some block in the list does not exists.
func (d *dbs) DropTable(ctx stochastikctx.Context, ti ast.Ident) (err error) {
	schemaReplicant, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if tb.Meta().IsView() {
		return schemareplicant.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}
	if tb.Meta().IsSequence() {
		return schemareplicant.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionDropTable,
		BinlogInfo: &perceptron.HistoryInfo{},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	if err != nil {
		return errors.Trace(err)
	}
	if !config.TableLockEnabled() {
		return nil
	}
	if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok {
		ctx.ReleaseTableLockByTableIDs([]int64{tb.Meta().ID})
	}
	return nil
}

// DropView will proceed even if some view in the list does not exists.
func (d *dbs) DropView(ctx stochastikctx.Context, ti ast.Ident) (err error) {
	schemaReplicant, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if !tb.Meta().IsView() {
		return ErrWrongObject.GenWithStackByArgs(ti.Schema, ti.Name, "VIEW")
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionDropView,
		BinlogInfo: &perceptron.HistoryInfo{},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) TruncateTable(ctx stochastikctx.Context, ti ast.Ident) error {
	schemaReplicant, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().IsView() || tb.Meta().IsSequence() {
		return schemareplicant.ErrTableNotExists.GenWithStackByArgs(schemaReplicant.Name.O, tb.Meta().Name.O)
	}
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	newTableID := genIDs[0]
	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionTruncateTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{newTableID},
	}
	if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok && config.TableLockEnabled() {
		// AddTableLock here to avoid this dbs job was executed successfully but the stochastik was been kill before return.
		// The stochastik will release all block locks it holds, if we don't add the new locking block id here,
		// the stochastik may forget to release the new locked block id when this dbs job was executed successfully
		// but the stochastik was killed before return.
		ctx.AddTableLock([]perceptron.TableLockTpInfo{{SchemaID: schemaReplicant.ID, TableID: newTableID, Tp: tb.Meta().Lock.Tp}})
	}
	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	if err != nil {
		if config.TableLockEnabled() {
			ctx.ReleaseTableLockByTableIDs([]int64{newTableID})
		}
		return errors.Trace(err)
	}
	oldTblInfo := tb.Meta()
	if oldTblInfo.PreSplitRegions > 0 {
		if _, tb, err := d.getSchemaAndTableByIdent(ctx, ti); err == nil {
			d.preSplitAndScatter(ctx, tb.Meta(), tb.Meta().GetPartitionInfo())
		}
	}

	if !config.TableLockEnabled() {
		return nil
	}
	if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok {
		ctx.ReleaseTableLockByTableIDs([]int64{tb.Meta().ID})
	}
	return nil
}

func (d *dbs) RenameTable(ctx stochastikctx.Context, oldIdent, newIdent ast.Ident, isAlterTable bool) error {
	is := d.GetSchemaReplicantWithInterceptor(ctx)
	oldSchema, ok := is.SchemaByName(oldIdent.Schema)
	if !ok {
		if isAlterTable {
			return schemareplicant.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
		}
		if is.TableExists(newIdent.Schema, newIdent.Name) {
			return schemareplicant.ErrTableExists.GenWithStackByArgs(newIdent)
		}
		return errFileNotFound.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
	}
	oldTbl, err := is.TableByName(oldIdent.Schema, oldIdent.Name)
	if err != nil {
		if isAlterTable {
			return schemareplicant.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
		}
		if is.TableExists(newIdent.Schema, newIdent.Name) {
			return schemareplicant.ErrTableExists.GenWithStackByArgs(newIdent)
		}
		return errFileNotFound.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
	}
	if isAlterTable && newIdent.Schema.L == oldIdent.Schema.L && newIdent.Name.L == oldIdent.Name.L {
		// oldIdent is equal to newIdent, do nothing
		return nil
	}
	newSchema, ok := is.SchemaByName(newIdent.Schema)
	if !ok {
		return ErrErrorOnRename.GenWithStackByArgs(
			fmt.Sprintf("%s.%s", oldIdent.Schema, oldIdent.Name),
			fmt.Sprintf("%s.%s", newIdent.Schema, newIdent.Name),
			168,
			fmt.Sprintf("Database `%s` doesn't exist", newIdent.Schema))
	}
	if is.TableExists(newIdent.Schema, newIdent.Name) {
		return schemareplicant.ErrTableExists.GenWithStackByArgs(newIdent)
	}
	if err := checkTooLongTable(newIdent.Name); err != nil {
		return errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   newSchema.ID,
		TableID:    oldTbl.Meta().ID,
		SchemaName: newSchema.Name.L,
		Type:       perceptron.CausetActionRenameTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{oldSchema.ID, newIdent.Name},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func getAnonymousIndex(t block.Block, defCausName perceptron.CIStr) perceptron.CIStr {
	id := 2
	l := len(t.Indices())
	indexName := defCausName
	if strings.EqualFold(indexName.L, allegrosql.PrimaryKeyName) {
		indexName = perceptron.NewCIStr(fmt.Sprintf("%s_%d", defCausName.O, id))
		id = 3
	}
	for i := 0; i < l; i++ {
		if t.Indices()[i].Meta().Name.L == indexName.L {
			indexName = perceptron.NewCIStr(fmt.Sprintf("%s_%d", defCausName.O, id))
			i = -1
			id++
		}
	}
	return indexName
}

func (d *dbs) CreatePrimaryKey(ctx stochastikctx.Context, ti ast.Ident, indexName perceptron.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption) error {
	if !config.GetGlobalConfig().AlterPrimaryKey {
		return ErrUnsupportedModifyPrimaryKey.GenWithStack("Unsupported add primary key, alter-primary-key is false")
	}

	schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return ErrTooLongIdent.GenWithStackByArgs(allegrosql.PrimaryKeyName)
	}

	indexName = perceptron.NewCIStr(allegrosql.PrimaryKeyName)
	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil ||
		// If the block's PKIsHandle is true, it also means that this block has a primary key.
		t.Meta().PKIsHandle {
		return schemareplicant.ErrMultiplePriKey
	}

	// Primary keys cannot include expression index parts. A primary key requires the generated defCausumn to be stored,
	// but expression index parts are implemented as virtual generated defCausumns, not stored generated defCausumns.
	for _, idxPart := range indexPartSpecifications {
		if idxPart.Expr != nil {
			return ErrFunctionalIndexPrimaryKey
		}
	}

	tblInfo := t.Meta()
	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DBS check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DBS job is put to the queue, and if the check fail, MilevaDB will run the DBS cancel logic.
	// The recover step causes DBS wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	indexDeferredCausets, err := buildIndexDeferredCausets(tblInfo.DeferredCausets, indexPartSpecifications)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = checkPKOnGeneratedDeferredCauset(tblInfo, indexPartSpecifications); err != nil {
		return err
	}

	global := false
	if tblInfo.GetPartitionInfo() != nil {
		ck, err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), indexDeferredCausets, tblInfo)
		if err != nil {
			return err
		}
		if !ck {
			if !config.GetGlobalConfig().EnableGlobalIndex {
				return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY")
			}
			//index defCausumns does not contain all partition defCausumns, must set global
			global = true
		}
	}

	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if _, err = validateCommentLength(ctx.GetStochaseinstein_dbars(), indexName.String(), indexOption); err != nil {
		return errors.Trace(err)
	}

	unique := true
	sqlMode := ctx.GetStochaseinstein_dbars().ALLEGROSQLMode
	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionAddPrimaryKey,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{unique, indexName, indexPartSpecifications, indexOption, sqlMode, nil, global},
		Priority:   ctx.GetStochaseinstein_dbars().DBSReorgPriority,
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func buildHiddenDeferredCausetInfo(ctx stochastikctx.Context, indexPartSpecifications []*ast.IndexPartSpecification, indexName perceptron.CIStr, tblInfo *perceptron.TableInfo, existDefCauss []*block.DeferredCauset) ([]*perceptron.DeferredCausetInfo, error) {
	hiddenDefCauss := make([]*perceptron.DeferredCausetInfo, 0, len(indexPartSpecifications))
	for i, idxPart := range indexPartSpecifications {
		if idxPart.Expr == nil {
			continue
		}
		idxPart.DeferredCauset = &ast.DeferredCausetName{Name: perceptron.NewCIStr(fmt.Sprintf("%s_%s_%d", expressionIndexPrefix, indexName, i))}
		// Check whether the hidden defCausumns have existed.
		defCaus := block.FindDefCaus(existDefCauss, idxPart.DeferredCauset.Name.L)
		if defCaus != nil {
			// TODO: Use expression index related error.
			return nil, schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(defCaus.Name.String())
		}
		idxPart.Length = types.UnspecifiedLength
		// The index part is an expression, prepare a hidden defCausumn for it.
		if len(idxPart.DeferredCauset.Name.L) > allegrosql.MaxDeferredCausetNameLength {
			// TODO: Refine the error message.
			return nil, ErrTooLongIdent.GenWithStackByArgs("hidden defCausumn")
		}
		// TODO: refine the error message.
		if err := checkIllegalFn4GeneratedDeferredCauset("expression index", idxPart.Expr); err != nil {
			return nil, errors.Trace(err)
		}

		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		sb.Reset()
		err := idxPart.Expr.Restore(restoreCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		expr, err := expression.RewriteSimpleExprWithTableInfo(ctx, tblInfo, idxPart.Expr)
		if err != nil {
			// TODO: refine the error message.
			return nil, err
		}
		if _, ok := expr.(*expression.DeferredCauset); ok {
			return nil, ErrFunctionalIndexOnField
		}

		defCausInfo := &perceptron.DeferredCausetInfo{
			Name:                idxPart.DeferredCauset.Name,
			GeneratedExprString: sb.String(),
			GeneratedStored:     false,
			Version:             perceptron.CurrLatestDeferredCausetInfoVersion,
			Dependences:         make(map[string]struct{}),
			Hidden:              true,
			FieldType:           *expr.GetType(),
		}
		checkDependencies := make(map[string]struct{})
		for _, defCausName := range findDeferredCausetNamesInExpr(idxPart.Expr) {
			defCausInfo.Dependences[defCausName.Name.O] = struct{}{}
			checkDependencies[defCausName.Name.O] = struct{}{}
		}
		if err = checkDependedDefCausExist(checkDependencies, existDefCauss); err != nil {
			return nil, errors.Trace(err)
		}
		if err = checkAutoIncrementRef("", defCausInfo.Dependences, tblInfo); err != nil {
			return nil, errors.Trace(err)
		}
		idxPart.Expr = nil
		hiddenDefCauss = append(hiddenDefCauss, defCausInfo)
	}
	return hiddenDefCauss, nil
}

func (d *dbs) CreateIndex(ctx stochastikctx.Context, ti ast.Ident, keyType ast.IndexKeyType, indexName perceptron.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {
	// not support Spatial and FullText index
	if keyType == ast.IndexKeyTypeFullText || keyType == ast.IndexKeyTypeSpatial {
		return errUnsupportedIndexType.GenWithStack("FULLTEXT and SPATIAL index is not supported")
	}
	unique := keyType == ast.IndexKeyTypeUnique
	schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		defCausName := perceptron.NewCIStr("expression_index")
		if indexPartSpecifications[0].DeferredCauset != nil {
			defCausName = indexPartSpecifications[0].DeferredCauset.Name
		}
		indexName = getAnonymousIndex(t, defCausName)
	}

	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil {
		if indexInfo.State != perceptron.StatePublic {
			// NOTE: explicit error message. See issue #18363.
			err = ErrDupKeyName.GenWithStack("index already exist %s; "+
				"a background job is trying to add the same index, "+
				"please check by `ADMIN SHOW DBS JOBS`", indexName)
		} else {
			err = ErrDupKeyName.GenWithStack("index already exist %s", indexName)
		}
		if ifNotExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return errors.Trace(err)
	}

	tblInfo := t.Meta()

	// Build hidden defCausumns if necessary.
	hiddenDefCauss, err := buildHiddenDeferredCausetInfo(ctx, indexPartSpecifications, indexName, t.Meta(), t.DefCauss())
	if err != nil {
		return err
	}
	if err = checkAddDeferredCausetTooManyDeferredCausets(len(t.DefCauss()) + len(hiddenDefCauss)); err != nil {
		return errors.Trace(err)
	}

	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DBS check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DBS job is put to the queue, and if the check fail, MilevaDB will run the DBS cancel logic.
	// The recover step causes DBS wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	indexDeferredCausets, err := buildIndexDeferredCausets(append(tblInfo.DeferredCausets, hiddenDefCauss...), indexPartSpecifications)
	if err != nil {
		return errors.Trace(err)
	}

	global := false
	if unique && tblInfo.GetPartitionInfo() != nil {
		ck, err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), indexDeferredCausets, tblInfo)
		if err != nil {
			return err
		}
		if !ck {
			if !config.GetGlobalConfig().EnableGlobalIndex {
				return ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("UNIQUE INDEX")
			}
			//index defCausumns does not contain all partition defCausumns, must set global
			global = true
		}
	}
	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if _, err = validateCommentLength(ctx.GetStochaseinstein_dbars(), indexName.String(), indexOption); err != nil {
		return errors.Trace(err)
	}
	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionAddIndex,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{unique, indexName, indexPartSpecifications, indexOption, hiddenDefCauss, global},
		Priority:   ctx.GetStochaseinstein_dbars().DBSReorgPriority,
	}

	err = d.doDBSJob(ctx, job)
	// key exists, but if_not_exists flags is true, so we ignore this error.
	if ErrDupKeyName.Equal(err) && ifNotExists {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func buildFKInfo(fkName perceptron.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef, defcaus []*block.DeferredCauset, tbInfo *perceptron.TableInfo) (*perceptron.FKInfo, error) {
	if len(keys) != len(refer.IndexPartSpecifications) {
		return nil, schemareplicant.ErrForeignKeyNotMatch.GenWithStackByArgs("foreign key without name")
	}

	// all base defCausumns of stored generated defCausumns
	baseDefCauss := make(map[string]struct{})
	for _, defCaus := range defcaus {
		if defCaus.IsGenerated() && defCaus.GeneratedStored {
			for name := range defCaus.Dependences {
				baseDefCauss[name] = struct{}{}
			}
		}
	}

	fkInfo := &perceptron.FKInfo{
		Name:     fkName,
		RefTable: refer.Block.Name,
		DefCauss: make([]perceptron.CIStr, len(keys)),
	}

	for i, key := range keys {
		// Check add foreign key to generated defCausumns
		// For more detail, see https://dev.allegrosql.com/doc/refman/8.0/en/innodb-foreign-key-constraints.html#innodb-foreign-key-generated-defCausumns
		for _, defCaus := range defcaus {
			if defCaus.Name.L != key.DeferredCauset.Name.L {
				continue
			}
			if defCaus.IsGenerated() {
				// Check foreign key on virtual generated defCausumns
				if !defCaus.GeneratedStored {
					return nil, schemareplicant.ErrCannotAddForeign
				}

				// Check wrong reference options of foreign key on stored generated defCausumns
				switch refer.OnUFIDelate.ReferOpt {
				case ast.ReferOptionCascade, ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, errWrongFKOptionForGeneratedDeferredCauset.GenWithStackByArgs("ON UFIDelATE " + refer.OnUFIDelate.ReferOpt.String())
				}
				switch refer.OnDelete.ReferOpt {
				case ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, errWrongFKOptionForGeneratedDeferredCauset.GenWithStackByArgs("ON DELETE " + refer.OnDelete.ReferOpt.String())
				}
				continue
			}
			// Check wrong reference options of foreign key on base defCausumns of stored generated defCausumns
			if _, ok := baseDefCauss[defCaus.Name.L]; ok {
				switch refer.OnUFIDelate.ReferOpt {
				case ast.ReferOptionCascade, ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, schemareplicant.ErrCannotAddForeign
				}
				switch refer.OnDelete.ReferOpt {
				case ast.ReferOptionCascade, ast.ReferOptionSetNull, ast.ReferOptionSetDefault:
					return nil, schemareplicant.ErrCannotAddForeign
				}
			}
		}
		if block.FindDefCaus(defcaus, key.DeferredCauset.Name.O) == nil {
			return nil, errKeyDeferredCausetDoesNotExits.GenWithStackByArgs(key.DeferredCauset.Name)
		}
		fkInfo.DefCauss[i] = key.DeferredCauset.Name
	}

	fkInfo.RefDefCauss = make([]perceptron.CIStr, len(refer.IndexPartSpecifications))
	for i, key := range refer.IndexPartSpecifications {
		fkInfo.RefDefCauss[i] = key.DeferredCauset.Name
	}

	fkInfo.OnDelete = int(refer.OnDelete.ReferOpt)
	fkInfo.OnUFIDelate = int(refer.OnUFIDelate.ReferOpt)

	return fkInfo, nil
}

func (d *dbs) CreateForeignKey(ctx stochastikctx.Context, ti ast.Ident, fkName perceptron.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef) error {
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	fkInfo, err := buildFKInfo(fkName, keys, refer, t.DefCauss(), t.Meta())
	if err != nil {
		return errors.Trace(err)
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionAddForeignKey,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{fkInfo},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)

}

func (d *dbs) DropForeignKey(ctx stochastikctx.Context, ti ast.Ident, fkName perceptron.CIStr) error {
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionDropForeignKey,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{fkName},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) DropIndex(ctx stochastikctx.Context, ti ast.Ident, indexName perceptron.CIStr, ifExists bool) error {
	is := d.infoHandle.Get()
	schemaReplicant, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(schemareplicant.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(schemareplicant.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	indexInfo := t.Meta().FindIndexByName(indexName.L)
	var isPK bool
	if indexName.L == strings.ToLower(allegrosql.PrimaryKeyName) &&
		// Before we fixed #14243, there might be a general index named `primary` but not a primary key.
		(indexInfo == nil || indexInfo.Primary) {
		isPK = true
	}
	if isPK {
		if !config.GetGlobalConfig().AlterPrimaryKey {
			return ErrUnsupportedModifyPrimaryKey.GenWithStack("Unsupported drop primary key when alter-primary-key is false")

		}
		// If the block's PKIsHandle is true, we can't find the index from the block. So we check the value of PKIsHandle.
		if indexInfo == nil && !t.Meta().PKIsHandle {
			return ErrCantDropFieldOrKey.GenWithStack("Can't DROP 'PRIMARY'; check that defCausumn/key exists")
		}
		if t.Meta().PKIsHandle {
			return ErrUnsupportedModifyPrimaryKey.GenWithStack("Unsupported drop primary key when the block's pkIsHandle is true")
		}
		if t.Meta().IsCommonHandle {
			return ErrUnsupportedModifyPrimaryKey.GenWithStack("Unsupported drop primary key when the block is using clustered index")
		}
	}
	if indexInfo == nil {
		err = ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
		if ifExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	// Check for drop index on auto_increment defCausumn.
	err = checkDropIndexOnAutoIncrementDeferredCauset(t.Meta(), indexInfo)
	if err != nil {
		return errors.Trace(err)
	}

	jobTp := perceptron.CausetActionDropIndex
	if isPK {
		jobTp = perceptron.CausetActionDropPrimaryKey
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    t.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       jobTp,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{indexName},
	}

	err = d.doDBSJob(ctx, job)
	// index not exists, but if_exists flags is true, so we ignore this error.
	if ErrCantDropFieldOrKey.Equal(err) && ifExists {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func isDroppableDeferredCauset(tblInfo *perceptron.TableInfo, defCausName perceptron.CIStr) error {
	// Check whether there are other defCausumns depend on this defCausumn or not.
	for _, defCaus := range tblInfo.DeferredCausets {
		for dep := range defCaus.Dependences {
			if dep == defCausName.L {
				return errDependentByGeneratedDeferredCauset.GenWithStackByArgs(dep)
			}
		}
	}
	if len(tblInfo.DeferredCausets) == 1 {
		return ErrCantRemoveAllFields.GenWithStack("can't drop only defCausumn %s in block %s",
			defCausName, tblInfo.Name)
	}
	// We only support dropping defCausumn with single-value none Primary Key index covered now.
	if !isDeferredCausetCanDropWithIndex(defCausName.L, tblInfo.Indices) {
		return errCantDropDefCausWithIndex.GenWithStack("can't drop defCausumn %s with composite index covered or Primary Key covered now", defCausName)
	}
	// Check the defCausumn with foreign key.
	if fkInfo := getDeferredCausetForeignKeyInfo(defCausName.L, tblInfo.ForeignKeys); fkInfo != nil {
		return errFkDeferredCausetCannotDrop.GenWithStackByArgs(defCausName, fkInfo.Name)
	}
	return nil
}

// validateCommentLength checks comment length of block, defCausumn, index and partition.
// If comment length is more than the standard length truncate it
// and causetstore the comment length upto the standard comment length size.
func validateCommentLength(vars *variable.Stochaseinstein_dbars, indexName string, indexOption *ast.IndexOption) (string, error) {
	if indexOption == nil {
		return "", nil
	}

	maxLen := MaxCommentLength
	if len(indexOption.Comment) > maxLen {
		err := errTooLongIndexComment.GenWithStackByArgs(indexName, maxLen)
		if vars.StrictALLEGROSQLMode {
			return "", err
		}
		vars.StmtCtx.AppendWarning(err)
		indexOption.Comment = indexOption.Comment[:maxLen]
	}
	return indexOption.Comment, nil
}

func buildPartitionInfo(ctx stochastikctx.Context, meta *perceptron.TableInfo, d *dbs, spec *ast.AlterTableSpec) (*perceptron.PartitionInfo, error) {
	if meta.Partition.Type == perceptron.PartitionTypeRange {
		if len(spec.PartDefinitions) == 0 {
			return nil, ast.ErrPartitionsMustBeDefined.GenWithStackByArgs(meta.Partition.Type)
		}
	} else {
		// we don't support ADD PARTITION for all other partition types yet.
		return nil, errors.Trace(ErrUnsupportedAddPartition)
	}

	part := &perceptron.PartitionInfo{
		Type:            meta.Partition.Type,
		Expr:            meta.Partition.Expr,
		DeferredCausets: meta.Partition.DeferredCausets,
		Enable:          meta.Partition.Enable,
	}

	genIDs, err := d.genGlobalIDs(len(spec.PartDefinitions))
	if err != nil {
		return nil, err
	}
	for ith, def := range spec.PartDefinitions {
		if err := def.Clause.Validate(part.Type, len(part.DeferredCausets)); err != nil {
			return nil, errors.Trace(err)
		}
		if err := checkTooLongTable(def.Name); err != nil {
			return nil, err
		}
		// For RANGE partition only VALUES LESS THAN should be possible.
		clause := def.Clause.(*ast.PartitionDefinitionClauseLessThan)
		if len(part.DeferredCausets) > 0 {
			if err := checkRangeDeferredCausetsTypeAndValuesMatch(ctx, meta, clause.Exprs); err != nil {
				return nil, err
			}
		}

		comment, _ := def.Comment()
		piDef := perceptron.PartitionDefinition{
			Name:    def.Name,
			ID:      genIDs[ith],
			Comment: comment,
		}

		buf := new(bytes.Buffer)
		for _, expr := range clause.Exprs {
			expr.Format(buf)
			piDef.LessThan = append(piDef.LessThan, buf.String())
			buf.Reset()
		}
		part.Definitions = append(part.Definitions, piDef)
	}
	return part, nil
}

func checkRangeDeferredCausetsTypeAndValuesMatch(ctx stochastikctx.Context, meta *perceptron.TableInfo, exprs []ast.ExprNode) error {
	// Validate() has already checked len(defCausNames) = len(exprs)
	// create block ... partition by range defCausumns (defcaus)
	// partition p0 values less than (expr)
	// check the type of defcaus[i] and expr is consistent.
	defCausNames := meta.Partition.DeferredCausets
	for i, defCausExpr := range exprs {
		if _, ok := defCausExpr.(*ast.MaxValueExpr); ok {
			continue
		}

		defCausName := defCausNames[i]
		defCausInfo := getDeferredCausetInfoByName(meta, defCausName.L)
		if defCausInfo == nil {
			return errors.Trace(ErrFieldNotFoundPart)
		}
		defCausType := &defCausInfo.FieldType

		val, err := expression.EvalAstExpr(ctx, defCausExpr)
		if err != nil {
			return err
		}

		// Check val.ConvertTo(defCausType) doesn't work, so we need this case by case check.
		switch defCausType.Tp {
		case allegrosql.TypeDate, allegrosql.TypeDatetime:
			switch val.HoTT() {
			case types.HoTTString, types.HoTTBytes:
			default:
				return ErrWrongTypeDeferredCausetValue.GenWithStackByArgs()
			}
		}
	}
	return nil
}

// LockTables uses to execute dagger blocks statement.
func (d *dbs) LockTables(ctx stochastikctx.Context, stmt *ast.LockTablesStmt) error {
	lockTables := make([]perceptron.TableLockTpInfo, 0, len(stmt.TableLocks))
	stochastikInfo := perceptron.StochastikInfo{
		ServerID:     d.GetID(),
		StochastikID: ctx.GetStochaseinstein_dbars().ConnectionID,
	}
	uniqueTableID := make(map[int64]struct{})
	// Check whether the block was already locked by another.
	for _, tl := range stmt.TableLocks {
		tb := tl.Block
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() || t.Meta().IsSequence() {
			return block.ErrUnsupportedOp.GenWithStackByArgs()
		}
		err = checkTableLocked(t.Meta(), tl.Type, stochastikInfo)
		if err != nil {
			return err
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return schemareplicant.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		lockTables = append(lockTables, perceptron.TableLockTpInfo{SchemaID: schemaReplicant.ID, TableID: t.Meta().ID, Tp: tl.Type})
	}

	unlockTables := ctx.GetAllTableLocks()
	arg := &lockTablesArg{
		LockTables:     lockTables,
		UnlockTables:   unlockTables,
		StochastikInfo: stochastikInfo,
	}
	job := &perceptron.Job{
		SchemaID:   lockTables[0].SchemaID,
		TableID:    lockTables[0].TableID,
		Type:       perceptron.CausetActionLockTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{arg},
	}
	// AddTableLock here is avoiding this job was executed successfully but the stochastik was killed before return.
	ctx.AddTableLock(lockTables)
	err := d.doDBSJob(ctx, job)
	if err == nil {
		ctx.ReleaseTableLocks(unlockTables)
		ctx.AddTableLock(lockTables)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// UnlockTables uses to execute unlock blocks statement.
func (d *dbs) UnlockTables(ctx stochastikctx.Context, unlockTables []perceptron.TableLockTpInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	arg := &lockTablesArg{
		UnlockTables: unlockTables,
		StochastikInfo: perceptron.StochastikInfo{
			ServerID:     d.GetID(),
			StochastikID: ctx.GetStochaseinstein_dbars().ConnectionID,
		},
	}
	job := &perceptron.Job{
		SchemaID:   unlockTables[0].SchemaID,
		TableID:    unlockTables[0].TableID,
		Type:       perceptron.CausetActionUnlockTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{arg},
	}

	err := d.doDBSJob(ctx, job)
	if err == nil {
		ctx.ReleaseAllTableLocks()
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// CleanDeadTableLock uses to clean dead block locks.
func (d *dbs) CleanDeadTableLock(unlockTables []perceptron.TableLockTpInfo, se perceptron.StochastikInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	arg := &lockTablesArg{
		UnlockTables:   unlockTables,
		StochastikInfo: se,
	}
	job := &perceptron.Job{
		SchemaID:   unlockTables[0].SchemaID,
		TableID:    unlockTables[0].TableID,
		Type:       perceptron.CausetActionUnlockTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{arg},
	}

	ctx, err := d.sessPool.get()
	if err != nil {
		return err
	}
	defer d.sessPool.put(ctx)
	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func throwErrIfInMemOrSysDB(ctx stochastikctx.Context, dbLowerName string) error {
	if soliton.IsMemOrSysDB(dbLowerName) {
		if ctx.GetStochaseinstein_dbars().User != nil {
			return schemareplicant.ErrAccessDenied.GenWithStackByArgs(ctx.GetStochaseinstein_dbars().User.Username, ctx.GetStochaseinstein_dbars().User.Hostname)
		}
		return schemareplicant.ErrAccessDenied.GenWithStackByArgs("", "")
	}
	return nil
}

func (d *dbs) CleanupTableLock(ctx stochastikctx.Context, blocks []*ast.TableName) error {
	uniqueTableID := make(map[int64]struct{})
	cleanupTables := make([]perceptron.TableLockTpInfo, 0, len(blocks))
	unlockedTablesNum := 0
	// Check whether the block was already locked by another.
	for _, tb := range blocks {
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schemaReplicant, t, err := d.getSchemaAndTableByIdent(ctx, ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() || t.Meta().IsSequence() {
			return block.ErrUnsupportedOp
		}
		// Maybe the block t was not locked, but still try to unlock this block.
		// If we skip unlock the block here, the job maybe not consistent with the job.Query.
		// eg: unlock blocks t1,t2;  If t2 is not locked and skip here, then the job will only unlock block t1,
		// and this behaviour is not consistent with the allegrosql query.
		if !t.Meta().IsLocked() {
			unlockedTablesNum++
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return schemareplicant.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		cleanupTables = append(cleanupTables, perceptron.TableLockTpInfo{SchemaID: schemaReplicant.ID, TableID: t.Meta().ID})
	}
	// If the num of cleanupTables is 0, or all cleanupTables is unlocked, just return here.
	if len(cleanupTables) == 0 || len(cleanupTables) == unlockedTablesNum {
		return nil
	}

	arg := &lockTablesArg{
		UnlockTables: cleanupTables,
		IsCleanup:    true,
	}
	job := &perceptron.Job{
		SchemaID:   cleanupTables[0].SchemaID,
		TableID:    cleanupTables[0].TableID,
		Type:       perceptron.CausetActionUnlockTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{arg},
	}
	err := d.doDBSJob(ctx, job)
	if err == nil {
		ctx.ReleaseTableLocks(cleanupTables)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

type lockTablesArg struct {
	LockTables     []perceptron.TableLockTpInfo
	IndexOfLock    int
	UnlockTables   []perceptron.TableLockTpInfo
	IndexOfUnlock  int
	StochastikInfo perceptron.StochastikInfo
	IsCleanup      bool
}

func (d *dbs) RepairTable(ctx stochastikctx.Context, block *ast.TableName, createStmt *ast.CreateTableStmt) error {
	// Existence of EDB and block has been checked in the preprocessor.
	oldTableInfo, ok := (ctx.Value(petriutil.RepairedTable)).(*perceptron.TableInfo)
	if !ok || oldTableInfo == nil {
		return ErrRepairTableFail.GenWithStack("Failed to get the repaired block")
	}
	oldDBInfo, ok := (ctx.Value(petriutil.RepairedDatabase)).(*perceptron.DBInfo)
	if !ok || oldDBInfo == nil {
		return ErrRepairTableFail.GenWithStack("Failed to get the repaired database")
	}
	// By now only support same EDB repair.
	if createStmt.Block.Schema.L != oldDBInfo.Name.L {
		return ErrRepairTableFail.GenWithStack("Repaired block should in same database with the old one")
	}

	// It is necessary to specify the block.ID and partition.ID manually.
	newTableInfo, err := buildTableInfoWithCheck(ctx, createStmt, oldTableInfo.Charset, oldTableInfo.DefCauslate)
	if err != nil {
		return errors.Trace(err)
	}
	// Override newTableInfo with oldTableInfo's element necessary.
	// TODO: There may be more element assignments here, and the new TableInfo should be verified with the actual data.
	newTableInfo.ID = oldTableInfo.ID
	if err = checkAndOverridePartitionID(newTableInfo, oldTableInfo); err != nil {
		return err
	}
	newTableInfo.AutoIncID = oldTableInfo.AutoIncID
	// If any old defCausumnInfo has lost, that means the old defCausumn ID lost too, repair failed.
	for i, newOne := range newTableInfo.DeferredCausets {
		old := getDeferredCausetInfoByName(oldTableInfo, newOne.Name.L)
		if old == nil {
			return ErrRepairTableFail.GenWithStackByArgs("DeferredCauset " + newOne.Name.L + " has lost")
		}
		if newOne.Tp != old.Tp {
			return ErrRepairTableFail.GenWithStackByArgs("DeferredCauset " + newOne.Name.L + " type should be the same")
		}
		if newOne.Flen != old.Flen {
			logutil.BgLogger().Warn("[dbs] admin repair block : DeferredCauset " + newOne.Name.L + " flen is not equal to the old one")
		}
		newTableInfo.DeferredCausets[i].ID = old.ID
	}
	// If any old indexInfo has lost, that means the index ID lost too, so did the data, repair failed.
	for i, newOne := range newTableInfo.Indices {
		old := getIndexInfoByNameAndDeferredCauset(oldTableInfo, newOne)
		if old == nil {
			return ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " has lost")
		}
		if newOne.Tp != old.Tp {
			return ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " type should be the same")
		}
		newTableInfo.Indices[i].ID = old.ID
	}

	newTableInfo.State = perceptron.StatePublic
	err = checkTableInfoValid(newTableInfo)
	if err != nil {
		return err
	}
	newTableInfo.State = perceptron.StateNone

	job := &perceptron.Job{
		SchemaID:   oldDBInfo.ID,
		TableID:    newTableInfo.ID,
		SchemaName: oldDBInfo.Name.L,
		Type:       perceptron.CausetActionRepairTable,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{newTableInfo},
	}
	err = d.doDBSJob(ctx, job)
	if err == nil {
		// Remove the old TableInfo from repairInfo before petri reload.
		petriutil.RepairInfo.RemoveFromRepairInfo(oldDBInfo.Name.L, oldTableInfo.Name.L)
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) OrderByDeferredCausets(ctx stochastikctx.Context, ident ast.Ident) error {
	_, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().GetPkDefCausInfo() != nil {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(errors.Errorf("ORDER BY ignored as there is a user-defined clustered index in the block '%s'", ident.Name))
	}
	return nil
}

func (d *dbs) CreateSequence(ctx stochastikctx.Context, stmt *ast.CreateSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	sequenceInfo, err := buildSequenceInfo(stmt, ident)
	if err != nil {
		return err
	}
	// MilevaDB describe the sequence within a blockInfo, as a same-level object of a block and view.
	tbInfo, err := buildTableInfo(ctx, ident.Name, nil, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.Sequence = sequenceInfo

	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, ident.Schema, tbInfo, onExist, false /*tryRetainID*/)
}

func (d *dbs) DropSequence(ctx stochastikctx.Context, ti ast.Ident, ifExists bool) (err error) {
	schemaReplicant, tbl, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if !tbl.Meta().IsSequence() {
		err = ErrWrongObject.GenWithStackByArgs(ti.Schema, ti.Name, "SEQUENCE")
		if ifExists {
			ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tbl.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionDropSequence,
		BinlogInfo: &perceptron.HistoryInfo{},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *dbs) AlterIndexVisibility(ctx stochastikctx.Context, ident ast.Ident, indexName perceptron.CIStr, visibility ast.IndexVisibility) error {
	schemaReplicant, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return err
	}

	invisible := false
	if visibility == ast.IndexVisibilityInvisible {
		invisible = true
	}

	skip, err := validateAlterIndexVisibility(indexName, invisible, tb.Meta())
	if err != nil {
		return errors.Trace(err)
	}
	if skip {
		return nil
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionAlterIndexVisibility,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{indexName, invisible},
	}

	err = d.doDBSJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func buildPlacementSpecReplicasAndConstraint(rule *memristed.MemruleOp, replicas uint64, cnstr string) ([]*memristed.MemruleOp, error) {
	var err error
	cnstr = strings.TrimSpace(cnstr)
	rules := make([]*memristed.MemruleOp, 0, 1)
	if len(cnstr) > 0 && cnstr[0] == '[' {
		// can not emit REPLICAS with an array label
		if replicas == 0 {
			return rules, errors.Errorf("array CONSTRAINTS should be with a positive REPLICAS")
		}
		rule.Count = int(replicas)

		constraints := []string{}

		err = json.Unmarshal([]byte(cnstr), &constraints)
		if err != nil {
			return rules, err
		}

		rule.LabelConstraints, err = memristed.CheckLabelConstraints(constraints)
		if err != nil {
			return rules, err
		}

		rules = append(rules, rule)
	} else if len(cnstr) > 0 && cnstr[0] == '{' {
		constraints := map[string]int{}
		err = json.Unmarshal([]byte(cnstr), &constraints)
		if err != nil {
			return rules, err
		}

		ruleCnt := int(replicas)
		for labels, cnt := range constraints {
			newMemrule := rule.Clone()
			if cnt <= 0 {
				err = errors.Errorf("count should be positive, but got %d", cnt)
				break
			}

			if replicas != 0 {
				ruleCnt -= cnt
				if ruleCnt < 0 {
					err = errors.Errorf("REPLICAS should be larger or equal to the number of total replicas, but got %d", replicas)
					break
				}
			}
			newMemrule.Count = cnt

			newMemrule.LabelConstraints, err = memristed.CheckLabelConstraints(strings.Split(strings.TrimSpace(labels), ","))
			if err != nil {
				break
			}
			rules = append(rules, newMemrule)
		}
		rule.Count = ruleCnt

		if rule.Count > 0 {
			rules = append(rules, rule)
		}
	} else {
		err = errors.Errorf("constraint should be a JSON array or object, but got '%s'", cnstr)
	}
	return rules, err
}

func buildPlacementSpecs(specs []*ast.PlacementSpec) ([]*memristed.MemruleOp, error) {
	rules := make([]*memristed.MemruleOp, 0, len(specs))

	var err error
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	for _, spec := range specs {
		rule := &memristed.MemruleOp{
			Memrule: &memristed.Memrule{
				GroupID:  memristed.MemruleDefaultGroupID,
				Override: true,
			},
		}

		switch spec.Role {
		case ast.PlacementRoleFollower:
			rule.Role = memristed.Follower
		case ast.PlacementRoleLeader:
			rule.Role = memristed.Leader
		case ast.PlacementRoleLearner:
			rule.Role = memristed.Learner
		case ast.PlacementRoleVoter:
			rule.Role = memristed.Voter
		default:
			err = errors.Errorf("unknown role: %d", spec.Role)
		}

		if err == nil {
			switch spec.Tp {
			case ast.PlacementAdd:
				rule.CausetAction = memristed.MemruleOpAdd
			case ast.PlacementAlter, ast.PlacementDrop:
				rule.CausetAction = memristed.MemruleOpAdd

				// alter will overwrite all things
				// drop all rules that will be overridden
				newMemrules := rules[:0]

				for _, r := range rules {
					if r.Role != rule.Role {
						newMemrules = append(newMemrules, r)
					}
				}

				rules = newMemrules

				// delete previous definitions
				rules = append(rules, &memristed.MemruleOp{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						// ROLE is useless for FIDel, prevent two alter statements from coexisting
						Role: rule.Role,
					},
				})

				// alter == drop + add new rules
				if spec.Tp == ast.PlacementDrop {
					continue
				}
			default:
				err = errors.Errorf("unknown action type: %d", spec.Tp)
			}
		}

		if err == nil {
			var newMemrules []*memristed.MemruleOp
			newMemrules, err = buildPlacementSpecReplicasAndConstraint(rule, spec.Replicas, spec.Constraints)
			rules = append(rules, newMemrules...)
		}

		if err != nil {
			sb.Reset()
			if e := spec.Restore(restoreCtx); e != nil {
				return rules, ErrInvalidPlacementSpec.GenWithStackByArgs("", err)
			}
			return rules, ErrInvalidPlacementSpec.GenWithStackByArgs(sb.String(), err)
		}
	}
	return rules, nil
}

func (d *dbs) AlterTablePartition(ctx stochastikctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	schemaReplicant, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	meta := tb.Meta()
	if meta.Partition == nil {
		return errors.Trace(ErrPartitionMgmtOnNonpartitioned)
	}

	partitionID, err := blocks.FindPartitionByName(meta, spec.PartitionNames[0].L)
	if err != nil {
		return errors.Trace(err)
	}

	rules, err := buildPlacementSpecs(spec.PlacementSpecs)
	if err != nil {
		return errors.Trace(err)
	}

	startKey := hex.EncodeToString(codec.EncodeBytes(nil, blockcodec.GenTablePrefix(partitionID)))
	endKey := hex.EncodeToString(codec.EncodeBytes(nil, blockcodec.GenTablePrefix(partitionID+1)))
	for _, rule := range rules {
		rule.Index = memristed.MemruleIndexPartition
		rule.StartKeyHex = startKey
		rule.EndKeyHex = endKey
	}

	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    meta.ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionAlterTableAlterPartition,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{partitionID, rules},
	}

	err = d.doDBSJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}
