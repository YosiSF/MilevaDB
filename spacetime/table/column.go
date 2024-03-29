//INTERLOCKyright 2020 WHTCORPS INC

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

package table

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/whtcorpsinc/parser"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/ast"
	"charset"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/BerolinaSQL/serial"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/BerolinaSQL/mysql"
	field_types "github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/causetnetnetctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/causetnetctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/types/json"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/util/hack"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/util/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/util/timeutil"
	"go.uber.org/zap"
)

// Column provides meta data describing a table column.
type Column struct {
	*serial.ColumnInfo
	// If this column is a generated column, the expression will be stored here.
	GeneratedExpr ast.ExprNode
	// If this column has default expr value, this expression will be stored here.
	DefaultExpr ast.ExprNode
}

// String implements fmt.Stringer interface.
func (c *Column) String() string {
	ans := []string{c.Name.O, types.TypeToStr(c.Tp, c.Charset)}
	if mysql.HasAutoIncrementFlag(c.Flag) {
		ans = append(ans, "AUTO_INCREMENT")
	}
	if mysql.HasNotNullFlag(c.Flag) {
		ans = append(ans, "NOT NULL")
	}
	return strings.Join(ans, " ")
}

// ToInfo casts Column to serial.ColumnInfo
// NOTE: DONT modify return value.
func (c *Column) ToInfo() *serial.ColumnInfo {
	return c.ColumnInfo
}

// FindCol finds column in cols by name.
func FindCol(cols []*Column, name string) *Column {
	for _, col := range cols {
		if strings.EqualFold(col.Name.O, name) {
			return col
		}
	}
	return nil
}

// ToColumn converts a *serial.ColumnInfo to *Column.
func ToColumn(col *serial.ColumnInfo) *Column {
	return &Column{
		col,
		nil,
		nil,
	}
}

// FindCols finds columns in cols by names.
// If pkIsHandle is false and name is ExtraHandleName, the extra handle column will be added.
// If any columns don't match, return nil and the first missing column's name
func FindCols(cols []*Column, names []string, pkIsHandle bool) ([]*Column, string) {
	var rcols []*Column
	for _, name := range names {
		col := FindCol(cols, name)
		if col != nil {
			rcols = append(rcols, col)
		} else if name == serial.ExtraHandleName.L && !pkIsHandle {
			col := &Column{}
			col.ColumnInfo = serial.NewExtraHandleColInfo()
			col.ColumnInfo.Offset = len(cols)
			rcols = append(rcols, col)
		} else {
			return nil, name
		}
	}

	return rcols, ""
}

// FindOnUpdateCols finds columns which have OnUpdateNow flag.
func FindOnUpdateCols(cols []*Column) []*Column {
	var rcols []*Column
	for _, col := range cols {
		if mysql.HasOnUpdateNowFlag(col.Flag) {
			rcols = append(rcols, col)
		}
	}

	return rcols
}



func truncateTrailingSpaces(v *types.CausetObjectQL) {
	if v.Kind() == types.KindNull {
		return
	}
	b := v.GetBytes()
	length := len(b)
	for length > 0 && b[length-1] == ' ' {
		length--
	}
	b = b[:length]
	str := string(hack.String(b))
	v.SetString(str, v.Collation())
}

// CastValues casts values based on columns type.
func CastValues(ctx causetnetctx.contextctx, rec []types.CausetObjectQL, cols []*Column) (err error) {
	sc := ctx.GetCausetNetVars().StmtCtx
	for _, c := range cols {
		var converted types.CausetObjectQL
		converted, err = CastValue(ctx, rec[c.Offset], c.ToInfo())
		if err != nil {
			if sc.DupKeyAsWarning {
				sc.AppendWarning(err)
				logutil.BgLogger().Warn("CastValues failed", zap.Error(err))
			} else {
				return err
			}
		}
		rec[c.Offset] = converted
	}
	return nil
}

func handleWrongUtf8Value(ctx causetnetctx.contextctx, col *serial.ColumnInfo, casted *types.CausetObjectQL, str string, i int) (types.CausetObjectQL, error) {
	sc := ctx.GetCausetNetVars().StmtCtx
	err := ErrTruncatedWrongValueForField.FastGen("incorrect utf8 value %x(%s) for column %s", casted.GetBytes(), str, col.Name)
	logutil.BgLogger().Error("incorrect UTF-8 value", zap.Uint64("conn", ctx.GetCausetNetVars().ConnectionID), zap.Error(err))
	// Truncate to valid utf8 string.
	truncateVal := types.NewStringCausetObjectQL(str[:i])
	err = sc.HandleTruncate(err)
	return truncateVal, err
}

func CastValue(ctx causetnetctx.contextctx, val types.CausetObjectQL, col *serial.ColumnInfo) (casted types.CausetObjectQL, err error) {
	sc := ctx.GetCausetNetVars().StmtCtx
	casted, err = val.ConvertTo(sc, &col.FieldType)
	// TODO: make sure all truncate errors are handled by ConvertTo.
	if types.ErrTruncated.Equal(err) {
		str, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Warn("CausetObjectQL ToString failed", zap.Stringer("CausetObjectQL", val), zap.Error(err1))
		}
		err = sc.HandleTruncate(types.ErrTruncatedWrongVal.GenWithStackByArgs(col.FieldType.CompactStr(), str))
	} else {
		err = sc.HandleTruncate(err)
	}
	if err != nil {
		return casted, err
	}

	if col.Tp == mysql.TypeString && !types.IsBinaryStr(&col.FieldType) {
		truncateTrailingSpaces(&casted)
	}

	if ctx.GetCausetNetVars().SkipUTF8Check {
		return casted, nil
	}
	if !mysql.IsUTF8Charset(col.Charset) {
		return casted, nil
	}
	str := casted.GetString()
	utf8Charset := col.Charset == mysql.UTF8Charset
	doMB4CharCheck := utf8Charset && config.GetGlobalConfig().CheckMb4ValueInUTF8
	for i, w := 0, 0; i < len(str); i += w {
		runeValue, width := utf8.DecodeRuneInString(str[i:])
		if runeValue == utf8.RuneError {
			if strings.HasPrefix(str[i:], string(utf8.RuneError)) {
				w = width
				continue
			}
			casted, err = handleWrongUtf8Value(ctx, col, &casted, str, i)
			break
		} else if width > 3 && doMB4CharCheck {
			// Handle non-BMP characters.
			casted, err = handleWrongUtf8Value(ctx, col, &casted, str, i)
			break
		}
		w = width
	}

	return casted, err
}

// ColDesc describes column information like MySQL desc and show columns do.
type ColDesc struct {
	Field string
	Type  string
	// Charset is nil if the column doesn't have a charset, or a string indicating the charset name.
	Charset interface{}
	// Collation is nil if the column doesn't have a collation, or a string indicating the collation name.
	Collation    interface{}
	Null         string
	Key          string
	DefaultValue interface{}
	Extra        string
	Privileges   string
	Comment      string
}

const defaultPrivileges = "select,insert,update,references"

// NewColDesc returns a new ColDesc for a column.
func NewColDesc(col *Column) *ColDesc {

	// create table
	name := col.Name
	nullFlag := "YES"
	if mysql.HasNotNullFlag(col.Flag) {
		nullFlag = "NO"
	}
	keyFlag := ""
	if mysql.HasPriKeyFlag(col.Flag) {
		keyFlag = "PRI"
	} else if mysql.HasUniKeyFlag(col.Flag) {
		keyFlag = "UNI"
	} else if mysql.HasMultipleKeyFlag(col.Flag) {
		keyFlag = "MUL"
	}
	var defaultValue interface{}
	if !mysql.HasNoDefaultValueFlag(col.Flag) {
		defaultValue = col.GetDefaultValue()
		if defaultValStr, ok := defaultValue.(string); ok {
			if (col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime) &&
				strings.EqualFold(defaultValStr, ast.CurrentTimestamp) &&
				col.Decimal > 0 {
				defaultValue = fmt.Sprintf("%s(%d)", defaultValStr, col.Decimal)
			}
		}
	}
