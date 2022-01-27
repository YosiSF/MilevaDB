// INTERLOCKyright 2020 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package block

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	field_types "github.com/whtcorpsinc/berolinaAllegroSQL/types"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	"go.uber.org/zap"
)

// DeferredCauset provides meta data describing a block defCausumn.
type DeferredCauset struct {
	*perceptron.DeferredCausetInfo
	// If this defCausumn is a generated defCausumn, the expression will be stored here.
	GeneratedExpr ast.ExprNode
	// If this defCausumn has default expr value, this expression will be stored here.
	DefaultExpr ast.ExprNode
}

// String implements fmt.Stringer interface.
func (c *DeferredCauset) String() string {
	ans := []string{c.Name.O, types.TypeToStr(c.Tp, c.Charset)}
	if allegrosql.HasAutoIncrementFlag(c.Flag) {
		ans = append(ans, "AUTO_INCREMENT")
	}
	if allegrosql.HasNotNullFlag(c.Flag) {
		ans = append(ans, "NOT NULL")
	}
	return strings.Join(ans, " ")
}

// ToInfo casts DeferredCauset to perceptron.DeferredCausetInfo
// NOTE: DONT modify return value.
func (c *DeferredCauset) ToInfo() *perceptron.DeferredCausetInfo {
	return c.DeferredCausetInfo
}

// FindDefCaus finds defCausumn in defcaus by name.
func FindDefCaus(defcaus []*DeferredCauset, name string) *DeferredCauset {
	for _, defCaus := range defcaus {
		if strings.EqualFold(defCaus.Name.O, name) {
			return defCaus
		}
	}
	return nil
}

// ToDeferredCauset converts a *perceptron.DeferredCausetInfo to *DeferredCauset.
func ToDeferredCauset(defCaus *perceptron.DeferredCausetInfo) *DeferredCauset {
	return &DeferredCauset{
		defCaus,
		nil,
		nil,
	}
}

// FindDefCauss finds defCausumns in defcaus by names.
// If pkIsHandle is false and name is ExtraHandleName, the extra handle defCausumn will be added.
// If any defCausumns don't match, return nil and the first missing defCausumn's name
func FindDefCauss(defcaus []*DeferredCauset, names []string, pkIsHandle bool) ([]*DeferredCauset, string) {
	var rdefcaus []*DeferredCauset
	for _, name := range names {
		defCaus := FindDefCaus(defcaus, name)
		if defCaus != nil {
			rdefcaus = append(rdefcaus, defCaus)
		} else if name == perceptron.ExtraHandleName.L && !pkIsHandle {
			defCaus := &DeferredCauset{}
			defCaus.DeferredCausetInfo = perceptron.NewExtraHandleDefCausInfo()
			defCaus.DeferredCausetInfo.Offset = len(defcaus)
			rdefcaus = append(rdefcaus, defCaus)
		} else {
			return nil, name
		}
	}

	return rdefcaus, ""
}

// FindOnUFIDelateDefCauss finds defCausumns which have OnUFIDelateNow flag.
func FindOnUFIDelateDefCauss(defcaus []*DeferredCauset) []*DeferredCauset {
	var rdefcaus []*DeferredCauset
	for _, defCaus := range defcaus {
		if allegrosql.HasOnUFIDelateNowFlag(defCaus.Flag) {
			rdefcaus = append(rdefcaus, defCaus)
		}
	}

	return rdefcaus
}

// truncateTrailingSpaces truncates trailing spaces for CHAR[(M)] defCausumn.
// fix: https://github.com/whtcorpsinc/milevadb/issues/3660
func truncateTrailingSpaces(v *types.Causet) {
	if v.HoTT() == types.HoTTNull {
		return
	}
	b := v.GetBytes()
	length := len(b)
	for length > 0 && b[length-1] == ' ' {
		length--
	}
	b = b[:length]
	str := string(replog.String(b))
	v.SetString(str, v.DefCauslation())
}

func handleWrongASCIIValue(ctx stochastikctx.Context, defCaus *perceptron.DeferredCausetInfo, casted *types.Causet, str string, i int) (types.Causet, error) {
	sc := ctx.GetStochastikVars().StmtCtx
	err := ErrTruncatedWrongValueForField.FastGen("incorrect ascii value %x(%s) for defCausumn %s", casted.GetBytes(), str, defCaus.Name)
	logutil.BgLogger().Error("incorrect ASCII value", zap.Uint64("conn", ctx.GetStochastikVars().ConnectionID), zap.Error(err))
	truncateVal := types.NewStringCauset(str[:i])
	err = sc.HandleTruncate(err)
	return truncateVal, err
}

func handleWrongUtf8Value(ctx stochastikctx.Context, defCaus *perceptron.DeferredCausetInfo, casted *types.Causet, str string, i int) (types.Causet, error) {
	sc := ctx.GetStochastikVars().StmtCtx
	err := ErrTruncatedWrongValueForField.FastGen("incorrect utf8 value %x(%s) for defCausumn %s", casted.GetBytes(), str, defCaus.Name)
	logutil.BgLogger().Error("incorrect UTF-8 value", zap.Uint64("conn", ctx.GetStochastikVars().ConnectionID), zap.Error(err))
	// Truncate to valid utf8 string.
	truncateVal := types.NewStringCauset(str[:i])
	err = sc.HandleTruncate(err)
	return truncateVal, err
}

// CastValue casts a value based on defCausumn type.
// If forceIgnoreTruncate is true, truncated errors will be ignored.
// If returnOverflow is true, don't handle overflow errors in this function.
// It's safe now and it's the same as the behavior of select statement.
// Set it to true only in FillVirtualDeferredCausetValue and UnionScanExec.Next()
// If the handle of err is changed latter, the behavior of forceIgnoreTruncate also need to change.
// TODO: change the third arg to TypeField. Not pass DeferredCausetInfo.
func CastValue(ctx stochastikctx.Context, val types.Causet, defCaus *perceptron.DeferredCausetInfo, returnOverflow, forceIgnoreTruncate bool) (casted types.Causet, err error) {
	sc := ctx.GetStochastikVars().StmtCtx
	casted, err = val.ConvertTo(sc, &defCaus.FieldType)
	// TODO: make sure all truncate errors are handled by ConvertTo.
	if returnOverflow && types.ErrOverflow.Equal(err) {
		return casted, err
	}
	if err != nil && types.ErrTruncated.Equal(err) && defCaus.Tp != allegrosql.TypeSet && defCaus.Tp != allegrosql.TypeEnum {
		str, err1 := val.ToString()
		if err1 != nil {
			logutil.BgLogger().Warn("Causet ToString failed", zap.Stringer("Causet", val), zap.Error(err1))
		}
		err = sc.HandleTruncate(types.ErrTruncatedWrongVal.GenWithStackByArgs(defCaus.FieldType.CompactStr(), str))
	} else {
		err = sc.HandleTruncate(err)
	}
	if forceIgnoreTruncate {
		err = nil
	} else if err != nil {
		return casted, err
	}

	if defCaus.Tp == allegrosql.TypeString && !types.IsBinaryStr(&defCaus.FieldType) {
		truncateTrailingSpaces(&casted)
	}

	if defCaus.Charset == charset.CharsetASCII {
		if ctx.GetStochastikVars().SkipASCIICheck {
			return casted, nil
		}

		str := casted.GetString()
		for i := 0; i < len(str); i++ {
			if str[i] > unicode.MaxASCII {
				casted, err = handleWrongASCIIValue(ctx, defCaus, &casted, str, i)
				break
			}
		}
		if forceIgnoreTruncate {
			err = nil
		}
		return casted, err
	}

	if ctx.GetStochastikVars().SkipUTF8Check {
		return casted, nil
	}

	if !allegrosql.IsUTF8Charset(defCaus.Charset) {
		return casted, nil
	}
	str := casted.GetString()
	utf8Charset := defCaus.Charset == allegrosql.UTF8Charset
	doMB4CharCheck := utf8Charset && config.GetGlobalConfig().CheckMb4ValueInUTF8
	for i, w := 0, 0; i < len(str); i += w {
		runeValue, width := utf8.DecodeRuneInString(str[i:])
		if runeValue == utf8.RuneError {
			if strings.HasPrefix(str[i:], string(utf8.RuneError)) {
				w = width
				continue
			}
			casted, err = handleWrongUtf8Value(ctx, defCaus, &casted, str, i)
			break
		} else if width > 3 && doMB4CharCheck {
			// Handle non-BMP characters.
			casted, err = handleWrongUtf8Value(ctx, defCaus, &casted, str, i)
			break
		}
		w = width
	}

	if forceIgnoreTruncate {
		err = nil
	}
	return casted, err
}

// DefCausDesc describes defCausumn information like MyALLEGROSQL desc and show defCausumns do.
type DefCausDesc struct {
	Field string
	Type  string
	// Charset is nil if the defCausumn doesn't have a charset, or a string indicating the charset name.
	Charset interface{}
	// DefCauslation is nil if the defCausumn doesn't have a defCauslation, or a string indicating the defCauslation name.
	DefCauslation interface{}
	Null          string
	Key           string
	DefaultValue  interface{}
	Extra         string
	Privileges    string
	Comment       string
}

const defaultPrivileges = "select,insert,uFIDelate,references"

// NewDefCausDesc returns a new DefCausDesc for a defCausumn.
func NewDefCausDesc(defCaus *DeferredCauset) *DefCausDesc {
	// TODO: if we have no primary key and a unique index which's defCausumns are all not null
	// we will set these defCausumns' flag as PriKeyFlag
	// see https://dev.allegrosql.com/doc/refman/5.7/en/show-defCausumns.html
	// create block
	name := defCaus.Name
	nullFlag := "YES"
	if allegrosql.HasNotNullFlag(defCaus.Flag) {
		nullFlag = "NO"
	}
	keyFlag := ""
	if allegrosql.HasPriKeyFlag(defCaus.Flag) {
		keyFlag = "PRI"
	} else if allegrosql.HasUniKeyFlag(defCaus.Flag) {
		keyFlag = "UNI"
	} else if allegrosql.HasMultipleKeyFlag(defCaus.Flag) {
		keyFlag = "MUL"
	}
	var defaultValue interface{}
	if !allegrosql.HasNoDefaultValueFlag(defCaus.Flag) {
		defaultValue = defCaus.GetDefaultValue()
		if defaultValStr, ok := defaultValue.(string); ok {
			if (defCaus.Tp == allegrosql.TypeTimestamp || defCaus.Tp == allegrosql.TypeDatetime) &&
				strings.EqualFold(defaultValStr, ast.CurrentTimestamp) &&
				defCaus.Decimal > 0 {
				defaultValue = fmt.Sprintf("%s(%d)", defaultValStr, defCaus.Decimal)
			}
		}
	}

	extra := ""
	if allegrosql.HasAutoIncrementFlag(defCaus.Flag) {
		extra = "auto_increment"
	} else if allegrosql.HasOnUFIDelateNowFlag(defCaus.Flag) {
		//in order to match the rules of allegrosql 8.0.16 version
		//see https://github.com/whtcorpsinc/milevadb/issues/10337
		extra = "DEFAULT_GENERATED on uFIDelate CURRENT_TIMESTAMP" + OptionalFsp(&defCaus.FieldType)
	} else if defCaus.IsGenerated() {
		if defCaus.GeneratedStored {
			extra = "STORED GENERATED"
		} else {
			extra = "VIRTUAL GENERATED"
		}
	}

	desc := &DefCausDesc{
		Field:         name.O,
		Type:          defCaus.GetTypeDesc(),
		Charset:       defCaus.Charset,
		DefCauslation: defCaus.DefCauslate,
		Null:          nullFlag,
		Key:           keyFlag,
		DefaultValue:  defaultValue,
		Extra:         extra,
		Privileges:    defaultPrivileges,
		Comment:       defCaus.Comment,
	}
	if !field_types.HasCharset(&defCaus.DeferredCausetInfo.FieldType) {
		desc.Charset = nil
		desc.DefCauslation = nil
	}
	return desc
}

// DefCausDescFieldNames returns the fields name in result set for desc and show defCausumns.
func DefCausDescFieldNames(full bool) []string {
	if full {
		return []string{"Field", "Type", "DefCauslation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	}
	return []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
}

// CheckOnce checks if there are duplicated defCausumn names in defcaus.
func CheckOnce(defcaus []*DeferredCauset) error {
	m := map[string]struct{}{}
	for _, defCaus := range defcaus {
		name := defCaus.Name
		_, ok := m[name.L]
		if ok {
			return errDuplicateDeferredCauset.GenWithStackByArgs(name)
		}

		m[name.L] = struct{}{}
	}

	return nil
}

// CheckNotNull checks if nil value set to a defCausumn with NotNull flag is set.
func (c *DeferredCauset) CheckNotNull(data *types.Causet) error {
	if (allegrosql.HasNotNullFlag(c.Flag) || allegrosql.HasPreventNullInsertFlag(c.Flag)) && data.IsNull() {
		return ErrDeferredCausetCantNull.GenWithStackByArgs(c.Name)
	}
	return nil
}

// HandleBadNull handles the bad null error.
// If BadNullAsWarning is true, it will append the error as a warning, else return the error.
func (c *DeferredCauset) HandleBadNull(d *types.Causet, sc *stmtctx.StatementContext) error {
	if err := c.CheckNotNull(d); err != nil {
		if sc.BadNullAsWarning {
			sc.AppendWarning(err)
			*d = GetZeroValue(c.ToInfo())
			return nil
		}
		return err
	}
	return nil
}

// IsPKHandleDeferredCauset checks if the defCausumn is primary key handle defCausumn.
func (c *DeferredCauset) IsPKHandleDeferredCauset(tbInfo *perceptron.BlockInfo) bool {
	return allegrosql.HasPriKeyFlag(c.Flag) && tbInfo.PKIsHandle
}

// IsCommonHandleDeferredCauset checks if the defCausumn is common handle defCausumn.
func (c *DeferredCauset) IsCommonHandleDeferredCauset(tbInfo *perceptron.BlockInfo) bool {
	return allegrosql.HasPriKeyFlag(c.Flag) && tbInfo.IsCommonHandle
}

// CheckNotNull checks if event has nil value set to a defCausumn with NotNull flag set.
func CheckNotNull(defcaus []*DeferredCauset, event []types.Causet) error {
	for _, c := range defcaus {
		if err := c.CheckNotNull(&event[c.Offset]); err != nil {
			return err
		}
	}
	return nil
}

// GetDefCausOriginDefaultValue gets default value of the defCausumn from original default value.
func GetDefCausOriginDefaultValue(ctx stochastikctx.Context, defCaus *perceptron.DeferredCausetInfo) (types.Causet, error) {
	// If the defCausumn type is BIT, both `OriginDefaultValue` and `DefaultValue` of DeferredCausetInfo are corrupted, because
	// after JSON marshaling and unmarshaling against the field with type `interface{}`, the content with actual type `[]byte` is changed.
	// We need `DefaultValueBit` to restore OriginDefaultValue before reading it.
	if defCaus.Tp == allegrosql.TypeBit && defCaus.DefaultValueBit != nil && defCaus.OriginDefaultValue != nil {
		defCaus.OriginDefaultValue = defCaus.DefaultValueBit
	}
	return getDefCausDefaultValue(ctx, defCaus, defCaus.OriginDefaultValue)
}

// GetDefCausDefaultValue gets default value of the defCausumn.
func GetDefCausDefaultValue(ctx stochastikctx.Context, defCaus *perceptron.DeferredCausetInfo) (types.Causet, error) {
	defaultValue := defCaus.GetDefaultValue()
	if !defCaus.DefaultIsExpr {
		return getDefCausDefaultValue(ctx, defCaus, defaultValue)
	}
	return getDefCausDefaultExprValue(ctx, defCaus, defaultValue.(string))
}

// EvalDefCausDefaultExpr eval default expr node to explicit default value.
func EvalDefCausDefaultExpr(ctx stochastikctx.Context, defCaus *perceptron.DeferredCausetInfo, defaultExpr ast.ExprNode) (types.Causet, error) {
	d, err := expression.EvalAstExpr(ctx, defaultExpr)
	if err != nil {
		return types.Causet{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastValue(ctx, d, defCaus, false, false)
	if err != nil {
		return types.Causet{}, err
	}
	return value, nil
}

func getDefCausDefaultExprValue(ctx stochastikctx.Context, defCaus *perceptron.DeferredCausetInfo, defaultValue string) (types.Causet, error) {
	var defaultExpr ast.ExprNode
	expr := fmt.Sprintf("select %s", defaultValue)
	stmts, _, err := berolinaAllegroSQL.New().Parse(expr, "", "")
	if err == nil {
		defaultExpr = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	d, err := expression.EvalAstExpr(ctx, defaultExpr)
	if err != nil {
		return types.Causet{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastValue(ctx, d, defCaus, false, false)
	if err != nil {
		return types.Causet{}, err
	}
	return value, nil
}

func getDefCausDefaultValue(ctx stochastikctx.Context, defCaus *perceptron.DeferredCausetInfo, defaultVal interface{}) (types.Causet, error) {
	if defaultVal == nil {
		return getDefCausDefaultValueFromNil(ctx, defCaus)
	}

	if defCaus.Tp != allegrosql.TypeTimestamp && defCaus.Tp != allegrosql.TypeDatetime {
		value, err := CastValue(ctx, types.NewCauset(defaultVal), defCaus, false, false)
		if err != nil {
			return types.Causet{}, err
		}
		return value, nil
	}

	// Check and get timestamp/datetime default value.
	sc := ctx.GetStochastikVars().StmtCtx
	var needChangeTimeZone bool
	// If the defCausumn's default value is not ZeroDatetimeStr nor CurrentTimestamp, should use the time zone of the default value itself.
	if defCaus.Tp == allegrosql.TypeTimestamp {
		if vv, ok := defaultVal.(string); ok && vv != types.ZeroDatetimeStr && !strings.EqualFold(vv, ast.CurrentTimestamp) {
			needChangeTimeZone = true
			originalTZ := sc.TimeZone
			// For defCaus.Version = 0, the timezone information of default value is already lost, so use the system timezone as the default value timezone.
			sc.TimeZone = timeutil.SystemLocation()
			if defCaus.Version >= perceptron.DeferredCausetInfoVersion1 {
				sc.TimeZone = time.UTC
			}
			defer func() { sc.TimeZone = originalTZ }()
		}
	}
	value, err := expression.GetTimeValue(ctx, defaultVal, defCaus.Tp, int8(defCaus.Decimal))
	if err != nil {
		return types.Causet{}, errGetDefaultFailed.GenWithStackByArgs(defCaus.Name)
	}
	// If the defCausumn's default value is not ZeroDatetimeStr or CurrentTimestamp, convert the default value to the current stochastik time zone.
	if needChangeTimeZone {
		t := value.GetMysqlTime()
		err = t.ConvertTimeZone(sc.TimeZone, ctx.GetStochastikVars().Location())
		if err != nil {
			return value, err
		}
		value.SetMysqlTime(t)
	}
	return value, nil
}

func getDefCausDefaultValueFromNil(ctx stochastikctx.Context, defCaus *perceptron.DeferredCausetInfo) (types.Causet, error) {
	if !allegrosql.HasNotNullFlag(defCaus.Flag) {
		return types.Causet{}, nil
	}
	if defCaus.Tp == allegrosql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		defEnum, err := types.ParseEnumValue(defCaus.FieldType.Elems, 1)
		if err != nil {
			return types.Causet{}, err
		}
		return types.NewDefCauslateMysqlEnumCauset(defEnum, defCaus.DefCauslate), nil
	}
	if allegrosql.HasAutoIncrementFlag(defCaus.Flag) {
		// Auto increment defCausumn doesn't has default value and we should not return error.
		return GetZeroValue(defCaus), nil
	}
	vars := ctx.GetStochastikVars()
	sc := vars.StmtCtx
	if sc.BadNullAsWarning {
		sc.AppendWarning(ErrDeferredCausetCantNull.FastGenByArgs(defCaus.Name))
		return GetZeroValue(defCaus), nil
	}
	if !vars.StrictALLEGROSQLMode {
		sc.AppendWarning(ErrNoDefaultValue.FastGenByArgs(defCaus.Name))
		return GetZeroValue(defCaus), nil
	}
	return types.Causet{}, ErrNoDefaultValue.FastGenByArgs(defCaus.Name)
}

// GetZeroValue gets zero value for given defCausumn type.
func GetZeroValue(defCaus *perceptron.DeferredCausetInfo) types.Causet {
	var d types.Causet
	switch defCaus.Tp {
	case allegrosql.TypeTiny, allegrosql.TypeInt24, allegrosql.TypeShort, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear:
		if allegrosql.HasUnsignedFlag(defCaus.Flag) {
			d.SetUint64(0)
		} else {
			d.SetInt64(0)
		}
	case allegrosql.TypeFloat:
		d.SetFloat32(0)
	case allegrosql.TypeDouble:
		d.SetFloat64(0)
	case allegrosql.TypeNewDecimal:
		d.SetLength(defCaus.Flen)
		d.SetFrac(defCaus.Decimal)
		d.SetMysqlDecimal(new(types.MyDecimal))
	case allegrosql.TypeString:
		if defCaus.Flen > 0 && defCaus.Charset == charset.CharsetBin {
			d.SetBytes(make([]byte, defCaus.Flen))
		} else {
			d.SetString("", defCaus.DefCauslate)
		}
	case allegrosql.TypeVarString, allegrosql.TypeVarchar:
		d.SetString("", defCaus.DefCauslate)
	case allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		d.SetBytes([]byte{})
	case allegrosql.TypeDuration:
		d.SetMysqlDuration(types.ZeroDuration)
	case allegrosql.TypeDate:
		d.SetMysqlTime(types.ZeroDate)
	case allegrosql.TypeTimestamp:
		d.SetMysqlTime(types.ZeroTimestamp)
	case allegrosql.TypeDatetime:
		d.SetMysqlTime(types.ZeroDatetime)
	case allegrosql.TypeBit:
		d.SetMysqlBit(types.ZeroBinaryLiteral)
	case allegrosql.TypeSet:
		d.SetMysqlSet(types.Set{}, defCaus.DefCauslate)
	case allegrosql.TypeEnum:
		d.SetMysqlEnum(types.Enum{}, defCaus.DefCauslate)
	case allegrosql.TypeJSON:
		d.SetMysqlJSON(json.CreateBinary(nil))
	}
	return d
}

// OptionalFsp convert a FieldType.Decimal to string.
func OptionalFsp(fieldType *types.FieldType) string {
	fsp := fieldType.Decimal
	if fsp == 0 {
		return ""
	}
	return "(" + strconv.Itoa(fsp) + ")"
}
