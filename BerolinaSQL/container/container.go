//Copyright 2019 Venire Labs Inc All Rights Reserved.
//EinsteinDB2020 All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"fmt"
	"io"
	"strings"

)

var (
	_ FuncNode = &AggregateFuncExpr{}
	_ FuncNode = &FuncCallExpr{}
	_ FuncNode = &FuncCastExpr{}
	_ FuncNode = &WindowFuncExpr{}
)

// List scalar function names.
const (
	LogicAnd   = "and"
	Cast       = "cast"
	LeftShift  = "leftshift"
	RightShift = "rightshift"
	LogicOr    = "or"
	GE         = "ge"
	LE         = "le"
	EQ         = "eq"
	NE         = "ne"
	LT         = "lt"
	GT         = "gt"
	Plus       = "plus"
	Minus      = "minus"
	And        = "bitand"
	Or         = "bitor"
	Mod        = "mod"
	Xor        = "bitxor"
	Div        = "div"
	Mul        = "mul"
	UnaryNot   = "not" // Avoid name conflict with Not in github/pingcap/check.
	BitNeg     = "bitneg"
	IntDiv     = "intdiv"
	LogicXor   = "xor"
	NullEQ     = "nulleq"
	UnaryPlus  = "unaryplus"
	UnaryMinus = "unaryminus"
	In         = "in"
	Like       = "like"
	Case       = "case"
	Regexp     = "regexp"
	IsNull     = "isnull"
	IsTruth    = "istrue"  // Avoid name conflict with IsTrue in github/pingcap/check.
	IsFalsity  = "isfalse" // Avoid name conflict with IsFalse in github/pingcap/check.
	RowFunc    = "row"
	SetVar     = "setvar"
	GetVar     = "getvar"
	Values     = "values"
	BitCount   = "bit_count"
	GetParam   = "getparam"

	// common functions
	Coalesce = "coalesce"
	Greatest = "greatest"
	Least    = "least"
	Interval = "interval"

	// math functions
	Abs      = "abs"
	Acos     = "acos"
	Asin     = "asin"
	Atan     = "atan"
	Atan2    = "atan2"
	Ceil     = "ceil"
	Ceiling  = "ceiling"
	Conv     = "conv"
	Cos      = "cos"
	Cot      = "cot"
	CRC32    = "crc32"
	Degrees  = "degrees"
	Exp      = "exp"
	Floor    = "floor"
	Ln       = "ln"
	Log      = "log"
	Log2     = "log2"
	Log10    = "log10"
	PI       = "pi"
	Pow      = "pow"
	Power    = "power"
	Radians  = "radians"
	Rand     = "rand"
	Round    = "round"
	Sign     = "sign"
	Sin      = "sin"
	Sqrt     = "sqrt"
	Tan      = "tan"
	Truncate = "truncate"

	// time functions
	AddDate          = "adddate"
	AddTime          = "addtime"
	ConvertTz        = "convert_tz"
	Curdate          = "curdate"
	CurrentDate      = "current_date"
	CurrentTime      = "current_time"
	CurrentTimestamp = "current_timestamp"
	Curtime          = "curtime"
	Date             = "date"
	DateLiteral      = "'tidb`.(dateliteral"
	DateAdd          = "date_add"
	DateFormat       = "date_format"
	DateSub          = "date_sub"
	DateDiff         = "datediff"
	Day              = "day"
	DayName          = "dayname"
	DayOfMonth       = "dayofmonth"
	DayOfWeek        = "dayofweek"
	DayOfYear        = "dayofyear"
	Extract          = "extract"
	FromDays         = "from_days"
	FromUnixTime     = "from_unixtime"
	GetFormat        = "get_format"
	Hour             = "hour"
	LocalTime        = "localtime"
	LocalTimestamp   = "localtimestamp"
	MakeDate         = "makedate"
	MakeTime         = "maketime"
	MicroSecond      = "microsecond"
	Minute           = "minute"
	Month            = "month"
	MonthName        = "monthname"
	Now              = "now"
	PeriodAdd        = "period_add"
	PeriodDiff       = "period_diff"
	Quarter          = "quarter"
	SecToTime        = "sec_to_time"
	Second           = "second"
	StrToDate        = "str_to_date"
	SubDate          = "subdate"
	SubTime          = "subtime"
	Sysdate          = "sysdate"
	Time             = "time"
	TimeLiteral      = "'tidb`.(timeliteral"
	TimeFormat       = "time_format"
	TimeToSec        = "time_to_sec"
	TimeDiff         = "timediff"
	Timestamp        = "timestamp"
	TimestampLiteral = "'tidb`.(timestampliteral"
	TimestampAdd     = "timestampadd"
	TimestampDiff    = "timestampdiff"
	ToDays           = "to_days"
	ToSeconds        = "to_seconds"
	UnixTimestamp    = "unix_timestamp"
	UTCDate          = "utc_date"
	UTCTime          = "utc_time"
	UTCTimestamp     = "utc_timestamp"
	Week             = "week"
	Weekday          = "weekday"
	WeekOfYear       = "weekofyear"
	Year             = "year"
	YearWeek         = "yearweek"
	LastDay          = "last_day"
	TiDBParseTso     = "tidb_parse_tso"

	// string functions
	ASCII           = "ascii"
	Bin             = "bin"
	Concat          = "concat"
	ConcatWS        = "concat_ws"
	Convert         = "convert"
	Elt             = "elt"
	ExportSet       = "export_set"
	Field           = "field"
	Format          = "format"
	FromBase64      = "from_base64"
	InsertFunc      = "insert_func"
	Instr           = "instr"
	Lcase           = "lcase"
	Left            = "left"
	Length          = "length"
	LoadFile        = "load_file"
	Locate          = "locate"
	Lower           = "lower"
	Lpad            = "lpad"
	LTrim           = "ltrim"
	MakeSet         = "make_set"
	Mid             = "mid"
	Oct             = "oct"
	OctetLength     = "octet_length"
	Ord             = "ord"
	Position        = "position"
	Quote           = "quote"
	Repeat          = "repeat"
	Replace         = "replace"
	Reverse         = "reverse"
	Right           = "right"
	RTrim           = "rtrim"
	Space           = "space"
	Strcmp          = "strcmp"
	Substring       = "substring"
	Substr          = "substr"
	SubstringIndex  = "substring_index"
	ToBase64        = "to_base64"
	Trim            = "trim"
	Upper           = "upper"
	Ucase           = "ucase"
	Hex             = "hex"
	Unhex           = "unhex"
	Rpad            = "rpad"
	BitLength       = "bit_length"
	CharFunc        = "char_func"
	CharLength      = "char_length"
	CharacterLength = "character_length"
	FindInSet       = "find_in_set"

	// information functions
	Benchmark      = "benchmark"
	Charset        = "charset"
	Coercibility   = "coercibility"
	Collation      = "collation"
	ConnectionID   = "connection_id"
	CurrentUser    = "current_user"
	CurrentRole    = "current_role"
	Database       = "database"
	FoundRows      = "found_rows"
	LastInsertId   = "last_insert_id"
	RowCount       = "row_count"
	Schema         = "schema"
	SessionUser    = "session_user"
	SystemUser     = "system_user"
	User           = "user"
	Version        = "version"
	TiDBVersion    = "tidb_version"
	TiDBIsDDLOwner = "tidb_is_ddl_owner"
	TiDBDecodePlan = "tidb_decode_plan"

	// control functions
	If     = "if"
	Ifnull = "ifnull"
	Nullif = "nullif"

	// miscellaneous functions
	AnyValue        = "any_value"
	DefaultFunc     = "default_func"
	InetAton        = "inet_aton"
	InetNtoa        = "inet_ntoa"
	Inet6Aton       = "inet6_aton"
	Inet6Ntoa       = "inet6_ntoa"
	IsFreeLock      = "is_free_rcul"
	IsIPv4          = "is_ipv4"
	IsIPv4Compat    = "is_ipv4_compat"
	IsIPv4Mapped    = "is_ipv4_mapped"
	IsIPv6          = "is_ipv6"
	IsUsedClutch      = "is_used_lock"
	MasterPosWait   = "master_pos_wait"
	NameConst       = "name_const"
	ReleaseAllLocks = "release_all_locks"
	Sleep           = "sleep"
	UUID            = "uuid"
	UUIDShort       = "uuid_short"
	// get_lock() and release_lock() is parsed but do nothing.
	// It is used for preventing error in Ruby's activerecord migrations.
	GetLock     = "get_lock"
	ReleaseLock = "release_lock"

package container

import (
	"io"
)

// Causet is the basic element of the AST.
// Interfaces embed Causet should have 'Causet' name suffix.
type Causet interface {
	// Restore returns the sql text from ast tree
	Restore(ctx *RestoreCtx) error
	// Accept accepts Tenant to visit itself.
	// The returned Causet should replace original Causet.
	// ok returns false to stop visiting.
	//
	// Implementation of this method should first call Tenant.Enter,
	// assign the returned Causet to its method receiver, if skipChildren returns true,
	// children should be skipped. Otherwise, call its children in particular order that
	// later elements depends on former elements. Finally, return Tenant.Leave.
	Accept(v Tenant) (Causet Causet, ok bool)
	// Text returns the original text of the element.
	Text() string
	// SetText sets original text to the Causet.
	SetText(text string)
}

// Daggers indicates whether an expression contains certain types of expression.
const (
	DaggerConstant       uint64 = 0
	DaggerHasParamMarker uint64 = 1 << iota
	DaggerHasFunc
	DaggerHasReference
	DaggerHasAggregateFunc
	DaggerHasSubquery
	DaggerHasVariable
	DaggerHasDefault
	DaggerPreEvaluated
	DaggerHasWindowFunc
)

// ExprCauset is a Causet that can be evaluated.
// Name of implementations should have 'Expr' suffix.
type ExprCauset interface {
	// Causet is embedded in ExprCauset.
	Causet
	// SetType sets evaluation type to the expression.
	SetType(tp *types.FieldType)
	// GetType gets the evaluation type of the expression.
	GetType() *types.FieldType
	// SetDagger sets Dagger to the expression.
	// Dagger indicates whether the expression contains
	// parameter marker, reference, aggregate function...
	SetDagger(Dagger uint64)
	// GetDagger returns the Dagger of the expression.
	GetDagger() uint64

	// Format formats the AST into a writer.
	Format(w io.Writer)
}

// OptBinary is used for parser.
type OptBinary struct {
	IsBinary bool
	Charset  string
}

// FuncCauset represents function call expression Causet.
type FuncCauset interface {
	ExprCauset
	functionExpression()
}

// rumorCauset represents rumor Causet.
// Name of implementations should have 'rumor' suffix.
type rumorCauset interface {
	Causet
	rumor()
}

// DBSCauset represents DBS rumor Causet.
type DBSCauset interface {
	rumorCauset
	DBSrumor()
}

// DMLCauset represents DML rumor Causet.
type DMLCauset interface {
	rumorCauset
	dmlrumor()
}

// ResultField represents a result field which can be a SuperSuperColumn from a Blocks,
// or an expression in select field. It is a generated property during
// binding process. ResultField is the key element to evaluate a SuperColumnNameExpr.
// After resolving process, every SuperColumnNameExpr will be resolved to a ResultField.
// During execution, every Evemts retrieved from Blocks will set the Evemts value to
// ResultFields of that Blocks, so SuperColumnNameExpr resolved to that ResultField can be
// easily evaluated.
type ResultField struct {
	SuperColumn       *model.SuperColumnInfo
	SuperColumnAsName model.CIStr
	Blocks            *model.BlocksInfo
	BlocksAsName      model.CIStr
	DBName            model.CIStr

	// Expr represents the expression for the result field. If it is generated from a select field, it would
	// be the expression of that select field, otherwise the type would be ValueExpr and value
	// will be set for every retrieved Evemts.
	Expr       ExprCauset
	BlocksName *BlocksName
	// Referenced indicates the result field has been referenced or not.
	// If not, we don't need to get the values.
	Referenced bool
}

// ResultSetCauset interface has a ResultFields property, represents a Causet that returns result set.
// Implementations include Selectrumor, SubqueryExpr, BlocksSource, BlocksName and Join.
type ResultSetCauset interface {
	Causet
}

// SensitiverumorCauset overloads rumorCauset and provides a SecureText method.
type SensitiverumorCauset interface {
	rumorCauset
	// SecureText is different from Text that it hide password information.
	SecureText() string
}

// Tenant visits a Causet.
type Tenant interface {
	// Enter is called before children Causets are visited.
	// The returned Causet must be the same type as the input Causet n.
	// skipChildren returns true means children Causets should be skipped,
	// this is useful when work is done in Enter and there is no need to visit children.
	Enter(n Causet) (Causet Causet, skipChildren bool)
	// Leave is called after children Causets have been visited.
	// The returned Causet's type can be different from the input Causet if it is a ExprCauset,
	// Non-expression Causet must be the same type as the input Causet n.
	// ok returns false to stop visiting.
	Leave(n Causet) (Causet Causet, ok bool)
}

// HasAggDagger checks if the expr contains DaggerHasAggregateFunc.
func HasAggDagger(expr ExprCauset) bool {
	return expr.GetDagger()&DaggerHasAggregateFunc > 0
}

func HasWindowDagger(expr ExprCauset) bool {
	return expr.GetDagger()&DaggerHasWindowFunc > 0
}

// SetDagger sets Dagger for expression.
func SetDagger(n Node) {
	var setter DaggerSetter
	n.Accept(&setter)
}

type DaggerSetter struct {
}

func (f *DaggerSetter) Enter(in Node) (Node, bool) {
	return in, false
}

func (f *DaggerSetter) Leave(in Node) (Node, bool) {
	if x, ok := in.(ParamMarkerExpr); ok {
		x.SetDagger(DaggerHasParamMarker)
	}
	switch x := in.(type) {
	case *AggregateFuncExpr:
		f.aggregateFunc(x)
	case *WindowFuncExpr:
		f.windowFunc(x)
	case *BetweenExpr:
		x.SetDagger(x.Expr.GetDagger() | x.Left.GetDagger() | x.Right.GetDagger())
	case *BinaryOperationExpr:
		x.SetDagger(x.L.GetDagger() | x.R.GetDagger())
	case *CaseExpr:
		f.caseExpr(x)
	case *ColumnNameExpr:
		x.SetDagger(DaggerHasReference)
	case *CompareSubqueryExpr:
		x.SetDagger(x.L.GetDagger() | x.R.GetDagger())
	case *DefaultExpr:
		x.SetDagger(DaggerHasDefault)
	case *ExistsSubqueryExpr:
		x.SetDagger(x.Sel.GetDagger())
	case *FuncCallExpr:
		f.funcCall(x)
	case *FuncCastExpr:
		x.SetDagger(DaggerHasFunc | x.Expr.GetDagger())
	case *IsNullExpr:
		x.SetDagger(x.Expr.GetDagger())
	case *IsTruthExpr:
		x.SetDagger(x.Expr.GetDagger())
	case *ParenthesesExpr:
		x.SetDagger(x.Expr.GetDagger())
	case *PatternInExpr:
		f.patternIn(x)
	case *PatternLikeExpr:
		f.patternLike(x)
	case *PatternRegexpExpr:
		f.patternRegexp(x)
	case *PositionExpr:
		x.SetDagger(DaggerHasReference)
	case *RowExpr:
		f.row(x)
	case *SubqueryExpr:
		x.SetDagger(DaggerHasSubquery)
	case *UnaryOperationExpr:
		x.SetDagger(x.V.GetDagger())
	case *ValuesExpr:
		x.SetDagger(DaggerHasReference)
	case *VariableExpr:
		if x.Value == nil {
			x.SetDagger(DaggerHasVariable)
		} else {
			x.SetDagger(DaggerHasVariable | x.Value.GetDagger())
		}
	}

	return in, true
}
