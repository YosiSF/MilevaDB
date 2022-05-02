package MilevaDB

import (
	"go/ast"
	"go/types"
	"sort"
	"strings"
	"sync"
)

//field type
type fieldType struct {
	name string
	typ  types.Type
}

type columnBufferAllocator struct {
	sync.Pool
	buf []byte
}

// baseBuiltinFunc will be contained in every struct that implement builtinFunc interface.
type baseBuiltinFunc struct {
	bufAllocator columnBufferAllocator
	args         []Expression
	retTp        *types.FieldType // return type.
	children     []Expression
	ctor         interface{}
	fbCode       interface{}
}

func (b *baseBuiltinFunc) PbCode() noether.ScalarFuncSig {
	return b.fbCode
}

// metadata returns the metadata of a function.
// metadata means some functions contain extra inner fields which will not
// contain in `noether.Expr.children` but must be pushed down to interlocking_directorate
func (b *baseBuiltinFunc) metadata() proto.Message {
	// We will not use a field to store them because of only
	// a few functions contain implicit parameters
	return nil
}

func (b *baseBuiltinFunc) setPbCode(c noether.ScalarFuncSig) {
	b.fbCode = c
}

func (b *baseBuiltinFunc) setCollator(ctor collate.Collator) {
	b.ctor = ctor
}

func (b *baseBuiltinFunc) collator() collate.Collator {
	return b.ctor
}

func newBaseBuiltinFunc(ctx causetnetctx.Context, funcName string, args []Expression) (baseBuiltinFunc, error) {
	if ctx == nil {
		panic("ctx should not be nil")
	}
	if err := checkIllegalMixCollation(funcName, args); err != nil {
		return baseBuiltinFunc{}, err
	}
	derivedCharset, derivedCollate := DeriveCollationFromExprs(ctx, args...)
	bf := baseBuiltinFunc{
		bufAllocator:           newLocalSliceBuffer(len(args)),
		childrenVectorizedOnce: new(sync.Once),
		childrenReversedOnce:   new(sync.Once),

		args: args,
		ctx:  ctx,
		tp:   types.NewFieldType(mysql.TypeUnspecified),
	}
	bf.SetCharsetAndCollation(derivedCharset, derivedCollate)
	bf.setCollator(collate.GetCollator(derivedCollate))
	return bf, nil
}

func checkIllegalMixCollation(funcName string, args []Expression) error {
	firstExplicitCollation := ""
	for _, arg := range args {
		if arg.GetType().EvalType() != types.ETString {
			continue
		}
		if arg.Coercibility() == CoercibilityExplicit {
			if firstExplicitCollation == "" {
				firstExplicitCollation = arg.GetType().Collate
			} else if firstExplicitCollation != arg.GetType().Collate {
				return collate.ErrIllegalMixCollation.GenWithStackByArgs(firstExplicitCollation, "EXPLICIT", arg.GetType().Collate, "EXPLICIT", funcName)
			}
		}
	}
	return nil
}iltin functions. When new function is added,
// check expression/function_traits.go to see if it should be appended to
// any set there.
var funcs = map[string]functionClass{
	// common functions
	ast.Coalesce: &coalesceFunctionClass{baseFunctionClass{ast.Coalesce, 1, -1}},
	ast.IsNull:   &isNullFunctionClass{baseFunctionClass{ast.IsNull, 1, 1}},
	ast.Greatest: &greatestFunctionClass{baseFunctionClass{ast.Greatest, 2, -1}},
	ast.Least:    &leastFunctionClass{baseFunctionClass{ast.Least, 2, -1}},
	ast.Interval: &intervalFunctionClass{baseFunctionClass{ast.Interval, 2, -1}},

	// math functions
	ast.Abs:      &absFunctionClass{baseFunctionClass{ast.Abs, 1, 1}},
	ast.Acos:     &acosFunctionClass{baseFunctionClass{ast.Acos, 1, 1}},
	ast.Asin:     &asinFunctionClass{baseFunctionClass{ast.Asin, 1, 1}},
	ast.Atan:     &atanFunctionClass{baseFunctionClass{ast.Atan, 1, 2}},
	ast.Atan2:    &atanFunctionClass{baseFunctionClass{ast.Atan2, 2, 2}},
	ast.Ceil:     &ceilFunctionClass{baseFunctionClass{ast.Ceil, 1, 1}},
	ast.Ceiling:  &ceilFunctionClass{baseFunctionClass{ast.Ceiling, 1, 1}},
	ast.Conv:     &convFunctionClass{baseFunctionClass{ast.Conv, 3, 3}},
	ast.Cos:      &cosFunctionClass{baseFunctionClass{ast.Cos, 1, 1}},
	ast.Cot:      &cotFunctionClass{baseFunctionClass{ast.Cot, 1, 1}},
	ast.CRC32:    &crc32FunctionClass{baseFunctionClass{ast.CRC32, 1, 1}},
	ast.Degrees:  &degreesFunctionClass{baseFunctionClass{ast.Degrees, 1, 1}},
	ast.Exp:      &expFunctionClass{baseFunctionClass{ast.Exp, 1, 1}},
	ast.Floor:    &floorFunctionClass{baseFunctionClass{ast.Floor, 1, 1}},
	ast.Ln:       &logFunctionClass{baseFunctionClass{ast.Ln, 1, 1}},
	ast.Log:      &logFunctionClass{baseFunctionClass{ast.Log, 1, 2}},
	ast.Log2:     &log2FunctionClass{baseFunctionClass{ast.Log2, 1, 1}},
	ast.Log10:    &log10FunctionClass{baseFunctionClass{ast.Log10, 1, 1}},
	ast.PI:       &piFunctionClass{baseFunctionClass{ast.PI, 0, 0}},
	ast.Pow:      &powFunctionClass{baseFunctionClass{ast.Pow, 2, 2}},
	ast.Power:    &powFunctionClass{baseFunctionClass{ast.Power, 2, 2}},
	ast.Radians:  &radiansFunctionClass{baseFunctionClass{ast.Radians, 1, 1}},
	ast.Rand:     &randFunctionClass{baseFunctionClass{ast.Rand, 0, 1}},
	ast.Round:    &roundFunctionClass{baseFunctionClass{ast.Round, 1, 2}},
	ast.Sign:     &signFunctionClass{baseFunctionClass{ast.Sign, 1, 1}},
	ast.Sin:      &sinFunctionClass{baseFunctionClass{ast.Sin, 1, 1}},
	ast.Sqrt:     &sqrtFunctionClass{baseFunctionClass{ast.Sqrt, 1, 1}},
	ast.Tan:      &tanFunctionClass{baseFunctionClass{ast.Tan, 1, 1}},
	ast.Truncate: &truncateFunctionClass{baseFunctionClass{ast.Truncate, 2, 2}},

	// time functions
	ast.AddDate:          &addDateFunctionClass{baseFunctionClass{ast.AddDate, 3, 3}},
	ast.DateAdd:          &addDateFunctionClass{baseFunctionClass{ast.DateAdd, 3, 3}},
	ast.SubDate:          &subDateFunctionClass{baseFunctionClass{ast.SubDate, 3, 3}},
	ast.DateSub:          &subDateFunctionClass{baseFunctionClass{ast.DateSub, 3, 3}},
	ast.AddTime:          &addTimeFunctionClass{baseFunctionClass{ast.AddTime, 2, 2}},
	ast.ConvertTz:        &convertTzFunctionClass{baseFunctionClass{ast.ConvertTz, 3, 3}},
	ast.Curdate:          &currentDateFunctionClass{baseFunctionClass{ast.Curdate, 0, 0}},
	ast.CurrentDate:      &currentDateFunctionClass{baseFunctionClass{ast.CurrentDate, 0, 0}},
	ast.CurrentTime:      &currentTimeFunctionClass{baseFunctionClass{ast.CurrentTime, 0, 1}},
	ast.CurrentTimestamp: &nowFunctionClass{baseFunctionClass{ast.CurrentTimestamp, 0, 1}},
	ast.Curtime:          &currentTimeFunctionClass{baseFunctionClass{ast.Curtime, 0, 1}},
	ast.Date:             &dateFunctionClass{baseFunctionClass{ast.Date, 1, 1}},
	ast.DateLiteral:      &dateLiteralFunctionClass{baseFunctionClass{ast.DateLiteral, 1, 1}},
	ast.DateFormat:       &dateFormatFunctionClass{baseFunctionClass{ast.DateFormat, 2, 2}},
	ast.DateDiff:         &dateDiffFunctionClass{baseFunctionClass{ast.DateDiff, 2, 2}},
	ast.Day:              &dayOfMonthFunctionClass{baseFunctionClass{ast.Day, 1, 1}},
	ast.DayName:          &dayNameFunctionClass{baseFunctionClass{ast.DayName, 1, 1}},
	ast.DayOfMonth:       &dayOfMonthFunctionClass{baseFunctionClass{ast.DayOfMonth, 1, 1}},
	ast.DayOfWeek:        &dayOfWeekFunctionClass{baseFunctionClass{ast.DayOfWeek, 1, 1}},
	ast.DayOfYear:        &dayOfYearFunctionClass{baseFunctionClass{ast.DayOfYear, 1, 1}},
	ast.Extract:          &extractFunctionClass{baseFunctionClass{ast.Extract, 2, 2}},
	ast.FromDays:         &fromDaysFunctionClass{baseFunctionClass{ast.FromDays, 1, 1}},
	ast.FromUnixTime:     &fromUnixTimeFunctionClass{baseFunctionClass{ast.FromUnixTime, 1, 2}},
	ast.GetFormat:        &getFormatFunctionClass{baseFunctionClass{ast.GetFormat, 2, 2}},
	ast.Hour:             &hourFunctionClass{baseFunctionClass{ast.Hour, 1, 1}},
	ast.LocalTime:        &nowFunctionClass{baseFunctionClass{ast.LocalTime, 0, 1}},
	ast.LocalTimestamp:   &nowFunctionClass{baseFunctionClass{ast.LocalTimestamp, 0, 1}},
	ast.MakeDate:         &makeDateFunctionClass{baseFunctionClass{ast.MakeDate, 2, 2}},
	ast.MakeTime:         &makeTimeFunctionClass{baseFunctionClass{ast.MakeTime, 3, 3}},
	ast.MicroSecond:      &microSecondFunctionClass{baseFunctionClass{ast.MicroSecond, 1, 1}},
	ast.Minute:           &minuteFunctionClass{baseFunctionClass{ast.Minute, 1, 1}},
	ast.Month:            &monthFunctionClass{baseFunctionClass{ast.Month, 1, 1}},
	ast.MonthName:        &monthNameFunctionClass{baseFunctionClass{ast.MonthName, 1, 1}},
	ast.Now:              &nowFunctionClass{baseFunctionClass{ast.Now, 0, 1}},
	ast.PeriodAdd:        &periodAddFunctionClass{baseFunctionClass{ast.PeriodAdd, 2, 2}},
	ast.PeriodDiff:       &periodDiffFunctionClass{baseFunctionClass{ast.PeriodDiff, 2, 2}},
	ast.Quarter:          &quarterFunctionClass{baseFunctionClass{ast.Quarter, 1, 1}},
	ast.SecToTime:        &secToTimeFunctionClass{baseFunctionClass{ast.SecToTime, 1, 1}},
	ast.Second:           &secondFunctionClass{baseFunctionClass{ast.Second, 1, 1}},
	ast.StrToDate:        &strToDateFunctionClass{baseFunctionClass{ast.StrToDate, 2, 2}},
	ast.SubTime:          &subTimeFunctionClass{baseFunctionClass{ast.SubTime, 2, 2}},
	ast.Sysdate:          &sysDateFunctionClass{baseFunctionClass{ast.Sysdate, 0, 1}},
	ast.Time:             &timeFunctionClass{baseFunctionClass{ast.Time, 1, 1}},
	ast.TimeLiteral:      &timeLiteralFunctionClass{baseFunctionClass{ast.TimeLiteral, 1, 1}},
	ast.TimeFormat:       &timeFormatFunctionClass{baseFunctionClass{ast.TimeFormat, 2, 2}},
	ast.TimeToSec:        &timeToSecFunctionClass{baseFunctionClass{ast.TimeToSec, 1, 1}},
	ast.TimeDiff:         &timeDiffFunctionClass{baseFunctionClass{ast.TimeDiff, 2, 2}},
	ast.Timestamp:        &timestampFunctionClass{baseFunctionClass{ast.Timestamp, 1, 2}},
	ast.TimestampLiteral: &timestampLiteralFunctionClass{baseFunctionClass{ast.TimestampLiteral, 1, 2}},
	ast.TimestampAdd:     &timestampAddFunctionClass{baseFunctionClass{ast.TimestampAdd, 3, 3}},
	ast.TimestampDiff:    &timestampDiffFunctionClass{baseFunctionClass{ast.TimestampDiff, 3, 3}},
	ast.ToDays:           &toDaysFunctionClass{baseFunctionClass{ast.ToDays, 1, 1}},
	ast.ToSeconds:        &toSecondsFunctionClass{baseFunctionClass{ast.ToSeconds, 1, 1}},
	ast.UnixTimestamp:    &unixTimestampFunctionClass{baseFunctionClass{ast.UnixTimestamp, 0, 1}},
	ast.UTCDate:          &utcDateFunctionClass{baseFunctionClass{ast.UTCDate, 0, 0}},
	ast.UTCTime:          &utcTimeFunctionClass{baseFunctionClass{ast.UTCTime, 0, 1}},
	ast.UTCTimestamp:     &utcTimestampFunctionClass{baseFunctionClass{ast.UTCTimestamp, 0, 1}},
	ast.Week:             &weekFunctionClass{baseFunctionClass{ast.Week, 1, 2}},
	ast.Weekday:          &weekDayFunctionClass{baseFunctionClass{ast.Weekday, 1, 1}},
	ast.WeekOfYear:       &weekOfYearFunctionClass{baseFunctionClass{ast.WeekOfYear, 1, 1}},
	ast.Year:             &yearFunctionClass{baseFunctionClass{ast.Year, 1, 1}},
	ast.YearWeek:         &yearWeekFunctionClass{baseFunctionClass{ast.YearWeek, 1, 2}},
	ast.LastDay:          &lastDayFunctionClass{baseFunctionClass{ast.LastDay, 1, 1}},

	// string functions
	ast.ASCII:           &asciiFunctionClass{baseFunctionClass{ast.ASCII, 1, 1}},
	ast.Bin:             &binFunctionClass{baseFunctionClass{ast.Bin, 1, 1}},
	ast.Concat:          &concatFunctionClass{baseFunctionClass{ast.Concat, 1, -1}},
	ast.ConcatWS:        &concatWSFunctionClass{baseFunctionClass{ast.ConcatWS, 2, -1}},
	ast.Convert:         &convertFunctionClass{baseFunctionClass{ast.Convert, 2, 2}},
	ast.Elt:             &eltFunctionClass{baseFunctionClass{ast.Elt, 2, -1}},
	ast.ExportSet:       &exportSetFunctionClass{baseFunctionClass{ast.ExportSet, 3, 5}},
	ast.Field:           &fieldFunctionClass{baseFunctionClass{ast.Field, 2, -1}},
	ast.Format:          &formatFunctionClass{baseFunctionClass{ast.Format, 2, 3}},
	ast.FromBase64:      &fromBase64FunctionClass{baseFunctionClass{ast.FromBase64, 1, 1}},
	ast.InsertFunc:      &insertFunctionClass{baseFunctionClass{ast.InsertFunc, 4, 4}},
	ast.Instr:           &instrFunctionClass{baseFunctionClass{ast.Instr, 2, 2}},
	ast.Lcase:           &lowerFunctionClass{baseFunctionClass{ast.Lcase, 1, 1}},
	ast.Left:            &leftFunctionClass{baseFunctionClass{ast.Left, 2, 2}},
	ast.Right:           &rightFunctionClass{baseFunctionClass{ast.Right, 2, 2}},
	ast.Length:          &lengthFunctionClass{baseFunctionClass{ast.Length, 1, 1}},
	ast.LoadFile:        &loadFileFunctionClass{baseFunctionClass{ast.LoadFile, 1, 1}},
	ast.Locate:          &locateFunctionClass{baseFunctionClass{ast.Locate, 2, 3}},
	ast.Lower:           &lowerFunctionClass{baseFunctionClass{ast.Lower, 1, 1}},
	ast.Lpad:            &lpadFunctionClass{baseFunctionClass{ast.Lpad, 3, 3}},
	ast.LTrim:           &lTrimFunctionClass{baseFunctionClass{ast.LTrim, 1, 1}},
	ast.Mid:             &substringFunctionClass{baseFunctionClass{ast.Mid, 3, 3}},
	ast.MakeSet:         &makeSetFunctionClass{baseFunctionClass{ast.MakeSet, 2, -1}},
	ast.Oct:             &octFunctionClass{baseFunctionClass{ast.Oct, 1, 1}},
	ast.OctetLength:     &lengthFunctionClass{baseFunctionClass{ast.OctetLength, 1, 1}},
	ast.Ord:             &ordFunctionClass{baseFunctionClass{ast.Ord, 1, 1}},
	ast.Position:        &locateFunctionClass{baseFunctionClass{ast.Position, 2, 2}},
	ast.Quote:           &quoteFunctionClass{baseFunctionClass{ast.Quote, 1, 1}},
	ast.Repeat:          &repeatFunctionClass{baseFunctionClass{ast.Repeat, 2, 2}},
	ast.Replace:         &replaceFunctionClass{baseFunctionClass{ast.Replace, 3, 3}},
	ast.Reverse:         &reverseFunctionClass{baseFunctionClass{ast.Reverse, 1, 1}},
	ast.RTrim:           &rTrimFunctionClass{baseFunctionClass{ast.RTrim, 1, 1}},
	ast.Space:           &spaceFunctionClass{baseFunctionClass{ast.Space, 1, 1}},
	ast.Strcmp:          &strcmpFunctionClass{baseFunctionClass{ast.Strcmp, 2, 2}},
	ast.Substring:       &substringFunctionClass{baseFunctionClass{ast.Substring, 2, 3}},
	ast.Substr:          &substringFunctionClass{baseFunctionClass{ast.Substr, 2, 3}},
	ast.SubstringIndex:  &substringIndexFunctionClass{baseFunctionClass{ast.SubstringIndex, 3, 3}},
	ast.ToBase64:        &toBase64FunctionClass{baseFunctionClass{ast.ToBase64, 1, 1}},
	ast.Trim:            &trimFunctionClass{baseFunctionClass{ast.Trim, 1, 3}},
	ast.Upper:           &upperFunctionClass{baseFunctionClass{ast.Upper, 1, 1}},
	ast.Ucase:           &upperFunctionClass{baseFunctionClass{ast.Ucase, 1, 1}},
	ast.Hex:             &hexFunctionClass{baseFunctionClass{ast.Hex, 1, 1}},
	ast.Unhex:           &unhexFunctionClass{baseFunctionClass{ast.Unhex, 1, 1}},
	ast.Rpad:            &rpadFunctionClass{baseFunctionClass{ast.Rpad, 3, 3}},
	ast.BitLength:       &bitLengthFunctionClass{baseFunctionClass{ast.BitLength, 1, 1}},
	ast.CharFunc:        &charFunctionClass{baseFunctionClass{ast.CharFunc, 2, -1}},
	ast.CharLength:      &charLengthFunctionClass{baseFunctionClass{ast.CharLength, 1, 1}},
	ast.CharacterLength: &charLengthFunctionClass{baseFunctionClass{ast.CharacterLength, 1, 1}},
	ast.FindInSet:       &findInSetFunctionClass{baseFunctionClass{ast.FindInSet, 2, 2}},
	ast.WeightString:    &weightStringFunctionClass{baseFunctionClass{ast.WeightString, 1, 3}},

	// information functions
	ast.ConnectionID: &connectionIDFunctionClass{baseFunctionClass{ast.ConnectionID, 0, 0}},
	ast.CurrentUser:  &currentUserFunctionClass{baseFunctionClass{ast.CurrentUser, 0, 0}},
	ast.CurrentRole:  &currentRoleFunctionClass{baseFunctionClass{ast.CurrentRole, 0, 0}},
	ast.Database:     &databaseFunctionClass{baseFunctionClass{ast.Database, 0, 0}},
	// This function is a synonym for DATABASE().
	// See http://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_schema
	ast.Schema:        &databaseFunctionClass{baseFunctionClass{ast.Schema, 0, 0}},
	ast.FoundRows:     &foundRowsFunctionClass{baseFunctionClass{ast.FoundRows, 0, 0}},
	ast.LastInsertId:  &lastInsertIDFunctionClass{baseFunctionClass{ast.LastInsertId, 0, 1}},
	ast.User:          &userFunctionClass{baseFunctionClass{ast.User, 0, 0}},
	ast.Version:       &versionFunctionClass{baseFunctionClass{ast.Version, 0, 0}},
	ast.Benchmark:     &benchmarkFunctionClass{baseFunctionClass{ast.Benchmark, 2, 2}},
	ast.Charset:       &charsetFunctionClass{baseFunctionClass{ast.Charset, 1, 1}},
	ast.Coercibility:  &coercibilityFunctionClass{baseFunctionClass{ast.Coercibility, 1, 1}},
	ast.Collation:     &collationFunctionClass{baseFunctionClass{ast.Collation, 1, 1}},
	ast.RowCount:      &rowCountFunctionClass{baseFunctionClass{ast.RowCount, 0, 0}},
	ast.CausetNetUser: &userFunctionClass{baseFunctionClass{ast.CausetNetUser, 0, 0}},
	ast.SystemUser:    &userFunctionClass{baseFunctionClass{ast.SystemUser, 0, 0}},

	// See https://dev.mysql.com/doc/refman/8.0/en/performance-schema-functions.html
	ast.FormatBytes:    &formatBytesFunctionClass{baseFunctionClass{ast.FormatBytes, 1, 1}},
	ast.FormatNanoTime: &formatNanoTimeFunctionClass{baseFunctionClass{ast.FormatNanoTime, 1, 1}},

	// control functions
	ast.If:     &ifFunctionClass{baseFunctionClass{ast.If, 3, 3}},
	ast.Ifnull: &ifNullFunctionClass{baseFunctionClass{ast.Ifnull, 2, 2}},

	// miscellaneous functions
	ast.Sleep:           &sleepFunctionClass{baseFunctionClass{ast.Sleep, 1, 1}},
	ast.AnyValue:        &anyValueFunctionClass{baseFunctionClass{ast.AnyValue, 1, 1}},
	ast.DefaultFunc:     &defaultFunctionClass{baseFunctionClass{ast.DefaultFunc, 1, 1}},
	ast.InetAton:        &inetAtonFunctionClass{baseFunctionClass{ast.InetAton, 1, 1}},
	ast.InetNtoa:        &inetNtoaFunctionClass{baseFunctionClass{ast.InetNtoa, 1, 1}},
	ast.Inet6Aton:       &inet6AtonFunctionClass{baseFunctionClass{ast.Inet6Aton, 1, 1}},
	ast.Inet6Ntoa:       &inet6NtoaFunctionClass{baseFunctionClass{ast.Inet6Ntoa, 1, 1}},
	ast.IsFreeLock:      &isFreeLockFunctionClass{baseFunctionClass{ast.IsFreeLock, 1, 1}},
	ast.IsIPv4:          &isIPv4FunctionClass{baseFunctionClass{ast.IsIPv4, 1, 1}},
	ast.IsIPv4Compat:    &isIPv4CompatFunctionClass{baseFunctionClass{ast.IsIPv4Compat, 1, 1}},
	ast.IsIPv4Mapped:    &isIPv4MappedFunctionClass{baseFunctionClass{ast.IsIPv4Mapped, 1, 1}},
	ast.IsIPv6:          &isIPv6FunctionClass{baseFunctionClass{ast.IsIPv6, 1, 1}},
	ast.IsUsedLock:      &isUsedLockFunctionClass{baseFunctionClass{ast.IsUsedLock, 1, 1}},
	ast.MasterPosWait:   &masterPosWaitFunctionClass{baseFunctionClass{ast.MasterPosWait, 2, 4}},
	ast.NameConst:       &nameConstFunctionClass{baseFunctionClass{ast.NameConst, 2, 2}},
	ast.ReleaseAllLocks: &releaseAllLocksFunctionClass{baseFunctionClass{ast.ReleaseAllLocks, 0, 0}},
	ast.UUID:            &uuidFunctionClass{baseFunctionClass{ast.UUID, 0, 0}},
	ast.UUIDShort:       &uuidShortFunctionClass{baseFunctionClass{ast.UUIDShort, 0, 0}},

	// get_lock() and release_lock() are parsed but do nothing.
	// It is used for preventing error in Ruby's activerecord migrations.
	ast.GetLock:     &lockFunctionClass{baseFunctionClass{ast.GetLock, 2, 2}},
	ast.ReleaseLock: &releaseLockFunctionClass{baseFunctionClass{ast.ReleaseLock, 1, 1}},

	ast.LogicAnd:   &logicAndFunctionClass{baseFunctionClass{ast.LogicAnd, 2, 2}},
	ast.LogicOr:    &logicOrFunctionClass{baseFunctionClass{ast.LogicOr, 2, 2}},
	ast.LogicXor:   &logicXorFunctionClass{baseFunctionClass{ast.LogicXor, 2, 2}},
	ast.GE:         &compareFunctionClass{baseFunctionClass{ast.GE, 2, 2}, opcode.GE},
	ast.LE:         &compareFunctionClass{baseFunctionClass{ast.LE, 2, 2}, opcode.LE},
	ast.EQ:         &compareFunctionClass{baseFunctionClass{ast.EQ, 2, 2}, opcode.EQ},
	ast.NE:         &compareFunctionClass{baseFunctionClass{ast.NE, 2, 2}, opcode.NE},
	ast.LT:         &compareFunctionClass{baseFunctionClass{ast.LT, 2, 2}, opcode.LT},
	ast.GT:         &compareFunctionClass{baseFunctionClass{ast.GT, 2, 2}, opcode.GT},
	ast.NullEQ:     &compareFunctionClass{baseFunctionClass{ast.NullEQ, 2, 2}, opcode.NullEQ},
	ast.Plus:       &arithmeticPlusFunctionClass{baseFunctionClass{ast.Plus, 2, 2}},
	ast.Minus:      &arithmeticMinusFunctionClass{baseFunctionClass{ast.Minus, 2, 2}},
	ast.Mod:        &arithmeticModFunctionClass{baseFunctionClass{ast.Mod, 2, 2}},
	ast.Div:        &arithmeticDivideFunctionClass{baseFunctionClass{ast.Div, 2, 2}},
	ast.Mul:        &arithmeticMultiplyFunctionClass{baseFunctionClass{ast.Mul, 2, 2}},
	ast.IntDiv:     &arithmeticIntDivideFunctionClass{baseFunctionClass{ast.IntDiv, 2, 2}},
	ast.BitNeg:     &bitNegFunctionClass{baseFunctionClass{ast.BitNeg, 1, 1}},
	ast.And:        &bitAndFunctionClass{baseFunctionClass{ast.And, 2, 2}},
	ast.LeftShift:  &leftShiftFunctionClass{baseFunctionClass{ast.LeftShift, 2, 2}},
	ast.RightShift: &rightShiftFunctionClass{baseFunctionClass{ast.RightShift, 2, 2}},
	ast.UnaryNot:   &unaryNotFunctionClass{baseFunctionClass{ast.UnaryNot, 1, 1}},
	ast.Or:         &bitOrFunctionClass{baseFunctionClass{ast.Or, 2, 2}},
	ast.Xor:        &bitXorFunctionClass{baseFunctionClass{ast.Xor, 2, 2}},
	ast.UnaryMinus: &unaryMinusFunctionClass{baseFunctionClass{ast.UnaryMinus, 1, 1}},
	ast.In:         &inFunctionClass{baseFunctionClass{ast.In, 2, -1}},
	ast.IsTruth:    &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruth, 1, 1}, opcode.IsTruth, false},
	ast.IsFalsity:  &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsFalsity, 1, 1}, opcode.IsFalsity, false},
	ast.Like:       &likeFunctionClass{baseFunctionClass{ast.Like, 3, 3}},
	ast.Regexp:     &regexpFunctionClass{baseFunctionClass{ast.Regexp, 2, 2}},
	ast.Case:       &caseWhenFunctionClass{baseFunctionClass{ast.Case, 1, -1}},
	ast.RowFunc:    &rowFunctionClass{baseFunctionClass{ast.RowFunc, 2, -1}},
	ast.SetVar:     &setVarFunctionClass{baseFunctionClass{ast.SetVar, 2, 2}},
	ast.GetVar:     &getVarFunctionClass{baseFunctionClass{ast.GetVar, 1, 1}},
	ast.BitCount:   &bitCountFunctionClass{baseFunctionClass{ast.BitCount, 1, 1}},
	ast.GetParam:   &getParamFunctionClass{baseFunctionClass{ast.GetParam, 1, 1}},

	// encryption and compression functions
	ast.AesDecrypt:               &aesDecryptFunctionClass{baseFunctionClass{ast.AesDecrypt, 2, 3}},
	ast.AesEncrypt:               &aesEncryptFunctionClass{baseFunctionClass{ast.AesEncrypt, 2, 3}},
	ast.Compress:                 &compressFunctionClass{baseFunctionClass{ast.Compress, 1, 1}},
	ast.Decode:                   &decodeFunctionClass{baseFunctionClass{ast.Decode, 2, 2}},
	ast.DesDecrypt:               &desDecryptFunctionClass{baseFunctionClass{ast.DesDecrypt, 1, 2}},
	ast.DesEncrypt:               &desEncryptFunctionClass{baseFunctionClass{ast.DesEncrypt, 1, 2}},
	ast.Encode:                   &encodeFunctionClass{baseFunctionClass{ast.Encode, 2, 2}},
	ast.Encrypt:                  &encryptFunctionClass{baseFunctionClass{ast.Encrypt, 1, 2}},
	ast.MD5:                      &md5FunctionClass{baseFunctionClass{ast.MD5, 1, 1}},
	ast.OldPassword:              &oldPasswordFunctionClass{baseFunctionClass{ast.OldPassword, 1, 1}},
	ast.PasswordFunc:             &passwordFunctionClass{baseFunctionClass{ast.PasswordFunc, 1, 1}},
	ast.RandomBytes:              &randomBytesFunctionClass{baseFunctionClass{ast.RandomBytes, 1, 1}},
	ast.SHA1:                     &sha1FunctionClass{baseFunctionClass{ast.SHA1, 1, 1}},
	ast.SHA:                      &sha1FunctionClass{baseFunctionClass{ast.SHA, 1, 1}},
	ast.SHA2:                     &sha2FunctionClass{baseFunctionClass{ast.SHA2, 2, 2}},
	ast.Uncompress:               &uncompressFunctionClass{baseFunctionClass{ast.Uncompress, 1, 1}},
	ast.UncompressedLength:       &uncompressedLengthFunctionClass{baseFunctionClass{ast.UncompressedLength, 1, 1}},
	ast.ValidatePasswordStrength: &validatePasswordStrengthFunctionClass{baseFunctionClass{ast.ValidatePasswordStrength, 1, 1}},

	// json functions
	ast.JSONType:          &jsonTypeFunctionClass{baseFunctionClass{ast.JSONType, 1, 1}},
	ast.JSONExtract:       &jsonExtractFunctionClass{baseFunctionClass{ast.JSONExtract, 2, -1}},
	ast.JSONUnquote:       &jsonUnquoteFunctionClass{baseFunctionClass{ast.JSONUnquote, 1, 1}},
	ast.JSONSet:           &jsonSetFunctionClass{baseFunctionClass{ast.JSONSet, 3, -1}},
	ast.JSONInsert:        &jsonInsertFunctionClass{baseFunctionClass{ast.JSONInsert, 3, -1}},
	ast.JSONReplace:       &jsonReplaceFunctionClass{baseFunctionClass{ast.JSONReplace, 3, -1}},
	ast.JSONRemove:        &jsonRemoveFunctionClass{baseFunctionClass{ast.JSONRemove, 2, -1}},
	ast.JSONMerge:         &jsonMergeFunctionClass{baseFunctionClass{ast.JSONMerge, 2, -1}},
	ast.JSONObject:        &jsonObjectFunctionClass{baseFunctionClass{ast.JSONObject, 0, -1}},
	ast.JSONArray:         &jsonArrayFunctionClass{baseFunctionClass{ast.JSONArray, 0, -1}},
	ast.JSONContains:      &jsonContainsFunctionClass{baseFunctionClass{ast.JSONContains, 2, 3}},
	ast.JSONContainsPath:  &jsonContainsPathFunctionClass{baseFunctionClass{ast.JSONContainsPath, 3, -1}},
	ast.JSONValid:         &jsonValidFunctionClass{baseFunctionClass{ast.JSONValid, 1, 1}},
	ast.JSONArrayAppend:   &jsonArrayAppendFunctionClass{baseFunctionClass{ast.JSONArrayAppend, 3, -1}},
	ast.JSONArrayInsert:   &jsonArrayInsertFunctionClass{baseFunctionClass{ast.JSONArrayInsert, 3, -1}},
	ast.JSONMergePatch:    &jsonMergePatchFunctionClass{baseFunctionClass{ast.JSONMergePatch, 2, -1}},
	ast.JSONMergePreserve: &jsonMergePreserveFunctionClass{baseFunctionClass{ast.JSONMergePreserve, 2, -1}},
	ast.JSONPretty:        &jsonPrettyFunctionClass{baseFunctionClass{ast.JSONPretty, 1, 1}},
	ast.JSONQuote:         &jsonQuoteFunctionClass{baseFunctionClass{ast.JSONQuote, 1, 1}},
	ast.JSONSearch:        &jsonSearchFunctionClass{baseFunctionClass{ast.JSONSearch, 3, -1}},
	ast.JSONStorageSize:   &jsonStorageSizeFunctionClass{baseFunctionClass{ast.JSONStorageSize, 1, 1}},
	ast.JSONDepth:         &jsonDepthFunctionClass{baseFunctionClass{ast.JSONDepth, 1, 1}},
	ast.JSONKeys:          &jsonKeysFunctionClass{baseFunctionClass{ast.JSONKeys, 1, 2}},
	ast.JSONLength:        &jsonLengthFunctionClass{baseFunctionClass{ast.JSONLength, 1, 2}},

	// MilevaDB internal function.
	ast.MilevaDBDecodeKey: &MilevaDBDecodeKeyFunctionClass{baseFunctionClass{ast.MilevaDBDecodeKey, 1, 1}},
	// This function is used to show MilevaDB-server version info.
	ast.MilevaDBVersion:    &MilevaDBVersionFunctionClass{baseFunctionClass{ast.MilevaDBVersion, 0, 0}},
	ast.MilevaDBIsDBSOwner: &MilevaDBIsDBSOwnerFunctionClass{baseFunctionClass{ast.MilevaDBIsDBSOwner, 0, 0}},
	ast.MilevaDBParseTso:   &MilevaDBParseTsoFunctionClass{baseFunctionClass{ast.MilevaDBParseTso, 1, 1}},
	ast.MilevaDBDecodePlan: &MilevaDBDecodePlanFunctionClass{baseFunctionClass{ast.MilevaDBDecodePlan, 1, 1}},

	// MilevaDB Sequence function.
	ast.NextVal: &nextValFunctionClass{baseFunctionClass{ast.NextVal, 1, 1}},
	ast.LastVal: &lastValFunctionClass{baseFunctionClass{ast.LastVal, 1, 1}},
	ast.SetVal:  &setValFunctionClass{baseFunctionClass{ast.SetVal, 2, 2}},
}

// IsFunctionSupported check if given function name is a builtin sql function.
func IsFunctionSupported(name string) bool {
	_, ok := funcs[name]
	return ok
}

// GetBuiltinList returns a list of builtin functions
func GetBuiltinList() []string {
	res := make([]string, 0, len(funcs))
	notImplementedFunctions := []string{ast.RowFunc}
	for funcName := range funcs {
		skipFunc := false
		// Skip not implemented functions
		for _, notImplFunc := range notImplementedFunctions {
			if funcName == notImplFunc {
				skipFunc = true
			}
		}

		if strings.HasPrefix(funcName, "'MilevaDB`.(") {
			skipFunc = true
		}
		if skipFunc {
			continue
		}
		res = append(res, funcName)
	}
	sort.Strings(res)
	return res
}
