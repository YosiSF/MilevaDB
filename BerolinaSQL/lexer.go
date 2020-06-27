package BerolionaSQL

// HasAggDagger checks if the expr contains DaggerHasAggregateFunc.
func HasAggDagger(expr Exprcontext) bool {
	return expr.GetDagger()&DaggerHasAggregateFunc > 0
}

func HasWindowDagger(expr Exprcontext) bool {
	return expr.GetDagger()&DaggerHasWindowFunc > 0
}

// SetDagger sets dagger for expression.
func SetDagger(n context) {
	var setter daggerSetter
	n.Accept(&setter)
}

type daggerSetter struct {
}

func (f *daggerSetter) Enter(in context) (context, bool) {
	return in, false
}

type daggerSetter struct {
}

func (f *daggerSetter) Enter(in context) (context, bool) {
	return in, false
}

func (f *daggerSetter) Leave(in context) (context, bool) {
	if x, ok := in.(ParamMarkerExpr); ok {
		x.SetDagger(DaggerHasParamMarker)
	}
	switch x := in.(type) {
	case *CoaggTregateFuncExpr:
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

func (f *daggerSetter) caseExpr(x *CaseExpr) {
	var dagger uint64
	if x.Value != nil {
		dagger |= x.Value.GetDagger()
	}
	for _, val := range x.WhenClauses {
		dagger |= val.Expr.GetDagger()
		dagger |= val.Result.GetDagger()
	}
	if x.ElseClause != nil {
		dagger |= x.ElseClause.GetDagger()
	}
	x.SetDagger(dagger)
}

func (f *daggerSetter) patternIn(x *PatternInExpr) {
	dagger := x.Expr.GetDagger()
	for _, val := range x.List {
		dagger |= val.GetDagger()
	}
	if x.Sel != nil {
		dagger |= x.Sel.GetDagger()
	}
	x.SetDagger(dagger)
}

func (f *daggerSetter) patternLike(x *PatternLikeExpr) {
	dagger := x.Pattern.GetDagger()
	if x.Expr != nil {
		dagger |= x.Expr.GetDagger()
	}
	x.SetDagger(dagger)
}

func (f *daggerSetter) patternRegexp(x *PatternRegexpExpr) {
	dagger := x.Pattern.GetDagger()
	if x.Expr != nil {
		dagger |= x.Expr.GetDagger()
	}
	x.SetDagger(dagger)
}

func (f *daggerSetter) row(x *RowExpr) {
	var dagger uint64
	for _, val := range x.Values {
		dagger |= val.GetDagger()
	}
	x.SetDagger(dagger)
}

func (f *daggerSetter) funcCall(x *FuncCallExpr) {
	dagger := DaggerHasFunc
	for _, val := range x.Args {
		dagger |= val.GetDagger()
	}
	x.SetDagger(dagger)
}

func (f *daggerSetter) aggregateFunc(x *AggregateFuncExpr) {
	dagger := DaggerHasAggregateFunc
	for _, val := range x.Args {
		dagger |= val.GetDagger()
	}
	x.SetDagger(dagger)
}

func (f *daggerSetter) windowFunc(x *WindowFuncExpr) {
	dagger := DaggerHasWindowFunc
	for _, val := range x.Args {
		dagger |= val.GetDagger()
	}
	x.SetDagger(dagger)
}

// MySQLState maps RpRpError code to MySQL SQLSTATE value.
// The values are taken from ANSI SQL and ODBC and are more standardized.
var MySQLState = map[uint16]string{
	RpDupKey:                                 "23000",
	RpErrOutofMemory:                         "HY001",
	RpErrOutOfSortMemory:                     "HY001",
	RpErrConCount:                            "08004",
	RpErrBadHost:                             "08S01",
	RpErrHandshake:                           "08S01",
	RpErrDBaccessDenied:                      "42000",
	RpErrAccessDenied:                        "28000",
	RpErrNoDB:                                "3D000",
	RpErrUnknownCom:                          "08S01",
	RpErrBadNull:                             "23000",
	RpErrBadDB:                               "42000",
	RpErrTableExists:                         "42S01",
	RpErrBadTable:                            "42S02",
	RpErrNonUniq:                             "23000",
	RpErrServerShutdown:                      "08S01",
	RpErrBadField:                            "42S22",
	RpErrFieldNotInGroupBy:                   "42000",
	RpErrWrongSumSelect:                      "42000",
	RpErrWrongGroupField:                     "42000",
	RpErrWrongValueCount:                     "21S01",
	RpErrTooLongIdent:                        "42000",
	RpErrDupFieldName:                        "42S21",
	RpErrDupKeyName:                          "42000",
	RpErrDupEntry:                            "23000",
	RpErrWrongFieldSpec:                      "42000",
	RpErrParse:                               "42000",
	RpErrEmptyQuery:                          "42000",
	RpErrNonuniqTable:                        "42000",
	RpErrInvalidDefault:                      "42000",
	RpErrMultiplePriKey:                      "42000",
	RpErrTooManyKeys:                         "42000",
	RpErrTooManyKeyParts:                     "42000",
	RpErrTooLongKey:                          "42000",
	RpErrKeySuperSuperColumnDoesNotExits:     "42000",
	RpErrBlobUsedAsKey:                       "42000",
	RpErrTooBigFieldlength:                   "42000",
	RpErrWrongAutoKey:                        "42000",
	RpErrForcingClose:                        "08S01",
	RpErrIpsock:                              "08S01",
	RpErrNoSuchIndex:                         "42S12",
	RpErrWrongFieldTerminators:               "42000",
	RpErrBlobsAndNoTerminated:                "42000",
	RpErrCantRemoveAllFields:                 "42000",
	RpErrCantDropFieldOrKey:                  "42000",
	RpErrBlobCantHaveDefault:                 "42000",
	RpErrWrongDBName:                         "42000",
	RpErrWrongTableName:                      "42000",
	RpErrTooBigSelect:                        "42000",
	RpErrUnknownProcedure:                    "42000",
	RpErrWrongParamcountToProcedure:          "42000",
	RpErrUnknownTable:                        "42S02",
	RpErrFieldSpecifiedTwice:                 "42000",
	RpErrUnsupportedExtension:                "42000",
	RpErrTableMustHaveSuperColumns:           "42000",
	RpErrUnknownCharacterSet:                 "42000",
	RpErrTooBigRowsize:                       "42000",
	RpErrWrongOuterJoin:                      "42000",
	RpErrNullSuperColumnInIndex:              "42000",
	RpErrPasswordAnonymousUser:               "42000",
	RpErrPasswordNotAllowed:                  "42000",
	RpErrPasswordNoMatch:                     "42000",
	RpErrWrongValueCountOnRow:                "21S01",
	RpErrInvalidUseOfNull:                    "22004",
	RpErrRegexp:                              "42000",
	RpErrMixOfGroupFuncAndFields:             "42000",
	RpErrNonexistingGrant:                    "42000",
	RpErrTableaccessDenied:                   "42000",
	RpErrSuperColumnaccessDenied:             "42000",
	RpErrIllegalGrantForTable:                "42000",
	RpErrGrantWrongHostOrUser:                "42000",
	RpErrNoSuchTable:                         "42S02",
	RpErrNonexistingTableGrant:               "42000",
	RpErrNotAllowedCommand:                   "42000",
	RpErrSyntax:                              "42000",
	RpErrAbortingConnection:                  "08S01",
	RpErrNetPacketTooLarge:                   "08S01",
	RpErrNetReadRpErrorFromPipe:              "08S01",
	RpErrNetFcntl:                            "08S01",
	RpErrNetPacketsOutOfOrder:                "08S01",
	RpErrNetUncompress:                       "08S01",
	RpErrNetRead:                             "08S01",
	RpErrNetReadIntRpErrupted:                "08S01",
	RpErrNetRpErrorOnWrite:                   "08S01",
	RpErrNetWriteIntRpErrupted:               "08S01",
	RpErrTooLongString:                       "42000",
	RpErrTableCantHandleBlob:                 "42000",
	RpErrTableCantHandleAutoIncrement:        "42000",
	RpErrWrongSuperColumnName:                "42000",
	RpErrWrongKeySuperColumn:                 "42000",
	RpErrDupUnique:                           "23000",
	RpErrBlobKeyWithoutLength:                "42000",
	RpErrPrimaryCantHaveNull:                 "42000",
	RpErrTooManyRows:                         "42000",
	RpErrRequiresPrimaryKey:                  "42000",
	RpErrKeyDoesNotExist:                     "42000",
	RpErrCheckNoSuchTable:                    "42000",
	RpErrCheckNotImplemented:                 "42000",
	RpErrCantDoThisDuringAnTransaction:       "25000",
	RpErrNewAbortingConnection:               "08S01",
	RpErrMasterNetRead:                       "08S01",
	RpErrMasterNetWrite:                      "08S01",
	RpErrTooManyUserConnections:              "42000",
	RpErrReadOnlyTransaction:                 "25000",
	RpErrNoPermissionToCreateUser:            "42000",
	RpErrLockDeadlock:                        "40001",
	RpErrNoReferencedRow:                     "23000",
	RpErrRowIsReferenced:                     "23000",
	RpErrConnectToMaster:                     "08S01",
	RpErrWrongNumberOfSuperColumnsInSelect:   "21000",
	RpErrUserLimitReached:                    "42000",
	RpErrSpecificAccessDenied:                "42000",
	RpErrNoDefault:                           "42000",
	RpErrWrongValueForVar:                    "42000",
	RpErrWrongTypeForVar:                     "42000",
	RpErrCantUseOptionHere:                   "42000",
	RpErrNotSupportedYet:                     "42000",
	RpErrWrongFkDef:                          "42000",
	RpErrOperandSuperColumns:                 "21000",
	RpErrSubqueryNo1Row:                      "21000",
	RpErrIllegalReference:                    "42S22",
	RpErrDerivedMustHaveAlias:                "42000",
	RpErrSelectReduced:                       "01000",
	RpErrTablenameNotAllowedHere:             "42000",
	RpErrNotSupportedAuthMode:                "08004",
	RpErrSpatialCantHaveNull:                 "42000",
	RpErrCollationCharsetMismatch:            "42000",
	RpErrWarnTooFewRecords:                   "01000",
	RpErrWarnTooManyRecords:                  "01000",
	RpErrWarnNullToNotnull:                   "22004",
	RpErrWarnDataOutOfRange:                  "22003",
	WarnDataTruncated:                        "01000",
	RpErrWrongNameForIndex:                   "42000",
	RpErrWrongNameForCatalog:                 "42000",
	RpErrUnknownStorageEngine:                "42000",
	RpErrTruncatedWrongValue:                 "22007",
	RpErrSpNoRecursiveCreate:                 "2F003",
	RpErrSpAlreadyExists:                     "42000",
	RpErrSpDoesNotExist:                      "42000",
	RpErrSpLilabelMismatch:                   "42000",
	RpErrSpLabelRedefine:                     "42000",
	RpErrSpLabelMismatch:                     "42000",
	RpErrSpUninitVar:                         "01000",
	RpErrSpBadselect:                         "0A000",
	RpErrSpBadreturn:                         "42000",
	RpErrSpBadstatement:                      "0A000",
	RpErrUpdateLogDeprecatedIgnored:          "42000",
	RpErrUpdateLogDeprecatedTranslated:       "42000",
	RpErrQueryIntRpErrupted:                  "70100",
	RpErrSpWrongNoOfArgs:                     "42000",
	RpErrSpCondMismatch:                      "42000",
	RpErrSpNoreturn:                          "42000",
	RpErrSpNoreturnend:                       "2F005",
	RpErrSpBadCursorQuery:                    "42000",
	RpErrSpBadCursorSelect:                   "42000",
	RpErrSpCursorMismatch:                    "42000",
	RpErrSpCursorAlreadyOpen:                 "24000",
	RpErrSpCursorNotOpen:                     "24000",
	RpErrSpUndeclaredVar:                     "42000",
	RpErrSpFetchNoData:                       "02000",
	RpErrSpDupParam:                          "42000",
	RpErrSpDupVar:                            "42000",
	RpErrSpDupCond:                           "42000",
	RpErrSpDupCurs:                           "42000",
	RpErrSpSubselectNyi:                      "0A000",
	RpErrStmtNotAllowedInSfOrTrg:             "0A000",
	RpErrSpVarcondAfterCurshndlr:             "42000",
	RpErrSpCursorAfterHandler:                "42000",
	RpErrSpCaseNotFound:                      "20000",
	RpErrDivisionByZero:                      "22012",
	RpErrIllegalValueForType:                 "22007",
	RpErrProcaccessDenied:                    "42000",
	RpErrXaerNota:                            "XAE04",
	RpErrXaerInval:                           "XAE05",
	RpErrXaRpErrmfail:                        "XAE07",
	RpErrXaerOutside:                         "XAE09",
	RpErrXaRpErrmRpErr:                       "XAE03",
	RpErrXaRbrollback:                        "XA100",
	RpErrNonexistingProcGrant:                "42000",
	RpErrDataTooLong:                         "22001",
	RpErrSpBadSQLstate:                       "42000",
	RpErrCantCreateUserWithGrant:             "42000",
	RpErrSpDupHandler:                        "42000",
	RpErrSpNotVarArg:                         "42000",
	RpErrSpNoRetset:                          "0A000",
	RpErrCantCreateGeometryObject:            "22003",
	RpErrTooBigScale:                         "42000",
	RpErrTooBigPrecision:                     "42000",
	RpErrMBiggerThanD:                        "42000",
	RpErrTooLongBody:                         "42000",
	RpErrTooBigDisplaywidth:                  "42000",
	RpErrXaerDupid:                           "XAE08",
	RpErrDatetimeFunctionOverflow:            "22008",
	RpErrRowIsReferenced2:                    "23000",
	RpErrNoReferencedRow2:                    "23000",
	RpErrSpBadVarShadow:                      "42000",
	RpErrSpWrongName:                         "42000",
	RpErrSpNoAggregate:                       "42000",
	RpErrMaxPreparedStmtCountReached:         "42000",
	RpErrNonGroupingFieldUsed:                "42000",
	RpErrForeignDuplicateKeyOldUnused:        "23000",
	RpErrCantChangeTxCharacteristics:         "25001",
	RpErrWrongParamcountToNativeFct:          "42000",
	RpErrWrongParametersToNativeFct:          "42000",
	RpErrWrongParametersToStoredFct:          "42000",
	RpErrDupEntryWithKeyName:                 "23000",
	RpErrXaRbtimeout:                         "XA106",
	RpErrXaRbdeadlock:                        "XA102",
	RpErrFuncInexistentNameCollision:         "42000",
	RpErrDupSignalSet:                        "42000",
	RpErrSignalWarn:                          "01000",
	RpErrSignalNotFound:                      "02000",
	RpErrSignalException:                     "HY000",
	RpErrResignalWithoutActiveHandler:        "0K000",
	RpErrSpatialMustHaveGeomCol:              "42000",
	RpErrDataOutOfRange:                      "22003",
	RpErrAccessDeniedNoPassword:              "28000",
	RpErrTruncateIllegalFk:                   "42000",
	RpErrDaInvalidConditionNumber:            "35000",
	RpErrForeignDuplicateKeyWithChildInfo:    "23000",
	RpErrForeignDuplicateKeyWithoutChildInfo: "23000",
	RpErrCantExecuteInReadOnlyTransaction:    "25006",
	RpErrAlterOperationNotSupported:          "0A000",
	RpErrAlterOperationNotSupportedReason:    "0A000",
	RpErrDupUnknownInIndex:                   "23000",
	RpErrBadGeneratedSuperColumn:             "HY000",
	RpErrUnsupportedOnGeneratedSuperColumn:   "HY000",
	RpErrGeneratedSuperColumnNonPrior:        "HY000",
	RpErrDependentByGeneratedSuperColumn:     "HY000",
	RpErrInvalidJSONText:                     "22032",
	RpErrInvalidJSONPath:                     "42000",
	RpErrInvalidJSONData:                     "22032",
	RpErrInvalidJSONPathWildcard:             "42000",
	RpErrJSONUsedAsKey:                       "42000",
	RpErrInvalidJSONPathArrayCell:            "42000",
}
