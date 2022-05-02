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

package core

import (
	allegrosql "github.com/whtcorpsinc/MilevaDB-Prod/errno"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
)

// error definitions.
var (
	ErrUnsupportedType                              = terror.ClassOptimizer.New(allegrosql.ErrUnsupportedType, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedType])
	ErrAnalyzeMissIndex                             = terror.ClassOptimizer.New(allegrosql.ErrAnalyzeMissIndex, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrAnalyzeMissIndex])
	ErrWrongParamCount                              = terror.ClassOptimizer.New(allegrosql.ErrWrongParamCount, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongParamCount])
	ErrSchemaChanged                                = terror.ClassOptimizer.New(allegrosql.ErrSchemaChanged, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSchemaChanged])
	ErrBlocknameNotAllowedHere                      = terror.ClassOptimizer.New(allegrosql.ErrBlocknameNotAllowedHere, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlocknameNotAllowedHere])
	ErrNotSupportedYet                              = terror.ClassOptimizer.New(allegrosql.ErrNotSupportedYet, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNotSupportedYet])
	ErrWrongUsage                                   = terror.ClassOptimizer.New(allegrosql.ErrWrongUsage, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongUsage])
	ErrUnknown                                      = terror.ClassOptimizer.New(allegrosql.ErrUnknown, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknown])
	ErrUnknownBlock                                 = terror.ClassOptimizer.New(allegrosql.ErrUnknownBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownBlock])
	ErrNoSuchBlock                                  = terror.ClassOptimizer.New(allegrosql.ErrNoSuchBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNoSuchBlock])
	ErrWrongArguments                               = terror.ClassOptimizer.New(allegrosql.ErrWrongArguments, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongArguments])
	ErrWrongNumberOfDeferredCausetsInSelect         = terror.ClassOptimizer.New(allegrosql.ErrWrongNumberOfDeferredCausetsInSelect, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongNumberOfDeferredCausetsInSelect])
	ErrBadGeneratedDeferredCauset                   = terror.ClassOptimizer.New(allegrosql.ErrBadGeneratedDeferredCauset, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadGeneratedDeferredCauset])
	ErrFieldNotInGroupBy                            = terror.ClassOptimizer.New(allegrosql.ErrFieldNotInGroupBy, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFieldNotInGroupBy])
	ErrBadBlock                                     = terror.ClassOptimizer.New(allegrosql.ErrBadBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadBlock])
	ErrKeyDoesNotExist                              = terror.ClassOptimizer.New(allegrosql.ErrKeyDoesNotExist, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrKeyDoesNotExist])
	ErrOperandDeferredCausets                       = terror.ClassOptimizer.New(allegrosql.ErrOperandDeferredCausets, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrOperandDeferredCausets])
	ErrInvalidGroupFuncUse                          = terror.ClassOptimizer.New(allegrosql.ErrInvalidGroupFuncUse, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidGroupFuncUse])
	ErrIllegalReference                             = terror.ClassOptimizer.New(allegrosql.ErrIllegalReference, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrIllegalReference])
	ErrNoDB                                         = terror.ClassOptimizer.New(allegrosql.ErrNoDB, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNoDB])
	ErrUnknownExplainFormat                         = terror.ClassOptimizer.New(allegrosql.ErrUnknownExplainFormat, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownExplainFormat])
	ErrWrongGroupField                              = terror.ClassOptimizer.New(allegrosql.ErrWrongGroupField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongGroupField])
	ErrDupFieldName                                 = terror.ClassOptimizer.New(allegrosql.ErrDupFieldName, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDupFieldName])
	ErrNonUFIDelablockBlock                         = terror.ClassOptimizer.New(allegrosql.ErrNonUFIDelablockBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNonUFIDelablockBlock])
	ErrInternal                                     = terror.ClassOptimizer.New(allegrosql.ErrInternal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInternal])
	ErrNonUniqBlock                                 = terror.ClassOptimizer.New(allegrosql.ErrNonuniqBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNonuniqBlock])
	ErrWindowInvalidWindowFuncUse                   = terror.ClassOptimizer.New(allegrosql.ErrWindowInvalidWindowFuncUse, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowInvalidWindowFuncUse])
	ErrWindowInvalidWindowFuncAliasUse              = terror.ClassOptimizer.New(allegrosql.ErrWindowInvalidWindowFuncAliasUse, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowInvalidWindowFuncAliasUse])
	ErrWindowNoSuchWindow                           = terror.ClassOptimizer.New(allegrosql.ErrWindowNoSuchWindow, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowNoSuchWindow])
	ErrWindowCircularityInWindowGraph               = terror.ClassOptimizer.New(allegrosql.ErrWindowCircularityInWindowGraph, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowCircularityInWindowGraph])
	ErrWindowNoChildPartitioning                    = terror.ClassOptimizer.New(allegrosql.ErrWindowNoChildPartitioning, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowNoChildPartitioning])
	ErrWindowNoInherentFrame                        = terror.ClassOptimizer.New(allegrosql.ErrWindowNoInherentFrame, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowNoInherentFrame])
	ErrWindowNoRedefineOrderBy                      = terror.ClassOptimizer.New(allegrosql.ErrWindowNoRedefineOrderBy, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowNoRedefineOrderBy])
	ErrWindowDuplicateName                          = terror.ClassOptimizer.New(allegrosql.ErrWindowDuplicateName, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowDuplicateName])
	ErrPartitionClauseOnNonpartitioned              = terror.ClassOptimizer.New(allegrosql.ErrPartitionClauseOnNonpartitioned, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPartitionClauseOnNonpartitioned])
	ErrWindowFrameStartIllegal                      = terror.ClassOptimizer.New(allegrosql.ErrWindowFrameStartIllegal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowFrameStartIllegal])
	ErrWindowFrameEndIllegal                        = terror.ClassOptimizer.New(allegrosql.ErrWindowFrameEndIllegal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowFrameEndIllegal])
	ErrWindowFrameIllegal                           = terror.ClassOptimizer.New(allegrosql.ErrWindowFrameIllegal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowFrameIllegal])
	ErrWindowRangeFrameOrderType                    = terror.ClassOptimizer.New(allegrosql.ErrWindowRangeFrameOrderType, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowRangeFrameOrderType])
	ErrWindowRangeFrameTemporalType                 = terror.ClassOptimizer.New(allegrosql.ErrWindowRangeFrameTemporalType, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowRangeFrameTemporalType])
	ErrWindowRangeFrameNumericType                  = terror.ClassOptimizer.New(allegrosql.ErrWindowRangeFrameNumericType, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowRangeFrameNumericType])
	ErrWindowRangeBoundNotCouplingConstantWithRadix = terror.ClassOptimizer.New(allegrosql.ErrWindowRangeBoundNotCouplingConstantWithRadix, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowRangeBoundNotCouplingConstantWithRadix])
	ErrWindowRowsIntervalUse                        = terror.ClassOptimizer.New(allegrosql.ErrWindowRowsIntervalUse, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowRowsIntervalUse])
	ErrWindowFunctionIgnoresFrame                   = terror.ClassOptimizer.New(allegrosql.ErrWindowFunctionIgnoresFrame, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWindowFunctionIgnoresFrame])
	ErrUnsupportedOnGeneratedDeferredCauset         = terror.ClassOptimizer.New(allegrosql.ErrUnsupportedOnGeneratedDeferredCauset, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedOnGeneratedDeferredCauset])
	ErrPrivilegeCheckFail                           = terror.ClassOptimizer.New(allegrosql.ErrPrivilegeCheckFail, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPrivilegeCheckFail])
	ErrInvalidWildCard                              = terror.ClassOptimizer.New(allegrosql.ErrInvalidWildCard, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidWildCard])
	ErrMixOfGroupFuncAndFields                      = terror.ClassOptimizer.New(allegrosql.ErrMixOfGroupFuncAndFieldsIncompatible, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrMixOfGroupFuncAndFieldsIncompatible])
	errTooBigPrecision                              = terror.ClassExpression.New(allegrosql.ErrTooBigPrecision, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooBigPrecision])
	ErrDBaccessDenied                               = terror.ClassOptimizer.New(allegrosql.ErrDBaccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDBaccessDenied])
	ErrBlockaccessDenied                            = terror.ClassOptimizer.New(allegrosql.ErrBlockaccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockaccessDenied])
	ErrSpecificAccessDenied                         = terror.ClassOptimizer.New(allegrosql.ErrSpecificAccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSpecificAccessDenied])
	ErrViewNoExplain                                = terror.ClassOptimizer.New(allegrosql.ErrViewNoExplain, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrViewNoExplain])
	ErrWrongValueCountOnRow                         = terror.ClassOptimizer.New(allegrosql.ErrWrongValueCountOnRow, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongValueCountOnRow])
	ErrViewInvalid                                  = terror.ClassOptimizer.New(allegrosql.ErrViewInvalid, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrViewInvalid])
	ErrNoSuchThread                                 = terror.ClassOptimizer.New(allegrosql.ErrNoSuchThread, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNoSuchThread])
	ErrUnknownDeferredCauset                        = terror.ClassOptimizer.New(allegrosql.ErrBadField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadField])
	ErrCartesianProductUnsupported                  = terror.ClassOptimizer.New(allegrosql.ErrCartesianProductUnsupported, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCartesianProductUnsupported])
	ErrStmtNotFound                                 = terror.ClassOptimizer.New(allegrosql.ErrPreparedStmtNotFound, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPreparedStmtNotFound])
	ErrAmbiguous                                    = terror.ClassOptimizer.New(allegrosql.ErrNonUniq, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNonUniq])
	// Since we cannot know if user logged in with a password, use message of ErrAccessDeniedNoPassword instead
	ErrAccessDenied = terror.ClassOptimizer.New(allegrosql.ErrAccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrAccessDeniedNoPassword])
)
