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

package executor

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
)

// Error instances.
var (
	ErrGetStartTS      = terror.ClassExecutor.New(allegrosql.ErrGetStartTS, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrGetStartTS])
	ErrUnknownPlan     = terror.ClassExecutor.New(allegrosql.ErrUnknownPlan, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownPlan])
	ErrPrepareMulti    = terror.ClassExecutor.New(allegrosql.ErrPrepareMulti, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPrepareMulti])
	ErrPrepareDBS      = terror.ClassExecutor.New(allegrosql.ErrPrepareDBS, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPrepareDBS])
	ErrResultIsEmpty   = terror.ClassExecutor.New(allegrosql.ErrResultIsEmpty, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrResultIsEmpty])
	ErrBuildExecutor   = terror.ClassExecutor.New(allegrosql.ErrBuildExecutor, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBuildExecutor])
	ErrBatchInsertFail = terror.ClassExecutor.New(allegrosql.ErrBatchInsertFail, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBatchInsertFail])

	ErrCantCreateUserWithGrant     = terror.ClassExecutor.New(allegrosql.ErrCantCreateUserWithGrant, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantCreateUserWithGrant])
	ErrPasswordNoMatch             = terror.ClassExecutor.New(allegrosql.ErrPasswordNoMatch, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPasswordNoMatch])
	ErrCannotUser                  = terror.ClassExecutor.New(allegrosql.ErrCannotUser, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCannotUser])
	ErrPasswordFormat              = terror.ClassExecutor.New(allegrosql.ErrPasswordFormat, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPasswordFormat])
	ErrCantChangeTxCharacteristics = terror.ClassExecutor.New(allegrosql.ErrCantChangeTxCharacteristics, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantChangeTxCharacteristics])
	ErrPsManyParam                 = terror.ClassExecutor.New(allegrosql.ErrPsManyParam, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPsManyParam])
	ErrAdminCheckBlock             = terror.ClassExecutor.New(allegrosql.ErrAdminCheckBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrAdminCheckBlock])
	ErrDBaccessDenied              = terror.ClassExecutor.New(allegrosql.ErrDBaccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDBaccessDenied])
	ErrBlockaccessDenied           = terror.ClassExecutor.New(allegrosql.ErrBlockaccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockaccessDenied])
	ErrBadDB                       = terror.ClassExecutor.New(allegrosql.ErrBadDB, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadDB])
	ErrWrongObject                 = terror.ClassExecutor.New(allegrosql.ErrWrongObject, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongObject])
	ErrRoleNotGranted              = terror.ClassPrivilege.New(allegrosql.ErrRoleNotGranted, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrRoleNotGranted])
	ErrDeadlock                    = terror.ClassExecutor.New(allegrosql.ErrLockDeadlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrLockDeadlock])
	ErrQueryInterrupted            = terror.ClassExecutor.New(allegrosql.ErrQueryInterrupted, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrQueryInterrupted])

	ErrBRIEBackupFailed  = terror.ClassExecutor.New(allegrosql.ErrBRIEBackupFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBRIEBackupFailed])
	ErrBRIERestoreFailed = terror.ClassExecutor.New(allegrosql.ErrBRIERestoreFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBRIERestoreFailed])
	ErrBRIEImportFailed  = terror.ClassExecutor.New(allegrosql.ErrBRIEImportFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBRIEImportFailed])
	ErrBRIEExportFailed  = terror.ClassExecutor.New(allegrosql.ErrBRIEExportFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBRIEExportFailed])
)
