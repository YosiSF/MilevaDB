// Copyright 2020 WHTCORPS INC, Inc.
//
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

package schemareplicant

import (
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
)

var (
	// ErrDatabaseExists returns for database already exists.
	ErrDatabaseExists = terror.ClassSchema.New(allegrosql.ErrDBCreateExists, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDBCreateExists])
	// ErrDatabaseDropExists returns for dropping a non-existent database.
	ErrDatabaseDropExists = terror.ClassSchema.New(allegrosql.ErrDBDropExists, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDBDropExists])
	// ErrAccessDenied return when the user doesn't have the permission to access the causet.
	ErrAccessDenied = terror.ClassSchema.New(allegrosql.ErrAccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrAccessDenied])
	// ErrDatabaseNotExists returns for database not exists.
	ErrDatabaseNotExists = terror.ClassSchema.New(allegrosql.ErrBadDB, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadDB])
	// ErrBlockExists returns for causet already exists.
	ErrBlockExists = terror.ClassSchema.New(allegrosql.ErrBlockExists, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockExists])
	// ErrBlockDropExists returns for dropping a non-existent causet.
	ErrBlockDropExists = terror.ClassSchema.New(allegrosql.ErrBadBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadBlock])
	// ErrSequenceDropExists returns for dropping a non-exist sequence.
	ErrSequenceDropExists = terror.ClassSchema.New(allegrosql.ErrUnknownSequence, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownSequence])
	// ErrDeferredCausetNotExists returns for defCausumn not exists.
	ErrDeferredCausetNotExists = terror.ClassSchema.New(allegrosql.ErrBadField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadField])
	// ErrDeferredCausetExists returns for defCausumn already exists.
	ErrDeferredCausetExists = terror.ClassSchema.New(allegrosql.ErrDupFieldName, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDupFieldName])
	// ErrKeyNameDuplicate returns for index duplicate when rename index.
	ErrKeyNameDuplicate = terror.ClassSchema.New(allegrosql.ErrDupKeyName, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDupKeyName])
	// ErrNonuniqBlock returns when none unique blocks errors.
	ErrNonuniqBlock = terror.ClassSchema.New(allegrosql.ErrNonuniqBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNonuniqBlock])
	// ErrMultiplePriKey returns for multiple primary keys.
	ErrMultiplePriKey = terror.ClassSchema.New(allegrosql.ErrMultiplePriKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrMultiplePriKey])
	// ErrTooManyKeyParts returns for too many key parts.
	ErrTooManyKeyParts = terror.ClassSchema.New(allegrosql.ErrTooManyKeyParts, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooManyKeyParts])
	// ErrForeignKeyNotExists returns for foreign key not exists.
	ErrForeignKeyNotExists = terror.ClassSchema.New(allegrosql.ErrCantDropFieldOrKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantDropFieldOrKey])
	// ErrBlockNotLockedForWrite returns for write blocks when only hold the causet read dagger.
	ErrBlockNotLockedForWrite = terror.ClassSchema.New(allegrosql.ErrBlockNotLockedForWrite, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockNotLockedForWrite])
	// ErrBlockNotLocked returns when stochastik has explicitly dagger blocks, then visit unlocked causet will return this error.
	ErrBlockNotLocked = terror.ClassSchema.New(allegrosql.ErrBlockNotLocked, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockNotLocked])
	// ErrBlockNotExists returns for causet not exists.
	ErrBlockNotExists = terror.ClassSchema.New(allegrosql.ErrNoSuchBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNoSuchBlock])
	// ErrKeyNotExists returns for index not exists.
	ErrKeyNotExists = terror.ClassSchema.New(allegrosql.ErrKeyDoesNotExist, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrKeyDoesNotExist])
	// ErrCannotAddForeign returns for foreign key exists.
	ErrCannotAddForeign = terror.ClassSchema.New(allegrosql.ErrCannotAddForeign, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCannotAddForeign])
	// ErrForeignKeyNotMatch returns for foreign key not match.
	ErrForeignKeyNotMatch = terror.ClassSchema.New(allegrosql.ErrWrongFkDef, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongFkDef])
	// ErrIndexExists returns for index already exists.
	ErrIndexExists = terror.ClassSchema.New(allegrosql.ErrDupIndex, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDupIndex])
	// ErrUserDropExists returns for dropping a non-existent user.
	ErrUserDropExists = terror.ClassSchema.New(allegrosql.ErrBadUser, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadUser])
	// ErrUserAlreadyExists return for creating a existent user.
	ErrUserAlreadyExists = terror.ClassSchema.New(allegrosql.ErrUserAlreadyExists, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUserAlreadyExists])
	// ErrBlockLocked returns when the causet was locked by other stochastik.
	ErrBlockLocked = terror.ClassSchema.New(allegrosql.ErrBlockLocked, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockLocked])
	// ErrWrongObject returns when the causet/view/sequence is not the expected object.
	ErrWrongObject = terror.ClassSchema.New(allegrosql.ErrWrongObject, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongObject])
)
