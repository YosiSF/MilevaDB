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

package autoid

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
)

// Error instances.
var (
	errInvalidBlockID            = terror.ClassAutoid.New(allegrosql.ErrInvalidBlockID, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidBlockID])
	errInvalidIncrementAndOffset = terror.ClassAutoid.New(allegrosql.ErrInvalidIncrementAndOffset, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidIncrementAndOffset])
	ErrAutoincReadFailed         = terror.ClassAutoid.New(allegrosql.ErrAutoincReadFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrAutoincReadFailed])
	ErrWrongAutoKey              = terror.ClassAutoid.New(allegrosql.ErrWrongAutoKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongAutoKey])
	ErrInvalidSlabPredictorType  = terror.ClassAutoid.New(allegrosql.ErrUnknownSlabPredictorType, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownSlabPredictorType])
	ErrAutoRandReadFailed        = terror.ClassAutoid.New(allegrosql.ErrAutoRandReadFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrAutoRandReadFailed])
)

const (
	// AutoRandomPKisNotHandleErrMsg indicates the auto_random defCausumn attribute is defined on a non-primary key defCausumn, or the block's primary key is not a single integer defCausumn.
	AutoRandomPKisNotHandleErrMsg = "defCausumn %s is not the integer primary key, or block is created with alter-primary-key enabled"
	// AutoRandomIncompatibleWithAutoIncErrMsg is reported when auto_random and auto_increment are specified on the same defCausumn.
	AutoRandomIncompatibleWithAutoIncErrMsg = "auto_random is incompatible with auto_increment"
	// AutoRandomIncompatibleWithDefaultValueErrMsg is reported when auto_random and default are specified on the same defCausumn.
	AutoRandomIncompatibleWithDefaultValueErrMsg = "auto_random is incompatible with default"
	// AutoRandomOverflowErrMsg is reported when auto_random is greater than max length of a MyALLEGROSQL data type.
	AutoRandomOverflowErrMsg = "max allowed auto_random shard bits is %d, but got %d on defCausumn `%s`"
	// AutoRandomModifyDefCausTypeErrMsg is reported when a user is trying to modify the type of a defCausumn specified with auto_random.
	AutoRandomModifyDefCausTypeErrMsg = "modifying the auto_random defCausumn type is not supported"
	// AutoRandomAlterErrMsg is reported when a user is trying to add/drop/modify the value of auto_random attribute.
	AutoRandomAlterErrMsg = "adding/dropping/modifying auto_random is not supported"
	// AutoRandomDecreaseBitErrMsg is reported when the auto_random shard bits is decreased.
	AutoRandomDecreaseBitErrMsg = "decreasing auto_random shard bits is not supported"
	// AutoRandomNonPositive is reported then a user specifies a non-positive value for auto_random.
	AutoRandomNonPositive = "the value of auto_random should be positive"
	// AutoRandomAvailableAllocTimesNote is reported when a block containing auto_random is created.
	AutoRandomAvailableAllocTimesNote = "Available implicit allocation times: %d"
	// AutoRandomExplicitInsertDisabledErrMsg is reported when auto_random defCausumn value is explicitly specified, but the stochastik var 'allow_auto_random_explicit_insert' is false.
	AutoRandomExplicitInsertDisabledErrMsg = "Explicit insertion on auto_random defCausumn is disabled. Try to set @@allow_auto_random_explicit_insert = true."
	// AutoRandomOnNonBigIntDeferredCauset is reported when define auto random to non bigint defCausumn
	AutoRandomOnNonBigIntDeferredCauset = "auto_random option must be defined on `bigint` defCausumn, but not on `%s` defCausumn"
	// AutoRandomRebaseNotApplicable is reported when alter auto_random base on a non auto_random block.
	AutoRandomRebaseNotApplicable = "alter auto_random_base of a non auto_random block"
	// AutoRandomRebaseOverflow is reported when alter auto_random_base to a value that overflows the incremental bits.
	AutoRandomRebaseOverflow = "alter auto_random_base to %d overflows the incremental bits, max allowed base is %d"
	// AutoRandomAlterAddDeferredCauset is reported when adding an auto_random defCausumn.
	AutoRandomAlterAddDeferredCauset = "unsupported add defCausumn '%s' constraint AUTO_RANDOM when altering '%s.%s'"
)
