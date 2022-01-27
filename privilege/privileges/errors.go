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

package privileges

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
)

// error definitions.
var (
	errInvalidPrivilegeType = terror.ClassPrivilege.New(allegrosql.ErrInvalidPrivilegeType, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidPrivilegeType])
	ErrNonexistingGrant     = terror.ClassPrivilege.New(allegrosql.ErrNonexistingGrant, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNonexistingGrant])
	errLoadPrivilege        = terror.ClassPrivilege.New(allegrosql.ErrLoadPrivilege, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrLoadPrivilege])
)
