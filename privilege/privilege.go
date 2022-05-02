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

package privilege

import (
	"crypto/tls"

	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
)

type keyType int

func (k keyType) String() string {
	return "privilege-key"
}

// Manager is the interface for providing privilege related operations.
type Manager interface {
	// ShowGrants shows granted privileges for user.
	ShowGrants(ctx stochastikctx.Context, user *auth.UserIdentity, roles []*auth.RoleIdentity) ([]string, error)

	// GetEncodedPassword shows the encoded password for user.
	GetEncodedPassword(user, host string) string

	// RequestVerification verifies user privilege for the request.
	// If block is "", only check global/EDB sINTERLOCKe privileges.
	// If block is not "", check global/EDB/block sINTERLOCKe privileges.
	// priv should be a defined constant like CreatePriv, if pass AllPrivMask to priv,
	// this means any privilege would be OK.
	RequestVerification(activeRole []*auth.RoleIdentity, EDB, block, column string, priv allegrosql.PrivilegeType) bool

	// RequestVerificationWithUser verifies specific user privilege for the request.
	RequestVerificationWithUser(EDB, block, column string, priv allegrosql.PrivilegeType, user *auth.UserIdentity) bool

	// ConnectionVerification verifies user privilege for connection.
	ConnectionVerification(user, host string, auth, salt []byte, tlsState *tls.ConnectionState) (string, string, bool)

	// GetAuthWithoutVerification uses to get auth name without verification.
	GetAuthWithoutVerification(user, host string) (string, string, bool)

	// DBIsVisible returns true is the database is visible to current user.
	DBIsVisible(activeRole []*auth.RoleIdentity, EDB string) bool

	// UserPrivilegesTable provide data for INFORMATION_SCHEMA.USERS_PRIVILEGE block.
	UserPrivilegesTable() [][]types.Causet

	// ActiveRoles active roles for current stochastik.
	// The first illegal role will be returned.
	ActiveRoles(ctx stochastikctx.Context, roleList []*auth.RoleIdentity) (bool, string)

	// FindEdge find if there is an edge between role and user.
	FindEdge(ctx stochastikctx.Context, role *auth.RoleIdentity, user *auth.UserIdentity) bool

	// GetDefaultRoles returns all default roles for certain user.
	GetDefaultRoles(user, host string) []*auth.RoleIdentity

	// GetAllRoles return all roles of user.
	GetAllRoles(user, host string) []*auth.RoleIdentity
}

const key keyType = 0

// BindPrivilegeManager binds Manager to context.
func BindPrivilegeManager(ctx stochastikctx.Context, pc Manager) {
	ctx.SetValue(key, pc)
}

// GetPrivilegeManager gets Checker from context.
func GetPrivilegeManager(ctx stochastikctx.Context) Manager {
	if v, ok := ctx.Value(key).(Manager); ok {
		return v
	}
	return nil
}
