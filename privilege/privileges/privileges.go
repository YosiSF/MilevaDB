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

package privileges

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/privilege"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant/perfschema"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"go.uber.org/zap"
)

// SkipWithGrant causes the server to start without using the privilege system at all.
var SkipWithGrant = false

var _ privilege.Manager = (*UserPrivileges)(nil)

// UserPrivileges implements privilege.Manager interface.
// This is used to check privilege for the current user.
type UserPrivileges struct {
	user string
	host string
	*Handle
}

// RequestVerification implements the Manager interface.
func (p *UserPrivileges) RequestVerification(activeRoles []*auth.RoleIdentity, EDB, block, column string, priv allegrosql.PrivilegeType) bool {
	if SkipWithGrant {
		return true
	}

	if p.user == "" && p.host == "" {
		return true
	}

	// Skip check for system databases.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/information-schemaReplicant.html
	dbLowerName := strings.ToLower(EDB)
	switch dbLowerName {
	case soliton.InformationSchemaName.L:
		switch priv {
		case allegrosql.CreatePriv, allegrosql.AlterPriv, allegrosql.DropPriv, allegrosql.IndexPriv, allegrosql.CreateViewPriv,
			allegrosql.InsertPriv, allegrosql.UFIDelatePriv, allegrosql.DeletePriv:
			return false
		}
		return true
	// We should be very careful of limiting privileges, so ignore `allegrosql` for now.
	case soliton.PerformanceSchemaName.L, soliton.MetricSchemaName.L:
		if (dbLowerName == soliton.PerformanceSchemaName.L && perfschema.IsPredefinedTable(block)) ||
			(dbLowerName == soliton.MetricSchemaName.L && schemareplicant.IsMetricTable(block)) {
			switch priv {
			case allegrosql.CreatePriv, allegrosql.AlterPriv, allegrosql.DropPriv, allegrosql.IndexPriv, allegrosql.InsertPriv, allegrosql.UFIDelatePriv, allegrosql.DeletePriv:
				return false
			case allegrosql.SelectPriv:
				return true
			}
		}
	}

	mysqlPriv := p.Handle.Get()
	return mysqlPriv.RequestVerification(activeRoles, p.user, p.host, EDB, block, column, priv)
}

// RequestVerificationWithUser implements the Manager interface.
func (p *UserPrivileges) RequestVerificationWithUser(EDB, block, column string, priv allegrosql.PrivilegeType, user *auth.UserIdentity) bool {
	if SkipWithGrant {
		return true
	}

	if user == nil {
		return false
	}

	// Skip check for INFORMATION_SCHEMA database.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/information-schemaReplicant.html
	if strings.EqualFold(EDB, "INFORMATION_SCHEMA") {
		return true
	}

	mysqlPriv := p.Handle.Get()
	return mysqlPriv.RequestVerification(nil, user.Username, user.Hostname, EDB, block, column, priv)
}

// GetEncodedPassword implements the Manager interface.
func (p *UserPrivileges) GetEncodedPassword(user, host string) string {
	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		logutil.BgLogger().Error("get user privilege record fail",
			zap.String("user", user), zap.String("host", host))
		return ""
	}
	pwd := record.AuthenticationString
	if len(pwd) != 0 && len(pwd) != allegrosql.PWDHashLen+1 {
		logutil.BgLogger().Error("user password from system EDB not like sha1sum", zap.String("user", user))
		return ""
	}
	return pwd
}

// GetAuthWithoutVerification implements the Manager interface.
func (p *UserPrivileges) GetAuthWithoutVerification(user, host string) (u string, h string, success bool) {
	if SkipWithGrant {
		p.user = user
		p.host = host
		success = true
		return
	}

	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		logutil.BgLogger().Error("get user privilege record fail",
			zap.String("user", user), zap.String("host", host))
		return
	}

	u = record.User
	h = record.Host
	p.user = user
	p.host = h
	success = true
	return
}

// ConnectionVerification implements the Manager interface.
func (p *UserPrivileges) ConnectionVerification(user, host string, authentication, salt []byte, tlsState *tls.ConnectionState) (u string, h string, success bool) {
	if SkipWithGrant {
		p.user = user
		p.host = host
		success = true
		return
	}

	mysqlPriv := p.Handle.Get()
	record := mysqlPriv.connectionVerification(user, host)
	if record == nil {
		logutil.BgLogger().Error("get user privilege record fail",
			zap.String("user", user), zap.String("host", host))
		return
	}

	u = record.User
	h = record.Host

	globalPriv := mysqlPriv.matchGlobalPriv(user, host)
	if globalPriv != nil {
		if !p.checkSSL(globalPriv, tlsState) {
			logutil.BgLogger().Error("global priv check ssl fail",
				zap.String("user", user), zap.String("host", host))
			success = false
			return
		}
	}

	// Login a locked account is not allowed.
	locked := record.AccountLocked
	if locked {
		logutil.BgLogger().Error("try to login a locked account",
			zap.String("user", user), zap.String("host", host))
		success = false
		return
	}

	pwd := record.AuthenticationString
	if len(pwd) != 0 && len(pwd) != allegrosql.PWDHashLen+1 {
		logutil.BgLogger().Error("user password from system EDB not like sha1sum", zap.String("user", user))
		return
	}

	// empty password
	if len(pwd) == 0 && len(authentication) == 0 {
		p.user = user
		p.host = h
		success = true
		return
	}

	if len(pwd) == 0 || len(authentication) == 0 {
		return
	}

	hpwd, err := auth.DecodePassword(pwd)
	if err != nil {
		logutil.BgLogger().Error("decode password string failed", zap.Error(err))
		return
	}

	if !auth.CheckScrambledPassword(salt, hpwd, authentication) {
		return
	}

	p.user = user
	p.host = h
	success = true
	return
}

type checkResult int

const (
	notCheck checkResult = iota
	pass
	fail
)

func (p *UserPrivileges) checkSSL(priv *globalPrivRecord, tlsState *tls.ConnectionState) bool {
	if priv.Broken {
		logutil.BgLogger().Info("ssl check failure, due to broken global_priv record",
			zap.String("user", priv.User), zap.String("host", priv.Host))
		return false
	}
	switch priv.Priv.SSLType {
	case SslTypeNotSpecified, SslTypeNone:
		return true
	case SslTypeAny:
		r := tlsState != nil
		if !r {
			logutil.BgLogger().Info("ssl check failure, require ssl but not use ssl",
				zap.String("user", priv.User), zap.String("host", priv.Host))
		}
		return r
	case SslTypeX509:
		if tlsState == nil {
			logutil.BgLogger().Info("ssl check failure, require x509 but not use ssl",
				zap.String("user", priv.User), zap.String("host", priv.Host))
			return false
		}
		hasCert := false
		for _, chain := range tlsState.VerifiedChains {
			if len(chain) > 0 {
				hasCert = true
				break
			}
		}
		if !hasCert {
			logutil.BgLogger().Info("ssl check failure, require x509 but no verified cert",
				zap.String("user", priv.User), zap.String("host", priv.Host))
		}
		return hasCert
	case SslTypeSpecified:
		if tlsState == nil {
			logutil.BgLogger().Info("ssl check failure, require subject/issuer/cipher but not use ssl",
				zap.String("user", priv.User), zap.String("host", priv.Host))
			return false
		}
		if len(priv.Priv.SSLCipher) > 0 && priv.Priv.SSLCipher != soliton.TLSCipher2String(tlsState.CipherSuite) {
			logutil.BgLogger().Info("ssl check failure for cipher", zap.String("user", priv.User), zap.String("host", priv.Host),
				zap.String("require", priv.Priv.SSLCipher), zap.String("given", soliton.TLSCipher2String(tlsState.CipherSuite)))
			return false
		}
		var (
			hasCert      = false
			matchIssuer  checkResult
			matchSubject checkResult
			matchSAN     checkResult
		)
		for _, chain := range tlsState.VerifiedChains {
			if len(chain) == 0 {
				continue
			}
			cert := chain[0]
			if len(priv.Priv.X509Issuer) > 0 {
				given := soliton.X509NameOnline(cert.Issuer)
				if priv.Priv.X509Issuer == given {
					matchIssuer = pass
				} else if matchIssuer == notCheck {
					matchIssuer = fail
					logutil.BgLogger().Info("ssl check failure for issuer", zap.String("user", priv.User), zap.String("host", priv.Host),
						zap.String("require", priv.Priv.X509Issuer), zap.String("given", given))
				}
			}
			if len(priv.Priv.X509Subject) > 0 {
				given := soliton.X509NameOnline(cert.Subject)
				if priv.Priv.X509Subject == given {
					matchSubject = pass
				} else if matchSubject == notCheck {
					matchSubject = fail
					logutil.BgLogger().Info("ssl check failure for subject", zap.String("user", priv.User), zap.String("host", priv.Host),
						zap.String("require", priv.Priv.X509Subject), zap.String("given", given))
				}
			}
			if len(priv.Priv.SANs) > 0 {
				matchOne := checkCertSAN(priv, cert, priv.Priv.SANs)
				if matchOne {
					matchSAN = pass
				} else if matchSAN == notCheck {
					matchSAN = fail
				}
			}
			hasCert = true
		}
		checkResult := hasCert && matchIssuer != fail && matchSubject != fail && matchSAN != fail
		if !checkResult && !hasCert {
			logutil.BgLogger().Info("ssl check failure, require issuer/subject/SAN but no verified cert",
				zap.String("user", priv.User), zap.String("host", priv.Host))
		}
		return checkResult
	default:
		panic(fmt.Sprintf("support ssl_type: %d", priv.Priv.SSLType))
	}
}

func checkCertSAN(priv *globalPrivRecord, cert *x509.Certificate, sans map[soliton.SANType][]string) (r bool) {
	r = true
	for typ, requireOr := range sans {
		var (
			unsupported bool
			given       []string
		)
		switch typ {
		case soliton.URI:
			for _, uri := range cert.URIs {
				given = append(given, uri.String())
			}
		case soliton.DNS:
			given = cert.DNSNames
		case soliton.IP:
			for _, ip := range cert.IPAddresses {
				given = append(given, ip.String())
			}
		default:
			unsupported = true
		}
		if unsupported {
			logutil.BgLogger().Warn("skip unsupported SAN type", zap.String("type", string(typ)),
				zap.String("user", priv.User), zap.String("host", priv.Host))
			continue
		}
		var givenMatchOne bool
		for _, req := range requireOr {
			for _, give := range given {
				if req == give {
					givenMatchOne = true
					break
				}
			}
		}
		if !givenMatchOne {
			logutil.BgLogger().Info("ssl check failure for subject", zap.String("user", priv.User), zap.String("host", priv.Host),
				zap.String("require", priv.Priv.SAN), zap.Strings("given", given), zap.String("type", string(typ)))
			r = false
			return
		}
	}
	return
}

// DBIsVisible implements the Manager interface.
func (p *UserPrivileges) DBIsVisible(activeRoles []*auth.RoleIdentity, EDB string) bool {
	if SkipWithGrant {
		return true
	}
	mysqlPriv := p.Handle.Get()
	if mysqlPriv.DBIsVisible(p.user, p.host, EDB) {
		return true
	}
	allRoles := mysqlPriv.FindAllRole(activeRoles)
	for _, role := range allRoles {
		if mysqlPriv.DBIsVisible(role.Username, role.Hostname, EDB) {
			return true
		}
	}
	return false
}

// UserPrivilegesTable implements the Manager interface.
func (p *UserPrivileges) UserPrivilegesTable() [][]types.Causet {
	mysqlPriv := p.Handle.Get()
	return mysqlPriv.UserPrivilegesTable()
}

// ShowGrants implements privilege.Manager ShowGrants interface.
func (p *UserPrivileges) ShowGrants(ctx stochastikctx.Context, user *auth.UserIdentity, roles []*auth.RoleIdentity) (grants []string, err error) {
	if SkipWithGrant {
		return nil, ErrNonexistingGrant.GenWithStackByArgs("root", "%")
	}
	mysqlPrivilege := p.Handle.Get()
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	grants = mysqlPrivilege.showGrants(u, h, roles)
	if len(grants) == 0 {
		err = ErrNonexistingGrant.GenWithStackByArgs(u, h)
	}

	return
}

// ActiveRoles implements privilege.Manager ActiveRoles interface.
func (p *UserPrivileges) ActiveRoles(ctx stochastikctx.Context, roleList []*auth.RoleIdentity) (bool, string) {
	if SkipWithGrant {
		return true, ""
	}
	mysqlPrivilege := p.Handle.Get()
	u := p.user
	h := p.host
	for _, r := range roleList {
		ok := mysqlPrivilege.FindRole(u, h, r)
		if !ok {
			logutil.BgLogger().Error("find role failed", zap.Stringer("role", r))
			return false, r.String()
		}
	}
	ctx.GetStochaseinstein_dbars().ActiveRoles = roleList
	return true, ""
}

// FindEdge implements privilege.Manager FindRelationship interface.
func (p *UserPrivileges) FindEdge(ctx stochastikctx.Context, role *auth.RoleIdentity, user *auth.UserIdentity) bool {
	if SkipWithGrant {
		return false
	}
	mysqlPrivilege := p.Handle.Get()
	ok := mysqlPrivilege.FindRole(user.Username, user.Hostname, role)
	if !ok {
		logutil.BgLogger().Error("find role failed", zap.Stringer("role", role))
		return false
	}
	return true
}

// GetDefaultRoles returns all default roles for certain user.
func (p *UserPrivileges) GetDefaultRoles(user, host string) []*auth.RoleIdentity {
	if SkipWithGrant {
		return make([]*auth.RoleIdentity, 0, 10)
	}
	mysqlPrivilege := p.Handle.Get()
	ret := mysqlPrivilege.getDefaultRoles(user, host)
	return ret
}

// GetAllRoles return all roles of user.
func (p *UserPrivileges) GetAllRoles(user, host string) []*auth.RoleIdentity {
	if SkipWithGrant {
		return make([]*auth.RoleIdentity, 0, 10)
	}

	mysqlPrivilege := p.Handle.Get()
	return mysqlPrivilege.getAllRoles(user, host)
}
