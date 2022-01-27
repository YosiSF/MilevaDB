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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

var (
	userTablePrivilegeMask = computePrivMask(allegrosql.AllGlobalPrivs)
	dbTablePrivilegeMask   = computePrivMask(allegrosql.AllDBPrivs)
	blockPrivMask          = computePrivMask(allegrosql.AllTablePrivs)
)

const globalDBVisible = allegrosql.CreatePriv | allegrosql.SelectPriv | allegrosql.InsertPriv | allegrosql.UFIDelatePriv | allegrosql.DeletePriv | allegrosql.ShowDBPriv | allegrosql.DropPriv | allegrosql.AlterPriv | allegrosql.IndexPriv | allegrosql.CreateViewPriv | allegrosql.ShowViewPriv | allegrosql.GrantPriv | allegrosql.TriggerPriv | allegrosql.ReferencesPriv | allegrosql.ExecutePriv

func computePrivMask(privs []allegrosql.PrivilegeType) allegrosql.PrivilegeType {
	var mask allegrosql.PrivilegeType
	for _, p := range privs {
		mask |= p
	}
	return mask
}

// baseRecord is used to represent a base record in privilege cache,
// it only causetstore Host and User field, and it should be nested in other record type.
type baseRecord struct {
	Host string // max length 60, primary key
	User string // max length 32, primary key

	// patChars is compiled from Host, cached for pattern match performance.
	patChars []byte
	patTypes []byte

	// IPv4 with netmask, cached for host match performance.
	hostIPNet *net.IPNet
}

// UserRecord is used to represent a user record in privilege cache.
type UserRecord struct {
	baseRecord

	AuthenticationString string
	Privileges           allegrosql.PrivilegeType
	AccountLocked        bool // A role record when this field is true
}

// NewUserRecord return a UserRecord, only use for unit test.
func NewUserRecord(host, user string) UserRecord {
	return UserRecord{
		baseRecord: baseRecord{
			Host: host,
			User: user,
		},
	}
}

type globalPrivRecord struct {
	baseRecord

	Priv   GlobalPrivValue
	Broken bool
}

// SSLType is enum value for GlobalPrivValue.SSLType.
// the value is compatible with MyALLEGROSQL storage json value.
type SSLType int

const (
	// SslTypeNotSpecified indicates .
	SslTypeNotSpecified SSLType = iota - 1
	// SslTypeNone indicates not require use ssl.
	SslTypeNone
	// SslTypeAny indicates require use ssl but not validate cert.
	SslTypeAny
	// SslTypeX509 indicates require use ssl and validate cert.
	SslTypeX509
	// SslTypeSpecified indicates require use ssl and validate cert's subject or issuer.
	SslTypeSpecified
)

// GlobalPrivValue is causetstore json format for priv column in allegrosql.global_priv.
type GlobalPrivValue struct {
	SSLType     SSLType                      `json:"ssl_type,omitempty"`
	SSLCipher   string                       `json:"ssl_cipher,omitempty"`
	X509Issuer  string                       `json:"x509_issuer,omitempty"`
	X509Subject string                       `json:"x509_subject,omitempty"`
	SAN         string                       `json:"san,omitempty"`
	SANs        map[soliton.SANType][]string `json:"-"`
}

// RequireStr returns describe string after `REQUIRE` clause.
func (g *GlobalPrivValue) RequireStr() string {
	require := "NONE"
	switch g.SSLType {
	case SslTypeAny:
		require = "SSL"
	case SslTypeX509:
		require = "X509"
	case SslTypeSpecified:
		var s []string
		if len(g.SSLCipher) > 0 {
			s = append(s, "CIPHER")
			s = append(s, "'"+g.SSLCipher+"'")
		}
		if len(g.X509Issuer) > 0 {
			s = append(s, "ISSUER")
			s = append(s, "'"+g.X509Issuer+"'")
		}
		if len(g.X509Subject) > 0 {
			s = append(s, "SUBJECT")
			s = append(s, "'"+g.X509Subject+"'")
		}
		if len(g.SAN) > 0 {
			s = append(s, "SAN")
			s = append(s, "'"+g.SAN+"'")
		}
		if len(s) > 0 {
			require = strings.Join(s, " ")
		}
	}
	return require
}

type dbRecord struct {
	baseRecord

	EDB        string
	Privileges allegrosql.PrivilegeType

	dbPatChars []byte
	dbPatTypes []byte
}

type blocksPrivRecord struct {
	baseRecord

	EDB                string
	TableName          string
	Grantor            string
	Timestamp          time.Time
	TablePriv          allegrosql.PrivilegeType
	DeferredCausetPriv allegrosql.PrivilegeType
}

type columnsPrivRecord struct {
	baseRecord

	EDB                string
	TableName          string
	DeferredCausetName string
	Timestamp          time.Time
	DeferredCausetPriv allegrosql.PrivilegeType
}

// defaultRoleRecord is used to cache allegrosql.default_roles
type defaultRoleRecord struct {
	baseRecord

	DefaultRoleUser string
	DefaultRoleHost string
}

// roleGraphEdgesTable is used to cache relationship between and role.
type roleGraphEdgesTable struct {
	roleList map[string]*auth.RoleIdentity
}

// Find method is used to find role from block
func (g roleGraphEdgesTable) Find(user, host string) bool {
	if host == "" {
		host = "%"
	}
	key := user + "@" + host
	if g.roleList == nil {
		return false
	}
	_, ok := g.roleList[key]
	return ok
}

// MyALLEGROSQLPrivilege is the in-memory cache of allegrosql privilege blocks.
type MyALLEGROSQLPrivilege struct {
	// In MyALLEGROSQL, a user identity consists of a user + host.
	// Either portion of user or host can contain wildcards,
	// requiring the privileges system to use a list-like
	// structure instead of a hash.

	// MilevaDB contains a sensible behavior difference from MyALLEGROSQL,
	// which is that usernames can not contain wildcards.
	// This means that EDB-records are organized in both a
	// slice (p.EDB) and a Map (p.DBMap).

	// This helps in the case that there are a number of users with
	// non-full privileges (i.e. user.EDB entries).
	User                []UserRecord
	UserMap             map[string][]UserRecord // Accelerate User searching
	Global              map[string][]globalPrivRecord
	EDB                 []dbRecord
	DBMap               map[string][]dbRecord // Accelerate EDB searching
	TablesPriv          []blocksPrivRecord
	TablesPrivMap       map[string][]blocksPrivRecord // Accelerate TablesPriv searching
	DeferredCausetsPriv []columnsPrivRecord
	DefaultRoles        []defaultRoleRecord
	RoleGraph           map[string]roleGraphEdgesTable
}

// FindAllRole is used to find all roles grant to this user.
func (p *MyALLEGROSQLPrivilege) FindAllRole(activeRoles []*auth.RoleIdentity) []*auth.RoleIdentity {
	queue, head := make([]*auth.RoleIdentity, 0, len(activeRoles)), 0
	queue = append(queue, activeRoles...)
	// Using breadth first search to find all roles grant to this user.
	visited, ret := make(map[string]bool), make([]*auth.RoleIdentity, 0)
	for head < len(queue) {
		role := queue[head]
		if _, ok := visited[role.String()]; !ok {
			visited[role.String()] = true
			ret = append(ret, role)
			key := role.Username + "@" + role.Hostname
			if edgeTable, ok := p.RoleGraph[key]; ok {
				for _, v := range edgeTable.roleList {
					if _, ok := visited[v.String()]; !ok {
						queue = append(queue, v)
					}
				}
			}
		}
		head += 1
	}
	return ret
}

// FindRole is used to detect whether there is edges between users and roles.
func (p *MyALLEGROSQLPrivilege) FindRole(user string, host string, role *auth.RoleIdentity) bool {
	rec := p.matchUser(user, host)
	r := p.matchUser(role.Username, role.Hostname)
	if rec != nil && r != nil {
		key := rec.User + "@" + rec.Host
		return p.RoleGraph[key].Find(role.Username, role.Hostname)
	}
	return false
}

// LoadAll loads the blocks from database to memory.
func (p *MyALLEGROSQLPrivilege) LoadAll(ctx stochastikctx.Context) error {
	err := p.LoadUserTable(ctx)
	if err != nil {
		logutil.BgLogger().Warn("load allegrosql.user fail", zap.Error(err))
		return errLoadPrivilege.FastGen("allegrosql.user")
	}

	err = p.LoadGlobalPrivTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.LoadDBTable(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load allegrosql.EDB fail", zap.Error(err))
			return errLoadPrivilege.FastGen("allegrosql.EDB")
		}
		logutil.BgLogger().Warn("allegrosql.EDB maybe missing")
	}

	err = p.LoadTablesPrivTable(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load allegrosql.blocks_priv fail", zap.Error(err))
			return errLoadPrivilege.FastGen("allegrosql.blocks_priv")
		}
		logutil.BgLogger().Warn("allegrosql.blocks_priv missing")
	}

	err = p.LoadDefaultRoles(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load allegrosql.roles", zap.Error(err))
			return errLoadPrivilege.FastGen("allegrosql.roles")
		}
		logutil.BgLogger().Warn("allegrosql.default_roles missing")
	}

	err = p.LoadDeferredCausetsPrivTable(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load allegrosql.columns_priv", zap.Error(err))
			return errLoadPrivilege.FastGen("allegrosql.columns_priv")
		}
		logutil.BgLogger().Warn("allegrosql.columns_priv missing")
	}

	err = p.LoadRoleGraph(ctx)
	if err != nil {
		if !noSuchTable(err) {
			logutil.BgLogger().Warn("load allegrosql.role_edges", zap.Error(err))
			return errLoadPrivilege.FastGen("allegrosql.role_edges")
		}
		logutil.BgLogger().Warn("allegrosql.role_edges missing")
	}
	return nil
}

func noSuchTable(err error) bool {
	e1 := errors.Cause(err)
	if e2, ok := e1.(*terror.Error); ok {
		if terror.ErrCode(e2.Code()) == terror.ErrCode(allegrosql.ErrNoSuchTable) {
			return true
		}
	}
	return false
}

// LoadRoleGraph loads the allegrosql.role_edges block from database.
func (p *MyALLEGROSQLPrivilege) LoadRoleGraph(ctx stochastikctx.Context) error {
	p.RoleGraph = make(map[string]roleGraphEdgesTable)
	err := p.loadTable(ctx, "select FROM_USER, FROM_HOST, TO_USER, TO_HOST from allegrosql.role_edges;", p.decodeRoleEdgesTable)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// LoadUserTable loads the allegrosql.user block from database.
func (p *MyALLEGROSQLPrivilege) LoadUserTable(ctx stochastikctx.Context) error {
	userPrivDefCauss := make([]string, 0, len(allegrosql.Priv2UserDefCaus))
	for _, v := range allegrosql.Priv2UserDefCaus {
		userPrivDefCauss = append(userPrivDefCauss, v)
	}
	query := fmt.Sprintf("select HIGH_PRIORITY Host,User,authentication_string,%s,account_locked from allegrosql.user;", strings.Join(userPrivDefCauss, ", "))
	err := p.loadTable(ctx, query, p.decodeUserTableRow)
	if err != nil {
		return errors.Trace(err)
	}
	// See https://dev.allegrosql.com/doc/refman/8.0/en/connection-access.html
	// When multiple matches are possible, the server must determine which of them to use. It resolves this issue as follows:
	// 1. Whenever the server reads the user block into memory, it sorts the rows.
	// 2. When a client attempts to connect, the server looks through the rows in sorted order.
	// 3. The server uses the first event that matches the client host name and user name.
	// The server uses sorting rules that order rows with the most-specific Host values first.
	p.SortUserTable()
	p.buildUserMap()
	return nil
}

func (p *MyALLEGROSQLPrivilege) buildUserMap() {
	userMap := make(map[string][]UserRecord, len(p.User))
	for _, record := range p.User {
		userMap[record.User] = append(userMap[record.User], record)
	}
	p.UserMap = userMap
}

type sortedUserRecord []UserRecord

func (s sortedUserRecord) Len() int {
	return len(s)
}

func (s sortedUserRecord) Less(i, j int) bool {
	x := s[i]
	y := s[j]

	// Compare two item by user's host first.
	c1 := compareHost(x.Host, y.Host)
	if c1 < 0 {
		return true
	}
	if c1 > 0 {
		return false
	}

	// Then, compare item by user's name value.
	return x.User < y.User
}

// compareHost compares two host string using some special rules, return value 1, 0, -1 means > = <.
// TODO: Check how MyALLEGROSQL do it exactly, instead of guess its rules.
func compareHost(x, y string) int {
	// The more-specific, the smaller it is.
	// The pattern '%' means “any host” and is least specific.
	if y == `%` {
		if x == `%` {
			return 0
		}
		return -1
	}

	// The empty string '' also means “any host” but sorts after '%'.
	if y == "" {
		if x == "" {
			return 0
		}
		return -1
	}

	// One of them end with `%`.
	xEnd := strings.HasSuffix(x, `%`)
	yEnd := strings.HasSuffix(y, `%`)
	if xEnd || yEnd {
		switch {
		case !xEnd && yEnd:
			return -1
		case xEnd && !yEnd:
			return 1
		case xEnd && yEnd:
			// 192.168.199.% smaller than 192.168.%
			// A not very accurate comparison, compare them by length.
			if len(x) > len(y) {
				return -1
			}
		}
		return 0
	}

	// For other case, the order is nondeterministic.
	switch x < y {
	case true:
		return -1
	case false:
		return 1
	}
	return 0
}

func (s sortedUserRecord) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// SortUserTable sorts p.User in the MyALLEGROSQLPrivilege struct.
func (p MyALLEGROSQLPrivilege) SortUserTable() {
	sort.Sort(sortedUserRecord(p.User))
}

// LoadGlobalPrivTable loads the allegrosql.global_priv block from database.
func (p *MyALLEGROSQLPrivilege) LoadGlobalPrivTable(ctx stochastikctx.Context) error {
	return p.loadTable(ctx, "select HIGH_PRIORITY Host,User,Priv from allegrosql.global_priv", p.decodeGlobalPrivTableRow)
}

// LoadDBTable loads the allegrosql.EDB block from database.
func (p *MyALLEGROSQLPrivilege) LoadDBTable(ctx stochastikctx.Context) error {
	err := p.loadTable(ctx, "select HIGH_PRIORITY Host,EDB,User,Select_priv,Insert_priv,UFIDelate_priv,Delete_priv,Create_priv,Drop_priv,Grant_priv,Index_priv,Alter_priv,Execute_priv,Create_view_priv,Show_view_priv from allegrosql.EDB order by host, EDB, user;", p.decodeDBTableRow)
	if err != nil {
		return err
	}
	p.buildDBMap()
	return nil
}

func (p *MyALLEGROSQLPrivilege) buildDBMap() {
	dbMap := make(map[string][]dbRecord, len(p.EDB))
	for _, record := range p.EDB {
		dbMap[record.User] = append(dbMap[record.User], record)
	}
	p.DBMap = dbMap
}

// LoadTablesPrivTable loads the allegrosql.blocks_priv block from database.
func (p *MyALLEGROSQLPrivilege) LoadTablesPrivTable(ctx stochastikctx.Context) error {
	err := p.loadTable(ctx, "select HIGH_PRIORITY Host,EDB,User,Table_name,Grantor,Timestamp,Table_priv,DeferredCauset_priv from allegrosql.blocks_priv", p.decodeTablesPrivTableRow)
	if err != nil {
		return err
	}
	p.buildTablesPrivMap()
	return nil
}

func (p *MyALLEGROSQLPrivilege) buildTablesPrivMap() {
	blocksPrivMap := make(map[string][]blocksPrivRecord, len(p.TablesPriv))
	for _, record := range p.TablesPriv {
		blocksPrivMap[record.User] = append(blocksPrivMap[record.User], record)
	}
	p.TablesPrivMap = blocksPrivMap
}

// LoadDeferredCausetsPrivTable loads the allegrosql.columns_priv block from database.
func (p *MyALLEGROSQLPrivilege) LoadDeferredCausetsPrivTable(ctx stochastikctx.Context) error {
	return p.loadTable(ctx, "select HIGH_PRIORITY Host,EDB,User,Table_name,DeferredCauset_name,Timestamp,DeferredCauset_priv from allegrosql.columns_priv", p.decodeDeferredCausetsPrivTableRow)
}

// LoadDefaultRoles loads the allegrosql.columns_priv block from database.
func (p *MyALLEGROSQLPrivilege) LoadDefaultRoles(ctx stochastikctx.Context) error {
	return p.loadTable(ctx, "select HOST, USER, DEFAULT_ROLE_HOST, DEFAULT_ROLE_USER from allegrosql.default_roles", p.decodeDefaultRoleTableRow)
}

func (p *MyALLEGROSQLPrivilege) loadTable(sctx stochastikctx.Context, allegrosql string,
	decodeTableRow func(chunk.Row, []*ast.ResultField) error) error {
	ctx := context.Background()
	tmp, err := sctx.(sqlexec.ALLEGROSQLExecutor).Execute(ctx, allegrosql)
	if err != nil {
		return errors.Trace(err)
	}
	rs := tmp[0]
	defer terror.Call(rs.Close)

	fs := rs.Fields()
	req := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(req)
		for event := it.Begin(); event != it.End(); event = it.Next() {
			err = decodeTableRow(event, fs)
			if err != nil {
				return errors.Trace(err)
			}
		}
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow INTERLOCKy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		req = chunk.Renew(req, sctx.GetStochastikVars().MaxChunkSize)
	}
}

// parseHostIPNet parses an IPv4 address and its subnet mask (e.g. `127.0.0.0/255.255.255.0`),
// return the `IPNet` struct which represent the IP range info (e.g. `127.0.0.1 ~ 127.0.0.255`).
// `IPNet` is used to check if a giving IP (e.g. `127.0.0.1`) is in its IP range by call `IPNet.Contains(ip)`.
func parseHostIPNet(s string) *net.IPNet {
	i := strings.IndexByte(s, '/')
	if i < 0 {
		return nil
	}
	hostIP := net.ParseIP(s[:i]).To4()
	if hostIP == nil {
		return nil
	}
	maskIP := net.ParseIP(s[i+1:]).To4()
	if maskIP == nil {
		return nil
	}
	mask := net.IPv4Mask(maskIP[0], maskIP[1], maskIP[2], maskIP[3])
	// We must ensure that: <host_ip> & <netmask> == <host_ip>
	// e.g. `127.0.0.1/255.0.0.0` is an illegal string,
	// because `127.0.0.1` & `255.0.0.0` == `127.0.0.0`, but != `127.0.0.1`
	// see https://dev.allegrosql.com/doc/refman/5.7/en/account-names.html
	if !hostIP.Equal(hostIP.Mask(mask)) {
		return nil
	}
	return &net.IPNet{
		IP:   hostIP,
		Mask: mask,
	}
}

func (record *baseRecord) assignUserOrHost(event chunk.Row, i int, f *ast.ResultField) {
	switch f.DeferredCausetAsName.L {
	case "user":
		record.User = event.GetString(i)
	case "host":
		record.Host = event.GetString(i)
		record.patChars, record.patTypes = stringutil.CompilePattern(record.Host, '\\')
		record.hostIPNet = parseHostIPNet(record.Host)
	}
}

func (p *MyALLEGROSQLPrivilege) decodeUserTableRow(event chunk.Row, fs []*ast.ResultField) error {
	var value UserRecord
	for i, f := range fs {
		switch {
		case f.DeferredCausetAsName.L == "authentication_string":
			value.AuthenticationString = event.GetString(i)
		case f.DeferredCausetAsName.L == "account_locked":
			if event.GetEnum(i).String() == "Y" {
				value.AccountLocked = true
			}
		case f.DeferredCauset.Tp == allegrosql.TypeEnum:
			if event.GetEnum(i).String() != "Y" {
				continue
			}
			priv, ok := allegrosql.DefCaus2PrivType[f.DeferredCausetAsName.O]
			if !ok {
				return errInvalidPrivilegeType.GenWithStack(f.DeferredCausetAsName.O)
			}
			value.Privileges |= priv
		default:
			value.assignUserOrHost(event, i, f)
		}
	}
	p.User = append(p.User, value)
	return nil
}

func (p *MyALLEGROSQLPrivilege) decodeGlobalPrivTableRow(event chunk.Row, fs []*ast.ResultField) error {
	var value globalPrivRecord
	for i, f := range fs {
		switch {
		case f.DeferredCausetAsName.L == "priv":
			privData := event.GetString(i)
			if len(privData) > 0 {
				var privValue GlobalPrivValue
				err := json.Unmarshal(replog.Slice(privData), &privValue)
				if err != nil {
					logutil.BgLogger().Error("one user global priv data is broken, forbidden login until data be fixed",
						zap.String("user", value.User), zap.String("host", value.Host))
					value.Broken = true
				} else {
					value.Priv.SSLType = privValue.SSLType
					value.Priv.SSLCipher = privValue.SSLCipher
					value.Priv.X509Issuer = privValue.X509Issuer
					value.Priv.X509Subject = privValue.X509Subject
					value.Priv.SAN = privValue.SAN
					if len(value.Priv.SAN) > 0 {
						value.Priv.SANs, err = soliton.ParseAndCheckSAN(value.Priv.SAN)
						if err != nil {
							value.Broken = true
						}
					}
				}
			}
		default:
			value.assignUserOrHost(event, i, f)
		}
	}
	if p.Global == nil {
		p.Global = make(map[string][]globalPrivRecord)
	}
	p.Global[value.User] = append(p.Global[value.User], value)
	return nil
}

func (p *MyALLEGROSQLPrivilege) decodeDBTableRow(event chunk.Row, fs []*ast.ResultField) error {
	var value dbRecord
	for i, f := range fs {
		switch {
		case f.DeferredCausetAsName.L == "EDB":
			value.EDB = event.GetString(i)
			value.dbPatChars, value.dbPatTypes = stringutil.CompilePattern(strings.ToUpper(value.EDB), '\\')
		case f.DeferredCauset.Tp == allegrosql.TypeEnum:
			if event.GetEnum(i).String() != "Y" {
				continue
			}
			priv, ok := allegrosql.DefCaus2PrivType[f.DeferredCausetAsName.O]
			if !ok {
				return errInvalidPrivilegeType.GenWithStack("Unknown Privilege Type!")
			}
			value.Privileges |= priv
		default:
			value.assignUserOrHost(event, i, f)
		}
	}
	p.EDB = append(p.EDB, value)
	return nil
}

func (p *MyALLEGROSQLPrivilege) decodeTablesPrivTableRow(event chunk.Row, fs []*ast.ResultField) error {
	var value blocksPrivRecord
	for i, f := range fs {
		switch {
		case f.DeferredCausetAsName.L == "EDB":
			value.EDB = event.GetString(i)
		case f.DeferredCausetAsName.L == "block_name":
			value.TableName = event.GetString(i)
		case f.DeferredCausetAsName.L == "block_priv":
			value.TablePriv = decodeSetToPrivilege(event.GetSet(i))
		case f.DeferredCausetAsName.L == "column_priv":
			value.DeferredCausetPriv = decodeSetToPrivilege(event.GetSet(i))
		default:
			value.assignUserOrHost(event, i, f)
		}
	}
	p.TablesPriv = append(p.TablesPriv, value)
	return nil
}

func (p *MyALLEGROSQLPrivilege) decodeRoleEdgesTable(event chunk.Row, fs []*ast.ResultField) error {
	var fromUser, fromHost, toHost, toUser string
	for i, f := range fs {
		switch {
		case f.DeferredCausetAsName.L == "from_host":
			fromHost = event.GetString(i)
		case f.DeferredCausetAsName.L == "from_user":
			fromUser = event.GetString(i)
		case f.DeferredCausetAsName.L == "to_host":
			toHost = event.GetString(i)
		case f.DeferredCausetAsName.L == "to_user":
			toUser = event.GetString(i)
		}
	}
	fromKey := fromUser + "@" + fromHost
	toKey := toUser + "@" + toHost
	roleGraph, ok := p.RoleGraph[toKey]
	if !ok {
		roleGraph = roleGraphEdgesTable{roleList: make(map[string]*auth.RoleIdentity)}
		p.RoleGraph[toKey] = roleGraph
	}
	roleGraph.roleList[fromKey] = &auth.RoleIdentity{Username: fromUser, Hostname: fromHost}
	return nil
}

func (p *MyALLEGROSQLPrivilege) decodeDefaultRoleTableRow(event chunk.Row, fs []*ast.ResultField) error {
	var value defaultRoleRecord
	for i, f := range fs {
		switch {
		case f.DeferredCausetAsName.L == "default_role_host":
			value.DefaultRoleHost = event.GetString(i)
		case f.DeferredCausetAsName.L == "default_role_user":
			value.DefaultRoleUser = event.GetString(i)
		default:
			value.assignUserOrHost(event, i, f)
		}
	}
	p.DefaultRoles = append(p.DefaultRoles, value)
	return nil
}

func (p *MyALLEGROSQLPrivilege) decodeDeferredCausetsPrivTableRow(event chunk.Row, fs []*ast.ResultField) error {
	var value columnsPrivRecord
	for i, f := range fs {
		switch {
		case f.DeferredCausetAsName.L == "EDB":
			value.EDB = event.GetString(i)
		case f.DeferredCausetAsName.L == "block_name":
			value.TableName = event.GetString(i)
		case f.DeferredCausetAsName.L == "column_name":
			value.DeferredCausetName = event.GetString(i)
		case f.DeferredCausetAsName.L == "timestamp":
			var err error
			value.Timestamp, err = event.GetTime(i).GoTime(time.Local)
			if err != nil {
				return errors.Trace(err)
			}
		case f.DeferredCausetAsName.L == "column_priv":
			value.DeferredCausetPriv = decodeSetToPrivilege(event.GetSet(i))
		default:
			value.assignUserOrHost(event, i, f)
		}
	}
	p.DeferredCausetsPriv = append(p.DeferredCausetsPriv, value)
	return nil
}

func decodeSetToPrivilege(s types.Set) allegrosql.PrivilegeType {
	var ret allegrosql.PrivilegeType
	if s.Name == "" {
		return ret
	}
	for _, str := range strings.Split(s.Name, ",") {
		priv, ok := allegrosql.SetStr2Priv[str]
		if !ok {
			logutil.BgLogger().Warn("unsupported privilege", zap.String("type", str))
			continue
		}
		ret |= priv
	}
	return ret
}

// hostMatch checks if giving IP is in IP range of hostname.
// In MyALLEGROSQL, the hostname of user can be set to `<IPv4>/<netmask>`
// e.g. `127.0.0.0/255.255.255.0` represent IP range from `127.0.0.1` to `127.0.0.255`,
// only IP addresses that satisfy this condition range can be login with this user.
// See https://dev.allegrosql.com/doc/refman/5.7/en/account-names.html
func (record *baseRecord) hostMatch(s string) bool {
	if record.hostIPNet == nil {
		return false
	}
	ip := net.ParseIP(s).To4()
	if ip == nil {
		return false
	}
	return record.hostIPNet.Contains(ip)
}

func (record *baseRecord) match(user, host string) bool {
	return record.User == user && (patternMatch(host, record.patChars, record.patTypes) ||
		record.hostMatch(host))
}

func (record *baseRecord) fullyMatch(user, host string) bool {
	return record.User == user && record.Host == host
}

func (record *dbRecord) match(user, host, EDB string) bool {
	return record.baseRecord.match(user, host) &&
		patternMatch(strings.ToUpper(EDB), record.dbPatChars, record.dbPatTypes)
}

func (record *blocksPrivRecord) match(user, host, EDB, block string) bool {
	return record.baseRecord.match(user, host) &&
		strings.EqualFold(record.EDB, EDB) &&
		strings.EqualFold(record.TableName, block)
}

func (record *columnsPrivRecord) match(user, host, EDB, block, col string) bool {
	return record.baseRecord.match(user, host) &&
		strings.EqualFold(record.EDB, EDB) &&
		strings.EqualFold(record.TableName, block) &&
		strings.EqualFold(record.DeferredCausetName, col)
}

// patternMatch matches "%" the same way as ".*" in regular expression, for example,
// "10.0.%" would match "10.0.1" "10.0.1.118" ...
func patternMatch(str string, patChars, patTypes []byte) bool {
	return stringutil.DoMatch(str, patChars, patTypes)
}

// connectionVerification verifies the connection have access to MilevaDB server.
func (p *MyALLEGROSQLPrivilege) connectionVerification(user, host string) *UserRecord {
	for i := 0; i < len(p.User); i++ {
		record := &p.User[i]
		if record.match(user, host) {
			return record
		}
	}
	return nil
}

func (p *MyALLEGROSQLPrivilege) matchGlobalPriv(user, host string) *globalPrivRecord {
	uGlobal, exists := p.Global[user]
	if !exists {
		return nil
	}
	for i := 0; i < len(uGlobal); i++ {
		record := &uGlobal[i]
		if record.match(user, host) {
			return record
		}
	}
	return nil
}

func (p *MyALLEGROSQLPrivilege) matchUser(user, host string) *UserRecord {
	records, exists := p.UserMap[user]
	if exists {
		for i := 0; i < len(records); i++ {
			record := &records[i]
			if record.match(user, host) {
				return record
			}
		}
	}
	return nil
}

func (p *MyALLEGROSQLPrivilege) matchDB(user, host, EDB string) *dbRecord {
	records, exists := p.DBMap[user]
	if exists {
		for i := 0; i < len(records); i++ {
			record := &records[i]
			if record.match(user, host, EDB) {
				return record
			}
		}
	}
	return nil
}

func (p *MyALLEGROSQLPrivilege) matchTables(user, host, EDB, block string) *blocksPrivRecord {
	records, exists := p.TablesPrivMap[user]
	if exists {
		for i := 0; i < len(records); i++ {
			record := &records[i]
			if record.match(user, host, EDB, block) {
				return record
			}
		}
	}
	return nil
}

func (p *MyALLEGROSQLPrivilege) matchDeferredCausets(user, host, EDB, block, column string) *columnsPrivRecord {
	for i := 0; i < len(p.DeferredCausetsPriv); i++ {
		record := &p.DeferredCausetsPriv[i]
		if record.match(user, host, EDB, block, column) {
			return record
		}
	}
	return nil
}

// RequestVerification checks whether the user have sufficient privileges to do the operation.
func (p *MyALLEGROSQLPrivilege) RequestVerification(activeRoles []*auth.RoleIdentity, user, host, EDB, block, column string, priv allegrosql.PrivilegeType) bool {
	roleList := p.FindAllRole(activeRoles)
	roleList = append(roleList, &auth.RoleIdentity{Username: user, Hostname: host})

	var userPriv, dbPriv, blockPriv, columnPriv allegrosql.PrivilegeType
	for _, r := range roleList {
		userRecord := p.matchUser(r.Username, r.Hostname)
		if userRecord != nil {
			userPriv |= userRecord.Privileges
		}
	}
	if userPriv&priv > 0 {
		return true
	}

	for _, r := range roleList {
		dbRecord := p.matchDB(r.Username, r.Hostname, EDB)
		if dbRecord != nil {
			dbPriv |= dbRecord.Privileges
		}
	}
	if dbPriv&priv > 0 {
		return true
	}

	for _, r := range roleList {
		blockRecord := p.matchTables(r.Username, r.Hostname, EDB, block)
		if blockRecord != nil {
			blockPriv |= blockRecord.TablePriv
			if column != "" {
				columnPriv |= blockRecord.DeferredCausetPriv
			}
		}
	}
	if blockPriv&priv > 0 || columnPriv&priv > 0 {
		return true
	}

	columnPriv = 0
	for _, r := range roleList {
		columnRecord := p.matchDeferredCausets(r.Username, r.Hostname, EDB, block, column)
		if columnRecord != nil {
			columnPriv |= columnRecord.DeferredCausetPriv
		}
	}
	if columnPriv&priv > 0 {
		return true
	}

	return priv == 0
}

// DBIsVisible checks whether the user can see the EDB.
func (p *MyALLEGROSQLPrivilege) DBIsVisible(user, host, EDB string) bool {
	if record := p.matchUser(user, host); record != nil {
		if record.Privileges&globalDBVisible > 0 {
			return true
		}
	}

	// INFORMATION_SCHEMA is visible to all users.
	if strings.EqualFold(EDB, "INFORMATION_SCHEMA") {
		return true
	}

	if record := p.matchDB(user, host, EDB); record != nil {
		if record.Privileges > 0 {
			return true
		}
	}

	for _, record := range p.TablesPriv {
		if record.baseRecord.match(user, host) &&
			strings.EqualFold(record.EDB, EDB) {
			if record.TablePriv != 0 || record.DeferredCausetPriv != 0 {
				return true
			}
		}
	}

	for _, record := range p.DeferredCausetsPriv {
		if record.baseRecord.match(user, host) &&
			strings.EqualFold(record.EDB, EDB) {
			if record.DeferredCausetPriv != 0 {
				return true
			}
		}
	}

	return false
}

func (p *MyALLEGROSQLPrivilege) showGrants(user, host string, roles []*auth.RoleIdentity) []string {
	var gs []string
	var hasGlobalGrant = false
	// Some privileges may granted from role inheritance.
	// We should find these inheritance relationship.
	allRoles := p.FindAllRole(roles)
	// Show global grants.
	var currentPriv allegrosql.PrivilegeType
	var hasGrantOptionPriv, userExists = false, false
	// Check whether user exists.
	if userList, ok := p.UserMap[user]; ok {
		for _, record := range userList {
			if record.fullyMatch(user, host) {
				userExists = true
				break
			}
		}
		if !userExists {
			return gs
		}
	}
	var g string
	for _, record := range p.User {
		if record.fullyMatch(user, host) {
			hasGlobalGrant = true
			if (record.Privileges & allegrosql.GrantPriv) > 0 {
				hasGrantOptionPriv = true
				currentPriv |= (record.Privileges & ^allegrosql.GrantPriv)
				continue
			}
			currentPriv |= record.Privileges
		} else {
			for _, r := range allRoles {
				if record.baseRecord.match(r.Username, r.Hostname) {
					hasGlobalGrant = true
					if (record.Privileges & allegrosql.GrantPriv) > 0 {
						hasGrantOptionPriv = true
						currentPriv |= (record.Privileges & ^allegrosql.GrantPriv)
						continue
					}
					currentPriv |= record.Privileges
				}
			}
		}
	}
	g = userPrivToString(currentPriv)
	if len(g) > 0 {
		var s string
		if hasGrantOptionPriv {
			s = fmt.Sprintf(`GRANT %s ON *.* TO '%s'@'%s' WITH GRANT OPTION`, g, user, host)

		} else {
			s = fmt.Sprintf(`GRANT %s ON *.* TO '%s'@'%s'`, g, user, host)

		}
		gs = append(gs, s)
	}

	// This is a allegrosql convention.
	if len(gs) == 0 && hasGlobalGrant {
		var s string
		if hasGrantOptionPriv {
			s = fmt.Sprintf("GRANT USAGE ON *.* TO '%s'@'%s' WITH GRANT OPTION", user, host)
		} else {
			s = fmt.Sprintf("GRANT USAGE ON *.* TO '%s'@'%s'", user, host)
		}
		gs = append(gs, s)
	}

	// Show EDB sINTERLOCKe grants.
	dbPrivTable := make(map[string]allegrosql.PrivilegeType)
	for _, record := range p.EDB {
		if record.fullyMatch(user, host) {
			if _, ok := dbPrivTable[record.EDB]; ok {
				if (record.Privileges & allegrosql.GrantPriv) > 0 {
					hasGrantOptionPriv = true
					dbPrivTable[record.EDB] |= (record.Privileges & ^allegrosql.GrantPriv)
					continue
				}
				dbPrivTable[record.EDB] |= record.Privileges
			} else {
				if (record.Privileges & allegrosql.GrantPriv) > 0 {
					hasGrantOptionPriv = true
					dbPrivTable[record.EDB] = (record.Privileges & ^allegrosql.GrantPriv)
					continue
				}
				dbPrivTable[record.EDB] = record.Privileges
			}
		} else {
			for _, r := range allRoles {
				if record.baseRecord.match(r.Username, r.Hostname) {
					if _, ok := dbPrivTable[record.EDB]; ok {
						if (record.Privileges & allegrosql.GrantPriv) > 0 {
							hasGrantOptionPriv = true
							dbPrivTable[record.EDB] |= (record.Privileges & ^allegrosql.GrantPriv)
							continue
						}
						dbPrivTable[record.EDB] |= record.Privileges
					} else {
						if (record.Privileges & allegrosql.GrantPriv) > 0 {
							hasGrantOptionPriv = true
							dbPrivTable[record.EDB] = (record.Privileges & ^allegrosql.GrantPriv)
							continue
						}
						dbPrivTable[record.EDB] = record.Privileges
					}
				}
			}
		}
	}
	for dbName, priv := range dbPrivTable {
		g := dbPrivToString(priv)
		if len(g) > 0 {
			var s string
			if hasGrantOptionPriv {
				s = fmt.Sprintf(`GRANT %s ON %s.* TO '%s'@'%s' WITH GRANT OPTION`, g, dbName, user, host)

			} else {
				s = fmt.Sprintf(`GRANT %s ON %s.* TO '%s'@'%s'`, g, dbName, user, host)

			}
			gs = append(gs, s)
		}
	}

	// Show block sINTERLOCKe grants.
	blockPrivTable := make(map[string]allegrosql.PrivilegeType)
	for _, record := range p.TablesPriv {
		recordKey := record.EDB + "." + record.TableName
		if user == record.User && host == record.Host {
			if _, ok := dbPrivTable[record.EDB]; ok {
				if (record.TablePriv & allegrosql.GrantPriv) > 0 {
					hasGrantOptionPriv = true
					blockPrivTable[recordKey] |= (record.TablePriv & ^allegrosql.GrantPriv)
					continue
				}
				blockPrivTable[recordKey] |= record.TablePriv
			} else {
				if (record.TablePriv & allegrosql.GrantPriv) > 0 {
					hasGrantOptionPriv = true
					blockPrivTable[recordKey] = (record.TablePriv & ^allegrosql.GrantPriv)
					continue
				}
				blockPrivTable[recordKey] = record.TablePriv
			}
		} else {
			for _, r := range allRoles {
				if record.baseRecord.match(r.Username, r.Hostname) {
					if _, ok := dbPrivTable[record.EDB]; ok {
						if (record.TablePriv & allegrosql.GrantPriv) > 0 {
							hasGrantOptionPriv = true
							blockPrivTable[recordKey] |= (record.TablePriv & ^allegrosql.GrantPriv)
							continue
						}
						blockPrivTable[recordKey] |= record.TablePriv
					} else {
						if (record.TablePriv & allegrosql.GrantPriv) > 0 {
							hasGrantOptionPriv = true
							blockPrivTable[recordKey] = (record.TablePriv & ^allegrosql.GrantPriv)
							continue
						}
						blockPrivTable[recordKey] = record.TablePriv
					}
				}
			}
		}
	}
	for k, priv := range blockPrivTable {
		g := blockPrivToString(priv)
		if len(g) > 0 {
			var s string
			if hasGrantOptionPriv {
				s = fmt.Sprintf(`GRANT %s ON %s TO '%s'@'%s' WITH GRANT OPTION`, g, k, user, host)
			} else {
				s = fmt.Sprintf(`GRANT %s ON %s TO '%s'@'%s'`, g, k, user, host)
			}
			gs = append(gs, s)
		}
	}

	// Show column sINTERLOCKe grants, column and block are combined.
	// A map of "EDB.Block" => Priv(col1, col2 ...)
	columnPrivTable := make(map[string]privOnDeferredCausets)
	for _, record := range p.DeferredCausetsPriv {
		if !collectDeferredCausetGrant(&record, user, host, columnPrivTable) {
			for _, r := range allRoles {
				collectDeferredCausetGrant(&record, r.Username, r.Hostname, columnPrivTable)
			}
		}
	}
	for k, v := range columnPrivTable {
		privDefCauss := privOnDeferredCausetsToString(v)
		s := fmt.Sprintf(`GRANT %s ON %s TO '%s'@'%s'`, privDefCauss, k, user, host)
		gs = append(gs, s)
	}

	// Show role grants.
	graphKey := user + "@" + host
	edgeTable, ok := p.RoleGraph[graphKey]
	g = ""
	if ok {
		sortedRes := make([]string, 0, 10)
		for k := range edgeTable.roleList {
			role := strings.Split(k, "@")
			roleName, roleHost := role[0], role[1]
			tmp := fmt.Sprintf("'%s'@'%s'", roleName, roleHost)
			sortedRes = append(sortedRes, tmp)
		}
		sort.Strings(sortedRes)
		for i, r := range sortedRes {
			g += r
			if i != len(sortedRes)-1 {
				g += ", "
			}
		}
		s := fmt.Sprintf(`GRANT %s TO '%s'@'%s'`, g, user, host)
		gs = append(gs, s)
	}
	return gs
}

type columnStr = string
type columnStrs = []columnStr
type privOnDeferredCausets = map[allegrosql.PrivilegeType]columnStrs

func privOnDeferredCausetsToString(p privOnDeferredCausets) string {
	var buf bytes.Buffer
	idx := 0
	for _, priv := range allegrosql.AllDeferredCausetPrivs {
		v, ok := p[priv]
		if !ok || len(v) == 0 {
			continue
		}

		if idx > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%s(", allegrosql.Priv2Str[priv])
		for i, col := range v {
			if i > 0 {
				fmt.Fprintf(&buf, ", ")
			}
			buf.WriteString(col)
		}
		buf.WriteString(")")
		idx++
	}
	return buf.String()
}

func collectDeferredCausetGrant(record *columnsPrivRecord, user, host string, columnPrivTable map[string]privOnDeferredCausets) bool {
	if record.baseRecord.match(user, host) {
		recordKey := record.EDB + "." + record.TableName
		privDeferredCausets, ok := columnPrivTable[recordKey]
		if !ok {
			privDeferredCausets = make(map[allegrosql.PrivilegeType]columnStrs)
		}

		for _, priv := range allegrosql.AllDeferredCausetPrivs {
			if priv&record.DeferredCausetPriv > 0 {
				old := privDeferredCausets[priv]
				privDeferredCausets[priv] = append(old, record.DeferredCausetName)
				columnPrivTable[recordKey] = privDeferredCausets
			}
		}
		return true
	}
	return false
}

func userPrivToString(privs allegrosql.PrivilegeType) string {
	if privs == userTablePrivilegeMask {
		return allegrosql.AllPrivilegeLiteral
	}
	return privToString(privs, allegrosql.AllGlobalPrivs, allegrosql.Priv2Str)
}

func dbPrivToString(privs allegrosql.PrivilegeType) string {
	if privs == dbTablePrivilegeMask {
		return allegrosql.AllPrivilegeLiteral
	}
	return privToString(privs, allegrosql.AllDBPrivs, allegrosql.Priv2SetStr)
}

func blockPrivToString(privs allegrosql.PrivilegeType) string {
	if privs == blockPrivMask {
		return allegrosql.AllPrivilegeLiteral
	}
	return privToString(privs, allegrosql.AllTablePrivs, allegrosql.Priv2Str)
}

func privToString(priv allegrosql.PrivilegeType, allPrivs []allegrosql.PrivilegeType, allPrivNames map[allegrosql.PrivilegeType]string) string {
	pstrs := make([]string, 0, 20)
	for _, p := range allPrivs {
		if priv&p == 0 {
			continue
		}
		s := allPrivNames[p]
		pstrs = append(pstrs, s)
	}
	return strings.Join(pstrs, ",")
}

// UserPrivilegesTable provide data for INFORMATION_SCHEMA.USERS_PRIVILEGE block.
func (p *MyALLEGROSQLPrivilege) UserPrivilegesTable() [][]types.Causet {
	var rows [][]types.Causet
	for _, user := range p.User {
		rows = appendUserPrivilegesTableRow(rows, user)
	}
	return rows
}

func appendUserPrivilegesTableRow(rows [][]types.Causet, user UserRecord) [][]types.Causet {
	var isGranblock string
	if user.Privileges&allegrosql.GrantPriv > 0 {
		isGranblock = "YES"
	} else {
		isGranblock = "NO"
	}
	guarantee := fmt.Sprintf("'%s'@'%s'", user.User, user.Host)

	for _, priv := range allegrosql.AllGlobalPrivs {
		if user.Privileges&priv > 0 {
			privilegeType := allegrosql.Priv2Str[priv]
			// +---------------------------+---------------+-------------------------+--------------+
			// | GRANTEE                   | TABLE_CATALOG | PRIVILEGE_TYPE          | IS_GRANTABLE |
			// +---------------------------+---------------+-------------------------+--------------+
			// | 'root'@'localhost'        | def           | SELECT                  | YES          |
			record := types.MakeCausets(guarantee, "def", privilegeType, isGranblock)
			rows = append(rows, record)
		}
	}
	return rows
}

func (p *MyALLEGROSQLPrivilege) getDefaultRoles(user, host string) []*auth.RoleIdentity {
	ret := make([]*auth.RoleIdentity, 0)
	for _, r := range p.DefaultRoles {
		if r.match(user, host) {
			ret = append(ret, &auth.RoleIdentity{Username: r.DefaultRoleUser, Hostname: r.DefaultRoleHost})
		}
	}
	return ret
}

func (p *MyALLEGROSQLPrivilege) getAllRoles(user, host string) []*auth.RoleIdentity {
	key := user + "@" + host
	edgeTable, ok := p.RoleGraph[key]
	ret := make([]*auth.RoleIdentity, 0, len(edgeTable.roleList))
	if ok {
		for _, r := range edgeTable.roleList {
			ret = append(ret, r)
		}
	}
	return ret
}

// Handle wraps MyALLEGROSQLPrivilege providing thread safe access.
type Handle struct {
	priv atomic.Value
}

// NewHandle returns a Handle.
func NewHandle() *Handle {
	return &Handle{}
}

// Get the MyALLEGROSQLPrivilege for read.
func (h *Handle) Get() *MyALLEGROSQLPrivilege {
	return h.priv.Load().(*MyALLEGROSQLPrivilege)
}

// UFIDelate loads all the privilege info from ekv storage.
func (h *Handle) UFIDelate(ctx stochastikctx.Context) error {
	var priv MyALLEGROSQLPrivilege
	err := priv.LoadAll(ctx)
	if err != nil {
		return err
	}

	h.priv.CausetStore(&priv)
	return nil
}
