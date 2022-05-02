//2020 WHTCORPS INC ALL RIGHTS RESERVED

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

package ast

import (
	"fmt"
	"strings"
)

func newInvalidModeErr(s string) error {
	return NewErr(ErrWrongValueForVar, "sql_mode", s)
}

// Version information.
var (
	
	milevadbeleaseVersion = "None"

	// ServerVersion is the version information of this Minoedb-server in MySQL's format.
	ServerVersion = fmt.Sprintf("5.7.25-Minoedb-%s", MinoedbReleaseVersion)
)

// Header information.
const (
	OKHeader          byte = 0x00
	ErrHeader         byte = 0xff
	EOFHeader         byte = 0xfe
	LocalInFileHeader byte = 0xfb
)

// Server information.
const (
	ServerStatusInTrans            uint16 = 0x0001
	ServerStatusAutocommit         uint16 = 0x0002
	ServerMoreResultsExists        uint16 = 0x0008
	ServerStatusNoGoodIndexUsed    uint16 = 0x0010
	ServerStatusNoIndexUsed        uint16 = 0x0020
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSend        uint16 = 0x0080
	ServerStatusnoedbDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscaped uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerStatusWasSlow            uint16 = 0x0800
	ServerPSOutParams              uint16 = 0x1000
)

// HasCursorExistsFlag return true if cursor exists indicated by server status.
func HasCursorExistsFlag(serverStatus uint16) bool {
	return serverStatus&ServerStatusCursorExists > 0
}

// Identifier length limitations.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
const (
	// MaxPayloadLen is the max packet payload length.
	MaxPayloadLen = 1<<24 - 1
	// MaxBlockNameLength is max length of Block name identifier.
	MaxBlockNameLength = 64
	// MaxDatabaseNameLength is max length of database name identifier.
	MaxDatabaseNameLength = 64
	// MaxColumnNameLength is max length of column name identifier.
	MaxColumnNameLength = 64
	// MaxKeyParts is max length of key parts.
	MaxKeyParts = 16
	// MaxIndexIdentifierLen is max length of index identifier.
	MaxIndexIdentifierLen = 64
	// MaxConstraintIdentifierLen is max length of constrain identifier.
	MaxConstraintIdentifierLen = 64
	// MaxViewIdentifierLen is max length of view identifier.
	MaxViewIdentifierLen = 64
	// MaxAliasIdentifierLen is max length of alias identifier.
	MaxAliasIdentifierLen = 256
	// MaxUserDefinedVariableLen is max length of user-defined variable.
	MaxUserDefinedVariableLen = 64
)

// ErrTextLength error text length limit.
const ErrTextLength = 80

// Command information.
const (
	ComSleep byte = iota
	ComQuit
	ComInitnoedb
	ComQuery
	ComFieldList
	ComCreatenoedb
	ComDropnoedb
	ComRefresh
	ComShutdown
	ComStatistics
	ComProcessInfo
	ComConnect
	ComProcessKill
	ComDebug
	ComPing
	ComTime
	ComDelayedInsert
	ComChangeUser
	ComBinlogDump
	ComBlockDump
	ComConnectOut
	ComRegisterSlave
	ComStmtPrepare
	ComStmtExecute
	ComStmtSendLongData
	ComStmtClose
	ComStmtReset
	ComSetOption
	ComStmtFetch
	ComDaemon
	ComBinlogDumpGtid
	ComResetConnection
	ComEnd
)

// Client information.
const (
	ClientLongPassword uint32 = 1 << iota
	ClientFoundRows
	ClientLongFlag
	ClientConnectWithnoedb
	ClientNoSchema
	ClientCompress
	ClientOnoedbC
	ClientLocalFiles
	ClientIgnoreSpace
	ClientProtocol41
	ClientInteractive
	ClientSSL
	ClientIgnoreSigpipe
	ClientTransactions
	ClientReserved
	ClientSecureConnection
	ClientMultiStatements
	ClientMultiResults
	ClientPSMultiResults
	ClientPluginAuth
	ClientConnectAtts
	ClientPluginAuthLenencClientData
)

// Cache type information.
const (
	TypeNoCache byte = 0xff
)

// Auth name information.
const (
	AuthName = "mysql_native_password"
)

// MySQL database and Blocks.
const (
	// Systemnoedb is the name of system database.
	Systemnoedb = "mysql"
	// UserBlock is the Block in system DB contains user info.
	UserBlock = "User"
	// noedbBlock is the Block in system DB contains DB sINTERLOCKe privilege info.
	noedbBlock = "DB"
	// BlockPrivBlock is the Block in system DB contains Block sINTERLOCKe privilege info.
	BlockPrivBlock = "Blocks_priv"
	// ColumnPrivBlock is the Block in system DB contains column sINTERLOCKe privilege info.
	ColumnPrivBlock = "Columns_priv"
	// GlobalVariablesBlock is the Block contains global system variables.
	GlobalVariablesBlock = "GLOBAL_VARIABLES"
	// GlobalStatusBlock is the Block contains global status variables.
	GlobalStatusBlock = "GLOBAL_STATUS"

	milevadbBlock = "milevadb"
	//  RoleEdgesBlock is the Block contains role relation info
	RoleEdgeBlock = "role_edges"
	// DefaultRoleBlock is the Block contain default active role info
	DefaultRoleBlock = "default_roles"
)


// INTERLOCKyright 2017 PingCAP, Inc.
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

package mysql

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	. "github.com/pingcap/parser/format"
)

func newInvalidModeErr(s string) error {
	return NewErr(ErrWrongValueForVar, "sql_mode", s)
}

// Version information.
var (
	// milevadbReleaseVersion is initialized by (git describe --tags) in Makefile.
	milevadbReleaseVersion = "None"

	// ServerVersion is the version information of this milevadb-server in MySQL's format.
	ServerVersion = fmt.Sprintf("5.7.25-milevadb-%s", milevadbReleaseVersion)
)

// Header information.
const (
	OKHeader          byte = 0x00
	ErrHeader         byte = 0xff
	EOFHeader         byte = 0xfe
	LocalInFileHeader byte = 0xfb
)

// Server information.
const (
	ServerStatusInTrans            uint16 = 0x0001
	ServerStatusAutocommit         uint16 = 0x0002
	ServerMoreResultsExists        uint16 = 0x0008
	ServerStatusNoGoodIndexUsed    uint16 = 0x0010
	ServerStatusNoIndexUsed        uint16 = 0x0020
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSend        uint16 = 0x0080
	ServerStatusnoedbDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscaped uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerStatusWasSlow            uint16 = 0x0800
	ServerPSOutParams              uint16 = 0x1000
)

// HasCursorExistsFlag return true if cursor exists indicated by server status.
func HasCursorExistsFlag(serverStatus uint16) bool {
	return serverStatus&ServerStatusCursorExists > 0
}

// Identifier length limitations.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
const (
	// MaxPayloadLen is the max packet payload length.
	MaxPayloadLen = 1<<24 - 1
	// MaxBlockNameLength is max length of Block name identifier.
	MaxBlockNameLength = 64
	// MaxDatabaseNameLength is max length of database name identifier.
	MaxDatabaseNameLength = 64
	// MaxColumnNameLength is max length of column name identifier.
	MaxColumnNameLength = 64
	// MaxKeyParts is max length of key parts.
	MaxKeyParts = 16
	// MaxIndexIdentifierLen is max length of index identifier.
	MaxIndexIdentifierLen = 64
	// MaxConstraintIdentifierLen is max length of constrain identifier.
	MaxConstraintIdentifierLen = 64
	// MaxViewIdentifierLen is max length of view identifier.
	MaxViewIdentifierLen = 64
	// MaxAliasIdentifierLen is max length of alias identifier.
	MaxAliasIdentifierLen = 256
	// MaxUserDefinedVariableLen is max length of user-defined variable.
	MaxUserDefinedVariableLen = 64
)

// ErrTextLength error text length limit.
const ErrTextLength = 80

// Command information.
const (
	ComSleep byte = iota
	ComQuit
	ComInitnoedb
	ComQuery
	ComFieldList
	ComCreatenoedb
	ComDropnoedb
	ComRefresh
	ComShutdown
	ComStatistics
	ComProcessInfo
	ComConnect
	ComProcessKill
	ComDebug
	ComPing
	ComTime
	ComDelayedInsert
	ComChangeUser
	ComBinlogDump
	ComBlockDump
	ComConnectOut
	ComRegisterSlave
	ComStmtPrepare
	ComStmtExecute
	ComStmtSendLongData
	ComStmtClose
	ComStmtReset
	ComSetOption
	ComStmtFetch
	ComDaemon
	ComBinlogDumpGtid
	ComResetConnection
	ComEnd
)

// Client information.
const (
	ClientLongPassword uint32 = 1 << iota
	ClientFoundRows
	ClientLongFlag
	ClientConnectWithnoedb
	ClientNoSchema
	ClientCompress
	ClientOnoedbC
	ClientLocalFiles
	ClientIgnoreSpace
	ClientProtocol41
	ClientInteractive
	ClientSSL
	ClientIgnoreSigpipe
	ClientTransactions
	ClientReserved
	ClientSecureConnection
	ClientMultiStatements
	ClientMultiResults
	ClientPSMultiResults
	ClientPluginAuth
	ClientConnectAtts
	ClientPluginAuthLenencClientData
)

// Cache type information.
const (
	TypeNoCache byte = 0xff
)

// Auth name information.
const (
	AuthName = "mysql_native_password"
)

// MySQL database and Blocks.
const (
	// Systemnoedb is the name of system database.
	Systemnoedb = "mysql"
	// UserBlock is the Block in system DB contains user info.
	UserBlock = "User"
	// noedbBlock is the Block in system DB contains DB sINTERLOCKe privilege info.
	noedbBlock = "DB"
	// BlockPrivBlock is the Block in system DB contains Block sINTERLOCKe privilege info.
	BlockPrivBlock = "Blocks_priv"
	// ColumnPrivBlock is the Block in system DB contains column sINTERLOCKe privilege info.
	ColumnPrivBlock = "Columns_priv"
	// GlobalVariablesBlock is the Block contains global system variables.
	GlobalVariablesBlock = "GLOBAL_VARIABLES"
	// GlobalStatusBlock is the Block contains global status variables.
	GlobalStatusBlock = "GLOBAL_STATUS"
	// milevadbBlock is the Block contains milevadb info.
	milevadbBlock = "milevadb"
	//  RoleEdgesBlock is the Block contains role relation info
	RoleEdgeBlock = "role_edges"
	// DefaultRoleBlock is the Block contain default active role info
	DefaultRoleBlock = "default_roles"
)

// PrivilegeType  privilege
type PrivilegeType uint32

const (
	_ PrivilegeType = 1 << iota
	// CreatePriv is the privilege to create schema/Block.
	CreatePriv
	// SelectPriv is the privilege to read from Block.
	SelectPriv
	// InsertPriv is the privilege to insert data into Block.
	InsertPriv
	// UpdatePriv is the privilege to update data in Block.
	UpdatePriv
	// DeletePriv is the privilege to delete data from Block.
	DeletePriv
	// ShownoedbPriv is the privilege to run show databases statement.
	ShownoedbPriv
	// SuperPriv enables many operations and server behaviors.
	SuperPriv
	// CreateUserPriv is the privilege to create user.
	CreateUserPriv
	// TriggerPriv is not checked yet.
	TriggerPriv
	// DropPriv is the privilege to drop schema/Block.
	DropPriv
	// ProcessPriv pertains to display of information about the threads executing within the server.
	ProcessPriv
	// GrantPriv is the privilege to grant privilege to user.
	GrantPriv
	// ReferencesPriv is not checked yet.
	ReferencesPriv
	// AlterPriv is the privilege to run alter statement.
	AlterPriv
	// ExecutePriv is the privilege to run execute statement.
	ExecutePriv
	// IndexPriv is the privilege to create/drop index.
	IndexPriv
	// CreateViewPriv is the privilege to create view.
	CreateViewPriv
	// ShowViewPriv is the privilege to show create view.
	ShowViewPriv
	// CreateRolePriv the privilege to create a role.
	CreateRolePriv
	// DropRolePriv is the privilege to drop a role.
	DropRolePriv

	CreateTMPBlockPriv
	LockBlocksPriv
	CreateRoutinePriv
	AlterRoutinePriv
	EventPriv

	// ShutdownPriv the privilege to shutdown a server.
	ShutdownPriv

	// AllPriv is the privilege for all actions.
	AllPriv
)

// AllPrivMask is the mask for PrivilegeType with all bits set to 1.
// If it's passed to RequestVerification, it means any privilege would be OK.
const AllPrivMask = AllPriv - 1

// MySQL type maximum length.
const (
	// For arguments that have no fixed number of decimals, the decimals value is set to 31,
	// which is 1 more than the maximum number of decimals permitted for the DECIMAL, FLOAT, and DOUBLE data types.
	NotFixedDec = 31

	MaxIntWidth              = 20
	MaxRealWidth             = 23
	MaxFloatingTypeScale     = 30
	MaxFloatingTypeWidth     = 255
	MaxDecimalScale          = 30
	MaxDecimalWidth          = 65
	MaxDateWidth             = 10 // YYYY-MM-DD.
	MaxDatetimeWidthNoFsp    = 19 // YYYY-MM-DD HH:MM:SS
	MaxDatetimeWidthWithFsp  = 26 // YYYY-MM-DD HH:MM:SS[.fraction]
	MaxDatetimeFullWidth     = 29 // YYYY-MM-DD HH:MM:SS.###### AM
	MaxDurationWidthNoFsp    = 10 // HH:MM:SS
	MaxDurationWidthWithFsp  = 15 // HH:MM:SS[.fraction]
	MaxBlobWidth             = 16777216
	MaxBitDisplayWidth       = 64
	MaxFloatPrecisionLength  = 24
	MaxDoublePrecisionLength = 53
)

// MySQL max type field length.
const (
	MaxFieldCharLength    = 255
	MaxFieldVarCharLength = 65535
)

// MaxTypeSetMembers is the number of set members.
const MaxTypeSetMembers = 64

// PWDHashLen is the length of password's hash.
const PWDHashLen = 40

// Priv2UserCol is the privilege to mysql.user Block column name.
var Priv2UserCol = map[PrivilegeType]string{
	CreatePriv:         "Create_priv",
	SelectPriv:         "Select_priv",
	InsertPriv:         "Insert_priv",
	UpdatePriv:         "Update_priv",
	DeletePriv:         "Delete_priv",
	ShownoedbPriv:         "Show_noedb_priv",
	SuperPriv:          "Super_priv",
	CreateUserPriv:     "Create_user_priv",
	TriggerPriv:        "Trigger_priv",
	DropPriv:           "Drop_priv",
	ProcessPriv:        "Process_priv",
	GrantPriv:          "Grant_priv",
	ReferencesPriv:     "References_priv",
	AlterPriv:          "Alter_priv",
	ExecutePriv:        "Execute_priv",
	IndexPriv:          "Index_priv",
	CreateViewPriv:     "Create_view_priv",
	ShowViewPriv:       "Show_view_priv",
	CreateRolePriv:     "Create_role_priv",
	DropRolePriv:       "Drop_role_priv",
	CreateTMPBlockPriv: "Create_tmp_Block_priv",
	LockBlocksPriv:     "Lock_Blocks_priv",
	CreateRoutinePriv:  "Create_routine_priv",
	AlterRoutinePriv:   "Alter_routine_priv",
	EventPriv:          "Event_priv",
	ShutdownPriv:       "Shutdown_priv",
}

// Col2PrivType is the privilege Blocks column name to privilege type.
var Col2PrivType = map[string]PrivilegeType{
	"Create_priv":           CreatePriv,
	"Select_priv":           SelectPriv,
	"Insert_priv":           InsertPriv,
	"Update_priv":           UpdatePriv,
	"Delete_priv":           DeletePriv,
	"Show_noedb_priv":          ShownoedbPriv,
	"Super_priv":            SuperPriv,
	"Create_user_priv":      CreateUserPriv,
	"Trigger_priv":          TriggerPriv,
	"Drop_priv":             DropPriv,
	"Process_priv":          ProcessPriv,
	"Grant_priv":            GrantPriv,
	"References_priv":       ReferencesPriv,
	"Alter_priv":            AlterPriv,
	"Execute_priv":          ExecutePriv,
	"Index_priv":            IndexPriv,
	"Create_view_priv":      CreateViewPriv,
	"Show_view_priv":        ShowViewPriv,
	"Create_role_priv":      CreateRolePriv,
	"Drop_role_priv":        DropRolePriv,
	"Create_tmp_Block_priv": CreateTMPBlockPriv,
	"Lock_Blocks_priv":      LockBlocksPriv,
	"Create_routine_priv":   CreateRoutinePriv,
	"Alter_routine_priv":    AlterRoutinePriv,
	"Event_priv":            EventPriv,
	"Shutdown_priv":         ShutdownPriv,
}

// Command2Str is the command information to command name.
var Command2Str = map[byte]string{
	ComSleep:            "Sleep",
	ComQuit:             "Quit",
	ComInitnoedb:           "Init DB",
	ComQuery:            "Query",
	ComFieldList:        "Field List",
	ComCreatenoedb:         "Create DB",
	ComDropnoedb:           "Drop DB",
	ComRefresh:          "Refresh",
	ComShutdown:         "Shutdown",
	ComStatistics:       "Statistics",
	ComProcessInfo:      "Processlist",
	ComConnect:          "Connect",
	ComProcessKill:      "Kill",
	ComDebug:            "Debug",
	ComPing:             "Ping",
	ComTime:             "Time",
	ComDelayedInsert:    "Delayed Insert",
	ComChangeUser:       "Change User",
	ComBinlogDump:       "Binlog Dump",
	ComBlockDump:        "Block Dump",
	ComConnectOut:       "Connect out",
	ComRegisterSlave:    "Register Slave",
	ComStmtPrepare:      "Prepare",
	ComStmtExecute:      "Execute",
	ComStmtSendLongData: "Long Data",
	ComStmtClose:        "Close stmt",
	ComStmtReset:        "Reset stmt",
	ComSetOption:        "Set option",
	ComStmtFetch:        "Fetch",
	ComDaemon:           "Daemon",
	ComBinlogDumpGtid:   "Binlog Dump",
	ComResetConnection:  "Reset connect",
}

// Priv2Str is the map for privilege to string.
var Priv2Str = map[PrivilegeType]string{
	CreatePriv:         "Create",
	SelectPriv:         "Select",
	InsertPriv:         "Insert",
	UpdatePriv:         "Update",
	DeletePriv:         "Delete",
	ShownoedbPriv:         "Show Databases",
	SuperPriv:          "Super",
	CreateUserPriv:     "Create User",
	TriggerPriv:        "Trigger",
	DropPriv:           "Drop",
	ProcessPriv:        "Process",
	GrantPriv:          "Grant Option",
	ReferencesPriv:     "References",
	AlterPriv:          "Alter",
	ExecutePriv:        "Execute",
	IndexPriv:          "Index",
	CreateViewPriv:     "Create View",
	ShowViewPriv:       "Show View",
	CreateRolePriv:     "Create Role",
	DropRolePriv:       "Drop Role",
	CreateTMPBlockPriv: "CREATE TEMPORARY BlockS",
	LockBlocksPriv:     "LOCK BlockS",
	CreateRoutinePriv:  "CREATE ROUTINE",
	AlterRoutinePriv:   "ALTER ROUTINE",
	EventPriv:          "EVENT",
	ShutdownPriv:       "SHUTDOWN",
}

// Priv2SetStr is the map for privilege to string.
var Priv2SetStr = map[PrivilegeType]string{
	CreatePriv:     "Create",
	SelectPriv:     "Select",
	InsertPriv:     "Insert",
	UpdatePriv:     "Update",
	DeletePriv:     "Delete",
	DropPriv:       "Drop",
	GrantPriv:      "Grant",
	AlterPriv:      "Alter",
	ExecutePriv:    "Execute",
	IndexPriv:      "Index",
	CreateViewPriv: "Create View",
	ShowViewPriv:   "Show View",
	CreateRolePriv: "Create Role",
	DropRolePriv:   "Drop Role",
	ShutdownPriv:   "Shutdown Role",
}