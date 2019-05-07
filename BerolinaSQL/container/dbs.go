//Copyright 2019 All Rights Reserved Venire Labs Inc

package container

import (
	"math"
)

// CausetActionType is the type for DBSƒ CausetCausetAction.
type CausetCausetActionType byte

// List DBS CausetCausetActions.
const (
	CausetCausetActionNone                   CausetActionType = 0
	CausetCausetActionCreateSchema           CausetActionType = 1
	CausetActionDropSchema                   CausetActionType = 2
	CausetActionCreateTable                  CausetActionType = 3
	CausetActionDropTable                    CausetActionType = 4
	CausetActionAddColumn                    CausetActionType = 5
	CausetActionDropColumn                   CausetActionType = 6
	CausetActionAddIndex                     CausetActionType = 7
	CausetActionDropIndex                    CausetActionType = 8
	CausetActionAddForeignKey                CausetActionType = 9
	CausetActionDropForeignKey               CausetActionType = 10
	CausetActionTruncateTable                CausetActionType = 11
	CausetActionModifyColumn                 CausetActionType = 12
	CausetActionRebaseAutoID                 CausetActionType = 13
	CausetActionRenameTable                  CausetActionType = 14
	CausetActionSetDefaultValue              CausetActionType = 15
	CausetActionShardRowID                   CausetActionType = 16
	CausetActionModifyTableComment           CausetActionType = 17
	CausetActionRenameIndex                  CausetActionType = 18
	CausetActionAddTablePartition            CausetActionType = 19
	CausetActionDropTablePartition           CausetActionType = 20
	CausetActionCreateView                   CausetActionType = 21
	CausetActionModifyTableCharsetAndCollate CausetActionType = 22
	CausetActionTruncateTablePartition       CausetActionType = 23
	CausetActionDropView                     CausetActionType = 24
	CausetActionRecoverTable                 CausetActionType = 25
)

// AddIndexStr is a string related to the operation of "add index".
const AddIndexStr = "add index"

var CausetActionMap = map[CausetActionType]string{
	CausetActionCreateSchema:                 "create schema",
	CausetActionDropSchema:                   "drop schema",
	CausetActionCreateTable:                  "create table",
	CausetActionDropTable:                    "drop table",
	CausetActionAddColumn:                    "add column",
	CausetActionDropColumn:                   "drop column",
	CausetActionAddIndex:                     AddIndexStr,
	CausetActionDropIndex:                    "drop index",
	CausetActionAddForeignKey:                "add foreign key",
	CausetActionDropForeignKey:               "drop foreign key",
	CausetActionTruncateTable:                "truncate table",
	CausetActionModifyColumn:                 "modify column",
	CausetActionRebaseAutoID:                 "rebase auto_increment ID",
	CausetActionRenameTable:                  "rename table",
	CausetActionSetDefaultValue:              "set default value",
	CausetActionShardRowID:                   "shard row ID",
	CausetActionModifyTableComment:           "modify table comment",
	CausetActionRenameIndex:                  "rename index",
	CausetActionAddTablePartition:            "add partition",
	CausetActionDropTablePartition:           "drop partition",
	CausetActionCreateView:                   "create view",
	CausetActionModifyTableCharsetAndCollate: "modify table charset and collate",
	CausetActionTruncateTablePartition:       "truncate partition",
	CausetActionDropView:                     "drop view",
	CausetActionRecoverTable:                 "recover table",
}

// String return current DBS CausetAction in string
func (CausetAction CausetActionType) String() string {
	if v, ok := CausetActionMap[CausetAction]; ok {
		return v
	}
	return "none"
}

// HistoryInfo is used for binlog.
type HistoryInfo struct {
	SchemaVersion int64
	DBInfo        *DBInfo
	TableInfo     *TableInfo
	FinishedTS    uint64
}

// AddDBInfo adds schema version and schema information that are used for binlog.
// dbInfo is added in the following operations: create database, drop database.
func (h *HistoryInfo) AddDBInfo(schemaVer int64, dbInfo *DBInfo) {
	h.SchemaVersion = schemaVer
	h.DBInfo = dbInfo
}

// AddTableInfo adds schema version and table information that are used for binlog.
// tblInfo is added except for the following operations: create database, drop database.
func (h *HistoryInfo) AddTableInfo(schemaVer int64, tblInfo *TableInfo) {
	h.SchemaVersion = schemaVer
	h.TableInfo = tblInfo
}

// Clean cleans history information.
func (h *HistoryInfo) Clean() {
	h.SchemaVersion = 0
	h.DBInfo = nil
	h.TableInfo = nil
}

// DBSReorgMeta is meta info of DBS reorganization.
type DBSReorgMeta struct {
	// EndHandle is the last handle of the adding indices table.
	// We should only backfill indices in the range [startHandle, EndHandle].
	EndHandle int64 `json:"end_handle"`
}

// NewDBSReorgMeta new a DBSReorgMeta.
func NewDBSReorgMeta() *DBSReorgMeta {
	return &DBSReorgMeta{
		EndHandle: math.MaxInt64,
	}
}

type Job struct
