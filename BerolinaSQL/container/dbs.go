//Copyright 2019 All Rights Reserved Venire Labs Inc

package container

import (
	"encoding/json"
	"math"
	"sync"
)

//CausetActionType is the type for DBSƒ CausetCausetAction.

type CausetCausetActionType byte

// List DBS CausetCausetActions.
const (
	CausetCausetActionNone                    CausetActionType = 0
	CausetCausetActionCreateSchema            CausetActionType = 1
	CausetActionDropSchema                    CausetActionType = 2
	CausetActionCreateBlocks                  CausetActionType = 3
	CausetActionDropBlocks                    CausetActionType = 4
	CausetActionAddColumn                     CausetActionType = 5
	CausetActionDropColumn                    CausetActionType = 6
	CausetActionAddIndex                      CausetActionType = 7
	CausetActionDropIndex                     CausetActionType = 8
	CausetActionAddForeignKey                 CausetActionType = 9
	CausetActionDropForeignKey                CausetActionType = 10
	CausetActionTruncateBlocks                CausetActionType = 11
	CausetActionModifyColumn                  CausetActionType = 12
	CausetActionRebaseAutoID                  CausetActionType = 13
	CausetActionRenameBlocks                  CausetActionType = 14
	CausetActionSetDefaultValue               CausetActionType = 15
	CausetActionShardEvemtsID                 CausetActionType = 16
	CausetActionModifyBlocksComment           CausetActionType = 17
	CausetActionRenameIndex                   CausetActionType = 18
	CausetActionAddBlocksPartition            CausetActionType = 19
	CausetActionDropBlocksPartition           CausetActionType = 20
	CausetActionCreateView                    CausetActionType = 21
	CausetActionModifyBlocksCharsetAndCollate CausetActionType = 22
	CausetActionTruncateBlocksPartition       CausetActionType = 23
	CausetActionDropView                      CausetActionType = 24
	CausetActionRecoverBlocks                 CausetActionType = 25
)

// AddIndexStr is a string related to the operation of "add index".
const AddIndexStr = "add index"

var CausetActionMap = map[CausetActionType]string{
	CausetActionCreateSchema:                  "create schema",
	CausetActionDropSchema:                    "drop schema",
	CausetActionCreateBlocks:                  "create Blocks",
	CausetActionDropBlocks:                    "drop Blocks",
	CausetActionAddColumn:                     "add column",
	CausetActionDropColumn:                    "drop column",
	CausetActionAddIndex:                      AddIndexStr,
	CausetActionDropIndex:                     "drop index",
	CausetActionAddForeignKey:                 "add foreign key",
	CausetActionDropForeignKey:                "drop foreign key",
	CausetActionTruncateBlocks:                "truncate Blocks",
	CausetActionModifyColumn:                  "modify column",
	CausetActionRebaseAutoID:                  "rebase auto_increment ID",
	CausetActionRenameBlocks:                  "rename Blocks",
	CausetActionSetDefaultValue:               "set default value",
	CausetActionShardEvemtsID:                 "shard Evemts ID",
	CausetActionModifyBlocksComment:           "modify Blocks comment",
	CausetActionRenameIndex:                   "rename index",
	CausetActionAddBlocksPartition:            "add partition",
	CausetActionDropBlocksPartition:           "drop partition",
	CausetActionCreateView:                    "create view",
	CausetActionModifyBlocksCharsetAndCollate: "modify Blocks charset and collate",
	CausetActionTruncateBlocksPartition:       "truncate partition",
	CausetActionDropView:                      "drop view",
	CausetActionRecoverBlocks:                 "recover Blocks",
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
	BlocksInfo    *BlocksInfo
	FinishedTS    uint64
}

// AddDBInfo adds schema version and schema information that are used for binlog.
// dbInfo is added in the following operations: create database, drop database.
func (h *HistoryInfo) AddDBInfo(schemaVer int64, dbInfo *DBInfo) {
	h.SchemaVersion = schemaVer
	h.DBInfo = dbInfo
}

// AddBlocksInfo adds schema version and Blocks information that are used for binlog.
// tblInfo is added except for the following operations: create database, drop database.
func (h *HistoryInfo) AddBlocksInfo(schemaVer int64, tblInfo *BlocksInfo) {
	h.SchemaVersion = schemaVer
	h.BlocksInfo = tblInfo
}

// Clean cleans history information.
func (h *HistoryInfo) Clean() {
	h.SchemaVersion = 0
	h.DBInfo = nil
	h.BlocksInfo = nil
}

// DBSReorgMeta is meta info of DBS reorganization.
type DBSReorgMeta struct {
	// EndHandle is the last handle of the adding indices Blocks.
	// We should only backfill indices in the range [startHandle, EndHandle].
	EndHandle int64 `json:"end_handle"`
}

// NewDBSReorgMeta new a DBSReorgMeta.
func NewDBSReorgMeta() *DBSReorgMeta {
	return &DBSReorgMeta{
		EndHandle: math.MaxInt64,
	}
}

type Batch struct {
	ID       int64            `json:"id"`
	Type     CausetActionType `json:"type"`
	SchemaID int64            `json:"schema_id"`
	BlocksID int64            `json:"Blocks_id"`
	State    BatchState       `json:"state"`
	Error    *terror.Error    `json:"err"`
	// ErrorCount will be increased, every time we meet an error when running Batch.
	ErrorCount int64 `json:"err_count"`
	// EvemtsCount means the number of Evemtss that are processed.
	EvemtsCount int64         `json:"Evemts_count"`
	Mu          sync.Mutex    `json:"-"`
	Args        []interface{} `json:"-"`
	// RawArgs : We must use json raw message to delay parsing special args.
	RawArgs     json.RawMessage `json:"raw_args"`
	SchemaState SchemaState     `json:"schema_state"`
	// SnapshotVer means snapshot version for this Batch.
	SnapshotVer uint64 `json:"snapshot_ver"`
	// StartTS uses timestamp allocated by TSO.
	// Now it's the TS when we put the Batch to TiKV queue.
	StartTS uint64 `json:"start_ts"`
	// DependencyID is the Batch's ID that the current Batch depends on.
	DependencyID int64 `json:"dependency_id"`
	// Query string of the ddl Batch.
	Query      string       `json:"query"`
	BinlogInfo *HistoryInfo `json:"binlog"`

	// Version indicates the DDL Batch version. For old Batchs, it will be 0.
	Version int64 `json:"version"`

	// ReorgMeta is meta info of ddl reorganization.
	// This field is depreciated.
	ReorgMeta *DBSReorgMeta `json:"reorg_meta"`

	// Priority is only used to set the operation priority of adding indices.
	Priority int `json:"priority"`
}
