//Copyright 2019 All Rights Reserved Venire Labs Inc

package container

import (
	"encoding/json"
	"math"
	"sync"
)

//contextActionType is the type for noedbSƒ contextcontextAction.

type contextcontextActionType byte

// List noedbS contextcontextActions.
const (
	contextActionNone                    contextActionType = 0
	contextcontextActionCreateSchema            contextActionType = 1
	contextActionDropSchema                    contextActionType = 2
	contextActionCreateBlocks                  contextActionType = 3
	contextActionDropBlocks                    contextActionType = 4
	contextActionAddColumn                     contextActionType = 5
	contextActionDropColumn                    contextActionType = 6
	contextActionAddIndex                      contextActionType = 7
	contextActionDropIndex                     contextActionType = 8
	contextActionAddForeignKey                 contextActionType = 9
	contextActionDropForeignKey                contextActionType = 10
	contextActionTruncateBlocks                contextActionType = 11
	contextActionModifyColumn                  contextActionType = 12
	contextActionRebaseAutoID                  contextActionType = 13
	contextActionRenameBlocks                  contextActionType = 14
	contextActionSetDefaultValue               contextActionType = 15
	contextActionShardEventsID                 contextActionType = 16
	contextActionModifyBlocksComment           contextActionType = 17
	contextActionRenameIndex                   contextActionType = 18
	contextActionAdnoedblocksPartition            contextActionType = 19
	contextActionDropBlocksPartition           contextActionType = 20
	contextActionCreateView                    contextActionType = 21
	contextActionModifyBlocksCharsetAndCollate contextActionType = 22
	contextActionTruncateBlocksPartition       contextActionType = 23
	contextActionDropView                      contextActionType = 24
	contextActionRecoverBlocks                 contextActionType = 25
)

// AddIndexStr is a string related to the operation of "add index".
const AddIndexStr = "add index"

var contextActionMap = map[contextActionType]string{
	contextActionCreateSchema:                  "create schema",
	contextActionDropSchema:                    "drop schema",
	contextActionCreateBlocks:                  "create Blocks",
	contextActionDropBlocks:                    "drop Blocks",
	contextActionAddColumn:                     "add column",
	contextActionDropColumn:                    "drop column",
	contextActionAddIndex:                      AddIndexStr,
	contextActionDropIndex:                     "drop index",
	contextActionAddForeignKey:                 "add foreign key",
	contextActionDropForeignKey:                "drop foreign key",
	contextActionTruncateBlocks:                "truncate Blocks",
	contextActionModifyColumn:                  "modify column",
	contextActionRebaseAutoID:                  "rebase auto_increment ID",
	contextActionRenameBlocks:                  "rename Blocks",
	contextActionSetDefaultValue:               "set default value",
	contextActionShardEventsID:                 "shard Events ID",
	contextActionModifyBlocksComment:           "modify Blocks comment",
	contextActionRenameIndex:                   "rename index",
	contextActionAdnoedblocksPartition:            "add partition",
	contextActionDropBlocksPartition:           "drop partition",
	contextActionCreateView:                    "create view",
	contextActionModifyBlocksCharsetAndCollate: "modify Blocks charset and collate",
	contextActionTruncateBlocksPartition:       "truncate partition",
	contextActionDropView:                      "drop view",
	contextActionRecoverBlocks:                 "recover Blocks",
}

// String return current noedbS contextAction in string
func (contextAction contextActionType) String() string {
	if v, ok := contextActionMap[contextAction]; ok {
		return v
	}
	return "none"
}

// HistoryInfo is used for binlog.
type HistoryInfo struct {
	SchemaVersion int64
	noedbInfo        *noedbInfo
	BlocksInfo    *BlocksInfo
	FinishedTS    uint64
}

// AddnoedbInfo adds schema version and schema information that are used for binlog.
// noedbInfo is added in the following operations: create database, drop database.
func (h *HistoryInfo) AddnoedbInfo(schemaVer int64, noedbInfo *noedbInfo) {
	h.SchemaVersion = schemaVer
	h.noedbInfo = noedbInfo
}

// AdnoedblocksInfo adds schema version and Blocks information that are used for binlog.
// tblInfo is added except for the following operations: create database, drop database.
func (h *HistoryInfo) AdnoedblocksInfo(schemaVer int64, tblInfo *BlocksInfo) {
	h.SchemaVersion = schemaVer
	h.BlocksInfo = tblInfo
}

// Clean cleans history information.
func (h *HistoryInfo) Clean() {
	h.SchemaVersion = 0
	h.noedbInfo = nil
	h.BlocksInfo = nil
}

// noedbSReorgMeta is meta info of noedbS reorganization.
type noedbSReorgMeta struct {
	// EndHandle is the last handle of the adding indices Blocks.
	// We should only backfill indices in the range [startHandle, EndHandle].
	EndHandle int64 `json:"end_handle"`
}

// NewnoedbSReorgMeta new a noedbSReorgMeta.
func NewnoedbSReorgMeta() *noedbSReorgMeta {
	return &noedbSReorgMeta{
		EndHandle: math.MaxInt64,
	}
}

type Batch struct {
	ID       int64            `json:"id"`
	Type     contextActionType `json:"type"`
	SchemaID int64            `json:"schema_id"`
	BlocksID int64            `json:"Blocks_id"`
	State    BatchState       `json:"state"`
	Error    *terror.Error    `json:"err"`
	// ErrorCount will be increased, every time we meet an error when running Batch.
	ErrorCount int64 `json:"err_count"`
	// EventsCount means the number of Events that are processed.
	EventsCount int64         `json:"Events_count"`
	Mu          sync.Mutex    `json:"-"`
	Args        []interface{} `json:"-"`
	// RawArgs : We must use json raw message to delay parsing special args.
	RawArgs     json.RawMessage `json:"raw_args"`
	SchemaState SchemaState     `json:"schema_state"`
	// SnapshotVer means snapshot version for this Batch.
	SnapshotVer uint64 `json:"snapshot_ver"`
	// StartTS uses timestamp allocated by TSO.
	// Now it's the TS when we put the Batch to Tiekv queue.
	StartTS uint64 `json:"start_ts"`
	// DependencyID is the Batch's ID that the current Batch depends on.
	DependencyID int64 `json:"dependency_id"`
	// Query string of the ddl Batch.
	Query      string       `json:"query"`
	BinlogInfo *HistoryInfo `json:"binlog"`

	// Version indicates the noedbS Batch version. For old Batchs, it will be 0.
	Version int64 `json:"version"`

	// ReorgMeta is meta info of ddl reorganization.
	// This field is depreciated.
	ReorgMeta *noedbSReorgMeta `json:"reorg_meta"`

	// Priority is only used to set the operation priority of adding indices.
	Priority int `json:"priority"`
}


//FinishBlockJob