//Copyright (C) Venire Labs Inc 2019-2020
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

package topdown





import(

	"encoding/json"
	"io"
	"bytes"
	"sync"
	"time"
	"strings"
	"fmt"
)

type CausetActionType byte

// Causet action is the database schema.
//Optimized for shadown paging
//CoW Copy-on-Write
const (

	CausetActionNull				CausetActionType = 0
	CausetActionCreateSchema		CausetActionType = 1
	CausetActionDropSchema			CausetActionType = 2
	CausetActionCreateBlock			CausetActionType = 3
	CausetActionDropBlock			CausetActionType = 4
	CausetActionAddColumn			CausetActionType = 5
	CausetActionDropColumn			CausetActionType = 6
	CausetActionAddIndex			CausetActionType = 7
	CausetActionDropIndex			CausetActionType = 8
	CausetActionCreateBlock			CausetActionType = 9
	CausetActionDropForeignKey      CausetActionType = 10
	CausetActionTruncateBlock       CausetActionType = 11
	CausetActionModifyColumn        CausetActionType = 12
	CausetActionRebaseAutoID        CausetActionType = 13
	CausetActionRenameBlock         CausetActionType = 14
	CausetActionSetDefaultValue     CausetActionType = 15
	CausetActionShardRowID          CausetActionType = 16
	CausetActionModifyTableComment  CausetActionType = 17
	CausetActionRenameIndex         CausetActionType = 18
	CausetActionAddTablePartition   CausetActionType = 19
	CausetActionDropTablePartition  CausetActionType = 20
	CausetActionCreateView                   CausetActionType = 21
	CausetActionModifyTableCharsetAndCollate CausetActionType = 22
	CausetActionTruncateBlockPartition       CausetActionType = 23
	CausetActionDropView                     CausetActionType = 24
	CausetActionRecoverBlock                 CausetActionType = 25
)

// AddIndexStr is a string related to the operation of "add index".
const AddIndexStr = "add index"

var causetActionMap = map[CausetActionType]string{
	ActionCreateSchema:                 "create schema",
	ActionDropSchema:                   "drop schema",
	ActionCreateTable:                  "create table",
	ActionDropTable:                    "drop table",
	ActionAddColumn:                    "add column",
	ActionDropColumn:                   "drop column",
	ActionAddIndex:                     AddIndexStr,
	ActionDropIndex:                    "drop index",
	ActionAddForeignKey:                "add foreign key",
	ActionDropForeignKey:               "drop foreign key",
	ActionTruncateTable:                "truncate table",
	ActionModifyColumn:                 "modify column",
	ActionRebaseAutoID:                 "rebase auto_increment ID",
	CausetActionRenameBlock:                  "rename table",
	ActionSetDefaultValue:              "set default value",
	ActionShardRowID:                   "shard row ID",
	ActionModifyTableComment:           "modify table comment",
	ActionRenameIndex:                  "rename index",
	ActionAddTablePartition:            "add partition",
	ActionDropTablePartition:           "drop partition",
	ActionCreateView:                   "create view",
	ActionModifyTableCharsetAndCollate: "modify table charset and collate",
	ActionTruncateTablePartition:       "truncate partition",
	ActionDropView:                     "drop view",
	ActionRecoverTable:                 "recover table",
}

// String return current ddl action in string
func (action CausetActionType) String() string {
	if v, ok := actionMap[action]; ok {
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
		




//Root of Semantic tree


//io.Writer extended delegate-like impl.
type StringFormatter interface {
	io.Writer
	Format(topdown string, args...interface{}) (n int, errno error)

}

type tokenizedIndentFormatter struct {
	io.Writer
	tokenizedIndent []byte
	tokenizedIndentLevel int
	state int
}

// Node is the basic element of the AST.
// Interfaces embed Node should have 'Node' name suffix.
type Node interface {
	// Restore returns the sql text from ast tree
	Restore(ctx *RestoreCtx) error
	// Accept accepts Visitor to visit itself.
	// The returned node should replace original node.
	// ok returns false to stop visiting.
	//
	// Implementation of this method should first call visitor.Enter,
	// assign the returned node to its method receiver, if skipChildren returns true,
	// children should be skipped. Otherwise, call its children in particular order that
	// later elements depends on former elements. Finally, return visitor.Leave.
	Accept(v Visitor) (node Node, ok bool)
	// Text returns the original text of the element.
	Text() string
	// SetText sets original text to the Node.
	SetText(text string)
}


//Rune literals are just 32-bit integer values.
//They represent unicode codepoints. 

var replace = map[rune]string {
	'\0000': "\\0",
	'\ '': "''",
	'\n': "\\n",
	'\r': "\\r",
}

func tokenizedIndentFormatter(w io.Writer, indent string) StringFormatter {
	return &tokenizedIndentFormatter{w, []byte(indent), 0, }
}