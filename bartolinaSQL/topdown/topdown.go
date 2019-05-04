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
	CausetActionDropBlock			

	synset0 = iota
	synsetBinaryBranching
	synsetSingletonBranching
	synsetSwarmBranching
	FlagConstant       uint64 = 0
	FlagHasParamMarker uint64 = 1 << iota
	FlagHasFunc
	FlagHasReference
	FlagHasAggregateFunc
	FlagHasSubquery
	FlagHasVariable
	FlagHasDefault
	FlagPreEvaluated
	FlagHasWindowFunc
)

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