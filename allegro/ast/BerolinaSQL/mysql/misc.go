//2020 WHTCORPS INC ALL RIGHTS RESERVED
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

package ast

import (
	"strconv"

	"github.com/whtcorpsinc/MilevaDB-Prod/errors"
)

var (
	_ StmtNode = &AdminStmt{}
	_ StmtNode = &AlterUserStmt{}
	_ StmtNode = &BeginStmt{}
	_ StmtNode = &BinlogStmt{}
	_ StmtNode = &CommitStmt{}
	_ StmtNode = &CreateUserStmt{}
	_ StmtNode = &DeallocateStmt{}
	_ StmtNode = &DoStmt{}
	_ StmtNode = &ExecuteStmt{}
	_ StmtNode = &ExplainStmt{}
	_ StmtNode = &GrantStmt{}
	_ StmtNode = &PrepareStmt{}
	_ StmtNode = &RollbackStmt{}
	_ StmtNode = &SetPwdStmt{}
	_ StmtNode = &SetRoleStmt{}
	_ StmtNode = &SetDefaultRoleStmt{}
	_ StmtNode = &SetStmt{}
	_ StmtNode = &UseStmt{}
	_ StmtNode = &FlushStmt{}
	_ StmtNode = &KillStmt{}
	_ StmtNode = &CreateBindingStmt{}
	_ StmtNode = &DropBindingStmt{}
	_ StmtNode = &ShutdownStmt{}

	_ Node = &PrivElem{}
	_ Node = &VariableAssignment{}
)

// Isolation level constants.
const (
	ReadCommitted   = "READ-COMMITTED"
	ReadUncommitted = "READ-UNCOMMITTED"
	Serializable    = "SERIALIZABLE"
	RepeatableRead  = "REPEATABLE-READ"

	// Valid formats for explain statement.
	ExplainFormatROW     = "row"
	ExplainFormatDOT     = "dot"
	ExplainFormatHint    = "hint"
	ExplainFormatVerbose = "verbose"
	PumpType             = "PUMP"
	DrainerType          = "DRAINER"
)

// Transaction mode constants.
const (
	Optimistic  = "OPTIMISTIC"
	Pessimistic = "PESSIMISTIC"
)

var (
	// ExplainFormats stores the valid formats for explain statement, used by validator.
	ExplainFormats = []string{
		ExplainFormatROW,
		ExplainFormatDOT,
		ExplainFormatHint,
		ExplainFormatVerbose,
	}
)

// TypeOpt is used for parsing data type option from SQL.
type TypeOpt struct {
	IsUnsigned bool
	IsZerofill bool
}

// FloatOpt is used for parsing floating-point type option from SQL.
// See http://dev.mysql.com/doc/refman/5.7/en/floating-point-types.html
type FloatOpt struct {
	Flen    int
	Decimal int
}

// AuthOption is used for parsing create use statement.
type AuthOption struct {
	// ByAuthString set as true, if AuthString is used for authorization. Otherwise, authorization is done by HashString.
	ByAuthString bool
	AuthString   string
	HashString   string
	// TODO: support auth_plugin
}

// Restore implements Node interface.
func (n *AuthOption) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("IDENTIFIED BY ")
	if n.ByAuthString {
		ctx.WriteString(n.AuthString)
	} else {
		ctx.WriteKeyWord("PASSWORD ")
		ctx.WriteString(n.HashString)
	}
	return nil
}

// TraceStmt is a statement to trace what sql actually does at background.
type TraceStmt struct {
	stmtNode

	Stmt   StmtNode
	Format string
}

// Restore implements Node interface.
func (n *TraceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("TRACE ")
	if n.Format != "json" {
		ctx.WriteKeyWord("FORMAT")
		ctx.WritePlain(" = ")
		ctx.WriteString(n.Format)
		ctx.WritePlain(" ")
	}
	if err := n.Stmt.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore TraceStmt.Stmt")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *TraceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TraceStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(StmtNode)
	return v.Leave(n)
}

// ExplainForStmt is a statement to provite information about how is SQL statement executeing
// in connection #ConnectionID
// See https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainForStmt struct {
	stmtNode

	Format       string
	ConnectionID uint64
}

// Restore implements Node interface.
func (n *ExplainForStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("EXPLAIN ")
	ctx.WriteKeyWord("FORMAT ")
	ctx.WritePlain("= ")
	ctx.WriteString(n.Format)
	ctx.WritePlain(" ")
	ctx.WriteKeyWord("FOR ")
	ctx.WriteKeyWord("CONNECTION ")
	ctx.WritePlain(strconv.FormatUint(n.ConnectionID, 10))
	return nil
}

// Accept implements Node Accept interface.
func (n *ExplainForStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExplainForStmt)
	return v.Leave(n)
}

// ExplainStmt is a statement to provide information about how is SQL statement executed
// or get columns information in a table.
// See https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainStmt struct {
	stmtNode

	Stmt    StmtNode
	Format  string
	Analyze bool
}

// Restore implements Node interface.
func (n *ExplainStmt) Restore(ctx *format.RestoreCtx) error {
	if showStmt, ok := n.Stmt.(*ShowStmt); ok {
		ctx.WriteKeyWord("DESC ")
		if err := showStmt.Block.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore ExplainStmt.ShowStmt.Block")
		}
		if showStmt.Column != nil {
			ctx.WritePlain(" ")
			if err := showStmt.Column.Restore(ctx); err != nil {
				return errors.Annotate(err, "An error occurred while restore ExplainStmt.ShowStmt.Column")
			}
		}
		return nil
	}
	ctx.WriteKeyWord("EXPLAIN ")
	if n.Analyze {
		ctx.WriteKeyWord("ANALYZE ")
	} else {
		ctx.WriteKeyWord("FORMAT ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.Format)
		ctx.WritePlain(" ")
	}
	if err := n.Stmt.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore ExplainStmt.Stmt")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ExplainStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExplainStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(DMLNode)
	return v.Leave(n)
}

// PrepareStmt is a statement to prepares a SQL statement which contains placeholders,
// and it is executed with ExecuteStmt and released with DeallocateStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/prepare.html
type PrepareStmt struct {
	stmtNode

	Name    string
	SQLText string
	SQLVar  *VariableExpr
}

// Restore implements Node interface.
func (n *PrepareStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PREPARE ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" FROM ")
	if n.SQLText != "" {
		ctx.WriteString(n.SQLText)
		return nil
	}
	if n.SQLVar != nil {
		if err := n.SQLVar.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore PrepareStmt.SQLVar")
		}
		return nil
	}
	return errors.New("An error occurred while restore PrepareStmt")
}

// Accept implements Node Accept interface.
func (n *PrepareStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PrepareStmt)
	if n.SQLVar != nil {
		node, ok := n.SQLVar.Accept(v)
		if !ok {
			return n, false
		}
		n.SQLVar = node.(*VariableExpr)
	}
	return v.Leave(n)
}

// DeallocateStmt is a statement to release PreparedStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/deallocate-prepare.html
type DeallocateStmt struct {
	stmtNode

	Name string
}

// Restore implements Node interface.
func (n *DeallocateStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DEALLOCATE PREPARE ")
	ctx.WriteName(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *DeallocateStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeallocateStmt)
	return v.Leave(n)
}

// Prepared represents a prepared statement.
type Prepared struct {
	Stmt          StmtNode
	StmtType      string
	Params        []ParamMarkerExpr
	SchemaVersion int64
	UseCache      bool
	CachedPlan    interface{}
	CachedNames   interface{}
}

// ExecuteStmt is a statement to execute PreparedStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/execute.html
type ExecuteStmt struct {
	stmtNode

	Name       string
	UsingVars  []ExprNode
	BinaryArgs interface{}
	ExecID     uint32
	IdxInMulti int
}

// Restore implements Node interface.
func (n *ExecuteStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("EXECUTE ")
	ctx.WriteName(n.Name)
	if len(n.UsingVars) > 0 {
		ctx.WriteKeyWord(" USING ")
		for i, val := range n.UsingVars {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := val.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore ExecuteStmt.UsingVars index %d", i)
			}
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ExecuteStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExecuteStmt)
	for i, val := range n.UsingVars {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.UsingVars[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// BeginStmt is a statement to start a new transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type BeginStmt struct {
	stmtNode
	Mode     string
	ReadOnly bool
	Bound    *TimestampBound
}

// Restore implements Node interface.
func (n *BeginStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Mode == "" {
		if n.ReadOnly {
			ctx.WriteKeyWord("START TRANSACTION READ ONLY")
			if n.Bound != nil {
				switch n.Bound.Mode {
				case TimestampBoundStrong:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND STRONG")
				case TimestampBoundMaxStaleness:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND MAX STALENESS ")
					return n.Bound.Timestamp.Restore(ctx)
				case TimestampBoundExactStaleness:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND EXACT STALENESS ")
					return n.Bound.Timestamp.Restore(ctx)
				case TimestampBoundReadTimestamp:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND READ TIMESTAMP ")
					return n.Bound.Timestamp.Restore(ctx)
				case TimestampBoundMinReadTimestamp:
					ctx.WriteKeyWord(" WITH TIMESTAMP BOUND MIN READ TIMESTAMP ")
					return n.Bound.Timestamp.Restore(ctx)
				}
			}
		} else {
			ctx.WriteKeyWord("START TRANSACTION")
		}
	} else {
		ctx.WriteKeyWord("BEGIN ")
		ctx.WriteKeyWord(n.Mode)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *BeginStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*BeginStmt)
	if n.Bound != nil && n.Bound.Timestamp != nil {
		newTimestamp, ok := n.Bound.Timestamp.Accept(v)
		if !ok {
			return n, false
		}
		n.Bound.Timestamp = newTimestamp.(ExprNode)
	}
	return v.Leave(n)
}

// BinlogStmt is an internal-use statement.
// We just parse and ignore it.
// See http://dev.mysql.com/doc/refman/5.7/en/binlog.html
type BinlogStmt struct {
	stmtNode
	Str string
}

// Restore implements Node interface.
