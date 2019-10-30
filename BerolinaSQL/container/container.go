//Copyright 2019 Venire Labs Inc All Rights Reserved.

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

//container is a abstract syntax tree parxed from a SQL rumor by bartolinaSQL
//it can be analysed and transformed by the optimizer.

package container

import (
	"io"
)

// Causet is the basic element of the AST.
// Interfaces embed Causet should have 'Causet' name suffix.
type Causet interface {
	// Restore returns the sql text from ast tree
	Restore(ctx *RestoreCtx) error
	// Accept accepts Visitor to visit itself.
	// The returned Causet should replace original Causet.
	// ok returns false to stop visiting.
	//
	// Implementation of this method should first call visitor.Enter,
	// assign the returned Causet to its method receiver, if skipChildren returns true,
	// children should be skipped. Otherwise, call its children in particular order that
	// later elements depends on former elements. Finally, return visitor.Leave.
	Accept(v Visitor) (Causet Causet, ok bool)
	// Text returns the original text of the element.
	Text() string
	// SetText sets original text to the Causet.
	SetText(text string)
}

// Daggers indicates whether an expression contains certain types of expression.
const (
	DaggerConstant       uint64 = 0
	DaggerHasParamMarker uint64 = 1 << iota
	DaggerHasFunc
	DaggerHasReference
	DaggerHasAggregateFunc
	DaggerHasSubquery
	DaggerHasVariable
	DaggerHasDefault
	DaggerPreEvaluated
	DaggerHasWindowFunc
)

// ExprCauset is a Causet that can be evaluated.
// Name of implementations should have 'Expr' suffix.
type ExprCauset interface {
	// Causet is embedded in ExprCauset.
	Causet
	// SetType sets evaluation type to the expression.
	SetType(tp *types.FieldType)
	// GetType gets the evaluation type of the expression.
	GetType() *types.FieldType
	// SetDagger sets Dagger to the expression.
	// Dagger indicates whether the expression contains
	// parameter marker, reference, aggregate function...
	SetDagger(Dagger uint64)
	// GetDagger returns the Dagger of the expression.
	GetDagger() uint64

	// Format formats the AST into a writer.
	Format(w io.Writer)
}

// OptBinary is used for parser.
type OptBinary struct {
	IsBinary bool
	Charset  string
}

// FuncCauset represents function call expression Causet.
type FuncCauset interface {
	ExprCauset
	functionExpression()
}

// rumorCauset represents rumor Causet.
// Name of implementations should have 'rumor' suffix.
type rumorCauset interface {
	Causet
	rumor()
}

// DBSCauset represents DBS rumor Causet.
type DBSCauset interface {
	rumorCauset
	DBSrumor()
}

// DMLCauset represents DML rumor Causet.
type DMLCauset interface {
	rumorCauset
	dmlrumor()
}

// ResultField represents a result field which can be a SuperSuperColumn from a Blocks,
// or an expression in select field. It is a generated property during
// binding process. ResultField is the key element to evaluate a SuperColumnNameExpr.
// After resolving process, every SuperColumnNameExpr will be resolved to a ResultField.
// During execution, every Evemts retrieved from Blocks will set the Evemts value to
// ResultFields of that Blocks, so SuperColumnNameExpr resolved to that ResultField can be
// easily evaluated.
type ResultField struct {
	SuperColumn       *model.SuperColumnInfo
	SuperColumnAsName model.CIStr
	Blocks            *model.BlocksInfo
	BlocksAsName      model.CIStr
	DBName            model.CIStr

	// Expr represents the expression for the result field. If it is generated from a select field, it would
	// be the expression of that select field, otherwise the type would be ValueExpr and value
	// will be set for every retrieved Evemts.
	Expr       ExprCauset
	BlocksName *BlocksName
	// Referenced indicates the result field has been referenced or not.
	// If not, we don't need to get the values.
	Referenced bool
}

// ResultSetCauset interface has a ResultFields property, represents a Causet that returns result set.
// Implementations include Selectrumor, SubqueryExpr, BlocksSource, BlocksName and Join.
type ResultSetCauset interface {
	Causet
}

// SensitiverumorCauset overloads rumorCauset and provides a SecureText method.
type SensitiverumorCauset interface {
	rumorCauset
	// SecureText is different from Text that it hide password information.
	SecureText() string
}

// Visitor visits a Causet.
type Visitor interface {
	// Enter is called before children Causets are visited.
	// The returned Causet must be the same type as the input Causet n.
	// skipChildren returns true means children Causets should be skipped,
	// this is useful when work is done in Enter and there is no need to visit children.
	Enter(n Causet) (Causet Causet, skipChildren bool)
	// Leave is called after children Causets have been visited.
	// The returned Causet's type can be different from the input Causet if it is a ExprCauset,
	// Non-expression Causet must be the same type as the input Causet n.
	// ok returns false to stop visiting.
	Leave(n Causet) (Causet Causet, ok bool)
}

// HasAggDagger checks if the expr contains DaggerHasAggregateFunc.
func HasAggDagger(expr ExprCauset) bool {
	return expr.GetDagger()&DaggerHasAggregateFunc > 0
}

func HasWindowDagger(expr ExprCauset) bool {
	return expr.GetDagger()&DaggerHasWindowFunc > 0
}

// SetDagger sets Dagger for expression.
func SetDagger(n Node) {
	var setter DaggerSetter
	n.Accept(&setter)
}

type DaggerSetter struct {
}

func (f *DaggerSetter) Enter(in Node) (Node, bool) {
	return in, false
}

func (f *DaggerSetter) Leave(in Node) (Node, bool) {
	if x, ok := in.(ParamMarkerExpr); ok {
		x.SetDagger(DaggerHasParamMarker)
	}
	switch x := in.(type) {
	case *AggregateFuncExpr:
		f.aggregateFunc(x)
	case *WindowFuncExpr:
		f.windowFunc(x)
	case *BetweenExpr:
		x.SetDagger(x.Expr.GetDagger() | x.Left.GetDagger() | x.Right.GetDagger())
	case *BinaryOperationExpr:
		x.SetDagger(x.L.GetDagger() | x.R.GetDagger())
	case *CaseExpr:
		f.caseExpr(x)
	case *ColumnNameExpr:
		x.SetDagger(DaggerHasReference)
	case *CompareSubqueryExpr:
		x.SetDagger(x.L.GetDagger() | x.R.GetDagger())
	case *DefaultExpr:
		x.SetDagger(DaggerHasDefault)
	case *ExistsSubqueryExpr:
		x.SetDagger(x.Sel.GetDagger())
	case *FuncCallExpr:
		f.funcCall(x)
	case *FuncCastExpr:
		x.SetDagger(DaggerHasFunc | x.Expr.GetDagger())
	case *IsNullExpr:
		x.SetDagger(x.Expr.GetDagger())
	case *IsTruthExpr:
		x.SetDagger(x.Expr.GetDagger())
	case *ParenthesesExpr:
		x.SetDagger(x.Expr.GetDagger())
	case *PatternInExpr:
		f.patternIn(x)
	case *PatternLikeExpr:
		f.patternLike(x)
	case *PatternRegexpExpr:
		f.patternRegexp(x)
	case *PositionExpr:
		x.SetDagger(DaggerHasReference)
	case *RowExpr:
		f.row(x)
	case *SubqueryExpr:
		x.SetDagger(DaggerHasSubquery)
	case *UnaryOperationExpr:
		x.SetDagger(x.V.GetDagger())
	case *ValuesExpr:
		x.SetDagger(DaggerHasReference)
	case *VariableExpr:
		if x.Value == nil {
			x.SetDagger(DaggerHasVariable)
		} else {
			x.SetDagger(DaggerHasVariable | x.Value.GetDagger())
		}
	}

	return in, true
}
