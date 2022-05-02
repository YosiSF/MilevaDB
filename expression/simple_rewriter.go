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

package expression

import (
	"context"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/opcode"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	driver "github.com/whtcorpsinc/MilevaDB-Prod/types/berolinaAllegroSQL_driver"
)

type simpleRewriter struct {
	exprStack

	schemaReplicant *Schema
	err             error
	ctx             stochastikctx.Context
	names           []*types.FieldName
}

// ParseSimpleExprWithBlockInfo parses simple expression string to Expression.
// The expression string must only reference the defCausumn in block Info.
func ParseSimpleExprWithBlockInfo(ctx stochastikctx.Context, exprStr string, blockInfo *perceptron.BlockInfo) (Expression, error) {
	exprStr = "select " + exprStr
	var stmts []ast.StmtNode
	var err error
	var warns []error
	if p, ok := ctx.(interface {
		ParseALLEGROSQL(context.Context, string, string, string) ([]ast.StmtNode, []error, error)
	}); ok {
		stmts, warns, err = p.ParseALLEGROSQL(context.Background(), exprStr, "", "")
	} else {
		stmts, warns, err = berolinaAllegroSQL.New().Parse(exprStr, "", "")
	}
	for _, warn := range warns {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(soliton.SyntaxWarn(warn))
	}

	if err != nil {
		return nil, errors.Trace(err)
	}
	expr := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	return RewriteSimpleExprWithBlockInfo(ctx, blockInfo, expr)
}

// ParseSimpleExprCastWithBlockInfo parses simple expression string to Expression.
// And the expr returns will cast to the target type.
func ParseSimpleExprCastWithBlockInfo(ctx stochastikctx.Context, exprStr string, blockInfo *perceptron.BlockInfo, targetFt *types.FieldType) (Expression, error) {
	e, err := ParseSimpleExprWithBlockInfo(ctx, exprStr, blockInfo)
	if err != nil {
		return nil, err
	}
	e = BuildCastFunction(ctx, e, targetFt)
	return e, nil
}

// RewriteSimpleExprWithBlockInfo rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithBlockInfo(ctx stochastikctx.Context, tbl *perceptron.BlockInfo, expr ast.ExprNode) (Expression, error) {
	dbName := perceptron.NewCIStr(ctx.GetStochaseinstein_dbars().CurrentDB)
	defCausumns, names, err := DeferredCausetInfos2DeferredCausetsAndNames(ctx, dbName, tbl.Name, tbl.DefCauss(), tbl)
	if err != nil {
		return nil, err
	}
	rewriter := &simpleRewriter{ctx: ctx, schemaReplicant: NewSchema(defCausumns...), names: names}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		return nil, rewriter.err
	}
	return rewriter.pop(), nil
}

// ParseSimpleExprsWithSchema parses simple expression string to Expression.
// The expression string must only reference the defCausumn in the given schemaReplicant.
func ParseSimpleExprsWithSchema(ctx stochastikctx.Context, exprStr string, schemaReplicant *Schema) ([]Expression, error) {
	exprStr = "select " + exprStr
	stmts, warns, err := berolinaAllegroSQL.New().Parse(exprStr, "", "")
	if err != nil {
		return nil, soliton.SyntaxWarn(err)
	}
	for _, warn := range warns {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(soliton.SyntaxWarn(warn))
	}

	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	exprs := make([]Expression, 0, len(fields))
	for _, field := range fields {
		expr, err := RewriteSimpleExprWithSchema(ctx, field.Expr, schemaReplicant)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}

// ParseSimpleExprsWithNames parses simple expression string to Expression.
// The expression string must only reference the defCausumn in the given NameSlice.
func ParseSimpleExprsWithNames(ctx stochastikctx.Context, exprStr string, schemaReplicant *Schema, names types.NameSlice) ([]Expression, error) {
	exprStr = "select " + exprStr
	var stmts []ast.StmtNode
	var err error
	var warns []error
	if p, ok := ctx.(interface {
		ParseALLEGROSQL(context.Context, string, string, string) ([]ast.StmtNode, []error, error)
	}); ok {
		stmts, warns, err = p.ParseALLEGROSQL(context.Background(), exprStr, "", "")
	} else {
		stmts, warns, err = berolinaAllegroSQL.New().Parse(exprStr, "", "")
	}
	if err != nil {
		return nil, soliton.SyntaxWarn(err)
	}
	for _, warn := range warns {
		ctx.GetStochaseinstein_dbars().StmtCtx.AppendWarning(soliton.SyntaxWarn(warn))
	}

	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	exprs := make([]Expression, 0, len(fields))
	for _, field := range fields {
		expr, err := RewriteSimpleExprWithNames(ctx, field.Expr, schemaReplicant, names)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}

// RewriteSimpleExprWithNames rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithNames(ctx stochastikctx.Context, expr ast.ExprNode, schemaReplicant *Schema, names []*types.FieldName) (Expression, error) {
	rewriter := &simpleRewriter{ctx: ctx, schemaReplicant: schemaReplicant, names: names}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		return nil, rewriter.err
	}
	return rewriter.pop(), nil
}

// RewriteSimpleExprWithSchema rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithSchema(ctx stochastikctx.Context, expr ast.ExprNode, schemaReplicant *Schema) (Expression, error) {
	rewriter := &simpleRewriter{ctx: ctx, schemaReplicant: schemaReplicant}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		return nil, rewriter.err
	}
	return rewriter.pop(), nil
}

// FindFieldName finds the defCausumn name from NameSlice.
func FindFieldName(names types.NameSlice, astDefCaus *ast.DeferredCausetName) (int, error) {
	dbName, tblName, defCausName := astDefCaus.Schema, astDefCaus.Block, astDefCaus.Name
	idx := -1
	for i, name := range names {
		if (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			(defCausName.L == name.DefCausName.L) {
			if idx == -1 {
				idx = i
			} else {
				return -1, errNonUniq.GenWithStackByArgs(astDefCaus.String(), "field list")
			}
		}
	}
	return idx, nil
}

// FindFieldNameIdxByDefCausName finds the index of corresponding name in the given slice. -1 for not found.
func FindFieldNameIdxByDefCausName(names []*types.FieldName, defCausName string) int {
	for i, name := range names {
		if name.DefCausName.L == defCausName {
			return i
		}
	}
	return -1
}

func (sr *simpleRewriter) rewriteDeferredCauset(nodeDefCausName *ast.DeferredCausetNameExpr) (*DeferredCauset, error) {
	idx, err := FindFieldName(sr.names, nodeDefCausName.Name)
	if idx >= 0 && err == nil {
		return sr.schemaReplicant.DeferredCausets[idx], nil
	}
	return nil, errBadField.GenWithStackByArgs(nodeDefCausName.Name.Name.O, "expression")
}

func (sr *simpleRewriter) Enter(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

func (sr *simpleRewriter) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	switch v := originInNode.(type) {
	case *ast.DeferredCausetNameExpr:
		defCausumn, err := sr.rewriteDeferredCauset(v)
		if err != nil {
			sr.err = err
			return originInNode, false
		}
		sr.push(defCausumn)
	case *driver.ValueExpr:
		value := &CouplingConstantWithRadix{Value: v.Causet, RetType: &v.Type}
		sr.push(value)
	case *ast.FuncCallExpr:
		sr.funcCallToExpression(v)
	case *ast.FuncCastExpr:
		arg := sr.pop()
		sr.err = CheckArgsNotMultiDeferredCausetEvent(arg)
		if sr.err != nil {
			return retNode, false
		}
		sr.push(BuildCastFunction(sr.ctx, arg, v.Tp))
	case *ast.BinaryOperationExpr:
		sr.binaryOpToExpression(v)
	case *ast.UnaryOperationExpr:
		sr.unaryOpToExpression(v)
	case *ast.BetweenExpr:
		sr.betweenToExpression(v)
	case *ast.IsNullExpr:
		sr.isNullToExpression(v)
	case *ast.IsTruthExpr:
		sr.isTrueToScalarFunc(v)
	case *ast.PatternLikeExpr:
		sr.likeToScalarFunc(v)
	case *ast.PatternRegexpExpr:
		sr.regexpToScalarFunc(v)
	case *ast.PatternInExpr:
		if v.Sel == nil {
			sr.inToExpression(len(v.List), v.Not, &v.Type)
		}
	case *driver.ParamMarkerExpr:
		var value Expression
		value, sr.err = ParamMarkerExpression(sr.ctx, v)
		if sr.err != nil {
			return retNode, false
		}
		sr.push(value)
	case *ast.EventExpr:
		sr.rowToScalarFunc(v)
	case *ast.ParenthesesExpr:
	case *ast.DeferredCausetName:
	// TODO: Perhaps we don't need to transcode these back to generic integers/strings
	case *ast.TrimDirectionExpr:
		sr.push(&CouplingConstantWithRadix{
			Value:   types.NewIntCauset(int64(v.Direction)),
			RetType: types.NewFieldType(allegrosql.TypeTiny),
		})
	case *ast.TimeUnitExpr:
		sr.push(&CouplingConstantWithRadix{
			Value:   types.NewStringCauset(v.Unit.String()),
			RetType: types.NewFieldType(allegrosql.TypeVarchar),
		})
	case *ast.GetFormatSelectorExpr:
		sr.push(&CouplingConstantWithRadix{
			Value:   types.NewStringCauset(v.Selector.String()),
			RetType: types.NewFieldType(allegrosql.TypeVarchar),
		})
	case *ast.SetDefCauslationExpr:
		arg := sr.stack[len(sr.stack)-1]
		if defCauslate.NewDefCauslationEnabled() {
			var defCauslInfo *charset.DefCauslation
			// TODO(bb7133): use charset.ValidCharsetAndDefCauslation when its bug is fixed.
			if defCauslInfo, sr.err = defCauslate.GetDefCauslationByName(v.DefCauslate); sr.err != nil {
				break
			}
			chs := arg.GetType().Charset
			if chs != "" && defCauslInfo.CharsetName != chs {
				sr.err = charset.ErrDefCauslationCharsetMismatch.GenWithStackByArgs(defCauslInfo.Name, chs)
				break
			}
		}
		// SetDefCauslationExpr sets the defCauslation explicitly, even when the evaluation type of the expression is non-string.
		if _, ok := arg.(*DeferredCauset); ok {
			// Wrap a cast here to avoid changing the original FieldType of the defCausumn expression.
			exprType := arg.GetType().Clone()
			exprType.DefCauslate = v.DefCauslate
			casted := BuildCastFunction(sr.ctx, arg, exprType)
			sr.pop()
			sr.push(casted)
		} else {
			// For constant and scalar function, we can set its defCauslate directly.
			arg.GetType().DefCauslate = v.DefCauslate
		}
		sr.stack[len(sr.stack)-1].SetCoercibility(CoercibilityExplicit)
		sr.stack[len(sr.stack)-1].SetCharsetAndDefCauslation(arg.GetType().Charset, arg.GetType().DefCauslate)
	default:
		sr.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}
	if sr.err != nil {
		return retNode, false
	}
	return originInNode, true
}

func (sr *simpleRewriter) useCache() bool {
	return sr.ctx.GetStochaseinstein_dbars().StmtCtx.UseCache
}

func (sr *simpleRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	right := sr.pop()
	left := sr.pop()
	var function Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		function, sr.err = sr.constructBinaryOpFunction(left, right,
			v.Op.String())
	default:
		lLen := GetEventLen(left)
		rLen := GetEventLen(right)
		if lLen != 1 || rLen != 1 {
			sr.err = ErrOperandDeferredCausets.GenWithStackByArgs(1)
			return
		}
		function, sr.err = NewFunction(sr.ctx, v.Op.String(), types.NewFieldType(allegrosql.TypeUnspecified), left, right)
	}
	if sr.err != nil {
		return
	}
	sr.push(function)
}

func (sr *simpleRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	args := sr.popN(len(v.Args))
	sr.err = CheckArgsNotMultiDeferredCausetEvent(args...)
	if sr.err != nil {
		return
	}
	if sr.rewriteFuncCall(v, args) {
		return
	}
	var function Expression
	function, sr.err = NewFunction(sr.ctx, v.FnName.L, &v.Type, args...)
	sr.push(function)
}

func (sr *simpleRewriter) rewriteFuncCall(v *ast.FuncCallExpr, args []Expression) bool {
	switch v.FnName.L {
	case ast.Nullif:
		if len(args) != 2 {
			sr.err = ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		param2 := args[1]
		param1 := args[0]
		// param1 = param2
		funcCompare, err := sr.constructBinaryOpFunction(param1, param2, ast.EQ)
		if err != nil {
			sr.err = err
			return true
		}
		// NULL
		nullTp := types.NewFieldType(allegrosql.TypeNull)
		nullTp.Flen, nullTp.Decimal = allegrosql.GetDefaultFieldLengthAndDecimal(allegrosql.TypeNull)
		paramNull := &CouplingConstantWithRadix{
			Value:   types.NewCauset(nil),
			RetType: nullTp,
		}
		// if(param1 = param2, NULL, param1)
		funcIf, err := NewFunction(sr.ctx, ast.If, &v.Type, funcCompare, paramNull, param1)
		if err != nil {
			sr.err = err
			return true
		}
		sr.push(funcIf)
		return true
	default:
		return false
	}
}

// constructBinaryOpFunction works as following:
// 1. If op are EQ or NE or NullEQ, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
// 2. If op are LE or GE, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( (a0 op b0) EQ 0, 0,
//      IF ( (a1 op b1) EQ 0, 0, a2 op b2))`
// 3. If op are LT or GT, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( a0 NE b0, a0 op b0,
//      IF( a1 NE b1,
//          a1 op b1,
//          a2 op b2)
// )`
func (sr *simpleRewriter) constructBinaryOpFunction(l Expression, r Expression, op string) (Expression, error) {
	lLen, rLen := GetEventLen(l), GetEventLen(r)
	if lLen == 1 && rLen == 1 {
		return NewFunction(sr.ctx, op, types.NewFieldType(allegrosql.TypeTiny), l, r)
	} else if rLen != lLen {
		return nil, ErrOperandDeferredCausets.GenWithStackByArgs(lLen)
	}
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		funcs := make([]Expression, lLen)
		for i := 0; i < lLen; i++ {
			var err error
			funcs[i], err = sr.constructBinaryOpFunction(GetFuncArg(l, i), GetFuncArg(r, i), op)
			if err != nil {
				return nil, err
			}
		}
		if op == ast.NE {
			return ComposeDNFCondition(sr.ctx, funcs...), nil
		}
		return ComposeCNFCondition(sr.ctx, funcs...), nil
	default:
		larg0, rarg0 := GetFuncArg(l, 0), GetFuncArg(r, 0)
		var expr1, expr2, expr3 Expression
		if op == ast.LE || op == ast.GE {
			expr1 = NewFunctionInternal(sr.ctx, op, types.NewFieldType(allegrosql.TypeTiny), larg0, rarg0)
			expr1 = NewFunctionInternal(sr.ctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), expr1, NewZero())
			expr2 = NewZero()
		} else if op == ast.LT || op == ast.GT {
			expr1 = NewFunctionInternal(sr.ctx, ast.NE, types.NewFieldType(allegrosql.TypeTiny), larg0, rarg0)
			expr2 = NewFunctionInternal(sr.ctx, op, types.NewFieldType(allegrosql.TypeTiny), larg0, rarg0)
		}
		var err error
		l, err = PopEventFirstArg(sr.ctx, l)
		if err != nil {
			return nil, err
		}
		r, err = PopEventFirstArg(sr.ctx, r)
		if err != nil {
			return nil, err
		}
		expr3, err = sr.constructBinaryOpFunction(l, r, op)
		if err != nil {
			return nil, err
		}
		return NewFunction(sr.ctx, ast.If, types.NewFieldType(allegrosql.TypeTiny), expr1, expr2, expr3)
	}
}

func (sr *simpleRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
	var op string
	switch v.Op {
	case opcode.Plus:
		// expression (+ a) is equal to a
		return
	case opcode.Minus:
		op = ast.UnaryMinus
	case opcode.BitNeg:
		op = ast.BitNeg
	case opcode.Not:
		op = ast.UnaryNot
	default:
		sr.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	expr := sr.pop()
	if GetEventLen(expr) != 1 {
		sr.err = ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return
	}
	newExpr, err := NewFunction(sr.ctx, op, &v.Type, expr)
	sr.err = err
	sr.push(newExpr)
}

func (sr *simpleRewriter) likeToScalarFunc(v *ast.PatternLikeExpr) {
	pattern := sr.pop()
	expr := sr.pop()
	sr.err = CheckArgsNotMultiDeferredCausetEvent(expr, pattern)
	if sr.err != nil {
		return
	}
	escapeTp := &types.FieldType{}
	char, defCaus := sr.ctx.GetStochaseinstein_dbars().GetCharsetInfo()
	types.DefaultTypeForValue(int(v.Escape), escapeTp, char, defCaus)
	function := sr.notToExpression(v.Not, ast.Like, &v.Type,
		expr, pattern, &CouplingConstantWithRadix{Value: types.NewIntCauset(int64(v.Escape)), RetType: escapeTp})
	sr.push(function)
}

func (sr *simpleRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	parttern := sr.pop()
	expr := sr.pop()
	sr.err = CheckArgsNotMultiDeferredCausetEvent(expr, parttern)
	if sr.err != nil {
		return
	}
	function := sr.notToExpression(v.Not, ast.Regexp, &v.Type, expr, parttern)
	sr.push(function)
}

func (sr *simpleRewriter) rowToScalarFunc(v *ast.EventExpr) {
	elems := sr.popN(len(v.Values))
	function, err := NewFunction(sr.ctx, ast.EventFunc, elems[0].GetType(), elems...)
	if err != nil {
		sr.err = err
		return
	}
	sr.push(function)
}

func (sr *simpleRewriter) betweenToExpression(v *ast.BetweenExpr) {
	right := sr.pop()
	left := sr.pop()
	expr := sr.pop()
	sr.err = CheckArgsNotMultiDeferredCausetEvent(expr)
	if sr.err != nil {
		return
	}
	var l, r Expression
	l, sr.err = NewFunction(sr.ctx, ast.GE, &v.Type, expr, left)
	if sr.err == nil {
		r, sr.err = NewFunction(sr.ctx, ast.LE, &v.Type, expr, right)
	}
	if sr.err != nil {
		return
	}
	function, err := NewFunction(sr.ctx, ast.LogicAnd, &v.Type, l, r)
	if err != nil {
		sr.err = err
		return
	}
	if v.Not {
		function, err = NewFunction(sr.ctx, ast.UnaryNot, &v.Type, function)
		if err != nil {
			sr.err = err
			return
		}
	}
	sr.push(function)
}

func (sr *simpleRewriter) isNullToExpression(v *ast.IsNullExpr) {
	arg := sr.pop()
	if GetEventLen(arg) != 1 {
		sr.err = ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return
	}
	function := sr.notToExpression(v.Not, ast.IsNull, &v.Type, arg)
	sr.push(function)
}

func (sr *simpleRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...Expression) Expression {
	opFunc, err := NewFunction(sr.ctx, op, tp, args...)
	if err != nil {
		sr.err = err
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = NewFunction(sr.ctx, ast.UnaryNot, tp, opFunc)
	if err != nil {
		sr.err = err
		return nil
	}
	return opFunc
}

func (sr *simpleRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	arg := sr.pop()
	op := ast.IsTruthWithoutNull
	if v.True == 0 {
		op = ast.IsFalsity
	}
	if GetEventLen(arg) != 1 {
		sr.err = ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return
	}
	function := sr.notToExpression(v.Not, op, &v.Type, arg)
	sr.push(function)
}

// inToExpression converts in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (sr *simpleRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	exprs := sr.popN(lLen + 1)
	leftExpr := exprs[0]
	elems := exprs[1:]
	l, leftFt := GetEventLen(leftExpr), leftExpr.GetType()
	for i := 0; i < lLen; i++ {
		if l != GetEventLen(elems[i]) {
			sr.err = ErrOperandDeferredCausets.GenWithStackByArgs(l)
			return
		}
	}
	leftIsNull := leftFt.Tp == allegrosql.TypeNull
	if leftIsNull {
		sr.push(NewNull())
		return
	}
	leftEt := leftFt.EvalType()

	if leftEt == types.CausetEDN {
		for i := 0; i < len(elems); i++ {
			if c, ok := elems[i].(*CouplingConstantWithRadix); ok {
				var isExceptional bool
				elems[i], isExceptional = RefineComparedCouplingConstantWithRadix(sr.ctx, *leftFt, c, opcode.EQ)
				if isExceptional {
					elems[i] = c
				}
			}
		}
	}
	allSameType := true
	for _, elem := range elems {
		if elem.GetType().Tp != allegrosql.TypeNull && GetAccurateCmpType(leftExpr, elem) != leftEt {
			allSameType = false
			break
		}
	}
	var function Expression
	if allSameType && l == 1 {
		function = sr.notToExpression(not, ast.In, tp, exprs...)
	} else {
		eqFunctions := make([]Expression, 0, lLen)
		for i := 0; i < len(elems); i++ {
			expr, err := sr.constructBinaryOpFunction(leftExpr, elems[i], ast.EQ)
			if err != nil {
				sr.err = err
				return
			}
			eqFunctions = append(eqFunctions, expr)
		}
		function = ComposeDNFCondition(sr.ctx, eqFunctions...)
		if not {
			var err error
			function, err = NewFunction(sr.ctx, ast.UnaryNot, tp, function)
			if err != nil {
				sr.err = err
				return
			}
		}
	}
	sr.push(function)
}
