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

package core

import (
	"context"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression/aggregation"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/collate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/hint"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/stringutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	driver "github.com/whtcorpsinc/MilevaDB-Prod/types/berolinaAllegroSQL_driver"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/opcode"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
)

// EvalSubqueryFirstRow evaluates incorrelated subqueries once, and get first event.
var EvalSubqueryFirstRow func(ctx context.Context, p PhysicalPlan, is schemareplicant.SchemaReplicant, sctx stochastikctx.Context) (event []types.Causet, err error)

// evalAstExpr evaluates ast expression directly.
func evalAstExpr(sctx stochastikctx.Context, expr ast.ExprNode) (types.Causet, error) {
	if val, ok := expr.(*driver.ValueExpr); ok {
		return val.Causet, nil
	}
	newExpr, err := rewriteAstExpr(sctx, expr, nil, nil)
	if err != nil {
		return types.Causet{}, err
	}
	return newExpr.Eval(chunk.Row{})
}

// rewriteAstExpr rewrites ast expression directly.
func rewriteAstExpr(sctx stochastikctx.Context, expr ast.ExprNode, schemaReplicant *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	var is schemareplicant.SchemaReplicant
	if sctx.GetStochaseinstein_dbars().TxnCtx.SchemaReplicant != nil {
		is = sctx.GetStochaseinstein_dbars().TxnCtx.SchemaReplicant.(schemareplicant.SchemaReplicant)
	}
	b := NewPlanBuilder(sctx, is, &hint.BlockHintProcessor{})
	fakePlan := LogicalBlockDual{}.Init(sctx, 0)
	if schemaReplicant != nil {
		fakePlan.schemaReplicant = schemaReplicant
		fakePlan.names = names
	}
	newExpr, _, err := b.rewrite(context.TODO(), expr, fakePlan, nil, true)
	if err != nil {
		return nil, err
	}
	return newExpr, nil
}

func (b *PlanBuilder) rewriteInsertOnDuplicateUFIDelate(ctx context.Context, exprNode ast.ExprNode, mockPlan LogicalPlan, insertPlan *Insert) (expression.Expression, error) {
	b.rewriterCounter++
	defer func() { b.rewriterCounter-- }()

	rewriter := b.getExpressionRewriter(ctx, mockPlan)
	// The rewriter maybe is obtained from "b.rewriterPool", "rewriter.err" is
	// not nil means certain previous procedure has not handled this error.
	// Here we give us one more chance to make a correct behavior by handling
	// this missed error.
	if rewriter.err != nil {
		return nil, rewriter.err
	}

	rewriter.insertPlan = insertPlan
	rewriter.asScalar = true

	expr, _, err := b.rewriteExprNode(rewriter, exprNode, true)
	return expr, err
}

// rewrite function rewrites ast expr to expression.Expression.
// aggMapper maps ast.AggregateFuncExpr to the columns offset in p's output schemaReplicant.
// asScalar means whether this expression must be treated as a scalar expression.
// And this function returns a result expression, a new plan that may have apply or semi-join.
func (b *PlanBuilder) rewrite(ctx context.Context, exprNode ast.ExprNode, p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int, asScalar bool) (expression.Expression, LogicalPlan, error) {
	expr, resultPlan, err := b.rewriteWithPreprocess(ctx, exprNode, p, aggMapper, nil, asScalar, nil)
	return expr, resultPlan, err
}

// rewriteWithPreprocess is for handling the situation that we need to adjust the input ast tree
// before really using its node in `expressionRewriter.Leave`. In that case, we first call
// er.preprocess(expr), which returns a new expr. Then we use the new expr in `Leave`.
func (b *PlanBuilder) rewriteWithPreprocess(
	ctx context.Context,
	exprNode ast.ExprNode,
	p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int,
	windowMapper map[*ast.WindowFuncExpr]int,
	asScalar bool,
	preprocess func(ast.Node) ast.Node,
) (expression.Expression, LogicalPlan, error) {
	b.rewriterCounter++
	defer func() { b.rewriterCounter-- }()

	rewriter := b.getExpressionRewriter(ctx, p)
	// The rewriter maybe is obtained from "b.rewriterPool", "rewriter.err" is
	// not nil means certain previous procedure has not handled this error.
	// Here we give us one more chance to make a correct behavior by handling
	// this missed error.
	if rewriter.err != nil {
		return nil, nil, rewriter.err
	}

	rewriter.aggrMap = aggMapper
	rewriter.windowMap = windowMapper
	rewriter.asScalar = asScalar
	rewriter.preprocess = preprocess

	expr, resultPlan, err := b.rewriteExprNode(rewriter, exprNode, asScalar)
	return expr, resultPlan, err
}

func (b *PlanBuilder) getExpressionRewriter(ctx context.Context, p LogicalPlan) (rewriter *expressionRewriter) {
	defer func() {
		if p != nil {
			rewriter.schemaReplicant = p.Schema()
			rewriter.names = p.OutputNames()
		}
	}()

	if len(b.rewriterPool) < b.rewriterCounter {
		rewriter = &expressionRewriter{p: p, b: b, sctx: b.ctx, ctx: ctx}
		b.rewriterPool = append(b.rewriterPool, rewriter)
		return
	}

	rewriter = b.rewriterPool[b.rewriterCounter-1]
	rewriter.p = p
	rewriter.asScalar = false
	rewriter.aggrMap = nil
	rewriter.preprocess = nil
	rewriter.insertPlan = nil
	rewriter.disableFoldCounter = 0
	rewriter.tryFoldCounter = 0
	rewriter.ctxStack = rewriter.ctxStack[:0]
	rewriter.ctxNameStk = rewriter.ctxNameStk[:0]
	rewriter.ctx = ctx
	return
}

func (b *PlanBuilder) rewriteExprNode(rewriter *expressionRewriter, exprNode ast.ExprNode, asScalar bool) (expression.Expression, LogicalPlan, error) {
	if rewriter.p != nil {
		curDefCausLen := rewriter.p.Schema().Len()
		defer func() {
			names := rewriter.p.OutputNames().Shallow()[:curDefCausLen]
			for i := curDefCausLen; i < rewriter.p.Schema().Len(); i++ {
				names = append(names, types.EmptyName)
			}
			// After rewriting finished, only old columns are visible.
			// e.g. select * from t where t.a in (select t1.a from t1);
			// The output columns before we enter the subquery are the columns from t.
			// But when we leave the subquery `t.a in (select t1.a from t1)`, we got a Apply operator
			// and the output columns become [t.*, t1.*]. But t1.* is used only inside the subquery. If there's another filter
			// which is also a subquery where t1 is involved. The name resolving will fail if we still expose the column from
			// the previous subquery.
			// So here we just reset the names to empty to avoid this situation.
			// TODO: implement ScalarSubQuery and resolve it during optimizing. In building phase, we will not change the plan's structure.
			rewriter.p.SetOutputNames(names)
		}()
	}
	exprNode.Accept(rewriter)
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}
	if !asScalar && len(rewriter.ctxStack) == 0 {
		return nil, rewriter.p, nil
	}
	if len(rewriter.ctxStack) != 1 {
		return nil, nil, errors.Errorf("context len %v is invalid", len(rewriter.ctxStack))
	}
	rewriter.err = expression.CheckArgsNotMultiDeferredCausetRow(rewriter.ctxStack[0])
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}
	return rewriter.ctxStack[0], rewriter.p, nil
}

type expressionRewriter struct {
	ctxStack        []expression.Expression
	ctxNameStk      []*types.FieldName
	p               LogicalPlan
	schemaReplicant *expression.Schema
	names           []*types.FieldName
	err             error
	aggrMap         map[*ast.AggregateFuncExpr]int
	windowMap       map[*ast.WindowFuncExpr]int
	b               *PlanBuilder
	sctx            stochastikctx.Context
	ctx             context.Context

	// asScalar indicates the return value must be a scalar value.
	// NOTE: This value can be changed during expression rewritten.
	asScalar bool

	// preprocess is called for every ast.Node in Leave.
	preprocess func(ast.Node) ast.Node

	// insertPlan is only used to rewrite the expressions inside the assignment
	// of the "INSERT" statement.
	insertPlan *Insert

	// disableFoldCounter controls fold-disabled sINTERLOCKe. If > 0, rewriter will NOT do constant folding.
	// Typically, during visiting AST, while entering the sINTERLOCKe(disable), the counter will +1; while
	// leaving the sINTERLOCKe(enable again), the counter will -1.
	// NOTE: This value can be changed during expression rewritten.
	disableFoldCounter int
	tryFoldCounter     int
}

func (er *expressionRewriter) ctxStackLen() int {
	return len(er.ctxStack)
}

func (er *expressionRewriter) ctxStackPop(num int) {
	l := er.ctxStackLen()
	er.ctxStack = er.ctxStack[:l-num]
	er.ctxNameStk = er.ctxNameStk[:l-num]
}

func (er *expressionRewriter) ctxStackAppend(col expression.Expression, name *types.FieldName) {
	er.ctxStack = append(er.ctxStack, col)
	er.ctxNameStk = append(er.ctxNameStk, name)
}

// constructBinaryOpFunction converts binary operator functions
// 1. If op are EQ or NE or NullEQ, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
// 2. Else constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( a0 NE b0, a0 op b0,
// 		IF ( isNull(a0 NE b0), Null,
// 			IF ( a1 NE b1, a1 op b1,
// 				IF ( isNull(a1 NE b1), Null, a2 op b2))))`
func (er *expressionRewriter) constructBinaryOpFunction(l expression.Expression, r expression.Expression, op string) (expression.Expression, error) {
	lLen, rLen := expression.GetRowLen(l), expression.GetRowLen(r)
	if lLen == 1 && rLen == 1 {
		return er.newFunction(op, types.NewFieldType(allegrosql.TypeTiny), l, r)
	} else if rLen != lLen {
		return nil, expression.ErrOperandDeferredCausets.GenWithStackByArgs(lLen)
	}
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		funcs := make([]expression.Expression, lLen)
		for i := 0; i < lLen; i++ {
			var err error
			funcs[i], err = er.constructBinaryOpFunction(expression.GetFuncArg(l, i), expression.GetFuncArg(r, i), op)
			if err != nil {
				return nil, err
			}
		}
		if op == ast.NE {
			return expression.ComposeDNFCondition(er.sctx, funcs...), nil
		}
		return expression.ComposeCNFCondition(er.sctx, funcs...), nil
	default:
		larg0, rarg0 := expression.GetFuncArg(l, 0), expression.GetFuncArg(r, 0)
		var expr1, expr2, expr3, expr4, expr5 expression.Expression
		expr1 = expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(allegrosql.TypeTiny), larg0, rarg0)
		expr2 = expression.NewFunctionInternal(er.sctx, op, types.NewFieldType(allegrosql.TypeTiny), larg0, rarg0)
		expr3 = expression.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(allegrosql.TypeTiny), expr1)
		var err error
		l, err = expression.PopRowFirstArg(er.sctx, l)
		if err != nil {
			return nil, err
		}
		r, err = expression.PopRowFirstArg(er.sctx, r)
		if err != nil {
			return nil, err
		}
		expr4, err = er.constructBinaryOpFunction(l, r, op)
		if err != nil {
			return nil, err
		}
		expr5, err = er.newFunction(ast.If, types.NewFieldType(allegrosql.TypeTiny), expr3, expression.NewNull(), expr4)
		if err != nil {
			return nil, err
		}
		return er.newFunction(ast.If, types.NewFieldType(allegrosql.TypeTiny), expr1, expr2, expr5)
	}
}

func (er *expressionRewriter) buildSubquery(ctx context.Context, subq *ast.SubqueryExpr) (LogicalPlan, error) {
	if er.schemaReplicant != nil {
		outerSchema := er.schemaReplicant.Clone()
		er.b.outerSchemas = append(er.b.outerSchemas, outerSchema)
		er.b.outerNames = append(er.b.outerNames, er.names)
		defer func() {
			er.b.outerSchemas = er.b.outerSchemas[0 : len(er.b.outerSchemas)-1]
			er.b.outerNames = er.b.outerNames[0 : len(er.b.outerNames)-1]
		}()
	}

	np, err := er.b.buildResultSetNode(ctx, subq.Query)
	if err != nil {
		return nil, err
	}
	// Pop the handle map generated by the subquery.
	er.b.handleHelper.popMap()
	return np, nil
}

// Enter implements Visitor interface.
func (er *expressionRewriter) Enter(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
		index, ok := -1, false
		if er.aggrMap != nil {
			index, ok = er.aggrMap[v]
		}
		if !ok {
			er.err = ErrInvalidGroupFuncUse
			return inNode, true
		}
		er.ctxStackAppend(er.schemaReplicant.DeferredCausets[index], er.names[index])
		return inNode, true
	case *ast.DeferredCausetNameExpr:
		if index, ok := er.b.colMapper[v]; ok {
			er.ctxStackAppend(er.schemaReplicant.DeferredCausets[index], er.names[index])
			return inNode, true
		}
	case *ast.CompareSubqueryExpr:
		return er.handleCompareSubquery(er.ctx, v)
	case *ast.ExistsSubqueryExpr:
		return er.handleExistSubquery(er.ctx, v)
	case *ast.PatternInExpr:
		if v.Sel != nil {
			return er.handleInSubquery(er.ctx, v)
		}
		if len(v.List) != 1 {
			break
		}
		// For 10 in ((select * from t)), the berolinaAllegroSQL won't set v.Sel.
		// So we must process this case here.
		x := v.List[0]
		for {
			switch y := x.(type) {
			case *ast.SubqueryExpr:
				v.Sel = y
				return er.handleInSubquery(er.ctx, v)
			case *ast.ParenthesesExpr:
				x = y.Expr
			default:
				return inNode, false
			}
		}
	case *ast.SubqueryExpr:
		return er.handleScalarSubquery(er.ctx, v)
	case *ast.ParenthesesExpr:
	case *ast.ValuesExpr:
		schemaReplicant, names := er.schemaReplicant, er.names
		// NOTE: "er.insertPlan != nil" means that we are rewriting the
		// expressions inside the assignment of "INSERT" statement. we have to
		// use the "blockSchema" of that "insertPlan".
		if er.insertPlan != nil {
			schemaReplicant = er.insertPlan.blockSchema
			names = er.insertPlan.blockDefCausNames
		}
		idx, err := expression.FindFieldName(names, v.DeferredCauset.Name)
		if err != nil {
			er.err = err
			return inNode, false
		}
		if idx < 0 {
			er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.DeferredCauset.Name.OrigDefCausName(), "field list")
			return inNode, false
		}
		col := schemaReplicant.DeferredCausets[idx]
		er.ctxStackAppend(expression.NewValuesFunc(er.sctx, col.Index, col.RetType), types.EmptyName)
		return inNode, true
	case *ast.WindowFuncExpr:
		index, ok := -1, false
		if er.windowMap != nil {
			index, ok = er.windowMap[v]
		}
		if !ok {
			er.err = ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(strings.ToLower(v.F))
			return inNode, true
		}
		er.ctxStackAppend(er.schemaReplicant.DeferredCausets[index], er.names[index])
		return inNode, true
	case *ast.FuncCallExpr:
		if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
			er.disableFoldCounter++
		}
		if _, ok := expression.TryFoldFunctions[v.FnName.L]; ok {
			er.tryFoldCounter++
		}
	case *ast.CaseExpr:
		if _, ok := expression.DisableFoldFunctions["case"]; ok {
			er.disableFoldCounter++
		}
		if _, ok := expression.TryFoldFunctions["case"]; ok {
			er.tryFoldCounter++
		}
	case *ast.SetDefCauslationExpr:
		// Do nothing
	default:
		er.asScalar = true
	}
	return inNode, false
}

func (er *expressionRewriter) buildSemiApplyFromEqualSubq(np LogicalPlan, l, r expression.Expression, not bool) {
	var condition expression.Expression
	if rDefCaus, ok := r.(*expression.DeferredCauset); ok && (er.asScalar || not) {
		// If both input columns of `!= all / = any` expression are not null, we can treat the expression
		// as normal column equal condition.
		if lDefCaus, ok := l.(*expression.DeferredCauset); !ok || !allegrosql.HasNotNullFlag(lDefCaus.GetType().Flag) || !allegrosql.HasNotNullFlag(rDefCaus.GetType().Flag) {
			rDefCausINTERLOCKy := *rDefCaus
			rDefCausINTERLOCKy.InOperand = true
			r = &rDefCausINTERLOCKy
		}
	}
	condition, er.err = er.constructBinaryOpFunction(l, r, ast.EQ)
	if er.err != nil {
		return
	}
	er.p, er.err = er.b.buildSemiApply(er.p, np, []expression.Expression{condition}, er.asScalar, not)
}

func (er *expressionRewriter) handleCompareSubquery(ctx context.Context, v *ast.CompareSubqueryExpr) (ast.Node, bool) {
	v.L.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.R.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T.", v.R)
		return v, true
	}
	np, err := er.buildSubquery(ctx, subq)
	if err != nil {
		er.err = err
		return v, true
	}
	// Only (a,b,c) = any (...) and (a,b,c) != all (...) can use event expression.
	canMultiDefCaus := (!v.All && v.Op == opcode.EQ) || (v.All && v.Op == opcode.NE)
	if !canMultiDefCaus && (expression.GetRowLen(lexpr) != 1 || np.Schema().Len() != 1) {
		er.err = expression.ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return v, true
	}
	lLen := expression.GetRowLen(lexpr)
	if lLen != np.Schema().Len() {
		er.err = expression.ErrOperandDeferredCausets.GenWithStackByArgs(lLen)
		return v, true
	}
	var rexpr expression.Expression
	if np.Schema().Len() == 1 {
		rexpr = np.Schema().DeferredCausets[0]
	} else {
		args := make([]expression.Expression, 0, np.Schema().Len())
		for _, col := range np.Schema().DeferredCausets {
			args = append(args, col)
		}
		rexpr, er.err = er.newFunction(ast.RowFunc, args[0].GetType(), args...)
		if er.err != nil {
			return v, true
		}
	}
	switch v.Op {
	// Only EQ, NE and NullEQ can be composed with and.
	case opcode.EQ, opcode.NE, opcode.NullEQ:
		if v.Op == opcode.EQ {
			if v.All {
				er.handleEQAll(lexpr, rexpr, np)
			} else {
				// `a = any(subq)` will be rewriten as `a in (subq)`.
				er.buildSemiApplyFromEqualSubq(np, lexpr, rexpr, false)
				if er.err != nil {
					return v, true
				}
			}
		} else if v.Op == opcode.NE {
			if v.All {
				// `a != all(subq)` will be rewriten as `a not in (subq)`.
				er.buildSemiApplyFromEqualSubq(np, lexpr, rexpr, true)
				if er.err != nil {
					return v, true
				}
			} else {
				er.handleNEAny(lexpr, rexpr, np)
			}
		} else {
			// TODO: Support this in future.
			er.err = errors.New("We don't support <=> all or <=> any now")
			return v, true
		}
	default:
		// When < all or > any , the agg function should use min.
		useMin := ((v.Op == opcode.LT || v.Op == opcode.LE) && v.All) || ((v.Op == opcode.GT || v.Op == opcode.GE) && !v.All)
		er.handleOtherComparableSubq(lexpr, rexpr, np, useMin, v.Op.String(), v.All)
	}
	if er.asScalar {
		// The parent expression only use the last column in schemaReplicant, which represents whether the condition is matched.
		er.ctxStack[len(er.ctxStack)-1] = er.p.Schema().DeferredCausets[er.p.Schema().Len()-1]
		er.ctxNameStk[len(er.ctxNameStk)-1] = er.p.OutputNames()[er.p.Schema().Len()-1]
	}
	return v, true
}

// handleOtherComparableSubq handles the queries like < any, < max, etc. For example, if the query is t.id < any (select s.id from s),
// it will be rewrote to t.id < (select max(s.id) from s).
func (er *expressionRewriter) handleOtherComparableSubq(lexpr, rexpr expression.Expression, np LogicalPlan, useMin bool, cmpFunc string, all bool) {
	plan4Agg := LogicalAggregation{}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.BlockHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)

	// Create a "max" or "min" aggregation.
	funcName := ast.AggFuncMax
	if useMin {
		funcName = ast.AggFuncMin
	}
	funcMaxOrMin, err := aggregation.NewAggFuncDesc(er.sctx, funcName, []expression.Expression{rexpr}, false)
	if err != nil {
		er.err = err
		return
	}

	// Create a column and append it to the schemaReplicant of that aggregation.
	colMaxOrMin := &expression.DeferredCauset{
		UniqueID: er.sctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
		RetType:  funcMaxOrMin.RetTp,
	}
	schemaReplicant := expression.NewSchema(colMaxOrMin)

	plan4Agg.names = append(plan4Agg.names, types.EmptyName)
	plan4Agg.SetSchema(schemaReplicant)
	plan4Agg.AggFuncs = []*aggregation.AggFuncDesc{funcMaxOrMin}

	cond := expression.NewFunctionInternal(er.sctx, cmpFunc, types.NewFieldType(allegrosql.TypeTiny), lexpr, colMaxOrMin)
	er.buildQuantifierPlan(plan4Agg, cond, lexpr, rexpr, all)
}

// buildQuantifierPlan adds extra condition for any / all subquery.
func (er *expressionRewriter) buildQuantifierPlan(plan4Agg *LogicalAggregation, cond, lexpr, rexpr expression.Expression, all bool) {
	innerIsNull := expression.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(allegrosql.TypeTiny), rexpr)
	outerIsNull := expression.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(allegrosql.TypeTiny), lexpr)

	funcSum, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncSum, []expression.Expression{innerIsNull}, false)
	if err != nil {
		er.err = err
		return
	}
	colSum := &expression.DeferredCauset{
		UniqueID: er.sctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
		RetType:  funcSum.RetTp,
	}
	plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcSum)
	plan4Agg.schemaReplicant.Append(colSum)
	innerHasNull := expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(allegrosql.TypeTiny), colSum, expression.NewZero())

	// Build `count(1)` aggregation to check if subquery is empty.
	funcCount, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	if err != nil {
		er.err = err
		return
	}
	colCount := &expression.DeferredCauset{
		UniqueID: er.sctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
		RetType:  funcCount.RetTp,
	}
	plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcCount)
	plan4Agg.schemaReplicant.Append(colCount)

	if all {
		// All of the inner record set should not contain null value. So for t.id < all(select s.id from s), it
		// should be rewrote to t.id < min(s.id) and if(sum(s.id is null) != 0, null, true).
		innerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(allegrosql.TypeTiny), innerHasNull, expression.NewNull(), expression.NewOne())
		cond = expression.ComposeCNFCondition(er.sctx, cond, innerNullChecker)
		// If the subquery is empty, it should always return true.
		emptyChecker := expression.NewFunctionInternal(er.sctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), colCount, expression.NewZero())
		// If outer key is null, and subquery is not empty, it should always return null, even when it is `null = all (1, 2)`.
		outerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(allegrosql.TypeTiny), outerIsNull, expression.NewNull(), expression.NewZero())
		cond = expression.ComposeDNFCondition(er.sctx, cond, emptyChecker, outerNullChecker)
	} else {
		// For "any" expression, if the subquery has null and the cond returns false, the result should be NULL.
		// Specifically, `t.id < any (select s.id from s)` would be rewrote to `t.id < max(s.id) or if(sum(s.id is null) != 0, null, false)`
		innerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(allegrosql.TypeTiny), innerHasNull, expression.NewNull(), expression.NewZero())
		cond = expression.ComposeDNFCondition(er.sctx, cond, innerNullChecker)
		// If the subquery is empty, it should always return false.
		emptyChecker := expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(allegrosql.TypeTiny), colCount, expression.NewZero())
		// If outer key is null, and subquery is not empty, it should return null.
		outerNullChecker := expression.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(allegrosql.TypeTiny), outerIsNull, expression.NewNull(), expression.NewOne())
		cond = expression.ComposeCNFCondition(er.sctx, cond, emptyChecker, outerNullChecker)
	}

	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
	// plan4Agg.buildProjectionIfNecessary()
	if !er.asScalar {
		// For Semi LogicalApply without aux column, the result is no matter false or null. So we can add it to join predicate.
		er.p, er.err = er.b.buildSemiApply(er.p, plan4Agg, []expression.Expression{cond}, false, false)
		return
	}
	// If we treat the result as a scalar value, we will add a projection with a extra column to output true, false or null.
	outerSchemaLen := er.p.Schema().Len()
	er.p = er.b.buildApplyWithJoinType(er.p, plan4Agg, InnerJoin)
	joinSchema := er.p.Schema()
	proj := LogicalProjection{
		Exprs: expression.DeferredCauset2Exprs(joinSchema.Clone().DeferredCausets[:outerSchemaLen]),
	}.Init(er.sctx, er.b.getSelectOffset())
	proj.names = make([]*types.FieldName, outerSchemaLen, outerSchemaLen+1)
	INTERLOCKy(proj.names, er.p.OutputNames())
	proj.SetSchema(expression.NewSchema(joinSchema.Clone().DeferredCausets[:outerSchemaLen]...))
	proj.Exprs = append(proj.Exprs, cond)
	proj.schemaReplicant.Append(&expression.DeferredCauset{
		UniqueID: er.sctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
		RetType:  cond.GetType(),
	})
	proj.names = append(proj.names, types.EmptyName)
	proj.SetChildren(er.p)
	er.p = proj
}

// handleNEAny handles the case of != any. For example, if the query is t.id != any (select s.id from s), it will be rewrote to
// t.id != s.id or count(distinct s.id) > 1 or [any checker]. If there are two different values in s.id ,
// there must exist a s.id that doesn't equal to t.id.
func (er *expressionRewriter) handleNEAny(lexpr, rexpr expression.Expression, np LogicalPlan) {
	// If there is NULL in s.id column, s.id should be the value that isn't null in condition t.id != s.id.
	// So use function max to filter NULL.
	maxFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncMax, []expression.Expression{rexpr}, false)
	if err != nil {
		er.err = err
		return
	}
	countFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []expression.Expression{rexpr}, true)
	if err != nil {
		er.err = err
		return
	}
	plan4Agg := LogicalAggregation{
		AggFuncs: []*aggregation.AggFuncDesc{maxFunc, countFunc},
	}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.BlockHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)
	maxResultDefCaus := &expression.DeferredCauset{
		UniqueID: er.sctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
		RetType:  maxFunc.RetTp,
	}
	count := &expression.DeferredCauset{
		UniqueID: er.sctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
		RetType:  countFunc.RetTp,
	}
	plan4Agg.names = append(plan4Agg.names, types.EmptyName, types.EmptyName)
	plan4Agg.SetSchema(expression.NewSchema(maxResultDefCaus, count))
	gtFunc := expression.NewFunctionInternal(er.sctx, ast.GT, types.NewFieldType(allegrosql.TypeTiny), count, expression.NewOne())
	neCond := expression.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(allegrosql.TypeTiny), lexpr, maxResultDefCaus)
	cond := expression.ComposeDNFCondition(er.sctx, gtFunc, neCond)
	er.buildQuantifierPlan(plan4Agg, cond, lexpr, rexpr, false)
}

// handleEQAll handles the case of = all. For example, if the query is t.id = all (select s.id from s), it will be rewrote to
// t.id = (select s.id from s having count(distinct s.id) <= 1 and [all checker]).
func (er *expressionRewriter) handleEQAll(lexpr, rexpr expression.Expression, np LogicalPlan) {
	firstRowFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncFirstRow, []expression.Expression{rexpr}, false)
	if err != nil {
		er.err = err
		return
	}
	countFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []expression.Expression{rexpr}, true)
	if err != nil {
		er.err = err
		return
	}
	plan4Agg := LogicalAggregation{
		AggFuncs: []*aggregation.AggFuncDesc{firstRowFunc, countFunc},
	}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.BlockHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)
	plan4Agg.names = append(plan4Agg.names, types.EmptyName)
	firstRowResultDefCaus := &expression.DeferredCauset{
		UniqueID: er.sctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
		RetType:  firstRowFunc.RetTp,
	}
	plan4Agg.names = append(plan4Agg.names, types.EmptyName)
	count := &expression.DeferredCauset{
		UniqueID: er.sctx.GetStochaseinstein_dbars().AllocPlanDeferredCausetID(),
		RetType:  countFunc.RetTp,
	}
	plan4Agg.SetSchema(expression.NewSchema(firstRowResultDefCaus, count))
	leFunc := expression.NewFunctionInternal(er.sctx, ast.LE, types.NewFieldType(allegrosql.TypeTiny), count, expression.NewOne())
	eqCond := expression.NewFunctionInternal(er.sctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), lexpr, firstRowResultDefCaus)
	cond := expression.ComposeCNFCondition(er.sctx, leFunc, eqCond)
	er.buildQuantifierPlan(plan4Agg, cond, lexpr, rexpr, true)
}

func (er *expressionRewriter) handleExistSubquery(ctx context.Context, v *ast.ExistsSubqueryExpr) (ast.Node, bool) {
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown exists type %T.", v.Sel)
		return v, true
	}
	np, err := er.buildSubquery(ctx, subq)
	if err != nil {
		er.err = err
		return v, true
	}
	np = er.popExistsSubPlan(np)
	if len(ExtractCorrelatedDefCauss4LogicalPlan(np)) > 0 {
		er.p, er.err = er.b.buildSemiApply(er.p, np, nil, er.asScalar, v.Not)
		if er.err != nil || !er.asScalar {
			return v, true
		}
		er.ctxStackAppend(er.p.Schema().DeferredCausets[er.p.Schema().Len()-1], er.p.OutputNames()[er.p.Schema().Len()-1])
	} else {
		// We don't want nth_plan hint to affect separately executed subqueries here, so disable nth_plan temporarily.
		NthPlanBackup := er.sctx.GetStochaseinstein_dbars().StmtCtx.StmtHints.ForceNthPlan
		er.sctx.GetStochaseinstein_dbars().StmtCtx.StmtHints.ForceNthPlan = -1
		physicalPlan, _, err := DoOptimize(ctx, er.sctx, er.b.optFlag, np)
		er.sctx.GetStochaseinstein_dbars().StmtCtx.StmtHints.ForceNthPlan = NthPlanBackup
		if err != nil {
			er.err = err
			return v, true
		}
		event, err := EvalSubqueryFirstRow(ctx, physicalPlan, er.b.is, er.b.ctx)
		if err != nil {
			er.err = err
			return v, true
		}
		if (event != nil && !v.Not) || (event == nil && v.Not) {
			er.ctxStackAppend(expression.NewOne(), types.EmptyName)
		} else {
			er.ctxStackAppend(expression.NewZero(), types.EmptyName)
		}
	}
	return v, true
}

// popExistsSubPlan will remove the useless plan in exist's child.
// See comments inside the method for more details.
func (er *expressionRewriter) popExistsSubPlan(p LogicalPlan) LogicalPlan {
out:
	for {
		switch plan := p.(type) {
		// This can be removed when in exists clause,
		// e.g. exists(select count(*) from t order by a) is equal to exists t.
		case *LogicalProjection, *LogicalSort:
			p = p.Children()[0]
		case *LogicalAggregation:
			if len(plan.GroupByItems) == 0 {
				p = LogicalBlockDual{RowCount: 1}.Init(er.sctx, er.b.getSelectOffset())
				break out
			}
			p = p.Children()[0]
		default:
			break out
		}
	}
	return p
}

func (er *expressionRewriter) handleInSubquery(ctx context.Context, v *ast.PatternInExpr) (ast.Node, bool) {
	asScalar := er.asScalar
	er.asScalar = true
	v.Expr.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T.", v.Sel)
		return v, true
	}
	np, err := er.buildSubquery(ctx, subq)
	if err != nil {
		er.err = err
		return v, true
	}
	lLen := expression.GetRowLen(lexpr)
	if lLen != np.Schema().Len() {
		er.err = expression.ErrOperandDeferredCausets.GenWithStackByArgs(lLen)
		return v, true
	}
	var rexpr expression.Expression
	if np.Schema().Len() == 1 {
		rexpr = np.Schema().DeferredCausets[0]
		rDefCaus := rexpr.(*expression.DeferredCauset)
		// For AntiSemiJoin/LeftOuterSemiJoin/AntiLeftOuterSemiJoin, we cannot treat `in` expression as
		// normal column equal condition, so we specially mark the inner operand here.
		if v.Not || asScalar {
			// If both input columns of `in` expression are not null, we can treat the expression
			// as normal column equal condition instead.
			if !allegrosql.HasNotNullFlag(lexpr.GetType().Flag) || !allegrosql.HasNotNullFlag(rDefCaus.GetType().Flag) {
				rDefCausINTERLOCKy := *rDefCaus
				rDefCausINTERLOCKy.InOperand = true
				rexpr = &rDefCausINTERLOCKy
			}
		}
	} else {
		args := make([]expression.Expression, 0, np.Schema().Len())
		for _, col := range np.Schema().DeferredCausets {
			args = append(args, col)
		}
		rexpr, er.err = er.newFunction(ast.RowFunc, args[0].GetType(), args...)
		if er.err != nil {
			return v, true
		}
	}
	checkCondition, err := er.constructBinaryOpFunction(lexpr, rexpr, ast.EQ)
	if err != nil {
		er.err = err
		return v, true
	}

	// If the leftKey and the rightKey have different collations, don't convert the sub-query to an inner-join
	// since when converting we will add a distinct-agg upon the right child and this distinct-agg doesn't have the right collation.
	// To keep it simple, we forbid this converting if they have different collations.
	lt, rt := lexpr.GetType(), rexpr.GetType()
	collFlag := collate.CompatibleDefCauslate(lt.DefCauslate, rt.DefCauslate)

	// If it's not the form of `not in (SUBQUERY)`,
	// and has no correlated column from the current level plan(if the correlated column is from upper level,
	// we can treat it as constant, because the upper LogicalApply cannot be eliminated since current node is a join node),
	// and don't need to append a scalar value, we can rewrite it to inner join.
	if er.sctx.GetStochaseinstein_dbars().GetAllowInSubqToJoinAnPosetDagg() && !v.Not && !asScalar && len(extractCorDeferredCausetsBySchema4LogicalPlan(np, er.p.Schema())) == 0 && collFlag {
		// We need to try to eliminate the agg and the projection produced by this operation.
		er.b.optFlag |= flagEliminateAgg
		er.b.optFlag |= flagEliminateProjection
		er.b.optFlag |= flagJoinReOrder
		// Build distinct for the inner query.
		agg, err := er.b.buildDistinct(np, np.Schema().Len())
		if err != nil {
			er.err = err
			return v, true
		}
		// Build inner join above the aggregation.
		join := LogicalJoin{JoinType: InnerJoin}.Init(er.sctx, er.b.getSelectOffset())
		join.SetChildren(er.p, agg)
		join.SetSchema(expression.MergeSchema(er.p.Schema(), agg.schemaReplicant))
		join.names = make([]*types.FieldName, er.p.Schema().Len()+agg.Schema().Len())
		INTERLOCKy(join.names, er.p.OutputNames())
		INTERLOCKy(join.names[er.p.Schema().Len():], agg.OutputNames())
		join.AttachOnConds(expression.SplitCNFItems(checkCondition))
		// Set join hint for this join.
		if er.b.BlockHints() != nil {
			join.setPreferredJoinType(er.b.BlockHints())
		}
		er.p = join
	} else {
		er.p, er.err = er.b.buildSemiApply(er.p, np, expression.SplitCNFItems(checkCondition), asScalar, v.Not)
		if er.err != nil {
			return v, true
		}
	}

	er.ctxStackPop(1)
	if asScalar {
		col := er.p.Schema().DeferredCausets[er.p.Schema().Len()-1]
		er.ctxStackAppend(col, er.p.OutputNames()[er.p.Schema().Len()-1])
	}
	return v, true
}

func (er *expressionRewriter) handleScalarSubquery(ctx context.Context, v *ast.SubqueryExpr) (ast.Node, bool) {
	np, err := er.buildSubquery(ctx, v)
	if err != nil {
		er.err = err
		return v, true
	}
	np = er.b.buildMaxOneRow(np)
	if len(ExtractCorrelatedDefCauss4LogicalPlan(np)) > 0 {
		er.p = er.b.buildApplyWithJoinType(er.p, np, LeftOuterJoin)
		if np.Schema().Len() > 1 {
			newDefCauss := make([]expression.Expression, 0, np.Schema().Len())
			for _, col := range np.Schema().DeferredCausets {
				newDefCauss = append(newDefCauss, col)
			}
			expr, err1 := er.newFunction(ast.RowFunc, newDefCauss[0].GetType(), newDefCauss...)
			if err1 != nil {
				er.err = err1
				return v, true
			}
			er.ctxStackAppend(expr, types.EmptyName)
		} else {
			er.ctxStackAppend(er.p.Schema().DeferredCausets[er.p.Schema().Len()-1], er.p.OutputNames()[er.p.Schema().Len()-1])
		}
		return v, true
	}
	// We don't want nth_plan hint to affect separately executed subqueries here, so disable nth_plan temporarily.
	NthPlanBackup := er.sctx.GetStochaseinstein_dbars().StmtCtx.StmtHints.ForceNthPlan
	er.sctx.GetStochaseinstein_dbars().StmtCtx.StmtHints.ForceNthPlan = -1
	physicalPlan, _, err := DoOptimize(ctx, er.sctx, er.b.optFlag, np)
	er.sctx.GetStochaseinstein_dbars().StmtCtx.StmtHints.ForceNthPlan = NthPlanBackup
	if err != nil {
		er.err = err
		return v, true
	}
	event, err := EvalSubqueryFirstRow(ctx, physicalPlan, er.b.is, er.b.ctx)
	if err != nil {
		er.err = err
		return v, true
	}
	if np.Schema().Len() > 1 {
		newDefCauss := make([]expression.Expression, 0, np.Schema().Len())
		for i, data := range event {
			newDefCauss = append(newDefCauss, &expression.CouplingConstantWithRadix{
				Value:   data,
				RetType: np.Schema().DeferredCausets[i].GetType()})
		}
		expr, err1 := er.newFunction(ast.RowFunc, newDefCauss[0].GetType(), newDefCauss...)
		if err1 != nil {
			er.err = err1
			return v, true
		}
		er.ctxStackAppend(expr, types.EmptyName)
	} else {
		er.ctxStackAppend(&expression.CouplingConstantWithRadix{
			Value:   event[0],
			RetType: np.Schema().DeferredCausets[0].GetType(),
		}, types.EmptyName)
	}
	return v, true
}

// Leave implements Visitor interface.
func (er *expressionRewriter) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	if er.err != nil {
		return retNode, false
	}
	var inNode = originInNode
	if er.preprocess != nil {
		inNode = er.preprocess(inNode)
	}
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr, *ast.DeferredCausetNameExpr, *ast.ParenthesesExpr, *ast.WhenClause,
		*ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr, *ast.ValuesExpr, *ast.WindowFuncExpr, *ast.BlockNameExpr:
	case *driver.ValueExpr:
		v.Causet.SetValue(v.Causet.GetValue(), &v.Type)
		value := &expression.CouplingConstantWithRadix{Value: v.Causet, RetType: &v.Type}
		er.ctxStackAppend(value, types.EmptyName)
	case *driver.ParamMarkerExpr:
		var value expression.Expression
		value, er.err = expression.ParamMarkerExpression(er.sctx, v)
		if er.err != nil {
			return retNode, false
		}
		er.ctxStackAppend(value, types.EmptyName)
	case *ast.VariableExpr:
		er.rewriteVariable(v)
	case *ast.FuncCallExpr:
		if _, ok := expression.TryFoldFunctions[v.FnName.L]; ok {
			er.tryFoldCounter--
		}
		er.funcCallToExpression(v)
		if _, ok := expression.DisableFoldFunctions[v.FnName.L]; ok {
			er.disableFoldCounter--
		}
	case *ast.BlockName:
		er.toBlock(v)
	case *ast.DeferredCausetName:
		er.toDeferredCauset(v)
	case *ast.UnaryOperationExpr:
		er.unaryOpToExpression(v)
	case *ast.BinaryOperationExpr:
		er.binaryOpToExpression(v)
	case *ast.BetweenExpr:
		er.betweenToExpression(v)
	case *ast.CaseExpr:
		if _, ok := expression.TryFoldFunctions["case"]; ok {
			er.tryFoldCounter--
		}
		er.caseToExpression(v)
		if _, ok := expression.DisableFoldFunctions["case"]; ok {
			er.disableFoldCounter--
		}
	case *ast.FuncCastExpr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		er.err = expression.CheckArgsNotMultiDeferredCausetRow(arg)
		if er.err != nil {
			return retNode, false
		}

		// check the decimal precision of "CAST(AS TIME)".
		er.err = er.checkTimePrecision(v.Tp)
		if er.err != nil {
			return retNode, false
		}

		er.ctxStack[len(er.ctxStack)-1] = expression.BuildCastFunction(er.sctx, arg, v.Tp)
		er.ctxNameStk[len(er.ctxNameStk)-1] = types.EmptyName
	case *ast.PatternLikeExpr:
		er.patternLikeToExpression(v)
	case *ast.PatternRegexpExpr:
		er.regexpToScalarFunc(v)
	case *ast.RowExpr:
		er.rowToScalarFunc(v)
	case *ast.PatternInExpr:
		if v.Sel == nil {
			er.inToExpression(len(v.List), v.Not, &v.Type)
		}
	case *ast.PositionExpr:
		er.positionToScalarFunc(v)
	case *ast.IsNullExpr:
		er.isNullToExpression(v)
	case *ast.IsTruthExpr:
		er.isTrueToScalarFunc(v)
	case *ast.DefaultExpr:
		er.evalDefaultExpr(v)
	// TODO: Perhaps we don't need to transcode these back to generic integers/strings
	case *ast.TrimDirectionExpr:
		er.ctxStackAppend(&expression.CouplingConstantWithRadix{
			Value:   types.NewIntCauset(int64(v.Direction)),
			RetType: types.NewFieldType(allegrosql.TypeTiny),
		}, types.EmptyName)
	case *ast.TimeUnitExpr:
		er.ctxStackAppend(&expression.CouplingConstantWithRadix{
			Value:   types.NewStringCauset(v.Unit.String()),
			RetType: types.NewFieldType(allegrosql.TypeVarchar),
		}, types.EmptyName)
	case *ast.GetFormatSelectorExpr:
		er.ctxStackAppend(&expression.CouplingConstantWithRadix{
			Value:   types.NewStringCauset(v.Selector.String()),
			RetType: types.NewFieldType(allegrosql.TypeVarchar),
		}, types.EmptyName)
	case *ast.SetDefCauslationExpr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		if collate.NewDefCauslationEnabled() {
			var collInfo *charset.DefCauslation
			// TODO(bb7133): use charset.ValidCharsetAndDefCauslation when its bug is fixed.
			if collInfo, er.err = collate.GetDefCauslationByName(v.DefCauslate); er.err != nil {
				break
			}
			chs := arg.GetType().Charset
			if chs != "" && collInfo.CharsetName != chs {
				er.err = charset.ErrDefCauslationCharsetMismatch.GenWithStackByArgs(collInfo.Name, chs)
				break
			}
		}
		// SetDefCauslationExpr sets the collation explicitly, even when the evaluation type of the expression is non-string.
		if _, ok := arg.(*expression.DeferredCauset); ok {
			// Wrap a cast here to avoid changing the original FieldType of the column expression.
			exprType := arg.GetType().Clone()
			exprType.DefCauslate = v.DefCauslate
			casted := expression.BuildCastFunction(er.sctx, arg, exprType)
			er.ctxStackPop(1)
			er.ctxStackAppend(casted, types.EmptyName)
		} else {
			// For constant and scalar function, we can set its collate directly.
			arg.GetType().DefCauslate = v.DefCauslate
		}
		er.ctxStack[len(er.ctxStack)-1].SetCoercibility(expression.CoercibilityExplicit)
		er.ctxStack[len(er.ctxStack)-1].SetCharsetAndDefCauslation(arg.GetType().Charset, arg.GetType().DefCauslate)
	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}

	if er.err != nil {
		return retNode, false
	}
	return originInNode, true
}

// newFunction chooses which expression.NewFunctionImpl() will be used.
func (er *expressionRewriter) newFunction(funcName string, retType *types.FieldType, args ...expression.Expression) (expression.Expression, error) {
	if er.disableFoldCounter > 0 {
		return expression.NewFunctionBase(er.sctx, funcName, retType, args...)
	}
	if er.tryFoldCounter > 0 {
		return expression.NewFunctionTryFold(er.sctx, funcName, retType, args...)
	}
	return expression.NewFunction(er.sctx, funcName, retType, args...)
}

func (er *expressionRewriter) checkTimePrecision(ft *types.FieldType) error {
	if ft.EvalType() == types.ETDuration && ft.Decimal > int(types.MaxFsp) {
		return errTooBigPrecision.GenWithStackByArgs(ft.Decimal, "CAST", types.MaxFsp)
	}
	return nil
}

func (er *expressionRewriter) useCache() bool {
	return er.sctx.GetStochaseinstein_dbars().StmtCtx.UseCache
}

func (er *expressionRewriter) rewriteVariable(v *ast.VariableExpr) {
	stkLen := len(er.ctxStack)
	name := strings.ToLower(v.Name)
	stochaseinstein_dbars := er.b.ctx.GetStochaseinstein_dbars()
	if !v.IsSystem {
		if v.Value != nil {
			er.ctxStack[stkLen-1], er.err = er.newFunction(ast.SetVar,
				er.ctxStack[stkLen-1].GetType(),
				expression.CausetToCouplingConstantWithRadix(types.NewCauset(name), allegrosql.TypeString),
				er.ctxStack[stkLen-1])
			er.ctxNameStk[stkLen-1] = types.EmptyName
			return
		}
		f, err := er.newFunction(ast.GetVar,
			// TODO: Here is wrong, the stochaseinstein_dbars should causetstore a name -> Causet map. Will fix it later.
			types.NewFieldType(allegrosql.TypeString),
			expression.CausetToCouplingConstantWithRadix(types.NewStringCauset(name), allegrosql.TypeString))
		if err != nil {
			er.err = err
			return
		}
		f.SetCoercibility(expression.CoercibilityImplicit)
		er.ctxStackAppend(f, types.EmptyName)
		return
	}
	var val string
	var err error
	if v.ExplicitSINTERLOCKe {
		err = variable.ValidateGetSystemVar(name, v.IsGlobal)
		if err != nil {
			er.err = err
			return
		}
	}
	sysVar := variable.SysVars[name]
	if sysVar == nil {
		er.err = variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
		return
	}
	// Variable is @@gobal.variable_name or variable is only global sINTERLOCKe variable.
	if v.IsGlobal || sysVar.SINTERLOCKe == variable.SINTERLOCKeGlobal {
		val, err = variable.GetGlobalSystemVar(stochaseinstein_dbars, name)
	} else {
		val, err = variable.GetStochastikSystemVar(stochaseinstein_dbars, name)
	}
	if err != nil {
		er.err = err
		return
	}
	e := expression.CausetToCouplingConstantWithRadix(types.NewStringCauset(val), allegrosql.TypeVarString)
	e.GetType().Charset, _ = er.sctx.GetStochaseinstein_dbars().GetSystemVar(variable.CharacterSetConnection)
	e.GetType().DefCauslate, _ = er.sctx.GetStochaseinstein_dbars().GetSystemVar(variable.DefCauslationConnection)
	er.ctxStackAppend(e, types.EmptyName)
}

func (er *expressionRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
	stkLen := len(er.ctxStack)
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
		er.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = expression.ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return
	}
	er.ctxStack[stkLen-1], er.err = er.newFunction(op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxNameStk[stkLen-1] = types.EmptyName
}

func (er *expressionRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var function expression.Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		function, er.err = er.constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1],
			v.Op.String())
	default:
		lLen := expression.GetRowLen(er.ctxStack[stkLen-2])
		rLen := expression.GetRowLen(er.ctxStack[stkLen-1])
		if lLen != 1 || rLen != 1 {
			er.err = expression.ErrOperandDeferredCausets.GenWithStackByArgs(1)
			return
		}
		function, er.err = er.newFunction(v.Op.String(), types.NewFieldType(allegrosql.TypeUnspecified), er.ctxStack[stkLen-2:]...)
	}
	if er.err != nil {
		return
	}
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...expression.Expression) expression.Expression {
	opFunc, err := er.newFunction(op, tp, args...)
	if err != nil {
		er.err = err
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = er.newFunction(ast.UnaryNot, tp, opFunc)
	if err != nil {
		er.err = err
		return nil
	}
	return opFunc
}

func (er *expressionRewriter) isNullToExpression(v *ast.IsNullExpr) {
	stkLen := len(er.ctxStack)
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = expression.ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, ast.IsNull, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) positionToScalarFunc(v *ast.PositionExpr) {
	pos := v.N
	str := strconv.Itoa(pos)
	if v.P != nil {
		stkLen := len(er.ctxStack)
		val := er.ctxStack[stkLen-1]
		intNum, isNull, err := expression.GetIntFromCouplingConstantWithRadix(er.sctx, val)
		str = "?"
		if err == nil {
			if isNull {
				return
			}
			pos = intNum
			er.ctxStackPop(1)
		}
		er.err = err
	}
	if er.err == nil && pos > 0 && pos <= er.schemaReplicant.Len() {
		er.ctxStackAppend(er.schemaReplicant.DeferredCausets[pos-1], er.names[pos-1])
	} else {
		er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(str, clauseMsg[er.b.curClause])
	}
}

func (er *expressionRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	stkLen := len(er.ctxStack)
	op := ast.IsTruthWithoutNull
	if v.True == 0 {
		op = ast.IsFalsity
	}
	if expression.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = expression.ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

// inToExpression converts in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (er *expressionRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	stkLen := len(er.ctxStack)
	l := expression.GetRowLen(er.ctxStack[stkLen-lLen-1])
	for i := 0; i < lLen; i++ {
		if l != expression.GetRowLen(er.ctxStack[stkLen-lLen+i]) {
			er.err = expression.ErrOperandDeferredCausets.GenWithStackByArgs(l)
			return
		}
	}
	args := er.ctxStack[stkLen-lLen-1:]
	leftFt := args[0].GetType()
	leftEt, leftIsNull := leftFt.EvalType(), leftFt.Tp == allegrosql.TypeNull
	if leftIsNull {
		er.ctxStackPop(lLen + 1)
		er.ctxStackAppend(expression.NewNull(), types.EmptyName)
		return
	}
	if leftEt == types.CausetEDN {
		for i := 1; i < len(args); i++ {
			if c, ok := args[i].(*expression.CouplingConstantWithRadix); ok {
				var isExceptional bool
				args[i], isExceptional = expression.RefineComparedCouplingConstantWithRadix(er.sctx, *leftFt, c, opcode.EQ)
				if isExceptional {
					args[i] = c
				}
			}
		}
	}
	allSameType := true
	for _, arg := range args[1:] {
		if arg.GetType().Tp != allegrosql.TypeNull && expression.GetAccurateCmpType(args[0], arg) != leftEt {
			allSameType = false
			break
		}
	}
	var function expression.Expression
	if allSameType && l == 1 && lLen > 1 {
		function = er.notToExpression(not, ast.In, tp, er.ctxStack[stkLen-lLen-1:]...)
	} else {
		eqFunctions := make([]expression.Expression, 0, lLen)
		for i := stkLen - lLen; i < stkLen; i++ {
			expr, err := er.constructBinaryOpFunction(args[0], er.ctxStack[i], ast.EQ)
			if err != nil {
				er.err = err
				return
			}
			eqFunctions = append(eqFunctions, expr)
		}
		function = expression.ComposeDNFCondition(er.sctx, eqFunctions...)
		if not {
			var err error
			function, err = er.newFunction(ast.UnaryNot, tp, function)
			if err != nil {
				er.err = err
				return
			}
		}
	}
	er.ctxStackPop(lLen + 1)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) caseToExpression(v *ast.CaseExpr) {
	stkLen := len(er.ctxStack)
	argsLen := 2 * len(v.WhenClauses)
	if v.ElseClause != nil {
		argsLen++
	}
	er.err = expression.CheckArgsNotMultiDeferredCausetRow(er.ctxStack[stkLen-argsLen:]...)
	if er.err != nil {
		return
	}

	// value                          -> ctxStack[stkLen-argsLen-1]
	// when clause(condition, result) -> ctxStack[stkLen-argsLen:stkLen-1];
	// else clause                    -> ctxStack[stkLen-1]
	var args []expression.Expression
	if v.Value != nil {
		// args:  eq scalar func(args: value, condition1), result1,
		//        eq scalar func(args: value, condition2), result2,
		//        ...
		//        else clause
		value := er.ctxStack[stkLen-argsLen-1]
		args = make([]expression.Expression, 0, argsLen)
		for i := stkLen - argsLen; i < stkLen-1; i += 2 {
			arg, err := er.newFunction(ast.EQ, types.NewFieldType(allegrosql.TypeTiny), value, er.ctxStack[i])
			if err != nil {
				er.err = err
				return
			}
			args = append(args, arg)
			args = append(args, er.ctxStack[i+1])
		}
		if v.ElseClause != nil {
			args = append(args, er.ctxStack[stkLen-1])
		}
		argsLen++ // for trimming the value element later
	} else {
		// args:  condition1, result1,
		//        condition2, result2,
		//        ...
		//        else clause
		args = er.ctxStack[stkLen-argsLen:]
	}
	function, err := er.newFunction(ast.Case, &v.Type, args...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackPop(argsLen)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) patternLikeToExpression(v *ast.PatternLikeExpr) {
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiDeferredCausetRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}

	char, col := er.sctx.GetStochaseinstein_dbars().GetCharsetInfo()
	var function expression.Expression
	fieldType := &types.FieldType{}
	isPatternExactMatch := false
	// Treat predicate 'like' the same way as predicate '=' when it is an exact match.
	if patExpression, ok := er.ctxStack[l-1].(*expression.CouplingConstantWithRadix); ok {
		patString, isNull, err := patExpression.EvalString(nil, chunk.Row{})
		if err != nil {
			er.err = err
			return
		}
		if !isNull {
			patValue, patTypes := stringutil.CompilePattern(patString, v.Escape)
			if stringutil.IsExactMatch(patTypes) && er.ctxStack[l-2].GetType().EvalType() == types.ETString {
				op := ast.EQ
				if v.Not {
					op = ast.NE
				}
				types.DefaultTypeForValue(string(patValue), fieldType, char, col)
				function, er.err = er.constructBinaryOpFunction(er.ctxStack[l-2],
					&expression.CouplingConstantWithRadix{Value: types.NewStringCauset(string(patValue)), RetType: fieldType},
					op)
				isPatternExactMatch = true
			}
		}
	}
	if !isPatternExactMatch {
		types.DefaultTypeForValue(int(v.Escape), fieldType, char, col)
		function = er.notToExpression(v.Not, ast.Like, &v.Type,
			er.ctxStack[l-2], er.ctxStack[l-1], &expression.CouplingConstantWithRadix{Value: types.NewIntCauset(int64(v.Escape)), RetType: fieldType})
	}

	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	l := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiDeferredCausetRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}
	function := er.notToExpression(v.Not, ast.Regexp, &v.Type, er.ctxStack[l-2], er.ctxStack[l-1])
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) rowToScalarFunc(v *ast.RowExpr) {
	stkLen := len(er.ctxStack)
	length := len(v.Values)
	rows := make([]expression.Expression, 0, length)
	for i := stkLen - length; i < stkLen; i++ {
		rows = append(rows, er.ctxStack[i])
	}
	er.ctxStackPop(length)
	function, err := er.newFunction(ast.RowFunc, rows[0].GetType(), rows...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *expressionRewriter) betweenToExpression(v *ast.BetweenExpr) {
	stkLen := len(er.ctxStack)
	er.err = expression.CheckArgsNotMultiDeferredCausetRow(er.ctxStack[stkLen-3:]...)
	if er.err != nil {
		return
	}

	expr, lexp, rexp := er.ctxStack[stkLen-3], er.ctxStack[stkLen-2], er.ctxStack[stkLen-1]

	if expression.GetCmpTp4MinMax([]expression.Expression{expr, lexp, rexp}) == types.ETDatetime {
		expr = expression.WrapWithCastAsTime(er.sctx, expr, types.NewFieldType(allegrosql.TypeDatetime))
		lexp = expression.WrapWithCastAsTime(er.sctx, lexp, types.NewFieldType(allegrosql.TypeDatetime))
		rexp = expression.WrapWithCastAsTime(er.sctx, rexp, types.NewFieldType(allegrosql.TypeDatetime))
	}

	var op string
	var l, r expression.Expression
	l, er.err = er.newFunction(ast.GE, &v.Type, expr, lexp)
	if er.err == nil {
		r, er.err = er.newFunction(ast.LE, &v.Type, expr, rexp)
	}
	op = ast.LogicAnd
	if er.err != nil {
		return
	}
	function, err := er.newFunction(op, &v.Type, l, r)
	if err != nil {
		er.err = err
		return
	}
	if v.Not {
		function, err = er.newFunction(ast.UnaryNot, &v.Type, function)
		if err != nil {
			er.err = err
			return
		}
	}
	er.ctxStackPop(3)
	er.ctxStackAppend(function, types.EmptyName)
}

// rewriteFuncCall handles a FuncCallExpr and generates a customized function.
// It should return true if for the given FuncCallExpr a rewrite is performed so that original behavior is skipped.
// Otherwise it should return false to indicate (the caller) that original behavior needs to be performed.
func (er *expressionRewriter) rewriteFuncCall(v *ast.FuncCallExpr) bool {
	switch v.FnName.L {
	// when column is not null, ifnull on such column is not necessary.
	case ast.Ifnull:
		if len(v.Args) != 2 {
			er.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		arg1 := er.ctxStack[stackLen-2]
		col, isDeferredCauset := arg1.(*expression.DeferredCauset)
		// if expr1 is a column and column has not null flag, then we can eliminate ifnull on
		// this column.
		if isDeferredCauset && allegrosql.HasNotNullFlag(col.RetType.Flag) {
			name := er.ctxNameStk[stackLen-2]
			newDefCaus := col.Clone().(*expression.DeferredCauset)
			er.ctxStackPop(len(v.Args))
			er.ctxStackAppend(newDefCaus, name)
			return true
		}

		return false
	case ast.Nullif:
		if len(v.Args) != 2 {
			er.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		param1 := er.ctxStack[stackLen-2]
		param2 := er.ctxStack[stackLen-1]
		// param1 = param2
		funcCompare, err := er.constructBinaryOpFunction(param1, param2, ast.EQ)
		if err != nil {
			er.err = err
			return true
		}
		// NULL
		nullTp := types.NewFieldType(allegrosql.TypeNull)
		nullTp.Flen, nullTp.Decimal = allegrosql.GetDefaultFieldLengthAndDecimal(allegrosql.TypeNull)
		paramNull := &expression.CouplingConstantWithRadix{
			Value:   types.NewCauset(nil),
			RetType: nullTp,
		}
		// if(param1 = param2, NULL, param1)
		funcIf, err := er.newFunction(ast.If, &v.Type, funcCompare, paramNull, param1)
		if err != nil {
			er.err = err
			return true
		}
		er.ctxStackPop(len(v.Args))
		er.ctxStackAppend(funcIf, types.EmptyName)
		return true
	default:
		return false
	}
}

func (er *expressionRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	stackLen := len(er.ctxStack)
	args := er.ctxStack[stackLen-len(v.Args):]
	er.err = expression.CheckArgsNotMultiDeferredCausetRow(args...)
	if er.err != nil {
		return
	}

	if er.rewriteFuncCall(v) {
		return
	}

	var function expression.Expression
	er.ctxStackPop(len(v.Args))
	if _, ok := expression.DeferredFunctions[v.FnName.L]; er.useCache() && ok {
		// When the expression is unix_timestamp and the number of argument is not zero,
		// we deal with it as normal expression.
		if v.FnName.L == ast.UnixTimestamp && len(v.Args) != 0 {
			function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
			er.ctxStackAppend(function, types.EmptyName)
		} else {
			function, er.err = expression.NewFunctionBase(er.sctx, v.FnName.L, &v.Type, args...)
			c := &expression.CouplingConstantWithRadix{Value: types.NewCauset(nil), RetType: function.GetType().Clone(), DeferredExpr: function}
			er.ctxStackAppend(c, types.EmptyName)
		}
	} else {
		function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
		er.ctxStackAppend(function, types.EmptyName)
	}
}

// Now BlockName in expression only used by sequence function like nextval(seq).
// The function arg should be evaluated as a block name rather than normal column name like allegrosql does.
func (er *expressionRewriter) toBlock(v *ast.BlockName) {
	fullName := v.Name.L
	if len(v.Schema.L) != 0 {
		fullName = v.Schema.L + "." + fullName
	}
	val := &expression.CouplingConstantWithRadix{
		Value:   types.NewCauset(fullName),
		RetType: types.NewFieldType(allegrosql.TypeString),
	}
	er.ctxStackAppend(val, types.EmptyName)
}

func (er *expressionRewriter) toDeferredCauset(v *ast.DeferredCausetName) {
	idx, err := expression.FindFieldName(er.names, v)
	if err != nil {
		er.err = ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
		return
	}
	if idx >= 0 {
		column := er.schemaReplicant.DeferredCausets[idx]
		if column.IsHidden {
			er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.Name, clauseMsg[er.b.curClause])
			return
		}
		er.ctxStackAppend(column, er.names[idx])
		return
	}
	for i := len(er.b.outerSchemas) - 1; i >= 0; i-- {
		outerSchema, outerName := er.b.outerSchemas[i], er.b.outerNames[i]
		idx, err = expression.FindFieldName(outerName, v)
		if idx >= 0 {
			column := outerSchema.DeferredCausets[idx]
			er.ctxStackAppend(&expression.CorrelatedDeferredCauset{DeferredCauset: *column, Data: new(types.Causet)}, outerName[idx])
			return
		}
		if err != nil {
			er.err = ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
			return
		}
	}
	if join, ok := er.p.(*LogicalJoin); ok && join.redundantSchema != nil {
		idx, err := expression.FindFieldName(join.redundantNames, v)
		if err != nil {
			er.err = err
			return
		}
		if idx >= 0 {
			er.ctxStackAppend(join.redundantSchema.DeferredCausets[idx], join.redundantNames[idx])
			return
		}
	}
	if _, ok := er.p.(*LogicalUnionAll); ok && v.Block.O != "" {
		er.err = ErrBlocknameNotAllowedHere.GenWithStackByArgs(v.Block.O, "SELECT", clauseMsg[er.b.curClause])
		return
	}
	if er.b.curClause == globalOrderByClause {
		er.b.curClause = orderByClause
	}
	er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.String(), clauseMsg[er.b.curClause])
}

func (er *expressionRewriter) evalDefaultExpr(v *ast.DefaultExpr) {
	var name *types.FieldName
	// Here we will find the corresponding column for default function. At the same time, we need to consider the issue
	// of subquery and name space.
	// For example, we have two blocks t1(a int default 1, b int) and t2(a int default -1, c int). Consider the following ALLEGROALLEGROSQL:
	// 		select a from t1 where a > (select default(a) from t2)
	// Refer to the behavior of MyALLEGROSQL, we need to find column a in block t2. If block t2 does not have column a, then find it
	// in block t1. If there are none, return an error message.
	// Based on the above description, we need to look in er.b.allNames from back to front.
	for i := len(er.b.allNames) - 1; i >= 0; i-- {
		idx, err := expression.FindFieldName(er.b.allNames[i], v.Name)
		if err != nil {
			er.err = err
			return
		}
		if idx >= 0 {
			name = er.b.allNames[i][idx]
			break
		}
	}
	if name == nil {
		idx, err := expression.FindFieldName(er.names, v.Name)
		if err != nil {
			er.err = err
			return
		}
		if idx < 0 {
			er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.Name.OrigDefCausName(), "field list")
			return
		}
		name = er.names[idx]
	}

	dbName := name.DBName
	if dbName.O == "" {
		// if database name is not specified, use current database name
		dbName = perceptron.NewCIStr(er.sctx.GetStochaseinstein_dbars().CurrentDB)
	}
	if name.OrigTblName.O == "" {
		// column is evaluated by some expressions, for example:
		// `select default(c) from (select (a+1) as c from t) as t0`
		// in such case, a 'no default' error is returned
		er.err = block.ErrNoDefaultValue.GenWithStackByArgs(name.DefCausName)
		return
	}
	var tbl block.Block
	tbl, er.err = er.b.is.BlockByName(dbName, name.OrigTblName)
	if er.err != nil {
		return
	}
	colName := name.OrigDefCausName.O
	if colName == "" {
		// in some cases, OrigDefCausName is empty, use DefCausName instead
		colName = name.DefCausName.O
	}
	col := block.FindDefCaus(tbl.DefCauss(), colName)
	if col == nil {
		er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.Name, "field_list")
		return
	}
	isCurrentTimestamp := hasCurrentDatetimeDefault(col)
	var val *expression.CouplingConstantWithRadix
	switch {
	case isCurrentTimestamp && col.Tp == allegrosql.TypeDatetime:
		// for DATETIME column with current_timestamp, use NULL to be compatible with MyALLEGROSQL 5.7
		val = expression.NewNull()
	case isCurrentTimestamp && col.Tp == allegrosql.TypeTimestamp:
		// for TIMESTAMP column with current_timestamp, use 0 to be compatible with MyALLEGROSQL 5.7
		zero := types.NewTime(types.ZeroCoreTime, allegrosql.TypeTimestamp, int8(col.Decimal))
		val = &expression.CouplingConstantWithRadix{
			Value:   types.NewCauset(zero),
			RetType: types.NewFieldType(allegrosql.TypeTimestamp),
		}
	default:
		// for other columns, just use what it is
		val, er.err = er.b.getDefaultValue(col)
	}
	if er.err != nil {
		return
	}
	er.ctxStackAppend(val, types.EmptyName)
}

// hasCurrentDatetimeDefault checks if column has current_timestamp default value
func hasCurrentDatetimeDefault(col *block.DeferredCauset) bool {
	x, ok := col.DefaultValue.(string)
	if !ok {
		return false
	}
	return strings.ToLower(x) == ast.CurrentTimestamp
}
