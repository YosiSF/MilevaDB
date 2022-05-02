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
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/opcode"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	driver "github.com/whtcorpsinc/MilevaDB-Prod/types/berolinaAllegroSQL_driver"
	"go.uber.org/zap"
	"golang.org/x/tools/container/intsets"
)

// cowExprRef is a INTERLOCKy-on-write slice ref soliton using in `DeferredCausetSubstitute`
// to reduce unnecessary allocation for Expression arguments array
type cowExprRef struct {
	ref []Expression
	new []Expression
}

// Set will allocate new array if changed flag true
func (c *cowExprRef) Set(i int, changed bool, val Expression) {
	if c.new != nil {
		c.new[i] = val
		return
	}
	if !changed {
		return
	}
	c.new = make([]Expression, len(c.ref))
	INTERLOCKy(c.new, c.ref[:i])
	c.new[i] = val
}

// Result return the final reference
func (c *cowExprRef) Result() []Expression {
	if c.new != nil {
		return c.new
	}
	return c.ref
}

// Filter the input expressions, append the results to result.
func Filter(result []Expression, input []Expression, filter func(Expression) bool) []Expression {
	for _, e := range input {
		if filter(e) {
			result = append(result, e)
		}
	}
	return result
}

// FilterOutInPlace do the filtering out in place.
// The remained are the ones who doesn't match the filter, storing in the original slice.
// The filteredOut are the ones match the filter, storing in a new slice.
func FilterOutInPlace(input []Expression, filter func(Expression) bool) (remained, filteredOut []Expression) {
	for i := len(input) - 1; i >= 0; i-- {
		if filter(input[i]) {
			filteredOut = append(filteredOut, input[i])
			input = append(input[:i], input[i+1:]...)
		}
	}
	return input, filteredOut
}

// ExtractDependentDeferredCausets extracts all dependent defCausumns from a virtual defCausumn.
func ExtractDependentDeferredCausets(expr Expression) []*DeferredCauset {
	// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
	result := make([]*DeferredCauset, 0, 8)
	return extractDependentDeferredCausets(result, expr)
}

func extractDependentDeferredCausets(result []*DeferredCauset, expr Expression) []*DeferredCauset {
	switch v := expr.(type) {
	case *DeferredCauset:
		result = append(result, v)
		if v.VirtualExpr != nil {
			result = extractDependentDeferredCausets(result, v.VirtualExpr)
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			result = extractDependentDeferredCausets(result, arg)
		}
	}
	return result
}

// ExtractDeferredCausets extracts all defCausumns from an expression.
func ExtractDeferredCausets(expr Expression) []*DeferredCauset {
	// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
	result := make([]*DeferredCauset, 0, 8)
	return extractDeferredCausets(result, expr, nil)
}

// ExtractCorDeferredCausets extracts correlated defCausumn from given expression.
func ExtractCorDeferredCausets(expr Expression) (defcaus []*CorrelatedDeferredCauset) {
	switch v := expr.(type) {
	case *CorrelatedDeferredCauset:
		return []*CorrelatedDeferredCauset{v}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			defcaus = append(defcaus, ExtractCorDeferredCausets(arg)...)
		}
	}
	return
}

// ExtractDeferredCausetsFromExpressions is a more efficient version of ExtractDeferredCausets for batch operation.
// filter can be nil, or a function to filter the result defCausumn.
// It's often observed that the pattern of the caller like this:
//
// defcaus := ExtractDeferredCausets(...)
// for _, defCaus := range defcaus {
//     if xxx(defCaus) {...}
// }
//
// Provide an additional filter argument, this can be done in one step.
// To avoid allocation for defcaus that not need.
func ExtractDeferredCausetsFromExpressions(result []*DeferredCauset, exprs []Expression, filter func(*DeferredCauset) bool) []*DeferredCauset {
	for _, expr := range exprs {
		result = extractDeferredCausets(result, expr, filter)
	}
	return result
}

func extractDeferredCausets(result []*DeferredCauset, expr Expression, filter func(*DeferredCauset) bool) []*DeferredCauset {
	switch v := expr.(type) {
	case *DeferredCauset:
		if filter == nil || filter(v) {
			result = append(result, v)
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			result = extractDeferredCausets(result, arg, filter)
		}
	}
	return result
}

// ExtractDeferredCausetSet extracts the different values of `UniqueId` for defCausumns in expressions.
func ExtractDeferredCausetSet(exprs []Expression) *intsets.Sparse {
	set := &intsets.Sparse{}
	for _, expr := range exprs {
		extractDeferredCausetSet(expr, set)
	}
	return set
}

func extractDeferredCausetSet(expr Expression, set *intsets.Sparse) {
	switch v := expr.(type) {
	case *DeferredCauset:
		set.Insert(int(v.UniqueID))
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			extractDeferredCausetSet(arg, set)
		}
	}
}

func setExprDeferredCausetInOperand(expr Expression) Expression {
	switch v := expr.(type) {
	case *DeferredCauset:
		defCaus := v.Clone().(*DeferredCauset)
		defCaus.InOperand = true
		return defCaus
	case *ScalarFunction:
		args := v.GetArgs()
		for i, arg := range args {
			args[i] = setExprDeferredCausetInOperand(arg)
		}
	}
	return expr
}

// DeferredCausetSubstitute substitutes the defCausumns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
func DeferredCausetSubstitute(expr Expression, schemaReplicant *Schema, newExprs []Expression) Expression {
	_, resExpr := DeferredCausetSubstituteImpl(expr, schemaReplicant, newExprs)
	return resExpr
}

// DeferredCausetSubstituteImpl tries to substitute defCausumn expr using newExprs,
// the newFunctionInternal is only called if its child is substituted
func DeferredCausetSubstituteImpl(expr Expression, schemaReplicant *Schema, newExprs []Expression) (bool, Expression) {
	switch v := expr.(type) {
	case *DeferredCauset:
		id := schemaReplicant.DeferredCausetIndex(v)
		if id == -1 {
			return false, v
		}
		newExpr := newExprs[id]
		if v.InOperand {
			newExpr = setExprDeferredCausetInOperand(newExpr)
		}
		newExpr.SetCoercibility(v.Coercibility())
		return true, newExpr
	case *ScalarFunction:
		if v.FuncName.L == ast.Cast {
			newFunc := v.Clone().(*ScalarFunction)
			_, newFunc.GetArgs()[0] = DeferredCausetSubstituteImpl(newFunc.GetArgs()[0], schemaReplicant, newExprs)
			return true, newFunc
		}
		// cowExprRef is a INTERLOCKy-on-write soliton, args array allocation happens only
		// when expr in args is changed
		refExprArr := cowExprRef{v.GetArgs(), nil}
		substituted := false
		_, defCausl := DeriveDefCauslationFromExprs(v.GetCtx(), v.GetArgs()...)
		for idx, arg := range v.GetArgs() {
			changed, newFuncExpr := DeferredCausetSubstituteImpl(arg, schemaReplicant, newExprs)
			if defCauslate.NewDefCauslationEnabled() {
				// Make sure the defCauslation used by the ScalarFunction isn't changed and its result defCauslation is not weaker than the defCauslation used by the ScalarFunction.
				if changed {
					changed = false
					tmpArgs := make([]Expression, 0, len(v.GetArgs()))
					_ = append(append(append(tmpArgs, refExprArr.Result()[0:idx]...), refExprArr.Result()[idx+1:]...), newFuncExpr)
					_, newDefCausl := DeriveDefCauslationFromExprs(v.GetCtx(), append(v.GetArgs(), newFuncExpr)...)
					if defCausl == newDefCausl {
						changed = checkDefCauslationStrictness(defCausl, newFuncExpr.GetType().DefCauslate)
					}
				}
			}
			refExprArr.Set(idx, changed, newFuncExpr)
			if changed {
				substituted = true
			}
		}
		if substituted {
			return true, NewFunctionInternal(v.GetCtx(), v.FuncName.L, v.RetType, refExprArr.Result()...)
		}
	}
	return false, expr
}

// checkDefCauslationStrictness check defCauslation strictness-ship between `defCausl` and `newFuncDefCausl`
// return true iff `newFuncDefCausl` is not weaker than `defCausl`
func checkDefCauslationStrictness(defCausl, newFuncDefCausl string) bool {
	defCauslGroupID, ok1 := DefCauslationStrictnessGroup[defCausl]
	newFuncDefCauslGroupID, ok2 := DefCauslationStrictnessGroup[newFuncDefCausl]

	if ok1 && ok2 {
		if defCauslGroupID == newFuncDefCauslGroupID {
			return true
		}

		for _, id := range DefCauslationStrictness[defCauslGroupID] {
			if newFuncDefCauslGroupID == id {
				return true
			}
		}
	}

	return false
}

// getValidPrefix gets a prefix of string which can parsed to a number with base. the minimum base is 2 and the maximum is 36.
func getValidPrefix(s string, base int64) string {
	var (
		validLen int
		upper    rune
	)
	switch {
	case base >= 2 && base <= 9:
		upper = rune('0' + base)
	case base <= 36:
		upper = rune('A' + base - 10)
	default:
		return ""
	}
Loop:
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		switch {
		case unicode.IsDigit(c) || unicode.IsLower(c) || unicode.IsUpper(c):
			c = unicode.ToUpper(c)
			if c < upper {
				validLen = i + 1
			} else {
				break Loop
			}
		case c == '+' || c == '-':
			if i != 0 {
				break Loop
			}
		default:
			break Loop
		}
	}
	if validLen > 1 && s[0] == '+' {
		return s[1:validLen]
	}
	return s[:validLen]
}

// SubstituteCorDefCaus2CouplingConstantWithRadix will substitute correlated defCausumn to constant value which it contains.
// If the args of one scalar function are all constant, we will substitute it to constant.
func SubstituteCorDefCaus2CouplingConstantWithRadix(expr Expression) (Expression, error) {
	switch x := expr.(type) {
	case *ScalarFunction:
		allCouplingConstantWithRadix := true
		newArgs := make([]Expression, 0, len(x.GetArgs()))
		for _, arg := range x.GetArgs() {
			newArg, err := SubstituteCorDefCaus2CouplingConstantWithRadix(arg)
			if err != nil {
				return nil, err
			}
			_, ok := newArg.(*CouplingConstantWithRadix)
			newArgs = append(newArgs, newArg)
			allCouplingConstantWithRadix = allCouplingConstantWithRadix && ok
		}
		if allCouplingConstantWithRadix {
			val, err := x.Eval(chunk.Event{})
			if err != nil {
				return nil, err
			}
			return &CouplingConstantWithRadix{Value: val, RetType: x.GetType()}, nil
		}
		var newSf Expression
		if x.FuncName.L == ast.Cast {
			newSf = BuildCastFunction(x.GetCtx(), newArgs[0], x.RetType)
		} else {
			newSf = NewFunctionInternal(x.GetCtx(), x.FuncName.L, x.GetType(), newArgs...)
		}
		return newSf, nil
	case *CorrelatedDeferredCauset:
		return &CouplingConstantWithRadix{Value: *x.Data, RetType: x.GetType()}, nil
	case *CouplingConstantWithRadix:
		if x.DeferredExpr != nil {
			newExpr := FoldCouplingConstantWithRadix(x)
			return &CouplingConstantWithRadix{Value: newExpr.(*CouplingConstantWithRadix).Value, RetType: x.GetType()}, nil
		}
	}
	return expr, nil
}

// timeZone2Duration converts timezone whose format should satisfy the regular condition
// `(^(+|-)(0?[0-9]|1[0-2]):[0-5]?\d$)|(^+13:00$)` to time.Duration.
func timeZone2Duration(tz string) time.Duration {
	sign := 1
	if strings.HasPrefix(tz, "-") {
		sign = -1
	}

	i := strings.Index(tz, ":")
	h, err := strconv.Atoi(tz[1:i])
	terror.Log(err)
	m, err := strconv.Atoi(tz[i+1:])
	terror.Log(err)
	return time.Duration(sign) * (time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
}

var logicalOps = map[string]struct{}{
	ast.LT:                 {},
	ast.GE:                 {},
	ast.GT:                 {},
	ast.LE:                 {},
	ast.EQ:                 {},
	ast.NE:                 {},
	ast.UnaryNot:           {},
	ast.LogicAnd:           {},
	ast.LogicOr:            {},
	ast.LogicXor:           {},
	ast.In:                 {},
	ast.IsNull:             {},
	ast.IsTruthWithoutNull: {},
	ast.IsFalsity:          {},
	ast.Like:               {},
}

var oppositeOp = map[string]string{
	ast.LT:       ast.GE,
	ast.GE:       ast.LT,
	ast.GT:       ast.LE,
	ast.LE:       ast.GT,
	ast.EQ:       ast.NE,
	ast.NE:       ast.EQ,
	ast.LogicOr:  ast.LogicAnd,
	ast.LogicAnd: ast.LogicOr,
}

// a op b is equal to b symmetriINTERLOCK a
var symmetriINTERLOCK = map[opcode.Op]opcode.Op{
	opcode.LT:     opcode.GT,
	opcode.GE:     opcode.LE,
	opcode.GT:     opcode.LT,
	opcode.LE:     opcode.GE,
	opcode.EQ:     opcode.EQ,
	opcode.NE:     opcode.NE,
	opcode.NullEQ: opcode.NullEQ,
}

func pushNotAcrossArgs(ctx stochastikctx.Context, exprs []Expression, not bool) ([]Expression, bool) {
	newExprs := make([]Expression, 0, len(exprs))
	flag := false
	for _, expr := range exprs {
		newExpr, changed := pushNotAcrossExpr(ctx, expr, not)
		flag = changed || flag
		newExprs = append(newExprs, newExpr)
	}
	return newExprs, flag
}

// pushNotAcrossExpr try to eliminate the NOT expr in expression tree.
// Input `not` indicates whether there's a `NOT` be pushed down.
// Output `changed` indicates whether the output expression differs from the
// input `expr` because of the pushed-down-not.
func pushNotAcrossExpr(ctx stochastikctx.Context, expr Expression, not bool) (_ Expression, changed bool) {
	if f, ok := expr.(*ScalarFunction); ok {
		switch f.FuncName.L {
		case ast.UnaryNot:
			child, err := wrapWithIsTrue(ctx, true, f.GetArgs()[0], true)
			if err != nil {
				return expr, false
			}
			var childExpr Expression
			childExpr, changed = pushNotAcrossExpr(f.GetCtx(), child, !not)
			if !changed && !not {
				return expr, false
			}
			return childExpr, true
		case ast.LT, ast.GE, ast.GT, ast.LE, ast.EQ, ast.NE:
			if not {
				return NewFunctionInternal(f.GetCtx(), oppositeOp[f.FuncName.L], f.GetType(), f.GetArgs()...), true
			}
			newArgs, changed := pushNotAcrossArgs(f.GetCtx(), f.GetArgs(), false)
			if !changed {
				return f, false
			}
			return NewFunctionInternal(f.GetCtx(), f.FuncName.L, f.GetType(), newArgs...), true
		case ast.LogicAnd, ast.LogicOr:
			var (
				newArgs []Expression
				changed bool
			)
			funcName := f.FuncName.L
			if not {
				newArgs, _ = pushNotAcrossArgs(f.GetCtx(), f.GetArgs(), true)
				funcName = oppositeOp[f.FuncName.L]
				changed = true
			} else {
				newArgs, changed = pushNotAcrossArgs(f.GetCtx(), f.GetArgs(), false)
			}
			if !changed {
				return f, false
			}
			return NewFunctionInternal(f.GetCtx(), funcName, f.GetType(), newArgs...), true
		}
	}
	if not {
		expr = NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(allegrosql.TypeTiny), expr)
	}
	return expr, not
}

// PushDownNot pushes the `not` function down to the expression's arguments.
func PushDownNot(ctx stochastikctx.Context, expr Expression) Expression {
	newExpr, _ := pushNotAcrossExpr(ctx, expr, false)
	return newExpr
}

// Contains tests if `exprs` contains `e`.
func Contains(exprs []Expression, e Expression) bool {
	for _, expr := range exprs {
		if e == expr {
			return true
		}
	}
	return false
}

// ExtractFiltersFromDNFs checks whether the cond is DNF. If so, it will get the extracted part and the remained part.
// The original DNF will be replaced by the remained part or just be deleted if remained part is nil.
// And the extracted part will be appended to the end of the orignal slice.
func ExtractFiltersFromDNFs(ctx stochastikctx.Context, conditions []Expression) []Expression {
	var allExtracted []Expression
	for i := len(conditions) - 1; i >= 0; i-- {
		if sf, ok := conditions[i].(*ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			extracted, remained := extractFiltersFromDNF(ctx, sf)
			allExtracted = append(allExtracted, extracted...)
			if remained == nil {
				conditions = append(conditions[:i], conditions[i+1:]...)
			} else {
				conditions[i] = remained
			}
		}
	}
	return append(conditions, allExtracted...)
}

// extractFiltersFromDNF extracts the same condition that occurs in every DNF item and remove them from dnf leaves.
func extractFiltersFromDNF(ctx stochastikctx.Context, dnfFunc *ScalarFunction) ([]Expression, Expression) {
	dnfItems := FlattenDNFConditions(dnfFunc)
	sc := ctx.GetStochaseinstein_dbars().StmtCtx
	codeMap := make(map[string]int)
	hashcode2Expr := make(map[string]Expression)
	for i, dnfItem := range dnfItems {
		innerMap := make(map[string]struct{})
		cnfItems := SplitCNFItems(dnfItem)
		for _, cnfItem := range cnfItems {
			code := cnfItem.HashCode(sc)
			if i == 0 {
				codeMap[string(code)] = 1
				hashcode2Expr[string(code)] = cnfItem
			} else if _, ok := codeMap[string(code)]; ok {
				// We need this check because there may be the case like `select * from t, t1 where (t.a=t1.a and t.a=t1.a) or (something).
				// We should make sure that the two `t.a=t1.a` contributes only once.
				// TODO: do this out of this function.
				if _, ok = innerMap[string(code)]; !ok {
					codeMap[string(code)]++
					innerMap[string(code)] = struct{}{}
				}
			}
		}
	}
	// We should make sure that this item occurs in every DNF item.
	for hashcode, cnt := range codeMap {
		if cnt < len(dnfItems) {
			delete(hashcode2Expr, hashcode)
		}
	}
	if len(hashcode2Expr) == 0 {
		return nil, dnfFunc
	}
	newDNFItems := make([]Expression, 0, len(dnfItems))
	onlyNeedExtracted := false
	for _, dnfItem := range dnfItems {
		cnfItems := SplitCNFItems(dnfItem)
		newCNFItems := make([]Expression, 0, len(cnfItems))
		for _, cnfItem := range cnfItems {
			code := cnfItem.HashCode(sc)
			_, ok := hashcode2Expr[string(code)]
			if !ok {
				newCNFItems = append(newCNFItems, cnfItem)
			}
		}
		// If the extracted part is just one leaf of the DNF expression. Then the value of the total DNF expression is
		// always the same with the value of the extracted part.
		if len(newCNFItems) == 0 {
			onlyNeedExtracted = true
			break
		}
		newDNFItems = append(newDNFItems, ComposeCNFCondition(ctx, newCNFItems...))
	}
	extractedExpr := make([]Expression, 0, len(hashcode2Expr))
	for _, expr := range hashcode2Expr {
		extractedExpr = append(extractedExpr, expr)
	}
	if onlyNeedExtracted {
		return extractedExpr, nil
	}
	return extractedExpr, ComposeDNFCondition(ctx, newDNFItems...)
}

// DeriveRelaxedFiltersFromDNF given a DNF expression, derive a relaxed DNF expression which only contains defCausumns
// in specified schemaReplicant; the derived expression is a superset of original expression, i.e, any tuple satisfying
// the original expression must satisfy the derived expression. Return nil when the derived expression is universal set.
// A running example is: for schemaReplicant of t1, `(t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2)` would be derived as
// `t1.a=1 or t1.a=2`, while `t1.a=1 or t2.a=1` would get nil.
func DeriveRelaxedFiltersFromDNF(expr Expression, schemaReplicant *Schema) Expression {
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.LogicOr {
		return nil
	}
	ctx := sf.GetCtx()
	dnfItems := FlattenDNFConditions(sf)
	newDNFItems := make([]Expression, 0, len(dnfItems))
	for _, dnfItem := range dnfItems {
		cnfItems := SplitCNFItems(dnfItem)
		newCNFItems := make([]Expression, 0, len(cnfItems))
		for _, cnfItem := range cnfItems {
			if itemSF, ok := cnfItem.(*ScalarFunction); ok && itemSF.FuncName.L == ast.LogicOr {
				relaxedCNFItem := DeriveRelaxedFiltersFromDNF(cnfItem, schemaReplicant)
				if relaxedCNFItem != nil {
					newCNFItems = append(newCNFItems, relaxedCNFItem)
				}
				// If relaxed expression for embedded DNF is universal set, just drop this CNF item
				continue
			}
			// This cnfItem must be simple expression now
			// If it cannot be fully covered by schemaReplicant, just drop this CNF item
			if ExprFromSchema(cnfItem, schemaReplicant) {
				newCNFItems = append(newCNFItems, cnfItem)
			}
		}
		// If this DNF item involves no defCausumn of specified schemaReplicant, the relaxed expression must be universal set
		if len(newCNFItems) == 0 {
			return nil
		}
		newDNFItems = append(newDNFItems, ComposeCNFCondition(ctx, newCNFItems...))
	}
	return ComposeDNFCondition(ctx, newDNFItems...)
}

// GetEventLen gets the length if the func is event, returns 1 if not event.
func GetEventLen(e Expression) int {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.EventFunc {
		return len(f.GetArgs())
	}
	return 1
}

// CheckArgsNotMultiDeferredCausetEvent checks the args are not multi-defCausumn event.
func CheckArgsNotMultiDeferredCausetEvent(args ...Expression) error {
	for _, arg := range args {
		if GetEventLen(arg) != 1 {
			return ErrOperandDeferredCausets.GenWithStackByArgs(1)
		}
	}
	return nil
}

// GetFuncArg gets the argument of the function at idx.
func GetFuncArg(e Expression, idx int) Expression {
	if f, ok := e.(*ScalarFunction); ok {
		return f.GetArgs()[idx]
	}
	return nil
}

// PopEventFirstArg pops the first element and returns the rest of event.
// e.g. After this function (1, 2, 3) becomes (2, 3).
func PopEventFirstArg(ctx stochastikctx.Context, e Expression) (ret Expression, err error) {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.EventFunc {
		args := f.GetArgs()
		if len(args) == 2 {
			return args[1], nil
		}
		ret, err = NewFunction(ctx, ast.EventFunc, f.GetType(), args[1:]...)
		return ret, err
	}
	return
}

// exprStack is a stack of expressions.
type exprStack struct {
	stack []Expression
}

// pop pops an expression from the stack.
func (s *exprStack) pop() Expression {
	if s.len() == 0 {
		return nil
	}
	lastIdx := s.len() - 1
	expr := s.stack[lastIdx]
	s.stack = s.stack[:lastIdx]
	return expr
}

// popN pops n expressions from the stack.
// If n greater than stack length or n is negative, it pops all the expressions.
func (s *exprStack) popN(n int) []Expression {
	if n > s.len() || n < 0 {
		n = s.len()
	}
	idx := s.len() - n
	exprs := s.stack[idx:]
	s.stack = s.stack[:idx]
	return exprs
}

// push pushes one expression to the stack.
func (s *exprStack) push(expr Expression) {
	s.stack = append(s.stack, expr)
}

// len returns the length of th stack.
func (s *exprStack) len() int {
	return len(s.stack)
}

// CausetToCouplingConstantWithRadix generates a CouplingConstantWithRadix expression from a Causet.
func CausetToCouplingConstantWithRadix(d types.Causet, tp byte) *CouplingConstantWithRadix {
	return &CouplingConstantWithRadix{Value: d, RetType: types.NewFieldType(tp)}
}

// ParamMarkerExpression generate a getparam function expression.
func ParamMarkerExpression(ctx stochastikctx.Context, v *driver.ParamMarkerExpr) (Expression, error) {
	useCache := ctx.GetStochaseinstein_dbars().StmtCtx.UseCache
	isPointExec := ctx.GetStochaseinstein_dbars().StmtCtx.PointExec
	tp := types.NewFieldType(allegrosql.TypeUnspecified)
	types.DefaultParamTypeForValue(v.GetValue(), tp)
	value := &CouplingConstantWithRadix{Value: v.Causet, RetType: tp}
	if useCache || isPointExec {
		value.ParamMarker = &ParamMarker{
			order: v.Order,
			ctx:   ctx,
		}
	}
	return value, nil
}

// DisableParseJSONFlag4Expr disables ParseToJSONFlag for `expr` except DeferredCauset.
// We should not *PARSE* a string as JSON under some scenarios. ParseToJSONFlag
// is 0 for JSON defCausumn yet(as well as JSON correlated defCausumn), so we can skip
// it. Moreover, DeferredCauset.RetType refers to the schemareplicant, if we modify it, data
// race may happen if another goroutine read from the schemareplicant at the same
// time.
func DisableParseJSONFlag4Expr(expr Expression) {
	if _, isDeferredCauset := expr.(*DeferredCauset); isDeferredCauset {
		return
	}
	if _, isCorDefCaus := expr.(*CorrelatedDeferredCauset); isCorDefCaus {
		return
	}
	expr.GetType().Flag &= ^allegrosql.ParseToJSONFlag
}

// ConstructPositionExpr constructs PositionExpr with the given ParamMarkerExpr.
func ConstructPositionExpr(p *driver.ParamMarkerExpr) *ast.PositionExpr {
	return &ast.PositionExpr{P: p}
}

// PosFromPositionExpr generates a position value from PositionExpr.
func PosFromPositionExpr(ctx stochastikctx.Context, v *ast.PositionExpr) (int, bool, error) {
	if v.P == nil {
		return v.N, false, nil
	}
	value, err := ParamMarkerExpression(ctx, v.P.(*driver.ParamMarkerExpr))
	if err != nil {
		return 0, true, err
	}
	pos, isNull, err := GetIntFromCouplingConstantWithRadix(ctx, value)
	if err != nil || isNull {
		return 0, true, err
	}
	return pos, false, nil
}

// GetStringFromCouplingConstantWithRadix gets a string value from the CouplingConstantWithRadix expression.
func GetStringFromCouplingConstantWithRadix(ctx stochastikctx.Context, value Expression) (string, bool, error) {
	con, ok := value.(*CouplingConstantWithRadix)
	if !ok {
		err := errors.Errorf("Not a CouplingConstantWithRadix expression %+v", value)
		return "", true, err
	}
	str, isNull, err := con.EvalString(ctx, chunk.Event{})
	if err != nil || isNull {
		return "", true, err
	}
	return str, false, nil
}

// GetIntFromCouplingConstantWithRadix gets an interger value from the CouplingConstantWithRadix expression.
func GetIntFromCouplingConstantWithRadix(ctx stochastikctx.Context, value Expression) (int, bool, error) {
	str, isNull, err := GetStringFromCouplingConstantWithRadix(ctx, value)
	if err != nil || isNull {
		return 0, true, err
	}
	intNum, err := strconv.Atoi(str)
	if err != nil {
		return 0, true, nil
	}
	return intNum, false, nil
}

// BuildNotNullExpr wraps up `not(isnull())` for given expression.
func BuildNotNullExpr(ctx stochastikctx.Context, expr Expression) Expression {
	isNull := NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(allegrosql.TypeTiny), expr)
	notNull := NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(allegrosql.TypeTiny), isNull)
	return notNull
}

// IsRuntimeConstExpr checks if a expr can be treated as a constant in **executor**.
func IsRuntimeConstExpr(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return false
		}
		for _, arg := range x.GetArgs() {
			if !IsRuntimeConstExpr(arg) {
				return false
			}
		}
		return true
	case *DeferredCauset:
		return false
	case *CouplingConstantWithRadix, *CorrelatedDeferredCauset:
		return true
	}
	return false
}

// IsMublockEffectsExpr checks if expr contains function which is mublock or has side effects.
func IsMublockEffectsExpr(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := mublockEffectsFunctions[x.FuncName.L]; ok {
			return true
		}
		for _, arg := range x.GetArgs() {
			if IsMublockEffectsExpr(arg) {
				return true
			}
		}
	case *DeferredCauset:
	case *CouplingConstantWithRadix:
		if x.DeferredExpr != nil {
			return IsMublockEffectsExpr(x.DeferredExpr)
		}
	}
	return false
}

// RemoveDupExprs removes identical exprs. Not that if expr contains functions which
// are mublock or have side effects, we cannot remove it even if it has duplicates;
// if the plan is going to be cached, we cannot remove expressions containing `?` neither.
func RemoveDupExprs(ctx stochastikctx.Context, exprs []Expression) []Expression {
	res := make([]Expression, 0, len(exprs))
	exists := make(map[string]struct{}, len(exprs))
	sc := ctx.GetStochaseinstein_dbars().StmtCtx
	for _, expr := range exprs {
		if ContainMublockConst(ctx, []Expression{expr}) {
			res = append(res, expr)
			continue
		}
		key := string(expr.HashCode(sc))
		if _, ok := exists[key]; !ok || IsMublockEffectsExpr(expr) {
			res = append(res, expr)
			exists[key] = struct{}{}
		}
	}
	return res
}

// GetUint64FromCouplingConstantWithRadix gets a uint64 from constant expression.
func GetUint64FromCouplingConstantWithRadix(expr Expression) (uint64, bool, bool) {
	con, ok := expr.(*CouplingConstantWithRadix)
	if !ok {
		logutil.BgLogger().Warn("not a constant expression", zap.String("expression", expr.ExplainInfo()))
		return 0, false, false
	}
	dt := con.Value
	if con.ParamMarker != nil {
		dt = con.ParamMarker.GetUserVar()
	} else if con.DeferredExpr != nil {
		var err error
		dt, err = con.DeferredExpr.Eval(chunk.Event{})
		if err != nil {
			logutil.BgLogger().Warn("eval deferred expr failed", zap.Error(err))
			return 0, false, false
		}
	}
	switch dt.HoTT() {
	case types.HoTTNull:
		return 0, true, true
	case types.HoTTInt64:
		val := dt.GetInt64()
		if val < 0 {
			return 0, false, false
		}
		return uint64(val), false, true
	case types.HoTTUint64:
		return dt.GetUint64(), false, true
	}
	return 0, false, false
}

// ContainVirtualDeferredCauset checks if the expressions contain a virtual defCausumn
func ContainVirtualDeferredCauset(exprs []Expression) bool {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *DeferredCauset:
			if v.VirtualExpr != nil {
				return true
			}
		case *ScalarFunction:
			if ContainVirtualDeferredCauset(v.GetArgs()) {
				return true
			}
		}
	}
	return false
}

// ContainMublockConst checks if the expressions contain a lazy constant.
func ContainMublockConst(ctx stochastikctx.Context, exprs []Expression) bool {
	// Treat all constants immublock if plan cache is not enabled for this query.
	if !ctx.GetStochaseinstein_dbars().StmtCtx.UseCache {
		return false
	}
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *CouplingConstantWithRadix:
			if v.ParamMarker != nil || v.DeferredExpr != nil {
				return true
			}
		case *ScalarFunction:
			if ContainMublockConst(ctx, v.GetArgs()) {
				return true
			}
		}
	}
	return false
}

const (
	_   = iota
	kib = 1 << (10 * iota)
	mib = 1 << (10 * iota)
	gib = 1 << (10 * iota)
	tib = 1 << (10 * iota)
	pib = 1 << (10 * iota)
	eib = 1 << (10 * iota)
)

const (
	nano    = 1
	micro   = 1000 * nano
	milli   = 1000 * micro
	sec     = 1000 * milli
	min     = 60 * sec
	hour    = 60 * min
	dayTime = 24 * hour
)

// GetFormatBytes convert byte count to value with units.
func GetFormatBytes(bytes float64) string {
	var divisor float64
	var unit string

	bytesAbs := math.Abs(bytes)
	if bytesAbs >= eib {
		divisor = eib
		unit = "EiB"
	} else if bytesAbs >= pib {
		divisor = pib
		unit = "PiB"
	} else if bytesAbs >= tib {
		divisor = tib
		unit = "TiB"
	} else if bytesAbs >= gib {
		divisor = gib
		unit = "GiB"
	} else if bytesAbs >= mib {
		divisor = mib
		unit = "MiB"
	} else if bytesAbs >= kib {
		divisor = kib
		unit = "KiB"
	} else {
		divisor = 1
		unit = "bytes"
	}

	if divisor == 1 {
		return strconv.FormatFloat(bytes, 'f', 0, 64) + " " + unit
	}
	value := float64(bytes) / divisor
	if math.Abs(value) >= 100000.0 {
		return strconv.FormatFloat(value, 'e', 2, 64) + " " + unit
	}
	return strconv.FormatFloat(value, 'f', 2, 64) + " " + unit
}

// GetFormatNanoTime convert time in nanoseconds to value with units.
func GetFormatNanoTime(time float64) string {
	var divisor float64
	var unit string

	timeAbs := math.Abs(time)
	if timeAbs >= dayTime {
		divisor = dayTime
		unit = "d"
	} else if timeAbs >= hour {
		divisor = hour
		unit = "h"
	} else if timeAbs >= min {
		divisor = min
		unit = "min"
	} else if timeAbs >= sec {
		divisor = sec
		unit = "s"
	} else if timeAbs >= milli {
		divisor = milli
		unit = "ms"
	} else if timeAbs >= micro {
		divisor = micro
		unit = "us"
	} else {
		divisor = 1
		unit = "ns"
	}

	if divisor == 1 {
		return strconv.FormatFloat(time, 'f', 0, 64) + " " + unit
	}
	value := float64(time) / divisor
	if math.Abs(value) >= 100000.0 {
		return strconv.FormatFloat(value, 'e', 2, 64) + " " + unit
	}
	return strconv.FormatFloat(value, 'f', 2, 64) + " " + unit
}
