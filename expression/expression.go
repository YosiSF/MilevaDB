// INTERLOCKyright 2020 WHTCORPS INC, Inc.
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
	goJSON "encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/opcode"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/generatedexpr"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	"go.uber.org/zap"
)

// These are byte flags used for `HashCode()`.
const (
	constantFlag       byte = 0
	defCausumnFlag     byte = 1
	scalarFunctionFlag byte = 3
)

// EvalAstExpr evaluates ast expression directly.
var EvalAstExpr func(sctx stochastikctx.Context, expr ast.ExprNode) (types.Causet, error)

// RewriteAstExpr rewrites ast expression directly.
var RewriteAstExpr func(sctx stochastikctx.Context, expr ast.ExprNode, schemaReplicant *Schema, names types.NameSlice) (Expression, error)

// VecExpr contains all vectorized evaluation methods.
type VecExpr interface {
	// Vectorized returns if this expression supports vectorized evaluation.
	Vectorized() bool

	// VecEvalInt evaluates this expression in a vectorized manner.
	VecEvalInt(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error

	// VecEvalReal evaluates this expression in a vectorized manner.
	VecEvalReal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error

	// VecEvalString evaluates this expression in a vectorized manner.
	VecEvalString(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error

	// VecEvalDecimal evaluates this expression in a vectorized manner.
	VecEvalDecimal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error

	// VecEvalTime evaluates this expression in a vectorized manner.
	VecEvalTime(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error

	// VecEvalDuration evaluates this expression in a vectorized manner.
	VecEvalDuration(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error

	// VecEvalJSON evaluates this expression in a vectorized manner.
	VecEvalJSON(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error
}

// ReverseExpr contains all resersed evaluation methods.
type ReverseExpr interface {
	// SupportReverseEval checks whether the builtinFunc support reverse evaluation.
	SupportReverseEval() bool

	// ReverseEval evaluates the only one defCausumn value with given function result.
	ReverseEval(sc *stmtctx.StatementContext, res types.Causet, rType types.RoundingType) (val types.Causet, err error)
}

// Expression represents all scalar expression in ALLEGROALLEGROSQL.
type Expression interface {
	fmt.Stringer
	goJSON.Marshaler
	VecExpr
	ReverseExpr
	DefCauslationInfo

	// Eval evaluates an expression through a event.
	Eval(event chunk.Event) (types.Causet, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(ctx stochastikctx.Context, event chunk.Event) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	EvalReal(ctx stochastikctx.Context, event chunk.Event) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(ctx stochastikctx.Context, event chunk.Event) (val string, isNull bool, err error)

	// EvalDecimal returns the decimal representation of expression.
	EvalDecimal(ctx stochastikctx.Context, event chunk.Event) (val *types.MyDecimal, isNull bool, err error)

	// EvalTime returns the DATE/DATETIME/TIMESTAMP representation of expression.
	EvalTime(ctx stochastikctx.Context, event chunk.Event) (val types.Time, isNull bool, err error)

	// EvalDuration returns the duration representation of expression.
	EvalDuration(ctx stochastikctx.Context, event chunk.Event) (val types.Duration, isNull bool, err error)

	// EvalJSON returns the JSON representation of expression.
	EvalJSON(ctx stochastikctx.Context, event chunk.Event) (val json.BinaryJSON, isNull bool, err error)

	// GetType gets the type that the expression returns.
	GetType() *types.FieldType

	// Clone INTERLOCKies an expression totally.
	Clone() Expression

	// Equal checks whether two expressions are equal.
	Equal(ctx stochastikctx.Context, e Expression) bool

	// IsCorrelated checks if this expression has correlated key.
	IsCorrelated() bool

	// ConstItem checks if this expression is constant item, regardless of query evaluation state.
	// An expression is constant item if it:
	// refers no blocks.
	// refers no correlated defCausumn.
	// refers no subqueries that refers any blocks.
	// refers no non-deterministic functions.
	// refers no statement parameters.
	// refers no param markers when prepare plan cache is enabled.
	ConstItem(sc *stmtctx.StatementContext) bool

	// Decorrelate try to decorrelate the expression by schemaReplicant.
	Decorrelate(schemaReplicant *Schema) Expression

	// ResolveIndices resolves indices by the given schemaReplicant. It will INTERLOCKy the original expression and return the INTERLOCKied one.
	ResolveIndices(schemaReplicant *Schema) (Expression, error)

	// resolveIndices is called inside the `ResolveIndices` It will perform on the expression itself.
	resolveIndices(schemaReplicant *Schema) error

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string

	// ExplainNormalizedInfo returns operator normalized information for generating digest.
	ExplainNormalizedInfo() string

	// HashCode creates the hashcode for expression which can be used to identify itself from other expression.
	// It generated as the following:
	// Constant: ConstantFlag+encoded value
	// DeferredCauset: DeferredCausetFlag+encoded value
	// ScalarFunction: SFFlag+encoded function name + encoded arg_1 + encoded arg_2 + ...
	HashCode(sc *stmtctx.StatementContext) []byte
}

// CNFExprs stands for a CNF expression.
type CNFExprs []Expression

// Clone clones itself.
func (e CNFExprs) Clone() CNFExprs {
	cnf := make(CNFExprs, 0, len(e))
	for _, expr := range e {
		cnf = append(cnf, expr.Clone())
	}
	return cnf
}

// Shallow makes a shallow INTERLOCKy of itself.
func (e CNFExprs) Shallow() CNFExprs {
	cnf := make(CNFExprs, 0, len(e))
	cnf = append(cnf, e...)
	return cnf
}

func isDeferredCausetInOperand(c *DeferredCauset) bool {
	return c.InOperand
}

// IsEQCondFromIn checks if an expression is equal condition converted from `[not] in (subq)`.
func IsEQCondFromIn(expr Expression) bool {
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return false
	}
	defcaus := make([]*DeferredCauset, 0, 1)
	defcaus = ExtractDeferredCausetsFromExpressions(defcaus, sf.GetArgs(), isDeferredCausetInOperand)
	return len(defcaus) > 0
}

// HandleOverflowOnSelection handles Overflow errors when evaluating selection filters.
// We should ignore overflow errors when evaluating selection conditions:
//		INSERT INTO t VALUES ("999999999999999999");
//		SELECT * FROM t WHERE v;
func HandleOverflowOnSelection(sc *stmtctx.StatementContext, val int64, err error) (int64, error) {
	if sc.InSelectStmt && err != nil && types.ErrOverflow.Equal(err) {
		return -1, nil
	}
	return val, err
}

// EvalBool evaluates expression list to a boolean value. The first returned value
// indicates bool result of the expression list, the second returned value indicates
// whether the result of the expression list is null, it can only be true when the
// first returned values is false.
func EvalBool(ctx stochastikctx.Context, exprList CNFExprs, event chunk.Event) (bool, bool, error) {
	hasNull := false
	for _, expr := range exprList {
		data, err := expr.Eval(event)
		if err != nil {
			return false, false, err
		}
		if data.IsNull() {
			// For queries like `select a in (select a from s where t.b = s.b) from t`,
			// if result of `t.a = s.a` is null, we cannot return immediately until
			// we have checked if `t.b = s.b` is null or false, because it means
			// subquery is empty, and we should return false as the result of the whole
			// exprList in that case, instead of null.
			if !IsEQCondFromIn(expr) {
				return false, false, nil
			}
			hasNull = true
			continue
		}

		i, err := data.ToBool(ctx.GetStochastikVars().StmtCtx)
		if err != nil {
			i, err = HandleOverflowOnSelection(ctx.GetStochastikVars().StmtCtx, i, err)
			if err != nil {
				return false, false, err

			}
		}
		if i == 0 {
			return false, false, nil
		}
	}
	if hasNull {
		return false, true, nil
	}
	return true, false, nil
}

var (
	defaultChunkSize = 1024
	selPool          = sync.Pool{
		New: func() interface{} {
			return make([]int, defaultChunkSize)
		},
	}
	zeroPool = sync.Pool{
		New: func() interface{} {
			return make([]int8, defaultChunkSize)
		},
	}
)

func allocSelSlice(n int) []int {
	if n > defaultChunkSize {
		return make([]int, n)
	}
	return selPool.Get().([]int)
}

func deallocateSelSlice(sel []int) {
	if cap(sel) <= defaultChunkSize {
		selPool.Put(sel)
	}
}

func allocZeroSlice(n int) []int8 {
	if n > defaultChunkSize {
		return make([]int8, n)
	}
	return zeroPool.Get().([]int8)
}

func deallocateZeroSlice(isZero []int8) {
	if cap(isZero) <= defaultChunkSize {
		zeroPool.Put(isZero)
	}
}

// VecEvalBool does the same thing as EvalBool but it works in a vectorized manner.
func VecEvalBool(ctx stochastikctx.Context, exprList CNFExprs, input *chunk.Chunk, selected, nulls []bool) ([]bool, []bool, error) {
	// If input.Sel() != nil, we will call input.SetSel(nil) to clear the sel slice in input chunk.
	// After the function finished, then we reset the input.Sel().
	// The caller will handle the input.Sel() and selected slices.
	defer input.SetSel(input.Sel())
	input.SetSel(nil)

	n := input.NumEvents()
	selected = selected[:0]
	nulls = nulls[:0]
	for i := 0; i < n; i++ {
		selected = append(selected, false)
		nulls = append(nulls, false)
	}

	sel := allocSelSlice(n)
	defer deallocateSelSlice(sel)
	sel = sel[:0]
	for i := 0; i < n; i++ {
		sel = append(sel, i)
	}
	input.SetSel(sel)

	// In isZero slice, -1 means Null, 0 means zero, 1 means not zero
	isZero := allocZeroSlice(n)
	defer deallocateZeroSlice(isZero)
	for _, expr := range exprList {
		eType := expr.GetType().EvalType()
		buf, err := globalDeferredCausetSlabPredictor.get(eType, n)
		if err != nil {
			return nil, nil, err
		}

		if err := EvalExpr(ctx, expr, input, buf); err != nil {
			return nil, nil, err
		}

		err = toBool(ctx.GetStochastikVars().StmtCtx, eType, buf, sel, isZero)
		if err != nil {
			return nil, nil, err
		}

		j := 0
		isEQCondFromIn := IsEQCondFromIn(expr)
		for i := range sel {
			if isZero[i] == -1 {
				if eType != types.ETInt && !isEQCondFromIn {
					continue
				}
				// In this case, we set this event to null and let it pass this filter.
				// The null flag may be set to false later by other expressions in some cases.
				nulls[sel[i]] = true
				sel[j] = sel[i]
				j++
				continue
			}

			if isZero[i] == 0 {
				continue
			}
			sel[j] = sel[i] // this event passes this filter
			j++
		}
		sel = sel[:j]
		input.SetSel(sel)
		globalDeferredCausetSlabPredictor.put(buf)
	}

	for _, i := range sel {
		if !nulls[i] {
			selected[i] = true
		}
	}

	return selected, nulls, nil
}

func toBool(sc *stmtctx.StatementContext, eType types.EvalType, buf *chunk.DeferredCauset, sel []int, isZero []int8) error {
	switch eType {
	case types.ETInt:
		i64s := buf.Int64s()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if i64s[i] == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETReal:
		f64s := buf.Float64s()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if f64s[i] == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETDuration:
		d64s := buf.GoDurations()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if d64s[i] == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		t64s := buf.Times()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if t64s[i].IsZero() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETString:
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				iVal, err := types.StrToFloat(sc, buf.GetString(i), false)
				if err != nil {
					return err
				}
				if iVal == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETDecimal:
		d64s := buf.Decimals()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if d64s[i].IsZero() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETJson:
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if buf.GetJSON(i).IsZero() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	}
	return nil
}

// EvalExpr evaluates this expr according to its type.
// And it selects the method for evaluating expression based on
// the environment variables and whether the expression can be vectorized.
func EvalExpr(ctx stochastikctx.Context, expr Expression, input *chunk.Chunk, result *chunk.DeferredCauset) (err error) {
	evalType := expr.GetType().EvalType()
	if expr.Vectorized() && ctx.GetStochastikVars().EnableVectorizedExpression {
		switch evalType {
		case types.ETInt:
			err = expr.VecEvalInt(ctx, input, result)
		case types.ETReal:
			err = expr.VecEvalReal(ctx, input, result)
		case types.ETDuration:
			err = expr.VecEvalDuration(ctx, input, result)
		case types.ETDatetime, types.ETTimestamp:
			err = expr.VecEvalTime(ctx, input, result)
		case types.ETString:
			err = expr.VecEvalString(ctx, input, result)
		case types.ETJson:
			err = expr.VecEvalJSON(ctx, input, result)
		case types.ETDecimal:
			err = expr.VecEvalDecimal(ctx, input, result)
		default:
			err = errors.New(fmt.Sprintf("invalid eval type %v", expr.GetType().EvalType()))
		}
	} else {
		ind, n := 0, input.NumEvents()
		iter := chunk.NewIterator4Chunk(input)
		switch evalType {
		case types.ETInt:
			result.ResizeInt64(n, false)
			i64s := result.Int64s()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalInt(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					i64s[ind] = value
				}
				ind++
			}
		case types.ETReal:
			result.ResizeFloat64(n, false)
			f64s := result.Float64s()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalReal(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					f64s[ind] = value
				}
				ind++
			}
		case types.ETDuration:
			result.ResizeGoDuration(n, false)
			d64s := result.GoDurations()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalDuration(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					d64s[ind] = value.Duration
				}
				ind++
			}
		case types.ETDatetime, types.ETTimestamp:
			result.ResizeTime(n, false)
			t64s := result.Times()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalTime(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					t64s[ind] = value
				}
				ind++
			}
		case types.ETString:
			result.ReserveString(n)
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalString(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendString(value)
				}
			}
		case types.ETJson:
			result.ReserveJSON(n)
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalJSON(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendJSON(value)
				}
			}
		case types.ETDecimal:
			result.ResizeDecimal(n, false)
			d64s := result.Decimals()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalDecimal(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					d64s[ind] = *value
				}
				ind++
			}
		default:
			err = errors.New(fmt.Sprintf("invalid eval type %v", expr.GetType().EvalType()))
		}
	}
	return
}

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(ctx stochastikctx.Context, conditions []Expression, funcName string) Expression {
	length := len(conditions)
	if length == 0 {
		return nil
	}
	if length == 1 {
		return conditions[0]
	}
	expr := NewFunctionInternal(ctx, funcName,
		types.NewFieldType(allegrosql.TypeTiny),
		composeConditionWithBinaryOp(ctx, conditions[:length/2], funcName),
		composeConditionWithBinaryOp(ctx, conditions[length/2:], funcName))
	return expr
}

// ComposeCNFCondition composes CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func ComposeCNFCondition(ctx stochastikctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(ctx stochastikctx.Context, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicOr)
}

func extractBinaryOpItems(conditions *ScalarFunction, funcName string) []Expression {
	var ret []Expression
	for _, arg := range conditions.GetArgs() {
		if sf, ok := arg.(*ScalarFunction); ok && sf.FuncName.L == funcName {
			ret = append(ret, extractBinaryOpItems(sf, funcName)...)
		} else {
			ret = append(ret, arg)
		}
	}
	return ret
}

// FlattenDNFConditions extracts DNF expression's leaf item.
// e.g. or(or(a=1, a=2), or(a=3, a=4)), we'll get [a=1, a=2, a=3, a=4].
func FlattenDNFConditions(DNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(DNFCondition, ast.LogicOr)
}

// FlattenCNFConditions extracts CNF expression's leaf item.
// e.g. and(and(a>1, a>2), and(a>3, a>4)), we'll get [a>1, a>2, a>3, a>4].
func FlattenCNFConditions(CNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(CNFCondition, ast.LogicAnd)
}

// Assignment represents a set assignment in UFIDelate, such as
// UFIDelate t set c1 = hex(12), c2 = c3 where c2 = 1
type Assignment struct {
	DefCaus *DeferredCauset
	// DefCausName indicates its original defCausumn name in block schemaReplicant. It's used for outputting helping message when executing meets some errors.
	DefCausName perceptron.CIStr
	Expr        Expression
}

// VarAssignment represents a variable assignment in Set, such as set global a = 1.
type VarAssignment struct {
	Name        string
	Expr        Expression
	IsDefault   bool
	IsGlobal    bool
	IsSystem    bool
	ExtendValue *Constant
}

// splitNormalFormItems split CNF(conjunctive normal form) like "a and b and c", or DNF(disjunctive normal form) like "a or b or c"
func splitNormalFormItems(onExpr Expression, funcName string) []Expression {
	switch v := onExpr.(type) {
	case *ScalarFunction:
		if v.FuncName.L == funcName {
			var ret []Expression
			for _, arg := range v.GetArgs() {
				ret = append(ret, splitNormalFormItems(arg, funcName)...)
			}
			return ret
		}
	}
	return []Expression{onExpr}
}

// SplitCNFItems splits CNF items.
// CNF means conjunctive normal form, e.g. "a and b and c".
func SplitCNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.LogicAnd)
}

// SplitDNFItems splits DNF items.
// DNF means disjunctive normal form, e.g. "a or b or c".
func SplitDNFItems(onExpr Expression) []Expression {
	return splitNormalFormItems(onExpr, ast.LogicOr)
}

// EvaluateExprWithNull sets defCausumns in schemaReplicant as null and calculate the final result of the scalar function.
// If the Expression is a non-constant value, it means the result is unknown.
func EvaluateExprWithNull(ctx stochastikctx.Context, schemaReplicant *Schema, expr Expression) Expression {
	switch x := expr.(type) {
	case *ScalarFunction:
		args := make([]Expression, len(x.GetArgs()))
		for i, arg := range x.GetArgs() {
			args[i] = EvaluateExprWithNull(ctx, schemaReplicant, arg)
		}
		return NewFunctionInternal(ctx, x.FuncName.L, x.RetType, args...)
	case *DeferredCauset:
		if !schemaReplicant.Contains(x) {
			return x
		}
		return &Constant{Value: types.Causet{}, RetType: types.NewFieldType(allegrosql.TypeNull)}
	case *Constant:
		if x.DeferredExpr != nil {
			return FoldConstant(x)
		}
	}
	return expr
}

// BlockInfo2SchemaAndNames converts the BlockInfo to the schemaReplicant and name slice.
func BlockInfo2SchemaAndNames(ctx stochastikctx.Context, dbName perceptron.CIStr, tbl *perceptron.BlockInfo) (*Schema, []*types.FieldName, error) {
	defcaus, names, err := DeferredCausetInfos2DeferredCausetsAndNames(ctx, dbName, tbl.Name, tbl.DefCauss(), tbl)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]KeyInfo, 0, len(tbl.Indices)+1)
	for _, idx := range tbl.Indices {
		if !idx.Unique || idx.State != perceptron.StatePublic {
			continue
		}
		ok := true
		newKey := make([]*DeferredCauset, 0, len(idx.DeferredCausets))
		for _, idxDefCaus := range idx.DeferredCausets {
			find := false
			for i, defCaus := range tbl.DeferredCausets {
				if idxDefCaus.Name.L == defCaus.Name.L {
					if !allegrosql.HasNotNullFlag(defCaus.Flag) {
						break
					}
					newKey = append(newKey, defcaus[i])
					find = true
					break
				}
			}
			if !find {
				ok = false
				break
			}
		}
		if ok {
			keys = append(keys, newKey)
		}
	}
	if tbl.PKIsHandle {
		for i, defCaus := range tbl.DeferredCausets {
			if allegrosql.HasPriKeyFlag(defCaus.Flag) {
				keys = append(keys, KeyInfo{defcaus[i]})
				break
			}
		}
	}
	schemaReplicant := NewSchema(defcaus...)
	schemaReplicant.SetUniqueKeys(keys)
	return schemaReplicant, names, nil
}

// DeferredCausetInfos2DeferredCausetsAndNames converts the DeferredCausetInfo to the *DeferredCauset and NameSlice.
func DeferredCausetInfos2DeferredCausetsAndNames(ctx stochastikctx.Context, dbName, tblName perceptron.CIStr, defCausInfos []*perceptron.DeferredCausetInfo, tblInfo *perceptron.BlockInfo) ([]*DeferredCauset, types.NameSlice, error) {
	defCausumns := make([]*DeferredCauset, 0, len(defCausInfos))
	names := make([]*types.FieldName, 0, len(defCausInfos))
	for i, defCaus := range defCausInfos {
		names = append(names, &types.FieldName{
			OrigTblName:     tblName,
			OrigDefCausName: defCaus.Name,
			DBName:          dbName,
			TblName:         tblName,
			DefCausName:     defCaus.Name,
		})
		newDefCaus := &DeferredCauset{
			RetType:  &defCaus.FieldType,
			ID:       defCaus.ID,
			UniqueID: ctx.GetStochastikVars().AllocPlanDeferredCausetID(),
			Index:    defCaus.Offset,
			OrigName: names[i].String(),
			IsHidden: defCaus.Hidden,
		}
		defCausumns = append(defCausumns, newDefCaus)
	}
	// Resolve virtual generated defCausumn.
	mockSchema := NewSchema(defCausumns...)
	// Ignore redundant warning here.
	save := ctx.GetStochastikVars().StmtCtx.IgnoreTruncate
	defer func() {
		ctx.GetStochastikVars().StmtCtx.IgnoreTruncate = save
	}()
	ctx.GetStochastikVars().StmtCtx.IgnoreTruncate = true
	for i, defCaus := range defCausInfos {
		if defCaus.IsGenerated() && !defCaus.GeneratedStored {
			expr, err := generatedexpr.ParseExpression(defCaus.GeneratedExprString)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			expr, err = generatedexpr.SimpleResolveName(expr, tblInfo)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			e, err := RewriteAstExpr(ctx, expr, mockSchema, names)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			if e != nil {
				defCausumns[i].VirtualExpr = e.Clone()
			}
			defCausumns[i].VirtualExpr, err = defCausumns[i].VirtualExpr.ResolveIndices(mockSchema)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}
	return defCausumns, names, nil
}

// NewValuesFunc creates a new values function.
func NewValuesFunc(ctx stochastikctx.Context, offset int, retTp *types.FieldType) *ScalarFunction {
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, offset, retTp}
	bt, err := fc.getFunction(ctx, nil)
	terror.Log(err)
	return &ScalarFunction{
		FuncName: perceptron.NewCIStr(ast.Values),
		RetType:  retTp,
		Function: bt,
	}
}

// IsBinaryLiteral checks whether an expression is a binary literal
func IsBinaryLiteral(expr Expression) bool {
	con, ok := expr.(*Constant)
	return ok && con.Value.HoTT() == types.HoTTBinaryLiteral
}

func canFuncBePushed(sf *ScalarFunction, storeType ekv.StoreType) bool {
	// Use the failpoint to control whether to push down an expression in the integration test.
	// Push down all expression if the `failpoint expression` is `all`, otherwise, check
	// whether scalar function's name is contained in the enabled expression list (e.g.`ne,eq,lt`).
	// If neither of the above is true, switch to original logic.
	failpoint.Inject("PushDownTestSwitcher", func(val failpoint.Value) {
		enabled := val.(string)
		if enabled == "all" {
			failpoint.Return(true)
		}
		exprs := strings.Split(enabled, ",")
		for _, expr := range exprs {
			if strings.ToLower(strings.TrimSpace(expr)) == sf.FuncName.L {
				failpoint.Return(true)
			}
		}
	})

	ret := false
	switch sf.FuncName.L {
	case
		// op functions.
		ast.LogicAnd,
		ast.LogicOr,
		ast.LogicXor,
		ast.UnaryNot,
		ast.And,
		ast.Or,
		ast.Xor,
		ast.BitNeg,
		ast.LeftShift,
		ast.RightShift,
		ast.UnaryMinus,

		// compare functions.
		ast.LT,
		ast.LE,
		ast.EQ,
		ast.NE,
		ast.GE,
		ast.GT,
		ast.NullEQ,
		ast.In,
		ast.IsNull,
		ast.Like,
		ast.IsTruthWithoutNull,
		ast.IsTruthWithNull,
		ast.IsFalsity,

		// arithmetical functions.
		ast.Plus,
		ast.Minus,
		ast.Mul,
		ast.Div,
		ast.Abs,

		// math functions.
		ast.Ceil,
		ast.Ceiling,
		ast.Floor,
		ast.Sqrt,
		ast.Sign,
		ast.Ln,
		ast.Log,
		ast.Log2,
		ast.Log10,
		ast.Exp,
		ast.Pow,
		// Rust use the llvm math functions, which have different precision with Golang/MyALLEGROSQL(cmath)
		// open the following switchers if we implement them in interlock via `cmath`
		// ast.Sin,
		// ast.Asin,
		// ast.Cos,
		// ast.Acos,
		// ast.Tan,
		// ast.Atan,
		// ast.Atan2,
		// ast.Cot,
		ast.Radians,
		ast.Degrees,
		ast.Conv,
		ast.CRC32,

		// control flow functions.
		ast.Case,
		ast.If,
		ast.Ifnull,
		ast.Coalesce,

		// string functions.
		ast.Length,
		ast.BitLength,
		ast.Concat,
		ast.ConcatWS,
		// ast.Locate,
		ast.Replace,
		ast.ASCII,
		ast.Hex,
		ast.Reverse,
		ast.LTrim,
		ast.RTrim,
		// ast.Left,
		ast.Strcmp,
		ast.Space,
		ast.Elt,
		ast.Field,

		// json functions.
		ast.JSONType,
		ast.JSONExtract,
		// FIXME: JSONUnquote is incompatible with interlocking_directorate
		// ast.JSONUnquote,
		ast.JSONObject,
		ast.JSONArray,
		ast.JSONMerge,
		ast.JSONSet,
		ast.JSONInsert,
		// ast.JSONReplace,
		ast.JSONRemove,
		ast.JSONLength,

		// date functions.
		ast.DateFormat,
		ast.FromDays,
		// ast.ToDays,
		ast.DayOfYear,
		ast.DayOfMonth,
		ast.Year,
		ast.Month,
		// FIXME: the interlock cannot keep the same behavior with MilevaDB in current compute framework
		// ast.Hour,
		// ast.Minute,
		// ast.Second,
		// ast.MicroSecond,
		// ast.DayName,
		ast.PeriodAdd,
		ast.PeriodDiff,
		ast.TimestamFIDeliff,
		ast.DateAdd,
		ast.FromUnixTime,

		// encryption functions.
		ast.MD5,
		ast.SHA1,
		ast.UncompressedLength,

		ast.Cast,

		// misc functions.
		ast.InetNtoa,
		ast.InetAton,
		ast.Inet6Ntoa,
		ast.Inet6Aton,
		ast.IsIPv4,
		ast.IsIPv4Compat,
		ast.IsIPv4Mapped,
		ast.IsIPv6:
		ret = true

	// A special case: Only push down Round by signature
	case ast.Round:
		switch sf.Function.PbCode() {
		case
			fidelpb.ScalarFuncSig_RoundReal,
			fidelpb.ScalarFuncSig_RoundInt,
			fidelpb.ScalarFuncSig_RoundDec:
			ret = true
		}
	case
		ast.Substring,
		ast.Substr:
		switch sf.Function.PbCode() {
		case
			fidelpb.ScalarFuncSig_Substring2ArgsUTF8,
			fidelpb.ScalarFuncSig_Substring3ArgsUTF8:
			ret = true
		}
	case ast.Rand:
		switch sf.Function.PbCode() {
		case
			fidelpb.ScalarFuncSig_RandWithSeedFirstGen:
			ret = true
		}
	}
	if ret {
		switch storeType {
		case ekv.TiFlash:
			ret = scalarExprSupportedByFlash(sf)
		case ekv.EinsteinDB:
			ret = scalarExprSupportedByEinsteinDB(sf)
		case ekv.MilevaDB:
			ret = scalarExprSupportedByMilevaDB(sf)
		}
	}
	if ret {
		ret = IsPushDownEnabled(sf.FuncName.L, storeType)
	}
	return ret
}

func storeTypeMask(storeType ekv.StoreType) uint32 {
	if storeType == ekv.UnSpecified {
		return 1<<ekv.EinsteinDB | 1<<ekv.TiFlash | 1<<ekv.MilevaDB
	}
	return 1 << storeType
}

// IsPushDownEnabled returns true if the input expr is not in the expr_pushdown_blacklist
func IsPushDownEnabled(name string, storeType ekv.StoreType) bool {
	value, exists := DefaultExprPushDownBlacklist.Load().(map[string]uint32)[name]
	if exists {
		mask := storeTypeMask(storeType)
		return !(value&mask == mask)
	}

	if storeType != ekv.TiFlash && name == ast.AggFuncApproxCountDistinct {
		// Can not push down approx_count_distinct to other causetstore except tiflash by now.
		return false
	}

	return true
}

// DefaultExprPushDownBlacklist indicates the expressions which can not be pushed down to EinsteinDB.
var DefaultExprPushDownBlacklist *atomic.Value

func init() {
	DefaultExprPushDownBlacklist = new(atomic.Value)
	DefaultExprPushDownBlacklist.CausetStore(make(map[string]uint32))
}

func canScalarFuncPushDown(scalarFunc *ScalarFunction, pc PbConverter, storeType ekv.StoreType) bool {
	pbCode := scalarFunc.Function.PbCode()
	if pbCode <= fidelpb.ScalarFuncSig_Unspecified {
		failpoint.Inject("PanicIfPbCodeUnspecified", func() {
			panic(errors.Errorf("unspecified PbCode: %T", scalarFunc.Function))
		})
		return false
	}

	// Check whether this function can be pushed.
	if !canFuncBePushed(scalarFunc, storeType) {
		return false
	}

	// Check whether all of its parameters can be pushed.
	for _, arg := range scalarFunc.GetArgs() {
		if !canExprPushDown(arg, pc, storeType) {
			return false
		}
	}

	if metadata := scalarFunc.Function.metadata(); metadata != nil {
		var err error
		_, err = proto.Marshal(metadata)
		if err != nil {
			logutil.BgLogger().Error("encode metadata", zap.Any("metadata", metadata), zap.Error(err))
			return false
		}
	}
	return true
}

func canExprPushDown(expr Expression, pc PbConverter, storeType ekv.StoreType) bool {
	if storeType == ekv.TiFlash && expr.GetType().Tp == allegrosql.TypeDuration {
		return false
	}
	switch x := expr.(type) {
	case *CorrelatedDeferredCauset:
		return pc.conOrCorDefCausToPBExpr(expr) != nil && pc.defCausumnToPBExpr(&x.DeferredCauset) != nil
	case *Constant:
		return pc.conOrCorDefCausToPBExpr(expr) != nil
	case *DeferredCauset:
		return pc.defCausumnToPBExpr(x) != nil
	case *ScalarFunction:
		return canScalarFuncPushDown(x, pc, storeType)
	}
	return false
}

// PushDownExprs split the input exprs into pushed and remained, pushed include all the exprs that can be pushed down
func PushDownExprs(sc *stmtctx.StatementContext, exprs []Expression, client ekv.Client, storeType ekv.StoreType) (pushed []Expression, remained []Expression) {
	pc := PbConverter{sc: sc, client: client}
	for _, expr := range exprs {
		if canExprPushDown(expr, pc, storeType) {
			pushed = append(pushed, expr)
		} else {
			remained = append(remained, expr)
		}
	}
	return
}

// CanExprsPushDown return true if all the expr in exprs can be pushed down
func CanExprsPushDown(sc *stmtctx.StatementContext, exprs []Expression, client ekv.Client, storeType ekv.StoreType) bool {
	_, remained := PushDownExprs(sc, exprs, client, storeType)
	return len(remained) == 0
}

func scalarExprSupportedByEinsteinDB(function *ScalarFunction) bool {
	switch function.FuncName.L {
	case ast.Substr, ast.Substring, ast.DateAdd, ast.TimestamFIDeliff,
		ast.FromUnixTime:
		return false
	default:
		return true
	}
}

func scalarExprSupportedByMilevaDB(function *ScalarFunction) bool {
	switch function.FuncName.L {
	case ast.Substr, ast.Substring, ast.DateAdd, ast.TimestamFIDeliff,
		ast.FromUnixTime:
		return false
	default:
		return true
	}
}

func scalarExprSupportedByFlash(function *ScalarFunction) bool {
	switch function.FuncName.L {
	case ast.Plus, ast.Minus, ast.Div, ast.Mul, ast.GE, ast.LE,
		ast.EQ, ast.NE, ast.LT, ast.GT, ast.Ifnull, ast.IsNull,
		ast.Or, ast.In, ast.Mod, ast.And, ast.LogicOr, ast.LogicAnd,
		ast.Like, ast.UnaryNot, ast.Case, ast.Month, ast.Substr,
		ast.Substring, ast.TimestamFIDeliff, ast.DateFormat, ast.FromUnixTime,
		ast.JSONLength, ast.If, ast.BitNeg, ast.Xor:
		return true
	case ast.Cast:
		switch function.Function.PbCode() {
		case fidelpb.ScalarFuncSig_CastIntAsDecimal:
			return true
		default:
			return false
		}
	case ast.DateAdd:
		switch function.Function.PbCode() {
		case fidelpb.ScalarFuncSig_AddDateDatetimeInt, fidelpb.ScalarFuncSig_AddDateStringInt:
			return true
		default:
			return false
		}
	case ast.Round:
		switch function.Function.PbCode() {
		case fidelpb.ScalarFuncSig_RoundInt, fidelpb.ScalarFuncSig_RoundReal,
			fidelpb.ScalarFuncSig_RoundDec:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

// wrapWithIsTrue wraps `arg` with istrue function if the return type of expr is not
// type int, otherwise, returns `arg` directly.
// The `keepNull` controls what the istrue function will return when `arg` is null:
// 1. keepNull is true and arg is null, the istrue function returns null.
// 2. keepNull is false and arg is null, the istrue function returns 0.
// The `wrapForInt` indicates whether we need to wrapIsTrue for non-logical Expression with int type.
// TODO: remove this function. ScalarFunction should be newed in one place.
func wrapWithIsTrue(ctx stochastikctx.Context, keepNull bool, arg Expression, wrapForInt bool) (Expression, error) {
	if arg.GetType().EvalType() == types.ETInt {
		if !wrapForInt {
			return arg, nil
		}
		if child, ok := arg.(*ScalarFunction); ok {
			if _, isLogicalOp := logicalOps[child.FuncName.L]; isLogicalOp {
				return arg, nil
			}
		}
	}
	var fc *isTrueOrFalseFunctionClass
	if keepNull {
		fc = &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruthWithNull, 1, 1}, opcode.IsTruth, keepNull}
	} else {
		fc = &isTrueOrFalseFunctionClass{baseFunctionClass{ast.IsTruthWithoutNull, 1, 1}, opcode.IsTruth, keepNull}
	}
	f, err := fc.getFunction(ctx, []Expression{arg})
	if err != nil {
		return nil, err
	}
	sf := &ScalarFunction{
		FuncName: perceptron.NewCIStr(ast.IsTruthWithoutNull),
		Function: f,
		RetType:  f.getRetTp(),
	}
	if keepNull {
		sf.FuncName = perceptron.NewCIStr(ast.IsTruthWithNull)
	}
	return FoldConstant(sf), nil
}
