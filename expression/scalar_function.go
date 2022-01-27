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
	"bytes"
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

// error definitions.
var (
	ErrNoDB = terror.ClassOptimizer.New(allegrosql.ErrNoDB, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNoDB])
)

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	FuncName perceptron.CIStr
	// RetType is the type that ScalarFunction returns.
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	RetType  *types.FieldType
	Function builtinFunc
	hashcode []byte
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalInt(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return sf.Function.vecEvalInt(input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalReal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return sf.Function.vecEvalReal(input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalString(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return sf.Function.vecEvalString(input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalDecimal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return sf.Function.vecEvalDecimal(input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalTime(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return sf.Function.vecEvalTime(input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalDuration(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return sf.Function.vecEvalDuration(input, result)
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (sf *ScalarFunction) VecEvalJSON(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return sf.Function.vecEvalJSON(input, result)
}

// GetArgs gets arguments of function.
func (sf *ScalarFunction) GetArgs() []Expression {
	return sf.Function.getArgs()
}

// Vectorized returns if this expression supports vectorized evaluation.
func (sf *ScalarFunction) Vectorized() bool {
	return sf.Function.vectorized() && sf.Function.isChildrenVectorized()
}

// SupportReverseEval returns if this expression supports reversed evaluation.
func (sf *ScalarFunction) SupportReverseEval() bool {
	switch sf.RetType.Tp {
	case allegrosql.TypeShort, allegrosql.TypeLong, allegrosql.TypeLonglong,
		allegrosql.TypeFloat, allegrosql.TypeDouble, allegrosql.TypeNewDecimal:
		return sf.Function.supportReverseEval() && sf.Function.isChildrenReversed()
	}
	return false
}

// ReverseEval evaluates the only one defCausumn value with given function result.
func (sf *ScalarFunction) ReverseEval(sc *stmtctx.StatementContext, res types.Causet, rType types.RoundingType) (val types.Causet, err error) {
	return sf.Function.reverseEval(sc, res, rType)
}

// GetCtx gets the context of function.
func (sf *ScalarFunction) GetCtx() stochastikctx.Context {
	return sf.Function.getCtx()
}

// String implements fmt.Stringer interface.
func (sf *ScalarFunction) String() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", sf.FuncName.L)
	switch sf.FuncName.L {
	case ast.Cast:
		for _, arg := range sf.GetArgs() {
			buffer.WriteString(arg.String())
			buffer.WriteString(", ")
			buffer.WriteString(sf.RetType.String())
		}
	default:
		for i, arg := range sf.GetArgs() {
			buffer.WriteString(arg.String())
			if i+1 != len(sf.GetArgs()) {
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// MarshalJSON implements json.Marshaler interface.
func (sf *ScalarFunction) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", sf)), nil
}

// typeInferForNull infers the NULL constants field type and set the field type
// of NULL constant same as other non-null operands.
func typeInferForNull(args []Expression) {
	if len(args) < 2 {
		return
	}
	var isNull = func(expr Expression) bool {
		cons, ok := expr.(*Constant)
		return ok && cons.RetType.Tp == allegrosql.TypeNull && cons.Value.IsNull()
	}
	// Infer the actual field type of the NULL constant.
	var retFieldTp *types.FieldType
	var hasNullArg bool
	for _, arg := range args {
		isNullArg := isNull(arg)
		if !isNullArg && retFieldTp == nil {
			retFieldTp = arg.GetType()
		}
		hasNullArg = hasNullArg || isNullArg
		// Break if there are both NULL and non-NULL expression
		if hasNullArg && retFieldTp != nil {
			break
		}
	}
	if !hasNullArg || retFieldTp == nil {
		return
	}
	for _, arg := range args {
		if isNull(arg) {
			*arg.GetType() = *retFieldTp
		}
	}
}

// newFunctionImpl creates a new scalar function or constant.
// fold: 1 means folding constants, while 0 means not,
// -1 means try to fold constants if without errors/warnings, otherwise not.
func newFunctionImpl(ctx stochastikctx.Context, fold int, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	if retType == nil {
		return nil, errors.Errorf("RetType cannot be nil for ScalarFunction.")
	}
	if funcName == ast.Cast {
		return BuildCastFunction(ctx, args[0], retType), nil
	}
	fc, ok := funcs[funcName]
	if !ok {
		EDB := ctx.GetStochastikVars().CurrentDB
		if EDB == "" {
			return nil, errors.Trace(ErrNoDB)
		}

		return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", EDB+"."+funcName)
	}
	if !ctx.GetStochastikVars().EnableNoopFuncs {
		if _, ok := noopFuncs[funcName]; ok {
			return nil, ErrFunctionsNoopImpl.GenWithStackByArgs(funcName)
		}
	}
	funcArgs := make([]Expression, len(args))
	INTERLOCKy(funcArgs, args)
	typeInferForNull(funcArgs)
	f, err := fc.getFunction(ctx, funcArgs)
	if err != nil {
		return nil, err
	}
	if builtinRetTp := f.getRetTp(); builtinRetTp.Tp != allegrosql.TypeUnspecified || retType.Tp == allegrosql.TypeUnspecified {
		retType = builtinRetTp
	}
	sf := &ScalarFunction{
		FuncName: perceptron.NewCIStr(funcName),
		RetType:  retType,
		Function: f,
	}
	if fold == 1 {
		return FoldConstant(sf), nil
	} else if fold == -1 {
		// try to fold constants, and return the original function if errors/warnings occur
		sc := ctx.GetStochastikVars().StmtCtx
		beforeWarns := sc.WarningCount()
		newSf := FoldConstant(sf)
		afterWarns := sc.WarningCount()
		if afterWarns > beforeWarns {
			sc.TruncateWarnings(int(beforeWarns))
			return sf, nil
		}
		return newSf, nil
	}
	return sf, nil
}

// NewFunction creates a new scalar function or constant via a constant folding.
func NewFunction(ctx stochastikctx.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, 1, funcName, retType, args...)
}

// NewFunctionBase creates a new scalar function with no constant folding.
func NewFunctionBase(ctx stochastikctx.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, 0, funcName, retType, args...)
}

// NewFunctionTryFold creates a new scalar function with trying constant folding.
func NewFunctionTryFold(ctx stochastikctx.Context, funcName string, retType *types.FieldType, args ...Expression) (Expression, error) {
	return newFunctionImpl(ctx, -1, funcName, retType, args...)
}

// NewFunctionInternal is similar to NewFunction, but do not returns error, should only be used internally.
func NewFunctionInternal(ctx stochastikctx.Context, funcName string, retType *types.FieldType, args ...Expression) Expression {
	expr, err := NewFunction(ctx, funcName, retType, args...)
	terror.Log(err)
	return expr
}

// ScalarFuncs2Exprs converts []*ScalarFunction to []Expression.
func ScalarFuncs2Exprs(funcs []*ScalarFunction) []Expression {
	result := make([]Expression, 0, len(funcs))
	for _, defCaus := range funcs {
		result = append(result, defCaus)
	}
	return result
}

// Clone implements Expression interface.
func (sf *ScalarFunction) Clone() Expression {
	c := &ScalarFunction{
		FuncName: sf.FuncName,
		RetType:  sf.RetType,
		Function: sf.Function.Clone(),
		hashcode: sf.hashcode,
	}
	c.SetCharsetAndDefCauslation(sf.CharsetAndDefCauslation(sf.GetCtx()))
	c.SetCoercibility(sf.Coercibility())
	return c
}

// GetType implements Expression interface.
func (sf *ScalarFunction) GetType() *types.FieldType {
	return sf.RetType
}

// Equal implements Expression interface.
func (sf *ScalarFunction) Equal(ctx stochastikctx.Context, e Expression) bool {
	fun, ok := e.(*ScalarFunction)
	if !ok {
		return false
	}
	if sf.FuncName.L != fun.FuncName.L {
		return false
	}
	return sf.Function.equal(fun.Function)
}

// IsCorrelated implements Expression interface.
func (sf *ScalarFunction) IsCorrelated() bool {
	for _, arg := range sf.GetArgs() {
		if arg.IsCorrelated() {
			return true
		}
	}
	return false
}

// ConstItem implements Expression interface.
func (sf *ScalarFunction) ConstItem(sc *stmtctx.StatementContext) bool {
	// Note: some unfoldable functions are deterministic, we use unFoldableFunctions here for simplification.
	if _, ok := unFoldableFunctions[sf.FuncName.L]; ok {
		return false
	}
	for _, arg := range sf.GetArgs() {
		if !arg.ConstItem(sc) {
			return false
		}
	}
	return true
}

// Decorrelate implements Expression interface.
func (sf *ScalarFunction) Decorrelate(schemaReplicant *Schema) Expression {
	for i, arg := range sf.GetArgs() {
		sf.GetArgs()[i] = arg.Decorrelate(schemaReplicant)
	}
	return sf
}

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(event chunk.Event) (d types.Causet, err error) {
	var (
		res    interface{}
		isNull bool
	)
	switch tp, evalType := sf.GetType(), sf.GetType().EvalType(); evalType {
	case types.ETInt:
		var intRes int64
		intRes, isNull, err = sf.EvalInt(sf.GetCtx(), event)
		if allegrosql.HasUnsignedFlag(tp.Flag) {
			res = uint64(intRes)
		} else {
			res = intRes
		}
	case types.ETReal:
		res, isNull, err = sf.EvalReal(sf.GetCtx(), event)
	case types.ETDecimal:
		res, isNull, err = sf.EvalDecimal(sf.GetCtx(), event)
	case types.ETDatetime, types.ETTimestamp:
		res, isNull, err = sf.EvalTime(sf.GetCtx(), event)
	case types.ETDuration:
		res, isNull, err = sf.EvalDuration(sf.GetCtx(), event)
	case types.ETJson:
		res, isNull, err = sf.EvalJSON(sf.GetCtx(), event)
	case types.ETString:
		res, isNull, err = sf.EvalString(sf.GetCtx(), event)
	}

	if isNull || err != nil {
		d.SetNull()
		return d, err
	}
	d.SetValue(res, sf.RetType)
	return
}

// EvalInt implements Expression interface.
func (sf *ScalarFunction) EvalInt(ctx stochastikctx.Context, event chunk.Event) (int64, bool, error) {
	if f, ok := sf.Function.(builtinFuncNew); ok {
		return f.evalIntWithCtx(ctx, event)
	}
	return sf.Function.evalInt(event)
}

// EvalReal implements Expression interface.
func (sf *ScalarFunction) EvalReal(ctx stochastikctx.Context, event chunk.Event) (float64, bool, error) {
	return sf.Function.evalReal(event)
}

// EvalDecimal implements Expression interface.
func (sf *ScalarFunction) EvalDecimal(ctx stochastikctx.Context, event chunk.Event) (*types.MyDecimal, bool, error) {
	return sf.Function.evalDecimal(event)
}

// EvalString implements Expression interface.
func (sf *ScalarFunction) EvalString(ctx stochastikctx.Context, event chunk.Event) (string, bool, error) {
	return sf.Function.evalString(event)
}

// EvalTime implements Expression interface.
func (sf *ScalarFunction) EvalTime(ctx stochastikctx.Context, event chunk.Event) (types.Time, bool, error) {
	return sf.Function.evalTime(event)
}

// EvalDuration implements Expression interface.
func (sf *ScalarFunction) EvalDuration(ctx stochastikctx.Context, event chunk.Event) (types.Duration, bool, error) {
	return sf.Function.evalDuration(event)
}

// EvalJSON implements Expression interface.
func (sf *ScalarFunction) EvalJSON(ctx stochastikctx.Context, event chunk.Event) (json.BinaryJSON, bool, error) {
	return sf.Function.evalJSON(event)
}

// HashCode implements Expression interface.
func (sf *ScalarFunction) HashCode(sc *stmtctx.StatementContext) []byte {
	if len(sf.hashcode) > 0 {
		return sf.hashcode
	}
	sf.hashcode = append(sf.hashcode, scalarFunctionFlag)
	sf.hashcode = codec.EncodeCompactBytes(sf.hashcode, replog.Slice(sf.FuncName.L))
	for _, arg := range sf.GetArgs() {
		sf.hashcode = append(sf.hashcode, arg.HashCode(sc)...)
	}
	return sf.hashcode
}

// ResolveIndices implements Expression interface.
func (sf *ScalarFunction) ResolveIndices(schemaReplicant *Schema) (Expression, error) {
	newSf := sf.Clone()
	err := newSf.resolveIndices(schemaReplicant)
	return newSf, err
}

func (sf *ScalarFunction) resolveIndices(schemaReplicant *Schema) error {
	if sf.FuncName.L == ast.In {
		args := []Expression{}
		switch inFunc := sf.Function.(type) {
		case *builtinInIntSig:
			args = inFunc.nonConstArgs
		case *builtinInStringSig:
			args = inFunc.nonConstArgs
		case *builtinInTimeSig:
			args = inFunc.nonConstArgs
		case *builtinInDurationSig:
			args = inFunc.nonConstArgs
		case *builtinInRealSig:
			args = inFunc.nonConstArgs
		case *builtinInDecimalSig:
			args = inFunc.nonConstArgs
		}
		for _, arg := range args {
			err := arg.resolveIndices(schemaReplicant)
			if err != nil {
				return err
			}
		}
	}
	for _, arg := range sf.GetArgs() {
		err := arg.resolveIndices(schemaReplicant)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetSingleDeferredCauset returns (DefCaus, Desc) when the ScalarFunction is equivalent to (DefCaus, Desc)
// when used as a sort key, otherwise returns (nil, false).
//
// Can only handle:
// - ast.Plus
// - ast.Minus
// - ast.UnaryMinus
func (sf *ScalarFunction) GetSingleDeferredCauset(reverse bool) (*DeferredCauset, bool) {
	switch sf.FuncName.String() {
	case ast.Plus:
		args := sf.GetArgs()
		switch tp := args[0].(type) {
		case *DeferredCauset:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp, reverse
		case *ScalarFunction:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp.GetSingleDeferredCauset(reverse)
		case *Constant:
			switch rtp := args[1].(type) {
			case *DeferredCauset:
				return rtp, reverse
			case *ScalarFunction:
				return rtp.GetSingleDeferredCauset(reverse)
			}
		}
		return nil, false
	case ast.Minus:
		args := sf.GetArgs()
		switch tp := args[0].(type) {
		case *DeferredCauset:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp, reverse
		case *ScalarFunction:
			if _, ok := args[1].(*Constant); !ok {
				return nil, false
			}
			return tp.GetSingleDeferredCauset(reverse)
		case *Constant:
			switch rtp := args[1].(type) {
			case *DeferredCauset:
				return rtp, !reverse
			case *ScalarFunction:
				return rtp.GetSingleDeferredCauset(!reverse)
			}
		}
		return nil, false
	case ast.UnaryMinus:
		args := sf.GetArgs()
		switch tp := args[0].(type) {
		case *DeferredCauset:
			return tp, !reverse
		case *ScalarFunction:
			return tp.GetSingleDeferredCauset(!reverse)
		}
		return nil, false
	}
	return nil, false
}

// Coercibility returns the coercibility value which is used to check defCauslations.
func (sf *ScalarFunction) Coercibility() Coercibility {
	if !sf.Function.HasCoercibility() {
		sf.SetCoercibility(deriveCoercibilityForScarlarFunc(sf))
	}
	return sf.Function.Coercibility()
}

// HasCoercibility ...
func (sf *ScalarFunction) HasCoercibility() bool {
	return sf.Function.HasCoercibility()
}

// SetCoercibility sets a specified coercibility for this expression.
func (sf *ScalarFunction) SetCoercibility(val Coercibility) {
	sf.Function.SetCoercibility(val)
}

// CharsetAndDefCauslation ...
func (sf *ScalarFunction) CharsetAndDefCauslation(ctx stochastikctx.Context) (string, string) {
	return sf.Function.CharsetAndDefCauslation(ctx)
}

// SetCharsetAndDefCauslation ...
func (sf *ScalarFunction) SetCharsetAndDefCauslation(chs, defCausl string) {
	sf.Function.SetCharsetAndDefCauslation(chs, defCausl)
}
