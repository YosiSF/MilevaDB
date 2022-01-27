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
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

// NewOne stands for a number 1.
func NewOne() *Constant {
	return &Constant{
		Value:   types.NewCauset(1),
		RetType: types.NewFieldType(allegrosql.TypeTiny),
	}
}

// NewZero stands for a number 0.
func NewZero() *Constant {
	return &Constant{
		Value:   types.NewCauset(0),
		RetType: types.NewFieldType(allegrosql.TypeTiny),
	}
}

// NewNull stands for null constant.
func NewNull() *Constant {
	return &Constant{
		Value:   types.NewCauset(nil),
		RetType: types.NewFieldType(allegrosql.TypeTiny),
	}
}

// Constant stands for a constant value.
type Constant struct {
	Value   types.Causet
	RetType *types.FieldType
	// DeferredExpr holds deferred function in PlanCache cached plan.
	// it's only used to represent non-deterministic functions(see expression.DeferredFunctions)
	// in PlanCache cached plan, so let them can be evaluated until cached item be used.
	DeferredExpr Expression
	// ParamMarker holds param index inside stochastikVars.PreparedParams.
	// It's only used to reference a user variable provided in the `EXECUTE` statement or `COM_EXECUTE` binary protodefCaus.
	ParamMarker *ParamMarker
	hashcode    []byte

	defCauslationInfo
}

// ParamMarker indicates param provided by COM_STMT_EXECUTE.
type ParamMarker struct {
	ctx   stochastikctx.Context
	order int
}

// GetUserVar returns the corresponding user variable presented in the `EXECUTE` statement or `COM_EXECUTE` command.
func (d *ParamMarker) GetUserVar() types.Causet {
	stochastikVars := d.ctx.GetStochastikVars()
	return stochastikVars.PreparedParams[d.order]
}

// String implements fmt.Stringer interface.
func (c *Constant) String() string {
	if c.ParamMarker != nil {
		dt := c.ParamMarker.GetUserVar()
		c.Value.SetValue(dt.GetValue(), c.RetType)
	} else if c.DeferredExpr != nil {
		return c.DeferredExpr.String()
	}
	return fmt.Sprintf("%v", c.Value.GetValue())
}

// MarshalJSON implements json.Marshaler interface.
func (c *Constant) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", c)), nil
}

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
	con := *c
	return &con
}

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	if c.ParamMarker != nil {
		// GetType() may be called in multi-threaded context, e.g, in building inner executors of IndexJoin,
		// so it should avoid data race. We achieve this by returning different FieldType pointer for each call.
		tp := types.NewFieldType(allegrosql.TypeUnspecified)
		dt := c.ParamMarker.GetUserVar()
		types.DefaultParamTypeForValue(dt.GetValue(), tp)
		return tp
	}
	return c.RetType
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalInt(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETInt, input, result)
	}
	return c.DeferredExpr.VecEvalInt(ctx, input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalReal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETReal, input, result)
	}
	return c.DeferredExpr.VecEvalReal(ctx, input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalString(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETString, input, result)
	}
	return c.DeferredExpr.VecEvalString(ctx, input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalDecimal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETDecimal, input, result)
	}
	return c.DeferredExpr.VecEvalDecimal(ctx, input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalTime(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETTimestamp, input, result)
	}
	return c.DeferredExpr.VecEvalTime(ctx, input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalDuration(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETDuration, input, result)
	}
	return c.DeferredExpr.VecEvalDuration(ctx, input, result)
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (c *Constant) VecEvalJSON(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETJson, input, result)
	}
	return c.DeferredExpr.VecEvalJSON(ctx, input, result)
}

func (c *Constant) getLazyCauset(event chunk.Event) (dt types.Causet, isLazy bool, err error) {
	if c.ParamMarker != nil {
		return c.ParamMarker.GetUserVar(), true, nil
	} else if c.DeferredExpr != nil {
		dt, err = c.DeferredExpr.Eval(event)
		return dt, true, err
	}
	return types.Causet{}, false, nil
}

// Eval implements Expression interface.
func (c *Constant) Eval(event chunk.Event) (types.Causet, error) {
	if dt, lazy, err := c.getLazyCauset(event); lazy {
		if err != nil {
			return c.Value, err
		}
		if dt.IsNull() {
			c.Value.SetNull()
			return c.Value, nil
		}
		if c.DeferredExpr != nil {
			sf, sfOk := c.DeferredExpr.(*ScalarFunction)
			if sfOk {
				val, err := dt.ConvertTo(sf.GetCtx().GetStochastikVars().StmtCtx, c.RetType)
				if err != nil {
					return dt, err
				}
				return val, nil
			}
		}
		return dt, nil
	}
	return c.Value, nil
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(ctx stochastikctx.Context, event chunk.Event) (int64, bool, error) {
	dt, lazy, err := c.getLazyCauset(event)
	if err != nil {
		return 0, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().Tp == allegrosql.TypeNull || dt.IsNull() {
		return 0, true, nil
	} else if dt.HoTT() == types.HoTTBinaryLiteral {
		val, err := dt.GetBinaryLiteral().ToInt(ctx.GetStochastikVars().StmtCtx)
		return int64(val), err != nil, err
	} else if c.GetType().Hybrid() || dt.HoTT() == types.HoTTString {
		res, err := dt.ToInt64(ctx.GetStochastikVars().StmtCtx)
		return res, false, err
	}
	return dt.GetInt64(), false, nil
}

// EvalReal returns real representation of Constant.
func (c *Constant) EvalReal(ctx stochastikctx.Context, event chunk.Event) (float64, bool, error) {
	dt, lazy, err := c.getLazyCauset(event)
	if err != nil {
		return 0, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().Tp == allegrosql.TypeNull || dt.IsNull() {
		return 0, true, nil
	}
	if c.GetType().Hybrid() || dt.HoTT() == types.HoTTBinaryLiteral || dt.HoTT() == types.HoTTString {
		res, err := dt.ToFloat64(ctx.GetStochastikVars().StmtCtx)
		return res, false, err
	}
	return dt.GetFloat64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(ctx stochastikctx.Context, event chunk.Event) (string, bool, error) {
	dt, lazy, err := c.getLazyCauset(event)
	if err != nil {
		return "", false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().Tp == allegrosql.TypeNull || dt.IsNull() {
		return "", true, nil
	}
	res, err := dt.ToString()
	return res, false, err
}

// EvalDecimal returns decimal representation of Constant.
func (c *Constant) EvalDecimal(ctx stochastikctx.Context, event chunk.Event) (*types.MyDecimal, bool, error) {
	dt, lazy, err := c.getLazyCauset(event)
	if err != nil {
		return nil, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().Tp == allegrosql.TypeNull || dt.IsNull() {
		return nil, true, nil
	}
	res, err := dt.ToDecimal(ctx.GetStochastikVars().StmtCtx)
	return res, false, err
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of Constant.
func (c *Constant) EvalTime(ctx stochastikctx.Context, event chunk.Event) (val types.Time, isNull bool, err error) {
	dt, lazy, err := c.getLazyCauset(event)
	if err != nil {
		return types.ZeroTime, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().Tp == allegrosql.TypeNull || dt.IsNull() {
		return types.ZeroTime, true, nil
	}
	return dt.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of Constant.
func (c *Constant) EvalDuration(ctx stochastikctx.Context, event chunk.Event) (val types.Duration, isNull bool, err error) {
	dt, lazy, err := c.getLazyCauset(event)
	if err != nil {
		return types.Duration{}, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().Tp == allegrosql.TypeNull || dt.IsNull() {
		return types.Duration{}, true, nil
	}
	return dt.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of Constant.
func (c *Constant) EvalJSON(ctx stochastikctx.Context, event chunk.Event) (json.BinaryJSON, bool, error) {
	dt, lazy, err := c.getLazyCauset(event)
	if err != nil {
		return json.BinaryJSON{}, false, err
	}
	if !lazy {
		dt = c.Value
	}
	if c.GetType().Tp == allegrosql.TypeNull || dt.IsNull() {
		return json.BinaryJSON{}, true, nil
	}
	return dt.GetMysqlJSON(), false, nil
}

// Equal implements Expression interface.
func (c *Constant) Equal(ctx stochastikctx.Context, b Expression) bool {
	y, ok := b.(*Constant)
	if !ok {
		return false
	}
	_, err1 := y.Eval(chunk.Event{})
	_, err2 := c.Eval(chunk.Event{})
	if err1 != nil || err2 != nil {
		return false
	}
	con, err := c.Value.CompareCauset(ctx.GetStochastikVars().StmtCtx, &y.Value)
	if err != nil || con != 0 {
		return false
	}
	return true
}

// IsCorrelated implements Expression interface.
func (c *Constant) IsCorrelated() bool {
	return false
}

// ConstItem implements Expression interface.
func (c *Constant) ConstItem(sc *stmtctx.StatementContext) bool {
	return !sc.UseCache || (c.DeferredExpr == nil && c.ParamMarker == nil)
}

// Decorrelate implements Expression interface.
func (c *Constant) Decorrelate(_ *Schema) Expression {
	return c
}

// HashCode implements Expression interface.
func (c *Constant) HashCode(sc *stmtctx.StatementContext) []byte {
	if len(c.hashcode) > 0 {
		return c.hashcode
	}
	_, err := c.Eval(chunk.Event{})
	if err != nil {
		terror.Log(err)
	}
	c.hashcode = append(c.hashcode, constantFlag)
	c.hashcode, err = codec.EncodeValue(sc, c.hashcode, c.Value)
	if err != nil {
		terror.Log(err)
	}
	return c.hashcode
}

// ResolveIndices implements Expression interface.
func (c *Constant) ResolveIndices(_ *Schema) (Expression, error) {
	return c, nil
}

func (c *Constant) resolveIndices(_ *Schema) error {
	return nil
}

// Vectorized returns if this expression supports vectorized evaluation.
func (c *Constant) Vectorized() bool {
	if c.DeferredExpr != nil {
		return c.DeferredExpr.Vectorized()
	}
	return true
}

// SupportReverseEval checks whether the builtinFunc support reverse evaluation.
func (c *Constant) SupportReverseEval() bool {
	if c.DeferredExpr != nil {
		return c.DeferredExpr.SupportReverseEval()
	}
	return true
}

// ReverseEval evaluates the only one defCausumn value with given function result.
func (c *Constant) ReverseEval(sc *stmtctx.StatementContext, res types.Causet, rType types.RoundingType) (val types.Causet, err error) {
	return c.Value, nil
}

// Coercibility returns the coercibility value which is used to check defCauslations.
func (c *Constant) Coercibility() Coercibility {
	if c.HasCoercibility() {
		return c.defCauslationInfo.Coercibility()
	}

	c.SetCoercibility(deriveCoercibilityForConstant(c))
	return c.defCauslationInfo.Coercibility()
}
