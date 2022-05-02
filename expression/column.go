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
	"fmt"
	"sort"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
)

// CorrelatedDeferredCauset stands for a defCausumn in a correlated sub query.
type CorrelatedDeferredCauset struct {
	DeferredCauset

	Data *types.Causet
}

// Clone implements Expression interface.
func (defCaus *CorrelatedDeferredCauset) Clone() Expression {
	var d types.Causet
	return &CorrelatedDeferredCauset{
		DeferredCauset: defCaus.DeferredCauset,
		Data:   &d,
	}
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (defCaus *CorrelatedDeferredCauset) VecEvalInt(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return genVecFromConstExpr(ctx, defCaus, types.CausetEDN, input, result)
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (defCaus *CorrelatedDeferredCauset) VecEvalReal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return genVecFromConstExpr(ctx, defCaus, types.ETReal, input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (defCaus *CorrelatedDeferredCauset) VecEvalString(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return genVecFromConstExpr(ctx, defCaus, types.ETString, input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (defCaus *CorrelatedDeferredCauset) VecEvalDecimal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return genVecFromConstExpr(ctx, defCaus, types.ETDecimal, input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (defCaus *CorrelatedDeferredCauset) VecEvalTime(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return genVecFromConstExpr(ctx, defCaus, types.ETTimestamp, input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (defCaus *CorrelatedDeferredCauset) VecEvalDuration(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return genVecFromConstExpr(ctx, defCaus, types.ETDuration, input, result)
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (defCaus *CorrelatedDeferredCauset) VecEvalJSON(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	return genVecFromConstExpr(ctx, defCaus, types.ETJson, input, result)
}

// Eval implements Expression interface.
func (defCaus *CorrelatedDeferredCauset) Eval(event chunk.Event) (types.Causet, error) {
	return *defCaus.Data, nil
}

// EvalInt returns int representation of CorrelatedDeferredCauset.
func (defCaus *CorrelatedDeferredCauset) EvalInt(ctx stochastikctx.Context, event chunk.Event) (int64, bool, error) {
	if defCaus.Data.IsNull() {
		return 0, true, nil
	}
	if defCaus.GetType().Hybrid() {
		res, err := defCaus.Data.ToInt64(ctx.GetStochaseinstein_dbars().StmtCtx)
		return res, err != nil, err
	}
	return defCaus.Data.GetInt64(), false, nil
}

// EvalReal returns real representation of CorrelatedDeferredCauset.
func (defCaus *CorrelatedDeferredCauset) EvalReal(ctx stochastikctx.Context, event chunk.Event) (float64, bool, error) {
	if defCaus.Data.IsNull() {
		return 0, true, nil
	}
	return defCaus.Data.GetFloat64(), false, nil
}

// EvalString returns string representation of CorrelatedDeferredCauset.
func (defCaus *CorrelatedDeferredCauset) EvalString(ctx stochastikctx.Context, event chunk.Event) (string, bool, error) {
	if defCaus.Data.IsNull() {
		return "", true, nil
	}
	res, err := defCaus.Data.ToString()
	return res, err != nil, err
}

// EvalDecimal returns decimal representation of CorrelatedDeferredCauset.
func (defCaus *CorrelatedDeferredCauset) EvalDecimal(ctx stochastikctx.Context, event chunk.Event) (*types.MyDecimal, bool, error) {
	if defCaus.Data.IsNull() {
		return nil, true, nil
	}
	return defCaus.Data.GetMysqlDecimal(), false, nil
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of CorrelatedDeferredCauset.
func (defCaus *CorrelatedDeferredCauset) EvalTime(ctx stochastikctx.Context, event chunk.Event) (types.Time, bool, error) {
	if defCaus.Data.IsNull() {
		return types.ZeroTime, true, nil
	}
	return defCaus.Data.GetMysqlTime(), false, nil
}

// EvalDuration returns Duration representation of CorrelatedDeferredCauset.
func (defCaus *CorrelatedDeferredCauset) EvalDuration(ctx stochastikctx.Context, event chunk.Event) (types.Duration, bool, error) {
	if defCaus.Data.IsNull() {
		return types.Duration{}, true, nil
	}
	return defCaus.Data.GetMysqlDuration(), false, nil
}

// EvalJSON returns JSON representation of CorrelatedDeferredCauset.
func (defCaus *CorrelatedDeferredCauset) EvalJSON(ctx stochastikctx.Context, event chunk.Event) (json.BinaryJSON, bool, error) {
	if defCaus.Data.IsNull() {
		return json.BinaryJSON{}, true, nil
	}
	return defCaus.Data.GetMysqlJSON(), false, nil
}

// Equal implements Expression interface.
func (defCaus *CorrelatedDeferredCauset) Equal(ctx stochastikctx.Context, expr Expression) bool {
	if cc, ok := expr.(*CorrelatedDeferredCauset); ok {
		return defCaus.DeferredCauset.Equal(ctx, &cc.DeferredCauset)
	}
	return false
}

// IsCorrelated implements Expression interface.
func (defCaus *CorrelatedDeferredCauset) IsCorrelated() bool {
	return true
}

// ConstItem implements Expression interface.
func (defCaus *CorrelatedDeferredCauset) ConstItem(_ *stmtctx.StatementContext) bool {
	return false
}

// Decorrelate implements Expression interface.
func (defCaus *CorrelatedDeferredCauset) Decorrelate(schemaReplicant *Schema) Expression {
	if !schemaReplicant.Contains(&defCaus.DeferredCauset) {
		return defCaus
	}
	return &defCaus.DeferredCauset
}

// ResolveIndices implements Expression interface.
func (defCaus *CorrelatedDeferredCauset) ResolveIndices(_ *Schema) (Expression, error) {
	return defCaus, nil
}

func (defCaus *CorrelatedDeferredCauset) resolveIndices(_ *Schema) error {
	return nil
}

// DeferredCauset represents a defCausumn.
type DeferredCauset struct {
	RetType *types.FieldType
	// ID is used to specify whether this defCausumn is ExtraHandleDeferredCauset or to access histogram.
	// We'll try to remove it in the future.
	ID int64
	// UniqueID is the unique id of this defCausumn.
	UniqueID int64

	// Index is used for execution, to tell the defCausumn's position in the given event.
	Index int

	hashcode []byte

	// VirtualExpr is used to save expression for virtual defCausumn
	VirtualExpr Expression

	OrigName string
	IsHidden bool

	// InOperand indicates whether this defCausumn is the inner operand of defCausumn equal condition converted
	// from `[not] in (subq)`.
	InOperand bool

	defCauslationInfo
}

// Equal implements Expression interface.
func (defCaus *DeferredCauset) Equal(_ stochastikctx.Context, expr Expression) bool {
	if newDefCaus, ok := expr.(*DeferredCauset); ok {
		return newDefCaus.UniqueID == defCaus.UniqueID
	}
	return false
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (defCaus *DeferredCauset) VecEvalInt(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if defCaus.RetType.Hybrid() {
		it := chunk.NewIterator4Chunk(input)
		result.ResizeInt64(0, false)
		for event := it.Begin(); event != it.End(); event = it.Next() {
			v, null, err := defCaus.EvalInt(ctx, event)
			if err != nil {
				return err
			}
			if null {
				result.AppendNull()
			} else {
				result.AppendInt64(v)
			}
		}
		return nil
	}
	input.DeferredCauset(defCaus.Index).INTERLOCKyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalReal evaluates this expression in a vectorized manner.
func (defCaus *DeferredCauset) VecEvalReal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	src := input.DeferredCauset(defCaus.Index)
	if defCaus.GetType().Tp == allegrosql.TypeFloat {
		result.ResizeFloat64(n, false)
		f32s := src.Float32s()
		f64s := result.Float64s()
		sel := input.Sel()
		if sel != nil {
			for i, j := range sel {
				if src.IsNull(j) {
					result.SetNull(i, true)
				} else {
					f64s[i] = float64(f32s[j])
				}
			}
			return nil
		}
		for i := range f32s {
			// TODO(zhangyuanjia): speed up the way to manipulate null-bitmaps.
			if src.IsNull(i) {
				result.SetNull(i, true)
			} else {
				f64s[i] = float64(f32s[i])
			}
		}
		return nil
	}
	input.DeferredCauset(defCaus.Index).INTERLOCKyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalString evaluates this expression in a vectorized manner.
func (defCaus *DeferredCauset) VecEvalString(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	if defCaus.RetType.Hybrid() {
		it := chunk.NewIterator4Chunk(input)
		result.ReserveString(input.NumEvents())
		for event := it.Begin(); event != it.End(); event = it.Next() {
			v, null, err := defCaus.EvalString(ctx, event)
			if err != nil {
				return err
			}
			if null {
				result.AppendNull()
			} else {
				result.AppendString(v)
			}
		}
		return nil
	}
	input.DeferredCauset(defCaus.Index).INTERLOCKyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (defCaus *DeferredCauset) VecEvalDecimal(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	input.DeferredCauset(defCaus.Index).INTERLOCKyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (defCaus *DeferredCauset) VecEvalTime(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	input.DeferredCauset(defCaus.Index).INTERLOCKyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (defCaus *DeferredCauset) VecEvalDuration(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	input.DeferredCauset(defCaus.Index).INTERLOCKyReconstruct(input.Sel(), result)
	return nil
}

// VecEvalJSON evaluates this expression in a vectorized manner.
func (defCaus *DeferredCauset) VecEvalJSON(ctx stochastikctx.Context, input *chunk.Chunk, result *chunk.DeferredCauset) error {
	input.DeferredCauset(defCaus.Index).INTERLOCKyReconstruct(input.Sel(), result)
	return nil
}

const defCausumnPrefix = "DeferredCauset#"

// String implements Stringer interface.
func (defCaus *DeferredCauset) String() string {
	if defCaus.OrigName != "" {
		return defCaus.OrigName
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s%d", defCausumnPrefix, defCaus.UniqueID)
	return builder.String()
}

// MarshalJSON implements json.Marshaler interface.
func (defCaus *DeferredCauset) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", defCaus)), nil
}

// GetType implements Expression interface.
func (defCaus *DeferredCauset) GetType() *types.FieldType {
	return defCaus.RetType
}

// Eval implements Expression interface.
func (defCaus *DeferredCauset) Eval(event chunk.Event) (types.Causet, error) {
	return event.GetCauset(defCaus.Index, defCaus.RetType), nil
}

// EvalInt returns int representation of DeferredCauset.
func (defCaus *DeferredCauset) EvalInt(ctx stochastikctx.Context, event chunk.Event) (int64, bool, error) {
	if defCaus.GetType().Hybrid() {
		val := event.GetCauset(defCaus.Index, defCaus.RetType)
		if val.IsNull() {
			return 0, true, nil
		}
		if val.HoTT() == types.HoTTMysqlBit {
			val, err := val.GetBinaryLiteral().ToInt(ctx.GetStochaseinstein_dbars().StmtCtx)
			return int64(val), err != nil, err
		}
		res, err := val.ToInt64(ctx.GetStochaseinstein_dbars().StmtCtx)
		return res, err != nil, err
	}
	if event.IsNull(defCaus.Index) {
		return 0, true, nil
	}
	return event.GetInt64(defCaus.Index), false, nil
}

// EvalReal returns real representation of DeferredCauset.
func (defCaus *DeferredCauset) EvalReal(ctx stochastikctx.Context, event chunk.Event) (float64, bool, error) {
	if event.IsNull(defCaus.Index) {
		return 0, true, nil
	}
	if defCaus.GetType().Tp == allegrosql.TypeFloat {
		return float64(event.GetFloat32(defCaus.Index)), false, nil
	}
	return event.GetFloat64(defCaus.Index), false, nil
}

// EvalString returns string representation of DeferredCauset.
func (defCaus *DeferredCauset) EvalString(ctx stochastikctx.Context, event chunk.Event) (string, bool, error) {
	if event.IsNull(defCaus.Index) {
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	if defCaus.GetType().Hybrid() {
		val := event.GetCauset(defCaus.Index, defCaus.RetType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	val := event.GetString(defCaus.Index)
	return val, false, nil
}

// EvalDecimal returns decimal representation of DeferredCauset.
func (defCaus *DeferredCauset) EvalDecimal(ctx stochastikctx.Context, event chunk.Event) (*types.MyDecimal, bool, error) {
	if event.IsNull(defCaus.Index) {
		return nil, true, nil
	}
	return event.GetMyDecimal(defCaus.Index), false, nil
}

// EvalTime returns DATE/DATETIME/TIMESTAMP representation of DeferredCauset.
func (defCaus *DeferredCauset) EvalTime(ctx stochastikctx.Context, event chunk.Event) (types.Time, bool, error) {
	if event.IsNull(defCaus.Index) {
		return types.ZeroTime, true, nil
	}
	return event.GetTime(defCaus.Index), false, nil
}

// EvalDuration returns Duration representation of DeferredCauset.
func (defCaus *DeferredCauset) EvalDuration(ctx stochastikctx.Context, event chunk.Event) (types.Duration, bool, error) {
	if event.IsNull(defCaus.Index) {
		return types.Duration{}, true, nil
	}
	duration := event.GetDuration(defCaus.Index, defCaus.RetType.Decimal)
	return duration, false, nil
}

// EvalJSON returns JSON representation of DeferredCauset.
func (defCaus *DeferredCauset) EvalJSON(ctx stochastikctx.Context, event chunk.Event) (json.BinaryJSON, bool, error) {
	if event.IsNull(defCaus.Index) {
		return json.BinaryJSON{}, true, nil
	}
	return event.GetJSON(defCaus.Index), false, nil
}

// Clone implements Expression interface.
func (defCaus *DeferredCauset) Clone() Expression {
	newDefCaus := *defCaus
	return &newDefCaus
}

// IsCorrelated implements Expression interface.
func (defCaus *DeferredCauset) IsCorrelated() bool {
	return false
}

// ConstItem implements Expression interface.
func (defCaus *DeferredCauset) ConstItem(_ *stmtctx.StatementContext) bool {
	return false
}

// Decorrelate implements Expression interface.
func (defCaus *DeferredCauset) Decorrelate(_ *Schema) Expression {
	return defCaus
}

// HashCode implements Expression interface.
func (defCaus *DeferredCauset) HashCode(_ *stmtctx.StatementContext) []byte {
	if len(defCaus.hashcode) != 0 {
		return defCaus.hashcode
	}
	defCaus.hashcode = make([]byte, 0, 9)
	defCaus.hashcode = append(defCaus.hashcode, defCausumnFlag)
	defCaus.hashcode = codec.EncodeInt(defCaus.hashcode, defCaus.UniqueID)
	return defCaus.hashcode
}

// ResolveIndices implements Expression interface.
func (defCaus *DeferredCauset) ResolveIndices(schemaReplicant *Schema) (Expression, error) {
	newDefCaus := defCaus.Clone()
	err := newDefCaus.resolveIndices(schemaReplicant)
	return newDefCaus, err
}

func (defCaus *DeferredCauset) resolveIndices(schemaReplicant *Schema) error {
	defCaus.Index = schemaReplicant.DeferredCausetIndex(defCaus)
	if defCaus.Index == -1 {
		return errors.Errorf("Can't find defCausumn %s in schemaReplicant %s", defCaus, schemaReplicant)
	}
	return nil
}

// Vectorized returns if this expression supports vectorized evaluation.
func (defCaus *DeferredCauset) Vectorized() bool {
	return true
}

// ToInfo converts the expression.DeferredCauset to perceptron.DeferredCausetInfo for casting values,
// beware it doesn't fill all the fields of the perceptron.DeferredCausetInfo.
func (defCaus *DeferredCauset) ToInfo() *perceptron.DeferredCausetInfo {
	return &perceptron.DeferredCausetInfo{
		ID:        defCaus.ID,
		FieldType: *defCaus.RetType,
	}
}

// DeferredCauset2Exprs will transfer defCausumn slice to expression slice.
func DeferredCauset2Exprs(defcaus []*DeferredCauset) []Expression {
	result := make([]Expression, 0, len(defcaus))
	for _, defCaus := range defcaus {
		result = append(result, defCaus)
	}
	return result
}

// DefCausInfo2DefCaus finds the corresponding defCausumn of the DeferredCausetInfo in a defCausumn slice.
func DefCausInfo2DefCaus(defcaus []*DeferredCauset, defCaus *perceptron.DeferredCausetInfo) *DeferredCauset {
	for _, c := range defcaus {
		if c.ID == defCaus.ID {
			return c
		}
	}
	return nil
}

// indexDefCaus2DefCaus finds the corresponding defCausumn of the IndexDeferredCauset in a defCausumn slice.
func indexDefCaus2DefCaus(defCausInfos []*perceptron.DeferredCausetInfo, defcaus []*DeferredCauset, defCaus *perceptron.IndexDeferredCauset) *DeferredCauset {
	for i, info := range defCausInfos {
		if info.Name.L == defCaus.Name.L {
			return defcaus[i]
		}
	}
	return nil
}

// IndexInfo2PrefixDefCauss gets the corresponding []*DeferredCauset of the indexInfo's []*IndexDeferredCauset,
// together with a []int containing their lengths.
// If this index has three IndexDeferredCauset that the 1st and 3rd IndexDeferredCauset has corresponding *DeferredCauset,
// the return value will be only the 1st corresponding *DeferredCauset and its length.
// TODO: Use a struct to represent {*DeferredCauset, int}. And merge IndexInfo2PrefixDefCauss and IndexInfo2DefCauss.
func IndexInfo2PrefixDefCauss(defCausInfos []*perceptron.DeferredCausetInfo, defcaus []*DeferredCauset, index *perceptron.IndexInfo) ([]*DeferredCauset, []int) {
	retDefCauss := make([]*DeferredCauset, 0, len(index.DeferredCausets))
	lengths := make([]int, 0, len(index.DeferredCausets))
	for _, c := range index.DeferredCausets {
		defCaus := indexDefCaus2DefCaus(defCausInfos, defcaus, c)
		if defCaus == nil {
			return retDefCauss, lengths
		}
		retDefCauss = append(retDefCauss, defCaus)
		if c.Length != types.UnspecifiedLength && c.Length == defCaus.RetType.Flen {
			lengths = append(lengths, types.UnspecifiedLength)
		} else {
			lengths = append(lengths, c.Length)
		}
	}
	return retDefCauss, lengths
}

// IndexInfo2DefCauss gets the corresponding []*DeferredCauset of the indexInfo's []*IndexDeferredCauset,
// together with a []int containing their lengths.
// If this index has three IndexDeferredCauset that the 1st and 3rd IndexDeferredCauset has corresponding *DeferredCauset,
// the return value will be [defCaus1, nil, defCaus2].
func IndexInfo2DefCauss(defCausInfos []*perceptron.DeferredCausetInfo, defcaus []*DeferredCauset, index *perceptron.IndexInfo) ([]*DeferredCauset, []int) {
	retDefCauss := make([]*DeferredCauset, 0, len(index.DeferredCausets))
	lens := make([]int, 0, len(index.DeferredCausets))
	for _, c := range index.DeferredCausets {
		defCaus := indexDefCaus2DefCaus(defCausInfos, defcaus, c)
		if defCaus == nil {
			retDefCauss = append(retDefCauss, defCaus)
			lens = append(lens, types.UnspecifiedLength)
			continue
		}
		retDefCauss = append(retDefCauss, defCaus)
		if c.Length != types.UnspecifiedLength && c.Length == defCaus.RetType.Flen {
			lens = append(lens, types.UnspecifiedLength)
		} else {
			lens = append(lens, c.Length)
		}
	}
	return retDefCauss, lens
}

// FindPrefixOfIndex will find defCausumns in index by checking the unique id.
// So it will return at once no matching defCausumn is found.
func FindPrefixOfIndex(defcaus []*DeferredCauset, idxDefCausIDs []int64) []*DeferredCauset {
	retDefCauss := make([]*DeferredCauset, 0, len(idxDefCausIDs))
idLoop:
	for _, id := range idxDefCausIDs {
		for _, defCaus := range defcaus {
			if defCaus.UniqueID == id {
				retDefCauss = append(retDefCauss, defCaus)
				continue idLoop
			}
		}
		// If no matching defCausumn is found, just return.
		return retDefCauss
	}
	return retDefCauss
}

// EvalVirtualDeferredCauset evals the virtual defCausumn
func (defCaus *DeferredCauset) EvalVirtualDeferredCauset(event chunk.Event) (types.Causet, error) {
	return defCaus.VirtualExpr.Eval(event)
}

// SupportReverseEval checks whether the builtinFunc support reverse evaluation.
func (defCaus *DeferredCauset) SupportReverseEval() bool {
	switch defCaus.RetType.Tp {
	case allegrosql.TypeShort, allegrosql.TypeLong, allegrosql.TypeLonglong,
		allegrosql.TypeFloat, allegrosql.TypeDouble, allegrosql.TypeNewDecimal:
		return true
	}
	return false
}

// ReverseEval evaluates the only one defCausumn value with given function result.
func (defCaus *DeferredCauset) ReverseEval(sc *stmtctx.StatementContext, res types.Causet, rType types.RoundingType) (val types.Causet, err error) {
	return types.ChangeReverseResultByUpperLowerBound(sc, defCaus.RetType, res, rType)
}

// Coercibility returns the coercibility value which is used to check defCauslations.
func (defCaus *DeferredCauset) Coercibility() Coercibility {
	if defCaus.HasCoercibility() {
		return defCaus.defCauslationInfo.Coercibility()
	}
	defCaus.SetCoercibility(deriveCoercibilityForDeferredCauset(defCaus))
	return defCaus.defCauslationInfo.Coercibility()
}

// SortDeferredCausets sort defCausumns based on UniqueID.
func SortDeferredCausets(defcaus []*DeferredCauset) []*DeferredCauset {
	sorted := make([]*DeferredCauset, len(defcaus))
	INTERLOCKy(sorted, defcaus)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].UniqueID < sorted[j].UniqueID
	})
	return sorted
}
