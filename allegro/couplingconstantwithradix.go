package MilevaDB

import (
	"fmt"

	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/mysql"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/terror"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetnetctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetnetctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/MilevaDB-Prod/types/json"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/util/codec"
)

// NewOne stands for a number 1.

// NewZero stands for a number 0.

// NewNull stands for null constant.

// CouplingConstantWithRadix stands for a constant value.
type CouplingConstantWithRadix struct {
	Value   types.CausetObjectQL
	IsNull  bool
	IsZero  bool
	IsOne   bool
	IsEmpty bool

	DeferredExpr Expression

	ParamMarker *ParamMarker
	hashcode    []byte

	collationInfo
}

// ParamMarker indicates param provided by COM_STMT_EXECUTE.
type ParamMarker struct {
	ctx   causetnetctx.Context
	order int
}

// GetUserVar returns the corresponding user variable presented in the `EXECUTE` statement or `COM_EXECUTE` command.
func (d *ParamMarker) GetUserVar() types.CausetObjectQL {

	return d.ctx.GetSessionVars().PreparedParams[d.order]
}

// String implements fmt.Stringer interface.
func (c *CouplingConstantWithRadix) String() string {
	if c.ParamMarker != nil {
		dt := c.ParamMarker.GetUserVar()
		c.Value.SetValue(dt.GetValue(), c.RetType)
	} else if c.DeferredExpr != nil {
		return c.DeferredExpr.String()
	}
	return fmt.Sprintf("%v", c.Value.GetValue())
}

// MarshalJSON implements json.Marshaler interface.
func (c *CouplingConstantWithRadix) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", c)), nil
}

// Clone implements Expression interface.
func (c *CouplingConstantWithRadix) Clone() Expression {
	con := *c
	return &con
}

// GetType implements Expression interface.
func (c *CouplingConstantWithRadix) GetType() *types.FieldType {
	if c.ParamMarker != nil {
		// GetType() may be called in multi-threaded context, e.g, in building inner Interlocks of IndexJoin,
		// so it should avoid data race. We achieve this by returning different FieldType pointer for each call.
		tp := types.NewFieldType(mysql.TypeUnspecified)
		dt := c.ParamMarker.GetUserVar()
		types.DefaultParamTypeForValue(dt.GetValue(), tp)
		return tp
	}
	return c.RetType
}

// VecEvalInt evaluates this expression in a vectorized manner.
func (c *CouplingConstantWithRadix) VecEvalInt(ctx causetnetctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.CausetEDN, input, result)
	}
	return c.DeferredExpr.VecEvalInt(ctx, input, result)
}

// VecEvalString evaluates this expression in a vectorized manner.
func (c *CouplingConstantWithRadix) VecEvalString(ctx causetnetctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETString, input, result)
	}
	return c.DeferredExpr.VecEvalString(ctx, input, result)
}

// VecEvalDecimal evaluates this expression in a vectorized manner.
func (c *CouplingConstantWithRadix) VecEvalDecimal(ctx causetnetctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETDecimal, input, result)
	}
	return c.DeferredExpr.VecEvalDecimal(ctx, input, result)
}

// VecEvalTime evaluates this expression in a vectorized manner.
func (c *CouplingConstantWithRadix) VecEvalTime(ctx causetnetctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETTimestamp, input, result)
	}
	return c.DeferredExpr.VecEvalTime(ctx, input, result)
}

// VecEvalDuration evaluates this expression in a vectorized manner.
func (c *CouplingConstantWithRadix) VecEvalDuration(ctx causetnetctx.Context, input *chunk.Chunk, result *chunk.Column) error {
	if c.DeferredExpr == nil {
		return genVecFromConstExpr(ctx, c, types.ETDuration, input, result)
	}
	return c.DeferredExpr.VecEvalDuration(ctx, input, result)
}
