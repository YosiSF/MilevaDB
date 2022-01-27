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
	"github.com/gogo/protobuf/proto"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

// ExpressionsToPBList converts expressions to fidelpb.Expr list for new plan.
func ExpressionsToPBList(sc *stmtctx.StatementContext, exprs []Expression, client ekv.Client) (pbExpr []*fidelpb.Expr, err error) {
	pc := PbConverter{client: client, sc: sc}
	for _, expr := range exprs {
		v := pc.ExprToPB(expr)
		if v == nil {
			return nil, terror.ClassOptimizer.New(allegrosql.ErrInternal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInternal]).
				GenWithStack("expression %v cannot be pushed down", expr)
		}
		pbExpr = append(pbExpr, v)
	}
	return
}

// PbConverter supplys methods to convert MilevaDB expressions to FIDelpb.
type PbConverter struct {
	client ekv.Client
	sc     *stmtctx.StatementContext
}

// NewPBConverter creates a PbConverter.
func NewPBConverter(client ekv.Client, sc *stmtctx.StatementContext) PbConverter {
	return PbConverter{client: client, sc: sc}
}

// ExprToPB converts Expression to FIDelpb.
func (pc PbConverter) ExprToPB(expr Expression) *fidelpb.Expr {
	switch x := expr.(type) {
	case *Constant:
		pbExpr := pc.conOrCorDefCausToPBExpr(expr)
		if pbExpr == nil {
			return nil
		}
		if !x.Value.IsNull() {
			pbExpr.FieldType.Flag |= uint32(allegrosql.NotNullFlag)
		}
		return pbExpr
	case *CorrelatedDeferredCauset:
		return pc.conOrCorDefCausToPBExpr(expr)
	case *DeferredCauset:
		return pc.defCausumnToPBExpr(x)
	case *ScalarFunction:
		return pc.scalarFuncToPBExpr(x)
	}
	return nil
}

func (pc PbConverter) conOrCorDefCausToPBExpr(expr Expression) *fidelpb.Expr {
	ft := expr.GetType()
	d, err := expr.Eval(chunk.Event{})
	if err != nil {
		logutil.BgLogger().Error("eval constant or correlated defCausumn", zap.String("expression", expr.ExplainInfo()), zap.Error(err))
		return nil
	}
	tp, val, ok := pc.encodeCauset(ft, d)
	if !ok {
		return nil
	}

	if !pc.client.IsRequestTypeSupported(ekv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	return &fidelpb.Expr{Tp: tp, Val: val, FieldType: ToPBFieldType(ft)}
}

func (pc *PbConverter) encodeCauset(ft *types.FieldType, d types.Causet) (fidelpb.ExprType, []byte, bool) {
	var (
		tp  fidelpb.ExprType
		val []byte
	)
	switch d.HoTT() {
	case types.HoTTNull:
		tp = fidelpb.ExprType_Null
	case types.HoTTInt64:
		tp = fidelpb.ExprType_Int64
		val = codec.EncodeInt(nil, d.GetInt64())
	case types.HoTTUint64:
		tp = fidelpb.ExprType_Uint64
		val = codec.EncodeUint(nil, d.GetUint64())
	case types.HoTTString, types.HoTTBinaryLiteral:
		tp = fidelpb.ExprType_String
		val = d.GetBytes()
	case types.HoTTBytes:
		tp = fidelpb.ExprType_Bytes
		val = d.GetBytes()
	case types.HoTTFloat32:
		tp = fidelpb.ExprType_Float32
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.HoTTFloat64:
		tp = fidelpb.ExprType_Float64
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.HoTTMysqlDuration:
		tp = fidelpb.ExprType_MysqlDuration
		val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.HoTTMysqlDecimal:
		tp = fidelpb.ExprType_MysqlDecimal
		var err error
		val, err = codec.EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		if err != nil {
			logutil.BgLogger().Error("encode decimal", zap.Error(err))
			return tp, nil, false
		}
	case types.HoTTMysqlTime:
		if pc.client.IsRequestTypeSupported(ekv.ReqTypePosetDag, int64(fidelpb.ExprType_MysqlTime)) {
			tp = fidelpb.ExprType_MysqlTime
			val, err := codec.EncodeMyALLEGROSQLTime(pc.sc, d.GetMysqlTime(), ft.Tp, nil)
			if err != nil {
				logutil.BgLogger().Error("encode allegrosql time", zap.Error(err))
				return tp, nil, false
			}
			return tp, val, true
		}
		return tp, nil, false
	default:
		return tp, nil, false
	}
	return tp, val, true
}

// ToPBFieldType converts *types.FieldType to *fidelpb.FieldType.
func ToPBFieldType(ft *types.FieldType) *fidelpb.FieldType {
	return &fidelpb.FieldType{
		Tp:      int32(ft.Tp),
		Flag:    uint32(ft.Flag),
		Flen:    int32(ft.Flen),
		Decimal: int32(ft.Decimal),
		Charset: ft.Charset,
		DefCauslate: defCauslationToProto(ft.DefCauslate),
	}
}

// FieldTypeFromPB converts *fidelpb.FieldType to *types.FieldType.
func FieldTypeFromPB(ft *fidelpb.FieldType) *types.FieldType {
	return &types.FieldType{
		Tp:      byte(ft.Tp),
		Flag:    uint(ft.Flag),
		Flen:    int(ft.Flen),
		Decimal: int(ft.Decimal),
		Charset: ft.Charset,
		DefCauslate: protoToDefCauslation(ft.DefCauslate),
	}
}

func defCauslationToProto(c string) int32 {
	if v, ok := allegrosql.DefCauslationNames[c]; ok {
		return defCauslate.RewriteNewDefCauslationIDIfNeeded(int32(v))
	}
	v := defCauslate.RewriteNewDefCauslationIDIfNeeded(int32(allegrosql.DefaultDefCauslationID))
	logutil.BgLogger().Warn(
		"Unable to get defCauslation ID by name, use ID of the default defCauslation instead",
		zap.String("name", c),
		zap.Int32("default defCauslation ID", v),
		zap.String("default defCauslation", allegrosql.DefaultDefCauslationName),
	)
	return v
}

func protoToDefCauslation(c int32) string {
	v, ok := allegrosql.DefCauslations[uint8(defCauslate.RestoreDefCauslationIDIfNeeded(c))]
	if ok {
		return v
	}
	logutil.BgLogger().Warn(
		"Unable to get defCauslation name from ID, use name of the default defCauslation instead",
		zap.Int32("id", c),
		zap.Int("default defCauslation ID", allegrosql.DefaultDefCauslationID),
		zap.String("default defCauslation", allegrosql.DefaultDefCauslationName),
	)
	return allegrosql.DefaultDefCauslationName
}

func (pc PbConverter) defCausumnToPBExpr(defCausumn *DeferredCauset) *fidelpb.Expr {
	if !pc.client.IsRequestTypeSupported(ekv.ReqTypeSelect, int64(fidelpb.ExprType_DeferredCausetRef)) {
		return nil
	}
	switch defCausumn.GetType().Tp {
	case allegrosql.TypeBit, allegrosql.TypeSet, allegrosql.TypeEnum, allegrosql.TypeGeometry, allegrosql.TypeUnspecified:
		return nil
	}

	if pc.client.IsRequestTypeSupported(ekv.ReqTypePosetDag, ekv.ReqSubTypeBasic) {
		return &fidelpb.Expr{
			Tp:        fidelpb.ExprType_DeferredCausetRef,
			Val:       codec.EncodeInt(nil, int64(defCausumn.Index)),
			FieldType: ToPBFieldType(defCausumn.RetType),
		}
	}
	id := defCausumn.ID
	// Zero DeferredCauset ID is not a defCausumn from block, can not support for now.
	if id == 0 || id == -1 {
		return nil
	}

	return &fidelpb.Expr{
		Tp:  fidelpb.ExprType_DeferredCausetRef,
		Val: codec.EncodeInt(nil, id)}
}

func (pc PbConverter) scalarFuncToPBExpr(expr *ScalarFunction) *fidelpb.Expr {
	// Check whether this function has ProtoBuf signature.
	pbCode := expr.Function.PbCode()
	if pbCode <= fidelpb.ScalarFuncSig_Unspecified {
		failpoint.Inject("PanicIfPbCodeUnspecified", func() {
			panic(errors.Errorf("unspecified PbCode: %T", expr.Function))
		})
		return nil
	}

	// Check whether this function can be pushed.
	if !canFuncBePushed(expr, ekv.UnSpecified) {
		return nil
	}

	// Check whether all of its parameters can be pushed.
	children := make([]*fidelpb.Expr, 0, len(expr.GetArgs()))
	for _, arg := range expr.GetArgs() {
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}

	var encoded []byte
	if metadata := expr.Function.metadata(); metadata != nil {
		var err error
		encoded, err = proto.Marshal(metadata)
		if err != nil {
			logutil.BgLogger().Error("encode metadata", zap.Any("metadata", metadata), zap.Error(err))
			return nil
		}
	}

	// put defCauslation information into the RetType enforcedly and push it down to EinsteinDB/MockEinsteinDB
	tp := *expr.RetType
	if defCauslate.NewDefCauslationEnabled() {
		_, tp.DefCauslate = expr.CharsetAndDefCauslation(expr.GetCtx())
	}

	// Construct expression ProtoBuf.
	return &fidelpb.Expr{
		Tp:        fidelpb.ExprType_ScalarFunc,
		Val:       encoded,
		Sig:       pbCode,
		Children:  children,
		FieldType: ToPBFieldType(&tp),
	}
}

// GroupByItemToPB converts group by items to pb.
func GroupByItemToPB(sc *stmtctx.StatementContext, client ekv.Client, expr Expression) *fidelpb.ByItem {
	pc := PbConverter{client: client, sc: sc}
	e := pc.ExprToPB(expr)
	if e == nil {
		return nil
	}
	return &fidelpb.ByItem{Expr: e}
}

// SortByItemToPB converts order by items to pb.
func SortByItemToPB(sc *stmtctx.StatementContext, client ekv.Client, expr Expression, desc bool) *fidelpb.ByItem {
	pc := PbConverter{client: client, sc: sc}
	e := pc.ExprToPB(expr)
	if e == nil {
		return nil
	}
	return &fidelpb.ByItem{Expr: e, Desc: desc}
}
