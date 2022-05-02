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
	"strconv"
	"sync"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/codec"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

// PbTypeToFieldType converts fidelpb.FieldType to FieldType
func PbTypeToFieldType(tp *fidelpb.FieldType) *types.FieldType {
	return &types.FieldType{
		Tp:          byte(tp.Tp),
		Flag:        uint(tp.Flag),
		Flen:        int(tp.Flen),
		Decimal:     int(tp.Decimal),
		Charset:     tp.Charset,
		DefCauslate: protoToDefCauslation(tp.DefCauslate),
	}
}

func getSignatureByPB(ctx stochastikctx.Context, sigCode fidelpb.ScalarFuncSig, tp *fidelpb.FieldType, args []Expression) (f builtinFunc, e error) {
	fieldTp := PbTypeToFieldType(tp)
	base, err := newBaseBuiltinFuncWithFieldType(ctx, fieldTp, args)
	if err != nil {
		return nil, err
	}
	valStr, _ := ctx.GetStochaseinstein_dbars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	base.tp = fieldTp
	switch sigCode {
	case fidelpb.ScalarFuncSig_CastIntAsInt:
		f = &builtinCastIntAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastIntAsReal:
		f = &builtinCastIntAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastIntAsString:
		f = &builtinCastIntAsStringSig{base}
	case fidelpb.ScalarFuncSig_CastIntAsDecimal:
		f = &builtinCastIntAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastIntAsTime:
		f = &builtinCastIntAsTimeSig{base}
	case fidelpb.ScalarFuncSig_CastIntAsDuration:
		f = &builtinCastIntAsDurationSig{base}
	case fidelpb.ScalarFuncSig_CastIntAsJson:
		f = &builtinCastIntAsJSONSig{base}
	case fidelpb.ScalarFuncSig_CastRealAsInt:
		f = &builtinCastRealAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastRealAsReal:
		f = &builtinCastRealAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastRealAsString:
		f = &builtinCastRealAsStringSig{base}
	case fidelpb.ScalarFuncSig_CastRealAsDecimal:
		f = &builtinCastRealAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastRealAsTime:
		f = &builtinCastRealAsTimeSig{base}
	case fidelpb.ScalarFuncSig_CastRealAsDuration:
		f = &builtinCastRealAsDurationSig{base}
	case fidelpb.ScalarFuncSig_CastRealAsJson:
		f = &builtinCastRealAsJSONSig{base}
	case fidelpb.ScalarFuncSig_CastDecimalAsInt:
		f = &builtinCastDecimalAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastDecimalAsReal:
		f = &builtinCastDecimalAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastDecimalAsString:
		f = &builtinCastDecimalAsStringSig{base}
	case fidelpb.ScalarFuncSig_CastDecimalAsDecimal:
		f = &builtinCastDecimalAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastDecimalAsTime:
		f = &builtinCastDecimalAsTimeSig{base}
	case fidelpb.ScalarFuncSig_CastDecimalAsDuration:
		f = &builtinCastDecimalAsDurationSig{base}
	case fidelpb.ScalarFuncSig_CastDecimalAsJson:
		f = &builtinCastDecimalAsJSONSig{base}
	case fidelpb.ScalarFuncSig_CastStringAsInt:
		f = &builtinCastStringAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastStringAsReal:
		f = &builtinCastStringAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastStringAsString:
		f = &builtinCastStringAsStringSig{base}
	case fidelpb.ScalarFuncSig_CastStringAsDecimal:
		f = &builtinCastStringAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastStringAsTime:
		f = &builtinCastStringAsTimeSig{base}
	case fidelpb.ScalarFuncSig_CastStringAsDuration:
		f = &builtinCastStringAsDurationSig{base}
	case fidelpb.ScalarFuncSig_CastStringAsJson:
		f = &builtinCastStringAsJSONSig{base}
	case fidelpb.ScalarFuncSig_CastTimeAsInt:
		f = &builtinCastTimeAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastTimeAsReal:
		f = &builtinCastTimeAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastTimeAsString:
		f = &builtinCastTimeAsStringSig{base}
	case fidelpb.ScalarFuncSig_CastTimeAsDecimal:
		f = &builtinCastTimeAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastTimeAsTime:
		f = &builtinCastTimeAsTimeSig{base}
	case fidelpb.ScalarFuncSig_CastTimeAsDuration:
		f = &builtinCastTimeAsDurationSig{base}
	case fidelpb.ScalarFuncSig_CastTimeAsJson:
		f = &builtinCastTimeAsJSONSig{base}
	case fidelpb.ScalarFuncSig_CastDurationAsInt:
		f = &builtinCastDurationAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastDurationAsReal:
		f = &builtinCastDurationAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastDurationAsString:
		f = &builtinCastDurationAsStringSig{base}
	case fidelpb.ScalarFuncSig_CastDurationAsDecimal:
		f = &builtinCastDurationAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastDurationAsTime:
		f = &builtinCastDurationAsTimeSig{base}
	case fidelpb.ScalarFuncSig_CastDurationAsDuration:
		f = &builtinCastDurationAsDurationSig{base}
	case fidelpb.ScalarFuncSig_CastDurationAsJson:
		f = &builtinCastDurationAsJSONSig{base}
	case fidelpb.ScalarFuncSig_CastJsonAsInt:
		f = &builtinCastJSONAsIntSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastJsonAsReal:
		f = &builtinCastJSONAsRealSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastJsonAsString:
		f = &builtinCastJSONAsStringSig{base}
	case fidelpb.ScalarFuncSig_CastJsonAsDecimal:
		f = &builtinCastJSONAsDecimalSig{newBaseBuiltinCastFunc(base, false)}
	case fidelpb.ScalarFuncSig_CastJsonAsTime:
		f = &builtinCastJSONAsTimeSig{base}
	case fidelpb.ScalarFuncSig_CastJsonAsDuration:
		f = &builtinCastJSONAsDurationSig{base}
	case fidelpb.ScalarFuncSig_CastJsonAsJson:
		f = &builtinCastJSONAsJSONSig{base}
	case fidelpb.ScalarFuncSig_CoalesceInt:
		f = &builtinCoalesceIntSig{base}
	case fidelpb.ScalarFuncSig_CoalesceReal:
		f = &builtinCoalesceRealSig{base}
	case fidelpb.ScalarFuncSig_CoalesceDecimal:
		f = &builtinCoalesceDecimalSig{base}
	case fidelpb.ScalarFuncSig_CoalesceString:
		f = &builtinCoalesceStringSig{base}
	case fidelpb.ScalarFuncSig_CoalesceTime:
		f = &builtinCoalesceTimeSig{base}
	case fidelpb.ScalarFuncSig_CoalesceDuration:
		f = &builtinCoalesceDurationSig{base}
	case fidelpb.ScalarFuncSig_CoalesceJson:
		f = &builtinCoalesceJSONSig{base}
	case fidelpb.ScalarFuncSig_LTInt:
		f = &builtinLTIntSig{base}
	case fidelpb.ScalarFuncSig_LTReal:
		f = &builtinLTRealSig{base}
	case fidelpb.ScalarFuncSig_LTDecimal:
		f = &builtinLTDecimalSig{base}
	case fidelpb.ScalarFuncSig_LTString:
		f = &builtinLTStringSig{base}
	case fidelpb.ScalarFuncSig_LTTime:
		f = &builtinLTTimeSig{base}
	case fidelpb.ScalarFuncSig_LTDuration:
		f = &builtinLTDurationSig{base}
	case fidelpb.ScalarFuncSig_LTJson:
		f = &builtinLTJSONSig{base}
	case fidelpb.ScalarFuncSig_LEInt:
		f = &builtinLEIntSig{base}
	case fidelpb.ScalarFuncSig_LEReal:
		f = &builtinLERealSig{base}
	case fidelpb.ScalarFuncSig_LEDecimal:
		f = &builtinLEDecimalSig{base}
	case fidelpb.ScalarFuncSig_LEString:
		f = &builtinLEStringSig{base}
	case fidelpb.ScalarFuncSig_LETime:
		f = &builtinLETimeSig{base}
	case fidelpb.ScalarFuncSig_LEDuration:
		f = &builtinLEDurationSig{base}
	case fidelpb.ScalarFuncSig_LEJson:
		f = &builtinLEJSONSig{base}
	case fidelpb.ScalarFuncSig_GTInt:
		f = &builtinGTIntSig{base}
	case fidelpb.ScalarFuncSig_GTReal:
		f = &builtinGTRealSig{base}
	case fidelpb.ScalarFuncSig_GTDecimal:
		f = &builtinGTDecimalSig{base}
	case fidelpb.ScalarFuncSig_GTString:
		f = &builtinGTStringSig{base}
	case fidelpb.ScalarFuncSig_GTTime:
		f = &builtinGTTimeSig{base}
	case fidelpb.ScalarFuncSig_GTDuration:
		f = &builtinGTDurationSig{base}
	case fidelpb.ScalarFuncSig_GTJson:
		f = &builtinGTJSONSig{base}
	case fidelpb.ScalarFuncSig_GreatestInt:
		f = &builtinGreatestIntSig{base}
	case fidelpb.ScalarFuncSig_GreatestReal:
		f = &builtinGreatestRealSig{base}
	case fidelpb.ScalarFuncSig_GreatestDecimal:
		f = &builtinGreatestDecimalSig{base}
	case fidelpb.ScalarFuncSig_GreatestString:
		f = &builtinGreatestStringSig{base}
	case fidelpb.ScalarFuncSig_GreatestTime:
		f = &builtinGreatestTimeSig{base}
	case fidelpb.ScalarFuncSig_LeastInt:
		f = &builtinLeastIntSig{base}
	case fidelpb.ScalarFuncSig_LeastReal:
		f = &builtinLeastRealSig{base}
	case fidelpb.ScalarFuncSig_LeastDecimal:
		f = &builtinLeastDecimalSig{base}
	case fidelpb.ScalarFuncSig_LeastString:
		f = &builtinLeastStringSig{base}
	case fidelpb.ScalarFuncSig_LeastTime:
		f = &builtinLeastTimeSig{base}
	case fidelpb.ScalarFuncSig_IntervalInt:
		f = &builtinIntervalIntSig{base, false} // Since interval function won't be pushed down to EinsteinDB, therefore it doesn't matter what value we give to hasNullable
	case fidelpb.ScalarFuncSig_IntervalReal:
		f = &builtinIntervalRealSig{base, false}
	case fidelpb.ScalarFuncSig_GEInt:
		f = &builtinGEIntSig{base}
	case fidelpb.ScalarFuncSig_GEReal:
		f = &builtinGERealSig{base}
	case fidelpb.ScalarFuncSig_GEDecimal:
		f = &builtinGEDecimalSig{base}
	case fidelpb.ScalarFuncSig_GEString:
		f = &builtinGEStringSig{base}
	case fidelpb.ScalarFuncSig_GETime:
		f = &builtinGETimeSig{base}
	case fidelpb.ScalarFuncSig_GEDuration:
		f = &builtinGEDurationSig{base}
	case fidelpb.ScalarFuncSig_GEJson:
		f = &builtinGEJSONSig{base}
	case fidelpb.ScalarFuncSig_EQInt:
		f = &builtinEQIntSig{base}
	case fidelpb.ScalarFuncSig_EQReal:
		f = &builtinEQRealSig{base}
	case fidelpb.ScalarFuncSig_EQDecimal:
		f = &builtinEQDecimalSig{base}
	case fidelpb.ScalarFuncSig_EQString:
		f = &builtinEQStringSig{base}
	case fidelpb.ScalarFuncSig_EQTime:
		f = &builtinEQTimeSig{base}
	case fidelpb.ScalarFuncSig_EQDuration:
		f = &builtinEQDurationSig{base}
	case fidelpb.ScalarFuncSig_EQJson:
		f = &builtinEQJSONSig{base}
	case fidelpb.ScalarFuncSig_NEInt:
		f = &builtinNEIntSig{base}
	case fidelpb.ScalarFuncSig_NEReal:
		f = &builtinNERealSig{base}
	case fidelpb.ScalarFuncSig_NEDecimal:
		f = &builtinNEDecimalSig{base}
	case fidelpb.ScalarFuncSig_NEString:
		f = &builtinNEStringSig{base}
	case fidelpb.ScalarFuncSig_NETime:
		f = &builtinNETimeSig{base}
	case fidelpb.ScalarFuncSig_NEDuration:
		f = &builtinNEDurationSig{base}
	case fidelpb.ScalarFuncSig_NEJson:
		f = &builtinNEJSONSig{base}
	case fidelpb.ScalarFuncSig_NullEQInt:
		f = &builtinNullEQIntSig{base}
	case fidelpb.ScalarFuncSig_NullEQReal:
		f = &builtinNullEQRealSig{base}
	case fidelpb.ScalarFuncSig_NullEQDecimal:
		f = &builtinNullEQDecimalSig{base}
	case fidelpb.ScalarFuncSig_NullEQString:
		f = &builtinNullEQStringSig{base}
	case fidelpb.ScalarFuncSig_NullEQTime:
		f = &builtinNullEQTimeSig{base}
	case fidelpb.ScalarFuncSig_NullEQDuration:
		f = &builtinNullEQDurationSig{base}
	case fidelpb.ScalarFuncSig_NullEQJson:
		f = &builtinNullEQJSONSig{base}
	case fidelpb.ScalarFuncSig_PlusReal:
		f = &builtinArithmeticPlusRealSig{base}
	case fidelpb.ScalarFuncSig_PlusDecimal:
		f = &builtinArithmeticPlusDecimalSig{base}
	case fidelpb.ScalarFuncSig_PlusInt:
		f = &builtinArithmeticPlusIntSig{base}
	case fidelpb.ScalarFuncSig_MinusReal:
		f = &builtinArithmeticMinusRealSig{base}
	case fidelpb.ScalarFuncSig_MinusDecimal:
		f = &builtinArithmeticMinusDecimalSig{base}
	case fidelpb.ScalarFuncSig_MinusInt:
		f = &builtinArithmeticMinusIntSig{base}
	case fidelpb.ScalarFuncSig_MultiplyReal:
		f = &builtinArithmeticMultiplyRealSig{base}
	case fidelpb.ScalarFuncSig_MultiplyDecimal:
		f = &builtinArithmeticMultiplyDecimalSig{base}
	case fidelpb.ScalarFuncSig_MultiplyInt:
		f = &builtinArithmeticMultiplyIntSig{base}
	case fidelpb.ScalarFuncSig_DivideReal:
		f = &builtinArithmeticDivideRealSig{base}
	case fidelpb.ScalarFuncSig_DivideDecimal:
		f = &builtinArithmeticDivideDecimalSig{base}
	case fidelpb.ScalarFuncSig_IntDivideInt:
		f = &builtinArithmeticIntDivideIntSig{base}
	case fidelpb.ScalarFuncSig_IntDivideDecimal:
		f = &builtinArithmeticIntDivideDecimalSig{base}
	case fidelpb.ScalarFuncSig_ModReal:
		f = &builtinArithmeticModRealSig{base}
	case fidelpb.ScalarFuncSig_ModDecimal:
		f = &builtinArithmeticModDecimalSig{base}
	case fidelpb.ScalarFuncSig_ModInt:
		f = &builtinArithmeticModIntSig{base}
	case fidelpb.ScalarFuncSig_MultiplyIntUnsigned:
		f = &builtinArithmeticMultiplyIntUnsignedSig{base}
	case fidelpb.ScalarFuncSig_AbsInt:
		f = &builtinAbsIntSig{base}
	case fidelpb.ScalarFuncSig_AbsUInt:
		f = &builtinAbsUIntSig{base}
	case fidelpb.ScalarFuncSig_AbsReal:
		f = &builtinAbsRealSig{base}
	case fidelpb.ScalarFuncSig_AbsDecimal:
		f = &builtinAbsDecSig{base}
	case fidelpb.ScalarFuncSig_CeilIntToDec:
		f = &builtinCeilIntToDecSig{base}
	case fidelpb.ScalarFuncSig_CeilIntToInt:
		f = &builtinCeilIntToIntSig{base}
	case fidelpb.ScalarFuncSig_CeilDecToInt:
		f = &builtinCeilDecToIntSig{base}
	case fidelpb.ScalarFuncSig_CeilDecToDec:
		f = &builtinCeilDecToDecSig{base}
	case fidelpb.ScalarFuncSig_CeilReal:
		f = &builtinCeilRealSig{base}
	case fidelpb.ScalarFuncSig_FloorIntToDec:
		f = &builtinFloorIntToDecSig{base}
	case fidelpb.ScalarFuncSig_FloorIntToInt:
		f = &builtinFloorIntToIntSig{base}
	case fidelpb.ScalarFuncSig_FloorDecToInt:
		f = &builtinFloorDecToIntSig{base}
	case fidelpb.ScalarFuncSig_FloorDecToDec:
		f = &builtinFloorDecToDecSig{base}
	case fidelpb.ScalarFuncSig_FloorReal:
		f = &builtinFloorRealSig{base}
	case fidelpb.ScalarFuncSig_RoundReal:
		f = &builtinRoundRealSig{base}
	case fidelpb.ScalarFuncSig_RoundInt:
		f = &builtinRoundIntSig{base}
	case fidelpb.ScalarFuncSig_RoundDec:
		f = &builtinRoundDecSig{base}
	case fidelpb.ScalarFuncSig_RoundWithFracReal:
		f = &builtinRoundWithFracRealSig{base}
	case fidelpb.ScalarFuncSig_RoundWithFracInt:
		f = &builtinRoundWithFracIntSig{base}
	case fidelpb.ScalarFuncSig_RoundWithFracDec:
		f = &builtinRoundWithFracDecSig{base}
	case fidelpb.ScalarFuncSig_Log1Arg:
		f = &builtinLog1ArgSig{base}
	case fidelpb.ScalarFuncSig_Log2Args:
		f = &builtinLog2ArgsSig{base}
	case fidelpb.ScalarFuncSig_Log2:
		f = &builtinLog2Sig{base}
	case fidelpb.ScalarFuncSig_Log10:
		f = &builtinLog10Sig{base}
	//case fidelpb.ScalarFuncSig_Rand:
	case fidelpb.ScalarFuncSig_RandWithSeedFirstGen:
		f = &builtinRandWithSeedFirstGenSig{base}
	case fidelpb.ScalarFuncSig_Pow:
		f = &builtinPowSig{base}
	case fidelpb.ScalarFuncSig_Conv:
		f = &builtinConvSig{base}
	case fidelpb.ScalarFuncSig_CRC32:
		f = &builtinCRC32Sig{base}
	case fidelpb.ScalarFuncSig_Sign:
		f = &builtinSignSig{base}
	case fidelpb.ScalarFuncSig_Sqrt:
		f = &builtinSqrtSig{base}
	case fidelpb.ScalarFuncSig_Acos:
		f = &builtinAcosSig{base}
	case fidelpb.ScalarFuncSig_Asin:
		f = &builtinAsinSig{base}
	case fidelpb.ScalarFuncSig_Atan1Arg:
		f = &builtinAtan1ArgSig{base}
	case fidelpb.ScalarFuncSig_Atan2Args:
		f = &builtinAtan2ArgsSig{base}
	case fidelpb.ScalarFuncSig_Cos:
		f = &builtinCosSig{base}
	case fidelpb.ScalarFuncSig_Cot:
		f = &builtinCotSig{base}
	case fidelpb.ScalarFuncSig_Degrees:
		f = &builtinDegreesSig{base}
	case fidelpb.ScalarFuncSig_Exp:
		f = &builtinExpSig{base}
	case fidelpb.ScalarFuncSig_PI:
		f = &builtinPISig{base}
	case fidelpb.ScalarFuncSig_Radians:
		f = &builtinRadiansSig{base}
	case fidelpb.ScalarFuncSig_Sin:
		f = &builtinSinSig{base}
	case fidelpb.ScalarFuncSig_Tan:
		f = &builtinTanSig{base}
	case fidelpb.ScalarFuncSig_TruncateInt:
		f = &builtinTruncateIntSig{base}
	case fidelpb.ScalarFuncSig_TruncateReal:
		f = &builtinTruncateRealSig{base}
	case fidelpb.ScalarFuncSig_TruncateDecimal:
		f = &builtinTruncateDecimalSig{base}
	case fidelpb.ScalarFuncSig_LogicalAnd:
		f = &builtinLogicAndSig{base}
	case fidelpb.ScalarFuncSig_LogicalOr:
		f = &builtinLogicOrSig{base}
	case fidelpb.ScalarFuncSig_LogicalXor:
		f = &builtinLogicXorSig{base}
	case fidelpb.ScalarFuncSig_UnaryNotInt:
		f = &builtinUnaryNotIntSig{base}
	case fidelpb.ScalarFuncSig_UnaryNotDecimal:
		f = &builtinUnaryNotDecimalSig{base}
	case fidelpb.ScalarFuncSig_UnaryNotReal:
		f = &builtinUnaryNotRealSig{base}
	case fidelpb.ScalarFuncSig_UnaryMinusInt:
		f = &builtinUnaryMinusIntSig{base}
	case fidelpb.ScalarFuncSig_UnaryMinusReal:
		f = &builtinUnaryMinusRealSig{base}
	case fidelpb.ScalarFuncSig_UnaryMinusDecimal:
		f = &builtinUnaryMinusDecimalSig{base, false}
	case fidelpb.ScalarFuncSig_DecimalIsNull:
		f = &builtinDecimalIsNullSig{base}
	case fidelpb.ScalarFuncSig_DurationIsNull:
		f = &builtinDurationIsNullSig{base}
	case fidelpb.ScalarFuncSig_RealIsNull:
		f = &builtinRealIsNullSig{base}
	case fidelpb.ScalarFuncSig_StringIsNull:
		f = &builtinStringIsNullSig{base}
	case fidelpb.ScalarFuncSig_TimeIsNull:
		f = &builtinTimeIsNullSig{base}
	case fidelpb.ScalarFuncSig_IntIsNull:
		f = &builtinIntIsNullSig{base}
	//case fidelpb.ScalarFuncSig_JsonIsNull:
	case fidelpb.ScalarFuncSig_BitAndSig:
		f = &builtinBitAndSig{base}
	case fidelpb.ScalarFuncSig_BitOrSig:
		f = &builtinBitOrSig{base}
	case fidelpb.ScalarFuncSig_BitXorSig:
		f = &builtinBitXorSig{base}
	case fidelpb.ScalarFuncSig_BitNegSig:
		f = &builtinBitNegSig{base}
	case fidelpb.ScalarFuncSig_IntIsTrue:
		f = &builtinIntIsTrueSig{base, false}
	case fidelpb.ScalarFuncSig_RealIsTrue:
		f = &builtinRealIsTrueSig{base, false}
	case fidelpb.ScalarFuncSig_DecimalIsTrue:
		f = &builtinDecimalIsTrueSig{base, false}
	case fidelpb.ScalarFuncSig_IntIsFalse:
		f = &builtinIntIsFalseSig{base, false}
	case fidelpb.ScalarFuncSig_RealIsFalse:
		f = &builtinRealIsFalseSig{base, false}
	case fidelpb.ScalarFuncSig_DecimalIsFalse:
		f = &builtinDecimalIsFalseSig{base, false}
	case fidelpb.ScalarFuncSig_IntIsTrueWithNull:
		f = &builtinIntIsTrueSig{base, true}
	case fidelpb.ScalarFuncSig_RealIsTrueWithNull:
		f = &builtinRealIsTrueSig{base, true}
	case fidelpb.ScalarFuncSig_DecimalIsTrueWithNull:
		f = &builtinDecimalIsTrueSig{base, true}
	case fidelpb.ScalarFuncSig_IntIsFalseWithNull:
		f = &builtinIntIsFalseSig{base, true}
	case fidelpb.ScalarFuncSig_RealIsFalseWithNull:
		f = &builtinRealIsFalseSig{base, true}
	case fidelpb.ScalarFuncSig_DecimalIsFalseWithNull:
		f = &builtinDecimalIsFalseSig{base, true}
	case fidelpb.ScalarFuncSig_LeftShift:
		f = &builtinLeftShiftSig{base}
	case fidelpb.ScalarFuncSig_RightShift:
		f = &builtinRightShiftSig{base}
	case fidelpb.ScalarFuncSig_BitCount:
		f = &builtinBitCountSig{base}
	case fidelpb.ScalarFuncSig_GetParamString:
		f = &builtinGetParamStringSig{base}
	case fidelpb.ScalarFuncSig_GetVar:
		f = &builtinGetVarSig{base}
	//case fidelpb.ScalarFuncSig_EventSig:
	case fidelpb.ScalarFuncSig_SetVar:
		f = &builtinSetVarSig{base}
	//case fidelpb.ScalarFuncSig_ValuesDecimal:
	//	f = &builtinValuesDecimalSig{base}
	//case fidelpb.ScalarFuncSig_ValuesDuration:
	//	f = &builtinValuesDurationSig{base}
	//case fidelpb.ScalarFuncSig_ValuesInt:
	//	f = &builtinValuesIntSig{base}
	//case fidelpb.ScalarFuncSig_ValuesJSON:
	//	f = &builtinValuesJSONSig{base}
	//case fidelpb.ScalarFuncSig_ValuesReal:
	//	f = &builtinValuesRealSig{base}
	//case fidelpb.ScalarFuncSig_ValuesString:
	//	f = &builtinValuesStringSig{base}
	//case fidelpb.ScalarFuncSig_ValuesTime:
	//	f = &builtinValuesTimeSig{base}
	case fidelpb.ScalarFuncSig_InInt:
		f = &builtinInIntSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case fidelpb.ScalarFuncSig_InReal:
		f = &builtinInRealSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case fidelpb.ScalarFuncSig_InDecimal:
		f = &builtinInDecimalSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case fidelpb.ScalarFuncSig_InString:
		f = &builtinInStringSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case fidelpb.ScalarFuncSig_InTime:
		f = &builtinInTimeSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case fidelpb.ScalarFuncSig_InDuration:
		f = &builtinInDurationSig{baseInSig: baseInSig{baseBuiltinFunc: base}}
	case fidelpb.ScalarFuncSig_InJson:
		f = &builtinInJSONSig{baseBuiltinFunc: base}
	case fidelpb.ScalarFuncSig_IfNullInt:
		f = &builtinIfNullIntSig{base}
	case fidelpb.ScalarFuncSig_IfNullReal:
		f = &builtinIfNullRealSig{base}
	case fidelpb.ScalarFuncSig_IfNullDecimal:
		f = &builtinIfNullDecimalSig{base}
	case fidelpb.ScalarFuncSig_IfNullString:
		f = &builtinIfNullStringSig{base}
	case fidelpb.ScalarFuncSig_IfNullTime:
		f = &builtinIfNullTimeSig{base}
	case fidelpb.ScalarFuncSig_IfNullDuration:
		f = &builtinIfNullDurationSig{base}
	case fidelpb.ScalarFuncSig_IfInt:
		f = &builtinIfIntSig{base}
	case fidelpb.ScalarFuncSig_IfReal:
		f = &builtinIfRealSig{base}
	case fidelpb.ScalarFuncSig_IfDecimal:
		f = &builtinIfDecimalSig{base}
	case fidelpb.ScalarFuncSig_IfString:
		f = &builtinIfStringSig{base}
	case fidelpb.ScalarFuncSig_IfTime:
		f = &builtinIfTimeSig{base}
	case fidelpb.ScalarFuncSig_IfDuration:
		f = &builtinIfDurationSig{base}
	case fidelpb.ScalarFuncSig_IfNullJson:
		f = &builtinIfNullJSONSig{base}
	case fidelpb.ScalarFuncSig_IfJson:
		f = &builtinIfJSONSig{base}
	case fidelpb.ScalarFuncSig_CaseWhenInt:
		f = &builtinCaseWhenIntSig{base}
	case fidelpb.ScalarFuncSig_CaseWhenReal:
		f = &builtinCaseWhenRealSig{base}
	case fidelpb.ScalarFuncSig_CaseWhenDecimal:
		f = &builtinCaseWhenDecimalSig{base}
	case fidelpb.ScalarFuncSig_CaseWhenString:
		f = &builtinCaseWhenStringSig{base}
	case fidelpb.ScalarFuncSig_CaseWhenTime:
		f = &builtinCaseWhenTimeSig{base}
	case fidelpb.ScalarFuncSig_CaseWhenDuration:
		f = &builtinCaseWhenDurationSig{base}
	case fidelpb.ScalarFuncSig_CaseWhenJson:
		f = &builtinCaseWhenJSONSig{base}
	//case fidelpb.ScalarFuncSig_AesDecrypt:
	//	f = &builtinAesDecryptSig{base}
	//case fidelpb.ScalarFuncSig_AesEncrypt:
	//	f = &builtinAesEncryptSig{base}
	case fidelpb.ScalarFuncSig_Compress:
		f = &builtinCompressSig{base}
	case fidelpb.ScalarFuncSig_MD5:
		f = &builtinMD5Sig{base}
	case fidelpb.ScalarFuncSig_Password:
		f = &builtinPasswordSig{base}
	case fidelpb.ScalarFuncSig_RandomBytes:
		f = &builtinRandomBytesSig{base}
	case fidelpb.ScalarFuncSig_SHA1:
		f = &builtinSHA1Sig{base}
	case fidelpb.ScalarFuncSig_SHA2:
		f = &builtinSHA2Sig{base}
	case fidelpb.ScalarFuncSig_Uncompress:
		f = &builtinUncompressSig{base}
	case fidelpb.ScalarFuncSig_UncompressedLength:
		f = &builtinUncompressedLengthSig{base}
	case fidelpb.ScalarFuncSig_Database:
		f = &builtinDatabaseSig{base}
	case fidelpb.ScalarFuncSig_FoundEvents:
		f = &builtinFoundEventsSig{base}
	case fidelpb.ScalarFuncSig_CurrentUser:
		f = &builtinCurrentUserSig{base}
	case fidelpb.ScalarFuncSig_User:
		f = &builtinUserSig{base}
	case fidelpb.ScalarFuncSig_ConnectionID:
		f = &builtinConnectionIDSig{base}
	case fidelpb.ScalarFuncSig_LastInsertID:
		f = &builtinLastInsertIDSig{base}
	case fidelpb.ScalarFuncSig_LastInsertIDWithID:
		f = &builtinLastInsertIDWithIDSig{base}
	case fidelpb.ScalarFuncSig_Version:
		f = &builtinVersionSig{base}
	case fidelpb.ScalarFuncSig_MilevaDBVersion:
		f = &builtinMilevaDBVersionSig{base}
	case fidelpb.ScalarFuncSig_EventCount:
		f = &builtinEventCountSig{base}
	case fidelpb.ScalarFuncSig_Sleep:
		f = &builtinSleepSig{base}
	case fidelpb.ScalarFuncSig_Lock:
		f = &builtinLockSig{base}
	case fidelpb.ScalarFuncSig_ReleaseLock:
		f = &builtinReleaseLockSig{base}
	case fidelpb.ScalarFuncSig_DecimalAnyValue:
		f = &builtinDecimalAnyValueSig{base}
	case fidelpb.ScalarFuncSig_DurationAnyValue:
		f = &builtinDurationAnyValueSig{base}
	case fidelpb.ScalarFuncSig_IntAnyValue:
		f = &builtinIntAnyValueSig{base}
	case fidelpb.ScalarFuncSig_JSONAnyValue:
		f = &builtinJSONAnyValueSig{base}
	case fidelpb.ScalarFuncSig_RealAnyValue:
		f = &builtinRealAnyValueSig{base}
	case fidelpb.ScalarFuncSig_StringAnyValue:
		f = &builtinStringAnyValueSig{base}
	case fidelpb.ScalarFuncSig_TimeAnyValue:
		f = &builtinTimeAnyValueSig{base}
	case fidelpb.ScalarFuncSig_InetAton:
		f = &builtinInetAtonSig{base}
	case fidelpb.ScalarFuncSig_InetNtoa:
		f = &builtinInetNtoaSig{base}
	case fidelpb.ScalarFuncSig_Inet6Aton:
		f = &builtinInet6AtonSig{base}
	case fidelpb.ScalarFuncSig_Inet6Ntoa:
		f = &builtinInet6NtoaSig{base}
	case fidelpb.ScalarFuncSig_IsIPv4:
		f = &builtinIsIPv4Sig{base}
	case fidelpb.ScalarFuncSig_IsIPv4Compat:
		f = &builtinIsIPv4CompatSig{base}
	case fidelpb.ScalarFuncSig_IsIPv4Mapped:
		f = &builtinIsIPv4MappedSig{base}
	case fidelpb.ScalarFuncSig_IsIPv6:
		f = &builtinIsIPv6Sig{base}
	case fidelpb.ScalarFuncSig_UUID:
		f = &builtinUUIDSig{base}
	case fidelpb.ScalarFuncSig_LikeSig:
		f = &builtinLikeSig{base, nil, false, sync.Once{}}
	//case fidelpb.ScalarFuncSig_RegexpSig:
	//	f = &builtinRegexpSig{base}
	//case fidelpb.ScalarFuncSig_RegexpUTF8Sig:
	//	f = &builtinRegexpUTF8Sig{base}
	case fidelpb.ScalarFuncSig_JsonExtractSig:
		f = &builtinJSONExtractSig{base}
	case fidelpb.ScalarFuncSig_JsonUnquoteSig:
		f = &builtinJSONUnquoteSig{base}
	case fidelpb.ScalarFuncSig_JsonTypeSig:
		f = &builtinJSONTypeSig{base}
	case fidelpb.ScalarFuncSig_JsonSetSig:
		f = &builtinJSONSetSig{base}
	case fidelpb.ScalarFuncSig_JsonInsertSig:
		f = &builtinJSONInsertSig{base}
	case fidelpb.ScalarFuncSig_JsonReplaceSig:
		f = &builtinJSONReplaceSig{base}
	case fidelpb.ScalarFuncSig_JsonRemoveSig:
		f = &builtinJSONRemoveSig{base}
	case fidelpb.ScalarFuncSig_JsonMergeSig:
		f = &builtinJSONMergeSig{base}
	case fidelpb.ScalarFuncSig_JsonObjectSig:
		f = &builtinJSONObjectSig{base}
	case fidelpb.ScalarFuncSig_JsonArraySig:
		f = &builtinJSONArraySig{base}
	case fidelpb.ScalarFuncSig_JsonValidJsonSig:
		f = &builtinJSONValidJSONSig{base}
	case fidelpb.ScalarFuncSig_JsonContainsSig:
		f = &builtinJSONContainsSig{base}
	case fidelpb.ScalarFuncSig_JsonArrayAppendSig:
		f = &builtinJSONArrayAppendSig{base}
	case fidelpb.ScalarFuncSig_JsonArrayInsertSig:
		f = &builtinJSONArrayInsertSig{base}
	//case fidelpb.ScalarFuncSig_JsonMergePatchSig:
	case fidelpb.ScalarFuncSig_JsonMergePreserveSig:
		f = &builtinJSONMergeSig{base}
	case fidelpb.ScalarFuncSig_JsonContainsPathSig:
		f = &builtinJSONContainsPathSig{base}
	//case fidelpb.ScalarFuncSig_JsonPrettySig:
	case fidelpb.ScalarFuncSig_JsonQuoteSig:
		f = &builtinJSONQuoteSig{base}
	case fidelpb.ScalarFuncSig_JsonSearchSig:
		f = &builtinJSONSearchSig{base}
	case fidelpb.ScalarFuncSig_JsonStorageSizeSig:
		f = &builtinJSONStorageSizeSig{base}
	case fidelpb.ScalarFuncSig_JsonDepthSig:
		f = &builtinJSONDepthSig{base}
	case fidelpb.ScalarFuncSig_JsonKeysSig:
		f = &builtinJSONKeysSig{base}
	case fidelpb.ScalarFuncSig_JsonLengthSig:
		f = &builtinJSONLengthSig{base}
	case fidelpb.ScalarFuncSig_JsonKeys2ArgsSig:
		f = &builtinJSONKeys2ArgsSig{base}
	case fidelpb.ScalarFuncSig_JsonValidStringSig:
		f = &builtinJSONValidStringSig{base}
	case fidelpb.ScalarFuncSig_JsonValidOthersSig:
		f = &builtinJSONValidOthersSig{base}
	case fidelpb.ScalarFuncSig_DateFormatSig:
		f = &builtinDateFormatSig{base}
	//case fidelpb.ScalarFuncSig_DateLiteral:
	//	f = &builtinDateLiteralSig{base}
	case fidelpb.ScalarFuncSig_DateDiff:
		f = &builtinDateDiffSig{base}
	case fidelpb.ScalarFuncSig_NullTimeDiff:
		f = &builtinNullTimeDiffSig{base}
	case fidelpb.ScalarFuncSig_TimeStringTimeDiff:
		f = &builtinTimeStringTimeDiffSig{base}
	case fidelpb.ScalarFuncSig_DurationStringTimeDiff:
		f = &builtinDurationStringTimeDiffSig{base}
	case fidelpb.ScalarFuncSig_DurationDurationTimeDiff:
		f = &builtinDurationDurationTimeDiffSig{base}
	case fidelpb.ScalarFuncSig_StringTimeTimeDiff:
		f = &builtinStringTimeTimeDiffSig{base}
	case fidelpb.ScalarFuncSig_StringDurationTimeDiff:
		f = &builtinStringDurationTimeDiffSig{base}
	case fidelpb.ScalarFuncSig_StringStringTimeDiff:
		f = &builtinStringStringTimeDiffSig{base}
	case fidelpb.ScalarFuncSig_TimeTimeTimeDiff:
		f = &builtinTimeTimeTimeDiffSig{base}
	case fidelpb.ScalarFuncSig_Date:
		f = &builtinDateSig{base}
	case fidelpb.ScalarFuncSig_Hour:
		f = &builtinHourSig{base}
	case fidelpb.ScalarFuncSig_Minute:
		f = &builtinMinuteSig{base}
	case fidelpb.ScalarFuncSig_Second:
		f = &builtinSecondSig{base}
	case fidelpb.ScalarFuncSig_MicroSecond:
		f = &builtinMicroSecondSig{base}
	case fidelpb.ScalarFuncSig_Month:
		f = &builtinMonthSig{base}
	case fidelpb.ScalarFuncSig_MonthName:
		f = &builtinMonthNameSig{base}
	case fidelpb.ScalarFuncSig_NowWithArg:
		f = &builtinNowWithArgSig{base}
	case fidelpb.ScalarFuncSig_NowWithoutArg:
		f = &builtinNowWithoutArgSig{base}
	case fidelpb.ScalarFuncSig_DayName:
		f = &builtinDayNameSig{base}
	case fidelpb.ScalarFuncSig_DayOfMonth:
		f = &builtinDayOfMonthSig{base}
	case fidelpb.ScalarFuncSig_DayOfWeek:
		f = &builtinDayOfWeekSig{base}
	case fidelpb.ScalarFuncSig_DayOfYear:
		f = &builtinDayOfYearSig{base}
	case fidelpb.ScalarFuncSig_WeekWithMode:
		f = &builtinWeekWithModeSig{base}
	case fidelpb.ScalarFuncSig_WeekWithoutMode:
		f = &builtinWeekWithoutModeSig{base}
	case fidelpb.ScalarFuncSig_WeekDay:
		f = &builtinWeekDaySig{base}
	case fidelpb.ScalarFuncSig_WeekOfYear:
		f = &builtinWeekOfYearSig{base}
	case fidelpb.ScalarFuncSig_Year:
		f = &builtinYearSig{base}
	case fidelpb.ScalarFuncSig_YearWeekWithMode:
		f = &builtinYearWeekWithModeSig{base}
	case fidelpb.ScalarFuncSig_YearWeekWithoutMode:
		f = &builtinYearWeekWithoutModeSig{base}
	case fidelpb.ScalarFuncSig_GetFormat:
		f = &builtinGetFormatSig{base}
	case fidelpb.ScalarFuncSig_SysDateWithFsp:
		f = &builtinSysDateWithFspSig{base}
	case fidelpb.ScalarFuncSig_SysDateWithoutFsp:
		f = &builtinSysDateWithoutFspSig{base}
	case fidelpb.ScalarFuncSig_CurrentDate:
		f = &builtinCurrentDateSig{base}
	case fidelpb.ScalarFuncSig_CurrentTime0Arg:
		f = &builtinCurrentTime0ArgSig{base}
	case fidelpb.ScalarFuncSig_CurrentTime1Arg:
		f = &builtinCurrentTime1ArgSig{base}
	case fidelpb.ScalarFuncSig_Time:
		f = &builtinTimeSig{base}
	//case fidelpb.ScalarFuncSig_TimeLiteral:
	//	f = &builtinTimeLiteralSig{base}
	case fidelpb.ScalarFuncSig_UTCDate:
		f = &builtinUTCDateSig{base}
	case fidelpb.ScalarFuncSig_UTCTimestampWithArg:
		f = &builtinUTCTimestampWithArgSig{base}
	case fidelpb.ScalarFuncSig_UTCTimestampWithoutArg:
		f = &builtinUTCTimestampWithoutArgSig{base}
	case fidelpb.ScalarFuncSig_AddDatetimeAndDuration:
		f = &builtinAddDatetimeAndDurationSig{base}
	case fidelpb.ScalarFuncSig_AddDatetimeAndString:
		f = &builtinAddDatetimeAndStringSig{base}
	case fidelpb.ScalarFuncSig_AddTimeDateTimeNull:
		f = &builtinAddTimeDateTimeNullSig{base}
	case fidelpb.ScalarFuncSig_AddStringAndDuration:
		f = &builtinAddStringAndDurationSig{base}
	case fidelpb.ScalarFuncSig_AddStringAndString:
		f = &builtinAddStringAndStringSig{base}
	case fidelpb.ScalarFuncSig_AddTimeStringNull:
		f = &builtinAddTimeStringNullSig{base}
	case fidelpb.ScalarFuncSig_AddDurationAndDuration:
		f = &builtinAddDurationAndDurationSig{base}
	case fidelpb.ScalarFuncSig_AddDurationAndString:
		f = &builtinAddDurationAndStringSig{base}
	case fidelpb.ScalarFuncSig_AddTimeDurationNull:
		f = &builtinAddTimeDurationNullSig{base}
	case fidelpb.ScalarFuncSig_AddDateAndDuration:
		f = &builtinAddDateAndDurationSig{base}
	case fidelpb.ScalarFuncSig_AddDateAndString:
		f = &builtinAddDateAndStringSig{base}
	case fidelpb.ScalarFuncSig_SubDatetimeAndDuration:
		f = &builtinSubDatetimeAndDurationSig{base}
	case fidelpb.ScalarFuncSig_SubDatetimeAndString:
		f = &builtinSubDatetimeAndStringSig{base}
	case fidelpb.ScalarFuncSig_SubTimeDateTimeNull:
		f = &builtinSubTimeDateTimeNullSig{base}
	case fidelpb.ScalarFuncSig_SubStringAndDuration:
		f = &builtinSubStringAndDurationSig{base}
	case fidelpb.ScalarFuncSig_SubStringAndString:
		f = &builtinSubStringAndStringSig{base}
	case fidelpb.ScalarFuncSig_SubTimeStringNull:
		f = &builtinSubTimeStringNullSig{base}
	case fidelpb.ScalarFuncSig_SubDurationAndDuration:
		f = &builtinSubDurationAndDurationSig{base}
	case fidelpb.ScalarFuncSig_SubDurationAndString:
		f = &builtinSubDurationAndStringSig{base}
	case fidelpb.ScalarFuncSig_SubTimeDurationNull:
		f = &builtinSubTimeDurationNullSig{base}
	case fidelpb.ScalarFuncSig_SubDateAndDuration:
		f = &builtinSubDateAndDurationSig{base}
	case fidelpb.ScalarFuncSig_SubDateAndString:
		f = &builtinSubDateAndStringSig{base}
	case fidelpb.ScalarFuncSig_UnixTimestampCurrent:
		f = &builtinUnixTimestampCurrentSig{base}
	case fidelpb.ScalarFuncSig_UnixTimestampInt:
		f = &builtinUnixTimestampIntSig{base}
	case fidelpb.ScalarFuncSig_UnixTimestamFIDelec:
		f = &builtinUnixTimestamFIDelecSig{base}
	//case fidelpb.ScalarFuncSig_ConvertTz:
	//	f = &builtinConvertTzSig{base}
	case fidelpb.ScalarFuncSig_MakeDate:
		f = &builtinMakeDateSig{base}
	case fidelpb.ScalarFuncSig_MakeTime:
		f = &builtinMakeTimeSig{base}
	case fidelpb.ScalarFuncSig_PeriodAdd:
		f = &builtinPeriodAddSig{base}
	case fidelpb.ScalarFuncSig_PeriodDiff:
		f = &builtinPeriodDiffSig{base}
	case fidelpb.ScalarFuncSig_Quarter:
		f = &builtinQuarterSig{base}
	case fidelpb.ScalarFuncSig_SecToTime:
		f = &builtinSecToTimeSig{base}
	case fidelpb.ScalarFuncSig_TimeToSec:
		f = &builtinTimeToSecSig{base}
	case fidelpb.ScalarFuncSig_TimestampAdd:
		f = &builtinTimestampAddSig{base}
	case fidelpb.ScalarFuncSig_ToDays:
		f = &builtinToDaysSig{base}
	case fidelpb.ScalarFuncSig_ToSeconds:
		f = &builtinToSecondsSig{base}
	case fidelpb.ScalarFuncSig_UTCTimeWithArg:
		f = &builtinUTCTimeWithArgSig{base}
	case fidelpb.ScalarFuncSig_UTCTimeWithoutArg:
		f = &builtinUTCTimeWithoutArgSig{base}
	//case fidelpb.ScalarFuncSig_Timestamp1Arg:
	//	f = &builtinTimestamp1ArgSig{base}
	//case fidelpb.ScalarFuncSig_Timestamp2Args:
	//	f = &builtinTimestamp2ArgsSig{base}
	//case fidelpb.ScalarFuncSig_TimestampLiteral:
	//	f = &builtinTimestampLiteralSig{base}
	case fidelpb.ScalarFuncSig_LastDay:
		f = &builtinLastDaySig{base}
	case fidelpb.ScalarFuncSig_StrToDateDate:
		f = &builtinStrToDateDateSig{base}
	case fidelpb.ScalarFuncSig_StrToDateDatetime:
		f = &builtinStrToDateDatetimeSig{base}
	case fidelpb.ScalarFuncSig_StrToDateDuration:
		f = &builtinStrToDateDurationSig{base}
	case fidelpb.ScalarFuncSig_FromUnixTime1Arg:
		f = &builtinFromUnixTime1ArgSig{base}
	case fidelpb.ScalarFuncSig_FromUnixTime2Arg:
		f = &builtinFromUnixTime2ArgSig{base}
	case fidelpb.ScalarFuncSig_ExtractDatetime:
		f = &builtinExtractDatetimeSig{base}
	case fidelpb.ScalarFuncSig_ExtractDuration:
		f = &builtinExtractDurationSig{base}
	//case fidelpb.ScalarFuncSig_AddDateStringString:
	//	f = &builtinAddDateStringStringSig{base}
	//case fidelpb.ScalarFuncSig_AddDateStringInt:
	//	f = &builtinAddDateStringIntSig{base}
	//case fidelpb.ScalarFuncSig_AddDateStringDecimal:
	//	f = &builtinAddDateStringDecimalSig{base}
	//case fidelpb.ScalarFuncSig_AddDateIntString:
	//	f = &builtinAddDateIntStringSig{base}
	//case fidelpb.ScalarFuncSig_AddDateIntInt:
	//	f = &builtinAddDateIntIntSig{base}
	//case fidelpb.ScalarFuncSig_AddDateDatetimeString:
	//	f = &builtinAddDateDatetimeStringSig{base}
	//case fidelpb.ScalarFuncSig_AddDateDatetimeInt:
	//	f = &builtinAddDateDatetimeIntSig{base}
	//case fidelpb.ScalarFuncSig_SubDateStringString:
	//	f = &builtinSubDateStringStringSig{base}
	//case fidelpb.ScalarFuncSig_SubDateStringInt:
	//	f = &builtinSubDateStringIntSig{base}
	//case fidelpb.ScalarFuncSig_SubDateStringDecimal:
	//	f = &builtinSubDateStringDecimalSig{base}
	//case fidelpb.ScalarFuncSig_SubDateIntString:
	//	f = &builtinSubDateIntStringSig{base}
	//case fidelpb.ScalarFuncSig_SubDateIntInt:
	//	f = &builtinSubDateIntIntSig{base}
	//case fidelpb.ScalarFuncSig_SubDateDatetimeString:
	//	f = &builtinSubDateDatetimeStringSig{base}
	//case fidelpb.ScalarFuncSig_SubDateDatetimeInt:
	//	f = &builtinSubDateDatetimeIntSig{base}
	case fidelpb.ScalarFuncSig_FromDays:
		f = &builtinFromDaysSig{base}
	case fidelpb.ScalarFuncSig_TimeFormat:
		f = &builtinTimeFormatSig{base}
	case fidelpb.ScalarFuncSig_TimestamFIDeliff:
		f = &builtinTimestamFIDeliffSig{base}
	case fidelpb.ScalarFuncSig_BitLength:
		f = &builtinBitLengthSig{base}
	case fidelpb.ScalarFuncSig_Bin:
		f = &builtinBinSig{base}
	case fidelpb.ScalarFuncSig_ASCII:
		f = &builtinASCIISig{base}
	case fidelpb.ScalarFuncSig_Char:
		f = &builtinCharSig{base}
	case fidelpb.ScalarFuncSig_CharLengthUTF8:
		f = &builtinCharLengthUTF8Sig{base}
	case fidelpb.ScalarFuncSig_Concat:
		f = &builtinConcatSig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_ConcatWS:
		f = &builtinConcatWSSig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_Convert:
		f = &builtinConvertSig{base}
	case fidelpb.ScalarFuncSig_Elt:
		f = &builtinEltSig{base}
	case fidelpb.ScalarFuncSig_ExportSet3Arg:
		f = &builtinExportSet3ArgSig{base}
	case fidelpb.ScalarFuncSig_ExportSet4Arg:
		f = &builtinExportSet4ArgSig{base}
	case fidelpb.ScalarFuncSig_ExportSet5Arg:
		f = &builtinExportSet5ArgSig{base}
	case fidelpb.ScalarFuncSig_FieldInt:
		f = &builtinFieldIntSig{base}
	case fidelpb.ScalarFuncSig_FieldReal:
		f = &builtinFieldRealSig{base}
	case fidelpb.ScalarFuncSig_FieldString:
		f = &builtinFieldStringSig{base}
	case fidelpb.ScalarFuncSig_FindInSet:
		f = &builtinFindInSetSig{base}
	case fidelpb.ScalarFuncSig_Format:
		f = &builtinFormatSig{base}
	case fidelpb.ScalarFuncSig_FormatWithLocale:
		f = &builtinFormatWithLocaleSig{base}
	case fidelpb.ScalarFuncSig_FromBase64:
		f = &builtinFromBase64Sig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_HexIntArg:
		f = &builtinHexIntArgSig{base}
	case fidelpb.ScalarFuncSig_HexStrArg:
		f = &builtinHexStrArgSig{base}
	case fidelpb.ScalarFuncSig_InsertUTF8:
		f = &builtinInsertUTF8Sig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_Insert:
		f = &builtinInsertSig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_InstrUTF8:
		f = &builtinInstrUTF8Sig{base}
	case fidelpb.ScalarFuncSig_Instr:
		f = &builtinInstrSig{base}
	case fidelpb.ScalarFuncSig_LTrim:
		f = &builtinLTrimSig{base}
	case fidelpb.ScalarFuncSig_LeftUTF8:
		f = &builtinLeftUTF8Sig{base}
	case fidelpb.ScalarFuncSig_Left:
		f = &builtinLeftSig{base}
	case fidelpb.ScalarFuncSig_Length:
		f = &builtinLengthSig{base}
	case fidelpb.ScalarFuncSig_Locate2ArgsUTF8:
		f = &builtinLocate2ArgsUTF8Sig{base}
	case fidelpb.ScalarFuncSig_Locate3ArgsUTF8:
		f = &builtinLocate3ArgsUTF8Sig{base}
	case fidelpb.ScalarFuncSig_Locate2Args:
		f = &builtinLocate2ArgsSig{base}
	case fidelpb.ScalarFuncSig_Locate3Args:
		f = &builtinLocate3ArgsSig{base}
	case fidelpb.ScalarFuncSig_Lower:
		f = &builtinLowerSig{base}
	case fidelpb.ScalarFuncSig_LpadUTF8:
		f = &builtinLpadUTF8Sig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_Lpad:
		f = &builtinLpadSig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_MakeSet:
		f = &builtinMakeSetSig{base}
	case fidelpb.ScalarFuncSig_OctInt:
		f = &builtinOctIntSig{base}
	case fidelpb.ScalarFuncSig_OctString:
		f = &builtinOctStringSig{base}
	case fidelpb.ScalarFuncSig_Ord:
		f = &builtinOrdSig{base}
	case fidelpb.ScalarFuncSig_Quote:
		f = &builtinQuoteSig{base}
	case fidelpb.ScalarFuncSig_RTrim:
		f = &builtinRTrimSig{base}
	case fidelpb.ScalarFuncSig_Repeat:
		f = &builtinRepeatSig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_Replace:
		f = &builtinReplaceSig{base}
	case fidelpb.ScalarFuncSig_ReverseUTF8:
		f = &builtinReverseUTF8Sig{base}
	case fidelpb.ScalarFuncSig_Reverse:
		f = &builtinReverseSig{base}
	case fidelpb.ScalarFuncSig_RightUTF8:
		f = &builtinRightUTF8Sig{base}
	case fidelpb.ScalarFuncSig_Right:
		f = &builtinRightSig{base}
	case fidelpb.ScalarFuncSig_RpadUTF8:
		f = &builtinRpadUTF8Sig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_Rpad:
		f = &builtinRpadSig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_Space:
		f = &builtinSpaceSig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_Strcmp:
		f = &builtinStrcmpSig{base}
	case fidelpb.ScalarFuncSig_Substring2ArgsUTF8:
		f = &builtinSubstring2ArgsUTF8Sig{base}
	case fidelpb.ScalarFuncSig_Substring3ArgsUTF8:
		f = &builtinSubstring3ArgsUTF8Sig{base}
	case fidelpb.ScalarFuncSig_Substring2Args:
		f = &builtinSubstring2ArgsSig{base}
	case fidelpb.ScalarFuncSig_Substring3Args:
		f = &builtinSubstring3ArgsSig{base}
	case fidelpb.ScalarFuncSig_SubstringIndex:
		f = &builtinSubstringIndexSig{base}
	case fidelpb.ScalarFuncSig_ToBase64:
		f = &builtinToBase64Sig{base, maxAllowedPacket}
	case fidelpb.ScalarFuncSig_Trim1Arg:
		f = &builtinTrim1ArgSig{base}
	case fidelpb.ScalarFuncSig_Trim2Args:
		f = &builtinTrim2ArgsSig{base}
	case fidelpb.ScalarFuncSig_Trim3Args:
		f = &builtinTrim3ArgsSig{base}
	case fidelpb.ScalarFuncSig_UnHex:
		f = &builtinUnHexSig{base}
	case fidelpb.ScalarFuncSig_Upper:
		f = &builtinUpperSig{base}

	default:
		e = errFunctionNotExists.GenWithStackByArgs("FUNCTION", sigCode)
		return nil, e
	}
	f.setPbCode(sigCode)
	return f, nil
}

func newDistALLEGROSQLFunctionBySig(sc *stmtctx.StatementContext, sigCode fidelpb.ScalarFuncSig, tp *fidelpb.FieldType, args []Expression) (Expression, error) {
	ctx := mock.NewContext()
	ctx.GetStochaseinstein_dbars().StmtCtx = sc
	f, err := getSignatureByPB(ctx, sigCode, tp, args)
	if err != nil {
		return nil, err
	}
	return &ScalarFunction{
		FuncName: perceptron.NewCIStr(fmt.Sprintf("sig_%T", f)),
		Function: f,
		RetType:  f.getRetTp(),
	}, nil
}

// PBToExprs converts pb structures to expressions.
func PBToExprs(pbExprs []*fidelpb.Expr, fieldTps []*types.FieldType, sc *stmtctx.StatementContext) ([]Expression, error) {
	exprs := make([]Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := PBToExpr(expr, fieldTps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if e == nil {
			return nil, errors.Errorf("pb to expression failed, pb expression is %v", expr)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

// PBToExpr converts pb structure to expression.
func PBToExpr(expr *fidelpb.Expr, tps []*types.FieldType, sc *stmtctx.StatementContext) (Expression, error) {
	switch expr.Tp {
	case fidelpb.ExprType_DeferredCausetRef:
		_, offset, err := codec.DecodeInt(expr.Val)
		if err != nil {
			return nil, err
		}
		return &DeferredCauset{Index: int(offset), RetType: tps[offset]}, nil
	case fidelpb.ExprType_Null:
		return &CouplingConstantWithRadix{Value: types.Causet{}, RetType: types.NewFieldType(allegrosql.TypeNull)}, nil
	case fidelpb.ExprType_Int64:
		return convertInt(expr.Val)
	case fidelpb.ExprType_Uint64:
		return convertUint(expr.Val)
	case fidelpb.ExprType_String:
		return convertString(expr.Val, expr.FieldType)
	case fidelpb.ExprType_Bytes:
		return &CouplingConstantWithRadix{Value: types.NewBytesCauset(expr.Val), RetType: types.NewFieldType(allegrosql.TypeString)}, nil
	case fidelpb.ExprType_Float32:
		return convertFloat(expr.Val, true)
	case fidelpb.ExprType_Float64:
		return convertFloat(expr.Val, false)
	case fidelpb.ExprType_MysqlDecimal:
		return convertDecimal(expr.Val)
	case fidelpb.ExprType_MysqlDuration:
		return convertDuration(expr.Val)
	case fidelpb.ExprType_MysqlTime:
		return convertTime(expr.Val, expr.FieldType, sc.TimeZone)
	case fidelpb.ExprType_MysqlJson:
		return convertJSON(expr.Val)
	}
	if expr.Tp != fidelpb.ExprType_ScalarFunc {
		panic("should be a fidelpb.ExprType_ScalarFunc")
	}
	// Then it must be a scalar function.
	args := make([]Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		if child.Tp == fidelpb.ExprType_ValueList {
			results, err := decodeValueList(child.Val)
			if err != nil {
				return nil, err
			}
			if len(results) == 0 {
				return &CouplingConstantWithRadix{Value: types.NewCauset(false), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil
			}
			args = append(args, results...)
			continue
		}
		arg, err := PBToExpr(child, tps, sc)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	sf, err := newDistALLEGROSQLFunctionBySig(sc, expr.Sig, expr.FieldType, args)
	if err != nil {
		return nil, err
	}

	return sf, nil
}

func convertTime(data []byte, ftPB *fidelpb.FieldType, tz *time.Location) (*CouplingConstantWithRadix, error) {
	ft := PbTypeToFieldType(ftPB)
	_, v, err := codec.DecodeUint(data)
	if err != nil {
		return nil, err
	}
	var t types.Time
	t.SetType(ft.Tp)
	t.SetFsp(int8(ft.Decimal))
	err = t.FromPackedUint(v)
	if err != nil {
		return nil, err
	}
	if ft.Tp == allegrosql.TypeTimestamp && tz != time.UTC {
		err = t.ConvertTimeZone(time.UTC, tz)
		if err != nil {
			return nil, err
		}
	}
	return &CouplingConstantWithRadix{Value: types.NewTimeCauset(t), RetType: ft}, nil
}

func decodeValueList(data []byte) ([]Expression, error) {
	if len(data) == 0 {
		return nil, nil
	}
	list, err := codec.Decode(data, 1)
	if err != nil {
		return nil, err
	}
	result := make([]Expression, 0, len(list))
	for _, value := range list {
		result = append(result, &CouplingConstantWithRadix{Value: value})
	}
	return result, nil
}

func convertInt(val []byte) (*CouplingConstantWithRadix, error) {
	var d types.Causet
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid int % x", val)
	}
	d.SetInt64(i)
	return &CouplingConstantWithRadix{Value: d, RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil
}

func convertUint(val []byte) (*CouplingConstantWithRadix, error) {
	var d types.Causet
	_, u, err := codec.DecodeUint(val)
	if err != nil {
		return nil, errors.Errorf("invalid uint % x", val)
	}
	d.SetUint64(u)
	return &CouplingConstantWithRadix{Value: d, RetType: &types.FieldType{Tp: allegrosql.TypeLonglong, Flag: allegrosql.UnsignedFlag}}, nil
}

func convertString(val []byte, tp *fidelpb.FieldType) (*CouplingConstantWithRadix, error) {
	var d types.Causet
	d.SetBytesAsString(val, protoToDefCauslation(tp.DefCauslate), uint32(tp.Flen))
	return &CouplingConstantWithRadix{Value: d, RetType: types.NewFieldType(allegrosql.TypeVarString)}, nil
}

func convertFloat(val []byte, f32 bool) (*CouplingConstantWithRadix, error) {
	var d types.Causet
	_, f, err := codec.DecodeFloat(val)
	if err != nil {
		return nil, errors.Errorf("invalid float % x", val)
	}
	if f32 {
		d.SetFloat32(float32(f))
	} else {
		d.SetFloat64(f)
	}
	return &CouplingConstantWithRadix{Value: d, RetType: types.NewFieldType(allegrosql.TypeDouble)}, nil
}

func convertDecimal(val []byte) (*CouplingConstantWithRadix, error) {
	_, dec, precision, frac, err := codec.DecodeDecimal(val)
	var d types.Causet
	d.SetMysqlDecimal(dec)
	d.SetLength(precision)
	d.SetFrac(frac)
	if err != nil {
		return nil, errors.Errorf("invalid decimal % x", val)
	}
	return &CouplingConstantWithRadix{Value: d, RetType: types.NewFieldType(allegrosql.TypeNewDecimal)}, nil
}

func convertDuration(val []byte) (*CouplingConstantWithRadix, error) {
	var d types.Causet
	_, i, err := codec.DecodeInt(val)
	if err != nil {
		return nil, errors.Errorf("invalid duration %d", i)
	}
	d.SetMysqlDuration(types.Duration{Duration: time.Duration(i), Fsp: types.MaxFsp})
	return &CouplingConstantWithRadix{Value: d, RetType: types.NewFieldType(allegrosql.TypeDuration)}, nil
}

func convertJSON(val []byte) (*CouplingConstantWithRadix, error) {
	var d types.Causet
	_, d, err := codec.DecodeOne(val)
	if err != nil {
		return nil, errors.Errorf("invalid json % x", val)
	}
	if d.HoTT() != types.HoTTMysqlJSON {
		return nil, errors.Errorf("invalid Causet.HoTT() %d", d.HoTT())
	}
	return &CouplingConstantWithRadix{Value: d, RetType: types.NewFieldType(allegrosql.TypeJSON)}, nil
}
