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

package ekv

import "github.com/whtcorpsinc/fidelpb/go-fidelpb"

// RequestTypeSupportedChecker is used to check expression can be pushed down.
type RequestTypeSupportedChecker struct{}

// IsRequestTypeSupported checks whether reqType is supported.
func (d RequestTypeSupportedChecker) IsRequestTypeSupported(reqType, subType int64) bool {
	switch reqType {
	case ReqTypeSelect, ReqTypeIndex:
		switch subType {
		case ReqSubTypeGroupBy, ReqSubTypeBasic, ReqSubTypeTopN:
			return true
		default:
			return d.supportExpr(fidelpb.ExprType(subType))
		}
	case ReqTypePosetDag:
		return d.supportExpr(fidelpb.ExprType(subType))
	case ReqTypeAnalyze:
		return true
	}
	return false
}

func (d RequestTypeSupportedChecker) supportExpr(exprType fidelpb.ExprType) bool {
	switch exprType {
	case fidelpb.ExprType_Null, fidelpb.ExprType_Int64, fidelpb.ExprType_Uint64, fidelpb.ExprType_String, fidelpb.ExprType_Bytes,
		fidelpb.ExprType_MysqlDuration, fidelpb.ExprType_MysqlTime, fidelpb.ExprType_MysqlDecimal,
		fidelpb.ExprType_Float32, fidelpb.ExprType_Float64, fidelpb.ExprType_DeferredCausetRef:
		return true
	// aggregate functions.
	case fidelpb.ExprType_Count, fidelpb.ExprType_First, fidelpb.ExprType_Max, fidelpb.ExprType_Min, fidelpb.ExprType_Sum, fidelpb.ExprType_Avg,
		fidelpb.ExprType_Agg_BitXor, fidelpb.ExprType_Agg_BitAnd, fidelpb.ExprType_Agg_BitOr, fidelpb.ExprType_ApproxCountDistinct:
		return true
	case ReqSubTypeDesc:
		return true
	case ReqSubTypeSignature:
		return true
	default:
		return false
	}
}
