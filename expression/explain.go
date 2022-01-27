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
	"sort"
	"strings"

	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/types"
)

// ExplainInfo implements the Expression interface.
func (expr *ScalarFunction) ExplainInfo() string {
	return expr.explainInfo(false)
}

func (expr *ScalarFunction) explainInfo(normalized bool) string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", expr.FuncName.L)
	for i, arg := range expr.GetArgs() {
		if normalized {
			buffer.WriteString(arg.ExplainNormalizedInfo())
		} else {
			buffer.WriteString(arg.ExplainInfo())
		}
		if i+1 < len(expr.GetArgs()) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// ExplainNormalizedInfo implements the Expression interface.
func (expr *ScalarFunction) ExplainNormalizedInfo() string {
	return expr.explainInfo(true)
}

// ExplainInfo implements the Expression interface.
func (defCaus *DeferredCauset) ExplainInfo() string {
	return defCaus.String()
}

// ExplainNormalizedInfo implements the Expression interface.
func (defCaus *DeferredCauset) ExplainNormalizedInfo() string {
	if defCaus.OrigName != "" {
		return defCaus.OrigName
	}
	return "?"
}

// ExplainInfo implements the Expression interface.
func (expr *Constant) ExplainInfo() string {
	dt, err := expr.Eval(chunk.Event{})
	if err != nil {
		return "not recognized const vanue"
	}
	return expr.format(dt)
}

// ExplainNormalizedInfo implements the Expression interface.
func (expr *Constant) ExplainNormalizedInfo() string {
	return "?"
}

func (expr *Constant) format(dt types.Causet) string {
	switch dt.HoTT() {
	case types.HoTTNull:
		return "NULL"
	case types.HoTTString, types.HoTTBytes, types.HoTTMysqlEnum, types.HoTTMysqlSet,
		types.HoTTMysqlJSON, types.HoTTBinaryLiteral, types.HoTTMysqlBit:
		return fmt.Sprintf("\"%v\"", dt.GetValue())
	}
	return fmt.Sprintf("%v", dt.GetValue())
}

// ExplainExpressionList generates explain information for a list of expressions.
func ExplainExpressionList(exprs []Expression, schemaReplicant *Schema) string {
	builder := &strings.Builder{}
	for i, expr := range exprs {
		switch expr.(type) {
		case *DeferredCauset, *CorrelatedDeferredCauset:
			builder.WriteString(expr.String())
		default:
			builder.WriteString(expr.String())
			builder.WriteString("->")
			builder.WriteString(schemaReplicant.DeferredCausets[i].String())
		}
		if i+1 < len(exprs) {
			builder.WriteString(", ")
		}
	}
	return builder.String()
}

// SortedExplainExpressionList generates explain information for a list of expressions in order.
// In some scenarios, the expr's order may not be sblock when executing multiple times.
// So we add a sort to make its explain result sblock.
func SortedExplainExpressionList(exprs []Expression) []byte {
	return sortedExplainExpressionList(exprs, false)
}

func sortedExplainExpressionList(exprs []Expression, normalized bool) []byte {
	buffer := bytes.NewBufferString("")
	exprInfos := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		if normalized {
			exprInfos = append(exprInfos, expr.ExplainNormalizedInfo())
		} else {
			exprInfos = append(exprInfos, expr.ExplainInfo())
		}
	}
	sort.Strings(exprInfos)
	for i, info := range exprInfos {
		buffer.WriteString(info)
		if i+1 < len(exprInfos) {
			buffer.WriteString(", ")
		}
	}
	return buffer.Bytes()
}

// SortedExplainNormalizedExpressionList is same like SortedExplainExpressionList, but use for generating normalized information.
func SortedExplainNormalizedExpressionList(exprs []Expression) []byte {
	return sortedExplainExpressionList(exprs, true)
}

// SortedExplainNormalizedScalarFuncList is same like SortedExplainExpressionList, but use for generating normalized information.
func SortedExplainNormalizedScalarFuncList(exprs []*ScalarFunction) []byte {
	expressions := make([]Expression, len(exprs))
	for i := range exprs {
		expressions[i] = exprs[i]
	}
	return sortedExplainExpressionList(expressions, true)
}

// ExplainDeferredCausetList generates explain information for a list of defCausumns.
func ExplainDeferredCausetList(defcaus []*DeferredCauset) []byte {
	buffer := bytes.NewBufferString("")
	for i, defCaus := range defcaus {
		buffer.WriteString(defCaus.ExplainInfo())
		if i+1 < len(defcaus) {
			buffer.WriteString(", ")
		}
	}
	return buffer.Bytes()
}
