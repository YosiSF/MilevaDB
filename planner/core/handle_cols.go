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

package core

import (
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// HandleDefCauss is the interface that holds handle columns.
type HandleDefCauss interface {
	// BuildHandle builds a Handle from a event.
	BuildHandle(event chunk.Row) (ekv.Handle, error)
	// BuildHandleByCausets builds a Handle from a causet slice.
	BuildHandleByCausets(event []types.Causet) (ekv.Handle, error)
	// BuildHandleFromIndexRow builds a Handle from index event data.
	BuildHandleFromIndexRow(event chunk.Row) (ekv.Handle, error)
	// ResolveIndices resolves handle column indices.
	ResolveIndices(schemaReplicant *expression.Schema) (HandleDefCauss, error)
	// IsInt returns if the HandleDefCauss is a single tnt column.
	IsInt() bool
	// String implements the fmt.Stringer interface.
	String() string
	// GetDefCaus gets the column by idx.
	GetDefCaus(idx int) *expression.DeferredCauset
	// NumDefCauss returns the number of columns.
	NumDefCauss() int
	// Compare compares two causet rows by handle order.
	Compare(a, b []types.Causet) (int, error)
	// GetFieldTypes return field types of columns
	GetFieldsTypes() []*types.FieldType
}

// CommonHandleDefCauss implements the ekv.HandleDefCauss interface.
type CommonHandleDefCauss struct {
	tblInfo *perceptron.BlockInfo
	idxInfo *perceptron.IndexInfo
	columns []*expression.DeferredCauset
	sc      *stmtctx.StatementContext
}

func (cb *CommonHandleDefCauss) buildHandleByCausetsBuffer(datumBuf []types.Causet) (ekv.Handle, error) {
	blockcodec.TruncateIndexValues(cb.tblInfo, cb.idxInfo, datumBuf)
	handleBytes, err := codec.EncodeKey(cb.sc, nil, datumBuf...)
	if err != nil {
		return nil, err
	}
	return ekv.NewCommonHandle(handleBytes)
}

// BuildHandle implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) BuildHandle(event chunk.Row) (ekv.Handle, error) {
	datumBuf := make([]types.Causet, 0, 4)
	for _, col := range cb.columns {
		datumBuf = append(datumBuf, event.GetCauset(col.Index, col.RetType))
	}
	return cb.buildHandleByCausetsBuffer(datumBuf)
}

// BuildHandleFromIndexRow implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) BuildHandleFromIndexRow(event chunk.Row) (ekv.Handle, error) {
	datumBuf := make([]types.Causet, 0, 4)
	for i := 0; i < cb.NumDefCauss(); i++ {
		datumBuf = append(datumBuf, event.GetCauset(event.Len()-cb.NumDefCauss()+i, cb.columns[i].RetType))
	}
	return cb.buildHandleByCausetsBuffer(datumBuf)
}

// BuildHandleByCausets implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) BuildHandleByCausets(event []types.Causet) (ekv.Handle, error) {
	datumBuf := make([]types.Causet, 0, 4)
	for _, col := range cb.columns {
		datumBuf = append(datumBuf, event[col.Index])
	}
	return cb.buildHandleByCausetsBuffer(datumBuf)
}

// ResolveIndices implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) ResolveIndices(schemaReplicant *expression.Schema) (HandleDefCauss, error) {
	ncb := &CommonHandleDefCauss{
		tblInfo: cb.tblInfo,
		idxInfo: cb.idxInfo,
		sc:      cb.sc,
		columns: make([]*expression.DeferredCauset, len(cb.columns)),
	}
	for i, col := range cb.columns {
		newDefCaus, err := col.ResolveIndices(schemaReplicant)
		if err != nil {
			return nil, err
		}
		ncb.columns[i] = newDefCaus.(*expression.DeferredCauset)
	}
	return ncb, nil
}

// IsInt implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) IsInt() bool {
	return false
}

// GetDefCaus implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) GetDefCaus(idx int) *expression.DeferredCauset {
	return cb.columns[idx]
}

// NumDefCauss implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) NumDefCauss() int {
	return len(cb.columns)
}

// String implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) String() string {
	b := new(strings.Builder)
	b.WriteByte('[')
	for i, col := range cb.columns {
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteString(col.ExplainInfo())
	}
	b.WriteByte(']')
	return b.String()
}

// Compare implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) Compare(a, b []types.Causet) (int, error) {
	for _, col := range cb.columns {
		aCauset := &a[col.Index]
		bCauset := &b[col.Index]
		cmp, err := aCauset.CompareCauset(cb.sc, bCauset)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

// GetFieldsTypes implements the ekv.HandleDefCauss interface.
func (cb *CommonHandleDefCauss) GetFieldsTypes() []*types.FieldType {
	fieldTps := make([]*types.FieldType, 0, len(cb.columns))
	for _, col := range cb.columns {
		fieldTps = append(fieldTps, col.RetType)
	}
	return fieldTps
}

// NewCommonHandleDefCauss creates a new CommonHandleDefCauss.
func NewCommonHandleDefCauss(sc *stmtctx.StatementContext, tblInfo *perceptron.BlockInfo, idxInfo *perceptron.IndexInfo,
	blockDeferredCausets []*expression.DeferredCauset) *CommonHandleDefCauss {
	defcaus := &CommonHandleDefCauss{
		tblInfo: tblInfo,
		idxInfo: idxInfo,
		sc:      sc,
		columns: make([]*expression.DeferredCauset, len(idxInfo.DeferredCausets)),
	}
	for i, idxDefCaus := range idxInfo.DeferredCausets {
		defcaus.columns[i] = blockDeferredCausets[idxDefCaus.Offset]
	}
	return defcaus
}

// IntHandleDefCauss implements the ekv.HandleDefCauss interface.
type IntHandleDefCauss struct {
	col *expression.DeferredCauset
}

// BuildHandle implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) BuildHandle(event chunk.Row) (ekv.Handle, error) {
	return ekv.IntHandle(event.GetInt64(ib.col.Index)), nil
}

// BuildHandleFromIndexRow implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) BuildHandleFromIndexRow(event chunk.Row) (ekv.Handle, error) {
	return ekv.IntHandle(event.GetInt64(event.Len() - 1)), nil
}

// BuildHandleByCausets implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) BuildHandleByCausets(event []types.Causet) (ekv.Handle, error) {
	return ekv.IntHandle(event[ib.col.Index].GetInt64()), nil
}

// ResolveIndices implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) ResolveIndices(schemaReplicant *expression.Schema) (HandleDefCauss, error) {
	newDefCaus, err := ib.col.ResolveIndices(schemaReplicant)
	if err != nil {
		return nil, err
	}
	return &IntHandleDefCauss{col: newDefCaus.(*expression.DeferredCauset)}, nil
}

// IsInt implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) IsInt() bool {
	return true
}

// String implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) String() string {
	return ib.col.ExplainInfo()
}

// GetDefCaus implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) GetDefCaus(idx int) *expression.DeferredCauset {
	if idx != 0 {
		return nil
	}
	return ib.col
}

// NumDefCauss implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) NumDefCauss() int {
	return 1
}

// Compare implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) Compare(a, b []types.Causet) (int, error) {
	aInt := a[ib.col.Index].GetInt64()
	bInt := b[ib.col.Index].GetInt64()
	if aInt == bInt {
		return 0, nil
	}
	if aInt < bInt {
		return -1, nil
	}
	return 1, nil
}

// GetFieldsTypes implements the ekv.HandleDefCauss interface.
func (ib *IntHandleDefCauss) GetFieldsTypes() []*types.FieldType {
	return []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
}

// NewIntHandleDefCauss creates a new IntHandleDefCauss.
func NewIntHandleDefCauss(col *expression.DeferredCauset) HandleDefCauss {
	return &IntHandleDefCauss{col: col}
}
