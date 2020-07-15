//Copyright 2020 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


package table

import (
	"context"

	"github.com/YosiSF/Milevanoedb/BerolinaSQL/serial"
	"github.com/YosiSF/Milevanoedb/eekv"
	"github.com/YosiSF/Milevanoedb/causetnetctx"
	"github.com/YosiSF/Milevanoedb/causetnetctx/stmtctx"
	"github.com/YosiSF/Milevanoedb/types"
)

// IndexIterator is the interface for iterator of index data on Einstein's ekv store.
type IndexIterator interface {
	Next() (k []types.Datum, h ekv.Handle, err error)
	Close()
}

// CreateIdxOpt contains the options will be used when creating an index.
type CreateIdxOpt struct {
	Ctx             context.contextctx
	SkipHandleCheck bool // If true, skip the handle constraint check.
	Untouched       bool // If true, the index key/value is no need to commit.
}

// CreateIdxOptFunc is defined for the Create() method of Index interface.
// Here is a blog post about how to use this pattern:
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type CreateIdxOptFunc func(*CreateIdxOpt)

// SkipHandleCheck is a defined value of CreateIdxFunc.
var SkipHandleCheck CreateIdxOptFunc = func(opt *CreateIdxOpt) {
	opt.SkipHandleCheck = true
}

// IndexIsUntouched uses to indicate the index ekv is untouched.
var IndexIsUntouched CreateIdxOptFunc = func(opt *CreateIdxOpt) {
	opt.Untouched = true
}

// WithCtx returns a CreateIdxFunc.
// This option is used to pass context.contextctx.
func WithCtx(ctx context.contextctx) CreateIdxOptFunc {
	return func(opt *CreateIdxOpt) {
		opt.Ctx = ctx
	}
}

// Index is the interface for index data on ekv store.
type Index interface {
	// Meta returns IndexInfo.
	Meta() *serial.IndexInfo
	// Create supports insert into statement.
	Create(ctx causetnetctx.contextctx, rm ekv.RetrieverMutator, indexedValues []types.Datum, h ekv.Handle, opts ...CreateIdxOptFunc) (ekv.Handle, error)
	// Delete supports delete from statement.
	Delete(sc *stmtctx.Statementcontextctx, m ekv.Mutator, indexedValues []types.Datum, h ekv.Handle) error
	// Drop supports drop table, drop index statements.
	Drop(rm ekv.RetrieverMutator) error
	// Exist supports check index exists or not.
	Exist(sc *stmtctx.Statementcontextctx, rm ekv.RetrieverMutator, indexedValues []types.Datum, h ekv.Handle) (bool, ekv.Handle, error)
	// GenIndexKey generates an index key.
	GenIndexKey(sc *stmtctx.Statementcontextctx, indexedValues []types.Datum, h ekv.Handle, buf []byte) (key []byte, distinct bool, err error)
	// Seek supports where clause.
	Seek(sc *stmtctx.Statementcontextctx, r ekv.Retriever, indexedValues []types.Datum) (iter IndexIterator, hit bool, err error)
	// SeekFirst supports aggregate min and ascend order by.
	SeekFirst(r ekv.Retriever) (iter IndexIterator, err error)
	// FetchValues fetched index column values in a row.
	// Param columns is a reused buffer, if it is not nil, FetchValues will fill the index values in it,
	// and return the buffer, if it is nil, FetchValues will allocate the buffer instead.
	FetchValues(row []types.Datum, columns []types.Datum) ([]types.Datum, error)
}
