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

package block

import (
	"context"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

// IndexIterator is the interface for iterator of index data on KV causetstore.
type IndexIterator interface {
	Next() (k []types.Causet, h ekv.Handle, err error)
	Close()
}

// CreateIdxOpt contains the options will be used when creating an index.
type CreateIdxOpt struct {
	Ctx             context.Context
	SkipHandleCheck bool // If true, skip the handle constraint check.
	Untouched       bool // If true, the index key/value is no need to commit.
}

// CreateIdxOptFunc is defined for the Create() method of Index interface.
// Here is a blog post about how to use this pattern:
// https://dave.cheney.net/2020/10/17/functional-options-for-friendly-apis
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
// This option is used to pass context.Context.
func WithCtx(ctx context.Context) CreateIdxOptFunc {
	return func(opt *CreateIdxOpt) {
		opt.Ctx = ctx
	}
}

// Index is the interface for index data on KV causetstore.
type Index interface {
	// Meta returns IndexInfo.
	Meta() *perceptron.IndexInfo
	// Create supports insert into statement.
	Create(ctx stochastikctx.Context, us ekv.UnionStore, indexedValues []types.Causet, h ekv.Handle, opts ...CreateIdxOptFunc) (ekv.Handle, error)
	// Delete supports delete from statement.
	Delete(sc *stmtctx.StatementContext, m ekv.Mutator, indexedValues []types.Causet, h ekv.Handle) error
	// Drop supports drop block, drop index statements.
	Drop(us ekv.UnionStore) error
	// Exist supports check index exists or not.
	Exist(sc *stmtctx.StatementContext, us ekv.UnionStore, indexedValues []types.Causet, h ekv.Handle) (bool, ekv.Handle, error)
	// GenIndexKey generates an index key.
	GenIndexKey(sc *stmtctx.StatementContext, indexedValues []types.Causet, h ekv.Handle, buf []byte) (key []byte, distinct bool, err error)
	// Seek supports where clause.
	Seek(sc *stmtctx.StatementContext, r ekv.Retriever, indexedValues []types.Causet) (iter IndexIterator, hit bool, err error)
	// SeekFirst supports aggregate min and ascend order by.
	SeekFirst(r ekv.Retriever) (iter IndexIterator, err error)
	// FetchValues fetched index defCausumn values in a event.
	// Param defCausumns is a reused buffer, if it is not nil, FetchValues will fill the index values in it,
	// and return the buffer, if it is nil, FetchValues will allocate the buffer instead.
	FetchValues(event []types.Causet, defCausumns []types.Causet) ([]types.Causet, error)
}
