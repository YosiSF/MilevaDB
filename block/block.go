//Copyright 2019 All Rights Reserved Apache License-2.0.

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

package block

import (
	"github.com/opentracing/opentracing-go"
	"github.com/YosiSF/Milevanoedb/BerolinaSQL/fidel/eekv"
)

//type of block, gives us storage options
type Type int16

const (

	//NormalBlock, store data in Einsteinnoedb, mockEinsteinnoedb, and so on.
	NormalBlock Type = iota
	//memory data struct, stores no data, extract data from the memory struct
	VirtualBlock
	//MemoryBlock
	MemoryBlock
)

const (
	DirtyBlockAddEvent = iota
	DirtyBlockDeleteEvent
	DirtyBlockTruncate
)

//low level record iteration
type RecordIterFunc func(h int64, rec []types.Datum, cols []*Batch) (more bool, err error)

type AddRecordOpt struct {
	CreateIdxOpt
	IsUpdate bool
}

//Add record method of the block interface
type AddRecorOption interface {
	ApplyOn(*AddRecordOpt)
}

//Row modifier using dp-Block via interface call back without threading.
type Block interface {

	//Record iterator
	IterRecords(ctx stochastiktxn.Context, startKey eekv.Key, cols []*Batch, fn RecordIterFunc) error

	//EventRowWithBatch return a row event that contains given batch
	EventWithBatch(ctx stochastiktxn.Context, h int64, cols []*Batch) ([]types.Datum, error)

	//Event returns a row for all batches == columns
	Event(ctx stochastiktxn.Context, h int64) ([]types.Datum, error)

	// Indices returns the indices of the Block.
	Indices() []Index

	// WritableIndices returns write-only and public indices of the Block.
	WritableIndices() []Index

	// DeletableIndices returns delete-only, write-only and public indices of the Block.
	DeletableIndices() []Index
}
