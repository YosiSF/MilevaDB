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

package core

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
)

func newLongType() types.FieldType {
	return *(types.NewFieldType(allegrosql.TypeLong))
}

func newStringType() types.FieldType {
	ft := types.NewFieldType(allegrosql.TypeVarchar)
	ft.Charset, ft.DefCauslate = types.DefaultCharsetForType(allegrosql.TypeVarchar)
	return *ft
}

func newDateType() types.FieldType {
	ft := types.NewFieldType(allegrosql.TypeDate)
	return *ft
}

// MockSignedBlock is only used for plan related tests.
func MockSignedBlock() *perceptron.BlockInfo {
	// column: a, b, c, d, e, c_str, d_str, e_str, f, g
	// PK: a
	// indices: c_d_e, e, f, g, f_g, c_d_e_str, c_d_e_str_prefix
	indices := []*perceptron.IndexInfo{
		{
			Name: perceptron.NewCIStr("c_d_e"),
			DeferredCausets: []*perceptron.IndexDeferredCauset{
				{
					Name:   perceptron.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
				{
					Name:   perceptron.NewCIStr("d"),
					Length: types.UnspecifiedLength,
					Offset: 3,
				},
				{
					Name:   perceptron.NewCIStr("e"),
					Length: types.UnspecifiedLength,
					Offset: 4,
				},
			},
			State:  perceptron.StatePublic,
			Unique: true,
		},
		{
			Name: perceptron.NewCIStr("e"),
			DeferredCausets: []*perceptron.IndexDeferredCauset{
				{
					Name:   perceptron.NewCIStr("e"),
					Length: types.UnspecifiedLength,
					Offset: 4,
				},
			},
			State:  perceptron.StateWriteOnly,
			Unique: true,
		},
		{
			Name: perceptron.NewCIStr("f"),
			DeferredCausets: []*perceptron.IndexDeferredCauset{
				{
					Name:   perceptron.NewCIStr("f"),
					Length: types.UnspecifiedLength,
					Offset: 8,
				},
			},
			State:  perceptron.StatePublic,
			Unique: true,
		},
		{
			Name: perceptron.NewCIStr("g"),
			DeferredCausets: []*perceptron.IndexDeferredCauset{
				{
					Name:   perceptron.NewCIStr("g"),
					Length: types.UnspecifiedLength,
					Offset: 9,
				},
			},
			State: perceptron.StatePublic,
		},
		{
			Name: perceptron.NewCIStr("f_g"),
			DeferredCausets: []*perceptron.IndexDeferredCauset{
				{
					Name:   perceptron.NewCIStr("f"),
					Length: types.UnspecifiedLength,
					Offset: 8,
				},
				{
					Name:   perceptron.NewCIStr("g"),
					Length: types.UnspecifiedLength,
					Offset: 9,
				},
			},
			State:  perceptron.StatePublic,
			Unique: true,
		},
		{
			Name: perceptron.NewCIStr("c_d_e_str"),
			DeferredCausets: []*perceptron.IndexDeferredCauset{
				{
					Name:   perceptron.NewCIStr("c_str"),
					Length: types.UnspecifiedLength,
					Offset: 5,
				},
				{
					Name:   perceptron.NewCIStr("d_str"),
					Length: types.UnspecifiedLength,
					Offset: 6,
				},
				{
					Name:   perceptron.NewCIStr("e_str"),
					Length: types.UnspecifiedLength,
					Offset: 7,
				},
			},
			State: perceptron.StatePublic,
		},
		{
			Name: perceptron.NewCIStr("e_d_c_str_prefix"),
			DeferredCausets: []*perceptron.IndexDeferredCauset{
				{
					Name:   perceptron.NewCIStr("e_str"),
					Length: types.UnspecifiedLength,
					Offset: 7,
				},
				{
					Name:   perceptron.NewCIStr("d_str"),
					Length: types.UnspecifiedLength,
					Offset: 6,
				},
				{
					Name:   perceptron.NewCIStr("c_str"),
					Length: 10,
					Offset: 5,
				},
			},
			State: perceptron.StatePublic,
		},
	}
	pkDeferredCauset := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    0,
		Name:      perceptron.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        1,
	}
	col0 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    1,
		Name:      perceptron.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    2,
		Name:      perceptron.NewCIStr("c"),
		FieldType: newLongType(),
		ID:        3,
	}
	col2 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    3,
		Name:      perceptron.NewCIStr("d"),
		FieldType: newLongType(),
		ID:        4,
	}
	col3 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    4,
		Name:      perceptron.NewCIStr("e"),
		FieldType: newLongType(),
		ID:        5,
	}
	colStr1 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    5,
		Name:      perceptron.NewCIStr("c_str"),
		FieldType: newStringType(),
		ID:        6,
	}
	colStr2 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    6,
		Name:      perceptron.NewCIStr("d_str"),
		FieldType: newStringType(),
		ID:        7,
	}
	colStr3 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    7,
		Name:      perceptron.NewCIStr("e_str"),
		FieldType: newStringType(),
		ID:        8,
	}
	col4 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    8,
		Name:      perceptron.NewCIStr("f"),
		FieldType: newLongType(),
		ID:        9,
	}
	col5 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    9,
		Name:      perceptron.NewCIStr("g"),
		FieldType: newLongType(),
		ID:        10,
	}
	col6 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    10,
		Name:      perceptron.NewCIStr("h"),
		FieldType: newLongType(),
		ID:        11,
	}
	col7 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    11,
		Name:      perceptron.NewCIStr("i_date"),
		FieldType: newDateType(),
		ID:        12,
	}
	pkDeferredCauset.Flag = allegrosql.PriKeyFlag | allegrosql.NotNullFlag
	// DeferredCauset 'b', 'c', 'd', 'f', 'g' is not null.
	col0.Flag = allegrosql.NotNullFlag
	col1.Flag = allegrosql.NotNullFlag
	col2.Flag = allegrosql.NotNullFlag
	col4.Flag = allegrosql.NotNullFlag
	col5.Flag = allegrosql.NotNullFlag
	col6.Flag = allegrosql.NoDefaultValueFlag
	block := &perceptron.BlockInfo{
		DeferredCausets: []*perceptron.DeferredCausetInfo{pkDeferredCauset, col0, col1, col2, col3, colStr1, colStr2, colStr3, col4, col5, col6, col7},
		Indices:         indices,
		Name:            perceptron.NewCIStr("t"),
		PKIsHandle:      true,
	}
	return block
}

// MockUnsignedBlock is only used for plan related tests.
func MockUnsignedBlock() *perceptron.BlockInfo {
	// column: a, b
	// PK: a
	// indeices: b
	indices := []*perceptron.IndexInfo{
		{
			Name: perceptron.NewCIStr("b"),
			DeferredCausets: []*perceptron.IndexDeferredCauset{
				{
					Name:   perceptron.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
			},
			State:  perceptron.StatePublic,
			Unique: true,
		},
		{
			Name: perceptron.NewCIStr("b_c"),
			DeferredCausets: []*perceptron.IndexDeferredCauset{
				{
					Name:   perceptron.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
				{
					Name:   perceptron.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
			},
			State: perceptron.StatePublic,
		},
	}
	pkDeferredCauset := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    0,
		Name:      perceptron.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        1,
	}
	col0 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    1,
		Name:      perceptron.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    2,
		Name:      perceptron.NewCIStr("c"),
		FieldType: newLongType(),
		ID:        3,
	}
	pkDeferredCauset.Flag = allegrosql.PriKeyFlag | allegrosql.NotNullFlag | allegrosql.UnsignedFlag
	// DeferredCauset 'b', 'c', 'd', 'f', 'g' is not null.
	col0.Flag = allegrosql.NotNullFlag
	col1.Flag = allegrosql.UnsignedFlag
	block := &perceptron.BlockInfo{
		DeferredCausets: []*perceptron.DeferredCausetInfo{pkDeferredCauset, col0, col1},
		Indices:         indices,
		Name:            perceptron.NewCIStr("t2"),
		PKIsHandle:      true,
	}
	return block
}

// MockView is only used for plan related tests.
func MockView() *perceptron.BlockInfo {
	selectStmt := "select b,c,d from t"
	col0 := &perceptron.DeferredCausetInfo{
		State:  perceptron.StatePublic,
		Offset: 0,
		Name:   perceptron.NewCIStr("b"),
		ID:     1,
	}
	col1 := &perceptron.DeferredCausetInfo{
		State:  perceptron.StatePublic,
		Offset: 1,
		Name:   perceptron.NewCIStr("c"),
		ID:     2,
	}
	col2 := &perceptron.DeferredCausetInfo{
		State:  perceptron.StatePublic,
		Offset: 2,
		Name:   perceptron.NewCIStr("d"),
		ID:     3,
	}
	view := &perceptron.ViewInfo{SelectStmt: selectStmt, Security: perceptron.SecurityDefiner, Definer: &auth.UserIdentity{Username: "root", Hostname: ""}, DefCauss: []perceptron.CIStr{col0.Name, col1.Name, col2.Name}}
	block := &perceptron.BlockInfo{
		Name:            perceptron.NewCIStr("v"),
		DeferredCausets: []*perceptron.DeferredCausetInfo{col0, col1, col2},
		View:            view,
	}
	return block
}

// MockContext is only used for plan related tests.
func MockContext() stochastikctx.Context {
	ctx := mock.NewContext()
	ctx.CausetStore = &mock.CausetStore{
		Client: &mock.Client{},
	}
	ctx.GetStochaseinstein_dbars().CurrentDB = "test"
	do := &petri.Petri{}
	do.CreateStatsHandle(ctx)
	petri.BindPetri(ctx, do)
	return ctx
}

// MockPartitionSchemaReplicant mocks an info schemaReplicant for partition block.
func MockPartitionSchemaReplicant(definitions []perceptron.PartitionDefinition) schemareplicant.SchemaReplicant {
	blockInfo := MockSignedBlock()
	defcaus := make([]*perceptron.DeferredCausetInfo, 0, len(blockInfo.DeferredCausets))
	defcaus = append(defcaus, blockInfo.DeferredCausets...)
	last := blockInfo.DeferredCausets[len(blockInfo.DeferredCausets)-1]
	defcaus = append(defcaus, &perceptron.DeferredCausetInfo{
		State:     perceptron.StatePublic,
		Offset:    last.Offset + 1,
		Name:      perceptron.NewCIStr("ptn"),
		FieldType: newLongType(),
		ID:        last.ID + 1,
	})
	partition := &perceptron.PartitionInfo{
		Type:        perceptron.PartitionTypeRange,
		Expr:        "ptn",
		Enable:      true,
		Definitions: definitions,
	}
	blockInfo.DeferredCausets = defcaus
	blockInfo.Partition = partition
	is := schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{blockInfo})
	return is
}
