//Copyright 2020 WHTCORPS INC ALL RIGHTS RESERVED
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions.


package Interlock

import(
		"context"
		"math"

	)


// CheckIndexRangeExec outputs the index values which has handle between begin and end.
type CheckIndexRangeExec struct {
	baseInterlock

	table    *serial.TableInfo
	index    *serial.IndexInfo
	is       schemaReplicant.SchemaReplicant
	startKey []types.Datum

	handleRanges []ast.HandleRange
	srcSoliton     *soliton.Soliton

	result distsql.SelectResult
	cols   []*serial.ColumnInfo
}

// Next implements the Interlock Next interface.
func (e *CheckIndexRangeExec) Next(ctx context.contextctx, req *soliton.Soliton) error {
	req.Reset()
	handleIdx := e.schema.Len() - 1
	for {
		err := e.result.Next(ctx, e.srcSoliton)
		if err != nil {
			return err
		}
		if e.srcSoliton.NumRows() == 0 {
			return nil
		}
		iteron := soliton.NewIterator4Soliton(e.srcSoliton)
		for row := iteron.Begin(); row != iteron.End(); row = iteron.Next() {
			handle := row.GetInt64(handleIdx)
			for _, hr := range e.handleRanges {
				if handle >= hr.Begin && handle < hr.End {
					req.AppendRow(row)
					break
				}
			}
		}
		if req.NumRows() > 0 {
			return nil
		}
	}
}


