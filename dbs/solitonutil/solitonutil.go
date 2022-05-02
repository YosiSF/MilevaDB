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

package solitonutil

import (
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastik"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
)

// StochastikExecInGoroutine export for testing.
func StochastikExecInGoroutine(c *check.C, s ekv.CausetStorage, allegrosql string, done chan error) {
	ExecMultiALLEGROSQLInGoroutine(c, s, "test_db", []string{allegrosql}, done)
}

// ExecMultiALLEGROSQLInGoroutine exports for testing.
func ExecMultiALLEGROSQLInGoroutine(c *check.C, s ekv.CausetStorage, dbName string, multiALLEGROSQL []string, done chan error) {
	go func() {
		se, err := stochastik.CreateStochastik4Test(s)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		defer se.Close()
		_, err = se.Execute(context.Background(), "use "+dbName)
		if err != nil {
			done <- errors.Trace(err)
			return
		}
		for _, allegrosql := range multiALLEGROSQL {
			rs, err := se.Execute(context.Background(), allegrosql)
			if err != nil {
				done <- errors.Trace(err)
				return
			}
			if rs != nil {
				done <- errors.Errorf("RecordSet should be empty.")
				return
			}
			done <- nil
		}
	}()
}

// ExtractAllBlockHandles extracts all handles of a given block.
func ExtractAllBlockHandles(se stochastik.Stochastik, dbName, tbName string) ([]int64, error) {
	dom := petri.GetPetri(se)
	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr(dbName), perceptron.NewCIStr(tbName))
	if err != nil {
		return nil, err
	}
	err = se.NewTxn(context.Background())
	if err != nil {
		return nil, err
	}
	var allHandles []int64
	err = tbl.IterRecords(se, tbl.FirstKey(), nil,
		func(h ekv.Handle, _ []types.Causet, _ []*block.DeferredCauset) (more bool, err error) {
			allHandles = append(allHandles, h.IntValue())
			return true, nil
		})
	return allHandles, err
}
