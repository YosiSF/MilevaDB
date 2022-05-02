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

package dbs_test

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
)

var _ = Suite(&testDBSAlgorithmSuite{})

var (
	allAlgorithm = []ast.AlgorithmType{ast.AlgorithmTypeINTERLOCKy,
		ast.AlgorithmTypeInplace, ast.AlgorithmTypeInstant}
)

type testDBSAlgorithmSuite struct{}

type testCase struct {
	alterSpec          ast.AlterBlockSpec
	supportedAlgorithm []ast.AlgorithmType
	expectedAlgorithm  []ast.AlgorithmType
}

func (s *testDBSAlgorithmSuite) TestFindAlterAlgorithm(c *C) {
	supportedInstantAlgorithms := []ast.AlgorithmType{ast.AlgorithmTypeDefault, ast.AlgorithmTypeINTERLOCKy, ast.AlgorithmTypeInplace, ast.AlgorithmTypeInstant}
	expectedInstantAlgorithms := []ast.AlgorithmType{ast.AlgorithmTypeInstant, ast.AlgorithmTypeInstant, ast.AlgorithmTypeInstant, ast.AlgorithmTypeInstant}

	testCases := []testCase{
		{
			ast.AlterBlockSpec{Tp: ast.AlterBlockAddConstraint},
			[]ast.AlgorithmType{ast.AlgorithmTypeDefault, ast.AlgorithmTypeINTERLOCKy, ast.AlgorithmTypeInplace},
			[]ast.AlgorithmType{ast.AlgorithmTypeInplace, ast.AlgorithmTypeInplace, ast.AlgorithmTypeInplace},
		},

		{ast.AlterBlockSpec{Tp: ast.AlterBlockAddDeferredCausets}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterBlockSpec{Tp: ast.AlterBlockDropDeferredCauset}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterBlockSpec{Tp: ast.AlterBlockDropPrimaryKey}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterBlockSpec{Tp: ast.AlterBlockDropIndex}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterBlockSpec{Tp: ast.AlterBlockDropForeignKey}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterBlockSpec{Tp: ast.AlterBlockRenameBlock}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterBlockSpec{Tp: ast.AlterBlockRenameIndex}, supportedInstantAlgorithms, expectedInstantAlgorithms},

		// Alter block options.
		{ast.AlterBlockSpec{Tp: ast.AlterBlockOption, Options: []*ast.BlockOption{{Tp: ast.BlockOptionShardRowID}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterBlockSpec{Tp: ast.AlterBlockOption, Options: []*ast.BlockOption{{Tp: ast.BlockOptionAutoIncrement}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterBlockSpec{Tp: ast.AlterBlockOption, Options: []*ast.BlockOption{{Tp: ast.BlockOptionComment}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterBlockSpec{Tp: ast.AlterBlockOption, Options: []*ast.BlockOption{{Tp: ast.TableOptionCharset}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableOption, Options: []*ast.TableOption{{Tp: ast.TableOptionDefCauslate}}}, supportedInstantAlgorithms, expectedInstantAlgorithms},

		// TODO: after we support migrate the data of partitions, change below cases.
		{ast.AlterTableSpec{Tp: ast.AlterTableCoalescePartitions}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableAddPartitions}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableDropPartition}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableTruncatePartition}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableExchangePartition}, supportedInstantAlgorithms, expectedInstantAlgorithms},

		// TODO: after we support dagger a block, change the below case.
		{ast.AlterTableSpec{Tp: ast.AlterTableLock}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		// TODO: after we support changing the defCausumn type, below cases need to change.
		{ast.AlterTableSpec{Tp: ast.AlterTableModifyDeferredCauset}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableChangeDeferredCauset}, supportedInstantAlgorithms, expectedInstantAlgorithms},
		{ast.AlterTableSpec{Tp: ast.AlterTableAlterDeferredCauset}, supportedInstantAlgorithms, expectedInstantAlgorithms},
	}

	for _, tc := range testCases {
		runAlterAlgorithmTestCases(c, &tc)
	}
}

func runAlterAlgorithmTestCases(c *C, tc *testCase) {
	unsupported := make([]ast.AlgorithmType, 0, len(allAlgorithm))
Loop:
	for _, alm := range allAlgorithm {
		for _, almSupport := range tc.supportedAlgorithm {
			if alm == almSupport {
				continue Loop
			}
		}

		unsupported = append(unsupported, alm)
	}

	var algorithm ast.AlgorithmType
	var err error

	// Test supported.
	for i, alm := range tc.supportedAlgorithm {
		algorithm, err = dbs.ResolveAlterAlgorithm(&tc.alterSpec, alm)
		if err != nil {
			c.Assert(dbs.ErrAlterOperationNotSupported.Equal(err), IsTrue)
		}
		c.Assert(algorithm, Equals, tc.expectedAlgorithm[i])
	}

	// Test unsupported.
	for _, alm := range unsupported {
		algorithm, err = dbs.ResolveAlterAlgorithm(&tc.alterSpec, alm)
		c.Assert(algorithm, Equals, ast.AlgorithmTypeDefault)
		c.Assert(err, NotNil, Commentf("Tp:%v, alm:%s", tc.alterSpec.Tp, alm))
		c.Assert(dbs.ErrAlterOperationNotSupported.Equal(err), IsTrue)
	}
}
