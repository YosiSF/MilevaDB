MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
)

var _ = Suite(&testPartitionPruningSuite{})

type testPartitionPruningSuite struct {
	partitionProcessor
}

func (s *testPartitionPruningSuite) TestCanBePrune(c *C) {
	// For the following case:
	// CREATE TABLE t1 ( recdate  DATETIME NOT NULL )
	// PARTITION BY RANGE( TO_DAYS(recdate) ) (
	// 	PARTITION p0 VALUES LESS THAN ( TO_DAYS('2007-03-08') ),
	// 	PARTITION p1 VALUES LESS THAN ( TO_DAYS('2007-04-01') )
	// );
	// SELECT * FROM t1 WHERE recdate < '2007-03-08 00:00:00';
	// SELECT * FROM t1 WHERE recdate > '2020-03-08 00:00:00';

	tc := prepareTestCtx(c,
		"create block t (d datetime not null)",
		"to_days(d)",
	)
	lessThan := lessThanDataInt{data: []int64{733108, 733132}, maxvalue: false}
	prunner := &rangePruner{lessThan, tc.col, tc.fn, true}

	queryExpr := tc.expr("d < '2000-03-08 00:00:00'")
	result := partitionRangeForCNFExpr(tc.sctx, queryExpr, prunner, fullRange(len(lessThan.data)))
	c.Assert(equalPartitionRangeOR(result, partitionRangeOR{{0, 1}}), IsTrue)

	queryExpr = tc.expr("d > '2020-03-08 00:00:00'")
	result = partitionRangeForCNFExpr(tc.sctx, queryExpr, prunner, fullRange(len(lessThan.data)))
	c.Assert(equalPartitionRangeOR(result, partitionRangeOR{}), IsTrue)

	// For the following case:
	// CREATE TABLE quarterly_report_status (
	// 	report_id INT NOT NULL,
	// 	report_status VARCHAR(20) NOT NULL,
	// 	report_uFIDelated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UFIDelATE CURRENT_TIMESTAMP)
	// PARTITION BY RANGE (UNIX_TIMESTAMP(report_uFIDelated)) (
	// 	PARTITION p0 VALUES LESS THAN (UNIX_TIMESTAMP('2008-01-01 00:00:00')),
	// 	PARTITION p1 VALUES LESS THAN (UNIX_TIMESTAMP('2008-04-01 00:00:00')),
	// 	PARTITION p2 VALUES LESS THAN (UNIX_TIMESTAMP('2010-01-01 00:00:00')),
	// 	PARTITION p3 VALUES LESS THAN (MAXVALUE)
	// );
	tc = prepareTestCtx(c,
		"create block t (report_uFIDelated timestamp)",
		"unix_timestamp(report_uFIDelated)",
	)
	lessThan = lessThanDataInt{data: []int64{1199145600, 1207008000, 1262304000, 0}, maxvalue: true}
	prunner = &rangePruner{lessThan, tc.col, tc.fn, true}

	queryExpr = tc.expr("report_uFIDelated > '2008-05-01 00:00:00'")
	result = partitionRangeForCNFExpr(tc.sctx, queryExpr, prunner, fullRange(len(lessThan.data)))
	c.Assert(equalPartitionRangeOR(result, partitionRangeOR{{2, 4}}), IsTrue)

	queryExpr = tc.expr("report_uFIDelated > unix_timestamp('2008-05-01 00:00:00')")
	partitionRangeForCNFExpr(tc.sctx, queryExpr, prunner, fullRange(len(lessThan.data)))
	// TODO: Uncomment the check after fixing issue https://github.com/whtcorpsinc/MilevaDB-Prod/issues/12028
	// c.Assert(equalPartitionRangeOR(result, partitionRangeOR{{2, 4}}), IsTrue)
	// report_uFIDelated > unix_timestamp('2008-05-01 00:00:00') is converted to gt(t.t.report_uFIDelated, <nil>)
	// Because unix_timestamp('2008-05-01 00:00:00') is fold to constant int 1564761600, and compare it with timestamp (report_uFIDelated)
	// need to convert 1564761600 to a timestamp, during that step, an error happen and the result is set to <nil>
}

func (s *testPartitionPruningSuite) TestPruneUseBinarySearch(c *C) {
	lessThan := lessThanDataInt{data: []int64{4, 7, 11, 14, 17, 0}, maxvalue: true}
	cases := []struct {
		input  dataForPrune
		result partitionRange
	}{
		{dataForPrune{ast.EQ, 66}, partitionRange{5, 6}},
		{dataForPrune{ast.EQ, 14}, partitionRange{4, 5}},
		{dataForPrune{ast.EQ, 10}, partitionRange{2, 3}},
		{dataForPrune{ast.EQ, 3}, partitionRange{0, 1}},
		{dataForPrune{ast.LT, 66}, partitionRange{0, 6}},
		{dataForPrune{ast.LT, 14}, partitionRange{0, 4}},
		{dataForPrune{ast.LT, 10}, partitionRange{0, 3}},
		{dataForPrune{ast.LT, 3}, partitionRange{0, 1}},
		{dataForPrune{ast.GE, 66}, partitionRange{5, 6}},
		{dataForPrune{ast.GE, 14}, partitionRange{4, 6}},
		{dataForPrune{ast.GE, 10}, partitionRange{2, 6}},
		{dataForPrune{ast.GE, 3}, partitionRange{0, 6}},
		{dataForPrune{ast.GT, 66}, partitionRange{5, 6}},
		{dataForPrune{ast.GT, 14}, partitionRange{4, 6}},
		{dataForPrune{ast.GT, 10}, partitionRange{3, 6}},
		{dataForPrune{ast.GT, 3}, partitionRange{1, 6}},
		{dataForPrune{ast.GT, 2}, partitionRange{0, 6}},
		{dataForPrune{ast.LE, 66}, partitionRange{0, 6}},
		{dataForPrune{ast.LE, 14}, partitionRange{0, 5}},
		{dataForPrune{ast.LE, 10}, partitionRange{0, 3}},
		{dataForPrune{ast.LE, 3}, partitionRange{0, 1}},
		{dataForPrune{ast.IsNull, 0}, partitionRange{0, 1}},
		{dataForPrune{"illegal", 0}, partitionRange{0, 6}},
	}

	for i, ca := range cases {
		start, end := pruneUseBinarySearch(lessThan, ca.input, false)
		c.Assert(ca.result.start, Equals, start, Commentf("fail = %d", i))
		c.Assert(ca.result.end, Equals, end, Commentf("fail = %d", i))
	}
}

type testCtx struct {
	c               *C
	sctx            stochastikctx.Context
	schemaReplicant *expression.Schema
	columns         []*expression.DeferredCauset
	names           types.NameSlice
	lessThan        lessThanDataInt
	col             *expression.DeferredCauset
	fn              *expression.ScalarFunction
}

func prepareTestCtx(c *C, createBlock string, partitionExpr string) *testCtx {
	p := berolinaAllegroSQL.New()
	stmt, err := p.ParseOneStmt(createBlock, "", "")
	c.Assert(err, IsNil)
	sctx := mock.NewContext()
	tblInfo, err := dbs.BuildBlockInfoFromAST(stmt.(*ast.CreateBlockStmt))
	c.Assert(err, IsNil)
	columns, names, err := expression.DeferredCausetInfos2DeferredCausetsAndNames(sctx, perceptron.NewCIStr("t"), tblInfo.Name, tblInfo.DefCauss(), tblInfo)
	c.Assert(err, IsNil)
	schemaReplicant := expression.NewSchema(columns...)

	col, fn, _, err := makePartitionByFnDefCaus(sctx, columns, names, partitionExpr)
	c.Assert(err, IsNil)
	return &testCtx{
		c:               c,
		sctx:            sctx,
		schemaReplicant: schemaReplicant,
		columns:         columns,
		names:           names,
		col:             col,
		fn:              fn,
	}
}

func (tc *testCtx) expr(expr string) []expression.Expression {
	res, err := expression.ParseSimpleExprsWithNames(tc.sctx, expr, tc.schemaReplicant, tc.names)
	tc.c.Assert(err, IsNil)
	return res
}

func (s *testPartitionPruningSuite) TestPartitionRangeForExpr(c *C) {
	tc := prepareTestCtx(c,
		"create block t (a int)",
		"a",
	)
	lessThan := lessThanDataInt{data: []int64{4, 7, 11, 14, 17, 0}, maxvalue: true}
	prunner := &rangePruner{lessThan, tc.columns[0], nil, false}
	cases := []struct {
		input  string
		result partitionRangeOR
	}{
		{"a > 3", partitionRangeOR{{1, 6}}},
		{"a < 3", partitionRangeOR{{0, 1}}},
		{"a >= 11", partitionRangeOR{{3, 6}}},
		{"a > 11", partitionRangeOR{{3, 6}}},
		{"a < 11", partitionRangeOR{{0, 3}}},
		{"a = 16", partitionRangeOR{{4, 5}}},
		{"a > 66", partitionRangeOR{{5, 6}}},
		{"a > 2 and a < 10", partitionRangeOR{{0, 3}}},
		{"a < 2 or a >= 15", partitionRangeOR{{0, 1}, {4, 6}}},
		{"a is null", partitionRangeOR{{0, 1}}},
		{"12 > a", partitionRangeOR{{0, 4}}},
		{"4 <= a", partitionRangeOR{{1, 6}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExprsWithNames(tc.sctx, ca.input, tc.schemaReplicant, tc.names)
		c.Assert(err, IsNil)
		result := fullRange(lessThan.length())
		result = partitionRangeForExpr(tc.sctx, expr[0], prunner, result)
		c.Assert(equalPartitionRangeOR(ca.result, result), IsTrue, Commentf("unexpected:", ca.input))
	}
}

func equalPartitionRangeOR(x, y partitionRangeOR) bool {
	if len(x) != len(y) {
		return false
	}
	for i := 0; i < len(x); i++ {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func (s *testPartitionPruningSuite) TestPartitionRangeOperation(c *C) {
	testIntersectionRange := []struct {
		input1 partitionRangeOR
		input2 partitionRange
		result partitionRangeOR
	}{
		{input1: partitionRangeOR{{0, 3}, {6, 12}},
			input2: partitionRange{4, 7},
			result: partitionRangeOR{{6, 7}}},
		{input1: partitionRangeOR{{0, 5}},
			input2: partitionRange{6, 7},
			result: partitionRangeOR{}},
		{input1: partitionRangeOR{{0, 4}, {6, 7}, {8, 11}},
			input2: partitionRange{3, 9},
			result: partitionRangeOR{{3, 4}, {6, 7}, {8, 9}}},
	}
	for i, ca := range testIntersectionRange {
		result := ca.input1.intersectionRange(ca.input2.start, ca.input2.end)
		c.Assert(equalPartitionRangeOR(ca.result, result), IsTrue, Commentf("failed %d", i))
	}

	testIntersection := []struct {
		input1 partitionRangeOR
		input2 partitionRangeOR
		result partitionRangeOR
	}{
		{input1: partitionRangeOR{{0, 3}, {6, 12}},
			input2: partitionRangeOR{{4, 7}},
			result: partitionRangeOR{{6, 7}}},
		{input1: partitionRangeOR{{4, 7}},
			input2: partitionRangeOR{{0, 3}, {6, 12}},
			result: partitionRangeOR{{6, 7}}},
		{input1: partitionRangeOR{{4, 7}, {8, 10}},
			input2: partitionRangeOR{{0, 5}, {6, 12}},
			result: partitionRangeOR{{4, 5}, {6, 7}, {8, 10}}},
	}
	for i, ca := range testIntersection {
		result := ca.input1.intersection(ca.input2)
		c.Assert(equalPartitionRangeOR(ca.result, result), IsTrue, Commentf("failed %d", i))
	}

	testUnion := []struct {
		input1 partitionRangeOR
		input2 partitionRangeOR
		result partitionRangeOR
	}{
		{input1: partitionRangeOR{{0, 1}, {2, 7}},
			input2: partitionRangeOR{{3, 5}},
			result: partitionRangeOR{{0, 1}, {2, 7}}},
		{input1: partitionRangeOR{{2, 7}},
			input2: partitionRangeOR{{0, 3}, {4, 12}},
			result: partitionRangeOR{{0, 12}}},
		{input1: partitionRangeOR{{4, 7}, {8, 10}},
			input2: partitionRangeOR{{0, 5}},
			result: partitionRangeOR{{0, 7}, {8, 10}}},
	}
	for i, ca := range testUnion {
		result := ca.input1.union(ca.input2)
		c.Assert(equalPartitionRangeOR(ca.result, result), IsTrue, Commentf("failed %d", i))
	}
}

func (s *testPartitionPruningSuite) TestPartitionRangePrunner2VarChar(c *C) {
	tc := prepareTestCtx(c,
		"create block t (a varchar(32))",
		"a",
	)
	lessThanDataInt := []string{"'c'", "'f'", "'h'", "'l'", "'t'"}
	lessThan := make([]expression.Expression, len(lessThanDataInt)+1) // +1 for maxvalue
	for i, str := range lessThanDataInt {
		tmp, err := expression.ParseSimpleExprsWithNames(tc.sctx, str, tc.schemaReplicant, tc.names)
		c.Assert(err, IsNil)
		lessThan[i] = tmp[0]
	}

	prunner := &rangeDeferredCausetsPruner{lessThan, tc.columns[0], true}
	cases := []struct {
		input  string
		result partitionRangeOR
	}{
		{"a > 'g'", partitionRangeOR{{2, 6}}},
		{"a < 'h'", partitionRangeOR{{0, 3}}},
		{"a >= 'm'", partitionRangeOR{{4, 6}}},
		{"a > 'm'", partitionRangeOR{{4, 6}}},
		{"a < 'f'", partitionRangeOR{{0, 2}}},
		{"a = 'c'", partitionRangeOR{{1, 2}}},
		{"a > 't'", partitionRangeOR{{5, 6}}},
		{"a > 'c' and a < 'q'", partitionRangeOR{{1, 5}}},
		{"a < 'l' or a >= 'w'", partitionRangeOR{{0, 4}, {5, 6}}},
		{"a is null", partitionRangeOR{{0, 1}}},
		{"'mm' > a", partitionRangeOR{{0, 5}}},
		{"'f' <= a", partitionRangeOR{{2, 6}}},
		{"'f' >= a", partitionRangeOR{{0, 3}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExprsWithNames(tc.sctx, ca.input, tc.schemaReplicant, tc.names)
		c.Assert(err, IsNil)
		result := fullRange(len(lessThan))
		result = partitionRangeForExpr(tc.sctx, expr[0], prunner, result)
		c.Assert(equalPartitionRangeOR(ca.result, result), IsTrue, Commentf("unexpected:", ca.input))
	}
}

func (s *testPartitionPruningSuite) TestPartitionRangePrunner2Date(c *C) {
	tc := prepareTestCtx(c,
		"create block t (a date)",
		"a",
	)
	lessThanDataInt := []string{
		"'1999-06-01'",
		"'2000-05-01'",
		"'2008-04-01'",
		"'2010-03-01'",
		"'2020-02-01'",
		"'2020-01-01'"}
	lessThan := make([]expression.Expression, len(lessThanDataInt))
	for i, str := range lessThanDataInt {
		tmp, err := expression.ParseSimpleExprsWithNames(tc.sctx, str, tc.schemaReplicant, tc.names)
		c.Assert(err, IsNil)
		lessThan[i] = tmp[0]
	}

	prunner := &rangeDeferredCausetsPruner{lessThan, tc.columns[0], false}
	cases := []struct {
		input  string
		result partitionRangeOR
	}{
		{"a < '1943-02-12'", partitionRangeOR{{0, 1}}},
		{"a >= '1969-02-13'", partitionRangeOR{{0, 6}}},
		{"a > '2003-03-13'", partitionRangeOR{{2, 6}}},
		{"a < '2006-02-03'", partitionRangeOR{{0, 3}}},
		{"a = '2007-07-07'", partitionRangeOR{{2, 3}}},
		{"a > '1949-10-10'", partitionRangeOR{{0, 6}}},
		{"a > '2020-02-01' and a < '2000-01-03'", partitionRangeOR{}},
		{"a < '1969-11-12' or a >= '2020-09-18'", partitionRangeOR{{0, 1}, {5, 6}}},
		{"a is null", partitionRangeOR{{0, 1}}},
		{"'2003-02-27' >= a", partitionRangeOR{{0, 3}}},
		{"'2020-10-24' < a", partitionRangeOR{{4, 6}}},
		{"'2003-03-30' > a", partitionRangeOR{{0, 3}}},
	}

	for _, ca := range cases {
		expr, err := expression.ParseSimpleExprsWithNames(tc.sctx, ca.input, tc.schemaReplicant, tc.names)
		c.Assert(err, IsNil)
		result := fullRange(len(lessThan))
		result = partitionRangeForExpr(tc.sctx, expr[0], prunner, result)
		c.Assert(equalPartitionRangeOR(ca.result, result), IsTrue, Commentf("unexpected:", ca.input))
	}
}
