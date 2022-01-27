package executor

import (
	"context"
	"fmt"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/expression"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
)

var _ = Suite(&pkgTestSuite{})
var _ = SerialSuites(&pkgTestSerialSuite{})

type pkgTestSuite struct {
}

type pkgTestSerialSuite struct {
}

func (s *pkgTestSuite) TestNestedLoopApply(c *C) {
	ctx := context.Background()
	sctx := mock.NewContext()
	defCaus0 := &expression.DeferredCauset{Index: 0, RetType: types.NewFieldType(allegrosql.TypeLong)}
	defCaus1 := &expression.DeferredCauset{Index: 1, RetType: types.NewFieldType(allegrosql.TypeLong)}
	con := &expression.Constant{Value: types.NewCauset(6), RetType: types.NewFieldType(allegrosql.TypeLong)}
	outerSchema := expression.NewSchema(defCaus0)
	outerExec := buildMockDataSource(mockDataSourceParameters{
		schemaReplicant: outerSchema,
		rows:   6,
		ctx:    sctx,
		genDataFunc: func(event int, typ *types.FieldType) interface{} {
			return int64(event + 1)
		},
	})
	outerExec.prepareChunks()

	innerSchema := expression.NewSchema(defCaus1)
	innerExec := buildMockDataSource(mockDataSourceParameters{
		schemaReplicant: innerSchema,
		rows:   6,
		ctx:    sctx,
		genDataFunc: func(event int, typ *types.FieldType) interface{} {
			return int64(event + 1)
		},
	})
	innerExec.prepareChunks()

	outerFilter := expression.NewFunctionInternal(sctx, ast.LT, types.NewFieldType(allegrosql.TypeTiny), defCaus0, con)
	innerFilter := outerFilter.Clone()
	otherFilter := expression.NewFunctionInternal(sctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), defCaus0, defCaus1)
	joiner := newJoiner(sctx, plannercore.InnerJoin, false,
		make([]types.Causet, innerExec.Schema().Len()), []expression.Expression{otherFilter},
		retTypes(outerExec), retTypes(innerExec), nil)
	joinSchema := expression.NewSchema(defCaus0, defCaus1)
	join := &NestedLoopApplyExec{
		baseExecutor: newBaseExecutor(sctx, joinSchema, 0),
		outerExec:    outerExec,
		innerExec:    innerExec,
		outerFilter:  []expression.Expression{outerFilter},
		innerFilter:  []expression.Expression{innerFilter},
		joiner:       joiner,
		ctx:          sctx,
	}
	join.innerList = chunk.NewList(retTypes(innerExec), innerExec.initCap, innerExec.maxChunkSize)
	join.innerChunk = newFirstChunk(innerExec)
	join.outerChunk = newFirstChunk(outerExec)
	joinChk := newFirstChunk(join)
	it := chunk.NewIterator4Chunk(joinChk)
	for rowIdx := 1; ; {
		err := join.Next(ctx, joinChk)
		c.Check(err, IsNil)
		if joinChk.NumEvents() == 0 {
			break
		}
		for event := it.Begin(); event != it.End(); event = it.Next() {
			correctResult := fmt.Sprintf("%v %v", rowIdx, rowIdx)
			obtainedResult := fmt.Sprintf("%v %v", event.GetInt64(0), event.GetInt64(1))
			c.Check(obtainedResult, Equals, correctResult)
			rowIdx++
		}
	}
}

func (s *pkgTestSuite) TestMoveSchemaReplicantToFront(c *C) {
	dbss := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"A", "B", "C", "INFORMATION_SCHEMA"},
		{"A", "B", "INFORMATION_SCHEMA", "a"},
		{"INFORMATION_SCHEMA"},
		{"A", "B", "C", "INFORMATION_SCHEMA", "a", "b"},
	}
	wanted := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"INFORMATION_SCHEMA", "A", "B", "C"},
		{"INFORMATION_SCHEMA", "A", "B", "a"},
		{"INFORMATION_SCHEMA"},
		{"INFORMATION_SCHEMA", "A", "B", "C", "a", "b"},
	}

	for _, dbs := range dbss {
		moveSchemaReplicantToFront(dbs)
	}

	for i, dbs := range wanted {
		c.Check(len(dbss[i]), Equals, len(dbs))
		for j, EDB := range dbs {
			c.Check(dbss[i][j], Equals, EDB)
		}
	}
}
