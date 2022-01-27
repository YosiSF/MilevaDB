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

package executor

import (
	"context"
	"fmt"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/block"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// PartitionBlockExecutor is a Executor for partitioned block.
// It works by wrap the underlying BlockReader/IndexReader/IndexLookUpReader.
type PartitionBlockExecutor struct {
	baseExecutor

	nextPartition
	partitions []block.PhysicalBlock
	cursor     int
	curr       Executor
}

type nextPartition interface {
	nextPartition(context.Context, block.PhysicalBlock) (Executor, error)
}

type nextPartitionForBlockReader struct {
	exec *BlockReaderExecutor
}

func (n nextPartitionForBlockReader) nextPartition(ctx context.Context, tbl block.PhysicalBlock) (Executor, error) {
	n.exec.block = tbl
	n.exec.kvRanges = n.exec.kvRanges[:0]
	if err := uFIDelatePosetDagRequestBlockID(ctx, n.exec.posetPosetDagPB, tbl.Meta().ID, tbl.GetPhysicalID()); err != nil {
		return nil, err
	}
	return n.exec, nil
}

type nextPartitionForIndexLookUp struct {
	exec *IndexLookUpExecutor
}

func (n nextPartitionForIndexLookUp) nextPartition(ctx context.Context, tbl block.PhysicalBlock) (Executor, error) {
	n.exec.block = tbl
	return n.exec, nil
}

type nextPartitionForIndexReader struct {
	exec *IndexReaderExecutor
}

func (n nextPartitionForIndexReader) nextPartition(ctx context.Context, tbl block.PhysicalBlock) (Executor, error) {
	exec := n.exec
	exec.block = tbl
	exec.physicalBlockID = tbl.GetPhysicalID()
	return exec, nil
}

type nextPartitionForIndexMerge struct {
	exec *IndexMergeReaderExecutor
}

func (n nextPartitionForIndexMerge) nextPartition(ctx context.Context, tbl block.PhysicalBlock) (Executor, error) {
	exec := n.exec
	exec.block = tbl
	return exec, nil
}

type nextPartitionForUnionScan struct {
	b     *executorBuilder
	us    *plannercore.PhysicalUnionScan
	child nextPartition
}

// nextPartition implements the nextPartition interface.
// For union scan on partitioned block, the executor should be PartitionBlock->UnionScan->BlockReader rather than
// UnionScan->PartitionBlock->BlockReader
func (n nextPartitionForUnionScan) nextPartition(ctx context.Context, tbl block.PhysicalBlock) (Executor, error) {
	childExec, err := n.child.nextPartition(ctx, tbl)
	if err != nil {
		return nil, err
	}

	n.b.err = nil
	ret := n.b.buildUnionScanFromReader(childExec, n.us)
	return ret, n.b.err
}

func nextPartitionWithTrace(ctx context.Context, n nextPartition, tbl block.PhysicalBlock) (Executor, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("nextPartition %d", tbl.GetPhysicalID()), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	return n.nextPartition(ctx, tbl)
}

// uFIDelatePosetDagRequestBlockID uFIDelate the block ID in the PosetDag request to partition ID.
// EinsteinDB only use that block ID for log, but TiFlash use it.
func uFIDelatePosetDagRequestBlockID(ctx context.Context, posetPosetDag *fidelpb.PosetDagRequest, blockID, partitionID int64) error {
	// TiFlash set RootExecutor field and ignore Executors field.
	if posetPosetDag.RootExecutor != nil {
		return uFIDelateExecutorBlockID(ctx, posetPosetDag.RootExecutor, blockID, partitionID, true)
	}
	for i := 0; i < len(posetPosetDag.Executors); i++ {
		exec := posetPosetDag.Executors[i]
		err := uFIDelateExecutorBlockID(ctx, exec, blockID, partitionID, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func uFIDelateExecutorBlockID(ctx context.Context, exec *fidelpb.Executor, blockID, partitionID int64, recursive bool) error {
	var child *fidelpb.Executor
	switch exec.Tp {
	case fidelpb.ExecType_TypeBlockScan:
		exec.TblScan.BlockId = partitionID
		// For test coverage.
		if tmp := ctx.Value("nextPartitionUFIDelatePosetDagReq"); tmp != nil {
			m := tmp.(map[int64]struct{})
			m[partitionID] = struct{}{}
		}
	case fidelpb.ExecType_TypeIndexScan:
		exec.IdxScan.BlockId = partitionID
	case fidelpb.ExecType_TypeSelection:
		child = exec.Selection.Child
	case fidelpb.ExecType_TypeAggregation, fidelpb.ExecType_TypeStreamAgg:
		child = exec.Aggregation.Child
	case fidelpb.ExecType_TypeTopN:
		child = exec.TopN.Child
	case fidelpb.ExecType_TypeLimit:
		child = exec.Limit.Child
	case fidelpb.ExecType_TypeJoin:
		// TiFlash currently does not support Join on partition block.
		// The planner should not generate this HoTT of plan.
		// So the code should never run here.
		return errors.New("wrong plan, join on partition block is not supported on TiFlash")
	default:
		return errors.Trace(fmt.Errorf("unknown new fidelpb protodefCaus %d", exec.Tp))
	}
	if child != nil && recursive {
		return uFIDelateExecutorBlockID(ctx, child, blockID, partitionID, recursive)
	}
	return nil
}

// Open implements the Executor interface.
func (e *PartitionBlockExecutor) Open(ctx context.Context) error {
	e.cursor = 0
	e.curr = nil
	return nil
}

// Next implements the Executor interface.
func (e *PartitionBlockExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	var err error
	for e.cursor < len(e.partitions) {
		if e.curr == nil {
			n := e.nextPartition
			e.curr, err = nextPartitionWithTrace(ctx, n, e.partitions[e.cursor])
			if err != nil {
				return err
			}
			if err := e.curr.Open(ctx); err != nil {
				return err
			}
		}

		err = Next(ctx, e.curr, chk)
		if err != nil {
			return err
		}

		if chk.NumEvents() > 0 {
			break
		}

		err = e.curr.Close()
		if err != nil {
			return err
		}
		e.curr = nil
		e.cursor++
	}
	return nil
}

// Close implements the Executor interface.
func (e *PartitionBlockExecutor) Close() error {
	var err error
	if e.curr != nil {
		err = e.curr.Close()
		e.curr = nil
	}
	return err
}
