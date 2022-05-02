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

package implementation

import (
	"math"

	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/memo"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
)

// BlockDualImpl implementation of PhysicalBlockDual.
type BlockDualImpl struct {
	baseImpl
}

// NewBlockDualImpl creates a new block dual Implementation.
func NewBlockDualImpl(dual *plannercore.PhysicalBlockDual) *BlockDualImpl {
	return &BlockDualImpl{baseImpl{plan: dual}}
}

// CalcCost calculates the cost of the block dual Implementation.
func (impl *BlockDualImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	return 0
}

// MemBlockScanImpl implementation of PhysicalBlockDual.
type MemBlockScanImpl struct {
	baseImpl
}

// NewMemBlockScanImpl creates a new block dual Implementation.
func NewMemBlockScanImpl(dual *plannercore.PhysicalMemBlock) *MemBlockScanImpl {
	return &MemBlockScanImpl{baseImpl{plan: dual}}
}

// CalcCost calculates the cost of the block dual Implementation.
func (impl *MemBlockScanImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	return 0
}

// BlockReaderImpl implementation of PhysicalBlockReader.
type BlockReaderImpl struct {
	baseImpl
	tblDefCausHists *statistics.HistDefCausl
}

// NewBlockReaderImpl creates a new block reader Implementation.
func NewBlockReaderImpl(reader *plannercore.PhysicalBlockReader, hists *statistics.HistDefCausl) *BlockReaderImpl {
	base := baseImpl{plan: reader}
	impl := &BlockReaderImpl{
		baseImpl:        base,
		tblDefCausHists: hists,
	}
	return impl
}

// CalcCost calculates the cost of the block reader Implementation.
func (impl *BlockReaderImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalBlockReader)
	width := impl.tblDefCausHists.GetAvgRowSize(impl.plan.SCtx(), reader.Schema().DeferredCausets, false, false)
	sessVars := reader.SCtx().GetStochaseinstein_dbars()
	networkCost := outCount * sessVars.NetworkFactor * width
	// CausetTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to INTERLOCK iterator workers. According to `INTERLOCKClient::Send`, the concurrency
	// is Min(DistALLEGROSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistALLEGROSQLScanConcurrency.
	INTERLOCKIterWorkers := float64(sessVars.DistALLEGROSQLScanConcurrency())
	impl.cost = (networkCost + children[0].GetCost()) / INTERLOCKIterWorkers
	return impl.cost
}

// GetCostLimit implements Implementation interface.
func (impl *BlockReaderImpl) GetCostLimit(costLimit float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalBlockReader)
	sessVars := reader.SCtx().GetStochaseinstein_dbars()
	INTERLOCKIterWorkers := float64(sessVars.DistALLEGROSQLScanConcurrency())
	if math.MaxFloat64/INTERLOCKIterWorkers < costLimit {
		return math.MaxFloat64
	}
	return costLimit * INTERLOCKIterWorkers
}

// BlockScanImpl implementation of PhysicalBlockScan.
type BlockScanImpl struct {
	baseImpl
	tblDefCausHists *statistics.HistDefCausl
	tblDefCauss     []*expression.DeferredCauset
}

// NewBlockScanImpl creates a new block scan Implementation.
func NewBlockScanImpl(ts *plannercore.PhysicalBlockScan, defcaus []*expression.DeferredCauset, hists *statistics.HistDefCausl) *BlockScanImpl {
	base := baseImpl{plan: ts}
	impl := &BlockScanImpl{
		baseImpl:        base,
		tblDefCausHists: hists,
		tblDefCauss:     defcaus,
	}
	return impl
}

// CalcCost calculates the cost of the block scan Implementation.
func (impl *BlockScanImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	ts := impl.plan.(*plannercore.PhysicalBlockScan)
	width := impl.tblDefCausHists.GetBlockAvgRowSize(impl.plan.SCtx(), impl.tblDefCauss, ekv.EinsteinDB, true)
	sessVars := ts.SCtx().GetStochaseinstein_dbars()
	impl.cost = outCount * sessVars.ScanFactor * width
	if ts.Desc {
		impl.cost = outCount * sessVars.DescScanFactor * width
	}
	return impl.cost
}

// IndexReaderImpl is the implementation of PhysicalIndexReader.
type IndexReaderImpl struct {
	baseImpl
	tblDefCausHists *statistics.HistDefCausl
}

// GetCostLimit implements Implementation interface.
func (impl *IndexReaderImpl) GetCostLimit(costLimit float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalIndexReader)
	sessVars := reader.SCtx().GetStochaseinstein_dbars()
	INTERLOCKIterWorkers := float64(sessVars.DistALLEGROSQLScanConcurrency())
	if math.MaxFloat64/INTERLOCKIterWorkers < costLimit {
		return math.MaxFloat64
	}
	return costLimit * INTERLOCKIterWorkers
}

// CalcCost implements Implementation interface.
func (impl *IndexReaderImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	reader := impl.plan.(*plannercore.PhysicalIndexReader)
	sessVars := reader.SCtx().GetStochaseinstein_dbars()
	networkCost := outCount * sessVars.NetworkFactor * impl.tblDefCausHists.GetAvgRowSize(reader.SCtx(), children[0].GetPlan().Schema().DeferredCausets, true, false)
	INTERLOCKIterWorkers := float64(sessVars.DistALLEGROSQLScanConcurrency())
	impl.cost = (networkCost + children[0].GetCost()) / INTERLOCKIterWorkers
	return impl.cost
}

// NewIndexReaderImpl creates a new IndexReader Implementation.
func NewIndexReaderImpl(reader *plannercore.PhysicalIndexReader, tblDefCausHists *statistics.HistDefCausl) *IndexReaderImpl {
	return &IndexReaderImpl{
		baseImpl:        baseImpl{plan: reader},
		tblDefCausHists: tblDefCausHists,
	}
}

// IndexScanImpl is the Implementation of PhysicalIndexScan.
type IndexScanImpl struct {
	baseImpl
	tblDefCausHists *statistics.HistDefCausl
}

// CalcCost implements Implementation interface.
func (impl *IndexScanImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	is := impl.plan.(*plannercore.PhysicalIndexScan)
	sessVars := is.SCtx().GetStochaseinstein_dbars()
	rowSize := impl.tblDefCausHists.GetIndexAvgRowSize(is.SCtx(), is.Schema().DeferredCausets, is.Index.Unique)
	cost := outCount * rowSize * sessVars.ScanFactor
	if is.Desc {
		cost = outCount * rowSize * sessVars.DescScanFactor
	}
	cost += float64(len(is.Ranges)) * sessVars.SeekFactor
	impl.cost = cost
	return impl.cost
}

// NewIndexScanImpl creates a new IndexScan Implementation.
func NewIndexScanImpl(scan *plannercore.PhysicalIndexScan, tblDefCausHists *statistics.HistDefCausl) *IndexScanImpl {
	return &IndexScanImpl{
		baseImpl:        baseImpl{plan: scan},
		tblDefCausHists: tblDefCausHists,
	}
}
