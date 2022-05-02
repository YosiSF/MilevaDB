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
	"unsafe"

	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/expression/aggregation"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/property"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/ranger"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
)

var (
	_ PhysicalPlan = &PhysicalSelection{}
	_ PhysicalPlan = &PhysicalProjection{}
	_ PhysicalPlan = &PhysicalTopN{}
	_ PhysicalPlan = &PhysicalMaxOneRow{}
	_ PhysicalPlan = &PhysicalBlockDual{}
	_ PhysicalPlan = &PhysicalUnionAll{}
	_ PhysicalPlan = &PhysicalSort{}
	_ PhysicalPlan = &NominalSort{}
	_ PhysicalPlan = &PhysicalLock{}
	_ PhysicalPlan = &PhysicalLimit{}
	_ PhysicalPlan = &PhysicalIndexScan{}
	_ PhysicalPlan = &PhysicalBlockScan{}
	_ PhysicalPlan = &PhysicalBlockReader{}
	_ PhysicalPlan = &PhysicalIndexReader{}
	_ PhysicalPlan = &PhysicalIndexLookUpReader{}
	_ PhysicalPlan = &PhysicalIndexMergeReader{}
	_ PhysicalPlan = &PhysicalHashAgg{}
	_ PhysicalPlan = &PhysicalStreamAgg{}
	_ PhysicalPlan = &PhysicalApply{}
	_ PhysicalPlan = &PhysicalIndexJoin{}
	_ PhysicalPlan = &PhysicalBroadCastJoin{}
	_ PhysicalPlan = &PhysicalHashJoin{}
	_ PhysicalPlan = &PhysicalMergeJoin{}
	_ PhysicalPlan = &PhysicalUnionScan{}
	_ PhysicalPlan = &PhysicalWindow{}
	_ PhysicalPlan = &PhysicalShuffle{}
	_ PhysicalPlan = &PhysicalShuffleDataSourceStub{}
	_ PhysicalPlan = &BatchPointGetPlan{}
)

// PhysicalBlockReader is the block reader in milevadb.
type PhysicalBlockReader struct {
	physicalSchemaProducer

	// BlockPlans flats the blockPlan to construct executor pb.
	BlockPlans []PhysicalPlan
	blockPlan  PhysicalPlan

	// StoreType indicates block read from which type of causetstore.
	StoreType ekv.StoreType

	IsCommonHandle bool

	// Used by partition block.
	PartitionInfo PartitionInfo
}

// PartitionInfo indicates partition helper info in physical plan.
type PartitionInfo struct {
	PruningConds        []expression.Expression
	PartitionNames      []perceptron.CIStr
	DeferredCausets     []*expression.DeferredCauset
	DeferredCausetNames types.NameSlice
}

// GetBlockPlan exports the blockPlan.
func (p *PhysicalBlockReader) GetBlockPlan() PhysicalPlan {
	return p.blockPlan
}

// GetBlockScan exports the blockScan that contained in blockPlan.
func (p *PhysicalBlockReader) GetBlockScan() *PhysicalBlockScan {
	curPlan := p.blockPlan
	for {
		chCnt := len(curPlan.Children())
		if chCnt == 0 {
			return curPlan.(*PhysicalBlockScan)
		} else if chCnt == 1 {
			curPlan = curPlan.Children()[0]
		} else {
			join := curPlan.(*PhysicalBroadCastJoin)
			curPlan = join.children[1-join.globalChildIndex]
		}
	}
}

// GetPhysicalBlockReader returns PhysicalBlockReader for logical EinsteinDBSingleGather.
func (sg *EinsteinDBSingleGather) GetPhysicalBlockReader(schemaReplicant *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalBlockReader {
	reader := PhysicalBlockReader{}.Init(sg.ctx, sg.blockOffset)
	reader.PartitionInfo = PartitionInfo{
		PruningConds:        sg.Source.allConds,
		PartitionNames:      sg.Source.partitionNames,
		DeferredCausets:     sg.Source.TblDefCauss,
		DeferredCausetNames: sg.Source.names,
	}
	reader.stats = stats
	reader.SetSchema(schemaReplicant)
	reader.childrenReqProps = props
	return reader
}

// GetPhysicalIndexReader returns PhysicalIndexReader for logical EinsteinDBSingleGather.
func (sg *EinsteinDBSingleGather) GetPhysicalIndexReader(schemaReplicant *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalIndexReader {
	reader := PhysicalIndexReader{}.Init(sg.ctx, sg.blockOffset)
	reader.stats = stats
	reader.SetSchema(schemaReplicant)
	reader.childrenReqProps = props
	return reader
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalBlockReader) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalBlockReader)
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	cloned.StoreType = p.StoreType
	cloned.IsCommonHandle = p.IsCommonHandle
	if cloned.blockPlan, err = p.blockPlan.Clone(); err != nil {
		return nil, err
	}
	if cloned.BlockPlans, err = clonePhysicalPlan(p.BlockPlans); err != nil {
		return nil, err
	}
	return cloned, nil
}

// SetChildren overrides PhysicalPlan SetChildren interface.
func (p *PhysicalBlockReader) SetChildren(children ...PhysicalPlan) {
	p.blockPlan = children[0]
	p.BlockPlans = flattenPushDownPlan(p.blockPlan)
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalBlockReader) ExtractCorrelatedDefCauss() (corDefCauss []*expression.CorrelatedDeferredCauset) {
	for _, child := range p.BlockPlans {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalPlan(child)...)
	}
	return corDefCauss
}

// PhysicalIndexReader is the index reader in milevadb.
type PhysicalIndexReader struct {
	physicalSchemaProducer

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	indexPlan  PhysicalPlan

	// OutputDeferredCausets represents the columns that index reader should return.
	OutputDeferredCausets []*expression.DeferredCauset

	// Used by partition block.
	PartitionInfo PartitionInfo
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalIndexReader) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalIndexReader)
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	if cloned.indexPlan, err = p.indexPlan.Clone(); err != nil {
		return nil, err
	}
	if cloned.IndexPlans, err = clonePhysicalPlan(p.IndexPlans); err != nil {
		return nil, err
	}
	cloned.OutputDeferredCausets = cloneDefCauss(p.OutputDeferredCausets)
	return cloned, err
}

// SetSchema overrides PhysicalPlan SetSchema interface.
func (p *PhysicalIndexReader) SetSchema(_ *expression.Schema) {
	if p.indexPlan != nil {
		p.IndexPlans = flattenPushDownPlan(p.indexPlan)
		switch p.indexPlan.(type) {
		case *PhysicalHashAgg, *PhysicalStreamAgg:
			p.schemaReplicant = p.indexPlan.Schema()
		default:
			is := p.IndexPlans[0].(*PhysicalIndexScan)
			p.schemaReplicant = is.dataSourceSchema
		}
		p.OutputDeferredCausets = p.schemaReplicant.Clone().DeferredCausets
	}
}

// SetChildren overrides PhysicalPlan SetChildren interface.
func (p *PhysicalIndexReader) SetChildren(children ...PhysicalPlan) {
	p.indexPlan = children[0]
	p.SetSchema(nil)
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalIndexReader) ExtractCorrelatedDefCauss() (corDefCauss []*expression.CorrelatedDeferredCauset) {
	for _, child := range p.IndexPlans {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalPlan(child)...)
	}
	return corDefCauss
}

// PushedDownLimit is the limit operator pushed down into PhysicalIndexLookUpReader.
type PushedDownLimit struct {
	Offset uint64
	Count  uint64
}

// Clone clones this pushed-down list.
func (p *PushedDownLimit) Clone() *PushedDownLimit {
	cloned := new(PushedDownLimit)
	*cloned = *p
	return cloned
}

// PhysicalIndexLookUpReader is the index look up reader in milevadb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	physicalSchemaProducer

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	// BlockPlans flats the blockPlan to construct executor pb.
	BlockPlans []PhysicalPlan
	indexPlan  PhysicalPlan
	blockPlan  PhysicalPlan

	ExtraHandleDefCaus *expression.DeferredCauset
	// PushedLimit is used to avoid unnecessary block scan tasks of IndexLookUpReader.
	PushedLimit *PushedDownLimit

	CommonHandleDefCauss []*expression.DeferredCauset

	// Used by partition block.
	PartitionInfo PartitionInfo
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalIndexLookUpReader)
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	if cloned.IndexPlans, err = clonePhysicalPlan(p.IndexPlans); err != nil {
		return nil, err
	}
	if cloned.BlockPlans, err = clonePhysicalPlan(p.BlockPlans); err != nil {
		return nil, err
	}
	if cloned.indexPlan, err = p.indexPlan.Clone(); err != nil {
		return nil, err
	}
	if cloned.blockPlan, err = p.blockPlan.Clone(); err != nil {
		return nil, err
	}
	if p.ExtraHandleDefCaus != nil {
		cloned.ExtraHandleDefCaus = p.ExtraHandleDefCaus.Clone().(*expression.DeferredCauset)
	}
	if p.PushedLimit != nil {
		cloned.PushedLimit = p.PushedLimit.Clone()
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) ExtractCorrelatedDefCauss() (corDefCauss []*expression.CorrelatedDeferredCauset) {
	for _, child := range p.BlockPlans {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalPlan(child)...)
	}
	for _, child := range p.IndexPlans {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalPlan(child)...)
	}
	return corDefCauss
}

// PhysicalIndexMergeReader is the reader using multiple indexes in milevadb.
type PhysicalIndexMergeReader struct {
	physicalSchemaProducer

	// PartialPlans flats the partialPlans to construct executor pb.
	PartialPlans [][]PhysicalPlan
	// BlockPlans flats the blockPlan to construct executor pb.
	BlockPlans []PhysicalPlan
	// partialPlans are the partial plans that have not been flatted. The type of each element is permitted PhysicalIndexScan or PhysicalBlockScan.
	partialPlans []PhysicalPlan
	// blockPlan is a PhysicalBlockScan to get the block tuples. Current, it must be not nil.
	blockPlan PhysicalPlan

	// Used by partition block.
	PartitionInfo PartitionInfo
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalIndexMergeReader) ExtractCorrelatedDefCauss() (corDefCauss []*expression.CorrelatedDeferredCauset) {
	for _, child := range p.BlockPlans {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalPlan(child)...)
	}
	for _, child := range p.partialPlans {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalPlan(child)...)
	}
	for _, PartialPlan := range p.PartialPlans {
		for _, child := range PartialPlan {
			corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalPlan(child)...)
		}
	}
	return corDefCauss
}

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	physicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression

	Block           *perceptron.BlockInfo
	Index           *perceptron.IndexInfo
	IdxDefCauss     []*expression.DeferredCauset
	IdxDefCausLens  []int
	Ranges          []*ranger.Range
	DeferredCausets []*perceptron.DeferredCausetInfo
	DBName          perceptron.CIStr

	BlockAsName *perceptron.CIStr

	// dataSourceSchema is the original schemaReplicant of DataSource. The schemaReplicant of index scan in KV and index reader in MilevaDB
	// will be different. The schemaReplicant of index scan will decode all columns of index but the MilevaDB only need some of them.
	dataSourceSchema *expression.Schema

	// Hist is the histogram when the query was issued.
	// It is used for query feedback.
	Hist *statistics.Histogram

	rangeInfo string

	// The index scan may be on a partition.
	physicalBlockID int64

	GenExprs map[perceptron.BlockDeferredCausetID]expression.Expression

	isPartition bool
	Desc        bool
	KeepOrder   bool
	// DoubleRead means if the index executor will read ekv two times.
	// If the query requires the columns that don't belong to index, DoubleRead will be true.
	DoubleRead bool

	NeedCommonHandle bool
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalIndexScan) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalIndexScan)
	*cloned = *p
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	cloned.AccessCondition = cloneExprs(p.AccessCondition)
	if p.Block != nil {
		cloned.Block = p.Block.Clone()
	}
	if p.Index != nil {
		cloned.Index = p.Index.Clone()
	}
	cloned.IdxDefCauss = cloneDefCauss(p.IdxDefCauss)
	cloned.IdxDefCausLens = make([]int, len(p.IdxDefCausLens))
	INTERLOCKy(cloned.IdxDefCausLens, p.IdxDefCausLens)
	cloned.Ranges = cloneRanges(p.Ranges)
	cloned.DeferredCausets = cloneDefCausInfos(p.DeferredCausets)
	if p.dataSourceSchema != nil {
		cloned.dataSourceSchema = p.dataSourceSchema.Clone()
	}
	if p.Hist != nil {
		cloned.Hist = p.Hist.INTERLOCKy()
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalIndexScan) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(p.AccessCondition))
	for _, expr := range p.AccessCondition {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(expr)...)
	}
	return corDefCauss
}

// PhysicalMemBlock reads memory block.
type PhysicalMemBlock struct {
	physicalSchemaProducer

	DBName          perceptron.CIStr
	Block           *perceptron.BlockInfo
	DeferredCausets []*perceptron.DeferredCausetInfo
	Extractor       MemBlockPredicateExtractor
	QueryTimeRange  QueryTimeRange
}

// PhysicalBlockScan represents a block scan plan.
type PhysicalBlockScan struct {
	physicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression
	filterCondition []expression.Expression

	Block           *perceptron.BlockInfo
	DeferredCausets []*perceptron.DeferredCausetInfo
	DBName          perceptron.CIStr
	Ranges          []*ranger.Range
	PkDefCauss      []*expression.DeferredCauset

	BlockAsName *perceptron.CIStr

	// Hist is the histogram when the query was issued.
	// It is used for query feedback.
	Hist *statistics.Histogram

	physicalBlockID int64

	rangeDecidedBy []*expression.DeferredCauset

	// HandleIdx is the index of handle, which is only used for admin check block.
	HandleIdx      []int
	HandleDefCauss HandleDefCauss

	StoreType ekv.StoreType

	IsGlobalRead bool

	// The block scan may be a partition, rather than a real block.
	isPartition bool
	// KeepOrder is true, if sort data by scanning pkcol,
	KeepOrder bool
	Desc      bool

	isChildOfIndexLookUp bool
}

// Clone implements PhysicalPlan interface.
func (ts *PhysicalBlockScan) Clone() (PhysicalPlan, error) {
	clonedScan := new(PhysicalBlockScan)
	*clonedScan = *ts
	prod, err := ts.physicalSchemaProducer.cloneWithSelf(clonedScan)
	if err != nil {
		return nil, err
	}
	clonedScan.physicalSchemaProducer = *prod
	clonedScan.AccessCondition = cloneExprs(ts.AccessCondition)
	clonedScan.filterCondition = cloneExprs(ts.filterCondition)
	if ts.Block != nil {
		clonedScan.Block = ts.Block.Clone()
	}
	clonedScan.DeferredCausets = cloneDefCausInfos(ts.DeferredCausets)
	clonedScan.Ranges = cloneRanges(ts.Ranges)
	clonedScan.PkDefCauss = cloneDefCauss(ts.PkDefCauss)
	clonedScan.BlockAsName = ts.BlockAsName
	if ts.Hist != nil {
		clonedScan.Hist = ts.Hist.INTERLOCKy()
	}
	clonedScan.rangeDecidedBy = cloneDefCauss(ts.rangeDecidedBy)
	return clonedScan, nil
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (ts *PhysicalBlockScan) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(ts.AccessCondition)+len(ts.filterCondition))
	for _, expr := range ts.AccessCondition {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(expr)...)
	}
	for _, expr := range ts.filterCondition {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(expr)...)
	}
	return corDefCauss
}

// IsPartition returns true and partition ID if it's actually a partition.
func (ts *PhysicalBlockScan) IsPartition() (bool, int64) {
	return ts.isPartition, ts.physicalBlockID
}

// ExpandVirtualDeferredCauset expands the virtual column's dependent columns to ts's schemaReplicant and column.
func ExpandVirtualDeferredCauset(columns []*perceptron.DeferredCausetInfo, schemaReplicant *expression.Schema,
	defcausInfo []*perceptron.DeferredCausetInfo) []*perceptron.DeferredCausetInfo {
	INTERLOCKyDeferredCauset := make([]*perceptron.DeferredCausetInfo, len(columns))
	INTERLOCKy(INTERLOCKyDeferredCauset, columns)
	var extraDeferredCauset *expression.DeferredCauset
	var extraDeferredCausetPerceptron *perceptron.DeferredCausetInfo
	if schemaReplicant.DeferredCausets[len(schemaReplicant.DeferredCausets)-1].ID == perceptron.ExtraHandleID {
		extraDeferredCauset = schemaReplicant.DeferredCausets[len(schemaReplicant.DeferredCausets)-1]
		extraDeferredCausetPerceptron = INTERLOCKyDeferredCauset[len(INTERLOCKyDeferredCauset)-1]
		schemaReplicant.DeferredCausets = schemaReplicant.DeferredCausets[:len(schemaReplicant.DeferredCausets)-1]
		INTERLOCKyDeferredCauset = INTERLOCKyDeferredCauset[:len(INTERLOCKyDeferredCauset)-1]
	}
	schemaDeferredCausets := schemaReplicant.DeferredCausets
	for _, col := range schemaDeferredCausets {
		if col.VirtualExpr == nil {
			continue
		}

		baseDefCauss := expression.ExtractDependentDeferredCausets(col.VirtualExpr)
		for _, baseDefCaus := range baseDefCauss {
			if !schemaReplicant.Contains(baseDefCaus) {
				schemaReplicant.DeferredCausets = append(schemaReplicant.DeferredCausets, baseDefCaus)
				INTERLOCKyDeferredCauset = append(INTERLOCKyDeferredCauset, FindDeferredCausetInfoByID(defcausInfo, baseDefCaus.ID))
			}
		}
	}
	if extraDeferredCauset != nil {
		schemaReplicant.DeferredCausets = append(schemaReplicant.DeferredCausets, extraDeferredCauset)
		INTERLOCKyDeferredCauset = append(INTERLOCKyDeferredCauset, extraDeferredCausetPerceptron)
	}
	return INTERLOCKyDeferredCauset
}

//SetIsChildOfIndexLookUp is to set the bool if is a child of IndexLookUpReader
func (ts *PhysicalBlockScan) SetIsChildOfIndexLookUp(isIsChildOfIndexLookUp bool) {
	ts.isChildOfIndexLookUp = isIsChildOfIndexLookUp
}

// PhysicalProjection is the physical operator of projection.
type PhysicalProjection struct {
	physicalSchemaProducer

	Exprs                        []expression.Expression
	CalculateNoDelay             bool
	AvoidDeferredCausetEvaluator bool
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalProjection) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalProjection)
	*cloned = *p
	base, err := p.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	cloned.Exprs = cloneExprs(p.Exprs)
	return cloned, err
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalProjection) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(p.Exprs))
	for _, expr := range p.Exprs {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(expr)...)
	}
	return corDefCauss
}

// PhysicalTopN is the physical operator of topN.
type PhysicalTopN struct {
	basePhysicalPlan

	ByItems []*soliton.ByItems
	Offset  uint64
	Count   uint64
}

// Clone implements PhysicalPlan interface.
func (lt *PhysicalTopN) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalTopN)
	*cloned = *lt
	base, err := lt.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	cloned.ByItems = make([]*soliton.ByItems, 0, len(lt.ByItems))
	for _, it := range lt.ByItems {
		cloned.ByItems = append(cloned.ByItems, it.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (lt *PhysicalTopN) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(lt.ByItems))
	for _, item := range lt.ByItems {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(item.Expr)...)
	}
	return corDefCauss
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	PhysicalHashJoin

	CanUseCache bool
	Concurrency int
	OuterSchema []*expression.CorrelatedDeferredCauset
}

// Clone implements PhysicalPlan interface.
func (la *PhysicalApply) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalApply)
	base, err := la.PhysicalHashJoin.Clone()
	if err != nil {
		return nil, err
	}
	hj := base.(*PhysicalHashJoin)
	cloned.PhysicalHashJoin = *hj
	cloned.CanUseCache = la.CanUseCache
	cloned.Concurrency = la.Concurrency
	for _, col := range la.OuterSchema {
		cloned.OuterSchema = append(cloned.OuterSchema, col.Clone().(*expression.CorrelatedDeferredCauset))
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (la *PhysicalApply) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := la.PhysicalHashJoin.ExtractCorrelatedDefCauss()
	for i := len(corDefCauss) - 1; i >= 0; i-- {
		if la.children[0].Schema().Contains(&corDefCauss[i].DeferredCauset) {
			corDefCauss = append(corDefCauss[:i], corDefCauss[i+1:]...)
		}
	}
	return corDefCauss
}

type basePhysicalJoin struct {
	physicalSchemaProducer

	JoinType JoinType

	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	InnerChildIdx int
	OuterJoinKeys []*expression.DeferredCauset
	InnerJoinKeys []*expression.DeferredCauset
	LeftJoinKeys  []*expression.DeferredCauset
	RightJoinKeys []*expression.DeferredCauset
	IsNullEQ      []bool
	DefaultValues []types.Causet
}

func (p *basePhysicalJoin) cloneWithSelf(newSelf PhysicalPlan) (*basePhysicalJoin, error) {
	cloned := new(basePhysicalJoin)
	base, err := p.physicalSchemaProducer.cloneWithSelf(newSelf)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	cloned.JoinType = p.JoinType
	cloned.LeftConditions = cloneExprs(p.LeftConditions)
	cloned.RightConditions = cloneExprs(p.RightConditions)
	cloned.OtherConditions = cloneExprs(p.OtherConditions)
	cloned.InnerChildIdx = p.InnerChildIdx
	cloned.OuterJoinKeys = cloneDefCauss(p.OuterJoinKeys)
	cloned.InnerJoinKeys = cloneDefCauss(p.InnerJoinKeys)
	cloned.LeftJoinKeys = cloneDefCauss(p.LeftJoinKeys)
	cloned.RightJoinKeys = cloneDefCauss(p.RightJoinKeys)
	for _, d := range p.DefaultValues {
		cloned.DefaultValues = append(cloned.DefaultValues, *d.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *basePhysicalJoin) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.LeftConditions {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.RightConditions {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(fun)...)
	}
	return corDefCauss
}

// PhysicalHashJoin represents hash join implementation of LogicalJoin.
type PhysicalHashJoin struct {
	basePhysicalJoin

	Concurrency     uint
	EqualConditions []*expression.ScalarFunction

	// use the outer block to build a hash block when the outer block is smaller.
	UseOuterToBuild bool
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalHashJoin) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalHashJoin)
	base, err := p.basePhysicalJoin.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalJoin = *base
	cloned.Concurrency = p.Concurrency
	cloned.UseOuterToBuild = p.UseOuterToBuild
	for _, c := range p.EqualConditions {
		cloned.EqualConditions = append(cloned.EqualConditions, c.Clone().(*expression.ScalarFunction))
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalHashJoin) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.EqualConditions {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.RightConditions {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(fun)...)
	}
	return corDefCauss
}

// NewPhysicalHashJoin creates a new PhysicalHashJoin from LogicalJoin.
func NewPhysicalHashJoin(p *LogicalJoin, innerIdx int, useOuterToBuild bool, newStats *property.StatsInfo, prop ...*property.PhysicalProperty) *PhysicalHashJoin {
	leftJoinKeys, rightJoinKeys, isNullEQ, _ := p.GetJoinKeys()
	baseJoin := basePhysicalJoin{
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		LeftJoinKeys:    leftJoinKeys,
		RightJoinKeys:   rightJoinKeys,
		IsNullEQ:        isNullEQ,
		JoinType:        p.JoinType,
		DefaultValues:   p.DefaultValues,
		InnerChildIdx:   innerIdx,
	}
	hashJoin := PhysicalHashJoin{
		basePhysicalJoin: baseJoin,
		EqualConditions:  p.EqualConditions,
		Concurrency:      uint(p.ctx.GetStochaseinstein_dbars().HashJoinConcurrency()),
		UseOuterToBuild:  useOuterToBuild,
	}.Init(p.ctx, newStats, p.blockOffset, prop...)
	return hashJoin
}

// PhysicalIndexJoin represents the plan of index look up join.
type PhysicalIndexJoin struct {
	basePhysicalJoin

	outerSchema *expression.Schema
	innerTask   task

	// Ranges stores the IndexRanges when the inner plan is index scan.
	Ranges []*ranger.Range
	// KeyOff2IdxOff maps the offsets in join key to the offsets in the index.
	KeyOff2IdxOff []int
	// IdxDefCausLens stores the length of each index column.
	IdxDefCausLens []int
	// CompareFilters stores the filters for last column if those filters need to be evaluated during execution.
	// e.g. select * from t, t1 where t.a = t1.a and t.b > t1.b and t.b < t1.b+10
	//      If there's index(t.a, t.b). All the filters can be used to construct index range but t.b > t1.b and t.b < t1.b=10
	//      need to be evaluated after we fetch the data of t1.
	// This struct stores them and evaluate them to ranges.
	CompareFilters *DefCausWithCmpFuncManager
}

// PhysicalIndexMergeJoin represents the plan of index look up merge join.
type PhysicalIndexMergeJoin struct {
	PhysicalIndexJoin

	// KeyOff2KeyOffOrderByIdx maps the offsets in join keys to the offsets in join keys order by index.
	KeyOff2KeyOffOrderByIdx []int
	// CompareFuncs causetstore the compare functions for outer join keys and inner join key.
	CompareFuncs []expression.CompareFunc
	// OuterCompareFuncs causetstore the compare functions for outer join keys and outer join
	// keys, it's for outer rows sort's convenience.
	OuterCompareFuncs []expression.CompareFunc
	// NeedOuterSort means whether outer rows should be sorted to build range.
	NeedOuterSort bool
	// Desc means whether inner child keep desc order.
	Desc bool
}

// PhysicalIndexHashJoin represents the plan of index look up hash join.
type PhysicalIndexHashJoin struct {
	PhysicalIndexJoin
	// KeepOuterOrder indicates whether keeping the output result order as the
	// outer side.
	KeepOuterOrder bool
}

// PhysicalMergeJoin represents merge join implementation of LogicalJoin.
type PhysicalMergeJoin struct {
	basePhysicalJoin

	CompareFuncs []expression.CompareFunc
	// Desc means whether inner child keep desc order.
	Desc bool
}

// PhysicalBroadCastJoin only works for TiFlash Engine, which broadcast the small block to every replica of probe side of blocks.
type PhysicalBroadCastJoin struct {
	basePhysicalJoin
	globalChildIndex int
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalMergeJoin) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalMergeJoin)
	base, err := p.basePhysicalJoin.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalJoin = *base
	for _, cf := range p.CompareFuncs {
		cloned.CompareFuncs = append(cloned.CompareFuncs, cf)
	}
	cloned.Desc = p.Desc
	return cloned, nil
}

// PhysicalLock is the physical operator of dagger, which is used for `select ... for uFIDelate` clause.
type PhysicalLock struct {
	basePhysicalPlan

	Lock *ast.SelectLockInfo

	TblID2Handle     map[int64][]HandleDefCauss
	PartitionedBlock []block.PartitionedBlock
}

// PhysicalLimit is the physical operator of Limit.
type PhysicalLimit struct {
	basePhysicalPlan

	Offset uint64
	Count  uint64
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalLimit) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalLimit)
	*cloned = *p
	base, err := p.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	return cloned, nil
}

// PhysicalUnionAll is the physical operator of UnionAll.
type PhysicalUnionAll struct {
	physicalSchemaProducer
}

type basePhysicalAgg struct {
	physicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
}

func (p *basePhysicalAgg) cloneWithSelf(newSelf PhysicalPlan) (*basePhysicalAgg, error) {
	cloned := new(basePhysicalAgg)
	base, err := p.physicalSchemaProducer.cloneWithSelf(newSelf)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	for _, aggDesc := range p.AggFuncs {
		cloned.AggFuncs = append(cloned.AggFuncs, aggDesc.Clone())
	}
	cloned.GroupByItems = cloneExprs(p.GroupByItems)
	return cloned, nil
}

func (p *basePhysicalAgg) numDistinctFunc() (num int) {
	for _, fun := range p.AggFuncs {
		if fun.HasDistinct {
			num++
		}
	}
	return
}

func (p *basePhysicalAgg) getAggFuncCostFactor() (factor float64) {
	factor = 0.0
	for _, agg := range p.AggFuncs {
		if fac, ok := aggFuncFactor[agg.Name]; ok {
			factor += fac
		} else {
			factor += aggFuncFactor["default"]
		}
	}
	if factor == 0 {
		factor = 1.0
	}
	return
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *basePhysicalAgg) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(p.GroupByItems)+len(p.AggFuncs))
	for _, expr := range p.GroupByItems {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(expr)...)
	}
	for _, fun := range p.AggFuncs {
		for _, arg := range fun.Args {
			corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(arg)...)
		}
	}
	return corDefCauss
}

// PhysicalHashAgg is hash operator of aggregate.
type PhysicalHashAgg struct {
	basePhysicalAgg
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalHashAgg) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalHashAgg)
	base, err := p.basePhysicalAgg.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalAgg = *base
	return cloned, nil
}

// NewPhysicalHashAgg creates a new PhysicalHashAgg from a LogicalAggregation.
func NewPhysicalHashAgg(la *LogicalAggregation, newStats *property.StatsInfo, prop *property.PhysicalProperty) *PhysicalHashAgg {
	agg := basePhysicalAgg{
		GroupByItems: la.GroupByItems,
		AggFuncs:     la.AggFuncs,
	}.initForHash(la.ctx, newStats, la.blockOffset, prop)
	return agg
}

// PhysicalStreamAgg is stream operator of aggregate.
type PhysicalStreamAgg struct {
	basePhysicalAgg
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalStreamAgg) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalStreamAgg)
	base, err := p.basePhysicalAgg.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalAgg = *base
	return cloned, nil
}

// PhysicalSort is the physical operator of sort, which implements a memory sort.
type PhysicalSort struct {
	basePhysicalPlan

	ByItems []*soliton.ByItems
}

// Clone implements PhysicalPlan interface.
func (ls *PhysicalSort) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalSort)
	base, err := ls.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	for _, it := range ls.ByItems {
		cloned.ByItems = append(cloned.ByItems, it.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (ls *PhysicalSort) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(ls.ByItems))
	for _, item := range ls.ByItems {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(item.Expr)...)
	}
	return corDefCauss
}

// NominalSort asks sort properties for its child. It is a fake operator that will not
// appear in final physical operator tree. It will be eliminated or converted to Projection.
type NominalSort struct {
	basePhysicalPlan

	// These two fields are used to switch ScalarFunctions to CouplingConstantWithRadixs. For these
	// NominalSorts, we need to converted to Projections check if the ScalarFunctions
	// are out of bounds. (issue #11653)
	ByItems            []*soliton.ByItems
	OnlyDeferredCauset bool
}

// PhysicalUnionScan represents a union scan operator.
type PhysicalUnionScan struct {
	basePhysicalPlan

	Conditions []expression.Expression

	HandleDefCauss HandleDefCauss
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalUnionScan) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0)
	for _, cond := range p.Conditions {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(cond)...)
	}
	return corDefCauss
}

// IsPartition returns true and partition ID if it works on a partition.
func (p *PhysicalIndexScan) IsPartition() (bool, int64) {
	return p.isPartition, p.physicalBlockID
}

// IsPointGetByUniqueKey checks whether is a point get by unique key.
func (p *PhysicalIndexScan) IsPointGetByUniqueKey(sc *stmtctx.StatementContext) bool {
	return len(p.Ranges) == 1 &&
		p.Index.Unique &&
		len(p.Ranges[0].LowVal) == len(p.Index.DeferredCausets) &&
		p.Ranges[0].IsPoint(sc)
}

// PhysicalSelection represents a filter.
type PhysicalSelection struct {
	basePhysicalPlan

	Conditions []expression.Expression
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalSelection) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalSelection)
	base, err := p.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	cloned.Conditions = cloneExprs(p.Conditions)
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalSelection) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(p.Conditions))
	for _, cond := range p.Conditions {
		corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(cond)...)
	}
	return corDefCauss
}

// PhysicalMaxOneRow is the physical operator of maxOneRow.
type PhysicalMaxOneRow struct {
	basePhysicalPlan
}

// PhysicalBlockDual is the physical operator of dual.
type PhysicalBlockDual struct {
	physicalSchemaProducer

	RowCount int

	// names is used for OutputNames() method. Dual may be inited when building point get plan.
	// So it needs to hold names for itself.
	names []*types.FieldName
}

// OutputNames returns the outputting names of each column.
func (p *PhysicalBlockDual) OutputNames() types.NameSlice {
	return p.names
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PhysicalBlockDual) SetOutputNames(names types.NameSlice) {
	p.names = names
}

// PhysicalWindow is the physical operator of window function.
type PhysicalWindow struct {
	physicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.Item
	OrderBy         []property.Item
	Frame           *WindowFrame
}

// ExtractCorrelatedDefCauss implements PhysicalPlan interface.
func (p *PhysicalWindow) ExtractCorrelatedDefCauss() []*expression.CorrelatedDeferredCauset {
	corDefCauss := make([]*expression.CorrelatedDeferredCauset, 0, len(p.WindowFuncDescs))
	for _, windowFunc := range p.WindowFuncDescs {
		for _, arg := range windowFunc.Args {
			corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(arg)...)
		}
	}
	if p.Frame != nil {
		if p.Frame.Start != nil {
			for _, expr := range p.Frame.Start.CalcFuncs {
				corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(expr)...)
			}
		}
		if p.Frame.End != nil {
			for _, expr := range p.Frame.End.CalcFuncs {
				corDefCauss = append(corDefCauss, expression.ExtractCorDeferredCausets(expr)...)
			}
		}
	}
	return corDefCauss
}

// PhysicalShuffle represents a shuffle plan.
// `Tail` and `DataSource` are the last plan within and the first plan following the "shuffle", respectively,
//  to build the child executors chain.
// Take `Window` operator for example:
//  Shuffle -> Window -> Sort -> DataSource, will be separated into:
//    ==> Shuffle: for main thread
//    ==> Window -> Sort(:Tail) -> shuffleWorker: for workers
//    ==> DataSource: for `fetchDataAndSplit` thread
type PhysicalShuffle struct {
	basePhysicalPlan

	Concurrency int
	Tail        PhysicalPlan
	DataSource  PhysicalPlan

	SplitterType PartitionSplitterType
	HashByItems  []expression.Expression
}

// PartitionSplitterType is the type of `Shuffle` executor splitter, which splits data source into partitions.
type PartitionSplitterType int

const (
	// PartitionHashSplitterType is the splitter splits by hash.
	PartitionHashSplitterType = iota
)

// PhysicalShuffleDataSourceStub represents a data source stub of `PhysicalShuffle`,
// and actually, is executed by `executor.shuffleWorker`.
type PhysicalShuffleDataSourceStub struct {
	physicalSchemaProducer

	// Worker points to `executor.shuffleWorker`.
	Worker unsafe.Pointer
}

// DefCauslectPlanStatsVersion uses to collect the statistics version of the plan.
func DefCauslectPlanStatsVersion(plan PhysicalPlan, statsInfos map[string]uint64) map[string]uint64 {
	for _, child := range plan.Children() {
		statsInfos = DefCauslectPlanStatsVersion(child, statsInfos)
	}
	switch INTERLOCKPlan := plan.(type) {
	case *PhysicalBlockReader:
		statsInfos = DefCauslectPlanStatsVersion(INTERLOCKPlan.blockPlan, statsInfos)
	case *PhysicalIndexReader:
		statsInfos = DefCauslectPlanStatsVersion(INTERLOCKPlan.indexPlan, statsInfos)
	case *PhysicalIndexLookUpReader:
		// For index loop up, only the indexPlan is necessary,
		// because they use the same stats and we do not set the stats info for blockPlan.
		statsInfos = DefCauslectPlanStatsVersion(INTERLOCKPlan.indexPlan, statsInfos)
	case *PhysicalIndexScan:
		statsInfos[INTERLOCKPlan.Block.Name.O] = INTERLOCKPlan.stats.StatsVersion
	case *PhysicalBlockScan:
		statsInfos[INTERLOCKPlan.Block.Name.O] = INTERLOCKPlan.stats.StatsVersion
	}

	return statsInfos
}

// PhysicalShow represents a show plan.
type PhysicalShow struct {
	physicalSchemaProducer

	ShowContents
}

// PhysicalShowDBSJobs is for showing DBS job list.
type PhysicalShowDBSJobs struct {
	physicalSchemaProducer

	JobNumber int64
}

// BuildMergeJoinPlan builds a PhysicalMergeJoin from the given fields. Currently, it is only used for test purpose.
func BuildMergeJoinPlan(ctx stochastikctx.Context, joinType JoinType, leftKeys, rightKeys []*expression.DeferredCauset) *PhysicalMergeJoin {
	baseJoin := basePhysicalJoin{
		JoinType:      joinType,
		DefaultValues: []types.Causet{types.NewCauset(1), types.NewCauset(1)},
		LeftJoinKeys:  leftKeys,
		RightJoinKeys: rightKeys,
	}
	return PhysicalMergeJoin{basePhysicalJoin: baseJoin}.Init(ctx, nil, 0)
}

// SafeClone clones this PhysicalPlan and handles its panic.
func SafeClone(v PhysicalPlan) (_ PhysicalPlan, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("%v", r)
		}
	}()
	return v.Clone()
}
