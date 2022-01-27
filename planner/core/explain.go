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

package core

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// A plan is dataAccesser means it can access underlying data.
// Include `PhysicalBlockScan`, `PhysicalIndexScan`, `PointGetPlan`, `BatchPointScan` and `PhysicalMemBlock`.
// ExplainInfo = AccessObject + OperatorInfo
type dataAccesser interface {

	// AccessObject return plan's `block`, `partition` and `index`.
	AccessObject() string

	// OperatorInfo return other operator information to be explained.
	OperatorInfo(normalized bool) string
}

type partitionAccesser interface {
	accessObject(stochastikctx.Context) string
}

// ExplainInfo implements Plan interface.
func (p *PhysicalLock) ExplainInfo() string {
	return fmt.Sprintf("%s %v", p.Lock.LockType.String(), p.Lock.WaitSec)
}

// ExplainID overrides the ExplainID in order to match different range.
func (p *PhysicalIndexScan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.isFullScan() {
			return "IndexFullScan_" + strconv.Itoa(p.id)
		}
		return "IndexRangeScan_" + strconv.Itoa(p.id)
	})
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexScan) ExplainNormalizedInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(true)
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalIndexScan) AccessObject() string {
	buffer := bytes.NewBufferString("")
	tblName := p.Block.Name.O
	if p.BlockAsName != nil && p.BlockAsName.O != "" {
		tblName = p.BlockAsName.O
	}
	fmt.Fprintf(buffer, "block:%s", tblName)
	if p.isPartition {
		if pi := p.Block.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalBlockID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	if len(p.Index.DeferredCausets) > 0 {
		buffer.WriteString(", index:" + p.Index.Name.O + "(")
		for i, idxDefCaus := range p.Index.DeferredCausets {
			buffer.WriteString(idxDefCaus.Name.O)
			if i+1 < len(p.Index.DeferredCausets) {
				buffer.WriteString(", ")
			}
		}
		buffer.WriteString(")")
	}
	return buffer.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalIndexScan) OperatorInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	if len(p.rangeInfo) > 0 {
		if !normalized {
			fmt.Fprintf(buffer, "range: decided by %v, ", p.rangeInfo)
		}
	} else if p.haveCorDefCaus() {
		if normalized {
			fmt.Fprintf(buffer, "range: decided by %s, ", expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
		} else {
			fmt.Fprintf(buffer, "range: decided by %v, ", p.AccessCondition)
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			fmt.Fprint(buffer, "range:[?,?], ")
		} else if !p.isFullScan() {
			fmt.Fprint(buffer, "range:")
			for _, idxRange := range p.Ranges {
				fmt.Fprint(buffer, idxRange.String()+", ")
			}
		}
	}
	fmt.Fprintf(buffer, "keep order:%v, ", p.KeepOrder)
	if p.Desc {
		buffer.WriteString("desc, ")
	}
	if p.stats.StatsVersion == statistics.PseudoVersion && !normalized {
		buffer.WriteString("stats:pseudo, ")
	}
	buffer.Truncate(buffer.Len() - 2)
	return buffer.String()
}

func (p *PhysicalIndexScan) haveCorDefCaus() bool {
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorDeferredCausets(cond)) > 0 {
			return true
		}
	}
	return false
}

func (p *PhysicalIndexScan) isFullScan() bool {
	if len(p.rangeInfo) > 0 || p.haveCorDefCaus() {
		return false
	}
	for _, ran := range p.Ranges {
		if !ran.IsFullRange() {
			return false
		}
	}
	return true
}

// ExplainID overrides the ExplainID in order to match different range.
func (p *PhysicalBlockScan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.isChildOfIndexLookUp {
			return "BlockRowIDScan_" + strconv.Itoa(p.id)
		} else if p.isFullScan() {
			return "BlockFullScan_" + strconv.Itoa(p.id)
		}
		return "BlockRangeScan_" + strconv.Itoa(p.id)
	})
}

// ExplainInfo implements Plan interface.
func (p *PhysicalBlockScan) ExplainInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalBlockScan) ExplainNormalizedInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(true)
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalBlockScan) AccessObject() string {
	buffer := bytes.NewBufferString("")
	tblName := p.Block.Name.O
	if p.BlockAsName != nil && p.BlockAsName.O != "" {
		tblName = p.BlockAsName.O
	}
	fmt.Fprintf(buffer, "block:%s", tblName)
	if p.isPartition {
		if pi := p.Block.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalBlockID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	return buffer.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalBlockScan) OperatorInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	for i, pkDefCaus := range p.PkDefCauss {
		var fmtStr string
		switch i {
		case 0:
			fmtStr = "pk defcaus: (%s, "
		case len(p.PkDefCauss) - 1:
			fmtStr = "%s)"
		default:
			fmtStr = "%s, "
		}
		fmt.Fprintf(buffer, fmtStr, pkDefCaus.ExplainInfo())
	}
	if len(p.rangeDecidedBy) > 0 {
		fmt.Fprintf(buffer, "range: decided by %v, ", p.rangeDecidedBy)
	} else if p.haveCorDefCaus() {
		if normalized {
			fmt.Fprintf(buffer, "range: decided by %s, ", expression.SortedExplainNormalizedExpressionList(p.AccessCondition))
		} else {
			fmt.Fprintf(buffer, "range: decided by %v, ", p.AccessCondition)
		}
	} else if len(p.Ranges) > 0 {
		if normalized {
			fmt.Fprint(buffer, "range:[?,?], ")
		} else if !p.isFullScan() {
			fmt.Fprint(buffer, "range:")
			for _, idxRange := range p.Ranges {
				fmt.Fprint(buffer, idxRange.String()+", ")
			}
		}
	}
	fmt.Fprintf(buffer, "keep order:%v, ", p.KeepOrder)
	if p.Desc {
		buffer.WriteString("desc, ")
	}
	if p.stats.StatsVersion == statistics.PseudoVersion && !normalized {
		buffer.WriteString("stats:pseudo, ")
	}
	if p.IsGlobalRead {
		buffer.WriteString("global read, ")
	}
	buffer.Truncate(buffer.Len() - 2)
	return buffer.String()
}

func (p *PhysicalBlockScan) haveCorDefCaus() bool {
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorDeferredCausets(cond)) > 0 {
			return true
		}
	}
	return false
}

func (p *PhysicalBlockScan) isFullScan() bool {
	if len(p.rangeDecidedBy) > 0 || p.haveCorDefCaus() {
		return false
	}
	for _, ran := range p.Ranges {
		if !ran.IsFullRange() {
			return false
		}
	}
	return true
}

// ExplainInfo implements Plan interface.
func (p *PhysicalBlockReader) ExplainInfo() string {
	return "data:" + p.blockPlan.ExplainID().String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalBlockReader) ExplainNormalizedInfo() string {
	return ""
}

func (p *PhysicalBlockReader) accessObject(sctx stochastikctx.Context) string {
	ts := p.BlockPlans[0].(*PhysicalBlockScan)
	pi := ts.Block.GetPartitionInfo()
	if pi == nil || !sctx.GetStochastikVars().UseDynamicPartitionPrune() {
		return ""
	}

	is := schemareplicant.GetSchemaReplicant(sctx)
	tmp, ok := is.BlockByID(ts.Block.ID)
	if !ok {
		return "partition block not found" + strconv.FormatInt(ts.Block.ID, 10)
	}
	tbl := tmp.(block.PartitionedBlock)

	return partitionAccessObject(sctx, tbl, pi, &p.PartitionInfo)
}

func partitionAccessObject(sctx stochastikctx.Context, tbl block.PartitionedBlock, pi *perceptron.PartitionInfo, partBlock *PartitionInfo) string {
	var buffer bytes.Buffer
	idxArr, err := PartitionPruning(sctx, tbl, partBlock.PruningConds, partBlock.PartitionNames, partBlock.DeferredCausets, partBlock.DeferredCausetNames)
	if err != nil {
		return "partition pruning error" + err.Error()
	}

	if len(idxArr) == 0 {
		return "partition:dual"
	}

	if len(idxArr) == 1 && idxArr[0] == FullRange {
		return "partition:all"
	}

	for i, idx := range idxArr {
		if i == 0 {
			buffer.WriteString("partition:")
		} else {
			buffer.WriteString(",")
		}
		buffer.WriteString(pi.Definitions[idx].Name.O)
	}

	return buffer.String()
}

// OperatorInfo return other operator information to be explained.
func (p *PhysicalBlockReader) OperatorInfo(normalized bool) string {
	return "data:" + p.blockPlan.ExplainID().String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainInfo() string {
	return "index:" + p.indexPlan.ExplainID().String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexReader) ExplainNormalizedInfo() string {
	return p.ExplainInfo()
}

func (p *PhysicalIndexReader) accessObject(sctx stochastikctx.Context) string {
	ts := p.IndexPlans[0].(*PhysicalIndexScan)
	pi := ts.Block.GetPartitionInfo()
	if pi == nil || !sctx.GetStochastikVars().UseDynamicPartitionPrune() {
		return ""
	}

	var buffer bytes.Buffer
	is := schemareplicant.GetSchemaReplicant(sctx)
	tmp, ok := is.BlockByID(ts.Block.ID)
	if !ok {
		fmt.Fprintf(&buffer, "partition block not found: %d", ts.Block.ID)
		return buffer.String()
	}

	tbl := tmp.(block.PartitionedBlock)
	return partitionAccessObject(sctx, tbl, pi, &p.PartitionInfo)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexLookUpReader) ExplainInfo() string {
	// The children can be inferred by the relation symbol.
	if p.PushedLimit != nil {
		return fmt.Sprintf("limit embedded(offset:%v, count:%v)", p.PushedLimit.Offset, p.PushedLimit.Count)
	}
	return ""
}

func (p *PhysicalIndexLookUpReader) accessObject(sctx stochastikctx.Context) string {
	ts := p.BlockPlans[0].(*PhysicalBlockScan)
	pi := ts.Block.GetPartitionInfo()
	if pi == nil || !sctx.GetStochastikVars().UseDynamicPartitionPrune() {
		return ""
	}

	var buffer bytes.Buffer
	is := schemareplicant.GetSchemaReplicant(sctx)
	tmp, ok := is.BlockByID(ts.Block.ID)
	if !ok {
		fmt.Fprintf(&buffer, "partition block not found: %d", ts.Block.ID)
		return buffer.String()
	}

	tbl := tmp.(block.PartitionedBlock)
	return partitionAccessObject(sctx, tbl, pi, &p.PartitionInfo)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexMergeReader) ExplainInfo() string {
	return ""
}

func (p *PhysicalIndexMergeReader) accessObject(sctx stochastikctx.Context) string {
	ts := p.BlockPlans[0].(*PhysicalBlockScan)
	pi := ts.Block.GetPartitionInfo()
	if pi == nil || !sctx.GetStochastikVars().UseDynamicPartitionPrune() {
		return ""
	}

	is := schemareplicant.GetSchemaReplicant(sctx)
	tmp, ok := is.BlockByID(ts.Block.ID)
	if !ok {
		return "partition block not found" + strconv.FormatInt(ts.Block.ID, 10)
	}
	tbl := tmp.(block.PartitionedBlock)

	return partitionAccessObject(sctx, tbl, pi, &p.PartitionInfo)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalUnionScan) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainInfo implements Plan interface.
func (p *PhysicalSelection) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalSelection) ExplainNormalizedInfo() string {
	return string(expression.SortedExplainNormalizedExpressionList(p.Conditions))
}

// ExplainInfo implements Plan interface.
func (p *PhysicalProjection) ExplainInfo() string {
	return expression.ExplainExpressionList(p.Exprs, p.schemaReplicant)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalProjection) ExplainNormalizedInfo() string {
	return string(expression.SortedExplainNormalizedExpressionList(p.Exprs))
}

// ExplainInfo implements Plan interface.
func (p *PhysicalBlockDual) ExplainInfo() string {
	return fmt.Sprintf("rows:%v", p.RowCount)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalSort) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	return explainByItems(buffer, p.ByItems).String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalLimit) ExplainInfo() string {
	return fmt.Sprintf("offset:%v, count:%v", p.Offset, p.Count)
}

// ExplainInfo implements Plan interface.
func (p *basePhysicalAgg) ExplainInfo() string {
	return p.explainInfo(false)
}

func (p *basePhysicalAgg) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = expression.SortedExplainNormalizedExpressionList
	}

	builder := &strings.Builder{}
	if len(p.GroupByItems) > 0 {
		fmt.Fprintf(builder, "group by:%s, ",
			sortedExplainExpressionList(p.GroupByItems))
	}
	for i := 0; i < len(p.AggFuncs); i++ {
		builder.WriteString("funcs:")
		var colName string
		if normalized {
			colName = p.schemaReplicant.DeferredCausets[i].ExplainNormalizedInfo()
		} else {
			colName = p.schemaReplicant.DeferredCausets[i].ExplainInfo()
		}
		fmt.Fprintf(builder, "%v->%v", aggregation.ExplainAggFunc(p.AggFuncs[i], normalized), colName)
		if i+1 < len(p.AggFuncs) {
			builder.WriteString(", ")
		}
	}
	return builder.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *basePhysicalAgg) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexJoin) ExplainInfo() string {
	return p.explainInfo(false)
}

func (p *PhysicalIndexJoin) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = expression.SortedExplainNormalizedExpressionList
	}

	buffer := bytes.NewBufferString(p.JoinType.String())
	if normalized {
		fmt.Fprintf(buffer, ", inner:%s", p.Children()[p.InnerChildIdx].TP())
	} else {
		fmt.Fprintf(buffer, ", inner:%s", p.Children()[p.InnerChildIdx].ExplainID())
	}
	if len(p.OuterJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", outer key:%s",
			expression.ExplainDeferredCausetList(p.OuterJoinKeys))
	}
	if len(p.InnerJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", inner key:%s",
			expression.ExplainDeferredCausetList(p.InnerJoinKeys))
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s",
			sortedExplainExpressionList(p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			sortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalHashJoin) ExplainInfo() string {
	return p.explainInfo(false)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalHashJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

func (p *PhysicalHashJoin) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = expression.SortedExplainNormalizedExpressionList
	}

	buffer := new(bytes.Buffer)

	if len(p.EqualConditions) == 0 {
		buffer.WriteString("CARTESIAN ")
	}

	buffer.WriteString(p.JoinType.String())

	if len(p.EqualConditions) > 0 {
		if normalized {
			fmt.Fprintf(buffer, ", equal:%s", expression.SortedExplainNormalizedScalarFuncList(p.EqualConditions))
		} else {
			fmt.Fprintf(buffer, ", equal:%v", p.EqualConditions)
		}
	}
	if len(p.LeftConditions) > 0 {
		if normalized {
			fmt.Fprintf(buffer, ", left cond:%s", expression.SortedExplainNormalizedExpressionList(p.LeftConditions))
		} else {
			fmt.Fprintf(buffer, ", left cond:%s", p.LeftConditions)
		}
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			sortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalMergeJoin) ExplainInfo() string {
	return p.explainInfo(false)
}

func (p *PhysicalMergeJoin) explainInfo(normalized bool) string {
	sortedExplainExpressionList := expression.SortedExplainExpressionList
	if normalized {
		sortedExplainExpressionList = expression.SortedExplainNormalizedExpressionList
	}

	buffer := bytes.NewBufferString(p.JoinType.String())
	if len(p.LeftJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", left key:%s",
			expression.ExplainDeferredCausetList(p.LeftJoinKeys))
	}
	if len(p.RightJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", right key:%s",
			expression.ExplainDeferredCausetList(p.RightJoinKeys))
	}
	if len(p.LeftConditions) > 0 {
		if normalized {
			fmt.Fprintf(buffer, ", left cond:%s", expression.SortedExplainNormalizedExpressionList(p.LeftConditions))
		} else {
			fmt.Fprintf(buffer, ", left cond:%s", p.LeftConditions)
		}
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			sortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			sortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalMergeJoin) ExplainNormalizedInfo() string {
	return p.explainInfo(true)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalBroadCastJoin) ExplainInfo() string {
	return p.explainInfo()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalBroadCastJoin) ExplainNormalizedInfo() string {
	return p.explainInfo()
}

func (p *PhysicalBroadCastJoin) explainInfo() string {
	buffer := new(bytes.Buffer)

	buffer.WriteString(p.JoinType.String())

	if len(p.LeftJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", left key:%s",
			expression.ExplainDeferredCausetList(p.LeftJoinKeys))
	}
	if len(p.RightJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", right key:%s",
			expression.ExplainDeferredCausetList(p.RightJoinKeys))
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTopN) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	buffer = explainByItems(buffer, p.ByItems)
	fmt.Fprintf(buffer, ", offset:%v, count:%v", p.Offset, p.Count)
	return buffer.String()
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalTopN) ExplainNormalizedInfo() string {
	buffer := bytes.NewBufferString("")
	buffer = explainNormalizedByItems(buffer, p.ByItems)
	return buffer.String()
}

func (p *PhysicalWindow) formatFrameBound(buffer *bytes.Buffer, bound *FrameBound) {
	if bound.Type == ast.CurrentRow {
		buffer.WriteString("current event")
		return
	}
	if bound.UnBounded {
		buffer.WriteString("unbounded")
	} else if len(bound.CalcFuncs) > 0 {
		sf := bound.CalcFuncs[0].(*expression.ScalarFunction)
		switch sf.FuncName.L {
		case ast.DateAdd, ast.DateSub:
			// For `interval '2:30' minute_second`.
			fmt.Fprintf(buffer, "interval %s %s", sf.GetArgs()[1].ExplainInfo(), sf.GetArgs()[2].ExplainInfo())
		case ast.Plus, ast.Minus:
			// For `1 preceding` of range frame.
			fmt.Fprintf(buffer, "%s", sf.GetArgs()[1].ExplainInfo())
		}
	} else {
		fmt.Fprintf(buffer, "%d", bound.Num)
	}
	if bound.Type == ast.Preceding {
		buffer.WriteString(" preceding")
	} else {
		buffer.WriteString(" following")
	}
}

// ExplainInfo implements Plan interface.
func (p *PhysicalWindow) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	formatWindowFuncDescs(buffer, p.WindowFuncDescs, p.schemaReplicant)
	buffer.WriteString(" over(")
	isFirst := true
	if len(p.PartitionBy) > 0 {
		buffer.WriteString("partition by ")
		for i, item := range p.PartitionBy {
			fmt.Fprintf(buffer, "%s", item.DefCaus.ExplainInfo())
			if i+1 < len(p.PartitionBy) {
				buffer.WriteString(", ")
			}
		}
		isFirst = false
	}
	if len(p.OrderBy) > 0 {
		if !isFirst {
			buffer.WriteString(" ")
		}
		buffer.WriteString("order by ")
		for i, item := range p.OrderBy {
			if item.Desc {
				fmt.Fprintf(buffer, "%s desc", item.DefCaus.ExplainInfo())
			} else {
				fmt.Fprintf(buffer, "%s", item.DefCaus.ExplainInfo())
			}

			if i+1 < len(p.OrderBy) {
				buffer.WriteString(", ")
			}
		}
		isFirst = false
	}
	if p.Frame != nil {
		if !isFirst {
			buffer.WriteString(" ")
		}
		if p.Frame.Type == ast.Rows {
			buffer.WriteString("rows")
		} else {
			buffer.WriteString("range")
		}
		buffer.WriteString(" between ")
		p.formatFrameBound(buffer, p.Frame.Start)
		buffer.WriteString(" and ")
		p.formatFrameBound(buffer, p.Frame.End)
	}
	buffer.WriteString(")")
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *PhysicalShuffle) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "execution info: concurrency:%v, data source:%v", p.Concurrency, p.DataSource.ExplainID())
	return buffer.String()
}

func formatWindowFuncDescs(buffer *bytes.Buffer, descs []*aggregation.WindowFuncDesc, schemaReplicant *expression.Schema) *bytes.Buffer {
	winFuncStartIdx := len(schemaReplicant.DeferredCausets) - len(descs)
	for i, desc := range descs {
		if i != 0 {
			buffer.WriteString(", ")
		}
		fmt.Fprintf(buffer, "%v->%v", desc, schemaReplicant.DeferredCausets[winFuncStartIdx+i])
	}
	return buffer
}

// ExplainInfo implements Plan interface.
func (p *LogicalJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.JoinType.String())
	if len(p.EqualConditions) > 0 {
		fmt.Fprintf(buffer, ", equal:%v", p.EqualConditions)
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s",
			expression.SortedExplainExpressionList(p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalAggregation) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	if len(p.GroupByItems) > 0 {
		fmt.Fprintf(buffer, "group by:%s, ",
			expression.SortedExplainExpressionList(p.GroupByItems))
	}
	if len(p.AggFuncs) > 0 {
		buffer.WriteString("funcs:")
		for i, agg := range p.AggFuncs {
			buffer.WriteString(aggregation.ExplainAggFunc(agg, false))
			if i+1 < len(p.AggFuncs) {
				buffer.WriteString(", ")
			}
		}
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalProjection) ExplainInfo() string {
	return expression.ExplainExpressionList(p.Exprs, p.schemaReplicant)
}

// ExplainInfo implements Plan interface.
func (p *LogicalSelection) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainInfo implements Plan interface.
func (p *LogicalApply) ExplainInfo() string {
	return p.LogicalJoin.ExplainInfo()
}

// ExplainInfo implements Plan interface.
func (p *LogicalBlockDual) ExplainInfo() string {
	return fmt.Sprintf("rowcount:%d", p.RowCount)
}

// ExplainInfo implements Plan interface.
func (p *DataSource) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	tblName := p.blockInfo.Name.O
	if p.BlockAsName != nil && p.BlockAsName.O != "" {
		tblName = p.BlockAsName.O
	}
	fmt.Fprintf(buffer, "block:%s", tblName)
	if p.isPartition {
		if pi := p.blockInfo.GetPartitionInfo(); pi != nil {
			partitionName := pi.GetNameByID(p.physicalBlockID)
			fmt.Fprintf(buffer, ", partition:%s", partitionName)
		}
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalUnionScan) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "conds:%s",
		expression.SortedExplainExpressionList(p.conditions))
	fmt.Fprintf(buffer, ", handle:%s", p.handleDefCauss)
	return buffer.String()
}

func explainByItems(buffer *bytes.Buffer, byItems []*soliton.ByItems) *bytes.Buffer {
	for i, item := range byItems {
		if item.Desc {
			fmt.Fprintf(buffer, "%s:desc", item.Expr.ExplainInfo())
		} else {
			fmt.Fprintf(buffer, "%s", item.Expr.ExplainInfo())
		}

		if i+1 < len(byItems) {
			buffer.WriteString(", ")
		}
	}
	return buffer
}

func explainNormalizedByItems(buffer *bytes.Buffer, byItems []*soliton.ByItems) *bytes.Buffer {
	for i, item := range byItems {
		if item.Desc {
			fmt.Fprintf(buffer, "%s:desc", item.Expr.ExplainNormalizedInfo())
		} else {
			fmt.Fprintf(buffer, "%s", item.Expr.ExplainNormalizedInfo())
		}

		if i+1 < len(byItems) {
			buffer.WriteString(", ")
		}
	}
	return buffer
}

// ExplainInfo implements Plan interface.
func (p *LogicalSort) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	return explainByItems(buffer, p.ByItems).String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalTopN) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	buffer = explainByItems(buffer, p.ByItems)
	fmt.Fprintf(buffer, ", offset:%v, count:%v", p.Offset, p.Count)
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalLimit) ExplainInfo() string {
	return fmt.Sprintf("offset:%v, count:%v", p.Offset, p.Count)
}

// ExplainInfo implements Plan interface.
func (p *LogicalBlockScan) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.Source.ExplainInfo())
	if p.Source.handleDefCauss != nil {
		fmt.Fprintf(buffer, ", pk col:%s", p.Source.handleDefCauss)
	}
	if len(p.AccessConds) > 0 {
		fmt.Fprintf(buffer, ", cond:%v", p.AccessConds)
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *LogicalIndexScan) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.Source.ExplainInfo())
	index := p.Index
	if len(index.DeferredCausets) > 0 {
		buffer.WriteString(", index:")
		for i, idxDefCaus := range index.DeferredCausets {
			buffer.WriteString(idxDefCaus.Name.O)
			if i+1 < len(index.DeferredCausets) {
				buffer.WriteString(", ")
			}
		}
	}
	if len(p.AccessConds) > 0 {
		fmt.Fprintf(buffer, ", cond:%v", p.AccessConds)
	}
	return buffer.String()
}

// ExplainInfo implements Plan interface.
func (p *EinsteinDBSingleGather) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.Source.ExplainInfo())
	if p.IsIndexGather {
		buffer.WriteString(", index:" + p.Index.Name.String())
	}
	return buffer.String()
}

// MetricBlockTimeFormat is the time format for metric block explain and format.
const MetricBlockTimeFormat = "2006-01-02 15:04:05.999"

// ExplainInfo implements Plan interface.
func (p *PhysicalMemBlock) ExplainInfo() string {
	accessObject, operatorInfo := p.AccessObject(), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// AccessObject implements dataAccesser interface.
func (p *PhysicalMemBlock) AccessObject() string {
	return "block:" + p.Block.Name.O
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalMemBlock) OperatorInfo(_ bool) string {
	if p.Extractor != nil {
		return p.Extractor.explainInfo(p)
	}
	return ""
}
