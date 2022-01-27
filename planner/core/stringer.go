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
	"strings"

	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
)

// ToString explains a Plan, returns description string.
func ToString(p Plan) string {
	strs, _ := toString(p, []string{}, []int{})
	return strings.Join(strs, "->")
}

func toString(in Plan, strs []string, idxs []int) ([]string, []int) {
	switch x := in.(type) {
	case LogicalPlan:
		if len(x.Children()) > 1 {
			idxs = append(idxs, len(strs))
		}

		for _, c := range x.Children() {
			strs, idxs = toString(c, strs, idxs)
		}
	case PhysicalPlan:
		if len(x.Children()) > 1 {
			idxs = append(idxs, len(strs))
		}

		for _, c := range x.Children() {
			strs, idxs = toString(c, strs, idxs)
		}
	}

	var str string
	switch x := in.(type) {
	case *CheckBlock:
		str = "CheckBlock"
	case *PhysicalIndexScan:
		str = fmt.Sprintf("Index(%s.%s)%v", x.Block.Name.L, x.Index.Name.L, x.Ranges)
	case *PhysicalBlockScan:
		str = fmt.Sprintf("Block(%s)", x.Block.Name.L)
	case *PhysicalHashJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		if x.InnerChildIdx == 0 {
			str = "RightHashJoin{" + strings.Join(children, "->") + "}"
		} else {
			str = "LeftHashJoin{" + strings.Join(children, "->") + "}"
		}
		for _, eq := range x.EqualConditions {
			l := eq.GetArgs()[0].String()
			r := eq.GetArgs()[1].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *PhysicalMergeJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		id := "MergeJoin"
		switch x.JoinType {
		case SemiJoin:
			id = "MergeSemiJoin"
		case AntiSemiJoin:
			id = "MergeAntiSemiJoin"
		case LeftOuterSemiJoin:
			id = "MergeLeftOuterSemiJoin"
		case AntiLeftOuterSemiJoin:
			id = "MergeAntiLeftOuterSemiJoin"
		case LeftOuterJoin:
			id = "MergeLeftOuterJoin"
		case RightOuterJoin:
			id = "MergeRightOuterJoin"
		case InnerJoin:
			id = "MergeInnerJoin"
		}
		str = id + "{" + strings.Join(children, "->") + "}"
		for i := range x.LeftJoinKeys {
			l := x.LeftJoinKeys[i].String()
			r := x.RightJoinKeys[i].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *LogicalApply, *PhysicalApply:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "Apply{" + strings.Join(children, "->") + "}"
	case *LogicalMaxOneRow, *PhysicalMaxOneRow:
		str = "MaxOneRow"
	case *LogicalLimit, *PhysicalLimit:
		str = "Limit"
	case *PhysicalLock, *LogicalLock:
		str = "Lock"
	case *ShowDBS:
		str = "ShowDBS"
	case *LogicalShow, *PhysicalShow:
		str = "Show"
	case *LogicalShowDBSJobs, *PhysicalShowDBSJobs:
		str = "ShowDBSJobs"
	case *LogicalSort, *PhysicalSort:
		str = "Sort"
	case *LogicalJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		str = "Join{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
		for _, eq := range x.EqualConditions {
			l := eq.GetArgs()[0].String()
			r := eq.GetArgs()[1].String()
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *LogicalUnionAll, *PhysicalUnionAll, *LogicalPartitionUnionAll:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		name := "UnionAll"
		if x.TP() == plancodec.TypePartitionUnion {
			name = "PartitionUnionAll"
		}
		str = name + "{" + strings.Join(children, "->") + "}"
		idxs = idxs[:last]
	case *DataSource:
		if x.isPartition {
			str = fmt.Sprintf("Partition(%d)", x.physicalBlockID)
		} else {
			if x.BlockAsName != nil && x.BlockAsName.L != "" {
				str = fmt.Sprintf("DataScan(%s)", x.BlockAsName)
			} else {
				str = fmt.Sprintf("DataScan(%s)", x.blockInfo.Name)
			}
		}
	case *LogicalSelection:
		str = fmt.Sprintf("Sel(%s)", x.Conditions)
	case *PhysicalSelection:
		str = fmt.Sprintf("Sel(%s)", x.Conditions)
	case *LogicalProjection, *PhysicalProjection:
		str = "Projection"
	case *LogicalTopN:
		str = fmt.Sprintf("TopN(%v,%d,%d)", x.ByItems, x.Offset, x.Count)
	case *PhysicalTopN:
		str = fmt.Sprintf("TopN(%v,%d,%d)", x.ByItems, x.Offset, x.Count)
	case *LogicalBlockDual, *PhysicalBlockDual:
		str = "Dual"
	case *PhysicalHashAgg:
		str = "HashAgg"
	case *PhysicalStreamAgg:
		str = "StreamAgg"
	case *LogicalAggregation:
		str = "Aggr("
		for i, aggFunc := range x.AggFuncs {
			str += aggFunc.String()
			if i != len(x.AggFuncs)-1 {
				str += ","
			}
		}
		str += ")"
	case *PhysicalBlockReader:
		str = fmt.Sprintf("BlockReader(%s)", ToString(x.blockPlan))
	case *PhysicalIndexReader:
		str = fmt.Sprintf("IndexReader(%s)", ToString(x.indexPlan))
	case *PhysicalIndexLookUpReader:
		str = fmt.Sprintf("IndexLookUp(%s, %s)", ToString(x.indexPlan), ToString(x.blockPlan))
	case *PhysicalIndexMergeReader:
		str = "IndexMergeReader(PartialPlans->["
		for i, paritalPlan := range x.partialPlans {
			if i > 0 {
				str += ", "
			}
			str += ToString(paritalPlan)
		}
		str += "], BlockPlan->" + ToString(x.blockPlan) + ")"
	case *PhysicalUnionScan:
		str = fmt.Sprintf("UnionScan(%s)", x.Conditions)
	case *PhysicalIndexJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "IndexJoin{" + strings.Join(children, "->") + "}"
		for i := range x.OuterJoinKeys {
			l := x.OuterJoinKeys[i]
			r := x.InnerJoinKeys[i]
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *PhysicalIndexMergeJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "IndexMergeJoin{" + strings.Join(children, "->") + "}"
		for i := range x.OuterJoinKeys {
			l := x.OuterJoinKeys[i]
			r := x.InnerJoinKeys[i]
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *PhysicalIndexHashJoin:
		last := len(idxs) - 1
		idx := idxs[last]
		children := strs[idx:]
		strs = strs[:idx]
		idxs = idxs[:last]
		str = "IndexHashJoin{" + strings.Join(children, "->") + "}"
		for i := range x.OuterJoinKeys {
			l := x.OuterJoinKeys[i]
			r := x.InnerJoinKeys[i]
			str += fmt.Sprintf("(%s,%s)", l, r)
		}
	case *Analyze:
		str = "Analyze{"
		var children []string
		for _, idx := range x.IdxTasks {
			children = append(children, fmt.Sprintf("Index(%s)", idx.IndexInfo.Name.O))
		}
		for _, col := range x.DefCausTasks {
			var colNames []string
			if col.HandleDefCauss != nil {
				colNames = append(colNames, col.HandleDefCauss.String())
			}
			for _, c := range col.DefCaussInfo {
				colNames = append(colNames, c.Name.O)
			}
			children = append(children, fmt.Sprintf("Block(%s)", strings.Join(colNames, ", ")))
		}
		str = str + strings.Join(children, ",") + "}"
	case *UFIDelate:
		str = fmt.Sprintf("%s->UFIDelate", ToString(x.SelectPlan))
	case *Delete:
		str = fmt.Sprintf("%s->Delete", ToString(x.SelectPlan))
	case *Insert:
		str = "Insert"
		if x.SelectPlan != nil {
			str = fmt.Sprintf("%s->Insert", ToString(x.SelectPlan))
		}
	case *LogicalWindow:
		buffer := bytes.NewBufferString("")
		formatWindowFuncDescs(buffer, x.WindowFuncDescs, x.schemaReplicant)
		str = fmt.Sprintf("Window(%s)", buffer.String())
	case *PhysicalWindow:
		str = fmt.Sprintf("Window(%s)", x.ExplainInfo())
	case *PhysicalShuffle:
		str = fmt.Sprintf("Partition(%s)", x.ExplainInfo())
	case *PhysicalShuffleDataSourceStub:
		str = fmt.Sprintf("PartitionDataSourceStub(%s)", x.ExplainInfo())
	case *PointGetPlan:
		str = fmt.Sprintf("PointGet(")
		if x.IndexInfo != nil {
			str += fmt.Sprintf("Index(%s.%s)%v)", x.TblInfo.Name.L, x.IndexInfo.Name.L, x.IndexValues)
		} else {
			str += fmt.Sprintf("Handle(%s.%s)%v)", x.TblInfo.Name.L, x.TblInfo.GetPkName().L, x.Handle)
		}
	case *BatchPointGetPlan:
		str = fmt.Sprintf("BatchPointGet(")
		if x.IndexInfo != nil {
			str += fmt.Sprintf("Index(%s.%s)%v)", x.TblInfo.Name.L, x.IndexInfo.Name.L, x.IndexValues)
		} else {
			str += fmt.Sprintf("Handle(%s.%s)%v)", x.TblInfo.Name.L, x.TblInfo.GetPkName().L, x.Handles)
		}
	default:
		str = fmt.Sprintf("%T", in)
	}
	strs = append(strs, str)
	return strs, idxs
}
