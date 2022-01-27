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
	"math"
	"sort"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	"github.com/whtcorpsinc/milevadb/planner/property"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/collate"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

func (p *LogicalUnionScan) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if prop.IsFlashOnlyProp() {
		return nil, true
	}
	childProp := prop.Clone()
	us := PhysicalUnionScan{
		Conditions:     p.conditions,
		HandleDefCauss: p.handleDefCauss,
	}.Init(p.ctx, p.stats, p.blockOffset, childProp)
	return []PhysicalPlan{us}, true
}

func getMaxSortPrefix(sortDefCauss, allDefCauss []*expression.DeferredCauset) []int {
	tmpSchema := expression.NewSchema(allDefCauss...)
	sortDefCausOffsets := make([]int, 0, len(sortDefCauss))
	for _, sortDefCaus := range sortDefCauss {
		offset := tmpSchema.DeferredCausetIndex(sortDefCaus)
		if offset == -1 {
			return sortDefCausOffsets
		}
		sortDefCausOffsets = append(sortDefCausOffsets, offset)
	}
	return sortDefCausOffsets
}

func findMaxPrefixLen(candidates [][]*expression.DeferredCauset, keys []*expression.DeferredCauset) int {
	maxLen := 0
	for _, candidateKeys := range candidates {
		matchedLen := 0
		for i := range keys {
			if i < len(candidateKeys) && keys[i].Equal(nil, candidateKeys[i]) {
				matchedLen++
			} else {
				break
			}
		}
		if matchedLen > maxLen {
			maxLen = matchedLen
		}
	}
	return maxLen
}

func (p *LogicalJoin) moveEqualToOtherConditions(offsets []int) []expression.Expression {
	// Construct used equal condition set based on the equal condition offsets.
	usedEqConds := set.NewIntSet()
	for _, eqCondIdx := range offsets {
		usedEqConds.Insert(eqCondIdx)
	}

	// Construct otherConds, which is composed of the original other conditions
	// and the remained unused equal conditions.
	numOtherConds := len(p.OtherConditions) + len(p.EqualConditions) - len(usedEqConds)
	otherConds := make([]expression.Expression, len(p.OtherConditions), numOtherConds)
	INTERLOCKy(otherConds, p.OtherConditions)
	for eqCondIdx := range p.EqualConditions {
		if !usedEqConds.Exist(eqCondIdx) {
			otherConds = append(otherConds, p.EqualConditions[eqCondIdx])
		}
	}

	return otherConds
}

// Only if the input required prop is the prefix fo join keys, we can pass through this property.
func (p *PhysicalMergeJoin) tryToGetChildReqProp(prop *property.PhysicalProperty) ([]*property.PhysicalProperty, bool) {
	all, desc := prop.AllSameOrder()
	lProp := property.NewPhysicalProperty(property.RootTaskType, p.LeftJoinKeys, desc, math.MaxFloat64, false)
	rProp := property.NewPhysicalProperty(property.RootTaskType, p.RightJoinKeys, desc, math.MaxFloat64, false)
	if !prop.IsEmpty() {
		// sort merge join fits the cases of massive ordered data, so desc scan is always expensive.
		if !all {
			return nil, false
		}
		if !prop.IsPrefix(lProp) && !prop.IsPrefix(rProp) {
			return nil, false
		}
		if prop.IsPrefix(rProp) && p.JoinType == LeftOuterJoin {
			return nil, false
		}
		if prop.IsPrefix(lProp) && p.JoinType == RightOuterJoin {
			return nil, false
		}
	}

	return []*property.PhysicalProperty{lProp, rProp}, true
}

func (p *LogicalJoin) checkJoinKeyDefCauslation(leftKeys, rightKeys []*expression.DeferredCauset) bool {
	// if a left key and its corresponding right key have different collation, don't use MergeJoin since
	// the their children may sort their records in different ways
	for i := range leftKeys {
		lt := leftKeys[i].RetType
		rt := rightKeys[i].RetType
		if (lt.EvalType() == types.ETString && rt.EvalType() == types.ETString) &&
			(leftKeys[i].RetType.Charset != rightKeys[i].RetType.Charset ||
				leftKeys[i].RetType.DefCauslate != rightKeys[i].RetType.DefCauslate) {
			return false
		}
	}
	return true
}

// GetMergeJoin convert the logical join to physical merge join based on the physical property.
func (p *LogicalJoin) GetMergeJoin(prop *property.PhysicalProperty, schemaReplicant *expression.Schema, statsInfo *property.StatsInfo, leftStatsInfo *property.StatsInfo, rightStatsInfo *property.StatsInfo) []PhysicalPlan {
	joins := make([]PhysicalPlan, 0, len(p.leftProperties)+1)
	// The leftProperties caches all the possible properties that are provided by its children.
	leftJoinKeys, rightJoinKeys, isNullEQ, hasNullEQ := p.GetJoinKeys()
	// TODO: support null equal join keys for merge join
	if hasNullEQ {
		return nil
	}
	for _, lhsChildProperty := range p.leftProperties {
		offsets := getMaxSortPrefix(lhsChildProperty, leftJoinKeys)
		if len(offsets) == 0 {
			continue
		}

		leftKeys := lhsChildProperty[:len(offsets)]
		rightKeys := expression.NewSchema(rightJoinKeys...).DeferredCausetsByIndices(offsets)
		newIsNullEQ := make([]bool, 0, len(offsets))
		for _, offset := range offsets {
			newIsNullEQ = append(newIsNullEQ, isNullEQ[offset])
		}

		prefixLen := findMaxPrefixLen(p.rightProperties, rightKeys)
		if prefixLen == 0 {
			continue
		}

		leftKeys = leftKeys[:prefixLen]
		rightKeys = rightKeys[:prefixLen]
		newIsNullEQ = newIsNullEQ[:prefixLen]
		if !p.checkJoinKeyDefCauslation(leftKeys, rightKeys) {
			continue
		}
		offsets = offsets[:prefixLen]
		baseJoin := basePhysicalJoin{
			JoinType:        p.JoinType,
			LeftConditions:  p.LeftConditions,
			RightConditions: p.RightConditions,
			DefaultValues:   p.DefaultValues,
			LeftJoinKeys:    leftKeys,
			RightJoinKeys:   rightKeys,
			IsNullEQ:        newIsNullEQ,
		}
		mergeJoin := PhysicalMergeJoin{basePhysicalJoin: baseJoin}.Init(p.ctx, statsInfo.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset)
		mergeJoin.SetSchema(schemaReplicant)
		mergeJoin.OtherConditions = p.moveEqualToOtherConditions(offsets)
		mergeJoin.initCompareFuncs()
		if reqProps, ok := mergeJoin.tryToGetChildReqProp(prop); ok {
			// Adjust expected count for children nodes.
			if prop.ExpectedCnt < statsInfo.RowCount {
				expCntScale := prop.ExpectedCnt / statsInfo.RowCount
				reqProps[0].ExpectedCnt = leftStatsInfo.RowCount * expCntScale
				reqProps[1].ExpectedCnt = rightStatsInfo.RowCount * expCntScale
			}
			mergeJoin.childrenReqProps = reqProps
			_, desc := prop.AllSameOrder()
			mergeJoin.Desc = desc
			joins = append(joins, mergeJoin)
		}
	}
	// If MilevaDB_SMJ hint is existed, it should consider enforce merge join,
	// because we can't trust lhsChildProperty completely.
	if (p.preferJoinType & preferMergeJoin) > 0 {
		joins = append(joins, p.getEnforcedMergeJoin(prop, schemaReplicant, statsInfo)...)
	}

	return joins
}

// Change JoinKeys order, by offsets array
// offsets array is generate by prop check
func getNewJoinKeysByOffsets(oldJoinKeys []*expression.DeferredCauset, offsets []int) []*expression.DeferredCauset {
	newKeys := make([]*expression.DeferredCauset, 0, len(oldJoinKeys))
	for _, offset := range offsets {
		newKeys = append(newKeys, oldJoinKeys[offset])
	}
	for pos, key := range oldJoinKeys {
		isExist := false
		for _, p := range offsets {
			if p == pos {
				isExist = true
				break
			}
		}
		if !isExist {
			newKeys = append(newKeys, key)
		}
	}
	return newKeys
}

func getNewNullEQByOffsets(oldNullEQ []bool, offsets []int) []bool {
	newNullEQ := make([]bool, 0, len(oldNullEQ))
	for _, offset := range offsets {
		newNullEQ = append(newNullEQ, oldNullEQ[offset])
	}
	for pos, key := range oldNullEQ {
		isExist := false
		for _, p := range offsets {
			if p == pos {
				isExist = true
				break
			}
		}
		if !isExist {
			newNullEQ = append(newNullEQ, key)
		}
	}
	return newNullEQ
}

func (p *LogicalJoin) getEnforcedMergeJoin(prop *property.PhysicalProperty, schemaReplicant *expression.Schema, statsInfo *property.StatsInfo) []PhysicalPlan {
	// Check whether SMJ can satisfy the required property
	leftJoinKeys, rightJoinKeys, isNullEQ, hasNullEQ := p.GetJoinKeys()
	// TODO: support null equal join keys for merge join
	if hasNullEQ {
		return nil
	}
	offsets := make([]int, 0, len(leftJoinKeys))
	all, desc := prop.AllSameOrder()
	if !all {
		return nil
	}
	for _, item := range prop.Items {
		isExist := false
		for joinKeyPos := 0; joinKeyPos < len(leftJoinKeys); joinKeyPos++ {
			var key *expression.DeferredCauset
			if item.DefCaus.Equal(p.ctx, leftJoinKeys[joinKeyPos]) {
				key = leftJoinKeys[joinKeyPos]
			}
			if item.DefCaus.Equal(p.ctx, rightJoinKeys[joinKeyPos]) {
				key = rightJoinKeys[joinKeyPos]
			}
			if key == nil {
				continue
			}
			for i := 0; i < len(offsets); i++ {
				if offsets[i] == joinKeyPos {
					isExist = true
					break
				}
			}
			if !isExist {
				offsets = append(offsets, joinKeyPos)
			}
			isExist = true
			break
		}
		if !isExist {
			return nil
		}
	}
	// Generate the enforced sort merge join
	leftKeys := getNewJoinKeysByOffsets(leftJoinKeys, offsets)
	rightKeys := getNewJoinKeysByOffsets(rightJoinKeys, offsets)
	newNullEQ := getNewNullEQByOffsets(isNullEQ, offsets)
	otherConditions := make([]expression.Expression, len(p.OtherConditions), len(p.OtherConditions)+len(p.EqualConditions))
	INTERLOCKy(otherConditions, p.OtherConditions)
	if !p.checkJoinKeyDefCauslation(leftKeys, rightKeys) {
		// if the join keys' collation are conflicted, we use the empty join key
		// and move EqualConditions to OtherConditions.
		leftKeys = nil
		rightKeys = nil
		newNullEQ = nil
		otherConditions = append(otherConditions, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
	}
	lProp := property.NewPhysicalProperty(property.RootTaskType, leftKeys, desc, math.MaxFloat64, true)
	rProp := property.NewPhysicalProperty(property.RootTaskType, rightKeys, desc, math.MaxFloat64, true)
	baseJoin := basePhysicalJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		DefaultValues:   p.DefaultValues,
		LeftJoinKeys:    leftKeys,
		RightJoinKeys:   rightKeys,
		IsNullEQ:        newNullEQ,
		OtherConditions: otherConditions,
	}
	enforcedPhysicalMergeJoin := PhysicalMergeJoin{basePhysicalJoin: baseJoin, Desc: desc}.Init(p.ctx, statsInfo.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset)
	enforcedPhysicalMergeJoin.SetSchema(schemaReplicant)
	enforcedPhysicalMergeJoin.childrenReqProps = []*property.PhysicalProperty{lProp, rProp}
	enforcedPhysicalMergeJoin.initCompareFuncs()
	return []PhysicalPlan{enforcedPhysicalMergeJoin}
}

func (p *PhysicalMergeJoin) initCompareFuncs() {
	p.CompareFuncs = make([]expression.CompareFunc, 0, len(p.LeftJoinKeys))
	for i := range p.LeftJoinKeys {
		p.CompareFuncs = append(p.CompareFuncs, expression.GetCmpFunction(p.ctx, p.LeftJoinKeys[i], p.RightJoinKeys[i]))
	}
}

// ForceUseOuterBuild4Test is a test option to control forcing use outer input as build.
// TODO: use hint and remove this variable
var ForceUseOuterBuild4Test = false

// ForcedHashLeftJoin4Test is a test option to force using HashLeftJoin
// TODO: use hint and remove this variable
var ForcedHashLeftJoin4Test = false

func (p *LogicalJoin) getHashJoins(prop *property.PhysicalProperty) []PhysicalPlan {
	if !prop.IsEmpty() { // hash join doesn't promise any orders
		return nil
	}
	joins := make([]PhysicalPlan, 0, 2)
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		joins = append(joins, p.getHashJoin(prop, 1, false))
	case LeftOuterJoin:
		if ForceUseOuterBuild4Test {
			joins = append(joins, p.getHashJoin(prop, 1, true))
		} else {
			joins = append(joins, p.getHashJoin(prop, 1, false))
			joins = append(joins, p.getHashJoin(prop, 1, true))
		}
	case RightOuterJoin:
		if ForceUseOuterBuild4Test {
			joins = append(joins, p.getHashJoin(prop, 0, true))
		} else {
			joins = append(joins, p.getHashJoin(prop, 0, false))
			joins = append(joins, p.getHashJoin(prop, 0, true))
		}
	case InnerJoin:
		if ForcedHashLeftJoin4Test {
			joins = append(joins, p.getHashJoin(prop, 1, false))
		} else {
			joins = append(joins, p.getHashJoin(prop, 1, false))
			joins = append(joins, p.getHashJoin(prop, 0, false))
		}
	}
	return joins
}

func (p *LogicalJoin) getHashJoin(prop *property.PhysicalProperty, innerIdx int, useOuterToBuild bool) *PhysicalHashJoin {
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	chReqProps[1-innerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	if prop.ExpectedCnt < p.stats.RowCount {
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		chReqProps[1-innerIdx].ExpectedCnt = p.children[1-innerIdx].statsInfo().RowCount * expCntScale
	}
	hashJoin := NewPhysicalHashJoin(p, innerIdx, useOuterToBuild, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	hashJoin.SetSchema(p.schemaReplicant)
	return hashJoin
}

// When inner plan is BlockReader, the parameter `ranges` will be nil. Because pk only have one column. So all of its range
// is generated during execution time.
func (p *LogicalJoin) constructIndexJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask task,
	ranges []*ranger.Range,
	keyOff2IdxOff []int,
	path *soliton.AccessPath,
	compareFilters *DefCausWithCmpFuncManager,
) []PhysicalPlan {
	joinType := p.JoinType
	var (
		innerJoinKeys []*expression.DeferredCauset
		outerJoinKeys []*expression.DeferredCauset
		isNullEQ      []bool
		hasNullEQ     bool
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, isNullEQ, hasNullEQ = p.GetJoinKeys()
	}
	// TODO: support null equal join keys for index join
	if hasNullEQ {
		return nil
	}
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[outerIdx] = &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64, Items: prop.Items}
	if prop.ExpectedCnt < p.stats.RowCount {
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		chReqProps[outerIdx].ExpectedCnt = p.children[outerIdx].statsInfo().RowCount * expCntScale
	}
	newInnerKeys := make([]*expression.DeferredCauset, 0, len(innerJoinKeys))
	newOuterKeys := make([]*expression.DeferredCauset, 0, len(outerJoinKeys))
	newIsNullEQ := make([]bool, 0, len(isNullEQ))
	newKeyOff := make([]int, 0, len(keyOff2IdxOff))
	newOtherConds := make([]expression.Expression, len(p.OtherConditions), len(p.OtherConditions)+len(p.EqualConditions))
	INTERLOCKy(newOtherConds, p.OtherConditions)
	for keyOff, idxOff := range keyOff2IdxOff {
		if keyOff2IdxOff[keyOff] < 0 {
			newOtherConds = append(newOtherConds, p.EqualConditions[keyOff])
			continue
		}
		newInnerKeys = append(newInnerKeys, innerJoinKeys[keyOff])
		newOuterKeys = append(newOuterKeys, outerJoinKeys[keyOff])
		newIsNullEQ = append(newIsNullEQ, isNullEQ[keyOff])
		newKeyOff = append(newKeyOff, idxOff)
	}
	baseJoin := basePhysicalJoin{
		InnerChildIdx:   1 - outerIdx,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: newOtherConds,
		JoinType:        joinType,
		OuterJoinKeys:   newOuterKeys,
		InnerJoinKeys:   newInnerKeys,
		IsNullEQ:        newIsNullEQ,
		DefaultValues:   p.DefaultValues,
	}
	join := PhysicalIndexJoin{
		basePhysicalJoin: baseJoin,
		innerTask:        innerTask,
		KeyOff2IdxOff:    newKeyOff,
		Ranges:           ranges,
		CompareFilters:   compareFilters,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, chReqProps...)
	if path != nil {
		join.IdxDefCausLens = path.IdxDefCausLens
	}
	join.SetSchema(p.schemaReplicant)
	return []PhysicalPlan{join}
}

func (p *LogicalJoin) constructIndexMergeJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask task,
	ranges []*ranger.Range,
	keyOff2IdxOff []int,
	path *soliton.AccessPath,
	compareFilters *DefCausWithCmpFuncManager,
) []PhysicalPlan {
	indexJoins := p.constructIndexJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, compareFilters)
	indexMergeJoins := make([]PhysicalPlan, 0, len(indexJoins))
	for _, plan := range indexJoins {
		join := plan.(*PhysicalIndexJoin)
		hasPrefixDefCaus := false
		for _, l := range join.IdxDefCausLens {
			if l != types.UnspecifiedLength {
				hasPrefixDefCaus = true
				break
			}
		}
		// If index column has prefix length, the merge join can not guarantee the relevance
		// between index and join keys. So we should skip this case.
		// For more details, please check the following code and comments.
		if hasPrefixDefCaus {
			continue
		}

		// keyOff2KeyOffOrderByIdx is map the join keys offsets to [0, len(joinKeys)) ordered by the
		// join key position in inner index.
		keyOff2KeyOffOrderByIdx := make([]int, len(join.OuterJoinKeys))
		keyOffMapList := make([]int, len(join.KeyOff2IdxOff))
		INTERLOCKy(keyOffMapList, join.KeyOff2IdxOff)
		keyOffMap := make(map[int]int, len(keyOffMapList))
		for i, idxOff := range keyOffMapList {
			keyOffMap[idxOff] = i
		}
		sort.Slice(keyOffMapList, func(i, j int) bool { return keyOffMapList[i] < keyOffMapList[j] })
		keyIsIndexPrefix := true
		for keyOff, idxOff := range keyOffMapList {
			if keyOff != idxOff {
				keyIsIndexPrefix = false
				break
			}
			keyOff2KeyOffOrderByIdx[keyOffMap[idxOff]] = keyOff
		}
		if !keyIsIndexPrefix {
			continue
		}
		// isOuterKeysPrefix means whether the outer join keys are the prefix of the prop items.
		isOuterKeysPrefix := len(join.OuterJoinKeys) <= len(prop.Items)
		compareFuncs := make([]expression.CompareFunc, 0, len(join.OuterJoinKeys))
		outerCompareFuncs := make([]expression.CompareFunc, 0, len(join.OuterJoinKeys))

		for i := range join.KeyOff2IdxOff {
			if isOuterKeysPrefix && !prop.Items[i].DefCaus.Equal(nil, join.OuterJoinKeys[keyOff2KeyOffOrderByIdx[i]]) {
				isOuterKeysPrefix = false
			}
			compareFuncs = append(compareFuncs, expression.GetCmpFunction(p.ctx, join.OuterJoinKeys[i], join.InnerJoinKeys[i]))
			outerCompareFuncs = append(outerCompareFuncs, expression.GetCmpFunction(p.ctx, join.OuterJoinKeys[i], join.OuterJoinKeys[i]))
		}
		// canKeepOuterOrder means whether the prop items are the prefix of the outer join keys.
		canKeepOuterOrder := len(prop.Items) <= len(join.OuterJoinKeys)
		for i := 0; canKeepOuterOrder && i < len(prop.Items); i++ {
			if !prop.Items[i].DefCaus.Equal(nil, join.OuterJoinKeys[keyOff2KeyOffOrderByIdx[i]]) {
				canKeepOuterOrder = false
			}
		}
		// Since index merge join requires prop items the prefix of outer join keys
		// or outer join keys the prefix of the prop items. So we need `canKeepOuterOrder` or
		// `isOuterKeysPrefix` to be true.
		if canKeepOuterOrder || isOuterKeysPrefix {
			indexMergeJoin := PhysicalIndexMergeJoin{
				PhysicalIndexJoin:       *join,
				KeyOff2KeyOffOrderByIdx: keyOff2KeyOffOrderByIdx,
				NeedOuterSort:           !isOuterKeysPrefix,
				CompareFuncs:            compareFuncs,
				OuterCompareFuncs:       outerCompareFuncs,
				Desc:                    !prop.IsEmpty() && prop.Items[0].Desc,
			}.Init(p.ctx)
			indexMergeJoins = append(indexMergeJoins, indexMergeJoin)
		}
	}
	return indexMergeJoins
}

func (p *LogicalJoin) constructIndexHashJoin(
	prop *property.PhysicalProperty,
	outerIdx int,
	innerTask task,
	ranges []*ranger.Range,
	keyOff2IdxOff []int,
	path *soliton.AccessPath,
	compareFilters *DefCausWithCmpFuncManager,
) []PhysicalPlan {
	indexJoins := p.constructIndexJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, path, compareFilters)
	indexHashJoins := make([]PhysicalPlan, 0, len(indexJoins))
	for _, plan := range indexJoins {
		join := plan.(*PhysicalIndexJoin)
		indexHashJoin := PhysicalIndexHashJoin{
			PhysicalIndexJoin: *join,
			// Prop is empty means that the parent operator does not need the
			// join operator to provide any promise of the output order.
			KeepOuterOrder: !prop.IsEmpty(),
		}.Init(p.ctx)
		indexHashJoins = append(indexHashJoins, indexHashJoin)
	}
	return indexHashJoins
}

// getIndexJoinByOuterIdx will generate index join by outerIndex. OuterIdx points out the outer child.
// First of all, we'll check whether the inner child is DataSource.
// Then, we will extract the join keys of p's equal conditions. Then check whether all of them are just the primary key
// or match some part of on index. If so we will choose the best one and construct a index join.
func (p *LogicalJoin) getIndexJoinByOuterIdx(prop *property.PhysicalProperty, outerIdx int) (joins []PhysicalPlan) {
	outerChild, innerChild := p.children[outerIdx], p.children[1-outerIdx]
	all, _ := prop.AllSameOrder()
	// If the order by columns are not all from outer child, index join cannot promise the order.
	if !prop.AllDefCaussFromSchema(outerChild.Schema()) || !all {
		return nil
	}
	var (
		innerJoinKeys []*expression.DeferredCauset
		outerJoinKeys []*expression.DeferredCauset
	)
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys, _, _ = p.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys, _, _ = p.GetJoinKeys()
	}
	ds, isDataSource := innerChild.(*DataSource)
	us, isUnionScan := innerChild.(*LogicalUnionScan)
	if (!isDataSource && !isUnionScan) || (isDataSource && ds.preferStoreType&preferTiFlash != 0) {
		return nil
	}
	if isUnionScan {
		// The child of union scan may be union all for partition block.
		ds, isDataSource = us.Children()[0].(*DataSource)
		if !isDataSource {
			return nil
		}
		// If one of the union scan children is a TiFlash block, then we can't choose index join.
		for _, child := range us.Children() {
			if ds, ok := child.(*DataSource); ok && ds.preferStoreType&preferTiFlash != 0 {
				return nil
			}
		}
	}
	var avgInnerRowCnt float64
	if outerChild.statsInfo().RowCount > 0 {
		avgInnerRowCnt = p.equalCondOutCnt / outerChild.statsInfo().RowCount
	}
	joins = p.buildIndexJoinInner2BlockScan(prop, ds, innerJoinKeys, outerJoinKeys, outerIdx, us, avgInnerRowCnt)
	if joins != nil {
		return
	}
	return p.buildIndexJoinInner2IndexScan(prop, ds, innerJoinKeys, outerJoinKeys, outerIdx, us, avgInnerRowCnt)
}

func (p *LogicalJoin) getIndexJoinBuildHelper(ds *DataSource, innerJoinKeys []*expression.DeferredCauset,
	checkPathValid func(path *soliton.AccessPath) bool) (*indexJoinBuildHelper, []int) {
	helper := &indexJoinBuildHelper{join: p}
	for _, path := range ds.possibleAccessPaths {
		if checkPathValid(path) {
			emptyRange, err := helper.analyzeLookUpFilters(path, ds, innerJoinKeys)
			if emptyRange {
				return nil, nil
			}
			if err != nil {
				logutil.BgLogger().Warn("build index join failed", zap.Error(err))
			}
		}
	}
	if helper.chosenPath == nil {
		return nil, nil
	}
	keyOff2IdxOff := make([]int, len(innerJoinKeys))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = -1
	}
	for idxOff, keyOff := range helper.idxOff2KeyOff {
		if keyOff != -1 {
			keyOff2IdxOff[keyOff] = idxOff
		}
	}
	return helper, keyOff2IdxOff
}

// buildIndexJoinInner2BlockScan builds a BlockScan as the inner child for an
// IndexJoin if possible.
// If the inner side of a index join is a BlockScan, only one tuple will be
// fetched from the inner side for every tuple from the outer side. This will be
// promised to be no worse than building IndexScan as the inner child.
func (p *LogicalJoin) buildIndexJoinInner2BlockScan(
	prop *property.PhysicalProperty, ds *DataSource, innerJoinKeys, outerJoinKeys []*expression.DeferredCauset,
	outerIdx int, us *LogicalUnionScan, avgInnerRowCnt float64) (joins []PhysicalPlan) {
	var tblPath *soliton.AccessPath
	for _, path := range ds.possibleAccessPaths {
		if path.IsBlockPath() && path.StoreType == ekv.EinsteinDB {
			tblPath = path
			break
		}
	}
	if tblPath == nil {
		return nil
	}
	keyOff2IdxOff := make([]int, len(innerJoinKeys))
	newOuterJoinKeys := make([]*expression.DeferredCauset, 0)
	var ranges []*ranger.Range
	var innerTask, innerTask2 task
	var helper *indexJoinBuildHelper
	if ds.blockInfo.IsCommonHandle {
		helper, keyOff2IdxOff = p.getIndexJoinBuildHelper(ds, innerJoinKeys, func(path *soliton.AccessPath) bool { return path.IsCommonHandlePath })
		if helper == nil {
			return nil
		}
		innerTask = p.constructInnerBlockScanTask(ds, nil, outerJoinKeys, us, false, false, avgInnerRowCnt)
		// The index merge join's inner plan is different from index join, so we
		// should construct another inner plan for it.
		// Because we can't keep order for union scan, if there is a union scan in inner task,
		// we can't construct index merge join.
		if us == nil {
			innerTask2 = p.constructInnerBlockScanTask(ds, nil, outerJoinKeys, us, true, !prop.IsEmpty() && prop.Items[0].Desc, avgInnerRowCnt)
		}
		ranges = helper.chosenRanges
	} else {
		pkMatched := false
		pkDefCaus := ds.getPKIsHandleDefCaus()
		if pkDefCaus == nil {
			return nil
		}
		for i, key := range innerJoinKeys {
			if !key.Equal(nil, pkDefCaus) {
				keyOff2IdxOff[i] = -1
				continue
			}
			pkMatched = true
			keyOff2IdxOff[i] = 0
			// Add to newOuterJoinKeys only if conditions contain inner primary key. For issue #14822.
			newOuterJoinKeys = append(newOuterJoinKeys, outerJoinKeys[i])
		}
		outerJoinKeys = newOuterJoinKeys
		if !pkMatched {
			return nil
		}
		innerTask = p.constructInnerBlockScanTask(ds, pkDefCaus, outerJoinKeys, us, false, false, avgInnerRowCnt)
		// The index merge join's inner plan is different from index join, so we
		// should construct another inner plan for it.
		// Because we can't keep order for union scan, if there is a union scan in inner task,
		// we can't construct index merge join.
		if us == nil {
			innerTask2 = p.constructInnerBlockScanTask(ds, pkDefCaus, outerJoinKeys, us, true, !prop.IsEmpty() && prop.Items[0].Desc, avgInnerRowCnt)
		}
	}
	joins = make([]PhysicalPlan, 0, 3)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(p.constructIndexHashJoin(prop, outerIdx, innerTask, nil, keyOff2IdxOff, nil, nil))
		}
	})
	joins = append(joins, p.constructIndexJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, nil, nil)...)
	// We can reuse the `innerTask` here since index nested loop hash join
	// do not need the inner child to promise the order.
	joins = append(joins, p.constructIndexHashJoin(prop, outerIdx, innerTask, ranges, keyOff2IdxOff, nil, nil)...)
	if innerTask2 != nil {
		joins = append(joins, p.constructIndexMergeJoin(prop, outerIdx, innerTask2, ranges, keyOff2IdxOff, nil, nil)...)
	}
	return joins
}

func (p *LogicalJoin) buildIndexJoinInner2IndexScan(
	prop *property.PhysicalProperty, ds *DataSource, innerJoinKeys, outerJoinKeys []*expression.DeferredCauset,
	outerIdx int, us *LogicalUnionScan, avgInnerRowCnt float64) (joins []PhysicalPlan) {
	helper, keyOff2IdxOff := p.getIndexJoinBuildHelper(ds, innerJoinKeys, func(path *soliton.AccessPath) bool { return !path.IsBlockPath() })
	if helper == nil {
		return nil
	}
	joins = make([]PhysicalPlan, 0, 3)
	rangeInfo := helper.buildRangeDecidedByInformation(helper.chosenPath.IdxDefCauss, outerJoinKeys)
	maxOneRow := false
	if helper.chosenPath.Index.Unique && helper.maxUsedDefCauss == len(helper.chosenPath.FullIdxDefCauss) {
		l := len(helper.chosenAccess)
		if l == 0 {
			maxOneRow = true
		} else {
			sf, ok := helper.chosenAccess[l-1].(*expression.ScalarFunction)
			maxOneRow = ok && (sf.FuncName.L == ast.EQ)
		}
	}
	innerTask := p.constructInnerIndexScanTask(ds, helper.chosenPath, helper.chosenRemained, outerJoinKeys, us, rangeInfo, false, false, avgInnerRowCnt, maxOneRow)
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(p.constructIndexHashJoin(prop, outerIdx, innerTask, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastDefCausManager))
		}
	})
	joins = append(joins, p.constructIndexJoin(prop, outerIdx, innerTask, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastDefCausManager)...)
	// We can reuse the `innerTask` here since index nested loop hash join
	// do not need the inner child to promise the order.
	joins = append(joins, p.constructIndexHashJoin(prop, outerIdx, innerTask, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastDefCausManager)...)
	// The index merge join's inner plan is different from index join, so we
	// should construct another inner plan for it.
	// Because we can't keep order for union scan, if there is a union scan in inner task,
	// we can't construct index merge join.
	if us == nil {
		innerTask2 := p.constructInnerIndexScanTask(ds, helper.chosenPath, helper.chosenRemained, outerJoinKeys, us, rangeInfo, true, !prop.IsEmpty() && prop.Items[0].Desc, avgInnerRowCnt, maxOneRow)
		if innerTask2 != nil {
			joins = append(joins, p.constructIndexMergeJoin(prop, outerIdx, innerTask2, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath, helper.lastDefCausManager)...)
		}
	}
	return joins
}

type indexJoinBuildHelper struct {
	join *LogicalJoin

	chosenIndexInfo    *perceptron.IndexInfo
	maxUsedDefCauss    int
	chosenAccess       []expression.Expression
	chosenRemained     []expression.Expression
	idxOff2KeyOff      []int
	lastDefCausManager *DefCausWithCmpFuncManager
	chosenRanges       []*ranger.Range
	chosenPath         *soliton.AccessPath

	curPossibleUsedKeys     []*expression.DeferredCauset
	curNotUsedIndexDefCauss []*expression.DeferredCauset
	curNotUsedDefCausLens   []int
	curIdxOff2KeyOff        []int
}

func (ijHelper *indexJoinBuildHelper) buildRangeDecidedByInformation(idxDefCauss []*expression.DeferredCauset, outerJoinKeys []*expression.DeferredCauset) string {
	buffer := bytes.NewBufferString("[")
	isFirst := true
	for idxOff, keyOff := range ijHelper.idxOff2KeyOff {
		if keyOff == -1 {
			continue
		}
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		buffer.WriteString(fmt.Sprintf("eq(%v, %v)", idxDefCauss[idxOff], outerJoinKeys[keyOff]))
	}
	for _, access := range ijHelper.chosenAccess {
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		buffer.WriteString(fmt.Sprintf("%v", access))
	}
	buffer.WriteString("]")
	return buffer.String()
}

// constructInnerBlockScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerBlockScanTask(
	ds *DataSource,
	pk *expression.DeferredCauset,
	outerJoinKeys []*expression.DeferredCauset,
	us *LogicalUnionScan,
	keepOrder bool,
	desc bool,
	rowCount float64,
) task {
	// If `ds.blockInfo.GetPartitionInfo() != nil`,
	// it means the data source is a partition block reader.
	// If the inner task need to keep order, the partition block reader can't satisfy it.
	if keepOrder && ds.blockInfo.GetPartitionInfo() != nil {
		return nil
	}
	var ranges []*ranger.Range
	// pk is nil means the block uses the common handle.
	if pk == nil {
		ranges = ranger.FullRange()
	} else {
		ranges = ranger.FullIntRange(allegrosql.HasUnsignedFlag(pk.RetType.Flag))
	}
	ts := PhysicalBlockScan{
		Block:           ds.blockInfo,
		DeferredCausets: ds.DeferredCausets,
		BlockAsName:     ds.BlockAsName,
		DBName:          ds.DBName,
		filterCondition: ds.pushedDownConds,
		Ranges:          ranges,
		rangeDecidedBy:  outerJoinKeys,
		KeepOrder:       keepOrder,
		Desc:            desc,
		physicalBlockID: ds.physicalBlockID,
		isPartition:     ds.isPartition,
	}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.schemaReplicant.Clone())
	if rowCount <= 0 {
		rowCount = float64(1)
	}
	selectivity := float64(1)
	countAfterAccess := rowCount
	if len(ts.filterCondition) > 0 {
		var err error
		selectivity, _, err = ds.blockStats.HistDefCausl.Selectivity(ds.ctx, ts.filterCondition, ds.possibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("block", ts.BlockAsName.L))
			selectivity = SelectionFactor
		}
		// rowCount is computed from result event count of join, which has already accounted the filters on DataSource,
		// i.e, rowCount equals to `countAfterAccess * selectivity`.
		countAfterAccess = rowCount / selectivity
	}
	ts.stats = &property.StatsInfo{
		// BlockScan as inner child of IndexJoin can return at most 1 tuple for each outer event.
		RowCount:     math.Min(1.0, countAfterAccess),
		StatsVersion: ds.stats.StatsVersion,
		// Cardinality would not be used in cost computation of IndexJoin, set leave it as default nil.
	}
	rowSize := ds.TblDefCausHists.GetBlockAvgRowSize(p.ctx, ds.TblDefCauss, ts.StoreType, true)
	sessVars := ds.ctx.GetStochastikVars()
	INTERLOCKTask := &INTERLOCKTask{
		blockPlan:         ts,
		indexPlanFinished: true,
		cst:               sessVars.ScanFactor * rowSize * ts.stats.RowCount,
		tblDefCausHists:   ds.TblDefCausHists,
		keepOrder:         ts.KeepOrder,
	}
	INTERLOCKTask.partitionInfo = PartitionInfo{
		PruningConds:        ds.allConds,
		PartitionNames:      ds.partitionNames,
		DeferredCausets:     ds.TblDefCauss,
		DeferredCausetNames: ds.names,
	}
	selStats := ts.stats.Scale(selectivity)
	ts.addPushedDownSelection(INTERLOCKTask, selStats)
	t := finishINTERLOCKTask(ds.ctx, INTERLOCKTask).(*rootTask)
	reader := t.p
	t.p = p.constructInnerUnionScan(us, reader)
	return t
}

func (p *LogicalJoin) constructInnerUnionScan(us *LogicalUnionScan, reader PhysicalPlan) PhysicalPlan {
	if us == nil {
		return reader
	}
	// Use `reader.stats` instead of `us.stats` because it should be more accurate. No need to specify
	// childrenReqProps now since we have got reader already.
	physicalUnionScan := PhysicalUnionScan{
		Conditions:     us.conditions,
		HandleDefCauss: us.handleDefCauss,
	}.Init(us.ctx, reader.statsInfo(), us.blockOffset, nil)
	physicalUnionScan.SetChildren(reader)
	return physicalUnionScan
}

// constructInnerIndexScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func (p *LogicalJoin) constructInnerIndexScanTask(
	ds *DataSource,
	path *soliton.AccessPath,
	filterConds []expression.Expression,
	outerJoinKeys []*expression.DeferredCauset,
	us *LogicalUnionScan,
	rangeInfo string,
	keepOrder bool,
	desc bool,
	rowCount float64,
	maxOneRow bool,
) task {
	// If `ds.blockInfo.GetPartitionInfo() != nil`,
	// it means the data source is a partition block reader.
	// If the inner task need to keep order, the partition block reader can't satisfy it.
	if keepOrder && ds.blockInfo.GetPartitionInfo() != nil {
		return nil
	}
	is := PhysicalIndexScan{
		Block:            ds.blockInfo,
		BlockAsName:      ds.BlockAsName,
		DBName:           ds.DBName,
		DeferredCausets:  ds.DeferredCausets,
		Index:            path.Index,
		IdxDefCauss:      path.IdxDefCauss,
		IdxDefCausLens:   path.IdxDefCausLens,
		dataSourceSchema: ds.schemaReplicant,
		KeepOrder:        keepOrder,
		Ranges:           ranger.FullRange(),
		rangeInfo:        rangeInfo,
		Desc:             desc,
		isPartition:      ds.isPartition,
		physicalBlockID:  ds.physicalBlockID,
	}.Init(ds.ctx, ds.blockOffset)
	INTERLOCK := &INTERLOCKTask{
		indexPlan:       is,
		tblDefCausHists: ds.TblDefCausHists,
		tblDefCauss:     ds.TblDefCauss,
		keepOrder:       is.KeepOrder,
	}
	INTERLOCK.partitionInfo = PartitionInfo{
		PruningConds:        ds.allConds,
		PartitionNames:      ds.partitionNames,
		DeferredCausets:     ds.TblDefCauss,
		DeferredCausetNames: ds.names,
	}
	if !ds.isCoveringIndex(ds.schemaReplicant.DeferredCausets, path.FullIdxDefCauss, path.FullIdxDefCausLens, is.Block) {
		// On this way, it's double read case.
		ts := PhysicalBlockScan{
			DeferredCausets: ds.DeferredCausets,
			Block:           is.Block,
			BlockAsName:     ds.BlockAsName,
			isPartition:     ds.isPartition,
			physicalBlockID: ds.physicalBlockID,
		}.Init(ds.ctx, ds.blockOffset)
		ts.schemaReplicant = is.dataSourceSchema.Clone()
		// If inner INTERLOCK task need keep order, the extraHandleDefCaus should be set.
		if INTERLOCK.keepOrder && !ds.blockInfo.IsCommonHandle {
			INTERLOCK.extraHandleDefCaus, INTERLOCK.doubleReadNeedProj = ts.appendExtraHandleDefCaus(ds)
		}
		INTERLOCK.blockPlan = ts
	}
	if INTERLOCK.blockPlan != nil && ds.blockInfo.IsCommonHandle {
		INTERLOCK.commonHandleDefCauss = ds.commonHandleDefCauss
	}
	is.initSchema(append(path.FullIdxDefCauss, ds.commonHandleDefCauss...), INTERLOCK.blockPlan != nil)
	indexConds, tblConds := ds.splitIndexFilterConditions(filterConds, path.FullIdxDefCauss, path.FullIdxDefCausLens, ds.blockInfo)
	// Specially handle cases when input rowCount is 0, which can only happen in 2 scenarios:
	// - estimated event count of outer plan is 0;
	// - estimated event count of inner "DataSource + filters" is 0;
	// if it is the first case, it does not matter what event count we set for inner task, since the cost of index join would
	// always be 0 then;
	// if it is the second case, HashJoin should always be cheaper than IndexJoin then, so we set event count of inner task
	// to block size, to simply make it more expensive.
	if rowCount <= 0 {
		rowCount = ds.blockStats.RowCount
	}
	if maxOneRow {
		// Theoretically, this line is unnecessary because event count estimation of join should guarantee rowCount is not larger
		// than 1.0; however, there may be rowCount larger than 1.0 in reality, e.g, pseudo statistics cases, which does not reflect
		// unique constraint in NDV.
		rowCount = math.Min(rowCount, 1.0)
	}
	tmpPath := &soliton.AccessPath{
		IndexFilters:     indexConds,
		BlockFilters:     tblConds,
		CountAfterIndex:  rowCount,
		CountAfterAccess: rowCount,
	}
	// Assume equal conditions used by index join and other conditions are independent.
	if len(tblConds) > 0 {
		selectivity, _, err := ds.blockStats.HistDefCausl.Selectivity(ds.ctx, tblConds, ds.possibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("block", ds.BlockAsName.L))
			selectivity = SelectionFactor
		}
		// rowCount is computed from result event count of join, which has already accounted the filters on DataSource,
		// i.e, rowCount equals to `countAfterIndex * selectivity`.
		cnt := rowCount / selectivity
		if maxOneRow {
			cnt = math.Min(cnt, 1.0)
		}
		tmpPath.CountAfterIndex = cnt
		tmpPath.CountAfterAccess = cnt
	}
	if len(indexConds) > 0 {
		selectivity, _, err := ds.blockStats.HistDefCausl.Selectivity(ds.ctx, indexConds, ds.possibleAccessPaths)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("block", ds.BlockAsName.L))
			selectivity = SelectionFactor
		}
		cnt := tmpPath.CountAfterIndex / selectivity
		if maxOneRow {
			cnt = math.Min(cnt, 1.0)
		}
		tmpPath.CountAfterAccess = cnt
	}
	is.stats = ds.blockStats.ScaleByExpectCnt(tmpPath.CountAfterAccess)
	rowSize := is.indexScanRowSize(path.Index, ds, true)
	sessVars := ds.ctx.GetStochastikVars()
	INTERLOCK.cst = tmpPath.CountAfterAccess * rowSize * sessVars.ScanFactor
	finalStats := ds.blockStats.ScaleByExpectCnt(rowCount)
	is.addPushedDownSelection(INTERLOCK, ds, tmpPath, finalStats)
	t := finishINTERLOCKTask(ds.ctx, INTERLOCK).(*rootTask)
	reader := t.p
	t.p = p.constructInnerUnionScan(us, reader)
	return t
}

var symmetriINTERLOCK = map[string]string{
	ast.LT: ast.GT,
	ast.GE: ast.LE,
	ast.GT: ast.LT,
	ast.LE: ast.GE,
}

// DefCausWithCmpFuncManager is used in index join to handle the column with compare functions(>=, >, <, <=).
// It stores the compare functions and build ranges in execution phase.
type DefCausWithCmpFuncManager struct {
	TargetDefCaus         *expression.DeferredCauset
	colLength             int
	OpType                []string
	opArg                 []expression.Expression
	TmpConstant           []*expression.Constant
	affectedDefCausSchema *expression.Schema
	compareFuncs          []chunk.CompareFunc
}

func (cwc *DefCausWithCmpFuncManager) appendNewExpr(opName string, arg expression.Expression, affectedDefCauss []*expression.DeferredCauset) {
	cwc.OpType = append(cwc.OpType, opName)
	cwc.opArg = append(cwc.opArg, arg)
	cwc.TmpConstant = append(cwc.TmpConstant, &expression.Constant{RetType: cwc.TargetDefCaus.RetType})
	for _, col := range affectedDefCauss {
		if cwc.affectedDefCausSchema.Contains(col) {
			continue
		}
		cwc.compareFuncs = append(cwc.compareFuncs, chunk.GetCompareFunc(col.RetType))
		cwc.affectedDefCausSchema.Append(col)
	}
}

// CompareRow compares the rows for deduplicate.
func (cwc *DefCausWithCmpFuncManager) CompareRow(lhs, rhs chunk.Row) int {
	for i, col := range cwc.affectedDefCausSchema.DeferredCausets {
		ret := cwc.compareFuncs[i](lhs, col.Index, rhs, col.Index)
		if ret != 0 {
			return ret
		}
	}
	return 0
}

// BuildRangesByRow will build range of the given event. It will eval each function's arg then call BuildRange.
func (cwc *DefCausWithCmpFuncManager) BuildRangesByRow(ctx stochastikctx.Context, event chunk.Row) ([]*ranger.Range, error) {
	exprs := make([]expression.Expression, len(cwc.OpType))
	for i, opType := range cwc.OpType {
		constantArg, err := cwc.opArg[i].Eval(event)
		if err != nil {
			return nil, err
		}
		cwc.TmpConstant[i].Value = constantArg
		newExpr, err := expression.NewFunction(ctx, opType, types.NewFieldType(allegrosql.TypeTiny), cwc.TargetDefCaus, cwc.TmpConstant[i])
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, newExpr)
	}
	ranges, err := ranger.BuildDeferredCausetRange(exprs, ctx.GetStochastikVars().StmtCtx, cwc.TargetDefCaus.RetType, cwc.colLength)
	if err != nil {
		return nil, err
	}
	return ranges, nil
}

func (cwc *DefCausWithCmpFuncManager) resolveIndices(schemaReplicant *expression.Schema) (err error) {
	for i := range cwc.opArg {
		cwc.opArg[i], err = cwc.opArg[i].ResolveIndices(schemaReplicant)
		if err != nil {
			return err
		}
	}
	return nil
}

// String implements Stringer interface.
func (cwc *DefCausWithCmpFuncManager) String() string {
	buffer := bytes.NewBufferString("")
	for i := range cwc.OpType {
		buffer.WriteString(fmt.Sprintf("%v(%v, %v)", cwc.OpType[i], cwc.TargetDefCaus, cwc.opArg[i]))
		if i < len(cwc.OpType)-1 {
			buffer.WriteString(" ")
		}
	}
	return buffer.String()
}

func (ijHelper *indexJoinBuildHelper) resetContextForIndex(innerKeys []*expression.DeferredCauset, idxDefCauss []*expression.DeferredCauset, colLens []int) {
	tmpSchema := expression.NewSchema(innerKeys...)
	ijHelper.curIdxOff2KeyOff = make([]int, len(idxDefCauss))
	ijHelper.curNotUsedIndexDefCauss = make([]*expression.DeferredCauset, 0, len(idxDefCauss))
	ijHelper.curNotUsedDefCausLens = make([]int, 0, len(idxDefCauss))
	for i, idxDefCaus := range idxDefCauss {
		ijHelper.curIdxOff2KeyOff[i] = tmpSchema.DeferredCausetIndex(idxDefCaus)
		if ijHelper.curIdxOff2KeyOff[i] >= 0 {
			continue
		}
		ijHelper.curNotUsedIndexDefCauss = append(ijHelper.curNotUsedIndexDefCauss, idxDefCaus)
		ijHelper.curNotUsedDefCausLens = append(ijHelper.curNotUsedDefCausLens, colLens[i])
	}
}

// findUsefulEqAndInFilters analyzes the pushedDownConds held by inner child and split them to three parts.
// usefulEqOrInFilters is the continuous eq/in conditions on current unused index columns.
// uselessFilters is the conditions which cannot be used for building ranges.
// remainingRangeCandidates is the other conditions for future use.
func (ijHelper *indexJoinBuildHelper) findUsefulEqAndInFilters(innerPlan *DataSource) (usefulEqOrInFilters, uselessFilters, remainingRangeCandidates []expression.Expression) {
	uselessFilters = make([]expression.Expression, 0, len(innerPlan.pushedDownConds))
	var remainedEqOrIn []expression.Expression
	// Extract the eq/in functions of possible join key.
	// you can see the comment of ExtractEqAndInCondition to get the meaning of the second return value.
	usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates, _ = ranger.ExtractEqAndInCondition(
		innerPlan.ctx, innerPlan.pushedDownConds,
		ijHelper.curNotUsedIndexDefCauss,
		ijHelper.curNotUsedDefCausLens,
	)
	uselessFilters = append(uselessFilters, remainedEqOrIn...)
	return usefulEqOrInFilters, uselessFilters, remainingRangeCandidates
}

// buildLastDefCausManager analyze the `OtherConditions` of join to see whether there're some filters can be used in manager.
// The returned value is just for outputting explain information
func (ijHelper *indexJoinBuildHelper) buildLastDefCausManager(nextDefCaus *expression.DeferredCauset,
	innerPlan *DataSource, cwc *DefCausWithCmpFuncManager) []expression.Expression {
	var lastDefCausAccesses []expression.Expression
loopOtherConds:
	for _, filter := range ijHelper.join.OtherConditions {
		sf, ok := filter.(*expression.ScalarFunction)
		if !ok || !(sf.FuncName.L == ast.LE || sf.FuncName.L == ast.LT || sf.FuncName.L == ast.GE || sf.FuncName.L == ast.GT) {
			continue
		}
		var funcName string
		var anotherArg expression.Expression
		if lDefCaus, ok := sf.GetArgs()[0].(*expression.DeferredCauset); ok && lDefCaus.Equal(nil, nextDefCaus) {
			anotherArg = sf.GetArgs()[1]
			funcName = sf.FuncName.L
		} else if rDefCaus, ok := sf.GetArgs()[1].(*expression.DeferredCauset); ok && rDefCaus.Equal(nil, nextDefCaus) {
			anotherArg = sf.GetArgs()[0]
			// The column manager always build expression in the form of col op arg1.
			// So we need use the symmetric one of the current function.
			funcName = symmetriINTERLOCK[sf.FuncName.L]
		} else {
			continue
		}
		affectedDefCauss := expression.ExtractDeferredCausets(anotherArg)
		if len(affectedDefCauss) == 0 {
			continue
		}
		for _, col := range affectedDefCauss {
			if innerPlan.schemaReplicant.Contains(col) {
				continue loopOtherConds
			}
		}
		lastDefCausAccesses = append(lastDefCausAccesses, sf)
		cwc.appendNewExpr(funcName, anotherArg, affectedDefCauss)
	}
	return lastDefCausAccesses
}

// removeUselessEqAndInFunc removes the useless eq/in conditions. It's designed for the following case:
//   t1 join t2 on t1.a=t2.a and t1.c=t2.c where t1.b > t2.b-10 and t1.b < t2.b+10 there's index(a, b, c) on t1.
//   In this case the curIdxOff2KeyOff is [0 -1 1] and the notKeyEqAndIn is [].
//   It's clearly that the column c cannot be used to access data. So we need to remove it and reset the IdxOff2KeyOff to
//   [0 -1 -1].
//   So that we can use t1.a=t2.a and t1.b > t2.b-10 and t1.b < t2.b+10 to build ranges then access data.
func (ijHelper *indexJoinBuildHelper) removeUselessEqAndInFunc(
	idxDefCauss []*expression.DeferredCauset,
	notKeyEqAndIn []expression.Expression) (
	usefulEqAndIn, uselessOnes []expression.Expression,
) {
	ijHelper.curPossibleUsedKeys = make([]*expression.DeferredCauset, 0, len(idxDefCauss))
	for idxDefCausPos, notKeyDefCausPos := 0, 0; idxDefCausPos < len(idxDefCauss); idxDefCausPos++ {
		if ijHelper.curIdxOff2KeyOff[idxDefCausPos] != -1 {
			ijHelper.curPossibleUsedKeys = append(ijHelper.curPossibleUsedKeys, idxDefCauss[idxDefCausPos])
			continue
		}
		if notKeyDefCausPos < len(notKeyEqAndIn) && ijHelper.curNotUsedIndexDefCauss[notKeyDefCausPos].Equal(nil, idxDefCauss[idxDefCausPos]) {
			notKeyDefCausPos++
			continue
		}
		for i := idxDefCausPos + 1; i < len(idxDefCauss); i++ {
			ijHelper.curIdxOff2KeyOff[i] = -1
		}
		remained := make([]expression.Expression, 0, len(notKeyEqAndIn)-notKeyDefCausPos)
		remained = append(remained, notKeyEqAndIn[notKeyDefCausPos:]...)
		notKeyEqAndIn = notKeyEqAndIn[:notKeyDefCausPos]
		return notKeyEqAndIn, remained
	}
	return notKeyEqAndIn, nil
}

func (ijHelper *indexJoinBuildHelper) analyzeLookUpFilters(path *soliton.AccessPath, innerPlan *DataSource, innerJoinKeys []*expression.DeferredCauset) (emptyRange bool, err error) {
	if len(path.IdxDefCauss) == 0 {
		return false, nil
	}
	accesses := make([]expression.Expression, 0, len(path.IdxDefCauss))
	ijHelper.resetContextForIndex(innerJoinKeys, path.IdxDefCauss, path.IdxDefCausLens)
	notKeyEqAndIn, remained, rangeFilterCandidates := ijHelper.findUsefulEqAndInFilters(innerPlan)
	var remainedEqAndIn []expression.Expression
	notKeyEqAndIn, remainedEqAndIn = ijHelper.removeUselessEqAndInFunc(path.IdxDefCauss, notKeyEqAndIn)
	matchedKeyCnt := len(ijHelper.curPossibleUsedKeys)
	// If no join key is matched while join keys actually are not empty. We don't choose index join for now.
	if matchedKeyCnt <= 0 && len(innerJoinKeys) > 0 {
		return false, nil
	}
	accesses = append(accesses, notKeyEqAndIn...)
	remained = append(remained, remainedEqAndIn...)
	lastDefCausPos := matchedKeyCnt + len(notKeyEqAndIn)
	// There should be some equal conditions. But we don't need that there must be some join key in accesses here.
	// A more strict check is applied later.
	if lastDefCausPos <= 0 {
		return false, nil
	}
	// If all the index columns are covered by eq/in conditions, we don't need to consider other conditions anymore.
	if lastDefCausPos == len(path.IdxDefCauss) {
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1. And t2 has index(a, b).
		//      If we don't have the following check, MilevaDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		remained = append(remained, rangeFilterCandidates...)
		ranges, emptyRange, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, false)
		if err != nil {
			return false, err
		}
		if emptyRange {
			return true, nil
		}
		ijHelper.uFIDelateBestChoice(ranges, path, accesses, remained, nil)
		return false, nil
	}
	lastPossibleDefCaus := path.IdxDefCauss[lastDefCausPos]
	lastDefCausManager := &DefCausWithCmpFuncManager{
		TargetDefCaus:         lastPossibleDefCaus,
		colLength:             path.IdxDefCausLens[lastDefCausPos],
		affectedDefCausSchema: expression.NewSchema(),
	}
	lastDefCausAccess := ijHelper.buildLastDefCausManager(lastPossibleDefCaus, innerPlan, lastDefCausManager)
	// If the column manager holds no expression, then we fallback to find whether there're useful normal filters
	if len(lastDefCausAccess) == 0 {
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1 and t2.c > 10 and t2.c < 20. And t2 has index(a, b, c).
		//      If we don't have the following check, MilevaDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		colAccesses, colRemained := ranger.DetachCondsForDeferredCauset(ijHelper.join.ctx, rangeFilterCandidates, lastPossibleDefCaus)
		var ranges, nextDefCausRange []*ranger.Range
		var err error
		if len(colAccesses) > 0 {
			nextDefCausRange, err = ranger.BuildDeferredCausetRange(colAccesses, ijHelper.join.ctx.GetStochastikVars().StmtCtx, lastPossibleDefCaus.RetType, path.IdxDefCausLens[lastDefCausPos])
			if err != nil {
				return false, err
			}
		}
		ranges, emptyRange, err = ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nextDefCausRange, false)
		if err != nil {
			return false, err
		}
		if emptyRange {
			return true, nil
		}
		remained = append(remained, colRemained...)
		if path.IdxDefCausLens[lastDefCausPos] != types.UnspecifiedLength {
			remained = append(remained, colAccesses...)
		}
		accesses = append(accesses, colAccesses...)
		ijHelper.uFIDelateBestChoice(ranges, path, accesses, remained, nil)
		return false, nil
	}
	accesses = append(accesses, lastDefCausAccess...)
	remained = append(remained, rangeFilterCandidates...)
	ranges, emptyRange, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, true)
	if err != nil {
		return false, err
	}
	if emptyRange {
		return true, nil
	}
	ijHelper.uFIDelateBestChoice(ranges, path, accesses, remained, lastDefCausManager)
	return false, nil
}

func (ijHelper *indexJoinBuildHelper) uFIDelateBestChoice(ranges []*ranger.Range, path *soliton.AccessPath, accesses,
	remained []expression.Expression, lastDefCausManager *DefCausWithCmpFuncManager) {
	// We choose the index by the number of used columns of the range, the much the better.
	// Notice that there may be the cases like `t1.a=t2.a and b > 2 and b < 1`. So ranges can be nil though the conditions are valid.
	// But obviously when the range is nil, we don't need index join.
	if len(ranges) > 0 && len(ranges[0].LowVal) > ijHelper.maxUsedDefCauss {
		ijHelper.chosenPath = path
		ijHelper.maxUsedDefCauss = len(ranges[0].LowVal)
		ijHelper.chosenRanges = ranges
		ijHelper.chosenAccess = accesses
		ijHelper.chosenRemained = remained
		ijHelper.idxOff2KeyOff = ijHelper.curIdxOff2KeyOff
		ijHelper.lastDefCausManager = lastDefCausManager
	}
}

func (ijHelper *indexJoinBuildHelper) buildTemplateRange(matchedKeyCnt int, eqAndInFuncs []expression.Expression, nextDefCausRange []*ranger.Range, haveExtraDefCaus bool) (ranges []*ranger.Range, emptyRange bool, err error) {
	pointLength := matchedKeyCnt + len(eqAndInFuncs)
	if nextDefCausRange != nil {
		for _, colRan := range nextDefCausRange {
			// The range's exclude status is the same with last col's.
			ran := &ranger.Range{
				LowVal:      make([]types.Causet, pointLength, pointLength+1),
				HighVal:     make([]types.Causet, pointLength, pointLength+1),
				LowExclude:  colRan.LowExclude,
				HighExclude: colRan.HighExclude,
			}
			ran.LowVal = append(ran.LowVal, colRan.LowVal[0])
			ran.HighVal = append(ran.HighVal, colRan.HighVal[0])
			ranges = append(ranges, ran)
		}
	} else if haveExtraDefCaus {
		// Reserve a position for the last col.
		ranges = append(ranges, &ranger.Range{
			LowVal:  make([]types.Causet, pointLength+1),
			HighVal: make([]types.Causet, pointLength+1),
		})
	} else {
		ranges = append(ranges, &ranger.Range{
			LowVal:  make([]types.Causet, pointLength),
			HighVal: make([]types.Causet, pointLength),
		})
	}
	sc := ijHelper.join.ctx.GetStochastikVars().StmtCtx
	for i, j := 0, 0; j < len(eqAndInFuncs); i++ {
		// This position is occupied by join key.
		if ijHelper.curIdxOff2KeyOff[i] != -1 {
			continue
		}
		oneDeferredCausetRan, err := ranger.BuildDeferredCausetRange([]expression.Expression{eqAndInFuncs[j]}, sc, ijHelper.curNotUsedIndexDefCauss[j].RetType, ijHelper.curNotUsedDefCausLens[j])
		if err != nil {
			return nil, false, err
		}
		if len(oneDeferredCausetRan) == 0 {
			return nil, true, nil
		}
		for _, ran := range ranges {
			ran.LowVal[i] = oneDeferredCausetRan[0].LowVal[0]
			ran.HighVal[i] = oneDeferredCausetRan[0].HighVal[0]
		}
		curRangeLen := len(ranges)
		for ranIdx := 1; ranIdx < len(oneDeferredCausetRan); ranIdx++ {
			newRanges := make([]*ranger.Range, 0, curRangeLen)
			for oldRangeIdx := 0; oldRangeIdx < curRangeLen; oldRangeIdx++ {
				newRange := ranges[oldRangeIdx].Clone()
				newRange.LowVal[i] = oneDeferredCausetRan[ranIdx].LowVal[0]
				newRange.HighVal[i] = oneDeferredCausetRan[ranIdx].HighVal[0]
				newRanges = append(newRanges, newRange)
			}
			ranges = append(ranges, newRanges...)
		}
		j++
	}
	return ranges, false, nil
}

// tryToGetIndexJoin will get index join by hints. If we can generate a valid index join by hint, the second return value
// will be true, which means we force to choose this index join. Otherwise we will select a join algorithm with min-cost.
func (p *LogicalJoin) tryToGetIndexJoin(prop *property.PhysicalProperty) (indexJoins []PhysicalPlan, canForced bool) {
	inljRightOuter := (p.preferJoinType & preferLeftAsINLJInner) > 0
	inljLeftOuter := (p.preferJoinType & preferRightAsINLJInner) > 0
	hasINLJHint := inljLeftOuter || inljRightOuter

	inlhjRightOuter := (p.preferJoinType & preferLeftAsINLHJInner) > 0
	inlhjLeftOuter := (p.preferJoinType & preferRightAsINLHJInner) > 0
	hasINLHJHint := inlhjLeftOuter || inlhjRightOuter

	inlmjRightOuter := (p.preferJoinType & preferLeftAsINLMJInner) > 0
	inlmjLeftOuter := (p.preferJoinType & preferRightAsINLMJInner) > 0
	hasINLMJHint := inlmjLeftOuter || inlmjRightOuter

	forceLeftOuter := inljLeftOuter || inlhjLeftOuter || inlmjLeftOuter
	forceRightOuter := inljRightOuter || inlhjRightOuter || inlmjRightOuter
	needForced := forceLeftOuter || forceRightOuter

	defer func() {
		// refine error message
		// If the required property is not empty, we will enforce it and try the hint again.
		// So we only need to generate warning message when the property is empty.
		if !canForced && needForced && prop.IsEmpty() {
			// Construct warning message prefix.
			var errMsg string
			switch {
			case hasINLJHint:
				errMsg = "Optimizer Hint INL_JOIN or MilevaDB_INLJ is inapplicable"
			case hasINLHJHint:
				errMsg = "Optimizer Hint INL_HASH_JOIN is inapplicable"
			case hasINLMJHint:
				errMsg = "Optimizer Hint INL_MERGE_JOIN is inapplicable"
			}
			if p.hintInfo != nil {
				t := p.hintInfo.indexNestedLoopJoinBlocks
				switch {
				case len(t.inljBlocks) != 0:
					errMsg = fmt.Sprintf("Optimizer Hint %s or %s is inapplicable",
						restore2JoinHint(HintINLJ, t.inljBlocks), restore2JoinHint(MilevaDBIndexNestedLoopJoin, t.inljBlocks))
				case len(t.inlhjBlocks) != 0:
					errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", restore2JoinHint(HintINLHJ, t.inlhjBlocks))
				case len(t.inlmjBlocks) != 0:
					errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", restore2JoinHint(HintINLMJ, t.inlmjBlocks))
				}
			}

			// Append inapplicable reason.
			if len(p.EqualConditions) == 0 {
				errMsg += " without column equal ON condition"
			}

			// Generate warning message to client.
			warning := ErrInternal.GenWithStack(errMsg)
			p.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
		}
	}()

	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, LeftOuterJoin:
		supportLeftOuter = true
	case RightOuterJoin:
		supportRightOuter = true
	case InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}

	var allLeftOuterJoins, allRightOuterJoins, forcedLeftOuterJoins, forcedRightOuterJoins []PhysicalPlan
	if supportLeftOuter {
		allLeftOuterJoins = p.getIndexJoinByOuterIdx(prop, 0)
		forcedLeftOuterJoins = make([]PhysicalPlan, 0, len(allLeftOuterJoins))
		for _, j := range allLeftOuterJoins {
			switch j.(type) {
			case *PhysicalIndexJoin:
				if inljLeftOuter {
					forcedLeftOuterJoins = append(forcedLeftOuterJoins, j)
				}
			case *PhysicalIndexHashJoin:
				if inlhjLeftOuter {
					forcedLeftOuterJoins = append(forcedLeftOuterJoins, j)
				}
			case *PhysicalIndexMergeJoin:
				if inlmjLeftOuter {
					forcedLeftOuterJoins = append(forcedLeftOuterJoins, j)
				}
			}
		}
		switch {
		case len(forcedLeftOuterJoins) == 0 && !supportRightOuter:
			return allLeftOuterJoins, false
		case len(forcedLeftOuterJoins) != 0 && (!supportRightOuter || (forceLeftOuter && !forceRightOuter)):
			return forcedLeftOuterJoins, true
		}
	}
	if supportRightOuter {
		allRightOuterJoins = p.getIndexJoinByOuterIdx(prop, 1)
		forcedRightOuterJoins = make([]PhysicalPlan, 0, len(allRightOuterJoins))
		for _, j := range allRightOuterJoins {
			switch j.(type) {
			case *PhysicalIndexJoin:
				if inljRightOuter {
					forcedRightOuterJoins = append(forcedRightOuterJoins, j)
				}
			case *PhysicalIndexHashJoin:
				if inlhjRightOuter {
					forcedRightOuterJoins = append(forcedRightOuterJoins, j)
				}
			case *PhysicalIndexMergeJoin:
				if inlmjRightOuter {
					forcedRightOuterJoins = append(forcedRightOuterJoins, j)
				}
			}
		}
		switch {
		case len(forcedRightOuterJoins) == 0 && !supportLeftOuter:
			return allRightOuterJoins, false
		case len(forcedRightOuterJoins) != 0 && (!supportLeftOuter || (forceRightOuter && !forceLeftOuter)):
			return forcedRightOuterJoins, true
		}
	}

	canForceLeft := len(forcedLeftOuterJoins) != 0 && forceLeftOuter
	canForceRight := len(forcedRightOuterJoins) != 0 && forceRightOuter
	canForced = canForceLeft || canForceRight
	if canForced {
		return append(forcedLeftOuterJoins, forcedRightOuterJoins...), true
	}
	return append(allLeftOuterJoins, allRightOuterJoins...), false
}

// LogicalJoin can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func (p *LogicalJoin) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	failpoint.Inject("MockOnlyEnableIndexHashJoin", func(val failpoint.Value) {
		if val.(bool) {
			indexJoins, _ := p.tryToGetIndexJoin(prop)
			failpoint.Return(indexJoins, true)
		}
	})

	if prop.IsFlashOnlyProp() && ((p.preferJoinType&preferBCJoin) == 0 && p.preferJoinType > 0) {
		return nil, false
	}
	joins := make([]PhysicalPlan, 0, 8)
	if p.ctx.GetStochastikVars().AllowBCJ {
		broadCastJoins := p.tryToGetBroadCastJoin(prop)
		if (p.preferJoinType & preferBCJoin) > 0 {
			return broadCastJoins, true
		}
		joins = append(joins, broadCastJoins...)
	}
	if prop.IsFlashOnlyProp() {
		return joins, true
	}

	mergeJoins := p.GetMergeJoin(prop, p.schemaReplicant, p.Stats(), p.children[0].statsInfo(), p.children[1].statsInfo())
	if (p.preferJoinType&preferMergeJoin) > 0 && len(mergeJoins) > 0 {
		return mergeJoins, true
	}
	joins = append(joins, mergeJoins...)

	indexJoins, forced := p.tryToGetIndexJoin(prop)
	if forced {
		return indexJoins, true
	}
	joins = append(joins, indexJoins...)

	hashJoins := p.getHashJoins(prop)
	if (p.preferJoinType&preferHashJoin) > 0 && len(hashJoins) > 0 {
		return hashJoins, true
	}
	joins = append(joins, hashJoins...)

	if p.preferJoinType > 0 {
		// If we reach here, it means we have a hint that doesn't work.
		// It might be affected by the required property, so we enforce
		// this property and try the hint again.
		return joins, false
	}
	return joins, true
}

func (p *LogicalJoin) tryToGetBroadCastJoin(prop *property.PhysicalProperty) []PhysicalPlan {
	/// todo remove this restriction after join on new collation is supported in TiFlash
	if collate.NewDefCauslationEnabled() {
		return nil
	}
	if !prop.IsEmpty() {
		return nil
	}
	if prop.TaskTp != property.RootTaskType && !prop.IsFlashOnlyProp() {
		return nil
	}

	// Disable broadcast join on partition block for TiFlash.
	for _, child := range p.children {
		if ds, isDataSource := child.(*DataSource); isDataSource {
			if ds.blockInfo.GetPartitionInfo() != nil {
				return nil
			}
		}
	}

	// for left join the global idx must be 1, and for right join the global idx must be 0
	if (p.JoinType != InnerJoin && p.JoinType != LeftOuterJoin && p.JoinType != RightOuterJoin) || len(p.LeftConditions) != 0 || len(p.RightConditions) != 0 || len(p.OtherConditions) != 0 || len(p.EqualConditions) == 0 {
		return nil
	}

	if hasPrefer, idx := p.getPreferredBCJLocalIndex(); hasPrefer {
		if (idx == 0 && p.JoinType == RightOuterJoin) || (idx == 1 && p.JoinType == LeftOuterJoin) {
			return nil
		}
		return p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 1-idx)
	}
	if p.JoinType == InnerJoin {
		results := p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 0)
		results = append(results, p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 1)...)
		return results
	} else if p.JoinType == LeftOuterJoin {
		return p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 1)
	}
	return p.tryToGetBroadCastJoinByPreferGlobalIdx(prop, 0)
}

func (p *LogicalJoin) tryToGetBroadCastJoinByPreferGlobalIdx(prop *property.PhysicalProperty, preferredGlobalIndex int) []PhysicalPlan {
	lkeys, rkeys, _, _ := p.GetJoinKeys()
	baseJoin := basePhysicalJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		DefaultValues:   p.DefaultValues,
		LeftJoinKeys:    lkeys,
		RightJoinKeys:   rkeys,
	}

	preferredBuildIndex := 0
	if p.children[0].statsInfo().Count() > p.children[1].statsInfo().Count() {
		preferredBuildIndex = 1
	}
	baseJoin.InnerChildIdx = preferredBuildIndex
	childrenReqProps := make([]*property.PhysicalProperty, 2)
	childrenReqProps[preferredGlobalIndex] = &property.PhysicalProperty{TaskTp: property.INTERLOCKTiFlashGlobalReadTaskType, ExpectedCnt: math.MaxFloat64}
	if prop.TaskTp == property.INTERLOCKTiFlashGlobalReadTaskType {
		childrenReqProps[1-preferredGlobalIndex] = &property.PhysicalProperty{TaskTp: property.INTERLOCKTiFlashGlobalReadTaskType, ExpectedCnt: math.MaxFloat64}
	} else {
		childrenReqProps[1-preferredGlobalIndex] = &property.PhysicalProperty{TaskTp: property.INTERLOCKTiFlashLocalReadTaskType, ExpectedCnt: math.MaxFloat64}
	}
	if prop.ExpectedCnt < p.stats.RowCount {
		expCntScale := prop.ExpectedCnt / p.stats.RowCount
		childrenReqProps[1-baseJoin.InnerChildIdx].ExpectedCnt = p.children[1-baseJoin.InnerChildIdx].statsInfo().RowCount * expCntScale
	}

	join := PhysicalBroadCastJoin{
		basePhysicalJoin: baseJoin,
		globalChildIndex: preferredGlobalIndex,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, childrenReqProps...)
	return []PhysicalPlan{join}
}

// TryToGetChildProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *LogicalProjection) TryToGetChildProp(prop *property.PhysicalProperty) (*property.PhysicalProperty, bool) {
	if prop.IsFlashOnlyProp() {
		return nil, false
	}
	newProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, ExpectedCnt: prop.ExpectedCnt}
	newDefCauss := make([]property.Item, 0, len(prop.Items))
	for _, col := range prop.Items {
		idx := p.schemaReplicant.DeferredCausetIndex(col.DefCaus)
		switch expr := p.Exprs[idx].(type) {
		case *expression.DeferredCauset:
			newDefCauss = append(newDefCauss, property.Item{DefCaus: expr, Desc: col.Desc})
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.Items = newDefCauss
	return newProp, true
}

func (p *LogicalProjection) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	newProp, ok := p.TryToGetChildProp(prop)
	if !ok {
		return nil, true
	}
	proj := PhysicalProjection{
		Exprs:                        p.Exprs,
		CalculateNoDelay:             p.CalculateNoDelay,
		AvoidDeferredCausetEvaluator: p.AvoidDeferredCausetEvaluator,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, newProp)
	proj.SetSchema(p.schemaReplicant)
	return []PhysicalPlan{proj}, true
}

func (lt *LogicalTopN) canPushToINTERLOCK() bool {
	// At present, only Aggregation, Limit, TopN can be pushed to INTERLOCK task, and Projection will be supported in the future.
	// When we push task to interlock, finishINTERLOCKTask will close the INTERLOCK task and create a root task in the current implementation.
	// Thus, we can't push two different tasks to interlock now, and can only push task to interlock when the child is Datasource.

	// TODO: develop this function after supporting push several tasks to INTERLOCKrecessor and supporting Projection to interlock.
	_, ok := lt.children[0].(*DataSource)
	return ok
}

func (lt *LogicalTopN) getPhysTopN(prop *property.PhysicalProperty) []PhysicalPlan {
	if lt.limitHints.preferLimitToINTERLOCK {
		if !lt.canPushToINTERLOCK() {
			errMsg := "Optimizer Hint LIMIT_TO_INTERLOCK is inapplicable"
			warning := ErrInternal.GenWithStack(errMsg)
			lt.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
			lt.limitHints.preferLimitToINTERLOCK = false
		}
	}
	allTaskTypes := []property.TaskType{property.INTERLOCKSingleReadTaskType, property.CoFIDeloubleReadTaskType}
	if !lt.limitHints.preferLimitToINTERLOCK {
		allTaskTypes = append(allTaskTypes, property.RootTaskType)
	}
	ret := make([]PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: math.MaxFloat64}
		topN := PhysicalTopN{
			ByItems: lt.ByItems,
			Count:   lt.Count,
			Offset:  lt.Offset,
		}.Init(lt.ctx, lt.stats, lt.blockOffset, resultProp)
		ret = append(ret, topN)
	}
	return ret
}

func (lt *LogicalTopN) getPhysLimits(prop *property.PhysicalProperty) []PhysicalPlan {
	p, canPass := GetPropByOrderByItems(lt.ByItems)
	if !canPass {
		return nil
	}

	if lt.limitHints.preferLimitToINTERLOCK {
		if !lt.canPushToINTERLOCK() {
			errMsg := "Optimizer Hint LIMIT_TO_INTERLOCK is inapplicable"
			warning := ErrInternal.GenWithStack(errMsg)
			lt.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
			lt.limitHints.preferLimitToINTERLOCK = false
		}
	}

	allTaskTypes := []property.TaskType{property.INTERLOCKSingleReadTaskType, property.CoFIDeloubleReadTaskType}
	if !lt.limitHints.preferLimitToINTERLOCK {
		allTaskTypes = append(allTaskTypes, property.RootTaskType)
	}
	ret := make([]PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(lt.Count + lt.Offset), Items: p.Items}
		limit := PhysicalLimit{
			Count:  lt.Count,
			Offset: lt.Offset,
		}.Init(lt.ctx, lt.stats, lt.blockOffset, resultProp)
		ret = append(ret, limit)
	}
	return ret
}

// MatchItems checks if this prop's columns can match by items totally.
func MatchItems(p *property.PhysicalProperty, items []*soliton.ByItems) bool {
	if len(items) < len(p.Items) {
		return false
	}
	for i, col := range p.Items {
		sortItem := items[i]
		if sortItem.Desc != col.Desc || !sortItem.Expr.Equal(nil, col.DefCaus) {
			return false
		}
	}
	return true
}

func (lt *LogicalTopN) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if MatchItems(prop, lt.ByItems) {
		return append(lt.getPhysTopN(prop), lt.getPhysLimits(prop)...), true
	}
	return nil, true
}

// GetHashJoin is public for cascades planner.
func (la *LogicalApply) GetHashJoin(prop *property.PhysicalProperty) *PhysicalHashJoin {
	return la.LogicalJoin.getHashJoin(prop, 1, false)
}

func (la *LogicalApply) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if !prop.AllDefCaussFromSchema(la.children[0].Schema()) || prop.IsFlashOnlyProp() { // for convenient, we don't pass through any prop
		return nil, true
	}
	join := la.GetHashJoin(prop)
	var columns []*expression.DeferredCauset
	for _, colDeferredCauset := range la.CorDefCauss {
		columns = append(columns, &colDeferredCauset.DeferredCauset)
	}
	cacheHitRatio := 0.0
	if la.stats.RowCount != 0 {
		ndv := getCardinality(columns, la.schemaReplicant, la.stats)
		// for example, if there are 100 rows and the number of distinct values of these correlated columns
		// are 70, then we can assume 30 rows can hit the cache so the cache hit ratio is 1 - (70/100) = 0.3
		cacheHitRatio = 1 - (ndv / la.stats.RowCount)
	}

	var canUseCache bool
	if cacheHitRatio > 0.1 && la.ctx.GetStochastikVars().NestedLoopJoinCacheCapacity > 0 {
		canUseCache = true
	} else {
		canUseCache = false
	}

	apply := PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.CorDefCauss,
		CanUseCache:      canUseCache,
	}.Init(la.ctx,
		la.stats.ScaleByExpectCnt(prop.ExpectedCnt),
		la.blockOffset,
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: prop.Items},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	apply.SetSchema(la.schemaReplicant)
	return []PhysicalPlan{apply}, true
}

func (p *LogicalWindow) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if prop.IsFlashOnlyProp() {
		return nil, true
	}
	var byItems []property.Item
	byItems = append(byItems, p.PartitionBy...)
	byItems = append(byItems, p.OrderBy...)
	childProperty := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: byItems, Enforced: true}
	if !prop.IsPrefix(childProperty) {
		return nil, true
	}
	window := PhysicalWindow{
		WindowFuncDescs: p.WindowFuncDescs,
		PartitionBy:     p.PartitionBy,
		OrderBy:         p.OrderBy,
		Frame:           p.Frame,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, childProperty)
	window.SetSchema(p.Schema())
	return []PhysicalPlan{window}, true
}

// exhaustPhysicalPlans is only for implementing interface. DataSource and Dual generate task in `findBestTask` directly.
func (p *baseLogicalPlan) exhaustPhysicalPlans(_ *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	panic("baseLogicalPlan.exhaustPhysicalPlans() should never be called.")
}

func (la *LogicalAggregation) canPushToINTERLOCK() bool {
	// At present, only Aggregation, Limit, TopN can be pushed to INTERLOCK task, and Projection will be supported in the future.
	// When we push task to interlock, finishINTERLOCKTask will close the INTERLOCK task and create a root task in the current implementation.
	// Thus, we can't push two different tasks to interlock now, and can only push task to interlock when the child is Datasource.

	// TODO: develop this function after supporting push several tasks to INTERLOCKrecessor and supporting Projection to interlock.
	_, ok := la.children[0].(*DataSource)
	return ok
}

func (la *LogicalAggregation) getEnforcedStreamAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	if prop.IsFlashOnlyProp() {
		return nil
	}
	_, desc := prop.AllSameOrder()
	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	enforcePosetDaggs := make([]PhysicalPlan, 0, len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt: math.Max(prop.ExpectedCnt*la.inputCount/la.stats.RowCount, prop.ExpectedCnt),
		Enforced:    true,
		Items:       property.ItemsFromDefCauss(la.groupByDefCauss, desc),
	}
	if !prop.IsPrefix(childProp) {
		return enforcePosetDaggs
	}
	taskTypes := []property.TaskType{property.INTERLOCKSingleReadTaskType, property.CoFIDeloubleReadTaskType}
	if la.HasDistinct() {
		// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
		// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
		if !la.canPushToINTERLOCK() || !la.ctx.GetStochastikVars().AllowDistinctAggPushDown {
			taskTypes = []property.TaskType{property.RootTaskType}
		}
	} else if !la.aggHints.preferAggToINTERLOCK {
		taskTypes = append(taskTypes, property.RootTaskType)
	}
	for _, taskTp := range taskTypes {
		INTERLOCKiedChildProperty := new(property.PhysicalProperty)
		*INTERLOCKiedChildProperty = *childProp // It's ok to not deep INTERLOCKy the "defcaus" field.
		INTERLOCKiedChildProperty.TaskTp = taskTp

		agg := basePhysicalAgg{
			GroupByItems: la.GroupByItems,
			AggFuncs:     la.AggFuncs,
		}.initForStream(la.ctx, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), la.blockOffset, INTERLOCKiedChildProperty)
		agg.SetSchema(la.schemaReplicant.Clone())
		enforcePosetDaggs = append(enforcePosetDaggs, agg)
	}
	return enforcePosetDaggs
}

func (la *LogicalAggregation) distinctArgsMeetsProperty() bool {
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.HasDistinct {
			for _, distinctArg := range aggFunc.Args {
				if !expression.Contains(la.GroupByItems, distinctArg) {
					return false
				}
			}
		}
	}
	return true
}

func (la *LogicalAggregation) getStreamAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	// TODO: support INTERLOCKTiFlash task type in stream agg
	if prop.IsFlashOnlyProp() {
		return nil
	}
	all, desc := prop.AllSameOrder()
	if !all {
		return nil
	}

	for _, aggFunc := range la.AggFuncs {
		if aggFunc.Mode == aggregation.FinalMode {
			return nil
		}
	}
	// group by a + b is not interested in any order.
	if len(la.groupByDefCauss) != len(la.GroupByItems) {
		return nil
	}

	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	streamAggs := make([]PhysicalPlan, 0, len(la.possibleProperties)*(len(allTaskTypes)-1)+len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt: math.Max(prop.ExpectedCnt*la.inputCount/la.stats.RowCount, prop.ExpectedCnt),
	}

	for _, possibleChildProperty := range la.possibleProperties {
		childProp.Items = property.ItemsFromDefCauss(possibleChildProperty[:len(la.groupByDefCauss)], desc)
		if !prop.IsPrefix(childProp) {
			continue
		}
		// The block read of "CoFIDeloubleReadTaskType" can't promises the sort
		// property that the stream aggregation required, no need to consider.
		taskTypes := []property.TaskType{property.INTERLOCKSingleReadTaskType}
		if la.HasDistinct() {
			// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
			// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
			if !la.canPushToINTERLOCK() || !la.ctx.GetStochastikVars().AllowDistinctAggPushDown {
				taskTypes = []property.TaskType{property.RootTaskType}
			} else {
				if !la.distinctArgsMeetsProperty() {
					continue
				}
			}
		} else if !la.aggHints.preferAggToINTERLOCK {
			taskTypes = append(taskTypes, property.RootTaskType)
		}
		for _, taskTp := range taskTypes {
			INTERLOCKiedChildProperty := new(property.PhysicalProperty)
			*INTERLOCKiedChildProperty = *childProp // It's ok to not deep INTERLOCKy the "defcaus" field.
			INTERLOCKiedChildProperty.TaskTp = taskTp

			agg := basePhysicalAgg{
				GroupByItems: la.GroupByItems,
				AggFuncs:     la.AggFuncs,
			}.initForStream(la.ctx, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), la.blockOffset, INTERLOCKiedChildProperty)
			agg.SetSchema(la.schemaReplicant.Clone())
			streamAggs = append(streamAggs, agg)
		}
	}
	// If STREAM_AGG hint is existed, it should consider enforce stream aggregation,
	// because we can't trust possibleChildProperty completely.
	if (la.aggHints.preferAggType & preferStreamAgg) > 0 {
		streamAggs = append(streamAggs, la.getEnforcedStreamAggs(prop)...)
	}
	return streamAggs
}

func (la *LogicalAggregation) getHashAggs(prop *property.PhysicalProperty) []PhysicalPlan {
	if !prop.IsEmpty() {
		return nil
	}
	hashAggs := make([]PhysicalPlan, 0, len(prop.GetAllPossibleChildTaskTypes()))
	taskTypes := []property.TaskType{property.INTERLOCKSingleReadTaskType, property.CoFIDeloubleReadTaskType}
	if la.ctx.GetStochastikVars().AllowBCJ {
		taskTypes = append(taskTypes, property.INTERLOCKTiFlashLocalReadTaskType)
	}
	if la.HasDistinct() {
		// TODO: remove AllowDistinctAggPushDown after the cost estimation of distinct pushdown is implemented.
		// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
		if !la.canPushToINTERLOCK() || !la.ctx.GetStochastikVars().AllowDistinctAggPushDown {
			taskTypes = []property.TaskType{property.RootTaskType}
		}
	} else if !la.aggHints.preferAggToINTERLOCK {
		taskTypes = append(taskTypes, property.RootTaskType)
	}
	if prop.IsFlashOnlyProp() {
		taskTypes = []property.TaskType{prop.TaskTp}
	}
	for _, taskTp := range taskTypes {
		agg := NewPhysicalHashAgg(la, la.stats.ScaleByExpectCnt(prop.ExpectedCnt), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, TaskTp: taskTp})
		agg.SetSchema(la.schemaReplicant.Clone())
		hashAggs = append(hashAggs, agg)
	}
	return hashAggs
}

// ResetHintIfConflicted resets the aggHints.preferAggType if they are conflicted,
// and returns the two preferAggType hints.
func (la *LogicalAggregation) ResetHintIfConflicted() (preferHash bool, preferStream bool) {
	preferHash = (la.aggHints.preferAggType & preferHashAgg) > 0
	preferStream = (la.aggHints.preferAggType & preferStreamAgg) > 0
	if preferHash && preferStream {
		errMsg := "Optimizer aggregation hints are conflicted"
		warning := ErrInternal.GenWithStack(errMsg)
		la.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
		la.aggHints.preferAggType = 0
		preferHash, preferStream = false, false
	}
	return
}

func (la *LogicalAggregation) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if la.aggHints.preferAggToINTERLOCK {
		if !la.canPushToINTERLOCK() {
			errMsg := "Optimizer Hint AGG_TO_INTERLOCK is inapplicable"
			warning := ErrInternal.GenWithStack(errMsg)
			la.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
			la.aggHints.preferAggToINTERLOCK = false
		}
	}

	preferHash, preferStream := la.ResetHintIfConflicted()

	hashAggs := la.getHashAggs(prop)
	if hashAggs != nil && preferHash {
		return hashAggs, true
	}

	streamAggs := la.getStreamAggs(prop)
	if streamAggs != nil && preferStream {
		return streamAggs, true
	}

	aggs := append(hashAggs, streamAggs...)

	if streamAggs == nil && preferStream && !prop.IsEmpty() {
		errMsg := "Optimizer Hint STREAM_AGG is inapplicable"
		warning := ErrInternal.GenWithStack(errMsg)
		la.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
	}

	return aggs, !(preferStream || preferHash)
}

func (p *LogicalSelection) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	childProp := prop.Clone()
	sel := PhysicalSelection{
		Conditions: p.Conditions,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, childProp)
	return []PhysicalPlan{sel}, true
}

func (p *LogicalLimit) canPushToINTERLOCK() bool {
	// At present, only Aggregation, Limit, TopN can be pushed to INTERLOCK task, and Projection will be supported in the future.
	// When we push task to interlock, finishINTERLOCKTask will close the INTERLOCK task and create a root task in the current implementation.
	// Thus, we can't push two different tasks to interlock now, and can only push task to interlock when the child is Datasource.

	// TODO: develop this function after supporting push several tasks to INTERLOCKrecessor and supporting Projection to interlock.
	_, ok := p.children[0].(*DataSource)
	return ok
}

func (p *LogicalLimit) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if !prop.IsEmpty() {
		return nil, true
	}
	// allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	if p.limitHints.preferLimitToINTERLOCK {
		if !p.canPushToINTERLOCK() {
			errMsg := "Optimizer Hint LIMIT_TO_INTERLOCK is inapplicable"
			warning := ErrInternal.GenWithStack(errMsg)
			p.ctx.GetStochastikVars().StmtCtx.AppendWarning(warning)
			p.limitHints.preferLimitToINTERLOCK = false
		}
	}

	allTaskTypes := []property.TaskType{property.INTERLOCKSingleReadTaskType, property.CoFIDeloubleReadTaskType}
	if !p.limitHints.preferLimitToINTERLOCK {
		allTaskTypes = append(allTaskTypes, property.RootTaskType)
	}
	ret := make([]PhysicalPlan, 0, len(allTaskTypes))
	for _, tp := range allTaskTypes {
		resultProp := &property.PhysicalProperty{TaskTp: tp, ExpectedCnt: float64(p.Count + p.Offset)}
		limit := PhysicalLimit{
			Offset: p.Offset,
			Count:  p.Count,
		}.Init(p.ctx, p.stats, p.blockOffset, resultProp)
		ret = append(ret, limit)
	}
	return ret, true
}

func (p *LogicalLock) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if prop.IsFlashOnlyProp() {
		return nil, true
	}
	childProp := prop.Clone()
	dagger := PhysicalLock{
		Lock:             p.Lock,
		TblID2Handle:     p.tblID2Handle,
		PartitionedBlock: p.partitionedBlock,
	}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), childProp)
	return []PhysicalPlan{dagger}, true
}

func (p *LogicalUnionAll) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	// TODO: UnionAll can not pass any order, but we can change it to sort merge to keep order.
	if !prop.IsEmpty() || prop.IsFlashOnlyProp() {
		return nil, true
	}
	chReqProps := make([]*property.PhysicalProperty, 0, len(p.children))
	for range p.children {
		chReqProps = append(chReqProps, &property.PhysicalProperty{ExpectedCnt: prop.ExpectedCnt})
	}
	ua := PhysicalUnionAll{}.Init(p.ctx, p.stats.ScaleByExpectCnt(prop.ExpectedCnt), p.blockOffset, chReqProps...)
	ua.SetSchema(p.Schema())
	return []PhysicalPlan{ua}, true
}

func (p *LogicalPartitionUnionAll) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	uas, flagHint := p.LogicalUnionAll.exhaustPhysicalPlans(prop)
	for _, ua := range uas {
		ua.(*PhysicalUnionAll).tp = plancodec.TypePartitionUnion
	}
	return uas, flagHint
}

func (ls *LogicalSort) getPhysicalSort(prop *property.PhysicalProperty) *PhysicalSort {
	ps := PhysicalSort{ByItems: ls.ByItems}.Init(ls.ctx, ls.stats.ScaleByExpectCnt(prop.ExpectedCnt), ls.blockOffset, &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	return ps
}

func (ls *LogicalSort) getNominalSort(reqProp *property.PhysicalProperty) *NominalSort {
	prop, canPass, onlyDeferredCauset := GetPropByOrderByItemsContainScalarFunc(ls.ByItems)
	if !canPass {
		return nil
	}
	prop.ExpectedCnt = reqProp.ExpectedCnt
	ps := NominalSort{OnlyDeferredCauset: onlyDeferredCauset, ByItems: ls.ByItems}.Init(
		ls.ctx, ls.stats.ScaleByExpectCnt(prop.ExpectedCnt), ls.blockOffset, prop)
	return ps
}

func (ls *LogicalSort) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if MatchItems(prop, ls.ByItems) {
		ret := make([]PhysicalPlan, 0, 2)
		ret = append(ret, ls.getPhysicalSort(prop))
		ns := ls.getNominalSort(prop)
		if ns != nil {
			ret = append(ret, ns)
		}
		return ret, true
	}
	return nil, true
}

func (p *LogicalMaxOneRow) exhaustPhysicalPlans(prop *property.PhysicalProperty) ([]PhysicalPlan, bool) {
	if !prop.IsEmpty() || prop.IsFlashOnlyProp() {
		return nil, true
	}
	mor := PhysicalMaxOneRow{}.Init(p.ctx, p.stats, p.blockOffset, &property.PhysicalProperty{ExpectedCnt: 2})
	return []PhysicalPlan{mor}, true
}
