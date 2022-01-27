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
	"math"
	"sort"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/planner/property"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

func (p *basePhysicalPlan) StatsCount() float64 {
	return p.stats.RowCount
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalBlockDual) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	profile := &property.StatsInfo{
		RowCount:    float64(p.RowCount),
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	for _, col := range selfSchema.DeferredCausets {
		profile.Cardinality[col.UniqueID] = float64(p.RowCount)
	}
	p.stats = profile
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalMemBlock) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	statsBlock := statistics.PseudoBlock(p.BlockInfo)
	stats := &property.StatsInfo{
		RowCount:     float64(statsBlock.Count),
		Cardinality:  make(map[int64]float64, len(p.BlockInfo.DeferredCausets)),
		HistDefCausl: statsBlock.GenerateHistDefCauslFromDeferredCausetInfo(p.BlockInfo.DeferredCausets, p.schemaReplicant.DeferredCausets),
		StatsVersion: statistics.PseudoVersion,
	}
	for _, col := range selfSchema.DeferredCausets {
		stats.Cardinality[col.UniqueID] = float64(statsBlock.Count)
	}
	p.stats = stats
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalShow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	// A fake count, just to avoid panic now.
	p.stats = getFakeStats(selfSchema)
	return p.stats, nil
}

func getFakeStats(schemaReplicant *expression.Schema) *property.StatsInfo {
	profile := &property.StatsInfo{
		RowCount:    1,
		Cardinality: make(map[int64]float64, schemaReplicant.Len()),
	}
	for _, col := range schemaReplicant.DeferredCausets {
		profile.Cardinality[col.UniqueID] = 1
	}
	return profile
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalShowDBSJobs) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	// A fake count, just to avoid panic now.
	p.stats = getFakeStats(selfSchema)
	return p.stats, nil
}

// RecursiveDeriveStats4Test is a exporter just for test.
func RecursiveDeriveStats4Test(p LogicalPlan) (*property.StatsInfo, error) {
	return p.recursiveDeriveStats(nil)
}

// GetStats4Test is a exporter just for test.
func GetStats4Test(p LogicalPlan) *property.StatsInfo {
	return p.statsInfo()
}

func (p *baseLogicalPlan) recursiveDeriveStats(colGroups [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	childStats := make([]*property.StatsInfo, len(p.children))
	childSchema := make([]*expression.Schema, len(p.children))
	cumDefCausGroups := p.self.ExtractDefCausGroups(colGroups)
	for i, child := range p.children {
		childProfile, err := child.recursiveDeriveStats(cumDefCausGroups)
		if err != nil {
			return nil, err
		}
		childStats[i] = childProfile
		childSchema[i] = child.Schema()
	}
	return p.self.DeriveStats(childStats, p.self.Schema(), childSchema, colGroups)
}

// ExtractDefCausGroups implements LogicalPlan ExtractDefCausGroups interface.
func (p *baseLogicalPlan) ExtractDefCausGroups(_ [][]*expression.DeferredCauset) [][]*expression.DeferredCauset {
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *baseLogicalPlan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if len(childStats) == 1 {
		p.stats = childStats[0]
		return p.stats, nil
	}
	if len(childStats) > 1 {
		err := ErrInternal.GenWithStack("LogicalPlans with more than one child should implement their own DeriveStats().")
		return nil, err
	}
	if p.stats != nil {
		return p.stats, nil
	}
	profile := &property.StatsInfo{
		RowCount:    float64(1),
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	for _, col := range selfSchema.DeferredCausets {
		profile.Cardinality[col.UniqueID] = 1
	}
	p.stats = profile
	return profile, nil
}

// getDeferredCausetNDV computes estimated NDV of specified column using the original
// histogram of `DataSource` which is retrieved from storage(not the derived one).
func (ds *DataSource) getDeferredCausetNDV(colID int64) (ndv float64) {
	hist, ok := ds.statisticBlock.DeferredCausets[colID]
	if ok && hist.Count > 0 {
		factor := float64(ds.statisticBlock.Count) / float64(hist.Count)
		ndv = float64(hist.NDV) * factor
	} else {
		ndv = float64(ds.statisticBlock.Count) * distinctFactor
	}
	return ndv
}

func (ds *DataSource) getGroupNDVs(colGroups [][]*expression.DeferredCauset) []property.GroupNDV {
	if colGroups == nil {
		return nil
	}
	tbl := ds.blockStats.HistDefCausl
	ndvs := make([]property.GroupNDV, 0, len(colGroups))
	for idxID, idx := range tbl.Indices {
		idxDefCauss := make([]int64, len(tbl.Idx2DeferredCausetIDs[idxID]))
		INTERLOCKy(idxDefCauss, tbl.Idx2DeferredCausetIDs[idxID])
		sort.Slice(idxDefCauss, func(i, j int) bool {
			return idxDefCauss[i] < idxDefCauss[j]
		})
		for _, g := range colGroups {
			// We only want those exact matches.
			if len(g) != len(idxDefCauss) {
				continue
			}
			match := true
			for i, col := range g {
				// Both slices are sorted according to UniqueID.
				if col.UniqueID != idxDefCauss[i] {
					match = false
					break
				}
			}
			if match {
				ndv := property.GroupNDV{
					DefCauss: idxDefCauss,
					NDV:      float64(idx.NDV),
				}
				ndvs = append(ndvs, ndv)
				break
			}
		}
	}
	return ndvs
}

func (ds *DataSource) initStats(colGroups [][]*expression.DeferredCauset) {
	if ds.blockStats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		ds.blockStats.GroupNDVs = ds.getGroupNDVs(colGroups)
		return
	}
	if ds.statisticBlock == nil {
		ds.statisticBlock = getStatsBlock(ds.ctx, ds.blockInfo, ds.block.Meta().ID)
	}
	blockStats := &property.StatsInfo{
		RowCount:     float64(ds.statisticBlock.Count),
		Cardinality:  make(map[int64]float64, ds.schemaReplicant.Len()),
		HistDefCausl: ds.statisticBlock.GenerateHistDefCauslFromDeferredCausetInfo(ds.DeferredCausets, ds.schemaReplicant.DeferredCausets),
		StatsVersion: ds.statisticBlock.Version,
	}
	if ds.statisticBlock.Pseudo {
		blockStats.StatsVersion = statistics.PseudoVersion
	}
	for _, col := range ds.schemaReplicant.DeferredCausets {
		blockStats.Cardinality[col.UniqueID] = ds.getDeferredCausetNDV(col.ID)
	}
	ds.blockStats = blockStats
	ds.blockStats.GroupNDVs = ds.getGroupNDVs(colGroups)
	ds.TblDefCausHists = ds.statisticBlock.ID2UniqueID(ds.TblDefCauss)
}

func (ds *DataSource) deriveStatsByFilter(conds expression.CNFExprs, filledPaths []*soliton.AccessPath) *property.StatsInfo {
	selectivity, nodes, err := ds.blockStats.HistDefCausl.Selectivity(ds.ctx, conds, filledPaths)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		selectivity = SelectionFactor
	}
	stats := ds.blockStats.Scale(selectivity)
	if ds.ctx.GetStochastikVars().OptimizerSelectivityLevel >= 1 {
		stats.HistDefCausl = stats.HistDefCausl.NewHistDefCauslBySelectivity(ds.ctx.GetStochastikVars().StmtCtx, nodes)
	}
	return stats
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (ds *DataSource) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if ds.stats != nil && len(colGroups) == 0 {
		return ds.stats, nil
	}
	ds.initStats(colGroups)
	if ds.stats != nil {
		// Just reload the GroupNDVs.
		selectivity := ds.stats.RowCount / ds.blockStats.RowCount
		ds.stats = ds.blockStats.Scale(selectivity)
		return ds.stats, nil
	}
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ds.pushedDownConds {
		ds.pushedDownConds[i] = expression.PushDownNot(ds.ctx, expr)
	}
	for _, path := range ds.possibleAccessPaths {
		if path.IsBlockPath() {
			continue
		}
		err := ds.fillIndexPath(path, ds.pushedDownConds)
		if err != nil {
			return nil, err
		}
	}
	ds.stats = ds.deriveStatsByFilter(ds.pushedDownConds, ds.possibleAccessPaths)
	for _, path := range ds.possibleAccessPaths {
		if path.IsBlockPath() {
			noIntervalRanges, err := ds.deriveBlockPathStats(path, ds.pushedDownConds, false)
			if err != nil {
				return nil, err
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.Ranges) == 0 {
				ds.possibleAccessPaths[0] = path
				ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
				break
			}
			continue
		}
		noIntervalRanges := ds.deriveIndexPathStats(path, ds.pushedDownConds, false)
		// If we have empty range, or point range on unique index, just remove other possible paths.
		if (noIntervalRanges && path.Index.Unique) || len(path.Ranges) == 0 {
			ds.possibleAccessPaths[0] = path
			ds.possibleAccessPaths = ds.possibleAccessPaths[:1]
			break
		}
	}

	// TODO: implement UnionScan + IndexMerge
	isReadOnlyTxn := true
	txn, err := ds.ctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn.Valid() && !txn.IsReadOnly() {
		isReadOnlyTxn = false
	}
	// Consider the IndexMergePath. Now, we just generate `IndexMergePath` in DNF case.
	isPossibleIdxMerge := len(ds.pushedDownConds) > 0 && len(ds.possibleAccessPaths) > 1
	stochastikAndStmtPermission := (ds.ctx.GetStochastikVars().GetEnableIndexMerge() || len(ds.indexMergeHints) > 0) && !ds.ctx.GetStochastikVars().StmtCtx.NoIndexMergeHint
	// If there is an index path, we current do not consider `IndexMergePath`.
	needConsiderIndexMerge := true
	for i := 1; i < len(ds.possibleAccessPaths); i++ {
		if len(ds.possibleAccessPaths[i].AccessConds) != 0 {
			needConsiderIndexMerge = false
			break
		}
	}
	if isPossibleIdxMerge && stochastikAndStmtPermission && needConsiderIndexMerge && isReadOnlyTxn {
		ds.generateAndPruneIndexMergePath(ds.indexMergeHints != nil)
	} else if len(ds.indexMergeHints) > 0 {
		ds.indexMergeHints = nil
		ds.ctx.GetStochastikVars().StmtCtx.AppendWarning(errors.Errorf("IndexMerge is inapplicable or disabled"))
	}
	return ds.stats, nil
}

func (ds *DataSource) generateAndPruneIndexMergePath(needPrune bool) {
	regularPathCount := len(ds.possibleAccessPaths)
	ds.generateIndexMergeOrPaths()
	// If without hints, it means that `enableIndexMerge` is true
	if len(ds.indexMergeHints) == 0 {
		return
	}
	// With hints and without generated IndexMerge paths
	if regularPathCount == len(ds.possibleAccessPaths) {
		ds.indexMergeHints = nil
		ds.ctx.GetStochastikVars().StmtCtx.AppendWarning(errors.Errorf("IndexMerge is inapplicable or disabled"))
		return
	}
	// Do not need to consider the regular paths in find_best_task().
	if needPrune {
		ds.possibleAccessPaths = ds.possibleAccessPaths[regularPathCount:]
	}
}

// DeriveStats implements LogicalPlan DeriveStats interface.
func (ts *LogicalBlockScan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (_ *property.StatsInfo, err error) {
	ts.Source.initStats(nil)
	// PushDownNot here can convert query 'not (a != 1)' to 'a = 1'.
	for i, expr := range ts.AccessConds {
		// TODO The expressions may be shared by BlockScan and several IndexScans, there would be redundant
		// `PushDownNot` function call in multiple `DeriveStats` then.
		ts.AccessConds[i] = expression.PushDownNot(ts.ctx, expr)
	}
	ts.stats = ts.Source.deriveStatsByFilter(ts.AccessConds, nil)
	sc := ts.SCtx().GetStochastikVars().StmtCtx
	// ts.Handle could be nil if PK is Handle, and PK column has been pruned.
	// TODO: support clustered index.
	if ts.HandleDefCauss != nil {
		ts.Ranges, err = ranger.BuildBlockRange(ts.AccessConds, sc, ts.HandleDefCauss.GetDefCaus(0).RetType)
	} else {
		isUnsigned := false
		if ts.Source.blockInfo.PKIsHandle {
			if pkDefCausInfo := ts.Source.blockInfo.GetPkDefCausInfo(); pkDefCausInfo != nil {
				isUnsigned = allegrosql.HasUnsignedFlag(pkDefCausInfo.Flag)
			}
		}
		ts.Ranges = ranger.FullIntRange(isUnsigned)
	}
	if err != nil {
		return nil, err
	}
	return ts.stats, nil
}

// DeriveStats implements LogicalPlan DeriveStats interface.
func (is *LogicalIndexScan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	is.Source.initStats(nil)
	for i, expr := range is.AccessConds {
		is.AccessConds[i] = expression.PushDownNot(is.ctx, expr)
	}
	is.stats = is.Source.deriveStatsByFilter(is.AccessConds, nil)
	if len(is.AccessConds) == 0 {
		is.Ranges = ranger.FullRange()
	}
	is.IdxDefCauss, is.IdxDefCausLens = expression.IndexInfo2PrefixDefCauss(is.DeferredCausets, selfSchema.DeferredCausets, is.Index)
	is.FullIdxDefCauss, is.FullIdxDefCausLens = expression.IndexInfo2DefCauss(is.DeferredCausets, selfSchema.DeferredCausets, is.Index)
	if !is.Index.Unique && !is.Index.Primary && len(is.Index.DeferredCausets) == len(is.IdxDefCauss) {
		handleDefCaus := is.getPKIsHandleDefCaus(selfSchema)
		if handleDefCaus != nil && !allegrosql.HasUnsignedFlag(handleDefCaus.RetType.Flag) {
			is.IdxDefCauss = append(is.IdxDefCauss, handleDefCaus)
			is.IdxDefCausLens = append(is.IdxDefCausLens, types.UnspecifiedLength)
		}
	}
	return is.stats, nil
}

// getIndexMergeOrPath generates all possible IndexMergeOrPaths.
func (ds *DataSource) generateIndexMergeOrPaths() {
	usedIndexCount := len(ds.possibleAccessPaths)
	for i, cond := range ds.pushedDownConds {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.LogicOr {
			continue
		}
		var partialPaths = make([]*soliton.AccessPath, 0, usedIndexCount)
		dnfItems := expression.FlattenDNFConditions(sf)
		for _, item := range dnfItems {
			cnfItems := expression.SplitCNFItems(item)
			itemPaths := ds.accessPathsForConds(cnfItems, usedIndexCount)
			if len(itemPaths) == 0 {
				partialPaths = nil
				break
			}
			partialPath := ds.buildIndexMergePartialPath(itemPaths)
			if partialPath == nil {
				partialPaths = nil
				break
			}
			partialPaths = append(partialPaths, partialPath)
		}
		if len(partialPaths) > 1 {
			possiblePath := ds.buildIndexMergeOrPath(partialPaths, i)
			if possiblePath != nil {
				ds.possibleAccessPaths = append(ds.possibleAccessPaths, possiblePath)
			}
		}
	}
}

// isInIndexMergeHints checks whether current index or primary key is in IndexMerge hints.
func (ds *DataSource) isInIndexMergeHints(name string) bool {
	if len(ds.indexMergeHints) == 0 {
		return true
	}
	for _, hint := range ds.indexMergeHints {
		if hint.indexHint == nil || len(hint.indexHint.IndexNames) == 0 {
			return true
		}
		for _, hintName := range hint.indexHint.IndexNames {
			if name == hintName.String() {
				return true
			}
		}
	}
	return false
}

// accessPathsForConds generates all possible index paths for conditions.
func (ds *DataSource) accessPathsForConds(conditions []expression.Expression, usedIndexCount int) []*soliton.AccessPath {
	var results = make([]*soliton.AccessPath, 0, usedIndexCount)
	for i := 0; i < usedIndexCount; i++ {
		path := &soliton.AccessPath{}
		if ds.possibleAccessPaths[i].IsBlockPath() {
			if !ds.isInIndexMergeHints("primary") {
				continue
			}
			if ds.blockInfo.IsCommonHandle {
				path.IsCommonHandlePath = true
				path.Index = ds.possibleAccessPaths[i].Index
			} else {
				path.IsIntHandlePath = true
			}
			noIntervalRanges, err := ds.deriveBlockPathStats(path, conditions, true)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			if len(path.BlockFilters) > 0 || len(path.AccessConds) == 0 {
				// If AccessConds is empty or blockFilter is not empty, we ignore the access path.
				// Now these conditions are too strict.
				// For example, a allegrosql `select * from t where a > 1 or (b < 2 and c > 3)` and block `t` with indexes
				// on a and b separately. we can generate a `IndexMergePath` with block filter `a > 1 or (b < 2 and c > 3)`.
				// TODO: solve the above case
				continue
			}
			// If we have point or empty range, just remove other possible paths.
			if noIntervalRanges || len(path.Ranges) == 0 {
				if len(results) == 0 {
					results = append(results, path)
				} else {
					results[0] = path
					results = results[:1]
				}
				break
			}
		} else {
			path.Index = ds.possibleAccessPaths[i].Index
			if !ds.isInIndexMergeHints(path.Index.Name.L) {
				continue
			}
			err := ds.fillIndexPath(path, conditions)
			if err != nil {
				logutil.BgLogger().Debug("can not derive statistics of a path", zap.Error(err))
				continue
			}
			noIntervalRanges := ds.deriveIndexPathStats(path, conditions, true)
			if len(path.BlockFilters) > 0 || len(path.AccessConds) == 0 {
				// If AccessConds is empty or blockFilter is not empty, we ignore the access path.
				// Now these conditions are too strict.
				// For example, a allegrosql `select * from t where a > 1 or (b < 2 and c > 3)` and block `t` with indexes
				// on a and b separately. we can generate a `IndexMergePath` with block filter `a > 1 or (b < 2 and c > 3)`.
				// TODO: solve the above case
				continue
			}
			// If we have empty range, or point range on unique index, just remove other possible paths.
			if (noIntervalRanges && path.Index.Unique) || len(path.Ranges) == 0 {
				if len(results) == 0 {
					results = append(results, path)
				} else {
					results[0] = path
					results = results[:1]
				}
				break
			}
		}
		results = append(results, path)
	}
	return results
}

// buildIndexMergePartialPath chooses the best index path from all possible paths.
// Now we just choose the index with most columns.
// We should improve this strategy, because it is not always better to choose index
// with most columns, e.g, filter is c > 1 and the input indexes are c and c_d_e,
// the former one is enough, and it is less expensive in execution compared with the latter one.
// TODO: improve strategy of the partial path selection
func (ds *DataSource) buildIndexMergePartialPath(indexAccessPaths []*soliton.AccessPath) *soliton.AccessPath {
	if len(indexAccessPaths) == 1 {
		return indexAccessPaths[0]
	}

	maxDefCaussIndex := 0
	maxDefCauss := len(indexAccessPaths[0].IdxDefCauss)
	for i := 1; i < len(indexAccessPaths); i++ {
		current := len(indexAccessPaths[i].IdxDefCauss)
		if current > maxDefCauss {
			maxDefCaussIndex = i
			maxDefCauss = current
		}
	}
	return indexAccessPaths[maxDefCaussIndex]
}

// buildIndexMergeOrPath generates one possible IndexMergePath.
func (ds *DataSource) buildIndexMergeOrPath(partialPaths []*soliton.AccessPath, current int) *soliton.AccessPath {
	indexMergePath := &soliton.AccessPath{PartialIndexPaths: partialPaths}
	indexMergePath.BlockFilters = append(indexMergePath.BlockFilters, ds.pushedDownConds[:current]...)
	indexMergePath.BlockFilters = append(indexMergePath.BlockFilters, ds.pushedDownConds[current+1:]...)
	return indexMergePath
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalSelection) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = childStats[0].Scale(SelectionFactor)
	p.stats.GroupNDVs = nil
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalUnionAll) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = &property.StatsInfo{
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	for _, childProfile := range childStats {
		p.stats.RowCount += childProfile.RowCount
		for _, col := range selfSchema.DeferredCausets {
			p.stats.Cardinality[col.UniqueID] += childProfile.Cardinality[col.UniqueID]
		}
	}
	return p.stats, nil
}

func deriveLimitStats(childProfile *property.StatsInfo, limitCount float64) *property.StatsInfo {
	stats := &property.StatsInfo{
		RowCount:    math.Min(limitCount, childProfile.RowCount),
		Cardinality: make(map[int64]float64, len(childProfile.Cardinality)),
	}
	for id, c := range childProfile.Cardinality {
		stats.Cardinality[id] = math.Min(c, stats.RowCount)
	}
	return stats
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalLimit) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = deriveLimitStats(childStats[0], float64(p.Count))
	return p.stats, nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (lt *LogicalTopN) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if lt.stats != nil {
		return lt.stats, nil
	}
	lt.stats = deriveLimitStats(childStats[0], float64(lt.Count))
	return lt.stats, nil
}

// getCardinality will return the Cardinality of a couple of columns. We simply return the max one, because we cannot know
// the Cardinality for multi-dimension attributes properly. This is a simple and naive scheme of Cardinality estimation.
func getCardinality(defcaus []*expression.DeferredCauset, schemaReplicant *expression.Schema, profile *property.StatsInfo) float64 {
	cardinality := 1.0
	indices := schemaReplicant.DeferredCausetsIndices(defcaus)
	if indices == nil {
		logutil.BgLogger().Error("column not found in schemaReplicant", zap.Any("columns", defcaus), zap.String("schemaReplicant", schemaReplicant.String()))
		return cardinality
	}
	for _, idx := range indices {
		// It is a very elementary estimation.
		col := schemaReplicant.DeferredCausets[idx]
		cardinality = math.Max(cardinality, profile.Cardinality[col.UniqueID])
	}
	return cardinality
}

func (p *LogicalProjection) getGroupNDVs(colGroups [][]*expression.DeferredCauset, childProfile *property.StatsInfo, selfSchema *expression.Schema) []property.GroupNDV {
	if len(colGroups) == 0 || len(childProfile.GroupNDVs) == 0 {
		return nil
	}
	exprDefCaus2ProjDefCaus := make(map[int64]int64)
	for i, expr := range p.Exprs {
		exprDefCaus, ok := expr.(*expression.DeferredCauset)
		if !ok {
			continue
		}
		exprDefCaus2ProjDefCaus[exprDefCaus.UniqueID] = selfSchema.DeferredCausets[i].UniqueID
	}
	ndvs := make([]property.GroupNDV, 0, len(childProfile.GroupNDVs))
	for _, childGroupNDV := range childProfile.GroupNDVs {
		projDefCauss := make([]int64, len(childGroupNDV.DefCauss))
		for i, col := range childGroupNDV.DefCauss {
			projDefCaus, ok := exprDefCaus2ProjDefCaus[col]
			if !ok {
				projDefCauss = nil
				break
			}
			projDefCauss[i] = projDefCaus
		}
		if projDefCauss == nil {
			continue
		}
		sort.Slice(projDefCauss, func(i, j int) bool {
			return projDefCauss[i] < projDefCauss[j]
		})
		groupNDV := property.GroupNDV{
			DefCauss: projDefCauss,
			NDV:      childGroupNDV.NDV,
		}
		ndvs = append(ndvs, groupNDV)
	}
	return ndvs
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalProjection) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	if p.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childProfile, selfSchema)
		return p.stats, nil
	}
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make(map[int64]float64, len(p.Exprs)),
	}
	for i, expr := range p.Exprs {
		defcaus := expression.ExtractDeferredCausets(expr)
		p.stats.Cardinality[selfSchema.DeferredCausets[i].UniqueID] = getCardinality(defcaus, childSchema[0], childProfile)
	}
	p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childProfile, selfSchema)
	return p.stats, nil
}

// ExtractDefCausGroups implements LogicalPlan ExtractDefCausGroups interface.
func (p *LogicalProjection) ExtractDefCausGroups(colGroups [][]*expression.DeferredCauset) [][]*expression.DeferredCauset {
	if len(colGroups) == 0 {
		return nil
	}
	extDefCausGroups, _ := p.Schema().ExtractDefCausGroups(colGroups)
	if len(extDefCausGroups) == 0 {
		return nil
	}
	extracted := make([][]*expression.DeferredCauset, 0, len(extDefCausGroups))
	for _, defcaus := range extDefCausGroups {
		exprs := make([]*expression.DeferredCauset, len(defcaus))
		allDefCauss := true
		for i, offset := range defcaus {
			col, ok := p.Exprs[offset].(*expression.DeferredCauset)
			// TODO: for functional dependent projections like `col1 + 1` -> `col2`, we can maintain GroupNDVs actually.
			if !ok {
				allDefCauss = false
				break
			}
			exprs[i] = col
		}
		if allDefCauss {
			extracted = append(extracted, expression.SortDeferredCausets(exprs))
		}
	}
	return extracted
}

func (la *LogicalAggregation) getGroupNDVs(colGroups [][]*expression.DeferredCauset, childProfile *property.StatsInfo, selfSchema *expression.Schema, gbyDefCauss []*expression.DeferredCauset) []property.GroupNDV {
	if len(colGroups) == 0 || len(childProfile.GroupNDVs) == 0 {
		return nil
	}
	// Check if the child profile provides GroupNDV for the GROUP BY columns.
	// Note that gbyDefCauss may not be the exact GROUP BY columns, e.g, GROUP BY a+b,
	// but we have no other approaches for the cardinality estimation of these cases
	// except for using the independent assumption, unless we can use stats of expression index.
	gbyDefCauss = expression.SortDeferredCausets(gbyDefCauss)
	for _, groupNDV := range childProfile.GroupNDVs {
		if len(gbyDefCauss) != len(groupNDV.DefCauss) {
			continue
		}
		match := true
		for i, col := range groupNDV.DefCauss {
			if col != gbyDefCauss[i].UniqueID {
				match = false
				break
			}
		}
		if match {
			return []property.GroupNDV{groupNDV}
		}
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalAggregation) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	gbyDefCauss := make([]*expression.DeferredCauset, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		defcaus := expression.ExtractDeferredCausets(gbyExpr)
		gbyDefCauss = append(gbyDefCauss, defcaus...)
	}
	if la.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		la.stats.GroupNDVs = la.getGroupNDVs(colGroups, childProfile, selfSchema, gbyDefCauss)
		return la.stats, nil
	}
	cardinality := getCardinality(gbyDefCauss, childSchema[0], childProfile)
	la.stats = &property.StatsInfo{
		RowCount:    cardinality,
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	// We cannot estimate the Cardinality for every output, so we use a conservative strategy.
	for _, col := range selfSchema.DeferredCausets {
		la.stats.Cardinality[col.UniqueID] = cardinality
	}
	la.inputCount = childProfile.RowCount
	la.stats.GroupNDVs = la.getGroupNDVs(colGroups, childProfile, selfSchema, gbyDefCauss)
	return la.stats, nil
}

// ExtractDefCausGroups implements LogicalPlan ExtractDefCausGroups interface.
func (la *LogicalAggregation) ExtractDefCausGroups(_ [][]*expression.DeferredCauset) [][]*expression.DeferredCauset {
	// Parent colGroups would be dicarded, because aggregation would make NDV of colGroups
	// which does not match GroupByItems invalid.
	// Note that gbyDefCauss may not be the exact GROUP BY columns, e.g, GROUP BY a+b,
	// but we have no other approaches for the cardinality estimation of these cases
	// except for using the independent assumption, unless we can use stats of expression index.
	gbyDefCauss := make([]*expression.DeferredCauset, 0, len(la.GroupByItems))
	for _, gbyExpr := range la.GroupByItems {
		defcaus := expression.ExtractDeferredCausets(gbyExpr)
		gbyDefCauss = append(gbyDefCauss, defcaus...)
	}
	if len(gbyDefCauss) > 0 {
		return [][]*expression.DeferredCauset{expression.SortDeferredCausets(gbyDefCauss)}
	}
	return nil
}

func (p *LogicalJoin) getGroupNDVs(colGroups [][]*expression.DeferredCauset, childStats []*property.StatsInfo) []property.GroupNDV {
	outerIdx := int(-1)
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		outerIdx = 0
	} else if p.JoinType == RightOuterJoin {
		outerIdx = 1
	}
	if outerIdx >= 0 && len(colGroups) > 0 {
		return childStats[outerIdx].GroupNDVs
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any event. The last column is a boolean value, whose Cardinality should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the Cardinality of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.stats, nil
	}
	leftProfile, rightProfile := childStats[0], childStats[1]
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	helper := &fullJoinRowCountHelper{
		cartesian:     0 == len(p.EqualConditions),
		leftProfile:   leftProfile,
		rightProfile:  rightProfile,
		leftJoinKeys:  leftJoinKeys,
		rightJoinKeys: rightJoinKeys,
		leftSchema:    childSchema[0],
		rightSchema:   childSchema[1],
	}
	p.equalCondOutCnt = helper.estimate()
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount * SelectionFactor,
			Cardinality: make(map[int64]float64, len(leftProfile.Cardinality)),
		}
		for id, c := range leftProfile.Cardinality {
			p.stats.Cardinality[id] = c * SelectionFactor
		}
		return p.stats, nil
	}
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		p.stats = &property.StatsInfo{
			RowCount:    leftProfile.RowCount,
			Cardinality: make(map[int64]float64, selfSchema.Len()),
		}
		for id, c := range leftProfile.Cardinality {
			p.stats.Cardinality[id] = c
		}
		p.stats.Cardinality[selfSchema.DeferredCausets[selfSchema.Len()-1].UniqueID] = 2.0
		p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.stats, nil
	}
	count := p.equalCondOutCnt
	if p.JoinType == LeftOuterJoin {
		count = math.Max(count, leftProfile.RowCount)
	} else if p.JoinType == RightOuterJoin {
		count = math.Max(count, rightProfile.RowCount)
	}
	cardinality := make(map[int64]float64, selfSchema.Len())
	for id, c := range leftProfile.Cardinality {
		cardinality[id] = math.Min(c, count)
	}
	for id, c := range rightProfile.Cardinality {
		cardinality[id] = math.Min(c, count)
	}
	p.stats = &property.StatsInfo{
		RowCount:    count,
		Cardinality: cardinality,
	}
	p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
	return p.stats, nil
}

// ExtractDefCausGroups implements LogicalPlan ExtractDefCausGroups interface.
func (p *LogicalJoin) ExtractDefCausGroups(colGroups [][]*expression.DeferredCauset) [][]*expression.DeferredCauset {
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	extracted := make([][]*expression.DeferredCauset, 0, 2+len(colGroups))
	if len(leftJoinKeys) > 1 && (p.JoinType == InnerJoin || p.JoinType == LeftOuterJoin || p.JoinType == RightOuterJoin) {
		extracted = append(extracted, expression.SortDeferredCausets(leftJoinKeys), expression.SortDeferredCausets(rightJoinKeys))
	}
	var outerSchema *expression.Schema
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		outerSchema = p.Children()[0].Schema()
	} else if p.JoinType == RightOuterJoin {
		outerSchema = p.Children()[1].Schema()
	}
	if len(colGroups) == 0 || outerSchema == nil {
		return extracted
	}
	_, offsets := outerSchema.ExtractDefCausGroups(colGroups)
	if len(offsets) == 0 {
		return extracted
	}
	for _, offset := range offsets {
		extracted = append(extracted, colGroups[offset])
	}
	return extracted
}

type fullJoinRowCountHelper struct {
	cartesian     bool
	leftProfile   *property.StatsInfo
	rightProfile  *property.StatsInfo
	leftJoinKeys  []*expression.DeferredCauset
	rightJoinKeys []*expression.DeferredCauset
	leftSchema    *expression.Schema
	rightSchema   *expression.Schema
}

func (h *fullJoinRowCountHelper) estimate() float64 {
	if h.cartesian {
		return h.leftProfile.RowCount * h.rightProfile.RowCount
	}
	leftKeyCardinality := getCardinality(h.leftJoinKeys, h.leftSchema, h.leftProfile)
	rightKeyCardinality := getCardinality(h.rightJoinKeys, h.rightSchema, h.rightProfile)
	count := h.leftProfile.RowCount * h.rightProfile.RowCount / math.Max(leftKeyCardinality, rightKeyCardinality)
	return count
}

func (la *LogicalApply) getGroupNDVs(colGroups [][]*expression.DeferredCauset, childStats []*property.StatsInfo) []property.GroupNDV {
	if len(colGroups) > 0 && (la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin || la.JoinType == LeftOuterJoin) {
		return childStats[0].GroupNDVs
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (la *LogicalApply) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if la.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		la.stats.GroupNDVs = la.getGroupNDVs(colGroups, childStats)
		return la.stats, nil
	}
	leftProfile := childStats[0]
	la.stats = &property.StatsInfo{
		RowCount:    leftProfile.RowCount,
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	for id, c := range leftProfile.Cardinality {
		la.stats.Cardinality[id] = c
	}
	if la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		la.stats.Cardinality[selfSchema.DeferredCausets[selfSchema.Len()-1].UniqueID] = 2.0
	} else {
		for i := childSchema[0].Len(); i < selfSchema.Len(); i++ {
			la.stats.Cardinality[selfSchema.DeferredCausets[i].UniqueID] = leftProfile.RowCount
		}
	}
	la.stats.GroupNDVs = la.getGroupNDVs(colGroups, childStats)
	return la.stats, nil
}

// ExtractDefCausGroups implements LogicalPlan ExtractDefCausGroups interface.
func (la *LogicalApply) ExtractDefCausGroups(colGroups [][]*expression.DeferredCauset) [][]*expression.DeferredCauset {
	var outerSchema *expression.Schema
	// Apply doesn't have RightOuterJoin.
	if la.JoinType == LeftOuterJoin || la.JoinType == LeftOuterSemiJoin || la.JoinType == AntiLeftOuterSemiJoin {
		outerSchema = la.Children()[0].Schema()
	}
	if len(colGroups) == 0 || outerSchema == nil {
		return nil
	}
	_, offsets := outerSchema.ExtractDefCausGroups(colGroups)
	if len(offsets) == 0 {
		return nil
	}
	extracted := make([][]*expression.DeferredCauset, len(offsets))
	for i, offset := range offsets {
		extracted[i] = colGroups[offset]
	}
	return extracted
}

// Exists and MaxOneRow produce at most one event, so we set the RowCount of stats one.
func getSingletonStats(schemaReplicant *expression.Schema) *property.StatsInfo {
	ret := &property.StatsInfo{
		RowCount:    1.0,
		Cardinality: make(map[int64]float64, schemaReplicant.Len()),
	}
	for _, col := range schemaReplicant.DeferredCausets {
		ret.Cardinality[col.UniqueID] = 1
	}
	return ret
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalMaxOneRow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, _ [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		return p.stats, nil
	}
	p.stats = getSingletonStats(selfSchema)
	return p.stats, nil
}

func (p *LogicalWindow) getGroupNDVs(colGroups [][]*expression.DeferredCauset, childStats []*property.StatsInfo) []property.GroupNDV {
	if len(colGroups) > 0 {
		return childStats[0].GroupNDVs
	}
	return nil
}

// DeriveStats implement LogicalPlan DeriveStats interface.
func (p *LogicalWindow) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.DeferredCauset) (*property.StatsInfo, error) {
	if p.stats != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
		return p.stats, nil
	}
	childProfile := childStats[0]
	p.stats = &property.StatsInfo{
		RowCount:    childProfile.RowCount,
		Cardinality: make(map[int64]float64, selfSchema.Len()),
	}
	childLen := selfSchema.Len() - len(p.WindowFuncDescs)
	for i := 0; i < childLen; i++ {
		id := selfSchema.DeferredCausets[i].UniqueID
		p.stats.Cardinality[id] = childProfile.Cardinality[id]
	}
	for i := childLen; i < selfSchema.Len(); i++ {
		p.stats.Cardinality[selfSchema.DeferredCausets[i].UniqueID] = childProfile.RowCount
	}
	p.stats.GroupNDVs = p.getGroupNDVs(colGroups, childStats)
	return p.stats, nil
}

// ExtractDefCausGroups implements LogicalPlan ExtractDefCausGroups interface.
func (p *LogicalWindow) ExtractDefCausGroups(colGroups [][]*expression.DeferredCauset) [][]*expression.DeferredCauset {
	if len(colGroups) == 0 {
		return nil
	}
	childSchema := p.Children()[0].Schema()
	_, offsets := childSchema.ExtractDefCausGroups(colGroups)
	if len(offsets) == 0 {
		return nil
	}
	extracted := make([][]*expression.DeferredCauset, len(offsets))
	for i, offset := range offsets {
		extracted[i] = colGroups[offset]
	}
	return extracted
}
