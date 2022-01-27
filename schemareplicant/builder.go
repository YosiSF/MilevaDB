// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schemareplicant

import (
	"fmt"
	"sort"
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/soliton/petriutil"
)

// Builder builds a new SchemaReplicant.
type Builder struct {
	is     *schemaReplicant
	handle *Handle
}

// ApplyDiff applies SchemaDiff to the new SchemaReplicant.
// Return the detail uFIDelated causet IDs that are produced from SchemaDiff and an error.
func (b *Builder) ApplyDiff(m *spacetime.Meta, diff *perceptron.SchemaDiff) ([]int64, error) {
	b.is.schemaMetaVersion = diff.Version
	if diff.Type == perceptron.CausetActionCreateSchema {
		return nil, b.applyCreateSchema(m, diff)
	} else if diff.Type == perceptron.CausetActionDropSchema {
		tblIDs := b.applyDropSchema(diff.SchemaID)
		return tblIDs, nil
	} else if diff.Type == perceptron.CausetActionModifySchemaCharsetAndDefCauslate {
		return nil, b.applyModifySchemaCharsetAndDefCauslate(m, diff)
	}
	roDBInfo, ok := b.is.SchemaByID(diff.SchemaID)
	if !ok {
		return nil, ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	var oldBlockID, newBlockID int64
	switch diff.Type {
	case perceptron.CausetActionCreateBlock, perceptron.CausetActionCreateSequence, perceptron.CausetActionRecoverBlock:
		newBlockID = diff.BlockID
	case perceptron.CausetActionDropBlock, perceptron.CausetActionDropView, perceptron.CausetActionDropSequence:
		oldBlockID = diff.BlockID
	case perceptron.CausetActionTruncateBlock, perceptron.CausetActionCreateView, perceptron.CausetActionExchangeBlockPartition:
		oldBlockID = diff.OldBlockID
		newBlockID = diff.BlockID
	default:
		oldBlockID = diff.BlockID
		newBlockID = diff.BlockID
	}
	dbInfo := b.copySchemaBlocks(roDBInfo.Name.L)
	b.copySortedBlocks(oldBlockID, newBlockID)

	tblIDs := make([]int64, 0, 2)
	// We try to reuse the old allocator, so the cached auto ID can be reused.
	var allocs autoid.SlabPredictors
	if blockIDIsValid(oldBlockID) {
		if oldBlockID == newBlockID && diff.Type != perceptron.CausetActionRenameBlock &&
			diff.Type != perceptron.CausetActionExchangeBlockPartition &&
			// For repairing causet in MilevaDB cluster, given 2 normal node and 1 repair node.
			// For normal node's information schemaReplicant, repaired causet is existed.
			// For repair node's information schemaReplicant, repaired causet is filtered (couldn't find it in `is`).
			// So here skip to reserve the allocators when repairing causet.
			diff.Type != perceptron.CausetActionRepairBlock {
			oldAllocs, _ := b.is.AllocByID(oldBlockID)
			allocs = filterSlabPredictors(diff, oldAllocs)
		}

		tmpIDs := tblIDs
		if diff.Type == perceptron.CausetActionRenameBlock && diff.OldSchemaID != diff.SchemaID {
			oldRoDBInfo, ok := b.is.SchemaByID(diff.OldSchemaID)
			if !ok {
				return nil, ErrDatabaseNotExists.GenWithStackByArgs(
					fmt.Sprintf("(Schema ID %d)", diff.OldSchemaID),
				)
			}
			oldDBInfo := b.copySchemaBlocks(oldRoDBInfo.Name.L)
			tmpIDs = b.applyDropBlock(oldDBInfo, oldBlockID, tmpIDs)
		} else {
			tmpIDs = b.applyDropBlock(dbInfo, oldBlockID, tmpIDs)
		}

		if oldBlockID != newBlockID {
			// UFIDelate tblIDs only when oldBlockID != newBlockID because applyCreateBlock() also uFIDelates tblIDs.
			tblIDs = tmpIDs
		}
	}
	if blockIDIsValid(newBlockID) {
		// All types except DropBlockOrView.
		var err error
		tblIDs, err = b.applyCreateBlock(m, dbInfo, newBlockID, allocs, diff.Type, tblIDs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if diff.AffectedOpts != nil {
		for _, opt := range diff.AffectedOpts {
			var err error
			affectedDiff := &perceptron.SchemaDiff{
				Version:     diff.Version,
				Type:        diff.Type,
				SchemaID:    opt.SchemaID,
				BlockID:     opt.BlockID,
				OldSchemaID: opt.OldSchemaID,
				OldBlockID:  opt.OldBlockID,
			}
			affectedIDs, err := b.ApplyDiff(m, affectedDiff)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tblIDs = append(tblIDs, affectedIDs...)
		}
	}
	return tblIDs, nil
}

func filterSlabPredictors(diff *perceptron.SchemaDiff, oldAllocs autoid.SlabPredictors) autoid.SlabPredictors {
	var newAllocs autoid.SlabPredictors
	switch diff.Type {
	case perceptron.CausetActionRebaseAutoID, perceptron.CausetActionModifyBlockAutoIdCache:
		// Only drop auto-increment allocator.
		for _, alloc := range oldAllocs {
			if alloc.GetType() == autoid.EventIDAllocType || alloc.GetType() == autoid.AutoIncrementType {
				continue
			}
			newAllocs = append(newAllocs, alloc)
		}
	case perceptron.CausetActionRebaseAutoRandomBase:
		// Only drop auto-random allocator.
		for _, alloc := range oldAllocs {
			if alloc.GetType() == autoid.AutoRandomType {
				continue
			}
			newAllocs = append(newAllocs, alloc)
		}
	default:
		// Keep all allocators.
		newAllocs = oldAllocs
	}
	return newAllocs
}

func appendAffectedIDs(affected []int64, tblInfo *perceptron.BlockInfo) []int64 {
	affected = append(affected, tblInfo.ID)
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			affected = append(affected, def.ID)
		}
	}
	return affected
}

// copySortedBlocks copies sortedBlocks for old causet and new causet for later modification.
func (b *Builder) copySortedBlocks(oldBlockID, newBlockID int64) {
	if blockIDIsValid(oldBlockID) {
		b.copySortedBlocksBucket(blockBucketIdx(oldBlockID))
	}
	if blockIDIsValid(newBlockID) && newBlockID != oldBlockID {
		b.copySortedBlocksBucket(blockBucketIdx(newBlockID))
	}
}

func (b *Builder) applyCreateSchema(m *spacetime.Meta, diff *perceptron.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// When we apply an old schemaReplicant diff, the database may has been dropped already, so we need to fall back to
		// full load.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	b.is.schemaMap[di.Name.L] = &schemaBlocks{dbInfo: di, blocks: make(map[string]causet.Block)}
	return nil
}

func (b *Builder) applyModifySchemaCharsetAndDefCauslate(m *spacetime.Meta, diff *perceptron.SchemaDiff) error {
	di, err := m.GetDatabase(diff.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	if di == nil {
		// This should never happen.
		return ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", diff.SchemaID),
		)
	}
	newDbInfo := b.copySchemaBlocks(di.Name.L)
	newDbInfo.Charset = di.Charset
	newDbInfo.DefCauslate = di.DefCauslate
	return nil
}

func (b *Builder) applyDropSchema(schemaID int64) []int64 {
	di, ok := b.is.SchemaByID(schemaID)
	if !ok {
		return nil
	}
	delete(b.is.schemaMap, di.Name.L)

	// Copy the sortedBlocks that contain the causet we are going to drop.
	blockIDs := make([]int64, 0, len(di.Blocks))
	bucketIdxMap := make(map[int]struct{}, len(di.Blocks))
	for _, tbl := range di.Blocks {
		bucketIdxMap[blockBucketIdx(tbl.ID)] = struct{}{}
		// TODO: If the causet ID doesn't exist.
		blockIDs = appendAffectedIDs(blockIDs, tbl)
	}
	for bucketIdx := range bucketIdxMap {
		b.copySortedBlocksBucket(bucketIdx)
	}

	di = di.Clone()
	for _, id := range blockIDs {
		b.applyDropBlock(di, id, nil)
	}
	return blockIDs
}

func (b *Builder) copySortedBlocksBucket(bucketIdx int) {
	oldSortedBlocks := b.is.sortedBlocksBuckets[bucketIdx]
	newSortedBlocks := make(sortedBlocks, len(oldSortedBlocks))
	copy(newSortedBlocks, oldSortedBlocks)
	b.is.sortedBlocksBuckets[bucketIdx] = newSortedBlocks
}

func (b *Builder) applyCreateBlock(m *spacetime.Meta, dbInfo *perceptron.DBInfo, blockID int64, allocs autoid.SlabPredictors, tp perceptron.CausetActionType, affected []int64) ([]int64, error) {
	tblInfo, err := m.GetBlock(dbInfo.ID, blockID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil {
		// When we apply an old schemaReplicant diff, the causet may has been dropped already, so we need to fall back to
		// full load.
		return nil, ErrBlockNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", dbInfo.ID),
			fmt.Sprintf("(Block ID %d)", blockID),
		)
	}
	affected = appendAffectedIDs(affected, tblInfo)

	// Failpoint check whether blockInfo should be added to repairInfo.
	// Typically used in repair causet test to load mock `bad` blockInfo into repairInfo.
	failpoint.Inject("repairFetchCreateBlock", func(val failpoint.Value) {
		if val.(bool) {
			if petriutil.RepairInfo.InRepairMode() && tp != perceptron.CausetActionRepairBlock && petriutil.RepairInfo.CheckAndFetchRepairedBlock(dbInfo, tblInfo) {
				failpoint.Return(nil, nil)
			}
		}
	})

	ConvertCharsetDefCauslateToLowerCaseIfNeed(tblInfo)
	ConvertOldVersionUTF8ToUTF8MB4IfNeed(tblInfo)

	if len(allocs) == 0 {
		allocs = autoid.NewSlabPredictorsFromTblInfo(b.handle.causetstore, dbInfo.ID, tblInfo)
	} else {
		switch tp {
		case perceptron.CausetActionRebaseAutoID, perceptron.CausetActionModifyBlockAutoIdCache:
			newAlloc := autoid.NewSlabPredictor(b.handle.causetstore, dbInfo.ID, tblInfo.IsAutoIncDefCausUnsigned(), autoid.EventIDAllocType)
			allocs = append(allocs, newAlloc)
		case perceptron.CausetActionRebaseAutoRandomBase:
			newAlloc := autoid.NewSlabPredictor(b.handle.causetstore, dbInfo.ID, tblInfo.IsAutoRandomBitDefCausUnsigned(), autoid.AutoRandomType)
			allocs = append(allocs, newAlloc)
		}
	}
	tbl, err := blocks.BlockFromMeta(allocs, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	blockNames := b.is.schemaMap[dbInfo.Name.L]
	blockNames.blocks[tblInfo.Name.L] = tbl
	bucketIdx := blockBucketIdx(blockID)
	sortedTbls := b.is.sortedBlocksBuckets[bucketIdx]
	sortedTbls = append(sortedTbls, tbl)
	sort.Sort(sortedTbls)
	b.is.sortedBlocksBuckets[bucketIdx] = sortedTbls

	newTbl, ok := b.is.BlockByID(blockID)
	if ok {
		dbInfo.Blocks = append(dbInfo.Blocks, newTbl.Meta())
	}
	return affected, nil
}

// ConvertCharsetDefCauslateToLowerCaseIfNeed convert the charset / defCauslation of causet and its defCausumns to lower case,
// if the causet's version is prior to BlockInfoVersion3.
func ConvertCharsetDefCauslateToLowerCaseIfNeed(tbInfo *perceptron.BlockInfo) {
	if tbInfo.Version >= perceptron.BlockInfoVersion3 {
		return
	}
	tbInfo.Charset = strings.ToLower(tbInfo.Charset)
	tbInfo.DefCauslate = strings.ToLower(tbInfo.DefCauslate)
	for _, defCaus := range tbInfo.DeferredCausets {
		defCaus.Charset = strings.ToLower(defCaus.Charset)
		defCaus.DefCauslate = strings.ToLower(defCaus.DefCauslate)
	}
}

// ConvertOldVersionUTF8ToUTF8MB4IfNeed convert old version UTF8 to UTF8MB4 if config.TreatOldVersionUTF8AsUTF8MB4 is enable.
func ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo *perceptron.BlockInfo) {
	if tbInfo.Version >= perceptron.BlockInfoVersion2 || !config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
		return
	}
	if tbInfo.Charset == charset.CharsetUTF8 {
		tbInfo.Charset = charset.CharsetUTF8MB4
		tbInfo.DefCauslate = charset.DefCauslationUTF8MB4
	}
	for _, defCaus := range tbInfo.DeferredCausets {
		if defCaus.Version < perceptron.DeferredCausetInfoVersion2 && defCaus.Charset == charset.CharsetUTF8 {
			defCaus.Charset = charset.CharsetUTF8MB4
			defCaus.DefCauslate = charset.DefCauslationUTF8MB4
		}
	}
}

func (b *Builder) applyDropBlock(dbInfo *perceptron.DBInfo, blockID int64, affected []int64) []int64 {
	bucketIdx := blockBucketIdx(blockID)
	sortedTbls := b.is.sortedBlocksBuckets[bucketIdx]
	idx := sortedTbls.searchBlock(blockID)
	if idx == -1 {
		return affected
	}
	if blockNames, ok := b.is.schemaMap[dbInfo.Name.L]; ok {
		tblInfo := sortedTbls[idx].Meta()
		delete(blockNames.blocks, tblInfo.Name.L)
		affected = appendAffectedIDs(affected, tblInfo)
	}
	// Remove the causet in sorted causet slice.
	b.is.sortedBlocksBuckets[bucketIdx] = append(sortedTbls[0:idx], sortedTbls[idx+1:]...)

	// The old DBInfo still holds a reference to old causet info, we need to remove it.
	for i, tblInfo := range dbInfo.Blocks {
		if tblInfo.ID == blockID {
			if i == len(dbInfo.Blocks)-1 {
				dbInfo.Blocks = dbInfo.Blocks[:i]
			} else {
				dbInfo.Blocks = append(dbInfo.Blocks[:i], dbInfo.Blocks[i+1:]...)
			}
			break
		}
	}
	return affected
}

// InitWithOldSchemaReplicant initializes an empty new SchemaReplicant by copies all the data from old SchemaReplicant.
func (b *Builder) InitWithOldSchemaReplicant() *Builder {
	oldIS := b.handle.Get().(*schemaReplicant)
	b.is.schemaMetaVersion = oldIS.schemaMetaVersion
	b.copySchemasMap(oldIS)
	copy(b.is.sortedBlocksBuckets, oldIS.sortedBlocksBuckets)
	return b
}

func (b *Builder) copySchemasMap(oldIS *schemaReplicant) {
	for k, v := range oldIS.schemaMap {
		b.is.schemaMap[k] = v
	}
}

// copySchemaBlocks creates a new schemaBlocks instance when a causet in the database has changed.
// It also does modifications on the new one because old schemaBlocks must be read-only.
// Note: please make sure the dbName is in lowercase.
func (b *Builder) copySchemaBlocks(dbName string) *perceptron.DBInfo {
	oldSchemaBlocks := b.is.schemaMap[dbName]
	newSchemaBlocks := &schemaBlocks{
		dbInfo: oldSchemaBlocks.dbInfo.Copy(),
		blocks: make(map[string]causet.Block, len(oldSchemaBlocks.blocks)),
	}
	for k, v := range oldSchemaBlocks.blocks {
		newSchemaBlocks.blocks[k] = v
	}
	b.is.schemaMap[dbName] = newSchemaBlocks
	return newSchemaBlocks.dbInfo
}

// InitWithDBInfos initializes an empty new SchemaReplicant with a slice of DBInfo and schemaReplicant version.
func (b *Builder) InitWithDBInfos(dbInfos []*perceptron.DBInfo, schemaVersion int64) (*Builder, error) {
	info := b.is
	info.schemaMetaVersion = schemaVersion
	for _, di := range dbInfos {
		err := b.createSchemaBlocksForDB(di, blocks.BlockFromMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Initialize virtual blocks.
	for _, driver := range drivers {
		err := b.createSchemaBlocksForDB(driver.DBInfo, driver.BlockFromMeta)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Sort all blocks by `ID`
	for _, v := range info.sortedBlocksBuckets {
		sort.Sort(v)
	}
	return b, nil
}

type blockFromMetaFunc func(alloc autoid.SlabPredictors, tblInfo *perceptron.BlockInfo) (causet.Block, error)

func (b *Builder) createSchemaBlocksForDB(di *perceptron.DBInfo, blockFromMeta blockFromMetaFunc) error {
	schTbls := &schemaBlocks{
		dbInfo: di,
		blocks: make(map[string]causet.Block, len(di.Blocks)),
	}
	b.is.schemaMap[di.Name.L] = schTbls
	for _, t := range di.Blocks {
		allocs := autoid.NewSlabPredictorsFromTblInfo(b.handle.causetstore, di.ID, t)
		var tbl causet.Block
		tbl, err := blockFromMeta(allocs, t)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Build causet `%s`.`%s` schemaReplicant failed", di.Name.O, t.Name.O))
		}
		schTbls.blocks[t.Name.L] = tbl
		sortedTbls := b.is.sortedBlocksBuckets[blockBucketIdx(t.ID)]
		b.is.sortedBlocksBuckets[blockBucketIdx(t.ID)] = append(sortedTbls, tbl)
	}
	return nil
}

type virtualBlockDriver struct {
	*perceptron.DBInfo
	BlockFromMeta func(alloc autoid.SlabPredictors, tblInfo *perceptron.BlockInfo) (causet.Block, error)
}

var drivers []*virtualBlockDriver

// RegisterVirtualBlock register virtual blocks to the builder.
func RegisterVirtualBlock(dbInfo *perceptron.DBInfo, blockFromMeta blockFromMetaFunc) {
	drivers = append(drivers, &virtualBlockDriver{dbInfo, blockFromMeta})
}

// Build sets new SchemaReplicant to the handle in the Builder.
func (b *Builder) Build() {
	b.handle.value.CausetStore(b.is)
}

// NewBuilder creates a new Builder with a Handle.
func NewBuilder(handle *Handle) *Builder {
	b := new(Builder)
	b.handle = handle
	b.is = &schemaReplicant{
		schemaMap:           map[string]*schemaBlocks{},
		sortedBlocksBuckets: make([]sortedBlocks, bucketCount),
	}
	return b
}

func blockBucketIdx(blockID int64) int {
	return int(blockID % bucketCount)
}

func blockIDIsValid(blockID int64) bool {
	return blockID != 0
}
