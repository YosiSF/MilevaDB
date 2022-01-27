// INTERLOCKyright 2020 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package blocks

import (
	"context"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-binlog"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/generatedexpr"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

// BlockCommon is shared by both Block and partition.
type BlockCommon struct {
	blockID int64
	// physicalBlockID is a unique int64 to identify a physical block.
	physicalBlockID                             int64
	DeferredCausets                             []*block.DeferredCauset
	PublicDeferredCausets                       []*block.DeferredCauset
	VisibleDeferredCausets                      []*block.DeferredCauset
	HiddenDeferredCausets                       []*block.DeferredCauset
	WriblockDeferredCausets                     []*block.DeferredCauset
	FullHiddenDefCaussAndVisibleDeferredCausets []*block.DeferredCauset
	wriblockIndices                             []block.Index
	indices                                     []block.Index
	meta                                        *perceptron.BlockInfo
	allocs                                      autoid.SlabPredictors
	sequence                                    *sequenceCommon

	// recordPrefix and indexPrefix are generated using physicalBlockID.
	recordPrefix ekv.Key
	indexPrefix  ekv.Key
}

// MockBlockFromMeta only serves for test.
func MockBlockFromMeta(tblInfo *perceptron.BlockInfo) block.Block {
	defCausumns := make([]*block.DeferredCauset, 0, len(tblInfo.DeferredCausets))
	for _, defCausInfo := range tblInfo.DeferredCausets {
		defCaus := block.ToDeferredCauset(defCausInfo)
		defCausumns = append(defCausumns, defCaus)
	}

	var t BlockCommon
	initBlockCommon(&t, tblInfo, tblInfo.ID, defCausumns, nil)
	if tblInfo.GetPartitionInfo() == nil {
		if err := initBlockIndices(&t); err != nil {
			return nil
		}
		return &t
	}

	ret, err := newPartitionedBlock(&t, tblInfo)
	if err != nil {
		return nil
	}
	return ret
}

// BlockFromMeta creates a Block instance from perceptron.BlockInfo.
func BlockFromMeta(allocs autoid.SlabPredictors, tblInfo *perceptron.BlockInfo) (block.Block, error) {
	if tblInfo.State == perceptron.StateNone {
		return nil, block.ErrBlockStateCantNone.GenWithStackByArgs(tblInfo.Name)
	}

	defcausLen := len(tblInfo.DeferredCausets)
	defCausumns := make([]*block.DeferredCauset, 0, defcausLen)
	for i, defCausInfo := range tblInfo.DeferredCausets {
		if defCausInfo.State == perceptron.StateNone {
			return nil, block.ErrDeferredCausetStateCantNone.GenWithStackByArgs(defCausInfo.Name)
		}

		// Print some information when the defCausumn's offset isn't equal to i.
		if defCausInfo.Offset != i {
			logutil.BgLogger().Error("wrong block schemaReplicant", zap.Any("block", tblInfo), zap.Any("defCausumn", defCausInfo), zap.Int("index", i), zap.Int("offset", defCausInfo.Offset), zap.Int("defCausumnNumber", defcausLen))
		}

		defCaus := block.ToDeferredCauset(defCausInfo)
		if defCaus.IsGenerated() {
			expr, err := generatedexpr.ParseExpression(defCausInfo.GeneratedExprString)
			if err != nil {
				return nil, err
			}
			expr, err = generatedexpr.SimpleResolveName(expr, tblInfo)
			if err != nil {
				return nil, err
			}
			defCaus.GeneratedExpr = expr
		}
		// default value is expr.
		if defCaus.DefaultIsExpr {
			expr, err := generatedexpr.ParseExpression(defCausInfo.DefaultValue.(string))
			if err != nil {
				return nil, err
			}
			defCaus.DefaultExpr = expr
		}
		defCausumns = append(defCausumns, defCaus)
	}

	var t BlockCommon
	initBlockCommon(&t, tblInfo, tblInfo.ID, defCausumns, allocs)
	if tblInfo.GetPartitionInfo() == nil {
		if err := initBlockIndices(&t); err != nil {
			return nil, err
		}
		return &t, nil
	}

	return newPartitionedBlock(&t, tblInfo)
}

// initBlockCommon initializes a BlockCommon struct.
func initBlockCommon(t *BlockCommon, tblInfo *perceptron.BlockInfo, physicalBlockID int64, defcaus []*block.DeferredCauset, allocs autoid.SlabPredictors) {
	t.blockID = tblInfo.ID
	t.physicalBlockID = physicalBlockID
	t.allocs = allocs
	t.meta = tblInfo
	t.DeferredCausets = defcaus
	t.PublicDeferredCausets = t.DefCauss()
	t.VisibleDeferredCausets = t.VisibleDefCauss()
	t.HiddenDeferredCausets = t.HiddenDefCauss()
	t.WriblockDeferredCausets = t.WriblockDefCauss()
	t.FullHiddenDefCaussAndVisibleDeferredCausets = t.FullHiddenDefCaussAndVisibleDefCauss()
	t.wriblockIndices = t.WriblockIndices()
	t.recordPrefix = blockcodec.GenBlockRecordPrefix(physicalBlockID)
	t.indexPrefix = blockcodec.GenBlockIndexPrefix(physicalBlockID)
	if tblInfo.IsSequence() {
		t.sequence = &sequenceCommon{meta: tblInfo.Sequence}
	}
}

// initBlockIndices initializes the indices of the BlockCommon.
func initBlockIndices(t *BlockCommon) error {
	tblInfo := t.meta
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.State == perceptron.StateNone {
			return block.ErrIndexStateCantNone.GenWithStackByArgs(idxInfo.Name)
		}

		// Use partition ID for index, because BlockCommon may be block or partition.
		idx := NewIndex(t.physicalBlockID, tblInfo, idxInfo)
		t.indices = append(t.indices, idx)
	}
	t.wriblockIndices = t.WriblockIndices()
	return nil
}

func initBlockCommonWithIndices(t *BlockCommon, tblInfo *perceptron.BlockInfo, physicalBlockID int64, defcaus []*block.DeferredCauset, allocs autoid.SlabPredictors) error {
	initBlockCommon(t, tblInfo, physicalBlockID, defcaus, allocs)
	return initBlockIndices(t)
}

// Indices implements block.Block Indices interface.
func (t *BlockCommon) Indices() []block.Index {
	return t.indices
}

// WriblockIndices implements block.Block WriblockIndices interface.
func (t *BlockCommon) WriblockIndices() []block.Index {
	if len(t.wriblockIndices) > 0 {
		return t.wriblockIndices
	}
	wriblock := make([]block.Index, 0, len(t.indices))
	for _, index := range t.indices {
		s := index.Meta().State
		if s != perceptron.StateDeleteOnly && s != perceptron.StateDeleteReorganization {
			wriblock = append(wriblock, index)
		}
	}
	return wriblock
}

// GetWriblockIndexByName gets the index meta from the block by the index name.
func GetWriblockIndexByName(idxName string, t block.Block) block.Index {
	indices := t.WriblockIndices()
	for _, idx := range indices {
		if idxName == idx.Meta().Name.L {
			return idx
		}
	}
	return nil
}

// DeleblockIndices implements block.Block DeleblockIndices interface.
func (t *BlockCommon) DeleblockIndices() []block.Index {
	// All indices are deleblock because we don't need to check StateNone.
	return t.indices
}

// Meta implements block.Block Meta interface.
func (t *BlockCommon) Meta() *perceptron.BlockInfo {
	return t.meta
}

// GetPhysicalID implements block.Block GetPhysicalID interface.
func (t *BlockCommon) GetPhysicalID() int64 {
	return t.physicalBlockID
}

type getDefCaussMode int64

const (
	_ getDefCaussMode = iota
	visible
	hidden
	full
)

func (t *BlockCommon) getDefCauss(mode getDefCaussMode) []*block.DeferredCauset {
	defCausumns := make([]*block.DeferredCauset, 0, len(t.DeferredCausets))
	for _, defCaus := range t.DeferredCausets {
		if defCaus.State != perceptron.StatePublic {
			continue
		}
		if (mode == visible && defCaus.Hidden) || (mode == hidden && !defCaus.Hidden) {
			continue
		}
		defCausumns = append(defCausumns, defCaus)
	}
	return defCausumns
}

// DefCauss implements block.Block DefCauss interface.
func (t *BlockCommon) DefCauss() []*block.DeferredCauset {
	if len(t.PublicDeferredCausets) > 0 {
		return t.PublicDeferredCausets
	}
	return t.getDefCauss(full)
}

// VisibleDefCauss implements block.Block VisibleDefCauss interface.
func (t *BlockCommon) VisibleDefCauss() []*block.DeferredCauset {
	if len(t.VisibleDeferredCausets) > 0 {
		return t.VisibleDeferredCausets
	}
	return t.getDefCauss(visible)
}

// HiddenDefCauss implements block.Block HiddenDefCauss interface.
func (t *BlockCommon) HiddenDefCauss() []*block.DeferredCauset {
	if len(t.HiddenDeferredCausets) > 0 {
		return t.HiddenDeferredCausets
	}
	return t.getDefCauss(hidden)
}

// WriblockDefCauss implements block WriblockDefCauss interface.
func (t *BlockCommon) WriblockDefCauss() []*block.DeferredCauset {
	if len(t.WriblockDeferredCausets) > 0 {
		return t.WriblockDeferredCausets
	}
	wriblockDeferredCausets := make([]*block.DeferredCauset, 0, len(t.DeferredCausets))
	for _, defCaus := range t.DeferredCausets {
		if defCaus.State == perceptron.StateDeleteOnly || defCaus.State == perceptron.StateDeleteReorganization {
			continue
		}
		wriblockDeferredCausets = append(wriblockDeferredCausets, defCaus)
	}
	return wriblockDeferredCausets
}

// FullHiddenDefCaussAndVisibleDefCauss implements block FullHiddenDefCaussAndVisibleDefCauss interface.
func (t *BlockCommon) FullHiddenDefCaussAndVisibleDefCauss() []*block.DeferredCauset {
	if len(t.FullHiddenDefCaussAndVisibleDeferredCausets) > 0 {
		return t.FullHiddenDefCaussAndVisibleDeferredCausets
	}

	defcaus := make([]*block.DeferredCauset, 0, len(t.DeferredCausets))
	for _, defCaus := range t.DeferredCausets {
		if defCaus.Hidden || defCaus.State == perceptron.StatePublic {
			defcaus = append(defcaus, defCaus)
		}
	}
	return defcaus
}

// RecordPrefix implements block.Block interface.
func (t *BlockCommon) RecordPrefix() ekv.Key {
	return t.recordPrefix
}

// IndexPrefix implements block.Block interface.
func (t *BlockCommon) IndexPrefix() ekv.Key {
	return t.indexPrefix
}

// RecordKey implements block.Block interface.
func (t *BlockCommon) RecordKey(h ekv.Handle) ekv.Key {
	return blockcodec.EncodeRecordKey(t.recordPrefix, h)
}

// FirstKey implements block.Block interface.
func (t *BlockCommon) FirstKey() ekv.Key {
	return t.RecordKey(ekv.IntHandle(math.MinInt64))
}

// UFIDelateRecord implements block.Block UFIDelateRecord interface.
// `touched` means which defCausumns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WriblockDefCauss()`.
func (t *BlockCommon) UFIDelateRecord(ctx context.Context, sctx stochastikctx.Context, h ekv.Handle, oldData, newData []types.Causet, touched []bool) error {
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}

	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	var defCausIDs, binlogDefCausIDs []int64
	var event, binlogOldRow, binlogNewRow []types.Causet
	numDefCaussCap := len(newData) + 1 // +1 for the extra handle defCausumn that we may need to append.
	defCausIDs = make([]int64, 0, numDefCaussCap)
	event = make([]types.Causet, 0, numDefCaussCap)
	if shouldWriteBinlog(sctx) {
		binlogDefCausIDs = make([]int64, 0, numDefCaussCap)
		binlogOldRow = make([]types.Causet, 0, numDefCaussCap)
		binlogNewRow = make([]types.Causet, 0, numDefCaussCap)
	}

	for _, defCaus := range t.DeferredCausets {
		var value types.Causet
		if defCaus.State == perceptron.StateDeleteOnly || defCaus.State == perceptron.StateDeleteReorganization {
			if defCaus.ChangeStateInfo != nil {
				// TODO: Check overflow or ignoreTruncate.
				value, err = block.CastValue(sctx, oldData[defCaus.DependencyDeferredCausetOffset], defCaus.DeferredCausetInfo, false, false)
				if err != nil {
					logutil.BgLogger().Info("uFIDelate record cast value failed", zap.Any("defCaus", defCaus), zap.Uint64("txnStartTS", txn.StartTS()),
						zap.String("handle", h.String()), zap.Any("val", oldData[defCaus.DependencyDeferredCausetOffset]), zap.Error(err))
				}
				oldData = append(oldData, value)
				touched = append(touched, touched[defCaus.DependencyDeferredCausetOffset])
			}
			continue
		}
		if defCaus.State != perceptron.StatePublic {
			// If defCaus is in write only or write reorganization state we should keep the oldData.
			// Because the oldData must be the original data(it's changed by other MilevaDBs.) or the original default value.
			// TODO: Use newData directly.
			value = oldData[defCaus.Offset]
			if defCaus.ChangeStateInfo != nil {
				// TODO: Check overflow or ignoreTruncate.
				value, err = block.CastValue(sctx, newData[defCaus.DependencyDeferredCausetOffset], defCaus.DeferredCausetInfo, false, false)
				if err != nil {
					return err
				}
				newData[defCaus.Offset] = value
				touched[defCaus.Offset] = touched[defCaus.DependencyDeferredCausetOffset]
			}
		} else {
			value = newData[defCaus.Offset]
		}
		if !t.canSkip(defCaus, &value) {
			defCausIDs = append(defCausIDs, defCaus.ID)
			event = append(event, value)
		}
		if shouldWriteBinlog(sctx) && !t.canSkipUFIDelateBinlog(defCaus, value) {
			binlogDefCausIDs = append(binlogDefCausIDs, defCaus.ID)
			binlogOldRow = append(binlogOldRow, oldData[defCaus.Offset])
			binlogNewRow = append(binlogNewRow, value)
		}
	}

	// rebuild index
	err = t.rebuildIndices(sctx, txn, h, touched, oldData, newData, block.WithCtx(ctx))
	if err != nil {
		return err
	}

	key := t.RecordKey(h)
	sessVars := sctx.GetStochastikVars()
	sc, rd := sessVars.StmtCtx, &sessVars.RowEncoder
	value, err := blockcodec.EncodeRow(sc, event, defCausIDs, nil, nil, rd)
	if err != nil {
		return err
	}
	if err = memBuffer.Set(key, value); err != nil {
		return err
	}
	memBuffer.Release(sh)
	if shouldWriteBinlog(sctx) {
		if !t.meta.PKIsHandle {
			binlogDefCausIDs = append(binlogDefCausIDs, perceptron.ExtraHandleID)
			binlogOldRow = append(binlogOldRow, types.NewIntCauset(h.IntValue()))
			binlogNewRow = append(binlogNewRow, types.NewIntCauset(h.IntValue()))
		}
		err = t.addUFIDelateBinlog(sctx, binlogOldRow, binlogNewRow, binlogDefCausIDs)
		if err != nil {
			return err
		}
	}
	defCausSize := make(map[int64]int64, len(t.DefCauss()))
	for id, defCaus := range t.DefCauss() {
		size, err := codec.EstimateValueSize(sc, newData[id])
		if err != nil {
			continue
		}
		newLen := size - 1
		size, err = codec.EstimateValueSize(sc, oldData[id])
		if err != nil {
			continue
		}
		oldLen := size - 1
		defCausSize[defCaus.ID] = int64(newLen - oldLen)
	}
	sessVars.TxnCtx.UFIDelateDeltaForBlock(t.physicalBlockID, 0, 1, defCausSize)
	return nil
}

func (t *BlockCommon) rebuildIndices(ctx stochastikctx.Context, txn ekv.Transaction, h ekv.Handle, touched []bool, oldData []types.Causet, newData []types.Causet, opts ...block.CreateIdxOptFunc) error {
	for _, idx := range t.DeleblockIndices() {
		if t.meta.IsCommonHandle && idx.Meta().Primary {
			continue
		}
		for _, ic := range idx.Meta().DeferredCausets {
			if !touched[ic.Offset] {
				continue
			}
			oldVs, err := idx.FetchValues(oldData, nil)
			if err != nil {
				return err
			}
			if err = t.removeRowIndex(ctx.GetStochastikVars().StmtCtx, h, oldVs, idx, txn); err != nil {
				return err
			}
			break
		}
	}
	for _, idx := range t.WriblockIndices() {
		if t.meta.IsCommonHandle && idx.Meta().Primary {
			continue
		}
		untouched := true
		for _, ic := range idx.Meta().DeferredCausets {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}
		// If txn is auto commit and index is untouched, no need to write index value.
		if untouched && !ctx.GetStochastikVars().InTxn() {
			continue
		}
		newVs, err := idx.FetchValues(newData, nil)
		if err != nil {
			return err
		}
		if err := t.buildIndexForRow(ctx, h, newVs, idx, txn, untouched, opts...); err != nil {
			return err
		}
	}
	return nil
}

// adjustRowValuesBuf adjust writeBufs.AddRowValues length, AddRowValues stores the inserting values that is used
// by blockcodec.EncodeRow, the encoded event format is `id1, defCausval, id2, defCausval`, so the correct length is rowLen * 2. If
// the inserting event has null value, AddRecord will skip it, so the rowLen will be different, so we need to adjust it.
func adjustRowValuesBuf(writeBufs *variable.WriteStmtBufs, rowLen int) {
	adjustLen := rowLen * 2
	if writeBufs.AddRowValues == nil || cap(writeBufs.AddRowValues) < adjustLen {
		writeBufs.AddRowValues = make([]types.Causet, adjustLen)
	}
	writeBufs.AddRowValues = writeBufs.AddRowValues[:adjustLen]
}

// FindPrimaryIndex uses to find primary index in blockInfo.
func FindPrimaryIndex(tblInfo *perceptron.BlockInfo) *perceptron.IndexInfo {
	var pkIdx *perceptron.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Primary {
			pkIdx = idx
			break
		}
	}
	return pkIdx
}

// CommonAddRecordCtx is used in `AddRecord` to avoid memory malloc for some temp slices.
// This is useful in lightning parse event data to key-values pairs. This can gain upto 5%  performance
// improvement in lightning's local mode.
type CommonAddRecordCtx struct {
	defCausIDs []int64
	event      []types.Causet
}

// commonAddRecordKey is used as key in `stochastikctx.Context.Value(key)`
type commonAddRecordKey struct{}

// String implement `stringer.String` for CommonAddRecordKey
func (c commonAddRecordKey) String() string {
	return "_common_add_record_context_key"
}

// addRecordCtxKey is key in `stochastikctx.Context` for CommonAddRecordCtx
var addRecordCtxKey = commonAddRecordKey{}

// SetAddRecordCtx set a CommonAddRecordCtx to stochastik context
func SetAddRecordCtx(ctx stochastikctx.Context, r *CommonAddRecordCtx) {
	ctx.SetValue(addRecordCtxKey, r)
}

// ClearAddRecordCtx remove `CommonAddRecordCtx` from stochastik context
func ClearAddRecordCtx(ctx stochastikctx.Context) {
	ctx.ClearValue(addRecordCtxKey)
}

// NewCommonAddRecordCtx create a context used for `AddRecord`
func NewCommonAddRecordCtx(size int) *CommonAddRecordCtx {
	return &CommonAddRecordCtx{
		defCausIDs: make([]int64, 0, size),
		event:      make([]types.Causet, 0, size),
	}
}

// TryGetCommonPkDeferredCausetIds get the IDs of primary key defCausumn if the block has common handle.
func TryGetCommonPkDeferredCausetIds(tbl *perceptron.BlockInfo) []int64 {
	var pkDefCausIds []int64
	if !tbl.IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(tbl)
	for _, idxDefCaus := range pkIdx.DeferredCausets {
		pkDefCausIds = append(pkDefCausIds, tbl.DeferredCausets[idxDefCaus.Offset].ID)
	}
	return pkDefCausIds
}

// TryGetCommonPkDeferredCausets get the primary key defCausumns if the block has common handle.
func TryGetCommonPkDeferredCausets(tbl block.Block) []*block.DeferredCauset {
	var pkDefCauss []*block.DeferredCauset
	if !tbl.Meta().IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(tbl.Meta())
	defcaus := tbl.DefCauss()
	for _, idxDefCaus := range pkIdx.DeferredCausets {
		pkDefCauss = append(pkDefCauss, defcaus[idxDefCaus.Offset])
	}
	return pkDefCauss
}

// AddRecord implements block.Block AddRecord interface.
func (t *BlockCommon) AddRecord(sctx stochastikctx.Context, r []types.Causet, opts ...block.AddRecordOption) (recordID ekv.Handle, err error) {
	var opt block.AddRecordOpt
	for _, fn := range opts {
		fn.ApplyOn(&opt)
	}
	var hasRecordID bool
	defcaus := t.DefCauss()
	// opt.IsUFIDelate is a flag for uFIDelate.
	// If handle ID is changed when uFIDelate, uFIDelate will remove the old record first, and then call `AddRecord` to add a new record.
	// Currently, only insert can set _milevadb_rowid, uFIDelate can not uFIDelate _milevadb_rowid.
	if len(r) > len(defcaus) && !opt.IsUFIDelate {
		// The last value is _milevadb_rowid.
		recordID = ekv.IntHandle(r[len(r)-1].GetInt64())
		hasRecordID = true
	} else {
		tblInfo := t.Meta()
		if tblInfo.PKIsHandle {
			recordID = ekv.IntHandle(r[tblInfo.GetPkDefCausInfo().Offset].GetInt64())
			hasRecordID = true
		} else if tblInfo.IsCommonHandle {
			pkIdx := FindPrimaryIndex(tblInfo)
			pkDts := make([]types.Causet, 0, len(pkIdx.DeferredCausets))
			for _, idxDefCaus := range pkIdx.DeferredCausets {
				pkDts = append(pkDts, r[tblInfo.DeferredCausets[idxDefCaus.Offset].Offset])
			}
			blockcodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
			var handleBytes []byte
			handleBytes, err = codec.EncodeKey(sctx.GetStochastikVars().StmtCtx, nil, pkDts...)
			if err != nil {
				return
			}
			recordID, err = ekv.NewCommonHandle(handleBytes)
			if err != nil {
				return
			}
			hasRecordID = true
		}
	}
	if !hasRecordID {
		if opt.ReserveAutoID > 0 {
			// Reserve a batch of auto ID in the statement context.
			// The reserved ID could be used in the future within this statement, by the
			// following AddRecord() operation.
			// Make the IDs continuous benefit for the performance of EinsteinDB.
			stmtCtx := sctx.GetStochastikVars().StmtCtx
			stmtCtx.BaseRowID, stmtCtx.MaxRowID, err = allocHandleIDs(sctx, t, uint64(opt.ReserveAutoID))
			if err != nil {
				return nil, err
			}
		}

		recordID, err = AllocHandle(sctx, t)
		if err != nil {
			return nil, err
		}
	}

	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, err
	}
	var defCausIDs, binlogDefCausIDs []int64
	var event, binlogRow []types.Causet
	if recordCtx, ok := sctx.Value(addRecordCtxKey).(*CommonAddRecordCtx); ok {
		defCausIDs = recordCtx.defCausIDs[:0]
		event = recordCtx.event[:0]
	} else {
		defCausIDs = make([]int64, 0, len(r))
		event = make([]types.Causet, 0, len(r))
	}
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	sessVars := sctx.GetStochastikVars()

	for _, defCaus := range t.WriblockDefCauss() {
		var value types.Causet
		if defCaus.ChangeStateInfo != nil && defCaus.State != perceptron.StatePublic {
			// TODO: Check overflow or ignoreTruncate.
			value, err = block.CastValue(sctx, r[defCaus.DependencyDeferredCausetOffset], defCaus.DeferredCausetInfo, false, false)
			if err != nil {
				return nil, err
			}
			if len(r) < len(t.WriblockDefCauss()) {
				r = append(r, value)
			} else {
				r[defCaus.Offset] = value
			}
			event = append(event, value)
			defCausIDs = append(defCausIDs, defCaus.ID)
			continue
		}
		if defCaus.State != perceptron.StatePublic &&
			// UFIDelate call `AddRecord` will already handle the write only defCausumn default value.
			// Only insert should add default value for write only defCausumn.
			!opt.IsUFIDelate {
			// If defCaus is in write only or write reorganization state, we must add it with its default value.
			value, err = block.GetDefCausOriginDefaultValue(sctx, defCaus.ToInfo())
			if err != nil {
				return nil, err
			}
			// add value to `r` for dirty EDB in transaction.
			// Otherwise when uFIDelate will panic cause by get value of defCausumn in write only state from dirty EDB.
			if defCaus.Offset < len(r) {
				r[defCaus.Offset] = value
			} else {
				r = append(r, value)
			}
		} else {
			value = r[defCaus.Offset]
		}
		if !t.canSkip(defCaus, &value) {
			defCausIDs = append(defCausIDs, defCaus.ID)
			event = append(event, value)
		}
	}

	writeBufs := sessVars.GetWriteStmtBufs()
	adjustRowValuesBuf(writeBufs, len(event))
	key := t.RecordKey(recordID)
	sc, rd := sessVars.StmtCtx, &sessVars.RowEncoder
	writeBufs.RowValBuf, err = blockcodec.EncodeRow(sc, event, defCausIDs, writeBufs.RowValBuf, writeBufs.AddRowValues, rd)
	if err != nil {
		return nil, err
	}
	value := writeBufs.RowValBuf

	var setPresume bool
	var ctx context.Context
	if opt.Ctx != nil {
		ctx = opt.Ctx
	} else {
		ctx = context.Background()
	}
	skipCheck := sctx.GetStochastikVars().StmtCtx.BatchCheck
	if (t.meta.IsCommonHandle || t.meta.PKIsHandle) && !skipCheck && !opt.SkipHandleCheck {
		if sctx.GetStochastikVars().LazyCheckKeyNotExists() {
			var v []byte
			v, err = txn.GetMemBuffer().Get(ctx, key)
			if err != nil {
				setPresume = true
			}
			if err == nil && len(v) == 0 {
				err = ekv.ErrNotExist
			}
		} else {
			_, err = txn.Get(ctx, key)
		}
		if err == nil {
			handleStr := ekv.GetDuplicateErrorHandleString(recordID)
			return recordID, ekv.ErrKeyExists.FastGenByArgs(handleStr, "PRIMARY")
		} else if !ekv.ErrNotExist.Equal(err) {
			return recordID, err
		}
	}

	if setPresume {
		err = memBuffer.SetWithFlags(key, value, ekv.SetPresumeKeyNotExists)
	} else {
		err = memBuffer.Set(key, value)
	}
	if err != nil {
		return nil, err
	}

	var createIdxOpts []block.CreateIdxOptFunc
	if len(opts) > 0 {
		createIdxOpts = make([]block.CreateIdxOptFunc, 0, len(opts))
		for _, fn := range opts {
			if raw, ok := fn.(block.CreateIdxOptFunc); ok {
				createIdxOpts = append(createIdxOpts, raw)
			}
		}
	}
	// Insert new entries into indices.
	h, err := t.addIndices(sctx, recordID, r, txn, createIdxOpts)
	if err != nil {
		return h, err
	}

	memBuffer.Release(sh)

	if shouldWriteBinlog(sctx) {
		// For insert, MilevaDB and Binlog can use same event and schemaReplicant.
		binlogRow = event
		binlogDefCausIDs = defCausIDs
		err = t.addInsertBinlog(sctx, recordID, binlogRow, binlogDefCausIDs)
		if err != nil {
			return nil, err
		}
	}
	sc.AddAffectedRows(1)
	if sessVars.TxnCtx == nil {
		return recordID, nil
	}
	defCausSize := make(map[int64]int64, len(r))
	for id, defCaus := range t.DefCauss() {
		size, err := codec.EstimateValueSize(sc, r[id])
		if err != nil {
			continue
		}
		defCausSize[defCaus.ID] = int64(size) - 1
	}
	sessVars.TxnCtx.UFIDelateDeltaForBlock(t.physicalBlockID, 1, 1, defCausSize)
	return recordID, nil
}

// genIndexKeyStr generates index content string representation.
func (t *BlockCommon) genIndexKeyStr(defCausVals []types.Causet) (string, error) {
	// Pass pre-composed error to txn.
	strVals := make([]string, 0, len(defCausVals))
	for _, cv := range defCausVals {
		cvs := "NULL"
		var err error
		if !cv.IsNull() {
			cvs, err = types.ToString(cv.GetValue())
			if err != nil {
				return "", err
			}
		}
		strVals = append(strVals, cvs)
	}
	return strings.Join(strVals, "-"), nil
}

// addIndices adds data into indices. If any key is duplicated, returns the original handle.
func (t *BlockCommon) addIndices(sctx stochastikctx.Context, recordID ekv.Handle, r []types.Causet, txn ekv.Transaction, opts []block.CreateIdxOptFunc) (ekv.Handle, error) {
	writeBufs := sctx.GetStochastikVars().GetWriteStmtBufs()
	indexVals := writeBufs.IndexValsBuf
	skipCheck := sctx.GetStochastikVars().StmtCtx.BatchCheck
	for _, v := range t.WriblockIndices() {
		if t.meta.IsCommonHandle && v.Meta().Primary {
			continue
		}
		indexVals, err := v.FetchValues(r, indexVals)
		if err != nil {
			return nil, err
		}
		var dupErr error
		if !skipCheck && v.Meta().Unique {
			entryKey, err := t.genIndexKeyStr(indexVals)
			if err != nil {
				return nil, err
			}
			idxMeta := v.Meta()
			dupErr = ekv.ErrKeyExists.FastGenByArgs(entryKey, idxMeta.Name.String())
		}
		if dupHandle, err := v.Create(sctx, txn.GetUnionStore(), indexVals, recordID, opts...); err != nil {
			if ekv.ErrKeyExists.Equal(err) {
				return dupHandle, dupErr
			}
			return nil, err
		}
	}
	// save the buffer, multi rows insert can use it.
	writeBufs.IndexValsBuf = indexVals
	return nil, nil
}

// RowWithDefCauss implements block.Block RowWithDefCauss interface.
func (t *BlockCommon) RowWithDefCauss(ctx stochastikctx.Context, h ekv.Handle, defcaus []*block.DeferredCauset) ([]types.Causet, error) {
	// Get raw event data from ekv.
	key := t.RecordKey(h)
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	value, err := txn.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	v, _, err := DecodeRawRowData(ctx, t.Meta(), h, defcaus, value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// DecodeRawRowData decodes raw event data into a causet slice and a (defCausumnID:defCausumnValue) map.
func DecodeRawRowData(ctx stochastikctx.Context, meta *perceptron.BlockInfo, h ekv.Handle, defcaus []*block.DeferredCauset,
	value []byte) ([]types.Causet, map[int64]types.Causet, error) {
	v := make([]types.Causet, len(defcaus))
	defCausTps := make(map[int64]*types.FieldType, len(defcaus))
	for i, defCaus := range defcaus {
		if defCaus == nil {
			continue
		}
		if defCaus.IsPKHandleDeferredCauset(meta) {
			if allegrosql.HasUnsignedFlag(defCaus.Flag) {
				v[i].SetUint64(uint64(h.IntValue()))
			} else {
				v[i].SetInt64(h.IntValue())
			}
			continue
		}
		if defCaus.IsCommonHandleDeferredCauset(meta) {
			pkIdx := FindPrimaryIndex(meta)
			var idxOfIdx int
			for i, idxDefCaus := range pkIdx.DeferredCausets {
				if meta.DeferredCausets[idxDefCaus.Offset].ID == defCaus.ID {
					idxOfIdx = i
					break
				}
			}
			dtBytes := h.EncodedDefCaus(idxOfIdx)
			_, dt, err := codec.DecodeOne(dtBytes)
			if err != nil {
				return nil, nil, err
			}
			dt, err = blockcodec.Unflatten(dt, &defCaus.FieldType, ctx.GetStochastikVars().Location())
			if err != nil {
				return nil, nil, err
			}
			v[i] = dt
			continue
		}
		defCausTps[defCaus.ID] = &defCaus.FieldType
	}
	rowMap, err := blockcodec.DecodeRowToCausetMap(value, defCausTps, ctx.GetStochastikVars().Location())
	if err != nil {
		return nil, rowMap, err
	}
	defaultVals := make([]types.Causet, len(defcaus))
	for i, defCaus := range defcaus {
		if defCaus == nil {
			continue
		}
		if defCaus.IsPKHandleDeferredCauset(meta) || defCaus.IsCommonHandleDeferredCauset(meta) {
			continue
		}
		ri, ok := rowMap[defCaus.ID]
		if ok {
			v[i] = ri
			continue
		}
		if defCaus.IsGenerated() && !defCaus.GeneratedStored {
			continue
		}
		if defCaus.ChangeStateInfo != nil {
			v[i], _, err = GetChangingDefCausVal(ctx, defcaus, defCaus, rowMap, defaultVals)
		} else {
			v[i], err = GetDefCausDefaultValue(ctx, defCaus, defaultVals)
		}
		if err != nil {
			return nil, rowMap, err
		}
	}
	return v, rowMap, nil
}

// GetChangingDefCausVal gets the changing defCausumn value when executing "modify/change defCausumn" statement.
func GetChangingDefCausVal(ctx stochastikctx.Context, defcaus []*block.DeferredCauset, defCaus *block.DeferredCauset, rowMap map[int64]types.Causet, defaultVals []types.Causet) (_ types.Causet, isDefaultVal bool, err error) {
	relativeDefCaus := defcaus[defCaus.ChangeStateInfo.DependencyDeferredCausetOffset]
	idxDeferredCausetVal, ok := rowMap[relativeDefCaus.ID]
	if ok {
		// It needs cast values here when filling back defCausumn or index values in "modify/change defCausumn" statement.
		if ctx.GetStochastikVars().StmtCtx.IsDBSJobInQueue {
			return idxDeferredCausetVal, false, nil
		}
		idxDeferredCausetVal, err := block.CastValue(ctx, rowMap[relativeDefCaus.ID], defCaus.DeferredCausetInfo, false, false)
		// TODO: Consider sql_mode and the error msg(encounter this error check whether to rollback).
		if err != nil {
			return idxDeferredCausetVal, false, errors.Trace(err)
		}
		return idxDeferredCausetVal, false, nil
	}

	idxDeferredCausetVal, err = GetDefCausDefaultValue(ctx, defCaus, defaultVals)
	if err != nil {
		return idxDeferredCausetVal, false, errors.Trace(err)
	}

	return idxDeferredCausetVal, true, nil
}

// Row implements block.Block Row interface.
func (t *BlockCommon) Row(ctx stochastikctx.Context, h ekv.Handle) ([]types.Causet, error) {
	return t.RowWithDefCauss(ctx, h, t.DefCauss())
}

// RemoveRecord implements block.Block RemoveRecord interface.
func (t *BlockCommon) RemoveRecord(ctx stochastikctx.Context, h ekv.Handle, r []types.Causet) error {
	err := t.removeRowData(ctx, h)
	if err != nil {
		return err
	}
	// The block has non-public defCausumn and this defCausumn is doing the operation of "modify/change defCausumn".
	if len(t.DeferredCausets) > len(r) && t.DeferredCausets[len(r)].ChangeStateInfo != nil {
		r = append(r, r[t.DeferredCausets[len(r)].ChangeStateInfo.DependencyDeferredCausetOffset])
	}
	err = t.removeRowIndices(ctx, h, r)
	if err != nil {
		return err
	}

	if shouldWriteBinlog(ctx) {
		defcaus := t.DefCauss()
		defCausIDs := make([]int64, 0, len(defcaus)+1)
		for _, defCaus := range defcaus {
			defCausIDs = append(defCausIDs, defCaus.ID)
		}
		var binlogRow []types.Causet
		if !t.meta.PKIsHandle {
			defCausIDs = append(defCausIDs, perceptron.ExtraHandleID)
			binlogRow = make([]types.Causet, 0, len(r)+1)
			binlogRow = append(binlogRow, r...)
			handleData, err := h.Data()
			if err != nil {
				return err
			}
			binlogRow = append(binlogRow, handleData...)
		} else {
			binlogRow = r
		}
		err = t.addDeleteBinlog(ctx, binlogRow, defCausIDs)
	}
	if ctx.GetStochastikVars().TxnCtx == nil {
		return nil
	}
	defCausSize := make(map[int64]int64, len(t.DefCauss()))
	sc := ctx.GetStochastikVars().StmtCtx
	for id, defCaus := range t.DefCauss() {
		size, err := codec.EstimateValueSize(sc, r[id])
		if err != nil {
			continue
		}
		defCausSize[defCaus.ID] = -int64(size - 1)
	}
	ctx.GetStochastikVars().TxnCtx.UFIDelateDeltaForBlock(t.physicalBlockID, -1, 1, defCausSize)
	return err
}

func (t *BlockCommon) addInsertBinlog(ctx stochastikctx.Context, h ekv.Handle, event []types.Causet, defCausIDs []int64) error {
	mutation := t.getMutation(ctx)
	handleData, err := h.Data()
	if err != nil {
		return err
	}
	pk, err := codec.EncodeValue(ctx.GetStochastikVars().StmtCtx, nil, handleData...)
	if err != nil {
		return err
	}
	value, err := blockcodec.EncodeOldRow(ctx.GetStochastikVars().StmtCtx, event, defCausIDs, nil, nil)
	if err != nil {
		return err
	}
	bin := append(pk, value...)
	mutation.InsertedRows = append(mutation.InsertedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_Insert)
	return nil
}

func (t *BlockCommon) addUFIDelateBinlog(ctx stochastikctx.Context, oldRow, newRow []types.Causet, defCausIDs []int64) error {
	old, err := blockcodec.EncodeOldRow(ctx.GetStochastikVars().StmtCtx, oldRow, defCausIDs, nil, nil)
	if err != nil {
		return err
	}
	newVal, err := blockcodec.EncodeOldRow(ctx.GetStochastikVars().StmtCtx, newRow, defCausIDs, nil, nil)
	if err != nil {
		return err
	}
	bin := append(old, newVal...)
	mutation := t.getMutation(ctx)
	mutation.UFIDelatedRows = append(mutation.UFIDelatedRows, bin)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_UFIDelate)
	return nil
}

func (t *BlockCommon) addDeleteBinlog(ctx stochastikctx.Context, r []types.Causet, defCausIDs []int64) error {
	data, err := blockcodec.EncodeOldRow(ctx.GetStochastikVars().StmtCtx, r, defCausIDs, nil, nil)
	if err != nil {
		return err
	}
	mutation := t.getMutation(ctx)
	mutation.DeletedRows = append(mutation.DeletedRows, data)
	mutation.Sequence = append(mutation.Sequence, binlog.MutationType_DeleteRow)
	return nil
}

func writeSequenceUFIDelateValueBinlog(ctx stochastikctx.Context, EDB, sequence string, end int64) error {
	// 1: when sequenceCommon uFIDelate the local cache passively.
	// 2: When sequenceCommon setval to the allocator actively.
	// Both of this two case means the upper bound the sequence has changed in meta, which need to write the binlog
	// to the downstream.
	// Sequence sends `select setval(seq, num)` allegrosql string to downstream via `setDBSBinlog`, which is mocked as a DBS binlog.
	binlogCli := ctx.GetStochastikVars().BinlogClient
	sqlMode := ctx.GetStochastikVars().ALLEGROSQLMode
	sequenceFullName := stringutil.Escape(EDB, sqlMode) + "." + stringutil.Escape(sequence, sqlMode)
	allegrosql := "select setval(" + sequenceFullName + ", " + strconv.FormatInt(end, 10) + ")"

	err := ekv.RunInNewTxn(ctx.GetStore(), true, func(txn ekv.Transaction) error {
		m := meta.NewMeta(txn)
		mockJobID, err := m.GenGlobalID()
		if err != nil {
			return err
		}
		binloginfo.SetDBSBinlog(binlogCli, txn, mockJobID, int32(perceptron.StatePublic), allegrosql)
		return nil
	})
	return err
}

func (t *BlockCommon) removeRowData(ctx stochastikctx.Context, h ekv.Handle) error {
	// Remove event data.
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	key := t.RecordKey(h)
	err = txn.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

// removeRowIndices removes all the indices of a event.
func (t *BlockCommon) removeRowIndices(ctx stochastikctx.Context, h ekv.Handle, rec []types.Causet) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	for _, v := range t.DeleblockIndices() {
		vals, err := v.FetchValues(rec, nil)
		if err != nil {
			logutil.BgLogger().Info("remove event index failed", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.String("handle", h.String()), zap.Any("record", rec), zap.Error(err))
			return err
		}
		if err = v.Delete(ctx.GetStochastikVars().StmtCtx, txn, vals, h); err != nil {
			if v.Meta().State != perceptron.StatePublic && ekv.ErrNotExist.Equal(err) {
				// If the index is not in public state, we may have not created the index,
				// or already deleted the index, so skip ErrNotExist error.
				logutil.BgLogger().Debug("event index not exists", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.String("handle", h.String()))
				continue
			}
			return err
		}
	}
	return nil
}

// removeRowIndex implements block.Block RemoveRowIndex interface.
func (t *BlockCommon) removeRowIndex(sc *stmtctx.StatementContext, h ekv.Handle, vals []types.Causet, idx block.Index, txn ekv.Transaction) error {
	return idx.Delete(sc, txn, vals, h)
}

// buildIndexForRow implements block.Block BuildIndexForRow interface.
func (t *BlockCommon) buildIndexForRow(ctx stochastikctx.Context, h ekv.Handle, vals []types.Causet, idx block.Index, txn ekv.Transaction, untouched bool, popts ...block.CreateIdxOptFunc) error {
	var opts []block.CreateIdxOptFunc
	opts = append(opts, popts...)
	if untouched {
		opts = append(opts, block.IndexIsUntouched)
	}
	if _, err := idx.Create(ctx, txn.GetUnionStore(), vals, h, opts...); err != nil {
		if ekv.ErrKeyExists.Equal(err) {
			// Make error message consistent with MyALLEGROSQL.
			entryKey, err1 := t.genIndexKeyStr(vals)
			if err1 != nil {
				// if genIndexKeyStr failed, return the original error.
				return err
			}

			return ekv.ErrKeyExists.FastGenByArgs(entryKey, idx.Meta().Name)
		}
		return err
	}
	return nil
}

// IterRecords implements block.Block IterRecords interface.
func (t *BlockCommon) IterRecords(ctx stochastikctx.Context, startKey ekv.Key, defcaus []*block.DeferredCauset,
	fn block.RecordIterFunc) error {
	prefix := t.RecordPrefix()
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	it, err := txn.Iter(startKey, prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	logutil.BgLogger().Debug("iterate records", zap.ByteString("startKey", startKey), zap.ByteString("key", it.Key()), zap.ByteString("value", it.Value()))

	defCausMap := make(map[int64]*types.FieldType, len(defcaus))
	for _, defCaus := range defcaus {
		defCausMap[defCaus.ID] = &defCaus.FieldType
	}
	defaultVals := make([]types.Causet, len(defcaus))
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first ekv pair is event dagger information.
		// TODO: check valid dagger
		// get event handle
		handle, err := blockcodec.DecodeRowKey(it.Key())
		if err != nil {
			return err
		}
		rowMap, err := blockcodec.DecodeRowToCausetMap(it.Value(), defCausMap, ctx.GetStochastikVars().Location())
		if err != nil {
			return err
		}
		pkIds, decodeLoc := TryGetCommonPkDeferredCausetIds(t.meta), ctx.GetStochastikVars().Location()
		data := make([]types.Causet, len(defcaus))
		for _, defCaus := range defcaus {
			if defCaus.IsPKHandleDeferredCauset(t.meta) {
				if allegrosql.HasUnsignedFlag(defCaus.Flag) {
					data[defCaus.Offset].SetUint64(uint64(handle.IntValue()))
				} else {
					data[defCaus.Offset].SetInt64(handle.IntValue())
				}
				continue
			} else if allegrosql.HasPriKeyFlag(defCaus.Flag) {
				data[defCaus.Offset], err = tryDecodeDeferredCausetFromCommonHandle(defCaus, handle, pkIds, decodeLoc)
				if err != nil {
					return err
				}
				continue
			}
			if _, ok := rowMap[defCaus.ID]; ok {
				data[defCaus.Offset] = rowMap[defCaus.ID]
				continue
			}
			data[defCaus.Offset], err = GetDefCausDefaultValue(ctx, defCaus, defaultVals)
			if err != nil {
				return err
			}
		}
		more, err := fn(handle, data, defcaus)
		if !more || err != nil {
			return err
		}

		rk := t.RecordKey(handle)
		err = ekv.NextUntil(it, soliton.RowKeyPrefixFilter(rk))
		if err != nil {
			return err
		}
	}

	return nil
}

func tryDecodeDeferredCausetFromCommonHandle(defCaus *block.DeferredCauset, handle ekv.Handle, pkIds []int64, decodeLoc *time.Location) (types.Causet, error) {
	for i, hid := range pkIds {
		if hid != defCaus.ID {
			continue
		}
		_, d, err := codec.DecodeOne(handle.EncodedDefCaus(i))
		if err != nil {
			return types.Causet{}, errors.Trace(err)
		}
		if d, err = blockcodec.Unflatten(d, &defCaus.FieldType, decodeLoc); err != nil {
			return types.Causet{}, err
		}
		return d, nil
	}
	return types.Causet{}, nil
}

// GetDefCausDefaultValue gets a defCausumn default value.
// The defaultVals is used to avoid calculating the default value multiple times.
func GetDefCausDefaultValue(ctx stochastikctx.Context, defCaus *block.DeferredCauset, defaultVals []types.Causet) (
	defCausVal types.Causet, err error) {
	if defCaus.OriginDefaultValue == nil && allegrosql.HasNotNullFlag(defCaus.Flag) {
		return defCausVal, errors.New("Miss defCausumn")
	}
	if defCaus.State != perceptron.StatePublic {
		return defCausVal, nil
	}
	if defaultVals[defCaus.Offset].IsNull() {
		defCausVal, err = block.GetDefCausOriginDefaultValue(ctx, defCaus.ToInfo())
		if err != nil {
			return defCausVal, err
		}
		defaultVals[defCaus.Offset] = defCausVal
	} else {
		defCausVal = defaultVals[defCaus.Offset]
	}

	return defCausVal, nil
}

// AllocHandle allocate a new handle.
// A statement could reserve some ID in the statement context, try those ones first.
func AllocHandle(ctx stochastikctx.Context, t block.Block) (ekv.Handle, error) {
	if ctx != nil {
		if stmtCtx := ctx.GetStochastikVars().StmtCtx; stmtCtx != nil {
			// First try to alloc if the statement has reserved auto ID.
			if stmtCtx.BaseRowID < stmtCtx.MaxRowID {
				stmtCtx.BaseRowID += 1
				return ekv.IntHandle(stmtCtx.BaseRowID), nil
			}
		}
	}

	_, rowID, err := allocHandleIDs(ctx, t, 1)
	return ekv.IntHandle(rowID), err
}

func allocHandleIDs(ctx stochastikctx.Context, t block.Block, n uint64) (int64, int64, error) {
	meta := t.Meta()
	base, maxID, err := t.SlabPredictors(ctx).Get(autoid.RowIDAllocType).Alloc(meta.ID, n, 1, 1)
	if err != nil {
		return 0, 0, err
	}
	if meta.ShardRowIDBits > 0 {
		// Use max record ShardRowIDBits to check overflow.
		if OverflowShardBits(maxID, meta.MaxShardRowIDBits, autoid.RowIDBitLength, true) {
			// If overflow, the rowID may be duplicated. For examples,
			// t.meta.ShardRowIDBits = 4
			// rowID = 0010111111111111111111111111111111111111111111111111111111111111
			// shard = 0100000000000000000000000000000000000000000000000000000000000000
			// will be duplicated with:
			// rowID = 0100111111111111111111111111111111111111111111111111111111111111
			// shard = 0010000000000000000000000000000000000000000000000000000000000000
			return 0, 0, autoid.ErrAutoincReadFailed
		}
		txnCtx := ctx.GetStochastikVars().TxnCtx
		shard := txnCtx.GetShard(meta.ShardRowIDBits, autoid.RowIDBitLength, true, int(n))
		base |= shard
		maxID |= shard
	}
	return base, maxID, nil
}

// OverflowShardBits checks whether the recordID overflow `1<<(typeBitsLength-shardRowIDBits-1) -1`.
func OverflowShardBits(recordID int64, shardRowIDBits uint64, typeBitsLength uint64, reservedSignBit bool) bool {
	var signBit uint64
	if reservedSignBit {
		signBit = 1
	}
	mask := (1<<shardRowIDBits - 1) << (typeBitsLength - shardRowIDBits - signBit)
	return recordID&int64(mask) > 0
}

// SlabPredictors implements block.Block SlabPredictors interface.
func (t *BlockCommon) SlabPredictors(ctx stochastikctx.Context) autoid.SlabPredictors {
	if ctx == nil || ctx.GetStochastikVars().IDSlabPredictor == nil {
		return t.allocs
	}

	// Replace the event id allocator with the one in stochastik variables.
	sessAlloc := ctx.GetStochastikVars().IDSlabPredictor
	retAllocs := make([]autoid.SlabPredictor, 0, len(t.allocs))
	INTERLOCKy(retAllocs, t.allocs)

	overwritten := false
	for i, a := range retAllocs {
		if a.GetType() == autoid.RowIDAllocType {
			retAllocs[i] = sessAlloc
			overwritten = true
			break
		}
	}
	if !overwritten {
		retAllocs = append(retAllocs, sessAlloc)
	}
	return retAllocs
}

// RebaseAutoID implements block.Block RebaseAutoID interface.
// Both auto-increment and auto-random can use this function to do rebase on explicit newBase value (without shadow bits).
func (t *BlockCommon) RebaseAutoID(ctx stochastikctx.Context, newBase int64, isSetStep bool, tp autoid.SlabPredictorType) error {
	return t.SlabPredictors(ctx).Get(tp).Rebase(t.blockID, newBase, isSetStep)
}

// Seek implements block.Block Seek interface.
func (t *BlockCommon) Seek(ctx stochastikctx.Context, h ekv.Handle) (ekv.Handle, bool, error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, false, err
	}
	seekKey := blockcodec.EncodeRowKeyWithHandle(t.physicalBlockID, h)
	iter, err := txn.Iter(seekKey, t.RecordPrefix().PrefixNext())
	if err != nil {
		return nil, false, err
	}
	if !iter.Valid() || !iter.Key().HasPrefix(t.RecordPrefix()) {
		// No more records in the block, skip to the end.
		return nil, false, nil
	}
	handle, err := blockcodec.DecodeRowKey(iter.Key())
	if err != nil {
		return nil, false, err
	}
	return handle, true, nil
}

// Type implements block.Block Type interface.
func (t *BlockCommon) Type() block.Type {
	return block.NormalBlock
}

func shouldWriteBinlog(ctx stochastikctx.Context) bool {
	if ctx.GetStochastikVars().BinlogClient == nil {
		return false
	}
	return !ctx.GetStochastikVars().InRestrictedALLEGROSQL
}

func (t *BlockCommon) getMutation(ctx stochastikctx.Context) *binlog.BlockMutation {
	return ctx.StmtGetMutation(t.blockID)
}

func (t *BlockCommon) canSkip(defCaus *block.DeferredCauset, value *types.Causet) bool {
	return CanSkip(t.Meta(), defCaus, value)
}

// CanSkip is for these cases, we can skip the defCausumns in encoded event:
// 1. the defCausumn is included in primary key;
// 2. the defCausumn's default value is null, and the value equals to that;
// 3. the defCausumn is virtual generated.
func CanSkip(info *perceptron.BlockInfo, defCaus *block.DeferredCauset, value *types.Causet) bool {
	if defCaus.IsPKHandleDeferredCauset(info) {
		return true
	}
	if defCaus.IsCommonHandleDeferredCauset(info) {
		pkIdx := FindPrimaryIndex(info)
		for _, idxDefCaus := range pkIdx.DeferredCausets {
			if info.DeferredCausets[idxDefCaus.Offset].ID != defCaus.ID {
				continue
			}
			canSkip := idxDefCaus.Length == types.UnspecifiedLength
			isNewDefCauslation := defCauslate.NewDefCauslationEnabled() &&
				defCaus.EvalType() == types.ETString &&
				!allegrosql.HasBinaryFlag(defCaus.Flag)
			canSkip = canSkip && !isNewDefCauslation
			return canSkip
		}
	}
	if defCaus.GetDefaultValue() == nil && value.IsNull() {
		return true
	}
	if defCaus.IsGenerated() && !defCaus.GeneratedStored {
		return true
	}
	return false
}

// canSkipUFIDelateBinlog checks whether the defCausumn can be skipped or not.
func (t *BlockCommon) canSkipUFIDelateBinlog(defCaus *block.DeferredCauset, value types.Causet) bool {
	if defCaus.IsGenerated() && !defCaus.GeneratedStored {
		return true
	}
	return false
}

// FindIndexByDefCausName returns a public block index containing only one defCausumn named `name`.
func FindIndexByDefCausName(t block.Block, name string) block.Index {
	for _, idx := range t.Indices() {
		// only public index can be read.
		if idx.Meta().State != perceptron.StatePublic {
			continue
		}

		if len(idx.Meta().DeferredCausets) == 1 && strings.EqualFold(idx.Meta().DeferredCausets[0].Name.L, name) {
			return idx
		}
	}
	return nil
}

// CheckHandleExists check whether recordID key exists. if not exists, return nil,
// otherwise return ekv.ErrKeyExists error.
func CheckHandleExists(ctx context.Context, sctx stochastikctx.Context, t block.Block, recordID ekv.Handle, data []types.Causet) error {
	if pt, ok := t.(*partitionedBlock); ok {
		info := t.Meta().GetPartitionInfo()
		pid, err := pt.locatePartition(sctx, info, data)
		if err != nil {
			return err
		}
		t = pt.GetPartition(pid)
	}
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}
	// Check key exists.
	recordKey := t.RecordKey(recordID)
	_, err = txn.Get(ctx, recordKey)
	if err == nil {
		handleStr := ekv.GetDuplicateErrorHandleString(recordID)
		return ekv.ErrKeyExists.FastGenByArgs(handleStr, "PRIMARY")
	} else if !ekv.ErrNotExist.Equal(err) {
		return err
	}
	return nil
}

func init() {
	block.BlockFromMeta = BlockFromMeta
	block.MockBlockFromMeta = MockBlockFromMeta
}

// sequenceCommon cache the sequence value.
// `alter sequence` will invalidate the cached range.
// `setval` will recompute the start position of cached value.
type sequenceCommon struct {
	meta *perceptron.SequenceInfo
	// base < end when increment > 0.
	// base > end when increment < 0.
	end  int64
	base int64
	// round is used to count the cycle times.
	round int64
	mu    sync.RWMutex
}

// GetSequenceBaseEndRound is used in test.
func (s *sequenceCommon) GetSequenceBaseEndRound() (int64, int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.base, s.end, s.round
}

// GetSequenceNextVal implements soliton.SequenceBlock GetSequenceNextVal interface.
// Caching the sequence value in block, we can easily be notified with the cache empty,
// and write the binlogInfo in block level rather than in allocator.
func (t *BlockCommon) GetSequenceNextVal(ctx interface{}, dbName, seqName string) (nextVal int64, err error) {
	seq := t.sequence
	if seq == nil {
		// TODO: refine the error.
		return 0, errors.New("sequenceCommon is nil")
	}
	seq.mu.Lock()
	defer seq.mu.Unlock()

	err = func() error {
		// Check if need to uFIDelate the cache batch from storage.
		// Because seq.base is not always the last allocated value (may be set by setval()).
		// So we should try to seek the next value in cache (not just add increment to seq.base).
		var (
			uFIDelateCache bool
			offset         int64
			ok             bool
		)
		if seq.base == seq.end {
			// There is no cache yet.
			uFIDelateCache = true
		} else {
			// Seek the first valid value in cache.
			offset = seq.getOffset()
			if seq.meta.Increment > 0 {
				nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.base, seq.end)
			} else {
				nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.end, seq.base)
			}
			if !ok {
				uFIDelateCache = true
			}
		}
		if !uFIDelateCache {
			return nil
		}
		// UFIDelate batch alloc from ekv storage.
		sequenceAlloc, err1 := getSequenceSlabPredictor(t.allocs)
		if err1 != nil {
			return err1
		}
		var base, end, round int64
		base, end, round, err1 = sequenceAlloc.AllocSeqCache(t.blockID)
		if err1 != nil {
			return err1
		}
		// Only uFIDelate local cache when alloc succeed.
		seq.base = base
		seq.end = end
		seq.round = round
		// write sequence binlog to the pumpClient.
		if ctx.(stochastikctx.Context).GetStochastikVars().BinlogClient != nil {
			err = writeSequenceUFIDelateValueBinlog(ctx.(stochastikctx.Context), dbName, seqName, seq.end)
			if err != nil {
				return err
			}
		}
		// Seek the first valid value in new cache.
		// Offset may have changed cause the round is uFIDelated.
		offset = seq.getOffset()
		if seq.meta.Increment > 0 {
			nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.base, seq.end)
		} else {
			nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.end, seq.base)
		}
		if !ok {
			return errors.New("can't find the first value in sequence cache")
		}
		return nil
	}()
	// Sequence alloc in ekv causetstore error.
	if err != nil {
		if err == autoid.ErrAutoincReadFailed {
			return 0, block.ErrSequenceHasRunOut.GenWithStackByArgs(dbName, seqName)
		}
		return 0, err
	}
	seq.base = nextVal
	return nextVal, nil
}

// SetSequenceVal implements soliton.SequenceBlock SetSequenceVal interface.
// The returned bool indicates the newVal is already under the base.
func (t *BlockCommon) SetSequenceVal(ctx interface{}, newVal int64, dbName, seqName string) (int64, bool, error) {
	seq := t.sequence
	if seq == nil {
		// TODO: refine the error.
		return 0, false, errors.New("sequenceCommon is nil")
	}
	seq.mu.Lock()
	defer seq.mu.Unlock()

	if seq.meta.Increment > 0 {
		if newVal <= t.sequence.base {
			return 0, true, nil
		}
		if newVal <= t.sequence.end {
			t.sequence.base = newVal
			return newVal, false, nil
		}
	} else {
		if newVal >= t.sequence.base {
			return 0, true, nil
		}
		if newVal >= t.sequence.end {
			t.sequence.base = newVal
			return newVal, false, nil
		}
	}

	// Invalid the current cache.
	t.sequence.base = t.sequence.end

	// Rebase from ekv storage.
	sequenceAlloc, err := getSequenceSlabPredictor(t.allocs)
	if err != nil {
		return 0, false, err
	}
	res, alreadySatisfied, err := sequenceAlloc.RebaseSeq(t.blockID, newVal)
	if err != nil {
		return 0, false, err
	}
	if !alreadySatisfied {
		// Write sequence binlog to the pumpClient.
		if ctx.(stochastikctx.Context).GetStochastikVars().BinlogClient != nil {
			err = writeSequenceUFIDelateValueBinlog(ctx.(stochastikctx.Context), dbName, seqName, seq.end)
			if err != nil {
				return 0, false, err
			}
		}
	}
	// Record the current end after setval succeed.
	// Consider the following case.
	// create sequence seq
	// setval(seq, 100) setval(seq, 50)
	// Because no cache (base, end keep 0), so the second setval won't return NULL.
	t.sequence.base, t.sequence.end = newVal, newVal
	return res, alreadySatisfied, nil
}

// getOffset is used in under GetSequenceNextVal & SetSequenceVal, which mu is locked.
func (s *sequenceCommon) getOffset() int64 {
	offset := s.meta.Start
	if s.meta.Cycle && s.round > 0 {
		if s.meta.Increment > 0 {
			offset = s.meta.MinValue
		} else {
			offset = s.meta.MaxValue
		}
	}
	return offset
}

// GetSequenceID implements soliton.SequenceBlock GetSequenceID interface.
func (t *BlockCommon) GetSequenceID() int64 {
	return t.blockID
}

// GetSequenceCommon is used in test to get sequenceCommon.
func (t *BlockCommon) GetSequenceCommon() *sequenceCommon {
	return t.sequence
}

func getSequenceSlabPredictor(allocs autoid.SlabPredictors) (autoid.SlabPredictor, error) {
	for _, alloc := range allocs {
		if alloc.GetType() == autoid.SequenceType {
			return alloc, nil
		}
	}
	// TODO: refine the error.
	return nil, errors.New("sequence allocator is nil")
}

// BuildBlockScanFromInfos build fidelpb.BlockScan with *perceptron.BlockInfo and *perceptron.DeferredCausetInfo.
func BuildBlockScanFromInfos(blockInfo *perceptron.BlockInfo, defCausumnInfos []*perceptron.DeferredCausetInfo) *fidelpb.BlockScan {
	pkDefCausIds := TryGetCommonPkDeferredCausetIds(blockInfo)
	tsExec := &fidelpb.BlockScan{
		BlockId:                  blockInfo.ID,
		DeferredCausets:          soliton.DeferredCausetsToProto(defCausumnInfos, blockInfo.PKIsHandle),
		PrimaryDeferredCausetIds: pkDefCausIds,
	}
	return tsExec
}
