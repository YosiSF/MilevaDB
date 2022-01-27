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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/helper"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/FIDelapi"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/stmtsummary"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	binaryJson "github.com/whtcorpsinc/milevadb/types/json"
	"go.etcd.io/etcd/clientv3"
)

type memblockRetriever struct {
	dummyCloser
	block       *perceptron.BlockInfo
	defCausumns     []*perceptron.DeferredCausetInfo
	rows        [][]types.Causet
	rowIdx      int
	retrieved   bool
	initialized bool
}

// retrieve implements the schemareplicantRetriever interface
func (e *memblockRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.retrieved {
		return nil, nil
	}

	//Cache the ret full rows in schemataRetriever
	if !e.initialized {
		is := schemareplicant.GetSchemaReplicant(sctx)
		dbs := is.AllSchemas()
		sort.Sort(schemareplicant.SchemasSorter(dbs))
		var err error
		switch e.block.Name.O {
		case schemareplicant.BlockSchemata:
			e.setDataFromSchemata(sctx, dbs)
		case schemareplicant.BlockStatistics:
			e.setDataForStatistics(sctx, dbs)
		case schemareplicant.BlockBlocks:
			err = e.setDataFromBlocks(sctx, dbs)
		case schemareplicant.BlockSequences:
			e.setDataFromSequences(sctx, dbs)
		case schemareplicant.BlockPartitions:
			err = e.setDataFromPartitions(sctx, dbs)
		case schemareplicant.BlockClusterInfo:
			err = e.dataForMilevaDBClusterInfo(sctx)
		case schemareplicant.BlockAnalyzeStatus:
			e.setDataForAnalyzeStatus(sctx)
		case schemareplicant.BlockMilevaDBIndexes:
			e.setDataFromIndexes(sctx, dbs)
		case schemareplicant.BlockViews:
			e.setDataFromViews(sctx, dbs)
		case schemareplicant.BlockEngines:
			e.setDataFromEngines()
		case schemareplicant.BlockCharacterSets:
			e.setDataFromCharacterSets()
		case schemareplicant.BlockDefCauslations:
			e.setDataFromDefCauslations()
		case schemareplicant.BlockKeyDeferredCauset:
			e.setDataFromKeyDeferredCausetUsage(sctx, dbs)
		case schemareplicant.BlockMetricBlocks:
			e.setDataForMetricBlocks(sctx)
		case schemareplicant.BlockProfiling:
			e.setDataForPseudoProfiling(sctx)
		case schemareplicant.BlockDefCauslationCharacterSetApplicability:
			e.dataForDefCauslationCharacterSetApplicability()
		case schemareplicant.BlockProcesslist:
			e.setDataForProcessList(sctx)
		case schemareplicant.ClusterBlockProcesslist:
			err = e.setDataForClusterProcessList(sctx)
		case schemareplicant.BlockUserPrivileges:
			e.setDataFromUserPrivileges(sctx)
		case schemareplicant.BlockEinsteinDBRegionStatus:
			err = e.setDataForEinsteinDBRegionStatus(sctx)
		case schemareplicant.BlockEinsteinDBRegionPeers:
			err = e.setDataForEinsteinDBRegionPeers(sctx)
		case schemareplicant.BlockMilevaDBHotRegions:
			err = e.setDataForMilevaDBHotRegions(sctx)
		case schemareplicant.BlockConstraints:
			e.setDataFromBlockConstraints(sctx, dbs)
		case schemareplicant.BlockStochastikVar:
			err = e.setDataFromStochastikVar(sctx)
		case schemareplicant.BlockMilevaDBServersInfo:
			err = e.setDataForServersInfo()
		case schemareplicant.BlockTiFlashReplica:
			e.dataForBlockTiFlashReplica(sctx, dbs)
		case schemareplicant.BlockEinsteinDBStoreStatus:
			err = e.dataForEinsteinDBStoreStatus(sctx)
		case schemareplicant.BlockStatementsSummary,
			schemareplicant.BlockStatementsSummaryHistory,
			schemareplicant.ClusterBlockStatementsSummary,
			schemareplicant.ClusterBlockStatementsSummaryHistory:
			err = e.setDataForStatementsSummary(sctx, e.block.Name.O)
		}
		if err != nil {
			return nil, err
		}
		e.initialized = true
	}

	//Adjust the amount of each return
	maxCount := 1024
	retCount := maxCount
	if e.rowIdx+maxCount > len(e.rows) {
		retCount = len(e.rows) - e.rowIdx
		e.retrieved = true
	}
	ret := make([][]types.Causet, retCount)
	for i := e.rowIdx; i < e.rowIdx+retCount; i++ {
		ret[i-e.rowIdx] = e.rows[i]
	}
	e.rowIdx += retCount
	return adjustDeferredCausets(ret, e.defCausumns, e.block), nil
}

func getEventCountAllBlock(ctx stochastikctx.Context) (map[int64]uint64, error) {
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL("select block_id, count from allegrosql.stats_meta")
	if err != nil {
		return nil, err
	}
	rowCountMap := make(map[int64]uint64, len(rows))
	for _, event := range rows {
		blockID := event.GetInt64(0)
		rowCnt := event.GetUint64(1)
		rowCountMap[blockID] = rowCnt
	}
	return rowCountMap, nil
}

type blockHistID struct {
	blockID int64
	histID  int64
}

func getDefCausLengthAllBlocks(ctx stochastikctx.Context) (map[blockHistID]uint64, error) {
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL("select block_id, hist_id, tot_defCaus_size from allegrosql.stats_histograms where is_index = 0")
	if err != nil {
		return nil, err
	}
	defCausLengthMap := make(map[blockHistID]uint64, len(rows))
	for _, event := range rows {
		blockID := event.GetInt64(0)
		histID := event.GetInt64(1)
		totalSize := event.GetInt64(2)
		if totalSize < 0 {
			totalSize = 0
		}
		defCausLengthMap[blockHistID{blockID: blockID, histID: histID}] = uint64(totalSize)
	}
	return defCausLengthMap, nil
}

func getDataAndIndexLength(info *perceptron.BlockInfo, physicalID int64, rowCount uint64, defCausumnLengthMap map[blockHistID]uint64) (uint64, uint64) {
	defCausumnLength := make(map[string]uint64, len(info.DeferredCausets))
	for _, defCaus := range info.DeferredCausets {
		if defCaus.State != perceptron.StatePublic {
			continue
		}
		length := defCaus.FieldType.StorageLength()
		if length != types.VarStorageLen {
			defCausumnLength[defCaus.Name.L] = rowCount * uint64(length)
		} else {
			length := defCausumnLengthMap[blockHistID{blockID: physicalID, histID: defCaus.ID}]
			defCausumnLength[defCaus.Name.L] = length
		}
	}
	dataLength, indexLength := uint64(0), uint64(0)
	for _, length := range defCausumnLength {
		dataLength += length
	}
	for _, idx := range info.Indices {
		if idx.State != perceptron.StatePublic {
			continue
		}
		for _, defCaus := range idx.DeferredCausets {
			if defCaus.Length == types.UnspecifiedLength {
				indexLength += defCausumnLength[defCaus.Name.L]
			} else {
				indexLength += rowCount * uint64(defCaus.Length)
			}
		}
	}
	return dataLength, indexLength
}

type statsCache struct {
	mu         sync.RWMutex
	modifyTime time.Time
	blockEvents  map[int64]uint64
	defCausLength  map[blockHistID]uint64
}

var blockStatsCache = &statsCache{}

// BlockStatsCacheExpiry is the expiry time for block stats cache.
var BlockStatsCacheExpiry = 3 * time.Second

func (c *statsCache) get(ctx stochastikctx.Context) (map[int64]uint64, map[blockHistID]uint64, error) {
	c.mu.RLock()
	if time.Since(c.modifyTime) < BlockStatsCacheExpiry {
		blockEvents, defCausLength := c.blockEvents, c.defCausLength
		c.mu.RUnlock()
		return blockEvents, defCausLength, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.modifyTime) < BlockStatsCacheExpiry {
		return c.blockEvents, c.defCausLength, nil
	}
	blockEvents, err := getEventCountAllBlock(ctx)
	if err != nil {
		return nil, nil, err
	}
	defCausLength, err := getDefCausLengthAllBlocks(ctx)
	if err != nil {
		return nil, nil, err
	}

	c.blockEvents = blockEvents
	c.defCausLength = defCausLength
	c.modifyTime = time.Now()
	return blockEvents, defCausLength, nil
}

func getAutoIncrementID(ctx stochastikctx.Context, schemaReplicant *perceptron.DBInfo, tblInfo *perceptron.BlockInfo) (int64, error) {
	is := schemareplicant.GetSchemaReplicant(ctx)
	tbl, err := is.BlockByName(schemaReplicant.Name, tblInfo.Name)
	if err != nil {
		return 0, err
	}
	return tbl.SlabPredictors(ctx).Get(autoid.EventIDAllocType).Base() + 1, nil
}

func (e *memblockRetriever) setDataFromSchemata(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	rows := make([][]types.Causet, 0, len(schemas))

	for _, schemaReplicant := range schemas {

		charset := allegrosql.DefaultCharset
		defCauslation := allegrosql.DefaultDefCauslationName

		if len(schemaReplicant.Charset) > 0 {
			charset = schemaReplicant.Charset // Overwrite default
		}

		if len(schemaReplicant.DefCauslate) > 0 {
			defCauslation = schemaReplicant.DefCauslate // Overwrite default
		}

		if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, "", "", allegrosql.AllPrivMask) {
			continue
		}
		record := types.MakeCausets(
			schemareplicant.CatalogVal, // CATALOG_NAME
			schemaReplicant.Name.O,         // SCHEMA_NAME
			charset,               // DEFAULT_CHARACTER_SET_NAME
			defCauslation,             // DEFAULT_COLLATION_NAME
			nil,
		)
		rows = append(rows, record)
	}
	e.rows = rows
}

func (e *memblockRetriever) setDataForStatistics(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	for _, schemaReplicant := range schemas {
		for _, block := range schemaReplicant.Blocks {
			if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, block.Name.L, "", allegrosql.AllPrivMask) {
				continue
			}
			e.setDataForStatisticsInBlock(schemaReplicant, block)
		}
	}
}

func (e *memblockRetriever) setDataForStatisticsInBlock(schemaReplicant *perceptron.DBInfo, block *perceptron.BlockInfo) {
	var rows [][]types.Causet
	if block.PKIsHandle {
		for _, defCaus := range block.DeferredCausets {
			if allegrosql.HasPriKeyFlag(defCaus.Flag) {
				record := types.MakeCausets(
					schemareplicant.CatalogVal, // TABLE_CATALOG
					schemaReplicant.Name.O,         // TABLE_SCHEMA
					block.Name.O,          // TABLE_NAME
					"0",                   // NON_UNIQUE
					schemaReplicant.Name.O,         // INDEX_SCHEMA
					"PRIMARY",             // INDEX_NAME
					1,                     // SEQ_IN_INDEX
					defCaus.Name.O,            // COLUMN_NAME
					"A",                   // COLLATION
					0,                     // CARDINALITY
					nil,                   // SUB_PART
					nil,                   // PACKED
					"",                    // NULLABLE
					"BTREE",               // INDEX_TYPE
					"",                    // COMMENT
					"",                    // INDEX_COMMENT
					"YES",                 // IS_VISIBLE
					nil,                   // Expression
				)
				rows = append(rows, record)
			}
		}
	}
	nameToDefCaus := make(map[string]*perceptron.DeferredCausetInfo, len(block.DeferredCausets))
	for _, c := range block.DeferredCausets {
		nameToDefCaus[c.Name.L] = c
	}
	for _, index := range block.Indices {
		nonUnique := "1"
		if index.Unique {
			nonUnique = "0"
		}
		for i, key := range index.DeferredCausets {
			defCaus := nameToDefCaus[key.Name.L]
			nullable := "YES"
			if allegrosql.HasNotNullFlag(defCaus.Flag) {
				nullable = ""
			}

			visible := "YES"
			if index.Invisible {
				visible = "NO"
			}

			defCausName := defCaus.Name.O
			var expression interface{}
			expression = nil
			tblDefCaus := block.DeferredCausets[defCaus.Offset]
			if tblDefCaus.Hidden {
				defCausName = "NULL"
				expression = fmt.Sprintf("(%s)", tblDefCaus.GeneratedExprString)
			}

			record := types.MakeCausets(
				schemareplicant.CatalogVal, // TABLE_CATALOG
				schemaReplicant.Name.O,         // TABLE_SCHEMA
				block.Name.O,          // TABLE_NAME
				nonUnique,             // NON_UNIQUE
				schemaReplicant.Name.O,         // INDEX_SCHEMA
				index.Name.O,          // INDEX_NAME
				i+1,                   // SEQ_IN_INDEX
				defCausName,               // COLUMN_NAME
				"A",                   // COLLATION
				0,                     // CARDINALITY
				nil,                   // SUB_PART
				nil,                   // PACKED
				nullable,              // NULLABLE
				"BTREE",               // INDEX_TYPE
				"",                    // COMMENT
				"",                    // INDEX_COMMENT
				visible,               // IS_VISIBLE
				expression,            // Expression
			)
			rows = append(rows, record)
		}
	}
	e.rows = append(e.rows, rows...)
}

func (e *memblockRetriever) setDataFromBlocks(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) error {
	blockEventsMap, defCausLengthMap, err := blockStatsCache.get(ctx)
	if err != nil {
		return err
	}

	checker := privilege.GetPrivilegeManager(ctx)

	var rows [][]types.Causet
	createTimeTp := allegrosql.TypeDatetime
	for _, schemaReplicant := range schemas {
		for _, block := range schemaReplicant.Blocks {
			defCauslation := block.DefCauslate
			if defCauslation == "" {
				defCauslation = allegrosql.DefaultDefCauslationName
			}
			createTime := types.NewTime(types.FromGoTime(block.GetUFIDelateTime()), createTimeTp, types.DefaultFsp)

			createOptions := ""

			if block.IsSequence() {
				continue
			}

			if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, block.Name.L, "", allegrosql.AllPrivMask) {
				continue
			}
			pkType := "NON-CLUSTERED"
			if !block.IsView() {
				if block.GetPartitionInfo() != nil {
					createOptions = "partitioned"
				}
				var autoIncID interface{}
				hasAutoIncID, _ := schemareplicant.HasAutoIncrementDeferredCauset(block)
				if hasAutoIncID {
					autoIncID, err = getAutoIncrementID(ctx, schemaReplicant, block)
					if err != nil {
						return err
					}
				}

				var rowCount, dataLength, indexLength uint64
				if block.GetPartitionInfo() == nil {
					rowCount = blockEventsMap[block.ID]
					dataLength, indexLength = getDataAndIndexLength(block, block.ID, rowCount, defCausLengthMap)
				} else {
					for _, pi := range block.GetPartitionInfo().Definitions {
						rowCount += blockEventsMap[pi.ID]
						parDataLen, parIndexLen := getDataAndIndexLength(block, pi.ID, blockEventsMap[pi.ID], defCausLengthMap)
						dataLength += parDataLen
						indexLength += parIndexLen
					}
				}
				avgEventLength := uint64(0)
				if rowCount != 0 {
					avgEventLength = dataLength / rowCount
				}
				blockType := "BASE TABLE"
				if soliton.IsSystemView(schemaReplicant.Name.L) {
					blockType = "SYSTEM VIEW"
				}
				if block.PKIsHandle {
					pkType = "INT CLUSTERED"
				} else if block.IsCommonHandle {
					pkType = "COMMON CLUSTERED"
				}
				shardingInfo := schemareplicant.GetShardingInfo(schemaReplicant, block)
				record := types.MakeCausets(
					schemareplicant.CatalogVal, // TABLE_CATALOG
					schemaReplicant.Name.O,         // TABLE_SCHEMA
					block.Name.O,          // TABLE_NAME
					blockType,             // TABLE_TYPE
					"InnoDB",              // ENGINE
					uint64(10),            // VERSION
					"Compact",             // ROW_FORMAT
					rowCount,              // TABLE_ROWS
					avgEventLength,          // AVG_ROW_LENGTH
					dataLength,            // DATA_LENGTH
					uint64(0),             // MAX_DATA_LENGTH
					indexLength,           // INDEX_LENGTH
					uint64(0),             // DATA_FREE
					autoIncID,             // AUTO_INCREMENT
					createTime,            // CREATE_TIME
					nil,                   // UFIDelATE_TIME
					nil,                   // CHECK_TIME
					defCauslation,             // TABLE_COLLATION
					nil,                   // CHECKSUM
					createOptions,         // CREATE_OPTIONS
					block.Comment,         // TABLE_COMMENT
					block.ID,              // MilevaDB_TABLE_ID
					shardingInfo,          // MilevaDB_ROW_ID_SHARDING_INFO
					pkType,                // MilevaDB_PK_TYPE
				)
				rows = append(rows, record)
			} else {
				record := types.MakeCausets(
					schemareplicant.CatalogVal, // TABLE_CATALOG
					schemaReplicant.Name.O,         // TABLE_SCHEMA
					block.Name.O,          // TABLE_NAME
					"VIEW",                // TABLE_TYPE
					nil,                   // ENGINE
					nil,                   // VERSION
					nil,                   // ROW_FORMAT
					nil,                   // TABLE_ROWS
					nil,                   // AVG_ROW_LENGTH
					nil,                   // DATA_LENGTH
					nil,                   // MAX_DATA_LENGTH
					nil,                   // INDEX_LENGTH
					nil,                   // DATA_FREE
					nil,                   // AUTO_INCREMENT
					createTime,            // CREATE_TIME
					nil,                   // UFIDelATE_TIME
					nil,                   // CHECK_TIME
					nil,                   // TABLE_COLLATION
					nil,                   // CHECKSUM
					nil,                   // CREATE_OPTIONS
					"VIEW",                // TABLE_COMMENT
					block.ID,              // MilevaDB_TABLE_ID
					nil,                   // MilevaDB_ROW_ID_SHARDING_INFO
					pkType,                // MilevaDB_PK_TYPE
				)
				rows = append(rows, record)
			}
		}
	}
	e.rows = rows
	return nil
}

func (e *hugeMemBlockRetriever) setDataForDeferredCausets(ctx stochastikctx.Context) error {
	checker := privilege.GetPrivilegeManager(ctx)
	e.rows = e.rows[:0]
	batch := 1024
	for ; e.dbsIdx < len(e.dbs); e.dbsIdx++ {
		schemaReplicant := e.dbs[e.dbsIdx]
		for e.tblIdx < len(schemaReplicant.Blocks) {
			block := schemaReplicant.Blocks[e.tblIdx]
			e.tblIdx++
			if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, block.Name.L, "", allegrosql.AllPrivMask) {
				continue
			}

			e.dataForDeferredCausetsInBlock(schemaReplicant, block)
			if len(e.rows) >= batch {
				return nil
			}
		}
		e.tblIdx = 0
	}
	return nil
}

func (e *hugeMemBlockRetriever) dataForDeferredCausetsInBlock(schemaReplicant *perceptron.DBInfo, tbl *perceptron.BlockInfo) {
	for i, defCaus := range tbl.DeferredCausets {
		if defCaus.Hidden {
			continue
		}
		var charMaxLen, charOctLen, numericPrecision, numericScale, datetimePrecision interface{}
		defCausLen, decimal := defCaus.Flen, defCaus.Decimal
		defaultFlen, defaultDecimal := allegrosql.GetDefaultFieldLengthAndDecimal(defCaus.Tp)
		if decimal == types.UnspecifiedLength {
			decimal = defaultDecimal
		}
		if defCausLen == types.UnspecifiedLength {
			defCausLen = defaultFlen
		}
		if defCaus.Tp == allegrosql.TypeSet {
			// Example: In MyALLEGROSQL set('a','bc','def','ghij') has length 13, because
			// len('a')+len('bc')+len('def')+len('ghij')+len(ThreeComma)=13
			// Reference link: https://bugs.allegrosql.com/bug.php?id=22613
			defCausLen = 0
			for _, ele := range defCaus.Elems {
				defCausLen += len(ele)
			}
			if len(defCaus.Elems) != 0 {
				defCausLen += (len(defCaus.Elems) - 1)
			}
			charMaxLen = defCausLen
			charOctLen = defCausLen
		} else if defCaus.Tp == allegrosql.TypeEnum {
			// Example: In MyALLEGROSQL enum('a', 'ab', 'cdef') has length 4, because
			// the longest string in the enum is 'cdef'
			// Reference link: https://bugs.allegrosql.com/bug.php?id=22613
			defCausLen = 0
			for _, ele := range defCaus.Elems {
				if len(ele) > defCausLen {
					defCausLen = len(ele)
				}
			}
			charMaxLen = defCausLen
			charOctLen = defCausLen
		} else if types.IsString(defCaus.Tp) {
			charMaxLen = defCausLen
			charOctLen = defCausLen
		} else if types.IsTypeFractionable(defCaus.Tp) {
			datetimePrecision = decimal
		} else if types.IsTypeNumeric(defCaus.Tp) {
			numericPrecision = defCausLen
			if defCaus.Tp != allegrosql.TypeFloat && defCaus.Tp != allegrosql.TypeDouble {
				numericScale = decimal
			} else if decimal != -1 {
				numericScale = decimal
			}
		}
		defCausumnType := defCaus.FieldType.SchemaReplicantStr()
		defCausumnDesc := block.NewDefCausDesc(block.ToDeferredCauset(defCaus))
		var defCausumnDefault interface{}
		if defCausumnDesc.DefaultValue != nil {
			defCausumnDefault = fmt.Sprintf("%v", defCausumnDesc.DefaultValue)
		}
		record := types.MakeCausets(
			schemareplicant.CatalogVal,                // TABLE_CATALOG
			schemaReplicant.Name.O,                        // TABLE_SCHEMA
			tbl.Name.O,                           // TABLE_NAME
			defCaus.Name.O,                           // COLUMN_NAME
			i+1,                                  // ORIGINAL_POSITION
			defCausumnDefault,                        // COLUMN_DEFAULT
			defCausumnDesc.Null,                      // IS_NULLABLE
			types.TypeToStr(defCaus.Tp, defCaus.Charset), // DATA_TYPE
			charMaxLen,                           // CHARACTER_MAXIMUM_LENGTH
			charOctLen,                           // CHARACTER_OCTET_LENGTH
			numericPrecision,                     // NUMERIC_PRECISION
			numericScale,                         // NUMERIC_SCALE
			datetimePrecision,                    // DATETIME_PRECISION
			defCausumnDesc.Charset,                   // CHARACTER_SET_NAME
			defCausumnDesc.DefCauslation,                 // COLLATION_NAME
			defCausumnType,                           // COLUMN_TYPE
			defCausumnDesc.Key,                       // COLUMN_KEY
			defCausumnDesc.Extra,                     // EXTRA
			"select,insert,uFIDelate,references",    // PRIVILEGES
			defCausumnDesc.Comment,                   // COLUMN_COMMENT
			defCaus.GeneratedExprString,              // GENERATION_EXPRESSION
		)
		e.rows = append(e.rows, record)
	}
}

func (e *memblockRetriever) setDataFromPartitions(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) error {
	blockEventsMap, defCausLengthMap, err := blockStatsCache.get(ctx)
	if err != nil {
		return err
	}
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Causet
	createTimeTp := allegrosql.TypeDatetime
	for _, schemaReplicant := range schemas {
		for _, block := range schemaReplicant.Blocks {
			if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, block.Name.L, "", allegrosql.SelectPriv) {
				continue
			}
			createTime := types.NewTime(types.FromGoTime(block.GetUFIDelateTime()), createTimeTp, types.DefaultFsp)

			var rowCount, dataLength, indexLength uint64
			if block.GetPartitionInfo() == nil {
				rowCount = blockEventsMap[block.ID]
				dataLength, indexLength = getDataAndIndexLength(block, block.ID, rowCount, defCausLengthMap)
				avgEventLength := uint64(0)
				if rowCount != 0 {
					avgEventLength = dataLength / rowCount
				}
				record := types.MakeCausets(
					schemareplicant.CatalogVal, // TABLE_CATALOG
					schemaReplicant.Name.O,         // TABLE_SCHEMA
					block.Name.O,          // TABLE_NAME
					nil,                   // PARTITION_NAME
					nil,                   // SUBPARTITION_NAME
					nil,                   // PARTITION_ORDINAL_POSITION
					nil,                   // SUBPARTITION_ORDINAL_POSITION
					nil,                   // PARTITION_METHOD
					nil,                   // SUBPARTITION_METHOD
					nil,                   // PARTITION_EXPRESSION
					nil,                   // SUBPARTITION_EXPRESSION
					nil,                   // PARTITION_DESCRIPTION
					rowCount,              // TABLE_ROWS
					avgEventLength,          // AVG_ROW_LENGTH
					dataLength,            // DATA_LENGTH
					nil,                   // MAX_DATA_LENGTH
					indexLength,           // INDEX_LENGTH
					nil,                   // DATA_FREE
					createTime,            // CREATE_TIME
					nil,                   // UFIDelATE_TIME
					nil,                   // CHECK_TIME
					nil,                   // CHECKSUM
					nil,                   // PARTITION_COMMENT
					nil,                   // NODEGROUP
					nil,                   // TABLESPACE_NAME
				)
				rows = append(rows, record)
			} else {
				for i, pi := range block.GetPartitionInfo().Definitions {
					rowCount = blockEventsMap[pi.ID]
					dataLength, indexLength = getDataAndIndexLength(block, pi.ID, blockEventsMap[pi.ID], defCausLengthMap)

					avgEventLength := uint64(0)
					if rowCount != 0 {
						avgEventLength = dataLength / rowCount
					}

					var partitionDesc string
					if block.Partition.Type == perceptron.PartitionTypeRange {
						partitionDesc = pi.LessThan[0]
					}

					partitionMethod := block.Partition.Type.String()
					partitionExpr := block.Partition.Expr
					if block.Partition.Type == perceptron.PartitionTypeRange && len(block.Partition.DeferredCausets) > 0 {
						partitionMethod = "RANGE COLUMNS"
						partitionExpr = block.Partition.DeferredCausets[0].String()
					}

					record := types.MakeCausets(
						schemareplicant.CatalogVal, // TABLE_CATALOG
						schemaReplicant.Name.O,         // TABLE_SCHEMA
						block.Name.O,          // TABLE_NAME
						pi.Name.O,             // PARTITION_NAME
						nil,                   // SUBPARTITION_NAME
						i+1,                   // PARTITION_ORDINAL_POSITION
						nil,                   // SUBPARTITION_ORDINAL_POSITION
						partitionMethod,       // PARTITION_METHOD
						nil,                   // SUBPARTITION_METHOD
						partitionExpr,         // PARTITION_EXPRESSION
						nil,                   // SUBPARTITION_EXPRESSION
						partitionDesc,         // PARTITION_DESCRIPTION
						rowCount,              // TABLE_ROWS
						avgEventLength,          // AVG_ROW_LENGTH
						dataLength,            // DATA_LENGTH
						uint64(0),             // MAX_DATA_LENGTH
						indexLength,           // INDEX_LENGTH
						uint64(0),             // DATA_FREE
						createTime,            // CREATE_TIME
						nil,                   // UFIDelATE_TIME
						nil,                   // CHECK_TIME
						nil,                   // CHECKSUM
						pi.Comment,            // PARTITION_COMMENT
						nil,                   // NODEGROUP
						nil,                   // TABLESPACE_NAME
					)
					rows = append(rows, record)
				}
			}
		}
	}
	e.rows = rows
	return nil
}

func (e *memblockRetriever) setDataFromIndexes(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Causet
	for _, schemaReplicant := range schemas {
		for _, tb := range schemaReplicant.Blocks {
			if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, tb.Name.L, "", allegrosql.AllPrivMask) {
				continue
			}

			if tb.PKIsHandle {
				var pkDefCaus *perceptron.DeferredCausetInfo
				for _, defCaus := range tb.DefCauss() {
					if allegrosql.HasPriKeyFlag(defCaus.Flag) {
						pkDefCaus = defCaus
						break
					}
				}
				record := types.MakeCausets(
					schemaReplicant.Name.O, // TABLE_SCHEMA
					tb.Name.O,     // TABLE_NAME
					0,             // NON_UNIQUE
					"PRIMARY",     // KEY_NAME
					1,             // SEQ_IN_INDEX
					pkDefCaus.Name.O,  // COLUMN_NAME
					nil,           // SUB_PART
					"",            // INDEX_COMMENT
					nil,           // Expression
					0,             // INDEX_ID
					"YES",         // IS_VISIBLE
				)
				rows = append(rows, record)
			}
			for _, idxInfo := range tb.Indices {
				if idxInfo.State != perceptron.StatePublic {
					continue
				}
				for i, defCaus := range idxInfo.DeferredCausets {
					nonUniq := 1
					if idxInfo.Unique {
						nonUniq = 0
					}
					var subPart interface{}
					if defCaus.Length != types.UnspecifiedLength {
						subPart = defCaus.Length
					}
					defCausName := defCaus.Name.O
					var expression interface{}
					expression = nil
					tblDefCaus := tb.DeferredCausets[defCaus.Offset]
					if tblDefCaus.Hidden {
						defCausName = "NULL"
						expression = fmt.Sprintf("(%s)", tblDefCaus.GeneratedExprString)
					}
					visible := "YES"
					if idxInfo.Invisible {
						visible = "NO"
					}
					record := types.MakeCausets(
						schemaReplicant.Name.O,   // TABLE_SCHEMA
						tb.Name.O,       // TABLE_NAME
						nonUniq,         // NON_UNIQUE
						idxInfo.Name.O,  // KEY_NAME
						i+1,             // SEQ_IN_INDEX
						defCausName,         // COLUMN_NAME
						subPart,         // SUB_PART
						idxInfo.Comment, // INDEX_COMMENT
						expression,      // Expression
						idxInfo.ID,      // INDEX_ID
						visible,         // IS_VISIBLE
					)
					rows = append(rows, record)
				}
			}
		}
	}
	e.rows = rows
}

func (e *memblockRetriever) setDataFromViews(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Causet
	for _, schemaReplicant := range schemas {
		for _, block := range schemaReplicant.Blocks {
			if !block.IsView() {
				continue
			}
			defCauslation := block.DefCauslate
			charset := block.Charset
			if defCauslation == "" {
				defCauslation = allegrosql.DefaultDefCauslationName
			}
			if charset == "" {
				charset = allegrosql.DefaultCharset
			}
			if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, block.Name.L, "", allegrosql.AllPrivMask) {
				continue
			}
			record := types.MakeCausets(
				schemareplicant.CatalogVal,           // TABLE_CATALOG
				schemaReplicant.Name.O,                   // TABLE_SCHEMA
				block.Name.O,                    // TABLE_NAME
				block.View.SelectStmt,           // VIEW_DEFINITION
				block.View.CheckOption.String(), // CHECK_OPTION
				"NO",                            // IS_UFIDelATABLE
				block.View.Definer.String(),     // DEFINER
				block.View.Security.String(),    // SECURITY_TYPE
				charset,                         // CHARACTER_SET_CLIENT
				defCauslation,                       // COLLATION_CONNECTION
			)
			rows = append(rows, record)
		}
	}
	e.rows = rows
}

func (e *memblockRetriever) dataForEinsteinDBStoreStatus(ctx stochastikctx.Context) (err error) {
	einsteindbStore, ok := ctx.GetStore().(einsteindb.CausetStorage)
	if !ok {
		return errors.New("Information about EinsteinDB causetstore status can be gotten only when the storage is EinsteinDB")
	}
	einsteindbHelper := &helper.Helper{
		CausetStore:       einsteindbStore,
		RegionCache: einsteindbStore.GetRegionCache(),
	}
	storesStat, err := einsteindbHelper.GetStoresStat()
	if err != nil {
		return err
	}
	for _, storeStat := range storesStat.Stores {
		event := make([]types.Causet, len(schemareplicant.BlockEinsteinDBStoreStatusDefCauss))
		event[0].SetInt64(storeStat.CausetStore.ID)
		event[1].SetString(storeStat.CausetStore.Address, allegrosql.DefaultDefCauslationName)
		event[2].SetInt64(storeStat.CausetStore.State)
		event[3].SetString(storeStat.CausetStore.StateName, allegrosql.DefaultDefCauslationName)
		data, err := json.Marshal(storeStat.CausetStore.Labels)
		if err != nil {
			return err
		}
		bj := binaryJson.BinaryJSON{}
		if err = bj.UnmarshalJSON(data); err != nil {
			return err
		}
		event[4].SetMysqlJSON(bj)
		event[5].SetString(storeStat.CausetStore.Version, allegrosql.DefaultDefCauslationName)
		event[6].SetString(storeStat.Status.Capacity, allegrosql.DefaultDefCauslationName)
		event[7].SetString(storeStat.Status.Available, allegrosql.DefaultDefCauslationName)
		event[8].SetInt64(storeStat.Status.LeaderCount)
		event[9].SetFloat64(storeStat.Status.LeaderWeight)
		event[10].SetFloat64(storeStat.Status.LeaderScore)
		event[11].SetInt64(storeStat.Status.LeaderSize)
		event[12].SetInt64(storeStat.Status.RegionCount)
		event[13].SetFloat64(storeStat.Status.RegionWeight)
		event[14].SetFloat64(storeStat.Status.RegionScore)
		event[15].SetInt64(storeStat.Status.RegionSize)
		startTs := types.NewTime(types.FromGoTime(storeStat.Status.StartTs), allegrosql.TypeDatetime, types.DefaultFsp)
		event[16].SetMysqlTime(startTs)
		lastHeartbeatTs := types.NewTime(types.FromGoTime(storeStat.Status.LastHeartbeatTs), allegrosql.TypeDatetime, types.DefaultFsp)
		event[17].SetMysqlTime(lastHeartbeatTs)
		event[18].SetString(storeStat.Status.Uptime, allegrosql.DefaultDefCauslationName)
		e.rows = append(e.rows, event)
	}
	return nil
}

// DBSJobsReaderExec executes DBSJobs information retrieving.
type DBSJobsReaderExec struct {
	baseExecutor
	DBSJobRetriever

	cacheJobs []*perceptron.Job
	is        schemareplicant.SchemaReplicant
}

// Open implements the Executor Next interface.
func (e *DBSJobsReaderExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	e.DBSJobRetriever.is = e.is
	e.activeRoles = e.ctx.GetStochastikVars().ActiveRoles
	err = e.DBSJobRetriever.initial(txn)
	if err != nil {
		return err
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *DBSJobsReaderExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	checker := privilege.GetPrivilegeManager(e.ctx)
	count := 0

	// Append running DBS jobs.
	if e.cursor < len(e.runningJobs) {
		num := mathutil.Min(req.Capacity(), len(e.runningJobs)-e.cursor)
		for i := e.cursor; i < e.cursor+num; i++ {
			e.appendJobToChunk(req, e.runningJobs[i], checker)
			req.AppendString(11, e.runningJobs[i].Query)
		}
		e.cursor += num
		count += num
	}
	var err error

	// Append history DBS jobs.
	if count < req.Capacity() {
		e.cacheJobs, err = e.historyJobIter.GetLastJobs(req.Capacity()-count, e.cacheJobs)
		if err != nil {
			return err
		}
		for _, job := range e.cacheJobs {
			e.appendJobToChunk(req, job, checker)
			req.AppendString(11, job.Query)
		}
		e.cursor += len(e.cacheJobs)
	}
	return nil
}

func (e *memblockRetriever) setDataFromEngines() {
	var rows [][]types.Causet
	rows = append(rows,
		types.MakeCausets(
			"InnoDB",  // Engine
			"DEFAULT", // Support
			"Supports transactions, event-level locking, and foreign keys", // Comment
			"YES", // Transactions
			"YES", // XA
			"YES", // Savepoints
		),
	)
	e.rows = rows
}

func (e *memblockRetriever) setDataFromCharacterSets() {
	var rows [][]types.Causet
	charsets := charset.GetSupportedCharsets()
	for _, charset := range charsets {
		rows = append(rows,
			types.MakeCausets(charset.Name, charset.DefaultDefCauslation, charset.Desc, charset.Maxlen),
		)
	}
	e.rows = rows
}

func (e *memblockRetriever) setDataFromDefCauslations() {
	var rows [][]types.Causet
	defCauslations := defCauslate.GetSupportedDefCauslations()
	for _, defCauslation := range defCauslations {
		isDefault := ""
		if defCauslation.IsDefault {
			isDefault = "Yes"
		}
		rows = append(rows,
			types.MakeCausets(defCauslation.Name, defCauslation.CharsetName, defCauslation.ID, isDefault, "Yes", 1),
		)
	}
	e.rows = rows
}

func (e *memblockRetriever) dataForDefCauslationCharacterSetApplicability() {
	var rows [][]types.Causet
	defCauslations := defCauslate.GetSupportedDefCauslations()
	for _, defCauslation := range defCauslations {
		rows = append(rows,
			types.MakeCausets(defCauslation.Name, defCauslation.CharsetName),
		)
	}
	e.rows = rows
}

func (e *memblockRetriever) dataForMilevaDBClusterInfo(ctx stochastikctx.Context) error {
	servers, err := schemareplicant.GetClusterServerInfo(ctx)
	if err != nil {
		e.rows = nil
		return err
	}
	rows := make([][]types.Causet, 0, len(servers))
	for _, server := range servers {
		startTimeStr := ""
		upTimeStr := ""
		if server.StartTimestamp > 0 {
			startTime := time.Unix(server.StartTimestamp, 0)
			startTimeStr = startTime.Format(time.RFC3339)
			upTimeStr = time.Since(startTime).String()
		}
		event := types.MakeCausets(
			server.ServerType,
			server.Address,
			server.StatusAddr,
			server.Version,
			server.GitHash,
			startTimeStr,
			upTimeStr,
		)
		rows = append(rows, event)
	}
	e.rows = rows
	return nil
}

func (e *memblockRetriever) setDataFromKeyDeferredCausetUsage(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	rows := make([][]types.Causet, 0, len(schemas)) // The capacity is not accurate, but it is not a big problem.
	for _, schemaReplicant := range schemas {
		for _, block := range schemaReplicant.Blocks {
			if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, block.Name.L, "", allegrosql.AllPrivMask) {
				continue
			}
			rs := keyDeferredCausetUsageInBlock(schemaReplicant, block)
			rows = append(rows, rs...)
		}
	}
	e.rows = rows
}

func (e *memblockRetriever) setDataForClusterProcessList(ctx stochastikctx.Context) error {
	e.setDataForProcessList(ctx)
	rows, err := schemareplicant.AppendHostInfoToEvents(e.rows)
	if err != nil {
		return err
	}
	e.rows = rows
	return nil
}

func (e *memblockRetriever) setDataForProcessList(ctx stochastikctx.Context) {
	sm := ctx.GetStochastikManager()
	if sm == nil {
		return
	}

	loginUser := ctx.GetStochastikVars().User
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(ctx); pm != nil {
		if pm.RequestVerification(ctx.GetStochastikVars().ActiveRoles, "", "", "", allegrosql.ProcessPriv) {
			hasProcessPriv = true
		}
	}

	pl := sm.ShowProcessList()

	records := make([][]types.Causet, 0, len(pl))
	for _, pi := range pl {
		// If you have the PROCESS privilege, you can see all threads.
		// Otherwise, you can see only your own threads.
		if !hasProcessPriv && loginUser != nil && pi.User != loginUser.Username {
			continue
		}

		rows := pi.ToEvent(ctx.GetStochastikVars().StmtCtx.TimeZone)
		record := types.MakeCausets(rows...)
		records = append(records, record)
	}
	e.rows = records
}

func (e *memblockRetriever) setDataFromUserPrivileges(ctx stochastikctx.Context) {
	pm := privilege.GetPrivilegeManager(ctx)
	e.rows = pm.UserPrivilegesBlock()
}

func (e *memblockRetriever) setDataForMetricBlocks(ctx stochastikctx.Context) {
	var rows [][]types.Causet
	blocks := make([]string, 0, len(schemareplicant.MetricBlockMap))
	for name := range schemareplicant.MetricBlockMap {
		blocks = append(blocks, name)
	}
	sort.Strings(blocks)
	for _, name := range blocks {
		schemaReplicant := schemareplicant.MetricBlockMap[name]
		record := types.MakeCausets(
			name,                             // METRICS_NAME
			schemaReplicant.PromQL,                    // PROMQL
			strings.Join(schemaReplicant.Labels, ","), // LABELS
			schemaReplicant.Quantile,                  // QUANTILE
			schemaReplicant.Comment,                   // COMMENT
		)
		rows = append(rows, record)
	}
	e.rows = rows
}

func keyDeferredCausetUsageInBlock(schemaReplicant *perceptron.DBInfo, block *perceptron.BlockInfo) [][]types.Causet {
	var rows [][]types.Causet
	if block.PKIsHandle {
		for _, defCaus := range block.DeferredCausets {
			if allegrosql.HasPriKeyFlag(defCaus.Flag) {
				record := types.MakeCausets(
					schemareplicant.CatalogVal,        // CONSTRAINT_CATALOG
					schemaReplicant.Name.O,                // CONSTRAINT_SCHEMA
					schemareplicant.PrimaryConstraint, // CONSTRAINT_NAME
					schemareplicant.CatalogVal,        // TABLE_CATALOG
					schemaReplicant.Name.O,                // TABLE_SCHEMA
					block.Name.O,                 // TABLE_NAME
					defCaus.Name.O,                   // COLUMN_NAME
					1,                            // ORDINAL_POSITION
					1,                            // POSITION_IN_UNIQUE_CONSTRAINT
					nil,                          // REFERENCED_TABLE_SCHEMA
					nil,                          // REFERENCED_TABLE_NAME
					nil,                          // REFERENCED_COLUMN_NAME
				)
				rows = append(rows, record)
				break
			}
		}
	}
	nameToDefCaus := make(map[string]*perceptron.DeferredCausetInfo, len(block.DeferredCausets))
	for _, c := range block.DeferredCausets {
		nameToDefCaus[c.Name.L] = c
	}
	for _, index := range block.Indices {
		var idxName string
		if index.Primary {
			idxName = schemareplicant.PrimaryConstraint
		} else if index.Unique {
			idxName = index.Name.O
		} else {
			// Only handle unique/primary key
			continue
		}
		for i, key := range index.DeferredCausets {
			defCaus := nameToDefCaus[key.Name.L]
			record := types.MakeCausets(
				schemareplicant.CatalogVal, // CONSTRAINT_CATALOG
				schemaReplicant.Name.O,         // CONSTRAINT_SCHEMA
				idxName,               // CONSTRAINT_NAME
				schemareplicant.CatalogVal, // TABLE_CATALOG
				schemaReplicant.Name.O,         // TABLE_SCHEMA
				block.Name.O,          // TABLE_NAME
				defCaus.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				nil,                   // POSITION_IN_UNIQUE_CONSTRAINT
				nil,                   // REFERENCED_TABLE_SCHEMA
				nil,                   // REFERENCED_TABLE_NAME
				nil,                   // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	for _, fk := range block.ForeignKeys {
		fkRefDefCaus := ""
		if len(fk.RefDefCauss) > 0 {
			fkRefDefCaus = fk.RefDefCauss[0].O
		}
		for i, key := range fk.DefCauss {
			defCaus := nameToDefCaus[key.L]
			record := types.MakeCausets(
				schemareplicant.CatalogVal, // CONSTRAINT_CATALOG
				schemaReplicant.Name.O,         // CONSTRAINT_SCHEMA
				fk.Name.O,             // CONSTRAINT_NAME
				schemareplicant.CatalogVal, // TABLE_CATALOG
				schemaReplicant.Name.O,         // TABLE_SCHEMA
				block.Name.O,          // TABLE_NAME
				defCaus.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				1,                     // POSITION_IN_UNIQUE_CONSTRAINT
				schemaReplicant.Name.O,         // REFERENCED_TABLE_SCHEMA
				fk.RefBlock.O,         // REFERENCED_TABLE_NAME
				fkRefDefCaus,              // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	return rows
}

func (e *memblockRetriever) setDataForEinsteinDBRegionStatus(ctx stochastikctx.Context) error {
	einsteindbStore, ok := ctx.GetStore().(einsteindb.CausetStorage)
	if !ok {
		return errors.New("Information about EinsteinDB region status can be gotten only when the storage is EinsteinDB")
	}
	einsteindbHelper := &helper.Helper{
		CausetStore:       einsteindbStore,
		RegionCache: einsteindbStore.GetRegionCache(),
	}
	regionsInfo, err := einsteindbHelper.GetRegionsInfo()
	if err != nil {
		return err
	}
	allSchemas := ctx.GetStochastikVars().TxnCtx.SchemaReplicant.(schemareplicant.SchemaReplicant).AllSchemas()
	blockInfos := einsteindbHelper.GetRegionsBlockInfo(regionsInfo, allSchemas)
	for _, region := range regionsInfo.Regions {
		blockList := blockInfos[region.ID]
		if len(blockList) == 0 {
			e.setNewEinsteinDBRegionStatusDefCaus(&region, nil)
		}
		for _, block := range blockList {
			e.setNewEinsteinDBRegionStatusDefCaus(&region, &block)
		}
	}
	return nil
}

func (e *memblockRetriever) setNewEinsteinDBRegionStatusDefCaus(region *helper.RegionInfo, block *helper.BlockInfo) {
	event := make([]types.Causet, len(schemareplicant.BlockEinsteinDBRegionStatusDefCauss))
	event[0].SetInt64(region.ID)
	event[1].SetString(region.StartKey, allegrosql.DefaultDefCauslationName)
	event[2].SetString(region.EndKey, allegrosql.DefaultDefCauslationName)
	if block != nil {
		event[3].SetInt64(block.Block.ID)
		event[4].SetString(block.EDB.Name.O, allegrosql.DefaultDefCauslationName)
		event[5].SetString(block.Block.Name.O, allegrosql.DefaultDefCauslationName)
		if block.IsIndex {
			event[6].SetInt64(1)
			event[7].SetInt64(block.Index.ID)
			event[8].SetString(block.Index.Name.O, allegrosql.DefaultDefCauslationName)
		} else {
			event[6].SetInt64(0)
		}
	}
	event[9].SetInt64(region.Epoch.ConfVer)
	event[10].SetInt64(region.Epoch.Version)
	event[11].SetInt64(region.WrittenBytes)
	event[12].SetInt64(region.ReadBytes)
	event[13].SetInt64(region.ApproximateSize)
	event[14].SetInt64(region.ApproximateKeys)
	if region.ReplicationStatus != nil {
		event[15].SetString(region.ReplicationStatus.State, allegrosql.DefaultDefCauslationName)
		event[16].SetInt64(region.ReplicationStatus.StateID)
	}
	e.rows = append(e.rows, event)
}

func (e *memblockRetriever) setDataForEinsteinDBRegionPeers(ctx stochastikctx.Context) error {
	einsteindbStore, ok := ctx.GetStore().(einsteindb.CausetStorage)
	if !ok {
		return errors.New("Information about EinsteinDB region status can be gotten only when the storage is EinsteinDB")
	}
	einsteindbHelper := &helper.Helper{
		CausetStore:       einsteindbStore,
		RegionCache: einsteindbStore.GetRegionCache(),
	}
	regionsInfo, err := einsteindbHelper.GetRegionsInfo()
	if err != nil {
		return err
	}
	for _, region := range regionsInfo.Regions {
		e.setNewEinsteinDBRegionPeersDefCauss(&region)
	}
	return nil
}

func (e *memblockRetriever) setNewEinsteinDBRegionPeersDefCauss(region *helper.RegionInfo) {
	records := make([][]types.Causet, 0, len(region.Peers))
	pendingPeerIDSet := set.NewInt64Set()
	for _, peer := range region.PendingPeers {
		pendingPeerIDSet.Insert(peer.ID)
	}
	downPeerMap := make(map[int64]int64, len(region.DownPeers))
	for _, peerStat := range region.DownPeers {
		downPeerMap[peerStat.ID] = peerStat.DownSec
	}
	for _, peer := range region.Peers {
		event := make([]types.Causet, len(schemareplicant.BlockEinsteinDBRegionPeersDefCauss))
		event[0].SetInt64(region.ID)
		event[1].SetInt64(peer.ID)
		event[2].SetInt64(peer.StoreID)
		if peer.IsLearner {
			event[3].SetInt64(1)
		} else {
			event[3].SetInt64(0)
		}
		if peer.ID == region.Leader.ID {
			event[4].SetInt64(1)
		} else {
			event[4].SetInt64(0)
		}
		if pendingPeerIDSet.Exist(peer.ID) {
			event[5].SetString(pendingPeer, allegrosql.DefaultDefCauslationName)
		} else if downSec, ok := downPeerMap[peer.ID]; ok {
			event[5].SetString(downPeer, allegrosql.DefaultDefCauslationName)
			event[6].SetInt64(downSec)
		} else {
			event[5].SetString(normalPeer, allegrosql.DefaultDefCauslationName)
		}
		records = append(records, event)
	}
	e.rows = append(e.rows, records...)
}

const (
	normalPeer  = "NORMAL"
	pendingPeer = "PENDING"
	downPeer    = "DOWN"
)

func (e *memblockRetriever) setDataForMilevaDBHotRegions(ctx stochastikctx.Context) error {
	einsteindbStore, ok := ctx.GetStore().(einsteindb.CausetStorage)
	if !ok {
		return errors.New("Information about hot region can be gotten only when the storage is EinsteinDB")
	}
	allSchemas := ctx.GetStochastikVars().TxnCtx.SchemaReplicant.(schemareplicant.SchemaReplicant).AllSchemas()
	einsteindbHelper := &helper.Helper{
		CausetStore:       einsteindbStore,
		RegionCache: einsteindbStore.GetRegionCache(),
	}
	metrics, err := einsteindbHelper.ScrapeHotInfo(FIDelapi.HotRead, allSchemas)
	if err != nil {
		return err
	}
	e.setDataForHotRegionByMetrics(metrics, "read")
	metrics, err = einsteindbHelper.ScrapeHotInfo(FIDelapi.HotWrite, allSchemas)
	if err != nil {
		return err
	}
	e.setDataForHotRegionByMetrics(metrics, "write")
	return nil
}

func (e *memblockRetriever) setDataForHotRegionByMetrics(metrics []helper.HotBlockIndex, tp string) {
	rows := make([][]types.Causet, 0, len(metrics))
	for _, tblIndex := range metrics {
		event := make([]types.Causet, len(schemareplicant.BlockMilevaDBHotRegionsDefCauss))
		if tblIndex.IndexName != "" {
			event[1].SetInt64(tblIndex.IndexID)
			event[4].SetString(tblIndex.IndexName, allegrosql.DefaultDefCauslationName)
		} else {
			event[1].SetNull()
			event[4].SetNull()
		}
		event[0].SetInt64(tblIndex.BlockID)
		event[2].SetString(tblIndex.DbName, allegrosql.DefaultDefCauslationName)
		event[3].SetString(tblIndex.BlockName, allegrosql.DefaultDefCauslationName)
		event[5].SetUint64(tblIndex.RegionID)
		event[6].SetString(tp, allegrosql.DefaultDefCauslationName)
		if tblIndex.RegionMetric == nil {
			event[7].SetNull()
			event[8].SetNull()
		} else {
			event[7].SetInt64(int64(tblIndex.RegionMetric.MaxHotDegree))
			event[8].SetInt64(int64(tblIndex.RegionMetric.Count))
		}
		event[9].SetUint64(tblIndex.RegionMetric.FlowBytes)
		rows = append(rows, event)
	}
	e.rows = append(e.rows, rows...)
}

// setDataFromBlockConstraints constructs data for block information_schema.constraints.See https://dev.allegrosql.com/doc/refman/5.7/en/block-constraints-block.html
func (e *memblockRetriever) setDataFromBlockConstraints(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Causet
	for _, schemaReplicant := range schemas {
		for _, tbl := range schemaReplicant.Blocks {
			if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, tbl.Name.L, "", allegrosql.AllPrivMask) {
				continue
			}

			if tbl.PKIsHandle {
				record := types.MakeCausets(
					schemareplicant.CatalogVal,     // CONSTRAINT_CATALOG
					schemaReplicant.Name.O,             // CONSTRAINT_SCHEMA
					allegrosql.PrimaryKeyName,      // CONSTRAINT_NAME
					schemaReplicant.Name.O,             // TABLE_SCHEMA
					tbl.Name.O,                // TABLE_NAME
					schemareplicant.PrimaryKeyType, // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
			}

			for _, idx := range tbl.Indices {
				var cname, ctype string
				if idx.Primary {
					cname = allegrosql.PrimaryKeyName
					ctype = schemareplicant.PrimaryKeyType
				} else if idx.Unique {
					cname = idx.Name.O
					ctype = schemareplicant.UniqueKeyType
				} else {
					// The index has no constriant.
					continue
				}
				record := types.MakeCausets(
					schemareplicant.CatalogVal, // CONSTRAINT_CATALOG
					schemaReplicant.Name.O,         // CONSTRAINT_SCHEMA
					cname,                 // CONSTRAINT_NAME
					schemaReplicant.Name.O,         // TABLE_SCHEMA
					tbl.Name.O,            // TABLE_NAME
					ctype,                 // CONSTRAINT_TYPE
				)
				rows = append(rows, record)
			}
		}
	}
	e.rows = rows
}

// blockStorageStatsRetriever is used to read slow log data.
type blockStorageStatsRetriever struct {
	dummyCloser
	block         *perceptron.BlockInfo
	outputDefCauss    []*perceptron.DeferredCausetInfo
	retrieved     bool
	initialized   bool
	extractor     *plannercore.BlockStorageStatsExtractor
	initialBlocks []*initialBlock
	curBlock      int
	helper        *helper.Helper
	stats         helper.FIDelRegionStats
}

func (e *blockStorageStatsRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		err := e.initialize(sctx)
		if err != nil {
			return nil, err
		}
	}
	if len(e.initialBlocks) == 0 || e.curBlock >= len(e.initialBlocks) {
		e.retrieved = true
		return nil, nil
	}

	rows, err := e.setDataForBlockStorageStats(sctx)
	if err != nil {
		return nil, err
	}
	if len(e.outputDefCauss) == len(e.block.DeferredCausets) {
		return rows, nil
	}
	retEvents := make([][]types.Causet, len(rows))
	for i, fullEvent := range rows {
		event := make([]types.Causet, len(e.outputDefCauss))
		for j, defCaus := range e.outputDefCauss {
			event[j] = fullEvent[defCaus.Offset]
		}
		retEvents[i] = event
	}
	return retEvents, nil
}

type initialBlock struct {
	EDB string
	*perceptron.BlockInfo
}

func (e *blockStorageStatsRetriever) initialize(sctx stochastikctx.Context) error {
	is := schemareplicant.GetSchemaReplicant(sctx)
	var databases []string
	schemas := e.extractor.BlockSchema
	blocks := e.extractor.BlockName

	// If not specify the block_schema, return an error to avoid traverse all schemas and their blocks.
	if len(schemas) == 0 {
		return errors.Errorf("Please specify the 'block_schema'")
	}

	// Filter the sys or memory schemaReplicant.
	for schemaReplicant := range schemas {
		if !soliton.IsMemOrSysDB(schemaReplicant) {
			databases = append(databases, schemaReplicant)
		}
	}

	// Extract the blocks to the initialBlock.
	for _, EDB := range databases {
		// The user didn't specified the block, extract all blocks of this EDB to initialBlock.
		if len(blocks) == 0 {
			tbs := is.SchemaBlocks(perceptron.NewCIStr(EDB))
			for _, tb := range tbs {
				e.initialBlocks = append(e.initialBlocks, &initialBlock{EDB, tb.Meta()})
			}
		} else {
			// The user specified the block, extract the specified blocks of this EDB to initialBlock.
			for tb := range blocks {
				if tb, err := is.BlockByName(perceptron.NewCIStr(EDB), perceptron.NewCIStr(tb)); err == nil {
					e.initialBlocks = append(e.initialBlocks, &initialBlock{EDB, tb.Meta()})
				}
			}
		}
	}

	// Cache the helper and return an error if FIDel unavailable.
	einsteindbStore, ok := sctx.GetStore().(einsteindb.CausetStorage)
	if !ok {
		return errors.Errorf("Information about EinsteinDB region status can be gotten only when the storage is EinsteinDB")
	}
	e.helper = helper.NewHelper(einsteindbStore)
	_, err := e.helper.GetFIDelAddr()
	if err != nil {
		return err
	}
	e.initialized = true
	return nil
}

func (e *blockStorageStatsRetriever) setDataForBlockStorageStats(ctx stochastikctx.Context) ([][]types.Causet, error) {
	rows := make([][]types.Causet, 0, 1024)
	count := 0
	for e.curBlock < len(e.initialBlocks) && count < 1024 {
		block := e.initialBlocks[e.curBlock]
		blockID := block.ID
		err := e.helper.GetFIDelRegionStats(blockID, &e.stats)
		if err != nil {
			return nil, err
		}
		peerCount := len(e.stats.StorePeerCount)

		record := types.MakeCausets(
			block.EDB,            // TABLE_SCHEMA
			block.Name.O,        // TABLE_NAME
			blockID,             // TABLE_ID
			peerCount,           // TABLE_PEER_COUNT
			e.stats.Count,       // TABLE_REGION_COUNT
			e.stats.EmptyCount,  // TABLE_EMPTY_REGION_COUNT
			e.stats.StorageSize, // TABLE_SIZE
			e.stats.StorageKeys, // TABLE_KEYS
		)
		rows = append(rows, record)
		count++
		e.curBlock++
	}
	return rows, nil
}

func (e *memblockRetriever) setDataFromStochastikVar(ctx stochastikctx.Context) error {
	var rows [][]types.Causet
	var err error
	stochastikVars := ctx.GetStochastikVars()
	for _, v := range variable.SysVars {
		var value string
		value, err = variable.GetStochastikSystemVar(stochastikVars, v.Name)
		if err != nil {
			return err
		}
		event := types.MakeCausets(v.Name, value)
		rows = append(rows, event)
	}
	e.rows = rows
	return nil
}

// dataForAnalyzeStatusHelper is a helper function which can be used in show_stats.go
func dataForAnalyzeStatusHelper(sctx stochastikctx.Context) (rows [][]types.Causet) {
	checker := privilege.GetPrivilegeManager(sctx)
	for _, job := range statistics.GetAllAnalyzeJobs() {
		job.Lock()
		var startTime interface{}
		if job.StartTime.IsZero() {
			startTime = nil
		} else {
			startTime = types.NewTime(types.FromGoTime(job.StartTime), allegrosql.TypeDatetime, 0)
		}
		if checker == nil || checker.RequestVerification(sctx.GetStochastikVars().ActiveRoles, job.DBName, job.BlockName, "", allegrosql.AllPrivMask) {
			rows = append(rows, types.MakeCausets(
				job.DBName,        // TABLE_SCHEMA
				job.BlockName,     // TABLE_NAME
				job.PartitionName, // PARTITION_NAME
				job.JobInfo,       // JOB_INFO
				job.EventCount,      // ROW_COUNT
				startTime,         // START_TIME
				job.State,         // STATE
			))
		}
		job.Unlock()
	}
	return
}

// setDataForAnalyzeStatus gets all the analyze jobs.
func (e *memblockRetriever) setDataForAnalyzeStatus(sctx stochastikctx.Context) {
	e.rows = dataForAnalyzeStatusHelper(sctx)
}

// setDataForPseudoProfiling returns pseudo data for block profiling when system variable `profiling` is set to `ON`.
func (e *memblockRetriever) setDataForPseudoProfiling(sctx stochastikctx.Context) {
	if v, ok := sctx.GetStochastikVars().GetSystemVar("profiling"); ok && variable.MilevaDBOptOn(v) {
		event := types.MakeCausets(
			0,                      // QUERY_ID
			0,                      // SEQ
			"",                     // STATE
			types.NewDecFromInt(0), // DURATION
			types.NewDecFromInt(0), // CPU_USER
			types.NewDecFromInt(0), // CPU_SYSTEM
			0,                      // CONTEXT_VOLUNTARY
			0,                      // CONTEXT_INVOLUNTARY
			0,                      // BLOCK_OPS_IN
			0,                      // BLOCK_OPS_OUT
			0,                      // MESSAGES_SENT
			0,                      // MESSAGES_RECEIVED
			0,                      // PAGE_FAULTS_MAJOR
			0,                      // PAGE_FAULTS_MINOR
			0,                      // SWAPS
			"",                     // SOURCE_FUNCTION
			"",                     // SOURCE_FILE
			0,                      // SOURCE_LINE
		)
		e.rows = append(e.rows, event)
	}
}

func (e *memblockRetriever) setDataForServersInfo() error {
	serversInfo, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return err
	}
	rows := make([][]types.Causet, 0, len(serversInfo))
	for _, info := range serversInfo {
		event := types.MakeCausets(
			info.ID,              // DBS_ID
			info.IP,              // IP
			int(info.Port),       // PORT
			int(info.StatusPort), // STATUS_PORT
			info.Lease,           // LEASE
			info.Version,         // VERSION
			info.GitHash,         // GIT_HASH
			info.BinlogStatus,    // BINLOG_STATUS
			stringutil.BuildStringFromLabels(info.Labels), // LABELS
		)
		rows = append(rows, event)
	}
	e.rows = rows
	return nil
}

func (e *memblockRetriever) setDataFromSequences(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Causet
	for _, schemaReplicant := range schemas {
		for _, block := range schemaReplicant.Blocks {
			if !block.IsSequence() {
				continue
			}
			if checker != nil && !checker.RequestVerification(ctx.GetStochastikVars().ActiveRoles, schemaReplicant.Name.L, block.Name.L, "", allegrosql.AllPrivMask) {
				continue
			}
			record := types.MakeCausets(
				schemareplicant.CatalogVal,     // TABLE_CATALOG
				schemaReplicant.Name.O,             // TABLE_SCHEMA
				block.Name.O,              // TABLE_NAME
				block.Sequence.Cache,      // Cache
				block.Sequence.CacheValue, // CACHE_VALUE
				block.Sequence.Cycle,      // CYCLE
				block.Sequence.Increment,  // INCREMENT
				block.Sequence.MaxValue,   // MAXVALUE
				block.Sequence.MinValue,   // MINVALUE
				block.Sequence.Start,      // START
				block.Sequence.Comment,    // COMMENT
			)
			rows = append(rows, record)
		}
	}
	e.rows = rows
}

// dataForBlockTiFlashReplica constructs data for block tiflash replica info.
func (e *memblockRetriever) dataForBlockTiFlashReplica(ctx stochastikctx.Context, schemas []*perceptron.DBInfo) {
	var rows [][]types.Causet
	progressMap, err := infosync.GetTiFlashBlockSyncProgress(context.Background())
	if err != nil {
		ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
	}
	for _, schemaReplicant := range schemas {
		for _, tbl := range schemaReplicant.Blocks {
			if tbl.TiFlashReplica == nil {
				continue
			}
			progress := 1.0
			if !tbl.TiFlashReplica.Available {
				if pi := tbl.GetPartitionInfo(); pi != nil && len(pi.Definitions) > 0 {
					progress = 0
					for _, p := range pi.Definitions {
						if tbl.TiFlashReplica.IsPartitionAvailable(p.ID) {
							progress += 1
						} else {
							progress += progressMap[p.ID]
						}
					}
					progress = progress / float64(len(pi.Definitions))
				} else {
					progress = progressMap[tbl.ID]
				}
			}
			record := types.MakeCausets(
				schemaReplicant.Name.O,                   // TABLE_SCHEMA
				tbl.Name.O,                      // TABLE_NAME
				tbl.ID,                          // TABLE_ID
				int64(tbl.TiFlashReplica.Count), // REPLICA_COUNT
				strings.Join(tbl.TiFlashReplica.LocationLabels, ","), // LOCATION_LABELS
				tbl.TiFlashReplica.Available,                         // AVAILABLE
				progress,                                             // PROGRESS
			)
			rows = append(rows, record)
		}
	}
	e.rows = rows
	return
}

func (e *memblockRetriever) setDataForStatementsSummary(ctx stochastikctx.Context, blockName string) error {
	user := ctx.GetStochastikVars().User
	isSuper := false
	if pm := privilege.GetPrivilegeManager(ctx); pm != nil {
		isSuper = pm.RequestVerificationWithUser("", "", "", allegrosql.SuperPriv, user)
	}
	switch blockName {
	case schemareplicant.BlockStatementsSummary,
		schemareplicant.ClusterBlockStatementsSummary:
		e.rows = stmtsummary.StmtSummaryByDigestMap.ToCurrentCauset(user, isSuper)
	case schemareplicant.BlockStatementsSummaryHistory,
		schemareplicant.ClusterBlockStatementsSummaryHistory:
		e.rows = stmtsummary.StmtSummaryByDigestMap.ToHistoryCauset(user, isSuper)
	}
	switch blockName {
	case schemareplicant.ClusterBlockStatementsSummary,
		schemareplicant.ClusterBlockStatementsSummaryHistory:
		rows, err := schemareplicant.AppendHostInfoToEvents(e.rows)
		if err != nil {
			return err
		}
		e.rows = rows
	}
	return nil
}

type hugeMemBlockRetriever struct {
	dummyCloser
	block       *perceptron.BlockInfo
	defCausumns     []*perceptron.DeferredCausetInfo
	retrieved   bool
	initialized bool
	rows        [][]types.Causet
	dbs         []*perceptron.DBInfo
	dbsIdx      int
	tblIdx      int
}

// retrieve implements the schemareplicantRetriever interface
func (e *hugeMemBlockRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.retrieved {
		return nil, nil
	}

	if !e.initialized {
		is := schemareplicant.GetSchemaReplicant(sctx)
		dbs := is.AllSchemas()
		sort.Sort(schemareplicant.SchemasSorter(dbs))
		e.dbs = dbs
		e.initialized = true
		e.rows = make([][]types.Causet, 0, 1024)
	}

	var err error
	switch e.block.Name.O {
	case schemareplicant.BlockDeferredCausets:
		err = e.setDataForDeferredCausets(sctx)
	}
	if err != nil {
		return nil, err
	}
	e.retrieved = len(e.rows) == 0

	return adjustDeferredCausets(e.rows, e.defCausumns, e.block), nil
}

func adjustDeferredCausets(input [][]types.Causet, outDeferredCausets []*perceptron.DeferredCausetInfo, block *perceptron.BlockInfo) [][]types.Causet {
	if len(outDeferredCausets) == len(block.DeferredCausets) {
		return input
	}
	rows := make([][]types.Causet, len(input))
	for i, fullEvent := range input {
		event := make([]types.Causet, len(outDeferredCausets))
		for j, defCaus := range outDeferredCausets {
			event[j] = fullEvent[defCaus.Offset]
		}
		rows[i] = event
	}
	return rows
}

// TiFlashSystemBlockRetriever is used to read system block from tiflash.
type TiFlashSystemBlockRetriever struct {
	dummyCloser
	block         *perceptron.BlockInfo
	outputDefCauss    []*perceptron.DeferredCausetInfo
	instanceCount int
	instanceIdx   int
	instanceInfos []tiflashInstanceInfo
	rowIdx        int
	retrieved     bool
	initialized   bool
	extractor     *plannercore.TiFlashSystemBlockExtractor
}

func (e *TiFlashSystemBlockRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		err := e.initialize(sctx, e.extractor.TiFlashInstances)
		if err != nil {
			return nil, err
		}
	}
	if e.instanceCount == 0 || e.instanceIdx >= e.instanceCount {
		e.retrieved = true
		return nil, nil
	}

	for {
		rows, err := e.dataForTiFlashSystemBlocks(sctx, e.extractor.MilevaDBDatabases, e.extractor.MilevaDBBlocks)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 || e.instanceIdx >= e.instanceCount {
			return rows, nil
		}
	}
}

type tiflashInstanceInfo struct {
	id  string
	url string
}

func (e *TiFlashSystemBlockRetriever) initialize(sctx stochastikctx.Context, tiflashInstances set.StringSet) error {
	causetstore := sctx.GetStore()
	if etcd, ok := causetstore.(einsteindb.EtcdBackend); ok {
		var addrs []string
		var err error
		if addrs, err = etcd.EtcdAddrs(); err != nil {
			return err
		}
		if addrs != nil {
			petriFromCtx := petri.GetPetri(sctx)
			if petriFromCtx != nil {
				cli := petriFromCtx.GetEtcdClient()
				prefix := "/tiflash/cluster/http_port/"
				ekv := clientv3.NewKV(cli)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				resp, err := ekv.Get(ctx, prefix, clientv3.WithPrefix())
				cancel()
				if err != nil {
					return errors.Trace(err)
				}
				for _, ev := range resp.Kvs {
					id := string(ev.Key)[len(prefix):]
					if len(tiflashInstances) > 0 && !tiflashInstances.Exist(id) {
						continue
					}
					url := fmt.Sprintf("%s://%s", soliton.InternalHTTPSchema(), ev.Value)
					req, err := http.NewRequest(http.MethodGet, url, nil)
					if err != nil {
						return errors.Trace(err)
					}
					_, err = soliton.InternalHTTPClient().Do(req)
					if err != nil {
						sctx.GetStochastikVars().StmtCtx.AppendWarning(err)
						continue
					}
					e.instanceInfos = append(e.instanceInfos, tiflashInstanceInfo{
						id:  id,
						url: url,
					})
					e.instanceCount += 1
				}
				e.initialized = true
				return nil
			}
		}
		return errors.Errorf("Etcd addrs not found")
	}
	return errors.Errorf("%T not an etcd backend", causetstore)
}

func (e *TiFlashSystemBlockRetriever) dataForTiFlashSystemBlocks(ctx stochastikctx.Context, milevadbDatabases string, milevadbBlocks string) ([][]types.Causet, error) {
	var defCausumnNames []string
	for _, c := range e.outputDefCauss {
		if c.Name.O == "TIFLASH_INSTANCE" {
			continue
		}
		defCausumnNames = append(defCausumnNames, c.Name.L)
	}
	maxCount := 1024
	targetBlock := strings.ToLower(strings.Replace(e.block.Name.O, "TIFLASH", "DT", 1))
	var filters []string
	if len(milevadbDatabases) > 0 {
		filters = append(filters, fmt.Sprintf("milevadb_database IN (%s)", strings.ReplaceAll(milevadbDatabases, "\"", "'")))
	}
	if len(milevadbBlocks) > 0 {
		filters = append(filters, fmt.Sprintf("milevadb_block IN (%s)", strings.ReplaceAll(milevadbBlocks, "\"", "'")))
	}
	allegrosql := fmt.Sprintf("SELECT %s FROM system.%s", strings.Join(defCausumnNames, ","), targetBlock)
	if len(filters) > 0 {
		allegrosql = fmt.Sprintf("%s WHERE %s", allegrosql, strings.Join(filters, " AND "))
	}
	allegrosql = fmt.Sprintf("%s LIMIT %d, %d", allegrosql, e.rowIdx, maxCount)
	notNumber := "nan"
	instanceInfo := e.instanceInfos[e.instanceIdx]
	url := instanceInfo.url
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	q := req.URL.Query()
	q.Add("query", allegrosql)
	req.URL.RawQuery = q.Encode()
	resp, err := soliton.InternalHTTPClient().Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	terror.Log(resp.Body.Close())
	if err != nil {
		return nil, errors.Trace(err)
	}
	records := strings.Split(string(body), "\n")
	var rows [][]types.Causet
	for _, record := range records {
		if len(record) == 0 {
			continue
		}
		fields := strings.Split(record, "\t")
		if len(fields) < len(e.outputDefCauss)-1 {
			return nil, errors.Errorf("Record from tiflash doesn't match schemaReplicant %v", fields)
		}
		event := make([]types.Causet, len(e.outputDefCauss))
		for index, defCausumn := range e.outputDefCauss {
			if defCausumn.Name.O == "TIFLASH_INSTANCE" {
				continue
			}
			if defCausumn.Tp == allegrosql.TypeVarchar {
				event[index].SetString(fields[index], allegrosql.DefaultDefCauslationName)
			} else if defCausumn.Tp == allegrosql.TypeLonglong {
				if fields[index] == notNumber {
					continue
				}
				value, err := strconv.ParseInt(fields[index], 10, 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
				event[index].SetInt64(value)
			} else if defCausumn.Tp == allegrosql.TypeDouble {
				if fields[index] == notNumber {
					continue
				}
				value, err := strconv.ParseFloat(fields[index], 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
				event[index].SetFloat64(value)
			} else {
				return nil, errors.Errorf("Meet defCausumn of unknown type %v", defCausumn)
			}
		}
		event[len(e.outputDefCauss)-1].SetString(instanceInfo.id, allegrosql.DefaultDefCauslationName)
		rows = append(rows, event)
	}
	e.rowIdx += len(rows)
	if len(rows) < maxCount {
		e.instanceIdx += 1
		e.rowIdx = 0
	}
	return rows, nil
}
