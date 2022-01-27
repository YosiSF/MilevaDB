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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/spacetimepb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/FIDelapi"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

const (
	// BlockSchemata is the string constant of schemareplicant causet.
	BlockSchemata = "SCHEMATA"
	// BlockBlocks is the string constant of schemareplicant causet.
	BlockBlocks = "TABLES"
	// BlockDeferredCausets is the string constant of schemareplicant causet
	BlockDeferredCausets          = "COLUMNS"
	blockDeferredCausetStatistics = "COLUMN_STATISTICS"
	// BlockStatistics is the string constant of schemareplicant causet
	BlockStatistics = "STATISTICS"
	// BlockCharacterSets is the string constant of schemareplicant charactersets memory causet
	BlockCharacterSets = "CHARACTER_SETS"
	// BlockDefCauslations is the string constant of schemareplicant defCauslations memory causet.
	BlockDefCauslations = "COLLATIONS"
	blockFiles          = "FILES"
	// CatalogVal is the string constant of TABLE_CATALOG.
	CatalogVal = "def"
	// BlockProfiling is the string constant of schemareplicant causet.
	BlockProfiling = "PROFILING"
	// BlockPartitions is the string constant of schemareplicant causet.
	BlockPartitions = "PARTITIONS"
	// BlockKeyDeferredCauset is the string constant of KEY_COLUMN_USAGE.
	BlockKeyDeferredCauset = "KEY_COLUMN_USAGE"
	blockReferConst        = "REFERENTIAL_CONSTRAINTS"
	// BlockStochastikVar is the string constant of SESSION_VARIABLES.
	BlockStochastikVar = "SESSION_VARIABLES"
	blockPlugins       = "PLUGINS"
	// BlockConstraints is the string constant of TABLE_CONSTRAINTS.
	BlockConstraints = "TABLE_CONSTRAINTS"
	blockTriggers    = "TRIGGERS"
	// BlockUserPrivileges is the string constant of schemareplicant user privilege causet.
	BlockUserPrivileges           = "USER_PRIVILEGES"
	blockSchemaPrivileges         = "SCHEMA_PRIVILEGES"
	blockBlockPrivileges          = "TABLE_PRIVILEGES"
	blockDeferredCausetPrivileges = "COLUMN_PRIVILEGES"
	// BlockEngines is the string constant of schemareplicant causet.
	BlockEngines = "ENGINES"
	// BlockViews is the string constant of schemareplicant causet.
	BlockViews            = "VIEWS"
	blockRoutines         = "ROUTINES"
	blockParameters       = "PARAMETERS"
	blockEvents           = "EVENTS"
	blockGlobalStatus     = "GLOBAL_STATUS"
	blockGlobalVariables  = "GLOBAL_VARIABLES"
	blockStochastikStatus = "SESSION_STATUS"
	blockOptimizerTrace   = "OPTIMIZER_TRACE"
	blockBlockSpaces      = "TABLESPACES"
	// BlockDefCauslationCharacterSetApplicability is the string constant of schemareplicant memory causet.
	BlockDefCauslationCharacterSetApplicability = "COLLATION_CHARACTER_SET_APPLICABILITY"
	// BlockProcesslist is the string constant of schemareplicant causet.
	BlockProcesslist = "PROCESSLIST"
	// BlockMilevaDBIndexes is the string constant of schemareplicant causet
	BlockMilevaDBIndexes = "MilevaDB_INDEXES"
	// BlockMilevaDBHotRegions is the string constant of schemareplicant causet
	BlockMilevaDBHotRegions = "MilevaDB_HOT_REGIONS"
	// BlockEinsteinDBStoreStatus is the string constant of schemareplicant causet
	BlockEinsteinDBStoreStatus = "EinsteinDB_STORE_STATUS"
	// BlockAnalyzeStatus is the string constant of Analyze Status
	BlockAnalyzeStatus = "ANALYZE_STATUS"
	// BlockEinsteinDBRegionStatus is the string constant of schemareplicant causet
	BlockEinsteinDBRegionStatus = "EinsteinDB_REGION_STATUS"
	// BlockEinsteinDBRegionPeers is the string constant of schemareplicant causet
	BlockEinsteinDBRegionPeers = "EinsteinDB_REGION_PEERS"
	// BlockMilevaDBServersInfo is the string constant of MilevaDB server information causet.
	BlockMilevaDBServersInfo = "MilevaDB_SERVERS_INFO"
	// BlockSlowQuery is the string constant of slow query memory causet.
	BlockSlowQuery = "SLOW_QUERY"
	// BlockClusterInfo is the string constant of cluster info memory causet.
	BlockClusterInfo = "CLUSTER_INFO"
	// BlockClusterConfig is the string constant of cluster configuration memory causet.
	BlockClusterConfig = "CLUSTER_CONFIG"
	// BlockClusterLog is the string constant of cluster log memory causet.
	BlockClusterLog = "CLUSTER_LOG"
	// BlockClusterLoad is the string constant of cluster load memory causet.
	BlockClusterLoad = "CLUSTER_LOAD"
	// BlockClusterHardware is the string constant of cluster hardware causet.
	BlockClusterHardware = "CLUSTER_HARDWARE"
	// BlockClusterSystemInfo is the string constant of cluster system info causet.
	BlockClusterSystemInfo = "CLUSTER_SYSTEMINFO"
	// BlockTiFlashReplica is the string constant of tiflash replica causet.
	BlockTiFlashReplica = "TIFLASH_REPLICA"
	// BlockInspectionResult is the string constant of inspection result causet.
	BlockInspectionResult = "INSPECTION_RESULT"
	// BlockMetricBlocks is a causet that contains all metrics causet definition.
	BlockMetricBlocks = "METRICS_TABLES"
	// BlockMetricSummary is a summary causet that contains all metrics.
	BlockMetricSummary = "METRICS_SUMMARY"
	// BlockMetricSummaryByLabel is a metric causet that contains all metrics that group by label info.
	BlockMetricSummaryByLabel = "METRICS_SUMMARY_BY_LABEL"
	// BlockInspectionSummary is the string constant of inspection summary causet.
	BlockInspectionSummary = "INSPECTION_SUMMARY"
	// BlockInspectionMemrules is the string constant of currently implemented inspection and summary rules.
	BlockInspectionMemrules = "INSPECTION_RULES"
	// BlockDBSJobs is the string constant of DBS job causet.
	BlockDBSJobs = "DBS_JOBS"
	// BlockSequences is the string constant of all sequences created by user.
	BlockSequences = "SEQUENCES"
	// BlockStatementsSummary is the string constant of memex summary causet.
	BlockStatementsSummary = "STATEMENTS_SUMMARY"
	// BlockStatementsSummaryHistory is the string constant of memexs summary history causet.
	BlockStatementsSummaryHistory = "STATEMENTS_SUMMARY_HISTORY"
	// BlockStorageStats is a causet that contains all blocks disk usage
	BlockStorageStats = "TABLE_STORAGE_STATS"
	// BlockTiFlashBlocks is the string constant of tiflash blocks causet.
	BlockTiFlashBlocks = "TIFLASH_TABLES"
	// BlockTiFlashSegments is the string constant of tiflash segments causet.
	BlockTiFlashSegments = "TIFLASH_SEGMENTS"
)

var blockIDMap = map[string]int64{
	BlockSchemata:                 autoid.InformationSchemaDBID + 1,
	BlockBlocks:                   autoid.InformationSchemaDBID + 2,
	BlockDeferredCausets:          autoid.InformationSchemaDBID + 3,
	blockDeferredCausetStatistics: autoid.InformationSchemaDBID + 4,
	BlockStatistics:               autoid.InformationSchemaDBID + 5,
	BlockCharacterSets:            autoid.InformationSchemaDBID + 6,
	BlockDefCauslations:           autoid.InformationSchemaDBID + 7,
	blockFiles:                    autoid.InformationSchemaDBID + 8,
	CatalogVal:                    autoid.InformationSchemaDBID + 9,
	BlockProfiling:                autoid.InformationSchemaDBID + 10,
	BlockPartitions:               autoid.InformationSchemaDBID + 11,
	BlockKeyDeferredCauset:        autoid.InformationSchemaDBID + 12,
	blockReferConst:               autoid.InformationSchemaDBID + 13,
	BlockStochastikVar:            autoid.InformationSchemaDBID + 14,
	blockPlugins:                  autoid.InformationSchemaDBID + 15,
	BlockConstraints:              autoid.InformationSchemaDBID + 16,
	blockTriggers:                 autoid.InformationSchemaDBID + 17,
	BlockUserPrivileges:           autoid.InformationSchemaDBID + 18,
	blockSchemaPrivileges:         autoid.InformationSchemaDBID + 19,
	blockBlockPrivileges:          autoid.InformationSchemaDBID + 20,
	blockDeferredCausetPrivileges: autoid.InformationSchemaDBID + 21,
	BlockEngines:                  autoid.InformationSchemaDBID + 22,
	BlockViews:                    autoid.InformationSchemaDBID + 23,
	blockRoutines:                 autoid.InformationSchemaDBID + 24,
	blockParameters:               autoid.InformationSchemaDBID + 25,
	blockEvents:                   autoid.InformationSchemaDBID + 26,
	blockGlobalStatus:             autoid.InformationSchemaDBID + 27,
	blockGlobalVariables:          autoid.InformationSchemaDBID + 28,
	blockStochastikStatus:         autoid.InformationSchemaDBID + 29,
	blockOptimizerTrace:           autoid.InformationSchemaDBID + 30,
	blockBlockSpaces:              autoid.InformationSchemaDBID + 31,
	BlockDefCauslationCharacterSetApplicability: autoid.InformationSchemaDBID + 32,
	BlockProcesslist:                     autoid.InformationSchemaDBID + 33,
	BlockMilevaDBIndexes:                 autoid.InformationSchemaDBID + 34,
	BlockSlowQuery:                       autoid.InformationSchemaDBID + 35,
	BlockMilevaDBHotRegions:              autoid.InformationSchemaDBID + 36,
	BlockEinsteinDBStoreStatus:           autoid.InformationSchemaDBID + 37,
	BlockAnalyzeStatus:                   autoid.InformationSchemaDBID + 38,
	BlockEinsteinDBRegionStatus:          autoid.InformationSchemaDBID + 39,
	BlockEinsteinDBRegionPeers:           autoid.InformationSchemaDBID + 40,
	BlockMilevaDBServersInfo:             autoid.InformationSchemaDBID + 41,
	BlockClusterInfo:                     autoid.InformationSchemaDBID + 42,
	BlockClusterConfig:                   autoid.InformationSchemaDBID + 43,
	BlockClusterLoad:                     autoid.InformationSchemaDBID + 44,
	BlockTiFlashReplica:                  autoid.InformationSchemaDBID + 45,
	ClusterBlockSlowLog:                  autoid.InformationSchemaDBID + 46,
	ClusterBlockProcesslist:              autoid.InformationSchemaDBID + 47,
	BlockClusterLog:                      autoid.InformationSchemaDBID + 48,
	BlockClusterHardware:                 autoid.InformationSchemaDBID + 49,
	BlockClusterSystemInfo:               autoid.InformationSchemaDBID + 50,
	BlockInspectionResult:                autoid.InformationSchemaDBID + 51,
	BlockMetricSummary:                   autoid.InformationSchemaDBID + 52,
	BlockMetricSummaryByLabel:            autoid.InformationSchemaDBID + 53,
	BlockMetricBlocks:                    autoid.InformationSchemaDBID + 54,
	BlockInspectionSummary:               autoid.InformationSchemaDBID + 55,
	BlockInspectionMemrules:              autoid.InformationSchemaDBID + 56,
	BlockDBSJobs:                         autoid.InformationSchemaDBID + 57,
	BlockSequences:                       autoid.InformationSchemaDBID + 58,
	BlockStatementsSummary:               autoid.InformationSchemaDBID + 59,
	BlockStatementsSummaryHistory:        autoid.InformationSchemaDBID + 60,
	ClusterBlockStatementsSummary:        autoid.InformationSchemaDBID + 61,
	ClusterBlockStatementsSummaryHistory: autoid.InformationSchemaDBID + 62,
	BlockStorageStats:                    autoid.InformationSchemaDBID + 63,
	BlockTiFlashBlocks:                   autoid.InformationSchemaDBID + 64,
	BlockTiFlashSegments:                 autoid.InformationSchemaDBID + 65,
}

type defCausumnInfo struct {
	name    string
	tp      byte
	size    int
	decimal int
	flag    uint
	deflt   interface{}
	comment string
}

func buildDeferredCausetInfo(defCaus defCausumnInfo) *perceptron.DeferredCausetInfo {
	mCharset := charset.CharsetBin
	mDefCauslation := charset.CharsetBin
	if defCaus.tp == allegrosql.TypeVarchar || defCaus.tp == allegrosql.TypeBlob || defCaus.tp == allegrosql.TypeLongBlob {
		mCharset = charset.CharsetUTF8MB4
		mDefCauslation = charset.DefCauslationUTF8MB4
	}
	fieldType := types.FieldType{
		Charset:     mCharset,
		DefCauslate: mDefCauslation,
		Tp:          defCaus.tp,
		Flen:        defCaus.size,
		Decimal:     defCaus.decimal,
		Flag:        defCaus.flag,
	}
	return &perceptron.DeferredCausetInfo{
		Name:         perceptron.NewCIStr(defCaus.name),
		FieldType:    fieldType,
		State:        perceptron.StatePublic,
		DefaultValue: defCaus.deflt,
		Comment:      defCaus.comment,
	}
}

func buildBlockMeta(blockName string, cs []defCausumnInfo) *perceptron.BlockInfo {
	defcaus := make([]*perceptron.DeferredCausetInfo, 0, len(cs))
	for _, c := range cs {
		defcaus = append(defcaus, buildDeferredCausetInfo(c))
	}
	for i, defCaus := range defcaus {
		defCaus.Offset = i
	}
	return &perceptron.BlockInfo{
		Name:            perceptron.NewCIStr(blockName),
		DeferredCausets: defcaus,
		State:           perceptron.StatePublic,
		Charset:         allegrosql.DefaultCharset,
		DefCauslate:     allegrosql.DefaultDefCauslationName,
	}
}

var schemataDefCauss = []defCausumnInfo{
	{name: "CATALOG_NAME", tp: allegrosql.TypeVarchar, size: 512},
	{name: "SCHEMA_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DEFAULT_CHARACTER_SET_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DEFAULT_COLLATION_NAME", tp: allegrosql.TypeVarchar, size: 32},
	{name: "ALLEGROSQL_PATH", tp: allegrosql.TypeVarchar, size: 512},
}

var blocksDefCauss = []defCausumnInfo{
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "ENGINE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "VERSION", tp: allegrosql.TypeLonglong, size: 21},
	{name: "ROW_FORMAT", tp: allegrosql.TypeVarchar, size: 10},
	{name: "TABLE_ROWS", tp: allegrosql.TypeLonglong, size: 21},
	{name: "AVG_ROW_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DATA_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "MAX_DATA_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "INDEX_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DATA_FREE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "AUTO_INCREMENT", tp: allegrosql.TypeLonglong, size: 21},
	{name: "CREATE_TIME", tp: allegrosql.TypeDatetime, size: 19},
	{name: "UFIDelATE_TIME", tp: allegrosql.TypeDatetime, size: 19},
	{name: "CHECK_TIME", tp: allegrosql.TypeDatetime, size: 19},
	{name: "TABLE_COLLATION", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag, deflt: "utf8_bin"},
	{name: "CHECKSUM", tp: allegrosql.TypeLonglong, size: 21},
	{name: "CREATE_OPTIONS", tp: allegrosql.TypeVarchar, size: 255},
	{name: "TABLE_COMMENT", tp: allegrosql.TypeVarchar, size: 2048},
	{name: "MilevaDB_TABLE_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "MilevaDB_ROW_ID_SHARDING_INFO", tp: allegrosql.TypeVarchar, size: 255},
	{name: "MilevaDB_PK_TYPE", tp: allegrosql.TypeVarchar, size: 64},
}

// See: http://dev.allegrosql.com/doc/refman/5.7/en/defCausumns-causet.html
var defCausumnsDefCauss = []defCausumnInfo{
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "COLUMN_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "ORDINAL_POSITION", tp: allegrosql.TypeLonglong, size: 64},
	{name: "COLUMN_DEFAULT", tp: allegrosql.TypeBlob, size: 196606},
	{name: "IS_NULLABLE", tp: allegrosql.TypeVarchar, size: 3},
	{name: "DATA_TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "CHARACTER_MAXIMUM_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "CHARACTER_OCTET_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "NUMERIC_PRECISION", tp: allegrosql.TypeLonglong, size: 21},
	{name: "NUMERIC_SCALE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DATETIME_PRECISION", tp: allegrosql.TypeLonglong, size: 21},
	{name: "CHARACTER_SET_NAME", tp: allegrosql.TypeVarchar, size: 32},
	{name: "COLLATION_NAME", tp: allegrosql.TypeVarchar, size: 32},
	{name: "COLUMN_TYPE", tp: allegrosql.TypeBlob, size: 196606},
	{name: "COLUMN_KEY", tp: allegrosql.TypeVarchar, size: 3},
	{name: "EXTRA", tp: allegrosql.TypeVarchar, size: 30},
	{name: "PRIVILEGES", tp: allegrosql.TypeVarchar, size: 80},
	{name: "COLUMN_COMMENT", tp: allegrosql.TypeVarchar, size: 1024},
	{name: "GENERATION_EXPRESSION", tp: allegrosql.TypeBlob, size: 589779, flag: allegrosql.NotNullFlag},
}

var defCausumnStatisticsDefCauss = []defCausumnInfo{
	{name: "SCHEMA_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "COLUMN_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "HISTOGRAM", tp: allegrosql.TypeJSON, size: 51},
}

var statisticsDefCauss = []defCausumnInfo{
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "NON_UNIQUE", tp: allegrosql.TypeVarchar, size: 1},
	{name: "INDEX_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INDEX_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SEQ_IN_INDEX", tp: allegrosql.TypeLonglong, size: 2},
	{name: "COLUMN_NAME", tp: allegrosql.TypeVarchar, size: 21},
	{name: "COLLATION", tp: allegrosql.TypeVarchar, size: 1},
	{name: "CARDINALITY", tp: allegrosql.TypeLonglong, size: 21},
	{name: "SUB_PART", tp: allegrosql.TypeLonglong, size: 3},
	{name: "PACKED", tp: allegrosql.TypeVarchar, size: 10},
	{name: "NULLABLE", tp: allegrosql.TypeVarchar, size: 3},
	{name: "INDEX_TYPE", tp: allegrosql.TypeVarchar, size: 16},
	{name: "COMMENT", tp: allegrosql.TypeVarchar, size: 16},
	{name: "INDEX_COMMENT", tp: allegrosql.TypeVarchar, size: 1024},
	{name: "IS_VISIBLE", tp: allegrosql.TypeVarchar, size: 3},
	{name: "Expression", tp: allegrosql.TypeVarchar, size: 64},
}

var profilingDefCauss = []defCausumnInfo{
	{name: "QUERY_ID", tp: allegrosql.TypeLong, size: 20},
	{name: "SEQ", tp: allegrosql.TypeLong, size: 20},
	{name: "STATE", tp: allegrosql.TypeVarchar, size: 30},
	{name: "DURATION", tp: allegrosql.TypeNewDecimal, size: 9},
	{name: "CPU_USER", tp: allegrosql.TypeNewDecimal, size: 9},
	{name: "CPU_SYSTEM", tp: allegrosql.TypeNewDecimal, size: 9},
	{name: "CONTEXT_VOLUNTARY", tp: allegrosql.TypeLong, size: 20},
	{name: "CONTEXT_INVOLUNTARY", tp: allegrosql.TypeLong, size: 20},
	{name: "BLOCK_OPS_IN", tp: allegrosql.TypeLong, size: 20},
	{name: "BLOCK_OPS_OUT", tp: allegrosql.TypeLong, size: 20},
	{name: "MESSAGES_SENT", tp: allegrosql.TypeLong, size: 20},
	{name: "MESSAGES_RECEIVED", tp: allegrosql.TypeLong, size: 20},
	{name: "PAGE_FAULTS_MAJOR", tp: allegrosql.TypeLong, size: 20},
	{name: "PAGE_FAULTS_MINOR", tp: allegrosql.TypeLong, size: 20},
	{name: "SWAPS", tp: allegrosql.TypeLong, size: 20},
	{name: "SOURCE_FUNCTION", tp: allegrosql.TypeVarchar, size: 30},
	{name: "SOURCE_FILE", tp: allegrosql.TypeVarchar, size: 20},
	{name: "SOURCE_LINE", tp: allegrosql.TypeLong, size: 20},
}

var charsetDefCauss = []defCausumnInfo{
	{name: "CHARACTER_SET_NAME", tp: allegrosql.TypeVarchar, size: 32},
	{name: "DEFAULT_COLLATE_NAME", tp: allegrosql.TypeVarchar, size: 32},
	{name: "DESCRIPTION", tp: allegrosql.TypeVarchar, size: 60},
	{name: "MAXLEN", tp: allegrosql.TypeLonglong, size: 3},
}

var defCauslationsDefCauss = []defCausumnInfo{
	{name: "COLLATION_NAME", tp: allegrosql.TypeVarchar, size: 32},
	{name: "CHARACTER_SET_NAME", tp: allegrosql.TypeVarchar, size: 32},
	{name: "ID", tp: allegrosql.TypeLonglong, size: 11},
	{name: "IS_DEFAULT", tp: allegrosql.TypeVarchar, size: 3},
	{name: "IS_COMPILED", tp: allegrosql.TypeVarchar, size: 3},
	{name: "SORTLEN", tp: allegrosql.TypeLonglong, size: 3},
}

var keyDeferredCausetUsageDefCauss = []defCausumnInfo{
	{name: "CONSTRAINT_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "CONSTRAINT_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "CONSTRAINT_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "COLUMN_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "ORDINAL_POSITION", tp: allegrosql.TypeLonglong, size: 10, flag: allegrosql.NotNullFlag},
	{name: "POSITION_IN_UNIQUE_CONSTRAINT", tp: allegrosql.TypeLonglong, size: 10},
	{name: "REFERENCED_TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "REFERENCED_TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "REFERENCED_COLUMN_NAME", tp: allegrosql.TypeVarchar, size: 64},
}

// See http://dev.allegrosql.com/doc/refman/5.7/en/referential-constraints-causet.html
var referConstDefCauss = []defCausumnInfo{
	{name: "CONSTRAINT_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "CONSTRAINT_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "CONSTRAINT_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "UNIQUE_CONSTRAINT_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "UNIQUE_CONSTRAINT_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "UNIQUE_CONSTRAINT_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "MATCH_OPTION", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "UFIDelATE_RULE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "DELETE_RULE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "REFERENCED_TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
}

// See http://dev.allegrosql.com/doc/refman/5.7/en/variables-causet.html
var stochastikVarDefCauss = []defCausumnInfo{
	{name: "VARIABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "VARIABLE_VALUE", tp: allegrosql.TypeVarchar, size: 1024},
}

// See https://dev.allegrosql.com/doc/refman/5.7/en/plugins-causet.html
var pluginsDefCauss = []defCausumnInfo{
	{name: "PLUGIN_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PLUGIN_VERSION", tp: allegrosql.TypeVarchar, size: 20},
	{name: "PLUGIN_STATUS", tp: allegrosql.TypeVarchar, size: 10},
	{name: "PLUGIN_TYPE", tp: allegrosql.TypeVarchar, size: 80},
	{name: "PLUGIN_TYPE_VERSION", tp: allegrosql.TypeVarchar, size: 20},
	{name: "PLUGIN_LIBRARY", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PLUGIN_LIBRARY_VERSION", tp: allegrosql.TypeVarchar, size: 20},
	{name: "PLUGIN_AUTHOR", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PLUGIN_DESCRIPTION", tp: allegrosql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "PLUGIN_LICENSE", tp: allegrosql.TypeVarchar, size: 80},
	{name: "LOAD_OPTION", tp: allegrosql.TypeVarchar, size: 64},
}

// See https://dev.allegrosql.com/doc/refman/5.7/en/partitions-causet.html
var partitionsDefCauss = []defCausumnInfo{
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PARTITION_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SUBPARTITION_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PARTITION_ORDINAL_POSITION", tp: allegrosql.TypeLonglong, size: 21},
	{name: "SUBPARTITION_ORDINAL_POSITION", tp: allegrosql.TypeLonglong, size: 21},
	{name: "PARTITION_METHOD", tp: allegrosql.TypeVarchar, size: 18},
	{name: "SUBPARTITION_METHOD", tp: allegrosql.TypeVarchar, size: 12},
	{name: "PARTITION_EXPRESSION", tp: allegrosql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "SUBPARTITION_EXPRESSION", tp: allegrosql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "PARTITION_DESCRIPTION", tp: allegrosql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "TABLE_ROWS", tp: allegrosql.TypeLonglong, size: 21},
	{name: "AVG_ROW_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DATA_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "MAX_DATA_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "INDEX_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DATA_FREE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "CREATE_TIME", tp: allegrosql.TypeDatetime},
	{name: "UFIDelATE_TIME", tp: allegrosql.TypeDatetime},
	{name: "CHECK_TIME", tp: allegrosql.TypeDatetime},
	{name: "CHECKSUM", tp: allegrosql.TypeLonglong, size: 21},
	{name: "PARTITION_COMMENT", tp: allegrosql.TypeVarchar, size: 80},
	{name: "NODEGROUP", tp: allegrosql.TypeVarchar, size: 12},
	{name: "TABLESPACE_NAME", tp: allegrosql.TypeVarchar, size: 64},
}

var blockConstraintsDefCauss = []defCausumnInfo{
	{name: "CONSTRAINT_CATALOG", tp: allegrosql.TypeVarchar, size: 512},
	{name: "CONSTRAINT_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "CONSTRAINT_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "CONSTRAINT_TYPE", tp: allegrosql.TypeVarchar, size: 64},
}

var blockTriggersDefCauss = []defCausumnInfo{
	{name: "TRIGGER_CATALOG", tp: allegrosql.TypeVarchar, size: 512},
	{name: "TRIGGER_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TRIGGER_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "EVENT_MANIPULATION", tp: allegrosql.TypeVarchar, size: 6},
	{name: "EVENT_OBJECT_CATALOG", tp: allegrosql.TypeVarchar, size: 512},
	{name: "EVENT_OBJECT_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "EVENT_OBJECT_TABLE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "ACTION_ORDER", tp: allegrosql.TypeLonglong, size: 4},
	{name: "ACTION_CONDITION", tp: allegrosql.TypeBlob, size: -1},
	{name: "ACTION_STATEMENT", tp: allegrosql.TypeBlob, size: -1},
	{name: "ACTION_ORIENTATION", tp: allegrosql.TypeVarchar, size: 9},
	{name: "ACTION_TIMING", tp: allegrosql.TypeVarchar, size: 6},
	{name: "ACTION_REFERENCE_OLD_TABLE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "ACTION_REFERENCE_NEW_TABLE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "ACTION_REFERENCE_OLD_ROW", tp: allegrosql.TypeVarchar, size: 3},
	{name: "ACTION_REFERENCE_NEW_ROW", tp: allegrosql.TypeVarchar, size: 3},
	{name: "CREATED", tp: allegrosql.TypeDatetime, size: 2},
	{name: "ALLEGROSQL_MODE", tp: allegrosql.TypeVarchar, size: 8192},
	{name: "DEFINER", tp: allegrosql.TypeVarchar, size: 77},
	{name: "CHARACTER_SET_CLIENT", tp: allegrosql.TypeVarchar, size: 32},
	{name: "COLLATION_CONNECTION", tp: allegrosql.TypeVarchar, size: 32},
	{name: "DATABASE_COLLATION", tp: allegrosql.TypeVarchar, size: 32},
}

var blockUserPrivilegesDefCauss = []defCausumnInfo{
	{name: "GRANTEE", tp: allegrosql.TypeVarchar, size: 81},
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512},
	{name: "PRIVILEGE_TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "IS_GRANTABLE", tp: allegrosql.TypeVarchar, size: 3},
}

var blockSchemaPrivilegesDefCauss = []defCausumnInfo{
	{name: "GRANTEE", tp: allegrosql.TypeVarchar, size: 81, flag: allegrosql.NotNullFlag},
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "PRIVILEGE_TYPE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "IS_GRANTABLE", tp: allegrosql.TypeVarchar, size: 3, flag: allegrosql.NotNullFlag},
}

var blockBlockPrivilegesDefCauss = []defCausumnInfo{
	{name: "GRANTEE", tp: allegrosql.TypeVarchar, size: 81, flag: allegrosql.NotNullFlag},
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "PRIVILEGE_TYPE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "IS_GRANTABLE", tp: allegrosql.TypeVarchar, size: 3, flag: allegrosql.NotNullFlag},
}

var blockDeferredCausetPrivilegesDefCauss = []defCausumnInfo{
	{name: "GRANTEE", tp: allegrosql.TypeVarchar, size: 81, flag: allegrosql.NotNullFlag},
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "COLUMN_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "PRIVILEGE_TYPE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "IS_GRANTABLE", tp: allegrosql.TypeVarchar, size: 3, flag: allegrosql.NotNullFlag},
}

var blockEnginesDefCauss = []defCausumnInfo{
	{name: "ENGINE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SUPPORT", tp: allegrosql.TypeVarchar, size: 8},
	{name: "COMMENT", tp: allegrosql.TypeVarchar, size: 80},
	{name: "TRANSACTIONS", tp: allegrosql.TypeVarchar, size: 3},
	{name: "XA", tp: allegrosql.TypeVarchar, size: 3},
	{name: "SAVEPOINTS", tp: allegrosql.TypeVarchar, size: 3},
}

var blockViewsDefCauss = []defCausumnInfo{
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "VIEW_DEFINITION", tp: allegrosql.TypeLongBlob, flag: allegrosql.NotNullFlag},
	{name: "CHECK_OPTION", tp: allegrosql.TypeVarchar, size: 8, flag: allegrosql.NotNullFlag},
	{name: "IS_UFIDelATABLE", tp: allegrosql.TypeVarchar, size: 3, flag: allegrosql.NotNullFlag},
	{name: "DEFINER", tp: allegrosql.TypeVarchar, size: 77, flag: allegrosql.NotNullFlag},
	{name: "SECURITY_TYPE", tp: allegrosql.TypeVarchar, size: 7, flag: allegrosql.NotNullFlag},
	{name: "CHARACTER_SET_CLIENT", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
	{name: "COLLATION_CONNECTION", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
}

var blockRoutinesDefCauss = []defCausumnInfo{
	{name: "SPECIFIC_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "ROUTINE_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "ROUTINE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "ROUTINE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "ROUTINE_TYPE", tp: allegrosql.TypeVarchar, size: 9, flag: allegrosql.NotNullFlag},
	{name: "DATA_TYPE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "CHARACTER_MAXIMUM_LENGTH", tp: allegrosql.TypeLong, size: 21},
	{name: "CHARACTER_OCTET_LENGTH", tp: allegrosql.TypeLong, size: 21},
	{name: "NUMERIC_PRECISION", tp: allegrosql.TypeLonglong, size: 21},
	{name: "NUMERIC_SCALE", tp: allegrosql.TypeLong, size: 21},
	{name: "DATETIME_PRECISION", tp: allegrosql.TypeLonglong, size: 21},
	{name: "CHARACTER_SET_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "COLLATION_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DTD_IDENTIFIER", tp: allegrosql.TypeLongBlob},
	{name: "ROUTINE_BODY", tp: allegrosql.TypeVarchar, size: 8, flag: allegrosql.NotNullFlag},
	{name: "ROUTINE_DEFINITION", tp: allegrosql.TypeLongBlob},
	{name: "EXTERNAL_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "EXTERNAL_LANGUAGE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PARAMETER_STYLE", tp: allegrosql.TypeVarchar, size: 8, flag: allegrosql.NotNullFlag},
	{name: "IS_DETERMINISTIC", tp: allegrosql.TypeVarchar, size: 3, flag: allegrosql.NotNullFlag},
	{name: "ALLEGROSQL_DATA_ACCESS", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "ALLEGROSQL_PATH", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SECURITY_TYPE", tp: allegrosql.TypeVarchar, size: 7, flag: allegrosql.NotNullFlag},
	{name: "CREATED", tp: allegrosql.TypeDatetime, flag: allegrosql.NotNullFlag, deflt: "0000-00-00 00:00:00"},
	{name: "LAST_ALTERED", tp: allegrosql.TypeDatetime, flag: allegrosql.NotNullFlag, deflt: "0000-00-00 00:00:00"},
	{name: "ALLEGROSQL_MODE", tp: allegrosql.TypeVarchar, size: 8192, flag: allegrosql.NotNullFlag},
	{name: "ROUTINE_COMMENT", tp: allegrosql.TypeLongBlob},
	{name: "DEFINER", tp: allegrosql.TypeVarchar, size: 77, flag: allegrosql.NotNullFlag},
	{name: "CHARACTER_SET_CLIENT", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
	{name: "COLLATION_CONNECTION", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
	{name: "DATABASE_COLLATION", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
}

var blockParametersDefCauss = []defCausumnInfo{
	{name: "SPECIFIC_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "SPECIFIC_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "SPECIFIC_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "ORDINAL_POSITION", tp: allegrosql.TypeVarchar, size: 21, flag: allegrosql.NotNullFlag},
	{name: "PARAMETER_MODE", tp: allegrosql.TypeVarchar, size: 5},
	{name: "PARAMETER_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DATA_TYPE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "CHARACTER_MAXIMUM_LENGTH", tp: allegrosql.TypeVarchar, size: 21},
	{name: "CHARACTER_OCTET_LENGTH", tp: allegrosql.TypeVarchar, size: 21},
	{name: "NUMERIC_PRECISION", tp: allegrosql.TypeVarchar, size: 21},
	{name: "NUMERIC_SCALE", tp: allegrosql.TypeVarchar, size: 21},
	{name: "DATETIME_PRECISION", tp: allegrosql.TypeVarchar, size: 21},
	{name: "CHARACTER_SET_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "COLLATION_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DTD_IDENTIFIER", tp: allegrosql.TypeLongBlob, flag: allegrosql.NotNullFlag},
	{name: "ROUTINE_TYPE", tp: allegrosql.TypeVarchar, size: 9, flag: allegrosql.NotNullFlag},
}

var blockEventsDefCauss = []defCausumnInfo{
	{name: "EVENT_CATALOG", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "EVENT_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "EVENT_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "DEFINER", tp: allegrosql.TypeVarchar, size: 77, flag: allegrosql.NotNullFlag},
	{name: "TIME_ZONE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "EVENT_BODY", tp: allegrosql.TypeVarchar, size: 8, flag: allegrosql.NotNullFlag},
	{name: "EVENT_DEFINITION", tp: allegrosql.TypeLongBlob},
	{name: "EVENT_TYPE", tp: allegrosql.TypeVarchar, size: 9, flag: allegrosql.NotNullFlag},
	{name: "EXECUTE_AT", tp: allegrosql.TypeDatetime},
	{name: "INTERVAL_VALUE", tp: allegrosql.TypeVarchar, size: 256},
	{name: "INTERVAL_FIELD", tp: allegrosql.TypeVarchar, size: 18},
	{name: "ALLEGROSQL_MODE", tp: allegrosql.TypeVarchar, size: 8192, flag: allegrosql.NotNullFlag},
	{name: "STARTS", tp: allegrosql.TypeDatetime},
	{name: "ENDS", tp: allegrosql.TypeDatetime},
	{name: "STATUS", tp: allegrosql.TypeVarchar, size: 18, flag: allegrosql.NotNullFlag},
	{name: "ON_COMPLETION", tp: allegrosql.TypeVarchar, size: 12, flag: allegrosql.NotNullFlag},
	{name: "CREATED", tp: allegrosql.TypeDatetime, flag: allegrosql.NotNullFlag, deflt: "0000-00-00 00:00:00"},
	{name: "LAST_ALTERED", tp: allegrosql.TypeDatetime, flag: allegrosql.NotNullFlag, deflt: "0000-00-00 00:00:00"},
	{name: "LAST_EXECUTED", tp: allegrosql.TypeDatetime},
	{name: "EVENT_COMMENT", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "ORIGINATOR", tp: allegrosql.TypeLong, size: 10, flag: allegrosql.NotNullFlag, deflt: 0},
	{name: "CHARACTER_SET_CLIENT", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
	{name: "COLLATION_CONNECTION", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
	{name: "DATABASE_COLLATION", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
}

var blockGlobalStatusDefCauss = []defCausumnInfo{
	{name: "VARIABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "VARIABLE_VALUE", tp: allegrosql.TypeVarchar, size: 1024},
}

var blockGlobalVariablesDefCauss = []defCausumnInfo{
	{name: "VARIABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "VARIABLE_VALUE", tp: allegrosql.TypeVarchar, size: 1024},
}

var blockStochastikStatusDefCauss = []defCausumnInfo{
	{name: "VARIABLE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "VARIABLE_VALUE", tp: allegrosql.TypeVarchar, size: 1024},
}

var blockOptimizerTraceDefCauss = []defCausumnInfo{
	{name: "QUERY", tp: allegrosql.TypeLongBlob, flag: allegrosql.NotNullFlag, deflt: ""},
	{name: "TRACE", tp: allegrosql.TypeLongBlob, flag: allegrosql.NotNullFlag, deflt: ""},
	{name: "MISSING_BYTES_BEYOND_MAX_MEM_SIZE", tp: allegrosql.TypeShort, size: 20, flag: allegrosql.NotNullFlag, deflt: 0},
	{name: "INSUFFICIENT_PRIVILEGES", tp: allegrosql.TypeTiny, size: 1, flag: allegrosql.NotNullFlag, deflt: 0},
}

var blockBlockSpacesDefCauss = []defCausumnInfo{
	{name: "TABLESPACE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag, deflt: ""},
	{name: "ENGINE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag, deflt: ""},
	{name: "TABLESPACE_TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LOGFILE_GROUP_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "EXTENT_SIZE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "AUTOEXTEND_SIZE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "MAXIMUM_SIZE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "NODEGROUP_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "TABLESPACE_COMMENT", tp: allegrosql.TypeVarchar, size: 2048},
}

var blockDefCauslationCharacterSetApplicabilityDefCauss = []defCausumnInfo{
	{name: "COLLATION_NAME", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
	{name: "CHARACTER_SET_NAME", tp: allegrosql.TypeVarchar, size: 32, flag: allegrosql.NotNullFlag},
}

var blockProcesslistDefCauss = []defCausumnInfo{
	{name: "ID", tp: allegrosql.TypeLonglong, size: 21, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, deflt: 0},
	{name: "USER", tp: allegrosql.TypeVarchar, size: 16, flag: allegrosql.NotNullFlag, deflt: ""},
	{name: "HOST", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag, deflt: ""},
	{name: "EDB", tp: allegrosql.TypeVarchar, size: 64},
	{name: "COMMAND", tp: allegrosql.TypeVarchar, size: 16, flag: allegrosql.NotNullFlag, deflt: ""},
	{name: "TIME", tp: allegrosql.TypeLong, size: 7, flag: allegrosql.NotNullFlag, deflt: 0},
	{name: "STATE", tp: allegrosql.TypeVarchar, size: 7},
	{name: "INFO", tp: allegrosql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "DIGEST", tp: allegrosql.TypeVarchar, size: 64, deflt: ""},
	{name: "MEM", tp: allegrosql.TypeLonglong, size: 21, flag: allegrosql.UnsignedFlag},
	{name: "TxnStart", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag, deflt: ""},
}

var blockMilevaDBIndexesDefCauss = []defCausumnInfo{
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "NON_UNIQUE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "KEY_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SEQ_IN_INDEX", tp: allegrosql.TypeLonglong, size: 21},
	{name: "COLUMN_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SUB_PART", tp: allegrosql.TypeLonglong, size: 21},
	{name: "INDEX_COMMENT", tp: allegrosql.TypeVarchar, size: 2048},
	{name: "Expression", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INDEX_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "IS_VISIBLE", tp: allegrosql.TypeVarchar, size: 64},
}

var slowQueryDefCauss = []defCausumnInfo{
	{name: variable.SlowLogTimeStr, tp: allegrosql.TypeTimestamp, size: 26, decimal: 6},
	{name: variable.SlowLogTxnStartTSStr, tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.UnsignedFlag},
	{name: variable.SlowLogUserStr, tp: allegrosql.TypeVarchar, size: 64},
	{name: variable.SlowLogHostStr, tp: allegrosql.TypeVarchar, size: 64},
	{name: variable.SlowLogConnIDStr, tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.UnsignedFlag},
	{name: variable.SlowLogInterDircRetryCount, tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.UnsignedFlag},
	{name: variable.SlowLogInterDircRetryTime, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogQueryTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogParseTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogCompileTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogRewriteTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogPreprocSubQueriesStr, tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.UnsignedFlag},
	{name: variable.SlowLogPreProcSubQueryTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogOptimizeTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogWaitTSTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.PreWriteTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.WaitPrewriteBinlogTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.CommitTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.GetCommitTSTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.CommitBackoffTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.BackoffTypesStr, tp: allegrosql.TypeVarchar, size: 64},
	{name: execdetails.ResolveLockTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.LocalLatchWaitTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.WriteKeysStr, tp: allegrosql.TypeLonglong, size: 22},
	{name: execdetails.WriteSizeStr, tp: allegrosql.TypeLonglong, size: 22},
	{name: execdetails.PrewriteRegionStr, tp: allegrosql.TypeLonglong, size: 22},
	{name: execdetails.TxnRetryStr, tp: allegrosql.TypeLonglong, size: 22},
	{name: execdetails.CopTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.ProcessTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.WaitTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.BackoffTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.LockKeysTimeStr, tp: allegrosql.TypeDouble, size: 22},
	{name: execdetails.RequestCountStr, tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.UnsignedFlag},
	{name: execdetails.TotalKeysStr, tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.UnsignedFlag},
	{name: execdetails.ProcessKeysStr, tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.UnsignedFlag},
	{name: variable.SlowLogDBStr, tp: allegrosql.TypeVarchar, size: 64},
	{name: variable.SlowLogIndexNamesStr, tp: allegrosql.TypeVarchar, size: 100},
	{name: variable.SlowLogIsInternalStr, tp: allegrosql.TypeTiny, size: 1},
	{name: variable.SlowLogDigestStr, tp: allegrosql.TypeVarchar, size: 64},
	{name: variable.SlowLogStatsInfoStr, tp: allegrosql.TypeVarchar, size: 512},
	{name: variable.SlowLogCopProcAvg, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogCopProcP90, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogCopProcMax, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogCopProcAddr, tp: allegrosql.TypeVarchar, size: 64},
	{name: variable.SlowLogCopWaitAvg, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogCopWaitP90, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogCopWaitMax, tp: allegrosql.TypeDouble, size: 22},
	{name: variable.SlowLogCopWaitAddr, tp: allegrosql.TypeVarchar, size: 64},
	{name: variable.SlowLogMemMax, tp: allegrosql.TypeLonglong, size: 20},
	{name: variable.SlowLogDiskMax, tp: allegrosql.TypeLonglong, size: 20},
	{name: variable.SlowLogSucc, tp: allegrosql.TypeTiny, size: 1},
	{name: variable.SlowLogCausetFromCache, tp: allegrosql.TypeTiny, size: 1},
	{name: variable.SlowLogCauset, tp: allegrosql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: variable.SlowLogCausetDigest, tp: allegrosql.TypeVarchar, size: 128},
	{name: variable.SlowLogPrevStmt, tp: allegrosql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: variable.SlowLogQueryALLEGROSQLStr, tp: allegrosql.TypeLongBlob, size: types.UnspecifiedLength},
}

// BlockMilevaDBHotRegionsDefCauss is MilevaDB hot region mem causet defCausumns.
var BlockMilevaDBHotRegionsDefCauss = []defCausumnInfo{
	{name: "TABLE_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "INDEX_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DB_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INDEX_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "REGION_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "MAX_HOT_DEGREE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "REGION_COUNT", tp: allegrosql.TypeLonglong, size: 21},
	{name: "FLOW_BYTES", tp: allegrosql.TypeLonglong, size: 21},
}

// BlockEinsteinDBStoreStatusDefCauss is MilevaDB ekv causetstore status defCausumns.
var BlockEinsteinDBStoreStatusDefCauss = []defCausumnInfo{
	{name: "STORE_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "ADDRESS", tp: allegrosql.TypeVarchar, size: 64},
	{name: "STORE_STATE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "STORE_STATE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LABEL", tp: allegrosql.TypeJSON, size: 51},
	{name: "VERSION", tp: allegrosql.TypeVarchar, size: 64},
	{name: "CAPACITY", tp: allegrosql.TypeVarchar, size: 64},
	{name: "AVAILABLE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LEADER_COUNT", tp: allegrosql.TypeLonglong, size: 21},
	{name: "LEADER_WEIGHT", tp: allegrosql.TypeDouble, size: 22},
	{name: "LEADER_SCORE", tp: allegrosql.TypeDouble, size: 22},
	{name: "LEADER_SIZE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "REGION_COUNT", tp: allegrosql.TypeLonglong, size: 21},
	{name: "REGION_WEIGHT", tp: allegrosql.TypeDouble, size: 22},
	{name: "REGION_SCORE", tp: allegrosql.TypeDouble, size: 22},
	{name: "REGION_SIZE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "START_TS", tp: allegrosql.TypeDatetime},
	{name: "LAST_HEARTBEAT_TS", tp: allegrosql.TypeDatetime},
	{name: "UPTIME", tp: allegrosql.TypeVarchar, size: 64},
}

var blockAnalyzeStatusDefCauss = []defCausumnInfo{
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PARTITION_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "JOB_INFO", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PROCESSED_ROWS", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.UnsignedFlag},
	{name: "START_TIME", tp: allegrosql.TypeDatetime},
	{name: "STATE", tp: allegrosql.TypeVarchar, size: 64},
}

// BlockEinsteinDBRegionStatusDefCauss is EinsteinDB region status mem causet defCausumns.
var BlockEinsteinDBRegionStatusDefCauss = []defCausumnInfo{
	{name: "REGION_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "START_KEY", tp: allegrosql.TypeBlob, size: types.UnspecifiedLength},
	{name: "END_KEY", tp: allegrosql.TypeBlob, size: types.UnspecifiedLength},
	{name: "TABLE_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DB_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "IS_INDEX", tp: allegrosql.TypeTiny, size: 1, flag: allegrosql.NotNullFlag, deflt: 0},
	{name: "INDEX_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "INDEX_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "EPOCH_CONF_VER", tp: allegrosql.TypeLonglong, size: 21},
	{name: "EPOCH_VERSION", tp: allegrosql.TypeLonglong, size: 21},
	{name: "WRITTEN_BYTES", tp: allegrosql.TypeLonglong, size: 21},
	{name: "READ_BYTES", tp: allegrosql.TypeLonglong, size: 21},
	{name: "APPROXIMATE_SIZE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "APPROXIMATE_KEYS", tp: allegrosql.TypeLonglong, size: 21},
	{name: "REPLICATIONSTATUS_STATE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "REPLICATIONSTATUS_STATEID", tp: allegrosql.TypeLonglong, size: 21},
}

// BlockEinsteinDBRegionPeersDefCauss is EinsteinDB region peers mem causet defCausumns.
var BlockEinsteinDBRegionPeersDefCauss = []defCausumnInfo{
	{name: "REGION_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "PEER_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "STORE_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "IS_LEARNER", tp: allegrosql.TypeTiny, size: 1, flag: allegrosql.NotNullFlag, deflt: 0},
	{name: "IS_LEADER", tp: allegrosql.TypeTiny, size: 1, flag: allegrosql.NotNullFlag, deflt: 0},
	{name: "STATUS", tp: allegrosql.TypeVarchar, size: 10, deflt: 0},
	{name: "DOWN_SECONDS", tp: allegrosql.TypeLonglong, size: 21, deflt: 0},
}

var blockMilevaDBServersInfoDefCauss = []defCausumnInfo{
	{name: "DBS_ID", tp: allegrosql.TypeVarchar, size: 64},
	{name: "IP", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PORT", tp: allegrosql.TypeLonglong, size: 21},
	{name: "STATUS_PORT", tp: allegrosql.TypeLonglong, size: 21},
	{name: "LEASE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "VERSION", tp: allegrosql.TypeVarchar, size: 64},
	{name: "GIT_HASH", tp: allegrosql.TypeVarchar, size: 64},
	{name: "BINLOG_STATUS", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LABELS", tp: allegrosql.TypeVarchar, size: 128},
}

var blockClusterConfigDefCauss = []defCausumnInfo{
	{name: "TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "KEY", tp: allegrosql.TypeVarchar, size: 256},
	{name: "VALUE", tp: allegrosql.TypeVarchar, size: 128},
}

var blockClusterLogDefCauss = []defCausumnInfo{
	{name: "TIME", tp: allegrosql.TypeVarchar, size: 32},
	{name: "TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LEVEL", tp: allegrosql.TypeVarchar, size: 8},
	{name: "MESSAGE", tp: allegrosql.TypeLongBlob, size: types.UnspecifiedLength},
}

var blockClusterLoadDefCauss = []defCausumnInfo{
	{name: "TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DEVICE_TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DEVICE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "NAME", tp: allegrosql.TypeVarchar, size: 256},
	{name: "VALUE", tp: allegrosql.TypeVarchar, size: 128},
}

var blockClusterHardwareDefCauss = []defCausumnInfo{
	{name: "TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DEVICE_TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DEVICE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "NAME", tp: allegrosql.TypeVarchar, size: 256},
	{name: "VALUE", tp: allegrosql.TypeVarchar, size: 128},
}

var blockClusterSystemInfoDefCauss = []defCausumnInfo{
	{name: "TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SYSTEM_TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SYSTEM_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "NAME", tp: allegrosql.TypeVarchar, size: 256},
	{name: "VALUE", tp: allegrosql.TypeVarchar, size: 128},
}

var filesDefCauss = []defCausumnInfo{
	{name: "FILE_ID", tp: allegrosql.TypeLonglong, size: 4},
	{name: "FILE_NAME", tp: allegrosql.TypeVarchar, size: 4000},
	{name: "FILE_TYPE", tp: allegrosql.TypeVarchar, size: 20},
	{name: "TABLESPACE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LOGFILE_GROUP_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LOGFILE_GROUP_NUMBER", tp: allegrosql.TypeLonglong, size: 32},
	{name: "ENGINE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "FULLTEXT_KEYS", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DELETED_ROWS", tp: allegrosql.TypeLonglong, size: 4},
	{name: "UFIDelATE_COUNT", tp: allegrosql.TypeLonglong, size: 4},
	{name: "FREE_EXTENTS", tp: allegrosql.TypeLonglong, size: 4},
	{name: "TOTAL_EXTENTS", tp: allegrosql.TypeLonglong, size: 4},
	{name: "EXTENT_SIZE", tp: allegrosql.TypeLonglong, size: 4},
	{name: "INITIAL_SIZE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "MAXIMUM_SIZE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "AUTOEXTEND_SIZE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "CREATION_TIME", tp: allegrosql.TypeDatetime, size: -1},
	{name: "LAST_UFIDelATE_TIME", tp: allegrosql.TypeDatetime, size: -1},
	{name: "LAST_ACCESS_TIME", tp: allegrosql.TypeDatetime, size: -1},
	{name: "RECOVER_TIME", tp: allegrosql.TypeLonglong, size: 4},
	{name: "TRANSACTION_COUNTER", tp: allegrosql.TypeLonglong, size: 4},
	{name: "VERSION", tp: allegrosql.TypeLonglong, size: 21},
	{name: "ROW_FORMAT", tp: allegrosql.TypeVarchar, size: 10},
	{name: "TABLE_ROWS", tp: allegrosql.TypeLonglong, size: 21},
	{name: "AVG_ROW_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DATA_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "MAX_DATA_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "INDEX_LENGTH", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DATA_FREE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "CREATE_TIME", tp: allegrosql.TypeDatetime, size: -1},
	{name: "UFIDelATE_TIME", tp: allegrosql.TypeDatetime, size: -1},
	{name: "CHECK_TIME", tp: allegrosql.TypeDatetime, size: -1},
	{name: "CHECKSUM", tp: allegrosql.TypeLonglong, size: 21},
	{name: "STATUS", tp: allegrosql.TypeVarchar, size: 20},
	{name: "EXTRA", tp: allegrosql.TypeVarchar, size: 255},
}

var blockClusterInfoDefCauss = []defCausumnInfo{
	{name: "TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "STATUS_ADDRESS", tp: allegrosql.TypeVarchar, size: 64},
	{name: "VERSION", tp: allegrosql.TypeVarchar, size: 64},
	{name: "GIT_HASH", tp: allegrosql.TypeVarchar, size: 64},
	{name: "START_TIME", tp: allegrosql.TypeVarchar, size: 32},
	{name: "UPTIME", tp: allegrosql.TypeVarchar, size: 32},
}

var blockBlockTiFlashReplicaDefCauss = []defCausumnInfo{
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "REPLICA_COUNT", tp: allegrosql.TypeLonglong, size: 64},
	{name: "LOCATION_LABELS", tp: allegrosql.TypeVarchar, size: 64},
	{name: "AVAILABLE", tp: allegrosql.TypeTiny, size: 1},
	{name: "PROGRESS", tp: allegrosql.TypeDouble, size: 22},
}

var blockInspectionResultDefCauss = []defCausumnInfo{
	{name: "RULE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "ITEM", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "STATUS_ADDRESS", tp: allegrosql.TypeVarchar, size: 64},
	{name: "VALUE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "REFERENCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SEVERITY", tp: allegrosql.TypeVarchar, size: 64},
	{name: "DETAILS", tp: allegrosql.TypeVarchar, size: 256},
}

var blockInspectionSummaryDefCauss = []defCausumnInfo{
	{name: "RULE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "METRICS_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LABEL", tp: allegrosql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: allegrosql.TypeDouble, size: 22},
	{name: "AVG_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "MIN_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "MAX_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "COMMENT", tp: allegrosql.TypeVarchar, size: 256},
}

var blockInspectionMemrulesDefCauss = []defCausumnInfo{
	{name: "NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "COMMENT", tp: allegrosql.TypeVarchar, size: 256},
}

var blockMetricBlocksDefCauss = []defCausumnInfo{
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "PROMQL", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LABELS", tp: allegrosql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: allegrosql.TypeDouble, size: 22},
	{name: "COMMENT", tp: allegrosql.TypeVarchar, size: 256},
}

var blockMetricSummaryDefCauss = []defCausumnInfo{
	{name: "METRICS_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: allegrosql.TypeDouble, size: 22},
	{name: "SUM_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "AVG_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "MIN_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "MAX_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "COMMENT", tp: allegrosql.TypeVarchar, size: 256},
}

var blockMetricSummaryByLabelDefCauss = []defCausumnInfo{
	{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "METRICS_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "LABEL", tp: allegrosql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: allegrosql.TypeDouble, size: 22},
	{name: "SUM_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "AVG_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "MIN_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "MAX_VALUE", tp: allegrosql.TypeDouble, size: 22, decimal: 6},
	{name: "COMMENT", tp: allegrosql.TypeVarchar, size: 256},
}

var blockDBSJobsDefCauss = []defCausumnInfo{
	{name: "JOB_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "DB_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "JOB_TYPE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SCHEMA_STATE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "SCHEMA_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "TABLE_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "ROW_COUNT", tp: allegrosql.TypeLonglong, size: 21},
	{name: "START_TIME", tp: allegrosql.TypeDatetime, size: 19},
	{name: "END_TIME", tp: allegrosql.TypeDatetime, size: 19},
	{name: "STATE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "QUERY", tp: allegrosql.TypeVarchar, size: 64},
}

var blockSequencesDefCauss = []defCausumnInfo{
	{name: "TABLE_CATALOG", tp: allegrosql.TypeVarchar, size: 512, flag: allegrosql.NotNullFlag},
	{name: "SEQUENCE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "SEQUENCE_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "CACHE", tp: allegrosql.TypeTiny, flag: allegrosql.NotNullFlag},
	{name: "CACHE_VALUE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "CYCLE", tp: allegrosql.TypeTiny, flag: allegrosql.NotNullFlag},
	{name: "INCREMENT", tp: allegrosql.TypeLonglong, size: 21, flag: allegrosql.NotNullFlag},
	{name: "MAX_VALUE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "MIN_VALUE", tp: allegrosql.TypeLonglong, size: 21},
	{name: "START", tp: allegrosql.TypeLonglong, size: 21},
	{name: "COMMENT", tp: allegrosql.TypeVarchar, size: 64},
}

var blockStatementsSummaryDefCauss = []defCausumnInfo{
	{name: "SUMMARY_BEGIN_TIME", tp: allegrosql.TypeTimestamp, size: 26, flag: allegrosql.NotNullFlag, comment: "Begin time of this summary"},
	{name: "SUMMARY_END_TIME", tp: allegrosql.TypeTimestamp, size: 26, flag: allegrosql.NotNullFlag, comment: "End time of this summary"},
	{name: "STMT_TYPE", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag, comment: "Statement type"},
	{name: "SCHEMA_NAME", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag, comment: "Current schemaReplicant"},
	{name: "DIGEST", tp: allegrosql.TypeVarchar, size: 64, flag: allegrosql.NotNullFlag},
	{name: "DIGEST_TEXT", tp: allegrosql.TypeBlob, size: types.UnspecifiedLength, flag: allegrosql.NotNullFlag, comment: "Normalized memex"},
	{name: "TABLE_NAMES", tp: allegrosql.TypeBlob, size: types.UnspecifiedLength, comment: "Involved blocks"},
	{name: "INDEX_NAMES", tp: allegrosql.TypeBlob, size: types.UnspecifiedLength, comment: "Used indices"},
	{name: "SAMPLE_USER", tp: allegrosql.TypeVarchar, size: 64, comment: "Sampled user who executed these memexs"},
	{name: "EXEC_COUNT", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Count of executions"},
	{name: "SUM_ERRORS", tp: allegrosql.TypeLong, size: 11, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Sum of errors"},
	{name: "SUM_WARNINGS", tp: allegrosql.TypeLong, size: 11, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Sum of warnings"},
	{name: "SUM_LATENCY", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Sum latency of these memexs"},
	{name: "MAX_LATENCY", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max latency of these memexs"},
	{name: "MIN_LATENCY", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Min latency of these memexs"},
	{name: "AVG_LATENCY", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average latency of these memexs"},
	{name: "AVG_PARSE_LATENCY", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average latency of parsing"},
	{name: "MAX_PARSE_LATENCY", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max latency of parsing"},
	{name: "AVG_COMPILE_LATENCY", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average latency of compiling"},
	{name: "MAX_COMPILE_LATENCY", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max latency of compiling"},
	{name: "SUM_COP_TASK_NUM", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Total number of CausetTasks"},
	{name: "MAX_COP_PROCESS_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max processing time of CausetTasks"},
	{name: "MAX_COP_PROCESS_ADDRESS", tp: allegrosql.TypeVarchar, size: 256, comment: "Address of the CopTask with max processing time"},
	{name: "MAX_COP_WAIT_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max waiting time of CausetTasks"},
	{name: "MAX_COP_WAIT_ADDRESS", tp: allegrosql.TypeVarchar, size: 256, comment: "Address of the CopTask with max waiting time"},
	{name: "AVG_PROCESS_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average processing time in EinsteinDB"},
	{name: "MAX_PROCESS_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max processing time in EinsteinDB"},
	{name: "AVG_WAIT_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average waiting time in EinsteinDB"},
	{name: "MAX_WAIT_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max waiting time in EinsteinDB"},
	{name: "AVG_BACKOFF_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average waiting time before retry"},
	{name: "MAX_BACKOFF_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max waiting time before retry"},
	{name: "AVG_TOTAL_KEYS", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average number of scanned keys"},
	{name: "MAX_TOTAL_KEYS", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max number of scanned keys"},
	{name: "AVG_PROCESSED_KEYS", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average number of processed keys"},
	{name: "MAX_PROCESSED_KEYS", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max number of processed keys"},
	{name: "AVG_PREWRITE_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average time of prewrite phase"},
	{name: "MAX_PREWRITE_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max time of prewrite phase"},
	{name: "AVG_COMMIT_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average time of commit phase"},
	{name: "MAX_COMMIT_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max time of commit phase"},
	{name: "AVG_GET_COMMIT_TS_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average time of getting commit_ts"},
	{name: "MAX_GET_COMMIT_TS_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max time of getting commit_ts"},
	{name: "AVG_COMMIT_BACKOFF_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average time before retry during commit phase"},
	{name: "MAX_COMMIT_BACKOFF_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max time before retry during commit phase"},
	{name: "AVG_RESOLVE_LOCK_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average time for resolving locks"},
	{name: "MAX_RESOLVE_LOCK_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max time for resolving locks"},
	{name: "AVG_LOCAL_LATCH_WAIT_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average waiting time of local transaction"},
	{name: "MAX_LOCAL_LATCH_WAIT_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max waiting time of local transaction"},
	{name: "AVG_WRITE_KEYS", tp: allegrosql.TypeDouble, size: 22, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average count of written keys"},
	{name: "MAX_WRITE_KEYS", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max count of written keys"},
	{name: "AVG_WRITE_SIZE", tp: allegrosql.TypeDouble, size: 22, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average amount of written bytes"},
	{name: "MAX_WRITE_SIZE", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max amount of written bytes"},
	{name: "AVG_PREWRITE_REGIONS", tp: allegrosql.TypeDouble, size: 22, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average number of involved regions in prewrite phase"},
	{name: "MAX_PREWRITE_REGIONS", tp: allegrosql.TypeLong, size: 11, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max number of involved regions in prewrite phase"},
	{name: "AVG_TXN_RETRY", tp: allegrosql.TypeDouble, size: 22, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average number of transaction retries"},
	{name: "MAX_TXN_RETRY", tp: allegrosql.TypeLong, size: 11, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max number of transaction retries"},
	{name: "SUM_EXEC_RETRY", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Sum number of execution retries in pessimistic transactions"},
	{name: "SUM_EXEC_RETRY_TIME", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Sum time of execution retries in pessimistic transactions"},
	{name: "SUM_BACKOFF_TIMES", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Sum of retries"},
	{name: "BACKOFF_TYPES", tp: allegrosql.TypeVarchar, size: 1024, comment: "Types of errors and the number of retries for each type"},
	{name: "AVG_MEM", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average memory(byte) used"},
	{name: "MAX_MEM", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max memory(byte) used"},
	{name: "AVG_DISK", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average disk space(byte) used"},
	{name: "MAX_DISK", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Max disk space(byte) used"},
	{name: "AVG_AFFECTED_ROWS", tp: allegrosql.TypeDouble, size: 22, flag: allegrosql.NotNullFlag | allegrosql.UnsignedFlag, comment: "Average number of rows affected"},
	{name: "FIRST_SEEN", tp: allegrosql.TypeTimestamp, size: 26, flag: allegrosql.NotNullFlag, comment: "The time these memexs are seen for the first time"},
	{name: "LAST_SEEN", tp: allegrosql.TypeTimestamp, size: 26, flag: allegrosql.NotNullFlag, comment: "The time these memexs are seen for the last time"},
	{name: "PLAN_IN_CACHE", tp: allegrosql.TypeTiny, size: 1, flag: allegrosql.NotNullFlag, comment: "Whether the last memex hit plan cache"},
	{name: "PLAN_CACHE_HITS", tp: allegrosql.TypeLonglong, size: 20, flag: allegrosql.NotNullFlag, comment: "The number of times these memexs hit plan cache"},
	{name: "QUERY_SAMPLE_TEXT", tp: allegrosql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled original memex"},
	{name: "PREV_SAMPLE_TEXT", tp: allegrosql.TypeBlob, size: types.UnspecifiedLength, comment: "The previous memex before commit"},
	{name: "PLAN_DIGEST", tp: allegrosql.TypeVarchar, size: 64, comment: "Digest of its execution plan"},
	{name: "PLAN", tp: allegrosql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled execution plan"},
}

var blockStorageStatsDefCauss = []defCausumnInfo{
	{name: "TABLE_SCHEMA", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: allegrosql.TypeLonglong, size: 21},
	{name: "PEER_COUNT", tp: allegrosql.TypeLonglong, size: 21},
	{name: "REGION_COUNT", tp: allegrosql.TypeLonglong, size: 21, comment: "The region count of single replica of the causet"},
	{name: "EMPTY_REGION_COUNT", tp: allegrosql.TypeLonglong, size: 21, comment: "The region count of single replica of the causet"},
	{name: "TABLE_SIZE", tp: allegrosql.TypeLonglong, size: 64, comment: "The disk usage(MB) of single replica of the causet, if the causet size is empty or less than 1MB, it would show 1MB "},
	{name: "TABLE_KEYS", tp: allegrosql.TypeLonglong, size: 64, comment: "The count of keys of single replica of the causet"},
}

var blockBlockTiFlashBlocksDefCauss = []defCausumnInfo{
	{name: "DATABASE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "MilevaDB_DATABASE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "MilevaDB_TABLE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: allegrosql.TypeLonglong, size: 64},
	{name: "IS_TOMBSTONE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "SEGMENT_COUNT", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TOTAL_ROWS", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TOTAL_SIZE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TOTAL_DELETE_RANGES", tp: allegrosql.TypeLonglong, size: 64},
	{name: "DELTA_RATE_ROWS", tp: allegrosql.TypeDouble, size: 64},
	{name: "DELTA_RATE_SEGMENTS", tp: allegrosql.TypeDouble, size: 64},
	{name: "DELTA_PLACED_RATE", tp: allegrosql.TypeDouble, size: 64},
	{name: "DELTA_CACHE_SIZE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "DELTA_CACHE_RATE", tp: allegrosql.TypeDouble, size: 64},
	{name: "DELTA_CACHE_WASTED_RATE", tp: allegrosql.TypeDouble, size: 64},
	{name: "DELTA_INDEX_SIZE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "AVG_SEGMENT_ROWS", tp: allegrosql.TypeDouble, size: 64},
	{name: "AVG_SEGMENT_SIZE", tp: allegrosql.TypeDouble, size: 64},
	{name: "DELTA_COUNT", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TOTAL_DELTA_ROWS", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TOTAL_DELTA_SIZE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "AVG_DELTA_ROWS", tp: allegrosql.TypeDouble, size: 64},
	{name: "AVG_DELTA_SIZE", tp: allegrosql.TypeDouble, size: 64},
	{name: "AVG_DELTA_DELETE_RANGES", tp: allegrosql.TypeDouble, size: 64},
	{name: "STABLE_COUNT", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TOTAL_STABLE_ROWS", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TOTAL_STABLE_SIZE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TOTAL_STABLE_SIZE_ON_DISK", tp: allegrosql.TypeLonglong, size: 64},
	{name: "AVG_STABLE_ROWS", tp: allegrosql.TypeDouble, size: 64},
	{name: "AVG_STABLE_SIZE", tp: allegrosql.TypeDouble, size: 64},
	{name: "TOTAL_PACK_COUNT_IN_DELTA", tp: allegrosql.TypeLonglong, size: 64},
	{name: "AVG_PACK_COUNT_IN_DELTA", tp: allegrosql.TypeDouble, size: 64},
	{name: "AVG_PACK_ROWS_IN_DELTA", tp: allegrosql.TypeDouble, size: 64},
	{name: "AVG_PACK_SIZE_IN_DELTA", tp: allegrosql.TypeDouble, size: 64},
	{name: "TOTAL_PACK_COUNT_IN_STABLE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "AVG_PACK_COUNT_IN_STABLE", tp: allegrosql.TypeDouble, size: 64},
	{name: "AVG_PACK_ROWS_IN_STABLE", tp: allegrosql.TypeDouble, size: 64},
	{name: "AVG_PACK_SIZE_IN_STABLE", tp: allegrosql.TypeDouble, size: 64},
	{name: "STORAGE_STABLE_NUM_SNAPSHOTS", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_STABLE_NUM_PAGES", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_STABLE_NUM_NORMAL_PAGES", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_STABLE_MAX_PAGE_ID", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_DELTA_NUM_SNAPSHOTS", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_DELTA_NUM_PAGES", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_DELTA_NUM_NORMAL_PAGES", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_DELTA_MAX_PAGE_ID", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_META_NUM_SNAPSHOTS", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_META_NUM_PAGES", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_META_NUM_NORMAL_PAGES", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STORAGE_META_MAX_PAGE_ID", tp: allegrosql.TypeLonglong, size: 64},
	{name: "BACKGROUND_TASKS_LENGTH", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TIFLASH_INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
}

var blockBlockTiFlashSegmentsDefCauss = []defCausumnInfo{
	{name: "DATABASE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "MilevaDB_DATABASE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "MilevaDB_TABLE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: allegrosql.TypeLonglong, size: 64},
	{name: "IS_TOMBSTONE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "SEGMENT_ID", tp: allegrosql.TypeLonglong, size: 64},
	{name: "RANGE", tp: allegrosql.TypeVarchar, size: 64},
	{name: "ROWS", tp: allegrosql.TypeLonglong, size: 64},
	{name: "SIZE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "DELETE_RANGES", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STABLE_SIZE_ON_DISK", tp: allegrosql.TypeLonglong, size: 64},
	{name: "DELTA_PACK_COUNT", tp: allegrosql.TypeLonglong, size: 64},
	{name: "STABLE_PACK_COUNT", tp: allegrosql.TypeLonglong, size: 64},
	{name: "AVG_DELTA_PACK_ROWS", tp: allegrosql.TypeDouble, size: 64},
	{name: "AVG_STABLE_PACK_ROWS", tp: allegrosql.TypeDouble, size: 64},
	{name: "DELTA_RATE", tp: allegrosql.TypeDouble, size: 64},
	{name: "DELTA_CACHE_SIZE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "DELTA_INDEX_SIZE", tp: allegrosql.TypeLonglong, size: 64},
	{name: "TIFLASH_INSTANCE", tp: allegrosql.TypeVarchar, size: 64},
}

// GetShardingInfo returns a nil or description string for the sharding information of given BlockInfo.
// The returned description string may be:
//  - "NOT_SHARDED": for blocks that SHARD_ROW_ID_BITS is not specified.
//  - "NOT_SHARDED(PK_IS_HANDLE)": for blocks of which primary key is event id.
//  - "PK_AUTO_RANDOM_BITS={bit_number}": for blocks of which primary key is sharded event id.
//  - "SHARD_BITS={bit_number}": for blocks that with SHARD_ROW_ID_BITS.
// The returned nil indicates that sharding information is not suiblock for the causet(for example, when the causet is a View).
// This function is exported for unit test.
func GetShardingInfo(dbInfo *perceptron.DBInfo, blockInfo *perceptron.BlockInfo) interface{} {
	if dbInfo == nil || blockInfo == nil || blockInfo.IsView() || soliton.IsMemOrSysDB(dbInfo.Name.L) {
		return nil
	}
	shardingInfo := "NOT_SHARDED"
	if blockInfo.PKIsHandle {
		if blockInfo.ContainsAutoRandomBits() {
			shardingInfo = "PK_AUTO_RANDOM_BITS=" + strconv.Itoa(int(blockInfo.AutoRandomBits))
		} else {
			shardingInfo = "NOT_SHARDED(PK_IS_HANDLE)"
		}
	} else if blockInfo.ShardEventIDBits > 0 {
		shardingInfo = "SHARD_BITS=" + strconv.Itoa(int(blockInfo.ShardEventIDBits))
	}
	return shardingInfo
}

const (
	// PrimaryKeyType is the string constant of PRIMARY KEY.
	PrimaryKeyType = "PRIMARY KEY"
	// PrimaryConstraint is the string constant of PRIMARY.
	PrimaryConstraint = "PRIMARY"
	// UniqueKeyType is the string constant of UNIQUE.
	UniqueKeyType = "UNIQUE"
)

// ServerInfo represents the basic server information of single cluster component
type ServerInfo struct {
	ServerType     string
	Address        string
	StatusAddr     string
	Version        string
	GitHash        string
	StartTimestamp int64
}

// GetClusterServerInfo returns all components information of cluster
func GetClusterServerInfo(ctx stochastikctx.Context) ([]ServerInfo, error) {
	failpoint.Inject("mockClusterInfo", func(val failpoint.Value) {
		// The cluster topology is injected by `failpoint` memex and
		// there is no extra checks for it. (let the test fail if the memex invalid)
		if s := val.(string); len(s) > 0 {
			var servers []ServerInfo
			for _, server := range strings.Split(s, ";") {
				parts := strings.Split(server, ",")
				servers = append(servers, ServerInfo{
					ServerType: parts[0],
					Address:    parts[1],
					StatusAddr: parts[2],
					Version:    parts[3],
					GitHash:    parts[4],
				})
			}
			failpoint.Return(servers, nil)
		}
	})

	type retriever func(ctx stochastikctx.Context) ([]ServerInfo, error)
	var servers []ServerInfo
	for _, r := range []retriever{GetMilevaDBServerInfo, GetFIDelServerInfo, GetStoreServerInfo} {
		nodes, err := r(ctx)
		if err != nil {
			return nil, err
		}
		servers = append(servers, nodes...)
	}
	return servers, nil
}

// GetMilevaDBServerInfo returns all MilevaDB nodes information of cluster
func GetMilevaDBServerInfo(ctx stochastikctx.Context) ([]ServerInfo, error) {
	// Get MilevaDB servers info.
	milevadbNodes, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}
	var servers []ServerInfo
	var isDefaultVersion bool
	if len(config.GetGlobalConfig().ServerVersion) == 0 {
		isDefaultVersion = true
	}
	for _, node := range milevadbNodes {
		servers = append(servers, ServerInfo{
			ServerType:     "milevadb",
			Address:        fmt.Sprintf("%s:%d", node.IP, node.Port),
			StatusAddr:     fmt.Sprintf("%s:%d", node.IP, node.StatusPort),
			Version:        FormatVersion(node.Version, isDefaultVersion),
			GitHash:        node.GitHash,
			StartTimestamp: node.StartTimestamp,
		})
	}
	return servers, nil
}

// FormatVersion make MilevaDBVersion consistent to EinsteinDB and FIDel.
// The default MilevaDBVersion is 5.7.25-MilevaDB-${MilevaDBReleaseVersion}.
func FormatVersion(MilevaDBVersion string, isDefaultVersion bool) string {
	var version, nodeVersion string

	// The user hasn't set the config 'ServerVersion'.
	if isDefaultVersion {
		nodeVersion = MilevaDBVersion[strings.LastIndex(MilevaDBVersion, "MilevaDB-")+len("MilevaDB-"):]
		if nodeVersion[0] == 'v' {
			nodeVersion = nodeVersion[1:]
		}
		nodeVersions := strings.Split(nodeVersion, "-")
		if len(nodeVersions) == 1 {
			version = nodeVersions[0]
		} else if len(nodeVersions) >= 2 {
			version = fmt.Sprintf("%s-%s", nodeVersions[0], nodeVersions[1])
		}
	} else { // The user has already set the config 'ServerVersion',it would be a complex scene, so just use the 'ServerVersion' as version.
		version = MilevaDBVersion
	}

	return version
}

// GetFIDelServerInfo returns all FIDel nodes information of cluster
func GetFIDelServerInfo(ctx stochastikctx.Context) ([]ServerInfo, error) {
	// Get FIDel servers info.
	causetstore := ctx.GetStore()
	etcd, ok := causetstore.(einsteindb.EtcdBackend)
	if !ok {
		return nil, errors.Errorf("%T not an etcd backend", causetstore)
	}
	var servers []ServerInfo
	members, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, addr := range members {
		// Get FIDel version
		url := fmt.Sprintf("%s://%s%s", soliton.InternalHTTPSchema(), addr, FIDelapi.ClusterVersion)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		req.Header.Add("FIDel-Allow-follower-handle", "true")
		resp, err := soliton.InternalHTTPClient().Do(req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		FIDelVersion, err := ioutil.ReadAll(resp.Body)
		terror.Log(resp.Body.Close())
		if err != nil {
			return nil, errors.Trace(err)
		}
		version := strings.Trim(strings.Trim(string(FIDelVersion), "\n"), "\"")

		// Get FIDel git_hash
		url = fmt.Sprintf("%s://%s%s", soliton.InternalHTTPSchema(), addr, FIDelapi.Status)
		req, err = http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		req.Header.Add("FIDel-Allow-follower-handle", "true")
		resp, err = soliton.InternalHTTPClient().Do(req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var content = struct {
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{}
		err = json.NewCausetDecoder(resp.Body).Decode(&content)
		terror.Log(resp.Body.Close())
		if err != nil {
			return nil, errors.Trace(err)
		}

		servers = append(servers, ServerInfo{
			ServerType:     "fidel",
			Address:        addr,
			StatusAddr:     addr,
			Version:        version,
			GitHash:        content.GitHash,
			StartTimestamp: content.StartTimestamp,
		})
	}
	return servers, nil
}

const tiflashLabel = "tiflash"

// GetStoreServerInfo returns all causetstore nodes(EinsteinDB or TiFlash) cluster information
func GetStoreServerInfo(ctx stochastikctx.Context) ([]ServerInfo, error) {
	isTiFlashStore := func(causetstore *spacetimepb.CausetStore) bool {
		isTiFlash := false
		for _, label := range causetstore.Labels {
			if label.GetKey() == "engine" && label.GetValue() == tiflashLabel {
				isTiFlash = true
			}
		}
		return isTiFlash
	}

	causetstore := ctx.GetStore()
	// Get EinsteinDB servers info.
	einsteindbStore, ok := causetstore.(einsteindb.CausetStorage)
	if !ok {
		return nil, errors.Errorf("%T is not an EinsteinDB or TiFlash causetstore instance", causetstore)
	}
	FIDelClient := einsteindbStore.GetRegionCache().FIDelClient()
	if FIDelClient == nil {
		return nil, errors.New("fidel unavailable")
	}
	stores, err := FIDelClient.GetAllStores(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}
	var servers []ServerInfo
	for _, causetstore := range stores {
		failpoint.Inject("mockStoreTombstone", func(val failpoint.Value) {
			if val.(bool) {
				causetstore.State = spacetimepb.StoreState_Tombstone
			}
		})

		if causetstore.GetState() == spacetimepb.StoreState_Tombstone {
			continue
		}
		var tp string
		if isTiFlashStore(causetstore) {
			tp = tiflashLabel
		} else {
			tp = einsteindb.GetStoreTypeByMeta(causetstore).Name()
		}
		servers = append(servers, ServerInfo{
			ServerType:     tp,
			Address:        causetstore.Address,
			StatusAddr:     causetstore.StatusAddress,
			Version:        causetstore.Version,
			GitHash:        causetstore.GitHash,
			StartTimestamp: causetstore.StartTimestamp,
		})
	}
	return servers, nil
}

// GetTiFlashStoreCount returns the count of tiflash server.
func GetTiFlashStoreCount(ctx stochastikctx.Context) (cnt uint64, err error) {
	failpoint.Inject("mockTiFlashStoreCount", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(uint64(10), nil)
		}
	})

	stores, err := GetStoreServerInfo(ctx)
	if err != nil {
		return cnt, err
	}
	for _, causetstore := range stores {
		if causetstore.ServerType == tiflashLabel {
			cnt++
		}
	}
	return cnt, nil
}

var blockNameToDeferredCausets = map[string][]defCausumnInfo{
	BlockSchemata:                 schemataDefCauss,
	BlockBlocks:                   blocksDefCauss,
	BlockDeferredCausets:          defCausumnsDefCauss,
	blockDeferredCausetStatistics: defCausumnStatisticsDefCauss,
	BlockStatistics:               statisticsDefCauss,
	BlockCharacterSets:            charsetDefCauss,
	BlockDefCauslations:           defCauslationsDefCauss,
	blockFiles:                    filesDefCauss,
	BlockProfiling:                profilingDefCauss,
	BlockPartitions:               partitionsDefCauss,
	BlockKeyDeferredCauset:        keyDeferredCausetUsageDefCauss,
	blockReferConst:               referConstDefCauss,
	BlockStochastikVar:            stochastikVarDefCauss,
	blockPlugins:                  pluginsDefCauss,
	BlockConstraints:              blockConstraintsDefCauss,
	blockTriggers:                 blockTriggersDefCauss,
	BlockUserPrivileges:           blockUserPrivilegesDefCauss,
	blockSchemaPrivileges:         blockSchemaPrivilegesDefCauss,
	blockBlockPrivileges:          blockBlockPrivilegesDefCauss,
	blockDeferredCausetPrivileges: blockDeferredCausetPrivilegesDefCauss,
	BlockEngines:                  blockEnginesDefCauss,
	BlockViews:                    blockViewsDefCauss,
	blockRoutines:                 blockRoutinesDefCauss,
	blockParameters:               blockParametersDefCauss,
	blockEvents:                   blockEventsDefCauss,
	blockGlobalStatus:             blockGlobalStatusDefCauss,
	blockGlobalVariables:          blockGlobalVariablesDefCauss,
	blockStochastikStatus:         blockStochastikStatusDefCauss,
	blockOptimizerTrace:           blockOptimizerTraceDefCauss,
	blockBlockSpaces:              blockBlockSpacesDefCauss,
	BlockDefCauslationCharacterSetApplicability: blockDefCauslationCharacterSetApplicabilityDefCauss,
	BlockProcesslist:              blockProcesslistDefCauss,
	BlockMilevaDBIndexes:          blockMilevaDBIndexesDefCauss,
	BlockSlowQuery:                slowQueryDefCauss,
	BlockMilevaDBHotRegions:       BlockMilevaDBHotRegionsDefCauss,
	BlockEinsteinDBStoreStatus:    BlockEinsteinDBStoreStatusDefCauss,
	BlockAnalyzeStatus:            blockAnalyzeStatusDefCauss,
	BlockEinsteinDBRegionStatus:   BlockEinsteinDBRegionStatusDefCauss,
	BlockEinsteinDBRegionPeers:    BlockEinsteinDBRegionPeersDefCauss,
	BlockMilevaDBServersInfo:      blockMilevaDBServersInfoDefCauss,
	BlockClusterInfo:              blockClusterInfoDefCauss,
	BlockClusterConfig:            blockClusterConfigDefCauss,
	BlockClusterLog:               blockClusterLogDefCauss,
	BlockClusterLoad:              blockClusterLoadDefCauss,
	BlockTiFlashReplica:           blockBlockTiFlashReplicaDefCauss,
	BlockClusterHardware:          blockClusterHardwareDefCauss,
	BlockClusterSystemInfo:        blockClusterSystemInfoDefCauss,
	BlockInspectionResult:         blockInspectionResultDefCauss,
	BlockMetricSummary:            blockMetricSummaryDefCauss,
	BlockMetricSummaryByLabel:     blockMetricSummaryByLabelDefCauss,
	BlockMetricBlocks:             blockMetricBlocksDefCauss,
	BlockInspectionSummary:        blockInspectionSummaryDefCauss,
	BlockInspectionMemrules:       blockInspectionMemrulesDefCauss,
	BlockDBSJobs:                  blockDBSJobsDefCauss,
	BlockSequences:                blockSequencesDefCauss,
	BlockStatementsSummary:        blockStatementsSummaryDefCauss,
	BlockStatementsSummaryHistory: blockStatementsSummaryDefCauss,
	BlockStorageStats:             blockStorageStatsDefCauss,
	BlockTiFlashBlocks:            blockBlockTiFlashBlocksDefCauss,
	BlockTiFlashSegments:          blockBlockTiFlashSegmentsDefCauss,
}

func createSchemaReplicantBlock(_ autoid.SlabPredictors, spacetime *perceptron.BlockInfo) (causet.Block, error) {
	defCausumns := make([]*causet.DeferredCauset, len(spacetime.DeferredCausets))
	for i, defCaus := range spacetime.DeferredCausets {
		defCausumns[i] = causet.ToDeferredCauset(defCaus)
	}
	tp := causet.VirtualBlock
	if isClusterBlockByName(soliton.InformationSchemaName.O, spacetime.Name.O) {
		tp = causet.ClusterBlock
	}
	return &schemareplicantBlock{spacetime: spacetime, defcaus: defCausumns, tp: tp}, nil
}

type schemareplicantBlock struct {
	spacetime *perceptron.BlockInfo
	defcaus   []*causet.DeferredCauset
	tp        causet.Type
}

// SchemasSorter implements the sort.Interface interface, sorts DBInfo by name.
type SchemasSorter []*perceptron.DBInfo

func (s SchemasSorter) Len() int {
	return len(s)
}

func (s SchemasSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SchemasSorter) Less(i, j int) bool {
	return s[i].Name.L < s[j].Name.L
}

func (it *schemareplicantBlock) getEvents(ctx stochastikctx.Context, defcaus []*causet.DeferredCauset) (fullEvents [][]types.Causet, err error) {
	is := GetSchemaReplicant(ctx)
	dbs := is.AllSchemas()
	sort.Sort(SchemasSorter(dbs))
	switch it.spacetime.Name.O {
	case blockFiles:
	case blockReferConst:
	case blockPlugins, blockTriggers:
	case blockRoutines:
	// TODO: Fill the following blocks.
	case blockSchemaPrivileges:
	case blockBlockPrivileges:
	case blockDeferredCausetPrivileges:
	case blockParameters:
	case blockEvents:
	case blockGlobalStatus:
	case blockGlobalVariables:
	case blockStochastikStatus:
	case blockOptimizerTrace:
	case blockBlockSpaces:
	}
	if err != nil {
		return nil, err
	}
	if len(defcaus) == len(it.defcaus) {
		return
	}
	rows := make([][]types.Causet, len(fullEvents))
	for i, fullEvent := range fullEvents {
		event := make([]types.Causet, len(defcaus))
		for j, defCaus := range defcaus {
			event[j] = fullEvent[defCaus.Offset]
		}
		rows[i] = event
	}
	return rows, nil
}

// IterRecords implements causet.Block IterRecords interface.
func (it *schemareplicantBlock) IterRecords(ctx stochastikctx.Context, startKey ekv.Key, defcaus []*causet.DeferredCauset,
	fn causet.RecordIterFunc) error {
	if len(startKey) != 0 {
		return causet.ErrUnsupportedOp
	}
	rows, err := it.getEvents(ctx, defcaus)
	if err != nil {
		return err
	}
	for i, event := range rows {
		more, err := fn(ekv.IntHandle(i), event, defcaus)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}

// EventWithDefCauss implements causet.Block EventWithDefCauss interface.
func (it *schemareplicantBlock) EventWithDefCauss(ctx stochastikctx.Context, h ekv.Handle, defcaus []*causet.DeferredCauset) ([]types.Causet, error) {
	return nil, causet.ErrUnsupportedOp
}

// Event implements causet.Block Event interface.
func (it *schemareplicantBlock) Event(ctx stochastikctx.Context, h ekv.Handle) ([]types.Causet, error) {
	return nil, causet.ErrUnsupportedOp
}

// DefCauss implements causet.Block DefCauss interface.
func (it *schemareplicantBlock) DefCauss() []*causet.DeferredCauset {
	return it.defcaus
}

// VisibleDefCauss implements causet.Block VisibleDefCauss interface.
func (it *schemareplicantBlock) VisibleDefCauss() []*causet.DeferredCauset {
	return it.defcaus
}

// HiddenDefCauss implements causet.Block HiddenDefCauss interface.
func (it *schemareplicantBlock) HiddenDefCauss() []*causet.DeferredCauset {
	return nil
}

// WriblockDefCauss implements causet.Block WriblockDefCauss interface.
func (it *schemareplicantBlock) WriblockDefCauss() []*causet.DeferredCauset {
	return it.defcaus
}

// FullHiddenDefCaussAndVisibleDefCauss implements causet FullHiddenDefCaussAndVisibleDefCauss interface.
func (it *schemareplicantBlock) FullHiddenDefCaussAndVisibleDefCauss() []*causet.DeferredCauset {
	return it.defcaus
}

// Indices implements causet.Block Indices interface.
func (it *schemareplicantBlock) Indices() []causet.Index {
	return nil
}

// WriblockIndices implements causet.Block WriblockIndices interface.
func (it *schemareplicantBlock) WriblockIndices() []causet.Index {
	return nil
}

// DeleblockIndices implements causet.Block DeleblockIndices interface.
func (it *schemareplicantBlock) DeleblockIndices() []causet.Index {
	return nil
}

// RecordPrefix implements causet.Block RecordPrefix interface.
func (it *schemareplicantBlock) RecordPrefix() ekv.Key {
	return nil
}

// IndexPrefix implements causet.Block IndexPrefix interface.
func (it *schemareplicantBlock) IndexPrefix() ekv.Key {
	return nil
}

// FirstKey implements causet.Block FirstKey interface.
func (it *schemareplicantBlock) FirstKey() ekv.Key {
	return nil
}

// RecordKey implements causet.Block RecordKey interface.
func (it *schemareplicantBlock) RecordKey(h ekv.Handle) ekv.Key {
	return nil
}

// AddRecord implements causet.Block AddRecord interface.
func (it *schemareplicantBlock) AddRecord(ctx stochastikctx.Context, r []types.Causet, opts ...causet.AddRecordOption) (recordID ekv.Handle, err error) {
	return nil, causet.ErrUnsupportedOp
}

// RemoveRecord implements causet.Block RemoveRecord interface.
func (it *schemareplicantBlock) RemoveRecord(ctx stochastikctx.Context, h ekv.Handle, r []types.Causet) error {
	return causet.ErrUnsupportedOp
}

// UFIDelateRecord implements causet.Block UFIDelateRecord interface.
func (it *schemareplicantBlock) UFIDelateRecord(gctx context.Context, ctx stochastikctx.Context, h ekv.Handle, oldData, newData []types.Causet, touched []bool) error {
	return causet.ErrUnsupportedOp
}

// SlabPredictors implements causet.Block SlabPredictors interface.
func (it *schemareplicantBlock) SlabPredictors(_ stochastikctx.Context) autoid.SlabPredictors {
	return nil
}

// RebaseAutoID implements causet.Block RebaseAutoID interface.
func (it *schemareplicantBlock) RebaseAutoID(ctx stochastikctx.Context, newBase int64, isSetStep bool, tp autoid.SlabPredictorType) error {
	return causet.ErrUnsupportedOp
}

// Meta implements causet.Block Meta interface.
func (it *schemareplicantBlock) Meta() *perceptron.BlockInfo {
	return it.spacetime
}

// GetPhysicalID implements causet.Block GetPhysicalID interface.
func (it *schemareplicantBlock) GetPhysicalID() int64 {
	return it.spacetime.ID
}

// Seek implements causet.Block Seek interface.
func (it *schemareplicantBlock) Seek(ctx stochastikctx.Context, h ekv.Handle) (ekv.Handle, bool, error) {
	return nil, false, causet.ErrUnsupportedOp
}

// Type implements causet.Block Type interface.
func (it *schemareplicantBlock) Type() causet.Type {
	return it.tp
}

// VirtualBlock is a dummy causet.Block implementation.
type VirtualBlock struct{}

// IterRecords implements causet.Block IterRecords interface.
func (vt *VirtualBlock) IterRecords(ctx stochastikctx.Context, startKey ekv.Key, defcaus []*causet.DeferredCauset,
	_ causet.RecordIterFunc) error {
	if len(startKey) != 0 {
		return causet.ErrUnsupportedOp
	}
	return nil
}

// EventWithDefCauss implements causet.Block EventWithDefCauss interface.
func (vt *VirtualBlock) EventWithDefCauss(ctx stochastikctx.Context, h ekv.Handle, defcaus []*causet.DeferredCauset) ([]types.Causet, error) {
	return nil, causet.ErrUnsupportedOp
}

// Event implements causet.Block Event interface.
func (vt *VirtualBlock) Event(ctx stochastikctx.Context, h ekv.Handle) ([]types.Causet, error) {
	return nil, causet.ErrUnsupportedOp
}

// DefCauss implements causet.Block DefCauss interface.
func (vt *VirtualBlock) DefCauss() []*causet.DeferredCauset {
	return nil
}

// VisibleDefCauss implements causet.Block VisibleDefCauss interface.
func (vt *VirtualBlock) VisibleDefCauss() []*causet.DeferredCauset {
	return nil
}

// HiddenDefCauss implements causet.Block HiddenDefCauss interface.
func (vt *VirtualBlock) HiddenDefCauss() []*causet.DeferredCauset {
	return nil
}

// WriblockDefCauss implements causet.Block WriblockDefCauss interface.
func (vt *VirtualBlock) WriblockDefCauss() []*causet.DeferredCauset {
	return nil
}

// FullHiddenDefCaussAndVisibleDefCauss implements causet FullHiddenDefCaussAndVisibleDefCauss interface.
func (vt *VirtualBlock) FullHiddenDefCaussAndVisibleDefCauss() []*causet.DeferredCauset {
	return nil
}

// Indices implements causet.Block Indices interface.
func (vt *VirtualBlock) Indices() []causet.Index {
	return nil
}

// WriblockIndices implements causet.Block WriblockIndices interface.
func (vt *VirtualBlock) WriblockIndices() []causet.Index {
	return nil
}

// DeleblockIndices implements causet.Block DeleblockIndices interface.
func (vt *VirtualBlock) DeleblockIndices() []causet.Index {
	return nil
}

// RecordPrefix implements causet.Block RecordPrefix interface.
func (vt *VirtualBlock) RecordPrefix() ekv.Key {
	return nil
}

// IndexPrefix implements causet.Block IndexPrefix interface.
func (vt *VirtualBlock) IndexPrefix() ekv.Key {
	return nil
}

// FirstKey implements causet.Block FirstKey interface.
func (vt *VirtualBlock) FirstKey() ekv.Key {
	return nil
}

// RecordKey implements causet.Block RecordKey interface.
func (vt *VirtualBlock) RecordKey(h ekv.Handle) ekv.Key {
	return nil
}

// AddRecord implements causet.Block AddRecord interface.
func (vt *VirtualBlock) AddRecord(ctx stochastikctx.Context, r []types.Causet, opts ...causet.AddRecordOption) (recordID ekv.Handle, err error) {
	return nil, causet.ErrUnsupportedOp
}

// RemoveRecord implements causet.Block RemoveRecord interface.
func (vt *VirtualBlock) RemoveRecord(ctx stochastikctx.Context, h ekv.Handle, r []types.Causet) error {
	return causet.ErrUnsupportedOp
}

// UFIDelateRecord implements causet.Block UFIDelateRecord interface.
func (vt *VirtualBlock) UFIDelateRecord(ctx context.Context, sctx stochastikctx.Context, h ekv.Handle, oldData, newData []types.Causet, touched []bool) error {
	return causet.ErrUnsupportedOp
}

// SlabPredictors implements causet.Block SlabPredictors interface.
func (vt *VirtualBlock) SlabPredictors(_ stochastikctx.Context) autoid.SlabPredictors {
	return nil
}

// RebaseAutoID implements causet.Block RebaseAutoID interface.
func (vt *VirtualBlock) RebaseAutoID(ctx stochastikctx.Context, newBase int64, isSetStep bool, tp autoid.SlabPredictorType) error {
	return causet.ErrUnsupportedOp
}

// Meta implements causet.Block Meta interface.
func (vt *VirtualBlock) Meta() *perceptron.BlockInfo {
	return nil
}

// GetPhysicalID implements causet.Block GetPhysicalID interface.
func (vt *VirtualBlock) GetPhysicalID() int64 {
	return 0
}

// Seek implements causet.Block Seek interface.
func (vt *VirtualBlock) Seek(ctx stochastikctx.Context, h ekv.Handle) (ekv.Handle, bool, error) {
	return nil, false, causet.ErrUnsupportedOp
}

// Type implements causet.Block Type interface.
func (vt *VirtualBlock) Type() causet.Type {
	return causet.VirtualBlock
}
