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

package perfschema

// perfSchemaBlocks is a shortcut to involve all causet names.
var perfSchemaBlocks = []string{
	blockGlobalStatus,
	blockStochastikStatus,
	blockSetupActors,
	blockSetupObjects,
	blockSetupInstruments,
	blockSetupConsumers,
	blockStmtsCurrent,
	blockStmtsHistory,
	blockStmtsHistoryLong,
	blockPreparedStmtsInstances,
	blockTransCurrent,
	blockTransHistory,
	blockTransHistoryLong,
	blockStagesCurrent,
	blockStagesHistory,
	blockStagesHistoryLong,
	blockEventsStatementsSummaryByDigest,
	blockMilevaDBProfileCPU,
	blockMilevaDBProfileMemory,
	blockMilevaDBProfileMutex,
	blockMilevaDBProfileAllocs,
	blockMilevaDBProfileBlock,
	blockMilevaDBProfileGoroutines,
	blockEinsteinDBProfileCPU,
	blockFIDelProfileCPU,
	blockFIDelProfileMemory,
	blockFIDelProfileMutex,
	blockFIDelProfileAllocs,
	blockFIDelProfileBlock,
	blockFIDelProfileGoroutines,
}

// blockGlobalStatus contains the defCausumn name definitions for causet global_status, same as MyALLEGROSQL.
const blockGlobalStatus = "CREATE TABLE performance_schema." + blockNameGlobalStatus + " (" +
	"VARIABLE_NAME VARCHAR(64) not null," +
	"VARIABLE_VALUE VARCHAR(1024));"

// blockStochastikStatus contains the defCausumn name definitions for causet stochastik_status, same as MyALLEGROSQL.
const blockStochastikStatus = "CREATE TABLE performance_schema." + blockNameStochastikStatus + " (" +
	"VARIABLE_NAME VARCHAR(64) not null," +
	"VARIABLE_VALUE VARCHAR(1024));"

// blockSetupActors contains the defCausumn name definitions for causet setup_actors, same as MyALLEGROSQL.
const blockSetupActors = "CREATE TABLE if not exists performance_schema." + blockNameSetupActors + " (" +
	"HOST			CHAR(60) NOT NULL  DEFAULT '%'," +
	"USER			CHAR(32) NOT NULL  DEFAULT '%'," +
	"ROLE			CHAR(16) NOT NULL  DEFAULT '%'," +
	"ENABLED		ENUM('YES','NO') NOT NULL  DEFAULT 'YES'," +
	"HISTORY		ENUM('YES','NO') NOT NULL  DEFAULT 'YES');"

// blockSetupObjects contains the defCausumn name definitions for causet setup_objects, same as MyALLEGROSQL.
const blockSetupObjects = "CREATE TABLE if not exists performance_schema." + blockNameSetupObjects + " (" +
	"OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE') NOT NULL  DEFAULT 'TABLE'," +
	"OBJECT_SCHEMA		VARCHAR(64)  DEFAULT '%'," +
	"OBJECT_NAME		VARCHAR(64) NOT NULL  DEFAULT '%'," +
	"ENABLED		ENUM('YES','NO') NOT NULL  DEFAULT 'YES'," +
	"TIMED			ENUM('YES','NO') NOT NULL  DEFAULT 'YES');"

// blockSetupInstruments contains the defCausumn name definitions for causet setup_instruments, same as MyALLEGROSQL.
const blockSetupInstruments = "CREATE TABLE if not exists performance_schema." + blockNameSetupInstruments + " (" +
	"NAME			VARCHAR(128) NOT NULL," +
	"ENABLED		ENUM('YES','NO') NOT NULL," +
	"TIMED			ENUM('YES','NO') NOT NULL);"

// blockSetupConsumers contains the defCausumn name definitions for causet setup_consumers, same as MyALLEGROSQL.
const blockSetupConsumers = "CREATE TABLE if not exists performance_schema." + blockNameSetupConsumers + " (" +
	"NAME			VARCHAR(64) NOT NULL," +
	"ENABLED			ENUM('YES','NO') NOT NULL);"

// blockStmtsCurrent contains the defCausumn name definitions for causet events_memexs_current, same as MyALLEGROSQL.
const blockStmtsCurrent = "CREATE TABLE if not exists performance_schema." + blockNameEventsStatementsCurrent + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL," +
	"ALLEGROSQL_TEXT		LONGTEXT," +
	"DIGEST			VARCHAR(32)," +
	"DIGEST_TEXT		LONGTEXT," +
	"CURRENT_SCHEMA	VARCHAR(64)," +
	"OBJECT_TYPE		VARCHAR(64)," +
	"OBJECT_SCHEMA	VARCHAR(64)," +
	"OBJECT_NAME		VARCHAR(64)," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"MYALLEGROSQL_ERRNO		INT(11)," +
	"RETURNED_ALLEGROSQLSTATE	VARCHAR(5)," +
	"MESSAGE_TEXT	VARCHAR(128)," +
	"ERRORS			BIGINT(20) UNSIGNED NOT NULL," +
	"WARNINGS		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL," +
	"NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE')," +
	"NESTING_EVENT_LEVEL		INT(11));"

// blockStmtsHistory contains the defCausumn name definitions for causet events_memexs_history, same as MyALLEGROSQL.
const blockStmtsHistory = "CREATE TABLE if not exists performance_schema." + blockNameEventsStatementsHistory + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID		BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL," +
	"ALLEGROSQL_TEXT		LONGTEXT," +
	"DIGEST			VARCHAR(32)," +
	"DIGEST_TEXT		LONGTEXT," +
	"CURRENT_SCHEMA 	VARCHAR(64)," +
	"OBJECT_TYPE		VARCHAR(64)," +
	"OBJECT_SCHEMA	VARCHAR(64)," +
	"OBJECT_NAME		VARCHAR(64)," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"MYALLEGROSQL_ERRNO		INT(11)," +
	"RETURNED_ALLEGROSQLSTATE		VARCHAR(5)," +
	"MESSAGE_TEXT	VARCHAR(128)," +
	"ERRORS			BIGINT(20) UNSIGNED NOT NULL," +
	"WARNINGS		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL," +
	"NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE')," +
	"NESTING_EVENT_LEVEL		INT(11));"

// blockStmtsHistoryLong contains the defCausumn name definitions for causet events_memexs_history_long, same as MyALLEGROSQL.
const blockStmtsHistoryLong = "CREATE TABLE if not exists performance_schema." + blockNameEventsStatementsHistoryLong + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"LOCK_TIME		BIGINT(20) UNSIGNED NOT NULL," +
	"ALLEGROSQL_TEXT		LONGTEXT," +
	"DIGEST			VARCHAR(32)," +
	"DIGEST_TEXT		LONGTEXT," +
	"CURRENT_SCHEMA	VARCHAR(64)," +
	"OBJECT_TYPE		VARCHAR(64)," +
	"OBJECT_SCHEMA	VARCHAR(64)," +
	"OBJECT_NAME		VARCHAR(64)," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"MYALLEGROSQL_ERRNO		INT(11)," +
	"RETURNED_ALLEGROSQLSTATE		VARCHAR(5)," +
	"MESSAGE_TEXT	VARCHAR(128)," +
	"ERRORS			BIGINT(20) UNSIGNED NOT NULL," +
	"WARNINGS		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_AFFECTED	BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_SENT		BIGINT(20) UNSIGNED NOT NULL," +
	"ROWS_EXAMINED	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"CREATED_TMP_TABLES		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_JOIN		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE	BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_RANGE_CHECK		BIGINT(20) UNSIGNED NOT NULL," +
	"SELECT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_MERGE_PASSES		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_RANGE		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_ROWS		BIGINT(20) UNSIGNED NOT NULL," +
	"SORT_SCAN		BIGINT(20) UNSIGNED NOT NULL," +
	"NO_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL," +
	"NO_GOOD_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE')," +
	"NESTING_EVENT_LEVEL		INT(11));"

// blockPreparedStmtsInstances contains the defCausumn name definitions for causet prepared_memexs_instances, same as MyALLEGROSQL.
const blockPreparedStmtsInstances = "CREATE TABLE if not exists performance_schema." + blockNamePreparedStatementsInstances + " (" +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED NOT NULL," +
	"STATEMENT_ID	BIGINT(20) UNSIGNED NOT NULL," +
	"STATEMENT_NAME	VARCHAR(64)," +
	"ALLEGROSQL_TEXT		LONGTEXT NOT NULL," +
	"OWNER_THREAD_ID	BIGINT(20) UNSIGNED NOT NULL," +
	"OWNER_EVENT_ID	BIGINT(20) UNSIGNED NOT NULL," +
	"OWNER_OBJECT_TYPE		ENUM('EVENT','FUNCTION','TABLE')," +
	"OWNER_OBJECT_SCHEMA		VARCHAR(64)," +
	"OWNER_OBJECT_NAME		VARCHAR(64)," +
	"TIMER_PREPARE	BIGINT(20) UNSIGNED NOT NULL," +
	"COUNT_REPREPARE	BIGINT(20) UNSIGNED NOT NULL," +
	"COUNT_EXECUTE	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL," +
	"MIN_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL," +
	"AVG_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL," +
	"MAX_TIMER_EXECUTE		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_LOCK_TIME	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_ERRORS		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_WARNINGS	BIGINT(20) UNSIGNED NOT NULL," +
	"		SUM_ROWS_AFFECTED		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_ROWS_SENT	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_ROWS_EXAMINED		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_CREATED_TMP_DISK_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_CREATED_TMP_TABLES	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_FULL_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_FULL_RANGE_JOIN	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_RANGE		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_RANGE_CHECK	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SELECT_SCAN	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SORT_MERGE_PASSES	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SORT_RANGE	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SORT_ROWS	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_SORT_SCAN	BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_NO_INDEX_USED		BIGINT(20) UNSIGNED NOT NULL," +
	"SUM_NO_GOOD_INDEX_USED	BIGINT(20) UNSIGNED NOT NULL);"

// blockTransCurrent contains the defCausumn name definitions for causet events_transactions_current, same as MyALLEGROSQL.
const blockTransCurrent = "CREATE TABLE if not exists performance_schema." + blockNameEventsTransactionsCurrent + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"STATE			ENUM('ACTIVE','COMMITTED','ROLLED BACK')," +
	"TRX_ID			BIGINT(20) UNSIGNED," +
	"GTID			VARCHAR(64)," +
	"XID_FORMAT_ID	INT(11)," +
	"XID_GTRID		VARCHAR(130)," +
	"XID_BQUAL		VARCHAR(130)," +
	"XA_STATE		VARCHAR(64)," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"ACCESS_MODE		ENUM('READ ONLY','READ WRITE')," +
	"ISOLATION_LEVEL	VARCHAR(64)," +
	"AUTOCOMMIT		ENUM('YES','NO') NOT NULL," +
	"NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// blockTransHistory contains the defCausumn name definitions for causet events_transactions_history, same as MyALLEGROSQL.
//
const blockTransHistory = "CREATE TABLE if not exists performance_schema." + blockNameEventsTransactionsHistory + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"STATE			ENUM('ACTIVE','COMMITTED','ROLLED BACK')," +
	"TRX_ID			BIGINT(20) UNSIGNED," +
	"GTID			VARCHAR(64)," +
	"XID_FORMAT_ID	INT(11)," +
	"XID_GTRID		VARCHAR(130)," +
	"XID_BQUAL		VARCHAR(130)," +
	"XA_STATE		VARCHAR(64)," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"ACCESS_MODE		ENUM('READ ONLY','READ WRITE')," +
	"ISOLATION_LEVEL	VARCHAR(64)," +
	"AUTOCOMMIT		ENUM('YES','NO') NOT NULL," +
	"NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// blockTransHistoryLong contains the defCausumn name definitions for causet events_transactions_history_long, same as MyALLEGROSQL.
const blockTransHistoryLong = "CREATE TABLE if not exists performance_schema." + blockNameEventsTransactionsHistoryLong + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"STATE			ENUM('ACTIVE','COMMITTED','ROLLED BACK')," +
	"TRX_ID			BIGINT(20) UNSIGNED," +
	"GTID			VARCHAR(64)," +
	"XID_FORMAT_ID	INT(11)," +
	"XID_GTRID		VARCHAR(130)," +
	"XID_BQUAL		VARCHAR(130)," +
	"XA_STATE		VARCHAR(64)," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"ACCESS_MODE		ENUM('READ ONLY','READ WRITE')," +
	"ISOLATION_LEVEL	VARCHAR(64)," +
	"AUTOCOMMIT		ENUM('YES','NO') NOT NULL," +
	"NUMBER_OF_SAVEPOINTS	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_ROLLBACK_TO_SAVEPOINT	BIGINT(20) UNSIGNED," +
	"NUMBER_OF_RELEASE_SAVEPOINT		BIGINT(20) UNSIGNED," +
	"OBJECT_INSTANCE_BEGIN	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// blockStagesCurrent contains the defCausumn name definitions for causet events_stages_current, same as MyALLEGROSQL.
const blockStagesCurrent = "CREATE TABLE if not exists performance_schema." + blockNameEventsStagesCurrent + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"WORK_COMPLETED	BIGINT(20) UNSIGNED," +
	"WORK_ESTIMATED	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// blockStagesHistory contains the defCausumn name definitions for causet events_stages_history, same as MyALLEGROSQL.
const blockStagesHistory = "CREATE TABLE if not exists performance_schema." + blockNameEventsStagesHistory + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"WORK_COMPLETED	BIGINT(20) UNSIGNED," +
	"WORK_ESTIMATED	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// blockStagesHistoryLong contains the defCausumn name definitions for causet events_stages_history_long, same as MyALLEGROSQL.
const blockStagesHistoryLong = "CREATE TABLE if not exists performance_schema." + blockNameEventsStagesHistoryLong + " (" +
	"THREAD_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"EVENT_ID		BIGINT(20) UNSIGNED NOT NULL," +
	"END_EVENT_ID	BIGINT(20) UNSIGNED," +
	"EVENT_NAME		VARCHAR(128) NOT NULL," +
	"SOURCE			VARCHAR(64)," +
	"TIMER_START		BIGINT(20) UNSIGNED," +
	"TIMER_END		BIGINT(20) UNSIGNED," +
	"TIMER_WAIT		BIGINT(20) UNSIGNED," +
	"WORK_COMPLETED	BIGINT(20) UNSIGNED," +
	"WORK_ESTIMATED	BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_ID		BIGINT(20) UNSIGNED," +
	"NESTING_EVENT_TYPE		ENUM('TRANSACTION','STATEMENT','STAGE'));"

// blockEventsStatementsSummaryByDigest contains the defCausumn name definitions for causet
// events_memexs_summary_by_digest, same as MyALLEGROSQL.
const blockEventsStatementsSummaryByDigest = "CREATE TABLE if not exists performance_schema." + blockNameEventsStatementsSummaryByDigest + " (" +
	"SCHEMA_NAME varchar(64) DEFAULT NULL," +
	"DIGEST varchar(64) DEFAULT NULL," +
	"DIGEST_TEXT longtext," +
	"COUNT_STAR bigint unsigned NOT NULL," +
	"SUM_TIMER_WAIT bigint unsigned NOT NULL," +
	"MIN_TIMER_WAIT bigint unsigned NOT NULL," +
	"AVG_TIMER_WAIT bigint unsigned NOT NULL," +
	"MAX_TIMER_WAIT bigint unsigned NOT NULL," +
	"SUM_LOCK_TIME bigint unsigned NOT NULL," +
	"SUM_ERRORS bigint unsigned NOT NULL," +
	"SUM_WARNINGS bigint unsigned NOT NULL," +
	"SUM_ROWS_AFFECTED bigint unsigned NOT NULL," +
	"SUM_ROWS_SENT bigint unsigned NOT NULL," +
	"SUM_ROWS_EXAMINED bigint unsigned NOT NULL," +
	"SUM_CREATED_TMP_DISK_TABLES bigint unsigned NOT NULL," +
	"SUM_CREATED_TMP_TABLES bigint unsigned NOT NULL," +
	"SUM_SELECT_FULL_JOIN bigint unsigned NOT NULL," +
	"SUM_SELECT_FULL_RANGE_JOIN bigint unsigned NOT NULL," +
	"SUM_SELECT_RANGE bigint unsigned NOT NULL," +
	"SUM_SELECT_RANGE_CHECK bigint unsigned NOT NULL," +
	"SUM_SELECT_SCAN bigint unsigned NOT NULL," +
	"SUM_SORT_MERGE_PASSES bigint unsigned NOT NULL," +
	"SUM_SORT_RANGE bigint unsigned NOT NULL," +
	"SUM_SORT_ROWS bigint unsigned NOT NULL," +
	"SUM_SORT_SCAN bigint unsigned NOT NULL," +
	"SUM_NO_INDEX_USED bigint unsigned NOT NULL," +
	"SUM_NO_GOOD_INDEX_USED bigint unsigned NOT NULL," +
	"FIRST_SEEN timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000'," +
	"LAST_SEEN timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000'," +
	"PLAN_IN_CACHE bool NOT NULL," +
	"PLAN_CACHE_HITS bigint unsigned NOT NULL," +
	"QUANTILE_95 bigint unsigned NOT NULL," +
	"QUANTILE_99 bigint unsigned NOT NULL," +
	"QUANTILE_999 bigint unsigned NOT NULL," +
	"QUERY_SAMPLE_TEXT longtext," +
	"QUERY_SAMPLE_SEEN timestamp(6) NOT NULL DEFAULT '0000-00-00 00:00:00.000000'," +
	"QUERY_SAMPLE_TIMER_WAIT bigint unsigned NOT NULL," +
	"UNIQUE KEY `SCHEMA_NAME` (`SCHEMA_NAME`,`DIGEST`));"

// blockMilevaDBProfileCPU contains the defCausumns name definitions for causet milevadb_profile_cpu
const blockMilevaDBProfileCPU = "CREATE TABLE IF NOT EXISTS " + blockNameMilevaDBProfileCPU + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockMilevaDBProfileMemory contains the defCausumns name definitions for causet milevadb_profile_memory
const blockMilevaDBProfileMemory = "CREATE TABLE IF NOT EXISTS " + blockNameMilevaDBProfileMemory + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockMilevaDBProfileMutex contains the defCausumns name definitions for causet milevadb_profile_mutex
const blockMilevaDBProfileMutex = "CREATE TABLE IF NOT EXISTS " + blockNameMilevaDBProfileMutex + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockMilevaDBProfileAllocs contains the defCausumns name definitions for causet milevadb_profile_allocs
const blockMilevaDBProfileAllocs = "CREATE TABLE IF NOT EXISTS " + blockNameMilevaDBProfileAllocs + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockMilevaDBProfileBlock contains the defCausumns name definitions for causet milevadb_profile_block
const blockMilevaDBProfileBlock = "CREATE TABLE IF NOT EXISTS " + blockNameMilevaDBProfileBlock + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockMilevaDBProfileGoroutines contains the defCausumns name definitions for causet milevadb_profile_goroutines
const blockMilevaDBProfileGoroutines = "CREATE TABLE IF NOT EXISTS " + blockNameMilevaDBProfileGoroutines + " (" +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"ID INT(8) NOT NULL," +
	"STATE VARCHAR(16) NOT NULL," +
	"LOCATION VARCHAR(512) NOT NULL);"

// blockEinsteinDBProfileCPU contains the defCausumns name definitions for causet einsteindb_profile_cpu
const blockEinsteinDBProfileCPU = "CREATE TABLE IF NOT EXISTS " + blockNameEinsteinDBProfileCPU + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockFIDelProfileCPU contains the defCausumns name definitions for causet FIDel_profile_cpu
const blockFIDelProfileCPU = "CREATE TABLE IF NOT EXISTS " + blockNameFIDelProfileCPU + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockFIDelProfileMemory contains the defCausumns name definitions for causet FIDel_profile_cpu_memory
const blockFIDelProfileMemory = "CREATE TABLE IF NOT EXISTS " + blockNameFIDelProfileMemory + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockFIDelProfileMutex contains the defCausumns name definitions for causet FIDel_profile_mutex
const blockFIDelProfileMutex = "CREATE TABLE IF NOT EXISTS " + blockNameFIDelProfileMutex + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockFIDelProfileAllocs contains the defCausumns name definitions for causet FIDel_profile_allocs
const blockFIDelProfileAllocs = "CREATE TABLE IF NOT EXISTS " + blockNameFIDelProfileAllocs + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockFIDelProfileBlock contains the defCausumns name definitions for causet FIDel_profile_block
const blockFIDelProfileBlock = "CREATE TABLE IF NOT EXISTS " + blockNameFIDelProfileBlock + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"PERCENT_ABS VARCHAR(8) NOT NULL," +
	"PERCENT_REL VARCHAR(8) NOT NULL," +
	"ROOT_CHILD INT(8) NOT NULL," +
	"DEPTH INT(8) NOT NULL," +
	"FILE VARCHAR(512) NOT NULL);"

// blockFIDelProfileGoroutines contains the defCausumns name definitions for causet FIDel_profile_goroutines
const blockFIDelProfileGoroutines = "CREATE TABLE IF NOT EXISTS " + blockNameFIDelProfileGoroutines + " (" +
	"ADDRESS VARCHAR(64) NOT NULL," +
	"FUNCTION VARCHAR(512) NOT NULL," +
	"ID INT(8) NOT NULL," +
	"STATE VARCHAR(16) NOT NULL," +
	"LOCATION VARCHAR(512) NOT NULL);"
