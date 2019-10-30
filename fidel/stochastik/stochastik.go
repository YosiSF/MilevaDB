//2019 Venire Labs Inc All Rights Reserved

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

package stochastik

import (
	"context"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/opentracing/opentracing-go"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/query"
)

const(

	//SQL statement creates User table in system db.
	CreateUserBlock = `CREATE TABLE if not exists mysql.user (
		Host				CHAR(64),
		User				CHAR(32),
		Password			CHAR(41),
		Select_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Insert_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Update_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Delete_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Process_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Grant_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		References_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_db_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, User)

		// CreateDBPrivBlock is the SQL statement creates DB scope privilege block in system db.
	CreateDBPrivBlock = `CREATE TABLE if not exists mysql.db (
		Host			CHAR(60),
		DB			CHAR(64),
		User			CHAR(32),
		Select_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Insert_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Update_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Delete_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Create_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Drop_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Grant_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		References_priv 	ENUM('N','Y') Not Null DEFAULT 'N',
		Index_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Alter_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Create_tmp_table_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_blocks_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Event_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, DB, User));`
	// CreateBlockPrivBlock is the SQL statement creates block scope privilege table in system db.
	CreateBlockrivBlock = `CREATE TABLE if not exists mysql.blocks_priv (
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Block_name	CHAR(64),
		Grantor		CHAR(77),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Block_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References'),
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Block_name));`
	// CreateColumnPrivBlock is the SQL statement creates column scope privilege table in system db.
	CreateColumnPrivBlock = `CREATE TABLE if not exists mysql.columns_priv(
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Block_name	CHAR(64),
		Column_name	CHAR(64),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Block_name, Column_name));`
	// CreateGloablVariablesBlock is the SQL statement creates global variable table in system db.

	CreateGloablVariablesBlock = `CREATE TABLE if not exists mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null);
		CreateTiDBBlock = `CREATE TABLE if not exists mysql.tidb(
			VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
			VARIABLE_VALUE VARCHAR(1024) DEFAULT Null,
			COMMENT VARCHAR(1024));`
	
		// CreateHelpTopic is the SQL statement creates help_topic table in system db.
		// See: https://dev.mysql.com/doc/refman/5.5/en/system-database.html#system-database-help-blocks
		CreateHelpTopic = `CREATE TABLE if not exists mysql.help_topic (
			  help_topic_id int(10) unsigned NOT NULL,
			  name char(64) NOT NULL,
			  help_category_id smallint(5) unsigned NOT NULL,
			  description text NOT NULL,
			  example text NOT NULL,
			  url text NOT NULL,
			  PRIMARY KEY (help_topic_id),
			  UNIQUE KEY name (name)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';`
	
		// CreateStatsMetaBlock stores the meta of table statistics.
		CreateStatsMetaBlock = `CREATE TABLE if not exists mysql.stats_meta (
			version bigint(64) unsigned NOT NULL,
			table_id bigint(64) NOT NULL,
			modify_count bigint(64) NOT NULL DEFAULT 0,
			count bigint(64) unsigned NOT NULL DEFAULT 0,
			index idx_ver(version),
			unique index tbl(table_id)
		);`
	
		// CreateStatsColsBlock stores the statistics of table columns.
		CreateStatsColsBlock = `CREATE TABLE if not exists mysql.stats_histograms (
			table_id bigint(64) NOT NULL,
			is_index tinyint(2) NOT NULL,
			hist_id bigint(64) NOT NULL,
			distinct_count bigint(64) NOT NULL,
			null_count bigint(64) NOT NULL DEFAULT 0,
			tot_col_size bigint(64) NOT NULL DEFAULT 0,
			modify_count bigint(64) NOT NULL DEFAULT 0,
			version bigint(64) unsigned NOT NULL DEFAULT 0,
			cm_sketch blob,
			stats_ver bigint(64) NOT NULL DEFAULT 0,
			flag bigint(64) NOT NULL DEFAULT 0,
			correlation double NOT NULL DEFAULT 0,
			last_analyze_pos blob DEFAULT NULL,
			unique index tbl(table_id, is_index, hist_id)
		);`
	
		// CreateStatsBucketsBlock stores the histogram info for every table columns.
		CreateStatsBucketsBlock = `CREATE TABLE if not exists mysql.stats_buckets (
			table_id bigint(64) NOT NULL,
			is_index tinyint(2) NOT NULL,
			hist_id bigint(64) NOT NULL,
			bucket_id bigint(64) NOT NULL,
			count bigint(64) NOT NULL,
			repeats bigint(64) NOT NULL,
			upper_bound blob NOT NULL,
			lower_bound blob ,
			unique index tbl(table_id, is_index, hist_id, bucket_id)
		);`
	
		// CreateGCDeleteRangeBlock stores schemas which can be deleted by DeleteRange.
		CreateGCDeleteRangeBlock = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range (
			job_id BIGINT NOT NULL COMMENT "the DDL job ID",
			element_id BIGINT NOT NULL COMMENT "the schema element ID",
			start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
			end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
			ts BIGINT NOT NULL COMMENT "timestamp in uint64",
			UNIQUE KEY delete_range_index (job_id, element_id)
		);`
	
		// CreateGCDeleteRangeDoneBlock stores schemas which are already deleted by DeleteRange.
		CreateGCDeleteRangeDoneBlock = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range_done (
			job_id BIGINT NOT NULL COMMENT "the DDL job ID",
			element_id BIGINT NOT NULL COMMENT "the schema element ID",
			start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
			end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
			ts BIGINT NOT NULL COMMENT "timestamp in uint64",
			UNIQUE KEY delete_range_done_index (job_id, element_id)
		);`
	
		// CreateStatsFeedbackBlock stores the feedback info which is used to update stats.
		CreateStatsFeedbackBlock = `CREATE TABLE IF NOT EXISTS mysql.stats_feedback (
			table_id bigint(64) NOT NULL,
			is_index tinyint(2) NOT NULL,
			hist_id bigint(64) NOT NULL,
			feedback blob NOT NULL,
			index hist(table_id, is_index, hist_id)
		);`
	
		// CreateBindInfoBlock stores the sql bind info which is used to update globalBindCache.
		CreateBindInfoBlock = `CREATE TABLE IF NOT EXISTS mysql.bind_info (
			original_sql text NOT NULL  ,
			  bind_sql text NOT NULL ,
			  default_db text  NOT NULL,
			status text NOT NULL,
			create_time timestamp(3) NOT NULL,
			update_time timestamp(3) NOT NULL,
			charset text NOT NULL,
			collation text NOT NULL,
			INDEX sql_index(original_sql(1024),default_db(1024)) COMMENT "accelerate the speed when add global binding query",
			INDEX time_index(update_time) COMMENT "accelerate the speed when querying with last update time"
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`
	
		// CreateRoleEdgesBlock stores the role and user relationship information.
		CreateRoleEdgesBlock = `CREATE TABLE IF NOT EXISTS mysql.role_edges (
			FROM_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
			FROM_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
			TO_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
			TO_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
			WITH_ADMIN_OPTION enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',
			PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
		);`
	
		// CreateDefaultRolesBlock stores the active roles for a user.
		CreateDefaultRolesBlock = `CREATE TABLE IF NOT EXISTS mysql.default_roles (
			HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
			USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
			DEFAULT_ROLE_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '%',
			DEFAULT_ROLE_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
			PRIMARY KEY (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER)
		)`
	
		// CreateStatsTopNBlock stores topn data of a cmsketch with top n.
		CreateStatsTopNlock = `CREATE TABLE if not exists mysql.stats_top_n (
			table_id bigint(64) NOT NULL,
			is_index tinyint(2) NOT NULL,
			hist_id bigint(64) NOT NULL,
			value longblob,
			count bigint(64) UNSIGNED NOT NULL,
			index tbl(table_id, is_index, hist_id)
		);`
	
		// CreateExprPushdownBlacklist stores the expressions which are not allowed to be pushed down.
		CreateExprPushdownBlacklist = `CREATE TABLE IF NOT EXISTS mysql.expr_pushdown_blacklist (
			name char(100) NOT NULL
		);`
	
		// CreateOptRuleBlacklist stores the list of disabled optimizing operations.
		CreateOptRuleBlacklist = `CREATE TABLE IF NOT EXISTS mysql.opt_rule_blacklist (
			name char(100) NOT NULL
		);`
	)
	
	// stochastik initiates system DB for a store.
	func stochastik(s Session) {
		startTime := time.Now()
		dom := domain.GetDomain(s)
		for {
			b, err := checkBootstrapped(s)
			if err != nil {
				logutil.BgLogger().Fatal("check stochastik error",
					zap.Error(err))
			}
			// For rolling upgrade, we can't do upgrade only in the owner.
			if b {
				upgrade(s)
				logutil.BgLogger().Info("upgrade successful in stochastik",
					zap.Duration("take time", time.Since(startTime)))
				return
			}
			// To reduce conflict when multiple TiDB-server start at the same time.
			// Actually only one server need to do the stochastik. So we chose DDL owner to do this.
			if dom.DDL().OwnerManager().IsOwner() {
				doDDLWorks(s)
				doDMLWorks(s)
				logutil.BgLogger().Info("stochastik successful",
					zap.Duration("take time", time.Since(startTime)))
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	const (
		//The variable name
		//The variable value in mysql.TiDB table for bootstrappedVar.
		stochsstiktrappedVar = "stochastik"
		stochastiktrappedVarTrue = "True"
	)

type Stochastik interface {
	//stochastikInfo is used by showStochastik(), and should be modified atomically.
	stochastiktxn.Context
	Status() uint16								//status flag
	LastInsertID() uint64						//Last auto_increment ID
	LastMessage() string
	AffectEvents() uint64						//Affected event rows after executed statement

}
