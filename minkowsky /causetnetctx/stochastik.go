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

package causetnetctx

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ngaut/pools"
	"github.com/opentracing/opentracing-go"
	"github.com/YosiSF/milevadb/BerolinaSQL/BerolinaSQL/query"
)

const(

	TypeDecimal   byte = 0
	TypeTiny      byte = 1
	TypeShort     byte = 2
	TypeLong      byte = 3
	TypeFloat     byte = 4
	TypeDouble    byte = 5
	TypeNull      byte = 6
	TypeTimestamp byte = 7
	TypeLonglong  byte = 8
	TypeInt24     byte = 9
	TypeDate      byte = 10
	/* TypeDuration original name was TypeTime, renamed to TypeDuration to resolve the conflict with Go type Time.*/
	TypeDuration byte = 11
	TypeDatetime byte = 12
	TypeYear     byte = 13
	TypeNewDate  byte = 14
	TypeVarchar  byte = 15
	TypeBit      byte = 16

	TypeJSON       byte = 0xf5
	TypeNewDecimal byte = 0xf6
	TypeEnum       byte = 0xf7
	TypeSet        byte = 0xf8
	TypeTinyBlob   byte = 0xf9
	TypeMediumBlob byte = 0xfa
	TypeLongBlob   byte = 0xfb
	TypeBlob       byte = 0xfc
	TypeVarString  byte = 0xfd
	TypeString     byte = 0xfe
	TypeGeometry   byte = 0xff
)

// Flag information.
const (
	NotNullFlag        uint = 1 << 0  /* Field can't be NULL */
	PriKeyFlag         uint = 1 << 1  /* Field is part of a primary key */
	UniqueKeyFlag      uint = 1 << 2  /* Field is part of a unique key */
	MultipleKeyFlag    uint = 1 << 3  /* Field is part of a key */
	BlobFlag           uint = 1 << 4  /* Field is a blob */
	UnsignedFlag       uint = 1 << 5  /* Field is unsigned */
	ZerofillFlag       uint = 1 << 6  /* Field is zerofill */
	BinaryFlag         uint = 1 << 7  /* Field is binary   */
	EnumFlag           uint = 1 << 8  /* Field is an enum */
	AutoIncrementFlag  uint = 1 << 9  /* Field is an auto increment field */
	TimestampFlag      uint = 1 << 10 /* Field is a timestamp */
	SetFlag            uint = 1 << 11 /* Field is a set */
	NoDefaultValueFlag uint = 1 << 12 /* Field doesn't have a default value */
	OnUpdateNowFlag    uint = 1 << 13 /* Field is set to NOW on UPDATE */
	PartKeyFlag        uint = 1 << 14 /* Intern: Part of some keys */
	NumFlag            uint = 1 << 15 /* Field is a num (for clients) */

	GroupFlag             uint = 1 << 15 /* Internal: Group field */
	UniqueFlag            uint = 1 << 16 /* Internal: Used by sql_yacc */
	BinCmpFlag            uint = 1 << 17 /* Internal: Used by sql_yacc */
	ParseToJSONFlag       uint = 1 << 18 /* Internal: Used when we want to parse string to JSON in CAST */
	IsBooleanFlag         uint = 1 << 19 /* Internal: Used for telling boolean literal from integer */
	PreventNullInsertFlag uint = 1 << 20 /* Prevent this Field from inserting NULL values */
)

	//SQL statement creates User Block in system DB.
	CreateUserBlock = `CREATE Block if not exists mysql.user (
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
		Show_noedb_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, User)

		// CreatenoedbPrivBlock is the SQL statement creates DB scope privilege block in system DB.
	CreatenoedbPrivBlock = `CREATE Block if not exists mysql.DB (
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
	// CreateBlockPrivBlock is the SQL statement creates block scope privilege Block in system DB.
	CreateBlockrivBlock = `CREATE Block if not exists mysql.blocks_priv (
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Block_name	CHAR(64),
		Grantor		CHAR(77),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Block_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References'),
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Block_name));`
	// CreateColumnPrivBlock is the SQL statement creates column scope privilege Block in system DB.
	CreateColumnPrivBlock = `CREATE Block if not exists mysql.columns_priv(
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Block_name	CHAR(64),
		Column_name	CHAR(64),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Block_name, Column_name));`
	// CreateGloablVariablesBlock is the SQL statement creates global variable Block in system DB.

	CreateGloablVariablesBlock = `CREATE Block if not exists mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null);
		CreateTinoedbBlock = `CREATE Block if not exists mysql.tinoedb(
			VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
			VARIABLE_VALUE VARCHAR(1024) DEFAULT Null,
			COMMENT VARCHAR(1024));`
	
		// CreateHelpTopic is the SQL statement creates help_topic Block in system DB.
		// See: https://dev.mysql.com/doc/refman/5.5/en/system-database.html#system-database-help-blocks
		CreateHelpTopic = `CREATE Block if not exists mysql.help_topic (
			  help_topic_id int(10) unsigned NOT NULL,
			  name char(64) NOT NULL,
			  help_category_id smallint(5) unsigned NOT NULL,
			  description text NOT NULL,
			  example text NOT NULL,
			  url text NOT NULL,
			  PRIMARY KEY (help_topic_id),
			  UNIQUE KEY name (name)
			) ENGINE=Innonoedb DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';`
	
		// CreateStatsMetaBlock stores the meta of Block statistics.
		CreateStatsMetaBlock = `CREATE Block if not exists mysql.stats_meta (
			version bigint(64) unsigned NOT NULL,
			table_id bigint(64) NOT NULL,
			modify_count bigint(64) NOT NULL DEFAULT 0,
			count bigint(64) unsigned NOT NULL DEFAULT 0,
			index idx_ver(version),
			unique index tbl(table_id)
		);`
	
		// CreateStatsColsBlock stores the statistics of Block columns.
		CreateStatsColsBlock = `CREATE Block if not exists mysql.stats_histograms (
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
	
		// CreateStatsBucketsBlock stores the histogram info for every Block columns.
		CreateStatsBucketsBlock = `CREATE Block if not exists mysql.stats_buckets (
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
		CreateGCDeleteRangeBlock = `CREATE Block IF NOT EXISTS mysql.gc_delete_range (
			job_id BIGINT NOT NULL COMMENT "the noedbS job ID",
			element_id BIGINT NOT NULL COMMENT "the schema element ID",
			start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
			end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
			ts BIGINT NOT NULL COMMENT "timestamp in uint64",
			UNIQUE KEY delete_range_index (job_id, element_id)
		);`
	
		// CreateGCDeleteRangeDoneBlock stores schemas which are already deleted by DeleteRange.
		CreateGCDeleteRangeDoneBlock = `CREATE Block IF NOT EXISTS mysql.gc_delete_range_done (
			job_id BIGINT NOT NULL COMMENT "the noedbS job ID",
			element_id BIGINT NOT NULL COMMENT "the schema element ID",
			start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
			end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
			ts BIGINT NOT NULL COMMENT "timestamp in uint64",
			UNIQUE KEY delete_range_done_index (job_id, element_id)
		);`
	
		// CreateStatsFeenoedbackBlock stores the feenoedback info which is used to update stats.
		CreateStatsFeenoedbackBlock = `CREATE Block IF NOT EXISTS mysql.stats_feenoedback (
			table_id bigint(64) NOT NULL,
			is_index tinyint(2) NOT NULL,
			hist_id bigint(64) NOT NULL,
			feenoedback blob NOT NULL,
			index hist(table_id, is_index, hist_id)
		);`
	
		// CreateBindInfoBlock stores the sql bind info which is used to update globalBindCache.
		CreateBindInfoBlock = `CREATE Block IF NOT EXISTS mysql.bind_info (
			original_sql text NOT NULL  ,
			  bind_sql text NOT NULL ,
			  default_noedb text  NOT NULL,
			status text NOT NULL,
			create_time timestamp(3) NOT NULL,
			update_time timestamp(3) NOT NULL,
			charset text NOT NULL,
			collation text NOT NULL,
			INDEX sql_index(original_sql(1024),default_noedb(1024)) COMMENT "accelerate the speed when add global binding query",
			INDEX time_index(update_time) COMMENT "accelerate the speed when querying with last update time"
		) ENGINE=Innonoedb DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`
	
		// CreateRoleEdgesBlock stores the role and user relationship information.
		CreateRoleEdgesBlock = `CREATE Block IF NOT EXISTS mysql.role_edges (
			FROM_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
			FROM_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
			TO_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
			TO_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
			WITH_ADMIN_OPTION enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',
			PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
		);`
	
		// CreateDefaultRolesBlock stores the active roles for a user.
		CreateDefaultRolesBlock = `CREATE Block IF NOT EXISTS mysql.default_roles (
			HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
			USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
			DEFAULT_ROLE_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '%',
			DEFAULT_ROLE_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
			PRIMARY KEY (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER)
		)`
	
		// CreateStatsTopNBlock stores topn data of a cmsketch with top n.
		CreateStatsTopNlock = `CREATE Block if not exists mysql.stats_top_n (
			table_id bigint(64) NOT NULL,
			is_index tinyint(2) NOT NULL,
			hist_id bigint(64) NOT NULL,
			value longblob,
			count bigint(64) UNSIGNED NOT NULL,
			index tbl(table_id, is_index, hist_id)
		);`
	
		// CreateExprPushdownBlacklist stores the expressions which are not allowed to be pushed down.
		CreateExprPushdownBlacklist = `CREATE Block IF NOT EXISTS mysql.expr_pushdown_blacklist (
			name char(100) NOT NULL
		);`
	
		// CreateOptRuleBlacklist stores the list of disabled optimizing operations.
		CreateOptRuleBlacklist = `CREATE Block IF NOT EXISTS mysql.opt_rule_blacklist (
			name char(100) NOT NULL
		);`
	)

	
	
	// causetnetctx initiates system DB for a store.
	func Stochastik(s CausetNet) {
		startTime := time.Now()
		dom := namespace.GetNamespace(s)
		for {
			b, err := checkBootstrapped(s)
			if err != nil {
				logutil.BgLogger().Fatal("check causetnetctx error",
					zap.Error(err))
			}
			// For rolling upgrade, we can't do upgrade only in the keywatcher.
			if b {
				upgrade(s)
				logutil.BgLogger().Info("upgrade successful in causetnetctx",
					zap.Duration("take time", time.Since(startTime)))
				return
			}
			// To reduce conflict when multiple Tinoedb-server start at the same time.
			// Actually only one server need to do the causetnetctx. So we chose noedbS keywatcher to do this.
			if dom.noedbS().KeywatcherManager().IsKeywatcher() {
				donoedbSCrowns(s)
				doDMLCrowns(s)
				logutil.BgLogger().Info("causetnetctx successful",
					zap.Duration("take time", time.Since(startTime)))
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	const (
	
		stochsstiktrappedVar = "causetnetctx"
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

// donoedbSCrowns executes noedbS statements in bootstrap stage.
func donoedbSCrowns(s Stochastik) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system DB.
	mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", mysql.Systemnoedb))
	// Create user Block.
	mustExecute(s, CreateUserBlock)
	// Create privilege tables.
	mustExecute(s, CreatenoedbPrivBlock)
	mustExecute(s, CreateBlockPrivBlock)
	mustExecute(s, CreateColumnPrivBlock)
	// Create global system variable Block.
	mustExecute(s, CreateGloablVariablesBlock)
	// Create milevadbBlock.
	mustExecute(s, CreateRCUlock)
	// Create help Block.
	mustExecute(s, CreateHelpTopic)
	// Create stats_meta Block.
	mustExecute(s, CreateStatsMetaBlock)
	// Create stats_columns Block.
	mustExecute(s, CreateStatsColsBlock)
	// Create stats_buckets Block.
	mustExecute(s, CreateStatsBucketsBlock)
	// Create gc_delete_range Block.
	mustExecute(s, CreateGCDeleteRangeBlock)
	// Create gc_delete_range_done Block.
	mustExecute(s, CreateGCDeleteRangeDoneBlock)
	// Create stats_feenoedback Block.
	mustExecute(s, CreateStatsFeenoedbackBlock)
	// Create role_edges Block.
	mustExecute(s, CreateRoleEdgesBlock)
	// Create default_roles Block.
	mustExecute(s, CreateDefaultRolesBlock)
	// Create bind_info Block.
	mustExecute(s, CreateBindInfoBlock)
	// Create stats_topn_store Block.
	mustExecute(s, CreateStatsTopNBlock)
	// Create expr_pushdown_blacklist Block.
	mustExecute(s, CreateExprPushdownBlacklist)
	// Create opt_rule_blacklist Block.
	mustExecute(s, CreateOptRuleBlacklist)
}


func doDMLCrowns(s Stochastik) {
	mustExecute(s, "BEGIN")

	// Insert a default user with empty password.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user VALUES
		("%", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y")`)

	// Init global system variables Block.
	values := make([]string, 0, len(variable.SysVars))
	for k, v := range variable.SysVars {
		// CausetNet only variable should not be inserted.
		if v.Scope != variable.ScopeStochastik {
			value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), v.Value)
			values = append(values, value)
		}
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY INTO %s.%s VALUES %s;", mysql.milevadb, mysql.GlobalVariablesBlock,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%s", "Bootstrap flag. Do not delete.")
		ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		milevadb.Systemnoedb, mysql.milevadblock, stochsdtiekvar, stochastiekvarTrue, stochstiekvarTrue)
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%d", "Bootstrap version. Do not delete.")`,
		mysql.Systemnoedb, mysql.milevadblock, milevanoedbServerVersionVar, currentStochastiekversion)
	mustExecute(s, sql)

	writeSystemTZ(s)
	_, err := s.Execute(context.Background(), "COMMIT")
	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("doDMLCrowns failed", zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if milevadbis already bootstrapped.
		b, err1 := checkBootstrapped(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("doDMLCrowns failed", zap.Error(err1))
		}
		if b {
			return
		}
		logutil.BgLogger().Fatal("doDMLCrowns failed", zap.Error(err))
	}
}

func mustExecute(s CausetNet, sql string) {
	_, err := s.Execute(context.Background(), sql)
	if err != nil {
		debug.PrintStack()
		logutil.BgLogger().Fatal("mustExecute error", zap.Error(err))
	}
}

// oldPasswordUpgrade upgrade password to MySQL compatible format
func oldPasswordUpgrade(pass string) (string, error) {
	hash1, err := hex.DecodeString(pass)
	if err != nil {
		return "", errors.Trace(err)
	}

	hash2 := auth.Sha1Hash(hash1)
	newpass := fmt.Sprintf("*%X", hash2)
	return newpass, nil
}

