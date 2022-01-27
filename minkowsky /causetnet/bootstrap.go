package causetnet

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/whtcorpsinc/MilevaDB/BerolinaSQL/auth"
	"github.com/whtcorpsinc/MilevaDB/BerolinaSQL/mysql"
	"github.com/whtcorpsinc/MilevaDB/BerolinaSQL/terror"
	"github.com/whtcorpsinc/MilevaDB/causetnetctx/variable"
	"github.com/whtcorpsinc/MilevaDB/config"
	"github.com/whtcorpsinc/MilevaDB/dbs"
	"github.com/whtcorpsinc/MilevaDB/namespace"
	"github.com/whtcorpsinc/MilevaDB/planner/core"
	"github.com/whtcorpsinc/MilevaDB/util/logutil"
	"github.com/whtcorpsinc/MilevaDB/util/timeutil"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

const (
	// CreateUserTable is the SQL statement creates User table in system DB.
	CreateUserTable = `CREATE TABLE if not exists mysql.user (
		Host				CHAR(64),
		User				CHAR(32),
		authentication_string	TEXT,
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
		Super_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_tmp_table_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_tables_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Index_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_user_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Event_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_role_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_role_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Account_locked			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Shutdown_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Reload_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		FILE_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Config_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, User));`
	// CreateGlobalPrivTable is the SQL statement creates Global sINTERLOCKe privilege table in system DB.
	CreateGlobalPrivTable = "CREATE TABLE if not exists mysql.global_priv (" +
		"Host char(60) NOT NULL DEFAULT ''," +
		"User char(80) NOT NULL DEFAULT ''," +
		"Priv longtext NOT NULL DEFAULT ''," +
		"PRIMARY KEY (Host, User)" +
		")"
	// CreatenoedbPrivTable is the SQL statement creates DB sINTERLOCKe privilege table in system DB.
	CreatenoedbPrivTable = `CREATE TABLE if not exists mysql.DB (
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
		Lock_tables_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Event_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, DB, User));`
	// CreateTablePrivTable is the SQL statement creates table sINTERLOCKe privilege table in system DB.
	CreateTablePrivTable = `CREATE TABLE if not exists mysql.tables_priv (
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		Grantor		CHAR(77),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Table_priv	SET('Select','Insert','Update','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References'),
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name));`
	// CreateColumnPrivTable is the SQL statement creates column sINTERLOCKe privilege table in system DB.
	CreateColumnPrivTable = `CREATE TABLE if not exists mysql.columns_priv(
		Host		CHAR(60),
		DB		CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		Column_name	CHAR(64),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Column_priv	SET('Select','Insert','Update'),
		PRIMARY KEY (Host, DB, User, Table_name, Column_name));`
	// CreateGlobalVariablesTable is the SQL statement creates global variable table in system DB.
	// TODO: MySQL puts GLOBAL_VARIABLES table in INFORMATION_SCHEMA DB.
	// INFORMATION_SCHEMA is a virtual DB in milevadb. So we put this table in system DB.
	// Maybe we will put it back to INFORMATION_SCHEMA.
	CreateGlobalVariablesTable = `CREATE TABLE if not exists mysql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null);`
	// CreatemilevanoedbTable is the SQL statement creates a table in system DB.
	// This table is a key-value struct contains some information used by milevadb.
	// Currently we only put bootstrapped in it which indicates if the system is already bootstrapped.
	CreatemilevanoedbTable = `CREATE TABLE if not exists mysql.milevadb(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null,
		COMMENT VARCHAR(1024));`

	// CreateHelpTopic is the SQL statement creates help_topic table in system DB.
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
		) ENGINE=Innonoedb DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';`

	// CreateStatsMetaTable stores the meta of table statistics.
	CreateStatsMetaTable = `CREATE TABLE if not exists mysql.stats_meta (
		version bigint(64) unsigned NOT NULL,
		table_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) unsigned NOT NULL DEFAULT 0,
		index idx_ver(version),
		unique index tbl(table_id)
	);`

	// CreateStatsColsTable stores the statistics of table columns.
	CreateStatsColsTable = `CREATE TABLE if not exists mysql.stats_histograms (
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

	// CreateStatsBucketsTable stores the histogram info for every table columns.
	CreateStatsBucketsTable = `CREATE TABLE if not exists mysql.stats_buckets (
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

	// CreateGCDeleteRangeTable stores schemas which can be deleted by DeleteRange.
	CreateGCDeleteRangeTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range (
		job_id BIGINT NOT NULL COMMENT "the dbs job ID",
		element_id BIGINT NOT NULL COMMENT "the schema element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_index (job_id, element_id)
	);`

	// CreateGCDeleteRangeDoneTable stores schemas which are already deleted by DeleteRange.
	CreateGCDeleteRangeDoneTable = `CREATE TABLE IF NOT EXISTS mysql.gc_delete_range_done (
		job_id BIGINT NOT NULL COMMENT "the dbs job ID",
		element_id BIGINT NOT NULL COMMENT "the schema element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_done_index (job_id, element_id)
	);`

	// CreateStatsFeenoedbackTable stores the feenoedback info which is used to update stats.
	CreateStatsFeenoedbackTable = `CREATE TABLE IF NOT EXISTS mysql.stats_feenoedback (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		feenoedback blob NOT NULL,
		index hist(table_id, is_index, hist_id)
	);`

	// CreateBindInfoTable stores the sql bind info which is used to update globalBindCache.
	CreateBindInfoTable = `CREATE TABLE IF NOT EXISTS mysql.bind_info (
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

	// CreateRoleEdgesTable stores the role and user relationship information.
	CreateRoleEdgesTable = `CREATE TABLE IF NOT EXISTS mysql.role_edges (
		FROM_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		FROM_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		WITH_ADMIN_OPTION enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',
		PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
	);`

	// CreateDefaultRolesTable stores the active roles for a user.
	CreateDefaultRolesTable = `CREATE TABLE IF NOT EXISTS mysql.default_roles (
		HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		DEFAULT_ROLE_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '%',
		DEFAULT_ROLE_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		PRIMARY KEY (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER)
	)`

	// CreateStatsTopNTable stores topn data of a cmsketch with top n.
	CreateStatsTopNTable = `CREATE TABLE if not exists mysql.stats_top_n (
		table_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		value longblob,
		count bigint(64) UNSIGNED NOT NULL,
		index tbl(table_id, is_index, hist_id)
	);`

	// CreateExprPushdownBlacklist stores the expressions which are not allowed to be pushed down.
	CreateExprPushdownBlacklist = `CREATE TABLE IF NOT EXISTS mysql.expr_pushdown_blacklist (
		name char(100) NOT NULL,
		store_type char(100) NOT NULL DEFAULT 'tiekv,Noether,milevadb',
		reason varchar(200)
	);`

	// CreateOptRuleBlacklist stores the list of disabled optimizing operations.
	CreateOptRuleBlacklist = `CREATE TABLE IF NOT EXISTS mysql.opt_rule_blacklist (
		name char(100) NOT NULL
	);`
)

// bootstrap initiates system DB for a store.
func bootstrap(s CausetNet) {
	startTime := time.Now()
	dom := namespace.GetNamespace(s)
	for {
		b, err := checkBootstrapped(s)
		if err != nil {
			logutil.BgLogger().Fatal("check bootstrap error",
				zap.Error(err))
		}
		// For rolling upgrade, we can't do upgrade only in the keywatcher.
		if b {
			upgrade(s)
			logutil.BgLogger().Info("upgrade successful in bootstrap",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		// To reduce conflict when multiple milevadb-server start at the same time.
		// Actually only one server need to do the bootstrap. So we chose dbs keywatcher to do this.
		if dom.dbs().KeywatcherManager().IsKeywatcher() {
			dodbsWorks(s)
			doDMLWorks(s)
			logutil.BgLogger().Info("bootstrap successful",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

const (
	// varTrue is the true value in mysql.milevadb table for boolean columns.
	varTrue = "True"
	// varFalse is the false value in mysql.milevadb table for boolean columns.
	varFalse = "False"
	// The variable name in mysql.milevadb table.
	// It is used for checking if the store is bootstrapped by any milevadb server.
	// If the value is `True`, the store is already bootstrapped by a milevadb server.
	bootstrappedVar = "bootstrapped"
	// The variable name in mysql.milevadb table.
	// It is used for getting the version of the milevadb server which bootstrapped the store.
	milevanoedbServerVersionVar = "milevanoedb_server_version"
	// The variable name in mysql.milevadb table and it will be used when we want to know
	// system timezone.
	milevanoedbSystemTZ = "system_tz"
	// The variable name in mysql.milevadb table and it will indicate if the new collations are enabled in the milevadb cluster.
	milevanoedbNewCollationEnabled = "new_collation_enabled"
	// Const for milevadb server version 2.
	version2  = 2
	version3  = 3
	version4  = 4
	version5  = 5
	version6  = 6
	version7  = 7
	version8  = 8
	version9  = 9
	version10 = 10
	version11 = 11
	version12 = 12
	version13 = 13
	version14 = 14
	version15 = 15
	version16 = 16
	version17 = 17
	version18 = 18
	version19 = 19
	version20 = 20
	version21 = 21
	version22 = 22
	version23 = 23
	version24 = 24
	version25 = 25
	version26 = 26
	version27 = 27
	version28 = 28
	// version29 is not needed.
	version30 = 30
	version31 = 31
	version32 = 32
	version33 = 33
	version34 = 34
	version35 = 35
	version36 = 36
	version37 = 37
	version38 = 38
	version39 = 39
	// version40 is the version that introduce new collation in milevadb,
	// see https://github.com/whtcorpsinc/milevadb/BerolinaSQL/pull/14574 for more details.
	version40 = 40
	version41 = 41
	// version42 add storeType and reason column in expr_pushdown_blacklist
	version42 = 42
	// version43 updates global variables related to statement summary.
	version43 = 43
	// version44 delete milevanoedb_isolation_read_engines from mysql.global_variables to avoid unexpected behavior after upgrade.
	version44 = 44
	// version45 introduces CONFIG_PRIV for SET CONFIG statements.
	version45 = 45
)

var (
	bootstrapVersion = []func(CausetNet, int64){
		upgradeToVer2,
		upgradeToVer3,
		upgradeToVer4,
		upgradeToVer5,
		upgradeToVer6,
		upgradeToVer7,
		upgradeToVer8,
		upgradeToVer9,
		upgradeToVer10,
		upgradeToVer11,
		upgradeToVer12,
		upgradeToVer13,
		upgradeToVer14,
		upgradeToVer15,
		upgradeToVer16,
		upgradeToVer17,
		upgradeToVer18,
		upgradeToVer19,
		upgradeToVer20,
		upgradeToVer21,
		upgradeToVer22,
		upgradeToVer23,
		upgradeToVer24,
		upgradeToVer25,
		upgradeToVer26,
		upgradeToVer27,
		upgradeToVer28,
		upgradeToVer29,
		upgradeToVer30,
		upgradeToVer31,
		upgradeToVer32,
		upgradeToVer33,
		upgradeToVer34,
		upgradeToVer35,
		upgradeToVer36,
		upgradeToVer37,
		upgradeToVer38,
		upgradeToVer39,
		upgradeToVer40,
		upgradeToVer41,
		upgradeToVer42,
		upgradeToVer43,
		upgradeToVer44,
		upgradeToVer45,
	}
)

func checkBootstrapped(s CausetNet) (bool, error) {
	//  Check if system DB exists.
	_, err := s.Execute(context.Background(), fmt.Sprintf("USE %s;", mysql.Systemnoedb))
	if err != nil && schemaReplicant.ErrDatabaseNotExists.NotEqual(err) {
		logutil.BgLogger().Fatal("check bootstrap error",
			zap.Error(err))
	}
	// Check bootstrapped variable value in milevadb table.
	sVal, _, err := getmilevanoedbVar(s, bootstrappedVar)
	if err != nil {
		if schemaReplicant.ErrTableNotExists.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	isBootstrapped := sVal == varTrue
	if isBootstrapped {
		// Make sure that doesn't affect the following operations.
		if err = s.CommitTxn(context.Background()); err != nil {
			return false, errors.Trace(err)
		}
	}
	return isBootstrapped, nil
}

// getmilevanoedbVar gets variable value from mysql.milevadb table.
// Those variables are used by milevadb server.
func getmilevanoedbVar(s CausetNet, name string) (sVal string, isNull bool, e error) {
	sql := fmt.Sprintf(`SELECT HIGH_PRIORITY VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s"`,
		mysql.Systemnoedb, mysql.milevanoedbTable, name)
	ctx := context.Background()
	rs, err := s.Execute(ctx, sql)
	if err != nil {
		return "", true, errors.Trace(err)
	}
	if len(rs) != 1 {
		return "", true, errors.New("Wrong number of Recordset")
	}
	r := rs[0]
	defer terror.Call(r.Close)
	req := r.NewSoliton()
	err = r.Next(ctx, req)
	if err != nil || req.NumRows() == 0 {
		return "", true, errors.Trace(err)
	}
	row := req.GetRow(0)
	if row.IsNull(0) {
		return "", true, nil
	}
	return row.GetString(0), false, nil
}

// upgrade function  will do some upgrade works, when the system is bootstrapped by low version milevadb server
// For example, add new system variables into mysql.global_variables table.
func upgrade(s CausetNet) {
	ver, err := getBootstrapVersion(s)
	terror.MustNil(err)
	if ver >= currentBootstrapVersion {
		// It is already bootstrapped/upgraded by a higher version milevadb server.
		return
	}
	// Do upgrade works then update bootstrap version.
	for _, upgrade := range bootstrapVersion {
		upgrade(s, ver)
	}

	updateBootstrapVer(s)
	_, err = s.Execute(context.Background(), "COMMIT")

	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("update bootstrap ver failed",
			zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if milevadb is already upgraded.
		v, err1 := getBootstrapVersion(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("upgrade failed", zap.Error(err1))
		}
		if v >= currentBootstrapVersion {
			// It is already bootstrapped/upgraded by a higher version milevadb server.
			return
		}
		logutil.BgLogger().Fatal("[Upgrade] upgrade failed",
			zap.Int64("from", ver),
			zap.Int("to", currentBootstrapVersion),
			zap.Error(err))
	}
}

// upgradeToVer2 updates to version 2.
func upgradeToVer2(s CausetNet, ver int64) {
	if ver >= version2 {
		return
	}
	// Version 2 add two system variable for DistSQL concurrency controlling.
	// Insert distsql related system variable.
	distSQLVars := []string{variable.milevanoedbDistSQLScanConcurrency}
	values := make([]string, 0, len(distSQLVars))
	for _, v := range distSQLVars {
		value := fmt.Sprintf(`("%s", "%s")`, v, variable.SysVars[v].Value)
		values = append(values, value)
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY IGNORE INTO %s.%s VALUES %s;", mysql.Systemnoedb, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)
}

// upgradeToVer3 updates to version 3.
func upgradeToVer3(s CausetNet, ver int64) {
	if ver >= version3 {
		return
	}
	// Version 3 fix tx_read_only variable value.
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %s.%s set variable_value = '0' where variable_name = 'tx_read_only';",
		mysql.Systemnoedb, mysql.GlobalVariablesTable)
	mustExecute(s, sql)
}

// upgradeToVer4 updates to version 4.
func upgradeToVer4(s CausetNet, ver int64) {
	if ver >= version4 {
		return
	}
	sql := CreateStatsMetaTable
	mustExecute(s, sql)
}

func upgradeToVer5(s CausetNet, ver int64) {
	if ver >= version5 {
		return
	}
	mustExecute(s, CreateStatsColsTable)
	mustExecute(s, CreateStatsBucketsTable)
}

func upgradeToVer6(s CausetNet, ver int64) {
	if ver >= version6 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `Super_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_noedb_priv`", schemaReplicant.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as milevadb doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Super_priv='Y'")
}

func upgradeToVer7(s CausetNet, ver int64) {
	if ver >= version7 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `Process_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Drop_priv`", schemaReplicant.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as milevadb doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Process_priv='Y'")
}

func upgradeToVer8(s CausetNet, ver int64) {
	if ver >= version8 {
		return
	}
	// This is a dummy upgrade, it checks whether upgradeToVer7 success, if not, do it again.
	if _, err := s.Execute(context.Background(), "SELECT HIGH_PRIORITY `Process_priv` from mysql.user limit 0"); err == nil {
		return
	}
	upgradeToVer7(s, ver)
}

func upgradeToVer9(s CausetNet, ver int64) {
	if ver >= version9 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `Trigger_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`", schemaReplicant.ErrColumnExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as milevadb doesn't check them in older versions.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Trigger_priv='Y'")
}

func doReentrantdbs(s CausetNet, sql string, ignorableErrs ...error) {
	_, err := s.Execute(context.Background(), sql)
	for _, ignorableErr := range ignorableErrs {
		if terror.ErrorEqual(err, ignorableErr) {
			return
		}
	}
	if err != nil {
		logutil.BgLogger().Fatal("doReentrantdbs error", zap.Error(err))
	}
}

func upgradeToVer10(s CausetNet, ver int64) {
	if ver >= version10 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.stats_buckets CHANGE COLUMN `value` `upper_bound` BLOB NOT NULL", schemaReplicant.ErrColumnNotExists, schemaReplicant.ErrColumnExists)
	doReentrantdbs(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `lower_bound` BLOB", schemaReplicant.ErrColumnExists)
	doReentrantdbs(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `null_count` bigint(64) NOT NULL DEFAULT 0", schemaReplicant.ErrColumnExists)
	doReentrantdbs(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN distinct_ratio", dbs.ErrCantDropFieldOrKey)
	doReentrantdbs(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN use_count_to_estimate", dbs.ErrCantDropFieldOrKey)
}

func upgradeToVer11(s CausetNet, ver int64) {
	if ver >= version11 {
		return
	}
	_, err := s.Execute(context.Background(), "ALTER TABLE mysql.user ADD COLUMN `References_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`")
	if err != nil {
		if terror.ErrorEqual(err, schemaReplicant.ErrColumnExists) {
			return
		}
		logutil.BgLogger().Fatal("upgradeToVer11 error", zap.Error(err))
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET References_priv='Y'")
}

func upgradeToVer12(s CausetNet, ver int64) {
	if ver >= version12 {
		return
	}
	ctx := context.Background()
	_, err := s.Execute(ctx, "BEGIN")
	terror.MustNil(err)
	sql := "SELECT HIGH_PRIORITY user, host, password FROM mysql.user WHERE password != ''"
	rs, err := s.Execute(ctx, sql)
	if terror.ErrorEqual(err, core.ErrUnknownColumn) {
		sql := "SELECT HIGH_PRIORITY user, host, authentication_string FROM mysql.user WHERE authentication_string != ''"
		rs, err = s.Execute(ctx, sql)
	}
	terror.MustNil(err)
	r := rs[0]
	sqls := make([]string, 0, 1)
	defer terror.Call(r.Close)
	req := r.NewSoliton()
	it := soliton.NewIterator4Soliton(req)
	err = r.Next(ctx, req)
	for err == nil && req.NumRows() != 0 {
		for row := it.Begin(); row != it.End(); row = it.Next() {
			user := row.GetString(0)
			host := row.GetString(1)
			pass := row.GetString(2)
			var newPass string
			newPass, err = oldPasswordUpgrade(pass)
			terror.MustNil(err)
			updateSQL := fmt.Sprintf(`UPDATE HIGH_PRIORITY mysql.user set password = "%s" where user="%s" and host="%s"`, newPass, user, host)
			sqls = append(sqls, updateSQL)
		}
		err = r.Next(ctx, req)
	}
	terror.MustNil(err)

	for _, sql := range sqls {
		mustExecute(s, sql)
	}

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%d", "milevadb bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.Systemnoedb, mysql.milevanoedbTable, milevanoedbServerVersionVar, version12, version12)
	mustExecute(s, sql)

	mustExecute(s, "COMMIT")
}

func upgradeToVer13(s CausetNet, ver int64) {
	if ver >= version13 {
		return
	}
	sqls := []string{
		"ALTER TABLE mysql.user ADD COLUMN `Create_tmp_table_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Super_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Lock_tables_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Show_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.user ADD COLUMN `Event_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`",
	}
	ctx := context.Background()
	for _, sql := range sqls {
		_, err := s.Execute(ctx, sql)
		if err != nil {
			if terror.ErrorEqual(err, schemaReplicant.ErrColumnExists) {
				continue
			}
			logutil.BgLogger().Fatal("upgradeToVer13 error", zap.Error(err))
		}
	}
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer14(s CausetNet, ver int64) {
	if ver >= version14 {
		return
	}
	sqls := []string{
		"ALTER TABLE mysql.DB ADD COLUMN `References_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`",
		"ALTER TABLE mysql.DB ADD COLUMN `Create_tmp_table_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Alter_priv`",
		"ALTER TABLE mysql.DB ADD COLUMN `Lock_tables_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
		"ALTER TABLE mysql.DB ADD COLUMN `Create_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Lock_tables_priv`",
		"ALTER TABLE mysql.DB ADD COLUMN `Show_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE mysql.DB ADD COLUMN `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE mysql.DB ADD COLUMN `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE mysql.DB ADD COLUMN `Event_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE mysql.DB ADD COLUMN `Trigger_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Event_priv`",
	}
	ctx := context.Background()
	for _, sql := range sqls {
		_, err := s.Execute(ctx, sql)
		if err != nil {
			if terror.ErrorEqual(err, schemaReplicant.ErrColumnExists) {
				continue
			}
			logutil.BgLogger().Fatal("upgradeToVer14 error", zap.Error(err))
		}
	}
}

func upgradeToVer15(s CausetNet, ver int64) {
	if ver >= version15 {
		return
	}
	var err error
	_, err = s.Execute(context.Background(), CreateGCDeleteRangeTable)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer15 error", zap.Error(err))
	}
}

func upgradeToVer16(s CausetNet, ver int64) {
	if ver >= version16 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `cm_sketch` blob", schemaReplicant.ErrColumnExists)
}

func upgradeToVer17(s CausetNet, ver int64) {
	if ver >= version17 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.user MODIFY User CHAR(32)")
}

func upgradeToVer18(s CausetNet, ver int64) {
	if ver >= version18 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `tot_col_size` bigint(64) NOT NULL DEFAULT 0", schemaReplicant.ErrColumnExists)
}

func upgradeToVer19(s CausetNet, ver int64) {
	if ver >= version19 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.DB MODIFY User CHAR(32)")
	doReentrantdbs(s, "ALTER TABLE mysql.tables_priv MODIFY User CHAR(32)")
	doReentrantdbs(s, "ALTER TABLE mysql.columns_priv MODIFY User CHAR(32)")
}

func upgradeToVer20(s CausetNet, ver int64) {
	if ver >= version20 {
		return
	}
	doReentrantdbs(s, CreateStatsFeenoedbackTable)
}

func upgradeToVer21(s CausetNet, ver int64) {
	if ver >= version21 {
		return
	}
	mustExecute(s, CreateGCDeleteRangeDoneTable)

	doReentrantdbs(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX job_id", dbs.ErrCantDropFieldOrKey)
	doReentrantdbs(s, "ALTER TABLE mysql.gc_delete_range ADD UNIQUE INDEX delete_range_index (job_id, element_id)", dbs.ErrDupKeyName)
	doReentrantdbs(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX element_id", dbs.ErrCantDropFieldOrKey)
}

func upgradeToVer22(s CausetNet, ver int64) {
	if ver >= version22 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `stats_ver` bigint(64) NOT NULL DEFAULT 0", schemaReplicant.ErrColumnExists)
}

func upgradeToVer23(s CausetNet, ver int64) {
	if ver >= version23 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `flag` bigint(64) NOT NULL DEFAULT 0", schemaReplicant.ErrColumnExists)
}

// writeSystemTZ writes system timezone info into mysql.milevadb
func writeSystemTZ(s CausetNet) {
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%s", "milevadb Global System Timezone.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		mysql.Systemnoedb, mysql.milevanoedbTable, milevanoedbSystemTZ, timeutil.InferSystemTZ(), timeutil.InferSystemTZ())
	mustExecute(s, sql)
}

// upgradeToVer24 initializes `System` timezone according to docs/design/2018-09-10-adding-tz-env.md
func upgradeToVer24(s CausetNet, ver int64) {
	if ver >= version24 {
		return
	}
	writeSystemTZ(s)
}

// upgradeToVer25 updates milevanoedb_max_soliton_size to new low bound value 32 if previous value is small than 32.
func upgradeToVer25(s CausetNet, ver int64) {
	if ver >= version25 {
		return
	}
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %[1]s.%[2]s SET VARIABLE_VALUE = '%[4]d' WHERE VARIABLE_NAME = '%[3]s' AND VARIABLE_VALUE < %[4]d",
		mysql.Systemnoedb, mysql.GlobalVariablesTable, variable.milevanoedbMaxSolitonsize, variable.DefInitSolitonsize)
	mustExecute(s, sql)
}

func upgradeToVer26(s CausetNet, ver int64) {
	if ver >= version26 {
		return
	}
	mustExecute(s, CreateRoleEdgesTable)
	mustExecute(s, CreateDefaultRolesTable)
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `Create_role_priv` ENUM('N','Y') DEFAULT 'N'", schemaReplicant.ErrColumnExists)
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `Drop_role_priv` ENUM('N','Y') DEFAULT 'N'", schemaReplicant.ErrColumnExists)
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `Account_locked` ENUM('N','Y') DEFAULT 'N'", schemaReplicant.ErrColumnExists)
	// user with Create_user_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_role_priv='Y',Drop_role_priv='Y' WHERE Create_user_priv='Y'")
	// user with Create_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer27(s CausetNet, ver int64) {
	if ver >= version27 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `correlation` double NOT NULL DEFAULT 0", schemaReplicant.ErrColumnExists)
}

func upgradeToVer28(s CausetNet, ver int64) {
	if ver >= version28 {
		return
	}
	doReentrantdbs(s, CreateBindInfoTable)
}

func upgradeToVer29(s CausetNet, ver int64) {
	// upgradeToVer29 only need to be run when the current version is 28.
	if ver != version28 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.bind_info change create_time create_time timestamp(3)")
	doReentrantdbs(s, "ALTER TABLE mysql.bind_info change update_time update_time timestamp(3)")
	doReentrantdbs(s, "ALTER TABLE mysql.bind_info add index sql_index (original_sql(1024),default_noedb(1024))", dbs.ErrDupKeyName)
}

func upgradeToVer30(s CausetNet, ver int64) {
	if ver >= version30 {
		return
	}
	mustExecute(s, CreateStatsTopNTable)
}

func upgradeToVer31(s CausetNet, ver int64) {
	if ver >= version31 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `last_analyze_pos` blob default null", schemaReplicant.ErrColumnExists)
}

func upgradeToVer32(s CausetNet, ver int64) {
	if ver >= version32 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.tables_priv MODIFY table_priv SET('Select','Insert','Update','Delete','Create','Drop','Grant', 'Index', 'Alter', 'Create View', 'Show View', 'Trigger', 'References')")
}

func upgradeToVer33(s CausetNet, ver int64) {
	if ver >= version33 {
		return
	}
	doReentrantdbs(s, CreateExprPushdownBlacklist)
}

func upgradeToVer34(s CausetNet, ver int64) {
	if ver >= version34 {
		return
	}
	doReentrantdbs(s, CreateOptRuleBlacklist)
}

func upgradeToVer35(s CausetNet, ver int64) {
	if ver >= version35 {
		return
	}
	sql := fmt.Sprintf("UPDATE HIGH_PRIORITY %s.%s SET VARIABLE_NAME = '%s' WHERE VARIABLE_NAME = 'milevanoedb_back_off_weight'",
		mysql.Systemnoedb, mysql.GlobalVariablesTable, variable.milevanoedbBackOffWeight)
	mustExecute(s, sql)
}

func upgradeToVer36(s CausetNet, ver int64) {
	if ver >= version36 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `Shutdown_priv` ENUM('N','Y') DEFAULT 'N'", schemaReplicant.ErrColumnExists)
	// A root user will have those privileges after upgrading.
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Shutdown_priv='Y' where Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
}

func upgradeToVer37(s CausetNet, ver int64) {
	if ver >= version37 {
		return
	}
	// when upgrade from old milevadb and no 'milevanoedb_enable_window_function' in GLOBAL_VARIABLES, init it with 0.
	sql := fmt.Sprintf("INSERT IGNORE INTO  %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%d')",
		mysql.Systemnoedb, mysql.GlobalVariablesTable, variable.milevanoedbEnableWindowFunction, 0)
	mustExecute(s, sql)
}

func upgradeToVer38(s CausetNet, ver int64) {
	if ver >= version38 {
		return
	}
	var err error
	_, err = s.Execute(context.Background(), CreateGlobalPrivTable)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer38 error", zap.Error(err))
	}
}

func upgradeToVer39(s CausetNet, ver int64) {
	if ver >= version39 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `Reload_priv` ENUM('N','Y') DEFAULT 'N'", schemaReplicant.ErrColumnExists)
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `File_priv` ENUM('N','Y') DEFAULT 'N'", schemaReplicant.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Reload_priv='Y' where Super_priv='Y'")
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET File_priv='Y' where Super_priv='Y'")
}

func writeNewCollationParameter(s CausetNet, flag bool) {
	comment := "If the new collations are enabled. Do not edit it."
	b := varFalse
	if flag {
		b = varTrue
	}
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", '%s', '%s') ON DUPLICATE KEY UPDATE VARIABLE_VALUE='%s'`,
		mysql.Systemnoedb, mysql.milevanoedbTable, milevanoedbNewCollationEnabled, b, comment, b)
	mustExecute(s, sql)
}

func upgradeToVer40(s CausetNet, ver int64) {
	if ver >= version40 {
		return
	}
	// There is no way to enable new collation for an existing milevadb cluster.
	writeNewCollationParameter(s, false)
}

func upgradeToVer41(s CausetNet, ver int64) {
	if ver >= version41 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.user CHANGE `password` `authentication_string` TEXT", schemaReplicant.ErrColumnExists, schemaReplicant.ErrColumnNotExists)
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `password` TEXT as (`authentication_string`)", schemaReplicant.ErrColumnExists)
}

// writeDefaultExprPushDownBlacklist writes default expr pushdown blacklist into mysql.expr_pushdown_blacklist
func writeDefaultExprPushDownBlacklist(s CausetNet) {
	mustExecute(s, "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES"+
		"('date_add','Noether', 'DST(daylight saving time) does not take effect in Noether date_add'),"+
		"('cast','Noether', 'Behavior of some corner cases(overflow, truncate etc) is different in Noether and milevadb')")
}

func upgradeToVer42(s CausetNet, ver int64) {
	if ver >= version42 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `store_type` char(100) NOT NULL DEFAULT 'tiekv,Noether,milevadb'", schemaReplicant.ErrColumnExists)
	doReentrantdbs(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `reason` varchar(200)", schemaReplicant.ErrColumnExists)
	writeDefaultExprPushDownBlacklist(s)
}

// Convert statement summary global variables to non-empty values.
func writeStmtSummaryVars(s CausetNet) {
	sql := fmt.Sprintf("UPDATE %s.%s SET variable_value='%%s' WHERE variable_name='%%s' AND variable_value=''", mysql.Systemnoedb, mysql.GlobalVariablesTable)
	stmtSummaryConfig := config.GetGlobalConfig().StmtSummary
	mustExecute(s, fmt.Sprintf(sql, variable.BoolToIntStr(stmtSummaryConfig.Enable), variable.milevanoedbEnableStmtSummary))
	mustExecute(s, fmt.Sprintf(sql, variable.BoolToIntStr(stmtSummaryConfig.EnableInternalQuery), variable.milevanoedbStmtSummaryInternalQuery))
	mustExecute(s, fmt.Sprintf(sql, strconv.Itoa(stmtSummaryConfig.RefreshInterval), variable.milevanoedbStmtSummaryRefreshInterval))
	mustExecute(s, fmt.Sprintf(sql, strconv.Itoa(stmtSummaryConfig.HistorySize), variable.milevanoedbStmtSummaryHistorySize))
	mustExecute(s, fmt.Sprintf(sql, strconv.FormatUint(uint64(stmtSummaryConfig.MaxStmtCount), 10), variable.milevanoedbStmtSummaryMaxStmtCount))
	mustExecute(s, fmt.Sprintf(sql, strconv.FormatUint(uint64(stmtSummaryConfig.MaxSQLLength), 10), variable.milevanoedbStmtSummaryMaxSQLLength))
}

func upgradeToVer43(s CausetNet, ver int64) {
	if ver >= version43 {
		return
	}
	writeStmtSummaryVars(s)
}

func upgradeToVer44(s CausetNet, ver int64) {
	if ver >= version44 {
		return
	}
	mustExecute(s, "DELETE FROM mysql.global_variables where variable_name = \"milevanoedb_isolation_read_engines\"")
}

func upgradeToVer45(s CausetNet, ver int64) {
	if ver >= version45 {
		return
	}
	doReentrantdbs(s, "ALTER TABLE mysql.user ADD COLUMN `Config_priv` ENUM('N','Y') DEFAULT 'N'", schemaReplicant.ErrColumnExists)
	mustExecute(s, "UPDATE HIGH_PRIORITY mysql.user SET Config_priv='Y' where Super_priv='Y'")
}

// updateBootstrapVer updates bootstrap version variable in mysql.milevadb table.
func updateBootstrapVer(s CausetNet) {
	// Update bootstrap version.
	sql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%d", "milevadb bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%d"`,
		mysql.Systemnoedb, mysql.milevanoedbTable, milevanoedbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion)
	mustExecute(s, sql)
}

// getBootstrapVersion gets bootstrap version from mysql.milevadb table;
func getBootstrapVersion(s CausetNet) (int64, error) {
	sVal, isNull, err := getmilevanoedbVar(s, milevanoedbServerVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

// dodbsWorks executes dbs statements in bootstrap stage.
func dodbsWorks(s CausetNet) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system DB.
	mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", mysql.Systemnoedb))
	// Create user table.
	mustExecute(s, CreateUserTable)
	// Create privilege blocks.
	mustExecute(s, CreateGlobalPrivTable)
	mustExecute(s, CreatenoedbPrivTable)
	mustExecute(s, CreateTablePrivTable)
	mustExecute(s, CreateColumnPrivTable)
	// Create global system variable table.
	mustExecute(s, CreateGlobalVariablesTable)
	// Create milevadb table.
	mustExecute(s, CreatemilevanoedbTable)
	// Create help table.
	mustExecute(s, CreateHelpTopic)
	// Create stats_meta table.
	mustExecute(s, CreateStatsMetaTable)
	// Create stats_columns table.
	mustExecute(s, CreateStatsColsTable)
	// Create stats_buckets table.
	mustExecute(s, CreateStatsBucketsTable)
	// Create gc_delete_range table.
	mustExecute(s, CreateGCDeleteRangeTable)
	// Create gc_delete_range_done table.
	mustExecute(s, CreateGCDeleteRangeDoneTable)
	// Create stats_feenoedback table.
	mustExecute(s, CreateStatsFeenoedbackTable)
	// Create role_edges table.
	mustExecute(s, CreateRoleEdgesTable)
	// Create default_roles table.
	mustExecute(s, CreateDefaultRolesTable)
	// Create bind_info table.
	mustExecute(s, CreateBindInfoTable)
	// Create stats_topn_store table.
	mustExecute(s, CreateStatsTopNTable)
	// Create expr_pushdown_blacklist table.
	mustExecute(s, CreateExprPushdownBlacklist)
	// Create opt_rule_blacklist table.
	mustExecute(s, CreateOptRuleBlacklist)
}

// doDMLWorks executes DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s CausetNet) {
	mustExecute(s, "BEGIN")

	// Insert a default user with empty password.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO mysql.user VALUES
		("%", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y")`)

	// Init global system variables table.
	values := make([]string, 0, len(variable.SysVars))
	for k, v := range variable.SysVars {
		// CausetNet only variable should not be inserted.
		if v.SINTERLOCKe != variable.SINTERLOCKeCausetNet {
			vVal := v.Value
			if v.Name == variable.milevanoedbTxnMode && config.GetGlobalConfig().Store == "tiekv" {
				vVal = "pessimistic"
			}
			if v.Name == variable.milevanoedbRowFormatVersion {
				vVal = strconv.Itoa(variable.DefmilevanoedbRowFormatV2)
			}
			value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), vVal)
			values = append(values, value)
		}
	}
	sql := fmt.Sprintf("INSERT HIGH_PRIORITY INTO %s.%s VALUES %s;", mysql.Systemnoedb, mysql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%s", "Bootstrap flag. Do not delete.")
		ON DUPLICATE KEY UPDATE VARIABLE_VALUE="%s"`,
		mysql.Systemnoedb, mysql.milevanoedbTable, bootstrappedVar, varTrue, varTrue)
	mustExecute(s, sql)

	sql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%d", "Bootstrap version. Do not delete.")`,
		mysql.Systemnoedb, mysql.milevanoedbTable, milevanoedbServerVersionVar, currentBootstrapVersion)
	mustExecute(s, sql)

	writeSystemTZ(s)

	writeNewCollationParameter(s, config.GetGlobalConfig().NewCollationsEnabledOnFirstBootstrap)

	writeDefaultExprPushDownBlacklist(s)

	writeStmtSummaryVars(s)

	_, err := s.Execute(context.Background(), "COMMIT")
	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("doDMLWorks failed", zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if milevadb is already bootstrapped.
		b, err1 := checkBootstrapped(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err1))
		}
		if b {
			return
		}
		logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err))
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
