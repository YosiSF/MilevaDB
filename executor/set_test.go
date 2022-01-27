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

package executor_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

func (s *testSuite5) TestSetVar(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	testALLEGROSQL := "SET @a = 1;"
	tk.MustExec(testALLEGROSQL)

	testALLEGROSQL = `SET @a = "1";`
	tk.MustExec(testALLEGROSQL)

	testALLEGROSQL = "SET @a = null;"
	tk.MustExec(testALLEGROSQL)

	testALLEGROSQL = "SET @@global.autocommit = 1;"
	tk.MustExec(testALLEGROSQL)

	// TODO: this test case should returns error.
	// testALLEGROSQL = "SET @@global.autocommit = null;"
	// _, err := tk.Exec(testALLEGROSQL)
	// c.Assert(err, NotNil)

	testALLEGROSQL = "SET @@autocommit = 1;"
	tk.MustExec(testALLEGROSQL)

	testALLEGROSQL = "SET @@autocommit = null;"
	_, err := tk.Exec(testALLEGROSQL)
	c.Assert(err, NotNil)

	errTestALLEGROSQL := "SET @@date_format = 1;"
	_, err = tk.Exec(errTestALLEGROSQL)
	c.Assert(err, NotNil)

	errTestALLEGROSQL = "SET @@rewriter_enabled = 1;"
	_, err = tk.Exec(errTestALLEGROSQL)
	c.Assert(err, NotNil)

	errTestALLEGROSQL = "SET xxx = abcd;"
	_, err = tk.Exec(errTestALLEGROSQL)
	c.Assert(err, NotNil)

	errTestALLEGROSQL = "SET @@global.a = 1;"
	_, err = tk.Exec(errTestALLEGROSQL)
	c.Assert(err, NotNil)

	errTestALLEGROSQL = "SET @@global.timestamp = 1;"
	_, err = tk.Exec(errTestALLEGROSQL)
	c.Assert(err, NotNil)

	// For issue 998
	testALLEGROSQL = "SET @issue998a=1, @issue998b=5;"
	tk.MustExec(testALLEGROSQL)
	tk.MustQuery(`select @issue998a, @issue998b;`).Check(testkit.Events("1 5"))
	testALLEGROSQL = "SET @@autocommit=0, @issue998a=2;"
	tk.MustExec(testALLEGROSQL)
	tk.MustQuery(`select @issue998a, @@autocommit;`).Check(testkit.Events("2 0"))
	testALLEGROSQL = "SET @@global.autocommit=1, @issue998b=6;"
	tk.MustExec(testALLEGROSQL)
	tk.MustQuery(`select @issue998b, @@global.autocommit;`).Check(testkit.Events("6 1"))

	// For issue 4302
	testALLEGROSQL = "use test;drop block if exists x;create block x(a int);insert into x value(1);"
	tk.MustExec(testALLEGROSQL)
	testALLEGROSQL = "SET @issue4302=(select a from x limit 1);"
	tk.MustExec(testALLEGROSQL)
	tk.MustQuery(`select @issue4302;`).Check(testkit.Events("1"))

	// Set default
	// {SINTERLOCKeGlobal | SINTERLOCKeStochastik, "low_priority_uFIDelates", "OFF"},
	// For global var
	tk.MustQuery(`select @@global.low_priority_uFIDelates;`).Check(testkit.Events("0"))
	tk.MustExec(`set @@global.low_priority_uFIDelates="ON";`)
	tk.MustQuery(`select @@global.low_priority_uFIDelates;`).Check(testkit.Events("1"))
	tk.MustExec(`set @@global.low_priority_uFIDelates=DEFAULT;`) // It will be set to compiled-in default value.
	tk.MustQuery(`select @@global.low_priority_uFIDelates;`).Check(testkit.Events("0"))
	// For stochastik
	tk.MustQuery(`select @@stochastik.low_priority_uFIDelates;`).Check(testkit.Events("0"))
	tk.MustExec(`set @@global.low_priority_uFIDelates="ON";`)
	tk.MustExec(`set @@stochastik.low_priority_uFIDelates=DEFAULT;`) // It will be set to global var value.
	tk.MustQuery(`select @@stochastik.low_priority_uFIDelates;`).Check(testkit.Events("1"))

	// For allegrosql jdbc driver issue.
	tk.MustQuery(`select @@stochastik.tx_read_only;`).Check(testkit.Events("0"))

	// Test stochastik variable states.
	vars := tk.Se.(stochastikctx.Context).GetStochastikVars()
	tk.Se.CommitTxn(context.TODO())
	tk.MustExec("set @@autocommit = 1")
	c.Assert(vars.InTxn(), IsFalse)
	c.Assert(vars.IsAutocommit(), IsTrue)
	tk.MustExec("set @@autocommit = 0")
	c.Assert(vars.IsAutocommit(), IsFalse)

	tk.MustExec("set @@sql_mode = 'strict_trans_blocks'")
	c.Assert(vars.StrictALLEGROSQLMode, IsTrue)
	tk.MustExec("set @@sql_mode = ''")
	c.Assert(vars.StrictALLEGROSQLMode, IsFalse)

	tk.MustExec("set names utf8")
	charset, defCauslation := vars.GetCharsetInfo()
	c.Assert(charset, Equals, "utf8")
	c.Assert(defCauslation, Equals, "utf8_bin")

	tk.MustExec("set names latin1 defCauslate latin1_swedish_ci")
	charset, defCauslation = vars.GetCharsetInfo()
	c.Assert(charset, Equals, "latin1")
	c.Assert(defCauslation, Equals, "latin1_swedish_ci")

	tk.MustExec("set names utf8 defCauslate default")
	charset, defCauslation = vars.GetCharsetInfo()
	c.Assert(charset, Equals, "utf8")
	c.Assert(defCauslation, Equals, "utf8_bin")

	expectErrMsg := "[dbs:1273]Unknown defCauslation: 'non_exist_defCauslation'"
	tk.MustGetErrMsg("set names utf8 defCauslate non_exist_defCauslation", expectErrMsg)
	tk.MustGetErrMsg("set @@stochastik.defCauslation_server='non_exist_defCauslation'", expectErrMsg)
	tk.MustGetErrMsg("set @@stochastik.defCauslation_database='non_exist_defCauslation'", expectErrMsg)
	tk.MustGetErrMsg("set @@stochastik.defCauslation_connection='non_exist_defCauslation'", expectErrMsg)
	tk.MustGetErrMsg("set @@global.defCauslation_server='non_exist_defCauslation'", expectErrMsg)
	tk.MustGetErrMsg("set @@global.defCauslation_database='non_exist_defCauslation'", expectErrMsg)
	tk.MustGetErrMsg("set @@global.defCauslation_connection='non_exist_defCauslation'", expectErrMsg)

	tk.MustExec("set character_set_results = NULL")
	tk.MustQuery("select @@character_set_results").Check(testkit.Events(""))

	tk.MustExec("set @@stochastik.dbs_slow_threshold=12345")
	tk.MustQuery("select @@stochastik.dbs_slow_threshold").Check(testkit.Events("12345"))
	c.Assert(variable.DBSSlowOprThreshold, Equals, uint32(12345))
	tk.MustExec("set stochastik dbs_slow_threshold=\"54321\"")
	tk.MustQuery("show variables like 'dbs_slow_threshold'").Check(testkit.Events("dbs_slow_threshold 54321"))
	c.Assert(variable.DBSSlowOprThreshold, Equals, uint32(54321))

	// Test set transaction isolation level, which is equivalent to setting variable "tx_isolation".
	tk.MustExec("SET STOCHASTIK TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustQuery("select @@stochastik.tx_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@stochastik.transaction_isolation").Check(testkit.Events("READ-COMMITTED"))
	// error
	_, err = tk.Exec("SET STOCHASTIKTIK TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@stochastik.tx_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@stochastik.transaction_isolation").Check(testkit.Events("READ-COMMITTED"))
	// Fails
	_, err = tk.Exec("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Events("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Events("REPEATABLE-READ"))

	// test synonyms variables
	tk.MustExec("SET STOCHASTIKTIK tx_isolation = 'READ-COMMITTED'")
	tk.MustQuery("select @@stochastik.tx_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@stochastik.transaction_isolation").Check(testkit.Events("READ-COMMITTED"))

	_, err = tk.Exec("SET STOCHASTIKTIK tx_isolation = 'READ-UNCOMMITTED'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@stochastik.tx_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@stochastik.transaction_isolation").Check(testkit.Events("READ-COMMITTED"))

	// fails
	_, err = tk.Exec("SET STOCHASTIKTIK transaction_isolation = 'SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@stochastik.tx_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@stochastik.transaction_isolation").Check(testkit.Events("READ-COMMITTED"))

	// fails
	_, err = tk.Exec("SET GLOBAL transaction_isolation = 'SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Events("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Events("REPEATABLE-READ"))

	_, err = tk.Exec("SET GLOBAL transaction_isolation = 'READ-UNCOMMITTED'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Events("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Events("REPEATABLE-READ"))

	_, err = tk.Exec("SET GLOBAL tx_isolation = 'SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Events("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Events("REPEATABLE-READ"))

	tk.MustExec("SET STOCHASTIKTIK tx_read_only = 1")
	tk.MustExec("SET STOCHASTIKTIK tx_read_only = 0")
	tk.MustQuery("select @@stochastik.tx_read_only").Check(testkit.Events("0"))
	tk.MustQuery("select @@stochastik.transaction_read_only").Check(testkit.Events("0"))

	tk.MustExec("SET GLOBAL tx_read_only = 1")
	tk.MustExec("SET GLOBAL tx_read_only = 0")
	tk.MustQuery("select @@global.tx_read_only").Check(testkit.Events("0"))
	tk.MustQuery("select @@global.transaction_read_only").Check(testkit.Events("0"))

	tk.MustExec("SET STOCHASTIKTIK transaction_read_only = 1")
	tk.MustExec("SET STOCHASTIKTIK transaction_read_only = 0")
	tk.MustQuery("select @@stochastik.tx_read_only").Check(testkit.Events("0"))
	tk.MustQuery("select @@stochastik.transaction_read_only").Check(testkit.Events("0"))

	tk.MustExec("SET STOCHASTIKTIK transaction_read_only = 1")
	tk.MustQuery("select @@stochastik.tx_read_only").Check(testkit.Events("1"))
	tk.MustQuery("select @@stochastik.transaction_read_only").Check(testkit.Events("1"))

	tk.MustExec("SET GLOBAL transaction_read_only = 1")
	tk.MustExec("SET GLOBAL transaction_read_only = 0")
	tk.MustQuery("select @@global.tx_read_only").Check(testkit.Events("0"))
	tk.MustQuery("select @@global.transaction_read_only").Check(testkit.Events("0"))

	tk.MustExec("SET GLOBAL transaction_read_only = 1")
	tk.MustQuery("select @@global.tx_read_only").Check(testkit.Events("1"))
	tk.MustQuery("select @@global.transaction_read_only").Check(testkit.Events("1"))

	// Even the transaction fail, set stochastik variable would success.
	tk.MustExec("BEGIN")
	tk.MustExec("SET STOCHASTIKTIK TRANSACTION ISOLATION LEVEL READ COMMITTED")
	_, err = tk.Exec(`INSERT INTO t VALUES ("sdfsdf")`)
	c.Assert(err, NotNil)
	tk.MustExec("COMMIT")
	tk.MustQuery("select @@stochastik.tx_isolation").Check(testkit.Events("READ-COMMITTED"))

	tk.MustExec("set global avoid_temporal_upgrade = on")
	tk.MustQuery(`select @@global.avoid_temporal_upgrade;`).Check(testkit.Events("1"))
	tk.MustExec("set @@global.avoid_temporal_upgrade = off")
	tk.MustQuery(`select @@global.avoid_temporal_upgrade;`).Check(testkit.Events("0"))
	tk.MustExec("set stochastik sql_log_bin = on")
	tk.MustQuery(`select @@stochastik.sql_log_bin;`).Check(testkit.Events("1"))
	tk.MustExec("set sql_log_bin = off")
	tk.MustQuery(`select @@stochastik.sql_log_bin;`).Check(testkit.Events("0"))
	tk.MustExec("set @@sql_log_bin = on")
	tk.MustQuery(`select @@stochastik.sql_log_bin;`).Check(testkit.Events("1"))

	tk.MustQuery(`select @@global.log_bin;`).Check(testkit.Events(variable.BoolToIntStr(config.GetGlobalConfig().Binlog.Enable)))
	tk.MustQuery(`select @@log_bin;`).Check(testkit.Events(variable.BoolToIntStr(config.GetGlobalConfig().Binlog.Enable)))

	tk.MustExec("set @@milevadb_general_log = 1")
	tk.MustExec("set @@milevadb_general_log = 0")

	tk.MustExec("set @@milevadb_pprof_sql_cpu = 1")
	tk.MustExec("set @@milevadb_pprof_sql_cpu = 0")

	tk.MustExec(`set milevadb_force_priority = "no_priority"`)
	tk.MustQuery(`select @@milevadb_force_priority;`).Check(testkit.Events("NO_PRIORITY"))
	tk.MustExec(`set milevadb_force_priority = "low_priority"`)
	tk.MustQuery(`select @@milevadb_force_priority;`).Check(testkit.Events("LOW_PRIORITY"))
	tk.MustExec(`set milevadb_force_priority = "high_priority"`)
	tk.MustQuery(`select @@milevadb_force_priority;`).Check(testkit.Events("HIGH_PRIORITY"))
	tk.MustExec(`set milevadb_force_priority = "delayed"`)
	tk.MustQuery(`select @@milevadb_force_priority;`).Check(testkit.Events("DELAYED"))
	tk.MustExec(`set milevadb_force_priority = "abc"`)
	tk.MustQuery(`select @@milevadb_force_priority;`).Check(testkit.Events("NO_PRIORITY"))
	_, err = tk.Exec(`set global milevadb_force_priority = ""`)
	c.Assert(err, NotNil)

	tk.MustExec("set milevadb_constraint_check_in_place = 1")
	tk.MustQuery(`select @@stochastik.milevadb_constraint_check_in_place;`).Check(testkit.Events("1"))
	tk.MustExec("set global milevadb_constraint_check_in_place = 0")
	tk.MustQuery(`select @@global.milevadb_constraint_check_in_place;`).Check(testkit.Events("0"))

	tk.MustExec("set milevadb_batch_commit = 0")
	tk.MustQuery("select @@stochastik.milevadb_batch_commit;").Check(testkit.Events("0"))
	tk.MustExec("set milevadb_batch_commit = 1")
	tk.MustQuery("select @@stochastik.milevadb_batch_commit;").Check(testkit.Events("1"))
	_, err = tk.Exec("set global milevadb_batch_commit = 0")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set global milevadb_batch_commit = 2")
	c.Assert(err, NotNil)

	// test skip isolation level check: init
	tk.MustExec("SET GLOBAL milevadb_skip_isolation_level_check = 0")
	tk.MustExec("SET STOCHASTIKTIK milevadb_skip_isolation_level_check = 0")
	tk.MustExec("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustExec("SET STOCHASTIKTIK TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@stochastik.tx_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@stochastik.transaction_isolation").Check(testkit.Events("READ-COMMITTED"))

	// test skip isolation level check: error
	_, err = tk.Exec("SET STOCHASTIKTIK TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@stochastik.tx_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@stochastik.transaction_isolation").Check(testkit.Events("READ-COMMITTED"))

	_, err = tk.Exec("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Events("READ-COMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Events("READ-COMMITTED"))

	// test skip isolation level check: success
	tk.MustExec("SET GLOBAL milevadb_skip_isolation_level_check = 1")
	tk.MustExec("SET STOCHASTIKTIK milevadb_skip_isolation_level_check = 1")
	tk.MustExec("SET STOCHASTIKTIK TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	tk.MustQuery("show warnings").Check(testkit.Events(
		"Warning 8048 The isolation level 'SERIALIZABLE' is not supported. Set milevadb_skip_isolation_level_check=1 to skip this error"))
	tk.MustQuery("select @@stochastik.tx_isolation").Check(testkit.Events("SERIALIZABLE"))
	tk.MustQuery("select @@stochastik.transaction_isolation").Check(testkit.Events("SERIALIZABLE"))

	// test skip isolation level check: success
	tk.MustExec("SET GLOBAL milevadb_skip_isolation_level_check = 0")
	tk.MustExec("SET STOCHASTIKTIK milevadb_skip_isolation_level_check = 1")
	tk.MustExec("SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	tk.MustQuery("show warnings").Check(testkit.Events(
		"Warning 8048 The isolation level 'READ-UNCOMMITTED' is not supported. Set milevadb_skip_isolation_level_check=1 to skip this error"))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Events("READ-UNCOMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Events("READ-UNCOMMITTED"))

	// test skip isolation level check: reset
	tk.MustExec("SET GLOBAL transaction_isolation='REPEATABLE-READ'") // should reset tx_isolation back to rr before reset milevadb_skip_isolation_level_check
	tk.MustExec("SET GLOBAL milevadb_skip_isolation_level_check = 0")
	tk.MustExec("SET STOCHASTIKTIK milevadb_skip_isolation_level_check = 0")

	tk.MustExec("set global read_only = 0")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Events("0"))
	tk.MustExec("set global read_only = off")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Events("0"))
	tk.MustExec("set global read_only = 1")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Events("1"))
	tk.MustExec("set global read_only = on")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Events("1"))
	_, err = tk.Exec("set global read_only = abc")
	c.Assert(err, NotNil)

	// test for milevadb_wait_split_region_finish
	tk.MustQuery(`select @@stochastik.milevadb_wait_split_region_finish;`).Check(testkit.Events("1"))
	tk.MustExec("set milevadb_wait_split_region_finish = 1")
	tk.MustQuery(`select @@stochastik.milevadb_wait_split_region_finish;`).Check(testkit.Events("1"))
	tk.MustExec("set milevadb_wait_split_region_finish = 0")
	tk.MustQuery(`select @@stochastik.milevadb_wait_split_region_finish;`).Check(testkit.Events("0"))

	// test for milevadb_scatter_region
	tk.MustQuery(`select @@global.milevadb_scatter_region;`).Check(testkit.Events("0"))
	tk.MustExec("set global milevadb_scatter_region = 1")
	tk.MustQuery(`select @@global.milevadb_scatter_region;`).Check(testkit.Events("1"))
	tk.MustExec("set global milevadb_scatter_region = 0")
	tk.MustQuery(`select @@global.milevadb_scatter_region;`).Check(testkit.Events("0"))
	_, err = tk.Exec("set stochastik milevadb_scatter_region = 0")
	c.Assert(err, NotNil)
	_, err = tk.Exec(`select @@stochastik.milevadb_scatter_region;`)
	c.Assert(err, NotNil)

	// test for milevadb_wait_split_region_timeout
	tk.MustQuery(`select @@stochastik.milevadb_wait_split_region_timeout;`).Check(testkit.Events(strconv.Itoa(variable.DefWaitSplitRegionTimeout)))
	tk.MustExec("set milevadb_wait_split_region_timeout = 1")
	tk.MustQuery(`select @@stochastik.milevadb_wait_split_region_timeout;`).Check(testkit.Events("1"))
	_, err = tk.Exec("set milevadb_wait_split_region_timeout = 0")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "milevadb_wait_split_region_timeout(0) cannot be smaller than 1")
	tk.MustQuery(`select @@stochastik.milevadb_wait_split_region_timeout;`).Check(testkit.Events("1"))

	tk.MustExec("set stochastik milevadb_backoff_weight = 3")
	tk.MustQuery("select @@stochastik.milevadb_backoff_weight;").Check(testkit.Events("3"))
	tk.MustExec("set stochastik milevadb_backoff_weight = 20")
	tk.MustQuery("select @@stochastik.milevadb_backoff_weight;").Check(testkit.Events("20"))
	_, err = tk.Exec("set stochastik milevadb_backoff_weight = -1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set global milevadb_backoff_weight = 0")
	c.Assert(err, NotNil)
	tk.MustExec("set global milevadb_backoff_weight = 10")
	tk.MustQuery("select @@global.milevadb_backoff_weight;").Check(testkit.Events("10"))

	tk.MustExec("set @@milevadb_expensive_query_time_threshold=70")
	tk.MustQuery("select @@milevadb_expensive_query_time_threshold;").Check(testkit.Events("70"))

	tk.MustQuery("select @@milevadb_store_limit;").Check(testkit.Events("0"))
	tk.MustExec("set @@milevadb_store_limit = 100")
	tk.MustQuery("select @@milevadb_store_limit;").Check(testkit.Events("100"))
	tk.MustQuery("select @@stochastik.milevadb_store_limit;").Check(testkit.Events("100"))
	tk.MustQuery("select @@global.milevadb_store_limit;").Check(testkit.Events("0"))
	tk.MustExec("set @@milevadb_store_limit = 0")

	tk.MustExec("set global milevadb_store_limit = 100")
	tk.MustQuery("select @@milevadb_store_limit;").Check(testkit.Events("0"))
	tk.MustQuery("select @@stochastik.milevadb_store_limit;").Check(testkit.Events("0"))
	tk.MustQuery("select @@global.milevadb_store_limit;").Check(testkit.Events("100"))

	tk.MustQuery("select @@milevadb_enable_change_defCausumn_type;").Check(testkit.Events("0"))
	tk.MustExec("set global milevadb_enable_change_defCausumn_type = 1")
	tk.MustQuery("select @@milevadb_enable_change_defCausumn_type;").Check(testkit.Events("1"))
	tk.MustExec("set global milevadb_enable_change_defCausumn_type = off")
	tk.MustQuery("select @@milevadb_enable_change_defCausumn_type;").Check(testkit.Events("0"))

	tk.MustQuery("select @@stochastik.milevadb_metric_query_step;").Check(testkit.Events("60"))
	tk.MustExec("set @@stochastik.milevadb_metric_query_step = 120")
	_, err = tk.Exec("set @@stochastik.milevadb_metric_query_step = 9")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "milevadb_metric_query_step(9) cannot be smaller than 10 or larger than 216000")
	tk.MustQuery("select @@stochastik.milevadb_metric_query_step;").Check(testkit.Events("120"))

	tk.MustQuery("select @@stochastik.milevadb_metric_query_range_duration;").Check(testkit.Events("60"))
	tk.MustExec("set @@stochastik.milevadb_metric_query_range_duration = 120")
	_, err = tk.Exec("set @@stochastik.milevadb_metric_query_range_duration = 9")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "milevadb_metric_query_range_duration(9) cannot be smaller than 10 or larger than 216000")
	tk.MustQuery("select @@stochastik.milevadb_metric_query_range_duration;").Check(testkit.Events("120"))

	// test for milevadb_slow_log_masking
	tk.MustQuery(`select @@global.milevadb_slow_log_masking;`).Check(testkit.Events("0"))
	tk.MustExec("set global milevadb_slow_log_masking = 1")
	tk.MustQuery(`select @@global.milevadb_slow_log_masking;`).Check(testkit.Events("1"))
	tk.MustExec("set global milevadb_slow_log_masking = 0")
	tk.MustQuery(`select @@global.milevadb_slow_log_masking;`).Check(testkit.Events("0"))
	_, err = tk.Exec("set stochastik milevadb_slow_log_masking = 0")
	c.Assert(err, NotNil)
	_, err = tk.Exec(`select @@stochastik.milevadb_slow_log_masking;`)
	c.Assert(err, NotNil)

	tk.MustQuery("select @@milevadb_dml_batch_size;").Check(testkit.Events("0"))
	tk.MustExec("set @@stochastik.milevadb_dml_batch_size = 120")
	tk.MustQuery("select @@milevadb_dml_batch_size;").Check(testkit.Events("120"))
	c.Assert(tk.ExecToErr("set @@stochastik.milevadb_dml_batch_size = -120"), NotNil)
	c.Assert(tk.ExecToErr("set @@global.milevadb_dml_batch_size = 200"), IsNil)    // now permitted due to MilevaDB #19809
	tk.MustQuery("select @@milevadb_dml_batch_size;").Check(testkit.Events("120")) // global only applies to new stochastik

	_, err = tk.Exec("set milevadb_enable_parallel_apply=-1")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue)
}

func (s *testSuite5) TestTruncateIncorrectIntStochastikVar(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	testCases := []struct {
		stochastikVarName string
		minValue          int
		maxValue          int
	}{
		{"auto_increment_increment", 1, 65535},
		{"auto_increment_offset", 1, 65535},
	}

	for _, tc := range testCases {
		name := tc.stochastikVarName
		selectALLEGROSQL := fmt.Sprintf("select @@%s;", name)
		validValue := tc.minValue + (tc.maxValue-tc.minValue)/2
		tk.MustExec(fmt.Sprintf("set @@%s = %d", name, validValue))
		tk.MustQuery(selectALLEGROSQL).Check(testkit.Events(fmt.Sprintf("%d", validValue)))

		tk.MustExec(fmt.Sprintf("set @@%s = %d", name, tc.minValue-1))
		warnMsg := fmt.Sprintf("Warning 1292 Truncated incorrect %s value: '%d'", name, tc.minValue-1)
		tk.MustQuery("show warnings").Check(testkit.Events(warnMsg))
		tk.MustQuery(selectALLEGROSQL).Check(testkit.Events(fmt.Sprintf("%d", tc.minValue)))

		tk.MustExec(fmt.Sprintf("set @@%s = %d", name, tc.maxValue+1))
		warnMsg = fmt.Sprintf("Warning 1292 Truncated incorrect %s value: '%d'", name, tc.maxValue+1)
		tk.MustQuery("show warnings").Check(testkit.Events(warnMsg))
		tk.MustQuery(selectALLEGROSQL).Check(testkit.Events(fmt.Sprintf("%d", tc.maxValue)))
	}
}

func (s *testSuite5) TestSetCharset(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	ctx := tk.Se.(stochastikctx.Context)
	stochastikVars := ctx.GetStochastikVars()

	var characterSetVariables = []string{
		"character_set_client",
		"character_set_connection",
		"character_set_results",
		"character_set_server",
		"character_set_database",
		"character_set_system",
		"character_set_filesystem",
	}

	check := func(args ...string) {
		for i, v := range characterSetVariables {
			sVar, err := variable.GetStochastikSystemVar(stochastikVars, v)
			c.Assert(err, IsNil)
			c.Assert(sVar, Equals, args[i], Commentf("%d: %s", i, characterSetVariables[i]))
		}
	}

	check(
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET NAMES latin1`)
	check(
		"latin1",
		"latin1",
		"latin1",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET NAMES default`)
	check(
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	// Issue #1523
	tk.MustExec(`SET NAMES binary`)
	check(
		"binary",
		"binary",
		"binary",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET NAMES utf8`)
	check(
		"utf8",
		"utf8",
		"utf8",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET CHARACTER SET latin1`)
	check(
		"latin1",
		"utf8mb4",
		"latin1",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)

	tk.MustExec(`SET CHARACTER SET default`)
	check(
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8mb4",
		"utf8",
		"binary",
	)
}

func (s *testSuite5) TestValidateSetVar(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	_, err := tk.Exec("set global milevadb_distsql_scan_concurrency='fff';")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set global milevadb_distsql_scan_concurrency=-2;")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@milevadb_distsql_scan_concurrency='fff';")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@milevadb_distsql_scan_concurrency=-2;")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@milevadb_batch_delete='ok';")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@milevadb_batch_delete='On';")
	tk.MustQuery("select @@milevadb_batch_delete;").Check(testkit.Events("1"))
	tk.MustExec("set @@milevadb_batch_delete='oFf';")
	tk.MustQuery("select @@milevadb_batch_delete;").Check(testkit.Events("0"))
	tk.MustExec("set @@milevadb_batch_delete=1;")
	tk.MustQuery("select @@milevadb_batch_delete;").Check(testkit.Events("1"))
	tk.MustExec("set @@milevadb_batch_delete=0;")
	tk.MustQuery("select @@milevadb_batch_delete;").Check(testkit.Events("0"))

	tk.MustExec("set @@milevadb_opt_agg_push_down=off;")
	tk.MustQuery("select @@milevadb_opt_agg_push_down;").Check(testkit.Events("0"))

	tk.MustExec("set @@milevadb_constraint_check_in_place=on;")
	tk.MustQuery("select @@milevadb_constraint_check_in_place;").Check(testkit.Events("1"))

	tk.MustExec("set @@milevadb_general_log=0;")
	tk.MustQuery("select @@milevadb_general_log;").Check(testkit.Events("0"))

	tk.MustExec("set @@milevadb_pprof_sql_cpu=1;")
	tk.MustQuery("select @@milevadb_pprof_sql_cpu;").Check(testkit.Events("1"))
	tk.MustExec("set @@milevadb_pprof_sql_cpu=0;")
	tk.MustQuery("select @@milevadb_pprof_sql_cpu;").Check(testkit.Events("0"))

	tk.MustExec("set @@milevadb_enable_streaming=1;")
	tk.MustQuery("select @@milevadb_enable_streaming;").Check(testkit.Events("1"))

	_, err = tk.Exec("set @@milevadb_batch_delete=3;")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@group_concat_max_len=1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect group_concat_max_len value: '1'"))
	result := tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Events("4"))

	_, err = tk.Exec("set @@group_concat_max_len = 18446744073709551616")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	// Test illegal type
	_, err = tk.Exec("set @@group_concat_max_len='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@default_week_format=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect default_week_format value: '-1'"))
	result = tk.MustQuery("select @@default_week_format;")
	result.Check(testkit.Events("0"))

	tk.MustExec("set @@default_week_format=9")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect default_week_format value: '9'"))
	result = tk.MustQuery("select @@default_week_format;")
	result.Check(testkit.Events("7"))

	_, err = tk.Exec("set @@error_count = 0")
	c.Assert(terror.ErrorEqual(err, variable.ErrReadOnly), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@warning_count = 0")
	c.Assert(terror.ErrorEqual(err, variable.ErrReadOnly), IsTrue, Commentf("err %v", err))

	tk.MustExec("set time_zone='SySTeM'")
	result = tk.MustQuery("select @@time_zone;")
	result.Check(testkit.Events("SYSTEM"))

	// The following cases test value out of range and illegal type when setting system variables.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/server-system-variables.html for more details.
	tk.MustExec("set @@global.max_connections=100001")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '100001'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Events("100000"))

	tk.MustExec("set @@global.max_connections=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '-1'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Events("1"))

	_, err = tk.Exec("set @@global.max_connections='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.thread_pool_size=65")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect thread_pool_size value: '65'"))
	result = tk.MustQuery("select @@global.thread_pool_size;")
	result.Check(testkit.Events("64"))

	tk.MustExec("set @@global.thread_pool_size=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect thread_pool_size value: '-1'"))
	result = tk.MustQuery("select @@global.thread_pool_size;")
	result.Check(testkit.Events("1"))

	_, err = tk.Exec("set @@global.thread_pool_size='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.max_allowed_packet=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect max_allowed_packet value: '-1'"))
	result = tk.MustQuery("select @@global.max_allowed_packet;")
	result.Check(testkit.Events("1024"))

	_, err = tk.Exec("set @@global.max_allowed_packet='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.max_connect_errors=18446744073709551615")

	tk.MustExec("set @@global.max_connect_errors=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect max_connect_errors value: '-1'"))
	result = tk.MustQuery("select @@global.max_connect_errors;")
	result.Check(testkit.Events("1"))

	_, err = tk.Exec("set @@global.max_connect_errors=18446744073709551616")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.max_connections=100001")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '100001'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Events("100000"))

	tk.MustExec("set @@global.max_connections=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '-1'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Events("1"))

	_, err = tk.Exec("set @@global.max_connections='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@max_sort_length=1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect max_sort_length value: '1'"))
	result = tk.MustQuery("select @@max_sort_length;")
	result.Check(testkit.Events("4"))

	tk.MustExec("set @@max_sort_length=-100")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect max_sort_length value: '-100'"))
	result = tk.MustQuery("select @@max_sort_length;")
	result.Check(testkit.Events("4"))

	tk.MustExec("set @@max_sort_length=8388609")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect max_sort_length value: '8388609'"))
	result = tk.MustQuery("select @@max_sort_length;")
	result.Check(testkit.Events("8388608"))

	_, err = tk.Exec("set @@max_sort_length='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.block_definition_cache=399")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect block_definition_cache value: '399'"))
	result = tk.MustQuery("select @@global.block_definition_cache;")
	result.Check(testkit.Events("400"))

	tk.MustExec("set @@global.block_definition_cache=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect block_definition_cache value: '-1'"))
	result = tk.MustQuery("select @@global.block_definition_cache;")
	result.Check(testkit.Events("400"))

	tk.MustExec("set @@global.block_definition_cache=524289")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect block_definition_cache value: '524289'"))
	result = tk.MustQuery("select @@global.block_definition_cache;")
	result.Check(testkit.Events("524288"))

	_, err = tk.Exec("set @@global.block_definition_cache='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@old_passwords=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect old_passwords value: '-1'"))
	result = tk.MustQuery("select @@old_passwords;")
	result.Check(testkit.Events("0"))

	tk.MustExec("set @@old_passwords=3")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect old_passwords value: '3'"))
	result = tk.MustQuery("select @@old_passwords;")
	result.Check(testkit.Events("2"))

	_, err = tk.Exec("set @@old_passwords='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@tmp_block_size=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect tmp_block_size value: '-1'"))
	result = tk.MustQuery("select @@tmp_block_size;")
	result.Check(testkit.Events("1024"))

	tk.MustExec("set @@tmp_block_size=1020")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect tmp_block_size value: '1020'"))
	result = tk.MustQuery("select @@tmp_block_size;")
	result.Check(testkit.Events("1024"))

	tk.MustExec("set @@tmp_block_size=167772161")
	result = tk.MustQuery("select @@tmp_block_size;")
	result.Check(testkit.Events("167772161"))

	tk.MustExec("set @@tmp_block_size=18446744073709551615")
	result = tk.MustQuery("select @@tmp_block_size;")
	result.Check(testkit.Events("18446744073709551615"))

	_, err = tk.Exec("set @@tmp_block_size=18446744073709551616")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	_, err = tk.Exec("set @@tmp_block_size='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue)

	tk.MustExec("set @@global.connect_timeout=1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect connect_timeout value: '1'"))
	result = tk.MustQuery("select @@global.connect_timeout;")
	result.Check(testkit.Events("2"))

	tk.MustExec("set @@global.connect_timeout=31536000")
	result = tk.MustQuery("select @@global.connect_timeout;")
	result.Check(testkit.Events("31536000"))

	tk.MustExec("set @@global.connect_timeout=31536001")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect connect_timeout value: '31536001'"))
	result = tk.MustQuery("select @@global.connect_timeout;")
	result.Check(testkit.Events("31536000"))

	result = tk.MustQuery("select @@sql_select_limit;")
	result.Check(testkit.Events("18446744073709551615"))
	tk.MustExec("set @@sql_select_limit=default")
	result = tk.MustQuery("select @@sql_select_limit;")
	result.Check(testkit.Events("18446744073709551615"))

	tk.MustExec("set @@sql_auto_is_null=00")
	result = tk.MustQuery("select @@sql_auto_is_null;")
	result.Check(testkit.Events("0"))

	tk.MustExec("set @@sql_warnings=001")
	result = tk.MustQuery("select @@sql_warnings;")
	result.Check(testkit.Events("1"))

	tk.MustExec("set @@sql_warnings=000")
	result = tk.MustQuery("select @@sql_warnings;")
	result.Check(testkit.Events("0"))

	tk.MustExec("set @@global.super_read_only=-0")
	result = tk.MustQuery("select @@global.super_read_only;")
	result.Check(testkit.Events("0"))

	_, err = tk.Exec("set @@global.super_read_only=-1")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@global.innodb_status_output_locks=-1")
	result = tk.MustQuery("select @@global.innodb_status_output_locks;")
	result.Check(testkit.Events("1"))

	tk.MustExec("set @@global.innodb_ft_enable_stopword=0000000")
	result = tk.MustQuery("select @@global.innodb_ft_enable_stopword;")
	result.Check(testkit.Events("0"))

	tk.MustExec("set @@global.innodb_stats_on_metadata=1")
	result = tk.MustQuery("select @@global.innodb_stats_on_metadata;")
	result.Check(testkit.Events("1"))

	tk.MustExec("set @@global.innodb_file_per_block=-50")
	result = tk.MustQuery("select @@global.innodb_file_per_block;")
	result.Check(testkit.Events("1"))

	_, err = tk.Exec("set @@global.innodb_ft_enable_stopword=2")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@query_cache_type=0")
	result = tk.MustQuery("select @@query_cache_type;")
	result.Check(testkit.Events("OFF"))

	tk.MustExec("set @@query_cache_type=2")
	result = tk.MustQuery("select @@query_cache_type;")
	result.Check(testkit.Events("DEMAND"))

	tk.MustExec("set @@global.sync_binlog=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect sync_binlog value: '-1'"))

	tk.MustExec("set @@global.sync_binlog=4294967299")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect sync_binlog value: '4294967299'"))

	tk.MustExec("set @@global.flush_time=31536001")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect flush_time value: '31536001'"))

	tk.MustExec("set @@global.interactive_timeout=31536001")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect interactive_timeout value: '31536001'"))

	tk.MustExec("set @@global.innodb_commit_concurrency = -1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect innodb_commit_concurrency value: '-1'"))

	tk.MustExec("set @@global.innodb_commit_concurrency = 1001")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect innodb_commit_concurrency value: '1001'"))

	tk.MustExec("set @@global.innodb_fast_shutdown = -1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect innodb_fast_shutdown value: '-1'"))

	tk.MustExec("set @@global.innodb_fast_shutdown = 3")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect innodb_fast_shutdown value: '3'"))

	tk.MustExec("set @@global.innodb_lock_wait_timeout = 0")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '0'"))

	tk.MustExec("set @@global.innodb_lock_wait_timeout = 1073741825")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '1073741825'"))

	tk.MustExec("set @@innodb_lock_wait_timeout = 0")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '0'"))

	tk.MustExec("set @@innodb_lock_wait_timeout = 1073741825")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '1073741825'"))

	tk.MustExec("set @@global.validate_password_number_count=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect validate_password_number_count value: '-1'"))

	tk.MustExec("set @@global.validate_password_length=-1")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Warning|1292|Truncated incorrect validate_password_length value: '-1'"))

	tk.MustExec("set @@global.validate_password_length=8")
	tk.MustQuery("show warnings").Check(testkit.Events())

	_, err = tk.Exec("set @@tx_isolation=''")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set global tx_isolation=''")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set @@transaction_isolation=''")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set global transaction_isolation=''")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("set global tx_isolation='REPEATABLE-READ1'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@tx_isolation='READ-COMMITTED'")
	result = tk.MustQuery("select @@tx_isolation;")
	result.Check(testkit.Events("READ-COMMITTED"))

	tk.MustExec("set @@tx_isolation='read-COMMITTED'")
	result = tk.MustQuery("select @@tx_isolation;")
	result.Check(testkit.Events("READ-COMMITTED"))

	tk.MustExec("set @@tx_isolation='REPEATABLE-READ'")
	result = tk.MustQuery("select @@tx_isolation;")
	result.Check(testkit.Events("REPEATABLE-READ"))

	tk.MustExec("SET GLOBAL milevadb_skip_isolation_level_check = 0")
	tk.MustExec("SET STOCHASTIKTIK milevadb_skip_isolation_level_check = 0")
	_, err = tk.Exec("set @@tx_isolation='SERIALIZABLE'")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), IsTrue, Commentf("err %v", err))

	tk.MustExec("set global allow_auto_random_explicit_insert=on;")
	tk.MustQuery("select @@global.allow_auto_random_explicit_insert;").Check(testkit.Events("1"))
}

func (s *testSuite5) TestSelectGlobalVar(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustQuery("select @@global.max_connections;").Check(testkit.Events("151"))
	tk.MustQuery("select @@max_connections;").Check(testkit.Events("151"))

	tk.MustExec("set @@global.max_connections=100;")

	tk.MustQuery("select @@global.max_connections;").Check(testkit.Events("100"))
	tk.MustQuery("select @@max_connections;").Check(testkit.Events("100"))

	tk.MustExec("set @@global.max_connections=151;")

	// test for unknown variable.
	err := tk.ExecToErr("select @@invalid")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnknownSystemVar), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select @@global.invalid")
	c.Assert(terror.ErrorEqual(err, variable.ErrUnknownSystemVar), IsTrue, Commentf("err %v", err))
}

func (s *testSuite5) TestSetConcurrency(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	// test default value
	tk.MustQuery("select @@milevadb_executor_concurrency;").Check(testkit.Events(strconv.Itoa(variable.DefExecutorConcurrency)))

	tk.MustQuery("select @@milevadb_index_lookup_concurrency;").Check(testkit.Events(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@milevadb_index_lookup_join_concurrency;").Check(testkit.Events(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@milevadb_hash_join_concurrency;").Check(testkit.Events(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@milevadb_hashagg_partial_concurrency;").Check(testkit.Events(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@milevadb_hashagg_final_concurrency;").Check(testkit.Events(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@milevadb_window_concurrency;").Check(testkit.Events(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@milevadb_projection_concurrency;").Check(testkit.Events(strconv.Itoa(variable.ConcurrencyUnset)))
	tk.MustQuery("select @@milevadb_distsql_scan_concurrency;").Check(testkit.Events(strconv.Itoa(variable.DefDistALLEGROSQLScanConcurrency)))

	tk.MustQuery("select @@milevadb_index_serial_scan_concurrency;").Check(testkit.Events(strconv.Itoa(variable.DefIndexSerialScanConcurrency)))

	vars := tk.Se.(stochastikctx.Context).GetStochastikVars()
	c.Assert(vars.ExecutorConcurrency, Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.IndexLookupConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashJoinConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashAggPartialConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashAggFinalConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.ProjectionConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.DistALLEGROSQLScanConcurrency(), Equals, variable.DefDistALLEGROSQLScanConcurrency)

	c.Assert(vars.IndexSerialScanConcurrency(), Equals, variable.DefIndexSerialScanConcurrency)

	// test setting deprecated variables
	warnTpl := "Warning 1287 '%s' is deprecated and will be removed in a future release. Please use milevadb_executor_concurrency instead"

	checkSet := func(v string) {
		tk.MustExec(fmt.Sprintf("set @@%s=1;", v))
		tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", fmt.Sprintf(warnTpl, v)))
		tk.MustQuery(fmt.Sprintf("select @@%s;", v)).Check(testkit.Events("1"))
	}

	checkSet(variable.MilevaDBIndexLookupConcurrency)
	c.Assert(vars.IndexLookupConcurrency(), Equals, 1)

	checkSet(variable.MilevaDBIndexLookupJoinConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency(), Equals, 1)

	checkSet(variable.MilevaDBHashJoinConcurrency)
	c.Assert(vars.HashJoinConcurrency(), Equals, 1)

	checkSet(variable.MilevaDBHashAggPartialConcurrency)
	c.Assert(vars.HashAggPartialConcurrency(), Equals, 1)

	checkSet(variable.MilevaDBHashAggFinalConcurrency)
	c.Assert(vars.HashAggFinalConcurrency(), Equals, 1)

	checkSet(variable.MilevaDBProjectionConcurrency)
	c.Assert(vars.ProjectionConcurrency(), Equals, 1)

	checkSet(variable.MilevaDBWindowConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, 1)

	tk.MustExec(fmt.Sprintf("set @@%s=1;", variable.MilevaDBDistALLEGROSQLScanConcurrency))
	tk.MustQuery(fmt.Sprintf("select @@%s;", variable.MilevaDBDistALLEGROSQLScanConcurrency)).Check(testkit.Events("1"))
	c.Assert(vars.DistALLEGROSQLScanConcurrency(), Equals, 1)

	tk.MustExec("set @@milevadb_index_serial_scan_concurrency=4")
	tk.MustQuery("show warnings").Check(testkit.Events())
	tk.MustQuery("select @@milevadb_index_serial_scan_concurrency;").Check(testkit.Events("4"))
	c.Assert(vars.IndexSerialScanConcurrency(), Equals, 4)

	// test setting deprecated value unset
	tk.MustExec("set @@milevadb_index_lookup_concurrency=-1;")
	tk.MustExec("set @@milevadb_index_lookup_join_concurrency=-1;")
	tk.MustExec("set @@milevadb_hash_join_concurrency=-1;")
	tk.MustExec("set @@milevadb_hashagg_partial_concurrency=-1;")
	tk.MustExec("set @@milevadb_hashagg_final_concurrency=-1;")
	tk.MustExec("set @@milevadb_window_concurrency=-1;")
	tk.MustExec("set @@milevadb_projection_concurrency=-1;")

	c.Assert(vars.IndexLookupConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashJoinConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashAggPartialConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.HashAggFinalConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, variable.DefExecutorConcurrency)
	c.Assert(vars.ProjectionConcurrency(), Equals, variable.DefExecutorConcurrency)

	_, err := tk.Exec("set @@milevadb_executor_concurrency=-1;")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))
}

func (s *testSuite5) TestEnableNoopFunctionsVar(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	// test for milevadb_enable_noop_functions
	tk.MustQuery(`select @@global.milevadb_enable_noop_functions;`).Check(testkit.Events("0"))
	tk.MustQuery(`select @@milevadb_enable_noop_functions;`).Check(testkit.Events("0"))

	_, err := tk.Exec(`select get_lock('lock1', 2);`)
	c.Assert(terror.ErrorEqual(err, expression.ErrFunctionsNoopImpl), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec(`select release_lock('lock1');`)
	c.Assert(terror.ErrorEqual(err, expression.ErrFunctionsNoopImpl), IsTrue, Commentf("err %v", err))

	// change stochastik var to 1
	tk.MustExec(`set milevadb_enable_noop_functions=1;`)
	tk.MustQuery(`select @@milevadb_enable_noop_functions;`).Check(testkit.Events("1"))
	tk.MustQuery(`select @@global.milevadb_enable_noop_functions;`).Check(testkit.Events("0"))
	tk.MustQuery(`select get_lock("dagger", 10)`).Check(testkit.Events("1"))
	tk.MustQuery(`select release_lock("dagger")`).Check(testkit.Events("1"))

	// restore to 0
	tk.MustExec(`set milevadb_enable_noop_functions=0;`)
	tk.MustQuery(`select @@milevadb_enable_noop_functions;`).Check(testkit.Events("0"))
	tk.MustQuery(`select @@global.milevadb_enable_noop_functions;`).Check(testkit.Events("0"))

	_, err = tk.Exec(`select get_lock('lock2', 10);`)
	c.Assert(terror.ErrorEqual(err, expression.ErrFunctionsNoopImpl), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec(`select release_lock('lock2');`)
	c.Assert(terror.ErrorEqual(err, expression.ErrFunctionsNoopImpl), IsTrue, Commentf("err %v", err))

	// set test
	_, err = tk.Exec(`set milevadb_enable_noop_functions='abc'`)
	c.Assert(err, NotNil)
	_, err = tk.Exec(`set milevadb_enable_noop_functions=11`)
	c.Assert(err, NotNil)
	tk.MustExec(`set milevadb_enable_noop_functions="off";`)
	tk.MustQuery(`select @@milevadb_enable_noop_functions;`).Check(testkit.Events("0"))
	tk.MustExec(`set milevadb_enable_noop_functions="on";`)
	tk.MustQuery(`select @@milevadb_enable_noop_functions;`).Check(testkit.Events("1"))
	tk.MustExec(`set milevadb_enable_noop_functions=0;`)
	tk.MustQuery(`select @@milevadb_enable_noop_functions;`).Check(testkit.Events("0"))
}

func (s *testSuite5) TestSetClusterConfig(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	serversInfo := []schemareplicant.ServerInfo{
		{ServerType: "milevadb", Address: "127.0.0.1:1111", StatusAddr: "127.0.0.1:1111"},
		{ServerType: "milevadb", Address: "127.0.0.1:2222", StatusAddr: "127.0.0.1:2222"},
		{ServerType: "fidel", Address: "127.0.0.1:3333", StatusAddr: "127.0.0.1:3333"},
		{ServerType: "fidel", Address: "127.0.0.1:4444", StatusAddr: "127.0.0.1:4444"},
		{ServerType: "einsteindb", Address: "127.0.0.1:5555", StatusAddr: "127.0.0.1:5555"},
		{ServerType: "einsteindb", Address: "127.0.0.1:6666", StatusAddr: "127.0.0.1:6666"},
	}
	var serverInfoErr error
	serverInfoFunc := func(stochastikctx.Context) ([]schemareplicant.ServerInfo, error) {
		return serversInfo, serverInfoErr
	}
	tk.Se.SetValue(executor.TestSetConfigServerInfoKey, serverInfoFunc)

	c.Assert(tk.ExecToErr("set config xxx log.level='info'"), ErrorMatches, "unknown type xxx")
	c.Assert(tk.ExecToErr("set config milevadb log.level='info'"), ErrorMatches, "MilevaDB doesn't support to change configs online, please use ALLEGROALLEGROSQL variables")
	c.Assert(tk.ExecToErr("set config '127.0.0.1:1111' log.level='info'"), ErrorMatches, "MilevaDB doesn't support to change configs online, please use ALLEGROALLEGROSQL variables")
	c.Assert(tk.ExecToErr("set config '127.a.b.c:1234' log.level='info'"), ErrorMatches, "invalid instance 127.a.b.c:1234")
	c.Assert(tk.ExecToErr("set config einsteindb log.level=null"), ErrorMatches, "can't set config to null")
	c.Assert(tk.ExecToErr("set config '1.1.1.1:1111' log.level='info'"), ErrorMatches, "instance 1.1.1.1:1111 is not found in this cluster")

	httpCnt := 0
	tk.Se.SetValue(executor.TestSetConfigHTTPHandlerKey, func(*http.Request) (*http.Response, error) {
		httpCnt++
		return &http.Response{StatusCode: http.StatusOK, Body: ioutil.NopCloser(nil)}, nil
	})
	tk.MustExec("set config einsteindb log.level='info'")
	c.Assert(httpCnt, Equals, 2)

	httpCnt = 0
	tk.MustExec("set config '127.0.0.1:5555' log.level='info'")
	c.Assert(httpCnt, Equals, 1)

	httpCnt = 0
	tk.Se.SetValue(executor.TestSetConfigHTTPHandlerKey, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("something wrong")
	})
	tk.MustExec("set config einsteindb log.level='info'")
	tk.MustQuery("show warnings").Check(testkit.Events(
		"Warning 1105 something wrong", "Warning 1105 something wrong"))

	tk.Se.SetValue(executor.TestSetConfigHTTPHandlerKey, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadRequest, Body: ioutil.NopCloser(bytes.NewBufferString("WRONG"))}, nil
	})
	tk.MustExec("set config einsteindb log.level='info'")
	tk.MustQuery("show warnings").Check(testkit.Events(
		"Warning 1105 bad request to http://127.0.0.1:5555/config: WRONG", "Warning 1105 bad request to http://127.0.0.1:6666/config: WRONG"))
}

func (s *testSuite5) TestSetClusterConfigJSONData(c *C) {
	var d types.MyDecimal
	c.Assert(d.FromFloat64(123.456), IsNil)
	tyBool := types.NewFieldType(allegrosql.TypeTiny)
	tyBool.Flag |= allegrosql.IsBooleanFlag
	cases := []struct {
		val    expression.Expression
		result string
		succ   bool
	}{
		{&expression.Constant{Value: types.NewIntCauset(1), RetType: tyBool}, `{"k":true}`, true},
		{&expression.Constant{Value: types.NewIntCauset(0), RetType: tyBool}, `{"k":false}`, true},
		{&expression.Constant{Value: types.NewIntCauset(2333), RetType: types.NewFieldType(allegrosql.TypeLong)}, `{"k":2333}`, true},
		{&expression.Constant{Value: types.NewFloat64Causet(23.33), RetType: types.NewFieldType(allegrosql.TypeDouble)}, `{"k":23.33}`, true},
		{&expression.Constant{Value: types.NewStringCauset("abcd"), RetType: types.NewFieldType(allegrosql.TypeString)}, `{"k":"abcd"}`, true},
		{&expression.Constant{Value: types.NewDecimalCauset(&d), RetType: types.NewFieldType(allegrosql.TypeNewDecimal)}, `{"k":123.456}`, true},
		{&expression.Constant{Value: types.NewCauset(nil), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, "", false},
		{&expression.Constant{RetType: types.NewFieldType(allegrosql.TypeJSON)}, "", false}, // unsupported type
		{nil, "", false},
	}

	ctx := mock.NewContext()
	for _, t := range cases {
		result, err := executor.ConvertConfigItem2JSON(ctx, "k", t.val)
		if t.succ {
			c.Assert(t.result, Equals, result)
		} else {
			c.Assert(err, NotNil)
		}
	}
}
