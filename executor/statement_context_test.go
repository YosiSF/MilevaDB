MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
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
	"fmt"
	"unicode/utf8"

	"github.com/whtcorpsinc/MilevaDB-Prod/block"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
)

const (
	strictModeALLEGROSQL    = "set sql_mode = 'STRICT_TRANS_TABLES'"
	nonStrictModeALLEGROSQL = "set sql_mode = ''"
)

func (s *testSuite1) TestStatementContext(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("create block sc (a int)")
	tk.MustExec("insert sc values (1), (2)")

	tk.MustExec(strictModeALLEGROSQL)
	tk.MustQuery("select * from sc where a > cast(1.1 as decimal)").Check(testkit.Events("2"))
	tk.MustExec("uFIDelate sc set a = 4 where a > cast(1.1 as decimal)")

	tk.MustExec(nonStrictModeALLEGROSQL)
	tk.MustExec("uFIDelate sc set a = 3 where a > cast(1.1 as decimal)")
	tk.MustQuery("select * from sc").Check(testkit.Events("1", "3"))

	tk.MustExec(strictModeALLEGROSQL)
	tk.MustExec("delete from sc")
	tk.MustExec("insert sc values ('1.8'+1)")
	tk.MustQuery("select * from sc").Check(testkit.Events("3"))

	// Handle interlock flags, '1x' is an invalid int.
	// UFIDelATE and DELETE do select request first which is handled by interlock.
	// In strict mode we expect error.
	_, err := tk.Exec("uFIDelate sc set a = 4 where a > '1x'")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete from sc where a < '1x'")
	c.Assert(err, NotNil)
	tk.MustQuery("select * from sc where a > '1x'").Check(testkit.Events("3"))

	// Non-strict mode never returns error.
	tk.MustExec(nonStrictModeALLEGROSQL)
	tk.MustExec("uFIDelate sc set a = 4 where a > '1x'")
	tk.MustExec("delete from sc where a < '1x'")
	tk.MustQuery("select * from sc where a > '1x'").Check(testkit.Events("4"))

	// Test invalid UTF8
	tk.MustExec("create block sc2 (a varchar(255))")
	// Insert an invalid UTF8
	tk.MustExec("insert sc2 values (unhex('4040ffff'))")
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Greater, uint16(0))
	tk.MustQuery("select * from sc2").Check(testkit.Events("@@"))
	tk.MustExec(strictModeALLEGROSQL)
	_, err = tk.Exec("insert sc2 values (unhex('4040ffff'))")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, block.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@milevadb_skip_utf8_check = '1'")
	_, err = tk.Exec("insert sc2 values (unhex('4040ffff'))")
	c.Assert(err, IsNil)
	tk.MustQuery("select length(a) from sc2").Check(testkit.Events("2", "4"))

	tk.MustExec("set @@milevadb_skip_utf8_check = '0'")
	runeErrStr := string(utf8.RuneError)
	tk.MustExec(fmt.Sprintf("insert sc2 values ('%s')", runeErrStr))

	// Test invalid ASCII
	tk.MustExec("create block sc3 (a varchar(255)) charset ascii")

	tk.MustExec(nonStrictModeALLEGROSQL)
	tk.MustExec("insert sc3 values (unhex('4040ffff'))")
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Greater, uint16(0))
	tk.MustQuery("select * from sc3").Check(testkit.Events("@@"))

	tk.MustExec(strictModeALLEGROSQL)
	_, err = tk.Exec("insert sc3 values (unhex('4040ffff'))")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, block.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@milevadb_skip_ascii_check = '1'")
	_, err = tk.Exec("insert sc3 values (unhex('4040ffff'))")
	c.Assert(err, IsNil)
	tk.MustQuery("select length(a) from sc3").Check(testkit.Events("2", "4"))

	// no placeholder in ASCII, so just insert '@@'...
	tk.MustExec("set @@milevadb_skip_ascii_check = '0'")
	tk.MustExec("insert sc3 values (unhex('4040'))")

	// Test non-BMP characters.
	tk.MustExec(nonStrictModeALLEGROSQL)
	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1(a varchar(100) charset utf8);")
	defer tk.MustExec("drop block if exists t1")
	tk.MustExec("insert t1 values (unhex('f09f8c80'))")
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Greater, uint16(0))
	tk.MustQuery("select * from t1").Check(testkit.Events(""))
	tk.MustExec("insert t1 values (unhex('4040f09f8c80'))")
	c.Assert(tk.Se.GetStochaseinstein_dbars().StmtCtx.WarningCount(), Greater, uint16(0))
	tk.MustQuery("select * from t1").Check(testkit.Events("", "@@"))
	tk.MustQuery("select length(a) from t1").Check(testkit.Events("0", "2"))
	tk.MustExec(strictModeALLEGROSQL)
	_, err = tk.Exec("insert t1 values (unhex('f09f8c80'))")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, block.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec("insert t1 values (unhex('F0A48BAE'))")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, block.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.CheckMb4ValueInUTF8 = false
	})
	tk.MustExec("insert t1 values (unhex('f09f8c80'))")
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.CheckMb4ValueInUTF8 = true
	})
	_, err = tk.Exec("insert t1 values (unhex('F0A48BAE'))")
	c.Assert(err, NotNil)
}
