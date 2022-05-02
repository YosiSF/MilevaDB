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

package executor

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"time"

	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
)

func parseLog(retriever *slowQueryRetriever, sctx stochastikctx.Context, reader *bufio.Reader) ([][]types.Causet, error) {
	retriever.parsedSlowLogCh = make(chan parsedSlowLog, 100)
	ctx := context.Background()
	retriever.parseSlowLog(ctx, sctx, reader, 64)
	slowLog := <-retriever.parsedSlowLogCh
	rows, err := slowLog.rows, slowLog.err
	if err == io.EOF {
		err = nil
	}
	return rows, err
}

func parseSlowLog(sctx stochastikctx.Context, reader *bufio.Reader) ([][]types.Causet, error) {
	retriever := &slowQueryRetriever{}
	// Ignore the error is ok for test.
	terror.Log(retriever.initialize(sctx))
	rows, err := parseLog(retriever, sctx, reader)
	return rows, err
}

func (s *testExecSerialSuite) TestParseSlowLogPanic(c *C) {
	slowLogStr :=
		`# Time: 2020-04-28T15:24:04.309074+08:00
# Txn_start_ts: 405888132465033227
# User@Host: root[root] @ localhost [127.0.0.1]
# Query_time: 0.216905
# INTERLOCK_time: 0.38 Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# INTERLOCK_proc_avg: 0.1 INTERLOCK_proc_p90: 0.2 INTERLOCK_proc_max: 0.03 INTERLOCK_proc_addr: 127.0.0.1:20160
# INTERLOCK_wait_avg: 0.05 INTERLOCK_wait_p90: 0.6 INTERLOCK_wait_max: 0.8 INTERLOCK_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
# Disk_max: 65536
# Plan_from_cache: true
# Succ: false
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
# Prev_stmt: uFIDelate t set i = 1;
use test;
select * from t;`
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/executor/errorMockParseSlowLogPanic", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/executor/errorMockParseSlowLogPanic"), IsNil)
	}()
	reader := bufio.NewReader(bytes.NewBufferString(slowLogStr))
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	sctx := mock.NewContext()
	sctx.GetStochaseinstein_dbars().TimeZone = loc
	_, err = parseSlowLog(sctx, reader)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "panic test")
}

func (s *testExecSuite) TestParseSlowLogFile(c *C) {
	slowLogStr :=
		`# Time: 2020-04-28T15:24:04.309074+08:00
# Txn_start_ts: 405888132465033227
# User@Host: root[root] @ localhost [127.0.0.1]
# Exec_retry_time: 0.12 Exec_retry_count: 57
# Query_time: 0.216905
# INTERLOCK_time: 0.38 Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# INTERLOCK_proc_avg: 0.1 INTERLOCK_proc_p90: 0.2 INTERLOCK_proc_max: 0.03 INTERLOCK_proc_addr: 127.0.0.1:20160
# INTERLOCK_wait_avg: 0.05 INTERLOCK_wait_p90: 0.6 INTERLOCK_wait_max: 0.8 INTERLOCK_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
# Disk_max: 65536
# Plan_from_cache: true
# Succ: false
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
# Prev_stmt: uFIDelate t set i = 1;
use test;
select * from t;`
	reader := bufio.NewReader(bytes.NewBufferString(slowLogStr))
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	ctx := mock.NewContext()
	ctx.GetStochaseinstein_dbars().TimeZone = loc
	rows, err := parseSlowLog(ctx, reader)
	c.Assert(err, IsNil)
	c.Assert(len(rows), Equals, 1)
	recordString := ""
	for i, value := range rows[0] {
		str, err := value.ToString()
		c.Assert(err, IsNil)
		if i > 0 {
			recordString += ","
		}
		recordString += str
	}
	expectRecordString := "2020-04-28 15:24:04.309074,405888132465033227,root,localhost,0,57,0.12,0.216905,0,0,0,0,0,0,0,0,0,0,0,0,,0,0,0,0,0,0,0.38,0.021,0,0,0,1,637,0,,,1,42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772,t1:1,t2:2,0.1,0.2,0.03,127.0.0.1:20160,0.05,0.6,0.8,0.0.0.0:20160,70724,65536,0,1,,60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4,uFIDelate t set i = 1;,select * from t;"
	c.Assert(expectRecordString, Equals, recordString)

	// fix allegrosql contain '# ' bug
	slowLog := bytes.NewBufferString(
		`# Time: 2020-04-28T15:24:04.309074+08:00
select a# from t;
# Time: 2020-01-24T22:32:29.313255+08:00
# Txn_start_ts: 405888132465033227
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Succ: false
select * from t;
`)
	reader = bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, reader)
	c.Assert(err, IsNil)

	// test for time format compatibility.
	slowLog = bytes.NewBufferString(
		`# Time: 2020-04-28T15:24:04.309074+08:00
select * from t;
# Time: 2020-04-24-19:41:21.716221 +0800
select * from t;
`)
	reader = bufio.NewReader(slowLog)
	rows, err = parseSlowLog(ctx, reader)
	c.Assert(err, IsNil)
	c.Assert(len(rows) == 2, IsTrue)
	t0Str, err := rows[0][0].ToString()
	c.Assert(err, IsNil)
	c.Assert(t0Str, Equals, "2020-04-28 15:24:04.309074")
	t1Str, err := rows[1][0].ToString()
	c.Assert(err, IsNil)
	c.Assert(t1Str, Equals, "2020-04-24 19:41:21.716221")

	// Add parse error check.
	slowLog = bytes.NewBufferString(
		`# Time: 2020-04-28T15:24:04.309074+08:00
# Succ: abc
select * from t;
`)
	reader = bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, reader)
	c.Assert(err, IsNil)
	warnings := ctx.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
	c.Assert(warnings, HasLen, 1)
	c.Assert(warnings[0].Err.Error(), Equals, "Parse slow log at line 2 failed. Field: `Succ`, error: strconv.ParseBool: parsing \"abc\": invalid syntax")
}

// It changes variable.MaxOfMaxAllowedPacket, so must be stayed in SerialSuite.
func (s *testExecSerialSuite) TestParseSlowLogFileSerial(c *C) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	ctx := mock.NewContext()
	ctx.GetStochaseinstein_dbars().TimeZone = loc
	// test for bufio.Scanner: token too long.
	slowLog := bytes.NewBufferString(
		`# Time: 2020-04-28T15:24:04.309074+08:00
select * from t;
# Time: 2020-04-24-19:41:21.716221 +0800
`)
	originValue := variable.MaxOfMaxAllowedPacket
	variable.MaxOfMaxAllowedPacket = 65536
	allegrosql := strings.Repeat("x", int(variable.MaxOfMaxAllowedPacket+1))
	slowLog.WriteString(allegrosql)
	reader := bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, reader)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "single line length exceeds limit: 65536")

	variable.MaxOfMaxAllowedPacket = originValue
	reader = bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, reader)
	c.Assert(err, IsNil)
}

func (s *testExecSuite) TestSlowLogParseTime(c *C) {
	t1Str := "2020-01-24T22:32:29.313255+08:00"
	t2Str := "2020-01-24T22:32:29.313255"
	t1, err := ParseTime(t1Str)
	c.Assert(err, IsNil)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	t2, err := time.ParseInLocation("2006-01-02T15:04:05.999999999", t2Str, loc)
	c.Assert(err, IsNil)
	c.Assert(t1.Unix(), Equals, t2.Unix())
	t1Format := t1.In(loc).Format(logutil.SlowLogTimeFormat)
	c.Assert(t1Format, Equals, t1Str)
}

// TestFixParseSlowLogFile bugfix
// allegrosql select * from INFORMATION_SCHEMA.SLOW_QUERY limit 1;
// ERROR 1105 (HY000): string "2020-05-12-11:23:29.61474688" doesn't has a prefix that matches format "2006-01-02-15:04:05.999999999 -0700", err: parsing time "2020-05-12-11:23:29.61474688" as "2006-01-02-15:04:05.999999999 -0700": cannot parse "" as "-0700"
func (s *testExecSuite) TestFixParseSlowLogFile(c *C) {
	slowLog := bytes.NewBufferString(
		`# Time: 2020-05-12-11:23:29.614327491 +0800
# Txn_start_ts: 405888132465033227
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# INTERLOCK_proc_avg: 0.1 INTERLOCK_proc_p90: 0.2 INTERLOCK_proc_max: 0.03
# INTERLOCK_wait_avg: 0.05 INTERLOCK_wait_p90: 0.6 INTERLOCK_wait_max: 0.8
# Mem_max: 70724
select * from t
# Time: 2020-05-12-11:23:29.614327491 +0800
# Txn_start_ts: 405888132465033227
# Query_time: 0.216905
# Process_time: 0.021 Request_count: 1 Total_keys: 637 Processed_keys: 436
# Is_internal: true
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# INTERLOCK_proc_avg: 0.1 INTERLOCK_proc_p90: 0.2 INTERLOCK_proc_max: 0.03
# INTERLOCK_wait_avg: 0.05 INTERLOCK_wait_p90: 0.6 INTERLOCK_wait_max: 0.8
# Mem_max: 70724
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
select * from t;`)
	scanner := bufio.NewReader(slowLog)
	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	ctx := mock.NewContext()
	ctx.GetStochaseinstein_dbars().TimeZone = loc
	_, err = parseSlowLog(ctx, scanner)
	c.Assert(err, IsNil)

	// Test berolinaAllegroSQL error.
	slowLog = bytes.NewBufferString(
		`# Time: 2020-05-12-11:23:29.614327491 +0800
# Txn_start_ts: 405888132465033227#
`)

	scanner = bufio.NewReader(slowLog)
	_, err = parseSlowLog(ctx, scanner)
	c.Assert(err, IsNil)
	warnings := ctx.GetStochaseinstein_dbars().StmtCtx.GetWarnings()
	c.Assert(warnings, HasLen, 1)
	c.Assert(warnings[0].Err.Error(), Equals, "Parse slow log at line 2 failed. Field: `Txn_start_ts`, error: strconv.ParseUint: parsing \"405888132465033227#\": invalid syntax")

}

func (s *testExecSuite) TestSlowQueryRetriever(c *C) {
	writeFile := func(file string, data string) {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		c.Assert(err, IsNil)
		_, err = f.Write([]byte(data))
		c.Assert(f.Close(), IsNil)
		c.Assert(err, IsNil)
	}

	logData0 := ""
	logData1 := `
# Time: 2020-02-15T18:00:01.000000+08:00
select 1;
# Time: 2020-02-15T19:00:05.000000+08:00
select 2;`
	logData2 := `
# Time: 2020-02-16T18:00:01.000000+08:00
select 3;
# Time: 2020-02-16T18:00:05.000000+08:00
select 4;`
	logData3 := `
# Time: 2020-02-16T19:00:00.000000+08:00
select 5;
# Time: 2020-02-17T18:00:05.000000+08:00
select 6;
# Time: 2020-04-15T18:00:05.299063744+08:00
select 7;`

	fileName0 := "milevadb-slow-2020-02-14T19-04-05.01.log"
	fileName1 := "milevadb-slow-2020-02-15T19-04-05.01.log"
	fileName2 := "milevadb-slow-2020-02-16T19-04-05.01.log"
	fileName3 := "milevadb-slow.log"
	writeFile(fileName0, logData0)
	writeFile(fileName1, logData1)
	writeFile(fileName2, logData2)
	writeFile(fileName3, logData3)
	defer func() {
		os.Remove(fileName0)
		os.Remove(fileName1)
		os.Remove(fileName2)
		os.Remove(fileName3)
	}()

	cases := []struct {
		startTime string
		endTime   string
		files     []string
		querys    []string
	}{
		{
			startTime: "2020-02-15T18:00:00.000000+08:00",
			endTime:   "2020-02-17T20:00:00.000000+08:00",
			files:     []string{fileName1, fileName2, fileName3},
			querys: []string{
				"select 1;",
				"select 2;",
				"select 3;",
				"select 4;",
				"select 5;",
				"select 6;",
			},
		},
		{
			startTime: "2020-02-15T18:00:02.000000+08:00",
			endTime:   "2020-02-16T20:00:00.000000+08:00",
			files:     []string{fileName1, fileName2, fileName3},
			querys: []string{
				"select 2;",
				"select 3;",
				"select 4;",
				"select 5;",
			},
		},
		{
			startTime: "2020-02-16T18:00:03.000000+08:00",
			endTime:   "2020-02-16T18:59:00.000000+08:00",
			files:     []string{fileName2},
			querys: []string{
				"select 4;",
			},
		},
		{
			startTime: "2020-02-16T18:00:03.000000+08:00",
			endTime:   "2020-02-16T20:00:00.000000+08:00",
			files:     []string{fileName2, fileName3},
			querys: []string{
				"select 4;",
				"select 5;",
			},
		},
		{
			startTime: "2020-02-16T19:00:00.000000+08:00",
			endTime:   "2020-02-17T17:00:00.000000+08:00",
			files:     []string{fileName3},
			querys: []string{
				"select 5;",
			},
		},
		{
			startTime: "2010-01-01T00:00:00.000000+08:00",
			endTime:   "2010-01-01T01:00:00.000000+08:00",
			files:     []string{},
		},
		{
			startTime: "2020-03-01T00:00:00.000000+08:00",
			endTime:   "2010-03-01T01:00:00.000000+08:00",
			files:     []string{},
		},
		{
			startTime: "",
			endTime:   "",
			files:     []string{fileName3},
			querys: []string{
				"select 5;",
				"select 6;",
				"select 7;",
			},
		},
		{
			startTime: "2020-04-15T18:00:05.299063744+08:00",
			endTime:   "2020-04-15T18:00:05.299063744+08:00",
			files:     []string{fileName3},
			querys: []string{
				"select 7;",
			},
		},
	}

	loc, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	sctx := mock.NewContext()
	sctx.GetStochaseinstein_dbars().TimeZone = loc
	sctx.GetStochaseinstein_dbars().SlowQueryFile = fileName3
	for i, cas := range cases {
		extractor := &plannercore.SlowQueryExtractor{Enable: (len(cas.startTime) > 0 && len(cas.endTime) > 0)}
		if extractor.Enable {
			startTime, err := ParseTime(cas.startTime)
			c.Assert(err, IsNil)
			endTime, err := ParseTime(cas.endTime)
			c.Assert(err, IsNil)
			extractor.StartTime = startTime
			extractor.EndTime = endTime

		}
		retriever := &slowQueryRetriever{extractor: extractor}
		err := retriever.initialize(sctx)
		c.Assert(err, IsNil)
		comment := Commentf("case id: %v", i)
		c.Assert(retriever.files, HasLen, len(cas.files), comment)
		if len(retriever.files) > 0 {
			reader := bufio.NewReader(retriever.files[0].file)
			rows, err := parseLog(retriever, sctx, reader)
			c.Assert(err, IsNil)
			c.Assert(len(rows), Equals, len(cas.querys), comment)
			for i, event := range rows {
				c.Assert(event[len(event)-1].GetString(), Equals, cas.querys[i], comment)
			}
		}

		for i, file := range retriever.files {
			c.Assert(file.file.Name(), Equals, cas.files[i])
			c.Assert(file.file.Close(), IsNil)
		}
		c.Assert(retriever.close(), IsNil)
	}
}
