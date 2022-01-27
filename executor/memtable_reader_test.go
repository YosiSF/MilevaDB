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
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	pperceptron "github.com/prometheus/common/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/diagnosticspb"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fn"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/FIDelapi"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/sysutil"
	"google.golang.org/grpc"
)

type testMemBlockReaderSuite struct{ *testClusterBlockBase }

type testClusterBlockBase struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

func (s *testClusterBlockBase) SetUpSuite(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	s.dom = dom
}

func (s *testClusterBlockBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *testMemBlockReaderSuite) TestMetricBlockData(c *C) {
	fpName := "github.com/whtcorpsinc/milevadb/executor/mockMetricsPromData"
	c.Assert(failpoint.Enable(fpName, "return"), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	// mock prometheus data
	matrix := pperceptron.Matrix{}
	metric := map[pperceptron.LabelName]pperceptron.LabelValue{
		"instance": "127.0.0.1:10080",
	}
	t, err := time.ParseInLocation("2006-01-02 15:04:05.999", "2020-12-23 20:11:35", time.Local)
	c.Assert(err, IsNil)
	v1 := pperceptron.SamplePair{
		Timestamp: pperceptron.Time(t.UnixNano() / int64(time.Millisecond)),
		Value:     pperceptron.SampleValue(0.1),
	}
	matrix = append(matrix, &pperceptron.SampleStream{Metric: metric, Values: []pperceptron.SamplePair{v1}})

	ctx := context.WithValue(context.Background(), "__mockMetricsPromData", matrix)
	ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
		return fpname == fpName
	})

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use metrics_schema")

	cases := []struct {
		allegrosql string
		exp        []string
	}{
		{
			allegrosql: "select time,instance,quantile,value from milevadb_query_duration;",
			exp: []string{
				"2020-12-23 20:11:35.000000 127.0.0.1:10080 0.9 0.1",
			},
		},
		{
			allegrosql: "select time,instance,quantile,value from milevadb_query_duration where quantile in (0.85, 0.95);",
			exp: []string{
				"2020-12-23 20:11:35.000000 127.0.0.1:10080 0.85 0.1",
				"2020-12-23 20:11:35.000000 127.0.0.1:10080 0.95 0.1",
			},
		},
		{
			allegrosql: "select time,instance,quantile,value from milevadb_query_duration where quantile=0.5",
			exp: []string{
				"2020-12-23 20:11:35.000000 127.0.0.1:10080 0.5 0.1",
			},
		},
	}

	for _, cas := range cases {
		rs, err := tk.Se.Execute(ctx, cas.allegrosql)
		c.Assert(err, IsNil)
		result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("allegrosql: %s", cas.allegrosql))
		result.Check(testkit.Events(cas.exp...))
	}
}

func (s *testMemBlockReaderSuite) TestMilevaDBClusterConfig(c *C) {
	// mock FIDel http server
	router := mux.NewRouter()

	type mockServer struct {
		address string
		server  *httptest.Server
	}
	const testServerCount = 3
	var testServers []*mockServer
	for i := 0; i < testServerCount; i++ {
		server := httptest.NewServer(router)
		address := strings.TrimPrefix(server.URL, "http://")
		testServers = append(testServers, &mockServer{
			address: address,
			server:  server,
		})
	}
	defer func() {
		for _, server := range testServers {
			server.server.Close()
		}
	}()

	// We check the counter to valid how many times request has been sent
	var requestCounter int32
	var mockConfig = func() (map[string]interface{}, error) {
		atomic.AddInt32(&requestCounter, 1)
		configuration := map[string]interface{}{
			"key1": "value1",
			"key2": map[string]string{
				"nest1": "n-value1",
				"nest2": "n-value2",
			},
		}
		return configuration, nil
	}

	// fidel config
	router.Handle(FIDelapi.Config, fn.Wrap(mockConfig))
	// MilevaDB/EinsteinDB config
	router.Handle("/config", fn.Wrap(mockConfig))

	// mock servers
	servers := []string{}
	for _, typ := range []string{"milevadb", "einsteindb", "fidel"} {
		for _, server := range testServers {
			servers = append(servers, strings.Join([]string{typ, server.address, server.address}, ","))
		}
	}

	fpName := "github.com/whtcorpsinc/milevadb/executor/mockClusterConfigServerInfo"
	fpExpr := strings.Join(servers, ";")
	c.Assert(failpoint.Enable(fpName, fmt.Sprintf(`return("%s")`, fpExpr)), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustQuery("select type, `key`, value from information_schema.cluster_config").Check(testkit.Events(
		"milevadb key1 value1",
		"milevadb key2.nest1 n-value1",
		"milevadb key2.nest2 n-value2",
		"milevadb key1 value1",
		"milevadb key2.nest1 n-value1",
		"milevadb key2.nest2 n-value2",
		"milevadb key1 value1",
		"milevadb key2.nest1 n-value1",
		"milevadb key2.nest2 n-value2",
		"einsteindb key1 value1",
		"einsteindb key2.nest1 n-value1",
		"einsteindb key2.nest2 n-value2",
		"einsteindb key1 value1",
		"einsteindb key2.nest1 n-value1",
		"einsteindb key2.nest2 n-value2",
		"einsteindb key1 value1",
		"einsteindb key2.nest1 n-value1",
		"einsteindb key2.nest2 n-value2",
		"fidel key1 value1",
		"fidel key2.nest1 n-value1",
		"fidel key2.nest2 n-value2",
		"fidel key1 value1",
		"fidel key2.nest1 n-value1",
		"fidel key2.nest2 n-value2",
		"fidel key1 value1",
		"fidel key2.nest1 n-value1",
		"fidel key2.nest2 n-value2",
	))
	warnings := tk.Se.GetStochastikVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v", warnings))
	c.Assert(requestCounter, Equals, int32(9))

	// type => server index => event
	rows := map[string][][]string{}
	for _, typ := range []string{"milevadb", "einsteindb", "fidel"} {
		for _, server := range testServers {
			rows[typ] = append(rows[typ], []string{
				fmt.Sprintf("%s %s key1 value1", typ, server.address),
				fmt.Sprintf("%s %s key2.nest1 n-value1", typ, server.address),
				fmt.Sprintf("%s %s key2.nest2 n-value2", typ, server.address),
			})
		}
	}
	var flatten = func(ss ...[]string) []string {
		var result []string
		for _, xs := range ss {
			result = append(result, xs...)
		}
		return result
	}
	var cases = []struct {
		allegrosql string
		reqCount   int32
		rows       []string
	}{
		{
			allegrosql: "select * from information_schema.cluster_config",
			reqCount:   9,
			rows: flatten(
				rows["milevadb"][0],
				rows["milevadb"][1],
				rows["milevadb"][2],
				rows["einsteindb"][0],
				rows["einsteindb"][1],
				rows["einsteindb"][2],
				rows["fidel"][0],
				rows["fidel"][1],
				rows["fidel"][2],
			),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='fidel' or type='einsteindb'",
			reqCount:   6,
			rows: flatten(
				rows["einsteindb"][0],
				rows["einsteindb"][1],
				rows["einsteindb"][2],
				rows["fidel"][0],
				rows["fidel"][1],
				rows["fidel"][2],
			),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='fidel' or instance='" + testServers[0].address + "'",
			reqCount:   9,
			rows: flatten(
				rows["milevadb"][0],
				rows["einsteindb"][0],
				rows["fidel"][0],
				rows["fidel"][1],
				rows["fidel"][2],
			),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='fidel' and type='einsteindb'",
			reqCount:   0,
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='einsteindb'",
			reqCount:   3,
			rows: flatten(
				rows["einsteindb"][0],
				rows["einsteindb"][1],
				rows["einsteindb"][2],
			),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='fidel'",
			reqCount:   3,
			rows: flatten(
				rows["fidel"][0],
				rows["fidel"][1],
				rows["fidel"][2],
			),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type='milevadb'",
			reqCount:   3,
			rows: flatten(
				rows["milevadb"][0],
				rows["milevadb"][1],
				rows["milevadb"][2],
			),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where 'milevadb'=type",
			reqCount:   3,
			rows: flatten(
				rows["milevadb"][0],
				rows["milevadb"][1],
				rows["milevadb"][2],
			),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type in ('milevadb', 'einsteindb')",
			reqCount:   6,
			rows: flatten(
				rows["milevadb"][0],
				rows["milevadb"][1],
				rows["milevadb"][2],
				rows["einsteindb"][0],
				rows["einsteindb"][1],
				rows["einsteindb"][2],
			),
		},
		{
			allegrosql: "select * from information_schema.cluster_config where type in ('milevadb', 'einsteindb', 'fidel')",
			reqCount:   9,
			rows: flatten(
				rows["milevadb"][0],
				rows["milevadb"][1],
				rows["milevadb"][2],
				rows["einsteindb"][0],
				rows["einsteindb"][1],
				rows["einsteindb"][2],
				rows["fidel"][0],
				rows["fidel"][1],
				rows["fidel"][2],
			),
		},
		{
			allegrosql: fmt.Sprintf(`select * from information_schema.cluster_config where instance='%s'`,
				testServers[0].address),
			reqCount: 3,
			rows: flatten(
				rows["milevadb"][0],
				rows["einsteindb"][0],
				rows["fidel"][0],
			),
		},
		{
			allegrosql: fmt.Sprintf(`select * from information_schema.cluster_config where type='milevadb' and instance='%s'`,
				testServers[0].address),
			reqCount: 1,
			rows: flatten(
				rows["milevadb"][0],
			),
		},
		{
			allegrosql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('milevadb', 'einsteindb') and instance='%s'`,
				testServers[0].address),
			reqCount: 2,
			rows: flatten(
				rows["milevadb"][0],
				rows["einsteindb"][0],
			),
		},
		{
			allegrosql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('milevadb', 'einsteindb') and instance in ('%s', '%s')`,
				testServers[0].address, testServers[0].address),
			reqCount: 2,
			rows: flatten(
				rows["milevadb"][0],
				rows["einsteindb"][0],
			),
		},
		{
			allegrosql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('milevadb', 'einsteindb') and instance in ('%s', '%s')`,
				testServers[0].address, testServers[1].address),
			reqCount: 4,
			rows: flatten(
				rows["milevadb"][0],
				rows["milevadb"][1],
				rows["einsteindb"][0],
				rows["einsteindb"][1],
			),
		},
		{
			allegrosql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('milevadb', 'einsteindb') and type='fidel' and instance in ('%s', '%s')`,
				testServers[0].address, testServers[1].address),
			reqCount: 0,
		},
		{
			allegrosql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('milevadb', 'einsteindb') and instance in ('%s', '%s') and instance='%s'`,
				testServers[0].address, testServers[1].address, testServers[2].address),
			reqCount: 0,
		},
		{
			allegrosql: fmt.Sprintf(`select * from information_schema.cluster_config where type in ('milevadb', 'einsteindb') and instance in ('%s', '%s') and instance='%s'`,
				testServers[0].address, testServers[1].address, testServers[0].address),
			reqCount: 2,
			rows: flatten(
				rows["milevadb"][0],
				rows["einsteindb"][0],
			),
		},
	}

	for _, ca := range cases {
		// reset the request counter
		requestCounter = 0
		tk.MustQuery(ca.allegrosql).Check(testkit.Events(ca.rows...))
		warnings := tk.Se.GetStochastikVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v", warnings))
		c.Assert(requestCounter, Equals, ca.reqCount, Commentf("ALLEGROALLEGROSQL: %s", ca.allegrosql))
	}
}

func (s *testClusterBlockBase) writeTmpFile(c *C, dir, filename string, lines []string) {
	err := ioutil.WriteFile(filepath.Join(dir, filename), []byte(strings.Join(lines, "\n")), os.ModePerm)
	c.Assert(err, IsNil, Commentf("write tmp file %s failed", filename))
}

type testServer struct {
	typ       string
	server    *grpc.Server
	address   string
	tmFIDelir string
	logFile   string
}

func (s *testClusterBlockBase) setupClusterGRPCServer(c *C) map[string]*testServer {
	// tp => testServer
	testServers := map[string]*testServer{}

	// create gRPC servers
	for _, typ := range []string{"milevadb", "einsteindb", "fidel"} {
		tmFIDelir, err := ioutil.TemFIDelir("", typ)
		c.Assert(err, IsNil)

		server := grpc.NewServer()
		logFile := filepath.Join(tmFIDelir, fmt.Sprintf("%s.log", typ))
		diagnosticspb.RegisterDiagnosticsServer(server, sysutil.NewDiagnosticsServer(logFile))

		// Find a available port
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		c.Assert(err, IsNil, Commentf("cannot find available port"))

		testServers[typ] = &testServer{
			typ:       typ,
			server:    server,
			address:   fmt.Sprintf("127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port),
			tmFIDelir: tmFIDelir,
			logFile:   logFile,
		}
		go func() {
			if err := server.Serve(listener); err != nil {
				log.Fatalf("failed to serve: %v", err)
			}
		}()
	}
	return testServers
}

func (s *testMemBlockReaderSuite) TestMilevaDBClusterLog(c *C) {
	testServers := s.setupClusterGRPCServer(c)
	defer func() {
		for _, s := range testServers {
			s.server.Stop()
			c.Assert(os.RemoveAll(s.tmFIDelir), IsNil, Commentf("remove tmFIDelir %v failed", s.tmFIDelir))
		}
	}()

	// time format of log file
	var logtime = func(s string) string {
		t, err := time.ParseInLocation("2006/01/02 15:04:05.000", s, time.Local)
		c.Assert(err, IsNil)
		return t.Format("[2006/01/02 15:04:05.000 -07:00]")
	}

	// time format of query output
	var restime = func(s string) string {
		t, err := time.ParseInLocation("2006/01/02 15:04:05.000", s, time.Local)
		c.Assert(err, IsNil)
		return t.Format("2006/01/02 15:04:05.000")
	}

	// prepare log files
	// MilevaDB
	s.writeTmpFile(c, testServers["milevadb"].tmFIDelir, "milevadb.log", []string{
		logtime(`2020/08/26 06:19:13.011`) + ` [INFO] [test log message milevadb 1, foo]`,
		logtime(`2020/08/26 06:19:14.011`) + ` [DEBUG] [test log message milevadb 2, foo]`,
		logtime(`2020/08/26 06:19:15.011`) + ` [error] [test log message milevadb 3, foo]`,
		logtime(`2020/08/26 06:19:16.011`) + ` [trace] [test log message milevadb 4, foo]`,
		logtime(`2020/08/26 06:19:17.011`) + ` [CRITICAL] [test log message milevadb 5, foo]`,
	})
	s.writeTmpFile(c, testServers["milevadb"].tmFIDelir, "milevadb-1.log", []string{
		logtime(`2020/08/26 06:25:13.011`) + ` [info] [test log message milevadb 10, bar]`,
		logtime(`2020/08/26 06:25:14.011`) + ` [debug] [test log message milevadb 11, bar]`,
		logtime(`2020/08/26 06:25:15.011`) + ` [ERROR] [test log message milevadb 12, bar]`,
		logtime(`2020/08/26 06:25:16.011`) + ` [TRACE] [test log message milevadb 13, bar]`,
		logtime(`2020/08/26 06:25:17.011`) + ` [critical] [test log message milevadb 14, bar]`,
	})

	// EinsteinDB
	s.writeTmpFile(c, testServers["einsteindb"].tmFIDelir, "einsteindb.log", []string{
		logtime(`2020/08/26 06:19:13.011`) + ` [INFO] [test log message einsteindb 1, foo]`,
		logtime(`2020/08/26 06:20:14.011`) + ` [DEBUG] [test log message einsteindb 2, foo]`,
		logtime(`2020/08/26 06:21:15.011`) + ` [error] [test log message einsteindb 3, foo]`,
		logtime(`2020/08/26 06:22:16.011`) + ` [trace] [test log message einsteindb 4, foo]`,
		logtime(`2020/08/26 06:23:17.011`) + ` [CRITICAL] [test log message einsteindb 5, foo]`,
	})
	s.writeTmpFile(c, testServers["einsteindb"].tmFIDelir, "einsteindb-1.log", []string{
		logtime(`2020/08/26 06:24:15.011`) + ` [info] [test log message einsteindb 10, bar]`,
		logtime(`2020/08/26 06:25:16.011`) + ` [debug] [test log message einsteindb 11, bar]`,
		logtime(`2020/08/26 06:26:17.011`) + ` [ERROR] [test log message einsteindb 12, bar]`,
		logtime(`2020/08/26 06:27:18.011`) + ` [TRACE] [test log message einsteindb 13, bar]`,
		logtime(`2020/08/26 06:28:19.011`) + ` [critical] [test log message einsteindb 14, bar]`,
	})

	// FIDel
	s.writeTmpFile(c, testServers["fidel"].tmFIDelir, "fidel.log", []string{
		logtime(`2020/08/26 06:18:13.011`) + ` [INFO] [test log message fidel 1, foo]`,
		logtime(`2020/08/26 06:19:14.011`) + ` [DEBUG] [test log message fidel 2, foo]`,
		logtime(`2020/08/26 06:20:15.011`) + ` [error] [test log message fidel 3, foo]`,
		logtime(`2020/08/26 06:21:16.011`) + ` [trace] [test log message fidel 4, foo]`,
		logtime(`2020/08/26 06:22:17.011`) + ` [CRITICAL] [test log message fidel 5, foo]`,
	})
	s.writeTmpFile(c, testServers["fidel"].tmFIDelir, "fidel-1.log", []string{
		logtime(`2020/08/26 06:23:13.011`) + ` [info] [test log message fidel 10, bar]`,
		logtime(`2020/08/26 06:24:14.011`) + ` [debug] [test log message fidel 11, bar]`,
		logtime(`2020/08/26 06:25:15.011`) + ` [ERROR] [test log message fidel 12, bar]`,
		logtime(`2020/08/26 06:26:16.011`) + ` [TRACE] [test log message fidel 13, bar]`,
		logtime(`2020/08/26 06:27:17.011`) + ` [critical] [test log message fidel 14, bar]`,
	})

	fullLogs := [][]string{
		{"2020/08/26 06:18:13.011", "fidel", "INFO", "[test log message fidel 1, foo]"},
		{"2020/08/26 06:19:13.011", "milevadb", "INFO", "[test log message milevadb 1, foo]"},
		{"2020/08/26 06:19:13.011", "einsteindb", "INFO", "[test log message einsteindb 1, foo]"},
		{"2020/08/26 06:19:14.011", "fidel", "DEBUG", "[test log message fidel 2, foo]"},
		{"2020/08/26 06:19:14.011", "milevadb", "DEBUG", "[test log message milevadb 2, foo]"},
		{"2020/08/26 06:19:15.011", "milevadb", "error", "[test log message milevadb 3, foo]"},
		{"2020/08/26 06:19:16.011", "milevadb", "trace", "[test log message milevadb 4, foo]"},
		{"2020/08/26 06:19:17.011", "milevadb", "CRITICAL", "[test log message milevadb 5, foo]"},
		{"2020/08/26 06:20:14.011", "einsteindb", "DEBUG", "[test log message einsteindb 2, foo]"},
		{"2020/08/26 06:20:15.011", "fidel", "error", "[test log message fidel 3, foo]"},
		{"2020/08/26 06:21:15.011", "einsteindb", "error", "[test log message einsteindb 3, foo]"},
		{"2020/08/26 06:21:16.011", "fidel", "trace", "[test log message fidel 4, foo]"},
		{"2020/08/26 06:22:16.011", "einsteindb", "trace", "[test log message einsteindb 4, foo]"},
		{"2020/08/26 06:22:17.011", "fidel", "CRITICAL", "[test log message fidel 5, foo]"},
		{"2020/08/26 06:23:13.011", "fidel", "info", "[test log message fidel 10, bar]"},
		{"2020/08/26 06:23:17.011", "einsteindb", "CRITICAL", "[test log message einsteindb 5, foo]"},
		{"2020/08/26 06:24:14.011", "fidel", "debug", "[test log message fidel 11, bar]"},
		{"2020/08/26 06:24:15.011", "einsteindb", "info", "[test log message einsteindb 10, bar]"},
		{"2020/08/26 06:25:13.011", "milevadb", "info", "[test log message milevadb 10, bar]"},
		{"2020/08/26 06:25:14.011", "milevadb", "debug", "[test log message milevadb 11, bar]"},
		{"2020/08/26 06:25:15.011", "fidel", "ERROR", "[test log message fidel 12, bar]"},
		{"2020/08/26 06:25:15.011", "milevadb", "ERROR", "[test log message milevadb 12, bar]"},
		{"2020/08/26 06:25:16.011", "milevadb", "TRACE", "[test log message milevadb 13, bar]"},
		{"2020/08/26 06:25:16.011", "einsteindb", "debug", "[test log message einsteindb 11, bar]"},
		{"2020/08/26 06:25:17.011", "milevadb", "critical", "[test log message milevadb 14, bar]"},
		{"2020/08/26 06:26:16.011", "fidel", "TRACE", "[test log message fidel 13, bar]"},
		{"2020/08/26 06:26:17.011", "einsteindb", "ERROR", "[test log message einsteindb 12, bar]"},
		{"2020/08/26 06:27:17.011", "fidel", "critical", "[test log message fidel 14, bar]"},
		{"2020/08/26 06:27:18.011", "einsteindb", "TRACE", "[test log message einsteindb 13, bar]"},
		{"2020/08/26 06:28:19.011", "einsteindb", "critical", "[test log message einsteindb 14, bar]"},
	}

	var cases = []struct {
		conditions []string
		expected   [][]string
	}{
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2099/08/26 06:28:19.011'",
				"message like '%'",
			},
			expected: fullLogs,
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:19:13.011'",
				"time<='2020/08/26 06:21:15.011'",
				"message like '%'",
			},
			expected: [][]string{
				{"2020/08/26 06:19:13.011", "milevadb", "INFO", "[test log message milevadb 1, foo]"},
				{"2020/08/26 06:19:13.011", "einsteindb", "INFO", "[test log message einsteindb 1, foo]"},
				{"2020/08/26 06:19:14.011", "fidel", "DEBUG", "[test log message fidel 2, foo]"},
				{"2020/08/26 06:19:14.011", "milevadb", "DEBUG", "[test log message milevadb 2, foo]"},
				{"2020/08/26 06:19:15.011", "milevadb", "error", "[test log message milevadb 3, foo]"},
				{"2020/08/26 06:19:16.011", "milevadb", "trace", "[test log message milevadb 4, foo]"},
				{"2020/08/26 06:19:17.011", "milevadb", "CRITICAL", "[test log message milevadb 5, foo]"},
				{"2020/08/26 06:20:14.011", "einsteindb", "DEBUG", "[test log message einsteindb 2, foo]"},
				{"2020/08/26 06:20:15.011", "fidel", "error", "[test log message fidel 3, foo]"},
				{"2020/08/26 06:21:15.011", "einsteindb", "error", "[test log message einsteindb 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:19:13.011'",
				"time<='2020/08/26 06:21:15.011'",
				"message like '%'",
				"type='fidel'",
			},
			expected: [][]string{
				{"2020/08/26 06:19:14.011", "fidel", "DEBUG", "[test log message fidel 2, foo]"},
				{"2020/08/26 06:20:15.011", "fidel", "error", "[test log message fidel 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time>='2020/08/26 06:19:13.011'",
				"time>='2020/08/26 06:19:14.011'",
				"time<='2020/08/26 06:21:15.011'",
				"type='fidel'",
			},
			expected: [][]string{
				{"2020/08/26 06:19:14.011", "fidel", "DEBUG", "[test log message fidel 2, foo]"},
				{"2020/08/26 06:20:15.011", "fidel", "error", "[test log message fidel 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time>='2020/08/26 06:19:13.011'",
				"time='2020/08/26 06:19:14.011'",
				"message like '%'",
				"type='fidel'",
			},
			expected: [][]string{
				{"2020/08/26 06:19:14.011", "fidel", "DEBUG", "[test log message fidel 2, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:19:13.011'",
				"time<='2020/08/26 06:21:15.011'",
				"message like '%'",
				"type='milevadb'",
			},
			expected: [][]string{
				{"2020/08/26 06:19:13.011", "milevadb", "INFO", "[test log message milevadb 1, foo]"},
				{"2020/08/26 06:19:14.011", "milevadb", "DEBUG", "[test log message milevadb 2, foo]"},
				{"2020/08/26 06:19:15.011", "milevadb", "error", "[test log message milevadb 3, foo]"},
				{"2020/08/26 06:19:16.011", "milevadb", "trace", "[test log message milevadb 4, foo]"},
				{"2020/08/26 06:19:17.011", "milevadb", "CRITICAL", "[test log message milevadb 5, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:19:13.011'",
				"time<='2020/08/26 06:21:15.011'",
				"message like '%'",
				"type='einsteindb'",
			},
			expected: [][]string{
				{"2020/08/26 06:19:13.011", "einsteindb", "INFO", "[test log message einsteindb 1, foo]"},
				{"2020/08/26 06:20:14.011", "einsteindb", "DEBUG", "[test log message einsteindb 2, foo]"},
				{"2020/08/26 06:21:15.011", "einsteindb", "error", "[test log message einsteindb 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:19:13.011'",
				"time<='2020/08/26 06:21:15.011'",
				"message like '%'",
				fmt.Sprintf("instance='%s'", testServers["fidel"].address),
			},
			expected: [][]string{
				{"2020/08/26 06:19:14.011", "fidel", "DEBUG", "[test log message fidel 2, foo]"},
				{"2020/08/26 06:20:15.011", "fidel", "error", "[test log message fidel 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:19:13.011'",
				"time<='2020/08/26 06:21:15.011'",
				"message like '%'",
				fmt.Sprintf("instance='%s'", testServers["milevadb"].address),
			},
			expected: [][]string{
				{"2020/08/26 06:19:13.011", "milevadb", "INFO", "[test log message milevadb 1, foo]"},
				{"2020/08/26 06:19:14.011", "milevadb", "DEBUG", "[test log message milevadb 2, foo]"},
				{"2020/08/26 06:19:15.011", "milevadb", "error", "[test log message milevadb 3, foo]"},
				{"2020/08/26 06:19:16.011", "milevadb", "trace", "[test log message milevadb 4, foo]"},
				{"2020/08/26 06:19:17.011", "milevadb", "CRITICAL", "[test log message milevadb 5, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:19:13.011'",
				"time<='2020/08/26 06:21:15.011'",
				"message like '%'",
				fmt.Sprintf("instance='%s'", testServers["einsteindb"].address),
			},
			expected: [][]string{
				{"2020/08/26 06:19:13.011", "einsteindb", "INFO", "[test log message einsteindb 1, foo]"},
				{"2020/08/26 06:20:14.011", "einsteindb", "DEBUG", "[test log message einsteindb 2, foo]"},
				{"2020/08/26 06:21:15.011", "einsteindb", "error", "[test log message einsteindb 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:19:13.011'",
				"time<='2020/08/26 06:21:15.011'",
				"message like '%'",
				fmt.Sprintf("instance in ('%s', '%s')", testServers["fidel"].address, testServers["milevadb"].address),
			},
			expected: [][]string{
				{"2020/08/26 06:19:13.011", "milevadb", "INFO", "[test log message milevadb 1, foo]"},
				{"2020/08/26 06:19:14.011", "fidel", "DEBUG", "[test log message fidel 2, foo]"},
				{"2020/08/26 06:19:14.011", "milevadb", "DEBUG", "[test log message milevadb 2, foo]"},
				{"2020/08/26 06:19:15.011", "milevadb", "error", "[test log message milevadb 3, foo]"},
				{"2020/08/26 06:19:16.011", "milevadb", "trace", "[test log message milevadb 4, foo]"},
				{"2020/08/26 06:19:17.011", "milevadb", "CRITICAL", "[test log message milevadb 5, foo]"},
				{"2020/08/26 06:20:15.011", "fidel", "error", "[test log message fidel 3, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"message like '%'",
				"level='critical'",
			},
			expected: [][]string{
				{"2020/08/26 06:19:17.011", "milevadb", "CRITICAL", "[test log message milevadb 5, foo]"},
				{"2020/08/26 06:22:17.011", "fidel", "CRITICAL", "[test log message fidel 5, foo]"},
				{"2020/08/26 06:23:17.011", "einsteindb", "CRITICAL", "[test log message einsteindb 5, foo]"},
				{"2020/08/26 06:25:17.011", "milevadb", "critical", "[test log message milevadb 14, bar]"},
				{"2020/08/26 06:27:17.011", "fidel", "critical", "[test log message fidel 14, bar]"},
				{"2020/08/26 06:28:19.011", "einsteindb", "critical", "[test log message einsteindb 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"message like '%'",
				"level='critical'",
				"type in ('fidel', 'einsteindb')",
			},
			expected: [][]string{
				{"2020/08/26 06:22:17.011", "fidel", "CRITICAL", "[test log message fidel 5, foo]"},
				{"2020/08/26 06:23:17.011", "einsteindb", "CRITICAL", "[test log message einsteindb 5, foo]"},
				{"2020/08/26 06:27:17.011", "fidel", "critical", "[test log message fidel 14, bar]"},
				{"2020/08/26 06:28:19.011", "einsteindb", "critical", "[test log message einsteindb 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"message like '%'",
				"level='critical'",
				"(type='fidel' or type='einsteindb')",
			},
			expected: [][]string{
				{"2020/08/26 06:22:17.011", "fidel", "CRITICAL", "[test log message fidel 5, foo]"},
				{"2020/08/26 06:23:17.011", "einsteindb", "CRITICAL", "[test log message einsteindb 5, foo]"},
				{"2020/08/26 06:27:17.011", "fidel", "critical", "[test log message fidel 14, bar]"},
				{"2020/08/26 06:28:19.011", "einsteindb", "critical", "[test log message einsteindb 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"level='critical'",
				"message like '%fidel%'",
			},
			expected: [][]string{
				{"2020/08/26 06:22:17.011", "fidel", "CRITICAL", "[test log message fidel 5, foo]"},
				{"2020/08/26 06:27:17.011", "fidel", "critical", "[test log message fidel 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"level='critical'",
				"message like '%fidel%'",
				"message like '%5%'",
			},
			expected: [][]string{
				{"2020/08/26 06:22:17.011", "fidel", "CRITICAL", "[test log message fidel 5, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"level='critical'",
				"message like '%fidel%'",
				"message like '%5%'",
				"message like '%x%'",
			},
			expected: [][]string{},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"level='critical'",
				"message regexp '.*fidel.*'",
			},
			expected: [][]string{
				{"2020/08/26 06:22:17.011", "fidel", "CRITICAL", "[test log message fidel 5, foo]"},
				{"2020/08/26 06:27:17.011", "fidel", "critical", "[test log message fidel 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"level='critical'",
				"message regexp '.*fidel.*'",
				"message regexp '.*foo]$'",
			},
			expected: [][]string{
				{"2020/08/26 06:22:17.011", "fidel", "CRITICAL", "[test log message fidel 5, foo]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"level='critical'",
				"message regexp '.*fidel.*'",
				"message regexp '.*5.*'",
				"message regexp '.*x.*'",
			},
			expected: [][]string{},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2020/08/26 06:28:19.011'",
				"level='critical'",
				"(message regexp '.*fidel.*' or message regexp '.*milevadb.*')",
			},
			expected: [][]string{
				{"2020/08/26 06:19:17.011", "milevadb", "CRITICAL", "[test log message milevadb 5, foo]"},
				{"2020/08/26 06:22:17.011", "fidel", "CRITICAL", "[test log message fidel 5, foo]"},
				{"2020/08/26 06:25:17.011", "milevadb", "critical", "[test log message milevadb 14, bar]"},
				{"2020/08/26 06:27:17.011", "fidel", "critical", "[test log message fidel 14, bar]"},
			},
		},
		{
			conditions: []string{
				"time>='2020/08/26 06:18:13.011'",
				"time<='2099/08/26 06:28:19.011'",
				// this pattern verifies that there is no optimization breaking
				// length of multiple wildcards, for example, %% may be
				// converted to %, but %_ cannot be converted to %.
				"message like '%milevadb_%_4%'",
			},
			expected: [][]string{
				{"2020/08/26 06:25:17.011", "milevadb", "critical", "[test log message milevadb 14, bar]"},
			},
		},
	}

	var servers []string
	for _, s := range testServers {
		servers = append(servers, strings.Join([]string{s.typ, s.address, s.address}, ","))
	}
	fpName := "github.com/whtcorpsinc/milevadb/executor/mockClusterLogServerInfo"
	fpExpr := strings.Join(servers, ";")
	c.Assert(failpoint.Enable(fpName, fmt.Sprintf(`return("%s")`, fpExpr)), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	tk := testkit.NewTestKit(c, s.causetstore)
	for _, cas := range cases {
		allegrosql := "select * from information_schema.cluster_log"
		if len(cas.conditions) > 0 {
			allegrosql = fmt.Sprintf("%s where %s", allegrosql, strings.Join(cas.conditions, " and "))
		}
		result := tk.MustQuery(allegrosql)
		warnings := tk.Se.GetStochastikVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, 0, Commentf("unexpected warnigns: %+v", warnings))
		var expected []string
		for _, event := range cas.expected {
			expectedEvent := []string{
				restime(event[0]),             // time defCausumn
				event[1],                      // type defCausumn
				testServers[event[1]].address, // instance defCausumn
				strings.ToUpper(sysutil.ParseLogLevel(event[2]).String()), // level defCausumn
				event[3], // message defCausumn
			}
			expected = append(expected, strings.Join(expectedEvent, " "))
		}
		result.Check(testkit.Events(expected...))
	}
}

func (s *testMemBlockReaderSuite) TestMilevaDBClusterLogError(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	fpName := "github.com/whtcorpsinc/milevadb/executor/mockClusterLogServerInfo"
	c.Assert(failpoint.Enable(fpName, `return("")`), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	// Test without start time error.
	rs, err := tk.Exec("select * from information_schema.cluster_log")
	c.Assert(err, IsNil)
	_, err = stochastik.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "denied to scan logs, please specified the start time, such as `time > '2020-01-01 00:00:00'`")

	// Test without end time error.
	rs, err = tk.Exec("select * from information_schema.cluster_log where time>='2020/08/26 06:18:13.011'")
	c.Assert(err, IsNil)
	_, err = stochastik.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "denied to scan logs, please specified the end time, such as `time < '2020-01-01 00:00:00'`")

	// Test without specified message error.
	rs, err = tk.Exec("select * from information_schema.cluster_log where time>='2020/08/26 06:18:13.011' and time<'2020/08/26 16:18:13.011'")
	c.Assert(err, IsNil)
	_, err = stochastik.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "denied to scan full logs (use `SELECT * FROM cluster_log WHERE message LIKE '%'` explicitly if intentionally)")
}
