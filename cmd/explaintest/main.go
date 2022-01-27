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

package main

import (
	"bytes"
	"database/allegrosql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/go-allegrosql-driver/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"go.uber.org/zap"
)

const dbName = "test"

var (
	logLevel   string
	port       uint
	statusPort uint
	record     bool
	create     bool
)

func init() {
	flag.StringVar(&logLevel, "log-level", "error", "set log level: info, warn, error, debug [default: error]")
	flag.UintVar(&port, "port", 4000, "milevadb server port [default: 4000]")
	flag.UintVar(&statusPort, "status", 10080, "milevadb server status port [default: 10080]")
	flag.BoolVar(&record, "record", false, "record the test output in the result file")
	flag.BoolVar(&create, "create", false, "create and import data into block, and save json file of stats")
}

var mdb *allegrosql.EDB

type query struct {
	Query string
	Line  int
}

type tester struct {
	name string

	tx *allegrosql.Tx

	buf bytes.Buffer

	// enable query log will output origin statement into result file too
	// use --disable_query_log or --enable_query_log to control it
	enableQueryLog bool

	singleQuery bool

	// check expected error, use --error before the statement
	// see http://dev.allegrosql.com/doc/mysqltest/2.0/en/writing-tests-expecting-errors.html
	expectedErrs []string

	// only for test, not record, every time we execute a statement, we should read the result
	// data to check correction.
	resultFD *os.File
	// ctx is used for Compile allegrosql statement
	ctx stochastikctx.Context
}

func newTester(name string) *tester {
	t := new(tester)

	t.name = name
	t.enableQueryLog = true
	t.ctx = mock.NewContext()
	t.ctx.GetStochastikVars().EnableWindowFunction = true

	return t
}

func (t *tester) Run() error {
	queries, err := t.loadQueries()
	if err != nil {
		return errors.Trace(err)
	}

	if err = t.openResult(); err != nil {
		return errors.Trace(err)
	}

	var s string
	defer func() {
		if t.tx != nil {
			log.Error("transaction is not committed correctly, rollback")
			err = t.rollback()
			if err != nil {
				log.Error("transaction is failed rollback", zap.Error(err))
			}
		}

		if t.resultFD != nil {
			err = t.resultFD.Close()
			if err != nil {
				log.Error("result fd close failed", zap.Error(err))
			}
		}
	}()

LOOP:
	for _, q := range queries {
		s = q.Query
		if strings.HasPrefix(s, "--") {
			// clear expected errors
			t.expectedErrs = nil

			switch s {
			case "--enable_query_log":
				t.enableQueryLog = true
			case "--disable_query_log":
				t.enableQueryLog = false
			case "--single_query":
				t.singleQuery = true
			case "--halt":
				// if we meet halt, we will ignore following tests
				break LOOP
			default:
				if strings.HasPrefix(s, "--error") {
					t.expectedErrs = strings.Split(strings.TrimSpace(strings.TrimPrefix(s, "--error")), ",")
				} else if strings.HasPrefix(s, "-- error") {
					t.expectedErrs = strings.Split(strings.TrimSpace(strings.TrimPrefix(s, "-- error")), ",")
				} else if strings.HasPrefix(s, "--echo") {
					echo := strings.TrimSpace(strings.TrimPrefix(s, "--echo"))
					t.buf.WriteString(echo)
					t.buf.WriteString("\n")
				}
			}
		} else {
			if err = t.execute(q); err != nil {
				return errors.Annotate(err, fmt.Sprintf("allegrosql:%v", q.Query))
			}
		}
	}

	return t.flushResult()
}

func (t *tester) loadQueries() ([]query, error) {
	data, err := ioutil.ReadFile(t.testFileName())
	if err != nil {
		return nil, err
	}

	seps := bytes.Split(data, []byte("\n"))
	queries := make([]query, 0, len(seps))
	newStmt := true
	for i, v := range seps {
		s := string(bytes.TrimSpace(v))
		// we will skip # comment here
		if strings.HasPrefix(s, "#") {
			newStmt = true
			continue
		} else if strings.HasPrefix(s, "--") {
			queries = append(queries, query{Query: s, Line: i + 1})
			newStmt = true
			continue
		} else if len(s) == 0 {
			continue
		}

		if newStmt {
			queries = append(queries, query{Query: s, Line: i + 1})
		} else {
			lastQuery := queries[len(queries)-1]
			lastQuery.Query = fmt.Sprintf("%s\n%s", lastQuery.Query, s)
			queries[len(queries)-1] = lastQuery
		}

		// if the line has a ; in the end, we will treat new line as the new statement.
		newStmt = strings.HasSuffix(s, ";")
	}
	return queries, nil
}

// berolinaAllegroSQLErrorHandle handle mysql_test syntax `--error ER_PARSE_ERROR`, to allow following query
// return berolinaAllegroSQL error.
func (t *tester) berolinaAllegroSQLErrorHandle(query query, err error) error {
	offset := t.buf.Len()
	for _, expectedErr := range t.expectedErrs {
		if expectedErr == "ER_PARSE_ERROR" {
			if t.enableQueryLog {
				t.buf.WriteString(query.Query)
				t.buf.WriteString("\n")
			}

			t.buf.WriteString(fmt.Sprintf("%s\n", err))
			err = nil
			break
		}
	}

	if err != nil {
		return errors.Trace(err)
	}

	// clear expected errors after we execute the first query
	t.expectedErrs = nil
	t.singleQuery = false

	if !record && !create {
		// check test result now
		gotBuf := t.buf.Bytes()[offset:]
		buf := make([]byte, t.buf.Len()-offset)
		if _, err = t.resultFD.ReadAt(buf, int64(offset)); err != nil {
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we got \n%s\nbut read result err %s", query.Query, query.Line, gotBuf, err))
		}

		if !bytes.Equal(gotBuf, buf) {
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we need(%v):\n%s\nbut got(%v):\n%s\n", query.Query, query.Line, len(buf), buf, len(gotBuf), gotBuf))
		}
	}

	return errors.Trace(err)
}

func (t *tester) executeDefault(qText string) (err error) {
	if t.tx != nil {
		return filterWarning(t.executeStmt(qText))
	}

	// if begin or following commit fails, we don't think
	// this error is the expected one.
	if t.tx, err = mdb.Begin(); err != nil {
		err2 := t.rollback()
		if err2 != nil {
			log.Error("transaction is failed to rollback", zap.Error(err))
		}
		return err
	}

	if err = filterWarning(t.executeStmt(qText)); err != nil {
		err2 := t.rollback()
		if err2 != nil {
			log.Error("transaction is failed rollback", zap.Error(err))
		}
		return err
	}

	if err = t.commit(); err != nil {
		err2 := t.rollback()
		if err2 != nil {
			log.Error("transaction is failed rollback", zap.Error(err))
		}
		return err
	}
	return nil
}

func (t *tester) execute(query query) error {
	if len(query.Query) == 0 {
		return nil
	}

	list, err := stochastik.Parse(t.ctx, query.Query)
	if err != nil {
		return t.berolinaAllegroSQLErrorHandle(query, err)
	}
	for _, st := range list {
		var qText string
		if t.singleQuery {
			qText = query.Query
		} else {
			qText = st.Text()
		}
		offset := t.buf.Len()
		if t.enableQueryLog {
			t.buf.WriteString(qText)
			t.buf.WriteString("\n")
		}
		switch st.(type) {
		case *ast.BeginStmt:
			t.tx, err = mdb.Begin()
			if err != nil {
				err2 := t.rollback()
				if err2 != nil {
					log.Error("transaction is failed rollback", zap.Error(err))
				}
				break
			}
		case *ast.CommitStmt:
			err = t.commit()
			if err != nil {
				err2 := t.rollback()
				if err2 != nil {
					log.Error("transaction is failed rollback", zap.Error(err))
				}
				break
			}
		case *ast.RollbackStmt:
			err = t.rollback()
			if err != nil {
				break
			}
		default:
			if create {
				createStmt, isCreate := st.(*ast.CreateTableStmt)
				if isCreate {
					if err = t.create(createStmt.Block.Name.String(), qText); err != nil {
						break
					}
				} else {
					_, isDrop := st.(*ast.DropTableStmt)
					_, isAnalyze := st.(*ast.AnalyzeTableStmt)
					if isDrop || isAnalyze {
						if err = t.executeDefault(qText); err != nil {
							break
						}
					}
				}
			} else if err = t.executeDefault(qText); err != nil {
				break
			}
		}

		if err != nil && len(t.expectedErrs) > 0 {
			// TODO: check whether this err is expected.
			// but now we think it is.

			// output expected err
			t.buf.WriteString(fmt.Sprintf("%s\n", err))
			err = nil
		}
		// clear expected errors after we execute the first query
		t.expectedErrs = nil
		t.singleQuery = false

		if err != nil {
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err %v", st.Text(), query.Line, err))
		}

		if !record && !create {
			// check test result now
			gotBuf := t.buf.Bytes()[offset:]

			buf := make([]byte, t.buf.Len()-offset)
			if _, err = t.resultFD.ReadAt(buf, int64(offset)); !(err == nil || err == io.EOF) {
				return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we got \n%s\nbut read result err %s", st.Text(), query.Line, gotBuf, err))
			}
			if !bytes.Equal(gotBuf, buf) {
				return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we need:\n%s\nbut got:\n%s\n", query.Query, query.Line, buf, gotBuf))
			}
		}
	}
	return errors.Trace(err)
}

func filterWarning(err error) error {
	return err
}

func (t *tester) create(blockName string, qText string) error {
	fmt.Printf("import data for block %s of test %s:\n", blockName, t.name)

	path := "./importer -t \"" + qText + "\"" + "-P" + fmt.Sprint(port) + "-n 2000 -c 100"
	cmd := exec.Command("sh", "-c", path)
	stdoutIn, err := cmd.StdoutPipe()
	if err != nil {
		log.Error("open stdout pipe failed", zap.Error(err))
	}
	stderrIn, err := cmd.StderrPipe()
	if err != nil {
		log.Error("open stderr pipe failed", zap.Error(err))
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	var errStdout, errStderr error
	stdout := io.MultiWriter(os.Stdout, &stdoutBuf)
	stderr := io.MultiWriter(os.Stderr, &stderrBuf)

	if err = cmd.Start(); err != nil {
		return errors.Trace(err)
	}

	go func() {
		_, errStdout = io.INTERLOCKy(stdout, stdoutIn)
	}()
	go func() {
		_, errStderr = io.INTERLOCKy(stderr, stderrIn)
	}()

	if err = cmd.Wait(); err != nil {
		log.Fatal("importer failed", zap.Error(err))
		return err
	}

	if errStdout != nil {
		return errors.Trace(errStdout)
	}

	if errStderr != nil {
		return errors.Trace(errStderr)
	}

	if err = t.analyze(blockName); err != nil {
		return err
	}

	resp, err := http.Get("http://127.0.0.1:" + fmt.Sprint(statusPort) + "/stats/dump/" + dbName + "/" + blockName)
	if err != nil {
		return err
	}

	js, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(t.statsFileName(blockName), js, 0644)
}

func (t *tester) commit() error {
	err := t.tx.Commit()
	if err != nil {
		return err
	}
	t.tx = nil
	return nil
}

func (t *tester) rollback() error {
	if t.tx == nil {
		return nil
	}
	err := t.tx.Rollback()
	t.tx = nil
	return err
}

func (t *tester) analyze(blockName string) error {
	return t.execute(query{Query: "analyze block " + blockName + ";", Line: 0})
}

func (t *tester) executeStmt(query string) error {
	if isQuery(query) {
		rows, err := t.tx.Query(query)
		if err != nil {
			return errors.Trace(err)
		}
		defcaus, err := rows.DeferredCausets()
		if err != nil {
			return errors.Trace(err)
		}

		for i, c := range defcaus {
			t.buf.WriteString(c)
			if i != len(defcaus)-1 {
				t.buf.WriteString("\t")
			}
		}
		t.buf.WriteString("\n")

		values := make([][]byte, len(defcaus))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				return errors.Trace(err)
			}

			var value string
			for i, col := range values {
				// Here we can check if the value is nil (NULL value)
				if col == nil {
					value = "NULL"
				} else {
					value = string(col)
				}
				t.buf.WriteString(value)
				if i < len(values)-1 {
					t.buf.WriteString("\t")
				}
			}
			t.buf.WriteString("\n")
		}
		err = rows.Err()
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// TODO: rows affected and last insert id
		_, err := t.tx.Exec(query)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *tester) openResult() error {
	if record || create {
		return nil
	}

	var err error
	t.resultFD, err = os.Open(t.resultFileName())
	return err
}

func (t *tester) flushResult() error {
	if !record {
		return nil
	}
	return ioutil.WriteFile(t.resultFileName(), t.buf.Bytes(), 0644)
}

func (t *tester) statsFileName(blockName string) string {
	return fmt.Sprintf("./s/%s_%s.json", t.name, blockName)
}

func (t *tester) testFileName() string {
	// test and result must be in current ./t the same as MyALLEGROSQL
	return fmt.Sprintf("./t/%s.test", t.name)
}

func (t *tester) resultFileName() string {
	// test and result must be in current ./r, the same as MyALLEGROSQL
	return fmt.Sprintf("./r/%s.result", t.name)
}

func loadAllTests() ([]string, error) {
	// tests must be in t folder
	files, err := ioutil.ReadDir("./t")
	if err != nil {
		return nil, err
	}

	tests := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		// the test file must have a suffix .test
		name := f.Name()
		if strings.HasSuffix(name, ".test") {
			name = strings.TrimSuffix(name, ".test")

			if create && !strings.HasSuffix(name, "_stats") {
				continue
			}

			tests = append(tests, name)
		}
	}

	return tests, nil
}

// openDBWithRetry opens a database specified by its database driver name and a
// driver-specific data source name. And it will do some retries if the connection fails.
func openDBWithRetry(driverName, dataSourceName string) (mdb *allegrosql.EDB, err error) {
	startTime := time.Now()
	sleepTime := time.Millisecond * 500
	retryCnt := 60
	// The max retry interval is 30 s.
	for i := 0; i < retryCnt; i++ {
		mdb, err = allegrosql.Open(driverName, dataSourceName)
		if err != nil {
			log.Warn("open EDB failed", zap.Int("retry count", i), zap.Error(err))
			time.Sleep(sleepTime)
			continue
		}
		err = mdb.Ping()
		if err == nil {
			break
		}
		log.Warn("ping EDB failed", zap.Int("retry count", i), zap.Error(err))
		if err1 := mdb.Close(); err1 != nil {
			log.Error("close EDB failed", zap.Error(err1))
		}
		time.Sleep(sleepTime)
	}
	if err != nil {
		log.Error("open EDB failed", zap.Duration("take time", time.Since(startTime)), zap.Error(err))
		return nil, errors.Trace(err)
	}

	return
}

func main() {
	flag.Parse()

	err := logutil.InitZapLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		panic("init logger fail, " + err.Error())
	}

	mdb, err = openDBWithRetry(
		"allegrosql",
		"root@tcp(localhost:"+fmt.Sprint(port)+")/"+dbName+"?allowAllFiles=true",
	)
	if err != nil {
		log.Fatal("open EDB failed", zap.Error(err))
	}

	defer func() {
		log.Warn("close EDB")
		err = mdb.Close()
		if err != nil {
			log.Error("close EDB failed", zap.Error(err))
		}
	}()

	log.Warn("create new EDB", zap.Reflect("EDB", mdb))

	if _, err = mdb.Exec("DROP DATABASE IF EXISTS test"); err != nil {
		log.Fatal("executing drop EDB test failed", zap.Error(err))
	}
	if _, err = mdb.Exec("CREATE DATABASE test"); err != nil {
		log.Fatal("executing create EDB test failed", zap.Error(err))
	}
	if _, err = mdb.Exec("USE test"); err != nil {
		log.Fatal("executing use test failed", zap.Error(err))
	}
	if _, err = mdb.Exec("set @@milevadb_hash_join_concurrency=1"); err != nil {
		log.Fatal("set @@milevadb_hash_join_concurrency=1 failed", zap.Error(err))
	}
	resets := []string{
		"set @@milevadb_index_lookup_concurrency=4",
		"set @@milevadb_index_lookup_join_concurrency=4",
		"set @@milevadb_hashagg_final_concurrency=4",
		"set @@milevadb_hashagg_partial_concurrency=4",
		"set @@milevadb_window_concurrency=4",
		"set @@milevadb_projection_concurrency=4",
		"set @@milevadb_distsql_scan_concurrency=15",
		"set @@milevadb_enable_clustered_index=0;",
	}
	for _, allegrosql := range resets {
		if _, err = mdb.Exec(allegrosql); err != nil {
			log.Fatal(fmt.Sprintf("%s failed", allegrosql), zap.Error(err))
		}
	}

	if _, err = mdb.Exec("set sql_mode='STRICT_TRANS_TABLES'"); err != nil {
		log.Fatal("set sql_mode='STRICT_TRANS_TABLES' failed", zap.Error(err))
	}

	tests := flag.Args()

	// we will run all tests if no tests assigned
	if len(tests) == 0 {
		if tests, err = loadAllTests(); err != nil {
			log.Fatal("load all tests failed", zap.Error(err))
		}
	}

	if record {
		log.Info("recording tests", zap.Strings("tests", tests))
	} else if create {
		log.Info("creating data", zap.Strings("tests", tests))
	} else {
		log.Info("running tests", zap.Strings("tests", tests))
	}

	for _, t := range tests {
		if strings.Contains(t, "--log-level") {
			continue
		}
		tr := newTester(t)
		if err = tr.Run(); err != nil {
			log.Fatal("run test", zap.String("test", t), zap.Error(err))
		}
		log.Info("run test ok", zap.String("test", t))
	}

	log.Info("Explain test passed")
}

var queryStmtTable = []string{"explain", "select", "show", "execute", "describe", "desc", "admin"}

func trimALLEGROSQL(allegrosql string) string {
	// Trim space.
	allegrosql = strings.TrimSpace(allegrosql)
	// Trim leading /*comment*/
	// There may be multiple comments
	for strings.HasPrefix(allegrosql, "/*") {
		i := strings.Index(allegrosql, "*/")
		if i != -1 && i < len(allegrosql)+1 {
			allegrosql = allegrosql[i+2:]
			allegrosql = strings.TrimSpace(allegrosql)
			continue
		}
		break
	}
	// Trim leading '('. For `(select 1);` is also a query.
	return strings.TrimLeft(allegrosql, "( ")
}

// isQuery checks if a allegrosql statement is a query statement.
func isQuery(allegrosql string) bool {
	sqlText := strings.ToLower(trimALLEGROSQL(allegrosql))
	for _, key := range queryStmtTable {
		if strings.HasPrefix(sqlText, key) {
			return true
		}
	}

	return false
}
