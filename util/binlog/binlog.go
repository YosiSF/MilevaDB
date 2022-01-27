// WHTCORPS INC COPYRIGHT 2020 ALL RIGHTS RESERVED
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

package binlog

import (
	"bytes"
  "context"
  "fmt"
  "os"
  "path"
  "runtime"
  "sort"
  "strings"
  "time"

  freelog "github.com/whtcorpsinc/MilevaDB/BerolinaSQL/Errors"
  log "github.com/sirupsen/logrus"
  "go.uber.org/zap"
  "gopkg.in/natefinch/lumberjack.v2"
)

const (

  defaultFreeLogTimeFormat = "2020/12/01 11   04:05.000"

  DefaultFreeLogFormat = "text"

 //default to logrus
  defaultFreeLogLevel = log.InfoLevel

  //DefaultFreeLogMaxSize = default log size.
  DefaultFreeLogMaxSize = 300 //MB

  // DefaultQueryLogMaxLen is the default max length of the query in the log.
  DefaultQueryLogMaxLen = 2048


)

// FileLogConfig serializes file log related config in toml/json.
type FileFreeLogConfig struct {
	freelog.FreeFileLogConfig
}



//FreeLogConfig serializes freeelog config in toml/json
type FreeLogConfig struct {
  freelog.FreeLogConfig
}

//Timestamp switch; New a freelog, default to disabled; slow query.
func NewFreeLogConfig(level, format, slowQueryFile string, fileCfg FileFreeLogConfig, disableTimestamp bool) *FreeLogConfig {
	return &FreeLogConfig{
		Config: freelog.Config{
			Level:            level,
			Format:           format,
			DisableTimestamp: disableTimestamp,
			File:             fileCfg.FileLogConfig,
		},
		SlowQueryFile: slowQueryFile,
	}
}
//
 tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/sirupsen/logrus") ||
		strings.Contains(name, "github.com/coreos/pkg/capnslog")
}

//injects file name and line pos into log entry.
type contextHook struct{}


func (hook *contextHook) Fire(entry *log.Entry) error {
	pc := make([]uintptr, 4)
	cnt := runtime.Callers(6, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *contextHook) Levels() []log.Level {
	return log.AllLevels
}

func stringToFreeLogLevel(level string) log.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return log.FatalLevel
	case "error":
		return log.ErrorLevel
	case "warn", "warning":
		return log.WarnLevel
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	}
	return defaultLogLevel
}

// logTypeToColor converts the Level to a color string.
func logTypeToColor(level log.Level) string {
	switch level {
	case log.DebugLevel:
		return "[0;37"
	case log.InfoLevel:
		return "[0;36"
	case log.WarnLevel:
		return "[0;33"
	case log.ErrorLevel:
		return "[0;31"
	case log.FatalLevel:
		return "[0;31"
	case log.PanicLevel:
		return "[0;31"
	}

	return "[0;37"
}

// textFormatter is for compatibility with ngaut/log
type textFormatter struct {
	DisableTimestamp bool
	EnableColors     bool
	EnableEntryOrder bool
}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	if f.EnableColors {
		colorStr := logTypeToColor(entry.Level)
		fmt.Fprintf(b, "\033%sm ", colorStr)
	}

	if !f.DisableTimestamp {
		fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	}
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)

	if f.EnableEntryOrder {
		keys := make([]string, 0, len(entry.Data))
		for k := range entry.Data {
			if k != "file" && k != "line" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(b, " %v=%v", k, entry.Data[k])
		}
	} else {
		for k, v := range entry.Data {
			if k != "file" && k != "line" {
				fmt.Fprintf(b, " %v=%v", k, v)
			}
		})
