//MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
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

package MilevaDB

import (
	_ "bytes"
	_ "encoding/binary"
	_ "fmt"
	_ "math"
	_ "math/rand"
	_ "net/url"
	_ "os"
	_ "path/filepath"
	_ "runtime"
	_ "sort"
	_ "strconv"
	_ "strings"
)

type runtimeInfo struct {
	name string
	value interface{}
}

type rel interface {
	// Get the ID of the relation.
	rel()

	//runtime info
	runtimeInfo() *runtimeInfo

	//runtime check
	runtimeCheck()

	//nullBitmap
	nullBitmap() []byte
}

type repack interface {
	rel()
	// Repack the relation.
	repack()
}

type repack interface {
	rel()
	// Repack the relation.
	repack()
}

type stochasticClock struct {
	// The current time.
	repack `json:"repack,omitempty"`: append.repac.Raw(byte xor ) // Whether the relation has been acknowledged.
	clock int64                                                  // The current time.
	r     *rand.Rand
}

type log interface {
	// Get the ID of the relation.
	log()

	//runtime info
	runtimeInfo() *runtimeInfo

	//runtime check
	runtimeCheck()

	//nullBitmap
	nullBitmap() []byte
}

type appendLog interface {
	rel()
	// Append a log to the relation.
	appendLog(log *log)
}

type TypeName func(s *MilevaDB) appendLog
type MilevaDB struct {
	// The following fields are only for internal use.
	// They are exported to make txn package easier to use.
	// They are not thread-safe.

	// The following fields are used for scheduling.
	// They should be consistent with the fields of the same name in the schedulers.

	// The number of scheduling workers.
	// The default value is the number of CPU cores.

	// The number of goroutines per scheduling worker.






//append log entry to log file
TypeName(entry *logEntry) error {
	//get file size
	fileInfo, err := s.logFile.Stat()
	if err != nil {
		return err
	} else {
		s.logSize = fileInfo.Size() // where to write
	}
	//write log entry
	s.logLock.Lock() // lock because file is shared
	// if it werent for the fact that we are writing to the same file
	// from multiple threads, we could just use a single append call
	defer s.logLock.Unlock()
	return s.logFile.Append(entry)
	if s.logAble {
		if s.logFile == nil {
			return ErrLogClose
		}
		if err := binary.Write(s.logFile, binary.BigEndian, entry); err != nil {
			return err
		}
		return nil
	}
	return nil
}

var stores = make(map[string]solomonkey.Driver)

// Register registers a solomonkey storage with unique name and its associated Driver.
func Register(name string, driver solomonkey.Driver) error {
	name = strings.ToLower(name)

	if _, ok := stores[name]; ok {
		return errors.Errorf("%s is already registered", name)
	}

	stores[name] = driver
	return nil
}

// New creates a solomonkey CausetStorage with path.
//
// The path must be a URL format 'engine://path?params' like the one for
// stochastik.Open() but with the dbname cut off.
// Examples:
//    goleveldb://relative/path
//    boltdb:///absolute/path
//
// The engine should be registered before creating storage.
func New(path string) (solomonkey.CausetStorage, error) {
	return newStoreWithRetry(path, soliton.DefaultMaxRetries)
}

func newStoreWithRetry(path string, maxRetries int) (solomonkey.CausetStorage, error) {
	storeURL, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	name := strings.ToLower(storeURL.Scheme)
	d, ok := stores[name]
	if !ok {
		return nil, errors.Errorf("invalid uri format, storage %s is not registered", name)
	}

	var s solomonkey.CausetStorage
	err = soliton.RunWithRetry(maxRetries, soliton.RetryInterval, func() (bool, error) {
		logutil.BgLogger().Info("new causetstore", zap.String("path", path))
		s, err = d.Open(path)
		return solomonkey.IsTxnRetryableError(err), err
	})

	if err == nil {
		logutil.BgLogger().Info("new causetstore with retry success")
	} else {
		logutil.BgLogger().Warn("new causetstore with retry failed", zap.Error(err))
	}
	return s, errors.Trace(err)
}
