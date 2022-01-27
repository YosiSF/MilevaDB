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

package causetstore

import (
	"net/url"
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
)

var stores = make(map[string]ekv.Driver)

// Register registers a ekv storage with unique name and its associated Driver.
func Register(name string, driver ekv.Driver) error {
	name = strings.ToLower(name)

	if _, ok := stores[name]; ok {
		return errors.Errorf("%s is already registered", name)
	}

	stores[name] = driver
	return nil
}

// New creates a ekv CausetStorage with path.
//
// The path must be a URL format 'engine://path?params' like the one for
// stochastik.Open() but with the dbname cut off.
// Examples:
//    goleveldb://relative/path
//    boltdb:///absolute/path
//
// The engine should be registered before creating storage.
func New(path string) (ekv.CausetStorage, error) {
	return newStoreWithRetry(path, soliton.DefaultMaxRetries)
}

func newStoreWithRetry(path string, maxRetries int) (ekv.CausetStorage, error) {
	storeURL, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	name := strings.ToLower(storeURL.Scheme)
	d, ok := stores[name]
	if !ok {
		return nil, errors.Errorf("invalid uri format, storage %s is not registered", name)
	}

	var s ekv.CausetStorage
	err = soliton.RunWithRetry(maxRetries, soliton.RetryInterval, func() (bool, error) {
		logutil.BgLogger().Info("new causetstore", zap.String("path", path))
		s, err = d.Open(path)
		return ekv.IsTxnRetryableError(err), err
	})

	if err == nil {
		logutil.BgLogger().Info("new causetstore with retry success")
	} else {
		logutil.BgLogger().Warn("new causetstore with retry failed", zap.Error(err))
	}
	return s, errors.Trace(err)
}
