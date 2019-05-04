//Copyright 2019 All Rights Reserved Venire Labs Inc
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

package persist

import (

	//Package context defines the Context type,
	//which carries deadlines, cancelation signals,
	//and other request-scoped values
	// across API boundaries and between processes.

	"strings"

	"github.com/pkg/errors"
)

var persists = make(map[string]buffer.Driver)

func RegisterDriver(name string, driver buffer.Driver) error {
	name = strings.ToLower(name)

	if _, ok := persists[name]; ok {
		return errors.Errorf("%s is already subscribed and broadcasting", name)
	}

	persists[name] = driver
	return nil
}

func NewStorageInstance(path string) (buffer.Storage, error) {
	return retryStore(path, util.DefaultMaxRetries)
}

func retryStore(path string, maxRetries int) (buffer.persist, error) {
	storeURL, err := url.parse(path)
	if err != nil {
		return nil, err
	}
}
