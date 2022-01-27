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

package einsteindb

import (
	. "github.com/whtcorpsinc/check"
)

// OneByOneSuite is a suite, When with-einsteindb flag is true, there is only one storage, so the test suite have to run one by one.
type OneByOneSuite struct{}

func (s *OneByOneSuite) SetUpSuite(c *C) {
	if *WithEinsteinDB {
		withEinsteinDBGlobalLock.Lock()
	} else {
		withEinsteinDBGlobalLock.RLock()
	}
}

func (s *OneByOneSuite) TearDownSuite(c *C) {
	if *WithEinsteinDB {
		withEinsteinDBGlobalLock.Unlock()
	} else {
		withEinsteinDBGlobalLock.RUnlock()
	}
}

type testEinsteinDBSuite struct {
	OneByOneSuite
}

var _ = Suite(&testEinsteinDBSuite{})
