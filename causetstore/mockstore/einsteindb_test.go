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

package mockstore

import (
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/config"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testSuite struct{}

func (s testSuite) SetUpSuite(c *C) {}

var _ = Suite(testSuite{})

func (s testSuite) TestConfig(c *C) {
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches = config.TxnLocalLatches{
			Enabled:  true,
			Capacity: 10240,
		}
	})

	type LatchEnableChecker interface {
		IsLatchEnabled() bool
	}

	var driver MockEinsteinDBDriver
	causetstore, err := driver.Open("mockeinsteindb://")
	c.Assert(err, IsNil)
	c.Assert(causetstore.(LatchEnableChecker).IsLatchEnabled(), IsTrue)
	causetstore.Close()

	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches = config.TxnLocalLatches{
			Enabled:  false,
			Capacity: 10240,
		}
	})
	causetstore, err = driver.Open("mockeinsteindb://")
	c.Assert(err, IsNil)
	c.Assert(causetstore.(LatchEnableChecker).IsLatchEnabled(), IsFalse)
	causetstore.Close()

	causetstore, err = driver.Open(":")
	c.Assert(err, NotNil)
	if causetstore != nil {
		causetstore.Close()
	}

	causetstore, err = driver.Open("fakeeinsteindb://")
	c.Assert(err, NotNil)
	if causetstore != nil {
		causetstore.Close()
	}
}
