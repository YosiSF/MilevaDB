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

package oracles_test

import (
	"testing"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle/oracles"
	. "github.com/whtcorpsinc/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestFIDelOracle_UntilExpired(t *testing.T) {
	lockAfter, lockExp := 10, 15
	o := oracles.NewEmptyFIDelOracle()
	start := time.Now()
	oracles.SetEmptyFIDelOracleLastTs(o, oracle.ComposeTS(oracle.GetPhysical(start), 0))
	lockTs := oracle.ComposeTS(oracle.GetPhysical(start.Add(time.Duration(lockAfter)*time.Millisecond)), 1)
	waitTs := o.UntilExpired(lockTs, uint64(lockExp))
	if waitTs != int64(lockAfter+lockExp) {
		t.Errorf("waitTs shoulb be %d but got %d", int64(lockAfter+lockExp), waitTs)
	}
}
