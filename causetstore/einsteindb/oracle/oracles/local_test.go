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

package oracles_test

import (
	"context"
	"testing"
	"time"

	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle/oracles"
)

func TestLocalOracle(t *testing.T) {
	l := oracles.NewLocalOracle()
	defer l.Close()
	m := map[uint64]struct{}{}
	for i := 0; i < 100000; i++ {
		ts, err := l.GetTimestamp(context.Background())
		if err != nil {
			t.Error(err)
		}
		m[ts] = struct{}{}
	}

	if len(m) != 100000 {
		t.Error("generated same ts")
	}
}

func TestIsExpired(t *testing.T) {
	o := oracles.NewLocalOracle()
	defer o.Close()
	start := time.Now()
	oracles.SetOracleHookCurrentTime(o, start)
	ts, _ := o.GetTimestamp(context.Background())
	oracles.SetOracleHookCurrentTime(o, start.Add(10*time.Millisecond))
	expire := o.IsExpired(ts, 5)
	if !expire {
		t.Error("should expired")
	}
	expire = o.IsExpired(ts, 200)
	if expire {
		t.Error("should not expired")
	}
}

func TestLocalOracle_UntilExpired(t *testing.T) {
	o := oracles.NewLocalOracle()
	defer o.Close()
	start := time.Now()
	oracles.SetOracleHookCurrentTime(o, start)
	ts, _ := o.GetTimestamp(context.Background())
	oracles.SetOracleHookCurrentTime(o, start.Add(10*time.Millisecond))
	if o.UntilExpired(ts, 5) != -5 || o.UntilExpired(ts, 15) != 5 {
		t.Error("until expired should be +-5")
	}
}
