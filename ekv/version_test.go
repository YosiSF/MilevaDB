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

package ekv

import (
	. "github.com/whtcorpsinc/check"
)

var _ = Suite(testVersionSuite{})

type testVersionSuite struct{}

func (s testVersionSuite) TestVersion(c *C) {
	le := NewVersion(42).Cmp(NewVersion(43))
	gt := NewVersion(42).Cmp(NewVersion(41))
	eq := NewVersion(42).Cmp(NewVersion(42))

	c.Assert(le < 0, IsTrue)
	c.Assert(gt > 0, IsTrue)
	c.Assert(eq == 0, IsTrue)

	c.Check(MinVersion.Cmp(MaxVersion) < 0, IsTrue)
}
