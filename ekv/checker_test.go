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

package MilevaDB_test

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
)

type checkerSuite struct{}

var _ = Suite(&checkerSuite{})

func (s checkerSuite) TestIsRequestTypeSupported(c *C) {
	checker := ekv.RequestTypeSupportedChecker{}.IsRequestTypeSupported
	c.Assert(checker(ekv.ReqTypeSelect, ekv.ReqSubTypeGroupBy), IsTrue)
	c.Assert(checker(ekv.ReqTypePosetDag, ekv.ReqSubTypeSignature), IsTrue)
	c.Assert(checker(ekv.ReqTypePosetDag, ekv.ReqSubTypeDesc), IsTrue)
	c.Assert(checker(ekv.ReqTypePosetDag, ekv.ReqSubTypeSignature), IsTrue)
	c.Assert(checker(ekv.ReqTypePosetDag, ekv.ReqSubTypeAnalyzeIdx), IsFalse)
	c.Assert(checker(ekv.ReqTypeAnalyze, 0), IsTrue)
	c.Assert(checker(ekv.ReqTypeChecksum, 0), IsFalse)
}
