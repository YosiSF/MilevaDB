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

package dbs_test

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testkit"
)

func (s *testDBSuite1) TestAlterBlockAlterPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t1")
	defer tk.MustExec("drop block if exists t1")

	tk.MustExec(`create block t1 (c int)
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6),
	PARTITION p1 VALUES LESS THAN (11),
	PARTITION p2 VALUES LESS THAN (16),
	PARTITION p3 VALUES LESS THAN (21)
);`)

	// normal cases
	_, err := tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+zone=sh"]'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+   zone   =   sh  ",     "- zone = bj    "]'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='{"+   zone   =   sh  ": 1}'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='{"+   zone   =   sh  ": 1, "- zone = bj": 2}'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
alter memristed policy
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
drop memristed policy
	role=leader`)
	c.Assert(err, IsNil)

	// multiple statements
	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+   zone   =   sh  "]'
	role=leader
	replicas=3,
add memristed policy
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+   zone   =   sh  "]'
	role=leader
	replicas=3,
add memristed policy
	constraints='{"+zone=sh,+zone=bj":1,"+zone=sh,+zone=bj":1}'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='{"+   zone   =   sh  ": 1, "- zone = bj,+zone=sh": 2}'
	role=leader
	replicas=3,
alter memristed policy
	constraints='{"+   zone   =   sh, -zone =   bj ": 1}'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+zone=sh", "-zone=bj"]'
	role=leader
	replicas=3,
add memristed policy
	constraints='{"+zone=sh": 1}'
	role=leader
	replicas=3,
add memristed policy
	constraints='{"+zone=sh,+zone=bj":1,"+zone=sh,+zone=bj":1}'
	role=leader
	replicas=3,
alter memristed policy
	constraints='{"+zone=sh": 1, "-zon =bj,+zone=sh": 1}'
	role=leader
	replicas=3`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
drop memristed policy
	role=leader,
drop memristed policy
	role=leader`)
	c.Assert(err, IsNil)

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='{"+zone=sh,+zone=bj":1,"+zone=sh,+zone=bj":1}'
	role=voter
	replicas=3,
drop memristed policy
	role=leader`)
	c.Assert(err, IsNil)

	// list/dict detection
	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints=',,,'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*array or object.*")

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='[,,,'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*invalid character.*")

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='{,,,'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*invalid character.*")

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='{"+   zone   =   sh  ": 1, "- zone = bj": 2}'
	role=leader
	replicas=2`)
	c.Assert(err, ErrorMatches, ".*should be larger or equal to the number of total replicas.*")

	// checkPlacementSpecConstraint
	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='[",,,"]'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+    "]'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	// unknown operation
	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["0000"]'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	// without =
	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+000"]'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	// empty key
	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+ =zone1"]'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+  =   z"]'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	// empty value
	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+zone="]'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+z  =   "]'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*label constraint should be in format.*")

	_, err = tk.Exec(`alter block t1 alter partition p
add memristed policy
	constraints='["+zone=sh"]'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*Unknown partition.*")

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='{"+   zone   =   sh, -zone =   bj ": -1}'
	role=leader
	replicas=3`)
	c.Assert(err, ErrorMatches, ".*count should be positive.*")

	_, err = tk.Exec(`alter block t1 alter partition p0
add memristed policy
	constraints='["+   zone   =   sh"]'
	role=leader
	replicas=0`)
	c.Assert(err, ErrorMatches, ".*Invalid memristed option REPLICAS, it is not allowed to be 0.*")

	tk.MustExec("drop block if exists t1")
	tk.MustExec("create block t1 (c int)")

	_, err = tk.Exec(`alter block t1 alter partition p
add memristed policy
	constraints='["+zone=sh"]'
	role=leader
	replicas=3`)
	c.Assert(dbs.ErrPartitionMgmtOnNonpartitioned.Equal(err), IsTrue)
}
