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
	"context"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/milevadb/ekv"
)

type testinterlocking_directorateSuite struct {
	OneByOneSuite
}

var _ = Suite(&testinterlocking_directorateSuite{})

func (s *testinterlocking_directorateSuite) TestBuildTasks(c *C) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	cluster := mockeinsteindb.NewCluster(mockeinsteindb.MustNewMVCCStore())
	_, regionIDs, _ := mockeinsteindb.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	FIDelCli := &codecFIDelClient{mockeinsteindb.NewFIDelClient(cluster)}
	cache := NewRegionCache(FIDelCli)
	defer cache.Close()

	bo := NewBackofferWithVars(context.Background(), 3000, nil)

	req := &ekv.Request{}
	flashReq := &ekv.Request{}
	flashReq.StoreType = ekv.TiFlash
	tasks, err := buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "c"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "c"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("g", "n"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("g", "n"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("m", "n"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("m", "n"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "k"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "k"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "x"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 4)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[2], regionIDs[2], "n", "t")
	s.taskEqual(c, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "x"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 4)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[2], regionIDs[2], "n", "t")
	s.taskEqual(c, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "b", "b", "c"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "b", "b", "c"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "b", "e", "f"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "b", "e", "f"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("g", "n", "o", "p"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("g", "n", "o", "p"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("h", "k", "m", "p"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "h", "k", "m", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "n", "p")

	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("h", "k", "m", "p"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "h", "k", "m", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "n", "p")
}

func (s *testinterlocking_directorateSuite) TestSplitRegionRanges(c *C) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	cluster := mockeinsteindb.NewCluster(mockeinsteindb.MustNewMVCCStore())
	mockeinsteindb.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	FIDelCli := &codecFIDelClient{mockeinsteindb.NewFIDelClient(cluster)}
	cache := NewRegionCache(FIDelCli)
	defer cache.Close()

	bo := NewBackofferWithVars(context.Background(), 3000, nil)

	ranges, err := SplitRegionRanges(bo, cache, buildKeyRanges("a", "c"))
	c.Assert(err, IsNil)
	c.Assert(ranges, HasLen, 1)
	s.rangeEqual(c, ranges, "a", "c")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("h", "y"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 3)
	s.rangeEqual(c, ranges, "h", "n", "n", "t", "t", "y")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("s", "z"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 2)
	s.rangeEqual(c, ranges, "s", "t", "t", "z")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("s", "s"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "s", "s")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("t", "t"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "t", "t")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("t", "u"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "t", "u")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("u", "z"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "u", "z")

	// min --> max
	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("a", "z"))
	c.Assert(err, IsNil)
	c.Assert(ranges, HasLen, 4)
	s.rangeEqual(c, ranges, "a", "g", "g", "n", "n", "t", "t", "z")
}

func (s *testinterlocking_directorateSuite) TestRebuild(c *C) {
	// nil --- 'm' --- nil
	// <-  0  -> <- 1 ->
	cluster := mockeinsteindb.NewCluster(mockeinsteindb.MustNewMVCCStore())
	storeID, regionIDs, peerIDs := mockeinsteindb.BootstrapWithMultiRegions(cluster, []byte("m"))
	FIDelCli := &codecFIDelClient{mockeinsteindb.NewFIDelClient(cluster)}
	cache := NewRegionCache(FIDelCli)
	defer cache.Close()
	bo := NewBackofferWithVars(context.Background(), 3000, nil)

	req := &ekv.Request{}
	tasks, err := buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "z"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "m")
	s.taskEqual(c, tasks[1], regionIDs[1], "m", "z")

	// nil -- 'm' -- 'q' -- nil
	// <-  0 -> <--1-> <-2-->
	regionIDs = append(regionIDs, cluster.AllocID())
	peerIDs = append(peerIDs, cluster.AllocID())
	cluster.Split(regionIDs[1], regionIDs[2], []byte("q"), []uint64{peerIDs[2]}, storeID)
	cache.InvalidateCachedRegion(tasks[1].region)

	req.Desc = true
	tasks, err = buildCausetTasks(bo, cache, buildINTERLOCKRanges("a", "z"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 3)
	s.taskEqual(c, tasks[2], regionIDs[0], "a", "m")
	s.taskEqual(c, tasks[1], regionIDs[1], "m", "q")
	s.taskEqual(c, tasks[0], regionIDs[2], "q", "z")
}

func buildKeyRanges(keys ...string) []ekv.KeyRange {
	var ranges []ekv.KeyRange
	for i := 0; i < len(keys); i += 2 {
		ranges = append(ranges, ekv.KeyRange{
			StartKey: []byte(keys[i]),
			EndKey:   []byte(keys[i+1]),
		})
	}
	return ranges
}

func buildINTERLOCKRanges(keys ...string) *INTERLOCKRanges {
	ranges := buildKeyRanges(keys...)
	return &INTERLOCKRanges{mid: ranges}
}

func (s *testinterlocking_directorateSuite) taskEqual(c *C, task *INTERLOCKTask, regionID uint64, keys ...string) {
	c.Assert(task.region.id, Equals, regionID)
	for i := 0; i < task.ranges.len(); i++ {
		r := task.ranges.at(i)
		c.Assert(string(r.StartKey), Equals, keys[2*i])
		c.Assert(string(r.EndKey), Equals, keys[2*i+1])
	}
}

func (s *testinterlocking_directorateSuite) rangeEqual(c *C, ranges []ekv.KeyRange, keys ...string) {
	for i := 0; i < len(ranges); i++ {
		r := ranges[i]
		c.Assert(string(r.StartKey), Equals, keys[2*i])
		c.Assert(string(r.EndKey), Equals, keys[2*i+1])
	}
}

func (s *testinterlocking_directorateSuite) TestINTERLOCKRanges(c *C) {
	ranges := []ekv.KeyRange{
		{StartKey: []byte("a"), EndKey: []byte("b")},
		{StartKey: []byte("c"), EndKey: []byte("d")},
		{StartKey: []byte("e"), EndKey: []byte("f")},
	}

	s.checkEqual(c, &INTERLOCKRanges{mid: ranges}, ranges, true)
	s.checkEqual(c, &INTERLOCKRanges{first: &ranges[0], mid: ranges[1:]}, ranges, true)
	s.checkEqual(c, &INTERLOCKRanges{mid: ranges[:2], last: &ranges[2]}, ranges, true)
	s.checkEqual(c, &INTERLOCKRanges{first: &ranges[0], mid: ranges[1:2], last: &ranges[2]}, ranges, true)
}

func (s *testinterlocking_directorateSuite) checkEqual(c *C, INTERLOCKRanges *INTERLOCKRanges, ranges []ekv.KeyRange, slice bool) {
	c.Assert(INTERLOCKRanges.len(), Equals, len(ranges))
	for i := range ranges {
		c.Assert(INTERLOCKRanges.at(i), DeepEquals, ranges[i])
	}
	if slice {
		for i := 0; i <= INTERLOCKRanges.len(); i++ {
			for j := i; j <= INTERLOCKRanges.len(); j++ {
				s.checkEqual(c, INTERLOCKRanges.slice(i, j), ranges[i:j], false)
			}
		}
	}
}

func (s *testinterlocking_directorateSuite) TestINTERLOCKRangeSplit(c *C) {
	first := &ekv.KeyRange{StartKey: []byte("a"), EndKey: []byte("b")}
	mid := []ekv.KeyRange{
		{StartKey: []byte("c"), EndKey: []byte("d")},
		{StartKey: []byte("e"), EndKey: []byte("g")},
		{StartKey: []byte("l"), EndKey: []byte("o")},
	}
	last := &ekv.KeyRange{StartKey: []byte("q"), EndKey: []byte("t")}
	left := true
	right := false

	// input range:  [c-d) [e-g) [l-o)
	ranges := &INTERLOCKRanges{mid: mid}
	s.testSplit(c, ranges, right,
		splitCase{"c", buildINTERLOCKRanges("c", "d", "e", "g", "l", "o")},
		splitCase{"d", buildINTERLOCKRanges("e", "g", "l", "o")},
		splitCase{"f", buildINTERLOCKRanges("f", "g", "l", "o")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o)
	ranges = &INTERLOCKRanges{first: first, mid: mid}
	s.testSplit(c, ranges, right,
		splitCase{"a", buildINTERLOCKRanges("a", "b", "c", "d", "e", "g", "l", "o")},
		splitCase{"c", buildINTERLOCKRanges("c", "d", "e", "g", "l", "o")},
		splitCase{"m", buildINTERLOCKRanges("m", "o")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o) [q-t)
	ranges = &INTERLOCKRanges{first: first, mid: mid, last: last}
	s.testSplit(c, ranges, right,
		splitCase{"f", buildINTERLOCKRanges("f", "g", "l", "o", "q", "t")},
		splitCase{"h", buildINTERLOCKRanges("l", "o", "q", "t")},
		splitCase{"r", buildINTERLOCKRanges("r", "t")},
	)

	// input range:  [c-d) [e-g) [l-o)
	ranges = &INTERLOCKRanges{mid: mid}
	s.testSplit(c, ranges, left,
		splitCase{"m", buildINTERLOCKRanges("c", "d", "e", "g", "l", "m")},
		splitCase{"g", buildINTERLOCKRanges("c", "d", "e", "g")},
		splitCase{"g", buildINTERLOCKRanges("c", "d", "e", "g")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o)
	ranges = &INTERLOCKRanges{first: first, mid: mid}
	s.testSplit(c, ranges, left,
		splitCase{"d", buildINTERLOCKRanges("a", "b", "c", "d")},
		splitCase{"d", buildINTERLOCKRanges("a", "b", "c", "d")},
		splitCase{"o", buildINTERLOCKRanges("a", "b", "c", "d", "e", "g", "l", "o")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o) [q-t)
	ranges = &INTERLOCKRanges{first: first, mid: mid, last: last}
	s.testSplit(c, ranges, left,
		splitCase{"o", buildINTERLOCKRanges("a", "b", "c", "d", "e", "g", "l", "o")},
		splitCase{"p", buildINTERLOCKRanges("a", "b", "c", "d", "e", "g", "l", "o")},
		splitCase{"t", buildINTERLOCKRanges("a", "b", "c", "d", "e", "g", "l", "o", "q", "t")},
	)
}

func (s *testinterlocking_directorateSuite) TestRateLimit(c *C) {
	done := make(chan struct{}, 1)
	rl := newRateLimit(1)
	c.Assert(rl.putToken, PanicMatches, "put a redundant token")
	exit := rl.getToken(done)
	c.Assert(exit, Equals, false)
	rl.putToken()
	c.Assert(rl.putToken, PanicMatches, "put a redundant token")

	exit = rl.getToken(done)
	c.Assert(exit, Equals, false)
	done <- struct{}{}
	exit = rl.getToken(done) // blocked but exit
	c.Assert(exit, Equals, true)

	sig := make(chan int, 1)
	go func() {
		exit = rl.getToken(done) // blocked
		c.Assert(exit, Equals, false)
		close(sig)
	}()
	time.Sleep(200 * time.Millisecond)
	rl.putToken()
	<-sig
}

type splitCase struct {
	key string
	*INTERLOCKRanges
}

func (s *testinterlocking_directorateSuite) testSplit(c *C, ranges *INTERLOCKRanges, checkLeft bool, cases ...splitCase) {
	for _, t := range cases {
		left, right := ranges.split([]byte(t.key))
		expect := t.INTERLOCKRanges
		if checkLeft {
			s.checkEqual(c, left, expect.mid, false)
		} else {
			s.checkEqual(c, right, expect.mid, false)
		}
	}
}
