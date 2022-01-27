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

package petri

import (
	"math/rand"
	"sync"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
)

type leaseGrantItem struct {
	leaseGrantTS uint64
	oldVer       int64
	schemaVer    int64
}

func (*testSuite) TestSchemaValidator(c *C) {
	defer testleak.AfterTest(c)()

	lease := 10 * time.Millisecond
	leaseGrantCh := make(chan leaseGrantItem)
	oracleCh := make(chan uint64)
	exit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go serverFunc(lease, leaseGrantCh, oracleCh, exit, &wg)

	validator := NewSchemaValidator(lease, nil).(*schemaValidator)
	c.Assert(validator.IsStarted(), IsTrue)

	for i := 0; i < 10; i++ {
		delay := time.Duration(100+rand.Intn(900)) * time.Microsecond
		time.Sleep(delay)
		// Reload can run arbitrarily, at any time.
		item := <-leaseGrantCh
		validator.UFIDelate(item.leaseGrantTS, item.oldVer, item.schemaVer, nil)
	}

	// Take a lease, check it's valid.
	item := <-leaseGrantCh
	validator.UFIDelate(item.leaseGrantTS, item.oldVer, item.schemaVer,
		&einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{10}, CausetActionTypes: []uint64{10}})
	_, valid := validator.Check(item.leaseGrantTS, item.schemaVer, []int64{10})
	c.Assert(valid, Equals, ResultSucc)

	// Stop the validator, validator's items value is nil.
	validator.Stop()
	c.Assert(validator.IsStarted(), IsFalse)
	_, isBlocksChanged := validator.isRelatedBlocksChanged(item.schemaVer, []int64{10})
	c.Assert(isBlocksChanged, IsTrue)
	_, valid = validator.Check(item.leaseGrantTS, item.schemaVer, []int64{10})
	c.Assert(valid, Equals, ResultUnknown)
	validator.Restart()

	// Increase the current time by 2 leases, check schemaReplicant is invalid.
	ts := uint64(time.Now().Add(2 * lease).UnixNano()) // Make sure that ts has timed out a lease.
	_, valid = validator.Check(ts, item.schemaVer, []int64{10})
	c.Assert(valid, Equals, ResultUnknown, Commentf("validator latest schemaReplicant ver %v, time %v, item schemaReplicant ver %v, ts %v",
		validator.latestSchemaVer, validator.latestSchemaExpire, 0, oracle.GetTimeFromTS(ts)))
	// Make sure newItem's version is greater than item.schemaReplicant.
	newItem := getGreaterVersionItem(c, lease, leaseGrantCh, item.schemaVer)
	currVer := newItem.schemaVer
	validator.UFIDelate(newItem.leaseGrantTS, newItem.oldVer, currVer, nil)
	_, valid = validator.Check(ts, item.schemaVer, nil)
	c.Assert(valid, Equals, ResultFail, Commentf("currVer %d, newItem %v", currVer, item))
	_, valid = validator.Check(ts, item.schemaVer, []int64{0})
	c.Assert(valid, Equals, ResultFail, Commentf("currVer %d, newItem %v", currVer, item))
	// Check the latest schemaReplicant version must changed.
	c.Assert(item.schemaVer, Less, validator.latestSchemaVer)

	// Make sure newItem's version is greater than currVer.
	newItem = getGreaterVersionItem(c, lease, leaseGrantCh, currVer)
	// UFIDelate current schemaReplicant version to newItem's version and the delta block IDs is 1, 2, 3.
	validator.UFIDelate(ts, currVer, newItem.schemaVer, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{1, 2, 3}, CausetActionTypes: []uint64{1, 2, 3}})
	// Make sure the uFIDelated block IDs don't be covered with the same schemaReplicant version.
	validator.UFIDelate(ts, newItem.schemaVer, newItem.schemaVer, nil)
	_, isBlocksChanged = validator.isRelatedBlocksChanged(currVer, nil)
	c.Assert(isBlocksChanged, IsFalse)
	_, isBlocksChanged = validator.isRelatedBlocksChanged(currVer, []int64{2})
	c.Assert(isBlocksChanged, IsTrue, Commentf("currVer %d, newItem %v", currVer, newItem))
	// The current schemaReplicant version is older than the oldest schemaReplicant version.
	_, isBlocksChanged = validator.isRelatedBlocksChanged(-1, nil)
	c.Assert(isBlocksChanged, IsTrue, Commentf("currVer %d, newItem %v", currVer, newItem))

	// All schemaReplicant versions is expired.
	ts = uint64(time.Now().Add(2 * lease).UnixNano())
	_, valid = validator.Check(ts, newItem.schemaVer, nil)
	c.Assert(valid, Equals, ResultUnknown)

	close(exit)
	wg.Wait()
}

func getGreaterVersionItem(c *C, lease time.Duration, leaseGrantCh chan leaseGrantItem, currVer int64) leaseGrantItem {
	var newItem leaseGrantItem
	for i := 0; i < 10; i++ {
		time.Sleep(lease / 2)
		newItem = <-leaseGrantCh
		if newItem.schemaVer > currVer {
			break
		}
	}
	c.Assert(newItem.schemaVer, Greater, currVer, Commentf("currVer %d, newItem %v", currVer, newItem))

	return newItem
}

// serverFunc plays the role as a remote server, runs in a separate goroutine.
// It can grant lease and provide timestamp oracle.
// Caller should communicate with it through channel to mock network.
func serverFunc(lease time.Duration, requireLease chan leaseGrantItem, oracleCh chan uint64, exit chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var version int64
	leaseTS := uint64(time.Now().UnixNano())
	ticker := time.NewTicker(lease)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			version++
			leaseTS = uint64(now.UnixNano())
		case requireLease <- leaseGrantItem{
			leaseGrantTS: leaseTS,
			oldVer:       version - 1,
			schemaVer:    version,
		}:
		case oracleCh <- uint64(time.Now().UnixNano()):
		case <-exit:
			return
		}
	}
}

func (*testSuite) TestEnqueue(c *C) {
	lease := 10 * time.Millisecond
	originalCnt := variable.GetMaxDeltaSchemaCount()
	defer variable.SetMaxDeltaSchemaCount(originalCnt)

	validator := NewSchemaValidator(lease, nil).(*schemaValidator)
	c.Assert(validator.IsStarted(), IsTrue)
	// maxCnt is 0.
	variable.SetMaxDeltaSchemaCount(0)
	validator.enqueue(1, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{11}, CausetActionTypes: []uint64{11}})
	c.Assert(validator.deltaSchemaInfos, HasLen, 0)

	// maxCnt is 10.
	variable.SetMaxDeltaSchemaCount(10)
	ds := []deltaSchemaInfo{
		{0, []int64{1}, []uint64{1}},
		{1, []int64{1}, []uint64{1}},
		{2, []int64{1}, []uint64{1}},
		{3, []int64{2, 2}, []uint64{2, 2}},
		{4, []int64{2}, []uint64{2}},
		{5, []int64{1, 4}, []uint64{1, 4}},
		{6, []int64{1, 4}, []uint64{1, 4}},
		{7, []int64{3, 1, 3}, []uint64{3, 1, 3}},
		{8, []int64{1, 2, 3}, []uint64{1, 2, 3}},
		{9, []int64{1, 2, 3}, []uint64{1, 2, 3}},
	}
	for _, d := range ds {
		validator.enqueue(d.schemaVersion, &einsteindb.RelatedSchemaChange{PhyTblIDS: d.relatedIDs, CausetActionTypes: d.relatedCausetActions})
	}
	validator.enqueue(10, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{1}, CausetActionTypes: []uint64{1}})
	ret := []deltaSchemaInfo{
		{0, []int64{1}, []uint64{1}},
		{2, []int64{1}, []uint64{1}},
		{3, []int64{2, 2}, []uint64{2, 2}},
		{4, []int64{2}, []uint64{2}},
		{6, []int64{1, 4}, []uint64{1, 4}},
		{9, []int64{1, 2, 3}, []uint64{1, 2, 3}},
		{10, []int64{1}, []uint64{1}},
	}
	c.Assert(validator.deltaSchemaInfos, DeepEquals, ret)
	// The Items' relatedBlockIDs have different order.
	validator.enqueue(11, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{1, 2, 3, 4}, CausetActionTypes: []uint64{1, 2, 3, 4}})
	validator.enqueue(12, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{4, 1, 2, 3, 1}, CausetActionTypes: []uint64{4, 1, 2, 3, 1}})
	validator.enqueue(13, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{4, 1, 3, 2, 5}, CausetActionTypes: []uint64{4, 1, 3, 2, 5}})
	ret[len(ret)-1] = deltaSchemaInfo{13, []int64{4, 1, 3, 2, 5}, []uint64{4, 1, 3, 2, 5}}
	c.Assert(validator.deltaSchemaInfos, DeepEquals, ret)
	// The length of deltaSchemaInfos is greater then maxCnt.
	validator.enqueue(14, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{1}, CausetActionTypes: []uint64{1}})
	validator.enqueue(15, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{2}, CausetActionTypes: []uint64{2}})
	validator.enqueue(16, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{3}, CausetActionTypes: []uint64{3}})
	validator.enqueue(17, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{4}, CausetActionTypes: []uint64{4}})
	ret = append(ret, deltaSchemaInfo{14, []int64{1}, []uint64{1}})
	ret = append(ret, deltaSchemaInfo{15, []int64{2}, []uint64{2}})
	ret = append(ret, deltaSchemaInfo{16, []int64{3}, []uint64{3}})
	ret = append(ret, deltaSchemaInfo{17, []int64{4}, []uint64{4}})
	c.Assert(validator.deltaSchemaInfos, DeepEquals, ret[1:])
}

func (*testSuite) TestEnqueueCausetActionType(c *C) {
	lease := 10 * time.Millisecond
	originalCnt := variable.GetMaxDeltaSchemaCount()
	defer variable.SetMaxDeltaSchemaCount(originalCnt)

	validator := NewSchemaValidator(lease, nil).(*schemaValidator)
	c.Assert(validator.IsStarted(), IsTrue)
	// maxCnt is 0.
	variable.SetMaxDeltaSchemaCount(0)
	validator.enqueue(1, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{11}, CausetActionTypes: []uint64{11}})
	c.Assert(validator.deltaSchemaInfos, HasLen, 0)

	// maxCnt is 10.
	variable.SetMaxDeltaSchemaCount(10)
	ds := []deltaSchemaInfo{
		{0, []int64{1}, []uint64{1}},
		{1, []int64{1}, []uint64{1}},
		{2, []int64{1}, []uint64{1}},
		{3, []int64{2, 2}, []uint64{2, 2}},
		{4, []int64{2}, []uint64{2}},
		{5, []int64{1, 4}, []uint64{1, 4}},
		{6, []int64{1, 4}, []uint64{1, 4}},
		{7, []int64{3, 1, 3}, []uint64{3, 1, 3}},
		{8, []int64{1, 2, 3}, []uint64{1, 2, 3}},
		{9, []int64{1, 2, 3}, []uint64{1, 2, 4}},
	}
	for _, d := range ds {
		validator.enqueue(d.schemaVersion, &einsteindb.RelatedSchemaChange{PhyTblIDS: d.relatedIDs, CausetActionTypes: d.relatedCausetActions})
	}
	validator.enqueue(10, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{1}, CausetActionTypes: []uint64{15}})
	ret := []deltaSchemaInfo{
		{0, []int64{1}, []uint64{1}},
		{2, []int64{1}, []uint64{1}},
		{3, []int64{2, 2}, []uint64{2, 2}},
		{4, []int64{2}, []uint64{2}},
		{6, []int64{1, 4}, []uint64{1, 4}},
		{8, []int64{1, 2, 3}, []uint64{1, 2, 3}},
		{9, []int64{1, 2, 3}, []uint64{1, 2, 4}},
		{10, []int64{1}, []uint64{15}},
	}
	c.Assert(validator.deltaSchemaInfos, DeepEquals, ret)
	// Check the flag set by schemaReplicant diff, note blockID = 3 has been set flag 0x3 in schemaReplicant version 9, and flag 0x4
	// in schemaReplicant version 10, so the resCausetActions for blockID = 3 should be 0x3 & 0x4 = 0x7.
	relatedChanges, isBlocksChanged := validator.isRelatedBlocksChanged(5, []int64{1, 2, 3, 4})
	c.Assert(isBlocksChanged, Equals, true)
	c.Assert(relatedChanges.PhyTblIDS, DeepEquals, []int64{1, 2, 3, 4})
	c.Assert(relatedChanges.CausetActionTypes, DeepEquals, []uint64{15, 2, 7, 4})
}
