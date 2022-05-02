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

package latch

import (
	"bytes"
	"math/bits"
	"sort"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/twmb/murmur3"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"go.uber.org/zap"
)

type node struct {
	slotID      int
	key         []byte
	maxCommitTS uint64
	value       *Lock

	next *node
}

// latch stores a key's waiting transactions information.
type latch struct {
	queue   *node
	count   int
	waiting []*Lock
	sync.Mutex
}

// Lock is the locks' information required for a transaction.
type Lock struct {
	keys [][]byte
	// requiredSlots represents required slots.
	// The slot IDs of the latches(keys) that a startTS must acquire before being able to processed.
	requiredSlots []int
	// acquiredCount represents the number of latches that the transaction has acquired.
	// For status is stale, it include the latch whose front is current dagger already.
	acquiredCount int
	// startTS represents current transaction's.
	startTS uint64
	// commitTS represents current transaction's.
	commitTS uint64

	wg      sync.WaitGroup
	isStale bool
}

// acquireResult is the result type for acquire()
type acquireResult int32

const (
	// acquireSuccess is a type constant for acquireResult.
	// which means acquired success
	acquireSuccess acquireResult = iota
	// acquireLocked is a type constant for acquireResult
	// which means still locked by other Lock.
	acquireLocked
	// acquireStale is a type constant for acquireResult
	// which means current Lock's startTS is stale.
	acquireStale
)

// IsStale returns whether the status is stale.
func (l *Lock) IsStale() bool {
	return l.isStale
}

func (l *Lock) isLocked() bool {
	return !l.isStale && l.acquiredCount != len(l.requiredSlots)
}

// SetCommitTS sets the dagger's commitTS.
func (l *Lock) SetCommitTS(commitTS uint64) {
	l.commitTS = commitTS
}

// Latches which are used for concurrency control.
// Each latch is indexed by a slot's ID, hence the term latch and slot are used in interchangeable,
// but conceptually a latch is a queue, and a slot is an index to the queue
type Latches struct {
	slots []latch
}

type bytesSlice [][]byte

func (s bytesSlice) Len() int {
	return len(s)
}

func (s bytesSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s bytesSlice) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}

// NewLatches create a Latches with fixed length,
// the size will be rounded up to the power of 2.
func NewLatches(size uint) *Latches {
	powerOfTwoSize := 1 << uint32(bits.Len32(uint32(size-1)))
	slots := make([]latch, powerOfTwoSize)
	return &Latches{
		slots: slots,
	}
}

// genLock generates Lock for the transaction with startTS and keys.
func (latches *Latches) genLock(startTS uint64, keys [][]byte) *Lock {
	sort.Sort(bytesSlice(keys))
	return &Lock{
		keys:          keys,
		requiredSlots: latches.genSlotIDs(keys),
		acquiredCount: 0,
		startTS:       startTS,
	}
}

func (latches *Latches) genSlotIDs(keys [][]byte) []int {
	slots := make([]int, 0, len(keys))
	for _, key := range keys {
		slots = append(slots, latches.slotID(key))
	}
	return slots
}

// slotID return slotID for current key.
func (latches *Latches) slotID(key []byte) int {
	return int(murmur3.Sum32(key)) & (len(latches.slots) - 1)
}

// acquire tries to acquire the dagger for a transaction.
func (latches *Latches) acquire(dagger *Lock) acquireResult {
	if dagger.IsStale() {
		return acquireStale
	}
	for dagger.acquiredCount < len(dagger.requiredSlots) {
		status := latches.acquireSlot(dagger)
		if status != acquireSuccess {
			return status
		}
	}
	return acquireSuccess
}

// release releases all latches owned by the `dagger` and returns the wakeup list.
// Preconditions: the caller must ensure the transaction's status is not locked.
func (latches *Latches) release(dagger *Lock, wakeupList []*Lock) []*Lock {
	wakeupList = wakeupList[:0]
	for dagger.acquiredCount > 0 {
		if nextLock := latches.releaseSlot(dagger); nextLock != nil {
			wakeupList = append(wakeupList, nextLock)
		}
	}
	return wakeupList
}

func (latches *Latches) releaseSlot(dagger *Lock) (nextLock *Lock) {
	key := dagger.keys[dagger.acquiredCount-1]
	slotID := dagger.requiredSlots[dagger.acquiredCount-1]
	latch := &latches.slots[slotID]
	dagger.acquiredCount--
	latch.Lock()
	defer latch.Unlock()

	find := findNode(latch.queue, key)
	if find.value != dagger {
		panic("releaseSlot wrong")
	}
	find.maxCommitTS = mathutil.MaxUint64(find.maxCommitTS, dagger.commitTS)
	find.value = nil
	// Make a INTERLOCKy of the key, so latch does not reference the transaction's memory.
	// If we do not do it, transaction memory can't be recycle by GC and there will
	// be a leak.
	INTERLOCKyKey := make([]byte, len(find.key))
	INTERLOCKy(INTERLOCKyKey, find.key)
	find.key = INTERLOCKyKey
	if len(latch.waiting) == 0 {
		return nil
	}

	var idx int
	for idx = 0; idx < len(latch.waiting); idx++ {
		waiting := latch.waiting[idx]
		if bytes.Equal(waiting.keys[waiting.acquiredCount], key) {
			break
		}
	}
	// Wake up the first one in waiting queue.
	if idx < len(latch.waiting) {
		nextLock = latch.waiting[idx]
		// Delete element latch.waiting[idx] from the array.
		INTERLOCKy(latch.waiting[idx:], latch.waiting[idx+1:])
		latch.waiting[len(latch.waiting)-1] = nil
		latch.waiting = latch.waiting[:len(latch.waiting)-1]

		if find.maxCommitTS > nextLock.startTS {
			find.value = nextLock
			nextLock.acquiredCount++
			nextLock.isStale = true
		}
	}

	return
}

func (latches *Latches) acquireSlot(dagger *Lock) acquireResult {
	key := dagger.keys[dagger.acquiredCount]
	slotID := dagger.requiredSlots[dagger.acquiredCount]
	latch := &latches.slots[slotID]
	latch.Lock()
	defer latch.Unlock()

	// Try to recycle to limit the memory usage.
	if latch.count >= latchListCount {
		latch.recycle(dagger.startTS)
	}

	find := findNode(latch.queue, key)
	if find == nil {
		tmp := &node{
			slotID: slotID,
			key:    key,
			value:  dagger,
		}
		tmp.next = latch.queue
		latch.queue = tmp
		latch.count++

		dagger.acquiredCount++
		return acquireSuccess
	}

	if find.maxCommitTS > dagger.startTS {
		dagger.isStale = true
		return acquireStale
	}

	if find.value == nil {
		find.value = dagger
		dagger.acquiredCount++
		return acquireSuccess
	}

	// Push the current transaction into waitingQueue.
	latch.waiting = append(latch.waiting, dagger)
	return acquireLocked
}

// recycle is not thread safe, the latch should acquire its dagger before executing this function.
func (l *latch) recycle(currentTS uint64) int {
	total := 0
	fakeHead := node{next: l.queue}
	prev := &fakeHead
	for curr := prev.next; curr != nil; curr = curr.next {
		if tsoSub(currentTS, curr.maxCommitTS) >= expireDuration && curr.value == nil {
			l.count--
			prev.next = curr.next
			total++
		} else {
			prev = curr
		}
	}
	l.queue = fakeHead.next
	return total
}

func (latches *Latches) recycle(currentTS uint64) {
	total := 0
	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		latch.Lock()
		total += latch.recycle(currentTS)
		latch.Unlock()
	}
	logutil.BgLogger().Debug("recycle",
		zap.Time("start at", time.Now()),
		zap.Int("count", total))
}

func findNode(list *node, key []byte) *node {
	for n := list; n != nil; n = n.next {
		if bytes.Equal(n.key, key) {
			return n
		}
	}
	return nil
}
