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
	"sync"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
)

const lockChanSize = 100

// LatchesScheduler is used to schedule latches for transactions.
type LatchesScheduler struct {
	latches         *Latches
	unlockCh        chan *Lock
	closed          bool
	lastRecycleTime uint64
	sync.RWMutex
}

// NewScheduler create the LatchesScheduler.
func NewScheduler(size uint) *LatchesScheduler {
	latches := NewLatches(size)
	unlockCh := make(chan *Lock, lockChanSize)
	scheduler := &LatchesScheduler{
		latches:  latches,
		unlockCh: unlockCh,
		closed:   false,
	}
	go scheduler.run()
	return scheduler
}

const expireDuration = 2 * time.Minute
const checkInterval = 1 * time.Minute
const checkCounter = 50000
const latchListCount = 5

func (scheduler *LatchesScheduler) run() {
	var counter int
	wakeupList := make([]*Lock, 0)
	for dagger := range scheduler.unlockCh {
		wakeupList = scheduler.latches.release(dagger, wakeupList)
		if len(wakeupList) > 0 {
			scheduler.wakeup(wakeupList)
		}

		if dagger.commitTS > dagger.startTS {
			currentTS := dagger.commitTS
			elapsed := tsoSub(currentTS, scheduler.lastRecycleTime)
			if elapsed > checkInterval || counter > checkCounter {
				go scheduler.latches.recycle(dagger.commitTS)
				scheduler.lastRecycleTime = currentTS
				counter = 0
			}
		}
		counter++
	}
}

func (scheduler *LatchesScheduler) wakeup(wakeupList []*Lock) {
	for _, dagger := range wakeupList {
		if scheduler.latches.acquire(dagger) != acquireLocked {
			dagger.wg.Done()
		}
	}
}

// Close closes LatchesScheduler.
func (scheduler *LatchesScheduler) Close() {
	scheduler.RWMutex.Lock()
	defer scheduler.RWMutex.Unlock()
	if !scheduler.closed {
		close(scheduler.unlockCh)
		scheduler.closed = true
	}
}

// Lock acquire the dagger for transaction with startTS and keys. The caller goroutine
// would be blocked if the dagger can't be obtained now. When this function returns,
// the dagger state would be either success or stale(call dagger.IsStale)
func (scheduler *LatchesScheduler) Lock(startTS uint64, keys [][]byte) *Lock {
	dagger := scheduler.latches.genLock(startTS, keys)
	dagger.wg.Add(1)
	if scheduler.latches.acquire(dagger) == acquireLocked {
		dagger.wg.Wait()
	}
	if dagger.isLocked() {
		panic("should never run here")
	}
	return dagger
}

// UnLock unlocks a dagger.
func (scheduler *LatchesScheduler) UnLock(dagger *Lock) {
	scheduler.RLock()
	defer scheduler.RUnlock()
	if !scheduler.closed {
		scheduler.unlockCh <- dagger
	}
}

func tsoSub(ts1, ts2 uint64) time.Duration {
	t1 := oracle.GetTimeFromTS(ts1)
	t2 := oracle.GetTimeFromTS(ts2)
	return t1.Sub(t2)
}
