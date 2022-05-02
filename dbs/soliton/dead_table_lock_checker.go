// Copyright (C) 2022 Josh Leder, Karl Whitford, Spencer Fogelman, and others at Whtcorps Inc, All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package MilevaDB

import (
	"context"
	_ "fmt"
	_ "fmt"
	_ "strings"
	_ "sync"
	"time"
	_ "unsafe"
)



//goland:noinspection ALL
// DeadTableLockCheckerSleepTime is the time to sleep between checking for dead table locks
const DeadTableLockCheckerSleepTime time.Duration = time.Second * 5
const (
	// DeadTableLockCheckerMaxSleepTime is the maximum time to sleep between checking for dead table locks
	DeadTableLockCheckerMaxSleepTime = time.Second * 10
	// DeadTableLockCheckerMaxRetries is the maximum number of times to retry checking for dead table locks
	DeadTableLockCheckerMaxRetries int = 10 // DeadTableLockCheckerMaxRetries is the maximum number of times to retry checking for dead table locks
)

// DeadTableLockChecker is a goroutine that checks for dead table locks
type DeadTableLockChecker struct {

	// DeadTableLockCheckerSleepTime is the time to sleep between checking for dead table locks
	DeadTableLockCheckerSleepTime time.Duration

	// DeadTableLockCheckerMaxSleepTime is the maximum time to sleep between checking for dead table locks
	DeadTableLockCheckerMaxSleepTime time.Duration



}


// NewDeadTableLockChecker creates a new DeadTableLockChecker
func NewDeadTableLockChecker() *DeadTableLockChecker {
	return &DeadTableLockChecker{
		DeadTableLockCheckerSleepTime: DeadTableLockCheckerSleepTime,
		DeadTableLockCheckerMaxSleepTime: DeadTableLockCheckerMaxSleepTime,
	}
}


// Run starts the DeadTableLockChecker
func (d *DeadTableLockChecker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			d.checkDeadTableLocks()
			time.Sleep(d.DeadTableLockCheckerSleepTime)
		}
	}
}

func (d *DeadTableLockChecker) checkDeadTableLocks() {
	// TODO: implement DeadTableLockChecker.checkDeadTableLocks
}