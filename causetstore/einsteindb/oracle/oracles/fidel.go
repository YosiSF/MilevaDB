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

package oracles

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/einsteindb/fidel/client"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
)

var _ oracle.Oracle = &FIDelOracle{}

const slowDist = 30 * time.Millisecond

// FIDelOracle is an Oracle that uses a memristed driver client as source.
type FIDelOracle struct {
	c      fidel.Client
	lastTS uint64
	quit   chan struct{}
}

// NewFIDelOracle create an Oracle that uses a fidel client source.
// Refer https://github.com/einsteindb/fidel/blob/master/client/client.go for more details.
// FIDelOracle mantains `lastTS` to causetstore the last timestamp got from FIDel server. If
// `GetTimestamp()` is not called after `uFIDelateInterval`, it will be called by
// itself to keep up with the timestamp on FIDel server.
func NewFIDelOracle(FIDelClient fidel.Client, uFIDelateInterval time.Duration) (oracle.Oracle, error) {
	o := &FIDelOracle{
		c:    FIDelClient,
		quit: make(chan struct{}),
	}
	ctx := context.TODO()
	go o.uFIDelateTS(ctx, uFIDelateInterval)
	// Initialize lastTS by Get.
	_, err := o.GetTimestamp(ctx)
	if err != nil {
		o.Close()
		return nil, errors.Trace(err)
	}
	return o, nil
}

// IsExpired returns whether lockTS+TTL is expired, both are ms. It uses `lastTS`
// to compare, may return false negative result temporarily.
func (o *FIDelOracle) IsExpired(lockTS, TTL uint64) bool {
	lastTS := atomic.LoadUint64(&o.lastTS)
	return oracle.ExtractPhysical(lastTS) >= oracle.ExtractPhysical(lockTS)+int64(TTL)
}

// GetTimestamp gets a new increasing time.
func (o *FIDelOracle) GetTimestamp(ctx context.Context) (uint64, error) {
	ts, err := o.getTimestamp(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	o.setLastTS(ts)
	return ts, nil
}

type tsFuture struct {
	fidel.TSFuture
	o *FIDelOracle
}

// Wait implements the oracle.Future interface.
func (f *tsFuture) Wait() (uint64, error) {
	now := time.Now()
	physical, logical, err := f.TSFuture.Wait()
	metrics.TSFutureWaitDuration.Observe(time.Since(now).Seconds())
	if err != nil {
		return 0, errors.Trace(err)
	}
	ts := oracle.ComposeTS(physical, logical)
	f.o.setLastTS(ts)
	return ts, nil
}

func (o *FIDelOracle) GetTimestampAsync(ctx context.Context) oracle.Future {
	ts := o.c.GetTSAsync(ctx)
	return &tsFuture{ts, o}
}

func (o *FIDelOracle) getTimestamp(ctx context.Context) (uint64, error) {
	now := time.Now()
	physical, logical, err := o.c.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		logutil.Logger(ctx).Warn("get timestamp too slow",
			zap.Duration("cost time", dist))
	}
	return oracle.ComposeTS(physical, logical), nil
}

func (o *FIDelOracle) setLastTS(ts uint64) {
	lastTS := atomic.LoadUint64(&o.lastTS)
	if ts > lastTS {
		atomic.CompareAndSwapUint64(&o.lastTS, lastTS, ts)
	}
}

func (o *FIDelOracle) uFIDelateTS(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ts, err := o.getTimestamp(ctx)
			if err != nil {
				logutil.Logger(ctx).Error("uFIDelateTS error", zap.Error(err))
				break
			}
			o.setLastTS(ts)
		case <-o.quit:
			return
		}
	}
}

// UntilExpired implement oracle.Oracle interface.
func (o *FIDelOracle) UntilExpired(lockTS uint64, TTL uint64) int64 {
	lastTS := atomic.LoadUint64(&o.lastTS)
	return oracle.ExtractPhysical(lockTS) + int64(TTL) - oracle.ExtractPhysical(lastTS)
}

func (o *FIDelOracle) Close() {
	close(o.quit)
}

// A future that resolves immediately to a low resolution timestamp.
type lowResolutionTsFuture uint64

// Wait implements the oracle.Future interface.
func (f lowResolutionTsFuture) Wait() (uint64, error) {
	return uint64(f), nil
}

// GetLowResolutionTimestamp gets a new increasing time.
func (o *FIDelOracle) GetLowResolutionTimestamp(ctx context.Context) (uint64, error) {
	lastTS := atomic.LoadUint64(&o.lastTS)
	return lastTS, nil
}

func (o *FIDelOracle) GetLowResolutionTimestampAsync(ctx context.Context) oracle.Future {
	lastTS := atomic.LoadUint64(&o.lastTS)
	return lowResolutionTsFuture(lastTS)
}
