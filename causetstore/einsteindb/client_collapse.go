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

// Package einsteindb provides tcp connection to kvserver.
package einsteindb

import (
	"context"
	"strconv"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"golang.org/x/sync/singleflight"
)

var _ Client = reqDefCauslapse{}

var resolveRegionSf singleflight.Group

type reqDefCauslapse struct {
	Client
}

func (r reqDefCauslapse) Close() error {
	if r.Client == nil {
		panic("client should not be nil")
	}
	return r.Client.Close()
}

func (r reqDefCauslapse) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	if r.Client == nil {
		panic("client should not be nil")
	}
	if canDefCauslapse, resp, err := r.tryDefCauslapseRequest(ctx, addr, req, timeout); canDefCauslapse {
		return resp, err
	}
	return r.Client.SendRequest(ctx, addr, req, timeout)
}

func (r reqDefCauslapse) tryDefCauslapseRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (canDefCauslapse bool, resp *einsteindbrpc.Response, err error) {
	switch req.Type {
	case einsteindbrpc.CmdResolveLock:
		resolveLock := req.ResolveLock()
		if len(resolveLock.Keys) > 0 {
			// can not collapse resolve dagger lite
			return
		}
		if len(resolveLock.TxnInfos) > 0 {
			// can not collapse batch resolve locks which is only used by GC worker.
			return
		}
		canDefCauslapse = true
		key := strconv.FormatUint(resolveLock.Context.RegionId, 10) + "-" + strconv.FormatUint(resolveLock.StartVersion, 10)
		resp, err = r.collapse(ctx, key, &resolveRegionSf, addr, req, timeout)
		return
	default:
		// now we only support collapse resolve dagger.
		return
	}
}

func (r reqDefCauslapse) collapse(ctx context.Context, key string, sf *singleflight.Group,
	addr string, req *einsteindbrpc.Request, timeout time.Duration) (resp *einsteindbrpc.Response, err error) {
	rsC := sf.DoChan(key, func() (interface{}, error) {
		return r.Client.SendRequest(context.Background(), addr, req, readTimeoutShort) // use resolveLock timeout.
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		err = errors.Trace(ctx.Err())
		return
	case <-timer.C:
		err = errors.Trace(context.DeadlineExceeded)
		return
	case rs := <-rsC:
		if rs.Err != nil {
			err = errors.Trace(rs.Err)
			return
		}
		resp = rs.Val.(*einsteindbrpc.Response)
		return
	}
}
