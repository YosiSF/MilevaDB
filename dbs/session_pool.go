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

package dbs

import (
	"sync"

	"github.com/ngaut/pools"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// stochastikPool is used to new stochastik.
type stochastikPool struct {
	mu struct {
		sync.Mutex
		closed bool
	}
	resPool *pools.ResourcePool
}

func newStochastikPool(resPool *pools.ResourcePool) *stochastikPool {
	return &stochastikPool{resPool: resPool}
}

// get gets stochastikctx from context resource pool.
// Please remember to call put after you finished using stochastikctx.
func (sg *stochastikPool) get() (stochastikctx.Context, error) {
	if sg.resPool == nil {
		return mock.NewContext(), nil
	}

	sg.mu.Lock()
	if sg.mu.closed {
		sg.mu.Unlock()
		return nil, errors.Errorf("stochastikPool is closed.")
	}
	sg.mu.Unlock()

	// no need to protect sg.resPool
	resource, err := sg.resPool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx := resource.(stochastikctx.Context)
	ctx.GetStochastikVars().SetStatusFlag(allegrosql.ServerStatusAutocommit, true)
	ctx.GetStochastikVars().InRestrictedALLEGROSQL = true
	return ctx, nil
}

// put returns stochastikctx to context resource pool.
func (sg *stochastikPool) put(ctx stochastikctx.Context) {
	if sg.resPool == nil {
		return
	}

	// no need to protect sg.resPool, even the sg.resPool is closed, the ctx still need to
	// put into resPool, because when resPool is closing, it will wait all the ctx returns, then resPool finish closing.
	sg.resPool.Put(ctx.(pools.Resource))
}

// close clean up the stochastikPool.
func (sg *stochastikPool) close() {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	// prevent closing resPool twice.
	if sg.mu.closed || sg.resPool == nil {
		return
	}
	logutil.BgLogger().Info("[dbs] closing stochastikPool")
	sg.resPool.Close()
	sg.mu.closed = true
}
