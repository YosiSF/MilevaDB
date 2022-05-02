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

package einsteindb

import (
	"time"

	"github.com/einsteindb/fidel/client"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
)

// CausetStorage represent the ekv.CausetStorage runs on EinsteinDB.
type CausetStorage interface {
	ekv.CausetStorage

	// GetRegionCache gets the RegionCache.
	GetRegionCache() *RegionCache

	// SendReq sends a request to EinsteinDB.
	SendReq(bo *Backoffer, req *einsteindbrpc.Request, regionID RegionVerID, timeout time.Duration) (*einsteindbrpc.Response, error)

	// GetLockResolver gets the LockResolver.
	GetLockResolver() *LockResolver

	// GetSafePointKV gets the SafePointKV.
	GetSafePointKV() SafePointKV

	// UFIDelateSPCache uFIDelates the cache of safe point.
	UFIDelateSPCache(cachedSP uint64, cachedTime time.Time)

	// GetGCHandler gets the GCHandler.
	GetGCHandler() GCHandler

	// SetOracle sets the Oracle.
	SetOracle(oracle oracle.Oracle)

	// SetEinsteinDBClient sets the EinsteinDB client.
	SetEinsteinDBClient(client Client)

	// GetEinsteinDBClient gets the EinsteinDB client.
	GetEinsteinDBClient() Client

	// Closed returns the closed channel.
	Closed() <-chan struct{}
}

// GCHandler runs garbage collection job.
type GCHandler interface {
	// Start starts the GCHandler.
	Start()

	// Close closes the GCHandler.
	Close()
}

// NewGCHandlerFunc creates a new GCHandler.
// To enable real GC, we should assign the function to `gcworker.NewGCWorker`.
var NewGCHandlerFunc func(storage CausetStorage, FIDelClient fidel.Client) (GCHandler, error)
