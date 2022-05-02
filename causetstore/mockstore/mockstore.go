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

package mockstore

import (
	"net/url"
	"strings"

	fidel "github.com/einsteindb/fidel/client"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/entangledstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/solomonkey"
	"github.com/whtcorpsinc/errors"
)

// MockEinsteinDBDriver is in memory mock EinsteinDB driver.
type MockEinsteinDBDriver struct{}

// Open creates a MockEinsteinDB storage.
func (d MockEinsteinDBDriver) Open(path string) (solomonkey.CausetStorage, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !strings.EqualFold(u.Scheme, "mockeinsteindb") {
		return nil, errors.Errorf("Uri scheme expected(mockeinsteindb) but found (%s)", u.Scheme)
	}

	opts := []MockEinsteinDBStoreOption{WithPath(u.Path), WithStoreType(MockEinsteinDB)}
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	if txnLocalLatches.Enabled {
		opts = append(opts, WithTxnLocalLatches(txnLocalLatches.Capacity))
	}

	return NewMockStore(opts...)
}

// EmbedEntangledStoreDriver is in embedded entangledstore driver.
type EmbedEntangledStoreDriver struct{}

// Open creates a EmbedEntangledStore storage.
func (d EmbedEntangledStoreDriver) Open(path string) (solomonkey.CausetStorage, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !strings.EqualFold(u.Scheme, "entangledstore") {
		return nil, errors.Errorf("Uri scheme expected(entangledstore) but found (%s)", u.Scheme)
	}

	opts := []MockEinsteinDBStoreOption{WithPath(u.Path), WithStoreType(EmbedEntangledStore)}
	txnLocalLatches := config.GetGlobalConfig().TxnLocalLatches
	if txnLocalLatches.Enabled {
		opts = append(opts, WithTxnLocalLatches(txnLocalLatches.Capacity))
	}

	return NewMockStore(opts...)
}

// StoreType is the type of backend mock storage.
type StoreType uint8

const (
	// MockEinsteinDB is the mock storage based on goleveldb.
	MockEinsteinDB StoreType = iota
	// EmbedEntangledStore is the mock storage based on entangledstore.
	EmbedEntangledStore

	defaultStoreType = EmbedEntangledStore
)

type mockOptions struct {
	clusterInspector    func(cluster.Cluster)
	clientHijacker      func(einsteindb.Client) einsteindb.Client
	FIDelClientHijacker func(fidel.Client) fidel.Client
	path                string
	txnLocalLatches     uint
	storeType           StoreType
}

// MockEinsteinDBStoreOption is used to control some behavior of mock einsteindb.
type MockEinsteinDBStoreOption func(*mockOptions)

// WithClientHijacker hijacks KV client's behavior, makes it easy to simulate the network
// problem between MilevaDB and EinsteinDB.
func WithClientHijacker(hijacker func(einsteindb.Client) einsteindb.Client) MockEinsteinDBStoreOption {
	return func(c *mockOptions) {
		c.clientHijacker = hijacker
	}
}

// WithFIDelClientHijacker hijacks FIDel client's behavior, makes it easy to simulate the network
// problem between MilevaDB and FIDel.
func WithFIDelClientHijacker(hijacker func(fidel.Client) fidel.Client) MockEinsteinDBStoreOption {
	return func(c *mockOptions) {
		c.FIDelClientHijacker = hijacker
	}
}

// WithClusterInspector lets user to inspect the mock cluster handler.
func WithClusterInspector(inspector func(cluster.Cluster)) MockEinsteinDBStoreOption {
	return func(c *mockOptions) {
		c.clusterInspector = inspector
	}
}

// WithStoreType lets user choose the backend storage's type.
func WithStoreType(tp StoreType) MockEinsteinDBStoreOption {
	return func(c *mockOptions) {
		c.storeType = tp
	}
}

// WithPath specifies the mockeinsteindb path.
func WithPath(path string) MockEinsteinDBStoreOption {
	return func(c *mockOptions) {
		c.path = path
	}
}

// WithTxnLocalLatches enable txnLocalLatches, when capacity > 0.
func WithTxnLocalLatches(capacity uint) MockEinsteinDBStoreOption {
	return func(c *mockOptions) {
		c.txnLocalLatches = capacity
	}
}

// NewMockStore creates a mocked einsteindb causetstore, the path is the file path to causetstore the data.
// If path is an empty string, a memory storage will be created.
func NewMockStore(options ...MockEinsteinDBStoreOption) (solomonkey.CausetStorage, error) {
	opt := mockOptions{
		clusterInspector: func(c cluster.Cluster) {
			BootstrapWithSingleStore(c)
		},
		storeType: defaultStoreType,
	}
	for _, f := range options {
		f(&opt)
	}

	switch opt.storeType {
	case MockEinsteinDB:
		return newMockEinsteinDBStore(&opt)
	case EmbedEntangledStore:
		return newEntangledStore(&opt)
	default:
		panic("unsupported mockstore")
	}
}

// BootstrapWithSingleStore initializes a Cluster with 1 Region and 1 CausetStore.
func BootstrapWithSingleStore(cluster cluster.Cluster) (storeID, peerID, regionID uint64) {
	switch x := cluster.(type) {
	case *mockeinsteindb.Cluster:
		return mockeinsteindb.BootstrapWithSingleStore(x)
	case *entangledstore.Cluster:
		return entangledstore.BootstrapWithSingleStore(x)
	default:
		panic("unsupported cluster type")
	}
}

// BootstrapWithMultiStores initializes a Cluster with 1 Region and n Stores.
func BootstrapWithMultiStores(cluster cluster.Cluster, n int) (storeIDs, peerIDs []uint64, regionID uint64, leaderPeer uint64) {
	switch x := cluster.(type) {
	case *mockeinsteindb.Cluster:
		return mockeinsteindb.BootstrapWithMultiStores(x, n)
	case *entangledstore.Cluster:
		return entangledstore.BootstrapWithMultiStores(x, n)
	default:
		panic("unsupported cluster type")
	}
}

// BootstrapWithMultiRegions initializes a Cluster with multiple Regions and 1
// CausetStore. The number of Regions will be len(splitKeys) + 1.
func BootstrapWithMultiRegions(cluster cluster.Cluster, splitKeys ...[]byte) (storeID uint64, regionIDs, peerIDs []uint64) {
	switch x := cluster.(type) {
	case *mockeinsteindb.Cluster:
		return mockeinsteindb.BootstrapWithMultiRegions(x, splitKeys...)
	case *entangledstore.Cluster:
		return entangledstore.BootstrapWithMultiRegions(x, splitKeys...)
	default:
		panic("unsupported cluster type")
	}
}
