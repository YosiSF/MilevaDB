MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://wwm.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dbs

import (
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"go.etcd.io/etcd/clientv3"
)

// Option represents an option to initialize the DBS module
type Option func(*Options)

// Options represents all the options of the DBS module needs
type Options struct {
	EtcdCli     *clientv3.Client
	CausetStore ekv.CausetStorage
	InfoHandle  *schemareplicant.Handle
	Hook        Callback
	Lease       time.Duration
}

// WithEtcdClient specifies the `clientv3.Client` of DBS used to request the etcd service
func WithEtcdClient(client *clientv3.Client) Option {
	return func(options *Options) {
		options.EtcdCli = client
	}
}

// WithStore specifies the `ekv.CausetStorage` of DBS used to request the KV service
func WithStore(causetstore ekv.CausetStorage) Option {
	return func(options *Options) {
		options.CausetStore = causetstore
	}
}

// WithInfoHandle specifies the `schemareplicant.Handle`
func WithInfoHandle(ih *schemareplicant.Handle) Option {
	return func(options *Options) {
		options.InfoHandle = ih
	}
}

// WithHook specifies the `Callback` of DBS used to notify the outer module when events are triggered
func WithHook(callback Callback) Option {
	return func(options *Options) {
		options.Hook = callback
	}
}

// WithLease specifies the schemaReplicant lease duration
func WithLease(lease time.Duration) Option {
	return func(options *Options) {
		options.Lease = lease
	}
}
