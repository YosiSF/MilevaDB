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

package dbs_test

import (
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"go.etcd.io/etcd/clientv3"
)

type dbsOptionsSuite struct{}

var _ = Suite(&dbsOptionsSuite{})

func (s *dbsOptionsSuite) TestOptions(c *C) {
	client, err := clientv3.NewFromURL("test")
	c.Assert(err, IsNil)
	callback := &dbs.BaseCallback{}
	lease := time.Second * 3
	causetstore := &mock.CausetStore{}
	infoHandle := schemareplicant.NewHandle(causetstore)

	options := []dbs.Option{
		dbs.WithEtcdClient(client),
		dbs.WithHook(callback),
		dbs.WithLease(lease),
		dbs.WithStore(causetstore),
		dbs.WithInfoHandle(infoHandle),
	}

	opt := &dbs.Options{}
	for _, o := range options {
		o(opt)
	}

	c.Assert(opt.EtcdCli, Equals, client)
	c.Assert(opt.Hook, Equals, callback)
	c.Assert(opt.Lease, Equals, lease)
	c.Assert(opt.CausetStore, Equals, causetstore)
	c.Assert(opt.InfoHandle, Equals, infoHandle)
}
