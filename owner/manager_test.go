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

package owner_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	. "github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	"github.com/whtcorpsinc/MilevaDB-Prod/owner"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/integration"
	goctx "golang.org/x/net/context"
)

const testLease = 5 * time.Millisecond

func TestT(t *testing.T) {
	TestingT(t)
}

func checkOwner(d DBS, fbVal bool) (isOwner bool) {
	manager := d.OwnerManager()
	// The longest to wait for 30 seconds to
	// make sure that campaigning owners is completed.
	for i := 0; i < 6000; i++ {
		time.Sleep(5 * time.Millisecond)
		isOwner = manager.IsOwner()
		if isOwner == fbVal {
			break
		}
	}
	return
}

func TestSingle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		t.Fatal(err)
	}
	defer causetstore.Close()

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	cli := clus.RandClient()
	ctx := goctx.Background()
	d := NewDBS(
		ctx,
		WithEtcdClient(cli),
		WithStore(causetstore),
		WithLease(testLease),
	)
	err = d.Start(nil)
	if err != nil {
		t.Fatalf("DBS start failed %v", err)
	}
	defer d.Stop()

	isOwner := checkOwner(d, true)
	if !isOwner {
		t.Fatalf("expect true, got isOwner:%v", isOwner)
	}

	// test for newStochastik failed
	ctx, cancel := goctx.WithCancel(ctx)
	manager := owner.NewOwnerManager(ctx, cli, "dbs", "dbs_id", DBSOwnerKey)
	cancel()
	err = manager.CampaignOwner()
	if !terror.ErrorEqual(err, goctx.Canceled) &&
		!terror.ErrorEqual(err, goctx.DeadlineExceeded) {
		t.Fatalf("campaigned result don't match, err %v", err)
	}
	isOwner = checkOwner(d, true)
	if !isOwner {
		t.Fatalf("expect true, got isOwner:%v", isOwner)
	}
	// The test is used to exit campaign loop.
	d.OwnerManager().Cancel()
	isOwner = checkOwner(d, false)
	if isOwner {
		t.Fatalf("expect false, got isOwner:%v", isOwner)
	}
	time.Sleep(200 * time.Millisecond)
	ownerID, _ := manager.GetOwnerID(goctx.Background())
	// The error is ok to be not nil since we canceled the manager.
	if ownerID != "" {
		t.Fatalf("owner %s is not empty", ownerID)
	}
}

func TestCluster(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	tmpTTL := 3
	orignalTTL := owner.ManagerStochastikTTL
	owner.ManagerStochastikTTL = tmpTTL
	defer func() {
		owner.ManagerStochastikTTL = orignalTTL
	}()
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		t.Fatal(err)
	}
	defer causetstore.Close()
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 4})
	defer clus.Terminate(t)

	cli := clus.Client(0)
	d := NewDBS(
		goctx.Background(),
		WithEtcdClient(cli),
		WithStore(causetstore),
		WithLease(testLease),
	)
	err = d.Start(nil)
	if err != nil {
		t.Fatalf("DBS start failed %v", err)
	}
	isOwner := checkOwner(d, true)
	if !isOwner {
		t.Fatalf("expect true, got isOwner:%v", isOwner)
	}
	cli1 := clus.Client(1)
	d1 := NewDBS(
		goctx.Background(),
		WithEtcdClient(cli1),
		WithStore(causetstore),
		WithLease(testLease),
	)
	err = d1.Start(nil)
	if err != nil {
		t.Fatalf("DBS start failed %v", err)
	}
	isOwner = checkOwner(d1, false)
	if isOwner {
		t.Fatalf("expect false, got isOwner:%v", isOwner)
	}

	// Delete the leader key, the d1 become the owner.
	cliRW := clus.Client(2)
	err = deleteLeader(cliRW, DBSOwnerKey)
	if err != nil {
		t.Fatal(err)
	}
	isOwner = checkOwner(d, false)
	if isOwner {
		t.Fatalf("expect false, got isOwner:%v", isOwner)
	}
	d.Stop()

	// d3 (not owner) stop
	cli3 := clus.Client(3)
	d3 := NewDBS(
		goctx.Background(),
		WithEtcdClient(cli3),
		WithStore(causetstore),
		WithLease(testLease),
	)
	err = d3.Start(nil)
	if err != nil {
		t.Fatalf("DBS start failed %v", err)
	}
	defer d3.Stop()
	isOwner = checkOwner(d3, false)
	if isOwner {
		t.Fatalf("expect false, got isOwner:%v", isOwner)
	}
	d3.Stop()

	// Cancel the owner context, there is no owner.
	d1.Stop()
	time.Sleep(time.Duration(tmpTTL+1) * time.Second)
	stochastik, err := concurrency.NewStochastik(cliRW)
	if err != nil {
		t.Fatalf("new stochastik failed %v", err)
	}
	elec := concurrency.NewElection(stochastik, DBSOwnerKey)
	logPrefix := fmt.Sprintf("[dbs] %s ownerManager %s", DBSOwnerKey, "useless id")
	logCtx := logutil.WithKeyValue(context.Background(), "owner info", logPrefix)
	_, err = owner.GetOwnerInfo(goctx.Background(), logCtx, elec, "useless id")
	if !terror.ErrorEqual(err, concurrency.ErrElectionNoLeader) {
		t.Fatalf("get owner info result don't match, err %v", err)
	}
}

func deleteLeader(cli *clientv3.Client, prefixKey string) error {
	stochastik, err := concurrency.NewStochastik(cli)
	if err != nil {
		return errors.Trace(err)
	}
	defer stochastik.Close()
	elec := concurrency.NewElection(stochastik, prefixKey)
	resp, err := elec.Leader(goctx.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = cli.Delete(goctx.Background(), string(resp.Kvs[0].Key))
	return errors.Trace(err)
}
