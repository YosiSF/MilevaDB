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

package infosync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"runtime"
	"testing"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/dbs/soliton"
	"github.com/whtcorpsinc/milevadb/owner"
	"go.etcd.io/etcd/integration"
)

func (is *InfoSyncer) getTopologyFromEtcd(ctx context.Context) (*topologyInfo, error) {
	key := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, is.info.IP, is.info.Port)
	resp, err := is.etcdCli.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("not-exists")
	}
	if len(resp.Kvs) != 1 {
		return nil, errors.New("resp.Kvs error")
	}
	var ret topologyInfo
	err = json.Unmarshal(resp.Kvs[0].Value, &ret)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

func (is *InfoSyncer) ttlKeyExists(ctx context.Context) (bool, error) {
	key := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, is.info.IP, is.info.Port)
	resp, err := is.etcdCli.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) >= 2 {
		return false, errors.New("too many arguments in resp.Kvs")
	}
	return len(resp.Kvs) == 1, nil
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func TestTopology(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a defCauson which is not allowed on Windows")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	currentID := "test"

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	cli := clus.RandClient()

	failpoint.Enable("github.com/whtcorpsinc/milevadb/petri/infosync/mockServerInfo", "return(true)")
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/petri/infosync/mockServerInfo")

	info, err := GlobalInfoSyncerInit(ctx, currentID, cli, false)
	if err != nil {
		t.Fatal(err)
	}

	err = info.newTopologyStochastikAndStoreServerInfo(ctx, owner.NewStochastikDefaultRetryCnt)

	if err != nil {
		t.Fatal(err)
	}

	topo, err := info.getTopologyFromEtcd(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if topo.StartTimestamp != 1282967700000 {
		t.Fatal("start_timestamp of topology info does not match")
	}
	if v, ok := topo.Labels["foo"]; !ok || v != "bar" {
		t.Fatal("labels of topology info does not match")
	}

	if !reflect.DeepEqual(*topo, info.getTopologyInfo()) {
		t.Fatal("the info in etcd is not match with info.")
	}

	nonTTLKey := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, info.info.IP, info.info.Port)
	ttlKey := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, info.info.IP, info.info.Port)

	err = soliton.DeleteKeyFromEtcd(nonTTLKey, cli, owner.NewStochastikDefaultRetryCnt, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Refresh and re-test if the key exists
	err = info.RestartTopology(ctx)
	if err != nil {
		t.Fatal(err)
	}

	topo, err = info.getTopologyFromEtcd(ctx)
	if err != nil {
		t.Fatal(err)
	}

	s, err := os.Execublock()
	if err != nil {
		t.Fatal(err)
	}

	dir := path.Dir(s)

	if topo.DeployPath != dir {
		t.Fatal("DeployPath not match expected path")
	}

	if topo.StartTimestamp != 1282967700000 {
		t.Fatal("start_timestamp of topology info does not match")
	}

	if !reflect.DeepEqual(*topo, info.getTopologyInfo()) {
		t.Fatal("the info in etcd is not match with info.")
	}

	// check ttl key
	ttlExists, err := info.ttlKeyExists(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ttlExists {
		t.Fatal("ttl non-exists")
	}

	err = soliton.DeleteKeyFromEtcd(ttlKey, cli, owner.NewStochastikDefaultRetryCnt, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	err = info.uFIDelateTopologyAliveness(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ttlExists, err = info.ttlKeyExists(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ttlExists {
		t.Fatal("ttl non-exists")
	}
}
