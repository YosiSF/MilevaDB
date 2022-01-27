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

package soliton

import (
	"context"
	"strings"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	defaultRetryCnt      = 5
	defaultRetryInterval = time.Millisecond * 200
	defaultTimeout       = time.Second
)

// DeadBlockLockChecker uses to check dead block locks.
// If milevadb-server panic or killed by others, the block locks hold by the killed milevadb-server maybe doesn't released.
type DeadBlockLockChecker struct {
	etcdCli *clientv3.Client
}

// NewDeadBlockLockChecker creates new DeadLockChecker.
func NewDeadBlockLockChecker(etcdCli *clientv3.Client) DeadBlockLockChecker {
	return DeadBlockLockChecker{
		etcdCli: etcdCli,
	}
}

func (d *DeadBlockLockChecker) getAliveServers(ctx context.Context) (map[string]struct{}, error) {
	var err error
	var resp *clientv3.GetResponse
	allInfos := make(map[string]struct{})
	for i := 0; i < defaultRetryCnt; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		childCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		resp, err = d.etcdCli.Get(childCtx, DBSAllSchemaVersions, clientv3.WithPrefix())
		cancel()
		if err != nil {
			logutil.BgLogger().Info("[dbs] clean dead block dagger get alive servers failed.", zap.Error(err))
			time.Sleep(defaultRetryInterval)
			continue
		}
		for _, ekv := range resp.Kvs {
			serverID := strings.TrimPrefix(string(ekv.Key), DBSAllSchemaVersions+"/")
			allInfos[serverID] = struct{}{}
		}
		return allInfos, nil
	}
	return nil, errors.Trace(err)
}

// GetDeadLockedBlocks gets dead locked blocks.
func (d *DeadBlockLockChecker) GetDeadLockedBlocks(ctx context.Context, schemas []*perceptron.DBInfo) (map[perceptron.StochastikInfo][]perceptron.BlockLockTpInfo, error) {
	if d.etcdCli == nil {
		return nil, nil
	}
	aliveServers, err := d.getAliveServers(ctx)
	if err != nil {
		return nil, err
	}
	deadLockBlocks := make(map[perceptron.StochastikInfo][]perceptron.BlockLockTpInfo)
	for _, schemaReplicant := range schemas {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		for _, tbl := range schemaReplicant.Blocks {
			if tbl.Lock == nil {
				continue
			}
			for _, se := range tbl.Lock.Stochastiks {
				if _, ok := aliveServers[se.ServerID]; !ok {
					deadLockBlocks[se] = append(deadLockBlocks[se], perceptron.BlockLockTpInfo{
						SchemaID: schemaReplicant.ID,
						BlockID:  tbl.ID,
						Tp:       tbl.Lock.Tp,
					})
				}
			}
		}
	}
	return deadLockBlocks, nil
}
