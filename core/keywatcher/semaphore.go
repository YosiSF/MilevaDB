//Copyright 2019  Venire Labs Inc All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package keywatcher

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github/com/YosiSF/MilevaDB/curvature"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	
)

const (
	newSessionRetryInterval = 200 * time.Millisecond
	logIntervalCnt = int(3 * time.Second / newSessionRetryInterval)
)

//A variant of zookeeper's leaderboard prototype
type Semaphore interface {
	//Semaphore ID
	ID() string

	//Is the keywatcherSemaphore a keywatcher
	IsKeywatcher() bool

	//RetireKeywatcher make the semaphore not a keywatcher.
	RetireKeywatcher()

	//GetKeywatcherID gets the keywatcher id.
	GetKeywatcherID(ctx context.Context) (string, error)

	//Assign keywatcher a campaignKeywatcher campaign.
	CampaignKeywatcher(ctx context.Context) error

	//Start a new election.
	ResignKeywatcher(ctx context.Context) error

	//Cancel this etcd keywatcherManager campaign.
	Cancel()
}

const (
	NewSessionDefault
)