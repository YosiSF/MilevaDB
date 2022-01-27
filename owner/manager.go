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

package owner

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	newStochastikRetryInterval = 200 * time.Millisecond
	logIntervalCnt             = int(3 * time.Second / newStochastikRetryInterval)
)

// Manager is used to campaign the owner and manage the owner information.
type Manager interface {
	// ID returns the ID of the manager.
	ID() string
	// IsOwner returns whether the ownerManager is the owner.
	IsOwner() bool
	// RetireOwner make the manager to be a not owner. It's exported for testing.
	RetireOwner()
	// GetOwnerID gets the owner ID.
	GetOwnerID(ctx context.Context) (string, error)
	// CampaignOwner campaigns the owner.
	CampaignOwner() error
	// ResignOwner lets the owner start a new election.
	ResignOwner(ctx context.Context) error
	// Cancel cancels this etcd ownerManager campaign.
	Cancel()
}

const (
	// NewStochastikDefaultRetryCnt is the default retry times when create new stochastik.
	NewStochastikDefaultRetryCnt = 3
	// NewStochastikRetryUnlimited is the unlimited retry times when create new stochastik.
	NewStochastikRetryUnlimited = math.MaxInt64
	keyOFIDelefaultTimeout      = 5 * time.Second
)

// DBSOwnerChecker is used to check whether milevadb is owner.
type DBSOwnerChecker interface {
	// IsOwner returns whether the ownerManager is the owner.
	IsOwner() bool
}

// ownerManager represents the structure which is used for electing owner.
type ownerManager struct {
	id        string // id is the ID of the manager.
	key       string
	ctx       context.Context
	prompt    string
	logPrefix string
	logCtx    context.Context
	etcdCli   *clientv3.Client
	cancel    context.CancelFunc
	elec      unsafe.Pointer
	wg        sync.WaitGroup
}

// NewOwnerManager creates a new Manager.
func NewOwnerManager(ctx context.Context, etcdCli *clientv3.Client, prompt, id, key string) Manager {
	logPrefix := fmt.Sprintf("[%s] %s ownerManager %s", prompt, key, id)
	ctx, cancelFunc := context.WithCancel(ctx)
	return &ownerManager{
		etcdCli:   etcdCli,
		id:        id,
		key:       key,
		ctx:       ctx,
		prompt:    prompt,
		cancel:    cancelFunc,
		logPrefix: logPrefix,
		logCtx:    logutil.WithKeyValue(context.Background(), "owner info", logPrefix),
	}
}

// ID implements Manager.ID interface.
func (m *ownerManager) ID() string {
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *ownerManager) IsOwner() bool {
	return atomic.LoadPointer(&m.elec) != unsafe.Pointer(nil)
}

// Cancel implements Manager.Cancel interface.
func (m *ownerManager) Cancel() {
	m.cancel()
	m.wg.Wait()
}

// ManagerStochastikTTL is the etcd stochastik's TTL in seconds. It's exported for testing.
var ManagerStochastikTTL = 60

// setManagerStochastikTTL sets the ManagerStochastikTTL value, it's used for testing.
func setManagerStochastikTTL() error {
	ttlStr := os.Getenv("milevadb_manager_ttl")
	if len(ttlStr) == 0 {
		return nil
	}
	ttl, err := strconv.Atoi(ttlStr)
	if err != nil {
		return errors.Trace(err)
	}
	ManagerStochastikTTL = ttl
	return nil
}

// NewStochastik creates a new etcd stochastik.
func NewStochastik(ctx context.Context, logPrefix string, etcdCli *clientv3.Client, retryCnt, ttl int) (*concurrency.Stochastik, error) {
	var err error

	var etcdStochastik *concurrency.Stochastik
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		if err = contextDone(ctx, err); err != nil {
			return etcdStochastik, errors.Trace(err)
		}

		failpoint.Inject("closeClient", func(val failpoint.Value) {
			if val.(bool) {
				if err := etcdCli.Close(); err != nil {
					failpoint.Return(etcdStochastik, errors.Trace(err))
				}
			}
		})

		failpoint.Inject("closeGrpc", func(val failpoint.Value) {
			if val.(bool) {
				if err := etcdCli.ActiveConnection().Close(); err != nil {
					failpoint.Return(etcdStochastik, errors.Trace(err))
				}
			}
		})

		startTime := time.Now()
		etcdStochastik, err = concurrency.NewStochastik(etcdCli,
			concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		metrics.NewStochastikHistogram.WithLabelValues(logPrefix, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err == nil {
			break
		}
		if failedCnt%logIntervalCnt == 0 {
			logutil.BgLogger().Warn("failed to new stochastik to etcd", zap.String("ownerInfo", logPrefix), zap.Error(err))
		}

		time.Sleep(newStochastikRetryInterval)
		failedCnt++
	}
	return etcdStochastik, errors.Trace(err)
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *ownerManager) CampaignOwner() error {
	logPrefix := fmt.Sprintf("[%s] %s", m.prompt, m.key)
	logutil.BgLogger().Info("start campaign owner", zap.String("ownerInfo", logPrefix))
	stochastik, err := NewStochastik(m.ctx, logPrefix, m.etcdCli, NewStochastikDefaultRetryCnt, ManagerStochastikTTL)
	if err != nil {
		return errors.Trace(err)
	}
	m.wg.Add(1)
	go m.campaignLoop(stochastik)
	return nil
}

// ResignOwner lets the owner start a new election.
func (m *ownerManager) ResignOwner(ctx context.Context) error {
	elec := (*concurrency.Election)(atomic.LoadPointer(&m.elec))
	if elec == nil {
		return errors.Errorf("This node is not a dbs owner, can't be resigned.")
	}

	childCtx, cancel := context.WithTimeout(ctx, keyOFIDelefaultTimeout)
	err := elec.Resign(childCtx)
	cancel()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(m.logCtx).Warn("resign dbs owner success")
	return nil
}

func (m *ownerManager) toBeOwner(elec *concurrency.Election) {
	atomic.StorePointer(&m.elec, unsafe.Pointer(elec))
}

// RetireOwner make the manager to be a not owner.
func (m *ownerManager) RetireOwner() {
	atomic.StorePointer(&m.elec, nil)
}

func (m *ownerManager) campaignLoop(etcdStochastik *concurrency.Stochastik) {
	var cancel context.CancelFunc
	ctx, cancel := context.WithCancel(m.ctx)
	defer func() {
		cancel()
		if r := recover(); r != nil {
			buf := soliton.GetStack()
			logutil.BgLogger().Error("recover panic", zap.String("prompt", m.prompt), zap.Any("error", r), zap.String("buffer", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelDBSOwner).Inc()
		}
		m.wg.Done()
	}()

	logPrefix := m.logPrefix
	logCtx := m.logCtx
	var err error
	for {
		if err != nil {
			metrics.CampaignOwnerCounter.WithLabelValues(m.prompt, err.Error()).Inc()
		}

		select {
		case <-etcdStochastik.Done():
			logutil.Logger(logCtx).Info("etcd stochastik is done, creates a new one")
			leaseID := etcdStochastik.Lease()
			etcdStochastik, err = NewStochastik(ctx, logPrefix, m.etcdCli, NewStochastikRetryUnlimited, ManagerStochastikTTL)
			if err != nil {
				logutil.Logger(logCtx).Info("break campaign loop, NewStochastik failed", zap.Error(err))
				m.revokeStochastik(logPrefix, leaseID)
				return
			}
		case <-ctx.Done():
			logutil.Logger(logCtx).Info("break campaign loop, context is done")
			m.revokeStochastik(logPrefix, etcdStochastik.Lease())
			return
		default:
		}
		// If the etcd server turns clocks forward，the following case may occur.
		// The etcd server deletes this stochastik's lease ID, but etcd stochastik doesn't find it.
		// In this time if we do the campaign operation, the etcd server will return ErrLeaseNotFound.
		if terror.ErrorEqual(err, rpctypes.ErrLeaseNotFound) {
			if etcdStochastik != nil {
				err = etcdStochastik.Close()
				logutil.Logger(logCtx).Info("etcd stochastik encounters the error of lease not found, closes it", zap.Error(err))
			}
			continue
		}

		elec := concurrency.NewElection(etcdStochastik, m.key)
		err = elec.Campaign(ctx, m.id)
		if err != nil {
			logutil.Logger(logCtx).Info("failed to campaign", zap.Error(err))
			continue
		}

		ownerKey, err := GetOwnerInfo(ctx, logCtx, elec, m.id)
		if err != nil {
			continue
		}

		m.toBeOwner(elec)
		m.watchOwner(ctx, etcdStochastik, ownerKey)
		m.RetireOwner()

		metrics.CampaignOwnerCounter.WithLabelValues(m.prompt, metrics.NoLongerOwner).Inc()
		logutil.Logger(logCtx).Warn("is not the owner")
	}
}

func (m *ownerManager) revokeStochastik(logPrefix string, leaseID clientv3.LeaseID) {
	// Revoke the stochastik lease.
	// If revoke takes longer than the ttl, lease is expired anyway.
	cancelCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(ManagerStochastikTTL)*time.Second)
	_, err := m.etcdCli.Revoke(cancelCtx, leaseID)
	cancel()
	logutil.Logger(m.logCtx).Info("revoke stochastik", zap.Error(err))
}

// GetOwnerID implements Manager.GetOwnerID interface.
func (m *ownerManager) GetOwnerID(ctx context.Context) (string, error) {
	resp, err := m.etcdCli.Get(ctx, m.key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return "", concurrency.ErrElectionNoLeader
	}
	return string(resp.Kvs[0].Value), nil
}

// GetOwnerInfo gets the owner information.
func GetOwnerInfo(ctx, logCtx context.Context, elec *concurrency.Election, id string) (string, error) {
	resp, err := elec.Leader(ctx)
	if err != nil {
		// If no leader elected currently, it returns ErrElectionNoLeader.
		logutil.Logger(logCtx).Info("failed to get leader", zap.Error(err))
		return "", errors.Trace(err)
	}
	ownerID := string(resp.Kvs[0].Value)
	logutil.Logger(logCtx).Info("get owner", zap.String("ownerID", ownerID))
	if ownerID != id {
		logutil.Logger(logCtx).Warn("is not the owner")
		return "", errors.New("ownerInfoNotMatch")
	}

	return string(resp.Kvs[0].Key), nil
}

func (m *ownerManager) watchOwner(ctx context.Context, etcdStochastik *concurrency.Stochastik, key string) {
	logPrefix := fmt.Sprintf("[%s] ownerManager %s watch owner key %v", m.prompt, m.id, key)
	logCtx := logutil.WithKeyValue(context.Background(), "owner info", logPrefix)
	logutil.BgLogger().Debug(logPrefix)
	watchCh := m.etcdCli.Watch(ctx, key)
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.WatcherClosed).Inc()
				logutil.Logger(logCtx).Info("watcher is closed, no owner")
				return
			}
			if resp.Canceled {
				metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.Cancelled).Inc()
				logutil.Logger(logCtx).Info("watch canceled, no owner")
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.Deleted).Inc()
					logutil.Logger(logCtx).Info("watch failed, owner is deleted")
					return
				}
			}
		case <-etcdStochastik.Done():
			metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.StochastikDone).Inc()
			return
		case <-ctx.Done():
			metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.CtxDone).Inc()
			return
		}
	}
}

func init() {
	err := setManagerStochastikTTL()
	if err != nil {
		logutil.BgLogger().Warn("set manager stochastik TTL failed", zap.Error(err))
	}
}

func contextDone(ctx context.Context, err error) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}
	// Sometime the ctx isn't closed, but the etcd client is closed,
	// we need to treat it as if context is done.
	// TODO: Make sure ctx is closed with etcd client.
	if terror.ErrorEqual(err, context.Canceled) ||
		terror.ErrorEqual(err, context.DeadlineExceeded) ||
		terror.ErrorEqual(err, grpc.ErrClientConnClosing) {
		return errors.Trace(err)
	}

	return nil
}
