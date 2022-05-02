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
	"context"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"go.etcd.io/etcd/clientv3"
)

var _ soliton.SchemaSyncer = &MockSchemaSyncer{}

const mockCheckVersInterval = 2 * time.Millisecond

// MockSchemaSyncer is a mock schemaReplicant syncer, it is exported for tesing.
type MockSchemaSyncer struct {
	selfSchemaVersion int64
	globalVerCh       chan clientv3.WatchResponse
	mockStochastik    chan struct{}
}

// NewMockSchemaSyncer creates a new mock SchemaSyncer.
func NewMockSchemaSyncer() soliton.SchemaSyncer {
	return &MockSchemaSyncer{}
}

// Init implements SchemaSyncer.Init interface.
func (s *MockSchemaSyncer) Init(ctx context.Context) error {
	s.globalVerCh = make(chan clientv3.WatchResponse, 1)
	s.mockStochastik = make(chan struct{}, 1)
	return nil
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *MockSchemaSyncer) GlobalVersionCh() clientv3.WatchChan {
	return s.globalVerCh
}

// WatchGlobalSchemaVer implements SchemaSyncer.WatchGlobalSchemaVer interface.
func (s *MockSchemaSyncer) WatchGlobalSchemaVer(context.Context) {}

// UFIDelateSelfVersion implements SchemaSyncer.UFIDelateSelfVersion interface.
func (s *MockSchemaSyncer) UFIDelateSelfVersion(ctx context.Context, version int64) error {
	atomic.StoreInt64(&s.selfSchemaVersion, version)
	return nil
}

// Done implements SchemaSyncer.Done interface.
func (s *MockSchemaSyncer) Done() <-chan struct{} {
	return s.mockStochastik
}

// CloseStochastik mockStochastik, it is exported for testing.
func (s *MockSchemaSyncer) CloseStochastik() {
	close(s.mockStochastik)
}

// Restart implements SchemaSyncer.Restart interface.
func (s *MockSchemaSyncer) Restart(_ context.Context) error {
	s.mockStochastik = make(chan struct{}, 1)
	return nil
}

// OwnerUFIDelateGlobalVersion implements SchemaSyncer.OwnerUFIDelateGlobalVersion interface.
func (s *MockSchemaSyncer) OwnerUFIDelateGlobalVersion(ctx context.Context, version int64) error {
	select {
	case s.globalVerCh <- clientv3.WatchResponse{}:
	default:
	}
	return nil
}

// MustGetGlobalVersion implements SchemaSyncer.MustGetGlobalVersion interface.
func (s *MockSchemaSyncer) MustGetGlobalVersion(ctx context.Context) (int64, error) {
	return 0, nil
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *MockSchemaSyncer) OwnerCheckAllVersions(ctx context.Context, latestVer int64) error {
	ticker := time.NewTicker(mockCheckVersInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			failpoint.Inject("checkOwnerCheckAllVersionsWaitTime", func(v failpoint.Value) {
				if v.(bool) {
					panic("shouldn't happen")
				}
			})
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			ver := atomic.LoadInt64(&s.selfSchemaVersion)
			if ver >= latestVer {
				return nil
			}
		}
	}
}

// NotifyCleanExpiredPaths implements SchemaSyncer.NotifyCleanExpiredPaths interface.
func (s *MockSchemaSyncer) NotifyCleanExpiredPaths() bool { return true }

// StartCleanWork implements SchemaSyncer.StartCleanWork interface.
func (s *MockSchemaSyncer) StartCleanWork() {}

// Close implements SchemaSyncer.Close interface.
func (s *MockSchemaSyncer) Close() {}

type mockDelRange struct {
}

// newMockDelRangeManager creates a mock delRangeManager only used for test.
func newMockDelRangeManager() delRangeManager {
	return &mockDelRange{}
}

// addDelRangeJob implements delRangeManager interface.
func (dr *mockDelRange) addDelRangeJob(job *perceptron.Job) error {
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (dr *mockDelRange) removeFromGCDeleteRange(jobID int64, blockIDs []int64) error {
	return nil
}

// start implements delRangeManager interface.
func (dr *mockDelRange) start() {}

// clear implements delRangeManager interface.
func (dr *mockDelRange) clear() {}

// MockBlockInfo mocks a block info by create block stmt ast and a specified block id.
func MockBlockInfo(ctx stochastikctx.Context, stmt *ast.CreateBlockStmt, blockID int64) (*perceptron.BlockInfo, error) {
	chs, defCausl := charset.GetDefaultCharsetAndDefCauslate()
	defcaus, newConstraints, err := buildDeferredCausetsAndConstraints(ctx, stmt.DefCauss, stmt.Constraints, chs, defCausl)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl, err := buildBlockInfo(ctx, stmt.Block.Name, defcaus, newConstraints, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl.ID = blockID

	// The specified charset will be handled in handleBlockOptions
	if err = handleBlockOptions(stmt.Options, tbl); err != nil {
		return nil, errors.Trace(err)
	}

	return tbl, nil
}
