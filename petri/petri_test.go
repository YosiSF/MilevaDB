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

package petri

import (
	"context"
	"crypto/tls"
	"math"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/ngaut/pools"
	dto "github.com/prometheus/client_perceptron/go"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/mockstore"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	"github.com/whtcorpsinc/MilevaDB-Prod/solomonkey"
	"github.com/whtcorpsinc/MilevaDB-Prod/errno"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri/infosync"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/mock"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/testleak"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"go.etcd.io/etcd/integration"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func mockFactory() (pools.Resource, error) {
	return nil, errors.New("mock factory should not be called")
}

func sysMockFactory(dom *Petri) (pools.Resource, error) {
	return nil, nil
}

type mockEtcdBackend struct {
	solomonkey.CausetStorage
	FIDelAddrs []string
}

func (mebd *mockEtcdBackend) EtcdAddrs() ([]string, error) {
	return mebd.FIDelAddrs, nil
}
func (mebd *mockEtcdBackend) TLSConfig() *tls.Config { return nil }
func (mebd *mockEtcdBackend) StartGCWorker() error {
	panic("not implemented")
}

// ETCD use ip:port as unix socket address, however this address is invalid on windows.
// We have to skip some of the test in such case.
// https://github.com/etcd-io/etcd/blob/f0faa5501d936cd8c9f561bb9d1baca70eb67ab1/pkg/types/urls.go#L42
func unixSocketAvailable() bool {
	c, err := net.Listen("unix", "127.0.0.1:0")
	if err == nil {
		c.Close()
		return true
	}
	return false
}

func TestInfo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a defCauson which is not allowed on Windows")
	}
	if !unixSocketAvailable() {
		return
	}
	testleak.BeforeTest()
	defer testleak.AfterTestT(t)()
	dbsLease := 80 * time.Millisecond
	s, err := mockstore.NewMockStore()
	if err != nil {
		t.Fatal(err)
	}
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	mockStore := &mockEtcdBackend{
		CausetStorage: s,
		FIDelAddrs:    []string{clus.Members[0].GRPCAddr()}}
	dom := NewPetri(mockStore, dbsLease, 0, 0, mockFactory)
	defer func() {
		dom.Close()
		s.Close()
	}()

	cli := clus.RandClient()
	dom.etcdClient = cli
	// Mock new DBS and init the schemaReplicant syncer with etcd client.
	goCtx := context.Background()
	dom.dbs = dbs.NewDBS(
		goCtx,
		dbs.WithEtcdClient(dom.GetEtcdClient()),
		dbs.WithStore(s),
		dbs.WithInfoHandle(dom.infoHandle),
		dbs.WithLease(dbsLease),
	)
	err = dom.dbs.Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	err = failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/petri/MockReplaceDBS", `return(true)`)
	if err != nil {
		t.Fatal(err)
	}
	err = dom.Init(dbsLease, sysMockFactory)
	if err != nil {
		t.Fatal(err)
	}
	err = failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/petri/MockReplaceDBS")
	if err != nil {
		t.Fatal(err)
	}

	// Test for GetServerInfo and GetServerInfoByID.
	dbsID := dom.dbs.GetID()
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		t.Fatal(err)
	}
	info, err := infosync.GetServerInfoByID(goCtx, dbsID)
	if err != nil {
		t.Fatal(err)
	}
	if serverInfo.ID != info.ID {
		t.Fatalf("server self info %v, info %v", serverInfo, info)
	}
	_, err = infosync.GetServerInfoByID(goCtx, "not_exist_id")
	if err == nil || (err != nil && err.Error() != "[info-syncer] get /milevadb/server/info/not_exist_id failed") {
		t.Fatal(err)
	}

	// Test for GetAllServerInfo.
	infos, err := infosync.GetAllServerInfo(goCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[dbsID].ID != info.ID {
		t.Fatalf("server one info %v, info %v", infos[dbsID], info)
	}

	// Test the scene where syncer.Done() gets the information.
	err = failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/soliton/ErrorMockStochastikDone", `return(true)`)
	if err != nil {
		t.Fatal(err)
	}
	<-dom.dbs.SchemaSyncer().Done()
	err = failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/dbs/soliton/ErrorMockStochastikDone")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	syncerStarted := false
	for i := 0; i < 1000; i++ {
		if dom.SchemaValidator.IsStarted() {
			syncerStarted = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !syncerStarted {
		t.Fatal("start syncer failed")
	}
	// Make sure loading schemaReplicant is normal.
	cs := &ast.CharsetOpt{
		Chs:     "utf8",
		DefCaus: "utf8_bin",
	}
	ctx := mock.NewContext()
	err = dom.dbs.CreateSchema(ctx, perceptron.NewCIStr("aaa"), cs)
	if err != nil {
		t.Fatal(err)
	}
	err = dom.Reload()
	if err != nil {
		t.Fatal(err)
	}
	if dom.SchemaReplicant().SchemaMetaVersion() != 1 {
		t.Fatalf("uFIDelate schemaReplicant version failed, ver %d", dom.SchemaReplicant().SchemaMetaVersion())
	}

	// Test for RemoveServerInfo.
	dom.info.RemoveServerInfo()
	infos, err = infosync.GetAllServerInfo(goCtx)
	if err != nil || len(infos) != 0 {
		t.Fatalf("err %v, infos %v", err, infos)
	}
}

type mockStochastikManager struct {
	PS []*soliton.ProcessInfo
}

func (msm *mockStochastikManager) ShowProcessList() map[uint64]*soliton.ProcessInfo {
	ret := make(map[uint64]*soliton.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockStochastikManager) GetProcessInfo(id uint64) (*soliton.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &soliton.ProcessInfo{}, false
}

func (msm *mockStochastikManager) Kill(cid uint64, query bool) {}

func (msm *mockStochastikManager) UFIDelateTLSConfig(cfg *tls.Config) {}

func (*testSuite) TestT(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dbsLease := 80 * time.Millisecond
	dom := NewPetri(causetstore, dbsLease, 0, 0, mockFactory)
	err = dom.Init(dbsLease, sysMockFactory)
	c.Assert(err, IsNil)
	ctx := mock.NewContext()
	ctx.CausetStore = dom.CausetStore()
	dd := dom.DBS()
	c.Assert(dd, NotNil)
	c.Assert(dd.GetLease(), Equals, 80*time.Millisecond)

	snapTS := oracle.EncodeTSO(oracle.GetPhysical(time.Now()))
	cs := &ast.CharsetOpt{
		Chs:     "utf8",
		DefCaus: "utf8_bin",
	}
	err = dd.CreateSchema(ctx, perceptron.NewCIStr("aaa"), cs)
	c.Assert(err, IsNil)
	// Test for fetchSchemasWithBlocks when "blocks" isn't nil.
	err = dd.CreateBlock(ctx, &ast.CreateBlockStmt{Block: &ast.BlockName{
		Schema: perceptron.NewCIStr("aaa"),
		Name:   perceptron.NewCIStr("tbl")}})
	c.Assert(err, IsNil)
	is := dom.SchemaReplicant()
	c.Assert(is, NotNil)

	// for uFIDelating the self schemaReplicant version
	goCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	c.Assert(err, IsNil)
	snapIs, err := dom.GetSnapshotSchemaReplicant(snapTS)
	c.Assert(snapIs, NotNil)
	c.Assert(err, IsNil)
	// Make sure that the self schemaReplicant version doesn't be changed.
	goCtx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = dd.SchemaSyncer().OwnerCheckAllVersions(goCtx, is.SchemaMetaVersion())
	cancel()
	c.Assert(err, IsNil)

	// for GetSnapshotSchemaReplicant
	currSnapTS := oracle.EncodeTSO(oracle.GetPhysical(time.Now()))
	currSnapIs, err := dom.GetSnapshotSchemaReplicant(currSnapTS)
	c.Assert(err, IsNil)
	c.Assert(currSnapIs, NotNil)
	c.Assert(currSnapIs.SchemaMetaVersion(), Equals, is.SchemaMetaVersion())

	// for GetSnapshotMeta
	dbInfo, ok := currSnapIs.SchemaByName(perceptron.NewCIStr("aaa"))
	c.Assert(ok, IsTrue)
	tbl, err := currSnapIs.BlockByName(perceptron.NewCIStr("aaa"), perceptron.NewCIStr("tbl"))
	c.Assert(err, IsNil)
	m, err := dom.GetSnapshotMeta(snapTS)
	c.Assert(err, IsNil)
	tblInfo1, err := m.GetBlock(dbInfo.ID, tbl.Meta().ID)
	c.Assert(meta.ErrDBNotExists.Equal(err), IsTrue)
	c.Assert(tblInfo1, IsNil)
	m, err = dom.GetSnapshotMeta(currSnapTS)
	c.Assert(err, IsNil)
	tblInfo2, err := m.GetBlock(dbInfo.ID, tbl.Meta().ID)
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta(), DeepEquals, tblInfo2)

	// Test for tryLoadSchemaDiffs when "isTooOldSchema" is false.
	err = dd.CreateSchema(ctx, perceptron.NewCIStr("bbb"), cs)
	c.Assert(err, IsNil)
	err = dom.Reload()
	c.Assert(err, IsNil)

	// for schemaValidator
	schemaVer := dom.SchemaValidator.(*schemaValidator).LatestSchemaVersion()
	ver, err := causetstore.CurrentVersion()
	c.Assert(err, IsNil)
	ts := ver.Ver

	_, succ := dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/MilevaDB-Prod/petri/ErrorMockReloadFailed", `return(true)`), IsNil)
	err = dom.Reload()
	c.Assert(err, NotNil)
	_, succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)
	time.Sleep(dbsLease)

	ver, err = causetstore.CurrentVersion()
	c.Assert(err, IsNil)
	ts = ver.Ver
	_, succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultUnknown)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/MilevaDB-Prod/petri/ErrorMockReloadFailed"), IsNil)
	err = dom.Reload()
	c.Assert(err, IsNil)
	_, succ = dom.SchemaValidator.Check(ts, schemaVer, nil)
	c.Assert(succ, Equals, ResultSucc)

	// For slow query.
	dom.LogSlowQuery(&SlowQueryInfo{ALLEGROALLEGROSQL: "aaa", Duration: time.Second, Internal: true})
	dom.LogSlowQuery(&SlowQueryInfo{ALLEGROALLEGROSQL: "bbb", Duration: 3 * time.Second})
	dom.LogSlowQuery(&SlowQueryInfo{ALLEGROALLEGROSQL: "ccc", Duration: 2 * time.Second})
	// DefCauslecting slow queries is asynchronous, wait a while to ensure it's done.
	time.Sleep(5 * time.Millisecond)

	res := dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2})
	c.Assert(res, HasLen, 2)
	c.Assert(res[0].ALLEGROALLEGROSQL, Equals, "bbb")
	c.Assert(res[0].Duration, Equals, 3*time.Second)
	c.Assert(res[1].ALLEGROALLEGROSQL, Equals, "ccc")
	c.Assert(res[1].Duration, Equals, 2*time.Second)

	res = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 2, HoTT: ast.ShowSlowHoTTInternal})
	c.Assert(res, HasLen, 1)
	c.Assert(res[0].ALLEGROALLEGROSQL, Equals, "aaa")
	c.Assert(res[0].Duration, Equals, time.Second)
	c.Assert(res[0].Internal, Equals, true)

	res = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowTop, Count: 4, HoTT: ast.ShowSlowHoTTAll})
	c.Assert(res, HasLen, 3)
	c.Assert(res[0].ALLEGROALLEGROSQL, Equals, "bbb")
	c.Assert(res[0].Duration, Equals, 3*time.Second)
	c.Assert(res[1].ALLEGROALLEGROSQL, Equals, "ccc")
	c.Assert(res[1].Duration, Equals, 2*time.Second)
	c.Assert(res[2].ALLEGROALLEGROSQL, Equals, "aaa")
	c.Assert(res[2].Duration, Equals, time.Second)
	c.Assert(res[2].Internal, Equals, true)

	res = dom.ShowSlowQuery(&ast.ShowSlow{Tp: ast.ShowSlowRecent, Count: 2})
	c.Assert(res, HasLen, 2)
	c.Assert(res[0].ALLEGROALLEGROSQL, Equals, "ccc")
	c.Assert(res[0].Duration, Equals, 2*time.Second)
	c.Assert(res[1].ALLEGROALLEGROSQL, Equals, "bbb")
	c.Assert(res[1].Duration, Equals, 3*time.Second)

	metrics.PanicCounter.Reset()
	// Since the stats lease is 0 now, so create a new ticker will panic.
	// Test that they can recover from panic correctly.
	dom.uFIDelateStatsWorker(ctx, nil)
	dom.autoAnalyzeWorker(nil)
	counter := metrics.PanicCounter.WithLabelValues(metrics.LabelPetri)
	pb := &dto.Metric{}
	counter.Write(pb)
	c.Assert(pb.GetCounter().GetValue(), Equals, float64(2))

	sINTERLOCKe := dom.GetSINTERLOCKe("status")
	c.Assert(sINTERLOCKe, Equals, variable.DefaultStatusVarSINTERLOCKeFlag)

	// For schemaReplicant check, it tests for getting the result of "ResultUnknown".
	schemaChecker := NewSchemaChecker(dom, is.SchemaMetaVersion(), nil)
	originalRetryTime := SchemaOutOfDateRetryTimes
	originalRetryInterval := SchemaOutOfDateRetryInterval
	// Make sure it will retry one time and doesn't take a long time.
	SchemaOutOfDateRetryTimes = 1
	SchemaOutOfDateRetryInterval = int64(time.Millisecond * 1)
	defer func() {
		SchemaOutOfDateRetryTimes = originalRetryTime
		SchemaOutOfDateRetryInterval = originalRetryInterval
	}()
	dom.SchemaValidator.Stop()
	_, err = schemaChecker.Check(uint64(123456))
	c.Assert(err.Error(), Equals, ErrSchemaReplicantExpired.Error())
	dom.SchemaValidator.Reset()

	// Test for reporting min start timestamp.
	infoSyncer := dom.InfoSyncer()
	sm := &mockStochastikManager{
		PS: make([]*soliton.ProcessInfo, 0),
	}
	infoSyncer.SetStochastikManager(sm)
	beforeTS := variable.GoTimeToTS(time.Now())
	infoSyncer.ReportMinStartTS(dom.CausetStore())
	afterTS := variable.GoTimeToTS(time.Now())
	c.Assert(infoSyncer.GetMinStartTS() > beforeTS && infoSyncer.GetMinStartTS() < afterTS, IsFalse)
	lowerLimit := time.Now().Add(-time.Duration(solomonkey.MaxTxnTimeUse) * time.Millisecond)
	validTS := variable.GoTimeToTS(lowerLimit.Add(time.Minute))
	sm.PS = []*soliton.ProcessInfo{
		{CurTxnStartTS: 0},
		{CurTxnStartTS: math.MaxUint64},
		{CurTxnStartTS: variable.GoTimeToTS(lowerLimit)},
		{CurTxnStartTS: validTS},
	}
	infoSyncer.SetStochastikManager(sm)
	infoSyncer.ReportMinStartTS(dom.CausetStore())
	c.Assert(infoSyncer.GetMinStartTS() == validTS, IsTrue)

	err = causetstore.Close()
	c.Assert(err, IsNil)
	isClose := dom.isClose()
	c.Assert(isClose, IsFalse)
	dom.Close()
	isClose = dom.isClose()
	c.Assert(isClose, IsTrue)
}

type testResource struct {
	status int
}

func (tr *testResource) Close() { tr.status = 1 }

func (*testSuite) TestStochastikPool(c *C) {
	f := func() (pools.Resource, error) { return &testResource{}, nil }
	pool := newStochastikPool(1, f)
	tr, err := pool.Get()
	c.Assert(err, IsNil)
	tr1, err := pool.Get()
	c.Assert(err, IsNil)
	pool.Put(tr)
	// Capacity is 1, so tr1 is closed.
	pool.Put(tr1)
	c.Assert(tr1.(*testResource).status, Equals, 1)
	pool.Close()

	pool.Close()
	pool.Put(tr1)
	tr, err = pool.Get()
	c.Assert(err.Error(), Equals, "stochastik pool closed")
	c.Assert(tr, IsNil)
}

func (*testSuite) TestErrorCode(c *C) {
	c.Assert(int(terror.ToALLEGROSQLError(ErrSchemaReplicantExpired).Code), Equals, errno.ErrSchemaReplicantExpired)
	c.Assert(int(terror.ToALLEGROSQLError(ErrSchemaReplicantChanged).Code), Equals, errno.ErrSchemaReplicantChanged)
}
