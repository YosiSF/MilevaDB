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

package infosync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs/memristed"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/errno"
	"github.com/whtcorpsinc/MilevaDB-Prod/owner"
	util2 "github.com/whtcorpsinc/MilevaDB-Prod/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/FIDelapi"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/replog"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/versioninfo"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
)

const (
	// ServerInformationPath causetstore server information such as IP, port and so on.
	ServerInformationPath = "/milevadb/server/info"
	// ServerMinStartTSPath causetstore the server min start timestamp.
	ServerMinStartTSPath = "/milevadb/server/minstartts"
	// TiFlashBlockSyncProgressPath causetstore the tiflash block replica sync progress.
	TiFlashBlockSyncProgressPath = "/tiflash/block/sync"
	// keyOFIDelefaultRetryCnt is the default retry count for etcd causetstore.
	keyOFIDelefaultRetryCnt = 5
	// keyOFIDelefaultTimeout is the default time out for etcd causetstore.
	keyOFIDelefaultTimeout = 1 * time.Second
	// InfoStochastikTTL is the ETCD stochastik's TTL in seconds.
	InfoStochastikTTL = 10 * 60
	// ReportInterval is interval of infoSyncerKeeper reporting min startTS.
	ReportInterval = 30 * time.Second
	// TopologyInformationPath means etcd path for storing topology info.
	TopologyInformationPath = "/topology/milevadb"
	// TopologyStochastikTTL is ttl for topology, ant it's the ETCD stochastik's TTL in seconds.
	TopologyStochastikTTL = 45
	// TopologyTimeToRefresh means time to refresh etcd.
	TopologyTimeToRefresh = 30 * time.Second
	// TopologyPrometheus means address of prometheus.
	TopologyPrometheus = "/topology/prometheus"
	// BlockPrometheusCacheExpiry is the expiry time for prometheus address cache.
	BlockPrometheusCacheExpiry = 10 * time.Second
)

// ErrPrometheusAddrIsNotSet is the error that Prometheus address is not set in FIDel and etcd
var ErrPrometheusAddrIsNotSet = terror.ClassPetri.New(errno.ErrPrometheusAddrIsNotSet, errno.MyALLEGROSQLErrName[errno.ErrPrometheusAddrIsNotSet])

// InfoSyncer stores server info to etcd when the milevadb-server starts and delete when milevadb-server shuts down.
type InfoSyncer struct {
	etcdCli            *clientv3.Client
	info               *ServerInfo
	serverInfoPath     string
	minStartTS         uint64
	minStartTSPath     string
	manager            util2.StochastikManager
	stochastik         *concurrency.Stochastik
	topologyStochastik *concurrency.Stochastik
	prometheusAddr     string
	modifyTime         time.Time
}

// ServerInfo is server static information.
// It will not be uFIDelated when milevadb-server running. So please only put static information in ServerInfo struct.
type ServerInfo struct {
	ServerVersionInfo
	ID             string            `json:"dbs_id"`
	IP             string            `json:"ip"`
	Port           uint              `json:"listening_port"`
	StatusPort     uint              `json:"status_port"`
	Lease          string            `json:"lease"`
	BinlogStatus   string            `json:"binlog_status"`
	StartTimestamp int64             `json:"start_timestamp"`
	Labels         map[string]string `json:"labels"`
}

// ServerVersionInfo is the server version and git_hash.
type ServerVersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

// globalInfoSyncer stores the global infoSyncer.
// Use a global variable for simply the code, use the petri.infoSyncer will have circle import problem in some pkg.
// Use atomic.Value to avoid data race in the test.
var globalInfoSyncer atomic.Value

func getGlobalInfoSyncer() (*InfoSyncer, error) {
	v := globalInfoSyncer.Load()
	if v == nil {
		return nil, errors.New("infoSyncer is not initialized")
	}
	return v.(*InfoSyncer), nil
}

func setGlobalInfoSyncer(is *InfoSyncer) {
	globalInfoSyncer.CausetStore(is)
}

// GlobalInfoSyncerInit return a new InfoSyncer. It is exported for testing.
func GlobalInfoSyncerInit(ctx context.Context, id string, etcdCli *clientv3.Client, skipRegisterToDashBoard bool) (*InfoSyncer, error) {
	is := &InfoSyncer{
		etcdCli:        etcdCli,
		info:           getServerInfo(id),
		serverInfoPath: fmt.Sprintf("%s/%s", ServerInformationPath, id),
		minStartTSPath: fmt.Sprintf("%s/%s", ServerMinStartTSPath, id),
	}
	err := is.init(ctx, skipRegisterToDashBoard)
	if err != nil {
		return nil, err
	}
	setGlobalInfoSyncer(is)
	return is, nil
}

// Init creates a new etcd stochastik and stores server info to etcd.
func (is *InfoSyncer) init(ctx context.Context, skipRegisterToDashboard bool) error {
	err := is.newStochastikAndStoreServerInfo(ctx, owner.NewStochastikDefaultRetryCnt)
	if err != nil {
		return err
	}
	if skipRegisterToDashboard {
		return nil
	}
	return is.newTopologyStochastikAndStoreServerInfo(ctx, owner.NewStochastikDefaultRetryCnt)
}

// SetStochastikManager set the stochastik manager for InfoSyncer.
func (is *InfoSyncer) SetStochastikManager(manager util2.StochastikManager) {
	is.manager = manager
}

// GetServerInfo gets self server static information.
func GetServerInfo() (*ServerInfo, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.info, nil
}

// GetServerInfoByID gets specified server static information from etcd.
func GetServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.getServerInfoByID(ctx, id)
}

func (is *InfoSyncer) getServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	if is.etcdCli == nil || id == is.info.ID {
		return is.info, nil
	}
	key := fmt.Sprintf("%s/%s", ServerInformationPath, id)
	infoMap, err := getInfo(ctx, is.etcdCli, key, keyOFIDelefaultRetryCnt, keyOFIDelefaultTimeout)
	if err != nil {
		return nil, err
	}
	info, ok := infoMap[id]
	if !ok {
		return nil, errors.Errorf("[info-syncer] get %s failed", key)
	}
	return info, nil
}

// GetAllServerInfo gets all servers static information from etcd.
func GetAllServerInfo(ctx context.Context) (map[string]*ServerInfo, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.getAllServerInfo(ctx)
}

// UFIDelateTiFlashBlockSyncProgress is used to uFIDelate the tiflash block replica sync progress.
func UFIDelateTiFlashBlockSyncProgress(ctx context.Context, tid int64, progress float64) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	if is.etcdCli == nil {
		return nil
	}
	key := fmt.Sprintf("%s/%v", TiFlashBlockSyncProgressPath, tid)
	return soliton.PutKVToEtcd(ctx, is.etcdCli, keyOFIDelefaultRetryCnt, key, strconv.FormatFloat(progress, 'f', 2, 64))
}

// DeleteTiFlashBlockSyncProgress is used to delete the tiflash block replica sync progress.
func DeleteTiFlashBlockSyncProgress(tid int64) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	if is.etcdCli == nil {
		return nil
	}
	key := fmt.Sprintf("%s/%v", TiFlashBlockSyncProgressPath, tid)
	return soliton.DeleteKeyFromEtcd(key, is.etcdCli, keyOFIDelefaultRetryCnt, keyOFIDelefaultTimeout)
}

// GetTiFlashBlockSyncProgress uses to get all the tiflash block replica sync progress.
func GetTiFlashBlockSyncProgress(ctx context.Context) (map[int64]float64, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	progressMap := make(map[int64]float64)
	if is.etcdCli == nil {
		return progressMap, nil
	}
	for i := 0; i < keyOFIDelefaultRetryCnt; i++ {
		resp, err := is.etcdCli.Get(ctx, TiFlashBlockSyncProgressPath+"/", clientv3.WithPrefix())
		if err != nil {
			logutil.BgLogger().Info("get tiflash block replica sync progress failed, continue checking.", zap.Error(err))
			continue
		}
		for _, ekv := range resp.Kvs {
			tid, err := strconv.ParseInt(string(ekv.Key[len(TiFlashBlockSyncProgressPath)+1:]), 10, 64)
			if err != nil {
				logutil.BgLogger().Info("invalid tiflash block replica sync progress key.", zap.String("key", string(ekv.Key)))
				continue
			}
			progress, err := strconv.ParseFloat(string(ekv.Value), 64)
			if err != nil {
				logutil.BgLogger().Info("invalid tiflash block replica sync progress value.",
					zap.String("key", string(ekv.Key)), zap.String("value", string(ekv.Value)))
				continue
			}
			progressMap[tid] = progress
		}
		break
	}
	return progressMap, nil
}

func doRequest(ctx context.Context, addrs []string, route, method string, body io.Reader) ([]byte, error) {
	var err error
	var req *http.Request
	var res *http.Response
	for _, addr := range addrs {
		var url string
		if strings.HasPrefix(addr, "http://") {
			url = fmt.Sprintf("%s%s", addr, route)
		} else {
			url = fmt.Sprintf("http://%s%s", addr, route)
		}

		if ctx != nil {
			req, err = http.NewRequestWithContext(ctx, method, url, body)
		} else {
			req, err = http.NewRequest(method, url, body)
		}
		if err != nil {
			return nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		res, err = http.DefaultClient.Do(req)
		if err == nil {
			defer terror.Call(res.Body.Close)

			bodyBytes, err := ioutil.ReadAll(res.Body)
			if res.StatusCode != http.StatusOK {
				err = errors.Wrapf(err, "%s", bodyBytes)
			}
			return bodyBytes, err
		}
	}
	return nil, err
}

// GetPlacementMemrules is used to retrieve memristed rules from FIDel.
func GetPlacementMemrules(ctx context.Context) ([]*memristed.MemruleOp, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}

	if is.etcdCli == nil {
		return nil, nil
	}

	addrs := is.etcdCli.Endpoints()

	if len(addrs) == 0 {
		return nil, errors.Errorf("fidel unavailable")
	}

	res, err := doRequest(ctx, addrs, path.Join(FIDelapi.Config, "rules"), http.MethodGet, nil)
	if err != nil {
		return nil, err
	}

	var rules []*memristed.MemruleOp
	err = json.Unmarshal(res, &rules)
	if err != nil {
		return nil, err
	}
	return rules, nil
}

// UFIDelatePlacementMemrules is used to notify FIDel changes of memristed rules.
func UFIDelatePlacementMemrules(ctx context.Context, rules []*memristed.MemruleOp) error {
	if len(rules) == 0 {
		return nil
	}

	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}

	if is.etcdCli == nil {
		return nil
	}

	addrs := is.etcdCli.Endpoints()

	if len(addrs) == 0 {
		return errors.Errorf("fidel unavailable")
	}

	b, err := json.Marshal(rules)
	if err != nil {
		return err
	}

	_, err = doRequest(ctx, addrs, path.Join(FIDelapi.Config, "rules/batch"), http.MethodPost, bytes.NewReader(b))
	return err
}

func (is *InfoSyncer) getAllServerInfo(ctx context.Context) (map[string]*ServerInfo, error) {
	allInfo := make(map[string]*ServerInfo)
	if is.etcdCli == nil {
		allInfo[is.info.ID] = getServerInfo(is.info.ID)
		return allInfo, nil
	}
	allInfo, err := getInfo(ctx, is.etcdCli, ServerInformationPath, keyOFIDelefaultRetryCnt, keyOFIDelefaultTimeout, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	return allInfo, nil
}

// storeServerInfo stores self server static information to etcd.
func (is *InfoSyncer) storeServerInfo(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	infoBuf, err := json.Marshal(is.info)
	if err != nil {
		return errors.Trace(err)
	}
	str := string(replog.String(infoBuf))
	err = soliton.PutKVToEtcd(ctx, is.etcdCli, keyOFIDelefaultRetryCnt, is.serverInfoPath, str, clientv3.WithLease(is.stochastik.Lease()))
	return err
}

// RemoveServerInfo remove self server static information from etcd.
func (is *InfoSyncer) RemoveServerInfo() {
	if is.etcdCli == nil {
		return
	}
	err := soliton.DeleteKeyFromEtcd(is.serverInfoPath, is.etcdCli, keyOFIDelefaultRetryCnt, keyOFIDelefaultTimeout)
	if err != nil {
		logutil.BgLogger().Error("remove server info failed", zap.Error(err))
	}
}

type topologyInfo struct {
	ServerVersionInfo
	StatusPort     uint              `json:"status_port"`
	DeployPath     string            `json:"deploy_path"`
	StartTimestamp int64             `json:"start_timestamp"`
	Labels         map[string]string `json:"labels"`
}

func (is *InfoSyncer) getTopologyInfo() topologyInfo {
	s, err := os.Execublock()
	if err != nil {
		s = ""
	}
	dir := path.Dir(s)
	return topologyInfo{
		ServerVersionInfo: ServerVersionInfo{
			Version: allegrosql.MilevaDBReleaseVersion,
			GitHash: is.info.ServerVersionInfo.GitHash,
		},
		StatusPort:     is.info.StatusPort,
		DeployPath:     dir,
		StartTimestamp: is.info.StartTimestamp,
		Labels:         is.info.Labels,
	}
}

// StoreTopologyInfo  stores the topology of milevadb to etcd.
func (is *InfoSyncer) StoreTopologyInfo(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	topologyInfo := is.getTopologyInfo()
	infoBuf, err := json.Marshal(topologyInfo)
	if err != nil {
		return errors.Trace(err)
	}
	str := string(replog.String(infoBuf))
	key := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, is.info.IP, is.info.Port)
	// Note: no lease is required here.
	err = soliton.PutKVToEtcd(ctx, is.etcdCli, keyOFIDelefaultRetryCnt, key, str)
	if err != nil {
		return err
	}
	// Initialize ttl.
	return is.uFIDelateTopologyAliveness(ctx)
}

// GetMinStartTS get min start timestamp.
// Export for testing.
func (is *InfoSyncer) GetMinStartTS() uint64 {
	return is.minStartTS
}

// storeMinStartTS stores self server min start timestamp to etcd.
func (is *InfoSyncer) storeMinStartTS(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	return soliton.PutKVToEtcd(ctx, is.etcdCli, keyOFIDelefaultRetryCnt, is.minStartTSPath,
		strconv.FormatUint(is.minStartTS, 10),
		clientv3.WithLease(is.stochastik.Lease()))
}

// RemoveMinStartTS removes self server min start timestamp from etcd.
func (is *InfoSyncer) RemoveMinStartTS() {
	if is.etcdCli == nil {
		return
	}
	err := soliton.DeleteKeyFromEtcd(is.minStartTSPath, is.etcdCli, keyOFIDelefaultRetryCnt, keyOFIDelefaultTimeout)
	if err != nil {
		logutil.BgLogger().Error("remove minStartTS failed", zap.Error(err))
	}
}

// ReportMinStartTS reports self server min start timestamp to ETCD.
func (is *InfoSyncer) ReportMinStartTS(causetstore ekv.CausetStorage) {
	if is.manager == nil {
		// Server may not start in time.
		return
	}
	pl := is.manager.ShowProcessList()

	// Calculate the lower limit of the start timestamp to avoid extremely old transaction delaying GC.
	currentVer, err := causetstore.CurrentVersion()
	if err != nil {
		logutil.BgLogger().Error("uFIDelate minStartTS failed", zap.Error(err))
		return
	}
	now := time.Unix(0, oracle.ExtractPhysical(currentVer.Ver)*1e6)
	startTSLowerLimit := variable.GoTimeToTS(now.Add(-time.Duration(ekv.MaxTxnTimeUse) * time.Millisecond))

	minStartTS := variable.GoTimeToTS(now)
	for _, info := range pl {
		if info.CurTxnStartTS > startTSLowerLimit && info.CurTxnStartTS < minStartTS {
			minStartTS = info.CurTxnStartTS
		}
	}

	is.minStartTS = minStartTS
	err = is.storeMinStartTS(context.Background())
	if err != nil {
		logutil.BgLogger().Error("uFIDelate minStartTS failed", zap.Error(err))
	}
}

// Done returns a channel that closes when the info syncer is no longer being refreshed.
func (is *InfoSyncer) Done() <-chan struct{} {
	if is.etcdCli == nil {
		return make(chan struct{}, 1)
	}
	return is.stochastik.Done()
}

// TopologyDone returns a channel that closes when the topology syncer is no longer being refreshed.
func (is *InfoSyncer) TopologyDone() <-chan struct{} {
	if is.etcdCli == nil {
		return make(chan struct{}, 1)
	}
	return is.topologyStochastik.Done()
}

// Restart restart the info syncer with new stochastik leaseID and causetstore server info to etcd again.
func (is *InfoSyncer) Restart(ctx context.Context) error {
	return is.newStochastikAndStoreServerInfo(ctx, owner.NewStochastikDefaultRetryCnt)
}

// RestartTopology restart the topology syncer with new stochastik leaseID and causetstore server info to etcd again.
func (is *InfoSyncer) RestartTopology(ctx context.Context) error {
	return is.newTopologyStochastikAndStoreServerInfo(ctx, owner.NewStochastikDefaultRetryCnt)
}

// newStochastikAndStoreServerInfo creates a new etcd stochastik and stores server info to etcd.
func (is *InfoSyncer) newStochastikAndStoreServerInfo(ctx context.Context, retryCnt int) error {
	if is.etcdCli == nil {
		return nil
	}
	logPrefix := fmt.Sprintf("[Info-syncer] %s", is.serverInfoPath)
	stochastik, err := owner.NewStochastik(ctx, logPrefix, is.etcdCli, retryCnt, InfoStochastikTTL)
	if err != nil {
		return err
	}
	is.stochastik = stochastik
	binloginfo.RegisterStatusListener(func(status binloginfo.BinlogStatus) error {
		is.info.BinlogStatus = status.String()
		err := is.storeServerInfo(ctx)
		return errors.Trace(err)
	})
	return is.storeServerInfo(ctx)
}

// newTopologyStochastikAndStoreServerInfo creates a new etcd stochastik and stores server info to etcd.
func (is *InfoSyncer) newTopologyStochastikAndStoreServerInfo(ctx context.Context, retryCnt int) error {
	if is.etcdCli == nil {
		return nil
	}
	logPrefix := fmt.Sprintf("[topology-syncer] %s/%s:%d", TopologyInformationPath, is.info.IP, is.info.Port)
	stochastik, err := owner.NewStochastik(ctx, logPrefix, is.etcdCli, retryCnt, TopologyStochastikTTL)
	if err != nil {
		return err
	}

	is.topologyStochastik = stochastik
	return is.StoreTopologyInfo(ctx)
}

// refreshTopology refreshes etcd topology with ttl stored in "/topology/milevadb/ip:port/ttl".
func (is *InfoSyncer) uFIDelateTopologyAliveness(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	key := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, is.info.IP, is.info.Port)
	return soliton.PutKVToEtcd(ctx, is.etcdCli, keyOFIDelefaultRetryCnt, key,
		fmt.Sprintf("%v", time.Now().UnixNano()),
		clientv3.WithLease(is.topologyStochastik.Lease()))
}

// GetPrometheusAddr gets prometheus Address
func GetPrometheusAddr() (string, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return "", err
	}

	// if the cache of prometheusAddr is over 10s, uFIDelate the prometheusAddr
	if time.Since(is.modifyTime) < BlockPrometheusCacheExpiry {
		return is.prometheusAddr, nil
	}
	return is.getPrometheusAddr()
}

type prometheus struct {
	IP         string `json:"ip"`
	BinaryPath string `json:"binary_path"`
	Port       int    `json:"port"`
}

type metricStorage struct {
	FIDelServer struct {
		MetricStorage string `json:"metric-storage"`
	} `json:"fidel-server"`
}

func (is *InfoSyncer) getPrometheusAddr() (string, error) {
	// Get FIDel servers info.
	FIDelAddrs := is.etcdCli.Endpoints()
	if len(FIDelAddrs) == 0 {
		return "", errors.Errorf("fidel unavailable")
	}

	// Get prometheus address from FIDelApi.
	var url, res string
	if strings.HasPrefix(FIDelAddrs[0], "http://") {
		url = fmt.Sprintf("%s%s", FIDelAddrs[0], FIDelapi.Config)
	} else {
		url = fmt.Sprintf("http://%s%s", FIDelAddrs[0], FIDelapi.Config)
	}
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	var metricStorage metricStorage
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&metricStorage)
	if err != nil {
		return "", err
	}
	res = metricStorage.FIDelServer.MetricStorage

	// Get prometheus address from etcdApi.
	if res == "" {
		values, err := is.getPrometheusAddrFromEtcd(TopologyPrometheus)
		if err != nil {
			return "", errors.Trace(err)
		}
		if values == "" {
			return "", ErrPrometheusAddrIsNotSet
		}
		var prometheus prometheus
		err = json.Unmarshal([]byte(values), &prometheus)
		if err != nil {
			return "", errors.Trace(err)
		}
		res = fmt.Sprintf("http://%s:%v", prometheus.IP, prometheus.Port)
	}
	is.prometheusAddr = res
	is.modifyTime = time.Now()
	setGlobalInfoSyncer(is)
	return res, nil
}

func (is *InfoSyncer) getPrometheusAddrFromEtcd(k string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), keyOFIDelefaultTimeout)
	resp, err := is.etcdCli.Get(ctx, k)
	cancel()
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Kvs) > 0 {
		return string(resp.Kvs[0].Value), nil
	}
	return "", nil
}

// getInfo gets server information from etcd according to the key and opts.
func getInfo(ctx context.Context, etcdCli *clientv3.Client, key string, retryCnt int, timeout time.Duration, opts ...clientv3.OpOption) (map[string]*ServerInfo, error) {
	var err error
	var resp *clientv3.GetResponse
	allInfo := make(map[string]*ServerInfo)
	for i := 0; i < retryCnt; i++ {
		select {
		case <-ctx.Done():
			err = errors.Trace(ctx.Err())
			return nil, err
		default:
		}
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err = etcdCli.Get(childCtx, key, opts...)
		cancel()
		if err != nil {
			logutil.BgLogger().Info("get key failed", zap.String("key", key), zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, ekv := range resp.Kvs {
			info := &ServerInfo{
				BinlogStatus: binloginfo.BinlogStatusUnknown.String(),
			}
			err = json.Unmarshal(ekv.Value, info)
			if err != nil {
				logutil.BgLogger().Info("get key failed", zap.String("key", string(ekv.Key)), zap.ByteString("value", ekv.Value),
					zap.Error(err))
				return nil, errors.Trace(err)
			}
			allInfo[info.ID] = info
		}
		return allInfo, nil
	}
	return nil, errors.Trace(err)
}

// getServerInfo gets self milevadb server information.
func getServerInfo(id string) *ServerInfo {
	cfg := config.GetGlobalConfig()
	info := &ServerInfo{
		ID:             id,
		IP:             cfg.AdvertiseAddress,
		Port:           cfg.Port,
		StatusPort:     cfg.Status.StatusPort,
		Lease:          cfg.Lease,
		BinlogStatus:   binloginfo.GetStatus().String(),
		StartTimestamp: time.Now().Unix(),
		Labels:         cfg.Labels,
	}
	info.Version = allegrosql.ServerVersion
	info.GitHash = versioninfo.MilevaDBGitHash

	failpoint.Inject("mockServerInfo", func(val failpoint.Value) {
		if val.(bool) {
			info.StartTimestamp = 1282967700000
			info.Labels = map[string]string{
				"foo": "bar",
			}
		}
	})

	return info
}
