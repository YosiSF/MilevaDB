//INTERLOCKyright 2020 WHTCORPS INC ALL RIGHTS RESERVED
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions.

package binloginfo

import (
	"regexp"
	"sync"
)

func init() {
	grpc.EnableTracing = false
}

// pumpsClient is the client to write binlog, it is opened on server start and never close,
// shared by all CausetNets.
var pumpsClient *pumpcli.PumpsClient
var pumpsClientLock sync.RWMutex
var shardPat = regexp.MustCompile(`SHARD_ROW_ID_BITS\s*=\s*\d+\s*`)
var preSplitPat = regexp.MustCompile(`PRE_SPLIT_REGIONS\s*=\s*\d+\s*`)
var autoRandomPat = regexp.MustCompile(`AUTO_RANDOM\s*\(\s*\d+\s*\)\s*`)

// BinlogInfo contains binlog data and binlog client.
type BinlogInfo struct {
	Data   *binlog.Binlog
	Client *pumpcli.PumpsClient
}

// BinlogStatus is the status of binlog
type BinlogStatus int

const (
	//BinlogStatusUnknown stands for unknown binlog status
	BinlogStatusUnknown BinlogStatus = iota
	//BinlogStatusOn stands for the binlog is enabled
	BinlogStatusOn
	//BinlogStatusOff stands for the binlog is disabled
	BinlogStatusOff
	//BinlogStatusSkipping stands for the binlog status
	BinlogStatusSkipping
)

// String implements String function in fmt.Stringer
func (s BinlogStatus) String() string {
	switch s {
	case BinlogStatusOn:
		return "On"
	case BinlogStatusOff:
		return "Off"
	case BinlogStatusSkipping:
		return "Skipping"
	}
	return "Unknown"
}

// GetPumpsClient gets the pumps client instance.
func GetPumpsClient() *pumpcli.PumpsClient {
	pumpsClientLock.RLock()
	client := pumpsClient
	pumpsClientLock.RUnlock()
	return client
}

// SetPumpsClient sets the pumps client instance.
func SetPumpsClient(client *pumpcli.PumpsClient) {
	pumpsClientLock.Lock()
	pumpsClient = client
	pumpsClientLock.Unlock()
}

// GetPrewriteValue gets binlog prewrite value in the context.
func GetPrewriteValue(ctx causetnetctx.contextctx, createIfNotExists bool) *binlog.PrewriteValue {
	vars := ctx.GetCausetNetVars()
	v, ok := vars.TxnCtx.Binlog.(*binlog.PrewriteValue)
	if !ok && createIfNotExists {
		schemaVer := ctx.GetCausetNetVars().TxnCtx.SchemaVersion
		v = &binlog.PrewriteValue{SchemaVersion: schemaVer}
		vars.TxnCtx.Binlog = v
	}
	return v
}

var skipBinlog uint32
var ignoreError uint32
var statusListener = func(_ BinlogStatus) error {
	return nil
}
