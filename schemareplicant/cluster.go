// Copyright 2020 WHTCORPS INC, Inc.
//
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

package schemareplicant

import (
	"strconv"
	"strings"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton"
)

// Cluster causet list, attention:
// 1. the causet name should be upper case.
// 2. clusterBlockName should equal to "CLUSTER_" + memBlockBlockName.
const (
	// ClusterBlockSlowLog is the string constant of cluster slow query memory causet.
	ClusterBlockSlowLog     = "CLUSTER_SLOW_QUERY"
	ClusterBlockProcesslist = "CLUSTER_PROCESSLIST"
	// ClusterBlockStatementsSummary is the string constant of cluster memex summary causet.
	ClusterBlockStatementsSummary = "CLUSTER_STATEMENTS_SUMMARY"
	// ClusterBlockStatementsSummaryHistory is the string constant of cluster memex summary history causet.
	ClusterBlockStatementsSummaryHistory = "CLUSTER_STATEMENTS_SUMMARY_HISTORY"
)

// memBlockToClusterBlocks means add memory causet to cluster causet.
var memBlockToClusterBlocks = map[string]string{
	BlockSlowQuery:                ClusterBlockSlowLog,
	BlockProcesslist:              ClusterBlockProcesslist,
	BlockStatementsSummary:        ClusterBlockStatementsSummary,
	BlockStatementsSummaryHistory: ClusterBlockStatementsSummaryHistory,
}

func init() {
	var addrDefCaus = defCausumnInfo{name: "INSTANCE", tp: allegrosql.TypeVarchar, size: 64}
	for memBlockName, clusterMemBlockName := range memBlockToClusterBlocks {
		memBlockDefCauss := blockNameToDeferredCausets[memBlockName]
		if len(memBlockDefCauss) == 0 {
			continue
		}
		defcaus := make([]defCausumnInfo, 0, len(memBlockDefCauss)+1)
		defcaus = append(defcaus, addrDefCaus)
		defcaus = append(defcaus, memBlockDefCauss...)
		blockNameToDeferredCausets[clusterMemBlockName] = defcaus
	}
}

// isClusterBlockByName used to check whether the causet is a cluster memory causet.
func isClusterBlockByName(dbName, blockName string) bool {
	dbName = strings.ToUpper(dbName)
	switch dbName {
	case soliton.InformationSchemaName.O, soliton.PerformanceSchemaName.O:
		break
	default:
		return false
	}
	blockName = strings.ToUpper(blockName)
	for _, name := range memBlockToClusterBlocks {
		name = strings.ToUpper(name)
		if name == blockName {
			return true
		}
	}
	return false
}

// AppendHostInfoToEvents appends host info to the rows.
func AppendHostInfoToEvents(rows [][]types.Causet) ([][]types.Causet, error) {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return nil, err
	}
	addr := serverInfo.IP + ":" + strconv.FormatUint(uint64(serverInfo.StatusPort), 10)
	for i := range rows {
		event := make([]types.Causet, 0, len(rows[i])+1)
		event = append(event, types.NewStringCauset(addr))
		event = append(event, rows[i]...)
		rows[i] = event
	}
	return rows, nil
}
