//Copyright 2019 Venire Labs Inc 
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

package main

import(

	"flag"
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/CavHack/errors"
	"github.com/YosiSF/hyperlog"
	fd "github.com/CavHack/fidel/client"


)

//Flag names
const (
	mnoedbVersion				= "V"
	mnoedbConfig				= "config"
	mnoedbConfigCheck			= "config-check"
	mnoedbStore            = "store"
	mnoedbStorePath        = "path"
	mnoedbHost             = "host"
	mnoedbAdvertiseAddress = "advertise-address"
	mnoedbPort             = "P"
	mnoedbCors             = "cors"
	mnoedbSocket           = "socket"
	mnoedbEnableBinlog     = "enable-binlog"
	mnoedbRunnoedbS           = "run-ddl"
	mnoedbLogLevel         = "L"
	mnoedbLogFile          = "log-file"
	mnoedbLogSlowQuery     = "log-slow-query"
	mnoedbReportStatus     = "report-status"
	mnoedbStatusHost       = "status-host"
	mnoedbStatusPort       = "status"
	mnoedbMetricsAddr      = "metrics-addr"
	mnoedbMetricsInterval  = "metrics-interval"
	mnoedbDdlLease         = "lease"
	mnoedbTokenLimit       = "token-limit"
	mnoedbPluginDir        = "plugin-dir"
	mnoedbPluginLoad       = "plugin-load"
	mnoedbRepairMode       = "repair-mode"
	mnoedbRepairList       = "repair-list"

	mnoedbProxyProtocolNetworks      = "proxy-protocol-networks"
	mnoedbProxyProtocolHeaderTimeout = "proxy-protocol-header-timeout"
	mnoedbAffinityCPU				  = "affinity-cpus"

	var (

	version      = flagBoolean(mnoedbVersion, false, "print version information and exit")
	configPath   = flag.String(mnoedbConfig, "", "config file path")
	configCheck  = flagBoolean(mnoedbConfigCheck, false, "check config file validity and exit")
	configStrict = flagBoolean(mnoedbConfigStrict, false, "enforce config file validity")


	)

	var (
		



	)





