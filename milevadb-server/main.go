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
	mdbVersion				= "V"
	mdbConfig				= "config"
	mdbConfigCheck			= "config-check"
	mdbStore            = "store"
	mdbStorePath        = "path"
	mdbHost             = "host"
	mdbAdvertiseAddress = "advertise-address"
	mdbPort             = "P"
	mdbCors             = "cors"
	mdbSocket           = "socket"
	mdbEnableBinlog     = "enable-binlog"
	mdbRunDBS           = "run-ddl"
	mdbLogLevel         = "L"
	mdbLogFile          = "log-file"
	mdbLogSlowQuery     = "log-slow-query"
	mdbReportStatus     = "report-status"
	mdbStatusHost       = "status-host"
	mdbStatusPort       = "status"
	mdbMetricsAddr      = "metrics-addr"
	mdbMetricsInterval  = "metrics-interval"
	mdbDdlLease         = "lease"
	mdbTokenLimit       = "token-limit"
	mdbPluginDir        = "plugin-dir"
	mdbPluginLoad       = "plugin-load"
	mdbRepairMode       = "repair-mode"
	mdbRepairList       = "repair-list"

	mdbProxyProtocolNetworks      = "proxy-protocol-networks"
	mdbProxyProtocolHeaderTimeout = "proxy-protocol-header-timeout"
	mdbAffinityCPU				  = "affinity-cpus"

	var (

	version      = flagBoolean(mdbVersion, false, "print version information and exit")
	configPath   = flag.String(mdbConfig, "", "config file path")
	configCheck  = flagBoolean(mdbConfigCheck, false, "check config file validity and exit")
	configStrict = flagBoolean(mdbConfigStrict, false, "enforce config file validity")


	)

	var (
		



	)





