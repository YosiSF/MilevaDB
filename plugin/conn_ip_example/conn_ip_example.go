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

package main

import (
	"context"
	"fmt"

	"github.com/whtcorpsinc/MilevaDB-Prod/plugin"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
)

// Validate implements MilevaDB plugin's Validate SPI.
func Validate(ctx context.Context, m *plugin.Manifest) error {
	fmt.Println("conn_ip_example validate called")
	return nil
}

// OnInit implements MilevaDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("conn_ip_example init called")
	fmt.Println("read cfg in init", manifest.SysVars["conn_ip_example_test_variable"].Value)
	return nil
}

// OnShutdown implements MilevaDB plugin's OnShutdown SPI.
func OnShutdown(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("conn_ip_examples hutdown called")
	return nil
}

// OnGeneralEvent implements MilevaDB Audit plugin's OnGeneralEvent SPI.
func OnGeneralEvent(ctx context.Context, sctx *variable.Stochaseinstein_dbars, event plugin.GeneralEvent, cmd string) {
	fmt.Println("conn_ip_example notifiy called")
	fmt.Println("variable test: ", variable.GetSysVar("conn_ip_example_test_variable").Value)
	fmt.Printf("new connection by %s\n", ctx.Value("ip"))
}

// OnConnectionEvent implements MilevaDB Audit plugin's OnConnectionEvent SPI.
func OnConnectionEvent(ctx context.Context, event plugin.ConnectionEvent, info *variable.ConnectionInfo) error {
	var reason string
	if r := ctx.Value(plugin.RejectReasonCtxValue{}); r != nil {
		reason = r.(string)
	}
	fmt.Println("conn_ip_example onConnect called")
	fmt.Printf("conenct event: %s, reason: %s\n", event, reason)
	return nil
}
