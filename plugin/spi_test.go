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

package plugin_test

import (
	"context"
	"testing"

	"github.com/whtcorpsinc/milevadb/plugin"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
)

func TestExportManifest(t *testing.T) {
	callRecorder := struct {
		OnInitCalled      bool
		NotifyEventCalled bool
	}{}
	manifest := &plugin.AuditManifest{
		Manifest: plugin.Manifest{
			HoTT:    plugin.Authentication,
			Name:    "test audit",
			Version: 1,
			OnInit: func(ctx context.Context, manifest *plugin.Manifest) error {
				callRecorder.OnInitCalled = true
				return nil
			},
		},
		OnGeneralEvent: func(ctx context.Context, sctx *variable.StochastikVars, event plugin.GeneralEvent, cmd string) {
			callRecorder.NotifyEventCalled = true
		},
	}
	exported := plugin.ExportManifest(manifest)
	exported.OnInit(context.Background(), exported)
	audit := plugin.DeclareAuditManifest(exported)
	audit.OnGeneralEvent(context.Background(), nil, plugin.Log, "QUERY")
	if !callRecorder.NotifyEventCalled || !callRecorder.OnInitCalled {
		t.Fatalf("export test failure")
	}
}
