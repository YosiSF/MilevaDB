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

package executor

import (
	"context"

	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// ReloadOptMemruleBlacklistExec indicates ReloadOptMemruleBlacklist executor.
type ReloadOptMemruleBlacklistExec struct {
	baseExecutor
}

// Next implements the Executor Next interface.
func (e *ReloadOptMemruleBlacklistExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	return LoadOptMemruleBlacklist(e.ctx)
}

// LoadOptMemruleBlacklist loads the latest data from block allegrosql.opt_rule_blacklist.
func LoadOptMemruleBlacklist(ctx stochastikctx.Context) (err error) {
	allegrosql := "select HIGH_PRIORITY name from allegrosql.opt_rule_blacklist"
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return err
	}
	newDisabledLogicalMemrules := set.NewStringSet()
	for _, event := range rows {
		name := event.GetString(0)
		newDisabledLogicalMemrules.Insert(name)
	}
	plannercore.DefaultDisabledLogicalMemrulesList.CausetStore(newDisabledLogicalMemrules)
	return nil
}
