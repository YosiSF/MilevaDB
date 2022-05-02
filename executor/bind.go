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

package executor

import (
	"context"

	"github.com/whtcorpsinc/MilevaDB-Prod/bindinfo"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/errors"
)

// ALLEGROSQLBindExec represents a bind executor.
type ALLEGROSQLBindExec struct {
	baseExecutor

	sqlBindOp           plannercore.ALLEGROSQLBindOpType
	normdOrigALLEGROSQL string
	bindALLEGROSQL      string
	charset             string
	defCauslation       string
	EDB                 string
	isGlobal            bool
	bindAst             ast.StmtNode
}

// Next implements the Executor Next interface.
func (e *ALLEGROSQLBindExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	switch e.sqlBindOp {
	case plannercore.OpALLEGROSQLBindCreate:
		return e.createALLEGROSQLBind()
	case plannercore.OpALLEGROSQLBindDrop:
		return e.dropALLEGROSQLBind()
	case plannercore.OpFlushBindings:
		return e.flushBindings()
	case plannercore.OpCaptureBindings:
		e.captureBindings()
	case plannercore.OpEvolveBindings:
		return e.evolveBindings()
	case plannercore.OpReloadBindings:
		return e.reloadBindings()
	default:
		return errors.Errorf("unsupported ALLEGROALLEGROSQL bind operation: %v", e.sqlBindOp)
	}
	return nil
}

func (e *ALLEGROSQLBindExec) dropALLEGROSQLBind() error {
	var bindInfo *bindinfo.Binding
	if e.bindALLEGROSQL != "" {
		bindInfo = &bindinfo.Binding{
			BindALLEGROSQL: e.bindALLEGROSQL,
			Charset:        e.charset,
			DefCauslation:  e.defCauslation,
		}
	}
	if !e.isGlobal {
		handle := e.ctx.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
		return handle.DropBindRecord(e.normdOrigALLEGROSQL, e.EDB, bindInfo)
	}
	return petri.GetPetri(e.ctx).BindHandle().DropBindRecord(e.normdOrigALLEGROSQL, e.EDB, bindInfo)
}

func (e *ALLEGROSQLBindExec) createALLEGROSQLBind() error {
	bindInfo := bindinfo.Binding{
		BindALLEGROSQL: e.bindALLEGROSQL,
		Charset:        e.charset,
		DefCauslation:  e.defCauslation,
		Status:         bindinfo.Using,
		Source:         bindinfo.Manual,
	}
	record := &bindinfo.BindRecord{
		OriginalALLEGROSQL: e.normdOrigALLEGROSQL,
		EDB:                e.EDB,
		Bindings:           []bindinfo.Binding{bindInfo},
	}
	if !e.isGlobal {
		handle := e.ctx.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
		return handle.CreateBindRecord(e.ctx, record)
	}
	return petri.GetPetri(e.ctx).BindHandle().CreateBindRecord(e.ctx, record)
}

func (e *ALLEGROSQLBindExec) flushBindings() error {
	return petri.GetPetri(e.ctx).BindHandle().FlushBindings()
}

func (e *ALLEGROSQLBindExec) captureBindings() {
	petri.GetPetri(e.ctx).BindHandle().CaptureBaselines()
}

func (e *ALLEGROSQLBindExec) evolveBindings() error {
	return petri.GetPetri(e.ctx).BindHandle().HandleEvolvePlanTask(e.ctx, true)
}

func (e *ALLEGROSQLBindExec) reloadBindings() error {
	return petri.GetPetri(e.ctx).BindHandle().ReloadBindings()
}
