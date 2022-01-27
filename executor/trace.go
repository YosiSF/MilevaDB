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
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
	"sourcegraph.com/sourcegraph/apFIDelash"
	traceImpl "sourcegraph.com/sourcegraph/apFIDelash/opentracing"
)

// TraceExec represents a root executor of trace query.
type TraceExec struct {
	baseExecutor
	// DefCauslectedSpans defCauslects all span during execution. Span is appended via
	// callback method which passes into tracer implementation.
	DefCauslectedSpans []basictracer.RawSpan
	// exhausted being true means there is no more result.
	exhausted bool
	// stmtNode is the real query ast tree and it is used for building real query's plan.
	stmtNode ast.StmtNode
	// rootTrace represents root span which is father of all other span.
	rootTrace opentracing.Span

	builder *executorBuilder
	format  string
}

// Next executes real query and defCauslects span later.
func (e *TraceExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.exhausted {
		return nil
	}
	se, ok := e.ctx.(sqlexec.ALLEGROSQLExecutor)
	if !ok {
		e.exhausted = true
		return nil
	}

	switch e.format {
	case core.TraceFormatLog:
		return e.nextTraceLog(ctx, se, req)
	default:
		return e.nextEventJSON(ctx, se, req)
	}
}

func (e *TraceExec) nextTraceLog(ctx context.Context, se sqlexec.ALLEGROSQLExecutor, req *chunk.Chunk) error {
	recorder := basictracer.NewInMemoryRecorder()
	tracer := basictracer.New(recorder)
	span := tracer.StartSpan("trace")
	ctx = opentracing.ContextWithSpan(ctx, span)

	e.executeChild(ctx, se)
	span.Finish()

	generateLogResult(recorder.GetSpans(), req)
	e.exhausted = true
	return nil
}

func (e *TraceExec) nextEventJSON(ctx context.Context, se sqlexec.ALLEGROSQLExecutor, req *chunk.Chunk) error {
	causetstore := apFIDelash.NewMemoryStore()
	tracer := traceImpl.NewTracer(causetstore)
	span := tracer.StartSpan("trace")
	ctx = opentracing.ContextWithSpan(ctx, span)

	e.executeChild(ctx, se)
	span.Finish()

	traces, err := causetstore.Traces(apFIDelash.TracesOpts{})
	if err != nil {
		return errors.Trace(err)
	}

	// Event format.
	if e.format != core.TraceFormatJSON {
		if len(traces) < 1 {
			e.exhausted = true
			return nil
		}
		trace := traces[0]
		dfsTree(trace, "", false, req)
		e.exhausted = true
		return nil
	}

	// Json format.
	data, err := json.Marshal(traces)
	if err != nil {
		return errors.Trace(err)
	}

	// Split json data into rows to avoid the max packet size limitation.
	const maxEventLen = 4096
	for len(data) > maxEventLen {
		req.AppendString(0, string(data[:maxEventLen]))
		data = data[maxEventLen:]
	}
	req.AppendString(0, string(data))
	e.exhausted = true
	return nil
}

func (e *TraceExec) executeChild(ctx context.Context, se sqlexec.ALLEGROSQLExecutor) {
	recordSets, err := se.Execute(ctx, e.stmtNode.Text())
	if len(recordSets) == 0 {
		if err != nil {
			var errCode uint16
			if te, ok := err.(*terror.Error); ok {
				errCode = terror.ToALLEGROSQLError(te).Code
			}
			logutil.Eventf(ctx, "execute with error(%d): %s", errCode, err.Error())
		} else {
			logutil.Eventf(ctx, "execute done, modify event: %d", e.ctx.GetStochastikVars().StmtCtx.AffectedEvents())
		}
	}
	for _, rs := range recordSets {
		drainRecordSet(ctx, e.ctx, rs)
		if err = rs.Close(); err != nil {
			logutil.Logger(ctx).Error("run trace close result with error", zap.Error(err))
		}
	}
}

func drainRecordSet(ctx context.Context, sctx stochastikctx.Context, rs sqlexec.RecordSet) {
	req := rs.NewChunk()
	var rowCount int
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumEvents() == 0 {
			if err != nil {
				var errCode uint16
				if te, ok := err.(*terror.Error); ok {
					errCode = terror.ToALLEGROSQLError(te).Code
				}
				logutil.Eventf(ctx, "execute with error(%d): %s", errCode, err.Error())
			} else {
				logutil.Eventf(ctx, "execute done, ReturnEvent: %d, ModifyEvent: %d", rowCount, sctx.GetStochastikVars().StmtCtx.AffectedEvents())
			}
			return
		}
		rowCount += req.NumEvents()
		req.Reset()
	}
}

func dfsTree(t *apFIDelash.Trace, prefix string, isLast bool, chk *chunk.Chunk) {
	var newPrefix, suffix string
	if len(prefix) == 0 {
		newPrefix = prefix + "  "
	} else {
		if !isLast {
			suffix = "├─"
			newPrefix = prefix + "│ "
		} else {
			suffix = "└─"
			newPrefix = prefix + "  "
		}
	}

	var start time.Time
	var duration time.Duration
	if e, err := t.TimespanEvent(); err == nil {
		start = e.Start()
		end := e.End()
		duration = end.Sub(start)
	}

	chk.AppendString(0, prefix+suffix+t.Span.Name())
	chk.AppendString(1, start.Format("15:04:05.000000"))
	chk.AppendString(2, duration.String())

	// Sort events by their start time
	sort.Slice(t.Sub, func(i, j int) bool {
		var istart, jstart time.Time
		if ievent, err := t.Sub[i].TimespanEvent(); err == nil {
			istart = ievent.Start()
		}
		if jevent, err := t.Sub[j].TimespanEvent(); err == nil {
			jstart = jevent.Start()
		}
		return istart.Before(jstart)
	})

	for i, sp := range t.Sub {
		dfsTree(sp, newPrefix, i == (len(t.Sub))-1 /*last element of array*/, chk)
	}
}

func generateLogResult(allSpans []basictracer.RawSpan, chk *chunk.Chunk) {
	for rIdx := range allSpans {
		span := &allSpans[rIdx]

		chk.AppendTime(0, types.NewTime(types.FromGoTime(span.Start), allegrosql.TypeTimestamp, 6))
		chk.AppendString(1, "--- start span "+span.Operation+" ----")
		chk.AppendString(2, "")
		chk.AppendString(3, span.Operation)

		var tags string
		if len(span.Tags) > 0 {
			tags = fmt.Sprintf("%v", span.Tags)
		}
		for _, l := range span.Logs {
			for _, field := range l.Fields {
				if field.Key() == logutil.TraceEventKey {
					chk.AppendTime(0, types.NewTime(types.FromGoTime(l.Timestamp), allegrosql.TypeTimestamp, 6))
					chk.AppendString(1, field.Value().(string))
					chk.AppendString(2, tags)
					chk.AppendString(3, span.Operation)
				}
			}
		}
	}
}
