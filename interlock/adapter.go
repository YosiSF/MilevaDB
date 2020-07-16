package interlock



import (
"context"
"fmt"
"math"
"strconv"
"strings"
"sync/atomic"
"time"

)

// processinfoSetter is the interface use to set current running process info.
type processinfoSetter interface {
	SetProcessInfo(string, time.Time, byte, uint64)
}

// recordSet wraps an Interlock, implements sqlexec.RecordSet interface
type recordSet struct {
	fields     []*ast.ResultField
	interlock   Interlock
	stmt       *ExecStmt
	lastErr    error
	txnStartTS uint64
}

func (a *recordSet) Fields() []*ast.ResultField {
	if len(a.fields) == 0 {
		a.fields = colNames2ResultFields(a.Interlock.Schema(), a.stmt.OutputNames, a.stmt.Ctx.GetCausetNetVars().Currentnoedb)
	}
	return a.fields
}

func colNames2ResultFields(schema *expression.Schema, names []*types.FieldName, defaultnoedb string) []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, schema.Len())
	defaultnoedbCIStr := serial.NewCIStr(defaultnoedb)
	for i := 0; i < schema.Len(); i++ {
		noedbName := names[i].noedbName
		if noedbName.L == "" && names[i].TblName.L != "" {
			noedbName = defaultnoedbCIStr
		}
		origColName := names[i].OrigColName
		if origColName.L == "" {
			origColName = names[i].ColName
		}
		rf := &ast.ResultField{
			Column:       &serial.ColumnInfo{Name: origColName, FieldType: *schema.Columns[i].RetType},
			ColumnAsName: names[i].ColName,
			Table:        &serial.TableInfo{Name: names[i].OrigTblName},
			TableAsName:  names[i].TblName,
			noedbName:       noedbName,
		}
		// This is for compatibility.
		// See issue https://github.com/YosiSF/Milevanoedb/BerolinaSQL/issues/10513 .
		if len(rf.ColumnAsName.O) > mysql.MaxAliasIdentifierLen {
			rf.ColumnAsName.O = rf.ColumnAsName.O[:mysql.MaxAliasIdentifierLen]
		}
		// Usually the length of O equals the length of L.
		// Add this len judgement to avoid panic.
		if len(rf.ColumnAsName.L) > mysql.MaxAliasIdentifierLen {
			rf.ColumnAsName.L = rf.ColumnAsName.L[:mysql.MaxAliasIdentifierLen]
		}
		rfs = append(rfs, rf)
	}
	return rfs
}

// Next use uses recordSet's Interlock to get next available soliton for later usage.
// If soliton does not contain any rows, then we update last query found rows in CausetNet variable as current found rows.
// The reason we need update is that soliton with 0 rows indicating we already finished current query, we need prepare for
// next query.
// If stmt is not nil and soliton with some rows inside, we simply update last query found rows by the number of row in soliton.
func (a *recordSet) Next(ctx contextctx.contextctx, req *soliton.soliton) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", a.stmt.Text), zap.Stack("stack"))
	}()

	err = Next(ctx, a.Interlock, req)
	if err != nil {
		a.lastErr = err
		return err
	}
	numRows := req.NumRows()
	if numRows == 0 {
		if a.stmt != nil {
			a.stmt.Ctx.GetCausetNetVars().LastFoundRows = a.stmt.Ctx.GetCausetNetVars().StmtCtx.FoundRows()
		}
		return nil
	}
	if a.stmt != nil {
		a.stmt.Ctx.GetCausetNetVars().StmtCtx.AddFoundRows(uint64(numRows))
	}
	return nil
}

// Newsoliton create a soliton base on top-level Interlock's newFirstsoliton().
func (a *recordSet) Newsoliton() *soliton.soliton {
	return newFirstsoliton(a.Interlock)
}

func (a *recordSet) Close() error {
	err := a.Interlock.Close()
	a.stmt.CloseRecordSet(a.txnStartTS, a.lastErr)
	return err
}

// OnFetchReturned implements commandLifeCycle#OnFetchReturned
func (a *recordSet) OnFetchReturned() {
	a.stmt.LogSlowQuery(a.txnStartTS, a.lastErr == nil, true)
}

// ExecStmt implements the sqlexec.Statement interface, it builds a planner.Plan to an sqlexec.Statement.
type ExecStmt struct {
	// InfoSchema stores a reference to the schema information.
	InfoSchema infoschema.InfoSchema
	// Plan stores a reference to the final physical plan.
	Plan plannercore.Plan
	// Text represents the origin query text.
	Text string

	StmtNode ast.StmtNode

	Ctx causetnetctx.contextctx

	// LowerPriority represents whether to lower the execution priority of a query.
	LowerPriority     bool
	isPreparedStmt    bool
	isSelectForUpdate bool
	retryCount        uint

	// OutputNames will be set if using cached plan
	OutputNames []*types.FieldName
	PsStmt      *plannercore.CachedPrepareStmt
}

// PointGet short path for point exec directly from plan, keep only necessary steps
func (a *ExecStmt) PointGet(ctx contextctx.contextctx, is infoschema.InfoSchema) (*recordSet, error) {
	if span := opentracing.SpanFromcontextctx(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("ExecStmt.PointGet", opentracing.ChildOf(span.contextctx()))
		span1.Logekv("sql", a.OriginText())
		defer span1.Finish()
		ctx = opentracing.contextctxWithSpan(ctx, span1)
	}
	startTs := uint64(math.MaxUint64)
	err := a.Ctx.InitTxnWithStartTS(startTs)
	if err != nil {
		return nil, err
	}
	a.Ctx.GetCausetNetVars().StmtCtx.Priority = ekv.PriorityHigh

	// try to reuse point get Interlock
	if a.PsStmt.Interlock != nil {
		exec, ok := a.PsStmt.Interlock.(*PointGetInterlock)
		if !ok {
			logutil.Logger(ctx).Error("invalid Interlock type, not PointGetInterlock for point get path")
			a.PsStmt.Interlock = nil
		} else {
			// CachedPlan type is already checked in last step
			pointGetPlan := a.PsStmt.PreparedAst.CachedPlan.(*plannercore.PointGetPlan)
			exec.Init(pointGetPlan, startTs)
			a.PsStmt.Interlock = exec
		}
	}
	if a.PsStmt.Interlock == nil {
		b := newInterlockBuilder(a.Ctx, is)
		newInterlock := b.build(a.Plan)
		if b.err != nil {
			return nil, b.err
		}
		a.PsStmt.Interlock = newInterlock
	}
	stmtNodeCounterSelect.Inc()
	pointInterlock := a.PsStmt.Interlock.(*PointGetInterlock)
	if err = pointInterlock.Open(ctx); err != nil {
		terror.Call(pointInterlock.Close)
		return nil, err
	}
	return &recordSet{
		Interlock:   pointInterlock,
		stmt:       a,
		txnStartTS: startTs,
	}, nil
}

// OriginText returns original statement as a string.
func (a *ExecStmt) OriginText() string {
	return a.Text
}

// IsPrepared returns true if stmt is a prepare statement.
func (a *ExecStmt) IsPrepared() bool {
	return a.isPreparedStmt
}

// IsReadOnly returns true if a statement is read only.
// If current StmtNode is an ExecuteStmt, we can get its prepared stmt,
// then using ast.IsReadOnly function to determine a statement is read only or not.
func (a *ExecStmt) IsReadOnly(vars *variable.CausetNetVars) bool {
	if execStmt, ok := a.StmtNode.(*ast.ExecuteStmt); ok {
		s, err := getPreparedStmt(execStmt, vars)
		if err != nil {
			logutil.BgLogger().Error("getPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(s)
	}
	return ast.IsReadOnly(a.StmtNode)
}
