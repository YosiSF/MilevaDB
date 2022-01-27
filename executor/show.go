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
	"bytes"
	"context"
	gjson "encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb-tools/milevadb-binlog/node"
	"github.com/whtcorpsinc/milevadb-tools/pkg/etcd"
	"github.com/whtcorpsinc/milevadb-tools/pkg/utils"
	"github.com/whtcorpsinc/milevadb/bindinfo"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/petri"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/plugin"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/privilege/privileges"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/format"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

var etcdDialTimeout = 5 * time.Second

// ShowExec represents a show executor.
type ShowExec struct {
	baseExecutor

	Tp             ast.ShowStmtType // Databases/Blocks/DeferredCausets/....
	DBName         perceptron.CIStr
	Block          *ast.BlockName          // Used for showing defCausumns.
	DeferredCauset *ast.DeferredCausetName // Used for `desc block defCausumn`.
	IndexName      perceptron.CIStr        // Used for show block regions.
	Flag           int                     // Some flag parsed from allegrosql, such as FULL.
	Roles          []*auth.RoleIdentity    // Used for show grants.
	User           *auth.UserIdentity      // Used by show grants, show create user.

	is schemareplicant.SchemaReplicant

	result *chunk.Chunk
	cursor int

	Full              bool
	IfNotExists       bool // Used for `show create database if not exists`
	GlobalSINTERLOCKe bool // GlobalSINTERLOCKe is used by show variables
	Extended          bool // Used for `show extended defCausumns from ...`
}

// Next implements the Executor Next interface.
func (e *ShowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.result == nil {
		e.result = newFirstChunk(e)
		err := e.fetchAll(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(e.result)
		for defCausIdx := 0; defCausIdx < e.Schema().Len(); defCausIdx++ {
			retType := e.Schema().DeferredCausets[defCausIdx].RetType
			if !types.IsTypeVarchar(retType.Tp) {
				continue
			}
			for event := iter.Begin(); event != iter.End(); event = iter.Next() {
				if valLen := len(event.GetString(defCausIdx)); retType.Flen < valLen {
					retType.Flen = valLen
				}
			}
		}
	}
	if e.cursor >= e.result.NumEvents() {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), e.result.NumEvents()-e.cursor)
	req.Append(e.result, e.cursor, e.cursor+numCurBatch)
	e.cursor += numCurBatch
	return nil
}

func (e *ShowExec) fetchAll(ctx context.Context) error {
	switch e.Tp {
	case ast.ShowCharset:
		return e.fetchShowCharset()
	case ast.ShowDefCauslation:
		return e.fetchShowDefCauslation()
	case ast.ShowDeferredCausets:
		return e.fetchShowDeferredCausets(ctx)
	case ast.ShowConfig:
		return e.fetchShowClusterConfigs(ctx)
	case ast.ShowCreateBlock:
		return e.fetchShowCreateBlock()
	case ast.ShowCreateSequence:
		return e.fetchShowCreateSequence()
	case ast.ShowCreateUser:
		return e.fetchShowCreateUser()
	case ast.ShowCreateView:
		return e.fetchShowCreateView()
	case ast.ShowCreateDatabase:
		return e.fetchShowCreateDatabase()
	case ast.ShowDatabases:
		return e.fetchShowDatabases()
	case ast.ShowDrainerStatus:
		return e.fetchShowPumpOrDrainerStatus(node.DrainerNode)
	case ast.ShowEngines:
		return e.fetchShowEngines()
	case ast.ShowGrants:
		return e.fetchShowGrants()
	case ast.ShowIndex:
		return e.fetchShowIndex()
	case ast.ShowProcedureStatus:
		return e.fetchShowProcedureStatus()
	case ast.ShowPumpStatus:
		return e.fetchShowPumpOrDrainerStatus(node.PumpNode)
	case ast.ShowStatus:
		return e.fetchShowStatus()
	case ast.ShowBlocks:
		return e.fetchShowBlocks()
	case ast.ShowOpenBlocks:
		return e.fetchShowOpenBlocks()
	case ast.ShowBlockStatus:
		return e.fetchShowBlockStatus()
	case ast.ShowTriggers:
		return e.fetchShowTriggers()
	case ast.ShowVariables:
		return e.fetchShowVariables()
	case ast.ShowWarnings:
		return e.fetchShowWarnings(false)
	case ast.ShowErrors:
		return e.fetchShowWarnings(true)
	case ast.ShowProcessList:
		return e.fetchShowProcessList()
	case ast.ShowEvents:
		// empty result
	case ast.ShowStatsMeta:
		return e.fetchShowStatsMeta()
	case ast.ShowStatsHistograms:
		return e.fetchShowStatsHistogram()
	case ast.ShowStatsBuckets:
		return e.fetchShowStatsBuckets()
	case ast.ShowStatsHealthy:
		e.fetchShowStatsHealthy()
		return nil
	case ast.ShowPlugins:
		return e.fetchShowPlugins()
	case ast.ShowProfiles:
		// empty result
	case ast.ShowMasterStatus:
		return e.fetchShowMasterStatus()
	case ast.ShowPrivileges:
		return e.fetchShowPrivileges()
	case ast.ShowBindings:
		return e.fetchShowBind()
	case ast.ShowAnalyzeStatus:
		e.fetchShowAnalyzeStatus()
		return nil
	case ast.ShowRegions:
		return e.fetchShowBlockRegions()
	case ast.ShowBuiltins:
		return e.fetchShowBuiltins()
	case ast.ShowBackups:
		return e.fetchShowBRIE(ast.BRIEHoTTBackup)
	case ast.ShowRestores:
		return e.fetchShowBRIE(ast.BRIEHoTTRestore)
	}
	return nil
}

// visibleChecker checks if a stmt is visible for a certain user.
type visibleChecker struct {
	defaultDB string
	ctx       stochastikctx.Context
	is        schemareplicant.SchemaReplicant
	manager   privilege.Manager
	ok        bool
}

func (v *visibleChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch x := in.(type) {
	case *ast.BlockName:
		schemaReplicant := x.Schema.L
		if schemaReplicant == "" {
			schemaReplicant = v.defaultDB
		}
		if !v.is.BlockExists(perceptron.NewCIStr(schemaReplicant), x.Name) {
			return in, true
		}
		activeRoles := v.ctx.GetStochastikVars().ActiveRoles
		if v.manager != nil && !v.manager.RequestVerification(activeRoles, schemaReplicant, x.Name.L, "", allegrosql.SelectPriv) {
			v.ok = false
		}
		return in, true
	}
	return in, false
}

func (v *visibleChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (e *ShowExec) fetchShowBind() error {
	var bindRecords []*bindinfo.BindRecord
	if !e.GlobalSINTERLOCKe {
		handle := e.ctx.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
		bindRecords = handle.GetAllBindRecord()
	} else {
		bindRecords = petri.GetPetri(e.ctx).BindHandle().GetAllBindRecord()
	}
	berolinaAllegroSQL := berolinaAllegroSQL.New()
	for _, bindData := range bindRecords {
		for _, hint := range bindData.Bindings {
			stmt, err := berolinaAllegroSQL.ParseOneStmt(hint.BindALLEGROSQL, hint.Charset, hint.DefCauslation)
			if err != nil {
				return err
			}
			checker := visibleChecker{
				defaultDB: bindData.EDB,
				ctx:       e.ctx,
				is:        e.is,
				manager:   privilege.GetPrivilegeManager(e.ctx),
				ok:        true,
			}
			stmt.Accept(&checker)
			if !checker.ok {
				continue
			}
			e.appendEvent([]interface{}{
				bindData.OriginalALLEGROSQL,
				hint.BindALLEGROSQL,
				bindData.EDB,
				hint.Status,
				hint.CreateTime,
				hint.UFIDelateTime,
				hint.Charset,
				hint.DefCauslation,
				hint.Source,
			})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowEngines() error {
	allegrosql := `SELECT * FROM information_schema.engines`
	rows, _, err := e.ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)

	if err != nil {
		return errors.Trace(err)
	}
	for _, event := range rows {
		e.result.AppendEvent(event)
	}
	return nil
}

// moveSchemaReplicantToFront moves information_schema to the first, and the others are sorted in the origin ascending order.
func moveSchemaReplicantToFront(dbs []string) {
	if len(dbs) > 0 && strings.EqualFold(dbs[0], "INFORMATION_SCHEMA") {
		return
	}

	i := sort.SearchStrings(dbs, "INFORMATION_SCHEMA")
	if i < len(dbs) && strings.EqualFold(dbs[i], "INFORMATION_SCHEMA") {
		INTERLOCKy(dbs[1:i+1], dbs[0:i])
		dbs[0] = "INFORMATION_SCHEMA"
	}
}

func (e *ShowExec) fetchShowDatabases() error {
	dbs := e.is.AllSchemaNames()
	checker := privilege.GetPrivilegeManager(e.ctx)
	sort.Strings(dbs)
	// let information_schema be the first database
	moveSchemaReplicantToFront(dbs)
	for _, d := range dbs {
		if checker != nil && !checker.DBIsVisible(e.ctx.GetStochastikVars().ActiveRoles, d) {
			continue
		}
		e.appendEvent([]interface{}{
			d,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowProcessList() error {
	sm := e.ctx.GetStochastikManager()
	if sm == nil {
		return nil
	}

	loginUser, activeRoles := e.ctx.GetStochastikVars().User, e.ctx.GetStochastikVars().ActiveRoles
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(e.ctx); pm != nil {
		if pm.RequestVerification(activeRoles, "", "", "", allegrosql.ProcessPriv) {
			hasProcessPriv = true
		}
	}

	pl := sm.ShowProcessList()
	for _, pi := range pl {
		// If you have the PROCESS privilege, you can see all threads.
		// Otherwise, you can see only your own threads.
		if !hasProcessPriv && pi.User != loginUser.Username {
			continue
		}
		event := pi.ToEventForShow(e.Full)
		e.appendEvent(event)
	}
	return nil
}

func (e *ShowExec) fetchShowOpenBlocks() error {
	// MilevaDB has no concept like allegrosql's "block cache" and "open block"
	// For simplicity, we just return an empty result with the same structure as MyALLEGROSQL's SHOW OPEN TABLES
	return nil
}

func (e *ShowExec) fetchShowBlocks() error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetStochastikVars().User != nil {
		if !checker.DBIsVisible(e.ctx.GetStochastikVars().ActiveRoles, e.DBName.O) {
			return e.dbAccessDenied()
		}
	}
	if !e.is.SchemaExists(e.DBName) {
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}
	// sort for blocks
	blockNames := make([]string, 0, len(e.is.SchemaBlocks(e.DBName)))
	activeRoles := e.ctx.GetStochastikVars().ActiveRoles
	var blockTypes = make(map[string]string)
	for _, v := range e.is.SchemaBlocks(e.DBName) {
		// Test with allegrosql.AllPrivMask means any privilege would be OK.
		// TODO: Should consider defCausumn privileges, which also make a block visible.
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, v.Meta().Name.O, "", allegrosql.AllPrivMask) {
			continue
		}
		blockNames = append(blockNames, v.Meta().Name.O)
		if v.Meta().IsView() {
			blockTypes[v.Meta().Name.O] = "VIEW"
		} else if v.Meta().IsSequence() {
			blockTypes[v.Meta().Name.O] = "SEQUENCE"
		} else if soliton.IsSystemView(e.DBName.L) {
			blockTypes[v.Meta().Name.O] = "SYSTEM VIEW"
		} else {
			blockTypes[v.Meta().Name.O] = "BASE TABLE"
		}
	}
	sort.Strings(blockNames)
	for _, v := range blockNames {
		if e.Full {
			e.appendEvent([]interface{}{v, blockTypes[v]})
		} else {
			e.appendEvent([]interface{}{v})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowBlockStatus() error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetStochastikVars().User != nil {
		if !checker.DBIsVisible(e.ctx.GetStochastikVars().ActiveRoles, e.DBName.O) {
			return e.dbAccessDenied()
		}
	}
	if !e.is.SchemaExists(e.DBName) {
		return ErrBadDB.GenWithStackByArgs(e.DBName)
	}

	allegrosql := fmt.Sprintf(`SELECT
               block_name, engine, version, row_format, block_rows,
               avg_row_length, data_length, max_data_length, index_length,
               data_free, auto_increment, create_time, uFIDelate_time, check_time,
               block_defCauslation, IFNULL(checksum,''), create_options, block_comment
               FROM information_schema.blocks
	       WHERE block_schema='%s' ORDER BY block_name`, e.DBName)

	rows, _, err := e.ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQLWithSnapshot(allegrosql)

	if err != nil {
		return errors.Trace(err)
	}

	activeRoles := e.ctx.GetStochastikVars().ActiveRoles
	for _, event := range rows {
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, event.GetString(0), "", allegrosql.AllPrivMask) {
			continue
		}
		e.result.AppendEvent(event)

	}
	return nil
}

func (e *ShowExec) fetchShowDeferredCausets(ctx context.Context) error {
	tb, err := e.getBlock()

	if err != nil {
		return errors.Trace(err)
	}
	checker := privilege.GetPrivilegeManager(e.ctx)
	activeRoles := e.ctx.GetStochastikVars().ActiveRoles
	if checker != nil && e.ctx.GetStochastikVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", allegrosql.AllPrivMask) {
		return e.blockAccessDenied("SELECT", tb.Meta().Name.O)
	}

	var defcaus []*block.DeferredCauset
	// The optional EXTENDED keyword causes the output to include information about hidden defCausumns that MyALLEGROSQL uses internally and are not accessible by users.
	// See https://dev.allegrosql.com/doc/refman/8.0/en/show-defCausumns.html
	if e.Extended {
		defcaus = tb.DefCauss()
	} else {
		defcaus = tb.VisibleDefCauss()
	}
	if tb.Meta().IsView() {
		// Because view's underblock's defCausumn could change or recreate, so view's defCausumn type may change overtime.
		// To avoid this situation we need to generate a logical plan and extract current defCausumn types from Schema.
		planBuilder := plannercore.NewPlanBuilder(e.ctx, e.is, &hint.BlockHintProcessor{})
		viewLogicalPlan, err := planBuilder.BuildDataSourceFromView(ctx, e.DBName, tb.Meta())
		if err != nil {
			return err
		}
		viewSchema := viewLogicalPlan.Schema()
		viewOutputNames := viewLogicalPlan.OutputNames()
		for _, defCaus := range defcaus {
			idx := expression.FindFieldNameIdxByDefCausName(viewOutputNames, defCaus.Name.L)
			if idx >= 0 {
				defCaus.FieldType = *viewSchema.DeferredCausets[idx].GetType()
			}
		}
	}
	for _, defCaus := range defcaus {
		if e.DeferredCauset != nil && e.DeferredCauset.Name.L != defCaus.Name.L {
			continue
		}

		desc := block.NewDefCausDesc(defCaus)
		var defCausumnDefault interface{}
		if desc.DefaultValue != nil {
			// SHOW COLUMNS result expects string value
			defaultValStr := fmt.Sprintf("%v", desc.DefaultValue)
			// If defCausumn is timestamp, and default value is not current_timestamp, should convert the default value to the current stochastik time zone.
			if defCaus.Tp == allegrosql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr && !strings.HasPrefix(strings.ToUpper(defaultValStr), strings.ToUpper(ast.CurrentTimestamp)) {
				timeValue, err := block.GetDefCausDefaultValue(e.ctx, defCaus.ToInfo())
				if err != nil {
					return errors.Trace(err)
				}
				defaultValStr = timeValue.GetMysqlTime().String()
			}
			if defCaus.Tp == allegrosql.TypeBit {
				defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
				defCausumnDefault = defaultValBinaryLiteral.ToBitLiteralString(true)
			} else {
				defCausumnDefault = defaultValStr
			}
		}

		// The FULL keyword causes the output to include the defCausumn defCauslation and comments,
		// as well as the privileges you have for each defCausumn.
		if e.Full {
			e.appendEvent([]interface{}{
				desc.Field,
				desc.Type,
				desc.DefCauslation,
				desc.Null,
				desc.Key,
				defCausumnDefault,
				desc.Extra,
				desc.Privileges,
				desc.Comment,
			})
		} else {
			e.appendEvent([]interface{}{
				desc.Field,
				desc.Type,
				desc.Null,
				desc.Key,
				defCausumnDefault,
				desc.Extra,
			})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowIndex() error {
	tb, err := e.getBlock()
	if err != nil {
		return errors.Trace(err)
	}

	checker := privilege.GetPrivilegeManager(e.ctx)
	activeRoles := e.ctx.GetStochastikVars().ActiveRoles
	if checker != nil && e.ctx.GetStochastikVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", allegrosql.AllPrivMask) {
		return e.blockAccessDenied("SELECT", tb.Meta().Name.O)
	}

	if tb.Meta().PKIsHandle {
		var pkDefCaus *block.DeferredCauset
		for _, defCaus := range tb.DefCauss() {
			if allegrosql.HasPriKeyFlag(defCaus.Flag) {
				pkDefCaus = defCaus
				break
			}
		}
		e.appendEvent([]interface{}{
			tb.Meta().Name.O, // Block
			0,                // Non_unique
			"PRIMARY",        // Key_name
			1,                // Seq_in_index
			pkDefCaus.Name.O, // DeferredCauset_name
			"A",              // DefCauslation
			0,                // Cardinality
			nil,              // Sub_part
			nil,              // Packed
			"",               // Null
			"BTREE",          // Index_type
			"",               // Comment
			"",               // Index_comment
			"YES",            // Index_visible
			"NULL",           // Expression
		})
	}
	for _, idx := range tb.Indices() {
		idxInfo := idx.Meta()
		if idxInfo.State != perceptron.StatePublic {
			continue
		}
		for i, defCaus := range idxInfo.DeferredCausets {
			nonUniq := 1
			if idx.Meta().Unique {
				nonUniq = 0
			}

			var subPart interface{}
			if defCaus.Length != types.UnspecifiedLength {
				subPart = defCaus.Length
			}

			nullVal := "YES"
			if idx.Meta().Name.O == allegrosql.PrimaryKeyName {
				nullVal = ""
			}

			visible := "YES"
			if idx.Meta().Invisible {
				visible = "NO"
			}

			defCausName := defCaus.Name.O
			expression := "NULL"
			tblDefCaus := tb.Meta().DeferredCausets[defCaus.Offset]
			if tblDefCaus.Hidden {
				defCausName = "NULL"
				expression = fmt.Sprintf("(%s)", tblDefCaus.GeneratedExprString)
			}

			e.appendEvent([]interface{}{
				tb.Meta().Name.O,       // Block
				nonUniq,                // Non_unique
				idx.Meta().Name.O,      // Key_name
				i + 1,                  // Seq_in_index
				defCausName,            // DeferredCauset_name
				"A",                    // DefCauslation
				0,                      // Cardinality
				subPart,                // Sub_part
				nil,                    // Packed
				nullVal,                // Null
				idx.Meta().Tp.String(), // Index_type
				"",                     // Comment
				idx.Meta().Comment,     // Index_comment
				visible,                // Index_visible
				expression,             // Expression
			})
		}
	}
	return nil
}

// fetchShowCharset gets all charset information and fill them into e.rows.
// See http://dev.allegrosql.com/doc/refman/5.7/en/show-character-set.html
func (e *ShowExec) fetchShowCharset() error {
	descs := charset.GetSupportedCharsets()
	for _, desc := range descs {
		e.appendEvent([]interface{}{
			desc.Name,
			desc.Desc,
			desc.DefaultDefCauslation,
			desc.Maxlen,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowMasterStatus() error {
	tso := e.ctx.GetStochastikVars().TxnCtx.StartTS
	e.appendEvent([]interface{}{"milevadb-binlog", tso, "", "", ""})
	return nil
}

func (e *ShowExec) fetchShowVariables() (err error) {
	var (
		value          string
		ok             bool
		stochastikVars = e.ctx.GetStochastikVars()
		unreachedVars  = make([]string, 0, len(variable.SysVars))
	)
	for _, v := range variable.SysVars {
		if !e.GlobalSINTERLOCKe {
			// For a stochastik sINTERLOCKe variable,
			// 1. try to fetch value from StochastikVars.Systems;
			// 2. if this variable is stochastik-only, fetch value from SysVars
			//		otherwise, fetch the value from block `allegrosql.Global_Variables`.
			value, ok, err = variable.GetStochastikOnlySysVars(stochastikVars, v.Name)
		} else {
			// If the sINTERLOCKe of a system variable is SINTERLOCKeNone,
			// it's a read-only variable, so we return the default value of it.
			// Otherwise, we have to fetch the values from block `allegrosql.Global_Variables` for global variable names.
			value, ok, err = variable.GetSINTERLOCKeNoneSystemVar(v.Name)
		}
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			unreachedVars = append(unreachedVars, v.Name)
			continue
		}
		e.appendEvent([]interface{}{v.Name, value})
	}
	if len(unreachedVars) != 0 {
		systemVars, err := stochastikVars.GlobalVarsAccessor.GetAllSysVars()
		if err != nil {
			return errors.Trace(err)
		}
		for _, varName := range unreachedVars {
			varValue, ok := systemVars[varName]
			if !ok {
				varValue = variable.SysVars[varName].Value
			}
			e.appendEvent([]interface{}{varName, varValue})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowStatus() error {
	stochastikVars := e.ctx.GetStochastikVars()
	statusVars, err := variable.GetStatusVars(stochastikVars)
	if err != nil {
		return errors.Trace(err)
	}
	for status, v := range statusVars {
		if e.GlobalSINTERLOCKe && v.SINTERLOCKe == variable.SINTERLOCKeStochastik {
			continue
		}
		switch v.Value.(type) {
		case []interface{}, nil:
			v.Value = fmt.Sprintf("%v", v.Value)
		}
		value, err := types.ToString(v.Value)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendEvent([]interface{}{status, value})
	}
	return nil
}

func getDefaultDefCauslate(charsetName string) string {
	for _, c := range charset.GetSupportedCharsets() {
		if strings.EqualFold(c.Name, charsetName) {
			return c.DefaultDefCauslation
		}
	}
	return ""
}

// ConstructResultOfShowCreateBlock constructs the result for show create block.
func ConstructResultOfShowCreateBlock(ctx stochastikctx.Context, blockInfo *perceptron.BlockInfo, allocators autoid.SlabPredictors, buf *bytes.Buffer) (err error) {
	if blockInfo.IsView() {
		fetchShowCreateBlock4View(ctx, blockInfo, buf)
		return nil
	}
	if blockInfo.IsSequence() {
		ConstructResultOfShowCreateSequence(ctx, blockInfo, buf)
		return nil
	}

	tblCharset := blockInfo.Charset
	if len(tblCharset) == 0 {
		tblCharset = allegrosql.DefaultCharset
	}
	tblDefCauslate := blockInfo.DefCauslate
	// Set default defCauslate if defCauslate is not specified.
	if len(tblDefCauslate) == 0 {
		tblDefCauslate = getDefaultDefCauslate(tblCharset)
	}

	sqlMode := ctx.GetStochastikVars().ALLEGROSQLMode
	fmt.Fprintf(buf, "CREATE TABLE %s (\n", stringutil.Escape(blockInfo.Name.O, sqlMode))
	var pkDefCaus *perceptron.DeferredCausetInfo
	var hasAutoIncID bool
	needAddComma := false
	for i, defCaus := range blockInfo.DefCauss() {
		if defCaus.Hidden {
			continue
		}
		if needAddComma {
			buf.WriteString(",\n")
		}
		fmt.Fprintf(buf, "  %s %s", stringutil.Escape(defCaus.Name.O, sqlMode), defCaus.GetTypeDesc())
		if defCaus.Charset != "binary" {
			if defCaus.Charset != tblCharset {
				fmt.Fprintf(buf, " CHARACTER SET %s", defCaus.Charset)
			}
			if defCaus.DefCauslate != tblDefCauslate {
				fmt.Fprintf(buf, " COLLATE %s", defCaus.DefCauslate)
			} else {
				defdefCaus, err := charset.GetDefaultDefCauslation(defCaus.Charset)
				if err == nil && defdefCaus != defCaus.DefCauslate {
					fmt.Fprintf(buf, " COLLATE %s", defCaus.DefCauslate)
				}
			}
		}
		if defCaus.IsGenerated() {
			// It's a generated defCausumn.
			fmt.Fprintf(buf, " GENERATED ALWAYS AS (%s)", defCaus.GeneratedExprString)
			if defCaus.GeneratedStored {
				buf.WriteString(" STORED")
			} else {
				buf.WriteString(" VIRTUAL")
			}
		}
		if allegrosql.HasAutoIncrementFlag(defCaus.Flag) {
			hasAutoIncID = true
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if allegrosql.HasNotNullFlag(defCaus.Flag) {
				buf.WriteString(" NOT NULL")
			}
			// default values are not shown for generated defCausumns in MyALLEGROSQL
			if !allegrosql.HasNoDefaultValueFlag(defCaus.Flag) && !defCaus.IsGenerated() {
				defaultValue := defCaus.GetDefaultValue()
				switch defaultValue {
				case nil:
					if !allegrosql.HasNotNullFlag(defCaus.Flag) {
						if defCaus.Tp == allegrosql.TypeTimestamp {
							buf.WriteString(" NULL")
						}
						buf.WriteString(" DEFAULT NULL")
					}
				case "CURRENT_TIMESTAMP":
					buf.WriteString(" DEFAULT CURRENT_TIMESTAMP")
					if defCaus.Decimal > 0 {
						buf.WriteString(fmt.Sprintf("(%d)", defCaus.Decimal))
					}
				default:
					defaultValStr := fmt.Sprintf("%v", defaultValue)
					// If defCausumn is timestamp, and default value is not current_timestamp, should convert the default value to the current stochastik time zone.
					if defCaus.Tp == allegrosql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr {
						timeValue, err := block.GetDefCausDefaultValue(ctx, defCaus)
						if err != nil {
							return errors.Trace(err)
						}
						defaultValStr = timeValue.GetMysqlTime().String()
					}

					if defCaus.Tp == allegrosql.TypeBit {
						defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
						fmt.Fprintf(buf, " DEFAULT %s", defaultValBinaryLiteral.ToBitLiteralString(true))
					} else if types.IsTypeNumeric(defCaus.Tp) || defCaus.DefaultIsExpr {
						fmt.Fprintf(buf, " DEFAULT %s", format.OutputFormat(defaultValStr))
					} else {
						fmt.Fprintf(buf, " DEFAULT '%s'", format.OutputFormat(defaultValStr))
					}
				}
			}
			if allegrosql.HasOnUFIDelateNowFlag(defCaus.Flag) {
				buf.WriteString(" ON UFIDelATE CURRENT_TIMESTAMP")
				buf.WriteString(block.OptionalFsp(&defCaus.FieldType))
			}
		}
		if dbs.IsAutoRandomDeferredCausetID(blockInfo, defCaus.ID) {
			buf.WriteString(fmt.Sprintf(" /*T![auto_rand] AUTO_RANDOM(%d) */", blockInfo.AutoRandomBits))
		}
		if len(defCaus.Comment) > 0 {
			buf.WriteString(fmt.Sprintf(" COMMENT '%s'", format.OutputFormat(defCaus.Comment)))
		}
		if i != len(blockInfo.DefCauss())-1 {
			needAddComma = true
		}
		if blockInfo.PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.Flag) {
			pkDefCaus = defCaus
		}
	}

	if pkDefCaus != nil {
		// If PKIsHanle, pk info is not in tb.Indices(). We should handle it here.
		buf.WriteString(",\n")
		fmt.Fprintf(buf, "  PRIMARY KEY (%s)", stringutil.Escape(pkDefCaus.Name.O, sqlMode))
	}

	publicIndices := make([]*perceptron.IndexInfo, 0, len(blockInfo.Indices))
	for _, idx := range blockInfo.Indices {
		if idx.State == perceptron.StatePublic {
			publicIndices = append(publicIndices, idx)
		}
	}
	if len(publicIndices) > 0 {
		buf.WriteString(",\n")
	}

	for i, idxInfo := range publicIndices {
		if idxInfo.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idxInfo.Unique {
			fmt.Fprintf(buf, "  UNIQUE KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		} else {
			fmt.Fprintf(buf, "  KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		}

		defcaus := make([]string, 0, len(idxInfo.DeferredCausets))
		var defCausInfo string
		for _, c := range idxInfo.DeferredCausets {
			if blockInfo.DeferredCausets[c.Offset].Hidden {
				defCausInfo = fmt.Sprintf("(%s)", blockInfo.DeferredCausets[c.Offset].GeneratedExprString)
			} else {
				defCausInfo = stringutil.Escape(c.Name.O, sqlMode)
				if c.Length != types.UnspecifiedLength {
					defCausInfo = fmt.Sprintf("%s(%s)", defCausInfo, strconv.Itoa(c.Length))
				}
			}
			defcaus = append(defcaus, defCausInfo)
		}
		fmt.Fprintf(buf, "(%s)", strings.Join(defcaus, ","))
		if idxInfo.Invisible {
			fmt.Fprintf(buf, ` /*!80000 INVISIBLE */`)
		}
		if i != len(publicIndices)-1 {
			buf.WriteString(",\n")
		}
	}

	// Foreign Keys are supported by data dictionary even though
	// they are not enforced by DBS. This is still helpful to applications.
	for _, fk := range blockInfo.ForeignKeys {
		buf.WriteString(fmt.Sprintf(",\n  CONSTRAINT %s FOREIGN KEY ", stringutil.Escape(fk.Name.O, sqlMode)))
		defCausNames := make([]string, 0, len(fk.DefCauss))
		for _, defCaus := range fk.DefCauss {
			defCausNames = append(defCausNames, stringutil.Escape(defCaus.O, sqlMode))
		}
		buf.WriteString(fmt.Sprintf("(%s)", strings.Join(defCausNames, ",")))
		buf.WriteString(fmt.Sprintf(" REFERENCES %s ", stringutil.Escape(fk.RefBlock.O, sqlMode)))
		refDefCausNames := make([]string, 0, len(fk.DefCauss))
		for _, refDefCaus := range fk.RefDefCauss {
			refDefCausNames = append(refDefCausNames, stringutil.Escape(refDefCaus.O, sqlMode))
		}
		buf.WriteString(fmt.Sprintf("(%s)", strings.Join(refDefCausNames, ",")))
		if ast.ReferOptionType(fk.OnDelete) != 0 {
			buf.WriteString(fmt.Sprintf(" ON DELETE %s", ast.ReferOptionType(fk.OnDelete).String()))
		}
		if ast.ReferOptionType(fk.OnUFIDelate) != 0 {
			buf.WriteString(fmt.Sprintf(" ON UFIDelATE %s", ast.ReferOptionType(fk.OnUFIDelate).String()))
		}
	}

	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	// We need to explicitly set the default charset and defCauslation
	// to make it work on MyALLEGROSQL server which has default defCauslate utf8_general_ci.
	if len(tblDefCauslate) == 0 || tblDefCauslate == "binary" {
		// If we can not find default defCauslate for the given charset,
		// or the defCauslate is 'binary'(MyALLEGROSQL-5.7 compatibility, see #15633 for details),
		// do not show the defCauslate part.
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s", tblCharset)
	} else {
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s COLLATE=%s", tblCharset, tblDefCauslate)
	}

	// Displayed if the compression typed is set.
	if len(blockInfo.Compression) != 0 {
		fmt.Fprintf(buf, " COMPRESSION='%s'", blockInfo.Compression)
	}

	incrementSlabPredictor := allocators.Get(autoid.EventIDAllocType)
	if hasAutoIncID && incrementSlabPredictor != nil {
		autoIncID, err := incrementSlabPredictor.NextGlobalAutoID(blockInfo.ID)
		if err != nil {
			return errors.Trace(err)
		}

		// It's compatible with MyALLEGROSQL.
		if autoIncID > 1 {
			fmt.Fprintf(buf, " AUTO_INCREMENT=%d", autoIncID)
		}
	}

	if blockInfo.AutoIdCache != 0 {
		fmt.Fprintf(buf, " /*T![auto_id_cache] AUTO_ID_CACHE=%d */", blockInfo.AutoIdCache)
	}

	randomSlabPredictor := allocators.Get(autoid.AutoRandomType)
	if randomSlabPredictor != nil {
		autoRandID, err := randomSlabPredictor.NextGlobalAutoID(blockInfo.ID)
		if err != nil {
			return errors.Trace(err)
		}

		if autoRandID > 1 {
			fmt.Fprintf(buf, " /*T![auto_rand_base] AUTO_RANDOM_BASE=%d */", autoRandID)
		}
	}

	if blockInfo.ShardEventIDBits > 0 {
		fmt.Fprintf(buf, "/*!90000 SHARD_ROW_ID_BITS=%d ", blockInfo.ShardEventIDBits)
		if blockInfo.PreSplitRegions > 0 {
			fmt.Fprintf(buf, "PRE_SPLIT_REGIONS=%d ", blockInfo.PreSplitRegions)
		}
		buf.WriteString("*/")
	}

	if len(blockInfo.Comment) > 0 {
		fmt.Fprintf(buf, " COMMENT='%s'", format.OutputFormat(blockInfo.Comment))
	}
	// add partition info here.
	appendPartitionInfo(blockInfo.Partition, buf)
	return nil
}

// ConstructResultOfShowCreateSequence constructs the result for show create sequence.
func ConstructResultOfShowCreateSequence(ctx stochastikctx.Context, blockInfo *perceptron.BlockInfo, buf *bytes.Buffer) {
	sqlMode := ctx.GetStochastikVars().ALLEGROSQLMode
	fmt.Fprintf(buf, "CREATE SEQUENCE %s ", stringutil.Escape(blockInfo.Name.O, sqlMode))
	sequenceInfo := blockInfo.Sequence
	fmt.Fprintf(buf, "start with %d ", sequenceInfo.Start)
	fmt.Fprintf(buf, "minvalue %d ", sequenceInfo.MinValue)
	fmt.Fprintf(buf, "maxvalue %d ", sequenceInfo.MaxValue)
	fmt.Fprintf(buf, "increment by %d ", sequenceInfo.Increment)
	if sequenceInfo.Cache {
		fmt.Fprintf(buf, "cache %d ", sequenceInfo.CacheValue)
	} else {
		buf.WriteString("nocache ")
	}
	if sequenceInfo.Cycle {
		buf.WriteString("cycle ")
	} else {
		buf.WriteString("nocycle ")
	}
	buf.WriteString("ENGINE=InnoDB")
	if len(sequenceInfo.Comment) > 0 {
		fmt.Fprintf(buf, " COMMENT='%s'", format.OutputFormat(sequenceInfo.Comment))
	}
}

func (e *ShowExec) fetchShowCreateSequence() error {
	tbl, err := e.getBlock()
	if err != nil {
		return errors.Trace(err)
	}
	blockInfo := tbl.Meta()
	if !blockInfo.IsSequence() {
		return ErrWrongObject.GenWithStackByArgs(e.DBName.O, blockInfo.Name.O, "SEQUENCE")
	}
	var buf bytes.Buffer
	ConstructResultOfShowCreateSequence(e.ctx, blockInfo, &buf)
	e.appendEvent([]interface{}{blockInfo.Name.O, buf.String()})
	return nil
}

// TestShowClusterConfigKey is the key used to causetstore TestShowClusterConfigFunc.
var TestShowClusterConfigKey stringutil.StringerStr = "TestShowClusterConfigKey"

// TestShowClusterConfigFunc is used to test 'show config ...'.
type TestShowClusterConfigFunc func() ([][]types.Causet, error)

func (e *ShowExec) fetchShowClusterConfigs(ctx context.Context) error {
	emptySet := set.NewStringSet()
	var confItems [][]types.Causet
	var err error
	if f := e.ctx.Value(TestShowClusterConfigKey); f != nil {
		confItems, err = f.(TestShowClusterConfigFunc)()
	} else {
		confItems, err = fetchClusterConfig(e.ctx, emptySet, emptySet)
	}
	if err != nil {
		return err
	}
	for _, items := range confItems {
		event := make([]interface{}, 0, 4)
		for _, item := range items {
			event = append(event, item.GetString())
		}
		e.appendEvent(event)
	}
	return nil
}

func (e *ShowExec) fetchShowCreateBlock() error {
	tb, err := e.getBlock()
	if err != nil {
		return errors.Trace(err)
	}

	blockInfo := tb.Meta()
	var buf bytes.Buffer
	// TODO: let the result more like MyALLEGROSQL.
	if err = ConstructResultOfShowCreateBlock(e.ctx, blockInfo, tb.SlabPredictors(e.ctx), &buf); err != nil {
		return err
	}
	if blockInfo.IsView() {
		e.appendEvent([]interface{}{blockInfo.Name.O, buf.String(), blockInfo.Charset, blockInfo.DefCauslate})
		return nil
	}

	e.appendEvent([]interface{}{blockInfo.Name.O, buf.String()})
	return nil
}

func (e *ShowExec) fetchShowCreateView() error {
	EDB, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	tb, err := e.getBlock()
	if err != nil {
		return errors.Trace(err)
	}

	if !tb.Meta().IsView() {
		return ErrWrongObject.GenWithStackByArgs(EDB.Name.O, tb.Meta().Name.O, "VIEW")
	}

	var buf bytes.Buffer
	fetchShowCreateBlock4View(e.ctx, tb.Meta(), &buf)
	e.appendEvent([]interface{}{tb.Meta().Name.O, buf.String(), tb.Meta().Charset, tb.Meta().DefCauslate})
	return nil
}

func fetchShowCreateBlock4View(ctx stochastikctx.Context, tb *perceptron.BlockInfo, buf *bytes.Buffer) {
	sqlMode := ctx.GetStochastikVars().ALLEGROSQLMode

	fmt.Fprintf(buf, "CREATE ALGORITHM=%s ", tb.View.Algorithm.String())
	fmt.Fprintf(buf, "DEFINER=%s@%s ", stringutil.Escape(tb.View.Definer.Username, sqlMode), stringutil.Escape(tb.View.Definer.Hostname, sqlMode))
	fmt.Fprintf(buf, "ALLEGROALLEGROSQL SECURITY %s ", tb.View.Security.String())
	fmt.Fprintf(buf, "VIEW %s (", stringutil.Escape(tb.Name.O, sqlMode))
	for i, defCaus := range tb.DeferredCausets {
		fmt.Fprintf(buf, "%s", stringutil.Escape(defCaus.Name.O, sqlMode))
		if i < len(tb.DeferredCausets)-1 {
			fmt.Fprintf(buf, ", ")
		}
	}
	fmt.Fprintf(buf, ") AS %s", tb.View.SelectStmt)
}

func appendPartitionInfo(partitionInfo *perceptron.PartitionInfo, buf *bytes.Buffer) {
	if partitionInfo == nil {
		return
	}
	if partitionInfo.Type == perceptron.PartitionTypeHash {
		fmt.Fprintf(buf, "\nPARTITION BY HASH( %s )", partitionInfo.Expr)
		fmt.Fprintf(buf, "\nPARTITIONS %d", partitionInfo.Num)
		return
	}
	// this if statement takes care of range defCausumns case
	if partitionInfo.DeferredCausets != nil && partitionInfo.Type == perceptron.PartitionTypeRange {
		buf.WriteString("\nPARTITION BY RANGE COLUMNS(")
		for i, defCaus := range partitionInfo.DeferredCausets {
			buf.WriteString(defCaus.L)
			if i < len(partitionInfo.DeferredCausets)-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString(") (\n")
	} else {
		fmt.Fprintf(buf, "\nPARTITION BY %s ( %s ) (\n", partitionInfo.Type.String(), partitionInfo.Expr)
	}
	for i, def := range partitionInfo.Definitions {
		lessThans := strings.Join(def.LessThan, ",")
		fmt.Fprintf(buf, "  PARTITION `%s` VALUES LESS THAN (%s)", def.Name, lessThans)
		if i < len(partitionInfo.Definitions)-1 {
			buf.WriteString(",\n")
		} else {
			buf.WriteString("\n")
		}
	}
	buf.WriteString(")")
}

// ConstructResultOfShowCreateDatabase constructs the result for show create database.
func ConstructResultOfShowCreateDatabase(ctx stochastikctx.Context, dbInfo *perceptron.DBInfo, ifNotExists bool, buf *bytes.Buffer) (err error) {
	sqlMode := ctx.GetStochastikVars().ALLEGROSQLMode
	var ifNotExistsStr string
	if ifNotExists {
		ifNotExistsStr = "/*!32312 IF NOT EXISTS*/ "
	}
	fmt.Fprintf(buf, "CREATE DATABASE %s%s", ifNotExistsStr, stringutil.Escape(dbInfo.Name.O, sqlMode))
	if dbInfo.Charset != "" {
		fmt.Fprintf(buf, " /*!40100 DEFAULT CHARACTER SET %s ", dbInfo.Charset)
		defaultDefCauslate, err := charset.GetDefaultDefCauslation(dbInfo.Charset)
		if err != nil {
			return errors.Trace(err)
		}
		if dbInfo.DefCauslate != "" && dbInfo.DefCauslate != defaultDefCauslate {
			fmt.Fprintf(buf, "COLLATE %s ", dbInfo.DefCauslate)
		}
		fmt.Fprint(buf, "*/")
		return nil
	}
	if dbInfo.DefCauslate != "" {
		defCauslInfo, err := defCauslate.GetDefCauslationByName(dbInfo.DefCauslate)
		if err != nil {
			return errors.Trace(err)
		}
		fmt.Fprintf(buf, " /*!40100 DEFAULT CHARACTER SET %s ", defCauslInfo.CharsetName)
		if !defCauslInfo.IsDefault {
			fmt.Fprintf(buf, "COLLATE %s ", dbInfo.DefCauslate)
		}
		fmt.Fprint(buf, "*/")
		return nil
	}
	// MyALLEGROSQL 5.7 always show the charset info but MilevaDB may ignore it, which makes a slight difference. We keep this
	// behavior unchanged because it is trivial enough.
	return nil
}

// fetchShowCreateDatabase composes show create database result.
func (e *ShowExec) fetchShowCreateDatabase() error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetStochastikVars().User != nil {
		if !checker.DBIsVisible(e.ctx.GetStochastikVars().ActiveRoles, e.DBName.String()) {
			return e.dbAccessDenied()
		}
	}
	dbInfo, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	var buf bytes.Buffer
	err := ConstructResultOfShowCreateDatabase(e.ctx, dbInfo, e.IfNotExists, &buf)
	if err != nil {
		return err
	}
	e.appendEvent([]interface{}{dbInfo.Name.O, buf.String()})
	return nil
}

func (e *ShowExec) fetchShowDefCauslation() error {
	defCauslations := defCauslate.GetSupportedDefCauslations()
	for _, v := range defCauslations {
		isDefault := ""
		if v.IsDefault {
			isDefault = "Yes"
		}
		e.appendEvent([]interface{}{
			v.Name,
			v.CharsetName,
			v.ID,
			isDefault,
			"Yes",
			1,
		})
	}
	return nil
}

// fetchShowCreateUser composes show create create user result.
func (e *ShowExec) fetchShowCreateUser() error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}

	userName, hostName := e.User.Username, e.User.Hostname
	sessVars := e.ctx.GetStochastikVars()
	if e.User.CurrentUser {
		userName = sessVars.User.AuthUsername
		hostName = sessVars.User.AuthHostname
	} else {
		// Show create user requires the SELECT privilege on allegrosql.user.
		// Ref https://dev.allegrosql.com/doc/refman/5.7/en/show-create-user.html
		activeRoles := sessVars.ActiveRoles
		if !checker.RequestVerification(activeRoles, allegrosql.SystemDB, allegrosql.UserBlock, "", allegrosql.SelectPriv) {
			return e.blockAccessDenied("SELECT", allegrosql.UserBlock)
		}
	}

	allegrosql := fmt.Sprintf(`SELECT * FROM %s.%s WHERE User='%s' AND Host='%s';`,
		allegrosql.SystemDB, allegrosql.UserBlock, userName, hostName)
	rows, _, err := e.ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return ErrCannotUser.GenWithStackByArgs("SHOW CREATE USER",
			fmt.Sprintf("'%s'@'%s'", e.User.Username, e.User.Hostname))
	}
	allegrosql = fmt.Sprintf(`SELECT PRIV FROM %s.%s WHERE User='%s' AND Host='%s'`,
		allegrosql.SystemDB, allegrosql.GlobalPrivBlock, userName, hostName)
	rows, _, err = e.ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return errors.Trace(err)
	}
	require := "NONE"
	if len(rows) == 1 {
		privData := rows[0].GetString(0)
		var privValue privileges.GlobalPrivValue
		err = gjson.Unmarshal(replog.Slice(privData), &privValue)
		if err != nil {
			return errors.Trace(err)
		}
		require = privValue.RequireStr()
	}
	showStr := fmt.Sprintf("CREATE USER '%s'@'%s' IDENTIFIED WITH 'mysql_native_password' AS '%s' REQUIRE %s PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK",
		e.User.Username, e.User.Hostname, checker.GetEncodedPassword(e.User.Username, e.User.Hostname), require)
	e.appendEvent([]interface{}{showStr})
	return nil
}

func (e *ShowExec) fetchShowGrants() error {
	// Get checker
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	sessVars := e.ctx.GetStochastikVars()
	if !e.User.CurrentUser {
		userName := sessVars.User.AuthUsername
		hostName := sessVars.User.AuthHostname
		// Show grant user requires the SELECT privilege on allegrosql schemaReplicant.
		// Ref https://dev.allegrosql.com/doc/refman/8.0/en/show-grants.html
		if userName != e.User.Username || hostName != e.User.Hostname {
			activeRoles := sessVars.ActiveRoles
			if !checker.RequestVerification(activeRoles, allegrosql.SystemDB, "", "", allegrosql.SelectPriv) {
				return ErrDBaccessDenied.GenWithStackByArgs(userName, hostName, allegrosql.SystemDB)
			}
		}
	}
	for _, r := range e.Roles {
		if r.Hostname == "" {
			r.Hostname = "%"
		}
		if !checker.FindEdge(e.ctx, r, e.User) {
			return ErrRoleNotGranted.GenWithStackByArgs(r.String(), e.User.String())
		}
	}
	gs, err := checker.ShowGrants(e.ctx, e.User, e.Roles)
	if err != nil {
		return errors.Trace(err)
	}
	for _, g := range gs {
		e.appendEvent([]interface{}{g})
	}
	return nil
}

func (e *ShowExec) fetchShowPrivileges() error {
	e.appendEvent([]interface{}{"Alter", "Blocks", "To alter the block"})
	e.appendEvent([]interface{}{"Alter", "Blocks", "To alter the block"})
	e.appendEvent([]interface{}{"Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"})
	e.appendEvent([]interface{}{"Create", "Databases,Blocks,Indexes", "To create new databases and blocks"})
	e.appendEvent([]interface{}{"Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"})
	e.appendEvent([]interface{}{"Create temporary blocks", "Databases", "To use CREATE TEMPORARY TABLE"})
	e.appendEvent([]interface{}{"Create view", "Blocks", "To create new views"})
	e.appendEvent([]interface{}{"Create user", "Server Admin", "To create new users"})
	e.appendEvent([]interface{}{"Delete", "Blocks", "To delete existing rows"})
	e.appendEvent([]interface{}{"Drop", "Databases,Blocks", "To drop databases, blocks, and views"})
	e.appendEvent([]interface{}{"Event", "Server Admin", "To create, alter, drop and execute events"})
	e.appendEvent([]interface{}{"Execute", "Functions,Procedures", "To execute stored routines"})
	e.appendEvent([]interface{}{"File", "File access on server", "To read and write files on the server"})
	e.appendEvent([]interface{}{"Grant option", "Databases,Blocks,Functions,Procedures", "To give to other users those privileges you possess"})
	e.appendEvent([]interface{}{"Index", "Blocks", "To create or drop indexes"})
	e.appendEvent([]interface{}{"Insert", "Blocks", "To insert data into blocks"})
	e.appendEvent([]interface{}{"Lock blocks", "Databases", "To use LOCK TABLES (together with SELECT privilege)"})
	e.appendEvent([]interface{}{"Process", "Server Admin", "To view the plain text of currently executing queries"})
	e.appendEvent([]interface{}{"Proxy", "Server Admin", "To make proxy user possible"})
	e.appendEvent([]interface{}{"References", "Databases,Blocks", "To have references on blocks"})
	e.appendEvent([]interface{}{"Reload", "Server Admin", "To reload or refresh blocks, logs and privileges"})
	e.appendEvent([]interface{}{"Replication client", "Server Admin", "To ask where the slave or master servers are"})
	e.appendEvent([]interface{}{"Replication slave", "Server Admin", "To read binary log events from the master"})
	e.appendEvent([]interface{}{"Select", "Blocks", "To retrieve rows from block"})
	e.appendEvent([]interface{}{"Show databases", "Server Admin", "To see all databases with SHOW DATABASES"})
	e.appendEvent([]interface{}{"Show view", "Blocks", "To see views with SHOW CREATE VIEW"})
	e.appendEvent([]interface{}{"Shutdown", "Server Admin", "To shut down the server"})
	e.appendEvent([]interface{}{"Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."})
	e.appendEvent([]interface{}{"Trigger", "Blocks", "To use triggers"})
	e.appendEvent([]interface{}{"Create blockspace", "Server Admin", "To create/alter/drop blockspaces"})
	e.appendEvent([]interface{}{"UFIDelate", "Blocks", "To uFIDelate existing rows"})
	e.appendEvent([]interface{}{"Usage", "Server Admin", "No privileges - allow connect only"})
	return nil
}

func (e *ShowExec) fetchShowTriggers() error {
	return nil
}

func (e *ShowExec) fetchShowProcedureStatus() error {
	return nil
}

func (e *ShowExec) fetchShowPlugins() error {
	tiPlugins := plugin.GetAll()
	for _, ps := range tiPlugins {
		for _, p := range ps {
			e.appendEvent([]interface{}{p.Name, p.StateValue(), p.HoTT.String(), p.Path, p.License, strconv.Itoa(int(p.Version))})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowWarnings(errOnly bool) error {
	warns := e.ctx.GetStochastikVars().StmtCtx.GetWarnings()
	for _, w := range warns {
		if errOnly && w.Level != stmtctx.WarnLevelError {
			continue
		}
		warn := errors.Cause(w.Err)
		switch x := warn.(type) {
		case *terror.Error:
			sqlErr := terror.ToALLEGROSQLError(x)
			e.appendEvent([]interface{}{w.Level, int64(sqlErr.Code), sqlErr.Message})
		default:
			e.appendEvent([]interface{}{w.Level, int64(allegrosql.ErrUnknown), warn.Error()})
		}
	}
	return nil
}

// fetchShowPumpOrDrainerStatus gets status of all pumps or drainers and fill them into e.rows.
func (e *ShowExec) fetchShowPumpOrDrainerStatus(HoTT string) error {
	registry, err := createRegistry(config.GetGlobalConfig().Path)
	if err != nil {
		return errors.Trace(err)
	}

	nodes, _, err := registry.Nodes(context.Background(), node.NodePrefix[HoTT])
	if err != nil {
		return errors.Trace(err)
	}
	err = registry.Close()
	if err != nil {
		return errors.Trace(err)
	}

	for _, n := range nodes {
		if n.State == node.Offline {
			continue
		}
		e.appendEvent([]interface{}{n.NodeID, n.Addr, n.State, n.MaxCommitTS, utils.TSOToRoughTime(n.UFIDelateTS).Format(types.TimeFormat)})
	}

	return nil
}

// createRegistry returns an ectd registry
func createRegistry(urls string) (*node.EtcdRegistry, error) {
	ectdEndpoints, err := utils.ParseHostPortAddr(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(ectdEndpoints, etcdDialTimeout, node.DefaultRootPath, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return node.NewEtcdRegistry(cli, etcdDialTimeout), nil
}

func (e *ShowExec) getBlock() (block.Block, error) {
	if e.Block == nil {
		return nil, errors.New("block not found")
	}
	tb, ok := e.is.BlockByID(e.Block.BlockInfo.ID)
	if !ok {
		return nil, errors.Errorf("block %s not found", e.Block.Name)
	}
	return tb, nil
}

func (e *ShowExec) dbAccessDenied() error {
	user := e.ctx.GetStochastikVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return ErrDBaccessDenied.GenWithStackByArgs(u, h, e.DBName)
}

func (e *ShowExec) blockAccessDenied(access string, block string) error {
	user := e.ctx.GetStochastikVars().User
	u := user.Username
	h := user.Hostname
	if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
		u = user.AuthUsername
		h = user.AuthHostname
	}
	return ErrBlockaccessDenied.GenWithStackByArgs(access, u, h, block)
}

func (e *ShowExec) appendEvent(event []interface{}) {
	for i, defCaus := range event {
		if defCaus == nil {
			e.result.AppendNull(i)
			continue
		}
		switch x := defCaus.(type) {
		case nil:
			e.result.AppendNull(i)
		case int:
			e.result.AppendInt64(i, int64(x))
		case int64:
			e.result.AppendInt64(i, x)
		case uint64:
			e.result.AppendUint64(i, x)
		case float64:
			e.result.AppendFloat64(i, x)
		case float32:
			e.result.AppendFloat32(i, x)
		case string:
			e.result.AppendString(i, x)
		case []byte:
			e.result.AppendBytes(i, x)
		case types.BinaryLiteral:
			e.result.AppendBytes(i, x)
		case *types.MyDecimal:
			e.result.AppendMyDecimal(i, x)
		case types.Time:
			e.result.AppendTime(i, x)
		case json.BinaryJSON:
			e.result.AppendJSON(i, x)
		case types.Duration:
			e.result.AppendDuration(i, x)
		case types.Enum:
			e.result.AppendEnum(i, x)
		case types.Set:
			e.result.AppendSet(i, x)
		default:
			e.result.AppendNull(i)
		}
	}
}

func (e *ShowExec) fetchShowBlockRegions() error {
	causetstore := e.ctx.GetStore()
	einsteindbStore, ok := causetstore.(einsteindb.CausetStorage)
	if !ok {
		return nil
	}
	splitStore, ok := causetstore.(ekv.SplitblockStore)
	if !ok {
		return nil
	}

	tb, err := e.getBlock()
	if err != nil {
		return errors.Trace(err)
	}

	physicalIDs := []int64{}
	if pi := tb.Meta().GetPartitionInfo(); pi != nil {
		for _, name := range e.Block.PartitionNames {
			pid, err := blocks.FindPartitionByName(tb.Meta(), name.L)
			if err != nil {
				return err
			}
			physicalIDs = append(physicalIDs, pid)
		}
		if len(physicalIDs) == 0 {
			for _, p := range pi.Definitions {
				physicalIDs = append(physicalIDs, p.ID)
			}
		}
	} else {
		if len(e.Block.PartitionNames) != 0 {
			return plannercore.ErrPartitionClauseOnNonpartitioned
		}
		physicalIDs = append(physicalIDs, tb.Meta().ID)
	}

	// Get block regions from from fidel, not from regionCache, because the region cache maybe outdated.
	var regions []regionMeta
	if len(e.IndexName.L) != 0 {
		indexInfo := tb.Meta().FindIndexByName(e.IndexName.L)
		if indexInfo == nil {
			return plannercore.ErrKeyDoesNotExist.GenWithStackByArgs(e.IndexName, tb.Meta().Name)
		}
		regions, err = getBlockIndexRegions(indexInfo, physicalIDs, einsteindbStore, splitStore)
	} else {
		regions, err = getBlockRegions(tb, physicalIDs, einsteindbStore, splitStore)
	}

	if err != nil {
		return err
	}
	e.fillRegionsToChunk(regions)
	return nil
}

func getBlockRegions(tb block.Block, physicalIDs []int64, einsteindbStore einsteindb.CausetStorage, splitStore ekv.SplitblockStore) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(physicalIDs))
	uniqueRegionMap := make(map[uint64]struct{})
	for _, id := range physicalIDs {
		rs, err := getPhysicalBlockRegions(id, tb.Meta(), einsteindbStore, splitStore, uniqueRegionMap)
		if err != nil {
			return nil, err
		}
		regions = append(regions, rs...)
	}
	return regions, nil
}

func getBlockIndexRegions(indexInfo *perceptron.IndexInfo, physicalIDs []int64, einsteindbStore einsteindb.CausetStorage, splitStore ekv.SplitblockStore) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(physicalIDs))
	uniqueRegionMap := make(map[uint64]struct{})
	for _, id := range physicalIDs {
		rs, err := getPhysicalIndexRegions(id, indexInfo, einsteindbStore, splitStore, uniqueRegionMap)
		if err != nil {
			return nil, err
		}
		regions = append(regions, rs...)
	}
	return regions, nil
}

func (e *ShowExec) fillRegionsToChunk(regions []regionMeta) {
	for i := range regions {
		e.result.AppendUint64(0, regions[i].region.Id)
		e.result.AppendString(1, regions[i].start)
		e.result.AppendString(2, regions[i].end)
		e.result.AppendUint64(3, regions[i].leaderID)
		e.result.AppendUint64(4, regions[i].storeID)

		peers := ""
		for i, peer := range regions[i].region.Peers {
			if i > 0 {
				peers += ", "
			}
			peers += strconv.FormatUint(peer.Id, 10)
		}
		e.result.AppendString(5, peers)
		if regions[i].scattering {
			e.result.AppendInt64(6, 1)
		} else {
			e.result.AppendInt64(6, 0)
		}

		e.result.AppendInt64(7, regions[i].writtenBytes)
		e.result.AppendInt64(8, regions[i].readBytes)
		e.result.AppendInt64(9, regions[i].approximateSize)
		e.result.AppendInt64(10, regions[i].approximateKeys)
	}
}

func (e *ShowExec) fetchShowBuiltins() error {
	for _, f := range expression.GetBuiltinList() {
		e.appendEvent([]interface{}{f})
	}
	return nil
}
