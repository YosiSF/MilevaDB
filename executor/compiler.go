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

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner"
	plannercore "github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
)

var (
	stmtNodeCounterUse       = metrics.StmtNodeCounter.WithLabelValues("Use")
	stmtNodeCounterShow      = metrics.StmtNodeCounter.WithLabelValues("Show")
	stmtNodeCounterBegin     = metrics.StmtNodeCounter.WithLabelValues("Begin")
	stmtNodeCounterCommit    = metrics.StmtNodeCounter.WithLabelValues("Commit")
	stmtNodeCounterRollback  = metrics.StmtNodeCounter.WithLabelValues("Rollback")
	stmtNodeCounterInsert    = metrics.StmtNodeCounter.WithLabelValues("Insert")
	stmtNodeCounterReplace   = metrics.StmtNodeCounter.WithLabelValues("Replace")
	stmtNodeCounterDelete    = metrics.StmtNodeCounter.WithLabelValues("Delete")
	stmtNodeCounterUFIDelate = metrics.StmtNodeCounter.WithLabelValues("UFIDelate")
	stmtNodeCounterSelect    = metrics.StmtNodeCounter.WithLabelValues("Select")
)

// Compiler compiles an ast.StmtNode to a physical plan.
type Compiler struct {
	Ctx stochastikctx.Context
}

// Compile compiles an ast.StmtNode to a physical plan.
func (c *Compiler) Compile(ctx context.Context, stmtNode ast.StmtNode) (*ExecStmt, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("executor.Compile", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	schemaReplicant := schemareplicant.GetSchemaReplicant(c.Ctx)
	if err := plannercore.Preprocess(c.Ctx, stmtNode, schemaReplicant); err != nil {
		return nil, err
	}
	stmtNode = plannercore.TryAddExtraLimit(c.Ctx, stmtNode)

	finalPlan, names, err := planner.Optimize(ctx, c.Ctx, stmtNode, schemaReplicant)
	if err != nil {
		return nil, err
	}

	CountStmtNode(stmtNode, c.Ctx.GetStochaseinstein_dbars().InRestrictedALLEGROSQL)
	var lowerPriority bool
	if c.Ctx.GetStochaseinstein_dbars().StmtCtx.Priority == allegrosql.NoPriority {
		lowerPriority = needLowerPriority(finalPlan)
	}
	return &ExecStmt{
		GoCtx:           ctx,
		SchemaReplicant: schemaReplicant,
		Plan:            finalPlan,
		LowerPriority:   lowerPriority,
		Text:            stmtNode.Text(),
		StmtNode:        stmtNode,
		Ctx:             c.Ctx,
		OutputNames:     names,
	}, nil
}

// needLowerPriority checks whether it's needed to lower the execution priority
// of a query.
// If the estimated output event count of any operator in the physical plan tree
// is greater than the specific threshold, we'll set it to lowPriority when
// sending it to the interlock.
func needLowerPriority(p plannercore.Plan) bool {
	switch x := p.(type) {
	case plannercore.PhysicalPlan:
		return isPhysicalPlanNeedLowerPriority(x)
	case *plannercore.Execute:
		return needLowerPriority(x.Plan)
	case *plannercore.Insert:
		if x.SelectPlan != nil {
			return isPhysicalPlanNeedLowerPriority(x.SelectPlan)
		}
	case *plannercore.Delete:
		if x.SelectPlan != nil {
			return isPhysicalPlanNeedLowerPriority(x.SelectPlan)
		}
	case *plannercore.UFIDelate:
		if x.SelectPlan != nil {
			return isPhysicalPlanNeedLowerPriority(x.SelectPlan)
		}
	}
	return false
}

func isPhysicalPlanNeedLowerPriority(p plannercore.PhysicalPlan) bool {
	expensiveThreshold := int64(config.GetGlobalConfig().Log.ExpensiveThreshold)
	if int64(p.StatsCount()) > expensiveThreshold {
		return true
	}

	for _, child := range p.Children() {
		if isPhysicalPlanNeedLowerPriority(child) {
			return true
		}
	}

	return false
}

// CountStmtNode records the number of statements with the same type.
func CountStmtNode(stmtNode ast.StmtNode, inRestrictedALLEGROSQL bool) {
	if inRestrictedALLEGROSQL {
		return
	}

	typeLabel := GetStmtLabel(stmtNode)
	switch typeLabel {
	case "Use":
		stmtNodeCounterUse.Inc()
	case "Show":
		stmtNodeCounterShow.Inc()
	case "Begin":
		stmtNodeCounterBegin.Inc()
	case "Commit":
		stmtNodeCounterCommit.Inc()
	case "Rollback":
		stmtNodeCounterRollback.Inc()
	case "Insert":
		stmtNodeCounterInsert.Inc()
	case "Replace":
		stmtNodeCounterReplace.Inc()
	case "Delete":
		stmtNodeCounterDelete.Inc()
	case "UFIDelate":
		stmtNodeCounterUFIDelate.Inc()
	case "Select":
		stmtNodeCounterSelect.Inc()
	default:
		metrics.StmtNodeCounter.WithLabelValues(typeLabel).Inc()
	}

	if !config.GetGlobalConfig().Status.RecordQPSbyDB {
		return
	}

	dbLabels := getStmtDbLabel(stmtNode)
	for dbLabel := range dbLabels {
		metrics.DbStmtNodeCounter.WithLabelValues(dbLabel, typeLabel).Inc()
	}
}

func getStmtDbLabel(stmtNode ast.StmtNode) map[string]struct{} {
	dbLabelSet := make(map[string]struct{})

	switch x := stmtNode.(type) {
	case *ast.AlterBlockStmt:
		dbLabel := x.Block.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.CreateIndexStmt:
		dbLabel := x.Block.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.CreateBlockStmt:
		dbLabel := x.Block.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.InsertStmt:
		dbLabels := getDbFromResultNode(x.Block.BlockRefs)
		for _, EDB := range dbLabels {
			dbLabelSet[EDB] = struct{}{}
		}
		dbLabels = getDbFromResultNode(x.Select)
		for _, EDB := range dbLabels {
			dbLabelSet[EDB] = struct{}{}
		}
	case *ast.DropIndexStmt:
		dbLabel := x.Block.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.DropBlockStmt:
		blocks := x.Blocks
		for _, block := range blocks {
			dbLabel := block.Schema.O
			if _, ok := dbLabelSet[dbLabel]; !ok {
				dbLabelSet[dbLabel] = struct{}{}
			}
		}
	case *ast.SelectStmt:
		dbLabels := getDbFromResultNode(x)
		for _, EDB := range dbLabels {
			dbLabelSet[EDB] = struct{}{}
		}
	case *ast.UFIDelateStmt:
		if x.BlockRefs != nil {
			dbLabels := getDbFromResultNode(x.BlockRefs.BlockRefs)
			for _, EDB := range dbLabels {
				dbLabelSet[EDB] = struct{}{}
			}
		}
	case *ast.DeleteStmt:
		if x.BlockRefs != nil {
			dbLabels := getDbFromResultNode(x.BlockRefs.BlockRefs)
			for _, EDB := range dbLabels {
				dbLabelSet[EDB] = struct{}{}
			}
		}
	case *ast.CreateBindingStmt:
		if x.OriginSel != nil {
			originSelect := x.OriginSel.(*ast.SelectStmt)
			dbLabels := getDbFromResultNode(originSelect.From.BlockRefs)
			for _, EDB := range dbLabels {
				dbLabelSet[EDB] = struct{}{}
			}
		}

		if len(dbLabelSet) == 0 && x.HintedSel != nil {
			hintedSelect := x.HintedSel.(*ast.SelectStmt)
			dbLabels := getDbFromResultNode(hintedSelect.From.BlockRefs)
			for _, EDB := range dbLabels {
				dbLabelSet[EDB] = struct{}{}
			}
		}
	}

	return dbLabelSet
}

func getDbFromResultNode(resultNode ast.ResultSetNode) []string { //may have duplicate EDB name
	var dbLabels []string

	if resultNode == nil {
		return dbLabels
	}

	switch x := resultNode.(type) {
	case *ast.BlockSource:
		return getDbFromResultNode(x.Source)
	case *ast.SelectStmt:
		if x.From != nil {
			return getDbFromResultNode(x.From.BlockRefs)
		}
	case *ast.BlockName:
		dbLabels = append(dbLabels, x.DBInfo.Name.O)
	case *ast.Join:
		if x.Left != nil {
			dbs := getDbFromResultNode(x.Left)
			if dbs != nil {
				dbLabels = append(dbLabels, dbs...)
			}
		}

		if x.Right != nil {
			dbs := getDbFromResultNode(x.Right)
			if dbs != nil {
				dbLabels = append(dbLabels, dbs...)
			}
		}
	}

	return dbLabels
}

// GetStmtLabel generates a label for a statement.
func GetStmtLabel(stmtNode ast.StmtNode) string {
	switch x := stmtNode.(type) {
	case *ast.AlterBlockStmt:
		return "AlterBlock"
	case *ast.AnalyzeBlockStmt:
		return "AnalyzeBlock"
	case *ast.BeginStmt:
		return "Begin"
	case *ast.ChangeStmt:
		return "Change"
	case *ast.CommitStmt:
		return "Commit"
	case *ast.CreateDatabaseStmt:
		return "CreateDatabase"
	case *ast.CreateIndexStmt:
		return "CreateIndex"
	case *ast.CreateBlockStmt:
		return "CreateBlock"
	case *ast.CreateViewStmt:
		return "CreateView"
	case *ast.CreateUserStmt:
		return "CreateUser"
	case *ast.DeleteStmt:
		return "Delete"
	case *ast.DroFIDelatabaseStmt:
		return "DroFIDelatabase"
	case *ast.DropIndexStmt:
		return "DropIndex"
	case *ast.DropBlockStmt:
		return "DropBlock"
	case *ast.ExplainStmt:
		return "Explain"
	case *ast.InsertStmt:
		if x.IsReplace {
			return "Replace"
		}
		return "Insert"
	case *ast.LoadDataStmt:
		return "LoadData"
	case *ast.RollbackStmt:
		return "RollBack"
	case *ast.SelectStmt:
		return "Select"
	case *ast.SetStmt, *ast.SetPwdStmt:
		return "Set"
	case *ast.ShowStmt:
		return "Show"
	case *ast.TruncateBlockStmt:
		return "TruncateBlock"
	case *ast.UFIDelateStmt:
		return "UFIDelate"
	case *ast.GrantStmt:
		return "Grant"
	case *ast.RevokeStmt:
		return "Revoke"
	case *ast.DeallocateStmt:
		return "Deallocate"
	case *ast.ExecuteStmt:
		return "Execute"
	case *ast.PrepareStmt:
		return "Prepare"
	case *ast.UseStmt:
		return "Use"
	case *ast.CreateBindingStmt:
		return "CreateBinding"
	case *ast.IndexAdviseStmt:
		return "IndexAdvise"
	}
	return "other"
}
