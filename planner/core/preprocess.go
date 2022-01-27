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

package core

import (
	"fmt"
	"math"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/petriutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	driver "github.com/whtcorpsinc/milevadb/types/berolinaAllegroSQL_driver"
)

// PreprocessOpt presents optional parameters to `Preprocess` method.
type PreprocessOpt func(*preprocessor)

// InPrepare is a PreprocessOpt that indicates preprocess is executing under prepare statement.
func InPrepare(p *preprocessor) {
	p.flag |= inPrepare
}

// InTxnRetry is a PreprocessOpt that indicates preprocess is executing under transaction retry.
func InTxnRetry(p *preprocessor) {
	p.flag |= inTxnRetry
}

// TryAddExtraLimit trys to add an extra limit for SELECT or UNION statement when sql_select_limit is set.
func TryAddExtraLimit(ctx stochastikctx.Context, node ast.StmtNode) ast.StmtNode {
	if ctx.GetStochastikVars().SelectLimit == math.MaxUint64 || ctx.GetStochastikVars().InRestrictedALLEGROSQL {
		return node
	}
	if explain, ok := node.(*ast.ExplainStmt); ok {
		explain.Stmt = TryAddExtraLimit(ctx, explain.Stmt)
		return explain
	} else if sel, ok := node.(*ast.SelectStmt); ok {
		if sel.Limit != nil || sel.SelectIntoOpt != nil {
			return node
		}
		newSel := *sel
		newSel.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetStochastikVars().SelectLimit, "", ""),
		}
		return &newSel
	} else if setOprStmt, ok := node.(*ast.SetOprStmt); ok {
		if setOprStmt.Limit != nil {
			return node
		}
		newSetOpr := *setOprStmt
		newSetOpr.Limit = &ast.Limit{
			Count: ast.NewValueExpr(ctx.GetStochastikVars().SelectLimit, "", ""),
		}
		return &newSetOpr
	}
	return node
}

// Preprocess resolves block names of the node, and checks some statements validation.
func Preprocess(ctx stochastikctx.Context, node ast.Node, is schemareplicant.SchemaReplicant, preprocessOpt ...PreprocessOpt) error {
	v := preprocessor{is: is, ctx: ctx, blockAliasInJoin: make([]map[string]interface{}, 0)}
	for _, optFn := range preprocessOpt {
		optFn(&v)
	}
	node.Accept(&v)
	return errors.Trace(v.err)
}

type preprocessorFlag uint8

const (
	// inPrepare is set when visiting in prepare statement.
	inPrepare preprocessorFlag = 1 << iota
	// inTxnRetry is set when visiting in transaction retry.
	inTxnRetry
	// inCreateOrDropBlock is set when visiting create/drop block statement.
	inCreateOrDropBlock
	// parentIsJoin is set when visiting node's parent is join.
	parentIsJoin
	// inRepairBlock is set when visiting a repair block statement.
	inRepairBlock
	// inSequenceFunction is set when visiting a sequence function.
	// This flag indicates the blockName in these function should be checked as sequence object.
	inSequenceFunction
)

// preprocessor is an ast.Visitor that preprocess
// ast Nodes parsed from berolinaAllegroSQL.
type preprocessor struct {
	is   schemareplicant.SchemaReplicant
	ctx  stochastikctx.Context
	err  error
	flag preprocessorFlag

	// blockAliasInJoin is a stack that keeps the block alias names for joins.
	// len(blockAliasInJoin) may bigger than 1 because the left/right child of join may be subquery that contains `JOIN`
	blockAliasInJoin []map[string]interface{}
}

func (p *preprocessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.CreateBlockStmt:
		p.flag |= inCreateOrDropBlock
		p.resolveCreateBlockStmt(node)
		p.checkCreateBlockGrammar(node)
	case *ast.CreateViewStmt:
		p.flag |= inCreateOrDropBlock
		p.checkCreateViewGrammar(node)
		p.checkCreateViewWithSelectGrammar(node)
	case *ast.DropBlockStmt:
		p.flag |= inCreateOrDropBlock
		p.checkDropBlockGrammar(node)
	case *ast.RenameBlockStmt:
		p.flag |= inCreateOrDropBlock
		p.checkRenameBlockGrammar(node)
	case *ast.CreateIndexStmt:
		p.checkCreateIndexGrammar(node)
	case *ast.AlterBlockStmt:
		p.resolveAlterBlockStmt(node)
		p.checkAlterBlockGrammar(node)
	case *ast.CreateDatabaseStmt:
		p.checkCreateDatabaseGrammar(node)
	case *ast.AlterDatabaseStmt:
		p.checkAlterDatabaseGrammar(node)
	case *ast.DroFIDelatabaseStmt:
		p.checkDroFIDelatabaseGrammar(node)
	case *ast.ShowStmt:
		p.resolveShowStmt(node)
	case *ast.SetOprSelectList:
		p.checkSetOprSelectList(node)
	case *ast.DeleteBlockList:
		return in, true
	case *ast.Join:
		p.checkNonUniqBlockAlias(node)
	case *ast.CreateBindingStmt:
		EraseLastSemicolon(node.OriginSel)
		EraseLastSemicolon(node.HintedSel)
		p.checkBindGrammar(node.OriginSel, node.HintedSel)
		return in, true
	case *ast.DropBindingStmt:
		EraseLastSemicolon(node.OriginSel)
		if node.HintedSel != nil {
			EraseLastSemicolon(node.HintedSel)
			p.checkBindGrammar(node.OriginSel, node.HintedSel)
		}
		return in, true
	case *ast.RecoverBlockStmt, *ast.FlashBackBlockStmt:
		// The specified block in recover block statement maybe already been dropped.
		// So skip check block name here, otherwise, recover block [block_name] syntax will return
		// block not exists error. But recover block statement is use to recover the dropped block. So skip children here.
		return in, true
	case *ast.RepairBlockStmt:
		// The RepairBlock should consist of the logic for creating blocks and renaming blocks.
		p.flag |= inRepairBlock
		p.checkRepairBlockGrammar(node)
	case *ast.CreateSequenceStmt:
		p.flag |= inCreateOrDropBlock
		p.resolveCreateSequenceStmt(node)
	case *ast.DropSequenceStmt:
		p.flag |= inCreateOrDropBlock
		p.checkDropSequenceGrammar(node)
	case *ast.FuncCallExpr:
		if node.FnName.L == ast.NextVal || node.FnName.L == ast.LastVal || node.FnName.L == ast.SetVal {
			p.flag |= inSequenceFunction
		}
	case *ast.BRIEStmt:
		if node.HoTT == ast.BRIEHoTTRestore {
			p.flag |= inCreateOrDropBlock
		}
	case *ast.CreateStatisticsStmt, *ast.DropStatisticsStmt:
		p.checkStatisticsOpGrammar(in)
	default:
		p.flag &= ^parentIsJoin
	}
	return in, p.err != nil
}

// EraseLastSemicolon removes last semicolon of allegrosql.
func EraseLastSemicolon(stmt ast.StmtNode) {
	allegrosql := stmt.Text()
	if len(allegrosql) > 0 && allegrosql[len(allegrosql)-1] == ';' {
		stmt.SetText(allegrosql[:len(allegrosql)-1])
	}
}

func (p *preprocessor) checkBindGrammar(originSel, hintedSel ast.StmtNode) {
	originALLEGROSQL := berolinaAllegroSQL.Normalize(originSel.(*ast.SelectStmt).Text())
	hintedALLEGROSQL := berolinaAllegroSQL.Normalize(hintedSel.(*ast.SelectStmt).Text())

	if originALLEGROSQL != hintedALLEGROSQL {
		p.err = errors.Errorf("hinted allegrosql and origin allegrosql don't match when hinted allegrosql erase the hint info, after erase hint info, originALLEGROSQL:%s, hintedALLEGROSQL:%s", originALLEGROSQL, hintedALLEGROSQL)
	}
}

func (p *preprocessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.CreateBlockStmt:
		p.flag &= ^inCreateOrDropBlock
		p.checkAutoIncrement(x)
		p.checkContainDotDeferredCauset(x)
	case *ast.CreateViewStmt:
		p.flag &= ^inCreateOrDropBlock
	case *ast.DropBlockStmt, *ast.AlterBlockStmt, *ast.RenameBlockStmt:
		p.flag &= ^inCreateOrDropBlock
	case *driver.ParamMarkerExpr:
		if p.flag&inPrepare == 0 {
			p.err = berolinaAllegroSQL.ErrSyntax.GenWithStack("syntax error, unexpected '?'")
			return
		}
	case *ast.ExplainStmt:
		if _, ok := x.Stmt.(*ast.ShowStmt); ok {
			break
		}
		valid := false
		for i, length := 0, len(ast.ExplainFormats); i < length; i++ {
			if strings.ToLower(x.Format) == ast.ExplainFormats[i] {
				valid = true
				break
			}
		}
		if !valid {
			p.err = ErrUnknownExplainFormat.GenWithStackByArgs(x.Format)
		}
	case *ast.BlockName:
		p.handleBlockName(x)
	case *ast.Join:
		if len(p.blockAliasInJoin) > 0 {
			p.blockAliasInJoin = p.blockAliasInJoin[:len(p.blockAliasInJoin)-1]
		}
	case *ast.FuncCallExpr:
		// The arguments for builtin NAME_CONST should be constants
		// See https://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
		if x.FnName.L == ast.NameConst {
			if len(x.Args) != 2 {
				p.err = expression.ErrIncorrectParameterCount.GenWithStackByArgs(x.FnName.L)
			} else {
				_, isValueExpr1 := x.Args[0].(*driver.ValueExpr)
				isValueExpr2 := false
				switch x.Args[1].(type) {
				case *driver.ValueExpr, *ast.UnaryOperationExpr:
					isValueExpr2 = true
				}

				if !isValueExpr1 || !isValueExpr2 {
					p.err = ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
				}
			}
			break
		}

		// no need sleep when retry transaction and avoid unexpect sleep caused by retry.
		if p.flag&inTxnRetry > 0 && x.FnName.L == ast.Sleep {
			if len(x.Args) == 1 {
				x.Args[0] = ast.NewValueExpr(0, "", "")
			}
		}

		if x.FnName.L == ast.NextVal || x.FnName.L == ast.LastVal || x.FnName.L == ast.SetVal {
			p.flag &= ^inSequenceFunction
		}
	case *ast.RepairBlockStmt:
		p.flag &= ^inRepairBlock
	case *ast.CreateSequenceStmt:
		p.flag &= ^inCreateOrDropBlock
	case *ast.BRIEStmt:
		if x.HoTT == ast.BRIEHoTTRestore {
			p.flag &= ^inCreateOrDropBlock
		}
	}

	return in, p.err == nil
}

func checkAutoIncrementOp(colDef *ast.DeferredCausetDef, index int) (bool, error) {
	var hasAutoIncrement bool

	if colDef.Options[index].Tp == ast.DeferredCausetOptionAutoIncrement {
		hasAutoIncrement = true
		if len(colDef.Options) == index+1 {
			return hasAutoIncrement, nil
		}
		for _, op := range colDef.Options[index+1:] {
			if op.Tp == ast.DeferredCausetOptionDefaultValue {
				if tmp, ok := op.Expr.(*driver.ValueExpr); ok {
					if !tmp.Causet.IsNull() {
						return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
					}
				}
			}
		}
	}
	if colDef.Options[index].Tp == ast.DeferredCausetOptionDefaultValue && len(colDef.Options) != index+1 {
		if tmp, ok := colDef.Options[index].Expr.(*driver.ValueExpr); ok {
			if tmp.Causet.IsNull() {
				return hasAutoIncrement, nil
			}
		}
		for _, op := range colDef.Options[index+1:] {
			if op.Tp == ast.DeferredCausetOptionAutoIncrement {
				return hasAutoIncrement, errors.Errorf("Invalid default value for '%s'", colDef.Name.Name.O)
			}
		}
	}

	return hasAutoIncrement, nil
}

func isConstraintKeyTp(constraints []*ast.Constraint, colDef *ast.DeferredCausetDef) bool {
	for _, c := range constraints {
		if c.Keys[0].Expr != nil {
			continue
		}
		// If the constraint as follows: primary key(c1, c2)
		// we only support c1 column can be auto_increment.
		if colDef.Name.Name.L != c.Keys[0].DeferredCauset.Name.L {
			continue
		}
		switch c.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex,
			ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
			return true
		}
	}

	return false
}

func (p *preprocessor) checkAutoIncrement(stmt *ast.CreateBlockStmt) {
	autoIncrementDefCauss := make(map[*ast.DeferredCausetDef]bool)

	for _, colDef := range stmt.DefCauss {
		var hasAutoIncrement bool
		var isKey bool
		for i, op := range colDef.Options {
			ok, err := checkAutoIncrementOp(colDef, i)
			if err != nil {
				p.err = err
				return
			}
			if ok {
				hasAutoIncrement = true
			}
			switch op.Tp {
			case ast.DeferredCausetOptionPrimaryKey, ast.DeferredCausetOptionUniqKey:
				isKey = true
			}
		}
		if hasAutoIncrement {
			autoIncrementDefCauss[colDef] = isKey
		}
	}

	if len(autoIncrementDefCauss) < 1 {
		return
	}
	if len(autoIncrementDefCauss) > 1 {
		p.err = autoid.ErrWrongAutoKey.GenWithStackByArgs()
		return
	}
	// Only have one auto_increment col.
	for col, isKey := range autoIncrementDefCauss {
		if !isKey {
			isKey = isConstraintKeyTp(stmt.Constraints, col)
		}
		autoIncrementMustBeKey := true
		for _, opt := range stmt.Options {
			if opt.Tp == ast.BlockOptionEngine && strings.EqualFold(opt.StrValue, "MyISAM") {
				autoIncrementMustBeKey = false
			}
		}
		if autoIncrementMustBeKey && !isKey {
			p.err = autoid.ErrWrongAutoKey.GenWithStackByArgs()
		}
		switch col.Tp.Tp {
		case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeLong,
			allegrosql.TypeFloat, allegrosql.TypeDouble, allegrosql.TypeLonglong, allegrosql.TypeInt24:
		default:
			p.err = errors.Errorf("Incorrect column specifier for column '%s'", col.Name.Name.O)
		}
	}

}

// checkSetOprSelectList checks union's selectList.
// refer: https://dev.allegrosql.com/doc/refman/5.7/en/union.html
//        https://mariadb.com/kb/en/intersect/
//        https://mariadb.com/kb/en/except/
// "To apply ORDER BY or LIMIT to an individual SELECT, place the clause inside the parentheses that enclose the SELECT."
func (p *preprocessor) checkSetOprSelectList(stmt *ast.SetOprSelectList) {
	for _, sel := range stmt.Selects[:len(stmt.Selects)-1] {
		if sel.IsInBraces {
			continue
		}
		if sel.Limit != nil {
			p.err = ErrWrongUsage.GenWithStackByArgs("UNION", "LIMIT")
			return
		}
		if sel.OrderBy != nil {
			p.err = ErrWrongUsage.GenWithStackByArgs("UNION", "ORDER BY")
			return
		}
	}
}

func (p *preprocessor) checkCreateDatabaseGrammar(stmt *ast.CreateDatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		p.err = dbs.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkAlterDatabaseGrammar(stmt *ast.AlterDatabaseStmt) {
	// for 'ALTER DATABASE' statement, database name can be empty to alter default database.
	if isIncorrectName(stmt.Name) && !stmt.AlterDefaultDatabase {
		p.err = dbs.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkDroFIDelatabaseGrammar(stmt *ast.DroFIDelatabaseStmt) {
	if isIncorrectName(stmt.Name) {
		p.err = dbs.ErrWrongDBName.GenWithStackByArgs(stmt.Name)
	}
}

func (p *preprocessor) checkCreateBlockGrammar(stmt *ast.CreateBlockStmt) {
	tName := stmt.Block.Name.String()
	if isIncorrectName(tName) {
		p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(tName)
		return
	}
	countPrimaryKey := 0
	for _, colDef := range stmt.DefCauss {
		if err := checkDeferredCauset(colDef); err != nil {
			p.err = err
			return
		}
		isPrimary, err := checkDeferredCausetOptions(colDef.Options)
		if err != nil {
			p.err = err
			return
		}
		countPrimaryKey += isPrimary
		if countPrimaryKey > 1 {
			p.err = schemareplicant.ErrMultiplePriKey
			return
		}
	}
	for _, constraint := range stmt.Constraints {
		switch tp := constraint.Tp; tp {
		case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			err := checHoTTexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				p.err = err
				return
			}
		case ast.ConstraintPrimaryKey:
			if countPrimaryKey > 0 {
				p.err = schemareplicant.ErrMultiplePriKey
				return
			}
			countPrimaryKey++
			err := checHoTTexInfo(constraint.Name, constraint.Keys)
			if err != nil {
				p.err = err
				return
			}
		}
	}
	if p.err = checkUnsupportedBlockOptions(stmt.Options); p.err != nil {
		return
	}
	if stmt.Select != nil {
		// FIXME: a temp error noticing 'not implemented' (issue 4754)
		p.err = errors.New("'CREATE TABLE ... SELECT' is not implemented yet")
		return
	} else if len(stmt.DefCauss) == 0 && stmt.ReferBlock == nil {
		p.err = dbs.ErrBlockMustHaveDeferredCausets
		return
	}
}

func (p *preprocessor) checkCreateViewGrammar(stmt *ast.CreateViewStmt) {
	vName := stmt.ViewName.Name.String()
	if isIncorrectName(vName) {
		p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(vName)
		return
	}
	for _, col := range stmt.DefCauss {
		if isIncorrectName(col.String()) {
			p.err = dbs.ErrWrongDeferredCausetName.GenWithStackByArgs(col)
			return
		}
	}
}

func (p *preprocessor) checkCreateViewWithSelect(stmt *ast.SelectStmt) {
	if stmt.SelectIntoOpt != nil {
		p.err = dbs.ErrViewSelectClause.GenWithStackByArgs("INFO")
		return
	}
	if stmt.LockInfo != nil && stmt.LockInfo.LockType != ast.SelectLockNone {
		stmt.LockInfo.LockType = ast.SelectLockNone
		return
	}
}

func (p *preprocessor) checkCreateViewWithSelectGrammar(stmt *ast.CreateViewStmt) {
	switch stmt := stmt.Select.(type) {
	case *ast.SelectStmt:
		p.checkCreateViewWithSelect(stmt)
	case *ast.SetOprStmt:
		for _, selectStmt := range stmt.SelectList.Selects {
			p.checkCreateViewWithSelect(selectStmt)
			if p.err != nil {
				return
			}
		}
	}
}

func (p *preprocessor) checkDropSequenceGrammar(stmt *ast.DropSequenceStmt) {
	p.checkDropBlockNames(stmt.Sequences)
}

func (p *preprocessor) checkDropBlockGrammar(stmt *ast.DropBlockStmt) {
	p.checkDropBlockNames(stmt.Blocks)
}

func (p *preprocessor) checkDropBlockNames(blocks []*ast.BlockName) {
	for _, t := range blocks {
		if isIncorrectName(t.Name.String()) {
			p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(t.Name.String())
			return
		}
	}
}

func (p *preprocessor) checkNonUniqBlockAlias(stmt *ast.Join) {
	if p.flag&parentIsJoin == 0 {
		p.blockAliasInJoin = append(p.blockAliasInJoin, make(map[string]interface{}))
	}
	blockAliases := p.blockAliasInJoin[len(p.blockAliasInJoin)-1]
	if err := isBlockAliasDuplicate(stmt.Left, blockAliases); err != nil {
		p.err = err
		return
	}
	if err := isBlockAliasDuplicate(stmt.Right, blockAliases); err != nil {
		p.err = err
		return
	}
	p.flag |= parentIsJoin
}

func isBlockAliasDuplicate(node ast.ResultSetNode, blockAliases map[string]interface{}) error {
	if ts, ok := node.(*ast.BlockSource); ok {
		tabName := ts.AsName
		if tabName.L == "" {
			if blockNode, ok := ts.Source.(*ast.BlockName); ok {
				if blockNode.Schema.L != "" {
					tabName = perceptron.NewCIStr(fmt.Sprintf("%s.%s", blockNode.Schema.L, blockNode.Name.L))
				} else {
					tabName = blockNode.Name
				}
			}
		}
		_, exists := blockAliases[tabName.L]
		if len(tabName.L) != 0 && exists {
			return ErrNonUniqBlock.GenWithStackByArgs(tabName)
		}
		blockAliases[tabName.L] = nil
	}
	return nil
}

func checkDeferredCausetOptions(ops []*ast.DeferredCausetOption) (int, error) {
	isPrimary, isGenerated, isStored := 0, 0, false

	for _, op := range ops {
		switch op.Tp {
		case ast.DeferredCausetOptionPrimaryKey:
			isPrimary = 1
		case ast.DeferredCausetOptionGenerated:
			isGenerated = 1
			isStored = op.Stored
		}
	}

	if isPrimary > 0 && isGenerated > 0 && !isStored {
		return isPrimary, ErrUnsupportedOnGeneratedDeferredCauset.GenWithStackByArgs("Defining a virtual generated column as primary key")
	}

	return isPrimary, nil
}

func (p *preprocessor) checkCreateIndexGrammar(stmt *ast.CreateIndexStmt) {
	tName := stmt.Block.Name.String()
	if isIncorrectName(tName) {
		p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(tName)
		return
	}
	p.err = checHoTTexInfo(stmt.IndexName, stmt.IndexPartSpecifications)
}

func (p *preprocessor) checkStatisticsOpGrammar(node ast.Node) {
	var statsName string
	switch stmt := node.(type) {
	case *ast.CreateStatisticsStmt:
		statsName = stmt.StatsName
	case *ast.DropStatisticsStmt:
		statsName = stmt.StatsName
	}
	if isIncorrectName(statsName) {
		msg := fmt.Sprintf("Incorrect statistics name: %s", statsName)
		p.err = ErrInternal.GenWithStack(msg)
	}
	return
}

func (p *preprocessor) checkRenameBlockGrammar(stmt *ast.RenameBlockStmt) {
	oldBlock := stmt.OldBlock.Name.String()
	newBlock := stmt.NewBlock.Name.String()

	p.checkRenameBlock(oldBlock, newBlock)
}

func (p *preprocessor) checkRenameBlock(oldBlock, newBlock string) {
	if isIncorrectName(oldBlock) {
		p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(oldBlock)
		return
	}

	if isIncorrectName(newBlock) {
		p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(newBlock)
		return
	}
}

func (p *preprocessor) checkRepairBlockGrammar(stmt *ast.RepairBlockStmt) {
	// Check create block stmt whether it's is in REPAIR MODE.
	if !petriutil.RepairInfo.InRepairMode() {
		p.err = dbs.ErrRepairBlockFail.GenWithStackByArgs("MilevaDB is not in REPAIR MODE")
		return
	}
	if len(petriutil.RepairInfo.GetRepairBlockList()) == 0 {
		p.err = dbs.ErrRepairBlockFail.GenWithStackByArgs("repair list is empty")
		return
	}

	// Check rename action as the rename statement does.
	oldBlock := stmt.Block.Name.String()
	newBlock := stmt.CreateStmt.Block.Name.String()
	p.checkRenameBlock(oldBlock, newBlock)
}

func (p *preprocessor) checkAlterBlockGrammar(stmt *ast.AlterBlockStmt) {
	tName := stmt.Block.Name.String()
	if isIncorrectName(tName) {
		p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(tName)
		return
	}
	specs := stmt.Specs
	for _, spec := range specs {
		if spec.NewBlock != nil {
			ntName := spec.NewBlock.Name.String()
			if isIncorrectName(ntName) {
				p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(ntName)
				return
			}
		}
		for _, colDef := range spec.NewDeferredCausets {
			if p.err = checkDeferredCauset(colDef); p.err != nil {
				return
			}
		}
		if p.err = checkUnsupportedBlockOptions(spec.Options); p.err != nil {
			return
		}
		switch spec.Tp {
		case ast.AlterBlockAddConstraint:
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex,
				ast.ConstraintUniqKey, ast.ConstraintPrimaryKey:
				p.err = checHoTTexInfo(spec.Constraint.Name, spec.Constraint.Keys)
				if p.err != nil {
					return
				}
			default:
				// Nothing to do now.
			}
		default:
			// Nothing to do now.
		}
	}
}

// checkDuplicateDeferredCausetName checks if index exists duplicated columns.
func checkDuplicateDeferredCausetName(IndexPartSpecifications []*ast.IndexPartSpecification) error {
	colNames := make(map[string]struct{}, len(IndexPartSpecifications))
	for _, IndexDefCausNameWithExpr := range IndexPartSpecifications {
		if IndexDefCausNameWithExpr.DeferredCauset != nil {
			name := IndexDefCausNameWithExpr.DeferredCauset.Name
			if _, ok := colNames[name.L]; ok {
				return schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs(name)
			}
			colNames[name.L] = struct{}{}
		}
	}
	return nil
}

// checHoTTexInfo checks index name and index column names.
func checHoTTexInfo(indexName string, IndexPartSpecifications []*ast.IndexPartSpecification) error {
	if strings.EqualFold(indexName, allegrosql.PrimaryKeyName) {
		return dbs.ErrWrongNameForIndex.GenWithStackByArgs(indexName)
	}
	if len(IndexPartSpecifications) > allegrosql.MaxKeyParts {
		return schemareplicant.ErrTooManyKeyParts.GenWithStackByArgs(allegrosql.MaxKeyParts)
	}
	return checkDuplicateDeferredCausetName(IndexPartSpecifications)
}

// checkUnsupportedBlockOptions checks if there exists unsupported block options
func checkUnsupportedBlockOptions(options []*ast.BlockOption) error {
	for _, option := range options {
		switch option.Tp {
		case ast.BlockOptionUnion:
			return dbs.ErrBlockOptionUnionUnsupported
		case ast.BlockOptionInsertMethod:
			return dbs.ErrBlockOptionInsertMethodUnsupported
		}
	}
	return nil
}

// checkDeferredCauset checks if the column definition is valid.
// See https://dev.allegrosql.com/doc/refman/5.7/en/storage-requirements.html
func checkDeferredCauset(colDef *ast.DeferredCausetDef) error {
	// Check column name.
	cName := colDef.Name.Name.String()
	if isIncorrectName(cName) {
		return dbs.ErrWrongDeferredCausetName.GenWithStackByArgs(cName)
	}

	if isInvalidDefaultValue(colDef) {
		return types.ErrInvalidDefault.GenWithStackByArgs(colDef.Name.Name.O)
	}

	// Check column type.
	tp := colDef.Tp
	if tp == nil {
		return nil
	}
	if tp.Flen > math.MaxUint32 {
		return types.ErrTooBigDisplayWidth.GenWithStack("Display width out of range for column '%s' (max = %d)", colDef.Name.Name.O, math.MaxUint32)
	}

	switch tp.Tp {
	case allegrosql.TypeString:
		if tp.Flen != types.UnspecifiedLength && tp.Flen > allegrosql.MaxFieldCharLength {
			return types.ErrTooBigFieldLength.GenWithStack("DeferredCauset length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDef.Name.Name.O, allegrosql.MaxFieldCharLength)
		}
	case allegrosql.TypeVarchar:
		if len(tp.Charset) == 0 {
			// It's not easy to get the schemaReplicant charset and block charset here.
			// The charset is determined by the order DeferredCausetDefaultCharset --> BlockDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
			// return nil, to make the check in the dbs.CreateBlock.
			return nil
		}
		err := dbs.IsTooBigFieldLength(colDef.Tp.Flen, colDef.Name.Name.O, tp.Charset)
		if err != nil {
			return err
		}
	case allegrosql.TypeFloat, allegrosql.TypeDouble:
		if tp.Decimal > allegrosql.MaxFloatingTypeScale {
			return types.ErrTooBigScale.GenWithStackByArgs(tp.Decimal, colDef.Name.Name.O, allegrosql.MaxFloatingTypeScale)
		}
		if tp.Flen > allegrosql.MaxFloatingTypeWidth {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.Flen, colDef.Name.Name.O, allegrosql.MaxFloatingTypeWidth)
		}
	case allegrosql.TypeSet:
		if len(tp.Elems) > allegrosql.MaxTypeSetMembers {
			return types.ErrTooBigSet.GenWithStack("Too many strings for column %s and SET", colDef.Name.Name.O)
		}
		// Check set elements. See https://dev.allegrosql.com/doc/refman/5.7/en/set.html.
		for _, str := range colDef.Tp.Elems {
			if strings.Contains(str, ",") {
				return types.ErrIllegalValueForType.GenWithStackByArgs(types.TypeStr(tp.Tp), str)
			}
		}
	case allegrosql.TypeNewDecimal:
		if tp.Decimal > allegrosql.MaxDecimalScale {
			return types.ErrTooBigScale.GenWithStackByArgs(tp.Decimal, colDef.Name.Name.O, allegrosql.MaxDecimalScale)
		}

		if tp.Flen > allegrosql.MaxDecimalWidth {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.Flen, colDef.Name.Name.O, allegrosql.MaxDecimalWidth)
		}
	case allegrosql.TypeBit:
		if tp.Flen <= 0 {
			return types.ErrInvalidFieldSize.GenWithStackByArgs(colDef.Name.Name.O)
		}
		if tp.Flen > allegrosql.MaxBitDisplayWidth {
			return types.ErrTooBigDisplayWidth.GenWithStackByArgs(colDef.Name.Name.O, allegrosql.MaxBitDisplayWidth)
		}
	default:
		// TODO: Add more types.
	}
	return nil
}

// isDefaultValNowSymFunc checks whether default value is a NOW() builtin function.
func isDefaultValNowSymFunc(expr ast.ExprNode) bool {
	if funcCall, ok := expr.(*ast.FuncCallExpr); ok {
		// Default value NOW() is transformed to CURRENT_TIMESTAMP() in berolinaAllegroSQL.
		if funcCall.FnName.L == ast.CurrentTimestamp {
			return true
		}
	}
	return false
}

func isInvalidDefaultValue(colDef *ast.DeferredCausetDef) bool {
	tp := colDef.Tp
	// Check the last default value.
	for i := len(colDef.Options) - 1; i >= 0; i-- {
		columnOpt := colDef.Options[i]
		if columnOpt.Tp == ast.DeferredCausetOptionDefaultValue {
			if !(tp.Tp == allegrosql.TypeTimestamp || tp.Tp == allegrosql.TypeDatetime) && isDefaultValNowSymFunc(columnOpt.Expr) {
				return true
			}
			break
		}
	}

	return false
}

// isIncorrectName checks if the identifier is incorrect.
// See https://dev.allegrosql.com/doc/refman/5.7/en/identifiers.html
func isIncorrectName(name string) bool {
	if len(name) == 0 {
		return true
	}
	if name[len(name)-1] == ' ' {
		return true
	}
	return false
}

// checkContainDotDeferredCauset checks field contains the block name.
// for example :create block t (c1.c2 int default null).
func (p *preprocessor) checkContainDotDeferredCauset(stmt *ast.CreateBlockStmt) {
	tName := stmt.Block.Name.String()
	sName := stmt.Block.Schema.String()

	for _, colDef := range stmt.DefCauss {
		// check schemaReplicant and block names.
		if colDef.Name.Schema.O != sName && len(colDef.Name.Schema.O) != 0 {
			p.err = dbs.ErrWrongDBName.GenWithStackByArgs(colDef.Name.Schema.O)
			return
		}
		if colDef.Name.Block.O != tName && len(colDef.Name.Block.O) != 0 {
			p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(colDef.Name.Block.O)
			return
		}
	}
}

func (p *preprocessor) handleBlockName(tn *ast.BlockName) {
	if tn.Schema.L == "" {
		currentDB := p.ctx.GetStochastikVars().CurrentDB
		if currentDB == "" {
			p.err = errors.Trace(ErrNoDB)
			return
		}
		tn.Schema = perceptron.NewCIStr(currentDB)
	}
	if p.flag&inCreateOrDropBlock > 0 {
		// The block may not exist in create block or drop block statement.
		if p.flag&inRepairBlock > 0 {
			// Create stmt is in repair stmt, skip resolving the block to avoid error.
			return
		}
		// Create stmt is not in repair stmt, check the block not in repair list.
		if petriutil.RepairInfo.InRepairMode() {
			p.checkNotInRepair(tn)
		}
		return
	}
	// repairStmt: admin repair block A create block B ...
	// repairStmt's blockName is whether `inCreateOrDropBlock` or `inRepairBlock` flag.
	if p.flag&inRepairBlock > 0 {
		p.handleRepairName(tn)
		return
	}

	block, err := p.is.BlockByName(tn.Schema, tn.Name)
	if err != nil {
		p.err = err
		return
	}
	blockInfo := block.Meta()
	dbInfo, _ := p.is.SchemaByName(tn.Schema)
	// blockName should be checked as sequence object.
	if p.flag&inSequenceFunction > 0 {
		if !blockInfo.IsSequence() {
			p.err = schemareplicant.ErrWrongObject.GenWithStackByArgs(dbInfo.Name.O, blockInfo.Name.O, "SEQUENCE")
			return
		}
	}
	tn.BlockInfo = blockInfo
	tn.DBInfo = dbInfo
}

func (p *preprocessor) checkNotInRepair(tn *ast.BlockName) {
	blockInfo, dbInfo := petriutil.RepairInfo.GetRepairedBlockInfoByBlockName(tn.Schema.L, tn.Name.L)
	if dbInfo == nil {
		return
	}
	if blockInfo != nil {
		p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(tn.Name.L, "this block is in repair")
	}
}

func (p *preprocessor) handleRepairName(tn *ast.BlockName) {
	// Check the whether the repaired block is system block.
	if soliton.IsMemOrSysDB(tn.Schema.L) {
		p.err = dbs.ErrRepairBlockFail.GenWithStackByArgs("memory or system database is not for repair")
		return
	}
	blockInfo, dbInfo := petriutil.RepairInfo.GetRepairedBlockInfoByBlockName(tn.Schema.L, tn.Name.L)
	// blockName here only has the schemaReplicant rather than DBInfo.
	if dbInfo == nil {
		p.err = dbs.ErrRepairBlockFail.GenWithStackByArgs("database " + tn.Schema.L + " is not in repair")
		return
	}
	if blockInfo == nil {
		p.err = dbs.ErrRepairBlockFail.GenWithStackByArgs("block " + tn.Name.L + " is not in repair")
		return
	}
	p.ctx.SetValue(petriutil.RepairedBlock, blockInfo)
	p.ctx.SetValue(petriutil.RepairedDatabase, dbInfo)
}

func (p *preprocessor) resolveShowStmt(node *ast.ShowStmt) {
	if node.DBName == "" {
		if node.Block != nil && node.Block.Schema.L != "" {
			node.DBName = node.Block.Schema.O
		} else {
			node.DBName = p.ctx.GetStochastikVars().CurrentDB
		}
	} else if node.Block != nil && node.Block.Schema.L == "" {
		node.Block.Schema = perceptron.NewCIStr(node.DBName)
	}
	if node.User != nil && node.User.CurrentUser {
		// Fill the Username and Hostname with the current user.
		currentUser := p.ctx.GetStochastikVars().User
		if currentUser != nil {
			node.User.Username = currentUser.Username
			node.User.Hostname = currentUser.Hostname
			node.User.AuthUsername = currentUser.AuthUsername
			node.User.AuthHostname = currentUser.AuthHostname
		}
	}
}

func (p *preprocessor) resolveCreateBlockStmt(node *ast.CreateBlockStmt) {
	for _, val := range node.Constraints {
		if val.Refer != nil && val.Refer.Block.Schema.String() == "" {
			val.Refer.Block.Schema = node.Block.Schema
		}
	}
}

func (p *preprocessor) resolveAlterBlockStmt(node *ast.AlterBlockStmt) {
	for _, spec := range node.Specs {
		if spec.Tp == ast.AlterBlockRenameBlock {
			p.flag |= inCreateOrDropBlock
			break
		}
		if spec.Tp == ast.AlterBlockAddConstraint && spec.Constraint.Refer != nil {
			block := spec.Constraint.Refer.Block
			if block.Schema.L == "" && node.Block.Schema.L != "" {
				block.Schema = perceptron.NewCIStr(node.Block.Schema.L)
			}
		}
	}
}

func (p *preprocessor) resolveCreateSequenceStmt(stmt *ast.CreateSequenceStmt) {
	sName := stmt.Name.Name.String()
	if isIncorrectName(sName) {
		p.err = dbs.ErrWrongBlockName.GenWithStackByArgs(sName)
		return
	}
}
