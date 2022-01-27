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

package dbs

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
)

// defCausumnGenerationInDBS is a struct for validating generated defCausumns in DBS.
type defCausumnGenerationInDBS struct {
	position    int
	generated   bool
	dependences map[string]struct{}
}

// verifyDeferredCausetGeneration is for CREATE TABLE, because we need verify all defCausumns in the block.
func verifyDeferredCausetGeneration(defCausName2Generation map[string]defCausumnGenerationInDBS, defCausName string) error {
	attribute := defCausName2Generation[defCausName]
	if attribute.generated {
		for depDefCaus := range attribute.dependences {
			if attr, ok := defCausName2Generation[depDefCaus]; ok {
				if attr.generated && attribute.position <= attr.position {
					// A generated defCausumn definition can refer to other
					// generated defCausumns occurring earlier in the block.
					err := errGeneratedDeferredCausetNonPrior.GenWithStackByArgs()
					return errors.Trace(err)
				}
			} else {
				err := ErrBadField.GenWithStackByArgs(depDefCaus, "generated defCausumn function")
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// verifyDeferredCausetGenerationSingle is for ADD GENERATED COLUMN, we just need verify one defCausumn itself.
func verifyDeferredCausetGenerationSingle(dependDefCausNames map[string]struct{}, defcaus []*block.DeferredCauset, position *ast.DeferredCausetPosition) error {
	// Since the added defCausumn does not exist yet, we should derive it's offset from DeferredCausetPosition.
	pos, err := findPositionRelativeDeferredCauset(defcaus, position)
	if err != nil {
		return errors.Trace(err)
	}
	// should check unknown defCausumn first, then the prior ones.
	for _, defCaus := range defcaus {
		if _, ok := dependDefCausNames[defCaus.Name.L]; ok {
			if defCaus.IsGenerated() && defCaus.Offset >= pos {
				// Generated defCausumn can refer only to generated defCausumns defined prior to it.
				return errGeneratedDeferredCausetNonPrior.GenWithStackByArgs()
			}
		}
	}
	return nil
}

// checkDependedDefCausExist ensure all depended defCausumns exist and not hidden.
// NOTE: this will MODIFY parameter `dependDefCauss`.
func checkDependedDefCausExist(dependDefCauss map[string]struct{}, defcaus []*block.DeferredCauset) error {
	for _, defCaus := range defcaus {
		if !defCaus.Hidden {
			delete(dependDefCauss, defCaus.Name.L)
		}
	}
	if len(dependDefCauss) != 0 {
		for arbitraryDefCaus := range dependDefCauss {
			return ErrBadField.GenWithStackByArgs(arbitraryDefCaus, "generated defCausumn function")
		}
	}
	return nil
}

// findPositionRelativeDeferredCauset returns a pos relative to added generated defCausumn position.
func findPositionRelativeDeferredCauset(defcaus []*block.DeferredCauset, pos *ast.DeferredCausetPosition) (int, error) {
	position := len(defcaus)
	// Get the defCausumn position, default is defcaus's length means appending.
	// For "alter block ... add defCausumn(...)", the position will be nil.
	// For "alter block ... add defCausumn ... ", the position will be default one.
	if pos == nil {
		return position, nil
	}
	if pos.Tp == ast.DeferredCausetPositionFirst {
		position = 0
	} else if pos.Tp == ast.DeferredCausetPositionAfter {
		var defCaus *block.DeferredCauset
		for _, c := range defcaus {
			if c.Name.L == pos.RelativeDeferredCauset.Name.L {
				defCaus = c
				break
			}
		}
		if defCaus == nil {
			return -1, ErrBadField.GenWithStackByArgs(pos.RelativeDeferredCauset, "generated defCausumn function")
		}
		// Inserted position is after the mentioned defCausumn.
		position = defCaus.Offset + 1
	}
	return position, nil
}

// findDependedDeferredCausetNames returns a set of string, which indicates
// the names of the defCausumns that are depended by defCausDef.
func findDependedDeferredCausetNames(defCausDef *ast.DeferredCausetDef) (generated bool, defcausMap map[string]struct{}) {
	defcausMap = make(map[string]struct{})
	for _, option := range defCausDef.Options {
		if option.Tp == ast.DeferredCausetOptionGenerated {
			generated = true
			defCausNames := findDeferredCausetNamesInExpr(option.Expr)
			for _, depDefCaus := range defCausNames {
				defcausMap[depDefCaus.Name.L] = struct{}{}
			}
			break
		}
	}
	return
}

// findDeferredCausetNamesInExpr returns a slice of ast.DeferredCausetName which is referred in expr.
func findDeferredCausetNamesInExpr(expr ast.ExprNode) []*ast.DeferredCausetName {
	var c generatedDeferredCausetChecker
	expr.Accept(&c)
	return c.defcaus
}

type generatedDeferredCausetChecker struct {
	defcaus []*ast.DeferredCausetName
}

func (c *generatedDeferredCausetChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	return inNode, false
}

func (c *generatedDeferredCausetChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	switch x := inNode.(type) {
	case *ast.DeferredCausetName:
		c.defcaus = append(c.defcaus, x)
	}
	return inNode, true
}

// checkModifyGeneratedDeferredCauset checks the modification between
// old and new is valid or not by such rules:
//  1. the modification can't change stored status;
//  2. if the new is generated, check its refer rules.
//  3. check if the modified expr contains non-deterministic functions
//  4. check whether new defCausumn refers to any auto-increment defCausumns.
//  5. check if the new defCausumn is indexed or stored
func checkModifyGeneratedDeferredCauset(tbl block.Block, oldDefCaus, newDefCaus *block.DeferredCauset, newDefCausDef *ast.DeferredCausetDef) error {
	// rule 1.
	oldDefCausIsStored := !oldDefCaus.IsGenerated() || oldDefCaus.GeneratedStored
	newDefCausIsStored := !newDefCaus.IsGenerated() || newDefCaus.GeneratedStored
	if oldDefCausIsStored != newDefCausIsStored {
		return ErrUnsupportedOnGeneratedDeferredCauset.GenWithStackByArgs("Changing the STORED status")
	}

	// rule 2.
	originDefCauss := tbl.DefCauss()
	var defCausName2Generation = make(map[string]defCausumnGenerationInDBS, len(originDefCauss))
	for i, defCausumn := range originDefCauss {
		// We can compare the pointers simply.
		if defCausumn == oldDefCaus {
			defCausName2Generation[newDefCaus.Name.L] = defCausumnGenerationInDBS{
				position:    i,
				generated:   newDefCaus.IsGenerated(),
				dependences: newDefCaus.Dependences,
			}
		} else if !defCausumn.IsGenerated() {
			defCausName2Generation[defCausumn.Name.L] = defCausumnGenerationInDBS{
				position:  i,
				generated: false,
			}
		} else {
			defCausName2Generation[defCausumn.Name.L] = defCausumnGenerationInDBS{
				position:    i,
				generated:   true,
				dependences: defCausumn.Dependences,
			}
		}
	}
	// We always need test all defCausumns, even if it's not changed
	// because other can depend on it so its name can't be changed.
	for _, defCausumn := range originDefCauss {
		var defCausName string
		if defCausumn == oldDefCaus {
			defCausName = newDefCaus.Name.L
		} else {
			defCausName = defCausumn.Name.L
		}
		if err := verifyDeferredCausetGeneration(defCausName2Generation, defCausName); err != nil {
			return errors.Trace(err)
		}
	}

	if newDefCaus.IsGenerated() {
		// rule 3.
		if err := checkIllegalFn4GeneratedDeferredCauset(newDefCaus.Name.L, newDefCaus.GeneratedExpr); err != nil {
			return errors.Trace(err)
		}

		// rule 4.
		if err := checkGeneratedWithAutoInc(tbl.Meta(), newDefCausDef); err != nil {
			return errors.Trace(err)
		}

		// rule 5.
		if err := checHoTTexOrStored(tbl, oldDefCaus, newDefCaus); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type illegalFunctionChecker struct {
	hasIllegalFunc bool
	hasAggFunc     bool
}

func (c *illegalFunctionChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	switch node := inNode.(type) {
	case *ast.FuncCallExpr:
		// Blocked functions & non-builtin functions is not allowed
		_, IsFunctionBlocked := expression.IllegalFunctions4GeneratedDeferredCausets[node.FnName.L]
		if IsFunctionBlocked || !expression.IsFunctionSupported(node.FnName.L) {
			c.hasIllegalFunc = true
			return inNode, true
		}
	case *ast.SubqueryExpr, *ast.ValuesExpr, *ast.VariableExpr:
		// Subquery & `values(x)` & variable is not allowed
		c.hasIllegalFunc = true
		return inNode, true
	case *ast.AggregateFuncExpr:
		// Aggregate function is not allowed
		c.hasAggFunc = true
		return inNode, true
	}
	return inNode, false
}

func (c *illegalFunctionChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	return inNode, true
}

func checkIllegalFn4GeneratedDeferredCauset(defCausName string, expr ast.ExprNode) error {
	if expr == nil {
		return nil
	}
	var c illegalFunctionChecker
	expr.Accept(&c)
	if c.hasIllegalFunc {
		return ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs(defCausName)
	}
	if c.hasAggFunc {
		return ErrInvalidGroupFuncUse
	}
	return nil
}

// Check whether newDeferredCausetDef refers to any auto-increment defCausumns.
func checkGeneratedWithAutoInc(blockInfo *perceptron.BlockInfo, newDeferredCausetDef *ast.DeferredCausetDef) error {
	_, dependDefCausNames := findDependedDeferredCausetNames(newDeferredCausetDef)
	if err := checkAutoIncrementRef(newDeferredCausetDef.Name.Name.L, dependDefCausNames, blockInfo); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checHoTTexOrStored(tbl block.Block, oldDefCaus, newDefCaus *block.DeferredCauset) error {
	if oldDefCaus.GeneratedExprString == newDefCaus.GeneratedExprString {
		return nil
	}

	if newDefCaus.GeneratedStored {
		return ErrUnsupportedOnGeneratedDeferredCauset.GenWithStackByArgs("modifying a stored defCausumn")
	}

	for _, idx := range tbl.Indices() {
		for _, defCaus := range idx.Meta().DeferredCausets {
			if defCaus.Name.L == newDefCaus.Name.L {
				return ErrUnsupportedOnGeneratedDeferredCauset.GenWithStackByArgs("modifying an indexed defCausumn")
			}
		}
	}
	return nil
}

// checkAutoIncrementRef checks if an generated defCausumn depends on an auto-increment defCausumn and raises an error if so.
// See https://dev.allegrosql.com/doc/refman/5.7/en/create-block-generated-defCausumns.html for details.
func checkAutoIncrementRef(name string, dependencies map[string]struct{}, tbInfo *perceptron.BlockInfo) error {
	exists, autoIncrementDeferredCauset := schemareplicant.HasAutoIncrementDeferredCauset(tbInfo)
	if exists {
		if _, found := dependencies[autoIncrementDeferredCauset]; found {
			return ErrGeneratedDeferredCausetRefAutoInc.GenWithStackByArgs(name)
		}
	}
	return nil
}
