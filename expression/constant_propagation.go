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

package expression

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/disjointset"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"go.uber.org/zap"
)

// MaxPropagateDefCaussCnt means the max number of defCausumns that can participate propagation.
var MaxPropagateDefCaussCnt = 100

type basePropConstSolver struct {
	defCausMapper map[int64]int       // defCausMapper maps defCausumn to its index
	eqList    []*CouplingConstantWithRadix         // if eqList[i] != nil, it means defCaus_i = eqList[i]
	unionSet  *disjointset.IntSet // unionSet stores the relations like defCaus_i = defCaus_j
	defCausumns   []*DeferredCauset           // defCausumns stores all defCausumns appearing in the conditions
	ctx       stochastikctx.Context
}

func (s *basePropConstSolver) getDefCausID(defCaus *DeferredCauset) int {
	return s.defCausMapper[defCaus.UniqueID]
}

func (s *basePropConstSolver) insertDefCaus(defCaus *DeferredCauset) {
	_, ok := s.defCausMapper[defCaus.UniqueID]
	if !ok {
		s.defCausMapper[defCaus.UniqueID] = len(s.defCausMapper)
		s.defCausumns = append(s.defCausumns, defCaus)
	}
}

// tryToUFIDelateEQList tries to uFIDelate the eqList. When the eqList has causetstore this defCausumn with a different constant, like
// a = 1 and a = 2, we set the second return value to false.
func (s *basePropConstSolver) tryToUFIDelateEQList(defCaus *DeferredCauset, con *CouplingConstantWithRadix) (bool, bool) {
	if con.Value.IsNull() {
		return false, true
	}
	id := s.getDefCausID(defCaus)
	oldCon := s.eqList[id]
	if oldCon != nil {
		return false, !oldCon.Equal(s.ctx, con)
	}
	s.eqList[id] = con
	return true, false
}

func validEqualCondHelper(ctx stochastikctx.Context, eq *ScalarFunction, defCausIsLeft bool) (*DeferredCauset, *CouplingConstantWithRadix) {
	var defCaus *DeferredCauset
	var con *CouplingConstantWithRadix
	defCausOk := false
	conOk := false
	if defCausIsLeft {
		defCaus, defCausOk = eq.GetArgs()[0].(*DeferredCauset)
	} else {
		defCaus, defCausOk = eq.GetArgs()[1].(*DeferredCauset)
	}
	if !defCausOk {
		return nil, nil
	}
	if defCausIsLeft {
		con, conOk = eq.GetArgs()[1].(*CouplingConstantWithRadix)
	} else {
		con, conOk = eq.GetArgs()[0].(*CouplingConstantWithRadix)
	}
	if !conOk {
		return nil, nil
	}
	if ContainMublockConst(ctx, []Expression{con}) {
		return nil, nil
	}
	if !defCauslate.CompatibleDefCauslate(defCaus.GetType().DefCauslate, con.GetType().DefCauslate) {
		return nil, nil
	}
	return defCaus, con
}

// validEqualCond checks if the cond is an expression like [defCausumn eq constant].
func validEqualCond(ctx stochastikctx.Context, cond Expression) (*DeferredCauset, *CouplingConstantWithRadix) {
	if eq, ok := cond.(*ScalarFunction); ok {
		if eq.FuncName.L != ast.EQ {
			return nil, nil
		}
		defCaus, con := validEqualCondHelper(ctx, eq, true)
		if defCaus == nil {
			return validEqualCondHelper(ctx, eq, false)
		}
		return defCaus, con
	}
	return nil, nil
}

// tryToReplaceCond aims to replace all occurrences of defCausumn 'src' and try to replace it with 'tgt' in 'cond'
// It returns
//  bool: if a rememristed happened
//  bool: if 'cond' contains non-deterministic expression
//  Expression: the replaced expression, or original 'cond' if the rememristed didn't happen
//
// For example:
//  for 'a, b, a < 3', it returns 'true, false, b < 3'
//  for 'a, b, sin(a) + cos(a) = 5', it returns 'true, false, returns sin(b) + cos(b) = 5'
//  for 'a, b, cast(a) < rand()', it returns 'false, true, cast(a) < rand()'
func tryToReplaceCond(ctx stochastikctx.Context, src *DeferredCauset, tgt *DeferredCauset, cond Expression, rejectControl bool) (bool, bool, Expression) {
	sf, ok := cond.(*ScalarFunction)
	if !ok {
		return false, false, cond
	}
	replaced := false
	var args []Expression
	if _, ok := unFoldableFunctions[sf.FuncName.L]; ok {
		return false, true, cond
	}
	if _, ok := inequalFunctions[sf.FuncName.L]; ok {
		return false, true, cond
	}
	// See https://github.com/whtcorpsinc/MilevaDB-Prod/issues/15782. The control function's result may rely on the original nullable
	// information of the outer side defCausumn. Its args cannot be replaced easily.
	// A more strict check is that after we replace the arg. We check the nullability of the new expression.
	// But we haven't maintained it yet, so don't replace the arg of the control function currently.
	if rejectControl && (sf.FuncName.L == ast.Ifnull || sf.FuncName.L == ast.If || sf.FuncName.L == ast.Case) {
		return false, false, cond
	}
	for idx, expr := range sf.GetArgs() {
		if src.Equal(nil, expr) {
			_, defCausl := cond.CharsetAndDefCauslation(ctx)
			if tgt.GetType().DefCauslate != defCausl {
				continue
			}
			replaced = true
			if args == nil {
				args = make([]Expression, len(sf.GetArgs()))
				INTERLOCKy(args, sf.GetArgs())
			}
			args[idx] = tgt
		} else {
			subReplaced, isNonDeterministic, subExpr := tryToReplaceCond(ctx, src, tgt, expr, rejectControl)
			if isNonDeterministic {
				return false, true, cond
			} else if subReplaced {
				replaced = true
				if args == nil {
					args = make([]Expression, len(sf.GetArgs()))
					INTERLOCKy(args, sf.GetArgs())
				}
				args[idx] = subExpr
			}
		}
	}
	if replaced {
		return true, false, NewFunctionInternal(ctx, sf.FuncName.L, sf.GetType(), args...)
	}
	return false, false, cond
}

type propConstSolver struct {
	basePropConstSolver
	conditions []Expression
}

// propagateCouplingConstantWithRadixEQ propagates expressions like 'defCausumn = constant' by substituting the constant for defCausumn, the
// procedure repeats multiple times. An example runs as following:
// a = d & b * 2 = c & c = d + 2 & b = 1 & a = 4, we pick eq cond b = 1 and a = 4
// d = 4 & 2 = c & c = d + 2 & b = 1 & a = 4, we propagate b = 1 and a = 4 and pick eq cond c = 2 and d = 4
// d = 4 & 2 = c & false & b = 1 & a = 4, we propagate c = 2 and d = 4, and do constant folding: c = d + 2 will be folded as false.
func (s *propConstSolver) propagateCouplingConstantWithRadixEQ() {
	s.eqList = make([]*CouplingConstantWithRadix, len(s.defCausumns))
	visited := make([]bool, len(s.conditions))
	for i := 0; i < MaxPropagateDefCaussCnt; i++ {
		mapper := s.pickNewEQConds(visited)
		if len(mapper) == 0 {
			return
		}
		defcaus := make([]*DeferredCauset, 0, len(mapper))
		cons := make([]Expression, 0, len(mapper))
		for id, con := range mapper {
			defcaus = append(defcaus, s.defCausumns[id])
			cons = append(cons, con)
		}
		for i, cond := range s.conditions {
			if !visited[i] {
				s.conditions[i] = DeferredCausetSubstitute(cond, NewSchema(defcaus...), cons)
			}
		}
	}
}

// propagateDeferredCausetEQ propagates expressions like 'defCausumn A = defCausumn B' by adding extra filters
// 'expression(..., defCausumn B, ...)' propagated from 'expression(..., defCausumn A, ...)' as long as:
//
//  1. The expression is deterministic
//  2. The expression doesn't have any side effect
//
// e.g. For expression a = b and b = c and c = d and c < 1 , we can get extra a < 1 and b < 1 and d < 1.
// However, for a = b and a < rand(), we cannot propagate a < rand() to b < rand() because rand() is non-deterministic
//
// This propagation may bring redundancies that we need to resolve later, for example:
// for a = b and a < 3 and b < 3, we get new a < 3 and b < 3, which are redundant
// for a = b and a < 3 and 3 > b, we get new b < 3 and 3 > a, which are redundant
// for a = b and a < 3 and b < 4, we get new a < 4 and b < 3 but should expect a < 3 and b < 3
// for a = b and a in (3) and b in (4), we get b in (3) and a in (4) but should expect 'false'
//
// TODO: remove redundancies later
//
// We maintain a unionSet representing the equivalent for every two defCausumns.
func (s *propConstSolver) propagateDeferredCausetEQ() {
	visited := make([]bool, len(s.conditions))
	s.unionSet = disjointset.NewIntSet(len(s.defCausumns))
	for i := range s.conditions {
		if fun, ok := s.conditions[i].(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
			lDefCaus, lOk := fun.GetArgs()[0].(*DeferredCauset)
			rDefCaus, rOk := fun.GetArgs()[1].(*DeferredCauset)
			if lOk && rOk && lDefCaus.GetType().DefCauslate == rDefCaus.GetType().DefCauslate {
				lID := s.getDefCausID(lDefCaus)
				rID := s.getDefCausID(rDefCaus)
				s.unionSet.Union(lID, rID)
				visited[i] = true
			}
		}
	}

	condsLen := len(s.conditions)
	for i, defCausi := range s.defCausumns {
		for j := i + 1; j < len(s.defCausumns); j++ {
			// unionSet doesn't have iterate(), we use a two layer loop to iterate defCaus_i = defCaus_j relation
			if s.unionSet.FindRoot(i) != s.unionSet.FindRoot(j) {
				continue
			}
			defCausj := s.defCausumns[j]
			for k := 0; k < condsLen; k++ {
				if visited[k] {
					// cond_k has been used to retrieve equality relation
					continue
				}
				cond := s.conditions[k]
				replaced, _, newExpr := tryToReplaceCond(s.ctx, defCausi, defCausj, cond, false)
				if replaced {
					s.conditions = append(s.conditions, newExpr)
				}
				replaced, _, newExpr = tryToReplaceCond(s.ctx, defCausj, defCausi, cond, false)
				if replaced {
					s.conditions = append(s.conditions, newExpr)
				}
			}
		}
	}
}

func (s *propConstSolver) setConds2ConstFalse() {
	s.conditions = []Expression{&CouplingConstantWithRadix{
		Value:   types.NewCauset(false),
		RetType: types.NewFieldType(allegrosql.TypeTiny),
	}}
}

// pickNewEQConds tries to pick new equal conds and puts them to retMapper.
func (s *propConstSolver) pickNewEQConds(visited []bool) (retMapper map[int]*CouplingConstantWithRadix) {
	retMapper = make(map[int]*CouplingConstantWithRadix)
	for i, cond := range s.conditions {
		if visited[i] {
			continue
		}
		defCaus, con := validEqualCond(s.ctx, cond)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		var ok bool
		if defCaus == nil {
			con, ok = cond.(*CouplingConstantWithRadix)
			if !ok {
				continue
			}
			visited[i] = true
			if ContainMublockConst(s.ctx, []Expression{con}) {
				continue
			}
			value, _, err := EvalBool(s.ctx, []Expression{con}, chunk.Event{})
			if err != nil {
				terror.Log(err)
				return nil
			}
			if !value {
				s.setConds2ConstFalse()
				return nil
			}
			continue
		}
		visited[i] = true
		uFIDelated, foreverFalse := s.tryToUFIDelateEQList(defCaus, con)
		if foreverFalse {
			s.setConds2ConstFalse()
			return nil
		}
		if uFIDelated {
			retMapper[s.getDefCausID(defCaus)] = con
		}
	}
	return
}

func (s *propConstSolver) solve(conditions []Expression) []Expression {
	defcaus := make([]*DeferredCauset, 0, len(conditions))
	for _, cond := range conditions {
		s.conditions = append(s.conditions, SplitCNFItems(cond)...)
		defcaus = append(defcaus, ExtractDeferredCausets(cond)...)
	}
	for _, defCaus := range defcaus {
		s.insertDefCaus(defCaus)
	}
	if len(s.defCausumns) > MaxPropagateDefCaussCnt {
		logutil.BgLogger().Warn("too many defCausumns in a single CNF",
			zap.Int("numDefCauss", len(s.defCausumns)),
			zap.Int("maxNumDefCauss", MaxPropagateDefCaussCnt),
		)
		return conditions
	}
	s.propagateCouplingConstantWithRadixEQ()
	s.propagateDeferredCausetEQ()
	s.conditions = propagateCouplingConstantWithRadixDNF(s.ctx, s.conditions)
	return s.conditions
}

// PropagateCouplingConstantWithRadix propagate constant values of deterministic predicates in a condition.
func PropagateCouplingConstantWithRadix(ctx stochastikctx.Context, conditions []Expression) []Expression {
	return newPropConstSolver().PropagateCouplingConstantWithRadix(ctx, conditions)
}

type propOuterJoinConstSolver struct {
	basePropConstSolver
	joinConds   []Expression
	filterConds []Expression
	outerSchema *Schema
	innerSchema *Schema
	// nullSensitive indicates if this outer join is null sensitive, if true, we cannot generate
	// additional `defCaus is not null` condition from defCausumn equal conditions. Specifically, this value
	// is true for LeftOuterSemiJoin and AntiLeftOuterSemiJoin.
	nullSensitive bool
}

func (s *propOuterJoinConstSolver) setConds2ConstFalse(filterConds bool) {
	s.joinConds = []Expression{&CouplingConstantWithRadix{
		Value:   types.NewCauset(false),
		RetType: types.NewFieldType(allegrosql.TypeTiny),
	}}
	if filterConds {
		s.filterConds = []Expression{&CouplingConstantWithRadix{
			Value:   types.NewCauset(false),
			RetType: types.NewFieldType(allegrosql.TypeTiny),
		}}
	}
}

// pickEQCondsOnOuterDefCaus picks constant equal expression from specified conditions.
func (s *propOuterJoinConstSolver) pickEQCondsOnOuterDefCaus(retMapper map[int]*CouplingConstantWithRadix, visited []bool, filterConds bool) map[int]*CouplingConstantWithRadix {
	var conds []Expression
	var condsOffset int
	if filterConds {
		conds = s.filterConds
	} else {
		conds = s.joinConds
		condsOffset = len(s.filterConds)
	}
	for i, cond := range conds {
		if visited[i+condsOffset] {
			continue
		}
		defCaus, con := validEqualCond(s.ctx, cond)
		// Then we check if this CNF item is a false constant. If so, we will set the whole condition to false.
		var ok bool
		if defCaus == nil {
			con, ok = cond.(*CouplingConstantWithRadix)
			if !ok {
				continue
			}
			visited[i+condsOffset] = true
			if ContainMublockConst(s.ctx, []Expression{con}) {
				continue
			}
			value, _, err := EvalBool(s.ctx, []Expression{con}, chunk.Event{})
			if err != nil {
				terror.Log(err)
				return nil
			}
			if !value {
				s.setConds2ConstFalse(filterConds)
				return nil
			}
			continue
		}
		// Only extract `outerDefCaus = const` expressions.
		if !s.outerSchema.Contains(defCaus) {
			continue
		}
		visited[i+condsOffset] = true
		uFIDelated, foreverFalse := s.tryToUFIDelateEQList(defCaus, con)
		if foreverFalse {
			s.setConds2ConstFalse(filterConds)
			return nil
		}
		if uFIDelated {
			retMapper[s.getDefCausID(defCaus)] = con
		}
	}
	return retMapper
}

// pickNewEQConds picks constant equal expressions from join and filter conditions.
func (s *propOuterJoinConstSolver) pickNewEQConds(visited []bool) map[int]*CouplingConstantWithRadix {
	retMapper := make(map[int]*CouplingConstantWithRadix)
	retMapper = s.pickEQCondsOnOuterDefCaus(retMapper, visited, true)
	if retMapper == nil {
		// Filter is constant false or error occurred, enforce early termination.
		return nil
	}
	retMapper = s.pickEQCondsOnOuterDefCaus(retMapper, visited, false)
	return retMapper
}

// propagateCouplingConstantWithRadixEQ propagates expressions like `outerDefCaus = const` by substituting `outerDefCaus` in *JOIN* condition
// with `const`, the procedure repeats multiple times.
func (s *propOuterJoinConstSolver) propagateCouplingConstantWithRadixEQ() {
	s.eqList = make([]*CouplingConstantWithRadix, len(s.defCausumns))
	lenFilters := len(s.filterConds)
	visited := make([]bool, lenFilters+len(s.joinConds))
	for i := 0; i < MaxPropagateDefCaussCnt; i++ {
		mapper := s.pickNewEQConds(visited)
		if len(mapper) == 0 {
			return
		}
		defcaus := make([]*DeferredCauset, 0, len(mapper))
		cons := make([]Expression, 0, len(mapper))
		for id, con := range mapper {
			defcaus = append(defcaus, s.defCausumns[id])
			cons = append(cons, con)
		}
		for i, cond := range s.joinConds {
			if !visited[i+lenFilters] {
				s.joinConds[i] = DeferredCausetSubstitute(cond, NewSchema(defcaus...), cons)
			}
		}
	}
}

func (s *propOuterJoinConstSolver) defcausFromOuterAndInner(defCaus1, defCaus2 *DeferredCauset) (*DeferredCauset, *DeferredCauset) {
	if s.outerSchema.Contains(defCaus1) && s.innerSchema.Contains(defCaus2) {
		return defCaus1, defCaus2
	}
	if s.outerSchema.Contains(defCaus2) && s.innerSchema.Contains(defCaus1) {
		return defCaus2, defCaus1
	}
	return nil, nil
}

// validDefCausEqualCond checks if expression is defCausumn equal condition that we can use for constant
// propagation over outer join. We only use expression like `outerDefCaus = innerDefCaus`, for expressions like
// `outerDefCaus1 = outerDefCaus2` or `innerDefCaus1 = innerDefCaus2`, they do not help deriving new inner block conditions
// which can be pushed down to children plan nodes, so we do not pick them.
func (s *propOuterJoinConstSolver) validDefCausEqualCond(cond Expression) (*DeferredCauset, *DeferredCauset) {
	if fun, ok := cond.(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
		lDefCaus, lOk := fun.GetArgs()[0].(*DeferredCauset)
		rDefCaus, rOk := fun.GetArgs()[1].(*DeferredCauset)
		if lOk && rOk && lDefCaus.GetType().DefCauslate == rDefCaus.GetType().DefCauslate {
			return s.defcausFromOuterAndInner(lDefCaus, rDefCaus)
		}
	}
	return nil, nil

}

// deriveConds given `outerDefCaus = innerDefCaus`, derive new expression for specified conditions.
func (s *propOuterJoinConstSolver) deriveConds(outerDefCaus, innerDefCaus *DeferredCauset, schemaReplicant *Schema, fCondsOffset int, visited []bool, filterConds bool) []bool {
	var offset, condsLen int
	var conds []Expression
	if filterConds {
		conds = s.filterConds
		offset = fCondsOffset
		condsLen = len(s.filterConds)
	} else {
		conds = s.joinConds
		condsLen = fCondsOffset
	}
	for k := 0; k < condsLen; k++ {
		if visited[k+offset] {
			// condition has been used to retrieve equality relation or contains defCausumn beyond children schemaReplicant.
			continue
		}
		cond := conds[k]
		if !ExprFromSchema(cond, schemaReplicant) {
			visited[k+offset] = true
			continue
		}
		replaced, _, newExpr := tryToReplaceCond(s.ctx, outerDefCaus, innerDefCaus, cond, true)
		if replaced {
			s.joinConds = append(s.joinConds, newExpr)
		}
	}
	return visited
}

// propagateDeferredCausetEQ propagates expressions like 'outerDefCaus = innerDefCaus' by adding extra filters
// 'expression(..., innerDefCaus, ...)' derived from 'expression(..., outerDefCaus, ...)' as long as
// 'expression(..., outerDefCaus, ...)' does not reference defCausumns outside children schemas of join node.
// Derived new expressions must be appended into join condition, not filter condition.
func (s *propOuterJoinConstSolver) propagateDeferredCausetEQ() {
	visited := make([]bool, 2*len(s.joinConds)+len(s.filterConds))
	s.unionSet = disjointset.NewIntSet(len(s.defCausumns))
	var outerDefCaus, innerDefCaus *DeferredCauset
	// Only consider defCausumn equal condition in joinConds.
	// If we have defCausumn equal in filter condition, the outer join should have been simplified already.
	for i := range s.joinConds {
		outerDefCaus, innerDefCaus = s.validDefCausEqualCond(s.joinConds[i])
		if outerDefCaus != nil {
			outerID := s.getDefCausID(outerDefCaus)
			innerID := s.getDefCausID(innerDefCaus)
			s.unionSet.Union(outerID, innerID)
			visited[i] = true
			// Generate `innerDefCaus is not null` from `outerDefCaus = innerDefCaus`. Note that `outerDefCaus is not null`
			// does not hold since we are in outer join.
			// For AntiLeftOuterSemiJoin, this does not work, for example:
			// `select *, t1.a not in (select t2.b from t t2) from t t1` does not imply `t2.b is not null`.
			// For LeftOuterSemiJoin, this does not work either, for example:
			// `select *, t1.a in (select t2.b from t t2) from t t1`
			// rows with t2.b is null would impact whether LeftOuterSemiJoin should output 0 or null if there
			// is no event satisfying t2.b = t1.a
			if s.nullSensitive {
				continue
			}
			childDefCaus := s.innerSchema.RetrieveDeferredCauset(innerDefCaus)
			if !allegrosql.HasNotNullFlag(childDefCaus.RetType.Flag) {
				notNullExpr := BuildNotNullExpr(s.ctx, childDefCaus)
				s.joinConds = append(s.joinConds, notNullExpr)
			}
		}
	}
	lenJoinConds := len(s.joinConds)
	mergedSchema := MergeSchema(s.outerSchema, s.innerSchema)
	for i, defCausi := range s.defCausumns {
		for j := i + 1; j < len(s.defCausumns); j++ {
			// unionSet doesn't have iterate(), we use a two layer loop to iterate defCaus_i = defCaus_j relation.
			if s.unionSet.FindRoot(i) != s.unionSet.FindRoot(j) {
				continue
			}
			defCausj := s.defCausumns[j]
			outerDefCaus, innerDefCaus = s.defcausFromOuterAndInner(defCausi, defCausj)
			if outerDefCaus == nil {
				continue
			}
			visited = s.deriveConds(outerDefCaus, innerDefCaus, mergedSchema, lenJoinConds, visited, false)
			visited = s.deriveConds(outerDefCaus, innerDefCaus, mergedSchema, lenJoinConds, visited, true)
		}
	}
}

func (s *propOuterJoinConstSolver) solve(joinConds, filterConds []Expression) ([]Expression, []Expression) {
	defcaus := make([]*DeferredCauset, 0, len(joinConds)+len(filterConds))
	for _, cond := range joinConds {
		s.joinConds = append(s.joinConds, SplitCNFItems(cond)...)
		defcaus = append(defcaus, ExtractDeferredCausets(cond)...)
	}
	for _, cond := range filterConds {
		s.filterConds = append(s.filterConds, SplitCNFItems(cond)...)
		defcaus = append(defcaus, ExtractDeferredCausets(cond)...)
	}
	for _, defCaus := range defcaus {
		s.insertDefCaus(defCaus)
	}
	if len(s.defCausumns) > MaxPropagateDefCaussCnt {
		logutil.BgLogger().Warn("too many defCausumns",
			zap.Int("numDefCauss", len(s.defCausumns)),
			zap.Int("maxNumDefCauss", MaxPropagateDefCaussCnt),
		)
		return joinConds, filterConds
	}
	s.propagateCouplingConstantWithRadixEQ()
	s.propagateDeferredCausetEQ()
	s.joinConds = propagateCouplingConstantWithRadixDNF(s.ctx, s.joinConds)
	s.filterConds = propagateCouplingConstantWithRadixDNF(s.ctx, s.filterConds)
	return s.joinConds, s.filterConds
}

// propagateCouplingConstantWithRadixDNF find DNF item from CNF, and propagate constant inside DNF.
func propagateCouplingConstantWithRadixDNF(ctx stochastikctx.Context, conds []Expression) []Expression {
	for i, cond := range conds {
		if dnf, ok := cond.(*ScalarFunction); ok && dnf.FuncName.L == ast.LogicOr {
			dnfItems := SplitDNFItems(cond)
			for j, item := range dnfItems {
				dnfItems[j] = ComposeCNFCondition(ctx, PropagateCouplingConstantWithRadix(ctx, []Expression{item})...)
			}
			conds[i] = ComposeDNFCondition(ctx, dnfItems...)
		}
	}
	return conds
}

// PropConstOverOuterJoin propagate constant equal and defCausumn equal conditions over outer join.
// First step is to extract `outerDefCaus = const` from join conditions and filter conditions,
// and substitute `outerDefCaus` in join conditions with `const`;
// Second step is to extract `outerDefCaus = innerDefCaus` from join conditions, and derive new join
// conditions based on this defCausumn equal condition and `outerDefCaus` related
// expressions in join conditions and filter conditions;
func PropConstOverOuterJoin(ctx stochastikctx.Context, joinConds, filterConds []Expression,
	outerSchema, innerSchema *Schema, nullSensitive bool) ([]Expression, []Expression) {
	solver := &propOuterJoinConstSolver{
		outerSchema:   outerSchema,
		innerSchema:   innerSchema,
		nullSensitive: nullSensitive,
	}
	solver.defCausMapper = make(map[int64]int)
	solver.ctx = ctx
	return solver.solve(joinConds, filterConds)
}

// PropagateCouplingConstantWithRadixSolver is a constant propagate solver.
type PropagateCouplingConstantWithRadixSolver interface {
	PropagateCouplingConstantWithRadix(ctx stochastikctx.Context, conditions []Expression) []Expression
}

// newPropConstSolver returns a PropagateCouplingConstantWithRadixSolver.
func newPropConstSolver() PropagateCouplingConstantWithRadixSolver {
	solver := &propConstSolver{}
	solver.defCausMapper = make(map[int64]int)
	return solver
}

// PropagateCouplingConstantWithRadix propagate constant values of deterministic predicates in a condition.
func (s *propConstSolver) PropagateCouplingConstantWithRadix(ctx stochastikctx.Context, conditions []Expression) []Expression {
	s.ctx = ctx
	return s.solve(conditions)
}
