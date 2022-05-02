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
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/disjointset"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

type hashPartitionPruner struct {
	unionSet          *disjointset.IntSet // unionSet stores the relations like defCaus_i = defCaus_j
	constantMap       []*CouplingConstantWithRadix
	conditions        []Expression
	defCausMapper     map[int64]int
	numDeferredCauset int
	ctx               stochastikctx.Context
}

func (p *hashPartitionPruner) getDefCausID(defCaus *DeferredCauset) int {
	return p.defCausMapper[defCaus.UniqueID]
}

func (p *hashPartitionPruner) insertDefCaus(defCaus *DeferredCauset) {
	_, ok := p.defCausMapper[defCaus.UniqueID]
	if !ok {
		p.numDeferredCauset += 1
		p.defCausMapper[defCaus.UniqueID] = len(p.defCausMapper)
	}
}

func (p *hashPartitionPruner) reduceDeferredCausetEQ() bool {
	p.unionSet = disjointset.NewIntSet(p.numDeferredCauset)
	for i := range p.conditions {
		if fun, ok := p.conditions[i].(*ScalarFunction); ok && fun.FuncName.L == ast.EQ {
			lDefCaus, lOk := fun.GetArgs()[0].(*DeferredCauset)
			rDefCaus, rOk := fun.GetArgs()[1].(*DeferredCauset)
			if lOk && rOk {
				lID := p.getDefCausID(lDefCaus)
				rID := p.getDefCausID(rDefCaus)
				p.unionSet.Union(lID, rID)
			}
		}
	}
	for i := 0; i < p.numDeferredCauset; i++ {
		father := p.unionSet.FindRoot(i)
		if p.constantMap[i] != nil {
			if p.constantMap[father] != nil {
				// May has conflict here.
				if !p.constantMap[father].Equal(p.ctx, p.constantMap[i]) {
					return true
				}
			} else {
				p.constantMap[father] = p.constantMap[i]
			}
		}
	}
	for i := 0; i < p.numDeferredCauset; i++ {
		father := p.unionSet.FindRoot(i)
		if p.constantMap[father] != nil && p.constantMap[i] == nil {
			p.constantMap[i] = p.constantMap[father]
		}
	}
	return false
}

func (p *hashPartitionPruner) reduceCouplingConstantWithRadixEQ() bool {
	for _, con := range p.conditions {
		var defCaus *DeferredCauset
		var cond *CouplingConstantWithRadix
		if fn, ok := con.(*ScalarFunction); ok {
			if fn.FuncName.L == ast.IsNull {
				defCaus, ok = fn.GetArgs()[0].(*DeferredCauset)
				if ok {
					cond = NewNull()
				}
			} else {
				defCaus, cond = validEqualCond(p.ctx, con)
			}
		}
		if defCaus != nil {
			id := p.getDefCausID(defCaus)
			if p.constantMap[id] != nil {
				if p.constantMap[id].Equal(p.ctx, cond) {
					continue
				}
				return true
			}
			p.constantMap[id] = cond
		}
	}
	return false
}

func (p *hashPartitionPruner) tryEvalPartitionExpr(piExpr Expression) (val int64, success bool, isNull bool) {
	switch pi := piExpr.(type) {
	case *ScalarFunction:
		if pi.FuncName.L == ast.Plus || pi.FuncName.L == ast.Minus || pi.FuncName.L == ast.Mul || pi.FuncName.L == ast.Div {
			left, right := pi.GetArgs()[0], pi.GetArgs()[1]
			leftVal, ok, isNull := p.tryEvalPartitionExpr(left)
			if !ok || isNull {
				return 0, ok, isNull
			}
			rightVal, ok, isNull := p.tryEvalPartitionExpr(right)
			if !ok || isNull {
				return 0, ok, isNull
			}
			switch pi.FuncName.L {
			case ast.Plus:
				return rightVal + leftVal, true, false
			case ast.Minus:
				return rightVal - leftVal, true, false
			case ast.Mul:
				return rightVal * leftVal, true, false
			case ast.Div:
				return rightVal / leftVal, true, false
			}
		} else if pi.FuncName.L == ast.Year || pi.FuncName.L == ast.Month || pi.FuncName.L == ast.ToDays {
			defCaus := pi.GetArgs()[0].(*DeferredCauset)
			idx := p.getDefCausID(defCaus)
			val := p.constantMap[idx]
			if val != nil {
				pi.GetArgs()[0] = val
				ret, isNull, err := pi.EvalInt(p.ctx, chunk.Row{})
				if err != nil {
					return 0, false, false
				}
				return ret, true, isNull
			}
			return 0, false, false
		}
	case *CouplingConstantWithRadix:
		val, err := pi.Eval(chunk.Row{})
		if err != nil {
			return 0, false, false
		}
		if val.IsNull() {
			return 0, true, true
		}
		if val.HoTT() == types.HoTTInt64 {
			return val.GetInt64(), true, false
		} else if val.HoTT() == types.HoTTUint64 {
			return int64(val.GetUint64()), true, false
		}
	case *DeferredCauset:
		// Look up map
		idx := p.getDefCausID(pi)
		val := p.constantMap[idx]
		if val != nil {
			return p.tryEvalPartitionExpr(val)
		}
		return 0, false, false
	}
	return 0, false, false
}

func newHashPartitionPruner() *hashPartitionPruner {
	pruner := &hashPartitionPruner{}
	pruner.defCausMapper = make(map[int64]int)
	pruner.numDeferredCauset = 0
	return pruner
}

// solve eval the hash partition expression, the first return value represent the result of partition expression. The second
// return value is whether eval success. The third return value represent whether the query conditions is always false.
func (p *hashPartitionPruner) solve(ctx stochastikctx.Context, conds []Expression, piExpr Expression) (val int64, ok bool, isAlwaysFalse bool) {
	p.ctx = ctx
	for _, cond := range conds {
		p.conditions = append(p.conditions, SplitCNFItems(cond)...)
		for _, defCaus := range ExtractDeferredCausets(cond) {
			p.insertDefCaus(defCaus)
		}
	}
	for _, defCaus := range ExtractDeferredCausets(piExpr) {
		p.insertDefCaus(defCaus)
	}
	p.constantMap = make([]*CouplingConstantWithRadix, p.numDeferredCauset)
	isAlwaysFalse = p.reduceCouplingConstantWithRadixEQ()
	if isAlwaysFalse {
		return 0, false, isAlwaysFalse
	}
	isAlwaysFalse = p.reduceDeferredCausetEQ()
	if isAlwaysFalse {
		return 0, false, isAlwaysFalse
	}
	res, ok, isNull := p.tryEvalPartitionExpr(piExpr)
	if isNull && ok {
		return 0, ok, false
	}
	return res, ok, false
}

// FastLocateHashPartition is used to get hash partition quickly.
func FastLocateHashPartition(ctx stochastikctx.Context, conds []Expression, piExpr Expression) (int64, bool, bool) {
	return newHashPartitionPruner().solve(ctx, conds, piExpr)
}
