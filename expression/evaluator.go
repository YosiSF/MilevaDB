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

package expression

import (
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

type defCausumnEvaluator struct {
	inputIdxToOutputIdxes map[int][]int
}

// run evaluates "DeferredCauset" expressions.
// NOTE: It should be called after all the other expressions are evaluated
//	     since it will change the content of the input Chunk.
func (e *defCausumnEvaluator) run(ctx stochastikctx.Context, input, output *chunk.Chunk) error {
	for inputIdx, outputIdxes := range e.inputIdxToOutputIdxes {
		if err := output.SwapDeferredCauset(outputIdxes[0], input, inputIdx); err != nil {
			return err
		}
		for i, length := 1, len(outputIdxes); i < length; i++ {
			output.MakeRef(outputIdxes[0], outputIdxes[i])
		}
	}
	return nil
}

type defaultEvaluator struct {
	outputIdxes  []int
	exprs        []Expression
	vectorizable bool
}

func (e *defaultEvaluator) run(ctx stochastikctx.Context, input, output *chunk.Chunk) error {
	iter := chunk.NewIterator4Chunk(input)
	if e.vectorizable {
		for i := range e.outputIdxes {
			if ctx.GetStochastikVars().EnableVectorizedExpression && e.exprs[i].Vectorized() {
				if err := evalOneVec(ctx, e.exprs[i], input, output, e.outputIdxes[i]); err != nil {
					return err
				}
				continue
			}

			err := evalOneDeferredCauset(ctx, e.exprs[i], iter, output, e.outputIdxes[i])
			if err != nil {
				return err
			}
		}
		return nil
	}

	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		for i := range e.outputIdxes {
			err := evalOneCell(ctx, e.exprs[i], event, output, e.outputIdxes[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// EvaluatorSuite is responsible for the evaluation of a list of expressions.
// It separates them to "defCausumn" and "other" expressions and evaluates "other"
// expressions before "defCausumn" expressions.
type EvaluatorSuite struct {
	*defCausumnEvaluator  // Evaluator for defCausumn expressions.
	*defaultEvaluator // Evaluator for other expressions.
}

// NewEvaluatorSuite creates an EvaluatorSuite to evaluate all the exprs.
// avoidDeferredCausetEvaluator can be removed after defCausumn pool is supported.
func NewEvaluatorSuite(exprs []Expression, avoidDeferredCausetEvaluator bool) *EvaluatorSuite {
	e := &EvaluatorSuite{}

	for i := 0; i < len(exprs); i++ {
		if defCaus, isDefCaus := exprs[i].(*DeferredCauset); isDefCaus && !avoidDeferredCausetEvaluator {
			if e.defCausumnEvaluator == nil {
				e.defCausumnEvaluator = &defCausumnEvaluator{inputIdxToOutputIdxes: make(map[int][]int)}
			}
			inputIdx, outputIdx := defCaus.Index, i
			e.defCausumnEvaluator.inputIdxToOutputIdxes[inputIdx] = append(e.defCausumnEvaluator.inputIdxToOutputIdxes[inputIdx], outputIdx)
			continue
		}
		if e.defaultEvaluator == nil {
			e.defaultEvaluator = &defaultEvaluator{
				outputIdxes: make([]int, 0, len(exprs)),
				exprs:       make([]Expression, 0, len(exprs)),
			}
		}
		e.defaultEvaluator.exprs = append(e.defaultEvaluator.exprs, exprs[i])
		e.defaultEvaluator.outputIdxes = append(e.defaultEvaluator.outputIdxes, i)
	}

	if e.defaultEvaluator != nil {
		e.defaultEvaluator.vectorizable = Vectorizable(e.defaultEvaluator.exprs)
	}
	return e
}

// Vectorizable checks whether this EvaluatorSuite can use vectorizd execution mode.
func (e *EvaluatorSuite) Vectorizable() bool {
	return e.defaultEvaluator == nil || e.defaultEvaluator.vectorizable
}

// Run evaluates all the expressions hold by this EvaluatorSuite.
// NOTE: "defaultEvaluator" must be evaluated before "defCausumnEvaluator".
func (e *EvaluatorSuite) Run(ctx stochastikctx.Context, input, output *chunk.Chunk) error {
	if e.defaultEvaluator != nil {
		err := e.defaultEvaluator.run(ctx, input, output)
		if err != nil {
			return err
		}
	}

	if e.defCausumnEvaluator != nil {
		return e.defCausumnEvaluator.run(ctx, input, output)
	}
	return nil
}
