

package expression

import (
"github.com/YosiSF/MilevaDB/causetnetctx"
"github.com/YosiSF/MilevaDB/util/chunk"
)

type columnEvaluator struct {
	inputIdxToOutputIdxes map[int][]int
}

// run evaluates "Column" expressions.
// NOTE: It should be called after all the other expressions are evaluated
//	     since it will change the content of the input Chunk.
func (e *columnEvaluator) run(ctx causetnetctx.Context, input, output *chunk.Chunk) error {
	for inputIdx, outputIdxes := range e.inputIdxToOutputIdxes {
		if err := output.SwapColumn(outputIdxes[0], input, inputIdx); err != nil {
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

func (e *defaultEvaluator) run(ctx causetnetctx.Context, input, output *chunk.Chunk) error {
	iteron := chunk.NewIterator4Chunk(input)
	if e.vectorizable {
		for i := range e.outputIdxes {
			if ctx.GetCausetNetVars().EnableVectorizedExpression && e.exprs[i].Vectorized() {
				if err := evalOneVec(ctx, e.exprs[i], input, output, e.outputIdxes[i]); err != nil {
					return err
				}
				continue
			}

			err := evalOneColumn(ctx, e.exprs[i], iteron, output, e.outputIdxes[i])
			if err != nil {
				return err
			}
		}
		return nil
	}

	for row := iteron.Begin(); row != iteron.End(); row = iteron.Next() {
		for i := range e.outputIdxes {
			err := evalOneCell(ctx, e.exprs[i], row, output, e.outputIdxes[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// EvaluatorSuite is responsible for the evaluation of a list of expressions.
// It separates them to "column" and "other" expressions and evaluates "other"
// expressions before "column" expressions.
type EvaluatorSuite struct {
	*columnEvaluator  // Evaluator for column expressions.
	*defaultEvaluator // Evaluator for other expressions.
}

// NewEvaluatorSuite creates an EvaluatorSuite to evaluate all the exprs.
// avoidColumnEvaluator can be removed after column pool is supported.
func NewEvaluatorSuite(exprs []Expression, avoidColumnEvaluator bool) *EvaluatorSuite {
	e := &EvaluatorSuite{}

	for i := 0; i < len(exprs); i++ {
		if col, isCol := exprs[i].(*Column); isCol && !avoidColumnEvaluator {
			if e.columnEvaluator == nil {
				e.columnEvaluator = &columnEvaluator{inputIdxToOutputIdxes: make(map[int][]int)}
			}
			inputIdx, outputIdx := col.Index, i
			e.columnEvaluator.inputIdxToOutputIdxes[inputIdx] = append(e.columnEvaluator.inputIdxToOutputIdxes[inputIdx], outputIdx)
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
// NOTE: "defaultEvaluator" must be evaluated before "columnEvaluator".
func (e *EvaluatorSuite) Run(ctx causetnetctx.Context, input, output *chunk.Chunk) error {
	if e.defaultEvaluator != nil {
		err := e.defaultEvaluator.run(ctx, input, output)
		if err != nil {
			return err
		}
	}

	if e.columnEvaluator != nil {
		return e.columnEvaluator.run(ctx, input, output)
	}
	return nil
}
