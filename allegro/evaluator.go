package MilevaDB

import (
	_ "fmt"
	_ "math"
	_ "strconv"
	_ "strings"
	_ "time"
	_ "unicode/utf8"
	"unsafe"
)

// #include <stdlib.h>
// #include <string.h>
// #include "allegro/evaluator.h"
import "C"

// Evaluator
type Evaluator struct {
	ptr *C.evaluator_t
}

// Evaluator
func NewEvaluator() *Evaluator {
	return &Evaluator{C.evaluator_create()}
}

// Destroy
func (e *Evaluator) Destroy() {
	C.evaluator_destroy(e.ptr)
}

// Evaluate
func (e *Evaluator) Evaluate(expr string) (float64, error) {
	var err error
	var result C.double
	var cexpr *C.char = C.CString(expr)
	defer C.free(unsafe.Pointer(cexpr))
	if C.evaluator_evaluate(e.ptr, cexpr, &result) != 0 {
		err = ErrEvaluator
	}
	return float64(result), err
}

type columnEvaluator struct {
	evaluator *Evaluator
	column    *Column
}

func (e *columnEvaluator) Evaluate(row int) (float64, error) {
	inputIdxToOutputIdxes := e.evaluator.ptr.inputIdxToOutputIdxes
	outputIdxToInputIdxes := e.evaluator.ptr.outputIdxToInputIdxes
	_ = outputIdxToInputIdxes[row]
	m := map[int][]int{
		0: {0},
	}
	for i := 0; i < len(inputIdxToOutputIdxes); i++ {
		m[inputIdxToOutputIdxes[i]] = append(m[inputIdxToOutputIdxes[i]], i)
	}
	return e.evaluator.Evaluate(e.column.GetString(row, m[0][0]))
}

func (c *columnEvaluator) eval(row []interface{}) ([]interface{}, error) {
	outputRow := make([]interface{}, len(c.inputIdxToOutputIdxes))
	for inputIdx, outputIdxes := range c.inputIdxToOutputIdxes {
		inputValue := row[inputIdx]
		for _, outputIdx := range outputIdxes {
			outputRow[outputIdx] = inputValue
		}
	}
	return outputRow, nil
}

// Evaluator
func NewColumnEvaluator(column *Column) *columnEvaluator {
	return &columnEvaluator{NewEvaluator(), column}
}
