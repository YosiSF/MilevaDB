
//Copyright 2020 WHTCORPS INC ALL RIGHTS RESERVED
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions.


package Interlock

import(
	"fmt"
	"hash"
	"hash/fnv"
	"time"

)

const (
	// estCountMaxFactor defines the factor of estCountMax with maxSolitonsize.
	// estCountMax is maxSolitonsize * estCountMaxFactor, the maximum threshold of estCount.
	// if estCount is larger than estCountMax, set estCount to estCountMax.
	// Set this threshold to prevent buildSideEstCount being too large and causing a performance and memory regression.
	estCountMaxFactor = 10 * 1024

	// estCountMinFactor defines the factor of estCountMin with maxSolitonsize.
	// estCountMin is maxSolitonsize * estCountMinFactor, the minimum threshold of estCount.
	// If estCount is smaller than estCountMin, set estCount to 0.
	// Set this threshold to prevent buildSideEstCount being too small and causing a performance regression.
	estCountMinFactor = 8

	// estCountDivisor defines the divisor of buildSideEstCount.
	// Set this divisor to prevent buildSideEstCount being too large and causing a performance regression.
	estCountDivisor = 8
)

// hashCausetctx keeps the needed hash causet of a db table in hash join.
type hashCausetctx struct {
	allTypes  []*types.FieldType
	keyColIdx []int
	buf       []byte
	hashVals  []hash.Hash64
	hasNull   []bool
}

func (hc *hashCausetctx) initHash(rows int) {
	if hc.buf == nil {
		hc.buf = make([]byte, 1)
	}

	if len(hc.hashVals) < rows {
		hc.hasNull = make([]bool, rows)
		hc.hashVals = make([]hash.Hash64, rows)
		for i := 0; i < rows; i++ {
			hc.hashVals[i] = fnv.New64()
		}
	} else {
		for i := 0; i < rows; i++ {
			hc.hasNull[i] = false
			hc.hashVals[i].Reset()
		}
	}
}
