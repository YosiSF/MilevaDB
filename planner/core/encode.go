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
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"sync"

	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
)

var encoderPool = sync.Pool{
	New: func() interface{} {
		return &planEncoder{}
	},
}

type planEncoder struct {
	buf          bytes.Buffer
	encodedPlans map[int]bool
}

// EncodePlan is used to encodePlan the plan to the plan tree with compressing.
func EncodePlan(p Plan) string {
	pn := encoderPool.Get().(*planEncoder)
	defer encoderPool.Put(pn)
	if p == nil || p.SCtx() == nil {
		return ""
	}
	selectPlan := getSelectPlan(p)
	if selectPlan != nil {
		failpoint.Inject("mockPlanRowCount", func(val failpoint.Value) {
			selectPlan.statsInfo().RowCount = float64(val.(int))
		})
	}
	return pn.encodePlanTree(p)
}

func (pn *planEncoder) encodePlanTree(p Plan) string {
	pn.encodedPlans = make(map[int]bool)
	pn.buf.Reset()
	pn.encodePlan(p, true, ekv.EinsteinDB, 0)
	return plancodec.Compress(pn.buf.Bytes())
}

func (pn *planEncoder) encodePlan(p Plan, isRoot bool, causetstore ekv.StoreType, depth int) {
	taskTypeInfo := plancodec.EncodeTaskType(isRoot, causetstore)
	actRows, analyzeInfo, memoryInfo, diskInfo := getRuntimeInfo(p.SCtx(), p)
	rowCount := 0.0
	if statsInfo := p.statsInfo(); statsInfo != nil {
		rowCount = p.statsInfo().RowCount
	}
	plancodec.EncodePlanNode(depth, p.ID(), p.TP(), rowCount, taskTypeInfo, p.ExplainInfo(), actRows, analyzeInfo, memoryInfo, diskInfo, &pn.buf)
	pn.encodedPlans[p.ID()] = true
	depth++

	selectPlan := getSelectPlan(p)
	if selectPlan == nil {
		return
	}
	if !pn.encodedPlans[selectPlan.ID()] {
		pn.encodePlan(selectPlan, isRoot, causetstore, depth)
		return
	}
	for _, child := range selectPlan.Children() {
		if pn.encodedPlans[child.ID()] {
			continue
		}
		pn.encodePlan(child.(PhysicalPlan), isRoot, causetstore, depth)
	}
	switch INTERLOCKPlan := selectPlan.(type) {
	case *PhysicalBlockReader:
		pn.encodePlan(INTERLOCKPlan.blockPlan, false, INTERLOCKPlan.StoreType, depth)
	case *PhysicalIndexReader:
		pn.encodePlan(INTERLOCKPlan.indexPlan, false, causetstore, depth)
	case *PhysicalIndexLookUpReader:
		pn.encodePlan(INTERLOCKPlan.indexPlan, false, causetstore, depth)
		pn.encodePlan(INTERLOCKPlan.blockPlan, false, causetstore, depth)
	case *PhysicalIndexMergeReader:
		for _, p := range INTERLOCKPlan.partialPlans {
			pn.encodePlan(p, false, causetstore, depth)
		}
		if INTERLOCKPlan.blockPlan != nil {
			pn.encodePlan(INTERLOCKPlan.blockPlan, false, causetstore, depth)
		}
	}
}

var digesterPool = sync.Pool{
	New: func() interface{} {
		return &planDigester{
			hasher: sha256.New(),
		}
	},
}

type planDigester struct {
	buf          bytes.Buffer
	encodedPlans map[int]bool
	hasher       hash.Hash
}

// NormalizePlan is used to normalize the plan and generate plan digest.
func NormalizePlan(p Plan) (normalized, digest string) {
	selectPlan := getSelectPlan(p)
	if selectPlan == nil {
		return "", ""
	}
	d := digesterPool.Get().(*planDigester)
	defer digesterPool.Put(d)
	d.normalizePlanTree(selectPlan)
	normalized = d.buf.String()
	d.hasher.Write(d.buf.Bytes())
	d.buf.Reset()
	digest = fmt.Sprintf("%x", d.hasher.Sum(nil))
	d.hasher.Reset()
	return
}

func (d *planDigester) normalizePlanTree(p PhysicalPlan) {
	d.encodedPlans = make(map[int]bool)
	d.buf.Reset()
	d.normalizePlan(p, true, ekv.EinsteinDB, 0)
}

func (d *planDigester) normalizePlan(p PhysicalPlan, isRoot bool, causetstore ekv.StoreType, depth int) {
	taskTypeInfo := plancodec.EncodeTaskTypeForNormalize(isRoot, causetstore)
	plancodec.NormalizePlanNode(depth, p.TP(), taskTypeInfo, p.ExplainNormalizedInfo(), &d.buf)
	d.encodedPlans[p.ID()] = true

	depth++
	for _, child := range p.Children() {
		if d.encodedPlans[child.ID()] {
			continue
		}
		d.normalizePlan(child.(PhysicalPlan), isRoot, causetstore, depth)
	}
	switch x := p.(type) {
	case *PhysicalBlockReader:
		d.normalizePlan(x.blockPlan, false, x.StoreType, depth)
	case *PhysicalIndexReader:
		d.normalizePlan(x.indexPlan, false, causetstore, depth)
	case *PhysicalIndexLookUpReader:
		d.normalizePlan(x.indexPlan, false, causetstore, depth)
		d.normalizePlan(x.blockPlan, false, causetstore, depth)
	case *PhysicalIndexMergeReader:
		for _, p := range x.partialPlans {
			d.normalizePlan(p, false, causetstore, depth)
		}
		if x.blockPlan != nil {
			d.normalizePlan(x.blockPlan, false, causetstore, depth)
		}
	}
}

func getSelectPlan(p Plan) PhysicalPlan {
	var selectPlan PhysicalPlan
	if physicalPlan, ok := p.(PhysicalPlan); ok {
		selectPlan = physicalPlan
	} else {
		switch x := p.(type) {
		case *Delete:
			selectPlan = x.SelectPlan
		case *UFIDelate:
			selectPlan = x.SelectPlan
		case *Insert:
			selectPlan = x.SelectPlan
		}
	}
	return selectPlan
}
