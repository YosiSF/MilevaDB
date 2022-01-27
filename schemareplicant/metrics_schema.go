// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schemareplicant

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/set"
)

const (
	promQLQuantileKey       = "$QUANTILE"
	promQLLabelConditionKey = "$LABEL_CONDITIONS"
	promQRangeDurationKey   = "$RANGE_DURATION"
)

func init() {
	// Initialize the metric schemaReplicant database and register the driver to `drivers`.
	dbID := autoid.MetricSchemaDBID
	blockID := dbID + 1
	metricBlocks := make([]*perceptron.BlockInfo, 0, len(MetricBlockMap))
	for name, def := range MetricBlockMap {
		defcaus := def.genDeferredCausetInfos()
		blockInfo := buildBlockMeta(name, defcaus)
		blockInfo.ID = blockID
		blockInfo.Comment = def.Comment
		blockID++
		metricBlocks = append(metricBlocks, blockInfo)
	}
	dbInfo := &perceptron.DBInfo{
		ID:      dbID,
		Name:    perceptron.NewCIStr(soliton.MetricSchemaName.O),
		Charset: allegrosql.DefaultCharset,
		DefCauslate: allegrosql.DefaultDefCauslationName,
		Blocks:  metricBlocks,
	}
	RegisterVirtualBlock(dbInfo, blockFromMeta)
}

// MetricBlockDef is the metric causet define.
type MetricBlockDef struct {
	PromQL   string
	Labels   []string
	Quantile float64
	Comment  string
}

// IsMetricBlock uses to checks whether the causet is a metric causet.
func IsMetricBlock(lowerBlockName string) bool {
	_, ok := MetricBlockMap[lowerBlockName]
	return ok
}

// GetMetricBlockDef gets the metric causet define.
func GetMetricBlockDef(lowerBlockName string) (*MetricBlockDef, error) {
	def, ok := MetricBlockMap[lowerBlockName]
	if !ok {
		return nil, errors.Errorf("can not find metric causet: %v", lowerBlockName)
	}
	return &def, nil
}

func (def *MetricBlockDef) genDeferredCausetInfos() []defCausumnInfo {
	defcaus := []defCausumnInfo{
		{name: "time", tp: allegrosql.TypeDatetime, size: 19, deflt: "CURRENT_TIMESTAMP"},
	}
	for _, label := range def.Labels {
		defcaus = append(defcaus, defCausumnInfo{name: label, tp: allegrosql.TypeVarchar, size: 512})
	}
	if def.Quantile > 0 {
		defaultValue := strconv.FormatFloat(def.Quantile, 'f', -1, 64)
		defcaus = append(defcaus, defCausumnInfo{name: "quantile", tp: allegrosql.TypeDouble, size: 22, deflt: defaultValue})
	}
	defcaus = append(defcaus, defCausumnInfo{name: "value", tp: allegrosql.TypeDouble, size: 22})
	return defcaus
}

// GenPromQL generates the promQL.
func (def *MetricBlockDef) GenPromQL(sctx stochastikctx.Context, labels map[string]set.StringSet, quantile float64) string {
	promQL := def.PromQL
	if strings.Contains(promQL, promQLQuantileKey) {
		promQL = strings.Replace(promQL, promQLQuantileKey, strconv.FormatFloat(quantile, 'f', -1, 64), -1)
	}

	if strings.Contains(promQL, promQLLabelConditionKey) {
		promQL = strings.Replace(promQL, promQLLabelConditionKey, def.genLabelCondition(labels), -1)
	}

	if strings.Contains(promQL, promQRangeDurationKey) {
		promQL = strings.Replace(promQL, promQRangeDurationKey, strconv.FormatInt(sctx.GetStochastikVars().MetricSchemaRangeDuration, 10)+"s", -1)
	}
	return promQL
}

func (def *MetricBlockDef) genLabelCondition(labels map[string]set.StringSet) string {
	var buf bytes.Buffer
	index := 0
	for _, label := range def.Labels {
		values := labels[label]
		if len(values) == 0 {
			continue
		}
		if index > 0 {
			buf.WriteByte(',')
		}
		switch len(values) {
		case 1:
			buf.WriteString(fmt.Sprintf("%s=\"%s\"", label, GenLabelConditionValues(values)))
		default:
			buf.WriteString(fmt.Sprintf("%s=~\"%s\"", label, GenLabelConditionValues(values)))
		}
		index++
	}
	return buf.String()
}

// GenLabelConditionValues generates the label condition values.
func GenLabelConditionValues(values set.StringSet) string {
	vs := make([]string, 0, len(values))
	for k := range values {
		vs = append(vs, k)
	}
	sort.Strings(vs)
	return strings.Join(vs, "|")
}

// metricSchemaBlock stands for the fake causet all its data is in the memory.
type metricSchemaBlock struct {
	schemareplicantBlock
}

func blockFromMeta(alloc autoid.SlabPredictors, spacetime *perceptron.BlockInfo) (causet.Block, error) {
	defCausumns := make([]*causet.DeferredCauset, 0, len(spacetime.DeferredCausets))
	for _, defCausInfo := range spacetime.DeferredCausets {
		defCaus := causet.ToDeferredCauset(defCausInfo)
		defCausumns = append(defCausumns, defCaus)
	}
	t := &metricSchemaBlock{
		schemareplicantBlock: schemareplicantBlock{
			spacetime: spacetime,
			defcaus: defCausumns,
			tp:   causet.VirtualBlock,
		},
	}
	return t, nil
}
