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

package executor

import (
	"context"
	"sort"

	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

type inspectionMemruleRetriever struct {
	dummyCloser
	retrieved bool
	extractor *plannercore.InspectionMemruleBlockExtractor
}

const (
	inspectionMemruleTypeInspection string = "inspection"
	inspectionMemruleTypeSummary    string = "summary"
)

func (e *inspectionMemruleRetriever) retrieve(ctx context.Context, sctx stochastikctx.Context) ([][]types.Causet, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true

	tps := inspectionFilter{set: e.extractor.Types}
	var finalEvents [][]types.Causet

	// Select inspection rules
	if tps.enable(inspectionMemruleTypeInspection) {
		for _, r := range inspectionMemrules {
			finalEvents = append(finalEvents, types.MakeCausets(
				r.name(),
				inspectionMemruleTypeInspection,
				// TODO: add rule explanation
				"",
			))
		}
	}
	// Select summary rules
	if tps.enable(inspectionMemruleTypeSummary) {
		// Get ordered key of map inspectionSummaryMemrules
		summaryMemrules := make([]string, 0)
		for rule := range inspectionSummaryMemrules {
			summaryMemrules = append(summaryMemrules, rule)
		}
		sort.Strings(summaryMemrules)

		for _, rule := range summaryMemrules {
			finalEvents = append(finalEvents, types.MakeCausets(
				rule,
				inspectionMemruleTypeSummary,
				// TODO: add rule explanation
				"",
			))
		}
	}
	return finalEvents, nil
}
