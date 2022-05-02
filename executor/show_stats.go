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

package executor

import (
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/statistics"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/errors"
)

func (e *ShowExec) fetchShowStatsMeta() error {
	do := petri.GetPetri(e.ctx)
	h := do.StatsHandle()
	dbs := do.SchemaReplicant().AllSchemas()
	for _, EDB := range dbs {
		for _, tbl := range EDB.Blocks {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				e.appendBlockForStatsMeta(EDB.Name.O, tbl.Name.O, "", h.GetBlockStats(tbl))
			} else {
				for _, def := range pi.Definitions {
					e.appendBlockForStatsMeta(EDB.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendBlockForStatsMeta(dbName, tblName, partitionName string, statsTbl *statistics.Block) {
	if statsTbl.Pseudo {
		return
	}
	e.appendEvent([]interface{}{
		dbName,
		tblName,
		partitionName,
		e.versionToTime(statsTbl.Version),
		statsTbl.ModifyCount,
		statsTbl.Count,
	})
}

func (e *ShowExec) fetchShowStatsHistogram() error {
	do := petri.GetPetri(e.ctx)
	h := do.StatsHandle()
	dbs := do.SchemaReplicant().AllSchemas()
	for _, EDB := range dbs {
		for _, tbl := range EDB.Blocks {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				e.appendBlockForStatsHistograms(EDB.Name.O, tbl.Name.O, "", h.GetBlockStats(tbl))
			} else {
				for _, def := range pi.Definitions {
					e.appendBlockForStatsHistograms(EDB.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendBlockForStatsHistograms(dbName, tblName, partitionName string, statsTbl *statistics.Block) {
	if statsTbl.Pseudo {
		return
	}
	for _, defCaus := range statsTbl.DeferredCausets {
		// Pass a nil StatementContext to avoid defCausumn stats being marked as needed.
		if defCaus.IsInvalid(nil, false) {
			continue
		}
		e.histogramToEvent(dbName, tblName, partitionName, defCaus.Info.Name.O, 0, defCaus.Histogram, defCaus.AvgDefCausSize(statsTbl.Count, false))
	}
	for _, idx := range statsTbl.Indices {
		e.histogramToEvent(dbName, tblName, partitionName, idx.Info.Name.O, 1, idx.Histogram, 0)
	}
}

func (e *ShowExec) histogramToEvent(dbName, tblName, partitionName, defCausName string, isIndex int, hist statistics.Histogram, avgDefCausSize float64) {
	e.appendEvent([]interface{}{
		dbName,
		tblName,
		partitionName,
		defCausName,
		isIndex,
		e.versionToTime(hist.LastUFIDelateVersion),
		hist.NDV,
		hist.NullCount,
		avgDefCausSize,
		hist.Correlation,
	})
}

func (e *ShowExec) versionToTime(version uint64) types.Time {
	t := time.Unix(0, oracle.ExtractPhysical(version)*int64(time.Millisecond))
	return types.NewTime(types.FromGoTime(t), allegrosql.TypeDatetime, 0)
}

func (e *ShowExec) fetchShowStatsBuckets() error {
	do := petri.GetPetri(e.ctx)
	h := do.StatsHandle()
	dbs := do.SchemaReplicant().AllSchemas()
	for _, EDB := range dbs {
		for _, tbl := range EDB.Blocks {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				if err := e.appendBlockForStatsBuckets(EDB.Name.O, tbl.Name.O, "", h.GetBlockStats(tbl)); err != nil {
					return err
				}
			} else {
				for _, def := range pi.Definitions {
					if err := e.appendBlockForStatsBuckets(EDB.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (e *ShowExec) appendBlockForStatsBuckets(dbName, tblName, partitionName string, statsTbl *statistics.Block) error {
	if statsTbl.Pseudo {
		return nil
	}
	defCausNameToType := make(map[string]byte, len(statsTbl.DeferredCausets))
	for _, defCaus := range statsTbl.DeferredCausets {
		err := e.bucketsToEvents(dbName, tblName, partitionName, defCaus.Info.Name.O, 0, defCaus.Histogram, nil)
		if err != nil {
			return errors.Trace(err)
		}
		defCausNameToType[defCaus.Info.Name.O] = defCaus.Histogram.Tp.Tp
	}
	for _, idx := range statsTbl.Indices {
		idxDeferredCausetTypes := make([]byte, 0, len(idx.Info.DeferredCausets))
		for i := 0; i < len(idx.Info.DeferredCausets); i++ {
			idxDeferredCausetTypes = append(idxDeferredCausetTypes, defCausNameToType[idx.Info.DeferredCausets[i].Name.O])
		}
		err := e.bucketsToEvents(dbName, tblName, partitionName, idx.Info.Name.O, len(idx.Info.DeferredCausets), idx.Histogram, idxDeferredCausetTypes)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// bucketsToEvents converts histogram buckets to rows. If the histogram is built from index, then numOfDefCauss equals to number
// of index defCausumns, else numOfDefCauss is 0.
func (e *ShowExec) bucketsToEvents(dbName, tblName, partitionName, defCausName string, numOfDefCauss int, hist statistics.Histogram, idxDeferredCausetTypes []byte) error {
	isIndex := 0
	if numOfDefCauss > 0 {
		isIndex = 1
	}
	for i := 0; i < hist.Len(); i++ {
		lowerBoundStr, err := statistics.ValueToString(e.ctx.GetStochaseinstein_dbars(), hist.GetLower(i), numOfDefCauss, idxDeferredCausetTypes)
		if err != nil {
			return errors.Trace(err)
		}
		upperBoundStr, err := statistics.ValueToString(e.ctx.GetStochaseinstein_dbars(), hist.GetUpper(i), numOfDefCauss, idxDeferredCausetTypes)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendEvent([]interface{}{
			dbName,
			tblName,
			partitionName,
			defCausName,
			isIndex,
			i,
			hist.Buckets[i].Count,
			hist.Buckets[i].Repeat,
			lowerBoundStr,
			upperBoundStr,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowStatsHealthy() {
	do := petri.GetPetri(e.ctx)
	h := do.StatsHandle()
	dbs := do.SchemaReplicant().AllSchemas()
	for _, EDB := range dbs {
		for _, tbl := range EDB.Blocks {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				e.appendBlockForStatsHealthy(EDB.Name.O, tbl.Name.O, "", h.GetBlockStats(tbl))
			} else {
				for _, def := range pi.Definitions {
					e.appendBlockForStatsHealthy(EDB.Name.O, tbl.Name.O, def.Name.O, h.GetPartitionStats(tbl, def.ID))
				}
			}
		}
	}
}

func (e *ShowExec) appendBlockForStatsHealthy(dbName, tblName, partitionName string, statsTbl *statistics.Block) {
	if statsTbl.Pseudo {
		return
	}
	var healthy int64
	if statsTbl.ModifyCount < statsTbl.Count {
		healthy = int64((1.0 - float64(statsTbl.ModifyCount)/float64(statsTbl.Count)) * 100.0)
	} else if statsTbl.ModifyCount == 0 {
		healthy = 100
	}
	e.appendEvent([]interface{}{
		dbName,
		tblName,
		partitionName,
		healthy,
	})
}

func (e *ShowExec) fetchShowAnalyzeStatus() {
	rows := dataForAnalyzeStatusHelper(e.baseExecutor.ctx)
	for _, event := range rows {
		for i, val := range event {
			e.result.AppendCauset(i, &val)
		}
	}
}
