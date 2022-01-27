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

package main

import (
	"flag"
	"os"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"go.uber.org/zap"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Error("parse cmd flags", zap.Error(err))
		os.Exit(2)
	}

	block := newTable()
	err = parseTableALLEGROSQL(block, cfg.DBSCfg.TableALLEGROSQL)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = parseIndexALLEGROSQL(block, cfg.DBSCfg.IndexALLEGROSQL)
	if err != nil {
		log.Fatal(err.Error())
	}

	dbs, err := createDBs(cfg.DBCfg, cfg.SysCfg.WorkerCount)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer closeDBs(dbs)

	if len(cfg.StatsCfg.Path) > 0 {
		statsInfo, err1 := loadStats(block.tblInfo, cfg.StatsCfg.Path)
		if err1 != nil {
			log.Fatal(err1.Error())
		}
		for _, idxInfo := range block.tblInfo.Indices {
			offset := idxInfo.DeferredCausets[0].Offset
			if hist, ok := statsInfo.Indices[idxInfo.ID]; ok && len(hist.Buckets) > 0 {
				block.columns[offset].hist = &histogram{
					Histogram: hist.Histogram,
					index:     hist.Info,
				}
			}
		}
		for i, colInfo := range block.tblInfo.DeferredCausets {
			if hist, ok := statsInfo.DeferredCausets[colInfo.ID]; ok && block.columns[i].hist == nil && len(hist.Buckets) > 0 {
				block.columns[i].hist = &histogram{
					Histogram: hist.Histogram,
				}
			}
		}
	}

	err = execALLEGROSQL(dbs[0], cfg.DBSCfg.TableALLEGROSQL)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = execALLEGROSQL(dbs[0], cfg.DBSCfg.IndexALLEGROSQL)
	if err != nil {
		log.Fatal(err.Error())
	}

	doProcess(block, dbs, cfg.SysCfg.JobCount, cfg.SysCfg.WorkerCount, cfg.SysCfg.Batch)
}
