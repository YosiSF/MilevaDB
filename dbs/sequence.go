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

package dbs

import (
	"math"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs/soliton"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	math2 "github.com/whtcorpsinc/MilevaDB-Prod/soliton/math"
)

func onCreateSequence(d *dbsCtx, t *meta.Meta, job *perceptron.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tbInfo := &perceptron.BlockInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tbInfo.State = perceptron.StateNone
	err := checkBlockNotExists(d, t, schemaID, tbInfo.Name.L)
	if err != nil {
		if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockExists.Equal(err) {
			job.State = perceptron.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	ver, err = uFIDelateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch tbInfo.State {
	case perceptron.StateNone:
		// none -> public
		tbInfo.State = perceptron.StatePublic
		tbInfo.UFIDelateTS = t.StartTS
		err = createSequenceWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, tbInfo)
		asyncNotifyEvent(d, &soliton.Event{Tp: perceptron.CausetActionCreateSequence, BlockInfo: tbInfo})
		return ver, nil
	default:
		return ver, ErrInvalidDBSState.GenWithStackByArgs("sequence", tbInfo.State)
	}
}

func createSequenceWithCheck(t *meta.Meta, job *perceptron.Job, schemaID int64, tbInfo *perceptron.BlockInfo) error {
	err := checkBlockInfoValid(tbInfo)
	if err != nil {
		job.State = perceptron.JobStateCancelled
		return errors.Trace(err)
	}
	var sequenceBase int64
	if tbInfo.Sequence.Increment >= 0 {
		sequenceBase = tbInfo.Sequence.Start - 1
	} else {
		sequenceBase = tbInfo.Sequence.Start + 1
	}
	return t.CreateSequenceAndSetSeqValue(schemaID, tbInfo, sequenceBase)
}

func handleSequenceOptions(SeqOptions []*ast.SequenceOption, sequenceInfo *perceptron.SequenceInfo) {
	var (
		minSetFlag   bool
		maxSetFlag   bool
		startSetFlag bool
	)
	for _, op := range SeqOptions {
		switch op.Tp {
		case ast.SequenceOptionIncrementBy:
			sequenceInfo.Increment = op.IntValue
		case ast.SequenceStartWith:
			sequenceInfo.Start = op.IntValue
			startSetFlag = true
		case ast.SequenceMinValue:
			sequenceInfo.MinValue = op.IntValue
			minSetFlag = true
		case ast.SequenceMaxValue:
			sequenceInfo.MaxValue = op.IntValue
			maxSetFlag = true
		case ast.SequenceCache:
			sequenceInfo.CacheValue = op.IntValue
		case ast.SequenceNoCache:
			sequenceInfo.Cache = false
		case ast.SequenceCycle:
			sequenceInfo.Cycle = true
		case ast.SequenceNoCycle:
			sequenceInfo.Cycle = false
		}
	}
	// Fill the default value, min/max/start should be adjusted with the sign of sequenceInfo.Increment.
	if !(minSetFlag && maxSetFlag && startSetFlag) {
		if sequenceInfo.Increment >= 0 {
			if !minSetFlag {
				sequenceInfo.MinValue = perceptron.DefaultPositiveSequenceMinValue
			}
			if !startSetFlag {
				sequenceInfo.Start = mathutil.MaxInt64(sequenceInfo.MinValue, perceptron.DefaultPositiveSequenceStartValue)
			}
			if !maxSetFlag {
				sequenceInfo.MaxValue = perceptron.DefaultPositiveSequenceMaxValue
			}
		} else {
			if !maxSetFlag {
				sequenceInfo.MaxValue = perceptron.DefaultNegativeSequenceMaxValue
			}
			if !startSetFlag {
				sequenceInfo.Start = mathutil.MinInt64(sequenceInfo.MaxValue, perceptron.DefaultNegativeSequenceStartValue)
			}
			if !minSetFlag {
				sequenceInfo.MinValue = perceptron.DefaultNegativeSequenceMinValue
			}
		}
	}
}

func validateSequenceOptions(seqInfo *perceptron.SequenceInfo) bool {
	// To ensure that cache * increment will never overflows.
	var maxIncrement int64
	if seqInfo.Increment == 0 {
		// Increment shouldn't be set as 0.
		return false
	}
	if seqInfo.CacheValue <= 0 {
		// Cache value should be bigger than 0.
		return false
	}
	maxIncrement = math2.Abs(seqInfo.Increment)

	return seqInfo.MaxValue >= seqInfo.Start &&
		seqInfo.MaxValue > seqInfo.MinValue &&
		seqInfo.Start >= seqInfo.MinValue &&
		seqInfo.MaxValue != math.MaxInt64 &&
		seqInfo.MinValue != math.MinInt64 &&
		seqInfo.CacheValue < (math.MaxInt64-maxIncrement)/maxIncrement
}

func buildSequenceInfo(stmt *ast.CreateSequenceStmt, ident ast.Ident) (*perceptron.SequenceInfo, error) {
	sequenceInfo := &perceptron.SequenceInfo{
		Cache:      perceptron.DefaultSequenceCacheBool,
		Cycle:      perceptron.DefaultSequenceCycleBool,
		CacheValue: perceptron.DefaultSequenceCacheValue,
		Increment:  perceptron.DefaultSequenceIncrementValue,
	}

	// Handle block comment options.
	for _, op := range stmt.TblOptions {
		switch op.Tp {
		case ast.BlockOptionComment:
			sequenceInfo.Comment = op.StrValue
		case ast.BlockOptionEngine:
			// BlockOptionEngine will always be 'InnoDB', thus we do nothing in this branch to avoid error happening.
		default:
			return nil, ErrSequenceUnsupportedBlockOption.GenWithStackByArgs(op.StrValue)
		}
	}
	handleSequenceOptions(stmt.SeqOptions, sequenceInfo)
	if !validateSequenceOptions(sequenceInfo) {
		return nil, ErrSequenceInvalidData.GenWithStackByArgs(ident.Schema.L, ident.Name.L)
	}
	return sequenceInfo, nil
}
