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
	"context"
	"fmt"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/config"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs"
	"github.com/whtcorpsinc/MilevaDB-Prod/meta"
	"github.com/whtcorpsinc/MilevaDB-Prod/petri"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/core"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/admin"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/gcutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/sqlexec"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx/variable"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

// DBSExec represents a DBS executor.
// It grabs a DBS instance from Petri, calling the DBS methods to do the work.
type DBSExec struct {
	baseExecutor

	stmt ast.StmtNode
	is   schemareplicant.SchemaReplicant
	done bool
}

// toErr converts the error to the ErrSchemaReplicantChanged when the schemaReplicant is outdated.
func (e *DBSExec) toErr(err error) error {
	// The err may be cause by schemaReplicant changed, here we distinguish the ErrSchemaReplicantChanged error from other errors.
	dom := petri.GetPetri(e.ctx)
	checker := petri.NewSchemaChecker(dom, e.is.SchemaMetaVersion(), nil)
	txn, err1 := e.ctx.Txn(true)
	if err1 != nil {
		logutil.BgLogger().Error("active txn failed", zap.Error(err))
		return err1
	}
	_, schemaInfoErr := checker.Check(txn.StartTS())
	if schemaInfoErr != nil {
		return errors.Trace(schemaInfoErr)
	}
	return err
}

// Next implements the Executor Next interface.
func (e *DBSExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	// For each DBS, we should commit the previous transaction and create a new transaction.
	if err = e.ctx.NewTxn(ctx); err != nil {
		return err
	}
	defer func() { e.ctx.GetStochaseinstein_dbars().StmtCtx.IsDBSJobInQueue = false }()

	switch x := e.stmt.(type) {
	case *ast.AlterDatabaseStmt:
		err = e.executeAlterDatabase(x)
	case *ast.AlterBlockStmt:
		err = e.executeAlterBlock(x)
	case *ast.CreateIndexStmt:
		err = e.executeCreateIndex(x)
	case *ast.CreateDatabaseStmt:
		err = e.executeCreateDatabase(x)
	case *ast.CreateBlockStmt:
		err = e.executeCreateBlock(x)
	case *ast.CreateViewStmt:
		err = e.executeCreateView(x)
	case *ast.DropIndexStmt:
		err = e.executeDropIndex(x)
	case *ast.DroFIDelatabaseStmt:
		err = e.executeDroFIDelatabase(x)
	case *ast.DropBlockStmt:
		if x.IsView {
			err = e.executeDropView(x)
		} else {
			err = e.executeDropBlock(x)
		}
	case *ast.RecoverBlockStmt:
		err = e.executeRecoverBlock(x)
	case *ast.FlashBackBlockStmt:
		err = e.executeFlashbackBlock(x)
	case *ast.RenameBlockStmt:
		err = e.executeRenameBlock(x)
	case *ast.TruncateBlockStmt:
		err = e.executeTruncateBlock(x)
	case *ast.LockBlocksStmt:
		err = e.executeLockBlocks(x)
	case *ast.UnlockBlocksStmt:
		err = e.executeUnlockBlocks(x)
	case *ast.CleanupBlockLockStmt:
		err = e.executeCleanupBlockLock(x)
	case *ast.RepairBlockStmt:
		err = e.executeRepairBlock(x)
	case *ast.CreateSequenceStmt:
		err = e.executeCreateSequence(x)
	case *ast.DropSequenceStmt:
		err = e.executeDropSequence(x)

	}
	if err != nil {
		// If the owner return ErrBlockNotExists error when running this DBS, it may be caused by schemaReplicant changed,
		// otherwise, ErrBlockNotExists can be returned before putting this DBS job to the job queue.
		if (e.ctx.GetStochaseinstein_dbars().StmtCtx.IsDBSJobInQueue && schemareplicant.ErrBlockNotExists.Equal(err)) ||
			!e.ctx.GetStochaseinstein_dbars().StmtCtx.IsDBSJobInQueue {
			return e.toErr(err)
		}
		return err

	}

	dom := petri.GetPetri(e.ctx)
	// UFIDelate SchemaReplicant in TxnCtx, so it will pass schemaReplicant check.
	is := dom.SchemaReplicant()
	txnCtx := e.ctx.GetStochaseinstein_dbars().TxnCtx
	txnCtx.SchemaReplicant = is
	txnCtx.SchemaVersion = is.SchemaMetaVersion()
	// DBS will force commit old transaction, after DBS, in transaction status should be false.
	e.ctx.GetStochaseinstein_dbars().SetStatusFlag(allegrosql.ServerStatusInTrans, false)
	return nil
}

func (e *DBSExec) executeTruncateBlock(s *ast.TruncateBlockStmt) error {
	ident := ast.Ident{Schema: s.Block.Schema, Name: s.Block.Name}
	err := petri.GetPetri(e.ctx).DBS().TruncateBlock(e.ctx, ident)
	return err
}

func (e *DBSExec) executeRenameBlock(s *ast.RenameBlockStmt) error {
	if len(s.BlockToBlocks) != 1 {
		// Now we only allow one schemaReplicant changing at the same time.
		return errors.Errorf("can't run multi schemaReplicant change")
	}
	oldIdent := ast.Ident{Schema: s.OldBlock.Schema, Name: s.OldBlock.Name}
	newIdent := ast.Ident{Schema: s.NewBlock.Schema, Name: s.NewBlock.Name}
	isAlterBlock := false
	err := petri.GetPetri(e.ctx).DBS().RenameBlock(e.ctx, oldIdent, newIdent, isAlterBlock)
	return err
}

func (e *DBSExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	var opt *ast.CharsetOpt
	if len(s.Options) != 0 {
		opt = &ast.CharsetOpt{}
		for _, val := range s.Options {
			switch val.Tp {
			case ast.DatabaseOptionCharset:
				opt.Chs = val.Value
			case ast.DatabaseOptionDefCauslate:
				opt.DefCaus = val.Value
			}
		}
	}
	err := petri.GetPetri(e.ctx).DBS().CreateSchema(e.ctx, perceptron.NewCIStr(s.Name), opt)
	if err != nil {
		if schemareplicant.ErrDatabaseExists.Equal(err) && s.IfNotExists {
			err = nil
		}
	}
	return err
}

func (e *DBSExec) executeAlterDatabase(s *ast.AlterDatabaseStmt) error {
	err := petri.GetPetri(e.ctx).DBS().AlterSchema(e.ctx, s)
	return err
}

func (e *DBSExec) executeCreateBlock(s *ast.CreateBlockStmt) error {
	err := petri.GetPetri(e.ctx).DBS().CreateBlock(e.ctx, s)
	return err
}

func (e *DBSExec) executeCreateView(s *ast.CreateViewStmt) error {
	err := petri.GetPetri(e.ctx).DBS().CreateView(e.ctx, s)
	return err
}

func (e *DBSExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: s.Block.Schema, Name: s.Block.Name}
	err := petri.GetPetri(e.ctx).DBS().CreateIndex(e.ctx, ident, s.KeyType, perceptron.NewCIStr(s.IndexName),
		s.IndexPartSpecifications, s.IndexOption, s.IfNotExists)
	return err
}

func (e *DBSExec) executeDroFIDelatabase(s *ast.DroFIDelatabaseStmt) error {
	dbName := perceptron.NewCIStr(s.Name)

	// Protect important system block from been dropped by a mistake.
	// I can hardly find a case that a user really need to do this.
	if dbName.L == "allegrosql" {
		return errors.New("Drop 'allegrosql' database is forbidden")
	}

	err := petri.GetPetri(e.ctx).DBS().DropSchema(e.ctx, dbName)
	if schemareplicant.ErrDatabaseNotExists.Equal(err) {
		if s.IfExists {
			err = nil
		} else {
			err = schemareplicant.ErrDatabaseDropExists.GenWithStackByArgs(s.Name)
		}
	}
	stochaseinstein_dbars := e.ctx.GetStochaseinstein_dbars()
	if err == nil && strings.ToLower(stochaseinstein_dbars.CurrentDB) == dbName.L {
		stochaseinstein_dbars.CurrentDB = ""
		err = variable.SetStochastikSystemVar(stochaseinstein_dbars, variable.CharsetDatabase, types.NewStringCauset(allegrosql.DefaultCharset))
		if err != nil {
			return err
		}
		err = variable.SetStochastikSystemVar(stochaseinstein_dbars, variable.DefCauslationDatabase, types.NewStringCauset(allegrosql.DefaultDefCauslationName))
		if err != nil {
			return err
		}
	}
	return err
}

// If one drop those blocks by mistake, it's difficult to recover.
// In the worst case, the whole MilevaDB cluster fails to bootstrap, so we prevent user from dropping them.
var systemBlocks = map[string]struct{}{
	"milevadb":             {},
	"gc_delete_range":      {},
	"gc_delete_range_done": {},
}

func isSystemBlock(schemaReplicant, block string) bool {
	if schemaReplicant != "allegrosql" {
		return false
	}
	if _, ok := systemBlocks[block]; ok {
		return true
	}
	return false
}

type objectType int

const (
	blockObject objectType = iota
	viewObject
	sequenceObject
)

func (e *DBSExec) executeDropBlock(s *ast.DropBlockStmt) error {
	return e.dropBlockObject(s.Blocks, blockObject, s.IfExists)
}

func (e *DBSExec) executeDropView(s *ast.DropBlockStmt) error {
	return e.dropBlockObject(s.Blocks, viewObject, s.IfExists)
}

func (e *DBSExec) executeDropSequence(s *ast.DropSequenceStmt) error {
	return e.dropBlockObject(s.Sequences, sequenceObject, s.IfExists)
}

// dropBlockObject actually applies to `blockObject`, `viewObject` and `sequenceObject`.
func (e *DBSExec) dropBlockObject(objects []*ast.BlockName, obt objectType, ifExists bool) error {
	var notExistBlocks []string
	for _, tn := range objects {
		fullti := ast.Ident{Schema: tn.Schema, Name: tn.Name}
		_, ok := e.is.SchemaByName(tn.Schema)
		if !ok {
			// TODO: we should return special error for block not exist, checking "not exist" is not enough,
			// because some other errors may contain this error string too.
			notExistBlocks = append(notExistBlocks, fullti.String())
			continue
		}
		_, err := e.is.BlockByName(tn.Schema, tn.Name)
		if err != nil && schemareplicant.ErrBlockNotExists.Equal(err) {
			notExistBlocks = append(notExistBlocks, fullti.String())
			continue
		} else if err != nil {
			return err
		}

		// Protect important system block from been dropped by a mistake.
		// I can hardly find a case that a user really need to do this.
		if isSystemBlock(tn.Schema.L, tn.Name.L) {
			return errors.Errorf("Drop milevadb system block '%s.%s' is forbidden", tn.Schema.L, tn.Name.L)
		}

		if obt == blockObject && config.CheckBlockBeforeDrop {
			logutil.BgLogger().Warn("admin check block before drop",
				zap.String("database", fullti.Schema.O),
				zap.String("block", fullti.Name.O),
			)
			allegrosql := fmt.Sprintf("admin check block `%s`.`%s`", fullti.Schema.O, fullti.Name.O)
			_, _, err = e.ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
			if err != nil {
				return err
			}
		}
		switch obt {
		case blockObject:
			err = petri.GetPetri(e.ctx).DBS().DropBlock(e.ctx, fullti)
		case viewObject:
			err = petri.GetPetri(e.ctx).DBS().DropView(e.ctx, fullti)
		case sequenceObject:
			err = petri.GetPetri(e.ctx).DBS().DropSequence(e.ctx, fullti, ifExists)
		}
		if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockNotExists.Equal(err) {
			notExistBlocks = append(notExistBlocks, fullti.String())
		} else if err != nil {
			return err
		}
	}
	if len(notExistBlocks) > 0 && !ifExists {
		if obt == sequenceObject {
			return schemareplicant.ErrSequenceDropExists.GenWithStackByArgs(strings.Join(notExistBlocks, ","))
		}
		return schemareplicant.ErrBlockDropExists.GenWithStackByArgs(strings.Join(notExistBlocks, ","))
	}
	// We need add warning when use if exists.
	if len(notExistBlocks) > 0 && ifExists {
		for _, block := range notExistBlocks {
			if obt == sequenceObject {
				e.ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(schemareplicant.ErrSequenceDropExists.GenWithStackByArgs(block))
			} else {
				e.ctx.GetStochaseinstein_dbars().StmtCtx.AppendNote(schemareplicant.ErrBlockDropExists.GenWithStackByArgs(block))
			}
		}
	}
	return nil
}

func (e *DBSExec) executeDropIndex(s *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: s.Block.Schema, Name: s.Block.Name}
	err := petri.GetPetri(e.ctx).DBS().DropIndex(e.ctx, ti, perceptron.NewCIStr(s.IndexName), s.IfExists)
	if (schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockNotExists.Equal(err)) && s.IfExists {
		err = nil
	}
	return err
}

func (e *DBSExec) executeAlterBlock(s *ast.AlterBlockStmt) error {
	ti := ast.Ident{Schema: s.Block.Schema, Name: s.Block.Name}
	err := petri.GetPetri(e.ctx).DBS().AlterBlock(e.ctx, ti, s.Specs)
	return err
}

// executeRecoverBlock represents a recover block executor.
// It is built from "recover block" statement,
// is used to recover the block that deleted by mistake.
func (e *DBSExec) executeRecoverBlock(s *ast.RecoverBlockStmt) error {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	t := meta.NewMeta(txn)
	dom := petri.GetPetri(e.ctx)
	var job *perceptron.Job
	var tblInfo *perceptron.BlockInfo
	if s.JobID != 0 {
		job, tblInfo, err = e.getRecoverBlockByJobID(s, t, dom)
	} else {
		job, tblInfo, err = e.getRecoverBlockByBlockName(s.Block)
	}
	if err != nil {
		return err
	}
	// Check the block ID was not exists.
	tbl, ok := dom.SchemaReplicant().BlockByID(tblInfo.ID)
	if ok {
		return schemareplicant.ErrBlockExists.GenWithStack("Block '%-.192s' already been recover to '%-.192s', can't be recover repeatedly", s.Block.Name.O, tbl.Meta().Name.O)
	}

	autoIncID, autoRandID, err := e.getBlockAutoIDsFromSnapshot(job)
	if err != nil {
		return err
	}

	recoverInfo := &dbs.RecoverInfo{
		SchemaID:      job.SchemaID,
		BlockInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		CurAutoIncID:  autoIncID,
		CurAutoRandID: autoRandID,
	}
	// Call DBS RecoverBlock.
	err = petri.GetPetri(e.ctx).DBS().RecoverBlock(e.ctx, recoverInfo)
	return err
}

func (e *DBSExec) getBlockAutoIDsFromSnapshot(job *perceptron.Job) (autoIncID, autoRandID int64, err error) {
	// Get block original autoIDs before block drop.
	dom := petri.GetPetri(e.ctx)
	m, err := dom.GetSnapshotMeta(job.StartTS)
	if err != nil {
		return 0, 0, err
	}
	autoIncID, err = m.GetAutoBlockID(job.SchemaID, job.BlockID)
	if err != nil {
		return 0, 0, errors.Errorf("recover block_id: %d, get original autoIncID from snapshot meta err: %s", job.BlockID, err.Error())
	}
	autoRandID, err = m.GetAutoRandomID(job.SchemaID, job.BlockID)
	if err != nil {
		return 0, 0, errors.Errorf("recover block_id: %d, get original autoRandID from snapshot meta err: %s", job.BlockID, err.Error())
	}
	return autoIncID, autoRandID, nil
}

func (e *DBSExec) getRecoverBlockByJobID(s *ast.RecoverBlockStmt, t *meta.Meta, dom *petri.Petri) (*perceptron.Job, *perceptron.BlockInfo, error) {
	job, err := t.GetHistoryDBSJob(s.JobID)
	if err != nil {
		return nil, nil, err
	}
	if job == nil {
		return nil, nil, admin.ErrDBSJobNotFound.GenWithStackByArgs(s.JobID)
	}
	if job.Type != perceptron.CausetActionDropBlock && job.Type != perceptron.CausetActionTruncateBlock {
		return nil, nil, errors.Errorf("Job %v type is %v, not dropped/truncated block", job.ID, job.Type)
	}

	// Check GC safe point for getting snapshot schemaReplicant.
	err = gcutil.ValidateSnapshot(e.ctx, job.StartTS)
	if err != nil {
		return nil, nil, err
	}

	// Get the snapshot schemaReplicant before drop block.
	snapInfo, err := dom.GetSnapshotSchemaReplicant(job.StartTS)
	if err != nil {
		return nil, nil, err
	}
	// Get block meta from snapshot schemaReplicant.
	block, ok := snapInfo.BlockByID(job.BlockID)
	if !ok {
		return nil, nil, schemareplicant.ErrBlockNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", job.SchemaID),
			fmt.Sprintf("(Block ID %d)", job.BlockID),
		)
	}
	return job, block.Meta(), nil
}

// GetDropOrTruncateBlockInfoFromJobs gets the dropped/truncated block information from DBS jobs,
// it will use the `start_ts` of DBS job as snapshot to get the dropped/truncated block information.
func GetDropOrTruncateBlockInfoFromJobs(jobs []*perceptron.Job, gcSafePoint uint64, dom *petri.Petri, fn func(*perceptron.Job, *perceptron.BlockInfo) (bool, error)) (bool, error) {
	for _, job := range jobs {
		// Check GC safe point for getting snapshot schemaReplicant.
		err := gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
		if err != nil {
			return false, err
		}
		if job.Type != perceptron.CausetActionDropBlock && job.Type != perceptron.CausetActionTruncateBlock {
			continue
		}

		snapMeta, err := dom.GetSnapshotMeta(job.StartTS)
		if err != nil {
			return false, err
		}
		tbl, err := snapMeta.GetBlock(job.SchemaID, job.BlockID)
		if err != nil {
			if meta.ErrDBNotExists.Equal(err) {
				// The dropped/truncated DBS maybe execute failed that caused by the parallel DBS execution,
				// then can't find the block from the snapshot info-schemaReplicant. Should just ignore error here,
				// see more in TestParallelDropSchemaAndDropBlock.
				continue
			}
			return false, err
		}
		if tbl == nil {
			// The dropped/truncated DBS maybe execute failed that caused by the parallel DBS execution,
			// then can't find the block from the snapshot info-schemaReplicant. Should just ignore error here,
			// see more in TestParallelDropSchemaAndDropBlock.
			continue
		}
		finish, err := fn(job, tbl)
		if err != nil || finish {
			return finish, err
		}
	}
	return false, nil
}

func (e *DBSExec) getRecoverBlockByBlockName(blockName *ast.BlockName) (*perceptron.Job, *perceptron.BlockInfo, error) {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return nil, nil, err
	}
	schemaName := blockName.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.ctx.GetStochaseinstein_dbars().CurrentDB)
	}
	if schemaName == "" {
		return nil, nil, errors.Trace(core.ErrNoDB)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(e.ctx)
	if err != nil {
		return nil, nil, err
	}
	var jobInfo *perceptron.Job
	var blockInfo *perceptron.BlockInfo
	dom := petri.GetPetri(e.ctx)
	handleJobAndBlockInfo := func(job *perceptron.Job, tblInfo *perceptron.BlockInfo) (bool, error) {
		if tblInfo.Name.L != blockName.Name.L {
			return false, nil
		}
		schemaReplicant, ok := dom.SchemaReplicant().SchemaByID(job.SchemaID)
		if !ok {
			return false, nil
		}
		if schemaReplicant.Name.L == schemaName {
			blockInfo = tblInfo
			jobInfo = job
			return true, nil
		}
		return false, nil
	}
	fn := func(jobs []*perceptron.Job) (bool, error) {
		return GetDropOrTruncateBlockInfoFromJobs(jobs, gcSafePoint, dom, handleJobAndBlockInfo)
	}
	err = admin.IterHistoryDBSJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			return nil, nil, errors.Errorf("Can't find dropped/truncated block '%s' in GC safe point %s", blockName.Name.O, perceptron.TSConvert2Time(gcSafePoint).String())
		}
		return nil, nil, err
	}
	if blockInfo == nil || jobInfo == nil {
		return nil, nil, errors.Errorf("Can't find dropped/truncated block: %v in DBS history jobs", blockName.Name)
	}
	return jobInfo, blockInfo, nil
}

func (e *DBSExec) executeFlashbackBlock(s *ast.FlashBackBlockStmt) error {
	job, tblInfo, err := e.getRecoverBlockByBlockName(s.Block)
	if err != nil {
		return err
	}
	if len(s.NewName) != 0 {
		tblInfo.Name = perceptron.NewCIStr(s.NewName)
	}
	// Check the block ID was not exists.
	is := petri.GetPetri(e.ctx).SchemaReplicant()
	tbl, ok := is.BlockByID(tblInfo.ID)
	if ok {
		return schemareplicant.ErrBlockExists.GenWithStack("Block '%-.192s' already been flashback to '%-.192s', can't be flashback repeatedly", s.Block.Name.O, tbl.Meta().Name.O)
	}

	autoIncID, autoRandID, err := e.getBlockAutoIDsFromSnapshot(job)
	if err != nil {
		return err
	}
	recoverInfo := &dbs.RecoverInfo{
		SchemaID:      job.SchemaID,
		BlockInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		CurAutoIncID:  autoIncID,
		CurAutoRandID: autoRandID,
	}
	// Call DBS RecoverBlock.
	err = petri.GetPetri(e.ctx).DBS().RecoverBlock(e.ctx, recoverInfo)
	return err
}

func (e *DBSExec) executeLockBlocks(s *ast.LockBlocksStmt) error {
	if !config.BlockLockEnabled() {
		return nil
	}
	return petri.GetPetri(e.ctx).DBS().LockBlocks(e.ctx, s)
}

func (e *DBSExec) executeUnlockBlocks(s *ast.UnlockBlocksStmt) error {
	if !config.BlockLockEnabled() {
		return nil
	}
	lockedBlocks := e.ctx.GetAllBlockLocks()
	return petri.GetPetri(e.ctx).DBS().UnlockBlocks(e.ctx, lockedBlocks)
}

func (e *DBSExec) executeCleanupBlockLock(s *ast.CleanupBlockLockStmt) error {
	return petri.GetPetri(e.ctx).DBS().CleanupBlockLock(e.ctx, s.Blocks)
}

func (e *DBSExec) executeRepairBlock(s *ast.RepairBlockStmt) error {
	return petri.GetPetri(e.ctx).DBS().RepairBlock(e.ctx, s.Block, s.CreateStmt)
}

func (e *DBSExec) executeCreateSequence(s *ast.CreateSequenceStmt) error {
	return petri.GetPetri(e.ctx).DBS().CreateSequence(e.ctx, s)
}
