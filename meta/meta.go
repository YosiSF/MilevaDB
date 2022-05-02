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

package meta

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/ekv"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/structure"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"go.uber.org/zap"
)

var (
	globalIDMutex sync.Mutex
)

// Meta structure:
//	NextGlobalID -> int64
//	SchemaVersion -> int64
//	DBs -> {
//		EDB:1 -> EDB meta data []byte
//		EDB:2 -> EDB meta data []byte
//	}
//	EDB:1 -> {
//		Block:1 -> block meta data []byte
//		Block:2 -> block meta data []byte
//		TID:1 -> int64
//		TID:2 -> int64
//	}
//

var (
	mMetaPrefix       = []byte("m")
	mNextGlobalIDKey  = []byte("NextGlobalID")
	mSchemaVersionKey = []byte("SchemaVersionKey")
	mDBs              = []byte("DBs")
	mDBPrefix         = "EDB"
	mBlockPrefix      = "Block"
	mSequencePrefix   = "SID"
	mSeqCyclePrefix   = "SequenceCycle"
	mBlockIDPrefix    = "TID"
	mRandomIDPrefix   = "TARID"
	mBootstrapKey     = []byte("BootstrapKey")
	mSchemaDiffPrefix = "Diff"
)

var (
	// ErrDBExists is the error for EDB exists.
	ErrDBExists = terror.ClassMeta.New(allegrosql.ErrDBCreateExists, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDBCreateExists])
	// ErrDBNotExists is the error for EDB not exists.
	ErrDBNotExists = terror.ClassMeta.New(allegrosql.ErrBadDB, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadDB])
	// ErrBlockExists is the error for block exists.
	ErrBlockExists = terror.ClassMeta.New(allegrosql.ErrBlockExists, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockExists])
	// ErrBlockNotExists is the error for block not exists.
	ErrBlockNotExists = terror.ClassMeta.New(allegrosql.ErrNoSuchBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNoSuchBlock])
)

// Meta is for handling meta information in a transaction.
type Meta struct {
	txn        *structure.TxStructure
	StartTS    uint64 // StartTS is the txn's start TS.
	jobListKey JobListKeyType
}

// NewMeta creates a Meta in transaction txn.
// If the current Meta needs to handle a job, jobListKey is the type of the job's list.
func NewMeta(txn ekv.Transaction, jobListKeys ...JobListKeyType) *Meta {
	txn.SetOption(ekv.Priority, ekv.PriorityHigh)
	txn.SetOption(ekv.SyncLog, true)
	t := structure.NewStructure(txn, txn, mMetaPrefix)
	listKey := DefaultJobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}
	return &Meta{txn: t,
		StartTS:    txn.StartTS(),
		jobListKey: listKey,
	}
}

// NewSnapshotMeta creates a Meta with snapshot.
func NewSnapshotMeta(snapshot ekv.Snapshot) *Meta {
	t := structure.NewStructure(snapshot, nil, mMetaPrefix)
	return &Meta{txn: t}
}

// GenGlobalID generates next id globally.
func (m *Meta) GenGlobalID() (int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	return m.txn.Inc(mNextGlobalIDKey, 1)
}

// GenGlobalIDs generates the next n global IDs.
func (m *Meta) GenGlobalIDs(n int) ([]int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	newID, err := m.txn.Inc(mNextGlobalIDKey, int64(n))
	if err != nil {
		return nil, err
	}
	origID := newID - int64(n)
	ids := make([]int64, 0, n)
	for i := origID + 1; i <= newID; i++ {
		ids = append(ids, i)
	}
	return ids, nil
}

// GetGlobalID gets current global id.
func (m *Meta) GetGlobalID() (int64, error) {
	return m.txn.GetInt64(mNextGlobalIDKey)
}

func (m *Meta) dbKey(dbID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mDBPrefix, dbID))
}

func (m *Meta) autoBlockIDKey(blockID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mBlockIDPrefix, blockID))
}

func (m *Meta) autoRandomBlockIDKey(blockID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mRandomIDPrefix, blockID))
}

func (m *Meta) blockKey(blockID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mBlockPrefix, blockID))
}

func (m *Meta) sequenceKey(sequenceID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mSequencePrefix, sequenceID))
}

func (m *Meta) sequenceCycleKey(sequenceID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mSeqCyclePrefix, sequenceID))
}

// DBSJobHistoryKey is only used for testing.
func DBSJobHistoryKey(m *Meta, jobID int64) []byte {
	return m.txn.EncodeHashDataKey(mDBSJobHistoryKey, m.jobIDKey(jobID))
}

// GenAutoBlockIDKeyValue generates meta key by dbID, blockID and corresponding value by autoID.
func (m *Meta) GenAutoBlockIDKeyValue(dbID, blockID, autoID int64) (key, value []byte) {
	dbKey := m.dbKey(dbID)
	autoBlockIDKey := m.autoBlockIDKey(blockID)
	return m.txn.EncodeHashAutoIDKeyValue(dbKey, autoBlockIDKey, autoID)
}

// GenAutoBlockID adds step to the auto ID of the block and returns the sum.
func (m *Meta) GenAutoBlockID(dbID, blockID, step int64) (int64, error) {
	// Check if EDB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return 0, errors.Trace(err)
	}
	// Check if block exists.
	blockKey := m.blockKey(blockID)
	if err := m.checkBlockExists(dbKey, blockKey); err != nil {
		return 0, errors.Trace(err)
	}

	return m.txn.HInc(dbKey, m.autoBlockIDKey(blockID), step)
}

// GenAutoRandomID adds step to the auto shard ID of the block and returns the sum.
func (m *Meta) GenAutoRandomID(dbID, blockID, step int64) (int64, error) {
	// Check if EDB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return 0, errors.Trace(err)
	}
	// Check if block exists.
	blockKey := m.blockKey(blockID)
	if err := m.checkBlockExists(dbKey, blockKey); err != nil {
		return 0, errors.Trace(err)
	}

	return m.txn.HInc(dbKey, m.autoRandomBlockIDKey(blockID), step)
}

// GetAutoBlockID gets current auto id with block id.
func (m *Meta) GetAutoBlockID(dbID int64, blockID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.autoBlockIDKey(blockID))
}

// GetAutoRandomID gets current auto random id with block id.
func (m *Meta) GetAutoRandomID(dbID int64, blockID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.autoRandomBlockIDKey(blockID))
}

// GenSequenceValue adds step to the sequence value and returns the sum.
func (m *Meta) GenSequenceValue(dbID, sequenceID, step int64) (int64, error) {
	// Check if EDB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return 0, errors.Trace(err)
	}
	// Check if sequence exists.
	blockKey := m.blockKey(sequenceID)
	if err := m.checkBlockExists(dbKey, blockKey); err != nil {
		return 0, errors.Trace(err)
	}
	return m.txn.HInc(dbKey, m.sequenceKey(sequenceID), step)
}

// GetSequenceValue gets current sequence value with sequence id.
func (m *Meta) GetSequenceValue(dbID int64, sequenceID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.sequenceKey(sequenceID))
}

// SetSequenceValue sets start value when sequence in cycle.
func (m *Meta) SetSequenceValue(dbID int64, sequenceID int64, start int64) error {
	return m.txn.HSet(m.dbKey(dbID), m.sequenceKey(sequenceID), []byte(strconv.FormatInt(start, 10)))
}

// GetSequenceCycle gets current sequence cycle times with sequence id.
func (m *Meta) GetSequenceCycle(dbID int64, sequenceID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.sequenceCycleKey(sequenceID))
}

// SetSequenceCycle sets cycle times value when sequence in cycle.
func (m *Meta) SetSequenceCycle(dbID int64, sequenceID int64, round int64) error {
	return m.txn.HSet(m.dbKey(dbID), m.sequenceCycleKey(sequenceID), []byte(strconv.FormatInt(round, 10)))
}

// GetSchemaVersion gets current global schemaReplicant version.
func (m *Meta) GetSchemaVersion() (int64, error) {
	return m.txn.GetInt64(mSchemaVersionKey)
}

// GenSchemaVersion generates next schemaReplicant version.
func (m *Meta) GenSchemaVersion() (int64, error) {
	return m.txn.Inc(mSchemaVersionKey, 1)
}

func (m *Meta) checkDBExists(dbKey []byte) error {
	v, err := m.txn.HGet(mDBs, dbKey)
	if err == nil && v == nil {
		err = ErrDBNotExists.GenWithStack("database doesn't exist")
	}
	return errors.Trace(err)
}

func (m *Meta) checkDBNotExists(dbKey []byte) error {
	v, err := m.txn.HGet(mDBs, dbKey)
	if err == nil && v != nil {
		err = ErrDBExists.GenWithStack("database already exists")
	}
	return errors.Trace(err)
}

func (m *Meta) checkBlockExists(dbKey []byte, blockKey []byte) error {
	v, err := m.txn.HGet(dbKey, blockKey)
	if err == nil && v == nil {
		err = ErrBlockNotExists.GenWithStack("block doesn't exist")
	}
	return errors.Trace(err)
}

func (m *Meta) checkBlockNotExists(dbKey []byte, blockKey []byte) error {
	v, err := m.txn.HGet(dbKey, blockKey)
	if err == nil && v != nil {
		err = ErrBlockExists.GenWithStack("block already exists")
	}
	return errors.Trace(err)
}

// CreateDatabase creates a database with EDB info.
func (m *Meta) CreateDatabase(dbInfo *perceptron.DBInfo) error {
	dbKey := m.dbKey(dbInfo.ID)

	if err := m.checkDBNotExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(dbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(mDBs, dbKey, data)
}

// UFIDelateDatabase uFIDelates a database with EDB info.
func (m *Meta) UFIDelateDatabase(dbInfo *perceptron.DBInfo) error {
	dbKey := m.dbKey(dbInfo.ID)

	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(dbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(mDBs, dbKey, data)
}

// CreateBlockOrView creates a block with blockInfo in database.
func (m *Meta) CreateBlockOrView(dbID int64, blockInfo *perceptron.BlockInfo) error {
	// Check if EDB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	// Check if block exists.
	blockKey := m.blockKey(blockInfo.ID)
	if err := m.checkBlockNotExists(dbKey, blockKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(blockInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(dbKey, blockKey, data)
}

// CreateBlockAndSetAutoID creates a block with blockInfo in database,
// and rebases the block autoID.
func (m *Meta) CreateBlockAndSetAutoID(dbID int64, blockInfo *perceptron.BlockInfo, autoIncID, autoRandID int64) error {
	err := m.CreateBlockOrView(dbID, blockInfo)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.txn.HInc(m.dbKey(dbID), m.autoBlockIDKey(blockInfo.ID), autoIncID)
	if err != nil {
		return errors.Trace(err)
	}
	if blockInfo.AutoRandomBits > 0 {
		_, err = m.txn.HInc(m.dbKey(dbID), m.autoRandomBlockIDKey(blockInfo.ID), autoRandID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CreateSequenceAndSetSeqValue creates sequence with blockInfo in database, and rebase the sequence seqValue.
func (m *Meta) CreateSequenceAndSetSeqValue(dbID int64, blockInfo *perceptron.BlockInfo, seqValue int64) error {
	err := m.CreateBlockOrView(dbID, blockInfo)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.txn.HInc(m.dbKey(dbID), m.sequenceKey(blockInfo.ID), seqValue)
	return errors.Trace(err)
}

// DroFIDelatabase drops whole database.
func (m *Meta) DroFIDelatabase(dbID int64) error {
	// Check if EDB exists.
	dbKey := m.dbKey(dbID)
	if err := m.txn.HClear(dbKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(mDBs, dbKey); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// DropSequence drops sequence in database.
// Sequence is made of block struct and ekv value pair.
func (m *Meta) DropSequence(dbID int64, tblID int64, delAutoID bool) error {
	err := m.DropBlockOrView(dbID, tblID, delAutoID)
	if err != nil {
		return err
	}
	err = m.txn.HDel(m.dbKey(dbID), m.sequenceKey(tblID))
	return errors.Trace(err)
}

// DropBlockOrView drops block in database.
// If delAutoID is true, it will delete the auto_increment id key-value of the block.
// For rename block, we do not need to rename auto_increment id key-value.
func (m *Meta) DropBlockOrView(dbID int64, tblID int64, delAutoID bool) error {
	// Check if EDB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	// Check if block exists.
	blockKey := m.blockKey(tblID)
	if err := m.checkBlockExists(dbKey, blockKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(dbKey, blockKey); err != nil {
		return errors.Trace(err)
	}
	if delAutoID {
		if err := m.txn.HDel(dbKey, m.autoBlockIDKey(tblID)); err != nil {
			return errors.Trace(err)
		}
		if err := m.txn.HDel(dbKey, m.autoRandomBlockIDKey(tblID)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// UFIDelateBlock uFIDelates the block with block info.
func (m *Meta) UFIDelateBlock(dbID int64, blockInfo *perceptron.BlockInfo) error {
	// Check if EDB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	// Check if block exists.
	blockKey := m.blockKey(blockInfo.ID)
	if err := m.checkBlockExists(dbKey, blockKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(blockInfo)
	if err != nil {
		return errors.Trace(err)
	}

	err = m.txn.HSet(dbKey, blockKey, data)
	return errors.Trace(err)
}

// ListBlocks shows all blocks in database.
func (m *Meta) ListBlocks(dbID int64) ([]*perceptron.BlockInfo, error) {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	res, err := m.txn.HGetAll(dbKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	blocks := make([]*perceptron.BlockInfo, 0, len(res)/2)
	for _, r := range res {
		// only handle block meta
		blockKey := string(r.Field)
		if !strings.HasPrefix(blockKey, mBlockPrefix) {
			continue
		}

		tbInfo := &perceptron.BlockInfo{}
		err = json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}

		blocks = append(blocks, tbInfo)
	}

	return blocks, nil
}

// ListDatabases shows all databases.
func (m *Meta) ListDatabases() ([]*perceptron.DBInfo, error) {
	res, err := m.txn.HGetAll(mDBs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbs := make([]*perceptron.DBInfo, 0, len(res))
	for _, r := range res {
		dbInfo := &perceptron.DBInfo{}
		err = json.Unmarshal(r.Value, dbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dbs = append(dbs, dbInfo)
	}
	return dbs, nil
}

// GetDatabase gets the database value with ID.
func (m *Meta) GetDatabase(dbID int64) (*perceptron.DBInfo, error) {
	dbKey := m.dbKey(dbID)
	value, err := m.txn.HGet(mDBs, dbKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	dbInfo := &perceptron.DBInfo{}
	err = json.Unmarshal(value, dbInfo)
	return dbInfo, errors.Trace(err)
}

// GetBlock gets the block value in database with blockID.
func (m *Meta) GetBlock(dbID int64, blockID int64) (*perceptron.BlockInfo, error) {
	// Check if EDB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	blockKey := m.blockKey(blockID)
	value, err := m.txn.HGet(dbKey, blockKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	blockInfo := &perceptron.BlockInfo{}
	err = json.Unmarshal(value, blockInfo)
	return blockInfo, errors.Trace(err)
}

// DBS job structure
//	DBSJobList: list jobs
//	DBSJobHistory: hash
//	DBSJobReorg: hash
//
// for multi DBS workers, only one can become the owner
// to operate DBS jobs, and dispatch them to MR Jobs.

var (
	mDBSJobListKey    = []byte("DBSJobList")
	mDBSJobAddIdxList = []byte("DBSJobAddIdxList")
	mDBSJobHistoryKey = []byte("DBSJobHistory")
	mDBSJobReorgKey   = []byte("DBSJobReorg")
)

// JobListKeyType is a key type of the DBS job queue.
type JobListKeyType []byte

var (
	// DefaultJobListKey keeps all actions of DBS jobs except "add index".
	DefaultJobListKey JobListKeyType = mDBSJobListKey
	// AddIndexJobListKey only keeps the action of adding index.
	AddIndexJobListKey JobListKeyType = mDBSJobAddIdxList
)

func (m *Meta) enQueueDBSJob(key []byte, job *perceptron.Job) error {
	b, err := job.Encode(true)
	if err == nil {
		err = m.txn.RPush(key, b)
	}
	return errors.Trace(err)
}

// EnQueueDBSJob adds a DBS job to the list.
func (m *Meta) EnQueueDBSJob(job *perceptron.Job, jobListKeys ...JobListKeyType) error {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	return m.enQueueDBSJob(listKey, job)
}

func (m *Meta) deQueueDBSJob(key []byte) (*perceptron.Job, error) {
	value, err := m.txn.LPop(key)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &perceptron.Job{}
	err = job.Decode(value)
	return job, errors.Trace(err)
}

// DeQueueDBSJob pops a DBS job from the list.
func (m *Meta) DeQueueDBSJob() (*perceptron.Job, error) {
	return m.deQueueDBSJob(m.jobListKey)
}

func (m *Meta) getDBSJob(key []byte, index int64) (*perceptron.Job, error) {
	value, err := m.txn.LIndex(key, index)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &perceptron.Job{
		// For compatibility, if the job is enqueued by old version MilevaDB and Priority field is omitted,
		// set the default priority to ekv.PriorityLow.
		Priority: ekv.PriorityLow,
	}
	err = job.Decode(value)
	// Check if the job.Priority is valid.
	if job.Priority < ekv.PriorityNormal || job.Priority > ekv.PriorityHigh {
		job.Priority = ekv.PriorityLow
	}
	return job, errors.Trace(err)
}

// GetDBSJobByIdx returns the corresponding DBS job by the index.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) GetDBSJobByIdx(index int64, jobListKeys ...JobListKeyType) (*perceptron.Job, error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	startTime := time.Now()
	job, err := m.getDBSJob(listKey, index)
	metrics.MetaHistogram.WithLabelValues(metrics.GetDBSJobByIdx, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return job, errors.Trace(err)
}

// uFIDelateDBSJob uFIDelates the DBS job with index and key.
// uFIDelateRawArgs is used to determine whether to uFIDelate the raw args when encode the job.
func (m *Meta) uFIDelateDBSJob(index int64, job *perceptron.Job, key []byte, uFIDelateRawArgs bool) error {
	b, err := job.Encode(uFIDelateRawArgs)
	if err == nil {
		err = m.txn.LSet(key, index, b)
	}
	return errors.Trace(err)
}

// UFIDelateDBSJob uFIDelates the DBS job with index.
// uFIDelateRawArgs is used to determine whether to uFIDelate the raw args when encode the job.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) UFIDelateDBSJob(index int64, job *perceptron.Job, uFIDelateRawArgs bool, jobListKeys ...JobListKeyType) error {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	startTime := time.Now()
	err := m.uFIDelateDBSJob(index, job, listKey, uFIDelateRawArgs)
	metrics.MetaHistogram.WithLabelValues(metrics.UFIDelateDBSJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// DBSJobQueueLen returns the DBS job queue length.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) DBSJobQueueLen(jobListKeys ...JobListKeyType) (int64, error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}
	return m.txn.LLen(listKey)
}

// GetAllDBSJobsInQueue gets all DBS Jobs in the current queue.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) GetAllDBSJobsInQueue(jobListKeys ...JobListKeyType) ([]*perceptron.Job, error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	values, err := m.txn.LGetAll(listKey)
	if err != nil || values == nil {
		return nil, errors.Trace(err)
	}

	jobs := make([]*perceptron.Job, 0, len(values))
	for _, val := range values {
		job := &perceptron.Job{}
		err = job.Decode(val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (m *Meta) jobIDKey(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

func (m *Meta) reorgJobStartHandle(id int64) []byte {
	// There is no "_start", to make it compatible with the older MilevaDB versions.
	return m.jobIDKey(id)
}

func (m *Meta) reorgJobEndHandle(id int64) []byte {
	b := make([]byte, 8, 12)
	binary.BigEndian.PutUint64(b, uint64(id))
	b = append(b, "_end"...)
	return b
}

func (m *Meta) reorgJobPhysicalBlockID(id int64) []byte {
	b := make([]byte, 8, 12)
	binary.BigEndian.PutUint64(b, uint64(id))
	b = append(b, "_pid"...)
	return b
}

func (m *Meta) addHistoryDBSJob(key []byte, job *perceptron.Job, uFIDelateRawArgs bool) error {
	b, err := job.Encode(uFIDelateRawArgs)
	if err == nil {
		err = m.txn.HSet(key, m.jobIDKey(job.ID), b)
	}
	return errors.Trace(err)
}

// AddHistoryDBSJob adds DBS job to history.
func (m *Meta) AddHistoryDBSJob(job *perceptron.Job, uFIDelateRawArgs bool) error {
	return m.addHistoryDBSJob(mDBSJobHistoryKey, job, uFIDelateRawArgs)
}

func (m *Meta) getHistoryDBSJob(key []byte, id int64) (*perceptron.Job, error) {
	value, err := m.txn.HGet(key, m.jobIDKey(id))
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &perceptron.Job{}
	err = job.Decode(value)
	return job, errors.Trace(err)
}

// GetHistoryDBSJob gets a history DBS job.
func (m *Meta) GetHistoryDBSJob(id int64) (*perceptron.Job, error) {
	startTime := time.Now()
	job, err := m.getHistoryDBSJob(mDBSJobHistoryKey, id)
	metrics.MetaHistogram.WithLabelValues(metrics.GetHistoryDBSJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return job, errors.Trace(err)
}

// GetAllHistoryDBSJobs gets all history DBS jobs.
func (m *Meta) GetAllHistoryDBSJobs() ([]*perceptron.Job, error) {
	pairs, err := m.txn.HGetAll(mDBSJobHistoryKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs, err := decodeJob(pairs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// sort job.
	sorter := &jobsSorter{jobs: jobs}
	sort.Sort(sorter)
	return jobs, nil
}

// GetLastNHistoryDBSJobs gets latest N history dbs jobs.
func (m *Meta) GetLastNHistoryDBSJobs(num int) ([]*perceptron.Job, error) {
	pairs, err := m.txn.HGetLastN(mDBSJobHistoryKey, num)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return decodeJob(pairs)
}

// LastJobIterator is the iterator for gets latest history.
type LastJobIterator struct {
	iter *structure.ReverseHashIterator
}

// GetLastHistoryDBSJobsIterator gets latest N history dbs jobs iterator.
func (m *Meta) GetLastHistoryDBSJobsIterator() (*LastJobIterator, error) {
	iter, err := structure.NewHashReverseIter(m.txn, mDBSJobHistoryKey)
	if err != nil {
		return nil, err
	}
	return &LastJobIterator{
		iter: iter,
	}, nil
}

// GetLastJobs gets last several jobs.
func (i *LastJobIterator) GetLastJobs(num int, jobs []*perceptron.Job) ([]*perceptron.Job, error) {
	if len(jobs) < num {
		jobs = make([]*perceptron.Job, 0, num)
	}
	jobs = jobs[:0]
	iter := i.iter
	for iter.Valid() && len(jobs) < num {
		job := &perceptron.Job{}
		err := job.Decode(iter.Value())
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
		err = iter.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return jobs, nil
}

func decodeJob(jobPairs []structure.HashPair) ([]*perceptron.Job, error) {
	jobs := make([]*perceptron.Job, 0, len(jobPairs))
	for _, pair := range jobPairs {
		job := &perceptron.Job{}
		err := job.Decode(pair.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// jobsSorter implements the sort.Interface interface.
type jobsSorter struct {
	jobs []*perceptron.Job
}

func (s *jobsSorter) Swap(i, j int) {
	s.jobs[i], s.jobs[j] = s.jobs[j], s.jobs[i]
}

func (s *jobsSorter) Len() int {
	return len(s.jobs)
}

func (s *jobsSorter) Less(i, j int) bool {
	return s.jobs[i].ID < s.jobs[j].ID
}

// GetBootstrapVersion returns the version of the server which bootstrap the causetstore.
// If the causetstore is not bootstraped, the version will be zero.
func (m *Meta) GetBootstrapVersion() (int64, error) {
	value, err := m.txn.GetInt64(mBootstrapKey)
	return value, errors.Trace(err)
}

// FinishBootstrap finishes bootstrap.
func (m *Meta) FinishBootstrap(version int64) error {
	err := m.txn.Set(mBootstrapKey, []byte(fmt.Sprintf("%d", version)))
	return errors.Trace(err)
}

// UFIDelateDBSReorgStartHandle saves the job reorganization latest processed start handle for later resuming.
func (m *Meta) UFIDelateDBSReorgStartHandle(job *perceptron.Job, startHandle ekv.Handle) error {
	return setReorgJobFieldHandle(m.txn, m.reorgJobStartHandle(job.ID), startHandle)
}

// UFIDelateDBSReorgHandle saves the job reorganization latest processed information for later resuming.
func (m *Meta) UFIDelateDBSReorgHandle(job *perceptron.Job, startHandle, endHandle ekv.Handle, physicalBlockID int64) error {
	err := setReorgJobFieldHandle(m.txn, m.reorgJobStartHandle(job.ID), startHandle)
	if err != nil {
		return errors.Trace(err)
	}
	err = setReorgJobFieldHandle(m.txn, m.reorgJobEndHandle(job.ID), endHandle)
	if err != nil {
		return errors.Trace(err)
	}
	err = m.txn.HSet(mDBSJobReorgKey, m.reorgJobPhysicalBlockID(job.ID), []byte(strconv.FormatInt(physicalBlockID, 10)))
	return errors.Trace(err)
}

func setReorgJobFieldHandle(t *structure.TxStructure, reorgJobField []byte, handle ekv.Handle) error {
	if handle == nil {
		return nil
	}
	var handleEncodedBytes []byte
	if handle.IsInt() {
		handleEncodedBytes = []byte(strconv.FormatInt(handle.IntValue(), 10))
	} else {
		handleEncodedBytes = handle.Encoded()
	}
	return t.HSet(mDBSJobReorgKey, reorgJobField, handleEncodedBytes)
}

// RemoveDBSReorgHandle removes the job reorganization related handles.
func (m *Meta) RemoveDBSReorgHandle(job *perceptron.Job) error {
	err := m.txn.HDel(mDBSJobReorgKey, m.reorgJobStartHandle(job.ID))
	if err != nil {
		return errors.Trace(err)
	}
	if err = m.txn.HDel(mDBSJobReorgKey, m.reorgJobEndHandle(job.ID)); err != nil {
		logutil.BgLogger().Warn("remove DBS reorg end handle", zap.Error(err))
	}
	if err = m.txn.HDel(mDBSJobReorgKey, m.reorgJobPhysicalBlockID(job.ID)); err != nil {
		logutil.BgLogger().Warn("remove DBS reorg physical ID", zap.Error(err))
	}
	return nil
}

// GetDBSReorgHandle gets the latest processed DBS reorganize position.
func (m *Meta) GetDBSReorgHandle(job *perceptron.Job, isCommonHandle bool) (startHandle, endHandle ekv.Handle, physicalBlockID int64, err error) {
	startHandle, err = getReorgJobFieldHandle(m.txn, m.reorgJobStartHandle(job.ID), isCommonHandle)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	endHandle, err = getReorgJobFieldHandle(m.txn, m.reorgJobEndHandle(job.ID), isCommonHandle)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}

	physicalBlockID, err = m.txn.HGetInt64(mDBSJobReorgKey, m.reorgJobPhysicalBlockID(job.ID))
	if err != nil {
		err = errors.Trace(err)
		return
	}
	// physicalBlockID may be 0, because older version MilevaDB (without block partition) doesn't causetstore them.
	// uFIDelate them to block's in this case.
	if physicalBlockID == 0 {
		if job.ReorgMeta != nil {
			endHandle = ekv.IntHandle(job.ReorgMeta.EndHandle)
		} else {
			endHandle = ekv.IntHandle(math.MaxInt64)
		}
		physicalBlockID = job.BlockID
		logutil.BgLogger().Warn("new MilevaDB binary running on old MilevaDB DBS reorg data",
			zap.Int64("partition ID", physicalBlockID),
			zap.Stringer("startHandle", startHandle),
			zap.Stringer("endHandle", endHandle))
	}
	return
}

func getReorgJobFieldHandle(t *structure.TxStructure, reorgJobField []byte, isCommonHandle bool) (ekv.Handle, error) {
	bs, err := t.HGet(mDBSJobReorgKey, reorgJobField)
	if err != nil {
		return nil, errors.Trace(err)
	}
	keyNotFound := bs == nil
	if keyNotFound {
		return nil, nil
	}
	if isCommonHandle {
		return ekv.NewCommonHandle(bs)
	}
	var n int64
	n, err = strconv.ParseInt(string(bs), 10, 64)
	if err != nil {
		return nil, err
	}
	return ekv.IntHandle(n), nil
}

func (m *Meta) schemaDiffKey(schemaVersion int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mSchemaDiffPrefix, schemaVersion))
}

// GetSchemaDiff gets the modification information on a given schemaReplicant version.
func (m *Meta) GetSchemaDiff(schemaVersion int64) (*perceptron.SchemaDiff, error) {
	diffKey := m.schemaDiffKey(schemaVersion)
	startTime := time.Now()
	data, err := m.txn.Get(diffKey)
	metrics.MetaHistogram.WithLabelValues(metrics.GetSchemaDiff, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil || len(data) == 0 {
		return nil, errors.Trace(err)
	}
	diff := &perceptron.SchemaDiff{}
	err = json.Unmarshal(data, diff)
	return diff, errors.Trace(err)
}

// SetSchemaDiff sets the modification information on a given schemaReplicant version.
func (m *Meta) SetSchemaDiff(diff *perceptron.SchemaDiff) error {
	data, err := json.Marshal(diff)
	if err != nil {
		return errors.Trace(err)
	}
	diffKey := m.schemaDiffKey(diff.Version)
	startTime := time.Now()
	err = m.txn.Set(diffKey, data)
	metrics.MetaHistogram.WithLabelValues(metrics.SetSchemaDiff, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}
