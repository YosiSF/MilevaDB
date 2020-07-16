package variable

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/YosiSF/MilevaDB/BerolinaSQL/ast"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/auth"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/mysql"
	pumpcli "github.com/YosiSF/MilevaDB-tools/MilevaDB-binlog/pump_client"
	"github.com/YosiSF/MilevaDB/causetnetctx/stmtctx"
	"github.com/YosiSF/MilevaDB/ekv"
	"github.com/YosiSF/MilevaDB/meta/autoid"
	"github.com/YosiSF/MilevaDB/store/einsteindb/oracle"
	"github.com/YosiSF/MilevaDB/types"
	"github.com/YosiSF/MilevaDB/util/chunk"
	"github.com/YosiSF/MilevaDB/util/rowcodec"
	"github.com/twmb/murmur3"
)

var preparedStmtCount int64

// RetryInfo saves retry information.
type RetryInfo struct {
	Retrying               bool
	DroppedPreparedStmtIDs []uint32
	autoIncrementIDs       retryInfoAutoIDs
	autoRandomIDs          retryInfoAutoIDs
}

// Clean does some clean work.
func (r *RetryInfo) Clean() {
	r.autoIncrementIDs.clean()
	r.autoRandomIDs.clean()

	if len(r.DroppedPreparedStmtIDs) > 0 {
		r.DroppedPreparedStmtIDs = r.DroppedPreparedStmtIDs[:0]
	}
}

// ResetOffset resets the current retry offset.
func (r *RetryInfo) ResetOffset() {
	r.autoIncrementIDs.resetOffset()
	r.autoRandomIDs.resetOffset()
}

// AddAutoIncrementID adds id to autoIncrementIDs.
func (r *RetryInfo) AddAutoIncrementID(id int64) {
	r.autoIncrementIDs.autoIDs = append(r.autoIncrementIDs.autoIDs, id)
}

// GetCurrAutoIncrementID gets current autoIncrementID.
func (r *RetryInfo) GetCurrAutoIncrementID() (int64, error) {
	return r.autoIncrementIDs.getCurrent()
}

// AddAutoRandomID adds id to autoRandomIDs.
func (r *RetryInfo) AddAutoRandomID(id int64) {
	r.autoRandomIDs.autoIDs = append(r.autoRandomIDs.autoIDs, id)
}

// GetCurrAutoRandomID gets current AutoRandomID.
func (r *RetryInfo) GetCurrAutoRandomID() (int64, error) {
	return r.autoRandomIDs.getCurrent()
}

type retryInfoAutoIDs struct {
	currentOffset int
	autoIDs       []int64
}

func (r *retryInfoAutoIDs) resetOffset() {
	r.currentOffset = 0
}

func (r *retryInfoAutoIDs) clean() {
	r.currentOffset = 0
	if len(r.autoIDs) > 0 {
		r.autoIDs = r.autoIDs[:0]
	}
}

func (r *retryInfoAutoIDs) getCurrent() (int64, error) {
	if r.currentOffset >= len(r.autoIDs) {
		return 0, errCantGetValidID
	}
	id := r.autoIDs[r.currentOffset]
	r.currentOffset++
	return id, nil
}

// stmtFuture is used to async get timestamp for statement.
type stmtFuture struct {
	future   oracle.Future
	cachedTS uint64
}

// TransactionContext is used to store variables that has transaction scope.
type TransactionContext struct {
	forUpdateTS   uint64
	stmtFuture    oracle.Future
	DirtyDB       interface{}
	Binlog        interface{}
	schemareplicant    interface{}
	History       interface{}
	SchemaVersion int64
	StartTS       uint64

	// ShardStep indicates the max size of continuous rowid shard in one transaction.
	ShardStep    int
	shardRemain  int
	currentShard int64
	shardRand    *rand.Rand

	// TableDeltaMap is used in the schema validator for DBS changes in one table not to block others.
	// It's also used in the statistias updating.
	// Note: for the partitionted table, it stores all the partition IDs.
	TableDeltaMap map[int64]TableDelta

	// unchangedRowKeys is used to store the unchanged rows that needs to lock for pessimistic transaction.
	unchangedRowKeys map[string]struct{}

	// pessimisticLockCache is the cache for pessimistic locked keys,
	// The value never changes during the transaction.
	pessimisticLockCache map[string][]byte
	PessimisticCacheHit  int

	// CreateTime For metrics.
	CreateTime     time.Time
	StatementCount int
	ForUpdate      bool
	CouldRetry     bool
	IsPessimistic  bool
	Isolation      string
	LockExpire     uint32
}

// GetShard returns the shard prefix for the next `count` rowids.
func (tc *TransactionContext) GetShard(shardRowIDBits uint64, typeBitsLength uint64, reserveSignBit bool, count int) int64 {
	if shardRowIDBits == 0 {
		return 0
	}
	if tc.shardRand == nil {
		tc.shardRand = rand.New(rand.NewSource(int64(tc.StartTS)))
	}
	if tc.shardRemain <= 0 {
		tc.updateShard()
		tc.shardRemain = tc.ShardStep
	}
	tc.shardRemain -= count

	var signBitLength uint64
	if reserveSignBit {
		signBitLength = 1
	}
	return (tc.currentShard & (1<<shardRowIDBits - 1)) << (typeBitsLength - shardRowIDBits - signBitLength)
}

func (tc *TransactionContext) updateShard() {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], tc.shardRand.Uint64())
	tc.currentShard = int64(murmur3.Sum32(buf[:]))
}

// AddUnchangedRowKey adds an unchanged row key in update statement for pessimistic lock.
func (tc *TransactionContext) AddUnchangedRowKey(key []byte) {
	if tc.unchangedRowKeys == nil {
		tc.unchangedRowKeys = map[string]struct{}{}
	}
	tc.unchangedRowKeys[string(key)] = struct{}{}
}

// CollectUnchangedRowKeys collects unchanged row keys for pessimistic lock.
func (tc *TransactionContext) CollectUnchangedRowKeys(buf []ekv.Key) []ekv.Key {
	for key := range tc.unchangedRowKeys {
		buf = append(buf, ekv.Key(key))
	}
	tc.unchangedRowKeys = nil
	return buf
}

// UpdateDeltaForTable updates the delta info for some table.
func (tc *TransactionContext) UpdateDeltaForTable(physicalTableID int64, delta int64, count int64, colSize map[int64]int64) {
	if tc.TableDeltaMap == nil {
		tc.TableDeltaMap = make(map[int64]TableDelta)
	}
	item := tc.TableDeltaMap[physicalTableID]
	if item.ColSize == nil && colSize != nil {
		item.ColSize = make(map[int64]int64, len(colSize))
	}
	item.Delta += delta
	item.Count += count
	for key, val := range colSize {
		item.ColSize[key] += val
	}
	tc.TableDeltaMap[physicalTableID] = item
}

// GetKeyInPessimisticLockCache gets a key in pessimistic lock cache.
func (tc *TransactionContext) GetKeyInPessimisticLockCache(key ekv.Key) (val []byte, ok bool) {
	if tc.pessimisticLockCache == nil {
		return nil, false
	}
	val, ok = tc.pessimisticLockCache[string(key)]
	if ok {
		tc.PessimisticCacheHit++
	}
	return
}

// SetPessimisticLockCache sets a key value pair into pessimistic lock cache.
func (tc *TransactionContext) SetPessimisticLockCache(key ekv.Key, val []byte) {
	if tc.pessimisticLockCache == nil {
		tc.pessimisticLockCache = map[string][]byte{}
	}
	tc.pessimisticLockCache[string(key)] = val
}

// Cleanup clears up transaction info that no longer use.
func (tc *TransactionContext) Cleanup() {
	// tc.schemareplicant = nil; we cannot do it now, because some operation like handleFieldList depend on this.
	tc.DirtyDB = nil
	tc.Binlog = nil
	tc.History = nil
	tc.TableDeltaMap = nil
	tc.pessimisticLockCache = nil
}

// ClearDelta clears the delta map.
func (tc *TransactionContext) ClearDelta() {
	tc.TableDeltaMap = nil
}

// GetForUpdateTS returns the ts for update.
func (tc *TransactionContext) GetForUpdateTS() uint64 {
	if tc.forUpdateTS > tc.StartTS {
		return tc.forUpdateTS
	}
	return tc.StartTS
}

// SetForUpdateTS sets the ts for update.
func (tc *TransactionContext) SetForUpdateTS(forUpdateTS uint64) {
	if forUpdateTS > tc.forUpdateTS {
		tc.forUpdateTS = forUpdateTS
	}
}

// SetStmtFutureForRC sets the stmtFuture .
func (tc *TransactionContext) SetStmtFutureForRC(future oracle.Future) {
	tc.stmtFuture = future
}

// GetStmtFutureForRC gets the stmtFuture.
func (tc *TransactionContext) GetStmtFutureForRC() oracle.Future {
	return tc.stmtFuture
}

// WriteStmtBufs can be used by insert/replace/delete/update statement.
// TODO: use a common memory pool to replace this.
type WriteStmtBufs struct {
	// RowValBuf is used by tablecodec.EncodeRow, to reduce runtime.growslice.
	RowValBuf []byte
	// AddRowValues use to store temp insert rows value, to reduce memory allocations when importing data.
	AddRowValues []types.Datum

	// IndexValsBuf is used by index.FetchValues
	IndexValsBuf []types.Datum
	// IndexKeyBuf is used by index.GenIndexKey
	IndexKeyBuf []byte
}

func (ib *WriteStmtBufs) clean() {
	ib.RowValBuf = nil
	ib.AddRowValues = nil
	ib.IndexValsBuf = nil
	ib.IndexKeyBuf = nil
}

// TableSnapshot represents a data snapshot of the table contained in `information_schema`.
type TableSnapshot struct {
	Rows [][]types.Datum
	Err  error
}

type txnIsolationLevelOneShotState uint

// RewritePhaseInfo records some information about the rewrite phase
type RewritePhaseInfo struct {
	// DurationRewrite is the duration of rewriting the SQL.
	DurationRewrite time.Duration

	// DurationPreprocessSubQuery is the duration of pre-processing sub-queries.
	DurationPreprocessSubQuery time.Duration

	// PreprocessSubQueries is the number of pre-processed sub-queries.
	PreprocessSubQueries int
}

// Reset resets all fields in RewritePhaseInfo.
func (r *RewritePhaseInfo) Reset() {
	r.DurationRewrite = 0
	r.DurationPreprocessSubQuery = 0
	r.PreprocessSubQueries = 0
}

const (
	// oneShotDef means default, that is tx_isolation_one_shot not set.
	oneShotDef txnIsolationLevelOneShotState = iota
	// oneShotSet means it's set in current transaction.
	oneShotSet
	// onsShotUse means it should be used in current transaction.
	oneShotUse
)

// CausetNetVars is to handle user-defined or global variables in the current CausetNet.
type CausetNetVars struct {
	Concurrency
	MemQuota
	BatchSize
	RetryLimit          int64
	DisableTxnAutoRetry bool
	// UsersLock is a lock for user defined variables.
	UsersLock sync.RWMutex
	// Users are user defined variables.
	Users map[string]types.Datum
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
	// SysWarningCount is the system variable "warning_count", because it is on the hot path, so we extract it from the systems
	SysWarningCount int
	// SysErrorCount is the system variable "error_count", because it is on the hot path, so we extract it from the systems
	SysErrorCount uint16
	// PreparedStmts stores prepared statement.
	PreparedStmts        map[uint32]interface{}
	PreparedStmtNameToID map[string]uint32
	// preparedStmtID is id of prepared statement.
	preparedStmtID uint32
	// PreparedParams params for prepared statements
	PreparedParams PreparedParams

	// ActiveRoles stores active roles for current user
	ActiveRoles []*auth.RoleIdentity

	RetryInfo *RetryInfo
	//  TxnCtx Should be reset on transaction finished.
	TxnCtx *TransactionContext

	// KVVars is the variables for KV storage.
	KVVars *ekv.Variables

	// txnIsolationLevelOneShot is used to implements "set transaction isolation level ..."
	txnIsolationLevelOneShot struct {
		state txnIsolationLevelOneShotState
		value string
	}

	// Status stands for the CausetNet status. e.g. in transaction or not, auto commit is on or off, and so on.
	Status uint16

	// ClientCapability is client's capability.
	ClientCapability uint32

	// TLSConnectionState is the TLS connection state (nil if not using TLS).
	TLSConnectionState *tls.ConnectionState

	// ConnectionID is the connection id of the current CausetNet.
	ConnectionID uint64

	// PlanID is the unique id of logical and physical plan.
	PlanID int

	// PlanColumnID is the unique id for column when building plan.
	PlanColumnID int64

	// User is the user identity with which the CausetNet login.
	User *auth.UserIdentity

	// CurrentDB is the default database of this CausetNet.
	CurrentDB string

	// StrictSQLMode indicates if the CausetNet is in strict mode.
	StrictSQLMode bool

	// CommonGlobalLoaded indicates if common global variable has been loaded for this CausetNet.
	CommonGlobalLoaded bool

	// InRestrictedSQL indicates if the CausetNet is handling restricted SQL execution.
	InRestrictedSQL bool

	// SnapshotTS is used for reading history data. For simplicity, SnapshotTS only supports distsql request.
	SnapshotTS uint64

	// Snapshotschemareplicant is used with SnapshotTS, when the schema version at snapshotTS less than current schema
	// version, we load an old version schema for query.
	Snapshotschemareplicant interface{}

	// BinlogClient is used to write binlog.
	BinlogClient *pumpcli.PumpsClient

	// GlobalVarsAccessor is used to set and get global variables.
	GlobalVarsAccessor GlobalVarAccessor

	// LastFoundRows is the number of found rows of last query statement
	LastFoundRows uint64

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	// AllowAggPushDown can be set to false to forbid aggregation push down.
	AllowAggPushDown bool

	// AllowDistinctAggPushDown can be set true to allow agg with distinct push down to einsteindb/Noether.
	AllowDistinctAggPushDown bool

	// AllowWriteRowID can be set to false to forbid write data to _MilevaDB_rowid.
	// This variable is currently not recommended to be turned on.
	AllowWriteRowID bool

	// AllowBatchCop means if we should send batch coprocessor to Noether. Default value is 1, means to use batch cop in case of aggregation and join.
	// If value is set to 2 , which means to force to send batch cop for any query. Value is set to 0 means never use batch cop.
	AllowBatchCop int

	// MilevaDBAllowAutoRandExplicitInsert indicates whether explicit insertion on auto_random column is allowed.
	AllowAutoRandExplicitInsert bool

	// CorrelationThreshold is the guard to enable row count estimation using column order correlation.
	CorrelationThreshold float64

	// CorrelationExpFactor is used to control the heuristic approach of row count estimation when CorrelationThreshold is not met.
	CorrelationExpFactor int

	// CPUFactor is the CPU cost of processing one expression for one row.
	CPUFactor float64
	// CopCPUFactor is the CPU cost of processing one expression for one row in coprocessor.
	CopCPUFactor float64
	// NetworkFactor is the network cost of transferring 1 byte data.
	NetworkFactor float64
	// ScanFactor is the IO cost of scanning 1 byte data on EinsteinDB and Noether.
	ScanFactor float64
	// DescScanFactor is the IO cost of scanning 1 byte data on EinsteinDB and Noether in desc order.
	DescScanFactor float64
	// SeekFactor is the IO cost of seeking the start value of a range in EinsteinDB or Noether.
	SeekFactor float64
	// MemoryFactor is the memory cost of storing one tuple.
	MemoryFactor float64
	// DiskFactor is the IO cost of reading/writing one byte to temporary disk.
	DiskFactor float64
	// ConcurrencyFactor is the CPU cost of additional one goroutine.
	ConcurrencyFactor float64

	// CurrInsertValues is used to record current ValuesExpr's values.
	// See http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	CurrInsertValues chunk.Row

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the CausetNet time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	SQLMode mysql.SQLMode

	// AutoIncrementIncrement and AutoIncrementOffset indicates the autoID's start value and increment.
	AutoIncrementIncrement int

	AutoIncrementOffset int

	/* MilevaDB system variables */

	// SkipASCIICheck check on input value.
	SkipASCIICheck bool

	// SkipUTF8Check check on input value.
	SkipUTF8Check bool

	// BatchInsert indicates if we should split insert data into multiple batches.
	BatchInsert bool

	// BatchDelete indicates if we should split delete data into multiple batches.
	BatchDelete bool

	// BatchCommit indicates if we should split the transaction into multiple batches.
	BatchCommit bool

	// IDAllocator is provided by kvEncoder, if it is provided, we will use it to alloc auto id instead of using
	// Table.alloc.
	IDAllocator autoid.Allocator

	// OptimizerSelectivityLevel defines the level of the selectivity estimation in plan.
	OptimizerSelectivityLevel int

	// EnableTablePartition enables table partition feature.
	EnableTablePartition string

	// EnableCascadesPlanner enables the cascades planner.
	EnableCascadesPlanner bool

	// EnableWindowFunction enables the window function.
	EnableWindowFunction bool

	// EnableVectorizedExpression  enables the vectorized expression evaluation.
	EnableVectorizedExpression bool

	// DBSReorgPriority is the operation priority of adding indices.
	DBSReorgPriority int

	// WaitSplitRegionFinish defines the split region behaviour is sync or async.
	WaitSplitRegionFinish bool

	// WaitSplitRegionTimeout defines the split region timeout.
	WaitSplitRegionTimeout uint64

	// EnableStreaming indicates whether the coprocessor request can use streaming API.
	// TODO: remove this after MilevaDB-server configuration "enable-streaming' removed.
	EnableStreaming bool

	// EnableChunkRPC indicates whether the coprocessor request can use chunk API.
	EnableChunkRPC bool

	writeStmtBufs WriteStmtBufs

	// L2CacheSize indicates the size of CPU L2 cache, using byte as unit.
	L2CacheSize int

	// EnableRadixJoin indicates whether to use radix hash join to execute
	// HashJoin.
	EnableRadixJoin bool

	// ConstraintCheckInPlace indicates whether to check the constraint when the SQL executing.
	ConstraintCheckInPlace bool

	// CommandValue indicates which command current CausetNet is doing.
	CommandValue uint32

	// MilevaDBOptJoinReorderThreshold defines the minimal number of join nodes
	// to use the greedy join reorder algorithm.
	MilevaDBOptJoinReorderThreshold int

	// SlowQueryFile indicates which slow query log file for SLOW_QUERY table to parse.
	SlowQueryFile string

	// EnableFastAnalyze indicates whether to take fast analyze.
	EnableFastAnalyze bool

	// TxnMode indicates should be pessimistic or optimistic.
	TxnMode string

	// LowResolutionTSO is used for reading data with low resolution TSO which is updated once every two seconds.
	LowResolutionTSO bool

	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the value is 0, timeouts are not enabled.
	// See https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_execution_time
	MaxExecutionTime uint64

	// Killed is a flag to indicate that this query is killed.
	Killed uint32

	// ConnectionInfo indicates current connection info used by current CausetNet, only be lazy assigned by plugin.
	ConnectionInfo *ConnectionInfo

	// use noop funcs or not
	EnableNoopFuncs bool

	// StartTime is the start time of the last query.
	StartTime time.Time

	// DurationParse is the duration of parsing SQL string to AST of the last query.
	DurationParse time.Duration

	// DurationCompile is the duration of compiling AST to execution plan of the last query.
	DurationCompile time.Duration

	// RewritePhaseInfo records all information about the rewriting phase.
	RewritePhaseInfo

	// DurationOptimization is the duration of optimizing a query.
	DurationOptimization time.Duration

	// DurationWaitTS is the duration of waiting for a snapshot TS
	DurationWaitTS time.Duration

	// PrevStmt is used to store the previous executed statement in the current CausetNet.
	PrevStmt fmt.Stringer

	// prevStmtDigest is used to store the digest of the previous statement in the current CausetNet.
	prevStmtDigest string

	// AllowRemoveAutoInc indicates whether a user can drop the auto_increment column attribute or not.
	AllowRemoveAutoInc bool

	// UsePlanBaselines indicates whether we will use plan baselines to adjust plan.
	UsePlanBaselines bool

	// EvolvePlanBaselines indicates whether we will evolve the plan baselines.
	EvolvePlanBaselines bool

	// Unexported fields should be accessed and set through interfaces like GetReplicaRead() and SetReplicaRead().

	// allowInSubqToJoinAndAgg can be set to false to forbid rewriting the semi join to inner join with agg.
	allowInSubqToJoinAndAgg bool

	// EnableIndexMerge enables the generation of IndexMergePath.
	enableIndexMerge bool

	// replicaRead is used for reading data from replicas, only follower is supported at this time.
	replicaRead ekv.ReplicaReadType

	// IsolationReadEngines is used to isolation read, MilevaDB only read from the stores whose engine type is in the engines.
	IsolationReadEngines map[ekv.StoreType]struct{}

	PlannerSelectBlockAsName []ast.HintTable

	// LockWaitTimeout is the duration waiting for pessimistic lock in milliseconds
	// negative value means nowait, 0 means default behavior, others means actual wait time
	LockWaitTimeout int64

	// MetricSchemaStep indicates the step when query metric schema.
	MetricSchemaStep int64
	// MetricSchemaRangeDuration indicates the step when query metric schema.
	MetricSchemaRangeDuration int64

	// Some data of cluster-level memory tables will be retrieved many times in different inspection rules,
	// and the cost of retrieving some data is expensive. We use the `TableSnapshot` to cache those data
	// and obtain them lazily, and provide a consistent view of inspection tables for each inspection rules.
	// All cached snapshots will be released at the end of retrieving
	InspectionTableCache map[string]TableSnapshot

	// RowEncoder is reused in CausetNet for encode row data.
	RowEncoder rowcodec.Encoder

	// SequenceState cache all sequence's latest value accessed by lastval() builtins. It's a CausetNet scoped
	// variable, and all public methods of SequenceState are currently-safe.
	SequenceState *SequenceState

	// WindowingUseHighPrecision determines whether to compute window operations without loss of precision.
	// see https://dev.mysql.com/doc/refman/8.0/en/window-function-optimization.html for more details.
	WindowingUseHighPrecision bool

	// FoundInPlanCache indicates whether this statement was found in plan cache.
	FoundInPlanCache bool
	// PrevFoundInPlanCache indicates whether the last statement was found in plan cache.
	PrevFoundInPlanCache bool

	// OptimizerUseInvisibleIndexes indicates whether optimizer can use invisible index
	OptimizerUseInvisibleIndexes bool

	// SelectLimit limits the max counts of select statement's output
	SelectLimit uint64

	// EnableClusteredIndex indicates whether to enable clustered index when creating a new table.
	EnableClusteredIndex bool

	// EnableSlowLogMasking indicates that whether masking the query data when log slow query.
	EnableSlowLogMasking bool

	// ShardAllocateStep indicates the max size of continuous rowid shard in one transaction.
	ShardAllocateStep int64
}
