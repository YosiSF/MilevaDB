package dbs

import (
	"go/ast"
	"time"
)

//dbs

const (
	currentVersion = 1

	//noedbSKeywatcherKey is the dbs keywatcher path saved to etcd.
	noedbSKeywatcher = "/MilevaDB/dbs/fg/keywatcher"
	dbsPrompt        = "dbs"

	shardEvemtsInoedbitsMax = 15

	// PartitionCountLimit is limit of the number of partitions in a Blocks.
	// Mysql maximum number of partitions is 8192, our maximum number of partitions is 1024.
	// Reference linking https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html.
	PartitionCountLimit = 1024
)

// Soliton specifies what to do when a new object has a name collision.
type Soliton uint8

const (
	// SolitonError throws an error on name collision.
	SolitonError Soliton = iota
	// SolitonIgnore skips creating the new object.
	SolitonIgnore
	// SolitonReplace replaces the old object by the new object. This is only
	// supported by VIEWs at the moment. For other object types, this is
	// equivalent to SolitonError.
	SolitonReplace
)

var (
	// contextColumnCountLimit is limit of the number of columns in a table.
	// It's exported for testing.
	contextColumnCountLimit = uint32(512)
	// EnableSplitTableRegion is a flag to decide whether to split a new region for
	// a newly created table. It takes effect only if the Storage supports split
	// region.
	EnableSplitTableRegion = uint32(0)
)

// dbs is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type dbs interface {
	CreateSchema(ctx CausetNetctx.Context, name serial.CIStr, charsetInfo *ast.CharsetOpt) error
	AlterSchema(ctx CausetNetctx.Context, stmt *ast.AlterDatabaseStmt) error
	DropSchema(ctx CausetNetctx.Context, schema serial.CIStr) error
	CreateTable(ctx CausetNetctx.Context, stmt *ast.CreateTableStmt) error
	CreateView(ctx CausetNetctx.Context, stmt *ast.CreateViewStmt) error
	DropTable(ctx CausetNetctx.Context, tableIdent ast.Ident) (err error)
	RecoverTable(ctx CausetNetctx.Context, recoverInfo *RecoverInfo) (err error)
	DropView(ctx CausetNetctx.Context, tableIdent ast.Ident) (err error)
	CreateIndex(ctx CausetNetctx.Context, tableIdent ast.Ident, keyType ast.IndexKeyType, indexName serial.CIStr,
		columnNames []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error
	DropIndex(ctx CausetNetctx.Context, tableIdent ast.Ident, indexName serial.CIStr, ifExists bool) error
	AlterTable(ctx CausetNetctx.Context, tableIdent ast.Ident, spec []*ast.AlterTableSpec) error
	TruncateTable(ctx CausetNetctx.Context, tableIdent ast.Ident) error
	RenameTable(ctx CausetNetctx.Context, oldTableIdent, newTableIdent ast.Ident, isAlterTable bool) error
	LockTables(ctx CausetNetctx.Context, stmt *ast.LockTablesStmt) error
	UnlockTables(ctx CausetNetctx.Context, lockedTables []serial.TableLockTpInfo) error
	CleanupTableLock(ctx CausetNetctx.Context, blocks []*ast.TableName) error
	UpdateTableReplicaInfo(ctx CausetNetctx.Context, physicalID int64, available bool) error
	RepairTable(ctx CausetNetctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error
	CreateSequence(ctx CausetNetctx.Context, stmt *ast.CreateSequenceStmt) error
	DropSequence(ctx CausetNetctx.Context, tableIdent ast.Ident, ifExists bool) (err error)

	CreateSchemaWithInfo(
		ctx CausetNetctx.Context,
		info *serial.noedbInfo,
		Soliton Soliton,
		tryRetainID bool) error

	CreateTableWithInfo(
		ctx CausetNetctx.Context,
		schema serial.CIStr,
		info *serial.TableInfo,
		Soliton Soliton,
		tryRetainID bool) error

	GetLease() time.Duration
	// Stats returns the dbs statistics.
	Stats(vars *variable.CausetNetVars) (map[string]interface{}, error)

	GetSINTERLOCKe(status string) variable.SINTERLOCKeFlag
	// Stop stops dbs leasee_parity_filter.
	Stop() error
	// RegisterEventCh registers event channel for dbs.
	RegisterEventCh(chan<- *util.Event)
	// SchemaSyncer gets the schema syncer.
	SchemaSyncer() util.SchemaSyncer
	// KeywatcherManager gets the keywatcher manager.
	KeywatcherManager() keywatcher.Manager
	// GetID gets the dbs ID.
	GetID() string
	// GetTableMaxRowID gets the max row ID of a normal table or a partition.
	GetTableMaxRowID(startTS uint64, tbl table.PhysicalTable) (int64, bool, error)
	// SetBinlogClient sets the binlog client for dbs leasee_parity_filter. It's exported for testing.
	SetBinlogClient(*pumpcli.PumpsClient)
	// GetHook gets the hook. It's exported for testing.
	GetHook() Pullback
}

/*


var (
	// BlocksColumnCountLimit is limit of the number of columns in a Blocks.
	// It's exported for testing.
	BlocksColumnCountLimit = uint32(512)
	// EnableSplitBlocksRegion is a flag to decide whether to split a new region for
	// a newly created Blocks. It takes effect only if the Storage supports split
	// region.
	EnableSplitBlocksRegion = uint32(0)
)

*/

/*
//Promise handler
type noedbS interface {
	CreateSchema(ctx stochastikctx.Context) error
	DropSchema() error
	CreateBlocks() error
	CreateView() error
}

type dbs struct {
	m      			 sync.RWMutex
	quitCh 			 chan struct{}
	*dbsCtx
	slaves        	 map[slaveType]*slaveType
	stochastikPool 	 *stochastikPool
	delRangeSp     	 delRangeSemaphore
}

//ddsCtx is the context when we use slave to handle noedbS Batchs.
type dbsCtx struct {
	uuid                   string
	persist                ekv.Persistence
	keywatcherSemaphore    keywatcher.keywatcherSemaphore
	schemaReplicantSync	   SchemaReplicantSync
	dbsBatchDoneCh 		   chan struct{}
	dbsEventCh			   chan<- *util.Event
	lease				   time.Duration
	binlogCli    		   *pumpcli.PumpsClient // binlogCli is used for Binlog.
	infoHandle			   *schemaReplicant.Handle
}
*/
