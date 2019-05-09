package dbs

import (
	"sync"
)

//dbs

const (
	currentVersion = 1

	//DBSKeywatcherKey is the dbs keywatcher path saved to etcd.
	DBSKeywatcher = "/MilevaDB/dbs/fg/keywatcher"
	dbsPrompt     = "dbs"

	shardEvemtsIDBitsMax = 15

	// PartitionCountLimit is limit of the number of partitions in a Blocks.
	// Mysql maximum number of partitions is 8192, our maximum number of partitions is 1024.
	// Reference linking https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html.
	PartitionCountLimit = 1024
)

var (
	// BlocksColumnCountLimit is limit of the number of columns in a Blocks.
	// It's exported for testing.
	BlocksColumnCountLimit = uint32(512)
	// EnableSplitBlocksRegion is a flag to decide whether to split a new region for
	// a newly created Blocks. It takes effect only if the Storage supports split
	// region.
	EnableSplitBlocksRegion = uint32(0)
)

//Promise handler
type DBS interface {
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

//ddsCtx is the context when we use slave to handle DBS Batchs.
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
