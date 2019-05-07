package dbs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/ngaut/pools"
	"github.com/YosiSF/MilevaDB/core/keywatcher"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/container"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/query"

	"github.com/twinj/uuid"
	"go.uber.org/zap"
)

//dbs 

const (
	currentVersion = 1

	//DBSKeywatcherKey is the dbs keywatcher path saved to etcd.
	DBSKeywatcher = "/MilevaDB/dbs/fg/keywatcher"
	dbsPrompt = "dbs"

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

type dds struct {
	m  sync.RWMutex
	quitCh chan struct{}
	*ddsCtx
	slaves map[slaveType]*slaveType
	stochastikPool *stochastikPool
	delRangeSp delRangeSemaphore
}

//ddsCtx is the context when we use slabe tp handle DDS Batchs.
type ddsCtx struct {
	uuid 				string
	persist				ekv.Persistence
	keywatcherSemaphore keywatcher.keywatcherSemaphore

}