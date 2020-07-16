package ekv

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// MaxKeyLength is the maximum size of a key in bytes.
	MaxKeyLength = math.MaxUint16

	maxUint30 = 1<<30 - 1

	// MaxValueLength is the maximum size of a value in bytes.
	MaxValueLength = maxUint30

	// MaxKeys is the maximum numbers of keys in the DB.
	MaxKeys = math.MaxUint32

	metaExt    = ".pmt"
	noedbMetaName = "DB" + metaExt
)

// DB represents the key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	mu                sync.RWMutex
	opts              *Options
	index             *index
	datalog           *datalog
	lock              fs.LockFile
	hashSeed          uint32
	metrics           Metrics
	syncWrites        bool
	cancelBgWorker    context.CancelFunc
	closeWg           sync.WaitGroup
	compactionRunning int32
}

type noedbMeta struct {
	HashSeed uint32
}


func Open(path string, opts *Options) (*DB, error) {
	opts = opts.copyWithDefaults(path)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	lock, acquiredExistingLock, err := createLockFile(opts)
	if err != nil {
		if err == os.ErrExist {
			err = errLocked
		}
		return nil, err
	}
	clean := lock.Unlock
	defer func() {
		if clean != nil {
			_ = clean()
		}
	}()
	if acquiredExistingLock {
		if err := backupNonsegmentFiles(path); err != nil {
			return nil, err
		}
	}
	index, err := openIndex(opts)
	if err != nil {
		return nil, err
	}
	datalog, err := openDatalog(opts)
	if err != nil {
		return nil, err
	}
	DB := &DB{
		opts:       opts,
		index:      index,
		datalog:    datalog,
		lock:       lock,
		metrics:    newMetrics(),
		syncWrites: opts.BackgroundSyncInterval == -1,
	}
	if index.count() == 0 {
		seed, err := hash.RandSeed()
		if err != nil {
			return nil, err
		}
		DB.hashSeed = seed
	} else {
		if err := DB.readMeta(); err != nil {
			return nil, err
		}
	}
	// Lock already exists - database wasn't closed properly.
	if acquiredExistingLock {
		if err := DB.recover(); err != nil {
			return nil, err
		}
	}
	if DB.opts.BackgroundSyncInterval > 0 || DB.opts.BackgroundCompactionInterval > 0 {
		DB.startBackgroundWorker()
	}
	clean = nil
	return DB, nil
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func (DB *DB) writeMeta() error {
	m := noedbMeta{
		HashSeed: DB.hashSeed,
	}
	return writeGobFile(DB.opts.FileSystem, filepath.Join(DB.opts.path, noedbMetaName), m)
}

func (DB *DB) readMeta() error {
	m := noedbMeta{}
	if err := readGobFile(DB.opts.FileSystem, filepath.Join(DB.opts.path, noedbMetaName), &m); err != nil {
		return err
	}
	DB.hashSeed = m.HashSeed
	return nil
}

func (DB *DB) hash(data []byte) uint32 {
	return hash.Sum32WithSeed(data, DB.hashSeed)
}

func newNullableTicker(d time.Duration) (<-chan time.Time, func()) {
	if d > 0 {
		t := time.NewTicker(d)
		return t.C, t.Stop
	}
	return nil, func() {}
}

func (DB *DB) startBackgroundWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	DB.cancelBgWorker = cancel
	DB.closeWg.Add(1)

	go func() {
		defer DB.closeWg.Done()

		syncC, syncStop := newNullableTicker(DB.opts.BackgroundSyncInterval)
		defer syncStop()
		compactC, compactStop := newNullableTicker(DB.opts.BackgroundCompactionInterval)
		defer compactStop()

		var lastModifications int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-syncC:
				modifications := DB.metrics.Puts.Value() + DB.metrics.Dels.Value()
				if modifications == lastModifications {
					break
				}
				lastModifications = modifications
				if err := DB.Sync(); err != nil {
					logger.Printf("error synchronizing databse: %v", err)
				}
			case <-compactC:
				if cr, err := DB.Compact(); err != nil {
					logger.Printf("error compacting databse: %v", err)
				} else if cr.CompactedSegments > 0 {
					logger.Printf("compacted database: %+v", cr)
				}
			}
		}
	}()
}