package ekv

import (
	"bytes"
	"context"
	"io/ioutil"
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

	// MaxKeys is the maximum numbers of keys in the noedb.
	MaxKeys = math.MaxUint32

	metaExt    = ".pmt"
	noedbMetaName = "noedb" + metaExt
)

// noedb represents the key-value storage.
// All noedb methods are safe for concurrent use by multiple goroutines.
type noedb struct {
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


func Open(path string, opts *Options) (*noedb, error) {
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
	noedb := &noedb{
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
		noedb.hashSeed = seed
	} else {
		if err := noedb.readMeta(); err != nil {
			return nil, err
		}
	}
	// Lock already exists - database wasn't closed properly.
	if acquiredExistingLock {
		if err := noedb.recover(); err != nil {
			return nil, err
		}
	}
	if noedb.opts.BackgroundSyncInterval > 0 || noedb.opts.BackgroundCompactionInterval > 0 {
		noedb.startBackgroundWorker()
	}
	clean = nil
	return noedb, nil
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func (noedb *noedb) writeMeta() error {
	m := noedbMeta{
		HashSeed: noedb.hashSeed,
	}
	return writeGobFile(noedb.opts.FileSystem, filepath.Join(noedb.opts.path, noedbMetaName), m)
}

func (noedb *noedb) readMeta() error {
	m := noedbMeta{}
	if err := readGobFile(noedb.opts.FileSystem, filepath.Join(noedb.opts.path, noedbMetaName), &m); err != nil {
		return err
	}
	noedb.hashSeed = m.HashSeed
	return nil
}

func (noedb *noedb) hash(data []byte) uint32 {
	return hash.Sum32WithSeed(data, noedb.hashSeed)
}

func newNullableTicker(d time.Duration) (<-chan time.Time, func()) {
	if d > 0 {
		t := time.NewTicker(d)
		return t.C, t.Stop
	}
	return nil, func() {}
}

func (noedb *noedb) startBackgroundWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	noedb.cancelBgWorker = cancel
	noedb.closeWg.Add(1)

	go func() {
		defer noedb.closeWg.Done()

		syncC, syncStop := newNullableTicker(noedb.opts.BackgroundSyncInterval)
		defer syncStop()
		compactC, compactStop := newNullableTicker(noedb.opts.BackgroundCompactionInterval)
		defer compactStop()

		var lastModifications int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-syncC:
				modifications := noedb.metrics.Puts.Value() + noedb.metrics.Dels.Value()
				if modifications == lastModifications {
					break
				}
				lastModifications = modifications
				if err := noedb.Sync(); err != nil {
					logger.Printf("error synchronizing databse: %v", err)
				}
			case <-compactC:
				if cr, err := noedb.Compact(); err != nil {
					logger.Printf("error compacting databse: %v", err)
				} else if cr.CompactedSegments > 0 {
					logger.Printf("compacted database: %+v", cr)
				}
			}
		}
	}()
}