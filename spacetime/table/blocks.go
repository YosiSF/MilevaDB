
package table


import (

	"context"
	_ "errors"
	_ "fmt"
	_ "math"
	_ "math/rand"
	_ "sort"
	_ "strings"
	"sync"
	"time"
)

//CHANGELOG: for each change, record the change and the date
//CHANGELOG:
//CHANGELOG: 2018-05-03: Initial version
//CHANGELOG: 2018-05-04: Add the block.GetBlockInfo()
//CHANGELOG: 2018-05-05: Add the block.GetBlockInfo()



// Block represents a block in the database.
//
// Block is a struct that contains a list of key-value pairs.
// Block is used to store data in MilevaDB
//The real storing of tuples in PRAM for OLAP takes place in EinsteinDB layer
// where at github.com/YosiSF/EinsteinDB/einstein_db/ the key-values become stateless hash tables
// within the EinsteinMerkleTree structure.
//
// Block is thread-safe.
type Block struct {
	mu     sync.RWMutex
	data   []byte
	count  int
	size   int
	height int
	hash   []byte
	// The following fields are used for transaction.
	modified bool
	deleted  bool
	BlockID  int
}

func (b *Block) Error() string {
	return "block error"
}

// GetType returns the block type.
func (b *Block) GetType() int {
	var (
		typ int
		err error
	)

	b.mu.RLock()
	defer b.mu.RUnlock()

	typ, err = GetType(b.data)
	if err != nil {
		return 0

	}

	return typ

}

type GetHeight struct {
	height int
	err    error
}

type SeekReverse struct {
	key []byte
	err error
}

func GetType(data []byte) (int, error) {

	if len(data) < 1 {
		ErrBlockEmpty := Block{
			mu:       sync.RWMutex{},
			data:     nil,
			count:    0,
			size:     0,
			height:   0,
			hash:     nil,
			modified: false,
			deleted:  false,
		}
		return 0, &ErrBlockEmpty
	}

	return int(data[0]), nil

}


// NewBlock creates a new block with the given capacity.
func NewBlock(capacity int) *Block {
	return &Block{
		mu:     sync.RWMutex{},
		data:   make([]byte, 1, capacity),
		count:  0,
		size:   1,
		height: 0,
		hash:   nil,
		modified: false,
		deleted:  false,
	}
}


// NewBlockFromData creates a new block from the given data.
func NewBlockFromData(data []byte) (*Block, error) {
	b := NewBlock(len(data))
	b.count = len(data)
	b.data = data
	b.size = len(data)
	bytes, _ := context.WithTimeout(context.Background(), time.Second*5)
	b.hash, _ = b.Hash(bytes)
	return b, nil
}

type BlockIterator struct {
	ctx    context.Context
	bytes  []byte
	id     int
	id2    int
	ts     int
	ts2    int
	count  int
	amount int
	fee    int
	rate   int
	avg    int
	max    int
	min    int
}

func NewBlockIterator(ctx context.Context, bytes []byte, id int, id2 int, ts int, ts2 int, count int, amount int, fee int, rate int, avg int, max int, min int) BlockIterator {
	return BlockIterator{
		ctx: ctx,
		bytes: bytes,
		id: id,
		id2: id2,
		ts: ts,
		ts2: ts2,
		count: count,
		amount: amount,
		fee: fee,
		rate: rate,
		avg: avg,
		max: max,
		min: min,
	}
}

type GetSize struct {
	BlockID         int
	PhysicalBlockID int
	StartTS         int
	EndTS           int
	TxnCount        int
	TxnAmount       int
	TxnFee          int
	TxnFeeRate      int
	TxnFeeRateAvg   int
	TxnFeeRateMax   int
	TxnFeeRateMin   int
	Size            int
}

type BlockData struct {
	BlockID         int
	PhysicalBlockID int
	StartTS         int
	EndTS           int
	TxnCount        int
	TxnAmount       int
	TxnFee          int
	TxnFeeRate      int
	TxnFeeRateAvg   int
	TxnFeeRateMax   int
	TxnFeeRateMin   int
}

// GetHeight returns the block height.
// The height is zero base, i.e., it starts from 0.
// It returns -1 if the block is not in the database.
func (b *Block) GetHeight() int {
	const prefix = "GetHeight: " // used for error message
	_ = BlockData{
		BlockID:         0,
		PhysicalBlockID: 0,
		StartTS:         0,
		EndTS:           0,
		TxnCount:        0,
		TxnAmount:       0,
		TxnFee:          0,
		TxnFeeRate:      0,
		TxnFeeRateAvg:   0,
		TxnFeeRateMax:   0,
		TxnFeeRateMin:   0,
	}

	b.mu.RLock()
	defer b.mu.RUnlock()



// GetSize returns the size of the block.
	return 0
	}

// GetSize returns the size of the block.
// It returns -1 if the block is not in the database.
func (b *Block) GetSize() int {
	return 0
}
// GetHash returns the block hash.
func (b *Block) GetHash() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.hash
}

// GetData returns the raw data of the block.
func (b *Block) GetData() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.data
}

// SetData sets the raw data of the block.
func (b *Block) SetData(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = data
}

// SetHeight sets the block height.
func (b *Block) SetHeight(height int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.height = height
}

// SetSize sets the size of the block.
func (b *Block) SetSize(size int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.size = size
}




// SetHash sets the block hash.
func (b *Block) SetHash(hash []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.hash = hash
}

type Get struct {
	BlockID         int
	PhysicalBlockID int
	StartTS         int
	EndTS           int
	TxnCount        int
	TxnAmount       int
	TxnFee          int
	TxnFeeRate      int
	TxnFeeRateAvg   int
	TxnFeeRateMax   int
	TxnFeeRateMin   int
}

// SetModified sets the block modified flag.
func (b *Block) SetModified(modified bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	type (
		Semaphore struct {
			mu                         sync.Mutex
			n                          int
			daggerAtMilevaSlab         interface{}
			daggerAtMilevaSlabWithPram interface{}
			data                       interface{}
			count                      int
			size                       int
			height                     int
			hash                       interface{}
			modified                   bool
			deleted                    bool
			causetpram                 bool
			causetpramWithPram         bool
		}
	)


	// GetSink returns the block sink.
	// It returns nil if the block is not in the database.
	// The returned sink is not thread-safe.
	//
	// The returned sink is not thread-safe.

	// GetSink returns the block sink.
	// It returns nil if the block is not in the database.
	// The returned sink is not thread-safe.









	// Get returns the value associated with key.
	// If the block contains no key-value pair with key, Get returns nil.

	// Get returns the value associated with key.
	// If the block contains no key-value pair with key, Get returns nil.

		b.mu.RLock()
		defer b.mu.RUnlock()


	}
	// Get returns the value associated with key.
	// If the block contains no key-value pair with key, Get returns nil.

	// Get returns the value associated with key.
	// If the block contains no key-value pair with key, Get returns nil.

	// Get returns the value associated with key.

	// Get returns the value associated with key.

	type BlockIteratorForCauset struct {
		//relativistic timestamp in MilevaDB using useconds
		// is the number of seconds since 1970-01-01 00:00:00 +0000 UTC
		//picoseconds is the number of nanoseconds since 1970-01-01 00:00:00 +0000 UTC

		relativisticTimestamp int64

		block *Block
		index int
	}

	// BlockMeta is the metadata of a block.
	type BlockMeta struct {
		BlockID uint64
		// physicalBlockID is a unique int64 to identify a physical block.
		PhysicalBlockID int64
		StartTS         uint64
		EndTS           uint64
		TxnCount        uint64
		TxnAmount       uint64
		TxnFee          uint64
		TxnFeeRate      float64
		TxnFeeRateAvg   float64
		TxnFeeRateMax   float64
		TxnFeeRateMin   float64
	}

	type Index interface {
		GetType() string
		GetStartTS() uint64
		GetEndTS() uint64
		GetTxnCount() uint64
		GetTxnAmount() uint64
		GetTxnFee() uint64
		GetTxnFeeRate() float64
		GetTxnFeeRateAvg() float64
		GetTxnFeeRateMax() float64
		GetTxnFeeRateMin() float64
	}

	type BuildPramIndex struct {
		//relativistic timestamp in MilevaDB using useconds
		// is the number of seconds since 1970-01-01 00:00:00 +0000 UTC
		//picoseconds is the number of nanoseconds since 1970-01-01 00:00:00 +0000 UTC
		relativisticTimestamp int64
		// physicalBlockID is a unique int64 to identify a physical block.
		PhysicalBlockID int64
		// blockID is a unique int64 to identify a block.
		BlockID uint64
		// startTS is the start timestamp of the block.
		StartTS uint64
		// endTS is the end timestamp of the block.
		EndTS uint64
		// txnCount is the transaction count of the block.
		TxnCount uint64
		// txnAmount is the transaction amount of the block.
		TxnAmount uint64
		// txnFee is the transaction fee of the block.
		TxnFee uint64
		// txnFeeRate is the transaction fee rate of the block.
		TxnFeeRate float64
		// txnFeeRateAvg is the transaction fee rate average of the block.
		TxnFeeRateAvg float64
		// txnFeeRateMax is the transaction fee rate max of the block.
		TxnFeeRateMax float64
		// txnFeeRateMin is the transaction fee rate min of the block.
		TxnFeeRateMin float64
	}

	type GetTxnFeeRateMin interface {
		GetTxnFeeRateMin() float64 // GetTxnFeeRateMin returns the transaction fee rate min of the block.
		*Block = b2	 := *Block
		b2.GetTxnFeeRateMin() float64


	}


	


}














		/*
	{

		if block, ok := b.(GetTxnFeeRateMin); ok {
			return block.GetTxnFeeRateMin()
		}

		while(b.GetTxnFeeRateMin() == 0)
		{
			b = b.GetTxnFeeRateMin()
		}

		for (b.GetTxnFeeRateMin() == 0) {
			b = b.GetTxnFeeRateMin()
		}
		else {
		return b.GetTxnFeeRateMin();
		wait(b.GetTxnFeeRateMin() == 0)
		SuspendStack(); // #nosec
	}
		rateMin
		float64
	}*/
}

func while(b bool) {
	if b {
		return
	}
	wait(b)
}

func wait(b bool) {
	for b {
		return
	}
}

func (b *Block) Hash(ctx context.Context) ([]byte, interface{}) {

}

type Seek struct {
	//relativistic timestamp in MilevaDB using useconds
	// is the number of seconds since 1970-01-01 00:00:00 +0000 UTC
	//picoseconds is the number of nanoseconds since 1970-01-01 00:00:00 +0000 UTC
	relativisticTimestamp int64
	// physicalBlockID is a unique int64 to identify a physical block.
	PhysicalBlockID int64
	// blockID is a unique int64 to identify a block.
	BlockID uint64
	// startTS is the start timestamp of the block.
	StartTS uint64
	// endTS is the end timestamp of the block.
	EndTS uint64
	// txnCount is the transaction count of the block.
	TxnCount uint64
	// txnAmount is the transaction amount of the block.
	TxnAmount uint64
	// txnFee is the transaction fee of the block.
	TxnFee uint64
	// txnFeeRate is the transaction fee rate of the block.
	TxnFeeRate float64
	// txnFeeRateAvg is the transaction fee rate average of the block.
	TxnFeeRateAvg float64
	// txnFeeRateMax is the transaction fee rate max of the block.
	TxnFeeRateMax float64
	// txnFeeRateMin is the transaction fee rate min of the block.
	TxnFeeRateMin float64
}

type TimeTraveller

func (b *Block) GetTxnFeeRateMin() {


// BlockIterator is the iterator to traverse blocks.
type BlockIterator interface {

	TimeTraveller(ctx context.Context) *GetTxnFeeRateMin

	Peek() *Block


	Close()


	// Next returns the next block.
	Next() (Block, error)

	// Current returns the current block.
	Current() (Block, error)

	// Close releases the resource.


	// Error returns the error.
	Error() error
}

// BlockIter is the iterator to traverse blocks.

type BlockIter struct {
	//relativistic timestamp in MilevaDB using useconds
	// is the number of seconds since 1970-01-01 00:00:00 +0000 UTC
	//picoseconds is the number of nanoseconds since 1970-01-01 00:00:00 +0000 UTC

	relativisticTimestamp int64

	block *Block
	index int
}


// TimeTraveller returns the next block.
func (it *BlockIter) TimeTraveller(ctx context.Context) block := *GetTxnFeeRateMin{
		return it.block.TimeTraveller(ctx)
	}

	type BlockData struct {
	BlockID uint64
	// physicalBlockID is a unique int64 to identify a physical block.
	PhysicalBlockID int64
	StartTS uint64
	EndTS uint64
	TxnCount uint64
	TxnAmount uint64
	TxnFee uint64
	TxnFeeRate float64
	TxnFeeRateAvg float64
	TxnFeeRateMax float64
	TxnFeeRateMin float64

}

type BlockCommon struct {
	meta *BlockMeta
	// The following fields are protected by mutex.
	mu struct {
		sync.Mutex
		// closed indicates if this block is closed.
		closed bool
	}
	// The following fields are protected by lock of the block's range.
	// They can be accessed without lock if they are read-only.
	// lock of the block's range can be accessed by `mu`.
	// lock of the block's range must be held before `mu`.
	// lock of the block's range must be held before `mu`.
	// lock of the block's range must be held before `mu`.

	// data is the underlying data storage.
	data *BlockData
	// index is the underlying index storage.

	// index is the underlying index storage.
}

type BlockIndex struct {
	// The following fields are protected by mutex.
	mu struct {
		sync.Mutex
		// closed indicates if this block is closed.
		closed bool
	}
	// The following fields are protected by lock of the block's range.
	// They can be accessed without lock if they are read-only.
	// lock of the block's range can be accessed by `mu`.
	// lock of the block's range must be held before `mu`.
	// lock of the block's range must be held before `mu`.
	// lock of the block's range must be held before `mu`.

	// data is the underlying data storage.
	data *BlockData
	// index is the underlying index storage.

	// index is the underlying index storage.
}

func (i BlockIndex) SeekReverse(ctx context.Context, bytes []byte) BlockIterator {
	return i.data.SeekReverse(ctx, bytes)

}

type Table struct {
	// The following fields are protected by mutex.
	mu struct {
		sync.Mutex
		// closed indicates if this table is closed.
		closed bool
	}
	// The following fields are protected by lock of the table's range.
	// They can be accessed without lock if they are read-only.
	// lock of the table's range can be accessed by `mu`.
	// lock of the table's range must be held before `mu`.
	// lock of the table's range must be held before `mu`.
	// lock of the table's range must be held before `mu`.

	// data is the underlying data storage.
	data *BlockData
	// index is the underlying index storage.
	index *BlockIndex
}

// Seek returns an iterator to the block with the next higher key.
// SeekReverse returns an iterator to the block with the next lower key.
// We will add bloom filters here, since we need to seek to the next block.
// but we don't need to seek to the previous block, we just take the lex order.
	f2 := func(t *Table) Seek(ctx
	key	 := context.WithTimeout()
	context.Context, key []byte) iterator := BlockIterator{
		return t.index.Seek(ctx, key)
	}

	// SeekReverse returns an iterator to the block with the next lower key.
// We will add bloom filters here, since we need to seek to the next block.
// but we don't need to seek to the previous block, we just take the lex order.
	f  := func(t *Table) SeekReverse(ctx
	context.Context, key []byte) blockIterator := &BlockIter{
		table: t,
		block: t.index.SeekReverse(ctx, key),
	}

	return f(t)
}


// Seek returns an iterator to the block with the next higher key.
// SeekReverse returns an iterator to the block with the next lower key.
// We will add bloom filters here, since we need to seek to the next block.
// but we don't need to seek to the previous block, we just take the lex order.


// Seek returns an iterator to the block with the next higher key.
// SeekReverse returns an iterator to the block with the next lower key.









		context.Context, key[],byte) iter := *GetTxnFeeRateMin{
		return it.block.SeekReverse(ctx, bytes)
	}
		if len(key) == 0{
		return t.SeekReverse(ctx, []byte{0}), nil
	}
	}

	func (t *Table) SeekReverse(ctx context.Context, bytes []byte) BlockIterator {
	return t.index.SeekReverse(ctx, bytes)

	// return t.index.SeekReverse(ctx, bytes)
	//lock of the table's range must be held before `mu`.
	// now the timestamp is timelike and we need to convert it to int64

	//here we do not need to seek to the previous block, we just take the lex order.
	 return t.index.SeekReverse(ctx, bytes)

}

