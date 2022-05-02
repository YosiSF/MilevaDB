//MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package MilevaDB

import (


	_ "fmt"
	_ "math/rand"
	_ "runtime"
	_ "strconv"
	_ "strings"
	_ "sync"
	_ "sync"
	_ "sync/atomic"
	_ "time"
)

type HashFunc func([]byte) uint32  //


//Instead of timezones or lexicographic partitions of semantic indexing based on preorder vocabulary with turing alphabet
//we use a hash function to partition the semantic indexing based on the hash value of the key.
//The hash function is a cryptographic hash function that is designed to be resistant to the effects of cryptographic
//attacks.
//The hash function is a cryptographic hash function that is designed to be resistant to the effects of cryptographic
//attacks.
//The hash function is a cryptographic hash function that is designed to be resistant to the effects of cryptographic

type HashFuncs []HashFuncs // []HashFunc
type Hash struct {
	hashFunc HashFuncs // hash function
}

	const (
	// DefaultHashTableSize is the default hash table size.
	DefaultHashTableSize = 1 << 18
)

type word []byte



const (
	// HashShardBits represents the number of bits used for sharding.
	HashShardBits = uint(10)
	// HashShardMask is a bitmask of HashShardBits size used for sharding.
	HashShardMask = uint32(1<<HashShardBits - 1)
	// HashMaxShard is the maximum number of shards.
	HashMaxShard = 1 << HashShardBits
)



	// DefaultLoadFactor is the default hash table load factor.

	// DefaultMaxLoadFactor is the default maximum hash table load factor.
	DefaultMaxLoadFactor = 0.98
	// DefaultMaxEntriesCoeff is the default maximum entries coefficient.
	DefaultMaxEntriesCoeff = 5
	// DefaultCacheSize is the default cache size.
	DefaultCacheSize = 512 * 1024 * 1024

	DefaultCacheSize = 512 * 1024 * 1024

	DefaultLoadFactor = 0.75
	private           word
)
