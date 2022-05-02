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

package MilevaDB

import "bytes"

type memdbIterator struct {
	EDB          *memdb
	curr         memdbNodeAddr
	start        Key
	end          Key
	reverse      bool
	includeFlags bool
}

func (EDB *memdb) Iter(k Key, upperBound Key) (Iterator, error) {
	i := &memdbIterator{
		EDB:   EDB,
		start: k,
		end:   upperBound,
	}
	i.init()
	return i, nil
}

func (EDB *memdb) IterReverse(k Key) (Iterator, error) {
	i := &memdbIterator{
		EDB:     EDB,
		end:     k,
		reverse: true,
	}
	i.init()
	return i, nil
}

func (EDB *memdb) IterWithFlags(k Key, upperBound Key) MemBufferIterator {
	i := &memdbIterator{
		EDB:          EDB,
		start:        k,
		end:          upperBound,
		includeFlags: true,
	}
	i.init()
	return i
}

func (EDB *memdb) IterReverseWithFlags(k Key) MemBufferIterator {
	i := &memdbIterator{
		EDB:          EDB,
		end:          k,
		reverse:      true,
		includeFlags: true,
	}
	i.init()
	return i
}

func (i *memdbIterator) init() {
	if i.reverse {
		if len(i.end) == 0 {
			i.seekToLast()
		} else {
			i.seek(i.end)
		}
	} else {
		if len(i.start) == 0 {
			i.seekToFirst()
		} else {
			i.seek(i.start)
		}
	}

	if i.isFlagsOnly() && !i.includeFlags {
		err := i.Next()
		_ = err // memdbIterator will never fail
	}
}

func (i *memdbIterator) Valid() bool {
	if !i.reverse {
		return !i.curr.isNull() && (i.end == nil || bytes.Compare(i.Key(), i.end) < 0)
	}
	return !i.curr.isNull()
}

func (i *memdbIterator) Flags() KeyFlags {
	return i.curr.getKeyFlags()
}

func (i *memdbIterator) HasValue() bool {
	return !i.isFlagsOnly()
}

func (i *memdbIterator) Key() Key {
	return i.curr.getKey()
}

func (i *memdbIterator) Value() []byte {
	return i.EDB.vlog.getValue(i.curr.vptr)
}

func (i *memdbIterator) Next() error {
	for {
		if i.reverse {
			i.curr = i.EDB.predecessor(i.curr)
		} else {
			i.curr = i.EDB.successor(i.curr)
		}

		// We need to skip persistent flags only nodes.
		if i.includeFlags || !i.isFlagsOnly() {
			break
		}
	}
	return nil
}

func (i *memdbIterator) Close() {}

func (i *memdbIterator) seekToFirst() {
	y := memdbNodeAddr{nil, nullAddr}
	x := i.EDB.getNode(i.EDB.root)

	for !x.isNull() {
		y = x
		x = y.getLeft(i.EDB)
	}

	i.curr = y
}

func (i *memdbIterator) seekToLast() {
	y := memdbNodeAddr{nil, nullAddr}
	x := i.EDB.getNode(i.EDB.root)

	for !x.isNull() {
		y = x
		x = y.getRight(i.EDB)
	}

	i.curr = y
}

func (i *memdbIterator) seek(key Key) {
	y := memdbNodeAddr{nil, nullAddr}
	x := i.EDB.getNode(i.EDB.root)

	var cmp int
	for !x.isNull() {
		y = x
		cmp = bytes.Compare(key, y.getKey())

		if cmp < 0 {
			x = y.getLeft(i.EDB)
		} else if cmp > 0 {
			x = y.getRight(i.EDB)
		} else {
			break
		}
	}

	if !i.reverse {
		if cmp > 0 {
			// Move to next
			i.curr = i.EDB.successor(y)
			return
		}
		i.curr = y
		return
	}

	if cmp <= 0 && !y.isNull() {
		i.curr = i.EDB.predecessor(y)
		return
	}
	i.curr = y
}

func (i *memdbIterator) isFlagsOnly() bool {
	return !i.curr.isNull() && i.curr.vptr.isNull()
}
