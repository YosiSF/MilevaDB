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

import "context"

func (EDB *memdb) SnapshotGetter() Getter {
	return &memdbSnapGetter{
		EDB: EDB,
		cp:  EDB.getSnapshot(),
	}
}

func (EDB *memdb) SnapshotIter(start, end Key) Iterator {
	it := &memdbSnapIter{
		memdbIterator: &memdbIterator{
			EDB:   EDB,
			start: start,
			end:   end,
		},
		cp: EDB.getSnapshot(),
	}
	it.init()
	return it
}

func (EDB *memdb) getSnapshot() memdbCheckpoint {
	if len(EDB.stages) > 0 {
		return EDB.stages[0]
	}
	return EDB.vlog.checkpoint()
}

type memdbSnapGetter struct {
	EDB *memdb
	cp  memdbCheckpoint
}

func (snap *memdbSnapGetter) Get(_ context.Context, key Key) ([]byte, error) {
	x := snap.EDB.traverse(key, false)
	if x.isNull() {
		return nil, ErrNotExist
	}
	if x.vptr.isNull() {
		// A flag only key, act as value not exists
		return nil, ErrNotExist
	}
	v, ok := snap.EDB.vlog.getSnapshotValue(x.vptr, &snap.cp)
	if !ok {
		return nil, ErrNotExist
	}
	return v, nil
}

type memdbSnapIter struct {
	*memdbIterator
	value []byte
	cp    memdbCheckpoint
}

func (i *memdbSnapIter) Value() []byte {
	return i.value
}

func (i *memdbSnapIter) Next() error {
	i.value = nil
	for i.Valid() {
		if err := i.memdbIterator.Next(); err != nil {
			return err
		}
		if i.setValue() {
			return nil
		}
	}
	return nil
}

func (i *memdbSnapIter) setValue() bool {
	if !i.Valid() {
		return false
	}
	if v, ok := i.EDB.vlog.getSnapshotValue(i.curr.vptr, &i.cp); ok {
		i.value = v
		return true
	}
	return false
}

func (i *memdbSnapIter) init() {
	if len(i.start) == 0 {
		i.seekToFirst()
	} else {
		i.seek(i.start)
	}

	if !i.setValue() {
		err := i.Next()
		_ = err // memdbIterator will never fail
	}
}
