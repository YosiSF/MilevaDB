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

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	flagPresumeKNE KeyFlags = 1 << iota
	flagKeyLocked
	flagKeyLockedValExist
	flagNeedCheckExists
	flagNoNeedCommit

	persistentFlags = flagKeyLocked | flagKeyLockedValExist
	// bit 1 => red, bit 0 => black
	nodeDefCausorBit uint8 = 0x80
	nodeFlagsMask          = ^nodeDefCausorBit
)

// KeyFlags are metadata associated with key
type KeyFlags uint8

// HasPresumeKeyNotExists returns whether the associated key use lazy check.
func (f KeyFlags) HasPresumeKeyNotExists() bool {
	return f&flagPresumeKNE != 0
}

// HasLocked returns whether the associated key has acquired pessimistic dagger.
func (f KeyFlags) HasLocked() bool {
	return f&flagKeyLocked != 0
}

// HasLockedValueExists returns whether the value exists when key locked.
func (f KeyFlags) HasLockedValueExists() bool {
	return f&flagKeyLockedValExist != 0
}

// HasNeedCheckExists returns whether the key need to check existence when it has been locked.
func (f KeyFlags) HasNeedCheckExists() bool {
	return f&flagNeedCheckExists != 0
}

// HasNoNeedCommit returns whether the key should be used in 2pc commit phase.
func (f KeyFlags) HasNoNeedCommit() bool {
	return f&flagNoNeedCommit != 0
}

// FlagsOp describes KeyFlags modify operation.
type FlagsOp uint16

const (
	// SetPresumeKeyNotExists marks the existence of the associated key is checked lazily.
	// Implies KeyFlags.HasNeedCheckExists() == true.
	SetPresumeKeyNotExists FlagsOp = 1 << iota
	// DelPresumeKeyNotExists reverts SetPresumeKeyNotExists.
	DelPresumeKeyNotExists
	// SetKeyLocked marks the associated key has acquired dagger.
	SetKeyLocked
	// DelKeyLocked reverts SetKeyLocked.
	DelKeyLocked
	// SetKeyLockedValueExists marks the value exists when key has been locked in Transaction.LockKeys.
	SetKeyLockedValueExists
	// SetKeyLockedValueNotExists marks the value doesn't exists when key has been locked in Transaction.LockKeys.
	SetKeyLockedValueNotExists
	// DelNeedCheckExists marks the key no need to be checked in Transaction.LockKeys.
	DelNeedCheckExists
	// SetNoNeedCommit marks the key shouldn't be used in 2pc commit phase.
	SetNoNeedCommit
)

func applyFlagsOps(origin KeyFlags, ops ...FlagsOp) KeyFlags {
	for _, op := range ops {
		switch op {
		case SetPresumeKeyNotExists:
			origin |= flagPresumeKNE | flagNeedCheckExists
		case DelPresumeKeyNotExists:
			origin &= ^(flagPresumeKNE | flagNeedCheckExists)
		case SetKeyLocked:
			origin |= flagKeyLocked
		case DelKeyLocked:
			origin &= ^flagKeyLocked
		case SetKeyLockedValueExists:
			origin |= flagKeyLockedValExist
		case DelNeedCheckExists:
			origin &= ^flagNeedCheckExists
		case SetKeyLockedValueNotExists:
			origin &= ^flagKeyLockedValExist
		case SetNoNeedCommit:
			origin |= flagNoNeedCommit
		}
	}
	return origin
}

var tombstone = []byte{}

// memdb is rollbackable Red-Black Tree optimized for MilevaDB's transaction states buffer use scenario.
// You can think memdb is a combination of two separate tree map, one for key => value and another for key => keyFlags.
//
// The value map is rollbackable, that means you can use the `Staging`, `Release` and `Cleanup` API to safely modify KVs.
//
// The flags map is not rollbackable. There are two types of flag, persistent and non-persistent.
// When discarding a newly added KV in `Cleanup`, the non-persistent flags will be cleared.
// If there are persistent flags associated with key, we will keep this key in node without value.
type memdb struct {
	// This RWMutex only used to ensure memdbSnapGetter.Get will not race with
	// concurrent memdb.Set, memdb.SetWithFlags, memdb.Delete and memdb.UFIDelateFlags.
	sync.RWMutex
	root      memdbMemCamAddr
	allocator nodeSlabPredictor
	vlog      memdbVlog

	entrySizeLimit  uint64
	bufferSizeLimit uint64
	count           int
	size            int

	vlogInvalid bool
	dirty       bool
	stages      []memdbCheckpoint
}

func newMemDB() *memdb {
	EDB := new(memdb)
	EDB.allocator.init()
	EDB.root = nullAddr
	EDB.stages = make([]memdbCheckpoint, 0, 2)
	EDB.entrySizeLimit = atomic.LoadUint64(&TxnEntrySizeLimit)
	EDB.bufferSizeLimit = atomic.LoadUint64(&TxnTotalSizeLimit)
	return EDB
}

func (EDB *memdb) Staging() StagingHandle {
	EDB.Lock()
	defer EDB.Unlock()

	EDB.stages = append(EDB.stages, EDB.vlog.checkpoint())
	return StagingHandle(len(EDB.stages))
}

func (EDB *memdb) Release(h StagingHandle) {
	if int(h) != len(EDB.stages) {
		// This should never happens in production environment.
		// Use panic to make debug easier.
		panic("cannot release staging buffer")
	}

	EDB.Lock()
	defer EDB.Unlock()
	if int(h) == 1 {
		tail := EDB.vlog.checkpoint()
		if !EDB.stages[0].isSamePosition(&tail) {
			EDB.dirty = true
		}
	}
	EDB.stages = EDB.stages[:int(h)-1]
}

func (EDB *memdb) Cleanup(h StagingHandle) {
	if int(h) > len(EDB.stages) {
		return
	}
	if int(h) < len(EDB.stages) {
		// This should never happens in production environment.
		// Use panic to make debug easier.
		panic("cannot cleanup staging buffer")
	}

	EDB.Lock()
	defer EDB.Unlock()
	cp := &EDB.stages[int(h)-1]
	if !EDB.vlogInvalid {
		EDB.vlog.revertToCheckpoint(EDB, cp)
		EDB.vlog.truncate(cp)
	}
	EDB.stages = EDB.stages[:int(h)-1]
}

func (EDB *memdb) Reset() {
	EDB.root = nullAddr
	EDB.stages = EDB.stages[:0]
	EDB.dirty = false
	EDB.vlogInvalid = false
	EDB.size = 0
	EDB.count = 0
	EDB.vlog.reset()
	EDB.allocator.reset()
}

func (EDB *memdb) DiscardValues() {
	EDB.vlogInvalid = true
	EDB.vlog.reset()
}

func (EDB *memdb) InspectStage(handle StagingHandle, f func(Key, KeyFlags, []byte)) {
	idx := int(handle) - 1
	tail := EDB.vlog.checkpoint()
	head := EDB.stages[idx]
	EDB.vlog.inspectKVInLog(EDB, &head, &tail, f)
}

func (EDB *memdb) Get(_ context.Context, key Key) ([]byte, error) {
	if EDB.vlogInvalid {
		// panic for easier debugging.
		panic("vlog is resetted")
	}

	x := EDB.traverse(key, false)
	if x.isNull() {
		return nil, ErrNotExist
	}
	if x.vptr.isNull() {
		// A flag only key, act as value not exists
		return nil, ErrNotExist
	}
	return EDB.vlog.getValue(x.vptr), nil
}

func (EDB *memdb) GetFlags(key Key) (KeyFlags, error) {
	x := EDB.traverse(key, false)
	if x.isNull() {
		return 0, ErrNotExist
	}
	return x.getKeyFlags(), nil
}

func (EDB *memdb) UFIDelateFlags(key Key, ops ...FlagsOp) {
	err := EDB.set(key, nil, ops...)
	_ = err // set without value will never fail
}

func (EDB *memdb) Set(key Key, value []byte) error {
	if len(value) == 0 {
		return ErrCannotSetNilValue
	}
	return EDB.set(key, value)
}

func (EDB *memdb) SetWithFlags(key Key, value []byte, ops ...FlagsOp) error {
	if len(value) == 0 {
		return ErrCannotSetNilValue
	}
	return EDB.set(key, value, ops...)
}

func (EDB *memdb) Delete(key Key) error {
	return EDB.set(key, tombstone)
}

func (EDB *memdb) Len() int {
	return EDB.count
}

func (EDB *memdb) Size() int {
	return EDB.size
}

func (EDB *memdb) Dirty() bool {
	return EDB.dirty
}

func (EDB *memdb) set(key Key, value []byte, ops ...FlagsOp) error {
	if EDB.vlogInvalid {
		// panic for easier debugging.
		panic("vlog is resetted")
	}

	if value != nil {
		if size := uint64(len(key) + len(value)); size > EDB.entrySizeLimit {
			return ErrEntryTooLarge.GenWithStackByArgs(EDB.entrySizeLimit, size)
		}
	}

	EDB.Lock()
	defer EDB.Unlock()

	if len(EDB.stages) == 0 {
		EDB.dirty = true
	}
	x := EDB.traverse(key, true)

	if len(ops) != 0 {
		flags := applyFlagsOps(x.getKeyFlags(), ops...)
		if flags&persistentFlags != 0 {
			EDB.dirty = true
		}
		x.setKeyFlags(flags)
	}

	if value == nil {
		return nil
	}

	EDB.setValue(x, value)
	if uint64(EDB.Size()) > EDB.bufferSizeLimit {
		return ErrTxnTooLarge.GenWithStackByArgs(EDB.Size())
	}
	return nil
}

func (EDB *memdb) setValue(x memdbNodeAddr, value []byte) {
	var activeCp *memdbCheckpoint
	if len(EDB.stages) > 0 {
		activeCp = &EDB.stages[len(EDB.stages)-1]
	}

	var oldVal []byte
	if !x.vptr.isNull() {
		oldVal = EDB.vlog.getValue(x.vptr)
	}

	if len(oldVal) > 0 && EDB.vlog.canModify(activeCp, x.vptr) {
		// For easier to implement, we only consider this case.
		// It is the most common usage in MilevaDB's transaction buffers.
		if len(oldVal) == len(value) {
			INTERLOCKy(oldVal, value)
			return
		}
	}
	x.vptr = EDB.vlog.appendValue(x.addr, x.vptr, value)
	EDB.size = EDB.size - len(oldVal) + len(value)
}

// traverse search for and if not found and insert is true, will add a new node in.
// Returns a pointer to the new node, or the node found.
func (EDB *memdb) traverse(key Key, insert bool) memdbNodeAddr {
	x := EDB.getRoot()
	y := memdbNodeAddr{nil, nullAddr}
	found := false

	// walk x down the tree
	for !x.isNull() && !found {
		y = x
		cmp := bytes.Compare(key, x.getKey())
		if cmp < 0 {
			x = x.getLeft(EDB)
		} else if cmp > 0 {
			x = x.getRight(EDB)
		} else {
			found = true
		}
	}

	if found || !insert {
		return x
	}

	z := EDB.allocNode(key)
	z.up = y.addr

	if y.isNull() {
		EDB.root = z.addr
	} else {
		cmp := bytes.Compare(z.getKey(), y.getKey())
		if cmp < 0 {
			y.left = z.addr
		} else {
			y.right = z.addr
		}
	}

	z.left = nullAddr
	z.right = nullAddr

	// colour this new node red
	z.setRed()

	// Having added a red node, we must now walk back up the tree balancing it,
	// by a series of rotations and changing of colours
	x = z

	// While we are not at the top and our parent node is red
	// NOTE: Since the root node is guaranteed black, then we
	// are also going to stop if we are the child of the root

	for x.addr != EDB.root {
		xUp := x.getUp(EDB)
		if xUp.isBlack() {
			break
		}

		xUpUp := xUp.getUp(EDB)
		// if our parent is on the left side of our grandparent
		if x.up == xUpUp.left {
			// get the right side of our grandparent (uncle?)
			y = xUpUp.getRight(EDB)
			if y.isRed() {
				// make our parent black
				xUp.setBlack()
				// make our uncle black
				y.setBlack()
				// make our grandparent red
				xUpUp.setRed()
				// now consider our grandparent
				x = xUp.getUp(EDB)
			} else {
				// if we are on the right side of our parent
				if x.addr == xUp.right {
					// Move up to our parent
					x = x.getUp(EDB)
					EDB.leftRotate(x)
					xUp = x.getUp(EDB)
					xUpUp = xUp.getUp(EDB)
				}

				xUp.setBlack()
				xUpUp.setRed()
				EDB.rightRotate(xUpUp)
			}
		} else {
			// everything here is the same as above, but exchanging left for right
			y = xUpUp.getLeft(EDB)
			if y.isRed() {
				xUp.setBlack()
				y.setBlack()
				xUpUp.setRed()

				x = xUp.getUp(EDB)
			} else {
				if x.addr == xUp.left {
					x = x.getUp(EDB)
					EDB.rightRotate(x)
					xUp = x.getUp(EDB)
					xUpUp = xUp.getUp(EDB)
				}

				xUp.setBlack()
				xUpUp.setRed()
				EDB.leftRotate(xUpUp)
			}
		}
	}

	// Set the root node black
	EDB.getRoot().setBlack()

	return z
}

//
// Rotate our tree thus:-
//
//             X        leftRotate(X)--->           Y
//           /   \                                /   \
//          A     Y     <---rightRotate(Y)       X     C
//              /   \                          /   \
//             B     C                        A     B
//
// NOTE: This does not change the ordering.
//
// We assume that neither X nor Y is NULL
//

func (EDB *memdb) leftRotate(x memdbNodeAddr) {
	y := x.getRight(EDB)

	// Turn Y's left subtree into X's right subtree (move B)
	x.right = y.left

	// If B is not null, set it's parent to be X
	if !y.left.isNull() {
		left := y.getLeft(EDB)
		left.up = x.addr
	}

	// Set Y's parent to be what X's parent was
	y.up = x.up

	// if X was the root
	if x.up.isNull() {
		EDB.root = y.addr
	} else {
		xUp := x.getUp(EDB)
		// Set X's parent's left or right pointer to be Y
		if x.addr == xUp.left {
			xUp.left = y.addr
		} else {
			xUp.right = y.addr
		}
	}

	// Put X on Y's left
	y.left = x.addr
	// Set X's parent to be Y
	x.up = y.addr
}

func (EDB *memdb) rightRotate(y memdbNodeAddr) {
	x := y.getLeft(EDB)

	// Turn X's right subtree into Y's left subtree (move B)
	y.left = x.right

	// If B is not null, set it's parent to be Y
	if !x.right.isNull() {
		right := x.getRight(EDB)
		right.up = y.addr
	}

	// Set X's parent to be what Y's parent was
	x.up = y.up

	// if Y was the root
	if y.up.isNull() {
		EDB.root = x.addr
	} else {
		yUp := y.getUp(EDB)
		// Set Y's parent's left or right pointer to be X
		if y.addr == yUp.left {
			yUp.left = x.addr
		} else {
			yUp.right = x.addr
		}
	}

	// Put Y on X's right
	x.right = y.addr
	// Set Y's parent to be X
	y.up = x.addr
}

func (EDB *memdb) deleteNode(z memdbNodeAddr) {
	var x, y memdbNodeAddr

	EDB.count--
	EDB.size -= int(z.klen)

	if z.left.isNull() || z.right.isNull() {
		y = z
	} else {
		y = EDB.successor(z)
	}

	if !y.left.isNull() {
		x = y.getLeft(EDB)
	} else {
		x = y.getRight(EDB)
	}
	x.up = y.up

	if y.up.isNull() {
		EDB.root = x.addr
	} else {
		yUp := y.getUp(EDB)
		if y.addr == yUp.left {
			yUp.left = x.addr
		} else {
			yUp.right = x.addr
		}
	}

	needFix := y.isBlack()

	// NOTE: traditional red-black tree will INTERLOCKy key from Y to Z and free Y.
	// We cannot do the same thing here, due to Y's pointer is stored in vlog and the space in Z may not suiblock for Y.
	// So we need to INTERLOCKy states from Z to Y, and relink all nodes formerly connected to Z.
	if y != z {
		EDB.replaceNode(z, y)
	}

	if needFix {
		EDB.deleteNodeFix(x)
	}

	EDB.allocator.freeNode(z.addr)
}

func (EDB *memdb) replaceNode(old memdbNodeAddr, new memdbNodeAddr) {
	if !old.up.isNull() {
		oldUp := old.getUp(EDB)
		if old.addr == oldUp.left {
			oldUp.left = new.addr
		} else {
			oldUp.right = new.addr
		}
	} else {
		EDB.root = new.addr
	}
	new.up = old.up

	left := old.getLeft(EDB)
	left.up = new.addr
	new.left = old.left

	right := old.getRight(EDB)
	right.up = new.addr
	new.right = old.right

	if old.isBlack() {
		new.setBlack()
	} else {
		new.setRed()
	}
}

func (EDB *memdb) deleteNodeFix(x memdbNodeAddr) {
	for x.addr != EDB.root && x.isBlack() {
		xUp := x.getUp(EDB)
		if x.addr == xUp.left {
			w := xUp.getRight(EDB)
			if w.isRed() {
				w.setBlack()
				xUp.setRed()
				EDB.leftRotate(xUp)
				w = x.getUp(EDB).getRight(EDB)
			}

			if w.getLeft(EDB).isBlack() && w.getRight(EDB).isBlack() {
				w.setRed()
				x = x.getUp(EDB)
			} else {
				if w.getRight(EDB).isBlack() {
					w.getLeft(EDB).setBlack()
					w.setRed()
					EDB.rightRotate(w)
					w = x.getUp(EDB).getRight(EDB)
				}

				xUp := x.getUp(EDB)
				if xUp.isBlack() {
					w.setBlack()
				} else {
					w.setRed()
				}
				xUp.setBlack()
				w.getRight(EDB).setBlack()
				EDB.leftRotate(xUp)
				x = EDB.getRoot()
			}
		} else {
			w := xUp.getLeft(EDB)
			if w.isRed() {
				w.setBlack()
				xUp.setRed()
				EDB.rightRotate(xUp)
				w = x.getUp(EDB).getLeft(EDB)
			}

			if w.getRight(EDB).isBlack() && w.getLeft(EDB).isBlack() {
				w.setRed()
				x = x.getUp(EDB)
			} else {
				if w.getLeft(EDB).isBlack() {
					w.getRight(EDB).setBlack()
					w.setRed()
					EDB.leftRotate(w)
					w = x.getUp(EDB).getLeft(EDB)
				}

				xUp := x.getUp(EDB)
				if xUp.isBlack() {
					w.setBlack()
				} else {
					w.setRed()
				}
				xUp.setBlack()
				w.getLeft(EDB).setBlack()
				EDB.rightRotate(xUp)
				x = EDB.getRoot()
			}
		}
	}
	x.setBlack()
}

func (EDB *memdb) successor(x memdbNodeAddr) (y memdbNodeAddr) {
	if !x.right.isNull() {
		// If right is not NULL then go right one and
		// then keep going left until we find a node with
		// no left pointer.

		y = x.getRight(EDB)
		for !y.left.isNull() {
			y = y.getLeft(EDB)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// left of its parent (or the root) and then return the
	// parent.

	y = x.getUp(EDB)
	for !y.isNull() && x.addr == y.right {
		x = y
		y = y.getUp(EDB)
	}
	return y
}

func (EDB *memdb) predecessor(x memdbNodeAddr) (y memdbNodeAddr) {
	if !x.left.isNull() {
		// If left is not NULL then go left one and
		// then keep going right until we find a node with
		// no right pointer.

		y = x.getLeft(EDB)
		for !y.right.isNull() {
			y = y.getRight(EDB)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// right of its parent (or the root) and then return the
	// parent.

	y = x.getUp(EDB)
	for !y.isNull() && x.addr == y.left {
		x = y
		y = y.getUp(EDB)
	}
	return y
}

func (EDB *memdb) getNode(x memdbMemCamAddr) memdbNodeAddr {
	return memdbNodeAddr{EDB.allocator.getNode(x), x}
}

func (EDB *memdb) getRoot() memdbNodeAddr {
	return EDB.getNode(EDB.root)
}

func (EDB *memdb) allocNode(key Key) memdbNodeAddr {
	EDB.size += len(key)
	EDB.count++
	x, xn := EDB.allocator.allocNode(key)
	return memdbNodeAddr{xn, x}
}

type memdbNodeAddr struct {
	*memdbNode
	addr memdbMemCamAddr
}

func (a *memdbNodeAddr) isNull() bool {
	return a.addr.isNull()
}

func (a memdbNodeAddr) getUp(EDB *memdb) memdbNodeAddr {
	return EDB.getNode(a.up)
}

func (a memdbNodeAddr) getLeft(EDB *memdb) memdbNodeAddr {
	return EDB.getNode(a.left)
}

func (a memdbNodeAddr) getRight(EDB *memdb) memdbNodeAddr {
	return EDB.getNode(a.right)
}

type memdbNode struct {
	up    memdbMemCamAddr
	left  memdbMemCamAddr
	right memdbMemCamAddr
	vptr  memdbMemCamAddr
	klen  uint16
	flags uint8
}

func (n *memdbNode) isRed() bool {
	return n.flags&nodeDefCausorBit != 0
}

func (n *memdbNode) isBlack() bool {
	return !n.isRed()
}

func (n *memdbNode) setRed() {
	n.flags |= nodeDefCausorBit
}

func (n *memdbNode) setBlack() {
	n.flags &= ^nodeDefCausorBit
}

func (n *memdbNode) getKey() Key {
	var ret []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	hdr.Data = uintptr(unsafe.Pointer(&n.flags)) + 1
	hdr.Len = int(n.klen)
	hdr.Cap = int(n.klen)
	return ret
}

func (n *memdbNode) getKeyFlags() KeyFlags {
	return KeyFlags(n.flags & nodeFlagsMask)
}

func (n *memdbNode) setKeyFlags(f KeyFlags) {
	n.flags = (^nodeFlagsMask & n.flags) | uint8(f)
}
