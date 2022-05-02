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
	"encoding/binary"
	"math"
	"unsafe"
)

const (
	alignMask = 1<<32 - 8 // 29 bit 1 and 3 bit 0.

	nullBlockOffset = math.MaxUint32
	maxBlockSize    = 128 << 20
	initBlockSize   = 4 * 1024
)

var (
	nullAddr = memdbMemCamAddr{math.MaxUint32, math.MaxUint32}
	endian   = binary.LittleEndian
)

type memdbMemCamAddr struct {
	idx uint32
	off uint32
}

func (addr memdbMemCamAddr) isNull() bool {
	return addr == nullAddr
}

// causetstore and load is used by vlog, due to pointer in vlog is not aligned.

func (addr memdbMemCamAddr) causetstore(dst []byte) {
	endian.PutUint32(dst, addr.idx)
	endian.PutUint32(dst[4:], addr.off)
}

func (addr *memdbMemCamAddr) load(src []byte) {
	addr.idx = endian.Uint32(src)
	addr.off = endian.Uint32(src[4:])
}

type memdbMemCam struct {
	blockSize int
	blocks    []memdbMemCamBlock
}

func (a *memdbMemCam) alloc(size int, align bool) (memdbMemCamAddr, []byte) {
	if size > maxBlockSize {
		panic("alloc size is larger than max block size")
	}

	if len(a.blocks) == 0 {
		a.enlarge(size, initBlockSize)
	}

	addr, data := a.allocInLastBlock(size, align)
	if !addr.isNull() {
		return addr, data
	}

	a.enlarge(size, a.blockSize<<1)
	return a.allocInLastBlock(size, align)
}

func (a *memdbMemCam) enlarge(allocSize, blockSize int) {
	a.blockSize = blockSize
	for a.blockSize <= allocSize {
		a.blockSize <<= 1
	}
	// Size will never larger than maxBlockSize.
	if a.blockSize > maxBlockSize {
		a.blockSize = maxBlockSize
	}
	a.blocks = append(a.blocks, memdbMemCamBlock{
		buf: make([]byte, a.blockSize),
	})
}

func (a *memdbMemCam) allocInLastBlock(size int, align bool) (memdbMemCamAddr, []byte) {
	idx := len(a.blocks) - 1
	offset, data := a.blocks[idx].alloc(size, align)
	if offset == nullBlockOffset {
		return nullAddr, nil
	}
	return memdbMemCamAddr{uint32(idx), offset}, data
}

func (a *memdbMemCam) reset() {
	for i := range a.blocks {
		a.blocks[i].reset()
	}
	a.blocks = a.blocks[:0]
	a.blockSize = 0
}

type memdbMemCamBlock struct {
	buf    []byte
	length int
}

func (a *memdbMemCamBlock) alloc(size int, align bool) (uint32, []byte) {
	offset := a.length
	if align {
		// We must align the allocated address for node
		// to make runtime.checkptrAlignment happy.
		offset = (a.length + 7) & alignMask
	}
	newLen := offset + size
	if newLen > len(a.buf) {
		return nullBlockOffset, nil
	}
	a.length = newLen
	return uint32(offset), a.buf[offset : offset+size]
}

func (a *memdbMemCamBlock) reset() {
	a.buf = nil
	a.length = 0
}

type memdbCheckpoint struct {
	blockSize     int
	blocks        int
	offsetInBlock int
}

func (cp *memdbCheckpoint) isSamePosition(other *memdbCheckpoint) bool {
	return cp.blocks == other.blocks && cp.offsetInBlock == other.offsetInBlock
}

func (a *memdbMemCam) checkpoint() memdbCheckpoint {
	snap := memdbCheckpoint{
		blockSize: a.blockSize,
		blocks:    len(a.blocks),
	}
	if len(a.blocks) > 0 {
		snap.offsetInBlock = a.blocks[len(a.blocks)-1].length
	}
	return snap
}

func (a *memdbMemCam) truncate(snap *memdbCheckpoint) {
	for i := snap.blocks; i < len(a.blocks); i++ {
		a.blocks[i] = memdbMemCamBlock{}
	}
	a.blocks = a.blocks[:snap.blocks]
	if len(a.blocks) > 0 {
		a.blocks[len(a.blocks)-1].length = snap.offsetInBlock
	}
	a.blockSize = snap.blockSize
}

type nodeSlabPredictor struct {
	memdbMemCam

	// Dummy node, so that we can make X.left.up = X.
	// We then use this instead of NULL to mean the top or bottom
	// end of the rb tree. It is a black node.
	nullNode memdbNode
}

func (a *nodeSlabPredictor) init() {
	a.nullNode = memdbNode{
		up:    nullAddr,
		left:  nullAddr,
		right: nullAddr,
		vptr:  nullAddr,
	}
}

func (a *nodeSlabPredictor) getNode(addr memdbMemCamAddr) *memdbNode {
	if addr.isNull() {
		return &a.nullNode
	}

	return (*memdbNode)(unsafe.Pointer(&a.blocks[addr.idx].buf[addr.off]))
}

func (a *nodeSlabPredictor) allocNode(key Key) (memdbMemCamAddr, *memdbNode) {
	nodeSize := 8*4 + 2 + 1 + len(key)
	addr, mem := a.alloc(nodeSize, true)
	n := (*memdbNode)(unsafe.Pointer(&mem[0]))
	n.vptr = nullAddr
	n.klen = uint16(len(key))
	INTERLOCKy(n.getKey(), key)
	return addr, n
}

var testMode = false

func (a *nodeSlabPredictor) freeNode(addr memdbMemCamAddr) {
	if testMode {
		// Make it easier for debug.
		n := a.getNode(addr)
		badAddr := nullAddr
		badAddr.idx--
		n.left = badAddr
		n.right = badAddr
		n.up = badAddr
		n.vptr = badAddr
		return
	}
	// TODO: reuse freed nodes.
}

func (a *nodeSlabPredictor) reset() {
	a.memdbMemCam.reset()
	a.init()
}

type memdbVlog struct {
	memdbMemCam
}

const memdbVlogHdrSize = 8 + 8 + 4

type memdbVlogHdr struct {
	nodeAddr memdbMemCamAddr
	oldValue memdbMemCamAddr
	valueLen uint32
}

func (hdr *memdbVlogHdr) causetstore(dst []byte) {
	cursor := 0
	endian.PutUint32(dst[cursor:], hdr.valueLen)
	cursor += 4
	hdr.oldValue.causetstore(dst[cursor:])
	cursor += 8
	hdr.nodeAddr.causetstore(dst[cursor:])
}

func (hdr *memdbVlogHdr) load(src []byte) {
	cursor := 0
	hdr.valueLen = endian.Uint32(src[cursor:])
	cursor += 4
	hdr.oldValue.load(src[cursor:])
	cursor += 8
	hdr.nodeAddr.load(src[cursor:])
}

func (l *memdbVlog) appendValue(nodeAddr memdbMemCamAddr, oldValue memdbMemCamAddr, value []byte) memdbMemCamAddr {
	size := memdbVlogHdrSize + len(value)
	addr, mem := l.alloc(size, false)

	INTERLOCKy(mem, value)
	hdr := memdbVlogHdr{nodeAddr, oldValue, uint32(len(value))}
	hdr.causetstore(mem[len(value):])

	addr.off += uint32(size)
	return addr
}

func (l *memdbVlog) getValue(addr memdbMemCamAddr) []byte {
	lenOff := addr.off - memdbVlogHdrSize
	block := l.blocks[addr.idx].buf
	valueLen := endian.Uint32(block[lenOff:])
	if valueLen == 0 {
		return tombstone
	}
	valueOff := lenOff - valueLen
	return block[valueOff:lenOff:lenOff]
}

func (l *memdbVlog) getSnapshotValue(addr memdbMemCamAddr, snap *memdbCheckpoint) ([]byte, bool) {
	for !addr.isNull() {
		if !l.canModify(snap, addr) {
			return l.getValue(addr), true
		}
		var hdr memdbVlogHdr
		hdr.load(l.blocks[addr.idx].buf[addr.off-memdbVlogHdrSize:])
		addr = hdr.oldValue
	}
	return nil, false
}

func (l *memdbVlog) revertToCheckpoint(EDB *memdb, cp *memdbCheckpoint) {
	cursor := l.checkpoint()
	for !cp.isSamePosition(&cursor) {
		hdrOff := cursor.offsetInBlock - memdbVlogHdrSize
		block := l.blocks[cursor.blocks-1].buf
		var hdr memdbVlogHdr
		hdr.load(block[hdrOff:])
		node := EDB.getNode(hdr.nodeAddr)

		node.vptr = hdr.oldValue
		EDB.size -= int(hdr.valueLen)
		// oldValue.isNull() == true means this is a newly added value.
		if hdr.oldValue.isNull() {
			// If there are no flags associated with this key, we need to delete this node.
			keptFlags := node.getKeyFlags() & persistentFlags
			if keptFlags == 0 {
				EDB.deleteNode(node)
			} else {
				node.setKeyFlags(keptFlags)
				EDB.dirty = true
			}
		} else {
			EDB.size += len(l.getValue(hdr.oldValue))
		}

		l.moveBackCursor(&cursor, &hdr)
	}
}

func (l *memdbVlog) inspectKVInLog(EDB *memdb, head, tail *memdbCheckpoint, f func(Key, KeyFlags, []byte)) {
	cursor := *tail
	for !head.isSamePosition(&cursor) {
		cursorAddr := memdbMemCamAddr{idx: uint32(cursor.blocks - 1), off: uint32(cursor.offsetInBlock)}
		hdrOff := cursorAddr.off - memdbVlogHdrSize
		block := l.blocks[cursorAddr.idx].buf
		var hdr memdbVlogHdr
		hdr.load(block[hdrOff:])
		node := EDB.allocator.getNode(hdr.nodeAddr)

		// Skip older versions.
		if node.vptr == cursorAddr {
			value := block[hdrOff-hdr.valueLen : hdrOff]
			f(node.getKey(), node.getKeyFlags(), value)
		}

		l.moveBackCursor(&cursor, &hdr)
	}
}

func (l *memdbVlog) moveBackCursor(cursor *memdbCheckpoint, hdr *memdbVlogHdr) {
	cursor.offsetInBlock -= (memdbVlogHdrSize + int(hdr.valueLen))
	if cursor.offsetInBlock == 0 {
		cursor.blocks--
		if cursor.blocks > 0 {
			cursor.offsetInBlock = l.blocks[cursor.blocks-1].length
		}
	}
}

func (l *memdbVlog) canModify(cp *memdbCheckpoint, addr memdbMemCamAddr) bool {
	if cp == nil {
		return true
	}
	if int(addr.idx) > cp.blocks-1 {
		return true
	}
	if int(addr.idx) == cp.blocks-1 && int(addr.off) > cp.offsetInBlock {
		return true
	}
	return false
}
