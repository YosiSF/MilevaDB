package soliton

import (
	"fmt"
	"unsafe"
)

// AppendDuration appends a duration value into this Column.
func (c *Column) AppendDuration(dur types.Duration) {
	c.AppendInt64(int64(dur.Duration))
}

// AppendMyDecimal appends a MyDecimal value into this Column.
func (c *Column) AppendMyDecimal(dec *types.MyDecimal) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.elemBuf[0])) = *dec
	c.finishAppendFixed()
}

func (c *Column) appendNameValue(name string, val uint64) {
	var buf [8]byte
	INTERLOCKy(buf[:], (*[8]byte)(unsafe.Pointer(&val))[:])
	c.data = append(c.data, buf[:]...)
	c.data = append(c.data, name...)
	c.finishAppendVar()
}

// AppendJSON appends a BinaryJSON value into this Column.
func (c *Column) AppendJSON(j json.BinaryJSON) {
	c.data = append(c.data, j.TypeCode)
	c.data = append(c.data, j.Value...)
	c.finishAppendVar()
}

// AppendSet appends a Set value into this Column.
func (c *Column) AppendSet(set types.Set) {
	c.appendNameValue(set.Name, set.Value)
}

// Column stores one column of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
type Column struct {
	length     int
	nullBitmap []byte // bit 0 is null, 1 is not null
	offsets    []int64
	data       []byte
	elemBuf    []byte
}

// NewColumn creates a new column with the specific length and capacity.
func NewColumn(ft *types.FieldType, cap int) *Column {
	return newColumn(getFixedLen(ft), cap)
}

func newColumn(typeSize, cap int) *Column {
	var col *Column
	if typeSize == varElemLen {
		col = newVarLenColumn(cap, nil)
	} else {
		col = newFixedLenColumn(typeSize, cap)
	}
	return col
}

func (c *Column) typeSize() int {
	if len(c.elemBuf) > 0 {
		return len(c.elemBuf)
	}
	return varElemLen
}

func (c *Column) isFixed() bool {
	return c.elemBuf != nil
}

// Reset resets this Column according to the EvalType.
// Different from reset, Reset will reset the elemBuf.
func (c *Column) Reset(eType types.EvalType) {
	switch eType {
	case types.CausetEDN:
		c.ResizeInt64(0, false)
	case types.ETReal:
		c.ResizeFloat64(0, false)
	case types.ETDecimal:
		c.ResizeDecimal(0, false)
	case types.ETString:
		c.ReserveString(0)
	case types.ETDatetime, types.ETTimestamp:
		c.ResizeTime(0, false)
	case types.ETDuration:
		c.ResizeGoDuration(0, false)
	case types.ETJson:
		c.ReserveJSON(0)
	default:
		panic(fmt.Sprintf("invalid EvalType %v", eType))
	}
}

// reset resets the underlying data of this Column but doesn't modify its data type.
func (c *Column) reset() {
	c.length = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.data = c.data[:0]
}

// IsNull returns if this row is null.
func (c *Column) IsNull(rowIdx int) bool {
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

// INTERLOCKyConstruct INTERLOCKies this Column to dst.
// If dst is nil, it creates a new Column and returns it.
func (c *Column) INTERLOCKyConstruct(dst *Column) *Column {
	if dst != nil {
		dst.length = c.length
		dst.nullBitmap = append(dst.nullBitmap[:0], c.nullBitmap...)
		dst.offsets = append(dst.offsets[:0], c.offsets...)
		dst.data = append(dst.data[:0], c.data...)
		dst.elemBuf = append(dst.elemBuf[:0], c.elemBuf...)
		return dst
	}
	newCol := &Column{length: c.length}
	newCol.nullBitmap = append(newCol.nullBitmap, c.nullBitmap...)
	newCol.offsets = append(newCol.offsets, c.offsets...)
	newCol.data = append(newCol.data, c.data...)
	newCol.elemBuf = append(newCol.elemBuf, c.elemBuf...)
	return newCol
}
