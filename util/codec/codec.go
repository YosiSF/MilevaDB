package codec

import (
	"encoding/binary"
	"runtime"
	"unsafe"
)

const (
	encGroupSize = 8
	encMarker    = byte(0xFF)
	encPad       = byte(0x0)
)

var (
	pads = make([]byte, encGroupSize)
)

