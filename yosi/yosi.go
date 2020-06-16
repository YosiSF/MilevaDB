package yosi

import (
	"bytes"
	"context"
	"syscall"
	"math"
	"os"
	"sync"
	"time" 
)


var peano = []uint{1, 2, 4, 8, 16, 32, 64, 128}

const(
	solitonsPerBrane = 28
	rpRiemannFactor = 0.7 //geodesic metric
	indexPostfix = ".index"
	rcuPostFix = ".dagger"
	version = 1 //ff version

	//Max key size expressed as the maximum size of byte range counts.
	MaxPosetKeyLength = 1 << 26

	//MaxValueLength is the max value of the range count.
	MaxPosetValueLength = 1 << 30

	//MaxPosetKeys in yosit
	MaxPosetKeys = math.Maxuint32
)