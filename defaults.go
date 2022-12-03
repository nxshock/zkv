package zkv

import (
	"runtime"

	"github.com/klauspost/compress/zstd"
)

var defaultOptions = Options{
	MaxParallelReads: runtime.NumCPU(),
	CompressionLevel: zstd.SpeedDefault,
	BufferSize:       4 * 1024 * 1024,
}
