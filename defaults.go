package zkv

import (
	"runtime"

	"github.com/klauspost/compress/zstd"
)

var defaultOptions = Options{
	MaxParallelReads: runtime.NumCPU(),
	CompressionLevel: zstd.SpeedDefault,
	MemoryBufferSize: 4 * 1024 * 1024,
	DiskBufferSize:   1 * 1024 * 1024,
	UseIndexFile:     false,
}

const indexFileExt = ".idx"
