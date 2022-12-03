package zkv

import "github.com/klauspost/compress/zstd"

var defaultOptions = Options{
	MaxParallelReads: 64,
	CompressionLevel: zstd.SpeedDefault,
}
