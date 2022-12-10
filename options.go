package zkv

import "github.com/klauspost/compress/zstd"

type Options struct {
	// Maximum number of concurrent reads
	MaxParallelReads int

	// Compression level
	CompressionLevel zstd.EncoderLevel

	// Memory write buffer size in bytes
	MemoryBufferSize int

	// Disk write buffer size in bytes
	DiskBufferSize int
}

func (o *Options) setDefaults() {
	if o.MaxParallelReads == 0 {
		o.MaxParallelReads = defaultOptions.MaxParallelReads
	}

	if o.CompressionLevel == 0 {
		o.CompressionLevel = defaultOptions.CompressionLevel
	}
}
