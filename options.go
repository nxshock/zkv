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

	// Use index file
	useIndexFile bool
}

func (o *Options) setDefaults() {
	o.useIndexFile = true // TODO: implement database search without index

	if o.MaxParallelReads == 0 {
		o.MaxParallelReads = defaultOptions.MaxParallelReads
	}

	if o.CompressionLevel == 0 {
		o.CompressionLevel = defaultOptions.CompressionLevel
	}

	if o.MemoryBufferSize == 0 {
		o.MemoryBufferSize = defaultOptions.MemoryBufferSize
	}

	if o.DiskBufferSize == 0 {
		o.DiskBufferSize = defaultOptions.DiskBufferSize
	}
}
