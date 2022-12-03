package zkv

import "github.com/klauspost/compress/zstd"

type Options struct {
	// Maximum number of concurrent reads
	MaxParallelReads uint

	// Compression level
	CompressionLevel zstd.EncoderLevel
}

func (o *Options) setDefaults() {
	if o.MaxParallelReads == 0 {
		o.MaxParallelReads = defaultOptions.MaxParallelReads
	}

	if o.CompressionLevel == 0 {
		o.CompressionLevel = defaultOptions.CompressionLevel
	}
}
