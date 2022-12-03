package zkv

type Options struct {
	MaxParallelReads uint
}

func (o *Options) Validate() error {
	if o.MaxParallelReads == 0 {
		o.MaxParallelReads = defaultOptions.MaxParallelReads
	}

	return nil
}
