package cas

// Option configures a Cache.
type Option interface {
	apply(*Cache)
}

// WithSource registers a source for the given URI scheme.
// For example, WithSource("gs", gcsSource) enables "gs://bucket/object" URIs.
func WithSource(scheme string, s Source) Option {
	return withSourceOption{scheme: scheme, source: s}
}

type withSourceOption struct {
	scheme string
	source Source
}

func (o withSourceOption) apply(c *Cache) {
	c.sources[o.scheme] = o.source
}
