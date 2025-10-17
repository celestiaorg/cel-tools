package registry

type MetricProvider interface {
	// Latency is a download time in milliseconds
	Latency() float64
	// TotalBytes is the response size
	TotalBytes() int64
	// Speed calculates how many bytes were received per 1ms
	Speed() float64
}

type concreteProvider struct {
	latency    float64
	totalBytes int64
}

func (c *concreteProvider) Latency() float64 {
	return c.latency
}

func (c *concreteProvider) TotalBytes() int64 {
	return c.totalBytes
}

func (c *concreteProvider) Speed() float64 {
	return float64(c.totalBytes) / c.latency
}
