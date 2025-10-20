package registry

type metricProvider struct {
	latency    float64
	totalBytes int64
}

// Latency returns a download time in milliseconds
func (m *metricProvider) Latency() float64 {
	return m.latency
}

// TotalBytes returns the total response size
func (m *metricProvider) TotalBytes() int64 {
	return m.totalBytes
}

// Speed returns how many bytes were received per 1ms
func (m *metricProvider) Speed() float64 {
	return float64(m.totalBytes) / m.latency
}
