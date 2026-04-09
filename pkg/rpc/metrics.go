package rpc

// Metrics holds key-value metrics for a component.
type Metrics struct {
	Component string                 `json:"component"`
	Metrics   map[string]interface{} `json:"metrics"`
}

// MetricsProvider is implemented by components that expose metrics.
type MetricsProvider interface {
	GetMetrics() map[string]interface{}
}
