package rpc

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
)

// Metrics holds key-value metrics for a component.
type Metrics struct {
	Component string            `json:"component"`
	Metrics   map[string]interface{} `json:"metrics"`
}

// MetricsProvider is implemented by components that expose metrics.
type MetricsProvider interface {
	GetMetrics() map[string]interface{}
}

// StartMetricsServer starts a lightweight HTTP server serving JSON metrics
// on the given port at /metrics. Non-blocking (runs in a goroutine).
func StartMetricsServer(port int, component string, provider MetricsProvider) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		m := Metrics{
			Component: component,
			Metrics:   provider.GetMetrics(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	addr := fmt.Sprintf(":%d", port)
	go func() {
		slog.Info("metrics server starting", "address", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Warn("metrics server stopped", "error", err)
		}
	}()
}
