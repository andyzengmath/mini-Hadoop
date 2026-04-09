package rpc

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
)

const dashboardHTML = `<!DOCTYPE html>
<html>
<head>
<title>mini-Hadoop — {{.Component}}</title>
<meta http-equiv="refresh" content="5">
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 40px; background: #f5f5f5; }
  h1 { color: #333; border-bottom: 2px solid #e74c3c; padding-bottom: 10px; }
  .card { background: white; border-radius: 8px; padding: 20px; margin: 15px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
  .card h2 { margin-top: 0; color: #555; font-size: 14px; text-transform: uppercase; letter-spacing: 1px; }
  .metrics { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 15px; }
  .metric { text-align: center; }
  .metric .value { font-size: 36px; font-weight: bold; color: #2c3e50; }
  .metric .label { font-size: 12px; color: #777; margin-top: 4px; }
  .green { color: #27ae60 !important; }
  .red { color: #e74c3c !important; }
  .yellow { color: #f39c12 !important; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #eee; }
  th { background: #fafafa; font-weight: 600; font-size: 13px; color: #555; }
  .footer { margin-top: 30px; font-size: 12px; color: #999; }
</style>
</head>
<body>
<h1>mini-Hadoop: {{.Component}}</h1>
<div class="card">
  <h2>Metrics</h2>
  <div class="metrics">
  {{range $key, $val := .Metrics}}
    <div class="metric">
      <div class="value">{{$val}}</div>
      <div class="label">{{$key}}</div>
    </div>
  {{end}}
  </div>
</div>
<div class="footer">Auto-refreshes every 5 seconds. <a href="/metrics">JSON metrics</a> | <a href="/health">Health check</a></div>
</body>
</html>`

var dashTmpl = template.Must(template.New("dashboard").Parse(dashboardHTML))

// StartDashboard adds a web dashboard to the metrics HTTP server.
// Call this INSTEAD of StartMetricsServer if you want both metrics + UI.
func StartDashboard(port int, component string, provider MetricsProvider) {
	mux := http.NewServeMux()

	// JSON metrics endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		m := Metrics{
			Component: component,
			Metrics:   provider.GetMetrics(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m)
	})

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Dashboard UI
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data := struct {
			Component string
			Metrics   map[string]interface{}
		}{
			Component: component,
			Metrics:   provider.GetMetrics(),
		}
		w.Header().Set("Content-Type", "text/html")
		dashTmpl.Execute(w, data)
	})

	addr := fmt.Sprintf(":%d", port)
	go func() {
		slog.Info("dashboard starting", "address", addr, "url", fmt.Sprintf("http://localhost:%d", port))
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Warn("dashboard stopped", "error", err)
		}
	}()
}
