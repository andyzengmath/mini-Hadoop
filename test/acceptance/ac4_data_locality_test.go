package acceptance

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// TestAC4_DataLocality tests AC-4: During a MapReduce job, verify that >70%
// of map tasks are scheduled on nodes where their input blocks reside.
// Parses structured locality logs emitted by MRAppMaster.
func TestAC4_DataLocality(t *testing.T) {
	if os.Getenv("MINIHADOOP_CLUSTER") == "" {
		t.Skip("Skipping: requires running cluster (set MINIHADOOP_CLUSTER=1)")
	}

	// In a Docker cluster test, the MRAppMaster emits structured JSON logs.
	// We capture these and parse for locality stats.
	//
	// Expected log format (from MRAppMaster.emitLocalityStats):
	// {"level":"INFO","msg":"locality_stats","job_id":"...","total_map_tasks":N,"data_local":M,"data_local_percentage":"P.P"}
	//
	// For local testing, we parse a log file if provided via env var.
	logFile := os.Getenv("MINIHADOOP_AM_LOG")
	if logFile == "" {
		t.Skip("Skipping: set MINIHADOOP_AM_LOG to the ApplicationMaster log file path")
	}

	t.Logf("Parsing AM log file: %s", logFile)

	stats, err := parseLocalityStats(logFile)
	if err != nil {
		t.Fatalf("parse locality stats: %v", err)
	}

	if stats.TotalMapTasks == 0 {
		t.Fatal("no locality_stats found in log file")
	}

	t.Logf("Locality stats: total=%d, data_local=%d, percentage=%.1f%%",
		stats.TotalMapTasks, stats.DataLocal, stats.Percentage)

	if stats.Percentage < 70.0 {
		t.Fatalf("AC-4 FAILED: Data locality %.1f%% is below 70%% threshold", stats.Percentage)
	}

	t.Logf("AC-4 PASSED: Data locality %.1f%% exceeds 70%% threshold", stats.Percentage)
}

// TestAC4_LocalityLogFormat verifies the structured log format is parseable.
// This runs without a cluster by checking our parsing logic.
func TestAC4_LocalityLogFormat(t *testing.T) {
	// Create a temp log file with the expected format
	logContent := `{"level":"INFO","msg":"task_launched","task_id":"map-000","task_type":"MAP","requested_node":"worker-1:9001","allocated_node":"worker-1:9001","is_data_local":true}
{"level":"INFO","msg":"task_launched","task_id":"map-001","task_type":"MAP","requested_node":"worker-2:9001","allocated_node":"worker-3:9001","is_data_local":false}
{"level":"INFO","msg":"task_launched","task_id":"map-002","task_type":"MAP","requested_node":"worker-1:9001","allocated_node":"worker-1:9001","is_data_local":true}
{"level":"INFO","msg":"locality_stats","job_id":"test-job","total_map_tasks":3,"data_local":2,"data_local_percentage":"66.7"}
`
	tmpFile := t.TempDir() + "/am.log"
	os.WriteFile(tmpFile, []byte(logContent), 0644)

	stats, err := parseLocalityStats(tmpFile)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if stats.TotalMapTasks != 3 {
		t.Errorf("expected 3 total map tasks, got %d", stats.TotalMapTasks)
	}
	if stats.DataLocal != 2 {
		t.Errorf("expected 2 data local, got %d", stats.DataLocal)
	}
	if stats.Percentage < 66.0 || stats.Percentage > 67.0 {
		t.Errorf("expected ~66.7%% locality, got %.1f%%", stats.Percentage)
	}

	// Also verify task launch logs can be parsed
	launches, err := parseTaskLaunches(tmpFile)
	if err != nil {
		t.Fatalf("parse launches: %v", err)
	}
	if len(launches) != 3 {
		t.Errorf("expected 3 task launches, got %d", len(launches))
	}

	localCount := 0
	for _, l := range launches {
		if l.IsDataLocal {
			localCount++
		}
	}
	if localCount != 2 {
		t.Errorf("expected 2 data-local launches, got %d", localCount)
	}

	t.Log("AC-4 log format parsing test PASSED")
}

type localityStats struct {
	TotalMapTasks int
	DataLocal     int
	Percentage    float64
}

type taskLaunch struct {
	TaskID        string `json:"task_id"`
	TaskType      string `json:"task_type"`
	RequestedNode string `json:"requested_node"`
	AllocatedNode string `json:"allocated_node"`
	IsDataLocal   bool   `json:"is_data_local"`
}

func parseLocalityStats(logFile string) (*localityStats, error) {
	f, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	stats := &localityStats{}

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, "locality_stats") {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		if msg, ok := entry["msg"].(string); ok && msg == "locality_stats" {
			if v, ok := entry["total_map_tasks"].(float64); ok {
				stats.TotalMapTasks = int(v)
			}
			if v, ok := entry["data_local"].(float64); ok {
				stats.DataLocal = int(v)
			}
			if v, ok := entry["data_local_percentage"].(string); ok {
				var pct float64
				if _, err := parseFloat(v, &pct); err == nil {
					stats.Percentage = pct
				}
			}
		}
	}

	// Calculate percentage if not in log
	if stats.Percentage == 0 && stats.TotalMapTasks > 0 {
		stats.Percentage = float64(stats.DataLocal) / float64(stats.TotalMapTasks) * 100
	}

	return stats, scanner.Err()
}

func parseTaskLaunches(logFile string) ([]taskLaunch, error) {
	f, err := os.Open(logFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var launches []taskLaunch
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, "task_launched") {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		if msg, ok := entry["msg"].(string); ok && msg == "task_launched" {
			l := taskLaunch{}
			if v, ok := entry["task_id"].(string); ok {
				l.TaskID = v
			}
			if v, ok := entry["task_type"].(string); ok {
				l.TaskType = v
			}
			if v, ok := entry["requested_node"].(string); ok {
				l.RequestedNode = v
			}
			if v, ok := entry["allocated_node"].(string); ok {
				l.AllocatedNode = v
			}
			if v, ok := entry["is_data_local"].(bool); ok {
				l.IsDataLocal = v
			}
			launches = append(launches, l)
		}
	}

	return launches, scanner.Err()
}

func parseFloat(s string, result *float64) (int, error) {
	var n int
	for i, c := range s {
		if (c >= '0' && c <= '9') || c == '.' || c == '-' {
			n = i + 1
		}
	}
	if n == 0 {
		return 0, nil
	}
	val := 0.0
	dec := false
	decPlace := 1.0
	for _, c := range s[:n] {
		if c == '.' {
			dec = true
			continue
		}
		if dec {
			decPlace *= 10
			val += float64(c-'0') / decPlace
		} else {
			val = val*10 + float64(c-'0')
		}
	}
	*result = val
	return n, nil
}
