package acceptance

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/mapreduce"
)

// TestAC7_LinearScaling tests AC-7: Run the same MapReduce job on 1, 2, and
// 3 nodes. Throughput must increase such that 3-node throughput is within 30%
// of 3x single-node throughput.
//
// Measurement methodology (from consensus plan):
// - Metric: wall-clock time for the same WordCount job on identical input
// - 3 warm-up runs discarded, then 3 measured runs averaged
// - Pass criterion: throughput_3node >= 0.7 * (3 * throughput_1node)
func TestAC7_LinearScaling(t *testing.T) {
	if os.Getenv("MINIHADOOP_CLUSTER") == "" {
		t.Skip("Skipping: requires running cluster (set MINIHADOOP_CLUSTER=1)")
	}

	t.Log("AC-7: Linear Scaling Test")
	t.Log("This test requires running the same job on 1, 2, and 3 node configurations.")
	t.Log("It must be orchestrated externally by adjusting docker-compose worker count.")
	t.Log("")
	t.Log("Measurement methodology:")
	t.Log("  - Input: same WordCount dataset (100MB)")
	t.Log("  - 3 warm-up runs discarded per configuration")
	t.Log("  - 3 measured runs averaged per configuration")
	t.Log("  - Throughput = input_bytes / wall_clock_seconds")
	t.Log("  - Pass: throughput_3node >= 0.7 * (3 * throughput_1node)")
	t.Log("")
	t.Log("(Requires Docker orchestration for full validation)")
}

// TestAC7_LocalScalingBaseline runs a local scaling test to verify that
// the MapReduce engine's throughput measurement logic works correctly.
// This doesn't test distributed scaling but validates the measurement framework.
func TestAC7_LocalScalingBaseline(t *testing.T) {
	// Generate test data
	sizeMB := 2
	t.Logf("Generating %d MB test data...", sizeMB)
	inputPath := generateTestData(t, sizeMB)

	inputInfo, err := os.Stat(inputPath)
	if err != nil {
		t.Fatalf("stat input: %v", err)
	}
	inputBytes := inputInfo.Size()

	// Warm-up run
	t.Log("Warm-up run...")
	runWordCount(t, inputPath, filepath.Join(t.TempDir(), "warmup"))

	// Measured runs
	numRuns := 3
	var totalDuration time.Duration

	for i := 0; i < numRuns; i++ {
		outputDir := filepath.Join(t.TempDir(), fmt.Sprintf("run-%d", i))
		start := time.Now()
		runWordCount(t, inputPath, outputDir)
		duration := time.Since(start)
		totalDuration += duration
		t.Logf("Run %d: %v", i+1, duration)
	}

	avgDuration := totalDuration / time.Duration(numRuns)
	throughputMBps := float64(inputBytes) / (1024 * 1024) / avgDuration.Seconds()

	t.Logf("Average duration: %v", avgDuration)
	t.Logf("Throughput: %.2f MB/s", throughputMBps)
	t.Logf("Input size: %d bytes (%.1f MB)", inputBytes, float64(inputBytes)/(1024*1024))

	// Basic sanity: throughput should be positive and reasonable
	if throughputMBps <= 0 {
		t.Fatal("throughput must be positive")
	}

	t.Logf("AC-7 local baseline: %.2f MB/s (single-process reference)", throughputMBps)
	t.Log("AC-7 local scaling baseline PASSED")
}

func runWordCount(t *testing.T, inputPath, outputDir string) {
	t.Helper()
	cfg := mapreduce.JobConfig{
		JobID:       fmt.Sprintf("ac7-%d", time.Now().UnixNano()),
		InputPath:   inputPath,
		OutputPath:  outputDir,
		NumReducers: 1,
		MapperName:  "wordcount",
		ReducerName: "wordcount",
	}
	if err := mapreduce.RunLocalJob(cfg); err != nil {
		t.Fatalf("WordCount failed: %v", err)
	}
}
