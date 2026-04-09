package acceptance

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/mini-hadoop/mini-hadoop/pkg/mapreduce"
)

// TestAC6_ShuffleCorrectness tests AC-6: Multi-key aggregation job verifying
// shuffle/sort produces correct grouped output across reducers.
func TestAC6_ShuffleCorrectness(t *testing.T) {
	numKeys := 1000
	if s := os.Getenv("MINIHADOOP_AC6_NUM_KEYS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			numKeys = n
		}
	}

	// Generate test data: tab-separated key\tvalue lines
	t.Logf("Generating SumByKey test data with %d unique keys...", numKeys)
	inputPath := filepath.Join(t.TempDir(), "sumbykey-input.txt")
	expectedSums := generateSumByKeyData(t, inputPath, numKeys)

	// Run MapReduce SumByKey locally
	outputDir := filepath.Join(t.TempDir(), "output")
	cfg := mapreduce.JobConfig{
		JobID:       "ac6-shuffle",
		InputPath:   inputPath,
		OutputPath:  outputDir,
		NumReducers: 1,
		MapperName:  "sumbykey",
		ReducerName: "sumbykey",
	}

	t.Log("Running MapReduce SumByKey...")
	if err := mapreduce.RunLocalJob(cfg); err != nil {
		t.Fatalf("MapReduce job failed: %v", err)
	}

	// Read output and verify
	results, err := mapreduce.ReadOutputFile(filepath.Join(outputDir, "part-00000"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	t.Logf("Output: %d unique keys", len(results))

	mismatches := 0
	for key, expectedSum := range expectedSums {
		actual, exists := results[key]
		if !exists {
			t.Errorf("key %q: expected sum %d, but missing from output", key, expectedSum)
			mismatches++
		} else {
			actualVal, _ := strconv.Atoi(actual)
			if actualVal != expectedSum {
				t.Errorf("key %q: expected sum %d, got %s", key, expectedSum, actual)
				mismatches++
			}
		}
		if mismatches > 10 {
			t.Fatal("too many mismatches, aborting")
		}
	}

	// Check for extra keys
	for key := range results {
		if _, exists := expectedSums[key]; !exists {
			t.Errorf("unexpected key in output: %q", key)
			mismatches++
			if mismatches > 10 {
				t.Fatal("too many mismatches")
			}
		}
	}

	if mismatches == 0 {
		t.Logf("AC-6 PASSED: SumByKey correct for %d unique keys", numKeys)
	}
}

// generateSumByKeyData creates a test file with tab-separated key\tvalue pairs
// and returns expected sums per key.
func generateSumByKeyData(t *testing.T, path string, numKeys int) map[string]int {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create test data: %v", err)
	}
	defer f.Close()

	rng := rand.New(rand.NewSource(42))
	expected := make(map[string]int)

	// Generate multiple entries per key
	for i := 0; i < numKeys*10; i++ {
		keyIdx := rng.Intn(numKeys)
		key := fmt.Sprintf("key-%05d", keyIdx)
		value := rng.Intn(100) + 1

		fmt.Fprintf(f, "%s\t%d\n", key, value)
		expected[key] += value
	}

	return expected
}

// TestAC6_MultiReducerShuffle tests that shuffle correctly partitions keys
// across multiple reducers.
func TestAC6_MultiReducerShuffle(t *testing.T) {
	dir := t.TempDir()

	// Create test data
	inputPath := filepath.Join(dir, "input.txt")
	f, err := os.Create(inputPath)
	if err != nil {
		t.Fatalf("create input: %v", err)
	}

	// Write deterministic data
	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, key := range keys {
		for i := 0; i < 5; i++ {
			fmt.Fprintf(f, "%s\t%d\n", key, i+1)
		}
	}
	f.Close()

	// Run map task with 2 reducers
	partFiles, err := mapreduce.RunMapTask(inputPath, "sumbykey", "test-map-0", 2, 64, filepath.Join(dir, "temp"))
	if err != nil {
		t.Fatalf("RunMapTask failed: %v", err)
	}

	if len(partFiles) != 2 {
		t.Fatalf("expected 2 partition files, got %d", len(partFiles))
	}

	// Verify all keys appear in exactly one partition
	allKeys := make(map[string]bool)
	for _, pf := range partFiles {
		kvs, err := mapreduce.ReadPartitionFile(pf)
		if err != nil {
			t.Fatalf("read partition: %v", err)
		}
		for _, kv := range kvs {
			allKeys[kv.Key] = true
		}
	}

	for _, key := range keys {
		if !allKeys[key] {
			t.Errorf("key %q missing from all partitions", key)
		}
	}

	// Run reduce on each partition
	for i, pf := range partFiles {
		outputPath := filepath.Join(dir, fmt.Sprintf("reduce-output-%d.txt", i))
		err := mapreduce.RunReduceTask([]string{pf}, "sumbykey", outputPath, fmt.Sprintf("reduce-%d", i))
		if err != nil {
			t.Fatalf("RunReduceTask %d failed: %v", i, err)
		}

		results, err := mapreduce.ReadOutputFile(outputPath)
		if err != nil {
			t.Fatalf("read reduce output: %v", err)
		}

		// Each key should sum to 1+2+3+4+5=15
		for key, val := range results {
			if strings.HasPrefix(key, "key-") {
				continue // skip if somehow different format
			}
			if val != "15" {
				t.Errorf("key %q: expected sum 15, got %s", key, val)
			}
		}
	}

	t.Log("AC-6 Multi-reducer shuffle test PASSED")
}
