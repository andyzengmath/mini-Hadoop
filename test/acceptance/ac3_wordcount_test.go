package acceptance

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/mini-hadoop/mini-hadoop/pkg/mapreduce"
)

// TestAC3_WordCount tests AC-3: WordCount on test data, verify correctness
// against a single-machine reference implementation.
func TestAC3_WordCount(t *testing.T) {
	// Generate test data (1MB for unit test, set MINIHADOOP_AC3_SIZE_MB for full)
	sizeMB := 1
	if s := os.Getenv("MINIHADOOP_AC3_SIZE_MB"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			sizeMB = n
		}
	}

	t.Logf("Generating %d MB test data for WordCount...", sizeMB)
	inputPath := generateTestData(t, sizeMB)

	// Run reference word count (single-machine)
	t.Log("Computing reference word counts...")
	referenceCounts, err := referenceWordCount(inputPath)
	if err != nil {
		t.Fatalf("reference word count: %v", err)
	}
	t.Logf("Reference: %d unique words", len(referenceCounts))

	// Run MapReduce WordCount locally
	outputDir := filepath.Join(t.TempDir(), "output")
	cfg := mapreduce.JobConfig{
		JobID:       "ac3-wordcount",
		InputPath:   inputPath,
		OutputPath:  outputDir,
		NumReducers: 1,
		MapperName:  "wordcount",
		ReducerName: "wordcount",
	}

	t.Log("Running MapReduce WordCount...")
	if err := mapreduce.RunLocalJob(cfg); err != nil {
		t.Fatalf("MapReduce job failed: %v", err)
	}

	// Read MapReduce output
	mrResults, err := mapreduce.ReadOutputFile(filepath.Join(outputDir, "part-00000"))
	if err != nil {
		t.Fatalf("read MR output: %v", err)
	}
	t.Logf("MapReduce: %d unique words", len(mrResults))

	// Compare
	mismatches := 0
	for word, refCount := range referenceCounts {
		mrCount, exists := mrResults[word]
		if !exists {
			t.Errorf("word %q: in reference (%s) but missing from MR output", word, refCount)
			mismatches++
		} else if mrCount != refCount {
			t.Errorf("word %q: reference=%s, MR=%s", word, refCount, mrCount)
			mismatches++
		}
		if mismatches > 10 {
			t.Fatal("too many mismatches, aborting")
		}
	}

	for word := range mrResults {
		if _, exists := referenceCounts[word]; !exists {
			t.Errorf("word %q: in MR output but missing from reference", word)
			mismatches++
			if mismatches > 10 {
				t.Fatal("too many mismatches, aborting")
			}
		}
	}

	if mismatches == 0 {
		t.Logf("AC-3 PASSED: WordCount output matches reference (%d unique words)", len(referenceCounts))
	}
}

// referenceWordCount computes word counts using simple line-by-line processing.
func referenceWordCount(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	counts := make(map[string]int)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		words := strings.Fields(scanner.Text())
		for _, word := range words {
			word = strings.ToLower(strings.Trim(word, ".,!?;:\"'()[]{}"))
			if word != "" {
				counts[word]++
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	result := make(map[string]string, len(counts))
	for word, count := range counts {
		result[word] = strconv.Itoa(count)
	}
	return result, nil
}
