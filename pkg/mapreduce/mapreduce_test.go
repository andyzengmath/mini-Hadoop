package mapreduce

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestWordCountMapper(t *testing.T) {
	m := &WordCountMapper{}
	var results []KeyValue

	m.Map("0", "Hello World hello", func(k, v string) {
		results = append(results, KeyValue{Key: k, Value: v})
	})

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	expected := map[string]int{"hello": 2, "world": 1}
	counts := make(map[string]int)
	for _, kv := range results {
		if kv.Value != "1" {
			t.Errorf("expected value '1', got %q", kv.Value)
		}
		counts[kv.Key]++
	}

	for word, count := range expected {
		if counts[word] != count {
			t.Errorf("word %q: expected %d emissions, got %d", word, count, counts[word])
		}
	}
}

func TestSumReducer(t *testing.T) {
	r := &SumReducer{}
	var results []KeyValue

	r.Reduce("hello", []string{"1", "1", "1"}, func(k, v string) {
		results = append(results, KeyValue{Key: k, Value: v})
	})

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Key != "hello" || results[0].Value != "3" {
		t.Errorf("expected (hello, 3), got (%s, %s)", results[0].Key, results[0].Value)
	}
}

func TestHashPartitioner(t *testing.T) {
	p := &HashPartitioner{}

	// Same key always maps to same partition
	p1 := p.Partition("hello", 3)
	p2 := p.Partition("hello", 3)
	if p1 != p2 {
		t.Error("same key produced different partitions")
	}

	// Partition is within range
	for i := 0; i < 100; i++ {
		part := p.Partition("key-"+strconv.Itoa(i), 5)
		if part < 0 || part >= 5 {
			t.Errorf("partition %d out of range [0,5)", part)
		}
	}
}

func TestSortBufferAndFlush(t *testing.T) {
	dir := t.TempDir()
	spillDir := filepath.Join(dir, "spills")
	outputDir := filepath.Join(dir, "output")

	sb := NewSortBuffer(1, spillDir, &HashPartitioner{}, 2)

	// Add some key-value pairs
	testData := []KeyValue{
		{Key: "banana", Value: "1"},
		{Key: "apple", Value: "1"},
		{Key: "cherry", Value: "1"},
		{Key: "apple", Value: "1"},
		{Key: "banana", Value: "1"},
	}

	for _, kv := range testData {
		if err := sb.Add(kv); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	partFiles, err := sb.Flush(outputDir)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	if len(partFiles) != 2 {
		t.Fatalf("expected 2 partition files, got %d", len(partFiles))
	}

	// Verify all key-values are present across partition files
	totalKVs := 0
	for _, path := range partFiles {
		kvs, err := ReadPartitionFile(path)
		if err != nil {
			t.Fatalf("read partition: %v", err)
		}
		totalKVs += len(kvs)
	}

	if totalKVs != 5 {
		t.Errorf("expected 5 total key-values across partitions, got %d", totalKVs)
	}
}

func TestRunLocalJob(t *testing.T) {
	dir := t.TempDir()

	// Create test input
	inputPath := filepath.Join(dir, "input.txt")
	err := os.WriteFile(inputPath, []byte("hello world\nhello go\nworld go go\n"), 0644)
	if err != nil {
		t.Fatalf("create input: %v", err)
	}

	outputDir := filepath.Join(dir, "output")

	cfg := JobConfig{
		JobID:       "test-job-1",
		InputPath:   inputPath,
		OutputPath:  outputDir,
		NumReducers: 1,
		MapperName:  "wordcount",
		ReducerName: "wordcount",
	}

	if err := RunLocalJob(cfg); err != nil {
		t.Fatalf("RunLocalJob failed: %v", err)
	}

	// Read output
	results, err := ReadOutputFile(filepath.Join(outputDir, "part-00000"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	expected := map[string]string{
		"hello": "2",
		"world": "2",
		"go":    "3",
	}

	for word, count := range expected {
		if results[word] != count {
			t.Errorf("word %q: expected %s, got %s", word, count, results[word])
		}
	}
}

func TestGroupByKey(t *testing.T) {
	kvs := []KeyValue{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
		{Key: "a", Value: "3"},
		{Key: "b", Value: "4"},
	}

	groups := GroupByKey(kvs)

	if len(groups["a"]) != 2 {
		t.Errorf("expected 2 values for key 'a', got %d", len(groups["a"]))
	}
	if len(groups["b"]) != 2 {
		t.Errorf("expected 2 values for key 'b', got %d", len(groups["b"]))
	}
}

func TestGetMapperAndReducer(t *testing.T) {
	mappers := []string{"wordcount", "sumbykey", "identity"}
	for _, name := range mappers {
		m, err := GetMapper(name)
		if err != nil {
			t.Errorf("GetMapper(%q) failed: %v", name, err)
		}
		if m == nil {
			t.Errorf("GetMapper(%q) returned nil", name)
		}
	}

	_, err := GetMapper("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent mapper")
	}

	reducers := []string{"sum", "wordcount", "identity"}
	for _, name := range reducers {
		r, err := GetReducer(name)
		if err != nil {
			t.Errorf("GetReducer(%q) failed: %v", name, err)
		}
		if r == nil {
			t.Errorf("GetReducer(%q) returned nil", name)
		}
	}
}
