package dagengine

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestRDD_MapAndCollect(t *testing.T) {
	data := []KeyValue{
		{Key: "1", Value: "hello"},
		{Key: "2", Value: "world"},
	}
	rdd := NewRDDFromData(data, 2)

	upper := rdd.Map(func(kv KeyValue) KeyValue {
		return KeyValue{Key: kv.Key, Value: strings.ToUpper(kv.Value)}
	})

	result := upper.Collect()
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}

	found := make(map[string]bool)
	for _, kv := range result {
		found[kv.Value] = true
	}
	if !found["HELLO"] || !found["WORLD"] {
		t.Errorf("expected HELLO and WORLD, got %v", result)
	}
}

func TestRDD_FlatMap(t *testing.T) {
	lines := NewRDDFromLines([]string{"hello world", "foo bar baz"}, 1)

	words := lines.FlatMap(func(kv KeyValue) []KeyValue {
		var result []KeyValue
		for _, w := range strings.Fields(kv.Value) {
			result = append(result, KeyValue{Key: w, Value: "1"})
		}
		return result
	})

	if words.Count() != 5 {
		t.Errorf("expected 5 words, got %d", words.Count())
	}
}

func TestRDD_Filter(t *testing.T) {
	data := []KeyValue{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	}
	rdd := NewRDDFromData(data, 1)

	filtered := rdd.Filter(func(kv KeyValue) bool {
		n, _ := strconv.Atoi(kv.Value)
		return n > 1
	})

	if filtered.Count() != 2 {
		t.Errorf("expected 2 after filter, got %d", filtered.Count())
	}
}

func TestRDD_ReduceByKey(t *testing.T) {
	data := []KeyValue{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
		{Key: "a", Value: "3"},
		{Key: "b", Value: "4"},
		{Key: "a", Value: "5"},
	}
	rdd := NewRDDFromData(data, 2)

	summed := rdd.ReduceByKey(func(a, b string) string {
		na, _ := strconv.Atoi(a)
		nb, _ := strconv.Atoi(b)
		return strconv.Itoa(na + nb)
	})

	result := summed.Collect()
	sums := make(map[string]string)
	for _, kv := range result {
		sums[kv.Key] = kv.Value
	}

	if sums["a"] != "9" {
		t.Errorf("expected a=9, got %s", sums["a"])
	}
	if sums["b"] != "6" {
		t.Errorf("expected b=6, got %s", sums["b"])
	}
}

func TestRDD_WordCount(t *testing.T) {
	// Full Spark-like WordCount pipeline
	lines := NewRDDFromLines([]string{
		"hello world hello",
		"world hello go",
		"go go go",
	}, 2)

	words := lines.FlatMap(func(kv KeyValue) []KeyValue {
		var result []KeyValue
		for _, w := range strings.Fields(kv.Value) {
			result = append(result, KeyValue{Key: w, Value: "1"})
		}
		return result
	})

	counts := words.ReduceByKey(func(a, b string) string {
		na, _ := strconv.Atoi(a)
		nb, _ := strconv.Atoi(b)
		return strconv.Itoa(na + nb)
	})

	result := counts.Collect()
	resultMap := make(map[string]string)
	for _, kv := range result {
		resultMap[kv.Key] = kv.Value
	}

	expected := map[string]string{
		"hello": "3",
		"world": "2",
		"go":    "4",
	}

	for word, count := range expected {
		if resultMap[word] != count {
			t.Errorf("word %q: expected %s, got %s", word, count, resultMap[word])
		}
	}
}

func TestRDD_CountByKey(t *testing.T) {
	data := []KeyValue{
		{Key: "x", Value: "a"},
		{Key: "y", Value: "b"},
		{Key: "x", Value: "c"},
	}
	rdd := NewRDDFromData(data, 1)
	counts := rdd.CountByKey()

	if counts["x"] != 2 {
		t.Errorf("expected x=2, got %d", counts["x"])
	}
	if counts["y"] != 1 {
		t.Errorf("expected y=1, got %d", counts["y"])
	}
}

func TestRDD_GroupByKey(t *testing.T) {
	data := []KeyValue{
		{Key: "color", Value: "red"},
		{Key: "color", Value: "blue"},
		{Key: "size", Value: "large"},
	}
	rdd := NewRDDFromData(data, 1)
	grouped := rdd.GroupByKey()

	result := grouped.Collect()
	resultMap := make(map[string]string)
	for _, kv := range result {
		resultMap[kv.Key] = kv.Value
	}

	// GroupByKey concatenates with comma
	colorVal := resultMap["color"]
	if !strings.Contains(colorVal, "red") || !strings.Contains(colorVal, "blue") {
		t.Errorf("expected color to contain red and blue, got %q", colorVal)
	}
}

func TestRDD_EmptyPartitions(t *testing.T) {
	rdd := NewRDDFromData(nil, 3)
	if rdd.Count() != 0 {
		t.Errorf("expected 0 count, got %d", rdd.Count())
	}
	result := rdd.Collect()
	if len(result) != 0 {
		t.Errorf("expected empty collect, got %d", len(result))
	}
}

func TestDAGScheduler_WordCount(t *testing.T) {
	lines := NewRDDFromLines([]string{"a b c", "a b", "a"}, 2)

	words := lines.FlatMap(func(kv KeyValue) []KeyValue {
		var result []KeyValue
		for _, w := range strings.Fields(kv.Value) {
			result = append(result, KeyValue{Key: w, Value: "1"})
		}
		return result
	})

	counts := words.ReduceByKey(func(a, b string) string {
		na, _ := strconv.Atoi(a)
		nb, _ := strconv.Atoi(b)
		return strconv.Itoa(na + nb)
	})

	scheduler := NewDAGScheduler()
	result, err := scheduler.Execute(counts)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	resultMap := make(map[string]string)
	for _, kv := range result {
		resultMap[kv.Key] = kv.Value
	}

	if resultMap["a"] != "3" {
		t.Errorf("a: expected 3, got %s", resultMap["a"])
	}
	if resultMap["b"] != "2" {
		t.Errorf("b: expected 2, got %s", resultMap["b"])
	}
	if resultMap["c"] != "1" {
		t.Errorf("c: expected 1, got %s", resultMap["c"])
	}
}

func TestDAGScheduler_StageCount(t *testing.T) {
	data := NewRDDFromData([]KeyValue{{Key: "k", Value: "v"}}, 1)

	// Map (narrow) → ReduceByKey (wide) → Map (narrow)
	mapped := data.Map(func(kv KeyValue) KeyValue { return kv })
	reduced := mapped.ReduceByKey(func(a, b string) string { return a })
	finalRDD := reduced.Map(func(kv KeyValue) KeyValue { return kv })

	scheduler := NewDAGScheduler()
	stages := scheduler.SubmitJob(finalRDD)

	// Should have multiple stages due to wide dependency at ReduceByKey
	if len(stages) < 2 {
		t.Errorf("expected at least 2 stages (shuffle boundary), got %d", len(stages))
	}

	// Verify at least one stage has IsShffle=true
	hasShufle := false
	for _, s := range stages {
		if s.IsShffle {
			hasShufle = true
		}
	}
	if !hasShufle {
		t.Error("expected at least one shuffle stage")
	}
}

func TestRDD_LargeDataset(t *testing.T) {
	// 10,000 records, verify ReduceByKey handles correctly
	var data []KeyValue
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%03d", i%100)
		data = append(data, KeyValue{Key: key, Value: "1"})
	}

	rdd := NewRDDFromData(data, 4)
	counts := rdd.ReduceByKey(func(a, b string) string {
		na, _ := strconv.Atoi(a)
		nb, _ := strconv.Atoi(b)
		return strconv.Itoa(na + nb)
	})

	result := counts.Collect()
	if len(result) != 100 {
		t.Errorf("expected 100 unique keys, got %d", len(result))
	}

	// Each key should have count 100
	for _, kv := range result {
		if kv.Value != "100" {
			t.Errorf("key %s: expected 100, got %s", kv.Key, kv.Value)
			break
		}
	}
}
