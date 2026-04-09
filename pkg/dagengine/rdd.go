package dagengine

import (
	"fmt"
	"sort"
	"sync/atomic"
)

var rddCounter int64

// RDD (Resilient Distributed Dataset) is an immutable, partitioned collection
// of records that can be operated on in parallel. Inspired by Apache Spark.
type RDD struct {
	ID         string
	Partitions []Partition
	Deps       []Dependency
	compute    func(partition *Partition) []KeyValue
	parent     *RDD
	cached     bool
	cacheData  map[int][]KeyValue // partitionIndex -> cached data
}

// Partition represents a slice of data in an RDD.
type Partition struct {
	Index int
	Data  []KeyValue
}

// KeyValue is a generic key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// Dependency describes how an RDD depends on its parent.
type Dependency struct {
	Type   DependencyType
	Parent *RDD
}

// DependencyType classifies the dependency between RDDs.
type DependencyType string

const (
	NarrowDep DependencyType = "narrow" // 1:1 partition mapping (map, filter)
	WideDep   DependencyType = "wide"   // Shuffle required (reduceByKey, groupByKey)
)

func nextRDDID() string {
	n := atomic.AddInt64(&rddCounter, 1)
	return fmt.Sprintf("rdd_%d", n)
}

// NewRDDFromData creates an RDD from in-memory key-value data, split into numPartitions.
func NewRDDFromData(data []KeyValue, numPartitions int) *RDD {
	if numPartitions <= 0 {
		numPartitions = 1
	}

	partitions := make([]Partition, numPartitions)
	for i := range partitions {
		partitions[i] = Partition{Index: i}
	}

	// Distribute data round-robin across partitions
	for i, kv := range data {
		pIdx := i % numPartitions
		partitions[pIdx].Data = append(partitions[pIdx].Data, kv)
	}

	return &RDD{
		ID:         nextRDDID(),
		Partitions: partitions,
	}
}

// NewRDDFromLines creates an RDD from text lines (key=lineNo, value=line).
func NewRDDFromLines(lines []string, numPartitions int) *RDD {
	data := make([]KeyValue, len(lines))
	for i, line := range lines {
		data[i] = KeyValue{Key: fmt.Sprintf("%d", i), Value: line}
	}
	return NewRDDFromData(data, numPartitions)
}

// Map applies a function to each key-value pair (narrow dependency).
func (r *RDD) Map(fn func(kv KeyValue) KeyValue) *RDD {
	child := &RDD{
		ID:     nextRDDID(),
		parent: r,
		Deps:   []Dependency{{Type: NarrowDep, Parent: r}},
	}

	child.Partitions = make([]Partition, len(r.Partitions))
	for i, p := range r.Partitions {
		mapped := make([]KeyValue, len(p.Data))
		for j, kv := range p.Data {
			mapped[j] = fn(kv)
		}
		child.Partitions[i] = Partition{Index: i, Data: mapped}
	}

	return child
}

// FlatMap applies a function that returns zero or more key-value pairs per input.
func (r *RDD) FlatMap(fn func(kv KeyValue) []KeyValue) *RDD {
	child := &RDD{
		ID:     nextRDDID(),
		parent: r,
		Deps:   []Dependency{{Type: NarrowDep, Parent: r}},
	}

	child.Partitions = make([]Partition, len(r.Partitions))
	for i, p := range r.Partitions {
		var result []KeyValue
		for _, kv := range p.Data {
			result = append(result, fn(kv)...)
		}
		child.Partitions[i] = Partition{Index: i, Data: result}
	}

	return child
}

// Filter retains only key-value pairs matching the predicate (narrow dependency).
func (r *RDD) Filter(fn func(kv KeyValue) bool) *RDD {
	child := &RDD{
		ID:     nextRDDID(),
		parent: r,
		Deps:   []Dependency{{Type: NarrowDep, Parent: r}},
	}

	child.Partitions = make([]Partition, len(r.Partitions))
	for i, p := range r.Partitions {
		var filtered []KeyValue
		for _, kv := range p.Data {
			if fn(kv) {
				filtered = append(filtered, kv)
			}
		}
		child.Partitions[i] = Partition{Index: i, Data: filtered}
	}

	return child
}

// ReduceByKey groups by key and reduces values (wide dependency — requires shuffle).
func (r *RDD) ReduceByKey(fn func(a, b string) string) *RDD {
	numPartitions := len(r.Partitions)
	child := &RDD{
		ID:     nextRDDID(),
		parent: r,
		Deps:   []Dependency{{Type: WideDep, Parent: r}},
	}

	// Shuffle phase: redistribute by key hash across partitions
	buckets := make([][]KeyValue, numPartitions)
	for i := range buckets {
		buckets[i] = make([]KeyValue, 0)
	}

	for _, p := range r.Partitions {
		for _, kv := range p.Data {
			h := hashKey(kv.Key) % numPartitions
			buckets[h] = append(buckets[h], kv)
		}
	}

	// Reduce phase: group by key within each partition and apply reduce
	child.Partitions = make([]Partition, numPartitions)
	for i, bucket := range buckets {
		groups := make(map[string]string)
		for _, kv := range bucket {
			if existing, ok := groups[kv.Key]; ok {
				groups[kv.Key] = fn(existing, kv.Value)
			} else {
				groups[kv.Key] = kv.Value
			}
		}

		var reduced []KeyValue
		for k, v := range groups {
			reduced = append(reduced, KeyValue{Key: k, Value: v})
		}
		// Sort by key for deterministic output
		sort.Slice(reduced, func(a, b int) bool { return reduced[a].Key < reduced[b].Key })
		child.Partitions[i] = Partition{Index: i, Data: reduced}
	}

	return child
}

// GroupByKey groups all values for each key (wide dependency — requires shuffle).
// Values are concatenated with comma separator.
func (r *RDD) GroupByKey() *RDD {
	return r.ReduceByKey(func(a, b string) string {
		return a + "," + b
	})
}

// Collect gathers all data from all partitions into a single slice.
// This is an action that triggers computation.
func (r *RDD) Collect() []KeyValue {
	var result []KeyValue
	for _, p := range r.Partitions {
		result = append(result, p.Data...)
	}
	return result
}

// Count returns the total number of records across all partitions.
func (r *RDD) Count() int {
	total := 0
	for _, p := range r.Partitions {
		total += len(p.Data)
	}
	return total
}

// CountByKey returns a map of key -> count.
func (r *RDD) CountByKey() map[string]int {
	counts := make(map[string]int)
	for _, p := range r.Partitions {
		for _, kv := range p.Data {
			counts[kv.Key]++
		}
	}
	return counts
}

func hashKey(key string) int {
	h := uint64(0)
	for _, c := range key {
		h = h*31 + uint64(c)
	}
	return int(h >> 1) // always non-negative on any platform
}
