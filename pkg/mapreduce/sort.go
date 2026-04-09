package mapreduce

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SortBuffer accumulates key-value pairs in memory, sorts them, and spills
// to disk when the buffer exceeds a size threshold.
type SortBuffer struct {
	buffer     []KeyValue
	bufferSize int // current estimated size in bytes
	maxSize    int // threshold for spilling
	spillDir   string
	spillFiles []string
	partitioner Partitioner
	numReducers int
}

// NewSortBuffer creates a new sort buffer.
func NewSortBuffer(maxSizeMB int, spillDir string, partitioner Partitioner, numReducers int) *SortBuffer {
	os.MkdirAll(spillDir, 0755)
	return &SortBuffer{
		buffer:      make([]KeyValue, 0, 1024),
		maxSize:     maxSizeMB * 1024 * 1024,
		spillDir:    spillDir,
		partitioner: partitioner,
		numReducers: numReducers,
	}
}

// Add inserts a key-value pair into the buffer, spilling if necessary.
func (sb *SortBuffer) Add(kv KeyValue) error {
	sb.buffer = append(sb.buffer, kv)
	sb.bufferSize += len(kv.Key) + len(kv.Value) + 16 // estimate overhead

	if sb.bufferSize >= int(float64(sb.maxSize)*0.8) {
		return sb.spill()
	}
	return nil
}

// spill sorts the buffer by (partition, key) and writes to a temp file.
func (sb *SortBuffer) spill() error {
	if len(sb.buffer) == 0 {
		return nil
	}

	// Sort by (partition, key)
	sort.Slice(sb.buffer, func(i, j int) bool {
		pi := sb.partitioner.Partition(sb.buffer[i].Key, sb.numReducers)
		pj := sb.partitioner.Partition(sb.buffer[j].Key, sb.numReducers)
		if pi != pj {
			return pi < pj
		}
		return sb.buffer[i].Key < sb.buffer[j].Key
	})

	// Write to spill file
	spillPath := filepath.Join(sb.spillDir, fmt.Sprintf("spill-%d.dat", len(sb.spillFiles)))
	f, err := os.Create(spillPath)
	if err != nil {
		return fmt.Errorf("create spill file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, kv := range sb.buffer {
		p := sb.partitioner.Partition(kv.Key, sb.numReducers)
		fmt.Fprintf(w, "%d\t%s\t%s\n", p, kv.Key, kv.Value)
	}
	w.Flush()

	sb.spillFiles = append(sb.spillFiles, spillPath)
	sb.buffer = sb.buffer[:0]
	sb.bufferSize = 0
	return nil
}

// Flush spills any remaining buffered data and merges all spill files
// into partition-sorted output files (one per reducer).
func (sb *SortBuffer) Flush(outputDir string) ([]string, error) {
	// Spill remaining buffer
	if len(sb.buffer) > 0 {
		if err := sb.spill(); err != nil {
			return nil, err
		}
	}

	os.MkdirAll(outputDir, 0755)

	// Merge all spill files into per-partition output files
	partitionFiles := make([]string, sb.numReducers)
	writers := make([]*os.File, sb.numReducers)
	bufWriters := make([]*bufio.Writer, sb.numReducers)

	for i := 0; i < sb.numReducers; i++ {
		path := filepath.Join(outputDir, fmt.Sprintf("partition-%d.dat", i))
		f, err := os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("create partition file: %w", err)
		}
		writers[i] = f
		bufWriters[i] = bufio.NewWriter(f)
		partitionFiles[i] = path
	}

	// Read all spill files and distribute to partition files
	for _, spillPath := range sb.spillFiles {
		if err := sb.mergeSpillToPartitions(spillPath, bufWriters); err != nil {
			return nil, err
		}
	}

	// Flush and close all partition files
	for i := 0; i < sb.numReducers; i++ {
		bufWriters[i].Flush()
		writers[i].Close()
	}

	// Clean up spill files
	for _, path := range sb.spillFiles {
		os.Remove(path)
	}

	return partitionFiles, nil
}

func (sb *SortBuffer) mergeSpillToPartitions(spillPath string, writers []*bufio.Writer) error {
	f, err := os.Open(spillPath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) != 3 {
			continue
		}

		var partition int
		fmt.Sscanf(parts[0], "%d", &partition)
		if partition >= 0 && partition < len(writers) {
			fmt.Fprintf(writers[partition], "%s\t%s\n", parts[1], parts[2])
		}
	}
	return scanner.Err()
}

// ReadPartitionFile reads a sorted partition file and returns key-value pairs.
func ReadPartitionFile(path string) ([]KeyValue, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ReadKeyValues(f)
}

// ReadKeyValues reads tab-separated key-value pairs from a reader.
func ReadKeyValues(r io.Reader) ([]KeyValue, error) {
	var kvs []KeyValue
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) == 2 {
			kvs = append(kvs, KeyValue{Key: parts[0], Value: parts[1]})
		}
	}
	return kvs, scanner.Err()
}

// GroupByKey groups sorted key-value pairs by key.
func GroupByKey(kvs []KeyValue) map[string][]string {
	groups := make(map[string][]string)
	for _, kv := range kvs {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}
	return groups
}

// SortedKeys returns the keys from a group map in sorted order.
func SortedKeys(groups map[string][]string) []string {
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
