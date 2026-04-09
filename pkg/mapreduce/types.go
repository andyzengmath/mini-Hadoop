package mapreduce

// Mapper processes input key-value pairs and emits intermediate key-value pairs.
type Mapper interface {
	Map(key, value string, emit func(key, value string))
}

// Reducer aggregates values for each key and emits final output.
type Reducer interface {
	Reduce(key string, values []string, emit func(key, value string))
}

// Partitioner determines which reducer receives a given key.
type Partitioner interface {
	Partition(key string, numReducers int) int
}

// HashPartitioner is the default partitioner using hash(key) % numReducers.
type HashPartitioner struct{}

// Partition returns the reducer index for the given key.
func (p *HashPartitioner) Partition(key string, numReducers int) int {
	if numReducers <= 0 {
		return 0
	}
	h := fnvHash(key)
	idx := int(h % uint32(numReducers))
	if idx < 0 {
		idx = -idx
	}
	return idx
}

// fnvHash computes FNV-1a hash of a string.
func fnvHash(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// KeyValue is an intermediate key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// JobConfig holds configuration for a MapReduce job.
type JobConfig struct {
	JobID          string
	InputPath      string
	OutputPath     string
	NumReducers    int
	MapperName     string // Built-in mapper name (e.g., "wordcount")
	ReducerName    string // Built-in reducer name
	SortBufferMB   int
	MaxTaskRetries int // Max retry attempts per task (default 3)
}
