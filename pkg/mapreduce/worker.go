package mapreduce

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
)

// RunMapTask executes a map task: reads input, applies mapper, sorts and spills output.
// Returns paths to partition output files (one per reducer).
func RunMapTask(inputPath string, mapperName string, taskID string, numReducers int, sortBufferMB int, tempDir string) ([]string, error) {
	mapper, err := GetMapper(mapperName)
	if err != nil {
		return nil, err
	}

	spillDir := fmt.Sprintf("%s/%s/spills", tempDir, taskID)
	outputDir := fmt.Sprintf("%s/%s/output", tempDir, taskID)

	sb := NewSortBuffer(sortBufferMB, spillDir, &HashPartitioner{}, numReducers)

	// Read input and run mapper
	inputFormat := &TextInputFormat{}
	err = inputFormat.ReadSplit(inputPath, mapper, func(key, value string) {
		sb.Add(KeyValue{Key: key, Value: value})
	})
	if err != nil {
		return nil, fmt.Errorf("map phase: %w", err)
	}

	// Flush sort buffer to partition files
	partitionFiles, err := sb.Flush(outputDir)
	if err != nil {
		return nil, fmt.Errorf("flush sort buffer: %w", err)
	}

	slog.Info("map task complete",
		"task_id", taskID,
		"input", inputPath,
		"partitions", len(partitionFiles),
	)
	return partitionFiles, nil
}

// RunReduceTask executes a reduce task: reads sorted input, groups by key, applies reducer.
func RunReduceTask(inputPaths []string, reducerName string, outputPath string, taskID string) error {
	reducer, err := GetReducer(reducerName)
	if err != nil {
		return err
	}

	// Read all input partition files and merge
	var allKVs []KeyValue
	for _, path := range inputPaths {
		kvs, err := ReadPartitionFile(path)
		if err != nil {
			return fmt.Errorf("read partition %s: %w", path, err)
		}
		allKVs = append(allKVs, kvs...)
	}

	// Sort by key
	sort.Slice(allKVs, func(i, j int) bool {
		return allKVs[i].Key < allKVs[j].Key
	})

	// Group by key and reduce
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer outFile.Close()

	w := bufio.NewWriter(outFile)
	outputFormat := &TextOutputFormat{}

	// Iterate over sorted key-values, grouping by key
	i := 0
	for i < len(allKVs) {
		key := allKVs[i].Key
		var values []string

		for i < len(allKVs) && allKVs[i].Key == key {
			values = append(values, allKVs[i].Value)
			i++
		}

		reducer.Reduce(key, values, func(k, v string) {
			outputFormat.WriteToWriter(w, k, v)
		})
	}

	w.Flush()

	slog.Info("reduce task complete",
		"task_id", taskID,
		"input_files", len(inputPaths),
		"output", outputPath,
	)
	return nil
}

// RunLocalJob runs a complete MapReduce job locally (single process, for testing).
func RunLocalJob(cfg JobConfig) error {
	slog.Info("starting local MapReduce job",
		"job_id", cfg.JobID,
		"input", cfg.InputPath,
		"output", cfg.OutputPath,
		"num_reducers", cfg.NumReducers,
	)

	mapper, err := GetMapper(cfg.MapperName)
	if err != nil {
		return err
	}
	reducer, err := GetReducer(cfg.ReducerName)
	if err != nil {
		return err
	}

	// Map phase: collect all intermediate key-values
	var intermediates []KeyValue
	inputFormat := &TextInputFormat{}
	err = inputFormat.ReadSplit(cfg.InputPath, mapper, func(key, value string) {
		intermediates = append(intermediates, KeyValue{Key: key, Value: value})
	})
	if err != nil {
		return fmt.Errorf("map phase: %w", err)
	}
	slog.Info("map phase complete", "intermediate_kvs", len(intermediates))

	// Sort by key
	sort.Slice(intermediates, func(i, j int) bool {
		return intermediates[i].Key < intermediates[j].Key
	})

	// Reduce phase: group by key, apply reducer
	os.MkdirAll(cfg.OutputPath, 0755)
	outPath := cfg.OutputPath + "/part-00000"
	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer outFile.Close()

	w := bufio.NewWriter(outFile)

	i := 0
	outputCount := 0
	for i < len(intermediates) {
		key := intermediates[i].Key
		var values []string
		for i < len(intermediates) && intermediates[i].Key == key {
			values = append(values, intermediates[i].Value)
			i++
		}
		reducer.Reduce(key, values, func(k, v string) {
			fmt.Fprintf(w, "%s\t%s\n", k, v)
			outputCount++
		})
	}
	w.Flush()

	slog.Info("job complete",
		"job_id", cfg.JobID,
		"output", outPath,
		"output_records", outputCount,
	)
	return nil
}

// ReadOutputFile reads a MapReduce output file into a map.
func ReadOutputFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	result := make(map[string]string)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	return result, scanner.Err()
}
