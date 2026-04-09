package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/mapreduce"
)

// mapworker is the single binary launched by the NodeManager for both
// map and reduce tasks. The --mode flag determines the behavior.
func main() {
	mode := flag.String("mode", "", "Task mode: map or reduce")
	taskID := flag.String("task-id", "", "Task ID")
	jobID := flag.String("job-id", "", "Job ID")
	mapperName := flag.String("mapper", "", "Mapper name (map mode)")
	reducerName := flag.String("reducer", "", "Reducer name (reduce mode)")
	blockID := flag.String("block-id", "", "HDFS block ID to read (map mode)")
	namenodeAddr := flag.String("namenode", "", "NameNode address")
	numReducers := flag.Int("num-reducers", 1, "Number of reducers (map mode)")
	partitionID := flag.Int("partition-id", 0, "Partition ID (reduce mode)")
	outputPath := flag.String("output", "", "HDFS output path (reduce mode)")
	inputFile := flag.String("input", "", "Local input file path (for local map tasks)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if *mode == "" || *taskID == "" || *jobID == "" {
		fmt.Fprintln(os.Stderr, "Usage: mapworker --mode <map|reduce> --task-id <id> --job-id <id> ...")
		os.Exit(1)
	}

	cfg, _ := config.LoadConfig("")

	switch *mode {
	case "map":
		if err := runMapMode(*taskID, *jobID, *mapperName, *blockID, *namenodeAddr, *numReducers, *inputFile, cfg); err != nil {
			slog.Error("map task failed", "task_id", *taskID, "error", err)
			os.Exit(1)
		}
	case "reduce":
		if err := runReduceMode(*taskID, *jobID, *reducerName, *partitionID, *namenodeAddr, *outputPath, cfg); err != nil {
			slog.Error("reduce task failed", "task_id", *taskID, "error", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown mode: %s\n", *mode)
		os.Exit(1)
	}
}

func runMapMode(taskID, jobID, mapperName, blockID, namenodeAddr string, numReducers int, inputFile string, cfg config.Config) error {
	slog.Info("map task starting",
		"task_id", taskID,
		"job_id", jobID,
		"mapper", mapperName,
		"block_id", blockID,
		"num_reducers", numReducers,
	)

	// Determine input path — either a local file or download block from HDFS
	localInput := inputFile
	if localInput == "" && blockID != "" {
		// In distributed mode, download block data from DataNode
		// For now, use local file if available
		localInput = fmt.Sprintf("/data/temp/%s/%s/input.dat", jobID, taskID)
	}

	if localInput == "" {
		return fmt.Errorf("no input specified (need --input or --block-id)")
	}

	// Configure sort buffer
	sortBufferMB := cfg.SortBufferSize / (1024 * 1024)
	if sortBufferMB <= 0 {
		sortBufferMB = 64
	}

	tempDir := filepath.Join(cfg.TempDir, jobID)

	// Run map task
	partFiles, err := mapreduce.RunMapTask(localInput, mapperName, taskID, numReducers, sortBufferMB, tempDir)
	if err != nil {
		return fmt.Errorf("map task: %w", err)
	}

	slog.Info("map task complete",
		"task_id", taskID,
		"partition_files", len(partFiles),
	)

	// Write partition file paths to a manifest for the shuffle service
	manifestPath := filepath.Join(tempDir, taskID, "manifest.txt")
	os.MkdirAll(filepath.Dir(manifestPath), 0755)
	f, err := os.Create(manifestPath)
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	for _, pf := range partFiles {
		fmt.Fprintln(f, pf)
	}
	f.Close()

	return nil
}

func runReduceMode(taskID, jobID, reducerName string, partitionID int, namenodeAddr, outputPath string, cfg config.Config) error {
	slog.Info("reduce task starting",
		"task_id", taskID,
		"job_id", jobID,
		"reducer", reducerName,
		"partition_id", partitionID,
	)

	tempDir := filepath.Join(cfg.TempDir, jobID)

	// Collect partition files from all map tasks
	// In distributed mode, these would be fetched via ShuffleService
	// For local execution, scan the temp directory for partition files
	var inputPaths []string
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return fmt.Errorf("read temp dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		partFile := filepath.Join(tempDir, entry.Name(), "output", fmt.Sprintf("partition-%d.dat", partitionID))
		if _, err := os.Stat(partFile); err == nil {
			inputPaths = append(inputPaths, partFile)
		}
	}

	if len(inputPaths) == 0 {
		return fmt.Errorf("no partition files found for partition %d", partitionID)
	}

	// Output path — write to local file or HDFS
	localOutput := outputPath
	if localOutput == "" {
		localOutput = filepath.Join(tempDir, fmt.Sprintf("reduce-output-%d.txt", partitionID))
	}
	os.MkdirAll(filepath.Dir(localOutput), 0755)

	err = mapreduce.RunReduceTask(inputPaths, reducerName, localOutput, taskID)
	if err != nil {
		return fmt.Errorf("reduce task: %w", err)
	}

	slog.Info("reduce task complete",
		"task_id", taskID,
		"partition_id", partitionID,
		"output", localOutput,
		"input_files", len(inputPaths),
	)

	return nil
}
