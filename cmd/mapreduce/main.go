package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/mapreduce"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	job := flag.String("job", "", "Job type: wordcount, sumbykey")
	input := flag.String("input", "", "HDFS input path")
	output := flag.String("output", "", "HDFS output path")
	numReducers := flag.Int("num-reducers", 1, "Number of reducers")
	local := flag.Bool("local", false, "Run locally (single-process, no cluster needed)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if *job == "" || *input == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "Usage: mapreduce --job <wordcount|sumbykey> --input <path> --output <path> [--num-reducers N] [--local]")
		os.Exit(1)
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Determine mapper and reducer names from job type
	mapperName := *job
	reducerName := *job

	jobCfg := mapreduce.JobConfig{
		JobID:       fmt.Sprintf("job-%d", time.Now().UnixMilli()),
		InputPath:   *input,
		OutputPath:  *output,
		NumReducers: *numReducers,
		MapperName:  mapperName,
		ReducerName: reducerName,
	}

	if *local {
		// Run in single-process mode (no cluster required)
		fmt.Printf("Running %s locally: %s -> %s\n", *job, *input, *output)
		if err := mapreduce.RunLocalJob(jobCfg); err != nil {
			fmt.Fprintf(os.Stderr, "Job failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Job completed successfully.")
		return
	}

	// Distributed mode: submit to ResourceManager
	fmt.Printf("Submitting %s job to cluster: %s -> %s\n", *job, *input, *output)

	rmConn, err := rpc.DialWithRetry(cfg.ResourceManagerAddress(), 3)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to ResourceManager: %v\n", err)
		os.Exit(1)
	}
	defer rmConn.Close()

	rmClient := pb.NewResourceManagerServiceClient(rmConn)

	// Submit application
	submitResp, err := rmClient.SubmitApplication(context.Background(), &pb.SubmitApplicationRequest{
		ApplicationType: "mapreduce",
		AmBinaryPath:    "mrappmaster",
		AmArgs: []string{
			"--job-id", jobCfg.JobID,
			"--mapper", jobCfg.MapperName,
			"--reducer", jobCfg.ReducerName,
			"--input", jobCfg.InputPath,
			"--output", jobCfg.OutputPath,
			"--num-reducers", fmt.Sprintf("%d", jobCfg.NumReducers),
			"--namenode", cfg.NameNodeAddress(),
			"--rm", cfg.ResourceManagerAddress(),
		},
		AmMemoryMb:  1024,
		AmCpuVcores: 1,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Submit failed: %v\n", err)
		os.Exit(1)
	}
	if submitResp.Error != "" {
		fmt.Fprintf(os.Stderr, "Submit rejected: %s\n", submitResp.Error)
		os.Exit(1)
	}

	appID := submitResp.AppId
	fmt.Printf("Application submitted: %s\n", appID)

	// Poll for completion
	for {
		time.Sleep(2 * time.Second)

		reportResp, err := rmClient.GetApplicationReport(context.Background(), &pb.GetApplicationReportRequest{
			AppId: appID,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting status: %v\n", err)
			continue
		}
		if reportResp.Error != "" {
			fmt.Fprintf(os.Stderr, "Status error: %s\n", reportResp.Error)
			continue
		}

		status := reportResp.Status
		fmt.Printf("  Status: %s  Progress: %.0f%%\n", status.State, status.Progress*100)

		switch status.State {
		case pb.AppState_FINISHED:
			fmt.Println("Job completed successfully!")
			return
		case pb.AppState_FAILED:
			fmt.Fprintf(os.Stderr, "Job failed: %s\n", status.Diagnostics)
			os.Exit(1)
		case pb.AppState_KILLED:
			fmt.Fprintln(os.Stderr, "Job was killed.")
			os.Exit(1)
		}
	}
}
