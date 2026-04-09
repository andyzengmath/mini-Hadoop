package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/hdfs"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	client, err := hdfs.NewClient(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to NameNode: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	cmd := args[0]
	switch cmd {
	case "put":
		if len(args) != 3 {
			fmt.Fprintln(os.Stderr, "Usage: hdfs put <local-path> <remote-path>")
			os.Exit(1)
		}
		handlePut(client, args[1], args[2])

	case "get":
		if len(args) != 3 {
			fmt.Fprintln(os.Stderr, "Usage: hdfs get <remote-path> <local-path>")
			os.Exit(1)
		}
		handleGet(client, args[1], args[2])

	case "ls":
		path := "/"
		if len(args) > 1 {
			path = args[1]
		}
		handleLs(client, path)

	case "mkdir":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "Usage: hdfs mkdir <path>")
			os.Exit(1)
		}
		handleMkdir(client, args[1])

	case "rm":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "Usage: hdfs rm <path>")
			os.Exit(1)
		}
		handleRm(client, args[1])

	case "info":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "Usage: hdfs info <path>")
			os.Exit(1)
		}
		handleInfo(client, args[1])

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: hdfs [--config <path>] <command> [args...]")
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  put <local> <remote>  Upload a local file")
	fmt.Fprintln(os.Stderr, "  get <remote> <local>  Download a file")
	fmt.Fprintln(os.Stderr, "  ls [path]             List directory")
	fmt.Fprintln(os.Stderr, "  mkdir <path>          Create directory")
	fmt.Fprintln(os.Stderr, "  rm <path>             Remove file/directory")
	fmt.Fprintln(os.Stderr, "  info <path>           Show file info")
}

func handlePut(client *hdfs.Client, localPath, remotePath string) {
	f, err := os.Open(localPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening local file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	if err := client.CreateFile(remotePath, f, 0); err != nil {
		fmt.Fprintf(os.Stderr, "Error uploading file: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Uploaded %s -> %s\n", localPath, remotePath)
}

func handleGet(client *hdfs.Client, remotePath, localPath string) {
	f, err := os.Create(localPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating local file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	if err := client.ReadFile(remotePath, f); err != nil {
		fmt.Fprintf(os.Stderr, "Error downloading file: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Downloaded %s -> %s\n", remotePath, localPath)
}

func handleLs(client *hdfs.Client, path string) {
	entries, err := client.ListDir(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing directory: %v\n", err)
		os.Exit(1)
	}

	for _, entry := range entries {
		typeChar := "-"
		if entry.IsDirectory {
			typeChar = "d"
		}
		fmt.Printf("%s  r=%d  %12d  %s\n",
			typeChar,
			entry.ReplicationFactor,
			entry.SizeBytes,
			entry.Path,
		)
	}
}

func handleMkdir(client *hdfs.Client, path string) {
	if err := client.MkDir(path, true); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created directory: %s\n", path)
}

func handleRm(client *hdfs.Client, path string) {
	if err := client.DeleteFile(path, true); err != nil {
		fmt.Fprintf(os.Stderr, "Error deleting: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Deleted: %s\n", path)
}

func handleInfo(client *hdfs.Client, path string) {
	info, err := client.GetFileInfo(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting info: %v\n", err)
		os.Exit(1)
	}

	typeStr := "file"
	if info.IsDirectory {
		typeStr = "directory"
	}

	fmt.Printf("Path:        %s\n", info.Path)
	fmt.Printf("Type:        %s\n", typeStr)
	fmt.Printf("Size:        %d bytes\n", info.SizeBytes)
	fmt.Printf("Replication: %d\n", info.ReplicationFactor)
	fmt.Printf("Blocks:      %d\n", len(info.BlockIds))
}
