package acceptance

import (
	"os"
	"strconv"
	"testing"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/hdfs"
)

// TestAC1_FileWriteRead tests AC-1: Write a 500MB file, read it back, verify SHA-256.
// For faster CI, uses 10MB by default; set MINIHADOOP_AC1_SIZE_MB=500 for full test.
func TestAC1_FileWriteRead(t *testing.T) {
	if os.Getenv("MINIHADOOP_CLUSTER") == "" {
		t.Skip("Skipping: requires running cluster (set MINIHADOOP_CLUSTER=1)")
	}
	sizeMB := 10
	if s := os.Getenv("MINIHADOOP_AC1_SIZE_MB"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			sizeMB = n
		}
	}

	t.Logf("Generating %d MB test data...", sizeMB)
	inputPath := generateTestData(t, sizeMB)
	inputHash, err := fileSHA256(inputPath)
	if err != nil {
		t.Fatalf("hash input: %v", err)
	}
	t.Logf("Input SHA-256: %s", inputHash)

	// Connect to HDFS
	cfg := config.DefaultConfig()
	cfg.NameNodeHost = os.Getenv("MINIHADOOP_NAMENODE_HOST")
	if cfg.NameNodeHost == "" {
		cfg.NameNodeHost = "localhost"
	}

	client, err := hdfs.NewClient(cfg)
	if err != nil {
		t.Fatalf("connect to NameNode: %v", err)
	}
	defer client.Close()

	// Create directory
	if err := client.MkDir("/test", true); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Upload file
	remotePath := "/test/ac1-testfile.dat"
	f, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("open input: %v", err)
	}
	defer f.Close()

	t.Log("Uploading file to HDFS...")
	if err := client.CreateFile(remotePath, f, 0); err != nil {
		t.Fatalf("upload file: %v", err)
	}

	// Download file
	outputPath := t.TempDir() + "/downloaded.dat"
	outFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create output: %v", err)
	}

	t.Log("Downloading file from HDFS...")
	if err := client.ReadFile(remotePath, outFile); err != nil {
		outFile.Close()
		t.Fatalf("download file: %v", err)
	}
	outFile.Close()

	// Verify SHA-256
	outputHash, err := fileSHA256(outputPath)
	if err != nil {
		t.Fatalf("hash output: %v", err)
	}
	t.Logf("Output SHA-256: %s", outputHash)

	if inputHash != outputHash {
		t.Fatalf("SHA-256 mismatch!\n  input:  %s\n  output: %s", inputHash, outputHash)
	}

	t.Log("AC-1 PASSED: File write/read with SHA-256 verification")
}
