package acceptance

import (
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
)

// getNameNodeAddr returns the NameNode address from env or default.
func getNameNodeAddr() string {
	host := os.Getenv("MINIHADOOP_NAMENODE_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("MINIHADOOP_NAMENODE_PORT")
	if port == "" {
		port = "9000"
	}
	return host + ":" + port
}

// getRMAddr returns the ResourceManager address from env or default.
func getRMAddr() string {
	host := os.Getenv("MINIHADOOP_RM_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("MINIHADOOP_RM_PORT")
	if port == "" {
		port = "9010"
	}
	return host + ":" + port
}

// generateTestData creates a temp file with random text data of the given size.
func generateTestData(t *testing.T, sizeMB int) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "testdata-*.txt")
	if err != nil {
		t.Fatalf("create test data: %v", err)
	}

	words := []string{"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
		"hadoop", "distributed", "system", "block", "replication", "mapreduce",
		"namenode", "datanode", "resource", "manager", "container", "shuffle"}

	rng := rand.New(rand.NewSource(42))
	bytesWritten := 0
	target := sizeMB * 1024 * 1024

	for bytesWritten < target {
		lineLen := rng.Intn(10) + 5 // 5-14 words per line
		line := ""
		for i := 0; i < lineLen; i++ {
			if i > 0 {
				line += " "
			}
			line += words[rng.Intn(len(words))]
		}
		line += "\n"
		n, _ := f.WriteString(line)
		bytesWritten += n
	}

	f.Close()
	return f.Name()
}

// fileSHA256 computes the SHA-256 hash of a file.
func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
