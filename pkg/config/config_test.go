package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.BlockSize != DefaultBlockSize {
		t.Errorf("BlockSize = %d, want %d", cfg.BlockSize, DefaultBlockSize)
	}
	if cfg.ReplicationFactor != DefaultReplicationFactor {
		t.Errorf("ReplicationFactor = %d, want %d", cfg.ReplicationFactor, DefaultReplicationFactor)
	}
	if cfg.HeartbeatInterval != DefaultHeartbeatInterval {
		t.Errorf("HeartbeatInterval = %v, want %v", cfg.HeartbeatInterval, DefaultHeartbeatInterval)
	}
	if cfg.DeadNodeTimeout != DefaultDeadNodeTimeout {
		t.Errorf("DeadNodeTimeout = %v, want %v", cfg.DeadNodeTimeout, DefaultDeadNodeTimeout)
	}
	if cfg.NameNodePort != DefaultNameNodePort {
		t.Errorf("NameNodePort = %d, want %d", cfg.NameNodePort, DefaultNameNodePort)
	}
	if cfg.ChunkSize != DefaultChunkSize {
		t.Errorf("ChunkSize = %d, want %d", cfg.ChunkSize, DefaultChunkSize)
	}
}

func TestNameNodeAddress(t *testing.T) {
	cfg := DefaultConfig()
	addr := cfg.NameNodeAddress()
	expected := "localhost:9000"
	if addr != expected {
		t.Errorf("NameNodeAddress() = %q, want %q", addr, expected)
	}
}

func TestResourceManagerAddress(t *testing.T) {
	cfg := DefaultConfig()
	addr := cfg.ResourceManagerAddress()
	expected := "localhost:9010"
	if addr != expected {
		t.Errorf("ResourceManagerAddress() = %q, want %q", addr, expected)
	}
}

func TestLoadConfigNonExistent(t *testing.T) {
	cfg, err := LoadConfig("/nonexistent/path/config.json")
	if err != nil {
		t.Fatalf("LoadConfig non-existent file should return defaults, got error: %v", err)
	}
	if cfg.BlockSize != DefaultBlockSize {
		t.Errorf("Expected default BlockSize, got %d", cfg.BlockSize)
	}
}

func TestSaveAndLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	original := DefaultConfig()
	original.BlockSize = 256 * 1024 * 1024
	original.ReplicationFactor = 2
	original.NameNodeHost = "master-node"
	original.NameNodePort = 8020

	if err := SaveConfig(original, path); err != nil {
		t.Fatalf("SaveConfig failed: %v", err)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("Config file was not created")
	}

	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if loaded.BlockSize != original.BlockSize {
		t.Errorf("BlockSize = %d, want %d", loaded.BlockSize, original.BlockSize)
	}
	if loaded.ReplicationFactor != original.ReplicationFactor {
		t.Errorf("ReplicationFactor = %d, want %d", loaded.ReplicationFactor, original.ReplicationFactor)
	}
	if loaded.NameNodeHost != original.NameNodeHost {
		t.Errorf("NameNodeHost = %q, want %q", loaded.NameNodeHost, original.NameNodeHost)
	}
}

func TestLoadConfigInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	os.WriteFile(path, []byte("{invalid json"), 0644)

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("Expected error for invalid JSON")
	}
}

func TestDefaultDurations(t *testing.T) {
	cfg := DefaultConfig()

	tests := []struct {
		name     string
		got      time.Duration
		expected time.Duration
	}{
		{"HeartbeatInterval", cfg.HeartbeatInterval, 3 * time.Second},
		{"DeadNodeTimeout", cfg.DeadNodeTimeout, 10 * time.Second},
		{"BlockReportInterval", cfg.BlockReportInterval, 30 * time.Second},
		{"MetadataDumpInterval", cfg.MetadataDumpInterval, 60 * time.Second},
	}

	for _, tt := range tests {
		if tt.got != tt.expected {
			t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.expected)
		}
	}
}
