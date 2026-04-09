package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	DefaultBlockSize         = 128 * 1024 * 1024 // 128 MB
	DefaultReplicationFactor = 3
	DefaultHeartbeatInterval = 3 * time.Second
	DefaultDeadNodeTimeout   = 10 * time.Second
	DefaultBlockReportInterval = 30 * time.Second
	DefaultMetadataDumpInterval = 60 * time.Second
	DefaultSortBufferSize    = 64 * 1024 * 1024 // 64 MB
	DefaultSpillThreshold    = 0.8
	DefaultMaxTaskRetries    = 3
	DefaultChunkSize         = 1 * 1024 * 1024 // 1 MB for gRPC streaming

	DefaultNameNodePort         = 9000
	DefaultDataNodePort         = 9001
	DefaultResourceManagerPort  = 9010
	DefaultNodeManagerPort      = 9011
	DefaultShufflePort          = 9020
)

// Config holds all configuration for mini-Hadoop components.
type Config struct {
	// HDFS settings
	BlockSize         int64         `json:"block_size"`
	ReplicationFactor int32         `json:"replication_factor"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	DeadNodeTimeout   time.Duration `json:"dead_node_timeout"`
	BlockReportInterval time.Duration `json:"block_report_interval"`
	MetadataDumpInterval time.Duration `json:"metadata_dump_interval"`
	ChunkSize         int           `json:"chunk_size"`

	// NameNode settings
	NameNodeHost string `json:"namenode_host"`
	NameNodePort int    `json:"namenode_port"`
	MetadataDir  string `json:"metadata_dir"`

	// DataNode settings
	DataNodePort int    `json:"datanode_port"`
	DataDir      string `json:"data_dir"`

	// ResourceManager settings
	ResourceManagerHost string `json:"resourcemanager_host"`
	ResourceManagerPort int    `json:"resourcemanager_port"`

	// NodeManager settings
	NodeManagerPort int   `json:"nodemanager_port"`
	TotalMemoryMB   int32 `json:"total_memory_mb"`
	TotalCPU        int32 `json:"total_cpu"`

	// MapReduce settings
	SortBufferSize  int     `json:"sort_buffer_size"`
	SpillThreshold  float64 `json:"spill_threshold"`
	MaxTaskRetries  int     `json:"max_task_retries"`
	ShufflePort     int     `json:"shuffle_port"`
	TempDir         string  `json:"temp_dir"`
}

// DefaultConfig returns a Config with all default values.
func DefaultConfig() Config {
	return Config{
		BlockSize:           DefaultBlockSize,
		ReplicationFactor:   DefaultReplicationFactor,
		HeartbeatInterval:   DefaultHeartbeatInterval,
		DeadNodeTimeout:     DefaultDeadNodeTimeout,
		BlockReportInterval: DefaultBlockReportInterval,
		MetadataDumpInterval: DefaultMetadataDumpInterval,
		ChunkSize:           DefaultChunkSize,

		NameNodeHost: "localhost",
		NameNodePort: DefaultNameNodePort,
		MetadataDir:  "/tmp/minihadoop/namenode",

		DataNodePort: DefaultDataNodePort,
		DataDir:      "/tmp/minihadoop/datanode",

		ResourceManagerHost: "localhost",
		ResourceManagerPort: DefaultResourceManagerPort,

		NodeManagerPort: DefaultNodeManagerPort,
		TotalMemoryMB:   4096,
		TotalCPU:        4,

		SortBufferSize: DefaultSortBufferSize,
		SpillThreshold: DefaultSpillThreshold,
		MaxTaskRetries: DefaultMaxTaskRetries,
		ShufflePort:    DefaultShufflePort,
		TempDir:        "/tmp/minihadoop/temp",
	}
}

// LoadConfig reads a JSON config file, merges with defaults, then applies
// environment variable overrides (MINIHADOOP_* prefix).
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				// Use defaults if file doesn't exist
			} else {
				return cfg, err
			}
		} else {
			if err := json.Unmarshal(data, &cfg); err != nil {
				return cfg, err
			}
		}
	}

	applyEnvOverrides(&cfg)
	return cfg, nil
}

// applyEnvOverrides reads MINIHADOOP_* environment variables and overrides config.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("MINIHADOOP_NAMENODE_HOST"); v != "" {
		cfg.NameNodeHost = v
	}
	if v := os.Getenv("MINIHADOOP_NAMENODE_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NameNodePort = n
		}
	}
	if v := os.Getenv("MINIHADOOP_RM_HOST"); v != "" {
		cfg.ResourceManagerHost = v
	}
	if v := os.Getenv("MINIHADOOP_RM_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ResourceManagerPort = n
		}
	}
	if v := os.Getenv("MINIHADOOP_DATANODE_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.DataNodePort = n
		}
	}
	if v := os.Getenv("MINIHADOOP_NM_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.NodeManagerPort = n
		}
	}
	if v := os.Getenv("MINIHADOOP_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("MINIHADOOP_METADATA_DIR"); v != "" {
		cfg.MetadataDir = v
	}
	if v := os.Getenv("MINIHADOOP_TEMP_DIR"); v != "" {
		cfg.TempDir = v
	}
}

// SaveConfig writes the config to a JSON file.
func SaveConfig(cfg Config, path string) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// NameNodeAddress returns the full NameNode address.
func (c Config) NameNodeAddress() string {
	return formatAddress(c.NameNodeHost, c.NameNodePort)
}

// ResourceManagerAddress returns the full ResourceManager address.
func (c Config) ResourceManagerAddress() string {
	return formatAddress(c.ResourceManagerHost, c.ResourceManagerPort)
}

func formatAddress(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}
