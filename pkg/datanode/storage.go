package datanode

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// BlockStorage manages block data on the local filesystem.
type BlockStorage struct {
	mu      sync.RWMutex
	dataDir string
	blocks  map[string]BlockEntry // blockID -> entry
}

// BlockEntry tracks a locally stored block.
type BlockEntry struct {
	BlockID         string
	SizeBytes       int64
	GenerationStamp int64
	FilePath        string
}

// NewBlockStorage creates a new block storage backed by the given directory.
func NewBlockStorage(dataDir string) (*BlockStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	bs := &BlockStorage{
		dataDir: dataDir,
		blocks:  make(map[string]BlockEntry),
	}

	// Scan existing blocks on startup
	if err := bs.scanExistingBlocks(); err != nil {
		slog.Warn("error scanning existing blocks", "error", err)
	}

	return bs, nil
}

// WriteBlock writes block data to local storage.
func (bs *BlockStorage) WriteBlock(blockID string, generationStamp int64, data io.Reader) (int64, []byte, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	path, err := bs.blockPath(blockID)
	if err != nil {
		return 0, nil, fmt.Errorf("invalid block ID: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, nil, fmt.Errorf("create block file: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	writer := io.MultiWriter(f, hasher)

	written, err := io.Copy(writer, data)
	if err != nil {
		os.Remove(path)
		return 0, nil, fmt.Errorf("write block data: %w", err)
	}

	bs.blocks[blockID] = BlockEntry{
		BlockID:         blockID,
		SizeBytes:       written,
		GenerationStamp: generationStamp,
		FilePath:        path,
	}

	slog.Info("block written", "blockID", blockID, "size", written)
	return written, hasher.Sum(nil), nil
}

// ReadBlock returns a reader for the block data.
func (bs *BlockStorage) ReadBlock(blockID string) (io.ReadCloser, int64, error) {
	if _, err := bs.blockPath(blockID); err != nil {
		return nil, 0, fmt.Errorf("invalid block ID: %w", err)
	}

	bs.mu.RLock()
	entry, exists := bs.blocks[blockID]
	bs.mu.RUnlock()

	if !exists {
		return nil, 0, fmt.Errorf("block not found: %s", blockID)
	}

	f, err := os.Open(entry.FilePath)
	if err != nil {
		return nil, 0, fmt.Errorf("open block file: %w", err)
	}

	return f, entry.SizeBytes, nil
}

// DeleteBlock removes a block from local storage.
func (bs *BlockStorage) DeleteBlock(blockID string) error {
	if _, err := bs.blockPath(blockID); err != nil {
		return fmt.Errorf("invalid block ID: %w", err)
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	entry, exists := bs.blocks[blockID]
	if !exists {
		return nil // Already gone
	}

	if err := os.Remove(entry.FilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove block file: %w", err)
	}

	delete(bs.blocks, blockID)
	slog.Info("block deleted", "blockID", blockID)
	return nil
}

// HasBlock checks if a block exists locally.
func (bs *BlockStorage) HasBlock(blockID string) bool {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	_, exists := bs.blocks[blockID]
	return exists
}

// GetBlockReport returns all locally stored blocks for reporting to the NameNode.
func (bs *BlockStorage) GetBlockReport() []BlockEntry {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	report := make([]BlockEntry, 0, len(bs.blocks))
	for _, entry := range bs.blocks {
		report = append(report, entry)
	}
	return report
}

// GetUsedBytes returns total bytes used by stored blocks.
func (bs *BlockStorage) GetUsedBytes() int64 {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	var total int64
	for _, entry := range bs.blocks {
		total += entry.SizeBytes
	}
	return total
}

// BlockCount returns the number of stored blocks.
func (bs *BlockStorage) BlockCount() int32 {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return int32(len(bs.blocks))
}

// blockPath returns a safe file path for a block, rejecting path traversal attempts.
func (bs *BlockStorage) blockPath(blockID string) (string, error) {
	if blockID == "" {
		return "", fmt.Errorf("empty block ID")
	}
	if strings.ContainsAny(blockID, `/\`) || strings.Contains(blockID, "..") {
		return "", fmt.Errorf("invalid block ID: %q", blockID)
	}
	resolved := filepath.Join(bs.dataDir, blockID+".blk")
	absResolved, err := filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	absDataDir, err := filepath.Abs(bs.dataDir)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absResolved, absDataDir+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal detected in block ID: %q", blockID)
	}
	return absResolved, nil
}

func (bs *BlockStorage) scanExistingBlocks() error {
	entries, err := os.ReadDir(bs.dataDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) > 4 && name[len(name)-4:] == ".blk" {
			blockID := name[:len(name)-4]
			info, err := entry.Info()
			if err != nil {
				continue
			}
			bs.blocks[blockID] = BlockEntry{
				BlockID:         blockID,
				SizeBytes:       info.Size(),
				GenerationStamp: 1,
				FilePath:        filepath.Join(bs.dataDir, name),
			}
		}
	}

	slog.Info("scanned existing blocks", "count", len(bs.blocks))
	return nil
}
