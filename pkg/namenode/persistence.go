package namenode

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/block"
)

// PersistentState is the JSON-serializable snapshot of NameNode metadata.
type PersistentState struct {
	Timestamp   time.Time                   `json:"timestamp"`
	Namespace   *PersistentNamespace        `json:"namespace"`
	Blocks      map[string]*block.Metadata  `json:"blocks"`
	DataNodes   map[string]*DataNodeInfo    `json:"datanodes"`
}

// PersistentNamespace is the JSON-serializable namespace tree.
type PersistentNamespace struct {
	Name        string                         `json:"name"`
	IsDir       bool                           `json:"is_dir"`
	Children    map[string]*PersistentNamespace `json:"children,omitempty"`
	BlockIDs    []string                       `json:"block_ids,omitempty"`
	Size        int64                          `json:"size,omitempty"`
	Replication int32                          `json:"replication,omitempty"`
	Permissions int32                          `json:"permissions,omitempty"`
}

// SaveState writes the NameNode state to a JSON file.
func SaveState(ns *Namespace, bm *BlockManager, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create metadata dir: %w", err)
	}

	ns.mu.RLock()
	pns := serializeNamespace(ns.root)
	ns.mu.RUnlock()

	bm.mu.RLock()
	blocks := make(map[string]*block.Metadata, len(bm.blocks))
	for k, v := range bm.blocks {
		copied := *v
		blocks[k] = &copied
	}
	datanodes := make(map[string]*DataNodeInfo, len(bm.datanodes))
	for k, v := range bm.datanodes {
		copied := *v
		datanodes[k] = &copied
	}
	bm.mu.RUnlock()

	state := PersistentState{
		Timestamp: time.Now(),
		Namespace: pns,
		Blocks:    blocks,
		DataNodes: datanodes,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	path := filepath.Join(dir, "namenode-state.json")
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("write state file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename state file: %w", err)
	}

	slog.Info("NameNode state saved", "path", path, "size", len(data))
	return nil
}

// LoadState reads the NameNode state from a JSON file.
// Returns nil, nil if the state file doesn't exist.
func LoadState(dir string) (*PersistentState, error) {
	path := filepath.Join(dir, "namenode-state.json")

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read state file: %w", err)
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}

	slog.Info("NameNode state loaded", "path", path, "timestamp", state.Timestamp)
	return &state, nil
}

func serializeNamespace(node *FSNode) *PersistentNamespace {
	pn := &PersistentNamespace{
		Name:        node.Name,
		IsDir:       node.IsDir,
		Permissions: node.Permissions,
	}

	if node.IsDir {
		pn.Children = make(map[string]*PersistentNamespace, len(node.Children))
		for name, child := range node.Children {
			pn.Children[name] = serializeNamespace(child)
		}
	} else {
		pn.BlockIDs = node.BlockIDs
		pn.Size = node.Size
		pn.Replication = node.Replication
	}

	return pn
}

// RestoreNamespace rebuilds the in-memory namespace from persisted state.
func RestoreNamespace(pns *PersistentNamespace) *Namespace {
	if pns == nil {
		return NewNamespace()
	}

	ns := &Namespace{
		root: deserializeNode(pns, nil),
	}
	return ns
}

func deserializeNode(pn *PersistentNamespace, parent *FSNode) *FSNode {
	node := &FSNode{
		Name:        pn.Name,
		IsDir:       pn.IsDir,
		Parent:      parent,
		Permissions: pn.Permissions,
		ModTime:     time.Now(),
	}

	if pn.IsDir {
		node.Children = make(map[string]*FSNode, len(pn.Children))
		for name, childPN := range pn.Children {
			node.Children[name] = deserializeNode(childPN, node)
		}
	} else {
		node.BlockIDs = pn.BlockIDs
		node.Size = pn.Size
		node.Replication = pn.Replication
	}

	return node
}
