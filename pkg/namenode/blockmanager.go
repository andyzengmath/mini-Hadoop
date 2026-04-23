package namenode

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/block"
)

// DataNodeInfo tracks a registered DataNode's state.
type DataNodeInfo struct {
	NodeID        string
	Address       string
	CapacityBytes int64
	UsedBytes     int64
	AvailableBytes int64
	NumBlocks     int32
	LastHeartbeat time.Time
	Alive         bool
}

// BlockCommand represents a command sent to a DataNode via heartbeat response.
type BlockCommand struct {
	Type        string // "REPLICATE" or "DELETE"
	BlockID     string
	TargetNodes []string // For REPLICATE: destination addresses
	SourceNode  string   // For REPLICATE: source address
}

// BlockManager tracks all blocks and their locations across DataNodes.
type BlockManager struct {
	mu sync.RWMutex

	// block ID -> block metadata
	blocks map[string]*block.Metadata

	// DataNode tracking
	datanodes map[string]*DataNodeInfo // nodeID -> info

	// Pending commands per DataNode (sent via next heartbeat)
	pendingCommands map[string][]BlockCommand // nodeID -> commands

	// Configuration
	defaultReplication int32
	deadNodeTimeout    time.Duration
}

// NewBlockManager creates a new BlockManager.
func NewBlockManager(defaultReplication int32, deadNodeTimeout time.Duration) *BlockManager {
	return &BlockManager{
		blocks:            make(map[string]*block.Metadata),
		datanodes:         make(map[string]*DataNodeInfo),
		pendingCommands:   make(map[string][]BlockCommand),
		defaultReplication: defaultReplication,
		deadNodeTimeout:   deadNodeTimeout,
	}
}

// Restore loads previously persisted block metadata and DataNode registrations.
// DataNodes are restored as initially dead — they become alive when their first heartbeat
// arrives after restart. This avoids incorrectly treating pre-restart addresses as live targets
// for block allocation before the real DNs have reconnected.
func (bm *BlockManager) Restore(blocks map[string]*block.Metadata, datanodes map[string]*DataNodeInfo) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if blocks != nil {
		for id, meta := range blocks {
			copied := *meta
			bm.blocks[id] = &copied
		}
	}
	if datanodes != nil {
		for id, dn := range datanodes {
			copied := *dn
			copied.Alive = false // wait for heartbeat to confirm liveness
			bm.datanodes[id] = &copied
		}
	}
}

// RegisterDataNode registers a new DataNode with the cluster.
func (bm *BlockManager) RegisterDataNode(nodeID, address string, capacity int64) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.datanodes[nodeID] = &DataNodeInfo{
		NodeID:         nodeID,
		Address:        address,
		CapacityBytes:  capacity,
		AvailableBytes: capacity,
		LastHeartbeat:  time.Now(),
		Alive:          true,
	}
	slog.Info("DataNode registered", "nodeID", nodeID, "address", address)
	return nil
}

// ProcessHeartbeat updates DataNode state and returns pending commands.
func (bm *BlockManager) ProcessHeartbeat(nodeID string, usedBytes, availableBytes int64, numBlocks int32) ([]BlockCommand, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	dn, exists := bm.datanodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("unknown DataNode: %s", nodeID)
	}

	dn.UsedBytes = usedBytes
	dn.AvailableBytes = availableBytes
	dn.NumBlocks = numBlocks
	dn.LastHeartbeat = time.Now()
	dn.Alive = true

	// Return and clear pending commands
	cmds := bm.pendingCommands[nodeID]
	delete(bm.pendingCommands, nodeID)
	return cmds, nil
}

// ProcessBlockReport reconciles a DataNode's block inventory with expected state.
// Returns block IDs the DataNode should delete (orphaned blocks).
func (bm *BlockManager) ProcessBlockReport(nodeID string, reportedBlocks []struct {
	BlockID         string
	SizeBytes       int64
	GenerationStamp int64
}) []string {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	var toDelete []string

	for _, rb := range reportedBlocks {
		meta, exists := bm.blocks[rb.BlockID]
		if !exists {
			// Block not in our records — orphaned, tell DN to delete
			toDelete = append(toDelete, rb.BlockID)
			continue
		}

		// Check generation stamp — stale blocks should be deleted
		if rb.GenerationStamp < meta.GenerationStamp {
			toDelete = append(toDelete, rb.BlockID)
			continue
		}

		// Ensure this DataNode is recorded as a location
		dn, dnExists := bm.datanodes[nodeID]
		if !dnExists || dn == nil {
			slog.Warn("block report from unknown DataNode", "nodeID", nodeID)
			continue
		}
		if !containsString(meta.Locations, dn.Address) {
			meta.Locations = append(meta.Locations, dn.Address)
		}
		// Confirm pending replication if this address was pending
		meta.ClearPendingLocation(dn.Address)
	}

	return toDelete
}

// AllocateBlock creates a new block and selects DataNodes for its pipeline.
// Placement policy: pick N different nodes with most available capacity.
func (bm *BlockManager) AllocateBlock(replication int32, clientAddr string) (*block.Metadata, []string, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if replication <= 0 {
		replication = bm.defaultReplication
	}

	// Get alive DataNodes sorted by available capacity
	aliveNodes := bm.getAliveNodesSorted()
	if int32(len(aliveNodes)) < replication {
		return nil, nil, fmt.Errorf("not enough DataNodes: need %d, have %d alive",
			replication, len(aliveNodes))
	}

	// Select nodes for pipeline (top N by available capacity, all different)
	selected := aliveNodes[:replication]
	pipeline := make([]string, len(selected))
	locations := make([]string, len(selected))
	for i, dn := range selected {
		pipeline[i] = dn.Address
		locations[i] = dn.Address
	}

	meta := block.NewMetadata(replication)
	meta.Locations = locations
	bm.blocks[meta.BlockID.String()] = &meta

	return &meta, pipeline, nil
}

// CompleteBlock finalizes a block after successful pipeline write.
func (bm *BlockManager) CompleteBlock(blockID string, sizeBytes int64, checksum []byte) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	meta, exists := bm.blocks[blockID]
	if !exists {
		return fmt.Errorf("unknown block: %s", blockID)
	}
	meta.SizeBytes = sizeBytes
	meta.Checksum = checksum
	return nil
}

// GetBlockLocations returns the locations of all blocks for the given block IDs.
func (bm *BlockManager) GetBlockLocations(blockIDs []string) []*block.Metadata {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	result := make([]*block.Metadata, 0, len(blockIDs))
	for _, id := range blockIDs {
		if meta, exists := bm.blocks[id]; exists {
			result = append(result, meta)
		}
	}
	return result
}

// RemoveBlock removes a block from tracking and queues delete commands to DataNodes.
func (bm *BlockManager) RemoveBlock(blockID string) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	meta, exists := bm.blocks[blockID]
	if !exists {
		return
	}

	// Queue delete commands to all DataNodes holding this block
	for _, addr := range meta.Locations {
		nodeID := bm.addressToNodeID(addr)
		if nodeID != "" {
			bm.pendingCommands[nodeID] = append(bm.pendingCommands[nodeID], BlockCommand{
				Type:    "DELETE",
				BlockID: blockID,
			})
		}
	}

	delete(bm.blocks, blockID)
}

// CheckAndReplicateBlocks scans for under-replicated blocks and schedules re-replication.
// This should be called periodically (e.g., every heartbeat cycle).
func (bm *BlockManager) CheckAndReplicateBlocks() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for blockID, meta := range bm.blocks {
		// Remove dead node locations
		aliveLocations := make([]string, 0, len(meta.Locations))
		for _, addr := range meta.Locations {
			nodeID := bm.addressToNodeID(addr)
			if nodeID != "" {
				dn := bm.datanodes[nodeID]
				if dn != nil && dn.Alive {
					aliveLocations = append(aliveLocations, addr)
				}
			}
		}
		meta.Locations = aliveLocations

		if !meta.IsUnderReplicated() {
			continue
		}

		deficit := meta.ReplicaDeficit()
		if len(meta.Locations) == 0 {
			slog.Error("block has NO replicas", "blockID", blockID)
			continue
		}

		// Find target nodes that don't already hold this block (or have pending replication)
		excludeAddrs := append(meta.Locations, meta.PendingLocations...)
		targets := bm.findReplicationTargets(excludeAddrs, deficit)
		if len(targets) == 0 {
			continue
		}

		sourceAddr := meta.Locations[0]
		for _, target := range targets {
			nodeID := bm.addressToNodeID(target)
			if nodeID == "" {
				continue
			}
			bm.pendingCommands[nodeID] = append(bm.pendingCommands[nodeID], BlockCommand{
				Type:        "REPLICATE",
				BlockID:     blockID,
				TargetNodes: []string{target},
				SourceNode:  sourceAddr,
			})
			// Track as pending — will be confirmed by block report or cleared on failure
			meta.PendingLocations = append(meta.PendingLocations, target)
			slog.Info("scheduled re-replication",
				"blockID", blockID,
				"source", sourceAddr,
				"target", target,
				"deficit_remaining", deficit-1,
			)
		}
	}
}

// DetectDeadNodes marks DataNodes as dead if their heartbeat has timed out.
// Returns the list of newly-dead node IDs.
func (bm *BlockManager) DetectDeadNodes() []string {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	var deadNodes []string
	now := time.Now()

	for nodeID, dn := range bm.datanodes {
		if dn.Alive && now.Sub(dn.LastHeartbeat) > bm.deadNodeTimeout {
			dn.Alive = false
			deadNodes = append(deadNodes, nodeID)
			slog.Warn("DataNode marked dead", "nodeID", nodeID, "lastHeartbeat", dn.LastHeartbeat)
		}
	}
	return deadNodes
}

// GetAliveDataNodes returns all alive DataNode addresses.
func (bm *BlockManager) GetAliveDataNodes() []string {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var addrs []string
	for _, dn := range bm.datanodes {
		if dn.Alive {
			addrs = append(addrs, dn.Address)
		}
	}
	return addrs
}

// GetDataNodeCount returns total and alive DataNode counts.
func (bm *BlockManager) GetDataNodeCount() (total, alive int) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	for _, dn := range bm.datanodes {
		total++
		if dn.Alive {
			alive++
		}
	}
	return
}

// getAliveNodesSorted returns alive DataNodes sorted by available capacity (descending).
func (bm *BlockManager) getAliveNodesSorted() []*DataNodeInfo {
	var alive []*DataNodeInfo
	for _, dn := range bm.datanodes {
		if dn.Alive {
			alive = append(alive, dn)
		}
	}
	// Simple insertion sort (small N for mini-Hadoop)
	for i := 1; i < len(alive); i++ {
		for j := i; j > 0 && alive[j].AvailableBytes > alive[j-1].AvailableBytes; j-- {
			alive[j], alive[j-1] = alive[j-1], alive[j]
		}
	}
	return alive
}

// findReplicationTargets finds nodes that don't hold the block yet.
func (bm *BlockManager) findReplicationTargets(existingLocations []string, count int32) []string {
	var targets []string
	for _, dn := range bm.getAliveNodesSorted() {
		if int32(len(targets)) >= count {
			break
		}
		if !containsString(existingLocations, dn.Address) {
			targets = append(targets, dn.Address)
		}
	}
	return targets
}

// addressToNodeID finds the nodeID for a given address.
func (bm *BlockManager) addressToNodeID(address string) string {
	for nodeID, dn := range bm.datanodes {
		if dn.Address == address {
			return nodeID
		}
	}
	return ""
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
