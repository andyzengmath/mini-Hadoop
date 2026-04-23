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

// pendingReplicationTTL bounds how long a target can sit in a block's
// PendingLocations without being confirmed by a block report. A DataNode that
// accepts a REPLICATE command but fails (connection reset, buffer error) never
// sends a block report for that block, so the target would otherwise be stuck
// in PendingLocations forever — effectively blacklisted from future
// re-replication attempts. With the TTL, expired entries are purged so
// CheckAndReplicateBlocks can schedule a fresh attempt (possibly to a
// different target if the original DN is still failing).
//
// 60s is a balance: long enough to avoid thrashing a slow-but-working DN
// (default replicateBlock timeout is on the order of a 128MB stream + buffer),
// short enough that transient failures don't leave a block permanently
// under-replicated.
const pendingReplicationTTL = 60 * time.Second

// BlockManager tracks all blocks and their locations across DataNodes.
type BlockManager struct {
	mu sync.RWMutex

	// block ID -> block metadata
	blocks map[string]*block.Metadata

	// DataNode tracking
	datanodes map[string]*DataNodeInfo // nodeID -> info

	// Pending commands per DataNode (sent via next heartbeat)
	pendingCommands map[string][]BlockCommand // nodeID -> commands

	// pendingReplicationDeadlines[blockID][targetAddr] = deadline.
	// Populated when CheckAndReplicateBlocks schedules a REPLICATE command.
	// Purged when ProcessBlockReport confirms the replica, or when the
	// deadline passes without confirmation (see pendingReplicationTTL).
	pendingReplicationDeadlines map[string]map[string]time.Time

	// Configuration
	defaultReplication int32
	deadNodeTimeout    time.Duration
}

// NewBlockManager creates a new BlockManager.
func NewBlockManager(defaultReplication int32, deadNodeTimeout time.Duration) *BlockManager {
	return &BlockManager{
		blocks:                      make(map[string]*block.Metadata),
		datanodes:                   make(map[string]*DataNodeInfo),
		pendingCommands:             make(map[string][]BlockCommand),
		pendingReplicationDeadlines: make(map[string]map[string]time.Time),
		defaultReplication:          defaultReplication,
		deadNodeTimeout:             deadNodeTimeout,
	}
}

// Restore loads previously persisted block metadata and DataNode registrations.
//
// DataNodes are restored as Alive with LastHeartbeat reset to now, giving them
// one full deadNodeTimeout to actually heartbeat before DetectDeadNodes marks
// them dead. This grace period is necessary because CheckAndReplicateBlocks
// (which runs every heartbeat tick) strips locations from blocks whose DN is
// not Alive — without the grace, every block's Locations would be wiped before
// any DN had a chance to re-register after NN restart, which caused spurious
// "no locations for block X" errors immediately after restart (observed in
// v2 F5 as 4/5 SHA match instead of 5/5). If a DN is genuinely dead, it
// simply won't heartbeat in time and gets marked dead correctly.
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
		now := time.Now()
		for id, dn := range datanodes {
			copied := *dn
			copied.Alive = true
			copied.LastHeartbeat = now
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
		// Confirm pending replication: the block is actually on this DN, so
		// both the pending list and its deadline are no longer needed.
		meta.ClearPendingLocation(dn.Address)
		if deadlines := bm.pendingReplicationDeadlines[rb.BlockID]; deadlines != nil {
			delete(deadlines, dn.Address)
			if len(deadlines) == 0 {
				delete(bm.pendingReplicationDeadlines, rb.BlockID)
			}
		}
	}

	return toDelete
}

// expireStalePendingLocked clears entries from meta.PendingLocations whose
// deadline has passed. Must be called with bm.mu held. Called at the top of
// CheckAndReplicateBlocks so expired pendings don't prevent a retry.
func (bm *BlockManager) expireStalePendingLocked(blockID string, meta *block.Metadata, now time.Time) {
	deadlines, ok := bm.pendingReplicationDeadlines[blockID]
	if !ok || len(deadlines) == 0 {
		return
	}
	kept := meta.PendingLocations[:0]
	for _, addr := range meta.PendingLocations {
		deadline, tracked := deadlines[addr]
		if tracked && now.After(deadline) {
			slog.Warn("expiring stale pending replication",
				"blockID", blockID, "target", addr, "age_over", now.Sub(deadline))
			delete(deadlines, addr)
			continue
		}
		kept = append(kept, addr)
	}
	meta.PendingLocations = kept
	if len(deadlines) == 0 {
		delete(bm.pendingReplicationDeadlines, blockID)
	}
}

// AllocateBlock creates a new block and selects DataNodes for its pipeline.
// Placement policy: pick N different nodes with most available capacity.
//
// When fewer DataNodes are alive than the requested replication factor, the
// block is allocated with DEGRADED replication: pipeline width = alive count,
// ReplicationTarget = requested. The NN's background re-replication cycle will
// create the missing replicas once more DNs rejoin. This lets the cluster keep
// serving writes through transient DN loss, matching Apache Hadoop's behavior.
// The v2 F7 benchmark (kill a DN mid-write) previously failed here with
// "not enough DataNodes: need 3, have 2 alive" even when 2 DNs were enough to
// store the data durably.
func (bm *BlockManager) AllocateBlock(replication int32, clientAddr string) (*block.Metadata, []string, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if replication <= 0 {
		replication = bm.defaultReplication
	}

	// Get alive DataNodes sorted by available capacity
	aliveNodes := bm.getAliveNodesSorted()
	if len(aliveNodes) == 0 {
		return nil, nil, fmt.Errorf("no DataNodes alive")
	}

	// Width of the write pipeline: capped at the number of alive nodes.
	// If this is less than the requested replication, the block starts out
	// under-replicated and CheckAndReplicateBlocks will restore the target.
	pipelineWidth := int(replication)
	if len(aliveNodes) < pipelineWidth {
		pipelineWidth = len(aliveNodes)
		slog.Warn("allocating block with degraded replication",
			"requested", replication,
			"actual_pipeline", pipelineWidth,
			"alive_datanodes", len(aliveNodes),
		)
	}

	// Select nodes for pipeline (top N by available capacity, all different)
	selected := aliveNodes[:pipelineWidth]
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
	delete(bm.pendingReplicationDeadlines, blockID)
}

// CheckAndReplicateBlocks scans for under-replicated blocks and schedules re-replication.
// This should be called periodically (e.g., every heartbeat cycle).
func (bm *BlockManager) CheckAndReplicateBlocks() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	now := time.Now()
	for blockID, meta := range bm.blocks {
		// Expire stale pending replications. A DN may have accepted a
		// REPLICATE command but failed (connection reset mid-stream, etc.)
		// and never sent a block report — without this, the target would
		// stay in PendingLocations forever, effectively blacklisting it.
		bm.expireStalePendingLocked(blockID, meta, now)

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
			// Track as pending — confirmed by block report, cleared by TTL if unconfirmed.
			meta.PendingLocations = append(meta.PendingLocations, target)
			if bm.pendingReplicationDeadlines[blockID] == nil {
				bm.pendingReplicationDeadlines[blockID] = make(map[string]time.Time)
			}
			bm.pendingReplicationDeadlines[blockID][target] = now.Add(pendingReplicationTTL)
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
