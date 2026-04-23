package namenode

import (
	"testing"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/block"
)

func makeManager(replication int32, timeout time.Duration) *BlockManager {
	return NewBlockManager(replication, timeout)
}

func TestRegisterDataNode(t *testing.T) {
	bm := makeManager(3, 10*time.Second)
	if err := bm.RegisterDataNode("n1", "host1:9001", 100<<30); err != nil {
		t.Fatalf("RegisterDataNode: %v", err)
	}
	total, alive := bm.GetDataNodeCount()
	if total != 1 || alive != 1 {
		t.Errorf("expected 1 total, 1 alive; got %d, %d", total, alive)
	}
}

func TestAllocateBlock_Degraded(t *testing.T) {
	// When fewer DNs are alive than requested replication, allocation should
	// succeed with a degraded pipeline (1 node) rather than failing outright.
	// Re-replication will restore the target once more DNs rejoin.
	bm := makeManager(3, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)

	meta, pipeline, err := bm.AllocateBlock(3, "client")
	if err != nil {
		t.Fatalf("expected degraded allocation to succeed with 1 alive DN, got: %v", err)
	}
	if len(pipeline) != 1 {
		t.Errorf("expected pipeline width 1 (the only alive node), got %d", len(pipeline))
	}
	if meta.ReplicationTarget != 3 {
		t.Errorf("ReplicationTarget should retain requested value 3 so re-replication can restore it; got %d", meta.ReplicationTarget)
	}
	if !meta.IsUnderReplicated() {
		t.Error("a degraded-allocated block should be flagged under-replicated so NN schedules repair")
	}
}

func TestAllocateBlock_NoAliveNodes(t *testing.T) {
	// With zero alive DNs, allocation must still fail — there's nowhere to
	// place even a single replica. The degraded path tolerates reduced
	// redundancy, not total loss.
	bm := makeManager(3, 10*time.Second)
	_, _, err := bm.AllocateBlock(3, "client")
	if err == nil {
		t.Fatal("expected error when zero DNs alive")
	}
}

func TestAllocateBlock_Success(t *testing.T) {
	bm := makeManager(3, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	bm.RegisterDataNode("n2", "host2:9001", 200<<30)
	bm.RegisterDataNode("n3", "host3:9001", 50<<30)

	meta, pipeline, err := bm.AllocateBlock(3, "client")
	if err != nil {
		t.Fatalf("AllocateBlock: %v", err)
	}
	if len(pipeline) != 3 {
		t.Fatalf("expected 3 pipeline nodes, got %d", len(pipeline))
	}
	if meta.BlockID == "" {
		t.Error("expected non-empty block ID")
	}
	// First in pipeline should be highest capacity (host2)
	if pipeline[0] != "host2:9001" {
		t.Errorf("expected host2:9001 first (highest capacity), got %s", pipeline[0])
	}
}

func TestAllocateBlock_DefaultReplication(t *testing.T) {
	bm := makeManager(3, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	bm.RegisterDataNode("n2", "host2:9001", 100<<30)
	bm.RegisterDataNode("n3", "host3:9001", 100<<30)

	meta, _, err := bm.AllocateBlock(0, "client") // 0 means use default
	if err != nil {
		t.Fatalf("AllocateBlock with default: %v", err)
	}
	if meta.ReplicationTarget != 3 {
		t.Errorf("expected default replication 3, got %d", meta.ReplicationTarget)
	}
}

func TestPendingReplicationTTL_ExpiresStaleEntries(t *testing.T) {
	// Regression guard for "block permanently under-replicated after a DN
	// fails mid-replicate". PendingLocations was never cleared on failure —
	// only on successful block report. After a transient replication failure,
	// the target stayed blacklisted forever, and if every DN ended up in that
	// state the block could never recover.
	//
	// After this fix: scheduling a REPLICATE stamps a deadline; the next
	// CheckAndReplicateBlocks call after the deadline expires the entry.
	bm := makeManager(3, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	bm.RegisterDataNode("n2", "host2:9001", 100<<30)
	bm.RegisterDataNode("n3", "host3:9001", 100<<30)

	// Create a block that's under-replicated (only one location).
	blockID := "blk_ttl_test"
	meta := block.NewMetadata(3)
	meta.BlockID = block.ID(blockID)
	meta.Locations = []string{"host1:9001"}
	bm.mu.Lock()
	bm.blocks[blockID] = &meta
	bm.mu.Unlock()

	// First cycle: CheckAndReplicateBlocks schedules a REPLICATE, stamping
	// pending-deadline ~= now + pendingReplicationTTL.
	bm.CheckAndReplicateBlocks()

	bm.mu.RLock()
	pendingAfterFirst := len(bm.blocks[blockID].PendingLocations)
	bm.mu.RUnlock()
	if pendingAfterFirst == 0 {
		t.Fatal("expected CheckAndReplicateBlocks to schedule a REPLICATE on first cycle")
	}

	// Simulate the deadline having passed: reach into the deadlines map and
	// rewind them into the past. A mid-replicate DN failure would match this
	// state naturally once the TTL elapses.
	bm.mu.Lock()
	for addr := range bm.pendingReplicationDeadlines[blockID] {
		bm.pendingReplicationDeadlines[blockID][addr] = time.Now().Add(-time.Second)
	}
	bm.mu.Unlock()

	// Second cycle: expiration should clear the stale pending and schedule a
	// fresh attempt (which picks a target out of PendingLocations, possibly
	// the same one — the point is it's no longer blacklisted).
	bm.CheckAndReplicateBlocks()

	// The block should still be under-replicated (no real DN confirmed), but
	// a new scheduling attempt should have happened — meaning PendingLocations
	// has at least one fresh entry, not the original stale one stuck forever.
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	pendingAfterSecond := len(bm.blocks[blockID].PendingLocations)
	if pendingAfterSecond == 0 {
		t.Error("expected re-scheduling after stale pending was expired; got empty PendingLocations")
	}

	// Deadlines map should have fresh (future) entries, not the stale ones.
	for addr, deadline := range bm.pendingReplicationDeadlines[blockID] {
		if !deadline.After(time.Now()) {
			t.Errorf("pending deadline for %s should be in the future after re-scheduling, got %v", addr, deadline)
		}
	}
}

func TestPendingReplicationTTL_RemoveBlockClearsDeadlines(t *testing.T) {
	// RemoveBlock should also drop the deadlines map entry; otherwise deleted
	// blocks would leak into bm.pendingReplicationDeadlines indefinitely.
	bm := makeManager(3, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)

	blockID := "blk_remove_test"
	bm.mu.Lock()
	bm.blocks[blockID] = &block.Metadata{BlockID: block.ID(blockID), Locations: []string{"host1:9001"}}
	bm.pendingReplicationDeadlines[blockID] = map[string]time.Time{
		"host2:9001": time.Now().Add(pendingReplicationTTL),
	}
	bm.mu.Unlock()

	bm.RemoveBlock(blockID)

	bm.mu.RLock()
	defer bm.mu.RUnlock()
	if _, exists := bm.pendingReplicationDeadlines[blockID]; exists {
		t.Error("RemoveBlock must also clear pendingReplicationDeadlines to avoid a slow leak")
	}
}

func TestRestore_PreservesLocationsThroughFirstCheckCycle(t *testing.T) {
	// Regression test for v2 F5 4/5 failure ("no locations for block X" after
	// NN restart). Restore() must give DNs a grace period as Alive so that
	// CheckAndReplicateBlocks, which runs every heartbeat tick, does NOT strip
	// block locations before DNs have had a chance to re-register. If this
	// invariant breaks, every block loses its Locations immediately after
	// restart and reads fail with "no locations for block".
	bm := makeManager(3, 10*time.Second)
	blockID := "blk_restore_test"
	addrs := []string{"host1:9001", "host2:9001", "host3:9001"}
	meta := block.NewMetadata(3)
	meta.BlockID = block.ID(blockID)
	meta.Locations = append([]string{}, addrs...)

	blocks := map[string]*block.Metadata{blockID: &meta}
	datanodes := map[string]*DataNodeInfo{
		"n1": {NodeID: "n1", Address: "host1:9001", CapacityBytes: 100 << 30, Alive: true},
		"n2": {NodeID: "n2", Address: "host2:9001", CapacityBytes: 100 << 30, Alive: true},
		"n3": {NodeID: "n3", Address: "host3:9001", CapacityBytes: 100 << 30, Alive: true},
	}

	bm.Restore(blocks, datanodes)

	// Simulate the first heartbeatMonitor tick immediately after restart —
	// no DN has heartbeated yet, but DNs must still count as alive during
	// the grace period so locations survive this pass.
	bm.CheckAndReplicateBlocks()

	bm.mu.RLock()
	got := bm.blocks[blockID]
	bm.mu.RUnlock()
	if got == nil {
		t.Fatal("block disappeared from BlockManager after Restore")
	}
	if len(got.Locations) != 3 {
		t.Errorf("expected 3 Locations preserved through first replication check (grace period), got %d: %v",
			len(got.Locations), got.Locations)
	}
}

func TestDetectDeadNodes(t *testing.T) {
	bm := makeManager(1, 50*time.Millisecond)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	time.Sleep(100 * time.Millisecond)
	dead := bm.DetectDeadNodes()
	if len(dead) != 1 || dead[0] != "n1" {
		t.Errorf("expected n1 dead, got %v", dead)
	}
	// Second call should not re-detect already-dead nodes
	dead2 := bm.DetectDeadNodes()
	if len(dead2) != 0 {
		t.Errorf("expected no new dead nodes, got %v", dead2)
	}
}

func TestDetectDeadNodes_AliveNode(t *testing.T) {
	bm := makeManager(1, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	dead := bm.DetectDeadNodes()
	if len(dead) != 0 {
		t.Errorf("expected no dead nodes, got %v", dead)
	}
}

func TestProcessHeartbeat_UpdatesLiveness(t *testing.T) {
	bm := makeManager(1, 50*time.Millisecond)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	time.Sleep(30 * time.Millisecond)

	// Heartbeat keeps it alive
	cmds, err := bm.ProcessHeartbeat("n1", 10<<30, 90<<30, 5)
	if err != nil {
		t.Fatalf("ProcessHeartbeat: %v", err)
	}
	if len(cmds) != 0 {
		t.Errorf("expected no commands, got %d", len(cmds))
	}

	time.Sleep(30 * time.Millisecond)
	dead := bm.DetectDeadNodes()
	if len(dead) != 0 {
		t.Error("node should still be alive after heartbeat")
	}
}

func TestProcessHeartbeat_UnknownNode(t *testing.T) {
	bm := makeManager(1, 10*time.Second)
	_, err := bm.ProcessHeartbeat("unknown", 0, 0, 0)
	if err == nil {
		t.Fatal("expected error for unknown node heartbeat")
	}
}

func TestCheckAndReplicateBlocks(t *testing.T) {
	bm := makeManager(2, 10*time.Second)
	// Give n1 highest capacity so it's always selected for the block
	bm.RegisterDataNode("n1", "host1:9001", 200<<30)
	bm.RegisterDataNode("n2", "host2:9001", 150<<30)
	bm.RegisterDataNode("n3", "host3:9001", 100<<30)

	meta, pipeline, _ := bm.AllocateBlock(2, "client")
	bm.CompleteBlock(meta.BlockID.String(), 1024, []byte("checksum"))
	t.Logf("block allocated on pipeline: %v", pipeline)

	// Simulate n1 dying (n1 should always be in the pipeline due to highest capacity)
	bm.mu.Lock()
	bm.datanodes["n1"].Alive = false
	bm.mu.Unlock()

	bm.CheckAndReplicateBlocks()

	// Check ALL alive nodes for a pending REPLICATE command
	found := false
	for _, nodeID := range []string{"n2", "n3"} {
		cmds, _ := bm.ProcessHeartbeat(nodeID, 0, 100<<30, 0)
		for _, cmd := range cmds {
			if cmd.Type == "REPLICATE" && cmd.BlockID == meta.BlockID.String() {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected REPLICATE command for under-replicated block on some alive node")
	}
}

func TestCheckAndReplicateBlocks_NoDuplicateCommands(t *testing.T) {
	bm := makeManager(2, 10*time.Second)
	// Give n1 highest capacity so it's always in the pipeline
	bm.RegisterDataNode("n1", "host1:9001", 200<<30)
	bm.RegisterDataNode("n2", "host2:9001", 150<<30)
	bm.RegisterDataNode("n3", "host3:9001", 100<<30)

	meta, _, _ := bm.AllocateBlock(2, "client")
	bm.CompleteBlock(meta.BlockID.String(), 1024, []byte("cs"))

	bm.mu.Lock()
	bm.datanodes["n1"].Alive = false
	bm.mu.Unlock()

	// First cycle schedules replication
	bm.CheckAndReplicateBlocks()
	// Second cycle should NOT re-queue (target is in PendingLocations)
	bm.CheckAndReplicateBlocks()

	// Check all alive nodes for REPLICATE commands
	replicateCount := 0
	for _, nodeID := range []string{"n2", "n3"} {
		cmds, _ := bm.ProcessHeartbeat(nodeID, 0, 100<<30, 0)
		for _, cmd := range cmds {
			if cmd.Type == "REPLICATE" {
				replicateCount++
			}
		}
	}
	if replicateCount > 1 {
		t.Errorf("expected at most 1 REPLICATE command across all nodes, got %d (replication storm)", replicateCount)
	}
}

func TestProcessBlockReport_OrphanDeletion(t *testing.T) {
	bm := makeManager(1, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)

	reported := []struct {
		BlockID         string
		SizeBytes       int64
		GenerationStamp int64
	}{{"unknown-blk-123", 1024, 1}}

	toDelete := bm.ProcessBlockReport("n1", reported)
	if len(toDelete) != 1 || toDelete[0] != "unknown-blk-123" {
		t.Errorf("expected orphan deletion, got %v", toDelete)
	}
}

func TestProcessBlockReport_ConfirmsPendingLocation(t *testing.T) {
	bm := makeManager(2, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	bm.RegisterDataNode("n2", "host2:9001", 100<<30)

	meta, _, _ := bm.AllocateBlock(2, "client")
	blockID := meta.BlockID.String()
	bm.CompleteBlock(blockID, 1024, []byte("cs"))

	// Manually add a pending location
	bm.mu.Lock()
	bm.blocks[blockID].PendingLocations = []string{"host2:9001"}
	bm.mu.Unlock()

	// Block report from n2 confirms the replica
	reported := []struct {
		BlockID         string
		SizeBytes       int64
		GenerationStamp int64
	}{{blockID, 1024, 1}}
	bm.ProcessBlockReport("n2", reported)

	bm.mu.RLock()
	pending := bm.blocks[blockID].PendingLocations
	bm.mu.RUnlock()

	if len(pending) != 0 {
		t.Errorf("expected PendingLocations cleared after block report, got %v", pending)
	}
}

func TestProcessBlockReport_UnknownDataNode(t *testing.T) {
	bm := makeManager(1, 10*time.Second)
	// No DataNodes registered — should not panic
	reported := []struct {
		BlockID         string
		SizeBytes       int64
		GenerationStamp int64
	}{{"blk_1", 1024, 1}}
	toDelete := bm.ProcessBlockReport("unknown-node", reported)
	// Should return early without panicking
	_ = toDelete
}

func TestRemoveBlock(t *testing.T) {
	bm := makeManager(1, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)

	meta, _, _ := bm.AllocateBlock(1, "client")
	blockID := meta.BlockID.String()
	bm.CompleteBlock(blockID, 1024, []byte("cs"))

	bm.RemoveBlock(blockID)

	locs := bm.GetBlockLocations([]string{blockID})
	if len(locs) != 0 {
		t.Error("expected block to be removed")
	}
}

func TestGetAliveDataNodes(t *testing.T) {
	bm := makeManager(1, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	bm.RegisterDataNode("n2", "host2:9001", 100<<30)

	addrs := bm.GetAliveDataNodes()
	if len(addrs) != 2 {
		t.Errorf("expected 2 alive DataNodes, got %d", len(addrs))
	}
}
