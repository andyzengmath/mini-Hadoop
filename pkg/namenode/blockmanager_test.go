package namenode

import (
	"testing"
	"time"
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

func TestAllocateBlock_NotEnoughNodes(t *testing.T) {
	bm := makeManager(3, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	_, _, err := bm.AllocateBlock(3, "client")
	if err == nil {
		t.Fatal("expected error when fewer nodes than replication factor")
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
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	bm.RegisterDataNode("n2", "host2:9001", 100<<30)
	bm.RegisterDataNode("n3", "host3:9001", 100<<30)

	meta, _, _ := bm.AllocateBlock(2, "client")
	bm.CompleteBlock(meta.BlockID.String(), 1024, []byte("checksum"))

	// Simulate n1 dying
	bm.mu.Lock()
	bm.datanodes["n1"].Alive = false
	bm.mu.Unlock()

	bm.CheckAndReplicateBlocks()

	// n3 should have a pending REPLICATE command
	cmds, _ := bm.ProcessHeartbeat("n3", 0, 100<<30, 0)
	found := false
	for _, cmd := range cmds {
		if cmd.Type == "REPLICATE" && cmd.BlockID == meta.BlockID.String() {
			found = true
		}
	}
	if !found {
		t.Error("expected REPLICATE command for under-replicated block")
	}
}

func TestCheckAndReplicateBlocks_NoDuplicateCommands(t *testing.T) {
	bm := makeManager(2, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)
	bm.RegisterDataNode("n2", "host2:9001", 100<<30)
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

	cmds, _ := bm.ProcessHeartbeat("n3", 0, 100<<30, 0)
	replicateCount := 0
	for _, cmd := range cmds {
		if cmd.Type == "REPLICATE" {
			replicateCount++
		}
	}
	if replicateCount > 1 {
		t.Errorf("expected at most 1 REPLICATE command, got %d (replication storm)", replicateCount)
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
