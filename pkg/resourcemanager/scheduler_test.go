package resourcemanager

import (
	"testing"
	"time"
)

func setupScheduler() *FIFOScheduler {
	s := NewFIFOScheduler(10 * time.Second)
	s.RegisterNode("n1", "host1:9011", 4096, 4)
	return s
}

func TestSubmitApp_NoNodes_Fails(t *testing.T) {
	s := NewFIFOScheduler(10 * time.Second)
	_, err := s.SubmitApp("mapreduce", "/bin/echo", nil, 512, 1, nil)
	if err == nil {
		t.Fatal("expected error submitting app with no nodes")
	}
}

func TestSubmitApp_AllocatesAMContainer(t *testing.T) {
	s := setupScheduler()
	appID, err := s.SubmitApp("mapreduce", "/bin/echo", nil, 512, 1, nil)
	if err != nil {
		t.Fatalf("SubmitApp: %v", err)
	}
	info, err := s.GetAppInfo(appID)
	if err != nil {
		t.Fatalf("GetAppInfo: %v", err)
	}
	if info.State != "RUNNING" {
		t.Errorf("expected RUNNING, got %s", info.State)
	}
	if info.AMContainerID == "" {
		t.Error("expected AMContainerID to be set")
	}
}

func TestSubmitApp_ResourceAccounting(t *testing.T) {
	s := setupScheduler()
	s.SubmitApp("mr", "/bin/echo", nil, 512, 1, nil)

	s.mu.Lock()
	node := s.nodes["n1"]
	usedMem := node.UsedMemoryMB
	usedCPU := node.UsedCPU
	s.mu.Unlock()

	if usedMem != 512 {
		t.Errorf("expected 512MB used, got %d", usedMem)
	}
	if usedCPU != 1 {
		t.Errorf("expected 1 CPU used, got %d", usedCPU)
	}
}

func TestAllocateContainers_WithLocality(t *testing.T) {
	s := NewFIFOScheduler(10 * time.Second)
	s.RegisterNode("n1", "host1:9011", 4096, 4)
	s.RegisterNode("n2", "host2:9011", 4096, 4)

	appID, _ := s.SubmitApp("mr", "/bin/echo", nil, 256, 1, nil)

	// Request container with locality preference for n2
	containers := s.AllocateContainers(appID, []PendingRequest{{
		AppID:          appID,
		MemoryMB:       256,
		CPUVCores:      1,
		PreferredNodes: []string{"host2:9011"},
		Relaxed:        true,
		Count:          1,
	}})

	if len(containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(containers))
	}
	// Should be allocated on n2 (preferred)
	if containers[0].NodeID != "n2" {
		t.Errorf("expected allocation on n2, got %s", containers[0].NodeID)
	}
}

func TestUpdateContainerStatus_AMFailed_AppFails(t *testing.T) {
	s := setupScheduler()
	appID, _ := s.SubmitApp("mr", "/bin/echo", nil, 256, 1, nil)

	info, _ := s.GetAppInfo(appID)
	s.UpdateContainerStatus(info.AMContainerID, "n1", "FAILED", 1, "OOM")

	updated, _ := s.GetAppInfo(appID)
	if updated.State != "FAILED" {
		t.Errorf("expected FAILED after AM death, got %s", updated.State)
	}
}

func TestUpdateContainerStatus_NoDoubleRelease(t *testing.T) {
	s := setupScheduler()
	appID, _ := s.SubmitApp("mr", "/bin/echo", nil, 512, 1, nil)
	info, _ := s.GetAppInfo(appID)

	// First completion — resources released
	s.UpdateContainerStatus(info.AMContainerID, "n1", "COMPLETED", 0, "")

	s.mu.Lock()
	afterFirst := s.nodes["n1"].UsedMemoryMB
	s.mu.Unlock()

	// Second identical status report — should NOT release again
	s.UpdateContainerStatus(info.AMContainerID, "n1", "COMPLETED", 0, "")

	s.mu.Lock()
	afterSecond := s.nodes["n1"].UsedMemoryMB
	s.mu.Unlock()

	if afterFirst != afterSecond {
		t.Errorf("double-free: memory after first=%d, after second=%d", afterFirst, afterSecond)
	}
	if afterFirst < 0 {
		t.Errorf("memory went negative: %d", afterFirst)
	}
}

func TestDetectDeadNodes(t *testing.T) {
	s := NewFIFOScheduler(50 * time.Millisecond)
	s.RegisterNode("n1", "host1:9011", 4096, 4)
	time.Sleep(100 * time.Millisecond)

	dead := s.DetectDeadNodes()
	if len(dead) != 1 || dead[0] != "n1" {
		t.Errorf("expected n1 dead, got %v", dead)
	}
}

func TestReleaseContainers(t *testing.T) {
	s := setupScheduler()
	appID, _ := s.SubmitApp("mr", "/bin/echo", nil, 512, 1, nil)
	info, _ := s.GetAppInfo(appID)

	s.mu.Lock()
	beforeMem := s.nodes["n1"].UsedMemoryMB
	s.mu.Unlock()

	s.ReleaseContainers(appID, []string{info.AMContainerID})

	s.mu.Lock()
	afterMem := s.nodes["n1"].UsedMemoryMB
	s.mu.Unlock()

	if afterMem >= beforeMem {
		t.Errorf("expected memory freed after release: before=%d, after=%d", beforeMem, afterMem)
	}
}

func TestGetAppInfo_Unknown(t *testing.T) {
	s := setupScheduler()
	_, err := s.GetAppInfo("nonexistent")
	if err == nil {
		t.Fatal("expected error for unknown app")
	}
}

func TestCompleteApp(t *testing.T) {
	s := setupScheduler()
	appID, _ := s.SubmitApp("mr", "/bin/echo", nil, 256, 1, nil)
	s.CompleteApp(appID, "FINISHED", "done")

	info, _ := s.GetAppInfo(appID)
	if info.State != "FINISHED" {
		t.Errorf("expected FINISHED, got %s", info.State)
	}
}
