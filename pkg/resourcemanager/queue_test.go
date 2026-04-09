package resourcemanager

import (
	"testing"
)

func TestQueueManager_Default(t *testing.T) {
	qm := NewQueueManager(nil)
	q, err := qm.GetQueue("")
	if err != nil {
		t.Fatalf("GetQueue default: %v", err)
	}
	if q.Name != "default" {
		t.Errorf("expected 'default', got %q", q.Name)
	}
	if q.MinCapacityPct != 1.0 {
		t.Errorf("expected 100%% capacity, got %f", q.MinCapacityPct)
	}
}

func TestQueueManager_MultiQueue(t *testing.T) {
	configs := []QueueConfig{
		{Name: "production", MinCapacityPct: 0.6, MaxCapacityPct: 1.0},
		{Name: "development", MinCapacityPct: 0.3, MaxCapacityPct: 0.5},
		{Name: "testing", MinCapacityPct: 0.1, MaxCapacityPct: 0.3},
	}
	qm := NewQueueManager(configs)

	queues := qm.ListQueues()
	if len(queues) != 3 {
		t.Fatalf("expected 3 queues, got %d", len(queues))
	}

	prod, err := qm.GetQueue("production")
	if err != nil {
		t.Fatalf("GetQueue production: %v", err)
	}
	if prod.MinCapacityPct != 0.6 {
		t.Errorf("production min capacity: expected 0.6, got %f", prod.MinCapacityPct)
	}
}

func TestQueueManager_CanAllocate(t *testing.T) {
	configs := []QueueConfig{
		{Name: "small", MinCapacityPct: 0.1, MaxCapacityPct: 0.2},
	}
	qm := NewQueueManager(configs)

	// Cluster has 10000 MB total. Queue max is 20% = 2000 MB.
	if !qm.CanAllocate("small", 1000, 1, 10000, 10) {
		t.Error("should be able to allocate 1000 MB within 20% of 10000")
	}

	// Allocate 1500 MB
	qm.Allocate("small", 1500, 1)

	// Now 1500 + 600 = 2100 > 2000 max
	if qm.CanAllocate("small", 600, 1, 10000, 10) {
		t.Error("should NOT be able to allocate beyond max capacity")
	}

	// But 400 is ok: 1500 + 400 = 1900 < 2000
	if !qm.CanAllocate("small", 400, 1, 10000, 10) {
		t.Error("should be able to allocate within max capacity")
	}
}

func TestQueueManager_AllocateRelease(t *testing.T) {
	qm := NewQueueManager(nil)
	qm.Allocate("default", 512, 2)

	q, _ := qm.GetQueue("default")
	if q.UsedMemoryMB != 512 {
		t.Errorf("expected 512 used, got %d", q.UsedMemoryMB)
	}

	qm.Release("default", 512, 2)
	if q.UsedMemoryMB != 0 {
		t.Errorf("expected 0 used after release, got %d", q.UsedMemoryMB)
	}
}

func TestQueueManager_ReleaseNoNegative(t *testing.T) {
	qm := NewQueueManager(nil)
	qm.Release("default", 100, 1)

	q, _ := qm.GetQueue("default")
	if q.UsedMemoryMB < 0 {
		t.Error("used memory went negative")
	}
}

func TestQueueManager_UnknownQueue(t *testing.T) {
	qm := NewQueueManager(nil)
	_, err := qm.GetQueue("nonexistent")
	if err == nil {
		t.Error("expected error for unknown queue")
	}
}
