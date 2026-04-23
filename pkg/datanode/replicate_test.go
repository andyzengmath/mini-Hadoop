package datanode

import (
	"testing"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
)

// TestReplicateSemaphore_IsBounded verifies that a new Server has a replicate
// semaphore sized to exactly maxConcurrentReplications. Without this, a
// NameNode under-replication storm can spawn an unbounded number of concurrent
// replication goroutines, each holding an open gRPC stream + file write —
// observed pushing worker RSS to 20 GiB in the v2 D1 benchmark.
func TestReplicateSemaphore_IsBounded(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.DataDir = t.TempDir()

	s, err := NewServer("n1", "host1:9001", cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	if s.replicateSem == nil {
		t.Fatal("replicateSem must be non-nil so the semaphore guard in executeCommand works")
	}
	if cap(s.replicateSem) != maxConcurrentReplications {
		t.Errorf("replicateSem capacity = %d, want %d (any other value either over-admits work and risks OOM, or under-admits and makes replication a bottleneck)",
			cap(s.replicateSem), maxConcurrentReplications)
	}

	// Draining the semaphore up to cap must succeed immediately (non-blocking).
	for i := 0; i < cap(s.replicateSem); i++ {
		select {
		case s.replicateSem <- struct{}{}:
		default:
			t.Fatalf("semaphore filled to %d/%d but blocked — cap not respected", i, cap(s.replicateSem))
		}
	}
	// One more must not fit (semaphore is at capacity).
	select {
	case s.replicateSem <- struct{}{}:
		t.Fatalf("semaphore accepted %d items, expected cap of %d", cap(s.replicateSem)+1, cap(s.replicateSem))
	default:
		// expected — semaphore full
	}
}
