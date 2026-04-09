package hdfs

import (
	"testing"
)

func TestAckQueue_EnqueueDequeue(t *testing.T) {
	aq := NewAckQueue()

	aq.Enqueue([]byte("chunk0"), 0, false)
	aq.Enqueue([]byte("chunk1"), 1024, false)
	aq.Enqueue([]byte("chunk2"), 2048, true)

	if aq.DataCount() != 3 {
		t.Fatalf("expected 3 data packets, got %d", aq.DataCount())
	}

	p0 := aq.Dequeue()
	if p0 == nil || p0.SeqNo != 0 {
		t.Fatalf("expected seq 0, got %v", p0)
	}
	if aq.DataCount() != 2 {
		t.Errorf("expected 2 data, got %d", aq.DataCount())
	}
	if aq.PendingCount() != 1 {
		t.Errorf("expected 1 pending, got %d", aq.PendingCount())
	}

	p1 := aq.Dequeue()
	p2 := aq.Dequeue()
	if p1.SeqNo != 1 || p2.SeqNo != 2 {
		t.Errorf("expected seq 1,2 got %d,%d", p1.SeqNo, p2.SeqNo)
	}
	if !p2.IsLast {
		t.Error("expected last packet")
	}

	if aq.Dequeue() != nil {
		t.Error("expected nil from empty data queue")
	}
}

func TestAckQueue_Acknowledge(t *testing.T) {
	aq := NewAckQueue()
	aq.Enqueue([]byte("a"), 0, false)
	aq.Enqueue([]byte("b"), 1, false)
	aq.Dequeue() // seq 0 → pending
	aq.Dequeue() // seq 1 → pending

	if !aq.Acknowledge(0) {
		t.Error("expected ack of seq 0 to succeed")
	}
	if aq.PendingCount() != 1 {
		t.Errorf("expected 1 pending after ack, got %d", aq.PendingCount())
	}

	if aq.Acknowledge(99) {
		t.Error("expected ack of unknown seq to fail")
	}
}

func TestAckQueue_DrainToDataQueue(t *testing.T) {
	aq := NewAckQueue()
	aq.Enqueue([]byte("c0"), 0, false)
	aq.Enqueue([]byte("c1"), 1024, false)
	aq.Enqueue([]byte("c2"), 2048, true)

	// Send first two
	aq.Dequeue() // seq 0 → pending
	aq.Dequeue() // seq 1 → pending

	// Simulate pipeline failure — drain pending back to data
	drained := aq.DrainToDataQueue()
	if drained != 2 {
		t.Fatalf("expected 2 drained, got %d", drained)
	}

	// Data queue should now have: [seq0, seq1, seq2] (pending first, then remaining)
	if aq.DataCount() != 3 {
		t.Errorf("expected 3 data after drain, got %d", aq.DataCount())
	}
	if aq.PendingCount() != 0 {
		t.Errorf("expected 0 pending after drain, got %d", aq.PendingCount())
	}

	// First dequeue should be the previously-pending seq 0
	p := aq.Dequeue()
	if p.SeqNo != 0 {
		t.Errorf("expected seq 0 first after drain, got %d", p.SeqNo)
	}
}

func TestAckQueue_IsEmpty(t *testing.T) {
	aq := NewAckQueue()
	if !aq.IsEmpty() {
		t.Error("new queue should be empty")
	}

	aq.Enqueue([]byte("x"), 0, true)
	if aq.IsEmpty() {
		t.Error("queue with data should not be empty")
	}

	aq.Dequeue()
	if aq.IsEmpty() {
		t.Error("queue with pending should not be empty")
	}

	aq.Acknowledge(0)
	if !aq.IsEmpty() {
		t.Error("fully acked queue should be empty")
	}
}

func TestAckQueue_Reset(t *testing.T) {
	aq := NewAckQueue()
	aq.Enqueue([]byte("a"), 0, false)
	aq.Dequeue()
	aq.Enqueue([]byte("b"), 1, false)

	aq.Reset()
	if !aq.IsEmpty() {
		t.Error("expected empty after reset")
	}
}

func TestAckQueue_DrainEmpty(t *testing.T) {
	aq := NewAckQueue()
	drained := aq.DrainToDataQueue()
	if drained != 0 {
		t.Errorf("drain on empty should return 0, got %d", drained)
	}
}
