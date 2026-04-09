package hdfs

import (
	"sync"
)

// Packet represents a chunk of block data in the pipeline.
type Packet struct {
	SeqNo  int
	Data   []byte
	Offset int64
	IsLast bool
}

// AckQueue implements the dual-queue pattern for pipeline write reliability.
// Packets flow: data queue → send → ack queue → acknowledged → removed.
// On pipeline failure: ack queue drains back to data queue for retry.
type AckQueue struct {
	mu      sync.Mutex
	data    []Packet // Waiting to be sent
	pending []Packet // Sent, awaiting acknowledgment from all downstream nodes
	nextSeq int
}

// NewAckQueue creates a new ack queue.
func NewAckQueue() *AckQueue {
	return &AckQueue{}
}

// Enqueue adds a packet to the data queue for sending.
func (aq *AckQueue) Enqueue(data []byte, offset int64, isLast bool) Packet {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	p := Packet{
		SeqNo:  aq.nextSeq,
		Data:   data,
		Offset: offset,
		IsLast: isLast,
	}
	aq.nextSeq++
	aq.data = append(aq.data, p)
	return p
}

// Dequeue removes and returns the next packet from the data queue for sending.
// Moves the packet to the pending (ack) queue.
// Returns nil if no packets are available.
func (aq *AckQueue) Dequeue() *Packet {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	if len(aq.data) == 0 {
		return nil
	}

	p := aq.data[0]
	aq.data = aq.data[1:]
	aq.pending = append(aq.pending, p)
	return &p
}

// Acknowledge removes a packet from the pending queue (confirmed by all downstream).
func (aq *AckQueue) Acknowledge(seqNo int) bool {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	for i, p := range aq.pending {
		if p.SeqNo == seqNo {
			aq.pending = append(aq.pending[:i], aq.pending[i+1:]...)
			return true
		}
	}
	return false
}

// DrainToDataQueue moves all pending (unacknowledged) packets back to the
// front of the data queue. Called on pipeline failure to retry unacked packets.
func (aq *AckQueue) DrainToDataQueue() int {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	count := len(aq.pending)
	if count == 0 {
		return 0
	}

	// Prepend pending packets to data queue (they need to be sent first)
	aq.data = append(aq.pending, aq.data...)
	aq.pending = nil
	return count
}

// PendingCount returns the number of unacknowledged packets.
func (aq *AckQueue) PendingCount() int {
	aq.mu.Lock()
	defer aq.mu.Unlock()
	return len(aq.pending)
}

// DataCount returns the number of packets waiting to be sent.
func (aq *AckQueue) DataCount() int {
	aq.mu.Lock()
	defer aq.mu.Unlock()
	return len(aq.data)
}

// IsEmpty returns true if both queues are empty.
func (aq *AckQueue) IsEmpty() bool {
	aq.mu.Lock()
	defer aq.mu.Unlock()
	return len(aq.data) == 0 && len(aq.pending) == 0
}

// Reset clears both queues.
func (aq *AckQueue) Reset() {
	aq.mu.Lock()
	defer aq.mu.Unlock()
	aq.data = nil
	aq.pending = nil
}
