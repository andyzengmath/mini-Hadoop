package block

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewID(t *testing.T) {
	id1 := NewID()
	id2 := NewID()

	if id1 == id2 {
		t.Error("NewID generated duplicate IDs")
	}
	if !strings.HasPrefix(string(id1), "blk_") {
		t.Errorf("ID %q does not have 'blk_' prefix", id1)
	}
	if len(string(id1)) < 10 {
		t.Errorf("ID %q is too short", id1)
	}
}

func TestIDString(t *testing.T) {
	id := ID("blk_test-123")
	if id.String() != "blk_test-123" {
		t.Errorf("String() = %q, want %q", id.String(), "blk_test-123")
	}
}

func TestNewMetadata(t *testing.T) {
	m := NewMetadata(3)
	if m.BlockID == "" {
		t.Error("NewMetadata generated empty block ID")
	}
	if m.ReplicationTarget != 3 {
		t.Errorf("ReplicationTarget = %d, want 3", m.ReplicationTarget)
	}
	if m.GenerationStamp != 1 {
		t.Errorf("GenerationStamp = %d, want 1", m.GenerationStamp)
	}
}

func TestIsUnderReplicated(t *testing.T) {
	tests := []struct {
		name      string
		locations int
		target    int32
		want      bool
	}{
		{"under-replicated", 1, 3, true},
		{"exactly replicated", 3, 3, false},
		{"over-replicated", 4, 3, false},
		{"no replicas", 0, 3, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := Metadata{
				ReplicationTarget: tt.target,
				Locations:         make([]string, tt.locations),
			}
			if got := m.IsUnderReplicated(); got != tt.want {
				t.Errorf("IsUnderReplicated() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReplicaDeficit(t *testing.T) {
	tests := []struct {
		name      string
		locations int
		target    int32
		want      int32
	}{
		{"deficit of 2", 1, 3, 2},
		{"no deficit", 3, 3, 0},
		{"over-replicated", 5, 3, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := Metadata{
				ReplicationTarget: tt.target,
				Locations:         make([]string, tt.locations),
			}
			if got := m.ReplicaDeficit(); got != tt.want {
				t.Errorf("ReplicaDeficit() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestComputeChecksum(t *testing.T) {
	data := []byte("hello world")
	cs1 := ComputeChecksum(data)
	cs2 := ComputeChecksum(data)

	if !bytes.Equal(cs1, cs2) {
		t.Error("Same data produced different checksums")
	}

	if len(cs1) != ChecksumSize {
		t.Errorf("Checksum length = %d, want %d", len(cs1), ChecksumSize)
	}

	different := ComputeChecksum([]byte("different data"))
	if bytes.Equal(cs1, different) {
		t.Error("Different data produced same checksum")
	}
}

func TestComputeChecksumFromReader(t *testing.T) {
	data := []byte("hello world")
	expected := ComputeChecksum(data)

	reader := bytes.NewReader(data)
	got, err := ComputeChecksumFromReader(reader)
	if err != nil {
		t.Fatalf("ComputeChecksumFromReader error: %v", err)
	}

	if !bytes.Equal(got, expected) {
		t.Error("Reader checksum doesn't match direct checksum")
	}
}

func TestVerifyChecksum(t *testing.T) {
	data := []byte("test data for checksum")
	cs := ComputeChecksum(data)

	if !VerifyChecksum(data, cs) {
		t.Error("VerifyChecksum returned false for matching data")
	}

	if VerifyChecksum([]byte("tampered data"), cs) {
		t.Error("VerifyChecksum returned true for tampered data")
	}

	if VerifyChecksum(data, []byte{0, 1, 2}) {
		t.Error("VerifyChecksum returned true for wrong-length checksum")
	}
}
