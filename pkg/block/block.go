package block

import (
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/google/uuid"
)

const (
	ChecksumSize = sha256.Size // 32 bytes (SHA-256)
)

// ID uniquely identifies a block.
type ID string

// NewID generates a new unique block ID.
func NewID() ID {
	return ID("blk_" + uuid.New().String())
}

// String returns the block ID as a string.
func (id ID) String() string {
	return string(id)
}

// Metadata holds block-level metadata tracked by the NameNode.
type Metadata struct {
	BlockID           ID       `json:"block_id"`
	SizeBytes         int64    `json:"size_bytes"`
	Checksum          []byte   `json:"checksum"`
	Locations         []string `json:"locations"`          // DataNode addresses with confirmed replicas
	PendingLocations  []string `json:"pending_locations"`  // Targets with in-flight replication (not yet confirmed)
	GenerationStamp   int64    `json:"generation_stamp"`   // For orphaning partial blocks
	ReplicationTarget int32    `json:"replication_target"`
}

// NewMetadata creates a new block metadata with a fresh ID.
func NewMetadata(replicationTarget int32) Metadata {
	return Metadata{
		BlockID:           NewID(),
		GenerationStamp:   1,
		ReplicationTarget: replicationTarget,
	}
}

// IsUnderReplicated returns true if current + pending replicas are below target.
func (m Metadata) IsUnderReplicated() bool {
	return int32(len(m.Locations)+len(m.PendingLocations)) < m.ReplicationTarget
}

// ReplicaDeficit returns how many more replicas are needed (accounting for pending).
func (m Metadata) ReplicaDeficit() int32 {
	total := int32(len(m.Locations) + len(m.PendingLocations))
	deficit := m.ReplicationTarget - total
	if deficit < 0 {
		return 0
	}
	return deficit
}

// ClearPendingLocation removes an address from PendingLocations (called when
// a block report confirms the replica exists, or when replication is known to have failed).
func (m *Metadata) ClearPendingLocation(addr string) {
	filtered := m.PendingLocations[:0]
	for _, loc := range m.PendingLocations {
		if loc != addr {
			filtered = append(filtered, loc)
		}
	}
	m.PendingLocations = filtered
}

// ComputeChecksum computes the SHA-256 checksum of the given data.
func ComputeChecksum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

// ComputeChecksumFromReader computes SHA-256 from an io.Reader.
func ComputeChecksumFromReader(r io.Reader) ([]byte, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return nil, fmt.Errorf("computing checksum: %w", err)
	}
	return h.Sum(nil), nil
}

// VerifyChecksum checks whether the data matches the expected checksum.
func VerifyChecksum(data []byte, expected []byte) bool {
	actual := ComputeChecksum(data)
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}
