package block

import (
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/google/uuid"
)

const (
	DefaultBlockSize = 128 * 1024 * 1024 // 128 MB
	ChecksumSize     = sha256.Size        // 32 bytes
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
	BlockID         ID       `json:"block_id"`
	SizeBytes       int64    `json:"size_bytes"`
	Checksum        []byte   `json:"checksum"`
	Locations       []string `json:"locations"`       // DataNode addresses
	GenerationStamp int64    `json:"generation_stamp"` // For orphaning partial blocks
	ReplicationTarget int32  `json:"replication_target"`
}

// NewMetadata creates a new block metadata with a fresh ID.
func NewMetadata(replicationTarget int32) Metadata {
	return Metadata{
		BlockID:           NewID(),
		GenerationStamp:   1,
		ReplicationTarget: replicationTarget,
	}
}

// IsUnderReplicated returns true if current replica count is below target.
func (m Metadata) IsUnderReplicated() bool {
	return int32(len(m.Locations)) < m.ReplicationTarget
}

// ReplicaDeficit returns how many more replicas are needed.
func (m Metadata) ReplicaDeficit() int32 {
	deficit := m.ReplicationTarget - int32(len(m.Locations))
	if deficit < 0 {
		return 0
	}
	return deficit
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
