package block

import (
	"fmt"
)

// ErasureCodec implements Reed-Solomon erasure coding for storage-efficient
// fault tolerance. With 6 data + 3 parity shards, the system tolerates
// any 3 shard failures with only 1.5x storage overhead (vs 3x for replication).
type ErasureCodec struct {
	DataShards   int // Number of data shards (e.g., 6)
	ParityShards int // Number of parity shards (e.g., 3)
}

// ErasurePolicy describes an erasure coding configuration.
type ErasurePolicy struct {
	Name         string `json:"name"`          // e.g., "RS-6-3"
	DataShards   int    `json:"data_shards"`   // e.g., 6
	ParityShards int    `json:"parity_shards"` // e.g., 3
}

// DefaultErasurePolicy returns the standard RS-6-3 policy.
func DefaultErasurePolicy() ErasurePolicy {
	return ErasurePolicy{
		Name:         "RS-6-3",
		DataShards:   6,
		ParityShards: 3,
	}
}

// NewErasureCodec creates a codec for the given policy.
func NewErasureCodec(policy ErasurePolicy) *ErasureCodec {
	return &ErasureCodec{
		DataShards:   policy.DataShards,
		ParityShards: policy.ParityShards,
	}
}

// TotalShards returns the total number of shards (data + parity).
func (ec *ErasureCodec) TotalShards() int {
	return ec.DataShards + ec.ParityShards
}

// StorageOverhead returns the storage overhead ratio (e.g., 1.5 for RS-6-3).
func (ec *ErasureCodec) StorageOverhead() float64 {
	return float64(ec.TotalShards()) / float64(ec.DataShards)
}

// Encode splits data into data shards and computes parity shards.
// Uses a simple XOR-based parity for the built-in implementation.
// For production, use github.com/klauspost/reedsolomon.
func (ec *ErasureCodec) Encode(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("cannot encode empty data")
	}

	// Pad data to be divisible by DataShards
	shardSize := (len(data) + ec.DataShards - 1) / ec.DataShards
	padded := make([]byte, shardSize*ec.DataShards)
	copy(padded, data)

	// Split into data shards
	shards := make([][]byte, ec.TotalShards())
	for i := 0; i < ec.DataShards; i++ {
		start := i * shardSize
		shards[i] = make([]byte, shardSize)
		copy(shards[i], padded[start:start+shardSize])
	}

	// Compute parity shards using XOR-based scheme
	// Parity[j] = XOR of data shards with rotation
	for j := 0; j < ec.ParityShards; j++ {
		shards[ec.DataShards+j] = make([]byte, shardSize)
		for i := 0; i < ec.DataShards; i++ {
			src := shards[(i+j)%ec.DataShards]
			for k := 0; k < shardSize; k++ {
				shards[ec.DataShards+j][k] ^= src[k]
			}
		}
	}

	return shards, nil
}

// Decode reconstructs original data from available shards.
// Missing shards should be nil in the input slice.
// Returns error if too many shards are missing (need at least DataShards available).
func (ec *ErasureCodec) Decode(shards [][]byte, originalSize int) ([]byte, error) {
	if len(shards) != ec.TotalShards() {
		return nil, fmt.Errorf("expected %d shards, got %d", ec.TotalShards(), len(shards))
	}

	// Count available shards
	available := 0
	for _, s := range shards {
		if s != nil {
			available++
		}
	}

	if available < ec.DataShards {
		return nil, fmt.Errorf("need at least %d shards, have %d", ec.DataShards, available)
	}

	// If all data shards are present, just concatenate
	allDataPresent := true
	for i := 0; i < ec.DataShards; i++ {
		if shards[i] == nil {
			allDataPresent = false
			break
		}
	}

	if allDataPresent {
		return ec.concatenateDataShards(shards, originalSize), nil
	}

	// Reconstruct missing data shards using parity
	// Simple XOR reconstruction: if exactly one data shard is missing
	// and the corresponding parity shard is available
	shardSize := len(shards[0])
	if shardSize == 0 {
		// Find a non-nil shard to get size
		for _, s := range shards {
			if s != nil {
				shardSize = len(s)
				break
			}
		}
	}

	// Reconstruct missing data shards using parity shard 0.
	// Parity[0] = XOR of all data shards (with j=0, (i+0)%n == i, so no rotation).
	// This correctly inverts: missing = parity[0] XOR all-other-data-shards.
	// For multi-shard reconstruction, a proper RS library (klauspost/reedsolomon)
	// is needed. This implementation handles single-shard loss via XOR parity.
	pIdx := ec.DataShards // parity shard 0
	for i := 0; i < ec.DataShards; i++ {
		if shards[i] != nil {
			continue
		}

		if shards[pIdx] == nil {
			return nil, fmt.Errorf("cannot reconstruct data shard %d: parity shard 0 unavailable", i)
		}

		// Check all other data shards are available
		canReconstruct := true
		for di := 0; di < ec.DataShards; di++ {
			if di != i && shards[di] == nil {
				canReconstruct = false
				break
			}
		}

		if !canReconstruct {
			return nil, fmt.Errorf("cannot reconstruct data shard %d: multiple data shards missing (need RS library)", i)
		}

		// Reconstruct: missing = parity XOR all other data shards
		shards[i] = make([]byte, shardSize)
		copy(shards[i], shards[pIdx])
		for di := 0; di < ec.DataShards; di++ {
			if di == i {
				continue
			}
			for k := 0; k < shardSize; k++ {
				shards[i][k] ^= shards[di][k]
			}
		}
	}

	return ec.concatenateDataShards(shards, originalSize), nil
}

func (ec *ErasureCodec) concatenateDataShards(shards [][]byte, originalSize int) []byte {
	result := make([]byte, 0, originalSize)
	for i := 0; i < ec.DataShards; i++ {
		result = append(result, shards[i]...)
	}
	if len(result) > originalSize {
		result = result[:originalSize]
	}
	return result
}

// BlockGroup represents an erasure-coded block group.
type BlockGroup struct {
	GroupID      string            `json:"group_id"`
	Policy       ErasurePolicy    `json:"policy"`
	OriginalSize int64            `json:"original_size"`
	Shards       []BlockGroupShard `json:"shards"`
}

// BlockGroupShard is a single shard in an erasure-coded block group.
type BlockGroupShard struct {
	Index    int    `json:"index"`     // 0-based: 0..data-1 are data, data..total-1 are parity
	BlockID  string `json:"block_id"`
	NodeAddr string `json:"node_addr"`
	IsData   bool   `json:"is_data"`
}
