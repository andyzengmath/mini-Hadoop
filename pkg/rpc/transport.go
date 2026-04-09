package rpc

import "io"

// BlockTransport abstracts the mechanism for transferring block data between
// nodes. The initial implementation uses gRPC streaming with 1MB chunks.
// This interface allows a future swap to raw TCP without changing callers.
//
// See ADR in .omc/plans/mini-hadoop-consensus-plan.md for rationale.
type BlockTransport interface {
	// SendBlock streams block data to a target node, optionally forwarding
	// through a pipeline of additional nodes.
	// pipeline contains addresses of downstream nodes (may be empty for reads).
	SendBlock(blockID string, data io.Reader, dataSize int64, pipeline []string) error

	// ReceiveBlock reads block data from a source node.
	// Returns an io.ReadCloser that must be closed after reading.
	ReceiveBlock(blockID string, sourceAddr string) (io.ReadCloser, int64, error)

	// Close releases any resources held by the transport.
	Close() error
}

// ChunkSize is the size of each data chunk sent over gRPC streaming (1 MB).
const ChunkSize = 1 * 1024 * 1024
