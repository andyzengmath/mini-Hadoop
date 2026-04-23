package hdfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/mini-hadoop/mini-hadoop/pkg/block"
	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"

	"google.golang.org/grpc"
)

// Client provides HDFS file operations.
type Client struct {
	cfg          config.Config
	nnConn       *grpc.ClientConn
	nnClient     pb.NameNodeServiceClient
	localDataDir string // If set, enables short-circuit local reads
}

// NewClient creates a new HDFS client connected to the NameNode.
func NewClient(cfg config.Config) (*Client, error) {
	conn, err := rpc.DialWithRetry(cfg.NameNodeAddress(), 3)
	if err != nil {
		return nil, fmt.Errorf("connect to NameNode: %w", err)
	}

	return &Client{
		cfg:          cfg,
		nnConn:       conn,
		nnClient:     pb.NewNameNodeServiceClient(conn),
		localDataDir: cfg.DataDir, // Enables short-circuit local reads if co-located with DataNode
	}, nil
}

// Close releases resources.
func (c *Client) Close() error {
	if c.nnConn != nil {
		return c.nnConn.Close()
	}
	return nil
}

// MkDir creates a directory.
func (c *Client) MkDir(path string, createParents bool) error {
	resp, err := c.nnClient.MkDir(context.Background(), &pb.MkDirRequest{
		Path:          path,
		CreateParents: createParents,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("mkdir failed: %s", resp.Error)
	}
	return nil
}

// CreateFile writes data from a reader to the distributed filesystem.
func (c *Client) CreateFile(path string, data io.Reader, replication int32) error {
	// Step 1: Create file in namespace
	createResp, err := c.nnClient.CreateFile(context.Background(), &pb.CreateFileRequest{
		Path:              path,
		ReplicationFactor: replication,
	})
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	if !createResp.Success {
		return fmt.Errorf("create file: %s", createResp.Error)
	}

	// Step 2: Write blocks
	blockSize := c.cfg.BlockSize
	chunkSize := c.cfg.ChunkSize
	var blockIDs []string
	buf := make([]byte, chunkSize)

	for {
		// Read one block's worth of data into a byte slice so we can retry the
		// pipeline if a DN dies mid-stream. Prior code kept data in a
		// *bytes.Buffer that Read drains once — on a pipeline failure the
		// second attempt would have seen an empty buffer.
		blockBuf := &bytes.Buffer{}
		n, err := io.CopyN(blockBuf, data, blockSize)
		if n == 0 && err == io.EOF {
			break
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("read input data: %w", err)
		}
		blockBytes := blockBuf.Bytes()

		// Retry pipeline writes up to maxBlockWriteAttempts. Each attempt
		// freshly allocates a block + pipeline from the NN so a dead DN
		// (e.g. from v2 F7 scenario: worker killed mid-write) is excluded
		// from the next try via AllocateBlock picking only alive nodes.
		// With degraded allocation from PR #21, even a single surviving DN
		// is enough to complete the write; NN background re-replication
		// restores rep-factor.
		var committedBlockID string
		var lastErr error
		for attempt := 1; attempt <= maxBlockWriteAttempts; attempt++ {
			addResp, addErr := c.nnClient.AddBlock(context.Background(), &pb.AddBlockRequest{
				Path: path,
			})
			if addErr != nil {
				return fmt.Errorf("add block: %w", addErr)
			}
			if addResp.Error != "" {
				return fmt.Errorf("add block: %s", addResp.Error)
			}

			blockID := addResp.Block.BlockId
			pipeline := addResp.Pipeline
			if len(pipeline) == 0 {
				return fmt.Errorf("empty pipeline for block %s", blockID)
			}

			writeErr := c.writeBlockToPipeline(
				blockID, addResp.Block.GenerationStamp,
				bytes.NewReader(blockBytes), pipeline, buf,
			)
			if writeErr == nil {
				committedBlockID = blockID
				slog.Info("block written", "blockID", blockID, "size", n, "pipeline", pipeline, "attempt", attempt)
				break
			}
			lastErr = writeErr
			slog.Warn("pipeline write failed, retrying with new allocation",
				"blockID", blockID, "attempt", attempt, "max", maxBlockWriteAttempts, "error", writeErr,
			)
			// The previous blockID is now orphaned on the NN — tracked in the
			// block map but no longer referenced by any file's BlockIDs once
			// CompleteFile runs with our successful replacement. Orphan
			// cleanup is a separate concern; see the NN block manager.
		}
		if committedBlockID == "" {
			return fmt.Errorf("write block after %d attempts: %w", maxBlockWriteAttempts, lastErr)
		}
		blockIDs = append(blockIDs, committedBlockID)

		if err == io.EOF {
			break
		}
	}

	// Step 3: Complete file
	completeResp, err := c.nnClient.CompleteFile(context.Background(), &pb.CompleteFileRequest{
		Path:     path,
		BlockIds: blockIDs,
	})
	if err != nil {
		return fmt.Errorf("complete file: %w", err)
	}
	if !completeResp.Success {
		return fmt.Errorf("complete file: %s", completeResp.Error)
	}

	slog.Info("file created", "path", path, "blocks", len(blockIDs))
	return nil
}

// maxBlockWriteAttempts is the number of times a client will retry a block
// write through freshly allocated pipelines. 3 gives enough headroom that a
// single DN death mid-stream (the v2 F7 scenario) recovers with one retry and
// a second retry handles a pathological 2-DN race, without masking a truly
// stuck cluster (all DNs down) with long retry storms.
const maxBlockWriteAttempts = 3

// writeBlockToPipeline sends block data to the first DataNode in the pipeline.
// data is a *bytes.Reader rather than io.Reader so the caller can hand in a
// fresh reader for each pipeline retry (bytes.NewReader(blockBytes)) and we
// retain Len() for last-chunk detection without needing a separate size arg.
func (c *Client) writeBlockToPipeline(blockID string, genStamp int64, data *bytes.Reader, pipeline []string, chunkBuf []byte) error {
	conn, err := rpc.Dial(pipeline[0])
	if err != nil {
		return fmt.Errorf("connect to DataNode %s: %w", pipeline[0], err)
	}
	defer conn.Close()

	dnClient := pb.NewDataNodeServiceClient(conn)
	stream, err := dnClient.WriteBlock(context.Background())
	if err != nil {
		return fmt.Errorf("open write stream: %w", err)
	}

	// Send header
	if err := stream.Send(&pb.WriteBlockRequest{
		Payload: &pb.WriteBlockRequest_Header{
			Header: &pb.WriteBlockHeader{
				BlockId:         blockID,
				GenerationStamp: genStamp,
				Pipeline:        pipeline,
				PipelineIndex:   0,
			},
		},
	}); err != nil {
		return fmt.Errorf("send header: %w", err)
	}

	// Send data chunks
	var offset int64
	for {
		n, readErr := data.Read(chunkBuf)
		if n > 0 {
			isLast := readErr == io.EOF || (readErr == nil && data.Len() == 0)
			if sendErr := stream.Send(&pb.WriteBlockRequest{
				Payload: &pb.WriteBlockRequest_Chunk{
					Chunk: &pb.DataChunk{
						Data:   chunkBuf[:n],
						Offset: offset,
						IsLast: isLast,
					},
				},
			}); sendErr != nil {
				return fmt.Errorf("send chunk: %w", sendErr)
			}
			offset += int64(n)
			if isLast {
				break
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read data: %w", readErr)
		}
	}

	// Close and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("close stream: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("write failed: %s", resp.Error)
	}

	return nil
}

// ReadFile reads a file from the distributed filesystem.
func (c *Client) ReadFile(path string, writer io.Writer) error {
	// Get block locations
	locResp, err := c.nnClient.GetBlockLocations(context.Background(), &pb.GetBlockLocationsRequest{
		Path: path,
	})
	if err != nil {
		return fmt.Errorf("get block locations: %w", err)
	}
	if locResp.Error != "" {
		return fmt.Errorf("get block locations: %s", locResp.Error)
	}

	// Read each block from the nearest DataNode
	for _, blockInfo := range locResp.Blocks {
		if len(blockInfo.Locations) == 0 {
			return fmt.Errorf("no locations for block %s", blockInfo.BlockId)
		}

		if err := c.readBlockFromDataNode(blockInfo, writer); err != nil {
			return fmt.Errorf("read block %s: %w", blockInfo.BlockId, err)
		}
	}

	return nil
}

// readBlockFromDataNode reads a block from the first available DataNode.
// It buffers each attempt to prevent partial writes on failure, and verifies
// the checksum if available. Attempts short-circuit local read first.
func (c *Client) readBlockFromDataNode(blockInfo *pb.BlockInfo, writer io.Writer) error {
	// Short-circuit local read: if we're co-located with a DataNode,
	// read the block directly from local disk (bypasses network entirely).
	if c.localDataDir != "" {
		localPath := c.localDataDir + "/" + blockInfo.BlockId + ".blk"
		if data, err := os.ReadFile(localPath); err == nil {
			if len(blockInfo.Checksum) > 0 && !block.VerifyChecksum(data, blockInfo.Checksum) {
				slog.Warn("short-circuit read checksum mismatch, falling back to network",
					"blockID", blockInfo.BlockId)
			} else {
				if _, writeErr := writer.Write(data); writeErr != nil {
					return writeErr
				}
				slog.Debug("short-circuit local read", "blockID", blockInfo.BlockId)
				return nil
			}
		}
		// Local file not found — fall through to network read
	}

	var lastErr error

	for _, addr := range blockInfo.Locations {
		data, err := c.tryReadFromDataNode(addr, blockInfo.BlockId)
		if err != nil {
			lastErr = err
			continue
		}

		// Verify checksum if available
		if len(blockInfo.Checksum) > 0 {
			if !block.VerifyChecksum(data, blockInfo.Checksum) {
				lastErr = fmt.Errorf("checksum mismatch from %s", addr)
				continue
			}
		}

		// Write verified data to output
		if _, writeErr := writer.Write(data); writeErr != nil {
			return writeErr
		}
		return nil // Success
	}

	return fmt.Errorf("all DataNodes failed for block %s: %w", blockInfo.BlockId, lastErr)
}

// tryReadFromDataNode attempts to read a full block from a single DataNode.
// Returns the complete block data or an error. No partial writes occur.
func (c *Client) tryReadFromDataNode(addr, blockID string) ([]byte, error) {
	conn, err := rpc.Dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	dnClient := pb.NewDataNodeServiceClient(conn)
	stream, err := dnClient.ReadBlock(context.Background(), &pb.ReadBlockRequest{
		BlockId: blockID,
	})
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	for {
		chunk, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return nil, recvErr
		}

		buf.Write(chunk.Data)

		if chunk.IsLast {
			break
		}
	}

	return buf.Bytes(), nil
}

// ListDir lists directory contents.
func (c *Client) ListDir(path string) ([]*pb.FileInfo, error) {
	resp, err := c.nnClient.ListDirectory(context.Background(), &pb.ListDirectoryRequest{
		Path: path,
	})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}
	return resp.Entries, nil
}

// DeleteFile deletes a file or directory.
func (c *Client) DeleteFile(path string, recursive bool) error {
	resp, err := c.nnClient.DeleteFile(context.Background(), &pb.DeleteFileRequest{
		Path:      path,
		Recursive: recursive,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Error)
	}
	return nil
}

// GetFileInfo returns metadata about a file or directory.
func (c *Client) GetFileInfo(path string) (*pb.FileInfo, error) {
	resp, err := c.nnClient.GetFileInfo(context.Background(), &pb.GetFileInfoRequest{
		Path: path,
	})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}
	return resp.Info, nil
}
