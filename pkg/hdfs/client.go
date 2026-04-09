package hdfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/mini-hadoop/mini-hadoop/pkg/block"
	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"

	"google.golang.org/grpc"
)

// Client provides HDFS file operations.
type Client struct {
	cfg        config.Config
	nnConn     *grpc.ClientConn
	nnClient   pb.NameNodeServiceClient
}

// NewClient creates a new HDFS client connected to the NameNode.
func NewClient(cfg config.Config) (*Client, error) {
	conn, err := rpc.DialWithRetry(cfg.NameNodeAddress(), 3)
	if err != nil {
		return nil, fmt.Errorf("connect to NameNode: %w", err)
	}

	return &Client{
		cfg:      cfg,
		nnConn:   conn,
		nnClient: pb.NewNameNodeServiceClient(conn),
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
		// Read one block's worth of data
		blockBuf := &bytes.Buffer{}
		n, err := io.CopyN(blockBuf, data, blockSize)
		if n == 0 && err == io.EOF {
			break
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("read input data: %w", err)
		}

		// Allocate a block from the NameNode
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

		// Write block through pipeline (first DataNode in pipeline)
		if len(pipeline) == 0 {
			return fmt.Errorf("empty pipeline for block %s", blockID)
		}

		genStamp := addResp.Block.GenerationStamp
		if writeErr := c.writeBlockToPipeline(blockID, genStamp, blockBuf, pipeline, buf); writeErr != nil {
			return fmt.Errorf("write block %s: %w", blockID, writeErr)
		}

		blockIDs = append(blockIDs, blockID)
		slog.Info("block written", "blockID", blockID, "size", n, "pipeline", pipeline)

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

// writeBlockToPipeline sends block data to the first DataNode in the pipeline.
func (c *Client) writeBlockToPipeline(blockID string, genStamp int64, data *bytes.Buffer, pipeline []string, chunkBuf []byte) error {
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
// the checksum if available.
func (c *Client) readBlockFromDataNode(blockInfo *pb.BlockInfo, writer io.Writer) error {
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
