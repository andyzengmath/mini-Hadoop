package datanode

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"

	"google.golang.org/grpc"
)

// maxConcurrentReplications caps how many replicateBlock calls can run at once.
// Without this cap, a NameNode under-replication storm (e.g. many blocks flagged
// under-replicated after a peer DN goes offline) spawns one goroutine per block,
// each holding a streaming buffer + open gRPC stream — empirically observed
// pushing worker memory to 20 GiB+ in the v2 D1 benchmark. 3 is conservative
// but leaves enough headroom that replication doesn't become a single-threaded
// bottleneck.
const maxConcurrentReplications = 3

// Server implements the DataNodeService gRPC interface.
type Server struct {
	pb.UnimplementedDataNodeServiceServer

	nodeID  string
	address string
	storage *BlockStorage
	cfg     config.Config

	namenodeConn   *grpc.ClientConn
	namenodeClient pb.NameNodeServiceClient

	replicateSem chan struct{} // bounded semaphore for replicateBlock
	stopCh       chan struct{}
}

// NewServer creates a new DataNode server.
func NewServer(nodeID, address string, cfg config.Config) (*Server, error) {
	storage, err := NewBlockStorage(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("init storage: %w", err)
	}

	return &Server{
		nodeID:       nodeID,
		address:      address,
		storage:      storage,
		cfg:          cfg,
		replicateSem: make(chan struct{}, maxConcurrentReplications),
		stopCh:       make(chan struct{}),
	}, nil
}

// Start connects to the NameNode and begins background tasks.
func (s *Server) Start() error {
	conn, err := rpc.DialWithRetry(s.cfg.NameNodeAddress(), 5)
	if err != nil {
		return fmt.Errorf("connect to NameNode: %w", err)
	}
	s.namenodeConn = conn
	s.namenodeClient = pb.NewNameNodeServiceClient(conn)

	// Register with NameNode
	resp, err := s.namenodeClient.RegisterDataNode(context.Background(), &pb.RegisterDataNodeRequest{
		NodeId:        s.nodeID,
		Address:       s.address,
		CapacityBytes: 100 * 1024 * 1024 * 1024, // 100 GB default capacity
	})
	if err != nil {
		return fmt.Errorf("register with NameNode: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("registration rejected: %s", resp.Error)
	}

	go s.heartbeatLoop()
	go s.blockReportLoop()

	slog.Info("DataNode started", "nodeID", s.nodeID, "address", s.address)
	return nil
}

// Stop halts background tasks and closes connections.
func (s *Server) Stop() {
	close(s.stopCh)
	if s.namenodeConn != nil {
		s.namenodeConn.Close()
	}
	slog.Info("DataNode stopped", "nodeID", s.nodeID)
}

// heartbeatLoop sends periodic heartbeats to the NameNode and processes commands.
func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sendHeartbeat()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) sendHeartbeat() {
	usedBytes := s.storage.GetUsedBytes()
	resp, err := s.namenodeClient.Heartbeat(context.Background(), &pb.HeartbeatRequest{
		NodeId:         s.nodeID,
		UsedBytes:      usedBytes,
		AvailableBytes: 100*1024*1024*1024 - usedBytes,
		NumBlocks:      s.storage.BlockCount(),
	})
	if err != nil {
		// Auto re-register when the NameNode has restarted and no longer knows this node.
		// Without this, a restarted NN permanently sees 0 alive DataNodes even though DNs
		// keep heartbeating — breaking writes with "not enough DataNodes" until cluster restart.
		if strings.Contains(err.Error(), "unknown DataNode") {
			slog.Info("NameNode does not recognize this node, re-registering", "nodeID", s.nodeID)
			if rerr := s.reregister(); rerr != nil {
				slog.Warn("re-registration failed", "error", rerr)
			}
			return
		}
		slog.Warn("heartbeat failed", "error", err)
		return
	}

	// Process NameNode commands
	for _, cmd := range resp.Commands {
		go s.executeCommand(cmd)
	}
}

// reregister re-sends the Register RPC, used when the NameNode has restarted
// and no longer knows about this DataNode.
func (s *Server) reregister() error {
	resp, err := s.namenodeClient.RegisterDataNode(context.Background(), &pb.RegisterDataNodeRequest{
		NodeId:        s.nodeID,
		Address:       s.address,
		CapacityBytes: 100 * 1024 * 1024 * 1024,
	})
	if err != nil {
		return fmt.Errorf("re-register RPC: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("re-register rejected: %s", resp.Error)
	}
	slog.Info("re-registered with NameNode", "nodeID", s.nodeID)
	return nil
}

func (s *Server) executeCommand(cmd *pb.BlockCommand) {
	switch cmd.Type {
	case pb.CommandType_REPLICATE:
		// Bounded concurrency: never hold more than maxConcurrentReplications
		// block-streams + file writes in flight. Prior to this, each REPLICATE
		// command spawned an uncapped goroutine, letting a single NN storm push
		// the DN to 20 GiB RSS and complete write failure under sustained load.
		select {
		case s.replicateSem <- struct{}{}:
		case <-s.stopCh:
			return
		}
		defer func() { <-s.replicateSem }()

		if err := s.replicateBlock(cmd.BlockId, cmd.SourceNode); err != nil {
			slog.Error("replicate command failed", "blockID", cmd.BlockId, "error", err)
		}
	case pb.CommandType_DELETE:
		if err := s.storage.DeleteBlock(cmd.BlockId); err != nil {
			slog.Error("delete command failed", "blockID", cmd.BlockId, "error", err)
		}
	}
}

// replicateBlock streams a block from sourceAddr into local storage.
// Previously this function buffered the entire block in a bytes.Buffer, which
// for 128 MB blocks × a storm of concurrent replications drove the DN to OOM.
// This version pipes chunks directly from the gRPC stream into WriteBlock so
// only one chunk is resident at a time.
func (s *Server) replicateBlock(blockID, sourceAddr string) error {
	slog.Info("replicating block", "blockID", blockID, "source", sourceAddr)

	conn, err := rpc.Dial(sourceAddr)
	if err != nil {
		return fmt.Errorf("connect to source for replication: %w", err)
	}
	defer conn.Close()

	client := pb.NewDataNodeServiceClient(conn)
	stream, err := client.ReadBlock(context.Background(), &pb.ReadBlockRequest{
		BlockId: blockID,
	})
	if err != nil {
		return fmt.Errorf("read block for replication: %w", err)
	}

	pr, pw := io.Pipe()

	// Writer goroutine: stream gRPC chunks into the pipe.
	go func() {
		for {
			chunk, recvErr := stream.Recv()
			if recvErr == io.EOF {
				pw.Close()
				return
			}
			if recvErr != nil {
				pw.CloseWithError(fmt.Errorf("receive chunk for replication: %w", recvErr))
				return
			}
			if _, werr := pw.Write(chunk.Data); werr != nil {
				// Consumer (WriteBlock) closed the pipe early.
				return
			}
		}
	}()

	// Main goroutine consumes the pipe and writes to local storage.
	// An error from WriteBlock (or a pipe error propagated from Recv) will
	// surface here; the goroutine above is guaranteed to exit because Close()
	// and CloseWithError() terminate the loop.
	if _, _, err := s.storage.WriteBlock(blockID, 1, pr); err != nil {
		_ = pr.CloseWithError(err) // ensure writer goroutine sees close
		return fmt.Errorf("write replicated block: %w", err)
	}

	slog.Info("block replicated successfully", "blockID", blockID)
	return nil
}

// blockReportLoop sends periodic block reports to the NameNode.
func (s *Server) blockReportLoop() {
	ticker := time.NewTicker(s.cfg.BlockReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sendBlockReport()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) sendBlockReport() {
	entries := s.storage.GetBlockReport()

	blocks := make([]*pb.BlockReportEntry, len(entries))
	for i, e := range entries {
		blocks[i] = &pb.BlockReportEntry{
			BlockId:         e.BlockID,
			SizeBytes:       e.SizeBytes,
			GenerationStamp: e.GenerationStamp,
		}
	}

	resp, err := s.namenodeClient.BlockReport(context.Background(), &pb.BlockReportRequest{
		NodeId: s.nodeID,
		Blocks: blocks,
	})
	if err != nil {
		slog.Warn("block report failed", "error", err)
		return
	}

	// Delete orphaned blocks
	for _, blockID := range resp.BlocksToDelete {
		if err := s.storage.DeleteBlock(blockID); err != nil {
			slog.Warn("failed to delete orphaned block", "blockID", blockID, "error", err)
		}
	}
}

// --- gRPC method implementations ---

func (s *Server) ReadBlock(req *pb.ReadBlockRequest, stream pb.DataNodeService_ReadBlockServer) error {
	reader, size, err := s.storage.ReadBlock(req.BlockId)
	if err != nil {
		return err
	}
	defer reader.Close()

	chunkSize := s.cfg.ChunkSize
	buf := make([]byte, chunkSize)
	var offset int64

	for offset < size {
		n, err := reader.Read(buf)
		if n > 0 {
			isLast := offset+int64(n) >= size
			if sendErr := stream.Send(&pb.DataChunk{
				Data:   buf[:n],
				Offset: offset,
				IsLast: isLast,
			}); sendErr != nil {
				return sendErr
			}
			offset += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) WriteBlock(stream pb.DataNodeService_WriteBlockServer) error {
	// First message must be the header
	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receive header: %w", err)
	}

	header := firstMsg.GetHeader()
	if header == nil {
		return fmt.Errorf("first message must be a WriteBlockHeader")
	}

	blockID := header.BlockId
	genStamp := header.GenerationStamp
	pipeline := header.Pipeline
	pipelineIdx := int(header.PipelineIndex)

	slog.Info("write block started",
		"blockID", blockID,
		"pipelineIndex", pipelineIdx,
		"pipelineLength", len(pipeline),
	)

	// Set up forwarding to next node in pipeline (if any)
	var nextStream pb.DataNodeService_WriteBlockClient
	var nextConn *grpc.ClientConn

	if pipelineIdx+1 < len(pipeline) {
		nextAddr := pipeline[pipelineIdx+1]
		nextConn, err = rpc.Dial(nextAddr)
		if err != nil {
			return fmt.Errorf("connect to next pipeline node %s: %w", nextAddr, err)
		}
		defer nextConn.Close()

		nextClient := pb.NewDataNodeServiceClient(nextConn)
		nextStream, err = nextClient.WriteBlock(stream.Context())
		if err != nil {
			return fmt.Errorf("open write to next pipeline node: %w", err)
		}

		// Forward header with incremented pipeline index
		if err := nextStream.Send(&pb.WriteBlockRequest{
			Payload: &pb.WriteBlockRequest_Header{
				Header: &pb.WriteBlockHeader{
					BlockId:         blockID,
					GenerationStamp: genStamp,
					Pipeline:        pipeline,
					PipelineIndex:   int32(pipelineIdx + 1),
				},
			},
		}); err != nil {
			return fmt.Errorf("forward header: %w", err)
		}
	}

	// Stream chunks directly to disk + forward + compute checksum incrementally.
	// Memory usage: O(chunk_size) per write, NOT O(block_size).
	blockPath, pathErr := s.storage.BlockPathForWrite(blockID)
	if pathErr != nil {
		return stream.SendAndClose(&pb.WriteBlockResponse{Success: false, Error: pathErr.Error()})
	}

	localFile, fileErr := os.Create(blockPath)
	if fileErr != nil {
		return stream.SendAndClose(&pb.WriteBlockResponse{Success: false, Error: fileErr.Error()})
	}

	hasher := sha256.New()
	var written int64

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			localFile.Close()
			os.Remove(blockPath)
			return fmt.Errorf("receive chunk: %w", err)
		}

		chunk := msg.GetChunk()
		if chunk == nil {
			continue
		}

		// Write to local disk
		n, writeErr := localFile.Write(chunk.Data)
		if writeErr != nil {
			localFile.Close()
			os.Remove(blockPath)
			return fmt.Errorf("write to disk: %w", writeErr)
		}
		written += int64(n)

		// Update checksum incrementally
		hasher.Write(chunk.Data)

		// Forward to next in pipeline — break forwarding on error
		if nextStream != nil {
			if err := nextStream.Send(&pb.WriteBlockRequest{
				Payload: &pb.WriteBlockRequest_Chunk{Chunk: chunk},
			}); err != nil {
				slog.Error("pipeline forward failed, stopping downstream", "blockID", blockID, "error", err)
				nextStream = nil
			}
		}

		if chunk.IsLast {
			break
		}
	}
	localFile.Close()
	checksum := hasher.Sum(nil)

	// Register the block in storage tracking
	s.storage.RegisterWrittenBlock(blockID, genStamp, written)

	// Close forwarding stream and get response (only if stream is still healthy)
	if nextStream != nil {
		nextResp, err := nextStream.CloseAndRecv()
		if err != nil {
			slog.Warn("pipeline close failed", "blockID", blockID, "error", err)
		} else if !nextResp.Success {
			slog.Warn("pipeline downstream write failed", "blockID", blockID, "error", nextResp.Error)
		}
	}
	if err != nil {
		return stream.SendAndClose(&pb.WriteBlockResponse{
			Success: false,
			Error:   err.Error(),
		})
	}

	return stream.SendAndClose(&pb.WriteBlockResponse{
		Success:      true,
		BytesWritten: written,
		Checksum:     checksum,
	})
}

func (s *Server) DeleteBlock(_ context.Context, req *pb.DeleteBlockRequest) (*pb.DeleteBlockResponse, error) {
	if err := s.storage.DeleteBlock(req.BlockId); err != nil {
		return &pb.DeleteBlockResponse{Success: false, Error: err.Error()}, nil
	}
	return &pb.DeleteBlockResponse{Success: true}, nil
}

func (s *Server) TransferBlock(_ context.Context, req *pb.TransferBlockRequest) (*pb.TransferBlockResponse, error) {
	if err := s.replicateBlock(req.BlockId, req.SourceAddress); err != nil {
		return &pb.TransferBlockResponse{Success: false, Error: err.Error()}, nil
	}
	return &pb.TransferBlockResponse{Success: true}, nil
}
