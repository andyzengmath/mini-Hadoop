package datanode

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"

	"google.golang.org/grpc"
)

// Server implements the DataNodeService gRPC interface.
type Server struct {
	pb.UnimplementedDataNodeServiceServer

	nodeID  string
	address string
	storage *BlockStorage
	cfg     config.Config

	namenodeConn   *grpc.ClientConn
	namenodeClient pb.NameNodeServiceClient

	stopCh chan struct{}
}

// NewServer creates a new DataNode server.
func NewServer(nodeID, address string, cfg config.Config) (*Server, error) {
	storage, err := NewBlockStorage(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("init storage: %w", err)
	}

	return &Server{
		nodeID:  nodeID,
		address: address,
		storage: storage,
		cfg:     cfg,
		stopCh:  make(chan struct{}),
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
		slog.Warn("heartbeat failed", "error", err)
		return
	}

	// Process NameNode commands
	for _, cmd := range resp.Commands {
		go s.executeCommand(cmd)
	}
}

func (s *Server) executeCommand(cmd *pb.BlockCommand) {
	switch cmd.Type {
	case pb.CommandType_REPLICATE:
		s.replicateBlock(cmd.BlockId, cmd.SourceNode)
	case pb.CommandType_DELETE:
		if err := s.storage.DeleteBlock(cmd.BlockId); err != nil {
			slog.Error("delete command failed", "blockID", cmd.BlockId, "error", err)
		}
	}
}

func (s *Server) replicateBlock(blockID, sourceAddr string) {
	slog.Info("replicating block", "blockID", blockID, "source", sourceAddr)

	conn, err := rpc.Dial(sourceAddr)
	if err != nil {
		slog.Error("connect to source for replication", "error", err)
		return
	}
	defer conn.Close()

	client := pb.NewDataNodeServiceClient(conn)
	stream, err := client.ReadBlock(context.Background(), &pb.ReadBlockRequest{
		BlockId: blockID,
	})
	if err != nil {
		slog.Error("read block for replication", "error", err)
		return
	}

	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Error("receive chunk for replication", "error", err)
			return
		}
		buf.Write(chunk.Data)
	}

	_, _, err = s.storage.WriteBlock(blockID, 1, &buf)
	if err != nil {
		slog.Error("write replicated block", "error", err)
		return
	}

	slog.Info("block replicated successfully", "blockID", blockID)
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

	// Receive data chunks, write locally, and forward
	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("receive chunk: %w", err)
		}

		chunk := msg.GetChunk()
		if chunk == nil {
			continue
		}

		buf.Write(chunk.Data)

		// Forward to next in pipeline
		if nextStream != nil {
			if err := nextStream.Send(&pb.WriteBlockRequest{
				Payload: &pb.WriteBlockRequest_Chunk{Chunk: chunk},
			}); err != nil {
				slog.Error("pipeline forward failed", "blockID", blockID, "error", err)
				// Pipeline failure: we still write locally
				// The NameNode will detect under-replication and fix it
			}
		}

		if chunk.IsLast {
			break
		}
	}

	// Close forwarding stream and get response
	if nextStream != nil {
		nextResp, err := nextStream.CloseAndRecv()
		if err != nil {
			slog.Warn("pipeline close failed", "blockID", blockID, "error", err)
		} else if !nextResp.Success {
			slog.Warn("pipeline downstream write failed", "blockID", blockID, "error", nextResp.Error)
		}
	}

	// Write block locally
	written, checksum, err := s.storage.WriteBlock(blockID, genStamp, &buf)
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
	s.replicateBlock(req.BlockId, req.SourceAddress)
	return &pb.TransferBlockResponse{Success: true}, nil
}
