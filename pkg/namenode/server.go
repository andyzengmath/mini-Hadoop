package namenode

import (
	"context"
	"log/slog"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"

	pb "github.com/mini-hadoop/mini-hadoop/proto"
)

// Server implements the NameNodeService gRPC interface.
type Server struct {
	pb.UnimplementedNameNodeServiceServer

	ns  *Namespace
	bm  *BlockManager
	cfg config.Config

	stopCh chan struct{}
}

// NewServer creates a new NameNode server.
func NewServer(cfg config.Config) *Server {
	return &Server{
		ns:     NewNamespace(),
		bm:     NewBlockManager(cfg.ReplicationFactor, cfg.DeadNodeTimeout),
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// Start begins background goroutines for heartbeat monitoring and re-replication.
func (s *Server) Start() {
	go s.heartbeatMonitor()
	go s.metadataDumper()
	slog.Info("NameNode background tasks started")
}

// Stop halts background goroutines and saves state.
func (s *Server) Stop() {
	close(s.stopCh)
	if err := SaveState(s.ns, s.bm, s.cfg.MetadataDir); err != nil {
		slog.Error("failed to save state on shutdown", "error", err)
	}
	slog.Info("NameNode stopped")
}

// heartbeatMonitor periodically checks for dead DataNodes and triggers re-replication.
func (s *Server) heartbeatMonitor() {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			deadNodes := s.bm.DetectDeadNodes()
			if len(deadNodes) > 0 {
				slog.Warn("dead nodes detected", "count", len(deadNodes), "nodes", deadNodes)
			}
			// Always check for under-replicated blocks, not just on new deaths.
			// Failed replications from prior cycles need to be retried.
			s.bm.CheckAndReplicateBlocks()
		case <-s.stopCh:
			return
		}
	}
}

// metadataDumper periodically saves state to disk.
func (s *Server) metadataDumper() {
	ticker := time.NewTicker(s.cfg.MetadataDumpInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := SaveState(s.ns, s.bm, s.cfg.MetadataDir); err != nil {
				slog.Error("periodic state dump failed", "error", err)
			}
		case <-s.stopCh:
			return
		}
	}
}

// --- gRPC method implementations ---

func (s *Server) CreateFile(_ context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	replication := req.ReplicationFactor
	if replication <= 0 {
		replication = s.cfg.ReplicationFactor
	}

	if err := s.ns.CreateFile(req.Path, replication); err != nil {
		return &pb.CreateFileResponse{Success: false, Error: err.Error()}, nil
	}

	slog.Info("file created", "path", req.Path, "replication", replication)
	return &pb.CreateFileResponse{Success: true}, nil
}

func (s *Server) GetBlockLocations(_ context.Context, req *pb.GetBlockLocationsRequest) (*pb.GetBlockLocationsResponse, error) {
	node, err := s.ns.GetFile(req.Path)
	if err != nil {
		return &pb.GetBlockLocationsResponse{Error: err.Error()}, nil
	}

	metas := s.bm.GetBlockLocations(node.BlockIDs)
	blocks := make([]*pb.BlockInfo, len(metas))
	for i, m := range metas {
		blocks[i] = &pb.BlockInfo{
			BlockId:         m.BlockID.String(),
			SizeBytes:       m.SizeBytes,
			Checksum:        m.Checksum,
			Locations:       m.Locations,
			GenerationStamp: m.GenerationStamp,
		}
	}

	return &pb.GetBlockLocationsResponse{Blocks: blocks}, nil
}

func (s *Server) AddBlock(_ context.Context, req *pb.AddBlockRequest) (*pb.AddBlockResponse, error) {
	node, err := s.ns.GetFile(req.Path)
	if err != nil {
		return &pb.AddBlockResponse{Error: err.Error()}, nil
	}

	meta, pipeline, err := s.bm.AllocateBlock(node.Replication, req.ClientAddress)
	if err != nil {
		return &pb.AddBlockResponse{Error: err.Error()}, nil
	}

	if err := s.ns.AddBlockToFile(req.Path, meta.BlockID.String()); err != nil {
		return &pb.AddBlockResponse{Error: err.Error()}, nil
	}

	return &pb.AddBlockResponse{
		Block: &pb.BlockInfo{
			BlockId:         meta.BlockID.String(),
			SizeBytes:       meta.SizeBytes,
			Checksum:        meta.Checksum,
			Locations:       meta.Locations,
			GenerationStamp: meta.GenerationStamp,
		},
		Pipeline: pipeline,
	}, nil
}

func (s *Server) CompleteFile(_ context.Context, req *pb.CompleteFileRequest) (*pb.CompleteFileResponse, error) {
	// Calculate total size from blocks
	metas := s.bm.GetBlockLocations(req.BlockIds)
	var totalSize int64
	for _, m := range metas {
		totalSize += m.SizeBytes
	}

	if err := s.ns.CompleteFile(req.Path, req.BlockIds, totalSize); err != nil {
		return &pb.CompleteFileResponse{Success: false, Error: err.Error()}, nil
	}

	slog.Info("file completed", "path", req.Path, "blocks", len(req.BlockIds), "size", totalSize)
	return &pb.CompleteFileResponse{Success: true}, nil
}

func (s *Server) DeleteFile(_ context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	blockIDs, err := s.ns.Delete(req.Path, req.Recursive)
	if err != nil {
		return &pb.DeleteFileResponse{Success: false, Error: err.Error()}, nil
	}

	for _, id := range blockIDs {
		s.bm.RemoveBlock(id)
	}

	slog.Info("file deleted", "path", req.Path, "blocks_removed", len(blockIDs))
	return &pb.DeleteFileResponse{Success: true}, nil
}

func (s *Server) ListDirectory(_ context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	children, err := s.ns.ListDir(req.Path)
	if err != nil {
		return &pb.ListDirectoryResponse{Error: err.Error()}, nil
	}

	entries := make([]*pb.FileInfo, len(children))
	for i, child := range children {
		entries[i] = &pb.FileInfo{
			Path:              child.Name,
			IsDirectory:       child.IsDir,
			SizeBytes:         child.Size,
			ReplicationFactor: child.Replication,
			Permissions:       child.Permissions,
			ModificationTime:  child.ModTime.UnixMilli(),
		}
	}

	return &pb.ListDirectoryResponse{Entries: entries}, nil
}

func (s *Server) MkDir(_ context.Context, req *pb.MkDirRequest) (*pb.MkDirResponse, error) {
	if err := s.ns.MkDir(req.Path, req.CreateParents); err != nil {
		return &pb.MkDirResponse{Success: false, Error: err.Error()}, nil
	}
	return &pb.MkDirResponse{Success: true}, nil
}

func (s *Server) GetFileInfo(_ context.Context, req *pb.GetFileInfoRequest) (*pb.GetFileInfoResponse, error) {
	node, err := s.ns.GetFile(req.Path)
	if err != nil {
		return &pb.GetFileInfoResponse{Error: err.Error()}, nil
	}

	return &pb.GetFileInfoResponse{
		Info: &pb.FileInfo{
			Path:              req.Path,
			IsDirectory:       node.IsDir,
			SizeBytes:         node.Size,
			BlockIds:          node.BlockIDs,
			ReplicationFactor: node.Replication,
			Permissions:       node.Permissions,
			ModificationTime:  node.ModTime.UnixMilli(),
		},
	}, nil
}

func (s *Server) RegisterDataNode(_ context.Context, req *pb.RegisterDataNodeRequest) (*pb.RegisterDataNodeResponse, error) {
	if err := s.bm.RegisterDataNode(req.NodeId, req.Address, req.CapacityBytes); err != nil {
		return &pb.RegisterDataNodeResponse{Success: false, Error: err.Error()}, nil
	}
	return &pb.RegisterDataNodeResponse{Success: true}, nil
}

func (s *Server) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	cmds, err := s.bm.ProcessHeartbeat(req.NodeId, req.UsedBytes, req.AvailableBytes, req.NumBlocks)
	if err != nil {
		return &pb.HeartbeatResponse{}, nil
	}

	pbCmds := make([]*pb.BlockCommand, len(cmds))
	for i, cmd := range cmds {
		cmdType := pb.CommandType_COMMAND_TYPE_UNSPECIFIED
		switch cmd.Type {
		case "REPLICATE":
			cmdType = pb.CommandType_REPLICATE
		case "DELETE":
			cmdType = pb.CommandType_DELETE
		}
		pbCmds[i] = &pb.BlockCommand{
			Type:        cmdType,
			BlockId:     cmd.BlockID,
			TargetNodes: cmd.TargetNodes,
			SourceNode:  cmd.SourceNode,
		}
	}

	return &pb.HeartbeatResponse{Commands: pbCmds}, nil
}

func (s *Server) BlockReport(_ context.Context, req *pb.BlockReportRequest) (*pb.BlockReportResponse, error) {
	reported := make([]struct {
		BlockID         string
		SizeBytes       int64
		GenerationStamp int64
	}, len(req.Blocks))

	for i, b := range req.Blocks {
		reported[i] = struct {
			BlockID         string
			SizeBytes       int64
			GenerationStamp int64
		}{
			BlockID:         b.BlockId,
			SizeBytes:       b.SizeBytes,
			GenerationStamp: b.GenerationStamp,
		}
	}

	toDelete := s.bm.ProcessBlockReport(req.NodeId, reported)
	return &pb.BlockReportResponse{BlocksToDelete: toDelete}, nil
}
