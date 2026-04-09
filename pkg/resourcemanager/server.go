package resourcemanager

import (
	"context"
	"log/slog"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"

	pb "github.com/mini-hadoop/mini-hadoop/proto"
)

// Server implements the ResourceManagerService gRPC interface.
type Server struct {
	pb.UnimplementedResourceManagerServiceServer

	scheduler *FIFOScheduler
	cfg       config.Config
	stopCh    chan struct{}
}

// NewServer creates a new ResourceManager server.
func NewServer(cfg config.Config) *Server {
	return &Server{
		scheduler: NewFIFOScheduler(cfg.DeadNodeTimeout),
		cfg:       cfg,
		stopCh:    make(chan struct{}),
	}
}

// Start begins background monitoring.
func (s *Server) Start() {
	go s.nodeMonitor()
	slog.Info("ResourceManager started")
}

// Stop halts background tasks.
func (s *Server) Stop() {
	close(s.stopCh)
	slog.Info("ResourceManager stopped")
}

func (s *Server) nodeMonitor() {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.scheduler.DetectDeadNodes()
		case <-s.stopCh:
			return
		}
	}
}

// --- gRPC implementations ---

func (s *Server) SubmitApplication(_ context.Context, req *pb.SubmitApplicationRequest) (*pb.SubmitApplicationResponse, error) {
	amMemMB := req.AmMemoryMb
	if amMemMB <= 0 {
		amMemMB = 512
	}
	amCPU := req.AmCpuVcores
	if amCPU <= 0 {
		amCPU = 1
	}

	appID, err := s.scheduler.SubmitApp(
		req.ApplicationType,
		req.AmBinaryPath,
		req.AmArgs,
		amMemMB, amCPU,
		req.AmEnv,
	)
	if err != nil {
		return &pb.SubmitApplicationResponse{AppId: appID, Error: err.Error()}, nil
	}

	return &pb.SubmitApplicationResponse{AppId: appID}, nil
}

func (s *Server) GetApplicationReport(_ context.Context, req *pb.GetApplicationReportRequest) (*pb.GetApplicationReportResponse, error) {
	app, err := s.scheduler.GetAppInfo(req.AppId)
	if err != nil {
		return &pb.GetApplicationReportResponse{Error: err.Error()}, nil
	}

	state := appStateToProto(app.State)
	return &pb.GetApplicationReportResponse{
		Status: &pb.ApplicationStatus{
			AppId:      app.AppID,
			State:      state,
			AmAddress:  app.AMAddress,
			Progress:   app.Progress,
			Diagnostics: app.Diagnostics,
			StartTime:  app.StartTime.UnixMilli(),
			FinishTime: app.FinishTime.UnixMilli(),
		},
	}, nil
}

func (s *Server) KillApplication(_ context.Context, req *pb.KillApplicationRequest) (*pb.KillApplicationResponse, error) {
	s.scheduler.CompleteApp(req.AppId, "KILLED", "Killed by user")
	return &pb.KillApplicationResponse{Success: true}, nil
}

func (s *Server) AllocateContainers(_ context.Context, req *pb.AllocateContainersRequest) (*pb.AllocateContainersResponse, error) {
	var requests []PendingRequest
	for _, r := range req.Requests {
		var preferred []string
		relaxed := true
		if r.Locality != nil {
			preferred = r.Locality.PreferredNodes
			relaxed = r.Locality.Relaxed
		}
		requests = append(requests, PendingRequest{
			AppID:          req.AppId,
			RequestID:      r.RequestId,
			MemoryMB:       r.MemoryMb,
			CPUVCores:      r.CpuVcores,
			PreferredNodes: preferred,
			Relaxed:        relaxed,
			Count:          r.Count,
		})
	}

	containers := s.scheduler.AllocateContainers(req.AppId, requests)

	pbContainers := make([]*pb.ContainerSpec, len(containers))
	for i, c := range containers {
		nodeAddr := s.scheduler.GetNodeAddress(c.NodeID)
		pbContainers[i] = &pb.ContainerSpec{
			ContainerId: c.ContainerID,
			NodeId:      c.NodeID,
			MemoryMb:    c.MemoryMB,
			CpuVcores:   c.CPUVCores,
		}
		// Include node address in env so AM knows where to contact
		pbContainers[i].Env = map[string]string{
			"NODE_ADDRESS": nodeAddr,
		}
	}

	return &pb.AllocateContainersResponse{Containers: pbContainers}, nil
}

func (s *Server) ReleaseContainers(_ context.Context, req *pb.ReleaseContainersRequest) (*pb.ReleaseContainersResponse, error) {
	s.scheduler.ReleaseContainers(req.AppId, req.ContainerIds)
	return &pb.ReleaseContainersResponse{Success: true}, nil
}

func (s *Server) RegisterNodeManager(_ context.Context, req *pb.RegisterNodeManagerRequest) (*pb.RegisterNodeManagerResponse, error) {
	s.scheduler.RegisterNode(req.NodeId, req.Address, req.TotalMemoryMb, req.TotalCpu)
	return &pb.RegisterNodeManagerResponse{Success: true}, nil
}

func (s *Server) NodeManagerHeartbeat(_ context.Context, req *pb.NodeManagerHeartbeatRequest) (*pb.NodeManagerHeartbeatResponse, error) {
	var nodeInfo *NodeInfo
	if req.Status != nil {
		nodeInfo = &NodeInfo{
			UsedMemoryMB: req.Status.UsedMemoryMb,
			UsedCPU:      req.Status.UsedCpu,
		}
	}

	s.scheduler.ProcessNodeHeartbeat(req.NodeId, nodeInfo)

	// Process container status updates
	for _, cs := range req.ContainerStatuses {
		state := containerStateToString(cs.State)
		s.scheduler.UpdateContainerStatus(cs.ContainerId, req.NodeId, state, cs.ExitCode, cs.Diagnostics)
	}

	return &pb.NodeManagerHeartbeatResponse{}, nil
}

func appStateToProto(state string) pb.AppState {
	switch state {
	case "SUBMITTED":
		return pb.AppState_SUBMITTED
	case "RUNNING":
		return pb.AppState_RUNNING
	case "FINISHED":
		return pb.AppState_FINISHED
	case "FAILED":
		return pb.AppState_FAILED
	case "KILLED":
		return pb.AppState_KILLED
	default:
		return pb.AppState_APP_STATE_UNSPECIFIED
	}
}

func containerStateToString(state pb.ContainerState) string {
	switch state {
	case pb.ContainerState_CONTAINER_RUNNING:
		return "RUNNING"
	case pb.ContainerState_CONTAINER_COMPLETED:
		return "COMPLETED"
	case pb.ContainerState_CONTAINER_FAILED:
		return "FAILED"
	case pb.ContainerState_CONTAINER_KILLED:
		return "KILLED"
	default:
		return "UNKNOWN"
	}
}
