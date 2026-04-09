package mapreduce

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/mini-hadoop/mini-hadoop/proto"
)

const shuffleChunkSize = 1024 * 1024 // 1MB chunks for shuffle streaming

// ShuffleServer implements the ShuffleService gRPC interface.
// It serves completed map output partitions to reducers.
type ShuffleServer struct {
	pb.UnimplementedShuffleServiceServer

	mu sync.RWMutex
	// jobID -> taskID -> list of partition file paths
	mapOutputs map[string]map[string][]string
	tempDir    string
}

// NewShuffleServer creates a new shuffle service.
func NewShuffleServer(tempDir string) *ShuffleServer {
	return &ShuffleServer{
		mapOutputs: make(map[string]map[string][]string),
		tempDir:    tempDir,
	}
}

// RegisterMapOutput records the partition output files from a completed map task.
func (s *ShuffleServer) RegisterMapOutput(jobID, taskID string, partitionFiles []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.mapOutputs[jobID]; !exists {
		s.mapOutputs[jobID] = make(map[string][]string)
	}
	s.mapOutputs[jobID][taskID] = partitionFiles

	slog.Info("map output registered",
		"job_id", jobID,
		"task_id", taskID,
		"partitions", len(partitionFiles),
	)
}

// GetMapOutput streams a specific partition of map output to a reducer.
func (s *ShuffleServer) GetMapOutput(req *pb.GetMapOutputRequest, stream pb.ShuffleService_GetMapOutputServer) error {
	s.mu.RLock()
	jobOutputs, exists := s.mapOutputs[req.JobId]
	if !exists {
		s.mu.RUnlock()
		return fmt.Errorf("no outputs for job %s", req.JobId)
	}
	partFiles, exists := jobOutputs[req.MapTaskId]
	if !exists {
		s.mu.RUnlock()
		return fmt.Errorf("no outputs for map task %s", req.MapTaskId)
	}

	partitionID := int(req.PartitionId)
	if partitionID < 0 || partitionID >= len(partFiles) {
		s.mu.RUnlock()
		return fmt.Errorf("partition %d out of range (have %d)", partitionID, len(partFiles))
	}
	filePath := partFiles[partitionID]
	s.mu.RUnlock()

	// Stream the partition file
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open partition file: %w", err)
	}
	defer f.Close()

	buf := make([]byte, shuffleChunkSize)
	var offset int64

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			isLast := readErr == io.EOF
			if sendErr := stream.Send(&pb.MapOutputChunk{
				Data:   buf[:n],
				Offset: offset,
				IsLast: isLast,
			}); sendErr != nil {
				return sendErr
			}
			offset += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read partition: %w", readErr)
		}
	}

	slog.Info("shuffle served",
		"job_id", req.JobId,
		"map_task_id", req.MapTaskId,
		"partition_id", req.PartitionId,
		"bytes", offset,
	)
	return nil
}

// ReportShuffleFetchFailure handles a reducer's report that it couldn't fetch
// map output from a mapper node. The AM should re-run the failed map task.
func (s *ShuffleServer) ReportShuffleFetchFailure(_ interface{}, req *pb.ShuffleFetchFailureRequest) (*pb.ShuffleFetchFailureResponse, error) {
	slog.Warn("shuffle fetch failure reported",
		"job_id", req.JobId,
		"map_task_id", req.MapTaskId,
		"reducer_task_id", req.ReducerTaskId,
		"failed_node", req.FailedNode,
		"error", req.ErrorMessage,
	)

	// Check if we have the output from a re-run
	s.mu.RLock()
	defer s.mu.RUnlock()

	if jobOutputs, exists := s.mapOutputs[req.JobId]; exists {
		if _, exists := jobOutputs[req.MapTaskId]; exists {
			return &pb.ShuffleFetchFailureResponse{
				Acknowledged: true,
			}, nil
		}
	}

	return &pb.ShuffleFetchFailureResponse{
		Acknowledged: true,
		Error:        "map output not available, map task re-execution needed",
	}, nil
}

// CleanupJob removes all map outputs for a completed job.
func (s *ShuffleServer) CleanupJob(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if jobOutputs, exists := s.mapOutputs[jobID]; exists {
		for _, partFiles := range jobOutputs {
			for _, f := range partFiles {
				os.Remove(f)
			}
			// Clean up task temp directories
		}
		delete(s.mapOutputs, jobID)
	}

	// Clean up job temp directory
	jobDir := filepath.Join(s.tempDir, jobID)
	os.RemoveAll(jobDir)

	slog.Info("shuffle cleanup", "job_id", jobID)
}
