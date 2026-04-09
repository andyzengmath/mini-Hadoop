package mapreduce

import (
	"context"
	"fmt"

	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"
)

// InputSplit represents a unit of work for a map task, aligned to an HDFS block.
type InputSplit struct {
	FilePath  string
	BlockID   string
	Offset    int64
	Length    int64
	Locations []string // DataNode addresses holding this block
}

// ComputeSplits queries the NameNode for block locations and creates one
// InputSplit per block. This is the bridge between HDFS storage and MapReduce
// computation — it enables data-local task scheduling.
func ComputeSplits(namenodeAddr, filePath string) ([]InputSplit, error) {
	conn, err := rpc.DialWithRetry(namenodeAddr, 3)
	if err != nil {
		return nil, fmt.Errorf("connect to NameNode: %w", err)
	}
	defer conn.Close()

	nnClient := pb.NewNameNodeServiceClient(conn)

	// Get block locations for the file
	resp, err := nnClient.GetBlockLocations(context.Background(), &pb.GetBlockLocationsRequest{
		Path: filePath,
	})
	if err != nil {
		return nil, fmt.Errorf("get block locations: %w", err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("get block locations: %s", resp.Error)
	}

	splits := make([]InputSplit, 0, len(resp.Blocks))
	var offset int64

	for _, block := range resp.Blocks {
		splits = append(splits, InputSplit{
			FilePath:  filePath,
			BlockID:   block.BlockId,
			Offset:    offset,
			Length:    block.SizeBytes,
			Locations: block.Locations,
		})
		offset += block.SizeBytes
	}

	return splits, nil
}

// SplitsToProto converts InputSplits to protobuf messages.
func SplitsToProto(splits []InputSplit) []*pb.InputSplit {
	result := make([]*pb.InputSplit, len(splits))
	for i, s := range splits {
		result[i] = &pb.InputSplit{
			FilePath:  s.FilePath,
			BlockId:   s.BlockID,
			Offset:    s.Offset,
			Length:    s.Length,
			Locations: s.Locations,
		}
	}
	return result
}

// SplitsFromProto converts protobuf InputSplits to local type.
func SplitsFromProto(pbSplits []*pb.InputSplit) []InputSplit {
	result := make([]InputSplit, len(pbSplits))
	for i, s := range pbSplits {
		result[i] = InputSplit{
			FilePath:  s.FilePath,
			BlockID:   s.BlockId,
			Offset:    s.Offset,
			Length:    s.Length,
			Locations: s.Locations,
		}
	}
	return result
}
