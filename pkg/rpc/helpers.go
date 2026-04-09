package rpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	DefaultDialTimeout    = 5 * time.Second
	DefaultMaxRecvMsgSize = 4 * 1024 * 1024 // 4 MB (for streaming chunks, not full blocks)
	DefaultMaxSendMsgSize = 4 * 1024 * 1024
)

// ServerConfig holds settings for creating a gRPC server.
type ServerConfig struct {
	Port           int
	MaxRecvMsgSize int
	MaxSendMsgSize int
}

// NewServer creates a new gRPC server with standard options.
func NewServer(cfg ServerConfig) *grpc.Server {
	recvSize := cfg.MaxRecvMsgSize
	if recvSize == 0 {
		recvSize = DefaultMaxRecvMsgSize
	}
	sendSize := cfg.MaxSendMsgSize
	if sendSize == 0 {
		sendSize = DefaultMaxSendMsgSize
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(recvSize),
		grpc.MaxSendMsgSize(sendSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	}

	return grpc.NewServer(opts...)
}

// ListenAndServe starts the gRPC server on the given port.
func ListenAndServe(server *grpc.Server, port int) error {
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	slog.Info("gRPC server listening", "address", addr)
	return server.Serve(lis)
}

// Dial creates a gRPC client connection to the given address.
func Dial(address string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultDialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(DefaultMaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(DefaultMaxSendMsgSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", address, err)
	}
	return conn, nil
}

// DialWithRetry attempts to connect with exponential backoff.
func DialWithRetry(address string, maxRetries int) (*grpc.ClientConn, error) {
	var lastErr error
	backoff := 500 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		conn, err := Dial(address)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if attempt < maxRetries {
			slog.Warn("dial failed, retrying",
				"address", address,
				"attempt", attempt+1,
				"backoff", backoff,
				"error", err,
			)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}
	}
	return nil, fmt.Errorf("failed to connect to %s after %d retries: %w", address, maxRetries, lastErr)
}
