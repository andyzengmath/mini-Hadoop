package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/namenode"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"
)

func main() {
	configPath := flag.String("config", "", "Path to config file (JSON)")
	port := flag.Int("port", 0, "Override NameNode port")
	flag.Parse()

	// Setup structured logging
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	if *port > 0 {
		cfg.NameNodePort = *port
	}

	// Create NameNode server
	nn := namenode.NewServer(cfg)
	nn.Start()

	// Create gRPC server
	grpcServer := rpc.NewServer(rpc.ServerConfig{Port: cfg.NameNodePort})
	pb.RegisterNameNodeServiceServer(grpcServer, nn)

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("shutdown signal received")
		nn.Stop()
		grpcServer.GracefulStop()
	}()

	// Start serving
	slog.Info("NameNode starting", "port", cfg.NameNodePort)
	if err := rpc.ListenAndServe(grpcServer, cfg.NameNodePort); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
