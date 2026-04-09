package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/nodemanager"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	nodeID := flag.String("id", "", "NodeManager ID (required)")
	port := flag.Int("port", 0, "Override NodeManager port")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if *nodeID == "" {
		slog.Error("--id flag is required")
		os.Exit(1)
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	if *port > 0 {
		cfg.NodeManagerPort = *port
	}

	hostname := os.Getenv("MINIHADOOP_HOSTNAME")
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
	if hostname == "" {
		hostname = "localhost"
	}
	address := fmt.Sprintf("%s:%d", hostname, cfg.NodeManagerPort)

	nm := nodemanager.NewServer(*nodeID, address, cfg)
	if err := nm.Start(); err != nil {
		slog.Error("failed to start NodeManager", "error", err)
		os.Exit(1)
	}

	grpcServer := rpc.NewServer(rpc.ServerConfig{Port: cfg.NodeManagerPort})
	pb.RegisterNodeManagerServiceServer(grpcServer, nm)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("shutdown signal received")
		nm.Stop()
		grpcServer.GracefulStop()
	}()

	slog.Info("NodeManager starting", "nodeID", *nodeID, "port", cfg.NodeManagerPort)
	if err := rpc.ListenAndServe(grpcServer, cfg.NodeManagerPort); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
