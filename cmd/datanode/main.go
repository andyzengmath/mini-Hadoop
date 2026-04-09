package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/datanode"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"
)

func main() {
	configPath := flag.String("config", "", "Path to config file (JSON)")
	nodeID := flag.String("id", "", "DataNode ID (required)")
	port := flag.Int("port", 0, "Override DataNode port")
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
		cfg.DataNodePort = *port
	}

	hostname := os.Getenv("MINIHADOOP_HOSTNAME")
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
	if hostname == "" {
		hostname = "localhost"
	}
	address := fmt.Sprintf("%s:%d", hostname, cfg.DataNodePort)

	dn, err := datanode.NewServer(*nodeID, address, cfg)
	if err != nil {
		slog.Error("failed to create DataNode", "error", err)
		os.Exit(1)
	}

	if err := dn.Start(); err != nil {
		slog.Error("failed to start DataNode", "error", err)
		os.Exit(1)
	}

	grpcServer := rpc.NewServer(rpc.ServerConfig{Port: cfg.DataNodePort})
	pb.RegisterDataNodeServiceServer(grpcServer, dn)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("shutdown signal received")
		dn.Stop()
		grpcServer.GracefulStop()
	}()

	slog.Info("DataNode starting", "nodeID", *nodeID, "port", cfg.DataNodePort)
	if err := rpc.ListenAndServe(grpcServer, cfg.DataNodePort); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
