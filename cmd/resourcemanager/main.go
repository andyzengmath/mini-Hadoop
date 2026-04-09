package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/resourcemanager"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	port := flag.Int("port", 0, "Override ResourceManager port")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	if *port > 0 {
		cfg.ResourceManagerPort = *port
	}

	rm := resourcemanager.NewServer(cfg)
	if err := rm.Start(); err != nil {
		slog.Error("failed to start ResourceManager", "error", err)
		os.Exit(1)
	}

	grpcServer := rpc.NewServer(rpc.ServerConfig{Port: cfg.ResourceManagerPort})
	pb.RegisterResourceManagerServiceServer(grpcServer, rm)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("shutdown signal received")
		rm.Stop()
		grpcServer.GracefulStop()
	}()

	slog.Info("ResourceManager starting", "port", cfg.ResourceManagerPort)
	if err := rpc.ListenAndServe(grpcServer, cfg.ResourceManagerPort); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
