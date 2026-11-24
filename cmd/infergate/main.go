package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	pb "github.com/erangamapa/infergate/gen/infergate/v1"
	"github.com/erangamapa/infergate/internal/config"
	"github.com/erangamapa/infergate/internal/server"
)

func main() {
	path := flag.String("config", "config/gateway.example.yaml", "path to gateway yaml")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(*path, log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(path string, log *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	httpClient := &http.Client{Transport: &http.Transport{
		MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second, ResponseHeaderTimeout: cfg.AttemptTimeout,
	}}
	providers, err := cfg.Providers(os.Getenv, httpClient)
	if err != nil {
		return err
	}
	rt, err := cfg.Router(providers)
	if err != nil {
		return err
	}
	keys := cfg.Keys()

	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(keys.Unary()),
		grpc.ChainStreamInterceptor(keys.Stream()),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 10 * time.Minute, MaxConnectionAgeGrace: time.Minute}),
	)
	pb.RegisterInferenceServer(gs, server.New(rt, cfg.Limiter(), log))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(gs, hs)
	reflection.Register(gs)

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		// stop advertising first so the load balancer drains us, then finish in flight streams
		hs.Shutdown()
		done := make(chan struct{})
		go func() { gs.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			gs.Stop()
		}
	}()
	log.Info("listening", "addr", cfg.Listen, "routes", len(rt.Routes()))
	return gs.Serve(lis)
}
