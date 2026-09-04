package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/coordinator"
	"github.com/fiberevm/rpc-proxy/internal/gateway"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
)

func main() {
	configPath := flag.String("config", "rpc-proxy.yaml", "path to YAML configuration")
	flag.Parse()
	logger := telemetry.NewLogger()
	if err := run(*configPath, logger); err != nil {
		logger.Error("rpc proxy stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	tel, err := telemetry.NewTelemetry(cfg.Datadog)
	if err != nil {
		return fmt.Errorf("start telemetry: %w", err)
	}
	defer func() {
		if err := tel.Close(); err != nil {
			logger.Error("close telemetry", "error", err)
		}
	}()
	storeOptions := head.RedisStoreOptions{URL: cfg.Redis.URL, KeyPrefix: cfg.Redis.KeyPrefix, StreamMaxLength: cfg.Redis.StreamMaxLength}
	store, err := head.NewRedisStore(storeOptions)
	if err != nil {
		return fmt.Errorf("create head store: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("close head store", "error", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := store.Ping(ctx); err != nil {
		tel.Count("redis.failure", 1, "operation:startup_ping")
		return fmt.Errorf("connect to redis: %w", err)
	}
	owner, err := getInstanceID()
	if err != nil {
		return err
	}
	runtimes := map[string]*chain.Runtime{}
	for _, chainConfig := range cfg.Chains {
		runtime := chain.NewRuntime(chainConfig, tel)
		validationCtx, cancel := context.WithTimeout(ctx, cfg.Server.RequestTimeout.Value())
		validationErr := runtime.Validate(validationCtx)
		cancel()
		if validationErr != nil {
			logger.Warn("one or more upstreams failed initial validation", "chain", chainConfig.Name, "error", validationErr)
		}
		runtimes[chainConfig.Name] = runtime
		coordinatorOptions := coordinator.Options{
			Runtime: runtime, Store: store, Owner: owner, LeaderTTL: cfg.Redis.LeaderTTL.Value(),
			Logger: logger, Telemetry: tel,
		}
		coord := coordinator.NewCoordinator(coordinatorOptions)
		go coord.Run(ctx)
	}
	proxy := gateway.NewGateway(gateway.Options{Config: cfg, Store: store, Runtimes: runtimes, Logger: logger, Telemetry: tel})
	publicMux := http.NewServeMux()
	publicMux.Handle("POST /rpc/{chain}", proxy.RPCHandler())
	publicMux.Handle("GET /ws/{chain}", proxy.WebsocketHandler())
	// Keep connection-wide read/write deadlines unset on the shared public
	// listener: net/http carries them onto hijacked WebSocket connections.
	// Request deadlines are applied inside the RPC handler; ingress bounds body
	// transfer time before traffic reaches this private service.
	publicServer := &http.Server{Addr: cfg.Server.ListenAddress, Handler: publicMux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 1 << 20}
	adminServer := &http.Server{Addr: cfg.Server.AdminAddress, Handler: proxy.AdminHandler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 64 << 10}
	errorsChannel := make(chan error, 2)
	go func() {
		logger.Info("public server listening", "address", cfg.Server.ListenAddress)
		errorsChannel <- publicServer.ListenAndServe()
	}()
	go func() {
		logger.Info("admin server listening", "address", cfg.Server.AdminAddress)
		errorsChannel <- adminServer.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
	case err := <-errorsChannel:
		if !errors.Is(err, http.ErrServerClosed) {
			stop()
			return err
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return errors.Join(publicServer.Shutdown(shutdownCtx), adminServer.Shutdown(shutdownCtx))
}

func getInstanceID() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("read coordinator hostname: %w", err)
	}
	if hostname == "" {
		return "", errors.New("coordinator hostname is empty")
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid()), nil
}
