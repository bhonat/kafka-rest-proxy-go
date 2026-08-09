package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/kafka-rest-proxy-go/internal/api"
	"github.com/example/kafka-rest-proxy-go/internal/config"
	"github.com/example/kafka-rest-proxy-go/internal/metrics"
	franzproducer "github.com/example/kafka-rest-proxy-go/internal/producer/franz"
	schemaproducer "github.com/example/kafka-rest-proxy-go/internal/schema"
)

var version = "dev"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("server exited", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		return err
	}

	prod, err := franzproducer.New(cfg.Kafka)
	if err != nil {
		return err
	}
	defer prod.Close()

	m, err := metrics.New()
	if err != nil {
		return err
	}
	m.SetAdmissionSnapshot(prod.AdmissionSnapshot)
	m.SetBufferedSnapshot(prod.BufferedSnapshot)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			log.Warn("shutdown metrics provider", "error", err)
		}
	}()

	registry, err := schemaproducer.NewHTTPRegistry(cfg.SchemaRegistry.URL, cfg.SchemaRegistry.Username, cfg.SchemaRegistry.Password)
	if err != nil {
		return err
	}
	var schemaEncoder *schemaproducer.Encoder
	if registry != nil {
		schemaEncoder = schemaproducer.NewEncoder(registry)
	}

	handler := api.NewHandlerWithSchema(prod, m, api.Config{
		MaxRequestBytes: cfg.RequestMaxBytes,
		MaxRecords:      cfg.RequestMaxRecords,
		MaxRecordBytes:  cfg.RequestMaxRecordBytes,
		MaxKeyBytes:     cfg.RequestMaxKeyBytes,
		MaxHeaders:      cfg.RequestMaxHeaders,
		MaxHeaderBytes:  cfg.RequestMaxHeaderBytes,
		AllowedTopics:   cfg.TopicAllowlist,
		ProduceTimeout:  cfg.ProduceTimeout,
		BearerTokens:    cfg.AuthBearerTokens,
		PprofEnable:     cfg.PprofEnable,
		ClusterID:       cfg.ClusterID,

		RateLimitRequestsPerSecond: cfg.RateLimit.RequestsPerSecond,
		RateLimitRequestsBurst:     cfg.RateLimit.RequestsBurst,
		RateLimitBytesPerSecond:    cfg.RateLimit.BytesPerSecond,
		RateLimitBytesBurst:        cfg.RateLimit.BytesBurst,
	}, log, schemaEncoder)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("starting kafka-rest-proxy-go", "version", version, "addr", cfg.HTTPAddr, "brokers", cfg.Kafka.Brokers)
		errCh <- srv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Info("shutdown signal received", "signal", sig.String())
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		return err
	}
	prod.Close()
	return nil
}
