package server

// Confirms the grpc listener stays disabled when --grpc-addr is empty (or
// when no GRPCHandler is attached). Writing this as `package server` so
// the assertion can read the unexported ingestGRPCSrv field directly.

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/danielloader/waggle/internal/api"
	"github.com/danielloader/waggle/internal/config"
	"github.com/danielloader/waggle/internal/ingest"
)

func TestGRPCDisabledWhenAddrEmpty(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		Addr:       "127.0.0.1:0",
		IngestAddr: "127.0.0.1:0",
		UIAddr:     "127.0.0.1:0",
		GRPCAddr:   "",
		Dev:        true,
	}
	// Even with a handler attached, an empty GRPCAddr disables binding.
	srv := New(cfg, log, nil, nil, api.NewRouter(nil, log)).
		WithGRPC(ingest.NewGRPCHandler(nil, log))

	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		sctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = srv.Shutdown(sctx)
	})

	if srv.ingestGRPCSrv != nil {
		t.Errorf("ingestGRPCSrv should be nil when GRPCAddr is empty, got %#v", srv.ingestGRPCSrv)
	}
	if srv.ingestGRPCLis != nil {
		t.Errorf("ingestGRPCLis should be nil when GRPCAddr is empty")
	}
}

func TestGRPCDisabledWhenHandlerNil(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		Addr:       "127.0.0.1:0",
		IngestAddr: "127.0.0.1:0",
		UIAddr:     "127.0.0.1:0",
		GRPCAddr:   "127.0.0.1:0", // address set, but no handler
		Dev:        true,
	}
	srv := New(cfg, log, nil, nil, api.NewRouter(nil, log))

	if err := srv.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		sctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = srv.Shutdown(sctx)
	})

	if srv.ingestGRPCSrv != nil {
		t.Errorf("ingestGRPCSrv should be nil when no handler is attached")
	}
}
