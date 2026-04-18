package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/browser"
	"golang.org/x/term"

	"github.com/danielloader/waggle/internal/api"
	"github.com/danielloader/waggle/internal/config"
	"github.com/danielloader/waggle/internal/ingest"
	"github.com/danielloader/waggle/internal/server"
	"github.com/danielloader/waggle/internal/store/sqlite"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid config", "err", err)
		os.Exit(2)
	}

	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := sqlite.Open(ctx, cfg.DBPath)
	if err != nil {
		log.Error("failed to open store", "err", err, "path", cfg.DBPath)
		os.Exit(1)
	}
	defer st.Close()

	writer := ingest.NewWriter(st, log, ingest.WriterConfig{})
	writer.Start(ctx)

	handler := ingest.NewHandler(writer, log)
	router := api.NewRouter(st, log)
	srv := server.New(cfg, log, st, handler, router)

	if err := srv.Start(ctx); err != nil {
		log.Error("failed to start server", "err", err)
		os.Exit(1)
	}

	go retentionSweep(ctx, log, st, cfg.Retention)

	if shouldOpenBrowser(cfg) {
		time.AfterFunc(250*time.Millisecond, func() {
			if err := browser.OpenURL(srv.URL()); err != nil {
				log.Warn("could not open browser", "err", err)
			}
		})
	}

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "err", err)
	}
	if err := writer.Stop(shutdownCtx); err != nil {
		log.Error("writer stop", "err", err)
	}
}

func shouldOpenBrowser(cfg *config.Config) bool {
	if cfg.Dev || cfg.NoOpenBrowser {
		return false
	}
	return term.IsTerminal(int(os.Stdin.Fd()))
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}

func retentionSweep(ctx context.Context, log *slog.Logger, st *sqlite.Store, retention time.Duration) {
	if retention <= 0 {
		return
	}
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			cutoff := now.Add(-retention).UnixNano()
			if err := st.Retain(ctx, cutoff); err != nil {
				log.Warn("retention sweep failed", "err", err)
			}
		}
	}
}
