package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/pkg/browser"
	"golang.org/x/term"

	"github.com/danielloader/waggle/internal/api"
	"github.com/danielloader/waggle/internal/config"
	"github.com/danielloader/waggle/internal/ingest"
	"github.com/danielloader/waggle/internal/mcpserver"
	"github.com/danielloader/waggle/internal/server"
	"github.com/danielloader/waggle/internal/store/sqlite"
)

// version is the build version, overridden at release time via
// -ldflags "-X main.version=...". Defaults to "dev" for local builds.
var version = "dev"

func main() {
	// `waggle mcp` runs the read-only MCP server over stdio against a database
	// file, for clients (e.g. Claude) that spawn the process directly. It must
	// be handled before config.Load parses the global flag set.
	if len(os.Args) > 1 && os.Args[1] == "mcp" {
		if err := runMCPStdio(os.Args[2:]); err != nil {
			slog.Error("mcp stdio exited", "err", err)
			os.Exit(1)
		}
		return
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid config", "err", err)
		os.Exit(2)
	}

	if cfg.Version {
		fmt.Println(version)
		return
	}

	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	// Block/mutex profiles are empty unless sampling rates are set. Gate on
	// --dev so release binaries pay no overhead — the pprof HTTP surface is
	// already --dev-only (see internal/server/server.go).
	if cfg.Dev {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}

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

	tee, err := ingest.NewTee(ingest.TeeConfig{
		Path:     cfg.TeePath,
		Services: cfg.TeeServices,
		MinSev:   cfg.TeeMinSev,
		Format:   cfg.TeeFormat,
		Color:    cfg.TeeColor,
	})
	if err != nil {
		log.Error("failed to open tee", "err", err)
		os.Exit(1)
	}
	if tee != nil {
		log.Info("tee enabled", "path", cfg.TeePath, "services", cfg.TeeServices, "min_severity", cfg.TeeMinSev, "format", cfg.TeeFormat)
	}

	handler := ingest.NewHandler(writer, log).WithTee(tee)
	grpcHandler := ingest.NewGRPCHandler(writer, log).WithTee(tee)
	router := api.NewRouter(st, log)
	srv := server.New(cfg, log, st, handler, router).WithGRPC(grpcHandler)
	if cfg.MCPEnabled {
		srv.WithMCP(mcpserver.New(st, log).HTTPHandler())
		log.Info("mcp endpoint enabled", "path", "/mcp")
	}

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
	if err := tee.Close(); err != nil {
		log.Warn("tee close", "err", err)
	}
}

// runMCPStdio serves the read-only MCP tools over stdio against a database
// file. Logs go to stderr so stdout stays clean for the MCP protocol.
func runMCPStdio(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	dbPath := fs.String("db-path", envOr("WAGGLE_DB", "./waggle.db"), "SQLite file path")
	logLevel := fs.String("log-level", envOr("WAGGLE_LOG_LEVEL", "info"), "slog level: debug, info, warn, error")
	if err := fs.Parse(args); err != nil {
		return err
	}

	log := newLogger(*logLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := sqlite.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	log.Info("mcp stdio server starting", "db", *dbPath)
	return mcpserver.New(st, log).RunStdio(ctx)
}

func envOr(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
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
	sweep := func(now time.Time) {
		cutoff := now.Add(-retention).UnixNano()
		if err := st.Retain(ctx, cutoff); err != nil {
			log.Warn("retention sweep failed", "err", err)
		}
	}
	// Sweep once at startup so a restart doesn't leave stale data sitting for up
	// to a full tick (and a sub-tick restart loop still culls).
	sweep(time.Now())
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			sweep(now)
		}
	}
}
