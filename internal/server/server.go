package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"google.golang.org/grpc"

	"github.com/danielloader/waggle/internal/api"
	"github.com/danielloader/waggle/internal/config"
	"github.com/danielloader/waggle/internal/ingest"
	"github.com/danielloader/waggle/internal/store"
	"github.com/danielloader/waggle/internal/ui"
)

type Server struct {
	cfg   *config.Config
	log   *slog.Logger
	store store.Store

	ingestHandler     *ingest.Handler
	ingestGRPCHandler *ingest.GRPCHandler
	apiRouter         *api.Router
	mcpHandler        http.Handler

	ingestSrv     *http.Server
	uiSrv         *http.Server
	ingestGRPCSrv *grpc.Server
	ingestGRPCLis net.Listener
}

func New(cfg *config.Config, log *slog.Logger, st store.Store, ih *ingest.Handler, ar *api.Router) *Server {
	return &Server{cfg: cfg, log: log, store: st, ingestHandler: ih, apiRouter: ar}
}

// WithGRPC attaches the OTLP gRPC handler. Pass nil (or omit the call) to
// keep the gRPC listener disabled. Chainable.
func (s *Server) WithGRPC(h *ingest.GRPCHandler) *Server {
	s.ingestGRPCHandler = h
	return s
}

// WithMCP attaches the read-only MCP HTTP handler, mounted at /mcp on the UI
// listener. Pass nil (or omit the call) to leave the endpoint off. Chainable.
func (s *Server) WithMCP(h http.Handler) *Server {
	s.mcpHandler = h
	return s
}

// Start binds the listeners and begins serving. It returns once both
// listeners are accepting connections. Use Wait to block on shutdown.
func (s *Server) Start(ctx context.Context) error {
	ingestMux := http.NewServeMux()
	uiMux := http.NewServeMux()

	s.mountIngest(ingestMux)
	s.mountAPI(uiMux)
	if !s.cfg.Dev {
		if err := s.mountUI(uiMux); err != nil {
			return err
		}
	}

	if s.cfg.SplitListeners() {
		s.ingestSrv = httpServer(s.cfg.IngestAddr, withCORS(ingestMux))
		s.uiSrv = httpServer(s.cfg.UIAddr, withCORS(uiMux))
	} else {
		combined := http.NewServeMux()
		combined.Handle("/v1/", ingestMux)
		combined.Handle("/api/", uiMux)
		combined.Handle("/", uiMux)
		s.uiSrv = httpServer(s.cfg.Addr, combined)
	}

	if err := s.bindGRPC(); err != nil {
		return err
	}

	if err := s.listen(); err != nil {
		return err
	}
	return nil
}

// bindGRPC creates the listener + grpc.Server when both an address and a
// handler are configured. Listening fails fast so port conflicts surface
// before Start returns.
func (s *Server) bindGRPC() error {
	if s.cfg.GRPCAddr == "" || s.ingestGRPCHandler == nil {
		return nil
	}
	lis, err := net.Listen("tcp", s.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("grpc listen %s: %w", s.cfg.GRPCAddr, err)
	}
	srv := grpc.NewServer()
	s.ingestGRPCHandler.Register(srv)
	s.ingestGRPCLis = lis
	s.ingestGRPCSrv = srv
	return nil
}

func (s *Server) listen() error {
	if s.ingestSrv != nil {
		go func() {
			s.log.Info("ingest listening", "addr", s.ingestSrv.Addr)
			if err := s.ingestSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error("ingest server exited", "err", err)
			}
		}()
	}
	if s.ingestGRPCSrv != nil {
		go func() {
			s.log.Info("grpc listening", "addr", s.ingestGRPCLis.Addr().String())
			if err := s.ingestGRPCSrv.Serve(s.ingestGRPCLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				s.log.Error("grpc server exited", "err", err)
			}
		}()
	}
	go func() {
		s.log.Info("ui/api listening", "addr", s.uiSrv.Addr)
		if err := s.uiSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("ui server exited", "err", err)
		}
	}()
	return nil
}

// Shutdown triggers a graceful shutdown of all listeners.
func (s *Server) Shutdown(ctx context.Context) error {
	var firstErr error
	if s.ingestSrv != nil {
		if err := s.ingestSrv.Shutdown(ctx); err != nil {
			firstErr = err
		}
	}
	if s.uiSrv != nil {
		if err := s.uiSrv.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.ingestGRPCSrv != nil {
		// grpc has no context-aware Shutdown — race GracefulStop against
		// the deadline and fall back to Stop() so we still respect the
		// caller's timeout.
		done := make(chan struct{})
		go func() {
			s.ingestGRPCSrv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			s.ingestGRPCSrv.Stop()
			<-done
		}
	}
	return firstErr
}

// URL returns the browser-openable URL for the UI.
func (s *Server) URL() string {
	addr := s.cfg.UIAddr
	if host, port, err := net.SplitHostPort(addr); err == nil && host == "0.0.0.0" {
		addr = net.JoinHostPort("127.0.0.1", port)
	}
	return "http://" + addr
}

func (s *Server) mountIngest(mux *http.ServeMux) {
	if s.ingestHandler != nil {
		s.ingestHandler.Mount(mux)
		return
	}
	mux.HandleFunc("POST /v1/traces", notImplemented("ingest handler not configured"))
	mux.HandleFunc("POST /v1/logs", notImplemented("ingest handler not configured"))
}

func (s *Server) mountAPI(mux *http.ServeMux) {
	// /api/health also echoes the listen address so the UI's sidebar
	// footer can show the real port waggle is running on (supports the
	// --ingest-addr / --addr overrides) instead of hardcoding ":4318".
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":          true,
			"listen_addr": s.cfg.IngestAddr,
			"grpc_addr":   s.cfg.GRPCAddr,
		})
	})
	// pprof endpoints — gated on --dev so release binaries don't expose
	// diagnostic surface. In dev the handlers are the stock library code
	// and the listener is localhost by default.
	if s.cfg.Dev {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	if s.apiRouter != nil {
		s.apiRouter.Mount(mux)
	}
	if s.mcpHandler != nil {
		mux.Handle("/mcp", s.mcpHandler)
	}
}

func (s *Server) mountUI(mux *http.ServeMux) error {
	h, err := ui.SPAHandler()
	if err != nil {
		return err
	}
	mux.Handle("/", h)
	return nil
}

func httpServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Encoding")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func notImplemented(msg string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": msg})
	}
}
