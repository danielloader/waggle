package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

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

	ingestHandler *ingest.Handler
	apiRouter     *api.Router

	ingestSrv *http.Server
	uiSrv     *http.Server
}

func New(cfg *config.Config, log *slog.Logger, st store.Store, ih *ingest.Handler, ar *api.Router) *Server {
	return &Server{cfg: cfg, log: log, store: st, ingestHandler: ih, apiRouter: ar}
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

	if err := s.listen(); err != nil {
		return err
	}
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
	go func() {
		s.log.Info("ui/api listening", "addr", s.uiSrv.Addr)
		if err := s.uiSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("ui server exited", "err", err)
		}
	}()
	return nil
}

// Shutdown triggers a graceful shutdown of both listeners.
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
		})
	})
	if s.apiRouter != nil {
		s.apiRouter.Mount(mux)
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
