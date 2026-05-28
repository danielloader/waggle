// Package mcpserver exposes waggle's stored traces, metrics, and logs to an
// MCP client (e.g. Claude). It is a thin adapter: every tool maps onto an
// existing store.Store read method or the internal/query builder, so there is
// no query logic here that the HTTP API doesn't already have.
//
// Two transports share one set of tool handlers:
//   - HTTPHandler returns a Streamable-HTTP handler to mount on the running
//     server's UI mux (the data is already live in-process).
//   - RunStdio serves the same tools over stdio for the `waggle mcp`
//     subcommand, pointed at a database file directly.
//
// All tools are read-only — the write paths on store.Store (WriteBatch,
// Clear, Retain) are never bound.
package mcpserver

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/danielloader/waggle/internal/store"
)

// version is reported in the MCP initialize handshake.
const version = "0.1.0"

// Server wraps the SDK server so callers never touch the SDK directly.
type Server struct {
	srv *mcp.Server
	log *slog.Logger
}

// New builds an MCP server with every waggle tool registered against st.
func New(st store.Store, log *slog.Logger) *Server {
	srv := mcp.NewServer(&mcp.Implementation{Name: "waggle", Version: version}, nil)
	registerTools(srv, &tools{store: st, log: log})
	return &Server{srv: srv, log: log}
}

// HTTPHandler returns a Streamable-HTTP handler serving this server. Mount it
// at a fixed path (waggle uses /mcp). Every request is served by the same
// server instance, which is safe: the store's reader pool handles concurrency.
func (s *Server) HTTPHandler() http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return s.srv
	}, nil)
}

// RunStdio serves the tools over stdin/stdout until the context is cancelled
// or stdin closes. Protocol traffic uses stdout, so all logging must go to
// stderr (the caller's slog handler does).
func (s *Server) RunStdio(ctx context.Context) error {
	return s.srv.Run(ctx, &mcp.StdioTransport{})
}
