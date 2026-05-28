package mcpserver

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/danielloader/waggle/internal/store/sqlite"
)

// TestNewRegistersTools guards tool schema generation: mcp.AddTool panics if a
// tool's input/output type can't be reflected into a JSON schema (e.g. a map
// with a non-string key). Constructing the server exercises every AddTool, so
// this catches such regressions at test time rather than at startup.
func TestNewRegistersTools(t *testing.T) {
	s, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "mcp.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	srv := New(s, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if srv == nil || srv.HTTPHandler() == nil {
		t.Fatal("New returned an unusable server")
	}
}
