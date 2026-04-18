package ui

import (
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// SPAHandler serves files from the embedded dist directory, falling back to
// index.html for any path that does not match a real asset. This lets the
// client-side router own arbitrary paths without the server 404-ing.
//
// In --dev mode the caller should skip mounting this handler and let the
// browser hit the Vite dev server on a separate port.
func SPAHandler() (http.Handler, error) {
	root, err := Dist()
	if err != nil {
		return nil, err
	}
	fileServer := http.FileServer(http.FS(root))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := path.Clean(r.URL.Path)
		if clean == "/" || clean == "." {
			serveIndex(w, r, root)
			return
		}
		name := strings.TrimPrefix(clean, "/")
		f, err := root.Open(name)
		if err != nil {
			serveIndex(w, r, root)
			return
		}
		f.Close()
		fileServer.ServeHTTP(w, r)
	}), nil
}

func serveIndex(w http.ResponseWriter, _ *http.Request, root fs.FS) {
	f, err := root.Open("index.html")
	if err != nil {
		http.Error(w, "UI not built; run `make build` or start in --dev mode and use the Vite dev server", http.StatusNotFound)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = io.Copy(w, f)
}
