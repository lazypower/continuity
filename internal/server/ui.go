package server

import (
	"io/fs"
	"net/http"
	"strings"
)

// uiFS holds the embedded UI filesystem. Set via SetUI before creating the server.
var uiFS fs.FS

// SetUI sets the embedded filesystem for serving the UI.
func SetUI(fsys fs.FS) {
	uiFS = fsys
}

// spaHandler serves static files from the embedded FS with SPA fallback.
// Any path not matching a real file returns index.html.
func spaHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if uiFS == nil {
			http.Error(w, "UI not embedded — build with 'make build'", http.StatusNotFound)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}

		// Try to open the requested file
		f, err := uiFS.Open(path)
		if err != nil {
			// SPA fallback — serve index.html for client-side routing
			path = "index.html"
		} else {
			f.Close()
		}

		http.ServeFileFS(w, r, uiFS, path)
	}
}
