// Package web serves the built frontend, which the Dockerfile and `make build`
// copy into web/dist before compiling. The directory is embedded so the
// backend ships as a single binary (system-design.md §6, decision A9).
package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed all:dist
var dist embed.FS

// Handler serves the embedded frontend. If the frontend has not been built,
// it serves a short explanation instead of a 404 so the dev loop is obvious.
func Handler() http.Handler {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err) // embed layout is fixed at compile time; this cannot happen at runtime
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte("AnywhereDrop backend is running, but the frontend is not built into this binary.\nRun `make build` (or use the Vite dev server with `make dev`).\n"))
		})
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Single-page app: unknown paths without a file extension fall back to index.html.
		if r.URL.Path != "/" {
			if _, err := fs.Stat(sub, r.URL.Path[1:]); err != nil {
				r.URL.Path = "/"
			}
		}
		fileServer.ServeHTTP(w, r)
	})
}
