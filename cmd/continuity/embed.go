package main

import (
	"embed"
	"io/fs"

	"github.com/lazypower/continuity/internal/server"
)

// The ui directory is populated by `make build` which copies ui/dist here.
//
//go:embed all:ui
var uiDist embed.FS

func init() {
	sub, err := fs.Sub(uiDist, "ui")
	if err != nil {
		return
	}
	server.SetUI(sub)
}
