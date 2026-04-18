package ui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

func Dist() (fs.FS, error) {
	return fs.Sub(distFS, "dist")
}
