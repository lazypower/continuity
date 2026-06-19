//go:build noembed

// The noembed build tag drops the embedded UI assets. This lets `go build`
// succeed without the `make ui` prerequisite, which is required by hermetic
// subprocess tests in CI environments that do not run the npm/devbox UI
// pipeline. The resulting binary serves the API but returns 404 for the
// viewer SPA.
package main
