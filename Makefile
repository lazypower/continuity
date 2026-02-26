BINARY := continuity
MODULE := github.com/lazypower/continuity
CLI_PKG := $(MODULE)/internal/cli

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "v0.0.1-dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X $(CLI_PKG).Version=$(VERSION) \
           -X $(CLI_PKG).Commit=$(COMMIT) \
           -X $(CLI_PKG).BuildDate=$(DATE)

PLATFORMS := darwin/arm64 darwin/amd64 linux/amd64 linux/arm64 windows/amd64

.PHONY: build test clean run ui dist

ui:
	devbox run -- bash -c 'cd ui && npm install && npm run build'
	rm -rf cmd/continuity/ui
	cp -r ui/dist cmd/continuity/ui

build: ui
	devbox run -- go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/continuity

test:
	devbox run -- go test ./... -v

dist: ui
	@mkdir -p dist
	@for platform in $(PLATFORMS); do \
		GOOS=$${platform%/*} GOARCH=$${platform#*/} ; \
		EXT="" ; \
		if [ "$$GOOS" = "windows" ]; then EXT=".exe"; fi ; \
		OUTPUT="dist/$(BINARY)-$$GOOS-$$GOARCH$$EXT" ; \
		echo "Building $$OUTPUT..." ; \
		devbox run -- env CGO_ENABLED=0 GOOS=$$GOOS GOARCH=$$GOARCH \
			go build -ldflags "$(LDFLAGS)" -o "$$OUTPUT" ./cmd/continuity ; \
	done
	@echo "Checksums:"
	@cd dist && shasum -a 256 *

clean:
	rm -f $(BINARY)
	rm -rf cmd/continuity/ui
	rm -rf dist

run: build
	./$(BINARY) serve
