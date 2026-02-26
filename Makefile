BINARY := continuity
MODULE := github.com/lazypower/continuity
CLI_PKG := $(MODULE)/internal/cli

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "v0.0.1-dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X $(CLI_PKG).Version=$(VERSION) \
           -X $(CLI_PKG).Commit=$(COMMIT) \
           -X $(CLI_PKG).BuildDate=$(DATE)

.PHONY: build test clean run

build:
	devbox run -- go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/continuity

test:
	devbox run -- go test ./... -v

clean:
	rm -f $(BINARY)

run: build
	./$(BINARY) serve
