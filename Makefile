BINARY := continuity
MODULE := github.com/lazypower/continuity
BUILDINFO_PKG := $(MODULE)/internal/buildinfo

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "v0.0.1-dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -X $(BUILDINFO_PKG).Version=$(VERSION) \
           -X $(BUILDINFO_PKG).Commit=$(COMMIT) \
           -X $(BUILDINFO_PKG).BuildDate=$(DATE)

PLATFORMS := darwin/arm64 darwin/amd64 linux/amd64 linux/arm64 windows/amd64

.PHONY: build test vet clean run ui dist migration-fixtures retrieval-fixtures

# Regenerate the retrieval golden fixture from a REAL Ollama nomic-embed-text.
# Embeds the curated corpus + queries (internal/goldretrieval) and writes
# internal/engine/testdata/retrieval/nomic.json. Requires: `ollama serve` with
# nomic-embed-text pulled. The committed fixture feeds the hermetic PR-gate test
# (TestRetrievalGolden_Nomic_*); rerun on the drift schedule or after changing
# the corpus — a rank flip afterward is a real embedder regression.
retrieval-fixtures:
	go run ./scripts/genretrievalfixtures

# Regenerate the migration golden fixtures from REAL released binaries.
# Downloads one published binary per distinct shipped schema (v5/v7/v8), boots
# each to mint + seed an isolated DB, and writes internal/store/testdata/migration/.
# Requires: gh (authenticated) + network. The committed fixtures feed the
# hermetic PR-gate test (TestMigrationFixtureE2E_*); rerun this after shipping a
# release that introduces a NEW distinct schema.
migration-fixtures:
	./scripts/gen-migration-fixtures.sh

ui:
	devbox run -- bash -c 'cd ui && npm install && npm run build'
	rm -rf cmd/continuity/ui
	cp -r ui/dist cmd/continuity/ui

build: ui
	devbox run -- go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/continuity

# Tests use -tags noembed so the UI dist is not a prerequisite — keeps
# `make test` runnable on a fresh clone without first running `make ui`.
# The subprocess e2e test (internal/cli/retract_e2e_test.go) likewise builds
# the binary it spawns with -tags noembed.
test:
	devbox run -- go test -tags noembed ./... -v

vet:
	devbox run -- go vet -tags noembed ./...

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
