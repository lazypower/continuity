#!/usr/bin/env bash
# gen-migration-fixtures.sh — mint migration golden fixtures from REAL released
# continuity binaries.
#
# For each shipped schema version we download the published release binary for
# the host platform, boot it to create + self-migrate an isolated database to
# its own schema, seed representative data, and write a clean single-file golden.
# The committed goldens are what the hermetic PR-gate test
# (internal/store/migration_fixture_test.go) migrates forward to head.
#
# Coverage: one fixture per DISTINCT shipped schema. Only v5/v7/v8 ever shipped
# (v0.1.0–v0.2.2 are byte-identical schema-v5), so three fixtures cover every
# upgrade path a real user can take into the current head schema.
#
# Requirements: gh (authenticated), go, a network connection.
#
# Usage:
#   scripts/gen-migration-fixtures.sh              # write committed testdata/
#   OUT_DIR=/tmp/fx scripts/gen-migration-fixtures.sh   # write elsewhere (CI regen)
set -euo pipefail

# (schema_version, release_tag) — newest release at each distinct schema.
FIXTURES=(
  "5:v0.2.2"
  "7:v0.4.0"
  "8:v0.5.0"
)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/internal/store/testdata/migration}"
CACHE_DIR="${CACHE_DIR:-${TMPDIR:-/tmp}/continuity-release-cache}"
mkdir -p "$CACHE_DIR"

# Host platform → release asset name (assets: continuity-<os>-<arch>[.exe]).
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$os" in
  darwin) os="darwin" ;;
  linux)  os="linux" ;;
  *) echo "unsupported OS for fixture generation: $os" >&2; exit 1 ;;
esac
arch="$(uname -m)"
case "$arch" in
  arm64|aarch64) arch="arm64" ;;
  x86_64|amd64)  arch="amd64" ;;
  *) echo "unsupported arch for fixture generation: $arch" >&2; exit 1 ;;
esac
ASSET="continuity-${os}-${arch}"

echo "host platform: ${os}/${arch}  (asset: ${ASSET})"
echo "output dir:    ${OUT_DIR}"
echo

for entry in "${FIXTURES[@]}"; do
  schema="${entry%%:*}"
  tag="${entry##*:}"
  bin="${CACHE_DIR}/continuity-${tag}-${os}-${arch}"

  if [[ ! -x "$bin" ]]; then
    echo "==> downloading ${tag} (${ASSET})"
    gh release download "$tag" --repo lazypower/continuity \
      --pattern "$ASSET" --output "$bin" --clobber
    chmod +x "$bin"
  else
    echo "==> using cached ${tag} binary"
  fi

  out="${OUT_DIR}/v${schema}/continuity.db"
  echo "==> minting schema v${schema} golden from ${tag} -> ${out}"
  ( cd "$REPO_ROOT" && go run ./scripts/genfixtures -bin "$bin" -schema "$schema" -out "$out" )
  echo
done

echo "done. fixtures:"
find "$OUT_DIR" -name 'continuity.db' -print
