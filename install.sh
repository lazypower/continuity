#!/bin/sh
set -e

REPO="lazypower/continuity"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$ARCH" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

case "$OS" in
  darwin|linux) ;;
  *) echo "Unsupported OS: $OS" >&2; exit 1 ;;
esac

BINARY="continuity-${OS}-${ARCH}"

# Get latest version
if command -v curl >/dev/null 2>&1; then
  VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed 's/.*"tag_name": *"//;s/".*//')
elif command -v wget >/dev/null 2>&1; then
  VERSION=$(wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed 's/.*"tag_name": *"//;s/".*//')
else
  echo "Error: curl or wget required" >&2
  exit 1
fi

if [ -z "$VERSION" ]; then
  echo "Error: could not determine latest version" >&2
  exit 1
fi

URL="https://github.com/${REPO}/releases/download/${VERSION}/${BINARY}"

echo "Installing continuity ${VERSION} (${OS}/${ARCH})..."

TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT

if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$URL" -o "$TMPFILE"
else
  wget -qO "$TMPFILE" "$URL"
fi

chmod +x "$TMPFILE"

# Install — use sudo if needed
if [ -w "$INSTALL_DIR" ]; then
  mv "$TMPFILE" "${INSTALL_DIR}/continuity"
else
  echo "Installing to ${INSTALL_DIR} (requires sudo)..."
  sudo mv "$TMPFILE" "${INSTALL_DIR}/continuity"
fi

echo "continuity installed to ${INSTALL_DIR}/continuity"
echo ""
echo "Quick start:"
echo "  continuity serve &     # Start the server"
echo "  continuity version     # Verify installation"
echo ""
echo "Add hooks to Claude Code (~/.claude/settings.json):"
echo '  "hooks": {'
echo '    "SessionStart": [{"type":"command","command":"continuity hook start"}],'
echo '    "UserPromptSubmit": [{"type":"command","command":"continuity hook submit"}],'
echo '    "PostToolUse": [{"type":"command","command":"continuity hook tool"}],'
echo '    "Stop": [{"type":"command","command":"continuity hook stop --transcript=\${CLAUDE_TRANSCRIPT}"}],'
echo '    "SessionEnd": [{"type":"command","command":"continuity hook end"}]'
echo '  }'
