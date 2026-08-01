[Docs](../README.md) › Getting started › Install

# Install

Get the `continuity` program onto your machine and confirm it runs.

**Audience:** operator · **Read time:** ~2 min

---

Continuity is a single program with no runtime dependencies. No Docker, no
Node.js, no Python.

## Pick one

```bash
# macOS / Linux — install script
curl -fsSL https://raw.githubusercontent.com/lazypower/continuity/main/install.sh | sh

# Homebrew
brew install lazypower/tap/continuity

# Homebrew — development channel, rebuilt from the main branch, expect rough edges
brew install lazypower/tap/continuity-dev

# Arch Linux (AUR) — community maintained by @klrmngr
yay -S continuity-bin

# From source — needs Go 1.24+
git clone https://github.com/lazypower/continuity.git
cd continuity && make build
```

The install script downloads and immediately runs code from GitHub. If you would
rather read it first:

```bash
curl -fsSL https://raw.githubusercontent.com/lazypower/continuity/main/install.sh -o install.sh
less install.sh
sh install.sh
```

## Check it worked

```bash
continuity version
```

```
continuity v0.11.0 (commit: 4afa791, built: 2026-08-01)
```

## Start the server

```bash
continuity serve
```

```
continuity serving on 127.0.0.1:37777
  db: ~/.continuity/continuity.db
  llm: claude-cli (haiku)
  embedder: model2vec:potion-retrieval-32M:512
```

That last line may pause for a moment on a brand new install — the built-in
search method downloads a ~33 MB model file once, into
`~/.continuity/models/`. If you are offline, Continuity falls back to simple
keyword matching for now and tells you so.

Leave that running and open a second terminal, or better, set it up to run on its
own:

```bash
continuity install-service
```

This shows you exactly what it will install and asks before doing anything. It
starts Continuity on login and restarts it if it crashes — a LaunchAgent on macOS,
a systemd user unit on Linux. Remove it with `continuity uninstall-service`.

> The server keeps running after you close your terminal. To stop it, see
> [Stopping it](../README.md#stopping-it).

## What gets created

| Path | What it is |
|---|---|
| `~/.continuity/continuity.db` | Everything Continuity stores |
| `~/.continuity/serve.log` | Server log |
| `~/.continuity/models/` | Downloaded search model, if used |
| `~/.continuity/config.toml` | Settings — only if you create it |

Full list in [Files and paths](../reference/files-and-paths.md).

---

**Next:** [Connect it to Claude Code](claude-code-setup.md)
**See also:** [Configuration](../guides/configuration.md) · [Troubleshooting](../guides/troubleshooting.md)
