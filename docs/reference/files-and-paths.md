[Docs](../README.md) › Reference › Files and paths

# Files and paths

Everything Continuity owns on disk — inside `~/.continuity/` and outside it — with what each file is for and whether it is safe to delete.

**Audience:** operator · **Read time:** ~5 min

---

## The short version

- Everything Continuity stores lives in **`~/.continuity/`**, and the only file
  in there you cannot recreate is **`continuity.db`**. Back up that one.
- Outside that directory, Continuity writes to exactly three places: a managed
  block in `~/.claude/CLAUDE.md`, a service definition, and per-session marker
  files in the system temp directory.
- Continuity never writes to your project directories.

---

## `~/.continuity/`

Created with mode `0700` (owner only). Continuity tightens permissions on
existing installs each time it opens the database, so an older, looser directory
is fixed on the next run.

| Path | What it is | Safe to delete | Created by |
|---|---|---|---|
| `continuity.db` | **The database.** Memories, vectors, sessions, observations, the relational profile — everything. Mode `0600`. | **No.** This is your memory. | First run of anything that opens the database |
| `continuity.db-wal`, `continuity.db-shm` | SQLite write-ahead log and shared-memory index. Mode `0600`. Part of the database — a copy of `continuity.db` alone is **incomplete**. | No — and never copy the database without them | SQLite (WAL mode) |
| `config.toml` | Settings. Mode `0600`. Optional; absent means all defaults. | Yes — you lose your settings, not your memories | You, or `continuity embedder use` |
| `serve.log` | Server log. Mode `0600`, appended to, **never rotated**. | Yes, any time | Detached/service `serve` |
| `autostart` | Marker file. Its *presence* means the SessionStart hook may launch the server. Contents are ignored. | Yes — that disables autostart | `continuity init --autostart` |
| `autostart-bounce` | Marker file. Its presence opts into the SessionStart hook auto-restarting a stale server, but only when the server is not service-managed. | Yes — you fall back to a warning | You, by hand. **Advanced.** |
| `restart.lock` | Short-lived lock held during a restart, containing the owner PID. Mode `0600`. | Yes if no restart is running; it is reaped automatically once its owner dies, or after 2 minutes if the owner cannot be identified | `continuity restart`, hook auto-bounce |
| `sandbox/` | An empty directory, mode `0700`. It is the working directory for the `claude -p` subprocess, so the LLM has no surrounding project to scan. | Yes — it is recreated | The `claude-cli` LLM provider |
| `models/` | Downloaded embedding model files. See below. | Yes — they re-download | The model2vec embedder |
| `snapshots/` | Database safety copies. See below. | Mostly — see below | Migrations, `doctor --repair-vectors --apply`, `embedder use` |

### `~/.continuity/models/`

```
~/.continuity/models/potion-retrieval-32M/
├── model.safetensors     ~32 MB
└── tokenizer.json
```

Downloaded once, the first time the model2vec backend is selected — which for a
fresh install is the default. The files come from Continuity's own GitHub release,
not from Hugging Face. A partial download is written to a temp file and renamed
into place, so an interrupted download can never leave a file that looks
complete.

Deleting the directory is safe; the files re-download on the next boot that
selects model2vec. If you are offline, that boot falls back to the lexical
backend for the session instead.

### `~/.continuity/snapshots/`

Snapshots are namespaced by database filename, so two databases sharing a parent
directory can never see or delete each other's copies:

```
~/.continuity/snapshots/continuity.db/
├── continuity-pre-v12-2026-08-01T14-03-11Z.db
└── continuity-pre-repair-vectors-2026-08-01T14-09-52Z.db
```

Each file is a complete, self-contained database copy (mode `0600`), made with
SQLite's `VACUUM INTO` so it includes everything in the write-ahead log. That
means a snapshot **is** a valid database on its own — no `-wal` file needed.

There are two kinds, and they behave differently:

| Prefix | Taken by | Tracked | Auto-deleted |
|---|---|---|---|
| `continuity-pre-v<N>-` | A risky schema migration, before it runs | Yes | Yes — after **3** successful `serve` boots |
| `continuity-pre-repair-vectors-` | `doctor --repair-vectors --apply` and `embedder use` | No | **No** |

Only the tracked kind is managed. `continuity snapshot list` shows them and how
many boots remain; `continuity snapshot prune` deletes them. **Repair snapshots
are neither listed nor pruned** — they accumulate until you delete them yourself.
Each one is the size of your database, so check this directory occasionally.

To restore from any snapshot, stop the server and copy the file over the live
database:

```bash
cp ~/.continuity/snapshots/continuity.db/continuity-pre-v12-....db \
   ~/.continuity/continuity.db
```

There is no `restore` command on purpose — that decision is yours to make
deliberately.

---

## Outside `~/.continuity/`

| Path | What it is | Written by | Safe to delete |
|---|---|---|---|
| `~/.claude/CLAUDE.md` | Continuity appends a **managed block** between `<!-- continuity:managed -->` and `<!-- /continuity:managed -->`, telling the agent to use Continuity as its memory. Mode `0644`. | `continuity init` | Delete the block, not the file — everything outside the markers is yours and is preserved verbatim on re-run |
| `~/.claude/settings.json` | Where your hook definitions live. **Continuity never reads or writes this file** — Claude Code does. | You | Deleting it stops Continuity capturing anything |
| `~/Library/LaunchAgents/com.continuity.server.plist` | macOS service definition. Mode `0644`. Runs `continuity serve` at login, restarts on crash, logs to `~/.continuity/serve.log`, working directory `~/.continuity`, with a `PATH` captured at install time. | `continuity install-service` | Use `continuity uninstall-service` instead |
| `~/.config/systemd/user/continuity.service` | Linux service definition. Mode `0644`. Same behavior as the plist. | `continuity install-service` | Use `continuity uninstall-service` instead |
| `$TMPDIR/continuity-unreachable-<session-id>` | A zero-byte marker so the "server unreachable" warning prints once per session rather than once per tool call. Mode `0600`. | Any hook, when the server is down | Yes, any time — your OS clears them anyway |
| `<project>/.mcp.json` | Registers `continuity mcp` with an MCP client. | You | Yes — you lose the MCP tools, not your memories |

### Files Continuity reads but does not own

| Path | Why |
|---|---|
| `~/.claude/projects/*/<session-id>.jsonl` | Claude Code's session transcripts. `continuity extract` auto-discovers them here. Read-only, and never modified. |

### The binary

| Install method | Location |
|---|---|
| `install.sh` | `/usr/local/bin/continuity`, or `$INSTALL_DIR/continuity` if you set it |
| Homebrew | Your Homebrew prefix, e.g. `/opt/homebrew/bin/continuity` |

`install-service` deliberately records the `PATH`-resolved location rather than
the real file, so a `brew upgrade` that moves the versioned binary does not break
the service.

---

## Practical answers

**What do I back up?** `~/.continuity/continuity.db` — but stop the server first,
or copy a snapshot instead. A live database's most recent writes sit in the
`-wal` file, so a plain `cp` of the `.db` alone can silently miss them.

**What is safe to delete when I am short on disk?**
Old files under `~/.continuity/snapshots/` (especially the repair snapshots
nothing cleans up), `~/.continuity/serve.log`, and `~/.continuity/models/`. Then
run [`continuity prune`](cli.md#prune) — that is what actually shrinks the
database file.

**How do I move everything to another machine?** Copy `~/.continuity/` in full
with the server stopped, then run `continuity init` on the new machine to write
the `~/.claude/CLAUDE.md` block, and `continuity install-service` if you want the
service. The service definitions themselves embed machine-specific paths and
should not be copied.

**How do I remove Continuity completely?**

```bash
continuity uninstall-service     # if you installed one
rm -rf ~/.continuity             # deletes your memories, permanently
```

Then delete the managed block from `~/.claude/CLAUDE.md`, remove the Continuity
hooks from `~/.claude/settings.json`, and delete the binary.

---

**See also:** [Configuration keys](configuration.md) · [CLI reference](cli.md) · [Keeping it healthy](../guides/keeping-it-healthy.md) · [Upgrading](../upgrading.md)
